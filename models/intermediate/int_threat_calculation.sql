{% set partitions_to_replace = [
    'date(date_add(current_date, interval -3 day))',
    'date(date_add(current_date, interval -2 day))',
    'date(date_add(current_date, interval -1 day))',
    'date(current_date)'
] %}

{{ 
    config(
        cluster_by = ['referenced_view_or_table'],
        partition_by={
            "field": "statement_date",
            "data_type": "date",
            "granularity": "day"
        },
        materialized = 'incremental',
        partitions = partitions_to_replace,
        incremental_strategy = 'insert_overwrite'
) }}

with statements as (

    select *
    from {{ ref('fct_executed_statements') }}   
    {% if is_incremental() %}
      where date(statement_date) >= current_date - 3
    {% endif %}

),

jobs as (

    select *
    from {{ ref('dim_job') }}

),

table_refs as (

    select *
    from {{ ref('dim_job_table_view_references') }}
    where object_type = 'table'

),

users as (

    select *
    from {{ ref('dim_bigquery_users') }}

),

base as (

    select
        statement_date,
        table_refs.referenced_view_or_table,
        table_refs.layer_used,
        users.principal_email,
        users.user_type,
        statements.total_billed_bytes,
        statements.total_slot_ms,
        if(statements.error_message_key = {{ generate_surrogate_key(["'NONE'", "'NONE'"]) }}, false, true) as is_errored,
        jobs.dbt_execution_type
    from statements
    inner join table_refs
        on statements.job_key = table_refs.job_key
    left outer join jobs
        on statements.job_key = jobs.job_key
    inner join users
        on statements.user_key = users.user_key
),

deduped_base as(
    select 
        referenced_view_or_table,
        statement_date
    from base
    qualify row_number() over(partition by statement_date, referenced_view_or_table) = 1
),

min_event_date as (

    select min(statement_date) as min_date
    from base

),

calendar as (
    select date_day
    from {{ ref('dim_leaner_query_date') }}
    inner join min_event_date
               on date_day between min_date and current_date

),

egress_use_counts as (

    select
        referenced_view_or_table,
        calendar.date_day,
        layer_used,
        coalesce(if(layer_used != 'prod', count(principal_email) * 5, count(principal_email)),0) as count_queries_with_multiplier
    from base
    left outer join calendar
        on calendar.date_day = base.statement_date
            and date(base.statement_date) > calendar.date_day - 7
    where user_type = 'Service Account'
    {% if var('leaner_query_custom_egress_emails')| length > 0 -%}
        and principal_email in {{ build_custom_egress_email_list() }}
    {%- endif -%}
    group by 1,2,3

),

percent_rank_egress_use as (

    select
        referenced_view_or_table,
        count_queries_with_multiplier,
        date_day,
        percent_rank() over (order by count_queries_with_multiplier asc) as percent_rank_count_queries_with_multiplier
    from egress_use_counts

),

cost_to_query as (

    select
        base.referenced_view_or_table,
        calendar.date_day,
        coalesce(sum({{ calc_bq_cost('total_billed_bytes', 'total_slot_ms') }}),0) as sum_daily_query_cost,

    from base
    left outer join calendar
        on calendar.date_day = base.statement_date
            and date(base.statement_date) > calendar.date_day - 7
    where user_type = 'User'
    group by 1,2
    order by 1,2

),

median_cost_to_query as (

    select distinct
        referenced_view_or_table,
        date_day,
        sum_daily_query_cost,
        percentile_cont(sum_daily_query_cost, 0.5 ignore nulls) over (partition by referenced_view_or_table) as sum_daily_query_cost_median
    from cost_to_query

),

percent_rank_cost_to_query as (

    select
        referenced_view_or_table,
        date_day,
        sum_daily_query_cost_median,
        percent_rank() over (order by sum_daily_query_cost_median asc) as percent_rank_sum_daily_query_cost
    from median_cost_to_query

),

cost_to_build as (

    select
        base.referenced_view_or_table,
        calendar.date_day,
        coalesce(sum({{ calc_bq_cost('total_billed_bytes', 'total_slot_ms') }}),0) as sum_daily_build_cost
    from base
    left outer join calendar
        on calendar.date_day = base.statement_date
            and date(base.statement_date) > calendar.date_day - 7
    where user_type = 'Service Account'
        and dbt_execution_type is not null
    group by 1,2
    order by 1,2

),

median_cost_to_build as (

    select distinct
        referenced_view_or_table,
        date_day,
        sum_daily_build_cost,
        percentile_cont(sum_daily_build_cost, 0.5 ignore nulls) over (partition by referenced_view_or_table) as sum_daily_build_cost_median
    from cost_to_build

),

percent_rank_cost_to_build as (

    select
        referenced_view_or_table,
        date_day,
        sum_daily_build_cost_median,
        percent_rank() over (order by sum_daily_build_cost_median asc) as percent_rank_daily_build_cost
    from median_cost_to_build

),

daily_errors as (

    select
        base.referenced_view_or_table,
        calendar.date_day,
        coalesce(count(is_errored),0) as count_errors
    from base
    left outer join calendar
        on calendar.date_day = base.statement_date
            and date(base.statement_date) > calendar.date_day - 7
    where is_errored = true
    group by 1,2
    order by 1,2

),

median_daily_errors as (

    select distinct
        referenced_view_or_table,
        count_errors,
        date_day,
        percentile_cont(count_errors, 0.5 ignore nulls) over (partition by referenced_view_or_table) as median_count_daily_errors
    from daily_errors

),

percent_rank_daily_errors as (

    select
        referenced_view_or_table,
        median_count_daily_errors,
        date_day,
        percent_rank() over (order by median_count_daily_errors asc) as percent_rank_daily_errors
    from median_daily_errors

),

joined as (

    select
        deduped_base.referenced_view_or_table,
        deduped_base.statement_date,
        coalesce(egress.count_queries_with_multiplier, 0) as count_queries_with_multiplier,
        coalesce(egress.percent_rank_count_queries_with_multiplier, 0) as percent_rank_count_queries_with_multiplier,
        coalesce(cost_query.sum_daily_query_cost_median, 0) as sum_daily_query_cost_median,
        coalesce(cost_query.percent_rank_sum_daily_query_cost, 0) as percent_rank_sum_daily_query_cost,
        coalesce(cost_build.sum_daily_build_cost_median, 0) as sum_daily_build_cost_median,
        coalesce(cost_build.percent_rank_daily_build_cost, 0) as percent_rank_daily_build_cost,
        coalesce(errors.median_count_daily_errors, 0) as median_count_daily_errors,
        coalesce(errors.percent_rank_daily_errors, 0) as percent_rank_daily_errors,
        ((coalesce(egress.percent_rank_count_queries_with_multiplier, 0) * {{ var("leaner_query_weight_threat__service_account_egress") }})
            + (coalesce(cost_query.percent_rank_sum_daily_query_cost, 0) * {{ var("leaner_query_weight_threat__cost_to_query") }})
            + (coalesce(cost_build.percent_rank_daily_build_cost, 0) * {{ var("leaner_query_weight_threat__cost_to_build") }})
            + (coalesce(percent_rank_daily_errors, 0) * {{ var("leaner_query_weight_threat__daily_errors") }})) as threat_score
    from deduped_base
    left outer join percent_rank_egress_use as egress
        on deduped_base.referenced_view_or_table = egress.referenced_view_or_table
           and deduped_base.statement_date = egress.date_day
    left outer join percent_rank_cost_to_query as cost_query
        on deduped_base.referenced_view_or_table = cost_query.referenced_view_or_table
           and deduped_base.statement_date = cost_query.date_day
    left outer join percent_rank_cost_to_build as cost_build
        on deduped_base.referenced_view_or_table = cost_build.referenced_view_or_table
           and deduped_base.statement_date = cost_build.date_day
    left outer join percent_rank_daily_errors as errors
        on deduped_base.referenced_view_or_table = errors.referenced_view_or_table
           and deduped_base.statement_date = errors.date_day

),

final as (

    select *
    from joined
    where threat_score > 0

)

select * from final
