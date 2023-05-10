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
            "field": "score_date",
            "data_type": "date",
            "granularity": "day"
        },
        materialized = 'incremental',
        partitions = partitions_to_replace,
        incremental_strategy = 'insert_overwrite'
) }}

with fct_executed_statements as (
    select *
    from {{ ref('fct_executed_statements') }}
    {% if is_incremental() %}
      where date(statement_date) >= current_date - 3
    {% endif %}

),

min_event_date as(
    select min(statement_date) as min_date
    from fct_executed_statements
),

calendar as (
    select date_day
    from {{ ref('dim_leaner_query_date') }}
    inner join min_event_date on date_day between min_date and current_date
),

dim_table_view_references as(
    select *
    from {{ ref('dim_job_table_view_references') }}
),

dim_bq_users as(
    select *
    from {{ ref('dim_bigquery_users') }}
),

dim_job_labels as(
    select *
    from {{ ref('dim_job_labels') }}
),

dim_user_agents as(
    select *
    from {{ ref('dim_user_agents') }}
),

int_table_active_users as(
    select *
    from {{ ref('int_table_active_users') }}
),

daily_service_account_queries as(
    select
        dim_table_view_references.referenced_view_or_table as ref_table,
        calendar.date_day,
        coalesce(count(fct_executed_statements.job_key), 0) as query_count,
    from fct_executed_statements
    left outer join calendar on calendar.date_day = fct_executed_statements.statement_date and date(fct_executed_statements.statement_date) > calendar.date_day - 7
    inner join dim_table_view_references on fct_executed_statements.job_key = dim_table_view_references.job_key and object_type = 'table'
    inner join dim_bq_users on fct_executed_statements.user_key = dim_bq_users.user_key
    where dim_bq_users.user_type = 'Service Account'
    group by 1,2
),

daily_dbt_queries as(
    select
        dim_table_view_references.referenced_view_or_table as ref_table,
        calendar.date_day,
        coalesce(count(dim_job_labels.label_value),0) as query_count,
    from fct_executed_statements
    left outer join calendar on calendar.date_day = fct_executed_statements.statement_date and date(fct_executed_statements.statement_date) > calendar.date_day - 7
    inner join dim_table_view_references on fct_executed_statements.job_key = dim_table_view_references.job_key and object_type = 'table'
    inner join dim_bq_users on fct_executed_statements.user_key = dim_bq_users.user_key
    left outer join dim_job_labels as dim_job_labels on fct_executed_statements.job_key = dim_job_labels.job_key and dim_job_labels.label_key = 'dbt_invocation_id'
    where dim_bq_users.user_type = 'Service Account'
    group by 1,2
),

daily_egress_use as(
    select
        dim_table_view_references.referenced_view_or_table as ref_table,
        calendar.date_day,
        sum(
            case
                when dim_user_agents.client_type in ({{ "\'" + var('leaner_query_importance_query_score_1')|join("\', \'") + "\'"}}) then 1
                when dim_user_agents.client_type in ({{ "\'" + var('leaner_query_importance_query_score_2')|join("\', \'") + "\'"}}) then 2
                when dim_user_agents.client_type in ({{ "\'" + var('leaner_query_importance_query_score_3')|join("\', \'") + "\'"}}) then 3
                when dim_user_agents.client_type in ({{ "\'" + var('leaner_query_importance_query_score_4')|join("\', \'") + "\'"}}) then 4
                else 0
            end
        ) as query_score
    from fct_executed_statements
    left outer join calendar on calendar.date_day = fct_executed_statements.statement_date and date(fct_executed_statements.statement_date) > calendar.date_day - 7
    inner join dim_table_view_references on fct_executed_statements.job_key = dim_table_view_references.job_key and object_type = 'table'
    inner join dim_bq_users on fct_executed_statements.user_key = dim_bq_users.user_key
    inner join dim_user_agents on fct_executed_statements.user_agent_key = dim_user_agents.user_agent_key
    where dim_bq_users.user_type = 'Service Account'
    group by 1,2

),

median_service_account_queries as(
    select
        *,
        percentile_cont(query_count, 0.5 ignore nulls) over(partition by ref_table) as median_query_count
    from daily_service_account_queries
),

median_dbt_queries as(
    select
        *,
        percentile_cont(query_count, 0.5 ignore nulls) over(partition by ref_table) as median_query_count
    from daily_dbt_queries
),

median_egress_use as(
    select
        *,
        percentile_cont(query_score, 0.5 ignore nulls) over(partition by ref_table) as median_query_count
    from daily_egress_use
),

median_active_users as(
    select
        *,
        percentile_cont(human_monthly_active_users_30_day_cnt, 0.5 ignore nulls) over(partition by table_name) as median_active_user_count
    from int_table_active_users
),

percent_rank_service_account_queries as(
    select
        ref_table,
        date_day,
        median_query_count,
        percent_rank() over (order by median_query_count asc) as perc_rnk
    from median_service_account_queries
    group by 1,2,3
),

percent_rank_dbt_queries as(
    select
        ref_table,
        date_day,
        median_query_count,
        percent_rank() over (order by median_query_count asc) as perc_rnk
    from median_dbt_queries
    group by 1,2,3
),

percent_rank_egress_use as(
    select
        ref_table,
        date_day,
        median_query_count,
        percent_rank() over (order by median_query_count asc) as perc_rnk
    from median_egress_use
    group by 1,2,3
),

percent_rank_active_users as(
    select
        table_name as ref_table,
        event_date as date_day,
        median_active_user_count,
        percent_rank() over (order by median_active_user_count asc) as perc_rnk
    from median_active_users
    group by 1,2,3
),

final as(
    select
        percent_rank_service_account_queries.ref_table as table_name,
        percent_rank_service_account_queries.date_day as score_date,
        percent_rank_service_account_queries.median_query_count as service_acct_median_queries,
        percent_rank_service_account_queries.perc_rnk as service_acct_percent_rank,
        percent_rank_dbt_queries.median_query_count as dbt_median_queries,
        percent_rank_dbt_queries.perc_rnk as dbt_percent_rank,
        percent_rank_egress_use.median_query_count as egress_median_queries,
        percent_rank_egress_use.perc_rnk as egress_percent_rank,
        percent_rank_active_users.median_active_user_count as active_user_median,
        percent_rank_active_users.perc_rnk as active_user_percent_rank,
        ((percent_rank_service_account_queries.perc_rnk * {{ var('leaner_query_weight_importance__service_account_queries') }}) +
          (percent_rank_dbt_queries.perc_rnk * {{ var('leaner_query_weight_importance__dbt_queries') }}) +
          (percent_rank_egress_use.perc_rnk * {{ var('leaner_query_weight_importance__egress_use') }}) +
          (percent_rank_active_users.perc_rnk * {{ var('leaner_query_weight_importance__user_breadth') }})) as importance_score
    from percent_rank_service_account_queries
    left outer join percent_rank_dbt_queries on percent_rank_service_account_queries.ref_table = percent_rank_dbt_queries.ref_table
        and percent_rank_service_account_queries.date_day = percent_rank_dbt_queries.date_day
    left outer join percent_rank_egress_use on percent_rank_service_account_queries.ref_table = percent_rank_egress_use.ref_table
        and percent_rank_service_account_queries.date_day = percent_rank_egress_use.date_day
    left outer join percent_rank_active_users on percent_rank_service_account_queries.ref_table = percent_rank_active_users.ref_table
        and percent_rank_service_account_queries.date_day = percent_rank_active_users.date_day

)

select *
from final
