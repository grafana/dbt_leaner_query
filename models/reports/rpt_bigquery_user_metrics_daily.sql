{% set partitions_to_replace = [
    'date(date_add(current_date, interval -3 day))',
    'date(date_add(current_date, interval -2 day))',
    'date(date_add(current_date, interval -1 day))',
    'date(current_date)'
] %}

{{ 
    config(
        enable = var("leaner_query_enable_reports"),
        require_partition_filter = var("leaner_query_require_partition_by_reports"),
        cluster_by = ['username', 'user_type'],
        partition_by={
            "field": "report_date",
            "data_type": "date",
            "granularity": "day"
        },
) }}

with fct_executed_statements as (
    select *
    from {{ ref('fct_executed_statements') }}
),

min_event_date as(
    select min(statement_date) as min_date
    from fct_executed_statements
),

calendar as (
    select date_day
    from {{ ref('dim_leaner_query_date') }}
    inner join min_event_date on date_day between min_date and current_date
    where 1=1
    {% if is_incremental() %}
        and date_day in ({{ partitions_to_replace | join(',') }})
    {% endif %}
    {% if target.name in var('leaner_query_dev_target_name') and var('leaner_query_enable_dev_limits') %}
        and date_day >= current_date - {{ var('leaner_query_dev_limit_days') }}
    {% endif %}

),

dim_bq_users as(
    select *
    from {{ ref('dim_bigquery_users') }}
),

dim_job as(
    select *
    from {{ ref('dim_job') }}
),

dim_job_table_view_references as(
    select *
    from {{ ref('dim_job_table_view_references') }}
),

tables_used_per_job as (
    select
        fct_executed_statements.job_key,
        sum(case when dim_job_table_view_references.layer_used = 'prod' then 1 else 0 end) as prod_tables_used,
        sum(case when dim_job_table_view_references.layer_used = 'stage' then 1 else 0 end) as stage_tables_used,
        sum(case when dim_job_table_view_references.layer_used = 'raw' then 1 else 0 end) as raw_tables_used,
        coalesce(count(fct_executed_statements.job_key), 0) as total_tables_used
    from fct_executed_statements 
    inner join dim_job_table_view_references on fct_executed_statements.job_key = dim_job_table_view_references.job_key
    group by 1
),

aggregates as(
    select
        calendar.date_day as report_date,
        dim_bq_users.username,
        dim_bq_users.user_type,
        coalesce(count(fct_executed_statements.job_key), 0) as total_queries_run,
        coalesce(sum(case when fct_executed_statements.error_message_key !={{ generate_surrogate_key(["'NONE'", "'NONE'"]) }} then 1 else 0 end), 0) as total_errors,
        coalesce(sum(prod_tables_used), 0) as total_prod_tables_used,
        coalesce(sum(stage_tables_used), 0) as total_stage_tables_used,
        coalesce(sum(raw_tables_used), 0) as total_raw_tables_used,
        coalesce(sum(total_tables_used), 0) as total_tables_used,
        coalesce(sum({{ calc_bq_cost('total_billed_bytes', 'total_slot_ms') }}), 0) as total_estimated_cost_usd,
        coalesce(sum(total_slot_ms), 0) as total_time_ms
    from calendar
    cross join dim_bq_users
    left outer join fct_executed_statements on fct_executed_statements.user_key = dim_bq_users.user_key and fct_executed_statements.statement_date = calendar.date_day
    left outer join dim_job on fct_executed_statements.job_key = dim_job.job_key and lower(dim_job.statement_type) = 'select'
    left outer join tables_used_per_job on fct_executed_statements.job_key = tables_used_per_job.job_key
    group by 1,2, 3
),

final as(
    select
        *,
        round((coalesce(safe_divide(total_errors, total_queries_run), 0.0) * 100), 3) as error_rate,
        round((coalesce(safe_divide(total_prod_tables_used, total_tables_used), 0.0) * 100), 3) as prod_table_use_rate,
        round((coalesce(safe_divide(total_stage_tables_used, total_tables_used), 0.0) * 100), 3) as stage_table_use_rate,
        round((coalesce(safe_divide(total_raw_tables_used, total_tables_used), 0.0) * 100), 3) as raw_table_use_rate
    from aggregates
)

select *
from final
