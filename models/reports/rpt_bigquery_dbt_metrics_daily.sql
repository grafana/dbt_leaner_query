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
        partition_by = {
        "field": "report_date",
        "data_type": "date",
        "granularity": "day"
        },
        cluster_by = ['dbt_model_name']
    )
}}

with fct_executed_statements as (
    select *
    from {{ ref('fct_executed_statements') }}
    {% if is_incremental() %}
      where date(statement_date) >= current_date - 3
    {% endif %}
),

min_event_date as (
    select min(statement_date) as min_date
    from fct_executed_statements
),

calendar as (
    select date_day
    from {{ ref('dim_leaner_query_date') }}
    inner join min_event_date on date_day between min_date and current_date
),

dim_job as(
    select *
    from {{ ref('dim_job') }}
),

dim_job_table_view_references as (
    select *
    from {{ ref('dim_job_table_view_references') }}
),

dim_job_labels as(
    select *
    from {{ ref('dim_job_labels') }}
),

aggregate_by_dbt_invocation as (
    select
        calendar.date_day as report_date, -- calendar
        dim_job.dbt_model_name,
        dim_job_labels.label_value as dbt_invocation_id,
        coalesce(sum(if((dim_job.dbt_execution_type = "DBT_RUN"), 1,0)), 0) as dbt_builds,
        coalesce(sum(if((dim_job.dbt_execution_type = "DBT_TEST"),1,0)), 0) as dbt_tests,
        coalesce(sum(if((dim_job.dbt_execution_type = "DBT_SNAPSHOT"),1,0)), 0) as dbt_snapshots,

        coalesce(sum(if((dim_job.dbt_execution_type = "DBT_RUN"),{{ calc_bq_cost('total_billed_bytes') }},0)), 0) as estimated_dbt_run_cost_usd,
        coalesce(sum(if((dim_job.dbt_execution_type = "DBT_TEST"),{{ calc_bq_cost('total_billed_bytes') }},0)), 0) as estimated_dbt_test_cost_usd,
        coalesce(sum(if((dim_job.dbt_execution_type = "DBT_SNAPSHOT"),{{ calc_bq_cost('total_billed_bytes') }},0)), 0) as estimated_dbt_snapshot_cost_usd,

        coalesce(sum(if((dim_job.dbt_execution_type = "DBT_RUN"),(timestamp_diff(fct_executed_statements.end_time, fct_executed_statements.start_time, millisecond)),0)), 0) as estimated_dbt_build_time_ms,
        coalesce(sum(if((dim_job.dbt_execution_type = "DBT_TEST"), (timestamp_diff(fct_executed_statements.end_time, fct_executed_statements.start_time, millisecond)),0)), 0) as estimated_dbt_test_time_ms,
        coalesce(sum(if((dim_job.dbt_execution_type = "DBT_SNAPSHOT"),(timestamp_diff(fct_executed_statements.end_time, fct_executed_statements.start_time, millisecond)),0)), 0) as estimated_dbt_snapshot_time_ms,
    from calendar
    inner join fct_executed_statements
        on calendar.date_day = fct_executed_statements.statement_date
    inner join dim_job_labels
        on fct_executed_statements.job_key = dim_job_labels.job_key
    inner join dim_job
        on fct_executed_statements.job_key = dim_job.job_key
    where dim_job.dbt_execution_type is not null
        and label_key = 'dbt_invocation_id'
    group by 1,2,3
),

aggregates as(
    select
        report_date,
        dbt_model_name,
        coalesce(sum(if(dbt_builds > 0, 1,0)),0) as total_dbt_builds,
        coalesce(sum(if(dbt_tests > 0, 1,0)),0) as total_dbt_tests,
        coalesce(sum(if(dbt_snapshots > 0, 1,0)),0) as total_dbt_snapshots,

        coalesce(sum(estimated_dbt_run_cost_usd), 0) as total_estimated_dbt_run_cost_usd,
        coalesce(sum(estimated_dbt_test_cost_usd), 0) as total_estimated_dbt_test_cost_usd,
        coalesce(sum(estimated_dbt_snapshot_cost_usd), 0) as total_estimated_dbt_snapshot_cost_usd,

        coalesce(sum(estimated_dbt_build_time_ms), 0) as total_estimated_dbt_build_time_ms,
        coalesce(sum(estimated_dbt_test_time_ms), 0) as total_estimated_dbt_test_time_ms,
        coalesce(sum(estimated_dbt_snapshot_time_ms), 0) as total_estimated_dbt_snapshot_time_ms
    from aggregate_by_dbt_invocation
    group by 1,2
),

final as(
    select
        *,
        round((coalesce(safe_divide(total_estimated_dbt_run_cost_usd, total_dbt_builds), 0.0)), 3) as average_build_cost,
        round((coalesce(safe_divide(total_estimated_dbt_test_cost_usd, total_dbt_tests), 0.0)), 3) as average_test_cost,
        round((coalesce(safe_divide(total_estimated_dbt_snapshot_cost_usd, total_dbt_snapshots), 0.0)), 3) as average_snapshot_cost,

        round((coalesce(safe_divide(total_estimated_dbt_build_time_ms, total_dbt_builds), 0.0)), 3) as average_build_time_ms,
        round((coalesce(safe_divide(total_estimated_dbt_test_time_ms, total_dbt_tests), 0.0)), 3) as average_test_time_ms,
        round((coalesce(safe_divide(total_estimated_dbt_snapshot_time_ms, total_dbt_snapshots), 0.0)), 3) as average_snapshot_time_ms

    from aggregates
)

select *
from final
