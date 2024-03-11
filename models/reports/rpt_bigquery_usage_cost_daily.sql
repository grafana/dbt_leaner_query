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
        cluster_by = ['client_type']
    )
}}

with fct_executed_statements as (
    select *
    from {{ ref('fct_executed_statements') }}
),

min_event_date as (
    select min(statement_date) as min_date
    from fct_executed_statements
),

dim_user_agents as (
    select *
    from {{ ref('dim_user_agents') }}
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

dim_job as(
    select *
    from {{ ref('dim_job') }}
),

aggregates as (
    select
        calendar.date_day as report_date, -- calendar
        dim_user_agents.client_type, -- dim_user_agents
        coalesce(count(fct_executed_statements.job_key), 0) as total_queries_run,  -- cnt(job_key) from fct_executed_statements
        coalesce(sum({{ calc_bq_cost('total_billed_bytes', 'total_slot_ms') }}), 0) as total_estimated_cost_usd,
        coalesce(sum(if(dim_job.dbt_execution_type = "DBT_RUN",{{ calc_bq_cost('total_billed_bytes', 'total_slot_ms') }},0)), 0) as total_estimated_dbt_run_build_cost_usd,
        coalesce(sum(if(dim_job.dbt_execution_type = "DBT_TEST",{{ calc_bq_cost('total_billed_bytes', 'total_slot_ms') }},0)), 0) as total_estimated_dbt_test_build_cost_usd,
        coalesce(sum(fct_executed_statements.total_slot_ms), 0) as total_time_ms,
        coalesce(sum(if(lower(dim_job.statement_type) = "select",{{ calc_bq_cost('total_billed_bytes', 'total_slot_ms') }},0)), 0) as total_estimated_query_cost_usd,
        coalesce(sum(if(lower(dim_job.statement_type) = "select", fct_executed_statements.total_slot_ms, 0)), 0) as total_query_time_ms
    from calendar
    cross join dim_user_agents
    left outer join fct_executed_statements
        on dim_user_agents.user_agent_key = fct_executed_statements.user_agent_key
            and calendar.date_day = fct_executed_statements.statement_date
    left outer join dim_job
        on fct_executed_statements.job_key = dim_job.job_key
    group by 1, 2
)

select *
from aggregates
