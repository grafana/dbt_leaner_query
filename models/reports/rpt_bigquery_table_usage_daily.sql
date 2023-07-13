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
        materialized='incremental',
        cluster_by = ['layer', 'client_type'],
        partition_by={
            "field": "report_date",
            "data_type": "date",
            "granularity": "day"
        },
        partitions = partitions_to_replace,
        incremental_strategy = 'insert_overwrite'
) }}

with fct_executed_statements as (
    select *
    from {{ ref('fct_executed_statements') }}
),

min_event_date as (
    select min(statement_date) as min_date
    from fct_executed_statements
),

dim_job as (
    select *
    from {{ ref('dim_job') }}
),

dim_bq_users as (
    select *
    from {{ ref('dim_bigquery_users') }}
),

dim_user_agents as (
    select *
    from {{ ref('dim_user_agents') }}
),

calendar as (
    select date_day
    from {{ ref('dim_leaner_query_date') }}
    inner join min_event_date on date_day between min_date and current_date

    {% if is_incremental() %}
    where date_day >= {{ partitions_to_replace[0] }}
    {% endif %}
),

dim_job_table_view_references as(
    select *
    from {{ ref('dim_job_table_view_references') }}
),

unique_tables as (
    select 
        distinct referenced_view_or_table as table_name
    from dim_job_table_view_references
),

threat_calculation as (
    select *
    from {{ ref('int_threat_calculation') }}
),

importance_calculation as (
    select *
    from {{ ref('int_importance_calculations') }}
),

aggregates as (
    select
        calendar.date_day as report_date, -- calendar
        unique_tables.table_name, -- dim_job_table
        dim_job_table_view_references.project_id,
        dim_job_table_view_references.dataset_id,
        dim_job_table_view_references.table_or_view_id,
        dim_job_table_view_references.qualified_table_name,
        dim_job_table_view_references.layer_used as layer, -- dim_job_table
        dim_user_agents.client_type, -- dim_user_agents
        coalesce(count(fct_executed_statements.job_key), 0) as total_queries_run,  -- cnt(job_key) from fct_executed_statements
        count(distinct if(dim_job.dbt_execution_type = "DBT_RUN", dim_job.dbt_model_name, null)) as dbt_models_run,
        count(distinct if(dim_job.dbt_execution_type = "DBT_TEST", dim_job.dbt_model_name, null)) as dbt_tests_run,
        count(distinct if(dim_bq_users.user_type = "User", fct_executed_statements.user_key, null)) as total_human_users,
        count(distinct if(dim_bq_users.user_type = "Service Account", fct_executed_statements.user_key, null)) as total_service_accounts,
        coalesce(sum(case when fct_executed_statements.error_message_key !={{ generate_surrogate_key(["'NONE'", "'NONE'"]) }} then 1 else 0 end), 0) as total_errors,
    from calendar
    cross join unique_tables
    left outer join dim_job_table_view_references
        on unique_tables.table_name = dim_job_table_view_references.referenced_view_or_table
    left outer join fct_executed_statements
        on dim_job_table_view_references.job_key = fct_executed_statements.job_key
            and calendar.date_day = fct_executed_statements.statement_date
    left outer join dim_user_agents
        on fct_executed_statements.user_agent_key = dim_user_agents.user_agent_key
    left outer join dim_bq_users
        on fct_executed_statements.user_key = dim_bq_users.user_key
    left outer join dim_job
        on fct_executed_statements.job_key = dim_job.job_key
    group by 1, 2, 3, 4, 5, 6, 7, 8
),

final as (
    select distinct
        aggregates.*,
        threat_calculation.threat_score,
        importance_calculation.importance_score,
        (importance_calculation.importance_score * {{ var('leaner_query_priority_importance_level_weight') }})
        + (threat_calculation.threat_score * {{ var('leaner_query_priority_threat_level_weight') }})
        as priority_score
    from aggregates
    left outer join threat_calculation
        on aggregates.table_name = threat_calculation.referenced_view_or_table
            and aggregates.report_date = threat_calculation.statement_date
    left outer join importance_calculation
        on aggregates.table_name = importance_calculation.table_name
            and aggregates.report_date = importance_calculation.score_date
)

select *
from final
