{% set partitions_to_replace = [
    'timestamp(timestamp_add(current_timestamp, interval -3 day))',
    'timestamp(timestamp_add(current_timestamp, interval -2 day))',
    'timestamp(timestamp_add(current_timestamp, interval -1 day))',
    'timestamp(current_timestamp)'
] %}

{{
    config(
        enable = var("leaner_query_enable_reports"),
        require_partition_filter = var("leaner_query_require_partition_by_reports"),
        materialized='incremental',
        cluster_by = ['username', 'user_type'],
        partition_by={
            "field": "start_time",
            "data_type": "timestamp",
            "granularity": "day"
        },
        partitions = partitions_to_replace,
        incremental_strategy = 'insert_overwrite'
    )
}}

with statements as (
    select *
    from {{ ref('fct_executed_statements') }}
    where 1=1
    {% if is_incremental() %}
        and date(statement_date) >= current_date - 3
    {% endif %}
    {% if target.name == var('leaner_query_dev_target_name') and var('leaner_query_enable_dev_limits') %}
        and date(statement_date) >= current_date - {{ var('leaner_query_dev_limit_days') }}
    {% endif %}
),

users as (
    select *
    from {{ ref('dim_bigquery_users') }}
),

jobs as (
    select *
    from {{ ref('dim_job') }}
),

final as (
    select
        statements.start_time,
        users.username,
        users.user_type,
        {{calc_bq_cost('total_billed_bytes', 'total_slot_ms')}} as query_cost,
        jobs.query_statement
    from statements
    inner join users
        on users.user_key = statements.user_key
    inner join jobs
        on jobs.job_key = statements.job_key
    where date(statements.start_time) >= current_date -2
)

select *
from final
