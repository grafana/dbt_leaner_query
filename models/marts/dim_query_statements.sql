{{ 
    config(
        cluster_by = ['job_key', 'query_statement'],
        materialized = 'incremental'
) }}

with data_access as (

    select *
    from {{ ref('stg_bigquery_audit_log__data_access') }}
    where 1=1
    {% if is_incremental() %}
        and date(event_timestamp) >= current_date - 3
    {% endif %}
    {% if target.name == var('leaner_query_dev_target_name') and var('leaner_query_enable_dev_limits') %}
        and date(event_timestamp) >= current_date - {{ var('leaner_query_dev_limit_days') }}
    {% endif %}

),

extract_json as(
    select
        *,
        replace(replace(regexp_extract(query_statement, r'^(\/\* \{+?[\w\W]+?\} \*\/)'), '/', ''), '*', '') dbt_info
    from data_access
),

final as (
    select distinct
        query_statement,
        job_id,
        dbt_info,
    from extract_json
)

select 
{{ generate_surrogate_key ([
    'job_id',
])}} as job_key,
    *
from final