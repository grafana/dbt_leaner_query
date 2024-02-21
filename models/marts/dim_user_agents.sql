{{ 
    config(
        unique_key = ['user_agent_key', 'client_type'],
        cluster_by = ['user_agent_key', 'client_type'],
        materialized = 'incremental'
    )
}}

with source as(
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

final as(
    select distinct
        caller_supplied_user_agent,
        principal_email,
        case
            {% for custom_client in var('leaner_query_custom_clients') %}
{{ build_client_type(custom_client) }}
            {% endfor %}

            when json_extract_scalar(config_labels, '$.sheets_trigger') = 'user'
                then 'Connected Sheet - User Initiated'
            when json_extract_scalar(config_labels, '$.sheets_trigger') = 'schedule'
                then 'Connected Sheet - Scheduled'
            when json_extract_scalar(config_labels, '$.data_source_id') = 'scheduled_query'
                then 'Scheduled Query'
            when caller_supplied_user_agent like 'Mozilla%' then 'Web console'
            when json_extract_scalar(config_labels, '$.dbt_invocation_id') is not null
                or caller_supplied_user_agent like 'dbt%' then 'dbt run'

            when caller_supplied_user_agent like 'gl-python%' then 'Python Client'
            when caller_supplied_user_agent like 'Fivetran%' then 'Fivetran'
            when caller_supplied_user_agent like 'Hightouch%' then 'Hightouch'
            when caller_supplied_user_agent like 'gcloud-golang-bigquery%'
                and principal_email like 'rudderstack%'
                then 'Rudderstack'
            when caller_supplied_user_agent like 'gcloud-golang%'
                or caller_supplied_user_agent like 'google-api-go%'
                then 'Golang Client'
            when caller_supplied_user_agent like 'gcloud-node%' then 'Node Client'
            when caller_supplied_user_agent like 'SimbaJDBCDriver%' then 'Java Client'
            when caller_supplied_user_agent like 'google-cloud-sdk%' 
                then 'Google Cloud SDK'

            else coalesce(caller_supplied_user_agent, 'Unknown')
        end as client_type
    from source
)

select
{{ generate_surrogate_key(['caller_supplied_user_agent', 'principal_email']) }} as user_agent_key,
    *
from final
