-- join to dim_job_labels via job_key for labels
{{ 
    config(
        unique_key = ['job_key', 'caller_ip_address'],
        cluster_by = ['job_key', 'statement_type'],
        materialized='incremental'
) }}

with data_access as (

    select *
    from {{ ref('stg_bigquery_audit_log__data_access') }}
    {% if is_incremental() %}
      where date(event_timestamp) >= current_date - 3
    {% endif %}

),

extract_json as(
    select
        *,
        replace(replace(regexp_extract(query_statement, r'^(\/\* \{+?[\w\W]+?\} \*\/)'), '/', ''), '*', '') dbt_info
    from data_access
),

dbt_statements as(
    select
        *,
        json_extract_scalar(dbt_info, '$.dbt_version') as dbt_version,
        json_extract_scalar(dbt_info, '$.profile_name') as dbt_profile_name,
        json_extract_scalar(dbt_info, '$.target_name') as dbt_target_name,
        json_extract_scalar(dbt_info, '$.node_id') as dbt_model_name
    from extract_json
    where length(extract_json.dbt_info) > 0
),

add_dbt_context as(
    select
        *,
        case
            when dbt_model_name like 'model.%' then 'DBT_RUN'
            when dbt_model_name like 'snapshot.%' then 'DBT_SNAPSHOT'
            when dbt_model_name like 'test.%' then 'DBT_TEST'
        end as dbt_execution_type
    from dbt_statements
),

adjust_modelname as(
    select
        *,
        concat(split(dbt_model_name, '.')[safe_offset(1)], '.',split(dbt_model_name, '.')[safe_offset(2)]) as dbt_adjusted_model_name
    from add_dbt_context
),

final as (

    select distinct
        event_type,
        job_id,
        resource_name,
        caller_ip_address,
        method_name,
        create_disposition,
        query_statement,
        statement_type,
        query_priority,
        dbt_info,
        dbt_version,
        dbt_profile_name,
        dbt_target_name,
        dbt_execution_type,
        dbt_adjusted_model_name as dbt_model_name
    from adjust_modelname
)

select
{{ generate_surrogate_key ([
        'job_id'
    ]) }} as job_key,
    *
from final
