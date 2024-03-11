{{ 
    config(
        unique_key = ['job_key', 'label_key'],
        cluster_by = ['job_key', 'label_key'],
        materialized = 'incremental'
    )
}}

with source as (

    select *
    from {{ ref('stg_bigquery_audit_log__data_access') }}
    where 1=1
    {% if is_incremental() %}
        and date(event_timestamp) >= current_date - 3
    {% endif %}
    {% if target.name in var('leaner_query_dev_target_name') and var('leaner_query_enable_dev_limits') %}
        and date(event_timestamp) >= current_date - {{ var('leaner_query_dev_limit_days') }}
    {% endif %}

),

split_labels as (

    select
        job_id,
        split(regexp_replace(config_labels, r'[{}"]', ""), ",") as split_config
    from source

),

unnested as (

    select
        job_id,
        sconfig
    from split_labels
    cross join unnest(split_config) as sconfig

),

final as (

    select
        job_id,
        split(sconfig, ":")[offset(0)] as label_key,
        split(sconfig, ":")[offset(1)] as label_value
    from unnested

)

select distinct
{{ 
    generate_surrogate_key ([
        'job_id'
    ]) }} as job_key,
    label_key,
    label_value
from final
