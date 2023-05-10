{{ 
    config(
        cluster_by = ['job_key', 'label_key'],
        materialized = 'incremental'
    )
}}

with source as (

    select *
    from {{ ref('stg_bigquery_audit_log__data_access') }}
    {% if is_incremental() %}
      where date(event_timestamp) >= current_date - 3
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
