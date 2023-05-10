{{ 
    config(
        cluster_by = ['job_key', 'referenced_view_or_table'],
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

views as (
    select distinct
        job_id,
        referenced_view as referenced_view_or_table,
        {{ determine_data_layer('referenced_view') }} as layer_used,
        "view" as object_type
    from source
    cross join unnest(referenced_views) as referenced_view
),

tables as (
    select distinct
        job_id,
        referenced_table as referenced_view_or_table,
        {{ determine_data_layer('referenced_table') }} as layer_used,
        "table" as object_type
    from source
    cross join unnest(referenced_tables) as referenced_table
),

unioned as (
    select *
    from views

    union all

    select *
    from tables
)

select distinct
{{ generate_surrogate_key([
        'job_id'
    ]) }} as job_key,
    * except(job_id)
from unioned
