{{ 
    config(
        unique_key = ['job_key', 'referenced_view_or_table', 'layer_used', 'object_type'],
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
        "view" as object_type
    from source
    cross join unnest(referenced_views) as referenced_view
),

tables as (
    select distinct
        job_id,
        referenced_table as referenced_view_or_table,
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
),

object_split as (
    select *,
        split(referenced_view_or_table, '/')[safe_offset(1)] as project_id,
        split(referenced_view_or_table, '/')[safe_offset(3)] as dataset_id,
        split(referenced_view_or_table, '/')[safe_offset(5)] as table_or_view_id
    from unioned
),

add_layer as(
    select *,
        concat(dim_job_table_view_references.project_id, '.', unique_tables.dataset_id, '.', unique_tables.table_or_view_id) as  qualified_table_name,
        {{ determine_data_layer('dataset_id') }} as layer_used
    from object_split
)

select distinct
{{ generate_surrogate_key([
        'job_id'
    ]) }} as job_key,
    * except(job_id)
from add_layer
