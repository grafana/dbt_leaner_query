{{ 
    config(
        unique_key = 'user_key',
        cluster_by = ['user_type', 'user_key'],
        materialized = 'incremental',
    )
}}

-- if this is too big, we may want to make this incremental
with source as (
    select *
    from {{ ref('stg_bigquery_audit_log__data_access') }}
    where 1=1
    {% if is_incremental() %}
        and date(event_timestamp) >= current_date - 3
    {% endif %}
    {% if target.name == var('leaner_query_dev_target_name') and var('leaner_query_enable_dev_limits') %}
        and date(event_timestamp) >= current_date - var('leaner_query_dev_limit_days')
    {% endif %}

),

user_emails as (
    select distinct principal_email,
    from source
)

select distinct
    principal_email,
    regexp_extract(principal_email, '([^@]+)') as username,
    case
        when principal_email like '%.iam.gserviceaccount.com' then 'Service Account'
        else 'User'
    end as user_type,
{{ generate_surrogate_key([
        'principal_email'
    ]) }} as user_key
from user_emails
