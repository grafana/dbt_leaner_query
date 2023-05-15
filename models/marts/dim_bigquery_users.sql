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
    {% if is_incremental() %}
      where date(event_timestamp) >= current_date - 3
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
