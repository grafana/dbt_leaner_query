{# error messages and code, fill with "NONE" if no error message #}
{{ 
    config(
        unique_key = 'error_message_key',
        cluster_by = 'error_message_key',
        materialized='incremental'

) }}


with data_access as (

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

error_messages as (

    select distinct
        coalesce(error_result_code, 'NONE') as error_result_code,
        coalesce(error_result_message, 'NONE') as error_result_message
    from data_access

),

final as (

    select
{{ generate_surrogate_key([
            'error_result_code'
            , 'error_result_message'
        ]) }} as error_message_key,
        *
    from error_messages

)

select *
from final
