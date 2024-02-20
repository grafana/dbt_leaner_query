{% set partitions_to_replace = [
    'date(date_add(current_date, interval -3 day))',
    'date(date_add(current_date, interval -2 day))',
    'date(date_add(current_date, interval -1 day))',
    'date(current_date)'
] %}

{{ 
    config(
        cluster_by = ['table_name'],
        partition_by={
            "field": "event_date",
            "data_type": "date",
            "granularity": "day"
        },
        materialized = 'incremental',
        partitions = partitions_to_replace,
        incremental_strategy = 'insert_overwrite'
) }}

with source as(
    select *
    from {{ ref('stg_bigquery_audit_log__data_access') }}
    where principal_email not like '%.iam.gserviceaccount.com'
    {% if is_incremental() %}
        and date(event_timestamp) >= current_date - 3
    {% endif %}
    {% if target.name == var('leaner_query_dev_target_name') and var('leaner_query_enable_dev_limits') %}
        and date(event_timestamp) >= current_date - var('leaner_query_dev_limit_days')
    {% endif %}

),

tables as (
    select distinct
        job_id,
        referenced_table as table_name,
    from source
    cross join unnest(referenced_tables) as referenced_table
),

base_joined as (
    select distinct
        date(event_timestamp) as event_date,
        table_name,
        principal_email
    from source
    left outer join tables
        on source.job_id = tables.job_id
),

first_date as (
    select min(date(event_timestamp)) as min_date
    from source
),

users as (
    select distinct principal_email,
    from source
),

dim_date as (
    select date_day as event_date
    from {{ ref('dim_leaner_query_date') }} as dim_date
    cross join first_date
    where date_day between min_date and current_date
),

single_tables as (
    select distinct table_name,
    from tables
),

base_tables as (
    select distinct
        event_date,
        table_name,
        principal_email
    from dim_date
    cross join single_tables
    cross join users
),

active_users as (
    select
        bt.*,
        count(if(bj.principal_email is not null, bt.principal_email, null))
        over (partition by bt.table_name, bt.principal_email
            order by bt.event_date rows between 29 preceding and current row)
        as num_days_active
    from base_tables as bt
    left outer join base_joined as bj
        on bt.event_date = bj.event_date
            and bt.table_name = bj.table_name
            and bt.principal_email = bj.principal_email
),

user_frequency as (
    select
        event_date,
        principal_email,
        table_name,
        case
            when num_days_active >= 5 then "weekly"
            when num_days_active > 0 then "monthly"
            else "other"
        end as user_activity_status
    from active_users
),

final as (
    select
        event_date,
        table_name,
        sum(count(distinct if(user_activity_status = "weekly", principal_email, null)))
        over (order by event_date rows between 29 preceding and current row)
        as human_weekly_active_users_30_day_cnt,
        sum(count(distinct if(user_activity_status = "monthly", principal_email, null)))
        over (order by event_date rows between 29 preceding and current row)
        as human_monthly_active_users_30_day_cnt
    from user_frequency
    group by 1,2
)

select *
from final
