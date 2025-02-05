with unioned as (
    {%- for audit_log_source in var('leaner_query_sources') %}
    select
        insertId as insert_id,
        json_query(protopayload_auditlog.metadataJson, "$.jobChange.job.jobConfig.labels.x-dashboard-uid") as grafana_dashboard_id,
        json_query(protopayload_auditlog.metadataJson, "$.jobChange.job.jobConfig.labels.x-panel-id") as grafana_panel_id,
        coalesce(
            concat(
                split(
                    json_value(protopayload_auditlog.metadataJson, '$.jobChange.job.jobName'), "/"
                )[safe_offset(1)],
                ":",
                split(
                    json_value(protopayload_auditlog.metadataJson, '$.jobChange.job.jobName'), "/"
                )[safe_offset(3)]
            )
        ) as job_id,
        case
            when json_query(protopayload_auditlog.metadataJson, '$.jobChange') is not null then 'JobChangeEvent'
        end as event_type,
        timestamp as event_timestamp,
        protopayload_auditlog.resourceName as resource_name,
        coalesce(protopayload_auditlog.authenticationInfo.principalEmail, 'UNKNOWN') as principal_email,
        resource.labels.project_id as project_id,
        resource.labels.dataset_id as dataset_id,
        resource.labels.location as location,
        protopayload_auditlog.requestMetadata.callerIp as caller_ip_address,
        protopayload_auditlog.requestMetadata.callerSuppliedUserAgent as caller_supplied_user_agent,
        protopayload_auditlog.serviceName as service_name,
        protopayload_auditlog.methodName as method_name,
        -- -- protopayload_auditlog.metadataJson as protopayload_auditlog.metadataJson,
        json_value(protopayload_auditlog.metadataJson, '$.jobChange.job.jobStatus.jobState') as job_state,
        json_value(protopayload_auditlog.metadataJson, '$.jobChange.job.jobConfig.type') as config_type,
        json_value(protopayload_auditlog.metadataJson, '$.jobChange.job.jobStatus.errorResult.code') as error_result_code,
        json_value(protopayload_auditlog.metadataJson,
            '$.jobChange.job.jobStatus.errorResult.message') as error_result_message,
        timestamp(json_value(protopayload_auditlog.metadataJson,
                '$.jobChange.job.jobStats.createTime')) as create_time,
        timestamp(json_value(protopayload_auditlog.metadataJson,
                '$.jobChange.job.jobStats.startTime')) as start_time,
        timestamp(json_value(protopayload_auditlog.metadataJson,
                '$.jobChange.job.jobStats.endTime')) as end_time,
        safe_cast(json_value(protopayload_auditlog.metadataJson,
                '$.jobChange.job.jobStats.totalSlotMs') as int64) as total_slot_ms,
        json_value(protopayload_auditlog.metadataJson,
            '$.jobChange.job.jobStats.queryStats.totalProcessedBytes') as total_processed_bytes,
        safe_cast(json_value(protopayload_auditlog.metadataJson,
                '$.jobChange.job.jobStats.queryStats.totalBilledBytes')
            as int64) as total_billed_bytes,
        json_value(protopayload_auditlog.metadataJson,
            '$.jobChange.job.jobStats.queryStats.billingTier') as billing_tier,
        split(trim(trim(json_query(protopayload_auditlog.metadataJson,
                    '$.jobChange.job.jobStats.queryStats.referencedTables'),
                    '["'), '"]'), '","') as referenced_tables,
        split(trim(trim(json_query(protopayload_auditlog.metadataJson,
                        '$.jobChange.job.jobStats.queryStats.referencedViews'),
                    '["'), '"]'), '","') as referenced_views,
        json_value(protopayload_auditlog.metadataJson,
            '$.jobChange.job.jobStats.queryStats.outputRowCount') as output_row_count,
        safe_cast(json_value(protopayload_auditlog.metadataJson,
                '$.jobChange.job.jobStats.queryStats.cacheHit') as bool) as cache_hit,
        safe_cast(json_value(protopayload_auditlog.metadataJson,
                '$.jobChange.job.jobStats.loadStats.totalOutputBytes')
            as int64) as total_output_bytes,
        json_query(protopayload_auditlog.metadataJson, '$.jobChange.job.jobConfig.labels') as config_labels,
        json_value(protopayload_auditlog.metadataJson,
            '$.jobChange.job.jobConfig.queryConfig.query') as query_statement,
        safe_cast(json_value(protopayload_auditlog.metadataJson,
                '$.jobChange.job.jobConfig.queryConfig.queryTruncated')
            as bool) as is_query_truncated,
        coalesce(json_value(protopayload_auditlog.metadataJson, '$.jobChange.job.jobConfig.queryConfig.destinationTable'),
            json_value(protopayload_auditlog.metadataJson, '$.jobChange.job.jobConfig.loadConfig.destinationTable')
        ) as destination_table,
        coalesce(json_value(protopayload_auditlog.metadataJson, '$.jobChange.job.jobConfig.queryConfig.createDisposition'),
            json_value(protopayload_auditlog.metadataJson, '$.jobChange.job.jobConfig.loadConfig.createDisposition'))
        as create_disposition,
        coalesce(json_value(protopayload_auditlog.metadataJson, '$.jobChange.job.jobConfig.queryConfig.writeDisposition'),
            json_value(protopayload_auditlog.metadataJson, '$.jobChange.job.jobConfig.loadConfig.writeDisposition'))
        as write_disposition,
        json_value(protopayload_auditlog.metadataJson,
            '$.jobChange.job.jobConfig.queryConfig.defaultDataset') as default_dataset,
        json_value(protopayload_auditlog.metadataJson,
            '$.jobChange.job.jobConfig.queryConfig.priority') as query_priority,
        json_value(protopayload_auditlog.metadataJson,
            '$.jobChange.job.jobConfig.queryConfig.statementType') as statement_type,
    from {{ audit_log_source }}
    where json_query(protopayload_auditlog.metadataJson, '$.jobChange') is not null
    {% if not loop.last -%}
    union all
    {%- endif -%}
    {%- endfor %}
)

select *
from unioned