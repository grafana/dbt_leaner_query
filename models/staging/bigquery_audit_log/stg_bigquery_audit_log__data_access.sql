with source as (

    select *
    from {{ source('bigquery_audit_log', 'cloudaudit_googleapis_com_data_access') }}
),

renamed as (
    select
        insertId as insert_id,
        JSON_EXTRACT(protopayload_auditlog.metadataJson, "$.jobChange.job.jobConfig.labels.x-dashboard-uid") as dashboard_id,
        JSON_EXTRACT(protopayload_auditlog.metadataJson, "$.jobChange.job.jobConfig.labels.x-panel-id") as panel_id,
        COALESCE(
            CONCAT(
                SPLIT(
                    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, '$.jobChange.job.jobName'), "/"
                )[SAFE_OFFSET(1)],
                ":",
                SPLIT(
                    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, '$.jobChange.job.jobName'), "/"
                )[SAFE_OFFSET(3)]
            )
        ) as job_id,
        case
            when JSON_EXTRACT(protopayload_auditlog.metadataJson, '$.jobChange') is not null then 'JobChangeEvent'
        end as event_type,
        timestamp as event_timestamp,
        protopayload_auditlog.resourceName as resource_name,
        COALESCE(protopayload_auditlog.authenticationInfo.principalEmail, 'UNKNOWN') as principal_email,
        resource.labels.project_id as project_id,
        resource.labels.dataset_id as dataset_id,
        resource.labels.location as location,
        protopayload_auditlog.requestMetadata.callerIp as caller_ip_address,
        protopayload_auditlog.requestMetadata.callerSuppliedUserAgent as caller_supplied_user_agent,
        protopayload_auditlog.serviceName as service_name,
        protopayload_auditlog.methodName as method_name,
        -- protopayload_auditlog.metadataJson as protopayload_auditlog.metadataJson,
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, '$.jobChange.job.jobStatus.jobState') as job_state,
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, '$.jobChange.job.jobConfig.type') as config_type,
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, '$.jobChange.job.jobStatus.errorResult.code') as error_result_code,
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
            '$.jobChange.job.jobStatus.errorResult.message') as error_result_message,
        TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
                '$.jobChange.job.jobStats.createTime')) as create_time,
        TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
                '$.jobChange.job.jobStats.startTime')) as start_time,
        TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
                '$.jobChange.job.jobStats.endTime')) as end_time,
        CAST(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
                '$.jobChange.job.jobStats.totalSlotMs') as INT64) as total_slot_ms,
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
            '$.jobChange.job.jobStats.queryStats.totalProcessedBytes') as total_processed_bytes,
        CAST(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
                '$.jobChange.job.jobStats.queryStats.totalBilledBytes')
            as INT64) as total_billed_bytes,
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
            '$.jobChange.job.jobStats.queryStats.billingTier') as billing_tier,
        SPLIT(TRIM(TRIM(JSON_EXTRACT(protopayload_auditlog.metadataJson,
                    '$.jobChange.job.jobStats.queryStats.referencedTables'),
                    '["'), '"]'), '","') as referenced_tables,
        SPLIT(TRIM(TRIM(JSON_EXTRACT(protopayload_auditlog.metadataJson,
                        '$.jobChange.job.jobStats.queryStats.referencedViews'),
                    '["'), '"]'), '","') as referenced_views,
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
            '$.jobChange.job.jobStats.queryStats.outputRowCount') as output_row_count,
        CAST(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
                '$.jobChange.job.jobStats.queryStats.cacheHit') as BOOL) as cache_hit,
        CAST(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
                '$.jobChange.job.jobStats.loadStats.totalOutputBytes')
            as INT64) as total_output_bytes,
        JSON_EXTRACT(protopayload_auditlog.metadataJson, '$.jobChange.job.jobConfig.labels') as config_labels,
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
            '$.jobChange.job.jobConfig.queryConfig.query') as query_statement,
        CAST(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
                '$.jobChange.job.jobConfig.queryConfig.queryTruncated')
            as BOOL) as is_query_truncated,
        COALESCE(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, '$.jobChange.job.jobConfig.queryConfig.destinationTable'),
            JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, '$.jobChange.job.jobConfig.loadConfig.destinationTable')
        ) as destination_table,
        COALESCE(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, '$.jobChange.job.jobConfig.queryConfig.createDisposition'),
            JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, '$.jobChange.job.jobConfig.loadConfig.createDisposition'))
        as create_disposition,
        COALESCE(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, '$.jobChange.job.jobConfig.queryConfig.writeDisposition'),
            JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, '$.jobChange.job.jobConfig.loadConfig.writeDisposition'))
        as write_disposition,
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
            '$.jobChange.job.jobConfig.queryConfig.defaultDataset') as default_dataset,
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
            '$.jobChange.job.jobConfig.queryConfig.priority') as query_priority,
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
            '$.jobChange.job.jobConfig.queryConfig.statementType') as statement_type,
    from source
    where JSON_EXTRACT(protopayload_auditlog.metadataJson, '$.jobChange') is not null
)

select *
from renamed
