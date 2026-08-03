{{ config(
    materialized='view',
    schema='CLEAN',
    tags=['silver', 'iot']
) }}

with raw_cdc as (

    select
        record_metadata,
        "SCHEMA" as kafka_schema,
        payload
    from {{ source('raw', 'IOT_EVENTS') }}

),

extracted as (

    select
        record_metadata,
        kafka_schema,
        payload,

        payload:op::string as operation,

        coalesce(
            payload:after:device_id::string,
            payload:before:device_id::string
        ) as device_id,

        try_to_double(
            coalesce(
                payload:after:latitude::string,
                payload:before:latitude::string
            )
        ) as latitude,

        try_to_double(
            coalesce(
                payload:after:longitude::string,
                payload:before:longitude::string
            )
        ) as longitude,

        try_to_number(
            coalesce(
                payload:after:aqi::string,
                payload:before:aqi::string
            )
        ) as aqi,

        coalesce(
            payload:after:event_time::string,
            payload:before:event_time::string,
            payload:after:timestamp::string,
            payload:before:timestamp::string
         ) as source_timestamp_text,

        try_to_number(payload:ts_ms::string) as cdc_timestamp_ms

    from raw_cdc

),

typed as (

    select
        record_metadata,
        kafka_schema,
        operation,
        device_id,
        latitude,
        longitude,
        aqi,

            coalesce(
            	try_to_timestamp_ntz(source_timestamp_text),
            dateadd(
                	millisecond,
                	try_to_number(source_timestamp_text),
                	to_timestamp_ntz('1970-01-01 00:00:00')
            ),
            dateadd(
                	millisecond,
                	cdc_timestamp_ms,
                	to_timestamp_ntz('1970-01-01 00:00:00')
            )
        ) as event_timestamp

    from extracted

),

classified as (

    select
        record_metadata,
        kafka_schema,
        operation,
        case operation
            when 'c' then 'CREATE'
            when 'u' then 'UPDATE'
            when 'd' then 'DELETE'
            when 'r' then 'SNAPSHOT'
            else 'UNKNOWN'
        end as operation_name,

        device_id,
        latitude,
        longitude,
        aqi,
        event_timestamp,
        to_date(event_timestamp) as event_date,

        case
            when aqi is null then 'UNKNOWN'
            when aqi <= 50 then 'GOOD'
            when aqi <= 100 then 'MODERATE'
            when aqi <= 150 then 'UNHEALTHY_FOR_SENSITIVE_GROUPS'
            when aqi <= 200 then 'UNHEALTHY'
            when aqi <= 300 then 'VERY_UNHEALTHY'
            else 'HAZARDOUS'
        end as severity_tag

    from typed

)

select *
from classified