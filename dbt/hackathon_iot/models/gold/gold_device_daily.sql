{{ config(
    materialized='table',
    schema='ANALYTICS',
    tags=['gold', 'iot']
) }}

select
    device_id,
    event_date,

    count(*) as total_cdc_events,
    count_if(operation = 'c') as create_events,
    count_if(operation = 'u') as update_events,
    count_if(operation = 'd') as delete_events,
    count_if(operation = 'r') as snapshot_events,

    min(event_timestamp) as first_event_timestamp,
    max(event_timestamp) as last_event_timestamp,

    min(aqi) as minimum_aqi,
    max(aqi) as maximum_aqi,
    avg(aqi) as average_aqi,

    avg(latitude) as average_latitude,
    avg(longitude) as average_longitude,

    max_by(severity_tag, aqi) as highest_severity_tag

from {{ ref('silver_iot_events') }}

where
    device_id is not null
    and event_date is not null

group by
    device_id,
    event_date