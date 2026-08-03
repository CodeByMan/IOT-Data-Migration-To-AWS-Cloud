select *
from {{ ref('gold_device_daily') }}
where total_cdc_events <= 0