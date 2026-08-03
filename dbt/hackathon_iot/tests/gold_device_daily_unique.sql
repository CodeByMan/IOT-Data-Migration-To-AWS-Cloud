select
    device_id,
    event_date,
    count(*) as duplicate_count
from {{ ref('gold_device_daily') }}
group by
    device_id,
    event_date
having count(*) > 1