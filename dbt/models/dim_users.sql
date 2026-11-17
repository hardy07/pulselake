select user_id, country, subscription, event_time as last_event_time
from {{ source('silver', 'events') }}
qualify row_number() over (partition by user_id order by event_time desc, event_id desc) = 1
