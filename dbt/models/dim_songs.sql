select song_id, song_title, artist
from {{ source('silver', 'events') }}
where event_type = 'listen'
qualify row_number() over (partition by song_id order by event_time desc, event_id desc) = 1
