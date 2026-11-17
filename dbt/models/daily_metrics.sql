-- Rebuild the small daily aggregate so late arrivals repair historical dates.
select event_date, count(*) as listens, count(distinct user_id) as active_listeners,
       round(sum(duration_seconds) / 3600.0, 2) as listening_hours
from {{ ref('fact_listens') }}
group by event_date
