{{ config(materialized='incremental', incremental_strategy='merge', unique_key='event_id') }}

select event_id, event_time, event_date, user_id, session_id, song_id, song_title,
       artist, duration_seconds, country, subscription, source_ingested_at
from {{ source('silver', 'events') }}
where event_type = 'listen'
{% if is_incremental() %}
  -- Use ingestion time, not event time: old events arriving today must still be processed.
  and source_ingested_at >= (
    select coalesce(max(source_ingested_at), timestamp '1970-01-01 00:00:00') from {{ this }}
  )
{% endif %}
