select event_id from {{ ref('fact_listens') }}
where duration_seconds is null or duration_seconds < 0
