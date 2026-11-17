-- Dataset: summary. Create separate counters from this single-row result.
SELECT
  count(*) AS total_listens,
  count(DISTINCT user_id) AS active_listeners,
  count(DISTINCT song_id) AS songs_played,
  round(coalesce(sum(duration_seconds), 0) / 3600.0, 2) AS listening_hours,
  max(event_time) AS latest_event_time
FROM workspace.pulselake_gold.fact_listens;
