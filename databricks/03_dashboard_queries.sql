-- Change workspace if you chose another catalog. Paste each query into a dashboard dataset.
-- Daily active listeners (distinct users with a listen event, not all website activity).
SELECT event_date, active_listeners, listens, listening_hours
FROM workspace.pulselake_gold.daily_metrics
ORDER BY event_date;

-- Top songs across the entire loaded dataset.
SELECT song_id, song_title, artist, count(*) AS listens,
       round(sum(duration_seconds) / 3600.0, 2) AS listening_hours
FROM workspace.pulselake_gold.fact_listens
GROUP BY song_id, song_title, artist
ORDER BY listens DESC
LIMIT 10;

-- Latest processing observations.
SELECT stage, max(recorded_at) AS last_completed_at
FROM workspace.pulselake_ops.pipeline_runs
GROUP BY stage;

-- Data quality issues.
SELECT rejection_reason, count(*) AS records
FROM workspace.pulselake_silver.quarantine
GROUP BY rejection_reason;
