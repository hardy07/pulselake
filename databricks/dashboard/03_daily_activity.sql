-- Dataset: daily_activity. Date on X; listens or active_listeners on Y.
-- Each date has its own distinct listener count. Summing these is not an all-time unique count.
SELECT event_date, listens, active_listeners, listening_hours
FROM workspace.pulselake_gold.daily_metrics
ORDER BY event_date;
