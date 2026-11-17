-- Dataset: subscription_activity. Counts listening events by their recorded plan.
SELECT subscription, count(*) AS listens,
       round(sum(duration_seconds) / 3600.0, 2) AS listening_hours
FROM workspace.pulselake_gold.fact_listens
GROUP BY subscription
ORDER BY subscription;
