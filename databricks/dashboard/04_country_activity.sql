-- Dataset: country_activity. Country is the value recorded on the listening event.
SELECT country, count(*) AS listens, count(DISTINCT user_id) AS active_listeners
FROM workspace.pulselake_gold.fact_listens
GROUP BY country
ORDER BY listens DESC;
