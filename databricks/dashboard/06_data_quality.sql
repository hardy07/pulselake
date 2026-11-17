-- Dataset: data_quality. Table or bar chart of quarantined delivery records.
SELECT rejection_reason, count(*) AS rejected_records
FROM workspace.pulselake_silver.quarantine
GROUP BY rejection_reason
ORDER BY rejected_records DESC;
