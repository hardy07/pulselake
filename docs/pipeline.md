# PulseLake pipeline

The bundle in `databricks.yml` defines `setup -> bronze -> silver -> gold`.
Each task waits for its predecessor to succeed; one run is allowed at a time.
Bronze and Silver use persistent checkpoints and `AvailableNow` runs. Gold runs
`dbt build` on the configured warehouse.

Configure local connection settings using `scripts/configure.py`, log in with
CLI profile `pulselake`, then run:

```powershell
.\.venv\Scripts\python.exe scripts\deploy_pipeline.py
```

Use `--validate-only` for validation without deployment. Inspect the Runs tab
until every task succeeds. Submission alone is not a successful run.

The schedule starts paused. After testing, change `schedule.pause_status` in the
bundle to `UNPAUSED` and redeploy. The schedule is hourly in Asia/Calcutta. Match
any UI edits in the local bundle before redeploying.

## Optional dashboard task

Publish the dashboard first. Add a task of type Dashboard, name it
`refresh_dashboard`, select the dashboard and SQL warehouse, and set its dependency
to `gold` with the All succeeded condition. Test the five-task run. Add the same
task to the bundle before redeployment; it is not included by default.

View the published dashboard when checking refresh results. Cache refresh and
refreshing an already-open browser tab are separate behaviors.
Reference: https://docs.databricks.com/aws/en/jobs/tasks/dashboard

## Troubleshooting

| Symptom | Check |
|---|---|
| Docker Linux engine pipe missing | Open Docker Desktop and check that `docker version` includes Server. |
| No uploads | Check producer delivery, Spark logs, and completed spool manifests. |
| Upload authentication fails | Log in with profile `pulselake` in your Windows user session. |
| Files uploaded but tables unchanged | Check job start time and notebook output; run after upload completion. |
| Gold increased but dashboard stayed unchanged | Run the summary dataset, refresh dashboard results, and check filters and table names. |
| Schedule/task settings reverted | Match UI settings in the bundle before redeploying. |

Do not run Bronze or Silver notebooks manually at the same time as the job.
They share checkpoints. Keep the local watcher and Docker running to collect new
events; the cloud job can still process previously uploaded files if the PC stops.
