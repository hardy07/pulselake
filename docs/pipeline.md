# PulseLake pipeline

The bundle in `databricks.yml` defines
`setup -> bronze -> silver -> gold -> refresh_dashboard`.
Each task waits for its predecessor to succeed; one run is allowed at a time.
Bronze and Silver use persistent checkpoints and `AvailableNow` runs. Gold runs
`dbt build` on the configured warehouse.

Configure local connection settings using `scripts/configure.py`, log in with
CLI profile `pulselake`, and save your published dashboard's ID with
`scripts/configure.py --dashboard-id YOUR_DASHBOARD_ID`, then run:

```powershell
.\.venv\Scripts\python.exe scripts\deploy_pipeline.py
```

Use `--validate-only` for validation without deployment. Inspect the Runs tab
until every task succeeds. Submission alone is not a successful run.

The schedule starts paused. After testing, change `schedule.pause_status` in the
bundle to `UNPAUSED` and redeploy. The schedule is hourly in Asia/Calcutta. Match
any UI edits in the local bundle before redeploying.

## Dashboard refresh task

The November 18 run history in the README confirms successful scheduled execution
of all five tasks in the existing demo, including dashboard refresh after Gold.
The source bundle now includes that fifth task with `run_if: ALL_SUCCESS` and a
dependency on `gold`. It uses the configured SQL warehouse and dashboard ID.

Publish the dashboard first and ensure its queries read this deployment's Gold
tables. For a new deployment, seed Bronze/Silver with the notebooks and build Gold
once with the local dbt runner before creating the dashboard. The [setup guide](getting-started.md) describes
this initial setup. The bundle references a dashboard; it does not create or
publish one, rewrite its queries, or grant permissions.

Save the ID in local settings:

```powershell
.\.venv\Scripts\python.exe scripts\configure.py --dashboard-id YOUR_DASHBOARD_ID
```

Existing host, warehouse, and catalog settings are retained. Configuration can
also come from `PULSELAKE_DASHBOARD_ID` or `BUNDLE_VAR_dashboard_id`. Deployment
fails early with instructions if the ID is missing. No email subscription is set.
Validate and deploy, then inspect all five task results.

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
