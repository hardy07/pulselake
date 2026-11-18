# Adopt the PulseLake source release

This directory is a clean, renamed source release for GitHub. The previous working
folder, local runtime, Docker resources, and deployed cloud job were retained.
No live data, dashboard definition, or remote job was renamed automatically.

## Fresh deployment (recommended)

1. Create a new Python environment and configure your workspace as in the README.
2. Stop the previous local generator and services before starting this Compose
   project: both use host ports 29092 and 4040. Stop only; retain their data.
3. Use a fresh `runtime/` and the new Compose volumes. Do not pair existing Kafka
   checkpoints with a new empty broker or manually change stream identity files.
4. Run the new setup notebook to create `pulselake_*` schemas and the landing volume.
5. Start uploads, generate events, run Bronze/Silver once, and build Gold with the
   local dbt runner to prepare the dashboard's source tables.
6. Create/repoint and publish dashboard datasets using the SQL in this release.
   Existing dashboards still reference their previously configured tables. Save
   the published dashboard ID with `scripts/configure.py --dashboard-id ID`, then
   deploy the five-task bundle and verify counts and dashboard refresh.
7. Validate the new pipeline before enabling its schedule. Pause the previous
   cloud job when retiring it to avoid running two demos unnecessarily.

Changing a bundle name and resource key creates a separate deployment identity.
It does not rename the previously deployed job. The new schedule starts paused.

## Optional replay of retained batches

Completed Parquet batches can be replayed into the new volume without modifying
the original local spool. After the new setup notebook succeeds, use a new ledger:

```powershell
.\.venv\Scripts\python.exe scripts\run_uploader.py --spool D:\path\to\previous\runtime\spool --ledger runtime\replay-uploads.sqlite3
```

Substitute the actual previous spool path. The destination is the newly configured
PulseLake volume. Run the new cloud job after uploads complete. Do not copy the
old Spark checkpoint or broker volume as part of this replay. Keep original files
until results have been verified.

A full in-place migration of populated tables and checkpoint paths is a separate
operation and has not been performed by this source rename.
