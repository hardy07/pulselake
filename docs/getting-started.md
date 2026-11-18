# Getting started with PulseLake

## Requirements

- Python 3.11 and Git.
- Docker Desktop using Linux containers; the Spark image targets Linux AMD64.
- Databricks Free Edition or a workspace with Unity Catalog, serverless jobs, and
  a serverless/pro SQL warehouse.
- The [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install) on PATH.

The examples use PowerShell from the repository root. On macOS/Linux, use
`.venv/bin/python` in place of `.venv/Scripts/python.exe`.

## Setup

### 1. Install Python dependencies

Clone or download this repository into a folder named `pulselake`, then:

```powershell
cd D:\projects\pulselake
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt -r requirements-dbt.txt
```

A virtual environment must be created locally; it is not included in the repository.

### 2. Configure your workspace

Replace the example host and warehouse ID with your own values. Find the warehouse
ID under **SQL Warehouses > your warehouse > Connection details**.

```powershell
.\.venv\Scripts\python.exe scripts\configure.py --host https://YOUR-WORKSPACE.cloud.databricks.com --warehouse-id YOUR_WAREHOUSE_ID
databricks auth login --host https://YOUR-WORKSPACE.cloud.databricks.com --profile pulselake
```

The configuration command writes ignored `runtime/settings.json` and creates
ignored `dbt/profiles.yml` from the example. It does not request a password or token.
The CLI stores authentication using its normal credential mechanism. Local dbt may
open a separate OAuth browser login. Python runners load the local settings and
allow environment variables to override them. `.env` is only used by Docker Compose.

The default catalog is `workspace`. Use `--catalog YOUR_CATALOG` during configuration
if required. Replace `workspace` in dashboard SQL when using another catalog.

### 3. Initialize Databricks storage

Import these files together into one workspace folder as Python notebooks:

```text
databricks/_common.py
databricks/00_setup.py
databricks/01_bronze.py
databricks/02_silver.py
```

Run **00_setup** once with the same catalog selected above. This creates the
schemas, empty ingestion tables, audit table, and volume. For the default catalog,
the landing directory is:

```text
/Volumes/workspace/pulselake_bronze/ingest/landing
```

### 4. Build local services

Open Docker Desktop, wait for the engine, then run:

```powershell
docker compose config --quiet
docker compose up -d --build kafka kafka-init spark
docker compose --profile generate build producer
```

Kafka initialization should exit successfully; Kafka and Spark should remain
running. First startup downloads images and the matching Spark Kafka connector.

## Run an end-to-end test

### Terminal 1: automatic upload watcher

```powershell
.\.venv\Scripts\python.exe scripts\run_uploader.py --watch
```

Leave it open. It checks for completed local batches every 15 seconds.
`uploaded_files` counts new transfers; `reconciled_files` counts verified existing
remote files; `already_uploaded_files` counts files recorded in the delivery ledger.

### Terminal 2: generate events

```powershell
docker compose run --rm producer --count 200 --rate 5
```

Allow roughly 2-3 minutes for Spark to finish a batch; startup can take longer.
Confirm an upload before starting cloud processing. For continuous generation:

```powershell
docker compose run --rm producer --count 0 --rate 5
```

Stop continuous generation with Ctrl+C. Keep the PC awake and Docker and the
watcher running while sending new events.

### First deployment: prepare the published dashboard

The dashboard task needs an existing published dashboard. If this is a new
deployment, run **01_bronze**, then **02_silver**, once after the first upload, and
build the initial Gold tables:

```powershell
.\.venv\Scripts\python.exe scripts\run_dbt.py
```

Create the dashboard using [the dashboard guide](dashboard.md), point its
datasets at this deployment's `pulselake_gold` tables, and publish it. If a suitable
published dashboard already exists, skip this one-time preparation.

Copy its dashboard ID from the URL, for example the identifier after
`/sql/dashboardsv3/`, excluding query parameters. Save the ID without changing your
existing connection settings:

```powershell
.\.venv\Scripts\python.exe scripts\configure.py --dashboard-id YOUR_DASHBOARD_ID
```

This value stays in ignored `runtime/settings.json`. You can alternatively set
`PULSELAKE_DASHBOARD_ID` or, when using the CLI directly, `BUNDLE_VAR_dashboard_id`.
The job identity needs access to the dashboard and its data. Selecting a dashboard
ID does not rewrite its dataset queries or publish draft changes.

### Deploy and run the cloud job

```powershell
.\.venv\Scripts\python.exe scripts\deploy_pipeline.py
```

This validates the bundle, uploads notebooks and dbt code, creates/updates the job,
and submits a run. In **Jobs & Pipelines > PulseLake Pipeline > Runs**, verify:

```text
setup -> bronze -> silver -> gold -> refresh_dashboard
```

The Gold task runs `dbt build`. The dashboard refresh runs only after Gold succeeds.
Normal operation does not require manually running
individual notebooks or running dbt locally. Data arriving after a task collects
its input can be processed in the next run.

The schedule starts **paused** for the initial test. After success, change
`schedule.pause_status` to `UNPAUSED` in `databricks.yml`, then redeploy. The schedule
runs hourly in Asia/Calcutta. Bundle-managed jobs should be changed in source;
UI-only edits can be overwritten on redeployment.

For optional local dbt development, after Bronze and Silver contain data:

```powershell
.\.venv\Scripts\python.exe scripts\run_dbt.py
```

