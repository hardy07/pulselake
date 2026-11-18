"""Load local connection settings without embedding workspace details in source."""
import json
import os
import re
import shutil
import sys
from pathlib import Path


def configure_environment(root, require_connection=False, require_dashboard=False):
    root = Path(root)
    settings_path = root / "runtime" / "settings.json"
    settings = json.loads(settings_path.read_text()) if settings_path.exists() else {}
    keys = {
        "host": "DATABRICKS_HOST", "warehouse_id": "PULSELAKE_WAREHOUSE_ID",
        "catalog": "PULSELAKE_CATALOG", "profile": "DATABRICKS_CONFIG_PROFILE",
        "dashboard_id": "PULSELAKE_DASHBOARD_ID",
    }
    for key, name in keys.items():
        if settings.get(key):
            os.environ.setdefault(name, settings[key])
    os.environ.setdefault("DATABRICKS_CONFIG_PROFILE", "pulselake")
    os.environ.setdefault("PULSELAKE_CATALOG", "workspace")
    catalog = os.environ["PULSELAKE_CATALOG"]
    if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", catalog):
        raise ValueError("PULSELAKE_CATALOG must be a simple SQL identifier")
    os.environ.setdefault("PULSELAKE_VOLUME", f"/Volumes/{catalog}/pulselake_bronze/ingest")
    host = os.environ.get("DATABRICKS_HOST", "")
    warehouse = os.environ.get("PULSELAKE_WAREHOUSE_ID", "")
    if require_connection and (not host or not warehouse):
        raise ValueError("Run scripts/configure.py with your workspace host and warehouse ID first.")
    if host:
        os.environ.setdefault("BUNDLE_VAR_workspace_host", host)
    if warehouse:
        os.environ.setdefault("DATABRICKS_HTTP_PATH", f"/sql/1.0/warehouses/{warehouse}")
        os.environ.setdefault("BUNDLE_VAR_warehouse_id", warehouse)
    os.environ.setdefault("BUNDLE_VAR_catalog", catalog)
    dashboard = os.environ.get("PULSELAKE_DASHBOARD_ID", "")
    if dashboard:
        os.environ.setdefault("BUNDLE_VAR_dashboard_id", dashboard)
    dashboard = os.environ.get("BUNDLE_VAR_dashboard_id", "")
    if require_dashboard and not dashboard:
        raise ValueError("Publish your dashboard, then run scripts/configure.py --dashboard-id YOUR_DASHBOARD_ID before deploying.")
    if dashboard and not re.fullmatch(r"[A-Za-z0-9_-]+", dashboard):
        raise ValueError("Dashboard ID must be an identifier, not a dashboard URL")
    local_cli = root / ".tools" / "databricks"
    os.environ["PATH"] = os.pathsep.join([str(Path(sys.executable).parent), str(local_cli), os.environ.get("PATH", "")])
    cli = shutil.which("databricks")
    if cli:
        os.environ["DATABRICKS_CLI_PATH"] = cli
    os.environ["DBT_SEND_ANONYMOUS_USAGE_STATS"] = "false"
    return cli
