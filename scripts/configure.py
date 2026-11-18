"""Save public connection settings locally; no password or token is needed."""
import argparse
import json
import re
from pathlib import Path
from urllib.parse import urlparse


def main(argv=None, root=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", help="https://your-workspace.cloud.databricks.com; required on first setup")
    parser.add_argument("--warehouse-id", help="Required on first setup")
    parser.add_argument("--catalog", help="Defaults to workspace on first setup")
    parser.add_argument("--dashboard-id", help="Published dashboard ID, not its URL; required for deployment")
    args = parser.parse_args(argv)
    root = Path(root) if root is not None else Path(__file__).resolve().parents[1]
    runtime = root / "runtime"
    settings_path = runtime / "settings.json"
    settings = json.loads(settings_path.read_text()) if settings_path.exists() else {}
    for key in ("host", "warehouse_id", "catalog", "dashboard_id"):
        value = getattr(args, key)
        if value is not None:
            settings[key] = value
    settings.setdefault("catalog", "workspace")
    settings.setdefault("profile", "pulselake")
    if not settings.get("host") or not settings.get("warehouse_id"):
        parser.error("Provide --host and --warehouse-id on first setup")
    host = urlparse(settings["host"].rstrip("/"))
    if host.scheme != "https" or not host.hostname or host.username or host.password or host.path not in ("", "/") or host.query or host.fragment:
        parser.error("Host must be a bare HTTPS workspace URL without credentials or query parameters")
    if not re.fullmatch(r"[A-Za-z0-9_-]+", settings["warehouse_id"]):
        parser.error("Invalid warehouse ID")
    if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", settings["catalog"]):
        parser.error("Catalog must be a simple SQL identifier")
    if "dashboard_id" in settings and not re.fullmatch(r"[A-Za-z0-9_-]+", settings["dashboard_id"]):
        parser.error("Dashboard ID must be an identifier, not a dashboard URL")
    settings["host"] = settings["host"].rstrip("/")
    runtime.mkdir(exist_ok=True)
    settings_path.write_text(json.dumps(settings, indent=2)+"\n")
    profile = root / "dbt" / "profiles.yml"
    if not profile.exists():
        profile.write_text((root / "dbt" / "profiles.yml.example").read_text())
    print("Saved runtime/settings.json and ensured the local dbt profile exists. Both are ignored by Git.")
    print(f"Next: databricks auth login --host {settings['host']} --profile pulselake")


if __name__ == "__main__":
    main()
