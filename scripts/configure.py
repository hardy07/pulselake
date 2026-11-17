"""Save public connection settings locally; no password or token is needed."""
import argparse
import json
import re
from pathlib import Path
from urllib.parse import urlparse


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", required=True, help="https://your-workspace.cloud.databricks.com")
    parser.add_argument("--warehouse-id", required=True)
    parser.add_argument("--catalog", default="workspace")
    args = parser.parse_args()
    host = urlparse(args.host.rstrip("/"))
    if host.scheme != "https" or not host.hostname or host.username or host.password or host.path not in ("", "/") or host.query or host.fragment:
        parser.error("Host must be a bare HTTPS workspace URL without credentials or query parameters")
    if not re.fullmatch(r"[A-Za-z0-9_-]+", args.warehouse_id):
        parser.error("Invalid warehouse ID")
    if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", args.catalog):
        parser.error("Catalog must be a simple SQL identifier")
    root = Path(__file__).resolve().parents[1]
    runtime = root / "runtime"
    runtime.mkdir(exist_ok=True)
    settings = {"host": args.host.rstrip("/"), "warehouse_id": args.warehouse_id,
                "catalog": args.catalog, "profile": "pulselake"}
    (runtime / "settings.json").write_text(json.dumps(settings, indent=2)+"\n")
    profile = root / "dbt" / "profiles.yml"
    if not profile.exists():
        profile.write_text((root / "dbt" / "profiles.yml.example").read_text())
    print("Saved runtime/settings.json and ensured the local dbt profile exists. Both are ignored by Git.")
    print(f"Next: databricks auth login --host {settings['host']} --profile pulselake")


if __name__ == "__main__":
    main()
