"""Check lakehouse counts through the authenticated CLI profile; never print credentials."""
import argparse
import json
import os
import time
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from project_config import configure_environment


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gold", action="store_true")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    configure_environment(root, require_connection=True)
    catalog = os.environ["PULSELAKE_CATALOG"]
    client = WorkspaceClient(config=Config(
        profile=os.environ["DATABRICKS_CONFIG_PROFILE"], http_timeout_seconds=30, retry_timeout_seconds=60,
    ))
    statement = f"""
      SELECT 'bronze' AS layer, count(*) AS rows FROM {catalog}.pulselake_bronze.events
      UNION ALL SELECT 'silver', count(*) FROM {catalog}.pulselake_silver.events
      UNION ALL SELECT 'silver_unique_ids', count(DISTINCT event_id) FROM {catalog}.pulselake_silver.events
      UNION ALL SELECT 'quarantine', count(*) FROM {catalog}.pulselake_silver.quarantine
    """
    if args.gold:
        statement += f"""
          UNION ALL SELECT 'fact_listens', count(*) FROM {catalog}.pulselake_gold.fact_listens
          UNION ALL SELECT 'dim_users', count(*) FROM {catalog}.pulselake_gold.dim_users
          UNION ALL SELECT 'dim_songs', count(*) FROM {catalog}.pulselake_gold.dim_songs
          UNION ALL SELECT 'daily_metrics', count(*) FROM {catalog}.pulselake_gold.daily_metrics
        """
    result = client.statement_execution.execute_statement(
        warehouse_id=os.environ["PULSELAKE_WAREHOUSE_ID"], statement=statement,
        wait_timeout="50s", row_limit=20,
    )
    deadline = time.monotonic() + 300
    while result.status.state.value in {"PENDING", "RUNNING"}:
        if time.monotonic() > deadline:
            raise TimeoutError(f"SQL still running: {result.statement_id}")
        time.sleep(5)
        result = client.statement_execution.get_statement(result.statement_id)
    if result.status.state.value != "SUCCEEDED":
        raise RuntimeError(f"SQL failed: {result.status.error}")
    counts = {row[0]: int(row[1]) for row in result.result.data_array}
    print(json.dumps(counts, indent=2))
    destination = Path(__file__).resolve().parents[1] / "runtime" / ("gold-counts.json" if args.gold else "silver-counts.json")
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(counts, indent=2), encoding="utf-8")
    if not counts["bronze"] or not counts["silver"]:
        raise RuntimeError("Bronze/Silver are empty; complete ingestion before dbt")
    if counts["silver"] != counts["silver_unique_ids"]:
        raise RuntimeError("Silver contains duplicate event IDs")


if __name__ == "__main__":
    main()
