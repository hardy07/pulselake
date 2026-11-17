# Databricks notebook source
import re
import uuid
from datetime import datetime, timezone

dbutils.widgets.text("catalog", "workspace")
CATALOG = dbutils.widgets.get("catalog")
if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", CATALOG):
    raise ValueError("Catalog must be a simple SQL identifier")
BRONZE = f"{CATALOG}.pulselake_bronze"
SILVER = f"{CATALOG}.pulselake_silver"
GOLD = f"{CATALOG}.pulselake_gold"
OPS = f"{CATALOG}.pulselake_ops"
VOLUME = f"/Volumes/{CATALOG}/pulselake_bronze/ingest"
spark.conf.set("spark.sql.session.timeZone", "UTC")


def record_audit(session, stage, run_id, row_count, detail):
    rows = [(run_id, stage, datetime.now(timezone.utc), int(row_count), detail)]
    session.createDataFrame(rows, "run_id string, stage string, recorded_at timestamp, row_count long, detail string").write.mode("append").saveAsTable(f"{OPS}.pipeline_runs")
