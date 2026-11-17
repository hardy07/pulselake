# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------
for schema in [BRONZE, SILVER, GOLD, OPS]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {BRONZE}.ingest")
for path in ["landing", "checkpoints/bronze", "checkpoints/silver"]:
    dbutils.fs.mkdirs(f"{VOLUME}/{path}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE}.events (
  raw_event STRING, kafka_topic STRING, kafka_partition INT, kafka_offset BIGINT,
  kafka_timestamp TIMESTAMP, source_stream_id STRING, source_record_id STRING,
  source_file STRING, ingested_at TIMESTAMP, ingestion_run_id STRING
) USING DELTA
""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER}.events (
  event_id STRING, event_time TIMESTAMP, event_date DATE, event_type STRING,
  user_id STRING, session_id STRING, song_id STRING, song_title STRING,
  artist STRING, duration_seconds INT, country STRING, subscription STRING,
  schema_version INT, source_record_id STRING, kafka_timestamp TIMESTAMP,
  source_ingested_at TIMESTAMP, processed_at TIMESTAMP
) USING DELTA
""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER}.quarantine (
  source_record_id STRING, raw_event STRING, rejection_reason STRING,
  source_ingested_at TIMESTAMP, quarantined_at TIMESTAMP
) USING DELTA
""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {OPS}.pipeline_runs (
  run_id STRING, stage STRING, recorded_at TIMESTAMP, row_count BIGINT, detail STRING
) USING DELTA
""")
print(f"Ready. Upload completed Parquet files to {VOLUME}/landing")

