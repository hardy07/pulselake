# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------
from pyspark.sql import functions as F

run_id = str(uuid.uuid4())
envelope_schema = """
raw_event STRING, kafka_topic STRING, kafka_partition INT, kafka_offset BIGINT,
kafka_timestamp TIMESTAMP, source_stream_id STRING, source_record_id STRING
"""
source = (spark.readStream.format("cloudFiles")
          .option("cloudFiles.format", "parquet")
          .option("cloudFiles.schemaEvolutionMode", "none")
          .option("cloudFiles.allowOverwrites", "false")
          .option("maxFilesPerTrigger", 100)
          .schema(envelope_schema)
          .load(f"{VOLUME}/landing"))
bronze = (source.withColumn("source_file", F.col("_metadata.file_path"))
          .withColumn("ingested_at", F.current_timestamp())
          .withColumn("ingestion_run_id", F.lit(run_id)))
query = (bronze.writeStream.format("delta").outputMode("append")
         .option("checkpointLocation", f"{VOLUME}/checkpoints/bronze")
         .trigger(availableNow=True).toTable(f"{BRONZE}.events"))
query.awaitTermination()
count = spark.table(f"{BRONZE}.events").where(F.col("ingestion_run_id") == run_id).count()
record_audit(spark, "bronze", run_id, count, "AvailableNow completed")
print(f"Bronze loaded {count} records")

