"""Continuously read Kafka with Spark and publish recoverable local Parquet batches."""
import json
import os
import uuid
from pathlib import Path

from pulselake.batches import publish_batch, validate_manifest


def main():
    from pyspark.sql import SparkSession, functions as F
    root = Path(os.environ.get("PULSELAKE_ROOT", "/data"))
    checkpoint = root / "checkpoints" / "kafka"
    checkpoint.mkdir(parents=True, exist_ok=True)
    identity = checkpoint / "pulselake_identity.json"
    config = {"bootstrap": os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"),
              "topic": os.environ.get("KAFKA_TOPIC", "music.events")}
    if identity.exists():
        saved = json.loads(identity.read_text())
        if saved["source"] != config:
            raise RuntimeError("Kafka source changed: use a new data directory/checkpoint")
        stream_id = saved["stream_id"]
    else:
        stream_id = uuid.uuid4().hex
        identity.write_text(json.dumps({"source": config, "stream_id": stream_id}))
    spark = (SparkSession.builder.appName("pulselake-kafka-to-parquet")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "2").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    source = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", config["bootstrap"])
              .option("subscribe", config["topic"])
              .option("startingOffsets", "earliest")
              .option("failOnDataLoss", "true")
              .option("maxOffsetsPerTrigger", "10000").load())
    events = source.select(
        F.col("value").cast("string").alias("raw_event"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    ).withColumn("source_stream_id", F.lit(stream_id)).withColumn(
        "source_record_id", F.sha2(F.concat_ws(":", "source_stream_id", "kafka_topic",
                                             "kafka_partition", "kafka_offset"), 256))

    def write_batch(frame, batch_id):
        destination = root / "spool" / "ready" / f"{stream_id}-{batch_id:020d}"
        if destination.exists():
            # Spark may replay foreachBatch after a crash between publication and checkpoint commit.
            validate_manifest(destination / "manifest.json")
            return
        staging = root / "spool" / ".staging" / f"{stream_id}-{batch_id:020d}"
        frame.persist()
        try:
            count = frame.count()
            if not count:
                return
            frame.coalesce(2).write.mode("overwrite").parquet(str(staging))
            publish_batch(staging, root / "spool" / "ready", stream_id, batch_id, count)
            print(json.dumps({"batch_id": batch_id, "rows": count, "status": "ready"}), flush=True)
        finally:
            frame.unpersist()

    query = (events.writeStream.foreachBatch(write_batch)
             .option("checkpointLocation", str(checkpoint / "spark"))
             .trigger(processingTime=os.environ.get("SPARK_TRIGGER_INTERVAL", "120 seconds"))
             .start())
    query.awaitTermination()


if __name__ == "__main__":
    main()
