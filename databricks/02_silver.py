# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------
# Each microbatch may be retried. Both MERGEs are idempotent; no watermark drops late events.
silver_schema = SILVER
ops_schema = OPS
run_id = str(uuid.uuid4())


def process_batch(frame, batch_id):
    session = frame.sparkSession
    frame.createOrReplaceTempView("incoming_bronze")
    session.sql("""
      CREATE OR REPLACE TEMP VIEW parsed_events AS
      SELECT *, from_json(raw_event,
        'schema_version INT, event_id STRING, event_time STRING, event_type STRING,
         user_id STRING, session_id STRING, song_id STRING, song_title STRING,
         artist STRING, duration_seconds INT, country STRING, subscription STRING,
         _corrupt_record STRING') AS e
      FROM incoming_bronze
    """)
    session.sql("""
      CREATE OR REPLACE TEMP VIEW checked_events AS
      SELECT *, try_cast(e.event_time AS TIMESTAMP) AS parsed_event_time,
        CASE
          WHEN e IS NULL OR e._corrupt_record IS NOT NULL THEN 'malformed_json_or_types'
          WHEN e.schema_version IS NULL OR e.schema_version <> 1 THEN 'unsupported_schema_version'
          WHEN e.event_id IS NULL OR trim(e.event_id) = '' THEN 'missing_event_id'
          WHEN e.user_id IS NULL OR trim(e.user_id) = '' THEN 'missing_user_id'
          WHEN try_cast(e.event_time AS TIMESTAMP) IS NULL THEN 'invalid_event_time'
          WHEN e.event_type IS NULL OR e.event_type NOT IN ('listen','page_view','login') THEN 'invalid_event_type'
          WHEN e.subscription IS NULL OR e.subscription NOT IN ('free','premium') THEN 'invalid_subscription'
          WHEN e.duration_seconds IS NULL OR e.duration_seconds < 0 THEN 'invalid_duration'
          WHEN e.event_type = 'listen' AND (e.song_id IS NULL OR trim(e.song_id) = '') THEN 'missing_song_id'
          ELSE NULL
        END AS rejection_reason
      FROM parsed_events
    """)
    session.sql(f"""
      MERGE INTO {silver_schema}.quarantine t USING (
        SELECT source_record_id, raw_event, rejection_reason,
               ingested_at AS source_ingested_at, current_timestamp() AS quarantined_at
        FROM checked_events WHERE rejection_reason IS NOT NULL
        QUALIFY row_number() OVER (PARTITION BY source_record_id ORDER BY ingested_at, raw_event) = 1
      ) s ON t.source_record_id = s.source_record_id
      WHEN NOT MATCHED THEN INSERT *
    """)
    session.sql(f"""
      MERGE INTO {silver_schema}.events t USING (
        SELECT e.event_id, parsed_event_time AS event_time, date(parsed_event_time) AS event_date,
               e.event_type, e.user_id, e.session_id, e.song_id, e.song_title,
               e.artist, e.duration_seconds, e.country, e.subscription, e.schema_version,
               source_record_id, kafka_timestamp, ingested_at AS source_ingested_at,
               current_timestamp() AS processed_at
        FROM checked_events WHERE rejection_reason IS NULL
        QUALIFY row_number() OVER (PARTITION BY e.event_id ORDER BY kafka_timestamp, source_record_id) = 1
      ) s ON t.event_id = s.event_id
      WHEN NOT MATCHED THEN INSERT *
    """)
    # Counts describe input records, including replayed event IDs, not newly inserted rows.
    total = frame.count()
    rejected = session.sql("SELECT count(*) FROM checked_events WHERE rejection_reason IS NOT NULL").first()[0]
    session.sql(f"""
      INSERT INTO {ops_schema}.pipeline_runs
      SELECT '{run_id}', 'silver', current_timestamp(), {total},
             'batch={batch_id}; rejected_input_records={rejected}'
    """)


query = (spark.readStream.table(f"{BRONZE}.events").writeStream
         .foreachBatch(process_batch)
         .option("checkpointLocation", f"{VOLUME}/checkpoints/silver")
         .trigger(availableNow=True).start())
query.awaitTermination()
print("Silver processing complete. Duplicate event IDs retain their first accepted value.")
