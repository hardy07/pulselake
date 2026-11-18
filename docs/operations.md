# Operations and recovery

## Recovery and limitations

- Spark publishes only completed batches from staging to the ready directory.
- Persistent Kafka offsets, deterministic upload names, checksum validation, and
  the upload ledger make retries recoverable. Silver additionally deduplicates IDs.
- A repeated event ID keeps the first accepted event; it is not an update.
- Timestamps use UTC. Gold incremental selection uses ingestion time so older
  events arriving later can still be processed.
- There is no single atomic exactly-once transaction across Kafka, disk, the Files
  API, and Delta. Complete distributed failure/replay testing remains open.
- Keep checkpoints, Kafka volumes, and receipts together with their matching source.
  Do not reuse old checkpoints with an empty Kafka broker.
- Confirmed remote files are assumed immutable. Local uploaded batches are retained;
  automatic spool cleanup is not implemented. Kafka retention is seven days.
- One local broker and local-mode Spark are development infrastructure. Free Edition
  compute quotas apply. No production throughput/latency claim has been measured.

Stop the producer, allow pending batches/uploads to finish, stop the watcher, then:

```powershell
docker compose stop
```

Restart with `docker compose up -d kafka kafka-init spark`. Avoid `down -v` when
retaining data. See [troubleshooting](pipeline.md) for common failures.

