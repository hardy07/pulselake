"""Verify completed Spark batches and report real Parquet row counts."""
import argparse
import json
from collections import Counter
from pathlib import Path

from pulselake.batches import validate_manifest


def inspect(spool):
    import pyarrow.parquet as pq
    manifests = sorted(Path(spool).glob("ready/*/manifest.json"))
    if not manifests:
        raise ValueError("No completed batches yet. Check Spark logs and allow a trigger to finish.")
    records = Counter()
    events = Counter()
    files = total = malformed = 0
    for path in manifests:
        manifest = validate_manifest(path)
        batch_rows = 0
        for item in manifest["files"]:
            table = pq.read_table(path.parent / item["name"], columns=["raw_event", "source_record_id"])
            batch_rows += table.num_rows
            files += 1
            for record in table.to_pylist():
                records[record["source_record_id"]] += 1
                try:
                    event = json.loads(record["raw_event"])
                    events[event["event_id"]] += 1
                except (ValueError, KeyError, TypeError):
                    malformed += 1
        if batch_rows != manifest["row_count"]:
            raise ValueError(f"Row count mismatch in {path}: manifest={manifest['row_count']}, actual={batch_rows}")
        total += batch_rows
    return {
        "completed_batches": len(manifests),
        "parquet_files": files,
        "rows": total,
        "unique_source_records": len(records),
        "repeated_source_records": sum(n - 1 for n in records.values()),
        "unique_event_ids": len(events),
        "duplicate_event_deliveries": sum(n - 1 for n in events.values()),
        "malformed_event_payloads": malformed,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spool", default="runtime/spool")
    parser.add_argument("--expected-rows", type=int)
    args = parser.parse_args()
    result = inspect(args.spool)
    print(json.dumps(result, indent=2))
    if args.expected_rows is not None and result["rows"] != args.expected_rows:
        raise SystemExit(f"Expected {args.expected_rows} rows; found {result['rows']}")
    if result["repeated_source_records"]:
        raise SystemExit("Unexpected duplicate source records in the local spool")


if __name__ == "__main__":
    main()
