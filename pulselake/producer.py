"""Publish finite or continuous synthetic events to Kafka."""
import argparse
import json
import time
import uuid
from datetime import datetime, timedelta, timezone

from pulselake.events import generate_events


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", default="kafka:9092")
    parser.add_argument("--topic", default="music.events")
    parser.add_argument("--count", type=int, default=10000, help="0 means run until stopped")
    parser.add_argument("--rate", type=float, default=25)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--run-id", default=None)
    args = parser.parse_args()
    if args.rate <= 0 or args.count < 0:
        parser.error("rate must be positive and count nonnegative")
    from confluent_kafka import Producer
    producer = Producer({"bootstrap.servers": args.bootstrap, "enable.idempotence": True,
                         "acks": "all", "client.id": "pulselake-simulator"})
    failures = []

    def delivered(error, message):
        if error:
            failures.append(str(error))

    total = 0
    run_id = args.run_id or str(uuid.uuid4())
    try:
        while args.count == 0 or total < args.count:
            size = min(1000, args.count - total) if args.count else 1000
            # Place a short history behind wall clock so generated times aren't in the future.
            start = datetime.now(timezone.utc) - timedelta(seconds=size)
            for event in generate_events(size, args.seed + total, f"{run_id}-{total}", start):
                payload = json.dumps(event).encode()
                while True:
                    try:
                        producer.produce(args.topic, key=event["user_id"] or "invalid",
                                         value=payload, on_delivery=delivered)
                        break
                    except BufferError:
                        producer.poll(1)
                producer.poll(0)
                if failures:
                    raise RuntimeError(f"Kafka delivery failed: {failures[0]}")
                total += 1
                time.sleep(1 / args.rate)
    except KeyboardInterrupt:
        print("Stopping generator; flushing queued messages.", flush=True)
    finally:
        pending = producer.flush(30)
    if pending or failures:
        raise RuntimeError(f"Delivery incomplete: {pending} pending; errors={failures[:3]}")
    print(json.dumps({"delivered": total, "run_id": run_id}), flush=True)


if __name__ == "__main__":
    main()
