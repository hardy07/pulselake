"""Failure/retry contract tests. No Kafka or Databricks account required."""
import json
import io
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from pulselake.batches import publish_batch, validate_manifest
from pulselake.events import generate_events
from pulselake.uploader import DatabricksTarget, LocalTarget, upload_once
from pulselake.inspect_batches import inspect


class BatchTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.spool = self.root / "spool"
        self.ledger = self.root / "uploads.sqlite3"
        self.target = LocalTarget(self.root / "remote")

    def batch(self, number=0, complete=True):
        staging = self.spool / ".staging" / str(number)
        staging.mkdir(parents=True)
        (staging / "part-00000.parquet").write_bytes(b"fixture-parquet-bytes")
        if complete:
            (staging / "_SUCCESS").touch()
        return staging

    def publish(self, number=0):
        return publish_batch(self.batch(number), self.spool / "ready", "teststream", number, 10)

    def test_unfinished_batch_is_not_published(self):
        with self.assertRaisesRegex(ValueError, "unfinished"):
            publish_batch(self.batch(complete=False), self.spool / "ready", "teststream", 0, 10)

    def test_staging_is_ignored_by_uploader(self):
        self.batch()
        self.assertEqual(upload_once(self.spool, self.ledger, self.target)["uploaded_files"], 0)

    def test_retry_does_not_duplicate_files(self):
        self.publish()
        self.assertEqual(upload_once(self.spool, self.ledger, self.target)["uploaded_files"], 1)
        again = upload_once(self.spool, self.ledger, self.target)
        self.assertEqual(again["uploaded_files"], 0)
        self.assertEqual(again["already_uploaded_files"], 1)
        self.assertEqual(len(list(self.target.root.glob("*.parquet"))), 1)

    def test_lost_response_is_reconciled_on_retry(self):
        self.publish()
        target = self.target

        class LostResponse:
            identity = target.identity

            def ensure(self, *args):
                target.ensure(*args)
                raise ConnectionError("remote write succeeded; response lost")

        with self.assertRaises(ConnectionError):
            upload_once(self.spool, self.ledger, LostResponse())
        recovery = upload_once(self.spool, self.ledger, target)
        self.assertEqual(recovery["uploaded_files"], 0)
        self.assertEqual(recovery["reconciled_files"], 1)
        self.assertEqual(len(list(target.root.glob("*.parquet"))), 1)

    def test_local_corruption_stops_upload(self):
        published = self.publish()
        (published / "part-00000.parquet").write_bytes(b"tampered")
        with self.assertRaisesRegex(ValueError, "Checksum"):
            upload_once(self.spool, self.ledger, self.target)
        self.assertEqual(len(list(self.target.root.glob("*.parquet"))), 0)

    def test_remote_conflict_is_not_overwritten(self):
        self.publish()
        upload_once(self.spool, self.ledger, self.target)
        remote = next(self.target.root.glob("*.parquet"))
        remote.write_bytes(b"unexpected content")
        with self.assertRaisesRegex(ValueError, "Remote file differs"):
            upload_once(self.spool, self.root / "new-ledger.sqlite3", self.target)
        self.assertEqual(remote.read_bytes(), b"unexpected content")

    def test_ledger_is_scoped_to_destination(self):
        self.publish()
        upload_once(self.spool, self.ledger, self.target)
        second = LocalTarget(self.root / "second-remote")
        self.assertEqual(upload_once(self.spool, self.ledger, second)["uploaded_files"], 1)

    def test_path_traversal_is_rejected(self):
        published = self.publish()
        path = published / "manifest.json"
        manifest = json.loads(path.read_text())
        manifest["files"][0]["name"] = "../outside.parquet"
        path.write_text(json.dumps(manifest))
        with self.assertRaisesRegex(ValueError, "Unsafe"):
            validate_manifest(path)

    def test_published_batch_cannot_be_replaced(self):
        self.publish()
        staging = self.batch(1)
        with self.assertRaises(FileExistsError):
            publish_batch(staging, self.spool / "ready", "teststream", 0, 10)

    def remote_target(self, files=None, lose_response=False):
        from databricks.sdk.errors import NotFound, ResourceAlreadyExists
        contents = dict(files or {})

        class FakeFiles:
            uploads = 0

            def download(self, path):
                if path not in contents:
                    raise NotFound("No such file")
                return SimpleNamespace(contents=io.BytesIO(contents[path]))

            def upload(self, path, content, overwrite=False):
                if path in contents:
                    raise ResourceAlreadyExists("File exists")
                contents[path] = content.read()
                self.uploads += 1
                if lose_response:
                    raise ConnectionError("Upload response lost")

        target = DatabricksTarget.__new__(DatabricksTarget)
        target.root = "/Volumes/workspace/test/ingest/landing"
        target.identity = "https://test-workspace" + target.root
        target.client = SimpleNamespace(files=FakeFiles())
        return target, contents

    def test_manual_upload_is_verified_without_creating_a_duplicate(self):
        self.publish()
        original = "/Volumes/workspace/test/ingest/landing/part-00000.parquet"
        target, files = self.remote_target({original: b"fixture-parquet-bytes"})
        result = upload_once(self.spool, self.ledger, target)
        self.assertEqual(result["uploaded_files"], 0)
        self.assertEqual(result["reconciled_files"], 1)
        self.assertEqual(target.client.files.uploads, 0)
        self.assertEqual(list(files), [original])
        self.assertEqual(upload_once(self.spool, self.ledger, target)["already_uploaded_files"], 1)

    def test_manual_upload_with_different_content_is_not_acknowledged(self):
        self.publish()
        original = "/Volumes/workspace/test/ingest/landing/part-00000.parquet"
        target, files = self.remote_target({original: b"different"})
        with self.assertRaisesRegex(ValueError, "Remote file differs"):
            upload_once(self.spool, self.ledger, target)
        files[original] = b"fixture-parquet-bytes"
        result = upload_once(self.spool, self.ledger, target)
        self.assertEqual(result["reconciled_files"], 1)
        self.assertEqual(result["already_uploaded_files"], 0)

    def test_remote_new_batch_upload_and_lost_response_recovery(self):
        self.publish()
        target, files = self.remote_target(lose_response=True)
        with self.assertRaises(ConnectionError):
            upload_once(self.spool, self.ledger, target)
        result = upload_once(self.spool, self.ledger, target)
        self.assertEqual(result["uploaded_files"], 0)
        self.assertEqual(result["reconciled_files"], 1)
        self.assertEqual(len(files), 1)
        self.assertEqual(target.client.files.uploads, 1)


class EventTests(unittest.TestCase):
    def events(self, **kwargs):
        return list(generate_events(200, start=datetime(2026, 1, 1, tzinfo=timezone.utc), **kwargs))

    def test_fixture_is_reproducible_and_includes_edge_cases(self):
        events = self.events()
        self.assertEqual(events, self.events())
        self.assertEqual(len(events), 200)
        self.assertLess(len({e["event_id"] for e in events}), len(events))
        self.assertTrue(any(e["user_id"] is None for e in events))
        self.assertTrue(any(e["event_time"].startswith("2025-12-31") for e in events))

    def test_independent_runs_have_independent_event_ids(self):
        first = {e["event_id"] for e in self.events(run_id="first")}
        second = {e["event_id"] for e in self.events(run_id="second")}
        self.assertFalse(first & second)


class ParquetTests(unittest.TestCase):
    def test_real_parquet_roundtrip_and_delivery_counts(self):
        import pyarrow as pa
        import pyarrow.parquet as pq
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            spool = root / "spool"
            staging = spool / ".staging" / "fixture"
            staging.mkdir(parents=True)
            events = list(generate_events(200, start=datetime(2026, 1, 1, tzinfo=timezone.utc)))
            table = pa.table({"raw_event": [json.dumps(e) for e in events],
                              "source_record_id": [f"record-{n}" for n in range(200)]})
            pq.write_table(table, staging / "part-00000.parquet")
            (staging / "_SUCCESS").touch()
            publish_batch(staging, spool / "ready", "fixture", 0, 200)
            result = inspect(spool)
            self.assertEqual(result["rows"], 200)
            self.assertEqual(result["unique_source_records"], 200)
            self.assertEqual(result["duplicate_event_deliveries"], 9)
            self.assertEqual(result["unique_event_ids"], 191)
            target = LocalTarget(root / "uploaded")
            upload_once(spool, root / "ledger.sqlite3", target)
            uploaded = pq.read_table(next(target.root.glob("*.parquet")))
            self.assertEqual(uploaded.to_pydict(), table.to_pydict())

    def test_missing_spark_output_is_not_success(self):
        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaisesRegex(ValueError, "No completed batches"):
                inspect(folder)


if __name__ == "__main__":
    unittest.main()
