"""Upload immutable completed batches; retries reconcile remote checksums before acknowledging."""
import argparse
import hashlib
import json
import logging
import os
import shutil
import sqlite3
import time
import uuid
from contextlib import closing
from pathlib import Path

from pulselake.batches import sha256_file, validate_manifest

LOG = logging.getLogger("pulselake.uploader")


class LocalTarget:
    """Offline integration-test target with the same immutable-file contract."""
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.identity = str(self.root)

    def ensure(self, source, name, checksum):
        destination = self.root / name
        if destination.exists():
            if sha256_file(destination) != checksum:
                raise ValueError(f"Remote file differs: {name}")
            return False
        temporary = self.root / f".{name}.{uuid.uuid4().hex}.tmp"
        try:
            shutil.copyfile(source, temporary)
            os.replace(temporary, destination)
        finally:
            temporary.unlink(missing_ok=True)
        return True


class DatabricksTarget:
    def __init__(self, volume, profile):
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.core import Config
        if not volume.startswith("/Volumes/") or len(volume.strip("/").split("/")) != 4:
            raise ValueError("Use /Volumes/<catalog>/<schema>/<volume>")
        self.client = WorkspaceClient(config=Config(
            profile=profile, http_timeout_seconds=30, retry_timeout_seconds=60,
        ))
        self.root = volume.rstrip("/") + "/landing"
        self.identity = self.client.config.host.rstrip("/") + self.root
        self.client.files.create_directory(self.root)

    def _matches_existing(self, destination, checksum):
        from databricks.sdk.errors import NotFound
        try:
            response = self.client.files.download(destination)
        except NotFound:
            return False
        digest = hashlib.sha256()
        try:
            for block in iter(lambda: response.contents.read(1024 * 1024), b""):
                digest.update(block)
        finally:
            response.contents.close()
        if digest.hexdigest() != checksum:
            raise ValueError(f"Remote file differs: {destination}")
        return True

    def ensure(self, source, name, checksum):
        from databricks.sdk.errors import ResourceAlreadyExists
        # A manually uploaded first batch retains Spark's original filenames.
        # Verify its bytes before acknowledging it; don't upload another copy.
        original_destination = self.root + "/" + Path(source).name
        if self._matches_existing(original_destination, checksum):
            return False
        destination = self.root + "/" + name
        try:
            with Path(source).open("rb") as content:
                self.client.files.upload(destination, content, overwrite=False)
        except ResourceAlreadyExists:
            # Handles upload success followed by a lost response or a local ledger crash.
            if not self._matches_existing(destination, checksum):
                raise RuntimeError("Remote file disappeared during reconciliation; retry the batch")
            return False
        return True


def upload_once(spool, ledger, target):
    ledger = Path(ledger)
    ledger.parent.mkdir(parents=True, exist_ok=True)
    uploaded = skipped = batches = reconciled = 0
    with closing(sqlite3.connect(ledger)) as connection:
        connection.execute("CREATE TABLE IF NOT EXISTS uploads (target TEXT, name TEXT, sha256 TEXT, PRIMARY KEY(target,name))")
        for manifest_path in sorted(Path(spool).glob("ready/*/manifest.json")):
            manifest = validate_manifest(manifest_path)
            batches += 1
            for number, item in enumerate(manifest["files"]):
                name = (f"{manifest['stream_id']}_{manifest['batch_id']:020d}_"
                        f"{number:04d}_{item['sha256']}.parquet")
                previous = connection.execute("SELECT sha256 FROM uploads WHERE target=? AND name=?",
                                              (target.identity, name)).fetchone()
                if previous:
                    if previous[0] != item["sha256"]:
                        raise ValueError("Upload ledger checksum conflict")
                    skipped += 1
                    continue
                created = target.ensure(manifest_path.parent / item["name"], name, item["sha256"])
                connection.execute("INSERT INTO uploads VALUES (?, ?, ?)",
                                   (target.identity, name, item["sha256"]))
                connection.commit()
                if created is False:
                    reconciled += 1
                else:
                    uploaded += 1
    return {"batches": batches, "uploaded_files": uploaded, "reconciled_files": reconciled,
            "already_uploaded_files": skipped}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spool", default="runtime/spool")
    parser.add_argument("--ledger", default="runtime/uploads.sqlite3")
    parser.add_argument("--volume", default=os.environ.get("PULSELAKE_VOLUME"))
    parser.add_argument("--profile", default=os.environ.get("DATABRICKS_CONFIG_PROFILE", "pulselake"))
    parser.add_argument("--local-target", help="Test locally instead of uploading to Databricks")
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("--interval", type=float, default=15)
    args = parser.parse_args()
    if not args.local_target and not args.volume:
        parser.error("Provide --volume or --local-target")
    if args.interval <= 0:
        parser.error("interval must be positive")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    target = LocalTarget(args.local_target) if args.local_target else DatabricksTarget(args.volume, args.profile)
    try:
        while True:
            try:
                LOG.info(json.dumps(upload_once(args.spool, args.ledger, target)))
            except Exception:
                LOG.exception("Upload failed; local batch retained for retry")
                if not args.watch:
                    raise
            if not args.watch:
                break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        LOG.info("Uploader stopped; remaining batches are retained")


if __name__ == "__main__":
    main()
