"""Publish immutable, checksummed batches only after all Parquet files are complete."""
import hashlib
import json
import os
import re
from pathlib import Path


def sha256_file(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def publish_batch(staging, ready, stream_id, batch_id, row_count):
    staging, ready = Path(staging), Path(ready)
    if not re.fullmatch(r"[a-zA-Z0-9_-]+", stream_id) or batch_id < 0:
        raise ValueError("Unsafe stream or batch identifier")
    if not (staging / "_SUCCESS").is_file():
        raise ValueError("Cannot publish unfinished Spark output")
    files = [{"name": p.name, "size": p.stat().st_size, "sha256": sha256_file(p)}
             for p in sorted(staging.glob("part-*.parquet"))]
    if not files or row_count <= 0:
        raise ValueError("Cannot publish an empty batch")
    manifest = {"version": 1, "stream_id": stream_id, "batch_id": batch_id,
                "row_count": row_count, "files": files}
    (staging / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    ready.mkdir(parents=True, exist_ok=True)
    destination = ready / f"{stream_id}-{batch_id:020d}"
    # Both folders reside on the same filesystem. The uploader ignores .staging entirely.
    if destination.exists():
        raise FileExistsError(destination)
    os.replace(staging, destination)
    return destination


def validate_manifest(path):
    path = Path(path)
    manifest = json.loads(path.read_text(encoding="utf-8"))
    if manifest.get("version") != 1 or not manifest.get("files"):
        raise ValueError("Unsupported or empty manifest")
    if not re.fullmatch(r"[a-zA-Z0-9_-]+", manifest["stream_id"]):
        raise ValueError("Unsafe stream id")
    if not isinstance(manifest["batch_id"], int) or manifest["batch_id"] < 0:
        raise ValueError("Invalid batch id")
    seen = set()
    for item in manifest["files"]:
        name = item["name"]
        if not re.fullmatch(r"part-[a-zA-Z0-9_.-]+\.parquet", name) or name in seen:
            raise ValueError("Unsafe or repeated file name")
        seen.add(name)
        candidate = path.parent / name
        if candidate.is_symlink() or not candidate.is_file():
            raise ValueError(f"Missing or unsafe file: {name}")
        if candidate.stat().st_size != item["size"] or sha256_file(candidate) != item["sha256"]:
            raise ValueError(f"Checksum mismatch: {name}")
    return manifest
