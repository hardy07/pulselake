"""Upload completed PulseLake batches using the current user's CLI login."""
import sys
import os
from pathlib import Path

from project_config import configure_environment


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[1]
    configure_environment(root)
    sys.path.insert(0, str(root))
    from pulselake.uploader import main

    # Run one pass by default; pass --watch for continuous file discovery.
    sys.argv = [sys.argv[0], "--spool", str(root / "runtime" / "spool"),
                "--ledger", str(root / "runtime" / "uploads.sqlite3"),
                "--profile", os.environ["DATABRICKS_CONFIG_PROFILE"], "--volume", os.environ["PULSELAKE_VOLUME"],
                *sys.argv[1:]]
    main()
