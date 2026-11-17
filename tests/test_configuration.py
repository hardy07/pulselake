"""Connection configuration stays local and supports environment overrides."""
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from project_config import configure_environment


class ConfigurationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        env = patch.dict(os.environ, {}, clear=True)
        env.start()
        self.addCleanup(env.stop)

    def settings(self):
        (self.root / "runtime").mkdir()
        (self.root / "runtime/settings.json").write_text(json.dumps({
            "host": "https://example.cloud.databricks.com",
            "warehouse_id": "example123", "catalog": "demo", "profile": "pulselake",
        }))

    def test_local_settings_configure_all_consumers(self):
        self.settings()
        configure_environment(self.root, require_connection=True)
        self.assertEqual(os.environ["PULSELAKE_VOLUME"], "/Volumes/demo/pulselake_bronze/ingest")
        self.assertEqual(os.environ["DATABRICKS_HTTP_PATH"], "/sql/1.0/warehouses/example123")
        self.assertEqual(os.environ["BUNDLE_VAR_catalog"], "demo")
        self.assertEqual(os.environ["BUNDLE_VAR_workspace_host"], "https://example.cloud.databricks.com")

    def test_environment_overrides_saved_settings(self):
        self.settings()
        os.environ["PULSELAKE_WAREHOUSE_ID"] = "override123"
        os.environ["PULSELAKE_CATALOG"] = "other"
        configure_environment(self.root, require_connection=True)
        self.assertEqual(os.environ["BUNDLE_VAR_warehouse_id"], "override123")
        self.assertEqual(os.environ["PULSELAKE_VOLUME"], "/Volumes/other/pulselake_bronze/ingest")

    def test_cloud_commands_require_settings(self):
        with self.assertRaisesRegex(ValueError, "configure.py"):
            configure_environment(self.root, require_connection=True)

    def test_invalid_catalog_rejected_before_sql_construction(self):
        os.environ["PULSELAKE_CATALOG"] = "invalid;catalog"
        with self.assertRaisesRegex(ValueError, "simple SQL identifier"):
            configure_environment(self.root)
