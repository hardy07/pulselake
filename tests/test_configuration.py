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
from configure import main as configure_main


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

    def test_deployment_requires_dashboard_but_local_processing_does_not(self):
        self.settings()
        configure_environment(self.root, require_connection=True)
        with self.assertRaisesRegex(ValueError, "--dashboard-id"):
            configure_environment(self.root, require_connection=True, require_dashboard=True)

    def test_dashboard_only_update_preserves_connection_settings(self):
        self.settings()
        (self.root / "dbt").mkdir()
        (self.root / "dbt/profiles.yml").write_text("existing local profile")
        configure_main(["--dashboard-id", "test-dashboard-123"], root=self.root)
        settings = json.loads((self.root / "runtime/settings.json").read_text())
        self.assertEqual(settings["host"], "https://example.cloud.databricks.com")
        self.assertEqual(settings["warehouse_id"], "example123")
        self.assertEqual(settings["catalog"], "demo")
        configure_environment(self.root, require_connection=True, require_dashboard=True)
        self.assertEqual(os.environ["BUNDLE_VAR_dashboard_id"], "test-dashboard-123")
        self.assertEqual((self.root / "dbt/profiles.yml").read_text(), "existing local profile")

    def test_dashboard_bundle_override_is_supported(self):
        os.environ["PULSELAKE_DASHBOARD_ID"] = "saved-dashboard"
        os.environ["BUNDLE_VAR_dashboard_id"] = "override-dashboard"
        configure_environment(self.root, require_dashboard=True)
        self.assertEqual(os.environ["BUNDLE_VAR_dashboard_id"], "override-dashboard")

    def test_dashboard_url_is_rejected(self):
        os.environ["PULSELAKE_DASHBOARD_ID"] = "https://example.com/dashboard"
        with self.assertRaisesRegex(ValueError, "not a dashboard URL"):
            configure_environment(self.root, require_dashboard=True)
