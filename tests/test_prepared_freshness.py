import copy
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine.prepared_freshness import (
    capture_prepared_freshness, require_prepared_freshness,
    verify_prepared_freshness,
)


class PreparedFreshnessTests(unittest.TestCase):
    def _prepared(self, root):
        setup_dir = os.path.join(root, "TB", "maestro")
        os.makedirs(setup_dir)
        maestro_sdb = os.path.join(setup_dir, "maestro.sdb")
        active_state = os.path.join(setup_dir, "active.state")
        with open(maestro_sdb, "w") as stream:
            stream.write("setup-v1")
        with open(active_state, "w") as stream:
            stream.write("state-v1")
        return {
            "setup": {"source": {
                "maestro_sdb": maestro_sdb,
                "active_state": active_state,
                "setup_dir": setup_dir,
            }},
            "hierarchy": {"designs": [{"hierarchy": {"cells": []}}]},
            "parameters": [{"parameter_id": "/M0:w", "numeric_value": 1e-6}],
            "outputs": [{"key": "test::gain"}],
            "source_identity": {
                "schema_version": "crescendo.source_design_identity.v1",
                "fingerprint": "source-v1",
            },
        }

    def _capture(self, prepared):
        with mock.patch(
                "crescendo_engine.prepared_freshness.source_design_identity",
                return_value=copy.deepcopy(prepared["source_identity"])):
            prepared["prepared_freshness"] = capture_prepared_freshness(prepared)

    def test_unchanged_prepared_design_is_current(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = self._prepared(root)
            self._capture(prepared)
            with mock.patch(
                    "crescendo_engine.prepared_freshness.source_design_identity",
                    return_value=copy.deepcopy(prepared["source_identity"])):
                result = verify_prepared_freshness(prepared)
            self.assertTrue(result["ok"])
            self.assertEqual(result["state"], "current")

    def test_maestro_sdb_or_active_state_change_requires_reload(self):
        for name in ("maestro_sdb", "active_state"):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as root:
                prepared = self._prepared(root)
                self._capture(prepared)
                path = prepared["setup"]["source"][name]
                with open(path, "a") as stream:
                    stream.write("-changed")
                with mock.patch(
                        "crescendo_engine.prepared_freshness.source_design_identity",
                        return_value=copy.deepcopy(prepared["source_identity"])):
                    with self.assertRaisesRegex(ValueError, "Load and Inspect again"):
                        require_prepared_freshness(prepared, action="start optimization")

    def test_source_schematic_change_requires_reload_without_hierarchy_rescan(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = self._prepared(root)
            self._capture(prepared)
            changed = dict(prepared["source_identity"], fingerprint="source-v2")
            with mock.patch(
                    "crescendo_engine.prepared_freshness.source_design_identity",
                    return_value=changed) as identity:
                result = verify_prepared_freshness(prepared)
            self.assertFalse(result["ok"])
            self.assertIn("source dependency identity changed (changed library/scope unavailable)", result["reasons"])
            identity.assert_called_once_with(
                prepared["setup"], prepared["hierarchy"], config=None,
            )

    def test_loaded_catalog_change_requires_reload(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = self._prepared(root)
            self._capture(prepared)
            prepared["parameters"][0]["numeric_value"] = 2e-6
            with mock.patch(
                    "crescendo_engine.prepared_freshness.source_design_identity",
                    return_value=copy.deepcopy(prepared["source_identity"])):
                result = verify_prepared_freshness(prepared)
            self.assertFalse(result["ok"])
            self.assertIn("loaded hierarchy or parameter catalog changed", result["reasons"])

    def test_ui_selection_and_ranges_do_not_change_prepared_identity(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = self._prepared(root)
            self._capture(prepared)
            ui_spec = {
                "variables": [{
                    "parameter_id": "/M0:w", "minimum": 1e-6, "maximum": 4e-6,
                }],
            }
            ui_spec["variables"][0]["maximum"] = 8e-6
            with mock.patch(
                    "crescendo_engine.prepared_freshness.source_design_identity",
                    return_value=copy.deepcopy(prepared["source_identity"])):
                result = verify_prepared_freshness(prepared)
            self.assertTrue(result["ok"])


if __name__ == "__main__":
    unittest.main()
