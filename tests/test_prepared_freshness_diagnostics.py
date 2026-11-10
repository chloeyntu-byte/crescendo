"""Prelaunch stale diagnostics retain their real cause without starting a run."""
import copy
import json
import tempfile
import unittest
from unittest import mock

from crescendo_engine import prepared_freshness as freshness
from tests import test_prepared_freshness as prepared_fixtures
from tests import test_environment_test as environment_fixtures


class PreparedFreshnessDiagnosticTests(unittest.TestCase):
    def prepared(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        fixture = prepared_fixtures.PreparedFreshnessTests()
        prepared = fixture._prepared(directory.name)
        fixture._capture(prepared)
        return prepared

    def fail(self, prepared):
        with mock.patch.object(freshness, "source_design_identity",
                return_value=prepared["source_identity"]):
            with self.assertRaises(ValueError) as failure:
                freshness.require_prepared_freshness(prepared, action="start optimization")
        return failure.exception

    def test_stale_catalog_reports_only_changed_component_and_small_hashes(self):
        for field in ("setup", "hierarchy", "parameters", "outputs"):
            with self.subTest(field=field):
                prepared = self.prepared()
                if isinstance(prepared[field], dict):
                    prepared[field]["late_native_metadata"] = True
                else:
                    prepared[field].append({"late_native_metadata": True})
                error = self.fail(prepared)
                proof = error.detail["prepared_freshness"]
                self.assertEqual(proof["changed_catalog_components"], [field])
                self.assertNotEqual(proof["loaded_fingerprint_before"], proof["loaded_fingerprint_after"])
                self.assertEqual(proof["source_fingerprint_before"], proof["source_fingerprint_after"])
                self.assertFalse(error.detail["execution_started"])
                self.assertFalse(error.detail["native_run_invoked"])
                self.assertIn(field, str(error))
                self.assertLess(len(json.dumps(error.detail)), 4096)
                self.assertNotIn("late_native_metadata", json.dumps(error.detail))

    def test_legacy_fingerprint_remains_valid_without_component_sidecar(self):
        prepared = self.prepared()
        prepared["prepared_freshness"]["loaded_identity"].pop("components")
        with mock.patch.object(freshness, "source_design_identity", return_value=prepared["source_identity"]):
            self.assertTrue(freshness.require_prepared_freshness(prepared)["ok"])
        prepared["parameters"][0]["numeric_value"] = 9e-6
        error = self.fail(prepared)
        self.assertEqual(error.detail["prepared_freshness"]["changed_catalog_components"], [])
        self.assertIn("loaded hierarchy or parameter catalog changed", str(error))

    def test_malformed_record_keeps_a_value_error_and_explicit_stage(self):
        prepared = self.prepared()
        prepared["prepared_freshness"] = "invalid"
        error = self.fail(prepared)
        self.assertEqual(error.detail["failure_stage"], "prepared_freshness")
        self.assertEqual(error.detail["prepared_freshness"]["state"], "malformed")

    def test_diagnostic_manager_preserves_prelaunch_cause_in_single_log(self):
        prepared = self.prepared()
        prepared["setup"]["test_corner_masks"] = {"main": {"known": True}}
        error = self.fail(prepared)
        case = environment_fixtures.EnvironmentTestManagerTests(
            "test_empty_ui_execution_selection_blocks_native_start_without_source_fallback")
        case.setUp()
        self.addCleanup(case.doCleanups)
        manager, service, spec = case.fixture()
        with mock.patch.object(service, "start", side_effect=error) as start:
            result = case.finish(manager, manager.start("prepared-fixture", spec)["test_id"])
        self.assertEqual(start.call_count, 1)
        self.assertEqual(service.roots, {})
        self.assertEqual(service.stops, [])
        failure = result["case_results"][0]
        self.assertEqual(failure["state"], "failed")
        self.assertIsNone(failure["run_id"])
        self.assertEqual(failure["first_cause"]["failure_stage"], "prepared_freshness")
        self.assertEqual(failure["first_cause_layer"], "netlist_generation")
        self.assertEqual(failure["first_cause"]["prepared_freshness"]["changed_catalog_components"], ["setup"])
        with open(result["main_log"], encoding="utf-8") as stream:
            text = stream.read()
        self.assertIn("prepared_freshness", text)
        self.assertIn("changed_catalog_components", text)
        self.assertNotIn('"failure_stage": "run_registration"', text)
        self.assertFalse(failure["stop_required"])


if __name__ == "__main__":
    unittest.main()
