"""Native capture idle retention is separate from the shared CLI idle policy."""
import copy
import json
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine import private_netlist


class CaptureLingerPolicyTests(unittest.TestCase):
    def config(self, requested=300, global_linger="300", test_linger=None):
        result = {"cadence": {"ocean_bin": "/bin/true"}, "runtime": {},
            "_capture_job_overrides": {"timeouts": {"start_seconds": 300, "configure_seconds": 300,
                                                    "run_seconds": 120, "linger_seconds": requested}},
            "_capture_source_job_policy": {"global": {"properties": {"lingertimeout": global_linger}},
                                            "tests": []}}
        if test_linger is not None:
            result["_capture_source_job_policy"]["tests"] = [{"name": "tb", "properties": {"lingertimeout": test_linger}}]
        return result

    def test_positive_capture_linger_preserves_user_and_source_policies(self):
        config = self.config()
        before = copy.deepcopy(config)
        profile = private_netlist._capture_timeout_profiles(config, ["tb"])
        for row in [profile["global"], profile["tests"]["tb"]]:
            self.assertEqual(row["linger"]["requested"], 300)
            self.assertEqual(row["linger"]["effective"], 300)
            self.assertEqual(row["linger"]["effective_source"], "manual_override")
            self.assertEqual(row["effective_overrides"]["timeouts"]["linger_seconds"], 300)
            self.assertEqual(row["requested_overrides"]["timeouts"]["linger_seconds"], 300)
            self.assertEqual(row["linger"]["scope"], "native_netlist_capture_only")
        self.assertEqual(config, before)
        self.assertFalse(profile["cli_policy_modified"])
        self.assertFalse(profile["source_policy_modified"])

    def test_blank_manual_linger_is_explicit_unlimited_not_inherit_or_zero(self):
        from crescendo_engine.job_policy import timeout_override_skill_arguments, timeout_override_skill_readback
        row = private_netlist._capture_timeout_profiles(self.config(requested=""), ["tb"])["global"]
        self.assertEqual(row["linger"]["requested"], -1)
        self.assertEqual(row["linger"]["effective"], -1)
        self.assertEqual(row["linger"]["status"], "unlimited")
        self.assertFalse(row["linger"]["zero_allowed"])
        self.assertIn('?lingertimeout "-1"', timeout_override_skill_arguments(row["effective_overrides"]))
        self.assertIn('equal(getq(policy lingertimeout) "-1")', timeout_override_skill_readback(row["effective_overrides"]))

    def test_positive_or_explicit_disabled_manual_linger_is_preserved(self):
        for requested in (120, -1):
            with self.subTest(requested=requested):
                profile = private_netlist._capture_timeout_profiles(self.config(requested=requested), ["tb"])
                self.assertEqual(profile["global"]["linger"]["effective"], requested)
                self.assertFalse(profile["global"]["linger"]["zero_allowed"])

    def test_source_zero_is_rejected_without_a_manual_positive_or_unlimited_override(self):
        config = self.config(global_linger="0")
        config["_capture_job_overrides"] = {}
        with self.assertRaisesRegex(private_netlist.PrivateNetlistUnsupported, "zero is invalid"):
            private_netlist._capture_timeout_profiles(config, ["tb"])

    def test_manual_zero_is_rejected_without_a_native_fallback(self):
        with self.assertRaises(ValueError):
            private_netlist._capture_timeout_profiles(self.config(requested=0), ["tb"])

    def test_each_invalid_native_source_timeout_requires_explicit_override(self):
        from crescendo_engine.job_policy import JOB_TIMEOUT_PROPERTIES
        for field, native in JOB_TIMEOUT_PROPERTIES.items():
            for raw in ("0", "not-seconds", "inf", "-2"):
                with self.subTest(field=field, raw=raw):
                    config = self.config()
                    config["_capture_job_overrides"] = {}
                    config["_capture_source_job_policy"]["global"]["properties"][native] = raw
                    with self.assertRaises(private_netlist.PrivateNetlistUnsupported) as caught:
                        private_netlist._capture_timeout_profiles(config, ["tb"])
                    self.assertEqual(caught.exception.detail["field"], field)
                    config["_capture_job_overrides"] = {"timeouts": {field: ""}}
                    profile = private_netlist._capture_timeout_profiles(config, ["tb"])
                    self.assertEqual(profile["global"]["fields"][field]["effective"], -1)

    def test_explicit_preparation_budget_replaces_hidden_legacy_capture_default(self):
        before = self.config()
        prepared = {"setup": {"tests": [{"name": "tb"}]}}
        for requested, effective in ((-1, -1), ("", -1), (1200, 1200), (3.5, 3.5)):
            with self.subTest(requested=requested):
                frozen = private_netlist._capture_request_config(prepared, before,
                    {"candidate_timeout_seconds": requested, "maestro_job_overrides": {"timeouts": {"linger_seconds": 300}}})
                self.assertEqual(frozen["runtime"]["private_netlist_prepare_timeout_seconds"], effective)
                self.assertEqual(frozen["_capture_outer_budget"]["source"], "explicit_preparation_measurement_budget")
                self.assertEqual(frozen["_capture_outer_budget"]["enabled"], effective > 0)
        self.assertNotIn("private_netlist_prepare_timeout_seconds", before["runtime"])
        legacy = private_netlist._capture_request_config(prepared, before, {})
        self.assertEqual(legacy["runtime"]["private_netlist_prepare_timeout_seconds"], 300)
        for raw in (0, True, "inf", -2):
            with self.subTest(raw=raw), self.assertRaises(private_netlist.PrivateNetlistUnsupported):
                private_netlist._capture_request_config(prepared, before, {"candidate_timeout_seconds": raw})

    def test_global_and_independent_test_positive_values_are_recorded_separately(self):
        config = self.config(test_linger="600")
        config["_capture_job_overrides"] = {}
        profile = private_netlist._capture_timeout_profiles(config, ["tb"])
        self.assertEqual(profile["global"]["linger"]["effective"], "300")
        self.assertEqual(profile["tests"]["tb"]["linger"]["effective"], "600")

    def test_real_script_and_capture_diagnostic_use_effective_global_and_test_values(self):
        with tempfile.TemporaryDirectory(prefix="crescendo zero linger ' ") as root:
            workspace = {"root": root, "primary_library": "PRIVATE", "setup_cell": "TB",
                         "setup_view": "maestro", "setup": {"tests": [{"name": "tb"}]}}
            phase = os.path.join(root, "capture")
            command = {"returncode": 1, "guardian_cleanup": {"state": "dead"}}
            with mock.patch.object(private_netlist, "_capture_setup_identity", return_value={}), \
                    mock.patch.object(private_netlist, "reset_results"), \
                    mock.patch.object(private_netlist, "write_schematic_patch", return_value={"parameters": []}), \
                    mock.patch.object(private_netlist, "_workspace_startup_load_lines", return_value=[]), \
                    mock.patch.object(private_netlist, "workspace_test_design_lines", return_value=[]), \
                    mock.patch.object(private_netlist, "native_corner_schedule_skill", return_value=[]), \
                    mock.patch.object(private_netlist, "child_environment", return_value=dict(os.environ)), \
                    mock.patch("crescendo_engine.netlist_measurement.run_owned_command", return_value=command), \
                    mock.patch("crescendo_engine.native_diagnostics.skill_command_diagnostics", return_value={"ok": False}), \
                    self.assertRaises(private_netlist.PrivateNetlistUnsupported) as caught:
                private_netlist._capture_phase(workspace, [], {}, {}, {}, self.config(), phase)
            with open(os.path.join(phase, "capture.il")) as stream:
                script = stream.read()
            self.assertEqual(script.count('?lingertimeout "300"'), 2)
            self.assertEqual(script.count('equal(getq(crescCapturePolicy lingertimeout) "300")'), 2)
            self.assertNotIn('?lingertimeout "0"', script)
            self.assertEqual(script.count('?runtimeout "120"'), 2)
            with open(os.path.join(phase, "capture_diagnostics.json")) as stream:
                diagnostic = json.load(stream)
            with open(os.path.join(phase, "capture_generation.json")) as stream:
                generation = json.load(stream)
            self.assertEqual(diagnostic["capture_timeout_profiles"], generation["capture_timeout_profiles"])
            self.assertEqual(caught.exception.detail["capture_diagnostics"]["capture_timeout_profiles"], diagnostic["capture_timeout_profiles"])
            self.assertEqual(diagnostic["capture_timeout_profiles"]["global"]["linger"]["requested"], 300)
            self.assertEqual(diagnostic["capture_timeout_profiles"]["global"]["linger"]["effective"], 300)


if __name__ == "__main__":
    unittest.main()
