"""Named source policy files are isolated before native Test materialization."""
import json
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine import private_netlist as capture


class CaptureNamedPolicyTests(unittest.TestCase):
    def workspace(self, root, references=None):
        return {"root": root, "setup": {"job_policy_references": references or {}}}

    def script(self, root, references=None, config=None, tests=None):
        tests = tests or ["lib:tb:1", "other:tb:2"]
        profiles = capture._capture_timeout_profiles(config or {}, tests)
        before, after, plan = capture._capture_setup_skill(
            self.workspace(root, references), tests, profiles, os.path.join(root, "capture_policy_mode.tsv"))
        return before, after, plan, profiles

    def test_preopen_aliases_include_global_and_disabled_test_references(self):
        with tempfile.TemporaryDirectory() as root:
            before, after, plan, _ = self.script(root, {"global": "ADEXL_Scheduler_short", "tests": [
                {"test": "disabled", "policy_name": "ADEXL_Scheduler_normal"}]})
            self.assertEqual(plan["alias_names"], ["ADEXL_Scheduler_normal", "ADEXL_Scheduler_short"])
            self.assertEqual("\n".join(before).count("errset(axlAdd" "JobPolicy("), 2)
            self.assertNotIn("maeSetJobPolicy", "\n".join(before))
            self.assertNotIn('maeSetJobPolicy("ADEXL_', "\n".join(after))
            self.assertIn('"preemptivestart"', "\n".join(before))
            self.assertEqual(plan["alias_directory"], os.path.join(root, ".cadence"))

    def test_private_saved_references_are_unioned_including_disabled_tests(self):
        with tempfile.TemporaryDirectory() as root:
            workspace = self.workspace(root, {"global": "prepared"})
            workspace["setup_sdb"] = os.path.join(root, "private.sdb")
            private = {"job_policy_references": {"global": "private_global", "tests": [
                {"test": "disabled", "policy_name": "private_disabled"}]}}
            with mock.patch("crescendo_engine.ade_setup.inspect_setup", return_value=private) as inspect:
                self.assertEqual(capture._capture_policy_names(workspace), ["prepared", "private_disabled", "private_global"])
            inspect.assert_called_once_with(workspace["setup_sdb"], backend="maestro")

    def test_unsafe_names_fail_before_any_policy_directory_is_written(self):
        with tempfile.TemporaryDirectory() as root:
            for name in ("../secret", "x/y", "x\\y", ".", "..", "x\ny", "x\ty", "x\x7fy"):
                with self.subTest(name=name), self.assertRaises(capture.PrivateNetlistUnsupported):
                    capture._capture_policy_names(self.workspace(root, {"global": name}))

    def test_global_and_each_test_receive_distinct_final_owned_names(self):
        with tempfile.TemporaryDirectory() as root:
            _, after, plan, _ = self.script(root)
            names = list(plan["final_policy_names"].values())
            self.assertEqual(len(names), 3)
            self.assertEqual(len(set(names)), 3)
            self.assertTrue(all(name.startswith("CRESCENDO_Capture_") for name in names))
            script = "\n".join(after)
            self.assertIn('?tests list("lib:tb:1")', script)
            self.assertIn('?testName "lib:tb:1"', script)
            self.assertEqual(script.count('?distributionmethod "Local"'), 3)
            self.assertEqual(script.count('?preemptivestart "0"'), 3)

    def test_source_and_manual_timeouts_are_explicitly_applied_and_read_back(self):
        config = {"_capture_source_job_policy": {"global": {"properties": {
            "starttimeout": "77", "configuretimeout": "88", "runtimeout": "99", "lingertimeout": "111"}},
            "tests": [{"name": "lib:tb:1", "properties": {"runtimeout": "123"}}]},
            "_capture_job_overrides": {"timeouts": {"linger_seconds": None}}}
        with tempfile.TemporaryDirectory() as root:
            _, after, plan, profiles = self.script(root, config=config)
            expected = plan["expected_policy_readback"]
            self.assertEqual(expected["global"]["starttimeout"], "77")
            self.assertEqual(expected["global"]["lingertimeout"], "-1")
            self.assertEqual(expected["test:lib:tb:1"]["runtimeout"], "123")
            self.assertEqual(expected["test:other:tb:2"]["runtimeout"], "99")
            self.assertIn('?configuretimeout "88"', "\n".join(after))
            self.assertEqual(profiles["global"]["effective_overrides"]["timeouts"]["start_seconds"], 77)

    def test_unknown_timeouts_do_not_claim_inheritance(self):
        with tempfile.TemporaryDirectory() as root:
            _, after, _, profiles = self.script(root)
            self.assertNotIn("?runtimeout", "\n".join(after))
            row = profiles["global"]["fields"]["run_seconds"]
            self.assertEqual(row["effective_source"], "unavailable")
            self.assertEqual(row["status"], "unknown")
            self.assertIn("not claimed", row["reason"])

    def test_nil_source_timeout_is_unknown_rather_than_unlimited(self):
        config = {"_capture_source_job_policy": {"global": {"properties": {
            "runtimeout": None, "lingertimeout": ""}}}}
        with tempfile.TemporaryDirectory() as root:
            _, after, _, profiles = self.script(root, config=config)
            self.assertNotIn("?runtimeout", "\n".join(after))
            self.assertEqual(profiles["global"]["fields"]["run_seconds"]["status"], "unknown")
            self.assertEqual(profiles["global"]["fields"]["linger_seconds"]["status"], "unknown")

    def test_mode_return_and_getter_are_recorded_before_failures(self):
        script = "\n".join(capture._capture_mode_skill(["lib:tb:1"]))
        self.assertLess(script.index('crescCaptureRecord("SET"'), script.index("unless(crescCaptureResult"))
        self.assertIn('equal(car(crescCaptureResult) "batch")', script)
        self.assertNotIn("maeSetTestEnvVar", script)
        self.assertNotIn("maeGetTestEnvVar", script)
        self.assertIn("maeSetEnvOption", script)
        self.assertIn("maeGetEnvOption", script)
        self.assertIn('crescCaptureRecord("MODE"', script)

    def test_record_writer_closes_each_row_before_later_guards(self):
        with tempfile.TemporaryDirectory() as root:
            before, _, _, _ = self.script(root)
            prefix = "\n".join(before)
            self.assertIn('port=outfile(', prefix)
            self.assertIn('"a")', prefix)
            self.assertIn("close(port)))", prefix)

    def test_actual_capture_script_initializes_policies_before_test_mode_and_run(self):
        from tests.test_native_baseline_capture import NativeBaselineCaptureTests
        helper = NativeBaselineCaptureTests()
        with tempfile.TemporaryDirectory() as root:
            workspace, parameters, config = helper.fixture(root)
            workspace["setup"]["job_policy_references"] = {"global": "MISSING_SCHEDULER"}
            phase = os.path.join(root, "baseline")
            helper.run_capture(workspace, parameters, config, phase)
            with open(os.path.join(phase, "capture.il")) as stream:
                script = stream.read()
            self.assertLess(script.index("axlAdd" "JobPolicy("), script.index("maeOpenSetup("))
            self.assertLess(script.index("maeOpenSetup("), script.index("maeSetJobPolicy("))
            self.assertLess(script.index("maeSetJobPolicy("), script.index("maeSetEnvOption("))
            self.assertLess(script.index("maeSetEnvOption("), script.index("maeGetEnvOption("))
            self.assertLess(script.index("maeGetEnvOption("), script.index("maeRunSimulation("))
            self.assertNotIn("maeSetTestEnvVar", script)
            self.assertNotIn("maeGetTestEnvVar", script)
            with open(os.path.join(phase, "capture_diagnostics.json")) as stream:
                diagnostics = json.load(stream)
            self.assertFalse(diagnostics["native_control_mode"]["confirmed"]["test_batch_mode"])
            self.assertIsNone(diagnostics["native_control_mode"]["confirmed"]["interactiveA"])
            self.assertEqual(diagnostics["native_control_mode"]["requested"]["mode"], "batch")
            self.assertEqual(diagnostics["native_control_mode"]["api"],
                             "maeSetEnvOption/maeGetEnvOption")

    def test_partial_failure_never_claims_policy_or_mode_confirmed(self):
        with tempfile.TemporaryDirectory() as root:
            _, _, plan, _ = self.script(root)
            path = plan["records_path"]
            with open(path, "w") as stream:
                stream.write('SET\t"lib:tb:1"\t"controlMode"\t"maeSetTestEnvVar"\tt\tnil\n')
            readback = capture._capture_setup_readback(plan, ["lib:tb:1"])
            self.assertFalse(readback["policy_confirmed"])
            self.assertFalse(readback["mode_confirmed"])
            self.assertIsNone(readback["records"][0]["value"])
            self.assertTrue(readback["records"][0]["call_succeeded"])

    def test_readback_requires_all_policy_fields_and_official_mode_getter(self):
        with tempfile.TemporaryDirectory() as root:
            _, _, plan, _ = self.script(root, tests=["lib:tb:1"])
            records = []
            for scope, fields in plan["expected_policy_readback"].items():
                name = plan["final_policy_names"][scope]
                records.append("\t".join(["SET", json.dumps(scope), json.dumps(name), json.dumps("maeSetJobPolicy"), "t", "t"]))
                for field, value in fields.items():
                    records.append("\t".join(["READ", json.dumps(scope), json.dumps(name), json.dumps(field), "t", json.dumps(value)]))
            for operation in ("maeSetEnvOption",):
                records.append('\t'.join(["SET", json.dumps("lib:tb:1"), json.dumps("controlMode"), json.dumps(operation), "t", json.dumps("batch")]))
                records.append('\t'.join(["MODE", json.dumps("lib:tb:1"), json.dumps("controlMode"), json.dumps(operation), "t", json.dumps("batch")]))
            with open(plan["records_path"], "w") as stream:
                stream.write("\n".join(records) + "\n")
            readback = capture._capture_setup_readback(plan, ["lib:tb:1"])
            self.assertTrue(readback["policy_confirmed"])
            self.assertTrue(readback["mode_confirmed"])
            with open(plan["records_path"], "w") as stream:
                stream.write("\n".join(records[1:]) + "\n")
            self.assertFalse(capture._capture_setup_readback(plan, ["lib:tb:1"])["policy_confirmed"])

    def write_mode_records(self, root, rows, tests=None):
        tests = tests or ["lib:tb:1"]
        _, _, plan, _ = self.script(root, tests=tests)
        with open(plan["records_path"], "w") as stream:
            stream.write("\n".join("\t".join(row) for row in rows) + "\n")
        return capture._capture_setup_readback(plan, tests)

    def mode_record(self, kind, operation="maeSetEnvOption", call="t",
                    value='"batch"', test="lib:tb:1", name="controlMode"):
        return [kind, json.dumps(test), json.dumps(name), json.dumps(operation), call, value]

    def test_official_env_option_only_accepts_colon_test_names(self):
        tests = ["lib:tb:1", "another:tb:two"]
        rows = [self.mode_record(kind, test=test) for test in tests for kind in ("SET", "MODE")]
        with tempfile.TemporaryDirectory() as root:
            observed = self.write_mode_records(root, rows, tests=tests)
        self.assertTrue(observed["mode_confirmed"])
        self.assertFalse(observed["policy_confirmed"])
        self.assertEqual(set(row["operation"] for row in observed["records"]), {"maeSetEnvOption"})

    def test_nil_or_exception_official_setter_cannot_be_qualified_by_getter(self):
        for call in ("t", "nil"):
            rows = [self.mode_record("SET", call=call, value="nil"), self.mode_record("MODE")]
            with self.subTest(call_succeeded=call), tempfile.TemporaryDirectory() as root:
                observed = self.write_mode_records(root, rows)
            self.assertFalse(observed["mode_confirmed"])
            self.assertEqual(observed["readback_status"], "collected")

    def test_official_getter_must_return_exact_batch_successfully(self):
        for call, value in (("t", '"interactive"'), ("t", "nil"), ("nil", '"batch"'),
                            ("t", '"Batch"'), ("t", "t")):
            rows = [self.mode_record("SET"), self.mode_record("MODE", call=call, value=value)]
            with self.subTest(call=call, value=value), tempfile.TemporaryDirectory() as root:
                observed = self.write_mode_records(root, rows)
            self.assertFalse(observed["mode_confirmed"])

    def test_missing_official_setter_or_getter_is_unqualified(self):
        for missing_kind in ("SET", "MODE"):
            rows = [self.mode_record(kind) for kind in ("SET", "MODE") if kind != missing_kind]
            with self.subTest(missing_kind=missing_kind), tempfile.TemporaryDirectory() as root:
                observed = self.write_mode_records(root, rows)
            self.assertFalse(observed["mode_confirmed"])

    def test_legacy_test_env_var_records_cannot_substitute_official_api(self):
        rows = [self.mode_record(kind, operation="maeSetTestEnvVar") for kind in ("SET", "MODE")]
        with tempfile.TemporaryDirectory() as root:
            self.assertFalse(self.write_mode_records(root, rows)["mode_confirmed"])
        for missing_kind in ("SET", "MODE"):
            mixed = rows + [self.mode_record(kind) for kind in ("SET", "MODE") if kind != missing_kind]
            with self.subTest(missing_kind=missing_kind), tempfile.TemporaryDirectory() as root:
                self.assertFalse(self.write_mode_records(root, mixed)["mode_confirmed"])

    def test_batch_readback_is_required_for_each_requested_test(self):
        rows = [self.mode_record(kind) for kind in ("SET", "MODE")]
        with tempfile.TemporaryDirectory() as root:
            observed = self.write_mode_records(root, rows, tests=["lib:tb:1", "missing:tb:2"])
        self.assertFalse(observed["mode_confirmed"])

    def test_mode_record_must_identify_control_mode_field(self):
        for wrong_kind in ("SET", "MODE"):
            rows = [self.mode_record(kind, name="otherOption" if kind == wrong_kind else "controlMode")
                    for kind in ("SET", "MODE")]
            with self.subTest(wrong_kind=wrong_kind), tempfile.TemporaryDirectory() as root:
                observed = self.write_mode_records(root, rows)
            self.assertFalse(observed["mode_confirmed"])

    def test_generated_guards_stop_nil_or_exception_before_run(self):
        script = "\n".join(capture._capture_mode_skill(["lib:tb:1"])) + "\nmaeRunSimulation()"
        setter = script.index("crescCaptureResult=errset(maeSetEnvOption(")
        setter_record = script.index('crescCaptureRecord("SET"')
        setter_guard = script.index('unless(crescCaptureResult && car(crescCaptureResult) error(')
        getter = script.index("crescCaptureResult=errset(maeGetEnvOption(")
        getter_guard = script.index('unless(crescCaptureResult && equal(car(crescCaptureResult) "batch") error(')
        self.assertLess(setter, setter_record)
        self.assertLess(setter_record, setter_guard)
        self.assertLess(setter_guard, getter)
        self.assertLess(getter, getter_guard)
        self.assertLess(getter_guard, script.index("maeRunSimulation()"))

    def test_symlink_policy_directory_is_rejected(self):
        with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as elsewhere:
            os.symlink(elsewhere, os.path.join(root, ".cadence"))
            with self.assertRaises(capture.PrivateNetlistUnsupported):
                self.script(root)

    def test_reused_phase_starts_fresh_and_cannot_borrow_previous_success(self):
        with tempfile.TemporaryDirectory() as root:
            _, _, plan, _ = self.script(root, tests=["lib:tb:1"])
            with open(plan["records_path"], "w") as stream:
                stream.write('MODE\t"lib:tb:1"\t"controlMode"\t"maeSetTestEnvVar"\tt\t"batch"\n')
            _, _, retried, _ = self.script(root, tests=["lib:tb:1"])
            self.assertEqual(os.path.getsize(retried["records_path"]), 0)
            observed = capture._capture_setup_readback(retried, ["lib:tb:1"])
            self.assertFalse(observed["mode_confirmed"])
            self.assertFalse(observed["policy_confirmed"])


if __name__ == "__main__":
    unittest.main()
