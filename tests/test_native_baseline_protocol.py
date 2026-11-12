"""The native protocol must not confuse local process death with remote Stop."""
import json
import os
import tempfile
import unittest
from contextlib import contextmanager, ExitStack
from unittest import mock

from crescendo_engine.native_baseline import (
    SCHEMA_VERSION, NativeBaselineError, _NativeSession, _read_protocol,
    reconcile_native_baseline, _native_policy_definitions, _baseline_launch_policy, _scripts, evaluate_native_baseline,
)


class NativeBaselineProtocolTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = self.directory.name
        self.driver = _NativeSession(self.root, {"root": self.root}, {}, "candidate")

    def tearDown(self):
        self.directory.cleanup()

    def protocol(self, state):
        with open(self.driver.protocol, "w", encoding="utf-8") as stream:
            stream.write(self.driver.nonce + "\t" + state + "\tproof\n")

    def test_missing_durable_launch_record_is_unknown(self):
        os.unlink(self.driver.path)
        proof = reconcile_native_baseline(self.root)
        self.assertFalse(proof["cleanup_proven"])
        self.assertTrue(proof["cleanup_pending"])

    def test_named_policy_preserves_scheduler_and_timeouts(self):
        workspace = {"root": self.root, "setup": {"job_policy_references": {"global": "SourcePolicy"}}}
        properties = {"name": "SourcePolicy", "distributionmethod": "Command",
                      "jobsubmitcommand": "scheduler_submit -queue design", "runtimeout": "600", "maxjobs": "4"}
        lines = _native_policy_definitions(workspace, {"global": {"available": True, "properties": properties}})
        text = "\n".join(lines)
        self.assertIn("axlAdd" "JobPolicy", text)
        self.assertIn('"Command"', text)
        self.assertIn('"scheduler_submit -queue design"', text)
        self.assertIn('"600"', text)
        self.assertNotIn('"Local"', text)
        self.assertNotIn('axlGetJobPolicy', text)

    def test_existing_failure_stage_survives_baseline_cleanup(self):
        @contextmanager
        def borrowed(*args, **kwargs):
            yield {"setup": {}}
        error = NativeBaselineError("source invalid")
        error.detail = {"failure_stage": "source_guard", "source_guard": {"ok": False}}
        prepared = {"setup_private_session": {"workspace_root": self.root},
                    "setup": {"tests": [{"name": "test", "simulator": "spectre"}]}}
        with mock.patch("crescendo_engine.setup_private_session.write_session_reference"), \
             mock.patch("crescendo_engine.setup_private_session.borrow_setup_private_session", borrowed), \
             mock.patch("crescendo_engine.test_selection.selection_plan", side_effect=error):
            with self.assertRaises(NativeBaselineError) as raised:
                evaluate_native_baseline("candidate", {"p": 1}, [{"parameter_id": "p"}],
                                         {"simulator": "spectre"}, prepared, {}, os.path.join(self.root, "request"))
        self.assertEqual(raised.exception.detail["failure_stage"], "source_guard")
        self.assertEqual(raised.exception.detail["source_guard"], {"ok": False})
        self.assertFalse(raised.exception.detail["cleanup_pending"])
        phases = raised.exception.detail["timing"]["phases"]
        self.assertFalse(phases["build"]["completed"])
        self.assertGreaterEqual(phases["build"]["elapsed_seconds"], 0)
        self.assertTrue(phases["native_cleanup"]["completed"])
        self.assertNotIn("execution", phases)

    def test_named_policy_without_frozen_definition_cannot_fall_back_local(self):
        workspace = {"root": self.root, "setup": {"job_policy_references": {"global": "SourcePolicy"}}}
        with self.assertRaises(NativeBaselineError):
            _native_policy_definitions(workspace, {})

    def test_launch_policy_rejects_wrong_observer_identity(self):
        with self.assertRaisesRegex(NativeBaselineError, 'identity differs'):
            _baseline_launch_policy({'global': {'properties': {'jobsubmitcommand': 'real'}}},
                                    [{'test': None, 'original_command': 'other', 'effective_command': 'wrapper'}])

    def test_startup_observer_is_bound_before_open_and_policy_is_unchanged(self):
        import copy
        from crescendo_engine.job_policy import command_job_policy
        original = command_job_policy({}, {'command': 'site_submit -q work', 'max_jobs': 4,
                                           'timeouts': {'run_seconds': 81}}, ['test'])
        before = copy.deepcopy(original)
        self.driver.workspace = {'root': self.root, 'primary_library': 'lib', 'setup_cell': 'tb',
                                 'setup_view': 'maestro', 'native_storage': {},
                                 'setup': {'job_policy_references': {'global': 'SitePolicy'}}}
        setup = {'tests': [{'name': 'test', 'enabled': True}]}
        patches = {
            'crescendo_engine.native_baseline._workspace_startup_load_lines': [],
            'crescendo_engine.storage_policy.native_storage_init_lines': [],
            'crescendo_engine.storage_policy.native_storage_capture_definitions': '',
            'crescendo_engine.storage_policy.native_storage_readback_lines': [],
            'crescendo_engine.storage_policy.native_history_readback_line': '',
            'crescendo_engine.workspace.workspace_test_design_lines': [],
            'crescendo_engine.single_test.native_selection': None,
            'crescendo_engine.test_selection.selection_skill_lines': [],
            'crescendo_engine.corner_selection.selection_skill_lines': [],
            'crescendo_engine.native_variable_scope_readback.native_variable_scope_skill_lines': [],
            'crescendo_engine.native_maestro_parameters.native_parameter_skill_lines': [],
            'crescendo_engine.private_netlist.native_corner_schedule_skill': [],
        }
        with ExitStack() as stack:
            for name, value in patches.items():
                stack.enter_context(mock.patch(name, return_value=value))
            startup, _, _ = _scripts(self.driver, [], {}, setup, None, None,
                                     {'job_policy': original}, {})
        with open(startup, encoding='utf-8') as stream:
            script = stream.read()
        wrappers = self.driver.job_observation['wrappers']
        self.assertEqual([row['test'] for row in wrappers], [None, 'test'])
        self.assertEqual(original, before)
        self.assertIn('"81"', script)
        self.assertIn('"4"', script)
        self.assertNotIn('site_submit -q work', script)
        self.assertIn(wrappers[0]['effective_command'], script)
        self.assertLess(script.index(wrappers[0]['effective_command']), script.index('maeOpenSetup('))

    def test_unlaunched_script_failure_can_release_suite(self):
        result = reconcile_native_baseline(self.root)
        self.assertTrue(result["cleanup_proven"])
        self.assertFalse(result["cleanup_pending"])

    def test_lost_controller_does_not_infer_stop_from_dead_ocean(self):
        self.driver.state["launch_attempted"] = True
        self.driver.persist()
        self.protocol("STOP_DONE")
        result = reconcile_native_baseline(self.root)
        self.assertFalse(result["cleanup_proven"])
        self.assertTrue(result["cleanup_pending"])

    def test_local_stop_requires_native_and_guardian_terminal_evidence(self):
        self.driver.state.update(launch_attempted=True, native_policy_local=True)
        self.protocol("STOP_DONE")
        with mock.patch("crescendo_engine.legacy_guardian.quiesce_legacy_guardian_attempt",
                        return_value={"state": "dead", "termination_proof": {"ok": True}}):
            proof = self.driver.cleanup()
        self.assertTrue(proof["cleanup_proven"])
        self.assertTrue(proof["native_stop"]["remote_stop_proven"])
        self.assertEqual(proof["native_stop"]["proof_scope"], "native_local_jobs_and_owned_descendants")

    def test_remote_stop_without_job_terminal_receipts_stays_unsafe(self):
        self.driver.state.update(launch_attempted=True, native_policy_local=False)
        self.protocol("STOP_DONE")
        with mock.patch("crescendo_engine.legacy_guardian.quiesce_legacy_guardian_attempt",
                        return_value={"state": "dead", "termination_proof": {"ok": True}}):
            proof = self.driver.cleanup()
        self.assertTrue(proof["native_stop"]["native_stop_wait_closed"])
        self.assertFalse(proof["native_stop"]["remote_stop_proven"])
        self.assertFalse(proof["cleanup_proven"])

    def test_completed_run_still_requires_guardian_cleanup(self):
        self.driver.state["launch_attempted"] = True
        self.protocol("CLOSED")
        with mock.patch("crescendo_engine.legacy_guardian.quiesce_legacy_guardian_attempt",
                        return_value={"state": "ambiguous", "termination_proof": {"ok": False}}):
            proof = self.driver.cleanup()
        self.assertFalse(proof["cleanup_proven"])
        self.assertTrue(proof["cleanup_pending"])

    def test_protocol_is_fenced_to_request_nonce(self):
        self.protocol("CLOSED")
        with self.assertRaises(NativeBaselineError):
            _read_protocol(self.driver.protocol, "different-request")

    def test_recovery_record_cannot_be_rebound_to_other_request(self):
        self.driver.state["request_root"] = self.root + "-other"
        self.driver.persist()
        with self.assertRaises(NativeBaselineError):
            reconcile_native_baseline(self.root)

    def test_previous_error_does_not_hide_completed_stop(self):
        self.driver.state["launch_attempted"] = True
        self.protocol("ERROR")
        with open(self.driver.protocol, "a", encoding="utf-8") as stream:
            stream.write(self.driver.nonce + "\tSTOP_DONE\tproof\n")
        self.assertEqual(self.driver.wait(("STOP_DONE",), 1)["state"], "STOP_DONE")


if __name__ == "__main__":
    unittest.main()
