from __future__ import print_function

import os
import copy
import tempfile
import threading
import unittest
from unittest import mock

from crescendo_engine.resident_validation import ResidentValidationSession
from crescendo_engine.server import OptimizerService


class EdaOverrideCleanupTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = self.temp.name
        self.service = OptimizerService({"runtime": {
            "run_root": self.root, "range_validation_timeout_seconds": 1,
            "range_validation_shutdown_timeout_seconds": 0.05,
        }, "cadence": {}})
        self.service.prepared["prepared"] = {
            "parameters": [{"parameter_id": "w", "optimizable": True}],
            "setup": {"tests": [{"name": "T", "enabled": True}], "selected_test": "T", "_source_tests": [{"name": "T", "enabled": True}]},
            "resident_validation": {"state": "deferred"},
        }
        self.session = ResidentValidationSession("prepared", {}, self.service.config,
            os.path.join(self.root, "range_validation_resident", "prepared_prepared"))
        self.service.resident_validation = self.session

    def _proven_close(self, **kwargs):
        self.session.closed = True
        return {"closed": True, "cleanup_proven": True}

    def _run_failed_callback(self, scope="resident_validation", close=None):
        def run(*args, **kwargs):
            self.service._bind_range_validation_cleanup(args[0]["job_id"], self.session)
            args[5](state="failed", unsafe_to_optimize=True,
                    unsafe_cleanup_scope=scope,
                    unsafe_cleanup_session_token=self.session.token if scope == "resident_validation" else None,
                    cleanup_error="initial owned cleanup was not yet proven")
        with mock.patch("crescendo_engine.server.require_prepared_freshness"), \
                mock.patch("crescendo_engine.server.run_range_validation", side_effect=run), \
                mock.patch.object(self.session, "close", side_effect=close or self._proven_close):
            response = self.service.validate_ranges("prepared", [{"parameter_id": "w", "minimum": 1, "maximum": 2}])
            self.service.range_validation_threads[response["job_id"]].join(1)
        return response["job_id"]

    def _pending_job(self, tracked=True, scope="resident_validation"):
        job_id = "range_validation_pending"
        job_root = os.path.join(self.root, "range_validations", job_id)
        os.makedirs(job_root)
        self.service.range_validations[job_id] = {
            "job_id": job_id, "job_root": job_root, "prepared_id": "prepared",
            "state": "failed", "unsafe_to_optimize": True,
            "unsafe_cleanup_scope": scope,
            "unsafe_cleanup_session_token": self.session.token,
        }
        self.service._bind_range_validation_cleanup(job_id, self.session)
        if tracked:
            thread = threading.Thread(target=lambda: None)
            thread.start()
            thread.join()
            self.service.range_validation_threads[job_id] = thread
            self.service.range_validation_cancels[job_id] = threading.Event()
        return job_id

    def _save(self):
        discovery = {"effective": {"spectre_model_dir": self.root}, "sources": {}, "ok": True}
        with mock.patch("crescendo_engine.server.configure_eda",
                        return_value=(discovery, os.path.join(self.root, "eda.json"))), \
                mock.patch("crescendo_engine.server.persist_eda_setup",
                           return_value=os.path.join(self.root, "eda.json")) as persist, \
                mock.patch.object(self.service, "eda_setup", return_value={}):
            result = self.service.update_eda_setup({"mode": "manual", "manual": {"spectre_model_dir": self.root}})
            self.assertTrue(persist.called)
            return result

    def test_override_recovers_exact_retained_legacy_receipt_without_restarting_direct_cdf(self):
        job = self._run_failed_callback()
        status = self.service.range_validation_status(job)
        self.assertTrue(status["unsafe_to_optimize"])
        self.assertEqual(status["resident_cleanup"]["state"], "pending")
        with mock.patch.object(self.service, "_start_resident_validation") as old_start, \
                mock.patch.object(self.session, "close", side_effect=self._proven_close):
            self.assertTrue(self._save()["prepared_invalidated"])
        old_start.assert_not_called()
        status = self.service.range_validation_status(job)
        self.assertFalse(status["unsafe_to_optimize"])
        self.assertEqual(status["resident_cleanup"]["state"], "proven")
        self.assertEqual(status["cleanup_recovery"]["previous_cleanup_error"],
                         "initial owned cleanup was not yet proven")
        self.assertIsNone(self.service.resident_validation)
        self.assertEqual(self.service.config["cadence"]["_manual_overrides"], {"spectre_model_dir": self.root})

    def test_nonresident_failure_after_resident_fallback_remains_unsafe(self):
        job = self._run_failed_callback(scope="nonresident_validation")
        self.assertTrue(self.service.range_validation_status(job)["unsafe_to_optimize"])
        with self.assertRaisesRegex(RuntimeError, "refusing to save EDA settings.*cleanup unproven"):
            self._save()

    def test_pending_exact_resident_close_is_retried_and_save_succeeds(self):
        job = self._pending_job()
        with mock.patch.object(self.session, "close", side_effect=self._proven_close) as close:
            self.assertTrue(self._save()["prepared_invalidated"])
        close.assert_called_once()
        self.assertGreater(close.call_args[1]["timeout_seconds"], 0)
        self.assertLessEqual(close.call_args[1]["timeout_seconds"], 0.05)
        self.assertFalse(self.service.range_validation_status(job)["unsafe_to_optimize"])

    def test_no_thread_is_recoverable_only_for_terminal_exact_bound_session(self):
        job = self._pending_job(tracked=False)
        with mock.patch.object(self.session, "close", side_effect=self._proven_close) as close:
            self.service._cancel_and_wait_range_validations(action="save EDA settings")
        close.assert_called_once()
        self.assertFalse(self.service.range_validation_status(job)["unsafe_to_optimize"])

    def test_running_untracked_job_remains_blocked(self):
        job = self._pending_job(tracked=False)
        self.service.range_validations[job]["state"] = "running"
        with mock.patch.object(self.session, "close") as close:
            with self.assertRaisesRegex(RuntimeError, "missing thread tracking"):
                self.service._cancel_and_wait_range_validations(action="save EDA settings")
        close.assert_not_called()

    def test_unknown_cleanup_source_is_not_cleared_by_matching_session(self):
        job = self._pending_job(scope="unknown")
        with mock.patch.object(self.session, "close") as close:
            with self.assertRaisesRegex(RuntimeError, "Cadence process cleanup unproven"):
                self.service._cancel_and_wait_range_validations()
        close.assert_not_called()
        self.assertTrue(self.service.range_validation_status(job)["unsafe_to_optimize"])

    def test_receipt_token_root_and_object_mismatch_never_close_foreign_session(self):
        for mismatch in ("token", "root", "object", "pointer", "binding_missing", "prepared_id", "job_root"):
            job = self._pending_job() if "range_validation_pending" not in self.service.range_validations else "range_validation_pending"
            self.service.range_validations[job]["prepared_id"] = "prepared"
            self.service.range_validations[job]["job_root"] = os.path.join(self.root, "range_validations", job)
            self.service._bind_range_validation_cleanup(job, self.session)
            self.service.resident_validation = self.session
            current = self.service.range_validations[job]
            current["unsafe_cleanup_scope"] = "resident_validation"
            current["unsafe_cleanup_session_token"] = self.session.token
            if mismatch == "token":
                current["unsafe_cleanup_session_token"] = "foreign-session"
            elif mismatch == "root":
                current["resident_cleanup"]["session_root"] = self.root
            elif mismatch == "object":
                self.service.range_validation_cleanup_sessions[job]["session"] = object()
            elif mismatch == "pointer":
                self.service.resident_validation = object()
            elif mismatch == "binding_missing":
                self.service.range_validation_cleanup_sessions.clear()
            elif mismatch == "prepared_id":
                current["prepared_id"] = "different-prepared-setup"
            else:
                current["job_root"] = self.root
            with mock.patch.object(self.session, "close") as close:
                with self.assertRaisesRegex(RuntimeError, "Cadence process cleanup unproven"):
                    self.service._cancel_and_wait_range_validations()
            close.assert_not_called()

    def test_live_callback_cannot_be_closed_while_it_still_owns_session(self):
        job = self._pending_job()
        release = threading.Event()
        thread = threading.Thread(target=release.wait)
        self.service.range_validation_threads[job] = thread
        thread.start()
        try:
            with mock.patch.object(self.session, "close") as close:
                with self.assertRaisesRegex(RuntimeError, "refusing to save EDA settings.*threads still alive"):
                    self.service._cancel_and_wait_range_validations(action="save EDA settings")
            close.assert_not_called()
        finally:
            release.set()
            thread.join(1)

    def test_exhausted_budget_does_not_begin_cleanup_retry(self):
        self._pending_job()
        with mock.patch.object(self.session, "close") as close:
            with self.assertRaisesRegex(RuntimeError, "Cleanup budget 0.000 seconds"):
                self.service._cancel_and_wait_range_validations(timeout_seconds=0)
        close.assert_not_called()

    def test_failed_retry_retains_owner_flag_and_settings(self):
        job = self._pending_job()
        before = self.service.config.copy()
        with mock.patch.object(self.session, "close", side_effect=RuntimeError("guardian token still unproven")):
            with self.assertRaisesRegex(RuntimeError, "refusing to save EDA settings"):
                self._save()
        self.assertTrue(self.service.range_validation_status(job)["unsafe_to_optimize"])
        self.assertIn("guardian token", self.service.range_validation_status(job)["cleanup_retry_error"])
        self.assertEqual(self.service.config, before)
        self.assertIs(self.service.resident_validation, self.session)

    def test_close_return_without_closed_state_cannot_clear_unsafe(self):
        job = self._pending_job()
        with mock.patch.object(self.session, "close", return_value={"closed": True, "cleanup_proven": True}):
            with self.assertRaisesRegex(RuntimeError, "cleanup unproven"):
                self.service._cancel_and_wait_range_validations()
        self.assertTrue(self.service.range_validation_status(job)["unsafe_to_optimize"])

    def test_unavailable_probe_with_exact_receipt_can_recover_without_thread(self):
        identity = self.service._owned_resident_cleanup_identity(self.session)
        response = self.service._record_optional_range_validation_unavailable(
            "prepared", [{"parameter_id": "w"}], "startup cleanup not yet proven",
            unsafe_to_optimize=True, resident_cleanup=dict(identity, state="pending"))
        self.assertNotIn(response["job_id"], self.service.range_validation_threads)
        with mock.patch.object(self.session, "close", side_effect=self._proven_close):
            self.assertTrue(self._save()["prepared_invalidated"])
        self.assertFalse(self.service.range_validation_status(response["job_id"])["unsafe_to_optimize"])

    def test_unavailable_probe_without_exact_receipt_cannot_borrow_current_session(self):
        response = self.service._record_optional_range_validation_unavailable(
            "prepared", [{"parameter_id": "w"}], "untracked cleanup",
            unsafe_to_optimize=True)
        with mock.patch.object(self.session, "close") as close:
            with self.assertRaisesRegex(RuntimeError, "missing thread tracking"):
                self._save()
        close.assert_not_called()
        self.assertTrue(self.service.range_validation_status(response["job_id"])["unsafe_to_optimize"])

    def test_startup_cleanup_failure_retains_exact_association_for_retry(self):
        error = self.service._resident_cleanup_error("startup guardian still live", self.session)
        with mock.patch("crescendo_engine.server.require_prepared_freshness", side_effect=error):
            response = self.service.validate_ranges("prepared", [{"parameter_id": "w", "minimum": 1, "maximum": 2}])
            self.service.range_validation_threads[response["job_id"]].join(1)
        self.assertEqual(self.service.range_validation_status(response["job_id"])["resident_cleanup"]["session_token"], self.session.token)
        with mock.patch.object(self.session, "close", side_effect=self._proven_close):
            self.assertTrue(self._save()["prepared_invalidated"])

    def test_error_reports_actual_operation_and_elapsed_not_an_elapsed_timeout(self):
        self._pending_job(scope="unknown")
        with mock.patch("crescendo_engine.server.time.monotonic", return_value=5.0):
            with self.assertRaises(RuntimeError) as caught:
                self.service._cancel_and_wait_range_validations(timeout_seconds=15, action="save EDA settings")
        message = str(caught.exception)
        self.assertIn("refusing to save EDA settings", message)
        self.assertIn("Cleanup budget 15.000 seconds; elapsed 0.000 seconds", message)
        self.assertNotIn("refusing to start optimization", message)
        self.assertNotIn("within 15", message)

    def test_exact_cleanup_recovers_prepared_capability_without_reloading(self):
        job = self._pending_job()
        identity = self.service._owned_resident_cleanup_identity(self.session)
        capability = {
            "prepared_id": "prepared", "state": "unavailable", "unsafe_to_optimize": True,
            "unsafe_cleanup_scope": "resident_validation", "unsafe_cleanup_session_token": self.session.token,
            "resident_cleanup": dict(identity, state="pending"),
        }
        self.service.prepared["prepared"]["resident_validation"] = capability
        self.service.prepared["other"] = {"resident_validation": copy.deepcopy(capability)}
        with mock.patch.object(self.session, "close", side_effect=self._proven_close):
            self.service._cancel_and_wait_range_validations()
        self.assertFalse(self.service.range_validation_status(job)["unsafe_to_optimize"])
        self.assertFalse(capability["unsafe_to_optimize"])
        self.service._require_prelaunch_safety(self.service.prepared["prepared"], "real")
        self.assertTrue(self.service.prepared["other"]["resident_validation"]["unsafe_to_optimize"])

    def test_unknown_or_nonresident_prepared_capability_is_not_cleared(self):
        self._pending_job()
        identity = self.service._owned_resident_cleanup_identity(self.session)
        for scope in ("unknown", "nonresident_validation"):
            capability = {
                "prepared_id": "prepared", "state": "unavailable", "unsafe_to_optimize": True,
                "unsafe_cleanup_scope": scope, "unsafe_cleanup_session_token": self.session.token,
                "resident_cleanup": dict(identity, state="pending"),
            }
            self.service.prepared["prepared"]["resident_validation"] = capability
            self.session.closed = False
            with mock.patch.object(self.session, "close", side_effect=self._proven_close):
                self.service._close_resident_validation()
            self.assertTrue(capability["unsafe_to_optimize"])
            with self.assertRaisesRegex(RuntimeError, "cleanup.*proven safe"):
                self.service._require_prelaunch_safety(self.service.prepared["prepared"], "real")
            self.service.resident_validation = self.session

    def test_source_models_are_consistent_across_effective_fields_and_both_profiles(self):
        directories = {}
        for solver in ("spectre", "alps"):
            directory = os.path.join(self.root, "pdk", solver)
            os.makedirs(directory)
            directories[solver] = directory
            with open(os.path.join(directory, "mos.scs"), "w") as stream:
                stream.write("section TT\nmodel nmos bsim4 vth0=.4\nendsection TT\n")
        self.service.config["cadence"] = {"spectre_bin": "/bin/true", "alps_bin": "/bin/true",
                                            "_manual_overrides": {}}
        before = copy.deepcopy(self.service.config)
        setup = {"tests": [{"name": "tb", "simulator": "spectre"}],
                 "model_groups": [{"name": "tt", "models": [{
                     "file": os.path.join(directories["spectre"], "mos.scs"), "section": "TT"}]}]}
        payload = self.service.eda_setup(setup)
        for solver, directory in directories.items():
            self.assertEqual(payload["effective"][solver + "_model_dir"], directory)
            self.assertEqual(payload["simulator_profiles"]["profiles"][solver]["model_dir"], directory)
            self.assertTrue(payload["sources"][solver + "_model_dir"])
        self.assertEqual(payload["manual"], {})
        self.assertEqual(self.service.config, before)

    def test_configured_runtime_alias_keeps_the_same_owned_cleanup_identity(self):
        real = os.path.join(self.root, "real-runtime")
        alias = os.path.join(self.root, "runtime-alias")
        os.makedirs(real)
        os.symlink(real, alias)
        service = OptimizerService({"runtime": {"run_root": alias}})
        session = ResidentValidationSession("prepared", {}, service.config,
            os.path.join(alias, "range_validation_resident", "prepared_prepared"))
        identity = service._owned_resident_cleanup_identity(session)
        self.assertEqual(identity["runtime_root"], real)
        self.assertEqual(identity["session_root"], os.path.join(real, "range_validation_resident", "prepared_prepared"))

    def test_saved_relative_include_path_freezes_same_models_for_run_and_diagnostics(self):
        from crescendo_engine.environment_test import _case_capability
        from crescendo_engine.native_environment import NATIVE_ENV_NAMES
        for name in ("spectre", "alps", "work", "view"):
            os.makedirs(os.path.join(self.root, name))
        for solver in ("spectre", "alps"):
            with open(os.path.join(self.root, solver, "toplevel.scs"), "w") as stream:
                stream.write("section TT\nmodel nmos bsim4\nendsection TT\n")
        source_db = os.path.join(self.root, "view", "maestro.sdb")
        with open(source_db, "w") as stream:
            stream.write('<root><field Name="includePath">"../spectre"</field></root>')
        self.service.config["cadence"] = {
            "project_dir": os.path.join(self.root, "work"), "ocean_bin": "/bin/true",
            "spectre_bin": "/bin/true", "alps_bin": "/bin/true",
            "launch_environment": {name: None for name in NATIVE_ENV_NAMES}, "_manual_overrides": {},
        }
        prepared = {"setup": {"source": {"setup_db": source_db},
            "tests": [{"name": "tb", "simulator": "spectre"}],
            "model_groups": [{"name": "tt", "models": [{"file": "toplevel.scs", "section": "TT"}]}]}}
        before, prepared_before = copy.deepcopy(self.service.config), copy.deepcopy(prepared)
        with mock.patch.dict(os.environ, {"PDK": "/different/gui/environment/pdk"}, clear=False):
            display = self.service.eda_setup(prepared["setup"])
            frozen = self.service._freeze_run_config({"evaluator": "mock", "simulator": "alps"}, prepared)
            for solver in ("spectre", "alps"):
                expected = os.path.join(self.root, solver)
                self.assertEqual(display["effective"][solver + "_model_dir"], expected)
                self.assertEqual(frozen["cadence"][solver + "_model_dir"], expected)
                self.assertTrue(_case_capability(frozen, prepared, solver, "private_netlist")["ready"])
        self.assertEqual(frozen["cadence"]["simulator"], "alps")
        self.assertEqual(frozen["cadence"]["_manual_overrides"], {})
        self.assertEqual(self.service.config, before)
        self.assertEqual(prepared, prepared_before)
        checkpoint = {"spec": {"evaluator": "mock", "simulator": "spectre"}, "manifest": {}}
        resumed = self.service._freeze_run_config({}, prepared, checkpoint=checkpoint)
        self.assertNotIn("spectre_model_dir", resumed["cadence"])
        self.assertNotIn("alps_model_dir", resumed["cadence"])


if __name__ == "__main__":
    unittest.main()
