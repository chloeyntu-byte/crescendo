"""Native bootstrap failures keep bounded evidence after owned epoch cleanup."""
import errno
import json
import os
import shutil
import tempfile
import types
import unittest
from unittest import mock

from crescendo_engine.persistent_maestro import (
    PersistentMaestroWorker, PersistentMaestroWorkerStartupError,
)
from crescendo_engine.runner import _WholeRunPersistentWorker, _worker_diagnostic_tail
from crescendo_engine.server import OptimizerService
from crescendo_engine.utils import load_json
from tests import test_persistent_maestro as native_fixture


class BootstrapFailureDiagnosticsTests(unittest.TestCase):
    def _failure(self, root, diagnostic_write_failure=False):
        fixture = native_fixture.PersistentMaestroTest()
        config = {
            "cadence": {"spectre_bin": "/eda/spectre", "ocean_bin": "/eda/ocean",
                        "cadence_root": "/eda/ic", "pdk_adapter": "generic-cdf",
                        "extra_binary_paths": [], "extra_library_paths": []},
            "runtime": {"ocean_startup_timeout_seconds": 5,
                        "candidate_timeout_seconds": 30},
        }
        parameter = fixture._parameter("p", "/M0", "/", [])
        worker = _WholeRunPersistentWorker(root, 0, {"setup": {}, "hierarchy": {}},
                                          [parameter], config)
        factory = native_fixture.FakePopenFactory("startup_error")
        clock = native_fixture.FakeClock()
        original_load = factory.on_load

        def on_load(process, path):
            original_load(process, path)
            if os.path.basename(path) == "bootstrap.il":
                marker = factory._request(path)["path"]
                with open(marker, "r", encoding="utf-8") as stream:
                    content = stream.read()
                with open(marker, "w", encoding="utf-8") as stream:
                    stream.write(content.replace("bootstrap failed", "bootstrap failed at job_policy_override"))
                output = factory.calls[-1][1]["stdout"]
                output.write(b"*Error* AUDIT_NATIVE_CAUSE: job policy readback unavailable\n")
                output.flush()

        factory.on_load = on_load

        def build_workspace(_setup, _hierarchy, _config, path, **kwargs):
            os.makedirs(path)
            workspace, _unused = fixture._fixture(path)
            return workspace

        def controller(workspace, cfg, parameters, worker_root, **kwargs):
            return PersistentMaestroWorker(
                workspace, cfg, parameters, worker_root,
                popen_factory=factory, clock=clock.monotonic, sleeper=clock.sleep,
                killpg=factory.killpg, getpgid=lambda pid: pid, **kwargs)

        with mock.patch("crescendo_engine.runner.build_workspace", side_effect=build_workspace), mock.patch(
                "crescendo_engine.runner.PersistentMaestroWorker", side_effect=controller):
            if diagnostic_write_failure:
                with mock.patch("crescendo_engine.runner.atomic_write_json",
                                side_effect=OSError(errno.EDQUOT, "Disk quota exceeded")):
                    try:
                        worker._new_epoch()
                    except PersistentMaestroWorkerStartupError as error:
                        return worker, factory, error
            else:
                try:
                    worker._new_epoch()
                except PersistentMaestroWorkerStartupError as error:
                    return worker, factory, error
        self.fail("the synthetic native startup failure must propagate")

    def test_native_cause_is_published_before_epoch_cleanup_and_survives(self):
        with tempfile.TemporaryDirectory() as root:
            worker, factory, error = self._failure(root)
            path = os.path.join(root, "startup_failure.json")
            document = load_json(path)
            self.assertEqual(document["schema_version"], "crescendo.startup_failure.v1")
            self.assertEqual(document["stage"], "job_policy_override")
            self.assertIn("AUDIT_NATIVE_CAUSE", document["native_error"])
            self.assertIn("AUDIT_NATIVE_CAUSE", str(error))
            self.assertIn(path, str(error))
            self.assertEqual(error.startup_diagnostic_path, path)
            self.assertTrue(worker.controller.closed)
            self.assertEqual(factory.processes[0].returncode, -15)
            self.assertEqual(worker.start_count, 0)
            epoch = worker.epoch_root

            def delete_epoch(epoch_root, _run_root, _source_guard):
                self.assertEqual(epoch_root, epoch)
                self.assertTrue(os.path.isfile(path))
                shutil.rmtree(epoch_root)
                return {"ok": True, "removed": True}

            with mock.patch("crescendo_engine.runner.verify_sources_unchanged", return_value={"ok": True}), mock.patch(
                    "crescendo_engine.runner.safe_delete_worker_epoch", side_effect=delete_epoch):
                retirement = worker.retire("synthetic bootstrap failure")
            self.assertTrue(retirement["termination_proven"])
            self.assertFalse(os.path.exists(epoch))
            self.assertEqual(load_json(path), document)

    def test_quota_failure_keeps_original_native_exception_and_termination(self):
        with tempfile.TemporaryDirectory() as root:
            worker, factory, error = self._failure(root, diagnostic_write_failure=True)
            self.assertEqual(str(error), "bootstrap failed at job_policy_override")
            self.assertIn("Disk quota exceeded", error.startup_diagnostic_write_error)
            self.assertFalse(os.path.exists(os.path.join(root, "startup_failure.json")))
            self.assertTrue(worker.controller.closed)
            self.assertEqual(factory.processes[0].returncode, -15)

    def test_diagnostic_tail_is_bounded_and_does_not_follow_links(self):
        with tempfile.TemporaryDirectory() as root:
            control = os.path.join(root, "control")
            os.mkdir(control)
            controller = types.SimpleNamespace(control_root=control, worker_root=root)
            with open(os.path.join(control, "ocean.stderr"), "wb") as stream:
                stream.write(b"A" * 20000)
            with open(os.path.join(control, "ocean.stdout"), "wb") as stream:
                stream.write(b"B" * 20000)
            diagnostic = _worker_diagnostic_tail(controller, 16384)
            self.assertEqual(sum(len(text.encode("utf-8")) for text in diagnostic.values()), 16384)
            self.assertEqual(set(diagnostic), {"ocean.stderr", "ocean.stdout"})
            with open(os.path.join(control, "ocean.stderr"), "wb") as stream:
                stream.write(b"\xff" * 20000)
            diagnostic = _worker_diagnostic_tail(controller, 16384)
            self.assertLessEqual(sum(len(text.encode("utf-8")) for text in diagnostic.values()), 16384)
            os.unlink(os.path.join(control, "ocean.stderr"))
            os.symlink(os.path.join(control, "ocean.stdout"), os.path.join(control, "ocean.stderr"))
            self.assertNotIn("ocean.stderr", _worker_diagnostic_tail(controller))
            os.unlink(os.path.join(control, "ocean.stderr"))
            os.link(os.path.join(control, "ocean.stdout"), os.path.join(control, "ocean.stderr"))
            self.assertEqual(_worker_diagnostic_tail(controller), {})

    def test_generated_bootstrap_tracks_exact_failure_phase_without_extra_states(self):
        with tempfile.TemporaryDirectory() as root:
            fixture = native_fixture.PersistentMaestroTest()
            worker, _factory, _clock = fixture._worker(root)
            worker.config["runtime"]["maestro_job_overrides"] = {"max_jobs": 2}
            identity = {"epoch": worker.epoch, "sequence": 0,
                        "nonce": "synthetic-nonce", "candidate_id": "startup"}
            path = worker._write_bootstrap(identity)
            with open(path, "r", encoding="utf-8") as stream:
                text = stream.read()
            phases = ("startup_load", "occurrence_clone", "pristine_capture",
                      "maestro_setup_open", "native_storage_readback", "native_corner_identity",
                      "history_overwrite", "job_policy_apply_readback")
            offsets = [text.index('crescBootstrapStage = "%s"' % phase) for phase in phases]
            self.assertEqual(offsets, sorted(offsets))
            self.assertIn('"STARTUP_ERROR" strcat("bootstrap failed at " crescBootstrapStage)', text)
            self.assertEqual(text.count('"STARTING"'), 1)

    def test_public_progress_reports_native_cause_after_logs_are_cleaned(self):
        with tempfile.TemporaryDirectory() as runtime:
            root = os.path.join(runtime, "run")
            os.mkdir(root)
            worker, _factory, error = self._failure(root)
            shutil.rmtree(worker.epoch_root)
            with open(os.path.join(root, "status.json"), "w", encoding="utf-8") as stream:
                json.dump({"run_id": "run", "run_root": root, "state": "failed",
                           "message": str(error)}, stream)
            progress = OptimizerService({"runtime": {"run_root": runtime}}).progress("run")
            execution = progress["execution"]
            self.assertIn("AUDIT_NATIVE_CAUSE", execution["error"]["startup_cause"])
            self.assertEqual(execution["error"]["startup_stage"], "job_policy_override")
            self.assertTrue(any("AUDIT_NATIVE_CAUSE" in row["text"] for row in execution["log_tails"]))


if __name__ == "__main__":
    unittest.main()
