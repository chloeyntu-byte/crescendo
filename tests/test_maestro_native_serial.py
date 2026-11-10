from __future__ import print_function
import json
import os
import tempfile
import unittest

import test_persistent_maestro as fixtures


def read_text(path):
    with open(path) as stream:
        return stream.read()


def read_json(path):
    return json.loads(read_text(path))


class NativeSerialFactory(fixtures.FakePopenFactory):
    def on_load(self, process, path):
        name = os.path.basename(path)
        if name == "persistent_worker_candidate.il":
            with open(os.path.join(os.path.dirname(path), "persistent_worker_candidate.json")) as stream:
                artifact = json.load(stream)
            if not artifact.get("native_serial"):
                return super(NativeSerialFactory, self).on_load(process, path)
            request = self._request(path)
            with open(artifact["pristine_restore_path"], "w") as stream:
                for row in artifact["pristine_cells"]:
                    stream.write("RESTORED\t%s\t%s\t%s\t%s\n" % (
                        row["library"], row["cell"], row["pristine_library"], row["pristine_cell"]))
            self._append(request, "ACCEPTED")
            self._append(request, "BEGIN")
            if self.mode == "prepare_error":
                self._append(request, "ERROR", message="cluster APIs unavailable")
                return
            self._append(request, "PATCHED")
            if self.mode == "serial_stopped":
                self.worker.request_batch_abort(reason="controlled native serial stop")
            elif self.mode != "stop_error":
                self._append(request, "SIM_DONE")
            return
        if name == "persistent_worker_candidate_finish.il":
            request = self._request(path)
            self._append(request, "RESULTS_READY", message=process.history_name)
            self._append(request, "DONE", message=process.history_name)
            return
        if name in ("persistent_worker_batch_abort.il", "startup_session_stop.il"):
            request = self._request(path)
            self._append(request, "STOP_BEGIN")
            self._append(request, "STOP_SENT")
            self._append(request, "STOP_ERROR" if self.mode == "stop_error" else "STOP_DONE")
            return
        return super(NativeSerialFactory, self).on_load(process, path)


class NativeSerialTest(unittest.TestCase):
    def worker(self, root, mode="success", required=True):
        worker, _factory, _clock = fixtures.PersistentMaestroTest()._worker(root)
        factory = NativeSerialFactory(mode)
        worker._popen_factory = factory
        worker._killpg = factory.killpg
        worker.config["runtime"]["native_cluster_stop_required"] = required
        worker.start()
        factory.worker = worker
        self.addCleanup(worker._close_streams)
        return worker, factory

    def test_native_serial_preserves_patch_history_and_cleanup_in_one_session(self):
        with tempfile.TemporaryDirectory() as root:
            worker, factory = self.worker(root)
            candidate = worker.evaluate("serial", {"p": 1e-6}, os.path.join(root, "candidate"))
            with open(candidate["script_path"]) as stream:
                script = stream.read()
            self.assertIn("?waitUntilDone nil", script)
            self.assertIn('?callback "crescCandidateFinished()"', script)
            self.assertEqual(script.count("runId = maeRunSimulation("), 1)
            self.assertNotIn("maeCreateTest(", script)
            self.assertIn("maeWaitUntilDone('All", read_text(candidate["finish_script_path"]))
            self.assertEqual(candidate["states"], list(worker.CANDIDATE_STATES))
            self.assertTrue(candidate["native_completion_proven"])
            self.assertTrue(candidate["simulation_submitted"])
            self.assertEqual(candidate["history_name"], factory.processes[0].history_name)
            state = read_json(worker.worker_state_path)
            self.assertTrue(state["remote_batch"]["native_completion_proven"])
            self.assertEqual(state["remote_batch"]["manifest_path"], candidate["manifest_path"])
            self.assertTrue(worker.request_cleanup(candidate)["ok"])
            pool = read_json(worker.worker_state_path)["remote_batch"]
            self.assertTrue(pool["simulation_submitted"])
            self.assertFalse(pool["no_remote_jobs_started"])
            self.assertFalse(pool["native_stop_proven"])
            self.assertEqual(len(factory.processes), 1)

    def test_local_serial_retains_existing_synchronous_path(self):
        with tempfile.TemporaryDirectory() as root:
            worker, _factory = self.worker(root, required=False)
            candidate = worker.evaluate("local", {"p": 1e-6}, os.path.join(root, "candidate"))
            self.assertIn("?waitUntilDone t", read_text(candidate["script_path"]))
            self.assertNotIn("native_serial", candidate)
            self.assertIsNone(read_json(worker.worker_state_path)["remote_batch"])

    def test_prepare_error_retains_possible_prestarted_pool_and_stop_api_is_failclosed(self):
        with tempfile.TemporaryDirectory() as root:
            worker, _factory = self.worker(root, mode="prepare_error")
            candidate = worker.evaluate("bad", {"p": 1e-6}, os.path.join(root, "candidate"))
            self.assertTrue(candidate["simulation_submitted"])
            self.assertFalse(candidate["no_remote_jobs_started"])
            self.assertFalse(candidate["native_completion_proven"])
            script = read_text(candidate["script_path"])
            self.assertLess(script.index("isCallable('maeStopSimulation)"), script.index(' "PATCHED"'))

    def test_native_stop_releases_serial_without_completion_callback(self):
        with tempfile.TemporaryDirectory() as root:
            worker, _factory = self.worker(root, mode="serial_stopped")
            with self.assertRaisesRegex(Exception, "native cancellation acknowledged"):
                worker.evaluate("stop", {"p": 1e-6}, os.path.join(root, "candidate"))
            self.assertTrue(worker.closed)
            self.assertTrue(read_json(worker.worker_state_path)["remote_batch"]["native_stop_proven"])

    def test_timeout_and_stop_failure_retain_unproven_remote_ownership(self):
        with tempfile.TemporaryDirectory() as root:
            worker, _factory = self.worker(root, mode="stop_error")
            with self.assertRaises(fixtures.PersistentMaestroWorkerTimeout):
                worker.evaluate("timeout", {"p": 1e-6}, os.path.join(root, "candidate"), timeout_seconds=.5)
            state = read_json(worker.worker_state_path)["remote_batch"]
            self.assertTrue(state["simulation_submitted"])
            self.assertFalse(state["no_remote_jobs_started"])
            self.assertFalse(state["native_completion_proven"])
            self.assertFalse(state["native_stop_proven"])
            proof = read_json(os.path.join(root, "candidate", "batch_stop_evidence.json"))
            self.assertFalse(proof["remote_stop_proven"])
            self.assertEqual(proof["status"][-1]["state"], "STOP_ERROR")
            script = read_text(os.path.join(root, "candidate", "persistent_worker_batch_abort.il"))
            self.assertIn("isCallable('maeStopSimulation)", script)

    def test_stop_during_prepare_fences_future_serial_submission(self):
        with tempfile.TemporaryDirectory() as root:
            worker, _factory = self.worker(root)
            stop = worker.request_batch_abort()
            self.assertTrue(stop["requested"])
            self.assertTrue(stop["remote_stop_proven"])
            self.assertFalse(stop.get("no_remote_jobs_started", False))
            with self.assertRaisesRegex(Exception, "aborted before submission"):
                worker.evaluate("stop", {"p": 1e-6}, os.path.join(root, "candidate"))
            self.assertFalse(os.path.exists(os.path.join(root, "candidate")))


if __name__ == "__main__":
    unittest.main()
