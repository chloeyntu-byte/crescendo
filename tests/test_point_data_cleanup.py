"""Owned Python simulator fixtures test protocol; these are not Spectre runs."""
import os
import sys
import tempfile
import unittest
from unittest import mock

from crescendo_engine.netlist_execution import NetlistExecutionContext, NetlistExecutionError
from crescendo_engine.run_data_store import RunDataStore, load_dataset
from crescendo_engine.utils import atomic_write_text, sha256_file


class PointDataCleanupTest(unittest.TestCase):
    def fixture(self, root, callback, measurement=None):
        task_root = os.path.join(root, "tasks", "p1")
        cwd = os.path.join(task_root, "netlist")
        os.makedirs(cwd)
        psf = os.path.join(task_root, "psf")
        os.makedirs(psf)
        netlist = os.path.join(cwd, "input.scs")
        atomic_write_text(netlist, "simulator lang=spectre\n")
        log = os.path.join(psf, "spectre.out")
        script = os.path.join(cwd, "fixture.py")
        atomic_write_text(script, "open(%r,'w').write('spectre completes with 0 errors.\\n')\nopen(%r,'w').write('fixture')\n" %
                          (log, os.path.join(psf, "logFile")))
        task = {"task_id": "p1", "test": "rc", "corner": "sweep", "point_id": 1,
                "netlist_path": netlist, "netlist_sha256": sha256_file(netlist), "cwd": cwd,
                "psf_dir": psf, "spectre_log": log, "argv": [sys.executable, script, netlist],
                "outputs": [{"key": "rc::gain", "name": "gain", "expression": "1.0"}]}
        manifest = {"root": root, "candidate_id": "g000_c0000", "template_sha256": "qualified", "tasks": [task]}
        prepared = {"job_policy": {"available": True, "global": {"properties": {
            "distributionmethod": "Local", "maxjobs": "1"}}, "tests": []}}
        evaluator = measurement or (lambda task, config, root, **kwargs: {
            "outputs": [dict(task["outputs"][0], value=3.0, error=None)]})
        context = NetlistExecutionContext({"cadence": {}, "runtime": {}}, prepared,
            {"objectives": [{"output_key": "rc::gain"}], "candidate_timeout_seconds": 3},
            measurement_evaluator=evaluator, owned_run_root=root, point_commit_callback=callback,
            ownership_root=os.path.join(root, "ownership"))
        self.addCleanup(context.close, True)
        return context, manifest, task

    def test_native_point_stops_then_durable_ack_before_payload_delete(self):
        with tempfile.TemporaryDirectory() as root:
            writer = RunDataStore(os.path.join(root, "optimization.crescendo"), {"spec": {}})
            self.addCleanup(writer.close)
            writer.commit_candidates([{"candidate_id": "g000_c0000", "generation": 0, "index": 0,
                                       "values": {"p": 1}, "status": "planned"}])
            def commit(point, _cancel):
                self.assertTrue(point["process_termination_proven"])
                self.assertTrue(point["remote_stop_proven"])
                self.assertFalse(point["candidate_completed"])
                self.assertTrue(os.path.exists(os.path.join(root, "tasks", "p1", "psf", "logFile")))
                revision = writer.append_event("point", point)
                self.assertEqual(load_dataset(writer.path)["candidates"][0]["status"], "planned")
                return {"ok": True, "revision": revision, "checkpoint_sha256": "owner_ack"}
            context, manifest, task = self.fixture(root, commit)
            result = context.evaluate(manifest)
            self.assertEqual(result["parsed"]["output_map"]["rc::gain"]["summary"]["mean"], 3.0)
            self.assertFalse(os.path.exists(task["psf_dir"]))
            self.assertFalse(os.path.exists(task["cwd"]))
            self.assertTrue(context.close()["cleanup_proven"])

    def test_write_failure_or_missing_ack_keeps_raw(self):
        for failure in (True, False):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as root:
                def commit(point, cancel):
                    if failure:
                        raise OSError(122, "quota exceeded")
                    return {"ok": False, "state": "no_checkpoint_ack"}
                context, manifest, task = self.fixture(root, commit)
                if failure:
                    with self.assertRaises(OSError):
                        context.evaluate(manifest)
                else:
                    context.evaluate(manifest)
                self.assertTrue(os.path.exists(task["psf_dir"]))
                self.assertTrue(os.path.exists(task["netlist_path"]))
                self.assertTrue(context.close()["cleanup_proven"])

    def test_missing_terminal_receipt_never_commits_or_cleans(self):
        with tempfile.TemporaryDirectory() as root:
            committed = mock.Mock()
            def measurement(task, config, boundary, **kwargs):
                os.unlink(os.path.join(os.path.dirname(task["psf_dir"]), "execution", "terminal.receipt"))
                return {"outputs": [dict(task["outputs"][0], value=3.0, error=None)]}
            context, manifest, task = self.fixture(root, committed, measurement)
            with self.assertRaises(NetlistExecutionError):
                context.evaluate(manifest)
            committed.assert_not_called()
            self.assertTrue(os.path.exists(task["psf_dir"]))
            self.assertFalse(context.close()["cleanup_proven"])

    def test_symlink_or_artifact_delete_error_preserves_truthful_stop_proof(self):
        for unsafe_link in (True, False):
            with self.subTest(unsafe_link=unsafe_link), tempfile.TemporaryDirectory() as root:
                def commit(point, cancel):
                    if unsafe_link:
                        os.symlink('/tmp', os.path.join(root, "tasks", "p1", "external"))
                    return {"ok": True, "revision": 2, "checkpoint_sha256": "owner_ack"}
                context, manifest, task = self.fixture(root, commit)
                if unsafe_link:
                    result = context.evaluate(manifest)
                else:
                    with mock.patch('crescendo_engine.netlist_execution.shutil.rmtree', side_effect=OSError(13, "permission denied")):
                        result = context.evaluate(manifest)
                cleanup = result["task_results"][0]["point_raw_cleanup"]
                self.assertFalse(cleanup["artifact_cleanup_complete"])
                self.assertTrue(os.path.exists(task["psf_dir"]))
                self.assertTrue(context.close()["cleanup_proven"])


if __name__ == '__main__':
    unittest.main()
