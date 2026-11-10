import json
import os
import shutil
import tempfile
import unittest
from unittest import mock

from crescendo_engine.server import OptimizerService


class CandidateLiveLogTest(unittest.TestCase):
    def _service(self, root):
        return OptimizerService({"runtime": {"run_root": root}})

    def _candidate(self, root, status="running", evidence=None):
        run_root = os.path.join(root, "run-1")
        candidate_root = os.path.join(run_root, "candidates", "g000_c0000")
        os.makedirs(candidate_root)
        with open(os.path.join(run_root, "status.json"), "w") as stream:
            json.dump({"schema_version": "crescendo.run_status.v1", "run_id": "run-1",
                       "run_root": run_root, "state": "running"}, stream)
        row = {
            "candidate_id": "g000_c0000", "generation": 0, "index": 0,
            "status": status,
        }
        if evidence is not None:
            row["completion_evidence"] = evidence
        with open(os.path.join(candidate_root, "candidate.json"), "w") as stream:
            json.dump(row, stream)
        return run_root, candidate_root

    @staticmethod
    def _write_log(candidate_root, point="1", test="AC", text="started\n"):
        path = os.path.join(
            candidate_root, "workspace", "simulation", "HH", "TB",
            "maestro", "results", "maestro", "History.1", point, test,
            "psf", "spectre.out",
        )
        os.makedirs(os.path.dirname(path))
        with open(path, "w") as stream:
            stream.write(text)
        return path

    def test_lists_candidate_owned_logs_without_exposing_paths(self):
        with tempfile.TemporaryDirectory() as root:
            evidence = {
                "spectre_runs": [{
                    "test": "AC", "corner": "TT", "point_id": 1,
                    "spectre": {"path": "simulation/HH/TB/maestro/results/maestro/History.1/1/AC/psf/spectre.out"},
                }],
            }
            _run_root, candidate_root = self._candidate(root, evidence=evidence)
            self._write_log(candidate_root)
            self._write_log(candidate_root, point="psf", text="alias\n")
            payload = self._service(root).candidate_logs("run-1", "g000_c0000")
            self.assertEqual(payload["state"], "available")
            self.assertEqual(len(payload["logs"]), 1)
            self.assertEqual(payload["logs"][0]["label"], "AC · TT")
            self.assertEqual(payload["logs"][0]["kind"], "spectre")
            self.assertNotIn("path", payload["logs"][0])
            self.assertNotIn("relative_path", payload["logs"][0])

    def test_preserves_corner_context_for_point_zero(self):
        with tempfile.TemporaryDirectory() as root:
            evidence = {
                "spectre_runs": [{
                    "test": "AC", "corner": "FF", "point_id": 0,
                    "spectre": {"path": "simulation/results/0/AC/psf/spectre.out"},
                }],
            }
            _run_root, candidate_root = self._candidate(root, evidence=evidence)
            self._write_log(candidate_root, point="0")
            payload = self._service(root).candidate_logs("run-1", "g000_c0000")
            self.assertEqual(payload["logs"][0]["label"], "AC · FF")

    def test_reads_appends_and_resets_after_log_replacement(self):
        with tempfile.TemporaryDirectory() as root:
            _run_root, candidate_root = self._candidate(root)
            path = self._write_log(candidate_root, text="first\n")
            service = self._service(root)
            log_id = service.candidate_logs("run-1", "g000_c0000")["logs"][0]["log_id"]
            first = service.read_candidate_log_chunk(
                "run-1", "g000_c0000", log_id, max_bytes=1024,
            )
            self.assertEqual(first["text"], "first\n")
            self.assertFalse(first["reset"])
            with open(path, "a") as stream:
                stream.write("second\n")
            second = service.read_candidate_log_chunk(
                "run-1", "g000_c0000", log_id, offset=first["next_offset"],
                file_token=first["file_token"], max_bytes=1024,
            )
            self.assertEqual(second["text"], "second\n")
            os.unlink(path)
            with open(path, "w") as stream:
                stream.write("replacement\n")
            replacement = service.read_candidate_log_chunk(
                "run-1", "g000_c0000", log_id, offset=second["next_offset"],
                file_token=second["file_token"], max_bytes=1024,
            )
            self.assertTrue(replacement["reset"])
            self.assertEqual(replacement["text"], "replacement\n")

    def test_resets_after_in_place_truncate_and_rewrite(self):
        with tempfile.TemporaryDirectory() as root:
            _run_root, candidate_root = self._candidate(root)
            path = self._write_log(candidate_root, text="old")
            service = self._service(root)
            log_id = service.candidate_logs("run-1", "g000_c0000")["logs"][0]["log_id"]
            first = service.read_candidate_log_chunk(
                "run-1", "g000_c0000", log_id, max_bytes=1024,
            )
            with open(path, "w") as stream:
                stream.write("NEW-CONTENT")
            rewritten = service.read_candidate_log_chunk(
                "run-1", "g000_c0000", log_id, offset=first["next_offset"],
                file_token=first["file_token"], max_bytes=1024,
            )
            self.assertTrue(rewritten["reset"])
            self.assertEqual(rewritten["text"], "NEW-CONTENT")

    def test_finds_namespaced_persistent_controller_worker_log(self):
        with tempfile.TemporaryDirectory() as root:
            run_root, candidate_root = self._candidate(root)
            identity = {
                "epoch": "worker-000-controller-000007-epoch-000003",
                "sequence": 1, "nonce": "candidate-a", "candidate_id": "g000_c0000",
            }
            with open(os.path.join(candidate_root, "persistent_worker_candidate.json"), "w") as stream:
                json.dump({
                    "candidate_id": "g000_c0000", "worker_index": 0,
                    "worker_epoch": "worker-000-controller-000007-epoch-000003",
                    "protocol_identity": identity,
                }, stream)
            epoch_root = os.path.join(
                run_root, "workers", "worker_000", "epoch_c000007_l000003",
            )
            os.makedirs(epoch_root)
            with open(os.path.join(epoch_root, "worker_state.json"), "w") as stream:
                json.dump({
                    "schema_version": "crescendo.persistent_maestro_worker.v1",
                    "worker_index": 0, "epoch": identity["epoch"],
                    "state": "busy", "active_identity": identity,
                    "pending_cleanup": None,
                }, stream)
            path = os.path.join(
                epoch_root,
                "workspace", "simulation", "HH", "TB", "maestro", "results",
                "maestro", "History.1", "1", "AC", "psf", "spectre.out",
            )
            os.makedirs(os.path.dirname(path))
            with open(path, "w") as stream:
                stream.write("controller log\n")
            service = self._service(root)
            payload = service.candidate_logs("run-1", "g000_c0000")
            self.assertEqual(payload["state"], "available")
            self.assertEqual(len(payload["logs"]), 1)
            chunk = service.read_candidate_log_chunk(
                "run-1", "g000_c0000", payload["logs"][0]["log_id"],
            )
            self.assertEqual(chunk["text"], "controller log\n")

    def test_rejects_reused_worker_workspace_owned_by_next_candidate(self):
        with tempfile.TemporaryDirectory() as root:
            run_root, candidate_root = self._candidate(root, status="completed")
            identity_a = {
                "epoch": "worker-000-controller-000007-epoch-000003",
                "sequence": 1, "nonce": "candidate-a", "candidate_id": "g000_c0000",
            }
            identity_b = dict(identity_a, sequence=2, nonce="candidate-b", candidate_id="g000_c0001")
            with open(os.path.join(candidate_root, "persistent_worker_candidate.json"), "w") as stream:
                json.dump({
                    "candidate_id": "g000_c0000", "worker_index": 0,
                    "worker_epoch": identity_a["epoch"],
                    "protocol_identity": identity_a,
                }, stream)
            epoch_root = os.path.join(
                run_root, "workers", "worker_000", "epoch_c000007_l000003",
            )
            os.makedirs(epoch_root)
            with open(os.path.join(epoch_root, "worker_state.json"), "w") as stream:
                json.dump({
                    "schema_version": "crescendo.persistent_maestro_worker.v1",
                    "worker_index": 0, "epoch": identity_a["epoch"],
                    "state": "busy", "active_identity": identity_b,
                    "pending_cleanup": None,
                }, stream)
            path = os.path.join(
                epoch_root, "workspace", "simulation", "HH", "TB", "maestro",
                "results", "maestro", "History.2", "1", "AC", "psf", "spectre.out",
            )
            os.makedirs(os.path.dirname(path))
            with open(path, "w") as stream:
                stream.write("candidate b\n")
            payload = self._service(root).candidate_logs("run-1", "g000_c0000")
            self.assertEqual(payload["state"], "removed")
            self.assertEqual(payload["logs"], [])

    def test_reports_waiting_then_removed_after_zero_retention_cleanup(self):
        with tempfile.TemporaryDirectory() as root:
            _run_root, candidate_root = self._candidate(root)
            service = self._service(root)
            waiting = service.candidate_logs("run-1", "g000_c0000")
            self.assertEqual(waiting["state"], "waiting")
            path = self._write_log(candidate_root)
            log_id = service.candidate_logs("run-1", "g000_c0000")["logs"][0]["log_id"]
            shutil.rmtree(os.path.join(candidate_root, "workspace"))
            candidate_path = os.path.join(candidate_root, "candidate.json")
            with open(candidate_path, "r") as stream:
                row = json.load(stream)
            row["status"] = "completed"
            with open(candidate_path, "w") as stream:
                json.dump(row, stream)
            removed = service.candidate_logs("run-1", "g000_c0000")
            self.assertEqual(removed["state"], "removed")
            chunk = service.read_candidate_log_chunk(
                "run-1", "g000_c0000", log_id, offset=os.path.getsize(path) if os.path.exists(path) else 0,
            )
            self.assertTrue(chunk["removed"])
            self.assertEqual(chunk["state"], "removed")

    def test_reads_retained_log_after_candidate_workspace_is_removed(self):
        with tempfile.TemporaryDirectory() as root:
            _run_root, candidate_root = self._candidate(root, status="completed")
            log_path = os.path.join(
                candidate_root, "retained_spectre_token.log",
            )
            meta_path = os.path.join(
                candidate_root, "retained_spectre_token.meta",
            )
            with open(log_path, "w") as stream:
                stream.write("retained candidate log\n")
            with open(meta_path, "w") as stream:
                stream.write("\n".join((
                    "schema_version=crescendo.retained_spectre_log.v1",
                    "source_path=/private/results/1/AC/psf/spectre.out",
                    "returncode=0",
                    "finished_epoch=1.0",
                )) + "\n")
            service = self._service(root)
            payload = service.candidate_logs("run-1", "g000_c0000")
            self.assertEqual(payload["state"], "available")
            self.assertEqual(len(payload["logs"]), 1)
            self.assertEqual(payload["logs"][0]["kind"], "spectre_retained")
            self.assertEqual(payload["logs"][0]["label"], "AC · point 1")
            chunk = service.read_candidate_log_chunk(
                "run-1", "g000_c0000", payload["logs"][0]["log_id"],
            )
            self.assertEqual(chunk["text"], "retained candidate log\n")

    def test_retained_logs_use_compact_completion_corner_context(self):
        with tempfile.TemporaryDirectory() as root:
            _run_root, candidate_root = self._candidate(root, status="completed")
            candidate_path = os.path.join(candidate_root, "candidate.json")
            with open(candidate_path) as stream:
                candidate = json.load(stream)
            candidate["completion"] = {
                "spectre_logs": [
                    {"test": "AC", "corner": "TT", "point_id": 1},
                    {"test": "AC", "corner": "FF", "point_id": 2},
                ],
            }
            with open(candidate_path, "w") as stream:
                json.dump(candidate, stream)
            for token, point in (("one", 1), ("two", 2)):
                with open(os.path.join(
                        candidate_root, "retained_spectre_%s.log" % token,
                ), "w") as stream:
                    stream.write("point %s\n" % point)
                with open(os.path.join(
                        candidate_root, "retained_spectre_%s.meta" % token,
                ), "w") as stream:
                    stream.write("\n".join((
                        "schema_version=crescendo.retained_spectre_log.v1",
                        "source_path=/private/results/%s/AC/psf/spectre.out" % point,
                        "returncode=0",
                    )) + "\n")
            labels = sorted(
                row["label"] for row in self._service(root).candidate_logs(
                    "run-1", "g000_c0000",
                )["logs"]
            )
            self.assertEqual(labels, ["AC · FF", "AC · TT"])

    def test_fresh_real_launch_preserves_prior_run_diagnostics(self):
        with tempfile.TemporaryDirectory() as root:
            run_root, candidate_root = self._candidate(root, status="completed")
            retained = os.path.join(candidate_root, "retained_spectre_old.log")
            with open(retained, "w") as stream:
                stream.write("old log\n")
            with open(os.path.join(run_root, "checkpoint.json"), "w") as stream:
                json.dump({"keep": True}, stream)
            service = self._service(root)
            lease = mock.Mock()
            lease.acquire.return_value = lease
            with mock.patch(
                    "crescendo_engine.server.startup_storage_preflight",
                    return_value={}), mock.patch(
                    "crescendo_engine.server.RuntimeOptimizationLease",
                    return_value=lease), mock.patch.object(
                    service, "_cancel_and_wait_range_validations"), mock.patch.object(
                    service, "_launch_registered",
                    return_value={"run_id": "optimization_new", "run_root": "unused"}):
                service._launch({"evaluator": "real"}, {})
            self.assertTrue(os.path.exists(retained))
            self.assertTrue(os.path.isfile(os.path.join(run_root, "checkpoint.json")))

    def test_invalid_fresh_launch_does_not_clear_retained_logs(self):
        with tempfile.TemporaryDirectory() as root:
            _run_root, candidate_root = self._candidate(root, status="completed")
            retained = os.path.join(candidate_root, "retained_spectre_old.log")
            with open(retained, "w") as stream:
                stream.write("old log\n")
            service = self._service(root)
            with self.assertRaisesRegex(ValueError, "display_name"):
                service._launch({
                    "evaluator": "real", "display_name": "unsafe\nname",
                }, {})
            self.assertTrue(os.path.isfile(retained))

    def test_safe_exit_clears_session_logs_without_touching_checkpoint(self):
        with tempfile.TemporaryDirectory() as root:
            run_root, candidate_root = self._candidate(root, status="completed")
            retained = os.path.join(candidate_root, "retained_spectre_current.log")
            with open(retained, "w") as stream:
                stream.write("current log\n")
            checkpoint = os.path.join(run_root, "checkpoint.json")
            with open(checkpoint, "w") as stream:
                json.dump({"keep": True}, stream)
            service = self._service(root)
            service.session_run_names.add("run-1")
            result = service.exit_optimizer()
            self.assertEqual(result["retained_log_cleanup"]["removed_count"], 1)
            self.assertFalse(os.path.exists(retained))
            self.assertTrue(os.path.isfile(checkpoint))

    def test_skips_symlink_logs_and_rejects_unsafe_ids(self):
        with tempfile.TemporaryDirectory() as root:
            _run_root, candidate_root = self._candidate(root)
            outside = os.path.join(root, "outside.log")
            with open(outside, "w") as stream:
                stream.write("secret\n")
            path = os.path.join(
                candidate_root, "workspace", "simulation", "HH", "TB",
                "maestro", "results", "maestro", "History.1", "1", "AC",
                "psf", "spectre.out",
            )
            os.makedirs(os.path.dirname(path))
            os.symlink(outside, path)
            service = self._service(root)
            self.assertEqual(
                service.candidate_logs("run-1", "g000_c0000")["logs"], [],
            )
            with self.assertRaises(ValueError):
                service.candidate_logs("run-1", "../g000_c0000")
            with self.assertRaises(ValueError):
                service.read_candidate_log_chunk(
                    "run-1", "g000_c0000", "../../outside.log",
                )


    def test_pre_spectre_workspace_failure_reports_diagnostics_instead_of_removed_log(self):
        with tempfile.TemporaryDirectory() as root:
            _run_root, candidate_root = self._candidate(root, status="failed")
            candidate_path = os.path.join(candidate_root, "candidate.json")
            with open(candidate_path, "r") as stream:
                row = json.load(stream)
            row.update({
                "failure_class": "design_or_simulation",
                "error": (
                    "WorkspaceBuildFailure: workspace build failed: "
                    "Permission denied: maestro.sdb"
                ),
            })
            with open(candidate_path, "w") as stream:
                json.dump(row, stream)
            payload = self._service(root).candidate_logs(
                "run-1", "g000_c0000",
            )
            self.assertEqual(payload["state"], "not_started")
            self.assertEqual(payload["failure_stage"], "workspace_build")
            self.assertEqual(payload["spectre_state"], "not_started")
            self.assertIn("Permission denied", payload["failure_reason"])
            self.assertIn("Spectre was not started", payload["message"])
            self.assertEqual(payload["logs"], [])


if __name__ == "__main__":
    unittest.main()
