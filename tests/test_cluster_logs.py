import copy
import json
import os
import shutil
import tempfile
import unittest

from crescendo_engine.server import OptimizerService


class ClusterCandidateLogTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="crescendo-cluster-logs-")
        self.run = os.path.join(self.root, "run-1")
        self.epoch = os.path.join(self.run, "workers", "worker_000", "epoch_c000007_l000003")
        self.simulation = os.path.join(self.epoch, "workspace", "simulation")
        self.identity = {
            "epoch": "worker-000-controller-000007-epoch-000003",
            "candidate_id": "batch_1_abc", "sequence": 1, "nonce": "unique-batch",
        }
        self.service = OptimizerService({"runtime": {"run_root": self.root}})
        self.write_json(os.path.join(self.run, "status.json"), {"state": "running"})
        self.state = {
            "schema_version": "crescendo.persistent_maestro_worker.v1",
            "worker_index": 0, "epoch": self.identity["epoch"],
            "state": "busy", "active_identity": self.identity,
        }
        self.write_json(os.path.join(self.epoch, "worker_state.json"), self.state)
        self.rows, self.artifacts, self.logs = {}, {}, {}
        for index in (0, 1):
            candidate_id = "g000_c000%d" % index
            alias = "cr_candidate%d_vco" % index
            mapping = {alias: "vco_nominal"}
            candidate_root = os.path.join(self.run, "candidates", candidate_id)
            row = {
                "candidate_id": candidate_id, "status": "running",
                "batch_execution": {"batch_id": self.identity["candidate_id"],
                                    "source_test_map": mapping, "shared_history": True},
                "completion_evidence": {
                    "candidate_test_name_map": mapping,
                    "spectre_runs": [
                        {"test": "vco_nominal", "artifact_test": alias,
                         "point_id": point, "corner": corner,
                         "spectre": {"path": "results/History.17/%d/%s/psf/spectre.out" % (point, alias)}}
                        for point, corner in ((0, "TT"), (1, "FF"))
                    ],
                },
            }
            artifact = {
                "candidate_id": candidate_id, "worker_index": 0,
                "worker_epoch": self.identity["epoch"], "protocol_identity": self.identity,
                "batch_id": self.identity["candidate_id"], "mode": "isolated_design_tests",
                "shared_history": True, "source_test_map": mapping,
            }
            self.rows[candidate_id], self.artifacts[candidate_id] = row, artifact
            self.write_json(os.path.join(candidate_root, "candidate.json"), row)
            self.write_json(os.path.join(candidate_root, "persistent_worker_candidate.json"), artifact)
            self.logs[candidate_id] = []
            for point in (0, 1):
                path = os.path.join(self.simulation, "HH", "TB", "maestro", "results", "maestro",
                                    "History.17", str(point), alias, "psf", "spectre.out")
                self.write_text(path, "candidate %d point %d\n" % (index, point))
                self.logs[candidate_id].append(path)

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    @staticmethod
    def write_text(path, value):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as stream:
            stream.write(value)

    @classmethod
    def write_json(cls, path, value):
        cls.write_text(path, json.dumps(value))

    def update_candidate(self, candidate_id):
        self.write_json(os.path.join(self.run, "candidates", candidate_id, "candidate.json"),
                        self.rows[candidate_id])

    def test_batch_live_api_reads_only_requested_candidate_and_labels_original_test(self):
        for index in (0, 1):
            candidate_id = "g000_c000%d" % index
            payload = self.service.candidate_logs("run-1", candidate_id)
            self.assertEqual(payload["state"], "available")
            self.assertEqual(sorted(row["label"] for row in payload["logs"]),
                             ["vco_nominal · FF", "vco_nominal · TT"])
            self.assertTrue(all(row["test"] == "vco_nominal" for row in payload["logs"]))
            for row in payload["logs"]:
                self.assertNotIn("relative_path", row)
                text = self.service.read_candidate_log_chunk("run-1", candidate_id, row["log_id"])["text"]
                self.assertIn("candidate %d" % index, text)
                self.assertNotIn("candidate %d" % (1 - index), text)
        other_id = self.service.candidate_logs("run-1", "g000_c0001")["logs"][0]["log_id"]
        with self.assertRaisesRegex(ValueError, "belongs to this candidate"):
            self.service.read_candidate_log_chunk("run-1", "g000_c0000", other_id)

    def test_active_candidate_without_completion_uses_artifact_original_test_names(self):
        row = self.rows["g000_c0000"]
        row.pop("completion_evidence")
        row.pop("batch_execution")
        self.update_candidate("g000_c0000")
        payload = self.service.candidate_logs("run-1", "g000_c0000")
        self.assertEqual(len(payload["logs"]), 2)
        self.assertTrue(all(log["test"] == "vco_nominal" for log in payload["logs"]))

    def test_retained_after_cleanup_keeps_mapping_corner_and_filters_wrong_candidate(self):
        for candidate_id in self.rows:
            candidate_root = os.path.join(self.run, "candidates", candidate_id)
            row = self.rows[candidate_id]
            row["status"] = "completed"
            evidence = row.pop("completion_evidence")
            row["completion"] = {
                "candidate_test_name_map": evidence["candidate_test_name_map"],
                "spectre_logs": [{k: v for k, v in entry.items() if k != "spectre"}
                                 for entry in evidence["spectre_runs"]],
            }
            self.update_candidate(candidate_id)
            os.unlink(os.path.join(candidate_root, "persistent_worker_candidate.json"))
            for point, path in enumerate(self.logs[candidate_id]):
                self.write_text(os.path.join(candidate_root, "retained_spectre_own%d.log" % point),
                                "retained %s point %d\n" % (candidate_id, point))
                self.write_text(os.path.join(candidate_root, "retained_spectre_own%d.meta" % point),
                                "schema_version=crescendo.retained_spectre_log.v1\nsource_path=%s\nreturncode=0\n" % path)
            other_id = "g000_c0001" if candidate_id == "g000_c0000" else "g000_c0000"
            self.write_text(os.path.join(candidate_root, "retained_spectre_wrong.log"), "WRONG CANDIDATE\n")
            self.write_text(os.path.join(candidate_root, "retained_spectre_wrong.meta"),
                            "schema_version=crescendo.retained_spectre_log.v1\nsource_path=%s\n" % self.logs[other_id][0])
        shutil.rmtree(self.epoch)
        for candidate_id in self.rows:
            logs = self.service.candidate_logs("run-1", candidate_id)["logs"]
            self.assertEqual(len(logs), 2)
            self.assertEqual(sorted(log["label"] for log in logs), ["vco_nominal · FF", "vco_nominal · TT"])
            for log in logs:
                self.assertEqual(log["kind"], "spectre_retained")
                text = self.service.read_candidate_log_chunk("run-1", candidate_id, log["log_id"])["text"]
                self.assertIn(candidate_id, text)
                self.assertNotIn("WRONG", text)

    def test_fenced_batch_identity_cannot_read_next_batch_or_wrong_candidate(self):
        self.state["active_identity"] = dict(self.identity, sequence=2, nonce="next-batch")
        self.write_json(os.path.join(self.epoch, "worker_state.json"), self.state)
        self.assertEqual(self.service.candidate_logs("run-1", "g000_c0000")["logs"], [])
        self.state["active_identity"] = self.identity
        self.write_json(os.path.join(self.epoch, "worker_state.json"), self.state)
        artifact = copy.deepcopy(self.artifacts["g000_c0000"])
        artifact["protocol_identity"]["candidate_id"] = "wrong_batch"
        self.write_json(os.path.join(self.run, "candidates", "g000_c0000", "persistent_worker_candidate.json"), artifact)
        self.assertEqual(self.service.candidate_logs("run-1", "g000_c0000")["logs"], [])

    def test_pending_cleanup_fence_still_exposes_only_candidate_aliases(self):
        self.state.update(state="awaiting_cleanup", pending_cleanup=self.identity, active_identity=None)
        self.write_json(os.path.join(self.epoch, "worker_state.json"), self.state)
        self.assertEqual(len(self.service.candidate_logs("run-1", "g000_c0000")["logs"]), 2)

    def test_bad_maps_paths_and_nonbijective_maps_are_rejected(self):
        candidate_id = "g000_c0000"
        original = copy.deepcopy(self.rows[candidate_id])
        for bad_map in ({"../escape": "vco_nominal"}, {"one": "same", "two": "same"},
                        {"other_alias": "vco_nominal"}, {}):
            self.rows[candidate_id] = copy.deepcopy(original)
            self.rows[candidate_id]["batch_execution"]["source_test_map"] = bad_map
            self.update_candidate(candidate_id)
            with self.subTest(mapping=bad_map), self.assertRaises(ValueError):
                self.service.candidate_logs("run-1", candidate_id)
        self.rows[candidate_id] = copy.deepcopy(original)
        self.rows[candidate_id]["completion_evidence"]["spectre_runs"][0]["artifact_test"] = "other_alias"
        self.update_candidate(candidate_id)
        with self.assertRaisesRegex(ValueError, "mapping disagrees"):
            self.service.candidate_logs("run-1", candidate_id)

    def test_bad_batch_mode_or_identity_cannot_fall_back_to_unfiltered_serial_logs(self):
        candidate_id = "g000_c0000"
        candidate_root = os.path.join(self.run, "candidates", candidate_id)
        self.rows[candidate_id].pop("batch_execution")
        self.rows[candidate_id].pop("completion_evidence")
        self.update_candidate(candidate_id)
        artifact = copy.deepcopy(self.artifacts[candidate_id])
        artifact["mode"] = "unknown"
        artifact["protocol_identity"]["candidate_id"] = candidate_id
        self.write_json(os.path.join(candidate_root, "persistent_worker_candidate.json"), artifact)
        with self.assertRaisesRegex(ValueError, "invalid candidate batch log artifact"):
            self.service.candidate_logs("run-1", candidate_id)
        artifact = copy.deepcopy(self.artifacts[candidate_id])
        artifact["protocol_identity"] = ["invalid"]
        self.write_json(os.path.join(candidate_root, "persistent_worker_candidate.json"), artifact)
        self.assertEqual(self.service.candidate_logs("run-1", candidate_id)["logs"], [])

    def test_retained_symlink_metadata_and_hardlinked_logs_are_not_exposed(self):
        candidate_id = "g000_c0000"
        candidate_root = os.path.join(self.run, "candidates", candidate_id)
        self.rows[candidate_id]["status"] = "completed"
        self.update_candidate(candidate_id)
        os.unlink(os.path.join(candidate_root, "persistent_worker_candidate.json"))
        shutil.rmtree(self.epoch)
        meta = os.path.join(self.root, "outside.meta")
        self.write_text(meta, "schema_version=crescendo.retained_spectre_log.v1\nsource_path=%s\n" % self.logs[candidate_id][0])
        self.write_text(os.path.join(candidate_root, "retained_spectre_one.log"), "bad metadata\n")
        os.symlink(meta, os.path.join(candidate_root, "retained_spectre_one.meta"))
        outside_log = os.path.join(self.root, "outside.log")
        self.write_text(outside_log, "bad hardlinked log\n")
        os.link(outside_log, os.path.join(candidate_root, "retained_spectre_two.log"))
        with open(meta) as stream:
            metadata = stream.read()
        self.write_text(os.path.join(candidate_root, "retained_spectre_two.meta"), metadata)
        self.assertEqual(self.service.candidate_logs("run-1", candidate_id)["logs"], [])

    def test_symlink_and_hardlinked_live_logs_are_not_exposed(self):
        paths = self.logs["g000_c0000"]
        outside = os.path.join(self.root, "outside.log")
        self.write_text(outside, "not candidate-owned\n")
        os.unlink(paths[0])
        os.symlink(outside, paths[0])
        os.unlink(paths[1])
        os.link(outside, paths[1])
        self.assertEqual(self.service.candidate_logs("run-1", "g000_c0000")["logs"], [])


if __name__ == "__main__":
    unittest.main()
