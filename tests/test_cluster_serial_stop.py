"""Serial cluster jobs keep fenced native-stop protection without launching EDA."""
import copy
import json
import os
import signal
import tempfile
import unittest
from unittest import mock

from crescendo_engine import cluster_execution
from crescendo_engine.persistent_controller import (
    PersistentControllerProcessError, PersistentControllerProxy,
)


class ClusterSerialStopTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="crescendo-cluster-serial-stop-")
        self.root = self.temporary.name

    def tearDown(self):
        self.temporary.cleanup()

    def proxy(self, required=True):
        proxy = PersistentControllerProxy(self.root, 0, {}, [], {
            "runtime": {"native_cluster_stop_required": required,
                        "cluster_stop_timeout_seconds": 0.1},
        })
        proxy.epoch_root = os.path.join(proxy.slot_root, "epoch_000")
        os.makedirs(os.path.join(proxy.epoch_root, "requests"), exist_ok=True)
        os.makedirs(os.path.join(proxy.epoch_root, "responses"), exist_ok=True)
        proxy.epoch_index = 0
        proxy.token = "f" * 32
        proxy.process = mock.Mock(pid=123456)
        proxy.process.poll.return_value = None
        proxy._batch_native_inflight = False
        proxy._batch_request_identity = None
        return proxy

    @staticmethod
    def response(proxy, request, result=None, ok=True):
        document = {key: request[key] for key in (
            "schema_version", "token", "controller_epoch", "sequence", "nonce", "operation",
        )}
        document.update(ok=ok, result=result, error="synthetic remote failure")
        with open(request["response_path"], "w") as stream:
            json.dump(document, stream)
        return proxy.poll(request)

    @staticmethod
    def acknowledgement(proxy, request):
        document = {key: request[key] for key in (
            "schema_version", "token", "controller_epoch", "sequence", "nonce", "operation",
        )}
        document["native"] = {"remote_stop_proven": True}
        with open(os.path.join(proxy.epoch_root, "native_stop.json"), "w") as stream:
            json.dump(document, stream)

    def test_serial_evaluate_on_cluster_keeps_native_request_identity(self):
        proxy = self.proxy()
        request = proxy.submit_evaluate(0, 0, {"w": 1}, {})
        self.assertTrue(proxy._batch_native_inflight)
        self.assertIs(proxy._batch_request_identity, request)
        self.assertEqual(request["operation"], "EVALUATE")

    def test_local_serial_without_flag_preserves_original_stop_behavior(self):
        proxy = self.proxy(required=False)
        proxy.submit_evaluate(0, 0, {"w": 1}, {})
        self.assertFalse(proxy._batch_native_inflight)
        with mock.patch("crescendo_engine.persistent_controller.os.kill") as kill:
            evidence = proxy.request_native_batch_stop("local serial stop")
        kill.assert_not_called()
        self.assertFalse(evidence["required"])

    def test_strict_native_terminal_proof_clears_serial_inflight(self):
        for proof in (
                {"simulation_submitted": False, "no_remote_jobs_started": True},
                {"simulation_submitted": True, "native_completion_proven": True},
                {"simulation_submitted": True, "native_stop_proven": True}):
            with self.subTest(proof=proof):
                proxy = self.proxy()
                request = proxy.submit_evaluate(0, 0, {"w": 1}, {})
                result = {"candidate_id": "g000_c0000", "native_job_state": proof}
                self.assertEqual(self.response(proxy, request, result), result)
                self.assertFalse(proxy._batch_native_inflight)

    def test_unknown_or_malformed_terminal_proof_keeps_serial_inflight(self):
        unknown = [None, [], {}, {"simulation_submitted": False},
                   {"simulation_submitted": True},
                   {"simulation_submitted": 0, "no_remote_jobs_started": True}]
        for value in (None, 0, 1, "true", [], {}):
            unknown.extend([
                {"simulation_submitted": True, "native_completion_proven": value},
                {"simulation_submitted": True, "native_stop_proven": value},
                {"simulation_submitted": False, "no_remote_jobs_started": value},
            ])
        for proof in unknown:
            with self.subTest(proof=proof):
                proxy = self.proxy()
                request = proxy.submit_evaluate(0, 0, {"w": 1}, {})
                self.response(proxy, request, {"candidate_id": "g000_c0000", "native_job_state": proof})
                self.assertTrue(proxy._batch_native_inflight)

    def test_remote_error_keeps_cluster_inflight_until_native_stop(self):
        proxy = self.proxy()
        request = proxy.submit_evaluate(0, 0, {"w": 1}, {})
        with self.assertRaises(PersistentControllerProcessError):
            self.response(proxy, request, ok=False)
        self.assertTrue(proxy._batch_native_inflight)
        self.assertIs(proxy._batch_request_identity, request)

    def test_commit_refreshes_stop_ack_fence_and_rejects_previous_evaluate_ack(self):
        proxy = self.proxy()
        evaluated = proxy.submit_evaluate(0, 0, {"w": 1}, {})
        self.response(proxy, evaluated, {"candidate_id": "g000_c0000"})
        committed = proxy._new_request("COMMIT", {"candidate_id": "g000_c0000"})
        self.assertIs(proxy._batch_request_identity, committed)
        self.assertNotEqual(evaluated["nonce"], committed["nonce"])
        self.acknowledgement(proxy, evaluated)
        with mock.patch.object(proxy, "_controller_identity_is_owned", return_value=True), \
                mock.patch("crescendo_engine.persistent_controller.os.kill") as kill, \
                mock.patch("crescendo_engine.persistent_controller.time.monotonic", side_effect=[0, 0.01, 1]), \
                mock.patch("crescendo_engine.persistent_controller.time.sleep"):
            evidence = proxy.request_native_batch_stop("stop during commit")
        kill.assert_called_once_with(proxy.process.pid, signal.SIGUSR1)
        self.assertTrue(evidence["required"])
        self.assertFalse(evidence["remote_stop_proven"])
        self.acknowledgement(proxy, committed)
        with mock.patch.object(proxy, "_controller_identity_is_owned", return_value=True), \
                mock.patch("crescendo_engine.persistent_controller.os.kill"):
            evidence = proxy.request_native_batch_stop("stop during commit")
        self.assertTrue(evidence["remote_stop_proven"])

    def test_execution_plan_requires_native_stop_for_serial_cluster_or_unknown_policy(self):
        local = {"job_policy": {"available": True, "global": {"properties": {
            "distributionmethod": "Local", "maxjobs": "2"}}}}
        command = copy.deepcopy(local)
        command["job_policy"]["global"]["properties"]["distributionmethod"] = "Command"
        per_test = copy.deepcopy(local)
        per_test["job_policy"]["tests"] = [
            {"name": "vco", "properties": {"distributionmethod": "Command"}}]
        for prepared in ({}, {"job_policy": {"available": False}}, command, per_test):
            with self.subTest(prepared=prepared):
                plan = cluster_execution.candidate_execution_plan(
                    {"maestro_candidate_mode": "serial"}, prepared, {})
                self.assertFalse(plan["enabled"])
                self.assertTrue(plan["native_cluster_stop_required"])
        plan = cluster_execution.candidate_execution_plan(
            {"maestro_candidate_mode": "serial"}, local, {})
        self.assertFalse(plan["native_cluster_stop_required"])
        plan = cluster_execution.candidate_execution_plan(
            {"maestro_candidate_mode": "serial", "maestro_job_overrides": {"command": "batch-submit"}}, local, {})
        self.assertTrue(plan["native_cluster_stop_required"])

    def test_native_state_helper_accepts_only_explicit_quiescence_evidence(self):
        helper = cluster_execution.native_job_state_quiescent
        self.assertTrue(helper({"simulation_submitted": False, "no_remote_jobs_started": True}))
        self.assertTrue(helper({"simulation_submitted": True, "native_completion_proven": True}))
        self.assertTrue(helper({"simulation_submitted": True, "native_stop_proven": True}))
        for evidence in (None, [], {}, {"simulation_submitted": False},
                         {"simulation_submitted": True, "native_completion_proven": 1},
                         {"simulation_submitted": True, "native_stop_proven": 1},
                         {"simulation_submitted": 0, "no_remote_jobs_started": True}):
            self.assertFalse(helper(evidence), evidence)


if __name__ == "__main__":
    unittest.main()
