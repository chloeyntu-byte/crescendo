"""Persistent cluster pools need owned Stop after candidate completion too."""
import signal
import unittest
from unittest import mock

from tests import test_cluster_serial_stop as serial_fixture


class ClusterPoolLifetimeTests(unittest.TestCase):
    def setUp(self):
        self.fixture = serial_fixture.ClusterSerialStopTests()
        self.fixture.setUp()

    def tearDown(self):
        self.fixture.tearDown()

    def test_completed_batch_keeps_pool_and_latest_ack_stop_fence(self):
        proxy = self.fixture.proxy()
        batch = proxy.submit_evaluate_batch([(0, {"w": 1})], 0, {})
        self.fixture.response(proxy, batch, {"candidates": [], "native_completion_proven": True})
        self.assertFalse(proxy._batch_native_inflight)
        self.assertTrue(proxy._native_pool_lifetime_active)
        readback = proxy._new_request("ACK", {"candidate_id": "g000_c0000"})
        self.fixture.response(proxy, readback, {"accepted": True})
        self.assertIs(proxy._batch_request_identity, readback)
        self.fixture.acknowledgement(proxy, batch)
        with mock.patch.object(proxy, "_controller_identity_is_owned", return_value=True), \
                mock.patch("crescendo_engine.persistent_controller.os.kill") as kill, \
                mock.patch("crescendo_engine.persistent_controller.time.monotonic", side_effect=[0, .01, 1]), \
                mock.patch("crescendo_engine.persistent_controller.time.sleep"):
            stale = proxy.request_native_batch_stop("between generations")
        kill.assert_called_once_with(proxy.process.pid, signal.SIGUSR1)
        self.assertTrue(stale["required"])
        self.assertFalse(stale["remote_stop_proven"])
        self.fixture.acknowledgement(proxy, readback)
        with mock.patch.object(proxy, "_controller_identity_is_owned", return_value=True), \
                mock.patch("crescendo_engine.persistent_controller.os.kill") as kill:
            current = proxy.request_native_batch_stop("between generations")
        kill.assert_called_once_with(proxy.process.pid, signal.SIGUSR1)
        self.assertTrue(current["required"])
        self.assertTrue(current["remote_stop_proven"])

    def test_local_batch_pool_refreshes_commit_nonce_after_inflight_cleared(self):
        proxy = self.fixture.proxy(required=False)
        batch = proxy.submit_evaluate_batch([(0, {"w": 1})], 0, {})
        self.fixture.response(proxy, batch, {"candidates": []})
        self.assertFalse(proxy._batch_native_inflight)
        self.assertTrue(proxy._native_pool_lifetime_active)
        commit = proxy._new_request("COMMIT", {"candidate_id": "g000_c0000"})
        self.assertIs(proxy._batch_request_identity, commit)
        self.assertNotEqual(batch["nonce"], commit["nonce"])
        self.fixture.response(proxy, commit, {"accepted": True})
        self.fixture.acknowledgement(proxy, commit)
        with mock.patch.object(proxy, "_controller_identity_is_owned", return_value=True), \
                mock.patch("crescendo_engine.persistent_controller.os.kill"):
            self.assertTrue(proxy.request_native_batch_stop("local pool stop after COMMIT")["remote_stop_proven"])

    def test_close_releases_pool_only_after_explicit_remote_proof(self):
        for proof in (True, False, None):
            with self.subTest(remote_stop_proven=proof):
                proxy = self.fixture.proxy()
                batch = proxy.submit_evaluate_batch([(0, {"w": 1})], 0, {})
                self.fixture.response(proxy, batch, {"candidates": []})
                close = proxy._new_request("CLOSE", {})
                self.fixture.response(proxy, close, {"remote_stop_proven": proof})
                self.assertEqual(proxy._native_pool_lifetime_active, proof is not True)

    def test_serial_completed_candidate_keeps_cluster_pool_lifetime(self):
        proxy = self.fixture.proxy()
        request = proxy.submit_evaluate(0, 0, {"w": 1}, {})
        self.fixture.response(proxy, request, {"native_job_state": {
            "simulation_submitted": True, "native_completion_proven": True}})
        self.assertFalse(proxy._batch_native_inflight)
        self.assertTrue(proxy._native_pool_lifetime_active)
        next_request = proxy._new_request("COMMIT", {"candidate_id": "g000_c0000"})
        self.assertIs(proxy._batch_request_identity, next_request)


if __name__ == "__main__":
    unittest.main()
