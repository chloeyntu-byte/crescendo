import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock

from crescendo_engine.runner import InfrastructureFailure, _evaluate_run_persistent_batch


class BatchPreparationFailureTests(unittest.TestCase):
    def run_batch(self, root, states, submitted=False, no_jobs=True, retirement=None):
        artifact = {"states": states, "simulation_submitted": submitted,
                    "no_remote_jobs_started": no_jobs}
        controller = SimpleNamespace(spectre_log=os.path.join(root, "missing.log"),
                                     evaluate_batch=mock.Mock(return_value=artifact))
        worker = SimpleNamespace(controller=controller, workspace={"root": root},
                                 ensure_started=mock.Mock(),
                                 retire=mock.Mock(return_value=retirement or {
                                     "termination_proven": True, "cleanup": {"ok": True},
                                     "source_guard": {"ok": True}}))
        return worker, lambda: _evaluate_run_persistent_batch(
            worker, root, [(0, {"width": 1}), (1, {"width": 2})], 0,
            {"candidate_timeout_seconds": 60}, {}, [], {"runtime": {}})

    def test_one_cdf_preparation_error_preserves_population_for_serial_isolation(self):
        with tempfile.TemporaryDirectory() as root:
            worker, run = self.run_batch(root, ["ACCEPTED", "BEGIN", "ERROR"])
            result = run()
            self.assertIn("serial candidate isolation", result["fallback"])
            self.assertEqual(result["rows"], [])
            worker.retire.assert_called_once()
            self.assertEqual(worker.controller.evaluate_batch.call_args[0][0][1]["values"], {"width": 2})

    def test_rejected_or_ambiguous_submission_must_not_trigger_serial_resubmission(self):
        for states, submitted, no_jobs in (
                (["ACCEPTED", "BEGIN", "PATCHED", "ERROR"], False, True),
                (["ACCEPTED", "BEGIN", "ERROR"], True, False),
                (["ACCEPTED", "BEGIN", "ERROR"], None, True)):
            with tempfile.TemporaryDirectory() as root:
                worker, run = self.run_batch(root, states, submitted, no_jobs)
                with self.assertRaises(InfrastructureFailure):
                    run()
                worker.retire.assert_not_called()

    def test_preparation_fallback_requires_source_and_owned_cleanup_proof(self):
        for retirement in (
                {"termination_proven": False, "cleanup": {"ok": True}, "source_guard": {"ok": True}},
                {"termination_proven": True, "cleanup": {"ok": False}, "source_guard": {"ok": True}},
                {"termination_proven": True, "cleanup": {"ok": True}, "source_guard": {"ok": False}}):
            with tempfile.TemporaryDirectory() as root:
                _worker, run = self.run_batch(root, ["ACCEPTED", "BEGIN", "ERROR"], retirement=retirement)
                with self.assertRaisesRegex(InfrastructureFailure, "retirement is unproven"):
                    run()


if __name__ == "__main__":
    unittest.main()
