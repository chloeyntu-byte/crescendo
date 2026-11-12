import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine import candidate_timing
from crescendo_engine.completion import _spectre_log_evidence


class CandidateTimingTests(unittest.TestCase):
    def test_pending_samples_require_consecutive_observations_of_owned_job(self):
        def record(count, state, job_id="10000001"):
            return {"query_count": count, "latest": {
                "job_id": job_id, "state": state, "reason": "exact_job_nonterminal"}}

        pending = candidate_timing.PendingSamples()
        with mock.patch.object(candidate_timing, "timing_tick", side_effect=[1.0, 4.0, 8.0, 11.0]):
            pending.observe(record(1, "PENDING"), "10000001")
            pending.observe(record(1, "PENDING"), "10000001")
            pending.observe(record(2, "PENDING", job_id="10000002"), "10000001")
            pending.observe(record(3, "PENDING"), "10000001")
            pending.observe({"query_count": None, "latest": "bad"}, "10000001")
            pending.observe(record(4, "PENDING"), "10000001")
            pending.observe(record(5, "PENDING"), "10000001")
            pending.observe(record(6, "RUNNING"), "10000001")
        self.assertEqual(pending.phase()["elapsed_seconds"], 3.0)
        self.assertEqual(pending.phase()["count"], 1)

    def test_unknown_clock_and_invalid_samples_do_not_mask_primary_error(self):
        phases = {}
        with mock.patch.object(candidate_timing, "timing_tick", return_value=None):
            with self.assertRaisesRegex(RuntimeError, "original failure"):
                with candidate_timing.measure(phases, "solver"):
                    raise RuntimeError("original failure")
        summary = candidate_timing.summarize_tasks([
            {"phases": phases}, {}, {"phases": {"solver": {"elapsed_seconds": float("inf")}}},
            {"phases": {"solver": {"elapsed_seconds": 2.0}}},
        ], "solver")
        self.assertIsNone(phases["solver"]["elapsed_seconds"])
        self.assertEqual(summary, {"elapsed_seconds": 2.0, "scope": "task_sum",
                                   "count": 1, "total_count": 4})

    def test_spectre_reads_native_wall_clock_and_preserves_unknown(self):
        # Synthetic native-summary fixture; no simulator or site data required.
        wall_clock = (
            "Simulation started at: 1:00:00 AM, Sat Jan 1, 2000, ended at: "
            "1:00:02 AM, Sat Jan 1, 2000, with elapsed time (wall clock): 1.28 s.\n")
        summary = "spectre completes with 0 errors, 1 warning, and 4 notices.\n"
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, "spectre.out")
            with open(path, "w") as stream:
                stream.write("Time used: CPU = 656 ms, elapsed = 1.28 s, util. = 51.1%.\n")
                stream.write(wall_clock + summary)
            evidence = _spectre_log_evidence(path, root, include_timing=True)
            self.assertEqual(evidence["solver_wall_seconds"], 1.28)
            alias = os.path.join(root, "shared.out")
            os.symlink(path, alias)
            self.assertNotIn("solver_wall_seconds", _spectre_log_evidence(alias, root, include_timing=True))
            with open(path, "a") as stream:
                stream.write("Time used: CPU = 1 s, elapsed = 1.0 s, util. = 100%.\n" + summary)
            self.assertIsNone(_spectre_log_evidence(path, root, include_timing=True)["solver_wall_seconds"])
