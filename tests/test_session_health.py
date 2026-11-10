import os
import signal
import subprocess
import sys
import tempfile
import unittest
import io
from unittest import mock

from crescendo_engine.session_health import (
    PersistentSessionHealthGuard, process_identity, sample_owned_process_tree,
    _rss_absolute_limit_details,
)


def health(rss_kib, fd_count=20, alive=True):
    return {"alive": alive, "rss_kib": rss_kib, "fd_count": fd_count}


class PersistentSessionHealthGuardTests(unittest.TestCase):
    def test_active_memory_cap_uses_effective_parent_cgroup_limit(self):
        for row, base, filename in (("2:memory:/jobs/task", "/sys/fs/cgroup/memory", "memory.limit_in_bytes"),
                                    ("0::/jobs/task", "/sys/fs/cgroup", "memory.max")):
            with self.subTest(row=row):
                files = {"/proc/meminfo": "MemTotal: 10000 kB\n", "/proc/self/cgroup": row + "\n",
                         base + "/jobs/task/" + filename: "max" if row.startswith("0") else "20000000",
                         base + "/jobs/" + filename: str(4000 * 1024)}
                def read(path, *args, **kwargs):
                    if path not in files:
                        raise IOError("unavailable")
                    return io.StringIO(files[path])
                with mock.patch("builtins.open", side_effect=read):
                    result = _rss_absolute_limit_details()
                self.assertEqual(result["limit_kib"], 3200)
                self.assertTrue(any(source["source"] == base + "/jobs/" + filename
                                    for source in result["sources"]))

    def test_unreadable_machine_memory_does_not_create_a_guessed_limit(self):
        with mock.patch("builtins.open", side_effect=IOError("unavailable")):
            result = _rss_absolute_limit_details()
        self.assertIsNone(result["limit_kib"])
        self.assertEqual(result["sources"], [])

    def test_active_tree_peak_does_not_train_or_compare_idle_growth(self):
        guard = PersistentSessionHealthGuard(
            warmup_samples=1, baseline_samples=1, consecutive_limit=1,
            rss_growth_limit_kib=100, fd_growth_limit=10,
            nofile_soft_limit=1000, rss_absolute_limit_kib=10000,
        )
        guard.observe(health(100, 20))
        peak = guard.observe(health(2000, 150), comparable=False)
        self.assertFalse(peak["triggered"])
        self.assertEqual(peak["violations"], [])
        self.assertEqual(peak["baseline"]["rss_kib"], 100)
        self.assertEqual(peak["growth_sample_count"], 1)
        boundary = guard.observe(health(201, 31))
        self.assertTrue(boundary["triggered"])
        self.assertEqual(boundary["violations"], ["rss_growth", "fd_growth"])

    def test_active_absolute_memory_fd_disk_and_log_limits_remain_enforced(self):
        cases = [({"rss_kib": 1001}, "rss_absolute_limit"),
                 ({"max_fd_count": 71}, "fd_per_process_limit"),
                 ({"free_disk_mib": 1}, "disk_low_watermark"),
                 ({"log_bytes": 101}, "log_growth")]
        for extra, expected in cases:
            with self.subTest(expected=expected):
                guard = PersistentSessionHealthGuard(
                    warmup_samples=5, consecutive_limit=1, nofile_soft_limit=100,
                    rss_absolute_limit_kib=1000, min_free_disk_mib=2,
                    log_growth_limit_bytes=100,
                )
                sample = dict(health(100), max_fd_count=20, free_disk_mib=10, log_bytes=0)
                guard.observe(sample, comparable=False)
                sample.update(extra)
                result = guard.observe(sample, comparable=False)
                self.assertTrue(result["triggered"])
                self.assertEqual(result["violations"], [expected])
                self.assertEqual(result["growth_sample_count"], 0)

    def test_warmup_uses_only_the_tail_for_the_baseline(self):
        guard = PersistentSessionHealthGuard(
            warmup_samples=5, baseline_samples=3, rss_growth_limit_kib=100,
            fd_growth_limit=10, nofile_soft_limit=1000,
        )

        for rss_kib in (100, 500, 510, 520, 530):
            result = guard.observe(health(rss_kib))

        self.assertEqual(result["state"], "warming")
        self.assertEqual(result["baseline"]["rss_kib"], 520)
        self.assertEqual(result["limits"]["rss_kib"], 620)

    def test_rss_growth_limit_is_an_absolute_delta_from_the_baseline(self):
        guard = PersistentSessionHealthGuard(
            warmup_samples=1, baseline_samples=1, consecutive_limit=1,
            rss_growth_limit_kib=100, nofile_soft_limit=1000,
        )

        baseline = guard.observe(health(900))
        at_limit = guard.observe(health(1000))
        over_limit = guard.observe(health(1001))

        self.assertEqual(baseline["baseline"]["rss_kib"], 900)
        self.assertEqual(baseline["limits"]["rss_kib"], 1000)
        self.assertEqual(at_limit["state"], "healthy")
        self.assertFalse(at_limit["triggered"])
        self.assertEqual(over_limit["violations"], ["rss_growth"])
        self.assertTrue(over_limit["triggered"])

    def test_three_consecutive_rss_violations_trigger_the_guard(self):
        guard = PersistentSessionHealthGuard(
            warmup_samples=3, baseline_samples=3, consecutive_limit=3,
            rss_growth_limit_kib=100, fd_growth_limit=10,
            nofile_soft_limit=1000,
        )
        for rss_kib in (100, 100, 100):
            guard.observe(health(rss_kib))

        first = guard.observe(health(250))
        second = guard.observe(health(260))
        third = guard.observe(health(270))

        self.assertEqual(first["state"], "warning")
        self.assertEqual(second["violation_streak"], 2)
        self.assertEqual(third["state"], "limit_exceeded")
        self.assertTrue(third["triggered"])
        self.assertEqual(third["violations"], ["rss_growth"])

    def test_healthy_sample_resets_violation_streak(self):
        guard = PersistentSessionHealthGuard(
            warmup_samples=1, baseline_samples=1, consecutive_limit=2,
            rss_growth_limit_kib=100, nofile_soft_limit=1000,
        )
        guard.observe(health(100))
        self.assertEqual(guard.observe(health(250))["violation_streak"], 1)
        self.assertEqual(guard.observe(health(150))["violation_streak"], 0)
        self.assertFalse(guard.observe(health(250))["triggered"])

    def test_per_process_fd_limit_is_not_applied_to_tree_aggregate(self):
        guard = PersistentSessionHealthGuard(
            warmup_samples=1, baseline_samples=1, consecutive_limit=1,
            rss_growth_limit_kib=10000, fd_growth_limit=128,
            nofile_soft_limit=100,
        )
        guard.observe(health(100, 20))
        result = guard.observe({
            "alive": True, "rss_kib": 101, "fd_count": 90,
            "max_fd_count": 71,
        })

        self.assertEqual(result["limits"]["fd_count"], 148)
        self.assertEqual(result["limits"]["max_fd_count"], 70)
        self.assertTrue(result["triggered"])
        self.assertEqual(result["violations"], ["fd_per_process_limit"])

    def test_dead_process_tree_fails_closed_after_consecutive_samples(self):
        guard = PersistentSessionHealthGuard(
            warmup_samples=5, consecutive_limit=3,
        )

        first = guard.observe({"alive": False, "rss_kib": None, "fd_count": None})
        second = guard.observe({"alive": False, "rss_kib": None, "fd_count": None})
        third = guard.observe({"alive": False, "rss_kib": None, "fd_count": None})

        self.assertEqual(first["state"], "warning")
        self.assertEqual(first["violation_streak"], 1)
        self.assertEqual(second["violation_streak"], 2)
        self.assertTrue(third["triggered"])
        self.assertEqual(third["violations"], ["owned_process_tree_dead"])

    def test_single_incomplete_sample_warns_during_warmup(self):
        guard = PersistentSessionHealthGuard(
            warmup_samples=5, consecutive_limit=2,
        )
        result = guard.observe({
            "alive": True, "tree_complete": False,
            "storage_complete": True, "logs_complete": True,
        })
        self.assertEqual(result["state"], "warning")
        self.assertFalse(result["triggered"])
        self.assertEqual(result["violations"], ["owned_process_tree_incomplete"])

    def test_storage_and_log_unavailable_count_from_first_sample(self):
        for field, violation in (
            ("storage_complete", "storage_health_unavailable"),
            ("logs_complete", "log_health_unavailable"),
        ):
            with self.subTest(field=field):
                guard = PersistentSessionHealthGuard(
                    warmup_samples=5, consecutive_limit=2,
                )
                sample = {
                    "alive": True, "tree_complete": True,
                    "storage_complete": True, "logs_complete": True,
                    "rss_kib": 100, "fd_count": 10,
                }
                sample[field] = False
                first = guard.observe(sample)
                second = guard.observe(sample)
                self.assertEqual(first["state"], "warning")
                self.assertEqual(first["violations"], [violation])
                self.assertTrue(second["triggered"])

    def test_unproven_session_identity_fails_closed_without_resetting_the_streak(self):
        guard = PersistentSessionHealthGuard(
            warmup_samples=1, consecutive_limit=2,
        )
        sample = {
            "alive": True, "tree_complete": True,
            "storage_complete": True, "logs_complete": True,
            "session_identity_complete": False,
        }

        first = guard.observe(sample)
        second = guard.observe(sample)

        self.assertEqual(first["violations"], ["session_identity_unproven"])
        self.assertFalse(first["triggered"])
        self.assertTrue(second["triggered"])

    def test_growth_checks_wait_for_the_warmup_baseline(self):
        guard = PersistentSessionHealthGuard(
            warmup_samples=3, baseline_samples=1, consecutive_limit=1,
            rss_growth_limit_kib=10, fd_growth_limit=5,
            log_growth_limit_bytes=10, nofile_soft_limit=1000,
            min_free_disk_mib=1,
        )
        base = {
            "alive": True, "rss_kib": 100, "fd_count": 10,
            "free_disk_mib": 2000, "log_bytes": 0,
        }
        guard.observe(base)
        early_growth = guard.observe(dict(
            base, rss_kib=1000, fd_count=100, log_bytes=1000,
        ))
        baseline = guard.observe(base)
        after_warmup = guard.observe(dict(
            base, rss_kib=1000, fd_count=100, log_bytes=1000,
        ))

        self.assertEqual(early_growth["state"], "warming")
        self.assertEqual(early_growth["violations"], [])
        self.assertEqual(baseline["baseline"]["rss_kib"], 100)
        self.assertTrue(after_warmup["triggered"])
        self.assertEqual(
            after_warmup["violations"], ["rss_growth", "fd_growth", "log_growth"],
        )

    def test_exact_owned_process_tree_includes_descendants(self):
        process = subprocess.Popen(
            [sys.executable, "-c", (
                "import subprocess,sys,time; "
                "child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(30)']); "
                "print(child.pid); sys.stdout.flush(); time.sleep(30)"
            )],
            stdout=subprocess.PIPE, universal_newlines=True, start_new_session=True,
        )
        try:
            child_pid = int(process.stdout.readline().strip())
            process.stdout.close()
            identity = process_identity(process.pid)
            self.assertIsNotNone(identity)
            sample = sample_owned_process_tree([identity])
            self.assertTrue(sample["alive"])
            self.assertTrue(sample["tree_complete"])
            self.assertGreaterEqual(sample["process_count"], 2)
            self.assertIn(child_pid, [row["pid"] for row in sample["processes"]])
            self.assertGreater(sample["rss_kib"], 0)
            self.assertGreater(sample["fd_count"], 0)
        finally:
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except OSError:
                pass
            process.wait()

    def test_pid_reuse_identity_mismatch_fails_closed(self):
        identity = process_identity(os.getpid())
        identity["start_ticks"] += 1
        sample = sample_owned_process_tree([identity])
        self.assertFalse(sample["alive"])
        self.assertFalse(sample["tree_complete"])
        self.assertEqual(sample["process_count"], 0)

    def test_disk_low_watermark_triggers_during_warmup(self):
        guard = PersistentSessionHealthGuard(
            warmup_samples=5, min_free_disk_mib=1000,
        )
        result = guard.observe({
            "alive": True, "rss_kib": 100, "fd_count": 10,
            "free_disk_mib": 999, "log_bytes": 0,
        })
        self.assertTrue(result["triggered"])
        self.assertEqual(result["violations"], ["disk_low_watermark"])

    def test_known_log_growth_is_bounded_without_directory_scan(self):
        guard = PersistentSessionHealthGuard(
            warmup_samples=1, baseline_samples=1, consecutive_limit=1,
            log_growth_limit_bytes=10, min_free_disk_mib=1,
        )
        base = {
            "alive": True, "rss_kib": 100, "fd_count": 10,
            "free_disk_mib": 2000,
        }
        guard.observe(dict(base, log_bytes=5))
        result = guard.observe(dict(base, log_bytes=16))
        self.assertTrue(result["triggered"])
        self.assertEqual(result["violations"], ["log_growth"])

        with tempfile.TemporaryDirectory() as root:
            log_path = os.path.join(root, "ocean.stderr")
            with open(log_path, "w") as stream:
                stream.write("known-log")
            sample = sample_owned_process_tree(
                [process_identity(os.getpid())],
                storage_paths=[root], log_paths=[log_path],
            )
            self.assertTrue(sample["storage_complete"])
            self.assertTrue(sample["logs_complete"])
            self.assertEqual(sample["log_bytes"], len("known-log"))


if __name__ == "__main__":
    unittest.main()
