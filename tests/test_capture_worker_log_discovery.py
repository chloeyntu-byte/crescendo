"""Discover per-user ADE logs only inside the owned generation workspace."""
import os
import tempfile
import unittest

from crescendo_engine.private_netlist import _capture_worker_logs


class CaptureWorkerLogDiscoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.workspace = {"root": self.temp.name}

    def write_log(self, user, text):
        root = os.path.join(self.temp.name, "logs_" + user, "logs0")
        os.makedirs(root, exist_ok=True)
        path = os.path.join(root, "Job0.log")
        with open(path, "w") as stream:
            stream.write(text)
        return path

    def test_server_user_log_is_discovered_without_root_username(self):
        before, snapshot = _capture_worker_logs(self.workspace)
        self.assertTrue(before["ok"])
        path = self.write_log("user01", "Run start for Point ID 1\n*Error* eval: unbound variable - schematic_FTdiff\n")
        result, _ = _capture_worker_logs(self.workspace, snapshot)
        self.assertFalse(result["ok"])
        self.assertEqual([row["path"] for row in result["logs"]], [path])
        self.assertEqual(result["logs_root"], os.path.dirname(os.path.dirname(path)))
        self.assertEqual(result["logs"][0]["generation_error_count"], 1)

    def test_unchanged_old_user_logs_do_not_reappear_in_next_capture(self):
        self.write_log("root", "Run start for Point ID 1\n*Error* old error\n")
        _, snapshot = _capture_worker_logs(self.workspace)
        path = self.write_log("new.user-42", "Run start for Point ID 2\nRunning simulation on testbench TB\nERROR (ASSEMBLER-5031): intentional capture exit\n")
        result, _ = _capture_worker_logs(self.workspace, snapshot)
        self.assertTrue(result["ok"])
        self.assertEqual([row["path"] for row in result["logs"]], [path])
        self.assertEqual(result["logs"][0]["expected_wrapper_aftermath_error_count"], 1)

    def test_linked_external_log_root_is_not_read(self):
        with tempfile.TemporaryDirectory() as outside:
            os.makedirs(os.path.join(outside, "logs0"))
            with open(os.path.join(outside, "logs0", "Job0.log"), "w") as stream:
                stream.write("*Error* outside owned workspace\n")
            os.symlink(outside, os.path.join(self.temp.name, "logs_foreign"))
            result, snapshot = _capture_worker_logs(self.workspace, {})
        self.assertEqual(result["logs"], [])
        self.assertEqual(snapshot, {})
        self.assertEqual(len(result["omitted_log_roots"]), 1)

    def test_aftermath_samples_keep_code_source_and_original_lines_without_becoming_generation_errors(self):
        path = self.write_log("user01", "old worker output\n")
        _, before = _capture_worker_logs(self.workspace)
        with open(path, "a") as stream:
            stream.write('Run start for Point ID 1\nRunning simulation on testbench TB\n'
                'ERROR (ADE-3036): The simulator run log has not been generated.\n'
                'ERROR (ASSEMBLER-5031): Simulator failed to complete the simulation.\n'
                '*Error* expected wrapper exit\n')
        result, _ = _capture_worker_logs(self.workspace, before)
        self.assertTrue(result['ok'])
        self.assertFalse(result['parent_capture_failed'])
        self.assertEqual(result['logs'][0]['generation_error_count'], 0)
        self.assertEqual(result['logs'][0]['expected_wrapper_aftermath_error_count'], 3)
        samples = result['expected_wrapper_aftermath_samples']
        self.assertEqual(len(samples), 2)
        self.assertEqual([row['line'] for row in samples], [4, 5])
        self.assertEqual([row['code'] for row in samples], ['ADE-3036', 'ASSEMBLER-5031'])
        self.assertEqual(samples[0]['console_source'], path)
        self.assertEqual(samples[0]['stage'], 'capture_wrapper_aftermath')
        self.assertFalse(samples[0]['primary_failure'])
        self.assertIn('run log has not been generated', samples[0]['message'])

    def test_aftermath_sample_limit_applies_across_all_workers_and_messages_are_bounded(self):
        for user in ('a', 'b', 'c'):
            self.write_log(user, 'Running simulation on testbench TB\nERROR (ADE-3036): ' + 'x' * 4096 + '\n')
        result, _ = _capture_worker_logs(self.workspace, {})
        self.assertTrue(result['ok'])
        self.assertEqual(len(result['expected_wrapper_aftermath_samples']), 2)
        self.assertEqual(len(result['logs']), 3)
        self.assertTrue(all(len(row['message']) <= 1024 for row in result['expected_wrapper_aftermath_samples']))


if __name__ == "__main__":
    unittest.main()
