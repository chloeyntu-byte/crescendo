import copy
import json
import os
import shutil
import tempfile
import threading
import time
import unittest
from unittest import mock

from crescendo_engine.netlist_optimizer import PrivateNetlistOptimizer, PROTOCOL
from crescendo_engine.server import OptimizerService
from crescendo_engine.shared_cli_logs import retain_shared_cli_task_logs, MAX_LOG_BYTES
from crescendo_engine.retention import summarize_successful_candidate_artifacts, retained_spectre_paths
from crescendo_engine.utils import atomic_write_json


class SharedCliLiveLogTests(unittest.TestCase):
    def fixture(self, root, simulator="alps", status="running"):
        run = os.path.join(root, "run-1")
        candidate = os.path.join(run, "candidates", "g000_c0000")
        task_root = os.path.join(candidate, "simulation", "tasks", "p1_ac")
        os.makedirs(os.path.join(task_root, "execution"))
        os.makedirs(os.path.join(task_root, "psf"))
        atomic_write_json(os.path.join(run, "status.json"), {
            "schema_version": "crescendo.run_status.v1", "run_id": "run-1", "run_root": run, "state": "running"})
        atomic_write_json(os.path.join(candidate, "candidate.json"), {
            "candidate_id": "g000_c0000", "generation": 0, "index": 0, "status": status,
            "execution_backend": "maestro_netlist"})
        task = {"candidate_id": "g000_c0000", "task_id": "p1_ac", "test": "AC", "corner": "TT",
            "point_id": 1, "execution_simulator": simulator, "task_root": task_root,
            "psf_dir": os.path.join(task_root, "psf"), "state": "RUNNING", "scheduler_state": "RUNNING",
            "job_id": "10000001", "simulator_log": os.path.join(task_root, "psf", "spectre.out"),
            "solver_stdout": os.path.join(task_root, "execution", "simulator.stdout"),
            "solver_stderr": os.path.join(task_root, "execution", "simulator.stderr"),
            "scheduler_stdout": os.path.join(task_root, "execution", "scheduler.stdout")}
        progress = {"schema_version": "crescendo.shared_cli_progress.v1", "candidate_id": "g000_c0000",
            "execution_backend": "maestro_netlist", "execution_simulator": simulator, "stage": "solve", "tasks": [task]}
        atomic_write_json(os.path.join(candidate, "cli_progress.json"), progress)
        return run, candidate, task, progress

    def write(self, path, text):
        with open(path, "w", encoding="utf-8") as stream:
            stream.write(text)

    def test_alps_stdout_and_companions_stream_before_candidate_completion(self):
        with tempfile.TemporaryDirectory() as root:
            run, candidate, task, _progress = self.fixture(root)
            self.write(task["solver_stdout"], "ALPS simulation starting\n")
            self.write(task["psf_dir"] + ".log", "model read\n")
            self.write(task["psf_dir"] + ".warn", "ALPS model warning\n")
            self.write(task["scheduler_stdout"], "TASK_INFO:\n JOB_ID: 10000001\n")
            service = OptimizerService({"runtime": {"run_root": root}})
            payload = service.candidate_logs("run-1", "g000_c0000")
            self.assertEqual(payload["candidate_status"], "running")
            self.assertEqual(payload["execution_simulator"], "alps")
            self.assertEqual(payload["tasks"][0]["scheduler_state"], "RUNNING")
            self.assertEqual({row["file_name"] for row in payload["logs"] if row.get("scope") == "task"}, {"simulator.stdout", "psf.log", "psf.warn", "scheduler.stdout"})
            descriptor = next(row for row in payload["logs"] if row["kind"] == "alps_stdout")
            self.assertIn("p1_ac/execution/simulator.stdout", descriptor["location"])
            first = service.read_candidate_log_chunk("run-1", "g000_c0000", descriptor["log_id"])
            self.write(task["solver_stdout"], "ALPS simulation starting\nnext point\n")
            second = service.read_candidate_log_chunk("run-1", "g000_c0000", descriptor["log_id"],
                offset=first["next_offset"], file_token=first["file_token"])
            self.assertEqual(second["text"], "next point\n")

    def test_progress_never_changes_terminal_optimizer_status(self):
        with tempfile.TemporaryDirectory() as root:
            _run, _candidate, task, _progress = self.fixture(root, status="completed")
            self.write(task["solver_stdout"], "done\n")
            payload = OptimizerService({"runtime": {"run_root": root}}).candidate_logs("run-1", "g000_c0000")
            self.assertEqual(payload["candidate_status"], "completed")

    def test_cross_candidate_or_cross_task_log_is_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            run, candidate, task, progress = self.fixture(root)
            foreign = os.path.join(candidate, "simulation", "tasks", "another", "simulator.stdout")
            os.makedirs(os.path.dirname(foreign))
            self.write(foreign, "another point\n")
            progress["tasks"][0]["solver_stdout"] = foreign
            atomic_write_json(os.path.join(candidate, "cli_progress.json"), progress)
            with self.assertRaisesRegex(ValueError, "exact task"):
                OptimizerService({"runtime": {"run_root": root}}).candidate_logs("run-1", "g000_c0000")

    def test_external_scheduler_output_does_not_hide_safe_live_solver_logs(self):
        with tempfile.TemporaryDirectory() as root:
            run, candidate, task, progress = self.fixture(root)
            self.write(task["solver_stdout"], "ALPS is running\n")
            external = os.path.join(root, "site-scheduler-output.log")
            self.write(external, "outside exact owned task\n")
            progress["tasks"][0]["scheduler_stdout"] = external
            atomic_write_json(os.path.join(candidate, "cli_progress.json"), progress)
            payload = OptimizerService({"runtime": {"run_root": root}}).candidate_logs("run-1", "g000_c0000")
            self.assertIn("alps_stdout", {row["kind"] for row in payload["logs"]})
            self.assertNotIn("scheduler_stdout", {row["kind"] for row in payload["logs"]})
            self.assertTrue(any(external in row and "outside this exact owned task" in row for row in payload["warnings"]))

    def test_actual_snapshots_survive_task_psf_retirement_and_are_identified(self):
        with tempfile.TemporaryDirectory() as root:
            run, candidate, task, progress = self.fixture(root)
            self.write(task["simulator_log"], "actual ALPS solver text\n")
            self.write(task["solver_stdout"], "real stdout\n")
            self.write(task["psf_dir"] + ".warn", "actual warning\n")
            self.write(task["scheduler_stdout"], "Scheduler actual output\n")
            retained = retain_shared_cli_task_logs(task, run, "g000_c0000")
            self.assertEqual(len(retained["files"]), 4)
            self.assertTrue(all(not row["completion_authority"] and not row["termination_authority"] for row in retained["files"]))
            progress["tasks"][0]["retained_logs"] = retained
            atomic_write_json(os.path.join(candidate, "cli_progress.json"), progress)
            shutil.rmtree(os.path.join(candidate, "simulation"))
            service = OptimizerService({"runtime": {"run_root": root}})
            payload = service.candidate_logs("run-1", "g000_c0000")
            snapshots = [row for row in payload["logs"] if row.get("scope") == "task"]
            self.assertEqual(len(snapshots), 4)
            self.assertTrue(all(row["snapshot"] for row in snapshots))
            descriptor = next(row for row in payload["logs"] if row["file_name"] == "psf.warn")
            self.assertIn("retained snapshot", descriptor["location"])
            self.assertEqual(service.read_candidate_log_chunk("run-1", "g000_c0000", descriptor["log_id"])["text"], "actual warning\n")
            summary = summarize_successful_candidate_artifacts(candidate, run)
            self.assertIsNotNone(summary)
            self.assertEqual(len(retained_spectre_paths(candidate)), 8)

    def test_snapshot_bytes_are_bounded_and_linked_sources_are_ignored(self):
        with tempfile.TemporaryDirectory() as root:
            run, _candidate, task, _progress = self.fixture(root, simulator="spectre")
            self.write(task["simulator_log"], "x" * (MAX_LOG_BYTES + 400))
            external = os.path.join(root, "unowned.log")
            self.write(external, "secret must not be read\n")
            os.symlink(external, task["solver_stdout"])
            result = retain_shared_cli_task_logs(task, run, "g000_c0000")
            self.assertEqual(len(result["files"]), 1)
            self.assertTrue(result["files"][0]["truncated"])
            self.assertLessEqual(result["files"][0]["retained_bytes"], MAX_LOG_BYTES)
            self.assertIn("spectre_stdout: unowned_source", result["warnings"])

    def test_alps_internal_companion_alias_streams_and_retains_actual_target_bytes(self):
        with tempfile.TemporaryDirectory() as root:
            run, candidate, task, progress = self.fixture(root)
            task['launch_epoch'] = time.time() - 1
            progress['tasks'][0]['launch_epoch'] = task['launch_epoch']
            target = os.path.join(task['psf_dir'], 'alps.log')
            warning = os.path.join(task['psf_dir'], 'alps.warn')
            self.write(target, 'owned ALPS starting\n')
            self.write(warning, 'WARNING: owned ALPS warning\n')
            os.symlink('psf/alps.log', task['psf_dir'] + '.log')
            os.symlink(warning, task['psf_dir'] + '.warn')
            atomic_write_json(os.path.join(candidate, 'cli_progress.json'), progress)
            service = OptimizerService({'runtime': {'run_root': root}})
            payload = service.candidate_logs('run-1', 'g000_c0000')
            descriptor = next(row for row in payload['logs'] if row['kind'] == 'alps_companion')
            self.assertTrue(descriptor['leaf_link'])
            self.assertTrue(descriptor['resolved_location'].endswith('psf/alps.log'))
            self.assertNotIn('_owned_log_task', descriptor)
            chunk = service.read_candidate_log_chunk('run-1', 'g000_c0000', descriptor['log_id'])
            self.assertEqual(chunk['text'], 'owned ALPS starting\n')
            self.write(target, 'owned ALPS starting\nowned next point\n')
            next_chunk = service.read_candidate_log_chunk('run-1', 'g000_c0000', descriptor['log_id'],
                offset=chunk['next_offset'], file_token=chunk['file_token'])
            self.assertEqual(next_chunk['text'], 'owned next point\n')
            retained = retain_shared_cli_task_logs(task, run, 'g000_c0000', launch_epoch=task['launch_epoch'])
            self.assertEqual(len(retained['files']), 2)
            self.assertTrue(all(row['leaf_link'] for row in retained['files']))
            self.assertEqual(retained['files'][0]['source_path'], task['psf_dir'] + '.log')
            self.assertEqual(retained['files'][0]['resolved_source_path'], target)
            progress['tasks'][0]['retained_logs'] = retained
            atomic_write_json(os.path.join(candidate, 'cli_progress.json'), progress)
            shutil.rmtree(os.path.join(candidate, 'simulation'))
            payload = service.candidate_logs('run-1', 'g000_c0000')
            descriptor = next(row for row in payload['logs'] if row['file_name'] == 'psf.log')
            self.assertTrue(descriptor['snapshot'])
            self.assertEqual(service.read_candidate_log_chunk('run-1', 'g000_c0000', descriptor['log_id'])['text'],
                             'owned ALPS starting\nowned next point\n')

    def test_unsafe_or_stale_companion_links_are_skipped_without_hiding_live_stdout(self):
        with tempfile.TemporaryDirectory() as root:
            run, candidate, task, progress = self.fixture(root)
            self.write(task['solver_stdout'], 'current safe ALPS stdout\n')
            external = os.path.join(root, 'external.log')
            self.write(external, 'DO_NOT_READ_EXTERNAL')
            os.symlink(external, task['psf_dir'] + '.log')
            stale = os.path.join(task['psf_dir'], 'alps.warn')
            self.write(stale, 'DO_NOT_READ_STALE')
            epoch = time.time() - 1
            os.utime(stale, (epoch - 20, epoch - 20))
            os.symlink(stale, task['psf_dir'] + '.warn')
            progress['tasks'][0]['launch_epoch'] = epoch
            atomic_write_json(os.path.join(candidate, 'cli_progress.json'), progress)
            payload = OptimizerService({'runtime': {'run_root': root}}).candidate_logs('run-1', 'g000_c0000')
            self.assertEqual({row['kind'] for row in payload['logs'] if row['scope'] == 'task'}, {'alps_stdout'})
            self.assertTrue(any('unowned_or_linked_path' in row for row in payload['warnings']))
            self.assertTrue(any('stale_log' in row for row in payload['warnings']))
            self.assertNotIn('DO_NOT_READ', json.dumps(payload))
            result = retain_shared_cli_task_logs(task, run, 'g000_c0000', launch_epoch=epoch)
            self.assertEqual(len(result['files']), 1)
            self.assertNotIn('DO_NOT_READ', json.dumps(result))

    def test_progress_retains_all_tasks_and_updates_one_identity(self):
        with tempfile.TemporaryDirectory() as root:
            run, candidate, _task, _progress = self.fixture(root)
            optimizer = PrivateNetlistOptimizer.__new__(PrivateNetlistOptimizer)
            optimizer.run_root = run
            optimizer.root = os.path.join(run, "private_netlist")
            os.makedirs(optimizer.root)
            optimizer.source_mode = "maestro_netlist"
            optimizer.spec = {"simulator": "alps"}
            optimizer.lock = threading.RLock()
            optimizer.candidate_progress = {}
            optimizer.progress = {"schema_version": PROTOCOL, "sequence": 0, "commands": [], "tasks": []}
            with mock.patch("crescendo_engine.run_log.append_run_event"):
                for index in range(35):
                    optimizer._progress({"candidate_id": "g000_c0000", "task_id": "task_" + str(index),
                        "point_id": index, "test": "AC", "stage": "submit", "scheduler_state": "PENDING"})
                optimizer._progress({"candidate_id": "g000_c0000", "task_id": "task_0", "stage": "solve", "scheduler_state": "RUNNING"})
            with open(os.path.join(candidate, "cli_progress.json"), encoding="utf-8") as stream:
                progress = json.load(stream)
            self.assertEqual(len(progress["tasks"]), 35)
            self.assertEqual(progress["tasks"][0]["state"], "RUNNING")
            with open(os.path.join(candidate, "candidate.json"), encoding="utf-8") as stream:
                self.assertNotIn("execution_progress", json.load(stream))
