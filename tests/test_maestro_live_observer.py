"""Native live observations stay diagnostic and preserve exact task log scope."""
import copy
import json
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine import maestro_progress

from crescendo_engine.maestro_progress import (
    MaestroProgressObserver, owned_regular, preserve_live_logs, read_batch_progress,
)


class MaestroLiveObserverTests(unittest.TestCase):
    def fixture(self, root):
        workspace = {"root": os.path.join(root, "workers", "worker_000", "epoch_000", "workspace"),
                     "native_storage": {"save_res_dir": os.path.join(root, "_scratch", "native")}}
        batch_root = os.path.join(root, "batches", "g000_g000_c0000")
        control = os.path.join(root, "workers", "worker_000", "epoch_000", "control")
        for path in (workspace["root"], batch_root, control):
            os.makedirs(path)
        candidates = [
            {"candidate_id": "g000_c0000", "source_test_map": {"alias_a": "lib:TB:1"}},
            {"candidate_id": "g000_c0001", "source_test_map": {"alias_b": "lib:TB:1"}},
        ]
        manifest = {"batch_root": batch_root, "batch_id": "batch_1_owned",
                    "protocol_identity": {"epoch": "worker_epoch", "sequence": 1,
                                          "nonce": "owned_nonce", "candidate_id": "batch_1_owned"},
                    "candidates": candidates,
                    "submission_log_path": os.path.join(root, "submission.jsonl")}
        return workspace, manifest, control

    @staticmethod
    def write(path, content):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as stream:
            stream.write(content)
        return path

    def point_log(self, root, alias="alias_a", point="1", content="Spectre simulator started\n"):
        return self.write(os.path.join(root, "_scratch", "native", "lib", "TB", "maestro", "results",
                                       "maestro", "history", point, alias, "psf", "spectre.out"), content)

    def test_serial_progress_carries_fenced_candidate_identity_and_start_time(self):
        with tempfile.TemporaryDirectory() as root:
            workspace, manifest, control = self.fixture(root)
            manifest['serial_observation'] = True
            observer = MaestroProgressObserver(root, workspace, manifest, control)
            rows = observer.poll([], force=True)['candidates']
            for row in rows:
                progress = row['execution_progress']
                self.assertTrue(progress['serial_observation'])
                self.assertEqual(progress['protocol_identity'], manifest['protocol_identity'])
                self.assertEqual(progress['started_epoch'], observer.started_epoch)
                self.assertNotIn('completion', progress)

    def test_real_point_log_updates_running_without_claiming_result_completion(self):
        with tempfile.TemporaryDirectory() as root:
            workspace, manifest, control = self.fixture(root)
            observer = MaestroProgressObserver(root, workspace, manifest, control)
            first = observer.poll([], force=True)
            self.assertTrue(all(row["execution_progress"]["stage"] == "preparing" for row in first["candidates"]))
            path = self.point_log(root)
            self.point_log(root, "foreign_alias", "1")
            running = observer.poll(["PATCHED", "SUBMITTED"], force=True)
            progress = running["candidates"][0]["execution_progress"]
            self.assertEqual(progress["stage"], "running")
            self.assertEqual(len(progress["tasks"]), 1)
            self.assertEqual(progress["tasks"][0]["spectre_log"], path)
            self.assertEqual(progress["tasks"][0]["point_id"], "1")
            self.assertEqual(progress["tasks"][0]["test"], "lib:TB:1")
            self.assertIsNone(progress["tasks"][0]["corner"])
            self.assertEqual(running["candidates"][1]["execution_progress"]["stage"], "dispatching")
            self.write(path, "spectre completes with 0 errors\n")
            finished = observer.poll(["SIM_DONE"], force=True)["candidates"][0]
            self.assertEqual(finished["execution_progress"]["stage"], "evaluating")
            self.assertNotIn("status", finished)
            self.assertNotIn("completion", finished)
            self.assertNotIn("termination_proven", finished)

    def test_exact_submission_alias_links_candidate_to_actual_ledger(self):
        with tempfile.TemporaryDirectory() as root:
            workspace, manifest, control = self.fixture(root)
            script = self.write(os.path.join(root, "_scratch", "run-native.sh"),
                                "spectre %s/1/alias_a/input.scs\n" % root)
            event = {"invocation_id": "submit_1", "argv": [script], "cwd": os.path.dirname(script),
                     "job_id": "12345", "returncode": 0}
            self.write(manifest["submission_log_path"], json.dumps(event) + "\n")
            observer = MaestroProgressObserver(root, workspace, manifest, control)
            snapshot = observer.poll(["PATCHED"], force=True)
            task = snapshot["candidates"][0]["execution_progress"]["tasks"][0]
            self.assertEqual(task["submit_log"], manifest["submission_log_path"])
            self.assertEqual(task["state"], "queued")
            self.assertEqual(task["point_id"], "1")
            self.assertEqual(task["job_id"], "12345")
            self.assertEqual(task["test"], "lib:TB:1")
            self.assertEqual(snapshot["candidates"][1]["execution_progress"]["tasks"], [])

    def test_shared_ambiguous_submission_does_not_invent_candidate_job_mapping(self):
        with tempfile.TemporaryDirectory() as root:
            workspace, manifest, control = self.fixture(root)
            event = {"invocation_id": "shared", "argv": ["alias_a", "alias_b"], "job_id": "9"}
            self.write(manifest["submission_log_path"], json.dumps(event) + "\n")
            snapshot = MaestroProgressObserver(root, workspace, manifest, control).poll(["SUBMITTED"], force=True)
            self.assertEqual(len(snapshot["shared_submissions"]), 1)
            self.assertTrue(all(not row["execution_progress"]["tasks"] for row in snapshot["candidates"]))

    def test_partial_ledger_line_is_consumed_only_once_after_newline(self):
        with tempfile.TemporaryDirectory() as root:
            workspace, manifest, control = self.fixture(root)
            event = json.dumps({"invocation_id": "one", "argv": [root + "/1/alias_a/run"], "job_id": "4"})
            self.write(manifest["submission_log_path"], event[:-1])
            observer = MaestroProgressObserver(root, workspace, manifest, control)
            first = observer.poll([], force=True)
            self.assertFalse(first["shared_submissions"])
            with open(manifest["submission_log_path"], "a") as stream:
                stream.write(event[-1:] + "\n")
            second = observer.poll([], force=True)
            self.assertEqual(len(second["shared_submissions"]), 1)
            self.assertIsNone(observer.poll([], force=True))

    def test_symlink_and_hardlink_logs_cannot_escape_owned_scope(self):
        with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as outside:
            workspace, manifest, control = self.fixture(root)
            target = self.write(os.path.join(outside, "spectre.out"), "outside\n")
            path = self.point_log(root)
            os.unlink(path)
            os.symlink(target, path)
            self.assertFalse(owned_regular(path, root))
            snapshot = MaestroProgressObserver(root, workspace, manifest, control).poll([], force=True)
            self.assertTrue(all(not row["execution_progress"]["tasks"] for row in snapshot["candidates"]))
            os.unlink(path)
            os.link(target, path)
            self.assertFalse(owned_regular(path, root))

    def test_preserved_log_metadata_retains_candidate_alias_and_point_without_authority(self):
        with tempfile.TemporaryDirectory() as root:
            workspace, manifest, control = self.fixture(root)
            path = self.point_log(root)
            self.write(os.path.join(control, "ocean.stdout"), "native stdout\n")
            snapshot = MaestroProgressObserver(root, workspace, manifest, control).poll([], force=True)
            row = {"candidate_id": "g000_c0000", "generation": 0, "index": 0,
                   "values": {"width": 1e-6}, "status": "running",
                   "execution_progress": snapshot["candidates"][0]["execution_progress"]}
            before = copy.deepcopy(row)
            retained = preserve_live_logs(root, [row])[0]
            self.assertEqual(row, before)
            task = retained["execution_progress"]["tasks"][0]
            self.assertNotEqual(task["spectre_log"], path)
            self.assertEqual(task["original_spectre_log"], path)
            self.assertTrue(task["retained_snapshot"])
            with open(task["spectre_log"], "r") as stream:
                self.assertEqual(stream.read(), "Spectre simulator started\n")
            with open(task["spectre_log"][:-4] + ".meta", "r") as stream:
                meta = stream.read()
            self.assertIn("candidate_id=g000_c0000\n", meta)
            self.assertIn("artifact_test=alias_a\n", meta)
            self.assertIn("point_id=1\n", meta)
            self.assertIn("snapshot_only=true\n", meta)
            self.assertNotIn("completion", retained)
            self.assertNotIn("termination_proven", retained)

    def test_repeated_retention_reuses_and_remaps_existing_valid_snapshot(self):
        with tempfile.TemporaryDirectory() as root:
            workspace, manifest, control = self.fixture(root)
            self.point_log(root)
            snapshot = MaestroProgressObserver(root, workspace, manifest, control).poll([], force=True)
            row = {"candidate_id": "g000_c0000", "generation": 0, "index": 0,
                   "values": {}, "status": "running", "execution_progress": snapshot["candidates"][0]["execution_progress"]}
            first = preserve_live_logs(root, [row])[0]
            second = preserve_live_logs(root, [row])[0]
            self.assertEqual(first["execution_progress"]["tasks"][0]["spectre_log"],
                             second["execution_progress"]["tasks"][0]["spectre_log"])
            self.assertTrue(second["execution_progress"]["tasks"][0]["retained_snapshot"])

    def test_wrong_candidate_snapshot_is_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            workspace, manifest, control = self.fixture(root)
            observer = MaestroProgressObserver(root, workspace, manifest, control)
            observer.poll([], force=True)
            self.assertIsNotNone(read_batch_progress(observer.path, root, ["g000_c0000", "g000_c0001"]))
            self.assertIsNone(read_batch_progress(observer.path, root, ["g000_c0000", "g000_c0002"]))

    def test_duplicate_candidate_snapshot_is_rejected_without_choosing_one(self):
        with tempfile.TemporaryDirectory() as root:
            workspace, manifest, control = self.fixture(root)
            observer = MaestroProgressObserver(root, workspace, manifest, control)
            snapshot = observer.poll([], force=True)
            snapshot["candidates"].append(copy.deepcopy(snapshot["candidates"][0]))
            self.write(observer.path, json.dumps(snapshot))
            self.assertIsNone(read_batch_progress(observer.path, root, ["g000_c0000", "g000_c0001"]))

    def test_snapshot_from_another_native_nonce_is_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            workspace, manifest, control = self.fixture(root)
            observer = MaestroProgressObserver(root, workspace, manifest, control)
            observer.poll([], force=True)
            other = copy.deepcopy(manifest)
            other["protocol_identity"]["nonce"] = "different_current_native_nonce"
            self.write(os.path.join(manifest["batch_root"], "live_identity.json"), json.dumps(other))
            self.assertIsNone(read_batch_progress(observer.path, root, ["g000_c0000", "g000_c0001"]))

    def test_log_symlink_swapped_at_open_cannot_read_foreign_inode(self):
        with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as outside:
            path = self.point_log(root)
            target = self.write(os.path.join(outside, 'private.txt'), 'foreign contents must not be read')
            real_open = os.open
            def swapped(source, flags, *args):
                if source == path:
                    os.unlink(source)
                    os.symlink(target, source)
                return real_open(source, flags, *args)
            with mock.patch('crescendo_engine.maestro_progress.os.open', side_effect=swapped):
                self.assertEqual(maestro_progress.tail_text(path, root), '')

    def test_realtime_reads_release_every_open_file_descriptor(self):
        with tempfile.TemporaryDirectory() as root:
            path = self.point_log(root)
            descriptors = []
            real_open = os.open
            def observed(source, flags, *args):
                descriptor = real_open(source, flags, *args)
                descriptors.append(descriptor)
                return descriptor
            with mock.patch('crescendo_engine.maestro_progress.os.open', side_effect=observed):
                self.assertTrue(maestro_progress.tail_text(path, root))
                self.assertTrue(maestro_progress._read_small_owned(path, root, 8192))
            for descriptor in descriptors:
                with self.assertRaises(OSError):
                    os.fstat(descriptor)

    def test_secondary_run_log_error_keeps_native_job_identity(self):
        with tempfile.TemporaryDirectory() as root:
            workspace, manifest, control = self.fixture(root)
            event = {'invocation_id': 'one', 'argv': [root + '/1/alias_a/run'], 'job_id': '44'}
            self.write(manifest['submission_log_path'], json.dumps(event) + '\n')
            with mock.patch('crescendo_engine.run_log.append_run_event', side_effect=OSError('quota')):
                snapshot = MaestroProgressObserver(root, workspace, manifest, control).poll([], force=True)
            task = snapshot['candidates'][0]['execution_progress']['tasks'][0]
            self.assertEqual(task['job_id'], '44')
            self.assertEqual(task['submit_log'], manifest['submission_log_path'])

    def test_repeated_console_output_is_bounded_but_actual_argv_is_exact(self):
        event = {'argv': ['batch-submit', '-R', 'cpu=6;mem=30000'], 'job_id': '4',
                 'text': 'a' * 60000, 'stdout_tail': 'b' * 60000, 'stderr_tail': 'c' * 60000}
        summary = maestro_progress._submission_summary(event)
        self.assertEqual(summary['argv'], event['argv'])
        self.assertEqual(summary['job_id'], event['job_id'])
        self.assertLessEqual(sum(len(summary[key].encode('utf-8')) for key in
                                 ('text', 'stdout_tail', 'stderr_tail')), 2048)
        self.assertEqual(len(event['text']), 60000)
        self.assertEqual(summary['complete_output_source'], 'submission_log_path')

    def test_observer_does_not_accumulate_full_repeated_submission_output_in_memory(self):
        with tempfile.TemporaryDirectory() as root:
            workspace, manifest, control = self.fixture(root)
            events = [{'event': 'console', 'invocation_id': 'one',
                       'argv': [root + '/1/alias_a/run'], 'text': 'a' * 60000},
                      {'event': 'submit_return', 'invocation_id': 'one',
                       'returncode': 0, 'job_ids': ['4'], 'stdout_tail': 'b' * 60000}]
            self.write(manifest['submission_log_path'], ''.join(json.dumps(row) + '\n' for row in events))
            observer = MaestroProgressObserver(root, workspace, manifest, control)
            snapshot = observer.poll(['SUBMITTED'], force=True)
            held = observer.submissions['one']
            self.assertLessEqual(sum(len((held.get(key) or '').encode('utf-8')) for key in
                                     ('text', 'stdout_tail', 'stderr_tail')), 2048)
            self.assertEqual(held['argv'], events[0]['argv'])
            task = snapshot['candidates'][0]['execution_progress']['tasks'][0]
            self.assertEqual(task['job_id'], '4')
            with open(manifest['submission_log_path']) as stream:
                full = [json.loads(line) for line in stream]
            self.assertEqual(len(full[0]['text']), 60000)
            self.assertEqual(len(full[1]['stdout_tail']), 60000)

    def test_large_snapshot_retains_all_native_tasks_and_warns(self):
        with tempfile.TemporaryDirectory() as root:
            workspace, manifest, control = self.fixture(root)
            observer = MaestroProgressObserver(root, workspace, manifest, control)
            for point in range(30):
                observer.tasks[('g000_c0000', 'alias_a', str(point))] = {
                    'state': 'running', 'test': 'lib:TB:1', 'artifact_test': 'alias_a',
                    'point_id': str(point), 'spectre_log': root + '/' + ('x' * 100)}
            with mock.patch.object(maestro_progress, 'SNAPSHOT_WARNING_BYTES', 4096):
                snapshot = observer.poll(['SUBMITTED'], force=True)
                readback = read_batch_progress(observer.path, root, ['g000_c0000', 'g000_c0001'])
            self.assertEqual(snapshot['telemetry_capacity']['state'], 'large_snapshot')
            self.assertEqual(len(readback['candidates'][0]['execution_progress']['tasks']), 30)
            self.assertEqual(readback['candidates'][0]['execution_progress']['stage'], 'running')

    def test_capacity_overflow_preserves_all_stages_and_complete_task_catalog(self):
        with tempfile.TemporaryDirectory() as root:
            workspace, manifest, control = self.fixture(root)
            observer = MaestroProgressObserver(root, workspace, manifest, control)
            for point in range(80):
                observer.tasks[('g000_c0000', 'alias_a', str(point))] = {
                    'state': 'running', 'test': 'lib:TB:1', 'artifact_test': 'alias_a',
                    'point_id': str(point), 'spectre_log': root + '/' + ('x' * 200)}
            with mock.patch.object(maestro_progress, 'MAX_SNAPSHOT', 8192), \
                    mock.patch.object(maestro_progress, 'SNAPSHOT_WARNING_BYTES', 4096):
                snapshot = observer.poll(['SUBMITTED'], force=True)
                readback = read_batch_progress(observer.path, root, ['g000_c0000', 'g000_c0001'])
            self.assertIsNotNone(readback)
            self.assertEqual(readback['telemetry_capacity']['state'], 'telemetry_capacity_exceeded')
            self.assertEqual(readback['candidates'][0]['execution_progress']['stage'], 'running')
            self.assertEqual(readback['candidates'][0]['execution_progress']['task_count'], 80)
            self.assertEqual(readback['candidates'][1]['execution_progress']['stage'], 'dispatching')
            catalog = snapshot['telemetry_capacity']['complete_catalog_path']
            with open(catalog) as stream:
                complete = json.load(stream)
            self.assertEqual(len(complete['candidates'][0]['execution_progress']['tasks']), 80)
            self.assertEqual(complete['candidates'][0]['execution_progress']['tasks'][-1]['artifact_test'], 'alias_a')
            self.assertNotIn('termination_proven', readback)

    def test_stop_snapshot_deadline_precedes_source_stat_and_shared_log_read(self):
        with tempfile.TemporaryDirectory() as root:
            row = {'candidate_id': 'g000_c0000', 'generation': 0, 'index': 0,
                   'execution_progress': {'tasks': [{'spectre_log': root + '/spectre.out'}],
                                          'shared_logs': [{'path': root + '/ocean.stdout'}]}}
            with mock.patch('crescendo_engine.maestro_progress.time.monotonic', side_effect=[0, 3, 3]), \
                    mock.patch('crescendo_engine.maestro_progress.owned_regular') as inspect:
                retained = preserve_live_logs(root, [row])[0]
            inspect.assert_not_called()
            progress = retained['execution_progress']
            self.assertIn('budget reached', progress['tasks'][0]['retention_warning'])
            self.assertIn('budget reached', progress['shared_logs'][0]['retention_warning'])


if __name__ == "__main__":
    unittest.main()
