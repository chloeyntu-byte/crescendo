"""Retained quarantine, native output handles, and final retry evidence."""
import errno
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

from crescendo_engine.persistent_controller import PersistentControllerProxy
from crescendo_engine.retention import (
    ArtifactCleanupError, _cleanup_open_handles, cleanup_abandoned_controller_epoch,
    clear_retained_spectre_logs, inspect_run_cleanup_scope,
    lightweight_candidate_record, resume_pending_cleanups,
)
from crescendo_engine.utils import load_json
from tests import test_persistent_controller as controller_fixture


class CleanupScopeAndHandlesTests(unittest.TestCase):
    def _proxy(self, root):
        fixture = controller_fixture.PersistentControllerTest()
        script = fixture._fake_script(root)
        return PersistentControllerProxy(
            root, 0, {}, [], fixture._config(root),
            command_builder=lambda bootstrap: [sys.executable, script, bootstrap],
        ).start()

    def test_lightweight_terminal_keeps_real_task_and_retained_log_mapping(self):
        row = {"candidate_id": "g000_c0000", "status": "cancelled",
               "execution_progress": {"stage": "cancelled", "batch_id": "owned_batch",
                   "tasks": [{"test": "source_test", "corner": "TT", "point_id": 3,
                              "spectre_log": "/owned/candidates/g000_c0000/retained_spectre_live_123.log"}]},
               "source_test_map": {"candidate_test": "source_test"}}
        compact = lightweight_candidate_record(row)
        self.assertEqual(compact["execution_progress"], row["execution_progress"])
        self.assertEqual(compact["source_test_map"], row["source_test_map"])

    def test_ui_exit_log_cleanup_preserves_stopped_live_log_pair(self):
        with tempfile.TemporaryDirectory() as root:
            candidate = os.path.join(root, "candidates", "g000_c0000")
            os.makedirs(candidate)
            for name in ("retained_spectre_live_abc.log", "retained_spectre_live_abc.meta",
                         "retained_spectre_ordinary.log", "retained_spectre_ordinary.meta"):
                with open(os.path.join(candidate, name), "w") as stream:
                    stream.write("owned log evidence")
            cleared = clear_retained_spectre_logs(root)
            self.assertEqual(cleared["removed_count"], 2)
            self.assertEqual(sorted(os.listdir(candidate)),
                             ["retained_spectre_live_abc.log", "retained_spectre_live_abc.meta"])
            self.assertEqual(clear_retained_spectre_logs(root, include_live=True)["removed_count"], 2)

    def test_stop_failure_then_close_recovery_keeps_exact_attempt_history(self):
        with tempfile.TemporaryDirectory() as root:
            proxy = self._proxy(root)
            def busy(payload, **_kwargs):
                path = os.path.join(payload, ".nfsfixture")
                with open(path, "w") as stream:
                    stream.write("retained evidence")
                raise OSError(errno.EBUSY, "Device or resource busy", path)
            with mock.patch("crescendo_engine.retention.shutil.rmtree", side_effect=busy):
                stopped = proxy.terminate_and_prove("user Stop")
            self.assertTrue(stopped["process_termination_proven"])
            self.assertTrue(stopped["remote_stop_proven"])
            self.assertFalse(stopped["artifact_cleanup_complete"])
            self.assertEqual(stopped["cleanup_attempt_number"], 1)
            self.assertFalse(os.path.lexists(proxy.epoch_root))
            pending = inspect_run_cleanup_scope(root)
            self.assertFalse(pending["artifact_cleanup_complete"])
            self.assertEqual(len(pending["pending_entries"]), 1)
            self.assertEqual(pending["pending_entries"][0]["artifact_cleanup"]["errno"], errno.EBUSY)
            closed = proxy.close()
            self.assertTrue(closed["artifact_cleanup_complete"])
            self.assertEqual(closed["cleanup_attempt_number"], 2)
            self.assertEqual(closed["cleanup_recovered_from_attempts"], [1])
            previous = closed["cleanup_attempt_history"][0]
            self.assertEqual(previous["reason"], "user Stop")
            self.assertEqual(previous["state"], "artifact_cleanup_failed")
            self.assertEqual(previous["artifact_failures"][0]["errno"], errno.EBUSY)
            self.assertEqual(previous["artifact_failures"][0]["attempt_count"], 3)
            self.assertFalse(stopped["artifact_cleanup_complete"])
            self.assertTrue(inspect_run_cleanup_scope(root)["artifact_cleanup_complete"])
            self.assertEqual(proxy.close(), closed)

    @unittest.skipUnless(os.path.isdir("/proc/self/fd"), "Linux /proc required")
    def test_deletion_failure_records_real_reader_without_signalling_it(self):
        with tempfile.TemporaryDirectory() as root:
            epoch = os.path.join(root, "controllers", "slot_000", "epoch_000")
            os.makedirs(epoch)
            with open(os.path.join(epoch, "controller_liveness.lock"), "w") as stream:
                stream.write("released")
            log = open(os.path.join(epoch, "ocean.stdout"), "w")
            try:
                descriptor = log.fileno()
                def busy(payload, **_kwargs):
                    source = os.path.join(payload, "ocean.stdout")
                    nfs = os.path.join(payload, ".nfsfixture")
                    if os.path.exists(source):
                        os.rename(source, nfs)
                    raise OSError(errno.EBUSY, "Device or resource busy", nfs)
                with mock.patch("crescendo_engine.retention.shutil.rmtree", side_effect=busy), \
                        mock.patch("os.kill") as kill:
                    with self.assertRaises(ArtifactCleanupError) as raised:
                        cleanup_abandoned_controller_epoch(
                            epoch, root, {"ok": True, "kind": "recorded_pid_is_dead"})
                    kill.assert_not_called()
                detail = raised.exception.detail
                self.assertTrue(detail["retryable"])
                self.assertIsNotNone(detail["filesystem"]["type"])
                matches = detail["open_handles"]["matches"]
                match = next(row for row in matches if row["pid"] == os.getpid() and row["fd"] == descriptor)
                self.assertTrue(match["path"].endswith("/.nfsfixture"))
                self.assertTrue(match["this_process"])
                self.assertEqual(match["action"], "retain_and_retry")
                self.assertFalse(log.closed)
            finally:
                log.close()
            self.assertTrue(resume_pending_cleanups(root)[0]["removed"])
            self.assertTrue(inspect_run_cleanup_scope(root)["artifact_cleanup_complete"])

    def test_foreign_unknown_epoch_cannot_be_reported_as_final_cleanup_success(self):
        with tempfile.TemporaryDirectory() as root:
            foreign = os.path.join(root, "workers", "worker_001", "epoch_001")
            os.makedirs(foreign)
            proof = inspect_run_cleanup_scope(root)
            self.assertFalse(proof["artifact_cleanup_complete"])
            self.assertEqual(proof["retained_worker_epochs"], [foreign])
            marker = os.path.join(foreign, "source-schematic.oa")
            with open(marker, "w") as stream:
                stream.write("do not delete")
            self.assertFalse(inspect_run_cleanup_scope(root)["artifact_cleanup_complete"])
            self.assertTrue(os.path.isfile(marker))

    def test_handle_discovery_is_bounded_and_not_process_absence_proof(self):
        with tempfile.TemporaryDirectory() as root:
            result = _cleanup_open_handles(root, max_seconds=0, max_fds=0)
            self.assertTrue(result["limit_reached"])
            self.assertFalse(result["complete"])
            self.assertTrue(result["does_not_prove_process_absence"])
            self.assertEqual(result["scanned_fds"], 0)

    def test_guardian_releases_output_descriptors_before_terminal_proof(self):
        with tempfile.TemporaryDirectory() as root:
            project = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            guardian = os.path.join(project, "tools", "ocean_worker_guardian.py")
            harness = os.path.join(root, "probe.py")
            proof_path = os.path.join(root, "terminal-descriptors.json")
            with open(harness, "w") as stream:
                stream.write("""
import importlib.util, json, os, sys
spec = importlib.util.spec_from_file_location('guardian', sys.argv[1])
guardian = importlib.util.module_from_spec(spec)
spec.loader.exec_module(guardian)
real_record = guardian._record
def observed_record(path, state, status, **values):
    if status in ('exited', 'launch_failed'):
        matches = []
        for fd in os.listdir('/proc/self/fd'):
            try:
                target = os.readlink('/proc/self/fd/' + fd)
            except OSError:
                continue
            if target.endswith('/ocean.stdout') or target.endswith('/ocean.stderr'):
                matches.append(target)
        with open(sys.argv[2], 'w') as stream:
            json.dump({'matches': matches, 'stdio_closed': values.get('stdio_closed')}, stream)
    return real_record(path, state, status, **values)
guardian._record = observed_record
sys.exit(guardian.main(sys.argv[3:]))
""")
            command = [sys.executable, harness, guardian, proof_path,
                       "--state-path", os.path.join(root, "guardian_state.json"),
                       "--stdout-path", os.path.join(root, "ocean.stdout"),
                       "--stderr-path", os.path.join(root, "ocean.stderr"),
                       "--token", "owned-test", "--cwd", root,
                       "--terminate-grace-seconds", "0.2", "--poll-interval-seconds", "0.01",
                       "--disable-pdeathsig", "--poll-expected-parent", "--expected-parent-pid", str(os.getpid()),
                       "--", sys.executable, "-c", "print('native output')"]
            process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            _stdout, stderr = process.communicate(timeout=5)
            self.assertIn(process.returncode, (0, 143), stderr)
            proof = load_json(proof_path)
            self.assertTrue(proof["stdio_closed"])
            self.assertEqual(proof["matches"], [])
            self.assertTrue(load_json(os.path.join(root, "guardian_state.json"))["stdio_closed"])
            failed = subprocess.Popen(command[:-3] + [os.path.join(root, "missing-native-command")],
                                      stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            _stdout, stderr = failed.communicate(timeout=5)
            self.assertEqual(failed.returncode, 127, stderr)
            proof = load_json(proof_path)
            self.assertTrue(proof["stdio_closed"])
            self.assertEqual(proof["matches"], [])
            self.assertEqual(load_json(os.path.join(root, "guardian_state.json"))["status"], "launch_failed")

    @unittest.skipUnless(sys.platform.startswith("linux"), "Linux parent-death signal required")
    def test_parent_death_during_guardian_cleanup_still_kills_owned_child_and_records_proof(self):
        with tempfile.TemporaryDirectory() as root:
            project = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            guardian = getattr(self, "_guardian_script_override",
                               os.path.join(project, "tools", "ocean_worker_guardian.py"))
            state_path = os.path.join(root, "guardian_state.json")
            ready = os.path.join(root, "child-ready")
            child = os.path.join(root, "stubborn-child.py")
            with open(child, "w") as stream:
                stream.write("import os,signal,time\nsignal.signal(signal.SIGTERM, signal.SIG_IGN)\n")
                stream.write("open(%r,'w').write(str(os.getpid()))\nwhile True: time.sleep(.02)\n" % ready)
            parent = os.path.join(root, "owned-parent.py")
            with open(parent, "w") as stream:
                stream.write("""import json, os, subprocess, sys, time
root, guardian, child = sys.argv[1:]
state_path = os.path.join(root, 'guardian_state.json')
command = [sys.executable, guardian, '--state-path', state_path,
           '--stdout-path', os.path.join(root,'ocean.stdout'),
           '--stderr-path', os.path.join(root,'ocean.stderr'), '--token', 'owned-parent-death-test',
           '--cwd', root, '--terminate-grace-seconds', '.4', '--poll-interval-seconds', '.01',
           '--expected-parent-pid', str(os.getpid()), '--', sys.executable, child]
with open(os.path.join(root,'guardian.stderr'),'wb') as errors:
    process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=errors)
deadline = time.monotonic() + 3
while not os.path.isfile(os.path.join(root,'child-ready')):
    if time.monotonic() > deadline: raise RuntimeError('owned child did not start')
    time.sleep(.005)
process.stdin.close()
while True:
    try:
        with open(state_path) as stream: state = json.load(stream)
    except (OSError,ValueError):
        state = {}
    if state.get('status') == 'terminating':
        with open(os.path.join(root,'parent-handoff.json'),'w') as stream:
            json.dump({'guardian_pid': process.pid, 'ocean_pid': state['pid']}, stream)
        os._exit(0)
    if time.monotonic() > deadline: raise RuntimeError('cleanup window not observed')
    time.sleep(.005)
""")
            process = subprocess.Popen([sys.executable, parent, root, guardian, child],
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            handoff = None
            try:
                _stdout, stderr = process.communicate(timeout=5)
                self.assertEqual(process.returncode, 0, stderr)
                handoff = load_json(os.path.join(root, "parent-handoff.json"))
                deadline = time.monotonic() + 5
                state = {}
                while time.monotonic() < deadline:
                    state = load_json(state_path)
                    if state.get("status") in ("exited", "cleanup_failed"):
                        break
                    time.sleep(.01)
                from crescendo_engine.persistent_controller import _process_stat
                self.parent_death_observation = {"state": state,
                    "child_alive": _process_stat(handoff["ocean_pid"]) is not None}
                self.assertEqual(state.get("status"), "exited", state)
                self.assertTrue(state["group_cleanup_complete"])
                self.assertTrue(state["descendant_cleanup_complete"])
                self.assertTrue(state["stdio_closed"])
                self.assertTrue(state["kill_sent"])
                self.assertEqual(state["remaining_group_pids"], [])
                self.assertEqual(state["remaining_adopted_pids"], [])
                from crescendo_engine.persistent_controller import _process_stat
                self.assertIsNone(_process_stat(handoff["ocean_pid"]))
            finally:
                # Exact subprocesses created by this finite fixture only; no
                # global pkill/process-name cleanup is used on test failure.
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=2)
                if handoff:
                    from crescendo_engine.persistent_controller import _process_stat
                    current = _process_stat(handoff["ocean_pid"])
                    if current and current.get("pgid") == handoff["ocean_pid"]:
                        try:
                            os.killpg(handoff["ocean_pid"], signal.SIGKILL)
                        except OSError:
                            pass


if __name__ == "__main__":
    unittest.main()
