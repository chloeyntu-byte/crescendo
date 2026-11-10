import fcntl
import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest

from crescendo_engine.cadence import build_guardian_command, run_command
from crescendo_engine.legacy_guardian import (
    LAUNCH_TOKEN_ENVIRONMENT, acquire_legacy_guardian_launch_lock,
    close_legacy_guardian_launch_lock, inspect_legacy_guardian_attempt,
    legacy_guardian_parent_identity, quiesce_legacy_guardian_attempt,
    write_legacy_guardian_manifest,
)


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GUARDIAN = os.path.join(PROJECT_ROOT, "tools", "ocean_worker_guardian.py")


def write_script(path, body):
    with open(path, "w") as stream:
        stream.write(body)


def read_json(path):
    try:
        with open(path) as stream:
            return json.load(stream)
    except (IOError, OSError, ValueError):
        return None


def wait_for(predicate, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.02)
    raise AssertionError("condition was not satisfied within %.1f seconds" % timeout)


def process_running(pid):
    try:
        with open("/proc/%d/stat" % int(pid)) as stream:
            return stream.read().split()[2] != "Z"
    except (IOError, OSError, IndexError):
        return False


LAUNCH_ID = "durable-launch"
CANCEL_ID = "cancel"
TIMEOUT_ID = "timeout"
CONFLICT_ID = "identity-conflict"


def make_durable_attempt(root, command, token=LAUNCH_ID):
    workspace = os.path.join(root, "workspace")
    attempt = os.path.join(root, "attempt_1")
    os.makedirs(workspace)
    os.makedirs(attempt)
    state = os.path.join(attempt, "guardian_state.json")
    stdout = os.path.join(attempt, "ocean.stdout")
    stderr = os.path.join(attempt, "ocean.stderr")
    descriptor, lock_path = acquire_legacy_guardian_launch_lock(attempt)
    parent = legacy_guardian_parent_identity()
    guardian_argv = build_guardian_command(
        command, workspace, stdout, stderr, state, token,
        guardian_launch_lock_fd=descriptor,
    )
    write_legacy_guardian_manifest(
        attempt, workspace, state, stdout, stderr, token, command,
        parent["pid"], GUARDIAN, launch_lock_path=lock_path,
        expected_parent_boot_id=parent["boot_id"],
        expected_parent_start_ticks=parent["start_ticks"],
        expected_uid=parent["uid"], guardian_argv=guardian_argv,
    )
    return workspace, attempt, state, stdout, stderr, descriptor, lock_path


class LegacyLaunchLockTest(unittest.TestCase):
    def assert_descriptor_closed(self, descriptor):
        with self.assertRaises(OSError):
            os.fstat(descriptor)

    def write_guardian_state(self, attempt, state_path, status, **updates):
        manifest = read_json(os.path.join(attempt, "legacy_guardian_launch.json"))
        state = {
            "schema_version": "crescendo.ocean_worker_guardian.v1",
            "token": manifest["token"],
            "cwd": manifest["workspace_root"],
            "boot_id": manifest["expected_parent_boot_id"],
            "launch_boot_id": manifest["expected_parent_boot_id"],
            "guardian_pid": 99999991,
            "guardian_start_ticks": 123,
            "pid": None, "pgid": None, "sid": None, "pid_start_ticks": None,
            "argv": manifest["argv"],
            "expected_parent_pid": manifest["expected_parent_pid"],
            "status": status,
        }
        state.update(updates)
        with open(state_path, "w") as stream:
            json.dump(state, stream)
        return state

    def test_run_command_owns_lock_fd_on_cancel_validation_timeout_and_popen_error(self):
        with tempfile.TemporaryDirectory() as root:
            workspace = os.path.join(root, "workspace")
            os.makedirs(workspace)

            def descriptor():
                attempt = tempfile.mkdtemp(dir=root)
                return acquire_legacy_guardian_launch_lock(attempt)[0]

            cancelled_fd = descriptor()
            cancelled = run_command(
                [sys.executable, "-c", "raise SystemExit(0)"], workspace,
                os.environ.copy(), 1, stdout_path=os.path.join(root, "cancel.out"),
                stderr_path=os.path.join(root, "cancel.err"),
                guardian_state_path=os.path.join(root, "cancel.state"),
                guardian_token=CANCEL_ID, guardian_launch_lock_fd=cancelled_fd,
                cancel_check=lambda: True,
            )
            self.assertTrue(cancelled["cancelled"])
            self.assert_descriptor_closed(cancelled_fd)

            invalid_fd = descriptor()
            with self.assertRaises(ValueError):
                run_command(
                    [sys.executable, "-c", "raise SystemExit(0)"], workspace,
                    os.environ.copy(), 1, guardian_launch_lock_fd=invalid_fd,
                )
            self.assert_descriptor_closed(invalid_fd)

            startup_lock = threading.Lock()
            startup_lock.acquire()
            timeout_fd = descriptor()
            try:
                timed_out = run_command(
                    [sys.executable, "-c", "raise SystemExit(0)"], workspace,
                    os.environ.copy(), 1, stdout_path=os.path.join(root, "timeout.out"),
                    stderr_path=os.path.join(root, "timeout.err"),
                    guardian_state_path=os.path.join(root, "timeout.state"),
                    guardian_token=TIMEOUT_ID, guardian_launch_lock_fd=timeout_fd,
                    startup_lock=startup_lock, lock_timeout_seconds=0.05,
                )
            finally:
                startup_lock.release()
            self.assertTrue(timed_out["startup_timed_out"])
            self.assert_descriptor_closed(timeout_fd)

            popen_fd = descriptor()
            with self.assertRaises(OSError):
                run_command(
                    [sys.executable, "-c", "raise SystemExit(0)"],
                    os.path.join(root, "missing-workspace"), os.environ.copy(), 1,
                    stdout_path=os.path.join(root, "popen.out"),
                    stderr_path=os.path.join(root, "popen.err"),
                    guardian_state_path=os.path.join(root, "popen.state"),
                    guardian_token="popen", guardian_launch_lock_fd=popen_fd,
                )
            self.assert_descriptor_closed(popen_fd)

    def test_idle_durable_lock_and_stable_no_match_proves_not_spawned(self):
        with tempfile.TemporaryDirectory() as root:
            fake = os.path.join(root, "fake.py")
            write_script(fake, "raise SystemExit(0)\n")
            workspace, attempt, _state, _stdout, _stderr, descriptor, _lock = (
                make_durable_attempt(root, [sys.executable, fake])
            )
            close_legacy_guardian_launch_lock(descriptor)
            inspection = inspect_legacy_guardian_attempt(attempt, workspace)
            self.assertEqual(inspection["state"], "dead")
            self.assertEqual(inspection["termination_proof"]["kind"], "ocean_not_spawned")

    def test_released_starting_state_without_ocean_identity_proves_not_spawned(self):
        with tempfile.TemporaryDirectory() as root:
            fake = os.path.join(root, "fake.py")
            write_script(fake, "raise SystemExit(0)\n")
            workspace, attempt, state_path, _stdout, _stderr, descriptor, _lock = (
                make_durable_attempt(root, [sys.executable, fake])
            )
            self.write_guardian_state(attempt, state_path, "starting")
            close_legacy_guardian_launch_lock(descriptor)
            inspection = inspect_legacy_guardian_attempt(attempt, workspace)
            self.assertEqual(inspection["state"], "dead")
            self.assertEqual(
                inspection["termination_proof"]["kind"],
                "owned_processes_absent_after_guardian_loss",
            )

    def test_released_running_state_without_live_token_process_fails_closed(self):
        with tempfile.TemporaryDirectory() as root:
            fake = os.path.join(root, "fake.py")
            write_script(fake, "raise SystemExit(0)\n")
            workspace, attempt, state_path, _stdout, _stderr, descriptor, _lock = (
                make_durable_attempt(root, [sys.executable, fake])
            )
            self.write_guardian_state(attempt, state_path, "running")
            close_legacy_guardian_launch_lock(descriptor)
            inspection = inspect_legacy_guardian_attempt(attempt, workspace)
            self.assertEqual(inspection["state"], "ambiguous")
            self.assertFalse(inspection["termination_proof"]["ok"])

    def test_released_starting_state_with_historical_ocean_identity_fails_closed(self):
        with tempfile.TemporaryDirectory() as root:
            fake = os.path.join(root, "fake.py")
            write_script(fake, "raise SystemExit(0)\n")
            workspace, attempt, state_path, _stdout, _stderr, descriptor, _lock = (
                make_durable_attempt(root, [sys.executable, fake])
            )
            self.write_guardian_state(
                attempt, state_path, "starting", pid=99999992, pgid=99999992,
                sid=99999992, pid_start_ticks=456,
            )
            close_legacy_guardian_launch_lock(descriptor)
            inspection = inspect_legacy_guardian_attempt(attempt, workspace)
            self.assertEqual(inspection["state"], "ambiguous")
            self.assertFalse(inspection["termination_proof"]["ok"])

    def test_held_launch_lock_is_active_and_never_cleaned(self):
        with tempfile.TemporaryDirectory() as root:
            fake = os.path.join(root, "fake.py")
            write_script(fake, "raise SystemExit(0)\n")
            workspace, attempt, _state, _stdout, _stderr, descriptor, _lock = (
                make_durable_attempt(root, [sys.executable, fake])
            )
            try:
                inspection = inspect_legacy_guardian_attempt(attempt, workspace)
                self.assertEqual(inspection["state"], "active")
                self.assertTrue(inspection["launch_handshake"])
                quiesced = quiesce_legacy_guardian_attempt(
                    attempt, workspace, timeout_seconds=0.12, terminate_owned=True,
                )
                self.assertEqual(quiesced["state"], "ambiguous")
                self.assertIn("handshake", quiesced["reason"])
            finally:
                close_legacy_guardian_launch_lock(descriptor)
            self.assertEqual(inspect_legacy_guardian_attempt(attempt, workspace)["state"], "dead")

    def test_guarded_popen_transfers_lock_and_guardian_releases_after_state(self):
        with tempfile.TemporaryDirectory() as root:
            fake = os.path.join(root, "fake.py")
            write_script(fake, "import time\nwhile True: time.sleep(1)\n")
            command = [sys.executable, fake]
            workspace, attempt, state, stdout, stderr, descriptor, lock_path = (
                make_durable_attempt(root, command)
            )
            result = {}

            def target():
                result.update(run_command(
                    command, workspace, os.environ.copy(), 60,
                    stdout_path=stdout, stderr_path=stderr,
                    guardian_state_path=state, guardian_token=LAUNCH_ID,
                    guardian_launch_lock_fd=descriptor,
                ))

            thread = threading.Thread(target=target)
            thread.start()
            running = wait_for(
                lambda: read_json(state)
                if read_json(state) and read_json(state).get("status") == "running" else None,
            )
            self.assertTrue(running["launch_lock_released"])
            probe = os.open(lock_path, os.O_RDWR)
            try:
                fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
            finally:
                os.close(probe)
            stopped = quiesce_legacy_guardian_attempt(
                attempt, workspace, timeout_seconds=5, terminate_owned=True,
            )
            self.assertEqual(stopped["state"], "dead")
            thread.join(timeout=5)
            self.assertFalse(thread.is_alive())
            self.assertFalse(result["termination_failed"])

    def test_token_process_with_wrong_identity_fails_closed(self):
        with tempfile.TemporaryDirectory() as root:
            fake = os.path.join(root, "fake.py")
            write_script(fake, "raise SystemExit(0)\n")
            workspace, attempt, _state, _stdout, _stderr, descriptor, _lock = (
                make_durable_attempt(root, [sys.executable, fake], token=CONFLICT_ID)
            )
            close_legacy_guardian_launch_lock(descriptor)
            environment = dict(os.environ)
            environment[LAUNCH_TOKEN_ENVIRONMENT] = CONFLICT_ID
            conflict = subprocess.Popen(
                [sys.executable, "-c", "import time; time.sleep(60)"],
                cwd=workspace, env=environment, start_new_session=True,
            )
            try:
                inspection = inspect_legacy_guardian_attempt(attempt, workspace)
                self.assertEqual(inspection["state"], "ambiguous")
                self.assertEqual(
                    inspection["termination_proof"]["kind"], "process_identity_conflict",
                )
            finally:
                os.killpg(conflict.pid, signal.SIGKILL)
                conflict.wait()

    def test_parent_sigkill_after_fork_before_exec_keeps_lock_until_child_settles(self):
        with tempfile.TemporaryDirectory() as root:
            driver = os.path.join(root, "driver.py")
            fake = os.path.join(root, "fake.py")
            ready = os.path.join(root, "forked.json")
            spawned = os.path.join(root, "spawned")
            write_script(fake, "open(%r, 'w').write('unexpected')\n" % spawned)
            write_script(driver, r'''
import json, os, signal, sys, time
from crescendo_engine.cadence import build_guardian_command
from crescendo_engine.legacy_guardian import (
    acquire_legacy_guardian_launch_lock, legacy_guardian_parent_identity,
    write_legacy_guardian_manifest,
)
root, guardian, fake, ready = sys.argv[1:]
workspace = os.path.join(root, 'workspace')
attempt = os.path.join(root, 'attempt_1')
os.makedirs(workspace)
os.makedirs(attempt)
state = os.path.join(attempt, 'guardian_state.json')
stdout = os.path.join(attempt, 'ocean.stdout')
stderr = os.path.join(attempt, 'ocean.stderr')
command = [sys.executable, fake]
descriptor, lock_path = acquire_legacy_guardian_launch_lock(attempt)
parent = legacy_guardian_parent_identity()
wrapper = build_guardian_command(
    command, workspace, stdout, stderr, state, 'fork-window',
    guardian_launch_lock_fd=descriptor,
)
write_legacy_guardian_manifest(
    attempt, workspace, state, stdout, stderr, 'fork-window', command,
    parent['pid'], guardian, launch_lock_path=lock_path,
    expected_parent_boot_id=parent['boot_id'],
    expected_parent_start_ticks=parent['start_ticks'], expected_uid=parent['uid'],
    guardian_argv=wrapper,
)
child = os.fork()
if child == 0:
    os.setsid()
    os.set_inheritable(descriptor, True)
    os.kill(os.getpid(), signal.SIGSTOP)
    environment = dict(os.environ)
    environment['CRESCENDO_LEGACY_GUARDIAN_TOKEN'] = 'fork-window'
    os.execve(wrapper[0], wrapper, environment)
with open(ready, 'w') as stream:
    json.dump({'child': child, 'attempt': attempt, 'workspace': workspace}, stream)
    stream.flush()
    os.fsync(stream.fileno())
while True:
    time.sleep(1)
''')
            environment = dict(os.environ)
            environment["PYTHONPATH"] = PROJECT_ROOT
            process = subprocess.Popen(
                [sys.executable, driver, root, GUARDIAN, fake, ready], env=environment,
            )
            child = None
            try:
                launch = wait_for(lambda: read_json(ready))
                child = int(launch["child"])
                wait_for(lambda: process_running(child))
                os.kill(process.pid, signal.SIGKILL)
                process.wait(timeout=2)
                held = inspect_legacy_guardian_attempt(launch["attempt"], launch["workspace"])
                self.assertEqual(held["state"], "active")
                self.assertTrue(held["launch_handshake"])
                os.kill(child, signal.SIGCONT)
                state_path = os.path.join(launch["attempt"], "guardian_state.json")
                wait_for(
                    lambda: read_json(state_path)
                    if read_json(state_path) and read_json(state_path).get("status") == "exited" else None,
                )
                wait_for(lambda: not process_running(child))
                final = inspect_legacy_guardian_attempt(launch["attempt"], launch["workspace"])
                self.assertEqual(final["state"], "dead")
                self.assertFalse(os.path.exists(spawned))
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait()
                if child and process_running(child):
                    try:
                        os.killpg(child, signal.SIGKILL)
                    except OSError:
                        os.kill(child, signal.SIGKILL)


if __name__ == "__main__":
    unittest.main()
