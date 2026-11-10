import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import uuid
from unittest import mock

from crescendo_engine.cadence import run_command
from crescendo_engine.legacy_guardian import (
    _ocean_identity_matches,
    _orphan_scope_identity_matches,
    _recover_orphaned_ocean_after_guardian_loss,
    acquire_legacy_guardian_launch_lock,
    build_guardian_token_identity,
    close_legacy_guardian_launch_lock,
    inspect_legacy_guardian_attempt,
    legacy_guardian_parent_identity,
    quiesce_legacy_guardian_attempt,
    write_legacy_guardian_manifest,
)


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GUARDIAN = os.path.join(PROJECT_ROOT, "tools", "ocean_worker_guardian.py")


def write_script(path, body):
    with open(path, "w") as stream:
        stream.write(body)


def load_json(path):
    with open(path) as stream:
        return json.load(stream)


def wait_for(predicate, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.02)
    raise AssertionError("condition was not satisfied within %.1f seconds" % timeout)


def read_state(path):
    try:
        return load_json(path)
    except (IOError, OSError, ValueError):
        return None


def process_is_running(pid):
    try:
        os.kill(int(pid), 0)
    except OSError:
        return False
    try:
        with open("/proc/%d/stat" % int(pid)) as stream:
            return stream.read().split()[2] != "Z"
    except (IOError, OSError, IndexError):
        return True


ATTEMPT_ID = "legacy-test"


def make_attempt(root, command, expected_parent_pid=None, token=ATTEMPT_ID):
    workspace = os.path.join(root, "workspace")
    attempt = os.path.join(root, "attempt_1")
    os.makedirs(workspace)
    os.makedirs(attempt)
    state = os.path.join(attempt, "guardian_state.json")
    stdout = os.path.join(attempt, "ocean.stdout")
    stderr = os.path.join(attempt, "ocean.stderr")
    write_legacy_guardian_manifest(
        attempt, workspace, state, stdout, stderr, token, command,
        expected_parent_pid or os.getpid(), GUARDIAN,
    )
    return workspace, attempt, state, stdout, stderr


class LegacyGuardianIntegrationTest(unittest.TestCase):
    def test_unscoped_orphan_recovery_and_helper_identity_change_fail_closed(self):
        manifest = {
            "protocol": {
                "durable_launch_liveness_lock": True,
                "guardian_inherits_launch_lock": True,
                "guardian_state_before_launch_lock_release": True,
                "stable_proc_double_scan": True,
            },
            "launch_lock_path": "/tmp/launch.lock",
            "expected_parent_boot_id": "boot",
            "expected_parent_start_ticks": 1,
            "expected_uid": os.getuid(),
            "guardian_argv": ["guardian"],
        }
        with mock.patch("crescendo_engine.legacy_guardian.os.kill") as kill:
            refused = _recover_orphaned_ocean_after_guardian_loss(
                {"manifest": manifest, "guardian": {}, "processes": []},
                time.monotonic() + 1.0,
            )
        self.assertEqual(refused["state"], "ambiguous")
        self.assertEqual(
            refused["termination_proof"]["kind"],
            "orphan_recovery_identity_incomplete",
        )
        kill.assert_not_called()

        record = {
            "kind": "scoped_token_helper", "pid": 41001,
            "start_ticks": 1234, "ppid": 41000, "pgrp": 41001,
            "session": 41001, "uid": os.getuid(), "cwd": "/tmp/workspace/subdir",
            "argv": ["/opt/cadence/helper"], "launch_token": "crescendo2-" + "a" * 64,
        }
        snapshot = dict(record, alive=True)
        snapshot.pop("kind")
        self.assertTrue(_orphan_scope_identity_matches(record, snapshot))
        for name, value in (
                ("start_ticks", 1235), ("ppid", 1), ("pgrp", 41002),
                ("session", 41002), ("uid", os.getuid() + 1),
                ("cwd", "/tmp/workspace/other"), ("argv", ["/bin/false"]),
                ("launch_token", "crescendo2-" + "b" * 64)):
            changed = dict(snapshot)
            changed[name] = value
            self.assertFalse(
                _orphan_scope_identity_matches(record, changed),
                msg="detached helper accepted changed %s" % name,
            )

    def _scoped_prestate(self, root, scope_token="a" * 32):
        workspace = os.path.join(root, "workspace")
        attempt = os.path.join(root, "attempt_1")
        os.makedirs(workspace)
        os.makedirs(attempt)
        state = os.path.join(attempt, "guardian_state.json")
        stdout = os.path.join(attempt, "ocean.stdout")
        stderr = os.path.join(attempt, "ocean.stderr")
        command = ["/opt/eda/bin/ocean", "-nograph"]
        identity = build_guardian_token_identity(
            scope_token, "worker-000-epoch-000", 0,
        )
        descriptor, lock_path = acquire_legacy_guardian_launch_lock(attempt)
        parent = legacy_guardian_parent_identity()
        guardian_command = [
            sys.executable, GUARDIAN,
            "--state-path", state,
            "--stdout-path", stdout,
            "--stderr-path", stderr,
            "--token", identity["token"],
            "--cwd", workspace,
            "--expected-parent-pid", str(parent["pid"]),
            "--launch-lock-fd", str(descriptor),
            "--",
        ] + command
        write_legacy_guardian_manifest(
            attempt, workspace, state, stdout, stderr, identity["token"], command,
            parent["pid"], GUARDIAN, launch_lock_path=lock_path,
            expected_parent_boot_id=parent["boot_id"],
            expected_parent_start_ticks=parent["start_ticks"],
            expected_uid=parent["uid"], guardian_argv=guardian_command,
            guardian_token_identity=identity,
        )
        return {
            "attempt": attempt, "workspace": workspace, "state": state,
            "descriptor": descriptor, "identity": identity, "parent": parent,
            "command": command,
        }

    def _start_scoped_guardian(self, root, command, environment=None):
        workspace = os.path.join(root, "workspace")
        attempt = os.path.join(root, "attempt_1")
        if not os.path.isdir(workspace):
            os.makedirs(workspace)
        os.makedirs(attempt)
        state = os.path.join(attempt, "guardian_state.json")
        stdout = os.path.join(attempt, "ocean.stdout")
        stderr = os.path.join(attempt, "ocean.stderr")
        identity = build_guardian_token_identity(
            uuid.uuid4().hex, "guardian-sigkill-probe", 0,
        )
        descriptor, lock_path = acquire_legacy_guardian_launch_lock(attempt)
        parent = legacy_guardian_parent_identity()
        guardian_command = [
            sys.executable, GUARDIAN,
            "--state-path", state,
            "--stdout-path", stdout,
            "--stderr-path", stderr,
            "--token", identity["token"],
            "--cwd", workspace,
            "--expected-parent-pid", str(parent["pid"]),
            "--launch-lock-fd", str(descriptor),
            "--terminate-grace-seconds", "0.08",
            "--poll-interval-seconds", "0.01",
            "--",
        ] + list(command)
        write_legacy_guardian_manifest(
            attempt, workspace, state, stdout, stderr, identity["token"], command,
            parent["pid"], GUARDIAN, launch_lock_path=lock_path,
            expected_parent_boot_id=parent["boot_id"],
            expected_parent_start_ticks=parent["start_ticks"],
            expected_uid=parent["uid"], guardian_argv=guardian_command,
            guardian_token_identity=identity,
        )
        process = subprocess.Popen(
            guardian_command, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL, cwd=workspace,
            env=environment or os.environ.copy(), start_new_session=True,
            pass_fds=(descriptor,),
        )
        close_legacy_guardian_launch_lock(descriptor)
        wait_for(
            lambda: read_state(state)
            if read_state(state) and read_state(state).get("status") == "running" else None,
        )
        return {
            "attempt": attempt, "workspace": workspace, "state": state,
            "identity": identity, "process": process,
        }

    def test_scoped_manifest_before_state_is_safe_after_launch_lock_release(self):
        with tempfile.TemporaryDirectory() as root:
            launch = self._scoped_prestate(root)
            close_legacy_guardian_launch_lock(launch["descriptor"])
            inspection = inspect_legacy_guardian_attempt(
                launch["attempt"], launch["workspace"], launch["identity"],
            )
            self.assertEqual(inspection["state"], "dead")
            self.assertEqual(inspection["termination_proof"]["kind"], "ocean_not_spawned")

    def test_scoped_manifest_and_guardian_token_tampering_fail_closed(self):
        with tempfile.TemporaryDirectory() as root:
            launch = self._scoped_prestate(root)
            close_legacy_guardian_launch_lock(launch["descriptor"])
            manifest_path = os.path.join(
                launch["attempt"], "legacy_guardian_launch.json",
            )
            manifest = load_json(manifest_path)
            original = json.loads(json.dumps(manifest))
            manifest["guardian_token_identity"]["scope_token"] = "b" * 32
            with open(manifest_path, "w") as stream:
                json.dump(manifest, stream)
            inspection = inspect_legacy_guardian_attempt(
                launch["attempt"], launch["workspace"], launch["identity"],
            )
            self.assertEqual(inspection["state"], "ambiguous")
            self.assertIn("digest does not match", inspection["reason"])

            with open(manifest_path, "w") as stream:
                json.dump(original, stream)
            with open(launch["state"], "w") as stream:
                json.dump({
                    "schema_version": "crescendo.ocean_worker_guardian.v1",
                    "token": "crescendo2-" + "f" * 64,
                    "cwd": launch["workspace"], "argv": launch["command"],
                    "expected_parent_pid": launch["parent"]["pid"],
                    "guardian_pid": None, "pid": None, "pgid": None,
                    "status": "launch_failed",
                }, stream)
            inspection = inspect_legacy_guardian_attempt(
                launch["attempt"], launch["workspace"], launch["identity"],
            )
            self.assertEqual(inspection["state"], "ambiguous")
            self.assertIn("state token does not match", inspection["reason"])

    def test_scoped_exec_continuation_requires_exact_non_command_identity(self):
        with tempfile.TemporaryDirectory() as root:
            launch = self._scoped_prestate(root)
            different_cwd = os.path.join(root, "different-workspace")
            os.makedirs(different_cwd)
            try:
                manifest = load_json(os.path.join(
                    launch["attempt"], "legacy_guardian_launch.json",
                ))
                guardian = {
                    "pid": 41001,
                    "pgid": 41001,
                    "sid": 41001,
                    "pid_start_ticks": 987654,
                }
                snapshot = {
                    "alive": True,
                    "pid": 41001,
                    "start_ticks": 987654,
                    "pgrp": 41001,
                    "session": 41001,
                    "uid": manifest["expected_uid"],
                    "cwd": launch["workspace"],
                    "argv": [
                        "/bin/ksh", "/opt/eda/cadence/virtuoso",
                        "-ocean", "-nograph",
                    ],
                    "launch_token": manifest["token"],
                }
                self.assertNotEqual(snapshot["argv"], manifest["argv"])
                self.assertTrue(_ocean_identity_matches(snapshot, manifest, guardian))

                mutations = (
                    ("pid", 41002),
                    ("start_ticks", 987655),
                    ("pgrp", 41002),
                    ("session", 41002),
                    ("uid", manifest["expected_uid"] + 1),
                    ("cwd", different_cwd),
                    ("launch_token", "crescendo2-" + "0" * 64),
                )
                for name, value in mutations:
                    changed = dict(snapshot)
                    changed[name] = value
                    self.assertFalse(
                        _ocean_identity_matches(changed, manifest, guardian),
                        msg="scoped exec continuation accepted changed %s" % name,
                    )

                unscoped = dict(manifest)
                unscoped.pop("guardian_token_identity")
                self.assertFalse(_ocean_identity_matches(snapshot, unscoped, guardian))
                launch_snapshot = dict(snapshot, argv=list(manifest["argv"]))
                self.assertTrue(_ocean_identity_matches(launch_snapshot, unscoped, guardian))
            finally:
                close_legacy_guardian_launch_lock(launch["descriptor"])

    def test_old_manifest_missing_state_fails_closed_even_with_dead_launch_owner(self):
        with tempfile.TemporaryDirectory() as root:
            fake = os.path.join(root, "fake_ocean.py")
            write_script(fake, "raise SystemExit(0)\n")
            command = [sys.executable, fake]
            workspace, attempt, _state, _stdout, _stderr = make_attempt(
                root, command, expected_parent_pid=99999999,
            )
            inspection = inspect_legacy_guardian_attempt(attempt, workspace)
            self.assertEqual(inspection["state"], "ambiguous")
            self.assertEqual(inspection["termination_proof"]["kind"], "missing_launch_lock_proof")

            os.unlink(os.path.join(attempt, "legacy_guardian_launch.json"))
            ambiguous = inspect_legacy_guardian_attempt(attempt, workspace)
            self.assertEqual(ambiguous["state"], "ambiguous")
            self.assertFalse(ambiguous["termination_proof"]["ok"])

    def test_guarded_run_preserves_output_exit_code_and_terminal_proof(self):
        with tempfile.TemporaryDirectory() as root:
            fake = os.path.join(root, "fake_ocean.py")
            write_script(fake, "import sys\nprint('ocean-out')\nprint('ocean-err', file=sys.stderr)\nsys.exit(7)\n")
            command = [sys.executable, fake]
            workspace, attempt, state, stdout, stderr = make_attempt(root, command)
            result = run_command(
                command, workspace, os.environ.copy(), 5,
                stdout_path=stdout, stderr_path=stderr,
                guardian_state_path=state, guardian_token=ATTEMPT_ID,
            )
            self.assertEqual(result["returncode"], 7)
            self.assertFalse(result["termination_failed"])
            self.assertIn("ocean-out", result["stdout_tail"])
            self.assertIn("ocean-err", result["stderr_tail"])
            inspection = inspect_legacy_guardian_attempt(attempt, workspace)
            self.assertEqual(inspection["state"], "dead")
            self.assertTrue(inspection["termination_proof"]["ok"])

    def test_guarded_timeout_cleans_stubborn_ocean_and_descendant(self):
        with tempfile.TemporaryDirectory() as root:
            fake = os.path.join(root, "stubborn_ocean.py")
            pids_path = os.path.join(root, "pids.txt")
            write_script(fake, """
import os
import signal
import subprocess
import sys
import time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
child = subprocess.Popen([
    sys.executable, '-c',
    'import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(60)',
])
with open(os.environ['LEGACY_GUARDIAN_PIDS'], 'w') as stream:
    stream.write('%d %d\\n' % (os.getpid(), child.pid))
    stream.flush()
    os.fsync(stream.fileno())
while True:
    time.sleep(1)
""")
            command = [sys.executable, fake]
            workspace, attempt, state, stdout, stderr = make_attempt(root, command)
            environment = dict(os.environ)
            environment["LEGACY_GUARDIAN_PIDS"] = pids_path
            result = run_command(
                # Include guardian + two Python starts on a loaded host; the
                # assertion still requires an actual timeout and dead descendants.
                command, workspace, environment, 1.0,
                stdout_path=stdout, stderr_path=stderr,
                guardian_state_path=state, guardian_token=ATTEMPT_ID,
                termination_grace_seconds=1.5, termination_kill_seconds=1,
            )
            self.assertTrue(result["timed_out"])
            self.assertFalse(result["termination_failed"])
            with open(pids_path) as stream:
                ocean_pid, descendant_pid = [int(value) for value in stream.read().split()]
            wait_for(lambda: not process_is_running(ocean_pid))
            wait_for(lambda: not process_is_running(descendant_pid))
            self.assertEqual(inspect_legacy_guardian_attempt(attempt, workspace)["state"], "dead")

    def test_parent_sigkill_closes_pipe_and_guardian_cleans_descendants(self):
        with tempfile.TemporaryDirectory() as root:
            fake = os.path.join(root, "stubborn_ocean.py")
            driver = os.path.join(root, "driver.py")
            pids_path = os.path.join(root, "pids.txt")
            write_script(fake, """
import os
import signal
import subprocess
import sys
import time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
child = subprocess.Popen([
    sys.executable, '-c',
    'import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(60)',
])
with open(os.environ['LEGACY_GUARDIAN_PIDS'], 'w') as stream:
    stream.write('%d %d\\n' % (os.getpid(), child.pid))
    stream.flush()
    os.fsync(stream.fileno())
while True:
    time.sleep(1)
""")
            write_script(driver, """
import os
import sys
from crescendo_engine.cadence import run_command
from crescendo_engine.legacy_guardian import write_legacy_guardian_manifest
root, guardian, fake = sys.argv[1:]
workspace = os.path.join(root, 'workspace')
attempt = os.path.join(root, 'attempt_1')
os.makedirs(workspace)
os.makedirs(attempt)
state = os.path.join(attempt, 'guardian_state.json')
stdout = os.path.join(attempt, 'ocean.stdout')
stderr = os.path.join(attempt, 'ocean.stderr')
command = [sys.executable, fake]
write_legacy_guardian_manifest(
    attempt, workspace, state, stdout, stderr, 'pkill', command,
    os.getpid(), guardian,
)
run_command(
    command, workspace, os.environ.copy(), 60,
    stdout_path=stdout, stderr_path=stderr,
    guardian_state_path=state, guardian_token='pkill',
    termination_grace_seconds=1.5, termination_kill_seconds=1,
)
""")
            environment = dict(os.environ)
            environment["PYTHONPATH"] = PROJECT_ROOT
            environment["LEGACY_GUARDIAN_PIDS"] = pids_path
            process = subprocess.Popen(
                [sys.executable, driver, root, GUARDIAN, fake], env=environment,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            state_path = os.path.join(root, "attempt_1", "guardian_state.json")
            try:
                wait_for(lambda: os.path.isfile(pids_path) and os.path.getsize(pids_path) > 0)
                running = wait_for(
                    lambda: read_state(state_path)
                    if read_state(state_path) and read_state(state_path).get("status") == "running" else None,
                )
                with open(pids_path) as stream:
                    ocean_pid, descendant_pid = [int(value) for value in stream.read().split()]
                os.kill(process.pid, signal.SIGKILL)
                process.wait(timeout=2)
                final = wait_for(
                    lambda: read_state(state_path)
                    if read_state(state_path) and read_state(state_path).get("status") == "exited" else None,
                    timeout=5,
                )
                self.assertEqual(running["expected_parent_pid"], process.pid)
                self.assertTrue(final["group_cleanup_complete"])
                wait_for(lambda: not process_is_running(ocean_pid))
                wait_for(lambda: not process_is_running(descendant_pid))
                inspection = quiesce_legacy_guardian_attempt(
                    os.path.join(root, "attempt_1"), os.path.join(root, "workspace"),
                    timeout_seconds=5, terminate_owned=False,
                )
                self.assertEqual(inspection["state"], "dead")
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait()

    def test_guardian_sigkill_recovers_only_exact_scoped_ocean_group(self):
        with tempfile.TemporaryDirectory() as root:
            fake = os.path.join(root, "stubborn_ocean.py")
            pids_path = os.path.join(root, "pids.txt")
            write_script(fake, """
import os
import signal
import subprocess
import sys
import time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
child = subprocess.Popen([
    sys.executable, '-c',
    'import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(60)',
])
with open(os.environ['LEGACY_GUARDIAN_PIDS'], 'w') as stream:
    stream.write('%d %d\\n' % (os.getpid(), child.pid))
    stream.flush()
    os.fsync(stream.fileno())
while True:
    time.sleep(1)
""")
            environment = dict(os.environ)
            environment["LEGACY_GUARDIAN_PIDS"] = pids_path
            launch = self._start_scoped_guardian(
                root, [sys.executable, fake], environment=environment,
            )
            guardian = launch["process"]
            unrelated = subprocess.Popen(
                [sys.executable, "-c", "import time; time.sleep(60)"],
                cwd=launch["workspace"], start_new_session=True,
            )
            ocean_pid = None
            child_pid = None
            try:
                wait_for(lambda: os.path.isfile(pids_path) and os.path.getsize(pids_path) > 0)
                with open(pids_path) as stream:
                    ocean_pid, child_pid = [int(value) for value in stream.read().split()]
                os.kill(guardian.pid, signal.SIGKILL)
                self.assertEqual(guardian.wait(timeout=2), -signal.SIGKILL)
                guardian.stdin.close()
                recovered = quiesce_legacy_guardian_attempt(
                    launch["attempt"], launch["workspace"], timeout_seconds=5,
                    terminate_owned=True, expected_token_identity=launch["identity"],
                )
                self.assertEqual(recovered["state"], "dead")
                self.assertTrue(recovered["termination_requested"])
                self.assertEqual(
                    recovered["termination_proof"]["kind"],
                    "orphaned_ocean_group_recovered",
                )
                self.assertEqual(
                    recovered["termination_proof"]["action"],
                    "killed_exact_ocean_group",
                )
                wait_for(lambda: not process_is_running(ocean_pid))
                wait_for(lambda: not process_is_running(child_pid))
                self.assertIsNone(unrelated.poll())
                persisted = read_state(launch["state"])
                self.assertEqual(persisted["termination_reason"], "guardian_loss_recovery")
                self.assertTrue(persisted["orphan_scope_cleanup_complete"])
                self.assertFalse(persisted["descendant_supervision_complete"])
                self.assertEqual(
                    inspect_legacy_guardian_attempt(
                        launch["attempt"], launch["workspace"], launch["identity"],
                    )["state"],
                    "dead",
                )
                legacy_proof = json.loads(json.dumps(persisted))
                legacy_proof["orphan_recovery"]["schema_version"] = (
                    "crescendo.guardian_orphan_recovery.v1"
                )
                legacy_proof["orphan_recovery"].pop("launch_argv")
                legacy_proof["orphan_recovery"].pop("runtime_identity")
                with open(launch["state"], "w") as stream:
                    json.dump(legacy_proof, stream)
                self.assertEqual(
                    inspect_legacy_guardian_attempt(
                        launch["attempt"], launch["workspace"], launch["identity"],
                    )["state"],
                    "dead",
                )
            finally:
                if guardian.poll() is None:
                    guardian.kill()
                    guardian.wait()
                for pid in (ocean_pid, child_pid):
                    if pid and process_is_running(pid):
                        try:
                            os.kill(pid, signal.SIGKILL)
                        except OSError:
                            pass
                if unrelated.poll() is None:
                    os.killpg(unrelated.pid, signal.SIGKILL)
                    unrelated.wait()

    def test_guardian_sigkill_recovers_scoped_ocean_exec_continuation(self):
        with tempfile.TemporaryDirectory() as root:
            launcher = os.path.join(root, "ocean_launcher.py")
            runtime = os.path.join(root, "virtuoso_runtime.py")
            pid_path = os.path.join(root, "runtime.pid")
            write_script(launcher, """
import os
import sys
os.execv(sys.executable, [sys.executable, sys.argv[1], sys.argv[2]])
""")
            write_script(runtime, """
import os
import signal
import sys
import time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
with open(sys.argv[1], 'w') as stream:
    stream.write(str(os.getpid()))
    stream.flush()
    os.fsync(stream.fileno())
while True:
    time.sleep(1)
""")
            command = [sys.executable, launcher, runtime, pid_path]
            launch = self._start_scoped_guardian(root, command)
            guardian = launch["process"]
            unrelated = subprocess.Popen(
                [sys.executable, "-c", "import time; time.sleep(60)"],
                cwd=launch["workspace"], start_new_session=True,
            )
            ocean_pid = None
            try:
                wait_for(lambda: os.path.isfile(pid_path) and os.path.getsize(pid_path) > 0)
                with open(pid_path) as stream:
                    ocean_pid = int(stream.read())
                running = read_state(launch["state"])
                self.assertEqual(ocean_pid, running["pid"])
                self.assertEqual(running["argv"], command)
                with open("/proc/%d/cmdline" % ocean_pid, "rb") as stream:
                    runtime_argv = [
                        value.decode("utf-8")
                        for value in stream.read().split(b"\0") if value
                    ]
                self.assertNotEqual(runtime_argv, command)
                self.assertIn(runtime, runtime_argv)

                os.kill(guardian.pid, signal.SIGKILL)
                self.assertEqual(guardian.wait(timeout=2), -signal.SIGKILL)
                guardian.stdin.close()
                recovered = quiesce_legacy_guardian_attempt(
                    launch["attempt"], launch["workspace"], timeout_seconds=5,
                    terminate_owned=True, expected_token_identity=launch["identity"],
                )
                self.assertEqual(recovered["state"], "dead")
                self.assertEqual(
                    recovered["termination_proof"]["kind"],
                    "orphaned_ocean_group_recovered",
                )
                wait_for(lambda: not process_is_running(ocean_pid))
                self.assertIsNone(unrelated.poll())

                persisted = read_state(launch["state"])
                recovery = persisted["orphan_recovery"]
                self.assertEqual(
                    recovery["schema_version"],
                    "crescendo.guardian_orphan_recovery.v2",
                )
                self.assertEqual(recovery["launch_argv"], command)
                self.assertEqual(recovery["argv"], command)
                self.assertEqual(recovery["runtime_identity"]["pid"], ocean_pid)
                self.assertEqual(recovery["runtime_identity"]["start_ticks"], running["pid_start_ticks"])
                self.assertEqual(recovery["runtime_identity"]["argv"], runtime_argv)
                self.assertEqual(
                    inspect_legacy_guardian_attempt(
                        launch["attempt"], launch["workspace"], launch["identity"],
                    )["state"],
                    "dead",
                )

                for name, value in (
                        ("start_ticks", running["pid_start_ticks"] + 1),
                        ("pgrp", running["pgid"] + 1),
                        ("cwd", root),
                        ("launch_token", "crescendo2-" + "0" * 64),
                        ("argv", ["/bin/false"])):
                    tampered = json.loads(json.dumps(persisted))
                    tampered["orphan_recovery"]["runtime_identity"][name] = value
                    with open(launch["state"], "w") as stream:
                        json.dump(tampered, stream)
                    refused = inspect_legacy_guardian_attempt(
                        launch["attempt"], launch["workspace"], launch["identity"],
                    )
                    self.assertEqual(refused["state"], "ambiguous")
                    self.assertEqual(
                        refused["termination_proof"]["kind"],
                        "orphaned_ocean_recovery_unproven",
                    )
                with open(launch["state"], "w") as stream:
                    json.dump(persisted, stream)
            finally:
                if guardian.poll() is None:
                    guardian.kill()
                    guardian.wait()
                if ocean_pid and process_is_running(ocean_pid):
                    try:
                        os.killpg(ocean_pid, signal.SIGKILL)
                    except OSError:
                        pass
                if unrelated.poll() is None:
                    os.killpg(unrelated.pid, signal.SIGKILL)
                    unrelated.wait()

    def test_guardian_sigkill_recovers_exact_detached_scoped_helper_by_pid(self):
        with tempfile.TemporaryDirectory() as root:
            fake = os.path.join(root, "detached_ocean.py")
            helper = os.path.join(root, "detached_helper.py")
            pids_path = os.path.join(root, "pids.txt")
            ocean_path = os.path.join(root, "ocean.pid")
            helper_cwd = os.path.join(root, "workspace", "cadence-helper")
            os.makedirs(helper_cwd)
            write_script(helper, """
import os
import signal
import subprocess
import sys
import time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
environment = dict(os.environ)
environment.pop('CRESCENDO_LEGACY_GUARDIAN_TOKEN', None)
companion = subprocess.Popen([
    sys.executable, '-c',
    'import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(60)',
], env=environment)
with open(sys.argv[1], 'w') as stream:
    stream.write('%d %d\\n' % (os.getpid(), companion.pid))
    stream.flush()
    os.fsync(stream.fileno())
while True:
    time.sleep(1)
""")
            write_script(fake, """
import os
import signal
import subprocess
import sys
import time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
child = subprocess.Popen([
    sys.executable, sys.argv[1], sys.argv[2],
], cwd=sys.argv[3], start_new_session=True)
with open(sys.argv[4], 'w') as stream:
    stream.write(str(os.getpid()))
    stream.flush()
    os.fsync(stream.fileno())
while True:
    time.sleep(1)
""")
            launch = self._start_scoped_guardian(
                root, [
                    sys.executable, fake, helper, pids_path, helper_cwd, ocean_path,
                ],
            )
            guardian = launch["process"]
            ocean_pid = None
            helper_pid = None
            companion_pid = None
            try:
                wait_for(
                    lambda: os.path.isfile(pids_path) and os.path.getsize(pids_path) > 0
                    and os.path.isfile(ocean_path) and os.path.getsize(ocean_path) > 0,
                )
                with open(pids_path) as stream:
                    helper_pid, companion_pid = [int(value) for value in stream.read().split()]
                with open(ocean_path) as stream:
                    ocean_pid = int(stream.read())
                os.kill(guardian.pid, signal.SIGKILL)
                self.assertEqual(guardian.wait(timeout=2), -signal.SIGKILL)
                guardian.stdin.close()
                recovered = quiesce_legacy_guardian_attempt(
                    launch["attempt"], launch["workspace"], timeout_seconds=5,
                    terminate_owned=True, expected_token_identity=launch["identity"],
                )
                self.assertEqual(recovered["state"], "dead")
                self.assertEqual(
                    recovered["termination_proof"]["action"],
                    "killed_exact_scoped_processes",
                )
                wait_for(lambda: not process_is_running(ocean_pid))
                wait_for(lambda: not process_is_running(helper_pid))
                self.assertTrue(process_is_running(companion_pid))
                persisted = read_state(launch["state"])
                recovery = persisted["orphan_recovery"]
                self.assertEqual(len(recovery["detached_helpers"]), 1)
                detached = recovery["detached_helpers"][0]
                self.assertEqual(detached["pid"], helper_pid)
                self.assertEqual(detached["cwd"], helper_cwd)
                self.assertEqual(
                    recovery["detached_helper_signals"][0]["identity"], detached,
                )
                self.assertEqual(
                    inspect_legacy_guardian_attempt(
                        launch["attempt"], launch["workspace"], launch["identity"],
                    )["state"],
                    "dead",
                )
                for name, value in (
                        ("start_ticks", detached["start_ticks"] + 1),
                        ("ppid", detached["ppid"] + 1),
                        ("pgrp", detached["pgrp"] + 1),
                        ("session", detached["session"] + 1),
                        ("uid", detached["uid"] + 1),
                        ("cwd", root),
                        ("argv", ["/bin/false"]),
                        ("launch_token", "crescendo2-" + "0" * 64)):
                    tampered = json.loads(json.dumps(persisted))
                    tampered["orphan_recovery"]["detached_helpers"][0][name] = value
                    with open(launch["state"], "w") as stream:
                        json.dump(tampered, stream)
                    refused = inspect_legacy_guardian_attempt(
                        launch["attempt"], launch["workspace"], launch["identity"],
                    )
                    self.assertEqual(refused["state"], "ambiguous")
                    self.assertEqual(
                        refused["termination_proof"]["kind"],
                        "orphaned_ocean_recovery_unproven",
                    )
                with open(launch["state"], "w") as stream:
                    json.dump(persisted, stream)
            finally:
                if guardian.poll() is None:
                    guardian.kill()
                    guardian.wait()
                if ocean_pid and process_is_running(ocean_pid):
                    try:
                        os.killpg(ocean_pid, signal.SIGKILL)
                    except OSError:
                        pass
                if helper_pid and process_is_running(helper_pid):
                    try:
                        os.killpg(helper_pid, signal.SIGKILL)
                    except OSError:
                        pass
                elif companion_pid and process_is_running(companion_pid):
                    try:
                        os.killpg(helper_pid, signal.SIGKILL)
                    except OSError:
                        pass

    def test_guardian_sigkill_rejects_detached_scoped_helper_outside_workspace(self):
        with tempfile.TemporaryDirectory() as root:
            fake = os.path.join(root, "detached_ocean.py")
            pids_path = os.path.join(root, "pids.txt")
            conflict_cwd = os.path.join(root, "outside-workspace")
            os.makedirs(conflict_cwd)
            write_script(fake, """
import os
import signal
import subprocess
import sys
import time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
child = subprocess.Popen([
    sys.executable, '-c',
    'import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(60)',
], cwd=os.environ['LEGACY_GUARDIAN_CONFLICT_CWD'], start_new_session=True)
with open(os.environ['LEGACY_GUARDIAN_PIDS'], 'w') as stream:
    stream.write('%d %d\\n' % (os.getpid(), child.pid))
    stream.flush()
    os.fsync(stream.fileno())
while True:
    time.sleep(1)
""")
            environment = dict(os.environ)
            environment["LEGACY_GUARDIAN_PIDS"] = pids_path
            environment["LEGACY_GUARDIAN_CONFLICT_CWD"] = conflict_cwd
            launch = self._start_scoped_guardian(
                root, [sys.executable, fake], environment=environment,
            )
            guardian = launch["process"]
            ocean_pid = None
            child_pid = None
            try:
                wait_for(lambda: os.path.isfile(pids_path) and os.path.getsize(pids_path) > 0)
                with open(pids_path) as stream:
                    ocean_pid, child_pid = [int(value) for value in stream.read().split()]
                os.kill(guardian.pid, signal.SIGKILL)
                self.assertEqual(guardian.wait(timeout=2), -signal.SIGKILL)
                guardian.stdin.close()
                refused = quiesce_legacy_guardian_attempt(
                    launch["attempt"], launch["workspace"], timeout_seconds=2,
                    terminate_owned=True, expected_token_identity=launch["identity"],
                )
                self.assertEqual(refused["state"], "ambiguous")
                self.assertEqual(
                    refused["termination_proof"]["kind"],
                    "orphan_recovery_process_conflict",
                )
                diagnostic = refused["processes"][0]
                self.assertEqual(
                    set((
                        "kind", "pid", "start_ticks", "ppid", "pgrp", "session",
                        "uid", "cwd", "argv", "launch_token",
                    )).difference(diagnostic),
                    set(),
                )
                self.assertEqual(diagnostic["pid"], child_pid)
                self.assertEqual(diagnostic["cwd"], conflict_cwd)
                self.assertTrue(process_is_running(ocean_pid))
                self.assertTrue(process_is_running(child_pid))
                self.assertNotIn("orphan_recovery", read_state(launch["state"]))
            finally:
                if guardian.poll() is None:
                    guardian.kill()
                    guardian.wait()
                if ocean_pid and process_is_running(ocean_pid):
                    try:
                        os.killpg(ocean_pid, signal.SIGKILL)
                    except OSError:
                        pass
                if child_pid and process_is_running(child_pid):
                    try:
                        os.killpg(child_pid, signal.SIGKILL)
                    except OSError:
                        pass

    def test_exact_active_guardian_can_be_terminated_and_corrupt_state_fails_closed(self):
        with tempfile.TemporaryDirectory() as root:
            fake = os.path.join(root, "fake_ocean.py")
            write_script(fake, "import time\nwhile True: time.sleep(1)\n")
            command = [sys.executable, fake]
            workspace, attempt, state, stdout, stderr = make_attempt(root, command)
            result = {}

            def target():
                result.update(run_command(
                    command, workspace, os.environ.copy(), 60,
                    stdout_path=stdout, stderr_path=stderr,
                    guardian_state_path=state, guardian_token=ATTEMPT_ID,
                    termination_grace_seconds=1.5, termination_kill_seconds=1,
                ))

            thread = threading.Thread(target=target)
            thread.start()
            wait_for(
                lambda: read_state(state)
                if read_state(state) and read_state(state).get("status") == "running" else None,
            )
            active = inspect_legacy_guardian_attempt(attempt, workspace)
            self.assertEqual(active["state"], "active")
            quiesced = quiesce_legacy_guardian_attempt(
                attempt, workspace, timeout_seconds=5, terminate_owned=True,
            )
            self.assertEqual(quiesced["state"], "dead")
            self.assertTrue(quiesced["termination_requested"])
            thread.join(timeout=5)
            self.assertFalse(thread.is_alive())
            self.assertFalse(result["termination_failed"])

            with open(state, "w") as stream:
                stream.write("{not-json\n")
            ambiguous = inspect_legacy_guardian_attempt(attempt, workspace)
            self.assertEqual(ambiguous["state"], "ambiguous")
            self.assertFalse(ambiguous["termination_proof"]["ok"])

    def test_exact_sigstopped_guardian_is_continued_then_quiesced(self):
        with tempfile.TemporaryDirectory() as root:
            fake = os.path.join(root, "fake_ocean.py")
            write_script(fake, "import time\nwhile True: time.sleep(1)\n")
            launch = self._start_scoped_guardian(
                root, [sys.executable, fake],
            )
            guardian = launch["process"]
            ocean_pid = read_state(launch["state"])["pid"]
            try:
                os.kill(guardian.pid, signal.SIGSTOP)
                quiesced = quiesce_legacy_guardian_attempt(
                    launch["attempt"], launch["workspace"], timeout_seconds=5,
                    terminate_owned=True,
                    expected_token_identity=launch["identity"],
                )
                self.assertEqual(quiesced["state"], "dead")
                self.assertTrue(quiesced["termination_requested"])
                self.assertTrue(quiesced["termination_proof"]["ok"])
                guardian.stdin.close()
                guardian.wait(timeout=2)
                wait_for(lambda: not process_is_running(ocean_pid))
            finally:
                if guardian.stdin and not guardian.stdin.closed:
                    guardian.stdin.close()
                if guardian.poll() is None:
                    try:
                        os.kill(guardian.pid, signal.SIGCONT)
                    except OSError:
                        pass
                    guardian.kill()
                    guardian.wait()
                if process_is_running(ocean_pid):
                    try:
                        os.killpg(ocean_pid, signal.SIGKILL)
                    except OSError:
                        pass


if __name__ == "__main__":
    unittest.main()
