import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest


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
        os.kill(pid, 0)
    except OSError:
        return False
    try:
        with open("/proc/%d/stat" % pid) as stream:
            state = stream.read().split()[2]
        return state != "Z"
    except (IOError, OSError, IndexError):
        return True


WORKER_ID = "worker-token"
SESSION_ID = "session-identity"


class OceanWorkerGuardianTest(unittest.TestCase):
    def start_guardian(self, root, fake_ocean, token=WORKER_ID, environment=None, grace=0.15,
                       poll_expected_parent=False):
        state_path = os.path.join(root, "state.json")
        stdout_path = os.path.join(root, "ocean.stdout")
        stderr_path = os.path.join(root, "ocean.stderr")
        command = [
            sys.executable, GUARDIAN,
            "--state-path", state_path,
            "--stdout-path", stdout_path,
            "--stderr-path", stderr_path,
            "--token", token,
            "--cwd", root,
            "--terminate-grace-seconds", str(grace),
            "--poll-interval-seconds", "0.01",
        ]
        if poll_expected_parent:
            command.extend((
                "--expected-parent-pid", str(os.getpid()),
                "--poll-expected-parent",
            ))
        command.extend(("--", sys.executable, fake_ocean))
        process = subprocess.Popen(
            command, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            env=environment,
        )
        return process, state_path, stdout_path, stderr_path

    def test_polled_parent_mode_survives_short_lived_launcher_thread(self):
        with tempfile.TemporaryDirectory() as root:
            fake_ocean = os.path.join(root, "fake_ocean.py")
            write_script(fake_ocean, """
import sys
for line in sys.stdin:
    if line.strip() == 'QUIT':
        sys.exit(0)
""")
            launched = []

            def launch():
                launched.append(self.start_guardian(
                    root, fake_ocean, poll_expected_parent=True,
                ))

            thread = threading.Thread(target=launch)
            thread.start()
            thread.join()
            guardian, state_path, _stdout, _stderr = launched[0]
            wait_for(
                lambda: read_state(state_path)
                if read_state(state_path) and read_state(state_path).get("status") == "running"
                else None,
            )
            self.assertIsNone(guardian.poll())
            guardian.stdin.write(b"QUIT\n")
            guardian.stdin.flush()
            self.assertEqual(guardian.wait(timeout=5), 0)
            guardian.stdin.close()

    def test_forwards_complete_lines_and_preserves_normal_ocean_exit_code(self):
        with tempfile.TemporaryDirectory() as root:
            fake_ocean = os.path.join(root, "fake_ocean.py")
            capture = os.path.join(root, "commands.txt")
            write_script(fake_ocean, """
from __future__ import print_function
import os
import sys
for line in sys.stdin:
    with open(os.environ['GUARDIAN_CAPTURE'], 'a') as stream:
        stream.write(line)
    print('OUT:' + line.rstrip('\\n'))
    print('ERR:' + os.environ['GUARDIAN_MARK'], file=sys.stderr)
    print('CHILD_LD:' + os.environ.get('LD_LIBRARY_PATH', 'missing'))
    sys.stdout.flush()
    sys.stderr.flush()
    if line.strip() == 'EXIT7':
        sys.exit(7)
""")
            environment = dict(os.environ)
            environment.pop("LD_LIBRARY_PATH", None)
            environment.update({"GUARDIAN_CAPTURE": capture, "GUARDIAN_MARK": "inherited"})
            environment["CRESCENDO_GUARDIAN_CHILD_LD_LIBRARY_PATH"] = "/eda/child-only-libraries"
            guardian, state_path, stdout_path, stderr_path = self.start_guardian(
                root, fake_ocean, environment=environment,
            )
            guardian.stdin.write(b"first command\nEXIT7\n")
            guardian.stdin.flush()
            self.assertEqual(guardian.wait(timeout=5), 7)
            guardian.stdin.close()
            with open(capture) as stream:
                self.assertEqual(stream.read(), "first command\nEXIT7\n")
            with open(stdout_path) as stream:
                stdout = stream.read()
                self.assertIn("OUT:first command", stdout)
                self.assertIn("CHILD_LD:/eda/child-only-libraries", stdout)
            with open(stderr_path) as stream:
                self.assertIn("ERR:inherited", stream.read())
            state = load_json(state_path)
            self.assertEqual(state["status"], "exited")
            self.assertEqual(state["returncode"], 7)
            self.assertEqual(state["termination_reason"], "ocean_exit")
            self.assertFalse(state["term_sent"])
            self.assertFalse(state["kill_sent"])

    def test_state_identifies_guardian_ocean_group_token_and_cwd(self):
        with tempfile.TemporaryDirectory() as root:
            fake_ocean = os.path.join(root, "fake_ocean.py")
            write_script(fake_ocean, """
import sys
for line in sys.stdin:
    if line.strip() == 'QUIT':
        sys.exit(0)
""")
            guardian, state_path, _stdout, _stderr = self.start_guardian(
                root, fake_ocean, token=SESSION_ID,
            )
            state = wait_for(
                lambda: read_state(state_path)
                if read_state(state_path) and read_state(state_path).get("status") == "running" else None,
            )
            self.assertEqual(state["schema_version"], "crescendo.ocean_worker_guardian.v1")
            self.assertEqual(state["token"], SESSION_ID)
            with open("/proc/sys/kernel/random/boot_id", "r") as stream:
                boot_id = stream.read().strip()
            self.assertEqual(state["boot_id"], boot_id)
            self.assertEqual(state["launch_boot_id"], boot_id)
            self.assertEqual(state["guardian_pid"], guardian.pid)
            self.assertIsInstance(state["guardian_start_ticks"], int)
            self.assertTrue(state["child_subreaper"])
            self.assertEqual(state["pid"], state["pgid"])
            self.assertEqual(state["pid"], state["sid"])
            self.assertIsInstance(state["pid_start_ticks"], int)
            self.assertEqual(os.getpgid(state["pid"]), state["pgid"])
            self.assertEqual(state["cwd"], os.path.realpath(root))
            guardian.stdin.write(b"QUIT\n")
            guardian.stdin.flush()
            self.assertEqual(guardian.wait(timeout=5), 0)
            guardian.stdin.close()
            final_state = load_json(state_path)
            self.assertEqual(final_state["returncode"], 0)
            self.assertEqual(final_state["termination_reason"], "ocean_exit")
            self.assertTrue(final_state["group_cleanup_complete"])
            self.assertEqual(final_state["remaining_group_pids"], [])

    def test_missing_expected_parent_prevents_late_ocean_spawn(self):
        with tempfile.TemporaryDirectory() as root:
            fake_ocean = os.path.join(root, "fake_ocean.py")
            spawned = os.path.join(root, "spawned.txt")
            state_path = os.path.join(root, "state.json")
            write_script(fake_ocean, "open(%r, 'w').write('spawned')\n" % spawned)
            command = [
                sys.executable, GUARDIAN,
                "--state-path", state_path,
                "--stdout-path", os.path.join(root, "ocean.stdout"),
                "--stderr-path", os.path.join(root, "ocean.stderr"),
                "--token", "missing-parent",
                "--cwd", root,
                "--expected-parent-pid", "99999999",
                "--", sys.executable, fake_ocean,
            ]
            process = subprocess.Popen(
                command, stdin=subprocess.PIPE,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            self.assertEqual(process.wait(timeout=5), 143)
            process.stdin.close()
            state = load_json(state_path)
            self.assertEqual(state["status"], "exited")
            self.assertEqual(state["termination_reason"], "expected_parent_missing_before_ocean_spawn")
            self.assertIsNone(state["pid"])
            self.assertTrue(state["group_cleanup_complete"])
            self.assertFalse(os.path.exists(spawned))

    def test_stdin_eof_terminates_only_owned_ocean_group_and_descendant(self):
        with tempfile.TemporaryDirectory() as root:
            fake_ocean = os.path.join(root, "stubborn_ocean.py")
            pids_path = os.path.join(root, "pids.txt")
            write_script(fake_ocean, """
from __future__ import print_function
import os
import signal
import subprocess
import sys
import time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
descendant = subprocess.Popen([
    sys.executable, '-c',
    'import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(60)',
])
with open(os.environ['GUARDIAN_PIDS'], 'w') as stream:
    stream.write('%d %d\\n' % (os.getpid(), descendant.pid))
    stream.flush()
    os.fsync(stream.fileno())
while True:
    time.sleep(1)
""")
            environment = dict(os.environ)
            environment["GUARDIAN_PIDS"] = pids_path
            unrelated = subprocess.Popen(
                [sys.executable, "-c", "import time; time.sleep(60)"],
                start_new_session=True,
            )
            guardian = None
            try:
                guardian, state_path, _stdout, _stderr = self.start_guardian(
                    root, fake_ocean, environment=environment, grace=0.08,
                )
                wait_for(lambda: os.path.isfile(pids_path) and os.path.getsize(pids_path) > 0)
                with open(pids_path) as stream:
                    ocean_pid, descendant_pid = [int(value) for value in stream.read().split()]
                guardian.stdin.close()
                guardian.stdin = None
                self.assertEqual(guardian.wait(timeout=5), 137)
                wait_for(lambda: not process_is_running(ocean_pid))
                wait_for(lambda: not process_is_running(descendant_pid))
                self.assertIsNone(unrelated.poll())
                state = load_json(state_path)
                self.assertEqual(state["pid"], ocean_pid)
                self.assertEqual(state["pgid"], ocean_pid)
                self.assertEqual(state["status"], "exited")
                self.assertEqual(state["termination_reason"], "stdin_eof")
                self.assertTrue(state["term_sent"])
                self.assertTrue(state["kill_sent"])
                self.assertEqual(state["returncode"], -signal.SIGKILL)
                self.assertTrue(state["group_cleanup_complete"])
                self.assertEqual(state["remaining_group_pids"], [])
            finally:
                if guardian is not None and guardian.poll() is None:
                    guardian.kill()
                    guardian.wait()
                if unrelated.poll() is None:
                    os.killpg(unrelated.pid, signal.SIGTERM)
                    try:
                        unrelated.wait(timeout=1)
                    except subprocess.TimeoutExpired:
                        os.killpg(unrelated.pid, signal.SIGKILL)
                        unrelated.wait()

    def test_ocean_leader_exit_cleans_stubborn_remaining_group_descendant(self):
        with tempfile.TemporaryDirectory() as root:
            fake_ocean = os.path.join(root, "exiting_ocean.py")
            descendant_path = os.path.join(root, "descendant.pid")
            write_script(fake_ocean, """
from __future__ import print_function
import os
import signal
import subprocess
import sys
import time
ready_path = os.environ['GUARDIAN_DESCENDANT_READY']
descendant = subprocess.Popen([
    sys.executable, '-c',
    'import signal,sys,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); '
    'open(sys.argv[1], "w").write("ready"); time.sleep(60)',
    ready_path,
])
while not os.path.isfile(ready_path) or os.path.getsize(ready_path) == 0:
    time.sleep(0.005)
with open(os.environ['GUARDIAN_DESCENDANT'], 'w') as stream:
    stream.write('%d\\n' % descendant.pid)
    stream.flush()
    os.fsync(stream.fileno())
sys.exit(0)
""")
            environment = dict(os.environ)
            environment["GUARDIAN_DESCENDANT"] = descendant_path
            environment["GUARDIAN_DESCENDANT_READY"] = descendant_path + ".ready"
            guardian, state_path, _stdout, _stderr = self.start_guardian(
                root, fake_ocean, environment=environment, grace=0.08,
            )
            wait_for(lambda: os.path.isfile(descendant_path) and os.path.getsize(descendant_path) > 0)
            with open(descendant_path) as stream:
                descendant_pid = int(stream.read().strip())
            self.assertEqual(guardian.wait(timeout=5), 0)
            guardian.stdin.close()
            wait_for(lambda: not process_is_running(descendant_pid))
            state = load_json(state_path)
            self.assertEqual(state["termination_reason"], "ocean_exit")
            self.assertEqual(state["returncode"], 0)
            self.assertTrue(state["term_sent"])
            self.assertTrue(state["kill_sent"])
            self.assertTrue(state["group_cleanup_complete"])
            self.assertEqual(state["remaining_group_pids"], [])

    def test_ocean_leader_exit_reaps_descendant_that_detached_its_session(self):
        with tempfile.TemporaryDirectory() as root:
            fake_ocean = os.path.join(root, "exiting_ocean.py")
            descendant_path = os.path.join(root, "detached.pid")
            write_script(fake_ocean, """
from __future__ import print_function
import os
import signal
import subprocess
import sys
import time
ready_path = os.environ['GUARDIAN_DESCENDANT_READY']
descendant = subprocess.Popen([
    sys.executable, '-c',
    'import signal,sys,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); '
    'open(sys.argv[1], "w").write("ready"); time.sleep(60)',
    ready_path,
], start_new_session=True)
while not os.path.isfile(ready_path) or os.path.getsize(ready_path) == 0:
    time.sleep(0.005)
with open(os.environ['GUARDIAN_DESCENDANT'], 'w') as stream:
    stream.write('%d\\n' % descendant.pid)
    stream.flush()
    os.fsync(stream.fileno())
sys.exit(0)
""")
            environment = dict(os.environ)
            environment["GUARDIAN_DESCENDANT"] = descendant_path
            environment["GUARDIAN_DESCENDANT_READY"] = descendant_path + ".ready"
            guardian, state_path, _stdout, _stderr = self.start_guardian(
                root, fake_ocean, environment=environment, grace=0.08,
            )
            wait_for(lambda: os.path.isfile(descendant_path) and os.path.getsize(descendant_path) > 0)
            with open(descendant_path) as stream:
                descendant_pid = int(stream.read().strip())
            self.assertEqual(guardian.wait(timeout=5), 0)
            guardian.stdin.close()
            wait_for(lambda: not process_is_running(descendant_pid))
            state = load_json(state_path)
            self.assertEqual(state["termination_reason"], "ocean_exit")
            self.assertTrue(state["adopted_term_sent"])
            self.assertTrue(state["adopted_kill_sent"])
            self.assertTrue(state["descendant_cleanup_complete"])
            self.assertTrue(state["descendant_supervision_complete"])
            self.assertEqual(state["remaining_adopted_pids"], [])
            self.assertTrue(state["group_cleanup_complete"])

    def test_controller_sigkill_closes_pipe_and_guardian_reaps_owned_ocean_group(self):
        with tempfile.TemporaryDirectory() as root:
            fake_ocean = os.path.join(root, "stubborn_ocean.py")
            controller_script = os.path.join(root, "controller.py")
            guardian_pid_path = os.path.join(root, "guardian.pid")
            pids_path = os.path.join(root, "ocean.pids")
            state_path = os.path.join(root, "state.json")
            stdout_path = os.path.join(root, "ocean.stdout")
            stderr_path = os.path.join(root, "ocean.stderr")
            write_script(fake_ocean, """
from __future__ import print_function
import os
import signal
import subprocess
import sys
import time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
descendant = subprocess.Popen([
    sys.executable, '-c',
    'import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(60)',
])
with open(os.environ['GUARDIAN_PIDS'], 'w') as stream:
    stream.write('%d %d\\n' % (os.getpid(), descendant.pid))
    stream.flush()
    os.fsync(stream.fileno())
while True:
    time.sleep(1)
""")
            write_script(controller_script, """
from __future__ import print_function
import os
import subprocess
import sys
import time
guardian = subprocess.Popen(
    sys.argv[2:], stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL, start_new_session=True,
)
# Publish the PID only after the complete record is flushed and closed.
# The consumer waits for this path, so existence must mean ready to parse.
guardian_pid_path = sys.argv[1]
with open(guardian_pid_path + '.tmp', 'w') as stream:
    stream.write('%d\\n' % guardian.pid)
    stream.flush()
    os.fsync(stream.fileno())
os.replace(guardian_pid_path + '.tmp', guardian_pid_path)
while True:
    time.sleep(1)
""")
            guardian_command = [
                sys.executable, GUARDIAN,
                "--state-path", state_path,
                "--stdout-path", stdout_path,
                "--stderr-path", stderr_path,
                "--token", "parent-crash-token",
                "--cwd", root,
                "--terminate-grace-seconds", "0.08",
                "--poll-interval-seconds", "0.01",
                "--", sys.executable, fake_ocean,
            ]
            environment = dict(os.environ)
            environment["GUARDIAN_PIDS"] = pids_path
            controller = subprocess.Popen(
                [sys.executable, controller_script, guardian_pid_path] + guardian_command,
                env=environment,
            )
            guardian_pid = None
            ocean_pid = None
            descendant_pid = None
            try:
                wait_for(lambda: os.path.isfile(guardian_pid_path))
                with open(guardian_pid_path) as stream:
                    guardian_pid = int(stream.read().strip())
                wait_for(
                    lambda: read_state(state_path)
                    if read_state(state_path) and read_state(state_path).get("status") == "running" else None,
                )
                wait_for(lambda: os.path.isfile(pids_path) and os.path.getsize(pids_path) > 0)
                with open(pids_path) as stream:
                    ocean_pid, descendant_pid = [int(value) for value in stream.read().split()]
                os.kill(controller.pid, signal.SIGKILL)
                self.assertEqual(controller.wait(timeout=5), -signal.SIGKILL)
                wait_for(lambda: not process_is_running(guardian_pid))
                wait_for(lambda: not process_is_running(ocean_pid))
                wait_for(lambda: not process_is_running(descendant_pid))
                state = load_json(state_path)
                self.assertEqual(state["guardian_pid"], guardian_pid)
                self.assertEqual(state["termination_reason"], "stdin_eof")
                self.assertTrue(state["term_sent"])
                self.assertTrue(state["kill_sent"])
                self.assertTrue(state["group_cleanup_complete"])
            finally:
                if controller.poll() is None:
                    controller.kill()
                    controller.wait()
                if guardian_pid and process_is_running(guardian_pid):
                    os.killpg(guardian_pid, signal.SIGKILL)
                if ocean_pid and process_is_running(ocean_pid):
                    os.killpg(ocean_pid, signal.SIGKILL)


if __name__ == "__main__":
    unittest.main()
