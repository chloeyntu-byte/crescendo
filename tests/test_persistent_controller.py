import errno
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest

try:
    import fcntl
except ImportError:
    fcntl = None

from crescendo_engine.legacy_guardian import (
    MANIFEST_SCHEMA_VERSION as GUARDIAN_LAUNCH_PROTOCOL,
    acquire_legacy_guardian_launch_lock,
    close_legacy_guardian_launch_lock,
    legacy_guardian_parent_identity,
    write_legacy_guardian_manifest,
)
from crescendo_engine.environment_identity import collect_external_eda_environment
from crescendo_engine.persistent_maestro import persistent_worker_control_root
from crescendo_engine.persistent_controller import (
    LIVENESS_LOCK_SCHEMA, PersistentControllerError, PersistentControllerProcessError,
    PersistentControllerProxy, PersistentControllerRemoteError,
    _active_worker_session_identity, _guardian_proof, _worker_epoch_proof,
    _controller_worker_config, _worker_epoch_owned_by_controller, _worker_epoch_roots,
)
from crescendo_engine.legacy_guardian import GUARDIAN_SCOPE_CONFIG_KEY
FAKE_CONTROLLER = r'''
import json
import os
import signal
import sys
import tempfile
import time


def atomic(path, value):
    directory = os.path.dirname(path)
    handle, temporary = tempfile.mkstemp(dir=directory, prefix=os.path.basename(path), suffix=".tmp")
    with os.fdopen(handle, "w") as stream:
        json.dump(value, stream)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


with open(sys.argv[1], "r") as stream:
    bootstrap = json.load(stream)
atomic(bootstrap["ready_path"], {
    "schema_version": bootstrap["schema_version"],
    "token": bootstrap["token"],
    "pid": os.getpid(),
})
sequence = 1
while True:
    prefix = "%08d_" % sequence
    names = sorted(name for name in os.listdir(bootstrap["requests_root"]) if name.endswith(".json") and name.startswith(prefix))
    if not names:
        time.sleep(0.01)
        continue
    request_path = os.path.join(bootstrap["requests_root"], names[0])
    with open(request_path, "r") as stream:
        request = json.load(stream)
    payload = request.get("payload") or {}
    if request["operation"] == "EVALUATE" and payload.get("values", {}).get("mode") == "hang":
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        while True:
            time.sleep(1)
    if request["operation"] == "COMMIT" and payload.get("candidate_id") == "hang":
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        while True:
            time.sleep(1)
    response = dict((key, request[key]) for key in (
        "schema_version", "token", "controller_epoch", "sequence", "nonce", "operation",
    ))
    if request["operation"] == "EVALUATE" and payload.get("values", {}).get("mode") == "external_environment":
        response.update({
            "ok": False,
            "error": "ExternalEdaEnvironmentChanged: model content changed",
            "error_type": "ExternalEdaEnvironmentChanged",
            "failure_class": "external_environment",
            "fatal_run": True,
            "traceback": "synthetic remote traceback",
        })
    elif request["operation"] == "EVALUATE" and payload.get("values", {}).get("mode") == "error":
        response.update({"ok": False, "error": "synthetic evaluation failure"})
    elif request["operation"] == "EVALUATE":
        response.update({"ok": True, "result": {
            "candidate_id": "g%03d_c%04d" % (payload["generation"], payload["index"]),
            "index": payload["index"], "generation": payload["generation"],
            "values": payload["values"], "controller_pid": os.getpid(),
        }})
    elif request["operation"] == "COMMIT":
        response.update({"ok": True, "result": {
            "ok": True, "state": "cleaned", "candidate_id": payload["candidate_id"],
            "controller_pid": os.getpid(),
        }})
    elif request["operation"] == "CLOSE":
        response.update({"ok": True, "result": {
            "reason": "closed", "cleanup": {"ok": True, "removed": True},
            "controller_pid": os.getpid(),
        }})
    else:
        response.update({"ok": False, "error": "unsupported"})
    atomic(request["response_path"], response)
    sequence += 1
    if request["operation"] == "CLOSE":
        break
'''


class PersistentControllerTest(unittest.TestCase):
    def test_controller_token_scopes_worker_config_without_mutating_bootstrap(self):
        bootstrap = {
            "token": "a" * 32,
            "config": {"runtime": {"candidate_timeout_seconds": 12}},
        }
        worker_config = _controller_worker_config(bootstrap)
        self.assertEqual(
            worker_config["runtime"][GUARDIAN_SCOPE_CONFIG_KEY], bootstrap["token"],
        )
        self.assertNotIn(GUARDIAN_SCOPE_CONFIG_KEY, bootstrap["config"]["runtime"])

    def test_worker_epoch_ownership_is_controller_namespaced(self):
        self.assertTrue(_worker_epoch_owned_by_controller("epoch_000", 0))
        self.assertFalse(_worker_epoch_owned_by_controller("epoch_000", 1))
        self.assertTrue(_worker_epoch_owned_by_controller(
            "epoch_c000001_l000000", 1,
        ))
        self.assertFalse(_worker_epoch_owned_by_controller(
            "epoch_c000002_l000000", 1,
        ))

    def test_active_worker_session_identity_requires_exact_owned_process_evidence(self):
        with tempfile.TemporaryDirectory() as root:
            epoch_root = os.path.join(root, "workers", "worker_000", "epoch_000")
            workspace = os.path.join(epoch_root, "workspace")
            control = persistent_worker_control_root(epoch_root, "worker-000-epoch-000")
            os.makedirs(workspace)
            os.makedirs(control)
            state_path = os.path.join(epoch_root, "worker_state.json")
            state = {
                "schema_version": "crescendo.persistent_maestro_worker.v1",
                "worker_index": 0, "epoch": "worker-000-epoch-000",
                "state": "ready", "tainted": False, "closed": False,
                "pid": 456, "guardian_start_ticks": 789,
                "workspace_root": workspace, "control_root": control,
            }
            with open(state_path, "w") as stream:
                json.dump(state, stream)
            health = {"processes": [{"pid": 456, "start_ticks": 789}]}

            proven = _active_worker_session_identity(root, 0, 0, set(), health)
            state["state"] = "busy"
            with open(state_path, "w") as stream:
                json.dump(state, stream)
            busy = _active_worker_session_identity(root, 0, 0, set(), health)
            state["tainted"] = True
            with open(state_path, "w") as stream:
                json.dump(state, stream)
            tainted = _active_worker_session_identity(root, 0, 0, set(), health)
            state["tainted"] = False
            state["epoch"] = "worker-000-epoch-999"
            with open(state_path, "w") as stream:
                json.dump(state, stream)
            mismatched = _active_worker_session_identity(root, 0, 0, set(), health)
            state["epoch"] = "worker-000-epoch-000"
            with open(state_path, "w") as stream:
                json.dump(state, stream)
            missing = _active_worker_session_identity(
                root, 0, 0, set(), {"processes": [{"pid": 456, "start_ticks": 790}]},
            )

            self.assertTrue(proven["proven"])
            self.assertEqual(proven["epoch"], "worker-000-epoch-000")
            self.assertEqual(proven["pid_start_ticks"], 789)
            self.assertTrue(busy["proven"])
            self.assertEqual(busy["state"], "busy")
            self.assertFalse(tainted["proven"])
            self.assertIn("not reusable", tainted["reason"])
            self.assertFalse(mismatched["proven"])
            self.assertIn("identity mismatch", mismatched["reason"])
            self.assertFalse(missing["proven"])
            self.assertIn("not in the owned controller tree", missing["reason"])

    def test_valid_non_object_guardian_documents_fail_without_attribute_error(self):
        with tempfile.TemporaryDirectory() as root:
            guardian_path = os.path.join(root, "guardian_state.json")
            with open(guardian_path, "w") as stream:
                json.dump([], stream)
            proof = _guardian_proof(guardian_path)
            self.assertFalse(proof["ok"])
            self.assertEqual(proof["kind"], "guardian_state_malformed")

            epoch_root = os.path.join(root, "epoch")
            os.makedirs(epoch_root)
            with open(os.path.join(epoch_root, "worker_state.json"), "w") as stream:
                json.dump([], stream)
            proof = _worker_epoch_proof(epoch_root)
            self.assertFalse(proof["ok"])
            self.assertEqual(proof["kind"], "worker_state_malformed")

    def _config(self, root):
        return {"runtime": {
            "run_root": root,
            "controller_startup_timeout_seconds": 3,
            "controller_commit_timeout_seconds": 3,
            "controller_close_timeout_seconds": 3,
            "controller_term_grace_seconds": 0.1,
            "controller_kill_grace_seconds": 1,
            "controller_worker_cleanup_timeout_seconds": 0.1,
        }}

    def _make_durable_worker_launch(self, root, with_worker_state=True):
        epoch_root = os.path.join(root, "workers", "worker_000", "epoch_000")
        workspace = os.path.join(epoch_root, "workspace")
        control = persistent_worker_control_root(epoch_root, "worker-000-epoch-000")
        os.makedirs(workspace)
        os.makedirs(control)
        state_path = os.path.join(control, "guardian_state.json")
        stdout_path = os.path.join(control, "ocean.stdout")
        stderr_path = os.path.join(control, "ocean.stderr")
        worker_id = "worker-000-epoch-000-worker-0"
        ocean_command = ["/opt/eda/bin/ocean", "-nograph"]
        guardian_script = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "tools", "ocean_worker_guardian.py",
        )
        descriptor, lock_path = acquire_legacy_guardian_launch_lock(control)
        parent = legacy_guardian_parent_identity()
        guardian_command = [
            sys.executable, guardian_script,
            "--state-path", state_path,
            "--stdout-path", stdout_path,
            "--stderr-path", stderr_path,
            "--token", worker_id,
            "--cwd", workspace,
            "--expected-parent-pid", str(parent["pid"]),
            "--launch-lock-fd", str(descriptor),
            "--",
        ] + ocean_command
        write_legacy_guardian_manifest(
            control, workspace, state_path, stdout_path, stderr_path, worker_id,
            ocean_command, parent["pid"], guardian_script,
            launch_lock_path=lock_path,
            expected_parent_boot_id=parent["boot_id"],
            expected_parent_start_ticks=parent["start_ticks"],
            expected_uid=parent["uid"], guardian_argv=guardian_command,
        )
        if with_worker_state:
            with open(os.path.join(epoch_root, "worker_state.json"), "w") as stream:
                json.dump({
                    "schema_version": "crescendo.persistent_maestro_worker.v1",
                    "worker_index": 0, "epoch": "worker-000-epoch-000",
                    "state": "launching", "workspace_root": workspace,
                    "control_root": control, "guardian_state_path": state_path,
                    "guardian_launch_protocol": GUARDIAN_LAUNCH_PROTOCOL,
                    "guardian_launch_manifest_path": os.path.join(
                        control, "legacy_guardian_launch.json",
                    ),
                    "guardian_launch_lock_path": lock_path,
                    "pid": None, "ocean_pid": None,
                }, stream)
        return {
            "epoch_root": epoch_root, "workspace": workspace, "control": control,
            "state_path": state_path, "descriptor": descriptor, "parent": parent,
            "token": worker_id, "ocean_command": ocean_command,
        }

    @staticmethod
    def _write_durable_guardian_state(launch, status):
        with open(launch["state_path"], "w") as stream:
            json.dump({
                "schema_version": "crescendo.ocean_worker_guardian.v1",
                "token": launch["token"], "cwd": launch["workspace"],
                "argv": launch["ocean_command"],
                "expected_parent_pid": launch["parent"]["pid"],
                "launch_boot_id": launch["parent"]["boot_id"],
                "guardian_pid": None, "guardian_start_ticks": None,
                "pid": None, "pgid": None, "sid": None,
                "pid_start_ticks": None, "status": status,
                "group_cleanup_complete": False,
                "descendant_cleanup_complete": False,
                "descendant_supervision_complete": False,
                "remaining_group_pids": [], "remaining_adopted_pids": [],
            }, stream)

    def test_worker_epoch_proof_blocks_manifest_to_guardian_state_launch_window(self):
        with tempfile.TemporaryDirectory() as root:
            launch = self._make_durable_worker_launch(root)
            try:
                proof = _worker_epoch_proof(launch["epoch_root"])
                self.assertFalse(proof["ok"])
                self.assertEqual(proof["kind"], "launch_in_progress")
            finally:
                close_legacy_guardian_launch_lock(launch["descriptor"])

            proof = _worker_epoch_proof(launch["epoch_root"])
            self.assertTrue(proof["ok"])
            self.assertEqual(proof["kind"], "ocean_not_spawned")

    def test_worker_epoch_proof_blocks_manifest_before_worker_state_commit(self):
        with tempfile.TemporaryDirectory() as root:
            launch = self._make_durable_worker_launch(root, with_worker_state=False)
            try:
                proof = _worker_epoch_proof(launch["epoch_root"])
                self.assertFalse(proof["ok"])
                self.assertEqual(proof["kind"], "launch_in_progress")
            finally:
                close_legacy_guardian_launch_lock(launch["descriptor"])

            proof = _worker_epoch_proof(launch["epoch_root"])
            self.assertTrue(proof["ok"])
            self.assertEqual(proof["kind"], "ocean_not_spawned")

    def test_worker_epoch_proof_blocks_starting_guardian_until_launch_lock_releases(self):
        with tempfile.TemporaryDirectory() as root:
            launch = self._make_durable_worker_launch(root)
            self._write_durable_guardian_state(launch, "starting")
            try:
                proof = _worker_epoch_proof(launch["epoch_root"])
                self.assertFalse(proof["ok"])
                self.assertEqual(proof["kind"], "launch_in_progress")
            finally:
                close_legacy_guardian_launch_lock(launch["descriptor"])

            proof = _worker_epoch_proof(launch["epoch_root"])
            self.assertTrue(proof["ok"])
            self.assertEqual(
                proof["kind"], "owned_processes_absent_after_guardian_loss",
            )

    def test_worker_epoch_proof_accepts_terminal_guardian_popen_failure(self):
        with tempfile.TemporaryDirectory() as root:
            launch = self._make_durable_worker_launch(root)
            self._write_durable_guardian_state(launch, "launch_failed")
            close_legacy_guardian_launch_lock(launch["descriptor"])

            proof = _worker_epoch_proof(launch["epoch_root"])
            self.assertTrue(proof["ok"])
            self.assertEqual(proof["kind"], "ocean_not_spawned")

    def _fake_script(self, root):
        path = os.path.join(root, "fake_controller.py")
        with open(path, "w") as stream:
            stream.write(FAKE_CONTROLLER)
        return path

    def _wait_file(self, path, timeout=3):
        deadline = time.time() + timeout
        while not os.path.isfile(path) and time.time() < deadline:
            time.sleep(0.01)
        self.assertTrue(os.path.isfile(path))

    def _liveness_lock_is_held(self, path):
        if fcntl is None:
            self.skipTest("requires POSIX fcntl.flock")
        try:
            descriptor = os.open(path, os.O_RDWR)
        except OSError as error:
            if getattr(error, "errno", None) == errno.ENOENT:
                return False
            raise
        try:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except (IOError, OSError):
                return True
            return False
        finally:
            os.close(descriptor)

    def test_liveness_lock_is_inherited_and_schema_persisted_until_close(self):
        with tempfile.TemporaryDirectory() as root:
            script = self._fake_script(root)
            proxy = PersistentControllerProxy(
                root, 0, {"setup": {}, "hierarchy": {}}, [], self._config(root),
                command_builder=lambda bootstrap: [sys.executable, script, bootstrap],
            ).start()
            self.assertIsNone(proxy._liveness_lock_descriptor)
            self.assertTrue(self._liveness_lock_is_held(proxy.liveness_lock_path))
            with open(os.path.join(proxy.epoch_root, "bootstrap.json"), "r") as stream:
                bootstrap = json.load(stream)
            with open(proxy.state_path, "r") as stream:
                state = json.load(stream)
            for document in (bootstrap, state):
                self.assertEqual(document["liveness_lock_schema"], LIVENESS_LOCK_SCHEMA)
                self.assertEqual(document["liveness_lock_path"], proxy.liveness_lock_path)
            self.assertEqual(proxy.close()["reason"], "closed")
            self.assertFalse(self._liveness_lock_is_held(proxy.liveness_lock_path))
            self.assertFalse(os.path.lexists(proxy.epoch_root))

    def test_child_startup_failure_releases_inherited_liveness_lock(self):
        with tempfile.TemporaryDirectory() as root:
            script = os.path.join(root, "exit_controller.py")
            with open(script, "w") as stream:
                stream.write("import sys, time\ntime.sleep(0.2)\nsys.exit(7)\n")
            proxy = PersistentControllerProxy(
                root, 0, {"setup": {}, "hierarchy": {}}, [], self._config(root),
                command_builder=lambda bootstrap: [sys.executable, script, bootstrap],
            )
            with self.assertRaisesRegex(PersistentControllerProcessError, "exited before ready"):
                proxy.start()
            self.assertIsNone(proxy._liveness_lock_descriptor)
            self.assertFalse(self._liveness_lock_is_held(proxy.liveness_lock_path))
            self.assertFalse(os.path.lexists(proxy.epoch_root))

    def test_popen_failure_releases_parent_liveness_lock(self):
        with tempfile.TemporaryDirectory() as root:
            missing = os.path.join(root, "missing-controller")
            proxy = PersistentControllerProxy(
                root, 0, {"setup": {}, "hierarchy": {}}, [], self._config(root),
                command_builder=lambda _bootstrap: [missing],
            )
            with self.assertRaises(OSError):
                proxy.start()
            self.assertIsNone(proxy._liveness_lock_descriptor)
            self.assertFalse(self._liveness_lock_is_held(proxy.liveness_lock_path))
            self.assertFalse(os.path.lexists(proxy.epoch_root))

    def test_atomic_rpc_reuses_one_controller_until_close(self):
        with tempfile.TemporaryDirectory() as root:
            script = self._fake_script(root)
            proxy = PersistentControllerProxy(
                root, 0, {"setup": {}, "hierarchy": {}}, [], self._config(root),
                command_builder=lambda bootstrap: [sys.executable, script, bootstrap],
            ).start()
            first = proxy.submit_evaluate(0, 0, {"x": 1.0}, {"candidate_timeout_seconds": 60})
            first_result = proxy._wait(first, 3, "test evaluate")
            commit = proxy.commit_candidate("g000_c0000")
            second = proxy.submit_evaluate(1, 0, {"x": 2.0}, {"candidate_timeout_seconds": 60})
            second_result = proxy._wait(second, 3, "test evaluate")
            closed = proxy.close()
            self.assertEqual(first_result["controller_pid"], second_result["controller_pid"])
            self.assertEqual(commit["controller_pid"], first_result["controller_pid"])
            self.assertTrue(commit["health"]["alive"])
            self.assertTrue(commit["health"]["tree_complete"])
            self.assertEqual(commit["health"]["pid"], first_result["controller_pid"])
            self.assertEqual(commit["health"]["pid_start_ticks"], proxy._start_ticks)
            self.assertIn(
                first_result["controller_pid"],
                [row["pid"] for row in commit["health"]["processes"]],
            )
            self.assertEqual(closed["controller_pid"], first_result["controller_pid"])
            self.assertTrue(closed["termination_proven"])
            self.assertTrue(closed["controller_epoch_cleanup"]["removed"])
            self.assertFalse(os.path.lexists(proxy.epoch_root))

    def test_active_health_sample_does_not_disturb_outstanding_rpc(self):
        with tempfile.TemporaryDirectory() as root:
            script = self._fake_script(root)
            proxy = PersistentControllerProxy(
                root, 0, {"setup": {}, "hierarchy": {}}, [], self._config(root),
                command_builder=lambda bootstrap: [sys.executable, script, bootstrap],
            ).start()
            request = proxy.submit_evaluate(
                0, 0, {"x": 1.0}, {"candidate_timeout_seconds": 60},
            )
            health = proxy.sample_health()
            self.assertTrue(health["alive"])
            self.assertTrue(health["tree_complete"])
            self.assertIs(proxy._outstanding, request)
            result = proxy._wait(request, 3, "test evaluate")
            self.assertEqual(result["candidate_id"], "g000_c0000")
            self.assertEqual(proxy.close()["reason"], "closed")

    @unittest.skipUnless(sys.platform.startswith("linux"), "requires Linux process identity")
    def test_real_controller_close_without_starting_eda_reclaims_epoch(self):
        with tempfile.TemporaryDirectory() as root:
            proxy = PersistentControllerProxy(
                root, 0, {"setup": {}, "hierarchy": {}}, [], self._config(root),
            ).start()
            epoch_root = proxy.epoch_root
            result = proxy.close()
            self.assertTrue(result["termination_proven"])
            self.assertTrue(result["controller_identity_owned"])
            self.assertEqual(
                result["controller_identity_proof"]["kind"],
                "controller_termination_proven",
            )
            self.assertTrue(result["controller_epoch_cleanup"]["removed"])
            self.assertFalse(os.path.lexists(epoch_root))

    def test_failed_rpc_response_is_retained_but_request_is_removed(self):
        with tempfile.TemporaryDirectory() as root:
            script = self._fake_script(root)
            proxy = PersistentControllerProxy(
                root, 0, {"setup": {}, "hierarchy": {}}, [], self._config(root),
                command_builder=lambda bootstrap: [sys.executable, script, bootstrap],
            ).start()
            request = proxy.submit_evaluate(
                0, 0, {"mode": "error"}, {"candidate_timeout_seconds": 60},
            )
            with self.assertRaisesRegex(Exception, "synthetic evaluation failure"):
                proxy._wait(request, 3, "test evaluate")
            self.assertEqual(os.listdir(os.path.join(proxy.epoch_root, "requests")), [])
            response_names = os.listdir(os.path.join(proxy.epoch_root, "responses"))
            self.assertEqual(response_names, [os.path.basename(request["response_path"])])
            with open(request["response_path"], "r") as stream:
                response = json.load(stream)
            self.assertFalse(response["ok"])
            self.assertEqual(response["error"], "synthetic evaluation failure")
            closed = proxy.close()
            self.assertTrue(closed["termination_proven"])
            self.assertFalse(closed["process_still_alive"])

    def test_structured_remote_fatal_response_preserves_environment_identity(self):
        with tempfile.TemporaryDirectory() as root:
            script = self._fake_script(root)
            proxy = PersistentControllerProxy(
                root, 0, {"setup": {}, "hierarchy": {}}, [], self._config(root),
                command_builder=lambda bootstrap: [sys.executable, script, bootstrap],
            ).start()
            request = proxy.submit_evaluate(
                0, 0, {"mode": "external_environment"},
                {"candidate_timeout_seconds": 60},
            )
            with self.assertRaises(PersistentControllerRemoteError) as raised:
                proxy._wait(request, 3, "test evaluate")
            error = raised.exception
            self.assertEqual(error.remote_error_type, "ExternalEdaEnvironmentChanged")
            self.assertEqual(error.failure_class, "external_environment")
            self.assertTrue(error.fatal_run)
            self.assertEqual(error.remote_traceback, "synthetic remote traceback")
            self.assertEqual(os.listdir(os.path.join(proxy.epoch_root, "requests")), [])
            self.assertTrue(os.path.isfile(request["response_path"]))
            closed = proxy.close()
            self.assertTrue(closed["termination_proven"])
            self.assertFalse(closed["process_still_alive"])

    @unittest.skipUnless(sys.platform.startswith("linux"), "requires controller subprocess")
    def test_real_controller_reports_environment_drift_before_maestro_start(self):
        with tempfile.TemporaryDirectory() as root:
            launcher = os.path.join(root, "spectre")
            with open(launcher, "w") as stream:
                stream.write("#!/bin/sh\nexit 0\n")
            os.chmod(launcher, 0o755)
            submit_script = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                "tools", "submit_spectre_job.sh",
            )
            setup = {"source": {}}
            cadence = {"spectre_bin": launcher}
            submission = {
                "schema_version": "crescendo.simulation_submission.v1",
                "controller": "maestro", "simulator": "spectre",
                "submit_script": os.path.abspath(submit_script),
            }
            environment = collect_external_eda_environment(
                setup, cadence, {}, submission=submission,
            )
            prepared = {
                "setup": setup, "hierarchy": {"designs": []},
                "eda_context": {
                    "external_environment": environment["identity"],
                    "external_environment_diagnostics": environment["diagnostics"],
                    "pdk": {}, "submission": submission,
                },
            }
            config = self._config(root)
            config["cadence"] = cadence
            config["runtime"]["spectre_submit_script"] = submit_script
            proxy = PersistentControllerProxy(root, 0, prepared, [], config).start()
            with open(launcher, "w") as stream:
                stream.write("#!/bin/sh\nexit 1\n")
            os.chmod(launcher, 0o755)
            request = proxy.submit_evaluate(
                0, 0, {}, {"candidate_timeout_seconds": 60},
            )
            with self.assertRaises(PersistentControllerRemoteError) as raised:
                proxy._wait(request, 5, "environment boundary")
            self.assertEqual(
                raised.exception.remote_error_type,
                "ExternalEdaEnvironmentChanged",
            )
            self.assertTrue(raised.exception.fatal_run)
            self.assertFalse(os.path.lexists(os.path.join(
                root, "workers", "worker_000", "epoch_000",
            )))
            retired = proxy.close()
            self.assertTrue(retired["termination_proven"])
            self.assertFalse(retired["process_still_alive"])

    def test_corrupt_response_taints_rpc_and_close_terminates_controller(self):
        with tempfile.TemporaryDirectory() as root:
            script = self._fake_script(root)
            proxy = PersistentControllerProxy(
                root, 0, {"setup": {}, "hierarchy": {}}, [], self._config(root),
                command_builder=lambda bootstrap: [sys.executable, script, bootstrap],
            ).start()
            request = proxy.submit_evaluate(0, 0, {"x": 1.0}, {"candidate_timeout_seconds": 60})
            self._wait_file(request["response_path"])
            with open(request["response_path"], "w") as stream:
                stream.write("{")
            with self.assertRaisesRegex(PersistentControllerProcessError, "response handling failed"):
                proxy.poll(request)
            self.assertIsNone(proxy._outstanding)
            closed = proxy.close()
            self.assertTrue(closed["termination_proven"])
            self.assertFalse(closed["process_still_alive"])

    def test_close_with_outstanding_request_terminates_instead_of_raising(self):
        with tempfile.TemporaryDirectory() as root:
            script = self._fake_script(root)
            proxy = PersistentControllerProxy(
                root, 0, {"setup": {}, "hierarchy": {}}, [], self._config(root),
                command_builder=lambda bootstrap: [sys.executable, script, bootstrap],
            ).start()
            proxy.submit_evaluate(0, 0, {"mode": "hang"}, {"candidate_timeout_seconds": 60})
            time.sleep(0.1)
            closed = proxy.close()
            self.assertTrue(closed["termination_proven"])
            self.assertFalse(closed["process_still_alive"])

    def test_fake_controller_ignores_unfinished_atomic_request(self):
        with tempfile.TemporaryDirectory() as root:
            script = self._fake_script(root)
            proxy = PersistentControllerProxy(
                root, 0, {"setup": {}, "hierarchy": {}}, [], self._config(root),
                command_builder=lambda bootstrap: [sys.executable, script, bootstrap],
            ).start()
            provisional = os.path.join(proxy.epoch_root, "requests",
                                       "00000001_EVALUATE.json.paused.tmp")
            try:
                with open(provisional, "w") as stream:
                    stream.flush()
                    os.fsync(stream.fileno())
                time.sleep(0.05)
                self.assertIsNone(proxy.process.poll())
                self.assertTrue(self._liveness_lock_is_held(proxy.liveness_lock_path))
                request = proxy.submit_evaluate(0, 0, {"x": 1.0}, {"candidate_timeout_seconds": 60})
                response = proxy._wait(request, 3, "final atomic request")
                self.assertEqual(response["candidate_id"], "g000_c0000")
                self.assertTrue(proxy.close()["artifact_cleanup_complete"])
            finally:
                if proxy.process.poll() is None:
                    proxy.terminate_and_prove("unfinished-request regression cleanup")

    def test_hung_controller_is_killed_after_term_grace(self):
        with tempfile.TemporaryDirectory() as root:
            script = self._fake_script(root)
            proxy = PersistentControllerProxy(
                root, 0, {"setup": {}, "hierarchy": {}}, [], self._config(root),
                command_builder=lambda bootstrap: [sys.executable, script, bootstrap],
            ).start()
            proxy.submit_evaluate(0, 0, {"mode": "hang"}, {"candidate_timeout_seconds": 60})
            time.sleep(0.1)
            self.assertTrue(self._liveness_lock_is_held(proxy.liveness_lock_path))
            result = proxy.terminate_and_prove("synthetic hang")
            self.assertTrue(result["term_sent"])
            self.assertTrue(result["kill_sent"])
            self.assertTrue(result["termination_proven"])
            self.assertFalse(result["process_still_alive"])
            self.assertFalse(self._liveness_lock_is_held(proxy.liveness_lock_path))
            self.assertFalse(os.path.lexists(proxy.epoch_root))

    def test_live_identity_mismatch_refuses_signal_and_retains_epoch(self):
        with tempfile.TemporaryDirectory() as root:
            script = self._fake_script(root)
            proxy = PersistentControllerProxy(
                root, 0, {"setup": {}, "hierarchy": {}}, [], self._config(root),
                command_builder=lambda bootstrap: [sys.executable, script, bootstrap],
            ).start()
            original_start_ticks = proxy._start_ticks
            proxy._start_ticks = original_start_ticks + 1
            result = proxy.terminate_and_prove("synthetic identity mismatch")
            self.assertFalse(result["term_sent"])
            self.assertFalse(result["termination_proven"])
            self.assertTrue(result["process_still_alive"])
            self.assertTrue(os.path.isdir(proxy.epoch_root))
            proxy._start_ticks = original_start_ticks
            cleanup = proxy.terminate_and_prove("test cleanup")
            self.assertTrue(cleanup["termination_proven"])
            self.assertFalse(os.path.lexists(proxy.epoch_root))

    def test_closed_controller_with_state_identity_mismatch_is_retained_and_blocks_restart(self):
        with tempfile.TemporaryDirectory() as root:
            script = self._fake_script(root)
            proxy = PersistentControllerProxy(
                root, 0, {"setup": {}, "hierarchy": {}}, [], self._config(root),
                command_builder=lambda bootstrap: [sys.executable, script, bootstrap],
            ).start()
            with open(proxy.state_path, "r") as stream:
                state = json.load(stream)
            state["token"] = "ambiguous-token"
            with open(proxy.state_path, "w") as stream:
                json.dump(state, stream)
            result = proxy.close()
            self.assertFalse(result["termination_proven"])
            self.assertFalse(result["process_still_alive"])
            self.assertEqual(
                result["controller_identity_proof"]["kind"],
                "controller_identity_unproven",
            )
            self.assertTrue(os.path.isdir(proxy.epoch_root))
            with self.assertRaisesRegex(
                PersistentControllerError, "prior controller epoch was not safely reclaimed",
            ):
                proxy.restart()

    def test_closed_controller_with_unsafe_epoch_tree_is_retained_and_blocks_restart(self):
        with tempfile.TemporaryDirectory() as root:
            script = self._fake_script(root)
            proxy = PersistentControllerProxy(
                root, 0, {"setup": {}, "hierarchy": {}}, [], self._config(root),
                command_builder=lambda bootstrap: [sys.executable, script, bootstrap],
            ).start()
            outside = os.path.join(root, "outside")
            os.makedirs(outside)
            marker = os.path.join(outside, "keep.txt")
            with open(marker, "w") as stream:
                stream.write("keep")
            os.symlink(outside, os.path.join(proxy.epoch_root, "unsafe_link"))
            result = proxy.close()
            self.assertTrue(result["termination_proven"])
            self.assertTrue(result["process_termination_proven"])
            self.assertFalse(result["artifact_cleanup_complete"])
            self.assertEqual(result["cleanup"]["state"], "artifact_cleanup_failed")
            self.assertFalse(result["cleanup"]["controller_epoch"]["ok"])
            self.assertTrue(os.path.isdir(proxy.epoch_root))
            self.assertTrue(os.path.isfile(marker))
            with self.assertRaisesRegex(
                PersistentControllerError, "prior controller epoch was not safely reclaimed",
            ):
                proxy.restart()

    def test_proven_terminated_worker_epoch_is_source_checked_and_removed(self):
        with tempfile.TemporaryDirectory() as root:
            script = self._fake_script(root)
            proxy = PersistentControllerProxy(
                root, 0, {"setup": {}, "hierarchy": {}}, [], self._config(root),
                command_builder=lambda bootstrap: [sys.executable, script, bootstrap],
            ).start()
            workspace = os.path.join(root, "workers", "worker_000", "epoch_000", "workspace")
            os.makedirs(workspace)
            with open(os.path.join(workspace, "workspace.json"), "w") as stream:
                json.dump({
                    "root": workspace,
                    "source_libraries": {},
                    "source_snapshots": {},
                }, stream)
            result = proxy.terminate_and_prove("synthetic cleanup")
            self.assertTrue(result["termination_proven"])
            self.assertTrue(result["cleanup"]["removed"])
            self.assertFalse(os.path.exists(os.path.dirname(workspace)))

    def test_proven_worker_not_created_partial_epoch_is_removed(self):
        with tempfile.TemporaryDirectory() as root:
            script = self._fake_script(root)
            proxy = PersistentControllerProxy(
                root, 0, {"setup": {}, "hierarchy": {}}, [], self._config(root),
                command_builder=lambda bootstrap: [sys.executable, script, bootstrap],
            ).start()
            epoch_root = os.path.join(root, "workers", "worker_000", "epoch_000")
            os.makedirs(epoch_root)
            result = proxy.terminate_and_prove("synthetic partial cleanup")
            self.assertTrue(result["termination_proven"])
            self.assertEqual(
                result["worker_cleanup_proof"]["worker_epochs"][0]["kind"],
                "worker_not_created",
            )
            self.assertEqual(
                result["cleanup"]["worker_epochs"][0]["proof_kind"],
                "worker_not_created",
            )
            self.assertFalse(os.path.exists(epoch_root))

    def test_proven_ocean_not_spawned_partial_epoch_is_removed(self):
        with tempfile.TemporaryDirectory() as root:
            script = self._fake_script(root)
            proxy = PersistentControllerProxy(
                root, 0, {"setup": {}, "hierarchy": {}}, [], self._config(root),
                command_builder=lambda bootstrap: [sys.executable, script, bootstrap],
            ).start()
            epoch_root = os.path.join(root, "workers", "worker_000", "epoch_000")
            os.makedirs(os.path.join(epoch_root, "workspace"))
            with open(os.path.join(epoch_root, "worker_state.json"), "w") as stream:
                json.dump({"state": "new", "pid": None, "ocean_pid": None}, stream)
            result = proxy.terminate_and_prove("synthetic pre-spawn cleanup")
            self.assertTrue(result["termination_proven"])
            self.assertEqual(
                result["worker_cleanup_proof"]["worker_epochs"][0]["kind"],
                "ocean_not_spawned",
            )
            self.assertFalse(os.path.exists(epoch_root))

    def test_guardian_launch_failed_partial_epoch_is_removed(self):
        with tempfile.TemporaryDirectory() as root:
            script = self._fake_script(root)
            proxy = PersistentControllerProxy(
                root, 0, {"setup": {}, "hierarchy": {}}, [], self._config(root),
                command_builder=lambda bootstrap: [sys.executable, script, bootstrap],
            ).start()
            epoch_root = os.path.join(root, "workers", "worker_000", "epoch_000")
            control_root = persistent_worker_control_root(
                epoch_root, "worker-000-epoch-000",
            )
            os.makedirs(control_root)
            with open(os.path.join(control_root, "guardian_state.json"), "w") as stream:
                json.dump({
                    "status": "launch_failed", "pid": None, "pgid": None,
                }, stream)
            result = proxy.terminate_and_prove("synthetic guardian launch failure")
            self.assertTrue(result["termination_proven"])
            self.assertEqual(
                result["worker_cleanup_proof"]["worker_epochs"][0]["kind"],
                "ocean_not_spawned",
            )
            self.assertFalse(os.path.exists(epoch_root))

    def test_worker_epoch_scan_rejects_symlinked_worker_slot(self):
        with tempfile.TemporaryDirectory() as root:
            workers_root = os.path.join(root, "workers")
            outside = os.path.join(root, "outside_worker")
            os.makedirs(workers_root)
            os.makedirs(outside)
            os.symlink(outside, os.path.join(workers_root, "worker_000"))
            with self.assertRaisesRegex(PersistentControllerError, "not a real directory"):
                _worker_epoch_roots(root, 0)

    def test_termination_fails_closed_on_symlinked_worker_epoch(self):
        with tempfile.TemporaryDirectory() as root:
            script = self._fake_script(root)
            proxy = PersistentControllerProxy(
                root, 0, {"setup": {}, "hierarchy": {}}, [], self._config(root),
                command_builder=lambda bootstrap: [sys.executable, script, bootstrap],
            ).start()
            worker_root = os.path.join(root, "workers", "worker_000")
            outside = os.path.join(root, "outside_epoch")
            os.makedirs(worker_root)
            os.makedirs(outside)
            marker = os.path.join(outside, "keep.txt")
            with open(marker, "w") as stream:
                stream.write("keep")
            os.symlink(outside, os.path.join(worker_root, "epoch_000"))
            result = proxy.terminate_and_prove("synthetic unsafe epoch")
            self.assertFalse(result["termination_proven"])
            self.assertEqual(result["worker_cleanup_proof"]["kind"], "worker_epoch_scan_failed")
            self.assertTrue(os.path.isfile(marker))
            self.assertFalse(result["process_still_alive"])

    def test_termination_retains_foreign_worker_epoch_namespace(self):
        with tempfile.TemporaryDirectory() as root:
            script = self._fake_script(root)
            proxy = PersistentControllerProxy(
                root, 0, {"setup": {}, "hierarchy": {}}, [], self._config(root),
                command_builder=lambda bootstrap: [sys.executable, script, bootstrap],
            ).start()
            worker_root = os.path.join(root, "workers", "worker_000")
            owned_epoch = os.path.join(worker_root, "epoch_000")
            foreign_epoch = os.path.join(worker_root, "epoch_c000001_l000000")
            os.makedirs(owned_epoch)
            os.makedirs(foreign_epoch)

            result = proxy.terminate_and_prove("synthetic foreign worker epoch")

            self.assertFalse(result["termination_proven"])
            proof = result["worker_cleanup_proof"]
            self.assertEqual(proof["kind"], "foreign_worker_epoch_namespace")
            self.assertEqual(proof["foreign_worker_epochs"], [foreign_epoch])
            self.assertEqual(
                [row["epoch_root"] for row in proof["worker_epochs"]],
                [owned_epoch],
            )
            self.assertTrue(os.path.isdir(owned_epoch))
            self.assertTrue(os.path.isdir(foreign_epoch))
            self.assertTrue(os.path.isdir(proxy.epoch_root))
            self.assertFalse(result["process_still_alive"])

    def test_hung_commit_recycles_the_slot_after_proven_termination(self):
        with tempfile.TemporaryDirectory() as root:
            script = self._fake_script(root)
            config = self._config(root)
            config["runtime"]["controller_commit_timeout_seconds"] = 0.1
            proxy = PersistentControllerProxy(
                root, 0, {"setup": {}, "hierarchy": {}}, [], config,
                command_builder=lambda bootstrap: [sys.executable, script, bootstrap],
            ).start()
            first_pid = proxy.process.pid
            first_lock_path = proxy.liveness_lock_path
            first_epoch_root = proxy.epoch_root
            cleanup = proxy.commit_candidate("hang")
            self.assertFalse(cleanup["ok"])
            self.assertTrue(cleanup["recycled"])
            self.assertNotEqual(proxy.process.pid, first_pid)
            self.assertFalse(self._liveness_lock_is_held(first_lock_path))
            self.assertFalse(os.path.lexists(first_epoch_root))
            self.assertEqual(proxy.epoch_index, 1)
            self.assertTrue(self._liveness_lock_is_held(proxy.liveness_lock_path))
            request = proxy.submit_evaluate(1, 0, {"x": 2.0}, {"candidate_timeout_seconds": 60})
            result = proxy._wait(request, 3, "test evaluate")
            self.assertEqual(result["index"], 1)
            self.assertEqual(proxy.close()["reason"], "closed")
            self.assertFalse(self._liveness_lock_is_held(proxy.liveness_lock_path))
            self.assertFalse(os.path.lexists(proxy.epoch_root))

    @unittest.skipUnless(sys.platform.startswith("linux"), "requires Linux parent-death signaling")
    def test_real_controller_exits_when_parent_is_sigkilled_without_starting_eda(self):
        with tempfile.TemporaryDirectory() as root:
            pid_path = os.path.join(root, "controller.pid")
            helper_path = os.path.join(root, "launch_parent.py")
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            helper = '''
import os
import time
from crescendo_engine.persistent_controller import PersistentControllerProxy

root = {root!r}
proxy = PersistentControllerProxy(
    root, 0, {{"setup": {{}}, "hierarchy": {{}}}}, [],
    {{"runtime": {{"run_root": root, "controller_startup_timeout_seconds": 5}}}},
).start()
with open({pid_path!r}, "w") as stream:
    stream.write(str(proxy.process.pid))
    stream.flush()
    os.fsync(stream.fileno())
while True:
    time.sleep(1)
'''.format(root=root, pid_path=pid_path)
            with open(helper_path, "w") as stream:
                stream.write(helper)
            environment = dict(os.environ)
            environment["PYTHONPATH"] = project_root
            parent = subprocess.Popen([sys.executable, helper_path], env=environment)
            deadline = time.time() + 10
            while not os.path.isfile(pid_path) and time.time() < deadline:
                time.sleep(0.05)
            with open(pid_path, "r") as stream:
                controller_pid = int(stream.read())
            os.kill(parent.pid, signal.SIGKILL)
            self.assertEqual(parent.wait(timeout=10), -signal.SIGKILL)
            while os.path.exists("/proc/%d" % controller_pid) and time.time() < deadline:
                try:
                    with open("/proc/%d/stat" % controller_pid, "r") as stream:
                        stat = stream.read()
                    closing = stat.rfind(")")
                    if closing >= 0 and stat[closing + 2:].split()[0] == "Z":
                        break
                except (IOError, OSError):
                    break
                time.sleep(0.05)
            if os.path.exists("/proc/%d" % controller_pid):
                with open("/proc/%d/stat" % controller_pid, "r") as stream:
                    stat = stream.read()
                closing = stat.rfind(")")
                self.assertEqual(stat[closing + 2:].split()[0], "Z")
            liveness_path = os.path.join(
                root, "controllers", "slot_000", "epoch_000", "controller_liveness.lock",
            )
            self.assertFalse(self._liveness_lock_is_held(liveness_path))


if __name__ == "__main__":
    unittest.main()
