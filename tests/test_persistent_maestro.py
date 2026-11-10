from __future__ import print_function

import json
import fcntl
import os
import re
import signal
import subprocess
import tempfile
import unittest

try:
    from unittest import mock
except ImportError:
    import mock

from crescendo_engine.persistent_maestro import (
    PersistentMaestroWorker,
    PersistentMaestroWorkerProcessError,
    PersistentMaestroWorkerProtocolError,
    PersistentMaestroWorkerStartupError,
    PersistentMaestroWorkerTainted,
    PersistentMaestroWorkerTimeout,
    _guardian_environment,
    _validate_pristine_records,
    prepare_persistent_batch,
    run_persistent_batch,
    validate_batch_clones,
    validate_candidate_records,
)
from crescendo_engine.legacy_guardian import GUARDIAN_SCOPE_CONFIG_KEY


class FakeClock(object):
    def __init__(self):
        self.value = 0.0

    def monotonic(self):
        return self.value

    def sleep(self, seconds):
        self.value += float(seconds)


class FakeStdin(object):
    def __init__(self, process):
        self.process = process
        self.commands = []
        self.closed = False

    def write(self, value):
        self.commands.append(value)
        match = re.search(r'load\("(.*)"\)', value.strip())
        if not match:
            raise AssertionError("worker stdin accepted a non-load command: %r" % value)
        path = match.group(1).replace('\\"', '"').replace('\\\\', '\\')
        self.process.factory.on_load(self.process, path)

    def flush(self):
        pass

    def close(self):
        self.closed = True


class FakeProcess(object):
    def __init__(self, factory, pid):
        self.factory = factory
        self.pid = pid
        self.returncode = None
        self.stdin = FakeStdin(self)
        self.terminated = False
        self.killed = False
        self.wait_timeouts = []
        self.history_name = "Interactive.%d" % pid

    def poll(self):
        return self.returncode

    def wait(self, timeout=None):
        self.wait_timeouts.append(timeout)
        if self.returncode is None:
            raise subprocess.TimeoutExpired(["fake-ocean"], timeout)
        return self.returncode

    def terminate(self):
        self.terminated = True
        self.returncode = -signal.SIGTERM

    def kill(self):
        self.killed = True
        self.returncode = -signal.SIGKILL


class FakePopenFactory(object):
    CALL = re.compile(
        r'crescWriteWorkerStatus\("([^"]*)" "([^"]*)" ([0-9]+) "([^"]*)" "([^"]*)" "([A-Z_]+)"'
    )

    def __init__(self, mode="success"):
        self.mode = mode
        self.calls = []
        self.processes = []
        self.signals = []
        self.loaded_paths = []

    def __call__(self, argv, **kwargs):
        self.calls.append((list(argv), kwargs))
        process = FakeProcess(self, 41000 + len(self.processes))
        self.processes.append(process)
        return process

    @staticmethod
    def _unescape(value):
        return value.replace('\\"', '"').replace('\\\\', '\\')

    def _request(self, path):
        with open(path, "r") as stream:
            text = stream.read()
        match = self.CALL.search(text)
        if not match:
            raise AssertionError("generated SKILL has no concrete worker marker call: %s" % path)
        return {
            "path": self._unescape(match.group(1)),
            "epoch": self._unescape(match.group(2)),
            "sequence": int(match.group(3)),
            "nonce": self._unescape(match.group(4)),
            "candidate_id": self._unescape(match.group(5)),
            "first_state": match.group(6),
        }

    @staticmethod
    def _append(request, state, epoch=None, nonce=None, sequence=None, candidate_id=None, message=""):
        with open(request["path"], "a") as stream:
            stream.write("%s\t%d\t%s\t%s\t%s\tfake-time\t%s\n" % (
                epoch if epoch is not None else request["epoch"],
                sequence if sequence is not None else request["sequence"],
                nonce if nonce is not None else request["nonce"],
                candidate_id if candidate_id is not None else request["candidate_id"],
                state, message,
            ))

    def on_load(self, process, path):
        self.loaded_paths.append(path)
        name = os.path.basename(path)
        request = self._request(path)
        if name == "bootstrap.il":
            manifest_path = os.path.join(os.path.dirname(path), "worker_manifest.json")
            with open(manifest_path, "r") as stream:
                manifest = json.load(stream)
            if "--expected-parent-pid" in self.calls[-1][0]:
                with open(manifest["native_corner_schedule_path"], "w") as stream:
                    stream.write('NOMINAL\t1\nNOMINAL_TEST\t"T"\t1\nREAD_OK\n')
            with open(manifest["clone_records"], "w") as stream:
                for row in manifest["clone_plan"]:
                    stream.write("CLONE\t%s\t%s\t%s\t%s\n" % (
                        row["occurrence_path"], row["source_cell"],
                        row["clone_cell"], row["parent_cell"],
                    ))
            with open(manifest["pristine_records"], "w") as stream:
                for row in manifest["pristine_cells"]:
                    stream.write("PRISTINE\t%s\t%s\t%s\t%s\n" % (
                        row["library"], row["cell"], row["pristine_library"],
                        row["pristine_cell"],
                    ))
            self._append(request, "STARTING")
            if self.mode == "startup_error":
                self._append(request, "STARTUP_ERROR", message="bootstrap failed")
            elif self.mode != "startup_timeout":
                self._append(request, "READY")
            return
        if name == "persistent_worker_candidate.il":
            manifest_path = os.path.join(os.path.dirname(path), "persistent_worker_candidate.json")
            with open(manifest_path, "r") as stream:
                manifest = json.load(stream)
            with open(manifest["pristine_restore_path"], "w") as stream:
                for row in manifest["pristine_cells"]:
                    stream.write("RESTORED\t%s\t%s\t%s\t%s\n" % (
                        row["library"], row["cell"], row["pristine_library"],
                        row["pristine_cell"],
                    ))
            if self.mode == "candidate_death":
                self._append(request, "ACCEPTED")
                process.returncode = 17
                return
            if self.mode == "candidate_timeout":
                for state in ("ACCEPTED", "BEGIN", "PATCHED"):
                    self._append(request, state)
                return
            if self.mode == "candidate_error":
                for state in ("ACCEPTED", "BEGIN", "ERROR"):
                    self._append(request, state)
                return
            if self.mode == "invalid_protocol":
                for state in ("ACCEPTED", "PATCHED", "DONE"):
                    self._append(request, state)
                return
            if self.mode == "stale_then_success":
                self._append(request, "DONE", epoch="old-epoch", nonce="old-nonce")
                self._append(request, "DONE", sequence=request["sequence"] + 1)
                self._append(request, "DONE", nonce="wrong-nonce")
                self._append(request, "DONE", candidate_id="wrong-candidate")
            for state in PersistentMaestroWorker.CANDIDATE_STATES:
                self._append(
                    request, state,
                    message=(process.history_name or "Interactive.%d" % request["sequence"])
                    if state in ("RESULTS_READY", "DONE") else "",
                )
            return
        if name == "persistent_worker_cleanup.il":
            if self.mode == "cleanup_timeout":
                self._append(request, "CLEANUP_BEGIN")
                return
            states = ("CLEANUP_BEGIN", "CLEANUP_ERROR") if self.mode == "cleanup_error" else (
                "CLEANUP_BEGIN", "CLEANUP_DONE",
            )
            for state in states:
                self._append(request, state)
            return
        if name == "shutdown.il":
            for state in PersistentMaestroWorker.SHUTDOWN_STATES:
                self._append(request, state)
            process.returncode = 0
            return
        raise AssertionError("unexpected worker load path: %s" % path)

    def killpg(self, pgid, sig):
        self.signals.append((pgid, sig))
        for process in self.processes:
            if process.pid == pgid:
                if self.mode == "unreapable":
                    continue
                if sig == signal.SIGTERM and self.mode != "term_hangs":
                    process.returncode = -signal.SIGTERM
                if sig == signal.SIGKILL:
                    process.returncode = -signal.SIGKILL


class PersistentMaestroTest(unittest.TestCase):
    def _fixture(self, root):
        copied = os.path.join(root, "D")
        os.makedirs(copied)
        workspace = {
            "root": root, "setup_dir": os.path.join(root, "SETUP", "maestro"),
            "primary_library": "D", "setup_cell": "SETUP",
            "copied_libraries": {"D": copied},
            "setup": {"tests": [{"name": "T", "enabled": True}], "corners": []},
        }
        os.makedirs(workspace["setup_dir"])
        workspace["setup_sdb"] = os.path.join(workspace["setup_dir"], "maestro.sdb")
        with open(workspace["setup_sdb"], "w") as stream:
            stream.write('<setupdb version="6">maestro<active>Active Setup<tests>'
                         '<test enabled="1">T<tooloptions><option>sim<value>spectre</value>'
                         '</option></tooloptions></test></tests><corners/></active></setupdb>')
        from crescendo_engine.ade_setup import inspect_setup
        workspace["setup"] = inspect_setup(workspace["setup_dir"])
        config = {
            "cadence": {"spectre_bin": "/eda/spectre", "ocean_bin": "/eda/ocean",
                        "cadence_root": "/eda/ic", "pdk_adapter": "generic-cdf",
                        "extra_binary_paths": [], "extra_library_paths": []},
            "runtime": {"simulation_timeout_seconds": 9, "candidate_timeout_seconds": 30,
                        "ocean_startup_timeout_seconds": 5, "command_timeout_seconds": 10},
        }
        return workspace, config

    def _parameter(self, parameter_id, hierarchy_path, owner_path, chain, name="w"):
        return {
            "parameter_id": parameter_id, "backend": "schematic", "name": name,
            "cdf_type": "string", "current_value": "1u", "default_value": "1u",
            "design_library": "D", "design_cell": "TOP", "design_view": "schematic",
            "owner_library": "D", "owner_cell": "LEAF" if owner_path != "/" else "TOP",
            "owner_occurrence_path": owner_path, "hierarchy_path": hierarchy_path,
            "master_library": "PDK", "master_cell": "nmos", "occurrence_chain": chain,
        }

    def _chain(self, prefix):
        return [
            {"hierarchy_path": prefix, "parent_path": "/", "name": prefix.strip("/"),
             "master_library": "D", "master_cell": "MID", "master_view": "symbol",
             "master_view_type": "schematicSymbol"},
            {"hierarchy_path": prefix + "/I0", "parent_path": prefix, "name": "I0",
             "master_library": "D", "master_cell": "LEAF", "master_view": "symbol",
             "master_view_type": "schematicSymbol"},
        ]

    def _worker(self, root, mode="success", parameters=None):
        workspace, config = self._fixture(root)
        factory = FakePopenFactory(mode)
        clock = FakeClock()
        parameters = parameters or [self._parameter("p", "/M0", "/", [])]
        worker = PersistentMaestroWorker(
            workspace, config, parameters, root, worker_index=3, epoch="epoch-one",
            popen_factory=factory, clock=clock.monotonic, sleeper=clock.sleep,
            killpg=factory.killpg, getpgid=lambda pid: pid, poll_interval=0.1,
        )
        return worker, factory, clock

    def test_guardian_python_environment_bridges_eda_dynamic_loader_paths(self):
        child = {
            "PATH": "/eda/bin:/usr/bin",
            "LD_LIBRARY_PATH": "/eda/lib:/usr/lib64",
            "LD_PRELOAD": "/eda/lib/preload.so",
            "CDS_LIC_FILE": "/eda/license.dat",
        }
        guardian = _guardian_environment(child)
        self.assertNotIn("LD_LIBRARY_PATH", guardian)
        self.assertNotIn("LD_PRELOAD", guardian)
        self.assertEqual(
            guardian["CRESCENDO_GUARDIAN_CHILD_LD_LIBRARY_PATH"], child["LD_LIBRARY_PATH"],
        )
        self.assertEqual(guardian["CRESCENDO_GUARDIAN_CHILD_LD_PRELOAD"], child["LD_PRELOAD"])
        self.assertEqual(guardian["CDS_LIC_FILE"], child["CDS_LIC_FILE"])

    def test_same_worker_epoch_in_two_runs_uses_distinct_scoped_guardian_tokens(self):
        with tempfile.TemporaryDirectory() as root:
            workers = []
            for name, scope in (("run-a", "a" * 32), ("run-b", "b" * 32)):
                worker_root = os.path.join(root, name)
                os.makedirs(worker_root)
                workspace, config = self._fixture(worker_root)
                config["runtime"][GUARDIAN_SCOPE_CONFIG_KEY] = scope
                workers.append(PersistentMaestroWorker(
                    workspace, config, [], worker_root,
                    worker_index=0, epoch="worker-000-epoch-000",
                ))
            self.assertNotEqual(workers[0].guardian_token, workers[1].guardian_token)
            for worker, scope in zip(workers, ("a" * 32, "b" * 32)):
                self.assertEqual(
                    worker.guardian_token_identity["scope_token"], scope,
                )
                with open(worker.worker_state_path) as stream:
                    state = json.load(stream)
                self.assertEqual(
                    state["guardian_token_identity"], worker.guardian_token_identity,
                )
                self.assertEqual(state["guardian_token"], worker.guardian_token)

    def test_worker_state_tolerates_valid_non_object_guardian_json(self):
        with tempfile.TemporaryDirectory() as root:
            worker, _factory, _clock = self._worker(root)
            with open(worker.guardian_state_path, "w") as stream:
                json.dump([], stream)
            with open(worker.guardian_launch_manifest_path, "w") as stream:
                json.dump([], stream)
            worker._write_state()
            with open(worker.worker_state_path, "r") as stream:
                state = json.load(stream)
            self.assertIsNone(state["guardian_state_token"])
            self.assertIsNone(state["guardian_parent_pid"])

    def test_guardian_launch_intent_is_atomic_before_popen(self):
        with tempfile.TemporaryDirectory() as root:
            worker, factory, _clock = self._worker(root)
            captured = {}

            def inspect_before_popen(argv, **kwargs):
                self.assertFalse(os.path.exists(worker.guardian_state_path))
                with open(worker.guardian_launch_manifest_path, "r") as stream:
                    captured["manifest"] = json.load(stream)
                with open(worker.worker_state_path, "r") as stream:
                    captured["worker"] = json.load(stream)
                captured["argv"] = list(argv)
                captured["kwargs"] = kwargs
                return factory(argv, **kwargs)

            worker._popen_factory = inspect_before_popen
            worker._use_guardian = True
            worker.start()
            manifest = captured["manifest"]
            self.assertEqual(manifest["token"], worker.guardian_token)
            self.assertEqual(
                manifest["guardian_token_identity"], worker.guardian_token_identity,
            )
            self.assertEqual(
                captured["worker"]["guardian_token_identity"],
                worker.guardian_token_identity,
            )
            with open("/proc/sys/kernel/random/boot_id", "r") as stream:
                boot_id = stream.read().strip()
            self.assertEqual(manifest["expected_parent_boot_id"], boot_id)
            self.assertEqual(manifest["expected_parent_pid"], os.getpid())
            self.assertGreater(manifest["expected_parent_start_ticks"], 0)
            self.assertEqual(manifest["expected_uid"], os.getuid())
            self.assertEqual(manifest["guardian_argv"], captured["argv"])
            lock_fd = int(captured["argv"][captured["argv"].index("--launch-lock-fd") + 1])
            self.assertEqual(captured["kwargs"]["pass_fds"], (lock_fd,))
            self.assertIn(lock_fd, captured["kwargs"]["pass_fds"])
            self.assertEqual(
                captured["kwargs"]["env"]["CRESCENDO_LEGACY_GUARDIAN_TOKEN"],
                worker.guardian_token,
            )
            self.assertEqual(captured["worker"]["state"], "launching")
            self.assertEqual(captured["worker"]["pid"], None)
            self.assertEqual(captured["worker"]["guardian_state_path"], worker.guardian_state_path)
            self.assertEqual(captured["worker"]["launch_boot_id"], boot_id)
            self.assertEqual(
                captured["worker"]["guardian_launch_manifest_path"],
                worker.guardian_launch_manifest_path,
            )
            self.assertEqual(
                captured["worker"]["guardian_launch_lock_path"],
                worker.guardian_launch_lock_path,
            )
            with self.assertRaises(OSError):
                os.fstat(lock_fd)
            descriptor = os.open(worker.guardian_launch_lock_path, os.O_RDWR)
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            finally:
                os.close(descriptor)
            worker._use_guardian = False
            worker.close()

    def test_guardian_popen_failure_records_safe_terminal_launch_failure(self):
        with tempfile.TemporaryDirectory() as root:
            worker, _factory, _clock = self._worker(root)

            def fail_popen(_argv, **_kwargs):
                raise OSError("synthetic guardian launch failure")

            worker._popen_factory = fail_popen
            worker._use_guardian = True
            with self.assertRaises(PersistentMaestroWorkerStartupError):
                worker.start()
            with open(worker.guardian_state_path, "r") as stream:
                guardian = json.load(stream)
            self.assertEqual(guardian["status"], "launch_failed")
            self.assertIsNone(guardian["guardian_pid"])
            self.assertIsNone(guardian["pid"])
            self.assertIsNone(guardian["pgid"])
            self.assertEqual(guardian["expected_parent_pid"], os.getpid())
            self.assertIn("synthetic guardian launch failure", guardian["error"])
            descriptor = os.open(worker.guardian_launch_lock_path, os.O_RDWR)
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            finally:
                os.close(descriptor)
            self.assertTrue(worker.closed)

    def test_guardian_launch_fd_is_not_double_closed_after_numeric_reuse(self):
        with tempfile.TemporaryDirectory() as root:
            worker, _factory, _clock = self._worker(root)
            worker._use_guardian = True
            sentinel_path = os.path.join(root, "sentinel")
            reused = []
            from crescendo_engine import persistent_maestro as module
            original_close = module.close_legacy_guardian_launch_lock

            def close_and_reuse(descriptor):
                original_close(descriptor)
                replacement = os.open(sentinel_path, os.O_RDWR | os.O_CREAT, 0o600)
                self.assertEqual(replacement, descriptor)
                reused.append(replacement)

            try:
                with mock.patch.object(
                    module, "close_legacy_guardian_launch_lock", side_effect=close_and_reuse,
                ):
                    worker.start()
                self.assertEqual(len(reused), 1)
                os.fstat(reused[0])
            finally:
                for descriptor in reused:
                    try:
                        os.close(descriptor)
                    except OSError:
                        pass
                worker._use_guardian = False
                worker.close()

    def test_external_watchdog_abort_signals_only_owned_supervisor_group(self):
        with tempfile.TemporaryDirectory() as root:
            worker, factory, _clock = self._worker(root)
            worker.start()
            process = worker.process
            result = worker.request_abort("synthetic scheduler watchdog")
            self.assertTrue(result["requested"])
            self.assertEqual(result["pid"], process.pid)
            self.assertEqual(factory.signals, [(process.pid, signal.SIGTERM)])
            self.assertTrue(process.stdin.closed)
            self.assertTrue(worker.tainted)
            self.assertEqual(worker.state, "abort_requested")
            worker._use_guardian = False
            worker.terminate(reason="test cleanup")

    def test_deep_shared_subcells_clone_once_and_keep_occurrences_independent(self):
        with tempfile.TemporaryDirectory() as root:
            workspace, config = self._fixture(root)
            parameters = [
                self._parameter("p1", "/X1/I0/M0", "/X1/I0", self._chain("/X1")),
                self._parameter("p2", "/X2/I0/M0", "/X2/I0", self._chain("/X2")),
            ]
            candidates = [{"candidate_id": "a", "values": {"p1": 2e-6, "p2": 3e-6}},
                          {"candidate_id": "b", "values": {"p1": 4e-6, "p2": 5e-6}}]
            with mock.patch("crescendo_engine.persistent_maestro.build_occurrence_clone_plan",
                            wraps=__import__("crescendo_engine.workspace", fromlist=["build_occurrence_clone_plan"]).build_occurrence_clone_plan) as build:
                batch = prepare_persistent_batch(workspace, config, parameters, candidates, os.path.join(root, "batch"))
            self.assertEqual(build.call_count, 1)
            self.assertEqual(len(batch["clone_plan"]), 4)
            first_targets = [(row["library"], row["cell"]) for row in batch["candidates"][0]["patch_plan"]["parameters"]["parameters"]]
            second_targets = [(row["library"], row["cell"]) for row in batch["candidates"][1]["patch_plan"]["parameters"]["parameters"]]
            self.assertEqual(first_targets, second_targets)
            self.assertNotEqual(first_targets[0], first_targets[1])
            with open(batch["script_path"]) as stream:
                script = stream.read()
            self.assertEqual(script.count("dbCopyCellView"), 8)
            self.assertEqual(script.count("maeOpenSetup("), 1)
            self.assertEqual(script.count("candidateResult = errset(progn("),
                             sum(row["candidate_generation"]["block_count"] for row in batch["candidates"]))
            self.assertTrue(all(row["candidate_generation"]["block_count"] >= 1
                                for row in batch["candidates"]))
            self.assertEqual(script.count("maeExportOutputView(?session session ?historyName"), 2)
            self.assertIn('fprintf(port "%s\\t%s\\t%s\\n"', script)
            self.assertIn('fprintf(progressPort "%s\\t%s\\t%s\\t%s\\n"', script)
            self.assertNotIn('fprintf(port "%%s', script)
        self.assertIn("maeCloseSession(?session session ?forceClose t)", script)

    def test_clone_records_are_bound_to_the_precomputed_plan(self):
        with tempfile.TemporaryDirectory() as root:
            workspace, config = self._fixture(root)
            parameter = self._parameter("p", "/X1/I0/M0", "/X1/I0", self._chain("/X1"))
            batch = prepare_persistent_batch(
                workspace, config, [parameter], [{"candidate_id": "c", "values": {"p": 2e-6}}],
                os.path.join(root, "batch"),
            )
            with open(batch["clone_records"], "w") as stream:
                for row in batch["clone_plan"]:
                    stream.write("CLONE\t%s\t%s\t%s\t%s\n" % (
                        row["occurrence_path"], row["source_cell"], row["clone_cell"], row["parent_cell"],
                    ))
            self.assertEqual(validate_batch_clones(batch)["count"], 2)

    def test_multiple_parameters_are_preassigned_and_counted(self):
        with tempfile.TemporaryDirectory() as root:
            workspace, config = self._fixture(root)
            parameters = [
                self._parameter("w", "/M0", "/", [], "w"),
                self._parameter("l", "/M0", "/", [], "l"),
            ]
            batch = prepare_persistent_batch(workspace, config, parameters,
                                             [{"candidate_id": "c", "values": {"w": 2e-6, "l": 0.2e-6}}],
                                             os.path.join(root, "batch"))
            rows = batch["candidates"][0]["patch_plan"]["parameters"]["parameters"]
            self.assertEqual([row["instance_selected_count"] for row in rows], [2, 2])
            with open(batch["script_path"]) as stream:
                script = stream.read()
            self.assertLess(script.index('existingProp = dbFindProp(inst "l")'),
                            script.index('cdfParam = get(cdfgData stringToSymbol("l"))'))
            self.assertLess(script.index('existingProp = dbFindProp(inst "w")'),
                            script.index('cdfParam = get(cdfgData stringToSymbol("l"))'))

    def test_script_escapes_candidate_paths_and_isolates_histories(self):
        with tempfile.TemporaryDirectory(prefix='cr_"slash\\') as root:
            workspace, config = self._fixture(root)
            parameter = self._parameter('p"\\', "/M0", "/", [], 'w"\\')
            batch = prepare_persistent_batch(workspace, config, [parameter], [
                {"candidate_id": 'one"\\', "values": {'p"\\': 2e-6}},
                {"candidate_id": "two", "values": {'p"\\': 7e-6}},
            ], os.path.join(root, "batch"))
            first, second = batch["candidates"]
            self.assertNotEqual(first["history_name"], second["history_name"])
            self.assertNotEqual(first["marker_path"], second["marker_path"])
            self.assertNotEqual(first["rdb_path"], second["rdb_path"])
            with open(batch["script_path"]) as stream:
                script = stream.read()
            self.assertIn('stringToSymbol("w\\"\\\\")', script)
            self.assertIn('"2u"', script)
            self.assertIn('"7u"', script)

    def test_error_marker_is_outside_errset_and_next_candidate_remains(self):
        with tempfile.TemporaryDirectory() as root:
            workspace, config = self._fixture(root)
            parameter = self._parameter("p", "/M0", "/", [])
            batch = prepare_persistent_batch(workspace, config, [parameter], [
                {"candidate_id": "first", "values": {"p": 2e-6}},
                {"candidate_id": "second", "values": {"p": 3e-6}},
            ], os.path.join(root, "batch"))
            with open(batch["script_path"]) as stream:
                script = stream.read()
            first_error = script.index('"ERROR" "candidate failed"')
            second_begin = script.index('"BEGIN" ""', first_error)
            self.assertLess(first_error, second_begin)
            for state in ("BEGIN", "PATCHED", "DONE", "ERROR"):
                self.assertIn('"%s"' % state, script)
            self.assertIn('exit()', script[first_error:second_begin])

    def test_validation_merges_effective_parameters_for_final_netlist_check(self):
        with tempfile.TemporaryDirectory() as root:
            workspace, config = self._fixture(root)
            parameter = self._parameter("p", "/M0", "/", [])
            batch = prepare_persistent_batch(workspace, config, [parameter],
                                             [{"candidate_id": "c", "values": {"p": 2e-6}}],
                                             os.path.join(root, "batch"))
            candidate = batch["candidates"][0]
            plans = candidate["patch_plan"]
            row = plans["parameters"]["parameters"][0]
            with open(plans["baseline"]["verification_path"], "w") as stream:
                stream.write('BASELINE_DIRECT\tp\tD\tTOP\tM0\tw\t"1u"\t"1u"\n')
                stream.write('BASELINE\tp\twf\t"1u"\n')
            self.assertTrue(plans["parameters"]["callback_value_proof_required"])
            with open(plans["parameters"]["verification_path"], "w") as stream:
                stream.write('CDF_CALLBACK_EFFECTIVE\tp\t"2u"\t"2u"\tt\t"2u"\tnil\tnil\n')
                stream.write('PARAM\tp\t"2u"\t"2u"\t"2u"\t"executed"\t"cb"\n')
            with open(plans["verify"]["verification_path"], "w") as stream:
                stream.write('POST_DIRECT\tp\tD\tTOP\tM0\tw\t"2u"\t"2u"\n')
                stream.write('POST\tp\twf\t"4u"\n')
            result = validate_candidate_records(candidate)
            merged = result["schematic_verification"]["parameters"][0]
            self.assertEqual(merged["netlist_parameters_before"], {"wf": ["1u"]})
            self.assertEqual(merged["netlist_parameters"], {"wf": ["4u"]})
            self.assertEqual(merged["instance_selected_count"], row["instance_selected_count"])
            self.assertEqual(merged["callback_value_proof"]["status"], "verified")
            with open(plans["parameters"]["verification_path"], "w") as stream:
                stream.write('PARAM\tp\t"2u"\t"2u"\t"2u"\t"executed"\t"cb"\n')
            with self.assertRaisesRegex(RuntimeError, "callback value proof is missing"):
                validate_candidate_records(candidate)

    def test_run_reuses_wrapper_timeout_and_returns_candidate_protocol(self):
        with tempfile.TemporaryDirectory() as root:
            workspace, config = self._fixture(root)
            parameter = self._parameter("p", "/M0", "/", [])
            batch = prepare_persistent_batch(workspace, config, [parameter],
                                             [{"candidate_id": "c", "values": {"p": 2e-6}}],
                                             os.path.join(root, "batch"))
            with mock.patch("crescendo_engine.persistent_maestro.run_command", return_value={
                    "returncode": 0, "timed_out": False}) as run:
                result = run_persistent_batch(workspace, config, batch, candidate_timeout_seconds=42)
            self.assertEqual(run.call_args[1]["timeout_seconds"], 47)
            self.assertEqual(run.call_args[1]["progress_path"], batch["progress_path"])
            self.assertEqual(run.call_args[1]["ready_path"], batch["progress_path"])
            self.assertEqual(run.call_args[1]["progress_timeout_seconds"], 42)
            self.assertEqual(run.call_args[1]["progress_initial_timeout_seconds"], 5)
            self.assertEqual(run.call_args[1]["env"]["CRESCENDO_SIMULATION_TIMEOUT_SECONDS"], "9")
            self.assertEqual(
                run.call_args[1]["env"]["CRESCENDO_SPECTRE_RETAIN_POINTER"],
                batch["spectre_retain_pointer"],
            )
            with open(batch["script_path"]) as stream:
                batch_script = stream.read()
            self.assertIn(batch["spectre_retain_pointer"], batch_script)
            self.assertIn('fprintf(retainPort "%s\\n"', batch_script)
            self.assertTrue(run.call_args[1]["env"]["CRESCENDO_SPECTRE_SUBMIT_SCRIPT"].endswith(
                os.path.join("tools", "submit_spectre_job.sh")))
            self.assertEqual(result["spectre_submit_script"], run.call_args[1]["env"]["CRESCENDO_SPECTRE_SUBMIT_SCRIPT"])
            self.assertEqual(result["candidates"][0]["history_name"], batch["candidates"][0]["history_name"])
            self.assertIn("patch_plan", result["candidates"][0])

    def test_long_lived_worker_reuses_one_ocean_for_multiple_candidates(self):
        with tempfile.TemporaryDirectory() as root:
            worker, factory, _clock = self._worker(root)
            worker.start()
            self.assertTrue(worker.is_alive())
            self.assertEqual(len(factory.calls), 1)
            popen_kwargs = factory.calls[0][1]
            self.assertIsNot(popen_kwargs["stdout"], subprocess.PIPE)
            self.assertIsNot(popen_kwargs["stderr"], subprocess.PIPE)
            self.assertEqual(popen_kwargs["preexec_fn"], os.setsid)

            first = worker.evaluate("first", {"p": 2e-6}, os.path.join(root, "candidates", "first"))
            with open(worker.spectre_retain_pointer) as stream:
                self.assertEqual(
                    stream.read().strip(),
                    os.path.abspath(os.path.join(root, "candidates", "first")),
                )
            self.assertEqual(first["states"], list(PersistentMaestroWorker.CANDIDATE_STATES))
            self.assertEqual(worker.state, "awaiting_cleanup")
            with open(first["script_path"], "r") as stream:
                candidate_script = stream.read()
            self.assertNotIn("axlSetHistoryName", candidate_script)
            self.assertIn("actualHistoryName = axlGetHistoryName(runData)", candidate_script)
            self.assertNotIn("maeExportOutputView", candidate_script)
            self.assertIn('"RESULTS_READY"', candidate_script)
            self.assertIn("crescExpectedHistoryName = actualHistoryName", candidate_script)
            self.assertIn("Crescendo native history overwrite did not reuse", candidate_script)
            with open(os.path.join(worker.control_root, "bootstrap.il"), "r") as stream:
                bootstrap_script = stream.read()
            self.assertIn("crescWorkspaceStartupLoaded", bootstrap_script)
            self.assertIn(
                'load("%s")' % os.path.join(worker.workspace["root"], ".cdsinit"),
                bootstrap_script,
            )
            self.assertLess(
                bootstrap_script.index("crescWorkspaceStartupLoaded"),
                bootstrap_script.index("maeOpenSetup"),
            )
            self.assertIn(
                'envSetVal("adexl.simulation" "saveLastNHistoryEntries" \'int 1)',
                bootstrap_script,
            )
            self.assertIn("axlSetOverwriteHistory(activeSetup t)", bootstrap_script)
            self.assertIn('crescOverwriteHistoryName = "Next History Run"', bootstrap_script)
            self.assertIn(
                "axlSetOverwriteHistoryName(activeSetup crescOverwriteHistoryName)",
                bootstrap_script,
            )

            cleanup = worker.request_cleanup(first)
            self.assertTrue(cleanup["ok"])
            with open(cleanup["script_path"], "r") as stream:
                cleanup_script = stream.read()
            self.assertIn("historySDB = axlGetHistoryEntry(mainSDB crescLastHistoryName)", cleanup_script)
            self.assertIn("axlRemoveSimulationResults(historySDB)", cleanup_script)
            second = worker.evaluate("second", {"p": 3e-6}, os.path.join(root, "candidates", "second"))
            self.assertEqual(second["protocol_identity"]["sequence"], 2)
            self.assertEqual(first["history_name"], second["history_name"])
            worker.request_cleanup(second)
            result = worker.close()
            self.assertEqual(result["state"], "closed")
            self.assertEqual(len(factory.calls), 1)
            self.assertEqual(len(factory.processes[0].stdin.commands), 6)

    def test_real_guardian_pipe_reuses_fake_ocean_and_proves_group_cleanup(self):
        with tempfile.TemporaryDirectory() as root:
            workspace, config = self._fixture(root)
            fake_ocean = os.path.join(root, "fake_ocean.py")
            with open(fake_ocean, "w") as stream:
                stream.write("""#!/usr/bin/env python3
from __future__ import print_function
import json
import os
import re
import sys
call = re.compile(r'crescWriteWorkerStatus\\(\"([^\"]*)\" \"([^\"]*)\" ([0-9]+) \"([^\"]*)\" \"([^\"]*)\" \"([A-Z_]+)\"')
for command in sys.stdin:
    match = re.search(r'load\\(\"([^\"]*)\"\\)', command.strip())
    if not match:
        continue
    path = match.group(1)
    with open(path) as source:
        text = source.read()
    request = call.search(text)
    if not request:
        sys.exit(11)
    marker, epoch, sequence, nonce, candidate, first_state = request.groups()
    if os.path.basename(path) == 'bootstrap.il':
        with open(os.path.join(os.path.dirname(path), 'worker_manifest.json')) as manifest_stream:
            manifest = json.load(manifest_stream)
        open(manifest['clone_records'], 'w').close()
        with open(manifest['native_corner_schedule_path'], 'w') as corner_stream:
            corner_stream.write('NOMINAL\\t1\\nNOMINAL_TEST\\t"T"\\t1\\nREAD_OK\\n')
        with open(manifest['pristine_records'], 'w') as pristine_stream:
            for row in manifest['pristine_cells']:
                pristine_stream.write('PRISTINE\\t%s\\t%s\\t%s\\t%s\\n' % (
                    row['library'], row['cell'], row['pristine_library'], row['pristine_cell'],
                ))
        states = ('STARTING', 'READY')
    elif os.path.basename(path) == 'persistent_worker_candidate.il':
        with open(os.path.join(os.path.dirname(path), 'persistent_worker_candidate.json')) as candidate_stream:
            candidate_manifest = json.load(candidate_stream)
        with open(candidate_manifest['pristine_restore_path'], 'w') as pristine_stream:
            for row in candidate_manifest['pristine_cells']:
                pristine_stream.write('RESTORED\\t%s\\t%s\\t%s\\t%s\\n' % (
                    row['library'], row['cell'], row['pristine_library'], row['pristine_cell'],
                ))
        states = ('ACCEPTED', 'BEGIN', 'PATCHED', 'SIM_DONE', 'RESULTS_READY', 'DONE')
    elif os.path.basename(path) == 'persistent_worker_cleanup.il':
        states = ('CLEANUP_BEGIN', 'CLEANUP_DONE')
    elif os.path.basename(path) == 'shutdown.il':
        states = ('SHUTDOWN_BEGIN', 'SHUTDOWN_DONE')
    else:
        sys.exit(12)
    with open(marker, 'a') as output:
        for state in states:
            message = 'Interactive.fixed' if state in ('RESULTS_READY', 'DONE') else ''
            output.write('%s\\t%s\\t%s\\t%s\\t%s\\tfake-time\\t%s\\n' % (
                epoch, sequence, nonce, candidate, state, message,
            ))
            output.flush()
            os.fsync(output.fileno())
    if os.path.basename(path) == 'shutdown.il':
        sys.exit(0)
""")
            os.chmod(fake_ocean, 0o755)
            config["cadence"]["ocean_bin"] = fake_ocean
            worker = PersistentMaestroWorker(
                workspace, config, [self._parameter("p", "/M0", "/", [])], root,
                worker_index=4, epoch="real-guardian-epoch", poll_interval=0.02,
            )
            try:
                worker.start(timeout_seconds=5)
                guardian_pid = worker.process.pid
                for candidate_id, value in (("first", 2e-6), ("second", 3e-6)):
                    artifact = worker.evaluate(
                        candidate_id, {"p": value}, os.path.join(root, "candidates", candidate_id),
                        timeout_seconds=5,
                    )
                    self.assertEqual(artifact["states"], list(worker.CANDIDATE_STATES))
                    self.assertTrue(worker.request_cleanup(artifact, timeout_seconds=5)["ok"])
                result = worker.close(timeout_seconds=5)
                self.assertEqual(result["returncode"], 0)
                with open(worker.guardian_state_path) as stream:
                    guardian = json.load(stream)
                self.assertEqual(guardian["guardian_pid"], guardian_pid)
                self.assertEqual(guardian["token"], worker.guardian_token)
                self.assertEqual(guardian["status"], "exited")
                self.assertTrue(guardian["group_cleanup_complete"])
                self.assertTrue(guardian["descendant_cleanup_complete"])
            finally:
                if worker.is_alive():
                    worker.terminate(reason="fake guardian test cleanup", grace_seconds=1)

    def test_long_lived_worker_aba_requests_restore_before_each_value(self):
        with tempfile.TemporaryDirectory() as root:
            worker, factory, _clock = self._worker(root)
            worker.start()
            with open(os.path.join(worker.control_root, "bootstrap.il")) as stream:
                bootstrap = stream.read()
            self.assertIn("procedure(crescPristineCellSignature(cv)", bootstrap)
            self.assertLess(
                bootstrap.index("procedure(crescPristineCellSignature(cv)"),
                bootstrap.index("Crescendo failed to capture pristine cellview"),
            )
            self.assertIn(
                "length(setof(signatureTerminal cv~>terminals signatureTerminal~>net == net))",
                bootstrap,
            )
            self.assertNotIn("CELLVIEW_PROPERTY", bootstrap)
            self.assertLess(
                bootstrap.index("dbClose(dstCv)"),
                bootstrap.index("Crescendo cannot reopen persisted pristine cellview"),
            )
            self.assertLess(
                bootstrap.index("Crescendo persisted pristine capture semantic mismatch"),
                bootstrap.index('fprintf(crescPristineVerify "PRISTINE'),
            )
            artifacts = []
            for candidate_id, value in (("a-first", 2e-6), ("b", 7e-6), ("a-last", 2e-6)):
                artifact = worker.evaluate(
                    candidate_id, {"p": value}, os.path.join(root, "candidates", candidate_id),
                )
                artifacts.append(artifact)
                self.assertEqual(worker.state, "awaiting_cleanup")
                self.assertTrue(worker.request_cleanup(artifact)["ok"])
                self.assertEqual(worker.state, "ready")
            self.assertEqual(len(factory.calls), 1)
            self.assertEqual(
                [artifact["protocol_identity"]["sequence"] for artifact in artifacts], [1, 2, 3],
            )
            self.assertEqual(len(set(
                artifact["protocol_identity"]["nonce"] for artifact in artifacts
            )), 3)
            self.assertEqual(len(set(artifact["history_name"] for artifact in artifacts)), 1)
            with open(artifacts[0]["script_path"]) as stream:
                first_script = stream.read()
            with open(artifacts[1]["script_path"]) as stream:
                middle_script = stream.read()
            with open(artifacts[2]["script_path"]) as stream:
                last_script = stream.read()
            for script in (first_script, middle_script, last_script):
                self.assertIn("dbFindOpenCellView", script)
                self.assertIn("dbPurge(workingCv)", script)
                self.assertIn("dbCopyCellView(pristineCv", script)
                self.assertIn("nil nil t", script)
                self.assertIn("crescPristineCellSignature(pristineCv)", script)
                self.assertIn("crescPristineCellSignature(restoredCv)", script)
                self.assertIn("pristine restore semantic mismatch", script)
                self.assertIn("Crescendo cannot reopen persisted restored cellview", script)
                self.assertIn("Crescendo persisted pristine restore semantic mismatch", script)
                self.assertNotIn("schCheck(restoredCv)", script)
                self.assertNotIn("dbSave(restoredCv)", script)
                self.assertIn("maeRunSimulation(?waitUntilDone t", script)
                self.assertNotIn("maeWaitUntilDone('All ?session session)", script)
                self.assertLess(
                    script.index("dbClose(restoredCv)"),
                    script.index("Crescendo cannot reopen persisted restored cellview"),
                )
                self.assertLess(
                    script.index("Crescendo persisted pristine restore semantic mismatch"),
                    script.index("BASELINE_DIRECT"),
                )
                self.assertLess(script.index("dbCopyCellView(pristineCv"),
                                script.index("BASELINE_DIRECT"))
            self.assertIn('"2u"', first_script)
            self.assertIn('"7u"', middle_script)
            self.assertIn('"2u"', last_script)
            self.assertNotIn('"7u"', last_script)
            worker.close()

    def test_pristine_restore_records_fail_closed_on_missing_or_reordered_cells(self):
        with tempfile.TemporaryDirectory() as root:
            plan = [
                {"library": "D", "cell": "A", "pristine_library": "D",
                 "pristine_cell": "cr_pristine_a"},
                {"library": "D", "cell": "B", "pristine_library": "D",
                 "pristine_cell": "cr_pristine_b"},
            ]
            path = os.path.join(root, "restore.records")
            with self.assertRaises(Exception):
                _validate_pristine_records(plan, path, "RESTORED")
            with open(path, "w") as stream:
                stream.write("RESTORED\tD\tB\tD\tcr_pristine_b\n")
                stream.write("RESTORED\tD\tA\tD\tcr_pristine_a\n")
            with self.assertRaises(Exception):
                _validate_pristine_records(plan, path, "RESTORED")
            with open(path, "w") as stream:
                stream.write("RESTORED\tD\tA\tD\tcr_pristine_a\n")
                stream.write("RESTORED\tD\tB\tD\tcr_pristine_b\n")
            self.assertTrue(_validate_pristine_records(plan, path, "RESTORED")["ok"])

    def test_long_lived_worker_fences_stale_epoch_and_nonce_markers(self):
        with tempfile.TemporaryDirectory() as root:
            worker, factory, _clock = self._worker(root)
            worker.start()
            factory.mode = "stale_then_success"
            candidate = worker.evaluate("fenced", {"p": 4e-6}, os.path.join(root, "candidates", "fenced"))
            self.assertEqual(candidate["states"], list(PersistentMaestroWorker.CANDIDATE_STATES))
            with open(candidate["marker_path"], "r") as stream:
                marker = stream.read()
            self.assertIn("old-epoch", marker)
            self.assertIn("wrong-nonce", marker)
            self.assertIn("wrong-candidate", marker)
            worker.request_cleanup(candidate)
            worker.close()

    def test_candidate_error_taints_epoch_and_blocks_reuse(self):
        with tempfile.TemporaryDirectory() as root:
            worker, factory, _clock = self._worker(root)
            worker.start()
            factory.mode = "candidate_error"
            candidate = worker.evaluate("bad", {"p": 4e-6}, os.path.join(root, "candidates", "bad"))
            self.assertEqual(candidate["states"], ["ACCEPTED", "BEGIN", "ERROR"])
            self.assertTrue(worker.tainted)
            with self.assertRaises(PersistentMaestroWorkerTainted):
                worker.evaluate("next", {"p": 5e-6}, os.path.join(root, "candidates", "next"))
            worker.close()

    def test_candidate_progress_timeout_terminates_private_process_group(self):
        with tempfile.TemporaryDirectory() as root:
            worker, factory, _clock = self._worker(root)
            worker.start()
            factory.mode = "candidate_timeout"
            with self.assertRaises(PersistentMaestroWorkerTimeout):
                worker.evaluate(
                    "hung", {"p": 4e-6}, os.path.join(root, "candidates", "hung"),
                    timeout_seconds=0.5,
                )
            self.assertTrue(worker.closed)
            self.assertIn((factory.processes[0].pid, signal.SIGTERM), factory.signals)

    def test_candidate_total_wall_deadline_is_not_reset_by_fenced_progress(self):
        with tempfile.TemporaryDirectory() as root:
            worker, factory, clock = self._worker(root)
            worker.start()
            states = ["ACCEPTED", "BEGIN", "PATCHED", "SIM_DONE", "RESULTS_READY"]
            calls = {"count": 0}

            def advancing_progress(_path, _identity):
                calls["count"] += 1
                return [{"state": state} for state in states[:calls["count"]]]

            with mock.patch(
                    "crescendo_engine.persistent_maestro._matching_worker_protocol",
                    side_effect=advancing_progress):
                with self.assertRaisesRegex(
                        PersistentMaestroWorkerTimeout,
                        "non-resettable total wall-clock deadline of 0.250 seconds"):
                    worker.evaluate(
                        "progressing", {"p": 4e-6}, os.path.join(root, "candidates", "progressing"),
                        timeout_seconds=0.25,
                    )
            self.assertGreaterEqual(calls["count"], 4)
            self.assertTrue(worker.closed)
            self.assertIn((factory.processes[0].pid, signal.SIGTERM), factory.signals)

    def test_candidate_no_progress_watchdog_remains_independent(self):
        with tempfile.TemporaryDirectory() as root:
            worker, factory, _clock = self._worker(root)
            worker.config["runtime"]["candidate_no_progress_timeout_seconds"] = 0.2
            worker.start()
            factory.mode = "candidate_timeout"
            with self.assertRaisesRegex(
                    PersistentMaestroWorkerTimeout,
                    "made no fenced progress for 0.200 seconds"):
                worker.evaluate(
                    "stalled", {"p": 4e-6}, os.path.join(root, "candidates", "stalled"),
                    timeout_seconds=1.0,
                )
            self.assertTrue(worker.closed)

    def test_candidate_deadline_covers_local_patch_request_preparation(self):
        with tempfile.TemporaryDirectory() as root:
            worker, factory, clock = self._worker(root)
            worker.start()
            original = worker._candidate_artifact

            def slow_artifact(*args, **kwargs):
                artifact = original(*args, **kwargs)
                clock.sleep(0.6)
                return artifact

            with mock.patch.object(worker, "_candidate_artifact", side_effect=slow_artifact):
                with self.assertRaisesRegex(
                        PersistentMaestroWorkerTimeout,
                        "during candidate patch and request preparation"):
                    worker.evaluate(
                        "slow-patch", {"p": 4e-6}, os.path.join(root, "candidates", "slow-patch"),
                        timeout_seconds=0.5,
                    )
            candidate_loads = [
                path for path in factory.loaded_paths
                if os.path.basename(path) == "persistent_worker_candidate.il"
            ]
            self.assertEqual(candidate_loads, [])
            self.assertTrue(worker.closed)

    def test_cleanup_has_independent_finite_deadline(self):
        with tempfile.TemporaryDirectory() as root:
            worker, factory, clock = self._worker(root)
            try:
                worker.start()
                candidate = worker.evaluate(
                    "cleanup-deadline", {"p": 4e-6},
                    os.path.join(root, "candidates", "cleanup-deadline"), timeout_seconds=0.5,
                )
                clock.sleep(0.6)
                result = worker.request_cleanup(candidate, timeout_seconds=10.0)
                self.assertTrue(result["ok"])
                cleanup_loads = [
                    path for path in factory.loaded_paths
                    if os.path.basename(path) == "persistent_worker_cleanup.il"
                ]
                self.assertEqual(len(cleanup_loads), 1)
                self.assertFalse(worker.closed)
            finally:
                worker.close()

    def test_cleanup_timeout_terminates_the_session_before_replacement(self):
        with tempfile.TemporaryDirectory() as root:
            worker, factory, _clock = self._worker(root)
            worker.start()
            candidate = worker.evaluate(
                "cleanup-timeout", {"p": 4e-6},
                os.path.join(root, "candidates", "cleanup-timeout"), timeout_seconds=1.0,
            )
            factory.mode = "cleanup_timeout"
            with self.assertRaises(PersistentMaestroWorkerTimeout):
                worker.request_cleanup(candidate, timeout_seconds=0.2)
            self.assertTrue(worker.closed)
            self.assertIn((factory.processes[0].pid, signal.SIGTERM), factory.signals)

    def test_candidate_process_death_is_detected_without_stdout_parsing(self):
        with tempfile.TemporaryDirectory() as root:
            worker, factory, _clock = self._worker(root)
            worker.start()
            factory.mode = "candidate_death"
            with self.assertRaises(PersistentMaestroWorkerProcessError):
                worker.evaluate("dead", {"p": 4e-6}, os.path.join(root, "candidates", "dead"))
            self.assertTrue(worker.closed)
            self.assertEqual(factory.processes[0].returncode, 17)

    def test_ready_timeout_is_startup_failure_and_terminates_worker(self):
        with tempfile.TemporaryDirectory() as root:
            worker, factory, _clock = self._worker(root, mode="startup_timeout")
            with self.assertRaises(PersistentMaestroWorkerStartupError):
                worker.start(timeout_seconds=0.5)
            self.assertTrue(worker.closed)
            self.assertIn((factory.processes[0].pid, signal.SIGTERM), factory.signals)

    def test_bootstrap_error_is_reported_without_waiting_for_startup_timeout(self):
        with tempfile.TemporaryDirectory() as root:
            worker, _factory, clock = self._worker(root, mode="startup_error")
            with self.assertRaisesRegex(PersistentMaestroWorkerStartupError, "bootstrap failed"):
                worker.start(timeout_seconds=5.0)
            self.assertEqual(clock.value, 0.0)
            self.assertTrue(worker.closed)

    def test_invalid_candidate_protocol_terminates_epoch(self):
        with tempfile.TemporaryDirectory() as root:
            worker, factory, _clock = self._worker(root)
            worker.start()
            factory.mode = "invalid_protocol"
            with self.assertRaises(PersistentMaestroWorkerProtocolError):
                worker.evaluate("invalid", {"p": 4e-6}, os.path.join(root, "candidates", "invalid"))
            self.assertTrue(worker.closed)

    def test_cleanup_error_taints_epoch_and_requires_replacement(self):
        with tempfile.TemporaryDirectory() as root:
            worker, factory, _clock = self._worker(root)
            worker.start()
            candidate = worker.evaluate("cleanup", {"p": 4e-6}, os.path.join(root, "candidates", "cleanup"))
            factory.mode = "cleanup_error"
            result = worker.request_cleanup(candidate)
            self.assertFalse(result["ok"])
            self.assertTrue(worker.tainted)
            with self.assertRaises(PersistentMaestroWorkerTainted):
                worker.evaluate("next", {"p": 5e-6}, os.path.join(root, "candidates", "next"))
            worker.close()

    def test_cleanup_ack_follows_result_removal_and_transient_reference_release(self):
        with tempfile.TemporaryDirectory() as root:
            worker, _factory, _clock = self._worker(root)
            worker.start()
            candidate = worker.evaluate("cleanup", {"p": 4e-6}, os.path.join(root, "candidate"))
            result = worker.request_cleanup(candidate)
            with open(result["script_path"]) as stream:
                script = stream.read()
            self.assertLess(script.index('"CLEANUP_BEGIN"'), script.index("Crescendo cleanup epoch mismatch"))
            removal = script.index("axlRemoveSimulationResults(historySDB)")
            self.assertGreater(script.index("unless(removalResult"), removal)
            barrier = script.index("maeWaitUntilDone('All ?session session)")
            self.assertGreater(barrier, removal)
            for assignment in (
                    "runData = nil", "runId = nil", "historySDB = nil", "candidateResult = nil",
                    "cdfgData = nil", "snapshotCdfParam = nil", "callbackResult = nil",
                    "crescCallbackEffectiveValue = nil", "crescCallbackFreshCdf = nil",
                    "crescCallbackDefaultValue = nil", "crescCallbackDefaultPresent = nil",
                    "crescCallbackFreshParam = nil", "crescCallbackOaProp = nil"):
                self.assertGreater(script.index(assignment), barrier)
            self.assertGreater(script.index("gc()"), script.index("cleanupResult = nil"))
            self.assertGreater(script.index('"CLEANUP_DONE"'), script.index("candidateResult = nil"))
            self.assertGreater(script.index('"CLEANUP_DONE"'), script.index("gc()"))
            worker.close()

    def test_close_is_idempotent_and_closes_parent_stdin(self):
        with tempfile.TemporaryDirectory() as root:
            worker, factory, _clock = self._worker(root)
            worker.start()
            first = worker.close()
            command_count = len(factory.processes[0].stdin.commands)
            second = worker.close()
            self.assertEqual(first["state"], "closed")
            self.assertEqual(second["state"], "closed")
            self.assertEqual(len(factory.processes[0].stdin.commands), command_count)
            self.assertTrue(factory.processes[0].stdin.closed)

    def test_guardian_outer_wait_exceeds_inner_group_cleanup_grace(self):
        with tempfile.TemporaryDirectory() as root:
            worker, factory, _clock = self._worker(root)
            worker.start()
            worker._use_guardian = True
            with mock.patch.object(worker, "_require_guardian_group_cleanup", return_value={}):
                worker.terminate(reason="test guardian handoff", grace_seconds=0.1)
            self.assertGreaterEqual(
                factory.processes[0].wait_timeouts[0],
                worker._guardian_terminate_grace_seconds
                + worker._guardian_termination_margin_seconds,
            )
            self.assertTrue(factory.processes[0].stdin.closed)

    def test_dead_guardian_accepts_only_persisted_scoped_recovery_proof(self):
        with tempfile.TemporaryDirectory() as root:
            worker, factory, _clock = self._worker(root)
            worker.start()
            worker._use_guardian = True
            process = factory.processes[0]
            process.returncode = -signal.SIGKILL
            running = {
                "token": worker.guardian_token,
                "guardian_pid": process.pid,
                "cwd": worker.workspace["root"],
                "status": "running",
                "group_cleanup_complete": False,
                "remaining_group_pids": [51001],
            }
            with open(worker.guardian_state_path, "w") as stream:
                json.dump(running, stream)

            def recover(*_args, **_kwargs):
                persisted = dict(running)
                persisted.update({
                    "status": "exited",
                    "group_cleanup_complete": True,
                    "remaining_group_pids": [],
                    "orphan_recovery": {"schema_version": "synthetic-validated-proof"},
                })
                with open(worker.guardian_state_path, "w") as stream:
                    json.dump(persisted, stream)
                return {
                    "state": "dead",
                    "termination_proof": {
                        "ok": True,
                        "kind": "orphaned_ocean_group_recovered",
                    },
                }

            with mock.patch(
                    "crescendo_engine.persistent_maestro.quiesce_legacy_guardian_attempt",
                    side_effect=recover) as quiesce:
                result = worker.terminate(reason="guardian SIGKILL recovery", grace_seconds=0.1)
            self.assertEqual(result, -signal.SIGKILL)
            self.assertTrue(worker.closed)
            self.assertEqual(worker.state, "terminated")
            quiesce.assert_called_once()
            args, kwargs = quiesce.call_args
            self.assertEqual(args, (worker.control_root, worker.workspace["root"]))
            self.assertTrue(kwargs["terminate_owned"])
            self.assertEqual(
                kwargs["expected_token_identity"], worker.guardian_token_identity,
            )

    def test_live_guardian_never_enters_orphan_recovery(self):
        with tempfile.TemporaryDirectory() as root:
            worker, _factory, _clock = self._worker(root)
            worker.start()
            worker._use_guardian = True
            with open(worker.guardian_state_path, "w") as stream:
                json.dump({
                    "token": worker.guardian_token,
                    "guardian_pid": worker.process.pid,
                    "cwd": worker.workspace["root"],
                    "status": "running",
                    "group_cleanup_complete": False,
                    "remaining_group_pids": [51002],
                }, stream)
            with mock.patch(
                    "crescendo_engine.persistent_maestro.quiesce_legacy_guardian_attempt") as quiesce:
                with self.assertRaisesRegex(
                        PersistentMaestroWorkerProcessError,
                        "did not prove Ocean group cleanup"):
                    worker._require_guardian_group_cleanup()
            quiesce.assert_not_called()
            worker._use_guardian = False
            worker.close()

    def test_ambiguous_dead_guardian_recovery_remains_unsafe(self):
        with tempfile.TemporaryDirectory() as root:
            worker, factory, _clock = self._worker(root)
            worker.start()
            worker._use_guardian = True
            process = factory.processes[0]
            process.returncode = -signal.SIGKILL
            with open(worker.guardian_state_path, "w") as stream:
                json.dump({
                    "token": worker.guardian_token,
                    "guardian_pid": process.pid,
                    "cwd": worker.workspace["root"],
                    "status": "running",
                    "group_cleanup_complete": False,
                    "remaining_group_pids": [51003],
                }, stream)
            ambiguous = {
                "state": "ambiguous",
                "reason": "detached token-owned process",
                "processes": [{
                    "kind": "scoped_token_helper_identity_conflict",
                    "pid": 51003,
                    "start_ticks": 91234,
                    "ppid": process.pid,
                    "pgrp": 51003,
                    "session": 51003,
                    "uid": os.getuid(),
                    "cwd": worker.workspace["root"],
                    "argv": ["/opt/cadence/helper"],
                    "launch_token": worker.guardian_token,
                }],
                "termination_proof": {
                    "ok": False,
                    "kind": "orphan_recovery_process_conflict",
                },
            }
            with mock.patch(
                    "crescendo_engine.persistent_maestro.quiesce_legacy_guardian_attempt",
                    return_value=ambiguous) as quiesce:
                with self.assertRaisesRegex(
                        PersistentMaestroWorkerProcessError,
                        "orphan_recovery_process_conflict.*launch_token.*start_ticks"):
                    worker.terminate(reason="ambiguous guardian loss", grace_seconds=0.1)
            quiesce.assert_called_once()
            self.assertFalse(worker.closed)
            self.assertTrue(worker.tainted)
            self.assertEqual(worker.state, "termination_failed")
            worker._close_stdin()
            worker._close_streams()

    def test_recovery_result_without_persisted_proof_remains_unsafe(self):
        with tempfile.TemporaryDirectory() as root:
            worker, factory, _clock = self._worker(root)
            worker.start()
            worker._use_guardian = True
            process = factory.processes[0]
            process.returncode = -signal.SIGKILL
            with open(worker.guardian_state_path, "w") as stream:
                json.dump({
                    "token": worker.guardian_token,
                    "guardian_pid": process.pid,
                    "cwd": worker.workspace["root"],
                    "status": "running",
                    "group_cleanup_complete": False,
                    "remaining_group_pids": [51004],
                }, stream)
            recovered = {
                "state": "dead",
                "termination_proof": {
                    "ok": True,
                    "kind": "orphaned_ocean_group_recovered",
                },
            }
            with mock.patch(
                    "crescendo_engine.persistent_maestro.quiesce_legacy_guardian_attempt",
                    return_value=recovered):
                with self.assertRaisesRegex(
                        PersistentMaestroWorkerProcessError,
                        "recovery proof was not persisted"):
                    worker.terminate(reason="unpersisted guardian recovery", grace_seconds=0.1)
            self.assertFalse(worker.closed)
            self.assertTrue(worker.tainted)
            self.assertEqual(worker.state, "termination_failed")
            worker._close_stdin()
            worker._close_streams()

    def test_missing_guardian_cleanup_proof_keeps_epoch_unsafe(self):
        with tempfile.TemporaryDirectory() as root:
            worker, _factory, _clock = self._worker(root)
            worker.start()
            worker._use_guardian = True
            with mock.patch.object(
                    worker, "_require_guardian_group_cleanup",
                    side_effect=PersistentMaestroWorkerProcessError("cleanup proof missing")):
                with self.assertRaises(PersistentMaestroWorkerProcessError):
                    worker.terminate(reason="guardian proof failure", grace_seconds=0.1)
            self.assertFalse(worker.closed)
            self.assertTrue(worker.tainted)
            self.assertEqual(worker.state, "termination_failed")

    def test_close_after_guardian_exit_requires_cleanup_proof(self):
        with tempfile.TemporaryDirectory() as root:
            worker, factory, _clock = self._worker(root)
            worker.start()
            worker._use_guardian = True
            factory.processes[0].returncode = 17
            with mock.patch.object(
                    worker, "_require_guardian_group_cleanup",
                    side_effect=PersistentMaestroWorkerProcessError("cleanup proof missing")):
                with self.assertRaises(PersistentMaestroWorkerProcessError):
                    worker.close()
            self.assertFalse(worker.closed)
            self.assertTrue(worker.tainted)
            self.assertEqual(worker.state, "termination_failed")

    def test_unreapable_supervisor_is_not_reported_closed(self):
        with tempfile.TemporaryDirectory() as root:
            worker, factory, _clock = self._worker(root, mode="unreapable")
            worker.start()
            with self.assertRaises(PersistentMaestroWorkerProcessError):
                worker.terminate(reason="cannot reap", grace_seconds=0.1)
            self.assertFalse(worker.closed)
            self.assertTrue(worker.tainted)
            self.assertEqual(worker.state, "termination_failed")
            self.assertTrue(worker.is_alive())
            self.assertIn((factory.processes[0].pid, signal.SIGTERM), factory.signals)
            self.assertIn((factory.processes[0].pid, signal.SIGKILL), factory.signals)

    def test_two_workers_keep_process_workspace_marker_and_history_isolated(self):
        with tempfile.TemporaryDirectory() as root:
            factory = FakePopenFactory()
            clock = FakeClock()
            workers = []
            artifacts = []
            for index in (0, 1):
                worker_root = os.path.join(root, "worker-%d" % index)
                os.makedirs(worker_root)
                workspace, config = self._fixture(worker_root)
                worker = PersistentMaestroWorker(
                    workspace, config, [self._parameter("p", "/M0", "/", [])],
                    worker_root, worker_index=index, epoch="epoch-%d" % index,
                    popen_factory=factory, clock=clock.monotonic, sleeper=clock.sleep,
                    killpg=factory.killpg, getpgid=lambda pid: pid, poll_interval=0.1,
                )
                worker.start()
                artifact = worker.evaluate(
                    "candidate-%d" % index, {"p": (index + 2) * 1e-6},
                    os.path.join(worker_root, "candidate"),
                )
                workers.append(worker)
                artifacts.append(artifact)
            self.assertEqual([process.pid for process in factory.processes], [41000, 41001])
            self.assertNotEqual(workers[0].control_root, workers[1].control_root)
            self.assertNotEqual(artifacts[0]["marker_path"], artifacts[1]["marker_path"])
            self.assertNotEqual(artifacts[0]["history_name"], artifacts[1]["history_name"])
            self.assertNotEqual(artifacts[0]["requested_history_name"], artifacts[1]["requested_history_name"])
            self.assertNotEqual(artifacts[0]["rdb_path"], artifacts[1]["rdb_path"])
            for worker, artifact in zip(workers, artifacts):
                worker.request_cleanup(artifact)
                worker.close()


if __name__ == "__main__":
    unittest.main()
