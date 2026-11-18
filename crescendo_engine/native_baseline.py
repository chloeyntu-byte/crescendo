"""Owned native Maestro Device Parameters / Run / exact-history evaluator.

This protocol never installs a solver wrapper or replays a captured command.
Native session completion and guardian process cleanup are independent proofs.
An interrupted/unknown session remains unsafe, even if its local Ocean exited.
"""
from __future__ import print_function

import copy
import hashlib
import json
import os
import re
import sqlite3
import stat
import subprocess
import threading
import time
import uuid

from crescendo_engine.runtime import bind_text
_contract_text = bind_text()

from .design_variables import native_parameter
from .native_startup import STAGE, StartupObservation, settings as startup_settings
from .diagnostic_timing import timing_tick, elapsed_seconds, phase_seconds
from .utils import atomic_write_json, atomic_write_text, ensure_dir
from .workspace import (_skill_quote, _workspace_startup_load_lines,
                        bounded_skill_body_lines, CADENCE_STARTUP_LOCK)

SCHEMA_VERSION = _contract_text("crescendo.maestro_baseline.v1")
_ACTIVE = {}
_ACTIVE_LOCK = threading.Lock()


class NativeBaselineError(RuntimeError):
    pass


class NativeStartupCrash(NativeBaselineError):
    pass


class NativeBaselineTimeout(NativeBaselineError):
    pass


def _load(path):
    with open(path, encoding="utf-8") as stream:
        return json.load(stream)


def _quote(value):
    return '"' + _skill_quote(str(value)) + '"'


def _record_storage_evidence(driver):
    """Persist a read-only diagnostic binding before Run can time out.

    This does not grant completion, Stop, cleanup or recovery authority.
    Failure to write this optional index is reported in the existing state.
    """
    storage = driver.workspace.get("native_storage") or {}
    try:
        root = os.path.dirname(storage["readback_path"])
        expected_parent = os.path.join(driver.workspace["root"], "native_baseline_runs")
        if (os.path.dirname(root) != expected_parent or
                not re.fullmatch(r"[a-f0-9]{32}", os.path.basename(root)) or
                os.path.realpath(root) != root):
            raise ValueError("native diagnostic storage is not this candidate's direct working directory")
        identity = os.lstat(root)
        if not stat.S_ISDIR(identity.st_mode) or identity.st_uid != os.getuid():
            raise ValueError("native diagnostic storage identity differs")
        payload = {"schema_version": _contract_text("crescendo.native_storage_evidence.v1"),
                   "candidate_id": driver.state["candidate_id"], "request_root": driver.root,
                   "workspace_root": driver.workspace["root"], "nonce": driver.nonce,
                   "storage_root": root,
                   "storage_identity": {"dev": identity.st_dev, "ino": identity.st_ino, "uid": identity.st_uid},
                   "native_readback_verified": storage.get("configured_roots_verified") is True,
                   "created_epoch": driver.state["started_epoch"],
                   "diagnostic_only": True,
                   "readback_path": storage["readback_path"],
                   "configured_paths": {key: storage.get(key) for key in
                       ("project_dir", "save_res_dir", "save_dir", "local_psf_dir", "local_rdb_dir")}}
        path = os.path.join(driver.control, _contract_text("native_storage_evidence.json"))
        atomic_write_json(path, payload)
        driver.state["native_storage_evidence"] = {
            "path": path, "storage_root": root,
            "native_readback_verified": payload["native_readback_verified"], "diagnostic_only": True}
        driver.state.pop("diagnostic_evidence_error", None)
    except (OSError, ValueError, KeyError) as error:
        driver.state["diagnostic_evidence_error"] = "%s: %s" % (type(error).__name__, error)
    driver.persist()


def _unqualified_library_init(diagnostics):
    retained = dict(diagnostics)
    if "library_init_warnings" in retained:
        retained["library_init_candidates"] = [dict(warning, severity="UNQUALIFIED")
                                                for warning in retained.pop("library_init_warnings")]
    return retained


def _session_tool_diagnostics(driver, startup, simulator):
    from .native_diagnostics import skill_command_diagnostics, log_ocean_tool_warnings, recorded_ocean_warnings
    process = driver.process
    diagnostics = skill_command_diagnostics(
        {"returncode": getattr(process, "returncode", None)},
        startup, owned_root=driver.control, simulators=(simulator,), native_completion=True)
    warnings = recorded_ocean_warnings(diagnostics)
    if warnings or diagnostics.get("native_error"):
        try:
            atomic_write_json(os.path.join(driver.control, _contract_text("native_skill_diagnostics.json")),
                              _unqualified_library_init(diagnostics))
        except OSError as error:
            diagnostics["diagnostic_write_error"] = str(error)
        log_ocean_tool_warnings(driver.config, driver.control,
                                diagnostics.get("ocean_tool_warnings") or [])
    return diagnostics


def _read_protocol(path, nonce):
    if not os.path.isfile(path):
        return []
    if os.path.islink(path) or os.path.getsize(path) > 1024 * 1024:
        raise NativeBaselineError("Invalid Native Run protocol file")
    with open(path, encoding="utf-8") as stream:
        rows = [line.rstrip("\n").split("\t") for line in stream]
    if any(len(row) != 3 or row[0] != nonce for row in rows):
        raise NativeBaselineError("Native Run protocol identity mismatch")
    return [{"state": row[1], "value": row[2], "sequence": index} for index, row in enumerate(rows)]


def native_attempt_control(root, attempt=1):
    if type(attempt) is not int or attempt < 1:
        raise NativeBaselineError("Invalid Native Run startup attempt")
    control = os.path.join(root, "native_baseline")
    if attempt > 1:
        control = os.path.join(control, "retry-%d" % attempt)
    if os.path.realpath(control) != control:
        raise NativeBaselineError("Native Run startup attempt path is unsafe")
    return control


def _local_termination_proven(proof):
    return proof.get("state") == "dead" and proof.get("termination_proof", {}).get("ok") is True


def native_submission_absent(state, rows):
    return (state.get("submission_intent") is False and
            not any(row["state"] in ("RUN_BEGIN", "RUN_ID", "SIM_DONE", "HISTORY", "CLOSED") for row in rows))


def _new_session_state(root, workspace, candidate_id, nonce):
    return {"schema_version": SCHEMA_VERSION, "nonce": nonce,
            "candidate_id": str(candidate_id), "request_root": root, "workspace_root": workspace,
            "started_epoch": time.time(), "launch_attempted": False, "submission_intent": False,
            "native_session_closed": False, "native_policy_local": False, "cleanup_proven": False,
            "cleanup_pending": False, "native_stop": {"requested": False, "remote_stop_proven": False}}


class _NativeSession(object):
    def __init__(self, root, workspace, config, candidate_id):
        self.root, self.workspace, self.config = root, workspace, config
        self.settings = startup_settings(config.get("runtime") or {})
        self.path = os.path.join(native_attempt_control(root), _contract_text("native_baseline_state.json"))
        if os.path.lexists(self.path):
            raise NativeBaselineError("Native Run request directory already used; no replay allowed")
        self._begin_attempt(candidate_id, [])

    def _begin_attempt(self, candidate_id, crashes):
        attempt = len(crashes) + 1
        self.control = native_attempt_control(self.root, attempt)
        if attempt > 1:
            os.mkdir(self.control)
        else:
            ensure_dir(self.control)
        self.nonce = uuid.uuid4().hex
        self.protocol = os.path.join(self.control, "native_baseline_protocol.tsv")
        self.process = None
        self.state = _new_session_state(self.root, self.workspace["root"], candidate_id, self.nonce)
        self.state.update(startup_attempt=attempt, previous_startup_crashes=crashes,
                          startup_recovery_settings=dict(self.settings))
        self.persist()

    def start(self, make_scripts, timeout, cancel_check):
        while True:
            scripts = make_scripts()
            locked = CADENCE_STARTUP_LOCK.acquire(timeout=max(1, timeout))
            if not locked:
                raise NativeBaselineError("Native startup lock timed out")
            try:
                self.launch(scripts[0])
                self.wait(("READY",), timeout, cancel_check)
                return scripts
            except NativeStartupCrash as error:
                crash = {"attempt": self.state["startup_attempt"], "nonce": self.nonce,
                         "message": str(error), "detected_epoch": time.time()}
                self.state["startup_crash"] = crash
                self.persist()
            except NativeBaselineError as error:
                crashes = self.state["previous_startup_crashes"]
                if not crashes or getattr(error, "cancelled", False):
                    raise
                failure = NativeStartupCrash(crashes[0]["message"])
                failure.detail = {"startup_retry_error": str(error)}
                raise failure from error
            finally:
                CADENCE_STARTUP_LOCK.release()
            proof = self.cleanup()
            crashes = self.state["previous_startup_crashes"] + [crash]
            if (not proof.get("native_not_submitted") or self.status() or
                    len(crashes) > self.settings["native_startup_crash_retries"] or
                    (cancel_check and cancel_check())):
                error = NativeStartupCrash(crashes[0]["message"])
                error.cancelled = bool(cancel_check and cancel_check())
                raise error
            self._begin_attempt(self.state["candidate_id"], crashes)

    def persist(self):
        atomic_write_json(self.path, self.state)

    def status(self):
        return _read_protocol(self.protocol, self.nonce)

    def send(self, script):
        if self.process is None or self.process.poll() is not None:
            raise NativeBaselineError("Owned Ocean is unavailable")
        self.process.stdin.write('load(%s)\n' % _quote(script))
        self.process.stdin.flush()

    def wait(self, terminal, timeout, cancel_check=None, deadline=None):
        if deadline is None and timeout > 0:
            deadline = time.monotonic() + timeout
        startup = (StartupObservation(self.control, self.settings["native_startup_crash_quiet_seconds"])
                   if terminal == ("READY",) else None)
        while True:
            rows = self.status()
            for row in reversed(rows):
                if row["state"] in terminal:
                    return row
                if row["state"] == "STOP_ERROR" or (row["state"] == "ERROR" and "STOP_DONE" not in terminal):
                    raise NativeBaselineError("Native Run failed: " + row["value"])
            if cancel_check and cancel_check():
                error = NativeBaselineError("Native Run cancelled")
                error.cancelled = True
                raise error
            if startup:
                signature = startup.observe(rows)
                if signature and not self.status():
                    raise NativeStartupCrash("Native Ocean startup crash: " + signature)
            if (self.process is None or self.process.poll() is not None) and not (
                    startup and startup.signature and not startup.stage_seen):
                raise NativeBaselineError("Owned Ocean exited before native terminal evidence")
            if deadline is not None and time.monotonic() >= deadline:
                stage = {"READY": "Ocean startup", "SIM_DONE": "native Run execution",
                         "HISTORY": "native Wait/history readback", "CLOSED": "native session close",
                         "STOP_DONE": "native Stop"}.get(terminal[0], "native operation")
                error = NativeBaselineTimeout("%s timed out after %g seconds waiting for %s" %
                                              (stage, timeout, "/".join(terminal)))
                error.detail = {"timeout_seconds": timeout, "expected_states": list(terminal),
                                "last_protocol_state": rows[-1]["state"] if rows else None}
                raise error
            time.sleep(0.1)

    def launch(self, script):
        from .cadence import child_environment, build_guardian_command
        from .persistent_maestro import _guardian_environment
        from .legacy_guardian import (acquire_legacy_guardian_launch_lock,
            close_legacy_guardian_launch_lock, legacy_guardian_parent_identity,
            write_legacy_guardian_manifest, LAUNCH_TOKEN_ENVIRONMENT)
        # No workspace/bin prefix: a prior capture wrapper must never become
        # this mode's real solver. Only configured native binary paths are used.
        env = child_environment(self.config, self.workspace["root"])
        # Ocean's launcher otherwise reuses the Virtuoso MPS session inherited
        # through the menu IPC process. Let Ocean allocate its own session so
        # its Maestro workers belong to this owned baseline process.
        env.pop("CDS_MPS_SESSION", None)
        local_bin = os.path.realpath(os.path.join(self.workspace["root"], "bin"))
        env["PATH"] = os.pathsep.join(path for path in env.get("PATH", "").split(os.pathsep)
                                      if os.path.realpath(path or ".") != local_bin)
        configured = self.config.get("cadence", {})
        binaries = [configured.get(name) for name in ("spectre_bin", "alps_bin")]
        env["PATH"] = os.pathsep.join([os.path.dirname(path) for path in binaries if path] + [env["PATH"]])
        argv = [configured["ocean_bin"]]
        if not self.config.get("runtime", {}).get("maestro_baseline_foreground", False):
            argv.append("-nograph")
        self.state["command"] = argv
        state_path = os.path.join(self.control, _contract_text("guardian_state.json"))
        out, err = [os.path.join(self.control, name) for name in ("ocean.stdout", "ocean.stderr")]
        fd, lock_path = acquire_legacy_guardian_launch_lock(self.control)
        try:
            parent = legacy_guardian_parent_identity()
            command = build_guardian_command(argv, self.workspace["root"], out, err,
                state_path, self.nonce, guardian_launch_lock_fd=fd, poll_expected_parent=True)
            from .resources import guardian_script_path
            write_legacy_guardian_manifest(self.control, self.workspace["root"], state_path,
                out, err, self.nonce, argv, parent["pid"], guardian_script_path(),
                launch_lock_path=lock_path, expected_parent_boot_id=parent["boot_id"],
                expected_parent_start_ticks=parent["start_ticks"], expected_uid=parent["uid"],
                guardian_argv=command)
            env = _guardian_environment(env)
            env[LAUNCH_TOKEN_ENVIRONMENT] = self.nonce
            self.state.update(launch_attempted=True, cleanup_pending=True)
            self.persist()  # Intent is durable before any native process exists.
            with open(os.path.join(self.control, "guardian.console"), "ab") as console:
                self.process = subprocess.Popen(command, cwd=self.workspace["root"], env=env,
                    stdin=subprocess.PIPE, stdout=console, stderr=console, start_new_session=True,
                    universal_newlines=True, bufsize=1, close_fds=True, pass_fds=(fd,))
            self.state["guardian_pid"] = self.process.pid
            self.persist()
        finally:
            close_legacy_guardian_launch_lock(fd)
        self.send(script)

    def cleanup(self, timeout=12):
        if "local_cleanup" in self.state:
            return dict(self.state)
        if not self.state["launch_attempted"]:
            self.state.update(cleanup_proven=True, cleanup_pending=False)
            self.persist()
            return dict(self.state)
        rows = self.status()
        closed = any(row["state"] in ("CLOSED", "STOP_DONE") for row in rows)
        not_submitted = native_submission_absent(self.state, rows)
        if not closed and not_submitted is False and self.process is not None and self.process.poll() is None:
            script = os.path.join(self.control, "stop.il")
            atomic_write_text(script, '\n'.join([
                _contract_text('unless(equal(crescBaselineNonce %s) error("Crescendo reference Stop nonce mismatch\\n"))') % _quote(self.nonce),
                _contract_text('crescBaselineStopping=t'),
                _contract_text('crescBaselineRecord("STOP_BEGIN" "")'),
                _contract_text('crescBaselineStop=errset(progn('),
                ' unless(boundp(\'session) && session error("Owned session unavailable\\n"))',
                _contract_text(' crescStopAccepted=errset(maeStopSimulation(?session session) t)'),
                _contract_text(' unless(crescStopAccepted error("Native Stop API failed\\n"))'),
                _contract_text(' unless(car(crescStopAccepted) || crescBaselineDone || !crescBaselineSubmitted error("Native Stop rejected\\n"))'),
                _contract_text(" crescWaitResult=maeWaitUntilDone('All ?session session)"),
                _contract_text(' crescBaselineRecord("WAIT_RETURN" sprintf(nil "%L" crescWaitResult))'),
                ' unless(maeCloseSession(?session session ?forceClose t) error("Native session close failed\\n"))',
                ' session=nil',
                _contract_text(' crescBaselineRecord("STOP_DONE" "owned session stopped, waited and closed")'),
                ' t) t)',
                _contract_text('unless(crescBaselineStop crescBaselineRecord("STOP_ERROR" "native Stop/Wait/Close failed"))'),
                ]) + '\n')
            self.state["native_stop"]["requested"] = True
            self.persist()
            try:
                self.send(script)
                self.wait(("STOP_DONE",), timeout)
            except Exception as error:
                self.state["native_stop"]["error"] = str(error)
            rows = self.status()
            closed = any(row["state"] in ("CLOSED", "STOP_DONE") for row in rows)
        native_stopped = any(row["state"] == "STOP_DONE" for row in rows)
        self.state["native_stop"]["native_stop_wait_closed"] = native_stopped
        self.state["native_stop"]["remote_stop_proven"] = False
        self.state["native_session_closed"] = closed
        command_terminal = (_command_terminal_proof(self) if native_stopped and not self.state.get('native_policy_local')
                            else {'ok': False})
        if self.process is not None and self.process.poll() is None and closed:
            try:
                self.process.stdin.write("exit(0)\n")
                self.process.stdin.flush()
                self.process.wait(timeout=max(1, timeout))
            except (OSError, subprocess.TimeoutExpired):
                pass
        if self.process is not None and self.process.stdin is not None:
            try:
                self.process.stdin.close()  # EOF tells the owned guardian to stop and reap its descendants.
            except OSError:
                pass
        from .legacy_guardian import quiesce_legacy_guardian_attempt
        proof = quiesce_legacy_guardian_attempt(self.control,
            expected_workspace_root=self.workspace["root"],
            expected_token_identity={"legacy": True, "token": self.nonce},
            timeout_seconds=self.settings["native_cleanup_proof_timeout_seconds"],
            poll_live_identity_conflict=self.process is not None)
        self.state["local_cleanup"] = proof
        process_proven = _local_termination_proven(proof)
        not_submitted = native_submission_absent(self.state, self.status())
        self.state["native_not_submitted"] = bool(not_submitted and process_proven)
        self.state["cleanup_proven"] = bool(process_proven and (closed or not_submitted))
        if self.process is not None:
            self.process.poll()
        if native_stopped:
            never_submitted = bool(getattr(self, 'batch', None)) and not self.state.get('submission_intent') and not any(r['state'] == 'RUN_BEGIN' for r in rows)
            local_stopped = self.state["cleanup_proven"] and (self.state.get("native_policy_local") is True or never_submitted)
            remote = command_terminal
            stopped = local_stopped or (self.state['cleanup_proven'] and remote.get('ok') is True)
            self.state['native_stop']['command_terminal_proof'] = remote
            self.state["native_stop"]["remote_stop_proven"] = stopped
            self.state["native_stop"]["proof_scope"] = ("native_local_jobs_and_owned_descendants" if local_stopped else
                "exact_owned_job_terminal_queries" if stopped else "native_stop_wait_close_without_remote_job_terminal_receipts")
            if not stopped:
                self.state["cleanup_proven"] = False
        # Local termination never substitutes for native Stop or remote status.
        self.state["cleanup_pending"] = not self.state["cleanup_proven"]
        self.persist()
        return dict(self.state)


def reconcile_native_baseline(root, request_cancel=True, timeout_seconds=12):
    from .legacy_guardian import inspect_legacy_guardian_attempt

    root = os.path.abspath(root)
    with _ACTIVE_LOCK:
        active = _ACTIVE.get(root)
    if active:
        return active.cleanup(timeout_seconds) if request_cancel else dict(active.state)
    path = os.path.join(native_attempt_control(root), _contract_text("native_baseline_state.json"))
    if not os.path.exists(path):
        return {"schema_version": SCHEMA_VERSION, "cleanup_proven": False,
                "cleanup_pending": True, "reason": "native launch record missing; termination cannot be established"}
    state = _load(path)
    if state.get("schema_version") != SCHEMA_VERSION or state.get("request_root") != root:
        raise NativeBaselineError("Native Run recovery protocol mismatch")
    attempt = state.get("startup_attempt", 1)
    control = native_attempt_control(root, attempt)
    crashes = state.get("previous_startup_crashes", [])
    if not isinstance(crashes, list) or len(crashes) != attempt - 1:
        raise NativeBaselineError("Native Run startup attempt history mismatch")

    def inspect(control, nonce):
        return inspect_legacy_guardian_attempt(control,
            expected_workspace_root=state["workspace_root"],
            expected_token_identity={"legacy": True, "token": nonce})

    for index, crash in enumerate(crashes, 1):
        previous = native_attempt_control(root, index)
        if (crash.get("attempt") != index or
                not _local_termination_proven(inspect(previous, crash.get("nonce"))) or
                _read_protocol(os.path.join(previous, "native_baseline_protocol.tsv"), crash["nonce"])):
            return dict(state, cleanup_proven=False, cleanup_pending=True,
                        reason="Previous Native Run startup attempt cleanup is unproven")
    if not state.get("launch_attempted"):
        return dict(state, cleanup_proven=True, cleanup_pending=False)
    if state.get("cleanup_proven") and state.get("native_session_closed"):
        return state
    if state.get("native_not_submitted") is True and state.get("cleanup_proven") is True:
        rows = _read_protocol(os.path.join(control, "native_baseline_protocol.tsv"), state["nonce"])
        if native_submission_absent(state, rows) and _local_termination_proven(inspect(control, state["nonce"])):
            return state
    # A lost controller cannot submit Stop into an unrelated session.
    return dict(state, cleanup_proven=False, cleanup_pending=True,
                reason="Owned native terminal proof missing; preserve suite and do not replay")


def _native_policy_definitions(workspace, policy):
    """Resolve saved policy names to the requested Command settings in owned HOME."""
    from .private_netlist import _capture_policy_names
    from .job_policy import _skill_policy
    names = _capture_policy_names(workspace)
    if not names:
        return []
    properties = copy.deepcopy((policy.get("global") or {}).get("properties") or {})
    if not str(properties.get("jobsubmitcommand") or "").strip():
        raise NativeBaselineError("Command was not captured; enter a Command in Job Setup before running")
    # Resolve saved names only inside this owned HOME, before maeOpenSetup.
    # Their original definition/location is irrelevant to the requested settings.
    definitions = {name: dict(properties, name=name) for name in names}
    directory = os.path.join(workspace["root"], ".cadence")
    for path in (directory, os.path.join(directory, "jobpolicy")):
        if os.path.lexists(path) and (os.path.islink(path) or not os.path.isdir(path)):
            raise NativeBaselineError("Owned native Job Policy directory is unsafe")
        ensure_dir(path)
    lines = ['unless(isCallable(\'axlAdd' 'JobPolicy) error("Native named Job Policy API unavailable\\n"))']
    for name, properties in sorted(definitions.items()):
        target = os.path.join(directory, "jobpolicy", name + ".jp")
        if os.path.lexists(target) and (os.path.islink(target) or not os.path.isfile(target)):
            raise NativeBaselineError("Owned native Job Policy target is unsafe")
        lines += [
            'unless(axlAdd' 'JobPolicy(%s %s %s) error("Cannot preserve native named Job Policy\\n"))' %
                (_quote(name), _quote(directory), _skill_policy(properties)),
        ]
    return lines


def _native_parallelism(source, readback, spec):
    """Keep the native point policy readback separate from candidate capacity."""
    original = (source.get("global") or {}).get("properties") or {}
    actual = (readback.get("global") or {}).get("properties") or {}
    overrides = spec.get("maestro_job_overrides") or {}
    run_options = readback.get("run_options") or {}
    tests = readback.get("tests") or []
    return {
        "source": source.get("source"),
        "source_global_max_jobs": original.get("maxjobs"),
        "max_jobs_input": overrides.get("max_jobs"),
        "command_input_supplied": "command" in overrides,
        "job_setup_readback_verified": True,
        "effective_global_max_jobs": actual.get("maxjobs"),
        "distribution": actual.get("distributionmethod"),
        "run_in": run_options.get("RunIn"),
        "divide_jobs": run_options.get("DivideJobs"),
        "job_limits": (run_options.get("JobLimit") or [])[:16],
        "test_count": len(tests),
        "tests": [{"test": row["name"], "max_jobs": (row.get("properties") or {}).get("maxjobs")}
                  for row in tests[:16]],
    }


def _baseline_launch_policy(policy, wrappers):
    """Bind observers before opening the setup, including prestarted workers."""
    result = copy.deepcopy(policy)
    tests = {row.get("name"): row for row in result.get("tests") or []}
    for wrapper in wrappers or []:
        test = wrapper.get("test")
        scope = result.get("global") if test is None else tests.get(test)
        properties = (scope or {}).get("properties") or {}
        if properties.get("jobsubmitcommand") != wrapper.get("original_command"):
            raise NativeBaselineError("Native observer command identity differs before setup open")
        properties["jobsubmitcommand"] = wrapper["effective_command"]
    return result


def _scripts(driver, rows, plan, run_setup, execution_selection, corner_selection, prepared, spec):
    from .native_maestro_parameters import native_parameter_skill_lines
    from .storage_policy import (native_storage_init_lines, native_storage_capture_definitions,
                                 native_storage_readback_lines, native_history_readback_line)
    from .job_policy import job_policy_application_skill, command_job_policy, prepare_job_command_observers
    from .private_netlist import native_corner_schedule_skill
    from .test_selection import selection_skill_lines as test_lines
    from .corner_selection import selection_skill_lines as corner_lines
    from .native_variable_scope_readback import native_variable_scope_skill_lines
    from .single_test import native_selection, native_selection_lines
    from .workspace import workspace_test_design_lines
    workspace, root = driver.workspace, driver.root
    definitions = [
        _contract_text('crescBaselineNonce=%s') % _quote(driver.nonce),
        _contract_text('crescBaselineSubmitted=nil crescBaselineDone=nil crescBaselineStopping=nil session=nil'),
        _contract_text('procedure(crescBaselineRecord(state value)'),
        ' let((port) port=outfile(%s "a")' % _quote(driver.protocol),
        '  unless(port error("Cannot write native protocol\\n"))',
        _contract_text('  fprintf(port "%s\\t%s\\t%s\\n" crescBaselineNonce state value) close(port)))'),
        _contract_text('procedure(crescBaselineFinished()'),
        _contract_text(' crescBaselineDone=t'),
        _contract_text(' unless(crescBaselineStopping crescBaselineRecord("SIM_DONE" "")))'),
    ]
    runtime = driver.config.get("runtime") or {}
    policy = prepared.get("job_policy") or runtime.get("maestro_source_job_policy") or {}
    tests = [row["name"] for row in run_setup["tests"] if row.get("enabled", True)]
    policy = command_job_policy(policy, spec.get("maestro_job_overrides"), tests)
    if getattr(driver, 'batch', None):
        from .generation_batch import native_batch_policy
        policy = native_batch_policy(policy)
    # Keep scheduler HOME scoped to the actual command, and retain its native
    # argv and submission evidence. Unsupported shell commands remain native.
    driver.job_observation = prepare_job_command_observers(
        policy, {}, os.path.join(driver.control, "job_observers"),
        os.path.join(root, _contract_text("native_dispatch.jsonl")))
    launch_policy = _baseline_launch_policy(policy, driver.job_observation.get("wrappers"))
    body = _workspace_startup_load_lines(workspace)
    body += native_storage_init_lines(workspace["native_storage"])
    body += native_storage_capture_definitions().splitlines()
    body += _native_policy_definitions(workspace, launch_policy)
    if getattr(driver, "batch", None):
        body += ['ocnSetXLMode("assembler")',
                 'envSetVal("maestro.simulation" "interactiveA" \'boolean nil)',
                 'envSetVal("maestro.simulation" "interactiveE" \'boolean nil)']
    body += [
        'sstatus(errsetTrace nil)',
        'envSetVal("adexl.historyNamePrefix" "showNameHistoryForm" \'boolean nil)',
        'envSetVal("adexl.simulation" "saveLastNHistoryEntries" \'int 1000)',
        'session=maeOpenSetup(%s %s %s ?mode "a")' % tuple(_quote(workspace[key]) for key in ("primary_library", "setup_cell", "setup_view")),
        'unless(session error("Cannot open owned Maestro working copy\\n"))',
        'mainSDB=axlGetMainSetupDB(session)',
        'unless(isCallable(\'maeStopSimulation) && isCallable(\'maeWaitUntilDone) error("Native Stop/Wait unavailable\\n"))',
    ]
    overrides = {}  # Requested fields are already resolved in the private policy.
    tests = [row["name"] for row in run_setup["tests"] if row.get("enabled", True)]
    body += job_policy_application_skill(launch_policy, overrides, session_expr="session",
        output_path=os.path.join(root, "effective_job_policy.tsv"), tests=tests).splitlines()
    body += native_storage_readback_lines(workspace)
    body += workspace_test_design_lines(workspace)
    selection = native_selection(workspace["setup"])
    if selection and execution_selection:
        selection["analyses"].update(execution_selection.get("analyses") or {})
    body += native_selection_lines(selection) if selection else test_lines(execution_selection)
    body += corner_lines(corner_selection)
    body += native_variable_scope_skill_lines(run_setup, os.path.join(root, "native_variable_scope.tsv"))
    body += native_corner_schedule_skill(run_setup, os.path.join(root, "native_corner_schedule.tsv"), include_members=True)
    if getattr(driver, "batch", None):
        from .generation_batch import native_batch_skill_lines
        body += native_batch_skill_lines(driver.batch, plan, os.path.join(root, "native_batch_columns.tsv"))
    else:
        body += native_parameter_skill_lines(plan, os.path.join(root, "native_parameters.tsv"))
    body.insert(0, 'sstatus(stacktrace 16)')
    body.insert(1, 'printf("%s\\n") drain(poport)' % STAGE.decode("ascii"))
    body += [_contract_text('crescBaselineRecord("READY" "")'),
             _contract_text('printf("CRESCENDO_NATIVE_RUN_STAGE Startup RETURN\\n")')]
    bounded, generation = bounded_skill_body_lines(body, _contract_text("crescBaselineBootstrap"))
    startup = os.path.join(driver.control, "startup.il")
    atomic_write_text(startup, '\n'.join(definitions + bounded + [
        _contract_text('unless(crescBaselineBootstrap crescBaselineRecord("ERROR" "working setup initialization failed"))')]) + '\n')
    atomic_write_json(os.path.join(driver.control, _contract_text("generation.json")), generation)
    run = os.path.join(driver.control, "run.il")
    atomic_write_text(run, '\n'.join([
        _contract_text('unless(equal(crescBaselineNonce %s) error("Native run nonce mismatch\\n"))') % _quote(driver.nonce),
        _contract_text('crescBaselineRun=errset(progn('),
        _contract_text(' crescBaselineSubmitted=t'),
        _contract_text(' crescBaselineRecord("RUN_BEGIN" "")'),
        _contract_text(' printf("CRESCENDO_NATIVE_RUN_STAGE Run BEGIN\\n")'),
        _contract_text(' runId=maeRunSimulation(?waitUntilDone nil ?returnRunId t ?callback "crescBaselineFinished()" ?session session)'),
        ' unless(runId error("Native Run did not return runId\\n"))',
        _contract_text(' crescBaselineRecord("RUN_ID" sprintf(nil "%L" runId))'),
        ' t) t)',
        _contract_text('unless(crescBaselineRun crescBaselineRecord("ERROR" "native Run failed"))')]) + '\n')
    finish = os.path.join(driver.control, "finish.il")
    atomic_write_text(finish, '\n'.join([
        _contract_text('unless(equal(crescBaselineNonce %s) error("Native finish nonce mismatch\\n"))') % _quote(driver.nonce),
        _contract_text('crescBaselineFinish=errset(progn('),
        _contract_text(" crescWaitResult=maeWaitUntilDone('All ?session session)"),
        _contract_text(' crescBaselineRecord("WAIT_RETURN" sprintf(nil "%L" crescWaitResult))'),
        ' runData=axlGetRunData(session runId)',
        ' unless(runData && runData!=0 error("Exact Run ID missing\\n"))',
        ' historyName=axlGetHistoryName(runData)',
        ' unless(stringp(historyName) && historyName!="" error("Exact history missing\\n"))',
        native_history_readback_line(workspace, root),
    ] + (native_batch_export(driver) if getattr(driver, 'batch', None) else []) + [
        _contract_text(' crescBaselineRecord("HISTORY" historyName)'),
        ' t) t)',
        _contract_text('unless(crescBaselineFinish crescBaselineRecord("ERROR" "native result finalization failed"))')]) + '\n')
    atomic_write_text(os.path.join(driver.control, "close.il"), '\n'.join([
        _contract_text('unless(equal(crescBaselineNonce %s) error("Native close nonce mismatch\\n"))') % _quote(driver.nonce),
        _contract_text('crescBaselineClose=errset(progn('),
        ' unless(maeCloseSession(?session session ?forceClose t) error("Native session close failed\\n"))',
        ' session=nil',
        _contract_text(' printf("CRESCENDO_NATIVE_RUN_STAGE Run RETURN\\n")'),
        _contract_text(' crescBaselineRecord("CLOSED" "native Run/Wait and session close complete")'),
        ' t) t)',
        _contract_text('unless(crescBaselineClose crescBaselineRecord("ERROR" "native session close failed"))')]) + '\n')
    return startup, run, finish


def _drain_native_spectre(workspace, rdb_path, history, cancel_check, timeout=2.0):
    """Give Spectre's footer time to reach disk before forcing Maestro closed."""
    from .completion import _SPECTRE_COMPLETION, _history_root, _tail_text
    from .rdb import read_rdb_test_statuses
    from .utils import is_under

    deadline = time.monotonic() + timeout
    tests = {test["name"]: test for test in workspace["setup"].get("tests", [])}
    pending = None
    if not is_under(os.path.realpath(rdb_path), workspace["root"]):
        return {"terminal_logs_seen": False, "reason": "unbound history path"}
    while not (cancel_check and cancel_check()):
        try:
            statuses = read_rdb_test_statuses(rdb_path)
        except (OSError, sqlite3.Error):
            statuses = []
        if statuses:
            pending = completed = 0
            for status in statuses:
                test = status["test_name"]
                point = status["pointID"]
                if (test not in tests or test in (".", "..") or os.path.basename(test) != test
                        or type(point) is not int or point < 0):
                    return {"terminal_logs_seen": False, "reason": "unbound point identity"}
                # A failed/omitted native point may never start its solver.
                if status["statusCode"] != 3:
                    continue
                completed += 1
                log = os.path.join(_history_root(workspace, tests[test], history), str(point), test, "psf", "spectre.out")
                if not is_under(os.path.realpath(log), workspace["root"]):
                    return {"terminal_logs_seen": False, "reason": "unbound solver log"}
                try:
                    terminal = bool(_SPECTRE_COMPLETION.search(_tail_text(log)))
                except OSError:
                    terminal = False
                pending += int(not terminal)
            if completed and pending == 0:
                return {"terminal_logs_seen": True}
            if not completed and all(status["stopTime"] is not None for status in statuses):
                return {"terminal_logs_seen": False, "reason": "no completed Spectre points"}
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(0.05, remaining))
    return {"terminal_logs_seen": False, "pending_logs": pending}


def evaluate_native_baseline(candidate_id, values, parameters, spec, prepared, config, root,
                             cancel_check=None, progress_callback=None, _batch=None, _consume=None):
    from .setup_private_session import borrow_setup_private_session, write_session_reference
    from .native_maestro_parameters import (build_native_parameter_plan, parse_native_parameter_settings,
                                           verify_native_parameter_application)
    from .workspace import _schematic_parameter_rows, verify_sources_unchanged, compact_source_guard
    from .storage_policy import (native_workspace_storage, workspace_rdb_path,
                                 load_native_storage_readback, qualify_native_history_paths)
    from .test_selection import selection_plan as test_plan, effective_setup as test_setup
    from .corner_selection import selection_plan as corner_plan, effective_setup as corner_setup
    from .private_netlist import parse_native_corner_schedule, _capture_setup_identity, _capture_worker_logs
    from .completion import (CompletionEvidenceError, UnsafeCompletionEvidence, validate_candidate_completion,
                             _history_root, prepare_native_point_parameter_requests)
    from .native_point_parameters import prefetch_point_parameters
    from .native_diagnostics import log_ocean_tool_warnings
    from .rdb import parse_rdb
    root = os.path.abspath(root)
    startup = native_diagnostics = worker_diagnostics = None
    spec = spec or {}
    deferred_consumers = []
    if _batch is not None:
        from .generation_batch import qualify_deterministic, qualify_shared_inputs
        qualify_deterministic(prepared["setup"])
        qualify_shared_inputs(_batch, prepared)
    if not prepared.get("setup_private_session"):
        raise NativeBaselineError("Native Run requires an applied Maestro working copy")
    if not parameters or set(values) != {row["parameter_id"] for row in parameters}:
        raise NativeBaselineError("Native Run selected parameter identity mismatch")
    if any(not native_parameter(row) for row in parameters):
        raise NativeBaselineError("Native Run supports selected Device Parameters only")
    from .simulators import selected_simulator
    selected = str(spec.get("simulator") or selected_simulator(config)).lower()
    saved = {(row.get("simulator") or "spectre").lower() for row in prepared["setup"].get("tests", [])
             if row.get("enabled", True)}
    if saved != {selected}:
        raise NativeBaselineError("Native Run simulator must match the saved Maestro Tests")
    timing = {"phases": {}}
    phase_scopes = {
        "build": "working storage, parameter plan, setup identity and SKILL scripts",
        "worker_setup": "Cadence startup lock through owned Ocean READY",
        "native_setup_readback": "native variable, parameter, storage and Job Setup readback",
        "execution": "native Run dispatch through observed SIM_DONE, including native queue and solve",
        "native_collection": "native Wait/Close, history lookup and point parameter readback",
        "native_cleanup": "owned Ocean and descendant cleanup",
        "collection": "RDB reopen, native completion and source verification",
    }

    def finish_phase(completed=True):
        now = timing_tick()
        timing["phases"][phase] = phase_seconds(
            elapsed_seconds(phase_started, now) if now is not None else None,
            boundary=phase_scopes[phase], completed=completed)
        return now

    ensure_dir(root)
    driver = _NativeSession(root, {"root": prepared["setup_private_session"]["workspace_root"]}, config, candidate_id)
    driver.batch = _batch
    if _batch is not None:
        atomic_write_json(os.path.join(root, _contract_text('candidate.json')), {'candidate_id': str(candidate_id),
            'execution_backend': 'maestro_baseline', 'batch_epoch': _batch['batch_epoch']})
    write_session_reference(root, prepared["setup_private_session"], kind="native_baseline")
    with borrow_setup_private_session(prepared, config, root, cancel_check,
                                     completion_reconciler=reconcile_native_baseline,
                                     phase_timings=timing["phases"]) as owned:
        phase, phase_started = "build", timing_tick()
        try:
            workspace = copy.deepcopy(owned)
            execution_selection = test_plan(workspace["setup"], spec)
            selected_setup = test_setup(workspace["setup"], execution_selection)
            corner_selection = corner_plan(selected_setup, spec)
            run_setup = corner_setup(selected_setup, corner_selection)
            workspace["native_storage"] = native_workspace_storage(
                ensure_dir(os.path.join(workspace["root"], "native_baseline_runs", uuid.uuid4().hex)),
                (owned.get("native_storage") or {}).get("source_local_options"))
            workspace["simulation_root"] = workspace["native_storage"]["save_res_dir"]
            _clones, rows = _schematic_parameter_rows(workspace, parameters, values, clone_state="existing")
            plan = build_native_parameter_plan(rows, run_setup.get("corners"),
                                              owned_paths=workspace.get("native_parameter_paths") or [])
            if _batch is not None:
                from .generation_batch import build_native_columns
                build_native_columns(_batch, plan["selected"])
            atomic_write_json(os.path.join(root, _contract_text("native_parameter_plan.json")), plan)
            snapshot = _capture_setup_identity(workspace, root, config=config)
            snapshot["run_execution_selection"] = copy.deepcopy(execution_selection)
            snapshot["run_corner_selection"] = copy.deepcopy(corner_selection)
            driver.workspace = workspace
            with _ACTIVE_LOCK:
                _ACTIVE[root] = driver
            if progress_callback:
                progress_callback({"stage": "native_baseline_startup", "candidate_id": str(candidate_id),
                                   "message": "Opening owned Maestro with native Device Parameters"})
            def make_scripts():
                nonlocal startup
                _record_storage_evidence(driver)
                scripts = _scripts(driver, rows, plan, run_setup, execution_selection,
                                   corner_selection, prepared, spec)
                startup = scripts[0]
                return scripts

            timeout = float(config.get("runtime", {}).get("ocean_startup_timeout_seconds", 90))
            phase_started = finish_phase()
            phase = "worker_setup"
            startup, run, finish = driver.start(make_scripts, timeout, cancel_check)
            phase_started = finish_phase()
            phase = "native_setup_readback"
            from .native_variable_scope_readback import parse_native_variable_scope
            native_scope = parse_native_variable_scope(os.path.join(root, "native_variable_scope.tsv"), run_setup)
            run_setup["native_variable_scope"] = native_scope
            if execution_selection is None:
                workspace["setup"]["native_variable_scope"] = native_scope
            if _batch is None:
                settings = parse_native_parameter_settings(os.path.join(root, "native_parameters.tsv"), plan)
            else:
                from .generation_batch import read_native_batch_columns
                settings = read_native_batch_columns(os.path.join(root, "native_batch_columns.tsv"), _batch)
                atomic_write_json(os.path.join(root, _contract_text("batch_manifest.json")), _batch)
            load_native_storage_readback(workspace, required=True)
            _record_storage_evidence(driver)
            from .job_policy import parse_policy_records, command_job_policy, verify_effective_job_policy
            runtime = config.get("runtime") or {}
            source_policy = prepared.get("job_policy") or runtime.get("maestro_source_job_policy") or {}
            expected = command_job_policy(source_policy, spec.get("maestro_job_overrides"),
                [row["name"] for row in run_setup["tests"] if row.get("enabled", True)])
            if _batch is not None:
                from .generation_batch import native_batch_policy
                expected = native_batch_policy(expected)
            with open(os.path.join(root, "effective_job_policy.tsv"), encoding="utf-8") as stream:
                actual_policy = parse_policy_records(stream.read())
            policy_proof = verify_effective_job_policy(actual_policy, expected,
                test_map={row["name"]: row["name"] for row in run_setup["tests"] if row.get("enabled", True)},
                command_wrappers=driver.job_observation.get("wrappers"))
            if not policy_proof["ok"]:
                raise NativeBaselineError("Native Job Setup differs from frozen policy: " + str(policy_proof["differences"]))
            atomic_write_json(os.path.join(root, _contract_text("effective_job_policy.json")), policy_proof)
            policies = [actual_policy.get("global") or {}] + list(actual_policy.get("tests") or [])
            driver.state["native_policy_local"] = bool(policies) and all(
                row.get("available") and str((row.get("properties") or {}).get("distributionmethod") or "").lower() == "local"
                for row in policies)
            driver.state["effective_job_policy"] = actual_policy
            native_parallelism = _native_parallelism(source_policy, actual_policy, spec)
            if _batch is not None:
                _batch['native_parallelism'] = native_parallelism
                _batch['job_policy'] = actual_policy
                _batch['native_schedule'] = parse_native_corner_schedule(os.path.join(root, 'native_corner_schedule.tsv'))
            budget = spec.get("candidate_timeout_seconds", runtime.get("simulation_timeout_seconds", 900))
            budget = float(budget) if budget not in (None, "") else -1
            driver.state["operation_budgets"] = {
                "startup_seconds": timeout, "candidate_seconds": budget,
                "candidate_scope": "elapsed wait for native SIM_DONE callback after Run dispatch",
                "job_policy_timeouts_are_independent": True}
            driver.persist()
            if cancel_check and cancel_check():
                error = NativeBaselineError("Native Run cancelled before Run")
                error.cancelled = True
                raise error
            if progress_callback:
                progress_callback({"stage": "native_baseline_run", "candidate_id": str(candidate_id),
                                   "message": "Native Maestro Run; using saved Job Setup"})
            phase_started = finish_phase()
            phase = "execution"
            _, worker_before = _capture_worker_logs(workspace)
            worker_started_ns = time.time_ns() if hasattr(time, "time_ns") else int(time.time() * 1e9)
            driver.state['submission_intent'] = True
            if _batch is not None:
                _batch['submitted'] = True
                atomic_write_json(os.path.join(root, _contract_text("batch_manifest.json")), _batch)
            driver.persist()  # A crash from this boundary forbids serial replay.
            driver.send(run)
            driver.wait(("SIM_DONE",), budget, cancel_check)
            phase_started = finish_phase()
            phase = "native_collection"
            collection_deadline = time.monotonic() + timeout if timeout > 0 else None
            driver.send(finish)
            history = driver.wait(("HISTORY",), timeout, cancel_check, deadline=collection_deadline)["value"]
            if history in (".", "..") or os.path.basename(history) != history:
                raise UnsafeCompletionEvidence("Unsafe Native Run history identity")
            if selected == "spectre":
                drain_timeout = (max(0, min(2.0, collection_deadline - time.monotonic()))
                                 if collection_deadline is not None else 2.0)
                driver.state["spectre_finalization"] = _drain_native_spectre(
                    workspace, workspace_rdb_path(workspace, history), history, cancel_check, timeout=drain_timeout)
                driver.persist()
            if cancel_check and cancel_check():
                error = NativeBaselineError("Native Run cancelled before session close")
                error.cancelled = True
                raise error
            driver.send(os.path.join(driver.control, "close.il"))
            driver.wait(("CLOSED",), timeout, cancel_check, deadline=collection_deadline)
            protocol = driver.status()
            run_id = next(row["value"] for row in protocol if row["state"] == "RUN_ID")
            command = {"schema_version": SCHEMA_VERSION, "backend": "maestro", "execution_simulator": selected,
                       "history_name": history, "run_id": run_id, "candidate_id": str(candidate_id),
                       "started_epoch": driver.state["started_epoch"], "command": driver.state["command"],
                       "native_corner_identity": {"setup_snapshot": snapshot,
                           "schedule": parse_native_corner_schedule(os.path.join(root, "native_corner_schedule.tsv")),
                           "point_parameters_required": True}}
            if "spectre_finalization" in driver.state:
                command["spectre_finalization"] = driver.state["spectre_finalization"]
            workspace["native_corner_identity"] = command["native_corner_identity"]
            command["native_history_storage"] = qualify_native_history_paths(workspace, root, history, command["started_epoch"])
            command["rdb_path"] = workspace_rdb_path(workspace, history)
            parsed = parse_rdb(command["rdb_path"])
            if _batch is not None:
                from .generation_batch import parse_native_rows, map_native_points, HistoryReferences
                from .rdb import slice_candidate_rdb
                native_rows = parse_native_rows(os.path.join(root, 'native_batch_points.tsv'))
                test_map = {t['name']: t['name'] for t in run_setup['tests'] if t.get('enabled', True)}

                def batch_partitions(data):
                    mapped = map_native_points(_batch, run_id, history, data, native_rows, allow_missing_status=True)
                    command["native_batch_points"] = [(point["test"], point["point_id"])
                                                      for points in mapped.values() for point in points]
                    if any(test not in test_map for test, _ in command["native_batch_points"]):
                        raise UnsafeCompletionEvidence("Native batch mapped an unexpected physical test")
                    partitions = []
                    for row in _batch["rows"]:
                        ids = {point["point_id"] for point in mapped[row["row_id"]]}
                        part = slice_candidate_rdb(data, test_map, ids)
                        part["expected_native_points"] = [
                            (point["test"], point["point_id"]) for point in mapped[row["row_id"]]]
                        partitions.append((row, part))
                    return partitions

            partitions = [(None, parsed)] if _batch is None else batch_partitions(parsed)
            readback_failures, parameter_readers = {}, {}
            for physical_row, partition in partitions:
                row_id = physical_row["row_id"] if physical_row else None
                readback_root = os.path.join(root, row_id) if physical_row else root
                try:
                    requests = prepare_native_point_parameter_requests(
                        workspace, run_setup, command, partition, readback_root, config=config)
                    parameter_readers[row_id] = prefetch_point_parameters(
                        requests, config, readback_root, driver, cancel_check=cancel_check)
                except UnsafeCompletionEvidence:
                    raise
                except CompletionEvidenceError as error:
                    readback_failures[row_id] = error
                except Exception as error:
                    reader_detail = getattr(error, 'detail', {}) or {}
                    error.detail = {'failure_stage': 'native_point_parameter_readback',
                                    'diagnostic_path': os.path.join(readback_root, _contract_text('point_identity.json')),
                                    'reader': reader_detail, 'command': reader_detail.get('command')}
                    raise
            phase_started = finish_phase()
            phase = "native_cleanup"
            cleanup = driver.cleanup(12)
            if not cleanup.get("cleanup_proven"):
                raise NativeBaselineError("Native session completed but owned process cleanup is unproven")
            native_diagnostics = _session_tool_diagnostics(driver, startup, selected)
            worker_diagnostics, _ = _capture_worker_logs(workspace, before=worker_before,
                simulators=(selected,), console=native_diagnostics, script_path=startup,
                capture_started_ns=worker_started_ns, completion=True)
            for key in ("ocean_tool_warnings", "diagnostic_write_error"):
                if key in native_diagnostics:
                    command[key] = native_diagnostics[key]
            phase_started = finish_phase()
            phase = "collection"
            command.update(guardian_returncode=driver.process.returncode, cleanup_pending=False,
                           finished_epoch=time.time(), native_stop=cleanup["native_stop"])
            if cleanup["previous_startup_crashes"]:
                command["native_startup_crashes"] = cleanup["previous_startup_crashes"]
            protocol = driver.status()
            with open(driver.protocol, "rb") as protocol_stream:
                protocol_digest = hashlib.sha256(protocol_stream.read()).hexdigest()
            command["native_baseline"] = {"schema_version": SCHEMA_VERSION,
                "nonce": driver.nonce, "candidate_id": str(candidate_id), "request_root": root,
                "startup_attempt": driver.state["startup_attempt"],
                "protocol_path": driver.protocol, "protocol_sha256": protocol_digest,
                "records": protocol, "run_id": run_id, "history_name": history,
                "started_epoch": command["started_epoch"], "finished_epoch": command["finished_epoch"],
                "native_session_closed": True, "local_cleanup_proven": True}
            # Reopen the RDB after cleanup; the cache is accepted only if final
            # validation independently derives the identical point requests.
            parsed = parse_rdb(command["rdb_path"])
            if _batch is not None:
                partitions = batch_partitions(parsed)
                atomic_write_json(os.path.join(root, _contract_text('batch_manifest.json')), _batch)
                references = HistoryReferences(os.path.join(root, _contract_text('history_references.json')), _batch)
            else:
                partitions = [(None, parsed)]
            def require_source_guard():
                guard = compact_source_guard(verify_sources_unchanged(owned))
                if not guard.get("ok"):
                    error = NativeBaselineError("Source changed during Native Run evaluation")
                    error.detail = {"failure_stage": "source_guard", "source_guard": guard}
                    raise error
                return guard

            from .design_variables import candidate_setup
            required = sorted({row["output_key"] for row in spec.get("objectives", []) + spec.get("constraints", [])
                               if row.get("output_key")})
            tests = {row["name"]: row for row in run_setup["tests"]}
            results = {}
            for physical_row, partition in partitions:
                parsed = partition
                if physical_row is not None:
                    values = physical_row['values']
                row_id = physical_row["row_id"] if physical_row else None
                try:
                    if row_id in readback_failures:
                        raise readback_failures[row_id]
                    point_output = (os.path.join(root, row_id + _contract_text("_completion.json"))
                                    if physical_row else None)
                    completion = validate_candidate_completion(workspace, root, command, parsed, output_path=point_output,
                        setup=candidate_setup(run_setup, parameters, values), required_output_keys=required or None, config=config,
                        reader_cancel_check=cancel_check, native_parameter_reader=parameter_readers[row_id],
                        native_diagnostics=native_diagnostics, native_worker_diagnostics=worker_diagnostics)
                    tasks = [{"test": point["test"], "point_id": point["point_id"], "corner": point.get("corner"),
                              "netlist_path": point.get("netlist_path") or os.path.join(_history_root(workspace, tests[point["test"]], history),
                                   str(point["point_id"]), point.get("artifact_test") or point["test"], "netlist", _contract_text("input.scs"))}
                             for point in completion["points"]]
                    parameter_application = verify_native_parameter_application(
                        rows, tasks, values, run_setup, parsed,
                        exact=bool(_batch and _batch.get('native_run_exact_values')))
                    parameter_application.update(settings=settings, solver_completion_proven=True)
                    guard = require_source_guard()
                    warnings = (completion.get("library_init_warnings") or []) + (completion.get("ocean_tool_warnings") or [])
                    log_ocean_tool_warnings(config, driver.control, warnings)
                    candidate_command = dict(command, library_init_warnings=completion.get("library_init_warnings") or [])
                    pipeline = {"schema_version": SCHEMA_VERSION, "solver": "native_maestro",
                                "submission": "native_job_setup", "run_id": run_id, "history_name": history,
                                "rdb_path": command["rdb_path"], "native_run_invoked": True,
                                "native_wait_completed": True, "native_session_closed": True,
                                "solver_completion_proven": True, "native_parallelism": native_parallelism}
                    completion["execution_pipeline"] = pipeline
                    if progress_callback:
                        progress_callback({"stage": "native_baseline_verified", "candidate_id": str(candidate_id),
                                           "history_name": history, "point_count": len(completion["points"]),
                                           "message": "Exact native history, solver completion and selected parameters verified"})
                    atomic_write_json(os.path.join(root, _contract_text("completion_evidence.json") if _batch is None else physical_row["row_id"] + _contract_text("_completion.json")), completion)
                    result = {"parsed": parsed, "parameter_application": parameter_application,
                              "completion_evidence": completion, "command": candidate_command, "source_guard": guard,
                              "timing": timing}
                    atomic_write_json(os.path.join(root, _contract_text("native_baseline_result.json") if _batch is None else physical_row["row_id"] + _contract_text("_result.json")), result)
                except UnsafeCompletionEvidence:
                    raise
                except CompletionEvidenceError as error:
                    if physical_row is None:
                        raise
                    guard = require_source_guard()
                    error.detail = dict(getattr(error, "detail", None) or {},
                        source_guard=guard, cleanup_pending=False,
                        native_baseline=cleanup, native_stop=cleanup["native_stop"])
                    error.detail.setdefault("failure_stage", "native_completion_evidence")
                    result = error
                if _batch is None:
                    finish_phase()
                    return result
                for candidate in [c for c in _batch['candidates'] if c['row_id'] == physical_row['row_id']]:
                    reference = {'schema_version': _batch['schema_version'],
                        'batch_epoch': _batch['batch_epoch'], 'root': root,
                        'manifest': os.path.join(root, _contract_text('batch_manifest.json')),
                        'timing': os.path.join(root, _contract_text('batch_timing.json')),
                        'physical_ids': _batch['candidate_to_physical'][candidate['candidate_id']]}
                    if isinstance(result, CompletionEvidenceError):
                        item = type(result)(str(result))
                        item.detail = copy.deepcopy(result.detail)
                        item.batch_reference = reference
                    else:
                        item = copy.deepcopy(result)
                        item['batch_reference'] = reference
                        item['timing'] = {'phases': {}, 'shared_batch': reference}
                    finish_consumer = _consume(candidate, item) if _consume else None
                    if callable(finish_consumer):
                        deferred_consumers.append((candidate["candidate_id"], finish_consumer))
                    else:
                        references.commit(candidate["candidate_id"])
                    results[candidate["candidate_id"]] = item
            finish_phase()
            atomic_write_json(os.path.join(root, _contract_text('batch_timing.json')), timing)
        except BaseException as error:
            if phase not in timing["phases"]:
                finish_phase(completed=False)
            failure_cleanup_started = timing_tick()
            try:
                proof = driver.cleanup(12) if driver is not None else {"cleanup_proven": True, "cleanup_pending": False}
            except Exception as cleanup_error:
                proof = {"cleanup_proven": False, "cleanup_pending": True,
                         "secondary_cleanup_error": "%s: %s" % (type(cleanup_error).__name__, cleanup_error)}
            if "native_cleanup" not in timing["phases"]:
                timing["phases"]["native_cleanup"] = phase_seconds(elapsed_seconds(failure_cleanup_started),
                    boundary=phase_scopes["native_cleanup"], completed=proof.get("cleanup_proven") is True)
            if _batch is not None:
                atomic_write_json(os.path.join(root, _contract_text('batch_timing.json')), timing)
            detail = dict(getattr(error, "detail", None) or {})
            if isinstance(error, CompletionEvidenceError):
                detail.setdefault("failure_stage", "native_completion_evidence")
                if isinstance(error, UnsafeCompletionEvidence):
                    detail["completion_evidence_unsafe"] = True
                source_guard = compact_source_guard(verify_sources_unchanged(owned))
                detail["source_guard"] = source_guard
                if not source_guard.get("ok"):
                    detail.update(failure_stage="source_guard",
                                  completion_evidence_unsafe=True)
            if isinstance(error, NativeStartupCrash):
                detail["failure_stage"] = "native_startup_crash"
            elif isinstance(error, NativeBaselineTimeout):
                detail.setdefault("failure_stage", "native_operation_timeout")
                detail["phase"] = phase
            crashes = driver.state.get("previous_startup_crashes") or []
            first_crash = crashes[0] if crashes else driver.state.get("startup_crash")
            if first_crash:
                detail["first_startup_crash"] = first_crash
            detail.setdefault("failure_stage", "native_maestro_baseline")
            if startup is not None and native_diagnostics is None and driver is not None:
                try:
                    native_diagnostics = _session_tool_diagnostics(driver, startup, selected)
                except (OSError, ValueError):
                    pass  # A diagnostic write cannot replace the native failure.
            if native_diagnostics and (native_diagnostics.get("native_error") or native_diagnostics.get("library_init_warnings")):
                detail["skill"] = _unqualified_library_init(native_diagnostics)
                if (not getattr(error, "cancelled", False)
                        and not isinstance(error, (CompletionEvidenceError, NativeStartupCrash, NativeBaselineTimeout))
                        and (native_diagnostics.get("first_simulator_tool_error")
                             or phase in ("worker_setup", "execution", "native_collection"))
                        and native_diagnostics.get("first_error")):
                    error.args = (native_diagnostics["first_error"]["message"],)
            if worker_diagnostics:
                detail["native_worker_diagnostics"] = dict(worker_diagnostics, logs=[
                    dict(log, warnings=[dict(row, severity="UNQUALIFIED")
                                        if row.get("category") == "library_init" else row
                                        for row in log["warnings"]])
                    for log in worker_diagnostics["logs"]])
            detail.update(cleanup_pending=not proof.get("cleanup_proven"), native_stop=proof.get("native_stop") or {},
                native_baseline=proof, cancelled=bool(getattr(error, "cancelled", False)),
                timing=timing)
            error.detail = detail
            if (_batch is not None and not driver.state.get('submission_intent')
                    and proof.get('cleanup_proven') and not getattr(error, 'cancelled', False)
                    and not detail.get('completion_evidence_unsafe')
                    and detail.get('failure_stage') != 'source_guard'
                    and not (cancel_check and cancel_check())):
                from .generation_batch import BatchUnsupported
                fallback = BatchUnsupported(str(error))
                fallback.detail = detail
                raise fallback
            raise
        finally:
            if driver is not None and driver.state.get("cleanup_proven"):
                with _ACTIVE_LOCK:
                    _ACTIVE.pop(root, None)
    # Retrying a partition borrows the same suite again. Release this batch's
    # lease first and retain its history reference until the retry is settled.
    for candidate_id, finish_consumer in deferred_consumers:
        finish_consumer()
        references.commit(candidate_id)
    return results


def native_batch_export(driver):
    from .generation_batch import native_point_export_lines
    return native_point_export_lines(os.path.join(driver.root, 'native_batch_points.tsv'))


def evaluate_native_baseline_generation(plan, parameters, spec, prepared, config, root,
                                        cancel_check=None, progress_callback=None, consume=None):
    if plan.get('schema_version') != _contract_text('crescendo.generation_batch.v1') or plan.get('submitted'):
        raise NativeBaselineError('Invalid or already submitted native generation plan')
    if len(plan["rows"]) == 1:
        from .generation_batch import BatchUnsupported
        # Cadence exports a single set as a scalar run without design conditions.
        raise BatchUnsupported("Native generation has only one distinct physical row")
    return evaluate_native_baseline('batch_' + plan['batch_epoch'], plan['rows'][0]['values'],
        parameters, spec, prepared, config, root, cancel_check, progress_callback,
        _batch=plan, _consume=consume)


def _command_terminal_proof(driver):
    """Native Stop/Wait/Close plus every observed Command job's exact terminal.

    Neither a vanished job nor a successful cancel command is a terminal.
    Missing/truncated native submission observations remain unsafe.
    """
    from .scheduler_cli import scheduler_command_argv
    from .scheduler_protocol import parse_scheduler_observation
    commands = (driver.config.get('runtime') or {}).get('execution_commands') or {}
    template = commands.get('scheduler_query_template')
    if not template:
        return {'ok': False, 'reason': 'No exact Command job query template'}
    path = os.path.join(driver.root, _contract_text('native_dispatch.jsonl'))
    try:
        if os.path.islink(path) or os.path.getsize(path) > 16 * 1024 * 1024:
            raise ValueError('Unsafe native Command observations')
        with open(path, encoding='utf-8') as stream:
            events = [json.loads(line) for line in stream]
        starts = [r for r in events if r.get('event') == 'submit_start']
        ends = [r for r in events if r.get('event') == 'submit_end']
        if not starts or len(starts) != len(ends):
            raise ValueError('Command submission observations incomplete')
        start_ids = {r['invocation_id'] for r in starts}
        if len(start_ids) != len(starts) or {r['invocation_id'] for r in ends} != start_ids:
            raise ValueError('Command invocation coverage differs')
        job_ids = []
        for end in ends:
            if (len(end.get('job_ids') or []) != 1 or any(end.get(k) for k in
                    ('output_incomplete', 'job_id_protocol_limited', 'output_truncated'))):
                raise ValueError('Command has no unique complete Job ID receipt')
            job_ids.append(end['job_ids'][0])
        if len(set(job_ids)) != len(job_ids):
            raise ValueError('Command job receipt identity duplicated')
        from .cadence import child_environment
        env = child_environment(driver.config, driver.workspace['root'], include_runtime_ld=False)
        from .native_environment import scheduler_environment
        # The query uses the same caller HOME context as the submission observer.
        env = scheduler_environment(env)
        observations = []
        for job_id in job_ids:
            process = subprocess.run(scheduler_command_argv(template, job_id), cwd=driver.root,
                env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=3)
            observation = parse_scheduler_observation(process.stdout, job_id)
            observations.append(observation)
            if process.returncode or not observation.get('terminal') or observation.get('invalid') or observation.get('not_found'):
                return {'ok': False, 'reason': 'Exact Command job terminal unproven', 'observations': observations}
        return {'ok': True, 'observations': observations, 'job_ids': job_ids}
    except Exception as error:
        return {'ok': False, 'reason': str(error)}
