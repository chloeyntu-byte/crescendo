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
import stat
import subprocess
import threading
import time
import uuid

from .utils import atomic_write_json, atomic_write_text, ensure_dir
from .workspace import (_skill_quote, _workspace_startup_load_lines,
                        bounded_skill_body_lines, CADENCE_STARTUP_LOCK)

SCHEMA_VERSION = "crescendo.maestro_baseline.v1"
_ACTIVE = {}
_ACTIVE_LOCK = threading.Lock()


class NativeBaselineError(RuntimeError):
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
            raise ValueError("native diagnostic storage is not this candidate's direct private directory")
        identity = os.lstat(root)
        if not stat.S_ISDIR(identity.st_mode) or identity.st_uid != os.getuid():
            raise ValueError("native diagnostic storage identity differs")
        payload = {"schema_version": "crescendo.native_storage_evidence.v1",
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
        path = os.path.join(driver.control, "native_storage_evidence.json")
        atomic_write_json(path, payload)
        driver.state["native_storage_evidence"] = {
            "path": path, "storage_root": root,
            "native_readback_verified": payload["native_readback_verified"], "diagnostic_only": True}
        driver.state.pop("diagnostic_evidence_error", None)
    except (OSError, ValueError, KeyError) as error:
        driver.state["diagnostic_evidence_error"] = "%s: %s" % (type(error).__name__, error)
    driver.persist()


def _read_protocol(path, nonce):
    if not os.path.isfile(path):
        return []
    if os.path.islink(path) or os.path.getsize(path) > 1024 * 1024:
        raise NativeBaselineError("Invalid native baseline protocol file")
    with open(path, encoding="utf-8") as stream:
        rows = [line.rstrip("\n").split("\t") for line in stream]
    if any(len(row) != 3 or row[0] != nonce for row in rows):
        raise NativeBaselineError("Native baseline protocol identity mismatch")
    return [{"state": row[1], "value": row[2], "sequence": index} for index, row in enumerate(rows)]


class _NativeSession(object):
    def __init__(self, root, workspace, config, candidate_id):
        self.root, self.workspace, self.config = root, workspace, config
        self.control = ensure_dir(os.path.join(root, "native_baseline"))
        self.path = os.path.join(self.control, "native_baseline_state.json")
        if os.path.lexists(self.path):
            raise NativeBaselineError("Native baseline request directory already used; no replay allowed")
        self.nonce = uuid.uuid4().hex
        self.protocol = os.path.join(self.control, "native_baseline_protocol.tsv")
        self.process = None
        self.state = {"schema_version": SCHEMA_VERSION, "nonce": self.nonce,
                      "candidate_id": str(candidate_id), "request_root": root, "workspace_root": workspace["root"],
                      "started_epoch": time.time(), "launch_attempted": False,
                      "native_session_closed": False, "native_policy_local": False, "cleanup_proven": False,
                      "cleanup_pending": False, "native_stop": {"requested": False,
                      "remote_stop_proven": False}}
        self.persist()

    def persist(self):
        atomic_write_json(self.path, self.state)

    def status(self):
        return _read_protocol(self.protocol, self.nonce)

    def send(self, script):
        if self.process is None or self.process.poll() is not None:
            raise NativeBaselineError("Owned Ocean is unavailable")
        self.process.stdin.write('load(%s)\n' % _quote(script))
        self.process.stdin.flush()

    def wait(self, terminal, timeout, cancel_check=None):
        deadline = time.monotonic() + timeout if timeout > 0 else None
        while True:
            rows = self.status()
            for row in reversed(rows):
                if row["state"] in terminal:
                    return row
                if row["state"] == "STOP_ERROR" or (row["state"] == "ERROR" and "STOP_DONE" not in terminal):
                    raise NativeBaselineError("Native baseline failed: " + row["value"])
            if cancel_check and cancel_check():
                error = NativeBaselineError("Native baseline cancelled")
                error.cancelled = True
                raise error
            if self.process is None or self.process.poll() is not None:
                raise NativeBaselineError("Owned Ocean exited before native terminal evidence")
            if deadline is not None and time.monotonic() >= deadline:
                raise NativeBaselineError("Native baseline exceeded its total operation deadline")
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
        state_path = os.path.join(self.control, "guardian_state.json")
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
        if not self.state["launch_attempted"]:
            self.state.update(cleanup_proven=True, cleanup_pending=False)
            self.persist()
            return dict(self.state)
        rows = self.status()
        closed = any(row["state"] in ("CLOSED", "STOP_DONE") for row in rows)
        if not closed and self.process is not None and self.process.poll() is None:
            script = os.path.join(self.control, "stop.il")
            atomic_write_text(script, '\n'.join([
                'unless(equal(crescBaselineNonce %s) error("Crescendo baseline Stop nonce mismatch\\n"))' % _quote(self.nonce),
                'crescBaselineStopping=t',
                'crescBaselineRecord("STOP_BEGIN" "")',
                'crescBaselineStop=errset(progn(',
                ' unless(boundp(\'session) && session error("Owned session unavailable\\n"))',
                ' crescStopAccepted=errset(maeStopSimulation(?session session) t)',
                ' unless(crescStopAccepted error("Native Stop API failed\\n"))',
                ' unless(car(crescStopAccepted) || crescBaselineDone || !crescBaselineSubmitted error("Native Stop rejected\\n"))',
                " crescWaitResult=maeWaitUntilDone('All ?session session)",
                ' crescBaselineRecord("WAIT_RETURN" sprintf(nil "%L" crescWaitResult))',
                ' unless(maeCloseSession(?session session ?forceClose t) error("Native session close failed\\n"))',
                ' session=nil',
                ' crescBaselineRecord("STOP_DONE" "owned session stopped, waited and closed")',
                ' t) t)',
                'unless(crescBaselineStop crescBaselineRecord("STOP_ERROR" "native Stop/Wait/Close failed"))',
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
        if closed:
            if self.process is not None and self.process.poll() is None:
                try:
                    self.process.stdin.write("exit(0)\n")
                    self.process.stdin.flush()
                    self.process.wait(timeout=max(1, timeout))
                except Exception:
                    pass
            from .legacy_guardian import quiesce_legacy_guardian_attempt
            proof = quiesce_legacy_guardian_attempt(self.control,
                expected_workspace_root=self.workspace["root"], timeout_seconds=timeout)
            self.state["local_cleanup"] = proof
            self.state["cleanup_proven"] = bool(proof.get("state") == "dead" and proof.get("termination_proof", {}).get("ok") is True)
        if native_stopped:
            local_stopped = self.state["cleanup_proven"] and self.state.get("native_policy_local") is True
            self.state["native_stop"]["remote_stop_proven"] = local_stopped
            self.state["native_stop"]["proof_scope"] = "native_local_jobs_and_owned_descendants" if local_stopped else "native_stop_wait_close_without_remote_job_terminal_receipts"
            if not local_stopped:
                self.state["cleanup_proven"] = False
        # Local termination never substitutes for native Stop or remote status.
        self.state["cleanup_pending"] = not self.state["cleanup_proven"]
        self.persist()
        return dict(self.state)


def reconcile_native_baseline(root, request_cancel=True, timeout_seconds=12):
    root = os.path.abspath(root)
    with _ACTIVE_LOCK:
        active = _ACTIVE.get(root)
    if active:
        return active.cleanup(timeout_seconds) if request_cancel else dict(active.state)
    path = os.path.join(root, "native_baseline", "native_baseline_state.json")
    if not os.path.exists(path):
        return {"schema_version": SCHEMA_VERSION, "cleanup_proven": False,
                "cleanup_pending": True, "reason": "native launch record missing; termination cannot be established"}
    state = _load(path)
    if state.get("schema_version") != SCHEMA_VERSION or state.get("request_root") != root:
        raise NativeBaselineError("Native baseline recovery protocol mismatch")
    if not state.get("launch_attempted"):
        return dict(state, cleanup_proven=True, cleanup_pending=False)
    if state.get("cleanup_proven") and state.get("native_session_closed"):
        return state
    # Lost Python controller cannot submit a new Stop into an unrelated session.
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
        'crescBaselineNonce=%s' % _quote(driver.nonce),
        'crescBaselineSubmitted=nil crescBaselineDone=nil crescBaselineStopping=nil session=nil',
        'procedure(crescBaselineRecord(state value)',
        ' let((port) port=outfile(%s "a")' % _quote(driver.protocol),
        '  unless(port error("Cannot write native protocol\\n"))',
        '  fprintf(port "%s\\t%s\\t%s\\n" crescBaselineNonce state value) close(port)))',
        'procedure(crescBaselineFinished()',
        ' crescBaselineDone=t',
        ' unless(crescBaselineStopping crescBaselineRecord("SIM_DONE" "")))',
    ]
    runtime = driver.config.get("runtime") or {}
    policy = prepared.get("job_policy") or runtime.get("maestro_source_job_policy") or {}
    tests = [row["name"] for row in run_setup["tests"] if row.get("enabled", True)]
    policy = command_job_policy(policy, spec.get("maestro_job_overrides"), tests)
    # Keep scheduler HOME scoped to the actual command, and retain its native
    # argv and submission evidence. Unsupported shell commands remain native.
    driver.job_observation = prepare_job_command_observers(
        policy, {}, os.path.join(driver.control, "job_observers"),
        os.path.join(root, "native_dispatch.jsonl"))
    launch_policy = _baseline_launch_policy(policy, driver.job_observation.get("wrappers"))
    body = _workspace_startup_load_lines(workspace)
    body += native_storage_init_lines(workspace["native_storage"])
    body += native_storage_capture_definitions().splitlines()
    body += _native_policy_definitions(workspace, launch_policy)
    body += [
        'sstatus(errsetTrace nil)',
        'envSetVal("adexl.historyNamePrefix" "showNameHistoryForm" \'boolean nil)',
        'envSetVal("adexl.simulation" "saveLastNHistoryEntries" \'int 1000)',
        'session=maeOpenSetup(%s %s %s ?mode "a")' % tuple(_quote(workspace[key]) for key in ("primary_library", "setup_cell", "setup_view")),
        'unless(session error("Cannot open owned private Maestro\\n"))',
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
    body += native_parameter_skill_lines(plan, os.path.join(root, "native_parameters.tsv"))
    body += native_corner_schedule_skill(run_setup, os.path.join(root, "native_corner_schedule.tsv"), include_members=True)
    body += ['crescBaselineRecord("READY" "")']
    bounded, generation = bounded_skill_body_lines(body, "crescBaselineBootstrap")
    startup = os.path.join(driver.control, "startup.il")
    atomic_write_text(startup, '\n'.join(definitions + bounded + [
        'unless(crescBaselineBootstrap crescBaselineRecord("ERROR" "private setup initialization failed"))']) + '\n')
    atomic_write_json(os.path.join(driver.control, "generation.json"), generation)
    run = os.path.join(driver.control, "run.il")
    atomic_write_text(run, '\n'.join([
        'unless(equal(crescBaselineNonce %s) error("Native run nonce mismatch\\n"))' % _quote(driver.nonce),
        'crescBaselineRun=errset(progn(',
        ' crescBaselineSubmitted=t',
        ' crescBaselineRecord("RUN_BEGIN" "")',
        ' runId=maeRunSimulation(?waitUntilDone nil ?returnRunId t ?callback "crescBaselineFinished()" ?session session)',
        ' unless(runId error("Native Run did not return runId\\n"))',
        ' crescBaselineRecord("RUN_ID" sprintf(nil "%L" runId))',
        ' t) t)',
        'unless(crescBaselineRun crescBaselineRecord("ERROR" "native Run failed"))']) + '\n')
    finish = os.path.join(driver.control, "finish.il")
    atomic_write_text(finish, '\n'.join([
        'unless(equal(crescBaselineNonce %s) error("Native finish nonce mismatch\\n"))' % _quote(driver.nonce),
        'crescBaselineFinish=errset(progn(',
        " crescWaitResult=maeWaitUntilDone('All ?session session)",
        ' crescBaselineRecord("WAIT_RETURN" sprintf(nil "%L" crescWaitResult))',
        ' runData=axlGetRunData(session runId)',
        ' unless(runData && runData!=0 error("Exact Run ID missing\\n"))',
        ' historyName=axlGetHistoryName(runData)',
        ' unless(stringp(historyName) && historyName!="" error("Exact history missing\\n"))',
        native_history_readback_line(workspace, root),
        ' crescBaselineRecord("HISTORY" historyName)',
        ' unless(maeCloseSession(?session session ?forceClose t) error("Native session close failed\\n"))',
        ' session=nil',
        ' crescBaselineRecord("CLOSED" "native Run/Wait and session close complete")',
        ' t) t)',
        'unless(crescBaselineFinish crescBaselineRecord("ERROR" "native result finalization failed"))']) + '\n')
    return startup, run, finish


def evaluate_native_baseline(candidate_id, values, parameters, spec, prepared, config, root,
                             cancel_check=None, progress_callback=None):
    from .setup_private_session import borrow_setup_private_session, write_session_reference
    from .native_maestro_parameters import (build_native_parameter_plan, parse_native_parameter_settings,
                                           verify_native_parameter_application)
    from .workspace import _schematic_parameter_rows, verify_sources_unchanged, compact_source_guard
    from .storage_policy import (native_workspace_storage, workspace_rdb_path,
                                 load_native_storage_readback, qualify_native_history_paths)
    from .test_selection import selection_plan as test_plan, effective_setup as test_setup
    from .corner_selection import selection_plan as corner_plan, effective_setup as corner_setup
    from .private_netlist import parse_native_corner_schedule, _capture_setup_identity
    from .completion import validate_candidate_completion, _history_root, prepare_native_point_parameter_requests
    from .native_point_parameters import prefetch_point_parameters
    from .rdb import parse_rdb
    root = os.path.abspath(root)
    spec = spec or {}
    if not prepared.get("setup_private_session"):
        raise NativeBaselineError("Native baseline requires an applied private Maestro suite")
    if not parameters or set(values) != {row["parameter_id"] for row in parameters}:
        raise NativeBaselineError("Native baseline selected parameter identity mismatch")
    if any(row.get("backend", "schematic") != "schematic" for row in parameters):
        raise NativeBaselineError("Native baseline supports selected Device Parameters only")
    from .simulators import selected_simulator
    selected = str(spec.get("simulator") or selected_simulator(config)).lower()
    saved = {(row.get("simulator") or "spectre").lower() for row in prepared["setup"].get("tests", [])
             if row.get("enabled", True)}
    if saved != {selected}:
        raise NativeBaselineError("Native baseline simulator must match the saved Maestro Tests")
    ensure_dir(root)
    driver = _NativeSession(root, {"root": prepared["setup_private_session"]["workspace_root"]}, config, candidate_id)
    write_session_reference(root, prepared["setup_private_session"], kind="native_baseline")
    with borrow_setup_private_session(prepared, config, root, cancel_check,
                                     completion_reconciler=reconcile_native_baseline) as owned:
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
            atomic_write_json(os.path.join(root, "native_parameter_plan.json"), plan)
            snapshot = _capture_setup_identity(workspace, root, config=config)
            snapshot["run_execution_selection"] = copy.deepcopy(execution_selection)
            snapshot["run_corner_selection"] = copy.deepcopy(corner_selection)
            driver.workspace = workspace
            _record_storage_evidence(driver)
            with _ACTIVE_LOCK:
                _ACTIVE[root] = driver
            if progress_callback:
                progress_callback({"stage": "native_baseline_startup", "candidate_id": str(candidate_id),
                                   "message": "Opening owned Maestro with native Device Parameters"})
            startup, run, finish = _scripts(driver, rows, plan, run_setup, execution_selection,
                                            corner_selection, prepared, spec)
            timeout = float(config.get("runtime", {}).get("ocean_startup_timeout_seconds", 90))
            locked = CADENCE_STARTUP_LOCK.acquire(timeout=max(1, timeout))
            if not locked:
                raise NativeBaselineError("Native startup lock timed out")
            try:
                driver.launch(startup)
                driver.wait(("READY",), timeout, cancel_check)
            finally:
                CADENCE_STARTUP_LOCK.release()
            from .native_variable_scope_readback import parse_native_variable_scope
            native_scope = parse_native_variable_scope(os.path.join(root, "native_variable_scope.tsv"), run_setup)
            run_setup["native_variable_scope"] = native_scope
            if execution_selection is None:
                workspace["setup"]["native_variable_scope"] = native_scope
            settings = parse_native_parameter_settings(os.path.join(root, "native_parameters.tsv"), plan)
            load_native_storage_readback(workspace, required=True)
            _record_storage_evidence(driver)
            from .job_policy import parse_policy_records, command_job_policy, verify_effective_job_policy
            runtime = config.get("runtime") or {}
            expected = command_job_policy(prepared.get("job_policy") or runtime.get("maestro_source_job_policy") or {},
                spec.get("maestro_job_overrides"), [row["name"] for row in run_setup["tests"] if row.get("enabled", True)])
            with open(os.path.join(root, "effective_job_policy.tsv"), encoding="utf-8") as stream:
                actual_policy = parse_policy_records(stream.read())
            policy_proof = verify_effective_job_policy(actual_policy, expected,
                test_map={row["name"]: row["name"] for row in run_setup["tests"] if row.get("enabled", True)},
                command_wrappers=driver.job_observation.get("wrappers"))
            if not policy_proof["ok"]:
                raise NativeBaselineError("Native Job Setup differs from frozen policy: " + str(policy_proof["differences"]))
            atomic_write_json(os.path.join(root, "effective_job_policy.json"), policy_proof)
            policies = [actual_policy.get("global") or {}] + list(actual_policy.get("tests") or [])
            driver.state["native_policy_local"] = bool(policies) and all(
                row.get("available") and str((row.get("properties") or {}).get("distributionmethod") or "").lower() == "local"
                for row in policies)
            driver.state["effective_job_policy"] = actual_policy
            budget = spec.get("candidate_timeout_seconds", runtime.get("simulation_timeout_seconds", 900))
            budget = float(budget) if budget not in (None, "") else -1
            driver.state["operation_budgets"] = {
                "startup_seconds": timeout, "candidate_seconds": budget,
                "candidate_scope": "elapsed wait for native SIM_DONE callback after Run dispatch",
                "job_policy_timeouts_are_independent": True}
            driver.persist()
            if cancel_check and cancel_check():
                error = NativeBaselineError("Native baseline cancelled before Run")
                error.cancelled = True
                raise error
            if progress_callback:
                progress_callback({"stage": "native_baseline_run", "candidate_id": str(candidate_id),
                                   "message": "Native Maestro Run; using saved Job Setup"})
            driver.send(run)
            driver.wait(("SIM_DONE",), budget, cancel_check)
            driver.send(finish)
            driver.wait(("CLOSED",), timeout, cancel_check)
            protocol = driver.status()
            history = next(row["value"] for row in protocol if row["state"] == "HISTORY")
            run_id = next(row["value"] for row in protocol if row["state"] == "RUN_ID")
            command = {"schema_version": SCHEMA_VERSION, "backend": "maestro", "execution_simulator": selected,
                       "history_name": history, "run_id": run_id, "candidate_id": str(candidate_id),
                       "started_epoch": driver.state["started_epoch"], "command": driver.state["command"],
                       "native_corner_identity": {"setup_snapshot": snapshot,
                           "schedule": parse_native_corner_schedule(os.path.join(root, "native_corner_schedule.tsv")),
                           "point_parameters_required": True}}
            workspace["native_corner_identity"] = command["native_corner_identity"]
            command["native_history_storage"] = qualify_native_history_paths(workspace, root, history, command["started_epoch"])
            command["rdb_path"] = workspace_rdb_path(workspace, history)
            parsed = parse_rdb(command["rdb_path"])
            requests = prepare_native_point_parameter_requests(workspace, run_setup, command, parsed, root, config=config)
            try:
                parameter_reader = prefetch_point_parameters(requests, config, root, driver, cancel_check=cancel_check)
            except Exception as error:
                reader_detail = getattr(error, 'detail', {}) or {}
                error.detail = {'failure_stage': 'native_point_parameter_readback',
                                'diagnostic_path': os.path.join(root, 'point_identity.json'),
                                'reader': reader_detail, 'command': reader_detail.get('command')}
                raise
            cleanup = driver.cleanup(12)
            if not cleanup.get("cleanup_proven"):
                raise NativeBaselineError("Native session completed but owned process cleanup is unproven")
            command.update(guardian_returncode=driver.process.returncode, cleanup_pending=False,
                           finished_epoch=time.time(), native_stop=cleanup["native_stop"])
            protocol = driver.status()
            with open(driver.protocol, "rb") as protocol_stream:
                protocol_digest = hashlib.sha256(protocol_stream.read()).hexdigest()
            command["native_baseline"] = {"schema_version": SCHEMA_VERSION,
                "nonce": driver.nonce, "candidate_id": str(candidate_id), "request_root": root,
                "protocol_path": driver.protocol, "protocol_sha256": protocol_digest,
                "records": protocol, "run_id": run_id, "history_name": history,
                "started_epoch": command["started_epoch"], "finished_epoch": command["finished_epoch"],
                "native_session_closed": True, "local_cleanup_proven": True}
            # Reopen the RDB after cleanup; the cache is accepted only if final
            # validation independently derives the identical point requests.
            parsed = parse_rdb(command["rdb_path"])
            required = sorted({row["output_key"] for row in spec.get("objectives", []) + spec.get("constraints", []) if row.get("output_key")})
            completion = validate_candidate_completion(workspace, root, command, parsed,
                setup=run_setup, required_output_keys=required or None, config=config,
                reader_cancel_check=cancel_check, native_parameter_reader=parameter_reader)
            tests = {row["name"]: row for row in run_setup["tests"]}
            tasks = [{"test": point["test"], "point_id": point["point_id"], "corner": point.get("corner"),
                      "netlist_path": point.get("netlist_path") or os.path.join(_history_root(workspace, tests[point["test"]], history),
                           str(point["point_id"]), point.get("artifact_test") or point["test"], "netlist", "input.scs")}
                     for point in completion["points"]]
            parameter_application = verify_native_parameter_application(rows, tasks, values, run_setup, parsed)
            parameter_application.update(settings=settings, solver_completion_proven=True)
            guard = compact_source_guard(verify_sources_unchanged(owned))
            if not guard.get("ok"):
                error = NativeBaselineError("Source changed during native baseline evaluation")
                error.detail = {"failure_stage": "source_guard", "source_guard": guard}
                raise error
            pipeline = {"schema_version": SCHEMA_VERSION, "solver": "native_maestro",
                        "submission": "native_job_setup", "run_id": run_id, "history_name": history,
                        "rdb_path": command["rdb_path"], "native_run_invoked": True,
                        "native_wait_completed": True, "native_session_closed": True,
                        "solver_completion_proven": True}
            completion["execution_pipeline"] = pipeline
            if progress_callback:
                progress_callback({"stage": "native_baseline_verified", "candidate_id": str(candidate_id),
                                   "history_name": history, "point_count": len(completion["points"]),
                                   "message": "Exact native history, solver completion and selected parameters verified"})
            atomic_write_json(os.path.join(root, "completion_evidence.json"), completion)
            result = {"parsed": parsed, "parameter_application": parameter_application,
                      "completion_evidence": completion, "command": command, "source_guard": guard}
            atomic_write_json(os.path.join(root, "native_baseline_result.json"), result)
            return result
        except BaseException as error:
            try:
                proof = driver.cleanup(12) if driver is not None else {"cleanup_proven": True, "cleanup_pending": False}
            except Exception as cleanup_error:
                proof = {"cleanup_proven": False, "cleanup_pending": True,
                         "secondary_cleanup_error": "%s: %s" % (type(cleanup_error).__name__, cleanup_error)}
            detail = dict(getattr(error, "detail", None) or {})
            detail.setdefault("failure_stage", "native_maestro_baseline")
            detail.update(cleanup_pending=not proof.get("cleanup_proven"), native_stop=proof.get("native_stop") or {},
                native_baseline=proof, cancelled=bool(getattr(error, "cancelled", False)))
            error.detail = detail
            raise
        finally:
            if driver is not None and driver.state.get("cleanup_proven"):
                with _ACTIVE_LOCK:
                    _ACTIVE.pop(root, None)
