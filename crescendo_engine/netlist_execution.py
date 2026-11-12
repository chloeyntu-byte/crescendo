"""Execute either private netlist producer through one solver/submission chain.

The optimizer still owns population, checkpoints, penalty policy and live rows.
This module owns only simulator tasks and native scalar evaluation.  Command
submission returns are not simulator completion evidence; an owned wrapper
must finish, Spectre must report zero errors, and Ocean must evaluate outputs.
"""
from __future__ import print_function

import concurrent.futures
import copy
import hashlib
import json
import math
import os
import re
import shlex
import shutil
import stat
import threading
import time
import uuid

from crescendo_engine.runtime import bind_text
_contract_text = bind_text()

from .cadence import child_environment, run_command
from .candidate_timing import ConcurrencyObserver, PendingSamples, elapsed, measure, summarize_tasks, timestamp
from .native_environment import scheduler_environment
from .native_log_io import OwnedLogReadersBusy, native_log_cleanup_scope
from .scheduler_cli import (scheduler_capability, scheduler_command_argv,
                            resolve_scheduler_binary)
from .scheduler_providers import (observation as parse_scheduler_observation,
                                  submission as parse_submit_streams)
from .scheduler_protocol import (parse_submit_job_id,
                                 valid_job_id, TERMINAL_STATES)
from .simulators import selected_simulator, submission_cpu, validate_execution_contract, build_simulator_argv
from .execution_commands import (ExecutionCommandError, normalize_execution_commands,
    build_solver_command_plan, validate_solver_command_plan, solver_bindings,
    build_submission_command, validate_submission_resources, resolve_submission_command,
    command_display)
from .completion import CompletionEvidenceError, UnsupportedCompletionEvidence, _SPECTRE_COMPLETION, _spectre_log_evidence, _alps_log_evidence, _tail_text
from .job_policy import normalize_job_overrides, job_timeout_policy
from .legacy_guardian import inspect_legacy_guardian_attempt, _validated_manifest as _validated_guardian_manifest
from .netlist_measurement import NetlistMeasurementError, evaluate_task_outputs, evaluate_task_outputs_batch, parsed_results, qualify_expressions, run_owned_command
from .utils import atomic_write_json, atomic_write_text, ensure_dir, is_under, sha256_file, utc_timestamp
from .solver_artifacts import collect_solver_artifacts
from .shared_cli_logs import retain_shared_cli_task_logs
from .owned_solver_logs import owned_solver_log_reader, OwnedSolverLogError


SCHEMA = _contract_text("crescendo.private_netlist_execution.v1")
COMPLETION_SCHEMA = _contract_text("crescendo.private_netlist_completion.v1")
RECEIPT = _contract_text("CRESCENDO_NETLIST_TASK_1")
SAFE_ID = re.compile(r"^[A-Za-z0-9_.-]+$")
_COMPANION_IDENTITY_FIELDS = ("source_path", "resolved_source_path", "leaf_link", "bytes", "mtime",
                              "source_device", "source_inode", "target_device", "target_inode")


class NetlistExecutionError(RuntimeError):
    def __init__(self, message, detail=None, infrastructure=True):
        super(NetlistExecutionError, self).__init__(message)
        self.detail = detail or {}
        self.infrastructure = infrastructure
        self.termination_failed = bool(self.detail.get("termination_failed"))


def _alps_companion_alias_snapshot(task, owned_root, launch_epoch):
    """Freeze narrow log aliases while the current task/result is still intact."""
    result = {"task_id": task["task_id"], "psf_dir": task["psf_dir"],
              "launch_epoch": launch_epoch, "aliases": [], "rejected": []}
    for path in (task["psf_dir"] + ".log", task["psf_dir"] + ".warn"):
        if not os.path.islink(path):
            continue
        try:
            with owned_solver_log_reader(path, owned_root, launch_epoch, task) as (unused, metadata):
                result["aliases"].append(dict(metadata))
        except (OSError, OwnedSolverLogError) as error:
            result["rejected"].append({"path": path, "reason": getattr(error, "reason", type(error).__name__)})
    return result


def _unlink_verified_alps_companion_aliases(task, owned_root, evidence):
    """Unlink proven leaf aliases only; all other tree links remain prohibited.

    The caller has already obtained point durability/termination proofs and
    holds the native-log cleanup barrier. This function grants no such proof.
    """
    if task.get("execution_simulator") != "alps":
        return []
    paths = (task["psf_dir"] + ".log", task["psf_dir"] + ".warn")
    linked = [path for path in paths if os.path.islink(path)]
    if not linked:
        return []
    evidence = evidence or {}
    if (evidence.get("task_id") != task["task_id"] or evidence.get("psf_dir") != task["psf_dir"]
            or evidence.get("launch_epoch") is None or evidence.get("rejected")):
        raise NetlistExecutionError("ALPS companion cleanup identity is unavailable; raw data retained")
    aliases = {row.get("source_path"): row for row in evidence.get("aliases") or []}
    verified = []
    # Verify every current alias before mutating any; partial or changed proof
    # must not turn into a broad unlink/cleanup attempt.
    for path in linked:
        frozen = aliases.get(path)
        if not frozen or not frozen.get("leaf_link"):
            raise NetlistExecutionError("ALPS companion alias was not frozen for this task")
        try:
            with owned_solver_log_reader(path, owned_root, evidence["launch_epoch"], task) as (unused, current):
                if any(frozen.get(key) != current.get(key) for key in _COMPANION_IDENTITY_FIELDS):
                    raise NetlistExecutionError("ALPS companion source or target changed; raw data retained")
                verified.append((path, dict(current)))
        except (OSError, OwnedSolverLogError) as error:
            raise NetlistExecutionError("ALPS companion alias cleanup is unsafe; raw data retained: " + str(error))
    task_root = os.path.dirname(task["psf_dir"])
    descriptor = os.open(task_root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0))
    removed = []
    try:
        directory = os.fstat(descriptor)
        if not os.path.samestat(directory, os.lstat(task_root)):
            raise NetlistExecutionError("ALPS companion parent changed before cleanup")
        for path, frozen in verified:
            current = os.stat(os.path.basename(path), dir_fd=descriptor, follow_symlinks=False)
            if (not stat.S_ISLNK(current.st_mode) or current.st_dev != frozen["source_device"]
                    or current.st_ino != frozen["source_inode"]):
                raise NetlistExecutionError("ALPS companion alias changed before unlink; raw data retained")
        for path, unused in verified:
            os.unlink(os.path.basename(path), dir_fd=descriptor)
            removed.append(os.path.basename(path))
        if not os.path.samestat(directory, os.lstat(task_root)):
            raise NetlistExecutionError("ALPS companion parent changed during cleanup")
    finally:
        os.close(descriptor)
    return removed


def _positive_int(value, default=None):
    if value in (None, ""):
        return default
    if isinstance(value, bool):
        raise ValueError("max jobs must be a positive integer")
    numeric = float(value)
    if not math.isfinite(numeric) or numeric < 1 or int(numeric) != numeric:
        raise ValueError("max jobs must be a positive integer")
    return int(numeric)


def execution_policy(prepared, spec, config):
    """Inherit actual Maestro values; never interpret an opaque shell command."""
    inherited = (prepared or {}).get("job_policy") or {}
    from .test_selection import selection_plan
    setup = (prepared or {}).get("setup") or {}
    selection = selection_plan(setup, spec)
    active_tests = (set(selection["selected"]) if selection is not None else
                    {row["name"] for row in setup.get("tests") or [] if row.get("enabled", True)})
    test_policies = [row for row in inherited.get("tests") or []
                     if "tests" not in setup or row.get("name") in active_tests]
    properties = copy.deepcopy((inherited.get("global") or {}).get("properties") or {})
    overrides = normalize_job_overrides((spec or {}).get("maestro_job_overrides"))
    commands = normalize_execution_commands(config, spec)
    runtime = config.get("runtime") or {}
    scheduler = spec.get("command_scheduler", runtime.get("command_scheduler", "auto"))
    from .runtime import scheduler_mode
    try:
        scheduler_mode(scheduler)
    except ValueError as error:
        raise NetlistExecutionError(str(error), {"failure_stage": "job_policy"})
    cluster_default = _positive_int(runtime.get("max_parallel_jobs"), 5)
    local_requested = _positive_int(spec.get("local_solver_max_jobs", runtime.get("local_solver_max_jobs")), 1)
    inherited_max = _positive_int(properties.get("maxjobs"), 1)
    configured_max = _positive_int(overrides.get("max_jobs"), inherited_max)

    def distribution_for(row):
        if commands["submission_template"]:
            return "command"
        if "command" in overrides:
            return "command" if overrides["command"].strip() else "local"
        return str(row.get("distributionmethod") or "local").strip().lower()

    any_remote = any(distribution_for(row) == "command" for row in
        [properties] + [dict(properties, **(test.get("properties") or {})) for test in test_policies])
    # Only selected effective Test policies can require Cluster commands.
    by_test = {test.get("name"): test.get("properties") or {} for test in test_policies}
    effective_sources = ([dict(properties, **by_test.get(name, {})) for name in active_tests]
                         if "tests" in setup else [properties] + [dict(properties, **row) for row in by_test.values()])
    if any(distribution_for(row) == "command" for row in effective_sources):
        from .scheduler_providers import get_provider
        from .native_environment import scheduler_environment
        context = None
        if callable(getattr(get_provider(), 'probe_context', None)):
            probe_config = dict(config)
            probe_config.setdefault('cadence', {})
            context = {'config': config, 'spec': spec, 'prepared': prepared,
                       'environment': scheduler_environment(child_environment(probe_config, home=os.environ.get("HOME") or os.getcwd()))}
        capability = scheduler_capability(commands, context)
        if not capability["available"] and not capability.get("receipt_only"):
            raise NetlistExecutionError(capability["reason"], {"failure_stage": "job_policy"})
    global_max = configured_max
    capacity_source = "manual_override" if "max_jobs" in overrides else "maestro_job_setup"
    if any_remote and "max_jobs" not in overrides and inherited_max <= 1:
        global_max, capacity_source = cluster_default, _contract_text("cr_cluster_parallel_default")

    def effective(original, scope):
        row = copy.deepcopy(original)
        if "command" in overrides:
            row["jobsubmitcommand"] = overrides["command"]
            row["distributionmethod"] = "command" if overrides["command"].strip() else "local"
        if "max_jobs" in overrides:
            row["maxjobs"] = overrides["max_jobs"]
        distribution = str(row.get("distributionmethod") or "local").strip().lower()
        if commands["submission_template"]:
            distribution = "command"
        if distribution not in ("local", "command", ""):
            raise NetlistExecutionError("Private netlist execution supports Local or Command job distribution",
                                        {"distribution": distribution})
        command = str(row.get("jobsubmitcommand") or "")
        argv = []
        if distribution == "command" and not commands["submission_template"]:
            if not command.strip():
                raise NetlistExecutionError("Command job distribution has no submission command")
            try:
                argv = shlex.split(command)
            except ValueError as error:
                raise NetlistExecutionError("Submission command quoting is invalid", {"error": str(error)})
            # The UI accepts a command prefix, as does Maestro Job Setup. Shell
            # programs/redirections require a real executable wrapper instead.
            if not argv or any(value in ("|", "||", "&&", ";", "&", ">", ">>", "<") for value in argv):
                raise NetlistExecutionError("Submission command must be an executable prefix")
        limit = _positive_int(row.get("maxjobs"), global_max)
        if distribution == "command" and "max_jobs" not in overrides and limit <= 1:
            limit = global_max
        timeouts = job_timeout_policy(original, overrides, {
            "run_seconds": spec.get("candidate_timeout_seconds", runtime.get("candidate_timeout_seconds", 600)),
        }, scope=scope)
        unknown = [name for name, value in timeouts["fields"].items()
                   if name == "run_seconds" and value["status"] == "unknown"]
        if unknown:
            raise NetlistExecutionError("Job Setup timeout values are unqualified; provide explicit seconds overrides", {
                "failure_stage": "job_policy", "scope": scope, "unknown_timeout_fields": unknown, "timeouts": timeouts})
        for field in ("start_seconds", "configure_seconds", "linger_seconds"):
            timeouts["fields"][field].update(applicable=False, effective_cli_seconds=None,
                cli_status="not_applicable_no_reusable_icrp" if field == "linger_seconds" else "native_maestro_capture_only")
        timeouts["phase_mapping"] = {
            "start": "native Maestro ICRP startup; private capture only",
            "configure": "native Maestro ICRP configuration; private capture only",
            "pending": "CLI queue has no inherited Job Setup timeout",
            "run": "accumulated exact Scheduler RUNNING intervals; Local owned solver-start until terminal receipt",
            "linger": "source ICRP idle retention; CLI job exits immediately after its solver",
            "source_run_scope": "native ICRP preparation, solve, evaluation and result delivery",
            "cli_run_scope": "CLI Run excludes queue PENDING and local Ocean measurement; repeated RUNNING intervals accumulate",
            "run_priority": "Maestro/manual run limit replaces candidate solver fallback; -1 disables solver time limit",
            "remote_run_authority": "submission-side accumulated RUNNING clock requests exact JobID cancellation; compute retains owner heartbeat and cancel watchdog",
        }
        return {"distribution": "command" if distribution == "command" or commands["submission_template"] else "local",
                "command": command, "submit_argv": argv,
                "submission_template": commands["submission_template"],
                "max_jobs": min(limit, global_max), "configured_max_jobs": _positive_int(original.get("maxjobs"), inherited_max),
                "effective_solver_max_jobs": min(limit, global_max) if distribution == "command" else min(local_requested, limit, global_max),
                "solver_capacity": {"requested": global_max if distribution == "command" else local_requested,
                    "effective": min(limit, global_max) if distribution == "command" else min(local_requested, limit, global_max),
                    "reason": "global_and_test_caps" if distribution == "command" else
                              "local_default_serial" if local_requested == 1 else "explicit_local_global_and_test_caps"},
                "timeouts": timeouts}

    policy = {"global": effective(properties, "global"), "tests": {}, "max_jobs": global_max,
              "source": inherited.get("source"), "overrides": overrides,
              "capacity_policy": "shared_private_netlist_task_semaphore",
              "local_solver_max_jobs": local_requested, "configured_max_jobs": configured_max,
              "inherited_max_jobs": inherited_max, "capacity_source": capacity_source,
              "command_scheduler": scheduler,
              "cluster_parallel_default": cluster_default,
              "measurement_jobs": min(global_max, _positive_int(
                  spec.get("measurement_jobs", runtime.get("measurement_jobs")), min(2, global_max))),
              "measurement_requested": spec.get("measurement_jobs", runtime.get("measurement_jobs")),
              "measurement_capacity_policy": "bounded_local_ocean_pool",
              "measurement_timeout_seconds": spec.get("candidate_timeout_seconds", runtime.get("candidate_timeout_seconds", 600))}
    for test in test_policies:
        policy["tests"][test["name"]] = effective(dict(properties, **(test.get("properties") or {})), "test:" + test["name"])
    return policy


class TaskTimeoutClock(object):
    """Accumulate execution intervals; queued/requeued time never consumes Run."""
    def __init__(self, policy, submitted, job_id=None, remote=None):
        self.policy = copy.deepcopy(policy)
        self.submitted, self.running, self.started = submitted, None, None
        self.finished = None
        self.job_id, self.expiration = job_id, None
        self.remote = bool(job_id) if remote is None else bool(remote)
        self.run_since, self.accumulated_run, self.state_since = None, 0.0, submitted
        self.scheduler_state, self.running_source = None, None
        self.lock = threading.RLock()

    def _pause(self, now):
        if self.run_since is not None:
            self.accumulated_run += max(0, now - self.run_since)
            self.run_since = None

    def _run(self, now, source):
        if self.finished is None and self.run_since is None:
            self.run_since = now
            self.state_since = now
            self.running_source = source
            if self.running is None:
                self.running = now

    def observe(self, observation, now):
        if (observation and observation.get("job_id") == self.job_id
                and not any(observation.get(key) for key in ("invalid", "not_found"))
                and observation.get("reason") in (None, "exact_job_nonterminal", "exact_job_terminal")):
            with self.lock:
                state = observation.get("state")
                if state == "RUNNING":
                    self._run(now, "exact_scheduler_RUNNING")
                elif state == "PENDING":
                    self._pause(now)
                    if self.scheduler_state != "PENDING":
                        self.state_since = now
                elif observation.get("terminal") is True:
                    self._pause(now)
                    self.finished = now
                self.scheduler_state = state

    def started_receipt(self, now):
        with self.lock:
            if self.started is None:
                self.started = now
                # An owned compute-start receipt supplements a scheduler state
                # not sampled yet. A subsequent exact PENDING pauses its clock.
                if not self.remote or self.scheduler_state is None:
                    self._run(now, "owned_solver_start_receipt")

    def terminal_receipt(self, now):
        with self.lock:
            if self.finished is None:
                self._pause(now)
                self.finished = now

    def snapshot(self, now):
        with self.lock:
            run_elapsed = self.accumulated_run + (max(0, now - self.run_since) if self.run_since is not None else 0)
            pending = self.run_since is None and self.finished is None
            phase = "pending" if pending else "run"
            limit = self.policy["fields"]["run_seconds"]
            return {"schema_version": _contract_text("crescendo.task_timeouts.v1"), "phase": phase,
                "phase_elapsed_seconds": max(0, now - self.state_since) if pending else run_elapsed,
                "running_elapsed_seconds": run_elapsed, "running_source": self.running_source,
                "solver_finished": self.finished is not None,
                "limit_seconds": None if pending else limit["seconds"], "enabled": False if pending else limit["enabled"],
                "source": "cli_queue_unlimited" if pending else limit["source"], "job_id": self.job_id,
                "scheduler_state": self.scheduler_state,
                "effective_policy": self.policy, "expiration": self.expiration}

    def expired(self, now):
        with self.lock:
            state = self.snapshot(now)
            if self.finished is None and self.expiration is None and state["enabled"] and state["running_elapsed_seconds"] >= state["limit_seconds"]:
                self.expiration = {key: state[key] for key in
                    ("phase", "phase_elapsed_seconds", "running_elapsed_seconds", "limit_seconds", "source", "job_id")}
            return self.expiration


def _owned_file(path, root, label, must_exist=True):
    absolute = os.path.abspath(path)
    if not is_under(absolute, root) or not is_under(os.path.realpath(absolute), root):
        raise NetlistExecutionError("%s is outside the owned candidate" % label, {"path": absolute})
    if must_exist:
        try:
            mode = os.lstat(absolute).st_mode
        except OSError:
            raise NetlistExecutionError("%s is missing" % label, {"path": absolute})
        if not stat.S_ISREG(mode):
            raise NetlistExecutionError("%s is not a private regular file" % label, {"path": absolute})
    return absolute


def _generation_digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True,
                                    separators=(",", ":")).encode("utf-8")).hexdigest()


def compact_netlist_generation(generation, manifest):
    """Point-level proof references; full snapshots stay once per candidate."""
    if not isinstance(generation, dict):
        raise NetlistExecutionError("Netlist producer evidence must be an object")
    source = generation.get("mode") or generation.get("source_mode") or generation.get("source")
    # Point annotations (for example a numeric-DPAR fallback reason) are not
    # themselves a producer. Inherit the actual candidate producer before
    # compacting, instead of silently classifying a partial dict as template.
    inherited = manifest.get("netlist_generation") or {}
    generation = dict(inherited, **generation)
    source = source or generation.get("mode") or generation.get("source_mode") or generation.get("source")
    if source not in (None, "maestro", "maestro_netlist", "template", "private_netlist", "netlist"):
        raise NetlistExecutionError("Netlist producer identity is unsupported")
    mode = "maestro" if source in ("maestro", "maestro_netlist") else "template"
    result = {"mode": mode, "source_mode": "maestro_netlist" if mode == "maestro" else "private_netlist",
              "netlist_method": generation.get("netlist_method"),
              "producer_manifest_sha256": manifest.get("manifest_sha256") or manifest.get("template_sha256"),
              "generation_fingerprint": generation.get("generation_fingerprint") or _generation_digest(generation)}
    source_identity = generation.get("source_identity") or manifest.get("source_identity") or {}
    private_identity = generation.get("private_identity") or {}
    result["source_identity_fingerprint"] = generation.get("source_identity_fingerprint") or source_identity.get("fingerprint")
    result["private_identity_fingerprint"] = generation.get("private_identity_fingerprint") or (_generation_digest(private_identity) if private_identity else None)
    for key in ("generation_protocol", "qualification_revision", "solver_invocations",
                "remote_submissions", "capture_only_wrapper_exit", "requested_backend",
                "native_generation_required", "template_qualified"):
        if key in generation:
            result[key] = generation[key]
    return result


def validate_execution_selection(manifest, expected, inherited=None):
    """A frozen input cannot be reused under a different Test/Analysis choice."""
    from .test_selection import SCHEMA
    observed = manifest.get("execution_selection")
    if observed is None and expected is None:
        return  # Existing source-inheritance callers and non-native fixtures.
    wanted = expected if expected is not None else inherited
    if not isinstance(observed, dict) or observed.get("schema_version") != SCHEMA or wanted is None:
        raise NetlistExecutionError("Netlist Test/Analysis selection evidence is unavailable", {"failure_stage": "execution_selection"})
    names = observed.get("selected")
    if (not isinstance(names, list) or not names or any(not isinstance(name, str) for name in names)
            or len(names) != len(set(names)) or set(names) != set(wanted.get("selected") or [])):
        raise NetlistExecutionError("Netlist Tests differ from this run's selection", {"failure_stage": "execution_selection"})
    for name in names:
        actual = (observed.get("analyses") or {}).get(name)
        desired = (wanted.get("analyses") or {}).get(name) or []
        if (not isinstance(actual, list) or any(not isinstance(item, str) for item in actual)
                or len(actual) != len(set(actual)) or set(actual) != set(desired)):
            raise NetlistExecutionError("Netlist analyses differ from this run's selection: " + name,
                                        {"failure_stage": "execution_selection"})
    if any(task.get("test") not in names for task in manifest.get("tasks") or []):
        raise NetlistExecutionError("Netlist contains an unselected Test", {"failure_stage": "execution_selection"})


def validate_candidate_manifest(manifest):
    from .native_qualification import require_current_native_qualification
    try:
        require_current_native_qualification(manifest)
    except ValueError as error:
        raise NetlistExecutionError(str(error), {"failure_stage": "native_point_qualification"})
    root = os.path.abspath(manifest.get("root") or manifest.get("candidate_root") or "")
    if root == os.path.dirname(root) or not os.path.isdir(root) or os.path.realpath(root) != root:
        raise NetlistExecutionError("Candidate root is not an owned real directory")
    candidate = manifest.get("candidate_id")
    if not isinstance(candidate, str) or not SAFE_ID.match(candidate) or candidate in (".", ".."):
        raise NetlistExecutionError("Candidate identity is invalid")
    tasks, seen = manifest.get("tasks") or [], set()
    if not tasks:
        raise NetlistExecutionError("Candidate has no Spectre tasks")
    for task in tasks:
        identifier = task.get("task_id")
        if not isinstance(identifier, str) or not SAFE_ID.match(identifier) or identifier in (".", "..") or identifier in seen:
            raise NetlistExecutionError("Task identity is missing or duplicated")
        seen.add(identifier)
        if not isinstance(task.get("test"), str) or not task.get("test") or not isinstance(task.get("corner"), str):
            raise NetlistExecutionError("Test/corner identity is incomplete")
        if isinstance(task.get("point_id"), bool) or not isinstance(task.get("point_id"), int):
            raise NetlistExecutionError("Point identity is invalid")
        cwd = os.path.abspath(task.get("cwd") or "")
        if not os.path.isdir(cwd) or not is_under(os.path.realpath(cwd), root):
            raise NetlistExecutionError("Spectre working directory is outside the owned candidate")
        netlist = _owned_file(task["netlist_path"], root, "Candidate netlist")
        if not task.get("netlist_sha256") or sha256_file(netlist) != task["netlist_sha256"]:
            raise NetlistExecutionError("Candidate netlist content changed")
        for artifact, expected in (task.get("artifact_sha256") or {}).items():
            artifact = _owned_file(artifact, root, "Candidate text/metadata artifact")
            if not expected or sha256_file(artifact) != expected:
                raise NetlistExecutionError("Candidate parameter or metadata content changed", {"path": artifact})
        from .shared_maestro import validate_static_file_inputs
        try:
            validate_static_file_inputs(task)
        except (OSError,ValueError) as error:
            raise NetlistExecutionError(str(error),{"failure_stage":"native_file_input_identity"})
        contract = task.get("simulator_contract")
        if contract is not None:
            validate_execution_contract(contract)
            if task.get("execution_simulator") != contract["execution_simulator"] or task.get("argv", [None])[0] != contract["executable"]["path"]:
                raise NetlistExecutionError("Candidate simulator identity differs from its frozen contract")
            if task.get("solver_command_plan"):
                validate_solver_command_plan(task["solver_command_plan"], contract, task)
            elif contract["execution_simulator"] == "alps" and task["argv"] != build_simulator_argv(contract, [], task["netlist_path"], task["psf_dir"], task["spectre_log"], task.get("ahdl_dir")):
                raise NetlistExecutionError("ALPS argv differs from its qualified simulator contract")
        elif task.get("execution_simulator", "spectre") == "alps":
            raise NetlistExecutionError("ALPS execution requires a frozen simulator/model contract")
        _owned_file(task["spectre_log"], root, "Simulator log", must_exist=False)
        if task.get("ahdl_dir"):
            _owned_file(os.path.join(task["ahdl_dir"], "owned.path"), root, "Simulator AHDL directory", must_exist=False)
        _owned_file(os.path.join(task["psf_dir"], "logFile"), root, "PSF", must_exist=False)
        if os.path.lexists(task["spectre_log"]) or os.path.lexists(os.path.join(task["psf_dir"], "logFile")):
            raise NetlistExecutionError("Candidate already contains simulator results; use a fresh candidate directory")
        argv = task.get("argv")
        if not isinstance(argv, list) or len(argv) < 2 or any(not isinstance(arg, str) or "\x00" in arg for arg in argv):
            raise NetlistExecutionError("Spectre argv is invalid")
        # A native command is qualified upstream. At this boundary it must be
        # tied to this fresh netlist, not a source/history input with the same name.
        if netlist not in [os.path.abspath(os.path.join(cwd, arg)) for arg in argv[1:] if arg and not arg.startswith(("+", "-"))]:
            raise NetlistExecutionError("Spectre argv is not bound to the candidate netlist")
    return root


def render_task_wrapper(task, root, token, environment, timeout_seconds):
    """POSIX compute-side wrapper; no Python/Tk dependency on compute nodes."""
    control = ensure_dir(os.path.join(os.path.dirname(task["psf_dir"]), "execution"))
    files = dict((name, os.path.join(control, name)) for name in (
        "started.receipt", "terminal.receipt", "cancel", "timed_out", "owner_heartbeat", "simulator.stdout", "simulator.stderr"))
    wrapper = os.path.join(control, "execute.sh")
    q = shlex.quote
    keys = ("PATH", "LD_LIBRARY_PATH", "CDS_LIC_FILE", "LM_LICENSE_FILE", "HOME",
            "CDS_AUTO_64BIT", "CDS_LOAD_ENV", "CDS_Netlisting_Mode")
    lines = ["#!/bin/sh", "umask 077", "cd %s || exit 72" % q(task["cwd"])]
    for key in keys:
        if key in environment:
            lines.append("export %s=%s" % (key, q(environment[key])))
    lines.extend([
        "mkdir %s 2>/dev/null || exit 73" % q(os.path.join(control, "once_" + token)),
        _contract_text("cr_token=%s") % q(token), _contract_text("cr_child="), _contract_text("cr_monitor="), _contract_text("cr_finished=0"),
        _contract_text('cr_compute_hostname=$(uname -n 2>/dev/null || :)'),
        _contract_text('cr_compute_uid=$(id -u 2>/dev/null || :)'),
        _contract_text("cr_cancel=%s") % q(files["cancel"]), _contract_text("cr_timeout=%s") % q(files["timed_out"]),
        _contract_text("cr_started=%s") % q(files["started.receipt"]), _contract_text("cr_terminal=%s") % q(files["terminal.receipt"]),
        _contract_text("cr_heartbeat=%s") % q(files["owner_heartbeat"]),
        _contract_text("cr_finish() {"), _contract_text("  cr_rc=$1"), _contract_text('  [ "$cr_finished" -eq 0 ] || return'), _contract_text("  cr_finished=1"),
        _contract_text('  if [ -n "$cr_monitor" ]; then kill "$cr_monitor" 2>/dev/null || :; wait "$cr_monitor" 2>/dev/null || :; fi'),
        _contract_text("  cr_clean=1"),
        # Native Spectre launchers can exit just before an adopted child is
        # reaped. Give the owned process group a bounded grace period; a group
        # still present afterwards remains unproven, including remote jobs.
        _contract_text("  cr_reap_wait=0"),
        _contract_text('  while [ -n "$cr_child" ] && /bin/kill -0 -- "-$cr_child" 2>/dev/null && [ "$cr_reap_wait" -lt 10 ]; do'),
        _contract_text('    sleep 0.1; cr_reap_wait=$((cr_reap_wait+1))'), "  done",
        _contract_text('  if [ -n "$cr_child" ] && /bin/kill -0 -- "-$cr_child" 2>/dev/null; then cr_clean=0; fi'),
        _contract_text('  printf "%s\\t%s\\texited\\t%s\\t%s\\t%s\\t%s\\n" %s "$cr_token" "$cr_rc" "$cr_clean" "$cr_compute_hostname" "$cr_compute_uid" > "$cr_terminal.tmp"') % ("%s", "%s", "%s", "%s", "%s", "%s", q(RECEIPT)),
        _contract_text('  mv "$cr_terminal.tmp" "$cr_terminal"'), "}",
        _contract_text("cr_interrupt() {"), "  trap - HUP INT TERM",
        _contract_text('  if [ -n "$cr_child" ]; then'),
        _contract_text('    /bin/kill -TERM -- "-$cr_child" 2>/dev/null || :'),
        '    sleep 1', _contract_text('    /bin/kill -KILL -- "-$cr_child" 2>/dev/null || :'),
        _contract_text('    wait "$cr_child" 2>/dev/null || :'), "  fi",
        _contract_text("  cr_finish 143"), "  exit 143", "}", _contract_text("trap 'cr_interrupt' HUP INT TERM"),
        _contract_text('if [ -f "$cr_cancel" ]; then cr_finish 125; exit 125; fi'),
        _contract_text('cr_stamp=$(stat -c %Y "$cr_heartbeat" 2>/dev/null) || { cr_finish 125; exit 125; }'),
        _contract_text('cr_now=$(date +%s)'),
        _contract_text('if [ "$((cr_now-cr_stamp))" -gt 300 ]; then cr_finish 125; exit 125; fi'),
        _contract_text('command -v setsid >/dev/null 2>&1 || { cr_finish 127; exit 127; }'),
        _contract_text('printf "%s\\t%s\\tstarted\\t%s\\t%s\\t%s\\n" %s "$cr_token" "$$" "$cr_compute_hostname" "$cr_compute_uid" > "$cr_started.tmp"') % ("%s", "%s", "%s", "%s", "%s", q(RECEIPT)),
        _contract_text('mv "$cr_started.tmp" "$cr_started"'),
        "setsid %s > %s 2> %s &" % (" ".join(q(arg) for arg in task["argv"]),
                                   q(files["simulator.stdout"]), q(files["simulator.stderr"])),
        _contract_text("cr_child=$!"), _contract_text("cr_parent=$$"), _contract_text("cr_start=$(date +%s)"), "(", "  while :; do",
        _contract_text('    if [ -f "$cr_cancel" ]; then kill -TERM "$cr_parent"; exit; fi'),
        _contract_text('    cr_now=$(date +%s)'),
        _contract_text('    cr_stamp=$(stat -c %Y "$cr_heartbeat" 2>/dev/null) || { kill -TERM "$cr_parent"; exit; }'),
        _contract_text('    if [ "$((cr_now-cr_stamp))" -gt 300 ]; then kill -TERM "$cr_parent"; exit; fi'),
        (_contract_text('    if [ "$((cr_now-cr_start))" -ge %d ]; then : > "$cr_timeout"; kill -TERM "$cr_parent"; exit; fi') % max(1, int(math.ceil(timeout_seconds)))) if timeout_seconds is not None else '    : # solver run timeout disabled; owned heartbeat and cancellation remain active',
        "    sleep 0.2", "  done", ") &", _contract_text("cr_monitor=$!"),
        _contract_text('wait "$cr_child"'), _contract_text("cr_rc=$?"), _contract_text("cr_finish $cr_rc"), "trap - HUP INT TERM", _contract_text("exit $cr_rc"), "",
    ])
    atomic_write_text(wrapper, "\n".join(lines))
    atomic_write_text(files["owner_heartbeat"], token + "\n")
    os.chmod(wrapper, 0o700)
    return wrapper, files


def read_task_receipt(path, token, terminal=False):
    if not os.path.isfile(path) or os.path.islink(path) or os.path.getsize(path) > 4096:
        return None
    with open(path, "r", encoding="utf-8") as stream:
        fields = stream.read().strip().split("\t")
    expected_count = 5 if terminal else 4
    if len(fields) not in (expected_count, expected_count + 1, expected_count + 2) or fields[:2] != [RECEIPT, token]:
        raise NetlistExecutionError("Task receipt belongs to another attempt", {"path": path})
    compute = {}
    if len(fields) > expected_count:
        compute["compute_hostname"] = fields[expected_count] or None
    if len(fields) > expected_count + 1:
        compute["compute_uid"] = (int(fields[expected_count + 1])
                                  if fields[expected_count + 1].isdigit() else None)
    if terminal:
        if fields[2] != "exited" or fields[4] not in ("0", "1"):
            raise NetlistExecutionError("Task terminal receipt is malformed")
        try:
            returncode = int(fields[3])
        except ValueError:
            raise NetlistExecutionError("Task return code is malformed")
        return dict(compute, token=token, returncode=returncode, cleanup_proven=fields[4] == "1")
    if fields[2] != "started" or not fields[3].isdigit():
        raise NetlistExecutionError("Task start receipt is malformed")
    return dict(compute, token=token, pid=int(fields[3]))


def failed_spectre_evidence(task, files, terminal):
    """Only a native simulator error summary qualifies a design failure.

    Missing licenses, launch failures and signals are infrastructure failures;
    a nonzero scheduler/launcher exit alone says nothing about circuit merit.
    Reads are bounded and restricted to the already-owned task paths.
    """
    returncode = terminal.get("returncode") if terminal else None
    texts = []
    for path in (task["spectre_log"], files["simulator.stdout"], files["simulator.stderr"]):
        if os.path.isfile(path) and not os.path.islink(path):
            texts.append(_tail_text(path))
    text = "\n".join(texts)
    if re.search(r"(?:license|licence).{0,100}(?:fail|denied|unavailable|cannot|can't|not found)|(?:cannot|can't).{0,60}(?:license|licence)|ERROR\s*\(LMC-", text, re.I):
        return {"infrastructure": True, "reason": "spectre_license_failure", "returncode": returncode}
    if returncode in (124, 125, 126, 127, 132, 133, 134, 135, 136, 137, 139, 143):
        return {"infrastructure": True, "reason": "native_launcher_or_process_failure", "returncode": returncode}
    if task.get("execution_simulator", "spectre") == "alps":
        return {"infrastructure": True, "reason": "alps_nonzero_exit_requires_native_error_adapter", "returncode": returncode}
    summaries = list(_SPECTRE_COMPLETION.finditer(text))
    if summaries and int(summaries[-1].group(1)) > 0:
        return {"infrastructure": False, "reason": "native_simulator_errors", "error_count": int(summaries[-1].group(1)), "returncode": returncode}
    return {"infrastructure": True, "reason": "native_failure_without_completion_summary", "returncode": returncode}


def scheduler_job_id(text):
    """Compatibility entry for the shared exact submission-record parser."""
    return parse_submit_job_id(text)


def scheduler_terminal_state(text, expected_job_id):
    """Return only a valid exact job terminal observation, never disappearance."""
    observation = parse_scheduler_observation(text, expected_job_id)
    return observation["state"] if observation["terminal"] else None


def _scheduler_command_ok(command):
    return command.get("returncode") == 0 and not isinstance(command.get("returncode"), bool) and not any(command.get(key) for key in
        ("timed_out", "cancelled", "termination_failed", "output_limited", "error"))


def task_termination_evidence(row, terminal):
    """Join exact local guardian or owned remote terminal proof to a receipt.

    Shell kill -0 also sees zombies. A current local subreaper guardian may
    independently prove their full cleanup; a login-node submission guardian
    never proves a remote compute job stopped.
    """
    remote, token = bool(row.get("remote")), row.get("token")
    evidence = {"cleanup_proven": bool(terminal and terminal.get("cleanup_proven")),
                "source": "owned_compute_receipt", "remote": remote,
                "receipt_token": token,
                "raw_cleanup_proven": bool(terminal and terminal.get("cleanup_proven"))}
    if terminal is not None and terminal.get("token") != token:
        evidence.update(cleanup_proven=False, source="receipt_identity_unproven", reason="terminal_receipt_token_differs")
        return evidence
    if evidence["cleanup_proven"]:
        return evidence
    if remote:
        identifier = row.get("scheduler_job_id")
        submission = row.get("submission_observation") or {}
        record = row.get("scheduler_observation") or {}
        stop = row.get("scheduler_stop") or {}
        expected_task = (row.get("task") or {}).get("task_id")
        observation, query = None, None
        if (record.get("schema_version") == _contract_text("crescendo.scheduler_task_tracking.v1")
                and record.get("job_id") == identifier and record.get("receipt_token") == token
                and isinstance(expected_task, str) and expected_task and record.get("task_id") == expected_task):
            observation, query = record.get("terminal_observation"), record.get("terminal_query")
        if observation is None and stop.get("job_id") == identifier and stop.get("remote_stop_proven") is True:
            observation, query = stop.get("query_observation"), stop.get("query")
        if (valid_job_id(identifier) and isinstance(token, str) and re.fullmatch(r"[0-9a-f]{32}", token)
                and submission.get("accepted") is True
                and submission.get("job_id") == identifier and isinstance(observation, dict)
                and isinstance(query, dict) and _scheduler_command_ok(query)
                and observation.get("schema_version") == _contract_text("crescendo.scheduler_observation.v1")
                and observation.get("observed_job_ids") == [identifier]
                and observation.get("expected_job_id") == identifier and observation.get("job_id") == identifier
                and observation.get("terminal") is True and not observation.get("invalid")
                and not observation.get("not_found") and observation.get("state") in TERMINAL_STATES
                and isinstance(observation.get("task_states"), dict)
                and all(isinstance(states, list) and states and all(isinstance(state, str) for state in states)
                        and set(states) <= TERMINAL_STATES
                        for states in (observation.get("task_states") or {}).values())):
            evidence.update(cleanup_proven=True, source="owned_scheduler_terminal", scheduler={
                "job_id": identifier, "query_returncode": query["returncode"],
                "observation": observation, "native_completion_authority": False})
            return evidence
        evidence.update(source="remote_cleanup_unproven", reason="local_submission_guardian_cannot_prove_remote_job_termination")
        return evidence
    evidence["source"] = "local_cleanup_unproven"
    files, task = row.get("files") or {}, row.get("task") or {}
    receipt_path, cwd = files.get("terminal.receipt"), task.get("cwd")
    if not isinstance(token, str) or not re.match(r"^[0-9a-f]{32}$", token) or not isinstance(receipt_path, str) or not isinstance(cwd, str):
        evidence["reason"] = "local_task_ownership_is_incomplete"
        return evidence
    attempt = os.path.dirname(receipt_path)
    expected_argv = ["/bin/sh", os.path.join(attempt, "execute.sh")]
    plan = row.get("submission_command_plan") or {}
    if plan and (plan.get("remote") is not False or plan.get("argv") != expected_argv):
        evidence["reason"] = "local_job_script_command_identity_differs"
        return evidence
    expected_token = _contract_text("cr_netlist_task_%s") % token
    expected_identity = {"legacy": True, "token": expected_token}
    inspection = inspect_legacy_guardian_attempt(attempt, expected_workspace_root=cwd,
                                               expected_token_identity=expected_identity)
    proof = inspection.get("termination_proof") or {}
    if inspection.get("state") != "dead" or proof.get("ok") is not True or proof.get("kind") != "guardian_cleanup_proven":
        evidence["reason"] = "exact_current_local_guardian_termination_is_unproven"
        evidence["guardian_inspection"] = inspection
        return evidence
    try:
        manifest_path, manifest = _validated_guardian_manifest(attempt, cwd, expected_identity)
        if manifest["argv"] != expected_argv:
            raise ValueError("local guardian command differs from the owned job script")
        with open(manifest["state_path"], "r", encoding="utf-8") as stream:
            state = json.load(stream)
        if (state.get("token") != expected_token or state.get("argv") != expected_argv or
                state.get("status") != "exited" or
                (terminal and state.get("returncode") != terminal.get("returncode"))):
            raise ValueError("local guardian terminal identity/exit differs from its receipt")
        if any(isinstance(state.get(key), bool) or not isinstance(state.get(key), int) or state[key] <= 0
               for key in ("pid", "pid_start_ticks", "guardian_pid", "guardian_start_ticks")):
            raise ValueError("local guardian process identities are incomplete")
        guardian = {key: state.get(key) for key in ("pid", "pid_start_ticks", "guardian_pid",
                    "guardian_start_ticks", "launch_boot_id", "returncode")}
        guardian.update(state="dead", termination_proof=proof, token=expected_token,
                        argv=expected_argv, cwd=cwd, expected_uid=manifest.get("expected_uid"),
                        manifest_path=manifest_path, state_path=manifest["state_path"],
                        manifest_sha256=sha256_file(manifest_path), state_sha256=sha256_file(manifest["state_path"]))
    except (ValueError, TypeError, OSError, KeyError) as error:
        evidence["reason"] = "local_guardian_binding_failed: " + str(error)
        return evidence
    evidence.update(cleanup_proven=True, source="owned_local_guardian", guardian=guardian)
    return evidence


def effective_terminal_receipt(terminal, evidence):
    if terminal is None:
        return None
    result = copy.deepcopy(terminal)
    result.update(raw_cleanup_proven=terminal.get("cleanup_proven") is True,
                  cleanup_proven=evidence.get("cleanup_proven") is True,
                  cleanup_basis=evidence.get("source"))
    return result


def _cleanup_dependencies(row, boundary):
    proofs = []
    for dependency in row.get("cleanup_dependencies") or []:
        attempt, cwd, token = dependency.get("attempt_root"), dependency.get("cwd"), dependency.get("token")
        if not all(isinstance(value, str) for value in (attempt, cwd, token)):
            raise NetlistExecutionError("Additional cleanup dependency is malformed")
        if not is_under(os.path.realpath(attempt), boundary) or not is_under(os.path.realpath(cwd), boundary):
            raise NetlistExecutionError("Additional cleanup dependency escapes its owned run")
        proof = inspect_legacy_guardian_attempt(attempt, expected_workspace_root=cwd,
                                               expected_token_identity={"legacy": True, "token": token})
        proofs.append(proof)
    return all(proof.get("state") == "dead" for proof in proofs), proofs


def reconcile_previous_ownership(ownership_root, owned_run_root, request_cancel=True):
    """Prove old task cleanup before checkpoint resume can submit another job.

    The caller supplies the previously verified origin run boundary. This never
    treats a saved path, a dead login-node CLI or a cancel request as remote
    termination. Unknown tasks keep their original files and ownership tokens.
    """
    ownership_root = os.path.realpath(os.path.abspath(ownership_root))
    boundary = os.path.realpath(os.path.abspath(owned_run_root))
    if not is_under(ownership_root, boundary):
        raise NetlistExecutionError("Ownership records are outside the verified run boundary")
    path = os.path.join(ownership_root, _contract_text("ownership.json"))
    if not os.path.lexists(path):
        return {"cleanup_proven": True, "active_task_count": 0, "retained": [], "path": path,
                "reason": "no_previous_private_netlist_ownership"}
    if os.path.islink(path) or not os.path.isfile(path) or os.path.getsize(path) > 16 * 1024 * 1024:
        raise NetlistExecutionError("Previous private netlist ownership record is unsafe")
    with open(path, "r", encoding="utf-8") as stream:
        previous = json.load(stream)
    if not isinstance(previous, dict) or previous.get("schema_version") != _contract_text("crescendo.private_netlist_ownership.v1"):
        raise NetlistExecutionError("Previous private netlist ownership schema is invalid")
    if os.path.realpath(previous.get("owned_run_root") or "") != boundary:
        raise NetlistExecutionError("Previous ownership record identifies another run boundary")
    rows = dict(previous.get("active") or {})
    rows.update(previous.get("unresolved") or {})
    retained, proofs = [], []
    for key, row in rows.items():
        files, token = row.get("files") or {}, row.get("token")
        if not isinstance(token, str) or not re.match(r"^[0-9a-f]{32}$", token):
            raise NetlistExecutionError("Previous private netlist task token is invalid")
        for name in ("terminal.receipt", "cancel", "owner_heartbeat"):
            value = files.get(name)
            if not isinstance(value, str) or not is_under(os.path.realpath(value), boundary):
                raise NetlistExecutionError("Previous task ownership path escapes the verified run")
        cwd = (row.get("task") or {}).get("cwd")
        if not isinstance(cwd, str) or not is_under(os.path.realpath(cwd), boundary):
            raise NetlistExecutionError("Previous task working directory escapes the verified run")
        terminal = read_task_receipt(files["terminal.receipt"], token, terminal=True)
        clean = bool(terminal and terminal["cleanup_proven"])
        proof = {"identity": key, "token": token, "remote": bool(row.get("remote")), "receipt": terminal}
        if row.get("remote") and not clean:
            proof["termination_evidence"] = task_termination_evidence(row, terminal)
            clean = proof["termination_evidence"]["cleanup_proven"]
        if not row.get("remote") and not clean:
            inspection = inspect_legacy_guardian_attempt(
                os.path.dirname(files["terminal.receipt"]), expected_workspace_root=cwd,
                expected_token_identity={"legacy": True, "token": _contract_text("cr_netlist_task_%s") % token})
            clean = inspection.get("state") == "dead"
            proof["guardian"] = inspection
        dependencies_clean, dependencies = _cleanup_dependencies(row, boundary)
        clean = clean and dependencies_clean
        if dependencies:
            proof["dependencies"] = dependencies
        proof["cleanup_proven"] = clean
        proofs.append(proof)
        if not clean:
            if request_cancel:
                atomic_write_text(files["cancel"], "previous owner ended; cancellation requested\n")
            retained.append({"identity": key, "token": token, "remote": bool(row.get("remote")),
                             "path": os.path.dirname(files["terminal.receipt"]), "record": row})
    return {"cleanup_proven": not retained, "active_task_count": len(rows), "retained": retained,
            "path": path, "proofs": proofs}


def _ownership_projection(rows):
    """Persist termination identity, never duplicate the full output catalog."""
    result = {}
    for key, row in rows.items():
        task = row.get("task") or {}
        projected = dict((field, row[field]) for field in
                         ("token", "remote", "stage", "candidate_root", "scheduler_job_id", "command_scheduler",
                          "scheduler_observation", "submission_observation", "scheduler_stop") if field in row)
        if row.get("timeout_state"):
            projected["timeout_state"] = copy.deepcopy(row["timeout_state"])
        projected["task"] = dict((field, task[field]) for field in
                                 ("cwd", "task_id", "test", "corner", "point_id") if field in task)
        projected["files"] = dict(row.get("files") or {})
        if row.get("cleanup_dependencies"):
            # Native Ocean guardian dependencies are part of termination
            # proof. Preserve them in full even after Spectre has exited.
            projected["cleanup_dependencies"] = copy.deepcopy(row["cleanup_dependencies"])
        result[key] = projected
    return result


class NetlistExecutionContext(object):
    """One run's shared task capacity; reusable by candidate executor threads."""
    def __init__(self, config, prepared, spec, cancel_check=None, progress_callback=None,
                 command_runner=None, measurement_evaluator=None, ownership_root=None,
                 owned_run_root=None, prior_ownership_roots=None, point_commit_callback=None):
        self.config, self.spec = copy.deepcopy(config), copy.deepcopy(spec)
        from .test_selection import selection_plan
        setup = (prepared or {}).get("setup") or {}
        self.execution_selection = selection_plan(setup, spec)
        self.inherited_execution_selection = {"selected": [row["name"] for row in setup.get("tests") or [] if row.get("enabled", True)],
            "analyses": {row["name"]: [item["name"] for item in row.get("analyses") or [] if item.get("enabled", True)]
                         for row in setup.get("tests") or []}} if setup.get("tests") else None
        try:
            self.commands = normalize_execution_commands(config, spec)
        except ExecutionCommandError as error:
            raise NetlistExecutionError(str(error), {"failure_stage": "command_render"})
        self.policy = execution_policy(prepared, spec, config)
        self.cancel_check = cancel_check
        self.progress_callback = progress_callback
        self.point_commit_callback = point_commit_callback
        self.command_runner = command_runner or run_command
        self.measurement_evaluator = measurement_evaluator or evaluate_task_outputs
        self.batch_reader = measurement_evaluator is None and spec.get("reader_protocol") == _contract_text("crescendo.batch_reader.v2")
        self.capacity = threading.BoundedSemaphore(self.policy["max_jobs"])
        self.local_capacity = threading.BoundedSemaphore(self.policy["local_solver_max_jobs"])
        self.test_capacity = dict((name, threading.BoundedSemaphore(policy["max_jobs"]))
                                  for name, policy in self.policy["tests"].items())
        self.unsafe = threading.Event()
        self.cancelled = threading.Event()
        self.lock = threading.Lock()
        self.active = {}
        self.observation_lock = threading.Lock()
        self.concurrency = ConcurrencyObserver()
        self.unresolved = {}
        self.command_scheduler = self.policy["command_scheduler"]
        interval = float((config.get("runtime") or {}).get("scheduler_poll_interval_seconds", 2.0))
        if not math.isfinite(interval) or interval < 2.0:
            raise NetlistExecutionError("Scheduler poll interval must be finite and at least 2 seconds")
        self.scheduler_poll_interval = interval
        receipt_grace = float((config.get("runtime") or {}).get("scheduler_receipt_grace_seconds", 10.0))
        if not math.isfinite(receipt_grace) or not 0 <= receipt_grace <= 60:
            raise NetlistExecutionError("Scheduler receipt grace must be finite and between 0 and 60 seconds")
        self.scheduler_receipt_grace = receipt_grace
        stop_timeout = float((config.get("runtime") or {}).get("cluster_stop_timeout_seconds", 15))
        if not math.isfinite(stop_timeout) or not 0 < stop_timeout <= 300:
            raise NetlistExecutionError("Cluster stop timeout must be finite, positive and at most 300 seconds")
        self.scheduler_stop_timeout = stop_timeout
        self.ownership_root = os.path.realpath(os.path.abspath(ownership_root)) if ownership_root else None
        self.owned_run_root = os.path.realpath(os.path.abspath(owned_run_root)) if owned_run_root else None
        if self.ownership_root and not self.owned_run_root:
            raise NetlistExecutionError("Private netlist ownership requires an explicit run boundary")
        self.ownership_path = os.path.join(self.ownership_root, _contract_text("ownership.json")) if self.ownership_root else None
        self.owner_token = uuid.uuid4().hex
        self.heartbeat_stop = threading.Event()
        self.required_outputs = sorted(set(row["output_key"] for row in
            list(self.spec.get("objectives") or []) + list(self.spec.get("constraints") or []))) or None
        for prior in prior_ownership_roots or []:
            if not isinstance(prior, dict) or not prior.get("ownership_root") or not prior.get("owned_run_root"):
                raise NetlistExecutionError("Prior private netlist ownership boundary is incomplete")
            proof = reconcile_previous_ownership(prior["ownership_root"], prior["owned_run_root"])
            if not proof["cleanup_proven"]:
                raise NetlistExecutionError("Previous private netlist jobs have unproven cleanup; duplicate submission is blocked",
                                            dict(proof, termination_failed=True))
        self._recover_previous_ownership()
        self.task_pool = concurrent.futures.ThreadPoolExecutor(max_workers=self.policy["max_jobs"])
        self.measurement_jobs = self.policy["measurement_jobs"]
        self.measurement_pool = concurrent.futures.ThreadPoolExecutor(max_workers=self.measurement_jobs)
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_loop, name=_contract_text("crescendo-netlist-owner"), daemon=True)
        self.heartbeat_thread.start()

    def _persist_ownership_locked(self):
        if not self.ownership_path:
            return
        atomic_write_json(self.ownership_path, {
            "schema_version": _contract_text("crescendo.private_netlist_ownership.v1"), "owner_token": self.owner_token,
            "owned_run_root": self.owned_run_root,
            "updated_at": utc_timestamp(), "active": _ownership_projection(self.active),
            "unresolved": _ownership_projection(self.unresolved)})

    def _recover_previous_ownership(self):
        if not self.ownership_path or not os.path.lexists(self.ownership_path):
            return
        proof = reconcile_previous_ownership(self.ownership_root, self.owned_run_root)
        if not proof["cleanup_proven"]:
            self.unsafe.set()
            raise NetlistExecutionError("Previous private netlist jobs have unproven cleanup; retained evidence blocks duplicate submission", {
                "termination_failed": True, "path": self.ownership_path,
                "tasks": [row["identity"] for row in proof["retained"]], "proof": proof})
        self._persist_ownership_locked()

    def _heartbeat_loop(self):
        while not self.heartbeat_stop.wait(2.0):
            with self.lock:
                rows = list(self.active.items())
            for key, row in rows:
                if row.get("stage", "spectre") not in ("submit", "solve", "spectre"):
                    continue
                path = row["files"]["owner_heartbeat"]
                try:
                    if not os.path.isfile(path) or os.path.islink(path):
                        raise OSError("owned task heartbeat is not a regular file")
                    os.utime(path, None)
                except OSError:
                    with self.lock:
                        if self.active.get(key) is not row or row.get("stage", "spectre") not in ("submit", "solve", "spectre"):
                            continue
                    self.unsafe.set()
                    try:
                        atomic_write_text(row["files"]["cancel"], "owner heartbeat failed\n")
                    except OSError:
                        pass

    def _cancelled(self, additional=None):
        return self.unsafe.is_set() or self.cancelled.is_set() or bool(self.cancel_check and self.cancel_check()) or bool(additional and additional())

    def _emit(self, value):
        with self.observation_lock:
            self.concurrency.observe(value)
        if self.progress_callback:
            self.progress_callback(value)

    def generation_capacity(self, generation):
        with self.observation_lock:
            return self.concurrency.snapshot(generation, self.policy)

    def _acquire(self, semaphore, cancel_check):
        while not semaphore.acquire(timeout=0.1):
            if self._cancelled(cancel_check):
                raise NetlistExecutionError("Candidate cancelled while waiting for job capacity", {"cancelled": True})
        if self._cancelled(cancel_check):
            semaphore.release()
            raise NetlistExecutionError("Candidate cancelled before submission", {"cancelled": True})

    def cancel(self):
        """Cooperative remote cancellation; retain artifacts until proof arrives."""
        self.cancelled.set()
        with self.lock:
            files = [row["files"]["cancel"] for row in list(self.active.values()) + list(self.unresolved.values())]
        for path in files:
            atomic_write_text(path, "cancel requested\n")

    def close(self, cancel=False):
        """Never deletes private evidence, PSFs, templates or optimization WAL."""
        if cancel:
            self.cancel()
        self.task_pool.shutdown(wait=True)
        self.measurement_pool.shutdown(wait=True)
        self.heartbeat_stop.set()
        self.heartbeat_thread.join(timeout=3)
        with self.lock:
            unresolved = list(self.unresolved.items())
        for key, row in unresolved:
            terminal = read_task_receipt(row["files"]["terminal.receipt"], row["token"], terminal=True)
            dependencies_clean, unused = _cleanup_dependencies(row, self.owned_run_root or row["candidate_root"])
            # A lost LOCAL receipt keeps the prior retention contract even if
            # its guardian is dead. Remote exact-ID terminal proof can release
            # a stopped job without manufacturing a missing compute receipt.
            if (terminal is not None or row.get("remote")) and task_termination_evidence(row, terminal)["cleanup_proven"] and dependencies_clean:
                with self.lock:
                    self.unresolved.pop(key, None)
        with self.lock:
            retained = [{"identity": key, "token": row["token"], "remote": row["remote"],
                         "path": os.path.dirname(row["files"]["terminal.receipt"])}
                        for key, row in sorted(self.unresolved.items())]
            active_count = len(self.active)
            self._persist_ownership_locked()
        return {"cleanup_proven": not retained and active_count == 0,
                "retained": retained, "active_task_count": active_count, "unsafe": self.unsafe.is_set()}

    def _provider_context(self, argv=None, task_root=None, env=None, deadline=None):
        from .scheduler_providers import get_provider
        if not callable(getattr(get_provider(), 'probe_context', None)):
            return None
        return {'config': self.config, 'spec': self.spec,
                'submit_argv': list(argv or []), 'root': task_root,
                'environment': scheduler_environment(env if env is not None else child_environment(self.config, home=os.environ.get("HOME") or os.getcwd())),
                'deadline': deadline, 'cancel': self.cancel_check,
                '_help_runner': self.command_runner}

    def _scheduler_submission(self, argv, task_root=None, env=None, deadline=None):
        """Select key-value tracking from explicit templates, never a CLI name."""
        from .runtime import scheduler_mode
        if not argv or scheduler_mode(self.command_scheduler) == "receipt":
            return False
        capability = scheduler_capability(self.commands, self._provider_context(argv, task_root, env, deadline))
        return capability['available'] or capability.get('submit_identity_required') is True

    def _scheduler_argv(self, kind, job_id, policy, task_root, env=None, deadline=None):
        values = dict(policy.get("scheduler_bindings") or {})
        values.setdefault("cwd", task_root)
        from .scheduler_providers import command
        return command(kind, self.commands, job_id, values, self._provider_context(policy.get("submit_argv"), task_root, env, deadline))

    def _scheduler_observe(self, policy, job_id, task_root, env, record, identity, final=False):
        """Observe one owned ID; scheduler state never authorizes solver success."""
        if not job_id:
            return record
        argv = policy.get("submit_argv") or []
        if not self._scheduler_submission(argv):
            return record
        if record is None:
            record = dict(identity, schema_version=_contract_text("crescendo.scheduler_task_tracking.v1"), job_id=job_id,
                samples=[], query_count=0, latest=None, native_completion_authority=False)
        else:
            record = copy.deepcopy(record)
        scheduler_env = scheduler_environment(env)
        capability = scheduler_capability(self.commands, self._provider_context(argv, task_root, env))
        record["capability"] = capability
        if not capability.get("query_available", capability["available"]):
            record["reason"] = capability["reason"]
        else:
            try:
                query = self.command_runner(self._scheduler_argv("query", job_id, policy, task_root),
                    cwd=task_root, env=scheduler_env, timeout_seconds=3,
                    stdout_path=os.path.join(task_root, "scheduler_status.stdout"),
                    stderr_path=os.path.join(task_root, "scheduler_status.stderr"),
                    termination_grace_seconds=1, termination_kill_seconds=1)
            except Exception as error:
                query = {"returncode": None, "error": type(error).__name__}
            text = (query.get("stdout_tail") or "") + "\n" + (query.get("stderr_tail") or "")
            observation = parse_scheduler_observation(text, job_id)
            if not _scheduler_command_ok(query):
                observation.update(terminal=False, reason="scheduler_query_failed")
            previous = record.get("latest") or {}
            changed = any(previous.get(key) != observation.get(key) for key in
                ("state", "terminal", "not_found", "invalid", "job_exit_code", "metadata", "reason"))
            record["query_count"] += 1
            record["latest"] = observation
            if observation["terminal"]:
                record["terminal_observation"] = observation
                record["terminal_query"] = dict((key, query.get(key)) for key in
                    ("returncode", "timed_out", "cancelled", "termination_failed", "output_limited", "error"))
            if not record["samples"] or changed or final:
                sample = {"observed_at": utc_timestamp(), "observation": observation,
                          "query_returncode": query.get("returncode"), "final": bool(final)}
                record["samples"].append(sample)
                if len(record["samples"]) > 32:
                    record["samples"] = record["samples"][:1] + record["samples"][-31:]
                self._emit(dict(identity, stage="scheduler_status", scheduler_job_id=job_id,
                    job_id=job_id, scheduler_state=observation.get("state"),
                    scheduler_observation=observation,
                    message="Scheduler %s; simulator completion requires the owned receipt and result evidence" % (
                        observation.get("state") or ("NOT_FOUND" if observation.get("not_found") else "UNKNOWN"))))
            record["final"] = bool(final)
        atomic_write_json(os.path.join(task_root, _contract_text("scheduler_observation.json")), record)
        return record

    def _scheduler_cancel(self, policy, submit_result, task_root, env):
        stop_started = time.monotonic()
        stop_deadline = stop_started + self.scheduler_stop_timeout
        env = scheduler_environment(env)
        argv = policy.get("submit_argv") or []
        from .runtime import scheduler_mode
        if not argv or scheduler_mode(self.command_scheduler) == "receipt":
            return {"attempted": False, "remote_stop_proven": False, "reason": "scheduler_cancel_adapter_unavailable"}
        job_id = parse_submit_streams(submit_result.get("stdout_tail"), submit_result.get("stderr_tail"))["job_id"]
        if not job_id:
            return {"attempted": False, "remote_stop_proven": False, "reason": "scheduler_submission_id_unavailable"}
        capability = scheduler_capability(self.commands, self._provider_context(argv, task_root, env, stop_deadline))
        if not capability["available"]:
            return {"attempted": False, "remote_stop_proven": False, "job_id": job_id,
                    "reason": capability["reason"], "capability": capability}
        remaining = stop_deadline - time.monotonic()
        if remaining <= 0:
            return {"attempted": False, "remote_stop_proven": False, "job_id": job_id,
                    "reason": "scheduler_stop_budget_expired", "timeout_seconds": self.scheduler_stop_timeout}
        try:
            cancel = self.command_runner(self._scheduler_argv("cancel", job_id, policy, task_root, env, stop_deadline),
                cwd=task_root, env=env, timeout_seconds=min(10, remaining),
                stdout_path=os.path.join(task_root, "scheduler_cancel.stdout"),
                stderr_path=os.path.join(task_root, "scheduler_cancel.stderr"),
                termination_grace_seconds=1, termination_kill_seconds=1)
        except Exception as error:
            cancel = {"returncode": None, "error": type(error).__name__}
        queries, observation, query_count = [], None, 0
        # A request ACK is never stop proof. Poll only this exact owned ID;
        # disappearance, invalid/mixed records and command errors stop as unknown.
        while time.monotonic() < stop_deadline:
            suffix = "" if query_count == 0 else "_%03d" % query_count
            remaining = stop_deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                query = self.command_runner(self._scheduler_argv("query", job_id, policy, task_root, env, stop_deadline),
                    cwd=task_root, env=env, timeout_seconds=min(5, remaining),
                    stdout_path=os.path.join(task_root, "scheduler_query%s.stdout" % suffix),
                    stderr_path=os.path.join(task_root, "scheduler_query%s.stderr" % suffix),
                    termination_grace_seconds=1, termination_kill_seconds=1)
            except Exception as error:
                query = {"returncode": None, "error": type(error).__name__}
            queries.append(query)
            query_count += 1
            if len(queries) > 32:
                queries = queries[:1] + queries[-31:]
            text = (query.get("stdout_tail") or "") + "\n" + (query.get("stderr_tail") or "")
            observation = parse_scheduler_observation(text, job_id)
            if not _scheduler_command_ok(query):
                observation.update(terminal=False, reason="scheduler_query_failed")
                break
            if observation["terminal"] or observation["invalid"] or observation["not_found"]:
                break
            remaining = stop_deadline - time.monotonic()
            if remaining > 0:
                time.sleep(min(0.5, remaining))
        state = observation["state"] if observation and observation["terminal"] else None
        return {"attempted": True, "job_id": job_id, "remote_stop_proven": state is not None,
                "terminal_state": state, "cancel": cancel, "query": queries[-1] if queries else None, "queries": queries,
                "query_count": query_count, "timeout_seconds": self.scheduler_stop_timeout,
                "deadline_expired": state is None and time.monotonic() >= stop_deadline,
                "query_observation": observation, "capability": capability}

    def _execute_task(self, manifest, task, timeout_seconds, cancel_check, phases=None, enqueued=None):
        collect_timing = phases is not None
        if phases is None:
            phases = {}
        phases["worker_queue"] = {"elapsed_seconds": elapsed(enqueued, timestamp())}
        acquired = []
        test_semaphore = self.test_capacity.get(task["test"])
        # Acquire per-test before global capacity so a crowded test cannot hold
        # global slots while another test is ready to run.
        try:
            policy = self.policy["tests"].get(task["test"], self.policy["global"])
            local = policy["distribution"] == "local"
            with measure(phases, "capacity_wait"):
                for semaphore in (([self.local_capacity] if local else []) +
                                  ([test_semaphore] if test_semaphore else []) + [self.capacity]):
                    self._acquire(semaphore, cancel_check)
                    acquired.append(semaphore)
            with measure(phases, "execution"):
                if not collect_timing:
                    return self._execute_task_owned(manifest, task, timeout_seconds, cancel_check)
                return self._execute_task_owned(manifest, task, timeout_seconds, cancel_check, phases)
        finally:
            for semaphore in reversed(acquired):
                semaphore.release()

    def _execute_task_owned(self, manifest, task, timeout_seconds, cancel_check, phases=None):
        if phases is None:
            phases = {}
        task_preparation_started = timestamp()
        root = manifest["root"]
        policy = self.policy["tests"].get(task["test"], self.policy["global"])
        simulator = task.get("execution_simulator", "spectre")
        if simulator != selected_simulator(self.config, self.spec):
            raise NetlistExecutionError("Selected simulator differs from this candidate execution identity", {"failure_stage": "solver_command"})
        contract = task.get("simulator_contract")
        if contract is not None:
            validate_execution_contract(contract)
        token = uuid.uuid4().hex
        launch_epoch = time.time()
        env = child_environment(self.config, home=ensure_dir(os.path.join(os.path.dirname(task["psf_dir"]), "home")))
        timeout_policy = copy.deepcopy(policy["timeouts"])
        if timeout_policy["fields"]["run_seconds"]["source"] == _contract_text("cr_runtime_fallback"):
            timeout_policy["fields"]["run_seconds"].update(seconds=timeout_seconds,
                raw_value=-1 if timeout_seconds is None else timeout_seconds,
                enabled=timeout_seconds is not None, status="disabled" if timeout_seconds is None else "enabled")
        run_timeout = timeout_policy["fields"]["run_seconds"]["seconds"]
        wrapper, files = render_task_wrapper(task, root, token, env,
            run_timeout if policy["distribution"] == "local" else None)
        task_root = os.path.dirname(wrapper)
        values = solver_bindings(contract, task["netlist_path"], task["psf_dir"], task["spectre_log"], task.get("ahdl_dir")) if contract else {}
        values.update(cwd=task["cwd"], owned_task_root=os.path.dirname(task["psf_dir"]), test=task["test"], corner=task["corner"], point_id=str(task["point_id"]))
        try:
            submission_plan = build_submission_command(policy, wrapper, self.commands, values)
        except ExecutionCommandError as error:
            detail = dict(error.detail, solver_command_plan=task.get("solver_command_plan"))
            detail.setdefault("failure_stage", "submit")
            raise NetlistExecutionError(str(error), detail)
        argv = submission_plan["argv"]
        remote = submission_plan["remote"]
        phases["scheduler_queue"] = {"elapsed_seconds": None,
            "source": "scheduler_timestamps_unavailable" if remote else "not_applicable"}
        submit_env = scheduler_environment(env) if remote else env
        if remote:
            binary = resolve_scheduler_binary(argv[0], environment=submit_env)
            if not binary or not os.path.isfile(binary) or not os.access(binary, os.X_OK):
                raise NetlistExecutionError("Submission executable is unavailable", {
                    "failure_stage": "submit", "execution_started": False,
                    "command": command_display(argv), "solver_command_plan": task.get("solver_command_plan"),
                    "submission_command_plan": submission_plan})
            submission_plan = resolve_submission_command(submission_plan, binary)
            argv = submission_plan["argv"]
            # Submission templates pass the owned executable job script.
        if contract:
            validate_submission_resources(submission_plan, task["argv"], contract)
        cancel_policy = dict(policy, submit_argv=argv, scheduler_bindings=submission_plan["bindings"])
        submitted = time.monotonic()
        timeout_clock = TaskTimeoutClock(timeout_policy, submitted, remote=remote)
        timeout_path = os.path.join(task_root, _contract_text("task_timeouts.json"))
        timeout_expired = [None]
        active_key = manifest["candidate_id"] + ":" + task["task_id"]
        active = {"files": files, "task": task, "token": token, "remote": remote, "stage": "submit",
                  "submission_command_plan": submission_plan, "command_scheduler": self.command_scheduler}
        active["candidate_root"] = root
        task_identity = {"candidate_id": manifest["candidate_id"], "task_id": task["task_id"],
            "test": task["test"], "corner": task["corner"], "point_id": task["point_id"],
            "execution_simulator": simulator, "task_root": os.path.dirname(task["psf_dir"]),
            "psf_dir": task["psf_dir"], "simulator_log": task["spectre_log"],
            "solver_stdout": files["simulator.stdout"], "solver_stderr": files["simulator.stderr"],
            "scheduler_stdout": None, "scheduler_stderr": None,
            "cwd": task["cwd"], "launch_epoch": launch_epoch}
        for key in ("stdout", "stderr"):
            task_identity["scheduler_" + key] = (submission_plan.get("scheduler_io") or {}).get(key)
        active["log_identity"] = task_identity

        def emit_task(value):
            self._emit(dict(task_identity, **value))

        def record_timeout_state():
            state = timeout_clock.snapshot(time.monotonic())
            active["timeout_state"] = state
            atomic_write_json(timeout_path, dict(task_identity, **state))
            return state

        seen_start = [None]

        def observe_start():
            started = read_task_receipt(files["started.receipt"], token)
            if started and seen_start[0] is None:
                seen_start[0] = time.monotonic()
                timeout_clock.started_receipt(seen_start[0])
                active["stage"] = "solve"
                emit_task({"stage": "solve", "token": token, "started_receipt": started,
                           "timeout_state": record_timeout_state()})
            if read_task_receipt(files["terminal.receipt"], token, terminal=True) and timeout_clock.finished is None:
                if os.path.exists(files["timed_out"]) and timeout_clock.expiration is None:
                    state = timeout_clock.snapshot(time.monotonic())
                    timeout_clock.expiration = {"phase": "run", "trigger": "owned_compute_watchdog",
                        "limit_seconds": run_timeout, "source": timeout_policy["fields"]["run_seconds"]["source"],
                        "job_id": timeout_clock.job_id, "phase_elapsed_seconds": state["phase_elapsed_seconds"]}
                    timeout_expired[0] = timeout_clock.expiration
                timeout_clock.terminal_receipt(time.monotonic())
                emit_task({"stage": "solver_terminal"})
            return started
        try:
            with self.lock:
                self.active[active_key] = active
                self._persist_ownership_locked()
            record_timeout_state()
            emit_task({"stage": "command_render", "candidate_id": manifest["candidate_id"],
                        "test": task["test"], "corner": task["corner"], "argv": task["argv"], "cwd": task["cwd"],
                        "solver_command_plan": task.get("solver_command_plan"),
                        "submission_command_plan": submission_plan,
                        "execution_simulator": simulator, "command": command_display(task["argv"]),
                        "message": "frozen solver command is prepared inside the owned job script"})
            emit_task({"stage": "submit", "candidate_id": manifest["candidate_id"],
                        "test": task["test"], "corner": task["corner"], "argv": argv, "cwd": task["cwd"],
                        "remote": remote, "submission_command_plan": submission_plan,
                        "command": command_display(argv)})
            atomic_write_json(os.path.join(task_root, _contract_text("submission.json")), {
                "schema_version": SCHEMA, "candidate_id": manifest["candidate_id"], "task_id": task["task_id"],
                "token": token, "argv": argv, "cwd": task["cwd"], "remote": remote,
                "solver_command_plan": task.get("solver_command_plan"), "submission_command_plan": submission_plan,
                "submitted_at": utc_timestamp(), "netlist_sha256": task["netlist_sha256"]})
        except Exception:
            # These writes and UI notifications occur before submit Popen.
            with self.lock:
                self.active.pop(active_key, None)
                try:
                    self._persist_ownership_locked()
                except OSError:
                    self.unsafe.set()
            raise

        def interrupted():
            if self._cancelled(cancel_check):
                atomic_write_text(files["cancel"], "cancel requested\n")
                return True
            observe_start()
            expiration = timeout_clock.expired(time.monotonic())
            if expiration:
                if timeout_expired[0] is None:
                    timeout_expired[0] = expiration
                    atomic_write_text(files["timed_out"], expiration["phase"] + " timeout\n")
                    atomic_write_text(files["cancel"], expiration["phase"] + " timeout\n")
                    emit_task({"stage": "task_timeout", "timeout_state": record_timeout_state(),
                               "message": expiration["phase"] + " time limit expired; owned cancellation requested"})
                return True
            return False

        submit_result, terminal, scheduler_stop = None, None, None
        scheduler_record, scheduler_job_id = None, None
        submission_observation, submission_identity_failed = None, False
        scheduler_paths = submission_plan.get("scheduler_io") or {}
        solver_artifacts = None
        scheduler_terminal_since, scheduler_receipt_missing = None, False
        scheduler_identity = dict(task_identity, receipt_token=token)
        awaiting_measurement = False
        failure_stage = "submit"
        phases["execution_preparation"] = {"elapsed_seconds": elapsed(task_preparation_started, timestamp())}
        pending = PendingSamples() if remote else None
        submit_finished = None
        try:
            with measure(phases, "submission" if remote else "local_command"):
                submit_result = run_owned_command(
                    argv, cwd=task["cwd"], env=submit_env,
                    timeout_seconds=(self.config.get("runtime") or {}).get("command_timeout_seconds", 1800) if remote else None,
                    control_root=task_root, token=_contract_text("cr_netlist_task_%s") % token,
                    cancel_check=interrupted, command_runner=self.command_runner)
            submit_finished = timestamp()
            if remote:
                submission_observation = parse_submit_streams(submit_result.get("stdout_tail"), submit_result.get("stderr_tail"))
                scheduler_job_id = submission_observation["job_id"]
                submission_identity_failed = bool(self._scheduler_submission(argv) and not submission_observation["accepted"])
                active["scheduler_job_id"] = scheduler_job_id
                active["submission_observation"] = submission_observation
                timeout_clock.job_id = scheduler_job_id
                task_identity["job_id"] = scheduler_job_id
            next_scheduler_poll = 0.0
            while True:
                observe_start()
                if seen_start[0] is not None:
                    failure_stage = "solve"
                terminal = read_task_receipt(files["terminal.receipt"], token, terminal=True)
                if remote and scheduler_job_id and time.monotonic() >= next_scheduler_poll:
                    scheduler_record = self._scheduler_observe(cancel_policy, scheduler_job_id, task_root, env,
                                                               scheduler_record, scheduler_identity, final=bool(terminal))
                    active["scheduler_observation"] = scheduler_record
                    pending.observe(scheduler_record, scheduler_job_id)
                    timeout_clock.observe((scheduler_record or {}).get("latest"), time.monotonic())
                    if timeout_clock.running is not None:
                        failure_stage = "solve"
                    record_timeout_state()
                    next_scheduler_poll = time.monotonic() + self.scheduler_poll_interval
                    with self.lock:
                        self._persist_ownership_locked()
                if terminal:
                    break
                scheduler_terminal = (scheduler_record or {}).get("terminal_observation")
                if remote and scheduler_terminal:
                    scheduler_stop = {"attempted": False, "job_id": scheduler_job_id,
                        "remote_stop_proven": True, "terminal_state": scheduler_terminal["state"],
                        "query_observation": scheduler_terminal, "cancel_requested": False,
                        "source": "exact_owned_terminal_observation"}
                    successful = (scheduler_terminal["state"] in ("SUCCEEDED", "SUCCESS", "DONE", "FINISHED")
                                  and scheduler_terminal.get("job_exit_code") in (None, 0))
                    if scheduler_terminal_since is None:
                        scheduler_terminal_since = time.monotonic()
                    if not successful or time.monotonic() - scheduler_terminal_since >= self.scheduler_receipt_grace:
                        scheduler_receipt_missing = True
                        break
                cancelled = interrupted()
                expired = bool(timeout_expired[0])
                if expired and timeout_expired[0]["phase"] == "configure":
                    failure_stage = "configure"
                if cancelled or expired or submit_result.get("returncode") != 0 or submission_identity_failed:
                    atomic_write_text(files["cancel"], "cancelled or execution deadline expired\n")
                    if remote:
                        scheduler_stop = self._scheduler_cancel(cancel_policy, submit_result, task_root, env)
                    grace = time.monotonic() + 5
                    while time.monotonic() < grace:
                        terminal = read_task_receipt(files["terminal.receipt"], token, terminal=True)
                        if terminal:
                            break
                        time.sleep(0.05)
                    break
                time.sleep(0.1)
            if remote and scheduler_job_id and not (scheduler_record or {}).get("final"):
                scheduler_record = self._scheduler_observe(cancel_policy, scheduler_job_id, task_root, env,
                                                           scheduler_record, scheduler_identity, final=True)
                active["scheduler_observation"] = scheduler_record
                pending.observe(scheduler_record, scheduler_job_id)
                timeout_clock.observe((scheduler_record or {}).get("latest"), time.monotonic())
                record_timeout_state()
            with measure(phases, "solver_artifact_collection"):
                solver_artifacts = collect_solver_artifacts(task, root, launch_epoch, job_id=scheduler_job_id,
                    scheduler_stdout=scheduler_paths.get("stdout"), scheduler_stderr=scheduler_paths.get("stderr"))
            active["scheduler_stop"] = scheduler_stop
            termination_evidence = task_termination_evidence(active, terminal)
            cleaned = termination_evidence["cleanup_proven"]
            if not cleaned:
                self.unsafe.set()
                with self.lock:
                    self.unresolved[active_key] = active
                    self._persist_ownership_locked()
                raise NetlistExecutionError("Simulator cleanup is unproven; retained private files prevent unsafe reuse", {
                    "failure_stage": "termination",
                    "primary_failure": {"failure_stage": failure_stage,
                        "submission_returncode": submit_result.get("returncode"),
                        "submission_identity_failed": submission_identity_failed,
                        "submission_observation": submission_observation,
                        "receipt_returncode": terminal.get("returncode") if terminal else None,
                        "cancelled": bool(self._cancelled(cancel_check) or submit_result.get("cancelled")),
                        "deadline_expired": bool(timeout_expired[0]), "timeout_state": record_timeout_state()},
                    "termination_failed": True, "remote": remote, "command": submit_result,
                    "solver_command_plan": task.get("solver_command_plan"), "submission_command_plan": submission_plan,
                    "scheduler_stop": scheduler_stop, "path": task_root, "token": token})
            timed_out = bool(os.path.exists(files["timed_out"]) or submit_result.get("timed_out") or timeout_expired[0])
            cancelled = bool(self._cancelled(cancel_check) or (submit_result.get("cancelled") and not timed_out))
            if submission_identity_failed:
                raise NetlistExecutionError("Scheduler submission did not provide one valid owned Job ID", {
                    "failure_stage": "submit", "submission_identity_failed": True,
                    "submission_observation": submission_observation, "receipt": terminal,
                    "command": submit_result, "scheduler_stop": scheduler_stop})
            scheduler_terminal = (scheduler_record or {}).get("terminal_observation")
            scheduler_failed = bool(scheduler_terminal and (
                scheduler_terminal["state"] not in ("SUCCEEDED", "SUCCESS", "DONE", "FINISHED")
                or scheduler_terminal.get("job_exit_code") not in (None, 0)))
            if scheduler_receipt_missing or scheduler_failed:
                raise NetlistExecutionError("Scheduler job ended but owned compute receipt is missing or conflicts with scheduler state", {
                    "failure_stage": failure_stage, "scheduler_terminal_state": scheduler_terminal["state"],
                    "compute_receipt_missing": terminal is None,
                    "shared_result_path_unproven": terminal is None,
                    "receipt_grace_seconds": self.scheduler_receipt_grace,
                    "receipt": terminal, "command": submit_result, "scheduler_stop": scheduler_stop})
            if not terminal or terminal["returncode"] != 0 or timed_out or cancelled:
                failure = failed_spectre_evidence(task, files, terminal)
                startup_timeout = bool(timed_out and seen_start[0] is None)
                raise NetlistExecutionError(simulator.upper() + " task was cancelled, timed out or failed", {
                    "failure_stage": failure_stage,
                    "timed_out": timed_out, "cancelled": cancelled, "receipt": terminal,
                    "timeout_state": record_timeout_state(),
                    "command": submit_result, "scheduler_stop": scheduler_stop, "path": task_root,
                    "native_failure_evidence": failure},
                    infrastructure=startup_timeout or bool(remote and not terminal) or (not timed_out and not cancelled and failure["infrastructure"]))
            if simulator == "alps":
                log_sources = [files["simulator.stdout"], files["simulator.stderr"]]
                if os.path.lexists(task["spectre_log"]):
                    log_sources.append(_owned_file(task["spectre_log"], root, "ALPS produced simulator log"))
                text = "\n".join(_tail_text(path) for path in log_sources if os.path.isfile(path))
                if _SPECTRE_COMPLETION.search(text):
                    raise NetlistExecutionError("ALPS execution returned conflicting Spectre completion evidence")
                atomic_write_text(task["spectre_log"], "ALPS owned stdout/stderr tail\n" + text)
                evidence = {"execution_simulator": "alps", "path": os.path.relpath(task["spectre_log"], root),
                            "receipt_token": token, "returncode": terminal["returncode"],
                            "completion": "owned_zero_exit_pending_native_result_qualification",
                            "size_bytes": os.path.getsize(task["spectre_log"])}
            else:
                evidence = _spectre_log_evidence(task["spectre_log"], root, include_timing=True)
                phases["solver"] = {"elapsed_seconds": evidence.pop("solver_wall_seconds", None),
                                    "source": "spectre_native_wall_clock"}
            if simulator == "alps":
                try:
                    evidence["solver_log"] = _alps_log_evidence(task, root, launch_epoch)
                    evidence["companion_aliases"] = _alps_companion_alias_snapshot(task, root, launch_epoch)
                except CompletionEvidenceError as error:
                    raise NetlistExecutionError(str(error), {"failure_stage": "alps_result_log",
                        "execution_simulator": simulator, "psf_dir": task["psf_dir"]}, infrastructure=True)
            else:
                psf_log = _owned_file(os.path.join(task["psf_dir"], "logFile"), root, "Simulator PSF logFile")
                if os.path.getsize(psf_log) == 0:
                    raise NetlistExecutionError("Simulator fresh PSF logFile is missing, empty or stale")
            retained_logs = retain_shared_cli_task_logs(dict(task, **task_identity),
                self.owned_run_root or root, manifest["candidate_id"], launch_epoch=launch_epoch)
            active["retained_logs"] = retained_logs
            emit_task({"stage": "solve_completed", "candidate_id": manifest["candidate_id"],
                        "test": task["test"], "corner": task["corner"], "argv": task["argv"], "cwd": task["cwd"],
                        "terminal_receipt": terminal,
                        "effective_terminal_receipt": effective_terminal_receipt(terminal, termination_evidence),
                        "termination_evidence": termination_evidence,
                        "retained_logs": retained_logs, "timeout_state": record_timeout_state(),
                        "execution_simulator": simulator, "message": simulator.upper() + " owned execution finished; native result evaluation follows"})
            result = {"task": task, "token": token, "submission": submit_result,
                      "solver_command_plan": task.get("solver_command_plan"), "submission_command_plan": submission_plan,
                      "scheduler_job_id": scheduler_job_id, "scheduler_observation": scheduler_record,
                      "submission_observation": submission_observation,
                      "solver_artifacts": solver_artifacts,
                      "timeout_state": record_timeout_state(),
                      "retained_logs": retained_logs,
                      "receipt": terminal, "spectre": evidence, "execution_simulator": simulator,
                      "effective_terminal_receipt": effective_terminal_receipt(terminal, termination_evidence),
                      "termination_evidence": termination_evidence,
                      "native_job_state": {"simulation_submitted": True, "native_completion_proven": True},
                      "elapsed_seconds": time.monotonic() - submitted}
            active["stage"] = "awaiting_measurement"
            with self.lock:
                self._persist_ownership_locked()
            awaiting_measurement = True
            return result
        except Exception as error:
            if isinstance(error, NetlistExecutionError):
                error.detail.setdefault("failure_stage", failure_stage)
                error.detail.setdefault("solver_command_plan", task.get("solver_command_plan"))
                error.detail.setdefault("submission_command_plan", submission_plan)
                if solver_artifacts is None:
                    solver_artifacts = collect_solver_artifacts(task, root, launch_epoch, job_id=scheduler_job_id,
                        scheduler_stdout=scheduler_paths.get("stdout"), scheduler_stderr=scheduler_paths.get("stderr"))
                error.detail.setdefault("scheduler_job_id", scheduler_job_id)
                error.detail.setdefault("submission_observation", submission_observation)
                error.detail.setdefault("scheduler_observation", scheduler_record)
                error.detail.setdefault("solver_artifacts", solver_artifacts)
                error.detail.setdefault("timeout_state", record_timeout_state())
            if (getattr(error, "detail", {}).get("command") or {}).get("termination_failed"):
                self.unsafe.set()
                failed_command = getattr(error, "detail", {}).get("command") or {}
                state_path = failed_command.get("guardian_state_path")
                failed_token = failed_command.get("guardian_token")
                if state_path and failed_token and os.path.dirname(state_path) != task_root:
                    active.setdefault("cleanup_dependencies", []).append({
                        "attempt_root": os.path.dirname(state_path), "cwd": failed_command.get("cwd"),
                        "token": failed_token})
                with self.lock:
                    self.unresolved[active_key] = active
                    self._persist_ownership_locked()
            atomic_write_json(os.path.join(task_root, _contract_text("error.json")), {
                "error": "%s: %s" % (error.__class__.__name__, error), "candidate_id": manifest["candidate_id"],
                "task_id": task["task_id"], "token": token, "detail": getattr(error, "detail", {}),
                "submission": submit_result, "receipt": terminal, "scheduler_stop": scheduler_stop,
                "scheduler_job_id": scheduler_job_id, "scheduler_observation": scheduler_record,
                "submission_observation": submission_observation,
                "solver_artifacts": solver_artifacts})
            if isinstance(error, CompletionEvidenceError):
                raise NetlistExecutionError(str(error), {"path": task_root, "command": submit_result,
                                            "failure_stage": failure_stage,
                                            "solver_command_plan": task.get("solver_command_plan"), "submission_command_plan": submission_plan,
                                            "scheduler_job_id": scheduler_job_id, "scheduler_observation": scheduler_record,
                                            "submission_observation": submission_observation,
                                            "solver_artifacts": solver_artifacts},
                                            infrastructure=isinstance(error, UnsupportedCompletionEvidence))
            raise
        finally:
            if pending is not None:
                phases["scheduler_observed_pending"] = pending.phase()
                phases["remote_start_observation"] = {
                    "elapsed_seconds": elapsed(submit_finished, seen_start[0]),
                    "source": "submit_return_to_first_owned_start_receipt_observation"}
            if not awaiting_measurement:
                with self.lock:
                    self.active.pop(active_key, None)
                    self._persist_ownership_locked()

    def _measure_task(self, manifest, result, timeout_seconds, cancel_check, phases=None, enqueued=None, precomputed=None):
        """Measure in a small local pool without occupying a Spectre job slot."""
        if phases is None:
            phases = {}
        phases["measurement_queue"] = {"elapsed_seconds": elapsed(enqueued, timestamp())}
        task, token, root = result["task"], result["token"], manifest["root"]
        key = manifest["candidate_id"] + ":" + task["task_id"]
        control = os.path.dirname(os.path.join(os.path.dirname(task["psf_dir"]), "execution", "terminal.receipt"))
        with self.lock:
            active = self.active[key]

        def cancelled():
            return self._cancelled(cancel_check)

        def before_measurement_launch(dependency):
            with self.lock:
                active["stage"] = "measurement"
                active.setdefault("cleanup_dependencies", []).append(dependency)
                self._persist_ownership_locked()

        def measurement_progress(event):
            identity = dict(active.get("log_identity") or {}, retained_logs=result.get("retained_logs"),
                            timeout_state=result.get("timeout_state"))
            self._emit(dict(identity, **event))

        try:
            if cancelled():
                raise NetlistExecutionError("Candidate cancelled before output evaluation", {"cancelled": True})
            with measure(phases, "result_collection"):
                if isinstance(precomputed, Exception):
                    raise precomputed
                measurement = precomputed
                if measurement is None:
                    try:
                        measurement = self.measurement_evaluator(
                            task, self.config, root, timeout_seconds=timeout_seconds,
                            required_output_keys=self.required_outputs, cancel_check=cancelled,
                            progress_callback=measurement_progress, ownership_callback=before_measurement_launch)
                    finally:
                        measurement_progress({"stage": "measurement_terminal"})
            if task.get("execution_simulator", "spectre") == "alps":
                proof = measurement.get("native_parameter_readback_verified") or {}
                if measurement.get("execution_simulator") != "alps" or not isinstance(proof, dict) or proof.get("ok") is not True or proof.get("source") != "current_psf_designParamVals":
                    raise NetlistExecutionError("ALPS actual native result/parameter qualification is unproven", {"measurement": measurement})
            result["measurement"] = measurement
            atomic_write_json(os.path.join(control, _contract_text("result.json")), result)
            if self.point_commit_callback is not None:
                # A point is measured, not a completed candidate. Freeze all
                # native evidence before deleting any PSF/AHDL. The optimizer
                # owner acknowledges only after core data AND the unchanged
                # active-plan checkpoint are durable. Resume may rerun this
                # nonterminal candidate after ownership reconciliation.
                dependencies_clean, dependencies = _cleanup_dependencies(active, self.owned_run_root or root)
                terminal = read_task_receipt(active["files"]["terminal.receipt"], token, terminal=True)
                termination_evidence = task_termination_evidence(active, terminal)
                if (not dependencies_clean or not terminal or terminal.get("returncode") != 0
                        or termination_evidence.get("cleanup_proven") is not True):
                    raise NetlistExecutionError("Measured point termination is unproven; raw data retained",
                                               {"termination_failed": True, "dependencies": dependencies,
                                                "failure_stage": "termination", "termination_evidence": termination_evidence})
                compact = {"candidate_id": manifest["candidate_id"], "task_id": task["task_id"],
                           "test": task["test"], "corner": task["corner"], "point_id": task["point_id"],
                           "design_point": task.get("design_point", task["point_id"]),
                           "token": token, "template_sha256": manifest.get("template_sha256"),
                           "netlist_sha256": task["netlist_sha256"], "spectre": result["spectre"],
                           "execution_simulator": task.get("execution_simulator", "spectre"),
                           "simulator_contract_fingerprint": (task.get("simulator_contract") or {}).get("fingerprint"),
                           "native_parameter_readback_verified": measurement.get("native_parameter_readback_verified"),
                           "netlist_generation": task.get("netlist_generation"),
                           "solver_command_plan": result.get("solver_command_plan"),
                           "submission_command_plan": result.get("submission_command_plan"),
                           "scheduler_job_id": result.get("scheduler_job_id"),
                           "scheduler_observation": result.get("scheduler_observation"),
                           "submission_observation": result.get("submission_observation"),
                           "solver_artifacts": result.get("solver_artifacts"),
                           "timeout_state": result.get("timeout_state"),
                           "retained_logs": result.get("retained_logs"),
                           "outputs": measurement.get("outputs") or [],
                           "effective_variables": measurement.get("effective_variables") or {},
                           "elapsed_seconds": result["elapsed_seconds"],
                           "process_termination_proven": True, "remote_stop_proven": True,
                           "terminal_receipt": terminal, "measurement_dependencies": dependencies,
                           "effective_terminal_receipt": effective_terminal_receipt(terminal, termination_evidence),
                           "termination_evidence": termination_evidence,
                           "candidate_completed": False}
                committed = self.point_commit_callback(compact, cancelled)
                if isinstance(committed, dict) and committed.get("ok") is True:
                    # Publish absence of this proven-dead writer before raw
                    # deletion, so crash recovery never needs a deleted receipt
                    # to prove that an already-retired point stopped.
                    with self.lock:
                        self.active.pop(key, None)
                        self._persist_ownership_locked()
                    active["point_retirement_proof"] = {"cleanup_proven": True, "dependencies": dependencies}
                    result["point_commit"] = committed
                    result["point_raw_cleanup"] = self._cleanup_measured_point(manifest, task, committed,
                        progress_identity=active.get("log_identity"), companion_evidence=result["spectre"].get("companion_aliases"))
            self._emit(dict(active.get("log_identity") or {}, stage="task_completed",
                candidate_id=manifest["candidate_id"], task_id=task["task_id"], test=task["test"],
                corner=task["corner"], point_id=task["point_id"], retained_logs=result.get("retained_logs"),
                timeout_state=result.get("timeout_state")))
            return result
        except Exception as error:
            if isinstance(error, NetlistExecutionError):
                error.detail.setdefault("failure_stage", "measurement")
                error.detail.setdefault("scheduler_job_id", result.get("scheduler_job_id"))
                error.detail.setdefault("scheduler_observation", result.get("scheduler_observation"))
                error.detail.setdefault("solver_artifacts", result.get("solver_artifacts"))
            retired = active.get("point_retirement_proof")
            if retired:
                dependencies_clean, dependencies = retired["cleanup_proven"], retired["dependencies"]
            else:
                dependencies_clean, dependencies = _cleanup_dependencies(active, self.owned_run_root or root)
            if (not dependencies_clean or getattr(error, "detail", {}).get("termination_failed")
                    or (getattr(error, "detail", {}).get("command") or {}).get("termination_failed")):
                self.unsafe.set()
                with self.lock:
                    self.unresolved[key] = active
                    self._persist_ownership_locked()
            atomic_write_json(os.path.join(control, _contract_text("measurement_error.json")), {
                "error": "%s: %s" % (error.__class__.__name__, error), "detail": getattr(error, "detail", {}),
                "candidate_id": manifest["candidate_id"], "task_id": task["task_id"],
                "dependencies": dependencies, "cleanup_proven": dependencies_clean})
            if isinstance(error, NetlistMeasurementError):
                detail = dict(getattr(error, "detail", {}))
                detail.setdefault("failure_stage", "measurement")
                raise NetlistExecutionError(str(error), detail,
                                            infrastructure=error.infrastructure or not dependencies_clean)
            raise
        finally:
            with self.lock:
                self.active.pop(key, None)
                self._persist_ownership_locked()

    def _measure_batch(self, manifest, ready, timeout_seconds, cancel_check):
        tasks = [result["task"] for result, timing in ready]
        dependencies = []
        def ownership(dependency):
            dependencies.append(copy.deepcopy(dependency))
            with self.lock:
                for task in tasks:
                    active = self.active[manifest["candidate_id"] + ":" + task["task_id"]]
                    active["stage"] = "measurement"
                    active.setdefault("cleanup_dependencies", []).append(copy.deepcopy(dependency))
                self._persist_ownership_locked()
        reader_started = timestamp()
        try:
            if self._cancelled(cancel_check):
                raise NetlistExecutionError("Candidate cancelled before output evaluation", {"cancelled": True})
            measured = evaluate_task_outputs_batch(tasks, self.config, manifest["root"], timeout_seconds,
                required_output_keys=self.required_outputs, cancel_check=lambda: self._cancelled(cancel_check),
                ownership_callback=ownership, progress_callback=lambda event: self._emit(dict(event,
                    candidate_id=manifest["candidate_id"], task_id=tasks[0]["task_id"])))
        except Exception as error:
            pending = getattr(error, "detail", {}).get("reader_control_cleanup_pending")
            if pending:
                # No process was launched, but failed control-file cleanup is
                # still an owned unresolved artifact, never a completed cleanup.
                ownership({"attempt_root": pending, "cwd": pending, "token": _contract_text("cr_prelaunch_cleanup")})
            measured = {task["task_id"]: error for task in tasks}
        finally:
            self._emit({"stage": "measurement_terminal", "candidate_id": manifest["candidate_id"],
                        "task_id": tasks[0]["task_id"]})
        batch_timing = {"elapsed_seconds": elapsed(reader_started, timestamp()), "scope": "batch",
                        "batch_id": manifest["candidate_id"] + ":" + tasks[0]["task_id"]}
        results, first_error = [], None
        for result, timing in ready:
            timing["phases"]["reader_batch"] = dict(batch_timing)
            try:
                results.append(self._measure_task(manifest, result, timeout_seconds, cancel_check,
                    timing["phases"], timestamp(), precomputed=measured[result["task"]["task_id"]]))
                timing["status"] = "completed"
            except Exception as error:
                timing["status"] = "failed"
                if first_error is None:
                    first_error = error
        # Every point has now persisted its retirement or unresolved owner.
        # Shared guardian evidence must outlive all of those point consumers.
        cleanup = self._cleanup_reader_batches(manifest["root"], dependencies)
        for result, timing in ready:
            timing["reader_batch_cleanup"] = cleanup
        if first_error is not None:
            raise first_error
        return results

    def _cleanup_reader_batches(self, root, dependencies):
        from .retention import _reject_mounts_or_cross_device_tree, _reject_unsafe_regular_tree
        result = {"artifact_cleanup_complete": True, "removed": [], "retained": []}
        parent = os.path.join(os.path.realpath(root), "reader_batches")
        for dependency in dependencies:
            attempt = dependency["attempt_root"]
            try:
                if os.path.dirname(attempt) != parent or os.path.realpath(attempt) != attempt:
                    raise NetlistExecutionError("Reader cleanup boundary is unsafe")
                with self.lock:
                    owners = list(self.active.values()) + list(self.unresolved.values())
                    if any(dependency in row.get("cleanup_dependencies", []) for row in owners):
                        raise NetlistExecutionError("Reader cleanup still has point owners")
                    # Recovery must observe the retired owners before their
                    # shared guardian files disappear, even after a write retry.
                    self._persist_ownership_locked()
                    clean, unused = _cleanup_dependencies(
                        {"cleanup_dependencies": [dependency]}, self.owned_run_root or root)
                    if not clean:
                        raise NetlistExecutionError("Reader termination remains unproven")
                    device = os.stat(root).st_dev
                    _reject_mounts_or_cross_device_tree(attempt, device, "retired batch reader")
                    _reject_unsafe_regular_tree(attempt, device, "retired batch reader")
                    shutil.rmtree(attempt)
                result["removed"].append(attempt)
            except Exception as error:
                result["artifact_cleanup_complete"] = False
                result["retained"].append({"path": attempt, "error": str(error)})
        return result

    def _cleanup_measured_point(self, manifest, task, committed, progress_identity=None, companion_evidence=None):
        try:
            with native_log_cleanup_scope() as barrier:
                result = self._cleanup_measured_point_unlocked(manifest, task, committed,
                    progress_identity=progress_identity, companion_evidence=companion_evidence)
                result["reader_barrier"] = barrier
                return result
        except OwnedLogReadersBusy as error:
            return {"artifact_cleanup_complete": False, "removed": [],
                    "reader_barrier": error.detail,
                    "errors": [{"path": os.path.dirname(task["psf_dir"]),
                                "error": str(error), "retryable": True}]}

    def _cleanup_measured_point_unlocked(self, manifest, task, committed, progress_identity=None, companion_evidence=None):
        task_root = os.path.dirname(task["psf_dir"])
        expected = os.path.join(manifest["root"], "tasks", task["task_id"])
        if (os.path.abspath(task_root) != os.path.abspath(expected)
                or os.path.realpath(task_root) != os.path.abspath(expected)
                or not is_under(expected, self.owned_run_root or manifest["root"])):
            raise NetlistExecutionError("Measured point cleanup boundary is unsafe")
        from .retention import _reject_mounts_or_cross_device_tree, _reject_unsafe_regular_tree
        run_device = os.stat(self.owned_run_root or manifest["root"]).st_dev
        removed = []
        try:
            _reject_mounts_or_cross_device_tree(expected, run_device, "committed measured point")
            removed = _unlink_verified_alps_companion_aliases(task, self.owned_run_root or manifest["root"], companion_evidence)
            _reject_unsafe_regular_tree(expected, run_device, "committed measured point")
        except Exception as error:
            return {"artifact_cleanup_complete": False, "removed": removed,
                    "errors": [{"path": expected, "error": str(error)}]}
        # Keep the small execution receipt/dependencies until candidate commit;
        # only this task's generated simulator and measurement payload is removed.
        errors, retained = [], []
        from .diagnosis_catalog import diagnostic_run
        preserve_netlist = diagnostic_run(self.spec)
        if preserve_netlist:
            source = os.path.join(expected, "netlist")
            if os.path.isdir(source):
                try:
                    # Candidate compaction may later remove its raw tree. Keep
                    # a separate owned snapshot outside candidates for manual
                    # inspection, after the existing termination/tree fences.
                    from .retention import _diagnostic_directory_fd
                    candidate_id, task_id = manifest["candidate_id"], task["task_id"]
                    if any(not isinstance(identifier, str) or not SAFE_ID.fullmatch(identifier)
                           or identifier in (".", "..") for identifier in (candidate_id, task_id)):
                        raise NetlistExecutionError("Diagnostic snapshot candidate/task identity is unsafe")
                    owned = os.path.abspath(self.owned_run_root or manifest["root"])
                    components = ["private_netlist", "diagnostic_netlists", candidate_id]
                    parent = os.path.join(owned, *components)
                    directory = _diagnostic_directory_fd(owned, components, run_device, create=True)
                    try:
                        if (os.path.realpath(parent) != parent or not os.path.samestat(
                                os.fstat(directory), os.lstat(parent))):
                            raise NetlistExecutionError("Diagnostic snapshot parent identity changed")
                        target = os.path.join(parent, task_id)
                        # An FD anchor prevents a changed path prefix from
                        # redirecting copytree into a linked external tree.
                        anchored = os.path.join("/proc/self/fd", str(directory), task_id)
                        shutil.copytree(source, anchored)
                        if (os.path.realpath(target) != target or not os.path.samestat(
                                os.fstat(directory), os.lstat(parent))):
                            raise NetlistExecutionError("Diagnostic snapshot publication path changed")
                        os.fsync(directory)
                        retained.append({"path": target, "source": source, "reason": "environment_test_manual_inspection"})
                    finally:
                        os.close(directory)
                except Exception as error:
                    errors.append({"path": source, "error": str(error)})
        for name in ("psf", "ahdl", "netlist", "home", "measurement"):
            if name == "netlist" and preserve_netlist:
                retained.append({"path": os.path.join(expected, name), "reason": "environment_test_manual_inspection"})
                continue
            path = os.path.join(expected, name)
            if not os.path.lexists(path):
                continue
            try:
                if not os.path.isdir(path) or os.path.islink(path):
                    raise NetlistExecutionError("Measured point payload is not a real directory")
                shutil.rmtree(path)
                removed.append(name)
            except (OSError, NetlistExecutionError) as error:
                errors.append({"path": path, "error": "%s: %s" % (error.__class__.__name__, error)})
        value = {"artifact_cleanup_complete": not errors, "removed": removed, "errors": errors,
                 "retained": retained, "retention_policy": "diagnostic_netlist_retained" if preserve_netlist else "normal_point_cleanup",
                 "point_data_revision": committed.get("revision"), "checkpoint_sha256": committed.get("checkpoint_sha256")}
        try:
            control = os.path.join(os.path.dirname(task["psf_dir"]), "execution")
            event = {"stage": "point_raw_cleanup", "candidate_id": manifest["candidate_id"],
                "test": task["test"], "corner": task["corner"], "task_id": task["task_id"], "point_id": task["point_id"],
                "task_root": os.path.dirname(task["psf_dir"]), "psf_dir": task["psf_dir"],
                "simulator_log": task["spectre_log"], "execution_simulator": task.get("execution_simulator", "spectre"),
                "solver_stdout": os.path.join(control, "simulator.stdout"), "solver_stderr": os.path.join(control, "simulator.stderr"),
                "scheduler_stdout": None, "scheduler_stderr": None, "cleanup": value}
            if progress_identity:
                event.update(progress_identity)
            self._emit(event)
        except Exception as error:
            value["progress_error"] = str(error)
        return value

    def evaluate(self, candidate_manifest, timeout_seconds=None, cancel_check=None, timing=None):
        timing = timing if isinstance(timing, dict) else {}
        if not isinstance(timing.get("phases"), dict):
            timing["phases"] = {}
        started = timestamp()
        try:
            return self._evaluate(candidate_manifest, timeout_seconds, cancel_check, timing, started)
        except ExecutionCommandError as error:
            detail = dict(error.detail)
            detail.setdefault("failure_stage", "command_render")
            raise NetlistExecutionError(str(error), detail)
        except NetlistExecutionError as error:
            error.detail.setdefault("failure_stage", "solver_command")
            raise
        finally:
            phases = timing["phases"]
            if "execution_preparation" not in phases:
                phases["execution_preparation"] = {"elapsed_seconds": elapsed(started, timestamp())}
            tasks = timing.get("tasks") or []
            for name in ("scheduler_queue", "scheduler_observed_pending", "solver", "result_collection"):
                phases[name] = summarize_tasks(tasks, name)
            batches = {task["phases"]["reader_batch"]["batch_id"]: task["phases"]["reader_batch"]
                       for task in tasks if "reader_batch" in task.get("phases", {})}
            if batches:
                phases["reader_batches"] = {"elapsed_seconds": sum(row["elapsed_seconds"] or 0 for row in batches.values()),
                    "count": len(batches), "total_count": len(batches), "scope": "task_sum"}

    def prepare_candidate(self, candidate_manifest):
        manifest = copy.deepcopy(candidate_manifest)
        validate_execution_selection(manifest, self.execution_selection, self.inherited_execution_selection)
        default_generation = compact_netlist_generation(manifest.get("netlist_generation") or {}, manifest)
        expected_mode = {"maestro_netlist": "maestro", "private_netlist": "template"}.get(self.spec.get("execution_backend"))
        producer = manifest.get("netlist_generation") or {}
        fallback = producer.get("template_fallback") or {}
        if (expected_mode == "template" and default_generation["mode"] == "maestro"
                and producer.get("requested_backend") == "private_netlist"
                and producer.get("native_generation_required") is True
                and producer.get("template_qualified") is False
                and ((fallback.get("failure_stage") == "native_numeric_derived_parameters"
                        and fallback.get("numeric_derived_parameters"))
                     or (fallback.get("failure_stage") == "native_selected_parameter_binding"
                        and fallback.get("selected_parameter_bindings")))
                and fallback.get("native_generation_required") is True and fallback.get("template_qualified") is False
                and (fallback.get("source_guard") or {}).get("ok") is True
                and (fallback.get("preparation_cleanup") or {}).get("cleanup_proven") is True
                and not fallback.get("cleanup_pending") and not fallback.get("termination_failed")):
            expected_mode = "maestro"
        for task in manifest.get("tasks") or []:
            generation = (compact_netlist_generation(task["netlist_generation"], manifest)
                          if task.get("netlist_generation") else copy.deepcopy(default_generation))
            if expected_mode is not None and generation["mode"] != expected_mode:
                raise NetlistExecutionError("Netlist producer differs from this run's frozen source selection")
            if (any(producer.get(key) for key in ("mode", "source_mode", "source"))
                    and generation["mode"] != default_generation["mode"]):
                raise NetlistExecutionError("Point netlist producer differs from its candidate producer")
            task["netlist_generation"] = generation
            contract = task.get("simulator_contract")
            if contract:
                if not task.get("ahdl_dir"):
                    task["ahdl_dir"] = os.path.join(os.path.dirname(task["psf_dir"]), "ahdl")
                plan = task.get("solver_command_plan")
                if plan:
                    if (plan.get("commands") or {}).get("fingerprint") != self.commands["fingerprint"]:
                        raise NetlistExecutionError("Task command settings differ from this run's frozen settings")
                else:
                    if contract["execution_simulator"] == "alps" and task.get("argv") != build_simulator_argv(contract, [], task["netlist_path"], task["psf_dir"], task["spectre_log"], task.get("ahdl_dir")):
                        raise NetlistExecutionError("ALPS argv differs from its qualified simulator contract")
                    plan = build_solver_command_plan(contract, task.get("argv"), task["netlist_path"],
                        task["psf_dir"], task["spectre_log"], task["ahdl_dir"], self.commands)
                    task["solver_command_plan"] = plan
                    task["argv"] = plan["argv"]
                    task["execution_commands"] = copy.deepcopy(self.commands)
            elif self.commands["solver_templates"][task.get("execution_simulator", "spectre")]:
                raise NetlistExecutionError("A custom solver command requires a frozen simulator/model contract")
        manifest["root"] = validate_candidate_manifest(manifest)
        # Validate every point's resource/command binding before any point can
        # consume a local process or a remote scheduler slot.
        for task in manifest["tasks"]:
            contract = task.get("simulator_contract")
            if contract:
                policy = self.policy["tests"].get(task["test"], self.policy["global"])
                values = solver_bindings(contract, task["netlist_path"], task["psf_dir"], task["spectre_log"], task.get("ahdl_dir"))
                values.update(cwd=task["cwd"], owned_task_root=os.path.dirname(task["psf_dir"]), test=task["test"], corner=task["corner"], point_id=str(task["point_id"]))
                wrapper = os.path.join(os.path.dirname(task["psf_dir"]), "execution", "execute.sh")
                submission_plan = build_submission_command(policy, wrapper, self.commands, values)
                try:
                    validate_submission_resources(submission_plan, task["argv"], contract)
                except ExecutionCommandError as error:
                    error.detail.setdefault("solver_command_plan", task.get("solver_command_plan"))
                    error.detail.setdefault("submission_command_plan", submission_plan)
                    raise
        if self.owned_run_root and not is_under(manifest["root"], self.owned_run_root):
            raise NetlistExecutionError("Candidate does not belong to this execution context's run")
        qualification = qualify_expressions(manifest["tasks"], self.required_outputs)
        if not qualification["supported"]:
            raise NetlistExecutionError("Required outputs cannot be qualified by the shared result reader",
                                        dict(qualification, failure_stage="measurement"))
        return manifest

    def _evaluate(self, candidate_manifest, timeout_seconds, cancel_check, timing, preparation_started):
        manifest = self.prepare_candidate(candidate_manifest)
        timeout_value = timeout_seconds if timeout_seconds is not None else self.spec.get("candidate_timeout_seconds", 600)
        if timeout_value is None or (isinstance(timeout_value, str) and not timeout_value.strip()):
            timeout_seconds = None
        else:
            timeout_seconds = float(timeout_value)
            if timeout_seconds == -1:
                timeout_seconds = None
            elif not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
                raise NetlistExecutionError("Candidate timeout must be positive finite seconds or blank for no limit; 0 is invalid")
        required = self.required_outputs or []
        started = time.monotonic()
        failed = threading.Event()

        def candidate_cancelled():
            return failed.is_set() or bool(cancel_check and cancel_check())

        results, first_error, futures = [], None, {}
        reader_ready = []
        phases = timing["phases"]
        phases["execution_preparation"] = {"elapsed_seconds": elapsed(preparation_started, timestamp())}
        timing["tasks"] = []
        for task in manifest["tasks"]:
            task_timing = {key: task[key] for key in ("task_id", "test", "corner", "point_id")}
            task_timing.update(status="pending", phases={"solver": {"elapsed_seconds": None},
                                                        "scheduler_queue": {"elapsed_seconds": None}})
            timing["tasks"].append(task_timing)
            future = self.task_pool.submit(self._execute_task, manifest, task, timeout_seconds,
                                           candidate_cancelled, task_timing["phases"], timestamp())
            futures[future] = ("spectre", task_timing)
        while futures:
            finished, unused = concurrent.futures.wait(list(futures), return_when=concurrent.futures.FIRST_COMPLETED)
            for future in finished:
                stage, task_timing = futures.pop(future)
                try:
                    task_result = future.result()
                    if stage == "spectre" and self.batch_reader:
                        reader_ready.append((task_result, task_timing))
                    elif stage == "spectre":
                        measured = self.measurement_pool.submit(self._measure_task, manifest, task_result,
                                                                timeout_seconds, candidate_cancelled,
                                                                task_timing["phases"], timestamp())
                        futures[measured] = ("measurement", task_timing)
                    elif stage == "reader_batch":
                        results.extend(task_result)
                    else:
                        task_timing["status"] = "completed"
                        results.append(task_result)
                except Exception as error:
                    task_timing["status"] = "failed"
                    if first_error is None:
                        first_error = error
                    failed.set()
            if reader_ready and not any(stage == "spectre" for stage, unused in futures.values()):
                for offset in range(0, len(reader_ready), 4):
                    batch = reader_ready[offset:offset + 4]
                    measured = self.measurement_pool.submit(self._measure_batch, manifest, batch,
                                                            timeout_seconds, candidate_cancelled)
                    futures[measured] = ("reader_batch", batch[0][1])
                reader_ready = []
        # All reader futures have retired. Until now a sibling batch could
        # still be between creating the shared parent and its control child.
        if self.batch_reader:
            try:
                os.rmdir(os.path.join(manifest["root"], "reader_batches"))
            except OSError:
                pass
        if first_error is not None:
            raise first_error
        result_by_id = dict((row["task"]["task_id"], row) for row in results)
        results = [result_by_id[task["task_id"]] for task in manifest["tasks"]]
        path = os.path.join(manifest["root"], _contract_text("cli_results.json"))
        with measure(phases, "result_aggregation"):
            parsed = parsed_results(results, path)
        completion = {"schema_version": COMPLETION_SCHEMA, "adapter": "private_netlist",
                      "candidate_id": manifest["candidate_id"], "template_sha256": manifest.get("template_sha256"),
                      "task_count": len(results), "native_rdb": False,
                      "required_output_keys": required,
                      "simulator_runs": [{"test": row["task"]["test"], "point_id": row["task"]["point_id"],
                                        "corner": row["task"]["corner"], "simulator": row["spectre"],
                                        "execution_simulator": row["task"].get("execution_simulator", "spectre"),
                                        "native_parameter_readback_verified": row["measurement"].get("native_parameter_readback_verified"),
                                        "psf": os.path.relpath(row["task"]["psf_dir"], manifest["root"]),
                                        "receipt_token": row["token"]} for row in results],
                      "outputs": [{"key": row["key"], "point_count": len(row["values"]),
                                   "scalar": row["scalar"]} for row in parsed["outputs"]]}
        simulator = selected_simulator(self.config, self.spec)
        completion.update(execution_simulator=simulator, simulator=simulator,
                          native_parameter_readback_verified=simulator == "alps" and all((row["measurement"].get("native_parameter_readback_verified") or {}).get("ok") is True for row in results))
        source_modes = set((row["task"].get("netlist_generation") or {}).get("mode", "template") for row in results)
        pipeline = {"schema_version": _contract_text("crescendo.execution_pipeline.v1"), "solver": "shared_cli",
                    "submission": "shared_local_or_command",
                    "netlist_source": next(iter(source_modes)) if len(source_modes) == 1 else "mixed",
                    "commands_fingerprint": self.commands["fingerprint"],
                    "points": [{"task_id": row["task"]["task_id"], "test": row["task"]["test"],
                                "corner": row["task"]["corner"], "point_id": row["task"]["point_id"],
                                "netlist_generation": row["task"].get("netlist_generation"),
                                "solver_command_plan": row.get("solver_command_plan"),
                                "submission_command_plan": row.get("submission_command_plan"),
                                "scheduler_job_id": row.get("scheduler_job_id"),
                                "scheduler_observation": row.get("scheduler_observation"),
                                "submission_observation": row.get("submission_observation"),
                                "solver_artifacts": row.get("solver_artifacts"),
                                "submission_returncode": row["submission"].get("returncode"),
                                "terminal_receipt": row["effective_terminal_receipt"],
                                "raw_terminal_receipt": row["receipt"],
                                "termination_evidence": row["termination_evidence"]} for row in results]}
        completion["execution_pipeline"] = pipeline
        completion["spectre_runs"] = [dict(run, spectre=run["simulator"]) for run in completion["simulator_runs"]] if simulator == "spectre" else []
        document = {"schema_version": SCHEMA, "candidate_id": manifest["candidate_id"], "parsed": parsed,
                    "execution_pipeline": pipeline,
                    "completion": completion, "task_results": results, "policy": self.policy,
                    "native_job_state": {"simulation_submitted": True, "native_completion_proven": True},
                    "command": {"returncode": 0, "timed_out": False, "cancelled": False,
                                "termination_failed": False, "backend": "private_netlist"},
                    "timing": {"elapsed_seconds": time.monotonic() - started}}
        atomic_write_json(path, document)
        return document
