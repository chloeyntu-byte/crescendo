from __future__ import print_function

import copy
import contextlib
import hashlib
import json
import errno
import fcntl
import math
import mimetypes
import os
import pwd
import re
import signal
import shutil
import stat
import subprocess
import sys
import threading
import time
import traceback
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
from urllib.parse import parse_qs, urlparse

from crescendo_engine.runtime import bind_text
_contract_text = bind_text()

from .cadence import (
    CommandCancelled, check_command_cancellation, command_cancellation_scope,
    detect_pdk_context, discover_eda_environment,
)
from .native_environment import restore_native_environment
from .config import DEFAULTS, MANUAL_KEYS, configure_eda, deep_merge, persist_eda_setup
from .ade_setup import (
    build_setup_reference, inspect_setup, normalize_setup_reference, resolve_setup_locator,
)
from .continuation import (
    ContinuationLease, ContinuationLeaseBusy, RuntimeOptimizationLease,
    checkpoint_continuation_id, clear_continuation_pending,
    continuation_checkpoint_path, continuation_pending_path,
    continuation_schema_sha256, validate_continuation_pending,
)
from .exports import candidates_csv_text
from .legacy_guardian import (
    LAUNCH_LOCK_NAME as LEGACY_GUARDIAN_LAUNCH_LOCK_NAME,
    MANIFEST_NAME as LEGACY_GUARDIAN_MANIFEST_NAME,
    MANIFEST_SCHEMA_VERSION as LEGACY_GUARDIAN_PROTOCOL,
    inspect_legacy_guardian_attempt,
    resolve_worker_guardian_token_identity,
    quiesce_legacy_guardian_attempt,
)
from .multiobjective import build_objective_presentation
from .retention import (
    clear_retained_spectre_logs,
    cleanup_abandoned_controller_epoch, cleanup_abandoned_nonterminal_candidate,
    cleanup_abandoned_worker_epoch, cleanup_terminal_candidate_artifacts,
    parse_worker_epoch_namespace, parse_worker_name, resume_pending_cleanups,
)
from .range_validation import (
    VariableRangeValidationError, run_range_validation,
    validate_variable_request, validate_variable_request_report,
)
from .resident_validation import ResidentValidationSession
from .persistent_maestro import persistent_worker_control_root
from .pdk import adapter_policy_options
from .prepared_freshness import capture_prepared_freshness, require_prepared_freshness
from .setup_baseline import (effective_setup_path, effective_setup_dir, freeze_setup_baseline,
    load_bound_setup, load_prepared_baseline, persist_prepared_baseline)
from .runner import (
    _recover_terminal_candidate_wal, _run_coordinated_resume, _terminal_wal_json_exact, checkpoint_candidates,
    cell_selection_scopes,
    checkpoint_continuation_schema, checkpoint_with_candidates, prepare_maestro,
    prepare_setup,
    resolve_design_config, resolve_metric_bindings, run_optimization, validate_checkpoint,
    validate_checkpoint_integrity,
)
from .resources import guardian_script_path, web_root
from .storage_preflight import (
    optimization_workspace_required, startup_storage_preflight,
)
from .utils import atomic_write_json, ensure_dir, is_under, load_json, run_id, sha256_file, utc_timestamp
from .runtime_layout import optimization_run_name
from .workspace import WorkspaceBuildFailure, verify_sources_unchanged
from .variable_scope_contract import require_unique_run_variables


WEB_ROOT = web_root()
_EPOCH_NAME = re.compile(r"^epoch_(\d+)$")
_CONTROLLER_SLOT_NAME = re.compile(r"^slot_(\d+)$")
_CONTROLLER_TOKEN = re.compile(r"^[0-9a-f]{32}$")
_BOOT_ID = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)
_GUARDIAN_TERMINAL_STATES = frozenset(("exited", "launch_failed"))
_TERMINAL_CANDIDATE_STATES = frozenset(("completed", "failed", "infrastructure_failed"))
_TERMINAL_RANGE_VALIDATION_STATES = frozenset(("completed", "failed", "cancelled"))
_TERMINAL_RUN_STATES = frozenset(("completed", "failed", "stopped"))
_FAILURE_STAGE_LABELS = {
    "workspace_build": "Workspace copy build",
    "pdk_callback": "PDK / CDF callback",
    "parameter_application": "Parameter application",
    "parameter_verification": "Parameter verification after simulation",
    "netlist_or_submission": "Netlisting / Spectre submission",
    "netlist_or_spectre": "Netlisting / Spectre boundary",
    "maestro_startup": "Maestro / Ocean startup",
    "persistent_startup": "Maestro / Ocean startup",
    "spectre": "Spectre simulation",
    "result_publication": "Result publication",
    "simulation_infrastructure": "Spectre infrastructure",
    "source_integrity": "Source integrity verification",
    "design_or_simulation": "Simulation / result evaluation",
    "infrastructure": "EDA infrastructure",
}
_CONTROLLER_STATES = frozenset((
    "launch_intent", "starting", "ready", "processing", "closed",
    "terminated", "termination_failed",
))
_CONTROLLER_SCHEMA = _contract_text("crescendo.persistent_controller.v1")
_CONTROLLER_LIVENESS_SCHEMA = _contract_text("crescendo.controller_liveness.v1")
_CANDIDATE_NAME = re.compile(r"^g(\d+)_c(\d+)$")
_LEGACY_ATTEMPT_NAME = re.compile(r"^attempt_(\d+)$")
_LEGACY_PRELAUNCH_ATTEMPT_FILES = frozenset((_contract_text("run_identity.json"),))
_RUN_DISPLAY_NAME_MAX_LENGTH = 120
_OPTIMIZED_VIEW_CLEANUP_PENDING = _contract_text(".optimized_schematic_cleanup_pending.json")
_OPTIMIZED_VIEW_CLEANUP_SCHEMA = _contract_text("crescendo.optimized_schematic_cleanup_pending.v1")


def _execution_text(value, limit=2048):
    """Bound diagnostic text without exposing protocol credentials or env dumps."""
    if not isinstance(value, str):
        return ""
    lines = []
    for line in value[-int(limit):].splitlines():
        if re.search(r"(?i)(?:token|nonce|password|secret|authorization|cookie)[\"']?\s*[=:]", line) or re.search(
                r"(?i)--(?:token|password|secret|authorization)(?:\s|=)", line) or re.match(
                r"\s*(?:export\s+)?[A-Z][A-Z0-9_]*\s*=", line):
            lines.append("[credential / environment record omitted]")
        else:
            lines.append(re.sub(r"\b[0-9a-fA-F]{32}\b", "[protocol id omitted]", line))
    return "\n".join(lines)


def _configured_execution_context(prepared, spec):
    """Capture only presentation fields; this does not select or submit jobs."""
    policy = (prepared or {}).get("job_policy") or {}
    properties = (policy.get("global") or {}).get("properties") or {}
    overrides = (spec or {}).get("maestro_job_overrides") or {}
    original = properties.get("jobsubmitcommand")
    command = overrides.get("command", original)
    return {
        "source": _execution_text(policy.get("source"), 120) or "unknown",
        "original_command": _execution_text(original, 8192) if isinstance(original, str) else None,
        "configured_command": _execution_text(command, 8192) if isinstance(command, str) else None,
        "original_max_jobs": _execution_text(str(properties.get("maxjobs")), 32)
        if properties.get("maxjobs") is not None else None,
        "configured_max_jobs": _execution_text(str(overrides.get("max_jobs", properties.get("maxjobs"))), 32)
        if overrides.get("max_jobs", properties.get("maxjobs")) is not None else None,
        "command_overridden": "command" in overrides,
        "max_jobs_overridden": "max_jobs" in overrides,
    }


def _selected_workspace_parameters(prepared, variables):
    """Pass only explicitly selected catalogue entries to mutable-view copying."""
    identifiers = {row.get("parameter_id") for row in variables or [] if isinstance(row, dict)}
    return [row for row in (prepared or {}).get("parameters") or []
            if row.get("parameter_id") in identifiers]


def _validated_run_display_name(value):
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("display_name must be a string")
    display_name = value.strip()
    if not display_name:
        raise ValueError("display_name must not be empty")
    if len(display_name) > _RUN_DISPLAY_NAME_MAX_LENGTH:
        raise ValueError(
            "display_name must be at most %d characters"
            % _RUN_DISPLAY_NAME_MAX_LENGTH
        )
    if any(not character.isprintable() for character in display_name):
        raise ValueError("display_name must not contain control characters")
    return display_name


def _nonnegative_run_count(value, fallback=0):
    try:
        return max(0, int(value))
    except (TypeError, ValueError, OverflowError):
        return max(0, int(fallback))


def _candidate_failure_stage(candidate):
    stage = str(candidate.get("failure_stage") or "").strip()
    if stage:
        return stage
    error = str(candidate.get("error") or "")
    if "WorkspaceBuildFailure" in error or "workspace build failed" in error.lower():
        return "workspace_build"
    failure_class = str(candidate.get("failure_class") or "").strip()
    return {
        "persistent_startup": "maestro_startup",
        "parameter_application": "parameter_application",
        "source_integrity": "source_integrity",
        "simulation_infrastructure": "simulation_infrastructure",
        "result_publication": "result_publication",
        "workspace_build": "workspace_build",
    }.get(failure_class, failure_class)


def _candidate_spectre_state(candidate, has_logs=False):
    state = str(candidate.get("spectre_state") or "").strip()
    if state:
        return state
    if has_logs or candidate.get("completion_evidence") or candidate.get("completion"):
        return "started"
    explicit_stage = str(candidate.get("failure_stage") or "").strip()
    stage = _candidate_failure_stage(candidate)
    if stage in (
            "workspace_build", "pdk_callback", "parameter_application",
            "netlist_or_submission", "maestro_startup", "persistent_startup") and (
                stage != "parameter_application" or explicit_stage):
        return "not_started"
    return "unknown"


def _candidate_failure_reason(candidate, limit=4096):
    reason = str(candidate.get("error") or candidate.get("message") or "").strip()
    limit = max(1, int(limit))
    if len(reason) > limit:
        return reason[-limit:]
    return reason


def _directory_setup_info(path):
    candidates = []
    for name, backend in (("maestro.sdb", "maestro"), ("data.sdb", "adexl")):
        candidate = os.path.join(path, name)
        if os.path.isfile(candidate):
            candidates.append({"path": candidate, "backend": backend})
    return {
        "is_setup": len(candidates) == 1,
        "ambiguous": len(candidates) > 1,
        "setup_backend": candidates[0]["backend"] if len(candidates) == 1 else None,
        "setup_db": candidates[0]["path"] if len(candidates) == 1 else None,
        "candidates": candidates,
    }


def _resume_cleanup_timeout(runtime_config, name, default):
    try:
        value = float((runtime_config or {}).get(name, default))
    except (TypeError, ValueError):
        value = float(default)
    if not math.isfinite(value):
        raise RuntimeError("%s must be finite" % name)
    return max(0.0, value)


def _guardian_cleanup_failure_summary(inspection):
    proof = inspection.get("termination_proof") or {}
    return {
        "state": inspection.get("state"),
        "proof": {"ok": proof.get("ok"), "kind": proof.get("kind")},
        "processes": [
            {
                "kind": row.get("kind"), "pid": row.get("pid"),
                "start_ticks": row.get("start_ticks"),
            }
            for row in inspection.get("processes") or []
        ],
    }


_SAFE_RUN_ID = re.compile(r"^[^\W_][\w.-]*$", re.UNICODE)
_RUN_STATUS_SNAPSHOT_MAX_BYTES = 1024 * 1024
_CHECKPOINT_SNAPSHOT_MAX_BYTES = 64 * 1024 * 1024
_PENDING_SNAPSHOT_MAX_BYTES = 65 * 1024 * 1024
_CHECKPOINT_SNAPSHOT_ATTEMPTS = 3


def _current_boot_id():
    try:
        with open("/proc/sys/kernel/random/boot_id", "r") as stream:
            value = stream.read().strip()
        return value or None
    except (IOError, OSError):
        return None


def _launch_boot_id(document):
    document = document or {}
    return document.get("launch_boot_id") or document.get("boot_id")


def _process_snapshot(pid):
    try:
        pid = int(pid)
    except (TypeError, ValueError):
        return {"pid": pid, "alive": False}
    root = "/proc/%d" % pid
    if pid <= 1 or not os.path.isdir(root):
        return {"pid": pid, "alive": False}
    try:
        with open(os.path.join(root, "stat"), "r") as stream:
            stat = stream.read()
        closing = stat.rfind(")")
        fields = stat[closing + 2:].split() if closing >= 0 else []
        state = fields[0] if fields else None
        start_ticks = int(fields[19]) if len(fields) > 19 else None
        if state == "Z":
            return {"pid": pid, "alive": False, "state": state, "start_ticks": start_ticks}
        with open(os.path.join(root, "cmdline"), "rb") as stream:
            command = [item.decode("utf-8", "replace") for item in stream.read().split(b"\0") if item]
        return {
            "pid": pid, "alive": True, "state": state, "command": command,
            "cwd": os.path.realpath(os.readlink(os.path.join(root, "cwd"))),
            "pgid": os.getpgid(pid), "start_ticks": start_ticks,
        }
    except (IOError, OSError, IndexError, ValueError) as error:
        return {
            "pid": pid, "alive": True,
            "identity_error": "%s: %s" % (error.__class__.__name__, error),
        }


def _command_option(command, name):
    try:
        return command[command.index(name) + 1]
    except (ValueError, IndexError):
        return None


def _controller_integer(document, name, allow_none=False):
    value = document.get(name)
    if allow_none and value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise RuntimeError("controller %s is not a positive integer" % name)
    return value


def _controller_index(document, name, expected):
    value = document.get(name)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0 or value != int(expected):
        raise RuntimeError("controller %s does not match its directory" % name)
    return value


def _controller_path_matches(value, expected):
    return bool(
        isinstance(value, str) and os.path.isabs(value)
        and os.path.abspath(value) == os.path.abspath(expected)
        and os.path.realpath(value) == os.path.realpath(expected)
    )


def _load_controller_document(path, description):
    if os.path.islink(path) or not os.path.isfile(path):
        raise RuntimeError("controller %s is not a regular file: %s" % (description, path))
    document = load_json(path)
    if not isinstance(document, dict):
        raise RuntimeError("controller %s is not a JSON object" % description)
    return document


def _controller_command_matches(command, bootstrap_path, expected_executable=None):
    if not isinstance(command, list) or len(command) != 5:
        return False
    if not all(isinstance(item, str) and item for item in command):
        return False
    if command[1:4] != ["-m", _contract_text("crescendo_engine.persistent_controller"), "--bootstrap"]:
        return False
    if not _controller_path_matches(command[4], bootstrap_path):
        return False
    if not os.path.isabs(command[0]):
        return False
    return bool(
        expected_executable is None
        or os.path.realpath(command[0]) == os.path.realpath(expected_executable)
    )


def _controller_liveness_lock_status(epoch_root):
    epoch_root = os.path.abspath(epoch_root)
    lock_path = os.path.join(epoch_root, "controller_liveness.lock")
    if not os.path.lexists(lock_path):
        return {"state": "missing", "path": lock_path}
    try:
        before = os.lstat(lock_path)
    except OSError as error:
        raise RuntimeError("controller liveness lock cannot be inspected: %s" % error)
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        raise RuntimeError("controller liveness lock is not a regular non-symbolic file")
    if os.path.ismount(lock_path):
        raise RuntimeError("controller liveness lock must not be a mount point")
    flags = os.O_RDWR | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = None
    try:
        descriptor = os.open(lock_path, flags)
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode) or not os.path.samestat(before, opened):
            raise RuntimeError("controller liveness lock changed while being opened")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (IOError, OSError) as error:
            if getattr(error, "errno", None) in (errno.EACCES, errno.EAGAIN):
                return {"state": "held", "path": lock_path}
            raise RuntimeError("controller liveness lock cannot be verified: %s" % error)
        current = os.lstat(lock_path)
        if not os.path.samestat(current, os.fstat(descriptor)):
            raise RuntimeError("controller liveness lock changed after acquisition")
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        return {"state": "released", "path": lock_path}
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _worker_guardian_launch_lock_status(lock_path):
    lock_path = os.path.abspath(lock_path)
    if not os.path.lexists(lock_path):
        return {"state": "missing", "path": lock_path}
    try:
        before = os.lstat(lock_path)
    except OSError as error:
        raise RuntimeError("worker guardian launch lock cannot be inspected: %s" % error)
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
        raise RuntimeError("worker guardian launch lock is not an owner-only regular file")
    if os.path.ismount(lock_path):
        raise RuntimeError("worker guardian launch lock must not be a mount point")
    flags = os.O_RDWR | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = None
    try:
        descriptor = os.open(lock_path, flags)
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode) or opened.st_nlink != 1 or not os.path.samestat(before, opened):
            raise RuntimeError("worker guardian launch lock changed while being opened")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (IOError, OSError) as error:
            if getattr(error, "errno", None) in (errno.EACCES, errno.EAGAIN):
                return {"state": "held", "path": lock_path}
            raise RuntimeError("worker guardian launch lock cannot be verified: %s" % error)
        current = os.lstat(lock_path)
        if not os.path.samestat(current, os.fstat(descriptor)):
            raise RuntimeError("worker guardian launch lock changed after acquisition")
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        return {"state": "released", "path": lock_path}
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _validate_controller_liveness_fields(document, lock_path, description, required=False):
    schema = document.get("liveness_lock_schema")
    recorded_path = document.get("liveness_lock_path")
    present = schema is not None or recorded_path is not None
    if required and not present:
        raise RuntimeError("controller %s lacks liveness lock identity" % description)
    if not present:
        return False
    if schema != _CONTROLLER_LIVENESS_SCHEMA:
        raise RuntimeError("controller %s liveness lock schema is not recognized" % description)
    if not _controller_path_matches(recorded_path, lock_path):
        raise RuntimeError("controller %s liveness lock path does not match its epoch" % description)
    return True


def _matching_controller_processes(bootstrap_path):
    from .runtime import application_root
    project_root = application_root(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    matches = []
    for name in os.listdir("/proc"):
        if not name.isdigit():
            continue
        snapshot = _process_snapshot(int(name))
        if (
            snapshot.get("alive") and not snapshot.get("identity_error")
            and snapshot.get("cwd") == os.path.realpath(project_root)
            and snapshot.get("pgid") == snapshot.get("pid")
            and _controller_command_matches(snapshot.get("command"), bootstrap_path)
        ):
            matches.append(snapshot)
    return matches


def _controller_from_liveness_lock(epoch_root, bootstrap_path, lock_status, status=None):
    if lock_status.get("state") == "released":
        return {
            "state": "dead", "status": status, "pid": None,
            "cleanup_supported": True,
            "proof": {"ok": True, "kind": "controller_not_started"},
        }
    if lock_status.get("state") == "missing":
        return {
            "state": "ambiguous", "status": status, "pid": None,
            "cleanup_supported": False,
            "proof": {"ok": False, "kind": "current_boot_pid_identity_missing"},
        }
    matches = _matching_controller_processes(bootstrap_path)
    if len(matches) == 1:
        snapshot = matches[0]
        return {
            "state": "active", "status": status, "pid": snapshot.get("pid"),
            "pgid": snapshot.get("pgid"), "start_ticks": snapshot.get("start_ticks"),
            "cleanup_supported": True, "identity_proven": True,
            "proof": {
                "ok": False, "kind": "owned_controller_is_alive",
                "observed": snapshot,
            },
        }
    return {
        "state": "ambiguous", "status": status, "pid": None,
        "cleanup_supported": True,
        "proof": {
            "ok": False,
            "kind": "held_liveness_lock_without_unique_controller_identity",
            "matching_pids": [row.get("pid") for row in matches],
        },
    }


def _inspect_partial_controller_epoch(epoch_root, origin_root, slot_index, epoch_index,
                                      bootstrap_exists, state_exists):
    if state_exists and not bootstrap_exists:
        raise RuntimeError("controller state exists without its bootstrap")
    lock_status = _controller_liveness_lock_status(epoch_root)
    lock_path = lock_status["path"]
    bootstrap_path = os.path.join(epoch_root, _contract_text("bootstrap.json"))
    if not bootstrap_exists:
        allowed = {"controller_liveness.lock"}
        unexpected = sorted(set(os.listdir(epoch_root)) - allowed)
        if unexpected:
            raise RuntimeError(
                "partial controller epoch has artifacts without a bootstrap: %s" % unexpected
            )
        if lock_status.get("state") == "missing":
            return {
                "state": "dead", "slot_index": int(slot_index),
                "epoch_index": int(epoch_index), "status": None, "pid": None,
                "cleanup_supported": False,
                "proof": {"ok": True, "kind": "controller_not_started"},
            }
        result = _controller_from_liveness_lock(
            epoch_root, bootstrap_path, lock_status, status=None,
        )
        result.update({"slot_index": int(slot_index), "epoch_index": int(epoch_index)})
        return result

    bootstrap = _load_controller_document(bootstrap_path, "bootstrap")
    if bootstrap.get("schema_version") != _CONTROLLER_SCHEMA:
        raise RuntimeError("controller bootstrap schema is not recognized")
    _controller_index(bootstrap, "worker_index", slot_index)
    _controller_index(bootstrap, "controller_epoch", epoch_index)
    token = bootstrap.get("token")
    if not isinstance(token, str) or not _CONTROLLER_TOKEN.match(token):
        raise RuntimeError("controller bootstrap token is malformed")
    if not _controller_path_matches(bootstrap.get("run_root"), origin_root):
        raise RuntimeError("controller bootstrap run root does not match checkpoint origin")
    expected_paths = {
        "requests_root": os.path.join(epoch_root, "requests"),
        "responses_root": os.path.join(epoch_root, "responses"),
        "state_path": os.path.join(epoch_root, _contract_text("controller_state.json")),
        "ready_path": os.path.join(epoch_root, _contract_text("ready.json")),
    }
    for name, expected in expected_paths.items():
        if not _controller_path_matches(bootstrap.get(name), expected):
            raise RuntimeError("controller bootstrap %s does not match its epoch" % name)
    for name in ("requests_root", "responses_root"):
        path = expected_paths[name]
        if os.path.islink(path) or not os.path.isdir(path):
            raise RuntimeError("controller %s is not a safe directory" % name)
    _controller_integer(bootstrap, "parent_pid")
    _controller_integer(bootstrap, "parent_start_ticks")
    _validate_controller_liveness_fields(bootstrap, lock_path, "bootstrap", required=True)
    if lock_status.get("state") == "missing":
        raise RuntimeError("controller bootstrap references a missing liveness lock")
    result = _controller_from_liveness_lock(
        epoch_root, bootstrap_path, lock_status, status="bootstrap_only",
    )
    result.update({"slot_index": int(slot_index), "epoch_index": int(epoch_index)})
    return result


def _inspect_controller_epoch(epoch_root, origin_root, slot_index, epoch_index):
    epoch_root = os.path.abspath(epoch_root)
    origin_root = os.path.abspath(origin_root)
    bootstrap_path = os.path.join(epoch_root, _contract_text("bootstrap.json"))
    state_path = os.path.join(epoch_root, _contract_text("controller_state.json"))
    bootstrap_exists = os.path.lexists(bootstrap_path)
    state_exists = os.path.lexists(state_path)
    if not bootstrap_exists or not state_exists:
        return _inspect_partial_controller_epoch(
            epoch_root, origin_root, slot_index, epoch_index,
            bootstrap_exists, state_exists,
        )
    bootstrap = _load_controller_document(bootstrap_path, "bootstrap")
    state = _load_controller_document(state_path, "state")
    for description, document in (("bootstrap", bootstrap), ("state", state)):
        if document.get("schema_version") != _CONTROLLER_SCHEMA:
            raise RuntimeError("controller %s schema is not recognized" % description)
        _controller_index(document, "worker_index", slot_index)
        _controller_index(document, "controller_epoch", epoch_index)
        token = document.get("token")
        if not isinstance(token, str) or not _CONTROLLER_TOKEN.match(token):
            raise RuntimeError("controller %s token is malformed" % description)
        if not _controller_path_matches(document.get("run_root"), origin_root):
            raise RuntimeError("controller %s run root does not match checkpoint origin" % description)
    if state.get("token") != bootstrap.get("token"):
        raise RuntimeError("controller state and bootstrap tokens disagree")
    if state.get("status") not in _CONTROLLER_STATES:
        raise RuntimeError("controller state status is not recognized")

    expected_paths = {
        "requests_root": os.path.join(epoch_root, "requests"),
        "responses_root": os.path.join(epoch_root, "responses"),
        "state_path": state_path,
        "ready_path": os.path.join(epoch_root, _contract_text("ready.json")),
    }
    for name, expected in expected_paths.items():
        if not _controller_path_matches(bootstrap.get(name), expected):
            raise RuntimeError("controller bootstrap %s does not match its epoch" % name)
    for name in ("requests_root", "responses_root"):
        path = expected_paths[name]
        if os.path.islink(path) or not os.path.isdir(path):
            raise RuntimeError("controller %s is not a safe directory" % name)
    recorded_bootstrap_path = state.get("bootstrap_path")
    if recorded_bootstrap_path is not None and not _controller_path_matches(
        recorded_bootstrap_path, bootstrap_path,
    ):
        raise RuntimeError("controller state bootstrap path does not match its epoch")
    recorded_command = state.get("command")
    if recorded_command is not None and not _controller_command_matches(
        recorded_command, bootstrap_path,
    ):
        raise RuntimeError("controller state command is malformed")

    for name in ("parent_pid", "parent_start_ticks"):
        state_value = _controller_integer(state, name)
        bootstrap_value = _controller_integer(bootstrap, name)
        if state_value != bootstrap_value:
            raise RuntimeError("controller state and bootstrap %s disagree" % name)
    lock_status = _controller_liveness_lock_status(epoch_root)
    lock_path = lock_status["path"]
    bootstrap_has_lock = _validate_controller_liveness_fields(
        bootstrap, lock_path, "bootstrap",
    )
    state_has_lock = _validate_controller_liveness_fields(state, lock_path, "state")
    if bootstrap_has_lock != state_has_lock:
        raise RuntimeError("controller state and bootstrap liveness identities disagree")
    if bootstrap_has_lock and lock_status.get("state") == "missing":
        raise RuntimeError("controller liveness lock identity is recorded but the lock is missing")
    launch_boot_id = state.get("launch_boot_id")
    if not isinstance(launch_boot_id, str) or not _BOOT_ID.match(launch_boot_id):
        raise RuntimeError("controller launch_boot_id is malformed or unavailable")

    pid = _controller_integer(state, "pid", allow_none=True)
    pgid = _controller_integer(state, "pgid", allow_none=True)
    start_ticks = _controller_integer(state, "pid_start_ticks", allow_none=True)
    if any(value is None for value in (pid, pgid, start_ticks)):
        if not all(value is None for value in (pid, pgid, start_ticks)):
            raise RuntimeError("controller PID identity is incomplete")
        pid_identity = None
    else:
        if pgid != pid:
            raise RuntimeError("controller process group is not isolated")
        pid_identity = {"pid": pid, "pgid": pgid, "start_ticks": start_ticks}

    current_boot_id = _current_boot_id()
    if current_boot_id and launch_boot_id != current_boot_id:
        if lock_status.get("state") == "held":
            return {
                "state": "ambiguous", "slot_index": int(slot_index),
                "epoch_index": int(epoch_index), "status": state.get("status"),
                "pid": pid, "cleanup_supported": bootstrap_has_lock,
                "proof": {"ok": False, "kind": "previous_boot_epoch_has_held_liveness_lock"},
            }
        return {
            "state": "dead", "slot_index": int(slot_index), "epoch_index": int(epoch_index),
            "status": state.get("status"), "pid": pid,
            "cleanup_supported": bootstrap_has_lock,
            "proof": {
                "ok": True, "kind": "prior_boot_processes_dead", "launch_boot_id": launch_boot_id,
                "current_boot_id": current_boot_id,
            },
        }
    if pid_identity is None:
        if bootstrap_has_lock:
            result = _controller_from_liveness_lock(
                epoch_root, bootstrap_path, lock_status, status=state.get("status"),
            )
            result.update({"slot_index": int(slot_index), "epoch_index": int(epoch_index)})
            return result
        return {
            "state": "ambiguous", "slot_index": int(slot_index), "epoch_index": int(epoch_index),
            "status": state.get("status"), "pid": None, "cleanup_supported": False,
            "proof": {"ok": False, "kind": "current_boot_pid_identity_missing"},
        }

    snapshot = _process_snapshot(pid)
    if not snapshot.get("alive"):
        if lock_status.get("state") == "held":
            result = _controller_from_liveness_lock(
                epoch_root, bootstrap_path, lock_status, status=state.get("status"),
            )
            result.update({"slot_index": int(slot_index), "epoch_index": int(epoch_index)})
            return result
        return {
            "state": "dead", "slot_index": int(slot_index), "epoch_index": int(epoch_index),
            "status": state.get("status"), "pid": pid,
            "cleanup_supported": bootstrap_has_lock,
            "proof": {"ok": True, "kind": "recorded_pid_is_dead"},
        }
    observed_start_ticks = snapshot.get("start_ticks")
    if (
        isinstance(observed_start_ticks, int) and not isinstance(observed_start_ticks, bool)
        and observed_start_ticks != start_ticks
    ):
        if lock_status.get("state") == "held":
            result = _controller_from_liveness_lock(
                epoch_root, bootstrap_path, lock_status, status=state.get("status"),
            )
            result.update({"slot_index": int(slot_index), "epoch_index": int(epoch_index)})
            return result
        return {
            "state": "dead", "slot_index": int(slot_index), "epoch_index": int(epoch_index),
            "status": state.get("status"), "pid": pid,
            "cleanup_supported": bootstrap_has_lock,
            "proof": {
                "ok": True, "kind": "recorded_pid_was_reused", "recorded_start_ticks": start_ticks,
                "observed_start_ticks": observed_start_ticks,
            },
        }
    expected_command = recorded_command or [
        os.path.abspath(sys.executable), "-m", _contract_text("crescendo_engine.persistent_controller"),
        "--bootstrap", bootstrap_path,
    ]
    from .runtime import application_root
    project_root = application_root(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    identity_matches = bool(
        not snapshot.get("identity_error")
        and snapshot.get("start_ticks") == start_ticks
        and snapshot.get("pgid") == pgid
        and snapshot.get("cwd") == os.path.realpath(project_root)
        and _controller_command_matches(
            snapshot.get("command"), bootstrap_path,
            expected_executable=expected_command[0],
        )
        and (recorded_command is None or snapshot.get("command")[1:] == recorded_command[1:])
    )
    if identity_matches and bootstrap_has_lock and lock_status.get("state") != "held":
        identity_matches = False
    return {
        "state": "active" if identity_matches else "ambiguous",
        "slot_index": int(slot_index), "epoch_index": int(epoch_index),
        "status": state.get("status"), "pid": pid, "pgid": pgid,
        "start_ticks": start_ticks, "cleanup_supported": bootstrap_has_lock,
        "identity_proven": identity_matches,
        "proof": {
            "ok": False,
            "kind": "owned_controller_is_alive" if identity_matches else "live_pid_identity_mismatch",
            "observed": {
                "pid": snapshot.get("pid"), "pgid": snapshot.get("pgid"),
                "start_ticks": snapshot.get("start_ticks"), "cwd": snapshot.get("cwd"),
                "command": snapshot.get("command"), "identity_error": snapshot.get("identity_error"),
            },
        },
    }


def _same_process_identity(snapshot, pid, pgid, start_ticks):
    return bool(
        snapshot.get("alive") and not snapshot.get("identity_error")
        and snapshot.get("pid") == pid and snapshot.get("pgid") == pgid
        and snapshot.get("start_ticks") == start_ticks
    )


def _terminate_owned_controller(inspection, timeout_seconds):
    if inspection.get("state") != "active" or inspection.get("identity_proven") is not True:
        raise RuntimeError("controller termination requires an exact owned process identity")
    pid = int(inspection["pid"])
    pgid = int(inspection["pgid"])
    start_ticks = int(inspection["start_ticks"])
    if pid <= 1 or pgid != pid:
        raise RuntimeError("controller termination requires an isolated non-system process group")
    before = _process_snapshot(pid)
    if not _same_process_identity(before, pid, pgid, start_ticks):
        raise RuntimeError("controller process identity changed before termination")
    timeout_seconds = max(0.5, float(timeout_seconds))
    events = []
    for signal_value, label, fraction in (
        (signal.SIGTERM, "SIGTERM", 0.6),
        (signal.SIGKILL, "SIGKILL", 0.4),
    ):
        current = _process_snapshot(pid)
        if not _same_process_identity(current, pid, pgid, start_ticks):
            break
        os.killpg(pgid, signal_value)
        events.append({"signal": label, "sent_at": utc_timestamp()})
        deadline = time.monotonic() + timeout_seconds * fraction
        while time.monotonic() < deadline:
            current = _process_snapshot(pid)
            if not _same_process_identity(current, pid, pgid, start_ticks):
                break
            time.sleep(0.05)
    current = _process_snapshot(pid)
    if _same_process_identity(current, pid, pgid, start_ticks):
        raise RuntimeError("owned persistent controller did not terminate within the resume timeout")
    return {
        "state": "dead",
        "slot_index": inspection.get("slot_index"),
        "epoch_index": inspection.get("epoch_index"),
        "status": inspection.get("status"),
        "pid": pid,
        "cleanup_supported": inspection.get("cleanup_supported") is True,
        "proof": {
            "ok": True,
            "kind": "controller_termination_proven",
            "pid": pid,
            "pgid": pgid,
            "pid_start_ticks": start_ticks,
            "signals": events,
        },
    }


def _checkpoint_origin_path(checkpoint, runtime_root):
    manifest = checkpoint.get("manifest") or {}
    origin_root = manifest.get("origin_run_root")
    origin_id = manifest.get("origin_run_id")
    if not origin_root or not origin_id:
        return None, "checkpoint has no local origin run hint"
    if not isinstance(origin_root, str) or not isinstance(origin_id, str):
        raise RuntimeError("checkpoint origin run hint is malformed")
    if origin_id in ("", ".", "..") or os.path.basename(origin_id) != origin_id:
        raise RuntimeError("checkpoint origin run id is malformed")
    runtime_absolute = os.path.abspath(os.path.expanduser(runtime_root))
    runtime_root = os.path.realpath(runtime_absolute)
    absolute_origin = os.path.abspath(os.path.expanduser(origin_root))
    resolved_origin = os.path.realpath(absolute_origin)
    exists = os.path.lexists(absolute_origin)
    if os.path.basename(absolute_origin) != origin_id:
        if exists:
            raise RuntimeError("checkpoint origin path and run id disagree")
        return None, "checkpoint origin run is unavailable on this machine"
    if resolved_origin == runtime_root:
        raise RuntimeError("checkpoint origin resolves to the configured runtime root")
    if not is_under(resolved_origin, runtime_root):
        if exists:
            raise RuntimeError("checkpoint origin exists outside the configured runtime root")
        return None, "checkpoint origin run is unavailable on this machine"
    if not exists:
        return None, "checkpoint origin run is unavailable on this machine"
    try:
        relative_origin = os.path.relpath(absolute_origin, runtime_absolute)
    except ValueError:
        relative_origin = ".."
    expected_resolved = os.path.abspath(os.path.join(runtime_root, relative_origin))
    if (
        relative_origin == os.pardir or relative_origin.startswith(os.pardir + os.sep)
        or os.path.islink(absolute_origin) or resolved_origin != expected_resolved
    ):
        raise RuntimeError("checkpoint origin path contains a symbolic-link alias")
    if not os.path.isdir(absolute_origin):
        raise RuntimeError("checkpoint origin is not a directory")
    if os.path.dirname(absolute_origin) != runtime_absolute:
        raise RuntimeError("checkpoint origin must be a direct child of the configured runtime root")
    if checkpoint.get("schema_version") == _contract_text("crescendo.checkpoint.v1"):
        status_path = os.path.join(absolute_origin, _contract_text("status.json"))
        if os.path.islink(status_path) or not os.path.isfile(status_path):
            raise RuntimeError("checkpoint origin status.json is missing or unsafe")
        status = load_json(status_path)
        if not isinstance(status, dict) or status.get("schema_version") != _contract_text("crescendo.run_status.v1"):
            raise RuntimeError("checkpoint origin status schema is not recognized")
        if status.get("run_id") != origin_id:
            raise RuntimeError("checkpoint origin status run_id does not match")
        status_root = status.get("run_root")
        if (
            not isinstance(status_root, str) or not os.path.isabs(status_root)
            or os.path.realpath(status_root) != resolved_origin
        ):
            raise RuntimeError("checkpoint origin status run_root does not match")
        continuation_id = manifest.get("continuation_id")
        if continuation_id is not None and status.get("continuation_id") != str(continuation_id):
            raise RuntimeError("checkpoint origin continuation_id does not match")
    return absolute_origin, None


def _checkpoint_planned_candidates(checkpoint):
    state = checkpoint.get("state") or {}
    plan = state.get("active_plan")
    if not plan:
        return []
    parameter_ids = list(checkpoint.get("parameter_ids") or [])
    generation = int(plan.get("generation"))
    rows = []
    for packed in plan.get("planned") or []:
        index = int(packed[0])
        values = dict(zip(parameter_ids, packed[1]))
        rows.append((index, generation, values))
    return rows


def _persist_reconciled_origin_checkpoint(origin_root, checkpoint):
    checkpoint_path = os.path.join(origin_root, _contract_text("checkpoint.json"))
    if os.path.lexists(checkpoint_path) and (
        os.path.islink(checkpoint_path) or not os.path.isfile(checkpoint_path)
    ):
        raise RuntimeError("origin checkpoint.json is not a safe regular file")
    validate_checkpoint_integrity(checkpoint)
    if os.path.isfile(checkpoint_path):
        existing = load_json(checkpoint_path)
        validate_checkpoint_integrity(existing)
        if not _checkpoint_immutable_schema_equal(existing, checkpoint):
            raise RuntimeError("existing origin checkpoint belongs to a different optimization state")
        existing_revision = _checkpoint_revision(existing)
        reconciled_revision = _checkpoint_revision(checkpoint)
        if existing_revision > reconciled_revision:
            raise RuntimeError(
                "refusing to overwrite newer origin checkpoint revision %d with revision %d"
                % (existing_revision, reconciled_revision)
            )
        if existing_revision == reconciled_revision:
            if not _checkpoints_semantically_equal(existing, checkpoint):
                raise RuntimeError(
                    "origin checkpoint forks at revision %d; refusing ambiguous overwrite"
                    % existing_revision
                )
            return checkpoint_path
        if not _checkpoint_candidate_extension(checkpoint, existing):
            raise RuntimeError(
                "reconciled checkpoint is not an exact candidate extension of origin state"
            )
    atomic_write_json(checkpoint_path, checkpoint)
    if os.path.islink(checkpoint_path) or not os.path.isfile(checkpoint_path):
        raise RuntimeError("reconciled origin checkpoint was not persisted safely")
    persisted = load_json(checkpoint_path)
    validate_checkpoint_integrity(persisted)
    if persisted.get("payload_sha256") != checkpoint.get("payload_sha256"):
        raise RuntimeError("reconciled origin checkpoint verification failed")
    return checkpoint_path


def _checkpoint_revision(checkpoint):
    value = (checkpoint.get("state") or {}).get("checkpoint_revision", 0)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise RuntimeError("checkpoint revision is malformed")
    return value


def _checkpoint_semantic_payload(checkpoint):
    # An embedded verified fixed prefix is a transport representation of the
    # same candidate_store identity. Validate it before excluding that field.
    if checkpoint.get("candidate_store") is not None:
        validate_checkpoint_integrity(checkpoint)
    payload = json.loads(json.dumps(checkpoint))
    if payload.get("candidate_store") is not None:
        payload.pop("candidate_store_embedded", None)
    payload.pop("created_at", None)
    payload.pop("payload_sha256", None)
    return payload


def _checkpoints_semantically_equal(left, right):
    return _checkpoint_semantic_payload(left) == _checkpoint_semantic_payload(right)


def _checkpoint_immutable_schema_equal(left, right):
    return all(left.get(name) == right.get(name) for name in (
        "schema_version", "manifest", "spec", "parameter_ids", "candidate_layout",
    ))


def _checkpoint_continuation_schema_equal(left, right):
    from .runner import checkpoint_model_profile_transition
    before = (left.get("spec") or {}).get("simulator_model_profile_identity")
    after = (right.get("spec") or {}).get("simulator_model_profile_identity")
    compatible = before == after
    if not compatible:
        left_revision = (left.get("state") or {}).get("checkpoint_revision", 0)
        right_revision = (right.get("state") or {}).get("checkpoint_revision", 0)
        if left_revision < right_revision:
            compatible = checkpoint_model_profile_transition(left, right)
        elif right_revision < left_revision:
            compatible = checkpoint_model_profile_transition(right, left)
    return (
        checkpoint_continuation_schema(left) == checkpoint_continuation_schema(right)
        and compatible
        and checkpoint_continuation_id(left) == checkpoint_continuation_id(right)
    )


def _checkpoint_candidate_extension(newer, older, portable=False):
    schema_matches = (
        _checkpoint_continuation_schema_equal(newer, older)
        if portable else _checkpoint_immutable_schema_equal(newer, older)
    )
    if not schema_matches:
        return False
    older_rows = dict(
        (row[0], row) for row in (older.get("candidates") or [])
        if isinstance(row, list) and row
    )
    newer_rows = dict(
        (row[0], row) for row in (newer.get("candidates") or [])
        if isinstance(row, list) and row
    )
    if len(older_rows) != len(older.get("candidates") or []):
        return False
    if len(newer_rows) != len(newer.get("candidates") or []):
        return False
    if any(newer_rows.get(candidate_id) != row for candidate_id, row in older_rows.items()):
        return False
    rows = checkpoint_candidates(newer)
    expected_counts = {
        "completed_candidates": len([row for row in rows if row.get("status") == "completed"]),
        "failed_candidates": len([row for row in rows if row.get("status") == "failed"]),
        "infrastructure_failed_candidates": len([
            row for row in rows if row.get("status") == "infrastructure_failed"
        ]),
    }
    return all(
        (newer.get("state") or {}).get(name, 0) == value
        for name, value in expected_counts.items()
    )


def _checkpoint_reconciliation_descendant(newer, older, portable=False):
    if not _checkpoint_candidate_extension(newer, older, portable=portable):
        return False
    newer_state = json.loads(json.dumps(newer.get("state") or {}))
    older_state = json.loads(json.dumps(older.get("state") or {}))
    if portable:
        try:
            newer_generation = _checkpoint_revision(newer)
            older_generation = _checkpoint_revision(older)
            if newer_generation <= older_generation:
                return False
            newer_next = int(newer_state.get("next_generation", 0))
            older_next = int(older_state.get("next_generation", 0))
        except (TypeError, ValueError):
            return False
        if newer_next < older_next:
            return False
        newer_history = newer_state.get("selection_history") or []
        older_history = older_state.get("selection_history") or []
        if (
            not isinstance(newer_history, list) or not isinstance(older_history, list)
            or newer_history[:len(older_history)] != older_history
        ):
            return False
        older_terminal = older_state.get("terminal_state")
        if older_terminal is not None and (
            newer_state.get("terminal_state") != older_terminal
            or newer_state.get("terminal_reason") != older_state.get("terminal_reason")
        ):
            return False
        older_checksum = older.get("payload_sha256")
        newer_ancestry = newer_state.get("ancestor_payload_sha256") or []
        older_ancestry = older_state.get("ancestor_payload_sha256") or []
        if newer_ancestry[:len(older_ancestry)] != older_ancestry:
            return False
        if older_checksum and newer_ancestry:
            return older_checksum in newer_ancestry
        if newer_next == older_next:
            newer_plan = newer_state.get("active_plan")
            older_plan = older_state.get("active_plan")
            if older_plan is not None:
                if not isinstance(newer_plan, dict):
                    return False
                older_planned = older_plan.get("planned") or []
                newer_planned = newer_plan.get("planned") or []
                if newer_planned[:len(older_planned)] != older_planned:
                    return False
                if int(newer_plan.get("next_index", -1)) < int(older_plan.get("next_index", 0)):
                    return False
                if int(newer_plan.get("replacements", -1)) < int(older_plan.get("replacements", 0)):
                    return False
        return True
    counter_names = (
        "checkpoint_revision", "completed_candidates", "failed_candidates",
        "infrastructure_failed_candidates", "ancestor_payload_sha256",
    )
    for name in counter_names:
        newer_state.pop(name, None)
        older_state.pop(name, None)
    if newer_state != older_state:
        return False
    return True


def _persist_continuation_anchor(runtime_root, checkpoint):
    validate_checkpoint_integrity(checkpoint)
    anchor_path = continuation_checkpoint_path(
        runtime_root, checkpoint_continuation_id(checkpoint), create=True,
    )
    if os.path.lexists(anchor_path) and (
        os.path.islink(anchor_path) or not os.path.isfile(anchor_path)
    ):
        raise RuntimeError("continuation checkpoint anchor is not a safe regular file")
    atomic_write_json(anchor_path, checkpoint)
    if os.path.islink(anchor_path) or not os.path.isfile(anchor_path):
        raise RuntimeError("continuation checkpoint anchor was not persisted safely")
    persisted = load_json(anchor_path)
    validate_checkpoint_integrity(persisted)
    if persisted != checkpoint:
        raise RuntimeError("continuation checkpoint anchor verification failed")
    return anchor_path


def _checkpoint_continuation_schema_payload(checkpoint):
    return checkpoint_continuation_schema(checkpoint)


def _legacy_checkpoint_continuation_schema_payload(checkpoint):
    manifest = json.loads(json.dumps(checkpoint.get("manifest") or {}))
    manifest.pop("origin_run_id", None)
    manifest.pop("origin_run_root", None)
    return {
        "schema_version": checkpoint.get("schema_version"),
        "manifest": manifest,
        "spec": checkpoint.get("spec"),
        "parameter_ids": checkpoint.get("parameter_ids"),
        "candidate_layout": checkpoint.get("candidate_layout"),
    }


def _validated_pending_checkpoint(pending, continuation_id, run_checkpoint=None,
                                  anchored_checkpoint=None):
    validate_continuation_pending(pending)
    pending_checkpoint = pending["checkpoint"]
    validate_checkpoint_integrity(pending_checkpoint)
    if (
        pending.get("continuation_id") != continuation_id
        or checkpoint_continuation_id(pending_checkpoint) != continuation_id
    ):
        raise RuntimeError("continuation pending journal identity does not match")
    if pending.get("revision") != _checkpoint_revision(pending_checkpoint):
        raise RuntimeError("continuation pending journal revision does not match")
    expected_schema_sha256 = continuation_schema_sha256(
        _checkpoint_continuation_schema_payload(pending_checkpoint)
    )
    legacy_schema_sha256 = continuation_schema_sha256(
        _legacy_checkpoint_continuation_schema_payload(pending_checkpoint)
    )
    if pending.get("continuation_schema_sha256") not in (
            expected_schema_sha256, legacy_schema_sha256):
        raise RuntimeError("continuation pending journal immutable schema does not match")

    if run_checkpoint is not None:
        validate_checkpoint_integrity(run_checkpoint)
        if (
            checkpoint_continuation_id(run_checkpoint) != continuation_id
            or not _checkpoint_continuation_schema_equal(
                pending_checkpoint, run_checkpoint,
            )
        ):
            raise RuntimeError(
                "continuation pending journal belongs to a different run checkpoint"
            )
        run_revision = _checkpoint_revision(run_checkpoint)
        pending_revision = _checkpoint_revision(pending_checkpoint)
        if pending_revision < run_revision:
            raise RuntimeError("continuation pending journal is stale")
        if pending_revision == run_revision:
            if not _checkpoints_semantically_equal(
                pending_checkpoint, run_checkpoint,
            ):
                raise RuntimeError(
                    "continuation pending journal forks at the run checkpoint revision"
                )
        elif not _checkpoint_reconciliation_descendant(
            pending_checkpoint, run_checkpoint, portable=True,
        ):
            raise RuntimeError(
                "continuation pending journal is not a legal run checkpoint descendant"
            )

    if anchored_checkpoint is not None:
        validate_checkpoint_integrity(anchored_checkpoint)
        if (
            checkpoint_continuation_id(anchored_checkpoint) != continuation_id
            or not _checkpoint_continuation_schema_equal(
                anchored_checkpoint, pending_checkpoint,
            )
        ):
            raise RuntimeError(
                "continuation pending journal anchor identity does not match"
            )
        anchor_checksum = anchored_checkpoint.get("payload_sha256")
        pending_checksum = pending_checkpoint.get("payload_sha256")
        if anchor_checksum == pending_checksum:
            if not _checkpoints_semantically_equal(
                anchored_checkpoint, pending_checkpoint,
            ):
                raise RuntimeError(
                    "continuation pending journal conflicts with promoted anchor"
                )
        else:
            if pending.get("parent_payload_sha256") != anchor_checksum:
                raise RuntimeError(
                    "continuation pending journal parent checksum does not match"
                )
            if _checkpoint_revision(anchored_checkpoint) >= _checkpoint_revision(
                pending_checkpoint,
            ):
                raise RuntimeError(
                    "continuation pending journal is stale or forks from its anchor"
                )
            if not _checkpoint_reconciliation_descendant(
                pending_checkpoint, anchored_checkpoint, portable=True,
            ):
                raise RuntimeError(
                    "continuation pending journal is not a legal anchor descendant"
                )
    elif pending.get("parent_payload_sha256") is not None:
        raise RuntimeError(
            "continuation pending journal names a missing parent anchor"
        )
    return pending_checkpoint


def _recover_continuation_pending(checkpoint, runtime_root):
    continuation_id = checkpoint_continuation_id(checkpoint)
    pending_path = continuation_pending_path(
        runtime_root, continuation_id, create=False,
    )
    if not os.path.lexists(pending_path):
        return None
    if os.path.islink(pending_path) or not os.path.isfile(pending_path):
        raise RuntimeError("continuation pending journal is not a safe regular file")
    pending = load_json(pending_path)
    uploaded_revision = _checkpoint_revision(checkpoint)
    anchor_path = continuation_checkpoint_path(
        runtime_root, continuation_id, create=False,
    )
    anchored = None
    if os.path.lexists(anchor_path):
        if os.path.islink(anchor_path) or not os.path.isfile(anchor_path):
            raise RuntimeError("continuation checkpoint anchor is not a safe regular file")
        anchored = load_json(anchor_path)
        validate_checkpoint_integrity(anchored)
    pending_checkpoint = _validated_pending_checkpoint(
        pending, continuation_id, run_checkpoint=checkpoint,
        anchored_checkpoint=anchored,
    )
    pending_revision = _checkpoint_revision(pending_checkpoint)

    if anchored is None or anchored.get("payload_sha256") != pending_checkpoint.get("payload_sha256"):
        _persist_continuation_anchor(runtime_root, pending_checkpoint)
    clear_continuation_pending(runtime_root, continuation_id)
    checkpoint.clear()
    checkpoint.update(pending_checkpoint)
    return {
        "state": "pending_journal_recovered",
        "checkpoint_path": anchor_path,
        "uploaded_revision": uploaded_revision,
        "anchor_revision": pending_revision,
    }


def _reconcile_continuation_anchor(checkpoint, runtime_root, persist_absent=True):
    validate_checkpoint_integrity(checkpoint)
    recovered = _recover_continuation_pending(checkpoint, runtime_root)
    if recovered is not None:
        return recovered
    continuation_id = checkpoint_continuation_id(checkpoint)
    anchor_path = continuation_checkpoint_path(runtime_root, continuation_id, create=False)
    if not os.path.lexists(anchor_path):
        if not persist_absent:
            return {"state": "portable_anchor_pending", "checkpoint_path": anchor_path}
        _persist_continuation_anchor(runtime_root, checkpoint)
        return {"state": "portable_anchor_created", "checkpoint_path": anchor_path}
    if os.path.islink(anchor_path) or not os.path.isfile(anchor_path):
        raise RuntimeError("continuation checkpoint anchor is not a safe regular file")
    anchored = load_json(anchor_path)
    validate_checkpoint_integrity(anchored)
    if checkpoint_continuation_id(anchored) != continuation_id:
        raise RuntimeError("continuation checkpoint anchor identity does not match")
    if not _checkpoint_continuation_schema_equal(anchored, checkpoint):
        raise RuntimeError("continuation checkpoint anchor immutable schema does not match")
    uploaded_revision = _checkpoint_revision(checkpoint)
    anchored_revision = _checkpoint_revision(anchored)
    if anchored_revision == uploaded_revision:
        if not _checkpoints_semantically_equal(anchored, checkpoint):
            raise RuntimeError(
                "continuation checkpoint forks at revision %d; refusing ambiguous resume"
                % anchored_revision
            )
        return {
            "state": "same_revision", "checkpoint_path": anchor_path,
            "revision": anchored_revision,
        }
    if anchored_revision > uploaded_revision:
        if not _checkpoint_reconciliation_descendant(
            anchored, checkpoint, portable=True,
        ):
            raise RuntimeError(
                "authoritative continuation checkpoint is not a legal descendant"
            )
        checkpoint.clear()
        checkpoint.update(anchored)
        return {
            "state": "anchor_promoted", "checkpoint_path": anchor_path,
            "uploaded_revision": uploaded_revision, "anchor_revision": anchored_revision,
        }
    if not _checkpoint_reconciliation_descendant(
        checkpoint, anchored, portable=True,
    ):
        raise RuntimeError("loaded checkpoint is not a legal continuation descendant")
    _persist_continuation_anchor(runtime_root, checkpoint)
    return {
        "state": "loaded_checkpoint_promoted", "checkpoint_path": anchor_path,
        "uploaded_revision": uploaded_revision, "anchor_revision": anchored_revision,
    }


def _guard_origin_checkpoint_revision(checkpoint, origin_root, runtime_root):
    checkpoint_path = os.path.join(origin_root, _contract_text("checkpoint.json"))
    if not os.path.lexists(checkpoint_path):
        return {"state": "external_only", "checkpoint_path": checkpoint_path}
    if os.path.islink(checkpoint_path) or not os.path.isfile(checkpoint_path):
        raise RuntimeError("origin checkpoint.json is not a safe regular file")
    origin_checkpoint = load_json(checkpoint_path)
    validate_checkpoint_integrity(origin_checkpoint)
    resolved_origin, unavailable_reason = _checkpoint_origin_path(
        origin_checkpoint, runtime_root,
    )
    if resolved_origin is None or os.path.realpath(resolved_origin) != os.path.realpath(origin_root):
        raise RuntimeError(
            "origin checkpoint does not bind to the selected checkpoint origin: %s"
            % unavailable_reason
        )
    if checkpoint_continuation_id(origin_checkpoint) != checkpoint_continuation_id(checkpoint):
        raise RuntimeError("origin checkpoint continuation does not match the loaded checkpoint")
    if not _checkpoint_immutable_schema_equal(origin_checkpoint, checkpoint):
        raise RuntimeError("origin checkpoint immutable optimization schema does not match")
    external_revision = _checkpoint_revision(checkpoint)
    origin_revision = _checkpoint_revision(origin_checkpoint)
    if origin_revision == external_revision:
        if not _checkpoints_semantically_equal(origin_checkpoint, checkpoint):
            raise RuntimeError(
                "loaded and origin checkpoints fork at the same revision; refusing ambiguous resume"
            )
        return {
            "state": "same_revision", "checkpoint_path": checkpoint_path,
            "revision": origin_revision,
        }
    if origin_revision < external_revision:
        if not _checkpoint_candidate_extension(checkpoint, origin_checkpoint):
            raise RuntimeError(
                "loaded newer checkpoint is not an exact candidate extension of its origin"
            )
        return {
            "state": "external_newer", "checkpoint_path": checkpoint_path,
            "external_revision": external_revision, "origin_revision": origin_revision,
        }
    if _checkpoint_reconciliation_descendant(origin_checkpoint, checkpoint):
        checkpoint.clear()
        checkpoint.update(origin_checkpoint)
        return {
            "state": "origin_reconciliation_promoted", "checkpoint_path": checkpoint_path,
            "external_revision": external_revision, "origin_revision": origin_revision,
        }
    raise RuntimeError(
        "loaded checkpoint revision %d is older than the authoritative origin revision %d; "
        "load %s to continue without rolling back optimizer state"
        % (external_revision, origin_revision, checkpoint_path)
    )


def _candidate_is_lightweight_only(candidate_root):
    allowed = {_contract_text("candidate.json"), _contract_text("timing.json"), _contract_text("completion_evidence.json"), _contract_text("cli_progress.json")}
    names = set(os.listdir(candidate_root))
    retained = set(
        name for name in names
        if re.match(r"^retained_spectre_[A-Za-z0-9_-]+\.(?:log|meta)$", name)
        or re.match(r"^retained_cli_[0-9a-f]{32}\.(?:log|meta)$", name)
    )
    if (
            not names or not names.issubset(allowed | retained)
            or _contract_text("candidate.json") not in names):
        return False
    for name in names:
        path = os.path.join(candidate_root, name)
        if os.path.islink(path) or not os.path.isfile(path) or os.path.getsize(path) <= 0:
            return False
    return True


def _legacy_guardian_protocol(checkpoint):
    spec = checkpoint.get("spec") or {}
    protocol = spec.get("legacy_guardian_protocol")
    if protocol is not None and protocol != LEGACY_GUARDIAN_PROTOCOL:
        raise RuntimeError(
            "checkpoint legacy guardian protocol is not recognized: %r" % protocol
        )
    return protocol


def _legacy_prelaunch_attempt_proof(attempt_root):
    names = set(os.listdir(attempt_root))
    manifest_temporary = set()
    temporary_prefix = LEGACY_GUARDIAN_MANIFEST_NAME + "."
    for name in names:
        if not (name.startswith(temporary_prefix) and name.endswith(".tmp")):
            continue
        nonce = name[len(temporary_prefix):-len(".tmp")]
        if nonce and all(
            character in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
            for character in nonce
        ):
            manifest_temporary.add(name)
    unexpected = names - _LEGACY_PRELAUNCH_ATTEMPT_FILES - manifest_temporary
    if unexpected:
        raise RuntimeError(
            "legacy Maestro attempt lacks its launch manifest and contains post-launch artifacts: %s"
            % sorted(unexpected)
        )
    for name in names:
        path = os.path.join(attempt_root, name)
        if os.path.islink(path) or not os.path.isfile(path):
            raise RuntimeError(
                "legacy Maestro prelaunch artifact is not a regular file: %s" % path
            )
    return {
        "state": "dead",
        "attempt_root": attempt_root,
        "termination_requested": False,
        "termination_proof": {
            "ok": True,
            "kind": "ocean_not_spawned",
            "reason": "guarded protocol writes the launch manifest before guardian Popen",
        },
    }


def _quiesce_legacy_candidate_attempts(origin_root, checkpoint, config,
                                       terminate_owned=False):
    protocol = _legacy_guardian_protocol(checkpoint)
    candidates_root = os.path.join(origin_root, "candidates")
    if not os.path.lexists(candidates_root):
        return []
    if os.path.islink(candidates_root) or not os.path.isdir(candidates_root):
        raise RuntimeError("cannot safely inspect origin candidate root")
    timeout_seconds = _resume_cleanup_timeout(
        config.get("runtime", {}), "resume_worker_cleanup_timeout_seconds", 30.0,
    )
    cleanup_deadline = time.monotonic() + timeout_seconds
    inspections = []
    for candidate_name in sorted(os.listdir(candidates_root)):
        if not _CANDIDATE_NAME.match(candidate_name):
            raise RuntimeError(
                "cannot inspect legacy Maestro processes under unrecognized candidate path: %s"
                % candidate_name
            )
        candidate_root = os.path.join(candidates_root, candidate_name)
        if os.path.islink(candidate_root) or not os.path.isdir(candidate_root):
            raise RuntimeError(
                "cannot inspect legacy Maestro processes under unsafe candidate path: %s"
                % candidate_name
            )
        attempts_root = os.path.join(candidate_root, "attempts")
        if not os.path.lexists(attempts_root):
            continue
        if os.path.islink(attempts_root) or not os.path.isdir(attempts_root):
            raise RuntimeError(
                "legacy Maestro attempts root is not a real directory: %s" % attempts_root
            )
        for attempt_name in sorted(os.listdir(attempts_root)):
            if not _LEGACY_ATTEMPT_NAME.match(attempt_name):
                raise RuntimeError(
                    "unrecognized legacy Maestro attempt path: %s/%s"
                    % (candidate_name, attempt_name)
                )
            attempt_root = os.path.join(attempts_root, attempt_name)
            if os.path.islink(attempt_root) or not os.path.isdir(attempt_root):
                raise RuntimeError(
                    "legacy Maestro attempt is not a real direct directory: %s" % attempt_root
                )
            manifest_path = os.path.join(attempt_root, LEGACY_GUARDIAN_MANIFEST_NAME)
            if not os.path.lexists(manifest_path):
                if protocol != LEGACY_GUARDIAN_PROTOCOL:
                    raise RuntimeError(
                        "legacy Maestro attempt has no durable guardian launch proof; "
                        "this older checkpoint cannot be resumed safely: %s" % attempt_root
                    )
                inspection = _legacy_prelaunch_attempt_proof(attempt_root)
            else:
                inspection = quiesce_legacy_guardian_attempt(
                    attempt_root,
                    expected_workspace_root=os.path.join(candidate_root, "workspace"),
                    timeout_seconds=max(0.0, cleanup_deadline - time.monotonic()),
                    terminate_owned=bool(terminate_owned),
                )
            inspection = dict(inspection)
            inspection.update({
                "candidate_id": candidate_name,
                "attempt": attempt_name,
                "attempt_root": attempt_root,
            })
            if (
                inspection.get("state") != "dead"
                or (inspection.get("termination_proof") or {}).get("ok") is not True
            ):
                raise RuntimeError(
                    "cannot resume checkpoint because legacy Maestro/Ocean cleanup is not proven: %s"
                    % _guardian_cleanup_failure_summary(inspection)
                )
            inspections.append(inspection)
    return inspections


def _guardian_identity_matches(snapshot, guardian_path, state_path, token, workspace_root):
    if snapshot.get("identity_error"):
        return False
    command = snapshot.get("command") or []
    script_matches = any(
        os.path.realpath(item) == os.path.realpath(guardian_path) for item in command
        if item and not item.startswith("-")
    )
    return (
        script_matches and
        os.path.realpath(_command_option(command, "--state-path") or "") == os.path.realpath(state_path) and
        _command_option(command, "--token") == token and
        os.path.realpath(_command_option(command, "--cwd") or "") == os.path.realpath(workspace_root) and
        snapshot.get("cwd") == os.path.realpath(workspace_root) and
        snapshot.get("pgid") == snapshot.get("pid")
    )


def _ocean_identity_matches(snapshot, command, workspace_root, expected_pgid):
    if snapshot.get("identity_error") or not command:
        return False
    observed = snapshot.get("command") or []
    if not observed:
        return False
    executable_matches = os.path.realpath(observed[0]) == os.path.realpath(command[0])
    return (
        executable_matches and observed[1:] == list(command[1:]) and
        snapshot.get("cwd") == os.path.realpath(workspace_root) and
        snapshot.get("pgid") == expected_pgid and expected_pgid == snapshot.get("pid")
    )


def _worker_termination_proof(worker_state, guardian, current_boot_id=None):
    worker_boot_id = _launch_boot_id(worker_state)
    guardian_boot_id = _launch_boot_id(guardian)
    if worker_boot_id and guardian_boot_id and worker_boot_id != guardian_boot_id:
        return {
            "ok": False, "kind": "launch_boot_id_mismatch",
            "worker_launch_boot_id": worker_boot_id,
            "guardian_launch_boot_id": guardian_boot_id,
        }
    launch_boot_id = guardian_boot_id or worker_boot_id
    if launch_boot_id and current_boot_id and launch_boot_id != current_boot_id:
        return {
            "ok": True, "kind": "prior_boot_processes_dead",
            "launch_boot_id": launch_boot_id, "current_boot_id": current_boot_id,
            "status": (guardian or worker_state or {}).get("status") or (worker_state or {}).get("state"),
        }
    if guardian:
        status = guardian.get("status")
        if status == "exited":
            remaining_group = list(guardian.get("remaining_group_pids") or [])
            remaining_adopted = list(guardian.get("remaining_adopted_pids") or [])
            proven = bool(
                guardian.get("group_cleanup_complete") is True
                and guardian.get("descendant_cleanup_complete") is True
                and guardian.get("descendant_supervision_complete") is True
                and not remaining_group
                and not remaining_adopted
            )
            return {
                "ok": proven,
                "kind": "guardian_cleanup_proven" if proven else "guardian_cleanup_unproven",
                "status": status,
                "remaining_group_pids": remaining_group,
                "remaining_adopted_pids": remaining_adopted,
            }
        if status == "launch_failed" and guardian.get("pid") is None and guardian.get("pgid") is None:
            return {"ok": True, "kind": "ocean_not_spawned", "status": status}
        return {"ok": False, "kind": "guardian_not_cleanly_terminated", "status": status}
    if worker_state is None:
        return {"ok": True, "kind": "worker_not_created", "status": None}
    if worker_state.get("pid") is None and worker_state.get("ocean_pid") is None:
        state = worker_state.get("state")
        if state == "new":
            return {"ok": True, "kind": "ocean_not_spawned", "status": state}
        return {
            "ok": False,
            "kind": "guardian_launch_record_missing",
            "status": state,
        }
    return {
        "ok": False,
        "kind": "guardian_cleanup_record_missing",
        "status": worker_state.get("state"),
    }


def _inspect_worker_epoch_processes(epoch_root, worker_index, epoch_index):
    epoch_root = os.path.abspath(epoch_root)
    workspace_root = os.path.join(epoch_root, "workspace")
    worker_state_path = os.path.join(epoch_root, _contract_text("worker_state.json"))
    namespace = parse_worker_epoch_namespace(
        os.path.basename(os.path.dirname(epoch_root)), os.path.basename(epoch_root),
    )
    if namespace is None:
        raise RuntimeError("worker epoch path is outside the strict namespace")
    if (
        namespace["worker_index"] != int(worker_index)
        or namespace["local_epoch"] != int(epoch_index)
    ):
        raise RuntimeError("worker epoch arguments do not match its directory namespace")
    expected_epoch = namespace["epoch_identity"]
    expected_control = persistent_worker_control_root(epoch_root, expected_epoch)
    expected_manifest_path = os.path.join(expected_control, LEGACY_GUARDIAN_MANIFEST_NAME)
    expected_launch_lock_path = os.path.join(
        expected_control, LEGACY_GUARDIAN_LAUNCH_LOCK_NAME,
    )
    worker_state = None
    if os.path.lexists(worker_state_path):
        if os.path.islink(worker_state_path) or not os.path.isfile(worker_state_path):
            raise RuntimeError("worker state is not a regular file: %s" % worker_state_path)
        worker_state = load_json(worker_state_path)
        if worker_state.get("schema_version") != _contract_text("crescendo.persistent_maestro_worker.v1"):
            raise RuntimeError("worker state schema is not recognized")
        if int(worker_state.get("worker_index", -1)) != int(worker_index):
            raise RuntimeError("worker state index does not match its directory")
        if worker_state.get("epoch") != expected_epoch:
            raise RuntimeError("worker epoch identity does not match its directory")
        if os.path.realpath(worker_state.get("workspace_root") or "") != os.path.realpath(workspace_root):
            raise RuntimeError("worker state workspace does not match its epoch")
        if os.path.realpath(worker_state.get("control_root") or "") != os.path.realpath(expected_control):
            raise RuntimeError("worker state control root does not match its epoch")
    try:
        guardian_token_identity = resolve_worker_guardian_token_identity(
            worker_state, expected_epoch, int(worker_index),
        )
    except ValueError as error:
        raise RuntimeError("worker guardian token identity is invalid: %s" % error)

    durable_values = {
        "protocol": (worker_state or {}).get("guardian_launch_protocol"),
        "manifest_path": (worker_state or {}).get("guardian_launch_manifest_path"),
        "lock_path": (worker_state or {}).get("guardian_launch_lock_path"),
    }
    durable_recorded = any(value is not None for value in durable_values.values())
    if durable_recorded:
        if any(value is None for value in durable_values.values()):
            raise RuntimeError("worker guardian durable launch identity is incomplete")
        if durable_values["protocol"] != LEGACY_GUARDIAN_PROTOCOL:
            raise RuntimeError("worker guardian durable launch protocol is not recognized")
        if not _controller_path_matches(durable_values["manifest_path"], expected_manifest_path):
            raise RuntimeError("worker guardian launch manifest path does not match its epoch")
        if not _controller_path_matches(durable_values["lock_path"], expected_launch_lock_path):
            raise RuntimeError("worker guardian launch lock path does not match its epoch")

    epochs_root = os.path.join(epoch_root, "epochs")
    expected_epoch_root = os.path.dirname(expected_control)
    expected_epoch_name = os.path.basename(expected_epoch_root)
    if os.path.lexists(epochs_root):
        if os.path.islink(epochs_root) or not os.path.isdir(epochs_root):
            raise RuntimeError("worker epochs root is not a regular directory")
        unexpected = sorted(
            name for name in os.listdir(epochs_root) if name != expected_epoch_name
        )
        if unexpected:
            raise RuntimeError("worker epochs root contains an unexpected child: %s" % unexpected[0])
    if os.path.lexists(expected_epoch_root) and (
            os.path.islink(expected_epoch_root) or not os.path.isdir(expected_epoch_root)):
        raise RuntimeError("worker hashed epoch root is not a regular directory")
    if os.path.lexists(expected_control) and (
            os.path.islink(expected_control) or not os.path.isdir(expected_control)):
        raise RuntimeError("worker control root is not a regular directory")

    state_path = (worker_state or {}).get("guardian_state_path")
    if state_path:
        state_path = os.path.abspath(state_path)
        expected_state_path = os.path.join(expected_control, _contract_text("guardian_state.json"))
        if os.path.realpath(state_path) != os.path.realpath(expected_state_path) or not is_under(state_path, epoch_root):
            raise RuntimeError("guardian state path does not match its worker epoch")
    else:
        candidate = os.path.join(expected_control, _contract_text("guardian_state.json"))
        state_path = candidate if os.path.lexists(candidate) else None

    guardian = None
    if state_path and os.path.lexists(state_path):
        if os.path.islink(state_path) or not os.path.isfile(state_path):
            raise RuntimeError("guardian state is not a regular file: %s" % state_path)
        guardian = load_json(state_path)
        if guardian.get("schema_version") != _contract_text("crescendo.ocean_worker_guardian.v1"):
            raise RuntimeError("guardian state schema is not recognized")
        if os.path.realpath(guardian.get("cwd") or "") != os.path.realpath(workspace_root):
            raise RuntimeError("guardian cwd does not match its worker workspace")

    guardian_token = guardian_token_identity["token"]
    if guardian and guardian.get("token") != guardian_token:
        raise RuntimeError("guardian token does not match its worker epoch")
    if worker_state and guardian:
        for worker_key, guardian_key in (("pid", "guardian_pid"), ("ocean_pid", "pid"), ("ocean_pgid", "pgid")):
            worker_value = worker_state.get(worker_key)
            guardian_value = guardian.get(guardian_key)
            if worker_value is not None and guardian_value is not None and int(worker_value) != int(guardian_value):
                raise RuntimeError("worker and guardian PID identity records disagree")
        worker_boot_id = _launch_boot_id(worker_state)
        guardian_boot_id = _launch_boot_id(guardian)
        if worker_boot_id and guardian_boot_id and worker_boot_id != guardian_boot_id:
            raise RuntimeError("worker and guardian launch_boot_id records disagree")

    manifest_exists = os.path.lexists(expected_manifest_path)
    launch_lock_exists = os.path.lexists(expected_launch_lock_path)
    if durable_recorded or manifest_exists or launch_lock_exists:
        if manifest_exists:
            durable_inspection = inspect_legacy_guardian_attempt(
                expected_control, expected_workspace_root=workspace_root,
                expected_token_identity=(
                    guardian_token_identity if worker_state is not None else None
                ),
            )
            state = durable_inspection.get("state")
            mapped_state = {
                "active": "active", "dead": "abandoned", "ambiguous": "unsafe",
            }.get(state, "unsafe")
            return {
                "state": mapped_state,
                "worker_index": int(worker_index), "epoch_index": int(epoch_index),
                "controller_epoch": namespace["controller_epoch"],
                "processes": durable_inspection.get("processes") or [],
                "guardian_status": (guardian or {}).get("status"),
                "launch_handshake": bool(durable_inspection.get("launch_handshake")),
                "termination_proof": durable_inspection.get("termination_proof") or {
                    "ok": False, "kind": "durable_guardian_inspection_failed",
                },
                "inspection_reason": durable_inspection.get("reason"),
            }
        lock_status = _worker_guardian_launch_lock_status(expected_launch_lock_path)
        if lock_status.get("state") == "held":
            return {
                "state": "active",
                "worker_index": int(worker_index), "epoch_index": int(epoch_index),
                "controller_epoch": namespace["controller_epoch"],
                "processes": [{"kind": "launch_handshake"}],
                "guardian_status": (guardian or {}).get("status"),
                "launch_handshake": True,
                "termination_proof": {"ok": False, "kind": "launch_in_progress"},
            }
        pristine = bool(
            guardian is None and
            (worker_state is None or (
                worker_state.get("state") == "new"
                and worker_state.get("pid") is None
                and worker_state.get("ocean_pid") is None
            ))
        )
        proof = {
            "ok": pristine,
            "kind": "worker_not_created" if worker_state is None else (
                "ocean_not_spawned" if pristine else "guardian_launch_manifest_missing"
            ),
            "status": (worker_state or {}).get("state"),
        }
        return {
            "state": "abandoned" if pristine else "unsafe",
            "worker_index": int(worker_index), "epoch_index": int(epoch_index),
            "controller_epoch": namespace["controller_epoch"],
            "processes": [], "guardian_status": (guardian or {}).get("status"),
            "termination_proof": proof,
        }

    terminal_guardian = bool(guardian and guardian.get("status") in _GUARDIAN_TERMINAL_STATES)
    guardian_pid = (guardian or {}).get("guardian_pid")
    if guardian_pid is None:
        guardian_pid = (worker_state or {}).get("pid")
    ocean_pid = (guardian or {}).get("pid")
    if ocean_pid is None:
        ocean_pid = (worker_state or {}).get("ocean_pid")
    active = []
    ambiguous = []
    guardian_script = guardian_script_path()
    current_boot_id = _current_boot_id()
    launch_boot_id = _launch_boot_id(guardian) or _launch_boot_id(worker_state)
    prior_boot = bool(launch_boot_id and current_boot_id and launch_boot_id != current_boot_id)
    if not prior_boot:
        guardian_snapshot = _process_snapshot(guardian_pid)
        if guardian_snapshot.get("alive"):
            matches = bool(state_path) and _guardian_identity_matches(
                guardian_snapshot, guardian_script, state_path, guardian_token, workspace_root,
            )
            if matches:
                active.append({"kind": "guardian", "pid": guardian_snapshot["pid"]})
            elif not terminal_guardian:
                ambiguous.append({"kind": "guardian", "pid": guardian_snapshot.get("pid")})
        ocean_snapshot = _process_snapshot(ocean_pid)
        if ocean_snapshot.get("alive"):
            command = (guardian or {}).get("argv") or []
            pgid = (guardian or {}).get("pgid")
            matches = _ocean_identity_matches(ocean_snapshot, command, workspace_root, pgid)
            if matches:
                active.append({"kind": "ocean", "pid": ocean_snapshot["pid"], "pgid": pgid})
            elif not terminal_guardian:
                ambiguous.append({"kind": "ocean", "pid": ocean_snapshot.get("pid")})
    if ambiguous:
        raise RuntimeError("live PID records cannot be proven to belong to this worker: %s" % ambiguous)
    termination_proof = _worker_termination_proof(worker_state, guardian, current_boot_id=current_boot_id)
    return {
        "state": "active" if active else ("abandoned" if termination_proof.get("ok") else "unsafe"),
        "worker_index": int(worker_index), "epoch_index": int(epoch_index),
        "controller_epoch": namespace["controller_epoch"],
        "processes": active, "guardian_status": (guardian or {}).get("status"),
        "termination_proof": termination_proof,
    }


def _quiesce_worker_epoch_processes(epoch_root, worker_index, epoch_index,
                                    timeout_seconds):
    epoch_root = os.path.abspath(epoch_root)
    namespace = parse_worker_epoch_namespace(
        os.path.basename(os.path.dirname(epoch_root)), os.path.basename(epoch_root),
    )
    if namespace is None or (
            namespace["worker_index"] != int(worker_index)
            or namespace["local_epoch"] != int(epoch_index)):
        raise RuntimeError("worker epoch path is outside the strict namespace")
    initial = _inspect_worker_epoch_processes(
        epoch_root, worker_index, epoch_index,
    )
    if (
            initial.get("state") == "abandoned"
            and (initial.get("termination_proof") or {}).get("ok") is True):
        return initial
    worker_state_path = os.path.join(epoch_root, _contract_text("worker_state.json"))
    if os.path.islink(worker_state_path) or not os.path.isfile(worker_state_path):
        raise RuntimeError("worker state is unavailable for scoped guardian cleanup")
    worker_state = load_json(worker_state_path)
    expected_epoch = namespace["epoch_identity"]
    workspace_root = os.path.join(epoch_root, "workspace")
    control_root = persistent_worker_control_root(epoch_root, expected_epoch)
    if (
            worker_state.get("schema_version") != _contract_text("crescendo.persistent_maestro_worker.v1")
            or int(worker_state.get("worker_index", -1)) != int(worker_index)
            or worker_state.get("epoch") != expected_epoch
            or not _controller_path_matches(
                worker_state.get("workspace_root"), workspace_root,
            )
            or not _controller_path_matches(
                worker_state.get("control_root"), control_root,
            )):
        raise RuntimeError("worker state changed before scoped guardian cleanup")
    try:
        token_identity = resolve_worker_guardian_token_identity(
            worker_state, expected_epoch, int(worker_index),
        )
    except ValueError as error:
        raise RuntimeError(
            "worker guardian token identity changed before cleanup: %s" % error
        )
    recovery = quiesce_legacy_guardian_attempt(
        control_root, expected_workspace_root=workspace_root,
        timeout_seconds=max(0.0, float(timeout_seconds)),
        terminate_owned=True, expected_token_identity=token_identity,
    )
    proof = recovery.get("termination_proof") or {}
    if recovery.get("state") != "dead" or proof.get("ok") is not True:
        raise RuntimeError(
            "scoped guardian cleanup was not proven: %s"
            % _guardian_cleanup_failure_summary(recovery)
        )
    inspected = _inspect_worker_epoch_processes(
        epoch_root, worker_index, epoch_index,
    )
    if (
            inspected.get("state") != "abandoned"
            or (inspected.get("termination_proof") or {}).get("ok") is not True):
        raise RuntimeError(
            "scoped guardian cleanup proof was not stable after recovery"
        )
    inspected["termination_requested"] = bool(
        recovery.get("termination_requested")
    )
    inspected["recovery_proof"] = proof
    return inspected


def _desktop_session():
    for name in os.listdir("/proc"):
        if not name.isdigit():
            continue
        root = os.path.join("/proc", name)
        try:
            with open(os.path.join(root, "comm"), "r") as stream:
                if stream.read().strip() != "gnome-shell":
                    continue
            with open(os.path.join(root, "status"), "r") as stream:
                uid_line = next(line for line in stream if line.startswith("Uid:"))
            uid = int(uid_line.split()[1])
            with open(os.path.join(root, "environ"), "rb") as stream:
                values = stream.read().decode("utf-8", "ignore").split("\0")
            environment = dict(value.split("=", 1) for value in values if "=" in value)
            if environment.get("DISPLAY"):
                return pwd.getpwuid(uid).pw_name, dict(
                    (key, environment[key]) for key in ("DISPLAY", "XAUTHORITY", "DBUS_SESSION_BUS_ADDRESS") if environment.get(key)
                )
        except (IOError, OSError, StopIteration, ValueError, KeyError):
            continue
    raise RuntimeError("active GNOME desktop session not found")


class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True


from .private_suite_service import PrivateSuiteServiceMixin


class OptimizerService(PrivateSuiteServiceMixin):
    def __init__(self, config):
        self.config = config
        # A manual saved-setup choice must not discard the menu caller or
        # recapture Python/Tk's loader state when switching back to that caller.
        self.invoking_config = copy.deepcopy(config)
        self.invoking_context = copy.deepcopy(config.get("_maestro_launch_context"))
        self.prepared = {}
        self.inspections = {}
        self.test_inspections = {}
        # Setup owns these suites until this UI closes. Runs borrow them; a
        # completed diagnostic/optimization never owns their view directories.
        self.setup_private_sessions = {}
        self.inspection_epoch = 0
        self.threads = {}
        self.optimization_launches = 0
        self.optimized_view_export_active = False
        self.optimized_view_cleanup_lock = threading.Lock()
        self.optimized_view_cleanup_pending = None
        self.optimized_view_cleanup_lease = None
        self.environment_tests = None
        self.range_validations = {}
        self.range_validation_cancels = {}
        self.range_validation_threads = {}
        self.range_validation_leases = {}
        self.range_validation_cleanup_sessions = {}
        self.resident_validation = None
        self.shutdown_callback = None
        self.lock = threading.Lock()
        self._activity_revision = 0
        self._activity_events = []
        self.exit_lock = threading.Lock()
        self.lifecycle_condition = threading.Condition(self.lock)
        self.lifecycle_operations = 0
        self.prepare_cancel_event = threading.Event()
        self.prepare_cleanup_failures = []
        self.closing = False
        self.eda_reconfiguring = False
        self.session_run_names = set()
        self._objective_presentation_lock = threading.Lock()
        self._objective_presentation_cache = {}
        self._run_data_reader_lock = threading.Lock()
        self._run_data_readers = {}
        self._run_locations = {}
        self._run_scopes = {}
        self._recent_checkpoint_cache = {}
        self._run_index_lock = threading.Lock()
        self._scratch_receipts = {}
        self._remember_scratch(config)
        self._load_run_index()

    def _set_activity(self, stage, message, state="running", level=None):
        """Small in-memory UI timeline; diagnostic evidence remains separate."""
        with self.lock:
            self._activity_revision += 1
            row = {"revision": self._activity_revision, "stage": str(stage)[:96],
                   "message": str(message)[:1200], "state": state, "time": time.time(),
                   "level": level or ("ERROR" if state == "error" else "INFO")}
            self._activity_events.append(row)
            self._activity_events = self._activity_events[-100:]

    def activity_snapshot(self):
        with self.lock:
            result = dict(self._activity_events[-1]) if self._activity_events else {
                "revision": 0, "stage": "idle", "message": "", "state": "complete"}
            result["events"] = copy.deepcopy(self._activity_events)
            return result

    @staticmethod
    def _require_single_test(prepared):
        setup = (prepared or {}).get("setup") or {}
        rows = setup.get("tests") or []
        name = setup.get("selected_test")
        if len(rows) != 1 or not name or rows[0].get("name") != name or not setup.get("_source_tests"):
            raise ValueError("Prepared setup lacks a qualified single Test; Load and Inspect again.")
        return name

    def start_environment_test(self, prepared_id, spec=None):
        from .environment_test import EnvironmentTestManager
        with self.lock:
            if prepared_id:
                self._require_single_test(self.prepared.get(prepared_id))
            if self.closing:
                raise RuntimeError("optimizer is closing")
            if self.environment_tests is None:
                self.environment_tests = EnvironmentTestManager(self)
            manager = self.environment_tests
        return manager.start(prepared_id, spec)

    def environment_test_progress(self, test_id):
        manager = getattr(self, "environment_tests", None)
        if manager is None:
            raise ValueError("environment test not found")
        return manager.progress(test_id)

    def stop_environment_test(self, test_id):
        manager = getattr(self, "environment_tests", None)
        if manager is None:
            raise ValueError("environment test not found")
        return manager.stop(test_id)

    def _remember_scratch(self, config):
        config = config or {}
        context = config.get("_maestro_launch_context") or {}
        runtime = config.get("runtime") or {}
        receipts = [context.get("scratch_ownership"), runtime.get("probe_scratch_ownership")]
        extra = runtime.get("scratch_ownership") or []
        receipts.extend(extra if isinstance(extra, list) else [extra])
        for receipt in receipts:
            if isinstance(receipt, dict) and isinstance(receipt.get("path"), str) and isinstance(receipt.get("token"), str):
                self._scratch_receipts[(receipt["path"], receipt["token"])] = dict(receipt)

    def _cleanup_session_scratch(self):
        from .scratch import cleanup_scratch
        self._remember_scratch(self.config)
        outcomes = []
        for receipt in list(self._scratch_receipts.values()):
            try:
                value = cleanup_scratch(receipt["path"], token=receipt["token"], producers_stopped=True)
            except (IOError, OSError, RuntimeError, ValueError) as error:
                value = {"ok": False, "state": "retained", "path": receipt["path"], "error": str(error)}
            outcomes.append(value)
        return {"ok": all(value.get("ok") for value in outcomes), "directories": outcomes}

    def _run_index_path(self, config=None):
        config = config or self.config
        runtime = config.get("runtime") or {}
        root = runtime.get("design_root")
        return os.path.join(root, _contract_text("index.json")) if root else None

    def _load_run_index(self):
        path = self._run_index_path()
        if not path:
            return
        try:
            info = os.lstat(path)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > 8 * 1024 * 1024:
                return
            document = load_json(path)
            if not isinstance(document, dict) or document.get("schema_version") != _contract_text("crescendo.tb_index.v1"):
                return
            root_identity = os.path.realpath(self.config["runtime"]["design_root"])
            if document.get("design_root") != root_identity:
                return
            if not isinstance(document.get("runs"), list):
                return
            for row in document["runs"][:100000]:
                if not isinstance(row, dict):
                    continue
                name, runtime = row.get("run_id"), row.get("runtime_root")
                if not isinstance(runtime, str) or not os.path.isabs(runtime) or os.path.realpath(runtime) != runtime:
                    continue
                self._safe_run_name(name)
                self._run_locations[name] = runtime
                self._run_scopes[name] = root_identity
        except (IOError, OSError, TypeError, ValueError, KeyError):
            return  # Corrupt/missing browsing index never gates a new run.

    def _register_run_location(self, run_name, runtime_root, config=None):
        self._safe_run_name(run_name)
        runtime_root = os.path.realpath(os.path.abspath(runtime_root))
        with self.lock:
            old = self._run_locations.get(run_name)
            if old and old != runtime_root:
                raise RuntimeError("run id already belongs to a different data directory")
            self._run_locations[run_name] = runtime_root
            scope = ((config or self.config).get("runtime") or {}).get("design_root")
            self._run_scopes[run_name] = os.path.realpath(scope) if scope else None
        path = self._run_index_path(config)
        if not path:
            return None
        descriptor = None
        with self._run_index_lock:
            try:
                directory = os.path.realpath(os.path.dirname(path))
                # Multiple UIs may create this optional metadata directory first.
                # exist_ok handles only an existing directory; quota/permission
                # failures still produce the precise nonblocking index warning.
                os.makedirs(directory, exist_ok=True)
                lock_path = os.path.join(directory, ".index.lock")
                descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0), 0o600)
                lock_info = os.fstat(descriptor)
                if not stat.S_ISREG(lock_info.st_mode) or lock_info.st_nlink != 1:
                    raise ValueError("TB index lock must be a singly linked regular file")
                deadline = time.monotonic() + 0.15
                while True:
                    try:
                        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        break
                    except OSError as error:
                        if error.errno not in (errno.EAGAIN, errno.EACCES) or time.monotonic() >= deadline:
                            raise
                        time.sleep(0.005)
                merged = {}
                if os.path.lexists(path):
                    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0))
                    try:
                        previous = self._read_json_snapshot_descriptor(fd, "TB run index", 8 * 1024 * 1024)
                    finally:
                        os.close(fd)
                    if not isinstance(previous, dict) or previous.get("schema_version") != _contract_text("crescendo.tb_index.v1") or previous.get("design_root") != directory:
                        raise ValueError("TB run index identity is invalid")
                    if not isinstance(previous.get("runs"), list):
                        raise ValueError("TB run index rows are invalid")
                    for row in previous["runs"][:100000]:
                        if not isinstance(row, dict):
                            raise ValueError("TB run index row is invalid")
                        name = self._safe_run_name(row.get("run_id"))
                        root = row.get("runtime_root")
                        if not isinstance(root, str) or not os.path.isabs(root) or os.path.realpath(root) != root:
                            raise ValueError("TB run index contains an invalid data root")
                        if name in merged and merged[name] != root:
                            raise RuntimeError("TB run index contains conflicting run roots")
                        merged[name] = root
                with self.lock:
                    own = {name: root for name, root in self._run_locations.items()
                           if self._run_scopes.get(name) == directory}
                for name, root in own.items():
                    if name in merged and merged[name] != root:
                        raise RuntimeError("run id already belongs to a different persisted data directory")
                    merged[name] = root
                rows = [{"run_id": name, "runtime_root": root} for name, root in
                        sorted(merged.items(), key=lambda row: self._run_recency_key(row[0]), reverse=True)[:100000]]
                atomic_write_json(path, {"schema_version": _contract_text("crescendo.tb_index.v1"), "design_root": directory, "runs": rows})
                with self.lock:
                    for row in rows:
                        self._run_locations[row["run_id"]] = row["runtime_root"]
                        self._run_scopes[row["run_id"]] = directory
            except (IOError, OSError, ValueError) as error:
                return "Run index was not saved: %s" % error
            finally:
                if descriptor is not None:
                    try:
                        fcntl.flock(descriptor, fcntl.LOCK_UN)
                    finally:
                        os.close(descriptor)
        return None

    def _runtime_for_run(self, run_name):
        self._safe_run_name(run_name)
        with self.lock:
            root = self._run_locations.get(run_name)
        return root or self.config["runtime"]["run_root"]

    def _config_for_run(self, run_name):
        config = copy.deepcopy(self.config)
        config["runtime"]["run_root"] = self._runtime_for_run(run_name)
        scope = self._run_scopes.get(run_name)
        if scope:
            config["runtime"]["design_root"] = scope
        return config

    def _freeze_run_config(self, spec, prepared, checkpoint=None):
        config = copy.deepcopy(self.config)
        from .simulators import selected_simulator
        config.setdefault("cadence", {})["simulator"] = selected_simulator(
            config, (checkpoint.get("spec") or spec) if checkpoint is not None else spec)
        if checkpoint is not None:
            origin = (checkpoint.get("manifest") or {}).get("origin_run_id")
            if origin in self._run_locations:
                config["runtime"]["run_root"] = self._runtime_for_run(origin)
                if self._run_scopes.get(origin):
                    config["runtime"]["design_root"] = self._run_scopes[origin]
            if str((checkpoint.get("spec") or spec).get("evaluator", "real")).lower() != "mock":
                from .storage_policy import validate_native_data_path
                validate_native_data_path(config["runtime"]["run_root"])
            return config
        if "data_root_override" in spec:
            config.setdefault("runtime", {})["data_root_override"] = spec["data_root_override"]
        setup = (prepared or {}).get("setup") or {}
        if setup:
            from .simulator_profiles import apply_source_profile_defaults
            config = apply_source_profile_defaults(config, setup=setup)
        if ("data_root_override" in spec or setup.get("save_options") or
                (config.get("runtime") or {}).get("run_root_origin") == "derived"):
            from .storage_policy import require_data_root, resolve_storage_policy
            policy = resolve_storage_policy(config, setup)
            if str(spec.get("evaluator", "real")).lower() != "mock":
                require_data_root(policy)
            config["runtime"].update(run_root=policy["run_root"], storage_policy=policy)
        if spec.get("environment_test"):
            manager = getattr(self, "environment_tests", None)
            if manager is None:
                raise RuntimeError("diagnostic run requires an active environment-test owner")
            config["runtime"]["run_root"] = manager.run_root(
                spec["environment_test"], spec.get("execution_backend"))
        return config

    @staticmethod
    def _acquire_tb_lease(config):
        runtime = config.get("runtime") or {}
        design = runtime.get("design_root")
        if not design or os.path.realpath(design) == os.path.realpath(runtime["run_root"]):
            return None
        # A data-root override cannot permit two UIs to mutate this TB's private
        # OA views concurrently. One tiny work-directory lease spans all roots.
        return RuntimeOptimizationLease(design).acquire()

    def _begin_lifecycle_operation(self, action, exclusive=False):
        with self.lifecycle_condition:
            if self.closing:
                raise RuntimeError("optimizer is closing; cannot %s" % action)
            if self.optimized_view_export_active and action in ("start an optimization", "resume an optimization"):
                raise RuntimeError("Schematic export is active; wait for its result before starting optimization")
            if exclusive:
                if self.eda_reconfiguring or self.lifecycle_operations:
                    raise RuntimeError(
                        "optimizer resource operations are active; cannot %s" % action
                    )
                self.eda_reconfiguring = True
            elif self.eda_reconfiguring:
                raise RuntimeError(
                    "EDA settings update is in progress; cannot %s" % action
                )
            if self.lifecycle_operations == 0:
                # A completed Setup Stop is cancellation of that operation,
                # not a permanent prohibition on the next Load/Apply.
                self.prepare_cancel_event.clear()
            self.lifecycle_operations += 1

    def stop_setup_preparation(self):
        """Signal owned Setup probes/binding processes; never destroy the suite."""
        with self.lifecycle_condition:
            active = bool(self.lifecycle_operations and self.eda_reconfiguring)
            if active:
                self.prepare_cancel_event.set()
        return {"stop_requested": active, "scope": "setup_preparation",
                "views_retained_until_ui_close": True}

    def _end_lifecycle_operation(self, exclusive=False):
        with self.lifecycle_condition:
            if self.lifecycle_operations <= 0:
                raise RuntimeError("optimizer lifecycle operation counter underflow")
            self.lifecycle_operations -= 1
            if exclusive:
                self.eda_reconfiguring = False
            self.lifecycle_condition.notify_all()

    def _retained_log_cleanup_targets(self, include_inactive=False,
                                      session_only=False):
        try:
            runtime_root, root_descriptor = self._open_runtime_directory()
        except (IOError, OSError, RuntimeError, TypeError, ValueError):
            root_descriptor = None
        try:
            names = sorted(set(self._run_locations) | (set(os.listdir(root_descriptor)) if root_descriptor is not None else set()))
        finally:
            if root_descriptor is not None:
                os.close(root_descriptor)
        with self.lock:
            session_runs = set(self.session_run_names)
            active_runs = set()
            for name, thread in self.threads.items():
                try:
                    alive = bool(thread and thread.is_alive())
                except (RuntimeError, StopIteration):
                    alive = True
                if alive:
                    active_runs.add(name)
        targets = []
        for name in names:
            if session_only and name not in session_runs:
                continue
            if name in active_runs:
                continue
            try:
                safe_name = self._safe_run_name(name)
                status = self._read_run_json_snapshot(
                    safe_name, _contract_text("status.json"), "retained-log run status",
                    _RUN_STATUS_SNAPSHOT_MAX_BYTES, missing_ok=True,
                )
            except (IOError, OSError, RuntimeError, TypeError, ValueError):
                continue
            if not isinstance(status, dict):
                continue
            if not (
                    include_inactive or name in session_runs
                    or status.get("state") in _TERMINAL_RUN_STATES):
                continue
            targets.append(self.run_path(safe_name, ""))
        return targets

    def _clear_retained_logs(self, include_inactive=False, session_only=False):
        results = []
        failures = []
        for run_root in self._retained_log_cleanup_targets(
                include_inactive=include_inactive, session_only=session_only):
            try:
                results.append(clear_retained_spectre_logs(run_root))
            except Exception as error:
                failures.append("%s: %s" % (os.path.basename(run_root), error))
        if failures:
            raise RuntimeError(
                "retained Spectre logs could not be safely cleared: %s"
                % "; ".join(failures)
            )
        return {
            "ok": True,
            "run_count": len(results),
            "removed_count": sum(row.get("removed_count", 0) for row in results),
        }

    @staticmethod
    def _objective_presentation_revision(objectives, candidates):
        digest = hashlib.sha256()

        def update(value):
            encoded = json.dumps(
                value, ensure_ascii=True, sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
            digest.update(str(len(encoded)).encode("ascii"))
            digest.update(b":")
            digest.update(encoded)

        update(objectives)
        for candidate in candidates:
            update({
                "candidate_id": candidate.get("candidate_id"),
                "generation": candidate.get("generation"),
                "index": candidate.get("index"),
                "status": candidate.get("status"),
                "metrics": candidate.get("metrics"),
            })
        return digest.hexdigest()

    def _invalidate_objective_presentation(self, run_name=None):
        with self._objective_presentation_lock:
            if run_name is None:
                self._objective_presentation_cache.clear()
            else:
                self._objective_presentation_cache.pop(run_name, None)

    def _prune_objective_presentations(self, cacheable_runs):
        cacheable_runs = set(cacheable_runs)
        with self._objective_presentation_lock:
            for run_name in list(self._objective_presentation_cache):
                if run_name not in cacheable_runs:
                    self._objective_presentation_cache.pop(run_name, None)

    def _cached_objective_presentation(self, run_name, candidates, objectives):
        revision = self._objective_presentation_revision(objectives, candidates)
        with self._objective_presentation_lock:
            cached = self._objective_presentation_cache.get(run_name)
            if cached and cached["revision"] == revision:
                return copy.deepcopy(cached["presentation"])
            presentation = build_objective_presentation(candidates, objectives)
            try:
                latest_status = self._read_run_json(run_name, _contract_text("status.json"))
            except (IOError, OSError, RuntimeError, TypeError, ValueError):
                latest_status = None
            if (
                    isinstance(latest_status, dict) and
                    latest_status.get("state") not in _TERMINAL_RUN_STATES):
                self._objective_presentation_cache[run_name] = {
                    "revision": revision,
                    "presentation": presentation,
                }
            else:
                self._objective_presentation_cache.pop(run_name, None)
            return copy.deepcopy(presentation)

    def _owned_resident_cleanup_identity(self, session):
        """Bind cleanup only to this service's exact optional private session."""
        try:
            if not isinstance(session, ResidentValidationSession):
                return None
            runtime = os.path.realpath(os.path.abspath(self.config["runtime"]["run_root"]))
            session_runtime = os.path.realpath(os.path.abspath(session.config["runtime"]["run_root"]))
            expected = os.path.join(runtime, "range_validation_resident", "prepared_%s" % session.prepared_id)
            created_path = os.path.join(os.path.abspath(session.config["runtime"]["run_root"]),
                                        "range_validation_resident", "prepared_%s" % session.prepared_id)
            if (session_runtime != runtime or os.path.abspath(session.session_root) != created_path
                    or os.path.realpath(session.session_root) != expected
                    or os.path.abspath(session.workspace_root) != os.path.join(created_path, "workspace")
                    or os.path.realpath(session.workspace_root) != os.path.join(expected, "workspace")
                    or not session.token.startswith("validation-%s-" % session.prepared_id)):
                return None
            return {"prepared_id": session.prepared_id, "session_token": session.token,
                    "session_root": expected, "runtime_root": runtime}
        except (AttributeError, KeyError, TypeError, ValueError):
            return None

    def _bind_range_validation_cleanup(self, job_id, session):
        identity = self._owned_resident_cleanup_identity(session)
        if identity is None:
            return False
        with self.lock:
            current = dict(self.range_validations.get(job_id) or {})
            expected_job_root = os.path.join(identity["runtime_root"], "range_validations", job_id)
            if (str(current.get("prepared_id")) != identity["prepared_id"]
                    or os.path.realpath(current.get("job_root") or "") != expected_job_root):
                return False
            self.range_validation_cleanup_sessions[job_id] = {
                "session": session, "identity": dict(identity),
            }
            current["resident_cleanup"] = dict(identity, state="pending")
            self.range_validations[job_id] = current
        return True

    def _record_resident_cleanup_proven(self, session, result):
        identity = self._owned_resident_cleanup_identity(session)
        if (identity is None or not isinstance(result, dict)
                or result.get("cleanup_proven") is not True or session.closed is not True):
            return []
        recovered = []
        with self.lock:
            for job_id, binding in list(self.range_validation_cleanup_sessions.items()):
                if binding.get("session") is not session or binding.get("identity") != identity:
                    continue
                current = dict(self.range_validations.get(job_id) or {})
                receipt = current.get("resident_cleanup") or {}
                if (str(current.get("prepared_id")) != identity["prepared_id"]
                        or os.path.realpath(current.get("job_root") or "") != os.path.join(
                            identity["runtime_root"], "range_validations", job_id)
                        or any(receipt.get(key) != value for key, value in identity.items())):
                    continue
                current["resident_cleanup"] = dict(identity, state="proven", finished_at=utc_timestamp())
                # A resident-to-nonresident fallback can fail later. Its unsafe
                # result, or an unknown cleanup source, is never cleared here.
                if (current.get("unsafe_cleanup_scope") == "resident_validation"
                        and current.get("unsafe_cleanup_session_token") == identity["session_token"]):
                    current["unsafe_to_optimize"] = False
                    current["cleanup_recovery"] = {
                        "state": "proven", "finished_at": utc_timestamp(),
                        "previous_cleanup_error": current.get("cleanup_error"),
                    }
                    recovered.append(job_id)
                self.range_validations[job_id] = current
                try:
                    atomic_write_json(os.path.join(current["job_root"], _contract_text("status.json")), current)
                except Exception as error:
                    current["diagnostic_write_error"] = "%s: %s" % (error.__class__.__name__, error)
            for prepared_id, prepared in self.prepared.items():
                capability = prepared.get("resident_validation") or {}
                receipt = capability.get("resident_cleanup") or {}
                if (str(prepared_id) == identity["prepared_id"]
                        and str(capability.get("prepared_id", prepared_id)) == identity["prepared_id"]
                        and capability.get("unsafe_cleanup_scope") == "resident_validation"
                        and capability.get("unsafe_cleanup_session_token") == identity["session_token"]
                        and all(receipt.get(key) == value for key, value in identity.items())):
                    capability["unsafe_to_optimize"] = False
                    capability["resident_cleanup"] = dict(identity, state="proven", finished_at=utc_timestamp())
        return recovered

    def _resident_cleanup_error(self, message, session):
        error = RuntimeError(message)
        error.unsafe_to_optimize = True
        identity = self._owned_resident_cleanup_identity(session)
        if identity is not None:
            error.unsafe_cleanup_scope = "resident_validation"
            error.unsafe_cleanup_session_token = identity["session_token"]
            error.resident_cleanup = dict(identity, state="pending")
        return error

    def _close_resident_validation(self, timeout_seconds=None):
        with self.lock:
            session = self.resident_validation
        if session is None:
            return None
        if timeout_seconds is None:
            timeout_seconds = self.config.get("runtime", {}).get(
                "range_validation_shutdown_timeout_seconds", 30,
            )
        timeout_seconds = max(0.0, float(timeout_seconds))
        try:
            result = session.close(timeout_seconds=timeout_seconds)
        except Exception as error:
            unsafe_error = self._resident_cleanup_error(
                "resident PDK callback validation cleanup could not be proven: %s"
                % error, session,
            )
            raise unsafe_error
        self._record_resident_cleanup_proven(session, result)
        with self.lock:
            if self.resident_validation is session:
                self.resident_validation = None
        return result

    def _record_optional_range_validation_unavailable(
            self, prepared_id, variables, reason, unsafe_to_optimize=False,
            failure_stage=None, workspace_preflight_failed=False,
            resident_cleanup=None):
        job_id = "range_validation_%s" % uuid.uuid4().hex[:12]
        job_root = os.path.join(
            self.config["runtime"]["run_root"], "range_validations", job_id,
        )
        reason = str(reason or "PDK callback validation is unavailable")
        unsafe_to_optimize = bool(unsafe_to_optimize)
        workspace_preflight_failed = bool(workspace_preflight_failed)
        message = "Optional PDK validation skipped; optimization remains available."
        status = {
            "schema_version": _contract_text("crescendo.range_validation.v1"),
            "job_id": job_id, "job_root": job_root,
            "prepared_id": prepared_id, "state": "failed",
            "message": message, "created_at": utc_timestamp(),
            "detail": reason, "detail_path": os.path.join(job_root, _contract_text("status.json")),
            "updated_at": utc_timestamp(), "finished_at": utc_timestamp(),
            "active_endpoint": None, "parameter_count": len(variables),
            "results": [], "optional": True,
            "blocking": False,
            "warning_code": (
                "cadence_cleanup_unproven" if unsafe_to_optimize
                else "pdk_callback_validation_unavailable"
            ),
        }
        if failure_stage:
            status["failure_stage"] = failure_stage
        if unsafe_to_optimize:
            status["unsafe_to_optimize"] = True
        ensure_dir(job_root)
        atomic_write_json(os.path.join(job_root, _contract_text("status.json")), status)
        with self.lock:
            self.range_validations[job_id] = status
            session = self.resident_validation
        identity = self._owned_resident_cleanup_identity(session)
        if (unsafe_to_optimize and identity is not None
                and isinstance(resident_cleanup, dict)
                and all(resident_cleanup.get(key) == value for key, value in identity.items())
                and self._bind_range_validation_cleanup(job_id, session)):
            with self.lock:
                current = self.range_validations[job_id]
                current["unsafe_cleanup_scope"] = "resident_validation"
                current["unsafe_cleanup_session_token"] = identity["session_token"]
                atomic_write_json(os.path.join(job_root, _contract_text("status.json")), current)
        return {
            "job_id": job_id, "state": "failed", "optional": True,
            "blocking": False,
        }

    def _start_resident_validation(self, prepared_id, prepared, cancel_check=None,
                                   deadline=None, diagnostic_root=None, parameters=None,
                                   validation_job_id=None):
        try:
            self._close_resident_validation()
        except Exception as error:
            if getattr(error, "unsafe_to_optimize", False):
                raise
            unsafe_error = RuntimeError(
                "existing PDK callback validation cleanup could not be proven: %s"
                % error
            )
            unsafe_error.unsafe_to_optimize = True
            raise unsafe_error
        root = os.path.join(
            self.config["runtime"]["run_root"], "range_validation_resident",
            "prepared_%s" % prepared_id,
        )
        session = ResidentValidationSession(prepared_id, prepared, self.config, root)
        with self.lock:
            self.resident_validation = session
        if validation_job_id is not None:
            self._bind_range_validation_cleanup(validation_job_id, session)
        try:
            if cancel_check is None and deadline is None and parameters is None:
                session.start()
            else:
                options = {"cancel_check": cancel_check, "deadline": deadline}
                if parameters is not None:
                    options["parameters"] = parameters
                session.start(**options)
        except Exception as error:
            if diagnostic_root:
                try:
                    session.export_diagnostics(diagnostic_root)
                except Exception:
                    pass
            cleanup_error = None
            try:
                cleanup = session.close(timeout_seconds=5)
                self._record_resident_cleanup_proven(session, cleanup)
            except Exception as close_error:
                cleanup_error = close_error
            if cleanup_error is not None:
                unsafe_error = self._resident_cleanup_error(
                    "resident validation startup failed and Cadence cleanup could "
                    "not be proven: %s; cleanup: %s" % (error, cleanup_error), session,
                )
                raise unsafe_error
            with self.lock:
                if self.resident_validation is session:
                    self.resident_validation = None
            raise
        with self.lock:
            self.resident_validation = session
        return session

    def _require_idle_setup_resources(self, action):
        with self.lock:
            optimization_active = self.optimization_launches or any(
                thread.is_alive() for thread in self.threads.values()
            )
            validation_active = any(
                status.get("state") not in _TERMINAL_RANGE_VALIDATION_STATES
                for status in self.range_validations.values()
            )
            validation_finishing = any(thread.is_alive() for thread in self.range_validation_threads.values())
        if optimization_active and action != "validate parameter ranges":
            raise RuntimeError("cannot %s while an optimization is running" % action)
        if validation_active or validation_finishing:
            if action == "validate parameter ranges":
                raise RuntimeError("a range validation job is already running")
            if action == "load an ADE setup" and self._validation_resources_are_isolated():
                return
            state = "running" if validation_active else "finishing"
            raise RuntimeError("cannot %s while parameter range validation is %s" % (action, state))

    def _validate_launch_binding(self, path):
        context = self.config.get("_maestro_launch_context")
        if context:
            from .maestro_context import validate_bound_setup
            validate_bound_setup(context, path)
        selected = self.config.get("_selected_setup_path")
        if selected:
            locator = resolve_setup_locator(os.path.realpath(os.path.abspath(os.path.expanduser(path))), backend="auto")
            if os.path.realpath(locator["path"]) != selected:
                raise ValueError(_contract_text("This Crescendo window currently uses another saved setup. Select the intended setup with Load and Inspect before preparing or running."))

    def select_setup(self, path):
        """Explicitly select a saved setup; ordinary prepare remains bound.

        Validate before closing owned resident resources or publishing a new
        configuration. Selection never writes a source design or starts EDA.
        """
        self._begin_lifecycle_operation("select an ADE setup", exclusive=True)
        try:
            return self._select_setup_registered(path)
        finally:
            self._end_lifecycle_operation(exclusive=True)

    def _select_setup_registered(self, path):
        """Select inside an already-owned Setup lifecycle operation."""
        self._remember_scratch(self.config)
        self._require_idle_setup_resources("select an ADE setup")
        with self.lock:
            if any(thread.is_alive() for thread in self.range_validation_threads.values()):
                raise RuntimeError("cannot select an ADE setup while parameter range validation is finishing")
        locator = resolve_setup_locator(os.path.realpath(os.path.abspath(os.path.expanduser(path))), backend="auto")
        canonical = os.path.realpath(locator["path"])
        locator = resolve_setup_locator(canonical, backend=locator["backend"])
        origin = self.invoking_context
        if origin and canonical == os.path.realpath((origin.get("source") or {}).get("setup_path") or ""):
            from .maestro_context import refresh_saved_launch_context
            current_context = self.config.get("_maestro_launch_context")
            context = refresh_saved_launch_context(current_context or origin, canonical)
            proposed = copy.deepcopy(self.config if current_context else self.invoking_config)
            # Explicit user PDK choices survive changing the saved setup.
            from .config import PDK_KEYS
            current = self.config.get("cadence") or {}
            explicit = deep_merge(current.get("_manual_overrides") or {}, current.get("_file_overrides") or {})
            for key in PDK_KEYS + ("search_roots",):
                if key in explicit:
                    proposed.setdefault("cadence", {})[key] = copy.deepcopy(explicit[key])
            for key in ("_manual_overrides", "_file_overrides"):
                if key in current:
                    proposed.setdefault("cadence", {})[key] = copy.deepcopy(current[key])
            proposed["_maestro_launch_context"] = copy.deepcopy(context)
            mode = "invoking"
            source = copy.deepcopy(context.get("source") or {})
        else:
            from .maestro_context import selected_setup_config
            proposed = selected_setup_config(self.invoking_config, self.config, locator)
            context, mode = None, "manual"
            source = copy.deepcopy(proposed.get("_selected_setup_source") or {})
        self._cancel_and_wait_range_validations(action="select an ADE setup")
        self._close_resident_validation()
        # Only explicit Load adopts newly saved Maestro settings. EDA
        # Overrides keep this binding while rechecking tools and models.
        proposed.pop("_setup_baseline", None)
        with self.lock:
            self.config = proposed
            self.prepared.clear()
            self.inspections.clear()
            self.test_inspections.clear()
            self.inspection_epoch += 1
        self._remember_scratch(self.config)
        self._load_run_index()
        return {
            "context": context, "setup_path": canonical, "mode": mode,
            "source": source, "eda_setup": self.eda_setup(),
        }

    def _validate_live_test_bindings(self, setup):
        if self.config.get("_selected_setup_path"):
            reference = ((setup or {}).get("setup_locator") or {}).get("path")
            source = (setup or {}).get("source") or {}
            reference = reference or source.get("setup_db") or source.get("maestro_sdb") or source.get("data_sdb")
            if not reference:
                raise ValueError("Prepared setup has no saved-setup identity; Load and Inspect again before running.")
            self._validate_launch_binding(reference)
        context = self.config.get("_maestro_launch_context")
        if not context:
            return
        selected_test = (setup or {}).get("selected_test")
        if not selected_test:
            # The saved catalogue precedes Test selection. Other live Test
            # bindings cannot decide whether the eventual selection is safe.
            return
        saved = dict((str(row.get("name")), row) for row in ((setup or {}).get("_source_tests") or (setup or {}).get("tests") or []))
        for live in context.get("live_tests") or []:
            name = str(live.get("name") or "")
            if name != selected_test:
                continue
            row = saved.get(name)
            fields = [key for key in ("library", "cell", "view") if live.get(key)]
            if fields and (row is None or any(str(row.get(key) or "") != str(live[key]) for key in fields)):
                raise ValueError(
                    "The saved setup differs from Maestro test %s. Save in Maestro and reopen the optimizer from its menu." % name
                )

    @staticmethod
    def _inspection_setup_identity(setup):
        reference = build_setup_reference(setup)
        source = setup.get("source") or {}
        path = effective_setup_path(setup) if setup.get("baseline") else reference["path"]
        state_path = (os.path.join(effective_setup_dir(setup), "active.state") if setup.get("baseline") else
                      source.get("active_state") or os.path.join(
            source.get("setup_dir") or os.path.dirname(reference["path"]), "active.state",
        ))
        return {
            "path": os.path.realpath(reference["path"]),
            "backend": reference["backend"],
            "sha256": sha256_file(path),
            "active_state_path": os.path.realpath(source.get("active_state") or os.path.join(
                source.get("setup_dir") or os.path.dirname(reference["path"]), "active.state")),
            "active_state_sha256": sha256_file(state_path) if os.path.isfile(state_path) else None,
        }

    def inspect_tests(self, path, backend=None):
        """Read the saved catalogue without hierarchy, CDF or native private work."""
        self._begin_lifecycle_operation("inspect saved Tests", exclusive=True)
        try:
            return self._inspect_tests_registered(path, backend=backend)
        finally:
            self._end_lifecycle_operation(exclusive=True)

    def _inspect_tests_registered(self, path, backend=None):
        from .test_catalogue import load_test_catalogue
        from .test_selection import catalogue
        with command_cancellation_scope(self.prepare_cancel_event):
            self._validate_launch_binding(path)
            self._require_idle_setup_resources("inspect saved Tests")
            self._set_activity("saved_setup", "Reading saved Maestro Tests")
            setup = load_test_catalogue(path, self.config, backend=backend or "auto")
            self._validate_live_test_bindings(setup)
            catalogue(setup)
            identity = self._inspection_setup_identity(setup)
            if setup.get("metadata_identity") and any(
                    identity.get(key) != value for key, value in setup["metadata_identity"].items()):
                raise ValueError("ADE setup changed while reading Tests; Load and Inspect again.")
            reference = build_setup_reference(setup)
            token = uuid.uuid4().hex
            preview = {"schema_version": _contract_text("crescendo.test_inspection.v1"), "setup": setup,
                       "setup_locator": reference, "setup_path": reference["path"],
                       "tests": copy.deepcopy(setup.get("tests") or [])}
            with self.lock:
                check_command_cancellation()
                self.test_inspections.clear()
                self.test_inspections[token] = {"preview": preview, "identity": identity,
                                                "epoch": self.inspection_epoch}
            self._set_activity("inspection_complete", "Saved Tests ready", "complete")
            return token, copy.deepcopy(preview)

    def inspect_test_cells(self, test_inspection_id, test_name, max_depth=32):
        self._begin_lifecycle_operation("inspect selected Test cells", exclusive=True)
        try:
            return self._inspect_test_cells_registered(test_inspection_id, test_name, max_depth)
        except Exception as error:
            self._set_activity("inspection_failed", str(error), "error")
            raise
        finally:
            self._end_lifecycle_operation(exclusive=True)

    def _inspect_test_cells_registered(self, test_inspection_id, test_name, max_depth=32):
        with self.lock:
            record = self.test_inspections.get(test_inspection_id)
            if record is None or record["epoch"] != self.inspection_epoch:
                raise ValueError("Test inspection is no longer current; Load and Inspect again.")
            record = copy.deepcopy(record)
        from .single_test import project_setup
        setup = project_setup(record["preview"]["setup"], test_name)
        if record["identity"] != self._inspection_setup_identity(setup):
            raise ValueError("ADE setup changed since Test inspection; Load and Inspect again.")
        self._require_idle_setup_resources("inspect selected Test cells")
        self._close_resident_validation()
        with self.lock:
            references = list(self.setup_private_sessions.values())
        from .setup_private_session import close_setup_private_session, private_session_info
        for reference in references:
            try:
                current = private_session_info(reference)
            except ValueError:
                current = None
            if current and current.get("selected_test") == test_name:
                continue
            try:
                result = close_setup_private_session(reference)
            except Exception as error:
                raise RuntimeError("Previous Maestro/config working-copy views could not be safely retired; "
                                   "no new working copy was created. Evidence: %s (%s)" %
                                   (reference.get("root"), error))
            if result.get("cleanup_proven") is not True:
                raise RuntimeError("Previous working Setup cleanup is unproven; no new working copy was created. Evidence: %s" % result)
            with self.lock:
                self.setup_private_sessions.pop(reference["session_id"], None)
        with self.lock:
            self.prepared.clear()
            self.inspections.clear()
        reference = record["preview"]["setup_locator"]
        result = self._inspect_cells_registered(reference["path"], max_depth=max_depth,
                                                backend=reference["backend"], selected_test=test_name)
        if record["identity"] != self._inspection_setup_identity(record["preview"]["setup"]):
            with self.lock:
                self.inspections.pop(result[0], None)
                self.prepared.clear()
            raise ValueError("ADE setup changed during Test cell discovery; Load and Inspect again.")
        return result

    def inspect_cells(self, path, max_depth=32, backend=None, selected_test=None):
        """Compatibility entry: discover Tests before any single-Test scan."""
        token, preview = self.inspect_tests(path, backend=backend)
        names = [row.get("name") for row in preview["tests"]]
        if selected_test is None:
            if len(names) != 1:
                raise ValueError("Select exactly one Test before loading cells.")
            selected_test = names[0]
        return self.inspect_test_cells(token, selected_test, max_depth=max_depth)

    def _inspect_cells_registered(self, path, max_depth=32, backend=None, selected_test=None):
        """Inspect inside an already-owned Setup lifecycle operation."""
        from .hierarchy import cell_scope_records, probe_hierarchy
        from .runner import configuration_binding_identity
        from .workspace import config_dependency_identity
        try:
            with command_cancellation_scope(self.prepare_cancel_event):
                self._validate_launch_binding(path)
                self._require_idle_setup_resources("inspect cell hierarchy")
                self._set_activity("saved_setup", "Reading saved Maestro settings")
                setup = load_bound_setup(path, self.config, backend="auto" if backend is None else backend)
                if selected_test is not None:
                    from .single_test import project_setup
                    setup = project_setup(setup, selected_test)
                self._validate_live_test_bindings(setup)
                from .job_policy import capture_setup_job_policy
                self._set_activity("job_policy", "Reading native Job Setup and Save Options")
                job_policy = capture_setup_job_policy(setup, self.config)
                identity = self._inspection_setup_identity(setup)
                self._set_activity("hierarchy", "Discovering design cells")
                hierarchy = probe_hierarchy(setup, self.config, max_depth=max_depth, cells_only=True)
                config_identity = config_dependency_identity(setup, hierarchy, config=self.config)
                binding_identity = configuration_binding_identity(hierarchy)
                config_dependencies = {"designs": [
                    {"design": row.get("design"), "config_dependencies": copy.deepcopy(
                        row.get("config_dependencies") or (row.get("hierarchy") or {}).get("config_dependencies") or []
                    )} for row in hierarchy.get("designs") or []
                    if row.get("resolved_top") or (row.get("hierarchy") or {}).get("resolved_top")
                ]}
                scopes = [row for row in cell_scope_records(
                    hierarchy, setup=setup, config=self.config,
                ) if row.get("selectable")]
                check_command_cancellation()
                if identity != self._inspection_setup_identity(setup):
                    raise ValueError("ADE setup changed during cell discovery; Load Cells again.")
                reference = build_setup_reference(setup)
                preview = {
                    "schema_version": _contract_text("crescendo.cell_inspection.v1"),
                    "setup_locator": reference, "setup_path": reference["path"],
                    "setup": setup, "hierarchy": hierarchy, "cell_scopes": scopes,
                    "job_policy": job_policy,
                    "eda_context": {"cadence": self.eda_setup(setup)},
                }
                self._set_activity("private_copy", "Creating Maestro and config working copies")
                self._prepare_setup_private_session(preview)
                token = uuid.uuid4().hex
                with self.lock:
                    check_command_cancellation()
                    self.inspections.clear()
                    self.inspections[token] = {
                        # Apply rescans native metadata. Retain only the exact
                        # choice and setup binding, not another copy of every
                        # leaf instance in a large testbench.
                        "preview": copy.deepcopy({
                            "setup_locator": reference, "setup": setup, "cell_scopes": scopes,
                            "setup_private_session": preview.get("setup_private_session"),
                        }), "identity": identity,
                        "max_depth": max_depth, "epoch": self.inspection_epoch,
                        "prepared_ids": [],
                        "config_identity": config_identity,
                        "binding_identity": binding_identity,
                        "config_dependencies": config_dependencies,
                    }
                self._set_activity("inspection_complete", "Maestro settings and design cells ready", "complete")
                return token, preview
        except CommandCancelled as error:
            if error.cleanup_failed:
                with self.lock:
                    self.prepare_cleanup_failures.append(str(error))
            raise

    def prepare_cells(self, inspection_id, scope_ids):
        """Apply selected cell occurrences through the complete prepare safeguards."""
        self._begin_lifecycle_operation("apply selected cells", exclusive=True)
        try:
            return self._prepare_cells_registered(inspection_id, scope_ids)
        except Exception as error:
            self._set_activity("parameter_preparation_failed", str(error), "error")
            raise
        finally:
            self._end_lifecycle_operation(exclusive=True)

    def _prepare_cells_registered(self, inspection_id, scope_ids):
        """Apply inside an already-owned Setup lifecycle operation."""
        from .hierarchy import cell_scope_records
        from .runner import configuration_binding_identity
        from .workspace import config_dependency_identity, source_design_identity
        prepared_id = None
        try:
            with command_cancellation_scope(self.prepare_cancel_event):
                self._require_idle_setup_resources("apply selected cells")
                if not isinstance(inspection_id, str):
                    raise ValueError("Cell inspection is no longer current; Load Cells again.")
                with self.lock:
                    record = self.inspections.get(inspection_id)
                    if record is None or record["epoch"] != self.inspection_epoch:
                        raise ValueError("Cell inspection is no longer current; Load Cells again.")
                    record = copy.deepcopy(record)
                if not isinstance(scope_ids, (list, tuple)) or not scope_ids:
                    raise ValueError("Select at least one cell before Apply Cells.")
                by_id = dict((row["scope_id"], row) for row in record["preview"]["cell_scopes"])
                selected = []
                selected_ids = set()
                for scope_id in scope_ids:
                    if not isinstance(scope_id, str) or scope_id not in by_id:
                        raise ValueError("Selected cell is absent from this inspection; Load Cells again.")
                    if scope_id not in selected_ids:
                        selected.append(by_id[scope_id])
                        selected_ids.add(scope_id)
                preview = record["preview"]
                reference = preview["setup_locator"]
                self._validate_launch_binding(reference["path"])
                self._validate_live_test_bindings(preview["setup"])
                if record["identity"] != self._inspection_setup_identity(preview["setup"]):
                    raise ValueError("ADE setup changed since cell discovery; Load Cells again.")
                if record.get("config_identity") is not None and record["config_identity"] != config_dependency_identity(
                        preview["setup"], record["config_dependencies"], config=self.config):
                    raise ValueError("Config bindings changed since cell discovery; Load Cells again.")
                self._set_activity("parameter_scan", "Reading parameters in selected cells")
                prepared_id, prepared = self._prepare_registered(
                    reference["path"], max_depth=record["max_depth"], backend=reference["backend"],
                    initialize_validation=False, selected_cell_paths=selected,
                    defer_finalize=True, selected_test=preview["setup"].get("selected_test"),
                )
                scanned_source_identity = copy.deepcopy(prepared.get("source_identity"))
                observed = dict((row["scope_id"], row) for row in cell_scope_records(
                    prepared["hierarchy"], setup=prepared["setup"], config=self.config,
                ) if row.get("selectable"))
                identity_fields = ("design", "hierarchy_path", "library", "cell", "view")
                if any(row["scope_id"] not in observed or any(
                        row.get(key) != observed[row["scope_id"]].get(key)
                        for key in identity_fields) for row in selected):
                    raise ValueError("Selected cell hierarchy changed; Load Cells again.")
                if record["identity"] != self._inspection_setup_identity(prepared["setup"]):
                    raise ValueError("ADE setup changed during Apply Cells; Load Cells again.")
                if record.get("binding_identity") != configuration_binding_identity(prepared["hierarchy"]):
                    raise ValueError("Resolved config hierarchy changed during Apply Cells; Load Cells again.")
                if record.get("config_identity") != config_dependency_identity(
                        prepared["setup"], prepared["hierarchy"], config=self.config):
                    raise ValueError("Config files changed during Apply Cells; Load Cells again.")
                reference = preview.get("setup_private_session")
                if reference:
                    from .setup_private_session import private_session_info
                    current = private_session_info(reference)
                    if not self._private_session_source_matches(current, prepared):
                        # The full parameter scan has already checked this
                        # current hierarchy. Adopt its source at this explicit
                        # Load/Apply boundary; never repair a running source
                        # guard by replacing snapshots underneath it.
                        self._prepare_setup_private_session(prepared,
                            expected_source_identity=prepared.get("source_identity"))
                        reference = prepared["setup_private_session"]
                        # Replacement retires every authority for the old
                        # suite. Re-publish only this checked inspection inside
                        # the exclusive transaction, with its new owner.
                        record["preview"]["setup_private_session"] = copy.deepcopy(reference)
                        record["prepared_ids"] = []
                        with self.lock:
                            check_command_cancellation()
                            self.inspections[inspection_id] = record
                self._set_activity("private_apply", "Applying selected cells to the working setup")
                self._apply_setup_private_cells(prepared, reference)
                check_command_cancellation()
                # Native readback can add legitimate metadata (for example
                # per-Test corner masks). Seal once after it has completed,
                # retaining the source identity from the parameter scan.
                if record["identity"] != self._inspection_setup_identity(prepared["setup"]):
                    raise ValueError("ADE setup changed during working Apply; Load and Inspect again.")
                current_source_identity = source_design_identity(
                    prepared["setup"], prepared["hierarchy"], config=self.config)
                if not self._private_session_source_matches(
                        {"source_identity": scanned_source_identity},
                        {"source_identity": current_source_identity}):
                    raise ValueError("Source design changed during working Apply; Load and Inspect again.")
                prepared["source_identity"] = scanned_source_identity
                prepared.pop("suite_selection_required", None)
                prepared["prepared_freshness"] = capture_prepared_freshness(prepared)
                # Saved baselines carry catalogue evidence, never the live
                # process authority attached by Apply. Resume reattaches only
                # a matching owner in its own UI registry.
                snapshot = dict(prepared)
                snapshot.pop("setup_private_session", None)
                snapshot.pop("resident_validation", None)
                persisted = persist_prepared_baseline(snapshot)
                if persisted:
                    prepared["setup_baseline_snapshot"] = persisted
                with self.lock:
                    check_command_cancellation()
                    live_record = self.inspections.get(inspection_id)
                    if live_record is None:
                        raise ValueError("Cell inspection is no longer current; Load Cells again.")
                    for previous_id in live_record["prepared_ids"]:
                        self.prepared.pop(previous_id, None)
                    self.prepared[prepared_id] = prepared
                    live_record["prepared_ids"] = [prepared_id]
                if prepared.get("setup_private_session"):
                    from .setup_private_session import update_setup_private_session_snapshot
                    update_setup_private_session_snapshot(prepared["setup_private_session"], prepared)
                self._set_activity("parameters_ready", "Selected cell parameters ready", "complete")
                return prepared_id, prepared
        except Exception as error:
            with self.lock:
                if prepared_id is not None:
                    self.prepared.pop(prepared_id, None)
                if isinstance(error, CommandCancelled) and error.cleanup_failed:
                    self.prepare_cleanup_failures.append(str(error))
            raise

    def prepare(self, path, max_depth=32, backend=None, initialize_validation=True,
                selected_cell_paths=None, selected_test=None):
        self._begin_lifecycle_operation("prepare an ADE setup")
        try:
            with command_cancellation_scope(self.prepare_cancel_event):
                setup = load_bound_setup(path, self.config, backend=backend or "auto")
                rows = setup.get("_source_tests") or setup.get("tests") or []
                if selected_test is None:
                    if len(rows) != 1:
                        raise ValueError("Select exactly one Test before preparing.")
                    selected_test = rows[0].get("name")
                options = {"max_depth": max_depth, "backend": backend,
                           "initialize_validation": initialize_validation, "selected_test": selected_test}
                if selected_cell_paths is not None:
                    options["selected_cell_paths"] = selected_cell_paths
                return self._prepare_registered(path, **options)
        except CommandCancelled as error:
            if error.cleanup_failed:
                with self.lock:
                    self.prepare_cleanup_failures.append(str(error))
            raise
        finally:
            self._end_lifecycle_operation()

    def _prepare_registered(self, path, max_depth=32, backend=None, initialize_validation=True,
                            selected_cell_paths=None, defer_finalize=False, selected_test=None):
        self._validate_launch_binding(path)
        self._require_idle_setup_resources(
            "initialize PDK callback validation" if initialize_validation else "load an ADE setup",
        )
        requested_backend = "auto" if backend is None else backend
        if selected_cell_paths is not None or selected_test is not None:
            prepared = prepare_setup(
                path, self.config, max_depth=max_depth, backend=requested_backend,
                selected_cell_paths=selected_cell_paths, selected_test=selected_test,
            )
        elif requested_backend == "maestro":
            prepared = prepare_maestro(path, self.config, max_depth=max_depth)
        else:
            prepared = prepare_setup(
                path, self.config, max_depth=max_depth, backend=requested_backend,
            )
        check_command_cancellation()
        self._validate_live_test_bindings(prepared.get("setup"))
        from .job_policy import capture_setup_job_policy
        if "job_policy" not in prepared:
            prepared["job_policy"] = capture_setup_job_policy(prepared.get("setup") or {}, self.config)
        eda_context = dict(prepared.get("eda_context") or {})
        pdk_context = eda_context.get("pdk")
        if not isinstance(pdk_context, dict):
            pdk_context = detect_pdk_context(
                prepared.get("setup"), prepared.get("hierarchy"),
                self.config.get("cadence"),
            )
        eda_context.update({
            "cadence": self.eda_setup(prepared.get("setup")),
            "pdk": pdk_context,
        })
        prepared["eda_context"] = eda_context
        if selected_cell_paths is not None:
            from .hierarchy import cell_scope_records
            cell_selection_scopes({"scopes": selected_cell_paths})
            allowed = dict((tuple(row["design"].get(key) for key in ("library", "cell", "view"))
                            + (row["hierarchy_path"],), row) for row in cell_scope_records(
                prepared["hierarchy"], setup=prepared["setup"], config=self.config,
            ) if row.get("selectable"))
            for selected in selected_cell_paths:
                key = tuple(selected["design"].get(name) for name in ("library", "cell", "view")) + (selected["hierarchy_path"],)
                actual = allowed.get(key)
                if actual is None or any(selected.get(key) is not None and selected[key] != actual.get(key)
                                         for key in ("library", "cell", "view")):
                    raise ValueError("Selected cell hierarchy changed or belongs to a reference library; Load Cells again.")
                if selected.get("scope_id") is not None and selected["scope_id"] != actual["scope_id"]:
                    raise ValueError("Selected cell identifier does not match its native occurrence; Load Cells again.")
            prepared["cell_selection"] = {
                "scope_ids": [row["scope_id"] for row in selected_cell_paths if row.get("scope_id")],
                "scopes": copy.deepcopy(selected_cell_paths),
            }
        baseline = freeze_setup_baseline(prepared, self.config)
        if baseline:
            from .workspace import source_design_identity
            prepared["source_identity"] = source_design_identity(
                prepared["setup"], prepared["hierarchy"], config=self.config)
        if defer_finalize:
            prepared.pop("prepared_freshness", None)
            prepared.pop("setup_baseline_snapshot", None)
        else:
            prepared["prepared_freshness"] = capture_prepared_freshness(prepared)
            persist_prepared_baseline(prepared)
        if baseline:
            self.config["_setup_baseline"] = {
                "origin_setup_path": (prepared["setup"].get("source") or {}).get("setup_db") or
                                     (prepared["setup"].get("source") or {}).get("maestro_sdb"),
                "baseline": copy.deepcopy(baseline),
            }
        check_command_cancellation()
        prepared_id = uuid.uuid4().hex
        # Loading/overriding environment never starts a bare-CDF process.
        # Endpoint validation uses the same native Run netlisting as candidates.
        prepared["resident_validation"] = {
            "state": "deferred", "prepared_id": prepared_id,
            "method": "native_run_device_parameters",
            "message": "Native Maestro endpoint netlists are generated only when range validation is requested.",
        }
        if self.resident_validation is not None:
            try:
                self._close_resident_validation()
            except Exception as error:
                prepared["resident_validation"].update(
                    state="unavailable", message=str(error), unsafe_to_optimize=True,
                    failure_stage="legacy_validation_cleanup")
        with self.lock:
            check_command_cancellation()
            if not defer_finalize:
                self.prepared[prepared_id] = prepared
        return prepared_id, prepared

    def _remember_setup_private_session(self, reference):
        if reference:
            with self.lock:
                self.setup_private_sessions[reference["session_id"]] = copy.deepcopy(reference)

    def _setup_private_diagnostic(self, reference, error=None):
        if not reference:
            return None
        try:
            from .setup_private_diagnostics import write_setup_diagnostic_log
            path = write_setup_diagnostic_log(reference, error=error)
            reference["diagnostic_log"] = path
            if error is not None:
                error.diagnostic_log = path
                # The UI must point to one report even if Setup failed before
                # an optimization or environment-test case could be launched.
                error.args = (str(error) + "\nSetup 总日志：" + path,)
            return path
        except Exception as diagnostic_error:
            if error is not None:
                error.diagnostic_collection_error = str(diagnostic_error)
            return None

    @staticmethod
    def _private_session_source_matches(reference, prepared):
        current = (reference or {}).get("source_identity") or {}
        expected = (prepared or {}).get("source_identity") or {}
        return bool(expected.get("schema_version") and expected.get("fingerprint") and
                    all(current.get(key) == expected.get(key) for key in ("schema_version", "fingerprint")))

    def _prepare_setup_private_session(self, preview, expected_source_identity=None):
        """Keep one owned suite; a new saved baseline replaces the old suite."""
        if (preview.get("setup") or {}).get("backend") != "maestro":
            return
        self._require_idle_setup_resources("replace working Setup views")
        from .workspace import source_design_identity
        from .setup_private_session import (close_setup_private_session,
            create_setup_private_session, private_session_info)
        baseline = freeze_setup_baseline(preview, self.config)
        if baseline:
            self.config["_setup_baseline"] = {
                "origin_setup_path": (preview["setup"].get("source") or {}).get("setup_db") or
                                     (preview["setup"].get("source") or {}).get("maestro_sdb"),
                "baseline": copy.deepcopy(baseline),
            }
        preview["source_identity"] = source_design_identity(
            preview["setup"], preview["hierarchy"], config=self.config)
        if expected_source_identity is not None and not self._private_session_source_matches(
                {"source_identity": expected_source_identity}, preview):
            raise ValueError("Source design changed during parameter inspection; Load and Inspect again.")
        with self.lock:
            references = list(self.setup_private_sessions.values())
        baseline_id = (baseline or {}).get("baseline_id")
        reusable = None
        source_changed = False
        for reference in references:
            if baseline_id and reference.get("baseline_id") == baseline_id:
                try:
                    current = private_session_info(reference)
                except ValueError:
                    # Closed references are retired below. An ownership error
                    # must still pass the exact close gate before a new copy.
                    current = None
                if (reusable is None and current and current.get("status") == "ready"
                        and current.get("selected_test") == (preview.get("setup") or {}).get("selected_test")
                        and current.get("selected_test") is not None
                        and not current.get("unsafe") and self._private_session_source_matches(current, preview)):
                    reusable = current
                elif current and not self._private_session_source_matches(current, preview):
                    source_changed = True
        if source_changed:
            self._set_activity("private_copy", "Source design changed; refreshing the working setup from the current design.", level="WARN")
        for reference in references:
            if reusable and reference["session_id"] == reusable["session_id"]:
                continue
            check_command_cancellation()
            try:
                cleanup = close_setup_private_session(reference)
            except Exception as error:
                raise RuntimeError(
                    "Previous Maestro/config working-copy views could not be safely retired; "
                    "no new working copy was created. Close any working editors, then "
                    "retry Load and Inspect. Evidence: %s (%s)" % (reference["root"], error))
            if cleanup.get("cleanup_proven") is not True:
                raise RuntimeError(
                    "Previous Maestro/config working-copy views are still in use or their "
                    "cleanup is unproven; no new working copy was created. Close any "
                    "working editors, then retry Load and Inspect. Evidence: %s (%s)" %
                    (reference["root"], cleanup.get("state") or "cleanup unproven"))
            with self.lock:
                self.setup_private_sessions.pop(reference["session_id"], None)
                for prepared_id, prepared in list(self.prepared.items()):
                    if ((prepared.get("setup_private_session") or {}).get("session_id")
                            == reference["session_id"]):
                        self.prepared.pop(prepared_id, None)
                for inspection_id, inspection in list(self.inspections.items()):
                    if (((inspection.get("preview") or {}).get("setup_private_session") or {}).get("session_id")
                            == reference["session_id"]):
                        self.inspections.pop(inspection_id, None)
        if reusable:
            reusable["diagnostic_log"] = os.path.join(reusable["root"], _contract_text("environment_test.log"))
            preview["setup_private_session"] = reusable
            if isinstance(reusable.get("test_corner_masks"), dict):
                preview["setup"]["test_corner_masks"] = copy.deepcopy(reusable["test_corner_masks"])
            return
        check_command_cancellation()
        try:
            reference = create_setup_private_session(preview, self.config,
                cancel_check=self.prepare_cancel_event.is_set)
        except Exception as error:
            failed = getattr(error, "setup_private_session", None)
            self._setup_private_diagnostic(failed, error=error)
            self._remember_setup_private_session(failed)
            raise
        reference["baseline_id"] = baseline_id
        self._setup_private_diagnostic(reference)
        self._remember_setup_private_session(reference)
        preview["setup_private_session"] = reference
        if source_changed:
            preview.setdefault("setup_refresh_warnings", []).append(
                "Source design changed since cell inspection; the working setup "
                "was rebuilt from the current design and matching selections retained.")

    def _apply_setup_private_cells(self, prepared, reference):
        if not reference:
            return
        from .setup_private_session import apply_setup_private_cells
        try:
            current = apply_setup_private_cells(reference, prepared, self.config,
                cancel_check=self.prepare_cancel_event.is_set)
        except Exception as error:
            failed = getattr(error, "setup_private_session", None) or reference
            self._setup_private_diagnostic(failed, error=error)
            self._remember_setup_private_session(failed)
            raise
        current["baseline_id"] = reference.get("baseline_id")
        self._setup_private_diagnostic(current)
        self._remember_setup_private_session(current)
        prepared["setup_private_session"] = current
        if isinstance(current.get("test_corner_masks"), dict):
            prepared["setup"]["test_corner_masks"] = copy.deepcopy(current["test_corner_masks"])

    def setup_private_session_info(self, inspection_id=None, prepared_id=None):
        """Return the registered native binding readback for Open Config."""
        from .setup_private_session import private_session_info
        with self.lock:
            prepared = self.prepared.get(prepared_id) if prepared_id else None
            inspected = self.inspections.get(inspection_id) if inspection_id else None
            reference = ((prepared or {}).get("setup_private_session") or
                         ((inspected or {}).get("preview") or {}).get("setup_private_session"))
        if not reference:
            raise ValueError("No working config is ready; use Load and Inspect first.")
        current = private_session_info(reference)
        current["diagnostic_log"] = os.path.join(reference["root"], _contract_text("environment_test.log"))
        return current

    def _attach_live_setup_private_session(self, prepared):
        """Borrow only one suite matching the checkpoint's frozen identity."""
        baseline_id = ((prepared.get("setup") or {}).get("baseline") or {}).get("baseline_id")
        test_name = (prepared.get("setup") or {}).get("selected_test")
        scopes = sorted((prepared.get("cell_selection") or {}).get("scope_ids") or [])
        parameters = sorted(row["parameter_id"] for row in prepared.get("parameters") or []
            if row.get("optimizable") and row.get("backend", "schematic") == "schematic")
        if not baseline_id or not scopes or not parameters:
            return
        expected_identity = prepared.get("source_identity") or {}
        def matches(row):
            identity = row.get("source_identity") or {}
            return (row.get("baseline_id") == baseline_id and row.get("selected_test") == test_name
                    and row.get("status") == "ready" and not row.get("unsafe")
                    and sorted(row.get("applied_scope_ids") or []) == scopes
                    and sorted(row.get("applied_parameter_ids") or []) == parameters
                    and (not expected_identity or all(identity.get(key) == expected_identity.get(key)
                        for key in ("schema_version", "fingerprint"))))
        from .setup_private_session import private_session_info
        with self.lock:
            references = list(self.setup_private_sessions.values())
        active = []
        for reference in references:
            if reference.get("baseline_id") != baseline_id:
                continue
            try:
                current = private_session_info(reference)
            except (ValueError, OSError, RuntimeError) as error:
                self._set_activity("suite_warning", "Checkpoint suite ownership could not be verified: " + str(error), level="WARN")
                continue
            if matches(current):
                active.append(current)
        if len(active) == 1:
            prepared["setup_private_session"] = active[0]
            return
        if active:
            self._set_activity("suite_warning", "Multiple Working Copies match the checkpoint; using a separate workspace.", level="WARN")
            return
        from .setup_private_session import list_setup_private_sessions, reopen_setup_private_session, close_setup_private_session
        candidates = [row for row in list_setup_private_sessions(self.config)
            if row.get("baseline_id") == baseline_id and row.get("selected_test") == test_name
            and not row.get("busy") and row.get("can_load")
            and sorted(row.get("applied_scope_ids") or []) == scopes
            and sorted(row.get("applied_parameter_ids") or []) == parameters]
        if len(candidates) != 1:
            if len(candidates) > 1:
                self._set_activity("suite_warning", "Multiple Working Copies match the checkpoint; using a separate workspace.", level="WARN")
            return
        current = None
        try:
            current = reopen_setup_private_session(candidates[0], self.config)
            current.pop("prepared", None)
            if not matches(current):
                released = close_setup_private_session(current)
                if not released.get("cleanup_proven"):
                    self._remember_setup_private_session(current)
                self._set_activity("suite_warning", "Checkpoint suite identity differs; using a separate workspace. "
                    + ("Release requires attention: " + str(released) if released.get("warnings") or not released.get("cleanup_proven") else ""), level="WARN")
                return
            self._remember_setup_private_session(current)
            prepared["setup_private_session"] = current
        except (ValueError, OSError, RuntimeError) as error:
            if current:
                try:
                    released = close_setup_private_session(current)
                    if not released.get("cleanup_proven") or released.get("warnings"):
                        self._remember_setup_private_session(current)
                except Exception:
                    self._remember_setup_private_session(current)
            self._set_activity("suite_warning", "Checkpoint suite could not be safely reused: " + str(error), level="WARN")

    def _close_setup_private_sessions(self, deadline):
        from .setup_private_session import close_setup_private_session
        with self.lock:
            references = list(self.setup_private_sessions.values())
        results, warnings = [], []
        for reference in references:
            try:
                result = close_setup_private_session(reference,
                    timeout_seconds=max(0.0, deadline - time.monotonic()))
                results.append(result)
                warnings.extend(result.get("warnings") or [])
                if result.get("cleanup_proven") is not True:
                    # Live native work remains a separate stop failure. A stale
                    # view lock or chmod failure is only a release warning.
                    raise RuntimeError("working-copy native work has not stopped: %s" % result)
            except (OSError, ValueError) as error:
                warnings.append("working copy retained; release could not be completed: %s" % error)
        return {"cleanup_proven": True, "sessions": results, "warnings": warnings}

    def eda_setup(self, setup=None):
        from .simulator_profiles import apply_source_profile_defaults, _discover_projected_simulator_profiles
        display_config = apply_source_profile_defaults(self.config, setup=setup)
        cadence = display_config.get("cadence", {})
        discovery = cadence.get("discovery", {})
        return {
            "schema_version": _contract_text("crescendo.eda_setup.v2"),
            "mode": cadence.get("setup_mode", "auto"),
            "user_config_path": cadence.get("user_config_path"),
            "ok": bool(discovery.get("ok")),
            "manual": copy.deepcopy(cadence.get("_manual_overrides") or {}),
            "effective": dict((key, cadence.get(key)) for key in (
                "virtuoso_bin", "ocean_bin", "spectre_bin", "cadence_root", "spectre_root", "cds_lib",
                "alps_bin", "alps_root", "alps_model_dir", "spectre_model_dir",
                "simulator", "simulator_threads", "simulator_result_reader", "alps_model_file_map",
                "spectre_result_reader", "alps_result_reader",
                "license_file", "extra_binary_paths", "extra_library_paths", "search_roots", "pdk_name",
                _contract_text("pdk_dir"), "pdk_adapter", "pdk_adapter_policy", "pdk_profiles", "model_roots",
                "model_path_remaps", "startup_extensions",
            )),
            "pdk_adapter_options": adapter_policy_options(),
            "sources": discovery.get("sources", {}),
            "checks": discovery.get("checks", []),
            "version_checks": discovery.get("version_checks", []),
            "missing": discovery.get("missing", []),
            "recommendations": discovery.get("recommendations", []),
            "compatibility": discovery.get("compatibility", {}),
            "simulator_profiles": _discover_projected_simulator_profiles(display_config, setup=setup),
            "model_discovery": copy.deepcopy(cadence.get("_model_discovery") or {}),
        }

    def update_eda_setup(self, body):
        self._begin_lifecycle_operation("update EDA settings", exclusive=True)
        try:
            return self._update_eda_setup_registered(body)
        finally:
            self._end_lifecycle_operation(exclusive=True)

    def _update_eda_setup_registered(self, body):
        baseline_binding = copy.deepcopy(self.config.get("_setup_baseline"))
        mode = body.get("mode", "manual")
        if mode not in ("auto", "manual"):
            raise ValueError("EDA setup mode must be auto or manual")
        manual = copy.deepcopy(body.get("manual") or {}) if mode == "manual" else {}
        cadence = self.config.get("cadence", {})
        with self.lock:
            optimization_active = self.optimization_launches or any(
                thread.is_alive() for thread in self.threads.values()
            )
        if optimization_active:
            raise RuntimeError("EDA settings cannot change while an optimization is active")
        file_overrides = cadence.get("_file_overrides") or {}
        configuration_base = deep_merge(DEFAULTS["cadence"], file_overrides)
        discovery_manual = (
            deep_merge(manual, file_overrides) if mode == "manual" else manual
        )
        context = self.config.get("_maestro_launch_context")
        selected_path = self.config.get("_selected_setup_path")
        if selected_path and self.invoking_context:
            # A different saved setup still uses the original menu toolchain;
            # it only replaces the design mapping and has no live-test binding.
            context = self.invoking_context
        bound_config = None
        if context:
            from .maestro_context import bind_launch_config
            unknown = sorted(set(manual) - set(MANUAL_KEYS))
            if unknown:
                raise ValueError("unsupported EDA override fields: %s" % ", ".join(unknown))
            requested_cadence = deep_merge(configuration_base, discovery_manual)
            proposed = copy.deepcopy(self.config)
            proposed_cadence = proposed["cadence"]
            for key in MANUAL_KEYS:
                proposed_cadence[key] = copy.deepcopy(requested_cadence.get(key))
            proposed_cadence["_manual_overrides"] = copy.deepcopy(manual)
            bound_config = bind_launch_config(proposed, context)
            if selected_path:
                from .maestro_context import selected_setup_config
                bound_config = selected_setup_config(
                    bound_config, proposed, resolve_setup_locator(selected_path),
                )
            bound_cadence = bound_config["cadence"]
            native_discovery = bound_cadence.get("discovery") or {}
            environment = restore_native_environment(
                os.environ, bound_cadence.get("launch_environment"),
            )
            discovery = discover_eda_environment(
                bound_cadence, validate=True, environ=environment,
            )
            discovery["sources"] = deep_merge(
                discovery.get("sources") or {}, native_discovery.get("sources") or {},
            )
            recommendations = list(discovery.get("recommendations") or [])
            for recommendation in native_discovery.get("recommendations") or []:
                if recommendation not in recommendations:
                    recommendations.append(recommendation)
            discovery["recommendations"] = recommendations
            if not discovery.get("ok"):
                failures = list(discovery.get("missing") or [])
                failures.extend(row["name"] for row in discovery.get("version_checks", []) if not row.get("ok"))
                raise ValueError("Invoking Maestro environment validation failed: %s. %s" % (
                    ", ".join(sorted(set(failures))) or "unknown check",
                    " ".join(discovery.get("recommendations") or []),
                ))
            bound_cadence.update(discovery["effective"])
            bound_cadence["discovery"] = discovery
            config_path = bound_cadence["user_config_path"]
        else:
            discovery, config_path = configure_eda(
                manual=discovery_manual,
                mode=mode,
                path=cadence.get("user_config_path"),
                validate=True,
                base_cadence=configuration_base,
                persist=False,
            )
        self._cancel_and_wait_range_validations(action="save EDA settings")
        self._close_resident_validation()
        config_path = persist_eda_setup(
            mode, manual, discovery, path=config_path,
        )
        if bound_config is not None:
            bound_config["cadence"]["user_config_path"] = config_path
        else:
            updated_cadence = copy.deepcopy(cadence)
            for key, value in discovery["effective"].items():
                updated_cadence[key] = value
            updated_cadence["setup_mode"] = mode
            updated_cadence["discovery"] = discovery
            updated_cadence["user_config_path"] = config_path
            updated_cadence["_manual_overrides"] = copy.deepcopy(manual)
        with self.lock:
            if bound_config is not None:
                self.config = bound_config
            else:
                self.config["cadence"] = updated_cadence
            if baseline_binding:
                self.config["_setup_baseline"] = baseline_binding
            self.prepared.clear()
            self.inspections.clear()
            self.test_inspections.clear()
            self.inspection_epoch += 1
        payload = self.eda_setup()
        payload["prepared_invalidated"] = True
        return payload

    @staticmethod
    def _require_prelaunch_safety(prepared, evaluator):
        evaluator = str(evaluator or "real").strip().lower()
        if evaluator == "mock":
            return
        validation_capability = (prepared or {}).get("resident_validation") or {}
        if validation_capability.get("unsafe_to_optimize"):
            raise RuntimeError(
                "optional PDK callback validation failed and Cadence cleanup "
                "could not be proven safe. Reload the setup before starting "
                "real EDA optimization: %s" % (
                    validation_capability.get("message") or "cleanup unproven"
                )
            )
        workspace_preflight = (prepared or {}).get("workspace_preflight") or {}
        if workspace_preflight.get("state") == "failed":
            raise RuntimeError(
                "the working optimization workspace cannot be built before "
                "Cadence or Spectre starts. Fix source readability and then Load "
                "and Inspect again; no candidate was launched: %s"
                % (workspace_preflight.get("message") or "workspace build failed")
            )

    def start(self, prepared_id, spec):
        self._recover_optimized_view_cleanup()
        self._begin_lifecycle_operation("start an optimization")
        try:
            return self._start_registered(prepared_id, spec)
        finally:
            self._end_lifecycle_operation()

    def _start_registered(self, prepared_id, spec):
        with self.lock:
            prepared = self.prepared.get(prepared_id)
        if not prepared:
            raise ValueError("prepared setup not found; load the ADE setup again")
        selected_test = self._require_single_test(prepared)
        spec = dict(spec)
        if spec.get("selected_test") not in (None, selected_test):
            raise ValueError("Requested Test differs from prepared Test.")
        spec["selected_test"] = selected_test
        evaluator = str(spec.get("evaluator", "real")).strip().lower()
        if evaluator == "real":
            require_unique_run_variables(prepared.get("setup") or {},
                                         action="start optimization", multiplicity_only=True)
        self._validate_live_test_bindings(prepared.get("setup"))
        require_prepared_freshness(prepared, self.config, action="start optimization")
        self._require_prelaunch_safety(prepared, evaluator)
        range_report = validate_variable_request_report(
            prepared, spec.get("variables") or [],
        )
        if not range_report["valid"]:
            raise VariableRangeValidationError(range_report)
        missing_bindings = (
            resolve_metric_bindings(
                prepared, spec.get("objectives") or [], spec.get("constraints") or [],
            )
            if evaluator != "mock" else {"outputs": [], "corners": []}
        )
        if missing_bindings["outputs"] or missing_bindings["corners"]:
            details = []
            if missing_bindings["outputs"]:
                details.append("outputs: %s" % ", ".join(missing_bindings["outputs"]))
            if missing_bindings["corners"]:
                details.append("corners: %s" % ", ".join(missing_bindings["corners"]))
            raise ValueError(
                "optimization metric bindings are missing from the prepared setup (%s); "
                "reload the design configuration and repair Objectives & Constraints"
                % "; ".join(details)
            )
        # Qualify each metric against the selected Test's proven source mask
        # before registering any worker; the native capture repeats this check.
        from .corner_selection import selection_plan as corner_selection_plan
        corner_selection_plan(prepared.get("setup") or {}, spec)
        spec.setdefault("setup_locator", prepared.get("setup_locator"))
        if prepared.get("maestro_path"):
            spec.setdefault("maestro_path", prepared.get("maestro_path"))
        return self._launch(spec, prepared)

    def validate_static_ranges(self, prepared_id, variables):
        with self.lock:
            prepared = self.prepared.get(prepared_id)
        if not prepared:
            raise ValueError("prepared setup not found; load the ADE setup again")
        selected_test = self._require_single_test(prepared)
        # Pure preview from the inspected catalogue. The real launch and native
        # callback validation each retain their own full source freshness check.
        return validate_variable_request_report(prepared, variables)

    def validate_ranges(self, prepared_id, variables):
        self._begin_lifecycle_operation("validate parameter ranges")
        try:
            return self._validate_ranges_registered(prepared_id, variables)
        finally:
            self._end_lifecycle_operation()

    def _validate_ranges_registered(self, prepared_id, variables):
        self._require_idle_setup_resources("validate parameter ranges")
        with self.lock:
            prepared = self.prepared.get(prepared_id)
        if not prepared:
            raise ValueError("prepared setup not found; load the ADE setup again")
        selected_test = self._require_single_test(prepared)
        if prepared.get("setup_private_session"):
            with self.lock:
                active = self.optimization_launches or any(thread.is_alive() for thread in self.threads.values())
            if active:
                raise RuntimeError("Range validation shares the working setup views; stop optimization before validating ranges.")
        require_unique_run_variables(prepared.get("setup") or {},
                                     action="validate CDF parameter ranges", multiplicity_only=True)
        self._validate_live_test_bindings(prepared.get("setup"))
        variables = validate_variable_request(prepared, variables)
        capability = prepared.get("resident_validation") or {}
        if capability.get("state") == "unavailable":
            return self._record_optional_range_validation_unavailable(
                prepared_id, variables, capability.get("message"),
                unsafe_to_optimize=capability.get("unsafe_to_optimize"),
                failure_stage=capability.get("failure_stage"),
                resident_cleanup=capability.get("resident_cleanup"),
            )
        timeout_seconds = float(self.config.get("runtime", {}).get(
            "range_validation_timeout_seconds", 120,
        ))
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError("range_validation_timeout_seconds must be positive")
        job_id = "range_validation_%s" % uuid.uuid4().hex[:12]
        job_root = os.path.join(self.config["runtime"]["run_root"], "range_validations", job_id)
        cancel_event = threading.Event()
        status_path = os.path.join(job_root, _contract_text("status.json"))
        job = {
            "schema_version": _contract_text("crescendo.range_validation.v1"),
            "job_id": job_id,
            "job_root": job_root,
            "prepared_id": prepared_id,
            "state": "queued",
            "message": "queued",
            "created_at": utc_timestamp(),
            "active_endpoint": None,
            "parameter_count": len(variables),
            "results": [],
            "optional": True, "blocking": False,
            "resource_scope": "isolated_optional_validation",
            "timeout_seconds": timeout_seconds,
            "detail_path": status_path,
        }
        if prepared.get("setup_private_session"):
            job["resource_scope"] = "setup_private_session"
            job["setup_private_session_id"] = prepared["setup_private_session"]["session_id"]
        runtime_lease = None
        timed_out = threading.Event()

        def persist(current):
            try:
                atomic_write_json(status_path, current)
            except Exception as error:
                current["diagnostic_write_error"] = "%s: %s" % (error.__class__.__name__, error)

        def update(**changes):
            with self.lock:
                current = dict(self.range_validations[job_id])
                if current.get("state") in _TERMINAL_RANGE_VALIDATION_STATES:
                    # A deadline is a UI outcome, never termination evidence.
                    # Preserve a later native cause/cleanup result without
                    # turning an expired job back into running/completed.
                    for key in ("traceback", "failure_stage", "cleanup_error",
                                "unsafe_cleanup_scope", "unsafe_cleanup_session_token", "cleanup_root"):
                        if changes.get(key):
                            current[key] = str(changes[key])[-16384:]
                    if changes.get("detail"):
                        detail = str(changes["detail"])[-16384:]
                        current["detail"] = ((str(current.get("detail") or "") + "\n" + detail)
                                             if current.get("timed_out") else detail)[-16384:]
                    if changes.get("unsafe_to_optimize"):
                        current["unsafe_to_optimize"] = True
                    self.range_validations[job_id] = current
                    persist(current)
                    return
                if changes.get("state") == "failed" and not str(changes.get("message") or "").startswith("Optional"):
                    changes = dict(changes)
                    changes.setdefault("detail", changes.get("message"))
                    changes["message"] = "Optional PDK validation skipped; optimization remains available."
                requested_state = changes.get("state")
                if cancel_event.is_set() and requested_state in ("queued", "running", "completed"):
                    changes = dict(changes)
                    changes.update({
                        "state": "cancelled",
                        "message": "Optional PDK validation cancelled; optimization remains available.",
                        "active_endpoint": None,
                        "finished_at": changes.get("finished_at") or utc_timestamp(),
                    })
                current.update(changes)
                current["updated_at"] = utc_timestamp()
                self.range_validations[job_id] = current
                persist(current)

        def expire():
            with self.lock:
                if (self.range_validations.get(job_id) or {}).get("state") in _TERMINAL_RANGE_VALIDATION_STATES:
                    return
            timed_out.set()
            update(
                state="failed", timed_out=True, active_endpoint=None,
                message="Optional PDK validation timed out; optimization remains available.",
                detail="The %.3g second total deadline includes workspace preparation, Cadence startup and both callback endpoints."
                       % timeout_seconds,
                finished_at=utc_timestamp(),
            )
            cancel_event.set()

        def target():
            resident_session = None
            timer = threading.Timer(timeout_seconds, expire)
            timer.daemon = True
            deadline = time.monotonic() + timeout_seconds
            timer.start()
            try:
                with command_cancellation_scope(cancel_event):
                    update(state="running", stage="workspace_preparation",
                           message="Preparing optional PDK validation workspace.")
                    require_prepared_freshness(prepared, self.config, action="validate parameter ranges")
                    check_command_cancellation()
                    update(stage="native_netlist_generation", message="Validating endpoints with native Maestro Run.")
                    run_range_validation(job, prepared, variables, self.config, cancel_event, update)
            except Exception as error:
                error_identity = getattr(error, "resident_cleanup", None)
                with self.lock:
                    retained = self.resident_validation
                identity = self._owned_resident_cleanup_identity(retained)
                if (isinstance(error_identity, dict) and identity is not None
                        and all(error_identity.get(key) == value for key, value in identity.items())):
                    self._bind_range_validation_cleanup(job_id, retained)
                update(
                    state="cancelled" if cancel_event.is_set() and not timed_out.is_set() else "failed",
                    message=("Optional PDK validation cancelled; optimization remains available."
                             if cancel_event.is_set() and not timed_out.is_set() else
                             "Optional PDK validation skipped; optimization remains available."),
                    detail="%s: %s" % (error.__class__.__name__, error),
                    traceback=traceback.format_exc(), active_endpoint=None,
                    failure_stage="workspace_build" if isinstance(error, WorkspaceBuildFailure) else "validation_startup",
                    unsafe_to_optimize=bool(getattr(error, "unsafe_to_optimize", False)
                                            or getattr(error, "cleanup_failed", False)),
                    unsafe_cleanup_scope=getattr(error, "unsafe_cleanup_scope", None),
                    unsafe_cleanup_session_token=getattr(error, "unsafe_cleanup_session_token", None),
                    finished_at=utc_timestamp(),
                )
            finally:
                timer.cancel()
                if resident_session is not None:
                    try:
                        cleanup = resident_session.close(timeout_seconds=min(5.0, float(self.config.get("runtime", {}).get(
                            "range_validation_shutdown_timeout_seconds", 15,
                        ))))
                        self._record_resident_cleanup_proven(resident_session, cleanup)
                        with self.lock:
                            if self.resident_validation is resident_session:
                                self.resident_validation = None
                    except Exception as error:
                        with self.lock:
                            current = dict(self.range_validations.get(job_id) or job)
                            prior_unsafe = bool(current.get("unsafe_to_optimize"))
                            current["unsafe_to_optimize"] = True
                            current["cleanup_error"] = "%s: %s" % (error.__class__.__name__, error)
                            identity = self._owned_resident_cleanup_identity(resident_session)
                            if (identity is not None and (not prior_unsafe or
                                    current.get("unsafe_cleanup_scope") == "resident_validation")):
                                current["unsafe_cleanup_scope"] = "resident_validation"
                                current["unsafe_cleanup_session_token"] = identity["session_token"]
                            self.range_validations[job_id] = current
                            persist(current)
                try:
                    with self.lock:
                        current = dict(self.range_validations.get(job_id) or job)
                        if current.get("state") not in _TERMINAL_RANGE_VALIDATION_STATES:
                            current.update({
                                "state": "cancelled" if cancel_event.is_set() else "failed",
                                "message": (
                                    "Optional PDK validation cancelled; optimization remains available." if cancel_event.is_set()
                                    else "range validation thread exited without a terminal result"
                                ),
                                "active_endpoint": None,
                                "finished_at": utc_timestamp(),
                                "updated_at": utc_timestamp(),
                            })
                            self.range_validations[job_id] = current
                            persist(current)
                finally:
                    if runtime_lease is not None:
                        runtime_lease.release()

        thread = threading.Thread(target=target, name=job_id)
        thread.daemon = True
        with self.lock:
            if not self.prepared.get(prepared_id):
                raise ValueError("prepared setup not found; load Maestro again")
            validation_active = any(
                status.get("state") not in _TERMINAL_RANGE_VALIDATION_STATES
                for status in self.range_validations.values()
            ) or any(thread.is_alive() for thread in self.range_validation_threads.values())
            if validation_active:
                raise RuntimeError("a range validation job is already running")
            try:
                runtime_lease = ContinuationLease(
                    self.config["runtime"]["run_root"], "optional-pdk-callback-validation-v1", run_root=job_root,
                    lease_kind="optional_pdk_callback_validation",
                ).acquire()
            except ContinuationLeaseBusy as error:
                raise ContinuationLeaseBusy(
                    "another optional PDK validation is already using this runtime_root: %s" % error
                )
            try:
                ensure_dir(job_root)
                self.range_validations[job_id] = job
                self.range_validation_cancels[job_id] = cancel_event
                self.range_validation_threads[job_id] = thread
                self.range_validation_leases[job_id] = runtime_lease
                atomic_write_json(status_path, job)
                thread.start()
            except Exception as error:
                try:
                    if job_id in self.range_validations:
                        failed = dict(job)
                        failed.update({
                            "state": "failed", "message": "%s: %s" % (error.__class__.__name__, error),
                            "finished_at": utc_timestamp(), "updated_at": utc_timestamp(),
                        })
                        self.range_validations[job_id] = failed
                        try:
                            atomic_write_json(status_path, failed)
                        except Exception:
                            pass
                finally:
                    self.range_validation_cancels.pop(job_id, None)
                    self.range_validation_threads.pop(job_id, None)
                    self.range_validation_leases.pop(job_id, None)
                    runtime_lease.release()
                raise
        return {"job_id": job_id, "state": "queued"}

    def exit_optimizer(self):
        with self.exit_lock:
            return self._exit_optimizer_serialized()

    def request_force_exit(self):
        from .ui_exit import request_detach
        request_detach(self)

    def force_exit_optimizer(self, reason="User forced UI exit"):
        from .ui_exit import detach_service
        return detach_service(self, reason)

    def _retry_session_run_cleanup(self, deadline):
        """Retry proven ownership cleanup for this UI's finished runs only."""
        from .run_log import append_run_event

        with self.lock:
            names = sorted(self.session_run_names)
            if not names:
                return {"state": "no_pending_cleanup", "runs": []}
            if any(thread.is_alive() for thread in self.threads.values()):
                raise RuntimeError("optimizer cleanup retry requires all run threads to finish")
        pending = []
        for name in names:
            status = self._read_run_json_snapshot(
                name, _contract_text("status.json"), "run status", _RUN_STATUS_SNAPSHOT_MAX_BYTES, missing_ok=False,
            )
            shutdown = self._read_shutdown_evidence(name, "worker shutdown", missing_ok=True)
            from .shutdown_evidence import shutdown_proven
            failed = shutdown is not None and not shutdown_proven(shutdown)
            run_root = self._validate_run_status_identity(name, status)
            # A startup may fail before the shutdown audit is registered.
            # Presence only requests strict recovery; it never proves ownership.
            retained_owned_namespace = os.path.lexists(os.path.join(run_root, ".cleanup_pending"))
            if shutdown is None:
                for namespace in ("workers", "controllers"):
                    path = os.path.join(run_root, namespace)
                    if os.path.lexists(path) and (
                            os.path.islink(path) or not os.path.isdir(path) or os.listdir(path)):
                        retained_owned_namespace = True
            if failed or retained_owned_namespace or (status.get("cleanup_failure") or {}).get("recovery_required"):
                pending.append((name, status))
        if not pending:
            return {"state": "no_pending_cleanup", "runs": []}
        leases = []
        recovered = []
        try:
            for root in sorted(set(self._runtime_for_run(name) for name, _status in pending)):
                leases.append(RuntimeOptimizationLease(root).acquire())
            for name, status in pending:
                if time.monotonic() >= deadline:
                    raise RuntimeError("optimizer exit timed out before owned cleanup retry for %s" % name)
                run_root = self._validate_run_status_identity(name, status)
                previous = status.get("exit_cleanup") or {}
                attempt = int(previous.get("attempt_count", 0)) + 1
                try:
                    snapshot = self._resolve_run_checkpoint_snapshot(name, status=status)
                    if snapshot is None:
                        raise RuntimeError("owned cleanup retry has no verified checkpoint; evidence retained for %s" % name)
                    checkpoint = snapshot["checkpoint"]
                    if (checkpoint.get("manifest") or {}).get("origin_run_id") != name:
                        raise RuntimeError("owned cleanup retry checkpoint does not belong to this session run")
                    proof = self._cleanup_checkpoint_origin(
                        checkpoint, terminate_owned=True, inspect_only=False,
                    )
                    if (proof.get("state") != "completed" or proof.get("retained_controllers")
                            or proof.get("skipped_workers") or proof.get("skipped")):
                        raise RuntimeError("owned cleanup retry remains unproven: %s" % proof)
                    audit = {"schema_version": _contract_text("crescendo.exit_cleanup.v1"), "state": "completed",
                             "attempt_count": attempt, "finished_at": utc_timestamp(),
                             "process_termination_proven": True, "remote_stop_proven": True,
                             "artifact_cleanup_complete": True, "proof": proof}
                    history = list(previous.get("worker_cleanup_history") or [])
                    known_epochs = set(row.get("worker_epoch") for row in history)
                    for row in proof.get("worker_cleanup_evidence") or []:
                        if row.get("worker_epoch") not in known_epochs:
                            history.append(row)
                            known_epochs.add(row.get("worker_epoch"))
                    if history:
                        # A repeated successful Retry must not discard the
                        # exact lock proof when its worker epoch is now absent.
                        audit["worker_cleanup_history"] = history
                    status["exit_cleanup"] = audit
                    if isinstance(status.get("cleanup_failure"), dict):
                        status["cleanup_failure"]["recovery_required"] = False
                    atomic_write_json(os.path.join(run_root, _contract_text("status.json")), status)
                    append_run_event(run_root, "exit_cleanup_recovered",
                                     "Owned processes/jobs stopped and remaining artifacts cleaned", audit)
                    recovered.append({"run_id": name, "state": "completed", "attempt_count": attempt})
                except Exception as error:
                    audit = {"schema_version": _contract_text("crescendo.exit_cleanup.v1"), "state": "failed",
                             "attempt_count": attempt, "finished_at": utc_timestamp(),
                             "artifact_cleanup_complete": False,
                             "error": "%s: %s" % (error.__class__.__name__, error)}
                    status["exit_cleanup"] = audit
                    try:
                        atomic_write_json(os.path.join(run_root, _contract_text("status.json")), status)
                    except Exception:
                        pass
                    append_run_event(run_root, "exit_cleanup_failed", audit["error"], audit, level="ERROR")
                    raise RuntimeError("optimizer exit cleanup remains blocked for %s: %s" % (name, audit["error"]))
        finally:
            for lease in reversed(leases):
                lease.release()
        return {"state": "completed", "runs": recovered}

    def _exit_optimizer_serialized(self):
        try:
            timeout_seconds = float(self.config.get("runtime", {}).get(
                "optimizer_exit_timeout_seconds", 60,
            ))
        except (TypeError, ValueError, OverflowError):
            raise ValueError("optimizer_exit_timeout_seconds must be a finite non-negative number")
        if not math.isfinite(timeout_seconds) or timeout_seconds < 0:
            raise ValueError("optimizer_exit_timeout_seconds must be a finite non-negative number")
        deadline = time.monotonic() + timeout_seconds
        with self.lifecycle_condition:
            self.closing = True
            # Signal first: a prepare may be waiting on the native startup lock
            # or on its own read-only Cadence process, rather than registering.
            self.prepare_cancel_event.set()
            for event in self.range_validation_cancels.values():
                event.set()
            active_runs = sorted(
                run_name for run_name, thread in self.threads.items() if thread.is_alive()
            )
        # Test cancellation precedes waiting, including any late native launch.
        environment_tests = getattr(self, "environment_tests", None)
        if environment_tests is not None:
            environment_tests.cancel_all()
        # Stop existing runs before waiting on unrelated optional validation or
        # probe cleanup. A cleanup timeout must never leave optimization running.
        for run_name in active_runs:
            try:
                self.stop(run_name)
            except ValueError:
                with self.lock:
                    thread = self.threads.get(run_name)
                    still_alive = bool(thread and thread.is_alive())
                if still_alive:
                    raise
        with self.lifecycle_condition:
            while self.lifecycle_operations:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise RuntimeError(
                        "optimizer exit timed out waiting for %d resource operation(s) "
                        "to finish registration" % self.lifecycle_operations
                    )
                self.lifecycle_condition.wait(timeout=remaining)
        self._recover_optimized_view_cleanup(
            timeout_seconds=min(12.0, max(0.0, deadline - time.monotonic())),
        )
        with self.lifecycle_condition:
            if self.prepare_cleanup_failures:
                raise RuntimeError(
                    "optimizer exit could not prove read-only probe cleanup: %s"
                    % "; ".join(self.prepare_cleanup_failures)
                )
        with self.lock:
            newly_registered_runs = sorted(
                run_name for run_name, thread in self.threads.items()
                if run_name not in active_runs and thread.is_alive()
            )
        for run_name in newly_registered_runs:
            try:
                self.stop(run_name)
            except ValueError:
                with self.lock:
                    thread = self.threads.get(run_name)
                    still_alive = bool(thread and thread.is_alive())
                if still_alive:
                    raise
        active_runs = sorted(set(active_runs + newly_registered_runs))
        self._cancel_and_wait_range_validations(
            timeout_seconds=max(0.0, deadline - time.monotonic()),
            action="exit optimizer",
        )
        for run_name in active_runs:
            with self.lock:
                thread = self.threads.get(run_name)
            if thread and thread.is_alive():
                thread.join(timeout=max(0.0, deadline - time.monotonic()))
        with self.lock:
            still_active = sorted(
                run_name for run_name, thread in self.threads.items() if thread.is_alive()
            )
        if still_active:
            raise RuntimeError("optimizer exit timed out waiting for active runs: %s" % still_active)
        if environment_tests is not None and not environment_tests.join(max(0.0, deadline - time.monotonic())):
            raise RuntimeError("optimizer exit timed out waiting for environment-test cancellation")
        self._close_resident_validation(
            timeout_seconds=max(0.0, deadline - time.monotonic()),
        )
        run_cleanup = self._retry_session_run_cleanup(deadline)
        setup_private_cleanup = self._close_setup_private_sessions(deadline)
        retained_cleanup = self._clear_retained_logs(session_only=True)
        scratch_cleanup = self._cleanup_session_scratch()
        self._invalidate_objective_presentation()
        return {
            "exiting": True, "stopped_runs": active_runs,
            "validation_session_closed": True,
            "retained_log_cleanup": retained_cleanup,
            "run_cleanup_retry": run_cleanup,
            "setup_private_cleanup": setup_private_cleanup,
            "warnings": setup_private_cleanup.get("warnings") or [],
            "scratch_cleanup": scratch_cleanup,
        }

    def request_http_shutdown(self):
        callback = self.shutdown_callback
        if callback:
            threading.Thread(
                target=callback, name="optimizer-http-shutdown", daemon=True,
            ).start()

    def range_validation_status(self, job_id):
        safe_id = os.path.basename(job_id)
        if safe_id != job_id or not safe_id.startswith("range_validation_"):
            raise ValueError("invalid range validation job id")
        with self.lock:
            status = self.range_validations.get(job_id)
        if status:
            return dict(status)
        path = os.path.join(
            self.config["runtime"]["run_root"], "range_validations", safe_id, _contract_text("status.json"),
        )
        return load_json(path) if os.path.isfile(path) else None

    def cancel_range_validations(self, prepared_id=None):
        cancelled = []
        with self.lock:
            for job_id, event in self.range_validation_cancels.items():
                status = self.range_validations.get(job_id) or {}
                thread = self.range_validation_threads.get(job_id)
                if status.get("state") in _TERMINAL_RANGE_VALIDATION_STATES and not (
                    thread and thread.is_alive()
                ):
                    continue
                if prepared_id is not None and status.get("prepared_id") != prepared_id:
                    continue
                event.set()
                if status.get("state") not in _TERMINAL_RANGE_VALIDATION_STATES:
                    status = dict(status)
                    status.update({
                        "state": "cancelling",
                        "message": "Cancelling optional PDK validation.",
                        "updated_at": utc_timestamp(),
                    })
                    self.range_validations[job_id] = status
                    try:
                        atomic_write_json(os.path.join(status["job_root"], _contract_text("status.json")), status)
                    except Exception as error:
                        status["diagnostic_write_error"] = "%s: %s" % (error.__class__.__name__, error)
                cancelled.append(job_id)
        return cancelled

    def _retry_range_validation_resident_cleanup(self, job_ids, deadline):
        """Retry a retained exact session only after its callback thread exits."""
        sessions = []
        with self.lock:
            for job_id in job_ids:
                current = self.range_validations.get(job_id) or {}
                thread = self.range_validation_threads.get(job_id)
                binding = self.range_validation_cleanup_sessions.get(job_id) or {}
                session, identity = binding.get("session"), binding.get("identity")
                if (not current.get("unsafe_to_optimize")
                        or current.get("state") not in _TERMINAL_RANGE_VALIDATION_STATES
                        or (thread is not None and thread.is_alive())
                        or current.get("unsafe_cleanup_scope") != "resident_validation"
                        or not identity
                        or str(current.get("prepared_id")) != identity.get("prepared_id")
                        or os.path.realpath(current.get("job_root") or "") != os.path.join(
                            identity.get("runtime_root", ""), "range_validations", job_id)
                        or current.get("unsafe_cleanup_session_token") != identity.get("session_token")
                        or any((current.get("resident_cleanup") or {}).get(key) != value
                               for key, value in identity.items())
                        or self._owned_resident_cleanup_identity(session) != identity
                        or (self.resident_validation is not session and not session.closed)):
                    continue
                if not any(item is session for item in sessions):
                    sessions.append(session)
        for session in sessions:
            remaining = max(0.0, deadline - time.monotonic())
            if remaining <= 0:
                return
            try:
                result = session.close(timeout_seconds=remaining)
            except Exception as error:
                with self.lock:
                    for job_id in job_ids:
                        if (self.range_validation_cleanup_sessions.get(job_id) or {}).get("session") is session:
                            current = self.range_validations.get(job_id) or {}
                            current["cleanup_retry_error"] = "%s: %s" % (error.__class__.__name__, error)
                            try:
                                atomic_write_json(os.path.join(current["job_root"], _contract_text("status.json")), current)
                            except Exception as write_error:
                                current["diagnostic_write_error"] = "%s: %s" % (write_error.__class__.__name__, write_error)
                continue
            self._record_resident_cleanup_proven(session, result)
            if (isinstance(result, dict) and result.get("cleanup_proven") is True
                    and session.closed is True):
                with self.lock:
                    if self.resident_validation is session:
                        self.resident_validation = None

    def _retry_range_validation_native_cleanup(self, job_ids, deadline):
        from .private_netlist import reconcile_preparation
        for job_id in job_ids:
            with self.lock:
                job = dict(self.range_validations.get(job_id) or {})
                thread = self.range_validation_threads.get(job_id)
                cancel = self.range_validation_cancels.get(job_id)
            if (not job.get("unsafe_to_optimize") or
                    job.get("unsafe_cleanup_scope") != "native_range_capture" or
                    thread is None or cancel is None or thread.is_alive()):
                continue
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            root = job.get("cleanup_root")
            allowed = [os.path.join(os.path.realpath(job.get("job_root") or ""), name)
                       for name in ("minimum", "maximum")]
            if not root or os.path.realpath(root) not in allowed:
                continue
            try:
                proof = reconcile_preparation(root, request_cancel=True, timeout_seconds=remaining)
                with self.lock:
                    current = dict(self.range_validations.get(job_id) or {})
                    current["cleanup_retry"] = proof
                    if proof.get("cleanup_proven"):
                        current["unsafe_to_optimize"] = False
                        current["cleanup_error"] = None
                        current["cleanup_retry_error"] = None
                    self.range_validations[job_id] = current
                    atomic_write_json(os.path.join(current["job_root"], _contract_text("status.json")), current)
            except Exception as error:
                with self.lock:
                    self.range_validations[job_id]["cleanup_retry_error"] = str(error)

    def _cancel_and_wait_range_validations(self, prepared_id=None,
                                           timeout_seconds=None,
                                           action="start optimization"):
        targets = []
        missing_threads = []
        with self.lock:
            job_ids = set(
                job_id for job_id, status in self.range_validations.items()
                if (status.get("state") not in _TERMINAL_RANGE_VALIDATION_STATES or
                    status.get("unsafe_to_optimize")) and
                (prepared_id is None or status.get("prepared_id") == prepared_id)
            )
            job_ids.update(
                job_id for job_id, validation_thread in self.range_validation_threads.items()
                if validation_thread.is_alive() and (
                    prepared_id is None or
                    (self.range_validations.get(job_id) or {}).get("prepared_id") == prepared_id
                )
            )
            if not job_ids:
                return []
            for job_id in sorted(job_ids):
                status = dict(self.range_validations.get(job_id) or {})
                event = self.range_validation_cancels.get(job_id)
                validation_thread = self.range_validation_threads.get(job_id)
                if event is None or validation_thread is None:
                    if event is not None:
                        event.set()
                    continue
                event.set()
                if status.get("state") not in _TERMINAL_RANGE_VALIDATION_STATES:
                    status.update({
                        "state": "cancelling",
                        "message": "%s requested; waiting for range validation to exit" % action,
                        "updated_at": utc_timestamp(),
                    })
                    self.range_validations[job_id] = status
                    atomic_write_json(os.path.join(status["job_root"], _contract_text("status.json")), status)
                targets.append((job_id, validation_thread))

        if timeout_seconds is None:
            try:
                timeout_seconds = float(self.config.get("runtime", {}).get(
                    "range_validation_shutdown_timeout_seconds", 30.0,
                ))
            except (TypeError, ValueError):
                timeout_seconds = 30.0
        else:
            timeout_seconds = float(timeout_seconds)
        timeout_seconds = max(0.0, timeout_seconds)
        started = time.monotonic()
        deadline = started + timeout_seconds
        for _job_id, validation_thread in targets:
            if not validation_thread.is_alive():
                continue
            validation_thread.join(timeout=max(0.0, deadline - time.monotonic()))

        self._retry_range_validation_resident_cleanup(sorted(job_ids), deadline)
        self._retry_range_validation_native_cleanup(sorted(job_ids), deadline)

        alive = []
        unsafe_results = []
        with self.lock:
            for job_id in sorted(job_ids):
                status = self.range_validations.get(job_id) or {}
                if (status.get("state") in _TERMINAL_RANGE_VALIDATION_STATES
                        and not status.get("unsafe_to_optimize")):
                    continue
                if (self.range_validation_cancels.get(job_id) is None
                        or self.range_validation_threads.get(job_id) is None):
                    missing_threads.append(job_id)
                    if status.get("unsafe_to_optimize"):
                        unsafe_results.append(job_id)
            for job_id, validation_thread in targets:
                if validation_thread.is_alive():
                    alive.append(job_id)
                    continue
                status = dict(self.range_validations.get(job_id) or {})
                if status.get("unsafe_to_optimize"):
                    unsafe_results.append(job_id)
                    continue
                if status.get("state") not in _TERMINAL_RANGE_VALIDATION_STATES:
                    status.update({
                        "state": "cancelled",
                        "message": "cancelled before optimization startup",
                        "active_endpoint": None,
                        "finished_at": utc_timestamp(),
                        "updated_at": utc_timestamp(),
                    })
                    self.range_validations[job_id] = status
                    atomic_write_json(os.path.join(status["job_root"], _contract_text("status.json")), status)
        unsafe = sorted(set(missing_threads + alive + unsafe_results))
        if unsafe:
            reasons = []
            if missing_threads:
                reasons.append("missing thread tracking: %s" % ", ".join(sorted(missing_threads)))
            if alive:
                reasons.append("threads still alive: %s" % ", ".join(sorted(alive)))
            if unsafe_results:
                reasons.append(
                    "Cadence process cleanup unproven: %s" % ", ".join(sorted(set(unsafe_results)))
                )
                with self.lock:
                    for job_id in sorted(set(unsafe_results)):
                        status = self.range_validations.get(job_id) or {}
                        detail = status.get("cleanup_retry_error") or status.get("cleanup_error")
                        if detail:
                            reasons.append("%s: %s" % (job_id, str(detail)[:512]))
            raise RuntimeError(
                "range validation cancellation was not proven safe; "
                "refusing to %s (%s). Cleanup budget %.3f seconds; elapsed %.3f seconds. "
                "This internal PDK validation cleanup budget is separate from simulation Run timeout."
                % (action, "; ".join(reasons), timeout_seconds,
                   max(0.0, time.monotonic() - started))
            )
        return [job_id for job_id, _validation_thread in targets]

    def _validation_resources_are_isolated(self):
        """Only legacy/shared validation resources require a handoff.

        New callback jobs have an actual, distinct POSIX lease and exact working
        workspace namespaces. The optimization still takes its own lease,
        checks prepared source freshness/storage, and builds/verifies a fresh
        worker workspace before creating any Maestro process. A required source
        copy failure therefore remains fatal in runner._WholeRunPersistentWorker.
        Unknown lease/session ownership retains the previous safety handoff.
        """
        runtime = os.path.realpath(os.path.abspath(self.config["runtime"]["run_root"]))
        optimization_lease_path = RuntimeOptimizationLease(runtime).path
        with self.lock:
            isolated = True
            for job_id, job in self.range_validations.items():
                thread = self.range_validation_threads.get(job_id)
                if (job.get("state") in _TERMINAL_RANGE_VALIDATION_STATES
                        and not job.get("unsafe_to_optimize") and not (thread and thread.is_alive())):
                    continue
                if (self.prepared.get(job.get("prepared_id")) or {}).get("setup_private_session"):
                    isolated = False
                    break
                lease = self.range_validation_leases.get(job_id)
                job_runtime = lease.runtime_root if isinstance(lease, ContinuationLease) else runtime
                expected_job = os.path.join(job_runtime, "range_validations", job_id)
                if not (
                        job.get("resource_scope") == "isolated_optional_validation"
                        and isinstance(lease, ContinuationLease)
                        and lease.lease_kind == "optional_pdk_callback_validation"
                        and lease.continuation_id == "optional-pdk-callback-validation-v1"
                        and lease.path != os.path.join(job_runtime, os.path.basename(optimization_lease_path))
                        and os.path.realpath(job.get("job_root") or "") == expected_job):
                    isolated = False
                    break
                session = self.resident_validation
                if session is not None:
                    expected_session = os.path.join(job_runtime, "range_validation_resident", "prepared_%s" % job["prepared_id"])
                    if not (
                            isinstance(session, ResidentValidationSession)
                            and os.path.realpath(session.session_root) == expected_session
                            and os.path.realpath(session.workspace_root) == os.path.join(expected_session, "workspace")
                            and session.prepared_id == job["prepared_id"]):
                        isolated = False
                        break
        return isolated

    def _wait_for_shared_validation_resources(self):
        if self._validation_resources_are_isolated():
            return []
        return self._cancel_and_wait_range_validations()

    def _require_session_runs_clean(self):
        """Check only this UI's owned terminal runs; no historical scavenging."""
        with self.lock:
            names = tuple(self.session_run_names)
        for name in names:
            status = self._read_run_json_snapshot(name, _contract_text("status.json"), "owned run status", _RUN_STATUS_SNAPSHOT_MAX_BYTES)
            if not isinstance(status, dict):
                continue
            failure = status.get("cleanup_failure") or {}
            shutdown = self._read_shutdown_evidence(name, "owned worker shutdown")
            from .shutdown_evidence import shutdown_proven
            failed_shutdown = shutdown is not None and not shutdown_proven(shutdown)
            if failure.get("recovery_required") or failed_shutdown:
                raise RuntimeError("Previous owned run cleanup remains incomplete: %s. Retry Exit before a new optimization." % name)

    def _launch(self, spec, prepared, checkpoint=None, continuation_lease=None,
                runtime_optimization_lease=None, run_config=None, tb_optimization_lease=None):
        self._validate_live_test_bindings((prepared or {}).get("setup"))
        run_config = run_config or self._freeze_run_config(spec, prepared, checkpoint=checkpoint)
        spec = dict(spec)
        display_name = _validated_run_display_name(spec.get("display_name"))
        if display_name is None:
            spec.pop("display_name", None)
        else:
            spec["display_name"] = display_name
        self._begin_lifecycle_operation("start an optimization")
        with self.lock:
            if any(thread and thread.is_alive() for thread in self.threads.values()):
                self.lifecycle_operations -= 1
                self.lifecycle_condition.notify_all()
                raise RuntimeError("An optimization is active; stop it before starting another run.")
            self.optimization_launches += 1
        owned_tb_lease = None
        try:
            if str(spec.get("evaluator", "real")).lower() != "mock" and tb_optimization_lease is None:
                owned_tb_lease = self._acquire_tb_lease(run_config)
                tb_optimization_lease = owned_tb_lease
            runtime_root = run_config["runtime"]["run_root"]
            startup_storage_preflight(
                os.path.join(runtime_root, ".optimization-startup-preflight"),
                runtime_root,
                run_config["runtime"].get("persistent_min_free_disk_mib", 1024),
                checkpoint=checkpoint, workspace_required=False,
            )
            self._wait_for_shared_validation_resources()
            self._require_session_runs_clean()
            real_eda = str(spec.get("evaluator", "real")).strip().lower() != "mock"
            runtime_lease = runtime_optimization_lease
            owns_runtime_lease = bool(real_eda and runtime_lease is None)
            if owns_runtime_lease:
                runtime_lease = RuntimeOptimizationLease(
                    run_config["runtime"]["run_root"],
                ).acquire()
            try:
                # Unrelated historical log housekeeping must not gate Start.
                # Owned process/artifact gates remain in the runtime lease.
                result = self._launch_registered(
                    spec, prepared, checkpoint=checkpoint,
                    continuation_lease=continuation_lease,
                    runtime_optimization_lease=runtime_lease, run_config=run_config,
                    tb_optimization_lease=tb_optimization_lease,
                )
                with self.lock:
                    self.session_run_names.add(result["run_id"])
                return result
            except Exception:
                if owns_runtime_lease:
                    runtime_lease.release()
                raise
        except Exception:
            if owned_tb_lease is not None:
                owned_tb_lease.release()
            raise
        finally:
            with self.lock:
                self.optimization_launches -= 1
            self._end_lifecycle_operation()

    def _launch_registered(self, spec, prepared, checkpoint=None, continuation_lease=None,
                           runtime_optimization_lease=None, run_config=None, tb_optimization_lease=None):
        run_config = run_config or self._freeze_run_config(spec, prepared, checkpoint=checkpoint)
        spec = dict(spec)
        execution_context = _configured_execution_context(prepared, spec)
        spec[_contract_text("cr_execution_context")] = execution_context
        display_name = _validated_run_display_name(spec.get("display_name"))
        if display_name is None:
            spec.pop("display_name", None)
        else:
            spec["display_name"] = display_name
        run_name = optimization_run_name(display_name)
        runtime_root = run_config["runtime"]["run_root"]
        run_root = os.path.join(runtime_root, run_name)
        startup_preflight = startup_storage_preflight(
            run_root, runtime_root,
            run_config["runtime"].get("persistent_min_free_disk_mib", 1024),
            setup=(prepared or {}).get("setup"),
            hierarchy=(prepared or {}).get("hierarchy"), config=run_config,
            checkpoint=checkpoint,
            workspace_required=optimization_workspace_required(spec, checkpoint),
            parameters=_selected_workspace_parameters(prepared, spec.get("variables")),
        )
        run_root = ensure_dir(run_root)
        index_warning = self._register_run_location(run_name, runtime_root, config=run_config)
        algorithm_parameters = dict(spec.get("algorithm_parameters") or {})
        atomic_write_json(os.path.join(run_root, _contract_text("status.json")), {
            "schema_version": _contract_text("crescendo.run_status.v1"),
            "run_id": run_name,
            "run_root": run_root,
            "display_name": display_name or run_name,
            "state": "queued",
            "started_at": utc_timestamp(),
            "generation": 0,
            "total_generations": int(spec.get("generations", 1)),
            "completed_candidates": 0,
            "failed_candidates": 0,
            "infrastructure_failed_candidates": 0,
            "total_candidates": int(spec.get("generations", 1)) * int(
                algorithm_parameters.get("population", spec.get("population", 4))
            ),
            "message": "queued",
            "algorithm": spec.get("algorithm"),
            "algorithm_parameters": algorithm_parameters,
            "startup_storage_preflight": startup_preflight,
            "execution_context": execution_context,
            "storage_policy": (run_config.get("runtime") or {}).get("storage_policy"),
            "index_warning": index_warning,
            **({"environment_test": spec["environment_test"]} if spec.get("environment_test") else {}),
        })

        def target():
            try:
                if checkpoint is None:
                    run_optimization(
                        spec, run_config, prepared=prepared, run_root=run_root,
                        runtime_optimization_lease=runtime_optimization_lease,
                    )
                else:
                    _run_coordinated_resume(
                        spec, run_config, prepared=prepared, run_root=run_root,
                        resume_checkpoint=checkpoint,
                        continuation_lease=continuation_lease,
                        runtime_optimization_lease=runtime_optimization_lease,
                    )
            except Exception as error:
                failure_traceback = traceback.format_exc()
                try:
                    atomic_write_json(
                        os.path.join(run_root, _contract_text("server_error.json")),
                        {"traceback": failure_traceback,
                         "error_type": error.__class__.__name__,
                         "message": str(error)[:8192],
                         "remote_traceback": getattr(error, "remote_traceback", None)[-65536:]
                         if isinstance(getattr(error, "remote_traceback", None), str) else None},
                    )
                except Exception:
                    pass
                status_path = os.path.join(run_root, _contract_text("status.json"))
                try:
                    failure_status = load_json(status_path) if os.path.isfile(status_path) else {}
                    if not isinstance(failure_status, dict):
                        failure_status = {}
                    primary = failure_status.get("primary_failure")
                    failure_message = (failure_status.get("message")
                                       if isinstance(primary, dict) else
                                       "%s: %s" % (error.__class__.__name__, error))
                    failure_status.setdefault("primary_failure", {
                        "error_type": error.__class__.__name__, "message": str(error),
                        "failure_stage": "optimization",
                    })
                    failure_status.update({
                        "schema_version": _contract_text("crescendo.run_status.v1"),
                        "run_id": run_name,
                        "run_root": run_root,
                        "state": "failed",
                        "message": failure_message,
                        "finished_at": utc_timestamp(),
                    })
                    atomic_write_json(status_path, failure_status)
                except Exception:
                    pass
            finally:
                self._invalidate_objective_presentation(run_name)
                if continuation_lease is not None:
                    continuation_lease.release()
                if runtime_optimization_lease is not None:
                    runtime_optimization_lease.release()
                if tb_optimization_lease is not None:
                    tb_optimization_lease.release()

        thread = threading.Thread(target=target, name=run_name)
        thread.daemon = True
        with self.lock:
            self.threads[run_name] = thread
        try:
            thread.start()
        except Exception:
            if continuation_lease is not None:
                continuation_lease.release()
            if runtime_optimization_lease is not None:
                runtime_optimization_lease.release()
            raise
        return {"run_id": run_name, "run_root": run_root}

    def _configuration_match_context(self, catalogue=None, inspection=None, path=None):
        """Build a read-only matcher context from freshly inspected identities."""
        catalogue, inspection = catalogue or {}, inspection or {}
        setup = catalogue.get("setup") or inspection.get("setup") or {}
        locator = catalogue.get("setup_locator") or inspection.get("setup_locator") or {}
        source = (self.config.get("_selected_setup_source") or
                  (self.config.get("_maestro_launch_context") or {}).get("source") or {})
        identity = dict((key, source.get(key) or locator.get(key + "_name"))
                        for key in ("library", "cell", "view"))
        corners = setup.get("corners") or []
        from .unified_configuration import maestro_settings_snapshot
        return {
            "setup": {"identity": identity, "designs": setup.get("tests") or [],
                      "tests": setup.get("tests") or [],
                      "backend": setup.get("backend") or locator.get("backend"),
                      "path": path or locator.get("path")},
            "parameter_by_id": dict((row["parameter_id"], row)
                for row in catalogue.get("parameters") or []
                if row.get("parameter_id") and row.get("optimizable")),
            "cell_scopes": inspection.get("cell_scopes") or [],
            "outputs": catalogue.get("outputs") or [],
            "corners": [str(row["name"]) for row in corners if row.get("name")] or (["_default"] if not corners else []),
            "maestro_snapshot": maestro_settings_snapshot(setup,
                catalogue.get("job_policy") or inspection.get("job_policy")),
        }

    def _restore_configuration_solver_overrides(self, document, warnings):
        """Restore usable solver preferences in memory, never source/startup state.

        A saved path is a portable preference, not installation qualification.
        Normal inspection and run checks remain responsible for native/model
        evidence. Unusable and environment-sensitive fields stay in the draft.
        """
        requested = document.get("environment_overrides") or {}
        binary_keys = ("spectre_bin", "alps_bin")
        directory_keys = ("spectre_root", "alps_root", "spectre_model_dir", "alps_model_dir")
        reader_keys = ("simulator_result_reader", "spectre_result_reader", "alps_result_reader")
        accepted = {}
        current = self.config.get("cadence") or {}
        current_manual = current.get("_manual_overrides") or {}
        file_overrides = current.get("_file_overrides") or {}
        for key, value in requested.items():
            if key == "simulator_threads":
                # UI drafts store text, project settings may store an integer.
                # Validate before equality: bool True must not equal CPU count 1.
                try:
                    number = int(value)
                    valid_number = (not isinstance(value, bool) and float(value) == number
                                    and 1 <= number <= 4096)
                except (TypeError, ValueError, OverflowError):
                    valid_number = False
                if not valid_number:
                    warnings.append("Saved simulator_threads is invalid; current value retained.")
                    continue
                value = number
            if ((key in current and current[key] == value) or
                    (key in current_manual and current_manual[key] == value)):
                continue
            if key in file_overrides and file_overrides[key] != value:
                warnings.append("Saved EDA setting %s differs from the current project override; current value retained." % key)
                continue
            if key in binary_keys + directory_keys:
                if not isinstance(value, str) or not value.strip() or "\x00" in value:
                    warnings.append("Saved EDA setting %s is incomplete; current value retained." % key)
                    continue
                expanded = os.path.expandvars(os.path.expanduser(value.strip()))
                valid = os.path.isabs(expanded) and "$" not in expanded
                if key in binary_keys:
                    valid = valid and os.path.isfile(expanded) and os.access(expanded, os.R_OK | os.X_OK)
                else:
                    valid = valid and os.path.isdir(expanded) and os.access(expanded, os.R_OK | os.X_OK)
                if not valid:
                    warnings.append("Saved EDA path %s is unavailable here; current value retained." % key)
                    continue
                accepted[key] = os.path.abspath(expanded)
            elif key == "simulator_threads":
                accepted[key] = value
            elif key in reader_keys:
                if value in ("spectre", "alps"):
                    accepted[key] = value
                else:
                    warnings.append("Saved result reader %s is unknown; current value retained." % key)
            elif key == "simulator" and value in ("spectre", "alps"):
                accepted[key] = value
            else:
                warnings.append("Saved EDA setting %s requires the current environment; retained as a draft." % key)
        if accepted:
            with self.lock:
                cadence = self.config.setdefault("cadence", {})
                cadence.update(copy.deepcopy(accepted))
                manual = copy.deepcopy(cadence.get("_manual_overrides") or {})
                manual.update(copy.deepcopy(accepted))
                cadence["_manual_overrides"] = manual
                sources = cadence.setdefault("discovery", {}).setdefault("sources", {})
                sources.update(dict((key, "configuration:manual") for key in accepted))
        return accepted

    def load_configuration(self, document, path=None, max_depth=32, selected_scope_ids=None, selected_test=None):
        """Import one draft, then safely Load and Apply every matching cell.

        Import mismatches never grant native authority and never discard the
        draft. Setup failures return a partial import; cancellation and unsafe
        cleanup still raise with that same partial result attached.
        """
        from .unified_configuration import (
            match_configuration, match_configuration_cells, normalize_configuration,
        )
        normalized = normalize_configuration(document)
        result = {"normalized": normalized, "document": normalized["document"],
                  "settings": normalized["settings"], "source": normalized["source"],
                  "warnings": list(normalized.get("warnings") or []), "selection": None,
                  "inspection_id": None, "inspection": None,
                  "prepared_id": None, "prepared": None,
                  "selected_cell_scope_ids": [], "ready_for_settings": False}
        warnings = result["warnings"]
        # Configuration suite metadata is historical evidence, never authority to
        # reopen or replace the private workspace owned by this window.
        with self.lock:
            active_inspections = [(key, copy.deepcopy(value)) for key, value in self.inspections.items()
                if (value.get("preview") or {}).get("setup_private_session")]
            active_suites = bool(self.setup_private_sessions)
        if active_inspections or active_suites:
            begun = False
            try:
                self._begin_lifecycle_operation("load a configuration", exclusive=True)
                begun = True
                with command_cancellation_scope(self.prepare_cancel_event):
                    self._require_idle_setup_resources("load a configuration")
                    check_command_cancellation()
                    with self.lock:
                        active_inspections = [(key, copy.deepcopy(value)) for key, value in self.inspections.items()
                            if (value.get("preview") or {}).get("setup_private_session")]
                    if len(active_inspections) != 1:
                        warnings.append("Current working copy has no unambiguous cell catalogue; configuration retained as a draft.")
                        result["match"] = match_configuration(normalized["document"], {})
                        result["match"]["selection_present"] = False
                        result["preserve_current_suite"] = True
                        return result
                    inspection_id, record = active_inspections[0]
                    inspection = record.get("preview") or {}
                    ids = record.get("prepared_ids") or []
                    with self.lock:
                        catalogue = copy.deepcopy(self.prepared.get(ids[0]) or {}) if len(ids) == 1 else {}
                    scopes = inspection.get("cell_scopes") or []
                    selected = set(selected_scope_ids if selected_scope_ids is not None else
                                   (catalogue.get("cell_selection") or {}).get("scope_ids") or [])
                    selected_scopes = [row for row in scopes if row.get("scope_id") in selected]
                    context = self._configuration_match_context(catalogue, inspection)
                    context["cell_scopes"] = selected_scopes
                    current_test = selected_test or (inspection.get("setup") or {}).get("selected_test")
                    if current_test:
                        context["setup"]["tests"] = [row for row in context["setup"]["tests"]
                            if row.get("name") == current_test]
                        context["outputs"] = [row for row in context["outputs"]
                            if not row.get("test") or row.get("test") == current_test]
                    cell_match = match_configuration_cells(normalized["document"], selected_scopes,
                                                           current_setup=context["setup"])
                    matched = set(cell_match["scope_ids"])
                    matching_scopes = [row for row in selected_scopes if row.get("scope_id") in matched]
                    def within_selected(row):
                        design = tuple(row.get("design_" + key) for key in ("library", "cell", "view"))
                        path_value = row.get("hierarchy_path") or ""
                        for scope in matching_scopes:
                            scope_design = scope.get("design") or {}
                            scope_path = scope.get("hierarchy_path") or ""
                            if (design == tuple(scope_design.get(key) for key in ("library", "cell", "view")) and
                                    scope_path and (scope_path == "/" or path_value == scope_path or
                                    path_value.startswith(scope_path.rstrip("/") + "/"))):
                                return True
                        return False
                    context["parameter_by_id"] = dict((key, row) for key, row in
                        context["parameter_by_id"].items() if within_selected(row))
                    match = match_configuration(normalized["document"], context)
                    execution = match["apply"].get("execution_selection")
                    if current_test and execution and execution.get("tests") != [current_test]:
                        match["apply"].pop("execution_selection", None)
                        match["warnings"].append("Saved Test selection differs from the current Test; execution selection retained: %s." %
                            ", ".join(execution.get("tests") or []))
                    # Selection is controlled by the window, never by a preset.
                    match["selection_present"] = False
                    result.update(match=match, cell_match=cell_match, preserve_current_suite=True,
                                  ready_for_settings=bool(catalogue))
                    warnings.extend(cell_match.get("warnings") or [])
                    warnings.extend(match.get("warnings") or [])
                    result["warnings"] = list(dict.fromkeys(warnings))[:100]
                    check_command_cancellation()
                    return result
            except Exception as error:
                error.configuration_import = dict(result, preserve_current_suite=True)
                raise
            finally:
                if begun:
                    self._end_lifecycle_operation(exclusive=True)
        current = (self.config.get("_selected_setup_path") or
                   ((self.config.get("_maestro_launch_context") or {}).get("source") or {}).get("setup_path") or
                   ((self.invoking_context or {}).get("source") or {}).get("setup_path"))
        saved = normalized.get("source") or {}
        chosen = path or current or saved.get("path")
        catalogue, inspection = {}, {}
        previous_scopes = []
        # A legacy recipe may omit cells. Retain only this window's explicit,
        # already prepared scopes, then match them again after fresh inspection.
        if chosen and current and os.path.realpath(chosen) == os.path.realpath(current):
            with self.lock:
                for previous in self.prepared.values():
                    previous_path = (previous.get("setup_locator") or {}).get("path") or previous.get("setup_path")
                    if previous_path and os.path.realpath(previous_path) == os.path.realpath(chosen):
                        previous_scopes = copy.deepcopy((previous.get("cell_selection") or {}).get("scopes") or [])
        begun, stage, rollback = False, "source_selection", None
        try:
            if not chosen:
                warnings.append("Configuration imported; select a saved Maestro before Load and Apply.")
                return result
            reference = normalize_setup_reference(chosen)
            result["setup_path"] = reference["path"]
            if saved.get("path") and os.path.realpath(saved["path"]) != os.path.realpath(reference["path"]):
                warnings.append("Saved configuration refers to another setup; the current selected setup is retained.")
            self._begin_lifecycle_operation("load a configuration", exclusive=True)
            begun = True
            with command_cancellation_scope(self.prepare_cancel_event):
                self._require_idle_setup_resources("load a configuration")
                with self.lock:
                    rollback = (copy.deepcopy(self.config), copy.deepcopy(self.prepared),
                        copy.deepcopy(self.inspections), copy.deepcopy(self.test_inspections), self.inspection_epoch)
                self._set_activity("source_selection", "Selecting the current saved Maestro")
                result["selection"] = self._select_setup_registered(reference["path"])
                check_command_cancellation()
                result["restored_environment_overrides"] = self._restore_configuration_solver_overrides(
                    normalized["document"], warnings)
                stage = "test_inspection"
                test_id, test_preview = self._inspect_tests_registered(reference["path"], backend=reference["backend"])
                result.update(test_inspection_id=test_id, test_inspection=test_preview)
                execution = normalized["document"].get("execution_selection") or {}
                requested = execution.get("tests") or []
                available = set(row.get("name") for row in test_preview.get("tests") or [])
                if not requested and len(available) == 1:
                    requested = list(available)
                if len(requested) != 1 or requested[0] not in available:
                    warnings.append("Configuration does not identify exactly one available Test; select a Test manually.")
                    return result
                stage = "cell_inspection"
                inspection_id, inspection = self._inspect_test_cells_registered(test_id, requested[0], max_depth)
                result.update(inspection_id=inspection_id, inspection=inspection, selected_test=requested[0])
                check_command_cancellation()
                self._set_activity("configuration_matching", "Matching saved settings to current Maestro")
                cell_match = match_configuration_cells(normalized["document"],
                    inspection.get("cell_scopes") or [], current_setup=self._configuration_match_context(
                        inspection=inspection, path=reference["path"])["setup"])
                if (not cell_match.get("selection_present") and not cell_match.get("scope_ids") and previous_scopes):
                    from .settings_state import _cell_evidence
                    retained = copy.deepcopy(normalized["document"])
                    retained["selection"] = {"provided": True}
                    retained["settings"]["cells"] = [{"scope_id": row.get("scope_id"), "selected": True,
                        "evidence": _cell_evidence(row)} for row in previous_scopes]
                    prior_match = match_configuration_cells(retained, inspection.get("cell_scopes") or [])
                    cell_match["scope_ids"] = prior_match["scope_ids"]
                    cell_match["warnings"].extend(prior_match.get("warnings") or [])
                    if cell_match["scope_ids"]:
                        cell_match["warnings"].append("Legacy configuration retained this window's matching selected cells; current Maestro bindings were inspected again.")
                result["cell_match"] = cell_match
                result["selected_cell_scope_ids"] = list(cell_match.get("scope_ids") or [])
                warnings.extend(cell_match.get("warnings") or [])
                if not result["selected_cell_scope_ids"]:
                    warnings.append("Configuration imported; no saved cells matched. Select cells and Apply when ready.")
                    return result
                stage = "apply_selected_cells"
                prepared_id, catalogue = self._prepare_cells_registered(
                    inspection_id, result["selected_cell_scope_ids"])
                check_command_cancellation()
                result.update(prepared_id=prepared_id, prepared=catalogue, ready_for_settings=True)
                if catalogue.get("setup_private_session"):
                    inspection["setup_private_session"] = copy.deepcopy(catalogue["setup_private_session"])
                    inspection["source_identity"] = copy.deepcopy(catalogue.get("source_identity"))
                warnings.extend(catalogue.get("setup_refresh_warnings") or [])
                return result
        except Exception as error:
            if isinstance(error, CommandCancelled) and rollback is not None:
                with self.lock:
                    # A newly created or uncertain suite is still owned. Keep
                    # its source binding and all evidence for explicit recovery.
                    can_rollback = (not self.setup_private_sessions and not error.cleanup_failed and
                        not getattr(error, "unsafe_to_optimize", False) and
                        not getattr(error, "setup_private_session", None))
                    if can_rollback:
                        (self.config, self.prepared, self.inspections,
                            self.test_inspections, self.inspection_epoch) = rollback
                    else:
                        warnings.append("Cancellation retained working-copy resources and source binding for recovery; review Working Copies before retrying.")
                result["cancelled"] = True
            self._set_activity(stage, str(error), "error")
            result["preparation_error"] = "%s: %s" % (error.__class__.__name__, error)
            result["preparation_stage"] = stage
            session = getattr(error, "setup_private_session", None) or {}
            if not isinstance(session, dict):
                session = {}
            if (isinstance(error, CommandCancelled) or getattr(error, "cleanup_failed", False)
                    or getattr(error, "unsafe_to_optimize", False) or session.get("unsafe")):
                error.configuration_import = result
                raise
            warnings.append("Configuration draft retained; %s did not complete: %s" %
                            (stage, result["preparation_error"]))
            return result
        finally:
            try:
                # Snapshot identity while this operation still owns Setup;
                # matching below is pure and cannot borrow later UI authority.
                context = self._configuration_match_context(catalogue, inspection, result.get("setup_path"))
            except Exception as context_error:
                context = {}
                warnings.append("Configuration draft retained; current catalogue is unavailable: %s" % context_error)
            finally:
                if begun:
                    self._end_lifecycle_operation(exclusive=True)
            try:
                result["match"] = match_configuration(normalized["document"], context)
                warnings.extend(result["match"].get("warnings") or [])
                matched_document = result["match"].get("document")
                if isinstance(matched_document, dict):
                    result["document"] = matched_document
                    result["settings"] = matched_document.get("settings") or result["settings"]
                    normalized["document"] = matched_document
                    normalized["settings"] = result["settings"]
            except Exception as match_error:
                warnings.append("Configuration draft retained; settings matching is incomplete: %s" % match_error)
            try:
                # Reuse this operation's final display projection. Native and
                # source qualification above still executes on every load.
                result["eda_setup"] = copy.deepcopy(
                    (catalogue.get("eda_context") or {}).get("cadence") or
                    (inspection.get("eda_context") or {}).get("cadence") or
                    self.eda_setup(catalogue.get("setup") or inspection.get("setup")))
            except Exception as display_error:
                warnings.append("Current EDA display is unavailable: %s" % display_error)
            result["warnings"] = list(dict.fromkeys(warnings))[:100]
            for warning in result["warnings"]:
                self._set_activity("configuration_warning", warning,
                    "error" if result.get("preparation_error") else "running", "WARN")
            if not result.get("preparation_error"):
                self._set_activity("configuration_complete", "Configuration restored%s" % (
                    "; review warnings" if result["warnings"] else ""), "complete")

    def load_design_config(self, document, path=None):
        generic_reference = document.get("setup_locator") or document.get("ade_setup")
        legacy_reference = document.get("maestro")
        reference = path or generic_reference or legacy_reference
        if not reference:
            raise ValueError("design config has no ADE setup path; select the setup and load again")
        reference = normalize_setup_reference(reference)
        self._validate_launch_binding(reference["path"])
        if self.invoking_context or self.config.get("_maestro_launch_context"):
            prepared_id, prepared = self.prepare(
                reference["path"], backend=reference["backend"], initialize_validation=False,
            )
            resolution = resolve_design_config(document, prepared)
            return {"prepared_id": prepared_id, "prepared": prepared, "resolution": resolution}
        if generic_reference or (path and reference["backend"] == "adexl"):
            prepared_id, prepared = self.prepare(reference["path"], backend=reference["backend"])
        else:
            prepared_id, prepared = self.prepare(reference["path"])
        resolution = resolve_design_config(document, prepared)
        return {"prepared_id": prepared_id, "prepared": prepared, "resolution": resolution}

    def resume(self, checkpoint, path=None):
        self._recover_optimized_view_cleanup()
        self._begin_lifecycle_operation("resume an optimization")
        try:
            return self._resume_registered(checkpoint, path=path)
        finally:
            self._end_lifecycle_operation()

    def _resume_registered(self, checkpoint, path=None):
        if not isinstance(checkpoint, dict):
            raise ValueError("checkpoint must be a JSON object")
        validate_checkpoint_integrity(checkpoint)
        run_config = self._freeze_run_config({}, None, checkpoint=checkpoint)
        manifest = checkpoint.get("manifest") or {}
        persisted_scopes = cell_selection_scopes(manifest.get("cell_selection"))
        if not (path or manifest.get("setup_locator") or manifest.get("maestro_path")):
            raise ValueError("checkpoint has no ADE setup path; select the setup and load again")
        bound_reference = normalize_setup_reference(
            path or manifest.get("setup_locator") or manifest.get("maestro_path")
        )
        self._validate_launch_binding(bound_reference["path"])
        runtime_root = run_config["runtime"]["run_root"]
        startup_storage_preflight(
            os.path.join(runtime_root, ".optimization-resume-preflight"),
            runtime_root,
            run_config["runtime"].get("persistent_min_free_disk_mib", 1024),
            checkpoint=checkpoint, workspace_required=False,
        )
        continuation_lease = ContinuationLease(
            runtime_root, checkpoint_continuation_id(checkpoint),
        ).acquire()
        runtime_lease = None
        tb_lease = None
        try:
            if str((checkpoint.get("spec") or {}).get("evaluator", "real")).lower() != "mock":
                tb_lease = self._acquire_tb_lease(run_config)
            anchor_selection = _reconcile_continuation_anchor(
                checkpoint, runtime_root, persist_absent=False,
            )
            generic_reference = manifest.get("setup_locator")
            reference = normalize_setup_reference(path or generic_reference or manifest.get("maestro_path"))
            spec = checkpoint.get("spec") or {}
            self._wait_for_shared_validation_resources()
            origin_root, _origin_reason = _checkpoint_origin_path(
                checkpoint, runtime_root,
            )
            real_eda = str(spec.get("evaluator", "real")).strip().lower() != "mock"
            if real_eda or origin_root is not None:
                runtime_lease = RuntimeOptimizationLease(
                    run_config["runtime"]["run_root"], run_root=origin_root,
                ).acquire()
            origin_inspection = self._cleanup_checkpoint_origin(
                checkpoint, terminate_owned=True, inspect_only=True,
            )
            anchor_after_origin = _reconcile_continuation_anchor(
                checkpoint, runtime_root, persist_absent=False,
            )
            baseline_snapshot = manifest.get("setup_baseline_snapshot")
            if baseline_snapshot is not None:
                prepared = load_prepared_baseline(baseline_snapshot)
                reference_path = (prepared.get("setup_locator") or {}).get("path") or prepared.get("maestro_path")
                if os.path.realpath(reference_path or "") != os.path.realpath(reference["path"]):
                    raise ValueError("Checkpoint reference belongs to another saved setup")
                require_prepared_freshness(prepared, run_config, action="resume optimization")
                from .runner import _reverify_prepared_external_environment
                external = _reverify_prepared_external_environment(prepared, run_config)
                if not external.get("ok"):
                    raise ValueError(external.get("error") or "Checkpoint EDA environment changed")
                prepared_id = uuid.uuid4().hex
                with self.lock:
                    self.prepared[prepared_id] = prepared
            elif persisted_scopes is not None:
                prepared_id, prepared = self.prepare(
                    reference["path"], backend=reference["backend"],
                    initialize_validation=False, selected_cell_paths=persisted_scopes,
                )
            elif reference["backend"] == "adexl":
                prepared_id, prepared = self.prepare(reference["path"], backend="adexl", initialize_validation=False)
            else:
                prepared_id, prepared = self.prepare(reference["path"], initialize_validation=False)
            self._attach_live_setup_private_session(prepared)
            validate_checkpoint(checkpoint, prepared)
            self._require_prelaunch_safety(
                prepared, spec.get("evaluator", "real"),
            )
            resume_preflight = startup_storage_preflight(
                os.path.join(runtime_root, ".optimization-resume-preflight"),
                runtime_root,
                run_config["runtime"].get("persistent_min_free_disk_mib", 1024),
                setup=(prepared or {}).get("setup"),
                hierarchy=(prepared or {}).get("hierarchy"), config=run_config,
                checkpoint=checkpoint,
                workspace_required=optimization_workspace_required(spec, checkpoint),
                parameters=_selected_workspace_parameters(prepared, spec.get("variables")),
            )
            anchor_before_cleanup = _reconcile_continuation_anchor(checkpoint, runtime_root)
            abandoned_cleanup = self._cleanup_checkpoint_origin(checkpoint)
            if spec.get('execution_backend') == 'maestro_baseline' and not prepared.get('setup_private_session'):
                self._prepare_setup_private_session(prepared)
                self._apply_setup_private_cells(prepared, prepared.get('setup_private_session'))
            abandoned_cleanup["preflight"] = origin_inspection
            abandoned_cleanup["checkpoint_anchor"] = _reconcile_continuation_anchor(
                checkpoint, runtime_root,
            )
            abandoned_cleanup["checkpoint_anchor_initial"] = anchor_selection
            abandoned_cleanup["checkpoint_anchor_after_origin"] = anchor_after_origin
            abandoned_cleanup["checkpoint_anchor_before_cleanup"] = anchor_before_cleanup
            abandoned_cleanup["startup_storage_preflight"] = resume_preflight
            launch_runtime_lease = runtime_lease if real_eda else None
            if runtime_lease is not None and not real_eda:
                runtime_lease.release()
                runtime_lease = None
            result = self._launch(
                spec, prepared, checkpoint=checkpoint,
                continuation_lease=continuation_lease,
                runtime_optimization_lease=launch_runtime_lease, run_config=run_config, tb_optimization_lease=tb_lease,
            )
        except Exception:
            if tb_lease is not None:
                tb_lease.release()
            if runtime_lease is not None:
                runtime_lease.release()
            continuation_lease.release()
            raise
        result.update({"prepared_id": prepared_id, "prepared": prepared, "abandoned_cleanup": abandoned_cleanup})
        return result

    def wait_for_terminal(self, run_name):
        with self.lock:
            thread = self.threads.get(run_name)
        if thread is None:
            raise ValueError("run is not owned by this service: %s" % run_name)
        if thread is threading.current_thread():
            raise RuntimeError("optimization thread cannot wait for itself")
        thread.join()
        status = self.read(run_name, _contract_text("status.json"))
        if not isinstance(status, dict):
            raise RuntimeError("optimization thread exited without a readable status")
        if status.get("state") not in ("completed", "failed", "stopped"):
            raise RuntimeError(
                "optimization thread exited without a terminal status: %s"
                % status.get("state")
            )
        return {
            "run_id": run_name,
            "run_root": self.run_path(run_name, ""),
            "status": status,
            "result": self.read(run_name, _contract_text("result.json")),
            "server_error": self.read(run_name, _contract_text("server_error.json")),
        }

    def _cleanup_checkpoint_origin(self, checkpoint, terminate_owned=False, inspect_only=False):
        manifest = checkpoint.get("manifest") or {}
        origin_id = manifest.get("origin_run_id")
        run_config = self._config_for_run(origin_id) if origin_id in self._run_locations else self.config
        origin_root, unavailable_reason = _checkpoint_origin_path(
            checkpoint, run_config["runtime"]["run_root"],
        )
        if origin_root is None:
            return {"state": "skipped", "reason": unavailable_reason}
        with self.lock:
            origin_thread = self.threads.get(origin_id)
            origin_active = bool(origin_thread and origin_thread.is_alive())
        if origin_active:
            raise RuntimeError("cannot resume checkpoint while its origin optimization is still running")
        checkpoint_selection = (
            _guard_origin_checkpoint_revision(
                checkpoint, origin_root, run_config["runtime"]["run_root"],
            )
            if checkpoint.get("schema_version") == _contract_text("crescendo.checkpoint.v1") else
            {"state": "legacy_checkpoint_without_integrity_schema"}
        )
        if (checkpoint.get("spec") or {}).get("execution_backend") == "maestro_baseline":
            from .lifecycle_recovery import reconcile_baseline_run
            return reconcile_baseline_run(origin_root, request_cancel=terminate_owned, inspect_only=inspect_only)
        netlist_origin = (checkpoint.get("spec") or {}).get("execution_backend") in ("private_netlist", "maestro_netlist")
        netlist_recovery = None
        if netlist_origin:
            from .netlist_execution import reconcile_previous_ownership
            from .netlist_run_ownership import reconcile_run_preparations
            # The shared native views may still back a submitted solver's
            # dependencies. Prove all old solver ownership stopped first.
            netlist_recovery = reconcile_previous_ownership(
                os.path.join(origin_root, "private_netlist", "execution_ownership"), origin_root,
                request_cancel=terminate_owned,
            )
            if not netlist_recovery.get("cleanup_proven"):
                raise RuntimeError(
                    "Previous Template Netlist jobs are not proven stopped; retained ownership prevents duplicate submission: %s"
                    % netlist_recovery.get("path")
                )
            native_recovery = reconcile_run_preparations(
                origin_root,
                request_cancel=terminate_owned, timeout_seconds=12 if terminate_owned else 0,
                solver_cleanup_proven=True,
            )
            if not native_recovery.get("cleanup_proven"):
                raise RuntimeError(
                    "Previous native netlist preparation is not proven stopped; retained evidence blocks duplicate preparation: %s"
                    % native_recovery.get("root")
                )
            netlist_recovery["native_preparation"] = native_recovery

        controller_epochs = []
        controllers_root = os.path.join(origin_root, "controllers")
        if os.path.lexists(controllers_root):
            if os.path.islink(controllers_root) or not os.path.isdir(controllers_root):
                raise RuntimeError("cannot safely inspect origin controller root")
            for slot_name in sorted(os.listdir(controllers_root)):
                slot_match = _CONTROLLER_SLOT_NAME.match(slot_name)
                if not slot_match:
                    raise RuntimeError("unrecognized controller slot path: %s" % slot_name)
                slot_root = os.path.join(controllers_root, slot_name)
                if os.path.islink(slot_root) or not os.path.isdir(slot_root):
                    raise RuntimeError("cannot safely inspect controller slot: %s" % slot_name)
                for epoch_name in sorted(os.listdir(slot_root)):
                    epoch_match = _EPOCH_NAME.match(epoch_name)
                    if not epoch_match:
                        raise RuntimeError(
                            "unrecognized controller epoch path: %s/%s" % (slot_name, epoch_name)
                        )
                    epoch_root = os.path.join(slot_root, epoch_name)
                    if os.path.islink(epoch_root) or not os.path.isdir(epoch_root):
                        raise RuntimeError(
                            "cannot safely inspect controller epoch: %s/%s" % (slot_name, epoch_name)
                        )
                    inspection = _inspect_controller_epoch(
                        epoch_root, origin_root, int(slot_match.group(1)), int(epoch_match.group(1)),
                    )
                    inspection["controller_epoch"] = "%s/%s" % (slot_name, epoch_name)
                    inspection["epoch_root"] = epoch_root
                    controller_epochs.append(inspection)
        active_controllers = [item for item in controller_epochs if item["state"] == "active"]
        if active_controllers:
            if not terminate_owned:
                raise RuntimeError(
                    "cannot resume checkpoint while an owned persistent controller is still alive: %s"
                    % [
                        {"controller_epoch": item["controller_epoch"], "pid": item.get("pid")}
                        for item in active_controllers
                    ]
                )
            if len(active_controllers) != 1:
                raise RuntimeError(
                    "cannot safely terminate more than one orphan persistent controller: %s"
                    % [item["controller_epoch"] for item in active_controllers]
                )
            timeout = _resume_cleanup_timeout(
                run_config.get("runtime", {}),
                "resume_controller_shutdown_timeout_seconds", 15.0,
            )
            terminated = _terminate_owned_controller(active_controllers[0], timeout)
            terminated["controller_epoch"] = active_controllers[0]["controller_epoch"]
            terminated["epoch_root"] = active_controllers[0]["epoch_root"]
            controller_epochs = [
                terminated if item is active_controllers[0] else item
                for item in controller_epochs
            ]
        ambiguous_controllers = [item for item in controller_epochs if item["state"] == "ambiguous"]
        if ambiguous_controllers:
            raise RuntimeError(
                "cannot resume checkpoint because persistent controller identity is ambiguous: %s"
                % [
                    {
                        "controller_epoch": item["controller_epoch"], "pid": item.get("pid"),
                        "proof": item.get("proof"),
                    }
                    for item in ambiguous_controllers
                ]
            )

        workers_root = os.path.join(origin_root, "workers")
        def inspect_worker_epochs():
            inspected = []
            if not os.path.lexists(workers_root):
                return inspected
            if os.path.islink(workers_root) or not os.path.isdir(workers_root):
                raise RuntimeError("cannot safely inspect origin worker root")
            for worker_name in sorted(os.listdir(workers_root)):
                worker_index = parse_worker_name(worker_name)
                if worker_index is None:
                    raise RuntimeError("unrecognized worker namespace path: %s" % worker_name)
                worker_root = os.path.join(workers_root, worker_name)
                if os.path.islink(worker_root) or not os.path.isdir(worker_root):
                    raise RuntimeError("cannot safely inspect worker directory: %s" % worker_name)
                for epoch_name in sorted(os.listdir(worker_root)):
                    namespace = parse_worker_epoch_namespace(worker_name, epoch_name)
                    if namespace is None:
                        raise RuntimeError(
                            "unrecognized worker epoch path: %s/%s" % (worker_name, epoch_name)
                        )
                    epoch_root = os.path.join(worker_root, epoch_name)
                    if os.path.islink(epoch_root) or not os.path.isdir(epoch_root):
                        raise RuntimeError("cannot safely inspect worker epoch: %s/%s" % (worker_name, epoch_name))
                    inspection = _inspect_worker_epoch_processes(
                        epoch_root, worker_index, namespace["local_epoch"],
                    )
                    inspection["epoch_root"] = epoch_root
                    inspection["worker_epoch"] = "%s/%s" % (worker_name, epoch_name)
                    inspected.append(inspection)
            return inspected

        worker_epochs = inspect_worker_epochs()
        worker_quiesce_results = []
        worker_timeout = _resume_cleanup_timeout(
            run_config.get("runtime", {}),
            "resume_worker_cleanup_timeout_seconds", 30.0,
        )
        worker_deadline = time.monotonic() + worker_timeout
        active_workers = [item for item in worker_epochs if item["state"] == "active"]
        if active_workers and terminate_owned:
            passive_deadline = min(
                worker_deadline,
                time.monotonic() + min(1.0, worker_timeout / 4.0),
            )
            while active_workers and time.monotonic() < passive_deadline:
                time.sleep(0.1)
                worker_epochs = inspect_worker_epochs()
                active_workers = [item for item in worker_epochs if item["state"] == "active"]
        recoverable_workers = [
            item for item in worker_epochs
            if item["state"] in ("active", "unsafe")
        ]
        if recoverable_workers and terminate_owned:
            for item in recoverable_workers:
                recovered = _quiesce_worker_epoch_processes(
                    item["epoch_root"], item["worker_index"], item["epoch_index"],
                    max(0.0, worker_deadline - time.monotonic()),
                )
                worker_quiesce_results.append({
                    "worker_epoch": item["worker_epoch"],
                    "termination_requested": bool(
                        recovered.get("termination_requested")
                    ),
                    "proof": recovered.get("recovery_proof")
                    or recovered.get("termination_proof"),
                })
            worker_epochs = inspect_worker_epochs()
            active_workers = [item for item in worker_epochs if item["state"] == "active"]
        if active_workers:
            identities = [
                {"worker_epoch": item["worker_epoch"], "processes": item["processes"]}
                for item in active_workers
            ]
            raise RuntimeError(
                "cannot resume checkpoint while owned Maestro/Ocean workers are still alive: %s" % identities
            )
        unsafe_workers = [item for item in worker_epochs if item["state"] == "unsafe"]
        if unsafe_workers:
            identities = [
                {
                    "worker_epoch": item["worker_epoch"],
                    "guardian_status": item.get("guardian_status"),
                    "termination_proof": item.get("termination_proof"),
                }
                for item in unsafe_workers
            ]
            raise RuntimeError(
                "cannot resume checkpoint because previous worker termination is not proven: %s"
                % identities
            )
        legacy_attempts = _quiesce_legacy_candidate_attempts(
            origin_root, checkpoint, run_config, terminate_owned=terminate_owned,
        )
        if inspect_only:
            return {
                "state": "inspected",
                "controller_epochs": len(controller_epochs),
                "worker_epochs": len(worker_epochs),
                "legacy_attempts": len(legacy_attempts),
                "legacy_guardians_terminated": len([
                    item for item in legacy_attempts if item.get("termination_requested")
                ]),
                "worker_guardians_terminated": len([
                    item for item in worker_quiesce_results
                    if item.get("termination_requested")
                ]),
                "worker_guardian_recovery": worker_quiesce_results,
                "checkpoint_selection": checkpoint_selection,
                "private_netlist_recovery": netlist_recovery,
                "orphan_controller_terminated": any(
                    (item.get("proof") or {}).get("kind") == "controller_termination_proven"
                    for item in controller_epochs
                ),
            }

        reconciled_checkpoint_path = None
        checkpoint_updated = False
        if checkpoint_selection.get("state") == "external_newer":
            reconciled_checkpoint_path = _persist_reconciled_origin_checkpoint(
                origin_root, checkpoint,
            )
            checkpoint_updated = True

        checkpoint_rows = []
        planned_candidates = []
        if checkpoint.get("schema_version") == _contract_text("crescendo.checkpoint.v1"):
            checkpoint_rows = checkpoint_candidates(checkpoint)
            planned_candidates = _checkpoint_planned_candidates(checkpoint)
        existing_ids = set(row.get("candidate_id") for row in checkpoint_rows)
        recovered_rows = []
        origin_termination = {
            "termination_proven": True,
            "process_still_alive": False,
            "reason": "checkpoint resume proved all origin controller and worker processes dead",
        }
        for index, generation, values in planned_candidates:
            candidate_id = "g%03d_c%04d" % (generation, index)
            if candidate_id in existing_ids:
                continue
            recovered = _recover_terminal_candidate_wal(
                origin_root, index, generation, values, checkpoint.get("spec") or {},
                0, origin_termination,
            )
            if recovered is not None:
                recovered_rows.append(recovered)
                existing_ids.add(candidate_id)
        if recovered_rows:
            reconciled = checkpoint_with_candidates(checkpoint, checkpoint_rows + recovered_rows)
            reconciled_checkpoint_path = _persist_reconciled_origin_checkpoint(
                origin_root, reconciled,
            )
            checkpoint.clear()
            checkpoint.update(reconciled)
            checkpoint_rows.extend(recovered_rows)
            checkpoint_updated = True

        if checkpoint_updated:
            _reconcile_continuation_anchor(
                checkpoint, run_config["runtime"]["run_root"],
            )

        resumed_pending_cleanup = resume_pending_cleanups(origin_root)

        candidate_root = os.path.join(origin_root, "candidates")
        if os.path.lexists(candidate_root) and (
            os.path.islink(candidate_root) or not os.path.isdir(candidate_root)
        ):
            raise RuntimeError("cannot safely inspect origin candidate root")
        cleaned = []
        cleaned_nonterminal = []
        retained_lightweight = []
        candidate_errors = []
        planned_by_id = dict(
            ("g%03d_c%04d" % (generation, index), values)
            for index, generation, values in planned_candidates
        )
        if os.path.isdir(candidate_root):
            for name in sorted(os.listdir(candidate_root)):
                if not _CANDIDATE_NAME.match(name):
                    candidate_errors.append({
                        "candidate_id": name, "reason": "unrecognized candidate namespace path",
                    })
                    continue
                root = os.path.join(candidate_root, name)
                if os.path.islink(root) or not os.path.isdir(root):
                    candidate_errors.append({
                        "candidate_id": name,
                        "reason": "candidate artifact is not a real direct directory",
                    })
                    continue
                try:
                    candidate_path = os.path.join(root, _contract_text("candidate.json"))
                    if not os.path.lexists(candidate_path):
                        if name in planned_by_id and not os.listdir(root):
                            os.rmdir(root)
                            cleaned_nonterminal.append(name)
                            continue
                        raise RuntimeError("candidate record is missing")
                    if os.path.islink(candidate_path) or not os.path.isfile(candidate_path):
                        raise RuntimeError("candidate record is not a regular file")
                    candidate = load_json(candidate_path)
                    if not isinstance(candidate, dict) or candidate.get("candidate_id") != name:
                        raise RuntimeError("candidate record identity does not match its directory")
                    status = candidate.get("status")
                    if netlist_origin and candidate.get("execution_backend") in ("private_netlist", "maestro_netlist"):
                        # All owned tasks were reconciled above. Keep interrupted
                        # diagnostics instead of passing this engine's raw files
                        # through an OA/legacy-specific recovery routine.
                        if status == "cancelled":
                            expected = planned_by_id.get(name)
                            if (candidate.get("failure_class") not in (None, "user_stop") or expected is None
                                    or not _terminal_wal_json_exact(candidate.get("values"), expected)
                                    or candidate.get("execution_backend") != (checkpoint.get("spec") or {}).get("execution_backend")
                                    or type(candidate.get("generation")) is not int
                                    or type(candidate.get("index")) is not int
                                    or "g%03d_c%04d" % (candidate["generation"], candidate["index"]) != name):
                                raise RuntimeError("cancelled Template Netlist candidate differs from its active checkpoint plan")
                            # Cancellation never supplies fitness. Keep its
                            # evidence and let Resume evaluate the saved values.
                        elif status not in _TERMINAL_CANDIDATE_STATES and status not in ("queued", "running"):
                            raise RuntimeError("Template Netlist candidate status is not recognized")
                        retained_lightweight.append(name)
                        continue
                    if status in _TERMINAL_CANDIDATE_STATES:
                        if _candidate_is_lightweight_only(root):
                            retained_lightweight.append(name)
                            continue
                        required_paths = [candidate_path]
                        if status == "completed":
                            required_paths.append(os.path.join(root, _contract_text("completion_evidence.json")))
                        cleanup_terminal_candidate_artifacts(
                            root, origin_root, verify_sources_unchanged,
                            required_lightweight_paths=required_paths,
                        )
                        cleaned.append(name)
                    elif status in ("queued", "running"):
                        cleanup_abandoned_nonterminal_candidate(
                            root, origin_root, verify_sources_unchanged,
                        )
                        cleaned_nonterminal.append(name)
                    else:
                        raise RuntimeError("candidate status is not recognized: %r" % status)
                except Exception as error:
                    candidate_errors.append({
                        "candidate_id": name,
                        "reason": "%s: %s" % (error.__class__.__name__, error),
                    })
        if candidate_errors:
            raise RuntimeError(
                "cannot resume checkpoint because origin candidate cleanup failed: %s"
                % candidate_errors
            )

        cleaned_workers = []
        worker_cleanup_evidence = []
        worker_errors = []
        for item in worker_epochs:
            try:
                cleanup_result = cleanup_abandoned_worker_epoch(
                    item["epoch_root"], origin_root, verify_sources_unchanged,
                    termination_proof=item.get("termination_proof"),
                )
                cleaned_workers.append(item["worker_epoch"])
                worker_cleanup_evidence.append({
                    "worker_epoch": item["worker_epoch"], "cleanup": cleanup_result,
                })
            except Exception as error:
                worker_errors.append({
                    "worker_epoch": item["worker_epoch"],
                    "reason": "%s: %s" % (error.__class__.__name__, error),
                })
        if worker_errors:
            raise RuntimeError(
                "cannot resume checkpoint because proven-dead worker cleanup failed: %s"
                % worker_errors
            )

        cleaned_controllers = []
        retained_controllers = []
        controller_errors = []
        for item in controller_epochs:
            if item["state"] != "dead":
                continue
            if not item.get("cleanup_supported"):
                retained_controllers.append({
                    "controller_epoch": item["controller_epoch"],
                    "status": item.get("status"), "pid": item.get("pid"),
                    "proof": item.get("proof"), "retained": True,
                    "reason": "legacy or pre-lock controller metadata",
                })
                continue
            try:
                cleanup_abandoned_controller_epoch(
                    item["epoch_root"], origin_root, item.get("proof"),
                )
                cleaned_controllers.append(item["controller_epoch"])
            except Exception as error:
                controller_errors.append({
                    "controller_epoch": item["controller_epoch"],
                    "reason": "%s: %s" % (error.__class__.__name__, error),
                })
        if controller_errors:
            raise RuntimeError(
                "cannot resume checkpoint because proven-dead controller cleanup failed: %s"
                % controller_errors
            )
        return {
            "state": "completed",
            "cleaned": cleaned,
            "cleaned_nonterminal": cleaned_nonterminal,
            "retained_lightweight": retained_lightweight,
            "recovered_terminal_wal": [row["candidate_id"] for row in recovered_rows],
            "checkpoint_updated": checkpoint_updated,
            "reconciled_checkpoint_path": reconciled_checkpoint_path,
            "resumed_pending_cleanup": resumed_pending_cleanup,
            "skipped": [],
            "cleaned_workers": cleaned_workers,
            "worker_cleanup_evidence": worker_cleanup_evidence,
            "skipped_workers": [],
            "cleaned_controllers": cleaned_controllers,
            "retained_controllers": retained_controllers,
            "checkpoint_selection": checkpoint_selection,
            "legacy_attempts": [
                {
                    "candidate_id": item["candidate_id"],
                    "attempt": item["attempt"],
                    "termination_requested": bool(item.get("termination_requested")),
                    "proof": item.get("termination_proof"),
                }
                for item in legacy_attempts
            ],
            "worker_guardian_recovery": worker_quiesce_results,
            "private_netlist_recovery": netlist_recovery,
        }

    def run_path(self, run_name, name):
        run_root, run_descriptor = self._open_run_directory(run_name)
        os.close(run_descriptor)
        parts = self._safe_relative_parts(name)
        return os.path.join(run_root, *parts) if parts else run_root

    @staticmethod
    def _safe_relative_parts(name):
        if not isinstance(name, str):
            raise ValueError("run path component must be text")
        if not name:
            return []
        if os.path.isabs(name) or "\x00" in name:
            raise ValueError("invalid run path")
        normalized = name.replace("\\", "/")
        parts = normalized.split("/")
        if any(
            not part or part in (".", "..") or
            any(ord(character) < 32 or ord(character) == 127 for character in part)
            for part in parts
        ):
            raise ValueError("invalid run path")
        return parts

    @staticmethod
    def _safe_run_name(run_name):
        if (
            not isinstance(run_name, str) or run_name in ("", ".", "..") or
            "\x00" in run_name or not _SAFE_RUN_ID.match(run_name) or
            any(ord(character) < 32 or ord(character) == 127 for character in run_name)
        ):
            raise ValueError("invalid run id")
        return run_name

    @staticmethod
    def _directory_open_flags():
        if not hasattr(os, "O_DIRECTORY") or not hasattr(os, "O_NOFOLLOW"):
            raise RuntimeError("secure directory access requires O_DIRECTORY and O_NOFOLLOW")
        return os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)

    def _open_runtime_directory(self, runtime_root=None):
        runtime_root = os.path.abspath(runtime_root or self.config["runtime"]["run_root"])
        if os.path.realpath(runtime_root) != runtime_root:
            raise ValueError("runtime root must be a real non-symlink directory")
        try:
            observed = os.lstat(runtime_root)
        except OSError as error:
            raise ValueError("runtime root is unavailable: %s" % error)
        if stat.S_ISLNK(observed.st_mode) or not stat.S_ISDIR(observed.st_mode):
            raise ValueError("runtime root must be a real non-symlink directory")
        descriptor = os.open(runtime_root, self._directory_open_flags())
        try:
            opened = os.fstat(descriptor)
        except Exception:
            os.close(descriptor)
            raise
        if not stat.S_ISDIR(opened.st_mode):
            os.close(descriptor)
            raise ValueError("runtime root is not a directory")
        return runtime_root, descriptor

    def _open_runtime_relative(self, name, missing_ok=False, runtime_root=None):
        parts = self._safe_relative_parts(name)
        if not parts:
            raise ValueError("a final runtime file is required")
        runtime_root, current_descriptor = self._open_runtime_directory(runtime_root)
        try:
            for part in parts[:-1]:
                try:
                    next_descriptor = os.open(
                        part, self._directory_open_flags(), dir_fd=current_descriptor,
                    )
                except OSError as error:
                    if missing_ok and error.errno == errno.ENOENT:
                        return None, None
                    raise ValueError("unsafe runtime path: %s" % error)
                os.close(current_descriptor)
                current_descriptor = next_descriptor
            flags = (
                os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NONBLOCK", 0)
            )
            try:
                descriptor = os.open(parts[-1], flags, dir_fd=current_descriptor)
            except OSError as error:
                if missing_ok and error.errno == errno.ENOENT:
                    return None, None
                raise ValueError("unsafe runtime path: %s" % error)
            try:
                opened = os.fstat(descriptor)
            except Exception:
                os.close(descriptor)
                raise
            if not stat.S_ISREG(opened.st_mode):
                os.close(descriptor)
                raise ValueError("runtime path has an unexpected file type")
            return os.path.join(runtime_root, *parts), descriptor
        finally:
            os.close(current_descriptor)

    @staticmethod
    def _read_json_snapshot_descriptor(descriptor, description, max_bytes):
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or int(before.st_nlink) != 1:
            raise ValueError("%s must be a singly linked regular file" % description)
        if int(before.st_size) > int(max_bytes):
            raise ValueError("%s exceeds the snapshot size limit" % description)
        chunks = []
        total = 0
        while True:
            chunk = os.read(descriptor, min(65536, int(max_bytes) + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > int(max_bytes):
                raise ValueError("%s exceeds the snapshot size limit" % description)
        after = os.fstat(descriptor)
        before_identity = (
            before.st_dev, before.st_ino, before.st_size,
            getattr(before, "st_mtime_ns", before.st_mtime),
            getattr(before, "st_ctime_ns", before.st_ctime),
        )
        after_identity = (
            after.st_dev, after.st_ino, after.st_size,
            getattr(after, "st_mtime_ns", after.st_mtime),
            getattr(after, "st_ctime_ns", after.st_ctime),
        )
        if before_identity != after_identity or int(after.st_nlink) not in (0, 1):
            raise RuntimeError("%s changed while being read" % description)
        try:
            return json.loads(b"".join(chunks).decode("utf-8"))
        except (TypeError, ValueError, UnicodeError) as error:
            raise ValueError("%s is not valid JSON: %s" % (description, error))

    def _read_runtime_json_snapshot(self, name, description, max_bytes,
                                    missing_ok=True, runtime_root=None):
        _path, descriptor = self._open_runtime_relative(
            name, missing_ok=missing_ok, runtime_root=runtime_root,
        )
        if descriptor is None:
            return None
        try:
            return self._read_json_snapshot_descriptor(
                descriptor, description, max_bytes,
            )
        finally:
            os.close(descriptor)

    def _open_run_directory(self, run_name):
        safe_name = self._safe_run_name(run_name)
        runtime_root, runtime_descriptor = self._open_runtime_directory(self._runtime_for_run(safe_name))
        try:
            descriptor = os.open(
                safe_name, self._directory_open_flags(), dir_fd=runtime_descriptor,
            )
        except OSError as error:
            raise ValueError("run directory is unavailable or unsafe: %s" % error)
        finally:
            os.close(runtime_descriptor)
        try:
            opened = os.fstat(descriptor)
        except Exception:
            os.close(descriptor)
            raise
        if not stat.S_ISDIR(opened.st_mode):
            os.close(descriptor)
            raise ValueError("run directory is not a directory")
        run_root = os.path.join(runtime_root, safe_name)
        if (
            os.path.dirname(run_root) != runtime_root or
            os.path.realpath(run_root) != run_root
        ):
            os.close(descriptor)
            raise ValueError("run directory must be a real direct child of runtime root")
        return run_root, descriptor

    def _open_run_relative(self, run_name, name, directory=False, missing_ok=False):
        parts = self._safe_relative_parts(name)
        run_root, current_descriptor = self._open_run_directory(run_name)
        if not parts:
            if directory:
                return run_root, current_descriptor
            os.close(current_descriptor)
            raise ValueError("a final run file is required")
        try:
            for part in parts[:-1]:
                try:
                    next_descriptor = os.open(
                        part, self._directory_open_flags(), dir_fd=current_descriptor,
                    )
                except OSError as error:
                    if missing_ok and error.errno == errno.ENOENT:
                        return None, None
                    raise ValueError("unsafe run path: %s" % error)
                os.close(current_descriptor)
                current_descriptor = next_descriptor
            flags = (
                self._directory_open_flags() if directory else
                os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NONBLOCK", 0)
            )
            try:
                descriptor = os.open(parts[-1], flags, dir_fd=current_descriptor)
            except OSError as error:
                if missing_ok and error.errno == errno.ENOENT:
                    return None, None
                raise ValueError("unsafe run path: %s" % error)
            try:
                opened = os.fstat(descriptor)
            except Exception:
                os.close(descriptor)
                raise
            expected = stat.S_ISDIR(opened.st_mode) if directory else stat.S_ISREG(opened.st_mode)
            if not expected:
                os.close(descriptor)
                raise ValueError("run path has an unexpected file type")
            return os.path.join(run_root, *parts), descriptor
        finally:
            os.close(current_descriptor)

    def _read_run_json(self, run_name, name):
        _path, descriptor = self._open_run_relative(
            run_name, name, directory=False, missing_ok=True,
        )
        if descriptor is None:
            return None
        try:
            with os.fdopen(descriptor, "r") as stream:
                descriptor = None
                return json.load(stream)
        finally:
            if descriptor is not None:
                os.close(descriptor)

    def _read_run_json_snapshot(self, run_name, name, description, max_bytes,
                                missing_ok=True):
        _path, descriptor = self._open_run_relative(
            run_name, name, directory=False, missing_ok=missing_ok,
        )
        if descriptor is None:
            return None
        try:
            return self._read_json_snapshot_descriptor(
                descriptor, description, max_bytes,
            )
        finally:
            os.close(descriptor)

    def _read_shutdown_evidence(self, run_name, description, missing_ok=True):
        from .shutdown_evidence import read_shutdown
        _path, descriptor = self._open_run_relative(
            run_name, _contract_text("worker_shutdown.json"), directory=False, missing_ok=missing_ok)
        if descriptor is None:
            return None
        try:
            before = os.fstat(descriptor)
            result = read_shutdown(descriptor, description)
            if before.st_size <= 16 * 1024 * 1024:
                # Preserve the existing full diagnostic response for ordinary records.
                os.lseek(descriptor, 0, os.SEEK_SET)
                result = self._read_json_snapshot_descriptor(descriptor, description, 16 * 1024 * 1024)
                after = os.fstat(descriptor)
                identity = lambda item: (item.st_dev, item.st_ino, item.st_size,
                    getattr(item, "st_mtime_ns", item.st_mtime),
                    getattr(item, "st_ctime_ns", item.st_ctime), item.st_nlink)
                if identity(before) != identity(after):
                    raise RuntimeError("%s changed while being read" % description)
            _path, check = self._open_run_relative(
                run_name, _contract_text("worker_shutdown.json"), directory=False, missing_ok=False)
            try:
                first, second = os.fstat(descriptor), os.fstat(check)
                identity = lambda item: (item.st_dev, item.st_ino, item.st_size,
                    getattr(item, "st_mtime_ns", item.st_mtime),
                    getattr(item, "st_ctime_ns", item.st_ctime), item.st_nlink)
                if identity(before) != identity(first) or identity(first) != identity(second):
                    raise RuntimeError("%s changed while being read" % description)
            finally:
                os.close(check)
            return result
        finally:
            os.close(descriptor)

    def _list_run_directory(self, run_name, name):
        _path, descriptor = self._open_run_relative(
            run_name, name, directory=True, missing_ok=True,
        )
        if descriptor is None:
            return None
        try:
            return os.listdir(descriptor)
        finally:
            os.close(descriptor)

    def read(self, run_name, name):
        value = (self._read_shutdown_evidence(run_name, "worker shutdown")
                 if name == _contract_text("worker_shutdown.json") else self._read_run_json(run_name, name))
        if name == _contract_text("result.json") and isinstance(value, dict) and "candidate_store" in value:
            reference = value.get("candidate_store")
            if reference is None:
                return value  # Preserve the failed publication summary.
            from .run_data_store import CORE_NAME, dataset_from_reference
            expected = self.run_path(run_name, CORE_NAME)
            if not isinstance(reference, dict) or reference.get("path") != expected:
                raise ValueError("run result core data does not match its owned run directory")
            dataset = dataset_from_reference(reference)
            if (dataset.get("metadata") or {}).get("run_id", run_name) != run_name:
                raise ValueError("run result core identity does not match")
            candidates = [row for row in dataset.get("candidates") or [] if row.get("status") in _TERMINAL_CANDIDATE_STATES]
            by_id = dict((row["candidate_id"], row) for row in candidates)
            result = dict(value)
            result["candidates"] = candidates
            summary = dataset.get("result") or {}
            ids = value.get("pareto_candidate_ids", summary.get("pareto_candidate_ids")) or []
            result["pareto_front"] = [by_id[key] for key in ids if key in by_id]
            representative = value.get("representative_candidate_id", summary.get("representative_candidate_id"))
            result["representative"] = by_id.get(representative)
            result["best"] = result["representative"]
            return result
        return value

    def stop(self, run_name):
        safe_name = self._safe_run_name(run_name)
        with self.lock:
            thread = self.threads.get(safe_name)
            active = bool(thread and thread.is_alive())
        if not active:
            raise ValueError("run is not active in this optimizer service")
        _run_root, run_descriptor = self._open_run_directory(safe_name)
        if not hasattr(os, "O_NOFOLLOW"):
            os.close(run_descriptor)
            raise RuntimeError("secure STOP creation requires O_NOFOLLOW")
        flags = (
            os.O_WRONLY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0) |
            getattr(os, "O_NONBLOCK", 0)
        )
        descriptor = None
        try:
            try:
                descriptor = os.open(
                    "STOP", flags | os.O_CREAT | os.O_EXCL, 0o600,
                    dir_fd=run_descriptor,
                )
            except OSError as error:
                if error.errno != errno.EEXIST:
                    raise ValueError("cannot create safe STOP file: %s" % error)
                try:
                    descriptor = os.open("STOP", flags, dir_fd=run_descriptor)
                except OSError as open_error:
                    raise ValueError("existing STOP path is unsafe: %s" % open_error)
            opened = os.fstat(descriptor)
            if not stat.S_ISREG(opened.st_mode) or int(opened.st_nlink) != 1:
                raise ValueError("STOP path must be a singly linked regular file")
            os.ftruncate(descriptor, 0)
            os.write(descriptor, b"stop requested\n")
            os.fsync(descriptor)
        finally:
            if descriptor is not None:
                os.close(descriptor)
            os.close(run_descriptor)
        return {"run_id": run_name, "stop_requested": True}

    def _read_continuation_json_snapshot(self, continuation_id, pending=False, runtime_root=None):
        runtime_root, runtime_descriptor = self._open_runtime_directory(runtime_root)
        os.close(runtime_descriptor)
        path = (
            continuation_pending_path(runtime_root, continuation_id, create=False)
            if pending else
            continuation_checkpoint_path(runtime_root, continuation_id, create=False)
        )
        try:
            relative = os.path.relpath(path, runtime_root)
        except ValueError as error:
            raise ValueError("continuation snapshot path is invalid: %s" % error)
        if relative == os.pardir or relative.startswith(os.pardir + os.sep):
            raise ValueError("continuation snapshot escapes the runtime root")
        return self._read_runtime_json_snapshot(
            relative,
            "continuation pending journal" if pending else "continuation checkpoint anchor",
            _PENDING_SNAPSHOT_MAX_BYTES if pending else _CHECKPOINT_SNAPSHOT_MAX_BYTES,
            missing_ok=True, runtime_root=runtime_root,
        )

    def _validate_run_status_identity(self, run_name, status, continuation_id=None):
        run_root, run_descriptor = self._open_run_directory(run_name)
        os.close(run_descriptor)
        if not isinstance(status, dict) or status.get("schema_version") != _contract_text("crescendo.run_status.v1"):
            raise RuntimeError("run status schema is not recognized")
        if status.get("run_id") != run_name:
            raise RuntimeError("run status run_id does not match its directory")
        recorded_root = status.get("run_root")
        if (
            not isinstance(recorded_root, str) or not os.path.isabs(recorded_root)
            or os.path.abspath(recorded_root) != run_root
            or os.path.realpath(recorded_root) != run_root
        ):
            raise RuntimeError("run status run_root does not match its directory")
        if continuation_id is not None and status.get("continuation_id") != continuation_id:
            raise RuntimeError("run status continuation_id does not match")
        return run_root

    def _validate_pending_origin(self, checkpoint, continuation_id, requested_run_name,
                                 requested_status=None):
        manifest = checkpoint.get("manifest") or {}
        origin_name = manifest.get("origin_run_id")
        origin_root = manifest.get("origin_run_root")
        if not isinstance(origin_name, str):
            raise RuntimeError("pending checkpoint origin run id is missing or malformed")
        self._safe_run_name(origin_name)
        runtime_root, runtime_descriptor = self._open_runtime_directory(self._runtime_for_run(requested_run_name))
        os.close(runtime_descriptor)
        expected_root = os.path.join(runtime_root, origin_name)
        if (
            not isinstance(origin_root, str) or not os.path.isabs(origin_root)
            or os.path.abspath(origin_root) != expected_root
            or os.path.realpath(origin_root) != expected_root
        ):
            raise RuntimeError("pending checkpoint origin run root does not match")
        if manifest.get("continuation_id") != continuation_id:
            raise RuntimeError("pending checkpoint manifest continuation_id does not match")
        origin_status = (
            requested_status if origin_name == requested_run_name and requested_status is not None
            else self._read_run_json_snapshot(
                origin_name, _contract_text("status.json"), "run status",
                _RUN_STATUS_SNAPSHOT_MAX_BYTES, missing_ok=False,
            )
        )
        validated_root = self._validate_run_status_identity(
            origin_name, origin_status, continuation_id=continuation_id,
        )
        if validated_root != expected_root:
            raise RuntimeError("pending checkpoint origin status root does not match")
        return origin_name

    def _validate_run_checkpoint_snapshot(self, run_name, checkpoint, status=None):
        validate_checkpoint_integrity(checkpoint)
        manifest = checkpoint.get("manifest") or {}
        origin_name = manifest.get("origin_run_id")
        origin_root = manifest.get("origin_run_root")
        continuation_id = manifest.get("continuation_id")
        if origin_name is not None or origin_root is not None:
            if status is None:
                status = self._read_run_json_snapshot(
                    run_name, _contract_text("status.json"), "run status",
                    _RUN_STATUS_SNAPSHOT_MAX_BYTES, missing_ok=False,
                )
            run_root = self._validate_run_status_identity(
                run_name, status,
                continuation_id=str(continuation_id) if continuation_id is not None else None,
            )
            if origin_name != run_name or origin_root != run_root:
                raise RuntimeError("run checkpoint origin does not match its run directory")
        elif (
            status is not None and status.get("continuation_id") is not None
            and continuation_id is not None
            and status.get("continuation_id") != str(continuation_id)
        ):
            raise RuntimeError("run checkpoint continuation_id does not match its status")
        return checkpoint

    def _resolve_run_checkpoint_snapshot(self, run_name, status=None):
        safe_name = self._safe_run_name(run_name)
        runtime_root = self._runtime_for_run(safe_name)
        last_error = None
        for attempt in range(_CHECKPOINT_SNAPSHOT_ATTEMPTS):
            try:
                current_status = (
                    status if attempt == 0 and status is not None else
                    self._read_run_json_snapshot(
                        safe_name, _contract_text("status.json"), "run status",
                        _RUN_STATUS_SNAPSHOT_MAX_BYTES, missing_ok=True,
                    )
                )
                continuation_id = None
                if isinstance(current_status, dict):
                    value = current_status.get("continuation_id")
                    if value is not None:
                        if not isinstance(value, str) or not value:
                            raise RuntimeError("run status continuation_id is malformed")
                        continuation_id = value

                run_checkpoint = None
                if continuation_id is None:
                    run_checkpoint = self._read_run_json_snapshot(
                        safe_name, _contract_text("checkpoint.json"), "run checkpoint",
                        _CHECKPOINT_SNAPSHOT_MAX_BYTES, missing_ok=True,
                    )
                    if run_checkpoint is not None:
                        self._validate_run_checkpoint_snapshot(
                            safe_name, run_checkpoint, status=current_status,
                        )
                        continuation_id = checkpoint_continuation_id(run_checkpoint)

                pending = (
                    self._read_continuation_json_snapshot(
                        continuation_id, pending=True, runtime_root=runtime_root,
                    )
                    if continuation_id is not None else None
                )
                if run_checkpoint is None:
                    run_checkpoint = self._read_run_json_snapshot(
                        safe_name, _contract_text("checkpoint.json"), "run checkpoint",
                        _CHECKPOINT_SNAPSHOT_MAX_BYTES, missing_ok=True,
                    )
                    if run_checkpoint is not None:
                        self._validate_run_checkpoint_snapshot(
                            safe_name, run_checkpoint, status=current_status,
                        )
                        checkpoint_id = checkpoint_continuation_id(run_checkpoint)
                        if continuation_id is None:
                            continuation_id = checkpoint_id
                        elif checkpoint_id != continuation_id:
                            raise RuntimeError(
                                "run checkpoint continuation_id does not match its status"
                            )

                if pending is None and continuation_id is not None:
                    pending = self._read_continuation_json_snapshot(
                        continuation_id, pending=True, runtime_root=runtime_root,
                    )
                if pending is not None:
                    anchored = self._read_continuation_json_snapshot(
                        continuation_id, pending=False, runtime_root=runtime_root,
                    )
                    pending_checkpoint = _validated_pending_checkpoint(
                        pending, continuation_id, run_checkpoint=run_checkpoint,
                        anchored_checkpoint=anchored,
                    )
                    origin_name = self._validate_pending_origin(
                        pending_checkpoint, continuation_id, safe_name,
                        requested_status=current_status,
                    )
                    if origin_name == safe_name:
                        return {
                            "checkpoint": copy.deepcopy(pending_checkpoint),
                            "source": "pending_journal",
                            "revision": _checkpoint_revision(pending_checkpoint),
                        }

                if run_checkpoint is None:
                    return None
                return {
                    "checkpoint": copy.deepcopy(run_checkpoint),
                    "source": "run_checkpoint",
                    "revision": _checkpoint_revision(run_checkpoint),
                }
            except (IOError, OSError, RuntimeError, TypeError, ValueError) as error:
                last_error = error
                if attempt + 1 < _CHECKPOINT_SNAPSHOT_ATTEMPTS:
                    time.sleep(0)
                    continue
                raise
        if last_error is not None:
            raise last_error
        return None

    def _recent_checkpoint_snapshot(self, run_name, status):
        """Cache display-only validation by exact file versions; Resume rereads."""
        root = self.run_path(run_name, "")
        runtime = self._runtime_for_run(run_name)
        cached = self._recent_checkpoint_cache.get(run_name)
        continuation = status.get("continuation_id")
        if not continuation and cached and cached.get("snapshot"):
            continuation = checkpoint_continuation_id(cached["snapshot"]["checkpoint"])
        paths = [os.path.join(root, _contract_text("checkpoint.json")), os.path.join(root, _contract_text("optimization.crescendo"))]
        if continuation:
            paths += [continuation_checkpoint_path(runtime, continuation, create=False),
                      continuation_pending_path(runtime, continuation, create=False)]
        signature = []
        for path in paths:
            try:
                info = os.lstat(path)
                signature.append((path, info.st_dev, info.st_ino, info.st_mode, info.st_nlink, info.st_size,
                                  getattr(info, "st_mtime_ns", info.st_mtime), getattr(info, "st_ctime_ns", info.st_ctime)))
            except FileNotFoundError:
                signature.append((path, None))
        signature = (status.get("continuation_id"), tuple(signature))
        if cached and cached["signature"] == signature:
            return cached["snapshot"]
        snapshot = self._resolve_run_checkpoint_snapshot(run_name, status=status)
        self._recent_checkpoint_cache[run_name] = {"signature": signature, "snapshot": snapshot}
        return snapshot

    @staticmethod
    def _run_recency_key(name):
        match = re.search(r"(?:__|_)([0-9]{8}_[0-9]{6})(?:_|$)", name)
        return (match.group(1) if match else "", name)

    def list_runs(self):
        rows = []
        try:
            _root, root_descriptor = self._open_runtime_directory()
        except (IOError, OSError, RuntimeError, TypeError, ValueError):
            root_descriptor = None
        try:
            with self.lock:
                scope = (self.config.get("runtime") or {}).get("design_root")
                registered = set(name for name in self._run_locations if not scope or
                                 self._run_scopes.get(name) == os.path.realpath(scope))
            names = sorted(registered | (set(os.listdir(root_descriptor)) if root_descriptor is not None else set()),
                           key=self._run_recency_key, reverse=True)
        finally:
            if root_descriptor is not None:
                os.close(root_descriptor)
        cacheable_runs = set()
        for name in names:
            if len(rows) >= 100:
                break
            try:
                status = self._read_run_json_snapshot(
                    name, _contract_text("status.json"), "run status",
                    _RUN_STATUS_SNAPSHOT_MAX_BYTES, missing_ok=True,
                )
            except (IOError, OSError, RuntimeError, TypeError, ValueError):
                continue
            if not isinstance(status, dict):
                continue
            if status.get("environment_test"):
                continue
            row = dict(status)
            row.update({
                "checkpoint_available": False,
                "checkpoint_revision": None,
                "checkpoint_source": None,
                "continuation_id": row.get("continuation_id"),
            })
            checkpoint = None
            try:
                snapshot = self._recent_checkpoint_snapshot(name, status)
                if snapshot is not None:
                    checkpoint = snapshot["checkpoint"]
                    row.update({
                        "checkpoint_available": True,
                        "checkpoint_revision": snapshot["revision"],
                        "checkpoint_source": snapshot["source"],
                        "continuation_id": checkpoint_continuation_id(checkpoint),
                    })
            except (IOError, OSError, RuntimeError, TypeError, ValueError) as error:
                row["checkpoint_error"] = "%s: %s" % (
                    error.__class__.__name__, error,
                )
            checkpoint_spec = (
                checkpoint.get("spec") or {}
                if isinstance(checkpoint, dict) else {}
            )
            if checkpoint_spec.get("environment_test"):
                continue
            checkpoint_state = (
                checkpoint.get("state") or {}
                if isinstance(checkpoint, dict) else {}
            )
            display_name = None
            for candidate_name in (
                    row.get("display_name"), checkpoint_spec.get("display_name"),
                    (checkpoint.get("manifest") or {}).get("display_name")
                    if isinstance(checkpoint, dict) else None):
                try:
                    display_name = _validated_run_display_name(candidate_name)
                except ValueError as error:
                    row["display_name_error"] = str(error)
                    continue
                if display_name is not None:
                    break
            row["display_name"] = display_name or str(row.get("run_id") or name)

            def checkpoint_count(key, status_key):
                if key in checkpoint_state:
                    return _nonnegative_run_count(checkpoint_state.get(key))
                return _nonnegative_run_count(row.get(status_key))

            successful = checkpoint_count("completed_candidates", "completed_candidates")
            simulation_failed = checkpoint_count("failed_candidates", "failed_candidates")
            infrastructure_failed = checkpoint_count(
                "infrastructure_failed_candidates", "infrastructure_failed_candidates",
            )
            total_generations = _nonnegative_run_count(
                checkpoint_spec.get("generations", row.get("total_generations", 0))
            )
            current_generation = _nonnegative_run_count(row.get("generation", 0))
            if checkpoint_state:
                current_generation = max(
                    current_generation,
                    _nonnegative_run_count(checkpoint_state.get("next_generation", 0)),
                )
                active_plan = checkpoint_state.get("active_plan")
                if isinstance(active_plan, dict):
                    current_generation = max(
                        current_generation,
                        _nonnegative_run_count(active_plan.get("generation", -1)) + 1,
                    )
            if total_generations:
                current_generation = min(current_generation, total_generations)
            row.update({
                "current_generation": current_generation,
                "total_generations": total_generations,
                "successful_candidates": successful,
                "simulation_failed_candidates": simulation_failed,
                "infrastructure_failed_candidates": infrastructure_failed,
                "success": successful,
                "simulation_fail": simulation_failed,
                "infrastructure_fail": infrastructure_failed,
            })
            with self.lock:
                thread = self.threads.get(name)
                row["active"] = bool(thread and thread.is_alive())
            if row.get("state") not in _TERMINAL_RUN_STATES:
                cacheable_runs.add(name)
            rows.append(row)
        self._prune_objective_presentations(cacheable_runs)
        return rows[:100]

    def browse(self, path=None):
        current = os.path.realpath(os.path.abspath(os.path.expanduser(path or "~")))
        if os.path.isfile(current) and os.path.basename(current) in ("maestro.sdb", "data.sdb"):
            current = os.path.dirname(current)
        if not os.path.isdir(current):
            raise ValueError("directory not found: %s" % current)
        entries = []
        try:
            children = list(os.scandir(current))
        except OSError as error:
            raise ValueError("cannot browse %s: %s" % (current, error))
        for child in children:
            if child.name.startswith("."):
                continue
            try:
                is_directory = child.is_dir(follow_symlinks=True)
            except OSError:
                continue
            if not is_directory:
                continue
            child_path = os.path.realpath(child.path)
            setup_info = _directory_setup_info(child_path)
            entries.append({
                "name": child.name,
                "path": child_path,
                "is_setup": setup_info["is_setup"],
                "is_maestro": setup_info["setup_backend"] == "maestro",
                "setup_backend": setup_info["setup_backend"],
                "setup_db": setup_info["setup_db"],
                "ambiguous_setup": setup_info["ambiguous"],
            })
        entries.sort(key=lambda row: (not row["is_setup"], row["name"].lower()))
        parent = os.path.dirname(current)
        current_setup = _directory_setup_info(current)
        return {
            "path": current,
            "parent": parent if parent != current else None,
            "is_setup": current_setup["is_setup"],
            "is_maestro": current_setup["setup_backend"] == "maestro",
            "setup_backend": current_setup["setup_backend"],
            "setup_db": current_setup["setup_db"],
            "ambiguous_setup": current_setup["ambiguous"],
            "entries": entries,
        }

    def native_browse(self, path=None):
        zenity = shutil.which("zenity")
        if not zenity:
            raise RuntimeError("Zenity is not installed")
        default_root = os.path.expanduser("~")
        current = os.path.realpath(os.path.abspath(os.path.expanduser(path or default_root)))
        if os.path.isfile(current):
            current = os.path.dirname(current)
        if not os.path.isdir(current):
            current = default_root
        setup_info = _directory_setup_info(current)
        suggested = setup_info["setup_db"] or current + os.sep
        user, environment = _desktop_session()
        command = ["runuser", "-u", user, "--", "env"]
        command.extend("%s=%s" % item for item in sorted(environment.items()))
        command.extend([
            zenity,
            "--file-selection",
            "--title=Select Cadence ADE SetupDB",
            "--filename=%s" % suggested,
            "--file-filter=ADE SetupDB | maestro.sdb data.sdb",
            "--file-filter=All files | *",
        ])
        try:
            selected = subprocess.check_output(command, stderr=subprocess.STDOUT, timeout=300).decode("utf-8", "replace").strip()
        except subprocess.CalledProcessError as error:
            if error.returncode == 1:
                return {"cancelled": True}
            raise RuntimeError("native file dialog failed: %s" % error.output.decode("utf-8", "replace").strip())
        if not selected:
            return {"cancelled": True}
        selected = os.path.realpath(selected)
        if os.path.basename(selected) in ("maestro.sdb", "data.sdb") and os.path.isfile(selected):
            selected = os.path.dirname(selected)
        setup_info = _directory_setup_info(selected)
        if setup_info["ambiguous"]:
            raise ValueError("setup directory contains both maestro.sdb and data.sdb; select one file")
        if not setup_info["is_setup"]:
            raise ValueError("select a Maestro maestro.sdb or ADE XL data.sdb file")
        return {
            "cancelled": False,
            "path": selected,
            "setup_db": setup_info["setup_db"],
            "setup_backend": setup_info["setup_backend"],
            "maestro_sdb": setup_info["setup_db"] if setup_info["setup_backend"] == "maestro" else None,
        }

    def _execution_log_tail(self, run_name, relative, limit):
        path, descriptor = self._open_run_relative(run_name, relative, missing_ok=True)
        if descriptor is None:
            return None
        try:
            before = os.fstat(descriptor)
            if int(before.st_nlink) != 1:
                raise ValueError("execution log must be a singly linked regular file")
            offset = max(0, int(before.st_size) - int(limit))
            os.lseek(descriptor, offset, os.SEEK_SET)
            raw = os.read(descriptor, int(limit))
            after = os.fstat(descriptor)
            if not os.path.samestat(before, after) or int(after.st_nlink) not in (0, 1):
                raise ValueError("execution log identity changed while being read")
            return {"path": path, "text": _execution_text(raw.decode("utf-8", "replace"), limit),
                    "truncated": offset > 0}
        finally:
            os.close(descriptor)

    def _execution_progress(self, run_name, status, spec, warnings):
        """Read only known bounded files; never execute commands or inspect bootstrap."""
        run_root, descriptor = self._open_run_directory(run_name)
        os.close(descriptor)
        payload = {
            "schema_version": _contract_text("crescendo.execution_progress.v1"), "run_root": run_root,
            "status_file": os.path.join(run_root, _contract_text("status.json")),
            "error_file": os.path.join(run_root, _contract_text("server_error.json")),
            "startup_error_file": os.path.join(run_root, _contract_text("startup_failure.json")),
            "main_log_file": os.path.join(run_root, _contract_text("crescendo.log")),
            "stage": _execution_text(status.get("state"), 120),
            "message": _execution_text(status.get("message"), 4096),
            "commands": [], "workers": [], "log_tails": [], "warnings": warnings,
        }

        def warn(relative, error):
            if len(warnings) < 16:
                warnings.append(_execution_text("%s: %s" % (relative, error), 512))

        def read(relative, limit=1024 * 1024):
            try:
                return self._read_run_json_snapshot(run_name, relative, "execution state", limit)
            except (IOError, OSError, RuntimeError, TypeError, ValueError) as error:
                warn(relative, error)
                return None

        def names(relative, pattern, count=4):
            try:
                return sorted((name for name in self._list_run_directory(run_name, relative) or []
                               if pattern.match(name)), reverse=True)[:count]
            except (IOError, OSError, RuntimeError, TypeError, ValueError) as error:
                warn(relative, error)
                return []

        remaining = [16384]

        def logs(relative, filenames):
            for filename in filenames:
                if remaining[0] <= 0:
                    return
                path = os.path.join(relative, filename)
                try:
                    row = self._execution_log_tail(run_name, path, min(4096, remaining[0]))
                    if row is not None:
                        encoded = row["text"].encode("utf-8")
                        if len(encoded) > remaining[0]:
                            row["text"] = encoded[:remaining[0]].decode("utf-8", "ignore")
                            row["truncated"] = True
                        payload["log_tails"].append(row)
                        remaining[0] -= len(row["text"].encode("utf-8"))
                except (IOError, OSError, RuntimeError, TypeError, ValueError) as error:
                    warn(path, error)

        try:
            main_log = self._execution_log_tail(run_name, _contract_text("crescendo.log"), 16384)
            if main_log is not None:
                payload["log_tails"].append(main_log)
                remaining[0] = 0
        except (IOError, OSError, RuntimeError, TypeError, ValueError) as error:
            warn(_contract_text("crescendo.log"), error)
        context = spec.get(_contract_text("cr_execution_context")) or status.get("execution_context") or {}
        if not isinstance(context, dict):
            context = {}
        command = context.get("configured_command")
        if command is None:
            overrides = spec.get("maestro_job_overrides") or {}
            command = overrides.get("command") if isinstance(overrides, dict) else None
        if isinstance(command, str):
            payload["commands"].append({
                "kind": "configured_submit", "label": "Configured Maestro submission command (not an execution record)",
                "command": _execution_text(command, 8192), "state": "configured", "source": "Maestro Job Setup",
                "max_jobs": _execution_text(context.get("configured_max_jobs"), 32),
            })
        live = status.get("maestro_live_progress") or {}
        if isinstance(live, dict):
            payload["stage"] = "native_batch_monitor" if live.get("batch_id") else payload["stage"]
            for submission in (live.get("shared_submissions") or [])[-6:]:
                if not isinstance(submission, dict) or not isinstance(submission.get("argv"), list):
                    continue
                payload["commands"].append({
                    "kind": "native_submission", "label": "Native submission · shared worker pool/batch",
                    "state": "observed_submission", "argv": [_execution_text(value, 4096) for value in submission["argv"][:64]],
                    "source": os.path.join(run_root, _contract_text("native_dispatch.jsonl")),
                    "invocation_id": _execution_text(submission.get("invocation_id"), 120),
                    "returncode": submission.get("returncode"),
                    "job_ids": [_execution_text(value, 120) for value in (submission.get("job_ids") or [])[:32]],
                    "original_command": _execution_text(submission.get("original_command"), 8192),
                    "stdout": _execution_text(submission.get("stdout_tail") or submission.get("stdout") or
                                              submission.get("output") or (submission.get("text") if submission.get("stream") == "stdout" else None), 2048),
                    "stderr": _execution_text(submission.get("stderr_tail") or submission.get("stderr") or
                                              (submission.get("text") if submission.get("stream") == "stderr" else None), 2048),
                    "scope": "shared_native_worker_pool", "batch_id": live.get("batch_id"),
                })
        error = read(_contract_text("server_error.json"), 1024 * 1024)
        if isinstance(error, dict) or status.get("state") == "failed":
            error = error if isinstance(error, dict) else {}
            payload["error"] = {
                "type": _execution_text(error.get("error_type"), 120),
                "message": _execution_text(error.get("message") or status.get("message"), 4096),
                "traceback_tail": _execution_text(error.get("traceback"), 8192),
                "remote_traceback_tail": _execution_text(error.get("remote_traceback"), 8192),
                "path": payload["error_file"] if error else payload["status_file"],
            }
        startup = read(_contract_text("startup_failure.json"), 128 * 1024)
        if isinstance(startup, dict) and startup.get("schema_version") == _contract_text("crescendo.startup_failure.v1"):
            marker = startup.get("marker") or {}
            if not isinstance(marker, dict):
                marker = {}
            payload["startup_diagnostic"] = {
                "path": payload["startup_error_file"],
                "stage": _execution_text(startup.get("stage"), 120),
                "message": _execution_text(startup.get("message"), 4096),
                "native_error": _execution_text(startup.get("native_error"), 1024),
                "recorded_at": _execution_text(startup.get("recorded_at"), 120),
                "marker_message": _execution_text(marker.get("message"), 1024),
            }
            if "error" in payload:
                payload["error"].update({
                    "startup_cause": payload["startup_diagnostic"]["native_error"],
                    "startup_stage": payload["startup_diagnostic"]["stage"],
                    "startup_diagnostic_path": payload["startup_error_file"],
                })
            diagnostic = startup.get("diagnostic") or {}
            log_paths = startup.get("log_paths") or {}
            if isinstance(diagnostic, dict):
                for name in ("ocean.stderr", "ocean.stdout"):
                    raw = diagnostic.get(name)
                    if not isinstance(raw, str) or not raw or remaining[0] <= 0:
                        continue
                    text = _execution_text(raw, min(16384, remaining[0]))
                    text = text.encode("utf-8")[:remaining[0]].decode("utf-8", "ignore")
                    original = log_paths.get(name) if isinstance(log_paths, dict) else None
                    if not (isinstance(original, str) and os.path.isabs(original)
                            and os.path.normpath(original) == original
                            and os.path.commonpath((run_root, original)) == run_root):
                        original = None
                    payload["log_tails"].append({
                        "path": original or payload["startup_error_file"], "text": text,
                        "retained": True, "source": payload["startup_error_file"],
                        "truncated": len(raw.encode("utf-8")) > len(text.encode("utf-8")),
                    })
                    remaining[0] -= len(text.encode("utf-8"))
        elif startup is not None:
            warn(_contract_text("startup_failure.json"), "unsupported retained startup diagnostic schema")
        if spec.get("execution_backend") in ("private_netlist", "maestro_netlist"):
            progress_relative = os.path.join("private_netlist", _contract_text("progress.json"))
            private_progress = read(progress_relative, 512 * 1024)
            if isinstance(private_progress, dict) and private_progress.get("schema_version") == _contract_text("crescendo.private_netlist_optimizer.v1"):
                latest = private_progress.get("latest") or {}
                from .ui_gates import gate_progress
                gates = gate_progress(private_progress.get("gate_progress") or
                                      (latest.get("gate_progress") if isinstance(latest, dict) else None))
                if gates:
                    payload["gate_progress"] = gates
                if isinstance(latest, dict):
                    payload["stage"] = _execution_text(latest.get("stage") or payload["stage"], 120)
                    payload["message"] = _execution_text(latest.get("message") or payload["message"], 4096)
                for event in reversed((private_progress.get("commands") or [])[-7:]):
                    if not isinstance(event, dict) or not isinstance(event.get("argv"), list):
                        continue
                    stage = _execution_text(event.get("stage"), 120)
                    command_state = {"command_render": "prepared", "submit": "submitted", "solve_completed": "completed",
                                     "spectre_command": "prepared", "solver_command": "prepared", "spectre_completed": "completed"}.get(stage, "recorded")
                    kind = "submission" if stage == "submit" else "solver" if (event.get("solver_command_plan") or stage in ("command_render", "solve", "solve_completed", "spectre_command", "spectre_completed")) else "netlist_generation"
                    payload["commands"].append({
                        "kind": kind, "label": {"solver": "Solver command", "submission": "Submission / Local execution command",
                                                "netlist_generation": "Netlist generation command"}[kind], "state": command_state,
                        "argv": [_execution_text(item, 2048) for item in event["argv"][:64]],
                        "source": os.path.join(run_root, progress_relative),
                        "stage": stage,
                        "candidate_id": _execution_text(event.get("candidate_id"), 120),
                        "test": _execution_text(event.get("test"), 120),
                        "corner": _execution_text(event.get("corner"), 120),
                        "cwd": _execution_text(event.get("cwd"), 4096),
                        "remote": event.get("remote"),
                    })
        for slot in names("controllers", _CONTROLLER_SLOT_NAME):
            slot_relative = os.path.join("controllers", slot)
            for epoch in names(slot_relative, _EPOCH_NAME, 1):
                relative = os.path.join(slot_relative, epoch)
                state = read(os.path.join(relative, _contract_text("controller_state.json")))
                if isinstance(state, dict) and state.get("schema_version") == _CONTROLLER_SCHEMA:
                    phase = state.get("startup_phase") or {}
                    request = state.get("active_request") or {}
                    if not isinstance(phase, dict):
                        phase = {}
                    if not isinstance(request, dict):
                        request = {}
                    payload["workers"].append({
                        "worker_index": int(_CONTROLLER_SLOT_NAME.match(slot).group(1)),
                        "controller_epoch": int(_EPOCH_NAME.match(epoch).group(1)),
                        "status": _execution_text(state.get("status"), 120),
                        "operation": _execution_text(request.get("operation"), 120),
                        "stage": _execution_text(phase.get("stage"), 120),
                    })
                    argv = state.get("command")
                    bootstrap = os.path.join(run_root, relative, _contract_text("bootstrap.json"))
                    if _controller_command_matches(argv, bootstrap):
                        payload["commands"].append({
                            "kind": "controller", "label": "Recorded controller launch", "state": "recorded",
                            "argv": [_execution_text(item, 4096) for item in argv],
                            "source": os.path.join(run_root, relative, _contract_text("controller_state.json")),
                        })
                logs(relative, ("controller.stderr", "controller.stdout"))
        for worker in names("workers", re.compile(r"^worker_(\d+)$")):
            worker_relative = os.path.join("workers", worker)
            for epoch in names(worker_relative, re.compile(r"^epoch_(?:\d+|c\d+_l\d+)$"), 1):
                namespace = parse_worker_epoch_namespace(worker, epoch)
                if namespace is None:
                    continue
                relative = os.path.join(worker_relative, epoch)
                state = read(os.path.join(relative, _contract_text("worker_state.json")))
                if not isinstance(state, dict) or state.get("epoch") != namespace["epoch_identity"]:
                    continue
                control = persistent_worker_control_root(os.path.join(run_root, relative), namespace["epoch_identity"])
                control_relative = os.path.relpath(control, run_root)
                payload["workers"].append({
                    "worker_index": namespace["worker_index"],
                    "worker_state": _execution_text(state.get("state"), 120),
                    "message": _execution_text(state.get("message"), 1024),
                })
                guardian = read(os.path.join(control_relative, _contract_text("guardian_state.json")))
                policy = read(os.path.join(control_relative, _contract_text("effective_job_policy.json")), 4 * 1024 * 1024)
                if isinstance(policy, dict):
                    actual = (policy.get("global") or {}).get("properties") or {}
                    wrappers = policy.get("command_wrappers") or []
                    original = next((row.get("original_command") for row in wrappers
                                     if isinstance(row, dict) and row.get("test") is None), None)
                    payload["commands"].append({
                        "kind": "native_job_readback", "label": "Native Maestro Job Setup readback",
                        "state": "verified" if policy.get("ok") is True else "readback_failed",
                        "command": _execution_text(actual.get("jobsubmitcommand"), 8192),
                        "original_command": _execution_text(original, 8192),
                        "distribution": _execution_text(actual.get("distributionmethod"), 120),
                        "max_jobs": actual.get("maxjobs"),
                        "run_mode": (policy.get("run_options") or {}).get("RunIn"),
                        "source": os.path.join(control, _contract_text("effective_job_policy.json")),
                    })
                argv = guardian.get("argv") if isinstance(guardian, dict) else None
                # Native Ocean uses this exact two-argument launch. Do not expose
                # the guardian wrapper argv containing the private token / env.
                if isinstance(argv, list) and len(argv) == 2 and argv[1] == "-nograph" and isinstance(argv[0], str):
                    payload["commands"].append({
                        "kind": "ocean", "label": "Recorded Ocean launch", "state": "recorded",
                        "argv": [_execution_text(item, 4096) for item in argv],
                        "source": os.path.join(control, _contract_text("guardian_state.json")),
                    })
                logs(control_relative, ("ocean.stderr", "ocean.stdout"))
        payload["commands"] = payload["commands"][:8]
        return payload

    def _core_progress(self, run_name, since_revision=0, include_candidates=True):
        from .run_data_store import CORE_NAME, RunDataReader
        path, descriptor = self._open_run_relative(run_name, CORE_NAME, missing_ok=True)
        if descriptor is None:
            return None
        opened_identity = os.fstat(descriptor)
        file_identity = (opened_identity.st_dev, opened_identity.st_ino)
        os.close(descriptor)
        with self._run_data_reader_lock:
            entry = self._run_data_readers.get(run_name)
            reset = entry is not None and entry.get("file_identity") != file_identity
            if entry is None or entry["path"] != path or reset:
                entry = {"path": path, "reader": RunDataReader(path), "revision": None,
                         "presentation": None, "file_identity": file_identity}
                self._run_data_readers[run_name] = entry
            payload = entry["reader"].read(since_revision=0 if reset else since_revision,
                                          include_candidates=include_candidates or reset,
                                          include_metadata=entry["revision"] is None)
            if reset or payload.get("reset") or since_revision > payload.get("revision", 0):
                payload = entry["reader"].read(include_candidates=True, include_metadata=True)
                payload["reset"] = True
            if payload.get("metadata") is not None:
                entry["metadata"] = payload["metadata"]
            payload["metadata"] = entry.get("metadata") or {}
            spec = payload["metadata"].get("spec") or {}
            revision = payload.get("revision")
            if entry["revision"] is None or entry["revision"] != revision and payload.get("changed_candidates"):
                rows = payload.get("candidates")
                if rows is None:
                    rows = entry["reader"].read(include_candidates=True).get("candidates") or []
                entry["presentation"] = (build_objective_presentation(rows, spec.get("objectives") or [])
                                         if spec.get("objectives") else None)
            entry["revision"] = revision
            payload["objective_presentation"] = entry["presentation"]
            return payload

    def progress_delta(self, run_name, since_revision=0):
        return self.progress(run_name, since_revision=since_revision, include_candidates=not bool(since_revision))

    def export_results(self, run_name, path):
        from .run_data_store import CORE_NAME, export_dataset
        source, descriptor = self._open_run_relative(run_name, CORE_NAME, missing_ok=False)
        os.close(descriptor)
        export_dataset(source, path)
        return {"path": os.path.abspath(path), "run_id": run_name}

    def progress(self, run_name, since_revision=0, include_candidates=True):
        try:
            status = self.read(run_name, _contract_text("status.json"))
        except Exception:
            self._invalidate_objective_presentation(run_name)
            raise
        if not status:
            self._invalidate_objective_presentation(run_name)
            raise ValueError("run not found: %s" % run_name)
        warnings = []
        def optional_read(relative, limit):
            try:
                return self._read_run_json_snapshot(run_name, relative, "progress metadata", limit)
            except (IOError, OSError, RuntimeError, TypeError, ValueError) as error:
                if len(warnings) < 16:
                    warnings.append(_execution_text("%s: %s" % (relative, error), 512))
                return None
        spec = optional_read(_contract_text("spec.json"), 4 * 1024 * 1024) or {}
        if not isinstance(spec, dict):
            warnings.append("spec.json is not an object")
            spec = {}
        try:
            core = self._core_progress(run_name, since_revision=since_revision, include_candidates=include_candidates)
        except (IOError, OSError, RuntimeError, TypeError, ValueError) as error:
            core = None
            warnings.append(_execution_text(_contract_text("optimization.crescendo: %s") % error, 512))
        if core is not None:
            generation_timings = (core.get("result") or {}).get("generation_timings")
            if generation_timings:
                status = dict(status, generation_timings=generation_timings)
            metadata = core.get("metadata") or {}
            if metadata.get("spec"):
                spec = metadata["spec"]
            payload = {"status": status, "spec": spec, "revision": core.get("revision"),
                       "changed_candidates": core.get("changed_candidates") or [],
                       "objective_presentation": core.get("objective_presentation"),
                       "dataset_path": self.run_path(run_name, _contract_text("optimization.crescendo")),
                       "execution": self._execution_progress(run_name, status, spec, warnings)}
            if include_candidates or core.get("reset"):
                payload["candidates"] = core.get("candidates") or []
                payload["reset"] = bool(core.get("reset"))
            return payload
        candidates_by_id = {}
        checkpoint = optional_read(_contract_text("checkpoint.json"), 64 * 1024 * 1024)
        if checkpoint:
            try:
                for index, generation, values in _checkpoint_planned_candidates(checkpoint):
                    candidate_id = "g%03d_c%04d" % (generation, index)
                    candidates_by_id[candidate_id] = {
                        "candidate_id": candidate_id, "generation": generation, "index": index,
                        "status": "planned", "values": values, "metrics": {},
                    }
            except (AttributeError, KeyError, TypeError, ValueError, IndexError, OverflowError) as error:
                warnings.append(_execution_text("checkpoint candidate plan: %s" % error, 512))
            try:
                candidates_by_id.update(
                    (row["candidate_id"], row) for row in checkpoint_candidates(checkpoint)
                )
            except (AttributeError, KeyError, TypeError, ValueError, IndexError, OverflowError) as error:
                warnings.append(_execution_text("checkpoint candidates: %s" % error, 512))
        try:
            candidate_names = self._list_run_directory(run_name, "candidates")
        except (IOError, OSError, RuntimeError, TypeError, ValueError) as error:
            warnings.append(_execution_text("candidate directory: %s" % error, 512))
            candidate_names = []
        for name in candidate_names or []:
            if not _CANDIDATE_NAME.match(name):
                continue
            row = optional_read(os.path.join("candidates", name, _contract_text("candidate.json")), 16 * 1024 * 1024)
            if isinstance(row, dict) and row.get("candidate_id") == name:
                if all(isinstance(row.get(key, -1), int) and not isinstance(row.get(key), bool)
                       for key in ("generation", "index")):
                    candidates_by_id[row["candidate_id"]] = row
                elif len(warnings) < 16:
                    warnings.append("%s/candidate.json has invalid generation or index" % name)
        candidates = list(candidates_by_id.values())
        candidates.sort(key=lambda row: (row.get("generation", -1), row.get("index", -1), row.get("candidate_id", "")))
        presentation = None
        if spec.get("objectives"):
            try:
                presentation = self._cached_objective_presentation(
                    run_name, candidates, spec.get("objectives") or [],
                )
            except (TypeError, ValueError):
                self._invalidate_objective_presentation(run_name)
                presentation = None
        if status.get("state") in _TERMINAL_RUN_STATES:
            self._invalidate_objective_presentation(run_name)
        return {
            "status": status, "spec": spec, "candidates": candidates,
            "objective_presentation": presentation,
            "execution": self._execution_progress(run_name, status, spec, warnings),
        }

    @staticmethod
    def _safe_candidate_name(candidate_id):
        if not isinstance(candidate_id, str) or not _CANDIDATE_NAME.match(candidate_id):
            raise ValueError("invalid candidate id")
        return candidate_id

    def _persistent_log_fence_matches(self, run_name, fence):
        if not isinstance(fence, dict):
            return True
        expected = fence.get("protocol_identity")
        if not isinstance(expected, dict) or not expected:
            return False
        state = self._read_run_json_snapshot(
            run_name, fence.get("state_relative") or "",
            "persistent worker live-log state", 1024 * 1024,
        )
        if not isinstance(state, dict):
            return False
        if state.get("schema_version") != _contract_text("crescendo.persistent_maestro_worker.v1"):
            return False
        try:
            if int(state.get("worker_index", -1)) != int(fence.get("worker_index", -2)):
                return False
        except (TypeError, ValueError, OverflowError):
            return False
        if state.get("epoch") != fence.get("worker_epoch"):
            return False
        worker_state = str(state.get("state") or "").strip().lower()
        if worker_state == "awaiting_cleanup":
            current = state.get("pending_cleanup")
        elif worker_state in ("busy", "cleaning"):
            current = state.get("active_identity")
        else:
            return False
        return isinstance(current, dict) and current == expected

    @staticmethod
    def _candidate_batch_log_scope(candidate, artifact=None):
        candidate = candidate or {}
        if not isinstance(candidate, dict):
            raise ValueError("invalid candidate log metadata")
        evidence = candidate.get("completion_evidence") or candidate.get("completion") or {}
        if not isinstance(evidence, dict):
            raise ValueError("invalid candidate log completion evidence")
        batch = candidate.get("batch_execution")
        if artifact is not None and not isinstance(artifact, dict):
            raise ValueError("invalid candidate log artifact")
        artifact = artifact or {}
        progress = candidate.get("execution_progress") or {}
        if not isinstance(progress, dict):
            raise ValueError("invalid candidate execution progress")
        serial = progress.get("serial_observation") is True or artifact.get("native_serial") is True
        if serial and (batch is not None or "candidate_test_name_map" in evidence
                       or artifact.get("mode") or artifact.get("shared_history")):
            raise ValueError("native serial log identity conflicts with candidate batch metadata")
        progress_mapped = bool(progress.get("source_test_map") and progress.get("batch_id"))
        mapped = "candidate_test_name_map" in evidence
        is_batch = (batch is not None or mapped or progress_mapped or artifact.get("mode") == "isolated_design_tests"
                    or any(key in artifact for key in ("batch_id", "source_test_map", "shared_history")))
        if not is_batch and not serial:
            return None
        mappings, batch_ids = [], []
        if batch is not None:
            if not isinstance(batch, dict) or batch.get("shared_history") is not True:
                raise ValueError("invalid candidate batch log identity")
            mappings.append(batch.get("source_test_map"))
            batch_ids.append(batch.get("batch_id"))
        if mapped:
            mappings.append(evidence.get("candidate_test_name_map"))
        if progress_mapped:
            mappings.append(progress["source_test_map"])
            batch_ids.append(progress["batch_id"])
        if artifact:
            if not serial and (artifact.get("mode") != "isolated_design_tests" or artifact.get("shared_history") is not True):
                raise ValueError("invalid candidate batch log artifact")
            if candidate.get("candidate_id") and artifact.get("candidate_id") != candidate["candidate_id"]:
                raise ValueError("candidate batch log artifact belongs to another candidate")
            if not serial or "source_test_map" in artifact:
                mappings.append(artifact.get("source_test_map"))
            if not serial or "batch_id" in artifact:
                batch_ids.append(artifact.get("batch_id"))
        if not mappings:
            raise ValueError("candidate batch test mapping is unavailable")
        for mapping in mappings:
            if not isinstance(mapping, dict) or not mapping or len(mapping) > 4096:
                raise ValueError("invalid candidate batch test mapping")
            for name in list(mapping) + list(mapping.values()):
                if (not isinstance(name, str) or not name or name in (".", "..")
                        or "/" in name or "\\" in name
                        or any(ord(char) < 32 for char in name)):
                    raise ValueError("unsafe candidate batch test mapping")
            if len(set(mapping.values())) != len(mapping):
                raise ValueError("candidate batch test mapping is not bijective")
        if any(mapping != mappings[0] for mapping in mappings[1:]):
            raise ValueError("candidate batch test mappings disagree")
        for batch_id in batch_ids:
            if not isinstance(batch_id, str) or not re.match(r"^[A-Za-z0-9_-]+$", batch_id):
                raise ValueError("invalid candidate batch id")
        if batch_ids and len(set(batch_ids)) != 1:
            raise ValueError("candidate batch ids disagree")
        if serial:
            candidate_id = candidate.get("candidate_id")
            if not candidate_id or not batch_ids or any(value != candidate_id for value in batch_ids):
                raise ValueError("native serial log identity belongs to another candidate")
            if any(name != source for name, source in mappings[0].items()):
                raise ValueError("native serial test mapping must preserve original tests")
            identities = [value for value in (progress.get("protocol_identity"), artifact.get("protocol_identity"))
                          if value is not None]
            for identity in identities:
                if (not isinstance(identity, dict) or identity.get("candidate_id") != candidate_id
                        or not isinstance(identity.get("epoch"), str) or not identity["epoch"]
                        or type(identity.get("sequence")) is not int or identity["sequence"] <= 0
                        or not isinstance(identity.get("nonce"), str) or not identity["nonce"]):
                    raise ValueError("invalid native serial protocol identity")
            if identities and any(value != identities[0] for value in identities[1:]):
                raise ValueError("native serial protocol identities disagree")
            started = progress.get("started_epoch", artifact.get("started_epoch"))
            if started is not None and (isinstance(started, bool) or not isinstance(started, (int, float))
                                        or not math.isfinite(started) or started <= 0):
                raise ValueError("invalid native serial log start time")
            return {"batch_id": candidate_id, "test_name_map": dict(mappings[0]), "serial": True,
                    "protocol_identity": identities[0] if identities else None, "started_epoch": started}
        return {"batch_id": batch_ids[0] if batch_ids else None,
                "test_name_map": dict(mappings[0])}

    def _candidate_serial_log_fence(self, run_name, scope):
        """Serial reuses a native history; a new request revokes old paths."""
        identity = (scope or {}).get("protocol_identity")
        if not isinstance(identity, dict):
            return None
        matches = []
        for worker_name in self._list_run_directory(run_name, "workers") or []:
            worker_index = parse_worker_name(worker_name)
            if worker_index is None:
                continue
            relative = os.path.join("workers", worker_name)
            for epoch_name in self._list_run_directory(run_name, relative) or []:
                namespace = parse_worker_epoch_namespace(worker_name, epoch_name)
                if namespace and namespace.get("epoch_identity") == identity["epoch"]:
                    matches.append({"state_relative": os.path.join(relative, epoch_name, _contract_text("worker_state.json")),
                                    "worker_index": worker_index, "worker_epoch": identity["epoch"],
                                    "protocol_identity": dict(identity)})
        if len(matches) > 1:
            raise ValueError("native serial log identity matches multiple worker epochs")
        return matches[0] if matches and self._persistent_log_fence_matches(run_name, matches[0]) else None

    def _native_candidate_record(self, run_name, candidate_id, description="candidate metadata", strict=False):
        candidate_id = self._safe_candidate_name(candidate_id)
        row = self._read_run_json_snapshot(run_name, os.path.join("candidates", candidate_id, _contract_text("candidate.json")),
                                          description, 64 * 1024 * 1024)
        if row is None and strict:
            from .run_data_store import CORE_NAME, dataset_from_reference
            path, descriptor = self._open_run_relative(run_name, CORE_NAME, missing_ok=True)
            if descriptor is not None:
                os.close(descriptor)
                snapshot = self._resolve_run_checkpoint_snapshot(run_name)
                checkpoint = snapshot["checkpoint"] if snapshot else {}
                reference = checkpoint.get("candidate_store")
                if not isinstance(reference, dict) or reference.get("path") != path:
                    raise ValueError("candidate has no verified fixed core checkpoint prefix")
                dataset = dataset_from_reference(reference, checkpoint.get("candidate_store_embedded"))
                if (dataset.get("metadata") or {}).get("run_id", run_name) != run_name:
                    raise ValueError("candidate core identity does not match its local run")
                row = next((item for item in dataset.get("candidates") or [] if item.get("candidate_id") == candidate_id), None)
        elif not strict:
            payload = self._core_progress(run_name, include_candidates=False)
            if payload is not None:
                with self._run_data_reader_lock:
                    entry = self._run_data_readers.get(run_name)
                    core_row = copy.deepcopy(entry["reader"].rows.get(candidate_id)) if entry else None
                if core_row is not None:
                    # The single-writer core drives the UI. Sidecar metadata
                    # may predate its latest live execution observation.
                    row = core_row
        if row is not None and row.get("candidate_id") != candidate_id:
            raise ValueError("candidate metadata identity does not match")
        return row

    def _candidate_log_roots(self, run_name, candidate_id, candidate=None):
        candidate_id = self._safe_candidate_name(candidate_id)
        candidate_relative = os.path.join("candidates", candidate_id)
        _candidate_root, candidate_descriptor = self._open_run_relative(
            run_name, candidate_relative, directory=True, missing_ok=True,
        )
        if candidate_descriptor is None:
            if candidate and candidate.get("candidate_id") == candidate_id:
                return []
            raise ValueError("candidate not found: %s" % candidate_id)
        os.close(candidate_descriptor)
        roots = []

        def add(relative, fence=None, scope=None):
            _path, descriptor = self._open_run_relative(
                run_name, relative, directory=True, missing_ok=True,
            )
            if descriptor is None:
                return
            os.close(descriptor)
            if not any(row["relative"] == relative for row in roots):
                roots.append({"relative": relative, "fence": fence,
                              "test_name_map": (scope or {}).get("test_name_map")})

        artifact = self._read_run_json_snapshot(
            run_name, os.path.join(candidate_relative, _contract_text("persistent_worker_candidate.json")),
            "persistent worker candidate live-log artifact", 4 * 1024 * 1024,
        )
        if candidate is None:
            candidate = self._read_run_json_snapshot(
                run_name, os.path.join(candidate_relative, _contract_text("candidate.json")),
                "candidate live-log metadata", 16 * 1024 * 1024,
            ) or {}
        scope = self._candidate_batch_log_scope(candidate, artifact)
        if (scope or {}).get("serial"):
            # Serial histories are overwritten between candidates. Only the
            # observer's fresh point paths, not an unfenced tree, are eligible.
            return []
        add(os.path.join(candidate_relative, "workspace", "simulation"), scope=scope)
        if isinstance(artifact, dict):
            protocol_identity = artifact.get("protocol_identity")
            token = str(artifact.get("worker_epoch") or "")
            recorded_worker = artifact.get("worker_index")
            try:
                worker_index = int(recorded_worker)
            except (TypeError, ValueError, OverflowError):
                worker_index = None
            protocol_candidate_matches = isinstance(protocol_identity, dict) and (
                protocol_identity.get("candidate_id") == candidate_id if scope is None else
                protocol_identity.get("candidate_id") == scope.get("batch_id")
            )
            if scope is not None:
                protocol = protocol_identity if isinstance(protocol_identity, dict) else {}
                sequence = protocol.get("sequence")
                nonce = protocol.get("nonce")
                protocol_candidate_matches = bool(
                    protocol_candidate_matches and protocol.get("epoch") == token
                    and isinstance(sequence, int) and not isinstance(sequence, bool) and sequence > 0
                    and isinstance(nonce, str) and nonce
                    and isinstance(recorded_worker, int) and not isinstance(recorded_worker, bool)
                )
            if (
                worker_index is not None and worker_index >= 0 and token
                and artifact.get("candidate_id") == candidate_id
                and protocol_candidate_matches
            ):
                worker_name = "worker_%03d" % worker_index
                worker_relative = os.path.join("workers", worker_name)
                epoch_names = self._list_run_directory(run_name, worker_relative) or []
                matches = []
                for epoch_name in epoch_names:
                    namespace = parse_worker_epoch_namespace(worker_name, epoch_name)
                    if namespace and namespace.get("epoch_identity") == token:
                        matches.append(epoch_name)
                if len(matches) > 1:
                    raise ValueError("candidate worker identity matches multiple epochs")
                if matches:
                    fence = {
                        "state_relative": os.path.join(
                            worker_relative, matches[0], _contract_text("worker_state.json"),
                        ),
                        "worker_index": worker_index,
                        "worker_epoch": token,
                        "protocol_identity": dict(protocol_identity),
                    }
                    if self._persistent_log_fence_matches(run_name, fence):
                        add(os.path.join(
                            worker_relative, matches[0], "workspace", "simulation",
                        ), fence=fence, scope=scope)
        return roots

    @staticmethod
    def _candidate_log_context(candidate):
        context = {}
        physical_context = {}
        evidence = (
            (candidate or {}).get("completion_evidence")
            or (candidate or {}).get("completion") or {}
        )
        scope = OptimizerService._candidate_batch_log_scope(candidate)
        test_name_map = (scope or {}).get("test_name_map") or {}
        physical_by_test = dict((logical, physical) for physical, logical in test_name_map.items())
        rows = list(
                evidence.get("spectre_runs")
                or evidence.get("spectre_logs") or [])
        progress = (candidate or {}).get("execution_progress") or {}
        if isinstance(progress, dict):
            rows.extend(progress.get("tasks") or [])
        for row in rows:
            spectre = row.get("spectre") or {}
            path = str(spectre.get("path") or "").replace("\\", "/")
            point_id = row.get("point_id")
            test = row.get("test")
            physical = row.get("artifact_test") or physical_by_test.get(test, test)
            if test_name_map and test_name_map.get(physical) != test:
                raise ValueError("candidate Spectre log test mapping disagrees")
            if test_name_map and path and (
                    not path.endswith("/psf/spectre.out") or path.split("/")[-3] != physical):
                raise ValueError("candidate Spectre log path does not match its test")
            row_context = {
                "corner": row.get("corner"), "test": test,
                "point_id": row.get("point_id"), "evidence_path": path,
            }
            point_token = "" if point_id is None else str(point_id)
            context[(str(test or ""), point_token)] = row_context
            physical_context[(str(physical or ""), point_token)] = row_context
        # File descriptors are indexed by physical names. Preserve that
        # identity even if a source test name happens to equal another alias.
        context.update(physical_context)
        return context

    def _candidate_spectre_files(self, run_name, root_relative, inspection, allowed_tests=None):
        _root_path, root_descriptor = self._open_run_relative(
            run_name, root_relative, directory=True, missing_ok=True,
        )
        if root_descriptor is None:
            return []
        results = []

        def visit(directory_descriptor, suffix, depth):
            inspection[0] += 1
            if inspection[0] > 5000:
                raise ValueError("candidate log tree exceeds the safe inspection limit")
            try:
                entries = os.scandir("/proc/self/fd/%d" % directory_descriptor)
            except OSError as error:
                raise ValueError("candidate log directory cannot be inspected: %s" % error)
            try:
                for entry in entries:
                    inspection[1] += 1
                    if inspection[1] > 100000:
                        raise ValueError(
                            "candidate log tree exceeds the safe entry limit"
                        )
                    name = entry.name
                    if not isinstance(name, str) or not name or name in (".", ".."):
                        continue
                    try:
                        observed = os.stat(
                            name, dir_fd=directory_descriptor,
                            follow_symlinks=False,
                        )
                    except OSError:
                        continue
                    if stat.S_ISDIR(observed.st_mode):
                        if depth >= 24:
                            continue
                        try:
                            child_descriptor = os.open(
                                name, self._directory_open_flags(),
                                dir_fd=directory_descriptor,
                            )
                        except OSError:
                            continue
                        try:
                            visit(child_descriptor, suffix + [name], depth + 1)
                        finally:
                            os.close(child_descriptor)
                        continue
                    if name != "spectre.out" or not stat.S_ISREG(observed.st_mode):
                        continue
                    if int(observed.st_nlink) != 1:
                        continue
                    if allowed_tests is not None and (
                            len(suffix) < 2 or suffix[-1] != "psf" or suffix[-2] not in allowed_tests):
                        continue
                    results.append({
                        "relative_path": os.path.join(root_relative, *(suffix + [name])),
                        "size_bytes": int(observed.st_size),
                    })
                    if len(results) > 256:
                        raise ValueError("candidate exposes too many Spectre logs")
            finally:
                entries.close()

        try:
            visit(root_descriptor, [], 0)
            return results
        finally:
            os.close(root_descriptor)

    @staticmethod
    def _read_retained_spectre_meta(candidate_descriptor, name):
        flags = (
            os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NONBLOCK", 0)
        )
        try:
            descriptor = os.open(name, flags, dir_fd=candidate_descriptor)
        except OSError:
            return {}
        try:
            observed = os.fstat(descriptor)
            if (
                    not stat.S_ISREG(observed.st_mode)
                    or int(observed.st_nlink) != 1
                    or int(observed.st_size) > 16384):
                return {}
            chunks = []
            remaining = int(observed.st_size)
            while remaining > 0:
                chunk = os.read(descriptor, min(remaining, 4096))
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            after = os.fstat(descriptor)
            if not os.path.samestat(observed, after) or int(after.st_nlink) != 1:
                return {}
            rows = {}
            for line in b"".join(chunks).decode("utf-8", "replace").splitlines():
                if "=" not in line:
                    continue
                key, value = line.split("=", 1)
                if key in (
                        "schema_version", "source_path", "source_cwd",
                        "returncode", "finished_epoch", "candidate_id", "artifact_test", "point_id"):
                    rows[key] = value
            if rows.get("schema_version") != _contract_text("crescendo.retained_spectre_log.v1"):
                return {}
            return rows
        finally:
            os.close(descriptor)

    def _candidate_retained_spectre_descriptors(self, run_name,
                                                candidate_relative, context,
                                                test_name_map=None):
        _candidate_path, candidate_descriptor = self._open_run_relative(
            run_name, candidate_relative, directory=True, missing_ok=True,
        )
        if candidate_descriptor is None:
            return []
        results = []
        try:
            entries = os.scandir("/proc/self/fd/%d" % candidate_descriptor)
            try:
                for entry in entries:
                    match = re.match(
                        r"^retained_spectre_([A-Za-z0-9_-]+)\.log$",
                        entry.name,
                    )
                    if not match:
                        continue
                    try:
                        observed = os.stat(
                            entry.name, dir_fd=candidate_descriptor,
                            follow_symlinks=False,
                        )
                    except OSError:
                        continue
                    if (
                            not stat.S_ISREG(observed.st_mode)
                            or int(observed.st_nlink) != 1
                            or int(observed.st_size) <= 0):
                        continue
                    token = match.group(1)
                    meta = self._read_retained_spectre_meta(
                        candidate_descriptor,
                        "retained_spectre_%s.meta" % token,
                    )
                    source = os.path.normpath(
                        str(meta.get("source_path") or "")
                    ).replace("\\", "/")
                    parts = source.split("/") if source else []
                    test = "Spectre"
                    point_id = None
                    alias = False
                    if (
                            len(parts) >= 5 and parts[-1] == "spectre.out"
                            and parts[-2] == "psf"):
                        point_token = parts[-4]
                        test = parts[-3]
                        alias = point_token == "psf"
                        if not alias:
                            try:
                                point_id = int(point_token)
                            except (TypeError, ValueError):
                                point_id = point_token
                    if test_name_map is not None and test not in test_name_map:
                        continue
                    physical_test = test
                    row_context = context.get((str(test), "" if point_id is None else str(point_id))) or {}
                    test = row_context.get("test") or (test_name_map or {}).get(test, test)
                    corner = row_context.get("corner")
                    label = str(test)
                    if corner not in (None, ""):
                        label += " · %s" % corner
                    elif point_id not in (None, ""):
                        label += " · point %s" % point_id
                    elif alias:
                        label += " · latest"
                    relative = os.path.join(
                        candidate_relative,
                        "retained_spectre_%s.log" % token,
                    )
                    results.append({
                        "log_id": hashlib.sha256(
                            relative.encode("utf-8")
                        ).hexdigest()[:24],
                        "label": label, "test": test, "corner": corner,
                        "artifact_test": physical_test,
                        "point_id": point_id, "kind": "spectre_retained",
                        "state": "available", "size_bytes": int(observed.st_size),
                        "relative_path": relative, "fence": None,
                        "returncode": meta.get("returncode"),
                    })
                    if len(results) > 256:
                        raise ValueError("candidate exposes too many retained Spectre logs")
            finally:
                entries.close()
        finally:
            os.close(candidate_descriptor)
        return results

    def _candidate_progress_log_descriptors(self, run_name, candidate_id, candidate, scope):
        """Use the controller's actual task paths, never a whole-run log search.

        Task identity is checked against the isolated native test alias and
        point layout. Every descriptor is reopened and closed for each read;
        viewers therefore do not retain NFS file handles between polls.
        """
        progress = candidate.get("execution_progress") or {}
        if not isinstance(progress, dict):
            raise ValueError("candidate execution progress is invalid")
        tasks = progress.get("tasks") or []
        if not isinstance(tasks, list):
            raise ValueError("candidate task progress is invalid")
        mapping = (scope or {}).get("test_name_map") or {}
        serial = (scope or {}).get("serial") is True
        serial_fence = self._candidate_serial_log_fence(run_name, scope) if serial else None
        run_root = os.path.abspath(self.run_path(run_name, ""))
        common = (candidate.get("execution_backend") in ("private_netlist", "maestro_netlist")
                  and progress.get("schema_version") == _contract_text("crescendo.shared_cli_progress.v1"))
        results = []

        def add(path, kind, label, task=None, shared=False, retained=None):
            if not isinstance(path, str) or not path:
                return
            absolute = os.path.abspath(path if os.path.isabs(path) else os.path.join(run_root, path))
            if common and not shared and retained is None and kind.startswith("scheduler_"):
                expected = os.path.join(run_root, "candidates", candidate_id, "simulation", "tasks", str((task or {}).get("task_id") or ""))
                if not is_under(absolute, expected):
                    warning = "Scheduler log display skipped: %s (outside this exact owned task; configured output location is retained)" % absolute
                    warnings = progress.setdefault("warnings", [])
                    if isinstance(warnings, list) and warning not in warnings and len(warnings) < 8:
                        warnings.append(warning[:4096])
                    return
            raw_common = common and not shared and retained is None
            under_run = absolute.startswith(run_root + os.sep) if raw_common else is_under(absolute, run_root)
            if not under_run or absolute == run_root:
                raise ValueError("candidate diagnostic path is outside its owned run")
            relative = os.path.relpath(absolute, run_root)
            task = task or {}
            fence = None
            fresh_after = None
            if common and not shared:
                task_id = task.get("task_id")
                if (not isinstance(task_id, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", task_id)
                        or task_id in (".", "..")):
                    raise ValueError("shared CLI task log identity is invalid")
                task_base = os.path.join(run_root, "candidates", candidate_id, "simulation", "tasks", task_id)
                if os.path.abspath(task.get("task_root") or "") != task_base:
                    raise ValueError("shared CLI task log directory identity disagrees")
                if retained is not None:
                    from .shared_cli_logs import RETAINED_SCHEMA, retained_cli_name
                    if os.path.dirname(absolute) != os.path.dirname(os.path.dirname(os.path.dirname(task_base))) or not retained_cli_name(os.path.basename(absolute)):
                        raise ValueError("retained shared CLI log is outside its candidate")
                    meta = self._read_run_json_snapshot(run_name, relative[:-4] + ".meta",
                                                       "retained shared CLI log identity", 16384)
                    if meta is None:
                        return  # Normal post-ACK cleanup may retire both snapshot files.
                    if (not isinstance(meta, dict) or meta.get("schema_version") != RETAINED_SCHEMA
                            or meta.get("candidate_id") != candidate_id or meta.get("task_id") != task_id
                            or meta.get("path") != absolute or meta.get("sha256") != retained.get("sha256")
                            or meta.get("execution_simulator") != task.get("execution_simulator")
                            or meta.get("test") != task.get("test") or str(meta.get("point_id")) != str(task.get("point_id"))
                            or not isinstance(meta.get("source_path"), str)
                            or not os.path.abspath(meta['source_path']).startswith(task_base + os.sep)):
                        raise ValueError("retained shared CLI log identity disagrees")
                    if meta.get('leaf_link') and (not isinstance(meta.get('resolved_source_path'), str)
                            or not os.path.abspath(meta['resolved_source_path']).startswith(os.path.abspath(task.get('psf_dir') or '') + os.sep)):
                        raise ValueError('retained shared CLI alias target identity disagrees')
                elif not absolute.startswith(task_base + os.sep):
                    raise ValueError("shared CLI log path is outside its exact task")
                epoch = task.get("launch_epoch")
                if epoch is not None:
                    if type(epoch) not in (int, float) or not math.isfinite(epoch):
                        raise ValueError("shared CLI log freshness identity is invalid")
                    fresh_after = epoch - 2
            elif not shared and kind == "spectre":
                source = relative
                if re.match(r"^retained_spectre_[A-Za-z0-9_-]+\.log$", os.path.basename(relative)):
                    candidate_relative = os.path.join("candidates", candidate_id)
                    if os.path.dirname(relative) != candidate_relative:
                        raise ValueError("retained task log belongs to another candidate")
                    _root, opened = self._open_run_relative(run_name, candidate_relative, directory=True)
                    try:
                        meta = self._read_retained_spectre_meta(opened, os.path.basename(relative)[:-4] + ".meta")
                    finally:
                        os.close(opened)
                    if meta.get("schema_version") != _contract_text("crescendo.retained_spectre_log.v1"):
                        raise ValueError("retained task Spectre log has no native source identity")
                    physical = task.get("artifact_test") or task.get("test")
                    if (meta.get("candidate_id") != candidate_id or meta.get("artifact_test") != physical
                            or meta.get("point_id") != str(task.get("point_id"))):
                        raise ValueError("retained task Spectre metadata identity disagrees")
                    source = str(meta.get("source_path") or "")
                    if not os.path.isabs(source) or not is_under(os.path.abspath(source), run_root):
                        raise ValueError("retained task Spectre source is outside its owned run")
                    kind = "spectre_retained"
                elif serial:
                    fresh_after = scope.get("started_epoch")
                    if serial_fence is None or fresh_after is None:
                        return
                    roots = (progress.get("log_search") or {}).get("roots") or []
                    if (not isinstance(roots, list) or not any(
                            isinstance(root, str) and os.path.isabs(root)
                            and is_under(os.path.abspath(root), run_root)
                            and is_under(absolute, os.path.abspath(root)) for root in roots)):
                        raise ValueError("native serial Spectre path is outside its observed history roots")
                    fence = serial_fence
                parts = source.replace("\\", "/").split("/")
                physical = task.get("artifact_test") or task.get("test")
                if (len(parts) < 4 or parts[-2:] != ["psf", "spectre.out"]
                        or parts[-3] != physical
                        or task.get("point_id") is not None and parts[-4] != str(task["point_id"])):
                    raise ValueError("candidate task Spectre path identity disagrees")
            owned_log_task, log_metadata = None, None
            if common and not shared and retained is None:
                from .owned_solver_logs import OwnedSolverLogError, owned_solver_log_reader
                owned_log_task = {key: task.get(key) for key in ('execution_simulator', 'psf_dir', 'task_root')}
                try:
                    with owned_solver_log_reader(absolute, task_base, epoch, task=owned_log_task) as (descriptor, log_metadata):
                        observed = os.fstat(descriptor)
                except FileNotFoundError:
                    return
                except OwnedSolverLogError as error:
                    warning = 'Log display skipped: %s (%s)' % (absolute, error.reason)
                    warnings = progress.setdefault('warnings', [])
                    if isinstance(warnings, list) and warning not in warnings and len(warnings) < 8:
                        warnings.append(warning[:4096])
                    return
            else:
                _path, descriptor = self._open_run_relative(run_name, relative, missing_ok=True)
                if descriptor is None:
                    return
                try:
                    observed = os.fstat(descriptor)
                finally:
                    os.close(descriptor)
            if int(observed.st_nlink) != 1:
                return
            if fresh_after is not None and observed.st_mtime < fresh_after:
                return
            result = {
                "log_id": hashlib.sha256(relative.encode("utf-8")).hexdigest()[:24],
                "label": label, "kind": kind, "scope": "shared" if shared else "task",
                "state": "available", "size_bytes": int(observed.st_size),
                "relative_path": relative, "fence": fence, "fresh_after": fresh_after,
                "test": task.get("test"), "artifact_test": task.get("artifact_test"),
                "corner": task.get("corner"), "point_id": task.get("point_id"),
                "job_id": task.get("job_id"),
            }
            if retained is not None:
                result.update(snapshot=True, truncated=bool(retained.get("truncated")),
                              file_name=retained.get("file_name"), display_location=retained.get("source_path") + " [retained snapshot]")
            if owned_log_task is not None:
                result['_owned_log_task'] = owned_log_task
                result['leaf_link'] = log_metadata['leaf_link']
                if log_metadata['leaf_link']:
                    result['resolved_location'] = os.path.relpath(log_metadata['resolved_source_path'], run_root)
            results.append(result)

        for task in tasks:
            if not isinstance(task, dict) or task.get("candidate_id", candidate_id) != candidate_id:
                raise ValueError("candidate task identity disagrees")
            physical = task.get("artifact_test") or task.get("test")
            if candidate.get("execution_backend") == "persistent_maestro" and not mapping:
                raise ValueError("candidate native task test mapping is unavailable")
            if mapping and mapping.get(physical) != task.get("test"):
                raise ValueError("candidate task native test mapping disagrees")
            label = str(task.get("test") or "Native task")
            if task.get("corner") not in (None, ""):
                label += " · %s" % task["corner"]
            if task.get("point_id") not in (None, ""):
                label += " · point %s" % task["point_id"]
            simulator = task.get("execution_simulator") or candidate.get("execution_simulator") or (progress.get("spec") or {}).get("simulator") or "spectre"
            add(task.get("simulator_log") or task.get("spectre_log"), simulator, "ALPS · " + label if simulator == "alps" else label, task)
            if common:
                for key, kind, description in (("solver_stdout", simulator + "_stdout", "Solver stdout"),
                        ("solver_stderr", simulator + "_stderr", "Solver stderr"),
                        ("scheduler_stdout", "scheduler_stdout", "Batch scheduler stdout"),
                        ("scheduler_stderr", "scheduler_stderr", "Batch scheduler stderr")):
                    add(task.get(key), kind, description + " · " + label, task)
                psf = task.get("psf_dir")
                if isinstance(psf, str) and psf:
                    add(os.path.join(psf, "logFile"), simulator + "_psf", "PSF logFile · " + label, task)
                    if simulator == "alps":
                        add(psf + ".log", "alps_companion", "ALPS PSF log · " + label, task)
                        add(psf + ".warn", "alps_warning", "ALPS warnings · " + label, task)
                retained = task.get("retained_logs") or {}
                if not isinstance(retained, dict) or not isinstance(retained.get("files", []), list):
                    raise ValueError("shared CLI retained logs are invalid")
                for entry in retained.get("files") or []:
                    if not isinstance(entry, dict):
                        raise ValueError("shared CLI retained log entry is invalid")
                    source = entry.get("source_path")
                    if source and os.path.relpath(os.path.abspath(source), run_root) in {row['relative_path'] for row in results}:
                        continue  # Display the actual growing/live file when available.
                    add(entry.get("path"), str(entry.get("kind") or simulator) + "_retained",
                        "Snapshot " + str(entry.get("file_name") or "log") + " · " + label, task, retained=entry)
            submit_shared = os.path.basename(str(task.get("submit_log") or "")) == _contract_text("native_dispatch.jsonl")
            add(task.get("submit_log"), "submission", "Submission ledger · shared batch" if submit_shared
                else "Submission · " + label, task, shared=submit_shared)
        # Shared diagnostics are explicitly labelled: they are not evidence
        # that this candidate's Spectre task has started.
        add(os.path.join(run_root, _contract_text("crescendo.log")), "optimizer", "Optimizer · shared run", shared=True)
        shared_logs = progress.get("shared_logs") or []
        if not isinstance(shared_logs, list) or len(shared_logs) > 16:
            raise ValueError("candidate shared diagnostics are invalid")
        for entry in shared_logs:
            if not isinstance(entry, dict):
                raise ValueError("candidate shared diagnostic is invalid")
            path = entry.get("path")
            name = os.path.basename(path) if isinstance(path, str) else ""
            retained = (entry.get("retained_snapshot") is True and
                        re.match(r"^retained_native_[0-9a-f]{16}\.log$", name))
            if name not in ("ocean.stdout", "ocean.stderr", "persistent_worker_batch.tsv",
                            "persistent_worker_batch_job_policy.tsv", _contract_text("native_dispatch.jsonl"),
                            "effective_job_policy.tsv", _contract_text("effective_job_policy.json"),
                            _contract_text("execution_progress_full.json")) and not retained:
                raise ValueError("candidate shared diagnostic name is unsupported")
            add(path, "ocean" if name.startswith("ocean.") else "native_batch",
                (entry.get("label") or name) + " · shared worker/batch", shared=True)
        return results

    def _candidate_log_descriptors(self, run_name, candidate_id):
        candidate_id = self._safe_candidate_name(candidate_id)
        candidate_relative = os.path.join("candidates", candidate_id)
        candidate = self._native_candidate_record(run_name, candidate_id, "candidate live-log metadata") or {}
        if candidate.get("candidate_id", candidate_id) != candidate_id:
            raise ValueError("candidate log metadata identity does not match")
        if candidate.get("execution_backend") in ("private_netlist", "maestro_netlist"):
            cli_progress = self._read_run_json_snapshot(run_name,
                os.path.join(candidate_relative, _contract_text("cli_progress.json")), "shared CLI candidate display progress", 32 * 1024 * 1024)
            if cli_progress is not None:
                if (not isinstance(cli_progress, dict) or cli_progress.get("schema_version") != _contract_text("crescendo.shared_cli_progress.v1")
                        or cli_progress.get("candidate_id") != candidate_id
                        or cli_progress.get("execution_backend") != candidate.get("execution_backend")
                        or cli_progress.get("execution_simulator") not in ("spectre", "alps")):
                    raise ValueError("shared CLI candidate display progress identity disagrees")
                candidate["execution_progress"] = cli_progress
                candidate["execution_simulator"] = cli_progress["execution_simulator"]
        scope = self._candidate_batch_log_scope(candidate)
        context = self._candidate_log_context(candidate)
        descriptors = []
        inspected = [0, 0]
        for root_record in self._candidate_log_roots(run_name, candidate_id, candidate):
            root_relative = root_record["relative"]
            test_name_map = root_record.get("test_name_map")
            root_descriptors = []
            for discovered in self._candidate_spectre_files(
                    run_name, root_relative, inspected,
                    allowed_tests=set(test_name_map) if test_name_map is not None else None):
                relative = discovered["relative_path"]
                parts = relative.replace("\\", "/").split("/")
                if len(parts) < 5 or parts[-1] != "spectre.out" or parts[-2] != "psf":
                    continue
                point_token = parts[-4]
                test = parts[-3]
                alias = point_token == "psf"
                point_id = None
                if not alias:
                    try:
                        point_id = int(point_token)
                    except (TypeError, ValueError):
                        point_id = point_token
                physical_test = test
                row_context = context.get((str(test), "" if point_id is None else str(point_id))) or {}
                test = row_context.get("test") or (test_name_map or {}).get(test, test)
                corner = row_context.get("corner")
                label = str(test)
                if corner not in (None, ""):
                    label += " · %s" % corner
                elif point_id not in (None, ""):
                    label += " · point %s" % point_id
                elif alias:
                    label += " · latest"
                log_id = hashlib.sha256(relative.encode("utf-8")).hexdigest()[:24]
                root_descriptors.append({
                    "log_id": log_id, "label": label, "test": test,
                    "artifact_test": physical_test,
                    "corner": corner, "point_id": point_id,
                    "kind": "spectre_alias" if alias else "spectre",
                    "state": "available", "size_bytes": discovered["size_bytes"],
                    "relative_path": relative, "fence": root_record.get("fence"),
                })
                if len(descriptors) + len(root_descriptors) > 256:
                    raise ValueError("candidate exposes too many Spectre logs")
            if self._persistent_log_fence_matches(
                    run_name, root_record.get("fence")):
                descriptors.extend(root_descriptors)
        primary_tests = set(
            str(row.get("test") or "") for row in descriptors
            if row["kind"] == "spectre"
        )
        descriptors = [
            row for row in descriptors
            if row["kind"] == "spectre" or row["test"] not in primary_tests
        ]
        if not descriptors:
            descriptors = self._candidate_retained_spectre_descriptors(
                run_name, candidate_relative, context,
                test_name_map=(scope or {}).get("test_name_map"),
            )
        known_paths = set(row["relative_path"] for row in descriptors)
        for row in self._candidate_progress_log_descriptors(run_name, candidate_id, candidate, scope):
            if row["relative_path"] not in known_paths:
                descriptors.append(row)
                known_paths.add(row["relative_path"])
        descriptors.sort(key=lambda row: (
            str(row.get("test") or ""), str(row.get("corner") or ""),
            str(row.get("point_id") or ""), row["kind"], row["log_id"],
        ))
        return candidate, descriptors

    def candidate_logs(self, run_name, candidate_id):
        candidate, descriptors = self._candidate_log_descriptors(run_name, candidate_id)
        public = []
        for row in descriptors:
            item = dict(
                (key, value) for key, value in row.items()
                if key not in ("relative_path", "fence", "fresh_after", "_owned_log_task")
            )
            item.setdefault("file_name", os.path.basename(row["relative_path"]))
            item["location"] = row.get("display_location") or row["relative_path"]
            public.append(item)
        status = str(candidate.get("status") or "pending")
        progress = candidate.get("execution_progress") or {}
        total_tasks = progress.get("task_count")
        if type(total_tasks) is not int or total_tasks < len(progress.get("tasks") or []):
            total_tasks = len(progress.get("tasks") or [])
        capacity = progress.get("telemetry_capacity") or {}
        public_tasks = []
        for row in (progress.get("tasks") or [])[:32]:
            item = dict((key, row.get(key)) for key in
                        ("test", "artifact_test", "corner", "point_id", "state", "job_id",
                         "command", "submit_argv", "submission_returncode", "submission_output"))
            for key in ("scheduler_state", "execution_host", "job_exit_code", "task_timeouts"):
                if key in row:
                    item[key] = copy.deepcopy(row[key])
            from .execution_commands import command_display
            for plan_key, public_key in (("solver_command_plan", "solver_command"),
                                         ("submission_command_plan", "submission_command")):
                plan = row.get(plan_key) or {}
                if isinstance(plan, dict) and isinstance(plan.get("argv"), list):
                    item[public_key] = command_display(plan["argv"])
            submission = row.get("submission") or {}
            if isinstance(submission, dict):
                item["submit_argv"] = item["submit_argv"] or submission.get("argv")
                item["submission_returncode"] = submission.get("returncode", item["submission_returncode"])
                item["submission_output"] = (item["submission_output"] or submission.get("stdout_tail")
                                               or submission.get("stdout") or submission.get("output")
                                               or submission.get("text"))
                if not item["command"] and isinstance(item["submit_argv"], list):
                    import shlex
                    item["command"] = " ".join(shlex.quote(str(value)) for value in item["submit_argv"][:64])
            public_tasks.append(item)
        failure_stage = _candidate_failure_stage(candidate)
        completion = candidate.get("completion_evidence") or candidate.get("completion") or {}
        simulator = completion.get("execution_simulator") or candidate.get("execution_simulator") or (progress.get("spec") or {}).get("simulator") or "spectre"
        simulator_label = "ALPS" if simulator == "alps" else "Spectre"
        spectre_available = any(str(row.get("kind") or "").startswith(simulator) for row in public)
        spectre_state = _candidate_spectre_state(candidate, has_logs=spectre_available)
        failure_reason = _candidate_failure_reason(candidate)
        if public:
            state = "available"
            message = ("Spectre log is available." if spectre_available else
                       "Shared diagnostics are available; no candidate Spectre log has been created yet.")
        elif spectre_state == "not_started":
            state = "not_started"
            stage_label = _FAILURE_STAGE_LABELS.get(
                failure_stage,
                failure_stage.replace("_", " ").title()
                if failure_stage else "an earlier stage",
            )
            message = (
                "Spectre was not started; the failure occurred during %s."
                % stage_label
            )
        elif status in _TERMINAL_CANDIDATE_STATES or status in ("stopped", "cancelled"):
            state = "removed"
            if spectre_state == "started":
                message = "Spectre ran, but no live or retained log is available."
            else:
                message = (
                    "No live or retained Spectre log is available; whether Spectre "
                    "started is unknown for this older candidate record."
                )
        else:
            state = "waiting"
            message = "Stage %s: no candidate Spectre log found in its recorded task paths." % (progress.get("stage") or status)
        if simulator == "alps":
            message = message.replace("Spectre", simulator_label)
        return {
            "run_id": self._safe_run_name(run_name), "candidate_id": candidate_id,
            "candidate_status": status, "state": state, "logs": public,
            "execution_simulator": simulator,
            "failure_class": candidate.get("failure_class"),
            "failure_stage": failure_stage,
            "failure_stage_label": (
                _FAILURE_STAGE_LABELS.get(
                    failure_stage,
                    failure_stage.replace("_", " ").title()
                    if failure_stage else None,
                )
            ),
            "failure_reason": failure_reason,
            "spectre_state": spectre_state,
            "stage": progress.get("stage") or status,
            "tasks": public_tasks,
            "task_count": total_tasks,
            "task_summary_omitted": max(0, total_tasks - len(public_tasks)),
            "log_count": len(public),
            "warnings": list(progress.get("warnings") or [])[:8] +
                        ([progress["capacity_warning"]] if progress.get("capacity_warning") else []) +
                        ([capacity["message"]] if isinstance(capacity, dict) and capacity.get("message") else []),
            "message": message,
        }

    @contextlib.contextmanager
    def _candidate_log_reader(self, run_name, descriptor):
        task = descriptor.get('_owned_log_task')
        if task is not None:
            from .owned_solver_logs import owned_solver_log_reader
            run_root = os.path.abspath(self.run_path(run_name, ''))
            source = os.path.join(run_root, descriptor['relative_path'])
            epoch = descriptor.get('fresh_after')
            with contextlib.ExitStack() as readers:
                try:
                    opened, _metadata = readers.enter_context(owned_solver_log_reader(
                        source, task['task_root'], None if epoch is None else epoch + 2, task=task))
                except FileNotFoundError:
                    yield None
                    return
                yield opened
        else:
            _path, opened = self._open_run_relative(run_name, descriptor['relative_path'],
                                                     directory=False, missing_ok=True)
            try:
                yield opened
            finally:
                if opened is not None:
                    os.close(opened)

    def read_candidate_log_chunk(self, run_name, candidate_id, log_id, offset=0,
                                 max_bytes=262144, file_token=None):
        if not isinstance(log_id, str) or not re.match(r"^[0-9a-f]{24}$", log_id):
            raise ValueError("invalid candidate log id")
        try:
            offset = int(offset)
            max_bytes = int(max_bytes)
        except (TypeError, ValueError):
            raise ValueError("log offset and max_bytes must be integers")
        if offset < 0:
            raise ValueError("log offset must not be negative")
        if max_bytes < 1 or max_bytes > 1024 * 1024:
            raise ValueError("log max_bytes must be between 1 and 1048576")
        candidate, descriptors = self._candidate_log_descriptors(run_name, candidate_id)
        descriptor = next((row for row in descriptors if row["log_id"] == log_id), None)
        if descriptor is None:
            status = str(candidate.get("status") or "pending")
            removed = status in _TERMINAL_CANDIDATE_STATES or status in ("stopped", "cancelled")
            if descriptors:
                raise ValueError("candidate log is unavailable or no longer belongs to this candidate")
            return {
                "log_id": log_id, "state": "removed" if removed else "waiting",
                "text": "", "next_offset": offset, "size_bytes": 0,
                "file_token": None, "eof": True, "reset": False,
                "removed": removed, "waiting": not removed,
            }
        if not self._persistent_log_fence_matches(run_name, descriptor.get("fence")):
            status = str(candidate.get("status") or "pending")
            removed = status in _TERMINAL_CANDIDATE_STATES or status in ("stopped", "cancelled")
            return {
                "log_id": log_id, "state": "removed" if removed else "waiting",
                "text": "", "next_offset": offset, "size_bytes": 0,
                "file_token": None, "eof": True, "reset": False,
                "removed": removed, "waiting": not removed,
            }
        from .native_log_io import native_log_read_scope
        with native_log_read_scope(), self._candidate_log_reader(run_name, descriptor) as opened:
            if opened is None:
                status = str(candidate.get("status") or "pending")
                removed = status in _TERMINAL_CANDIDATE_STATES or status in ("stopped", "cancelled")
                return {
                    "log_id": log_id, "state": "removed" if removed else "waiting", "text": "",
                    "next_offset": offset, "size_bytes": 0, "file_token": None,
                    "eof": True, "reset": False, "removed": removed, "waiting": not removed,
                }
            before = os.fstat(opened)
            if not stat.S_ISREG(before.st_mode) or int(before.st_nlink) != 1:
                raise ValueError("candidate log must be a singly linked regular file")
            if descriptor.get("fresh_after") is not None and before.st_mtime < descriptor["fresh_after"]:
                raise ValueError("native serial Spectre log predates this candidate")
            if not self._persistent_log_fence_matches(run_name, descriptor.get("fence")):
                raise ValueError("candidate log no longer belongs to this candidate")
            identity_token = "%x:%x" % (int(before.st_dev), int(before.st_ino))
            reset = False
            if file_token:
                token_parts = str(file_token).split(":")
                if len(token_parts) == 5 and token_parts[0] == "v2":
                    try:
                        token_identity = "%x:%x" % (
                            int(token_parts[1], 16), int(token_parts[2], 16),
                        )
                        token_offset = int(token_parts[3], 16)
                    except (TypeError, ValueError, OverflowError):
                        token_identity = ""
                        token_offset = -1
                    continuity = self._candidate_log_continuity(opened, offset)
                    reset = bool(
                        token_identity != identity_token
                        or token_offset != offset
                        or token_parts[4] != continuity
                    )
                else:
                    reset = str(file_token) != identity_token
            if reset or offset > int(before.st_size):
                offset = 0
                reset = True
            os.lseek(opened, offset, os.SEEK_SET)
            data = os.read(opened, max_bytes)
            next_offset = offset + len(data)
            after = os.fstat(opened)
            if (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
                raise ValueError("candidate log identity changed while being read")
            if not self._persistent_log_fence_matches(run_name, descriptor.get("fence")):
                raise ValueError("candidate log no longer belongs to this candidate")
            current_token = "v2:%x:%x:%x:%s" % (
                int(after.st_dev), int(after.st_ino), int(next_offset),
                self._candidate_log_continuity(opened, next_offset),
            )
            return {
                "log_id": log_id, "state": "available",
                "text": data.decode("utf-8", "replace"),
                "next_offset": next_offset, "size_bytes": int(after.st_size),
                "file_token": current_token, "eof": next_offset >= int(after.st_size),
                "reset": reset, "removed": False, "waiting": False,
            }


    @staticmethod
    def _candidate_log_continuity(descriptor, offset, window=64):
        offset = max(0, int(offset))
        start = max(0, offset - int(window))
        os.lseek(descriptor, start, os.SEEK_SET)
        return hashlib.sha256(os.read(descriptor, offset - start)).hexdigest()[:16]

    def _optimized_cleanup_runtime(self, run_name=None):
        runtime = self.config.get("runtime") or {}
        scope = self._run_scopes.get(run_name) if run_name else None
        return os.path.abspath(scope or runtime.get("design_root") or (self._runtime_for_run(run_name) if run_name else runtime["run_root"]))

    def _recover_optimized_view_cleanup(self, timeout_seconds=12.0):
        """Recover only durable, owned export commands before releasing their lease."""
        pending_run = (self.optimized_view_cleanup_pending or {}).get("run_id")
        runtime = self._optimized_cleanup_runtime(pending_run)
        marker = os.path.join(runtime, _OPTIMIZED_VIEW_CLEANUP_PENDING)
        if self.optimized_view_cleanup_pending is None and not os.path.lexists(marker):
            return
        with self.optimized_view_cleanup_lock:
            lease = self.optimized_view_cleanup_lease
            if lease is None:
                lease = RuntimeOptimizationLease(runtime).acquire()
                self.optimized_view_cleanup_lease = lease
            try:
                pending = self._read_runtime_json_snapshot(
                    _OPTIMIZED_VIEW_CLEANUP_PENDING, "schematic export cleanup barrier", 4096, runtime_root=runtime,
                ) or self.optimized_view_cleanup_pending
                if pending is None:
                    self.optimized_view_cleanup_lease = None
                    lease.release()
                    return
                self.optimized_view_cleanup_pending = pending
                with self.lock:
                    self.optimized_view_export_active = True
                if not isinstance(pending, dict) or pending.get("schema_version") != _OPTIMIZED_VIEW_CLEANUP_SCHEMA:
                    raise ValueError("schematic export cleanup barrier is invalid")
                run_name = self._safe_run_name(pending.get("run_id"))
                relative = pending.get("export_relative")
                if not isinstance(relative, str) or not re.match(r"^optimized_schematics/export_[a-f0-9]{32}$", relative):
                    raise ValueError("schematic export cleanup path is invalid")
                failed_phase = pending.get("phase")
                if failed_phase not in ("stage", "publish"):
                    raise ValueError("schematic export cleanup phase is invalid")
                root, descriptor = self._open_run_relative(run_name, relative, directory=True)
                os.close(descriptor)
                context, descriptor = self._open_run_relative(run_name, relative + "/context", directory=True)
                os.close(descriptor)
                deadline = time.monotonic() + min(12.0, max(0.0, float(timeout_seconds)))
                for phase in ("stage", "publish"):
                    phase_relative = relative + "/" + phase
                    control, descriptor = self._open_run_relative(run_name, phase_relative, directory=True, missing_ok=True)
                    if descriptor is None:
                        if phase == "stage" or failed_phase == "publish":
                            raise ValueError("schematic export process evidence is missing")
                        continue
                    os.close(descriptor)
                    manifest = self._read_run_json_snapshot(
                        run_name, phase_relative + "/" + LEGACY_GUARDIAN_MANIFEST_NAME,
                        "schematic export command ownership", 1024 * 1024,
                    )
                    if manifest is None:
                        if phase == "stage" or failed_phase == "publish" or self._list_run_directory(run_name, phase_relative):
                            raise ValueError("schematic export launch ownership evidence is missing")
                        continue
                    token = manifest.get("token") or ""
                    argv = manifest.get("argv") or []
                    if (not re.match(r"^optimized-export-[a-f0-9]{32}$", token) or
                            manifest.get("attempt_root") != control or manifest.get("workspace_root") != context or
                            len(argv) != 4 or argv[1:3] != ["-nograph", "-restore"] or
                            argv[3] != os.path.join(control, "export.il")):
                        raise ValueError("schematic export command ownership does not match this export")
                    inspection = quiesce_legacy_guardian_attempt(
                        control, expected_workspace_root=context,
                        timeout_seconds=max(0.0, deadline - time.monotonic()), terminate_owned=True,
                    )
                    if inspection.get("state") != "dead":
                        raise RuntimeError("owned %s process cleanup remains unproven" % phase)
                if os.path.lexists(marker):
                    os.unlink(marker)
                self.optimized_view_cleanup_pending = None
                self.optimized_view_cleanup_lease = None
                with self.lock:
                    self.optimized_view_export_active = False
                lease.release()
            except Exception as error:
                # Keep both the durable barrier and lease until exact ownership
                # evidence proves the native writer has stopped. Never delete views.
                raise RuntimeError(
                    "Schematic export cleanup is unproven; Start/Resume and Exit remain blocked. "
                    "Retry Exit to recover its owned process: %s" % error
                ) from error

    def generate_optimized_schematic(self, run_name, candidate_id):
        from .optimized_schematic import export_optimized_schematic
        from .runner import _reverify_prepared_source_identity
        run_name = self._safe_run_name(run_name)
        candidate_id = self._safe_candidate_name(candidate_id)
        self._recover_optimized_view_cleanup()
        self._begin_lifecycle_operation("generate optimized schematic views")
        registered = False
        lease = None
        tb_lease = None
        root = None
        try:
            with self.lock:
                if self.optimized_view_export_active or self.optimization_launches or any(
                        thread.is_alive() for thread in self.threads.values()):
                    raise RuntimeError("Stop optimization and wait for cleanup before generating schematic views")
                self.optimized_view_export_active = True
                registered = True
            status = self._read_run_json_snapshot(run_name, _contract_text("status.json"), "schematic export run status", 4 * 1024 * 1024)
            if not status or status.get("state") not in _TERMINAL_RUN_STATES:
                raise ValueError("Schematic export requires a completed or stopped run")
            run_root = self._validate_run_status_identity(run_name, status)
            tb_lease = self._acquire_tb_lease(self._config_for_run(run_name))
            lease = RuntimeOptimizationLease(self._runtime_for_run(run_name), run_root=run_root).acquire()
            shutdown = self._read_shutdown_evidence(run_name, "schematic export shutdown")
            from .shutdown_evidence import shutdown_proven
            if shutdown is not None and not shutdown_proven(shutdown):
                raise RuntimeError("Simulation worker cleanup is unproven; inspect run diagnostics before exporting")
            context = self._read_run_json_snapshot(run_name, _contract_text("schematic_export_context.json"), "schematic export context", 64 * 1024 * 1024)
            if not context or context.get("schema_version") != _contract_text("crescendo.schematic_export_context.v1"):
                raise ValueError("This run has no schematic export context; run again with this version")
            candidate = self._native_candidate_record(run_name, candidate_id, "schematic export candidate", strict=True)
            if not candidate or candidate.get("candidate_id") != candidate_id or candidate.get("status") != "completed":
                raise ValueError("Select a completed real simulation candidate")
            spec = self._read_run_json_snapshot(run_name, _contract_text("spec.json"), "schematic export specification", 4 * 1024 * 1024) or {}
            if spec.get("evaluator", "real") != "real":
                raise ValueError("Mock candidates cannot generate optimized schematic views")
            prepared, config = context["prepared"], context["config"]
            evidence_run = run_name
            seen = {run_name}
            while context.get("origin_run_id") and not (
                    (candidate.get("completion_evidence") or candidate.get("completion") or {}).get("ok") is True):
                origin = self._safe_run_name(context["origin_run_id"])
                if origin in seen or len(seen) >= 32:
                    raise ValueError("Original candidate evidence chain is unavailable; open its original run to export")
                seen.add(origin)
                origin_status = self._read_run_json_snapshot(origin, _contract_text("status.json"), "original export run status", 4 * 1024 * 1024)
                if not origin_status or origin_status.get("state") not in _TERMINAL_RUN_STATES:
                    raise ValueError("Original candidate run is not proven finalized")
                self._validate_run_status_identity(origin, origin_status)
                origin_shutdown = self._read_shutdown_evidence(origin, "original export shutdown")
                if origin_shutdown is not None and not shutdown_proven(origin_shutdown):
                    raise ValueError("Original simulation worker cleanup is unproven")
                origin_context = self._read_run_json_snapshot(origin, _contract_text("schematic_export_context.json"), "original export context", 64 * 1024 * 1024)
                origin_candidate = self._native_candidate_record(origin, candidate_id, "original export candidate", strict=True)
                before = prepared.get("source_identity") or {}
                observed = ((origin_context or {}).get("prepared") or {}).get("source_identity") or {}
                if (not origin_context or origin_context.get("schema_version") != _contract_text("crescendo.schematic_export_context.v1")
                        or not origin_candidate or origin_candidate.get("candidate_id") != candidate_id
                        or origin_candidate.get("status") != "completed"
                        or origin_candidate.get("values") != candidate.get("values")
                        or not before.get("fingerprint") or not observed.get("fingerprint")
                        or (before.get("schema_version"), before["fingerprint"]) != (observed.get("schema_version"), observed["fingerprint"])):
                    raise ValueError("Original candidate evidence does not match this resumed solution")
                context, candidate, evidence_run = origin_context, origin_candidate, origin
            prepared, config = context["prepared"], context["config"]
            template = None
            if candidate.get("execution_backend") == "private_netlist":
                template = self._read_run_json_snapshot(evidence_run, "private_netlist/templates/private_netlist.json", "schematic export template", 64 * 1024 * 1024)
            root = os.path.join(run_root, "optimized_schematics", "export_" + uuid.uuid4().hex)
            return export_optimized_schematic(
                prepared, spec, candidate, config, root, template_manifest=template,
                cancel_check=self.prepare_cancel_event.is_set,
                identity_check=lambda: _reverify_prepared_source_identity(prepared, config),
            )
        except Exception as error:
            command = (getattr(error, "detail", None) or {}).get("command")
            if root is not None and command is None:
                relative = os.path.relpath(root, run_root)
                result = self._read_run_json_snapshot(
                    run_name, relative + "/export_result.json", "schematic export result", 16 * 1024 * 1024,
                ) or {}
                command = next((row for row in reversed(result.get("commands") or []) if
                                row.get("termination_failed") or (row.get("guardian_cleanup") or {}).get("state") != "dead"), None)
            if root is not None and command and (command.get("termination_failed") or
                    (command.get("guardian_cleanup") or {}).get("state") != "dead"):
                phase = (getattr(error, "detail", None) or {}).get("phase")
                if phase not in ("stage", "publish"):
                    argv = command.get("argv") or []
                    phase = "stage" if argv and argv[-1] == os.path.join(root, "stage", "export.il") else "publish"
                pending = {"schema_version": _OPTIMIZED_VIEW_CLEANUP_SCHEMA, "run_id": run_name,
                           "export_relative": os.path.relpath(root, run_root), "phase": phase}
                self.optimized_view_cleanup_pending = pending
                self.optimized_view_cleanup_lease = lease
                lease = None
                marker_root = self._optimized_cleanup_runtime(run_name)
                ensure_dir(marker_root)
                atomic_write_json(os.path.join(marker_root, _OPTIMIZED_VIEW_CLEANUP_PENDING), pending)
                self._recover_optimized_view_cleanup()
            raise
        finally:
            if lease is not None:
                lease.release()
            if tb_lease is not None:
                tb_lease.release()
            if registered and self.optimized_view_cleanup_pending is None:
                with self.lock:
                    self.optimized_view_export_active = False
            self._end_lifecycle_operation()

    def export_checkpoint(self, run_name):
        snapshot = self._resolve_run_checkpoint_snapshot(run_name)
        if snapshot is None:
            return None
        checkpoint = snapshot["checkpoint"]
        if checkpoint.get("schema_version") != _contract_text("crescendo.checkpoint.v1"):
            raise ValueError("legacy checkpoint export is not supported")
        validate_checkpoint_integrity(checkpoint)
        from .runner import self_contained_checkpoint
        return self_contained_checkpoint(checkpoint)

    def candidates_csv(self, run_name):
        progress = self.progress(run_name)
        labels = (progress.get("spec") or {}).get("variable_labels") or {}
        return candidates_csv_text(progress["candidates"], variable_labels=labels)


def make_handler(service):
    class Handler(BaseHTTPRequestHandler):
        server_version = _contract_text("Crescendo/1.0")

        def _json(self, status, payload, download_name=None):
            data = json.dumps(payload, sort_keys=True).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            if download_name:
                self.send_header("Content-Disposition", 'attachment; filename="%s"' % download_name)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(data)

        def _download(self, content_type, data, download_name):
            data = data.encode("utf-8") if isinstance(data, str) else data
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Disposition", 'attachment; filename="%s"' % download_name)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(data)

        def _body(self):
            length = int(self.headers.get("Content-Length", "0"))
            if length > 64 * 1024 * 1024:
                raise ValueError("request body exceeds the 64 MiB checkpoint limit")
            return json.loads(self.rfile.read(length).decode("utf-8")) if length else {}

        def _static(self, path):
            relative = "index.html" if path in ("", "/") else path.lstrip("/")
            target = os.path.realpath(os.path.join(WEB_ROOT, relative))
            if not target.startswith(os.path.realpath(WEB_ROOT) + os.sep) or not os.path.isfile(target):
                self.send_error(404)
                return
            with open(target, "rb") as stream:
                data = stream.read()
            self.send_response(200)
            self.send_header("Content-Type", mimetypes.guess_type(target)[0] or "application/octet-stream")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query)
            try:
                if parsed.path == "/api/health":
                    self._json(200, {"ok": True, "service": _contract_text("crescendo")})
                elif parsed.path == "/api/eda-setup":
                    self._json(200, service.eda_setup())
                elif parsed.path == "/api/runs":
                    self._json(200, {"runs": service.list_runs()})
                elif parsed.path == "/api/browse":
                    self._json(200, service.browse(query.get("path", [None])[0]))
                elif parsed.path == "/api/progress":
                    self._json(200, service.progress(query.get("run_id", [""])[0]))
                elif parsed.path == "/api/status":
                    payload = service.read(query.get("run_id", [""])[0], _contract_text("status.json"))
                    self._json(200 if payload else 404, payload or {"error": "status not found"})
                elif parsed.path == "/api/result":
                    payload = service.read(query.get("run_id", [""])[0], _contract_text("result.json"))
                    self._json(200 if payload else 404, payload or {"error": "result not ready"})
                elif parsed.path == "/api/checkpoint":
                    run_name = query.get("run_id", [""])[0]
                    payload = service.export_checkpoint(run_name)
                    self._json(
                        200 if payload else 404, payload or {"error": "checkpoint not ready"},
                        "%s-checkpoint.json" % run_name if payload else None,
                    )
                elif parsed.path == "/api/candidates.csv":
                    run_name = query.get("run_id", [""])[0]
                    self._download("text/csv; charset=utf-8", service.candidates_csv(run_name), "%s-candidates.csv" % run_name)
                elif parsed.path == "/api/range-validation/status":
                    payload = service.range_validation_status(query.get("job_id", [""])[0])
                    self._json(200 if payload else 404, payload or {"error": "range validation job not found"})
                else:
                    self._static(parsed.path)
            except Exception as error:
                self._json(400, {"error": "%s: %s" % (error.__class__.__name__, error)})

        def do_POST(self):
            try:
                body = self._body()
                if self.path == "/api/inspect-tests":
                    token, preview = service.inspect_tests(body["path"], backend=body.get("backend", "auto"))
                    self._json(200, {"test_inspection_id": token, "preview": preview})
                elif self.path == "/api/inspect-test-cells":
                    token, preview = service.inspect_test_cells(body["test_inspection_id"], body["test_name"],
                                                               max_depth=body.get("max_depth", 32))
                    self._json(200, {"inspection_id": token, "inspection": preview})
                elif self.path == "/api/prepare-cells":
                    token, prepared = service.prepare_cells(body["inspection_id"], body.get("scope_ids") or [])
                    self._json(200, {"prepared_id": token, "prepared": prepared})
                elif self.path == "/api/prepare":
                    prepared_id, prepared = service.prepare(
                        body["path"], body.get("max_depth", 32), body.get("backend", "auto"),
                        selected_test=body.get("selected_test"))
                    self._json(200, {"prepared_id": prepared_id, "prepared": prepared})
                elif self.path == "/api/start":
                    self._json(202, service.start(body["prepared_id"], body["spec"]))
                elif self.path == "/api/range-validation/start":
                    self._json(202, service.validate_ranges(body["prepared_id"], body.get("variables") or []))
                elif self.path == "/api/range-validation/static":
                    self._json(200, service.validate_static_ranges(
                        body["prepared_id"], body.get("variables") or [],
                    ))
                elif self.path == "/api/design-config/load":
                    self._json(200, service.load_design_config(body["document"], body.get("path")))
                elif self.path == "/api/configuration/load":
                    self._json(200, service.load_configuration(
                        body["document"], body.get("path"), body.get("max_depth", 32)))
                elif self.path == "/api/resume":
                    self._json(202, service.resume(body["checkpoint"], body.get("path")))
                elif self.path == "/api/stop":
                    self._json(200, service.stop(body["run_id"]))
                elif self.path == "/api/optimized-schematic":
                    self._json(200, service.generate_optimized_schematic(body["run_id"], body["candidate_id"]))
                elif self.path == "/api/native-browse":
                    self._json(200, service.native_browse(body.get("path")))
                elif self.path == "/api/eda-setup":
                    self._json(200, service.update_eda_setup(body))
                elif self.path == "/api/exit":
                    result = service.exit_optimizer()
                    service.request_http_shutdown()
                    self._json(200, result)
                else:
                    self._json(404, {"error": "unknown endpoint"})
            except VariableRangeValidationError as error:
                self._json(400, {
                    "error": "%s: %s" % (error.__class__.__name__, error),
                    "validation": error.report, "issues": error.issues,
                })
            except Exception as error:
                self._json(400, {"error": "%s: %s" % (error.__class__.__name__, error)})

        def log_message(self, format_string, *args):
            print("%s - %s" % (self.address_string(), format_string % args))

    return Handler


def serve(config, host="127.0.0.1", port=8877):
    if host not in ("127.0.0.1", "localhost", "::1"):
        raise ValueError(_contract_text("Crescendo UI only binds to loopback; use SSH port forwarding for remote access"))
    service = OptimizerService(config)
    server = ThreadingHTTPServer((host, int(port)), make_handler(service))
    service.shutdown_callback = server.shutdown
    print(_contract_text("Crescendo UI: http://%s:%d") % (host, int(port)))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        try:
            service._cancel_and_wait_range_validations()
            service._close_resident_validation()
        except Exception as error:
            print(_contract_text("Crescendo shutdown cleanup failed: %s: %s") % (error.__class__.__name__, error))
        server.server_close()
