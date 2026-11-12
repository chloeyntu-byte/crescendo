from __future__ import print_function

import argparse
import copy
import ctypes
import errno
import math
import os
import re
import signal
import stat
import subprocess
import sys
import threading
import time
import traceback
import uuid

try:
    import fcntl
except ImportError:
    fcntl = None

from crescendo_engine.runtime import bind_text
_contract_text = bind_text()

from .utils import atomic_write_json, ensure_dir, load_json, utc_timestamp
from .native_environment import clean_gui_environment
from .session_health import sample_owned_process_tree
from .legacy_guardian import (
    GUARDIAN_SCOPE_CONFIG_KEY,
    LAUNCH_LOCK_NAME as LEGACY_GUARDIAN_LAUNCH_LOCK_NAME,
    MANIFEST_NAME as LEGACY_GUARDIAN_MANIFEST_NAME,
    MANIFEST_SCHEMA_VERSION as LEGACY_GUARDIAN_PROTOCOL,
    inspect_legacy_guardian_attempt,
    resolve_worker_guardian_token_identity,
)
from .retention import parse_worker_epoch_component, parse_worker_epoch_namespace


SCHEMA_VERSION = _contract_text("crescendo.persistent_controller.v1")
LIVENESS_LOCK_SCHEMA = _contract_text("crescendo.controller_liveness.v1")
PR_SET_PDEATHSIG = 1
_LIVENESS_FD_ENV = _contract_text("CRESCENDO_CONTROLLER_LIVENESS_FD")
_LIVENESS_PATH_ENV = _contract_text("CRESCENDO_CONTROLLER_LIVENESS_PATH")
_LIVENESS_SCHEMA_ENV = _contract_text("CRESCENDO_CONTROLLER_LIVENESS_SCHEMA")


class PersistentControllerError(RuntimeError):
    pass


class PersistentControllerProcessError(PersistentControllerError):
    pass


class PersistentControllerRemoteError(PersistentControllerProcessError):
    def __init__(self, message, remote_error_type=None, failure_class=None,
                 fatal_run=False, remote_traceback=None):
        PersistentControllerProcessError.__init__(self, message)
        self.remote_error_type = remote_error_type
        self.failure_class = failure_class
        self.fatal_run = bool(fatal_run)
        self.remote_traceback = remote_traceback


class PersistentControllerTimeout(PersistentControllerError):
    pass


def _linux_boot_id():
    try:
        with open("/proc/sys/kernel/random/boot_id", "r") as stream:
            value = stream.read().strip()
    except (IOError, OSError):
        return None
    return value or None


def _process_stat(pid):
    try:
        with open("/proc/%d/stat" % int(pid), "r") as stream:
            value = stream.read()
    except (IOError, OSError, TypeError, ValueError):
        return None
    closing = value.rfind(")")
    if closing < 0:
        return None
    fields = value[closing + 2:].split()
    if len(fields) <= 19:
        return None
    try:
        return {
            "state": fields[0],
            "ppid": int(fields[1]),
            "pgid": int(fields[2]),
            "sid": int(fields[3]),
            "start_ticks": int(fields[19]),
        }
    except (TypeError, ValueError):
        return None


def _set_parent_death_signal(expected_parent_pid, expected_parent_start_ticks):
    if not sys.platform.startswith("linux"):
        return
    if os.getppid() != int(expected_parent_pid):
        os.kill(os.getpid(), signal.SIGTERM)
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(PR_SET_PDEATHSIG, signal.SIGTERM, 0, 0, 0) != 0:
        error_number = ctypes.get_errno()
        raise OSError(error_number, os.strerror(error_number))
    parent_stat = _process_stat(expected_parent_pid) or {}
    if (
        os.getppid() != int(expected_parent_pid)
        or parent_stat.get("start_ticks") != expected_parent_start_ticks
    ):
        os.kill(os.getpid(), signal.SIGTERM)


def _open_controller_liveness_lock(path):
    if fcntl is None:
        raise PersistentControllerError("controller liveness locks require POSIX fcntl.flock")
    path = os.path.abspath(path)
    if os.path.islink(path):
        raise PersistentControllerError("controller liveness lock must not be a symbolic link: %s" % path)
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode) or os.path.islink(path):
            raise PersistentControllerError("controller liveness lock is not a regular file: %s" % path)
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return descriptor
    except Exception:
        os.close(descriptor)
        raise


def _adopt_controller_liveness_lock_from_environment():
    if fcntl is None:
        raise PersistentControllerError("controller liveness locks require POSIX fcntl.flock")
    raw_descriptor = os.environ.pop(_LIVENESS_FD_ENV, None)
    path = os.environ.pop(_LIVENESS_PATH_ENV, None)
    schema = os.environ.pop(_LIVENESS_SCHEMA_ENV, None)
    if schema != LIVENESS_LOCK_SCHEMA or not path or not os.path.isabs(path):
        raise PersistentControllerError("controller liveness lock environment is missing or malformed")
    try:
        descriptor = int(raw_descriptor)
    except (TypeError, ValueError):
        raise PersistentControllerError("controller liveness lock descriptor is malformed")
    if descriptor < 3:
        raise PersistentControllerError("controller liveness lock descriptor is unsafe")
    try:
        descriptor_stat = os.fstat(descriptor)
        if os.path.islink(path) or not os.path.isfile(path):
            raise PersistentControllerError("controller liveness lock path is unsafe: %s" % path)
        path_stat = os.stat(path)
        if (
            not stat.S_ISREG(descriptor_stat.st_mode) or
            descriptor_stat.st_dev != path_stat.st_dev or
            descriptor_stat.st_ino != path_stat.st_ino
        ):
            raise PersistentControllerError("controller liveness descriptor does not match its path")
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return descriptor, os.path.abspath(path)
    except Exception:
        os.close(descriptor)
        raise


def _validate_controller_liveness_bootstrap(descriptor, inherited_path, bootstrap_path, bootstrap):
    expected_path = os.path.join(os.path.dirname(os.path.abspath(bootstrap_path)), "controller_liveness.lock")
    recorded_path = bootstrap.get("liveness_lock_path")
    if bootstrap.get("liveness_lock_schema") != LIVENESS_LOCK_SCHEMA:
        raise PersistentControllerError("controller bootstrap liveness schema is not recognized")
    if (
        not isinstance(recorded_path, str) or not os.path.isabs(recorded_path) or
        os.path.abspath(recorded_path) != expected_path or
        os.path.realpath(recorded_path) != os.path.realpath(expected_path) or
        os.path.abspath(inherited_path) != expected_path
    ):
        raise PersistentControllerError("controller bootstrap liveness path does not match its epoch")
    if os.path.islink(expected_path) or not os.path.isfile(expected_path):
        raise PersistentControllerError("controller bootstrap liveness path is unsafe")
    descriptor_stat = os.fstat(descriptor)
    path_stat = os.stat(expected_path)
    if descriptor_stat.st_dev != path_stat.st_dev or descriptor_stat.st_ino != path_stat.st_ino:
        raise PersistentControllerError("controller bootstrap liveness path does not match its descriptor")


def _safe_controller_environment(project_root, liveness_descriptor, liveness_path):
    environment = clean_gui_environment(os.environ)
    environment["PYTHONPATH"] = project_root
    environment[_LIVENESS_FD_ENV] = str(int(liveness_descriptor))
    environment[_LIVENESS_PATH_ENV] = os.path.abspath(liveness_path)
    environment[_LIVENESS_SCHEMA_ENV] = LIVENESS_LOCK_SCHEMA
    return environment


def _next_epoch_root(slot_root, minimum_index=0):
    ensure_dir(slot_root)
    indices = []
    for name in os.listdir(slot_root):
        if not name.startswith("epoch_"):
            continue
        try:
            indices.append(int(name.split("_", 1)[1]))
        except (IndexError, ValueError):
            continue
    index = max(max(indices) + 1 if indices else 0, int(minimum_index))
    return ensure_dir(os.path.join(slot_root, "epoch_%03d" % index)), index


def _worker_epoch_roots(run_root, worker_index):
    workers_root = os.path.join(os.path.abspath(run_root), "workers")
    if not os.path.lexists(workers_root):
        return []
    if os.path.islink(workers_root) or not os.path.isdir(workers_root):
        raise PersistentControllerError("worker scan root is not a real directory: %s" % workers_root)
    root = os.path.join(workers_root, "worker_%03d" % int(worker_index))
    if not os.path.lexists(root):
        return []
    if os.path.islink(root) or not os.path.isdir(root):
        raise PersistentControllerError("worker slot is not a real directory: %s" % root)
    result = []
    for name in sorted(os.listdir(root)):
        path = os.path.join(root, name)
        if os.path.islink(path):
            raise PersistentControllerError("worker slot contains a symbolic link: %s" % path)
        if not name.startswith("epoch_"):
            continue
        if parse_worker_epoch_component(name) is None:
            raise PersistentControllerError("worker epoch has an invalid name: %s" % name)
        if not os.path.isdir(path):
            raise PersistentControllerError("worker epoch is not a real directory: %s" % path)
        result.append(os.path.abspath(path))
    return result


def _worker_epoch_owned_by_controller(epoch_root, controller_epoch):
    name = os.path.basename(os.path.abspath(epoch_root))
    controller_epoch = int(controller_epoch)
    namespace = parse_worker_epoch_component(name)
    return bool(
        namespace is not None
        and int(namespace["controller_epoch"]) == controller_epoch
    )


def _guardian_proof(path, expected_token_identity=None):
    try:
        state = load_json(path)
    except (IOError, OSError, ValueError) as error:
        return {"ok": False, "state_path": path, "error": "%s: %s" % (error.__class__.__name__, error)}
    if not isinstance(state, dict):
        return {
            "ok": False, "state_path": path,
            "error": "guardian state is not a JSON object",
            "kind": "guardian_state_malformed",
        }
    if (
            expected_token_identity is not None
            and state.get("token") != expected_token_identity.get("token")):
        return {
            "ok": False, "state_path": path,
            "error": "guardian state token does not match its worker identity",
            "kind": "guardian_token_identity_mismatch",
        }
    status = state.get("status")
    if (
        status == "launch_failed" and "pid" in state and "pgid" in state and
        state.get("pid") is None and state.get("pgid") is None
    ):
        return {"ok": True, "state_path": path, "status": status, "kind": "ocean_not_spawned"}
    remaining_group = list(state.get("remaining_group_pids") or [])
    remaining_adopted = list(state.get("remaining_adopted_pids") or [])
    ok = bool(
        status == "exited"
        and state.get("group_cleanup_complete") is True
        and state.get("descendant_cleanup_complete") is True
        and state.get("descendant_supervision_complete") is True
        and not remaining_group
        and not remaining_adopted
    )
    return {
        "ok": ok,
        "state_path": path,
        "status": status,
        "kind": "guardian_cleanup_proven" if ok else "guardian_cleanup_unproven",
        "remaining_group_pids": remaining_group,
        "remaining_adopted_pids": remaining_adopted,
    }


def _worker_launch_lock_status(path):
    if fcntl is None:
        return {"state": "unsafe", "reason": "POSIX flock is unavailable"}
    try:
        path_stat = os.lstat(path)
        if stat.S_ISLNK(path_stat.st_mode) or not stat.S_ISREG(path_stat.st_mode):
            return {"state": "unsafe", "reason": "launch lock is not a regular file"}
        flags = os.O_RDWR | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(path, flags)
        try:
            opened_stat = os.fstat(descriptor)
            if not os.path.samestat(path_stat, opened_stat):
                return {"state": "unsafe", "reason": "launch lock changed while opening"}
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except (IOError, OSError) as error:
                if getattr(error, "errno", None) in (errno.EACCES, errno.EAGAIN):
                    return {"state": "held"}
                return {"state": "unsafe", "reason": "%s: %s" % (error.__class__.__name__, error)}
            return {"state": "released"}
        finally:
            os.close(descriptor)
    except (IOError, OSError) as error:
        return {"state": "unsafe", "reason": "%s: %s" % (error.__class__.__name__, error)}


def _worker_epoch_proof(epoch_root):
    epoch_root = os.path.abspath(epoch_root)
    state_path = os.path.join(epoch_root, _contract_text("worker_state.json"))
    state = None
    if os.path.lexists(state_path):
        if os.path.islink(state_path) or not os.path.isfile(state_path):
            return {"ok": False, "epoch_root": epoch_root, "kind": "unsafe_worker_state"}
        try:
            state = load_json(state_path)
        except (IOError, OSError, ValueError) as error:
            return {
                "ok": False, "epoch_root": epoch_root,
                "error": "%s: %s" % (error.__class__.__name__, error),
            }
        if not isinstance(state, dict):
            return {
                "ok": False, "epoch_root": epoch_root,
                "kind": "worker_state_malformed",
                "error": "worker state is not a JSON object",
            }

    namespace = parse_worker_epoch_namespace(
        os.path.basename(os.path.dirname(epoch_root)), os.path.basename(epoch_root),
    )
    expected_control = None
    guardian_token_identity = None
    if namespace is not None:
        from .persistent_maestro import persistent_worker_control_root

        expected_control = persistent_worker_control_root(
            epoch_root, namespace["epoch_identity"],
        )
        if state is not None:
            if state.get("schema_version") not in (None, _contract_text("crescendo.persistent_maestro_worker.v1")):
                return {"ok": False, "epoch_root": epoch_root, "kind": "worker_state_schema_mismatch"}
            if state.get("worker_index") is not None:
                try:
                    recorded_worker_index = int(state.get("worker_index"))
                except (TypeError, ValueError):
                    return {"ok": False, "epoch_root": epoch_root, "kind": "worker_state_identity_mismatch"}
                if recorded_worker_index != namespace["worker_index"]:
                    return {"ok": False, "epoch_root": epoch_root, "kind": "worker_state_identity_mismatch"}
            if state.get("epoch") is not None and state.get("epoch") != namespace["epoch_identity"]:
                return {"ok": False, "epoch_root": epoch_root, "kind": "worker_state_identity_mismatch"}
            for key, expected in (
                ("workspace_root", os.path.join(epoch_root, "workspace")),
                ("control_root", expected_control),
                ("guardian_state_path", os.path.join(expected_control, _contract_text("guardian_state.json"))),
            ):
                value = state.get(key)
                if value is not None and (
                    not isinstance(value, str) or not os.path.isabs(value)
                    or os.path.realpath(value) != os.path.realpath(expected)
                ):
                    return {"ok": False, "epoch_root": epoch_root, "kind": "worker_state_identity_mismatch"}
            try:
                guardian_token_identity = resolve_worker_guardian_token_identity(
                    state, namespace["epoch_identity"], namespace["worker_index"],
                )
            except ValueError as error:
                return {
                    "ok": False, "epoch_root": epoch_root,
                    "kind": "guardian_token_identity_mismatch", "error": str(error),
                }

    guardian_paths = []
    epochs_root = os.path.join(epoch_root, "epochs")
    if os.path.lexists(epochs_root):
        if os.path.islink(epochs_root) or not os.path.isdir(epochs_root):
            return {"ok": False, "epoch_root": epoch_root, "kind": "unsafe_epoch_state_root"}
        names = sorted(os.listdir(epochs_root))
        if expected_control is not None:
            expected_name = os.path.basename(os.path.dirname(expected_control))
            unexpected = [name for name in names if name != expected_name]
            if unexpected:
                return {"ok": False, "epoch_root": epoch_root, "kind": "unexpected_child_epoch_state"}
        for name in names:
            child_root = os.path.join(epochs_root, name)
            if os.path.islink(child_root) or not os.path.isdir(child_root):
                return {"ok": False, "epoch_root": epoch_root, "kind": "unsafe_child_epoch_state"}
            control_root = os.path.join(child_root, "control")
            if not os.path.lexists(control_root):
                continue
            if os.path.islink(control_root) or not os.path.isdir(control_root):
                return {"ok": False, "epoch_root": epoch_root, "kind": "unsafe_guardian_control"}
            path = os.path.join(control_root, _contract_text("guardian_state.json"))
            if not os.path.lexists(path):
                continue
            if os.path.islink(path) or not os.path.isfile(path):
                return {"ok": False, "epoch_root": epoch_root, "kind": "unsafe_guardian_state"}
            guardian_paths.append(path)

    expected_manifest = (
        os.path.join(expected_control, LEGACY_GUARDIAN_MANIFEST_NAME)
        if expected_control is not None else None
    )
    expected_lock = (
        os.path.join(expected_control, LEGACY_GUARDIAN_LAUNCH_LOCK_NAME)
        if expected_control is not None else None
    )
    durable_values = {
        "protocol": (state or {}).get("guardian_launch_protocol"),
        "manifest_path": (state or {}).get("guardian_launch_manifest_path"),
        "lock_path": (state or {}).get("guardian_launch_lock_path"),
    }
    durable_recorded = any(value is not None for value in durable_values.values())
    manifest_exists = bool(expected_manifest and os.path.lexists(expected_manifest))
    lock_exists = bool(expected_lock and os.path.lexists(expected_lock))
    if durable_recorded:
        if expected_control is None or any(value is None for value in durable_values.values()):
            return {"ok": False, "epoch_root": epoch_root, "kind": "durable_launch_identity_incomplete"}
        if durable_values["protocol"] != LEGACY_GUARDIAN_PROTOCOL:
            return {"ok": False, "epoch_root": epoch_root, "kind": "durable_launch_protocol_mismatch"}
        for key, expected in (("manifest_path", expected_manifest), ("lock_path", expected_lock)):
            value = durable_values[key]
            if (
                not isinstance(value, str) or not os.path.isabs(value)
                or os.path.abspath(value) != os.path.abspath(expected)
                or os.path.realpath(value) != os.path.realpath(expected)
            ):
                return {"ok": False, "epoch_root": epoch_root, "kind": "durable_launch_identity_mismatch"}
        if not manifest_exists or not lock_exists:
            return {"ok": False, "epoch_root": epoch_root, "kind": "durable_launch_artifact_missing"}

    if manifest_exists:
        inspection = inspect_legacy_guardian_attempt(
            expected_control, expected_workspace_root=os.path.join(epoch_root, "workspace"),
            expected_token_identity=guardian_token_identity,
        )
        termination_proof = inspection.get("termination_proof") or {}
        ok = bool(inspection.get("state") == "dead" and termination_proof.get("ok") is True)
        return {
            "ok": ok,
            "epoch_root": epoch_root,
            "kind": termination_proof.get("kind") or "durable_guardian_inspection_failed",
            "durable_guardian": inspection,
        }
    if lock_exists:
        lock_status = _worker_launch_lock_status(expected_lock)
        if lock_status.get("state") == "held":
            return {"ok": False, "epoch_root": epoch_root, "kind": "launch_in_progress"}
        if lock_status.get("state") != "released":
            return {
                "ok": False, "epoch_root": epoch_root, "kind": "launch_lock_unproven",
                "error": lock_status.get("reason"),
            }
        pristine = bool(
            state is None or (
                state.get("state") == "new"
                and state.get("pid") is None and state.get("ocean_pid") is None
            )
        )
        return {
            "ok": pristine,
            "epoch_root": epoch_root,
            "kind": (
                "worker_not_created" if pristine and state is None else
                ("ocean_not_spawned" if pristine else "launch_manifest_missing")
            ),
        }
    if durable_recorded:
        return {"ok": False, "epoch_root": epoch_root, "kind": "durable_launch_manifest_missing"}
    if guardian_paths:
        proofs = [
            _guardian_proof(path, guardian_token_identity) for path in guardian_paths
        ]
        ok = all(row.get("ok") for row in proofs)
        all_not_spawned = bool(ok and all(row.get("kind") == "ocean_not_spawned" for row in proofs))
        return {
            "ok": ok,
            "epoch_root": epoch_root,
            "guardians": proofs,
            "kind": "ocean_not_spawned" if all_not_spawned else (
                "guardian_cleanup_proven" if ok else "guardian_cleanup_unproven"
            ),
        }
    if state is None:
        return {"ok": True, "epoch_root": epoch_root, "kind": "worker_not_created"}
    if (
        state.get("state") == "new"
        and "pid" in state and "ocean_pid" in state
        and state.get("pid") is None and state.get("ocean_pid") is None
    ):
        return {"ok": True, "epoch_root": epoch_root, "kind": "ocean_not_spawned"}
    return {"ok": False, "epoch_root": epoch_root, "kind": "guardian_cleanup_record_missing"}


def _active_worker_session_identity(run_root, worker_index, controller_epoch,
                                    baseline_worker_epochs, health):
    result = {
        "schema_version": _contract_text("crescendo.persistent_session_identity.v1"),
        "proven": False,
        "worker_index": int(worker_index),
        "controller_epoch": controller_epoch,
    }
    try:
        controller_epoch = int(controller_epoch)
        roots = set(_worker_epoch_roots(run_root, worker_index))
        current = sorted(roots - set(baseline_worker_epochs or []))
        owned = [
            root for root in current
            if _worker_epoch_owned_by_controller(root, controller_epoch)
        ]
        foreign = sorted(set(current) - set(owned))
        if foreign:
            raise ValueError("worker epochs outside the active controller namespace exist")
        if len(owned) != 1:
            raise ValueError("expected exactly one active worker epoch, found %d" % len(owned))
        epoch_root = owned[0]
        namespace = parse_worker_epoch_namespace(
            os.path.basename(os.path.dirname(epoch_root)), os.path.basename(epoch_root),
        )
        if namespace is None or int(namespace["controller_epoch"]) != controller_epoch:
            raise ValueError("active worker epoch namespace is invalid")
        state_path = os.path.join(epoch_root, _contract_text("worker_state.json"))
        state_stat = os.lstat(state_path)
        if stat.S_ISLNK(state_stat.st_mode) or not stat.S_ISREG(state_stat.st_mode):
            raise ValueError("active worker state is not a regular non-symbolic file")
        state = load_json(state_path)
        if not isinstance(state, dict):
            raise ValueError("active worker state is not a JSON object")
        from .persistent_maestro import WORKER_SCHEMA_VERSION, persistent_worker_control_root

        expected_epoch = namespace["epoch_identity"]
        expected_workspace = os.path.join(epoch_root, "workspace")
        expected_control = persistent_worker_control_root(epoch_root, expected_epoch)
        expected = {
            "schema_version": WORKER_SCHEMA_VERSION,
            "worker_index": int(worker_index),
            "epoch": expected_epoch,
            "workspace_root": os.path.abspath(expected_workspace),
            "control_root": os.path.abspath(expected_control),
        }
        observed = {
            "schema_version": state.get("schema_version"),
            "worker_index": state.get("worker_index"),
            "epoch": state.get("epoch"),
            "workspace_root": os.path.abspath(state.get("workspace_root") or ""),
            "control_root": os.path.abspath(state.get("control_root") or ""),
        }
        if observed != expected:
            raise ValueError("active worker state identity mismatch")
        if (
                state.get("state") not in ("ready", "busy", "awaiting_cleanup", "cleaning")
                or state.get("tainted") is not False or state.get("closed") is not False):
            raise ValueError("active worker session is not reusable")
        session_pid = state.get("pid")
        session_start_ticks = state.get("guardian_start_ticks")
        if (
                isinstance(session_pid, bool) or isinstance(session_start_ticks, bool)
                or not isinstance(session_pid, int) or session_pid <= 1
                or not isinstance(session_start_ticks, int) or session_start_ticks <= 0):
            raise ValueError("active worker process identity is incomplete")
        process_rows = health.get("processes") if isinstance(health, dict) else None
        process_identities = set(
            (row.get("pid"), row.get("start_ticks"))
            for row in (process_rows or []) if isinstance(row, dict)
        )
        if (session_pid, session_start_ticks) not in process_identities:
            raise ValueError("active worker process is not in the owned controller tree")
        result.update({
            "proven": True,
            "epoch": expected_epoch,
            "local_epoch": int(namespace["local_epoch"]),
            "pid": session_pid,
            "pid_start_ticks": session_start_ticks,
            "state": state.get("state"),
            "epoch_root": epoch_root,
        })
        return result
    except (IOError, OSError, TypeError, ValueError) as error:
        result["reason"] = "%s: %s" % (error.__class__.__name__, error)
        return result


class PersistentControllerProxy(object):
    def __init__(self, run_root, worker_index, prepared, selected, config, command_builder=None):
        self.run_root = os.path.abspath(run_root)
        self.worker_index = int(worker_index)
        self.prepared = prepared
        self.selected = list(selected)
        self.config = config
        self.command_builder = command_builder
        self.slot_root = ensure_dir(os.path.join(
            self.run_root, "controllers", "slot_%03d" % self.worker_index,
        ))
        self.epoch_root = None
        self.epoch_index = None
        self.token = None
        self.process = None
        self._pgid = None
        self._start_ticks = None
        self._sequence = 0
        self._outstanding = None
        self._stdout_handle = None
        self._stderr_handle = None
        self.liveness_lock_path = None
        self._liveness_lock_descriptor = None
        self._baseline_worker_epochs = set()
        self._last_termination = None
        self._cleanup_attempt_count = 0
        self._cleanup_attempt_history = []
        self._rpc_failed_reason = None
        self._next_epoch_index = 0
        self._launch_boot_id = None
        self._parent_pid = None
        self._parent_start_ticks = None
        self._termination_lock = threading.RLock()

    @property
    def state_path(self):
        return os.path.join(self.epoch_root, _contract_text("controller_state.json")) if self.epoch_root else None

    def _write_state(self, status, **updates):
        document = {
            "schema_version": SCHEMA_VERSION,
            "token": self.token,
            "worker_index": self.worker_index,
            "controller_epoch": self.epoch_index,
            "status": status,
            "pid": getattr(self.process, "pid", None),
            "pgid": self._pgid,
            "pid_start_ticks": self._start_ticks,
            "launch_boot_id": self._launch_boot_id,
            "parent_pid": self._parent_pid,
            "parent_start_ticks": self._parent_start_ticks,
            "run_root": self.run_root,
            "liveness_lock_schema": LIVENESS_LOCK_SCHEMA,
            "liveness_lock_path": self.liveness_lock_path,
            "updated_at": utc_timestamp(),
        }
        document.update(updates)
        atomic_write_json(self.state_path, document)

    def _close_logs(self):
        for handle_name in ("_stdout_handle", "_stderr_handle"):
            handle = getattr(self, handle_name)
            if handle is not None:
                try:
                    handle.close()
                except (IOError, OSError):
                    pass
                setattr(self, handle_name, None)

    def _close_parent_liveness_descriptor(self):
        descriptor = self._liveness_lock_descriptor
        self._liveness_lock_descriptor = None
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError:
                pass

    def start(self):
        if self.process is not None and self.process.poll() is None:
            return self
        if os.path.lexists(os.path.join(self.run_root, ".cleanup_pending")):
            raise PersistentControllerError("cannot start while proven-dead artifact cleanup is pending")
        if self.epoch_root is not None and os.path.lexists(self.epoch_root):
            raise PersistentControllerError(
                "cannot start while the previous controller epoch remains unreclaimed"
            )
        self.process = None
        self._pgid = None
        self._start_ticks = None
        self.epoch_root, self.epoch_index = _next_epoch_root(
            self.slot_root, self._next_epoch_index,
        )
        self._next_epoch_index = self.epoch_index + 1
        self.token = uuid.uuid4().hex
        self._sequence = 0
        self._outstanding = None
        self._last_termination = None
        self._rpc_failed_reason = None
        self._launch_boot_id = _linux_boot_id()
        self._parent_pid = os.getpid()
        self._parent_start_ticks = (_process_stat(self._parent_pid) or {}).get("start_ticks")
        self._baseline_worker_epochs = set(_worker_epoch_roots(self.run_root, self.worker_index))
        self._batch_native_inflight = False
        self._native_pool_lifetime_active = False
        self._batch_request_identity = None
        self.liveness_lock_path = os.path.join(self.epoch_root, "controller_liveness.lock")
        self._liveness_lock_descriptor = _open_controller_liveness_lock(self.liveness_lock_path)
        try:
            requests_root = ensure_dir(os.path.join(self.epoch_root, "requests"))
            responses_root = ensure_dir(os.path.join(self.epoch_root, "responses"))
            bootstrap_path = os.path.join(self.epoch_root, _contract_text("bootstrap.json"))
            bootstrap = {
                "schema_version": SCHEMA_VERSION,
                "token": self.token,
                "worker_index": self.worker_index,
                "controller_epoch": self.epoch_index,
                "run_root": self.run_root,
                "requests_root": requests_root,
                "responses_root": responses_root,
                "state_path": self.state_path,
                "ready_path": os.path.join(self.epoch_root, _contract_text("ready.json")),
                "parent_pid": self._parent_pid,
                "parent_start_ticks": self._parent_start_ticks,
                "launch_boot_id": self._launch_boot_id,
                "liveness_lock_schema": LIVENESS_LOCK_SCHEMA,
                "liveness_lock_path": self.liveness_lock_path,
                "prepared": self.prepared,
                "selected": self.selected,
                "config": self.config,
            }
            atomic_write_json(bootstrap_path, bootstrap)
            self._write_state("launch_intent", bootstrap_path=bootstrap_path)
            from .runtime import application_root
            project_root = application_root(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            command = (
                self.command_builder(bootstrap_path)
                if self.command_builder is not None else
                [sys.executable, "-m", _contract_text("crescendo_engine.persistent_controller"), "--bootstrap", bootstrap_path]
            )
            self._stdout_handle = open(os.path.join(self.epoch_root, "controller.stdout"), "ab", 0)
            self._stderr_handle = open(os.path.join(self.epoch_root, "controller.stderr"), "ab", 0)
            liveness_descriptor = self._liveness_lock_descriptor
            self.process = subprocess.Popen(
                command,
                cwd=project_root,
                env=_safe_controller_environment(
                    project_root, liveness_descriptor, self.liveness_lock_path,
                ),
                stdout=self._stdout_handle,
                stderr=self._stderr_handle,
                start_new_session=True,
                close_fds=True,
                pass_fds=(liveness_descriptor,),
            )
            self._close_parent_liveness_descriptor()
            self._pgid = os.getpgid(self.process.pid)
            stat = _process_stat(self.process.pid) or {}
            self._start_ticks = stat.get("start_ticks")
            if self._pgid != self.process.pid:
                raise PersistentControllerProcessError("controller did not start in an isolated process group")
            self._write_state("starting", command=list(command))
            timeout = max(1.0, float(self.config.get("runtime", {}).get(
                "controller_startup_timeout_seconds", 30,
            )))
            deadline = time.monotonic() + timeout
            ready_path = bootstrap["ready_path"]
            while time.monotonic() < deadline:
                if os.path.isfile(ready_path):
                    ready = load_json(ready_path)
                    if ready.get("token") != self.token or ready.get("schema_version") != SCHEMA_VERSION:
                        raise PersistentControllerProcessError("controller ready identity mismatch")
                    self._write_state("ready", command=list(command))
                    return self
                if self.process.poll() is not None:
                    raise PersistentControllerProcessError(
                        "controller exited before ready with code %s" % self.process.returncode
                    )
                time.sleep(0.05)
            raise PersistentControllerTimeout("controller startup timed out after %.1f seconds" % timeout)
        except Exception:
            self._close_parent_liveness_descriptor()
            self._close_logs()
            self.terminate_and_prove("controller startup failed")
            raise

    def _new_request(self, operation, payload):
        if self.process is None or self.process.poll() is not None:
            raise PersistentControllerProcessError("controller is not alive")
        if self._rpc_failed_reason is not None:
            raise PersistentControllerProcessError(
                "controller RPC is tainted: %s" % self._rpc_failed_reason
            )
        if self._outstanding is not None:
            raise PersistentControllerError("controller already has an outstanding request")
        self._sequence += 1
        nonce = uuid.uuid4().hex
        identity = {
            "schema_version": SCHEMA_VERSION,
            "token": self.token,
            "controller_epoch": self.epoch_index,
            "sequence": self._sequence,
            "nonce": nonce,
            "operation": operation,
        }
        name = "%08d_%s.json" % (self._sequence, nonce)
        request = dict(identity)
        request["payload"] = payload
        request_path = os.path.join(self.epoch_root, "requests", name)
        response_path = os.path.join(self.epoch_root, "responses", name)
        request["response_path"] = response_path
        atomic_write_json(request_path, request)
        identity.update({
            "request_path": request_path,
            "response_path": response_path,
            "started_monotonic": time.monotonic(),
        })
        self._outstanding = identity
        if (getattr(self, "_batch_native_inflight", False)
                or getattr(self, "_native_pool_lifetime_active", False)
                or self.config.get("runtime", {}).get("native_cluster_stop_required")):
            # ACK/readback/COMMIT become the current native request fence,
            # even while the persistent cluster pool is between generations.
            self._batch_request_identity = identity
        return identity

    def submit_evaluate(self, index, generation, values, spec):
        request = self._new_request("EVALUATE", {
            "index": int(index),
            "generation": int(generation),
            "values": values,
            "spec": spec,
        })
        if self.config.get("runtime", {}).get("native_cluster_stop_required"):
            self._batch_native_inflight = True
            self._native_pool_lifetime_active = True
            self._batch_request_identity = request
        return request

    def submit_evaluate_batch(self, planned, generation, spec):
        request = self._new_request("EVALUATE_BATCH", {
            "planned": [[int(index), values] for index, values in planned],
            "generation": int(generation), "spec": spec,
        })
        self._batch_request_identity = request
        self._batch_native_inflight = True
        self._native_pool_lifetime_active = True
        return request

    def poll(self, request=None):
        request = request or self._outstanding
        if request is None:
            raise PersistentControllerError("controller has no outstanding request")
        if request is not self._outstanding:
            raise PersistentControllerError("controller request is no longer active")
        path = request["response_path"]
        if not os.path.isfile(path):
            if self.process is not None and self.process.poll() is not None:
                self._outstanding = None
                self._close_logs()
                error = PersistentControllerProcessError(
                    "controller exited with code %s before %s response" % (
                        self.process.returncode, request["operation"],
                    )
                )
                self._rpc_failed_reason = str(error)
                raise error
            return None
        try:
            response = load_json(path)
            for key in ("schema_version", "token", "controller_epoch", "sequence", "nonce", "operation"):
                if response.get(key) != request.get(key):
                    raise PersistentControllerProcessError(
                        "controller response identity mismatch for %s" % key
                    )
            removable = [request["request_path"]]
            if response.get("ok"):
                removable.append(request["response_path"])
            for artifact_path in removable:
                try:
                    os.unlink(artifact_path)
                except OSError as error:
                    if getattr(error, "errno", None) != errno.ENOENT:
                        raise
            if not response.get("ok"):
                error_message = response.get("error") or "controller request failed"
                if response.get("error_type") is not None:
                    raise PersistentControllerRemoteError(
                        error_message,
                        remote_error_type=response.get("error_type"),
                        failure_class=response.get("failure_class"),
                        fatal_run=response.get("fatal_run") is True,
                        remote_traceback=response.get("traceback"),
                    )
                raise PersistentControllerProcessError(error_message)
            result = response.get("result")
            if request.get("operation") == "EVALUATE_BATCH" and isinstance(result, dict):
                self._batch_native_inflight = False
            elif request.get("operation") == "EVALUATE" and getattr(self, "_batch_native_inflight", False):
                from .cluster_execution import native_job_state_quiescent
                self._batch_native_inflight = not native_job_state_quiescent(
                    result.get("native_job_state") if isinstance(result, dict) else None,
                )
            if request.get("operation") == "CLOSE" and isinstance(result, dict):
                remote = result.get("remote_stop") or {}
                if (result.get("remote_stop_proven") is True
                        or isinstance(remote, dict) and remote.get("remote_stop_proven") is True):
                    self._native_pool_lifetime_active = False
                    self._batch_native_inflight = False
            return result
        except PersistentControllerProcessError as error:
            self._rpc_failed_reason = str(error)
            raise
        except Exception as error:
            converted = PersistentControllerProcessError(
                "controller response handling failed for %s: %s: %s" % (
                    request.get("operation"), error.__class__.__name__, error,
                )
            )
            self._rpc_failed_reason = str(converted)
            raise converted
        finally:
            self._outstanding = None

    def _wait(self, request, timeout, operation):
        deadline = time.monotonic() + max(0.1, float(timeout))
        while time.monotonic() < deadline:
            result = self.poll(request)
            if result is not None:
                return result
            time.sleep(0.05)
        raise PersistentControllerTimeout("controller %s timed out after %.1f seconds" % (operation, timeout))

    def commit_candidate(self, candidate_id):
        request = self._new_request("COMMIT", {"candidate_id": candidate_id})
        timeout = max(1.0, float(self.config.get("runtime", {}).get(
            "controller_commit_timeout_seconds", 60,
        )))
        try:
            result = self._wait(request, timeout, "commit")
            if not isinstance(result, dict):
                raise PersistentControllerProcessError(
                    "controller commit returned a non-object result"
                )
            worker_health = result.get("health") if isinstance(result.get("health"), dict) else {}
            health = self.sample_health()
            session_identity = dict(health.get("session_identity") or {})
            worker_epoch = worker_health.get("epoch")
            worker_start_count = worker_health.get("start_count")
            if session_identity.get("proven") is True:
                try:
                    worker_start_count = int(worker_start_count)
                except (TypeError, ValueError, OverflowError):
                    worker_start_count = None
                if (
                        worker_epoch != session_identity.get("epoch")
                        or worker_start_count != int(session_identity.get("local_epoch")) + 1):
                    session_identity.update({
                        "proven": False,
                        "reason": "authenticated worker health did not match the active session",
                    })
                else:
                    session_identity["worker_start_count"] = worker_start_count
            health.update({
                "epoch": worker_epoch,
                "worker_start_count": worker_start_count,
                "session_identity": session_identity,
                "session_identity_proven": session_identity.get("proven") is True,
            })
            result["health"] = health
            return result
        except Exception as error:
            termination = self.terminate_and_prove("controller commit failed: %s" % error)
            recycled = False
            restart_error = None
            if termination.get("termination_proven"):
                try:
                    self.restart()
                    recycled = True
                except Exception as replacement_error:
                    restart_error = "%s: %s" % (
                        replacement_error.__class__.__name__, replacement_error,
                    )
                    termination["cleanup"] = dict(
                        termination.get("cleanup") or {}, ok=False,
                        replacement_error=restart_error,
                    )
            return {
                "ok": False,
                "state": "controller_commit_failed_recycled" if recycled else "controller_commit_failed",
                "error": "%s: %s" % (error.__class__.__name__, error),
                "replacement_error": restart_error,
                "recycled": recycled,
                "retirement": termination,
            }

    def sample_health(self):
        health = sample_owned_process_tree(
            [{
                "pid": getattr(self.process, "pid", None),
                "start_ticks": self._start_ticks,
                "role": "persistent_controller",
            }],
            storage_paths=[self.run_root, self.epoch_root],
            log_paths=self._known_persistent_log_paths(),
        )
        health.update({
            "worker_index": self.worker_index,
            "controller_epoch": self.epoch_index,
        })
        session_identity = _active_worker_session_identity(
            self.run_root, self.worker_index, self.epoch_index,
            self._baseline_worker_epochs, health,
        )
        health.update({
            "session_identity": session_identity,
            "session_identity_proven": session_identity.get("proven") is True,
        })
        health["startup_phase"] = self._authenticated_startup_phase(health)
        return health

    def _authenticated_startup_phase(self, health):
        """Only the live controller's current RPC can authorize preparation time."""
        result = {"proven": False}
        request = self._outstanding
        if not request or request.get("operation") not in ("EVALUATE", "EVALUATE_BATCH"):
            return result
        try:
            if (
                    self.process is None or self.process.poll() is not None
                    or health.get("alive") is not True
                    or health.get("tree_complete") is not True
                    or not self._controller_identity_is_owned()):
                raise ValueError("startup controller process ownership is unproven")
            document = self._load_epoch_identity_document(
                self.state_path, "controller startup state",
            )
            expected = {
                "schema_version": SCHEMA_VERSION, "token": self.token,
                "controller_epoch": self.epoch_index, "worker_index": self.worker_index,
                "pid": self.process.pid, "pid_start_ticks": self._start_ticks,
                "pgid": self._pgid, "run_root": self.run_root,
                "status": "processing",
            }
            if any(document.get(key) != value for key, value in expected.items()):
                raise ValueError("controller startup identity mismatch")
            if document.get("active_request") != _request_identity(request):
                raise ValueError("controller startup request identity mismatch")
            phase = document.get("startup_phase")
            if not isinstance(phase, dict) or phase.get("stage") not in (
                    "workspace_prepare", "ocean_starting", "session_ready"):
                raise ValueError("controller startup phase is not available")
            now = time.monotonic()
            request_started = float(request["started_monotonic"])
            started = float(phase["started_monotonic"])
            stage_started = float(phase["stage_started_monotonic"])
            first_ocean = phase.get("first_ocean_started_monotonic")
            times = [request_started, started, stage_started]
            if first_ocean is not None:
                first_ocean = float(first_ocean)
                times.append(first_ocean)
            if (
                    not all(math.isfinite(value) for value in times)
                    or started < request_started - 1.0 or stage_started < started
                    or max(times) > now + 0.5
                    or (first_ocean is not None and first_ocean < started)):
                raise ValueError("controller startup timestamps are invalid")
            epoch = phase.get("epoch")
            if epoch is not None and (not isinstance(epoch, str) or not epoch):
                raise ValueError("controller startup epoch is invalid")
            result.update({
                "proven": True, "stage": phase["stage"], "epoch": epoch,
                "started_monotonic": started,
                "stage_started_monotonic": stage_started,
                "first_ocean_started_monotonic": first_ocean,
                "request_started_monotonic": request_started,
                "controller_epoch": self.epoch_index,
                "controller_pid": self.process.pid,
                "controller_pid_start_ticks": self._start_ticks,
                "request_sequence": request["sequence"],
                "request_nonce": request["nonce"],
            })
        except (IOError, OSError, KeyError, TypeError, ValueError, PersistentControllerError) as error:
            result["reason"] = "%s: %s" % (error.__class__.__name__, error)
        return result

    def _known_persistent_log_paths(self):
        paths = []
        if self.epoch_root:
            paths.extend([
                os.path.join(self.epoch_root, "controller.stdout"),
                os.path.join(self.epoch_root, "controller.stderr"),
            ])
        for worker_epoch in _worker_epoch_roots(self.run_root, self.worker_index):
            epochs_root = os.path.join(worker_epoch, "epochs")
            if not os.path.lexists(epochs_root):
                continue
            if os.path.islink(epochs_root) or not os.path.isdir(epochs_root):
                raise PersistentControllerError(
                    "worker health log root is not a real directory: %s" % epochs_root
                )
            for name in sorted(os.listdir(epochs_root)):
                epoch_root = os.path.join(epochs_root, name)
                if os.path.islink(epoch_root) or not os.path.isdir(epoch_root):
                    raise PersistentControllerError(
                        "worker health epoch is not a real directory: %s" % epoch_root
                    )
                control_root = os.path.join(epoch_root, "control")
                if not os.path.lexists(control_root):
                    continue
                if os.path.islink(control_root) or not os.path.isdir(control_root):
                    raise PersistentControllerError(
                        "worker health control root is not a real directory: %s" % control_root
                    )
                paths.extend([
                    os.path.join(control_root, "ocean.stdout"),
                    os.path.join(control_root, "ocean.stderr"),
                    os.path.join(control_root, _contract_text("spectre_timeout.log")),
                ])
        return paths

    def _controller_identity_is_owned(self):
        if self.process is None or self.process.poll() is not None:
            return True
        stat = _process_stat(self.process.pid)
        return bool(
            stat
            and stat.get("start_ticks") == self._start_ticks
            and stat.get("pgid") == self._pgid
            and self._pgid == self.process.pid
        )

    def _load_epoch_identity_document(self, path, description):
        try:
            path_stat = os.lstat(path)
        except OSError as error:
            raise PersistentControllerError(
                "%s is missing or cannot be inspected: %s" % (description, error)
            )
        if stat.S_ISLNK(path_stat.st_mode) or not stat.S_ISREG(path_stat.st_mode):
            raise PersistentControllerError(
                "%s is not a regular non-symbolic file: %s" % (description, path)
            )
        try:
            document = load_json(path)
        except Exception as error:
            raise PersistentControllerError(
                "%s cannot be read safely: %s: %s" % (
                    description, error.__class__.__name__, error,
                )
            )
        if not isinstance(document, dict):
            raise PersistentControllerError("%s is not a JSON object" % description)
        return document

    def _dead_controller_identity_proof(self):
        if self.epoch_root is None or not os.path.lexists(self.epoch_root):
            previous = self._last_termination or {}
            saved = previous.get("controller_identity_proof") or {}
            current = _process_stat(getattr(self.process, "pid", None))
            if (previous.get("process_termination_proven") is True and saved.get("ok") is True
                    and saved.get("token") == self.token
                    and saved.get("controller_epoch") == self.epoch_index
                    and (self.process is None or self.process.poll() is not None)
                    and (not current or current.get("start_ticks") != self._start_ticks)):
                return dict(saved, retained_after_quarantine=True)
            return {"ok": False, "kind": "controller_epoch_missing"}
        if os.path.islink(self.epoch_root) or not os.path.isdir(self.epoch_root):
            return {"ok": False, "kind": "unsafe_controller_epoch"}
        expected_common = {
            "schema_version": SCHEMA_VERSION,
            "token": self.token,
            "worker_index": self.worker_index,
            "controller_epoch": self.epoch_index,
            "run_root": self.run_root,
            "parent_pid": self._parent_pid,
            "parent_start_ticks": self._parent_start_ticks,
            "launch_boot_id": self._launch_boot_id,
            "liveness_lock_schema": LIVENESS_LOCK_SCHEMA,
            "liveness_lock_path": self.liveness_lock_path,
        }
        try:
            bootstrap = self._load_epoch_identity_document(
                os.path.join(self.epoch_root, _contract_text("bootstrap.json")), "controller bootstrap",
            )
            state = self._load_epoch_identity_document(
                self.state_path, "controller state",
            )
            for description, document in (("bootstrap", bootstrap), ("state", state)):
                for key, expected in expected_common.items():
                    if document.get(key) != expected:
                        raise PersistentControllerError(
                            "controller %s identity mismatch for %s" % (description, key)
                        )
            expected_paths = {
                "requests_root": os.path.join(self.epoch_root, "requests"),
                "responses_root": os.path.join(self.epoch_root, "responses"),
                "state_path": self.state_path,
                "ready_path": os.path.join(self.epoch_root, _contract_text("ready.json")),
            }
            for key, expected in expected_paths.items():
                if os.path.abspath(bootstrap.get(key) or "") != os.path.abspath(expected):
                    raise PersistentControllerError(
                        "controller bootstrap path mismatch for %s" % key
                    )
            if self.process is None:
                if any(state.get(key) is not None for key in ("pid", "pgid", "pid_start_ticks")):
                    raise PersistentControllerError(
                        "controller-not-started state contains a process identity"
                    )
                return {
                    "ok": True, "kind": "controller_not_started",
                    "token": self.token, "controller_epoch": self.epoch_index,
                }
            if self.process.poll() is None:
                raise PersistentControllerError("controller process is still alive")
            if (
                not isinstance(self.process.pid, int) or self.process.pid <= 1 or
                self._pgid != self.process.pid or
                not isinstance(self._start_ticks, int) or self._start_ticks <= 0
            ):
                raise PersistentControllerError("controller launch identity is incomplete")
            expected_process = {
                "pid": self.process.pid,
                "pgid": self._pgid,
                "pid_start_ticks": self._start_ticks,
            }
            for key, expected in expected_process.items():
                if state.get(key) != expected:
                    raise PersistentControllerError(
                        "controller state process identity mismatch for %s" % key
                    )
            current = _process_stat(self.process.pid)
            if current and current.get("start_ticks") == self._start_ticks:
                raise PersistentControllerError(
                    "controller launch identity still exists after child wait"
                )
            return {
                "ok": True, "kind": "controller_termination_proven",
                "pid": self.process.pid, "pgid": self._pgid,
                "pid_start_ticks": self._start_ticks,
                "token": self.token, "controller_epoch": self.epoch_index,
            }
        except Exception as error:
            return {
                "ok": False, "kind": "controller_identity_unproven",
                "error": "%s: %s" % (error.__class__.__name__, error),
            }

    def _cleanup_dead_controller_epoch(self, identity_proof):
        if not identity_proof.get("ok"):
            return {
                "ok": False, "removed": False,
                "error": identity_proof.get("error") or "controller identity was not proven",
                "identity_proof": identity_proof,
            }
        if self.epoch_root is None or not os.path.lexists(self.epoch_root):
            return {
                "ok": True, "removed": False, "state": "already_removed",
                "identity_proof": identity_proof,
            }
        from .retention import cleanup_abandoned_controller_epoch

        try:
            self._write_state("terminated", identity_proof=identity_proof)
            self._close_logs()
            cleanup = cleanup_abandoned_controller_epoch(
                self.epoch_root, self.run_root,
                {"ok": True, "kind": identity_proof["kind"]},
            )
            cleanup["identity_proof"] = identity_proof
            return cleanup
        except Exception as error:
            result = {
                "ok": False, "removed": False,
                "error": "%s: %s" % (error.__class__.__name__, error),
                "identity_proof": identity_proof,
            }
            if getattr(error, "detail", None):
                result["detail"] = error.detail
            return result

    def _wait_worker_cleanup_proof(self, timeout):
        deadline = time.monotonic() + max(0.0, float(timeout))
        last = None
        foreign_seen = set()
        while True:
            current = set(_worker_epoch_roots(self.run_root, self.worker_index))
            roots = sorted(current - self._baseline_worker_epochs)
            owned_roots = [
                root for root in roots
                if _worker_epoch_owned_by_controller(root, self.epoch_index)
            ]
            foreign_seen.update(set(roots) - set(owned_roots))
            proofs = [_worker_epoch_proof(root) for root in owned_roots]
            foreign_roots = sorted(foreign_seen)
            last = {
                "ok": bool(
                    not foreign_roots and all(row.get("ok") for row in proofs)
                ),
                "worker_epochs": proofs,
                "foreign_worker_epochs": foreign_roots,
                "kind": (
                    "foreign_worker_epoch_namespace" if foreign_roots else
                    ("no_worker_epoch" if not owned_roots else "worker_epoch_cleanup")
                ),
            }
            if foreign_roots:
                last["error"] = (
                    "worker epochs outside controller namespace %d appeared after launch: %s"
                    % (self.epoch_index, ", ".join(foreign_roots))
                )
            if last["ok"] or time.monotonic() >= deadline:
                return last
            time.sleep(0.1)

    def _cleanup_proven_worker_epochs(self, proof):
        from .retention import cleanup_abandoned_worker_epoch
        from .workspace import verify_sources_unchanged

        results = []
        for row in proof.get("worker_epochs") or []:
            epoch_root = row.get("epoch_root")
            if not row.get("ok"):
                results.append({
                    "ok": False, "worker_epoch": epoch_root,
                    "error": "worker process cleanup was not proven",
                })
                continue
            if not epoch_root or not os.path.lexists(epoch_root):
                results.append({"ok": True, "worker_epoch": epoch_root, "removed": False})
                continue
            if os.path.islink(epoch_root) or not os.path.isdir(epoch_root):
                results.append({
                    "ok": False, "worker_epoch": epoch_root,
                    "error": "worker epoch changed to an unsafe filesystem object before cleanup",
                })
                continue
            try:
                cleanup = cleanup_abandoned_worker_epoch(
                    epoch_root, self.run_root, verify_sources_unchanged,
                    termination_proof=row,
                )
                results.append(cleanup)
            except Exception as error:
                result = {
                    "ok": False, "worker_epoch": epoch_root,
                    "error": "%s: %s" % (error.__class__.__name__, error),
                }
                if getattr(error, "detail", None):
                    result["detail"] = error.detail
                results.append(result)
        return {
            "ok": all(row.get("ok") for row in results),
            "removed": bool(results) and all(row.get("removed", False) for row in results),
            "worker_epochs": results,
        }

    def _finalize_dead_cleanup(self, identity_proof, worker_proof, native_stop,
                               process_error=None):
        """Keep process, remote job, and artifact proofs independent."""
        from .retention import (
            _require_remote_batch_quiescent, inspect_run_cleanup_scope, resume_pending_cleanups,
        )

        controller_dead = self.process is None or self.process.poll() is not None
        if self.epoch_root and os.path.lexists(self.epoch_root):
            liveness = (_worker_launch_lock_status(self.liveness_lock_path)
                        if self.liveness_lock_path else
                        {"state": "unsafe", "reason": "controller liveness lock identity is missing"})
        else:
            liveness = ({"state": "released", "retained_after_quarantine": True}
                        if identity_proof.get("retained_after_quarantine") else
                        {"state": "unsafe", "reason": "controller epoch identity is missing"})
        liveness_proven = liveness.get("state") == "released"
        process_proven = bool(controller_dead and identity_proof.get("ok")
                              and worker_proof.get("ok") and liveness_proven
                              and process_error is None)
        remote_rows = []
        for row in worker_proof.get("worker_epochs") or []:
            root = row.get("epoch_root")
            try:
                if root and os.path.lexists(root):
                    _require_remote_batch_quiescent(root)
                remote_rows.append({"worker_epoch": root, "remote_stop_proven": True})
            except Exception as error:
                remote_rows.append({"worker_epoch": root, "remote_stop_proven": False,
                                    "error": "%s: %s" % (error.__class__.__name__, error)})
        remote_proven = bool(native_stop.get("remote_stop_proven") is True
                             and all(row["remote_stop_proven"] for row in remote_rows))
        termination_proven = bool(process_proven and remote_proven)
        recovery = {"ok": True, "resumed": []}
        if termination_proven:
            try:
                recovery["resumed"] = resume_pending_cleanups(self.run_root)
            except Exception as error:
                recovery = {"ok": False, "error": "%s: %s" % (error.__class__.__name__, error)}
                if getattr(error, "detail", None):
                    recovery["detail"] = error.detail
        epoch_cleanup = (
            self._cleanup_proven_worker_epochs(worker_proof)
            if termination_proven and recovery.get("ok") else
            {"ok": False, "removed": False, "worker_epochs": []}
        )
        controller_cleanup = (
            self._cleanup_dead_controller_epoch(identity_proof)
            if termination_proven and recovery.get("ok") and epoch_cleanup.get("ok") else
            {"ok": False, "removed": False, "identity_proof": identity_proof,
             "error": (recovery.get("error") or process_error or identity_proof.get("error")
                       or (liveness.get("reason") or "controller liveness lock remains held"
                           if not liveness_proven else None)
                       or ("worker artifact cleanup failed; controller evidence retained"
                           if termination_proven else
                           "owned local or remote process termination was not proven"))}
        )
        workers_absent = all(not os.path.lexists(row["epoch_root"])
                             for row in worker_proof.get("worker_epochs") or [])
        run_cleanup_scope = inspect_run_cleanup_scope(self.run_root)
        artifacts_complete = bool(termination_proven and recovery.get("ok")
                                  and epoch_cleanup.get("ok") and controller_cleanup.get("ok")
                                  and workers_absent and not os.path.lexists(self.epoch_root)
                                  and not os.path.lexists(os.path.join(self.run_root, ".cleanup_pending")))
        cleanup = dict(epoch_cleanup)
        cleanup.update({
            "ok": artifacts_complete, "removed": artifacts_complete,
            "state": ("all_epochs_removed" if artifacts_complete else
                      ("artifact_cleanup_failed" if termination_proven else "unproven")),
            "process_termination_proven": process_proven,
            "remote_stop_proven": remote_proven,
            "artifact_cleanup_complete": artifacts_complete,
            "controller_liveness_proof": liveness,
            "controller_epoch": controller_cleanup,
            "worker_epoch_cleanup": epoch_cleanup,
            "pending_recovery": recovery,
            "scope": "owned_controller_and_worker_epochs",
            "run_cleanup_scope": run_cleanup_scope,
        })
        error = None
        if not artifacts_complete:
            error = (recovery.get("error") or
                     next((row.get("error") for row in epoch_cleanup.get("worker_epochs") or []
                           if not row.get("ok") and row.get("error")), None) or
                     controller_cleanup.get("error") or
                     next((row.get("error") for row in remote_rows if not row["remote_stop_proven"]), None)
                     or native_stop.get("reason") or "owned epoch cleanup was not proven")
        cleanup["error"] = error
        return {
            "termination_proven": termination_proven,
            "process_termination_proven": process_proven,
            "remote_stop_proven": remote_proven,
            "artifact_cleanup_complete": artifacts_complete,
            "remote_worker_proof": remote_rows,
            "cleanup": cleanup, "cleanup_error": error,
            "controller_epoch_cleanup": controller_cleanup,
        }

    def _record_cleanup_attempt(self, result):
        """Keep prior deletion failure separate from a later successful retry."""
        self._cleanup_attempt_count += 1
        cleanup = result.get("cleanup") or {}
        failures = []
        for row in ((cleanup.get("worker_epoch_cleanup") or {}).get("worker_epochs") or []):
            if row.get("detail"):
                failures.append(copy.deepcopy(row["detail"]))
        for row in (cleanup.get("controller_epoch") or {}, cleanup.get("pending_recovery") or {}):
            if row.get("detail"):
                failures.append(copy.deepcopy(row["detail"]))
        record = {"attempt": self._cleanup_attempt_count, "observed_at": utc_timestamp(),
                  "controller_epoch": self.epoch_index, "controller_token": self.token,
                  "worker_index": self.worker_index, "reason": result.get("reason"),
                  "process_termination_proven": result.get("process_termination_proven"),
                  "remote_stop_proven": result.get("remote_stop_proven"),
                  "artifact_cleanup_complete": result.get("artifact_cleanup_complete"),
                  "state": cleanup.get("state"), "error": result.get("error"),
                  "artifact_failures": failures}
        self._cleanup_attempt_history.append(record)
        self._cleanup_attempt_history = self._cleanup_attempt_history[-16:]
        result["cleanup_attempt_number"] = self._cleanup_attempt_count
        result["cleanup_attempt_history"] = copy.deepcopy(self._cleanup_attempt_history)
        result["cleanup_recovered_from_attempts"] = [
            row["attempt"] for row in self._cleanup_attempt_history[:-1]
            if result.get("artifact_cleanup_complete") is True
            and row.get("artifact_cleanup_complete") is False
            and row.get("controller_token") == self.token
            and row.get("controller_epoch") == self.epoch_index]
        return result

    def request_native_batch_stop(self, reason):
        required = bool(getattr(self, "_batch_native_inflight", False)
                        or getattr(self, "_native_pool_lifetime_active", False))
        if not required:
            return {"required": False, "remote_stop_proven": True}
        if self.process is None or self.process.poll() is not None:
            return {"required": True, "remote_stop_proven": False,
                    "reason": "live controller is unavailable for native cluster stop"}
        request = getattr(self, "_batch_request_identity", None)
        if not request or not self._controller_identity_is_owned():
            return {"required": True, "remote_stop_proven": False,
                    "reason": "owned live controller is unavailable for native cluster stop"}
        path = os.path.join(self.epoch_root, _contract_text("native_stop.json"))
        timeout = max(0.1, float(self.config.get("runtime", {}).get(
            "cluster_stop_timeout_seconds", 15,
        )))
        try:
            os.kill(self.process.pid, signal.SIGUSR1)
        except OSError as error:
            return {"required": True, "remote_stop_proven": False, "error": str(error)}
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if os.path.isfile(path) and not os.path.islink(path):
                try:
                    document = load_json(path)
                except (IOError, OSError, ValueError):
                    return {"required": True, "remote_stop_proven": False,
                            "reason": "native stop acknowledgement could not be read"}
                if not isinstance(document, dict):
                    return {"required": True, "remote_stop_proven": False,
                            "reason": "native stop acknowledgement is malformed"}
                keys = ("schema_version", "token", "controller_epoch", "sequence", "nonce", "operation")
                if all(document.get(key) == request.get(key) for key in keys):
                    native = document.get("native") or {}
                    if not isinstance(native, dict):
                        return {"required": True, "remote_stop_proven": False,
                                "reason": "native stop evidence is malformed"}
                    return {"required": True, "remote_stop_proven": native.get("remote_stop_proven") is True,
                            "native": native, "reason": reason}
            if self.process is None or self.process.poll() is not None:
                break
            time.sleep(0.05)
        return {"required": True, "remote_stop_proven": False,
                "reason": "native cluster stop acknowledgement unavailable; retain workspace"}

    def terminate_and_prove(self, reason):
        with self._termination_lock:
            return self._terminate_and_prove(reason)

    def _terminate_and_prove(self, reason):
        if (self._last_termination is not None
                and self._last_termination.get("artifact_cleanup_complete") is True
                and not os.path.lexists(self.epoch_root)
                and not os.path.lexists(os.path.join(self.run_root, ".cleanup_pending"))) and (
            self.process is None or self.process.poll() is not None
        ):
            return self._last_termination
        term_sent = False
        kill_sent = False
        identity_ok = self._controller_identity_is_owned()
        native_stop = (
            dict(self._last_termination.get("remote_stop") or {})
            if (self._last_termination or {}).get("remote_stop_proven") is True
            and (self.process is None or self.process.poll() is not None)
            else self.request_native_batch_stop(reason)
        )
        error = None
        term_grace = max(0.1, float(self.config.get("runtime", {}).get(
            "controller_term_grace_seconds", 5,
        )))
        kill_grace = max(0.1, float(self.config.get("runtime", {}).get(
            "controller_kill_grace_seconds", 5,
        )))
        if self.process is not None and self.process.poll() is None:
            if not identity_ok:
                error = "controller process identity mismatch; refusing to signal"
            else:
                try:
                    os.killpg(self._pgid, signal.SIGTERM)
                    term_sent = True
                except OSError as signal_error:
                    if getattr(signal_error, "errno", None) != errno.ESRCH:
                        error = "%s: %s" % (signal_error.__class__.__name__, signal_error)
                if error is None:
                    try:
                        self.process.wait(timeout=term_grace)
                    except subprocess.TimeoutExpired:
                        if self._controller_identity_is_owned():
                            try:
                                os.killpg(self._pgid, signal.SIGKILL)
                                kill_sent = True
                            except OSError as signal_error:
                                if getattr(signal_error, "errno", None) != errno.ESRCH:
                                    error = "%s: %s" % (signal_error.__class__.__name__, signal_error)
                        else:
                            error = "controller process identity changed before SIGKILL"
                    if error is None and self.process.poll() is None:
                        try:
                            self.process.wait(timeout=kill_grace)
                        except subprocess.TimeoutExpired:
                            error = "controller survived TERM and KILL grace periods"
        controller_dead = bool(self.process is None or self.process.poll() is not None)
        if controller_dead:
            try:
                proof = self._wait_worker_cleanup_proof(self.config.get("runtime", {}).get(
                    "controller_worker_cleanup_timeout_seconds", 15,
                ))
            except Exception as proof_error:
                proof = {
                    "ok": False,
                    "kind": "worker_epoch_scan_failed",
                    "worker_epochs": [],
                    "error": "%s: %s" % (proof_error.__class__.__name__, proof_error),
                }
                if error is None:
                    error = proof["error"]
        else:
            proof = {"ok": False, "kind": "controller_still_alive", "worker_epochs": []}
        identity_proof = (
            self._dead_controller_identity_proof()
            if controller_dead else
            {"ok": False, "kind": "controller_still_alive"}
        )
        finalized = self._finalize_dead_cleanup(identity_proof, proof, native_stop, error)
        if error is None:
            error = finalized["cleanup_error"]
        result = {
            "reason": reason,
            "controller_pid": getattr(self.process, "pid", None),
            "controller_returncode": getattr(self.process, "returncode", None),
            "controller_identity_owned": bool(identity_ok and identity_proof.get("ok")),
            "controller_identity_proof": identity_proof,
            "term_sent": term_sent,
            "kill_sent": kill_sent,
            "process_still_alive": not controller_dead,
            "worker_cleanup_proof": proof,
            "remote_stop": native_stop,
            "error": error,
        }
        result.update(finalized)
        self._record_cleanup_attempt(result)
        self._last_termination = result
        self._outstanding = None
        self._close_logs()
        if (
            not finalized["artifact_cleanup_complete"] and identity_proof.get("ok")
            and self.epoch_root and os.path.isdir(self.epoch_root)
        ):
            try:
                self._write_state("artifact_cleanup_failed" if finalized["termination_proven"]
                                  else "termination_failed", result=result)
            except Exception:
                pass
        return result

    def restart(self):
        if self.process is not None and self.process.poll() is None:
            raise PersistentControllerError("cannot restart a live controller")
        if self.epoch_root is not None and (
                os.path.lexists(self.epoch_root) or
                (self._last_termination is not None and
                 not self._last_termination.get("artifact_cleanup_complete"))):
            retirement = self.terminate_and_prove(
                "controller restart requested with an unreclaimed prior epoch"
            )
            if (not retirement.get("termination_proven")
                    or not (retirement.get("cleanup") or {}).get("ok")
                    or os.path.lexists(self.epoch_root)):
                raise PersistentControllerError(
                    "cannot restart because the prior controller epoch was not safely reclaimed"
                )
        return self.start()

    def close(self):
        with self._termination_lock:
            return self._close()

    def _close(self):
        if self.process is None:
            if self.epoch_root is not None and os.path.lexists(self.epoch_root):
                return self.terminate_and_prove("controller close requested before process start")
            return {"reason": "already_closed", "cleanup": {"ok": True, "removed": False}}
        if self.process.poll() is not None:
            return self.terminate_and_prove("controller already exited before close")
        if self._rpc_failed_reason is not None:
            return self.terminate_and_prove(
                "controller close requested after RPC failure: %s" % self._rpc_failed_reason
            )
        if self._outstanding is not None:
            return self.terminate_and_prove(
                "controller close requested with an outstanding %s request"
                % self._outstanding.get("operation")
            )
        timeout = max(1.0, float(self.config.get("runtime", {}).get(
            "controller_close_timeout_seconds", 30,
        )))
        identity_was_owned = self._controller_identity_is_owned()
        if not identity_was_owned:
            return self.terminate_and_prove(
                "controller close refused because the live process identity is ambiguous"
            )
        try:
            request = self._new_request("CLOSE", {})
            result = self._wait(request, timeout, "close")
            try:
                self.process.wait(timeout=max(1.0, timeout))
            except subprocess.TimeoutExpired:
                return self.terminate_and_prove("controller did not exit after close response")
            self._close_logs()
            identity_proof = self._dead_controller_identity_proof()
            try:
                worker_proof = self._wait_worker_cleanup_proof(
                    self.config.get("runtime", {}).get(
                        "controller_worker_cleanup_timeout_seconds", 15,
                    )
                )
            except Exception as proof_error:
                worker_proof = {
                    "ok": False, "kind": "worker_epoch_scan_failed",
                    "worker_epochs": [],
                    "error": "%s: %s" % (proof_error.__class__.__name__, proof_error),
                }
            result = dict(result) if isinstance(result, dict) else {
                "reason": "closed", "worker_close_result": result,
            }
            worker_response_process_ok = bool(
                result.get("process_still_alive") is not True
                and result.get("process_termination_proven", result.get("termination_proven")) is not False
            )
            native_stop = result.get("remote_stop") or {
                "required": False,
                "remote_stop_proven": result.get("remote_stop_proven") is not False,
            }
            finalized = self._finalize_dead_cleanup(
                identity_proof, worker_proof, native_stop,
                None if identity_was_owned and worker_response_process_ok else
                "controller or worker close process proof was incomplete",
            )
            result.update({
                "process_still_alive": False,
                "controller_identity_owned": bool(
                    identity_was_owned and identity_proof.get("ok")
                ),
                "controller_identity_proof": identity_proof,
                "worker_cleanup_proof": worker_proof,
                "remote_stop": native_stop,
            })
            result.update(finalized)
            if not finalized["artifact_cleanup_complete"]:
                result["error"] = finalized["cleanup_error"]
                if (
                    identity_proof.get("ok")
                    and self.epoch_root and os.path.isdir(self.epoch_root)
                ):
                    try:
                        self._write_state("artifact_cleanup_failed" if finalized["termination_proven"]
                                          else "termination_failed", result=result)
                    except Exception:
                        pass
            self._record_cleanup_attempt(result)
            self._last_termination = result
            return result
        except Exception as error:
            return self.terminate_and_prove("controller close failed: %s" % error)


def _controller_state(bootstrap, status, **updates):
    document = {
        "schema_version": SCHEMA_VERSION,
        "token": bootstrap["token"],
        "worker_index": bootstrap["worker_index"],
        "controller_epoch": bootstrap["controller_epoch"],
        "status": status,
        "pid": os.getpid(),
        "pgid": os.getpgid(os.getpid()),
        "pid_start_ticks": (_process_stat(os.getpid()) or {}).get("start_ticks"),
        "launch_boot_id": _linux_boot_id(),
        "parent_pid": bootstrap["parent_pid"],
        "parent_start_ticks": bootstrap.get("parent_start_ticks"),
        "run_root": bootstrap["run_root"],
        "liveness_lock_schema": bootstrap.get("liveness_lock_schema"),
        "liveness_lock_path": bootstrap.get("liveness_lock_path"),
        "updated_at": utc_timestamp(),
    }
    document.update(updates)
    atomic_write_json(bootstrap["state_path"], document)


def _request_identity(request):
    return dict((key, request[key]) for key in (
        "schema_version", "token", "controller_epoch", "sequence", "nonce", "operation",
    ))


def _publish_controller_startup_phase(bootstrap, identity, stage, epoch, context):
    """Publish fixed phase timestamps, fenced by the current authenticated RPC."""
    if stage not in ("workspace_prepare", "ocean_starting", "session_ready"):
        raise PersistentControllerError("unsupported controller startup phase")
    now = time.monotonic()
    if not context:
        context.update({
            "started_monotonic": now,
            "first_ocean_started_monotonic": None,
        })
    if context.get("stage") != stage or (
            context.get("epoch") is not None and epoch != context.get("epoch")):
        context["stage_started_monotonic"] = now
    context.update({"stage": stage, "epoch": epoch})
    if stage in ("ocean_starting", "session_ready"):
        if context.get("first_ocean_started_monotonic") is None:
            context["first_ocean_started_monotonic"] = now
    _controller_state(
        bootstrap, "processing", active_request=identity,
        startup_phase=dict(context),
    )


def _controller_worker_config(bootstrap):
    config = copy.deepcopy(bootstrap.get("config") or {})
    runtime = config.setdefault("runtime", {})
    runtime[GUARDIAN_SCOPE_CONFIG_KEY] = bootstrap["token"]
    return config


def run_controller(bootstrap):
    if bootstrap.get("schema_version") != SCHEMA_VERSION:
        raise PersistentControllerError("unsupported controller bootstrap schema")
    _set_parent_death_signal(
        bootstrap["parent_pid"], bootstrap.get("parent_start_ticks"),
    )
    from .runner import (
        ExternalEdaEnvironmentChanged, _WholeRunPersistentWorker,
        _evaluate_persistent_candidate, _evaluate_run_persistent_batch,
    )

    worker_config = _controller_worker_config(bootstrap)
    worker = _WholeRunPersistentWorker(
        bootstrap["run_root"], bootstrap["worker_index"], bootstrap["prepared"],
        bootstrap["selected"], worker_config,
        epoch_namespace=bootstrap["controller_epoch"],
    )
    active_native_request = {}

    def native_stop_handler(signum, frame):
        worker._native_stop_requested = True
        request_identity = dict(active_native_request)

        def stop_owned_native():
            controller = worker.controller
            try:
                native = (controller.request_batch_abort("controller stop requested")
                          if controller is not None else
                          {"remote_stop_proven": True, "no_remote_jobs_started": True})
            except Exception as error:
                native = {"remote_stop_proven": False, "error": str(error)}
            document = dict(request_identity, native=native, finished_at=utc_timestamp())
            atomic_write_json(os.path.join(os.path.dirname(bootstrap["state_path"]), _contract_text("native_stop.json")), document)

        thread = threading.Thread(target=stop_owned_native, name="native-maestro-stop")
        thread.daemon = True
        thread.start()

    signal.signal(signal.SIGUSR1, native_stop_handler)
    _controller_state(bootstrap, "ready")
    atomic_write_json(bootstrap["ready_path"], {
        "schema_version": SCHEMA_VERSION,
        "token": bootstrap["token"],
        "pid": os.getpid(),
        "ready_at": utc_timestamp(),
    })
    expected_sequence = 1
    while True:
        if not sys.platform.startswith("linux") and os.getppid() != int(bootstrap["parent_pid"]):
            os._exit(143)
        names = sorted(
            name for name in os.listdir(bootstrap["requests_root"])
            if name.endswith(".json") and name.startswith("%08d_" % expected_sequence)
        )
        if not names:
            time.sleep(0.05)
            continue
        request_path = os.path.join(bootstrap["requests_root"], names[0])
        if os.path.islink(request_path) or not os.path.isfile(request_path):
            raise PersistentControllerError("controller request is not a regular file")
        request = load_json(request_path)
        if (
            request.get("schema_version") != SCHEMA_VERSION
            or request.get("token") != bootstrap["token"]
            or int(request.get("controller_epoch", -1)) != int(bootstrap["controller_epoch"])
            or int(request.get("sequence", -1)) != expected_sequence
        ):
            raise PersistentControllerError("controller request identity mismatch")
        expected_name = "%08d_%s.json" % (expected_sequence, request.get("nonce"))
        expected_response = os.path.abspath(os.path.join(bootstrap["responses_root"], expected_name))
        if names[0] != expected_name or os.path.abspath(request.get("response_path") or "") != expected_response:
            raise PersistentControllerError("controller request path fence mismatch")
        identity = _request_identity(request)
        active_native_request.clear()
        active_native_request.update(identity)
        operation = request["operation"]
        payload = request.get("payload") or {}
        _controller_state(bootstrap, "processing", active_request=identity)
        response = dict(identity)
        close_after_response = False
        try:
            if operation in ("EVALUATE", "EVALUATE_BATCH"):
                startup_context = {}
                worker.startup_phase_callback = lambda stage, epoch: _publish_controller_startup_phase(
                    bootstrap, identity, stage, epoch, startup_context,
                )
                worker.startup_phase_callback("workspace_prepare", None)
            if operation == "EVALUATE":
                result = _evaluate_persistent_candidate(
                    worker,
                    bootstrap["run_root"],
                    payload["index"],
                    payload["generation"],
                    payload["values"],
                    payload["spec"],
                    bootstrap["prepared"],
                    bootstrap["selected"],
                    worker_config,
                )
            elif operation == "EVALUATE_BATCH":
                result = _evaluate_run_persistent_batch(
                    worker, bootstrap["run_root"], payload["planned"],
                    payload["generation"], payload["spec"], bootstrap["prepared"],
                    bootstrap["selected"], worker_config,
                )
            elif operation == "COMMIT":
                result = worker.commit_candidate(payload["candidate_id"])
            elif operation == "CLOSE":
                result = worker.close()
                close_after_response = True
            else:
                raise PersistentControllerError("unsupported controller operation: %s" % operation)
            response.update({"ok": True, "result": result, "finished_at": utc_timestamp()})
        except Exception as error:
            response.update({
                "ok": False,
                "error": "%s: %s" % (error.__class__.__name__, error),
                "error_type": error.__class__.__name__,
                "failure_class": (
                    "external_environment"
                    if isinstance(error, ExternalEdaEnvironmentChanged)
                    else "controller_request"
                ),
                "fatal_run": isinstance(error, ExternalEdaEnvironmentChanged),
                "traceback": traceback.format_exc(),
                "finished_at": utc_timestamp(),
            })
            if operation == "CLOSE":
                close_after_response = True
        atomic_write_json(request["response_path"], response)
        expected_sequence += 1
        _controller_state(bootstrap, "closed" if close_after_response else "ready")
        if close_after_response:
            return 0 if response.get("ok") else 2


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Own one run-lifetime Maestro worker behind atomic file RPC.")
    parser.add_argument("--bootstrap", required=True)
    return parser.parse_args(argv)


def main(argv=None):
    liveness_descriptor = None
    try:
        liveness_descriptor, inherited_path = _adopt_controller_liveness_lock_from_environment()
        args = parse_args(argv)
        bootstrap = load_json(args.bootstrap)
        _validate_controller_liveness_bootstrap(
            liveness_descriptor, inherited_path, args.bootstrap, bootstrap,
        )
        return run_controller(bootstrap)
    except Exception as error:
        sys.stderr.write("persistent controller failed: %s: %s\n" % (error.__class__.__name__, error))
        return 2
    finally:
        if liveness_descriptor is not None:
            try:
                os.close(liveness_descriptor)
            except OSError:
                pass


if __name__ == "__main__":
    sys.exit(main())
