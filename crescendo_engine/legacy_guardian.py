from __future__ import print_function

import errno
import fcntl
import hashlib
import os
import re
import signal
import stat
import time

from .resources import guardian_script_path
from .utils import atomic_write_json, load_json


MANIFEST_SCHEMA_VERSION = "crescendo.legacy_maestro_guardian.v1"
GUARDIAN_SCHEMA_VERSION = "crescendo.ocean_worker_guardian.v1"
ORPHAN_RECOVERY_LEGACY_SCHEMA_VERSION = "crescendo.guardian_orphan_recovery.v1"
ORPHAN_RECOVERY_SCHEMA_VERSION = "crescendo.guardian_orphan_recovery.v2"
MANIFEST_NAME = "legacy_guardian_launch.json"
STATE_NAME = "guardian_state.json"
LAUNCH_LOCK_NAME = MANIFEST_NAME + ".launch_lock.tmp"
LAUNCH_TOKEN_ENVIRONMENT = "CRESCENDO_LEGACY_GUARDIAN_TOKEN"
GUARDIAN_TOKEN_IDENTITY_SCHEMA = "crescendo.guardian_token_identity.v2"
GUARDIAN_SCOPE_CONFIG_KEY = "_persistent_guardian_scope_token"
TERMINAL_GUARDIAN_STATES = frozenset(("exited", "cleanup_failed", "launch_failed"))
_STABLE_SCAN_INTERVAL_SECONDS = 0.05
_GUARDIAN_SCOPE_TOKEN = re.compile(r"^[0-9a-f]{32}$")
_SCOPED_GUARDIAN_TOKEN = re.compile(r"^crescendo2-[0-9a-f]{64}$")


def legacy_worker_guardian_token(epoch, worker_index):
    return "%s-worker-%d" % (str(epoch), int(worker_index))


def build_guardian_token_identity(scope_token, epoch, worker_index):
    scope_token = str(scope_token or "")
    epoch = str(epoch or "")
    try:
        worker_index = int(worker_index)
    except (TypeError, ValueError):
        raise ValueError("guardian token worker index is malformed")
    if not _GUARDIAN_SCOPE_TOKEN.match(scope_token):
        raise ValueError("guardian token scope is malformed")
    if not epoch or worker_index < 0:
        raise ValueError("guardian token worker identity is malformed")
    payload = "%s\0%s\0%d" % (scope_token, epoch, worker_index)
    token = "crescendo2-" + hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return {
        "schema_version": GUARDIAN_TOKEN_IDENTITY_SCHEMA,
        "scope_token": scope_token,
        "epoch": epoch,
        "worker_index": worker_index,
        "token": token,
    }


def validate_guardian_token_identity(identity):
    required = frozenset((
        "schema_version", "scope_token", "epoch", "worker_index", "token",
    ))
    if not isinstance(identity, dict) or set(identity) != required:
        raise ValueError("guardian token identity is incomplete or contains unknown fields")
    if identity.get("schema_version") != GUARDIAN_TOKEN_IDENTITY_SCHEMA:
        raise ValueError("guardian token identity schema is not recognized")
    expected = build_guardian_token_identity(
        identity.get("scope_token"), identity.get("epoch"), identity.get("worker_index"),
    )
    if not isinstance(identity.get("token"), str) or not _SCOPED_GUARDIAN_TOKEN.match(
            identity["token"]):
        raise ValueError("guardian token identity token is malformed")
    if identity != expected:
        raise ValueError("guardian token identity digest does not match its scope")
    return expected


def resolve_worker_guardian_token_identity(worker_state, epoch, worker_index):
    state = worker_state or {}
    recorded = state.get("guardian_token_identity")
    if recorded is None:
        token = legacy_worker_guardian_token(epoch, worker_index)
        for key in ("guardian_token", "guardian_state_token"):
            value = state.get(key)
            if value is not None and value != token:
                raise ValueError("legacy %s does not match its deterministic worker token" % key)
        return {
            "legacy": True,
            "epoch": str(epoch),
            "worker_index": int(worker_index),
            "token": token,
        }
    identity = validate_guardian_token_identity(recorded)
    if identity["epoch"] != str(epoch) or identity["worker_index"] != int(worker_index):
        raise ValueError("guardian token identity does not match its worker epoch")
    if state.get("guardian_token") != identity["token"]:
        raise ValueError("worker guardian token does not match its scoped identity")
    state_token = state.get("guardian_state_token")
    if state_token is not None and state_token != identity["token"]:
        raise ValueError("worker guardian state token does not match its scoped identity")
    return identity


def _current_boot_id():
    try:
        with open("/proc/sys/kernel/random/boot_id", "r") as stream:
            value = stream.read().strip()
    except (IOError, OSError):
        return None
    return value or None


def _process_stat(pid):
    try:
        with open("/proc/%d/stat" % int(pid)) as stream:
            text = stream.read()
    except (IOError, OSError, TypeError, ValueError):
        return None
    closing = text.rfind(")")
    if closing < 0:
        return None
    fields = text[closing + 2:].split()
    if len(fields) <= 19:
        return None
    try:
        return {
            "state": fields[0], "ppid": int(fields[1]), "pgrp": int(fields[2]),
            "session": int(fields[3]), "start_ticks": int(fields[19]),
        }
    except (TypeError, ValueError):
        return None


def _process_environment_value(pid, name):
    try:
        with open("/proc/%d/environ" % int(pid), "rb") as stream:
            rows = stream.read().split(b"\0")
    except (IOError, OSError, TypeError, ValueError):
        return None
    prefix = (str(name) + "=").encode("utf-8")
    for row in rows:
        if row.startswith(prefix):
            return row[len(prefix):].decode("utf-8", "replace")
    return None


def _process_snapshot(pid, include_launch_token=False):
    try:
        pid = int(pid)
    except (TypeError, ValueError):
        return {"alive": False, "pid": None}
    process_stat = _process_stat(pid)
    if not process_stat or process_stat.get("state") == "Z":
        return {"alive": False, "pid": pid}
    try:
        process_root = "/proc/%d" % pid
        with open(os.path.join(process_root, "cmdline"), "rb") as stream:
            command = [item.decode("utf-8", "replace") for item in stream.read().split(b"\0") if item]
        cwd = os.path.realpath(os.readlink(os.path.join(process_root, "cwd")))
        uid = os.stat(process_root).st_uid
    except (IOError, OSError):
        return {"alive": False, "pid": pid}
    result = dict(process_stat)
    result.update({"alive": True, "pid": pid, "argv": command, "cwd": cwd, "uid": uid})
    if include_launch_token:
        result["launch_token"] = _process_environment_value(pid, LAUNCH_TOKEN_ENVIRONMENT)
    return result


def _open_launch_lock(path, create=False):
    flags = os.O_RDWR | getattr(os, "O_CLOEXEC", 0)
    if create:
        flags |= os.O_CREAT
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    try:
        value = os.fstat(descriptor)
        if not stat.S_ISREG(value.st_mode) or value.st_nlink != 1:
            raise ValueError("legacy guardian launch lock is not a private regular file")
        current = os.stat(path)
        if current.st_dev != value.st_dev or current.st_ino != value.st_ino:
            raise ValueError("legacy guardian launch lock changed while opening")
        return descriptor
    except Exception:
        os.close(descriptor)
        raise


def acquire_legacy_guardian_launch_lock(attempt_root):
    attempt_root = os.path.realpath(os.path.abspath(attempt_root))
    if os.path.islink(attempt_root) or not os.path.isdir(attempt_root):
        raise ValueError("legacy guardian attempt is not a real directory")
    lock_path = os.path.join(attempt_root, LAUNCH_LOCK_NAME)
    descriptor = _open_launch_lock(lock_path, create=True)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        os.fsync(descriptor)
        try:
            directory_descriptor = os.open(
                attempt_root, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
            )
            try:
                os.fsync(directory_descriptor)
            finally:
                os.close(directory_descriptor)
        except OSError:
            pass
    except Exception:
        os.close(descriptor)
        raise
    return descriptor, lock_path


def close_legacy_guardian_launch_lock(descriptor):
    if descriptor is None:
        return
    try:
        os.close(int(descriptor))
    except OSError as error:
        if getattr(error, "errno", None) != errno.EBADF:
            raise


def legacy_guardian_parent_identity(pid=None):
    pid = os.getpid() if pid is None else int(pid)
    process_stat = _process_stat(pid)
    boot_id = _current_boot_id()
    if not process_stat or not boot_id:
        raise RuntimeError("cannot establish durable legacy guardian parent identity")
    return {
        "pid": pid,
        "boot_id": boot_id,
        "start_ticks": int(process_stat["start_ticks"]),
        "uid": os.getuid(),
    }


def _regular_file(path):
    try:
        value = os.lstat(path)
    except OSError:
        return False
    return stat.S_ISREG(value.st_mode) and not stat.S_ISLNK(value.st_mode)


def _same_path(left, right):
    return os.path.realpath(os.path.abspath(str(left or ""))) == os.path.realpath(
        os.path.abspath(str(right or ""))
    )


def _path_within(path, root):
    candidate = os.path.realpath(os.path.abspath(str(path or "")))
    boundary = os.path.realpath(os.path.abspath(str(root or "")))
    prefix = boundary if boundary.endswith(os.sep) else boundary + os.sep
    return candidate == boundary or candidate.startswith(prefix)


def _option_value(argv, name):
    try:
        index = argv.index(name)
    except ValueError:
        return None
    return argv[index + 1] if index + 1 < len(argv) else None


def _command_matches(observed, expected):
    if not observed or not expected or len(observed) != len(expected):
        return False
    if _same_path(observed[0], expected[0]):
        return observed[1:] == expected[1:]
    return observed == expected


def _guardian_identity_matches(snapshot, manifest, guardian):
    if not snapshot.get("alive"):
        return False
    expected_pid = int(guardian.get("guardian_pid", -1))
    if not (
            snapshot.get("pid") == expected_pid and
            snapshot.get("pgrp") == expected_pid and
            snapshot.get("session") == expected_pid):
        return False
    if int(snapshot.get("start_ticks", -1)) != int(guardian.get("guardian_start_ticks", -2)):
        return False
    if _has_durable_launch_protocol(manifest):
        return bool(
            snapshot.get("uid") == manifest.get("expected_uid") and
            _same_path(snapshot.get("cwd"), manifest["workspace_root"]) and
            _command_matches(snapshot.get("argv") or [], manifest.get("guardian_argv") or [])
        )
    argv = snapshot.get("argv") or []
    script = manifest["guardian_script"]
    if len(argv) < 2 or not _same_path(argv[1], script):
        return False
    expected = {
        "--state-path": manifest["state_path"],
        "--token": manifest["token"],
        "--cwd": manifest["workspace_root"],
        "--expected-parent-pid": str(manifest["expected_parent_pid"]),
    }
    for name, value in expected.items():
        observed = _option_value(argv, name)
        if name in ("--state-path", "--cwd"):
            if not observed or not _same_path(observed, value):
                return False
        elif str(observed) != str(value):
            return False
    return _same_path(snapshot.get("cwd"), manifest["workspace_root"])


def _ocean_identity_matches(snapshot, manifest, guardian):
    if not snapshot.get("alive"):
        return False
    expected_pid = int(guardian.get("pid", -1))
    expected_pgid = int(guardian.get("pgid", -2))
    expected_sid = int(guardian.get("sid", -3))
    matches = bool(
        snapshot.get("pid") == expected_pid and
        int(snapshot.get("start_ticks", -4)) == int(guardian.get("pid_start_ticks", -5)) and
        snapshot.get("pgrp") == expected_pgid == expected_pid and
        snapshot.get("session") == expected_sid == expected_pid and
        _same_path(snapshot.get("cwd"), manifest["workspace_root"])
    )
    if not matches:
        return False
    try:
        durable = _has_durable_launch_protocol(manifest)
        scoped_identity = manifest.get("guardian_token_identity")
        if durable and scoped_identity is not None:
            scoped_identity = validate_guardian_token_identity(scoped_identity)
            if scoped_identity.get("token") != manifest.get("token"):
                return False
    except Exception:
        return False
    if durable and scoped_identity is not None:
        return bool(
            snapshot.get("uid") == manifest.get("expected_uid") and
            snapshot.get("launch_token") == manifest.get("token")
        )
    if not _command_matches(snapshot.get("argv") or [], manifest["argv"]):
        return False
    if durable:
        return bool(
            snapshot.get("uid") == manifest.get("expected_uid") and
            snapshot.get("launch_token") == manifest.get("token")
        )
    return True


def write_legacy_guardian_manifest(attempt_root, workspace_root, state_path, stdout_path,
                                   stderr_path, token, argv, expected_parent_pid,
                                   guardian_script, launch_lock_path=None,
                                   expected_parent_boot_id=None,
                                   expected_parent_start_ticks=None, expected_uid=None,
                                   guardian_argv=None, guardian_token_identity=None):
    attempt_root = os.path.realpath(os.path.abspath(attempt_root))
    workspace_root = os.path.realpath(os.path.abspath(workspace_root))
    durable_launch = any(value is not None for value in (
        launch_lock_path, expected_parent_boot_id, expected_parent_start_ticks,
        expected_uid, guardian_argv,
    ))
    if durable_launch and any(value is None for value in (
            launch_lock_path, expected_parent_boot_id, expected_parent_start_ticks,
            expected_uid, guardian_argv)):
        raise ValueError("durable legacy guardian launch identity is incomplete")
    document = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "created_epoch": time.time(),
        "attempt_root": attempt_root,
        "workspace_root": workspace_root,
        "state_path": os.path.abspath(state_path),
        "stdout_path": os.path.abspath(stdout_path),
        "stderr_path": os.path.abspath(stderr_path),
        "guardian_script": os.path.realpath(os.path.abspath(guardian_script)),
        "token": str(token),
        "argv": [str(value) for value in argv],
        "expected_parent_pid": int(expected_parent_pid),
        "protocol": {
            "manifest_before_guardian_popen": True,
            "guardian_state_before_ocean_popen": True,
            "expected_parent_death_guard": True,
            "stdin_eof_cleanup": True,
        },
    }
    if guardian_token_identity is not None:
        token_identity = validate_guardian_token_identity(guardian_token_identity)
        if token_identity["token"] != document["token"]:
            raise ValueError("guardian manifest token does not match its scoped identity")
        document["guardian_token_identity"] = token_identity
    if durable_launch:
        document.update({
            "launch_lock_path": os.path.abspath(launch_lock_path),
            "expected_parent_boot_id": str(expected_parent_boot_id),
            "expected_parent_start_ticks": int(expected_parent_start_ticks),
            "expected_uid": int(expected_uid),
            "guardian_argv": [str(value) for value in guardian_argv],
        })
        document["protocol"].update({
            "durable_launch_liveness_lock": True,
            "guardian_inherits_launch_lock": True,
            "guardian_state_before_launch_lock_release": True,
            "stable_proc_double_scan": True,
        })
    atomic_write_json(os.path.join(attempt_root, MANIFEST_NAME), document)
    return document


def _has_durable_launch_protocol(manifest):
    protocol = manifest.get("protocol") or {}
    names = (
        "durable_launch_liveness_lock", "guardian_inherits_launch_lock",
        "guardian_state_before_launch_lock_release", "stable_proc_double_scan",
    )
    enabled = [protocol.get(name) is True for name in names]
    if any(enabled) and not all(enabled):
        raise ValueError("legacy guardian durable launch protocol is incomplete")
    fields = (
        "launch_lock_path", "expected_parent_boot_id", "expected_parent_start_ticks",
        "expected_uid", "guardian_argv",
    )
    present = [manifest.get(name) is not None for name in fields]
    if any(present) and not all(present):
        raise ValueError("legacy guardian durable launch identity is incomplete")
    if any(enabled) != any(present):
        raise ValueError("legacy guardian durable launch protocol does not match its identity")
    return all(enabled)


def _guardian_file_digest(path):
    """Hash a bounded, singly linked regular helper without following its leaf."""
    before = os.lstat(path)
    if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_size > 4 * 1024 * 1024:
        raise ValueError("legacy guardian helper is not a bounded regular file")
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    with os.fdopen(descriptor, "rb") as stream:
        info = os.fstat(stream.fileno())
        if not os.path.samestat(before, info):
            raise ValueError("legacy guardian helper changed while opening")
        content = stream.read(4 * 1024 * 1024 + 1)
        if len(content) > 4 * 1024 * 1024:
            raise ValueError("legacy guardian helper is too large")
    after = os.lstat(path)
    if not os.path.samestat(before, after) or before.st_size != after.st_size or before.st_mtime_ns != after.st_mtime_ns:
        raise ValueError("legacy guardian helper changed during inspection")
    return hashlib.sha256(content).hexdigest()


def _validated_manifest(attempt_root, expected_workspace_root=None,
                        expected_token_identity=None):
    attempt_root = os.path.abspath(attempt_root)
    if os.path.islink(attempt_root) or not os.path.isdir(attempt_root):
        raise ValueError("legacy guardian attempt is not a real directory")
    manifest_path = os.path.join(attempt_root, MANIFEST_NAME)
    if not _regular_file(manifest_path):
        raise ValueError("legacy guardian launch manifest is missing or unsafe")
    manifest = load_json(manifest_path)
    if not isinstance(manifest, dict) or manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION:
        raise ValueError("legacy guardian launch manifest schema is not recognized")
    if not _same_path(manifest.get("attempt_root"), attempt_root):
        raise ValueError("legacy guardian attempt root does not match its manifest")
    workspace_root = manifest.get("workspace_root")
    if not isinstance(workspace_root, str) or not os.path.isabs(workspace_root):
        raise ValueError("legacy guardian workspace root is malformed")
    if expected_workspace_root and not _same_path(workspace_root, expected_workspace_root):
        raise ValueError("legacy guardian workspace does not match its candidate")
    fixed_paths = {
        "state_path": os.path.join(attempt_root, STATE_NAME),
        "stdout_path": os.path.join(attempt_root, "ocean.stdout"),
        "stderr_path": os.path.join(attempt_root, "ocean.stderr"),
    }
    for name, expected in fixed_paths.items():
        value = manifest.get(name)
        if not isinstance(value, str) or not os.path.isabs(value) or not _same_path(value, expected):
            raise ValueError("legacy guardian %s is not the fixed attempt path" % name)
    guardian_script = manifest.get("guardian_script")
    if not isinstance(guardian_script, str) or not os.path.isabs(guardian_script):
        raise ValueError("legacy guardian script path is malformed")
    moved_helper = not _same_path(guardian_script, guardian_script_path())
    if moved_helper:
        try:
            if _guardian_file_digest(guardian_script) != _guardian_file_digest(guardian_script_path()):
                raise ValueError("legacy guardian script differs from the current trusted helper")
        except OSError as error:
            raise ValueError("legacy guardian script cannot be verified: %s" % error)
    if not isinstance(manifest.get("token"), str) or not manifest["token"].strip():
        raise ValueError("legacy guardian token is malformed")
    manifest_token_identity = manifest.get("guardian_token_identity")
    if manifest_token_identity is not None:
        manifest_token_identity = validate_guardian_token_identity(manifest_token_identity)
        if manifest_token_identity["token"] != manifest["token"]:
            raise ValueError("guardian manifest token does not match its scoped identity")
    if expected_token_identity is not None:
        if expected_token_identity.get("legacy") is True:
            if manifest_token_identity is not None:
                raise ValueError("legacy worker artifact unexpectedly uses a scoped guardian token")
            expected_token = expected_token_identity.get("token")
        else:
            expected_token_identity = validate_guardian_token_identity(expected_token_identity)
            if manifest_token_identity is None:
                raise ValueError("scoped worker artifact is missing guardian token identity")
            if manifest_token_identity != expected_token_identity:
                raise ValueError("guardian manifest scoped identity does not match its worker state")
            expected_token = expected_token_identity["token"]
        if manifest["token"] != expected_token:
            raise ValueError("guardian manifest token does not match its worker state")
    argv = manifest.get("argv")
    if not isinstance(argv, list) or not argv or any(not isinstance(value, str) for value in argv):
        raise ValueError("legacy guardian command is malformed")
    try:
        expected_parent_pid = int(manifest.get("expected_parent_pid"))
    except (TypeError, ValueError):
        raise ValueError("legacy guardian expected parent pid is malformed")
    if expected_parent_pid <= 1:
        raise ValueError("legacy guardian expected parent pid is unsafe")
    protocol = manifest.get("protocol") or {}
    required = (
        "manifest_before_guardian_popen", "guardian_state_before_ocean_popen",
        "expected_parent_death_guard", "stdin_eof_cleanup",
    )
    if any(protocol.get(name) is not True for name in required):
        raise ValueError("legacy guardian process protocol is incomplete")
    if _has_durable_launch_protocol(manifest):
        launch_lock_path = manifest.get("launch_lock_path")
        expected_lock_path = os.path.join(attempt_root, LAUNCH_LOCK_NAME)
        if (
                not isinstance(launch_lock_path, str) or not os.path.isabs(launch_lock_path)
                or not _same_path(launch_lock_path, expected_lock_path)):
            raise ValueError("legacy guardian launch lock is not the fixed attempt path")
        if not _regular_file(expected_lock_path):
            raise ValueError("legacy guardian launch lock is missing or unsafe")
        boot_id = manifest.get("expected_parent_boot_id")
        if not isinstance(boot_id, str) or not boot_id.strip():
            raise ValueError("legacy guardian parent boot identity is malformed")
        for name in ("expected_parent_start_ticks", "expected_uid"):
            value = manifest.get(name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError("legacy guardian %s is malformed" % name)
        if manifest["expected_parent_start_ticks"] <= 0:
            raise ValueError("legacy guardian parent start identity is malformed")
        guardian_argv = manifest.get("guardian_argv")
        if (
                not isinstance(guardian_argv, list) or not guardian_argv
                or any(not isinstance(value, str) or not value for value in guardian_argv)):
            raise ValueError("legacy guardian wrapper command is malformed")
        if len(guardian_argv) < 3 or not _same_path(guardian_argv[1], guardian_script):
            raise ValueError("legacy guardian wrapper command does not use the project guardian")
        expected_options = {
            "--state-path": manifest["state_path"],
            "--stdout-path": manifest["stdout_path"],
            "--stderr-path": manifest["stderr_path"],
            "--token": manifest["token"],
            "--cwd": manifest["workspace_root"],
            "--expected-parent-pid": str(manifest["expected_parent_pid"]),
        }
        for name, expected in expected_options.items():
            observed = _option_value(guardian_argv, name)
            if name in ("--state-path", "--stdout-path", "--stderr-path", "--cwd"):
                matches = bool(observed and _same_path(observed, expected))
            else:
                matches = str(observed) == str(expected)
            if not matches:
                raise ValueError("legacy guardian wrapper %s does not match its manifest" % name)
        try:
            launch_lock_fd = int(_option_value(guardian_argv, "--launch-lock-fd"))
        except (TypeError, ValueError):
            raise ValueError("legacy guardian wrapper launch lock descriptor is malformed")
        if launch_lock_fd < 3:
            raise ValueError("legacy guardian wrapper launch lock descriptor is unsafe")
        try:
            separator = guardian_argv.index("--")
        except ValueError:
            raise ValueError("legacy guardian wrapper command has no Ocean separator")
        if guardian_argv[separator + 1:] != manifest["argv"]:
            raise ValueError("legacy guardian wrapper Ocean command does not match its manifest")
    manifest['_cr_moved_helper'] = moved_helper
    return manifest_path, manifest


def _terminal_cleanup_proof(guardian, manifest=None):
    if guardian.get("status") == "launch_failed":
        proven = guardian.get("pid") is None and guardian.get("pgid") is None
        return {
            "ok": proven, "kind": "ocean_not_spawned" if proven else "launch_failure_unproven",
            "status": guardian.get("status"),
        }
    recovery = guardian.get("orphan_recovery")
    if recovery is not None:
        action = recovery.get("action") if isinstance(recovery, dict) else None
        members = recovery.get("owned_members") if isinstance(recovery, dict) else None
        detached_helpers = (
            recovery.get("detached_helpers") if isinstance(recovery, dict) else None
        )
        helper_signals = (
            recovery.get("detached_helper_signals")
            if isinstance(recovery, dict) else None
        )
        recovery_schema = recovery.get("schema_version") if isinstance(recovery, dict) else None
        legacy_recovery = recovery_schema == ORPHAN_RECOVERY_LEGACY_SCHEMA_VERSION
        current_recovery = recovery_schema == ORPHAN_RECOVERY_SCHEMA_VERSION
        launch_argv = (
            recovery.get("argv") if legacy_recovery
            else recovery.get("launch_argv")
        ) if isinstance(recovery, dict) else None
        scoped_manifest = False
        if current_recovery and isinstance(manifest, dict):
            try:
                token_identity = validate_guardian_token_identity(
                    manifest.get("guardian_token_identity")
                )
                scoped_manifest = bool(
                    _has_durable_launch_protocol(manifest)
                    and token_identity.get("token") == manifest.get("token")
                )
            except Exception:
                scoped_manifest = False
        proven = bool(
            isinstance(recovery, dict) and
            isinstance(manifest, dict) and
            (legacy_recovery or current_recovery) and
            (legacy_recovery or scoped_manifest) and
            action in (
                "killed_exact_ocean_group", "killed_exact_scoped_processes",
                "stable_owned_process_absence",
            ) and
            recovery.get("boot_id") == (guardian.get("launch_boot_id") or guardian.get("boot_id")) and
            recovery.get("token") == guardian.get("token") == manifest.get("token") and
            recovery.get("guardian_pid") == guardian.get("guardian_pid") and
            recovery.get("guardian_start_ticks") == guardian.get("guardian_start_ticks") and
            recovery.get("ocean_pid") == guardian.get("pid") and
            recovery.get("ocean_start_ticks") == guardian.get("pid_start_ticks") and
            recovery.get("pgid") == guardian.get("pgid") and
            recovery.get("sid") == guardian.get("sid") and
            isinstance(recovery.get("cwd"), str) and
            isinstance(guardian.get("cwd"), str) and
            _same_path(recovery.get("cwd"), guardian.get("cwd")) and
            _same_path(recovery.get("cwd"), manifest.get("workspace_root")) and
            isinstance(launch_argv, list) and launch_argv and
            list(launch_argv) == list(guardian.get("argv") or []) and
            list(launch_argv) == list(manifest.get("argv") or []) and
            (legacy_recovery or list(recovery.get("argv") or []) == list(launch_argv)) and
            isinstance(members, list) and
            recovery.get("stable_scans_before") == 2 and
            recovery.get("stable_scans_after") == 2 and
            recovery.get("remaining_owned_processes") == [] and
            guardian.get("status") == "exited" and
            guardian.get("termination_reason") == "guardian_loss_recovery" and
            guardian.get("group_cleanup_complete") is True and
            guardian.get("orphan_scope_cleanup_complete") is True and
            not list(guardian.get("remaining_group_pids") or []) and
            not list(guardian.get("remaining_adopted_pids") or [])
        )
        if proven and action == "killed_exact_ocean_group":
            if legacy_recovery:
                proven = bool(
                    recovery.get("signal") == signal.SIGKILL and
                    members and any(
                        row.get("pid") == guardian.get("pid") and
                        row.get("start_ticks") == guardian.get("pid_start_ticks") and
                        row.get("pgrp") == guardian.get("pgid") and
                        row.get("session") == guardian.get("sid") and
                        row.get("uid") == manifest.get("expected_uid") and
                        row.get("launch_token") == guardian.get("token") and
                        isinstance(row.get("cwd"), str) and
                        _same_path(row.get("cwd"), guardian.get("cwd")) and
                        list(row.get("argv") or []) == list(guardian.get("argv") or [])
                        for row in members if isinstance(row, dict)
                    )
                )
            else:
                runtime_identity = recovery.get("runtime_identity")
                leaders = [
                    row for row in members
                    if isinstance(row, dict) and row.get("kind") == "ocean_leader"
                ]
                owned_scope = all(
                    isinstance(row, dict) and
                    row.get("kind") in ("ocean_leader", "ocean_group_member") and
                    isinstance(row.get("pid"), int) and row.get("pid") > 1 and
                    isinstance(row.get("start_ticks"), int) and row.get("start_ticks") > 0 and
                    row.get("pgrp") == guardian.get("pgid") and
                    row.get("session") == guardian.get("sid") and
                    row.get("uid") == manifest.get("expected_uid") and
                    row.get("launch_token") == guardian.get("token") and
                    isinstance(row.get("cwd"), str) and
                    _path_within(row.get("cwd"), guardian.get("cwd")) and
                    isinstance(row.get("argv"), list) and bool(row.get("argv")) and
                    all(isinstance(value, str) for value in row.get("argv"))
                    for row in members
                )
                proven = bool(
                    recovery.get("signal") == signal.SIGKILL and
                    not list(detached_helpers or []) and
                    not list(helper_signals or []) and
                    isinstance(runtime_identity, dict) and
                    len(leaders) == 1 and runtime_identity == leaders[0] and
                    runtime_identity.get("pid") == guardian.get("pid") and
                    runtime_identity.get("start_ticks") == guardian.get("pid_start_ticks") and
                    runtime_identity.get("pgrp") == guardian.get("pgid") == guardian.get("pid") and
                    runtime_identity.get("session") == guardian.get("sid") == guardian.get("pid") and
                    owned_scope
                )
        if proven and action == "killed_exact_scoped_processes":
            runtime_identity = recovery.get("runtime_identity")
            leaders = [
                row for row in members
                if isinstance(row, dict) and row.get("kind") == "ocean_leader"
            ]
            owned_scope = all(
                isinstance(row, dict) and
                row.get("kind") in ("ocean_leader", "ocean_group_member") and
                isinstance(row.get("pid"), int) and row.get("pid") > 1 and
                isinstance(row.get("start_ticks"), int) and row.get("start_ticks") > 0 and
                isinstance(row.get("ppid"), int) and row.get("ppid") >= 0 and
                row.get("pgrp") == guardian.get("pgid") and
                row.get("session") == guardian.get("sid") and
                row.get("uid") == manifest.get("expected_uid") and
                row.get("launch_token") == guardian.get("token") and
                isinstance(row.get("cwd"), str) and
                _path_within(row.get("cwd"), guardian.get("cwd")) and
                isinstance(row.get("argv"), list) and bool(row.get("argv")) and
                all(isinstance(value, str) for value in row.get("argv"))
                for row in members
            )
            helper_scope = bool(
                isinstance(detached_helpers, list) and detached_helpers and
                all(
                    isinstance(row, dict) and
                    row.get("kind") == "scoped_token_helper" and
                    isinstance(row.get("pid"), int) and row.get("pid") > 1 and
                    row.get("pid") != guardian.get("pid") and
                    isinstance(row.get("start_ticks"), int) and
                    row.get("start_ticks") > 0 and
                    isinstance(row.get("ppid"), int) and row.get("ppid") >= 0 and
                    isinstance(row.get("pgrp"), int) and row.get("pgrp") > 0 and
                    isinstance(row.get("session"), int) and row.get("session") > 0 and
                    (
                        row.get("pgrp") != guardian.get("pgid") or
                        row.get("session") != guardian.get("sid")
                    ) and
                    row.get("uid") == manifest.get("expected_uid") and
                    row.get("launch_token") == guardian.get("token") and
                    isinstance(row.get("cwd"), str) and
                    _path_within(row.get("cwd"), guardian.get("cwd")) and
                    isinstance(row.get("argv"), list) and bool(row.get("argv")) and
                    all(isinstance(value, str) for value in row.get("argv"))
                    for row in detached_helpers
                )
            )
            helper_keys = [
                (row.get("pid"), row.get("start_ticks"))
                for row in detached_helpers or [] if isinstance(row, dict)
            ]
            signal_keys = []
            helper_signal_scope = isinstance(helper_signals, list)
            if helper_signal_scope:
                for row in helper_signals:
                    if not isinstance(row, dict):
                        helper_signal_scope = False
                        break
                    outcome = row.get("result")
                    signal_identity = row.get("identity")
                    valid_signal = bool(
                        (outcome == "sent" and row.get("signal") == signal.SIGKILL) or
                        (outcome == "already_absent" and row.get("signal") is None)
                    )
                    if not (
                            isinstance(row.get("pid"), int) and row.get("pid") > 1 and
                            isinstance(row.get("start_ticks"), int) and
                            row.get("start_ticks") > 0 and valid_signal and
                            isinstance(signal_identity, dict) and
                            signal_identity in (detached_helpers or []) and
                            signal_identity.get("pid") == row.get("pid") and
                            signal_identity.get("start_ticks") == row.get("start_ticks")):
                        helper_signal_scope = False
                        break
                    signal_keys.append((row.get("pid"), row.get("start_ticks")))
            helper_signal_scope = bool(
                helper_signal_scope and
                len(helper_keys) == len(set(helper_keys)) and
                len(signal_keys) == len(set(signal_keys)) and
                set(signal_keys) == set(helper_keys)
            )
            member_pids = {
                row.get("pid") for row in members if isinstance(row, dict)
            }
            helper_pids = {
                row.get("pid") for row in detached_helpers or []
                if isinstance(row, dict)
            }
            if members:
                group_scope = bool(
                    recovery.get("signal") == signal.SIGKILL and
                    isinstance(runtime_identity, dict) and
                    len(leaders) == 1 and runtime_identity == leaders[0] and
                    runtime_identity.get("pid") == guardian.get("pid") and
                    runtime_identity.get("start_ticks") == guardian.get("pid_start_ticks") and
                    runtime_identity.get("pgrp") == guardian.get("pgid") == guardian.get("pid") and
                    runtime_identity.get("session") == guardian.get("sid") == guardian.get("pid")
                )
            else:
                group_scope = bool(
                    recovery.get("signal") is None and runtime_identity is None
                )
            proven = bool(
                current_recovery and owned_scope and helper_scope and
                helper_signal_scope and not member_pids.intersection(helper_pids) and
                group_scope
            )
        if proven and action == "stable_owned_process_absence":
            proven = bool(
                not members and recovery.get("signal") is None and
                not list(detached_helpers or []) and not list(helper_signals or []) and
                (legacy_recovery or recovery.get("runtime_identity") is None)
            )
        return {
            "ok": proven,
            "kind": (
                "orphaned_ocean_group_recovered" if proven
                else "orphaned_ocean_recovery_unproven"
            ),
            "status": guardian.get("status"),
            "action": action,
            "remaining_group_pids": list(guardian.get("remaining_group_pids") or []),
            "remaining_adopted_pids": list(guardian.get("remaining_adopted_pids") or []),
        }
    remaining_group = list(guardian.get("remaining_group_pids") or [])
    remaining_adopted = list(guardian.get("remaining_adopted_pids") or [])
    proven = bool(
        guardian.get("status") == "exited" and
        guardian.get("group_cleanup_complete") is True and
        guardian.get("descendant_cleanup_complete") is True and
        guardian.get("descendant_supervision_complete") is True and
        not remaining_group and not remaining_adopted
    )
    return {
        "ok": proven,
        "kind": "guardian_cleanup_proven" if proven else "guardian_cleanup_unproven",
        "status": guardian.get("status"),
        "remaining_group_pids": remaining_group,
        "remaining_adopted_pids": remaining_adopted,
    }


def _prestate_guardian_identity_matches(snapshot, manifest):
    return bool(
        snapshot.get("alive") and
        snapshot.get("uid") == manifest.get("expected_uid") and
        snapshot.get("pid") == snapshot.get("pgrp") == snapshot.get("session") and
        _same_path(snapshot.get("cwd"), manifest.get("workspace_root")) and
        _command_matches(snapshot.get("argv") or [], manifest.get("guardian_argv") or [])
    )


def _prestate_ocean_identity_matches(snapshot, manifest):
    return bool(
        snapshot.get("alive") and
        snapshot.get("launch_token") == manifest.get("token") and
        snapshot.get("uid") == manifest.get("expected_uid") and
        snapshot.get("pid") == snapshot.get("pgrp") == snapshot.get("session") and
        _same_path(snapshot.get("cwd"), manifest.get("workspace_root")) and
        _command_matches(snapshot.get("argv") or [], manifest.get("argv") or [])
    )


def _prestate_process_scan(manifest):
    exact_guardians = []
    exact_oceans = []
    conflicts = []
    try:
        entries = os.listdir("/proc")
    except OSError as error:
        raise ValueError("cannot scan /proc for legacy guardian ownership: %s" % error)
    for entry in entries:
        if not entry.isdigit():
            continue
        snapshot = _process_snapshot(int(entry), include_launch_token=True)
        if not snapshot.get("alive"):
            continue
        argv = snapshot.get("argv") or []
        guardian_token = _option_value(argv, "--token")
        environment_token = snapshot.get("launch_token")
        if guardian_token != manifest["token"] and environment_token != manifest["token"]:
            continue
        if guardian_token == manifest["token"]:
            if _prestate_guardian_identity_matches(snapshot, manifest):
                exact_guardians.append(snapshot)
            else:
                conflicts.append(dict(snapshot, kind="guardian_identity_conflict"))
            continue
        if _prestate_ocean_identity_matches(snapshot, manifest):
            exact_oceans.append(snapshot)
        else:
            conflicts.append(dict(snapshot, kind="token_process_identity_conflict"))
    return {
        "exact_guardians": sorted(exact_guardians, key=lambda row: row["pid"]),
        "exact_oceans": sorted(exact_oceans, key=lambda row: row["pid"]),
        "conflicts": sorted(conflicts, key=lambda row: (row.get("kind"), row["pid"])),
    }


def _prestate_scan_signature(scan):
    rows = []
    for name in ("exact_guardians", "exact_oceans", "conflicts"):
        for row in scan.get(name) or []:
            rows.append((
                name, row.get("kind"), row.get("pid"), row.get("start_ticks"),
                row.get("ppid"), row.get("pgrp"), row.get("session"), row.get("uid"),
                row.get("cwd"), tuple(row.get("argv") or []), row.get("launch_token"),
            ))
    return tuple(rows)


def _orphan_scope_snapshot_signature(rows):
    return tuple(sorted((
        row.get("kind"), row.get("pid"), row.get("start_ticks"), row.get("ppid"),
        row.get("pgrp"), row.get("session"), row.get("uid"), row.get("cwd"),
        tuple(row.get("argv") or []), row.get("launch_token"),
    ) for row in rows))


def _orphan_scope_member(snapshot, kind):
    return {
        "kind": kind,
        "pid": snapshot.get("pid"),
        "start_ticks": snapshot.get("start_ticks"),
        "ppid": snapshot.get("ppid"),
        "pgrp": snapshot.get("pgrp"),
        "session": snapshot.get("session"),
        "uid": snapshot.get("uid"),
        "cwd": snapshot.get("cwd"),
        "argv": list(snapshot.get("argv") or []),
        "launch_token": snapshot.get("launch_token"),
    }


def _orphan_scope_identity_matches(record, snapshot):
    if not isinstance(record, dict) or not snapshot.get("alive"):
        return False
    observed = _orphan_scope_member(snapshot, record.get("kind"))
    return _orphan_scope_snapshot_signature([record]) == _orphan_scope_snapshot_signature(
        [observed]
    )


def _orphan_helper_cleanup_order(rows):
    by_pid = {
        row.get("pid"): row for row in rows
        if isinstance(row, dict) and isinstance(row.get("pid"), int)
    }

    def ancestry_depth(row):
        depth = 0
        seen = set()
        parent_pid = row.get("ppid")
        while parent_pid in by_pid and parent_pid not in seen:
            seen.add(parent_pid)
            depth += 1
            parent_pid = by_pid[parent_pid].get("ppid")
        return depth

    return sorted(
        rows, key=lambda row: (ancestry_depth(row), row.get("pid") or -1), reverse=True,
    )


def _scan_orphaned_ocean_scope(manifest, guardian):
    if manifest.get("guardian_token_identity") is None:
        return {
            "members": [],
            "helpers": [],
            "conflicts": [{"kind": "unscoped_guardian_token", "pid": None}],
        }
    token_identity = validate_guardian_token_identity(
        manifest["guardian_token_identity"]
    )
    expected_pid = int(guardian.get("pid", -1))
    expected_pgid = int(guardian.get("pgid", -2))
    expected_sid = int(guardian.get("sid", -3))
    expected_uid = int(manifest.get("expected_uid", -4))
    expected_token = manifest.get("token")
    if token_identity.get("token") != expected_token:
        raise ValueError("guardian scoped token does not match its manifest")
    members = []
    helpers = []
    conflicts = []
    try:
        entries = os.listdir("/proc")
    except OSError as error:
        raise ValueError("cannot scan /proc for orphaned Ocean ownership: %s" % error)
    for entry in entries:
        if not entry.isdigit():
            continue
        pid = int(entry)
        process_stat = _process_stat(pid)
        if not process_stat or process_stat.get("state") == "Z":
            continue
        group_related = bool(
            pid == expected_pid or process_stat.get("pgrp") == expected_pgid
            or process_stat.get("session") == expected_sid
        )
        snapshot = _process_snapshot(pid, include_launch_token=True)
        argv_token = _option_value(snapshot.get("argv") or [], "--token")
        token_related = bool(
            snapshot.get("alive") and (
                snapshot.get("launch_token") == expected_token or argv_token == expected_token
            )
        )
        if not group_related and not token_related:
            continue
        if not snapshot.get("alive"):
            conflicts.append({
                "kind": "owned_process_uninspectable", "pid": pid,
                "start_ticks": process_stat.get("start_ticks"),
                "ppid": process_stat.get("ppid"), "pgrp": process_stat.get("pgrp"),
                "session": process_stat.get("session"), "uid": None, "cwd": None,
                "argv": [], "launch_token": None,
            })
            continue
        exact_scope = bool(
            snapshot.get("uid") == expected_uid and
            snapshot.get("launch_token") == expected_token and
            _path_within(snapshot.get("cwd"), manifest.get("workspace_root")) and
            isinstance(snapshot.get("argv"), list) and bool(snapshot.get("argv"))
        )
        if pid == expected_pid:
            if exact_scope and _ocean_identity_matches(snapshot, manifest, guardian):
                members.append(_orphan_scope_member(snapshot, "ocean_leader"))
            else:
                conflicts.append(_orphan_scope_member(snapshot, "ocean_leader_identity_conflict"))
            continue
        if (
                snapshot.get("pgrp") == expected_pgid and
                snapshot.get("session") == expected_sid and exact_scope):
            members.append(_orphan_scope_member(snapshot, "ocean_group_member"))
            continue
        if token_related and exact_scope:
            helpers.append(_orphan_scope_member(snapshot, "scoped_token_helper"))
            continue
        kind = (
            "scoped_token_helper_identity_conflict" if token_related
            else "ocean_group_or_session_identity_conflict"
        )
        conflicts.append(_orphan_scope_member(snapshot, kind))
    return {
        "members": sorted(members, key=lambda row: row.get("pid") or -1),
        "helpers": sorted(helpers, key=lambda row: row.get("pid") or -1),
        "conflicts": sorted(
            conflicts, key=lambda row: (row.get("kind") or "", row.get("pid") or -1),
        ),
    }


def _orphan_scope_scan_signature(scan):
    return (
        _orphan_scope_snapshot_signature(scan.get("members") or []),
        _orphan_scope_snapshot_signature(scan.get("helpers") or []),
        _orphan_scope_snapshot_signature(scan.get("conflicts") or []),
    )


def _classify_prestate_scan(manifest_path, manifest, scan, lock_held=False):
    state_path = manifest["state_path"]
    conflicts = scan["conflicts"]
    guardians = scan["exact_guardians"]
    oceans = scan["exact_oceans"]
    if conflicts:
        return {
            "state": "ambiguous",
            "reason": "token-matching process identity conflicts with the launch manifest",
            "manifest_path": manifest_path, "state_path": state_path,
            "processes": [
                _orphan_scope_member(row, row.get("kind"))
                for row in conflicts
            ],
            "termination_proof": {"ok": False, "kind": "process_identity_conflict"},
        }
    if oceans:
        return {
            "state": "ambiguous",
            "reason": "token-owned Ocean exists without durable guardian state",
            "manifest_path": manifest_path, "state_path": state_path,
            "processes": [
                {"kind": "ocean", "pid": row["pid"], "pgid": row["pgrp"]}
                for row in oceans
            ],
            "termination_proof": {"ok": False, "kind": "ocean_without_guardian_state"},
        }
    if len(guardians) > 1:
        return {
            "state": "ambiguous",
            "reason": "multiple exact legacy guardians share one launch token",
            "manifest_path": manifest_path, "state_path": state_path,
            "processes": [{"kind": "guardian", "pid": row["pid"]} for row in guardians],
            "termination_proof": {"ok": False, "kind": "multiple_exact_guardians"},
        }
    if guardians:
        guardian = guardians[0]
        return {
            "state": "active", "manifest_path": manifest_path,
            "state_path": state_path,
            "processes": [{"kind": "guardian", "pid": guardian["pid"]}],
            "termination_proof": {"ok": False, "kind": "guardian_prestate_active"},
            "manifest": manifest, "prestate_guardian": guardian,
        }
    if lock_held:
        return {
            "state": "active",
            "reason": "guardian launch handshake lock is held",
            "manifest_path": manifest_path, "state_path": state_path,
            "processes": [{"kind": "launch_handshake"}],
            "launch_handshake": True,
            "termination_proof": {"ok": False, "kind": "launch_in_progress"},
        }
    return {
        "state": "dead", "manifest_path": manifest_path, "state_path": state_path,
        "processes": [],
        "termination_proof": {"ok": True, "kind": "ocean_not_spawned"},
    }


def _inspect_missing_guardian_state(manifest_path, manifest):
    state_path = manifest["state_path"]
    if not _has_durable_launch_protocol(manifest):
        return {
            "state": "ambiguous",
            "reason": "legacy guardian manifest predates durable launch-lock proof",
            "manifest_path": manifest_path, "state_path": state_path,
            "processes": [],
            "termination_proof": {"ok": False, "kind": "missing_launch_lock_proof"},
        }
    descriptor = None
    try:
        descriptor = _open_launch_lock(manifest["launch_lock_path"], create=False)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (IOError, OSError) as error:
            if getattr(error, "errno", None) in (errno.EACCES, errno.EAGAIN):
                first = _prestate_process_scan(manifest)
                time.sleep(_STABLE_SCAN_INTERVAL_SECONDS)
                if os.path.lexists(state_path):
                    return {"retry_with_state": True}
                second = _prestate_process_scan(manifest)
                if _prestate_scan_signature(first) != _prestate_scan_signature(second):
                    return _classify_prestate_scan(
                        manifest_path, manifest,
                        {"exact_guardians": [], "exact_oceans": [], "conflicts": []},
                        lock_held=True,
                    )
                return _classify_prestate_scan(
                    manifest_path, manifest, second, lock_held=True,
                )
            raise
        if os.path.lexists(state_path):
            return {"retry_with_state": True}
        first = _prestate_process_scan(manifest)
        time.sleep(_STABLE_SCAN_INTERVAL_SECONDS)
        if os.path.lexists(state_path):
            return {"retry_with_state": True}
        second = _prestate_process_scan(manifest)
        if _prestate_scan_signature(first) != _prestate_scan_signature(second):
            return {
                "state": "ambiguous",
                "reason": "legacy guardian pre-state /proc ownership scan was unstable",
                "manifest_path": manifest_path, "state_path": state_path,
                "processes": [],
                "termination_proof": {"ok": False, "kind": "unstable_proc_scan"},
            }
        return _classify_prestate_scan(manifest_path, manifest, second)
    finally:
        if descriptor is not None:
            close_legacy_guardian_launch_lock(descriptor)


def _classify_orphaned_nonterminal_scan(manifest_path, manifest, guardian, scan):
    state_path = manifest["state_path"]
    conflicts = list(scan.get("conflicts") or [])
    guardians = list(scan.get("exact_guardians") or [])
    oceans = list(scan.get("exact_oceans") or [])
    expected_guardian_pid = guardian.get("guardian_pid")
    expected_guardian_start = guardian.get("guardian_start_ticks")
    if guardians:
        exact = guardians[0] if len(guardians) == 1 else None
        if (
                exact is None or expected_guardian_pid is None
                or int(exact.get("pid", -1)) != int(expected_guardian_pid)
                or int(exact.get("start_ticks", -1)) != int(expected_guardian_start or -2)):
            conflicts.extend(dict(row, kind="guardian_state_identity_conflict") for row in guardians)
    expected_ocean_pid = guardian.get("pid")
    expected_ocean_start = guardian.get("pid_start_ticks")
    if oceans and expected_ocean_pid is not None:
        if len(oceans) != 1 or (
                int(oceans[0].get("pid", -1)) != int(expected_ocean_pid)
                or int(oceans[0].get("start_ticks", -1)) != int(expected_ocean_start or -2)):
            conflicts.extend(dict(row, kind="ocean_state_identity_conflict") for row in oceans)
    if len(oceans) > 1:
        conflicts.extend(dict(row, kind="multiple_exact_oceans") for row in oceans)
    if conflicts:
        return {
            "state": "ambiguous",
            "reason": "token-owned process conflicts with nonterminal guardian state",
            "manifest_path": manifest_path, "state_path": state_path,
            "processes": [
                _orphan_scope_member(row, row.get("kind")) for row in conflicts
            ],
            "termination_proof": {"ok": False, "kind": "process_identity_conflict"},
        }
    if guardians:
        return {
            "state": "active", "manifest_path": manifest_path, "state_path": state_path,
            "processes": [{"kind": "guardian", "pid": guardians[0]["pid"]}],
            "termination_proof": {"ok": False, "kind": "guardian_nonterminal_active"},
            "guardian": guardian, "manifest": manifest,
        }
    if oceans:
        return {
            "state": "active",
            "reason": "orphaned Ocean is still settling after guardian loss",
            "manifest_path": manifest_path, "state_path": state_path,
            "processes": [
                {"kind": "ocean", "pid": row["pid"], "pgid": row["pgrp"]}
                for row in oceans
            ],
            "launch_handshake": True, "orphan_settling": True,
            "termination_proof": {"ok": False, "kind": "orphaned_ocean_settling"},
        }
    return {
        "state": "dead", "manifest_path": manifest_path, "state_path": state_path,
        "processes": [],
        "termination_proof": {
            "ok": True, "kind": "owned_processes_absent_after_guardian_loss",
            "status": guardian.get("status"),
        },
    }


def _inspect_orphaned_nonterminal_state(manifest_path, manifest, guardian):
    if not _has_durable_launch_protocol(manifest):
        return None
    if (
            guardian.get("status") != "starting"
            or any(guardian.get(name) is not None for name in (
                "pid", "pgid", "sid", "pid_start_ticks",
            ))):
        return {
            "state": "ambiguous",
            "reason": "nonterminal guardian state is outside the proven pre-Ocean launch window",
            "manifest_path": manifest_path, "state_path": manifest["state_path"],
            "processes": [],
            "termination_proof": {
                "ok": False, "kind": "nonterminal_ocean_history_unproven",
                "status": guardian.get("status"),
            },
        }
    descriptor = None
    try:
        descriptor = _open_launch_lock(manifest["launch_lock_path"], create=False)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (IOError, OSError) as error:
            if getattr(error, "errno", None) in (errno.EACCES, errno.EAGAIN):
                return {
                    "state": "active",
                    "reason": "guardian canonical state is being committed before launch unlock",
                    "manifest_path": manifest_path, "state_path": manifest["state_path"],
                    "processes": [{"kind": "launch_handshake"}],
                    "launch_handshake": True,
                    "termination_proof": {"ok": False, "kind": "launch_in_progress"},
                }
            raise
        first = _prestate_process_scan(manifest)
        time.sleep(_STABLE_SCAN_INTERVAL_SECONDS)
        if not _regular_file(manifest["state_path"]):
            return {"retry_with_state": True}
        current = load_json(manifest["state_path"])
        if current != guardian:
            return {"retry_with_state": True}
        second = _prestate_process_scan(manifest)
        if _prestate_scan_signature(first) != _prestate_scan_signature(second):
            return {
                "state": "ambiguous",
                "reason": "nonterminal guardian ownership scan was unstable",
                "manifest_path": manifest_path, "state_path": manifest["state_path"],
                "processes": [],
                "termination_proof": {"ok": False, "kind": "unstable_proc_scan"},
            }
        return _classify_orphaned_nonterminal_scan(
            manifest_path, manifest, guardian, second,
        )
    finally:
        if descriptor is not None:
            close_legacy_guardian_launch_lock(descriptor)


def _orphan_recovery_result(inspection, reason, kind, processes=None):
    return {
        "state": "ambiguous",
        "reason": reason,
        "manifest_path": inspection.get("manifest_path"),
        "state_path": inspection.get("state_path"),
        "processes": list(processes or []),
        "termination_proof": {"ok": False, "kind": kind},
        "guardian": inspection.get("guardian"),
        "manifest": inspection.get("manifest"),
    }


def _recover_orphaned_ocean_after_guardian_loss(inspection, deadline):
    manifest = inspection.get("manifest") or {}
    guardian = inspection.get("guardian") or {}
    if (
            not _has_durable_launch_protocol(manifest) or
            manifest.get("guardian_token_identity") is None):
        return _orphan_recovery_result(
            inspection,
            "orphaned Ocean recovery requires a scoped durable guardian token",
            "orphan_recovery_identity_incomplete",
            inspection.get("processes"),
        )
    try:
        validate_guardian_token_identity(manifest["guardian_token_identity"])
    except Exception as error:
        return _orphan_recovery_result(
            inspection,
            "orphaned Ocean scoped token identity is invalid: %s" % error,
            "orphan_recovery_identity_invalid",
            inspection.get("processes"),
        )
    if guardian.get("status") in TERMINAL_GUARDIAN_STATES:
        return _orphan_recovery_result(
            inspection, "terminal guardian state cannot enter orphan recovery",
            "orphan_recovery_state_invalid", inspection.get("processes"),
        )
    try:
        guardian_pid = int(guardian.get("guardian_pid"))
        guardian_start_ticks = int(guardian.get("guardian_start_ticks"))
        ocean_pid = int(guardian.get("pid"))
        ocean_start_ticks = int(guardian.get("pid_start_ticks"))
        ocean_pgid = int(guardian.get("pgid"))
        ocean_sid = int(guardian.get("sid"))
    except (TypeError, ValueError):
        return _orphan_recovery_result(
            inspection, "orphaned Ocean persisted process identity is malformed",
            "orphan_recovery_identity_incomplete", inspection.get("processes"),
        )
    if not (
            guardian_pid > 1 and guardian_start_ticks > 0 and
            ocean_pid > 1 and ocean_start_ticks > 0 and
            ocean_pid == ocean_pgid == ocean_sid and
            isinstance(guardian.get("cwd"), str) and
            _same_path(guardian.get("cwd"), manifest.get("workspace_root")) and
            list(guardian.get("argv") or []) == list(manifest.get("argv") or [])):
        return _orphan_recovery_result(
            inspection, "orphaned Ocean persisted process identity is unsafe",
            "orphan_recovery_identity_incomplete", inspection.get("processes"),
        )
    guardian_snapshot = _process_snapshot(
        guardian_pid, include_launch_token=True,
    )
    if guardian_snapshot.get("alive"):
        return _orphan_recovery_result(
            inspection, "guardian is still live during orphan recovery",
            "orphan_recovery_guardian_live", inspection.get("processes"),
        )

    descriptor = None
    try:
        descriptor = _open_launch_lock(manifest["launch_lock_path"], create=False)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (IOError, OSError) as error:
            if getattr(error, "errno", None) in (errno.EACCES, errno.EAGAIN):
                return {
                    "state": "active",
                    "reason": "orphaned Ocean recovery lock is held",
                    "manifest_path": inspection.get("manifest_path"),
                    "state_path": inspection.get("state_path"),
                    "processes": [{"kind": "orphan_recovery_lock"}],
                    "recovery_in_progress": True,
                    "termination_proof": {
                        "ok": False, "kind": "orphan_recovery_in_progress",
                    },
                }
            raise
        if not _regular_file(manifest["state_path"]):
            return _orphan_recovery_result(
                inspection, "guardian state changed before orphan recovery",
                "orphan_recovery_state_changed",
            )
        current = load_json(manifest["state_path"])
        if current != guardian:
            return {"retry_with_state": True}

        first = _scan_orphaned_ocean_scope(manifest, guardian)
        time.sleep(_STABLE_SCAN_INTERVAL_SECONDS)
        if load_json(manifest["state_path"]) != guardian:
            return {"retry_with_state": True}
        second = _scan_orphaned_ocean_scope(manifest, guardian)
        if _orphan_scope_scan_signature(first) != _orphan_scope_scan_signature(second):
            return _orphan_recovery_result(
                inspection, "orphaned Ocean ownership scan was unstable",
                "orphan_recovery_unstable_scan",
            )
        conflicts = list(second.get("conflicts") or [])
        members = list(second.get("members") or [])
        helpers = list(second.get("helpers") or [])
        if conflicts:
            return _orphan_recovery_result(
                inspection,
                "orphaned Ocean has detached, cross-group, or conflicting token-owned processes",
                "orphan_recovery_process_conflict",
                conflicts,
            )
        leaders = [row for row in members if row.get("kind") == "ocean_leader"]
        if members and len(leaders) != 1:
            return _orphan_recovery_result(
                inspection,
                "orphaned Ocean leader exact scoped identity is unavailable",
                "orphan_recovery_leader_unavailable",
                members,
            )

        action = "stable_owned_process_absence"
        signal_sent = False
        group_signal_sent = False
        runtime_identity = None
        helper_identities = []
        helper_signals = []
        ordered_helpers = _orphan_helper_cleanup_order(helpers)
        for helper in ordered_helpers:
            snapshot = _process_snapshot(
                helper.get("pid"), include_launch_token=True,
            )
            if not _orphan_scope_identity_matches(helper, snapshot):
                return _orphan_recovery_result(
                    inspection,
                    "scoped token helper identity changed before cleanup",
                    "orphan_recovery_helper_identity_changed",
                    [helper, _orphan_scope_member(
                        snapshot, "scoped_token_helper_identity_changed",
                    )],
                )
        for helper in ordered_helpers:
            snapshot = _process_snapshot(
                helper.get("pid"), include_launch_token=True,
            )
            if not _orphan_scope_identity_matches(helper, snapshot):
                return _orphan_recovery_result(
                    inspection,
                    "scoped token helper identity changed immediately before SIGKILL",
                    "orphan_recovery_helper_identity_changed",
                    [helper, _orphan_scope_member(
                        snapshot, "scoped_token_helper_identity_changed",
                    )],
                )
            identity = _orphan_scope_member(snapshot, "scoped_token_helper")
            signal_record = {
                "pid": identity["pid"],
                "start_ticks": identity["start_ticks"],
                "signal": signal.SIGKILL,
                "result": "sent",
                "identity": dict(identity, argv=list(identity["argv"])),
            }
            try:
                os.kill(identity["pid"], signal.SIGKILL)
                signal_sent = True
            except OSError as error:
                if getattr(error, "errno", None) != errno.ESRCH:
                    return _orphan_recovery_result(
                        inspection, "scoped token helper SIGKILL failed: %s" % error,
                        "orphan_recovery_signal_failed", [identity],
                    )
                signal_record["signal"] = None
                signal_record["result"] = "already_absent"
            helper_identities.append(identity)
            helper_signals.append(signal_record)

        if leaders:
            expected_pid = int(guardian.get("pid"))
            expected_pgid = int(guardian.get("pgid"))
            expected_sid = int(guardian.get("sid"))
            if expected_pid != expected_pgid or expected_pid != expected_sid:
                return _orphan_recovery_result(
                    inspection, "orphaned Ocean persisted process-group identity is unsafe",
                    "orphan_recovery_group_identity_invalid",
                )
            leader = _process_snapshot(expected_pid, include_launch_token=True)
            if not _ocean_identity_matches(leader, manifest, guardian):
                return _orphan_recovery_result(
                    inspection, "orphaned Ocean identity changed before SIGKILL",
                    "orphan_recovery_identity_changed",
                )
            runtime_identity = _orphan_scope_member(leader, "ocean_leader")
            members = [
                runtime_identity if row.get("kind") == "ocean_leader" else row
                for row in members
            ]
            try:
                os.killpg(expected_pgid, signal.SIGKILL)
                signal_sent = True
                group_signal_sent = True
            except OSError as error:
                if getattr(error, "errno", None) != errno.ESRCH:
                    return _orphan_recovery_result(
                        inspection, "orphaned Ocean SIGKILL failed: %s" % error,
                        "orphan_recovery_signal_failed",
                    )
            action = "killed_exact_ocean_group"
        if helper_identities:
            action = "killed_exact_scoped_processes"

        final_scan = None
        while True:
            final_scan = _scan_orphaned_ocean_scope(manifest, guardian)
            if (
                    not final_scan.get("members") and
                    not final_scan.get("helpers") and
                    not final_scan.get("conflicts")):
                time.sleep(_STABLE_SCAN_INTERVAL_SECONDS)
                stable = _scan_orphaned_ocean_scope(manifest, guardian)
                if (
                        not stable.get("members") and
                        not stable.get("helpers") and
                        not stable.get("conflicts")):
                    final_scan = stable
                    break
            if time.monotonic() >= deadline:
                rows = list(final_scan.get("members") or []) + list(
                    final_scan.get("helpers") or []) + list(
                    final_scan.get("conflicts") or [])
                return _orphan_recovery_result(
                    inspection, "orphaned Ocean cleanup did not reach stable absence",
                    "orphan_recovery_cleanup_incomplete",
                    rows,
                )
            time.sleep(min(0.02, max(0.001, deadline - time.monotonic())))

        if load_json(manifest["state_path"]) != guardian:
            return {"retry_with_state": True}
        recovered_epoch = time.time()
        updated = dict(guardian)
        updated.update({
            "status": "exited",
            "updated_epoch": recovered_epoch,
            "finished_epoch": recovered_epoch,
            "returncode": -signal.SIGKILL if signal_sent else guardian.get("returncode"),
            "termination_reason": "guardian_loss_recovery",
            "term_sent": bool(guardian.get("term_sent")),
            "kill_sent": bool(guardian.get("kill_sent")) or signal_sent,
            "group_cleanup_complete": True,
            "remaining_group_pids": [],
            "adopted_term_sent": bool(guardian.get("adopted_term_sent")),
            "adopted_kill_sent": bool(guardian.get("adopted_kill_sent")),
            "descendant_cleanup_complete": True,
            "descendant_supervision_complete": False,
            "remaining_adopted_pids": [],
            "orphan_scope_cleanup_complete": True,
            "orphan_recovery": {
                "schema_version": ORPHAN_RECOVERY_SCHEMA_VERSION,
                "action": action,
                "recovered_epoch": recovered_epoch,
                "boot_id": guardian.get("launch_boot_id") or guardian.get("boot_id"),
                "token": guardian.get("token"),
                "guardian_pid": guardian.get("guardian_pid"),
                "guardian_start_ticks": guardian.get("guardian_start_ticks"),
                "ocean_pid": guardian.get("pid"),
                "ocean_start_ticks": guardian.get("pid_start_ticks"),
                "pgid": guardian.get("pgid"),
                "sid": guardian.get("sid"),
                "cwd": guardian.get("cwd"),
                "argv": list(guardian.get("argv") or []),
                "launch_argv": list(guardian.get("argv") or []),
                "runtime_identity": runtime_identity,
                "signal": signal.SIGKILL if group_signal_sent else None,
                "owned_members": members,
                "detached_helpers": helper_identities,
                "detached_helper_signals": helper_signals,
                "stable_scans_before": 2,
                "stable_scans_after": 2,
                "remaining_owned_processes": [],
            },
        })
        atomic_write_json(manifest["state_path"], updated)
        proof = _terminal_cleanup_proof(updated, manifest)
        if proof.get("ok") is not True:
            return _orphan_recovery_result(
                inspection, "persisted orphaned Ocean recovery proof is invalid",
                "orphan_recovery_proof_invalid",
            )
        return {
            "state": "dead",
            "manifest_path": inspection.get("manifest_path"),
            "state_path": inspection.get("state_path"),
            "processes": [],
            "termination_proof": proof,
            "termination_requested": signal_sent,
        }
    except Exception as error:
        return _orphan_recovery_result(
            inspection, "orphaned Ocean recovery inspection failed: %s: %s" % (
                error.__class__.__name__, error,
            ), "orphan_recovery_inspection_failed",
        )
    finally:
        if descriptor is not None:
            close_legacy_guardian_launch_lock(descriptor)


def inspect_legacy_guardian_attempt(attempt_root, expected_workspace_root=None,
                                    expected_token_identity=None):
    try:
        manifest_path, manifest = _validated_manifest(
            attempt_root, expected_workspace_root, expected_token_identity,
        )
        state_path = manifest["state_path"]
        if not os.path.lexists(state_path):
            if manifest.get('_cr_moved_helper'):
                raise ValueError("moved guardian lacks durable terminal state")
            missing = _inspect_missing_guardian_state(manifest_path, manifest)
            if missing.get("retry_with_state"):
                return inspect_legacy_guardian_attempt(
                    attempt_root, expected_workspace_root, expected_token_identity,
                )
            return missing
        if not _regular_file(state_path):
            raise ValueError("legacy guardian state is not a regular file")
        guardian = load_json(state_path)
        if not isinstance(guardian, dict) or guardian.get("schema_version") != GUARDIAN_SCHEMA_VERSION:
            raise ValueError("legacy guardian state schema is not recognized")
        if guardian.get("token") != manifest["token"]:
            raise ValueError("legacy guardian state token does not match its manifest")
        if not _same_path(guardian.get("cwd"), manifest["workspace_root"]):
            raise ValueError("legacy guardian state cwd does not match its manifest")
        if list(guardian.get("argv") or []) != manifest["argv"]:
            raise ValueError("legacy guardian state command does not match its manifest")
        if guardian.get("expected_parent_pid") != manifest["expected_parent_pid"]:
            raise ValueError("legacy guardian state parent identity does not match its manifest")

        current_boot_id = _current_boot_id()
        launch_boot_id = guardian.get("launch_boot_id") or guardian.get("boot_id")
        prior_boot = bool(current_boot_id and launch_boot_id and current_boot_id != launch_boot_id)
        terminal_proof = _terminal_cleanup_proof(guardian, manifest)
        if manifest.get('_cr_moved_helper'):
            identity_fields = ('guardian_pid', 'guardian_start_ticks', 'pid', 'pid_start_ticks')
            if (not _has_durable_launch_protocol(manifest) or guardian.get('status') != 'exited' or
                    terminal_proof.get('ok') is not True or not launch_boot_id or
                    launch_boot_id != manifest.get('expected_parent_boot_id') or
                    any(isinstance(guardian.get(key), bool) or not isinstance(guardian.get(key), int) or
                        guardian.get(key) <= 0 for key in identity_fields) or
                    os.stat(manifest_path).st_uid != manifest.get('expected_uid') or
                    os.stat(state_path).st_uid != manifest.get('expected_uid')):
                raise ValueError("moved guardian terminal identity or cleanup proof is incomplete")
        if prior_boot:
            return {
                "state": "dead", "manifest_path": manifest_path, "state_path": state_path,
                "processes": [],
                "termination_proof": {"ok": True, "kind": "prior_boot_processes_absent"},
            }

        guardian_snapshot = _process_snapshot(guardian.get("guardian_pid"))
        ocean_snapshot = _process_snapshot(
            guardian.get("pid"), include_launch_token=True,
        )
        active = []
        ambiguous = []
        if guardian_snapshot.get("alive"):
            if _guardian_identity_matches(guardian_snapshot, manifest, guardian):
                active.append({"kind": "guardian", "pid": guardian_snapshot["pid"]})
            elif guardian.get("status") not in TERMINAL_GUARDIAN_STATES:
                ambiguous.append(_orphan_scope_member(
                    guardian_snapshot, "guardian_identity_conflict",
                ))
        if ocean_snapshot.get("alive"):
            if _ocean_identity_matches(ocean_snapshot, manifest, guardian):
                active.append({
                    "kind": "ocean", "pid": ocean_snapshot["pid"],
                    "pgid": ocean_snapshot["pgrp"],
                })
            elif guardian.get("status") not in TERMINAL_GUARDIAN_STATES:
                ambiguous.append(_orphan_scope_member(
                    ocean_snapshot, "ocean_identity_conflict",
                ))
        if ambiguous:
            return {
                "state": "ambiguous", "reason": "live PID identity does not match persisted ownership",
                "manifest_path": manifest_path, "state_path": state_path,
                "processes": ambiguous, "termination_proof": terminal_proof,
            }
        nonterminal_orphan = bool(
            guardian.get("status") == "starting"
            and all(guardian.get(name) is None for name in (
                "pid", "pgid", "sid", "pid_start_ticks",
            ))
            and not any(row.get("kind") == "guardian" for row in active)
        )
        if nonterminal_orphan:
            recovered = _inspect_orphaned_nonterminal_state(
                manifest_path, manifest, guardian,
            )
            if recovered is not None:
                if recovered.get("retry_with_state"):
                    return inspect_legacy_guardian_attempt(
                        attempt_root, expected_workspace_root, expected_token_identity,
                    )
                return recovered
        if active:
            if manifest.get('_cr_moved_helper'):
                return {"state": "ambiguous", "reason": "moved guardian still has a live owned process",
                        "manifest_path": manifest_path, "state_path": state_path,
                        "processes": active, "termination_proof": {"ok": False, "kind": "moved_helper_active"}}
            return {
                "state": "active", "manifest_path": manifest_path, "state_path": state_path,
                "processes": active, "termination_proof": terminal_proof,
                "guardian": guardian, "manifest": manifest,
            }
        if terminal_proof.get("ok"):
            return {
                "state": "dead", "manifest_path": manifest_path, "state_path": state_path,
                "processes": [], "termination_proof": terminal_proof,
            }
        return {
            "state": "ambiguous", "reason": "guardian final cleanup proof is unavailable",
            "manifest_path": manifest_path, "state_path": state_path,
            "processes": [], "termination_proof": terminal_proof,
            "guardian": guardian, "manifest": manifest,
        }
    except Exception as error:
        return {
            "state": "ambiguous",
            "reason": "%s: %s" % (error.__class__.__name__, error),
            "manifest_path": os.path.join(os.path.abspath(attempt_root), MANIFEST_NAME),
            "processes": [],
            "termination_proof": {"ok": False, "kind": "inspection_failed"},
        }


def quiesce_legacy_guardian_attempt(attempt_root, expected_workspace_root=None,
                                    timeout_seconds=30.0, terminate_owned=False,
                                    expected_token_identity=None):
    deadline = time.monotonic() + max(0.0, float(timeout_seconds))
    termination_requested = False
    while True:
        inspection = inspect_legacy_guardian_attempt(
            attempt_root, expected_workspace_root, expected_token_identity,
        )
        if inspection.get("state") == "dead":
            inspection["termination_requested"] = termination_requested
            return inspection
        orphan_recovery = False
        if inspection.get("state") == "ambiguous":
            reason = inspection.get("reason") or ""
            if "final cleanup proof is unavailable" not in reason:
                return inspection
            orphan_recovery = bool(terminate_owned and not termination_requested)
        elif inspection.get("recovery_in_progress"):
            pass
        elif inspection.get("launch_handshake"):
            pass
        elif terminate_owned and not termination_requested:
            guardians = [row for row in inspection.get("processes", []) if row.get("kind") == "guardian"]
            oceans = [row for row in inspection.get("processes", []) if row.get("kind") == "ocean"]
            if not guardians and len(oceans) <= 1 and inspection.get("guardian") is not None:
                orphan_recovery = True
            elif len(guardians) != 1:
                inspection["state"] = "ambiguous"
                inspection["reason"] = (
                    "owned Ocean is active without one exact live guardian" if oceans
                    else "exact live guardian identity is unavailable"
                )
                return inspection
            else:
                manifest = inspection["manifest"]
                snapshot = _process_snapshot(guardians[0]["pid"], include_launch_token=True)
                prestate_guardian = inspection.get("prestate_guardian")
                if prestate_guardian is not None:
                    unchanged = bool(
                        snapshot.get("start_ticks") == prestate_guardian.get("start_ticks") and
                        _prestate_guardian_identity_matches(snapshot, manifest)
                    )
                else:
                    unchanged = _guardian_identity_matches(snapshot, manifest, inspection["guardian"])
                if not unchanged:
                    inspection["state"] = "ambiguous"
                    inspection["reason"] = "guardian identity changed before termination"
                    return inspection
                try:
                    os.kill(snapshot["pid"], signal.SIGCONT)
                except OSError as error:
                    if getattr(error, "errno", None) == errno.ESRCH:
                        continue
                    inspection["state"] = "ambiguous"
                    inspection["reason"] = "guardian continuation failed: %s" % error
                    return inspection
                resumed = _process_snapshot(
                    snapshot["pid"], include_launch_token=True,
                )
                if resumed.get("alive"):
                    if prestate_guardian is not None:
                        unchanged = bool(
                            resumed.get("start_ticks") == prestate_guardian.get("start_ticks") and
                            _prestate_guardian_identity_matches(resumed, manifest)
                        )
                    else:
                        unchanged = _guardian_identity_matches(
                            resumed, manifest, inspection["guardian"],
                        )
                    if not unchanged:
                        inspection["state"] = "ambiguous"
                        inspection["reason"] = (
                            "guardian identity changed after continuation before termination"
                        )
                        return inspection
                    try:
                        os.kill(resumed["pid"], signal.SIGTERM)
                    except OSError as error:
                        if getattr(error, "errno", None) != errno.ESRCH:
                            inspection["state"] = "ambiguous"
                            inspection["reason"] = "guardian termination failed: %s" % error
                            return inspection
                    else:
                        termination_requested = True
        if orphan_recovery:
            recovered = _recover_orphaned_ocean_after_guardian_loss(inspection, deadline)
            if recovered.get("retry_with_state"):
                continue
            if recovered.get("state") == "dead":
                recovered["termination_requested"] = bool(
                    termination_requested or recovered.get("termination_requested")
                )
                return recovered
            if recovered.get("recovery_in_progress"):
                inspection = recovered
            else:
                recovered["termination_requested"] = bool(
                    termination_requested or recovered.get("termination_requested")
                )
                return recovered
        if time.monotonic() >= deadline:
            inspection["state"] = "ambiguous"
            inspection["reason"] = (
                "guardian launch handshake did not settle before timeout"
                if inspection.get("launch_handshake") else
                "guardian did not produce terminal cleanup proof before timeout"
            )
            inspection["termination_requested"] = termination_requested
            return inspection
        time.sleep(min(0.05, max(0.001, deadline - time.monotonic())))
