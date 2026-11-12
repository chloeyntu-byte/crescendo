from __future__ import print_function

import errno
import fcntl
import functools
import hashlib
import json
import os
import shutil
import stat
import threading
import time

from crescendo_engine.runtime import bind_text
_contract_text = bind_text()

from .candidate_timing import elapsed, timestamp, update_cleanup_timing
from .legacy_guardian import (
    LAUNCH_LOCK_NAME as LEGACY_GUARDIAN_LAUNCH_LOCK_NAME,
    MANIFEST_NAME as LEGACY_GUARDIAN_MANIFEST_NAME,
    MANIFEST_SCHEMA_VERSION as LEGACY_GUARDIAN_PROTOCOL,
    inspect_legacy_guardian_attempt,
)
from .utils import atomic_write_json, is_under, load_json, sha256_file, utc_timestamp
from .native_log_io import OwnedLogReadersBusy, native_log_cleanup_scope, native_log_read_scope


class UnsafeCleanupError(RuntimeError):
    pass


class ArtifactCleanupError(OSError):
    """Proven-dead artifacts remain; keep the process proof and deletion evidence."""

    def __init__(self, error, detail):
        self.detail = detail
        message = "artifact directory cleanup failed at %s: %s" % (
            detail["payload_root"], error,
        )
        OSError.__init__(self, getattr(error, "errno", None), message,
                         detail.get("failed_path") or detail["payload_root"])


_CLEANUP_THREAD_LOCK = threading.RLock()
_CLEANUP_LOCAL = threading.local()


def _serialize_cleanup(run_root_index):
    """One writer per run, including recovery, without another persistent file."""
    def decorate(function):
        @functools.wraps(function)
        def serialized(*args, **kwargs):
            value = args[run_root_index] if len(args) > run_root_index else kwargs["run_root"]
            run_root, _device = _validated_cleanup_root(value)
            try:
                with native_log_cleanup_scope() as barrier:
                    result = locked_cleanup(*args, **kwargs)
                    if isinstance(result, dict):
                        result.setdefault("reader_barrier", barrier)
                    return result
            except OwnedLogReadersBusy as error:
                detail = {"payload_root": run_root, "failed_path": run_root,
                          "artifact_cleanup_complete": False, "retryable": True,
                          "reader_barrier": error.detail, "attempt_count": 0,
                          "retry_hint": "Owned log readers are still active; artifacts retained. Retry Exit after the bounded read closes."}
                raise ArtifactCleanupError(OSError(errno.EBUSY, str(error), run_root), detail)

        def locked_cleanup(*args, **kwargs):
            value = args[run_root_index] if len(args) > run_root_index else kwargs["run_root"]
            run_root, _device = _validated_cleanup_root(value)
            with _CLEANUP_THREAD_LOCK:
                active = getattr(_CLEANUP_LOCAL, "active", None)
                if active is None:
                    active = {}
                    _CLEANUP_LOCAL.active = active
                if run_root in active:
                    return function(*args, **kwargs)
                descriptor = os.open(run_root, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) |
                                     getattr(os, "O_NOFOLLOW", 0))
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_EX)
                    if not os.path.samestat(os.fstat(descriptor), os.lstat(run_root)):
                        raise UnsafeCleanupError("cleanup run directory changed before locking")
                    active[run_root] = descriptor
                    try:
                        return function(*args, **kwargs)
                    finally:
                        del active[run_root]
                finally:
                    os.close(descriptor)
        return serialized
    return decorate


def _cleanup_filesystem(path):
    """Describe the actual mount; never infer NFS from a deletion error alone."""
    result = {"type": None, "mount_point": None}
    try:
        with open("/proc/self/mountinfo", "r") as stream:
            lines = stream.read(512 * 1024).splitlines()
        for line in lines:
            left, right = line.split(" - ", 1)
            fields, filesystem = left.split(), right.split()[0]
            mount = fields[4]
            for escaped, literal in (("\\040", " "), ("\\011", "\t"),
                                     ("\\012", "\n"), ("\\134", "\\")):
                mount = mount.replace(escaped, literal)
            if ((path == mount or path.startswith(mount.rstrip("/") + "/"))
                    and len(mount) > len(result["mount_point"] or "")):
                result = {"type": filesystem, "mount_point": mount}
    except (IOError, OSError, ValueError, IndexError):
        pass
    return result


def _cleanup_open_handles(payload_root, max_seconds=0.25, max_fds=8192):
    """Bounded read-only evidence, not authority to signal an unrelated reader."""
    evidence = {"matches": [], "scanned_pids": 0, "scanned_fds": 0,
                "permission_denied": 0, "complete": False,
                "does_not_prove_process_absence": True}
    started = time.monotonic()
    payload_root = os.path.abspath(payload_root)
    try:
        process_ids = [int(name) for name in os.listdir("/proc") if name.isdigit()]
    except OSError as error:
        evidence["error"] = str(error)
        return evidence
    # Find our own accidentally retained log handle before looking at other PIDs.
    preferred = [os.getpid(), os.getppid()]
    process_ids = preferred + sorted(set(process_ids) - set(preferred))
    for pid in process_ids:
        if time.monotonic() - started >= max_seconds or evidence["scanned_fds"] >= max_fds:
            evidence["limit_reached"] = True
            break
        evidence["scanned_pids"] += 1
        try:
            descriptors = os.listdir("/proc/%d/fd" % pid)
        except OSError as error:
            if error.errno in (errno.EPERM, errno.EACCES):
                evidence["permission_denied"] += 1
            continue
        for descriptor in descriptors:
            if time.monotonic() - started >= max_seconds or evidence["scanned_fds"] >= max_fds:
                evidence["limit_reached"] = True
                break
            evidence["scanned_fds"] += 1
            try:
                target = os.readlink("/proc/%d/fd/%s" % (pid, descriptor))
            except OSError:
                continue
            actual_path = target[:-10] if target.endswith(" (deleted)") else target
            if not (actual_path == payload_root or actual_path.startswith(payload_root + os.sep)):
                continue
            record = {"pid": pid, "fd": int(descriptor), "path": actual_path,
                      "deleted": target.endswith(" (deleted)"),
                      "this_process": pid == os.getpid(), "action": "retain_and_retry"}
            try:
                with open("/proc/%d/comm" % pid, "r") as stream:
                    record["process_name"] = stream.read(128).strip()
                with open("/proc/%d/stat" % pid, "r") as stream:
                    process_stat = stream.read(8192)
                fields = process_stat[process_stat.rfind(")") + 2:].split()
                record["start_ticks"] = int(fields[19])
            except (IOError, OSError, ValueError, IndexError):
                record["identity_unavailable"] = True
            evidence["matches"].append(record)
            if len(evidence["matches"]) >= 32:
                evidence["limit_reached"] = True
                break
        if evidence.get("limit_reached"):
            break
    evidence["complete"] = not evidence.get("limit_reached", False) and not evidence["permission_denied"]
    evidence["elapsed_seconds"] = round(time.monotonic() - started, 4)
    return evidence


def _artifact_deletion_detail(payload_root, error, attempts):
    failed_path = getattr(error, "filename", None)
    if isinstance(failed_path, str) and not os.path.isabs(failed_path):
        failed_path = os.path.join(payload_root, failed_path)
    failed_path = os.path.abspath(failed_path) if isinstance(failed_path, str) else payload_root
    remaining = []
    try:
        if is_under(failed_path, payload_root) and os.path.isdir(failed_path) and not os.path.islink(failed_path):
            remaining = sorted(os.listdir(failed_path))[:32]
        elif os.path.isdir(payload_root) and not os.path.islink(payload_root):
            remaining = sorted(os.listdir(payload_root))[:32]
    except OSError as inspection_error:
        remaining = ["<unavailable: %s>" % inspection_error]
    return {
        "artifact_cleanup_complete": False,
        "payload_root": os.path.abspath(payload_root),
        "failed_path": failed_path,
        "errno": getattr(error, "errno", None),
        "exception": "%s: %s" % (error.__class__.__name__, error),
        "remaining_names": remaining,
        "attempts": list(attempts),
    }


_DIAGNOSTIC_IDENTITY_LIMIT = 4 * 1024 * 1024
_DIAGNOSTIC_IDENTITY_PATHS = (
    _contract_text("point_identity.json"), _contract_text("portability_settings.json"),
    "workspace/portability_settings.json",
)


def _diagnostic_directory_fd(run_root, components, run_device, create=False):
    """Anchor diagnostic reads/writes to owned directories, without links."""
    if os.path.realpath(run_root) != run_root:
        raise UnsafeCleanupError("diagnostic run path contains a symbolic link")
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(run_root, flags)
    path = run_root
    try:
        for component in components:
            if component in ("", ".", "..") or "/" in component or "\\" in component:
                raise UnsafeCleanupError("diagnostic directory component is not canonical")
            path = os.path.join(path, component)
            if create:
                try:
                    os.mkdir(component, 0o700, dir_fd=descriptor)
                    os.fsync(descriptor)
                except OSError as error:
                    if error.errno != errno.EEXIST:
                        raise
            child = os.open(component, flags, dir_fd=descriptor)
            child_stat = os.fstat(child)
            if (not stat.S_ISDIR(child_stat.st_mode) or child_stat.st_dev != run_device
                    or os.path.ismount(path)):
                os.close(child)
                raise UnsafeCleanupError("diagnostic directory crosses an owned filesystem boundary: %s" % path)
            os.close(descriptor)
            descriptor = child
        root_stat = os.fstat(descriptor)
        if not stat.S_ISDIR(root_stat.st_mode) or root_stat.st_dev != run_device:
            raise UnsafeCleanupError("diagnostic directory is outside the owned filesystem")
        return descriptor
    except Exception:
        os.close(descriptor)
        raise


def _read_diagnostic_bytes(run_root, relative, run_device, limit):
    parts = relative.split("/")
    with native_log_read_scope():
        directory = _diagnostic_directory_fd(run_root, parts[:-1], run_device)
        descriptor = None
        try:
            descriptor = os.open(parts[-1], os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) |
                                 getattr(os, "O_NONBLOCK", 0), dir_fd=directory)
            before = os.fstat(descriptor)
            if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or
                    before.st_dev != run_device or before.st_size > limit):
                raise UnsafeCleanupError("diagnostic metadata is unsafe or exceeds %s bytes: %s" % (limit, relative))
            with os.fdopen(descriptor, "rb") as stream:
                descriptor = None
                data = stream.read(limit + 1)
                after = os.fstat(stream.fileno())
            current = os.stat(parts[-1], dir_fd=directory, follow_symlinks=False)
            if (len(data) > limit or not os.path.samestat(before, current) or
                    (before.st_size, before.st_mtime_ns, before.st_ctime_ns) !=
                    (after.st_size, after.st_mtime_ns, after.st_ctime_ns)):
                raise UnsafeCleanupError("diagnostic metadata changed while reading: %s" % relative)
            return data
        finally:
            if descriptor is not None:
                os.close(descriptor)
            os.close(directory)


def _diagnostic_environment_run(run_root, run_device):
    # The marker only preserves small metadata; it grants no cleanup ownership
    # or process proof. Match the runner's bounded diagnostic marker contract.
    for name in (_contract_text("spec.json"), _contract_text("status.json")):
        if not os.path.lexists(os.path.join(run_root, name)):
            continue
        try:
            document = json.loads(_read_diagnostic_bytes(
                run_root, name, run_device, _DIAGNOSTIC_IDENTITY_LIMIT).decode("utf-8"))
        except (ValueError, UnicodeError):
            continue
        marker = document.get("environment_test") if isinstance(document, dict) else None
        if not isinstance(marker, dict):
            continue
        test_id = marker.get("test_id")
        if (set(marker) == {"schema_version", "test_id"} and
                marker.get("schema_version") == _contract_text("crescendo.environment_test.run.v1") and
                isinstance(test_id, str) and test_id.strip() and len(test_id) <= 128 and
                not any(ord(char) < 32 for char in test_id)):
            return True
        return False
    return False


def _preserve_diagnostic_identity(payload_root, run_root, run_device, manifest):
    """Keep two producer JSON contracts before diagnostic-owned raw cleanup."""
    if not _diagnostic_environment_run(run_root, run_device):
        return []
    source_relative = _validated_cleanup_source_relative(
        manifest["cleanup_kind"], manifest["source_relative"])
    payload_relative = os.path.relpath(payload_root, run_root).replace(os.sep, "/")
    preserved = []
    for relative in _DIAGNOSTIC_IDENTITY_PATHS:
        source = os.path.join(payload_root, *relative.split("/"))
        if not os.path.lexists(source):
            continue
        data = _read_diagnostic_bytes(run_root, payload_relative + "/" + relative,
                                      run_device, _DIAGNOSTIC_IDENTITY_LIMIT)
        digest = hashlib.sha256(data).hexdigest()
        destination = "diagnostics/%s/%s/%s" % (source_relative, digest, relative)
        parts = destination.split("/")
        directory = _diagnostic_directory_fd(run_root, parts[:-1], run_device, create=True)
        temporary = ".identity-%s.tmp" % os.urandom(12).hex()
        descriptor = None
        try:
            try:
                descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                                     getattr(os, "O_NOFOLLOW", 0), 0o600, dir_fd=directory)
                with os.fdopen(descriptor, "wb") as stream:
                    descriptor = None
                    stream.write(data)
                    stream.flush()
                    os.fsync(stream.fileno())
                try:
                    # Exclusive publication never replaces an existing snapshot.
                    os.link(temporary, parts[-1], src_dir_fd=directory,
                            dst_dir_fd=directory, follow_symlinks=False)
                except OSError as error:
                    if error.errno != errno.EEXIST:
                        raise
            finally:
                if descriptor is not None:
                    os.close(descriptor)
                try:
                    os.unlink(temporary, dir_fd=directory)
                except OSError as error:
                    if error.errno != errno.ENOENT:
                        raise
            os.fsync(directory)
        finally:
            os.close(directory)
        saved = _read_diagnostic_bytes(run_root, destination, run_device, _DIAGNOSTIC_IDENTITY_LIMIT)
        if saved != data:
            raise UnsafeCleanupError("diagnostic snapshot differs; owned payload retained: %s" % destination)
        preserved.append({"source_relative": source_relative + "/" + relative,
                          "path": destination, "sha256": digest, "size_bytes": len(data)})
    return preserved


def _delete_proven_payload(payload_root, run_root, run_device, manifest):
    """Only retry an owned quarantine after its termination guards have passed."""
    def fail_with_full_path(_function, path, exception_info):
        error = exception_info[1]
        if isinstance(error, OSError):
            error.filename = os.path.abspath(path)
        raise error

    _validated_pending_payload(os.path.dirname(payload_root), run_device)
    _reject_mounts_or_cross_device_tree(payload_root, run_device, "proven-dead cleanup payload")
    try:
        preserved = _preserve_diagnostic_identity(payload_root, run_root, run_device, manifest)
        if preserved:
            manifest = dict(manifest)
            manifest["diagnostic_identity_files"] = preserved
            _write_pending_manifest(os.path.dirname(payload_root), run_root, run_device, manifest)
    except (OSError, UnsafeCleanupError) as error:
        detail = _artifact_deletion_detail(payload_root, error, [])
        detail.update(attempt_count=0, retryable=True,
                      diagnostic_preservation="failed_before_artifact_deletion",
                      retry_hint="Diagnostic identity metadata was not safely preserved; owned artifacts retained. Correct the reported path and retry cleanup.")
        result = dict(manifest)
        result["artifact_cleanup"] = detail
        try:
            _write_pending_manifest(os.path.dirname(payload_root), run_root, run_device, result)
        except Exception as diagnostic_error:
            detail["diagnostic_write_error"] = "%s: %s" % (diagnostic_error.__class__.__name__, diagnostic_error)
        raise ArtifactCleanupError(error, detail)

    attempts = []
    for attempt in range(1, 4):
        _validated_pending_payload(os.path.dirname(payload_root), run_device)
        _reject_mounts_or_cross_device_tree(payload_root, run_device, "proven-dead cleanup payload")
        try:
            shutil.rmtree(payload_root, onerror=fail_with_full_path)
            result = dict(manifest)
            if attempts:
                result["artifact_cleanup"] = {
                    "artifact_cleanup_complete": True, "attempts": attempts,
                    "attempt_count": attempt,
                }
            return result
        except OSError as error:
            attempts.append({"attempt": attempt, "errno": getattr(error, "errno", None),
                             "exception": "%s: %s" % (error.__class__.__name__, error)})
            detail = _artifact_deletion_detail(payload_root, error, attempts)
            detail["attempt_count"] = attempt
            terminal_attempt = (getattr(error, "errno", None) not in
                                (errno.ENOTEMPTY, errno.EBUSY) or attempt == 3)
            if terminal_attempt:
                detail["filesystem"] = _cleanup_filesystem(payload_root)
                detail["open_handles"] = _cleanup_open_handles(payload_root)
                detail["retryable"] = getattr(error, "errno", None) in (errno.ENOTEMPTY, errno.EBUSY)
                detail["retry_hint"] = (
                    "Retained owned artifacts; close listed readers and retry Exit. "
                    "No unrelated process was signalled."
                )
            result = dict(manifest)
            result["artifact_cleanup"] = detail
            try:
                _write_pending_manifest(os.path.dirname(payload_root), run_root, run_device, result)
            except Exception as diagnostic_error:
                detail["diagnostic_write_error"] = "%s: %s" % (
                    diagnostic_error.__class__.__name__, diagnostic_error)
            if terminal_attempt:
                raise ArtifactCleanupError(error, detail)
            time.sleep(0.1 * attempt)


_PARTIAL_WORKER_EPOCH_PROOFS = frozenset((
    "worker_not_created", "ocean_not_spawned",
    "owned_processes_absent_after_guardian_loss", "guardian_cleanup_proven",
    "prior_boot_processes_absent",
))
_CONTROLLER_EPOCH_LIVENESS_PROOFS = frozenset((
    "controller_not_started",
    "recorded_pid_is_dead",
    "recorded_pid_was_reused",
    "prior_boot_processes_dead",
    "controller_termination_proven",
))
_CONTROLLER_LIVENESS_LOCK = "controller_liveness.lock"
_CLEANUP_PENDING_DIRECTORY = ".cleanup_pending"
_CLEANUP_PENDING_SCHEMA = _contract_text("crescendo.cleanup_pending.v1")
_CLEANUP_PENDING_STATES = frozenset(("prepared", "quarantined", "deleted"))
_CLEANUP_KINDS = frozenset((
    "controller_epoch", "worker_epoch", "nonterminal_candidate",
    "terminal_candidate_artifacts",
))
_TERMINAL_CANDIDATE_STATES = frozenset((
    "completed", "failed", "infrastructure_failed",
))
_RETAINED_SPECTRE_PREFIX = "retained_spectre_"
_RETAINED_SPECTRE_SUFFIXES = (".log", ".meta")


def _numbered_component(name, prefix):
    suffix = name[len(prefix):] if name.startswith(prefix) else ""
    return bool(suffix and all(character in "0123456789" for character in suffix))


def _canonical_numbered_component(name, prefix, minimum_width):
    if not isinstance(name, str) or not _numbered_component(name, prefix):
        return None
    try:
        value = int(name[len(prefix):])
    except (TypeError, ValueError):
        return None
    if name != prefix + ("%0*d" % (int(minimum_width), value)):
        return None
    return value


def parse_worker_name(name):
    return _canonical_numbered_component(name, "worker_", 3)


def parse_worker_epoch_component(name):
    local_epoch = _canonical_numbered_component(name, "epoch_", 3)
    if local_epoch is not None:
        return {
            "epoch_name": name,
            "controller_epoch": 0,
            "local_epoch": local_epoch,
            "namespaced": False,
        }
    if not isinstance(name, str) or not name.startswith("epoch_c") or "_l" not in name:
        return None
    controller_text, local_text = name[len("epoch_c"):].split("_l", 1)
    if not (
        controller_text and local_text
        and all(character in "0123456789" for character in controller_text)
        and all(character in "0123456789" for character in local_text)
    ):
        return None
    try:
        controller_epoch = int(controller_text)
        local_epoch = int(local_text)
    except (TypeError, ValueError):
        return None
    if controller_epoch <= 0 or name != "epoch_c%06d_l%06d" % (
        controller_epoch, local_epoch,
    ):
        return None
    return {
        "epoch_name": name,
        "controller_epoch": controller_epoch,
        "local_epoch": local_epoch,
        "namespaced": True,
    }


def parse_worker_epoch_namespace(worker_name, epoch_name):
    worker_index = parse_worker_name(worker_name)
    epoch = parse_worker_epoch_component(epoch_name)
    if worker_index is None or epoch is None:
        return None
    result = dict(epoch)
    result["worker_name"] = worker_name
    result["worker_index"] = worker_index
    if epoch["namespaced"]:
        result["epoch_identity"] = "worker-%03d-controller-%06d-epoch-%06d" % (
            worker_index, epoch["controller_epoch"], epoch["local_epoch"],
        )
    else:
        result["epoch_identity"] = "worker-%03d-epoch-%03d" % (
            worker_index, epoch["local_epoch"],
        )
    return result


def _fsync_directory(path):
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _reject_mounts_or_cross_device_tree(root, expected_device, description):
    def walk_error(error):
        raise UnsafeCleanupError("%s cannot be inspected safely: %s" % (description, error))

    for directory, directory_names, file_names in os.walk(
        root, topdown=True, onerror=walk_error, followlinks=False,
    ):
        for name in directory_names + file_names:
            path = os.path.join(directory, name)
            try:
                path_stat = os.lstat(path)
            except OSError as error:
                raise UnsafeCleanupError(
                    "%s changed during validation: %s" % (description, error)
                )
            if stat.S_ISLNK(path_stat.st_mode):
                continue
            if os.path.ismount(path) or path_stat.st_dev != expected_device:
                raise UnsafeCleanupError(
                    "%s contains a mount or filesystem boundary: %s"
                    % (description, path)
                )


def _validated_cleanup_source_relative(cleanup_kind, source_relative):
    if cleanup_kind not in _CLEANUP_KINDS:
        raise UnsafeCleanupError("cleanup manifest kind is not recognized: %r" % cleanup_kind)
    if not isinstance(source_relative, str) or not source_relative:
        raise UnsafeCleanupError("cleanup manifest source path is missing or malformed")
    if os.path.isabs(source_relative) or "\\" in source_relative:
        raise UnsafeCleanupError("cleanup manifest source path must be run-relative")
    normalized = os.path.normpath(source_relative).replace(os.sep, "/")
    if normalized != source_relative or normalized in (".", ".."):
        raise UnsafeCleanupError("cleanup manifest source path is not canonical")
    parts = source_relative.split("/")
    if cleanup_kind == "controller_epoch":
        valid = (
            len(parts) == 3 and parts[0] == "controllers" and
            _numbered_component(parts[1], "slot_") and
            _numbered_component(parts[2], "epoch_")
        )
    elif cleanup_kind == "worker_epoch":
        valid = (
            len(parts) == 3 and parts[0] == "workers" and
            parse_worker_epoch_namespace(parts[1], parts[2]) is not None
        )
    elif cleanup_kind in ("nonterminal_candidate", "terminal_candidate_artifacts"):
        valid = len(parts) == 2 and parts[0] == "candidates" and parts[1] not in ("", ".", "..")
    else:
        valid = False
    if not valid:
        raise UnsafeCleanupError(
            "cleanup manifest source path does not match its strict namespace: %s"
            % source_relative
        )
    return source_relative


def _cleanup_pending_entry_name(cleanup_kind, source_relative):
    source_relative = _validated_cleanup_source_relative(cleanup_kind, source_relative)
    digest = hashlib.sha256(
        (cleanup_kind + "\x00" + source_relative).encode("utf-8")
    ).hexdigest()
    return "cleanup-%s-%s" % (cleanup_kind.replace("_", "-"), digest)


def _cleanup_manifest(cleanup_kind, source_relative, run_root, proof_kind, evidence):
    source_relative = _validated_cleanup_source_relative(cleanup_kind, source_relative)
    return {
        "schema_version": _CLEANUP_PENDING_SCHEMA,
        "cleanup_kind": cleanup_kind,
        "entry_name": _cleanup_pending_entry_name(cleanup_kind, source_relative),
        "source_relative": source_relative,
        "run_root": os.path.abspath(run_root),
        "proof_kind": proof_kind,
        "evidence": dict(evidence or {}),
        "state": "prepared",
        "created_at": utc_timestamp(),
    }


def _validate_cleanup_manifest(document, run_root, entry_name):
    if not isinstance(document, dict) or document.get("schema_version") != _CLEANUP_PENDING_SCHEMA:
        raise UnsafeCleanupError("cleanup pending manifest schema is not recognized")
    cleanup_kind = document.get("cleanup_kind")
    source_relative = _validated_cleanup_source_relative(
        cleanup_kind, document.get("source_relative"),
    )
    expected_entry = _cleanup_pending_entry_name(cleanup_kind, source_relative)
    if entry_name != expected_entry or document.get("entry_name") != expected_entry:
        raise UnsafeCleanupError("cleanup pending manifest identity does not match its directory")
    if document.get("run_root") != os.path.abspath(run_root):
        raise UnsafeCleanupError("cleanup pending manifest run root does not match")
    if document.get("state") not in _CLEANUP_PENDING_STATES:
        raise UnsafeCleanupError("cleanup pending manifest state is not recognized")
    proof_kind = document.get("proof_kind")
    if proof_kind is not None and (not isinstance(proof_kind, str) or not proof_kind):
        raise UnsafeCleanupError("cleanup pending manifest proof kind is malformed")
    if not isinstance(document.get("evidence"), dict):
        raise UnsafeCleanupError("cleanup pending manifest evidence is malformed")
    return document


def _validated_cleanup_root(run_root):
    run_root = os.path.abspath(run_root)
    if os.path.islink(run_root) or not os.path.isdir(run_root):
        raise UnsafeCleanupError("cleanup run root is missing, unsafe, or a symbolic link: %s" % run_root)
    try:
        run_device = os.lstat(run_root).st_dev
    except OSError as error:
        raise UnsafeCleanupError("cleanup run root cannot be inspected: %s" % error)
    return run_root, run_device


def _validated_pending_directory(run_root, run_device, create=False):
    pending_root = os.path.join(run_root, _CLEANUP_PENDING_DIRECTORY)
    if not os.path.lexists(pending_root):
        if not create:
            return None
        os.mkdir(pending_root, 0o700)
        _fsync_directory(run_root)
    try:
        pending_stat = os.lstat(pending_root)
    except OSError as error:
        raise UnsafeCleanupError("cleanup pending directory cannot be inspected: %s" % error)
    if (
        stat.S_ISLNK(pending_stat.st_mode) or not stat.S_ISDIR(pending_stat.st_mode) or
        os.path.ismount(pending_root) or pending_stat.st_dev != run_device or
        os.path.dirname(pending_root) != run_root
    ):
        raise UnsafeCleanupError("cleanup pending directory is unsafe: %s" % pending_root)
    return pending_root


def _validated_pending_entry(pending_root, entry_name, run_device, create=False):
    entry_root = os.path.join(pending_root, entry_name)
    if not os.path.lexists(entry_root):
        if not create:
            raise UnsafeCleanupError("cleanup pending entry is missing: %s" % entry_name)
        os.mkdir(entry_root, 0o700)
        _fsync_directory(pending_root)
    try:
        entry_stat = os.lstat(entry_root)
    except OSError as error:
        raise UnsafeCleanupError("cleanup pending entry cannot be inspected: %s" % error)
    if (
        stat.S_ISLNK(entry_stat.st_mode) or not stat.S_ISDIR(entry_stat.st_mode) or
        os.path.ismount(entry_root) or entry_stat.st_dev != run_device or
        os.path.dirname(entry_root) != pending_root
    ):
        raise UnsafeCleanupError("cleanup pending entry is unsafe: %s" % entry_root)
    return entry_root


def _is_pending_manifest_tmp(name):
    prefix = "manifest.json."
    suffix = ".tmp"
    return name.startswith(prefix) and name.endswith(suffix) and len(name) > len(prefix) + len(suffix)


def _cleanup_pending_manifest_temps(entry_root, run_device):
    removed = []
    for name in sorted(os.listdir(entry_root)):
        if not _is_pending_manifest_tmp(name):
            continue
        path = os.path.join(entry_root, name)
        try:
            path_stat = os.lstat(path)
        except OSError as error:
            raise UnsafeCleanupError(
                "cleanup pending manifest temp cannot be inspected: %s" % error
            )
        if (
            stat.S_ISLNK(path_stat.st_mode) or not stat.S_ISREG(path_stat.st_mode) or
            os.path.ismount(path) or path_stat.st_dev != run_device or
            os.path.dirname(path) != entry_root
        ):
            raise UnsafeCleanupError(
                "cleanup pending manifest temp is unsafe: %s" % path
            )
        os.unlink(path)
        removed.append(name)
    if removed:
        _fsync_directory(entry_root)
    return removed


def _load_pending_manifest(entry_root, run_root, run_device):
    manifest_path = os.path.join(entry_root, _contract_text("manifest.json"))
    try:
        manifest_stat = os.lstat(manifest_path)
    except OSError as error:
        raise UnsafeCleanupError("cleanup pending manifest is missing: %s" % error)
    if (
        stat.S_ISLNK(manifest_stat.st_mode) or not stat.S_ISREG(manifest_stat.st_mode) or
        manifest_stat.st_size <= 0 or manifest_stat.st_dev != run_device
    ):
        raise UnsafeCleanupError("cleanup pending manifest is unsafe: %s" % manifest_path)
    try:
        document = load_json(manifest_path)
    except Exception as error:
        raise UnsafeCleanupError("cleanup pending manifest cannot be read: %s" % error)
    return _validate_cleanup_manifest(document, run_root, os.path.basename(entry_root))


def _write_pending_manifest(entry_root, run_root, run_device, document):
    _cleanup_pending_manifest_temps(entry_root, run_device)
    manifest_path = os.path.join(entry_root, _contract_text("manifest.json"))
    if os.path.lexists(manifest_path) and (
        os.path.islink(manifest_path) or not os.path.isfile(manifest_path)
    ):
        raise UnsafeCleanupError("cleanup pending manifest target is unsafe: %s" % manifest_path)
    atomic_write_json(manifest_path, document)
    return _load_pending_manifest(entry_root, run_root, run_device)


def _same_cleanup_identity(left, right):
    keys = (
        "schema_version", "cleanup_kind", "entry_name", "source_relative",
        "run_root", "proof_kind", "evidence",
    )
    return all(left.get(key) == right.get(key) for key in keys)


def _validated_pending_payload(entry_root, run_device):
    payload_root = os.path.join(entry_root, "payload")
    try:
        payload_stat = os.lstat(payload_root)
    except OSError as error:
        raise UnsafeCleanupError("cleanup pending payload cannot be inspected: %s" % error)
    if (
        stat.S_ISLNK(payload_stat.st_mode) or not stat.S_ISDIR(payload_stat.st_mode) or
        os.path.ismount(payload_root) or payload_stat.st_dev != run_device or
        os.path.dirname(payload_root) != entry_root
    ):
        raise UnsafeCleanupError("cleanup pending payload is unsafe: %s" % payload_root)
    return payload_root


def _finish_pending_entry(run_root, pending_root, entry_root):
    payload_root = os.path.join(entry_root, "payload")
    if os.path.lexists(payload_root):
        raise UnsafeCleanupError("cleanup pending payload still exists after deletion")
    manifest_path = os.path.join(entry_root, _contract_text("manifest.json"))
    if os.path.lexists(manifest_path):
        if os.path.islink(manifest_path) or not os.path.isfile(manifest_path):
            raise UnsafeCleanupError("cleanup pending manifest became unsafe")
        os.unlink(manifest_path)
        _fsync_directory(entry_root)
    remaining = os.listdir(entry_root)
    if remaining:
        raise UnsafeCleanupError("cleanup pending entry contains unexpected files: %s" % remaining)
    os.rmdir(entry_root)
    _fsync_directory(pending_root)
    try:
        os.rmdir(pending_root)
        _fsync_directory(run_root)
    except OSError as error:
        if error.errno not in (errno.ENOENT, errno.ENOTEMPTY):
            raise


@_serialize_cleanup(1)
def _quarantine_and_delete(source_root, run_root, cleanup_kind, source_relative,
                           proof_kind=None, evidence=None):
    run_root, run_device = _validated_cleanup_root(run_root)
    source_relative = _validated_cleanup_source_relative(cleanup_kind, source_relative)
    expected_source = os.path.abspath(os.path.join(run_root, *source_relative.split("/")))
    source_root = os.path.abspath(source_root)
    if source_root != expected_source:
        raise UnsafeCleanupError("cleanup source does not match its manifest identity")
    try:
        source_stat = os.lstat(source_root)
    except OSError as error:
        raise UnsafeCleanupError("cleanup source disappeared before quarantine: %s" % error)
    if (
        stat.S_ISLNK(source_stat.st_mode) or not stat.S_ISDIR(source_stat.st_mode) or
        os.path.ismount(source_root) or source_stat.st_dev != run_device
    ):
        raise UnsafeCleanupError("cleanup source is unsafe before quarantine: %s" % source_root)
    _reject_mounts_or_cross_device_tree(
        source_root, run_device, "%s cleanup source" % cleanup_kind,
    )

    expected_manifest = _cleanup_manifest(
        cleanup_kind, source_relative, run_root, proof_kind, evidence,
    )
    pending_root = _validated_pending_directory(run_root, run_device, create=True)
    entry_root = _validated_pending_entry(
        pending_root, expected_manifest["entry_name"], run_device, create=True,
    )
    _cleanup_pending_manifest_temps(entry_root, run_device)
    manifest_path = os.path.join(entry_root, _contract_text("manifest.json"))
    payload_root = os.path.join(entry_root, "payload")
    unexpected = set(os.listdir(entry_root)) - {_contract_text("manifest.json"), "payload"}
    if unexpected:
        raise UnsafeCleanupError("cleanup pending entry contains unexpected files: %s" % sorted(unexpected))
    if os.path.lexists(manifest_path):
        manifest = _load_pending_manifest(entry_root, run_root, run_device)
        if not _same_cleanup_identity(manifest, expected_manifest):
            raise UnsafeCleanupError("cleanup pending entry conflicts with the current proof")
    else:
        if os.path.lexists(payload_root):
            raise UnsafeCleanupError("cleanup pending payload exists without its manifest")
        manifest = _write_pending_manifest(
            entry_root, run_root, run_device, expected_manifest,
        )
    if os.path.lexists(payload_root):
        raise UnsafeCleanupError("cleanup source and pending payload both exist")

    os.rename(source_root, payload_root)
    _fsync_directory(os.path.dirname(source_root))
    _fsync_directory(entry_root)
    manifest = dict(manifest)
    manifest.update({"state": "quarantined", "quarantined_at": utc_timestamp()})
    manifest = _write_pending_manifest(entry_root, run_root, run_device, manifest)
    payload_root = _validated_pending_payload(entry_root, run_device)
    _reject_mounts_or_cross_device_tree(
        payload_root, run_device, "%s cleanup payload" % cleanup_kind,
    )
    manifest = _delete_proven_payload(payload_root, run_root, run_device, manifest)
    manifest = dict(manifest)
    manifest.update({"state": "deleted", "deleted_at": utc_timestamp()})
    _write_pending_manifest(entry_root, run_root, run_device, manifest)
    _finish_pending_entry(run_root, pending_root, entry_root)
    return manifest


def _validated_cleanup_item_names(values, field):
    if not isinstance(values, list):
        raise UnsafeCleanupError("terminal cleanup manifest %s is malformed" % field)
    result = []
    seen = set()
    for value in values:
        if (
            not isinstance(value, str) or not value or value in (".", "..") or
            os.path.basename(value) != value or value in seen
        ):
            raise UnsafeCleanupError("terminal cleanup manifest %s contains an unsafe name" % field)
        seen.add(value)
        result.append(value)
    return result


def _validated_terminal_cleanup_evidence(manifest):
    evidence = manifest.get("evidence") or {}
    preserved = _validated_cleanup_item_names(evidence.get("preserved"), "preserved")
    disposable = _validated_cleanup_item_names(evidence.get("disposable"), "disposable")
    protected = {_contract_text("candidate.json"), _contract_text("timing.json"), _contract_text("completion_evidence.json")}
    if _contract_text("candidate.json") not in preserved:
        raise UnsafeCleanupError("terminal cleanup manifest does not preserve candidate.json")
    if set(preserved).intersection(disposable) or protected.intersection(disposable):
        raise UnsafeCleanupError("terminal cleanup manifest would delete lightweight recovery evidence")
    candidate_id = manifest["source_relative"].split("/")[-1]
    if evidence.get("candidate_id") != candidate_id:
        raise UnsafeCleanupError("terminal cleanup manifest candidate identity does not match")
    proof_kind = manifest.get("proof_kind")
    if proof_kind == "source_integrity_verified":
        if evidence.get("source_verified") is not True:
            raise UnsafeCleanupError("terminal cleanup manifest lacks persisted source verification")
    elif proof_kind == "controller_termination_proven":
        termination = evidence.get("controller_termination")
        if evidence.get("source_verified") is not False:
            raise UnsafeCleanupError(
                "controller-termination cleanup must not claim source verification"
            )
        if not isinstance(termination, dict) or (
            termination.get("termination_proven") is not True or
            termination.get("process_still_alive") is not False or
            termination.get("cleanup_ok") is not True or
            termination.get("candidate_status") != "infrastructure_failed"
        ):
            raise UnsafeCleanupError(
                "terminal cleanup manifest lacks proven controller termination"
            )
        digest = termination.get("proof_sha256")
        if (
            not isinstance(digest, str) or len(digest) != 64 or
            any(character not in "0123456789abcdef" for character in digest)
        ):
            raise UnsafeCleanupError(
                "terminal cleanup controller-termination digest is malformed"
            )
    else:
        raise UnsafeCleanupError("terminal cleanup proof kind is not recognized")
    workspace_removed = evidence.get("workspace_removed")
    if not isinstance(workspace_removed, bool) or workspace_removed != ("workspace" in disposable):
        raise UnsafeCleanupError("terminal cleanup manifest workspace state is inconsistent")
    workspace_relative = evidence.get("workspace_relative")
    expected_workspace = manifest["source_relative"] + "/workspace" if workspace_removed else None
    if workspace_relative != expected_workspace:
        raise UnsafeCleanupError("terminal cleanup manifest workspace path is inconsistent")
    if proof_kind == "controller_termination_proven" and (
        workspace_removed or workspace_relative is not None or "workspace" in disposable
    ):
        raise UnsafeCleanupError(
            "controller-termination cleanup cannot reclaim a candidate workspace"
        )
    raw_removed = evidence.get("raw_artifacts_removed")
    if not isinstance(raw_removed, list):
        raise UnsafeCleanupError("terminal cleanup manifest raw artifact summary is malformed")
    for display_name in raw_removed:
        if not isinstance(display_name, str) or not display_name:
            raise UnsafeCleanupError("terminal cleanup manifest raw artifact summary is malformed")
        name = display_name[:-1] if display_name.endswith("/") else display_name
        if name not in disposable or name == "workspace":
            raise UnsafeCleanupError("terminal cleanup manifest raw artifact summary is inconsistent")
    return evidence, preserved, disposable


def _controller_termination_evidence(candidate):
    if not isinstance(candidate, dict) or candidate.get("status") != "infrastructure_failed":
        raise UnsafeCleanupError(
            "controller-termination cleanup requires an infrastructure-failed candidate"
        )
    persistent = candidate.get("persistent_worker")
    termination = persistent.get("controller_termination") if isinstance(persistent, dict) else None
    cleanup = termination.get("cleanup") if isinstance(termination, dict) else None
    if (
        not isinstance(termination, dict) or
        termination.get("termination_proven") is not True or
        termination.get("process_still_alive") is not False or
        not isinstance(cleanup, dict) or cleanup.get("ok") is not True
    ):
        raise UnsafeCleanupError(
            "candidate lacks a successful persisted controller-termination proof"
        )
    try:
        serialized = json.dumps(
            termination, sort_keys=True, separators=(",", ":"), allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise UnsafeCleanupError(
            "candidate controller-termination proof is not canonical JSON: %s" % error
        )
    return {
        "candidate_status": "infrastructure_failed",
        "failure_class": candidate.get("failure_class"),
        "termination_proven": True,
        "process_still_alive": False,
        "cleanup_ok": True,
        "proof_sha256": hashlib.sha256(serialized).hexdigest(),
    }


def _validate_controller_termination_candidate(candidate_root, evidence):
    if os.path.lexists(os.path.join(candidate_root, "workspace")):
        raise UnsafeCleanupError(
            "controller-termination cleanup refuses any candidate workspace"
        )
    candidate_path = os.path.join(candidate_root, _contract_text("candidate.json"))
    try:
        candidate = load_json(candidate_path)
    except Exception as error:
        raise UnsafeCleanupError(
            "controller-termination candidate record cannot be read: %s" % error
        )
    if candidate.get("candidate_id") != os.path.basename(candidate_root):
        raise UnsafeCleanupError(
            "controller-termination candidate identity does not match its directory"
        )
    current = _controller_termination_evidence(candidate)
    if current != evidence.get("controller_termination"):
        raise UnsafeCleanupError(
            "persisted controller-termination proof changed during cleanup"
        )


def _validate_terminal_preserved_files(candidate_root, preserved):
    for name in preserved:
        path = os.path.join(candidate_root, name)
        if not _regular_nonempty(path, candidate_root) or os.path.dirname(path) != candidate_root:
            raise UnsafeCleanupError(
                "terminal cleanup preserved artifact is missing or unsafe: %s" % path
            )
    for name in (_contract_text("candidate.json"), _contract_text("timing.json"), _contract_text("completion_evidence.json")):
        path = os.path.join(candidate_root, name)
        if os.path.lexists(path) and name not in preserved:
            raise UnsafeCleanupError(
                "terminal cleanup manifest omitted lightweight recovery artifact: %s" % path
            )


def _terminal_candidate_root_from_manifest(manifest, run_root, run_device):
    candidate_root = os.path.abspath(os.path.join(
        run_root, *manifest["source_relative"].split("/")
    ))
    candidate_root, _run_root, _candidates_root, candidate_device = (
        _validated_direct_candidate_paths(candidate_root, run_root)
    )
    if candidate_device != run_device:
        raise UnsafeCleanupError("terminal cleanup candidate crosses a filesystem boundary")
    return candidate_root


def _ensure_pending_payload(entry_root, run_device):
    payload_root = os.path.join(entry_root, "payload")
    if not os.path.lexists(payload_root):
        os.mkdir(payload_root, 0o700)
        _fsync_directory(entry_root)
    return _validated_pending_payload(entry_root, run_device)


def _validate_disposable_item(path, expected_device):
    try:
        path_stat = os.lstat(path)
    except OSError as error:
        raise UnsafeCleanupError("terminal cleanup item cannot be inspected: %s" % error)
    if os.path.ismount(path) or path_stat.st_dev != expected_device:
        raise UnsafeCleanupError(
            "terminal cleanup item crosses a mount or filesystem boundary: %s" % path
        )
    if stat.S_ISDIR(path_stat.st_mode):
        _reject_mounts_or_cross_device_tree(
            path, expected_device, "terminal cleanup item",
        )


def _move_terminal_cleanup_items(candidate_root, payload_root, disposable, run_device):
    unexpected = set(os.listdir(payload_root)) - set(disposable)
    if unexpected:
        raise UnsafeCleanupError(
            "terminal cleanup payload contains unexpected files: %s" % sorted(unexpected)
        )
    for name in disposable:
        source = os.path.join(candidate_root, name)
        target = os.path.join(payload_root, name)
        source_exists = os.path.lexists(source)
        target_exists = os.path.lexists(target)
        if source_exists and target_exists:
            raise UnsafeCleanupError("terminal cleanup item exists in source and quarantine: %s" % name)
        if source_exists:
            _validate_disposable_item(source, run_device)
            os.rename(source, target)
            _fsync_directory(candidate_root)
            _fsync_directory(payload_root)
            target_exists = True
        if not target_exists:
            raise UnsafeCleanupError("terminal cleanup item disappeared before quarantine: %s" % name)
        _validate_disposable_item(target, run_device)


def _resume_terminal_cleanup_entry(run_root, run_device, pending_root, entry_root, manifest):
    evidence, preserved, disposable = _validated_terminal_cleanup_evidence(manifest)
    candidate_root = _terminal_candidate_root_from_manifest(manifest, run_root, run_device)
    _validate_terminal_preserved_files(candidate_root, preserved)
    if manifest.get("proof_kind") == "controller_termination_proven":
        _validate_controller_termination_candidate(candidate_root, evidence)
        unexpected = (
            set(os.listdir(candidate_root)) - set(preserved) - set(disposable)
        )
        if unexpected:
            raise UnsafeCleanupError(
                "controller-termination candidate contains unmanifested artifacts: %s"
                % sorted(unexpected)
            )
    payload_root = os.path.join(entry_root, "payload")
    payload_exists = os.path.lexists(payload_root)
    if manifest["state"] == "prepared":
        payload_root = _ensure_pending_payload(entry_root, run_device)
        _move_terminal_cleanup_items(
            candidate_root, payload_root, disposable, run_device,
        )
        _validate_terminal_preserved_files(candidate_root, preserved)
        manifest = dict(manifest)
        manifest.update({"state": "quarantined", "quarantined_at": utc_timestamp()})
        manifest = _write_pending_manifest(entry_root, run_root, run_device, manifest)
        payload_exists = True
    elif manifest["state"] == "quarantined":
        for name in disposable:
            if os.path.lexists(os.path.join(candidate_root, name)):
                raise UnsafeCleanupError(
                    "quarantined terminal cleanup item reappeared in candidate: %s" % name
                )
        if payload_exists:
            payload_root = _validated_pending_payload(entry_root, run_device)
            unexpected = set(os.listdir(payload_root)) - set(disposable)
            if unexpected:
                raise UnsafeCleanupError(
                    "terminal cleanup payload contains unexpected files: %s" % sorted(unexpected)
                )
    else:
        if payload_exists:
            raise UnsafeCleanupError("deleted terminal cleanup manifest still has a payload")
        for name in disposable:
            if os.path.lexists(os.path.join(candidate_root, name)):
                raise UnsafeCleanupError(
                    "deleted terminal cleanup item reappeared in candidate: %s" % name
                )
    if manifest["state"] != "deleted":
        if payload_exists:
            _reject_mounts_or_cross_device_tree(
                payload_root, run_device, "terminal cleanup payload",
            )
            manifest = _delete_proven_payload(payload_root, run_root, run_device, manifest)
        manifest = dict(manifest)
        manifest.update({"state": "deleted", "deleted_at": utc_timestamp()})
        manifest = _write_pending_manifest(entry_root, run_root, run_device, manifest)
    _validate_terminal_preserved_files(candidate_root, preserved)
    _finish_pending_entry(run_root, pending_root, entry_root)
    return {
        "entry_name": manifest["entry_name"],
        "cleanup_kind": manifest["cleanup_kind"],
        "source_relative": manifest["source_relative"],
        "removed": True,
        "manifest": manifest,
        "evidence": evidence,
    }


@_serialize_cleanup(1)
def _resume_pending_terminal_candidate(candidate_root, run_root):
    run_root, run_device = _validated_cleanup_root(run_root)
    candidate_root = os.path.abspath(candidate_root)
    source_relative = os.path.relpath(candidate_root, run_root).replace(os.sep, "/")
    entry_name = _cleanup_pending_entry_name(
        "terminal_candidate_artifacts", source_relative,
    )
    pending_root = _validated_pending_directory(run_root, run_device, create=False)
    if pending_root is None:
        return None
    entry_root = os.path.join(pending_root, entry_name)
    if not os.path.lexists(entry_root):
        return None
    entry_root = _validated_pending_entry(pending_root, entry_name, run_device)
    _cleanup_pending_manifest_temps(entry_root, run_device)
    names = set(os.listdir(entry_root))
    if _contract_text("manifest.json") not in names:
        if "payload" in names:
            raise UnsafeCleanupError("terminal cleanup payload exists without its manifest")
        if names:
            raise UnsafeCleanupError(
                "terminal cleanup pending entry contains unexpected files: %s" % sorted(names)
            )
        _finish_pending_entry(run_root, pending_root, entry_root)
        return None
    unexpected = names - {_contract_text("manifest.json"), "payload"}
    if unexpected:
        raise UnsafeCleanupError(
            "terminal cleanup pending entry contains unexpected files: %s" % sorted(unexpected)
        )
    manifest = _load_pending_manifest(entry_root, run_root, run_device)
    if manifest["cleanup_kind"] != "terminal_candidate_artifacts":
        raise UnsafeCleanupError("terminal cleanup pending manifest kind does not match")
    return _resume_terminal_cleanup_entry(
        run_root, run_device, pending_root, entry_root, manifest,
    )


@_serialize_cleanup(1)
def _terminal_candidate_cleanup_transaction(candidate_root, run_root, evidence,
                                            proof_kind="source_integrity_verified"):
    run_root, run_device = _validated_cleanup_root(run_root)
    source_relative = os.path.relpath(candidate_root, run_root).replace(os.sep, "/")
    expected_manifest = _cleanup_manifest(
        "terminal_candidate_artifacts", source_relative, run_root,
        proof_kind, evidence,
    )
    pending_root = _validated_pending_directory(run_root, run_device, create=True)
    entry_root = _validated_pending_entry(
        pending_root, expected_manifest["entry_name"], run_device, create=True,
    )
    _cleanup_pending_manifest_temps(entry_root, run_device)
    manifest_path = os.path.join(entry_root, _contract_text("manifest.json"))
    payload_root = os.path.join(entry_root, "payload")
    unexpected = set(os.listdir(entry_root)) - {_contract_text("manifest.json"), "payload"}
    if unexpected:
        raise UnsafeCleanupError(
            "terminal cleanup pending entry contains unexpected files: %s" % sorted(unexpected)
        )
    if os.path.lexists(manifest_path):
        manifest = _load_pending_manifest(entry_root, run_root, run_device)
        if not _same_cleanup_identity(manifest, expected_manifest):
            raise UnsafeCleanupError("terminal cleanup pending entry conflicts with current evidence")
    else:
        if os.path.lexists(payload_root):
            raise UnsafeCleanupError("terminal cleanup payload exists without its manifest")
        manifest = _write_pending_manifest(
            entry_root, run_root, run_device, expected_manifest,
        )
    return _resume_terminal_cleanup_entry(
        run_root, run_device, pending_root, entry_root, manifest,
    )


@_serialize_cleanup(0)
def resume_pending_cleanups(run_root):
    run_root, run_device = _validated_cleanup_root(run_root)
    pending_root = _validated_pending_directory(run_root, run_device, create=False)
    if pending_root is None:
        return []
    resumed = []
    for entry_name in sorted(os.listdir(pending_root)):
        entry_root = _validated_pending_entry(pending_root, entry_name, run_device)
        _cleanup_pending_manifest_temps(entry_root, run_device)
        names = set(os.listdir(entry_root))
        if _contract_text("manifest.json") not in names:
            if names:
                raise UnsafeCleanupError(
                    "cleanup pending entry has no manifest: %s" % entry_name
                )
            os.rmdir(entry_root)
            _fsync_directory(pending_root)
            resumed.append({"entry_name": entry_name, "state": "empty_entry_removed"})
            continue
        unexpected = names - {_contract_text("manifest.json"), "payload"}
        if unexpected:
            raise UnsafeCleanupError(
                "cleanup pending entry contains unexpected files: %s" % sorted(unexpected)
            )
        manifest = _load_pending_manifest(entry_root, run_root, run_device)
        if manifest["cleanup_kind"] == "terminal_candidate_artifacts":
            resumed.append(_resume_terminal_cleanup_entry(
                run_root, run_device, pending_root, entry_root, manifest,
            ))
            pending_root = _validated_pending_directory(run_root, run_device, create=False)
            if pending_root is None:
                break
            continue
        source_root = os.path.abspath(os.path.join(
            run_root, *manifest["source_relative"].split("/")
        ))
        source_exists = os.path.lexists(source_root)
        payload_root = os.path.join(entry_root, "payload")
        payload_exists = os.path.lexists(payload_root)
        if source_exists and payload_exists:
            raise UnsafeCleanupError("cleanup source and pending payload both exist")
        removed = False
        if payload_exists:
            if manifest["state"] == "deleted":
                raise UnsafeCleanupError("deleted cleanup manifest still has a payload")
            payload_root = _validated_pending_payload(entry_root, run_device)
            _reject_mounts_or_cross_device_tree(
                payload_root, run_device,
                "%s cleanup payload" % manifest["cleanup_kind"],
            )
            if manifest["state"] == "prepared":
                manifest = dict(manifest)
                manifest.update({"state": "quarantined", "quarantined_at": utc_timestamp()})
                manifest = _write_pending_manifest(
                    entry_root, run_root, run_device, manifest,
                )
            manifest = _delete_proven_payload(payload_root, run_root, run_device, manifest)
            manifest = dict(manifest)
            manifest.update({"state": "deleted", "deleted_at": utc_timestamp()})
            _write_pending_manifest(entry_root, run_root, run_device, manifest)
            removed = True
        elif manifest["state"] == "prepared" and source_exists:
            removed = False
        elif manifest["state"] == "prepared":
            raise UnsafeCleanupError(
                "prepared cleanup manifest lost both its source and payload"
            )
        elif source_exists:
            raise UnsafeCleanupError(
                "quarantined cleanup manifest unexpectedly has its original source"
            )
        else:
            removed = True
        _finish_pending_entry(run_root, pending_root, entry_root)
        resumed.append({
            "entry_name": entry_name,
            "cleanup_kind": manifest["cleanup_kind"],
            "source_relative": manifest["source_relative"],
            "removed": removed,
            "artifact_cleanup": manifest.get("artifact_cleanup"),
        })
        pending_root = _validated_pending_directory(run_root, run_device, create=False)
        if pending_root is None:
            break
    if pending_root is not None and not os.listdir(pending_root):
        os.rmdir(pending_root)
        _fsync_directory(run_root)
    return resumed


def inspect_run_cleanup_scope(run_root):
    """Read all retained epoch/quarantine namespaces, without retrying deletion.

    An absent original epoch is insufficient: it may have been renamed to its
    owned quarantine. This snapshot is for final status aggregation, not a
    process-termination proof or authorization to remove any resource.
    """
    snapshot = {"schema_version": _contract_text("crescendo.run_cleanup_scope.v1"),
                "run_root": os.path.abspath(run_root), "observed_at": utc_timestamp(),
                "retained_worker_epochs": [], "retained_controller_epochs": [],
                "pending_entries": [], "errors": []}
    try:
        run_root, run_device = _validated_cleanup_root(run_root)
        for namespace, slot_prefix, target in (
                ("workers", "worker_", "retained_worker_epochs"),
                ("controllers", "slot_", "retained_controller_epochs")):
            namespace_root = os.path.join(run_root, namespace)
            if not os.path.lexists(namespace_root):
                continue
            if os.path.islink(namespace_root) or not os.path.isdir(namespace_root):
                raise UnsafeCleanupError("cleanup namespace is unsafe: %s" % namespace_root)
            for slot_name in sorted(os.listdir(namespace_root)):
                slot_root = os.path.join(namespace_root, slot_name)
                if (not _numbered_component(slot_name, slot_prefix)
                        or os.path.islink(slot_root) or not os.path.isdir(slot_root)):
                    snapshot["errors"].append("unexpected retained namespace object: %s" % slot_root)
                    continue
                for epoch_name in sorted(os.listdir(slot_root)):
                    epoch_root = os.path.join(slot_root, epoch_name)
                    canonical = (parse_worker_epoch_component(epoch_name) is not None
                                 if namespace == "workers" else
                                 _numbered_component(epoch_name, "epoch_"))
                    if not canonical or os.path.islink(epoch_root) or not os.path.isdir(epoch_root):
                        snapshot["errors"].append("unexpected retained epoch object: %s" % epoch_root)
                    else:
                        snapshot[target].append(epoch_root)
        pending_root = _validated_pending_directory(run_root, run_device, create=False)
        if pending_root is not None:
            for entry_name in sorted(os.listdir(pending_root)):
                entry_root = _validated_pending_entry(pending_root, entry_name, run_device)
                manifest = _load_pending_manifest(entry_root, run_root, run_device)
                snapshot["pending_entries"].append({
                    "entry_root": entry_root, "source_relative": manifest["source_relative"],
                    "cleanup_kind": manifest["cleanup_kind"], "state": manifest["state"],
                    "payload_exists": os.path.lexists(os.path.join(entry_root, "payload")),
                    "artifact_cleanup": manifest.get("artifact_cleanup"),
                })
            if not snapshot["pending_entries"]:
                snapshot["errors"].append("empty cleanup-pending namespace remains: %s" % pending_root)
    except Exception as error:
        snapshot["errors"].append("%s: %s" % (error.__class__.__name__, error))
    snapshot["artifact_cleanup_complete"] = not bool(
        snapshot["retained_worker_epochs"] or snapshot["retained_controller_epochs"]
        or snapshot["pending_entries"] or snapshot["errors"])
    snapshot["state"] = ("no_owned_artifacts" if snapshot["artifact_cleanup_complete"]
                         else "artifact_cleanup_pending")
    return snapshot


def _validated_worker_epoch_paths(epoch_root, run_root):
    epoch_root = os.path.abspath(epoch_root)
    run_root = os.path.abspath(run_root)
    workers_root = os.path.join(run_root, "workers")
    worker_root = os.path.dirname(epoch_root)
    if os.path.islink(run_root) or not os.path.isdir(run_root):
        raise UnsafeCleanupError(
            "worker run root is missing, not a directory, or a symbolic link: %s" % run_root
        )
    if os.path.islink(workers_root) or not os.path.isdir(workers_root):
        raise UnsafeCleanupError(
            "run workers root is missing, not a directory, or a symbolic link: %s" % workers_root
        )
    namespace = parse_worker_epoch_namespace(
        os.path.basename(worker_root), os.path.basename(epoch_root),
    )
    if os.path.dirname(worker_root) != workers_root or namespace is None:
        raise UnsafeCleanupError(
            "worker cleanup target must be exactly inside its numbered worker epoch namespace: %s"
            % epoch_root
        )
    if os.path.islink(worker_root) or not os.path.isdir(worker_root):
        raise UnsafeCleanupError("worker root is missing, not a directory, or a symbolic link: %s" % worker_root)
    if os.path.islink(epoch_root) or not os.path.isdir(epoch_root):
        raise UnsafeCleanupError("worker epoch is missing, not a directory, or a symbolic link: %s" % epoch_root)
    for description, path in (
        ("workers root", workers_root),
        ("worker root", worker_root),
        ("worker epoch", epoch_root),
    ):
        if os.path.ismount(path):
            raise UnsafeCleanupError("%s is a mount point: %s" % (description, path))
    try:
        run_device = os.lstat(run_root).st_dev
        hierarchy_devices = [
            os.lstat(workers_root).st_dev,
            os.lstat(worker_root).st_dev,
            os.lstat(epoch_root).st_dev,
        ]
    except OSError as error:
        raise UnsafeCleanupError(
            "worker cleanup hierarchy changed during validation: %s" % error
        )
    if any(device != run_device for device in hierarchy_devices):
        raise UnsafeCleanupError(
            "worker cleanup hierarchy crosses a filesystem boundary: %s" % epoch_root
        )
    if (
        os.path.realpath(os.path.dirname(worker_root)) != os.path.realpath(workers_root) or
        os.path.realpath(os.path.dirname(epoch_root)) != os.path.realpath(worker_root) or
        not is_under(epoch_root, workers_root)
    ):
        raise UnsafeCleanupError("worker cleanup target resolves outside its exact run hierarchy: %s" % epoch_root)
    return epoch_root, run_root, workers_root, worker_root


def _validated_controller_epoch_paths(epoch_root, run_root):
    epoch_root = os.path.abspath(epoch_root)
    run_root = os.path.abspath(run_root)
    controllers_root = os.path.join(run_root, "controllers")
    slot_root = os.path.dirname(epoch_root)
    if os.path.islink(run_root) or not os.path.isdir(run_root):
        raise UnsafeCleanupError(
            "controller run root is missing, not a directory, or a symbolic link: %s" % run_root
        )
    if (
        os.path.dirname(slot_root) != controllers_root or
        not _numbered_component(os.path.basename(slot_root), "slot_") or
        not _numbered_component(os.path.basename(epoch_root), "epoch_")
    ):
        raise UnsafeCleanupError(
            "controller cleanup target must be exactly run/controllers/slot_N/epoch_N: %s"
            % epoch_root
        )
    for description, path in (
        ("controllers root", controllers_root),
        ("controller slot", slot_root),
        ("controller epoch", epoch_root),
    ):
        if os.path.islink(path) or not os.path.isdir(path):
            raise UnsafeCleanupError(
                "%s is missing, not a directory, or a symbolic link: %s" % (description, path)
            )
        if os.path.ismount(path):
            raise UnsafeCleanupError("%s is a mount point: %s" % (description, path))
    try:
        run_device = os.lstat(run_root).st_dev
        hierarchy_devices = [
            os.lstat(controllers_root).st_dev,
            os.lstat(slot_root).st_dev,
            os.lstat(epoch_root).st_dev,
        ]
    except OSError as error:
        raise UnsafeCleanupError(
            "controller cleanup hierarchy changed during validation: %s" % error
        )
    if any(device != run_device for device in hierarchy_devices):
        raise UnsafeCleanupError(
            "controller cleanup hierarchy crosses a filesystem boundary: %s" % epoch_root
        )
    if (
        os.path.realpath(os.path.dirname(slot_root)) != os.path.realpath(controllers_root) or
        os.path.realpath(os.path.dirname(epoch_root)) != os.path.realpath(slot_root) or
        not is_under(epoch_root, controllers_root)
    ):
        raise UnsafeCleanupError(
            "controller cleanup target resolves outside its exact run hierarchy: %s" % epoch_root
        )
    return epoch_root, run_root, controllers_root, slot_root, run_device


def _reject_unsafe_controller_epoch_tree(epoch_root, expected_device):
    def walk_error(error):
        raise UnsafeCleanupError(
            "controller epoch cannot be inspected safely: %s" % error
        )

    for directory, directory_names, file_names in os.walk(
        epoch_root, topdown=True, onerror=walk_error, followlinks=False,
    ):
        for name in directory_names + file_names:
            path = os.path.join(directory, name)
            try:
                path_stat = os.lstat(path)
            except OSError as error:
                raise UnsafeCleanupError(
                    "controller epoch changed during validation: %s" % error
                )
            if stat.S_ISLNK(path_stat.st_mode):
                raise UnsafeCleanupError(
                    "controller epoch contains a symbolic link: %s" % path
                )
            if os.path.ismount(path) or path_stat.st_dev != expected_device:
                raise UnsafeCleanupError(
                    "controller epoch contains a mount or filesystem boundary: %s" % path
                )


def _validate_controller_liveness_proof(liveness_proof):
    proof = liveness_proof if isinstance(liveness_proof, dict) else {}
    kind = proof.get("kind")
    if proof.get("ok") is not True or kind not in _CONTROLLER_EPOCH_LIVENESS_PROOFS:
        raise UnsafeCleanupError(
            "controller epoch requires explicit proven-dead liveness proof"
        )
    return kind


def _acquire_released_controller_liveness_lock(epoch_root, expected_device):
    lock_path = os.path.join(epoch_root, _CONTROLLER_LIVENESS_LOCK)
    try:
        lock_stat = os.lstat(lock_path)
    except OSError as error:
        raise UnsafeCleanupError(
            "controller liveness lock is missing or cannot be inspected: %s" % error
        )
    if stat.S_ISLNK(lock_stat.st_mode) or not stat.S_ISREG(lock_stat.st_mode):
        raise UnsafeCleanupError(
            "controller liveness lock is not a regular non-symbolic file: %s" % lock_path
        )
    if os.path.ismount(lock_path) or lock_stat.st_dev != expected_device:
        raise UnsafeCleanupError(
            "controller liveness lock crosses a filesystem boundary: %s" % lock_path
        )
    flags = os.O_RDWR
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = None
    try:
        try:
            descriptor = os.open(lock_path, flags)
        except OSError as error:
            raise UnsafeCleanupError(
                "controller liveness lock cannot be opened safely: %s" % error
            )
        opened_stat = os.fstat(descriptor)
        if not stat.S_ISREG(opened_stat.st_mode) or not os.path.samestat(lock_stat, opened_stat):
            raise UnsafeCleanupError(
                "controller liveness lock changed while being opened: %s" % lock_path
            )
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (IOError, OSError) as error:
            if getattr(error, "errno", None) in (errno.EACCES, errno.EAGAIN):
                raise UnsafeCleanupError(
                    "controller liveness lock is still held: %s" % lock_path
                )
            raise UnsafeCleanupError(
                "controller liveness lock cannot be verified: %s" % error
            )
        try:
            current_stat = os.lstat(lock_path)
        except OSError as error:
            raise UnsafeCleanupError(
                "controller liveness lock disappeared after acquisition: %s" % error
            )
        if not os.path.samestat(current_stat, os.fstat(descriptor)):
            raise UnsafeCleanupError(
                "controller liveness lock changed after acquisition: %s" % lock_path
            )
        return descriptor, lock_path
    except Exception:
        if descriptor is not None:
            os.close(descriptor)
        raise


def _validated_direct_candidate_paths(candidate_root, run_root):
    candidate_root = os.path.abspath(candidate_root)
    run_root = os.path.abspath(run_root)
    candidates_root = os.path.join(run_root, "candidates")
    if os.path.islink(run_root) or not os.path.isdir(run_root):
        raise UnsafeCleanupError(
            "candidate run root is missing, not a directory, or a symbolic link: %s" % run_root
        )
    if os.path.dirname(candidate_root) != candidates_root:
        raise UnsafeCleanupError(
            "candidate cleanup target must be exactly run/candidates/direct-child: %s"
            % candidate_root
        )
    for description, path in (
        ("candidates root", candidates_root),
        ("candidate root", candidate_root),
    ):
        if os.path.islink(path) or not os.path.isdir(path):
            raise UnsafeCleanupError(
                "%s is missing, not a directory, or a symbolic link: %s" % (description, path)
            )
        if os.path.ismount(path):
            raise UnsafeCleanupError("%s is a mount point: %s" % (description, path))
    try:
        run_device = os.lstat(run_root).st_dev
        candidates_device = os.lstat(candidates_root).st_dev
        candidate_device = os.lstat(candidate_root).st_dev
    except OSError as error:
        raise UnsafeCleanupError(
            "candidate cleanup hierarchy changed during validation: %s" % error
        )
    if candidates_device != run_device or candidate_device != run_device:
        raise UnsafeCleanupError(
            "candidate cleanup hierarchy crosses a filesystem boundary: %s" % candidate_root
        )
    if (
        os.path.realpath(os.path.dirname(candidate_root)) != os.path.realpath(candidates_root) or
        not is_under(candidate_root, candidates_root) or
        os.path.realpath(candidate_root) == os.path.realpath(candidates_root)
    ):
        raise UnsafeCleanupError(
            "candidate cleanup target resolves outside its exact run hierarchy: %s" % candidate_root
        )
    return candidate_root, run_root, candidates_root, run_device


def _reject_unsafe_regular_tree(root, expected_device, description):
    def walk_error(error):
        raise UnsafeCleanupError("%s cannot be inspected safely: %s" % (description, error))

    for directory, directory_names, file_names in os.walk(
        root, topdown=True, onerror=walk_error, followlinks=False,
    ):
        for name in directory_names + file_names:
            path = os.path.join(directory, name)
            try:
                path_stat = os.lstat(path)
            except OSError as error:
                raise UnsafeCleanupError("%s changed during validation: %s" % (description, error))
            if stat.S_ISLNK(path_stat.st_mode):
                raise UnsafeCleanupError("%s contains a symbolic link: %s" % (description, path))
            if os.path.ismount(path) or path_stat.st_dev != expected_device:
                raise UnsafeCleanupError(
                    "%s contains a mount or filesystem boundary: %s" % (description, path)
                )
            if not (stat.S_ISDIR(path_stat.st_mode) or stat.S_ISREG(path_stat.st_mode)):
                raise UnsafeCleanupError(
                    "%s contains a non-regular filesystem object: %s" % (description, path)
                )


def _load_nonterminal_candidate_record(candidate_root):
    candidate_path = os.path.join(candidate_root, _contract_text("candidate.json"))
    try:
        candidate_stat = os.lstat(candidate_path)
    except OSError as error:
        raise UnsafeCleanupError(
            "nonterminal candidate record is missing or cannot be inspected: %s" % error
        )
    if (
        stat.S_ISLNK(candidate_stat.st_mode) or
        not stat.S_ISREG(candidate_stat.st_mode) or
        candidate_stat.st_size <= 0
    ):
        raise UnsafeCleanupError(
            "nonterminal candidate record is not a nonempty regular file: %s" % candidate_path
        )
    try:
        candidate = load_json(candidate_path)
    except Exception as error:
        raise UnsafeCleanupError(
            "nonterminal candidate record cannot be read safely: %s" % error
        )
    if not isinstance(candidate, dict):
        raise UnsafeCleanupError("nonterminal candidate record is not a JSON object")
    expected_id = os.path.basename(candidate_root)
    if not isinstance(candidate.get("candidate_id"), str) or candidate.get("candidate_id") != expected_id:
        raise UnsafeCleanupError(
            "candidate id does not match its directory: %r != %s"
            % (candidate.get("candidate_id"), expected_id)
        )
    status = candidate.get("status")
    if not isinstance(status, str) or not status or status != status.strip():
        raise UnsafeCleanupError("candidate status is missing or malformed")
    if status.lower() in _TERMINAL_CANDIDATE_STATES:
        raise UnsafeCleanupError("terminal candidate cannot be deleted as abandoned nonterminal state")
    return candidate, candidate_path


def _candidate_workspace_metadata(candidate_root):
    workspace = os.path.join(candidate_root, "workspace")
    if not os.path.lexists(workspace):
        return None
    if os.path.islink(workspace) or not os.path.isdir(workspace):
        raise UnsafeCleanupError("nonterminal candidate workspace is not a real directory")
    metadata_path = os.path.join(workspace, _contract_text("workspace.json"))
    try:
        metadata_stat = os.lstat(metadata_path)
    except OSError as error:
        raise UnsafeCleanupError(
            "nonterminal candidate workspace metadata is missing: %s" % error
        )
    if (
        stat.S_ISLNK(metadata_stat.st_mode) or
        not stat.S_ISREG(metadata_stat.st_mode) or
        metadata_stat.st_size <= 0
    ):
        raise UnsafeCleanupError(
            "nonterminal candidate workspace metadata is not a nonempty regular file: %s"
            % metadata_path
        )
    try:
        metadata = load_json(metadata_path)
    except Exception as error:
        raise UnsafeCleanupError(
            "nonterminal candidate workspace metadata cannot be read safely: %s" % error
        )
    if not isinstance(metadata, dict):
        raise UnsafeCleanupError("nonterminal candidate workspace metadata is not a JSON object")
    metadata_root = metadata.get("root")
    if (
        not isinstance(metadata_root, str) or not os.path.isabs(metadata_root) or
        os.path.realpath(metadata_root) != os.path.realpath(workspace)
    ):
        raise UnsafeCleanupError("workspace metadata root does not match nonterminal candidate workspace")
    source_libraries = metadata.get("source_libraries")
    if not isinstance(source_libraries, dict):
        raise UnsafeCleanupError("workspace metadata source_libraries is missing or malformed")
    for library, source in source_libraries.items():
        if not isinstance(source, str) or not os.path.isabs(source):
            raise UnsafeCleanupError("workspace source path is malformed for %s" % library)
        if is_under(source, candidate_root) or is_under(candidate_root, source):
            raise UnsafeCleanupError(
                "workspace source library overlaps disposable candidate root: %s" % source
            )
    return {"workspace": workspace, "metadata_path": metadata_path, "metadata": metadata}


def _reject_symlinks_below(root):
    for directory, directory_names, file_names in os.walk(root, topdown=True, followlinks=False):
        for name in directory_names + file_names:
            path = os.path.join(directory, name)
            if os.path.islink(path):
                raise UnsafeCleanupError("partial worker epoch contains a symbolic link: %s" % path)


def _expected_worker_epoch_control(epoch_root):
    namespace = parse_worker_epoch_namespace(
        os.path.basename(os.path.dirname(epoch_root)), os.path.basename(epoch_root),
    )
    if namespace is None:
        raise UnsafeCleanupError("worker epoch namespace is not canonical: %s" % epoch_root)
    from .persistent_maestro import persistent_worker_control_root

    return namespace, persistent_worker_control_root(
        epoch_root, namespace["epoch_identity"],
    )


def _worker_control_roots(epoch_root):
    _namespace, expected_control = _expected_worker_epoch_control(epoch_root)
    epochs_root = os.path.join(epoch_root, "epochs")
    if not os.path.lexists(epochs_root):
        return []
    if os.path.islink(epochs_root) or not os.path.isdir(epochs_root):
        raise UnsafeCleanupError("worker epochs state root is unsafe: %s" % epochs_root)
    expected_child = os.path.dirname(expected_control)
    expected_name = os.path.basename(expected_child)
    unexpected = sorted(name for name in os.listdir(epochs_root) if name != expected_name)
    if unexpected:
        raise UnsafeCleanupError(
            "worker epochs state root contains an unexpected child: %s" % unexpected[0]
        )
    if not os.path.lexists(expected_child):
        return []
    if os.path.islink(expected_child) or not os.path.isdir(expected_child):
        raise UnsafeCleanupError("worker child epoch state is unsafe: %s" % expected_child)
    if not os.path.lexists(expected_control):
        return []
    if os.path.islink(expected_control) or not os.path.isdir(expected_control):
        raise UnsafeCleanupError("worker guardian control root is unsafe: %s" % expected_control)
    return [expected_control]


def _guardian_state_paths(epoch_root):
    result = []
    for control_root in _worker_control_roots(epoch_root):
        state_path = os.path.join(control_root, _contract_text("guardian_state.json"))
        if not os.path.lexists(state_path):
            continue
        if os.path.islink(state_path) or not os.path.isfile(state_path):
            raise UnsafeCleanupError("worker guardian state is unsafe: %s" % state_path)
        result.append(state_path)
    return result


def _guardian_terminal_proof(state):
    if not isinstance(state, dict):
        return {"ok": False, "kind": "guardian_state_malformed"}
    status = state.get("status")
    if status == "launch_failed":
        proven = state.get("pid") is None and state.get("pgid") is None
        return {
            "ok": proven,
            "kind": "ocean_not_spawned" if proven else "launch_failure_unproven",
            "status": status,
        }
    remaining_group = list(state.get("remaining_group_pids") or [])
    remaining_adopted = list(state.get("remaining_adopted_pids") or [])
    proven = bool(
        status == "exited"
        and state.get("group_cleanup_complete") is True
        and state.get("descendant_cleanup_complete") is True
        and state.get("descendant_supervision_complete") is True
        and not remaining_group and not remaining_adopted
    )
    return {
        "ok": proven,
        "kind": "guardian_cleanup_proven" if proven else "guardian_cleanup_unproven",
        "status": status,
    }


def _recorded_path_matches(value, expected):
    return bool(
        isinstance(value, str) and os.path.isabs(value)
        and os.path.abspath(value) == os.path.abspath(expected)
        and os.path.realpath(value) == os.path.realpath(expected)
    )


def _path_signature(path):
    if not os.path.lexists(path):
        return None
    value = os.lstat(path)
    if stat.S_ISLNK(value.st_mode) or not stat.S_ISREG(value.st_mode):
        raise UnsafeCleanupError("worker launch artifact is not a regular file: %s" % path)
    return (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns)


def _acquire_released_worker_launch_lock(path):
    before = _path_signature(path)
    flags = os.O_RDWR | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = None
    try:
        descriptor = os.open(path, flags)
        opened = os.fstat(descriptor)
        if before is None or (opened.st_dev, opened.st_ino) != before[:2]:
            raise UnsafeCleanupError("worker launch lock changed while being opened: %s" % path)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (IOError, OSError) as error:
            if getattr(error, "errno", None) in (errno.EACCES, errno.EAGAIN):
                raise UnsafeCleanupError("worker guardian launch lock is still held: %s" % path)
            raise
        if _path_signature(path)[:2] != (opened.st_dev, opened.st_ino):
            raise UnsafeCleanupError("worker launch lock changed after acquisition: %s" % path)
        return descriptor
    except Exception:
        if descriptor is not None:
            os.close(descriptor)
        raise


def _acquire_worker_launch_cleanup_guards(epoch_root):
    namespace, expected_control = _expected_worker_epoch_control(epoch_root)
    controls = _worker_control_roots(epoch_root)
    expected_manifest = os.path.join(expected_control, LEGACY_GUARDIAN_MANIFEST_NAME)
    expected_lock = os.path.join(expected_control, LEGACY_GUARDIAN_LAUNCH_LOCK_NAME)
    worker_state_path = os.path.join(epoch_root, _contract_text("worker_state.json"))
    worker_state = None
    worker_state_signature = _path_signature(worker_state_path)
    if worker_state_signature is not None:
        worker_state = load_json(worker_state_path)
        if not isinstance(worker_state, dict):
            raise UnsafeCleanupError("worker state is not a JSON object")
    durable_values = {
        "protocol": (worker_state or {}).get("guardian_launch_protocol"),
        "manifest_path": (worker_state or {}).get("guardian_launch_manifest_path"),
        "lock_path": (worker_state or {}).get("guardian_launch_lock_path"),
    }
    durable_recorded = any(value is not None for value in durable_values.values())
    if durable_recorded:
        if any(value is None for value in durable_values.values()):
            raise UnsafeCleanupError("worker guardian durable launch identity is incomplete")
        if (worker_state or {}).get("schema_version") != _contract_text("crescendo.persistent_maestro_worker.v1"):
            raise UnsafeCleanupError("worker state schema is not recognized")
        try:
            recorded_worker_index = int(worker_state.get("worker_index", -1))
        except (TypeError, ValueError):
            raise UnsafeCleanupError("worker state index is malformed")
        if recorded_worker_index != namespace["worker_index"]:
            raise UnsafeCleanupError("worker state index does not match its namespace")
        if worker_state.get("epoch") != namespace["epoch_identity"]:
            raise UnsafeCleanupError("worker state epoch does not match its namespace")
        for key, expected in (
            ("workspace_root", os.path.join(epoch_root, "workspace")),
            ("control_root", expected_control),
            ("guardian_state_path", os.path.join(expected_control, _contract_text("guardian_state.json"))),
        ):
            if not _recorded_path_matches(worker_state.get(key), expected):
                raise UnsafeCleanupError("worker state %s does not match its epoch" % key)
        if durable_values["protocol"] != LEGACY_GUARDIAN_PROTOCOL:
            raise UnsafeCleanupError("worker guardian durable launch protocol is not recognized")
        if not _recorded_path_matches(durable_values["manifest_path"], expected_manifest):
            raise UnsafeCleanupError("worker guardian launch manifest path does not match its epoch")
        if not _recorded_path_matches(durable_values["lock_path"], expected_lock):
            raise UnsafeCleanupError("worker guardian launch lock path does not match its epoch")

    manifest_signature = _path_signature(expected_manifest)
    lock_signature = _path_signature(expected_lock)
    if durable_recorded and (manifest_signature is None or lock_signature is None):
        raise UnsafeCleanupError("worker durable launch artifacts are incomplete")
    durable_proof = None
    guardian_inspections = []
    guardian_state_path = os.path.join(expected_control, _contract_text("guardian_state.json"))
    guardian_signature = (_path_signature(guardian_state_path)
                          if manifest_signature is not None else None)
    if manifest_signature is not None:
        inspection = inspect_legacy_guardian_attempt(
            expected_control,
            expected_workspace_root=os.path.join(epoch_root, "workspace"),
        )
        durable_proof = inspection.get("termination_proof") or {}
        guardian_inspections.append(inspection)
        if inspection.get("state") != "dead" or durable_proof.get("ok") is not True:
            raise UnsafeCleanupError(
                "worker guardian launch termination is not proven: %s"
                % (inspection.get("reason") or durable_proof.get("kind") or inspection.get("state"))
            )
    elif lock_signature is not None:
        safe_pre_manifest = bool(
            worker_state is None or (
                worker_state.get("state") == "new"
                and worker_state.get("pid") is None
                and worker_state.get("ocean_pid") is None
            )
        )
        if not safe_pre_manifest:
            raise UnsafeCleanupError("worker launch lock exists without a proven pre-manifest state")

    descriptors = []
    try:
        if lock_signature is not None:
            descriptors.append(_acquire_released_worker_launch_lock(expected_lock))
        if _path_signature(expected_manifest) != manifest_signature:
            raise UnsafeCleanupError("worker guardian launch manifest changed during cleanup proof")
        if guardian_inspections:
            inspection = guardian_inspections[0]
            if (not _recorded_path_matches(inspection.get("manifest_path"), expected_manifest)
                    or not _recorded_path_matches(inspection.get("state_path"), guardian_state_path)):
                raise UnsafeCleanupError("worker guardian inspection paths changed during cleanup proof")
            # A dead inspection intentionally omits raw records. Forward the
            # exact validated, unchanged records only while holding its launch lock.
            inspection["manifest"] = load_json(expected_manifest)
            inspection["guardian"] = (load_json(guardian_state_path)
                                      if guardian_signature is not None else {})
            if (_path_signature(guardian_state_path) != guardian_signature
                    or _path_signature(expected_manifest) != manifest_signature):
                raise UnsafeCleanupError("worker guardian records changed during cleanup proof")
        if _path_signature(worker_state_path) != worker_state_signature:
            raise UnsafeCleanupError("worker state changed during cleanup proof")
        if controls and controls != [expected_control]:
            raise UnsafeCleanupError("worker control root does not match its strict epoch identity")
        return {
            "descriptors": descriptors,
            "durable_proof": durable_proof,
            "guardian_inspections": guardian_inspections,
            "manifest_path": expected_manifest if manifest_signature is not None else None,
        }
    except Exception:
        for descriptor in descriptors:
            os.close(descriptor)
        raise


def _release_worker_launch_cleanup_guards(guard):
    for descriptor in (guard or {}).get("descriptors") or []:
        try:
            os.close(descriptor)
        except OSError:
            pass


def _validate_partial_worker_epoch_proof(epoch_root, termination_proof, launch_guard=None):
    proof = termination_proof if isinstance(termination_proof, dict) else {}
    kind = proof.get("kind")
    if proof.get("ok") is not True or kind not in _PARTIAL_WORKER_EPOCH_PROOFS:
        raise UnsafeCleanupError(
            "partial worker epoch requires explicit proven-dead or no-spawn proof"
        )
    _reject_symlinks_below(epoch_root)
    state_path = os.path.join(epoch_root, _contract_text("worker_state.json"))
    guardian_paths = _guardian_state_paths(epoch_root)
    if kind == "worker_not_created":
        if os.path.lexists(state_path) or guardian_paths:
            raise UnsafeCleanupError("worker_not_created proof conflicts with persisted process state")
        return kind
    durable_proof = (launch_guard or {}).get("durable_proof") or {}
    durable_is_safe = bool(
        durable_proof.get("ok") is True and durable_proof.get("kind") == kind
    )
    state_is_safe = False
    state_exists = os.path.lexists(state_path)
    if state_exists:
        if os.path.islink(state_path) or not os.path.isfile(state_path):
            raise UnsafeCleanupError("worker state is unsafe: %s" % state_path)
        state = load_json(state_path)
        state_is_safe = bool(
            state.get("state") == "new" and
            "pid" in state and "ocean_pid" in state and
            state.get("pid") is None and state.get("ocean_pid") is None
        )
    guardians_are_safe = bool(guardian_paths)
    if not durable_is_safe:
        for path in guardian_paths:
            state = load_json(path)
            guardian_proof = _guardian_terminal_proof(state)
            if guardian_proof.get("ok") is not True:
                raise UnsafeCleanupError("proven-dead proof conflicts with guardian state: %s" % path)
            if kind == "ocean_not_spawned" and guardian_proof.get("kind") != "ocean_not_spawned":
                raise UnsafeCleanupError("ocean_not_spawned proof conflicts with guardian state: %s" % path)
    if state_exists and not state_is_safe and not durable_is_safe and not guardians_are_safe:
        raise UnsafeCleanupError("proven-dead proof conflicts with persisted worker process identity")
    if kind not in ("ocean_not_spawned", "worker_not_created") and not durable_is_safe and not guardians_are_safe:
        raise UnsafeCleanupError("proven-dead worker proof has no matching persisted process evidence")
    if kind == "ocean_not_spawned" and not state_is_safe and not guardians_are_safe and not durable_is_safe:
        raise UnsafeCleanupError("ocean_not_spawned proof has no matching persisted process evidence")
    return kind


def _error_tail(value, limit=4096):
    text = str(value or "")
    limit = max(0, int(limit))
    if len(text) <= limit:
        return text
    if not limit:
        return ""
    marker = "\n...[middle truncated; original_chars=%s]...\n" % len(text)
    if len(marker) >= limit:
        return text[:max(0, limit - 3)] + "." * min(3, limit)
    available = limit - len(marker)
    head = (available + 1) // 2
    tail = available - head
    return text[:head] + marker + (text[-tail:] if tail else "")


def _compact_parameter_evidence(application):
    evidence = []
    for parameter in application.get("parameters") or []:
        effective = []
        seen = set()
        for row in parameter.get("effective_netlist") or []:
            item = {"mode": row.get("mode"), "value": row.get("value")}
            identity = (item["mode"], str(item["value"]))
            if identity not in seen:
                seen.add(identity)
                effective.append(item)
        evidence.append({
            "parameter_id": parameter.get("parameter_id"),
            "hierarchy_path": parameter.get("hierarchy_path"),
            "property": parameter.get("property"),
            "requested": parameter.get("requested"),
            "persisted_schematic_value": parameter.get("persisted_schematic_value"),
            "persisted_cdf_value": parameter.get("persisted_cdf_value"),
            "callback_status": parameter.get("callback_status"),
            "effective_netlist": effective,
        })
    return evidence


def lightweight_candidate_record(row):
    result = {}
    for key in (
        "candidate_id", "generation", "index", "values", "status", "metrics",
        "failure_class", "started_at", "finished_at", "source_guard", "batch_reference",
        "shared_with", "physical_execution_candidate",
        "failure_stage", "spectre_state", "execution_backend",
        "persistent_worker", "simulation_submission",
        "batch_execution",
        "native_job_state",
        "cleanup_pending", "process_cleanup",
        "native_storage_paths",
        "execution_progress", "source_test_map",
        "execution_started", "shared_preparation_failed", "shared_preparation_failure",
        "execution_pipeline", "netlist_generation", "preparation_cleanup",
    ):
        if key in row:
            result[key] = row[key]
    if row.get("error"):
        result["error"] = _error_tail(row["error"])
    completion = row.get("completion_evidence") or row.get("completion")
    if completion:
        spectre_logs = []
        seen_spectre_logs = set()
        for spectre_row in (
                completion.get("spectre_runs")
                or completion.get("spectre_logs") or []):
            if not isinstance(spectre_row, dict):
                continue
            compact = {
                "test": spectre_row.get("test"),
                "corner": spectre_row.get("corner"),
                "point_id": spectre_row.get("point_id"),
            }
            if spectre_row.get("artifact_test"):
                compact["artifact_test"] = spectre_row["artifact_test"]
            identity = (
                repr(compact["test"]), repr(compact["corner"]),
                repr(compact["point_id"]),
            )
            if identity in seen_spectre_logs:
                continue
            seen_spectre_logs.add(identity)
            spectre_logs.append(compact)
        result["completion"] = {
            "ok": bool(completion.get("ok")),
            "adapter": completion.get("adapter"),
            "history_name": completion.get("history_name"),
            "tests": list(completion.get("tests") or []),
            "corners": list(completion.get("corners") or []),
            "point_count": len(completion.get("points") or []),
            "output_count": len(completion.get("outputs") or []),
            "spectre_logs": spectre_logs,
        }
        if completion.get("candidate_test_name_map"):
            result["completion"]["candidate_test_name_map"] = dict(completion["candidate_test_name_map"])
        if row.get("execution_backend") in ("private_netlist", "maestro_netlist"):
            result["completion"]["parameters"] = dict(completion.get("parameters") or {})
            result["completion"]["template_sha256"] = completion.get("template_sha256")
            for key in ("execution_pipeline", "execution_simulator", "native_parameter_readback_verified", "netlist_generation"):
                if key in completion:
                    result["completion"][key] = completion[key]
    application = row.get("parameter_application")
    if application:
        parameters = _compact_parameter_evidence(application)
        result["parameter_application"] = {
            "ok": bool(application.get("ok")),
            "parameter_count": len(parameters),
            "netlist_count": len(application.get("netlists") or []),
            "parameters": parameters,
        }
        if application.get("export_snapshot"):
            result["parameter_application"]["export_snapshot"] = application["export_snapshot"]
    return result


def persist_lightweight_candidate(candidate_root, row):
    candidate_root = os.path.abspath(candidate_root)
    path = os.path.join(candidate_root, _contract_text("candidate.json"))
    atomic_write_json(path, lightweight_candidate_record(row))
    return path


def summarize_successful_candidate_artifacts(candidate_root, run_root):
    """Freeze only the remaining direct, known lightweight files for core data."""
    candidate_root, run_root, _parent, run_device = _validated_direct_candidate_paths(candidate_root, run_root)
    names = sorted(os.listdir(candidate_root))
    summaries = []
    for name in names:
        if name not in (_contract_text("candidate.json"), _contract_text("timing.json"), _contract_text("completion_evidence.json"), _contract_text("execution_diagnostics.json"), _contract_text("cli_progress.json"), _contract_text("artifact_summary.json")) and not _retained_spectre_name(name):
            return None  # Raw/pending/unknown evidence still belongs to native cleanup.
        path = os.path.join(candidate_root, name)
        value = os.lstat(path)
        if (not stat.S_ISREG(value.st_mode) or value.st_nlink != 1 or value.st_dev != run_device):
            raise UnsafeCleanupError("successful candidate summary contains an unsafe file: %s" % path)
        if name != _contract_text("artifact_summary.json"):
            summaries.append({"name": name, "bytes": value.st_size, "sha256": sha256_file(path)})
    return summaries


@_serialize_cleanup(1)
def compact_successful_candidate_artifacts(candidate_root, run_root):
    """Compact direct success files after their core/checkpoint ACK.

    Shared CLI candidates retain hashed logical audit files. Other candidates
    keep timing/completion/actual-parameter evidence in the core candidate.
    Failure/unknown/pending artifacts never enter this path.
    """
    candidate_root, run_root, _parent, run_device = _validated_direct_candidate_paths(candidate_root, run_root)
    from .runner import validate_checkpoint_integrity
    from .run_data_store import dataset_from_reference
    checkpoint_path = os.path.join(run_root, _contract_text("checkpoint.json"))
    if not _regular_nonempty(checkpoint_path, run_root):
        raise UnsafeCleanupError("successful compaction requires the durable run checkpoint")
    checkpoint = load_json(checkpoint_path)
    validate_checkpoint_integrity(checkpoint)
    reference = checkpoint.get("candidate_store")
    if not reference or os.path.dirname(reference.get("path") or "") != run_root:
        raise UnsafeCleanupError("successful compaction requires this run's core data")
    dataset = dataset_from_reference(reference)
    identity = os.path.basename(candidate_root)
    rows = [row for row in dataset["candidates"] if row.get("candidate_id") == identity]
    if len(rows) != 1:
        raise UnsafeCleanupError("successful candidate core identity is ambiguous")
    row = rows[0]
    if row.get('batch_reference'):
        from .generation_batch import require_history_released
        try:
            require_history_released(row['batch_reference'], identity)
        except (OSError, ValueError) as error:
            raise UnsafeCleanupError(str(error))
    proof = row.get("raw_cleanup_proof") or {}
    if (row.get("status") != "completed" or (row.get("source_guard") or {}).get("ok") is not True
            or (row.get("completion_evidence") or row.get("completion") or {}).get("ok") is not True
            or proof.get("ok") is not True or proof.get("process_termination_proven") is not True
            or proof.get("remote_stop_proven") is not True):
        raise UnsafeCleanupError("successful candidate completion/cleanup proof is incomplete")
    expected = row.get("artifact_summary")
    actual = summarize_successful_candidate_artifacts(candidate_root, run_root)
    if not expected or actual != expected:
        raise UnsafeCleanupError("successful candidate files changed after their durable summary")
    if row.get('batch_reference') and row.get('execution_backend') in ('private_netlist', 'maestro_netlist'):
        summary_path = os.path.join(candidate_root, _contract_text('artifact_summary.json'))
        summary = {'schema_version': _contract_text('crescendo.logical_candidate_artifacts.v1'),
            'candidate_id': identity, 'batch_reference': row['batch_reference'],
            'physical_execution_candidate': row.get('physical_execution_candidate'),
            'shared_with': row.get('shared_with'), 'files': expected,
            'artifact_cleanup_complete': True, 'checkpoint_sha256': checkpoint.get('payload_sha256')}
        atomic_write_json(summary_path, summary)
        return {'ok': True, 'artifact_cleanup_complete': True, 'removed': [],
            'retained': [item['name'] for item in expected] + [_contract_text('artifact_summary.json')],
            'summary_path': summary_path, 'candidate_id': identity,
            'checkpoint_sha256': checkpoint.get('payload_sha256')}
    removed = []
    for artifact in expected:
        path = os.path.join(candidate_root, artifact["name"])
        os.unlink(path)
        removed.append(artifact["name"])
    os.rmdir(candidate_root)
    _fsync_directory(os.path.dirname(candidate_root))
    return {"ok": True, "artifact_cleanup_complete": True, "removed": removed,
            "candidate_id": identity, "checkpoint_sha256": checkpoint.get("payload_sha256")}


def _regular_nonempty(path, candidate_root):
    return (
        is_under(path, candidate_root) and
        not os.path.islink(path) and
        os.path.isfile(path) and
        os.path.getsize(path) > 0
    )


def _retained_spectre_name(name):
    from .shared_cli_logs import retained_cli_name
    if retained_cli_name(name):
        return True
    if not isinstance(name, str) or not name.startswith(_RETAINED_SPECTRE_PREFIX):
        return False
    for suffix in _RETAINED_SPECTRE_SUFFIXES:
        if not name.endswith(suffix):
            continue
        token = name[len(_RETAINED_SPECTRE_PREFIX):-len(suffix)]
        return bool(token) and all(
            character in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
            for character in token
        )
    return False


def retained_spectre_paths(candidate_root):
    candidate_root = os.path.abspath(candidate_root)
    if os.path.islink(candidate_root) or not os.path.isdir(candidate_root):
        raise UnsafeCleanupError(
            "retained Spectre log candidate root is missing or unsafe: %s" % candidate_root
        )
    paths = []
    for name in sorted(os.listdir(candidate_root)):
        if not _retained_spectre_name(name):
            continue
        path = os.path.join(candidate_root, name)
        try:
            observed = os.lstat(path)
        except OSError as error:
            raise UnsafeCleanupError(
                "retained Spectre log changed during inspection: %s" % error
            )
        if (
                not stat.S_ISREG(observed.st_mode) or int(observed.st_nlink) != 1
                or int(observed.st_size) <= 0 or os.path.dirname(path) != candidate_root):
            raise UnsafeCleanupError("retained Spectre log is unsafe: %s" % path)
        paths.append(path)
    return paths


def distribute_batch_spectre_logs(batch_root, candidates, workspace_root):
    """Move owned retained log pairs to their candidate before history cleanup."""
    by_test = {}
    for candidate in candidates:
        root = os.path.abspath(candidate["candidate_root"])
        if os.path.islink(root) or not os.path.isdir(root):
            raise UnsafeCleanupError("batch log destination is unsafe: %s" % root)
        for physical in candidate["source_test_map"]:
            if physical in by_test:
                raise UnsafeCleanupError("batch retained log test is ambiguous: %s" % physical)
            by_test[physical] = root
    paths = set(retained_spectre_paths(batch_root))
    copied = []
    for meta_path in sorted(path for path in paths if path.endswith(".meta")):
        if os.path.getsize(meta_path) > 65536:
            raise UnsafeCleanupError("batch retained log metadata is oversized")
        with open(meta_path, "r", encoding="utf-8") as stream:
            meta = dict(line.rstrip("\r\n").split("=", 1) for line in stream if "=" in line)
        source_path = meta.get("source_path") or ""
        parts = source_path.replace("\\", "/").split("/")
        if (meta.get("schema_version") != _contract_text("crescendo.retained_spectre_log.v1")
                or not os.path.isabs(source_path) or not is_under(source_path, workspace_root)
                or len(parts) < 4 or parts[-2:] != ["psf", "spectre.out"]):
            continue
        root = by_test.get(parts[-3])
        log_path = meta_path[:-5] + ".log"
        if root is None or log_path not in paths:
            continue
        for source in (log_path, meta_path):
            destination = os.path.join(root, os.path.basename(source))
            if os.path.lexists(destination):
                raise UnsafeCleanupError("batch retained log destination already exists")
            descriptor = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, "wb") as target, open(source, "rb") as stream:
                shutil.copyfileobj(stream, target)
                target.flush()
                os.fsync(target.fileno())
            copied.append((source, destination))
    # All copies survive before removing shared duplicates. Any failed copy
    # leaves the source pair available for diagnosis and recovery.
    for source, _destination in copied:
        os.unlink(source)
    return {"copied_files": len(copied), "candidate_count": len(candidates)}


@_serialize_cleanup(0)
def clear_retained_spectre_logs(run_root, include_live=False):
    """Clear ordinary temporary retained logs; keep stopped live evidence.

    Live snapshots are run data, usable after Stop or UI Exit. Removing them
    requires an explicit data-deletion action with include_live=True.
    """
    run_root = os.path.abspath(run_root)
    if os.path.islink(run_root) or not os.path.isdir(run_root):
        raise UnsafeCleanupError(
            "retained Spectre log run root is missing or unsafe: %s" % run_root
        )
    candidates_root = os.path.join(run_root, "candidates")
    if not os.path.lexists(candidates_root):
        return {"ok": True, "removed": [], "removed_count": 0}
    if os.path.islink(candidates_root) or not os.path.isdir(candidates_root):
        raise UnsafeCleanupError(
            "retained Spectre log candidates root is unsafe: %s" % candidates_root
        )
    removed = []
    for name in sorted(os.listdir(candidates_root)):
        candidate_root = os.path.join(candidates_root, name)
        if os.path.islink(candidate_root) or not os.path.isdir(candidate_root):
            continue
        candidate_root, _run_root, _candidates_root, _run_device = (
            _validated_direct_candidate_paths(candidate_root, run_root)
        )
        for path in retained_spectre_paths(candidate_root):
            if not include_live and os.path.basename(path).startswith(_RETAINED_SPECTRE_PREFIX + "live_"):
                continue
            os.unlink(path)
            removed.append(os.path.relpath(path, run_root).replace(os.sep, "/"))
    return {
        "ok": True, "removed": removed, "removed_count": len(removed),
        "cleaned_at": utc_timestamp(),
    }


@_serialize_cleanup(1)
def safe_delete_candidate_workspace(candidate_root, run_root, source_guard, required_lightweight_paths=None):
    cleanup_started = timestamp()
    candidate_root, run_root, _candidates_root, run_device = _validated_direct_candidate_paths(
        candidate_root, run_root,
    )
    if not source_guard or not source_guard.get("ok"):
        raise UnsafeCleanupError("candidate workspace cannot be deleted before source integrity is verified")
    _reject_mounts_or_cross_device_tree(
        candidate_root, run_device, "terminal candidate tree",
    )
    candidate_path = os.path.join(candidate_root, _contract_text("candidate.json"))
    required = list(required_lightweight_paths or [
        candidate_path,
        os.path.join(candidate_root, _contract_text("completion_evidence.json")),
    ])
    if candidate_path not in required:
        required.append(candidate_path)
    for retained_path in retained_spectre_paths(candidate_root):
        if retained_path not in required:
            required.append(retained_path)
    completion_path = os.path.join(candidate_root, _contract_text("completion_evidence.json"))
    timing_path = os.path.join(candidate_root, _contract_text("timing.json"))
    for optional_path in (completion_path, timing_path):
        if os.path.lexists(optional_path):
            if not _regular_nonempty(optional_path, candidate_root):
                raise UnsafeCleanupError(
                    "lightweight recovery artifact is unsafe: %s" % optional_path
                )
            if optional_path not in required:
                required.append(optional_path)
    preserved = set()
    for path in required:
        path = os.path.abspath(path)
        if not _regular_nonempty(path, candidate_root):
            raise UnsafeCleanupError("required lightweight recovery artifact is missing or unsafe: %s" % path)
        if is_under(path, os.path.join(candidate_root, "workspace")):
            raise UnsafeCleanupError("recovery artifact would be deleted with the workspace: %s" % path)
        if os.path.dirname(path) != candidate_root:
            raise UnsafeCleanupError("candidate lightweight artifacts must be direct files under the candidate directory: %s" % path)
        preserved.add(path)
    resumed = _resume_pending_terminal_candidate(candidate_root, run_root)
    if resumed is not None:
        evidence = resumed["evidence"]
        workspace_removed = evidence["workspace_removed"]
        workspace_relative = evidence["workspace_relative"]
        raw_removed = list(evidence["raw_artifacts_removed"])
        result = {
            "ok": True,
            "removed": workspace_removed,
            "workspace": workspace_relative,
            "raw_artifacts_removed": sorted(raw_removed),
            "cleaned_at": utc_timestamp(),
        }
        if timing_path in preserved:
            update_cleanup_timing(timing_path, elapsed_seconds=elapsed(cleanup_started, timestamp()),
                                  workspace_removed=workspace_removed, raw_artifact_count=len(raw_removed),
                                  resumed=True)
        return result
    workspace = os.path.join(candidate_root, "workspace")
    workspace_relative = None
    workspace_removed = False
    if os.path.lexists(workspace):
        if os.path.islink(workspace):
            raise UnsafeCleanupError("candidate workspace is a symbolic link: %s" % workspace)
        if not os.path.isdir(workspace):
            raise UnsafeCleanupError("candidate workspace is not a directory: %s" % workspace)
        if not is_under(workspace, candidate_root):
            raise UnsafeCleanupError("candidate workspace resolves outside the candidate directory: %s" % workspace)
        metadata_path = os.path.join(workspace, _contract_text("workspace.json"))
        if not _regular_nonempty(metadata_path, workspace):
            raise UnsafeCleanupError("candidate workspace metadata is missing: %s" % metadata_path)
        metadata = load_json(metadata_path)
        if os.path.realpath(metadata.get("root", "")) != os.path.realpath(workspace):
            raise UnsafeCleanupError("workspace metadata root does not match cleanup target")
        for source in (metadata.get("source_libraries") or {}).values():
            if not isinstance(source, str) or not os.path.isabs(source):
                raise UnsafeCleanupError("workspace source library path is malformed: %r" % source)
            if is_under(source, workspace) or is_under(workspace, source):
                raise UnsafeCleanupError(
                    "source library overlaps disposable workspace: %s" % source
                )
        workspace_relative = os.path.relpath(workspace, run_root).replace(os.sep, "/")
        if metadata.get("same_library_registry"):
            inspection = inspect_legacy_guardian_attempt(candidate_root, expected_workspace_root=workspace)
            if inspection.get("state") != "dead":
                raise UnsafeCleanupError("Candidate native process termination is unproven; owned views retained")
            from .same_library_views import cleanup_same_library_views
            cleanup = cleanup_same_library_views(metadata, process_cleanup_proven=True)
            if not cleanup.get("cleanup_proven"):
                raise UnsafeCleanupError("Candidate owned view cleanup is unproven: %s" % cleanup)
        workspace_removed = True
    deletion_plan = []
    disposable = []
    for name in sorted(os.listdir(candidate_root)):
        path = os.path.join(candidate_root, name)
        if path in preserved:
            continue
        if not is_under(os.path.dirname(path), candidate_root):
            raise UnsafeCleanupError("raw artifact parent escapes candidate directory: %s" % path)
        is_link = os.path.islink(path)
        is_directory = os.path.isdir(path) and not is_link
        if is_directory and not is_under(path, candidate_root):
            raise UnsafeCleanupError("raw artifact directory resolves outside candidate directory: %s" % path)
        disposable.append(name)
        if name != "workspace":
            deletion_plan.append((name, is_directory))
    raw_removed = [name + ("/" if is_directory else "") for name, is_directory in deletion_plan]
    evidence = {
        "candidate_id": os.path.basename(candidate_root),
        "source_verified": True,
        "preserved": sorted(os.path.basename(path) for path in preserved),
        "disposable": disposable,
        "workspace_removed": workspace_removed,
        "workspace_relative": workspace_relative,
        "raw_artifacts_removed": sorted(raw_removed),
    }
    _terminal_candidate_cleanup_transaction(candidate_root, run_root, evidence)
    result = {
        "ok": True,
        "removed": workspace_removed,
        "workspace": workspace_relative,
        "raw_artifacts_removed": sorted(raw_removed),
        "cleaned_at": utc_timestamp(),
    }
    if timing_path in preserved:
        update_cleanup_timing(timing_path, elapsed_seconds=elapsed(cleanup_started, timestamp()),
                              workspace_removed=workspace_removed, raw_artifact_count=len(raw_removed))
    return result


@_serialize_cleanup(1)
def cleanup_proven_terminal_infrastructure_candidate(candidate_root, run_root,
                                                     required_lightweight_paths=None):
    cleanup_started = timestamp()
    candidate_root, run_root, _candidates_root, run_device = _validated_direct_candidate_paths(
        candidate_root, run_root,
    )
    if os.path.lexists(os.path.join(candidate_root, "workspace")):
        raise UnsafeCleanupError(
            "controller-termination cleanup refuses any candidate workspace"
        )
    _reject_mounts_or_cross_device_tree(
        candidate_root, run_device, "proven-dead terminal candidate tree",
    )
    candidate_path = os.path.join(candidate_root, _contract_text("candidate.json"))
    timing_path = os.path.join(candidate_root, _contract_text("timing.json"))
    if not _regular_nonempty(candidate_path, candidate_root):
        raise UnsafeCleanupError(
            "controller-termination candidate record is missing or unsafe: %s" % candidate_path
        )
    try:
        candidate = load_json(candidate_path)
    except Exception as error:
        raise UnsafeCleanupError(
            "controller-termination candidate record cannot be read: %s" % error
        )
    if candidate.get("candidate_id") != os.path.basename(candidate_root):
        raise UnsafeCleanupError(
            "controller-termination candidate identity does not match its directory"
        )
    termination_evidence = _controller_termination_evidence(candidate)
    persist_lightweight_candidate(candidate_root, candidate)

    required = list(required_lightweight_paths or [candidate_path])
    if candidate_path not in required:
        required.append(candidate_path)
    if os.path.lexists(timing_path):
        if timing_path not in required:
            required.append(timing_path)
    else:
        required = [path for path in required if os.path.abspath(path) != timing_path]
    for retained_path in retained_spectre_paths(candidate_root):
        if retained_path not in required:
            required.append(retained_path)
    completion_path = os.path.join(candidate_root, _contract_text("completion_evidence.json"))
    if os.path.lexists(completion_path) and completion_path not in required:
        required.append(completion_path)
    preserved = set()
    for path in required:
        path = os.path.abspath(path)
        if not _regular_nonempty(path, candidate_root) or os.path.dirname(path) != candidate_root:
            raise UnsafeCleanupError(
                "required controller-failure recovery artifact is missing or unsafe: %s" % path
            )
        preserved.add(path)

    resumed = _resume_pending_terminal_candidate(candidate_root, run_root)
    if resumed is not None:
        raw_removed = sorted(resumed["evidence"]["raw_artifacts_removed"])
        was_resumed = True
    else:
        disposable = []
        raw_removed = []
        for name in sorted(os.listdir(candidate_root)):
            path = os.path.join(candidate_root, name)
            if path in preserved:
                continue
            if name == "workspace":
                raise UnsafeCleanupError(
                    "controller-termination cleanup refuses any candidate workspace"
                )
            is_directory = os.path.isdir(path) and not os.path.islink(path)
            disposable.append(name)
            raw_removed.append(name + ("/" if is_directory else ""))
        evidence = {
            "candidate_id": os.path.basename(candidate_root),
            "source_verified": False,
            "controller_termination": termination_evidence,
            "preserved": sorted(os.path.basename(path) for path in preserved),
            "disposable": disposable,
            "workspace_removed": False,
            "workspace_relative": None,
            "raw_artifacts_removed": sorted(raw_removed),
        }
        _terminal_candidate_cleanup_transaction(
            candidate_root, run_root, evidence,
            proof_kind="controller_termination_proven",
        )
        was_resumed = False

    result = {
        "ok": True,
        "removed": False,
        "workspace": None,
        "raw_artifacts_removed": sorted(raw_removed),
        "proof_kind": "controller_termination_proven",
        "cleaned_at": utc_timestamp(),
    }
    if timing_path in preserved:
        update_cleanup_timing(timing_path, elapsed_seconds=elapsed(cleanup_started, timestamp()),
                              workspace_removed=False, raw_artifact_count=len(raw_removed),
                              controller_termination_proven=True, resumed=was_resumed)
    return result


def _require_remote_batch_quiescent(epoch_root):
    state_path = os.path.join(epoch_root, _contract_text("worker_state.json"))
    if not os.path.lexists(state_path):
        return
    if not _regular_nonempty(state_path, epoch_root):
        raise UnsafeCleanupError("worker state is unsafe while checking remote batch cleanup")
    state = load_json(state_path)
    pool = state.get("remote_session_pool") if isinstance(state, dict) else None
    if pool is not None:
        if (not isinstance(pool, dict) or
                (pool.get("simulation_submitted") is not True and
                 pool.get("simulation_submitted") is not False)):
            raise UnsafeCleanupError(
                "remote Maestro session pool state is unknown; workspace retained")
        if pool.get("simulation_submitted") is True and not (
                pool.get("native_session_closed") is True and
                pool.get("native_stop_proven") is True):
            raise UnsafeCleanupError(
                "remote Maestro session pool closure/cancellation is unproven; workspace retained")
    remote = state.get("remote_batch") if isinstance(state, dict) else None
    if remote is None:
        return
    if (not isinstance(remote, dict) or
            (remote.get("simulation_submitted") is not True and
             remote.get("simulation_submitted") is not False)):
        raise UnsafeCleanupError("remote batch submission state is unknown; workspace retained")
    if remote.get("simulation_submitted") is True and not (
            remote.get("native_completion_proven") is True or remote.get("native_stop_proven") is True):
        raise UnsafeCleanupError(
            "remote Maestro batch completion/cancellation is unproven; workspace retained"
        )


@_serialize_cleanup(1)
def safe_delete_worker_epoch(epoch_root, run_root, source_guard):
    epoch_root, run_root, _workers_root, _worker_root = _validated_worker_epoch_paths(
        epoch_root, run_root,
    )
    _require_remote_batch_quiescent(epoch_root)
    launch_guard = _acquire_worker_launch_cleanup_guards(epoch_root)
    try:
        if not source_guard or not source_guard.get("ok"):
            raise UnsafeCleanupError("worker epoch cannot be deleted before source integrity is verified")
        workspace = os.path.join(epoch_root, "workspace")
        if os.path.islink(workspace) or not os.path.isdir(workspace):
            raise UnsafeCleanupError("worker workspace is missing, not a directory, or a symbolic link: %s" % workspace)
        metadata_path = os.path.join(workspace, _contract_text("workspace.json"))
        if not _regular_nonempty(metadata_path, workspace):
            raise UnsafeCleanupError("worker workspace metadata is missing: %s" % metadata_path)
        metadata = load_json(metadata_path)
        if os.path.realpath(metadata.get("root", "")) != os.path.realpath(workspace):
            raise UnsafeCleanupError("worker workspace metadata root does not match cleanup target")
        for source in (metadata.get("source_libraries") or {}).values():
            if not isinstance(source, str) or not os.path.isabs(source):
                raise UnsafeCleanupError("worker source library path is malformed: %r" % source)
            if is_under(source, workspace) or is_under(workspace, source):
                raise UnsafeCleanupError(
                    "source library overlaps disposable worker workspace: %s" % source
                )
        relative = os.path.relpath(epoch_root, run_root).replace(os.sep, "/")
        from .same_library_views import cleanup_same_library_views
        view_cleanup = cleanup_same_library_views(
            metadata, process_cleanup_proven=True,
            process_cleanup_evidence={
                "process_termination_proven": True,
                "remote_stop_proven": True,
                "guardian_inspections": launch_guard.get("guardian_inspections") or [],
            },
        )
        if not view_cleanup.get("cleanup_proven"):
            raise UnsafeCleanupError("Worker owned view cleanup is unproven: %s" % view_cleanup)
        manifest = _quarantine_and_delete(
            epoch_root, run_root, "worker_epoch", relative,
            proof_kind="source_integrity_verified", evidence={"source_verified": True},
        )
        return {"ok": True, "removed": True, "worker_epoch": relative,
                "artifact_cleanup_complete": True,
                "artifact_cleanup": manifest.get("artifact_cleanup"),
                "owned_view_cleanup": view_cleanup,
                "cleaned_at": utc_timestamp()}
    finally:
        _release_worker_launch_cleanup_guards(launch_guard)


def _safe_delete_partial_worker_epoch(epoch_root, run_root, termination_proof):
    epoch_root, run_root, _workers_root, _worker_root = _validated_worker_epoch_paths(
        epoch_root, run_root,
    )
    launch_guard = _acquire_worker_launch_cleanup_guards(epoch_root)
    try:
        workspace = os.path.join(epoch_root, "workspace")
        if os.path.lexists(workspace):
            if os.path.islink(workspace) or not os.path.isdir(workspace):
                raise UnsafeCleanupError("partial worker workspace is unsafe: %s" % workspace)
            metadata_path = os.path.join(workspace, _contract_text("workspace.json"))
            if os.path.lexists(metadata_path):
                raise UnsafeCleanupError(
                    "partial worker cleanup cannot bypass existing workspace metadata: %s" % metadata_path
                )
        kind = _validate_partial_worker_epoch_proof(
            epoch_root, termination_proof, launch_guard=launch_guard,
        )
        relative = os.path.relpath(epoch_root, run_root).replace(os.sep, "/")
        manifest = _quarantine_and_delete(
            epoch_root, run_root, "worker_epoch", relative,
            proof_kind=kind, evidence={"termination_proven": True},
        )
        return {
            "ok": True,
            "removed": True,
            "worker_epoch": relative,
            "proof_kind": kind,
            "artifact_cleanup_complete": True,
            "artifact_cleanup": manifest.get("artifact_cleanup"),
            "cleaned_at": utc_timestamp(),
        }
    finally:
        _release_worker_launch_cleanup_guards(launch_guard)


@_serialize_cleanup(1)
def cleanup_abandoned_candidate(candidate_root, run_root, source_verifier, required_lightweight_paths=None):
    candidate_root = os.path.abspath(candidate_root)
    candidates_root = os.path.join(os.path.abspath(run_root), "candidates")
    if (
        os.path.islink(candidates_root) or not os.path.isdir(candidates_root) or
        not is_under(candidate_root, candidates_root) or
        os.path.realpath(candidate_root) == os.path.realpath(candidates_root) or
        os.path.islink(candidate_root) or not os.path.isdir(candidate_root)
    ):
        raise UnsafeCleanupError("abandoned candidate target is outside its run or is unsafe: %s" % candidate_root)
    workspace_path = os.path.join(candidate_root, "workspace")
    if os.path.islink(workspace_path) or not os.path.isdir(workspace_path):
        raise UnsafeCleanupError("abandoned candidate workspace is missing or unsafe: %s" % workspace_path)
    metadata_path = os.path.join(workspace_path, _contract_text("workspace.json"))
    if not _regular_nonempty(metadata_path, workspace_path):
        raise UnsafeCleanupError("abandoned candidate has no workspace metadata: %s" % metadata_path)
    source_guard = source_verifier(load_json(metadata_path))
    candidate_path = os.path.join(candidate_root, _contract_text("candidate.json"))
    if source_guard.get("ok") and os.path.isfile(candidate_path):
        persist_lightweight_candidate(candidate_root, load_json(candidate_path))
    return safe_delete_candidate_workspace(
        candidate_root, run_root, source_guard,
        required_lightweight_paths=required_lightweight_paths,
    )


@_serialize_cleanup(1)
def cleanup_abandoned_nonterminal_candidate(candidate_root, run_root, source_verifier):
    candidate_root, run_root, _candidates_root, run_device = _validated_direct_candidate_paths(
        candidate_root, run_root,
    )
    _reject_unsafe_regular_tree(candidate_root, run_device, "nonterminal candidate tree")
    candidate, _candidate_path = _load_nonterminal_candidate_record(candidate_root)
    workspace = _candidate_workspace_metadata(candidate_root)
    if workspace is not None:
        try:
            source_guard = source_verifier(workspace["metadata"])
        except Exception as error:
            raise UnsafeCleanupError(
                "nonterminal candidate source verification failed: %s" % error
            )
        if not isinstance(source_guard, dict) or source_guard.get("ok") is not True:
            raise UnsafeCleanupError(
                "nonterminal candidate cannot be deleted before source integrity is verified"
            )

    candidate_root, run_root, _candidates_root, run_device = _validated_direct_candidate_paths(
        candidate_root, run_root,
    )
    _reject_unsafe_regular_tree(candidate_root, run_device, "nonterminal candidate tree")
    current_candidate, _candidate_path = _load_nonterminal_candidate_record(candidate_root)
    if (
        current_candidate.get("candidate_id") != candidate.get("candidate_id") or
        current_candidate.get("status") != candidate.get("status")
    ):
        raise UnsafeCleanupError("nonterminal candidate identity or status changed before cleanup")
    current_workspace = _candidate_workspace_metadata(candidate_root)
    if (workspace is None) != (current_workspace is None):
        raise UnsafeCleanupError("nonterminal candidate workspace presence changed before cleanup")
    if workspace is not None and current_workspace["metadata"] != workspace["metadata"]:
        raise UnsafeCleanupError("nonterminal candidate workspace metadata changed before cleanup")
    relative = os.path.relpath(candidate_root, run_root).replace(os.sep, "/")
    candidate_id = candidate["candidate_id"]
    _quarantine_and_delete(
        candidate_root, run_root, "nonterminal_candidate", relative,
        proof_kind=(
            "source_integrity_verified" if workspace is not None
            else "nonterminal_without_workspace"
        ),
        evidence={
            "candidate_id": candidate_id,
            "candidate_status": candidate.get("status"),
            "workspace_verified": workspace is not None,
        },
    )
    return {
        "ok": True,
        "removed": True,
        "candidate_id": candidate_id,
        "candidate_root": relative,
        "workspace_verified": workspace is not None,
        "cleaned_at": utc_timestamp(),
    }


@_serialize_cleanup(1)
def cleanup_terminal_candidate_artifacts(candidate_root, run_root, source_verifier,
                                         required_lightweight_paths=None):
    candidate_root = os.path.abspath(candidate_root)
    candidates_root = os.path.join(os.path.abspath(run_root), "candidates")
    if (
        os.path.islink(candidates_root) or not os.path.isdir(candidates_root) or
        not is_under(candidate_root, candidates_root) or
        os.path.realpath(candidate_root) == os.path.realpath(candidates_root) or
        os.path.islink(candidate_root) or not os.path.isdir(candidate_root)
    ):
        raise UnsafeCleanupError("terminal candidate target is outside its run or is unsafe: %s" % candidate_root)
    candidate_path = os.path.join(candidate_root, _contract_text("candidate.json"))
    if not os.path.isfile(candidate_path) or os.path.islink(candidate_path):
        raise UnsafeCleanupError("terminal candidate record is missing or unsafe: %s" % candidate_path)
    candidate = load_json(candidate_path)
    if candidate.get("status") not in _TERMINAL_CANDIDATE_STATES:
        raise UnsafeCleanupError("candidate is not terminal and cannot be reclaimed")
    persistent = candidate.get("persistent_worker")
    controller_termination = (
        persistent.get("controller_termination") if isinstance(persistent, dict) else None
    )
    if candidate.get("status") == "infrastructure_failed" and controller_termination:
        return cleanup_proven_terminal_infrastructure_candidate(
            candidate_root, run_root,
            required_lightweight_paths=required_lightweight_paths,
        )
    workspace_path = os.path.join(candidate_root, "workspace")
    if os.path.lexists(workspace_path):
        if os.path.islink(workspace_path) or not os.path.isdir(workspace_path):
            raise UnsafeCleanupError("terminal candidate workspace is not a real directory")
        return cleanup_abandoned_candidate(
            candidate_root, run_root, source_verifier,
            required_lightweight_paths=required_lightweight_paths,
        )
    source_guard = candidate.get("source_guard") or {}
    if not source_guard.get("ok"):
        raise UnsafeCleanupError(
            "terminal candidate without a private workspace lacks a successful persisted source guard"
        )
    persist_lightweight_candidate(candidate_root, candidate)
    return safe_delete_candidate_workspace(
        candidate_root, run_root, source_guard,
        required_lightweight_paths=required_lightweight_paths,
    )


@_serialize_cleanup(1)
def cleanup_abandoned_worker_epoch(epoch_root, run_root, source_verifier, termination_proof=None):
    epoch_root, run_root, _workers_root, _worker_root = _validated_worker_epoch_paths(
        epoch_root, run_root,
    )
    _require_remote_batch_quiescent(epoch_root)
    workspace = os.path.join(epoch_root, "workspace")
    if not os.path.lexists(workspace):
        return _safe_delete_partial_worker_epoch(epoch_root, run_root, termination_proof)
    if os.path.islink(workspace) or not os.path.isdir(workspace):
        raise UnsafeCleanupError("abandoned worker workspace is unsafe: %s" % workspace)
    metadata_path = os.path.join(workspace, _contract_text("workspace.json"))
    if not _regular_nonempty(metadata_path, workspace):
        if os.path.lexists(metadata_path):
            raise UnsafeCleanupError("abandoned worker epoch has invalid workspace metadata: %s" % metadata_path)
        return _safe_delete_partial_worker_epoch(epoch_root, run_root, termination_proof)
    source_guard = source_verifier(load_json(metadata_path))
    return safe_delete_worker_epoch(epoch_root, run_root, source_guard)


@_serialize_cleanup(1)
def cleanup_abandoned_controller_epoch(epoch_root, run_root, liveness_proof):
    proof_kind = _validate_controller_liveness_proof(liveness_proof)
    epoch_root, run_root, _controllers_root, _slot_root, run_device = (
        _validated_controller_epoch_paths(epoch_root, run_root)
    )
    _reject_unsafe_controller_epoch_tree(epoch_root, run_device)
    descriptor, lock_path = _acquire_released_controller_liveness_lock(
        epoch_root, run_device,
    )
    try:
        epoch_root, run_root, _controllers_root, _slot_root, run_device = (
        _validated_controller_epoch_paths(epoch_root, run_root)
        )
        _reject_unsafe_controller_epoch_tree(epoch_root, run_device)
        try:
            current_lock_stat = os.lstat(lock_path)
        except OSError as error:
            raise UnsafeCleanupError(
                "controller liveness lock disappeared before cleanup: %s" % error
            )
        if not os.path.samestat(current_lock_stat, os.fstat(descriptor)):
            raise UnsafeCleanupError(
                "controller liveness lock changed before cleanup: %s" % lock_path
            )
        relative = os.path.relpath(epoch_root, run_root).replace(os.sep, "/")
        manifest = _quarantine_and_delete(
            epoch_root, run_root, "controller_epoch", relative,
            proof_kind=proof_kind,
            evidence={"liveness_lock_acquired": True, "termination_proven": True},
        )
    finally:
        os.close(descriptor)
    return {
        "ok": True,
        "removed": True,
        "controller_epoch": relative,
        "proof_kind": proof_kind,
        "artifact_cleanup_complete": True,
        "artifact_cleanup": manifest.get("artifact_cleanup"),
        "cleaned_at": utc_timestamp(),
    }
