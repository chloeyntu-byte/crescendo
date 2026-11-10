"""One bounded user-facing run log; JSON files remain machine evidence."""
from __future__ import print_function

from collections import OrderedDict
import datetime
import fcntl
import json
import os
import stat
import threading


LOG_NAME = "crescendo.log"
MAX_EVENT_BYTES = 65536
MAX_LOG_BYTES = 16 * 1024 * 1024
_lock = threading.RLock()
_last_status = OrderedDict()


def _text(value, limit=16384):
    text = str(value if value is not None else "")
    return "".join(c for c in text if ord(c) >= 32 or c in "\t\n\r")[:limit]


def _write_all(descriptor, raw):
    offset = 0
    while offset < len(raw):
        count = os.write(descriptor, raw[offset:])
        if count <= 0:
            raise IOError("log write made no progress")
        offset += count


def _encode_event(event):
    raw = json.dumps(event, sort_keys=True, ensure_ascii=False, default=str).encode("utf-8")
    if len(raw) >= MAX_EVENT_BYTES:
        original = json.dumps(event.get("details"), default=str, ensure_ascii=False)
        event["details"] = {"truncated": True, "text": original[:8000]}
        event["message"] = event["message"][:8000]
        raw = json.dumps(event, sort_keys=True, ensure_ascii=False).encode("utf-8")
    # Escaped control characters and non-ASCII strings can have different byte
    # widths. Bound again using serialized bytes; never emit partial JSON.
    while len(raw) >= MAX_EVENT_BYTES:
        event["details"]["text"] = event["details"]["text"][:len(event["details"]["text"]) // 2]
        event["message"] = event["message"][:len(event["message"]) // 2]
        raw = json.dumps(event, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return raw + b"\n"


def _first_failure_records(descriptor):
    records = []
    error_seen = primary_seen = False
    os.lseek(descriptor, 0, os.SEEK_SET)
    # Only scan at bounded compaction boundaries, on the same verified inode.
    # No sidecar files; each process sees previous preserved failures too.
    with os.fdopen(os.dup(descriptor), "rb") as stream:
        for line in stream:
            try:
                row = json.loads(line.decode("utf-8"))
            except (ValueError, UnicodeError):
                continue
            if not isinstance(row, dict):
                continue
            is_error = not error_seen and row.get("level") == "ERROR"
            is_primary = not primary_seen and isinstance(row.get("details"), dict) and bool(row["details"].get("primary_failure"))
            if is_error or is_primary:
                records.append(line if line.endswith(b"\n") else line + b"\n")
                error_seen = error_seen or is_error
                primary_seen = primary_seen or is_primary
            if error_seen and primary_seen:
                break
    return records


def append_run_event(run_root, stage, message, details=None, level="INFO"):
    """Never replace a primary EDA/storage failure with a logging failure."""
    descriptor = None
    try:
        root = os.path.abspath(run_root)
        if os.path.realpath(root) != root or not os.path.isdir(root):
            return False
        event = {"time": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                 "level": _text(level, 24), "stage": _text(stage, 160), "message": _text(message)}
        if details is not None:
            event["details"] = details
        raw = _encode_event(event)
        with _lock:
            descriptor = os.open(os.path.join(root, LOG_NAME), os.O_RDWR | os.O_APPEND | os.O_CREAT |
                                 getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0), 0o600)
            identity = os.fstat(descriptor)
            if not stat.S_ISREG(identity.st_mode) or identity.st_nlink != 1 or identity.st_uid != os.getuid():
                return False
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            # No rotated files. Retain the initial cause/identity and a recent
            # tail; record compaction rather than silently truncating history.
            size = os.fstat(descriptor).st_size
            if size > MAX_LOG_BYTES:
                failures = _first_failure_records(descriptor)
                os.lseek(descriptor, 0, os.SEEK_SET)
                head = os.read(descriptor, MAX_EVENT_BYTES)
                if not head.endswith(b"\n"):
                    head = head.rpartition(b"\n")[0] + b"\n"
                os.lseek(descriptor, max(0, size - MAX_LOG_BYTES // 2), os.SEEK_SET)
                tail = os.read(descriptor, MAX_LOG_BYTES // 2)
                tail = tail.partition(b"\n")[2]
                os.ftruncate(descriptor, 0)
                pinned = b"".join(line for line in failures if line not in head and line not in tail)
                _write_all(descriptor, head + pinned + b'{"stage":"log_compaction","message":"Initial evidence, first error/primary failure and recent events retained; middle progress compacted"}\n' + tail)
            _write_all(descriptor, raw)
        return True
    except (IOError, OSError, ValueError, TypeError):
        return False
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _shutdown_summary(value):
    workers = []
    for row in value.get("workers") or []:
        result = row.get("result") or {}
        cleanup = result.get("cleanup") or {}
        workers.append({"worker_index": row.get("worker_index"), "ok": row.get("ok"),
                        "process_termination_proven": result.get("process_termination_proven", result.get("termination_proven")),
                        "remote_stop_proven": result.get("remote_stop_proven"),
                        "artifact_cleanup_complete": result.get("artifact_cleanup_complete", cleanup.get("ok")),
                        "cleanup": dict((key, cleanup.get(key)) for key in
                                        ("ok", "state", "error", "path", "remaining", "detail") if key in cleanup),
                        "cleanup_attempt_number": result.get("cleanup_attempt_number"),
                        "cleanup_attempt_history": result.get("cleanup_attempt_history"),
                        "cleanup_recovered_from_attempts": result.get("cleanup_recovered_from_attempts"),
                        "pending_recovery": cleanup.get("pending_recovery"),
                        "error": row.get("error") or result.get("close_error")})
    return {"workers": workers, "final_cleanup": value.get("final_cleanup")}


def observe_saved_json(path, value):
    """Observe only known durable run records, never arbitrary config/env JSON."""
    if not isinstance(value, dict):
        return
    name = os.path.basename(path)
    root = os.path.dirname(os.path.abspath(path))
    if name == "status.json" and value.get("schema_version") == "crescendo.run_status.v1":
        fields = ("run_id", "display_name", "state", "message", "generation", "total_generations",
                  "execution_backend", "completed_candidates", "failed_candidates", "infrastructure_failed_candidates",
                  "maestro_max_jobs", "effective_parallel_jobs", "primary_failure", "shared_preparation_failure", "cleanup_failure", "final_cleanup")
        details = dict((key, value[key]) for key in fields if key in value)
        signature = json.dumps(details, sort_keys=True, default=str)
        with _lock:
            if _last_status.get(root) == signature:
                return
            _last_status[root] = signature
            _last_status.move_to_end(root)
            if len(_last_status) > 256:
                _last_status.popitem(last=False)
        append_run_event(root, value.get("state") or "status", value.get("message"), details,
                         "ERROR" if value.get("state") == "failed" else "INFO")
    elif name == "startup_failure.json" and value.get("schema_version") == "crescendo.startup_failure.v1":
        details = dict((key, value[key]) for key in ("stage", "native_error", "diagnostic", "log_paths", "loading_file", "last_successful_step") if key in value)
        append_run_event(root, value.get("stage") or "startup", value.get("native_error") or value.get("message"), details, "ERROR")
    elif name == "server_error.json":
        append_run_event(root, "run_failure", value.get("message"),
                         dict((key, _text(value[key], 12000)) for key in ("error_type", "traceback", "remote_traceback") if value.get(key)), "ERROR")
    elif name == "worker_shutdown.json" and value.get("schema_version") == "crescendo.worker_shutdown.v1":
        ok = all(row.get("ok") is True for row in value.get("workers") or [])
        if value.get("final_cleanup") is not None:
            ok = ok and value["final_cleanup"].get("artifact_cleanup_complete") is True
        append_run_event(root, "cleanup", "Owned tasks stopped and artifacts cleaned" if ok else "Owned task shutdown/artifact cleanup failed", _shutdown_summary(value), "INFO" if ok else "ERROR")
    elif name == "candidate.json" and value.get("candidate_id") == os.path.basename(root) and os.path.basename(os.path.dirname(root)) == "candidates":
        fields = ("candidate_id", "status", "execution_backend", "failure_stage", "error", "execution_started", "shared_preparation_failed", "metrics")
        append_run_event(os.path.dirname(os.path.dirname(root)), "candidate", value.get("status"),
                         dict((key, value[key]) for key in fields if key in value),
                         "ERROR" if value.get("status") in ("failed", "infrastructure_failed") else "INFO")
