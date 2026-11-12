from __future__ import print_function

import errno
import hashlib
import json
import os
import socket
import stat
import uuid

try:
    import fcntl
except ImportError:  # pragma: no cover - Cadence deployments are POSIX/Linux.
    fcntl = None

from crescendo_engine.runtime import bind_text
_contract_text = bind_text()

from .utils import atomic_write_json, ensure_dir, load_json, utc_timestamp


PENDING_SCHEMA_VERSION = _contract_text("crescendo.continuation_pending.v1")
PENDING_NAME = _contract_text("pending.json")


class ContinuationLeaseBusy(RuntimeError):
    pass


def _fsync_directory(path):
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _ensure_checkpoint_directory(path, parent, mode, description, create):
    created = False
    if not os.path.lexists(path) and create:
        try:
            os.mkdir(path, mode)
            created = True
        except OSError as error:
            if error.errno != errno.EEXIST:
                raise
    if os.path.lexists(path) and (
        os.path.islink(path) or not os.path.isdir(path)
    ):
        raise RuntimeError("%s must be a real directory" % description)
    if created:
        _fsync_directory(parent)


def new_continuation_id():
    return "continuation-%s" % uuid.uuid4().hex


def checkpoint_continuation_id(checkpoint):
    manifest = checkpoint.get("manifest") or {}
    continuation_id = manifest.get("continuation_id")
    if continuation_id:
        return str(continuation_id)
    payload = {
        "schema_version": checkpoint.get("schema_version"),
        "origin_run_id": manifest.get("origin_run_id"),
        "algorithm": manifest.get("algorithm"),
        "prepared_schema_sha256": manifest.get("prepared_schema_sha256"),
        "parameter_ids": checkpoint.get("parameter_ids") or [],
        "spec": checkpoint.get("spec") or {},
    }
    encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return "legacy-%s" % hashlib.sha256(encoded).hexdigest()


def continuation_checkpoint_path(runtime_root, continuation_id, create=False):
    runtime_root = os.path.realpath(os.path.abspath(os.path.expanduser(runtime_root)))
    continuation_id = str(continuation_id or "")
    if not continuation_id:
        raise ValueError("continuation_id is required")
    if not os.path.isdir(runtime_root):
        if not create:
            return os.path.join(
                runtime_root, ".continuation_checkpoints",
                hashlib.sha256(continuation_id.encode("utf-8")).hexdigest(),
                _contract_text("checkpoint.json"),
            )
        ensure_dir(runtime_root)
    if os.path.islink(runtime_root) or not os.path.isdir(runtime_root):
        raise RuntimeError("runtime_root must be a real directory")

    anchor_root = os.path.join(runtime_root, ".continuation_checkpoints")
    digest = hashlib.sha256(continuation_id.encode("utf-8")).hexdigest()
    continuation_root = os.path.join(anchor_root, digest)
    _ensure_checkpoint_directory(
        anchor_root, runtime_root, 0o700, "continuation checkpoint root", create,
    )
    _ensure_checkpoint_directory(
        continuation_root, anchor_root, 0o700,
        "continuation checkpoint directory", create,
    )
    return os.path.join(continuation_root, _contract_text("checkpoint.json"))


def continuation_pending_path(runtime_root, continuation_id, create=False):
    return os.path.join(
        os.path.dirname(continuation_checkpoint_path(
            runtime_root, continuation_id, create=create,
        )),
        PENDING_NAME,
    )


def _canonical_sha256(value):
    encoded = json.dumps(value, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def continuation_schema_sha256(schema):
    return _canonical_sha256(schema)


def _pending_payload(document):
    payload = dict(document)
    payload.pop("journal_sha256", None)
    return payload


def build_continuation_pending(checkpoint, continuation_id, revision,
                               parent_payload_sha256, schema_sha256):
    document = {
        "schema_version": PENDING_SCHEMA_VERSION,
        "continuation_id": str(continuation_id),
        "revision": int(revision),
        "parent_payload_sha256": parent_payload_sha256,
        "checkpoint_payload_sha256": checkpoint.get("payload_sha256"),
        "continuation_schema_sha256": str(schema_sha256),
        "checkpoint": checkpoint,
        "created_at": utc_timestamp(),
    }
    document["journal_sha256"] = _canonical_sha256(document)
    return document


def validate_continuation_pending(document):
    required = {
        "schema_version", "continuation_id", "revision",
        "parent_payload_sha256", "checkpoint_payload_sha256",
        "continuation_schema_sha256", "checkpoint", "created_at", "journal_sha256",
    }
    if not isinstance(document, dict) or set(document) != required:
        raise RuntimeError("continuation pending journal has an invalid schema")
    if document.get("schema_version") != PENDING_SCHEMA_VERSION:
        raise RuntimeError("continuation pending journal schema version is unsupported")
    if not isinstance(document.get("continuation_id"), str) or not document["continuation_id"]:
        raise RuntimeError("continuation pending journal identity is malformed")
    revision = document.get("revision")
    if isinstance(revision, bool) or not isinstance(revision, int) or revision < 0:
        raise RuntimeError("continuation pending journal revision is malformed")
    hexadecimal = _is_sha256
    for name in (
        "checkpoint_payload_sha256", "continuation_schema_sha256", "journal_sha256",
    ):
        if not hexadecimal(document.get(name)):
            raise RuntimeError("continuation pending journal %s is malformed" % name)
    parent = document.get("parent_payload_sha256")
    if parent is not None and not hexadecimal(parent):
        raise RuntimeError("continuation pending journal parent checksum is malformed")
    checkpoint = document.get("checkpoint")
    if not isinstance(checkpoint, dict):
        raise RuntimeError("continuation pending journal checkpoint is malformed")
    if checkpoint.get("payload_sha256") != document.get("checkpoint_payload_sha256"):
        raise RuntimeError("continuation pending journal checkpoint checksum does not match")
    expected = _canonical_sha256(_pending_payload(document))
    if expected != document.get("journal_sha256"):
        raise RuntimeError("continuation pending journal checksum does not match")
    return document


def _is_sha256(value):
    if not isinstance(value, str) or len(value) != 64:
        return False
    try:
        int(value, 16)
    except ValueError:
        return False
    return True


def persist_continuation_pending(runtime_root, continuation_id, document):
    validate_continuation_pending(document)
    path = continuation_pending_path(runtime_root, continuation_id, create=True)
    if os.path.lexists(path):
        if os.path.islink(path) or not os.path.isfile(path):
            raise RuntimeError("continuation pending journal is not a safe regular file")
        raise RuntimeError("an unresolved continuation pending journal already exists")
    atomic_write_json(path, document)
    if os.path.islink(path) or not os.path.isfile(path):
        raise RuntimeError("continuation pending journal was not persisted safely")
    persisted = load_json(path)
    validate_continuation_pending(persisted)
    if (
        persisted.get("journal_sha256") != document.get("journal_sha256")
        or persisted.get("checkpoint_payload_sha256")
        != document.get("checkpoint_payload_sha256")
    ):
        raise RuntimeError("continuation pending journal verification failed")
    return path


def clear_continuation_pending(runtime_root, continuation_id):
    path = continuation_pending_path(runtime_root, continuation_id, create=False)
    if not os.path.lexists(path):
        return False
    if os.path.islink(path) or not os.path.isfile(path):
        raise RuntimeError("continuation pending journal is not a safe regular file")
    os.unlink(path)
    _fsync_directory(os.path.dirname(path))
    return True


def _process_start_ticks(pid):
    try:
        with open("/proc/%d/stat" % int(pid), "r") as stream:
            raw = stream.read()
        closing = raw.rfind(")")
        fields = raw[closing + 2:].split()
        return int(fields[19])
    except (IOError, OSError, IndexError, TypeError, ValueError):
        return None


def _read_locked_document(descriptor):
    os.lseek(descriptor, 0, os.SEEK_SET)
    chunks = []
    while True:
        chunk = os.read(descriptor, 65536)
        if not chunk:
            break
        chunks.append(chunk)
    if not chunks:
        return None
    try:
        return json.loads(b"".join(chunks).decode("utf-8"))
    except (TypeError, ValueError):
        return {"state": "unreadable"}


def _write_locked_document(descriptor, document):
    payload = (json.dumps(document, separators=(",", ":"), sort_keys=True) + "\n").encode("utf-8")
    os.lseek(descriptor, 0, os.SEEK_SET)
    os.ftruncate(descriptor, 0)
    offset = 0
    while offset < len(payload):
        offset += os.write(descriptor, payload[offset:])
    os.fsync(descriptor)


class ContinuationLease(object):
    def __init__(self, runtime_root, continuation_id, run_root=None, lease_kind="continuation"):
        if fcntl is None:
            raise RuntimeError("continuation leases require POSIX fcntl.flock support")
        self.runtime_root = os.path.realpath(os.path.abspath(os.path.expanduser(runtime_root)))
        self.continuation_id = str(continuation_id or "")
        if not self.continuation_id:
            raise ValueError("continuation_id is required")
        self.run_root = os.path.abspath(run_root) if run_root else None
        self.lease_kind = lease_kind
        self.directory = os.path.join(self.runtime_root, ".continuation_leases")
        digest = hashlib.sha256(self.continuation_id.encode("utf-8")).hexdigest()
        self.path = os.path.join(self.directory, "%s.lock" % digest)
        self.descriptor = None
        self.token = uuid.uuid4().hex
        self.metadata = None
        self.recovered_stale_owner = None

    @property
    def held(self):
        return self.descriptor is not None

    def acquire(self):
        if self.held:
            return self
        ensure_dir(self.runtime_root)
        if os.path.lexists(self.directory) and os.path.islink(self.directory):
            raise RuntimeError("continuation lease directory must not be a symlink")
        ensure_dir(self.directory)
        flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(self.path, flags, 0o600)
        locked = False
        try:
            if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                raise RuntimeError("continuation lease path is not a regular file")
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except (IOError, OSError) as error:
                if error.errno not in (errno.EACCES, errno.EAGAIN):
                    raise
                owner = _read_locked_document(descriptor) or {}
                if self.lease_kind == "runtime_real_eda_optimization":
                    message = "another real EDA optimization is already running for this runtime_root"
                else:
                    message = "checkpoint continuation is already running"
                raise ContinuationLeaseBusy(
                    "%s (pid=%s, run_root=%s, acquired_at=%s)" % (
                        message,
                        owner.get("pid"), owner.get("run_root"), owner.get("acquired_at"),
                    )
                )
            locked = True
            previous = _read_locked_document(descriptor)
            if previous and previous.get("state") == "held":
                self.recovered_stale_owner = previous
            self.descriptor = descriptor
            self.metadata = {
                "schema_version": _contract_text("crescendo.continuation_lease.v1"),
                "state": "held",
                "lease_kind": self.lease_kind,
                "continuation_id": self.continuation_id,
                "token": self.token,
                "pid": os.getpid(),
                "process_start_ticks": _process_start_ticks(os.getpid()),
                "hostname": socket.gethostname(),
                "run_root": self.run_root,
                "acquired_at": utc_timestamp(),
            }
            if self.recovered_stale_owner:
                self.metadata["recovered_stale_owner"] = {
                    "pid": self.recovered_stale_owner.get("pid"),
                    "process_start_ticks": self.recovered_stale_owner.get("process_start_ticks"),
                    "hostname": self.recovered_stale_owner.get("hostname"),
                    "run_root": self.recovered_stale_owner.get("run_root"),
                    "acquired_at": self.recovered_stale_owner.get("acquired_at"),
                }
            _write_locked_document(descriptor, self.metadata)
            return self
        except Exception:
            if locked:
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_UN)
                except (IOError, OSError):
                    pass
            os.close(descriptor)
            self.descriptor = None
            raise

    def release(self):
        descriptor = self.descriptor
        if descriptor is None:
            return
        try:
            document = dict(self.metadata or {})
            document.update({"state": "released", "released_at": utc_timestamp()})
            _write_locked_document(descriptor, document)
            self.metadata = document
        except (IOError, OSError, TypeError, ValueError) as error:
            self.release_error = "%s: %s" % (error.__class__.__name__, error)
        finally:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)
                self.descriptor = None

    def __enter__(self):
        return self.acquire()

    def __exit__(self, _exception_type, _exception, _traceback):
        self.release()


class RuntimeOptimizationLease(ContinuationLease):
    def __init__(self, runtime_root, run_root=None):
        ContinuationLease.__init__(
            self, runtime_root, "runtime-real-eda-optimization-v1", run_root=run_root,
            lease_kind="runtime_real_eda_optimization",
        )
