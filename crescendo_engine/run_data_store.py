"""One durable, incremental optimization dataset; no external dependencies.

Records are newline-delimited canonical JSON, chained by SHA256.  A record is
committed only after fsync.  Runtime readers ignore an unfinished final record;
imports and checkpoint evidence reject it.  This file never grants authority to
submit, resume or export OA: those operations still require their native proofs.
"""
from __future__ import print_function
from crescendo_engine.runtime import bind_text
_contract_text = bind_text()


import copy
import bisect
import hashlib
import json
import os
import stat
import threading

try:
    import fcntl
except ImportError:  # pragma: no cover - supported deployment is Linux
    fcntl = None

CORE_NAME = _contract_text("optimization.crescendo")
SCHEMA_VERSION = _contract_text("crescendo.optimization_dataset.v1")
RECORD_SCHEMA = _contract_text("crescendo.optimization_record.v1")
MAX_RECORD_BYTES = 64 * 1024 * 1024
TERMINAL_STATES = ("completed", "failed", "infrastructure_failed")
_ACTIVE_WRITERS = {}
_ACTIVE_WRITERS_LOCK = threading.RLock()


class RunDataError(RuntimeError):
    def __init__(self, message, detail=None):
        self.detail = detail or {}
        if self.detail:
            message += " [storage=%s]" % json.dumps(self.detail, sort_keys=True, separators=(",", ":"))
        super(RunDataError, self).__init__(message)


def _encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=True, allow_nan=False).encode("utf-8")


def _safe_stat(path):
    value = os.lstat(path)
    if not stat.S_ISREG(value.st_mode) or value.st_nlink != 1:
        raise RunDataError("optimization data must be a regular file without links")
    return value


def _open_read(path):
    expected = _safe_stat(path)
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    actual = os.fstat(fd)
    if not os.path.samestat(expected, actual) or actual.st_nlink != 1:
        os.close(fd)
        raise RunDataError("optimization data changed while opening")
    return os.fdopen(fd, "rb")


def _fsync_dir(path):
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _validated_candidate(row):
    if not isinstance(row, dict):
        raise RunDataError("optimization candidate must be an object")
    for name in ("generation", "index"):
        value = row.get(name)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise RunDataError("optimization candidate %s is invalid" % name)
    expected = "g%03d_c%04d" % (row["generation"], row["index"])
    if row.get("candidate_id") != expected:
        raise RunDataError("optimization candidate identity is invalid")
    if row.get("status") not in TERMINAL_STATES + ("planned", "running", "queued", "cancelled"):
        raise RunDataError("optimization candidate status is invalid")
    if not isinstance(row.get("values"), dict):
        raise RunDataError("optimization candidate parameters are invalid")
    _encoded(row)  # reject NaN/Inf and nonportable values before publishing
    return row


class RunDataReader(object):
    """Incrementally verify appended records and retain latest display rows."""
    def __init__(self, path):
        self.path = os.path.abspath(path)
        self._lock = threading.RLock()
        self._reset()

    def _reset(self):
        self.identity = None
        self.file_stamp = None
        self.offset = 0
        self.revision = 0
        self.head = None
        self.hasher = hashlib.sha256()
        self.metadata = None
        self.rows = {}
        self.changed_at = {}
        self.change_revisions = []
        self.change_ids = []
        self.result = {}
        self.status = {}
        self.tail_incomplete = False

    def _apply(self, document):
        required = {"schema_version", "revision", "previous_sha256", "kind", "payload", "sha256"}
        if not isinstance(document, dict) or set(document) != required:
            raise RunDataError("optimization record schema is invalid")
        if (document["schema_version"] != RECORD_SCHEMA
                or type(document["revision"]) is not int
                or document["revision"] != self.revision + 1
                or document["previous_sha256"] != self.head):
            raise RunDataError("optimization record revision/hash chain is invalid")
        payload = dict(document)
        checksum = payload.pop("sha256")
        if checksum != hashlib.sha256(_encoded(payload)).hexdigest():
            raise RunDataError("optimization record checksum is invalid")
        kind, value = document["kind"], document["payload"]
        if self.revision == 0 and kind != "metadata":
            raise RunDataError("optimization dataset has no metadata header")
        if kind == "metadata":
            if self.metadata is not None or not isinstance(value, dict):
                raise RunDataError("optimization metadata is invalid or repeated")
            if not isinstance(value.get("spec"), dict):
                raise RunDataError("optimization metadata spec is missing")
            self.metadata = value
        elif kind == "candidates":
            if not isinstance(value, list):
                raise RunDataError("optimization candidates record is invalid")
            seen = set()
            for row in value:
                _validated_candidate(row)
                identity = row["candidate_id"]
                if identity in seen:
                    raise RunDataError("optimization record has duplicate candidates")
                seen.add(identity)
                previous = self.rows.get(identity)
                if previous and previous.get("values") != row.get("values"):
                    raise RunDataError("optimization candidate parameters changed")
                if (previous and previous.get("status") in TERMINAL_STATES
                        and row.get("status") not in TERMINAL_STATES):
                    raise RunDataError("terminal optimization candidate became pending")
            for row in value:
                identity = row["candidate_id"]
                self.rows[identity] = row
                self.changed_at[identity] = document["revision"]
            self.change_revisions.append(document["revision"])
            self.change_ids.append(tuple(seen))
        elif kind in ("result", "status", "generation", "point"):
            if not isinstance(value, dict):
                raise RunDataError("optimization %s record is invalid" % kind)
            if kind == "result":
                self.result = value
            elif kind == "status":
                self.status = value
        else:
            raise RunDataError("optimization record kind is unsupported: %s" % kind)
        self.revision = document["revision"]
        self.head = checksum

    def read(self, since_revision=0, include_candidates=True, strict=False,
             stop_bytes=None, include_metadata=True):
        with self._lock:
            value = _safe_stat(self.path)
            identity = (value.st_dev, value.st_ino)
            stamp = (getattr(value, "st_mtime_ns", value.st_mtime),
                     getattr(value, "st_ctime_ns", value.st_ctime), value.st_size)
            reset = self.identity != identity or value.st_size < self.offset or (
                    self.file_stamp is not None and self.file_stamp != stamp and value.st_size <= self.offset) or (
                    stop_bytes is not None and stop_bytes < self.offset)
            if reset:
                self._reset()
                since_revision = 0
            self.identity = identity
            self.file_stamp = stamp
            self.tail_incomplete = False
            with _open_read(self.path) as stream:
                stream.seek(self.offset)
                while stop_bytes is None or self.offset < stop_bytes:
                    remaining = (stop_bytes - self.offset) if stop_bytes is not None else MAX_RECORD_BYTES + 1
                    raw = stream.readline(min(MAX_RECORD_BYTES + 1, remaining))
                    if not raw:
                        break
                    if len(raw) > MAX_RECORD_BYTES:
                        raise RunDataError("optimization record exceeds the size limit")
                    if not raw.endswith(b"\n"):
                        self.tail_incomplete = True
                        break
                    try:
                        document = json.loads(raw.decode("utf-8"))
                    except (UnicodeError, ValueError) as error:
                        raise RunDataError("optimization record cannot be read: %s" % error)
                    self._apply(document)
                    self.hasher.update(raw)
                    self.offset += len(raw)
            if stop_bytes is not None and self.offset != stop_bytes:
                raise RunDataError("checkpoint optimization data prefix is incomplete")
            if strict and self.tail_incomplete:
                raise RunDataError("optimization data has an incomplete final record")
            if self.metadata is None:
                raise RunDataError("optimization data metadata is missing")
            if since_revision > self.revision:
                since_revision = 0
                reset = True
            ordered = lambda rows: sorted(rows, key=lambda row: (row["generation"], row["index"]))
            first_change = bisect.bisect_right(self.change_revisions, since_revision)
            changed_ids = set(identity for identities in self.change_ids[first_change:]
                              for identity in identities)
            result = {
                "schema_version": SCHEMA_VERSION,
                "revision": self.revision, "result": copy.deepcopy(self.result),
                "status": copy.deepcopy(self.status), "committed_bytes": self.offset,
                "head_sha256": self.head, "prefix_sha256": self.hasher.hexdigest(),
                "tail_incomplete": self.tail_incomplete,
                "reset": reset,
                "changed_candidates": copy.deepcopy(ordered([self.rows[identity] for identity in changed_ids])),
            }
            if include_metadata:
                result["metadata"] = copy.deepcopy(self.metadata)
            if self.result:
                result["result"]["pareto_front"] = copy.deepcopy([
                    self.rows[identity] for identity in self.result.get("pareto_candidate_ids") or []
                    if identity in self.rows])
                result["result"]["representative"] = copy.deepcopy(self.rows.get(
                    self.result.get("representative_candidate_id")))
            if include_candidates:
                result["candidates"] = copy.deepcopy(ordered(self.rows.values()))
            return result


class RunDataStore(object):
    def __init__(self, path, metadata=None, writable=True):
        if not writable:
            raise ValueError("use RunDataReader for read-only optimization data")
        if fcntl is None:
            raise RunDataError("optimization data requires POSIX file locks")
        self.path = os.path.abspath(path)
        self.lock = threading.RLock()
        self.fd = None
        self.failed = False
        self._reference_datasets = {}
        created = not os.path.lexists(self.path)
        flags = os.O_RDWR | getattr(os, "O_NOFOLLOW", 0)
        if created:
            flags |= os.O_CREAT | os.O_EXCL
        self.fd = os.open(self.path, flags, 0o600)
        try:
            value = os.fstat(self.fd)
            if not stat.S_ISREG(value.st_mode) or value.st_nlink != 1:
                raise RunDataError("optimization data is not a safe regular file")
            fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            if not os.path.samestat(value, os.lstat(self.path)):
                raise RunDataError("optimization data changed before locking")
            self.reader = RunDataReader(self.path)
            if value.st_size:
                existing = self.reader.read(strict=True)
                if metadata is not None and existing["metadata"] != metadata:
                    raise RunDataError("optimization data belongs to another run")
                self.revision = existing["revision"]
                self.head = existing["head_sha256"]
                self.offset = existing["committed_bytes"]
                self.hasher = self.reader.hasher.copy()
            else:
                if metadata is None:
                    raise RunDataError("new optimization data needs metadata")
                self.revision, self.head, self.offset = 0, None, 0
                self.hasher = hashlib.sha256()
                self.expected_stamp = None
                self.append_event("metadata", metadata)
            self.expected_stamp = self._file_stamp()
            if created:
                _fsync_dir(os.path.dirname(self.path))
            with _ACTIVE_WRITERS_LOCK:
                _ACTIVE_WRITERS[self.path] = self
        except Exception:
            self.close()
            raise

    def append_event(self, kind, payload):
        with self.lock:
            if self.fd is None or self.failed:
                raise RunDataError("optimization data writer is unavailable")
            if kind == "candidates":
                for row in payload:
                    _validated_candidate(row)
            document = {"schema_version": RECORD_SCHEMA, "revision": self.revision + 1,
                        "previous_sha256": self.head, "kind": kind, "payload": payload}
            document["sha256"] = hashlib.sha256(_encoded(document)).hexdigest()
            raw = _encoded(document) + b"\n"
            if len(raw) > MAX_RECORD_BYTES:
                raise RunDataError("optimization record exceeds the size limit")
            try:
                self._check_held_writer("append")
                os.lseek(self.fd, self.offset, os.SEEK_SET)
                written = 0
                while written < len(raw):
                    amount = os.write(self.fd, raw[written:])
                    if amount <= 0:
                        raise RunDataError("optimization data write did not advance")
                    written += amount
                os.fsync(self.fd)
            except Exception:
                # No later checkpoint/raw cleanup may claim this record durable.
                self.failed = True
                raise
            self.revision += 1
            self.head = document["sha256"]
            self.offset += len(raw)
            self.hasher.update(raw)
            self.expected_stamp = self._file_stamp()
            return self.revision

    def _file_stamp(self):
        value = os.fstat(self.fd)
        return (getattr(value, "st_mtime_ns", value.st_mtime),
                getattr(value, "st_ctime_ns", value.st_ctime), value.st_size)

    def _check_held_writer(self, stage):
        """Validate the held file; timestamps are hints, bytes are authority.

        Callers hold self.lock. Normal appends only stat the file; a metadata
        change requires a stable full committed-prefix hash before accepting
        the new stamp, including bytes beyond an older checkpoint reference.
        """
        detail = {"stage": stage, "revision": self.revision, "expected_bytes": self.offset}
        if self.fd is None or self.failed:
            detail["reason"] = "writer_unavailable"
            raise RunDataError("optimization data writer is unavailable", detail)
        held = os.fstat(self.fd)
        current = os.lstat(self.path)
        detail.update(actual_bytes=current.st_size,
                      held_identity=[held.st_dev, held.st_ino],
                      path_identity=[current.st_dev, current.st_ino])
        if (not stat.S_ISREG(current.st_mode) or current.st_nlink != 1
                or not stat.S_ISREG(held.st_mode) or held.st_nlink != 1):
            detail["reason"] = "file_type_or_links"
            raise RunDataError("optimization data must be a regular file without links", detail)
        if not os.path.samestat(held, current):
            detail["reason"] = "identity"
            raise RunDataError("optimization data changed outside its held writer", detail)
        if held.st_size != self.offset or current.st_size != self.offset:
            detail["reason"] = "size"
            detail["held_bytes"] = held.st_size
            raise RunDataError("optimization data changed outside its held writer", detail)
        stamp = self._file_stamp()
        if self.expected_stamp is None or stamp == self.expected_stamp:
            return
        detail.update(expected_stamp=self.expected_stamp, actual_stamp=stamp)
        digest = self._disk_prefix_sha256(self.offset)
        if digest != self.hasher.hexdigest():
            detail["reason"] = "checksum_after_stamp_change"
            raise RunDataError("optimization data prefix checksum changed outside its writer", detail)
        if self._file_stamp() != stamp:
            detail["reason"] = "stamp_changed_during_verification"
            raise RunDataError("optimization data changed while verifying its prefix", detail)
        self.expected_stamp = stamp

    def _disk_prefix_sha256(self, stop_bytes):
        """Strict evidence hashes bytes even when same-tick stat is unchanged.

        Keep the parsed dataset cache and UI's incremental reader. Only the
        checkpoint/cleanup-authority APIs use this bounded-memory disk check.
        A separate reader leaves the exclusive writer's position untouched.
        """
        if self.fd is None or type(stop_bytes) is not int or not 0 < stop_bytes <= self.offset:
            raise RunDataError("optimization data prefix length is invalid")
        held = os.fstat(self.fd)
        digest = hashlib.sha256()
        with _open_read(self.path) as stream:
            before = os.fstat(stream.fileno())
            if not os.path.samestat(held, before) or before.st_size != self.offset:
                raise RunDataError("optimization data changed outside its held writer")
            remaining = stop_bytes
            while remaining:
                data = stream.read(min(remaining, 1024 * 1024))
                if not data:
                    raise RunDataError("optimization data prefix is incomplete")
                digest.update(data)
                remaining -= len(data)
            after = os.fstat(stream.fileno())
            current = _safe_stat(self.path)
            before_stamp = (getattr(before, "st_mtime_ns", before.st_mtime),
                            getattr(before, "st_ctime_ns", before.st_ctime), before.st_size)
            after_stamp = (getattr(after, "st_mtime_ns", after.st_mtime),
                           getattr(after, "st_ctime_ns", after.st_ctime), after.st_size)
            if (not os.path.samestat(before, after) or not os.path.samestat(after, current)
                    or not os.path.samestat(after, os.fstat(self.fd))
                    or after.st_nlink != 1 or before_stamp != after_stamp):
                raise RunDataError("optimization data changed while verifying its prefix")
        return digest.hexdigest()

    def commit_candidates(self, rows):
        return self.append_event("candidates", copy.deepcopy(list(rows)))

    def commit_result(self, result):
        # Candidate history already lives in incremental records, once.
        summary = copy.deepcopy(dict((key, value) for key, value in result.items()
                                    if key not in ("candidates", "pareto_front", "representative", "best")))
        summary["pareto_candidate_ids"] = [row["candidate_id"] for row in result.get("pareto_front") or []]
        representative = result.get("representative")
        summary["representative_candidate_id"] = representative.get("candidate_id") if representative else None
        return self.append_event("result", summary)

    def reference(self):
        with self.lock:
            if self.failed:
                raise RunDataError("failed optimization data cannot be checkpoint evidence")
            self._check_held_writer("reference")
            if self._disk_prefix_sha256(self.offset) != self.hasher.hexdigest():
                raise RunDataError("optimization data prefix checksum changed before checkpoint evidence")
            reference = {"schema_version": SCHEMA_VERSION, "path": self.path,
                    "revision": self.revision, "bytes": self.offset,
                    "head_sha256": self.head, "prefix_sha256": self.hasher.hexdigest()}
            # This process owns the exclusive writer. Verify newly appended
            # bytes once, then reuse this immutable prefix for run+anchor+WAL
            # verification. A later process/resume always verifies from disk.
            if self.revision not in self._reference_datasets:
                dataset = self.reader.read(strict=True, stop_bytes=self.offset,
                                           since_revision=self.reader.revision,
                                           include_candidates=False, include_metadata=False)
                dataset["metadata"] = self.reader.metadata
                # Parsed records are immutable in this reader: later updates
                # replace dictionaries. Fixed-prefix snapshots share those
                # immutable rows rather than copying four complete histories.
                dataset["candidates"] = list(self.reader.rows.values())
                self._reference_datasets[self.revision] = (reference, dataset)
                for revision in sorted(self._reference_datasets)[:-4]:
                    del self._reference_datasets[revision]
            return reference

    def close(self):
        with _ACTIVE_WRITERS_LOCK:
            if _ACTIVE_WRITERS.get(self.path) is self:
                del _ACTIVE_WRITERS[self.path]
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None


def read_run_data(path, since_revision=0):
    return RunDataReader(path).read(since_revision=since_revision)


def compact_result_summary(result, reference=None):
    value = dict((key, copy.deepcopy(item)) for key, item in result.items()
                 if key not in ("candidates", "pareto_front", "representative", "best"))
    value["candidate_store"] = reference
    value["pareto_candidate_ids"] = [row["candidate_id"] for row in result.get("pareto_front") or []]
    representative = result.get("representative") or result.get("best")
    value["representative_candidate_id"] = representative.get("candidate_id") if representative else None
    return value


def load_dataset(path):
    return RunDataReader(path).read(strict=True)


def close_run_data(path):
    with _ACTIVE_WRITERS_LOCK:
        writer = _ACTIVE_WRITERS.get(os.path.abspath(path))
    if writer is not None:
        writer.close()


def export_dataset(source, target):
    dataset = RunDataReader(source).read()
    target = os.path.abspath(target)
    if os.path.realpath(source) == os.path.realpath(target):
        raise RunDataError("export target must differ from the active dataset")
    if os.path.lexists(target):
        _safe_stat(target)
    temporary = target + ".tmp-%d-%s" % (os.getpid(), os.urandom(6).hex())
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with _open_read(source) as stream, os.fdopen(fd, "wb") as destination:
            remaining = dataset["committed_bytes"]
            digest = hashlib.sha256()
            while remaining:
                block = stream.read(min(remaining, 1024 * 1024))
                if not block:
                    raise RunDataError("optimization data changed during export")
                destination.write(block)
                digest.update(block)
                remaining -= len(block)
            destination.flush()
            os.fsync(destination.fileno())
        if digest.hexdigest() != dataset["prefix_sha256"]:
            raise RunDataError("optimization data prefix changed during export")
        load_dataset(temporary)
        os.replace(temporary, target)
        _fsync_dir(os.path.dirname(target))
    finally:
        if os.path.lexists(temporary):
            os.unlink(temporary)
    return target


def dataset_from_reference(reference, embedded=None):
    if not isinstance(reference, dict) or reference.get("schema_version") != SCHEMA_VERSION:
        raise RunDataError("checkpoint optimization data reference is invalid")
    for name in ("revision", "bytes"):
        if type(reference.get(name)) is not int or reference[name] < 1:
            raise RunDataError("checkpoint optimization data %s is invalid" % name)
    for name in ("head_sha256", "prefix_sha256"):
        value = reference.get(name)
        if not isinstance(value, str) or len(value) != 64:
            raise RunDataError("checkpoint optimization data checksum is invalid")
    if embedded is not None:
        if not isinstance(embedded, str):
            raise RunDataError("embedded checkpoint optimization data is invalid")
        raw = embedded.encode("utf-8")
        if len(raw) != reference["bytes"] or hashlib.sha256(raw).hexdigest() != reference["prefix_sha256"]:
            raise RunDataError("embedded checkpoint optimization data checksum is invalid")
        reader = RunDataReader(reference.get("path") or "embedded")
        for line in raw.splitlines(True):
            if not line.endswith(b"\n") or len(line) > MAX_RECORD_BYTES:
                raise RunDataError("embedded optimization record is incomplete")
            reader._apply(json.loads(line.decode("utf-8")))
        dataset = {"metadata": reader.metadata, "candidates": list(reader.rows.values()),
                   "revision": reader.revision, "head_sha256": reader.head}
    else:
        path = reference.get("path") or ""
        with _ACTIVE_WRITERS_LOCK:
            writer = _ACTIVE_WRITERS.get(path)
        cached = writer._reference_datasets.get(reference["revision"]) if writer else None
        if cached is not None and cached[0] == reference:
            with writer.lock:
                writer._check_held_writer("cached_reference")
                if writer._disk_prefix_sha256(reference["bytes"]) != reference["prefix_sha256"]:
                    raise RunDataError("checkpoint optimization data prefix checksum is invalid")
                dataset = cached[1]
        else:
            reader = RunDataReader(path)
            dataset = reader.read(stop_bytes=reference["bytes"], strict=True)
        if dataset["prefix_sha256"] != reference["prefix_sha256"]:
            raise RunDataError("checkpoint optimization data prefix checksum is invalid")
    if dataset["revision"] != reference["revision"] or dataset["head_sha256"] != reference["head_sha256"]:
        raise RunDataError("checkpoint optimization data identity is invalid")
    return dataset
