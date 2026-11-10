"""Run-owned numeric netlist generation; solver execution is outside this lock."""
from __future__ import print_function

import json
import hashlib
import os
import re
import stat
import threading
import time
import uuid

from .utils import atomic_write_json, ensure_dir

SCHEMA = "crescendo.shared_maestro.v1"
REFERENCE = "shared_maestro_reference.json"
OWNER = "owner.json"
REGISTRY = "epochs.json"
_TOKEN = re.compile(r"^[a-f0-9]{32}$")
_CANDIDATE = re.compile(r"^g[0-9]{3,}_c[0-9]{4,}$")


def validate_static_file_inputs(task):
    """Shared by producer and execution gates; old manifests perform no IO."""
    rows = task.get("static_file_inputs") or []
    if not isinstance(rows, list) or len(rows) > 512:
        raise ValueError("Static native file input identity list is invalid")
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("Static native file input identity is invalid")
        path, reference = row.get("path"), row.get("reference_path") or row.get("path")
        if (not isinstance(path, str) or not os.path.isabs(path) or not isinstance(reference, str)
                or not os.path.isabs(reference) or os.path.realpath(reference) != path
                or os.path.realpath(path) != path):
            raise ValueError("Static native file input location changed")
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0))
        with os.fdopen(descriptor, "rb") as stream:
            before = os.fstat(stream.fileno())
            if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1
                    or before.st_size > 64*1024*1024 or before.st_size != row.get("bytes")):
                raise ValueError("Static native file input changed: " + path)
            digest, read_bytes = hashlib.sha256(), 0
            while True:
                chunk = stream.read(256*1024)
                if not chunk:
                    break
                read_bytes += len(chunk)
                if read_bytes > 64*1024*1024:
                    raise ValueError("Static native file input grew beyond its identity bound")
                digest.update(chunk)
            after = os.fstat(stream.fileno())
        if (digest.hexdigest() != row.get("sha256") or not os.path.samestat(before, after)
                or before.st_mtime != after.st_mtime or before.st_ctime != after.st_ctime
                or os.path.realpath(reference) != path or not os.path.samestat(before,os.stat(path))):
            raise ValueError("Static native file input changed: " + path)
    return True


def _load(path):
    if os.path.realpath(path) != os.path.abspath(path):
        raise ValueError("Shared Maestro identity traverses a link")
    before = os.lstat(path)
    if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_size > 1024 * 1024:
        raise ValueError("Shared Maestro identity is not a bounded single-link file")
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0))
    with os.fdopen(descriptor, "r") as stream:
        if not os.path.samestat(before, os.fstat(stream.fileno())):
            raise ValueError("Shared Maestro identity changed while opening")
        value = json.load(stream)
    if not isinstance(value, dict):
        raise ValueError("Shared Maestro identity is not an object")
    return value


def owner_records(run_root):
    """Only a durable explicit epoch registry authorizes shared view cleanup."""
    run_root = os.path.abspath(run_root)
    base = os.path.join(run_root, "private_netlist", "maestro_shared")
    registry = os.path.join(base, REGISTRY)
    if not os.path.lexists(registry):
        return []
    value = _load(registry)
    tokens = value.get("epochs")
    if (value.get("schema_version") != SCHEMA or value.get("run_root") != run_root
            or not isinstance(tokens, list) or len(tokens) > 128 or len(tokens) != len(set(tokens))):
        raise ValueError("Shared Maestro epoch registry is invalid")
    result = []
    for token in tokens:
        if not isinstance(token, str) or not _TOKEN.fullmatch(token):
            raise ValueError("Shared Maestro epoch token is invalid")
        root = os.path.join(base, token)
        row = _load(os.path.join(root, OWNER))
        if (row.get("schema_version") != SCHEMA or row.get("token") != token
                or row.get("run_root") != run_root or row.get("root") != root
                or row.get("workspace_root") != os.path.join(root, "workspace")):
            raise ValueError("Shared Maestro owner does not match its run and epoch")
        result.append(row)
    return result


def candidate_reference(root):
    """Validate both identities and the exact candidate/run/shared layout."""
    root = os.path.abspath(root)
    path = os.path.join(root, REFERENCE)
    if not os.path.lexists(path):
        return None
    candidate = os.path.dirname(root)
    identifier = os.path.basename(candidate)
    candidates = os.path.dirname(candidate)
    run_root = os.path.dirname(candidates)
    if (os.path.basename(root) != "simulation" or os.path.basename(candidates) != "candidates"
            or not _CANDIDATE.fullmatch(identifier) or os.path.realpath(root) != root):
        raise ValueError("Shared Maestro candidate reference has an invalid layout")
    row = _load(path)
    owner = next((item for item in owner_records(run_root) if item["token"] == row.get("token")), None)
    if (row.get("schema_version") != SCHEMA or row.get("candidate_id") != identifier
            or row.get("run_root") != run_root or row.get("candidate_root") != root or owner is None
            or row.get("workspace_root") != owner["workspace_root"]):
        raise ValueError("Shared Maestro candidate owner or token does not match")
    return dict(owner, candidate_id=identifier)


def write_reference(root, candidate_id, owner):
    atomic_write_json(os.path.join(root, REFERENCE), {
        "schema_version": SCHEMA, "token": owner["token"], "run_root": owner["run_root"],
        "candidate_root": root, "candidate_id": candidate_id,
        "workspace_root": owner["workspace_root"]})
    candidate_reference(root)


class SharedMaestroProducer(object):
    """One suite per adapter epoch; repeated candidates overwrite numeric values."""

    def __init__(self, run_root, prepared, parameters, config, spec, cancel_check=None,
                 progress_callback=None):
        from .variable_scope_contract import require_unique_run_variables
        require_unique_run_variables((prepared or {}).get("setup") or {},
                                     action="prepare shared Maestro SCS generation", multiplicity_only=True)
        self.run_root = os.path.abspath(run_root)
        self.prepared, self.parameters, self.config, self.spec = prepared, parameters, config, spec
        self.cancel_check, self.progress_callback = cancel_check, progress_callback
        self.lock, self.cancelled = threading.Lock(), threading.Event()
        self.closed, self.failed = False, None
        self.state = {"workspace": None, "initialized": False}
        if prepared.get("setup_private_session"):
            from .setup_private_session import private_session_info
            self.state["owner"] = private_session_info(prepared["setup_private_session"])
            self.state["session_owned"] = True
            return
        base = ensure_dir(os.path.join(self.run_root, "private_netlist", "maestro_shared"))
        if os.path.realpath(base) != base:
            raise ValueError("Shared Maestro epoch directory is linked")
        prior = owner_records(self.run_root)
        if len(prior) >= 128:
            raise ValueError("Shared Maestro epoch registry capacity reached")
        token = uuid.uuid4().hex
        root = os.path.join(base, token)
        os.mkdir(root)
        owner = {"schema_version": SCHEMA, "token": token, "run_root": self.run_root,
                 "root": root, "workspace_root": os.path.join(root, "workspace")}
        atomic_write_json(os.path.join(root, OWNER), owner)
        atomic_write_json(os.path.join(base, REGISTRY), {
            "schema_version": SCHEMA, "run_root": self.run_root,
            "epochs": [row["token"] for row in prior] + [token]})
        self.state["owner"] = owner

    def _cancelled(self):
        return self.cancelled.is_set() or bool(self.cancel_check and self.cancel_check())

    def generate(self, values, root, candidate_id):
        from .netlist_providers import prepare_maestro_netlists
        from .private_netlist import PrivateNetlistError, reconcile_preparation, _manifest_digest, validate_manifest
        started = time.monotonic()
        while not self.lock.acquire(timeout=0.05):
            if self._cancelled():
                raise PrivateNetlistError("Maestro generation cancelled while waiting for the shared suite")
        try:
            if self.closed or self._cancelled():
                raise PrivateNetlistError("Maestro generation cancelled before private schematic update")
            if self.failed is not None:
                raise PrivateNetlistError("Shared Maestro suite is unavailable after an earlier generation failure: " + str(self.failed))
            try:
                manifest = prepare_maestro_netlists(
                    self.prepared, self.parameters, values, self.config, root, candidate_id,
                    spec=self.spec, cancel_check=self._cancelled, progress_callback=self.progress_callback,
                    shared_state=self.state)
                proof = reconcile_preparation(root, request_cancel=False)
                if not proof.get("cleanup_proven"):
                    error = PrivateNetlistError("Shared Maestro native generation has not been proven stopped")
                    error.detail = {"failure_stage": "native_generation_termination", "termination_failed": True,
                                    "preparation_cleanup": proof}
                    raise error
                self.state["initialized"] = True
                manifest["shared_generation"] = {"token": self.state["owner"]["token"],
                    "workspace_root": self.state["owner"]["workspace_root"],
                    "generation_elapsed_seconds": time.monotonic()-started,
                    "native_termination": proof, "config_initialized_once": True,
                    "owner_scope": "ui_session" if self.state.get("session_owned") else "run"}
                manifest["manifest_sha256"] = _manifest_digest(manifest)
                validate_manifest(manifest)
                atomic_write_json(os.path.join(root,"private_netlist.json"),manifest)
                atomic_write_json(os.path.join(root,"private_netlist_candidate.json"),manifest)
                return manifest
            except Exception as error:
                self.failed = error
                # The suite is never reused after a failed/incomplete CDF or
                # capture transaction, even when its process has now stopped.
                proof = reconcile_preparation(root, request_cancel=True)
                if not proof.get("cleanup_proven"):
                    detail = getattr(error, "detail", None)
                    if not isinstance(detail, dict):
                        error.detail = detail = {}
                    detail.update(termination_failed=True, preparation_cleanup=proof)
                raise
        finally:
            self.lock.release()

    def cancel(self):
        self.cancelled.set()

    def close(self, cancel=False, timeout_seconds=12):
        if cancel:
            self.cancel()
        acquired = self.lock.acquire(timeout=max(0, float(timeout_seconds)))
        if not acquired:
            return {"cleanup_proven": False, "state": "generation_lock_still_active",
                    "shared_owner": self.state["owner"]}
        try:
            self.closed = True
            return {"cleanup_proven": True, "state": "generation_lock_quiescent",
                    "shared_owner": self.state["owner"],
                    "view_cleanup": "ui_close_only" if self.state.get("session_owned") else "run_close_only"}
        finally:
            self.lock.release()
