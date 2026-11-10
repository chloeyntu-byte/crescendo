"""UI-owned private Maestro/config views borrowed by every SCS producer.

Only this module's exact registry authorizes retention and final cleanup.
The JSON reference is serializable; locks and native ownership stay in process.
"""
from __future__ import print_function

import contextlib
import copy
import fcntl
import hashlib
import json
import os
import re
import shutil
import stat
import threading
import tempfile
import time
import uuid
import xml.etree.ElementTree as ET

from .utils import atomic_write_json, atomic_write_text, ensure_dir
from .private_view_access import PrivateViewBusy, check_private_view_edit_locks, write_private_views, restore_private_views

SCHEMA = "crescendo.setup_private_session.v1"
REFERENCE = "setup_private_session_reference.json"
OWNER = "session_owner.json"
_SESSIONS = {}
_CLOSED = set()
_QUARANTINED = set()
_REGISTRY_LOCK = threading.Lock()
_TOKEN = re.compile(r"^[a-f0-9]{32}$")


def _load(path, max_bytes=4 * 1024 * 1024):
    if os.path.realpath(path) != os.path.abspath(path):
        raise ValueError("Private setup identity traverses a symbolic link")
    before = os.lstat(path)
    if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_size > max_bytes:
        raise ValueError("Private setup identity is not a bounded regular file")
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    with os.fdopen(descriptor, "r") as stream:
        if not os.path.samestat(before, os.fstat(stream.fileno())):
            raise ValueError("Private setup identity changed while opening")
        return json.load(stream)


def _owner(reference):
    if not isinstance(reference, dict):
        raise ValueError("Private setup session reference is not an object")
    token = reference.get("session_id") if isinstance(reference, dict) else None
    root = reference.get("root") if isinstance(reference, dict) else None
    if (reference.get("schema_version") != SCHEMA or not isinstance(token, str)
            or not _TOKEN.fullmatch(token) or not isinstance(root, str)
            or not os.path.isabs(root) or os.path.basename(root) != token
            or os.path.basename(os.path.dirname(root)) != "setup_private_sessions"
            or os.path.realpath(root) != root):
        raise ValueError("Private setup session reference is invalid")
    owner = _load(os.path.join(root, OWNER))
    keys = ("schema_version", "session_id", "root", "workspace_root", "owner_uid")
    if (any(reference.get(key) != owner.get(key) for key in keys)
            or owner.get("workspace_root") != os.path.join(root, "workspace")
            or owner.get("owner_uid") != os.getuid()):
        raise ValueError("Private setup session owner differs from its durable registry")
    return owner


QUARANTINE = "suite_quarantine.json"


def detach_setup_private_session(reference, reason):
    """Persist isolation without waiting for a native writer or altering its proof."""
    owner = _owner(reference)
    if not isinstance(reason, str) or not reason.strip():
        raise ValueError("Suite isolation requires a reason")
    with _REGISTRY_LOCK:
        _QUARANTINED.add(owner["session_id"])
    path = os.path.join(owner["root"], QUARANTINE)
    errors = []
    try:
        atomic_write_json(path, {"schema_version": "crescendo.suite_quarantine.v1",
                                 "session_id": owner["session_id"], "root": owner["root"],
                                 "reason": reason[:4096], "time": time.time()})
    except Exception as error:
        errors.append(str(error))
    return {"detached": True, "durable": not errors, "state": "quarantined",
            "session_id": owner["session_id"], "quarantine_path": path, "errors": errors}


def _quarantine(owner):
    with _REGISTRY_LOCK:
        in_memory = owner["session_id"] in _QUARANTINED
    path = os.path.join(owner["root"], QUARANTINE)
    if not os.path.lexists(path):
        return {"reason": "in-process isolation; durable record unavailable"} if in_memory else None
    document = _load(path)
    if (document.get("schema_version") != "crescendo.suite_quarantine.v1" or
            document.get("session_id") != owner["session_id"] or
            document.get("root") != owner["root"]):
        raise ValueError("CRESCENDO-SUITE-RECOVERY: Invalid suite isolation record")
    return document


def _controller(reference):
    owner = _owner(reference)
    with _REGISTRY_LOCK:
        controller = _SESSIONS.get(owner["session_id"])
    if controller is None or controller.owner != owner or controller.closed:
        raise ValueError("Private setup session is no longer owned by this Crescendo window; Load and Inspect again")
    return controller


def private_session_info(reference):
    return _controller(reference).info()


def owned_setup_private_sessions():
    with _REGISTRY_LOCK:
        controllers = list(_SESSIONS.values())
    return [item.info() for item in controllers if not item.closed]


def write_session_reference(root, reference, kind=None):
    controller = _controller(reference)
    if _quarantine(controller.owner):
        raise RuntimeError("CRESCENDO-SUITE-RECOVERY: Suite is isolated")
    root = os.path.abspath(root)
    if os.path.realpath(root) != root or not os.path.isdir(root):
        raise ValueError("Private setup capture root is unsafe")
    if kind not in (None, "native_baseline"):
        raise ValueError("Unknown private session capture kind")
    record = dict(controller.owner, capture_root=root)
    if kind is not None:
        record["capture_kind"] = kind
    atomic_write_json(os.path.join(root, REFERENCE), record)
    controller.capture_roots.add(root)
    if kind:
        controller.capture_kinds[root] = kind
        controller.capture_proofs.pop(root, None)
    controller.persist()


def _reconcile_owned_capture(root, request_cancel, timeout_seconds=12):
    reference = _load(os.path.join(root, REFERENCE))
    owner = preparation_session_reference(root)  # Exact owner and append-only root registration.
    kind = reference.get("capture_kind")
    state = _load(os.path.join(owner["root"], "session_state.json"), max_bytes=128 * 1024 * 1024)
    if (state.get("capture_kinds") or {}).get(root) != kind:
        raise ValueError("Private session capture kind does not match durable registry")
    if kind == "native_baseline":
        from .native_baseline import reconcile_native_baseline
        return reconcile_native_baseline(root, request_cancel=request_cancel, timeout_seconds=timeout_seconds)
    if kind is not None:
        raise ValueError("Unknown private session capture kind")
    from .private_netlist import reconcile_preparation
    return reconcile_preparation(root, request_cancel=request_cancel, timeout_seconds=timeout_seconds)


def preparation_session_reference(root):
    """Retention proof only; this never grants permission to remove any view."""
    path = os.path.join(os.path.abspath(root), REFERENCE)
    if not os.path.lexists(path):
        return None
    reference = _load(path)
    owner = _owner(reference)
    if reference.get("capture_root") != os.path.abspath(root):
        raise ValueError("Private setup capture reference belongs to another preparation")
    # The owner's append-only list binds this otherwise arbitrary capture path.
    state = _load(os.path.join(owner["root"], "session_state.json"), max_bytes=128 * 1024 * 1024)
    if (state.get("session_id") != owner["session_id"]
            or os.path.abspath(root) not in state.get("capture_roots", [])):
        raise ValueError("Private setup capture is absent from its session registry")
    return owner


def _design_descriptors(prepared):
    result = []
    for row in (prepared.get("hierarchy") or {}).get("designs") or []:
        design = row.get("design") or row
        if isinstance(design, (list, tuple)):
            design = dict(zip(("library", "cell", "view"), design))
        result.append(dict(backend="schematic", **dict(
            ("design_" + key, design.get(key)) for key in ("library", "cell", "view"))))
    return result


def _clear_inherited_history(workspace):
    """Remove copied metadata only, never ask Cadence to delete source PSF."""
    paths = [workspace["setup_sdb"], os.path.join(workspace["setup_dir"], "active.state")]
    removed = 0
    for path in paths:
        if not os.path.isfile(path):
            continue
        with open(path, "rb") as stream:
            prefix = stream.read(128).lstrip()
        if not prefix.startswith(b"<"):
            continue
        tree = ET.parse(path)
        changed = False
        for parent in tree.iter():
            for child in list(parent):
                if str(child.tag).rsplit("}", 1)[-1].lower() == "historyentry":
                    parent.remove(child)
                    removed += 1
                    changed = True
        if changed:
            tree.write(path, encoding="utf-8", xml_declaration=True)
    return removed


def _acquire_lease(root):
    path = os.path.join(root, "suite.lease")
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags, 0o600)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.getuid():
            raise ValueError("Private suite lease identity is invalid")
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            raise RuntimeError("CRESCENDO-SUITE-LEASE: Private suite is in use by another process")
        return fd
    except BaseException:
        os.close(fd)
        raise


def _release_lease(fd):
    fcntl.flock(fd, fcntl.LOCK_UN)
    os.close(fd)


def _pending_creation_stages(owner, workspace):
    root = owner["workspace_root"]
    pending = []
    if not os.path.isdir(root):
        return pending
    for name in os.listdir(root):
        if not (name.startswith("suite_create_") and name.endswith(".json")):
            continue
        intent = _load(os.path.join(root, name))
        row = intent.get("row") or {}
        stage = intent.get("stage") or ""
        path = row.get("path") or ""
        key = hashlib.sha256(path.encode("utf-8")).hexdigest()
        if (intent.get("schema_version") != "crescendo.suite_view_create.v1" or
                intent.get("owner") != owner or name != "suite_create_" + key + ".json" or
                row not in (workspace.get("same_library_registry") or []) or
                os.path.dirname(stage) != os.path.dirname(path) or
                not os.path.basename(stage).startswith(".crescendo-create-") or
                not _TOKEN.fullmatch(os.path.basename(stage)[len(".crescendo-create-"):])):
            raise ValueError("CRESCENDO-SUITE-RECOVERY: Creation staging identity invalid")
        if os.path.lexists(stage):
            pending.append(stage)
    return pending


def _deletion_access_workspace(workspace):
    """Idle guards omit only receipt-proven in-place deletions, never load them."""
    from .same_library_views import in_place_deletion_active
    result = dict(workspace)
    result["same_library_registry"] = [row for row in workspace.get("same_library_registry") or []
                                       if not in_place_deletion_active(workspace, row)]
    return result


def _read_suite(reference, deep=True, require_snapshot=True, allow_missing=False):
    owner = _owner(reference)
    state_path = os.path.join(owner["root"], "session_state.json")
    state = _load(state_path, max_bytes=128 * 1024 * 1024) if os.path.lexists(state_path) else {}
    workspace_path = os.path.join(owner["workspace_root"], "workspace.json")
    registry_path = os.path.join(owner["workspace_root"], "same_library_views.json")
    intent_path = os.path.join(owner["workspace_root"], "suite_registry_intent.json")
    workspace = _load(workspace_path, max_bytes=128 * 1024 * 1024) if os.path.lexists(workspace_path) else None
    commit_path = os.path.join(owner["root"], "suite_metadata_commit.json")
    if os.path.lexists(commit_path):
        commit = _load(commit_path, max_bytes=256 * 1024 * 1024)
        if (commit.get("schema_version") != "crescendo.suite_metadata_commit.v1" or
                commit.get("owner") != owner or not isinstance(commit.get("state"), dict) or
                not isinstance(commit.get("workspace"), dict) or not _TOKEN.fullmatch(commit.get("revision") or "") or
                commit["state"].get("_suite_revision") != commit["revision"] or
                commit["workspace"].get("_suite_revision") != commit["revision"]):
            raise ValueError("CRESCENDO-SUITE-RECOVERY: Invalid metadata commit")
        if (state.get("_suite_revision") != commit["revision"] or
                not workspace or workspace.get("_suite_revision") != commit["revision"]):
            state, workspace = commit["state"], commit["workspace"]
    registry = _load(registry_path) if os.path.lexists(registry_path) else None
    coherent = (isinstance(workspace, dict) and isinstance(registry, dict) and
                registry.get("token") == workspace.get("same_library_token") and
                registry.get("views") == workspace.get("same_library_registry"))
    if not coherent:
        intent = _load(intent_path, max_bytes=128 * 1024 * 1024)
        candidate = intent.get("workspace") or {}
        old, new = intent.get("old_views"), candidate.get("same_library_registry")
        if (intent.get("schema_version") != "crescendo.suite_registry_intent.v1" or
                intent.get("owner") != owner or candidate.get("setup_private_session") != owner or
                candidate.get("root") != owner["workspace_root"] or not isinstance(old, list) or
                not isinstance(new, list) or len(new) != len(old) + 1 or new[:-1] != old or
                (workspace is not None and (workspace.get("same_library_token") != candidate.get("same_library_token")
                   or workspace.get("same_library_registry") not in (old, new))) or
                (registry is not None and (registry.get("token") != candidate.get("same_library_token")
                   or registry.get("views") not in (old, new)))):
            raise ValueError("Private suite exact view registry differs from workspace")
        from .same_library_views import owns_view
        if not all(owns_view(candidate, row.get("target") or [], role=row.get("role")) for row in new):
            raise ValueError("Private suite registry intent ownership is invalid")
        # Existing workspace may contain later valid fields; only reconcile registry.
        workspace = dict(workspace or candidate, same_library_registry=new)
        registry = {"schema_version": "crescendo.same_library_views.v1", "token": candidate["same_library_token"], "views": new}
    if state.get("session_id") != owner["session_id"] or workspace.get("root") != owner["workspace_root"]:
        raise ValueError("Private suite workspace identity differs from registry")
    if workspace.get("setup_private_session") != owner:
        raise ValueError("Private suite workspace owner differs from registry")
    if deep:
        pending_stages = _pending_creation_stages(owner, workspace)
        if pending_stages:
            raise ValueError("CRESCENDO-SUITE-RECOVERY: Incomplete creation staging retained: " + ", ".join(pending_stages))
        from .private_view_access import _inventory
        _inventory(_deletion_access_workspace(workspace) if allow_missing else workspace)
        for row in workspace.get("same_library_registry") or []:
            if not os.path.isdir(row["path"]) and not (allow_missing and not os.path.lexists(row["path"])):
                raise ValueError("Private suite registered view is missing: " + row["path"])
    snapshot_hash = state.get("prepared_hash")
    if snapshot_hash:
        if not re.fullmatch(r"[a-f0-9]{64}", snapshot_hash):
            raise ValueError("CRESCENDO-SUITE-SNAPSHOT: Private suite snapshot identity is invalid")
        if require_snapshot:
            path = os.path.join(owner["root"], "prepared-" + snapshot_hash + ".json")
            try:
                snapshot = _load(path, max_bytes=128 * 1024 * 1024)
            except (OSError, ValueError) as error:
                raise ValueError("CRESCENDO-SUITE-SNAPSHOT: Private suite snapshot unavailable or invalid: " + str(error))
            if _snapshot_hash(snapshot) != snapshot_hash:
                raise ValueError("CRESCENDO-SUITE-SNAPSHOT: Private suite snapshot changed")
            state["prepared"] = snapshot
    if require_snapshot and not isinstance(state.get("prepared"), dict):
        raise ValueError("CRESCENDO-SUITE-SNAPSHOT: Legacy suite has no persisted inspected snapshot; cannot safely reopen")
    return owner, state, workspace


def _suite_bases(config):
    runtime = config.get("runtime") or {}
    roots = [runtime.get("design_root"), runtime.get("run_root")]
    bases = []
    for root in roots:
        if not root:
            continue
        base = os.path.join(os.path.abspath(root), "setup_private_sessions")
        if base not in bases:
            bases.append(base)
    if not bases:
        raise ValueError("Private suite storage root is not configured")
    return bases


def list_setup_private_sessions(config):
    result = []
    for base in _suite_bases(config):
        result.extend(_list_setup_private_base(base))
    counts = {}
    for row in result:
        token = row.get("suite_id")
        counts[token] = counts.get(token, 0) + 1
    for row in result:
        if counts.get(row.get("suite_id"), 0) > 1:
            row["status"] = "unavailable"
            row["can_load"] = row["can_delete"] = False
            row.setdefault("warnings", []).append("CRESCENDO-SUITE-RECOVERY: Duplicate suite identifier across storage roots")
    return result


def _list_setup_private_base(base):
    if not os.path.isdir(base) or os.path.realpath(base) != base:
        return []
    result = []
    for token in sorted(os.listdir(base)):
        if not _TOKEN.fullmatch(token):
            continue
        root = os.path.join(base, token)
        try:
            owner = _owner(_load(os.path.join(root, OWNER)))
            index_path = os.path.join(root, "suite_index.json")
            item = None
            if (not os.path.lexists(os.path.join(root, "suite_metadata_commit.json"))
                    and os.path.isfile(index_path) and os.path.isfile(os.path.join(root, "session_state.json"))
                    and os.path.isfile(os.path.join(owner["workspace_root"], "workspace.json"))
                    and os.path.isfile(os.path.join(owner["workspace_root"], "same_library_views.json"))):
                index = _load(index_path)
                state_stat = os.stat(os.path.join(root, "session_state.json"))
                workspace_stat = os.stat(os.path.join(owner["workspace_root"], "workspace.json"))
                registry_stat = os.stat(os.path.join(owner["workspace_root"], "same_library_views.json"))
                if (index.get("session_id") == token and index.get("root") == root and
                        index.get("state_stat") == [state_stat.st_mtime_ns, state_stat.st_size] and
                        index.get("workspace_stat") == [workspace_stat.st_mtime_ns, workspace_stat.st_size] and
                        index.get("registry_stat") == [registry_stat.st_mtime_ns, registry_stat.st_size]):
                    item = dict(owner, **index["summary"])
            if item is None:
                owner, state, workspace = _read_suite(owner, deep=False, require_snapshot=False)
                item = _suite_summary(owner, state, workspace)
            quarantine = _quarantine(owner)
            if quarantine:
                item.update(status="quarantined", can_load=False, can_delete=False, can_recover=False)
                item.setdefault("warnings", []).append("CRESCENDO-SUITE-RECOVERY: Isolated suite; " + str(quarantine.get("reason") or "recovery required"))
            try:
                lease = _acquire_lease(root)
                _release_lease(lease)
            except RuntimeError:
                item["busy"] = True
                if item.get("can_recover"):
                    item["can_recover"] = False
                    item["can_recover_reason"] = ("Suite lease is still held; release it after native work stops, then reopen to recover.")
            result.append(item)
        except (OSError, ValueError, KeyError, TypeError) as error:
            result.append({"suite_id": token, "root": root, "status": "unavailable", "warnings": [str(error)]})
    return result


def _suite_summary(owner, state, workspace):
    token = owner["session_id"]
    item = dict(owner, suite_id=token, label=state.get("label") or "Suite " + token[:8],
                status=state.get("status"), views=copy.deepcopy(workspace.get("same_library_registry") or []),
                warnings=[], busy=False)
    setup = workspace.get("setup") or {}
    source = setup.get("source") or {}
    selection = state.get("applied_selection") or {}
    item.update(source_path=source.get("setup_db") or source.get("maestro_sdb"),
                selected_test=setup.get("selected_test"),
                baseline_id=(setup.get("baseline") or {}).get("baseline_id"),
                applied_scope_ids=selection.get("scope_ids") or [],
                applied_parameter_ids=selection.get("parameter_ids") or [],
                can_load=bool(state.get("prepared_hash") or isinstance(state.get("prepared"), dict))
                    and not state.get("unsafe") and not state.get("refresh_pending")
                    and state.get("status") in ("ready", "released", "closed"),
                can_delete=state.get("status") != "deleted" and not state.get("refresh_pending"),
                can_recover=isinstance(state.get("refresh_pending"), dict) and
                    state["refresh_pending"].get("schema_version") == "crescendo.suite_refresh_pending.v1")
    if not (state.get("prepared_hash") or isinstance(state.get("prepared"), dict)):
        item["warnings"].append("CRESCENDO-SUITE-SNAPSHOT: Legacy suite has no saved inspected snapshot; delete only")
    stages = _pending_creation_stages(owner, workspace)
    if stages:
        item.update(status="recovery_required", can_load=False, can_delete=False, can_recover=False)
        item["warnings"].append("CRESCENDO-SUITE-RECOVERY: Incomplete creation staging retained: " + ", ".join(stages))
    if state.get("refresh_pending"):
        item["status"] = "recovery_required"
    if state.get("unsafe") or state.get("refresh_pending"):
        item["warnings"].append("CRESCENDO-SUITE-RECOVERY: Suite has unresolved native or refresh state")
    return item

def reopen_setup_private_session(reference, config, for_delete=False):
    owner = _owner(reference)
    if _quarantine(owner):
        raise ValueError("CRESCENDO-SUITE-RECOVERY: Suite is isolated; retained for recovery")
    lease = _acquire_lease(owner["root"])
    try:
        owner, state, workspace = _read_suite(owner, require_snapshot=not for_delete, allow_missing=for_delete)
        if state.get("refresh_pending") or (not for_delete and (state.get("unsafe") or state.get("status") not in ("ready", "released", "closed"))) or state.get("status") == "deleted":
            raise ValueError("Private suite has unresolved native activity or incomplete setup")
        from .legacy_guardian import quiesce_legacy_guardian_attempt
        for root in state.get("capture_roots") or []:
            if (not os.path.isdir(root) and (state.get("capture_kinds") or {}).get(root) == "native_baseline"
                    and (state.get("capture_proofs") or {}).get(root, {}).get("cleanup_proven") is not True):
                raise ValueError("Private suite native baseline evidence directory is missing")
            if os.path.isdir(root) and not _reconcile_owned_capture(root, request_cancel=False).get("cleanup_proven"):
                raise ValueError("Private suite prior native activity is not proven stopped")
        for control in state.get("operations") or []:
            if os.path.lexists(os.path.join(control, "legacy_guardian_launch.json")):
                proof = quiesce_legacy_guardian_attempt(control, expected_workspace_root=owner["workspace_root"],
                                                        timeout_seconds=0, terminate_owned=False)
                if proof.get("state") != "dead":
                    raise ValueError("Private suite prior native activity is not proven stopped")
        # Under the exclusive lease, reconcile an interrupted metadata/registry mirror.
        mirror_state = dict(state)
        mirror_state.pop("prepared", None)
        atomic_write_json(os.path.join(owner["root"], "session_state.json"), mirror_state)
        atomic_write_json(os.path.join(owner["workspace_root"], "same_library_views.json"), {
            "schema_version": "crescendo.same_library_views.v1", "token": workspace["same_library_token"],
            "views": workspace.get("same_library_registry") or []})
        atomic_write_json(os.path.join(owner["workspace_root"], "workspace.json"), workspace)
        controller = _PrivateSession.__new__(_PrivateSession)
        controller.owner, controller.lease = owner, lease
        controller.prepared, controller.config, controller.workspace = state.get("prepared") or {}, copy.deepcopy(config), workspace
        controller.deletion_only = for_delete
        controller.stored_prepared_hash = state.get("prepared_hash")
        controller.label = state.get("label") or "Suite " + owner["session_id"][:8]
        controller.lock = threading.Lock()
        controller.capture_roots = set(state.get("capture_roots") or [])
        controller.capture_kinds = dict(state.get("capture_kinds") or {})
        controller.capture_proofs = dict(state.get("capture_proofs") or {})
        controller.operations = state.get("operations") or []
        controller.closed, controller.unsafe, controller.status = False, False, "ready"
        controller.binding_plan = state.get("binding_plan") or []
        controller.binding_verification = state.get("binding_verification")
        controller.view_fingerprints = state.get("view_fingerprints") or {}
        controller.applied_selection = state.get("applied_selection") or {"scope_ids": [], "parameter_ids": []}
        controller.restore_pending = state.get("restore_pending") or []
        controller.refresh_pending = None
        controller.metadata_warnings = state.get("metadata_warnings") or []
        controller.private_views_read_only = False
        access_workspace = _deletion_access_workspace(workspace) if for_delete else workspace
        check_private_view_edit_locks(access_workspace)
        from .private_view_access import protect_private_views
        protect_private_views(access_workspace)
        controller.private_views_read_only = True
        if not for_delete:
            controller.persist()
        with _REGISTRY_LOCK:
            _SESSIONS[owner["session_id"]] = controller
            _CLOSED.discard(owner["session_id"])
        info = controller.info()
        info["prepared"] = copy.deepcopy(controller.prepared)
        return info
    except BaseException:
        _release_lease(lease)
        raise


def setup_private_session_snapshot(reference):
    return copy.deepcopy(_controller(reference).prepared)


def update_setup_private_session_snapshot(reference, prepared):
    controller = _controller(reference)
    controller._lock()
    try:
        _require_matching_test(controller, prepared)
        from .setup_baseline import verify_setup_baseline
        original = verify_setup_baseline(controller.prepared["setup"])
        current = verify_setup_baseline(prepared["setup"])
        if not original or not current or original["fingerprint"] != current["fingerprint"]:
            raise ValueError("Cannot replace a suite's frozen baseline")
        controller.prepared = copy.deepcopy(prepared)
        controller.prepared.pop("setup_private_session", None)
        controller.persist()
    finally:
        controller.lock.release()



def _backup_legacy_metadata_once(owner, workspace, allow_missing_snapshot=False):
    """Retain pre-migration metadata; the backup is published without replacement."""
    from .utils import _fsync_directory
    path = os.path.join(owner["root"], "suite_legacy_metadata_backup.json")
    if os.path.lexists(path):
        existing = _load(path, max_bytes=384 * 1024 * 1024)
        if existing.get("schema_version") != "crescendo.suite_legacy_backup.v1" or existing.get("owner") != owner:
            raise ValueError("CRESCENDO-SUITE-RECOVERY: Existing legacy backup identity is invalid")
        return
    state_path = os.path.join(owner["root"], "session_state.json")
    workspace_path = os.path.join(owner["workspace_root"], "workspace.json")
    if not os.path.isfile(state_path) or not os.path.isfile(workspace_path):
        # A newly creating suite has no legacy workspace to migrate.
        return
    prior_state = _load(state_path, max_bytes=128 * 1024 * 1024)
    prior_workspace = _load(workspace_path, max_bytes=128 * 1024 * 1024)
    if (prior_state.get("session_id") != owner["session_id"] or
            prior_workspace.get("setup_private_session") != owner or
            prior_workspace.get("root") != owner["workspace_root"]):
        raise ValueError("CRESCENDO-SUITE-RECOVERY: Legacy metadata identity is invalid")
    snapshot = None
    snapshot_hash = prior_state.get("prepared_hash")
    if snapshot_hash:
        if not re.fullmatch(r"[a-f0-9]{64}", snapshot_hash):
            raise ValueError("CRESCENDO-SUITE-RECOVERY: Legacy snapshot hash is invalid")
        snapshot_path = os.path.join(owner["root"], "prepared-" + snapshot_hash + ".json")
        if not os.path.lexists(snapshot_path) and allow_missing_snapshot:
            snapshot = None
        else:
            snapshot = _load(snapshot_path, max_bytes=128 * 1024 * 1024)
            if _snapshot_hash(snapshot) != snapshot_hash:
                raise ValueError("CRESCENDO-SUITE-RECOVERY: Legacy snapshot identity changed")
    elif isinstance(prior_state.get("prepared"), dict):
        snapshot = prior_state["prepared"]
    backup = {"schema_version": "crescendo.suite_legacy_backup.v1", "owner": owner,
              "state": prior_state, "workspace": prior_workspace, "prepared": snapshot,
              "snapshot_missing": bool(snapshot_hash and snapshot is None)}
    fd, tmp = tempfile.mkstemp(prefix=".suite-legacy-backup-", suffix=".tmp", dir=owner["root"])
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(backup, stream, ensure_ascii=True, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(tmp, path)
        except FileExistsError:
            existing = _load(path, max_bytes=384 * 1024 * 1024)
            if existing.get("schema_version") != "crescendo.suite_legacy_backup.v1" or existing.get("owner") != owner:
                raise ValueError("CRESCENDO-SUITE-RECOVERY: Existing legacy backup identity is invalid")
        _fsync_directory(owner["root"])
    finally:
        if os.path.lexists(tmp):
            os.unlink(tmp)


def _snapshot_hash(snapshot):
    payload = json.dumps(snapshot, sort_keys=True, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


class _PrivateSession(object):
    def __init__(self, prepared, config, label=None):
        token = uuid.uuid4().hex
        base = ensure_dir(_suite_bases(config)[0])
        if os.path.realpath(base) != base:
            raise ValueError("Private setup session directory is linked")
        root = os.path.join(base, token)
        os.mkdir(root, 0o700)
        self.owner = {"schema_version": SCHEMA, "session_id": token, "id": token, "token": token,
                      "root": root, "workspace_root": os.path.join(root, "workspace"),
                      "owner_uid": os.getuid()}
        self.lease = _acquire_lease(root)
        try:
            self.deletion_only = False
            self.stored_prepared_hash = None
            self.label = str(label or "Suite " + token[:8])
            self.prepared = copy.deepcopy(prepared)
            self.prepared.pop("setup_private_session", None)
            self.config, self.workspace = copy.deepcopy(config), None
            self.lock = threading.Lock()
            self.capture_roots, self.capture_kinds, self.capture_proofs, self.operations = set(), {}, {}, []
            self.closed, self.unsafe = False, False
            self.status = "creating"
            self.binding_plan, self.binding_verification = [], None
            self.view_fingerprints = {}
            self.applied_selection = {"scope_ids": [], "parameter_ids": []}
            self.restore_pending = []
            self.refresh_pending = None
            self.private_views_read_only = False
            atomic_write_json(os.path.join(root, OWNER), self.owner)
            self.persist()
        except BaseException:
            try:
                _release_lease(self.lease)
            finally:
                self.lease = None
            raise

    def persist(self):
        snapshot_hash = (_snapshot_hash(self.prepared) if not self.deletion_only
                         else getattr(self, "stored_prepared_hash", None))
        if snapshot_hash and not self.deletion_only:
            snapshot_path = os.path.join(self.owner["root"], "prepared-" + snapshot_hash + ".json")
            if not os.path.exists(snapshot_path):
                atomic_write_json(snapshot_path, self.prepared)
        self.stored_prepared_hash = snapshot_hash
        state = {
            "schema_version": SCHEMA, "session_id": self.owner["session_id"],
            "status": self.status, "closed": self.closed, "unsafe": self.unsafe,
            "capture_roots": sorted(self.capture_roots), "capture_kinds": self.capture_kinds,
            "capture_proofs": self.capture_proofs, "operations": self.operations,
            "applied_selection": self.applied_selection,
            "restore_pending": self.restore_pending, "refresh_pending": self.refresh_pending,
            "private_views_read_only": self.private_views_read_only,
            "binding_verification": self.binding_verification,
            "binding_plan": self.binding_plan, "view_fingerprints": self.view_fingerprints,
            "label": self.label, "prepared_hash": snapshot_hash,
            "metadata_warnings": list(getattr(self, "metadata_warnings", []))}
        if self.workspace:
            if not os.path.lexists(os.path.join(self.owner["root"], "suite_metadata_commit.json")):
                _backup_legacy_metadata_once(self.owner, self.workspace,
                                             allow_missing_snapshot=self.deletion_only)
            revision = uuid.uuid4().hex
            state["_suite_revision"] = revision
            self.workspace["_suite_revision"] = revision
            atomic_write_json(os.path.join(self.owner["root"], "suite_metadata_commit.json"), {
                "schema_version": "crescendo.suite_metadata_commit.v1", "owner": self.owner,
                "revision": revision, "state": state, "workspace": self.workspace})
        atomic_write_json(os.path.join(self.owner["root"], "session_state.json"), state)
        if self.workspace:
            workspace_path = os.path.join(self.workspace["root"], "workspace.json")
            atomic_write_json(workspace_path, self.workspace)
            state_path = os.path.join(self.owner["root"], "session_state.json")
            state = _load(state_path, max_bytes=128 * 1024 * 1024)
            summary = _suite_summary(self.owner, state, self.workspace)
            state_stat, workspace_stat = os.stat(state_path), os.stat(workspace_path)
            registry_stat = os.stat(os.path.join(self.workspace["root"], "same_library_views.json"))
            atomic_write_json(os.path.join(self.owner["root"], "suite_index.json"), {
                "session_id": self.owner["session_id"], "root": self.owner["root"],
                "state_stat": [state_stat.st_mtime_ns, state_stat.st_size],
                "workspace_stat": [workspace_stat.st_mtime_ns, workspace_stat.st_size],
                "registry_stat": [registry_stat.st_mtime_ns, registry_stat.st_size],
                "summary": summary})

    def info(self):
        result = copy.deepcopy(self.owner)
        result.update(suite_id=self.owner["session_id"], label=self.label, status=self.status, unsafe=self.unsafe,
                      private_views_read_only=self.private_views_read_only,
                      binding_verification=copy.deepcopy(self.binding_verification))
        result["baseline_id"] = ((self.prepared.get("setup") or {}).get("baseline") or {}).get("baseline_id")
        result["selected_test"] = (self.prepared.get("setup") or {}).get("selected_test")
        result["test_corner_masks"] = copy.deepcopy(self.prepared.get("setup", {}).get("test_corner_masks") or {})
        result["metadata_warnings"] = list(getattr(self, "metadata_warnings", []))
        result["applied_selection"] = copy.deepcopy(self.applied_selection)
        result["applied_scope_ids"] = list(self.applied_selection["scope_ids"])
        result["applied_parameter_ids"] = list(self.applied_selection["parameter_ids"])
        workspace = self.workspace or {}
        identity = workspace.get("source_identity") or {}
        result["source_identity"] = {key: identity[key] for key in ("schema_version", "fingerprint") if key in identity}
        storage = workspace.get("native_storage") or {}
        fields = ("project_dir", "save_dir", "save_res_dir", "scratch_root", "local_psf_dir", "local_rdb_dir",
                  "verified", "configured_roots_verified", "results_location", "readback_warning", "readback_error", "local_readback_warnings")
        result["native_storage"] = {key: copy.deepcopy(storage[key]) for key in fields if key in storage}
        result["native_storage"]["readback_values"] = copy.deepcopy((storage.get("readback") or {}).get("raw") or {})
        result["maestro"] = {"library": workspace.get("primary_library"), "cell": workspace.get("setup_cell"),
                             "view": workspace.get("setup_view"), "path": workspace.get("setup_dir")}
        result["views"] = copy.deepcopy(workspace.get("same_library_registry") or [])
        result["configs"] = []
        for row in self.binding_plan:
            target = row["config"]
            owned = next((item for item in result["views"] if tuple(item["target"]) == tuple(target)), {})
            result["configs"].append(dict(zip(("library", "cell", "view"), target),
                path=owned.get("path"), top=list(row["top"]), bindings=copy.deepcopy(row["bindings"])))
        return result

    @contextlib.contextmanager
    def writable_views(self):
        """Caller holds the session lock and reconciles native writers inside."""
        check_private_view_edit_locks(self.workspace)
        self.private_views_read_only = False
        first_operation = len(self.operations)
        body_error = None
        try:
            with write_private_views(self.workspace, restore_when=lambda: not self.unsafe):
                try:
                    yield
                except BaseException as error:
                    body_error = error
                    # Binding may fail before returning its usual stop proof.
                    # Never chmod a view while an owned native writer may live.
                    from .legacy_guardian import quiesce_legacy_guardian_attempt
                    for control in self.operations[first_operation:]:
                        if not os.path.lexists(os.path.join(control, "legacy_guardian_launch.json")):
                            continue
                        try:
                            proof = quiesce_legacy_guardian_attempt(control,
                                expected_workspace_root=self.owner["workspace_root"],
                                timeout_seconds=12, terminate_owned=True)
                            stopped = proof.get("state") == "dead"
                        except Exception:
                            stopped = False
                        if not stopped:
                            self.unsafe, self.status = True, "unsafe"
                    raise
            self.private_views_read_only = not self.unsafe
        except BaseException as error:
            # Propagation of the original body error means the access context
            # finished its protection successfully; a protection error differs.
            if error is body_error and not self.unsafe:
                self.private_views_read_only = True
            raise
        finally:
            self.persist()

    def view_identity(self):
        from .same_library_views import owns_view
        from .workspace import tree_content_fingerprint
        result = {}
        for row in self.workspace.get("same_library_registry") or []:
            if row["role"] == "maestro" or not os.path.exists(row["path"]):
                continue
            if not owns_view(self.workspace, row["target"], role=row["role"], require_marker=True):
                raise ValueError("Private config/schematic ownership changed")
            result[row["path"]] = tree_content_fingerprint(row["path"])["fingerprint"]
        return result

    def _lock(self, cancel_check=None, allow_failed=False):
        while not self.lock.acquire(timeout=0.05):
            if cancel_check and cancel_check():
                raise RuntimeError("Private setup operation cancelled while waiting")
        try:
            isolated = bool(_quarantine(self.owner))
        except BaseException:
            self.lock.release()
            raise
        if (isolated or self.closed or self.unsafe or (not allow_failed and self.status != "ready")
                or (cancel_check and cancel_check())):
            self.lock.release()
            raise RuntimeError("Private setup session is closed, unsafe, or cancelled")

    def _native_bind(self, parameters, config, cancel_check):
        from .cadence import child_environment, run_command
        from .workspace import (write_schematic_patch, workspace_test_design_lines,
            _workspace_startup_load_lines, _skill_quote, validate_schematic_application,
            bounded_skill_body_lines, CADENCE_STARTUP_LOCK)
        from .private_netlist import _capture_setup_skill, _capture_timeout_profiles, _capture_request_config
        from .netlist_measurement import run_owned_command
        workspace = self.workspace
        config = _capture_request_config(self.prepared, config, None)
        control = ensure_dir(os.path.join(self.owner["root"], "operations", uuid.uuid4().hex))
        self.operations.append(control)
        self.persist()
        patch, records = os.path.join(control, "bind.il"), os.path.join(control, "binding.records")
        values = dict((row["parameter_id"], None) for row in parameters)
        plan = write_schematic_patch(workspace, parameters, values, patch, records,
            emit_parameters=False, emit_clones=True, exit_after=False, clone_state="mixed")
        tests = [row["name"] for row in workspace["setup"].get("tests") or [] if row.get("enabled", True)]
        aliases, policies, unused = _capture_setup_skill(workspace, tests,
            _capture_timeout_profiles(config, tests), os.path.join(control, "policy.records"))
        from .private_netlist import native_corner_schedule_skill, parse_native_corner_schedule
        from .storage_policy import native_storage_readback_lines, load_native_storage_readback
        corner_metadata = os.path.join(control, "source_corner_schedule.tsv")
        metadata_lines = ['crescSetupMetadataRead=errset(progn('] + native_corner_schedule_skill(
            workspace["setup"], corner_metadata, include_disabled_tests=True, masks_only=True) + ['t) nil)']
        storage_lines = ['crescSetupStorageRead=errset(progn('] + native_storage_readback_lines(workspace) + ['t) nil)']
        from .single_test import native_selection, native_selection_lines
        selected_plan = native_selection(workspace['setup'])
        body = _workspace_startup_load_lines(workspace) + aliases + [
            'unless(errset(load("%s") t) error("Crescendo Setup private config creation failed\\n"))' % _skill_quote(patch),
            'unless(crescSchematicPatchResult error("Crescendo Setup private binding incomplete\\n"))',
            'session=maeOpenSetup("%s" "%s" "%s" ?mode "a")' % tuple(
                _skill_quote(workspace[key]) for key in ("primary_library", "setup_cell", "setup_view")),
            'unless(session error("Crescendo Setup private Maestro could not open\\n"))',
            'mainSDB=axlGetMainSetupDB(session)'] + policies + workspace_test_design_lines(workspace) + native_selection_lines(selected_plan) + metadata_lines + storage_lines + [
            'unless(maeSaveSetup(?session session) error("Crescendo Setup private Maestro save rejected\\n"))',
            'unless(maeCloseSession(?session session ?forceClose t) error("Crescendo Setup private Maestro close rejected\\n"))',
            'printf("CRESCENDO_SETUP_PRIVATE_BINDING_DONE\\n")']
        script, ready = os.path.join(control, "setup.il"), os.path.join(control, "ready")
        body.insert(0, 'crescReady=outfile("%s") when(crescReady close(crescReady))' % _skill_quote(ready))
        lines, generation = bounded_skill_body_lines(body, "crescSetupPrivateResult")
        lines.append('if(crescSetupPrivateResult then exit(0) else exit(1))')
        atomic_write_text(script, "\n".join(lines) + "\n")
        atomic_write_json(os.path.join(control, "setup_generation.json"), generation)
        def guarded(arguments, **kwargs):
            return run_command(arguments, startup_lock=CADENCE_STARTUP_LOCK, ready_path=ready, **kwargs)
        command = run_owned_command([config["cadence"]["ocean_bin"], "-nograph", "-restore", script],
            cwd=workspace["root"], env=child_environment(config, workspace["root"]),
            timeout_seconds=float(config.get("runtime", {}).get("command_timeout_seconds", 180)),
            control_root=control, token="setup-private-" + uuid.uuid4().hex,
            cancel_check=cancel_check, command_runner=guarded)
        atomic_write_json(os.path.join(control, "command.json"), command)
        if (command.get("returncode") != 0 or command.get("timed_out") or command.get("cancelled")
                or command.get("termination_failed") or command.get("guardian_cleanup", {}).get("state") != "dead"):
            self.unsafe = command.get("guardian_cleanup", {}).get("state") != "dead"
            raise RuntimeError("Private Setup binding failed; inspect " + os.path.join(control, "ocean.stdout"))
        if selected_plan:
            workspace["_native_execution_selection_applied"] = selected_plan
        verification = validate_schematic_application(plan)
        if verification.get("ok") is not True:
            raise RuntimeError("Private Setup config or schematic readback did not qualify")
        self.metadata_warnings = []
        load_native_storage_readback(workspace, required=False)
        storage = workspace.get("native_storage") or {}
        if storage.get("readback_error"):
            self.metadata_warnings.append("Native private save path readback unavailable: " + storage["readback_error"])
        try:
            schedule = parse_native_corner_schedule(corner_metadata)
            from .ade_setup import apply_native_corner_schedule
            apply_native_corner_schedule(self.prepared["setup"], schedule)
            apply_native_corner_schedule(workspace["setup"], schedule)
        except (OSError, ValueError, RuntimeError) as error:
            self.metadata_warnings.append("Native per-Test corner masks unavailable: " + str(error))
        self.binding_plan = copy.deepcopy(plan["config_binding_plan"])
        self.binding_verification = {"ok": True, "records": records,
            "config_records": plan["config_verification_path"], "command_root": control,
            "native_run_invoked": False, "solver_invocations": 0,
            "evidence_scope": "last_saved_native_readback_snapshot"}
        self.view_fingerprints = self.view_identity()
        if self.status != "applying":
            self.status = "ready"
        self.persist()

    def reset_maestro(self, prepared, config):
        """Reset only an exact owned, unlocked file view to the frozen source."""
        from .same_library_views import MARKER, owns_view
        from .workspace import _copy_private_file, _make_private_tree_owner_writable, copy_ignore
        from .setup_baseline import effective_setup_dir
        workspace = self.workspace
        self._recover_restore_files()
        record = workspace.get("setup_owned_view") or {}
        if not owns_view(workspace, record.get("target") or [], role="maestro", require_marker=True):
            raise ValueError("Private Maestro restore ownership is unproven")
        path = record["path"]
        for directory, directories, names in os.walk(path):
            if os.path.islink(directory) or any(os.path.islink(os.path.join(directory, name)) for name in directories + names):
                raise ValueError("Private Maestro restore encountered a symbolic link")
            locks = [{"path": os.path.join(directory, name), "view_path": path}
                     for name in directories + names if ".cdslck" in name]
            if locks:
                raise PrivateViewBusy(locks)
        source = effective_setup_dir(prepared["setup"])
        nonce = uuid.uuid4().hex
        stage, old = path + "__restore_" + nonce, path + "__previous_" + nonce
        pending = {"nonce": nonce, "live": path, "stage": stage, "previous": old}
        if os.path.lexists(stage) or os.path.lexists(old):
            raise ValueError("Private Maestro restore destination already exists")
        self.restore_pending.append(pending)
        self.persist()
        def ignore(directory, names):
            return list(set(copy_ignore(directory, names)) | {name for name in names if name == MARKER or name.endswith("~") or ".bak" in name})
        os.mkdir(stage, 0o700)
        atomic_write_json(os.path.join(stage, MARKER), {"schema_version": "crescendo.same_library_views.v1",
            "token": record["token"], "target": record["target"]})
        for name in os.listdir(source):
            if name in ignore(source, [name]):
                continue
            src, dst = os.path.join(source, name), os.path.join(stage, name)
            if os.path.isdir(src):
                shutil.copytree(src, dst, ignore=ignore, copy_function=_copy_private_file)
            else:
                _copy_private_file(src, dst)
        if prepared["setup"].get("selected_test"):
            from .single_test import project_native_files
            project_native_files(os.path.join(stage, os.path.basename(workspace["setup_sdb"])),
                                 prepared["setup"]["selected_test"],
                                 os.path.join(self.owner["root"], "native_test_projection.json"))
        _make_private_tree_owner_writable(stage)
        from .setup_baseline import _files
        staged_files = _files(stage)
        os.rename(path, old)
        try:
            os.rename(stage, path)
        except BaseException:
            os.rename(old, path)
            raise
        if _files(path) != staged_files or not owns_view(workspace, record["target"], role="maestro", require_marker=True):
            self.unsafe = True
            self.persist()
            raise ValueError("Restored private Maestro verification failed; previous copy retained")
        shutil.rmtree(old)
        self.restore_pending.remove(pending)
        workspace["setup"] = copy.deepcopy(prepared["setup"])
        workspace.pop("native_parameter_paths", None)
        workspace.pop("native_parameter_last_values", None)
        workspace.pop("_native_corner_selection_applied", None)
        workspace.pop("_native_execution_selection_applied", None)
        workspace.pop("_cr_selected_corners", None)
        from .model_paths import remap_workspace_setup_paths
        from .cadence import detect_pdk_context
        workspace["model_paths"] = remap_workspace_setup_paths(workspace, config,
            pdk_context=detect_pdk_context(workspace["setup"], prepared["hierarchy"], config.get("cadence") or {}))
        workspace["inherited_history_entries_removed"] = _clear_inherited_history(workspace)
        self.persist()

    def _recover_restore_files(self):
        """Recover only this session's durably reserved, same-mount swap paths."""
        from .same_library_views import MARKER, owns_view
        workspace = self.workspace
        record = workspace.get("setup_owned_view") or {}
        live = record.get("path")
        for row in list(self.restore_pending):
            nonce = row.get("nonce")
            if (not isinstance(nonce, str) or not _TOKEN.fullmatch(nonce)
                    or row.get("live") != live or row.get("stage") != live + "__restore_" + nonce
                    or row.get("previous") != live + "__previous_" + nonce):
                raise ValueError("Private Maestro restore registry is ambiguous")
            for path in (row["stage"], row["previous"]):
                if not os.path.lexists(path):
                    continue
                if os.path.realpath(path) != path or not os.path.isdir(path):
                    raise ValueError("Private Maestro restore copy escaped its reserved directory")
                marker = _load(os.path.join(path, MARKER))
                if marker.get("token") != record.get("token") or marker.get("target") != record.get("target"):
                    raise ValueError("Private Maestro restore copy ownership changed")
                for directory, directories, names in os.walk(path):
                    if (os.path.islink(directory) or any(os.path.islink(os.path.join(directory, name))
                            for name in directories + names) or any(".cdslck" in name for name in names)):
                        raise ValueError("Private Maestro restore copy contains a live lock or link")
            if not os.path.lexists(live) and os.path.isdir(row["previous"]):
                os.rename(row["previous"], live)
            if not owns_view(workspace, record.get("target") or [], role="maestro", require_marker=True):
                raise ValueError("Private Maestro live view is not recoverable; restore copies retained")
            for path in (row["stage"], row["previous"]):
                if os.path.isdir(path):
                    shutil.rmtree(path)
            self.restore_pending.remove(row)
            self.persist()


def create_setup_private_session(prepared, config, cancel_check=None, label=None):
    from .workspace import build_workspace
    config = copy.deepcopy(config)
    if not label:
        setup = prepared.get("setup") or {}
        source = setup.get("source") or {}
        cell = setup.get("cell") or source.get("cell") or os.path.basename(os.path.dirname(os.path.dirname(source.get("setup_db") or source.get("maestro_sdb") or ""))) or "suite"
        test = setup.get("selected_test") or "test"
        clean = lambda value: re.sub(r"[^A-Za-z0-9_]+", "_", str(value)).strip("_")[:20] or "suite"
        label = "%s_%s_%s" % (clean(cell)[:8], clean(test)[:7], time.strftime("%Y%m%d_%H%M%S"))
    config.setdefault("runtime", {})["_run_display_name"] = label
    controller = _PrivateSession(prepared, config, label=label)
    with _REGISTRY_LOCK:
        _SESSIONS[controller.owner["session_id"]] = controller
    try:
        # build_workspace fences its root under runtime.run_root. The suite has
        # its own durable owner root, so scope that fence to this exact suite;
        # native execution and capture keep the caller's real run configuration.
        build_config = copy.deepcopy(config)
        build_config.setdefault("runtime", {})["run_root"] = controller.owner["root"]
        build_config["runtime"]["_setup_private_owner"] = copy.deepcopy(controller.owner)
        controller.workspace = build_workspace(prepared["setup"], prepared["hierarchy"], build_config,
            controller.owner["workspace_root"], expected_source_identity=prepared.get("source_identity"),
            parameters=_design_descriptors(prepared))
        if controller.workspace.get("copy_policy") != "same_library_views":
            raise ValueError("Setup private sessions require the same-library-view adapter")
        controller.workspace["setup_private_session"] = copy.deepcopy(controller.owner)
        with controller.writable_views():
            from .single_test import project_native_files
            if prepared["setup"].get("selected_test"):
                project_native_files(controller.workspace["setup_sdb"], prepared["setup"]["selected_test"],
                                     os.path.join(controller.owner["root"], "native_test_projection.json"))
            controller.workspace["inherited_history_entries_removed"] = _clear_inherited_history(controller.workspace)
            controller._native_bind([], config, cancel_check)
        prepared["setup"]["test_corner_masks"] = copy.deepcopy(controller.prepared["setup"].get("test_corner_masks") or {})
        return controller.info()
    except Exception as error:
        controller.status = "failed"
        controller.persist()
        error.setup_private_session = controller.info()
        raise


def _require_matching_test(controller, prepared):
    owner_setup = (controller.prepared or {}).get("setup") or {}
    request_setup = (prepared or {}).get("setup") or {}
    owner_name = owner_setup.get("selected_test")
    request_name = request_setup.get("selected_test")
    owner_rows = owner_setup.get("tests") or []
    request_rows = request_setup.get("tests") or []
    if (not isinstance(owner_name, str) or not owner_name or request_name != owner_name
            or len(owner_rows) != 1 or owner_rows[0].get("name") != owner_name
            or len(request_rows) != 1 or request_rows[0].get("name") != request_name):
        raise ValueError("Private setup session belongs to a different or unqualified Test; Load and Inspect again")


def apply_setup_private_cells(reference, prepared, config, cancel_check=None):
    controller = _controller(reference)
    if controller.deletion_only:
        raise ValueError("Legacy suite is open for deletion only")
    _require_matching_test(controller, prepared)
    controller._lock(cancel_check, allow_failed=True)
    reset_started = False
    try:
        # Persist an incomplete transaction before touching owned views. A crash
        # cannot make its draft snapshot eligible for a later reopen.
        previous_status = controller.status
        controller.status = "applying"
        controller.persist()
        with controller.writable_views():
            reset_started = True
            parameters = [copy.deepcopy(row) for row in prepared.get("parameters") or []
                          if row.get("backend", "schematic") == "schematic" and row.get("optimizable")]
            controller.reset_maestro(prepared, config)
            # Install the requested snapshot before native readback so the
            # freshly qualified masks survive publication and persistence.
            controller.prepared = copy.deepcopy(prepared)
            controller.prepared.pop("setup_private_session", None)
            controller._native_bind(parameters, config, cancel_check)
            controller.applied_selection = {
                "scope_ids": sorted((prepared.get("cell_selection") or {}).get("scope_ids") or []),
                "parameter_ids": sorted(row["parameter_id"] for row in parameters)}
            # The native binding readback may persist internally; keep the
            # transaction unpublished until write protection also succeeds.
            controller.status = "applying"
        controller.status = "ready"
        controller.persist()
        return controller.info()
    except PrivateViewBusy as error:
        controller.status = "failed" if reset_started else previous_status
        controller.persist()
        error.setup_private_session = controller.info()
        raise
    except Exception as error:
        controller.status = "failed"
        controller.persist()
        error.setup_private_session = controller.info()
        raise
    finally:
        controller.lock.release()


@contextlib.contextmanager
def borrow_setup_private_session(prepared, config, root, cancel_check=None, completion_reconciler=None):
    reference = (prepared or {}).get("setup_private_session")
    if not reference:
        yield None
        return
    controller = _controller(reference)
    if controller.deletion_only:
        raise ValueError("Legacy suite is open for deletion only")
    _require_matching_test(controller, prepared)
    controller._lock(cancel_check)
    try:
        from .setup_baseline import verify_setup_baseline
        expected = verify_setup_baseline(controller.prepared["setup"])
        actual = verify_setup_baseline(prepared["setup"])
        if not expected or not actual or expected["fingerprint"] != actual["fingerprint"]:
            raise ValueError("Private setup session belongs to another saved Maestro baseline")
        from .workspace import verify_sources_unchanged, source_change_summary, compact_source_guard
        source_proof = verify_sources_unchanged(controller.workspace)
        if not source_proof.get("ok"):
            error = ValueError(source_change_summary(source_proof) + "; Load and Inspect to refresh the source baseline")
            error.detail = {"failure_stage": "source_freshness", "execution_started": False,
                "native_run_invoked": False, "source_guard": compact_source_guard(source_proof)}
            raise error
        if controller.view_identity() != controller.view_fingerprints:
            raise ValueError("Private schematic/config changed after native readback; Apply Selected Cells again")
        with controller.writable_views():
            body_error = None
            try:
                controller.reset_maestro(prepared, config)
                yield controller.workspace
            except BaseException as error:
                body_error = error
                raise
            finally:
                # Prove native writers stopped before restoring idle permissions.
                try:
                    if completion_reconciler is not None:
                        proof = completion_reconciler(root, request_cancel=True, timeout_seconds=12)
                        if not isinstance(proof, dict) or proof.get("cleanup_proven") is not True:
                            error = RuntimeError("Private Maestro native stop is unproven; suite retained")
                            error.detail = {"cleanup_proof": proof}
                            raise error
                        if controller.capture_kinds.get(os.path.abspath(root)) == "native_baseline":
                            controller.capture_proofs[os.path.abspath(root)] = copy.deepcopy(proof)
                            controller.persist()
                    elif os.path.isdir(root) and os.path.lexists(os.path.join(root, REFERENCE)):
                        from .private_netlist import reconcile_preparation
                        proof = reconcile_preparation(root, request_cancel=True)
                        if not proof.get("cleanup_proven"):
                            controller.unsafe = True
                            controller.status = "unsafe"
                except Exception as error:
                    controller.unsafe = True
                    controller.status = "unsafe"
                    controller.persist()
                    if body_error is None:
                        raise
                    detail = getattr(body_error, "detail", None)
                    if not isinstance(detail, dict):
                        detail = {}
                        body_error.detail = detail
                    detail["cleanup_pending"] = True
                    detail["reconciliation_error"] = str(error)
                    if isinstance(getattr(error, "detail", None), dict):
                        detail["reconciliation_proof"] = copy.deepcopy(error.detail)
    finally:
        controller.lock.release()


def close_setup_private_session(reference, timeout_seconds=12):
    owner = _owner(reference)
    with _REGISTRY_LOCK:
        controller = _SESSIONS.get(owner["session_id"])
    if controller is None or controller.owner != owner:
        if owner["session_id"] in _CLOSED:
            return {"cleanup_proven": True, "state": "released", "session_id": owner["session_id"], "removed": [], "warnings": []}
        raise ValueError("Private setup session is not owned by this Crescendo process")
    if controller.closed:
        return {"cleanup_proven": True, "state": "closed", "session_id": owner["session_id"], "removed": []}
    deadline = time.monotonic() + max(0.0, float(timeout_seconds))
    if not controller.lock.acquire(timeout=max(0.0, deadline - time.monotonic())):
        return {"cleanup_proven": False, "state": "private_setup_generation_active"}
    try:
        if controller.unsafe:
            return {"cleanup_proven": False, "state": "private_setup_native_stop_unproven"}
        if any(kind == "native_baseline" and not os.path.isdir(root)
               and controller.capture_proofs.get(root, {}).get("cleanup_proven") is not True
               for root, kind in controller.capture_kinds.items()):
            return {"cleanup_proven": False, "state": "private_setup_native_evidence_missing"}
        from .legacy_guardian import quiesce_legacy_guardian_attempt
        proofs = [_reconcile_owned_capture(root, request_cancel=True, timeout_seconds=max(0.0, deadline-time.monotonic()))
                  for root in sorted(controller.capture_roots) if os.path.isdir(root)]
        operations = [quiesce_legacy_guardian_attempt(control,
            expected_workspace_root=controller.owner["workspace_root"], timeout_seconds=max(0.0, deadline-time.monotonic()),
            terminate_owned=True) for control in controller.operations
            if os.path.lexists(os.path.join(control, "legacy_guardian_launch.json"))]
        if not all(row.get("cleanup_proven") for row in proofs) or any(row.get("state") != "dead" for row in operations):
            return {"cleanup_proven": False, "state": "private_setup_native_stop_unproven",
                    "preparations": proofs, "operations": operations}
        warnings = []
        if controller.workspace:
            try:
                check_private_view_edit_locks(controller.workspace)
                controller._recover_restore_files()
                restore_private_views(controller.workspace)
                controller.private_views_read_only = False
            except Exception as error:
                warnings.append("Private view permissions could not be restored: " + str(error))
        controller.closed = True
        if controller.status == "ready":
            controller.status = "released"
        try:
            controller.persist()
        except Exception as error:
            warnings.append("Suite state could not be saved: " + str(error))
        try:
            _release_lease(controller.lease)
            controller.lease = None
            with _REGISTRY_LOCK:
                _SESSIONS.pop(owner["session_id"], None)
                _CLOSED.add(owner["session_id"])
        except Exception as error:
            warnings.append("Suite lease could not be released: " + str(error))
        return {"cleanup_proven": True, "state": "released", "session_id": owner["session_id"],
                "removed": [], "warnings": warnings}

    finally:
        controller.lock.release()


def refresh_setup_private_session(reference, prepared, config, cancel_check=None):
    """Refresh setup in place, preserving the exact registered views and their identity.

    Hierarchy and selected parameter identities must match the inspected snapshot.
    An interrupted refresh stays explicitly blocked with backups retained.
    """
    controller = _controller(reference)
    if controller.deletion_only:
        raise ValueError("Legacy suite is open for deletion only")
    _require_matching_test(controller, prepared)
    old_prepared = controller.prepared
    if old_prepared.get("hierarchy") != prepared.get("hierarchy"):
        raise ValueError("Hierarchy changed; a design refresh is required")
    old_parameters = {row.get("parameter_id"): row for row in old_prepared.get("parameters") or []}
    new_parameters = {row.get("parameter_id"): row for row in prepared.get("parameters") or []}
    selected = set(controller.applied_selection.get("parameter_ids") or [])
    identity_keys = ("parameter_id", "backend", "name", "hierarchy_path", "occurrence_chain",
                     "design_library", "design_cell", "design_view", "owner_library", "owner_cell",
                     "owner_occurrence_path", "master_library", "master_cell")
    if any(key not in old_parameters or key not in new_parameters or
           any(old_parameters[key].get(field) != new_parameters[key].get(field) for field in identity_keys)
           for key in selected):
        raise ValueError("Selected device identity changed; a design refresh is required")
    controller._lock(cancel_check)
    try:
        backup_root = os.path.join(controller.owner["root"], "refresh_backup_" + uuid.uuid4().hex)
        saved_workspace = copy.deepcopy(controller.workspace)
        saved_state = _read_suite(controller.owner, deep=False, require_snapshot=False)[1]
        saved_plan = copy.deepcopy(controller.binding_plan)
        saved_verification = copy.deepcopy(controller.binding_verification)
        saved_fingerprints = copy.deepcopy(controller.view_fingerprints)
        saved_warnings = list(getattr(controller, "metadata_warnings", []))
        from .workspace import verify_sources_unchanged, source_change_summary
        proof = verify_sources_unchanged(controller.workspace)
        if not proof.get("ok"):
            raise ValueError(source_change_summary(proof) + "; a design refresh is required")
        expected_identity = prepared.get("source_identity")
        if expected_identity:
            from .workspace import source_design_identity
            actual_identity = source_design_identity(prepared["setup"], prepared["hierarchy"], config=config)
            if (actual_identity.get("schema_version") != expected_identity.get("schema_version") or
                    actual_identity.get("fingerprint") != expected_identity.get("fingerprint")):
                raise ValueError("Updated source identity differs from current inspected source")
        check_private_view_edit_locks(controller.workspace)
        os.mkdir(backup_root, 0o700)
        rows = []
        for index, row in enumerate(controller.workspace.get("same_library_registry") or []):
            backup = os.path.join(backup_root, str(index))
            shutil.copytree(row["path"], backup)
            rows.append({"path": row["path"], "backup": backup})
        init_path = os.path.join(controller.workspace["root"], ".cdsinit")
        with open(init_path, "r") as stream:
            old_init = stream.read()
        atomic_write_text(os.path.join(backup_root, "cdsinit.backup"), old_init)
        from .workspace import tree_content_fingerprint
        for row in rows:
            row["fingerprint"] = tree_content_fingerprint(row["backup"])["fingerprint"]
        recovery = {"schema_version": "crescendo.suite_refresh_recovery.v1", "owner": controller.owner,
                    "backup_root": backup_root, "views": rows, "old_state": saved_state,
                    "old_workspace": saved_workspace, "old_prepared": old_prepared,
                    "init_sha256": hashlib.sha256(old_init.encode("utf-8")).hexdigest()}
        atomic_write_json(os.path.join(backup_root, "recovery.json"), recovery)
        controller.refresh_pending = {"schema_version": "crescendo.suite_refresh_pending.v1",
            "backup_root": backup_root, "views": rows, "recovery_hash": _snapshot_hash(recovery)}
        controller.status = "refreshing"
        controller.persist()
        with controller.writable_views():
            try:
                controller.prepared = copy.deepcopy(prepared)
                controller.prepared.pop("setup_private_session", None)
                from .storage_policy import native_workspace_storage, native_storage_init_lines
                prior_storage = controller.workspace.get("native_storage")
                new_storage = native_workspace_storage(controller.workspace["root"],
                    (controller.prepared.get("setup") or {}).get("save_options"))
                old_lines = "\n".join(native_storage_init_lines(prior_storage))
                new_lines = "\n".join(native_storage_init_lines(new_storage))
                if not old_lines or old_init.count(old_lines) != 1:
                    raise ValueError("Private workspace storage initialization cannot be updated safely")
                atomic_write_text(init_path, old_init.replace(old_lines, new_lines, 1))
                controller.workspace["native_storage"] = new_storage
                if expected_identity:
                    for identity_field in ("source_identity", "copied_source_identity", "materialized_source_identity"):
                        controller.workspace[identity_field] = copy.deepcopy(expected_identity)
                controller.reset_maestro(controller.prepared, config)
                parameters = [copy.deepcopy(new_parameters[key]) for key in sorted(selected)]
                controller._native_bind(parameters, config, cancel_check)
                controller.refresh_pending = None
                controller.status = "ready"
                controller.persist()
            except BaseException:
                if controller.unsafe:
                    controller.status = "unsafe"
                    controller.persist()
                    raise
                try:
                    check_private_view_edit_locks(controller.workspace)
                    controller._recover_restore_files()
                    for row in rows:
                        live, backup = row["path"], row["backup"]
                        nonce = uuid.uuid4().hex
                        stage, previous = live + "__refresh_" + nonce, live + "__refresh_old_" + nonce
                        shutil.copytree(backup, stage)
                        os.rename(live, previous)
                        try:
                            os.rename(stage, live)
                        except BaseException:
                            os.rename(previous, live)
                            raise
                        shutil.rmtree(previous)
                    atomic_write_text(init_path, old_init)
                    controller.prepared = old_prepared
                    controller.workspace = saved_workspace
                    controller.binding_plan = saved_plan
                    controller.binding_verification = saved_verification
                    controller.view_fingerprints = saved_fingerprints
                    controller.metadata_warnings = saved_warnings
                    controller.refresh_pending = None
                    controller.status = "ready"
                    controller.persist()
                    from .workspace import _make_private_tree_owner_writable
                    _make_private_tree_owner_writable(backup_root)
                    shutil.rmtree(backup_root)
                except BaseException as rollback_error:
                    controller.unsafe, controller.status = True, "unsafe"
                    controller.persist()
                    raise RuntimeError("CRESCENDO-SUITE-ROLLBACK: Refresh rollback failed; evidence retained: " + str(rollback_error)) from rollback_error
                raise
        from .workspace import _make_private_tree_owner_writable
        _make_private_tree_owner_writable(backup_root)
        shutil.rmtree(backup_root)
        return controller.info()
    finally:
        controller.lock.release()


def _recovery_tree_identity(path):
    """Inspect remaining recovery files even after a recorded partial removal."""
    from .private_view_access import _object_identity
    root = _object_identity(path)
    if not stat.S_ISDIR(root.st_mode) or os.path.realpath(path) != path or os.path.ismount(path):
        raise ValueError("CRESCENDO-SUITE-RECOVERY: Unsafe recovery directory")
    for directory, dirs, files in os.walk(path, followlinks=False):
        for name in dirs + files:
            child = os.path.join(directory, name)
            info = _object_identity(child)
            if info.st_dev != root.st_dev or os.path.ismount(child) or ".cdslck" in name:
                raise ValueError("CRESCENDO-SUITE-RECOVERY: Lock or filesystem boundary in recovery directory")
    return root


def _recovery_move(source, target, receipt_path):
    """Preserve a complete copy before retiring a tree on older filesystems.

    Native no-replace rename remains the fast path. The compatibility path is
    explicitly journaled; it does not claim atomic directory publication.
    The caller owns the suite lease and has reconciled all native writers.
    """
    from .same_library_views import _rename_noreplace, RenameNoReplaceUnavailable, copy_tree_exclusive
    from .workspace import _make_private_tree_owner_writable
    from .same_library_views import _copy_tree_manifest
    from .utils import _fsync_directory
    def fingerprint(path):
        manifest = _copy_tree_manifest(path)
        return hashlib.sha256(json.dumps(manifest, ensure_ascii=True, separators=(",", ":")).encode("utf-8")).hexdigest()
    if os.path.lexists(receipt_path):
        record = _load(receipt_path)
        if (record.get("schema_version") != "crescendo.recovery_move.v1" or
                record.get("source") != source or record.get("target") != target or
                record.get("state") not in ("copying", "copied", "complete")):
            raise ValueError("CRESCENDO-SUITE-RECOVERY: Invalid compatibility move receipt")
    else:
        try:
            _rename_noreplace(source, target)
            _fsync_directory(os.path.dirname(source))
            return
        except RenameNoReplaceUnavailable:
            info = _recovery_tree_identity(source)
            if os.path.lexists(target):
                raise ValueError("CRESCENDO-SUITE-RECOVERY: Recovery target became occupied")
            record = {"schema_version": "crescendo.recovery_move.v1", "source": source,
                      "target": target, "source_dev": info.st_dev, "source_ino": info.st_ino,
                      "fingerprint": fingerprint(source), "state": "copying"}
            atomic_write_json(receipt_path, record)
    if record["state"] == "copying":
        info = _recovery_tree_identity(source)
        if ((info.st_dev, info.st_ino) != (record.get("source_dev"), record.get("source_ino")) or
                fingerprint(source) != record.get("fingerprint")):
            raise ValueError("CRESCENDO-SUITE-RECOVERY: Recovery source changed; copies retained")
        copy_tree_exclusive(source, target, receipt_path + ".copy.json")
        target_info = _recovery_tree_identity(target)
        if fingerprint(target) != record["fingerprint"]:
            raise ValueError("CRESCENDO-SUITE-RECOVERY: Recovery copy verification failed")
        record.update(state="copied", target_dev=target_info.st_dev, target_ino=target_info.st_ino)
        atomic_write_json(receipt_path, record)
    info = _recovery_tree_identity(target)
    if ((info.st_dev, info.st_ino) != (record.get("target_dev"), record.get("target_ino")) or
            fingerprint(target) != record.get("fingerprint")):
        raise ValueError("CRESCENDO-SUITE-RECOVERY: Completed recovery copy changed")
    if record["state"] == "complete":
        # A later move may legitimately reuse the former source name.
        return
    if os.path.lexists(source):
        info = _recovery_tree_identity(source)
        if (info.st_dev, info.st_ino) != (record.get("source_dev"), record.get("source_ino")):
            raise ValueError("CRESCENDO-SUITE-RECOVERY: Recovery source inode changed")
        _make_private_tree_owner_writable(source)
        shutil.rmtree(source)
        _fsync_directory(os.path.dirname(source))
    record["state"] = "complete"
    atomic_write_json(receipt_path, record)


def recover_setup_private_session(reference, config):
    """Replay a recorded refresh rollback after exact ownership and stop checks.

    Old pending formats do not contain enough information and remain isolated.
    Former live trees are retained as evidence beside their original locations.
    """
    from .private_view_access import _object_identity
    from .same_library_views import MARKER, owns_view
    from .workspace import tree_content_fingerprint
    from .utils import _fsync_directory
    owner = _owner(reference)
    if _quarantine(owner):
        raise ValueError("CRESCENDO-SUITE-RECOVERY: Suite is isolated")
    with _REGISTRY_LOCK:
        controller = _SESSIONS.get(owner["session_id"])
    if controller is not None and not controller.closed:
        raise RuntimeError("CRESCENDO-SUITE-RECOVERY: This Crescendo process still owns the suite lease; release the suite after proving its native work stopped, then reopen to recover")
    lease = _acquire_lease(owner["root"])
    try:
        owner, state, workspace = _read_suite(owner, deep=False, require_snapshot=False)
        pending = state.get("refresh_pending") or {}
        if pending.get("schema_version") != "crescendo.suite_refresh_pending.v1":
            raise ValueError("CRESCENDO-SUITE-RECOVERY: Legacy or missing refresh recovery record")
        backup_root = pending.get("backup_root") or ""
        name = os.path.basename(backup_root)
        if (os.path.dirname(backup_root) != owner["root"] or not name.startswith("refresh_backup_") or
                not _TOKEN.fullmatch(name[len("refresh_backup_"):]) or os.path.realpath(backup_root) != backup_root):
            raise ValueError("CRESCENDO-SUITE-RECOVERY: Invalid refresh backup root")
        info = _object_identity(backup_root)
        if not stat.S_ISDIR(info.st_mode):
            raise ValueError("CRESCENDO-SUITE-RECOVERY: Invalid refresh backup")
        recovery = _load(os.path.join(backup_root, "recovery.json"), max_bytes=128 * 1024 * 1024)
        old_workspace = recovery.get("old_workspace") or {}
        old_state = recovery.get("old_state") or {}
        old_prepared = recovery.get("old_prepared")
        rows = recovery.get("views")
        if (recovery.get("schema_version") != "crescendo.suite_refresh_recovery.v1" or
                recovery.get("owner") != owner or recovery.get("backup_root") != backup_root or
                _snapshot_hash(recovery) != pending.get("recovery_hash") or rows != pending.get("views") or
                not isinstance(rows, list) or old_workspace.get("setup_private_session") != owner or
                old_workspace.get("root") != owner["workspace_root"] or
                old_workspace.get("same_library_registry") != workspace.get("same_library_registry") or
                old_state.get("session_id") != owner["session_id"] or not isinstance(old_prepared, dict) or
                _snapshot_hash(old_prepared) != old_state.get("prepared_hash") or state.get("restore_pending")):
            raise ValueError("CRESCENDO-SUITE-RECOVERY: Refresh recovery identity is incomplete or changed")
        if state.get("unsafe") and not (state.get("capture_roots") or state.get("operations")):
            raise ValueError("CRESCENDO-SUITE-RECOVERY: Unsafe state has no recorded stop evidence")
        # Publish the committed state mirror so capture reconciliation reads the
        # same durable state selected above. No ownership/stop gate is bypassed.
        atomic_write_json(os.path.join(owner["root"], "session_state.json"), state)
        for root in state.get("capture_roots") or []:
            if not os.path.isdir(root):
                if ((state.get("capture_kinds") or {}).get(root) == "native_baseline" and
                        (state.get("capture_proofs") or {}).get(root, {}).get("cleanup_proven") is not True):
                    raise ValueError("Private suite native baseline evidence directory is missing")
            elif not _reconcile_owned_capture(root, request_cancel=False).get("cleanup_proven"):
                raise ValueError("Private suite prior native activity is not proven stopped")
        from .legacy_guardian import quiesce_legacy_guardian_attempt
        for control in state.get("operations") or []:
            if state.get("unsafe") and not os.path.lexists(os.path.join(control, "legacy_guardian_launch.json")):
                raise ValueError("CRESCENDO-SUITE-RECOVERY: Unsafe operation has no guardian launch evidence")
            if os.path.lexists(os.path.join(control, "legacy_guardian_launch.json")):
                proof = quiesce_legacy_guardian_attempt(control, expected_workspace_root=owner["workspace_root"],
                                                        timeout_seconds=0, terminate_owned=False)
                if proof.get("state") != "dead":
                    raise ValueError("Private suite prior native activity is not proven stopped")
        # Partial compatibility removals may already lack their marker. Native
        # stop proof above plus exact move receipts authorize only those trees.
        for member in workspace.get("same_library_registry") or []:
            if os.path.lexists(member.get("path") or ""):
                _recovery_tree_identity(member["path"])
        registry = workspace.get("same_library_registry") or []
        if len(rows) != len(registry):
            raise ValueError("CRESCENDO-SUITE-RECOVERY: Backup member count differs")
        nonce = name[len("refresh_backup_"):]
        evidence = []
        def inspect_tree(path, member, expected_hash=None):
            st = _object_identity(path)
            if not stat.S_ISDIR(st.st_mode) or os.path.realpath(path) != path or os.path.ismount(path):
                raise ValueError("CRESCENDO-SUITE-RECOVERY: Unsafe recovery tree")
            marker = _load(os.path.join(path, MARKER))
            if marker.get("token") != member.get("token") or marker.get("target") != member.get("target"):
                raise ValueError("CRESCENDO-SUITE-RECOVERY: Recovery marker differs")
            for directory, dirs, files in os.walk(path, followlinks=False):
                for child_name in dirs + files:
                    child = os.path.join(directory, child_name)
                    child_st = _object_identity(child)
                    if child_st.st_dev != st.st_dev or os.path.ismount(child) or ".cdslck" in child_name:
                        raise ValueError("CRESCENDO-SUITE-RECOVERY: Lock or filesystem boundary in recovery tree")
            if expected_hash and tree_content_fingerprint(path)["fingerprint"] != expected_hash:
                raise ValueError("CRESCENDO-SUITE-RECOVERY: Backup fingerprint changed")
            return st
        init_path = os.path.join(backup_root, "cdsinit.backup")
        init_info = _object_identity(init_path)
        if not stat.S_ISREG(init_info.st_mode):
            raise ValueError("CRESCENDO-SUITE-RECOVERY: Invalid startup backup")
        with open(init_path, "r") as stream:
            old_init = stream.read()
        if hashlib.sha256(old_init.encode("utf-8")).hexdigest() != recovery.get("init_sha256"):
            raise ValueError("CRESCENDO-SUITE-RECOVERY: Startup backup changed")
        for index, (row, member) in enumerate(zip(rows, registry)):
            live = member.get("path")
            backup = os.path.join(backup_root, str(index))
            if row.get("path") != live or row.get("backup") != backup or not owns_view(workspace, member.get("target") or [], role=member.get("role")):
                raise ValueError("CRESCENDO-SUITE-RECOVERY: Backup member identity differs")
            inspect_tree(backup, member, row.get("fingerprint"))
            stage, previous = live + "__cr_recover_" + nonce, live + "__cr_before_" + nonce
            before_receipt = os.path.join(backup_root, "move_before_%d.json" % index)
            restore_receipt = os.path.join(backup_root, "move_restore_%d.json" % index)
            for src, dst, receipt in ((live, previous, before_receipt), (stage, live, restore_receipt)):
                if os.path.lexists(receipt):
                    _recovery_move(src, dst, receipt)
            live_exists, stage_exists, previous_exists = [os.path.lexists(p) for p in (live, stage, previous)]
            for path in (live, stage, previous):
                if os.path.lexists(path):
                    inspect_tree(path, member)
            if live_exists and previous_exists:
                if stage_exists or tree_content_fingerprint(live)["fingerprint"] != row.get("fingerprint"):
                    raise ValueError("CRESCENDO-SUITE-RECOVERY: Ambiguous partially recovered member")
            else:
                if not stage_exists:
                    shutil.copytree(backup, stage)
                    inspect_tree(stage, member, row.get("fingerprint"))
                else:
                    inspect_tree(stage, member, row.get("fingerprint"))
                if live_exists:
                    if previous_exists:
                        raise ValueError("CRESCENDO-SUITE-RECOVERY: Recovery previous view occupied")
                    _recovery_move(live, previous, before_receipt)
                    _fsync_directory(os.path.dirname(live))
                elif not previous_exists:
                    raise ValueError("CRESCENDO-SUITE-RECOVERY: Original live member is missing")
                _recovery_move(stage, live, restore_receipt)
                _fsync_directory(os.path.dirname(live))
                inspect_tree(live, member, row.get("fingerprint"))
            evidence.append(previous)
        atomic_write_text(os.path.join(workspace["root"], ".cdsinit"), old_init)
        restored_state = copy.deepcopy(old_state)
        for key in ("capture_roots", "capture_kinds", "capture_proofs", "operations"):
            restored_state[key] = copy.deepcopy(state.get(key))
        restored_state.update(status="released", closed=True, unsafe=False, refresh_pending=None)
        restored_workspace = copy.deepcopy(old_workspace)
        restore_private_views(restored_workspace)
        restored_state["private_views_read_only"] = False
        revision = uuid.uuid4().hex
        restored_state["_suite_revision"] = revision
        restored_workspace["_suite_revision"] = revision
        atomic_write_json(os.path.join(owner["root"], "suite_metadata_commit.json"), {
            "schema_version": "crescendo.suite_metadata_commit.v1", "owner": owner,
            "revision": revision, "state": restored_state, "workspace": restored_workspace})
        atomic_write_json(os.path.join(owner["root"], "session_state.json"), restored_state)
        atomic_write_json(os.path.join(owner["workspace_root"], "workspace.json"), restored_workspace)
        return {"recovered": True, "state": "released", "session_id": owner["session_id"],
                "retained_evidence": [backup_root] + evidence}
    finally:
        _release_lease(lease)

def delete_setup_private_session(reference, timeout_seconds=12):
    """Delete exactly registered views after proving all owned writers stopped."""
    controller = _controller(reference)
    if _quarantine(controller.owner):
        raise RuntimeError("CRESCENDO-SUITE-RECOVERY: Suite is isolated")
    deadline = time.monotonic() + max(0.0, float(timeout_seconds))
    if not controller.lock.acquire(timeout=max(0.0, deadline - time.monotonic())):
        return {"cleanup_proven": False, "state": "private_setup_generation_active"}
    try:
        if any(kind == "native_baseline" and not os.path.isdir(root)
               and controller.capture_proofs.get(root, {}).get("cleanup_proven") is not True
               for root, kind in controller.capture_kinds.items()):
            return {"cleanup_proven": False, "state": "private_setup_native_evidence_missing"}
        from .legacy_guardian import quiesce_legacy_guardian_attempt
        proofs = [_reconcile_owned_capture(root, request_cancel=True, timeout_seconds=max(0.0, deadline-time.monotonic()))
                  for root in sorted(controller.capture_roots) if os.path.isdir(root)]
        operations = [quiesce_legacy_guardian_attempt(control,
            expected_workspace_root=controller.owner["workspace_root"], timeout_seconds=max(0.0, deadline-time.monotonic()),
            terminate_owned=True) for control in controller.operations
            if os.path.lexists(os.path.join(control, "legacy_guardian_launch.json"))]
        if not all(row.get("cleanup_proven") for row in proofs) or any(row.get("state") != "dead" for row in operations):
            return {"cleanup_proven": False, "state": "private_setup_native_stop_unproven",
                    "preparations": proofs, "operations": operations}
        _read_suite(controller.owner, require_snapshot=False, allow_missing=True)
        access_workspace = _deletion_access_workspace(controller.workspace)
        check_private_view_edit_locks(access_workspace)
        controller._recover_restore_files()
        restore_private_views(access_workspace)
        from .same_library_views import cleanup_same_library_views
        result = cleanup_same_library_views(controller.workspace, process_cleanup_proven=True)
        if not result.get("cleanup_proven"):
            controller.unsafe, controller.status = True, "delete_incomplete"
            controller.persist()
            return dict(result, state=controller.status, session_id=controller.owner["session_id"])
        controller.closed, controller.status = True, "deleted"
        controller.persist()
        _release_lease(controller.lease)
        controller.lease = None
        with _REGISTRY_LOCK:
            _SESSIONS.pop(controller.owner["session_id"], None)
            _CLOSED.add(controller.owner["session_id"])
        return dict(result, state="deleted", session_id=controller.owner["session_id"])
    finally:
        controller.lock.release()


def rename_setup_private_session(reference, label, config, cancel_check=None):
    """Rebuild and validate an entire named suite before retiring the old views."""
    if not isinstance(label, str) or not label.strip():
        raise ValueError("Suite name must not be empty")
    controller = _controller(reference)
    prepared = setup_private_session_snapshot(reference)
    replacement = create_setup_private_session(prepared, config, cancel_check=cancel_check, label=label.strip())
    try:
        if controller.applied_selection.get("scope_ids") or controller.applied_selection.get("parameter_ids"):
            replacement = apply_setup_private_cells(replacement, prepared, config, cancel_check=cancel_check)
        new_controller = _controller(replacement)
        new_controller.owner  # ownership and lease remain held until caller releases it
        association = {"previous_session_id": controller.owner["session_id"],
                       "previous_root": controller.owner["root"],
                       "replacement_session_id": new_controller.owner["session_id"],
                       "replacement_root": new_controller.owner["root"],
                       "previous_state": _load(os.path.join(controller.owner["root"], "session_state.json")),
                       "previous_workspace": copy.deepcopy(controller.workspace)}
        atomic_write_json(os.path.join(new_controller.owner["root"], "replacement.json"), association)
        atomic_write_json(os.path.join(controller.owner["root"], "replacement.json"), association)
        result = delete_setup_private_session(reference)
        if not result.get("cleanup_proven"):
            raise RuntimeError("New suite was created but retirement of the old suite failed; both registries retained: " + str(result))
        return replacement
    except BaseException as error:
        error.replacement_setup_private_session = replacement
        raise
