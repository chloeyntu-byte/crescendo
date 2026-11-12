"""Owned, occurrence-specific views in a source cell; canonical views stay read only."""
from __future__ import print_function

import hashlib
import json
import os
import shutil
import uuid
import copy
import re
import socket
import stat
import pwd
import math
import tempfile

from crescendo_engine.runtime import bind_text
_contract_text = bind_text()

from .utils import atomic_write_json, atomic_write_text

SCHEMA = _contract_text("crescendo.same_library_views.v1")
MARKER = _contract_text(".crescendo-owned-view.json")


class RenameNoReplaceUnavailable(OSError):
    """The kernel or filesystem cannot perform a no-replace rename."""


def _rename_noreplace(source, target):
    """Atomically publish a directory without replacing any existing object.

    CentOS 7's libc may not export renameat2 although its kernel supports it.
    Unsupported architectures/kernels fail closed and retain staging evidence.
    """
    import ctypes
    import errno
    import platform
    machine = platform.machine().lower()
    number = {"x86_64": 316, "amd64": 316, "aarch64": 276,
              "arm64": 276, "i386": 353, "i686": 353}.get(machine)
    if number is None:
        raise RenameNoReplaceUnavailable(_contract_text("CRESCENDO-SUITE-RENAME: Atomic no-replace rename is unsupported on architecture ") + machine + "; staging and target retained for recovery")
    libc = ctypes.CDLL(None, use_errno=True)
    result = libc.syscall(ctypes.c_long(number), ctypes.c_int(-100),
                          ctypes.c_char_p(os.fsencode(source)), ctypes.c_int(-100),
                          ctypes.c_char_p(os.fsencode(target)), ctypes.c_uint(1))
    if result != 0:
        code = ctypes.get_errno()
        unavailable = set((errno.ENOSYS, errno.EINVAL,
                           getattr(errno, "EOPNOTSUPP", errno.ENOSYS),
                           getattr(errno, "ENOTSUP", errno.ENOSYS)))
        if code in unavailable:
            raise RenameNoReplaceUnavailable(code, _contract_text("CRESCENDO-SUITE-RENAME: Atomic no-replace rename is unavailable on this kernel/filesystem; staging and target retained for recovery (%s)") % os.strerror(code), target)
        if code == errno.EXDEV:
            raise OSError(code, _contract_text("CRESCENDO-SUITE-RENAME: Cross-device rename refused; staging and target retained (%s)") % os.strerror(code), target)
        raise OSError(code, os.strerror(code), target)



def _copy_tree_manifest(root):
    """Hash all regular entries and modes; reject links and special files."""
    from .private_view_access import _object_identity
    if os.path.realpath(root) != root or os.path.ismount(root):
        raise ValueError("copy tree has an indirect path or mount: " + root)
    device = _object_identity(root).st_dev
    result = []
    def visit(directory, relative):
        current = _object_identity(directory)
        if not stat.S_ISDIR(current.st_mode):
            raise ValueError("copy tree contains a non-directory: " + directory)
        result.append((relative, "directory", stat.S_IMODE(current.st_mode)))
        for name in sorted(os.listdir(directory)):
            path = os.path.join(directory, name)
            rel = os.path.join(relative, name)
            entry = _object_identity(path)
            if entry.st_dev != device or os.path.ismount(path) or ".cdslck" in name:
                raise ValueError("copy tree has a lock or filesystem boundary: " + path)
            if stat.S_ISDIR(entry.st_mode):
                visit(path, rel)
            elif stat.S_ISREG(entry.st_mode):
                digest = hashlib.sha256()
                fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
                with os.fdopen(fd, "rb") as stream:
                    if not os.path.samestat(entry, os.fstat(stream.fileno())):
                        raise ValueError("copy source changed: " + path)
                    for block in iter(lambda: stream.read(1024 * 1024), b""):
                        digest.update(block)
                result.append((rel, "file", stat.S_IMODE(entry.st_mode), digest.hexdigest()))
            else:
                raise ValueError("copy tree contains a link or special file: " + path)
    visit(root, "")
    return result


def copy_tree_exclusive(source, target, receipt_path):
    """Copy into an exclusively created directory; retain evidence on failure.

    This is not atomic publication. The caller must keep readers blocked until
    the complete receipt is committed, and handle ambiguous interrupted attempts.
    Only a receipt-bound partial target can be cleared and retried; callers
    must hold the suite lease and have proven native writers stopped.
    """
    from .utils import _fsync_directory
    from .private_view_access import _object_identity
    source, target, receipt_path = map(os.path.abspath, (source, target, receipt_path))
    if (source == target or os.path.dirname(source) != os.path.dirname(target) or
            os.path.realpath(os.path.dirname(source)) != os.path.dirname(source)):
        raise ValueError("exclusive copy requires same-parent source and target")
    original = os.lstat(source)
    if not stat.S_ISDIR(original.st_mode):
        raise ValueError("exclusive copy source is not a directory")
    manifest = _copy_tree_manifest(source)
    fingerprint = hashlib.sha256(json.dumps(manifest, ensure_ascii=True, separators=(",", ":")).encode("utf-8")).hexdigest()
    receipt = {"schema_version": _contract_text("crescendo.exclusive_tree_copy.v1"), "fingerprint": fingerprint, "source": source,
               "target": target, "source_dev": original.st_dev,
               "source_ino": original.st_ino, "status": "intent"}
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(receipt_path, flags, 0o600)
    except FileExistsError:
        descriptor = os.open(receipt_path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        with os.fdopen(descriptor, "r") as stream:
            existing = json.load(stream)
        identity = _object_identity(target)
        if (existing.get("schema_version") != receipt["schema_version"] or
                existing.get("source") != source or existing.get("target") != target or
                existing.get("source_dev") != original.st_dev or existing.get("source_ino") != original.st_ino or
                existing.get("fingerprint") != fingerprint or not stat.S_ISDIR(identity.st_mode) or
                (identity.st_dev, identity.st_ino) != (existing.get("target_dev"), existing.get("target_ino"))):
            raise ValueError("exclusive copy receipt or target identity changed")
        if existing.get("status") == "complete":
            if _copy_tree_manifest(target) != manifest:
                raise ValueError("exclusive copy completed target changed")
            return existing
        if existing.get("status") != "copying":
            raise ValueError("exclusive copy reservation has no proven identity")
        from .retention import _reject_unsafe_regular_tree
        _reject_unsafe_regular_tree(target, identity.st_dev, "exclusive partial copy")
        allowed = {entry[0]: entry[1] for entry in manifest}
        for directory, dirs, files in os.walk(target):
            for name in dirs + files:
                path = os.path.join(directory, name)
                rel = os.path.relpath(path, target)
                st = _object_identity(path)
                kind = "directory" if stat.S_ISDIR(st.st_mode) else "file"
                if ".cdslck" in name or allowed.get(rel) != kind:
                    raise ValueError("exclusive partial copy has an unrecognized entry: " + path)
        # The lease and native-stop proof are caller preconditions. Preserve
        # the reserved root inode so no other creator can claim this name.
        os.chmod(target, stat.S_IMODE(identity.st_mode) | 0o700)
        for directory, dirs, files in os.walk(target, topdown=True):
            for name in dirs:
                path = os.path.join(directory, name)
                os.chmod(path, stat.S_IMODE(os.lstat(path).st_mode) | 0o700)
        for name in os.listdir(target):
            path = os.path.join(target, name)
            if stat.S_ISDIR(os.lstat(path).st_mode):
                shutil.rmtree(path)
            else:
                os.unlink(path)
        _fsync_directory(target)
        receipt = existing
    else:
        with os.fdopen(fd, "w") as stream:
            json.dump(receipt, stream, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        _fsync_directory(os.path.dirname(receipt_path))
        os.mkdir(target, 0o700)
        _fsync_directory(os.path.dirname(target))
        identity = os.lstat(target)
        receipt.update(target_dev=identity.st_dev, target_ino=identity.st_ino, status="copying")
        atomic_write_json(receipt_path, receipt)
    def copy_dir(src, dst, root=False):
        names = sorted(os.listdir(src), key=lambda n: (root and n == MARKER, n))
        for name in names:
            a, b = os.path.join(src, name), os.path.join(dst, name)
            entry = os.lstat(a)
            if stat.S_ISDIR(entry.st_mode):
                os.mkdir(b, 0o700)
                copy_dir(a, b)
                os.chmod(b, stat.S_IMODE(entry.st_mode))
                _fsync_directory(b)
            elif stat.S_ISREG(entry.st_mode):
                source_fd = os.open(a, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
                with os.fdopen(source_fd, "rb") as reader:
                    if not os.path.samestat(entry, os.fstat(reader.fileno())):
                        raise ValueError("copy source changed: " + a)
                    target_fd = os.open(b, flags, 0o600)
                    with os.fdopen(target_fd, "wb") as writer:
                        shutil.copyfileobj(reader, writer, 1024 * 1024)
                        os.fchmod(writer.fileno(), stat.S_IMODE(entry.st_mode))
                        writer.flush()
                        os.fsync(writer.fileno())
            else:
                raise ValueError("copy tree contains a link or special file: " + a)
        _fsync_directory(dst)
    copy_dir(source, target, True)
    os.chmod(target, stat.S_IMODE(original.st_mode))
    _fsync_directory(target)
    if (not os.path.samestat(original, os.lstat(source)) or
            not os.path.samestat(identity, os.lstat(target)) or
            manifest != _copy_tree_manifest(source) or manifest != _copy_tree_manifest(target)):
        raise ValueError("exclusive copy identity or content changed")
    receipt["status"] = "complete"
    atomic_write_json(receipt_path, receipt)
    return receipt

def enabled(config, parameters):
    return parameters is not None and ((config or {}).get("cadence") or {}).get(
        "workspace_copy_strategy", "same_library_views") == "same_library_views"


def _design(row):
    return tuple(row.get("design_" + name) or ("schematic" if name == "view" else None)
                 for name in ("library", "cell", "view"))


def _quote(value):
    return json.dumps(str(value), ensure_ascii=True)


def config_designs_for_same_library(hierarchy, parameters):
    from .config_binding import config_designs
    selected = set(_design(row) for row in parameters or [] if row.get("backend", "schematic") == "schematic")
    known = dict((tuple(row[name] for name in ("library", "cell", "view")), row)
                 for row in config_designs(hierarchy))
    result = []
    for design in (hierarchy or {}).get("designs") or []:
        requested = design.get("design") or design
        key = tuple(requested.get(name) for name in ("library", "cell", "view")) if isinstance(requested, dict) else tuple(requested)
        if key not in selected:
            continue
        if key in known:
            result.append(copy.deepcopy(known[key]))
            continue
        payload = design.get("hierarchy") or {}
        cells = dict((row.get("hierarchy_path") or "/", row) for row in payload.get("cells") or [])
        from .config_hierarchy import is_schematic_implementation
        top = next((row for row in payload.get("cells") or []
                    if tuple(row.get(name) for name in ("library", "cell", "view")) == key), {})
        top_type = top.get("view_type")
        # Older plain-schematic probes did not persist their OA type. Custom
        # names need the explicit type evidence emitted by current probes.
        if top_type is None and key[2] == "schematic":
            top_type = "schematic"
        if not is_schematic_implementation(key[2], top_type):
            raise ValueError("same-library adapter requires an observed schematic implementation or config hierarchy")
        rows = []
        for source in payload.get("instances") or []:
            row = dict(source)
            path = row.get("hierarchy_path")
            parent = row.get("parent_path") or "/"
            owner = cells.get(parent)
            if not owner:
                raise ValueError("ordinary schematic hierarchy lacks owner evidence: %s" % path)
            if (row.get("master_library") == "basic" and row.get("master_cell") in ("ipin", "opin", "iopin")) or (row.get("master_library") == "analogLib" and row.get("master_cell") == "gnd"):
                continue
            actual = cells.get(path)
            if actual:
                target = tuple(actual.get(name) or ("schematic" if name == "view" else None) for name in ("library", "cell", "view"))
            else:
                # Leaf devices retain the native Spectre view. Opaque views from
                # an observed resolver retain their actual implementation.
                target = tuple(row.get("resolved_" + name) or row.get("master_" + name) for name in ("library", "cell", "view"))
                target = target[:2] + ((row.get("resolved_view") or "spectre"),)
            row.update({"resolved_library": target[0], "resolved_cell": target[1], "resolved_view": target[2],
                        "owner_library": owner["library"], "owner_cell": owner["cell"], "owner_view": owner.get("view") or "schematic",
                        "stop": actual is None, "scan_traversable": actual is not None,
                        "fixed_library": True, "fixed_cell": True, "fixed_view": False})
            rows.append(row)
        result.append({"library": key[0], "cell": key[1], "view": key[2],
                       "resolved_top": dict(zip(("library", "cell", "view"), key)),
                       "config_bindings": rows, "config_dependencies": [], "generated_config": True})
    if selected != set(tuple(row[name] for name in ("library", "cell", "view")) for row in result):
        raise ValueError("selected design lacks complete inspected hierarchy")
    return result


def register_view(workspace, source, key, role="working", target_view=None):
    """Reserve an absent exact view before launching native writes; no prefix scans."""
    source = tuple(source)
    token = workspace.setdefault("same_library_token", uuid.uuid4().hex)
    registry = workspace.setdefault("same_library_registry", [])
    identity = list(key)
    for row in registry:
        if row["key"] == identity and row["role"] == role:
            return row
    library, cell, view = source
    root = (workspace.get("source_libraries") or {}).get(library)
    if not root:
        raise ValueError("selected source library is unresolved: %s" % library)
    if any(not value or os.path.basename(value) != value or value in (".", "..") for value in source):
        raise ValueError("unsafe source cellview identity")
    if target_view is None:
        digest = hashlib.sha256((token + "\x1f" + role + "\x1f" + repr(identity)).encode("utf-8")).hexdigest()[:16]
        label = re.sub(r"[^A-Za-z0-9_]+", "_", str(workspace.get("run_display_name") or "optimization")).strip("_")[:32] or "optimization"
        target_view = _contract_text("cr_s_%s_%s_%s_%s") % (token[:8], label, role, digest[:10])
    path = os.path.join(os.path.realpath(root), cell, target_view)
    if os.path.lexists(path):
        raise ValueError(_contract_text("new Crescendo view already exists: %s/%s/%s") % (library, cell, target_view))
    row = {"key": identity, "role": role, "source": list(source),
           "target": [library, cell, target_view], "path": path,
           "token": token, "existed_before": False}
    old_registry = copy.deepcopy(registry)
    registry.append(row)
    if workspace.get("setup_private_session") and workspace.get("root") and os.path.isdir(workspace["root"]):
        atomic_write_json(os.path.join(workspace["root"], _contract_text("suite_registry_intent.json")), {
            "schema_version": _contract_text("crescendo.suite_registry_intent.v1"), "owner": workspace["setup_private_session"],
            "old_views": old_registry, "workspace": workspace})
    if workspace.get("root") and os.path.isdir(workspace["root"]):
        atomic_write_json(os.path.join(workspace["root"], _contract_text("same_library_views.json")),
                          {"schema_version": SCHEMA, "token": token, "views": registry})
        atomic_write_json(os.path.join(workspace["root"], _contract_text("workspace.json")), workspace)
    return row


def owns_view(workspace, target, role=None, require_marker=False):
    """Prove an exact new cellview; a source library is never owned wholesale."""
    for row in workspace.get("same_library_registry") or []:
        role_matches = not role or row.get("role") == role or (role == "config" and row.get("role") == "batch_config")
        if tuple(row.get("target") or ()) != tuple(target) or not role_matches:
            continue
        library, cell, view = target
        root = (workspace.get("source_libraries") or {}).get(library)
        expected = os.path.join(os.path.realpath(root or ""), cell, view)
        if (not root or row.get("path") != expected or row.get("existed_before") is not False
                or row.get("token") != workspace.get("same_library_token")
                or not view.startswith(_contract_text("cr_s_") + row["token"][:8] + "_")
                or tuple(row.get("source") or ()) == tuple(target)):
            return False
        if require_marker:
            try:
                with open(os.path.join(expected, MARKER)) as stream:
                    marker = json.load(stream)
                if os.path.islink(expected) or os.path.realpath(expected) != expected or marker.get("token") != row["token"] or marker.get("target") != row["target"]:
                    return False
            except (OSError, ValueError):
                return False
        return True
    return False


def ownership_marker_lines(record):
    payload = json.dumps({"schema_version": SCHEMA, "token": record["token"],
                          "target": record["target"]}, sort_keys=True)
    return [_contract_text('crescOwnedPort = outfile(%s)') % _quote(os.path.join(record["path"], MARKER)),
            _contract_text('unless(crescOwnedPort error("Crescendo cannot persist view ownership\\n"))'),
            _contract_text('fprintf(crescOwnedPort "%%s" %s)') % _quote(payload), _contract_text('close(crescOwnedPort)')]


def copy_setup_view(workspace, source, key):
    """Copy only the Maestro file view, excluding histories and backup copies."""
    if not workspace.get("setup_private_session"):
        return _legacy_copy_setup_view(workspace, source, key)
    from .workspace import copy_ignore, _copy_private_file, _make_private_tree_owner_writable
    record = register_view(workspace, source, key, role="maestro")
    from .setup_baseline import effective_setup_dir
    origin = (effective_setup_dir(workspace["setup"]) if workspace["setup"].get("baseline") else
              os.path.join(workspace["source_libraries"][source[0]], source[1], source[2]))
    # The registered target remains absent until a complete marked copy exists.
    # Stages are never treated as owned views after an interrupted process.
    if workspace.get("setup_private_session"):
        stage = os.path.join(os.path.dirname(record["path"]), _contract_text(".crescendo-create-") + uuid.uuid4().hex)
        key = hashlib.sha256(record["path"].encode("utf-8")).hexdigest()
        atomic_write_json(os.path.join(workspace["root"], "suite_create_" + key + ".json"), {
            "schema_version": _contract_text("crescendo.suite_view_create.v1"), "owner": workspace["setup_private_session"],
            "row": record, "stage": stage})
        os.mkdir(stage, 0o700)
    else:
        stage = tempfile.mkdtemp(prefix=_contract_text(".crescendo-create-"), dir=os.path.dirname(record["path"]))
    try:
        atomic_write_json(os.path.join(stage, MARKER), {
            "schema_version": SCHEMA, "token": record["token"], "target": record["target"]})
        def ignore(directory, names):
            omitted = copy_ignore(directory, names)
            omitted.update(name for name in names if name == MARKER or name.endswith("~") or ".bak" in name)
            return omitted
        for name in sorted(os.listdir(origin)):
            if name in ignore(origin, [name]):
                continue
            src, dst = os.path.join(origin, name), os.path.join(stage, name)
            if os.path.isdir(src):
                shutil.copytree(src, dst, symlinks=False, ignore=ignore, copy_function=_copy_private_file)
            else:
                _copy_private_file(src, dst)
        _make_private_tree_owner_writable(stage)
        if os.path.lexists(record["path"]):
            raise ValueError(_contract_text("new Crescendo view became occupied before publication"))
        try:
            _rename_noreplace(stage, record["path"])
        except RenameNoReplaceUnavailable:
            receipt = os.path.join(workspace["root"], "suite_copy_" + key + ".json")
            copy_tree_exclusive(stage, record["path"], receipt)
            shutil.rmtree(stage)
        from .utils import _fsync_directory
        _fsync_directory(os.path.dirname(record["path"]))
    except BaseException:
        # Leave interrupted staging evidence alone: it was never published as a view.
        raise
    return record



def _legacy_copy_setup_view(workspace, source, key):
    """Copy only the Maestro file view, excluding histories and backup copies."""
    from .workspace import copy_ignore, _copy_private_file, _make_private_tree_owner_writable
    record = register_view(workspace, source, key, role="maestro")
    from .setup_baseline import effective_setup_dir
    origin = (effective_setup_dir(workspace["setup"]) if workspace["setup"].get("baseline") else
              os.path.join(workspace["source_libraries"][source[0]], source[1], source[2]))
    os.mkdir(record["path"])
    atomic_write_json(os.path.join(record["path"], MARKER), {
        "schema_version": SCHEMA, "token": record["token"], "target": record["target"]})
    def ignore(directory, names):
        omitted = copy_ignore(directory, names)
        omitted.update(name for name in names if name.endswith("~") or ".bak" in name)
        return omitted
    for name in sorted(os.listdir(origin)):
        if name in ignore(origin, [name]):
            continue
        src, dst = os.path.join(origin, name), os.path.join(record["path"], name)
        if os.path.isdir(src):
            shutil.copytree(src, dst, symlinks=False, ignore=ignore, copy_function=_copy_private_file)
        else:
            _copy_private_file(src, dst)
    _make_private_tree_owner_writable(record["path"])
    return record


def build_same_library_clone_plan(workspace, parameters, values, clone_state="absent"):
    result = {}
    for parameter in parameters:
        if parameter.get("backend", "schematic") != "schematic" or parameter["parameter_id"] not in values:
            continue
        design = _design(parameter)
        path = parameter.get("owner_occurrence_path") or "/"
        key = design + (path,)
        if key in result:
            continue
        source = (parameter["owner_library"], parameter["owner_cell"], parameter.get("owner_view") or "schematic")
        record = register_view(workspace, source, key)
        exists = os.path.isdir(record["path"])
        if exists:
            marker = os.path.join(record["path"], MARKER)
            try:
                with open(marker) as stream:
                    owned = json.load(stream)
            except (ValueError, OSError):
                raise ValueError(_contract_text("existing Crescendo view has no ownership evidence: %s") % record["path"])
            if owned.get("token") != record["token"] or owned.get("target") != record["target"]:
                raise ValueError(_contract_text("Crescendo view ownership mismatch: %s") % record["path"])
        if clone_state == "existing" and not exists:
            raise ValueError(_contract_text("expected Crescendo view is absent: %s") % record["path"])
        chain = parameter.get("occurrence_chain") or []
        occurrence = next((row for row in chain if row.get("hierarchy_path") == path), {})
        if path != "/" and not occurrence:
            raise ValueError("selected owner occurrence evidence is absent: %s" % path)
        if occurrence.get("fixed_view"):
            raise ValueError("selected occurrence fixes its view; choose the copied-library adapter: %s" % path)
        result[key] = {"design_library": design[0], "design_cell": design[1], "design_view": design[2],
                       "occurrence_path": path, "parent_occurrence_path": occurrence.get("parent_path") or "/",
                       "instance": occurrence.get("name"), "source_library": source[0], "source_cell": source[1],
                       "source_view": source[2], "source_view_type": parameter.get("owner_view_type") or "schematic",
                       "clone_library": source[0], "clone_cell": source[1], "target_view": record["target"][2],
                       "master_view": occurrence.get("master_view") or "symbol", "master_view_type": "schematicSymbol",
                       "parent_cell": occurrence.get("owner_cell") or parameter.get("design_cell"),
                       "clone_preexisting": exists, "same_library_view": True, "owned_view": record,
                       "parameter_owner_view": source[2]}
    return [result[key] for key in sorted(result)]


def copy_lines(clones, verify_port=_contract_text("crescCloneVerify")):
    lines = []
    for clone in clones:
        if clone.get("clone_preexisting"):
            continue
        source = (clone["source_library"], clone["source_cell"], clone["source_view"])
        target = (clone["clone_library"], clone["clone_cell"], clone["target_view"])
        record = clone["owned_view"]
        marker = os.path.join(record["path"], MARKER)
        payload = json.dumps({"schema_version": SCHEMA, "token": record["token"],
                              "target": record["target"]}, sort_keys=True)
        lines.extend([
            _contract_text('unless(ddGetObj(%s) == nil error("Crescendo owned destination view already exists\\n"))') % ' '.join(_quote(v) for v in target),
            'srcCv = dbOpenCellViewByType(%s %s "r")' % (' '.join(_quote(v) for v in source), _quote(clone["source_view_type"])),
            _contract_text('unless(srcCv error("Crescendo cannot open selected owner schematic\\n"))'),
            'dstCv = dbCopyCellView(srcCv %s)' % ' '.join(_quote(v) for v in target),
            _contract_text('unless(dstCv error("Crescendo cannot create selected owner view\\n"))'),
            _contract_text('unless(dbSave(dstCv) error("Crescendo cannot save selected owner view\\n"))'),
            'dbClose(srcCv)', 'dbClose(dstCv)',
            _contract_text('crescOwnedPort = outfile(%s)') % _quote(marker),
            _contract_text('unless(crescOwnedPort error("Crescendo cannot persist view ownership\\n"))'),
            _contract_text('fprintf(crescOwnedPort "%%s" %s)') % _quote(payload),
            _contract_text('close(crescOwnedPort)'),
            'fprintf(%s "CLONE\\t%%L\\t%%L\\t%%L\\t%%L\\n" %s)' % (verify_port, ' '.join(_quote(v or "") for v in (clone["occurrence_path"], clone["source_cell"], clone["clone_cell"], clone.get("parent_cell")))),
        ])
    return lines


def _stale_owned_lock_proof(workspace, lock_path, evidence):
    """Reconcile only this run's dead native edit lock; foreign locks stay put."""
    if not isinstance(evidence, dict) or not (
            evidence.get("process_termination_proven") is True
            and evidence.get("remote_stop_proven") is True):
        raise ValueError("full process and remote termination proof is required for edit locks")
    if not lock_path.endswith(".cdslck") or os.path.realpath(lock_path) != lock_path:
        raise ValueError("edit lock path is not a direct regular owned path")
    descriptor = os.open(lock_path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        identity = os.fstat(descriptor)
        if not stat.S_ISREG(identity.st_mode) or identity.st_nlink != 1 or identity.st_size > 8192:
            raise ValueError("edit lock file identity is unsupported")
        raw = os.read(descriptor, 8193)
        after = os.fstat(descriptor)
        if (identity.st_dev, identity.st_ino, identity.st_size, identity.st_mtime_ns) != (
                after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
            raise ValueError("edit lock changed during reconciliation")
    finally:
        os.close(descriptor)
    text = raw.decode("utf-8")
    fields = {}
    for line in text.splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        parts = line.split(None, 1)
        if len(parts) == 2:
            if parts[0] in fields:
                raise ValueError("edit lock has duplicate identity fields")
            fields[parts[0]] = parts[1].strip()
    if fields.get("LockStakeVersion") != "1.1" or fields.get("HostName") != socket.gethostname():
        raise ValueError("edit lock format or host is not this native machine")
    if fields.get("FilePathUsedToEditLock") != lock_path[:-len(".cdslck")]:
        raise ValueError("edit lock target differs from the exact owned file")
    try:
        pid = int(fields["ProcessIdentifier"])
        created = int(fields["ProcessCreationTime_UTC"])
    except (KeyError, ValueError):
        raise ValueError("edit lock process creation identity is unavailable")
    if pid <= 1 or created <= 0 or os.path.lexists("/proc/%d" % pid):
        raise ValueError("edit lock PID is alive or reused; lock retained")
    for inspection in evidence.get("guardian_inspections") or []:
        guardian = inspection.get("guardian") or {}
        manifest = inspection.get("manifest") or {}
        proof = inspection.get("termination_proof") or {}
        if not (inspection.get("state") == "dead" and proof.get("ok") is True
                and guardian.get("status") == "exited"
                and guardian.get("group_cleanup_complete") is True
                and guardian.get("descendant_cleanup_complete") is True
                and guardian.get("pid") == pid and guardian.get("pid_start_ticks")
                and guardian.get("cwd") == workspace["root"]
                and manifest.get("workspace_root") == workspace["root"]
                and guardian.get("token") and guardian.get("token") == manifest.get("token")):
            continue
        uid = manifest.get("expected_uid")
        if isinstance(uid, bool) or uid is None or identity.st_uid != uid or fields.get("LoginName") != pwd.getpwuid(uid).pw_name:
            continue
        # Cadence writes whole seconds, while the launch record is fractional.
        started = float(guardian.get("started_epoch") or 0)
        if not math.isfinite(started) or abs(created - started) >= 2:
            continue
        current = os.lstat(lock_path)
        if (current.st_dev, current.st_ino, current.st_size, current.st_mtime_ns) != (
                identity.st_dev, identity.st_ino, identity.st_size, identity.st_mtime_ns):
            raise ValueError("edit lock was replaced during reconciliation")
        if os.path.lexists("/proc/%d" % pid):
            raise ValueError("edit lock PID reappeared; lock retained")
        return {"path": lock_path, "state": "proven_owned_stale", "lock_text": text,
                "lock_sha256": hashlib.sha256(raw).hexdigest(), "lock_identity": fields,
                "native_identity": {"pid": pid, "pid_start_ticks": guardian["pid_start_ticks"],
                                    "started_epoch": guardian["started_epoch"], "token": guardian["token"]},
                "termination_proof": proof}
    raise ValueError("edit lock does not match this run's proven-dead native identity")



def in_place_deletion_active(workspace, row):
    """Verify a partial in-place deletion receipt without changing disk."""
    from .private_view_access import _object_identity
    from .retention import _reject_unsafe_regular_tree
    path = row.get("path") or ""
    key = hashlib.sha256(path.encode("utf-8")).hexdigest()
    receipt_path = os.path.join(workspace["root"], "suite_delete_" + key + ".json")
    if not os.path.lexists(receipt_path):
        return False
    before = _object_identity(receipt_path)
    fd = os.open(receipt_path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    with os.fdopen(fd, "r") as stream:
        if not os.path.samestat(before, os.fstat(stream.fileno())):
            raise ValueError("deletion receipt changed")
        receipt = json.load(stream)
    if receipt.get("mode") != "in_place":
        return False
    library, cell, view = row["target"]
    base = (workspace.get("source_libraries") or {}).get(library)
    tomb = os.path.join(os.path.dirname(path), _contract_text(".crescendo-delete-") + key[:32])
    if (not base or path != os.path.join(os.path.realpath(base), cell, view) or
            os.path.realpath(os.path.dirname(path)) != os.path.dirname(path) or
            row.get("existed_before") is not False or row.get("token") != workspace.get("same_library_token") or
            not view.startswith(_contract_text("cr_s_") + str(row.get("token"))[:8] + "_") or
            view == row["source"][2] or
            receipt.get("schema_version") != _contract_text("crescendo.suite_view_delete.v1") or
            receipt.get("row") != row or receipt.get("path") != path or receipt.get("tomb") != tomb or
            os.path.lexists(tomb)):
        raise ValueError("in-place deletion receipt identity mismatch")
    if os.path.lexists(path):
        current = _object_identity(path)
        if (not stat.S_ISDIR(current.st_mode) or os.path.ismount(path) or
                (current.st_dev, current.st_ino) != (receipt.get("dev"), receipt.get("ino"))):
            raise ValueError("in-place deletion directory identity changed")
        _reject_unsafe_regular_tree(path, current.st_dev, "suite in-place deletion")
        for directory, dirs, files in os.walk(path):
            for name in dirs + files:
                _object_identity(os.path.join(directory, name))
                if ".cdslck" in name:
                    raise ValueError("native lock retained in deletion view")
    return True

def _delete_transaction(workspace, row):
    """Move one proven view to an exact same-parent tombstone before deletion.

    The durable receipt binds the directory inode before rename. A retry may
    finish a partly removed tombstone without relying on its deleted marker.
    """
    from .private_view_access import _object_identity
    from .retention import _reject_unsafe_regular_tree
    from .utils import _fsync_directory
    path = row["path"]
    library, cell, view = row["target"]
    base = (workspace.get("source_libraries") or {}).get(library)
    expected = os.path.join(os.path.realpath(base or ""), cell, view)
    if (not base or path != expected or row.get("existed_before") is not False or
            row.get("token") != workspace.get("same_library_token") or
            not view.startswith(_contract_text("cr_s_") + str(row.get("token"))[:8] + "_") or
            view == row["source"][2] or os.path.realpath(os.path.dirname(path)) != os.path.dirname(path)):
        raise ValueError("ownership mismatch")
    key = hashlib.sha256(path.encode("utf-8")).hexdigest()
    tomb = os.path.join(os.path.dirname(path), _contract_text(".crescendo-delete-") + key[:32])
    receipt_path = os.path.join(workspace["root"], "suite_delete_" + key + ".json")
    receipt = None
    if os.path.lexists(receipt_path):
        before = _object_identity(receipt_path)
        fd = os.open(receipt_path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        with os.fdopen(fd, "r") as stream:
            if not os.path.samestat(before, os.fstat(stream.fileno())):
                raise ValueError("deletion receipt changed")
            receipt = json.load(stream)
        if (receipt.get("schema_version") != _contract_text("crescendo.suite_view_delete.v1") or
                receipt.get("row") != row or receipt.get("path") != path or
                receipt.get("tomb") != tomb):
            raise ValueError("deletion receipt identity mismatch")
    if receipt is None:
        if not os.path.lexists(path):
            if os.path.lexists(tomb):
                raise ValueError("unregistered deletion tombstone")
            return False
        if os.path.lexists(tomb):
            raise ValueError("deletion tombstone already exists")
        if not owns_view(workspace, row["target"], role=row["role"], require_marker=True):
            raise ValueError("ownership marker mismatch")
        root_stat = _object_identity(path)
        if not stat.S_ISDIR(root_stat.st_mode) or os.path.ismount(path):
            raise ValueError("unsafe view directory")
        _reject_unsafe_regular_tree(path, root_stat.st_dev, "suite deletion")
        receipt = {"schema_version": _contract_text("crescendo.suite_view_delete.v1"), "row": row,
                   "path": path, "tomb": tomb, "dev": root_stat.st_dev,
                   "ino": root_stat.st_ino}
        atomic_write_json(receipt_path, receipt)
    if receipt.get("mode") == "in_place":
        if os.path.lexists(tomb):
            raise ValueError("in-place deletion has an unexpected tombstone")
        if os.path.lexists(path):
            current = _object_identity(path)
            if (not stat.S_ISDIR(current.st_mode) or os.path.ismount(path) or
                    (current.st_dev, current.st_ino) != (receipt.get("dev"), receipt.get("ino"))):
                raise ValueError("in-place deletion directory identity changed")
            _reject_unsafe_regular_tree(path, current.st_dev, "suite in-place deletion")
            for directory, dirs, files in os.walk(path):
                for name in dirs + files:
                    _object_identity(os.path.join(directory, name))
                    if ".cdslck" in name:
                        raise ValueError("native lock retained in deletion view")
            shutil.rmtree(path)
            _fsync_directory(os.path.dirname(path))
        if os.path.lexists(path):
            raise ValueError("in-place view deletion could not be verified")
        return True
    if receipt.get("mode") not in (None, "tombstone"):
        raise ValueError("unknown deletion receipt mode")
    if os.path.lexists(path):
        if os.path.lexists(tomb):
            raise ValueError("both live view and deletion tombstone exist")
        current = _object_identity(path)
        if (current.st_dev, current.st_ino) != (receipt.get("dev"), receipt.get("ino")):
            raise ValueError("view inode changed before deletion")
        if not owns_view(workspace, row["target"], role=row["role"], require_marker=True):
            raise ValueError("view marker changed before deletion")
        try:
            _rename_noreplace(path, tomb)
        except RenameNoReplaceUnavailable:
            current = _object_identity(path)
            if ((current.st_dev, current.st_ino) != (receipt.get("dev"), receipt.get("ino")) or
                    os.path.lexists(tomb) or
                    not owns_view(workspace, row["target"], role=row["role"], require_marker=True)):
                raise ValueError("view identity changed before in-place deletion")
            receipt["mode"] = "in_place"
            atomic_write_json(receipt_path, receipt)
            return _delete_transaction(workspace, row)
        _fsync_directory(os.path.dirname(path))
    if os.path.lexists(tomb):
        current = _object_identity(tomb)
        if (not stat.S_ISDIR(current.st_mode) or os.path.ismount(tomb) or
                (current.st_dev, current.st_ino) != (receipt.get("dev"), receipt.get("ino"))):
            raise ValueError("deletion tombstone identity changed")
        _reject_unsafe_regular_tree(tomb, current.st_dev, "suite deletion tombstone")
        for directory, dirs, files in os.walk(tomb):
            for name in dirs + files:
                child = os.path.join(directory, name)
                _object_identity(child)
                if ".cdslck" in name:
                    raise ValueError("native lock retained in deletion tombstone")
        shutil.rmtree(tomb)
        _fsync_directory(os.path.dirname(tomb))
    if os.path.lexists(path) or os.path.lexists(tomb):
        raise ValueError("view deletion could not be verified")
    return True


def cleanup_same_library_views(workspace, process_cleanup_proven=False, process_cleanup_evidence=None):
    """Remove exact owned views through durable same-filesystem tombstones."""
    if not workspace.get("setup_private_session"):
        return _legacy_cleanup_same_library_views(workspace, process_cleanup_proven, process_cleanup_evidence)
    registry_path = os.path.join(workspace["root"], _contract_text("same_library_views.json"))
    registry = workspace.get("same_library_registry") or []
    if os.path.isfile(registry_path):
        with open(registry_path) as stream:
            registry = json.load(stream).get("views") or []
    if not process_cleanup_proven:
        return {"cleanup_proven": False, "removed": [], "retained": [row["path"] for row in registry]}
    removed, retained, errors = [], [], []
    for row in registry:
        try:
            if _delete_transaction(workspace, row):
                removed.append(row["path"])
        except Exception as error:
            retained.append(row["path"])
            errors.append({"path": row["path"], "error": "%s: %s" % (error.__class__.__name__, error),
                           "errno": getattr(error, "errno", None)})
    result = {"cleanup_proven": not retained, "removed": removed, "retained": retained,
              "lock_reconciliation": [], "errors": errors}
    atomic_write_json(os.path.join(workspace["root"], _contract_text("same_library_cleanup.json")), result)
    return result


def _legacy_cleanup_same_library_views(workspace, process_cleanup_proven=False, process_cleanup_evidence=None):
    """Remove only exact newly-created view directories after owned processes end."""
    registry_path = os.path.join(workspace["root"], _contract_text("same_library_views.json"))
    registry = workspace.get("same_library_registry") or []
    if os.path.isfile(registry_path):
        with open(registry_path) as stream:
            registry = json.load(stream).get("views") or []
    if not registry:
        return {"cleanup_proven": True, "removed": [], "retained": []}
    if not process_cleanup_proven:
        return {"cleanup_proven": False, "removed": [], "retained": [row["path"] for row in registry if os.path.lexists(row["path"])]}
    removed, retained, reconciled, errors = [], [], [], []
    for row in registry:
        path = row["path"]
        if not os.path.lexists(path):
            continue
        library, cell, view = row["target"]
        library_root = (workspace.get("source_libraries") or {}).get(library)
        expected = os.path.join(os.path.realpath(library_root or ""), cell, view)
        try:
            with open(os.path.join(path, MARKER)) as stream:
                marker = json.load(stream)
            if (row.get("existed_before") is not False or path != expected or os.path.realpath(path) != path
                    or os.path.islink(path) or marker.get("token") != row.get("token")
                    or row.get("token") != workspace.get("same_library_token")
                    or not view.startswith(_contract_text("cr_s_") + str(row.get("token"))[:8] + "_")
                    or marker.get("target") != row["target"] or view == row["source"][2]):
                raise ValueError("ownership mismatch")
            # A lock requires stronger, exact child identity evidence than the
            # ordinary process-ended fence. Never infer staleness from its name.
            locks = [os.path.join(directory, name) for directory, _, names in os.walk(path)
                     for name in names if ".cdslck" in name]
            row_reconciled = [_stale_owned_lock_proof(workspace, lock, process_cleanup_evidence)
                              for lock in locks]
            reconciled.extend(row_reconciled)
            if row_reconciled:
                atomic_write_json(os.path.join(workspace["root"], _contract_text("same_library_cleanup.json")), {
                    "cleanup_proven": False, "removed": removed, "retained": retained + [path],
                    "lock_reconciliation": reconciled, "errors": errors})
            shutil.rmtree(path)
            removed.append(path)
        except (ValueError, OSError, UnicodeError, KeyError) as error:
            retained.append(path)
            errors.append({"path": path, "error": "%s: %s" % (error.__class__.__name__, error),
                           "errno": getattr(error, "errno", None)})
    result = {"cleanup_proven": not retained, "removed": removed, "retained": retained,
              "lock_reconciliation": reconciled, "errors": errors}
    atomic_write_json(os.path.join(workspace["root"], _contract_text("same_library_cleanup.json")), result)
    return result
