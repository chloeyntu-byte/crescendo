from __future__ import print_function

import ast
import fnmatch
import hashlib
import json
import os
import re
import shlex
import shutil
import stat
import threading
import time
import uuid

from .cadence import (
    build_guardian_command, check_command_cancellation, child_environment,
    detect_pdk_context, nearest_cds_lib, run_command,
)
from .hierarchy import SPICE_SUFFIXES, parse_physical_number
from .config_binding import config_designs, config_binding_lines, config_clone_cdf_lines, validate_config_binding_records
from .legacy_guardian import (
    acquire_legacy_guardian_launch_lock, close_legacy_guardian_launch_lock,
    legacy_guardian_parent_identity, write_legacy_guardian_manifest,
)
from .model_paths import MODEL_PATH_SCHEMA_VERSION, remap_workspace_setup_paths
from .cdf_callback_diagnostics import (
    callback_failure_lines, callback_value_proof_lines, selected_property_assignment_lines,
)
from .pdk import GENERIC_ADAPTER_ID, parse_cds_lib
from .resources import guardian_script_path, submit_spectre_script_path
from .technology import (
    TechnologyBindingError, technology_probe_skill, validate_workspace_technology,
    workspace_technology_manifest, workspace_technology_validation_required,
)
from .utils import atomic_write_json, atomic_write_text, cds_lib_token, ensure_dir, is_under, require_under, utc_timestamp
from .same_library_views import enabled as same_library_enabled, build_same_library_clone_plan, copy_lines as same_library_copy_lines


CADENCE_STARTUP_LOCK = threading.Lock()
MAX_CLONE_CELL_NAME = 32
ADAPTER_ERROR_PATTERN = re.compile(
    r"CRESCENDO_ADAPTER_ERROR:([a-z0-9_]+)", re.IGNORECASE,
)


class ParameterApplicationFailure(RuntimeError):
    def __init__(self, message, code=None, evidence=None):
        self.code = code
        self.evidence = evidence
        RuntimeError.__init__(self, message)


class ParameterApplicationInfrastructureFailure(ParameterApplicationFailure):
    pass


def adapter_failure_code(value):
    code = getattr(value, "code", None)
    if code:
        return str(code)
    if isinstance(value, dict):
        text = json.dumps(value, sort_keys=True)
    else:
        text = str(value or "")
    match = ADAPTER_ERROR_PATTERN.search(text)
    return match.group(1).lower() if match else None


class WorkspaceBuildFailure(RuntimeError):
    def __init__(self, cause, cleanup):
        self.cause = cause
        self.cleanup = cleanup
        message = "workspace build failed: %s; partial cleanup: %s" % (
            cause, json.dumps(cleanup, sort_keys=True),
        )
        RuntimeError.__init__(self, message)


IGNORE_PATTERNS = (
    "*.cdslck",
    "*.cdslck.*",
    "CDS.log*",
    ".tmpADEDir_*",
    "input.ahdlSimDB",
)

SOURCE_IDENTITY_IGNORE_PATTERNS = (
    "*~",
    "*.bak",
    "*.bak-*",
    "*.bak_*",
    "thumbnail_*.png",
)


def _setup_view_directory(directory, names=None):
    if os.path.basename(directory) == "maestro":
        return True
    names = set(names or ())
    if names.intersection(("maestro.sdb", "data.sdb")):
        return True
    return any(
        os.path.isfile(os.path.join(directory, name))
        for name in ("maestro.sdb", "data.sdb")
    )


def _nfs_unlinked_regular_file(directory, name):
    # Linux fs/nfs/unlink.c: .nfs + 16 hex fileid + 8 hex counter.
    # These are unlinked, still-open files, not named design dependencies.
    # Do not hide similarly named designs, directories, or symbolic links.
    if not re.match(r"^\.nfs[0-9a-f]{24}\Z", name):
        return False
    try:
        return stat.S_ISREG(os.lstat(os.path.join(directory, name)).st_mode)
    except OSError:
        return False


def _oa_cache_regular_file(directory, name):
    # Native OA cache: <existing OA file>.<host>.<pid>.oacache.
    # Require both files to be regular, without following symbolic links.
    match = re.match(r"^([A-Za-z0-9_-]+\.oa)\.([A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)*)\.([0-9]+)\.oacache\Z", name)
    if not match:
        return False
    try:
        return (stat.S_ISREG(os.lstat(os.path.join(directory, name)).st_mode) and
                stat.S_ISREG(os.lstat(os.path.join(directory, match.group(1))).st_mode))
    except OSError:
        return False


def copy_ignore(source_dir, names):
    ignored = []
    for name in names:
        if (_nfs_unlinked_regular_file(source_dir, name) or
                _oa_cache_regular_file(source_dir, name) or
                any(fnmatch.fnmatch(name, pattern) for pattern in IGNORE_PATTERNS)):
            ignored.append(name)
        elif _setup_view_directory(source_dir, names) and name == "results":
            ignored.append(name)
    return set(ignored)


def _source_identity_ignore(directory, name):
    if (_nfs_unlinked_regular_file(directory, name) or
            _oa_cache_regular_file(directory, name)):
        return True
    if any(fnmatch.fnmatch(name, pattern) for pattern in IGNORE_PATTERNS):
        return True
    if any(fnmatch.fnmatch(name, pattern) for pattern in SOURCE_IDENTITY_IGNORE_PATTERNS):
        return True
    return _setup_view_directory(directory) and name in (
        "active.state", "results", "test_states",
    )


def _content_fingerprint_scopes(relative_paths):
    normalized = []
    for raw_path in (relative_paths or ["."]):
        raw_path = str(raw_path or ".")
        if os.path.isabs(raw_path):
            raise ValueError("source identity scope must be relative: %s" % raw_path)
        path = os.path.normpath(raw_path)
        if path == os.pardir or path.startswith(os.pardir + os.sep):
            raise ValueError("source identity scope escapes library root: %s" % raw_path)
        normalized.append(path)
    compact = []
    for path in sorted(set(normalized), key=lambda value: (value.count(os.sep), value)):
        if any(
                parent == "." or path == parent or path.startswith(parent + os.sep)
                for parent in compact):
            continue
        compact.append(path)
    return sorted(compact)


def _audit_materializable_tree(source, ignore=None):
    source = os.path.abspath(source)
    materialized = []

    def visit(directory, ancestors):
        check_command_cancellation()
        real_directory = os.path.realpath(directory)
        if real_directory in ancestors:
            raise ValueError("source library contains a symbolic-link directory cycle: %s" % directory)
        try:
            names = sorted(os.listdir(directory))
        except OSError as error:
            raise ValueError("source library directory cannot be read: %s: %s" % (directory, error))
        ignored = (ignore or copy_ignore)(directory, names)
        next_ancestors = ancestors + (real_directory,)
        for name in names:
            check_command_cancellation()
            if name in ignored:
                continue
            path = os.path.join(directory, name)
            if os.path.islink(path):
                target = os.path.realpath(path)
                if not os.path.exists(target):
                    raise ValueError("source library contains a dangling symbolic link: %s" % path)
                if not (os.path.isdir(target) or os.path.isfile(target)):
                    raise ValueError("source library symbolic link has an unsupported target: %s" % path)
                materialized.append({
                    "path": os.path.relpath(path, source).replace(os.sep, "/"),
                    "kind": "directory" if os.path.isdir(target) else "file",
                    "target_inside_source": is_under(target, source),
                })
            if os.path.isdir(path):
                visit(path, next_ancestors)
            elif not os.path.isfile(path):
                raise ValueError("source library contains an unsupported filesystem entry: %s" % path)

    visit(source, ())
    return materialized


def _workspace_copy_cells(setup, hierarchy, sources, fingerprint_scopes):
    """Copy every known simulation dependency, not unrelated cells in a library.

    Unknown auxiliary directories remain conservative copies. Complete views of
    referenced cells are retained; selected parameter scopes do not narrow this
    dependency set.
    """
    cells = dict((name, set(paths)) for name, paths in fingerprint_scopes.items())
    for test in (setup or {}).get("tests") or []:
        if test.get("library") in cells and test.get("cell"):
            cells[test["library"]].add(test["cell"])
    for design in (hierarchy or {}).get("designs") or []:
        for instance in (design.get("hierarchy") or {}).get("instances") or []:
            library = instance.get("master_library") or instance.get("library")
            cell = instance.get("master_cell") or instance.get("cell")
            if library in cells and cell:
                cells[library].add(cell)
    return cells


def _library_copy_ignore(source, required_cells):
    source = os.path.abspath(source)
    required_cells = set(required_cells or ())
    whole_library = "." in required_cells
    known_views = frozenset((
        "schematic", "symbol", "layout", "config", "spectre", "veriloga",
        "verilog", "maestro", "adexl", "extracted", "av_extracted",
    ))

    def ignore(directory, names):
        ignored = copy_ignore(directory, names)
        if whole_library or os.path.abspath(directory) != source:
            return ignored
        for name in names:
            check_command_cancellation()
            if name in required_cells or name in ignored:
                continue
            # Library/OA/CDF/technology metadata is never treated as an
            # unrelated design cell, even if it contains familiar view names.
            lower = name.lower()
            if (name.startswith(".") or lower.startswith(("tech", "cdf", "oadm"))
                    or lower in ("data.dm", "prop.xx", "props", "tag", "oa")):
                continue
            path = os.path.join(directory, name)
            if not os.path.isdir(path):
                continue
            try:
                views = os.listdir(path)
            except OSError:
                continue  # An unknown directory is copied conservatively.
            is_cell = any(view in known_views and os.path.isdir(os.path.join(path, view))
                          for view in views)
            if not is_cell:
                is_cell = any(os.path.isfile(os.path.join(path, view, "master.tag"))
                              for view in views if os.path.isdir(os.path.join(path, view)))
            if is_cell:
                ignored.add(name)
        return ignored

    return ignore


def private_library_name(workspace, library):
    """Return a write target; source master references keep their original name."""
    return (workspace.get("private_library_names") or {}).get(library, library)


def private_design_identity(workspace, design):
    identity = tuple(design.get(name) for name in ("library", "cell", "view")) if isinstance(design, dict) else tuple(design)
    for row in workspace.get("private_design_identities") or []:
        if tuple(row["source"]) == identity:
            return tuple(row["target"])
    return identity


def private_library_is_owned(workspace, library):
    if workspace.get("same_library_objects"):
        return False  # Ownership is confined to exact registry cellviews.
    names = workspace.get("private_library_names") or {}
    return library in set(names.values()) if names else library in (workspace.get("copied_libraries") or {})


def workspace_reference_plan(setup, hierarchy, sources, primary_library, root, parameters, same_library=False):
    """Plan only views that will be written; other masters resolve in source libs.

    Nested mutable branches are created by the existing native occurrence-clone
    DAG. They do not need a preliminary filesystem copy of their source cells.
    """
    names = dict((library, "cr_w_" + hashlib.sha256(
        (os.path.realpath(root) + "\x1f" + library).encode("utf-8")).hexdigest()[:20])
        for library in sources)
    views = dict((library, {}) for library in sources)
    mappings = {}

    def add_view(identity):
        library, cell, view = identity
        if library not in sources or not cell or not view:
            raise ValueError("mutable view has no resolved user library: %r" % (identity,))
        if any(os.path.basename(str(part)) != str(part) or part in (".", "..") for part in (cell, view)):
            raise ValueError("mutable cellview identity is unsafe: %r" % (identity,))
        path = os.path.join(sources[library], cell, view)
        if not os.path.isdir(path):
            raise ValueError("required mutable source cellview is unavailable: %s/%s/%s" % identity)
        views[library].setdefault(cell, set()).add(view)
        mappings[identity] = (names[library], cell, view)

    relative = os.path.relpath(_setup_database_path(setup), sources[primary_library]).split(os.sep)
    if len(relative) < 3 or relative[0] == "..":
        raise ValueError("Maestro setup is not a cellview below its source library")
    add_view((primary_library, relative[0], relative[1]))
    configs = dict((tuple(row[name] for name in ("library", "cell", "view")), row)
                   for row in config_designs(hierarchy))
    selected_designs = set(_design_key(row) for row in parameters or []
                          if row.get("backend", "schematic") == "schematic")
    # Explicit netlist variables do not modify OA. A schematic parameter must
    # carry its real test identity, rather than silently materializing all tests.
    for design in sorted(selected_designs):
        if not all(design):
            raise ValueError("selected parameter lacks a complete source design identity")
        config = configs.get(design)
        if not same_library or config:
            add_view(design)
        if config and not same_library:
            add_view(tuple(config["resolved_top"][name] for name in ("library", "cell", "view")))
        if same_library:
            mappings[design] = (names[design[0]], design[1], design[2] if config else "cr_config")
    return {
        "schema_version": "crescendo.workspace_reference_plan.v1",
        "private_library_names": names,
        "copy_views": dict((lib, dict((cell, sorted(value)) for cell, value in cells.items()))
                           for lib, cells in views.items()),
        "private_design_identities": [{"source": list(key), "target": list(mappings[key])}
                                      for key in sorted(mappings)],
        "selected_designs": [list(key) for key in sorted(selected_designs)],
        "external_master_policy": "source_library_read_only",
    }


def reference_copy_ignore(source, cell_views):
    """Copy library/cell metadata and explicit views, never other design cells."""
    source = os.path.abspath(source)
    cell_views = cell_views or {}

    def ignore(directory, names):
        ignored = copy_ignore(directory, names)
        relative = os.path.relpath(directory, source)
        if relative == ".":
            for name in names:
                if name in ignored or not os.path.isdir(os.path.join(directory, name)):
                    continue
                # Only documented library administrative directories are
                # metadata. Unknown/user cell directories remain source refs.
                if name not in cell_views and name not in (".oa", ".cds", ".oalib", "oa", "props"):
                    ignored.add(name)
        elif os.sep not in relative and relative in cell_views:
            for name in names:
                if os.path.isdir(os.path.join(directory, name)) and name not in cell_views[relative]:
                    ignored.add(name)
        return ignored

    return ignore


def _reference_copy_scopes(source, cell_views):
    scopes = []
    for name in sorted(os.listdir(source)):
        path = os.path.join(source, name)
        if name in copy_ignore(source, [name]):
            continue
        if os.path.isfile(path) or name in (".oa", ".cds", ".oalib", "oa", "props"):
            scopes.append(name)
    for cell, views in sorted((cell_views or {}).items()):
        for name in sorted(os.listdir(os.path.join(source, cell))):
            path = os.path.join(source, cell, name)
            if name in copy_ignore(os.path.dirname(path), [name]):
                continue
            if os.path.isfile(path) or name in views:
                scopes.append(os.path.join(cell, name))
    return scopes or ["__cr_no_materialized_views__"]


def workspace_test_design_lines(workspace, read_only=False):
    """Redirect only modified test designs inside the owned Maestro session."""
    lines = []
    for test in (workspace.get("setup") or {}).get("tests") or []:
        if test.get("enabled", True) and (test.get("simulator") or "spectre").lower() == "alps":
            lines.extend([
                'crescAlpsTest = axlGetTest(mainSDB "%s")' % _skill_quote(test["name"]),
                'unless(crescAlpsTest error("Crescendo saved ALPS test unavailable\\n"))',
                'crescAlpsArgs = errset(axlGetTestToolArgs(crescAlpsTest) t)',
                'unless(crescAlpsArgs && equal(cadr(assoc("sim" car(crescAlpsArgs))) "alps") error("Crescendo native ALPS simulator integration/readback unavailable\\n"))',
            ])
        source = tuple(test.get(name) for name in ("library", "cell", "view"))
        target = private_design_identity(workspace, source)
        if target == source:
            continue
        binding_lines = [
            'crescPrivateTest = axlGetTest(mainSDB "%s")' % _skill_quote(test["name"]),
            'unless(crescPrivateTest error("Crescendo cannot find test for private design binding\\n"))',
            'crescPrivateArgs = axlGetTestToolArgs(crescPrivateTest)',
            'unless(crescPrivateArgs error("Crescendo cannot obtain source test design arguments\\n"))',
            'crescPrivateArgs = append(list(list("lib" "%s") list("cell" "%s") list("view" "%s")) setof(item crescPrivateArgs !member(car(item) list("lib" "cell" "view"))))' % tuple(_skill_quote(v) for v in target),
            'unless(axlSetTestToolArgs(crescPrivateTest crescPrivateArgs) error("Crescendo cannot bind private test design\\n"))',
            'crescPrivateReadback = axlGetTestToolArgs(crescPrivateTest)',
            'unless(equal(cadr(assoc("lib" crescPrivateReadback)) "%s") && equal(cadr(assoc("cell" crescPrivateReadback)) "%s") && equal(cadr(assoc("view" crescPrivateReadback)) "%s") error("Crescendo private test design readback mismatch\\n"))' % tuple(_skill_quote(v) for v in target),
        ]
        if read_only:
            binding_lines = [line for line in binding_lines if not (
                "axlSetTestToolArgs(" in line or "crescPrivateArgs = append(" in line)]
        lines.extend(binding_lines)
    return lines


def _copy_private_file(source, destination):
    """Materialize bytes with cooperative cancellation before copying metadata."""
    check_command_cancellation()
    with open(source, "rb") as reader, open(destination, "wb") as writer:
        while True:
            check_command_cancellation()
            block = reader.read(1024 * 1024)
            if not block:
                break
            writer.write(block)
    check_command_cancellation()
    shutil.copystat(source, destination, follow_symlinks=True)
    return destination


def _assert_private_tree_has_no_symlinks(root):
    for current, directories, files in os.walk(root, followlinks=False):
        check_command_cancellation()
        for name in directories + files:
            check_command_cancellation()
            path = os.path.join(current, name)
            if os.path.islink(path):
                raise ValueError("private workspace still contains a symbolic link: %s" % path)


def _make_private_tree_owner_writable(root):
    root = os.path.abspath(root)
    if os.path.islink(root):
        raise ValueError("private workspace tree is a symbolic link: %s" % root)
    for current, directories, files in os.walk(root, topdown=True, followlinks=False):
        check_command_cancellation()
        current_stat = os.lstat(current)
        if not stat.S_ISDIR(current_stat.st_mode):
            raise ValueError("private workspace tree contains a non-directory: %s" % current)
        os.chmod(current, stat.S_IMODE(current_stat.st_mode) | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
        for name in directories:
            check_command_cancellation()
            path = os.path.join(current, name)
            path_stat = os.lstat(path)
            if not stat.S_ISDIR(path_stat.st_mode) or stat.S_ISLNK(path_stat.st_mode):
                raise ValueError("private workspace tree contains an unsupported directory: %s" % path)
            os.chmod(path, stat.S_IMODE(path_stat.st_mode) | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
        for name in files:
            check_command_cancellation()
            path = os.path.join(current, name)
            path_stat = os.lstat(path)
            if not stat.S_ISREG(path_stat.st_mode) or stat.S_ISLNK(path_stat.st_mode):
                raise ValueError("private workspace tree contains an unsupported file: %s" % path)
            os.chmod(path, stat.S_IMODE(path_stat.st_mode) | stat.S_IRUSR | stat.S_IWUSR)


def tree_fingerprint(root, relative_paths=None):
    root = os.path.abspath(root)
    scopes = sorted(set(
        os.path.normpath(str(path or ".")).lstrip(os.sep)
        for path in (relative_paths or ["."])
    ))
    digest = hashlib.sha256()
    count = 0
    visited = set()
    for scope in scopes:
        scope_root = os.path.abspath(os.path.join(root, scope))
        if not is_under(scope_root, root):
            raise ValueError("fingerprint scope escapes library root: %s" % scope)
        digest.update(("SCOPE\x1f%s\n" % scope).encode("utf-8"))
        if not os.path.lexists(scope_root):
            digest.update(("MISSING\x1f%s\n" % scope).encode("utf-8"))
            continue
        for current, directories, files in os.walk(scope_root):
            directories[:] = sorted(name for name in directories if not name.endswith(".cdslck"))
            for name in sorted(files):
                if any(fnmatch.fnmatch(name, pattern) for pattern in ("*.cdslck", "*.cdslck.*")):
                    continue
                path = os.path.join(current, name)
                relative = os.path.relpath(path, root)
                if relative in visited:
                    continue
                visited.add(relative)
                stat_result = os.lstat(path)
                row = "%s\x1f%s\x1f%s\x1f%s\n" % (
                    relative, stat_result.st_size, int(stat_result.st_mtime * 1000000000), stat_result.st_mode
                )
                digest.update(row.encode("utf-8"))
                count += 1
    return {"root": root, "paths": scopes, "file_count": count, "fingerprint": digest.hexdigest()}


SOURCE_SCOPE_DIAGNOSTIC_LIMIT = 64
SOURCE_CHANGE_DIAGNOSTIC_LIMIT = 12
SOURCE_FILE_DIAGNOSTIC_LIMIT = 16
SOURCE_LIBRARY_DIAGNOSTIC_LIMIT = 8


def tree_content_fingerprint(root, relative_paths=None):
    """Return a path-independent digest of the materialized scoped source content."""
    check_command_cancellation()
    root = os.path.abspath(root)
    scopes = _content_fingerprint_scopes(relative_paths)
    digest = hashlib.sha256()
    files = set()
    directories = set()
    scope_rows = []
    scope_digest = None
    scope_files = []
    scope_file_count = [0]
    # Library-level bags and management files often sort after thousands of
    # cellviews. Keep them observable within the same fixed diagnostic budget.
    diagnostic_scopes = set(sorted(scopes, key=lambda path: (os.sep in path, path))
                            [:SOURCE_SCOPE_DIAGNOSTIC_LIMIT])

    def update(encoded):
        digest.update(encoded)
        if scope_digest is not None:
            scope_digest.update(encoded)

    def record_file(path, size, fingerprint):
        # Reuse the digest already read for the guard. This sidecar never
        # participates in source identity, and never causes an extra file read.
        if scope_digest is None:
            return
        scope_file_count[0] += 1
        if len(scope_files) < SOURCE_FILE_DIAGNOSTIC_LIMIT:
            scope_files.append({"path": path, "bytes": size, "sha256": fingerprint})

    def visit(directory, logical_directory, ancestors):
        check_command_cancellation()
        real_directory = os.path.realpath(directory)
        if real_directory in ancestors:
            raise ValueError("source identity contains a symbolic-link directory cycle: %s" % directory)
        try:
            names = sorted(os.listdir(directory))
        except OSError as error:
            raise ValueError("source identity directory cannot be read: %s: %s" % (directory, error))
        next_ancestors = ancestors + (real_directory,)
        for name in names:
            check_command_cancellation()
            if _source_identity_ignore(directory, name):
                continue
            path = os.path.join(directory, name)
            logical_path = os.path.normpath(os.path.join(logical_directory, name)).lstrip(os.sep)
            if os.path.isdir(path):
                if logical_path not in directories:
                    directories.add(logical_path)
                    update(("D\x1f%s\n" % logical_path).encode("utf-8"))
                visit(path, logical_path, next_ancestors)
                continue
            if not os.path.isfile(path):
                raise ValueError("source identity contains an unsupported filesystem entry: %s" % path)
            if logical_path in files:
                continue
            files.add(logical_path)
            content_digest = hashlib.sha256()
            with open(path, "rb") as stream:
                while True:
                    check_command_cancellation()
                    block = stream.read(1024 * 1024)
                    if not block:
                        break
                    content_digest.update(block)
            size, fingerprint = os.path.getsize(path), content_digest.hexdigest()
            update(("F\x1f%s\x1f%s\x1f%s\n" % (
                logical_path, size, fingerprint,
            )).encode("utf-8"))
            record_file(logical_path, size, fingerprint)

    for scope in scopes:
        check_command_cancellation()
        scope_root = os.path.abspath(os.path.join(root, scope))
        if not is_under(scope_root, root):
            raise ValueError("source identity scope escapes library root: %s" % scope)
        scope_digest = hashlib.sha256() if scope in diagnostic_scopes else None
        scope_files = []
        scope_file_count[0] = 0
        update(("SCOPE\x1f%s\n" % scope).encode("utf-8"))
        if not os.path.lexists(scope_root):
            kind = "missing"
            update(("MISSING\x1f%s\n" % scope).encode("utf-8"))
        elif os.path.isdir(scope_root):
            kind = "directory"
            visit(scope_root, scope, ())
        elif os.path.isfile(scope_root):
            kind = "file"
            logical_path = os.path.normpath(scope).lstrip(os.sep)
            content_digest = hashlib.sha256()
            with open(scope_root, "rb") as stream:
                while True:
                    check_command_cancellation()
                    block = stream.read(1024 * 1024)
                    if not block:
                        break
                    content_digest.update(block)
            files.add(logical_path)
            size, fingerprint = os.path.getsize(scope_root), content_digest.hexdigest()
            update(("F\x1f%s\x1f%s\x1f%s\n" % (
                logical_path, size, fingerprint,
            )).encode("utf-8"))
            record_file(logical_path, size, fingerprint)
        else:
            raise ValueError("source identity scope is unsupported: %s" % scope_root)
        if scope_digest is not None:
            scope_rows.append({"path": scope, "kind": kind, "fingerprint": scope_digest.hexdigest(),
                "files": scope_files, "file_count": scope_file_count[0],
                "omitted_files": max(0, scope_file_count[0] - len(scope_files))})
    check_command_cancellation()
    return {
        "schema_version": "crescendo.tree_content_fingerprint.v1",
        "paths": scopes,
        "file_count": len(files),
        "directory_count": len(directories),
        "fingerprint": digest.hexdigest(),
        # Bounded diagnostic sidecar; not part of any identity or equality
        # decision. It reuses the bytes already hashed above, without reading
        # files again. The per-scope file sample is bounded independently.
        "scope_diagnostics": {"scopes": scope_rows, "total_scopes": len(scopes),
                              "omitted_scopes": max(0, len(scopes) - len(scope_rows))},
    }


def parse_cds_defines(path, seen=None):
    del seen
    return parse_cds_lib(path).get("definitions") or {}


def _setup_database_path(setup):
    source = (setup or {}).get("source") or {}
    path = source.get("setup_db") or source.get("maestro_sdb") or source.get("data_sdb")
    if not path:
        raise ValueError("ADE setup source does not contain a SetupDB path")
    return os.path.abspath(path)


def _logical_primary_library(setup, primary_root, definitions):
    test_libraries = []
    for test in (setup or {}).get("tests") or []:
        library = test.get("library")
        if library and library not in test_libraries:
            test_libraries.append(library)
    mapped = sorted(
        name for name, path in (definitions or {}).items()
        if os.path.realpath(os.path.abspath(path)) == primary_root
    )
    preferred = [library for library in test_libraries if library in mapped]
    if len(preferred) == 1:
        return preferred[0]
    if len(mapped) == 1:
        return mapped[0]
    if len(test_libraries) == 1:
        return test_libraries[0]
    locator_name = ((setup or {}).get("setup_locator") or {}).get("library_name")
    return locator_name or os.path.basename(primary_root)


def _hierarchy_libraries(hierarchy):
    libraries = set()
    for design in (hierarchy or {}).get("designs", []):
        payload = design.get("hierarchy") or {}
        for cell in payload.get("cells", []):
            if cell.get("library"):
                libraries.add(cell["library"])
        for dependency in design.get("config_dependencies") or payload.get("config_dependencies") or []:
            if dependency.get("library"):
                libraries.add(dependency["library"])
        for binding in design.get("config_bindings") or payload.get("config_bindings") or []:
            for field in ("master_library", "bound_library", "resolved_library", "owner_library"):
                if binding.get(field):
                    libraries.add(binding[field])
    return libraries


def _source_fingerprint_scopes(setup, hierarchy, sources, actual_views=False):
    # A sealed setup is guarded through its owned snapshot. Electrical views
    # in the same TB cell remain live dependencies, so never omit the cell.
    if setup.get("baseline"):
        actual_views = True
    scopes = dict((library, set()) for library in sources)
    primary_root = os.path.abspath(setup["source"]["library_root"])
    primary_name = next(
        (name for name, path in sources.items() if os.path.realpath(path) == os.path.realpath(primary_root)),
        os.path.basename(primary_root),
    )
    setup_db = _setup_database_path(setup)
    if is_under(setup_db, primary_root):
        relative = os.path.relpath(setup_db, primary_root)
        scopes.setdefault(primary_name, set()).add(relative.split(os.sep, 1)[0])
    for design in (hierarchy or {}).get("designs", []):
        for cell in design.get("hierarchy", {}).get("cells", []):
            library = cell.get("library")
            name = cell.get("cell")
            if library in scopes and name:
                scopes[library].add(name)
        payload = design.get("hierarchy") or {}
        # External unchanged masters remain simulation dependencies even when
        # their opaque views were not traversed into editable parameter cells.
        for instance in payload.get("instances") or []:
            for prefix in ("master_", "resolved_", "bound_"):
                library, cell = instance.get(prefix + "library"), instance.get(prefix + "cell")
                if library in scopes and cell:
                    scopes[library].add(cell)
        for dependency in design.get("config_dependencies") or payload.get("config_dependencies") or []:
            if dependency.get("library") in scopes and dependency.get("cell"):
                scopes[dependency["library"]].add(dependency["cell"])
        for binding in design.get("config_bindings") or payload.get("config_bindings") or []:
            for prefix in ("master_", "bound_", "resolved_", "owner_"):
                library, cell = binding.get(prefix + "library"), binding.get(prefix + "cell")
                if library in scopes and cell:
                    scopes[library].add(cell)
    if not actual_views:
        return dict((library, sorted(paths) or ["."]) for library, paths in scopes.items())
    observed = dict((library, set()) for library in sources)
    def add(library, cell, view):
        if library in observed and cell and view:
            observed[library].add(os.path.join(cell, view))
    if is_under(setup_db, primary_root):
        relative = os.path.relpath(setup_db, primary_root).split(os.sep)
        if len(relative) >= 2:
            add(primary_name, relative[0], relative[1])
    for design in (hierarchy or {}).get("designs") or []:
        requested = design.get("design") or design
        if isinstance(requested, (list, tuple)):
            requested = dict(zip(("library", "cell", "view"), requested))
        add(requested.get("library"), requested.get("cell"), requested.get("view"))
        payload = design.get("hierarchy") or {}
        top = design.get("resolved_top") or payload.get("resolved_top") or {}
        add(top.get("library"), top.get("cell"), top.get("view"))
        for cell in payload.get("cells") or []:
            add(cell.get("library"), cell.get("cell"), cell.get("view") or "schematic")
        for row in (payload.get("instances") or []) + (design.get("config_bindings") or payload.get("config_bindings") or []):
            for prefix in ("master_", "resolved_", "bound_", "owner_"):
                add(row.get(prefix + "library"), row.get(prefix + "cell"), row.get(prefix + "view"))
        for row in design.get("config_dependencies") or payload.get("config_dependencies") or []:
            add(row.get("library"), row.get("cell"), row.get("view"))
    # Cell CDF lives in cell administrative files, outside view directories.
    # Keep exact files and actual view scopes; adding unrelated views cannot
    # alter these scopes and no schematic-name pattern is ignored.
    for library, cells in scopes.items():
        for cell in cells:
            folder = os.path.join(sources[library], cell)
            if os.path.isdir(folder):
                for name in sorted(os.listdir(folder)):
                    if os.path.isfile(os.path.join(folder, name)) and not _source_identity_ignore(folder, name):
                        observed[library].add(os.path.join(cell, name))
                # Native switch/stop implementations remain dependencies even
                # for opaque leaves whose compact record omitted the view.
                for view in ("schematic", "symbol", "spectre", "veriloga", "verilog", "config"):
                    if os.path.isdir(os.path.join(folder, view)):
                        observed[library].add(os.path.join(cell, view))
        for name in sorted(os.listdir(sources[library])):
            if os.path.isfile(os.path.join(sources[library], name)) and not _source_identity_ignore(sources[library], name):
                observed[library].add(name)
    if setup.get("baseline") and is_under(setup_db, primary_root):
        relative = os.path.relpath(setup_db, primary_root).split(os.sep)
        if len(relative) >= 2:
            observed[primary_name].discard(os.path.join(relative[0], relative[1]))
    return dict((library, sorted(paths) or ["__cr_no_observed_views__"]) for library, paths in observed.items())


def _configured_cadence_context(config):
    config = config or {}
    cadence = dict(config.get("cadence") or {})
    discovery = cadence.get("discovery") or {}
    for key, value in (discovery.get("effective") or {}).items():
        if cadence.get(key) in (None, "", []):
            cadence[key] = value
    prepared_pdk = ((config.get("eda_context") or {}).get("pdk") or {})
    return cadence, prepared_pdk


def _sha256_file(path):
    check_command_cancellation()
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        while True:
            check_command_cancellation()
            chunk = stream.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    check_command_cancellation()
    return digest.hexdigest()


def _configured_startup_extensions(setup, hierarchy, config):
    cadence, prepared_pdk = _configured_cadence_context(config)
    detected_pdk = detect_pdk_context(setup, hierarchy, cadence)
    paths = list(cadence.get("startup_extensions") or [])
    context_ids = set()
    for payload in (prepared_pdk, detected_pdk):
        for context in payload.get("contexts") or []:
            context_id = context.get("id") or context.get("profile_id")
            if context_id and context_id in context_ids:
                continue
            if context_id:
                context_ids.add(context_id)
            paths.extend(context.get("startup_extensions") or [])
    normalized = []
    seen = set()
    for raw_path in paths:
        path = os.path.realpath(os.path.abspath(os.path.expanduser(str(raw_path))))
        if path in seen:
            continue
        seen.add(path)
        if not os.path.isfile(path):
            raise ValueError("configured Cadence startup extension is not a readable file: %s" % path)
        normalized.append(path)
    return normalized


def _materialize_startup_extensions(setup, hierarchy, config, workspace_root):
    sources = _configured_startup_extensions(setup, hierarchy, config)
    if not sources:
        return []
    destination_root = ensure_dir(os.path.join(workspace_root, "startup_extensions"))
    records = []
    for index, source in enumerate(sources):
        digest = _sha256_file(source)
        basename = re.sub(r"[^A-Za-z0-9_.-]+", "_", os.path.basename(source)) or "startup.il"
        destination = os.path.join(
            destination_root, "%02d_%s_%s" % (index, digest[:12], basename),
        )
        shutil.copy2(source, destination)
        if _sha256_file(destination) != digest:
            raise ValueError("private startup extension copy failed content verification: %s" % source)
        records.append({
            "source": source,
            "private_copy": destination,
            "sha256": digest,
        })
    return records


def _reference_library_policy(setup, hierarchy, config):
    cadence, prepared_pdk = _configured_cadence_context(config)
    detected_pdk = detect_pdk_context(setup, hierarchy, cadence)
    roots = []
    context_rows = []
    for payload in (prepared_pdk, detected_pdk):
        rows = payload.get("contexts") or []
        if rows:
            context_rows.extend(rows)
        elif payload.get("pdk_dir") or payload.get("pdk_name"):
            context_rows.append({
                "package_root": payload.get("pdk_dir"),
                "reference_libraries": [payload.get("pdk_name")],
                "technology_libraries": [], "library_paths": {},
            })
    context_rows.extend(cadence.get("pdk_profiles") or [])
    for value in (
            cadence.get("cadence_root"), cadence.get("spectre_root"), cadence.get("pdk_dir"),
            prepared_pdk.get("pdk_dir"), detected_pdk.get("pdk_dir")):
        if value:
            roots.append(os.path.realpath(os.path.abspath(os.path.expanduser(str(value)))))
    for context in context_rows:
        for value in [context.get("package_root")] + list(
                (context.get("library_paths") or {}).values()
        ):
            if value:
                roots.append(os.path.realpath(os.path.abspath(os.path.expanduser(str(value)))))
    pdk_names = set()
    for value in (
            cadence.get("pdk_name"), prepared_pdk.get("pdk_name"),
            prepared_pdk.get("name"), detected_pdk.get("pdk_name")):
        if value:
            pdk_names.add(str(value))
    pdk_names.update(str(value) for value in prepared_pdk.get("candidate_libraries") or [] if value)
    pdk_names.update(str(value) for value in detected_pdk.get("candidate_libraries") or [] if value)
    explicit_pdk_names = set(
        str(value) for value in (
            cadence.get("pdk_name"), prepared_pdk.get("pdk_name"), prepared_pdk.get("name"),
        ) if value
    )
    for context in context_rows:
        names = list(context.get("reference_libraries") or [])
        names.extend(context.get("technology_libraries") or [])
        pdk_names.update(str(value) for value in names if value)
        explicit_pdk_names.update(str(value) for value in names if value)
    return {
        "cadence": cadence,
        "reference_roots": sorted(set(roots)),
        "pdk_names": pdk_names,
        "explicit_pdk_names": explicit_pdk_names,
    }


def resolve_copy_libraries(setup, hierarchy, config=None):
    primary_root = os.path.realpath(os.path.abspath(setup["source"]["library_root"]))
    source_cds = (config or {}).get("cadence", {}).get("source_cds_lib_override") or nearest_cds_lib(primary_root)
    policy = _reference_library_policy(setup, hierarchy, config)
    defines = parse_cds_defines(policy["cadence"].get("cds_lib"))
    defines.update(parse_cds_defines(source_cds))
    primary_name = _logical_primary_library(setup, primary_root, defines)
    defines[primary_name] = primary_root
    wanted = _hierarchy_libraries(hierarchy) or set([primary_name])
    wanted.add(primary_name)
    resolved = {}
    unresolved = []
    for library in sorted(wanted):
        path = defines.get(library)
        if not path or not os.path.isdir(path):
            if library != primary_name and library in policy["explicit_pdk_names"]:
                continue
            unresolved.append(library)
            continue
        path = os.path.realpath(os.path.abspath(path))
        if library != primary_name and (
                library in policy["explicit_pdk_names"]
                or any(is_under(path, root) for root in policy["reference_roots"])):
            continue
        resolved[library] = path
    if unresolved:
        raise ValueError(
            "required user hierarchy libraries cannot be resolved from cds.lib: %s"
            % ", ".join(sorted(unresolved))
        )
    resolved[primary_name] = primary_root
    return primary_name, source_cds, resolved


def _source_design_identity_from_sources(setup, hierarchy, sources, scopes=None, actual_views=False, captured_snapshots=None):
    if scopes is None:
        scopes = _source_fingerprint_scopes(setup, hierarchy, sources, actual_views=actual_views)
    libraries = []
    for library, source in sorted(sources.items()):
        snapshot = ((captured_snapshots or {}).get(library) or
                    tree_content_fingerprint(source, scopes.get(library)))
        if captured_snapshots is not None and snapshot.get("paths") != (sorted(scopes.get(library)) if scopes.get(library) is not None else None):
            raise ValueError("captured source snapshot scope mismatch")
        libraries.append({
            "library": library,
            "paths": snapshot["paths"],
            "file_count": snapshot["file_count"],
            "directory_count": snapshot["directory_count"],
            "fingerprint": snapshot["fingerprint"],
        })
    payload = {
        "schema_version": "crescendo.source_design_identity.v2" if actual_views else "crescendo.source_design_identity.v1",
        "libraries": libraries,
    }
    if setup.get("baseline"):
        from .setup_baseline import verify_setup_baseline
        baseline = verify_setup_baseline(setup)
        payload["schema_version"] = "crescendo.source_design_identity.v3"
        payload["setup_baseline"] = {"baseline_id": baseline["baseline_id"], "fingerprint": baseline["fingerprint"]}
    encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    payload["fingerprint"] = hashlib.sha256(encoded).hexdigest()
    return payload


def source_design_identity(setup, hierarchy, config=None):
    _primary_name, _source_cds, sources = resolve_copy_libraries(setup, hierarchy, config=config)
    return _source_design_identity_from_sources(setup, hierarchy, sources,
        actual_views=config is not None and ((config or {}).get("cadence") or {}).get("workspace_copy_strategy", "same_library_views") == "same_library_views")



def config_dependency_identity(setup, hierarchy, config=None):
    """Small path-independent config-only identity for staged inspection freshness."""
    dependencies = {}
    for design in (hierarchy or {}).get("designs") or []:
        payload = design.get("hierarchy") or {}
        for row in design.get("config_dependencies") or payload.get("config_dependencies") or []:
            key = tuple(row.get(field) for field in ("library", "cell", "view"))
            if not all(key):
                raise ValueError("config dependency identity is incomplete")
            dependencies[key] = row
    if not dependencies:
        return None
    primary, source_cds, sources = resolve_copy_libraries(setup, hierarchy, config=config)
    cadence, _pdk = _configured_cadence_context(config)
    definitions = parse_cds_defines(cadence.get("cds_lib"))
    definitions.update(parse_cds_defines(source_cds))
    definitions.update(sources)
    rows = []
    for key in sorted(dependencies):
        library, cell, view = key
        root = definitions.get(library)
        if not root or not os.path.isdir(os.path.join(root, cell, view)):
            raise ValueError("config dependency cannot be read: %s/%s/%s" % key)
        fingerprint = tree_content_fingerprint(root, [os.path.join(cell, view)])
        rows.append({"library": library, "cell": cell, "view": view,
                     "fingerprint": fingerprint["fingerprint"],
                     "file_count": fingerprint["file_count"]})
    identity = {"schema_version": "crescendo.config_dependency_identity.v1", "dependencies": rows}
    identity["fingerprint"] = hashlib.sha256(json.dumps(
        identity, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")).hexdigest()
    return identity


def _rewrite_text_file(path, replacements, simulation_roots):
    try:
        with open(path, "r") as stream:
            text = stream.read()
    except (UnicodeDecodeError, OSError):
        return False
    original = text
    for source, destination in replacements:
        text = text.replace(source, destination)
    for library, destination in simulation_roots.items():
        text = text.replace("$AXL_PROJECT_DIR/%s" % library, destination)
        pattern = re.compile(r"(?P<prefix>[\"'])/[^\"']*?/simulation/%s" % re.escape(library))
        text = pattern.sub(lambda match: match.group("prefix") + destination, text)
    if text != original:
        atomic_write_text(path, text)
        return True
    return False


def rewrite_copied_paths(workspace):
    replacements = []
    simulation_roots = {}
    for library, source in workspace["source_libraries"].items():
        destination = workspace["copied_libraries"][library]
        if workspace.get("copy_policy") == "source_reference":
            for cell, views in (workspace.get("copy_views") or {}).get(library, {}).items():
                for view in views:
                    replacements.append((os.path.join(source, cell, view), os.path.join(destination, cell, view)))
        else:
            replacements.append((source, destination))
        simulation_roots[library] = os.path.join(workspace["simulation_root"], library)
    changed = []
    for library_root in workspace["copied_libraries"].values():
        for current, _directories, files in os.walk(library_root):
            for name in files:
                path = os.path.join(current, name)
                if _rewrite_text_file(path, replacements, simulation_roots):
                    changed.append(path)
    return changed


def _cleanup_partial_workspace(root, run_root, sources, snapshots):
    root = os.path.abspath(root)
    run_root = os.path.abspath(run_root)
    result = {
        "ok": False,
        "removed": False,
        "workspace": root,
        "cleanup_error": None,
    }
    if not os.path.lexists(root):
        result.update({"ok": True, "cleanup_error": "workspace was not created"})
        return result
    private_roots = [
        os.path.join(run_root, "candidates"),
        os.path.join(run_root, "workers"),
        os.path.join(run_root, "range_validations"),
        os.path.join(run_root, "range_validation_resident"),
    ]
    if os.path.islink(root):
        result["cleanup_error"] = "partial workspace is a symbolic link"
        return result
    if not os.path.isdir(root):
        result["cleanup_error"] = "partial workspace is not a directory"
        return result
    if not any(
            is_under(root, private_root)
            and os.path.realpath(root) != os.path.realpath(private_root)
            for private_root in private_roots):
        result["cleanup_error"] = "partial workspace is not strictly under a private run directory"
        return result
    if set(sources) != set(snapshots):
        result["cleanup_error"] = "source snapshots are incomplete"
        return result
    for library, source in sorted(sources.items()):
        if is_under(root, source) or is_under(source, root):
            result["cleanup_error"] = "partial workspace overlaps source library %s" % library
            return result
        if not os.path.isdir(source):
            result["cleanup_error"] = "source library is unavailable during cleanup: %s" % library
            return result
        try:
            before = snapshots[library]
            after = tree_content_fingerprint(source, before.get("paths"))
        except Exception as error:
            result["cleanup_error"] = "source fingerprint revalidation failed for %s: %s" % (library, error)
            return result
        if not _content_snapshots_equal(before, after):
            result["cleanup_error"] = "source fingerprint changed during workspace build: %s" % library
            return result
    try:
        _make_private_tree_owner_writable(root)
        shutil.rmtree(root)
    except Exception as error:
        result["cleanup_error"] = "failed to remove partial workspace: %s" % error
        return result
    result.update({"ok": True, "removed": True, "cleanup_error": None})
    return result


def _content_snapshots_equal(before, after):
    return all(before.get(key) == after.get(key) for key in (
        "schema_version", "paths", "file_count", "directory_count", "fingerprint",
    ))


def _workspace_run_root(config, root):
    configured = ((config or {}).get("runtime") or {}).get("run_root")
    if configured:
        return os.path.abspath(configured)
    current = os.path.abspath(root)
    while os.path.dirname(current) != current:
        if os.path.basename(current) in ("candidates", "workers"):
            return os.path.dirname(current)
        current = os.path.dirname(current)
    raise ValueError("runtime.run_root is required when workspace is not under candidates/ or workers/")


def _build_same_library_workspace(setup, hierarchy, config, root, primary_name,
                                  source_cds, sources, snapshots, source_identity,
                                  parameters, build_started, fingerprint_seconds):
    from .same_library_views import config_designs_for_same_library, register_view, copy_setup_view
    simulation_root = ensure_dir(os.path.join(root, "simulation"))
    workspace = {"schema_version": "crescendo.workspace.v1", "created_at": utc_timestamp(),
                 "root": root, "primary_library": primary_name, "source_primary_library": primary_name,
                 "copy_policy": "same_library_views", "same_library_objects": True,
                 "run_display_name": (config.get("runtime") or {}).get("_run_display_name") or "optimization",
                 "same_library_token": uuid.uuid4().hex, "same_library_registry": [],
                 "source_cds_lib": source_cds, "source_libraries": sources,
                 "copied_libraries": dict(sources), "private_library_names": dict((name, name) for name in sources),
                 "reference_libraries": dict(sources), "private_design_identities": [],
                 "copy_views": dict((name, {}) for name in sources), "copy_dependency_cells": {},
                 "source_snapshots": snapshots, "source_identity": source_identity,
                 "copied_source_identity": source_identity, "materialized_source_identity": source_identity,
                 "materialized_identity_scope": "source_references_and_exact_owned_cellviews",
                 "materialized_symlinks": {}, "simulation_root": simulation_root, "setup": setup,
                 "config_designs": config_designs_for_same_library(hierarchy, parameters)}
    suite_owner = (config.get("runtime") or {}).get("_setup_private_owner")
    if suite_owner:
        workspace["setup_private_session"] = dict(suite_owner)
    source_setup = _setup_database_path(setup)
    relative = os.path.relpath(source_setup, sources[primary_name]).split(os.sep)
    if len(relative) < 3 or relative[0] == "..":
        raise ValueError("Maestro setup is not below its source library")
    source_setup_identity = (primary_name, relative[0], relative[1])
    started = time.monotonic()
    record = copy_setup_view(workspace, source_setup_identity, ("setup",) + source_setup_identity)
    workspace.update({"setup_dir": record["path"], "setup_sdb": os.path.join(record["path"], *relative[2:]),
                      "setup_cell": record["target"][1], "setup_view": record["target"][2],
                      "setup_backend": setup.get("backend") or "maestro",
                      "setup_owned_view": record})
    for design in workspace["config_designs"]:
        source = tuple(design[name] for name in ("library", "cell", "view"))
        owned = register_view(workspace, source, ("design",) + source, role="config")
        workspace["private_design_identities"].append({"source": list(source), "target": owned["target"]})
        # The complete observed HDB graph supplies every resolved occurrence;
        # write a new config without modifying or backing up the source view.
        design["generated_config"] = True
    workspace["selected_designs"] = [row["source"] for row in workspace["private_design_identities"]]
    workspace["private_design_identities"].append({"source": list(source_setup_identity), "target": record["target"]})
    lines = []
    installation_cds = (config.get("cadence") or {}).get("cds_lib")
    if installation_cds:
        lines.append("SOFTINCLUDE %s" % cds_lib_token(installation_cds))
    if source_cds:
        lines.append("INCLUDE %s" % cds_lib_token(source_cds))
    definitions = parse_cds_defines(source_cds)
    for library, path in sorted(sources.items()):
        if os.path.realpath(definitions.get(library) or "") != os.path.realpath(path):
            if library in definitions:
                lines.append("UNDEFINE %s" % cds_lib_token(library))
            lines.append("DEFINE %s %s" % (cds_lib_token(library), cds_lib_token(path)))
    atomic_write_text(os.path.join(root, "cds.lib"), "\n".join(lines) + "\n")
    extensions = _materialize_startup_extensions(setup, hierarchy, config, root)
    workspace["startup_extensions"] = extensions
    source_init = os.path.join(os.path.dirname(source_cds), ".cdsinit") if source_cds else None
    workspace["source_cdsinit"] = {"path": source_init, "present": bool(source_init and os.path.isfile(source_init)),
                                   "loaded": False, "policy": "ignored_unless_explicitly_configured"}
    technology_path = os.path.join(root, "technology_bindings.records")
    workspace["technology"] = workspace_technology_manifest(hierarchy, sources, technology_path)
    init_lines = ['; Crescendo uses exact owned views in original libraries; source views remain read only.']
    init_lines.extend('load("%s")' % _skill_quote(row["private_copy"]) for row in extensions)
    init_lines.extend(technology_probe_skill(workspace["technology"]))
    from .storage_policy import native_workspace_storage, native_storage_init_lines, native_storage_capture_definitions
    workspace["native_storage"] = native_workspace_storage(root, setup.get("save_options"))
    init_lines.extend(native_storage_capture_definitions().splitlines())
    init_lines.extend(native_storage_init_lines(workspace["native_storage"]))
    init_lines.append("crescWorkspaceStartupLoaded = t")
    atomic_write_text(os.path.join(root, ".cdsinit"), "\n".join(init_lines) + "\n")
    workspace["rewritten_files"] = []
    for current, _directories, names in os.walk(workspace["setup_dir"]):
        for name in names:
            path = os.path.join(current, name)
            if _rewrite_text_file(path, [], dict((name, os.path.join(simulation_root, name)) for name in sources)):
                workspace["rewritten_files"].append(path)
    if os.path.isfile(workspace["setup_sdb"]) and setup.get("schema_version") == "crescendo.ade_setup.v1":
        workspace["model_paths"] = remap_workspace_setup_paths(workspace, config,
            pdk_context=detect_pdk_context(setup, hierarchy, config.get("cadence") or {}))
        workspace["rewritten_files"] = sorted(set(workspace["rewritten_files"] + workspace["model_paths"]["changed_files"]))
    else:
        workspace["model_paths"] = {"schema_version": MODEL_PATH_SCHEMA_VERSION, "status": "setup_db_unavailable_or_uninspected",
                                    "references": [], "changed_files": [], "limitations": []}
    workspace["build_timing"] = {"elapsed_seconds": time.monotonic() - build_started,
                                 "source_fingerprint_seconds": fingerprint_seconds,
                                 "copy_seconds": time.monotonic() - started,
                                 "copy_identity_seconds": 0.0, "library_count": 0,
                                 "copied_cellview_count": 1}
    guard = verify_sources_unchanged(workspace)
    if not guard["ok"]:
        raise ValueError("source libraries changed while preparing owned Maestro view")
    atomic_write_json(os.path.join(root, "workspace.json"), workspace)
    return workspace


def build_workspace(setup, hierarchy, config=None, root=None, expected_source_identity=None, parameters=None):
    build_started = time.monotonic()
    if root is None and isinstance(config, (str, bytes)):
        root, config = config, None
    if root is None:
        raise ValueError("workspace root is required")
    config = config or {}
    root = os.path.abspath(root)
    run_root = _workspace_run_root(config, root)
    require_under(root, run_root, "workspace")
    if os.path.lexists(root):
        raise ValueError("workspace already exists: %s" % root)
    fingerprint_started = time.monotonic()
    sources = {}
    snapshots = {}
    try:
        primary_name, source_cds, sources = resolve_copy_libraries(setup, hierarchy, config=config)
        same_library = same_library_enabled(config, parameters)
        fingerprint_scopes = _source_fingerprint_scopes(setup, hierarchy, sources, actual_views=same_library)
        snapshots = dict(
            (library, tree_content_fingerprint(source, fingerprint_scopes.get(library)))
            for library, source in sorted(sources.items())
        )
        ensure_dir(root)
        source_identity = _source_design_identity_from_sources(setup, hierarchy, sources, scopes=fingerprint_scopes, actual_views=same_library, captured_snapshots=snapshots)
        if expected_source_identity is not None and (
                expected_source_identity.get("schema_version") != source_identity.get("schema_version")
                or expected_source_identity.get("fingerprint") != source_identity.get("fingerprint")):
            raise ValueError(
                "source design changed after Maestro preparation; reload the setup before running"
            )
        fingerprint_seconds = time.monotonic() - fingerprint_started
        if same_library:
            return _build_same_library_workspace(setup, hierarchy, config, root, primary_name,
                source_cds, sources, snapshots, source_identity, parameters, build_started, fingerprint_seconds)
        copies = {}
        materialized_symlinks = {}
        copy_cells = _workspace_copy_cells(setup, hierarchy, sources, fingerprint_scopes)
        reference_plan = workspace_reference_plan(setup, hierarchy, sources, primary_name, root, parameters, same_library=same_library) if parameters is not None else None
        copy_scopes = dict((library, _reference_copy_scopes(source, reference_plan["copy_views"].get(library)))
                           for library, source in sources.items()) if reference_plan else fingerprint_scopes
        if setup.get("baseline"):
            setup_relative = os.path.dirname(os.path.relpath(_setup_database_path(setup), sources[primary_name]))
            copy_scopes = dict((library, [path for path in paths if not (
                library == primary_name and (path == setup_relative or path.startswith(setup_relative + os.sep)))])
                for library, paths in copy_scopes.items())
        materialized_source_identity = (_source_design_identity_from_sources(setup, hierarchy, sources, scopes=copy_scopes)
                                        if reference_plan else source_identity)
        libraries_root = ensure_dir(os.path.join(root, "libraries"))
        copy_started = time.monotonic()
        for library, source in sorted(sources.items()):
            destination = os.path.join(libraries_root, library)
            ignore = (reference_copy_ignore(source, reference_plan["copy_views"].get(library))
                      if reference_plan else _library_copy_ignore(source, copy_cells.get(library)))
            if setup.get("baseline"):
                base_ignore = ignore
                original_setup_dir = os.path.dirname(_setup_database_path(setup))
                def ignore(directory, names, base_ignore=base_ignore):
                    return set(base_ignore(directory, names)) | set(name for name in names
                        if os.path.join(directory, name) == original_setup_dir)
            materialized_symlinks[library] = _audit_materializable_tree(source, ignore=ignore)
            shutil.copytree(source, destination, symlinks=False, ignore=ignore,
                            copy_function=_copy_private_file)
            _assert_private_tree_has_no_symlinks(destination)
            _make_private_tree_owner_writable(destination)
            copies[library] = destination
        copy_seconds = time.monotonic() - copy_started
        copy_identity_started = time.monotonic()
        copied_source_identity = _source_design_identity_from_sources(
            setup, hierarchy, copies, scopes=copy_scopes,
        )
        if copied_source_identity.get("fingerprint") != materialized_source_identity.get("fingerprint"):
            raise ValueError("private workspace source copy identity does not match its source")
        post_copy_snapshots = dict(
            (library, tree_content_fingerprint(source, fingerprint_scopes.get(library)))
            for library, source in sorted(sources.items())
        )
        changed_sources = [
            library for library in sorted(sources)
            if not _content_snapshots_equal(snapshots[library], post_copy_snapshots[library])
        ]
        if changed_sources:
            raise ValueError(
                "source libraries changed during private workspace copy: %s"
                % ", ".join(changed_sources)
            )
        copy_identity_seconds = time.monotonic() - copy_identity_started
        simulation_root = ensure_dir(os.path.join(root, "simulation"))
        workspace = {
            "schema_version": "crescendo.workspace.v1",
            "created_at": utc_timestamp(),
            "root": root,
            "primary_library": reference_plan["private_library_names"][primary_name] if reference_plan else primary_name,
            "source_primary_library": primary_name,
            "copy_policy": "same_library_views" if same_library else "source_reference" if reference_plan else "dependency_cells",
            "source_cds_lib": source_cds,
            "source_libraries": sources,
            "copied_libraries": copies,
            "materialized_symlinks": materialized_symlinks,
            "copy_dependency_cells": (dict((name, sorted(cells)) for name, cells in reference_plan["copy_views"].items())
                                      if reference_plan else dict((name, sorted(cells)) for name, cells in copy_cells.items())),
            "source_dependency_cells": dict((name, sorted(cells)) for name, cells in copy_cells.items()),
            "source_snapshots": snapshots,
            "source_identity": source_identity,
            "copied_source_identity": copied_source_identity,
            "materialized_source_identity": materialized_source_identity,
            "materialized_identity_scope": "private_copy_views" if reference_plan else "full_source_dependency_cells",
            "simulation_root": simulation_root,
            "setup": setup,
        }
        if reference_plan:
            workspace.update(dict((key, value) for key, value in reference_plan.items() if key != "schema_version"))
            workspace["reference_plan_schema_version"] = reference_plan["schema_version"]
            workspace["reference_libraries"] = dict(sources)
        native_configs = config_designs(hierarchy)
        if reference_plan:
            selected = set(tuple(key) for key in reference_plan["selected_designs"])
            native_configs = [row for row in native_configs if tuple(row[field] for field in ("library", "cell", "view")) in selected]
        if native_configs:
            workspace["config_designs"] = native_configs
        if same_library:
            from .same_library_views import config_designs_for_same_library
            workspace["config_designs"] = config_designs_for_same_library(hierarchy, parameters)
            workspace["same_library_token"] = uuid.uuid4().hex
        detected_pdk = detect_pdk_context(setup, hierarchy, (config.get("cadence") or {}))
        startup_extensions = _materialize_startup_extensions(
            setup, hierarchy, config, root,
        )
        workspace["startup_extensions"] = startup_extensions
        lines = []
        installation_cds_lib = (config.get("cadence") or {}).get("cds_lib")
        if installation_cds_lib:
            lines.append("SOFTINCLUDE %s" % cds_lib_token(installation_cds_lib))
        if source_cds:
            lines.append("INCLUDE %s" % cds_lib_token(source_cds))
        source_definitions = parse_cds_defines(source_cds)
        for library, path in sorted(copies.items()):
            private_name = private_library_name(workspace, library)
            if not reference_plan:
                lines.append("UNDEFINE %s" % cds_lib_token(library))
            else:
                # Explicit source mappings also cover a setup library absent
                # from source cds.lib; neither source name is shadowed.
                existing = source_definitions.get(library)
                if not existing or os.path.realpath(existing) != os.path.realpath(sources[library]):
                    if existing:
                        lines.append("UNDEFINE %s" % cds_lib_token(library))
                    lines.append("DEFINE %s %s" % (cds_lib_token(library), cds_lib_token(sources[library])))
            lines.append("DEFINE %s %s" % (cds_lib_token(private_name), cds_lib_token(path)))
        atomic_write_text(os.path.join(root, "cds.lib"), "\n".join(lines) + "\n")
        source_init = os.path.join(os.path.dirname(source_cds), ".cdsinit") if source_cds else None
        workspace["source_cdsinit"] = {
            "path": source_init,
            "present": bool(source_init and os.path.isfile(source_init)),
            "loaded": False,
            "policy": "ignored_unless_explicitly_configured",
        }
        init_lines = [
            '; Crescendo isolated workspace. All design writes must remain below this directory.',
            '; Source-adjacent .cdsinit is intentionally not loaded.',
        ]
        for extension in startup_extensions:
            private_copy = extension["private_copy"]
            init_lines.append('load("%s")' % private_copy.replace("\\", "\\\\").replace('"', '\\"'))
        technology_path = os.path.join(root, "technology_bindings.records")
        workspace["technology"] = workspace_technology_manifest(
            hierarchy, copies, technology_path,
        )
        if reference_plan:
            for binding in workspace["technology"]["bindings"]:
                binding["source_library"] = binding["library"]
                binding["library"] = private_library_name(workspace, binding["library"])
        init_lines.extend(technology_probe_skill(workspace["technology"]))
        from .storage_policy import native_workspace_storage, native_storage_init_lines, native_storage_capture_definitions
        workspace["native_storage"] = native_workspace_storage(root, setup.get("save_options"))
        init_lines.extend(native_storage_capture_definitions().splitlines())
        init_lines.extend(native_storage_init_lines(workspace["native_storage"]))
        init_lines.append("crescWorkspaceStartupLoaded = t")
        atomic_write_text(os.path.join(root, ".cdsinit"), "\n".join(init_lines) + "\n")
        rewrite_started = time.monotonic()
        source_setup = _setup_database_path(setup)
        relative_setup = os.path.relpath(source_setup, setup["source"]["library_root"])
        workspace["setup_sdb"] = os.path.join(copies[primary_name], relative_setup)
        workspace["setup_dir"] = os.path.dirname(workspace["setup_sdb"])
        if setup.get("baseline"):
            from .setup_baseline import effective_setup_dir
            if not is_under(workspace["setup_dir"], root) or workspace["setup_dir"] == root:
                raise ValueError("private baseline setup copy escaped its owned workspace")
            if os.path.isdir(workspace["setup_dir"]):
                shutil.rmtree(workspace["setup_dir"])
            shutil.copytree(effective_setup_dir(setup), workspace["setup_dir"], copy_function=_copy_private_file)
            _make_private_tree_owner_writable(workspace["setup_dir"])
        workspace["rewritten_files"] = rewrite_copied_paths(workspace)
        workspace["setup_cell"] = os.path.basename(os.path.dirname(workspace["setup_dir"]))
        workspace["setup_backend"] = setup.get("backend") or "maestro"
        workspace["setup_view"] = (
            (setup.get("setup_locator") or {}).get("view_name")
            or os.path.basename(workspace["setup_dir"])
        )
        if (
                os.path.isfile(workspace["setup_sdb"])
                and setup.get("schema_version") == "crescendo.ade_setup.v1"):
            workspace["model_paths"] = remap_workspace_setup_paths(
                workspace, config, pdk_context=detected_pdk,
            )
            workspace["rewritten_files"] = sorted(set(
                workspace["rewritten_files"] + workspace["model_paths"]["changed_files"]
            ))
        else:
            workspace["model_paths"] = {
                "schema_version": MODEL_PATH_SCHEMA_VERSION,
                "status": "setup_db_unavailable_or_uninspected",
                "references": [],
                "changed_files": [],
                "limitations": [{
                    "reason": "private_setup_db_unavailable_or_uninspected",
                    "file": workspace["setup_sdb"],
                }],
            }
        rewrite_seconds = time.monotonic() - rewrite_started
        workspace["build_timing"] = {
            "elapsed_seconds": time.monotonic() - build_started,
            "source_fingerprint_seconds": fingerprint_seconds,
            "copy_seconds": copy_seconds,
            "copy_identity_seconds": copy_identity_seconds,
            "rewrite_seconds": rewrite_seconds,
            "library_count": len(copies),
        }
        atomic_write_json(os.path.join(root, "workspace.json"), workspace)
        return workspace
    except Exception as error:
        # A pre-registered staging directory can outlive interrupted copying.
        # Keep its owner/intent evidence rather than erase the workspace around it.
        if (config.get("runtime") or {}).get("_setup_private_owner") and os.path.isdir(root):
            for name in os.listdir(root):
                if not (name.startswith("suite_create_") and name.endswith(".json")):
                    continue
                try:
                    intent = load_json(os.path.join(root, name))
                    stage = intent.get("stage")
                    if not isinstance(stage, str) or os.path.lexists(stage):
                        raise WorkspaceBuildFailure(error, {"ok": False, "removed": False,
                            "cleanup_error": "owned creation staging evidence retained: %s" % stage})
                except WorkspaceBuildFailure:
                    raise
                except Exception as intent_error:
                    raise WorkspaceBuildFailure(error, {"ok": False, "removed": False,
                        "cleanup_error": "creation staging identity unavailable: %s" % intent_error})
        if os.path.isfile(os.path.join(root, "workspace.json")):
            try:
                metadata = load_json(os.path.join(root, "workspace.json"))
                if metadata.get("same_library_objects"):
                    from .same_library_views import cleanup_same_library_views
                    owned_cleanup = cleanup_same_library_views(metadata, process_cleanup_proven=True)
                    if not owned_cleanup.get("cleanup_proven"):
                        raise WorkspaceBuildFailure(error, {"ok": False, "removed": False, "owned_view_cleanup": owned_cleanup})
            except WorkspaceBuildFailure:
                raise
            except Exception as cleanup_error:
                raise WorkspaceBuildFailure(error, {"ok": False, "removed": False,
                    "cleanup_error": "owned view cleanup proof unavailable: %s: %s" % (
                        cleanup_error.__class__.__name__, cleanup_error)})
        cleanup = _cleanup_partial_workspace(root, run_root, sources, snapshots)
        raise WorkspaceBuildFailure(error, cleanup)


def _source_scope_category(library, path, kind, workspace):
    parts = path.split(os.sep)
    if path in (".", "__cr_no_observed_views__"):
        return "library_content", "库级内容（缺少更细的历史快照）"
    if len(parts) == 1:
        if path in ("data.dm", "prop.xx", "dd.db", "cdsinfo.tag", ".oalib", "tech.db", "techfile.cds"):
            return "library_metadata", "库级属性/管理文件（可能含技术绑定）"
        return "library_content", "库级依赖内容（类型未记录）"
    if len(parts) == 2 and parts[1] in ("data.dm", "prop.xx", "dd.db", "props"):
        return "cell_metadata", "Cell 属性/CDF"
    if len(parts) == 2 and kind == "file":
        return "cell_content", "Cell 级依赖文件（类型未记录）"
    identity = (library, parts[0], parts[1])
    for design in workspace.get("config_designs") or []:
        if not isinstance(design, dict):
            continue
        source = tuple(design.get(key) for key in ("library", "cell", "view"))
        top = design.get("resolved_top") or {}
        resolved_top = tuple(top.get(key) for key in ("library", "cell", "view"))
        if identity == source and source != resolved_top:
            return "config_view", "Config 绑定视图"
        for row in design.get("config_bindings") or []:
            if not isinstance(row, dict):
                continue
            resolved = tuple(row.get("resolved_" + key) for key in ("library", "cell", "view"))
            if identity == resolved:
                if row.get("scan_traversable") is True:
                    return "schematic_view", "Schematic 电路视图"
                if row.get("stop") is True:
                    return "opaque_view", "Verilog-A/模型/后抽等终止视图"
    if parts[1] == "config":
        return "config_view", "Config 绑定视图"
    if parts[1] == "schematic":
        return "schematic_view", "Schematic 电路视图"
    if parts[1] in ("symbol", "spectre", "veriloga", "verilog"):
        return "opaque_view", "Verilog-A/模型/终止视图"
    return "electrical_view", "电路依赖视图（类型未记录）"


def _scope_diagnostic_rows(snapshot):
    evidence = snapshot.get("scope_diagnostics") or {}
    if not isinstance(evidence, dict):
        return None
    rows = evidence.get("scopes")
    if not isinstance(rows, list) or len(rows) > SOURCE_SCOPE_DIAGNOSTIC_LIMIT:
        return None
    paths = set(snapshot.get("paths") or [])
    result = {}
    for row in rows:
        if (not isinstance(row, dict) or not isinstance(row.get("path"), str) or row.get("path") not in paths
                or row.get("kind") not in ("file", "directory", "missing")
                or not isinstance(row.get("fingerprint"), str)
                or not re.match(r"^[a-f0-9]{64}$", row["fingerprint"])
                or row["path"] in result):
            return None
        result[row["path"]] = row
    return result


def _scope_file_changes(before, after):
    def catalogue(scope):
        rows = scope.get("files")
        count = scope.get("file_count")
        if (not isinstance(rows, list) or len(rows) > SOURCE_FILE_DIAGNOSTIC_LIMIT
                or not isinstance(count, int) or count < len(rows)):
            return None
        result = {}
        for row in rows:
            if (not isinstance(row, dict) or not isinstance(row.get("path"), str)
                    or not isinstance(row.get("bytes"), int) or row["bytes"] < 0
                    or not isinstance(row.get("sha256"), str)
                    or not re.match(r"^[a-f0-9]{64}$", row["sha256"])
                    or row["path"] in result):
                return None
            result[row["path"]] = row
        return result

    previous, current = catalogue(before), catalogue(after)
    if previous is None or current is None:
        return {"state": "file_details_unavailable", "files": [],
                "message": "历史范围没有可比较的文件摘要；不能推断具体文件或属性。"}
    changes = []
    for path in sorted(set(previous) | set(current)):
        old, new = previous.get(path), current.get(path)
        if old is not None and new is not None:
            if (old["bytes"], old["sha256"]) == (new["bytes"], new["sha256"]):
                continue
            state = "modified"
        elif old is None and before["file_count"] == len(previous):
            state = "added"
        elif new is None and after["file_count"] == len(current):
            state = "removed"
        else:
            # Absence in a bounded sample is not proof of addition/removal.
            continue
        changes.append({"path": path, "state": state,
            "before": {key: old[key] for key in ("bytes", "sha256")} if old else None,
            "after": {key: new[key] for key in ("bytes", "sha256")} if new else None})
    return {"state": "file_changes" if changes else "file_details_unavailable",
            "files": changes[:SOURCE_FILE_DIAGNOSTIC_LIMIT],
            "omitted_changed_files": max(0, len(changes) - SOURCE_FILE_DIAGNOSTIC_LIMIT),
            "unexamined_before_files": before["file_count"] - len(previous),
            "unexamined_after_files": after["file_count"] - len(current),
            "message": "文件内容/存在性摘要；不推断电路属性、网表语义或修改者。"}


def source_scope_changes(before, after, library, workspace=None):
    """Describe bounded changed scopes; never changes the source guard verdict."""
    if _content_snapshots_equal(before, after):
        return {"state": "unchanged", "scopes": [], "omitted_changed_scopes": 0,
                "unexamined_scopes": 0}
    previous, current = _scope_diagnostic_rows(before), _scope_diagnostic_rows(after)
    if previous is None or current is None:
        return {"state": "scope_details_unavailable", "scopes": [],
                "message": "历史快照未记录可比较的范围摘要；只能确认该库的依赖内容变化，不能断言是 schematic。"}
    comparable = set(previous) & set(current)
    changed = sorted(path for path in comparable
                     if previous[path]["fingerprint"] != current[path]["fingerprint"])
    rows = []
    for path in changed[:SOURCE_CHANGE_DIAGNOSTIC_LIMIT]:
        kind = current[path]["kind"] if current[path]["kind"] != "missing" else previous[path]["kind"]
        category, label = _source_scope_category(library, path, kind, workspace or {})
        rows.append({"path": path, "category": category, "label": label,
            "before": {key: previous[path].get(key) for key in ("kind", "fingerprint", "file_count")},
            "after": {key: current[path].get(key) for key in ("kind", "fingerprint", "file_count")},
            "file_changes": _scope_file_changes(previous[path], current[path])})
    all_paths = set(before.get("paths") or []) | set(after.get("paths") or [])
    unexamined = len(all_paths - comparable)
    return {"state": "changed_scopes" if rows else "scope_details_unavailable", "scopes": rows,
            "omitted_changed_scopes": max(0, len(changed) - len(rows)),
            "unexamined_scopes": unexamined,
            "message": ("范围摘要只定位到该目录/文件；未推断具体属性或网表语义。" if rows else
                        "变化位于未保留摘要的范围，或历史作用域发生变化；不能进一步定位。")}


def source_change_reason(library, changes):
    scopes = changes.get("scopes") or []
    if not scopes:
        return "source dependencies changed in library %s (exact changed scope unavailable)" % library
    details = ", ".join("%s [%s]" % (
        ("%s/%s" % (library, row["path"]))[:240], row["label"]) for row in scopes[:3])
    suffix = ""
    if len(scopes) > 3 or changes.get("omitted_changed_scopes") or changes.get("unexamined_scopes"):
        suffix = " (additional or unexamined scopes recorded in source_changes)"
    return "source dependencies changed: %s%s" % (details, suffix)


def source_change_summary(proof):
    """Bounded user-facing explanation for an existing source-guard proof."""
    changed = [row for row in proof.get("libraries") or [] if row.get("unchanged") is False]
    reasons = [source_change_reason(row.get("library") or "?", row.get("changes") or {})
               for row in changed[:4]]
    if len(changed) > 4:
        reasons.append("additional changed libraries recorded in source guard evidence")
    baseline = proof.get("setup_baseline") or {}
    if baseline.get("ok") is False:
        reasons.append("frozen Maestro baseline changed: %s" % str(baseline.get("error") or "unavailable")[:240])
    return "; ".join(reasons)[:2200] or "source dependency identity could not be verified (changed scope unavailable)"


def compact_source_guard(proof):
    """Keep failure evidence readable without serializing whole library scopes."""
    libraries = proof.get("libraries") or []
    changed = [row for row in libraries if row.get("unchanged") is False]
    rows = []
    result = {"schema_version": "crescendo.source_guard_diagnostic.v1", "ok": proof.get("ok"),
        "libraries": rows, "total_libraries": len(libraries), "changed_libraries": len(changed),
        "omitted_changed_libraries": 0,
        "scope": "bounded source content evidence; timestamps and diagnostics do not define source identity"}
    for source in changed[:SOURCE_LIBRARY_DIAGNOSTIC_LIMIT]:
        row = {"library": source.get("library"), "unchanged": False,
               "changes": source.get("changes") or {"state": "scope_details_unavailable", "scopes": []}}
        for side in ("before", "after"):
            snapshot = source.get(side) or {}
            row[side] = {key: snapshot.get(key) for key in
                         ("schema_version", "file_count", "directory_count", "fingerprint")}
            row[side]["scope_count"] = len(snapshot.get("paths") or [])
        # Keep this evidence below producer/inline limits even for huge designs.
        if len(json.dumps(result, ensure_ascii=True)) + len(json.dumps(row, ensure_ascii=True)) > 48 * 1024:
            row["changes"] = {"state": "scope_details_omitted", "scopes": [],
                              "message": "Changed-scope details exceed the bounded producer evidence budget."}
        if len(json.dumps(result, ensure_ascii=True)) + len(json.dumps(row, ensure_ascii=True)) > 48 * 1024:
            break
        rows.append(row)
    result["omitted_changed_libraries"] = len(changed) - len(rows)
    if isinstance(proof.get("setup_baseline"), dict):
        baseline = proof["setup_baseline"]
        result["setup_baseline"] = {key: baseline.get(key) for key in ("ok", "baseline_id")}
        if baseline.get("error"):
            result["setup_baseline"]["error"] = str(baseline["error"])[:320]
    return result


def verify_sources_unchanged(workspace):
    rows = []
    for library, before in sorted(workspace["source_snapshots"].items()):
        after = tree_content_fingerprint(workspace["source_libraries"][library], before.get("paths"))
        rows.append({
            "library": library, "unchanged": _content_snapshots_equal(before, after),
            "before": before, "after": after,
            "changes": source_scope_changes(before, after, library, workspace),
        })
    result = {"ok": all(row["unchanged"] for row in rows), "libraries": rows}
    if (workspace.get("setup") or {}).get("baseline"):
        from .setup_baseline import verify_setup_baseline
        try:
            baseline = verify_setup_baseline(workspace["setup"])
            result["setup_baseline"] = {"ok": True, "baseline_id": baseline["baseline_id"]}
        except (OSError, ValueError) as error:
            result.update(ok=False, setup_baseline={"ok": False, "error": str(error)})
    return result


def _suffix_of(value):
    match = re.match(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?([A-Za-z]+)?$", str(value or "").strip())
    return (match.group(1) or "") if match else ""


def render_physical_value(value, parameter):
    cdf_type = str(parameter.get("cdf_type") or "").lower()
    if cdf_type in ("int", "integer", "fixnum"):
        return int(round(float(value))), "int"
    if cdf_type in ("float", "flonum"):
        return float(value), "float"
    if isinstance(value, str):
        return value, "string"
    suffix = _suffix_of(parameter.get("current_value")) or _suffix_of(parameter.get("default_value"))
    if suffix and suffix.lower() in SPICE_SUFFIXES:
        rendered = "%.12g%s" % (float(value) / SPICE_SUFFIXES[suffix.lower()], suffix)
    else:
        rendered = "%.12g" % float(value)
    return rendered, "string"


def _skill_quote(value):
    return str(value).replace("\\", "\\\\").replace('"', '\\"')


def _workspace_startup_load_lines(workspace):
    init_path = os.path.join(os.path.abspath(workspace["root"]), ".cdsinit")
    return [
        "unless(boundp('crescWorkspaceStartupLoaded)",
        '  load("%s")' % _skill_quote(init_path),
        ")",
        "unless(boundp('crescWorkspaceStartupLoaded) && crescWorkspaceStartupLoaded "
        'error("Crescendo private workspace startup did not complete\\n"))',
    ]


def bounded_skill_body_lines(lines, result_name="crescSkillBodyResult", max_forms=128,
                             max_bytes=65536):
    """Render ordered complete expressions with a bounded, latched errset.

    A single generated progn can overflow Cadence's value stack before its
    first statement executes.  load() alone is not a failure fence: Cadence
    can report a file error and still return t.  Each bounded block therefore
    clears its success latch *before* compilation/evaluation, and a failed
    block prevents every later block from performing mutations.
    """
    if not re.match(r"^[A-Za-z][A-Za-z0-9_]*$", result_name):
        raise ValueError("invalid bounded SKILL result symbol")
    if int(max_forms) < 1 or int(max_bytes) < 1024:
        raise ValueError("invalid bounded SKILL block limits")
    forms, pending = [], []
    depth, quoted, escaped, block_comment = 0, False, False, False
    for number, raw in enumerate(lines, 1):
        raw = str(raw)
        pending.append(raw)
        index = 0
        while index < len(raw):
            char = raw[index]
            pair = raw[index:index + 2]
            if block_comment:
                if pair == "*/":
                    block_comment = False
                    index += 2
                    continue
            elif quoted:
                if escaped:
                    escaped = False
                elif char == "\\":
                    escaped = True
                elif char == '"':
                    quoted = False
            elif pair == "/*":
                block_comment = True
                index += 2
                continue
            elif char == ";" or pair == "//":
                break
            elif char == '"':
                quoted = True
            elif char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
                if depth < 0:
                    raise ValueError("unbalanced generated SKILL at line %d" % number)
            index += 1
        if depth == 0 and not quoted and not block_comment:
            forms.append((number - len(pending) + 1, number, list(pending)))
            pending = []
    if pending or depth or quoted or block_comment:
        raise ValueError("incomplete generated SKILL body")
    chunks, chunk, size = [], [], 0
    for form in forms:
        encoded_size = len(("\n".join(form[2]) + "\n").encode("utf-8"))
        if chunk and (len(chunk) >= int(max_forms) or size + encoded_size > int(max_bytes)):
            chunks.append(chunk)
            chunk, size = [], 0
        # Never split a foreach/when/procedure; its semantics and callbacks
        # depend on keeping the complete expression intact.
        chunk.append(form)
        size += encoded_size
    if chunk:
        chunks.append(chunk)
    rendered = ["%s = list(t)" % result_name]
    prefix = result_name + "_"
    for number, rows in enumerate(chunks, 1):
        rendered.extend([
            "when(%s" % result_name,
            "  %sblock = %d" % (prefix, number),
            "  %sfirstLine = %d" % (prefix, rows[0][0]),
            "  %slastLine = %d" % (prefix, rows[-1][1]),
            '  printf("CRESCENDO_SKILL_BLOCK %s %%L/%%L lines=%%L..%%L\\n" %sblock %d %sfirstLine %slastLine)' % (
                result_name, prefix, len(chunks), prefix, prefix),
            ")",
            "%sallowed = %s" % (prefix, result_name),
            "%s = nil" % result_name,
            "when(%sallowed" % prefix,
            "  %s = errset(progn(" % result_name,
        ])
        rendered.extend(line for row in rows for line in row[2])
        rendered.extend(["    t", "  ) t)", ")"])
    return rendered, {"schema_version": "crescendo.bounded_skill.v1",
                      "block_count": len(chunks), "expression_count": len(forms),
                      "body_line_count": len(lines), "body_bytes": len(("\n".join(lines) + "\n").encode("utf-8")),
                      "max_forms_per_block": int(max_forms), "max_bytes_per_block": int(max_bytes)}


def _design_key(parameter):
    return (
        parameter.get("design_library"), parameter.get("design_cell"),
        parameter.get("design_view") or "schematic",
    )


def _clone_cell_name(parameter, occurrence):
    occurrence_path = occurrence["hierarchy_path"]
    master = occurrence
    base = re.sub(r"[^A-Za-z0-9_]", "_", str(master.get("master_cell") or "cell"))[:10]
    raw = "\x1f".join(str(part) for part in (_design_key(parameter) + (
        occurrence_path, master.get("master_library"), master.get("master_cell"),
    )))
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]
    name = "cr_%s_%s" % (base, digest)
    if len(name) > MAX_CLONE_CELL_NAME:
        raise ParameterApplicationFailure("generated clone cell name exceeds %d characters: %s" % (MAX_CLONE_CELL_NAME, name))
    return name


def build_occurrence_clone_plan(workspace, parameters, values, clone_state="absent"):
    if clone_state not in ("absent", "existing", "mixed"):
        raise ValueError("unsupported occurrence clone state: %s" % clone_state)
    if workspace.get("copy_policy") == "same_library_views":
        return build_same_library_clone_plan(workspace, parameters, values, clone_state)
    copied = workspace["copied_libraries"]
    clone_by_occurrence = {}
    clone_names = {}
    for parameter in parameters:
        if parameter.get("backend", "schematic") != "schematic" or parameter["parameter_id"] not in values:
            continue
        chain = parameter.get("occurrence_chain") or []
        if parameter.get("owner_occurrence_path", "/") != "/" and not chain:
            raise ParameterApplicationFailure(
                "nested parameter lacks occurrence metadata; reload the Maestro setup: %s" % parameter["parameter_id"]
            )
        seen_cells = set()
        for occurrence in chain:
            config_bound = bool(occurrence.get("resolved_library"))
            source_identity = (
                occurrence.get("resolved_library") if config_bound else occurrence.get("master_library"),
                occurrence.get("resolved_cell") if config_bound else occurrence.get("master_cell"),
            )
            if source_identity in seen_cells:
                raise ParameterApplicationFailure(
                    "cyclic schematic hierarchy is not safe to clone independently: %s" % occurrence.get("hierarchy_path")
                )
            seen_cells.add(source_identity)
            library, cell = source_identity
            if library not in copied:
                raise ParameterApplicationFailure(
                    "hierarchical occurrence belongs to a non-copied library and will not be cloned: %s/%s" % (library, cell)
                )
            occurrence_key = _design_key(parameter) + (occurrence["hierarchy_path"],)
            if occurrence_key in clone_by_occurrence:
                continue
            clone_cell = _clone_cell_name(parameter, occurrence)
            clone_identity = (library, clone_cell)
            prior = clone_names.get(clone_identity)
            if prior and prior != occurrence_key:
                raise ParameterApplicationFailure("deterministic clone-name collision for %s/%s" % clone_identity)
            clone_names[clone_identity] = occurrence_key
            clone_exists = os.path.exists(os.path.join(copied[library], clone_cell))
            if clone_state == "absent" and clone_exists:
                raise ParameterApplicationFailure("clone cell already exists in private workspace: %s/%s" % clone_identity)
            if clone_state == "existing" and not clone_exists:
                raise ParameterApplicationFailure("expected clone cell is missing from private workspace: %s/%s" % clone_identity)
            clone_by_occurrence[occurrence_key] = {
                "design_library": parameter.get("design_library"),
                "design_cell": parameter.get("design_cell"),
                "design_view": parameter.get("design_view") or "schematic",
                "occurrence_path": occurrence["hierarchy_path"],
                "parent_occurrence_path": occurrence.get("parent_path") or "/",
                "instance": occurrence.get("name"),
                "source_library": library,
                "source_cell": cell,
                "master_view": occurrence.get("master_view") or "symbol",
                "master_view_type": occurrence.get("master_view_type") or "schematicSymbol",
                "clone_library": private_library_name(workspace, library),
                "clone_cell": clone_cell,
                "clone_preexisting": clone_exists,
                "source_reference": workspace.get("copy_policy") == "source_reference",
            }
            if config_bound:
                clone_by_occurrence[occurrence_key].update({
                    "config_bound": True,
                    "source_view": occurrence.get("resolved_view") or "schematic",
                    "source_view_type": occurrence.get("resolved_view_type") or "schematic",
                    "parent_view": occurrence.get("owner_view") or "schematic",
                    "parent_view_type": occurrence.get("owner_view_type") or "schematic",
                    "parent_source_library": occurrence.get("owner_library"),
                    "parent_source_cell": occurrence.get("owner_cell"),
                    "symbol_source_library": occurrence.get("master_library"),
                    "symbol_source_cell": occurrence.get("master_cell"),
                })
    for occurrence_key, row in clone_by_occurrence.items():
        parent_path = row["parent_occurrence_path"]
        if parent_path == "/":
            row["parent_library"] = private_library_name(workspace, row.get("parent_source_library") or row["design_library"])
            row["parent_cell"] = row.get("parent_source_cell") or row["design_cell"]
        else:
            parent_key = occurrence_key[:3] + (parent_path,)
            parent = clone_by_occurrence.get(parent_key)
            if not parent:
                raise ParameterApplicationFailure("clone DAG is missing parent occurrence: %s" % parent_path)
            row["parent_library"] = parent["clone_library"]
            row["parent_cell"] = parent["clone_cell"]
    return sorted(clone_by_occurrence.values(), key=lambda row: (row["occurrence_path"].count("/"), row["occurrence_path"]))


def _skill_literal(value, prop_type):
    if prop_type == "string":
        return '"%s"' % _skill_quote(value)
    if prop_type == "int":
        return str(int(value))
    return "%.16g" % float(value)


def _cdf_snapshot_skill(record_kind, parameter_id):
    return [
        'snapshotBase = cdfGetBaseCellCDF(ddGetObj(inst~>master~>libName inst~>master~>cellName))',
        'snapshotSim = get(snapshotBase \'simInfo)',
        'snapshotSpec = get(snapshotSim \'spectre)',
        'snapshotNames = get(snapshotSpec \'instParameters)',
        'snapshotRawMappings = get(snapshotSpec \'propMapping)',
        'snapshotMappingRows = nil',
        'snapshotPairs = snapshotRawMappings',
        'when(snapshotPairs && car(snapshotPairs) == nil snapshotPairs = cdr(snapshotPairs))',
        'while(snapshotPairs',
        '  unless(cdr(snapshotPairs) error("CRESCENDO_ADAPTER_ERROR:cdf_parameter_mapping_incompatible odd Spectre propMapping length\\n"))',
        '  snapshotNetName = car(snapshotPairs)',
        '  snapshotCdfName = cadr(snapshotPairs)',
        '  unless((symbolp(snapshotNetName) || stringp(snapshotNetName)) && (symbolp(snapshotCdfName) || stringp(snapshotCdfName)) error("CRESCENDO_ADAPTER_ERROR:cdf_parameter_mapping_incompatible non-name Spectre propMapping entry\\n"))',
        '  snapshotNetSymbol = if(stringp(snapshotNetName) then stringToSymbol(snapshotNetName) else snapshotNetName)',
        '  snapshotCdfSymbol = if(stringp(snapshotCdfName) then stringToSymbol(snapshotCdfName) else snapshotCdfName)',
        '  when(assoc(snapshotNetSymbol snapshotMappingRows) error("CRESCENDO_ADAPTER_ERROR:cdf_parameter_mapping_incompatible duplicate Spectre propMapping target %L\\n" snapshotNetSymbol))',
        '  snapshotMappingRows = cons(list(snapshotNetSymbol snapshotCdfSymbol) snapshotMappingRows)',
        '  fprintf(crescVerify "%s_MAP\\t%%L\\t%%L\\t%%L\\n" "%s" snapshotCdfSymbol snapshotNetSymbol)' % (
            record_kind, _skill_quote(parameter_id),
        ),
        '  snapshotPairs = cddr(snapshotPairs)',
        ')',
        'foreach(snapshotName snapshotNames',
        '  snapshotSymbol = if(stringp(snapshotName) then stringToSymbol(snapshotName) else snapshotName)',
        '  snapshotMapping = assoc(snapshotSymbol snapshotMappingRows)',
        '  snapshotSourceSymbol = if(snapshotMapping then cadr(snapshotMapping) else snapshotSymbol)',
        '  effectiveParam = get(cdfgData snapshotSourceSymbol)',
        '  effectiveFields = if(effectiveParam then errset(list(snapshotSymbol effectiveParam~>value)) else nil)',
        '  when(effectiveFields',
        '    effectiveFields = car(effectiveFields)',
        '    fprintf(crescVerify "%s\\t%%L\\t%%L\\t%%L\\n" "%s" car(effectiveFields) cadr(effectiveFields))' % (
            record_kind, _skill_quote(parameter_id),
        ),
        '  )',
        ')',
    ]


def _schematic_parameter_rows(workspace, parameters, values, target_mode="patched",
                              clone_state="absent", render_values=True):
    if target_mode not in ("baseline", "patched"):
        raise ValueError("unsupported schematic parameter target: %s" % target_mode)
    clone_plan = build_occurrence_clone_plan(
        workspace, parameters, values, clone_state=clone_state,
    )
    clone_lookup = {
        (clone["design_library"], clone["design_cell"], clone["design_view"], clone["occurrence_path"]): clone
        for clone in clone_plan
    }
    rows = []
    copied = workspace["copied_libraries"]
    for parameter in parameters:
        if parameter.get("backend", "schematic") != "schematic":
            continue
        parameter_id = parameter["parameter_id"]
        if parameter_id not in values:
            continue
        owner_library = parameter["owner_library"]
        if owner_library not in copied:
            raise ParameterApplicationFailure("selected parameter owner library is not copied: %s" % owner_library)
        owner_occurrence_path = parameter.get("owner_occurrence_path") or "/"
        if target_mode == "baseline":
            target_library = owner_library
            target_cell = parameter["owner_cell"]
        elif owner_occurrence_path == "/" and workspace.get("copy_policy") != "same_library_views":
            target_library = private_library_name(workspace, owner_library)
            target_cell = parameter["owner_cell"]
        else:
            clone = clone_lookup.get(_design_key(parameter) + (owner_occurrence_path,))
            if not clone:
                raise ParameterApplicationFailure("no private clone exists for owner occurrence %s" % owner_occurrence_path)
            target_library = clone["clone_library"]
            target_cell = clone["clone_cell"]
        instance_name = (parameter.get("hierarchy_path") or "").rstrip("/").split("/")[-1]
        rendered, prop_type = render_physical_value(values[parameter_id], parameter) if render_values else (None, None)
        rows.append({
            "design_library": parameter.get("design_library"),
            "design_cell": parameter.get("design_cell"),
            "design_view": parameter.get("design_view") or "schematic",
            "hierarchy_path": parameter.get("hierarchy_path"),
            "owner_occurrence_path": owner_occurrence_path,
            "library": target_library,
            "cell": target_cell,
            "instance": instance_name,
            "master_library": parameter.get("master_library"),
            "master_cell": parameter.get("master_cell"),
            "property": parameter["name"],
            "effective_aliases": list(parameter.get("effective_aliases") or []),
            "prop_type": prop_type,
            "value": rendered,
            "parameter_id": parameter_id,
        })
        if parameter.get("owner_view") or parameter.get("resolved_top") or workspace.get("private_library_names"):
            rows[-1].update({
                "view": clone.get("target_view") if target_mode != "baseline" and workspace.get("copy_policy") == "same_library_views" else parameter.get("owner_view") or "schematic",
                "view_type": parameter.get("owner_view_type") or "schematic",
                "netlist_design_library": ((parameter.get("resolved_top") or {}).get("library") or parameter.get("design_library")) if workspace.get("copy_policy") == "same_library_views" else private_design_identity(workspace, parameter.get("resolved_top") or {"library": parameter.get("design_library"), "cell": parameter.get("design_cell"), "view": parameter.get("design_view") or "schematic"})[0],
                "netlist_design_cell": (parameter.get("resolved_top") or {}).get("cell") or parameter.get("design_cell"),
            })
    return clone_plan, rows


def write_parameter_snapshot(workspace, parameters, values, output_path, verification_path, record_kind,
                             target_mode="patched", exit_after=True, clone_state="absent"):
    if record_kind not in ("BASELINE", "POST"):
        raise ValueError("unsupported snapshot record kind: %s" % record_kind)
    clone_plan, rows = _schematic_parameter_rows(
        workspace, parameters, values, target_mode, clone_state=clone_state,
    )
    lines = [
        'printf("CRESCENDO_SNAPSHOT_BEGIN\\n")',
        'crescVerify = outfile("%s")' % _skill_quote(verification_path),
    ]
    grouped = {}
    for row in rows:
        grouped.setdefault((row["library"], row["cell"], row.get("view", "schematic"), row.get("view_type", "schematic"), row["instance"]), []).append(row)
    for (library, cell, view, view_type, instance_name), instance_rows in sorted(grouped.items()):
        lines.append('cv = dbOpenCellViewByType("%s" "%s" "%s" "%s" "r")' % (
            _skill_quote(library), _skill_quote(cell), _skill_quote(view), _skill_quote(view_type),
        ))
        lines.append('unless(cv error("Crescendo cannot open snapshot cell %s/%s/schematic\\n"))' % (
            _skill_quote(library), _skill_quote(cell),
        ))
        lines.append('snapshotInstances = setof(item cv~>instances item~>name == "%s")' % _skill_quote(instance_name))
        lines.append('unless(length(snapshotInstances) == 1 error("Crescendo snapshot instance is not unique: %s/%s/%s count=%%L\\n" length(snapshotInstances)))' % (
            _skill_quote(library), _skill_quote(cell), _skill_quote(instance_name),
        ))
        lines.extend([
            'inst = car(snapshotInstances)',
            'cdfgData = cdfGetInstCDF(inst)',
        ])
        for row in sorted(instance_rows, key=lambda item: item["parameter_id"]):
            lines.extend([
                'snapshotProp = dbFindProp(inst "%s")' % _skill_quote(row["property"]),
                'snapshotCdfParam = get(cdfgData stringToSymbol("%s"))' % _skill_quote(row["property"]),
                'unless(snapshotCdfParam error("Crescendo snapshot CDF parameter is missing: %s\\n"))' % _skill_quote(row["property"]),
                'snapshotValue = if(snapshotProp then snapshotProp~>value else snapshotCdfParam~>value)',
                'fprintf(crescVerify "%s_DIRECT\\t%%L\\t%%L\\t%%L\\t%%L\\t%%L\\t%%L\\t%%L\\n" "%s" "%s" "%s" "%s" "%s" snapshotValue snapshotCdfParam~>value)' % (
                    record_kind, _skill_quote(row["parameter_id"]), _skill_quote(library),
                    _skill_quote(cell), _skill_quote(instance_name), _skill_quote(row["property"]),
                ),
            ])
            lines.extend(_cdf_snapshot_skill(record_kind, row["parameter_id"]))
        lines.append('dbClose(cv)')
    lines.append('close(crescVerify)')
    lines, generation = bounded_skill_body_lines(lines, "crescParameterSnapshotResult")
    lines.extend(['unless(crescParameterSnapshotResult crescGeneratedPhaseFailed = t)',
                  'if(crescParameterSnapshotResult then',
                  '  printf("CRESCENDO_SNAPSHOT_DONE\\n")',
                  'else',
                  '  error("Crescendo parameter snapshot failed; see preceding native error\\n")', ')'])
    if exit_after:
        lines.append('exit(if(crescParameterSnapshotResult then 0 else 1))')
    atomic_write_text(output_path, "\n".join(lines) + "\n")
    return {
        "clones": clone_plan if target_mode == "patched" else [],
        "parameters": rows,
        "verification_path": verification_path,
        "record_kind": record_kind,
        "target_mode": target_mode,
        "generation": generation,
    }


def write_schematic_patch(workspace, parameters, values, output_path, verification_path=None,
                          pdk_adapter="generic-cdf", emit_clones=True, emit_parameters=True,
                          exit_after=True, clone_state="absent"):
    verification_path = verification_path or os.path.splitext(output_path)[0] + ".records"
    if emit_parameters:
        clone_plan, parameter_rows = _schematic_parameter_rows(
            workspace, parameters, values, "patched", clone_state=clone_state,
        )
    else:
        # Clone/config preparation needs selection identities only.  Do not
        # render original ADE expressions as candidate numbers, or require a
        # fabricated numeric seed for a transaction that changes no values.
        clone_plan = build_occurrence_clone_plan(workspace, parameters, values, clone_state)
        parameter_rows = []
    adapter_path = None
    if pdk_adapter and pdk_adapter != GENERIC_ADAPTER_ID:
        adapter_path = os.path.abspath(os.path.expanduser(str(pdk_adapter)))
        if not os.path.isfile(adapter_path):
            raise ParameterApplicationFailure(
                "unsupported PDK adapter or missing SKILL file: %s" % pdk_adapter,
                code="cdf_update_api_incompatible",
            )
    lines = [
        'printf("CRESCENDO_PATCH_BEGIN\\n")',
        'crescVerify = outfile("%s")' % _skill_quote(verification_path),
    ]
    if adapter_path:
        lines.extend([
            'load("%s")' % _skill_quote(adapter_path),
            'unless(isCallable(\'crescPdkApplyParameter) error("CRESCENDO_ADAPTER_ERROR:cdf_update_api_incompatible PDK adapter must define crescPdkApplyParameter\\n"))',
        ])
    emitted_clones = [clone for clone in clone_plan if not clone["clone_preexisting"]]
    if workspace.get("copy_policy") == "same_library_views" and emit_clones:
        lines.extend(same_library_copy_lines(emitted_clones, "crescVerify"))
    for clone in emitted_clones if emit_clones else []:
        if clone.get("same_library_view"):
            continue
        views = [(clone.get("source_view", "schematic"), clone.get("source_view_type", "schematic"))]
        if clone["master_view"] != views[0][0]:
            views.append((clone["master_view"], clone["master_view_type"]))
        for view, view_type in views:
            source_library = clone.get("symbol_source_library") if clone.get("config_bound") and view == clone["master_view"] else clone["source_library"]
            source_cell = clone.get("symbol_source_cell") if clone.get("config_bound") and view == clone["master_view"] else clone["source_cell"]
            lines.append('srcCv = dbOpenCellViewByType("%s" "%s" "%s" "%s" "r")' % (
                _skill_quote(source_library), _skill_quote(source_cell),
                _skill_quote(view), _skill_quote(view_type),
            ))
            lines.append('unless(srcCv error("Crescendo cannot open clone source %s/%s/%s\\n"))' % (
                _skill_quote(clone["source_library"]), _skill_quote(clone["source_cell"]), _skill_quote(view),
            ))
            lines.append('dstCv = dbCopyCellView(srcCv "%s" "%s" "%s")' % (
                _skill_quote(clone["clone_library"]), _skill_quote(clone["clone_cell"]), _skill_quote(view),
            ))
            lines.append('unless(dstCv error("Crescendo failed to clone %s/%s/%s\\n"))' % (
                _skill_quote(clone["source_library"]), _skill_quote(clone["source_cell"]), _skill_quote(view),
            ))
            lines.extend(['dbClose(srcCv)', 'dbClose(dstCv)'])
        lines.extend(config_clone_cdf_lines(clone))
        lines.append('parentCv = dbOpenCellViewByType("%s" "%s" "%s" "%s" "a")' % (
            _skill_quote(clone["parent_library"]), _skill_quote(clone["parent_cell"]),
            _skill_quote(clone.get("parent_view", "schematic")), _skill_quote(clone.get("parent_view_type", "schematic")),
        ))
        lines.append('unless(parentCv error("Crescendo cannot open clone parent %s/%s/schematic\\n"))' % (
            _skill_quote(clone["parent_library"]), _skill_quote(clone["parent_cell"]),
        ))
        lines.append('oldInst = car(setof(item parentCv~>instances item~>name == "%s"))' % _skill_quote(clone["instance"]))
        lines.append('unless(oldInst error("Crescendo missing occurrence instance %s in %s/%s\\n"))' % (
            _skill_quote(clone["instance"]), _skill_quote(clone["parent_library"]), _skill_quote(clone["parent_cell"]),
        ))
        lines.append('unless(oldInst~>objType == "inst" error("Crescendo does not support independent cloning of arrays/mosaics: %s\\n"))' % _skill_quote(clone["occurrence_path"]))
        lines.append('when(oldInst~>magnification && oldInst~>magnification != 1.0 error("Crescendo does not support magnified hierarchical instances: %s\\n"))' % _skill_quote(clone["occurrence_path"]))
        lines.extend([
            'oldName = oldInst~>name',
            'oldXY = oldInst~>xy',
            'oldOrient = oldInst~>orient',
            'oldParams = foreach(mapcar prop oldInst~>prop list(prop~>name prop~>valueType prop~>value))',
            'oldConnections = foreach(mapcar instTerm oldInst~>instTerms list(instTerm~>term~>name if(instTerm~>net then instTerm~>net~>name else nil)))',
            'dbDeleteObject(oldInst)',
        ])
        lines.append('cloneMaster = dbOpenCellViewByType("%s" "%s" "%s" "%s" "r")' % (
            _skill_quote(clone["clone_library"]), _skill_quote(clone["clone_cell"]),
            _skill_quote(clone["master_view"]), _skill_quote(clone["master_view_type"]),
        ))
        lines.append('unless(cloneMaster error("Crescendo cannot open cloned master %s/%s/%s\\n"))' % (
            _skill_quote(clone["clone_library"]), _skill_quote(clone["clone_cell"]), _skill_quote(clone["master_view"]),
        ))
        lines.extend([
            'newInst = dbCreateParamInst(parentCv cloneMaster oldName oldXY oldOrient 1 oldParams)',
            'unless(newInst error("Crescendo failed to recreate independent occurrence\\n"))',
            'schCheck(parentCv)',
            'unless(newInst~>name == oldName && newInst~>xy == oldXY && newInst~>orient == oldOrient error("Crescendo recreated occurrence geometry mismatch\\n"))',
        ])
        lines.append('unless(newInst~>master~>libName == "%s" && newInst~>master~>cellName == "%s" && newInst~>master~>viewName == "%s" error("Crescendo recreated occurrence master mismatch\\n"))' % (
            _skill_quote(clone["clone_library"]), _skill_quote(clone["clone_cell"]), _skill_quote(clone["master_view"]),
        ))
        lines.extend([
            'unless(length(newInst~>instTerms) == length(oldConnections) error("Crescendo recreated occurrence terminal count mismatch\\n"))',
            'foreach(connection oldConnections',
            '  newTerm = car(setof(term newInst~>instTerms term~>term~>name == car(connection)))',
            '  unless(newTerm error("Crescendo recreated occurrence missing terminal %L\\n" car(connection)))',
            '  newNetName = if(newTerm~>net then newTerm~>net~>name else nil)',
            '  unless(equal(newNetName cadr(connection)) error("Crescendo recreated occurrence connectivity mismatch at %L\\n" car(connection)))',
            ')',
            'foreach(paramRow oldParams',
            '  newProp = dbFindProp(newInst car(paramRow))',
            '  unless(newProp && equal(newProp~>value caddr(paramRow)) error("Crescendo recreated occurrence property mismatch at %L\\n" car(paramRow)))',
            ')',
            'unless(dbSave(parentCv) error("Crescendo failed to save clone parent\\n"))',
            'dbClose(parentCv)',
            'dbClose(cloneMaster)',
        ])
        lines.append('fprintf(crescVerify "CLONE\\t%%L\\t%%L\\t%%L\\t%%L\\n" "%s" "%s" "%s" "%s")' % (
            _skill_quote(clone["occurrence_path"]), _skill_quote(clone["source_cell"]),
            _skill_quote(clone["clone_cell"]), _skill_quote(clone["parent_cell"]),
        ))
    grouped = {}
    for row in parameter_rows:
        grouped.setdefault((row["library"], row["cell"], row.get("view", "schematic"), row.get("view_type", "schematic")), []).append(row)
    for (library, cell, view, view_type), rows in sorted(grouped.items()) if emit_parameters else []:
        lines.append('cv = dbOpenCellViewByType("%s" "%s" "%s" "%s" "a")' % (_skill_quote(library), _skill_quote(cell), _skill_quote(view), _skill_quote(view_type)))
        lines.append('unless(cv error("Crescendo cannot open copied cell %s/%s/schematic\\n"))' % (_skill_quote(library), _skill_quote(cell)))
        instance_groups = {}
        for row in rows:
            instance_groups.setdefault(row["instance"], []).append(row)
        for instance_name, instance_rows in sorted(instance_groups.items()):
            instance_rows = sorted(instance_rows, key=lambda item: item["property"])
            for row in instance_rows:
                row["instance_selected_count"] = len(instance_rows)
            lines.append('inst = car(setof(item cv~>instances item~>name == "%s"))' % _skill_quote(instance_name))
            lines.append('unless(inst error("Crescendo missing instance %s in %s/%s\\n"))' % (_skill_quote(instance_name), _skill_quote(library), _skill_quote(cell)))
            lines.extend([
                'cdfgData = cdfGetInstCDF(inst)',
                'baseCdf = cdfGetBaseCellCDF(ddGetObj(inst~>master~>libName inst~>master~>cellName))',
            ])
            for row in instance_rows:
                literal = _skill_literal(row["value"], row["prop_type"])
                lines.extend(selected_property_assignment_lines(
                    row, literal, _skill_quote, generic=not adapter_path))
            for row in instance_rows:
                literal = _skill_literal(row["value"], row["prop_type"])
                lines.extend([
                    'cdfResult = errset(cdfGetInstCDF(inst) t)',
                    'unless(cdfResult && car(cdfResult) error("Crescendo cannot obtain instance CDF for parameter callback\\n"))',
                    'cdfgData = car(cdfResult)',
                    'cdfParam = get(cdfgData stringToSymbol("%s"))' % _skill_quote(row["property"]),
                    'unless(cdfParam error("Crescendo CDF parameter disappeared after update: %s\\n"))' % _skill_quote(row["property"]),
                    'cdfParam~>value = %s' % literal,
                ])
                if adapter_path:
                    lines.extend([
                        'callbackResult = errset(crescPdkApplyParameter(inst "%s" %s) t)' % (_skill_quote(row["property"]), literal),
                        'unless(callbackResult && car(callbackResult) error("CRESCENDO_ADAPTER_ERROR:cdf_parameter_mapping_incompatible PDK adapter failed for %s\\n"))' % _skill_quote(row["property"]),
                    ])
                else:
                    lines.extend([
                        'callbackExpr = cdfParam~>callback',
                        'when(callbackExpr && callbackExpr != ""',
                        "  crescCallbackDefaultValue = get(cdfParam 'defValue)",
                        '  crescCallbackDefaultPresent = numberp(crescCallbackDefaultValue) || stringp(crescCallbackDefaultValue)',
                    ])
                    lines.extend(callback_failure_lines(
                        row, literal, library, cell, view, instance_name,
                        [item["property"] for item in instance_rows], _skill_quote))
                    lines.extend([
                        '  crescCallbackEffectiveValue = cdfParam~>value',
                        ')',
                        'when(isCallable(\'cdfUpdateInstParam)',
                        '  updateResult = errset(cdfUpdateInstParam(inst) t)',
                        '  unless(updateResult error("CRESCENDO_ADAPTER_ERROR:cdf_update_api_incompatible cdfUpdateInstParam failed for %s\\n"))' % _skill_quote(row["property"]),
                        ')',
                    ])
                    lines.extend(callback_value_proof_lines(row, _skill_quote))
            for row in instance_rows:
                literal = _skill_literal(row["value"], row["prop_type"])
                lines.extend([
                    'schematicProp = dbFindProp(inst "%s")' % _skill_quote(row["property"]),
                    'cdfgData = cdfGetInstCDF(inst)',
                    'cdfParam = get(cdfgData stringToSymbol("%s"))' % _skill_quote(row["property"]),
                    'unless(cdfParam error("Crescendo updated CDF parameter is missing: %s\\n"))' % _skill_quote(row["property"]),
                    'schematicValue = if(schematicProp then schematicProp~>value else cdfParam~>value)',
                ])
                if adapter_path:
                    lines.extend(['callbackStatus = "adapter_executed"', 'callbackExpr = "pdk_adapter"'])
                else:
                    lines.extend([
                        'callbackExpr = cdfParam~>callback',
                        'callbackStatus = if(callbackExpr && callbackExpr != "" then "executed" else "not_defined")',
                    ])
                lines.extend([
                    'fprintf(crescVerify "PARAM\\t%%L\\t%%L\\t%%L\\t%%L\\t%%L\\t%%L\\n" "%s" %s schematicValue cdfParam~>value callbackStatus callbackExpr)' % (
                        _skill_quote(row["parameter_id"]), literal,
                    ),
                    'printf("CRESCENDO_PATCH %%s/%%s %%s=%%L id=%%s\\n" "%s" "%s" "%s" %s "%s")' % (
                        _skill_quote(cell), _skill_quote(row["instance"]), _skill_quote(row["property"]), literal, _skill_quote(row["parameter_id"])
                    ),
                ])
        lines.append('schCheck(cv)')
        lines.append('unless(dbSave(cv) error("Crescendo failed to save copied cell %s/%s\\n"))' % (_skill_quote(library), _skill_quote(cell)))
        lines.append('dbClose(cv)')
    native_config_plan = []
    config_records = verification_path + ".config"
    if emit_clones:
        config_lines, native_config_plan = config_binding_lines(workspace, clone_plan, config_records)
        lines.extend(config_lines)
    export_snapshot_path = verification_path + ".export.tsv"
    if emit_parameters and parameter_rows:
        from .optimized_schematic import export_snapshot_skill
        lines.extend(export_snapshot_skill(parameter_rows, export_snapshot_path))
    lines.append('close(crescVerify)')
    lines, generation = bounded_skill_body_lines(lines, "crescSchematicPatchResult")
    lines.extend(['unless(crescSchematicPatchResult crescGeneratedPhaseFailed = t)',
                  'if(crescSchematicPatchResult then',
                  '  printf("CRESCENDO_PATCH_DONE\\n")',
                  'else',
                  '  error("Crescendo schematic patch failed; see preceding native error\\n")', ')'])
    if exit_after:
        lines.append('exit(if(crescSchematicPatchResult then 0 else 1))')
    atomic_write_text(output_path, "\n".join(lines) + "\n")
    return {
        "clones": emitted_clones if emit_clones else [],
        "parameters": parameter_rows if emit_parameters else [],
        "callback_value_proof_required": bool(emit_parameters and not adapter_path),
        "verification_path": verification_path,
        "config_binding_plan": native_config_plan,
        "config_verification_path": config_records,
        "export_snapshot_path": export_snapshot_path if emit_parameters and parameter_rows else None,
        "generation": generation,
    }


def write_combined_cadence_script(output_path, phase_scripts, exit_after=True):
    lines = ['printf("CRESCENDO_COMBINED_BEGIN\\n")']
    for phase_name, script_path in phase_scripts:
        lines.append('printf("CRESCENDO_PHASE_BEGIN %s\\n")' % _skill_quote(phase_name))
        if exit_after:
            lines.extend([
                'crescGeneratedPhaseFailed = nil',
                'crescCombinedPhaseResult = errset(load("%s") t)' % _skill_quote(script_path),
                'unless(crescCombinedPhaseResult && !crescGeneratedPhaseFailed',
                '  printf("CRESCENDO_PHASE_FAILED %s\\n")' % _skill_quote(phase_name),
                '  exit(1)',
                ')',
            ])
        else:
            lines.append('load("%s")' % _skill_quote(script_path))
        lines.append('printf("CRESCENDO_PHASE_DONE %s\\n")' % _skill_quote(phase_name))
    lines.append('printf("CRESCENDO_COMBINED_DONE\\n")')
    if exit_after:
        lines.append('exit()')
    atomic_write_text(output_path, "\n".join(lines) + "\n")
    return output_path


def _patch_design_vars(path, values):
    try:
        with open(path, "r") as stream:
            text = stream.read()
    except (UnicodeDecodeError, OSError):
        return []
    original = text
    changed = []
    for name, value in sorted(values.items()):
        if os.path.basename(path) in ("maestro.sdb", "data.sdb"):
            pattern = r"(<var>%s\s*<value>)(.*?)(</value>\s*</var>)" % re.escape(name)
            text, count = re.subn(pattern, lambda match: match.group(1) + str(value) + match.group(3), text, flags=re.S)
        elif path.endswith(".state"):
            pattern = (
                r"(<field Name=\"name\" Type=\"string\">\"%s\"</field>\s*"
                r"<field Name=\"expression\" Type=\"string\">)(.*?)(</field>)"
            ) % re.escape(name)
            rendered = str(value)
            if not (rendered.startswith('"') and rendered.endswith('"')):
                rendered = '"%s"' % rendered
            text, count = re.subn(pattern, lambda match: match.group(1) + rendered + match.group(3), text, flags=re.S)
        else:
            count = 0
        if count:
            changed.append({"file": path, "name": name, "value": value, "count": count})
    if text != original:
        atomic_write_text(path, text)
    return changed


def _record_literal(token):
    token = token.strip()
    if token == "nil":
        return None
    if token == "t":
        return True
    try:
        return ast.literal_eval(token)
    except Exception:
        return token


def _parse_patch_verification(path):
    records = []
    if not os.path.isfile(path):
        raise ParameterApplicationFailure("schematic verification artifact was not generated: %s" % path)
    with open(path, "r") as stream:
        for line_number, raw in enumerate(stream, 1):
            columns = raw.rstrip("\n").split("\t")
            if not columns or not columns[0]:
                continue
            records.append({
                "kind": columns[0],
                "values": [_record_literal(value) for value in columns[1:]],
                "line": line_number,
            })
    return records


def _display_resolution(value):
    match = re.match(
        r"^[+-]?(\d+)(?:\.(\d+))?([A-Za-z]+)?$",
        str(value or "").strip(),
    )
    if not match:
        return None
    decimals = len(match.group(2) or "")
    suffix = (match.group(3) or "").lower()
    multiplier = SPICE_SUFFIXES.get(suffix, 1.0) if suffix else 1.0
    return (10.0 ** (-decimals)) * multiplier


def _physical_values_match(expected, actual, relative_tolerance=1.0e-9, absolute_tolerance=1.0e-18,
                           allow_display_rounding=False):
    expected_number = parse_physical_number(expected)
    actual_number = parse_physical_number(actual)
    if expected_number is None or actual_number is None:
        return str(expected) == str(actual)
    tolerance = max(absolute_tolerance, relative_tolerance * max(abs(expected_number), abs(actual_number), 1.0e-30))
    if allow_display_rounding:
        resolution = _display_resolution(actual)
        if resolution is not None:
            magnitude = max(abs(expected_number), abs(actual_number), 1.0e-30)
            quantization_tolerance = min(5.0 * resolution, 5.0e-3 * magnitude)
            tolerance = max(tolerance, quantization_tolerance)
    return abs(expected_number - actual_number) <= tolerance


def validate_schematic_application(patch_plan, allow_effective_adjustment=False):
    records = _parse_patch_verification(patch_plan["verification_path"])
    parameter_records = {}
    callback_value_records = {}
    clone_records = []
    effective_records = {}
    before_records = {}
    for record in records:
        if record["kind"] == "PARAM" and len(record["values"]) >= 6:
            parameter_records.setdefault(str(record["values"][0]), []).append(record)
        elif record["kind"] == "CDF_CALLBACK_EFFECTIVE" and record["values"]:
            callback_value_records.setdefault(str(record["values"][0]), []).append(record)
        elif record["kind"] == "CLONE":
            clone_records.append(record)
        elif record["kind"] == "NETLIST_PARAM" and len(record["values"]) >= 3:
            effective_records.setdefault(str(record["values"][0]), {}).setdefault(
                str(record["values"][1]), []
            ).append(record["values"][2])
        elif record["kind"] == "NETLIST_BEFORE" and len(record["values"]) >= 3:
            before_records.setdefault(str(record["values"][0]), {}).setdefault(
                str(record["values"][1]), []
            ).append(record["values"][2])
    verified = []
    for row in patch_plan["parameters"]:
        matches = parameter_records.get(str(row["parameter_id"]), [])
        if len(matches) != 1:
            raise ParameterApplicationFailure(
                "schematic verification record is not unique for %s: %d" % (row["parameter_id"], len(matches))
            )
        values = matches[0]["values"]
        requested, schematic_value, cdf_value, callback_status, callback_expression = values[1:6]
        callback_proof = None
        proofs = callback_value_records.get(str(row["parameter_id"]), [])
        if patch_plan.get("callback_value_proof_required") and (callback_status == "executed" or proofs):
            if len(proofs) != 1 or len(proofs[0]["values"]) != 7:
                raise ParameterApplicationFailure(
                    "CDF callback value proof is missing or not unique for %s" % row["parameter_id"]
                )
            immediate, refreshed, oa_present, oa_value, default_present, native_default = proofs[0]["values"][1:]
            callback_proof = {"status": "verified", "scope": "selected_parameter_after_callback",
                              "immediate_cdf_value": immediate, "refreshed_cdf_value": refreshed,
                              "oa_property_present": oa_present, "oa_property_value": oa_value,
                              "native_default_present": default_present,
                              "native_default_value": native_default}
            oa_matches = oa_present is True and _physical_values_match(immediate, oa_value)
            # OA may omit a value that equals the current inherited CDF default.
            # Only a numeric native default captured before this callback can
            # prove that omission; an expression or unknown default cannot.
            default_matches = (oa_present is None and default_present is True
                               and parse_physical_number(native_default) is not None
                               and parse_physical_number(immediate) is not None
                               and _physical_values_match(immediate, native_default))
            if not _physical_values_match(immediate, refreshed) or not (oa_matches or default_matches):
                callback_proof["status"] = "mismatch"
                raise ParameterApplicationFailure(
                    "CDF callback value did not persist for %s: immediate=%r refreshed=%r OA_present=%r OA=%r native_default_present=%r native_default=%r" % (
                        row["parameter_id"], immediate, refreshed, oa_present, oa_value,
                        default_present, native_default,
                    ), evidence=callback_proof,
                )
            callback_proof["storage_evidence"] = "oa_property" if oa_matches else "native_cdf_default"
        if not allow_effective_adjustment and not _physical_values_match(
                requested, schematic_value, allow_display_rounding=True):
            raise ParameterApplicationFailure(
                "schematic property mismatch for %s: requested=%r schematic=%r" % (
                    row["parameter_id"], requested, schematic_value,
                )
            )
        if not allow_effective_adjustment and not _physical_values_match(
                requested, cdf_value, allow_display_rounding=True):
            raise ParameterApplicationFailure(
                "CDF value mismatch for %s: requested=%r cdf=%r" % (
                    row["parameter_id"], requested, cdf_value,
                )
            )
        verified.append(dict(row, requested=requested, schematic_value=schematic_value,
                             cdf_value=cdf_value, callback_status=callback_status,
                             callback_expression=callback_expression,
                             netlist_parameters=effective_records.get(str(row["parameter_id"]), {}),
                             netlist_parameters_before=before_records.get(str(row["parameter_id"]), {})))
        if callback_proof is not None:
            verified[-1]["callback_value_proof"] = callback_proof
    if len(clone_records) != len(patch_plan["clones"]):
        raise ParameterApplicationFailure(
            "clone verification count mismatch: planned=%d recorded=%d" % (
                len(patch_plan["clones"]), len(clone_records),
            )
        )
    config_verification = validate_config_binding_records(
        patch_plan.get("config_binding_plan") or [], patch_plan.get("config_verification_path"),
    )
    result = {"ok": True, "parameters": verified, "clones": patch_plan["clones"]}
    if patch_plan.get("config_binding_plan"):
        result["config_verification"] = config_verification
    return result


def validate_parameter_snapshot(patch_plan, allow_effective_adjustment=False):
    records = _parse_patch_verification(patch_plan["verification_path"])
    record_kind = patch_plan["record_kind"]
    direct_kind = "%s_DIRECT" % record_kind
    direct_by_id = {}
    effective_by_id = {}
    mappings_by_id = {}
    for record in records:
        if record["kind"] == direct_kind and len(record["values"]) >= 7:
            direct_by_id.setdefault(str(record["values"][0]), []).append(record["values"][1:7])
        elif record["kind"] == record_kind and len(record["values"]) >= 3:
            effective_by_id.setdefault(str(record["values"][0]), {}).setdefault(
                str(record["values"][1]), []
            ).append(record["values"][2])
        elif record["kind"] == "%s_MAP" % record_kind and len(record["values"]) >= 3:
            mappings_by_id.setdefault(str(record["values"][0]), []).append((
                str(record["values"][1]), str(record["values"][2]),
            ))
    verified = []
    for row in patch_plan["parameters"]:
        matches = direct_by_id.get(str(row["parameter_id"]), [])
        if len(matches) != 1:
            raise ParameterApplicationFailure(
                "%s direct verification record is not unique for %s: %d" % (
                    record_kind, row["parameter_id"], len(matches),
                )
            )
        library, cell, instance, property_name, schematic_value, cdf_value = matches[0]
        expected_identity = (row["library"], row["cell"], row["instance"], row["property"])
        if (library, cell, instance, property_name) != expected_identity:
            raise ParameterApplicationFailure(
                "%s identity mismatch for %s: expected=%r actual=%r" % (
                    record_kind, row["parameter_id"], expected_identity,
                    (library, cell, instance, property_name),
                )
            )
        if record_kind == "POST" and not allow_effective_adjustment:
            if not _physical_values_match(row["value"], schematic_value, allow_display_rounding=True):
                raise ParameterApplicationFailure(
                    "persisted schematic property mismatch for %s: requested=%r schematic=%r" % (
                        row["parameter_id"], row["value"], schematic_value,
                    )
                )
            if not _physical_values_match(row["value"], cdf_value, allow_display_rounding=True):
                raise ParameterApplicationFailure(
                    "persisted CDF value mismatch for %s: requested=%r cdf=%r" % (
                        row["parameter_id"], row["value"], cdf_value,
                    )
                )
        effective = effective_by_id.get(str(row["parameter_id"]), {})
        if not effective:
            raise ParameterApplicationFailure(
                "%s Spectre CDF snapshot is empty for %s" % (record_kind, row["parameter_id"])
            )
        mapping_pairs = mappings_by_id.get(str(row["parameter_id"]), [])
        if len(mapping_pairs) != len(set(mapping_pairs)):
            raise ParameterApplicationFailure(
                "%s Spectre propMapping contains duplicate entries for %s" % (
                    record_kind, row["parameter_id"],
                ), code="cdf_parameter_mapping_incompatible",
            )
        sources_by_alias = {}
        aliases_by_source = {}
        for source_name, alias_name in mapping_pairs:
            sources_by_alias.setdefault(alias_name, set()).add(source_name)
            aliases_by_source.setdefault(source_name, set()).add(alias_name)
        if any(len(sources) != 1 for sources in sources_by_alias.values()):
            raise ParameterApplicationFailure(
                "%s Spectre propMapping is ambiguous for %s" % (record_kind, row["parameter_id"]),
                code="cdf_parameter_mapping_incompatible",
            )
        mapped_aliases = sorted(aliases_by_source.get(str(row["property"]), set()))
        if len(mapped_aliases) > 1:
            raise ParameterApplicationFailure(
                "%s Spectre propMapping maps selected CDF parameter %s to multiple netlist parameters: %s" % (
                    record_kind, row["parameter_id"], ", ".join(mapped_aliases),
                ), code="cdf_parameter_mapping_incompatible",
            )
        effective_aliases = [name for name in mapped_aliases if name != str(row["property"])]
        verified.append(dict(
            row, schematic_value=schematic_value, cdf_value=cdf_value,
            netlist_parameters=effective, effective_aliases=effective_aliases,
        ))
    return {"ok": True, "parameters": verified, "clones": []}


def merge_parameter_verification(application, baseline, post):
    baseline_by_id = {str(row["parameter_id"]): row for row in baseline["parameters"]}
    post_by_id = {str(row["parameter_id"]): row for row in post["parameters"]}
    merged = []
    for row in application["parameters"]:
        parameter_id = str(row["parameter_id"])
        before = baseline_by_id.get(parameter_id)
        after = post_by_id.get(parameter_id)
        if not before or not after:
            raise ParameterApplicationFailure(
                "baseline/post verification is incomplete for %s" % row["parameter_id"]
            )
        before_aliases = sorted(set(before.get("effective_aliases") or []))
        after_aliases = sorted(set(after.get("effective_aliases") or []))
        if before_aliases != after_aliases:
            raise ParameterApplicationFailure(
                "baseline/post Spectre propMapping changed for %s" % row["parameter_id"],
                code="cdf_parameter_mapping_incompatible",
            )
        effective_aliases = sorted(set(
            list(row.get("effective_aliases") or []) + after_aliases
        ))
        merged.append(dict(
            row,
            persisted_schematic_value=after["schematic_value"],
            persisted_cdf_value=after["cdf_value"],
            netlist_parameters_before=before["netlist_parameters"],
            netlist_parameters=after["netlist_parameters"],
            effective_aliases=effective_aliases,
            baseline_identity={
                "library": before["library"], "cell": before["cell"],
                "instance": before["instance"], "property": before["property"],
            },
            post_identity={
                "library": after["library"], "cell": after["cell"],
                "instance": after["instance"], "property": after["property"],
            },
        ))
    return {"ok": True, "parameters": merged, "clones": application["clones"]}


def apply_candidate(workspace, config, parameters, values, candidate_root, timeout_seconds=None,
                    allow_effective_adjustment=False, cancel_check=None, cadence_session=None,
                    emit_occurrence_clones=True):
    candidate_root = ensure_dir(candidate_root)
    design_vars = {}
    for parameter in parameters:
        if parameter.get("backend") == "design_variable" and parameter["parameter_id"] in values:
            design_vars[parameter["name"]] = values[parameter["parameter_id"]]
    design_changes = []
    if design_vars:
        for current, _directories, files in os.walk(workspace["setup_dir"]):
            for name in files:
                if name in ("maestro.sdb", "data.sdb") or name.endswith(".state"):
                    design_changes.extend(_patch_design_vars(os.path.join(current, name), design_vars))
    adapter = config.get("cadence", {}).get("pdk_adapter") or "generic-cdf"
    default_timeout = timeout_seconds if timeout_seconds is not None else config["runtime"].get("command_timeout_seconds", 1800)
    deadline = time.monotonic() + float(default_timeout)
    phases = []
    baseline_skill = os.path.join(candidate_root, "snapshot_schematic_baseline.il")
    baseline_records = os.path.join(candidate_root, "schematic_baseline.records")
    clone_state = "mixed" if emit_occurrence_clones else "existing"
    baseline_plan = write_parameter_snapshot(
        workspace, parameters, values, baseline_skill, baseline_records, "BASELINE", "baseline",
        exit_after=False, clone_state=clone_state,
    )
    baseline_restore_skill = baseline_skill
    if cadence_session is None and baseline_plan["parameters"]:
        baseline_restore_skill = write_combined_cadence_script(
            os.path.join(candidate_root, "snapshot_schematic_baseline_run.il"),
            [("baseline", baseline_skill)],
        )
    clone_skill = os.path.join(candidate_root, "patch_schematic_clone.il")
    clone_records = os.path.join(candidate_root, "schematic_clone.records")
    clone_plan = write_schematic_patch(
        workspace, parameters, values, clone_skill, clone_records, adapter,
        emit_clones=emit_occurrence_clones, emit_parameters=False, exit_after=False,
        clone_state=clone_state,
    )
    parameter_skill = os.path.join(candidate_root, "patch_schematic.il")
    parameter_records = os.path.join(candidate_root, "schematic_application.records")
    parameter_plan = write_schematic_patch(
        workspace, parameters, values, parameter_skill, parameter_records, adapter,
        emit_clones=False, emit_parameters=True, exit_after=False, clone_state=clone_state,
    )
    post_skill = os.path.join(candidate_root, "snapshot_schematic_post.il")
    post_records = os.path.join(candidate_root, "schematic_post.records")
    post_plan = write_parameter_snapshot(
        workspace, parameters, values, post_skill, post_records, "POST", "patched",
        exit_after=False, clone_state=clone_state,
    )
    patch_plan = {
        "clones": clone_plan["clones"],
        "parameters": parameter_plan["parameters"],
        "verification_paths": [baseline_records, clone_records, parameter_records, post_records],
    }
    private_fingerprints_before_baseline = {
        library: tree_fingerprint(path) for library, path in sorted(workspace["copied_libraries"].items())
    }
    combined_skill = os.path.join(candidate_root, "patch_schematic_apply_verify.il")
    combined_logical_phases = []
    if clone_plan["clones"] or clone_plan.get("config_binding_plan"):
        combined_logical_phases.append(("clone", clone_skill, clone_plan))
    if parameter_plan["parameters"]:
        combined_logical_phases.append(("parameters", parameter_skill, parameter_plan))
    if post_plan["parameters"]:
        combined_logical_phases.append(("verify", post_skill, post_plan))
    if combined_logical_phases:
        write_combined_cadence_script(
            combined_skill,
            [(name, skill_path) for name, skill_path, _plan in combined_logical_phases],
            exit_after=cadence_session is None,
        )
    verification_by_phase = {}
    failure = None
    if baseline_plan["parameters"]:
        phase_name = "baseline"
        if cancel_check and cancel_check():
            raise ParameterApplicationInfrastructureFailure(
                "copied schematic callback validation cancelled before %s phase" % phase_name
            )
        remaining = int(deadline - time.monotonic())
        if remaining < 1:
            command = {"returncode": -1, "timed_out": True, "phase": phase_name}
        else:
            command = cadence_session.execute(
                baseline_skill, phase_name, remaining, cancel_check=cancel_check,
            ) if cadence_session is not None else run_command(
                [config["cadence"]["virtuoso_bin"], "-nograph", "-restore", baseline_restore_skill,
                 "-log", os.path.join(candidate_root, "patch_schematic_%s.log" % phase_name)],
                cwd=workspace["root"],
                env=child_environment(config, workspace["root"], include_runtime_ld=True),
                timeout_seconds=remaining,
                stdout_path=os.path.join(candidate_root, "patch_schematic_%s.stdout" % phase_name),
                stderr_path=os.path.join(candidate_root, "patch_schematic_%s.stderr" % phase_name),
                startup_lock=CADENCE_STARTUP_LOCK,
                lock_timeout_seconds=config["runtime"].get("command_timeout_seconds", 1800),
                cancel_check=cancel_check,
            )
            command["phase"] = phase_name
            deadline += float(command.get("lock_wait_seconds", 0.0))
        verification = None
        if command["returncode"] == 0:
            try:
                private_after = {
                    library: tree_fingerprint(path)
                    for library, path in sorted(workspace["copied_libraries"].items())
                }
                changed = [
                    library for library in private_fingerprints_before_baseline
                    if private_fingerprints_before_baseline[library]["fingerprint"] != private_after[library]["fingerprint"]
                ]
                if changed:
                    raise ParameterApplicationFailure(
                        "read-only baseline phase changed private copied libraries: %s" % ", ".join(changed)
                    )
                verification = validate_parameter_snapshot(
                    baseline_plan, allow_effective_adjustment=allow_effective_adjustment,
                )
                verification_by_phase[phase_name] = verification
            except Exception as error:
                failure = error
        phase_row = {"name": phase_name, "plan": baseline_plan, "command": command, "verification": verification}
        if failure:
            phase_row["validation_error"] = "%s: %s" % (failure.__class__.__name__, failure)
        phases.append(phase_row)
    can_apply = not failure and (not phases or phases[-1]["command"]["returncode"] == 0)
    if can_apply and combined_logical_phases:
        phase_name = "apply_verify"
        if cancel_check and cancel_check():
            raise ParameterApplicationInfrastructureFailure(
                "copied schematic callback validation cancelled before %s phase" % phase_name
            )
        remaining = int(deadline - time.monotonic())
        if remaining < 1:
            command = {"returncode": -1, "timed_out": True, "phase": phase_name}
        else:
            command = cadence_session.execute(
                combined_skill, phase_name, remaining, cancel_check=cancel_check,
            ) if cadence_session is not None else run_command(
                [config["cadence"]["virtuoso_bin"], "-nograph", "-restore", combined_skill,
                 "-log", os.path.join(candidate_root, "patch_schematic_%s.log" % phase_name)],
                cwd=workspace["root"],
                env=child_environment(config, workspace["root"], include_runtime_ld=True),
                timeout_seconds=remaining,
                stdout_path=os.path.join(candidate_root, "patch_schematic_%s.stdout" % phase_name),
                stderr_path=os.path.join(candidate_root, "patch_schematic_%s.stderr" % phase_name),
                startup_lock=CADENCE_STARTUP_LOCK,
                lock_timeout_seconds=config["runtime"].get("command_timeout_seconds", 1800),
                cancel_check=cancel_check,
            )
            command["phase"] = phase_name
            deadline += float(command.get("lock_wait_seconds", 0.0))
        logical_rows = []
        if command["returncode"] == 0:
            for logical_name, _skill_path, logical_plan in combined_logical_phases:
                logical_verification = None
                try:
                    if logical_name == "verify":
                        logical_verification = validate_parameter_snapshot(
                            logical_plan, allow_effective_adjustment=allow_effective_adjustment,
                        )
                    else:
                        logical_verification = validate_schematic_application(
                            logical_plan, allow_effective_adjustment=allow_effective_adjustment,
                        )
                    verification_by_phase[logical_name] = logical_verification
                except Exception as error:
                    failure = error
                logical_row = {
                    "name": logical_name, "plan": logical_plan,
                    "verification": logical_verification,
                }
                if failure:
                    logical_row["validation_error"] = "%s: %s" % (failure.__class__.__name__, failure)
                logical_rows.append(logical_row)
                if failure:
                    break
        phase_row = {
            "name": phase_name, "command": command,
            "logical_phases": logical_rows,
        }
        if failure:
            phase_row["validation_error"] = "%s: %s" % (failure.__class__.__name__, failure)
        phases.append(phase_row)
    command = phases[-1]["command"] if phases else None
    if not failure and parameter_plan["parameters"]:
        try:
            application = verification_by_phase["parameters"]
            application["clones"] = verification_by_phase.get("clone", {"clones": []})["clones"]
            schematic_verification = merge_parameter_verification(
                application, verification_by_phase["baseline"], verification_by_phase["verify"],
            )
        except Exception as error:
            failure = error
            schematic_verification = {"ok": False, "parameters": [], "clones": []}
    else:
        schematic_verification = {"ok": not failure, "parameters": [], "clones": []}
    lock_wait_seconds = sum(float(row["command"].get("lock_wait_seconds", 0.0)) for row in phases)
    elapsed_seconds = sum(float(row["command"].get("elapsed_seconds", 0.0)) for row in phases)
    result = {
        "design_variable_changes": design_changes,
        "schematic_patch_plan": patch_plan,
        "schematic_patch_command": command,
        "schematic_patch_phases": phases,
        "schematic_verification": schematic_verification,
        "startup_lock_wait_seconds": lock_wait_seconds,
        "cadence_timing": {
            "process_count": len(phases), "elapsed_seconds": elapsed_seconds,
            "lock_wait_seconds": lock_wait_seconds,
            "active_seconds": max(0.0, elapsed_seconds - lock_wait_seconds),
        },
    }
    atomic_write_json(os.path.join(candidate_root, "candidate_patch.json"), result)
    if command and command.get("termination_failed"):
        error = ParameterApplicationInfrastructureFailure(
            "copied schematic patch/callback process cleanup could not be proven during %s"
            % command.get("phase", "unknown")
        )
        error.unsafe_to_optimize = True
        raise error
    if command and command.get("cancelled"):
        raise ParameterApplicationInfrastructureFailure(
            "copied schematic callback validation cancelled during %s"
            % command.get("phase", "unknown")
        )
    if command and command.get("timed_out"):
        raise ParameterApplicationInfrastructureFailure(
            "copied schematic patch/callback timed out during %s" % command.get("phase", "unknown")
        )
    if command and command["returncode"] != 0:
        raise ParameterApplicationFailure(
            "copied schematic patch/callback failed: %s" % json.dumps(command, sort_keys=True),
            code=adapter_failure_code(command), evidence={
                "phase": command.get("phase"),
                "returncode": command.get("returncode"),
            },
        )
    if failure:
        if isinstance(failure, ParameterApplicationFailure):
            raise failure
        raise ParameterApplicationFailure("copied schematic verification failed: %s" % failure)
    return result


def reset_results(workspace):
    removed = []
    setup_results = os.path.join(workspace["setup_dir"], "results")
    paths = [setup_results]
    storage = workspace.get("native_storage")
    if storage:
        for key in ("save_dir", "save_res_dir"):
            paths.append(os.path.join(storage[key], workspace["primary_library"], workspace["setup_cell"],
                                      workspace.get("setup_view") or "maestro", "results"))
    for test in workspace["setup"].get("tests", []):
        paths.append(os.path.join(
            workspace["simulation_root"], test["library"], test["cell"],
            workspace.get("setup_view") or "maestro", "results",
        ))
    for path in sorted(set(paths)):
        if os.path.isdir(path) and is_under(path, workspace["root"]):
            if os.path.islink(path) or not is_under(os.path.realpath(path), os.path.realpath(workspace["root"])):
                raise ValueError("private result reset path escapes its owned workspace")
            shutil.rmtree(path)
            removed.append(path)
    return removed


def _history_identity(workspace, candidate_root):
    candidate_root = os.path.abspath(candidate_root)
    attempt_name = os.path.basename(candidate_root)
    parent = os.path.dirname(candidate_root)
    if os.path.basename(parent) == "attempts":
        candidate_name = os.path.basename(os.path.dirname(parent))
    else:
        candidate_name = attempt_name
        attempt_name = "run"
    candidate_token = re.sub(r"[^A-Za-z0-9_]+", "_", candidate_name).strip("_") or "candidate"
    attempt_token = re.sub(r"[^A-Za-z0-9_]+", "_", attempt_name).strip("_") or "run"
    digest = hashlib.sha256((os.path.realpath(workspace["root"]) + "\0" + candidate_root).encode("utf-8")).hexdigest()[:10]
    history_name = "CRESCENDO_%s_%s_%s" % (candidate_token[:20], attempt_token[:12], digest)
    return {
        "schema_version": "crescendo.history_identity.v1",
        "history_name": history_name,
        "candidate_token": candidate_token,
        "attempt_token": attempt_token,
    }


def write_batch_files(workspace, config, history_name=None):
    root = workspace["root"]
    local_bin = ensure_dir(os.path.join(root, "bin"))
    from .simulators import selected_simulator
    simulator = selected_simulator(config)
    saved = set((row.get("simulator") or "spectre").lower() for row in (workspace.get("setup") or {}).get("tests", []) if row.get("enabled", True))
    if saved and saved != {simulator}:
        raise ValueError("Maestro saved simulator integration differs from selected " + simulator.upper() + "; select a matching native setup or Private Netlist with explicit model mapping")
    wrapper_path = os.path.join(local_bin, simulator)
    real_spectre = config["cadence"].get(simulator + "_bin")
    if not real_spectre:
        raise ValueError(simulator.upper() + " executable is missing")
    submit_script = config.get("runtime", {}).get(
        "spectre_submit_script"
    ) or submit_spectre_script_path()
    wrapper = '''#!/bin/sh
real_spectre="${CRESCENDO_REAL_SIMULATOR_BIN:-${CRESCENDO_REAL_SPECTRE_BIN:-}}"
submit_script="${CRESCENDO_SPECTRE_SUBMIT_SCRIPT:-}"
if [ -z "$real_spectre" ]; then real_spectre=%s; fi
if [ -z "$submit_script" ]; then submit_script=%s; fi
if [ ! -x "$submit_script" ]; then
  echo "Crescendo simulator submit script is missing or not executable: $submit_script" >&2
  exit 126
fi
case "${CRESCENDO_SIMULATION_HOME_SET-unrecorded}" in
  x) HOME=${CRESCENDO_SIMULATION_HOME_VALUE-}; export HOME ;;
  '') unset HOME ;;
  unrecorded) : ;;
  *) echo "Invalid Crescendo simulation HOME marker" >&2; exit 64 ;;
esac
unset CRESCENDO_SIMULATION_HOME_SET CRESCENDO_SIMULATION_HOME_VALUE
exec "$submit_script" "$real_spectre" "$@"
''' % (shlex.quote(str(real_spectre)), shlex.quote(str(submit_script)))
    atomic_write_text(wrapper_path, wrapper)
    os.chmod(wrapper_path, os.stat(wrapper_path).st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    setup_library = workspace["primary_library"]
    setup_cell = workspace["setup_cell"]
    ocn_path = os.path.join(root, "run_maestro.ocn")
    history_name = history_name or "CRESCENDO_batch"
    ocn_lines = [';; Crescendo isolated Maestro batch run.']
    ocn_lines.extend(_workspace_startup_load_lines(workspace))
    ocn_lines.extend([
        'session = maeOpenSetup("%s" "%s" "%s" ?mode "a")' % (
            _skill_quote(setup_library), _skill_quote(setup_cell),
            _skill_quote(workspace.get("setup_view") or "maestro"),
        ),
    ])
    ocn_lines.extend(["mainSDB = axlGetMainSetupDB(session)"])
    from .storage_policy import native_storage_readback_lines
    ocn_lines.extend(native_storage_readback_lines(workspace))
    ocn_lines.extend(workspace_test_design_lines(workspace))
    ocn_lines.extend([
        'runId = maeRunSimulation(?waitUntilDone t ?returnRunId t ?session session)',
        "maeWaitUntilDone('All ?session session)",
        'runData = axlGetRunData(session runId)',
        'axlSetHistoryName(runData "%s")' % _skill_quote(history_name),
        'unless(equal(axlGetHistoryName(runData) "%s") error("Crescendo history rename failed"))'
        % _skill_quote(history_name),
        'maeExportOutputView(?session session)',
        "maeWaitUntilDone('All ?session session)",
        'exit()',
    ])
    ocn = "\n".join(ocn_lines) + "\n"
    atomic_write_text(ocn_path, ocn)
    return {"spectre_wrapper": wrapper_path, "simulator_wrapper": wrapper_path, "execution_simulator": simulator, "ocean_script": ocn_path, "history_name": history_name}


def summarize_spectre_timing(path, workspace_root=None):
    result = {
        "log_present": os.path.isfile(path), "run_count": 0,
        "completed_run_count": 0, "total_seconds": 0.0,
        "span_seconds": 0.0, "runs": [],
    }
    if not result["log_present"]:
        return result
    pending_by_pid = {}
    pending_without_pid = []
    starts = []
    ends = []
    with open(path, "r") as stream:
        for raw in stream:
            start = re.match(r"^START epoch=([0-9.]+)(?: pid=([0-9]+))? cwd=(.*?) argv=", raw.rstrip("\n"))
            if start:
                row = {"started_epoch": float(start.group(1)), "pid": start.group(2), "cwd": start.group(3)}
                starts.append(row["started_epoch"])
                if row["pid"]:
                    pending_by_pid[row["pid"]] = row
                else:
                    pending_without_pid.append(row)
                continue
            end = re.match(r"^END epoch=([0-9.]+)(?: pid=([0-9]+))? returncode=(-?[0-9]+)", raw.rstrip("\n"))
            if not end:
                continue
            ended_epoch = float(end.group(1))
            pid = end.group(2)
            started = pending_by_pid.pop(pid, None) if pid else (
                pending_without_pid.pop(0) if pending_without_pid else None
            )
            if not started:
                continue
            cwd = started["cwd"]
            if workspace_root and is_under(cwd, workspace_root):
                cwd = os.path.relpath(cwd, workspace_root).replace(os.sep, "/")
            duration = max(0.0, ended_epoch - started["started_epoch"])
            result["runs"].append({
                "started_epoch": started["started_epoch"], "finished_epoch": ended_epoch,
                "elapsed_seconds": duration, "returncode": int(end.group(3)), "cwd": cwd,
            })
            ends.append(ended_epoch)
    result["run_count"] = len(starts)
    result["completed_run_count"] = len(result["runs"])
    result["total_seconds"] = sum(row["elapsed_seconds"] for row in result["runs"])
    if starts and ends:
        result["span_seconds"] = max(0.0, max(ends) - min(starts))
    return result


def run_maestro(workspace, config, candidate_root, timeout_seconds=None,
                retained_log_root=None):
    candidate_root = ensure_dir(candidate_root)
    reset_results(workspace)
    identity = _history_identity(workspace, candidate_root)
    atomic_write_json(os.path.join(candidate_root, "run_identity.json"), identity)
    batch = write_batch_files(workspace, config, history_name=identity["history_name"])
    env = child_environment(config, workspace["root"], include_runtime_ld=True, extra={
        "PATH": os.path.dirname(batch["spectre_wrapper"]) + ":" + child_environment(config, workspace["root"])["PATH"],
        "CRESCENDO_SIMULATION_TIMEOUT_SECONDS": config["runtime"].get("simulation_timeout_seconds", 900),
        "CRESCENDO_REAL_SIMULATOR_BIN": config["cadence"][batch["execution_simulator"] + "_bin"],
        "CRESCENDO_REAL_SPECTRE_BIN": config["cadence"][batch["execution_simulator"] + "_bin"],
        "CRESCENDO_EXECUTION_SIMULATOR": batch["execution_simulator"],
        "CRESCENDO_CADENCE_ROOT": config["cadence"].get("cadence_root") or "",
        "CRESCENDO_SPECTRE_TIMEOUT_LOG": os.path.join(candidate_root, "spectre_timeout.log"),
        "CRESCENDO_SPECTRE_RETAIN_ROOT": os.path.abspath(
            retained_log_root or candidate_root
        ),
        "CRESCENDO_SPECTRE_SUBMIT_SCRIPT": config["runtime"].get(
            "spectre_submit_script"
        ) or submit_spectre_script_path(),
    })
    candidate_timeout = int(timeout_seconds if timeout_seconds is not None else config["runtime"].get("candidate_timeout_seconds", 600))
    started_epoch = time.time()
    ocean_command = [config["cadence"]["ocean_bin"], "-nograph", "-restore", batch["ocean_script"]]
    guardian_script = guardian_script_path()
    guardian_state_path = os.path.join(candidate_root, "guardian_state.json")
    guardian_token = "legacy-%s-%s" % (identity["history_name"], uuid.uuid4().hex)
    guardian_stdout_path = os.path.join(candidate_root, "ocean.stdout")
    guardian_stderr_path = os.path.join(candidate_root, "ocean.stderr")
    launch_lock_fd, launch_lock_path = acquire_legacy_guardian_launch_lock(candidate_root)
    try:
        parent_identity = legacy_guardian_parent_identity()
        guardian_argv = build_guardian_command(
            ocean_command, workspace["root"], guardian_stdout_path,
            guardian_stderr_path, guardian_state_path, guardian_token,
            guardian_launch_lock_fd=launch_lock_fd,
        )
        guardian_manifest = write_legacy_guardian_manifest(
            candidate_root, workspace["root"], guardian_state_path,
            guardian_stdout_path, guardian_stderr_path,
            guardian_token, ocean_command, parent_identity["pid"], guardian_script,
            launch_lock_path=launch_lock_path,
            expected_parent_boot_id=parent_identity["boot_id"],
            expected_parent_start_ticks=parent_identity["start_ticks"],
            expected_uid=parent_identity["uid"], guardian_argv=guardian_argv,
        )
        command_launch_lock_fd = launch_lock_fd
        launch_lock_fd = None
        command = run_command(
            ocean_command,
            cwd=workspace["root"],
            env=env,
            timeout_seconds=candidate_timeout,
            stdout_path=guardian_stdout_path,
            stderr_path=guardian_stderr_path,
            startup_lock=CADENCE_STARTUP_LOCK,
            ready_path=os.path.join(candidate_root, "spectre_timeout.log"),
            startup_timeout_seconds=min(candidate_timeout, int(config["runtime"].get("ocean_startup_timeout_seconds", 90))),
            lock_timeout_seconds=config["runtime"].get("command_timeout_seconds", 1800),
            guardian_state_path=guardian_state_path,
            guardian_token=guardian_token,
            guardian_launch_lock_fd=command_launch_lock_fd,
        )
    finally:
        close_legacy_guardian_launch_lock(launch_lock_fd)
    command["execution_simulator"] = batch["execution_simulator"]
    command["started_epoch"] = started_epoch
    command["finished_epoch"] = time.time()
    command["history_name"] = identity["history_name"]
    command["history_identity"] = identity
    command["guardian_manifest_path"] = os.path.join(candidate_root, "legacy_guardian_launch.json")
    command["guardian_protocol"] = guardian_manifest["protocol"]
    command["spectre_submit_script"] = env["CRESCENDO_SPECTRE_SUBMIT_SCRIPT"]
    command["spectre_timing"] = summarize_spectre_timing(
        os.path.join(candidate_root, "spectre_timeout.log"), workspace["root"],
    )
    technology_manifest = workspace.get("technology") or {}
    if (
            command.get("returncode") == 0 and not command.get("timed_out")
            and workspace_technology_validation_required(technology_manifest)):
        try:
            command["technology_validation"] = validate_workspace_technology(
                technology_manifest, require_complete=True,
            )
        except TechnologyBindingError as error:
            command["technology_validation"] = {
                "ok": False, "status": "failed", "error": str(error),
            }
            atomic_write_json(os.path.join(candidate_root, "maestro_command.json"), command)
            raise ParameterApplicationInfrastructureFailure(str(error))
    atomic_write_json(os.path.join(candidate_root, "maestro_command.json"), command)
    return command


def _candidate_netlists(workspace, history_name=None):
    paths = []
    history_fragment = None
    if history_name:
        results_kind = "data" if workspace.get("setup_backend") == "adexl" else "maestro"
        history_fragment = "/results/%s/%s/" % (
            results_kind, str(history_name).replace(os.sep, "/"),
        )
    for current, _directories, files in os.walk(workspace["simulation_root"]):
        if "netlist" not in files or os.path.basename(current) != "netlist":
            continue
        path = os.path.join(current, "netlist")
        normalized = path.replace(os.sep, "/")
        if "/ihnl/" in normalized or "/.tmpADEDir_" in normalized:
            continue
        if history_fragment and history_fragment not in normalized:
            continue
        marker = history_fragment
        if marker is None:
            match = re.search(r"/results/maestro/[^/]+/", normalized)
            marker = match.group(0) if match else None
        if not marker or marker not in normalized:
            continue
        relative = normalized.split(marker, 1)[1]
        components = relative.split("/")
        legacy_psf = len(components) >= 4 and components[0] == "psf"
        point_result = (
            len(components) == 4 and components[0].isdigit()
            and components[-2:] == ["netlist", "netlist"]
        )
        if not legacy_psf and not point_result:
            continue
        paths.append(os.path.abspath(path))
    return sorted(set(paths))


def _spectre_sections(path):
    sections = {}
    library = None
    cell = None
    view = None
    statement = ""

    def append_statement(text):
        if library and cell and text.strip():
            sections.setdefault((library, cell), []).append(text.strip())
            if view:
                sections.setdefault((library, cell, view), []).append(text.strip())

    with open(path, "r") as stream:
        for raw in stream:
            line = raw.rstrip("\n")
            library_match = re.match(r"^//\s*Library name:\s*(.+?)\s*$", line)
            if library_match:
                append_statement(statement)
                statement = ""
                library = library_match.group(1)
                cell = None
                view = None
                continue
            cell_match = re.match(r"^//\s*Cell name:\s*(.+?)\s*$", line)
            if cell_match:
                append_statement(statement)
                statement = ""
                cell = cell_match.group(1)
                view = None
                continue
            view_match = re.match(r"^//\s*View name:\s*(.+?)\s*$", line)
            if view_match:
                append_statement(statement)
                statement = ""
                view = view_match.group(1)
                continue
            if line.startswith("//") or not line.strip():
                append_statement(statement)
                statement = ""
                continue
            stripped = line.strip()
            if statement:
                statement += " " + stripped
            else:
                statement = stripped
            if statement.endswith("\\"):
                statement = statement[:-1].rstrip()
            else:
                append_statement(statement)
                statement = ""
    append_statement(statement)
    return sections


def _netlist_instance(sections, library, cell, instance, view=None):
    section = sections.get((library, cell, view)) if view is not None else sections.get((library, cell))
    if section is None:
        raise ParameterApplicationFailure("netlist is missing cell section %s/%s" % (library, cell))
    instance_pattern = re.compile(r"^%s\s+\(" % re.escape(str(instance)))
    statements = [row for row in section if instance_pattern.match(row)]
    if len(statements) != 1:
        raise ParameterApplicationFailure(
            "netlist instance mapping is not unique for %s/%s/%s: %d" % (library, cell, instance, len(statements))
        )
    assignment_pattern = re.compile(r"(?<!\S)([A-Za-z_][A-Za-z0-9_]*)=(\"[^\"]*\"|[^\s]+)")
    assignments = {}
    for name, value in assignment_pattern.findall(statements[0]):
        assignments.setdefault(name, []).append(value.strip('"'))
    return assignments, statements[0]


def _effective_netlist_value(sections, library, cell, instance, property_name):
    assignments, statement = _netlist_instance(sections, library, cell, instance)
    matches = assignments.get(property_name, [])
    if len(matches) != 1:
        raise ParameterApplicationFailure(
            "effective netlist parameter mapping is not unique for %s/%s/%s/%s: %d" % (
                library, cell, instance, property_name, len(matches),
            )
        )
    return matches[0], statement


def _effective_property_names(row):
    names = [row["property"]]
    master_library = str(row.get("master_library") or "").lower()
    master_cell = str(row.get("master_cell") or "").lower()
    property_name = str(row.get("property") or "").lower()
    if master_library == "analoglib" and master_cell in ("vdc", "idc") and property_name in ("vdc", "idc"):
        names.append("dc")
    for name in row.get("effective_aliases") or []:
        if name not in names:
            names.append(name)
    return names


def verify_parameter_application(workspace, patch_result, candidate_root, history_name=None):
    schematic = patch_result.get("schematic_verification") or {}
    schematic_rows = schematic.get("parameters") or []
    if not schematic_rows:
        result = {"ok": True, "parameters": [], "netlists": []}
        atomic_write_json(os.path.join(candidate_root, "parameter_application.json"), result)
        return result
    netlist_paths = _candidate_netlists(workspace, history_name=history_name)
    if not netlist_paths:
        raise ParameterApplicationInfrastructureFailure(
            "ADE simulation completed without a final Spectre netlist for parameter verification"
        )
    parsed_netlists = [(path, _spectre_sections(path)) for path in netlist_paths]
    verified = []
    for row in schematic_rows:
        relevant = [
            (path, sections) for path, sections in parsed_netlists
            if (row.get("netlist_design_library") or row["design_library"], row.get("netlist_design_cell") or row["design_cell"]) in sections
            and (workspace.get("copy_policy") != "same_library_views" or
                 (row["library"], row["cell"], row.get("view")) in sections)
        ]
        if not relevant:
            raise ParameterApplicationFailure(
                "no generated netlist corresponds to design %s/%s for %s" % (
                    row["design_library"], row["design_cell"], row["parameter_id"],
                )
            )
        effective_rows = []
        for path, sections in relevant:
            assignments, statement = _netlist_instance(
                sections, row["library"], row["cell"], row["instance"],
                view=row.get("view") if workspace.get("copy_policy") == "same_library_views" else None,
            )
            effective_names = _effective_property_names(row)
            direct_matches = [
                (name, value)
                for name in effective_names
                for value in assignments.get(name, [])
            ]
            if len(direct_matches) > 1:
                raise ParameterApplicationFailure(
                    "effective netlist parameter mapping is ambiguous for %s in %s" % (row["parameter_id"], path)
                )
            if direct_matches:
                effective_name, effective_value = direct_matches[0]
                expected_value = row.get("persisted_cdf_value")
                if expected_value in (None, ""):
                    expected_value = row["requested"]
                if not _physical_values_match(
                    expected_value, effective_value,
                    relative_tolerance=1.0e-8, allow_display_rounding=True,
                ):
                    raise ParameterApplicationFailure(
                        "effective netlist mismatch for %s: persisted_cdf=%r effective=%r netlist=%s" % (
                            row["parameter_id"], expected_value, effective_value, path,
                        )
                    )
                effective_rows.append({
                    "netlist": path,
                    "mode": "direct" if effective_name == row["property"] else "direct_alias",
                    "parameter": effective_name,
                    "value": effective_value,
                    "statement": statement,
                })
                continue
            if int(row.get("instance_selected_count") or 0) != 1:
                raise ParameterApplicationFailure(
                    "requested CDF parameter %s is absent from the netlist and generic CDF mode cannot attribute derived changes when multiple parameters on the same instance are selected; use a PDK adapter with explicit effective aliases" % row["parameter_id"],
                    code="cdf_parameter_mapping_incompatible",
                )
            evidence = []
            declared = row.get("netlist_parameters") or {}
            before = row.get("netlist_parameters_before") or {}
            cached_names = set(str(name) for name in row.get("derived_parameter_names") or [])
            for name, cdf_values in sorted(declared.items()):
                netlist_values = assignments.get(name, [])
                before_values = before.get(name, [])
                if len(cdf_values) != 1 or len(netlist_values) != 1:
                    continue
                changed = (
                    len(before_values) == 1 and
                    not _physical_values_match(before_values[0], cdf_values[0], relative_tolerance=1.0e-12)
                )
                if not changed and name not in cached_names:
                    continue
                if parse_physical_number(cdf_values[0]) is None or parse_physical_number(netlist_values[0]) is None:
                    continue
                if _physical_values_match(cdf_values[0], netlist_values[0], relative_tolerance=1.0e-8):
                    evidence.append({
                        "name": name, "before_cdf_value": before_values[0] if len(before_values) == 1 else None,
                        "cdf_value": cdf_values[0], "netlist_value": netlist_values[0],
                        "mapping_source": "cached" if name in cached_names and not changed else "callback_delta",
                    })
            if not evidence:
                raise ParameterApplicationFailure(
                "requested CDF parameter %s is absent from the netlist and caused no uniquely attributable CDF callback change present in the final Spectre netlist: %s" % (
                        row["parameter_id"], path,
                    ), code="netlist_effect_unverified",
                )
            effective_rows.append({
                "netlist": path, "mode": "cdf_callback_derived", "value": None,
                "evidence": evidence, "statement": statement,
            })
        verified.append(dict(row, effective_netlist=effective_rows))
    result = {"ok": True, "parameters": verified, "netlists": netlist_paths}
    snapshot_path = patch_result.get("export_snapshot_path") or os.path.join(candidate_root, "schematic_application.records.export.tsv")
    if os.path.isfile(snapshot_path):
        from .optimized_schematic import parse_export_snapshot
        result["export_snapshot"] = parse_export_snapshot(snapshot_path)
    atomic_write_json(os.path.join(candidate_root, "parameter_application.json"), result)
    return result
