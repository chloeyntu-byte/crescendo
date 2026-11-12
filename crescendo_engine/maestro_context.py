"""Bind one Crescendo window to the ADE window that invoked its native menu.

Native records locate saved setup and registered histories. They do not certify
simulation success, and captured runtime variables only enter EDA children.
"""
from __future__ import print_function

import copy
import hashlib
import json
import os
import re
import shutil
import stat
import tempfile
import uuid

from crescendo_engine.runtime import bind_text
_contract_text = bind_text()

from .ade_setup import resolve_setup_locator
from .cadence import (CADENCE_BINARY_RELATIVES, _first_existing,
                      _root_from_binary, discover_eda_environment)
from .utils import atomic_write_json, cds_lib_token, ensure_dir
from .runtime_layout import design_storage_root, project_directory
from .native_environment import (
    NATIVE_ENV_NAMES, NATIVE_ENV_SCHEMA, native_snapshot_warnings,
    restore_native_environment, validate_native_values,
)

SCHEMA = _contract_text("CRESCENDO_MAESTRO_CONTEXT_1")
MAX_CONTEXT_BYTES = 1024 * 1024
MAX_FIELD_CHARS = 16384
# makeTempFileName keeps only a short basename prefix on Cadence 6.1.8.
NATIVE_TEMP_PREFIXES = (_contract_text("cr_ctx_"), _contract_text("cr_maestro_"), _contract_text("cr_maes"))
RUNTIME_ENV_NAMES = frozenset(NATIVE_ENV_NAMES)
_TOKEN = re.compile(r'\s*("(?:[^"\\\r\n]|\\.)*"|nil|t|-?[0-9]+)(?=\s|$)')


def _fields(line):
    fields, pos = [], 0
    while line[pos:].strip():
        match = _TOKEN.match(line, pos)
        if not match:
            raise ValueError("Invalid quoted Maestro context field")
        token = match.group(1)
        fields.append(None if token == "nil" else True if token == "t" else json.loads(token))
        pos = match.end()
    return fields


def _text(value, label, optional=False):
    if value is None and optional:
        return None
    if not isinstance(value, str) or (not value and not optional):
        raise ValueError("Maestro context %s must be a string" % label)
    if len(value) > MAX_FIELD_CHARS or any(ord(char) < 32 for char in value):
        raise ValueError("Maestro context %s contains control characters or is too long" % label)
    return value or None


def _identifier(value, label):
    value = _text(value, label)
    if value in (".", "..") or any(char in value for char in '/\\\"'):
        raise ValueError("Invalid Maestro %s identity" % label)
    return value


def _absolute(value, label, work_dir=None, optional=False, resolve=True):
    value = _text(value, label, optional=optional)
    if value is None:
        return None
    if not os.path.isabs(value):
        if not work_dir:
            raise ValueError("Maestro context %s must be absolute" % label)
        value = os.path.join(work_dir, value)
    # Cadence executable aliases depend on their invocation name (argv[0]).
    return os.path.realpath(value) if resolve else os.path.abspath(value)


def parse_launch_records(raw):
    """Parse bounded SKILL %L records without eval or environment expansion."""
    if len(raw) > MAX_CONTEXT_BYTES:
        raise ValueError("Maestro launch context exceeds its size limit")
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    lines = raw.splitlines()
    if not lines or lines[0] != SCHEMA:
        raise ValueError("Unsupported Maestro launch context schema")
    result = {"schema": SCHEMA, "histories": [], "libraries": {}, "live_tests": [], "runtime_env": {}}
    seen, history_names, test_names, policy_records = set(), set(), set(), []
    storage_records = []
    for line in lines[1:]:
        if not line.strip():
            continue
        tag, separator, body = line.partition("\t")
        if not separator:
            raise ValueError("Invalid Maestro context record")
        fields = _fields(body)
        sizes = {"C": (7,), "R": (1,), "E": (2,), "H": (3,), "L": (2, 3), "T": (5,), "S": (1,), "W": (1,), "M": (1,), "P": (3,), "J": (4,), "O": (3, 4), "D": (2,), "Q": (1,), "U": (3,), "V": (3,)}
        if tag not in sizes or len(fields) not in sizes[tag]:
            raise ValueError("Unsupported Maestro context record %s" % tag)
        if tag in ("C", "R", "S", "W", "M"):
            if tag in seen:
                raise ValueError("Duplicate Maestro context record %s" % tag)
            seen.add(tag)
        if tag == "V":
            storage_records.append((tag, fields))
        elif tag in ("P", "J", "D", "O", "Q", "U"):
            policy_records.append((tag, fields))
        elif tag == "C":
            result["source"] = dict(zip(("library", "cell", "view", "work_dir", "view_dir", "cds_lib", "ocean"), fields))
        elif tag in ("R", "E"):
            key, value = ("LD_LIBRARY_PATH", fields[0]) if tag == "R" else fields
            if key not in RUNTIME_ENV_NAMES or key in result["runtime_env"]:
                raise ValueError("Unsupported or duplicate Maestro runtime variable")
            # Environment fields preserve nil (unset) separately from "".
            result["runtime_env"][key] = validate_native_values({key: value})[key]
        elif tag == "H":
            name = _text(fields[0], "history name")
            if name in history_names or len(history_names) >= 512:
                raise ValueError("Duplicate or excessive Maestro histories")
            history_names.add(name)
            result["histories"].append(dict(zip(("name", "results_dir", "rdb_file"), fields)))
        elif tag == "L":
            name = _identifier(fields[0], "library")
            if name in result["libraries"] or len(result["libraries"]) >= 4096:
                raise ValueError("Duplicate or excessive native library mappings")
            result["libraries"][name] = fields[1]
        elif tag == "T":
            name = _text(fields[0], "test name")
            if name in test_names or len(test_names) >= 4096:
                raise ValueError("Duplicate or excessive native tests")
            test_names.add(name)
            result["live_tests"].append(dict(zip(("name", "library", "cell", "view", "simulator"), fields)))
        elif tag == "S":
            result["selected_history"] = _text(fields[0], "selected history", optional=True)
        elif tag == "W":
            if fields[0] is not None and (isinstance(fields[0], bool) or not isinstance(fields[0], int)):
                raise ValueError("Invalid native window number")
            result["window_number"] = fields[0]
        elif tag == "M":
            value = fields[0]
            if value is not True and value is not None and value != "unknown":
                raise ValueError("Invalid Maestro setup modified flag")
            result["modified"] = None if value == "unknown" else value is True
    if "C" not in seen:
        raise ValueError("Maestro context has no source identity")
    source = result["source"]
    for key in ("library", "cell", "view"):
        source[key] = _identifier(source[key], key)
    source["work_dir"] = _absolute(source["work_dir"], "work_dir")
    work_dir = source["work_dir"]
    source["view_dir"] = _absolute(source["view_dir"], "view_dir", work_dir)
    locator = resolve_setup_locator(source["view_dir"], backend="auto")
    source.update(setup_path=os.path.realpath(locator["path"]), view_dir=os.path.realpath(locator["view_dir"]),
                  library_root=os.path.realpath(locator["library_root"]), backend=locator["backend"],
                  modified=result.pop("modified", None))
    # Tie launch-time effective policies to the saved setup used by this window.
    # A later Reload after saving new Job Setup values must not replay the old
    # invocation policy merely because the LCV/path still matches.
    from .utils import sha256_file
    try:
        source["setup_content_sha256"] = sha256_file(source["setup_path"])
    except OSError:
        source["setup_content_sha256"] = None
    state_path = os.path.join(os.path.dirname(source["setup_path"]), "active.state")
    source["active_state_path"] = os.path.realpath(state_path)
    source["active_state_sha256"] = sha256_file(state_path) if os.path.isfile(state_path) else None
    if source["cell"] != locator["cell_name"] or source["view"] != locator["view_name"]:
        raise ValueError("Native Maestro identity does not match the saved view path")
    source["cds_lib"] = _absolute(source.get("cds_lib"), "cds_lib", work_dir, optional=True)
    source["ocean"] = _absolute(source["ocean"], "ocean", work_dir, resolve=False)
    result["libraries"] = dict((name, _absolute(path, "library path", work_dir))
                               for name, path in result["libraries"].items())
    mapped_root = result["libraries"].get(source["library"])
    if mapped_root and mapped_root != source["library_root"]:
        raise ValueError("Invoking Maestro view conflicts with its loaded library mapping")
    # Explicit native identity remains valid when logical name != directory name.
    result["libraries"][source["library"]] = source["library_root"]
    for row in result["histories"]:
        for key in ("results_dir", "rdb_file"):
            row[key] = _absolute(row[key], key, work_dir, optional=True)
        if row["rdb_file"] and not row["rdb_file"].lower().endswith(".rdb"):
            raise ValueError("Native history does not identify an RDB file")
    for row in result["live_tests"]:
        for key in ("library", "cell", "view", "simulator"):
            row[key] = _text(row[key], "test %s" % key, optional=True)
    if policy_records:
        from .job_policy import policy_from_records
        result["job_policy"] = policy_from_records(policy_records)
    if storage_records:
        from .storage_policy import save_options_from_records
        result["save_options"] = save_options_from_records(storage_records, identity=source)
    result["runtime_env_schema"] = NATIVE_ENV_SCHEMA
    result["runtime_env_warnings"] = native_snapshot_warnings(result["runtime_env"])
    return result


def _cds_path(path):
    return cds_lib_token(path)


def _write_immutable_mapping(path, contents):
    """Publish once so another UI cannot invalidate an unchanged prepared map."""
    descriptor, temporary = tempfile.mkstemp(prefix="mapping_", dir=os.path.dirname(path))
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(contents)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            if os.path.islink(path):
                raise ValueError("Plugin mapping must not be a symbolic link")
            with open(path, encoding="utf-8") as stream:
                if stream.read(MAX_CONTEXT_BYTES + 1) != contents:
                    raise ValueError("Existing plugin mapping differs from its source namespace")
    finally:
        os.unlink(temporary)


def _publish_mapping(design_root, contents):
    digest = hashlib.sha256(contents.encode('utf-8')).hexdigest()
    parent = ensure_dir(os.path.join(design_root, 'cache', 'mappings', digest))
    path = os.path.join(parent, 'cds.lib')
    _write_immutable_mapping(path, contents)
    return path


def load_launch_context(path, consume=True, storage_base_root=None):
    """Persist an independent private snapshot before consuming native temp input."""
    path = os.path.abspath(path)
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    with os.fdopen(descriptor, "rb") as stream:
        identity = os.fstat(stream.fileno())
        if not stat.S_ISREG(identity.st_mode) or identity.st_uid != os.getuid():
            raise ValueError("Launch context must be a regular file owned by this user")
        native_temp = (os.path.basename(path).startswith(NATIVE_TEMP_PREFIXES) and
                       os.path.realpath(os.path.dirname(path)) == os.path.realpath(tempfile.gettempdir()))
        if native_temp:
            os.fchmod(stream.fileno(), 0o600)
        raw = stream.read(MAX_CONTEXT_BYTES + 1)
    context = parse_launch_records(raw)
    # Persistent mapping/settings/results are named by TB in its working
    # directory. Each launch only adds an isolated, explicitly temporary probe.
    override = os.environ.get(_contract_text("CRESCENDO_RUN_ROOT")) or storage_base_root
    design_dir = design_storage_root(context["source"], context["libraries"], base_root=override)
    os.makedirs(design_dir, mode=0o700, exist_ok=True)
    parent = ensure_dir(os.path.join(design_dir, "cache", "probes"))
    context_dir = tempfile.mkdtemp(prefix="launch_", dir=parent)
    from .scratch import register_scratch
    context["scratch_ownership"] = register_scratch(context_dir, kind="launch_context")
    context["launch_id"] = uuid.uuid4().hex
    context["context_path"] = os.path.join(context_dir, _contract_text("launch_context.json"))
    context["state_path"] = os.path.join(context_dir, _contract_text("launch_state.json"))
    # Same source + loaded map keeps one runtime for recent runs and resume.
    # Different real paths remain distinct even when logical names are equal.
    context["design_root"] = design_dir
    context["storage_base_override"] = os.path.realpath(os.path.abspath(override)) if override else None
    context["settings_root"] = ensure_dir(os.path.join(design_dir, "states"))
    context["cache_root"] = ensure_dir(os.path.join(design_dir, "cache"))
    from .storage_policy import resolve_storage_policy
    context["storage_policy"] = resolve_storage_policy({"_maestro_launch_context": context, "runtime": {"storage_base_override": override}})
    context["runtime_root"] = context["storage_policy"]["run_root"]
    context["probe_root"] = os.path.join(context_dir, "probes")
    context["legacy_design_root"] = context["storage_policy"]["legacy_design_root"]
    lines = []
    source_cds = context["source"].get("cds_lib")
    if source_cds and os.path.isfile(source_cds):
        lines.append("INCLUDE %s" % _cds_path(source_cds))
    lines.extend("DEFINE %s %s" % (_cds_path(name), _cds_path(root))
                 for name, root in sorted(context["libraries"].items()))
    contents = "\n".join(lines) + "\n"
    context["effective_cds_lib"] = _publish_mapping(design_dir, contents)
    atomic_write_json(context["context_path"], context)
    # Never unlink a source setup/RDB or a user-supplied arbitrary file.
    if consume and os.path.basename(path).startswith(NATIVE_TEMP_PREFIXES) and os.path.realpath(os.path.dirname(path)) == os.path.realpath(tempfile.gettempdir()):
        current = os.lstat(path)
        if (current.st_ino, current.st_dev) == (identity.st_ino, identity.st_dev) and stat.S_ISREG(current.st_mode):
            os.unlink(path)
    return context


def validate_bound_setup(context, path):
    source = context.get("source") or {}
    if source.get("modified") is True:
        raise ValueError(_contract_text("The invoking Maestro has unsaved changes. Save the setup in Maestro and reopen Crescendo before preparing or running."))
    locator = resolve_setup_locator(path, backend=source.get("backend") or "auto")
    if os.path.realpath(locator["path"]) != os.path.realpath(source.get("setup_path") or ""):
        raise ValueError(_contract_text("This Crescendo window is bound to %s/%s/%s. Select another saved setup with Browse and Load and Inspect, or open Crescendo from its Maestro.") % (
            source.get("library"), source.get("cell"), source.get("view")))
    return locator["path"]


def refresh_saved_launch_context(context, path):
    """Rebind changed saved content without recapturing the invoking toolchain.

    A manual reload is authority to reread the same saved LCV, not to attach
    this window to a different library or to treat unsaved live edits as saved.
    """
    from .ade_setup import inspect_setup
    from .utils import sha256_file
    source = context.get("source") or {}
    locator = resolve_setup_locator(path, backend=source.get("backend") or "auto")
    canonical = os.path.realpath(locator["path"])
    if (canonical != os.path.realpath(source.get("setup_path") or "") or
            source.get("cell") != locator["cell_name"] or source.get("view") != locator["view_name"] or
            source.get("library_root") and os.path.realpath(source["library_root"]) != os.path.realpath(locator["library_root"])):
        raise ValueError("Saved setup reload does not match the invoking Maestro LCV/path.")
    state_path = os.path.join(os.path.dirname(canonical), "active.state")
    def fingerprint():
        return {"setup_content_sha256": sha256_file(canonical),
                "active_state_path": os.path.realpath(state_path),
                "active_state_sha256": sha256_file(state_path) if os.path.isfile(state_path) else None}
    current = fingerprint()
    changed = bool(source.get("setup_content_sha256") and
                   source["setup_content_sha256"] != current["setup_content_sha256"])
    changed = changed or ("active_state_sha256" in source and
                          source.get("active_state_sha256") != current["active_state_sha256"])
    if not changed:
        validate_bound_setup(context, canonical)
        return copy.deepcopy(context)
    setup = inspect_setup(canonical, backend=locator["backend"])
    if current != fingerprint():
        raise ValueError("ADE setup changed while reloading saved metadata; Load and Inspect again.")
    result = copy.deepcopy(context)
    result["source"].update(current, modified=False)
    result["live_tests"] = [{key: row.get(key) for key in ("name", "library", "cell", "view", "simulator")}
                            for row in setup.get("tests") or []]
    # These belonged to the original session snapshot. The existing read-only
    # saved-setup query supplies current policy and Save Options instead.
    result.pop("job_policy", None)
    result.pop("save_options", None)
    previous_root = ((context.get("save_options") or {}).get("effective") or {}).get("simulation_root")
    if not previous_root:
        previous_root = (context.get("saved_setup_refresh") or {}).get("previous_simulation_root_hint")
    result["saved_setup_refresh"] = {"source": "explicit_saved_setup_reload", "fingerprint": current,
                                    "previous_simulation_root_hint": previous_root}
    return result


def selected_history_record(context):
    histories = context.get("histories") or []
    selected = context.get("selected_history")
    if selected:
        return next((copy.deepcopy(row) for row in histories if row["name"] == selected),
                    {"name": selected, "results_dir": None, "rdb_file": None})
    for row in reversed(histories):
        path = row.get("rdb_file")
        if path and os.path.isfile(path) and os.access(path, os.R_OK):
            return copy.deepcopy(row)
    return None


def selected_setup_config(origin_config, current_config, locator):
    """Configure an explicitly selected saved setup without recapturing GUI env.

    The invoking toolchain remains authoritative. A loaded native map is useful
    only when it contains the selected library; another workspace gets its own
    cds.lib and immutable working mapping, rather than the invoking namespace.
    """
    from .cadence import nearest_cds_lib
    from .config import DEFAULTS, PDK_KEYS, deep_merge
    from .ade_setup import inspect_setup
    from .pdk import parse_cds_lib

    result = copy.deepcopy(origin_config)
    cadence = result.setdefault("cadence", {})
    current_cadence = current_config.get("cadence") or {}
    origin_context = origin_config.get("_maestro_launch_context") or {}
    native_values = cadence.get("launch_environment")
    environment = restore_native_environment(os.environ, native_values)
    manual = copy.deepcopy(current_cadence.get("_manual_overrides") or {})
    file_overrides = copy.deepcopy(current_cadence.get("_file_overrides") or {})
    explicit = deep_merge(manual, file_overrides)
    for key in PDK_KEYS + ("search_roots",):
        cadence[key] = copy.deepcopy(explicit.get(key, DEFAULTS["cadence"].get(key)))
    for key, values in (("_manual_overrides", manual), ("_file_overrides", file_overrides)):
        if key in current_cadence:
            cadence[key] = values

    library_root = os.path.realpath(locator["library_root"])
    loaded = origin_context.get("libraries") or {}
    matching = sorted(name for name, root in loaded.items()
                      if os.path.realpath(root) == library_root)
    explicit_cds = explicit.get("cds_lib")
    if matching and not explicit_cds:
        libraries = copy.deepcopy(loaded)
        cadence["_native_library_mappings"] = copy.deepcopy(loaded)
        source_cds = (origin_context.get("source") or {}).get("cds_lib")
        mapping_source = "invoking Maestro loaded mapping"
    else:
        cadence.pop("_native_library_mappings", None)
        source_cds = explicit_cds or nearest_cds_lib(library_root)
        if not source_cds or not os.path.isfile(source_cds):
            raise ValueError("Selected setup has no readable workspace cds.lib; select its workspace mapping in EDA Settings.")
        parsed = parse_cds_lib(source_cds, environment=environment)
        libraries = parsed.get("definitions") or {}
        matching = sorted(name for name, root in libraries.items()
                          if os.path.realpath(root) == library_root)
        mapping_source = "explicit cds.lib override" if explicit_cds else "selected setup workspace cds.lib"
    setup = inspect_setup(locator["path"], backend=locator["backend"])
    required = set(row.get("library") for row in setup.get("tests") or []
                   if row.get("library"))
    missing = sorted(name for name in required
                     if not libraries.get(name) or not os.path.isdir(libraries[name]))
    if missing:
        raise ValueError("Selected setup mapping has no readable design libraries: %s" % ", ".join(missing))
    source = {
        "library": matching[0] if matching else locator["library_name"],
        "cell": locator["cell_name"], "view": locator["view_name"],
        "view_dir": os.path.realpath(locator["view_dir"]),
        "setup_path": os.path.realpath(locator["path"]),
        "library_root": library_root, "backend": locator["backend"],
        "work_dir": os.path.dirname(os.path.abspath(source_cds)) if source_cds else os.path.dirname(library_root),
        "cds_lib": os.path.abspath(source_cds) if source_cds else None,
        "modified": None,
    }
    origin_runtime = origin_config.get("runtime") or {}
    explicit_base = origin_runtime.get("storage_base_override")
    namespace_root = design_storage_root(source, libraries, base_root=explicit_base)
    os.makedirs(namespace_root, mode=0o700, exist_ok=True)
    lines = []
    if source_cds and os.path.isfile(source_cds):
        lines.append("INCLUDE %s" % _cds_path(os.path.abspath(source_cds)))
    lines.extend("DEFINE %s %s" % (_cds_path(name), _cds_path(root))
                 for name, root in sorted(libraries.items()))
    mapping = _publish_mapping(namespace_root, "\n".join(lines) + "\n")
    cadence["cds_lib"] = mapping
    cadence["source_cds_lib_override"] = mapping
    cadence["discovery"] = copy.deepcopy(cadence.get("discovery") or {})
    cadence["discovery"].setdefault("effective", {}).update(cds_lib=mapping)
    cadence["discovery"].setdefault("sources", {})["cds_lib"] = mapping_source
    for check in cadence["discovery"].get("checks") or []:
        if check.get("name") == "cds_lib":
            check.update(value=mapping, ok=True)
    result.setdefault("runtime", {})["run_root"] = os.path.join(namespace_root, "runs")
    probe_parent = ensure_dir(os.path.join(namespace_root, "cache", "probes"))
    result["runtime"]["hierarchy_probe_root"] = tempfile.mkdtemp(prefix="browse_", dir=probe_parent)
    from .scratch import register_scratch
    result["runtime"]["probe_scratch_ownership"] = register_scratch(result["runtime"]["hierarchy_probe_root"], kind="browse_probe")
    result["runtime"]["design_root"] = namespace_root
    result["runtime"]["storage_namespace_override"] = bool(explicit_base)
    result["runtime"]["settings_root"] = ensure_dir(os.path.join(namespace_root, "states"))
    result["runtime"]["cache_root"] = ensure_dir(os.path.join(namespace_root, "cache"))
    cadence["user_config_path"] = os.path.join(result["runtime"]["settings_root"], _contract_text("eda.json"))
    cadence["project_dir"] = source["work_dir"]
    result["_maestro_launch_context"] = None
    result["_selected_setup_path"] = source["setup_path"]
    result["_selected_setup_source"] = source
    from .storage_policy import resolve_storage_policy
    policy = resolve_storage_policy(result, setup, source, libraries)
    result["runtime"]["storage_policy"] = policy
    result["runtime"]["run_root"] = policy["run_root"]
    result["runtime"]["run_root_origin"] = "derived"
    return result


def bind_launch_config(config, context):
    result = copy.deepcopy(config)
    cadence = result.setdefault("cadence", {})
    source = context["source"]
    captured = validate_native_values(context.get("runtime_env") or {})
    environ = restore_native_environment(os.environ, captured)
    overrides = copy.deepcopy(cadence)
    explicit_spectre = {}
    # Effective discovery values alone are not user choices. Preserve only
    # declarations, including a bad binary that initial discovery made None.
    # Configuration-file values retain their existing priority over UI values.
    for origin in ("_manual_overrides", "_file_overrides"):
        declared = cadence.get(origin) or {}
        for key in ("spectre_bin", "spectre_root"):
            value = declared.get(key)
            if value not in (None, "", []):
                explicit_spectre[key] = copy.deepcopy(value)
    overrides.update(explicit_spectre)
    explicit_alps = {}
    for origin in ("_manual_overrides", "_file_overrides"):
        for key in ("alps_bin", "alps_root"):
            value = (cadence.get(origin) or {}).get(key)
            if value not in (None, "", []):
                explicit_alps[key] = copy.deepcopy(value)
    # Initial automatic values can belong to another environment. Rebuild
    # ALPS from declarations and this Maestro's PATH instead of promoting a
    # cached root/binary into an explicit installation selection.
    for key in ("alps_bin", "alps_root"):
        overrides.pop(key, None)
    overrides.update(explicit_alps)
    ocean = source["ocean"]
    if not os.path.isfile(ocean) or not os.access(ocean, os.X_OK):
        raise ValueError("Invoking Maestro Ocean executable is unavailable: %s" % ocean)
    native_root = _root_from_binary(ocean, ("share/cdssetup/cds.lib", "tools.lnx86/dfII")) or os.path.dirname(os.path.dirname(ocean))
    virtuoso = os.path.join(os.path.dirname(ocean), "virtuoso")
    if not os.path.isfile(virtuoso) or not os.access(virtuoso, os.X_OK):
        virtuoso = _first_existing(native_root, CADENCE_BINARY_RELATIVES["virtuoso_bin"], executable=True)
    if not virtuoso:
        raise ValueError("Cannot locate Virtuoso belonging to the invoking Maestro")
    overrides.update(ocean_bin=ocean, cadence_root=native_root,
                     virtuoso_bin=virtuoso,
                     cds_lib=context["effective_cds_lib"],
                     extra_binary_paths=[value for value in (captured.get("PATH") or "").split(os.pathsep) if value],
                     extra_library_paths=[value for value in (captured.get("LD_LIBRARY_PATH") or "").split(os.pathsep) if value])
    native_spectre = shutil.which("spectre", path=environ.get("PATH", os.defpath))
    if native_spectre and not explicit_spectre:
        overrides.update(spectre_bin=native_spectre, spectre_root=
                         _root_from_binary(native_spectre, ("tools.lnx86/spectre", "tools.lnx86")) or os.path.dirname(os.path.dirname(native_spectre)))
    native_alps = None
    if not explicit_alps and not (environ.get(_contract_text("CRESCENDO_ALPS_BIN")) or environ.get(_contract_text("CRESCENDO_ALPS_ROOT")) or environ.get("ALPS_HOME")):
        native_alps = shutil.which("alps", path=environ.get("PATH", os.defpath))
        if native_alps:
            parent = os.path.dirname(native_alps)
            overrides.update(alps_bin=native_alps, alps_root=
                             os.path.dirname(parent) if os.path.basename(parent) == "bin" else parent)
    if captured.get("CDS_LIC_FILE") or captured.get("LM_LICENSE_FILE"):
        overrides["license_file"] = captured.get("CDS_LIC_FILE") or captured.get("LM_LICENSE_FILE")
    discovery = discover_eda_environment(overrides, validate=False, environ=environ)
    native_sources = {
        "ocean_bin": "invoking Maestro installation", "virtuoso_bin": "invoking Maestro installation",
        "cadence_root": "invoking Maestro installation", "cds_lib": "invoking Maestro loaded mapping",
    }
    if native_spectre and not explicit_spectre:
        native_sources.update(spectre_bin="invoking Maestro PATH", spectre_root="invoking Maestro PATH")
    elif not explicit_spectre:
        native_sources["spectre_bin"] = "configuration/discovery fallback; absent from invoking Maestro PATH"
        discovery.setdefault("recommendations", []).append(
            "Spectre was not found in the invoking Maestro PATH; verify the configured fallback before running."
        )
    if native_alps:
        native_sources.update(alps_bin="invoking Maestro PATH", alps_root="invoking Maestro PATH")
    discovery.setdefault("sources", {}).update(native_sources)
    discovery.setdefault("recommendations", []).extend(native_snapshot_warnings(captured))
    cadence.update(discovery["effective"])
    cadence["discovery"] = discovery
    cadence["source_cds_lib_override"] = context["effective_cds_lib"]
    cadence["launch_environment"] = captured
    cadence["_native_library_mappings"] = copy.deepcopy(context.get("libraries") or {})
    cadence["setup_mode"] = "auto"
    cadence["user_config_path"] = os.path.join(context.get("settings_root") or os.path.dirname(context["context_path"]), _contract_text("eda.json"))
    cadence["project_dir"] = source["work_dir"]
    # Service run IDs already contain UUIDs; probes need per-launch isolation.
    result.setdefault("runtime", {})["run_root"] = context["runtime_root"]
    result["runtime"]["run_root_origin"] = "derived"
    result["runtime"]["hierarchy_probe_root"] = context["probe_root"]
    result["runtime"]["design_root"] = context.get("design_root")
    result["runtime"]["settings_root"] = context.get("settings_root")
    result["runtime"]["cache_root"] = context.get("cache_root")
    result["runtime"]["storage_policy"] = copy.deepcopy(context.get("storage_policy"))
    result["runtime"]["storage_base_override"] = context.get("storage_base_override")
    result["runtime"]["storage_namespace_override"] = bool(context.get("storage_base_override"))
    result["_maestro_launch_context"] = copy.deepcopy(context)
    return result
