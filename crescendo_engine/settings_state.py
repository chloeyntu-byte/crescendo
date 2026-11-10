"""Portable UI preferences, separate from design preparation and run evidence.

Loading only matches in-memory catalogues. This module never opens OA, prepares a
workspace, restores a run, or changes the selected Maestro.
"""

from __future__ import print_function

import datetime
import errno
import contextlib
import fcntl
import hashlib
import json
import math
import os
import re
import stat
import uuid

from .gui_state import ALGORITHM_FIELDS, PHYSICAL_SCALES


SCHEMA = "crescendo.ui_settings.v1"
MAX_FILE_BYTES = 64 * 1024 * 1024
MAX_RECORDS = 100000
MAX_TEXT = 4096
MAX_WARNINGS = 100
IDENTITY_FIELDS = ("library", "cell", "view")
PARAMETER_EVIDENCE = (
    "design_library", "design_cell", "design_view", "hierarchy_path", "name",
    "master_library", "master_cell", "owner_library", "owner_cell", "owner_view",
    "bound_library", "bound_cell", "bound_view", "resolved_library", "resolved_cell",
    "resolved_view", "resolved_view_type", "value_type", "source",
)
PARAMETER_CONFIG = ("selected", "minimum", "maximum", "unit", "scale", "value_type")
OPTIMIZATION_FIELDS = (
    "algorithm", "generations", "candidate_timeout_seconds", "run_name",
    "diagnostic_generations", "diagnostic_total_candidates",
    "advanced_visible", "last_single_algorithm", "job_command_override", "job_command",
    "job_max_override", "job_max_jobs", "maestro_candidate_mode", "execution_backend", "simulator",
    "data_root_override_enabled", "data_root_override",
    "command_scheduler",
) + tuple("job_timeout_" + key for key in ("start_seconds", "configure_seconds", "run_seconds", "linger_seconds")) + tuple(
    "job_timeout_override_" + key for key in ("start_seconds", "configure_seconds", "run_seconds", "linger_seconds"))


def _text(value, limit=MAX_TEXT):
    if not isinstance(value, str) or len(value) > limit or "\x00" in value:
        return None
    return value


def _scalar(value):
    if isinstance(value, str):
        return _text(value)
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, (float, int)):
        try:
            if math.isfinite(value):
                return value
        except OverflowError:
            pass
    return None


def _fields(row, names):
    if not isinstance(row, dict):
        return {}
    result = {}
    for key in names:
        value = row.get(key)
        clean = _scalar(value)
        if key in row and (clean is not None or value is None):
            result[key] = clean
    return result


def _entry_text(value, numeric=False):
    """Keep editable text, or a finite JSON number from an older preference file."""
    if isinstance(value, str):
        return _text(value)
    if numeric and type(value) in (int, float) and _scalar(value) is not None:
        return str(value)
    return None


def _entry_fields(row, names, numeric=False, skipped=None):
    for key in names:
        if key not in row:
            continue
        clean = _entry_text(row[key], numeric=numeric)
        if clean is None:
            row.pop(key)
            if skipped is not None:
                skipped(key)
        else:
            row[key] = clean


def _rows(value):
    if isinstance(value, dict):
        value = list(value.values())
    if not isinstance(value, (list, tuple)):
        return []
    return [row for row in value[:MAX_RECORDS] if isinstance(row, dict)]


def _identity(row):
    row = row if isinstance(row, dict) else {}
    value = tuple(_text(row.get(key)) for key in IDENTITY_FIELDS)
    return value if all(value) else None


def _setup(row):
    row = row if isinstance(row, dict) else {}
    result = _fields(row, ("backend", "path"))
    identity = _identity(row.get("identity") or row)
    if identity:
        result["identity"] = dict(zip(IDENTITY_FIELDS, identity))
    designs = []
    seen = set()
    for design in _rows(row.get("designs")):
        key = _identity(design)
        if key and key not in seen:
            seen.add(key)
            designs.append(dict(zip(IDENTITY_FIELDS, key)))
    result["designs"] = designs
    result["tests"] = [_fields(test, ("name", "library", "cell", "view"))
                       for test in _rows(row.get("tests"))]
    return result


def settings_state_directory(config=None, launch_context=None, setup_path=None, cwd=None):
    """Use the actual project, never the package or home runtime directory.

    ``setup_path`` is intentionally not interpreted as a writable project: an OA
    library can be read-only and is not the Virtuoso working directory.
    """
    config = config or {}
    named_root = (config.get("runtime") or {}).get("settings_root")
    if named_root:
        return os.path.realpath(os.path.abspath(named_root))
    cadence = config.get("cadence") or {}
    selected_source = config.get("_selected_setup_source") or {}
    context = launch_context or config.get("_maestro_launch_context") or {}
    source = context.get("source") or {}
    root = (cadence.get("project_dir") or selected_source.get("work_dir") or
            source.get("work_dir") or cwd or os.getcwd())
    if not isinstance(root, str) or not root or "\x00" in root:
        raise ValueError("Project working directory is unavailable.")
    return os.path.join(os.path.realpath(os.path.abspath(root)), ".crescendo", "settings")


def _parameter_evidence(row):
    evidence = _fields(row, PARAMETER_EVIDENCE)
    if "hierarchy_path" not in evidence:
        for key in ("instance_path", "path"):
            if _text(row.get(key)):
                evidence["hierarchy_path"] = row[key]
                break
    return evidence


def _parameter_key(evidence):
    if not isinstance(evidence, dict):
        return None
    required = ("design_library", "design_cell", "design_view", "hierarchy_path", "name",
                "master_library", "master_cell")
    if not all(_text(evidence.get(key)) for key in required):
        return None
    return tuple(evidence.get(key) for key in required)


def _same_evidence(saved, current):
    # An ID alone is insufficient: the same TB occurrence may now use a new PDK
    # or a different config implementation. Every saved identity field must agree.
    return bool(saved and current and all(current.get(key) == value
                for key, value in saved.items() if value not in (None, "")))


def _index_unique(index, key, value):
    # Ambiguous catalogues are skipped rather than storing an unbounded bucket
    # that could turn a sequence of duplicate saved entries into quadratic work.
    if key in index:
        index[key] = None
    else:
        index[key] = value


def _cell_evidence(row):
    value = _fields(row, ("hierarchy_path", "library", "cell", "view", "master_library",
                         "master_cell", "master_view"))
    design = _identity(row.get("design"))
    if design:
        value["design"] = dict(zip(IDENTITY_FIELDS, design))
    return value


def _cell_key(row):
    if not isinstance(row, dict):
        return None
    design = _identity(row.get("design"))
    path = _text(row.get("hierarchy_path"))
    master = tuple(_text(row.get(key) or row.get(fallback)) for key, fallback in (
        ("master_library", "library"), ("master_cell", "cell"), ("master_view", "view")))
    return design + (path,) + master if design and path and all(master) else None


def _optimization(row):
    row = row if isinstance(row, dict) else {}
    value = _fields(row, OPTIMIZATION_FIELDS)
    commands = row.get("execution_commands")
    if isinstance(commands, dict):
        cleaned_commands = _fields(commands, ("submission_template", "scheduler_query_template", "scheduler_cancel_template"))
        solvers = commands.get("solver_templates")
        if isinstance(solvers, dict):
            cleaned_commands["solver_templates"] = _fields(solvers, ("spectre", "alps"))
        value["execution_commands"] = cleaned_commands
    parameters = row.get("algorithm_parameters")
    if isinstance(parameters, dict):
        cleaned = {}
        for algorithm, fields in ALGORITHM_FIELDS.items():
            nested = parameters.get(algorithm)
            if isinstance(nested, dict):
                cleaned[algorithm] = _fields(nested, fields)
        # Accept an ordinary saved run's flat algorithm_parameters too.
        if not cleaned and isinstance(row.get("algorithm"), str) and row["algorithm"] in ALGORITHM_FIELDS:
            cleaned[row["algorithm"]] = _fields(parameters, ALGORITHM_FIELDS[row["algorithm"]])
        value["algorithm_parameters"] = cleaned
    manual = row.get("algorithm_parameter_manual")
    if isinstance(manual, list):
        value["algorithm_parameter_manual"] = [list(pair) for pair in manual[:100]
            if isinstance(pair, (list, tuple)) and len(pair) == 2 and all(isinstance(item, str) for item in pair) and
            pair[0] in ALGORITHM_FIELDS and pair[1] in ALGORITHM_FIELDS[pair[0]]]
    overrides = row.get("maestro_job_overrides")
    if isinstance(overrides, dict):
        value["maestro_job_overrides"] = _fields(overrides, ("command", "max_jobs"))
        if isinstance(overrides.get("timeouts"), dict):
            value["maestro_job_overrides"]["timeouts"] = _fields(overrides["timeouts"],
                ("start_seconds", "configure_seconds", "run_seconds", "linger_seconds"))
    return value


def _metric(row, kind):
    keys = ("output_key", "aggregation", "direction", "target") if kind == "objective" else (
        "output_key", "aggregation", "operator", "limit")
    result = _fields(row, keys)
    corners = row.get("corners")
    if isinstance(corners, list):
        result["corners"] = [item for item in corners[:4096] if _text(item) is not None]
    return result


def build_settings_state(raw_ui, parameter_by_id=None, cell_scopes=None, outputs=None):
    """Snapshot even incomplete/invalid edits, with an explicit field allowlist."""
    raw_ui = raw_ui if isinstance(raw_ui, dict) else {}
    configs = raw_ui.get("parameter_configs")
    configs = configs if isinstance(configs, dict) else {}
    parameters = []
    for row in _rows(parameter_by_id):
        key = _text(row.get("parameter_id"))
        if key and isinstance(configs.get(key), dict):
            parameters.append({"parameter_id": key, "evidence": _parameter_evidence(row),
                               "config": _fields(configs[key], PARAMETER_CONFIG)})
    selected = set(item for item in raw_ui.get("selected_cell_scope_ids") or []
                   if isinstance(item, str))
    cells = []
    for row in _rows(cell_scopes):
        key = _text(row.get("scope_id"))
        if key:
            cells.append({"scope_id": key, "evidence": _cell_evidence(row), "selected": key in selected})
    document = {"schema_version": SCHEMA,
        "saved_at": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "setup": _setup(raw_ui.get("setup")), "parameters": parameters, "cells": cells,
        "objectives": [_metric(row, "objective") for row in _rows(raw_ui.get("objectives"))],
        "constraints": [_metric(row, "constraint") for row in _rows(raw_ui.get("constraints"))],
        "optimization": _optimization(raw_ui.get("optimization"))}
    # Output evidence stays logical; no expressions, result paths, or histories.
    document["outputs"] = [_fields(row, ("key", "test", "name")) for row in _rows(outputs)]
    _encoded(document)  # Fail before opening any output if the file would be too large.
    return document


def match_settings_state(document, current_context):
    """Return a bounded best-effort application plan in O(saved + current)."""
    if not isinstance(document, dict) or document.get("schema_version") != SCHEMA:
        raise ValueError("Unsupported settings file; expected {}.".format(SCHEMA))
    current_context = current_context or {}
    warnings = []
    total_warnings = [0]
    def warn(message):
        total_warnings[0] += 1
        if len(warnings) < MAX_WARNINGS:
            warnings.append(str(message)[:720])
    saved_setup = _setup(document.get("setup"))
    current_setup = _setup(current_context.get("setup"))
    saved_identity = _identity(saved_setup.get("identity"))
    current_identity = _identity(current_setup.get("identity"))
    setup_match = bool(saved_identity and current_identity and saved_identity == current_identity)
    if saved_identity and current_identity and not setup_match:
        warn("Saved Maestro differs from the current Maestro; the current binding is retained.")
    designs = set(_identity(row) for row in current_setup["designs"])
    saved_designs = set(_identity(row) for row in saved_setup["designs"])
    shared_designs = designs.intersection(saved_designs)
    current_tests, saved_tests = {}, {}
    for target, rows in ((current_tests, current_setup["tests"]), (saved_tests, saved_setup["tests"])):
        for row in rows:
            name = _text(row.get("name"))
            identity = _identity(row)
            if name and identity:
                _index_unique(target, name, identity)
    application = {"setup": {"matches": setup_match, "saved_identity": saved_setup.get("identity"),
                             "current_identity": current_setup.get("identity")},
                   "parameter_configs": {}, "selected_cell_scope_ids": [],
                   "objectives": [], "constraints": [],
                   "optimization": _optimization(document.get("optimization"))}
    optimization = application["optimization"]
    storage_warning = None
    old_setup_path, new_setup_path = saved_setup.get("path"), current_setup.get("path")
    saved_data_root = optimization.get("data_root_override")
    if (optimization.get("data_root_override_enabled") is True
            and isinstance(saved_data_root, str) and os.path.isabs(saved_data_root)
            and isinstance(old_setup_path, str) and isinstance(new_setup_path, str)
            and os.path.isabs(old_setup_path) and os.path.isabs(new_setup_path)
            and old_setup_path != new_setup_path):
        optimization["data_root_override_enabled"] = False
        storage_warning = "Saved absolute data-root override disabled after TB relocation; re-enable it to reuse this path."
        warn(storage_warning)
    for key, choices in (("algorithm", ALGORITHM_FIELDS), ("last_single_algorithm", ALGORITHM_FIELDS),
                         ("maestro_candidate_mode", ("auto", "serial", "batch")),
                         ("execution_backend", ("auto", "maestro_netlist", "persistent_maestro", "legacy", "adexl", "private_netlist", "maestro_baseline")),
                         ("simulator", ("spectre", "alps")),
                         ("command_scheduler", ("auto", "batch", "receipt"))):
        if key in optimization and (not isinstance(optimization[key], str) or optimization[key] not in choices):
            optimization.pop(key)
            warn("Skipped unknown optimization setting: {}.".format(key))
    for key in ("advanced_visible", "job_command_override", "job_max_override", "data_root_override_enabled") + tuple(
            "job_timeout_override_" + name for name in ("start_seconds", "configure_seconds", "run_seconds", "linger_seconds")):
        if key in optimization and type(optimization[key]) is not bool:
            optimization.pop(key)
            warn("Skipped invalid checkbox setting: {}.".format(key))
    bad_entry = lambda key: warn("Skipped invalid optimization entry: {}.".format(key))
    _entry_fields(optimization, ("generations", "candidate_timeout_seconds", "job_max_jobs",
                                 "diagnostic_generations", "diagnostic_total_candidates"),
                  numeric=True, skipped=bad_entry)
    _entry_fields(optimization, tuple("job_timeout_" + name for name in
        ("start_seconds", "configure_seconds", "run_seconds", "linger_seconds")), numeric=True, skipped=bad_entry)
    _entry_fields(optimization, ("run_name", "job_command", "data_root_override"), skipped=bad_entry)
    for algorithm, fields in optimization.get("algorithm_parameters", {}).items():
        _entry_fields(fields, tuple(fields), numeric=True, skipped=bad_entry)
    commands = optimization.get("execution_commands")
    if isinstance(commands, dict):
        _entry_fields(commands, ("submission_template", "scheduler_query_template", "scheduler_cancel_template"), skipped=bad_entry)
        solvers = commands.get("solver_templates")
        if isinstance(solvers, dict):
            _entry_fields(solvers, ("spectre", "alps"), skipped=bad_entry)
    overrides = optimization.get("maestro_job_overrides")
    if overrides is not None:
        _entry_fields(overrides, ("command",), skipped=bad_entry)
        _entry_fields(overrides, ("max_jobs",), numeric=True, skipped=bad_entry)
        if isinstance(overrides.get("timeouts"), dict):
            _entry_fields(overrides["timeouts"], tuple(overrides["timeouts"]), numeric=True, skipped=bad_entry)
    counts = dict((section, {"matched": 0, "skipped": 0}) for section in (
        "parameters", "cells", "objectives", "constraints"))
    parameter_index, evidence_index = {}, {}
    for row in _rows(current_context.get("parameter_by_id") or current_context.get("parameters")):
        key = _text(row.get("parameter_id"))
        evidence = _parameter_evidence(row)
        if key:
            _index_unique(parameter_index, key, evidence)
        stable = _parameter_key(evidence)
        if stable:
            _index_unique(evidence_index, stable, (key, evidence))
    seen = set()
    for row in _rows(document.get("parameters")):
        key = _text(row.get("parameter_id"))
        evidence = _fields(row.get("evidence"), PARAMETER_EVIDENCE)
        stable = _parameter_key(evidence)
        current_evidence = parameter_index.get(key)
        candidates = [(key, current_evidence)] if stable and _same_evidence(evidence, current_evidence) else []
        if not candidates and stable:
            fallback = evidence_index.get(stable)
            if fallback and _same_evidence(evidence, fallback[1]):
                candidates = [fallback]
        candidates = [candidate for candidate in candidates if candidate[0] and candidate[0] not in seen]
        if len(candidates) != 1 or not isinstance(row.get("config"), dict):
            counts["parameters"]["skipped"] += 1
            warn("Skipped unmatched or ambiguous parameter: {}.".format(key or "unknown"))
            continue
        identifier = candidates[0][0]
        seen.add(identifier)
        config = _fields(row["config"], PARAMETER_CONFIG)
        # Type belongs to the freshly inspected PDK/catalogue, not preferences.
        config.pop("value_type", None)
        # Selection and enumerated controls cannot acquire truthy arbitrary JSON.
        if "selected" in config and type(config["selected"]) is not bool:
            config.pop("selected")
        if "scale" in config and config["scale"] not in ("linear", "log"):
            config.pop("scale")
        _entry_fields(config, ("minimum", "maximum"), numeric=True,
                      skipped=lambda field: warn("Skipped invalid parameter {}: {}.".format(field, identifier)))
        if "unit" in config and (not isinstance(config["unit"], str) or
                (config["unit"] and config["unit"].lower() not in PHYSICAL_SCALES)):
            config.pop("unit")
            warn("Skipped unsupported parameter unit: {}.".format(identifier))
        application["parameter_configs"][identifier] = config
        counts["parameters"]["matched"] += 1
    cell_index = {}
    for row in _rows(current_context.get("cell_scopes")):
        stable = _cell_key(_cell_evidence(row))
        if stable and row.get("selectable", True):
            _index_unique(cell_index, stable, row.get("scope_id"))
    seen = set()
    for row in _rows(document.get("cells")):
        if row.get("selected") is not True:
            continue
        stable = _cell_key(row.get("evidence"))
        identifier = cell_index.get(stable)
        if identifier and identifier not in seen:
            seen.add(identifier)
            application["selected_cell_scope_ids"].append(identifier)
            counts["cells"]["matched"] += 1
        else:
            counts["cells"]["skipped"] += 1
            warn("Skipped unavailable selected cell: {}.".format(row.get("scope_id") or "unknown"))
    output_index = {}
    for row in _rows(current_context.get("outputs")):
        key = _text(row.get("key"))
        name = _text(row.get("name"))
        if name is None and key:
            name = key.split("::", 1)[-1]
        if key and name and name.strip() and not name.strip().startswith("/"):
            output_index[key] = row
    saved_outputs = dict((row.get("key"), row) for row in _rows(document.get("outputs"))
                         if _text(row.get("key")))
    corners = set(item if isinstance(item, str) else item.get("name")
                  for item in current_context.get("corners") or []
                  if isinstance(item, (str, dict)))
    for kind, section in (("objective", "objectives"), ("constraint", "constraints")):
        for row in _rows(document.get(section)):
            definition = _metric(row, kind)
            key = definition.get("output_key")
            current_output = output_index.get(key)
            saved_output = saved_outputs.get(key)
            identity_ok = bool(setup_match or shared_designs)
            test_name = saved_output.get("test") if saved_output else None
            if test_name and saved_tests:
                identity_ok = bool(saved_tests.get(test_name) and
                                   saved_tests[test_name] == current_tests.get(test_name))
            evidence_ok = bool(current_output and saved_output and all(
                current_output.get(field) == saved_output.get(field)
                for field in ("test", "name") if saved_output.get(field) is not None))
            aggregation_ok = definition.get("aggregation", "mean") in ("mean", "min", "max", "first", "last")
            mode = definition.get("direction" if kind == "objective" else "operator")
            mode_ok = mode in ("minimize", "maximize", "target") if kind == "objective" else mode in ("<=", ">=")
            requested = definition.get("corners")
            matched_corners = [item for item in requested or [] if item in corners]
            if requested is not None:
                definition["corners"] = matched_corners
            if not identity_ok or not evidence_ok or not aggregation_ok or not mode_ok or (requested and not matched_corners):
                counts[section]["skipped"] += 1
                warn("Skipped unavailable {} output or corners: {}.".format(kind, key or "unknown"))
                continue
            application[section].append(definition)
            counts[section]["matched"] += 1
            missing = len(requested or []) - len(matched_corners)
            if missing:
                warn("Skipped {} unavailable corner(s) for {}.".format(missing, key))
    if total_warnings[0] > len(warnings):
        warnings.append("{} additional unmatched items omitted.".format(total_warnings[0] - len(warnings)))
    matched = sum(row["matched"] for row in counts.values())
    skipped = sum(row["skipped"] for row in counts.values())
    result = {"apply": application, "counts": counts, "warnings": warnings,
              "summary": "Loaded {} matching item(s); skipped {} unavailable item(s).".format(matched, skipped)}
    if storage_warning:
        result["storage_warning"] = storage_warning
    return result


def _encoded(document):
    try:
        raw = (json.dumps(document, ensure_ascii=False, allow_nan=False, separators=(",", ":")) + "\n").encode("utf-8")
    except (TypeError, ValueError, OverflowError, RecursionError):
        raise ValueError("Settings file contains invalid JSON values.")
    if len(raw) > MAX_FILE_BYTES:
        raise ValueError("Settings file exceeds the 64 MiB size limit.")
    return raw


def _open_directory(path, create=False):
    absolute = os.path.abspath(path)
    parts = [part for part in absolute.split(os.sep) if part]
    if len(parts) > 128 or len(absolute) > 4096:
        raise ValueError("Settings directory path is too long.")
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(os.sep, flags)
    try:
        for part in parts:
            if create:
                try:
                    os.mkdir(part, 0o700, dir_fd=descriptor)
                except FileExistsError:
                    pass
            child = os.open(part, flags, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


# A sentinel distinguishes an unspecified check from an expected missing file.
REVISION_UNCHECKED = object()


class ConfigurationConflict(ValueError):
    pass


def _revision(raw):
    return None if raw is None else hashlib.sha256(raw).hexdigest()


@contextlib.contextmanager
def _write_lock(parent_fd, name):
    # Stable sidecar is never removed: unlinking a lock permits split locks.
    lock_name = '.crescendo-config-lock-' + hashlib.sha256(name.encode('utf-8')).hexdigest()
    descriptor = os.open(lock_name, os.O_RDWR | os.O_CREAT | getattr(os, 'O_NOFOLLOW', 0), 0o600, dir_fd=parent_fd)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise ValueError('Configuration lock must be a regular file without links.')
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            if error.errno in (errno.EACCES, errno.EAGAIN):
                raise ConfigurationConflict('Configuration is being saved by another window; retry after it finishes.')
            raise
        yield
    finally:
        os.close(descriptor)


def configuration_revision(path):
    path = os.path.abspath(path)
    try:
        parent_fd = _open_directory(os.path.dirname(path))
    except FileNotFoundError:
        return None
    try:
        try:
            return _revision(_read_regular(parent_fd, os.path.basename(path)))
        except FileNotFoundError:
            return None
    finally:
        os.close(parent_fd)


def _read_regular(parent_fd, name):
    descriptor = os.open(name, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | os.O_NONBLOCK, dir_fd=parent_fd)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise ValueError("Settings file must be a regular file without links.")
        if info.st_size > MAX_FILE_BYTES:
            raise ValueError("Settings file exceeds the 64 MiB size limit.")
        with os.fdopen(descriptor, "rb") as stream:
            descriptor = None
            raw = stream.read(MAX_FILE_BYTES + 1)
        if len(raw) > MAX_FILE_BYTES:
            raise ValueError("Settings file exceeds the 64 MiB size limit.")
        return raw
    finally:
        if descriptor is not None:
            os.close(descriptor)


def read_settings_state(path):
    """Read one bounded plain JSON document; links and invalid JSON are rejected."""
    parent_fd = _open_directory(os.path.dirname(os.path.abspath(path)))
    try:
        raw = _read_regular(parent_fd, os.path.basename(path))
    finally:
        os.close(parent_fd)
    def reject_constant(_value):
        raise ValueError("Non-finite settings value")
    def bounded_integer(value):
        if len(value) > 64:
            raise ValueError("Oversized integer settings value")
        return int(value)
    try:
        document = json.loads(raw.decode("utf-8"), parse_constant=reject_constant, parse_int=bounded_integer)
    except (ValueError, UnicodeError, RecursionError):
        raise ValueError("Settings file is not valid UTF-8 JSON.")
    if not isinstance(document, dict) or document.get("schema_version") != SCHEMA:
        raise ValueError("Unsupported settings file; expected {}.".format(SCHEMA))
    return document


def save_settings_state(path, document, retain_backups=3):
    """Atomically save a document; keep at most a small number of own backups."""
    if not isinstance(document, dict) or document.get("schema_version") != SCHEMA:
        raise ValueError("Unsupported settings file; expected {}.".format(SCHEMA))
    raw = _encoded(document)
    path = os.path.abspath(path)
    name = os.path.basename(path)
    if not name or name in (".", ".."):
        raise ValueError("Choose a JSON settings filename.")
    parent_fd = _open_directory(os.path.dirname(path), create=True)
    temporary = ".state_" + uuid.uuid4().hex
    try:
        try:
            previous = _read_regular(parent_fd, name)
        except FileNotFoundError:
            previous = None
        if previous is not None and retain_backups:
            backup = name + ".bak-" + datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
            for index in range(100):
                candidate = backup if not index else backup + "." + str(index)
                try:
                    backup_fd = os.open(candidate, os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                                        0o600, dir_fd=parent_fd)
                    break
                except FileExistsError:
                    continue
            else:
                raise ValueError("Too many same-second settings backups.")
            with os.fdopen(backup_fd, "wb") as stream:
                stream.write(previous)
                stream.flush()
                os.fsync(stream.fileno())
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600, dir_fd=parent_fd)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, name, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
        os.fsync(parent_fd)
        # Only inspect siblings of this exact settings filename. Never traverse
        # another TB or delete user-chosen presets. Invalid/link backups remain.
        pattern = re.compile(r"^" + re.escape(name) + r"\.bak-([0-9]{8}-[0-9]{6})(?:\.([0-9]+))?$")
        backups = []
        for entry in os.listdir(parent_fd):
            match = pattern.match(entry)
            if match:
                backups.append((match.group(1), int(match.group(2) or 0), entry))
        backups.sort(reverse=True)
        for _stamp, _index, old in backups[max(0, int(retain_backups)):]:
            try:
                info = os.stat(old, dir_fd=parent_fd, follow_symlinks=False)
                if stat.S_ISREG(info.st_mode) and info.st_nlink == 1:
                    os.unlink(old, dir_fd=parent_fd)
            except OSError:
                pass  # A preference backup must never gate a new optimization.
        return path
    finally:
        try:
            os.unlink(temporary, dir_fd=parent_fd)
        except FileNotFoundError:
            pass
        os.close(parent_fd)


def last_settings_path(config=None, launch_context=None):
    return os.path.join(settings_state_directory(config, launch_context), "last.json")


def save_last_settings_state(document, config=None, launch_context=None):
    """Replace one best-effort preference snapshot, without accumulating files."""
    return save_settings_state(last_settings_path(config, launch_context), document, retain_backups=0)


def _last_settings_candidates(config=None, launch_context=None):
    """Find relocated namespaces for exactly this LCV, within one work dir."""
    from .runtime_layout import path_component, project_directory
    config = config or {}
    context = launch_context or config.get("_maestro_launch_context") or {}
    source = config.get("_selected_setup_source") or context.get("source") or {}
    if not _identity(source):
        return []
    base = os.path.join(project_directory(config, source), ".crescendo")
    prefix = "%s__%s_%s_" % (path_component(source.get("cell")),
                              path_component(source.get("library"), "lib", 36),
                              path_component(source.get("view"), "maestro", 24))
    candidates = []
    try:
        descriptor = _open_directory(base)
    except (OSError, ValueError):
        return []
    try:
        # A directory-name bound keeps this a quick preference lookup. No
        # native hierarchy, histories or run data are opened here.
        for name in os.listdir(descriptor)[:4096]:
            if not name.startswith(prefix):
                continue
            digest = name[len(prefix):]
            if not 8 <= len(digest) <= 64 or any(c not in "0123456789abcdef" for c in digest):
                continue
            try:
                info = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
            except OSError:
                continue
            if not stat.S_ISDIR(info.st_mode):
                continue
            for folder in ("states", "settings"):
                path = os.path.join(base, name, folder, "last.json")
                try:
                    info = os.lstat(path)
                    if stat.S_ISREG(info.st_mode) and info.st_nlink == 1:
                        candidates.append((info.st_mtime, path))
                except OSError:
                    continue
    finally:
        os.close(descriptor)
    return [path for _stamp, path in sorted(candidates, reverse=True)[:5]]


def read_last_settings_state(config=None, launch_context=None, current_setup=None):
    exact = last_settings_path(config, launch_context)
    # A corrupt current last.json is reported, rather than silently substituting
    # an older draft. Fallback is only for a namespace moved to a new machine.
    try:
        document = read_settings_state(exact)
        path = exact
    except FileNotFoundError:
        document = None
        for path in _last_settings_candidates(config, launch_context):
            try:
                value = read_settings_state(path)
                saved = _identity((value.get("setup") or {}).get("identity"))
                current = _identity((current_setup or {}).get("identity"))
                if current is not None and saved == current:
                    document = value
                    break
            except (OSError, ValueError):
                continue
        if document is None:
            raise ValueError("No previous settings for this TB; current settings are retained.")
    if current_setup is not None:
        saved = _identity((document.get("setup") or {}).get("identity"))
        current = _identity((current_setup or {}).get("identity"))
        if not saved or not current or saved != current:
            raise ValueError("Last settings belong to a different TB; current settings are retained.")
    document["_restore_source"] = path
    return document
