"""One portable configuration for both unfinished UI drafts and ready designs.

This module only reads, cleans and matches JSON.  It never grants authority to a
saved private session, opens a design, restores a checkpoint, or validates an EDA
installation.  Matching uses the same identity rules as the earlier UI state.
"""
from __future__ import print_function

import copy
from .gui_performance import snapshot_copy
import datetime
import json
import os
import re
import stat
import uuid

from . import settings_state as state
from .config import MANUAL_KEYS

SCHEMA = "crescendo.configuration.v1"
LEGACY_DESIGN_SCHEMA = "crescendo.design_config.v1"
SUPPORTED_SCHEMAS = (SCHEMA, state.SCHEMA, LEGACY_DESIGN_SCHEMA)
TIMEOUT_NAMES = ("start_seconds", "configure_seconds", "run_seconds", "linger_seconds")
PROFILE_FIELDS = ("schema_version", "id", "name", "pdk_name", "package_root", "pdk_dir", "custom_adapter", "pdk_adapter")
PROFILE_LIST_FIELDS = ("reference_libraries", "libraries", "technology_libraries", "technology_library", "model_roots", "startup_extensions")
PORTABLE_MANUAL_KEYS = tuple(key for key in MANUAL_KEYS if key != "license_file")
LIST_OVERRIDES = ("extra_binary_paths", "extra_library_paths", "search_roots", "alps_search_roots", "model_roots", "startup_extensions")


def _semantic_value(value, depth=0):
    """Bounded preference evidence, never native/checkpoint authority."""
    if depth > 12:
        return None
    if isinstance(value, dict):
        return {key: _semantic_value(item, depth + 1)
                for key, item in list(value.items())[:state.MAX_RECORDS]
                if state._text(key) is not None and key != "native_content"}
    if isinstance(value, (list, tuple)):
        return [_semantic_value(item, depth + 1) for item in value[:state.MAX_RECORDS]]
    return state._scalar(value)


def maestro_settings_snapshot(setup, job_policy=None, save_options=None):
    """Compact saved Maestro values for warnings and three-way preference merge."""
    setup, job_policy = setup or {}, job_policy or {}
    def named(rows, fields=None):
        return {str(row["name"]): _semantic_value(
            {key: row[key] for key in fields if key in row} if fields else
            {key: value for key, value in row.items() if key != "name"})
            for row in _rows(rows) if row.get("name")}
    tests = {}
    for row in _rows(setup.get("tests")):
        if not row.get("name"):
            continue
        test = state._fields(row, ("enabled", "library", "cell", "view", "simulator"))
        test["variables"] = named(row.get("variables"))
        test["analyses"] = named(row.get("analyses"), ("enabled", "fields"))
        test["outputs"] = named(row.get("outputs"))
        tests[row["name"]] = test
    result = {"tests": tests, "corners": named(setup.get("corners")),
              "global_variables": named(setup.get("global_variables")),
              "model_groups": _semantic_value(setup.get("model_groups") or []),
              "test_corner_masks": _semantic_value(setup.get("test_corner_masks") or {}),
              "execution_settings": _semantic_value(setup.get("execution_settings") or {})}
    policy = job_policy.get("global") or {}
    if policy.get("available") is True:
        result["job"] = {"global": _semantic_value(policy.get("properties") or {}),
            "tests": {str(row["name"]): _semantic_value(row.get("properties") or {})
                      for row in _rows(job_policy.get("tests"))
                      if row.get("name") and row.get("available") is True}}
    save = save_options if save_options is not None else setup.get("save_options") or job_policy.get("save_options") or {}
    effective = save.get("effective") or {}
    # An unavailable query root is a hint, never a replacement for native values.
    result["save"] = state._fields(effective, (
        "simulation_root", "rdb_root", "project_dir", "use_local_psf_dir", "use_local_rdb_dir",
        "local_psf_dir", "local_rdb_dir"))
    return result


def _snapshot_differences(saved, current, prefix="", limit=state.MAX_WARNINGS):
    result = []
    if not isinstance(saved, dict) or not isinstance(current, dict):
        return result
    for key in sorted(set(saved) | set(current)):
        path = prefix + "." + key if prefix else key
        before, after = saved.get(key), current.get(key)
        if key in saved and key in current and isinstance(before, dict) and isinstance(after, dict):
            result.extend(_snapshot_differences(before, after, path, max(0, limit - len(result))))
        elif key not in saved or key not in current or before != after:
            result.append({"field": path, "saved": before, "current": after})
        if len(result) >= limit:
            return result[:limit]
    return result


def _same_native_scalar(left, right):
    if left is None or right is None or isinstance(left, bool) or isinstance(right, bool):
        return type(left) is type(right) and left == right
    return str(left).strip() == str(right).strip()


def _mapping(value):
    return value if isinstance(value, dict) else {}


def _merge_native_preferences(result, saved, current):
    """Native changes win only for their corresponding saved override fields."""
    if not isinstance(current, dict):
        return
    optimization = result["apply"]["optimization"]
    old_job = _mapping(_mapping(_mapping(saved).get("job")).get("global"))
    job = _mapping(_mapping(current.get("job")).get("global"))
    old_test_jobs = _mapping(_mapping(_mapping(saved).get("job")).get("tests"))
    test_jobs = _mapping(_mapping(current.get("job")).get("tests"))
    fields = [("jobsubmitcommand", "job_command_override", "job_command", "command"),
              ("maxjobs", "job_max_override", "job_max_jobs", "max_jobs")]
    from .job_policy import JOB_TIMEOUT_PROPERTIES
    fields.extend((native, "job_timeout_override_" + key, "job_timeout_" + key, key)
                  for key, native in JOB_TIMEOUT_PROPERTIES.items())
    for native, flag, value_key, override_key in fields:
        if optimization.get(flag) is not True:
            continue
        changed_scopes = []
        for scope, before, after in [("global", old_job, job)] + [
                ("test:" + name, _mapping(old_test_jobs.get(name)), _mapping(after))
                for name, after in test_jobs.items()]:
            changed = native in after and (not saved or native not in before or
                                           not _same_native_scalar(before[native], after[native]))
            if changed and not _same_native_scalar(optimization.get(value_key), after[native]):
                changed_scopes.append(scope)
        if not changed_scopes:
            continue
        optimization[flag] = False
        if native in job:
            optimization[value_key] = "" if job[native] is None else str(job[native])
        overrides = optimization.get("maestro_job_overrides") or {}
        (overrides.get("timeouts") or {}).pop(override_key, None)
        overrides.pop(override_key, None)
        result["warnings"].append("Maestro job.%s changed (%s); current values retained and saved override disabled." %
                                  (native, ", ".join(changed_scopes)))
    old_save, save = _mapping(_mapping(saved).get("save")), _mapping(current.get("save"))
    root = save.get("simulation_root")
    if (root and optimization.get("data_root_override_enabled") is True and
            (not saved or old_save.get("simulation_root") != root) and
            optimization.get("data_root_override") != root):
        optimization["data_root_override_enabled"] = False
        optimization["data_root_override"] = root
        result["warnings"].append("Maestro save.simulation_root changed; current path retained and saved override disabled.")


def clean_execution_selection(value):
    if not isinstance(value, dict):
        return None
    result = {}
    if isinstance(value.get("tests"), list):
        result["tests"] = list(dict.fromkeys(item for item in value["tests"][:state.MAX_RECORDS]
                                            if state._text(item) and item))
    if isinstance(value.get("analyses"), dict):
        result["analyses"] = {key: list(dict.fromkeys(item for item in rows[:state.MAX_RECORDS]
            if state._text(item) and item)) for key, rows in value["analyses"].items()
            if state._text(key) and isinstance(rows, list)}
    return result


def _match_execution_selection(selection, saved, current, warnings):
    selection = clean_execution_selection(selection)
    if selection is None or not isinstance(current, dict):
        return None
    tests = {name: row for name, row in _mapping(current.get("tests")).items() if isinstance(row, dict)}
    old_tests = _mapping(_mapping(saved).get("tests"))
    requested = selection.get("tests", [name for name, row in tests.items() if row.get("enabled", True)])
    selected = set(name for name in requested if name in tests)
    for name in requested:
        if name not in tests:
            warnings.append("Saved test is unavailable; skipped: %s." % name)
    if requested and not selected:
        selected = set(name for name, row in tests.items() if row.get("enabled", True))
        warnings.append("No saved test remains available; current Maestro enabled tests retained.")
    # Explicit Crescendo Test selection is independent of the saved enabled flag;
    # the private session applies the user's selection without changing source.
    merged = {"tests": [name for name in tests if name in selected], "analyses": {}}
    for name in merged["tests"]:
        available = {key: row for key, row in _mapping(tests[name].get("analyses")).items() if isinstance(row, dict)}
        requested_analyses = (selection.get("analyses") or {}).get(name)
        if requested_analyses is None:
            continue
        chosen = set(item for item in requested_analyses if item in available)
        for item in requested_analyses:
            if item not in available:
                warnings.append("Saved analysis is unavailable; skipped: %s/%s." % (name, item))
        if requested_analyses and not chosen:
            chosen = set(item for item, row in available.items() if row.get("enabled", True))
            warnings.append("No saved analysis remains available for %s; current Maestro enabled analyses retained." % name)
        for item, row in available.items():
            old = _mapping(_mapping(old_tests.get(name)).get("analyses")).get(item)
            old = old if isinstance(old, dict) else None
            if old is not None and old.get("enabled", True) != row.get("enabled", True):
                if row.get("enabled", True):
                    chosen.add(item)
                else:
                    chosen.discard(item)
                warnings.append("Maestro analysis %s/%s enabled state changed; current state retained." % (name, item))
        merged["analyses"][name] = [item for item in available if item in chosen]
    return merged


def _known_corner_mask(snapshot, test_name, corner):
    mask = _mapping(_mapping(_mapping(snapshot).get("test_corner_masks")).get(test_name))
    if mask.get("known") is not True:
        return None
    if corner == "_default":
        value = mask.get("nominal_enabled")
        return value if type(value) is bool else None
    disabled = mask.get("disabled_corners")
    if not isinstance(disabled, list) or any(not isinstance(name, str) for name in disabled):
        return None
    return corner not in disabled


def _metric_corner_draft_settings(settings, current_context):
    """A vanished corner scope stays visibly empty; never borrow native defaults."""
    corners = set(item if isinstance(item, str) else item.get("name")
                  for item in (current_context or {}).get("corners") or []
                  if isinstance(item, (str, dict)))
    result, warnings, conflicts = settings, [], []
    for section in ("objectives", "constraints"):
        for index, definition in enumerate(settings.get(section) or []):
            requested = definition.get("corners")
            if not requested or any(name in corners for name in requested):
                continue
            if result is settings:
                result = copy.deepcopy(settings)
            result[section][index]["corners"] = []
            conflicts.append({"kind": section[:-1], "index": index,
                "output_key": definition.get("output_key"), "saved_corners": list(requested),
                "current_corners": [], "reason": "saved_corner_names_unavailable"})
            warnings.append("All saved corners are unavailable for %s; the metric remains an empty selection draft. Select its intended corners before Start." %
                            (definition.get("output_key") or "unknown output"))
    return result, warnings, conflicts


def _merge_metric_corner_preferences(result, saved, current, current_context):
    """Apply only proved native changes to the same existing metric corner scope."""
    old_corners = _mapping(_mapping(saved).get("corners"))
    corners = _mapping(_mapping(current).get("corners"))
    if not old_corners or not corners:
        return
    available = set(item if isinstance(item, str) else item.get("name")
                    for item in (current_context or {}).get("corners") or []
                    if isinstance(item, (str, dict)))
    outputs = {}
    for row in _rows((current_context or {}).get("outputs")):
        if row.get("key") and row.get("test"):
            state._index_unique(outputs, row["key"], row["test"])
    for section in ("objectives", "constraints"):
        for index, definition in enumerate(result["apply"].get(section) or []):
            requested = definition.get("corners")
            # Omitted corners inherit source settings. An explicit empty draft
            # must remain empty until the user chooses its measurement scope.
            if not requested:
                continue
            test = outputs.get(definition.get("output_key"))
            selected = set(requested)
            changes = []
            for name in corners:
                if name not in old_corners or name not in available:
                    continue  # Adding a new native corner is not user selection.
                before, after = _mapping(old_corners[name]), _mapping(corners[name])
                old_enabled, enabled = before.get("enabled"), after.get("enabled")
                global_changed = (type(old_enabled) is bool and type(enabled) is bool and
                                  old_enabled != enabled)
                old_mask = _known_corner_mask(saved, test, name)
                mask = _known_corner_mask(current, test, name)
                mask_changed = type(old_mask) is bool and type(mask) is bool and old_mask != mask
                if not global_changed and not mask_changed:
                    continue
                reason = ("global enabled" if global_changed else "") + (
                    (" and " if global_changed else "") + "Test mask" if mask_changed else "")
                if (global_changed and enabled is False) or (mask_changed and mask is False):
                    selected.discard(name)
                    changes.append("%s disabled by current %s" % (name, reason))
                elif enabled is True and mask is True:
                    selected.add(name)
                    changes.append("%s enabled by current %s" % (name, reason))
                else:
                    # A newly enabled bit alone cannot prove availability when
                    # another required bit is unknown or remains disabled.
                    changes.append("%s %s changed; existing metric choice retained because current global/Test availability is disabled or unproven" % (name, reason))
            if not changes:
                continue
            ordered = list(dict.fromkeys(name for name in requested if name in selected))
            ordered.extend(name for name in corners if name in selected and name not in ordered)
            definition["corners"] = ordered
            result.setdefault("metric_corner_conflicts", []).append({"kind": section[:-1], "index": index,
                "output_key": definition.get("output_key"), "saved_corners": list(requested),
                "current_corners": ordered, "reason": "native_corner_scope_changed"})
            result["warnings"].append("Maestro corner changes for %s: %s.%s" % (
                definition.get("output_key"), "; ".join(changes),
                " Metric kept as an empty selection draft; select its intended corners before Start." if not ordered else ""))


def _rows(value):
    return state._rows(value)


def _texts(value):
    if isinstance(value, str):
        return value if state._text(value) is not None else None
    if isinstance(value, (list, tuple)):
        return [item for item in value[:state.MAX_RECORDS] if state._text(item) is not None]
    return None


def _policy(value):
    if isinstance(value, str):
        return state._text(value)
    if isinstance(value, dict):
        return state._fields(value, ("mode", "path", "legacy_direct"))
    return None


def _path_remaps(value):
    if isinstance(value, dict):
        return dict((key, item) for key, item in list(value.items())[:state.MAX_RECORDS]
                    if state._text(key) is not None and state._text(item) is not None)
    if isinstance(value, list):
        result = []
        for row in value[:state.MAX_RECORDS]:
            if isinstance(row, dict):
                result.append(state._fields(row, ("from", "to", "source", "destination", "target", "old", "new")))
            elif isinstance(row, (list, tuple)) and len(row) == 2 and all(state._text(item) is not None for item in row):
                result.append(list(row))
        return result
    return None


def clean_environment_overrides(value):
    """Keep only explicit manual EDA fields, including unfinished scalar text."""
    if not isinstance(value, dict):
        return {}
    result = state._fields(value, PORTABLE_MANUAL_KEYS)
    for key in LIST_OVERRIDES:
        if key in value:
            clean = _texts(value[key])
            if clean is not None:
                result[key] = clean
    for key in ("pdk_adapter_policy",):
        if key in value:
            clean = _policy(value[key])
            if clean is not None:
                result[key] = clean
    if isinstance(value.get("model_path_remaps"), (dict, list)):
        result["model_path_remaps"] = _path_remaps(value["model_path_remaps"])
    if isinstance(value.get("alps_model_file_map"), dict):
        mapping = {}
        for source, target in list(value["alps_model_file_map"].items())[:state.MAX_RECORDS]:
            if state._text(source) is None:
                continue
            if isinstance(target, str) and state._text(target) is not None:
                mapping[source] = target
            elif isinstance(target, dict):
                row = state._fields(target, ("path", "section"))
                if isinstance(target.get("sections"), dict):
                    row["sections"] = dict((key, item) for key, item in list(target["sections"].items())[:4096]
                        if state._text(key) is not None and state._text(item) is not None)
                mapping[source] = row
        result["alps_model_file_map"] = mapping
    if isinstance(value.get("pdk_profiles"), list):
        profiles = []
        for profile in _rows(value["pdk_profiles"]):
            row = state._fields(profile, PROFILE_FIELDS)
            for key in PROFILE_LIST_FIELDS:
                if key in profile:
                    clean = _texts(profile[key])
                    if clean is not None:
                        row[key] = clean
            for key in ("adapter_policy",):
                if key in profile:
                    clean = _policy(profile[key])
                    if clean is not None:
                        row[key] = clean
            for key in ("model_path_remaps", "path_remaps"):
                if key in profile:
                    clean = _path_remaps(profile[key])
                    if clean is not None:
                        row[key] = clean
            profiles.append(row)
        result["pdk_profiles"] = profiles
    return result


def clean_environment_draft(value):
    """Store editable text only; saving a draft never validates or executes it."""
    if not isinstance(value, dict):
        return {}
    result = state._fields(value, ("policy",))
    for key in ("pdk_profiles_text", "alps_model_map_text", "extra_binary_paths_text", "extra_library_paths_text"):
        if state._text(value.get(key), limit=1024 * 1024) is not None:
            result[key] = value[key]
    if isinstance(value.get("fields"), dict):
        result["fields"] = state._fields(value["fields"], PORTABLE_MANUAL_KEYS)
    return result


def _clean_settings(document):
    document = document if isinstance(document, dict) else {}
    result = {"schema_version": state.SCHEMA,
        "saved_at": state._text(document.get("saved_at")) or "",
        "setup": state._setup(document.get("setup")),
        "parameters": [], "cells": [], "outputs": [],
        "objectives": [state._metric(row, "objective") for row in _rows(document.get("objectives"))],
        "constraints": [state._metric(row, "constraint") for row in _rows(document.get("constraints"))],
        "optimization": state._optimization(document.get("optimization"))}
    for row in _rows(document.get("parameters")):
        key = state._text(row.get("parameter_id"))
        if key:
            result["parameters"].append({"parameter_id": key,
                "evidence": state._fields(row.get("evidence"), state.PARAMETER_EVIDENCE),
                "config": state._fields(row.get("config"), state.PARAMETER_CONFIG)})
    for row in _rows(document.get("cells")):
        key = state._text(row.get("scope_id"))
        if key:
            result["cells"].append({"scope_id": key, "evidence": state._cell_evidence(row.get("evidence") or {}),
                                    "selected": row.get("selected") is True})
    result["outputs"] = [state._fields(row, ("key", "test", "name")) for row in _rows(document.get("outputs"))]
    return result


def _legacy_design(document):
    reference = document.get("setup_locator") or document.get("ade_setup") or document.get("maestro") or {}
    reference = reference if isinstance(reference, dict) else {}
    setup = state._setup(reference)
    identity = dict((key, reference.get(key) or reference.get(key + "_name")) for key in state.IDENTITY_FIELDS)
    if state._identity(identity):
        setup["identity"] = identity
    optimization = document.get("optimization")
    optimization = optimization if isinstance(optimization, dict) else {}
    run = optimization.get("run")
    run = run if isinstance(run, dict) else {}
    parameters = []
    for variable in _rows(optimization.get("variables")):
        key = state._text(variable.get("parameter_id"))
        if key:
            config = state._fields(variable, ("minimum", "maximum", "scale", "value_type"))
            config["selected"] = True
            # Legacy minimum/maximum are base-unit numbers, not display-unit text.
            config["unit"] = ""
            parameters.append({"parameter_id": key, "evidence": {}, "config": config})
    settings = _clean_settings({"setup": setup, "parameters": parameters,
        "objectives": optimization.get("objectives"), "constraints": optimization.get("constraints"),
        "optimization": run})
    opt = settings["optimization"]
    if opt.get("algorithm_parameters"):
        opt["algorithm_parameter_manual"] = [[algorithm, field]
            for algorithm, fields in opt["algorithm_parameters"].items() for field in fields]
    if "candidate_timeout_seconds" in opt and opt["candidate_timeout_seconds"] in (None, -1, "-1"):
        opt["candidate_timeout_seconds"] = ""
    overrides = run.get("maestro_job_overrides")
    overrides = overrides if isinstance(overrides, dict) else {}
    if "command" in overrides:
        opt["job_command_override"] = True
        opt["job_command"] = overrides["command"]
    if "max_jobs" in overrides:
        opt["job_max_override"] = True
        opt["job_max_jobs"] = overrides["max_jobs"]
    timeouts = overrides.get("timeouts")
    timeouts = timeouts if isinstance(timeouts, dict) else {}
    for key in TIMEOUT_NAMES:
        if key in timeouts:
            opt["job_timeout_override_" + key] = True
            opt["job_timeout_" + key] = "" if timeouts[key] is None else timeouts[key]
            if timeouts[key] is None:
                opt.setdefault("maestro_job_overrides", {}).setdefault("timeouts", {})[key] = ""
    if "data_root_override" in run:
        opt["data_root_override_enabled"] = bool(run["data_root_override"])
        if run["data_root_override"] is None:
            opt["data_root_override"] = ""
    return settings


def _same_source(saved, current):
    saved = state._setup(saved)
    current = state._setup(current)
    left, right = state._identity(saved.get("identity")), state._identity(current.get("identity"))
    if left and right:
        return left == right
    return bool(saved.get("path") and saved.get("path") == current.get("path"))


def _hydrate_legacy(settings, current_context, warnings):
    """Legacy recipes lacked identity evidence. Match exact unique IDs in one TB."""
    if not current_context:
        return settings
    if not _same_source(settings.get("setup"), current_context.get("setup")):
        warnings.append("Legacy configuration source does not match this TB; unproven parameter and output bindings remain as drafts.")
        return settings
    current = state._setup(current_context.get("setup"))
    saved_source_path = settings["setup"].get("path")
    settings["setup"] = current
    if saved_source_path:
        settings["setup"]["path"] = saved_source_path
    index = {}
    for row in _rows(current_context.get("parameter_by_id") or current_context.get("parameters")):
        key = state._text(row.get("parameter_id"))
        if key:
            state._index_unique(index, key, row)
    hydrated = 0
    for row in settings["parameters"]:
        if state._parameter_key(row["evidence"]):
            continue
        candidate = index.get(row["parameter_id"])
        if candidate:
            row["evidence"] = state._parameter_evidence(candidate)
            hydrated += 1
    outputs = {}
    for row in _rows(current_context.get("outputs")):
        key = state._text(row.get("key"))
        if key:
            state._index_unique(outputs, key, row)
    keys = set(row.get("output_key") for section in ("objectives", "constraints") for row in settings[section])
    settings["outputs"] = [state._fields(outputs[key], ("key", "test", "name"))
        for key in sorted(item for item in keys if isinstance(item, str)) if outputs.get(key)]
    if hydrated or settings["outputs"]:
        warnings.append("Legacy configuration has limited identity evidence; restored only unique exact parameter/output IDs in the same TB.")
    return settings


def clean_private_suite(value):
    # A preference only: no root, ownership token, PID or live handle imported.
    if not isinstance(value, dict):
        return None
    token = value.get('suite_id')
    if not isinstance(token, str) or not re.fullmatch(r'[a-f0-9]{32}', token):
        return None
    result = {'suite_id': token}
    for key in ('revision', 'source_path'):
        item = state._text(value.get(key))
        if item:
            result[key] = item
    return result


def normalize_configuration(document, current_context=None):
    """Return a cleaned envelope; missing sections are valid unfinished drafts."""
    if isinstance(document, dict) and "document" in document and "schema_version" not in document:
        document = document["document"]
    if not isinstance(document, dict) or document.get("schema_version") not in SUPPORTED_SCHEMAS:
        raise ValueError("Unsupported configuration file; expected {} or an earlier Crescendo settings/config file.".format(SCHEMA))
    state._encoded(document)
    schema = document["schema_version"]
    warnings = []
    if schema == LEGACY_DESIGN_SCHEMA:
        settings = _legacy_design(document)
        legacy = {"design_config": True}
        provided = False
        warnings.append("Imported an earlier optimization configuration. It did not save selected cells; select the required cells once if they cannot be matched.")
    elif schema == state.SCHEMA:
        settings = _clean_settings(document)
        legacy = {"design_config": False}
        provided = isinstance(document.get("cells"), list)
    else:
        settings = _clean_settings(document.get("settings"))
        source = state._setup(document.get("source_setup"))
        if source.get("identity") or source.get("path"):
            settings["setup"] = source
        legacy = {"design_config": (document.get("legacy") or {}).get("design_config") is True} if isinstance(document.get("legacy"), dict) else {"design_config": False}
        selection = document.get("selection")
        saved_settings = document.get("settings") if isinstance(document.get("settings"), dict) else {}
        provided = selection.get("provided") is True if isinstance(selection, dict) else isinstance(saved_settings.get("cells"), list)
    canonical = {"schema_version": SCHEMA,
        "saved_at": state._text(document.get("saved_at")) or settings.get("saved_at") or "",
        "source_setup": copy.deepcopy(settings["setup"]), "settings": settings,
        "environment_overrides": clean_environment_overrides(document.get("environment_overrides")),
        "environment_draft": clean_environment_draft(document.get("environment_draft")),
        "selection": {"provided": provided}, "legacy": legacy}
    if isinstance(document.get("maestro_snapshot"), dict):
        canonical["maestro_snapshot"] = _semantic_value(document["maestro_snapshot"])
    execution = document.get("execution_selection")
    if execution is None and schema == LEGACY_DESIGN_SCHEMA:
        execution = ((document.get("optimization") or {}).get("run") or {}).get("execution_selection")
    execution = clean_execution_selection(execution)
    if execution is not None:
        canonical["execution_selection"] = execution
    suite = clean_private_suite(document.get("private_suite"))
    if suite:
        canonical["private_suite"] = suite
    applied_settings = snapshot_copy(settings)
    if legacy["design_config"]:
        _hydrate_legacy(applied_settings, current_context, warnings)
    return {"document": canonical, "settings": applied_settings,
            "source": copy.deepcopy(canonical["source_setup"]), "warnings": warnings, "legacy": legacy}


def configuration_settings(document, current_context=None):
    return normalize_configuration(document, current_context)["settings"]


def _merge_records(previous, current, key, known):
    result = list(current)
    current_keys = set(key(row) for row in current)
    for row in previous:
        identity = key(row)
        if identity not in current_keys and not known(row):
            result.append(row)
    return result[:state.MAX_RECORDS]


def build_configuration(raw_ui, parameter_by_id=None, cell_scopes=None, outputs=None, previous=None):
    """Save complete or unfinished UI inputs, retaining unmatched earlier drafts."""
    raw_ui = raw_ui if isinstance(raw_ui, dict) else {}
    settings = state.build_settings_state(raw_ui, parameter_by_id, cell_scopes, outputs)
    document = {"schema_version": SCHEMA, "saved_at": settings["saved_at"],
                "source_setup": copy.deepcopy(settings["setup"]), "settings": settings,
                "environment_overrides": clean_environment_overrides(raw_ui.get("environment_overrides")),
                "environment_draft": clean_environment_draft(raw_ui.get("environment_draft")),
                "selection": {"provided": cell_scopes is not None}, "legacy": {"design_config": False}}
    for key, cleaner in (("private_suite", clean_private_suite), ("maestro_snapshot", _semantic_value),
                         ("execution_selection", clean_execution_selection)):
        if isinstance(raw_ui.get(key), dict):
            document[key] = cleaner(raw_ui[key])
    if previous is not None:
        old = normalize_configuration(previous)["document"]
        prior = old["settings"]
        if old["legacy"]["design_config"]:
            prior = configuration_settings(old, {"setup": settings["setup"],
                "parameter_by_id": parameter_by_id or {}, "outputs": outputs or []})
        # A moved or unavailable TB must not discard drafts from an import.
        if not settings["setup"].get("identity") and not settings["setup"].get("path"):
            settings["setup"] = copy.deepcopy(prior["setup"])
            document["source_setup"] = copy.deepcopy(prior["setup"])
        param_rows = _rows(parameter_by_id)
        cell_rows = _rows(cell_scopes)
        parameter_keys = set((row.get("parameter_id"), json.dumps(state._parameter_evidence(row), sort_keys=True)) for row in param_rows)
        cell_keys = set(state._cell_key(state._cell_evidence(row)) for row in cell_rows)
        pkey = lambda row: (row.get("parameter_id"), json.dumps(row.get("evidence") or {}, sort_keys=True))
        ckey = lambda row: state._cell_key(row.get("evidence")) or (row.get("scope_id"),)
        settings["parameters"] = _merge_records(prior["parameters"], settings["parameters"], pkey, lambda row: pkey(row) in parameter_keys)
        settings["cells"] = _merge_records(prior["cells"], settings["cells"], ckey, lambda row: ckey(row) in cell_keys)
        known_outputs = set(row.get("key") for row in _rows(outputs))
        for section in ("objectives", "constraints"):
            settings[section] = _merge_records(prior[section], settings[section],
                lambda row: json.dumps(row, sort_keys=True), lambda row: row.get("output_key") in known_outputs)
        settings["outputs"] = _merge_records(prior["outputs"], settings["outputs"],
            lambda row: row.get("key"), lambda row: row.get("key") in known_outputs)
        if cell_scopes is None:
            document["selection"] = old["selection"]
        # Keep imported draft controls until a current GUI snapshot supplies them.
        combined = copy.deepcopy(prior["optimization"])
        combined.update(settings["optimization"])
        settings["optimization"] = combined
        if "environment_overrides" not in raw_ui:
            document["environment_overrides"] = old["environment_overrides"]
        if "environment_draft" not in raw_ui:
            document["environment_draft"] = old["environment_draft"]
        for key in ("maestro_snapshot", "execution_selection", "private_suite"):
            if key not in raw_ui and key in old:
                document[key] = copy.deepcopy(old[key])
        document["legacy"] = copy.deepcopy(old["legacy"])
        known_output_keys = set(row.get("key") for row in settings["outputs"])
        if (document["legacy"]["design_config"] and document["selection"]["provided"]
                and all(state._parameter_key(row["evidence"]) for row in settings["parameters"])
                and all(row.get("output_key") in known_output_keys
                    for section in ("objectives", "constraints") for row in settings[section])):
            document["legacy"]["design_config"] = False
    state._encoded(document)
    return document


def match_configuration(document, current_context):
    envelope = normalize_configuration(document, current_context)
    matching_settings, corner_warnings, corner_conflicts = _metric_corner_draft_settings(
        envelope["settings"], current_context)
    result = state.match_settings_state(matching_settings, current_context)
    result["warnings"] = (envelope["warnings"] + corner_warnings + result["warnings"])[:state.MAX_WARNINGS]
    result["metric_corner_conflicts"] = corner_conflicts
    result["document"] = envelope["document"]
    result["selection_present"] = envelope["document"]["selection"]["provided"]
    result["pending"] = envelope["document"]
    result["environment_overrides"] = envelope["document"]["environment_overrides"]
    result["environment_draft"] = envelope["document"]["environment_draft"]
    saved = envelope["document"].get("maestro_snapshot")
    current = (current_context or {}).get("maestro_snapshot")
    differences = _snapshot_differences(saved, current)
    result["maestro_differences"] = differences
    _merge_native_preferences(result, saved, current)
    _merge_metric_corner_preferences(result, saved, current, current_context)
    execution = _match_execution_selection(envelope["document"].get("execution_selection"),
                                           saved, current, result["warnings"])
    if execution is not None:
        result["apply"]["execution_selection"] = execution
    for row in differences:
        result["warnings"].append("Maestro setting changed; current value retained: %s." % row["field"])
    result["warnings"] = list(dict.fromkeys(result["warnings"]))[:state.MAX_WARNINGS]
    return result


def _inferred_parameter_scopes(settings, cell_scopes, current_setup):
    """Legacy selection can be reconstructed only from explicit design/path evidence."""
    if not _same_source(settings.get("setup"), current_setup):
        return []
    result = []
    for parameter in settings.get("parameters") or []:
        if (parameter.get("config") or {}).get("selected") is not True:
            continue
        evidence = parameter.get("evidence") or {}
        design = tuple(evidence.get("design_" + key) for key in state.IDENTITY_FIELDS)
        path = evidence.get("hierarchy_path")
        if not all(design) or not isinstance(path, str) or not path.startswith("/"):
            continue
        choices = [row for row in _rows(cell_scopes) if row.get("selectable", True) is True and
                   state._identity(row.get("design")) == design and row.get("hierarchy_path") and
                   (row["hierarchy_path"] == "/" or path == row["hierarchy_path"] or
                    path.startswith(row["hierarchy_path"].rstrip("/") + "/"))]
        if not choices:
            continue
        depth = max(len(row["hierarchy_path"].rstrip("/")) for row in choices)
        choices = [row for row in choices if len(row["hierarchy_path"].rstrip("/")) == depth]
        if len(choices) == 1 and choices[0].get("scope_id") not in result:
            result.append(choices[0]["scope_id"])
    return result


def match_configuration_cells(document, cell_scopes, current_setup=None):
    """Match exact selected cell occurrences; never replace absent data with all."""
    envelope = normalize_configuration(document)
    settings = envelope["settings"]
    warnings = list(envelope["warnings"])
    selected = [row for row in settings["cells"] if row.get("selected") is True]
    index = {}
    for row in _rows(cell_scopes):
        if row.get("selectable", True) is not True:
            continue
        key = state._cell_key(state._cell_evidence(row))
        if key:
            state._index_unique(index, key, row.get("scope_id"))
    scope_ids = []
    seen = set()
    for row in selected:
        key = state._cell_key(row.get("evidence"))
        identifier = index.get(key)
        if identifier and identifier not in seen:
            seen.add(identifier)
            scope_ids.append(identifier)
        elif len(warnings) < state.MAX_WARNINGS:
            warnings.append("Saved cell is unavailable or ambiguous; retained as a draft: {}.".format(row.get("scope_id") or "unknown"))
    provided = envelope["document"]["selection"]["provided"]
    if not provided:
        inferred = _inferred_parameter_scopes(settings, cell_scopes, current_setup)
        if inferred:
            scope_ids = inferred
            warnings.append("Saved cell selection reconstructed from exact parameter design/path evidence; only matching containing cells are inspected.")
        else:
            warnings.append("This configuration has no saved cell selection or unambiguous parameter paths; current choices are retained where proven.")
    return {"scope_ids": scope_ids, "warnings": warnings[:state.MAX_WARNINGS],
            "selection_present": provided, "selected_saved_count": len(selected),
            "matched_count": len(scope_ids), "skipped_count": max(0, len(selected) - len(scope_ids))}


def read_configuration(path, return_result=False):
    """Read one bounded JSON configuration; unsafe files are never followed."""
    parent_fd = state._open_directory(os.path.dirname(os.path.abspath(path)))
    try:
        raw = state._read_regular(parent_fd, os.path.basename(path))
    finally:
        os.close(parent_fd)
    def reject_constant(_value):
        raise ValueError("Non-finite configuration value")
    def bounded_integer(value):
        if len(value) > 64:
            raise ValueError("Oversized integer configuration value")
        return int(value)
    try:
        document = json.loads(raw.decode("utf-8"), parse_constant=reject_constant, parse_int=bounded_integer)
    except (ValueError, UnicodeError, RecursionError):
        raise ValueError("Configuration file is not valid UTF-8 JSON; current settings are retained.")
    document = normalize_configuration(document)["document"]
    if return_result:
        return {"path": os.path.abspath(path), "document": document, "revision": state._revision(raw)}
    return document


def save_configuration(path, document, retain_backups=3, expected_revision=state.REVISION_UNCHECKED, return_result=False):
    """Write only portable fields, atomically, retaining bounded exact backups."""
    canonical = normalize_configuration(document)["document"]
    raw = state._encoded(canonical)
    path = os.path.abspath(path)
    name = os.path.basename(path)
    if not name or name in (".", ".."):
        raise ValueError("Choose a JSON configuration filename.")
    parent_fd = state._open_directory(os.path.dirname(path), create=True)
    temporary = ".configuration_" + uuid.uuid4().hex
    lock = state._write_lock(parent_fd, name)
    locked = False
    committed = False
    synced = False
    try:
        lock.__enter__()
        locked = True
        try:
            previous = state._read_regular(parent_fd, name)
        except FileNotFoundError:
            previous = None
        if expected_revision is not state.REVISION_UNCHECKED and state._revision(previous) != expected_revision:
            raise state.ConfigurationConflict("Configuration changed since it was read; reload before saving.")
        if previous is not None and retain_backups:
            backup = name + ".bak-" + datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
            for index in range(100):
                candidate = backup if not index else backup + "." + str(index)
                try:
                    descriptor = os.open(candidate, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600, dir_fd=parent_fd)
                    break
                except FileExistsError:
                    continue
            else:
                raise ValueError("Too many same-second configuration backups.")
            try:
                with os.fdopen(descriptor, "wb") as stream:
                    stream.write(previous)
                    stream.flush()
                    os.fsync(stream.fileno())
            except BaseException:
                try:
                    os.unlink(candidate, dir_fd=parent_fd)
                except OSError:
                    pass
                raise
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600, dir_fd=parent_fd)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, name, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
        committed = True
        os.fsync(parent_fd)
        synced = True
        pattern = re.compile(r"^" + re.escape(name) + r"\.bak-([0-9]{8}-[0-9]{6})(?:\.([0-9]+))?$")
        backups = []
        for entry in os.listdir(parent_fd):
            match = pattern.match(entry)
            if match:
                backups.append((match.group(1), int(match.group(2) or 0), entry))
        for _stamp, _index, old in sorted(backups, reverse=True)[max(0, int(retain_backups)):]:
            try:
                info = os.stat(old, dir_fd=parent_fd, follow_symlinks=False)
                if stat.S_ISREG(info.st_mode) and info.st_nlink == 1:
                    os.unlink(old, dir_fd=parent_fd)
            except OSError:
                pass
        if return_result:
            return {"path": path, "revision": state._revision(raw)}
        return path
    except Exception as error:
        if committed:
            error.configuration_commit_state = "committed" if synced else "committed_durability_unknown"
        raise
    finally:
        try:
            os.unlink(temporary, dir_fd=parent_fd)
        except FileNotFoundError:
            pass
        if locked:
            lock.__exit__(None, None, None)
        os.close(parent_fd)


configuration_revision = state.configuration_revision
ConfigurationConflict = state.ConfigurationConflict
REVISION_UNCHECKED = state.REVISION_UNCHECKED


def save_last_configuration(document, config=None, launch_context=None,
                            expected_revision=state.REVISION_UNCHECKED, return_result=False):
    return save_configuration(state.last_settings_path(config, launch_context), document, retain_backups=0,
                              expected_revision=expected_revision, return_result=return_result)


def read_last_configuration(config=None, launch_context=None, current_setup=None, return_result=False):
    """Restore the current TB's last configuration, including moved namespaces."""
    exact = state.last_settings_path(config, launch_context)
    try:
        read_result = read_configuration(exact, return_result=True)
        document = read_result["document"]
        path = exact
    except FileNotFoundError:
        document = None
        for path in state._last_settings_candidates(config, launch_context):
            try:
                candidate_result = read_configuration(path, return_result=True)
                candidate = candidate_result["document"]
                if current_setup is not None and _same_source(candidate["source_setup"], current_setup):
                    document = candidate
                    read_result = candidate_result
                    break
            except (OSError, ValueError):
                continue
        if document is None:
            raise ValueError("No previous configuration for this TB; current settings are retained.")
    if current_setup is not None and not _same_source(document["source_setup"], current_setup):
        raise ValueError("Last configuration belongs to a different TB; current settings are retained.")
    document["_restore_source"] = path
    if return_result:
        return {"path": os.path.abspath(path), "document": document, "revision": read_result["revision"]}
    return document
