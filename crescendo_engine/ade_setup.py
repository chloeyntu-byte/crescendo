from __future__ import print_function

import ast
import os
import re
import xml.etree.ElementTree as ET

from crescendo_engine.runtime import bind_text
_contract_text = bind_text()

from .utils import sha256_file


SETUP_SCHEMA_VERSION = _contract_text("crescendo.ade_setup.v1")
SETUP_REFERENCE_SCHEMA_VERSION = _contract_text("crescendo.ade_setup_reference.v1")
SUPPORTED_BACKENDS = ("maestro", "adexl")


def _local_name(node):
    return str(node.tag).rsplit("}", 1)[-1].lower()


def _children(node, name):
    expected = name.lower()
    return [child for child in list(node or []) if _local_name(child) == expected]


def _child(node, name):
    rows = _children(node, name)
    return rows[0] if rows else None


def _descendants(node, name):
    expected = name.lower()
    return [row for row in (node.iter() if node is not None else []) if _local_name(row) == expected]


def _text(node):
    return (node.text or "").strip() if node is not None else ""


def _name(node, default=None):
    if node is None:
        return default
    return (
        node.attrib.get("Name")
        or node.attrib.get("name")
        or _text(_child(node, "name"))
        or _text(node)
        or default
    )


def _skill_string(value):
    value = (value or "").strip()
    if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
        try:
            return ast.literal_eval(value)
        except Exception:
            return value[1:-1]
    if value == "nil":
        return None
    if value == "t":
        return True
    return value


def _enabled(node, default=True):
    value = node.attrib.get("enabled") if node is not None else None
    if value is None and node is not None:
        value = node.attrib.get("Enabled")
    if value is None:
        return default
    return str(value).strip().lower() not in ("0", "false", "nil", "(nil)", "no", "off")


def _normalize_backend(backend, allow_auto=True):
    value = str(backend or "auto").strip().lower().replace("_", "").replace("-", "").replace(" ", "")
    aliases = {"maestro": "maestro", "adexl": "adexl", "xl": "adexl"}
    if allow_auto and value in ("", "auto"):
        return "auto"
    if value not in aliases:
        choices = "auto, maestro, adexl" if allow_auto else "maestro, adexl"
        raise ValueError("unsupported ADE backend %r; expected one of: %s" % (backend, choices))
    return aliases[value]


def _setupdb_root(path):
    try:
        root = ET.parse(path).getroot()
    except (IOError, OSError, ET.ParseError) as error:
        raise ValueError("ADE setup is not readable SetupDB XML: %s (%s)" % (path, error))
    if _local_name(root) != "setupdb":
        raise ValueError("ADE setup root must be setupdb: %s" % path)
    return root


def _content_backend(root):
    markers = [str(root.attrib.get("type") or ""), str(root.attrib.get("application") or ""), _text(root)]
    marker = " ".join(markers).lower().replace("_", "").replace("-", "")
    if "maestro" in marker:
        return "maestro"
    if "adexl" in marker or "ade xl" in marker:
        return "adexl"
    return None


def _candidate_paths(path, backend):
    if os.path.isdir(path):
        available = [
            os.path.join(path, name)
            for name in ("maestro.sdb", "data.sdb")
            if os.path.isfile(os.path.join(path, name))
        ]
        if backend == "auto":
            return available
        expected_name = "maestro.sdb" if backend == "maestro" else "data.sdb"
        matching = [candidate for candidate in available if os.path.basename(candidate) == expected_name]
        return matching or available
    if os.path.isfile(path) and os.path.basename(path) in ("active.state", "data.dm"):
        return _candidate_paths(os.path.dirname(path), backend)
    if os.path.isfile(path):
        return [path]
    return []


def resolve_setup_locator(path, backend="auto"):
    requested_backend = _normalize_backend(backend)
    requested_path = os.path.abspath(os.path.expanduser(path))
    candidates = _candidate_paths(requested_path, requested_backend)
    if not candidates:
        raise ValueError("not an ADE SetupDB path: %s" % requested_path)
    if len(candidates) > 1:
        raise ValueError(
            "ADE setup path is ambiguous; select maestro.sdb/data.sdb or specify --backend: %s"
            % requested_path
        )

    setup_path = candidates[0]
    root = _setupdb_root(setup_path)
    file_backend = {
        "maestro.sdb": "maestro",
        "data.sdb": "adexl",
    }.get(os.path.basename(setup_path).lower())
    content_backend = _content_backend(root)
    detected_backend = file_backend or content_backend
    if detected_backend is None and requested_backend != "auto":
        detected_backend = requested_backend
    if detected_backend is None:
        raise ValueError(
            "cannot identify ADE backend from SetupDB; use canonical maestro.sdb/data.sdb or specify backend: %s"
            % setup_path
        )
    if file_backend and content_backend and file_backend != content_backend:
        raise ValueError(
            "ADE backend evidence conflicts for %s: filename=%s content=%s"
            % (setup_path, file_backend, content_backend)
        )
    if requested_backend != "auto" and detected_backend != requested_backend:
        raise ValueError(
            "ADE backend mismatch for %s: requested=%s detected=%s"
            % (setup_path, requested_backend, detected_backend)
        )

    view_dir = os.path.dirname(setup_path)
    cell_dir = os.path.dirname(view_dir)
    library_root = os.path.dirname(cell_dir)
    return {
        "backend": detected_backend,
        "path": setup_path,
        "view_dir": view_dir,
        "view_name": os.path.basename(view_dir),
        "cell_dir": cell_dir,
        "cell_name": os.path.basename(cell_dir),
        "library_root": library_root,
        "library_name": os.path.basename(library_root),
    }


def _option_map(test):
    values = {}
    tool_options = _child(test, "tooloptions")
    for option in _children(tool_options, "option"):
        option_name = option.attrib.get("Name") or option.attrib.get("name") or _text(option)
        if option_name:
            values[option_name] = _text(_child(option, "value"))
    return values


def _variables(node):
    rows = []
    variables = _child(node, "vars")
    for variable in _children(variables, "var"):
        row = {
            "name": _name(variable, "unnamed"),
            "value": _text(_child(variable, "value")),
        }
        if "enabled" in variable.attrib or "Enabled" in variable.attrib:
            row["sweep_enabled"] = _enabled(variable)
        rows.append(row)
    return rows


def _native_content(node):
    """Preserve unknown native settings without pretending to evaluate them."""
    return {"tag": _local_name(node), "attributes": dict(sorted(node.attrib.items())),
            "text": _text(node), "children": [_native_content(child) for child in node]}


def _global_variables(root):
    rows = []
    active = _active_node(root)
    for variable in _children(_child(active, "vars"), "var"):
        dependencies = _child(variable, "dependenttests")
        rows.append({"name": _name(variable, "unnamed"),
                     "value": _text(_child(variable, "value")),
                     # Legacy field name: this is the individual variable's
                     # enabled flag, independent of the Global total checkbox
                     # and per-test participation stored separately below.
                     "sweep_enabled": _enabled(variable),
                     "dependent_tests": None if dependencies is None else [
                         {"test": _name(test, "unnamed"), "enabled": _enabled(test),
                          "omitted": str(test.attrib.get("omitted", "0")).lower() in ("1", "true", "t")}
                         for test in _children(dependencies, "dependenttest")],
                     "native_content": _native_content(variable)})
    return rows


def _model_groups(root):
    rows = []
    for group in _children(_child(_active_node(root), "modelgroups"), "modelgroup"):
        models = []
        for model in _children(_child(group, "models"), "model"):
            models.append({"name": _name(model), "enabled": _enabled(model),
                           "test": _text(_child(model, "modeltest")) or "All",
                           "file": _skill_string(_text(_child(model, "modelfile"))),
                           "section": _skill_string(_text(_child(model, "modelsection"))) or None})
        rows.append({"name": _name(group), "models": models, "native_content": _native_content(group)})
    return rows


def _execution_settings(root):
    """The saved scope/sweep/run-plan contract, including extension fields."""
    active = _active_node(root)
    rows = []
    for node in active:
        tag = _local_name(node).replace("_", "")
        if any(token in tag for token in ("sweep", "runplan", "parametric", "global")):
            rows.append(_native_content(node))
    # Test-side sweep/run-plan settings are not necessarily active siblings.
    for test in _children(_child(active, "tests"), "test"):
        for node in test:
            tag = _local_name(node).replace("_", "")
            if any(token in tag for token in ("sweep", "runplan", "parametric", "global")):
                rows.append({"test": _name(test), "content": _native_content(node)})
    return rows


def _field_map(node):
    result = {}
    for child in _children(node, "field"):
        result[child.attrib.get("Name") or child.attrib.get("name")] = _skill_string(_text(child))
    return result


def _analysis_rows(node):
    rows = []
    seen = set()
    for analysis in _descendants(node, "analysis"):
        name = _name(analysis)
        if not name or name in seen:
            continue
        fields = {}
        for field in _descendants(analysis, "field"):
            field_name = field.attrib.get("Name") or field.attrib.get("name")
            if field_name:
                fields[field_name] = _skill_string(_text(field))
        # Native save can append a skillDpl override after the earlier list
        # value. An explicitly stored nil/empty override disables the analysis;
        # only an absent enable field falls back to the element attribute.
        enabled = fields["enable"] if "enable" in fields else _enabled(analysis)
        if enabled is None or enabled is False:
            enabled = False
        elif isinstance(enabled, str):
            enabled = enabled.strip().lower() not in ("", "nil", "(nil)", "()", "false", "0", "no", "off")
        rows.append({
            "name": name,
            "enabled": bool(enabled),
            "fields": fields,
        })
        seen.add(name)
    return rows


def _output_row(output, source):
    values = {}
    for child in list(output):
        child_name = _local_name(child)
        if child_name not in ("field", "vars"):
            values[child_name] = _skill_string(_text(child))
    if _local_name(output) == "field":
        values.update(_field_map(output))
    name = values.get("name") or values.get("signal") or _name(output)
    if not name:
        return None
    return {
        "name": name,
        "expression": values.get("expression") or values.get("expr"),
        "signal": values.get("signal"),
        "eval_type": values.get("evaltype") or values.get("evalType"),
        "last_value": values.get("value"),
        "unit": values.get("yaxisunit") or values.get("yaxisUnit") or values.get("unit"),
        "source": source,
    }


def _setup_outputs(test, source_name):
    rows = []
    outputs = _child(test, "outputs")
    for output in _children(outputs, "output"):
        row = _output_row(output, source_name)
        if row:
            rows.append(row)
    return rows


def _state_details(state_path):
    if not state_path or not os.path.isfile(state_path):
        return {}
    try:
        root = ET.parse(state_path).getroot()
    except (IOError, OSError, ET.ParseError) as error:
        raise ValueError("ADE state file is not readable XML: %s (%s)" % (state_path, error))
    result = {}
    for test in _children(root, "test"):
        name = _name(test, "unnamed")
        outputs = []
        seen = set()
        for field in _descendants(test, "field"):
            field_name = field.attrib.get("Name") or field.attrib.get("name") or ""
            if not field_name.startswith("outputList_"):
                continue
            row = _output_row(field, os.path.basename(state_path))
            if not row or row["name"] in seen:
                continue
            seen.add(row["name"])
            outputs.append(row)
        result[name] = {"analyses": _analysis_rows(test), "outputs": outputs}
    return result


def _merge_outputs(primary, secondary):
    merged = []
    by_name = {}
    for row in list(primary) + list(secondary):
        name = row.get("name")
        if not name:
            continue
        if name not in by_name:
            by_name[name] = dict(row)
            merged.append(by_name[name])
        else:
            for key, value in row.items():
                if value not in (None, ""):
                    by_name[name][key] = value
    return merged


def _active_node(root):
    active = _child(root, "active")
    return active if active is not None else root


def _merge_analyses(primary, secondary):
    """Keep the full catalogue; saved active.state overrides matching fields."""
    rows, indexed = [], {}
    for item in list(primary or []) + list(secondary or []):
        name = item.get("name")
        if not name:
            continue
        if name not in indexed:
            indexed[name] = dict(item, fields=dict(item.get("fields") or {}))
            rows.append(indexed[name])
        else:
            indexed[name]["fields"].update(item.get("fields") or {})
            indexed[name]["enabled"] = item.get("enabled", True)
    return rows


def _test_corner_masks(root):
    """Read saved IC618 corner/disabledtests/test, preserving unknown formats."""
    result = {}
    active = _active_node(root)
    corners = _children(_child(active, "corners"), "corner")
    # An observed native disabledtests container establishes this encoding.
    native_masks = any(_child(corner, "disabledtests") is not None for corner in corners)
    for test in _children(_child(active, "tests"), "test"):
        name = _name(test, "unnamed")
        masks = _child(test, "disabledcorners")
        disabled = [_name(row) for row in _children(masks, "disabledcorner") if _name(row)]
        nominal = _child(test, "nominalcornerenabled")
        nominal_enabled = None if nominal is None else _text(nominal).lower() not in ("0", "nil", "false", "off")
        if native_masks:
            for corner in corners:
                members = [_name(row) for row in _children(_child(corner, "disabledtests"), "test")]
                corner_name = _name(corner)
                if corner_name == "_default":
                    nominal_enabled = name not in members
                elif name in members and corner_name not in disabled:
                    disabled.append(corner_name)
        known = native_masks or masks is not None
        result[name] = {"known": known, "disabled_corners": disabled,
                        "nominal_enabled": nominal_enabled,
                        "source": "saved_setup_xml" if known else "unavailable_in_saved_xml"}
    return result


def _disabled_test_names(root):
    return {_name(row) for row in _children(_child(_active_node(root), "disabledtests"), "test") if _name(row)}


def _test_rows(root, locator, state):
    active = _active_node(root)
    tests_node = _child(active, "tests")
    disabled_tests = _disabled_test_names(root)
    tests = []
    for test in _children(tests_node, "test"):
        name = _name(test, "unnamed")
        options = _option_map(test)
        details = state.get(name, {})
        embedded_analyses = _analysis_rows(test)
        tests.append({
            "name": name,
            "enabled": _enabled(test) and name not in disabled_tests,
            "library": options.get("lib") or options.get("library") or locator["library_name"],
            "cell": options.get("cell") or locator["cell_name"],
            "view": options.get("view") or "schematic",
            "simulator": options.get("sim") or options.get("simulator"),
            "state": options.get("state"),
            "variables": _variables(test),
            "analyses": _merge_analyses(embedded_analyses, details.get("analyses", [])),
            "outputs": _merge_outputs(
                _setup_outputs(test, os.path.basename(locator["path"])),
                details.get("outputs", []),
            ),
        })
    if not tests:
        for name, details in state.items():
            tests.append({
                "name": name,
                "enabled": True,
                "library": locator["library_name"],
                "cell": locator["cell_name"],
                "view": "schematic",
                "simulator": None,
                "state": None,
                "variables": [],
                "analyses": details.get("analyses", []),
                "outputs": details.get("outputs", []),
            })
    return tests


def _corner_rows(root):
    active = _active_node(root)
    corners_node = _child(active, "corners")
    rows = []
    for corner in _children(corners_node, "corner"):
        groups = _text(_child(corner, "modelgroupname"))
        model_names = re.findall(r'"([^"\\]*(?:\\.[^"\\]*)*)"', groups) if '"' in groups else groups.split()
        rows.append({
            "name": _name(corner, "unnamed"),
            "enabled": _enabled(corner),
            "variables": _variables(corner),
            "model_groups": model_names,
        })
    return rows


def inspect_setup(path, backend="auto"):
    locator = resolve_setup_locator(path, backend=backend)
    root = _setupdb_root(locator["path"])
    state_path = os.path.join(locator["view_dir"], "active.state")
    state = _state_details(state_path)
    tests = _test_rows(root, locator, state)
    corners = _corner_rows(root)
    active = _active_node(root)
    globals_ = _global_variables(root)

    outputs = []
    variables = []
    for test in tests:
        for output in test.get("outputs", []):
            row = dict(output)
            row["test"] = test["name"]
            outputs.append(row)
        for variable in test.get("variables", []):
            row = dict(variable)
            row.update({"scope": "test", "test": test["name"]})
            variables.append(row)
    for corner in corners:
        for variable in corner.get("variables", []):
            row = dict(variable)
            row.update({"scope": "corner", "corner": corner["name"]})
            variables.append(row)
    for variable in globals_:
        row = {key: value for key, value in variable.items() if key != "native_content"}
        row["scope"] = "global"
        variables.append(row)

    source = {
        "setup_db": locator["path"],
        "active_state": state_path if os.path.isfile(state_path) else None,
        "setup_dir": locator["view_dir"],
        "library_root": locator["library_root"],
        "sha256": sha256_file(locator["path"]),
    }
    if locator["backend"] == "maestro":
        source["maestro_sdb"] = locator["path"]
    else:
        source["data_sdb"] = locator["path"]

    return {
        "schema_version": SETUP_SCHEMA_VERSION,
        "backend": locator["backend"],
        "setup_locator": locator,
        "source": source,
        "mode": _text(_child(active, "currentmode")) or None,
        "job_policy_references": {
            "source": "saved_setup_xml",
            "global": _text(_child(active, "jobpolicyname")) or None,
            "tests": [
                {"test": _name(test, "unnamed"), "policy_name": _text(_child(test, "jobpolicyname"))}
                for test in _children(_child(active, "tests"), "test")
                if _text(_child(test, "jobpolicyname"))
            ],
        },
        "tests": tests,
        "test_corner_masks": _test_corner_masks(root),
        "corners": corners,
        "global_variables": globals_,
        "model_groups": _model_groups(root),
        "execution_settings": _execution_settings(root),
        "outputs": outputs,
        "variables": variables,
        "analysis_names": sorted(set(
            analysis.get("name")
            for test in tests
            for analysis in test.get("analyses", [])
            if analysis.get("name")
        )),
        "output_names": sorted(set(row.get("name") for row in outputs if row.get("name"))),
        "libraries": sorted(set(test.get("library") for test in tests if test.get("library"))),
        "cells": sorted(set(test.get("cell") for test in tests if test.get("cell"))),
    }


def build_setup_reference(setup):
    locator = dict((setup or {}).get("setup_locator") or {})
    source = dict((setup or {}).get("source") or {})
    path = locator.get("path") or source.get("setup_db") or source.get("maestro_sdb") or source.get("data_sdb")
    if not path:
        raise ValueError("setup does not contain a setup locator path")
    return {
        "schema_version": SETUP_REFERENCE_SCHEMA_VERSION,
        "backend": _normalize_backend((setup or {}).get("backend") or locator.get("backend")),
        "path": os.path.abspath(os.path.expanduser(path)),
        "sha256": source.get("sha256"),
    }


def normalize_setup_reference(reference, path_override=None):
    if isinstance(reference, str):
        reference = {"path": reference}
    if not isinstance(reference, dict):
        raise ValueError("ADE setup reference must be a path or object")

    nested = reference.get("setup_locator")
    if isinstance(nested, str):
        nested = {"path": nested}
    nested = nested if isinstance(nested, dict) else {}
    source = reference.get("source") if isinstance(reference.get("source"), dict) else {}
    path = (
        path_override
        or nested.get("path")
        or reference.get("path")
        or reference.get("maestro_path")
        or source.get("setup_db")
        or source.get("maestro_sdb")
        or source.get("data_sdb")
    )
    if not path:
        prepared = reference.get("prepared") if isinstance(reference.get("prepared"), dict) else {}
        setup = prepared.get("setup") if isinstance(prepared.get("setup"), dict) else {}
        if setup:
            return normalize_setup_reference(setup, path_override=path_override)
        raise ValueError("ADE setup reference does not contain a path")

    backend = reference.get("backend") or nested.get("backend")
    if not backend and (reference.get("maestro_path") or source.get("maestro_sdb")):
        backend = "maestro"
    if not backend and source.get("data_sdb"):
        backend = "adexl"
    if not backend and os.path.basename(path).lower() == "maestro.sdb":
        backend = "maestro"
    if not backend and os.path.basename(path).lower() == "data.sdb":
        backend = "adexl"
    return {
        "schema_version": SETUP_REFERENCE_SCHEMA_VERSION,
        "backend": _normalize_backend(backend or "auto"),
        "path": os.path.abspath(os.path.expanduser(path)),
        "sha256": reference.get("sha256") or source.get("sha256"),
    }


def apply_native_corner_schedule(setup, schedule):
    """Attach read-only per-Test masks from the existing owned native process."""
    if not schedule or not schedule.get('read_ok'):
        return setup.get('test_corner_masks') or {}
    groups = schedule.get('groups') or {}
    masks = {}
    for test in setup.get('tests') or []:
        name = test['name']
        masks[name] = {'known': True, 'disabled_corners': [corner for corner, group in groups.items()
                       if name in group.get('disabled_tests', [])],
                       'nominal_enabled': schedule.get('nominal_tests', {}).get(name),
                       'source': 'native_setupdb_apis'}
    setup['test_corner_masks'] = masks
    return masks


def test_corner_availability(setup, test_name):
    from .corner_selection import catalogue
    mask = (setup.get('test_corner_masks') or {}).get(test_name) or {}
    result = []
    for corner in catalogue(setup):
        name = corner['name']
        active = (mask.get('nominal_enabled') if name == '_default' else
                  name not in mask.get('disabled_corners', []) if mask.get('known') else None)
        result.append({'name': name, 'enabled': bool(corner.get('enabled', True)),
                       'test_enabled': active, 'reason': ('disabled for this Test in Maestro' if active is False else
                       'native Test mask unavailable' if active is None else 'enabled for this Test'),
                       'source': mask.get('source', 'unknown')})
    return result


def native_settings_summary(setup, job_policy=None, workspace=None):
    """Compact UI audit; retain unknown native contents in the complete file copy."""
    policy = job_policy or {}
    save = setup.get('save_options') or policy.get('save_options') or {}
    rows = [{'scope': 'Global', 'available': bool((policy.get('global') or {}).get('available')),
             'properties': dict((policy.get('global') or {}).get('properties') or {}),
             'opaque_properties': dict((policy.get('global') or {}).get('opaque_properties') or {})}]
    rows.extend({'scope': row.get('name'), 'available': bool(row.get('available')),
                 'properties': dict(row.get('properties') or {}),
                 'opaque_properties': dict(row.get('opaque_properties') or {})} for row in policy.get('tests') or [])
    selected = setup.get('selected_test')
    return {'copy_policy': 'selected_test_native_projection' if selected else 'complete_saved_maestro_view',
            'selected_test': selected,
            'preserved': ['complete saved source snapshot',
                          'selected Test and its native analyses' if selected else 'all Tests and analyses', 'per-Test corner masks',
                          'source Job Setup references and captured effective values'],
            'owned_overrides': ['private design bindings', 'run-local result and scratch paths',
                                _contract_text('explicit Crescendo execution selection and overrides')],
            'excluded': ['historical result data', 'copied history pointers', 'locks and backups'] +
                        (['unselected Tests in the native run copy', 'corners outside the current run selection'] if selected else []),
            'job_policies': rows, 'save_options': save,
            'execution_layers': [
                'Source: complete saved Maestro files and source Job Setup metadata are retained.',
                'Private Maestro capture: owned Local/2 policy generates netlists without submitting source jobs; source timeouts are carried when available.',
                'Solver CLI: source command, max jobs and applicable timeouts are inherited unless explicitly overridden; arbitrary native/plugin job properties are not translated.'],
            'private_storage': (workspace or {}).get('native_storage') or {},
            'warnings': list(policy.get('warnings') or []) + list(save.get('warnings') or []),
            'limitation': 'External startup/plugin settings are not a complete serialized inventory; any observed opaque Job properties are listed explicitly.'}
