"""Plan native ADE device overrides without invoking a PDK callback.

Native settings readback and generated point SCS are separate evidence.
Neither is an OA backannotation or solver-completion claim.

IC618 references: maeSKILLref/maestroSKILL.html (maeSetParameter,
maeGetParameter, maeSetSetup); adexlSKILLref/ParameterRelated.html;
assembler/asmDesignVars.html#callbacks.
"""
from __future__ import print_function

import copy
import hashlib
import math
import os
import re
import sqlite3
from urllib.parse import quote

from crescendo_engine.runtime import bind_text
_contract_text = bind_text()

from .hierarchy import parse_physical_number


SCHEMA = _contract_text("crescendo.native_device_parameter_plan.v1")
GENERATION_PROTOCOL = _contract_text("crescendo.native_run_device_parameters.v1")
MAX_SELECTED = 512
MAX_CORNERS = 1024
MAX_SETTINGS = 16384
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _text(value, label, maximum=1024):
    if (not isinstance(value, str) or not value or len(value) > maximum
            or any(ord(char) < 32 or ord(char) == 127 for char in value)):
        raise ValueError("Invalid native parameter " + label)
    return value


def _component(value, label):
    value = _text(value, label, 256)
    if "/" in value or value in (".", ".."):
        raise ValueError("Native parameter %s must name one exact component" % label)
    return value


def _quote(value):
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _number(value):
    number = parse_physical_number(value)
    if number is None or not math.isfinite(number):
        raise ValueError("Native candidate must contain one finite numeric value")
    # Keep the already-rendered physical literal; do not evaluate an ADE
    # variable, a sweep, a CDF default, or an arbitrary SKILL/AEL expression.
    if isinstance(value, str):
        text = value.strip()
        if not re.match(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?[A-Za-z]*$", text):
            raise ValueError("Native candidate is not one numeric literal")
        return text
    return "%.17g" % number


def build_native_parameter_plan(parameter_rows, corners, inherited_parameters=None,
                                owned_paths=None, allowed_symbols=None):
    """Consume resolved private owner-CV rows, never original source rows.

    Each row has parameter_id/library/cell/view/instance/property/value, as
    produced by the private schematic clone plan.  The caller must prove that
    this CV is private and reachable from its private config.  A non-private
    source path is not made private by this function.
    """
    rows, corners = list(parameter_rows or []), list(corners or [])
    if not rows or len(rows) > MAX_SELECTED or len(corners) > MAX_CORNERS:
        raise ValueError("Native parameter selection/corner count is invalid")
    selected, paths, identifiers = [], set(), set()
    symbols = set(allowed_symbols or [])
    if any(not re.match(_contract_text(r"^CRESCENDO_NL_[A-Za-z0-9_]+$"), name) for name in symbols):
        raise ValueError("Only exact owned template symbols are permitted")
    for row in rows:
        identifier = _text(row.get("parameter_id"), "identity")
        path = "/".join(_component(row.get(name), name) for name in
                        ("library", "cell", "view", "instance", "property"))
        if identifier in identifiers or path in paths:
            raise ValueError("Native selected parameter identity/path is duplicated")
        identifiers.add(identifier)
        paths.add(path)
        selected.append({"parameter_id": identifier, "path": path,
                         "value": row["value"] if isinstance(row.get("value"), str) and row["value"] in symbols else _number(row.get("value"))})
    enabled, names = [], set()
    for row in corners:
        if isinstance(row, dict):
            if row.get("enabled", True) is False:
                continue
            name = row.get("name")
        else:
            name = row
        name = _text(name, "corner name", 256)
        if name in names:
            raise ValueError("Native enabled corner name is duplicated")
        names.add(name)
        enabled.append(name)
    if len(selected) * (len(enabled) + 1) > MAX_SETTINGS:
        raise ValueError("Native parameter override count exceeds its bound")
    return {"schema_version": SCHEMA,
            "selected": sorted(selected, key=lambda row: row["parameter_id"]),
            "enabled_corners": enabled,
            "inherited_parameters": copy.deepcopy(inherited_parameters or []),
            "owned_paths": list(owned_paths or []),
            "allowed_symbols": sorted(symbols),
            "initial_parameter_policy": "empty_or_exact_previously_owned_paths_only",
            "override_priority": "selected_device_global_and_each_enabled_corner",
            "pending_native_qualification": [
                "private_parameter_path_matches_config_occurrence",
                "inherited_native_parameter_paths_are_remapped_and_preserved",
                "native_job_flow_supports_parameterization_and_callbacks",
                "per_point_effective_and_companion_values_match_generated_scs"],
            "point_execution_proven": False,
            "private_oa_backannotation_proven": False}


def native_parameter_skill_lines(plan, verification_path,
                                 session_expr="session", main_sdb_expr="mainSDB"):
    """Configure owned Parameters in the private Maestro; Run applies callbacks.

    Only an empty source Parameters set or paths registered by an earlier
    successful capture in this same workspace are compatible.  This permits
    enabling the top Parameters switch without activating unrelated entries.
    A later failure taints the caller's private generation workspace.
    """
    if plan.get("schema_version") != SCHEMA:
        raise ValueError("Unknown native device parameter plan")
    raw_rows = []
    for row in plan.get("selected") or []:
        parts = str(row.get("path") or "").split("/")
        if len(parts) != 5:
            raise ValueError("Native private owner parameter path is invalid")
        raw = dict(zip(("library", "cell", "view", "instance", "property"), parts))
        raw.update(parameter_id=row.get("parameter_id"), value=row.get("value"))
        raw_rows.append(raw)
    verified = build_native_parameter_plan(raw_rows, plan.get("enabled_corners"),
        owned_paths=plan.get("owned_paths"), allowed_symbols=plan.get("allowed_symbols"))
    selected_paths = {row["path"] for row in verified["selected"]}
    if any(path not in selected_paths for path in verified["owned_paths"]):
        raise ValueError("Owned native parameter path differs from the selected private design")
    for expression in (session_expr, main_sdb_expr):
        if not isinstance(expression, str) or not _IDENTIFIER.match(expression):
            raise ValueError("Native session/database expression must be an identifier")
    verification_path = _text(verification_path, "verification file", 8192)
    if not os.path.isabs(verification_path):
        raise ValueError("Native parameter verification file must be absolute")
    entries = " ".join("list(%s %s %s)" %
                       (_quote(row["parameter_id"]), _quote(row["path"]), _quote(row["value"]))
                       for row in verified["selected"])
    corners = " ".join(_quote(name) for name in verified["enabled_corners"])
    owned = " ".join(_quote(path) for path in verified["owned_paths"])
    s, db = session_expr, main_sdb_expr
    return [
        _contract_text("let((crescNpOut crescNpRows crescNpCorners crescNpOwned crescNpExisting crescNpBound"),
        _contract_text("     crescNpCallbacks crescNpDisabled crescNpEntry crescNpPath crescNpValue"),
        _contract_text("     crescNpHandle crescNpCorner crescNpActual)"),
        "  unless(isCallable('maeSetParameter) && isCallable('maeGetParameter) &&",
        "         isCallable('maeSetSetup) && isCallable('maeGetSetup) &&",
        "         isCallable('axlGetParameter) && isCallable('axlGetEnabled) &&",
        "         isCallable('axlGetAllParametersDisabled) && isCallable('axlSetAllParametersDisabled)",
        _contract_text('    error("Crescendo native device parameter APIs unavailable; no override applied\\n"))'),
        _contract_text('  crescNpOut=outfile(%s)') % _quote(verification_path),
        _contract_text('  unless(crescNpOut error("Crescendo native parameter records unavailable\\n"))'),
        '  unwindProtect(progn(',
        _contract_text('    fprintf(crescNpOut "CRESCENDO_NATIVE_PARAMETERS_2\\n")'),
        _contract_text("    crescNpBound=boundp('axlExecuteCallbacks)"),
        _contract_text("    crescNpCallbacks=if(crescNpBound then axlExecuteCallbacks else t)"),
        _contract_text("    crescNpDisabled=axlGetAllParametersDisabled(%s)") % db,
        _contract_text('    fprintf(crescNpOut "POLICY\\t%L\\t%L\\t%L\\n" crescNpBound crescNpCallbacks crescNpDisabled)'),
        _contract_text('    unless(crescNpCallbacks==t error("Crescendo native device callbacks disabled/unqualified\\n"))'),
        _contract_text('    unless(crescNpDisabled==t || null(crescNpDisabled) error("Crescendo Parameters status unqualified\\n"))'),
        _contract_text("    crescNpRows=list(%s)") % entries,
        _contract_text("    crescNpCorners=list(%s)") % corners,
        _contract_text("    crescNpOwned=list(%s)") % owned,
        _contract_text('    crescNpExisting=maeGetSetup(?typeName "parameters" ?session %s)') % s,
        _contract_text('    foreach(crescNpPath crescNpExisting'),
        _contract_text('      unless(stringp(crescNpPath) && member(crescNpPath crescNpOwned)'),
        _contract_text('        error("Crescendo existing native Device Parameters need explicit remapping: %L\\n" crescNpPath)))'),
        _contract_text('    when(crescNpDisabled'),
        _contract_text('      unless(axlSetAllParametersDisabled(%s 0) error("Crescendo cannot enable owned Parameters\\n")))') % db,
        _contract_text('    foreach(crescNpEntry crescNpRows'),
        _contract_text('      crescNpPath=cadr(crescNpEntry)'),
        _contract_text('      crescNpValue=caddr(crescNpEntry)'),
        _contract_text('      unless(maeSetParameter(crescNpPath crescNpValue ?typeName "test" ?session %s)') % s,
        _contract_text('        error("Crescendo native global override rejected: %s\\n" crescNpPath))'),
        _contract_text('      crescNpActual=maeGetParameter(crescNpPath ?typeName "test" ?session %s)') % s,
        _contract_text('      fprintf(crescNpOut "SETTING\\t%L\\t%L\\t%L\\t%L\\t%L\\t%L\\n" car(crescNpEntry) crescNpPath "test" nil crescNpActual type(crescNpActual))'),
        _contract_text('      unless(maeSetSetup(?parameters list(crescNpPath) ?enabled t ?session %s)') % s,
        _contract_text('        error("Crescendo selected device parameter enable rejected: %s\\n" crescNpPath))'),
        _contract_text('      crescNpHandle=axlGetParameter(%s crescNpPath)') % db,
        _contract_text('      unless(crescNpHandle && crescNpHandle!=0 && axlGetEnabled(crescNpHandle)'),
        _contract_text('        error("Crescendo selected device parameter is disabled: %s\\n" crescNpPath))'),
        _contract_text('      foreach(crescNpCorner crescNpCorners'),
        _contract_text('        unless(maeSetParameter(crescNpPath crescNpValue ?typeName "corner" ?typeValue list(crescNpCorner) ?session %s)') % s,
        _contract_text('          error("Crescendo native corner override rejected: %s/%s\\n" crescNpCorner crescNpPath))'),
        _contract_text('        crescNpActual=maeGetParameter(crescNpPath ?typeName "corner" ?typeValue crescNpCorner ?session %s)') % s,
        _contract_text('        fprintf(crescNpOut "SETTING\\t%L\\t%L\\t%L\\t%L\\t%L\\t%L\\n" car(crescNpEntry) crescNpPath "corner" crescNpCorner crescNpActual type(crescNpActual))'),
        '      )',
        '    )',
        _contract_text('    when(axlGetAllParametersDisabled(%s) error("Crescendo native Parameters still globally disabled\\n"))') % db,
        _contract_text('    fprintf(crescNpOut "DONE\\t%L\\n" "native_override_settings_only")'),
        '    t',
        _contract_text('  ) close(crescNpOut))'),
        ')',
    ]


def parse_native_parameter_settings(path, plan):
    """Check raw native getter values with physical numeric equivalence."""
    from .workspace import _record_literal, _physical_values_match
    if not os.path.isfile(path) or os.path.islink(path) or os.path.getsize(path) > 4 * 1024 * 1024:
        raise ValueError("Native device override records unavailable or unsafe")
    with open(path, encoding='utf-8') as stream:
        lines = stream.read().splitlines()
    if not lines or lines[0] != _contract_text("CRESCENDO_NATIVE_PARAMETERS_2") or lines[-1] != 'DONE\t"native_override_settings_only"':
        raise ValueError("Native device override readback did not finish")
    expected = {}
    for row in plan["selected"]:
        for scope, corner in [("test", None)] + [("corner", name) for name in plan["enabled_corners"]]:
            expected[(row["parameter_id"], row["path"], scope, corner)] = row["value"]
    seen, records, policy = set(), [], None
    for line in lines[1:-1]:
        fields = line.split("\t")
        if fields[0] == "POLICY" and len(fields) == 4 and policy is None:
            policy = [_record_literal(value) for value in fields[1:]]
            if policy[1] is not True or policy[2] not in (True, None):
                raise ValueError("Native device callback/enable policy is unqualified")
        elif fields[0] == "SETTING" and len(fields) == 7:
            values = [_record_literal(value) for value in fields[1:]]
            key = tuple(values[:4])
            if key not in expected or key in seen:
                raise ValueError("Native device parameter readback identity is duplicated/unexpected")
            if not _physical_values_match(expected[key], values[4]):
                raise ValueError("Native device parameter getter differs: " + key[0])
            seen.add(key)
            records.append(dict(zip(("parameter_id", "path", "scope", "corner", "raw_value", "raw_type"), values)))
        else:
            raise ValueError("Malformed native device override readback")
    if policy is None or set(expected) != seen:
        raise ValueError("Native device override readback is incomplete")
    return {"ok": True, "proof_kind": "native_parameter_settings_only", "records": records,
            "callback_policy": policy, "point_execution_proven": False}


def _native_rdb_input(row, task, parsed):
    """Read one native device input by full private identity and point.

    parameterValue records the chosen Run input, not the value after a PDK
    callback.  It must never qualify callback adjustment or backannotation.
    """
    path = (parsed or {}).get("path")
    if not path or not os.path.isfile(path) or os.path.islink(path) or os.path.getsize(path) > 512 * 1024 * 1024:
        raise ValueError("Native full-identity device input RDB unavailable")
    connection = sqlite3.connect("file:%s?mode=ro" % quote(os.path.abspath(path), safe="/"), uri=True)
    try:
        connection.execute("PRAGMA query_only=ON")
        values = connection.execute('''
            SELECT p.parameterID, pv.value, t.name, p.testID
            FROM parameter p JOIN parameterValue pv ON pv.parameterID=p.parameterID
            LEFT JOIN test t ON t.testID=p.testID
            WHERE p.type='deviceParam' AND p.lib=? AND p.cell=? AND p.view=?
              AND p.instance=? AND p.name=? AND pv.pointID=?
              AND (t.name=? OR p.testID IS NULL OR p.testID='')
        ''', (row["library"], row["cell"], row["view"], row["instance"],
              row["property"], task["point_id"], task["test"])).fetchall()
    except sqlite3.Error as error:
        raise ValueError("Native full-identity device input could not be read: " + str(error))
    finally:
        connection.close()
    if len(values) != 1:
        raise ValueError("Native full-identity device input is absent/ambiguous for " + row["parameter_id"])
    parameter_id, raw, _test, _test_id = values[0]
    number = parse_physical_number(raw)
    if number is None or not math.isfinite(number):
        raise ValueError("Native full-identity device input is not one finite number")
    return {"value": number, "rdb_path": path, "parameter_id": parameter_id,
            "identity": dict((key, row[key]) for key in ("library", "cell", "view", "instance", "property")),
            "point_id": task["point_id"], "test": task["test"],
            "proof_kind": "native_Run_selected_device_input_only"}


def read_native_parameter_points(parameter_rows, tasks, setup=None, parsed=None):
    """Bind each selected private instance field to its exact generated SCS.

    DPAR names are discovered from the instance assignment.  RDB's deviceParam
    name can be only ``r`` for multiple devices and is not a unique binding.
    Original Global variable symbols remain legal and are never rewritten.
    """
    from .private_netlist import _logical_statements, _assignments, _expression_value, top_level_native_parameters
    from .workspace import _effective_property_names
    from .netlist_scope import NetlistScope, BindingError
    tests = dict((row["name"], row) for row in (setup or {}).get("tests") or [])
    result = []
    for row in parameter_rows:
        points = []
        for task in tasks:
            test = tests.get(task["test"], {})
            if (test.get("library") and row.get("design_library") and
                    (test.get("library"), test.get("cell"), test.get("view") or "schematic") !=
                    (row.get("design_library"), row.get("design_cell"), row.get("design_view") or "schematic")):
                continue
            path = task["netlist_path"]
            if not os.path.isfile(path) or os.path.islink(path) or os.path.getsize(path) > 64 * 1024 * 1024:
                raise ValueError("Native selected-parameter SCS unavailable: " + path)
            with open(path, encoding="utf-8") as stream:
                text = stream.read()
            lexical = NetlistScope(text)
            matches = [(index, statement) for index, (library, cell, statement, view) in enumerate(lexical.statements)
                          if library == row["library"] and cell == row["cell"]
                          and (not row.get("view") or view == row["view"])
                          and re.match(r"^%s\s*\(" % re.escape(row["instance"]), statement)]
            statements = [statement for index, statement in matches]
            if len(statements) != 1:
                raise ValueError("Native private instance is absent/ambiguous for %s in %s" % (row["parameter_id"], path))
            aliases = _effective_property_names(row)
            all_assignments = _assignments(statements[0])
            assignments = [(name, expression) for name, expression, _a, _b in all_assignments
                           if name in aliases]
            inferred_dpar, native_input = False, None
            if not assignments:
                same_instance = [item for item in parameter_rows if
                    tuple(item.get(key) for key in ("library", "cell", "view", "instance")) ==
                    tuple(row.get(key) for key in ("library", "cell", "view", "instance"))]
                dpar_names = sorted(set(symbol for _name, value, _a, _b in all_assignments
                    for _scope, symbol in lexical.trace(value, lexical.scopes[matches[0][0]])[2]))
                if len(same_instance) == 1 and len(dpar_names) == 1:
                    name, expression = "$native_device_parameter", dpar_names[0]
                else:
                    # Some PDK callbacks create several independent DPARs for
                    # area/perimeter/width.  A selected field cannot be mapped
                    # to one of those by its numeric value or list order.
                    native_input = _native_rdb_input(row, task, parsed)
                    name, expression = "$native_device_input", str(native_input["value"])
                inferred_dpar = True
            elif len(assignments) == 1:
                name, expression = assignments[0]
            else:
                raise ValueError("Native selected property/alias is ambiguous for %s in %s" % (row["parameter_id"], path))
            scope = lexical.scopes[matches[0][0]]
            effective, dependencies, bindings = lexical.trace(expression, scope)
            lexical.validate_instance_bindings(lexical.last_references)
            if effective is None:
                raise BindingError("unsupported_expression", expression,
                    "Native selected property has no proven point value: " + row["parameter_id"])
            dpars = sorted(set(name for binding_scope, name in bindings))
            if len(dpars) != len(bindings):
                raise BindingError("scope_shadow", expression, "DPAR name resolves to several lexical declarations")
            numbers = {}
            scoped_bindings = []
            for binding_scope, symbol in sorted(bindings):
                value, unused, nested = lexical.trace(symbol, binding_scope)
                if value is None:
                    raise BindingError("binding_unresolved", expression, "Native DPAR binding lacks an exact point value")
                numbers[symbol] = value
                scoped_bindings.append({"name": symbol, "scope": list(binding_scope)})
            points.append({"test": task["test"], "point_id": task["point_id"],
                "corner": task["corner"], "effective": effective,
                "scs_path": path, "netlist_property": name,
                "netlist_expression": expression,
                "statement_preview": statements[0][:192],
                "statement_sha256": hashlib.sha256(statements[0].encode("utf-8")).hexdigest(),
                "dpar_status": "bound_in_generated_point_scs" if dpars else "not_used",
                "dpar_values": dict((symbol, numbers[symbol]) for symbol in dpars),
                "dpar_bindings": scoped_bindings, "lexical_scope": list(scope),
                "effective_kind": "native_device_parameter_value" if inferred_dpar else "netlisted_property_value",
                "callback_adjustment_proven": not inferred_dpar,
                "proof_source": ("full_identity_native_RDB_input" if native_input else
                                 "unique_selected_instance_DPAR" if inferred_dpar else
                                 "generated_point_scs_bound_parameters")})
            if native_input:
                points[-1].update(native_input_proof=native_input,
                    effective_kind="native_device_parameter_input",
                    dpar_status="selected_field_DPAR_binding_unresolved",
                    callback_adjustment_proven=False)
        if not points:
            raise ValueError("No native point covers selected parameter " + row["parameter_id"])
        result.append({"parameter_id": row["parameter_id"], "points": points})
    return result


def verify_native_parameter_application(parameter_rows, tasks, expected_values, setup=None,
                                        parsed=None, allow_effective_adjustment=False):
    from .workspace import _physical_values_match
    parameters = read_native_parameter_points(parameter_rows, tasks, setup, parsed)
    for row in parameters:
        requested = expected_values[row["parameter_id"]]
        row["requested"] = requested
        actual = [point["effective"] for point in row["points"]]
        for point in row["points"]:
            point["requested"] = requested
            point["matches_requested"] = _physical_values_match(requested, point["effective"])
            if not allow_effective_adjustment and not point["matches_requested"]:
                raise ValueError("Native Run changed selected candidate %s at %s/%s: requested=%s effective=%s" %
                                 (row["parameter_id"], point["test"], point["corner"], requested, point["effective"]))
        row["effective"] = actual[0] if all(_physical_values_match(actual[0], value) for value in actual) else None
        row["callback_status"] = "native_Run_parameterization"
    from .native_qualification import NATIVE_QUALIFICATION_REVISION
    return {"ok": True, "proof_kind": "native_run_device_parameters",
            "generation_protocol": GENERATION_PROTOCOL,
            "qualification_revision": NATIVE_QUALIFICATION_REVISION,
            "parameters": parameters, "private_oa_backannotation_proven": False,
            "solver_completion_proven": False}
