"""Explicit design controls and scope-proven priority over device dimensions."""
from .runtime import bind_text
_contract_text = bind_text()

import copy
import re

from .hierarchy import parse_physical_number
from .variable_scope_contract import global_variable_status, require_unique_run_variables

SCHEMA = _contract_text("crescendo.design_variables.v1")


def is_design(row):
    return row.get("source_kind") in ("global", "top")


def catalog(setup, hierarchy):
    result = []
    for test in setup.get("tests") or []:
        if test.get("enabled", True) is False:
            continue
        effective = {}
        identity = tuple(test.get(k) or "schematic" for k in ("library", "cell", "view"))
        for design in hierarchy.get("designs") or []:
            raw = design.get("design") or ()
            key = tuple(raw.get(k) for k in ("library", "cell", "view")) if isinstance(raw, dict) else tuple(raw)
            if key == identity:
                for row in design.get("hierarchy", {}).get("top_parameters") or []:
                    effective[row["name"]] = (row, "top", "top cell")
        for row in test.get("variables") or []:
            effective[row["name"]] = (row, "top", "test")
        for row in setup.get("global_variables") or []:
            status = global_variable_status(setup, row, test)
            if status["applies"] is True:
                effective[row["name"]] = (row, "global", "setup global")
        # This recognizes saved arithmetic only in this Test's effective scope;
        # the resolved number is never used as a native point/seed claim.
        from .private_netlist import resolve_native_parameters
        expressions = [{"name": name, "expression": str(row.get("value", row.get("current_value")))}
                       for name, (row, kind, origin) in effective.items()]
        resolved = {row["name"]: row.get("value") for row in resolve_native_parameters(expressions)}
        for name, (row, kind, origin) in sorted(effective.items()):
            value = row.get("value", row.get("current_value"))
            number = parse_physical_number(value)
            expression = number is None and resolved.get(name) is not None
            valid = (number is not None or expression) and bool(re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name))
            result.append(dict(parameter_id="design_variable:%s:%s:%s:%s:%s" % (identity + (test["name"], name)),
                backend="design_variable", source=origin, source_kind=kind, variable_test=test["name"],
                name=name, prompt=origin, current_value=value, default_value=value, numeric_value=number,
                hierarchy_path="$design_variables/" + test["name"], owner_occurrence_path="/",
                owner_library=identity[0], owner_cell=identity[1], owner_view=identity[2],
                design_library=identity[0], design_cell=identity[1], design_view=identity[2],
                tunable=valid, optimizable=valid, editable=True, display=True, default_selected=False,
                cdf_type="string", value_type="float", value_kind="number" if number is not None else "expression",
                candidate_expression=expression, requires_point_numeric_value=expression))
    return result


def require_no_corner_conflicts(setup, parameters):
    require_unique_run_variables(setup, action="optimize design variables", multiplicity_only=True)
    names = {r["name"] for r in parameters if is_design(r)}
    for corner in setup.get("corners") or []:
        if corner.get("enabled", True):
            for row in corner.get("variables") or []:
                if row.get("name") in names:
                    raise ValueError("Design variable conflict: corner %s defines variable %s" % (corner["name"], row["name"]))


def working_row(parameter, value):
    row = copy.deepcopy(parameter)
    row.update(property=parameter["name"], value=value)
    return row


def read_design_points(row, tasks):
    from .netlist_scope import NetlistScope, BindingError
    points = []
    for task in tasks:
        if task["test"] != row["variable_test"]:
            continue
        with open(task["netlist_path"], encoding="utf-8") as stream:
            lexical = NetlistScope(stream.read())
        name = row["name"]
        if lexical.lookup((), name) != ((), name):
            raise BindingError("binding_unresolved", name, "Selected design variable missing from top parameters: " + name)
        value, unused, bindings = lexical.trace(name, ())
        if value is None:
            raise ValueError("Selected design variable is not numeric: " + name)
        points.append(dict(test=task["test"], point_id=task["point_id"], corner=task["corner"],
            effective=value, scs_path=task["netlist_path"], netlist_property=name,
            netlist_expression=lexical.declarations[()][name], dpar_values={name: value},
            dpar_bindings=[dict(name=name, scope=[])], dpar_status="bound_design_variable",
            effective_kind="top_parameter_value", callback_adjustment_proven=False))
    if not points:
        raise ValueError("No native point covers design variable " + row["parameter_id"])
    return {"parameter_id": row["parameter_id"], "source_kind": row["source_kind"], "points": points}


def priority_report(parameters, working_rows, tasks):
    """Use P0 owner/view/instance aliases and lexical declaration identities."""
    from .netlist_scope import NetlistScope
    from .private_netlist import _assignments
    from .workspace import _effective_property_names
    controls = [r for r in parameters if is_design(r)]
    locked, warnings, evidence = set(), [], []
    for task in tasks:
        with open(task["netlist_path"], encoding="utf-8") as stream:
            lexical = NetlistScope(stream.read())
        bindings = {((), r["name"]): r for r in controls if r["variable_test"] == task["test"]}
        for row in working_rows:
            if is_design(row):
                continue
            matches = [(i, statement) for i, (lib, cell, statement, view) in enumerate(lexical.statements)
                if (lib, cell) == (row["library"], row["cell"]) and (not row.get("view") or view == row["view"])
                and re.match(r"^%s\s*\(" % re.escape(row["instance"]), statement)]
            if len(matches) != 1:
                raise ValueError("Cannot prove design-variable priority for " + row["parameter_id"])
            i, statement = matches[0]
            assignments = [(n, v) for n, v, a, z in _assignments(statement) if n in _effective_property_names(row)]
            if len(assignments) != 1:
                raise ValueError("Cannot resolve selected device alias for " + row["parameter_id"])
            lexical.trace(assignments[0][1], lexical.scopes[i])
            refs = set(lexical.last_references)
            # A CDF callback may have exported a numeric field. Resolve the
            # saved numeric expression in this exact owner scope as well.
            original = row.get("baseline_expression")
            if isinstance(original, str) and parse_physical_number(original) is None:
                numeric, unused, unused_bindings = lexical.trace(original, lexical.scopes[i])
                if numeric is None:
                    raise ValueError("Cannot resolve original device expression in its native scope: " + row["parameter_id"])
                refs.update(lexical.last_references)
            lexical.validate_instance_bindings(refs)
            for binding in sorted(refs & set(bindings)):
                control = bindings[binding]
                locked.add(row["parameter_id"])
                proof = dict(variable=control["parameter_id"], name=control["name"],
                    device_parameter=row["parameter_id"], property=row["property"],
                    hierarchy_path=row.get("hierarchy_path"), test=task["test"], corner=task["corner"],
                    scs_path=task["netlist_path"], lexical_binding=[[], binding[1]])
                evidence.append(proof)
                warning = "%s -> %s/%s: device dimension locked by design variable" % (control["name"], row.get("hierarchy_path"), row["property"])
                if warning not in warnings:
                    warnings.append(warning)
    return dict(schema_version=SCHEMA, locked_parameter_ids=sorted(locked), warnings=warnings, evidence=evidence)


def qualify_priority(prepared, parameters, config, root, spec):
    from .setup_private_session import borrow_setup_private_session, write_session_reference
    from .private_netlist import capture_unmodified_baseline, _capture_request_config
    from .workspace import _schematic_parameter_rows
    require_no_corner_conflicts(prepared["setup"], parameters)
    capture_config = _capture_request_config(prepared, config, spec)
    with borrow_setup_private_session(prepared, capture_config, root) as workspace:
        if workspace is None:
            raise ValueError("Design-variable priority requires an applied working copy")
        # Capture only: the owned simulator wrapper exits without solving.
        baseline = capture_unmodified_baseline(workspace, parameters, capture_config, root,
            emit_clones=False, private_config_initialized=False)
        write_session_reference(root, prepared["setup_private_session"])
        unused, rows = _schematic_parameter_rows(workspace, parameters,
            {r["parameter_id"]: None for r in parameters}, clone_state="existing", render_values=False)
        return priority_report(parameters, rows, baseline["tasks"])


def candidate_setup(setup, parameters, values):
    """Candidate-only expected values; saved setup/scope multiplicity is unchanged."""
    controls = [r for r in parameters if is_design(r)]
    if not controls:
        return setup
    result = copy.deepcopy(setup)
    for control in controls:
        name = control["name"]
        value = "%.17g" % values[control["parameter_id"]]
        for test in result.get("tests") or []:
            if test["name"] == control["variable_test"]:
                rows = [r for r in test.get("variables") or [] if r["name"] != name]
                test["variables"] = rows + [dict(name=name, value=value)]
        for row in result.get("global_variables") or []:
            if row["name"] == name:
                row["value"] = value
    return result


def native_parameter(row):
    return row.get("backend", "schematic") == "schematic" or (row.get("backend") == "design_variable" and is_design(row))


def normalize_owned_globals(original, private, owned):
    """Undo only a preceding point-proven candidate value for comparison."""
    from .workspace import _physical_values_match
    result = copy.deepcopy(private)
    proofs = []
    expected = {r["name"]: r for r in original.get("global_variables") or []}
    for previous in owned or []:
        name = previous["name"]
        matches = [r for r in result.get("global_variables") or [] if r["name"] == name]
        if len(matches) != 1 or not _physical_values_match(previous["value"], matches[0].get("value")):
            raise ValueError("Previous owned design-variable value changed: " + name)
        row = matches[0]
        if name not in expected:
            # A selected top/Test control was temporarily promoted to Global.
            # Permit only the exact native shape, enabled for this one Test.
            content = row.get("native_content") or {}
            groups = content.get("children") or []
            dependencies = row.get("dependent_tests")
            if (row.get("sweep_enabled") is not True or content.get("tag") != "var"
                    or content.get("text") != name or content.get("attributes") not in ({}, {"enabled": "1"})
                    or any(g.get("tag") not in ("value", "dependenttests") for g in groups)
                    or any(r.get("test") != previous["variable_test"] or not r.get("enabled", True) or r.get("omitted") for r in dependencies or [])):
                raise ValueError("Owned top/Test Global metadata is unqualified: " + name)
            result["global_variables"].remove(row)
        else:
            row["value"] = expected[name]["value"]
            values = [g for g in (row.get("native_content") or {}).get("children") or [] if g.get("tag") == "value"]
            source_values = [g for g in (expected[name].get("native_content") or {}).get("children") or [] if g.get("tag") == "value"]
            if len(values) != 1 or len(source_values) != 1:
                raise ValueError("Owned design-variable native value shape changed: " + name)
            # Preserve every non-value bit; the ordinary semantic comparison
            # still rejects enable/scope/metadata changes.
            values[0]["text"] = source_values[0]["text"]
        proofs.append(dict(name=name, previous_verified_value=previous["value"],
                           source_kind=previous["source_kind"], variable_test=previous["variable_test"]))
    return result, proofs
