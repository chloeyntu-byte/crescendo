"""Saved single-point variable limits and explicit native Global participation.

Expressions stay native.  Passing the saved-scope guard is not proof that a
device expression has a numeric value in a particular test/corner point.
"""
import copy

from .spectre_values import parse_spectre_number


SCHEMA_VERSION = "crescendo.variable_scope_contract.v1"


class VariableScopeContractError(ValueError):
    def __init__(self, report, action="generate SCS"):
        self.report = report
        self.issues = list(report.get("issues") or [])
        labels = ["%s %s%s: %s" % (
            row.get("scope", "variable"), row.get("name", ""),
            " (test %s)" % row["test"] if row.get("test") else "",
            row.get("message") or row.get("code", "unresolved scope"))
            for row in self.issues[:8]]
        ValueError.__init__(self, "Cannot %s: %s" % (action, "; ".join(labels)))


def _flag(value):
    if value is True or value == 1:
        return True
    if value is False or value == 0:
        return False
    if isinstance(value, str):
        value = value.strip().lower()
        if value in ("t", "true", "1", "on", "yes"):
            return True
        if value in ("nil", "(nil)", "false", "0", "off", "no"):
            return False
    return None


def classify_variable_value(value):
    """Classify known scalar/list/sweep syntax without evaluating expressions."""
    from .native_identity import numeric_domain
    number = parse_spectre_number(value)
    if number is not None:
        return {"kind": "scalar", "point_count": 1, "value": number}
    if value is None or isinstance(value, bool) or (isinstance(value, str) and not value.strip()):
        return {"kind": "missing", "point_count": None}
    if isinstance(value, (list, tuple)):
        if not value:
            return {"kind": "missing", "point_count": 0}
        numbers = [parse_spectre_number(item) for item in value]
        if len(value) > 1:
            return {"kind": "multiple", "point_count": len(value)}
        if numbers[0] is not None:
            return {"kind": "scalar", "point_count": 1, "value": numbers[0]}
        return {"kind": "expression", "point_count": None}
    domain = numeric_domain(value)
    if domain is not None:
        if len(domain) == 1:
            return {"kind": "scalar", "point_count": 1, "value": domain[0]}
        # Multiplicity matters even when all list elements are identical.
        return {"kind": "multiple", "point_count": len(domain)}
    if str(value).strip().startswith("{From/To}"):
        return {"kind": "native_sweep", "point_count": None}
    return {"kind": "expression", "point_count": None}


def global_variable_status(setup, variable, test):
    """Return tri-state participation and the evidence used for each layer.

    Optional native evidence lives at setup['native_variable_scope']:
    {read_ok, global_variables_enabled, global_variables: [
        {name, read_ok, enabled, tests: [{test, read_ok, enabled}]}]}.
    Missing native evidence is labelled saved-only; a failed/partial native
    query never becomes an enabled Global or a successful local fallback.
    """
    test_name = test.get("name") if isinstance(test, dict) else str(test)
    dependencies = variable.get("dependent_tests")
    saved_total = _flag(setup.get("global_variables_enabled", True))
    saved_variable = _flag(variable.get("sweep_enabled", True))
    if dependencies is None:
        saved_test = True
        saved_test_source = "saved_default_all_tests"
    else:
        matches = [row for row in dependencies if row.get("test") == test_name]
        flags = [False if row.get("omitted") else _flag(row.get("enabled", True))
                 for row in matches]
        saved_test = True if True in flags else (None if None in flags else False)
        saved_test_source = "saved_dependent_tests"
    saved_layers = {"global_variables_enabled": saved_total,
                    "variable_enabled": saved_variable, "test_enabled": saved_test}
    saved_applies = False if False in saved_layers.values() else (
        True if all(value is True for value in saved_layers.values()) else None)
    result = {"name": variable.get("name"), "test": test_name,
              "applies": saved_applies, "source": "saved_setup",
              "native_verified": False, "saved_applies": saved_applies,
              "saved_layers": saved_layers, "saved_test_source": saved_test_source}
    native = setup.get("native_variable_scope")
    if native is None:
        result["state"] = "enabled" if saved_applies is True else (
            "disabled" if saved_applies is False else "unknown")
        return result
    result.update(source="native_setup", applies=None, state="unknown")
    if not isinstance(native, dict) or native.get("read_ok") is not True:
        result["reason"] = "native_global_scope_read_failed"
        return result
    total = _flag(native.get("global_variables_enabled"))
    layers = {"global_variables_enabled": total}
    result["native_layers"] = layers
    if total is False:
        result.update(applies=False, state="disabled", native_verified=True)
        return result
    if total is not True:
        result["reason"] = "native_global_total_enabled_unknown"
        return result
    rows = [row for row in native.get("global_variables") or []
            if isinstance(row, dict) and row.get("name") == variable.get("name")]
    if len(rows) != 1 or rows[0].get("read_ok") is not True:
        result["reason"] = "native_global_variable_enabled_unknown"
        return result
    enabled = _flag(rows[0].get("enabled"))
    layers["variable_enabled"] = enabled
    if enabled is False:
        result.update(applies=False, state="disabled", native_verified=True)
        return result
    if enabled is not True:
        result["reason"] = "native_global_variable_enabled_unknown"
        return result
    tests = [row for row in rows[0].get("tests") or []
             if isinstance(row, dict) and row.get("test") == test_name]
    if len(tests) != 1 or tests[0].get("read_ok") is not True:
        result["reason"] = "native_global_per_test_enabled_unknown"
        return result
    enabled = _flag(tests[0].get("enabled"))
    layers["test_enabled"] = enabled
    if enabled is None:
        result["reason"] = "native_global_per_test_enabled_unknown"
        return result
    result.update(applies=enabled, state="enabled" if enabled else "disabled", native_verified=True)
    return result


def inspect_variable_scope(setup):
    """Describe saved non-corner multiplicity and outstanding native proof.

    All source Local rows of enabled tests are checked before corner/Global
    overlay.  Explicitly non-participating Globals are inherited but skipped.
    Corner lists are allowed and remain native point dimensions.
    """
    setup = setup or {}
    issues, evidence, variables, participation = [], [], [], []
    tests = [row for row in setup.get("tests") or [] if row.get("enabled", True)]

    def issue(code, scope, row, test, message):
        issues.append({"code": code, "scope": scope, "name": row.get("name"),
                       "test": test, "value": copy.deepcopy(row.get("value")), "message": message})

    def inspect_row(row, scope, test=None, corner=None):
        classified = classify_variable_value(row.get("value"))
        entry = dict(classified, name=row.get("name"), scope=scope, test=test,
                     value=copy.deepcopy(row.get("value")))
        if corner is not None:
            entry["corner"] = corner
        variables.append(entry)
        if scope != "corner" and classified["kind"] in ("multiple", "native_sweep"):
            issue("noncorner_variable_has_multiple_points", scope, row, test,
                  "Global and Local variables must have one value; retain multiple values only in corners")
        elif classified["kind"] == "missing":
            # ADE permits values supplied later by a definitions file or by
            # another effective scope.  A blank saved field is not an error.
            evidence.append({"kind": "point_value_from_native_setup", "scope": scope,
                             "name": row.get("name"), "test": test, "corner": corner,
                             "saved_value": copy.deepcopy(row.get("value"))})
        elif classified["kind"] == "expression":
            evidence.append({"kind": "point_numeric_value", "scope": scope,
                             "name": row.get("name"), "test": test, "corner": corner,
                             "expression": copy.deepcopy(row.get("value"))})

    for test in tests:
        name = test.get("name")
        seen = set()
        for row in test.get("variables") or []:
            if row.get("name") in seen:
                issue("duplicate_variable_definition", "local", row, name, "duplicate Local variable name")
            seen.add(row.get("name"))
            inspect_row(row, "local", name)
        seen = set()
        for row in setup.get("global_variables") or []:
            status = global_variable_status(setup, row, test)
            participation.append(status)
            if not status["native_verified"]:
                evidence.append({"kind": "global_participation", "scope": "global",
                                 "name": row.get("name"), "test": name})
            if status["applies"] is None:
                issue("global_variable_participation_unknown", "global", row, name,
                      status.get("reason") or "saved Global enabled state is unknown")
                continue
            if status["applies"] is False:
                continue
            if row.get("name") in seen:
                issue("duplicate_variable_definition", "global", row, name, "duplicate Global variable name")
            seen.add(row.get("name"))
            inspect_row(row, "global", name)
    for corner in setup.get("corners") or []:
        if corner.get("enabled", True):
            for row in corner.get("variables") or []:
                inspect_row(row, "corner", corner=corner.get("name"))
    return {"schema_version": SCHEMA_VERSION, "ok": not issues,
            "source": "native_and_saved_setup" if setup.get("native_variable_scope") is not None else "saved_setup_only",
            "issues": issues, "variables": variables, "global_participation": participation,
            "native_evidence_required": evidence,
            "native_ready": not issues and not evidence,
            "native_pending": True,
            "effective_point_values_proven": False}


def require_unique_run_variables(setup, action="generate SCS", multiplicity_only=False):
    """Reject known non-corner sweeps; return pending native proof explicitly."""
    report = inspect_variable_scope(setup)
    blocking = [row for row in report["issues"]
                if not multiplicity_only or row["code"] == "noncorner_variable_has_multiple_points"]
    report["multiplicity_ok"] = not any(
        row["code"] == "noncorner_variable_has_multiple_points" for row in report["issues"])
    if blocking:
        failure_report = dict(report, issues=blocking)
        if multiplicity_only:
            failure_report["source_issues"] = report["issues"]
        raise VariableScopeContractError(failure_report, action)
    return report
