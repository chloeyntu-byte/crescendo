"""Evaluate original ADE Calculator expressions against one owned CLI PSF.

This is an Ocean bridge, not a Python reimplementation of Calculator or an
attempt to write a proprietary RDB.  Its public result has the same in-memory
shape consumed by :mod:`metrics`, while its provenance explicitly says CLI.
"""
from __future__ import print_function

import math
import json
import os
import re
import uuid

from .cadence import build_guardian_command, child_environment, run_command
from .legacy_guardian import (
    acquire_legacy_guardian_launch_lock, close_legacy_guardian_launch_lock,
    legacy_guardian_parent_identity, write_legacy_guardian_manifest,
)
from .resources import guardian_script_path
from .utils import atomic_write_json, atomic_write_text, ensure_dir, is_under


SCHEMA = "crescendo.netlist_measurement.v1"
PROTOCOL = "CRESCENDO_NETLIST_MEASURE_1"
_EVALUATOR_LOCALS = set(("crescPort", "crescOpened", "crescValue", "crescPrinted", "crescFailed", "crescParam", "crescVarFailed",
                         "crescAnalyses", "crescAnalysis", "crescAnalysisIndex", "crescMissingVars", "crescVariable", "crescBlockedVars"))
CAPABILITY_SCHEMA = "crescendo.result_reader_capability.v1"


class NetlistMeasurementError(RuntimeError):
    def __init__(self, message, detail=None, infrastructure=True):
        super(NetlistMeasurementError, self).__init__(message)
        self.detail = detail or {}
        self.infrastructure = bool(infrastructure)


def run_owned_command(argv, cwd, env, timeout_seconds, control_root, token,
                      cancel_check=None, command_runner=None, before_launch=None):
    """Reuse the existing durable guardian launch and cleanup contract."""
    ensure_dir(control_root)
    state_path = os.path.join(control_root, "guardian_state.json")
    stdout_path = os.path.join(control_root, "ocean.stdout")
    stderr_path = os.path.join(control_root, "ocean.stderr")
    descriptor, lock_path = acquire_legacy_guardian_launch_lock(control_root)
    try:
        identity = legacy_guardian_parent_identity()
        guardian_argv = build_guardian_command(
            argv, cwd, stdout_path, stderr_path, state_path, token,
            termination_grace_seconds=6, guardian_launch_lock_fd=descriptor)
        write_legacy_guardian_manifest(
            control_root, cwd, state_path, stdout_path, stderr_path, token, argv,
            identity["pid"], guardian_script_path(), launch_lock_path=lock_path,
            expected_parent_boot_id=identity["boot_id"],
            expected_parent_start_ticks=identity["start_ticks"], expected_uid=identity["uid"],
            guardian_argv=guardian_argv)
        if before_launch:
            before_launch({"attempt_root": control_root, "cwd": cwd, "token": token})
        owned_descriptor = descriptor
        descriptor = None
        runner = command_runner or run_command
        result = runner(argv, cwd=cwd, env=env, timeout_seconds=timeout_seconds,
                        stdout_path=stdout_path, stderr_path=stderr_path, cancel_check=cancel_check,
                        termination_grace_seconds=6, termination_kill_seconds=2,
                        guardian_state_path=state_path, guardian_token=token,
                        guardian_launch_lock_fd=owned_descriptor)
        result["guardian_token"] = token
        return result
    finally:
        close_legacy_guardian_launch_lock(descriptor)


def _quote(value):
    return '"%s"' % str(value).replace("\\", "\\\\").replace('"', '\\"').replace(
        "\n", "\\n").replace("\r", "\\r").replace("\t", "\\t")


def _startup_lines(config):
    """Load only explicitly configured native extensions, before reader choice."""
    paths = ((config or {}).get("cadence") or {}).get("startup_extensions") or []
    if not isinstance(paths, (list, tuple)):
        raise NetlistMeasurementError("Native startup extensions must be an explicit file list")
    lines, seen = [], set()
    for path in paths:
        if not isinstance(path, str) or not os.path.isabs(path) or not os.path.isfile(path):
            raise NetlistMeasurementError("Native startup extension is unavailable: " + str(path))
        path = os.path.realpath(path)
        if path in seen:
            continue
        seen.add(path)
        lines.append('unless(errset(load(%s) t) error("Crescendo native result-reader startup failed\\n"))' % _quote(path))
    return lines


def _output_references(expression, names):
    # Only bare value references are output aliases. Strings, quoted analysis
    # symbols, function calls and keyword arguments retain native meanings.
    expression = re.sub(r'"(?:\\.|[^"\\])*"', '""', expression or "")
    expression = re.sub(r";[^\n]*", "", expression)
    expression = re.sub(r"'[A-Za-z_][A-Za-z0-9_]*", "", expression)
    result = set()
    for match in re.finditer(r"\b[A-Za-z_][A-Za-z0-9_]*\b", expression):
        name = match.group(0)
        if name not in names or (match.start() and expression[match.start() - 1] in "?.'"):
            continue
        if expression[match.end():].lstrip().startswith("("):
            continue
        result.add(name)
    return result


def evaluation_plan(task, required_output_keys=None):
    """Topologically order same-test Calculator aliases, including waves."""
    requested = _requested_outputs(task, required_output_keys)
    rows = _requested_outputs(task, None)
    if len(rows) > 16384:
        raise NetlistMeasurementError("Too many output expressions to qualify")
    aliases = dict((row["name"], row) for row in rows
                   if isinstance(row.get("name"), str) and re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", row["name"]))
    dependencies = dict((row["key"], sorted(_output_references(row.get("expression") or row.get("expr") or "", aliases))) for row in rows)
    visiting, visited, ordered = set(), set(), []

    def visit(row, depth=0):
        key = row["key"]
        if key in visited:
            return
        if key in visiting:
            raise NetlistMeasurementError("Output dependency cycle: %s" % key)
        if depth > 512:
            raise NetlistMeasurementError("Output dependency chain is too deep")
        visiting.add(key)
        for name in dependencies[key]:
            visit(aliases[name], depth + 1)
        visiting.remove(key)
        visited.add(key)
        ordered.append(row)
    for row in requested:
        visit(row)
    return ordered, requested


def _expression_variables(expression, output_key):
    """Read literal VAR dependencies without treating strings as expressions."""
    names = set()
    tokens_pattern = re.compile(r'"(?:\\.|[^"\\])*"|;[^\n]*|\b[A-Za-z_][A-Za-z0-9_]*\b|\S')
    tokens = [m.group(0) for m in tokens_pattern.finditer(expression or "") if not m.group(0).startswith(";")]
    for index, token in enumerate(tokens):
        if token != "VAR" or tokens[index + 1:index + 2] != ["("]:
            continue
        tail = tokens[index + 2:index + 4]
        if len(tail) != 2 or not tail[0].startswith('"') or tail[1] != ")":
            raise NetlistMeasurementError("Dynamic VAR name is unsupported by the common CLI evaluator", {"output_key": output_key})
        try:
            name = json.loads(tail[0])
        except (ValueError, TypeError):
            raise NetlistMeasurementError("VAR name is not a supported literal", {"output_key": output_key})
        if not isinstance(name, str) or not name:
            raise NetlistMeasurementError("VAR name is empty", {"output_key": output_key})
        names.add(name)
    return names


def output_variable_dependencies(task, required_output_keys=None):
    """Include transitive output aliases when deciding safe diagnostic reads."""
    ordered, unused = evaluation_plan(task, required_output_keys)
    aliases = {row["name"]: row for row in ordered if row.get("name")}
    dependencies = {}
    for output in ordered:
        expression = output.get("expression") or output.get("expr") or ""
        names = _expression_variables(expression, output["key"])
        for name in _output_references(expression, aliases):
            names.update(dependencies[aliases[name]["key"]])
        dependencies[output["key"]] = names
    return dependencies


def required_variables(task, required_output_keys=None):
    """Constant VAR names and required actual ALPS candidate sizing."""
    dependencies = output_variable_dependencies(task, required_output_keys)
    names = set(name for values in dependencies.values() for name in values)
    if (task.get("simulator_contract") or {}).get("execution_simulator", task.get("execution_simulator")) == "alps":
        # Calculator constants must not stand in for actual candidate sizing.
        # Read the selected symbols and qualified top-level values from PSF.
        for row in task.get("variables") or []:
            if isinstance(row, dict) and str(row.get("name", "")).startswith("CRESCENDO_NL_"):
                names.add(row["name"])
        for row in task.get("native_parameters") or []:
            if row.get("name") and isinstance(row.get("value"), (int, float)) and not isinstance(row.get("value"), bool):
                names.add(row["name"])
        if not names:
            raise NetlistMeasurementError("ALPS result qualification requires actual native parameter readback")
    return sorted(names)


def qualify_expressions(tasks, required_output_keys=None):
    """Reject ADE context dependencies rather than silently evaluate another TB.

Ordinary waveform, RF and user Calculator functions still run in native Ocean.
Cross-test/output references and evaluation scripts need a separate dependency
adapter.  Their qualification failure affects this selected backend only.
"""
    required = None if required_output_keys is None else set(required_output_keys)
    reasons, seen = [], set()
    for task in tasks or []:
        test = task.get("test") or task.get("test_name")
        task = dict(task, test=test)
        try:
            ordered, requested = evaluation_plan(task, required)
            required_variables(task, required)
        except NetlistMeasurementError as error:
            reasons.append("output_dependency:%s:%s" % (test, error))
            continue
        seen.update(row["key"] for row in requested)
        for output in ordered:
            name = output.get("name")
            key = output.get("key") or "%s::%s" % (test, name)
            if name in _EVALUATOR_LOCALS:
                reasons.append("reserved_output_alias:%s" % key)
            expression = output.get("expression") or output.get("expr")
            if not isinstance(expression, str) or not expression.strip():
                reasons.append("missing_output_expression:%s" % key)
                continue
            if re.search(r"\b(calcVal|valueFromTest|ocnxlGetOutputValue|axlGetOutputValue)\s*\(", expression):
                reasons.append("cross_test_output:%s" % key)
            if re.search(r"\?(test|history|run)\b", expression):
                reasons.append("external_result_context:%s" % key)
            output_type = str(output.get("output_type") or output.get("type") or "").lower()
            if output_type in ("script", "ocean_script", "skill_script"):
                reasons.append("output_script:%s" % key)
    if required is not None:
        reasons.extend("missing_output:%s" % key for key in sorted(required - seen))
    return {"supported": not reasons, "reasons": sorted(set(reasons)),
            "evaluation": "native_ocean_calculator", "required_output_keys": sorted(required or seen)}


def _requested_outputs(task, required_output_keys=None):
    required = None if required_output_keys is None else set(required_output_keys)
    outputs, seen = [], set()
    for row in task.get("outputs") or []:
        output = dict(row)
        output["key"] = output.get("key") or "%s::%s" % (task["test"], output.get("name"))
        if required is not None and output["key"] not in required:
            continue
        if output["key"] in seen:
            raise NetlistMeasurementError("Duplicate output identity: %s" % output["key"])
        seen.add(output["key"])
        outputs.append(output)
    return outputs


def render_measurement_script(task, records_path, token, required_output_keys=None):
    """Return an Ocean program with a bounded, identity-fenced scalar protocol."""
    outputs = _requested_outputs(task, required_output_keys)
    ordered, unused = evaluation_plan(task, required_output_keys)
    variables_required = required_variables(task, required_output_keys)
    variable_dependencies = output_variable_dependencies(task, required_output_keys)
    output_indexes = dict((row["key"], index) for index, row in enumerate(outputs))
    lines = ["; Original ADE expressions; no simulation or source writes.",
             "procedure(crescEvaluatePrivateNetlist()",
             "  let((crescPort crescOpened crescValue crescPrinted crescFailed crescParam crescVarFailed crescAnalyses crescAnalysis crescAnalysisIndex crescMissingVars crescVariable crescBlockedVars)",
             "    crescFailed = nil", "    crescVarFailed = nil", "    crescMissingVars = nil"]
    reader = (task.get("simulator_contract") or {}).get("result_reader", "spectre")
    if reader not in ("spectre", "alps"):
        raise NetlistMeasurementError("Unqualified simulator result reader")
    for path in task.get("ocean_loads") or []:
        lines.append("    load(%s)" % _quote(path))
    lines.extend(["    unless(errset(simulator('%s) t) error(\"Crescendo requested Ocean simulator reader is unavailable\\n\"))" % reader])
    # Expected/input values must never seed Calculator VAR. Only successful
    # current-result pv reads below may define a design variable in this Ocean.
    lines.extend([
        "    crescPort = outfile(%s)" % _quote(records_path),
        '    unless(crescPort error("Crescendo measurement records cannot open\\n"))',
        '    fprintf(crescPort "%s\\t%s\\n")' % (PROTOCOL, token),
        "    crescOpened = errset(openResults(%s) t)" % _quote(task["psf_dir"]),
        '    unless(crescOpened && car(crescOpened)',
        '      fprintf(crescPort "O\\t0\\tunavailable\\n")',
        '      fprintf(crescPort "END\\t%d\\n")' % len(outputs),
        '      close(crescPort)',
        '      printf("Crescendo candidate PSF cannot open\\n")',
        '      exit(4)', "    )",
        '    fprintf(crescPort "O\\t0\\topened\\n")',
        "    crescAnalyses = errset(results() t)",
        "    if(crescAnalyses && listp(car(crescAnalyses)) then",
        "      crescAnalysisIndex = 0",
        "      foreach(crescAnalysis car(crescAnalyses)",
        "        when(crescAnalysisIndex < 256",
        '          fprintf(crescPort "A\\t%d\\t%L\\n" crescAnalysisIndex crescAnalysis)',
        "        )",
        "        crescAnalysisIndex = crescAnalysisIndex + 1",
        "      )",
        "      if(crescAnalysisIndex <= 256 then",
        '        fprintf(crescPort "I\\t0\\tavailable\\n")',
        "      else", '        fprintf(crescPort "I\\t0\\ttruncated\\n")', "      )",
        "    else", '      fprintf(crescPort "I\\t0\\tunavailable\\n")', "    )",
    ])
    if task.get("analysis"):
        lines.append("    selectResult(%s)" % _quote(task["analysis"]))
    for index, name in enumerate(variables_required):
        lines.extend([
            "    crescParam = errset(pv(\"top-level\" %s ?result 'designParamVals) t)" % _quote(name),
            "    if(crescParam && numberp(car(crescParam)) then",
            "      desVar(%s car(crescParam))" % _quote(name),
            '      fprintf(crescPort "P\\t%d\\t%%.17g\\n" car(crescParam))' % index,
            "    else", '      fprintf(crescPort "X\\t%d\\tunavailable_design_parameter\\n")' % index,
            "      crescMissingVars = cons(%s crescMissingVars)" % _quote(name),
            "      crescVarFailed = t", "    )",
        ])
    for output in ordered:
        if variable_dependencies[output["key"]]:
            lines.extend(["    crescBlockedVars = setof(crescVariable list(%s) member(crescVariable crescMissingVars))" %
                          " ".join(_quote(name) for name in sorted(variable_dependencies[output["key"]])),
                          "    if(crescBlockedVars then"])
            if output["key"] in output_indexes:
                lines.append('      fprintf(crescPort "E\\t%d\\tparameter_metadata_unavailable\\n")' % output_indexes[output["key"]])
            lines.append("    else")
        lines.append("    crescValue = errset(evalstring(%s) t)" % _quote(output.get("expression") or output.get("expr")))
        name = output.get("name") or ""
        if re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name):
            lines.append("    when(crescValue set('%s car(crescValue)))" % name)
        if output["key"] in output_indexes:
            index = output_indexes[output["key"]]
            lines.extend([
                "    if(crescValue && numberp(car(crescValue)) then",
                '      crescPrinted = errset(fprintf(crescPort "V\\t%d\\t%%.17g\\n" car(crescValue)) t)' % index,
                "      unless(crescPrinted crescFailed = t)",
                "    else", '      fprintf(crescPort "E\\t%d\\tnon_scalar_or_expression_error\\n")' % index,
                "      crescFailed = t", "    )",
            ])
        if variable_dependencies[output["key"]]:
            lines.append("    )")
    lines.extend(['    fprintf(crescPort "END\\t%d\\n")' % len(outputs),
                  "    close(crescPort)", "    if(crescVarFailed then exit(3) else if(crescFailed then exit(2) else exit(0)))",
                  "  )", ")", "crescEvaluatePrivateNetlist()", ""])
    return "\n".join(lines), outputs


def _reader_capability(task, token, outputs, variable_names, parameters,
                       missing_parameters, values, errors, analyses, inventory, open_status):
    required = list(variable_names)
    actual = {required[index]: value for index, value in parameters.items()}
    missing = [name for index, name in enumerate(required) if index not in parameters]
    dependencies = output_variable_dependencies(task, [row["key"] for row in outputs])
    metadata_ok = open_status != "unavailable" and not missing and len(parameters) == len(required)
    reader = (task.get("simulator_contract") or {}).get("result_reader", "spectre")
    simulator = (task.get("simulator_contract") or {}).get("execution_simulator", task.get("execution_simulator", "spectre"))
    diagnostics = []
    for index, output in enumerate(outputs):
        error = errors.get(index)
        status = ("PASS" if index in values else "BLOCKED" if error == "parameter_metadata_unavailable"
                  else "FAIL" if error else "NOT_TESTED")
        diagnostics.append({"key": output["key"], "status": status, "value": values.get(index),
                            "error": error, "requires_variables": sorted(dependencies[output["key"]])})
    return {"schema_version": CAPABILITY_SCHEMA, "token": token,
            "task_id": task.get("task_id"), "test": task.get("test"), "corner": task.get("corner"),
            "point_id": task.get("point_id"), "execution_simulator": simulator,
            "result_reader": reader, "psf_dir": task.get("psf_dir"),
            "netlist_sha256": task.get("netlist_sha256"),
            "open_results_status": open_status,
            "analysis_names": analyses, "analysis_inventory_status": inventory,
            "native_parameter_readback": {"source": "current_psf_designParamVals", "required": required,
                "actual": actual, "missing": missing, "ok": metadata_ok,
                "missing_reasons": {required[index]: reason for index, reason in missing_parameters.items()}},
            "output_diagnostics": diagnostics,
            "candidate_qualification": {"ok": False,
                "reason": ("current_result_cannot_open" if open_status == "unavailable" else
                           "diagnostic_readback_is_not_candidate_completion" if metadata_ok else "required_current_result_parameters_missing")}}


def parse_measurement_records(path, token, outputs, task, variable_names=None, variable_values=None, capability=None):
    if not os.path.isfile(path) or os.path.islink(path) or os.path.getsize(path) > 1024 * 1024:
        raise NetlistMeasurementError("Measurement protocol is missing or invalid", {"path": path})
    with open(path, "r", encoding="utf-8") as stream:
        rows = stream.read().splitlines()
    if not rows or rows[0] != PROTOCOL + "\t" + token:
        raise NetlistMeasurementError("Measurement identity does not match this candidate")
    if rows[-1] != "END\t%d" % len(outputs):
        raise NetlistMeasurementError("Measurement protocol did not finish")
    values, errors, parameters, missing_parameters, analyses = {}, {}, {}, {}, []
    inventory = "not_recorded"
    open_status = "not_recorded"
    variable_names = list(variable_names or [])
    for raw in rows[1:-1]:
        fields = raw.split("\t")
        if len(fields) != 3 or fields[0] not in ("V", "E", "P", "X", "A", "I", "O"):
            raise NetlistMeasurementError("Malformed measurement protocol")
        try:
            index = int(fields[1])
        except ValueError:
            raise NetlistMeasurementError("Malformed measurement index")
        if fields[0] == "O":
            if index != 0 or open_status != "not_recorded" or fields[2] not in ("opened", "unavailable"):
                raise NetlistMeasurementError("Unexpected openResults identity")
            open_status = fields[2]
            continue
        if fields[0] == "A":
            if index != len(analyses) or index >= 256 or not fields[2] or len(fields[2]) > 4096:
                raise NetlistMeasurementError("Unexpected analysis inventory identity")
            name = fields[2]
            if name.startswith('"'):
                try:
                    name = json.loads(name)
                except ValueError:
                    raise NetlistMeasurementError("Malformed analysis inventory name")
            if not isinstance(name, str) or not name or any(ord(char) < 32 for char in name):
                raise NetlistMeasurementError("Malformed analysis inventory name")
            analyses.append(name)
            continue
        if fields[0] == "I":
            if index != 0 or inventory != "not_recorded" or fields[2] not in ("available", "unavailable", "truncated"):
                raise NetlistMeasurementError("Unexpected analysis inventory status")
            inventory = fields[2]
            continue
        if fields[0] in ("P", "X"):
            if index < 0 or index >= len(variable_names) or index in parameters or index in missing_parameters:
                raise NetlistMeasurementError("Unexpected design parameter identity")
            if fields[0] == "X":
                missing_parameters[index] = fields[2]
                continue
            try:
                parameter = float(fields[2])
            except ValueError:
                raise NetlistMeasurementError("Required design parameter is not numeric")
            if not math.isfinite(parameter):
                raise NetlistMeasurementError("Required design parameter is not finite")
            parameters[index] = parameter
            continue
        if index < 0 or index >= len(outputs) or index in values or index in errors:
            raise NetlistMeasurementError("Duplicate or unexpected measurement output")
        if fields[0] == "E":
            errors[index] = fields[2]
        else:
            try:
                value = float(fields[2])
            except ValueError:
                raise NetlistMeasurementError("Measurement is not a numeric scalar")
            if not math.isfinite(value):
                raise NetlistMeasurementError("Measurement is not a finite scalar")
            values[index] = value
    if open_status == "unavailable" and (values or errors or parameters or missing_parameters or analyses or inventory != "not_recorded"):
        raise NetlistMeasurementError("Conflicting unavailable openResults protocol")
    reader_capability = _reader_capability(task, token, outputs, variable_names, parameters,
                                          missing_parameters, values, errors, analyses, inventory, open_status)
    if capability is not None:
        capability.update(reader_capability)
    if open_status == "unavailable":
        raise NetlistMeasurementError("Selected native result reader cannot open the current owned result", {
            "failure_stage": "result_reader_open_results", "path": path,
            "result_reader_capability": reader_capability})
    if missing_parameters or len(parameters) != len(variable_names):
        raise NetlistMeasurementError("Required VAR data or candidate parameter metadata is unavailable in the current result; shared CLI candidate qualification is blocked", {
            "failure_stage": "result_reader_parameter_metadata", "path": path,
            "variables": reader_capability["native_parameter_readback"]["missing"],
            "result_reader_capability": reader_capability})
    if variable_values is not None:
        variable_values.update((variable_names[index], value) for index, value in parameters.items())
    if len(values) + len(errors) != len(outputs):
        raise NetlistMeasurementError("Measurement output is missing")
    result = []
    for index, output in enumerate(outputs):
        result.append(dict(output, value=values.get(index), error=errors.get(index),
                           test=task["test"], point_id=task["point_id"],
                           design_point=task.get("design_point", task["point_id"]), corner=task["corner"]))
    return result


def verify_actual_parameters(task, effective_variables):
    """ALPS acceptance requires current-result sizing, not input substitutes."""
    if (task.get("simulator_contract") or {}).get("execution_simulator", task.get("execution_simulator")) != "alps":
        return None
    expected = {}
    for row in task.get("native_parameters") or []:
        if row.get("name") and isinstance(row.get("value"), (int, float)) and not isinstance(row.get("value"), bool):
            expected[row["name"]] = row["value"]
    for row in task.get("variables") or []:
        if str(row.get("name", "")).startswith("CRESCENDO_NL_"):
            expected[row["name"]] = row.get("value")
    if not expected:
        raise NetlistMeasurementError("ALPS current results have no qualified parameter identity")
    for name, value in expected.items():
        actual = effective_variables.get(name)
        if (isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value)
                or isinstance(actual, bool) or not isinstance(actual, (int, float)) or not math.isfinite(actual)
                or abs(float(actual) - float(value)) > max(abs(float(value)), 1e-300) * 1e-9):
            raise NetlistMeasurementError("ALPS actual PSF parameter differs from candidate: " + name)
    return {"ok": True, "source": "current_psf_designParamVals", "actual": dict(effective_variables),
            "expected": expected, "simulator": "alps"}


def evaluate_task_outputs(task, config, candidate_root, timeout_seconds,
                          required_output_keys=None, cancel_check=None,
                          progress_callback=None, command_runner=None, ownership_callback=None):
    """Run one owned, headless Ocean evaluator, without opening an ADE setup."""
    candidate_root = os.path.realpath(os.path.abspath(candidate_root))
    if task.get("simulator_contract"):
        from .simulators import validate_execution_contract
        validate_execution_contract(task["simulator_contract"])
    psf = os.path.abspath(task["psf_dir"])
    if not is_under(os.path.realpath(psf), candidate_root):
        raise NetlistMeasurementError("Candidate PSF resolves outside the owned candidate")
    required = None if required_output_keys is None else set(required_output_keys)
    task_required = None if required is None else set(key for key in required if key.startswith(task["test"] + "::"))
    qualification = qualify_expressions([task], task_required)
    if not qualification["supported"]:
        raise NetlistMeasurementError("Output expression needs the Maestro backend", qualification)
    root = os.path.abspath(task.get("measurement_root") or os.path.join(os.path.dirname(psf), "measurement"))
    if not is_under(os.path.realpath(root), candidate_root):
        raise NetlistMeasurementError("Measurement evidence path is outside the owned candidate")
    root = ensure_dir(root)
    token = uuid.uuid4().hex
    records = os.path.join(root, "values_%s.tsv" % token)
    script_path = os.path.join(root, "evaluate_%s.il" % token)
    script, outputs = render_measurement_script(task, records, token, required)
    script = "\n".join(_startup_lines(config)) + "\n" + script
    atomic_write_text(script_path, script)
    binary = config.get("cadence", {}).get("ocean_bin")
    if not binary or not os.path.isfile(binary) or not os.access(binary, os.X_OK):
        raise NetlistMeasurementError("Ocean executable is not available for Calculator evaluation")
    argv = [binary, "-nograph", "-restore", script_path]
    cwd = task.get("measurement_cwd") or task["cwd"]
    if not is_under(os.path.realpath(cwd), candidate_root):
        raise NetlistMeasurementError("Measurement working directory is not owned by this candidate")
    environment = child_environment(config, home=ensure_dir(os.path.join(root, "home")))
    for key in list(environment):
        if key.startswith("CDS_MPS_"):
            environment.pop(key, None)
    if progress_callback:
        progress_callback({"stage": "measurement", "argv": argv, "cwd": cwd,
                           "test": task["test"], "corner": task["corner"]})
    command = run_owned_command(argv, cwd=cwd, env=environment, timeout_seconds=timeout_seconds,
                                control_root=root, token="cr_netlist_measure_%s" % token,
                                cancel_check=cancel_check, command_runner=command_runner,
                                before_launch=ownership_callback)
    if command.get("timed_out") or command.get("cancelled") or command.get("termination_failed"):
        raise NetlistMeasurementError("Calculator evaluation was interrupted", {"command": command, "path": root})
    effective_variables, capability = {}, {}
    capability_path = os.path.join(root, "result_reader_capability.json")
    try:
        measured = parse_measurement_records(records, token, outputs, task,
            required_variables(task, required), effective_variables, capability)
    except NetlistMeasurementError as error:
        if capability:
            # This complete, token-bound diagnostic record survives a failed
            # parameter qualification. It cannot create metrics or point ACKs.
            capability["command_returncode"] = command.get("returncode")
            atomic_write_json(capability_path, capability)
            error.detail.update(result_reader_capability=capability, capability_path=capability_path)
        error.detail.setdefault("command", command)
        error.detail.setdefault("path", root)
        raise
    capability["command_returncode"] = command.get("returncode")
    atomic_write_json(capability_path, capability)
    if command.get("returncode") != 0 or any(row.get("error") for row in measured):
        raise NetlistMeasurementError("Required output did not produce a numeric scalar",
                                      {"failure_stage": "result_reader_expression", "command": command,
                                       "outputs": measured, "path": root, "capability_path": capability_path,
                                       "result_reader_capability": capability}, infrastructure=False)
    try:
        parameter_proof = verify_actual_parameters(task, effective_variables)
    except NetlistMeasurementError as error:
        capability["native_parameter_readback"]["ok"] = False
        capability["native_parameter_readback"]["identity_error"] = str(error)
        capability["candidate_qualification"]["reason"] = "current_result_parameter_identity_mismatch"
        atomic_write_json(capability_path, capability)
        error.detail.update(failure_stage="result_reader_parameter_identity", command=command,
                            capability_path=capability_path, result_reader_capability=capability)
        raise
    if parameter_proof:
        capability["native_parameter_readback"]["expected"] = parameter_proof["expected"]
        capability["native_parameter_readback"]["candidate_identity_verified"] = True
        atomic_write_json(capability_path, capability)
    result = {"schema_version": SCHEMA, "token": token, "task_id": task["task_id"],
              "test": task["test"], "corner": task["corner"], "point_id": task["point_id"],
              "psf_dir": psf, "outputs": measured, "command": command, "records_path": records,
              "effective_variables": effective_variables, "variable_source": "current_psf_designParamVals",
              "execution_simulator": (task.get("simulator_contract") or {}).get("execution_simulator", task.get("execution_simulator", "spectre")),
              "result_reader": (task.get("simulator_contract") or {}).get("result_reader", "spectre"),
              "native_parameter_readback_verified": parameter_proof,
              "result_reader_capability": capability, "capability_path": capability_path}
    atomic_write_json(os.path.join(root, "measurement.json"), result)
    return result


def parsed_results(task_results, path=None):
    """Produce the existing *memory* result contract, with explicit CLI provenance."""
    output_map, points, statuses, parameters = {}, {}, [], []
    for result in task_results:
        task = result["task"]
        point_id, corner = task["point_id"], task["corner"]
        design_point = task.get("design_point", point_id)
        prior = points.setdefault(point_id, {"pointID": point_id,
                                           "designPointNumber": design_point, "corner_name": corner})
        if prior["corner_name"] != corner or prior["designPointNumber"] != design_point:
            raise NetlistMeasurementError("One point ID identifies conflicting corners")
        statuses.append({"pointID": point_id, "test_name": task["test"], "statusCode": 3,
                         "status_message": "completed", "provenance": "private_netlist_cli"})
        for row in (result.get("measurement") or {}).get("outputs", []):
            output = output_map.setdefault(row["key"], {
                "key": row["key"], "test": task["test"], "name": row["name"],
                "expression": row.get("expression") or row.get("expr"),
                "output_type": row.get("output_type", row.get("type")), "values": []})
            if output["expression"] != (row.get("expression") or row.get("expr")):
                raise NetlistMeasurementError("Output expression changed across candidate points")
            output["values"].append({"point_id": point_id, "design_point": design_point,
                                     "corner": corner, "source_corner": task.get("source_corner", corner),
                                     "value": row["value"], "numeric": row["value"],
                                     "scalar": row["value"] is not None, "error": row.get("error")})
    for output in output_map.values():
        values = [row["numeric"] for row in output["values"] if row["numeric"] is not None]
        output["scalar"] = bool(values) and len(values) == len(output["values"])
        output["summary"] = {"count": len(values), "min": min(values) if values else None,
                             "max": max(values) if values else None,
                             "mean": sum(values) / float(len(values)) if values else None,
                             "first": values[0] if values else None, "last": values[-1] if values else None}
    return {"schema_version": "crescendo.rdb.v1", "path": path, "provenance": "private_netlist_cli",
            "native_rdb": False, "outputs": [output_map[key] for key in sorted(output_map)],
            "output_map": output_map, "points": [points[key] for key in sorted(points)],
            "test_statuses": statuses, "parameters": parameters}
