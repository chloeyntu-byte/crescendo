"""Export a proven candidate as permanent, same-library schematic views.

The source schematic and config are never edited.  Native instance snapshots
preserve CDF-derived fields; raw netlist candidates resolve their recorded
symbolic fields without invoking callbacks a second time.
"""
from __future__ import print_function

import ast
import copy
import hashlib
import json
import os
import re
import time
import unicodedata
import uuid

from crescendo_engine.runtime import bind_text
_contract_text = bind_text()

from .cadence import child_environment, run_command, write_readonly_cds_context
from .engineering import parse_engineering_number
from .design_variables import is_design
from .netlist_measurement import run_owned_command
from .utils import atomic_write_json, atomic_write_text, ensure_dir, utc_timestamp, cds_lib_token
from .workspace import (CADENCE_STARTUP_LOCK, _skill_quote, _configured_startup_extensions,
                        resolve_copy_libraries, source_design_identity, tree_content_fingerprint)


SCHEMA = _contract_text("crescendo.optimized_schematic.v1")
EVIDENCE_SCHEMA = _contract_text("crescendo.optimized_schematic_evidence.v1")
MAX_VIEW_NAME = 96
SNAPSHOT_LIMIT = 32 * 1024 * 1024


class OptimizedSchematicError(RuntimeError):
    def __init__(self, message, detail=None):
        RuntimeError.__init__(self, message)
        self.detail = detail or {}


def optimized_view_name(display_name):
    """A deterministic legal view name, with loss/truncation collision guards."""
    original = str(display_name or "optimization")
    normalized = unicodedata.normalize("NFKD", original).encode("ascii", "ignore").decode("ascii")
    token = re.sub(r"[^A-Za-z0-9_]+", "_", normalized).strip("_") or "optimization"
    digest = hashlib.sha256(original.encode("utf-8")).hexdigest()[:10]
    if normalized != original or token != original or len(token) > MAX_VIEW_NAME - 10:
        token = token[:MAX_VIEW_NAME - 21].rstrip("_") + "_" + digest
    return "schematic_" + token


def _number(value, identity):
    try:
        return parse_engineering_number(value, identity)
    except (ValueError, TypeError) as error:
        raise OptimizedSchematicError("Actual numeric parameter is unavailable: %s (%s)" % (identity, error))


def _same(left, right):
    return abs(float(left) - float(right)) <= max(abs(float(left)), abs(float(right)), 1e-300) * 1e-10


def _instance(parameter):
    name = (parameter.get("hierarchy_path") or "").rstrip("/").split("/")[-1]
    if not name:
        raise OptimizedSchematicError("Selected instance identity is missing")
    return name


def export_snapshot_skill(rows, output_path):
    """Dump selected instances in an already owned native snapshot process."""
    identities = sorted(set((str(row["library"]), str(row["cell"]),
                             str(row.get("view") or "schematic"), str(row["instance"]))
                            for row in rows))
    lines = [_contract_text('crescExportSnapshot = outfile("%s")') % _skill_quote(output_path),
             _contract_text('unless(crescExportSnapshot error("Crescendo cannot write export snapshot\\n"))')]
    for row in rows:
        lines.append(_contract_text('fprintf(crescExportSnapshot "TARGET\\t%%L\\t%%L\\t%%L\\t%%L\\t%%L\\n" "%s" "%s" "%s" "%s" "%s")') % tuple(
            _skill_quote(str(value)) for value in (row["parameter_id"], row["library"], row["cell"],
                                                   row.get("view") or "schematic", row["instance"])))
    for library, cell, view, instance in identities:
        quoted = tuple(_skill_quote(value) for value in (library, cell, view, instance))
        lines.extend([
            _contract_text('crescExportCv = dbOpenCellViewByType("%s" "%s" "%s" "schematic" "r")') % quoted[:3],
            _contract_text('unless(crescExportCv error("Crescendo export snapshot owner cannot open\\n"))'),
            _contract_text('crescExportInstances = setof(item crescExportCv~>instances item~>name == "%s")') % quoted[3],
            _contract_text('unless(length(crescExportInstances) == 1 error("Crescendo export snapshot instance is not unique\\n"))'),
            _contract_text('crescExportInst = car(crescExportInstances)'),
            _contract_text('foreach(crescExportProp crescExportInst~>prop'),
            _contract_text('  fprintf(crescExportSnapshot "PROP\\t%%L\\t%%L\\t%%L\\t%%L\\t%%L\\t%%L\\t%%L\\n" "%s" "%s" "%s" "%s" crescExportProp~>name crescExportProp~>valueType crescExportProp~>value)') % quoted,
            ')',
            _contract_text('crescExportCdf = cdfGetInstCDF(crescExportInst)'),
            _contract_text('when(crescExportCdf foreach(crescExportParam crescExportCdf~>parameters'),
            _contract_text('  fprintf(crescExportSnapshot "CDF\\t%%L\\t%%L\\t%%L\\t%%L\\t%%L\\t%%L\\t%%L\\n" "%s" "%s" "%s" "%s" crescExportParam~>name crescExportParam~>type crescExportParam~>value)') % quoted,
            '))',
            _contract_text('dbClose(crescExportCv)'),
        ])
    lines.append(_contract_text('close(crescExportSnapshot)'))
    return lines


def parse_export_snapshot(path):
    if os.path.getsize(path) > SNAPSHOT_LIMIT:
        raise OptimizedSchematicError("Native export snapshot exceeds size limit")
    instances, targets = {}, []
    with open(path, "r", encoding="utf-8") as stream:
        for index, raw in enumerate(stream, 1):
            fields = raw.rstrip("\n").split("\t")
            if (fields[0] in ("PROP", "CDF") and len(fields) != 8) or (fields[0] == "TARGET" and len(fields) != 6) or fields[0] not in ("PROP", "CDF", "TARGET"):
                raise OptimizedSchematicError("Malformed native export snapshot at line %d" % index)
            values = []
            for token in fields[1:]:
                if token == "nil": values.append(None)
                elif token == "t": values.append(True)
                else:
                    try: values.append(ast.literal_eval(token))
                    except (ValueError, SyntaxError): values.append(token)
            if fields[0] == "TARGET":
                targets.append(values)
                continue
            library, cell, view, instance, name, value_type, value = values
            key = json.dumps([library, cell, view, instance], separators=(",", ":"))
            row = instances.setdefault(key, {"library": library, "cell": cell, "view": view,
                                             "instance": instance, "properties": {}, "cdf": {}})
            destination = row["properties"] if fields[0] == "PROP" else row["cdf"]
            if name in destination:
                raise OptimizedSchematicError("Duplicate native snapshot property: %s" % name)
            destination[name] = {"type": value_type, "value": value}
    seen_targets = set()
    for parameter_id, library, cell, view, instance in targets:
        if parameter_id in seen_targets:
            raise OptimizedSchematicError("Duplicate native snapshot parameter target: %s" % parameter_id)
        seen_targets.add(parameter_id)
        key = json.dumps([library, cell, view, instance], separators=(",", ":"))
        if key not in instances:
            raise OptimizedSchematicError("Native snapshot target has no properties: %s" % parameter_id)
        instances[key].setdefault("parameter_ids", []).append(parameter_id)
    return {"schema_version": SCHEMA + ".snapshot", "instances": list(instances.values())}



def _point_key(point):
    return point.get("test"), point.get("point_id"), point.get("corner")


def _point_readback(parameter, record, completion, setup, native_run):
    key = parameter["parameter_id"]
    design = is_design(parameter)
    tests = {parameter.get("variable_test")} if design else {
        row["name"] for row in (setup or {}).get("tests", [])
        if (row.get("library"), row.get("cell"), row.get("view") or "schematic") ==
        (parameter.get("design_library"), parameter.get("design_cell"), parameter.get("design_view") or "schematic")
    }
    points = record.get("points") or []
    completed = [row for row in completion.get("points") or []
                 if not tests or row.get("test") in tests]
    expected = {_point_key(row): row for row in completed}
    observed = {_point_key(row) for row in points}
    if not points or len(expected) != len(completed) or len(observed) != len(points) or observed != set(expected):
        raise OptimizedSchematicError("Effective parameter point coverage is missing or ambiguous: %s" % key)
    values = []
    for point in points:
        completed_point = expected[_point_key(point)]
        if native_run and (not point.get("scs_path") or point["scs_path"] != completed_point.get("netlist_path")):
            raise OptimizedSchematicError("Effective parameter points to a different Native Run netlist: %s" % key)
        if design:
            proven = (record.get("source_kind") == parameter["source_kind"]
                      and point.get("effective_kind") == "top_parameter_value"
                      and point.get("netlist_property") == parameter["name"])
        else:
            proven = (point.get("proof_source") == "generated_point_scs_bound_parameters"
                      and point.get("callback_adjustment_proven") is True
                      and re.fullmatch(r"[0-9a-f]{64}", point.get("statement_sha256") or ""))
        if not proven:
            raise OptimizedSchematicError("No effective instance/property readback for %s at %s/%s (proof: %s)" %
                                          (key, point.get("test"), point.get("corner"), point.get("proof_source")))
        values.append(_number(point.get("effective"), key))
        psf = completed_point.get("psf_parameters") or {}
        for name, value in (point.get("dpar_values") or {}).items():
            if name in psf and not _same(_number(psf[name], key), _number(value, key)):
                raise OptimizedSchematicError("PSF and netlist parameter readback disagree: %s/%s" % (key, name))
    if any(not _same(values[0], value) for value in values):
        raise OptimizedSchematicError("Effective parameter varies across completed points: %s" % key)
    return values[0]


def _application_records(application):
    records = {}
    for row in application.get("parameters") or []:
        key = str(row.get("parameter_id"))
        if key in records:
            raise OptimizedSchematicError("Duplicate actual parameter evidence: %s" % key)
        records[key] = row
    return records


def _unit(parameter):
    unit = parameter.get("unit") or parameter.get("units") or ""
    return {"resistance": "Ω", "impedance": "Ω", "capacitance": "F", "inductance": "H",
            "length": "m", "width": "m", "radius": "m", "spacing": "m", "voltage": "V",
            "current": "A", "frequency": "Hz", "delay": "s", "phase": "°", "temperature": "°C",
            "fingers": "", "multiplier": "", "segments": "", "array": "", "turns": "",
            "integer": "", "number": "", "dimensionless": ""}.get(unit, unit)


def build_export_evidence(candidate, parameters, template_manifest=None, source_identity=None,
                          snapshot=None, setup=None, design_variable_priority=None):
    """Bind actual values to one completed candidate and its source design."""
    if candidate.get("status") != "completed":
        raise OptimizedSchematicError("Only completed real simulation candidates can be exported")
    completion = candidate.get("completion_evidence") or candidate.get("completion") or {}
    if completion.get("ok") is not True:
        raise OptimizedSchematicError("Simulation completion evidence is missing")
    if completion.get("candidate_id") not in (None, candidate.get("candidate_id")):
        raise OptimizedSchematicError("Completion evidence belongs to another candidate")
    backend = candidate.get("execution_backend") or completion.get("backend")
    native_run = backend == "maestro_baseline"
    if backend not in ("private_netlist", "maestro_netlist", "persistent_maestro", "maestro", "legacy", "adexl", "maestro_baseline"):
        raise OptimizedSchematicError("Mock or unsupported candidates cannot create optimized schematics")
    selected = {str(row.get("parameter_id")): row for row in parameters}
    if len(selected) != len(parameters) or not selected or any(not key or key == "None" for key in selected):
        raise OptimizedSchematicError("Selected parameter identities are missing or duplicated")
    if any(row.get("backend", "schematic") != "schematic" and not is_design(row) for row in parameters):
        raise OptimizedSchematicError("Schematic export requires device parameters or scoped design variables")
    application = candidate.get("parameter_application") or {}
    generation = completion.get("netlist_generation") or candidate.get("netlist_generation") or {}
    from .native_maestro_parameters import GENERATION_PROTOCOL
    if backend == "maestro_netlist" and generation.get("parameter_application"):
        application = generation["parameter_application"]
    point_proof = (native_run or (backend == "maestro_netlist" and
                   application.get("proof_kind") == "native_run_device_parameters"))
    manifest = template_manifest or {}
    proof = {}
    if native_run:
        pipeline = completion.get("execution_pipeline") or {}
        if (application.get("generation_protocol") != GENERATION_PROTOCOL
                or application.get("proof_kind") != "native_run_device_parameters"
                or application.get("solver_completion_proven") is not True
                or pipeline.get("schema_version") != _contract_text("crescendo.maestro_baseline.v1")
                or pipeline.get("solver_completion_proven") is not True
                or not completion.get("history_name") or completion["history_name"] != pipeline.get("history_name")):
            raise OptimizedSchematicError("Native Run is missing matching completed history and effective device parameter evidence")
        proof["history_name"] = completion["history_name"]
        proof["rdb"] = copy.deepcopy(completion.get("rdb"))
    if backend == "private_netlist":
        qualification = manifest.get("qualification") or {}
        if not qualification.get("ok"):
            raise OptimizedSchematicError("Qualified Template Netlist identity is missing")
        if not completion.get("template_sha256") or completion["template_sha256"] != manifest.get("manifest_sha256"):
            raise OptimizedSchematicError("Candidate and native template identities do not match")
        if set(completion.get("parameters") or {}) != set(selected) or set(manifest.get("parameter_symbols") or {}) != set(selected):
            raise OptimizedSchematicError("Actual candidate parameter evidence is incomplete")
        snapshot = snapshot or manifest.get("export_snapshot")
        if not snapshot:
            from .native_qualification import NATIVE_QUALIFICATION_REVISION
            mapping = (manifest.get("netlist_generation") or {}).get("mapping_qualification") or {}
            if (qualification.get("revision") != NATIVE_QUALIFICATION_REVISION or mapping.get("ok") is not True
                    or mapping.get("revision") != qualification["revision"]):
                raise OptimizedSchematicError("Qualified template parameter bindings or native symbolic snapshot are unavailable")
        proof["kind"] = "qualified_template_parameters"
    elif point_proof:
        if application.get("ok") is not True or application.get("generation_protocol") != GENERATION_PROTOCOL:
            raise OptimizedSchematicError("Effective parameter application evidence is incomplete")
        proof["kind"] = "native_run_point_parameters" if native_run else "maestro_netlist_point_parameters"
        proof["parameters"] = {}
        snapshot = None
        if not native_run and not completion.get("points"):
            completion = dict(completion, points=(completion.get("execution_pipeline") or {}).get("points"))
    else:
        if application.get("ok") is not True:
            raise OptimizedSchematicError("Native parameter application evidence is unavailable")
        proof["kind"] = "verified_oa_parameters"
        snapshot = snapshot or application.get("export_snapshot")

    records = _application_records(application)
    actual, assignments, design_variables = {}, [], []
    for key, parameter in selected.items():
        record = records.get(key) or {}
        if backend == "private_netlist" or (is_design(parameter) and not point_proof):
            value = _number((completion.get("parameters") or {}).get(key), key)
        elif point_proof:
            value = _point_readback(parameter, record, completion, setup, native_run)
            requested = (candidate.get("values") or {}).get(key)
            if not _same(_number(record.get("requested"), key), _number(requested, key)):
                raise OptimizedSchematicError("Effective parameter evidence belongs to another candidate: %s" % key)
            if not native_run and not _same(value, _number((completion.get("parameters") or {}).get(key), key)):
                raise OptimizedSchematicError("Captured parameter and completed netlist values disagree: %s" % key)
            proof["parameters"][key] = copy.deepcopy(record["points"])
        else:
            if record.get("property") != parameter.get("name") or record.get("hierarchy_path") != parameter.get("hierarchy_path"):
                raise OptimizedSchematicError("Actual parameter identity does not match the selected device: %s" % key)
            value = _number(record.get("persisted_schematic_value"), key)
            if not _same(value, _number(record.get("persisted_cdf_value"), key)):
                raise OptimizedSchematicError("Actual schematic and CDF values disagree: %s" % key)
        actual[key] = value
        if is_design(parameter):
            design_variables.append({"parameter_id": key, "name": parameter["name"], "value": value,
                                     "unit": _unit(parameter), "source_kind": parameter["source_kind"],
                                     "test": parameter.get("variable_test")})
            continue
        assignments.append({"parameter_id": key, "library": parameter["owner_library"],
                            "cell": parameter["owner_cell"], "source_view": parameter.get("owner_view") or "schematic",
                            "owner_occurrence_path": parameter.get("owner_occurrence_path") or "/",
                            "design_library": parameter.get("design_library"), "design_cell": parameter.get("design_cell"),
                            "design_view": parameter.get("design_view") or "schematic", "hierarchy_path": parameter.get("hierarchy_path"),
                            "instance": _instance(parameter), "property": parameter["name"], "actual_value": value,
                            "unit": _unit(parameter), "cdf_type": parameter.get("cdf_parameter_type") or "string"})
    return {"schema_version": EVIDENCE_SCHEMA, "candidate_id": candidate["candidate_id"],
            "execution_backend": backend, "source_identity": copy.deepcopy(source_identity),
            "values": actual, "assignments": assignments, "snapshot": copy.deepcopy(snapshot), "proof": proof,
            "design_variables_not_persisted": design_variables,
            "locked_device_parameters": copy.deepcopy((design_variable_priority or {}).get("evidence") or []),
            "parameter_symbols": copy.deepcopy(manifest.get("parameter_symbols") or {}),
            "template_sha256": completion.get("template_sha256"), "native_completion_proven": True}


def _snapshot_instances(evidence):
    snapshot = evidence.get("snapshot") or {}
    return list(snapshot.get("instances") or [])


def build_export_plan(evidence, display_name, protected_views=None, library_paths=None):
    if evidence.get("schema_version") != EVIDENCE_SCHEMA or evidence.get("native_completion_proven") is not True:
        raise OptimizedSchematicError("Bound actual schematic export evidence is unavailable")
    grouped = {}
    for row in evidence.get("assignments") or []:
        key = (row["library"], row["cell"], row["source_view"], row.get("design_library"),
               row.get("design_cell"), row.get("design_view"), row["owner_occurrence_path"])
        grouped.setdefault(key, []).append(copy.deepcopy(row))
    if not grouped and not evidence.get("design_variables_not_persisted"):
        raise OptimizedSchematicError("There are no selected schematic devices or design variables")
    def physical_library(library):
        path = (library_paths or {}).get(library)
        return os.path.realpath(path) if path else library

    protected = {(physical_library(library), cell, view) for library, cell, view in protected_views or []}
    protected.update((physical_library(identity[0]), identity[1], identity[2]) for identity in grouped)
    base = optimized_view_name(display_name)
    groups, by_signature, view_by_owner = [], {}, {}
    for identity, rows in sorted(grouped.items(), key=lambda pair: str(pair[0])):
        signature = tuple(sorted((row["instance"], row["property"], "%.17g" % row["actual_value"]) for row in rows))
        owner_key = (physical_library(identity[0]),) + identity[1:3]
        reuse_key = owner_key + (signature,)
        binding = {"design_library": identity[3], "design_cell": identity[4], "design_view": identity[5],
                   "owner_occurrence_path": identity[6]}
        if reuse_key in by_signature:
            by_signature[reuse_key]["occurrences"].append(binding)
            by_signature[reuse_key]["assignments"].extend(rows)
            continue
        owner = owner_key[:2]
        count = view_by_owner.get(owner, 0)
        view_by_owner[owner] = count + 1
        target, reason = base, None
        if owner + (base,) in protected:
            reason = "existing_view" if not count else "multiple_occurrences"
            counter = 0
            while owner + (target,) in protected:
                seed = str(identity) + (str(counter) if counter else "")
                suffix = "_p" + hashlib.sha256(seed.encode("utf-8")).hexdigest()[:10]
                target = base[:MAX_VIEW_NAME - len(suffix)] + suffix
                counter += 1
        protected.add(owner + (target,))
        group = {"library": identity[0], "cell": identity[1], "source_view": identity[2], "target_view": target,
                 "base_view": base, "view_name_reason": reason, "assignments": rows, "occurrences": [binding]}
        groups.append(group); by_signature[reuse_key] = group
    return {"schema_version": SCHEMA, "candidate_id": evidence["candidate_id"], "display_name": str(display_name),
            "execution_backend": evidence["execution_backend"], "groups": groups, "created_at": utc_timestamp()}


def _literal(value, value_type):
    if value_type == "string":
        return '"%s"' % _skill_quote(str(value))
    if value_type in ("boolean", "bool"):
        return "t" if value else "nil"
    if value_type in ("int", "integer", "fixnum"):
        number = _number(value, "integer property")
        if number != int(number):
            raise OptimizedSchematicError("Actual integer property is not integral")
        return str(int(number))
    if value_type in ("float", "flonum"):
        return "%.17g" % _number(value, "floating point property")
    raise OptimizedSchematicError("Unsupported native property type: %s" % value_type)


def _locked_group_fields(group, evidence):
    owners = {(row["owner_occurrence_path"].rstrip("/"), row["instance"]) for row in group["assignments"]}
    locked = set()
    for row in evidence.get("locked_device_parameters") or []:
        owner, _, instance = (row.get("hierarchy_path") or "").rpartition("/")
        if (owner, instance) in owners:
            locked.add((instance, row["property"]))
    return locked


def _resolved_properties(group, evidence):
    from .private_netlist import _expression_value
    result, snapshots = {}, _snapshot_instances(evidence)
    locked = _locked_group_fields(group, evidence)
    symbols = dict((symbol, evidence["values"][key]) for key, symbol in evidence.get("parameter_symbols", {}).items())
    for assignment in group["assignments"]:
        matches = [row for row in snapshots if assignment["parameter_id"] in row.get("parameter_ids", [])]
        if len(matches) > 1:
            raise OptimizedSchematicError("Actual instance snapshot is ambiguous")
        if evidence["execution_backend"] == "private_netlist" and evidence.get("snapshot") and not matches:
            raise OptimizedSchematicError("Actual symbolic instance snapshot is missing: %s" % assignment["parameter_id"])
        if matches:
            snapshot = matches[0]
            properties = copy.deepcopy(snapshot.get("properties") or {})
            for name, row in (snapshot.get("cdf") or {}).items():
                if name not in properties and isinstance(row.get("value"), str) and _contract_text("CRESCENDO_NL_") in row["value"]:
                    properties[name] = {"type": "string", "value": row["value"]}
            for name, row in properties.items():
                if (assignment["instance"], name) in locked:
                    continue
                value, value_type = row.get("value"), row.get("type")
                if isinstance(value, str) and _contract_text("CRESCENDO_NL_") in value:
                    try: numeric = _expression_value(value, symbols)
                    except (ValueError, SyntaxError, TypeError, ZeroDivisionError, OverflowError) as error:
                        raise OptimizedSchematicError("Cannot resolve native derived property %s: %s" % (name, error))
                    value = "%.17g" % numeric if value_type == "string" else numeric
                if value_type not in ("string", "boolean", "bool", "int", "integer", "fixnum", "float", "flonum"):
                    continue  # Native source copy retains unrelated compound properties.
                key = (assignment["instance"], name)
                entry = {"instance": key[0], "name": name, "type": value_type, "value": value}
                if key in result and result[key] != entry:
                    raise OptimizedSchematicError("Conflicting actual derived instance property: %s" % name)
                result[key] = entry
    # Overlay selected numbers after every source snapshot has been merged;
    # one instance can have several independently selected CDF parameters.
    for assignment in group["assignments"]:
        key = (assignment["instance"], assignment["property"])
        recorded = result.get(key)
        value_type = recorded["type"] if recorded else assignment["cdf_type"]
        if value_type not in ("int", "integer", "fixnum", "float", "flonum"):
            value_type = "string"
        value = "%.17g" % assignment["actual_value"] if value_type == "string" else assignment["actual_value"]
        result[key] = {"instance": key[0], "name": key[1], "type": value_type, "value": value}
    return sorted(result.values(), key=lambda row: (row["instance"], row["name"]))


def _readback_lines(group, view_expression, label):
    lines = [_contract_text('cr_exp_step = "Cannot open the saved schematic for readback"'),
             _contract_text('crescCv = dbOpenCellViewByType("%s" "%s" %s "schematic" "r")') %
             (_skill_quote(group["library"]), _skill_quote(group["cell"]), view_expression),
             _contract_text('unless(crescCv error("Crescendo optimized readback failed to open\\n"))')]
    for row in group["assignments"]:
        instance, prop = (_skill_quote(row[key]) for key in ("instance", "property"))
        parameter_id = _skill_quote(row["parameter_id"])
        expected, magnitude = row["actual_value"], abs(row["actual_value"])
        lines.extend([
            _contract_text('cr_exp_step = "Cannot uniquely locate instance %s"') % instance,
            _contract_text('crescMatches = setof(item crescCv~>instances item~>name == "%s")') % instance,
            _contract_text('unless(length(crescMatches) == 1 error("Crescendo optimized instance is not unique\\n"))'),
            _contract_text('crescInst = car(crescMatches)'),
            _contract_text('cr_exp_step = "Missing saved OA property or CDF value for %s/%s"') % (instance, prop),
            _contract_text('crescProp = dbFindProp(crescInst "%s")') % prop,
            _contract_text('crescCdf = cdfGetInstCDF(crescInst)'),
            _contract_text('crescParam = get(crescCdf stringToSymbol("%s"))') % prop,
            _contract_text('unless(crescProp && crescParam error("Crescendo optimized actual property or CDF is missing\\n"))'),
            _contract_text('cr_exp_step = "Non-numeric OA or CDF readback for %s/%s"') % (instance, prop),
            _contract_text('crescPropNumber = cr_export_number(crescProp~>value)'),
            _contract_text('crescCdfNumber = cr_export_number(crescParam~>value)'),
            _contract_text('unless(numberp(crescPropNumber) && numberp(crescCdfNumber) error("Crescendo optimized numeric readback is unavailable\\n"))'),
            _contract_text('cr_exp_step = sprintf(nil "Readback for %s/%s differs from %.17g: OA %%L, CDF %%L" crescProp~>value crescParam~>value)') %
            (instance.replace("%", "%%"), prop.replace("%", "%%"), expected),
            _contract_text('unless(abs(crescPropNumber - %.17g) <= max(abs(crescPropNumber) %.17g 1e-300)*1e-10 && abs(crescCdfNumber - %.17g) <= max(abs(crescCdfNumber) %.17g 1e-300)*1e-10 error("Crescendo optimized readback changed completed solution for %s\\n"))') %
            (expected, magnitude, expected, magnitude, parameter_id),
            _contract_text('crescExportReadbacks = cons(list("%s" "%s" "%s" %s crescProp~>value crescParam~>value) crescExportReadbacks)') %
            (parameter_id, _skill_quote(group["library"]), _skill_quote(group["cell"]), view_expression),
        ])
    lines.extend(_property_readback(group, "final" if label == "FINAL" else "staged"))
    lines.append(_contract_text('dbClose(crescCv) crescCv = nil'))
    return lines


def _property_readback(group, label):
    """Collect simple OA properties for the touched instances."""
    lines = []
    for name in sorted({row["instance"] for row in group["assignments"]}):
        lines.extend([
            _contract_text('cr_exp_inst = car(setof(item crescCv~>instances item~>name == "%s"))') % _skill_quote(name),
            _contract_text('foreach(cr_exp_prop cr_exp_inst~>prop when(member(cr_exp_prop~>valueType list("string" "boolean" "int" "float"))'),
            _contract_text('  cr_exp_fields = cons(list("%s" "%s" cr_exp_prop~>name cr_exp_prop~>value) cr_exp_fields)))') %
            (label, _skill_quote(name)),
        ])
    return lines


def _phase_start(records, ready_path):
    return [
        _contract_text('crescReady = outfile("%s") close(crescReady)') % _skill_quote(ready_path),
        _contract_text('crescRecords = outfile("%s")') % _skill_quote(records),
        _contract_text('procedure(cr_export_number(value) let((parsed)'),
        '  if(numberp(value) then value else',
        '    if(stringp(value) && isCallable(\'cdfParseFloatString) then',
        '      parsed = errset(cdfParseFloatString(value) t)',
        '      if(parsed then car(parsed) else nil)',
        '    else nil)',
        '  )',
        '))',
    ]


def _group_end(group, label):
    library, cell, target = tuple(_skill_quote(group[name]) for name in ("library", "cell", "target_view"))
    return [
        't) t)',
        _contract_text('if(cr_exp_group then'),
        _contract_text('  foreach(cr_exp_row reverse(crescExportReadbacks) fprintf(crescRecords "%s\\t%%L\\t%%L\\t%%L\\t%%L\\t%%L\\t%%L\\n" car(cr_exp_row) cadr(cr_exp_row) caddr(cr_exp_row) cadddr(cr_exp_row) nth(4 cr_exp_row) nth(5 cr_exp_row)))') % label,
        _contract_text('  foreach(cr_exp_row cr_exp_fields fprintf(crescRecords "PROPERTY\\t%%L\\t%%L\\t%%L\\t%%L\\t%%L\\t%%L\\t%%L\\n" "%s" "%s" "%s" car(cr_exp_row) cadr(cr_exp_row) caddr(cr_exp_row) cadddr(cr_exp_row)))') %
        (library, cell, target),
        _contract_text('  fprintf(crescRecords "VIEW\\t%%L\\t%%L\\t%%L\\t%%L\\n" "%s" "%s" "%s" crescTarget)') % (library, cell, target),
        'else',
        _contract_text('  when(crescCv errset(dbClose(crescCv) t))'),
        _contract_text('  when(crescSource errset(dbClose(crescSource) t))'),
        _contract_text('  fprintf(crescRecords "FAILURE\\t%%L\\t%%L\\t%%L\\t%%L\\t%%L\\n" "%s" "%s" "%s" crescTarget cr_exp_step)') %
        (library, cell, target),
        ')',
        _contract_text('drain(crescRecords)'),
    ]


def _group_start():
    return [
        _contract_text('crescCv = nil crescSource = nil crescTarget = nil'),
        _contract_text('crescExportReadbacks = nil cr_exp_fields = nil'),
        _contract_text('cr_exp_group = errset(progn('),
    ]


def _stage_script(plan, evidence, records, ready_path):
    lines = _phase_start(records, ready_path)
    for group in plan["groups"]:
        library, cell, source, stage = tuple(_skill_quote(group[name]) for name in
                                             ("library", "cell", "source_view", "staging_view"))
        lines.extend(_group_start())
        lines.extend([
            _contract_text('cr_exp_step = "Open original schematic"'),
            _contract_text('when(ddGetObj("%s" "%s" "%s") error("Crescendo owned export staging already exists\\n"))') % (library, cell, stage),
            _contract_text('crescCv = dbOpenCellViewByType("%s" "%s" "%s" "schematic" "r")') % (library, cell, source),
            _contract_text('unless(crescCv error("Crescendo original schematic cannot open\\n"))'),
        ])
        lines.extend(_property_readback(group, "original"))
        for row in group["assignments"]:
            lines.extend([
                _contract_text('cr_exp_inst = car(setof(item crescCv~>instances item~>name == "%s"))') % _skill_quote(row["instance"]),
                _contract_text('cr_exp_cdf = cdfGetInstCDF(cr_exp_inst)'),
                _contract_text('cr_exp_param = get(cr_exp_cdf stringToSymbol("%s"))') % _skill_quote(row["property"]),
                _contract_text('unless(cr_exp_param error("Crescendo original parameter CDF is missing\\n"))'),
                _contract_text('cr_exp_fields = cons(list("original_cdf" "%s" "%s" cr_exp_param~>value) cr_exp_fields)') %
                (_skill_quote(row["instance"]), _skill_quote(row["property"])),
            ])
        lines.extend([
            _contract_text('cr_exp_step = "Copy original schematic to staging"'),
            _contract_text('crescSource = crescCv crescCv = nil'),
            _contract_text('crescCv = dbCopyCellView(crescSource "%s" "%s" "%s")') % (library, cell, stage),
            _contract_text('unless(crescCv error("Crescendo optimized staging copy failed\\n"))'),
            _contract_text('dbClose(crescSource) crescSource = nil'),
            _contract_text('cr_exp_step = "Write instance parameters in staging"'),
        ])
        for row in _resolved_properties(group, evidence):
            lines.extend([
                _contract_text('cr_exp_step = "Write instance parameter %s/%s"') %
                (_skill_quote(row["instance"]), _skill_quote(row["name"])),
                _contract_text('crescMatches = setof(item crescCv~>instances item~>name == "%s")') % _skill_quote(row["instance"]),
                _contract_text('unless(length(crescMatches) == 1 error("Crescendo optimized instance is not unique\\n"))'),
                _contract_text('crescInst = car(crescMatches)'),
                _contract_text('unless(dbReplaceProp(crescInst "%s" "%s" %s) error("Crescendo instance property write failed\\n"))') %
                (_skill_quote(row["name"]), row["type"], _literal(row["value"], row["type"])),
            ])
        if not evidence.get("snapshot"):
            for row in group["assignments"]:
                lines.extend([
                    _contract_text('crescInst = car(setof(item crescCv~>instances item~>name == "%s"))') % _skill_quote(row["instance"]),
                    _contract_text('cdfgData = cdfGetInstCDF(crescInst)'),
                    _contract_text('crescParam = get(cdfgData stringToSymbol("%s"))') % _skill_quote(row["property"]),
                    _contract_text('unless(crescParam error("Crescendo original parameter CDF is missing\\n"))'),
                    _contract_text('when(crescParam~>callback && crescParam~>callback != ""'),
                    _contract_text('  crescParam~>value = "%s"') % ("%.17g" % row["actual_value"]),
                    _contract_text('  evalstring(crescParam~>callback)'),
                    _contract_text('  when(isCallable(\'cdfUpdateInstParam) cdfUpdateInstParam(crescInst)))'),
                ])
        lines.extend([
            _contract_text('cr_exp_step = "Check and save staging schematic"'),
            _contract_text('schCheck(crescCv)'),
            _contract_text('unless(dbSave(crescCv) error("Crescendo optimized staging save failed\\n"))'),
            _contract_text('dbClose(crescCv) crescCv = nil'),
            _contract_text('cr_exp_step = "Verify staging OA and CDF values"'),
        ])
        lines.extend(_readback_lines(group, '"%s"' % stage, "STAGE"))
        lines.append(_contract_text('crescTarget = "%s"') % stage)
        lines.extend(_group_end(group, "STAGE"))
    lines.extend([_contract_text('close(crescRecords)'), 'exit()'])
    return "\n".join(lines) + "\n"


def _publish_script(plan, records, ready_path):
    lines = _phase_start(records, ready_path)
    for group in plan["groups"]:
        library, cell, stage, target, fallback = tuple(_skill_quote(group[name]) for name in
            ("library", "cell", "staging_view", "target_view", "fallback_view"))
        lines.extend(_group_start())
        lines.extend([
            _contract_text('cr_exp_step = "Open verified staging schematic"'),
            _contract_text('crescSource = dbOpenCellViewByType("%s" "%s" "%s" "schematic" "r")') % (library, cell, stage),
            _contract_text('unless(crescSource error("Crescendo verified export staging is unavailable\\n"))'),
            _contract_text('cr_exp_step = "Create a new view without overwriting existing views"'),
            _contract_text('crescTarget = "%s"') % target,
            _contract_text('when(ddGetObj("%s" "%s" crescTarget) crescTarget = "%s")') % (library, cell, fallback),
            _contract_text('when(ddGetObj("%s" "%s" crescTarget) error("Crescendo optimized candidate name is already in use\\n"))') % (library, cell),
            _contract_text('crescCv = dbCopyCellView(crescSource "%s" "%s" crescTarget)') % (library, cell),
            _contract_text('unless(crescCv error("Crescendo optimized view copy failed\\n"))'),
            _contract_text('unless(dbSave(crescCv) error("Crescendo optimized view save failed\\n"))'),
            _contract_text('dbClose(crescCv) crescCv = nil dbClose(crescSource) crescSource = nil'),
            _contract_text('cr_exp_step = "Verify final OA and CDF values"'),
        ])
        lines.extend(_readback_lines(group, _contract_text('crescTarget'), "FINAL"))
        lines.extend(_group_end(group, "FINAL"))
        lines.extend([
            _contract_text('when(cr_exp_group'),
            _contract_text('  cr_exp_stage = ddGetObj("%s" "%s" "%s")') % (library, cell, stage),
            _contract_text('  when(cr_exp_stage ddDeleteObj(cr_exp_stage)))'),
        ])
    lines.extend([_contract_text('close(crescRecords)'), 'exit()'])
    return "\n".join(lines) + "\n"


def _group_key(group):
    return group["library"], group["cell"], group["target_view"]


def _failure(group, phase, reason, view=None, diagnostic_path=None):
    return {"library": group["library"], "cell": group["cell"], "source_view": group["source_view"],
            "requested_view": group["target_view"], "view": view, "phase": phase,
            "reason": reason, "diagnostic_path": diagnostic_path}


def _verify_readback(path, plan, label):
    parameters, views, fields, failed = {}, {}, {}, {}
    if not os.path.isfile(path) or os.path.getsize(path) > SNAPSHOT_LIMIT:
        raise OptimizedSchematicError("Optimized native readback is unavailable or too large")
    with open(path, "r", encoding="utf-8") as stream:
        for raw in stream:
            parts = raw.rstrip("\n").split("\t")
            try:
                values = [None if value == "nil" else True if value == "t" else ast.literal_eval(value)
                          for value in parts[1:]]
            except (ValueError, SyntaxError):
                raise OptimizedSchematicError("Malformed optimized native readback")
            if parts[0] == label and len(values) == 6:
                parameter_id, library, cell, view, schematic, cdf = values
                if parameter_id in parameters:
                    raise OptimizedSchematicError("Duplicate optimized parameter readback")
                parameters[parameter_id] = {"parameter_id": parameter_id, "library": library, "cell": cell, "view": view,
                                            "schematic_value": schematic, "cdf_value": cdf}
            elif parts[0] in ("VIEW", "FAILURE") and len(values) == (4 if parts[0] == "VIEW" else 5):
                key = tuple(values[:3])
                if key in views or key in failed:
                    raise OptimizedSchematicError("Duplicate optimized view readback")
                if parts[0] == "VIEW":
                    views[key] = values[3]
                else:
                    failed[key] = values[3:]
            elif parts[0] == "PROPERTY" and len(values) == 7:
                group_fields = fields.setdefault(tuple(values[:3]), {})
                key = tuple(values[3:6])
                if key[0] == "original_cdf" and key in group_fields and group_fields[key] == values[6]:
                    continue  # Equivalent hierarchy occurrences share the same OA property.
                if key in group_fields:
                    raise OptimizedSchematicError("Duplicate optimized instance property readback")
                group_fields[key] = values[6]
            else:
                raise OptimizedSchematicError("Unexpected optimized native readback record")
    groups = {_group_key(group): group for group in plan["groups"]}
    if set(views) | set(failed) != set(groups) or set(fields) - set(views):
        raise OptimizedSchematicError("Optimized native view readback is incomplete")
    expected = [row for key in views for row in groups[key]["assignments"]]
    if set(parameters) != {row["parameter_id"] for row in expected}:
        raise OptimizedSchematicError("Optimized native parameter readback is incomplete")
    for key, view in views.items():
        group = groups[key]
        allowed = (group["staging_view"],) if label == "STAGE" else (group["target_view"], group["fallback_view"])
        if view not in allowed:
            raise OptimizedSchematicError("Optimized native view identity does not match the export plan")
        for row in group["assignments"]:
            recorded = parameters[row["parameter_id"]]
            if (recorded["library"], recorded["cell"], recorded["view"]) != (group["library"], group["cell"], view):
                raise OptimizedSchematicError("Optimized parameter readback points to the wrong view")
            for field in ("schematic_value", "cdf_value"):
                if not _same(_number(recorded[field], row["parameter_id"]), row["actual_value"]):
                    raise OptimizedSchematicError("Optimized %s changed the completed solution: %s" % (field, row["parameter_id"]))
            recorded["actual_value"] = row["actual_value"]
    return parameters, views, fields, failed


def _view_result(group, view, parameters, before, after):
    writes = {}
    selected = []
    for row in group["assignments"]:
        instance, name = row["instance"], row["property"]
        original = before.get(("original", instance, name), before.get(("original_cdf", instance, name)))
        proof = dict(parameters[row["parameter_id"]], old_value=original, new_value=row["actual_value"],
                     unit=row["unit"], instance=instance, parameter=name)
        selected.append(proof)
        writes[(instance, name)] = {key: proof[key] for key in ("parameter", "old_value", "new_value", "unit")}
    fields = {(instance, name) for phase, instance, name in set(before) | set(after)
              if phase in ("original", "final")}
    for instance, name in sorted(fields - set(writes)):
        key = (instance, name)
        old = before.get(("original", instance, name))
        new = after.get(("final", instance, name))
        if old != new:
            writes[key] = {"parameter": name, "old_value": old, "new_value": new, "unit": ""}
    instances = {}
    for (instance, _name), row in sorted(writes.items()):
        instances.setdefault(instance, []).append(row)
    return {"library": group["library"], "cell": group["cell"], "source_view": group["source_view"],
            "view": view, "base_view": group["base_view"], "requested_view": group["target_view"],
            "view_name_reason": group["view_name_reason"] if view == group["target_view"] else "concurrent_view",
            "occurrences": group["occurrences"], "parameters": selected,
            "instances": [{"instance": name, "parameters": rows} for name, rows in sorted(instances.items())]}


def export_optimized_schematic(prepared, spec, candidate, config, export_root,
                               template_manifest=None, cancel_check=None, identity_check=None,
                               timeout_seconds=None, progress_callback=None, command_runner=None):
    """Run after owned optimization workers stop; outputs remain user-owned."""
    root = os.path.abspath(export_root)
    if os.path.lexists(root) and (os.path.realpath(root) != root or os.listdir(root)):
        raise OptimizedSchematicError("Export diagnostics require a fresh independent directory")
    selected_ids = {str(row.get("parameter_id")) for row in spec.get("variables") or []}
    selected = [row for row in prepared.get("parameters") or [] if str(row.get("parameter_id")) in selected_ids]
    if {row["parameter_id"] for row in selected} != selected_ids:
        raise OptimizedSchematicError("Run parameters no longer match the current TB catalog")
    if candidate.get("status") != "completed" or str(spec.get("evaluator", "real")).lower() == "mock":
        raise OptimizedSchematicError("Only completed real simulation candidates can be exported")
    from .native_maestro_parameters import GENERATION_PROTOCOL
    current_protocol = (candidate.get("execution_backend") == "maestro_baseline"
                        or spec.get("netlist_generation_protocol") == GENERATION_PROTOCOL)
    evidence = None if current_protocol else candidate.get("optimized_schematic_evidence")
    if not evidence:
        evidence = build_export_evidence(candidate, selected, template_manifest=template_manifest,
                                         source_identity=prepared.get("source_identity"), setup=prepared["setup"],
                                         design_variable_priority=spec.get("design_variable_priority"))
    if evidence.get("candidate_id") != candidate.get("candidate_id") or set(evidence.get("values") or {}) != selected_ids:
        raise OptimizedSchematicError("Saved optimized schematic evidence belongs to another solution")
    bound_identity, prepared_identity = evidence.get("source_identity") or {}, prepared.get("source_identity") or {}
    if (bound_identity.get("schema_version"), bound_identity.get("fingerprint")) != (
            prepared_identity.get("schema_version"), prepared_identity.get("fingerprint")):
        raise OptimizedSchematicError("Saved optimized schematic evidence belongs to another source design")
    primary, source_cds, libraries = resolve_copy_libraries(prepared["setup"], prepared.get("hierarchy"), config=config)
    protected = set()
    for row in evidence["assignments"]:
        library, cell = row["library"], row["cell"]
        if library not in libraries:
            raise OptimizedSchematicError("Selected owner library is not resolved: %s" % library)
        cell_path = os.path.join(libraries[library], cell)
        protected.update((library, cell, name) for name in os.listdir(cell_path))
    plan = build_export_plan(evidence, spec.get("display_name") or spec.get("name") or "optimization", protected, libraries)
    for group in plan["groups"]:
        token = uuid.uuid4().hex[:16]
        prefix = _contract_text("cr_export_stage_")
        group.update(staging_view=prefix + token[:32 - len(prefix)],
                     fallback_view=group["base_view"][:MAX_VIEW_NAME - len(token) - 1] + "_" + token)
        for row in group["assignments"]:
            row.setdefault("unit", "")
    if cancel_check and cancel_check():
        raise OptimizedSchematicError("Schematic export cancelled")
    if identity_check:
        check = identity_check()
        if check is not True and not (isinstance(check, dict) and check.get("ok") is True):
            raise OptimizedSchematicError("Source or run identity changed before schematic export")
    actual_source = source_design_identity(prepared["setup"], prepared.get("hierarchy"), config=config)
    if (actual_source.get("schema_version"), actual_source.get("fingerprint")) != (
            prepared_identity.get("schema_version"), prepared_identity.get("fingerprint")):
        raise OptimizedSchematicError("Source design changed since this solution was simulated")
    originals = {}
    for group in plan["groups"]:
        library, cell, view = group["library"], group["cell"], group["source_view"]
        originals[(library, cell, view)] = tree_content_fingerprint(libraries[library], [os.path.join(cell, view)])
    ensure_dir(root)
    plan_path = os.path.join(root, _contract_text("export_plan.json"))
    result_path = os.path.join(root, _contract_text("export_result.json"))
    atomic_write_json(plan_path, plan)
    result = {"schema_version": SCHEMA, "candidate_id": candidate["candidate_id"], "root": root, "ok": False,
              "display_name": plan["display_name"], "execution_backend": evidence["execution_backend"],
              "started_at": utc_timestamp(), "commands": [], "views": [], "failures": [],
              "evidence_path": result_path, "plan_path": plan_path, "proof": evidence.get("proof"),
              "source_identity": evidence["source_identity"], "template_sha256": evidence.get("template_sha256"),
              "design_variables_not_persisted": evidence.get("design_variables_not_persisted") or [],
              "locked_device_parameters": evidence.get("locked_device_parameters") or [], "retained_owned_views": []}
    try:
        groups = []
        for group in plan["groups"]:
            try:
                for row in _resolved_properties(group, evidence):
                    _literal(row["value"], row["type"])
            except OptimizedSchematicError as error:
                result["failures"].append(_failure(group, "plan", str(error)))
            else:
                groups.append(group)
        if groups:
            context = ensure_dir(os.path.join(root, "context"))
            cadence = config.get("cadence") or {}
            write_readonly_cds_context(context, primary, libraries[primary], source_cds_lib_override=source_cds or cadence.get("cds_lib"))
            startup = [_contract_text('; Crescendo authorized optimized-view export; original views are read only.')]
            startup.extend('load("%s")' % _skill_quote(path) for path in _configured_startup_extensions(prepared["setup"], prepared.get("hierarchy"), config))
            startup.append(_contract_text("crescExportStartupLoaded = t"))
            atomic_write_text(os.path.join(context, ".cdsinit"), "\n".join(startup) + "\n")
            deadline = time.monotonic() + float(timeout_seconds or config.get("runtime", {}).get("command_timeout_seconds", 180))
            before = {}
            for phase, label in (("stage", "STAGE"), ("publish", "FINAL")):
                if not groups:
                    break
                if cancel_check and cancel_check():
                    raise OptimizedSchematicError("Schematic export cancelled")
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise OptimizedSchematicError("Schematic export exceeded its total deadline")
                control = ensure_dir(os.path.join(root, phase))
                records = os.path.join(control, "readback.records")
                ready = os.path.join(control, "ready")
                script = os.path.join(control, "export.il")
                phase_plan = dict(plan, groups=groups)
                text = _stage_script(phase_plan, evidence, records, ready) if phase == "stage" else _publish_script(phase_plan, records, ready)
                prefix = _contract_text('unless(boundp(\'crescExportStartupLoaded) load("%s"))\n') % _skill_quote(os.path.join(context, ".cdsinit"))
                body = prefix + text.rsplit('exit()', 1)[0]
                atomic_write_text(script, _contract_text('crescExportPhaseResult = errset(progn(\n') + body +
                                  _contract_text(') t)\nunless(crescExportPhaseResult exit(1))\nexit()\n'))
                argv = [cadence["ocean_bin"], "-nograph", "-restore", script]
                if progress_callback:
                    progress_callback({"stage": "optimized_schematic_" + phase, "argv": argv})

                def guarded_runner(arguments, **kwargs):
                    return (command_runner or run_command)(arguments, startup_lock=CADENCE_STARTUP_LOCK, ready_path=ready, **kwargs)

                command = run_owned_command(argv, cwd=context, env=child_environment(config, context), timeout_seconds=remaining,
                                            control_root=control, token="optimized-export-" + uuid.uuid4().hex,
                                            cancel_check=cancel_check, command_runner=guarded_runner)
                result["commands"].append(command)
                atomic_write_json(result_path, result)
                diagnostic_path = os.path.join(control, "ocean.stdout")
                if command.get("returncode") != 0 or command.get("timed_out") or command.get("cancelled") or command.get("termination_failed"):
                    raise OptimizedSchematicError("Optimized schematic %s failed; inspect %s" % (phase, diagnostic_path),
                                                  {"phase": phase, "command": command})
                if command.get("guardian_cleanup", {}).get("state") != "dead":
                    raise OptimizedSchematicError("Optimized schematic process cleanup is not proven",
                                                  {"phase": phase, "command": command})
                parameters, views, fields, failed = _verify_readback(records, phase_plan, label)
                for group in groups:
                    key = _group_key(group)
                    if phase == "stage" and key in views:
                        state = fields.get(key, {})
                        changed = [(inst, prop) for inst, prop in _locked_group_fields(group, evidence)
                                   if state.get(("original", inst, prop)) != state.get(("staged", inst, prop))]
                        if changed:
                            failed[key] = (None, "CDF callback changed locked device properties: " +
                                           ", ".join("/".join(item) for item in sorted(changed)))
                            del views[key]
                    if key in failed:
                        view, reason = failed[key]
                        result["failures"].append(_failure(group, phase, reason, view, diagnostic_path))
                        retained = group["staging_view"]
                        if os.path.lexists(os.path.join(libraries[group["library"]], group["cell"], retained)):
                            result["retained_owned_views"].append({"library": group["library"], "cell": group["cell"], "view": retained})
                    elif phase == "publish":
                        result["views"].append(_view_result(group, views[key], parameters, before.get(key, {}), fields.get(key, {})))
                if phase == "stage":
                    before = fields
                    groups = [group for group in groups if _group_key(group) in views]
        for (library, cell, view), before in originals.items():
            if tree_content_fingerprint(libraries[library], [os.path.join(cell, view)]) != before:
                raise OptimizedSchematicError("Original schematic changed during export; retained diagnostics require review")
        result.update(ok=not result["failures"], partial=bool(result["views"] and result["failures"]),
                      finished_at=utc_timestamp(), original_views_unchanged=True)
        return result
    except Exception as error:
        result.update(error=str(error), finished_at=utc_timestamp())
        if isinstance(error, OptimizedSchematicError):
            result["detail"] = error.detail
        raise
    finally:
        atomic_write_json(result_path, result)
