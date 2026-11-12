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
import math
import os
import re
import time
import unicodedata
import uuid

from crescendo_engine.runtime import bind_text
_contract_text = bind_text()

from .cadence import child_environment, run_command, write_readonly_cds_context
from .engineering import parse_engineering_number
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



def _reject_native_parameters_export(candidate, spec=None):
    # Native Run applies Device Parameters to its netlist, not to the OA view.
    # Neither the requested number nor unchanged OA is a backannotation proof.
    from .native_maestro_parameters import GENERATION_PROTOCOL
    application = candidate.get("parameter_application") or {}
    completion = candidate.get("completion_evidence") or candidate.get("completion") or {}
    if (application.get("proof_kind") == "native_run_device_parameters" or
            any(row.get("netlist_generation_protocol") == GENERATION_PROTOCOL or
                row.get("generation_protocol") == GENERATION_PROTOCOL
                for row in (spec or {}, candidate, completion, application,
                            candidate.get("netlist_generation") or {},
                            completion.get("netlist_generation") or {}))):
        raise OptimizedSchematicError(
            "Native Device Parameters result has no verified OA/CDF backannotation snapshot; "
            "standalone optimized schematic cannot be exported. Retained SCS and results remain "
            "available. Older records with verified OA snapshots are still supported.",
            {"failure_stage": "optimized_schematic_export", "capability": "native_backannotation",
             "generation_protocol": GENERATION_PROTOCOL, "scs_results_retained": True})

def build_export_evidence(candidate, parameters, template_manifest=None, source_identity=None,
                          snapshot=None):
    """Bind actual values to a completed native result, never to requested values."""
    if candidate.get("status") != "completed":
        raise OptimizedSchematicError("Only completed real simulation candidates can be exported")
    completion = candidate.get("completion_evidence") or candidate.get("completion") or {}
    if completion.get("ok") is not True:
        raise OptimizedSchematicError("Native simulation completion evidence is missing")
    backend = candidate.get("execution_backend") or completion.get("backend")
    if backend not in ("private_netlist", "maestro_netlist", "persistent_maestro", "maestro", "legacy", "adexl"):
        raise OptimizedSchematicError("Mock or unsupported candidates cannot create optimized schematics")
    _reject_native_parameters_export(candidate)
    selected = {str(row.get("parameter_id")): row for row in parameters}
    if len(selected) != len(parameters) or not selected or any(not key or key == "None" for key in selected):
        raise OptimizedSchematicError("Selected parameter identities are missing or duplicated")
    if any(row.get("backend", "schematic") != "schematic" for row in parameters):
        raise OptimizedSchematicError("Schematic export requires device parameters; design variables have no owner view")
    actual, assignments = {}, []
    if backend == "private_netlist":
        manifest = template_manifest or {}
        if not manifest.get("qualification", {}).get("ok"):
            raise OptimizedSchematicError("Qualified Template Netlist identity is missing")
        if completion.get("template_sha256") != manifest.get("manifest_sha256"):
            raise OptimizedSchematicError("Candidate and native template identities do not match")
        proven = completion.get("parameters") or {}
        if set(proven) != set(selected) or set(manifest.get("parameter_symbols") or {}) != set(selected):
            raise OptimizedSchematicError("Actual candidate parameter evidence is incomplete")
        actual = dict((key, _number(proven[key], key)) for key in selected)
        if not snapshot:
            snapshot = manifest.get("export_snapshot")
        if not snapshot:
            raise OptimizedSchematicError("Native symbolic instance snapshot is unavailable; rerun with export support")
    else:
        application = candidate.get("parameter_application") or {}
        if application.get("ok") is not True:
            raise OptimizedSchematicError("Native parameter application evidence is unavailable")
        records = {}
        for row in application.get("parameters") or []:
            key = str(row.get("parameter_id"))
            if key in records: raise OptimizedSchematicError("Duplicate actual parameter evidence: %s" % key)
            records[key] = row
        for key, parameter in selected.items():
            record = records.get(key) or {}
            if record.get("property") != parameter.get("name") or record.get("hierarchy_path") != parameter.get("hierarchy_path"):
                raise OptimizedSchematicError("Actual parameter identity does not match the selected device: %s" % key)
            schematic = _number(record.get("persisted_schematic_value"), key)
            cdf = _number(record.get("persisted_cdf_value"), key)
            if not _same(schematic, cdf):
                raise OptimizedSchematicError("Actual schematic and CDF values disagree: %s" % key)
            actual[key] = schematic
        snapshot = snapshot or application.get("export_snapshot")
    for key, parameter in selected.items():
        assignments.append({"parameter_id": key, "library": parameter["owner_library"],
                            "cell": parameter["owner_cell"], "source_view": parameter.get("owner_view") or "schematic",
                            "owner_occurrence_path": parameter.get("owner_occurrence_path") or "/",
                            "design_library": parameter.get("design_library"), "design_cell": parameter.get("design_cell"),
                            "design_view": parameter.get("design_view") or "schematic", "hierarchy_path": parameter.get("hierarchy_path"),
                            "instance": _instance(parameter), "property": parameter["name"], "actual_value": actual[key],
                            "cdf_type": parameter.get("cdf_parameter_type") or "string"})
    return {"schema_version": EVIDENCE_SCHEMA, "candidate_id": candidate["candidate_id"],
            "execution_backend": backend, "source_identity": copy.deepcopy(source_identity),
            "values": actual, "assignments": assignments, "snapshot": copy.deepcopy(snapshot),
            "parameter_symbols": copy.deepcopy((template_manifest or {}).get("parameter_symbols") or {}),
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
    if not grouped: raise OptimizedSchematicError("There are no selected schematic devices")
    protected = set(protected_views or [])
    protected.update(identity[:3] for identity in grouped)
    base = optimized_view_name(display_name)
    groups, by_signature, view_by_owner = [], {}, {}
    for identity, rows in sorted(grouped.items(), key=lambda pair: str(pair[0])):
        signature = tuple(sorted((row["instance"], row["property"], "%.17g" % row["actual_value"]) for row in rows))
        physical_library = os.path.realpath((library_paths or {}).get(identity[0])) if (library_paths or {}).get(identity[0]) else identity[0]
        owner_key = (physical_library,) + identity[1:3]
        reuse_key = owner_key + (signature,)
        binding = {"design_library": identity[3], "design_cell": identity[4], "design_view": identity[5],
                   "owner_occurrence_path": identity[6]}
        if reuse_key in by_signature:
            by_signature[reuse_key]["occurrences"].append(binding)
            by_signature[reuse_key]["assignments"].extend(rows)
            continue
        count = view_by_owner.get(owner_key[:2], 0)
        view_by_owner[owner_key[:2]] = count + 1
        suffix = "" if not count else "_p" + hashlib.sha256(str(identity).encode("utf-8")).hexdigest()[:10]
        target = base[:MAX_VIEW_NAME - len(suffix)] + suffix
        if (identity[0], identity[1], target) in protected or target in ("schematic", "symbol", "config", "maestro"):
            suffix = "_p" + hashlib.sha256(str(identity).encode("utf-8")).hexdigest()[:10]
            target = base[:MAX_VIEW_NAME - len(suffix)] + suffix
            counter = 1
            while (identity[0], identity[1], target) in protected:
                suffix = "_p" + hashlib.sha256((str(identity) + str(counter)).encode("utf-8")).hexdigest()[:10]
                target = base[:MAX_VIEW_NAME - len(suffix)] + suffix
                counter += 1
        group = {"library": identity[0], "cell": identity[1], "source_view": identity[2], "target_view": target,
                 "assignments": rows, "occurrences": [binding]}
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


def _resolved_properties(group, evidence):
    from .private_netlist import _expression_value
    result, snapshots = {}, _snapshot_instances(evidence)
    symbols = dict((symbol, evidence["values"][key]) for key, symbol in evidence.get("parameter_symbols", {}).items())
    for assignment in group["assignments"]:
        matches = [row for row in snapshots if assignment["parameter_id"] in row.get("parameter_ids", [])]
        if len(matches) > 1:
            raise OptimizedSchematicError("Actual instance snapshot is ambiguous")
        if evidence["execution_backend"] == "private_netlist" and not matches:
            raise OptimizedSchematicError("Actual symbolic instance snapshot is missing: %s" % assignment["parameter_id"])
        if matches:
            snapshot = matches[0]
            properties = copy.deepcopy(snapshot.get("properties") or {})
            for name, row in (snapshot.get("cdf") or {}).items():
                if name not in properties and isinstance(row.get("value"), str) and _contract_text("CRESCENDO_NL_") in row["value"]:
                    properties[name] = {"type": "string", "value": row["value"]}
            for name, row in properties.items():
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


def _readback_lines(group, view_expression, records, label, validate=False, collect=False):
    lines = [_contract_text('crescCv = dbOpenCellViewByType("%s" "%s" %s "schematic" "r")') %
             (_skill_quote(group["library"]), _skill_quote(group["cell"]), view_expression),
             _contract_text('unless(crescCv error("Crescendo optimized readback failed to open\\n"))')]
    for row in group["assignments"]:
        lines.extend([
            _contract_text('crescMatches = setof(item crescCv~>instances item~>name == "%s")') % _skill_quote(row["instance"]),
            _contract_text('unless(length(crescMatches) == 1 error("Crescendo optimized instance is not unique\\n"))'),
            _contract_text('crescInst = car(crescMatches)'),
            _contract_text('crescProp = dbFindProp(crescInst "%s")') % _skill_quote(row["property"]),
            _contract_text('crescCdf = cdfGetInstCDF(crescInst)'),
            _contract_text('crescParam = get(crescCdf stringToSymbol("%s"))') % _skill_quote(row["property"]),
            _contract_text('unless(crescProp && crescParam error("Crescendo optimized actual property or CDF is missing\\n"))'),
        ])
        if validate:
            lines.extend([
                _contract_text('crescPropNumber = crescExportNumber(crescProp~>value)'),
                _contract_text('crescCdfNumber = crescExportNumber(crescParam~>value)'),
                _contract_text('unless(numberp(crescPropNumber) && numberp(crescCdfNumber) error("Crescendo optimized numeric readback is unavailable\\n"))'),
                _contract_text('unless(abs(crescPropNumber - %.17g) <= max(abs(crescPropNumber) %.17g 1e-300)*1e-10 && abs(crescCdfNumber - %.17g) <= max(abs(crescCdfNumber) %.17g 1e-300)*1e-10 error("Crescendo optimized readback changed completed solution for %s\\n"))') %
                (row["actual_value"], abs(row["actual_value"]), row["actual_value"], abs(row["actual_value"]), _skill_quote(row["parameter_id"])),
            ])
        if collect:
            lines.append(_contract_text('crescExportReadbacks = cons(list("%s" "%s" "%s" %s crescProp~>value crescParam~>value) crescExportReadbacks)') %
                         (_skill_quote(row["parameter_id"]), _skill_quote(group["library"]),
                          _skill_quote(group["cell"]), view_expression))
        else:
            lines.append(_contract_text('fprintf(crescRecords "%s\\t%%L\\t%%L\\t%%L\\t%%L\\t%%L\\t%%L\\n" "%s" "%s" "%s" %s crescProp~>value crescParam~>value)') %
                         (label, _skill_quote(row["parameter_id"]), _skill_quote(group["library"]),
                          _skill_quote(group["cell"]), view_expression))
    lines.append(_contract_text('dbClose(crescCv)'))
    return lines


def _stage_script(plan, evidence, records, ready_path):
    lines = [_contract_text('crescReady = outfile("%s") close(crescReady)') % _skill_quote(ready_path),
             _contract_text('crescRecords = outfile("%s")') % _skill_quote(records)]
    for group in plan["groups"]:
        library, cell, source, stage = tuple(_skill_quote(group[name]) for name in
                                             ("library", "cell", "source_view", "staging_view"))
        lines.extend([
            _contract_text('when(ddGetObj("%s" "%s" "%s") error("Crescendo owned export staging already exists\\n"))') % (library, cell, stage),
            _contract_text('crescSource = dbOpenCellViewByType("%s" "%s" "%s" "schematic" "r")') % (library, cell, source),
            _contract_text('unless(crescSource error("Crescendo original schematic cannot open\\n"))'),
            _contract_text('crescCv = dbCopyCellView(crescSource "%s" "%s" "%s")') % (library, cell, stage),
            _contract_text('unless(crescCv error("Crescendo optimized staging copy failed\\n"))'),
            _contract_text('dbClose(crescSource)'),
        ])
        for row in _resolved_properties(group, evidence):
            lines.extend([
                _contract_text('crescMatches = setof(item crescCv~>instances item~>name == "%s")') % _skill_quote(row["instance"]),
                _contract_text('unless(length(crescMatches) == 1 error("Crescendo optimized instance is not unique\\n"))'),
                _contract_text('crescInst = car(crescMatches)'),
                _contract_text('dbReplaceProp(crescInst "%s" "%s" %s)') % (_skill_quote(row["name"]), row["type"], _literal(row["value"], row["type"])),
            ])
        if not evidence.get("snapshot"):
            # Compatibility for completed old runs that retained direct actual
            # values but predate the native snapshot feature. The second
            # readback must still equal that actual completed solution.
            for row in group["assignments"]:
                lines.extend([
                    _contract_text('crescInst = car(setof(item crescCv~>instances item~>name == "%s"))') % _skill_quote(row["instance"]),
                    _contract_text('cdfgData = cdfGetInstCDF(crescInst)'),
                    _contract_text('crescParam = get(cdfgData stringToSymbol("%s"))') % _skill_quote(row["property"]),
                    _contract_text('unless(crescParam error("Crescendo original parameter CDF is missing\\n"))'),
                    _contract_text('crescParam~>value = "%s"') % ("%.17g" % row["actual_value"]),
                    _contract_text('when(crescParam~>callback && crescParam~>callback != "" evalstring(crescParam~>callback))'),
                    _contract_text('when(isCallable(\'cdfUpdateInstParam) cdfUpdateInstParam(crescInst))'),
                ])
        lines.extend([_contract_text('schCheck(crescCv)'), _contract_text('unless(dbSave(crescCv) error("Crescendo optimized staging save failed\\n"))'), _contract_text('dbClose(crescCv)')])
        lines.extend(_readback_lines(group, '"%s"' % stage, records, "STAGE"))
    lines.extend([_contract_text('close(crescRecords)'), _contract_text('printf("CRESCENDO_OPTIMIZED_STAGE_DONE\\n")'), 'exit()'])
    return "\n".join(lines) + "\n"


def _publish_script(plan, records, ready_path):
    lines = [_contract_text('crescReady = outfile("%s") close(crescReady)') % _skill_quote(ready_path),
             _contract_text('crescRecords = outfile("%s")') % _skill_quote(records),
             _contract_text('procedure(crescExportNumber(value) let((parsed)'),
             '  if(numberp(value) then value else',
             '    if(stringp(value) && isCallable(\'cdfParseFloatString) then',
             '      parsed = errset(cdfParseFloatString(value) t)',
             '      if(parsed then car(parsed) else nil)',
             '    else nil)',
             '  )',
             '))']
    for group in plan["groups"]:
        library, cell, stage, target, backup, fallback = tuple(_skill_quote(group[name]) for name in
            ("library", "cell", "staging_view", "target_view", "backup_view", "fallback_view"))
        lines.extend([
            _contract_text('crescStage = dbOpenCellViewByType("%s" "%s" "%s" "schematic" "r")') % (library, cell, stage),
            _contract_text('unless(crescStage error("Crescendo verified export staging is unavailable\\n"))'),
            _contract_text('crescTarget = "%s"') % (fallback if group.get("overwrite_blocked") else target),
            _contract_text('crescBackupCreated = nil'),
            _contract_text('crescExisting = ddGetObj("%s" "%s" crescTarget)') % (library, cell),
            _contract_text('when(crescExisting'),
            _contract_text('  crescExistingCv = dbOpenCellViewByType("%s" "%s" crescTarget "schematic" "r")') % (library, cell),
            _contract_text('  crescBackup = if(crescExistingCv then errset(dbCopyCellView(crescExistingCv "%s" "%s" "%s") t) else nil)') % (library, cell, backup),
            _contract_text('  when(crescExistingCv dbClose(crescExistingCv))'),
            _contract_text('  if(crescBackup && car(crescBackup) then'),
            _contract_text('    dbSave(car(crescBackup)) dbClose(car(crescBackup)) crescBackupCreated = t'),
            _contract_text('  else crescTarget = "%s")') % fallback,
            ')',
            _contract_text('crescExportReadbacks = nil'),
            _contract_text('crescPublished = errset(progn('),
            _contract_text('  crescCopied = dbCopyCellView(crescStage "%s" "%s" crescTarget nil nil t)') % (library, cell),
            _contract_text('  unless(crescCopied error("Crescendo optimized view copy failed\\n"))'),
            _contract_text('  unless(dbSave(crescCopied) error("Crescendo optimized view save failed\\n"))'),
            _contract_text('  dbClose(crescCopied)'),
        ])
        lines.extend(_readback_lines(group, _contract_text('crescTarget'), records, "FINAL", validate=True, collect=True))
        lines.extend([
            '  t',
            ') t)',
            _contract_text('unless(crescPublished && car(crescPublished)'),
            _contract_text('  when(crescCopied errset(dbClose(crescCopied) t))'),
            _contract_text('  when(crescBackupCreated'),
            _contract_text('    crescRecovery = dbOpenCellViewByType("%s" "%s" "%s" "schematic" "r")') % (library, cell, backup),
            _contract_text('    crescRestored = errset(progn('),
            _contract_text('      crescRestoreCv = dbCopyCellView(crescRecovery "%s" "%s" "%s" nil nil t)') % (library, cell, target),
            _contract_text('      unless(crescRestoreCv error("Crescendo original optimized target restore failed\\n"))'),
            _contract_text('      unless(dbSave(crescRestoreCv) error("Crescendo original optimized target restore save failed\\n"))'),
            _contract_text('      dbClose(crescRestoreCv) t'),
            '    ) t)',
            _contract_text('    when(crescRecovery dbClose(crescRecovery))'),
            _contract_text('    unless(crescRestored && car(crescRestored) error("Crescendo overwrite failed; original target backup retained at %s/%s/%s\\n"))') % (library, cell, backup),
            '  )',
            _contract_text('  crescTarget = "%s"') % fallback,
            _contract_text('  when(ddGetObj("%s" "%s" crescTarget) error("Crescendo unique optimized fallback already exists\\n"))') % (library, cell),
            _contract_text('  crescExportReadbacks = nil'),
            _contract_text('  crescCopied = dbCopyCellView(crescStage "%s" "%s" crescTarget)') % (library, cell),
            _contract_text('  unless(crescCopied error("Crescendo optimized fallback view copy failed\\n"))'),
            _contract_text('  unless(dbSave(crescCopied) error("Crescendo optimized fallback view save failed\\n"))'),
            _contract_text('  dbClose(crescCopied)'),
        ])
        lines.extend(_readback_lines(group, _contract_text('crescTarget'), records, "FINAL", validate=True, collect=True))
        lines.extend([
            ')',
            _contract_text('dbClose(crescStage)'),
            _contract_text('foreach(crescRow reverse(crescExportReadbacks) fprintf(crescRecords "FINAL\\t%L\\t%L\\t%L\\t%L\\t%L\\t%L\\n" car(crescRow) cadr(crescRow) caddr(crescRow) cadddr(crescRow) nth(4 crescRow) nth(5 crescRow)))'),
            _contract_text('fprintf(crescRecords "VIEW\\t%%L\\t%%L\\t%%L\\t%%L\\n" "%s" "%s" "%s" crescTarget)') % (library, cell, target),
            _contract_text('crescOwnedStage = ddGetObj("%s" "%s" "%s")') % (library, cell, stage),
            _contract_text('when(crescOwnedStage ddDeleteObj(crescOwnedStage))'),
            _contract_text('when(crescBackupCreated crescOwnedBackup = ddGetObj("%s" "%s" "%s") when(crescOwnedBackup ddDeleteObj(crescOwnedBackup)))') % (library, cell, backup),
        ])
    lines.extend([_contract_text('close(crescRecords)'), _contract_text('printf("CRESCENDO_OPTIMIZED_EXPORT_DONE\\n")'), 'exit()'])
    return "\n".join(lines) + "\n"


def _verify_readback(path, plan, label):
    parameters, views = {}, {}
    if not os.path.isfile(path) or os.path.getsize(path) > SNAPSHOT_LIMIT:
        raise OptimizedSchematicError("Optimized native readback is unavailable or too large")
    with open(path, "r", encoding="utf-8") as stream:
        for raw in stream:
            fields = raw.rstrip("\n").split("\t")
            try: values = [ast.literal_eval(value) for value in fields[1:]]
            except (ValueError, SyntaxError): raise OptimizedSchematicError("Malformed optimized native readback")
            if fields[0] == label and len(values) == 6:
                parameter_id, library, cell, view, schematic, cdf = values
                if parameter_id in parameters: raise OptimizedSchematicError("Duplicate optimized parameter readback")
                parameters[parameter_id] = {"parameter_id": parameter_id, "library": library, "cell": cell, "view": view,
                                            "schematic_value": schematic, "cdf_value": cdf}
            elif fields[0] == "VIEW" and len(values) == 4:
                library, cell, requested, actual = values
                views[(library, cell, requested)] = actual
            else: raise OptimizedSchematicError("Unexpected optimized native readback record")
    expected = [row for group in plan["groups"] for row in group["assignments"]]
    if set(parameters) != set(row["parameter_id"] for row in expected):
        raise OptimizedSchematicError("Optimized native parameter readback is incomplete")
    for group in plan["groups"]:
        view = group["staging_view"] if label == "STAGE" else views.get((group["library"], group["cell"], group["target_view"]))
        if not view or (label == "FINAL" and view not in (group["target_view"], group["fallback_view"])):
            raise OptimizedSchematicError("Optimized native final view identity is missing")
        for row in group["assignments"]:
            recorded = parameters[row["parameter_id"]]
            if (recorded["library"], recorded["cell"], recorded["view"]) != (group["library"], group["cell"], view):
                raise OptimizedSchematicError("Optimized parameter readback points to the wrong view")
            for key in ("schematic_value", "cdf_value"):
                if not _same(_number(recorded[key], row["parameter_id"]), row["actual_value"]):
                    raise OptimizedSchematicError("Optimized %s changed the completed solution: %s" % (key, row["parameter_id"]))
            recorded["actual_value"] = row["actual_value"]
    return parameters, views


def export_optimized_schematic(prepared, spec, candidate, config, export_root,
                               template_manifest=None, cancel_check=None, identity_check=None,
                               timeout_seconds=None, progress_callback=None, command_runner=None):
    """Run after owned optimization workers stop; outputs remain user-owned."""
    _reject_native_parameters_export(candidate, spec)
    root = os.path.abspath(export_root)
    if os.path.lexists(root) and (os.path.realpath(root) != root or os.listdir(root)):
        raise OptimizedSchematicError("Export diagnostics require a fresh independent directory")
    ensure_dir(root)
    selected_ids = set(str(row.get("parameter_id")) for row in spec.get("variables") or [])
    selected = [row for row in prepared.get("parameters") or [] if str(row.get("parameter_id")) in selected_ids]
    if set(row["parameter_id"] for row in selected) != selected_ids:
        raise OptimizedSchematicError("Run parameters no longer match the current TB catalog")
    if candidate.get("status") != "completed" or str(spec.get("evaluator", "real")).lower() == "mock":
        raise OptimizedSchematicError("Only completed real simulation candidates can be exported")
    evidence = candidate.get("optimized_schematic_evidence")
    if not evidence:
        evidence = build_export_evidence(candidate, selected, template_manifest=template_manifest,
                                         source_identity=prepared.get("source_identity"))
    if evidence.get("candidate_id") != candidate.get("candidate_id") or set(evidence.get("values") or {}) != selected_ids:
        raise OptimizedSchematicError("Saved optimized schematic evidence belongs to another solution")
    bound_identity, prepared_identity = evidence.get("source_identity") or {}, prepared.get("source_identity") or {}
    if (bound_identity.get("schema_version"), bound_identity.get("fingerprint")) != (
            prepared_identity.get("schema_version"), prepared_identity.get("fingerprint")):
        raise OptimizedSchematicError("Saved optimized schematic evidence belongs to another source design")
    primary, source_cds, libraries = resolve_copy_libraries(prepared["setup"], prepared.get("hierarchy"), config=config)
    protected_views = set()
    for library in prepared_identity.get("libraries") or []:
        for path in library.get("paths") or []:
            parts = str(path).split(os.sep)
            if len(parts) == 2:
                protected_views.add((library["library"], parts[0], parts[1]))
    for source_library, cell, view in list(protected_views):
        source_root = libraries.get(source_library)
        if source_root:
            for alias, path in libraries.items():
                if os.path.realpath(path) == os.path.realpath(source_root):
                    protected_views.add((alias, cell, view))
    plan = build_export_plan(evidence, spec.get("display_name") or spec.get("name") or "optimization", protected_views, libraries)
    for group in plan["groups"]:
        token = uuid.uuid4().hex[:20]
        group.update(staging_view=_contract_text("cr_export_stage_") + token, backup_view=_contract_text("cr_export_backup_") + token,
                     fallback_view=group["target_view"][:MAX_VIEW_NAME - 22] + "_" + token)
        _resolved_properties(group, evidence)  # Fail unsupported expressions before any native write.
    if cancel_check and cancel_check(): raise OptimizedSchematicError("Schematic export cancelled")
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
        if library not in libraries: raise OptimizedSchematicError("Selected owner library is not resolved")
        originals[(library, cell, view)] = tree_content_fingerprint(libraries[library], [os.path.join(cell, view)])
        target_path = os.path.join(libraries[library], cell, group["target_view"])
        if os.path.isdir(target_path):
            group["overwrite_blocked"] = any(".cdslck" in name for name in os.listdir(target_path))
    context = ensure_dir(os.path.join(root, "context"))
    cadence = config.get("cadence") or {}
    write_readonly_cds_context(context, primary, libraries[primary], source_cds_lib_override=source_cds or cadence.get("cds_lib"))
    startup = [_contract_text('; Crescendo authorized optimized-view export; original views are read only.')]
    startup.extend('load("%s")' % _skill_quote(path) for path in _configured_startup_extensions(prepared["setup"], prepared.get("hierarchy"), config))
    startup.append(_contract_text("crescExportStartupLoaded = t"))
    atomic_write_text(os.path.join(context, ".cdsinit"), "\n".join(startup) + "\n")
    deadline = time.monotonic() + float(timeout_seconds or config.get("runtime", {}).get("command_timeout_seconds", 180))
    atomic_write_json(os.path.join(root, _contract_text("export_plan.json")), plan)
    result = {"schema_version": SCHEMA, "candidate_id": candidate["candidate_id"], "root": root, "ok": False,
              "started_at": utc_timestamp(), "commands": [], "retained_owned_views": [
                  {"library": group["library"], "cell": group["cell"], "view": group[key]} for group in plan["groups"]
                  for key in ("staging_view", "backup_view", "fallback_view")]}
    try:
        for phase in ("stage", "publish"):
            if cancel_check and cancel_check(): raise OptimizedSchematicError("Schematic export cancelled")
            remaining = deadline - time.monotonic()
            if remaining <= 0: raise OptimizedSchematicError("Schematic export exceeded its total deadline")
            control = ensure_dir(os.path.join(root, phase)); records = os.path.join(control, "readback.records")
            ready = os.path.join(control, "ready"); script = os.path.join(control, "export.il")
            text = _stage_script(plan, evidence, records, ready) if phase == "stage" else _publish_script(plan, records, ready)
            prefix = _contract_text('unless(boundp(\'crescExportStartupLoaded) load("%s"))\n') % _skill_quote(os.path.join(context, ".cdsinit"))
            # A restore-file error otherwise drops Ocean into an interactive
            # prompt. Convert any native phase error into a terminal owned
            # command immediately, preserving its full stdout diagnostics.
            body = prefix + text.rsplit('exit()', 1)[0]
            atomic_write_text(script, _contract_text('crescExportPhaseResult = errset(progn(\n') + body +
                              _contract_text(') t)\nunless(crescExportPhaseResult exit(1))\nexit()\n'))
            argv = [cadence["ocean_bin"], "-nograph", "-restore", script]
            if progress_callback: progress_callback({"stage": "optimized_schematic_" + phase, "argv": argv})
            def guarded_runner(arguments, **kwargs):
                return (command_runner or run_command)(arguments, startup_lock=CADENCE_STARTUP_LOCK, ready_path=ready, **kwargs)
            command = run_owned_command(argv, cwd=context, env=child_environment(config, context), timeout_seconds=remaining,
                                        control_root=control, token="optimized-export-" + uuid.uuid4().hex,
                                        cancel_check=cancel_check, command_runner=guarded_runner)
            result["commands"].append(command); atomic_write_json(os.path.join(root, _contract_text("export_result.json")), result)
            if command.get("returncode") != 0 or command.get("timed_out") or command.get("cancelled") or command.get("termination_failed"):
                raise OptimizedSchematicError("Optimized schematic %s failed; inspect %s" % (phase, os.path.join(control, "ocean.stdout")),
                                              {"phase": phase, "command": command})
            if command.get("guardian_cleanup", {}).get("state") != "dead":
                raise OptimizedSchematicError("Optimized schematic process cleanup is not proven",
                                              {"phase": phase, "command": command})
            parameters, views = _verify_readback(records, plan, "STAGE" if phase == "stage" else "FINAL")
        result["views"] = []
        for group in plan["groups"]:
            actual_view = views[(group["library"], group["cell"], group["target_view"])]
            result["views"].append({"library": group["library"], "cell": group["cell"], "view": actual_view,
                                    "requested_view": group["target_view"], "occurrences": group["occurrences"],
                                    "parameters": [parameters[row["parameter_id"]] for row in group["assignments"]]})
        for identity, before in originals.items():
            library, cell, view = identity
            if tree_content_fingerprint(libraries[library], [os.path.join(cell, view)]) != before:
                raise OptimizedSchematicError("Original schematic changed during export; retained diagnostics require review")
        result.update(ok=True, finished_at=utc_timestamp(), retained_owned_views=[], original_views_unchanged=True)
        return result
    except Exception as error:
        result.update(error=str(error), finished_at=utc_timestamp())
        if isinstance(error, OptimizedSchematicError): result["detail"] = error.detail
        raise
    finally:
        atomic_write_json(os.path.join(root, _contract_text("export_result.json")), result)
