from __future__ import print_function

import hashlib
import json
import os
import re
import signal
import subprocess
import sys
import threading
import time
import uuid

from .cadence import child_environment, run_command
from .simulators import selected_simulator
from .run_log import append_run_event
from .native_environment import GUI_ENV_NAMES
from .legacy_guardian import (
    GUARDIAN_SCOPE_CONFIG_KEY,
    LAUNCH_LOCK_NAME as GUARDIAN_LAUNCH_LOCK_NAME,
    LAUNCH_TOKEN_ENVIRONMENT as GUARDIAN_LAUNCH_TOKEN_ENVIRONMENT,
    MANIFEST_NAME as GUARDIAN_LAUNCH_MANIFEST_NAME,
    MANIFEST_SCHEMA_VERSION as GUARDIAN_LAUNCH_PROTOCOL,
    acquire_legacy_guardian_launch_lock,
    build_guardian_token_identity,
    close_legacy_guardian_launch_lock,
    legacy_guardian_parent_identity,
    quiesce_legacy_guardian_attempt,
    write_legacy_guardian_manifest,
)
from .cdf_callback_diagnostics import (
    callback_failure_lines, callback_value_proof_lines, selected_property_assignment_lines,
)
from .pdk import GENERIC_ADAPTER_ID
from .resources import guardian_script_path, submit_spectre_script_path
from .technology import (
    TechnologyBindingError, validate_workspace_technology,
    workspace_technology_validation_required,
)
from .utils import atomic_write_json, atomic_write_text, ensure_dir, is_under, utc_timestamp
from .config_binding import config_binding_lines, config_clone_cdf_lines, private_config_plan, validate_config_binding_records
from .workspace import (
    CADENCE_STARTUP_LOCK,
    ParameterApplicationFailure,
    _cdf_snapshot_skill,
    _design_key,
    _history_identity,
    _skill_literal,
    _skill_quote,
    _workspace_startup_load_lines,
    bounded_skill_body_lines,
    build_occurrence_clone_plan,
    private_library_name, private_design_identity, workspace_test_design_lines,
    merge_parameter_verification,
    render_physical_value,
    summarize_spectre_timing,
    validate_parameter_snapshot,
    validate_schematic_application,
    verify_parameter_application,
    write_batch_files,
)

from .storage_policy import (workspace_rdb_path, native_storage_readback_lines, load_native_storage_readback,
                             native_history_readback_line, qualify_native_history_paths)

SCHEMA_VERSION = "crescendo.persistent_maestro.v1"
WORKER_SCHEMA_VERSION = "crescendo.persistent_maestro_worker.v1"
GUARDIAN_SCHEMA_VERSION = "crescendo.ocean_worker_guardian.v1"
GUARDIAN_CHILD_ENVIRONMENT_KEYS = GUI_ENV_NAMES
GUARDIAN_CHILD_ENVIRONMENT_PREFIX = "CRESCENDO_GUARDIAN_CHILD_"


class PersistentMaestroWorkerError(RuntimeError):
    pass


class PersistentMaestroWorkerStartupError(PersistentMaestroWorkerError):
    pass


class PersistentMaestroWorkerTimeout(PersistentMaestroWorkerError):
    pass


class PersistentMaestroWorkerProcessError(PersistentMaestroWorkerError):
    pass


class PersistentMaestroWorkerProtocolError(PersistentMaestroWorkerError):
    pass


class PersistentMaestroWorkerTainted(PersistentMaestroWorkerError):
    pass


def _linux_boot_id():
    try:
        with open("/proc/sys/kernel/random/boot_id", "r") as stream:
            value = stream.read().strip()
    except (IOError, OSError):
        return None
    return value or None


def _guardian_environment(child_environment):
    environment = dict(child_environment or {})
    for name in GUARDIAN_CHILD_ENVIRONMENT_KEYS:
        bridge = GUARDIAN_CHILD_ENVIRONMENT_PREFIX + name
        environment.pop(bridge, None)
        if name in environment:
            environment[bridge] = environment.pop(name)
    return environment


def _safe_token(value):
    text = str(value)
    token = re.sub(r"[^A-Za-z0-9_.-]+", "_", text).strip("_.-") or "candidate"
    digest = hashlib.sha1(text.encode("utf-8")).hexdigest()[:8]
    return "%s_%s" % (token[:32], digest)


def persistent_worker_control_root(worker_root, epoch):
    """Return the deterministic control directory for one worker epoch."""
    return os.path.join(
        os.path.abspath(worker_root), "epochs", _safe_token(epoch), "control",
    )


def _candidate_rows(workspace, parameters, values, clone_lookup):
    rows = []
    for parameter in parameters:
        if parameter.get("backend", "schematic") != "schematic":
            continue
        parameter_id = parameter["parameter_id"]
        if parameter_id not in values:
            raise ParameterApplicationFailure("candidate is missing selected parameter: %s" % parameter_id)
        owner_library = parameter["owner_library"]
        if owner_library not in workspace["copied_libraries"]:
            raise ParameterApplicationFailure("selected parameter owner library is not copied: %s" % owner_library)
        occurrence_path = parameter.get("owner_occurrence_path") or "/"
        if occurrence_path == "/" and workspace.get("copy_policy") != "same_library_views":
            target_library = private_library_name(workspace, owner_library)
            target_cell = parameter["owner_cell"]
        else:
            clone = clone_lookup.get(_design_key(parameter) + (occurrence_path,))
            if not clone:
                raise ParameterApplicationFailure("precomputed clone target is missing: %s" % occurrence_path)
            target_library = clone["clone_library"]
            target_cell = clone["clone_cell"]
        rendered, prop_type = render_physical_value(values[parameter_id], parameter)
        rows.append({
            "design_library": parameter.get("design_library"),
            "design_cell": parameter.get("design_cell"),
            "design_view": parameter.get("design_view") or "schematic",
            "hierarchy_path": parameter.get("hierarchy_path"),
            "owner_occurrence_path": occurrence_path,
            "library": target_library,
            "cell": target_cell,
            "instance": (parameter.get("hierarchy_path") or "").rstrip("/").split("/")[-1],
            "master_library": parameter.get("master_library"),
            "master_cell": parameter.get("master_cell"),
            "property": parameter["name"],
            "prop_type": prop_type,
            "value": rendered,
            "parameter_id": parameter_id,
        })
        if parameter.get("owner_view") or parameter.get("resolved_top") or workspace.get("private_library_names"):
            rows[-1].update({
                "view": clone.get("target_view") if workspace.get("copy_policy") == "same_library_views" else parameter.get("owner_view") or "schematic",
                "view_type": parameter.get("owner_view_type") or "schematic",
                "netlist_design_library": ((parameter.get("resolved_top") or {}).get("library") or parameter.get("design_library")) if workspace.get("copy_policy") == "same_library_views" else private_design_identity(workspace, parameter.get("resolved_top") or {"library": parameter.get("design_library"), "cell": parameter.get("design_cell"), "view": parameter.get("design_view") or "schematic"})[0],
                "netlist_design_cell": (parameter.get("resolved_top") or {}).get("cell") or parameter.get("design_cell"),
            })
    grouped = {}
    for row in rows:
        grouped.setdefault((row["library"], row["cell"], row.get("view", "schematic"), row["instance"]), []).append(row)
    for instance_rows in grouped.values():
        for row in instance_rows:
            row["instance_selected_count"] = len(instance_rows)
    return rows


def _snapshot_lines(rows, path, kind):
    lines = ['crescVerify = outfile("%s")' % _skill_quote(path)]
    grouped = {}
    for row in rows:
        grouped.setdefault((row["library"], row["cell"], row.get("view", "schematic"), row.get("view_type", "schematic"), row["instance"]), []).append(row)
    for (library, cell, view, view_type, instance), instance_rows in sorted(grouped.items()):
        lines.extend([
            'cv = dbOpenCellViewByType("%s" "%s" "%s" "%s" "r")' % (
                _skill_quote(library), _skill_quote(cell), _skill_quote(view), _skill_quote(view_type)),
            'unless(cv error("Crescendo cannot open snapshot cell %s/%s/schematic\\n"))' % (
                _skill_quote(library), _skill_quote(cell)),
            'snapshotInstances = setof(item cv~>instances item~>name == "%s")' % _skill_quote(instance),
            'unless(length(snapshotInstances) == 1 error("Crescendo snapshot instance is not unique: %s/%s/%s count=%%L\\n" length(snapshotInstances)))' % (
                _skill_quote(library), _skill_quote(cell), _skill_quote(instance)),
            'inst = car(snapshotInstances)',
            'cdfgData = cdfGetInstCDF(inst)',
        ])
        for row in sorted(instance_rows, key=lambda item: item["parameter_id"]):
            lines.extend([
                'snapshotProp = dbFindProp(inst "%s")' % _skill_quote(row["property"]),
                'snapshotCdfParam = get(cdfgData stringToSymbol("%s"))' % _skill_quote(row["property"]),
                'unless(snapshotCdfParam error("Crescendo snapshot CDF parameter is missing: %s\\n"))' % _skill_quote(row["property"]),
                'snapshotValue = if(snapshotProp then snapshotProp~>value else snapshotCdfParam~>value)',
                'fprintf(crescVerify "%s_DIRECT\\t%%L\\t%%L\\t%%L\\t%%L\\t%%L\\t%%L\\t%%L\\n" "%s" "%s" "%s" "%s" "%s" snapshotValue snapshotCdfParam~>value)' % (
                    kind, _skill_quote(row["parameter_id"]), _skill_quote(library),
                    _skill_quote(cell), _skill_quote(instance), _skill_quote(row["property"])),
            ])
            lines.extend(_cdf_snapshot_skill(kind, row["parameter_id"]))
        lines.append('dbClose(cv)')
    lines.append('close(crescVerify)')
    return lines


def _patch_lines(rows, path, adapter_path):
    lines = ['crescVerify = outfile("%s")' % _skill_quote(path)]
    grouped = {}
    for row in rows:
        grouped.setdefault((row["library"], row["cell"], row.get("view", "schematic"), row.get("view_type", "schematic")), []).append(row)
    for (library, cell, view, view_type), cell_rows in sorted(grouped.items()):
        lines.extend([
            'cv = dbOpenCellViewByType("%s" "%s" "%s" "%s" "a")' % (
                _skill_quote(library), _skill_quote(cell), _skill_quote(view), _skill_quote(view_type)),
            'unless(cv error("Crescendo cannot open copied cell %s/%s/schematic\\n"))' % (
                _skill_quote(library), _skill_quote(cell)),
        ])
        instance_groups = {}
        for row in cell_rows:
            instance_groups.setdefault(row["instance"], []).append(row)
        for instance, instance_rows in sorted(instance_groups.items()):
            instance_rows = sorted(instance_rows, key=lambda item: item["property"])
            lines.extend([
                'instances = setof(item cv~>instances item~>name == "%s")' % _skill_quote(instance),
                'unless(length(instances) == 1 error("Crescendo patch instance is not unique: %s/%s/%s count=%%L\\n" length(instances)))' % (
                    _skill_quote(library), _skill_quote(cell), _skill_quote(instance)),
                'inst = car(instances)',
                'cdfgData = cdfGetInstCDF(inst)',
                'baseCdf = cdfGetBaseCellCDF(ddGetObj(inst~>master~>libName inst~>master~>cellName))',
            ])
            for row in instance_rows:
                literal = _skill_literal(row["value"], row["prop_type"])
                lines.extend(selected_property_assignment_lines(
                    row, literal, _skill_quote, generic=not adapter_path))
            for row in instance_rows:
                literal = _skill_literal(row["value"], row["prop_type"])
                lines.extend([
                    'cdfResult = errset(cdfGetInstCDF(inst) t)',
                    'unless(cdfResult && car(cdfResult) error("Crescendo cannot obtain instance CDF for parameter callback\\n"))',
                    'cdfgData = car(cdfResult)',
                    'cdfParam = get(cdfgData stringToSymbol("%s"))' % _skill_quote(row["property"]),
                    'unless(cdfParam error("Crescendo CDF parameter disappeared after update: %s\\n"))' % _skill_quote(row["property"]),
                    'cdfParam~>value = %s' % literal,
                ])
                if adapter_path:
                    lines.extend([
                        'callbackResult = errset(crescPdkApplyParameter(inst "%s" %s) t)' % (
                            _skill_quote(row["property"]), literal),
                        'unless(callbackResult && car(callbackResult) error("CRESCENDO_ADAPTER_ERROR:cdf_parameter_mapping_incompatible PDK adapter failed for %s\\n"))' % _skill_quote(row["property"]),
                    ])
                else:
                    lines.extend([
                        'callbackExpr = cdfParam~>callback',
                        'when(callbackExpr && callbackExpr != ""',
                        "  crescCallbackDefaultValue = get(cdfParam 'defValue)",
                        '  crescCallbackDefaultPresent = numberp(crescCallbackDefaultValue) || stringp(crescCallbackDefaultValue)',
                    ])
                    lines.extend(callback_failure_lines(
                        row, literal, library, cell, view, instance,
                        [item["property"] for item in instance_rows], _skill_quote))
                    lines.extend([
                        '  crescCallbackEffectiveValue = cdfParam~>value',
                        ')',
                        'when(isCallable(\'cdfUpdateInstParam)',
                        '  updateResult = errset(cdfUpdateInstParam(inst) t)',
                        '  unless(updateResult error("CRESCENDO_ADAPTER_ERROR:cdf_update_api_incompatible cdfUpdateInstParam failed for %s\\n"))' % _skill_quote(row["property"]),
                        ')',
                    ])
                    lines.extend(callback_value_proof_lines(row, _skill_quote))
            for row in instance_rows:
                literal = _skill_literal(row["value"], row["prop_type"])
                lines.extend([
                    'schematicProp = dbFindProp(inst "%s")' % _skill_quote(row["property"]),
                    'cdfgData = cdfGetInstCDF(inst)',
                    'cdfParam = get(cdfgData stringToSymbol("%s"))' % _skill_quote(row["property"]),
                    'unless(cdfParam error("Crescendo updated CDF parameter is missing: %s\\n"))' % _skill_quote(row["property"]),
                    'schematicValue = if(schematicProp then schematicProp~>value else cdfParam~>value)',
                ])
                if adapter_path:
                    lines.extend(['callbackStatus = "adapter_executed"', 'callbackExpr = "pdk_adapter"'])
                else:
                    lines.extend([
                        'callbackExpr = cdfParam~>callback',
                        'callbackStatus = if(callbackExpr && callbackExpr != "" then "executed" else "not_defined")',
                    ])
                lines.append(
                    'fprintf(crescVerify "PARAM\\t%%L\\t%%L\\t%%L\\t%%L\\t%%L\\t%%L\\n" "%s" %s schematicValue cdfParam~>value callbackStatus callbackExpr)' % (
                        _skill_quote(row["parameter_id"]), literal))
        lines.extend([
            'schCheck(cv)',
            'unless(dbSave(cv) error("Crescendo failed to save copied cell %s/%s\\n"))' % (
                _skill_quote(library), _skill_quote(cell)),
            'dbClose(cv)',
        ])
    lines.append('close(crescVerify)')
    if rows:
        from .optimized_schematic import export_snapshot_skill
        lines.extend(export_snapshot_skill(rows, path + ".export.tsv"))
    return lines


def _clone_lines(clone_plan, verification_path, workspace=None):
    lines = ['crescCloneVerify = outfile("%s")' % _skill_quote(verification_path)]
    if workspace and workspace.get("copy_policy") == "same_library_views":
        from .same_library_views import copy_lines
        lines.extend(copy_lines(clone_plan))
        config_lines, _plan = config_binding_lines(workspace, clone_plan, verification_path + ".config")
        lines.extend(config_lines)
        lines.append('close(crescCloneVerify)')
        return lines
    for clone in clone_plan:
        views = [(clone.get("source_view", "schematic"), clone.get("source_view_type", "schematic"))]
        if clone["master_view"] != views[0][0]:
            views.append((clone["master_view"], clone["master_view_type"]))
        for view, view_type in views:
            source_library = clone.get("symbol_source_library") if clone.get("config_bound") and view == clone["master_view"] else clone["source_library"]
            source_cell = clone.get("symbol_source_cell") if clone.get("config_bound") and view == clone["master_view"] else clone["source_cell"]
            lines.extend([
                'srcCv = dbOpenCellViewByType("%s" "%s" "%s" "%s" "r")' % (
                    _skill_quote(source_library), _skill_quote(source_cell),
                    _skill_quote(view), _skill_quote(view_type)),
                'unless(srcCv error("Crescendo cannot open clone source %s/%s/%s\\n"))' % (
                    _skill_quote(clone["source_library"]), _skill_quote(clone["source_cell"]), _skill_quote(view)),
                'dstCv = dbCopyCellView(srcCv "%s" "%s" "%s")' % (
                    _skill_quote(clone["clone_library"]), _skill_quote(clone["clone_cell"]), _skill_quote(view)),
                'unless(dstCv error("Crescendo failed to clone %s/%s/%s\\n"))' % (
                    _skill_quote(clone["source_library"]), _skill_quote(clone["source_cell"]), _skill_quote(view)),
                'dbClose(srcCv)', 'dbClose(dstCv)',
            ])
        lines.extend(config_clone_cdf_lines(clone))
        lines.extend([
            'parentCv = dbOpenCellViewByType("%s" "%s" "%s" "%s" "a")' % (
                _skill_quote(clone["parent_library"]), _skill_quote(clone["parent_cell"]),
                _skill_quote(clone.get("parent_view", "schematic")), _skill_quote(clone.get("parent_view_type", "schematic"))),
            'unless(parentCv error("Crescendo cannot open clone parent %s/%s/schematic\\n"))' % (
                _skill_quote(clone["parent_library"]), _skill_quote(clone["parent_cell"])),
            'oldInstances = setof(item parentCv~>instances item~>name == "%s")' % _skill_quote(clone["instance"]),
            'unless(length(oldInstances) == 1 error("Crescendo clone occurrence is not unique: %s count=%%L\\n" length(oldInstances)))' % _skill_quote(clone["occurrence_path"]),
            'oldInst = car(oldInstances)',
            'unless(oldInst~>objType == "inst" error("Crescendo does not support independent cloning of arrays/mosaics: %s\\n"))' % _skill_quote(clone["occurrence_path"]),
            'when(oldInst~>magnification && oldInst~>magnification != 1.0 error("Crescendo does not support magnified hierarchical instances: %s\\n"))' % _skill_quote(clone["occurrence_path"]),
            'oldName = oldInst~>name', 'oldXY = oldInst~>xy', 'oldOrient = oldInst~>orient',
            'oldParams = foreach(mapcar prop oldInst~>prop list(prop~>name prop~>valueType prop~>value))',
            'oldConnections = foreach(mapcar instTerm oldInst~>instTerms list(instTerm~>term~>name if(instTerm~>net then instTerm~>net~>name else nil)))',
            'dbDeleteObject(oldInst)',
            'cloneMaster = dbOpenCellViewByType("%s" "%s" "%s" "%s" "r")' % (
                _skill_quote(clone["clone_library"]), _skill_quote(clone["clone_cell"]),
                _skill_quote(clone["master_view"]), _skill_quote(clone["master_view_type"])),
            'unless(cloneMaster error("Crescendo cannot open cloned master %s/%s/%s\\n"))' % (
                _skill_quote(clone["clone_library"]), _skill_quote(clone["clone_cell"]), _skill_quote(clone["master_view"])),
            'newInst = dbCreateParamInst(parentCv cloneMaster oldName oldXY oldOrient 1 oldParams)',
            'unless(newInst error("Crescendo failed to recreate independent occurrence\\n"))',
            'schCheck(parentCv)',
            'unless(newInst~>name == oldName && newInst~>xy == oldXY && newInst~>orient == oldOrient error("Crescendo recreated occurrence geometry mismatch\\n"))',
            'unless(newInst~>master~>libName == "%s" && newInst~>master~>cellName == "%s" && newInst~>master~>viewName == "%s" error("Crescendo recreated occurrence master mismatch\\n"))' % (
                _skill_quote(clone["clone_library"]), _skill_quote(clone["clone_cell"]), _skill_quote(clone["master_view"])),
            'unless(length(newInst~>instTerms) == length(oldConnections) error("Crescendo recreated occurrence terminal count mismatch\\n"))',
            'foreach(connection oldConnections',
            '  newTerm = car(setof(term newInst~>instTerms term~>term~>name == car(connection)))',
            '  unless(newTerm error("Crescendo recreated occurrence missing terminal %L\\n" car(connection)))',
            '  newNetName = if(newTerm~>net then newTerm~>net~>name else nil)',
            '  unless(equal(newNetName cadr(connection)) error("Crescendo recreated occurrence connectivity mismatch at %L\\n" car(connection)))',
            ')',
            'foreach(paramRow oldParams',
            '  newProp = dbFindProp(newInst car(paramRow))',
            '  unless(newProp && equal(newProp~>value caddr(paramRow)) error("Crescendo recreated occurrence property mismatch at %L\\n" car(paramRow)))',
            ')',
            'unless(dbSave(parentCv) error("Crescendo failed to save clone parent\\n"))',
            'dbClose(parentCv)', 'dbClose(cloneMaster)',
            'fprintf(crescCloneVerify "CLONE\\t%%L\\t%%L\\t%%L\\t%%L\\n" "%s" "%s" "%s" "%s")' % (
                _skill_quote(clone["occurrence_path"]), _skill_quote(clone["source_cell"]),
                _skill_quote(clone["clone_cell"]), _skill_quote(clone["parent_cell"])),
        ])
    if workspace is not None:
        config_lines, _plan = config_binding_lines(workspace, clone_plan, verification_path + ".config")
        lines.extend(config_lines)
    lines.append('close(crescCloneVerify)')
    return lines


def _pristine_cell_plan(rows, epoch, workspace=None):
    cells = {}
    for row in rows:
        key = (row["library"], row["cell"], row.get("view", "schematic"), row.get("view_type", "schematic"))
        cells.setdefault(key, set()).add(row["instance"])
    result = []
    for (library, cell, view, view_type), instances in sorted(cells.items()):
        digest = hashlib.sha1(
            ("%s\0%s\0%s" % (epoch, library, cell) + ("\0" + view if view != "schematic" else "")).encode("utf-8")
        ).hexdigest()[:16]
        result.append({
            "library": library,
            "cell": cell,
            "view": view,
            "view_type": view_type,
            "pristine_library": private_library_name(workspace, library) if workspace and workspace.get("copy_policy") == "same_library_views" else library,
            "pristine_cell": "cr_pristine_%s" % digest,
            "instances": sorted(instances),
        })
        if workspace and workspace.get("same_library_objects"):
            from .same_library_views import register_view
            owned = register_view(workspace, (library, cell, view), (epoch, library, cell, view), role="pristine")
            result[-1].update({"pristine_library": library, "pristine_cell": cell,
                               "pristine_view": owned["target"][2], "owned_view": owned})
    return result


def _pristine_signature_procedure_lines():
    return [
        "procedure(crescPristineCellSignature(cv)",
        "  let((records instance property instTerm net terminal figure signatureTerminal)",
        '    records = list(sprintf(nil "COUNTS|%L" list(length(cv~>instances) length(cv~>nets) length(cv~>terminals) length(cv~>shapes))))',
        "    foreach(instance cv~>instances",
        '      records = cons(sprintf(nil "INSTANCE|%L" list(instance~>name instance~>objType instance~>master~>libName instance~>master~>cellName instance~>master~>viewName instance~>xy instance~>orient instance~>magnification)) records)',
        "      foreach(property instance~>prop",
        '        records = cons(sprintf(nil "INSTANCE_PROPERTY|%L" list(instance~>name property~>name property~>type property~>value)) records)',
        "      )",
        "      foreach(instTerm instance~>instTerms",
        '        records = cons(sprintf(nil "INSTANCE_TERM|%L" list(instance~>name instTerm~>term~>name if(instTerm~>net then instTerm~>net~>name else nil))) records)',
        "      )",
        "    )",
        "    foreach(net cv~>nets",
        '      records = cons(sprintf(nil "NET|%L" list(net~>name length(net~>instTerms) length(setof(signatureTerminal cv~>terminals signatureTerminal~>net == net)))) records)',
        "      foreach(property net~>prop",
        '        records = cons(sprintf(nil "NET_PROPERTY|%L" list(net~>name property~>name property~>type property~>value)) records)',
        "      )",
        "    )",
        "    foreach(terminal cv~>terminals",
        '      records = cons(sprintf(nil "TERMINAL|%L" list(terminal~>name terminal~>direction if(terminal~>net then terminal~>net~>name else nil))) records)',
        "      foreach(property terminal~>prop",
        '        records = cons(sprintf(nil "TERMINAL_PROPERTY|%L" list(terminal~>name property~>name property~>type property~>value)) records)',
        "      )",
        "    )",
        "    foreach(figure cv~>shapes",
        '      records = cons(sprintf(nil "FIGURE|%L" list(figure~>objType figure~>bBox)) records)',
        "    )",
        "    sort(records 'alphalessp)",
        "  )",
        ")",
    ]


def _pristine_capture_lines(plan, verification_path):
    lines = ['crescPristineVerify = outfile("%s")' % _skill_quote(verification_path)]
    for row in plan:
        lines.extend([
            'srcCv = dbOpenCellViewByType("%s" "%s" "%s" "%s" "r")' % (
                _skill_quote(row["library"]), _skill_quote(row["cell"]),
                _skill_quote(row["view"]), _skill_quote(row["view_type"])),
            'unless(srcCv error("Crescendo cannot open pristine source %s/%s/%s\\n"))' % (
                _skill_quote(row["library"]), _skill_quote(row["cell"]),
                _skill_quote(row["view"])),
            'dstCv = dbCopyCellView(srcCv "%s" "%s" "%s")' % (
                _skill_quote(row["pristine_library"]), _skill_quote(row["pristine_cell"]),
                _skill_quote(row.get("pristine_view") or row["view"])),
            'unless(dstCv error("Crescendo failed to capture pristine cellview %s/%s/%s\\n"))' % (
                _skill_quote(row["library"]), _skill_quote(row["cell"]),
                _skill_quote(row["view"])),
            'pristineSourceSignature = crescPristineCellSignature(srcCv)',
            'pristineCopySignature = crescPristineCellSignature(dstCv)',
            'unless(equal(pristineSourceSignature pristineCopySignature) error("Crescendo pristine capture semantic mismatch: %s/%s/%s\\n"))' % (
                _skill_quote(row["library"]), _skill_quote(row["cell"]),
                _skill_quote(row["view"])),
            'dbClose(dstCv)',
            'dstCv = nil',
            'persistedCv = dbOpenCellViewByType("%s" "%s" "%s" "%s" "r")' % (
                _skill_quote(row["pristine_library"]), _skill_quote(row["pristine_cell"]),
                _skill_quote(row.get("pristine_view") or row["view"]), _skill_quote(row["view_type"])),
            'unless(persistedCv error("Crescendo cannot reopen persisted pristine cellview %s/%s/%s\\n"))' % (
                _skill_quote(row["pristine_library"]), _skill_quote(row["pristine_cell"]),
                _skill_quote(row["view"])),
            'pristinePersistedSignature = crescPristineCellSignature(persistedCv)',
            'unless(equal(pristineSourceSignature pristinePersistedSignature) error("Crescendo persisted pristine capture semantic mismatch: %s/%s/%s\\n"))' % (
                _skill_quote(row["library"]), _skill_quote(row["cell"]),
                _skill_quote(row["view"])),
            'fprintf(crescPristineVerify "PRISTINE\\t%%L\\t%%L\\t%%L\\t%%L\\n" "%s" "%s" "%s" "%s")' % (
                _skill_quote(row["library"]), _skill_quote(row["cell"]),
                _skill_quote(row["pristine_library"]), _skill_quote(row["pristine_cell"])),
            'dbClose(srcCv)',
            'dbClose(persistedCv)',
            'srcCv = nil',
            'persistedCv = nil',
        ])
        if row.get("owned_view"):
            from .same_library_views import ownership_marker_lines
            lines.extend(ownership_marker_lines(row["owned_view"]))
    lines.append('close(crescPristineVerify)')
    return lines


def _pristine_restore_lines(plan, verification_path):
    lines = ['crescPristineVerify = outfile("%s")' % _skill_quote(verification_path)]
    for row in plan:
        lines.extend([
            'workingLib = ddGetObj("%s")' % _skill_quote(row["library"]),
            'unless(workingLib error("Crescendo cannot obtain library id before pristine restore: %s\\n"))' % (
                _skill_quote(row["library"]),
            ),
            'workingCv = dbFindOpenCellView(workingLib "%s" "%s" nil)' % (
                _skill_quote(row["cell"]),
                _skill_quote(row["view"])),
            'when(workingCv dbPurge(workingCv))',
            'when(dbFindOpenCellView(workingLib "%s" "%s" nil) error("Crescendo cannot purge working cellview before pristine restore: %s/%s/%s\\n"))' % (
                _skill_quote(row["cell"]), _skill_quote(row["view"]),
                _skill_quote(row["library"]),
                _skill_quote(row["cell"]), _skill_quote(row["view"])),
            'pristineCv = dbOpenCellViewByType("%s" "%s" "%s" "%s" "r")' % (
                _skill_quote(row["pristine_library"]), _skill_quote(row["pristine_cell"]),
                _skill_quote(row.get("pristine_view") or row["view"]), _skill_quote(row["view_type"])),
            'unless(pristineCv error("Crescendo cannot open pristine cellview %s/%s/%s\\n"))' % (
                _skill_quote(row["pristine_library"]), _skill_quote(row["pristine_cell"]),
                _skill_quote(row["view"])),
            'restoredCv = dbCopyCellView(pristineCv "%s" "%s" "%s" nil nil t)' % (
                _skill_quote(row["library"]), _skill_quote(row["cell"]),
                _skill_quote(row["view"])),
            'unless(restoredCv error("Crescendo failed to restore pristine cellview %s/%s/%s\\n"))' % (
                _skill_quote(row["library"]), _skill_quote(row["cell"]),
                _skill_quote(row["view"])),
            'pristineSourceSignature = crescPristineCellSignature(pristineCv)',
            'pristineRestoreSignature = crescPristineCellSignature(restoredCv)',
            'unless(equal(pristineSourceSignature pristineRestoreSignature) error("Crescendo pristine restore semantic mismatch: %s/%s/%s\\n"))' % (
                _skill_quote(row["library"]), _skill_quote(row["cell"]),
                _skill_quote(row["view"])),
            'dbClose(restoredCv)',
            'restoredCv = nil',
            'persistedCv = dbOpenCellViewByType("%s" "%s" "%s" "%s" "r")' % (
                _skill_quote(row["library"]), _skill_quote(row["cell"]),
                _skill_quote(row["view"]), _skill_quote(row["view_type"])),
            'unless(persistedCv error("Crescendo cannot reopen persisted restored cellview %s/%s/%s\\n"))' % (
                _skill_quote(row["library"]), _skill_quote(row["cell"]),
                _skill_quote(row["view"])),
            'pristinePersistedSignature = crescPristineCellSignature(persistedCv)',
            'unless(equal(pristineSourceSignature pristinePersistedSignature) error("Crescendo persisted pristine restore semantic mismatch: %s/%s/%s\\n"))' % (
                _skill_quote(row["library"]), _skill_quote(row["cell"]),
                _skill_quote(row["view"])),
            'fprintf(crescPristineVerify "RESTORED\\t%%L\\t%%L\\t%%L\\t%%L\\n" "%s" "%s" "%s" "%s")' % (
                _skill_quote(row["library"]), _skill_quote(row["cell"]),
                _skill_quote(row["pristine_library"]), _skill_quote(row["pristine_cell"])),
            'dbClose(persistedCv)',
            'dbClose(pristineCv)',
            'persistedCv = nil',
            'pristineCv = nil',
        ])
    lines.append('close(crescPristineVerify)')
    return lines


def _validate_pristine_records(plan, path, kind):
    actual = []
    if not os.path.isfile(path):
        raise ParameterApplicationFailure(
            "persistent pristine %s artifact was not generated: %s" % (kind.lower(), path)
        )
    with open(path, "r") as stream:
        for raw in stream:
            fields = raw.rstrip("\n").split("\t")
            if len(fields) == 5 and fields[0] == kind:
                actual.append(tuple(_record.strip('"') for _record in fields[1:5]))
    expected = [(
        row["library"], row["cell"], row["pristine_library"], row["pristine_cell"],
    ) for row in plan]
    if actual != expected:
        raise ParameterApplicationFailure(
            "persistent pristine %s mismatch: expected=%r actual=%r" % (
                kind.lower(), expected, actual,
            )
        )
    return {"ok": True, "kind": kind, "count": len(actual), "records": actual}


def _candidate_plans(rows, candidate_root, callback_value_proof_required):
    baseline_path = os.path.join(candidate_root, "schematic_baseline.records")
    application_path = os.path.join(candidate_root, "schematic_application.records")
    post_path = os.path.join(candidate_root, "schematic_post.records")
    return {
        "baseline": {"clones": [], "parameters": rows, "verification_path": baseline_path,
                     "record_kind": "BASELINE", "target_mode": "patched"},
        "parameters": {"clones": [], "parameters": rows, "verification_path": application_path,
                       "callback_value_proof_required": bool(callback_value_proof_required)},
        "verify": {"clones": [], "parameters": rows, "verification_path": post_path,
                   "record_kind": "POST", "target_mode": "patched"},
    }


def prepare_persistent_batch(workspace, config, parameters, candidates, batch_root):
    batch_root = ensure_dir(os.path.abspath(batch_root))
    candidates = list(candidates or [])
    if not candidates:
        raise ValueError("persistent Maestro batch requires at least one candidate")
    seen = set()
    normalized = []
    for index, candidate in enumerate(candidates):
        candidate_id = str(candidate.get("candidate_id", index))
        if candidate_id in seen:
            raise ValueError("duplicate candidate_id: %s" % candidate_id)
        seen.add(candidate_id)
        values = dict(candidate.get("values") or {})
        normalized.append((candidate_id, values, candidate.get("candidate_root")))
    seed_values = normalized[0][1]
    clone_plan = build_occurrence_clone_plan(workspace, parameters, seed_values)
    clone_lookup = {
        (row["design_library"], row["design_cell"], row["design_view"], row["occurrence_path"]): row
        for row in clone_plan
    }
    adapter = config.get("cadence", {}).get("pdk_adapter") or GENERIC_ADAPTER_ID
    adapter_path = None
    if adapter != GENERIC_ADAPTER_ID:
        adapter_path = os.path.abspath(os.path.expanduser(str(adapter)))
        if not os.path.isfile(adapter_path):
            raise ParameterApplicationFailure("unsupported PDK adapter or missing SKILL file: %s" % adapter)
    wrapper = write_batch_files(workspace, config, history_name="CRESCENDO_persistent_batch")
    clone_records = os.path.join(batch_root, "clone.records")
    artifacts = []
    for candidate_id, values, explicit_root in normalized:
        candidate_root = ensure_dir(os.path.abspath(explicit_root or os.path.join(batch_root, "candidates", _safe_token(candidate_id))))
        rows = _candidate_rows(workspace, parameters, values, clone_lookup)
        plans = _candidate_plans(rows, candidate_root, not adapter_path)
        identity = _history_identity(workspace, candidate_root)
        marker_path = os.path.join(candidate_root, "persistent_status.tsv")
        atomic_write_text(marker_path, "")
        artifacts.append({
            "candidate_id": candidate_id,
            "candidate_root": candidate_root,
            "values": values,
            "history_name": identity["history_name"],
            "history_identity": identity,
            "rdb_path": workspace_rdb_path(workspace, identity["history_name"]),
            "marker_path": marker_path,
            "patch_plan": plans,
        })
    script_path = os.path.join(batch_root, "persistent_maestro.ocn")
    progress_path = os.path.join(batch_root, "persistent_progress.tsv")
    spectre_retain_pointer = os.path.join(batch_root, "spectre_retain_candidate.txt")
    atomic_write_text(spectre_retain_pointer, "")
    lines = [
        'procedure(crescWriteStatus(path state message)',
        '  let((port progressPort)',
        '    port = outfile(path "a")',
        '    fprintf(port "%s\\t%s\\t%s\\n" state getCurrentTime() message)',
        '    close(port)',
        '    when(state != "PATCHED"',
        '      progressPort = outfile("%s" "a")' % _skill_quote(progress_path),
        '      fprintf(progressPort "%s\\t%s\\t%s\\t%s\\n" path state getCurrentTime() message)',
        '      close(progressPort)',
        '    )',
        '  )',
        ')',
    ]
    lines.extend(_workspace_startup_load_lines(workspace))
    lines.append('printf("CRESCENDO_PERSISTENT_BEGIN\\n")')
    if adapter_path:
        lines.extend([
            'load("%s")' % _skill_quote(adapter_path),
            'unless(isCallable(\'crescPdkApplyParameter) error("CRESCENDO_ADAPTER_ERROR:cdf_update_api_incompatible PDK adapter must define crescPdkApplyParameter\\n"))',
        ])
    lines.extend(_clone_lines(clone_plan, clone_records, workspace))
    lines.append('session = maeOpenSetup("%s" "%s" "%s" ?mode "a")' % (
        _skill_quote(workspace["primary_library"]), _skill_quote(workspace["setup_cell"]),
        _skill_quote(workspace.get("setup_view") or "maestro")))
    lines.append('unless(session error("Crescendo cannot open Maestro setup\\n"))')
    lines.append('mainSDB = axlGetMainSetupDB(session)')
    lines.extend(native_storage_readback_lines(workspace))
    lines.extend(workspace_test_design_lines(workspace))
    for artifact in artifacts:
        plans = artifact["patch_plan"]
        marker = _skill_quote(artifact["marker_path"])
        lines.extend([
            'crescWriteStatus("%s" "BEGIN" "")' % marker,
            'candidateResult = errset(progn(',
        ])
        lines.extend('  ' + row for row in _snapshot_lines(
            plans["baseline"]["parameters"], plans["baseline"]["verification_path"], "BASELINE"))
        lines.extend('  ' + row for row in _patch_lines(
            plans["parameters"]["parameters"], plans["parameters"]["verification_path"], adapter_path))
        lines.extend('  ' + row for row in _snapshot_lines(
            plans["verify"]["parameters"], plans["verify"]["verification_path"], "POST"))
        lines.extend([
            '  crescWriteStatus("%s" "PATCHED" "")' % marker,
            '  retainPort = outfile("%s" "w")' % _skill_quote(spectre_retain_pointer),
            '  fprintf(retainPort "%%s\\n" "%s")' % _skill_quote(artifact["candidate_root"]),
            '  close(retainPort)',
            '  runId = maeRunSimulation(?waitUntilDone t ?returnRunId t ?session session)',
            '  maeWaitUntilDone(\'All ?session session)',
            '  runData = axlGetRunData(session runId)',
            '  unless(runData error("Crescendo cannot obtain run data\\n"))',
            '  axlSetHistoryName(runData "%s")' % _skill_quote(artifact["history_name"]),
            '  unless(equal(axlGetHistoryName(runData) "%s") error("Crescendo history rename failed\\n"))' % _skill_quote(artifact["history_name"]),
            '  maeExportOutputView(?session session ?historyName "%s")' % _skill_quote(artifact["history_name"]),
            '  maeWaitUntilDone(\'All ?session session)',
            '  crescWriteStatus("%s" "DONE" "")' % marker,
            '  t',
            ') t)',
            'unless(candidateResult',
            '  crescWriteStatus("%s" "ERROR" "candidate failed")' % marker,
            '  errset(maeCloseSession(?session session ?forceClose t) t)',
            '  exit()',
            ')',
        ])
    lines.extend([
        'maeCloseSession(?session session ?forceClose t)',
        'printf("CRESCENDO_PERSISTENT_DONE\\n")',
        'exit()',
    ])
    # The legacy one-shot entry is still supported. Keep each candidate body
    # bounded just like requests sent to the resident worker.
    search_from = 0
    for artifact in artifacts:
        opening = lines.index('candidateResult = errset(progn(', search_from)
        closing = lines.index(') t)', opening + 1)
        candidate_lines, generation = bounded_skill_body_lines(
            lines[opening + 1:closing], "candidateResult")
        lines[opening:closing + 1] = candidate_lines
        artifact["candidate_generation"] = generation
        search_from = opening + len(candidate_lines)
    atomic_write_text(script_path, "\n".join(lines) + "\n")
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "batch_root": batch_root,
        "script_path": script_path,
        "progress_path": progress_path,
        "spectre_retain_pointer": spectre_retain_pointer,
        "spectre_wrapper": wrapper["spectre_wrapper"],
        "clone_plan": clone_plan,
        "clone_records": clone_records,
        "config_binding_plan": private_config_plan(workspace, clone_plan),
        "config_verification_path": clone_records + ".config",
        "candidates": artifacts,
    }
    atomic_write_json(os.path.join(batch_root, "persistent_batch.json"), manifest)
    return manifest


def validate_candidate_records(candidate, allow_effective_adjustment=False):
    pristine_restore = None
    if "pristine_cells" in candidate:
        pristine_restore = _validate_pristine_records(
            candidate.get("pristine_cells") or [],
            candidate.get("pristine_restore_path") or "",
            "RESTORED",
        )
    plans = candidate["patch_plan"]
    baseline = validate_parameter_snapshot(
        plans["baseline"], allow_effective_adjustment=allow_effective_adjustment)
    application = validate_schematic_application(
        plans["parameters"], allow_effective_adjustment=allow_effective_adjustment)
    post = validate_parameter_snapshot(
        plans["verify"], allow_effective_adjustment=allow_effective_adjustment)
    merged = merge_parameter_verification(application, baseline, post)
    return {
        "schema_version": SCHEMA_VERSION,
        "schematic_patch_plan": plans,
        "schematic_patch_phases": [
            {"name": "baseline", "plan": plans["baseline"], "verification": baseline},
            {"name": "parameters", "plan": plans["parameters"], "verification": application},
            {"name": "verify", "plan": plans["verify"], "verification": post},
        ],
        "schematic_verification": merged,
        "design_variable_changes": [],
        "pristine_restore": pristine_restore,
    }


def validate_batch_clones(batch):
    validate_config_binding_records(batch.get("config_binding_plan") or [], batch.get("config_verification_path"))
    expected = [(
        row["occurrence_path"], row["source_cell"], row["clone_cell"], row["parent_cell"],
    ) for row in batch.get("clone_plan") or []]
    actual = []
    path = batch["clone_records"]
    if not os.path.isfile(path):
        raise ParameterApplicationFailure("persistent clone verification artifact was not generated: %s" % path)
    with open(path, "r") as stream:
        for raw in stream:
            fields = raw.rstrip("\n").split("\t")
            if fields and fields[0] == "CLONE" and len(fields) == 5:
                actual.append(tuple(_record.strip('"') for _record in fields[1:5]))
    if actual != expected:
        raise ParameterApplicationFailure(
            "persistent clone verification mismatch: expected=%r actual=%r" % (expected, actual)
        )
    return {"ok": True, "count": len(actual), "records": actual}


def verify_candidate_netlist(workspace, candidate, patch_result=None, derived_parameter_names=None):
    patch_result = patch_result or validate_candidate_records(candidate)
    if derived_parameter_names:
        for row in (patch_result.get("schematic_verification") or {}).get("parameters") or []:
            names = derived_parameter_names.get(str(row.get("parameter_id")))
            if names:
                row["derived_parameter_names"] = list(names)
    return verify_parameter_application(
        workspace, patch_result, candidate["candidate_root"],
        history_name=candidate["history_name"],
    )


def read_candidate_status(candidate):
    rows = []
    path = candidate["marker_path"]
    if not os.path.isfile(path):
        return rows
    with open(path, "r") as stream:
        for raw in stream:
            fields = raw.rstrip("\n").split("\t", 2)
            if len(fields) >= 2:
                rows.append({"state": fields[0], "timestamp": fields[1],
                             "message": fields[2] if len(fields) == 3 else ""})
    return rows


def run_persistent_batch(workspace, config, batch, timeout_seconds=None, candidate_timeout_seconds=None):
    spectre_log = os.path.join(batch["batch_root"], "spectre_timeout.log")
    base_env = child_environment(config, workspace["root"])
    env = child_environment(config, workspace["root"], include_runtime_ld=True, extra={
        "PATH": os.path.dirname(batch["spectre_wrapper"]) + ":" + base_env["PATH"],
        "CRESCENDO_SIMULATION_TIMEOUT_SECONDS": config["runtime"].get("simulation_timeout_seconds", 900),
        "CRESCENDO_REAL_SIMULATOR_BIN": config["cadence"].get(selected_simulator(config) + "_bin"),
        "CRESCENDO_REAL_SPECTRE_BIN": config["cadence"].get(selected_simulator(config) + "_bin"),
        "CRESCENDO_EXECUTION_SIMULATOR": selected_simulator(config),
        "CRESCENDO_CADENCE_ROOT": config["cadence"].get("cadence_root") or "",
        "CRESCENDO_SPECTRE_TIMEOUT_LOG": spectre_log,
        "CRESCENDO_SPECTRE_RETAIN_POINTER": batch["spectre_retain_pointer"],
        "CRESCENDO_SPECTRE_SUBMIT_SCRIPT": config["runtime"].get(
            "spectre_submit_script"
        ) or submit_spectre_script_path(),
    })
    per_candidate = int(
        candidate_timeout_seconds if candidate_timeout_seconds is not None
        else config["runtime"].get("candidate_timeout_seconds", 600)
    )
    startup_allowance = int(config["runtime"].get("ocean_startup_timeout_seconds", 90))
    batch_timeout = int(
        timeout_seconds if timeout_seconds is not None
        else startup_allowance + per_candidate * len(batch["candidates"])
    )
    started = time.time()
    command = run_command(
        [config["cadence"]["ocean_bin"], "-nograph", "-restore", batch["script_path"]],
        cwd=workspace["root"], env=env, timeout_seconds=batch_timeout,
        stdout_path=os.path.join(batch["batch_root"], "ocean.stdout"),
        stderr_path=os.path.join(batch["batch_root"], "ocean.stderr"),
        startup_lock=CADENCE_STARTUP_LOCK, ready_path=batch["progress_path"],
        startup_timeout_seconds=min(batch_timeout, int(config["runtime"].get("ocean_startup_timeout_seconds", 90))),
        lock_timeout_seconds=config["runtime"].get("command_timeout_seconds", 1800),
        progress_path=batch["progress_path"], progress_timeout_seconds=per_candidate,
        progress_initial_timeout_seconds=startup_allowance,
    )
    command["started_epoch"] = started
    command["finished_epoch"] = time.time()
    command["spectre_submit_script"] = env["CRESCENDO_SPECTRE_SUBMIT_SCRIPT"]
    command["spectre_timing"] = summarize_spectre_timing(spectre_log, workspace["root"])
    command["candidates"] = [{
        "candidate_id": row["candidate_id"], "history_name": row["history_name"],
        "rdb_path": row["rdb_path"], "marker_path": row["marker_path"],
        "status": read_candidate_status(row), "patch_plan": row["patch_plan"],
    } for row in batch["candidates"]]
    technology_manifest = workspace.get("technology") or {}
    if (
            command.get("returncode") == 0 and not command.get("timed_out")
            and workspace_technology_validation_required(technology_manifest)):
        try:
            command["technology_validation"] = validate_workspace_technology(
                technology_manifest, require_complete=True,
            )
        except TechnologyBindingError as error:
            command["technology_validation"] = {
                "ok": False, "status": "failed", "error": str(error),
            }
            atomic_write_json(os.path.join(batch["batch_root"], "persistent_command.json"), command)
            raise PersistentMaestroWorkerStartupError(str(error))
    atomic_write_json(os.path.join(batch["batch_root"], "persistent_command.json"), command)
    return command


def _worker_protocol_lines(path):
    rows = []
    if not os.path.isfile(path):
        return rows
    with open(path, "r") as stream:
        for raw in stream:
            if not raw.endswith("\n"):
                continue
            fields = raw.rstrip("\n").split("\t", 6)
            if len(fields) != 7:
                continue
            try:
                sequence = int(fields[1])
            except ValueError:
                continue
            rows.append({
                "epoch": fields[0], "sequence": sequence, "nonce": fields[2],
                "candidate_id": fields[3], "state": fields[4],
                "timestamp": fields[5], "message": fields[6],
            })
    return rows


def _matching_worker_protocol(path, identity):
    return [
        row for row in _worker_protocol_lines(path)
        if row["epoch"] == identity["epoch"]
        and row["sequence"] == identity["sequence"]
        and row["nonce"] == identity["nonce"]
        and row["candidate_id"] == identity["candidate_id"]
    ]


def _validate_worker_protocol(rows, successful_states, error_state):
    states = [row["state"] for row in rows]
    if not states:
        raise PersistentMaestroWorkerProtocolError("worker returned no fenced protocol states")
    terminal = states[-1]
    if terminal == successful_states[-1]:
        if states != list(successful_states):
            raise PersistentMaestroWorkerProtocolError(
                "invalid successful worker protocol: %s" % ",".join(states)
            )
        return terminal
    if terminal == error_state:
        prefix = states[:-1]
        if not prefix or prefix != list(successful_states[:len(prefix)]):
            raise PersistentMaestroWorkerProtocolError(
                "invalid failed worker protocol: %s" % ",".join(states)
            )
        return terminal
    raise PersistentMaestroWorkerProtocolError(
        "worker protocol has no recognized terminal state: %s" % ",".join(states)
    )


class PersistentMaestroWorker(object):
    """One supervised interactive Ocean/Maestro process for one private workspace.

    Atomic SKILL requests deliver one candidate or a batch of immutable designs.
    The interactive stdin only receives ``load(path)``; fenced marker files are the
    sole completion authority.  A candidate error, timeout, process death, or
    protocol mismatch taints this epoch so a controller must replace the worker.
    """

    CANDIDATE_STATES = ("ACCEPTED", "BEGIN", "PATCHED", "SIM_DONE", "RESULTS_READY", "DONE")
    CLEANUP_STATES = ("CLEANUP_BEGIN", "CLEANUP_DONE")
    SHUTDOWN_STATES = ("SHUTDOWN_BEGIN", "SHUTDOWN_DONE")

    def __init__(self, workspace, config, parameters, worker_root, worker_index=0,
                 epoch=None, popen_factory=None, clock=None, sleeper=None,
                 killpg=None, getpgid=None, poll_interval=0.1):
        self.workspace = workspace
        self.config = config
        self.parameters = list(parameters or [])
        self.worker_root = os.path.abspath(worker_root)
        self.worker_index = int(worker_index)
        self.epoch = str(epoch or uuid.uuid4().hex)
        self.control_root = ensure_dir(
            persistent_worker_control_root(self.worker_root, self.epoch)
        )
        workspace_root = os.path.abspath(self.workspace.get("root") or "")
        if not workspace_root or not is_under(workspace_root, self.worker_root):
            raise ValueError("persistent worker workspace must be private below worker_root")
        self._popen_factory = popen_factory or subprocess.Popen
        self._use_guardian = popen_factory is None
        self._clock = clock or time.monotonic
        self._sleep = sleeper or time.sleep
        self._killpg = killpg or os.killpg
        self._getpgid = getpgid or os.getpgid
        self.poll_interval = max(0.01, float(poll_interval))
        self.process = None
        self._pgid = None
        self._stdout_handle = None
        self._stderr_handle = None
        self._operation_lock = threading.RLock()
        self._batch_submit_lock = threading.RLock()
        self._batch_abort_before_submit = False
        self.sequence = 0
        self.state = "new"
        self.tainted = False
        self.closed = False
        self.active_identity = None
        self.pending_cleanup = None
        self.clone_plan = []
        self.clone_lookup = {}
        self.clone_records = os.path.join(self.control_root, "clone.records")
        self.pristine_cells = []
        self.pristine_records = os.path.join(self.control_root, "pristine_cells.records")
        self.startup_marker = os.path.join(self.control_root, "startup.tsv")
        self.shutdown_marker = os.path.join(self.control_root, "shutdown.tsv")
        self.worker_state_path = os.path.join(self.worker_root, "worker_state.json")
        self.guardian_state_path = os.path.join(self.control_root, "guardian_state.json")
        self.guardian_launch_manifest_path = os.path.join(
            self.control_root, GUARDIAN_LAUNCH_MANIFEST_NAME,
        )
        self.guardian_launch_lock_path = os.path.join(
            self.control_root, GUARDIAN_LAUNCH_LOCK_NAME,
        )
        guardian_scope = (self.config.get("runtime") or {}).get(
            GUARDIAN_SCOPE_CONFIG_KEY,
        ) or uuid.uuid4().hex
        self.guardian_token_identity = build_guardian_token_identity(
            guardian_scope, self.epoch, self.worker_index,
        )
        self.guardian_token = self.guardian_token_identity["token"]
        self._guardian_terminate_grace_seconds = 5.0
        self._guardian_termination_margin_seconds = 3.0
        self.spectre_log = os.path.join(self.control_root, "spectre_timeout.log")
        self.spectre_retain_pointer = os.path.join(
            self.control_root, "spectre_retain_candidate.txt",
        )
        atomic_write_text(self.spectre_retain_pointer, "")
        self.spectre_wrapper = None
        self._write_state()

    def _write_state(self, message=None):
        guardian = None
        guardian_launch = None
        if os.path.isfile(self.guardian_state_path):
            try:
                with open(self.guardian_state_path, "r") as stream:
                    guardian = json.load(stream)
            except (IOError, OSError, ValueError):
                guardian = None
        if not isinstance(guardian, dict):
            guardian = None
        if os.path.isfile(self.guardian_launch_manifest_path):
            try:
                with open(self.guardian_launch_manifest_path, "r") as stream:
                    guardian_launch = json.load(stream)
            except (IOError, OSError, ValueError):
                guardian_launch = None
        if not isinstance(guardian_launch, dict):
            guardian_launch = None
        batch = (getattr(self, "_active_batch", None)
                 or getattr(self, "_startup_remote_scope", None))
        remote_batch = None
        if batch:
            remote_batch = dict((key, batch.get(key)) for key in (
                "batch_id", "manifest_path", "simulation_submitted",
                "native_completion_proven", "native_stop_proven", "no_remote_jobs_started",
                "abort_marker_path", "protocol_identity"))
        atomic_write_json(self.worker_state_path, {
            "schema_version": WORKER_SCHEMA_VERSION,
            "worker_index": self.worker_index,
            "remote_session_pool": (
                dict((key, getattr(self, '_startup_remote_scope', {}).get(key))
                     for key in ('manifest_path', 'protocol_identity', 'simulation_submitted',
                                 'native_stop_proven', 'native_session_closed', 'submission_scope'))
                if getattr(self, '_startup_remote_scope', None) else None),
            "epoch": self.epoch,
            "state": self.state,
            "tainted": bool(self.tainted),
            "closed": bool(self.closed),
            "sequence": self.sequence,
            "pid": getattr(self.process, "pid", None),
            "pgid": self._pgid,
            "guardian_state_path": self.guardian_state_path if self._use_guardian else None,
            "guardian_token": self.guardian_token if self._use_guardian else None,
            "guardian_token_identity": (
                self.guardian_token_identity if self._use_guardian else None
            ),
            "guardian_launch_protocol": GUARDIAN_LAUNCH_PROTOCOL if self._use_guardian else None,
            "guardian_launch_manifest_path": (
                self.guardian_launch_manifest_path if self._use_guardian else None
            ),
            "guardian_launch_lock_path": (
                self.guardian_launch_lock_path if self._use_guardian else None
            ),
            "guardian_parent_pid": (guardian_launch or {}).get("expected_parent_pid"),
            "guardian_parent_boot_id": (guardian_launch or {}).get("expected_parent_boot_id"),
            "guardian_parent_start_ticks": (
                (guardian_launch or {}).get("expected_parent_start_ticks")
            ),
            "guardian_expected_uid": (guardian_launch or {}).get("expected_uid"),
            "guardian_state_token": (guardian or {}).get("token"),
            "guardian_start_ticks": (guardian or {}).get("guardian_start_ticks"),
            "launch_boot_id": (
                (guardian or {}).get("launch_boot_id")
                or (guardian or {}).get("boot_id")
                or _linux_boot_id()
            ),
            "ocean_pid": (guardian or {}).get("pid"),
            "ocean_pgid": (guardian or {}).get("pgid"),
            "ocean_sid": (guardian or {}).get("sid"),
            "ocean_pid_start_ticks": (guardian or {}).get("pid_start_ticks"),
            "ocean_group_cleanup_complete": (guardian or {}).get("group_cleanup_complete"),
            "workspace_root": self.workspace.get("root"),
            "control_root": self.control_root,
            "pristine_cell_count": len(self.pristine_cells),
            "pristine_records": self.pristine_records,
            "active_identity": self.active_identity,
            "pending_cleanup": self.pending_cleanup,
            "remote_batch": remote_batch,
            "message": message,
            "updated_at": utc_timestamp(),
        })

    def _write_guardian_launch_state(self, ocean_command, status, error=None,
                                     parent_identity=None, launch_lock_fd=None):
        parent_identity = parent_identity or {}
        state = {
            "schema_version": GUARDIAN_SCHEMA_VERSION,
            "token": self.guardian_token,
            "cwd": os.path.realpath(os.path.abspath(self.workspace["root"])),
            "boot_id": _linux_boot_id(),
            "launch_boot_id": _linux_boot_id(),
            "guardian_pid": None,
            "guardian_start_ticks": None,
            "child_subreaper": None,
            "pid": None,
            "pgid": None,
            "sid": None,
            "pid_start_ticks": None,
            "argv": list(ocean_command),
            "started_epoch": time.time(),
            "returncode": 127 if status == "launch_failed" else None,
            "termination_reason": None,
            "term_sent": False,
            "kill_sent": False,
            "expected_parent_pid": parent_identity.get("pid"),
            "expected_parent_boot_id": parent_identity.get("boot_id"),
            "expected_parent_start_ticks": parent_identity.get("start_ticks"),
            "expected_uid": parent_identity.get("uid"),
            "launch_lock_fd": launch_lock_fd,
            "launch_lock_released": False,
            "status": status,
            "updated_epoch": time.time(),
        }
        if error is not None:
            state["finished_epoch"] = time.time()
            state["error"] = "%s: %s" % (error.__class__.__name__, error)
        atomic_write_json(self.guardian_state_path, state)

    def _environment(self):
        wrapper = write_batch_files(
            self.workspace, self.config, history_name="CRESCENDO_persistent_worker_%d" % self.worker_index,
        )
        self.spectre_wrapper = wrapper["spectre_wrapper"]
        base_env = child_environment(self.config, self.workspace["root"])
        return child_environment(self.config, self.workspace["root"], include_runtime_ld=True, extra={
            "PATH": os.path.dirname(self.spectre_wrapper) + ":" + base_env["PATH"],
            "CRESCENDO_SIMULATION_TIMEOUT_SECONDS": self.config["runtime"].get("simulation_timeout_seconds", 900),
            "CRESCENDO_REAL_SIMULATOR_BIN": self.config["cadence"].get(selected_simulator(self.config) + "_bin"),
            "CRESCENDO_REAL_SPECTRE_BIN": self.config["cadence"].get(selected_simulator(self.config) + "_bin"),
            "CRESCENDO_EXECUTION_SIMULATOR": selected_simulator(self.config),
            "CRESCENDO_CADENCE_ROOT": self.config["cadence"].get("cadence_root") or "",
            "CRESCENDO_SPECTRE_TIMEOUT_LOG": self.spectre_log,
            "CRESCENDO_SPECTRE_RETAIN_POINTER": self.spectre_retain_pointer,
            "CRESCENDO_SPECTRE_SUBMIT_SCRIPT": self.config["runtime"].get(
                "spectre_submit_script"
            ) or submit_spectre_script_path(),
        })

    @staticmethod
    def _status_procedure_lines():
        return [
            "procedure(crescWriteWorkerStatus(path epoch sequence nonce candidate state message)",
            "  let((port)",
            "    port = outfile(path \"a\")",
            '    fprintf(port "%s\\t%L\\t%s\\t%s\\t%s\\t%s\\t%s\\n" epoch sequence nonce candidate state getCurrentTime() message)',
            "    close(port)",
            "  )",
            ")",
        ]

    def _adapter_path(self):
        adapter = self.config.get("cadence", {}).get("pdk_adapter") or GENERIC_ADAPTER_ID
        if adapter == GENERIC_ADAPTER_ID:
            return None
        path = os.path.abspath(os.path.expanduser(str(adapter)))
        if not os.path.isfile(path):
            raise ParameterApplicationFailure("unsupported PDK adapter or missing SKILL file: %s" % adapter)
        return path

    def _log_event(self, stage, message, details=None, level="INFO"):
        root = (self.config.get("runtime") or {}).get("run_root")
        if root:
            append_run_event(root, stage, message, details, level=level)

    def _terminate_after_failure(self, reason, primary_error, request_native_stop=False):
        """Attempt owned cleanup while preserving the caller's first exception.

        Only exceptional unwinding uses this best-effort path. Native Stop,
        termination, source integrity and deletion proofs keep their existing
        fail-closed contracts; a secondary diagnostic write grants no proof.
        """
        secondary = []
        def observe(action, error):
            secondary.append({'action': action, 'error_type': error.__class__.__name__,
                              'error': str(error), 'errno': getattr(error, 'errno', None)})
        try:
            self._write_state("%s: %s" % (primary_error.__class__.__name__, primary_error))
        except Exception as state_error:
            observe('failure_state_write', state_error)
        if request_native_stop:
            try:
                self.request_batch_abort(reason=reason)
            except Exception as stop_error:
                observe('native_stop', stop_error)
        try:
            self.terminate(reason=reason)
        except Exception as cleanup_error:
            observe('owned_termination', cleanup_error)
        if secondary:
            detail = {'primary_error_type': primary_error.__class__.__name__,
                      'primary_error': str(primary_error), 'reason': reason,
                      'secondary_errors': secondary}
            previous = list(getattr(self, '_secondary_cleanup_errors', None) or [])
            self._secondary_cleanup_errors = (previous + [detail])[-16:]
            try:
                self._log_event('maestro_secondary_cleanup_error',
                    'Original native failure retained; owned cleanup requires its existing proofs',
                    detail, level='ERROR')
            except Exception:
                # Logging is itself secondary; quota/permission failures must
                # never replace the native failure being re-raised by caller.
                pass
        return secondary

    def _write_bootstrap(self, identity):
        from .job_policy import (job_policy_application_skill, effective_job_policy,
                                 prepare_job_command_observers)
        from .private_netlist import native_corner_schedule_skill

        seed_values = dict((parameter["parameter_id"], 0.0) for parameter in self.parameters)
        self.clone_plan = build_occurrence_clone_plan(self.workspace, self.parameters, seed_values)
        self.clone_lookup = dict(
            ((_design_key(row) + (row["occurrence_path"],)), row) for row in self.clone_plan
        )
        seed_rows = _candidate_rows(
            self.workspace, self.parameters, seed_values, self.clone_lookup,
        )
        self.pristine_cells = _pristine_cell_plan(seed_rows, self.epoch, self.workspace)
        adapter_path = self._adapter_path()
        path = os.path.join(self.control_root, "bootstrap.il")
        lines = self._status_procedure_lines()
        bootstrap_lines = _workspace_startup_load_lines(self.workspace) + [
            'crescWorkerEpoch = "%s"' % _skill_quote(self.epoch),
            "crescWorkerTainted = nil",
            "crescLastRunId = nil",
            "crescLastHistoryName = nil",
            "crescExpectedHistoryName = nil",
            "crescLastEpoch = nil",
            "crescLastSequence = nil",
            "crescLastNonce = nil",
            "crescLastCandidate = nil",
        ]
        if adapter_path:
            bootstrap_lines.extend([
                'crescBootstrapStage = "pdk_adapter"',
                'load("%s")' % _skill_quote(adapter_path),
                "unless(isCallable('crescPdkApplyParameter) error(\"CRESCENDO_ADAPTER_ERROR:cdf_update_api_incompatible PDK adapter must define crescPdkApplyParameter\\n\"))",
            ])
        bootstrap_lines.extend(_pristine_signature_procedure_lines())
        bootstrap_lines.append('crescBootstrapStage = "occurrence_clone"')
        bootstrap_lines.extend(_clone_lines(self.clone_plan, self.clone_records, self.workspace))
        bootstrap_lines.append('crescBootstrapStage = "pristine_capture"')
        bootstrap_lines.extend(_pristine_capture_lines(
            self.pristine_cells, self.pristine_records,
        ))
        bootstrap_lines.extend([
            "crescCloneVerify = nil",
            "crescPristineVerify = nil",
            "srcCv = nil",
            "dstCv = nil",
            "parentCv = nil",
            "oldInstances = nil",
            "oldInst = nil",
            "oldName = nil",
            "oldXY = nil",
            "oldOrient = nil",
            "oldParams = nil",
            "oldConnections = nil",
            "cloneMaster = nil",
            "newInst = nil",
            "connection = nil",
            "newTerm = nil",
            "newNetName = nil",
            "paramRow = nil",
            "newProp = nil",
            "workingCv = nil",
            "workingLib = nil",
            "pristineCv = nil",
            "restoredCv = nil",
            "persistedCv = nil",
            "pristineSourceSignature = nil",
            "pristineCopySignature = nil",
            "pristineRestoreSignature = nil",
            "pristinePersistedSignature = nil",
            'crescBootstrapStage = "maestro_setup_open"',
            'envSetVal("adexl.simulation" "saveLastNHistoryEntries" \'int 1)',
            'session = maeOpenSetup("%s" "%s" "%s" ?mode "a")' % (
                _skill_quote(self.workspace["primary_library"]), _skill_quote(self.workspace["setup_cell"]),
                _skill_quote(self.workspace.get("setup_view") or "maestro"),
            ),
            'unless(session error("Crescendo cannot open Maestro setup\\n"))',
            "mainSDB = axlGetMainSetupDB(session)",
            'unless(mainSDB error("Crescendo cannot obtain the Maestro setup database\\n"))',
            "activeSetup = axlGetActiveSetup(mainSDB)",
            'unless(activeSetup error("Crescendo cannot obtain the active Maestro setup\\n"))',
        ])
        bootstrap_lines.append('crescBootstrapStage = "native_storage_readback"')
        bootstrap_lines.extend(native_storage_readback_lines(self.workspace))
        bootstrap_lines.append('crescBootstrapStage = "native_corner_identity"')
        bootstrap_lines.extend(native_corner_schedule_skill(
            self.workspace["setup"], os.path.join(self.control_root, "native_corner_schedule.tsv")))
        bootstrap_lines.extend([
            'crescBootstrapStage = "history_overwrite"',
            'crescOverwriteHistoryName = "Next History Run"',
            "overwriteModeResult = errset(axlSetOverwriteHistory(activeSetup t) t)",
            'unless(overwriteModeResult && axlGetOverwriteHistory(activeSetup) error("Crescendo cannot enable native history overwrite\\n"))',
            "overwriteNameResult = errset(axlSetOverwriteHistoryName(activeSetup crescOverwriteHistoryName) t)",
            'unless(overwriteNameResult && equal(axlGetOverwriteHistoryName(activeSetup) crescOverwriteHistoryName) error("Crescendo cannot bind the native overwrite history selection\\n"))',
            "t",
        ])
        bootstrap_lines.extend(workspace_test_design_lines(self.workspace))
        runtime = self.config.get('runtime') or {}
        overrides = runtime.get('maestro_job_overrides') or {}
        snapshot = runtime.get('maestro_source_job_policy') or {}
        self.expected_job_policy = effective_job_policy(snapshot, overrides)
        policies = [self.expected_job_policy.get('global') or {}]
        policy_tests = self.expected_job_policy.get('tests') or []
        policies.extend(policy_tests.values() if isinstance(policy_tests, dict) else policy_tests)
        self.remote_session_possible = bool(runtime.get('native_cluster_stop_required') or any(
            str((row.get('properties') or {}).get('distributionmethod')
                or row.get('distribution') or '').lower() not in ('', 'local', 'none')
            for row in policies if isinstance(row, dict)))
        # A native Command policy may prestart ICRP before maeRunSimulation.
        # Persist ownership before loading/opening the private setup; lack of a
        # candidate PATCHED/SUBMITTED marker is not proof of no remote workers.
        if self.remote_session_possible:
            scope = {
                'native_serial': True, 'startup_session': True,
                'submission_scope': 'owned_session_pool_may_prestart',
                'batch_id': identity['candidate_id'], 'candidate_id': identity['candidate_id'],
                'batch_root': self.control_root, 'candidate_root': self.control_root,
                'manifest_path': os.path.join(self.control_root, 'startup_session_ownership.json'),
                'protocol_identity': dict(identity), 'simulation_submitted': True,
                'native_completion_proven': False, 'native_stop_proven': False,
                'no_remote_jobs_started': False, 'abort_requested': False,
                'abort_marker_path': os.path.join(self.control_root, 'startup_session_stop.tsv'),
                'abort_script_path': os.path.join(self.control_root, 'startup_session_stop.il'),
            }
            atomic_write_text(scope['abort_marker_path'], '')
            atomic_write_json(scope['manifest_path'], scope)
            self._startup_remote_scope = scope
            self._active_batch = scope
        self.job_observation = {'wrappers': []}
        if snapshot.get('available') or bool(overrides.get('command')):
            from .resources import tool_path
            submission_root = runtime.get('run_root') or self.worker_root
            submission_log = os.path.join(submission_root, 'native_dispatch.jsonl')
            self.job_observation = prepare_job_command_observers(snapshot, overrides,
                os.path.join(self.control_root, 'job_observers'), submission_log,
                python_bin=sys.executable, helper_path=tool_path('maestro_job_observer.py'))
            self.config.setdefault('runtime', {})['maestro_submission_log_path'] = submission_log
        self.job_policy_readback_path = os.path.join(self.control_root, 'effective_job_policy.tsv')
        bootstrap_lines.append('crescBootstrapStage = "job_policy_apply_readback"')
        bootstrap_lines.extend(job_policy_application_skill(snapshot, overrides,
            session_expr='session', output_path=self.job_policy_readback_path,
            command_wrappers=self.job_observation.get('wrappers')).splitlines())
        startup_status = (
            'crescWriteWorkerStatus("%s" "%s" %d "%s" "%s"' % (
                _skill_quote(self.startup_marker), _skill_quote(identity["epoch"]), identity["sequence"],
                _skill_quote(identity["nonce"]), _skill_quote(identity["candidate_id"]),
            )
        )
        lines.append('crescBootstrapStage = "startup_load"')
        lines.append(startup_status + ' "STARTING" "")')
        bounded_lines, generation = bounded_skill_body_lines(bootstrap_lines, "bootstrapResult")
        lines.extend(bounded_lines)
        lines.extend([
            "if(bootstrapResult then",
            "  " + startup_status + ' "READY" "")',
            "else",
            "  crescWorkerTainted = t",
            "  " + startup_status + ' "STARTUP_ERROR" strcat("bootstrap failed at " crescBootstrapStage))',
            ")",
        ])
        atomic_write_text(path, "\n".join(lines) + "\n")
        atomic_write_json(os.path.join(self.control_root, "worker_manifest.json"), {
            "schema_version": WORKER_SCHEMA_VERSION,
            "epoch": self.epoch,
            "worker_index": self.worker_index,
            "workspace_root": self.workspace["root"],
            "clone_plan": self.clone_plan,
            "clone_records": self.clone_records,
            "pristine_cells": self.pristine_cells,
            "pristine_records": self.pristine_records,
            "startup_marker": self.startup_marker,
            "bootstrap_path": path,
            "bootstrap_generation": generation,
            "selected_parameter_count": len(self.parameters),
            "clone_count": len(self.clone_plan),
            "pristine_cell_count": len(self.pristine_cells),
            "native_corner_schedule_path": os.path.join(self.control_root, "native_corner_schedule.tsv"),
        })
        self._log_event("maestro_bootstrap", "Native startup file prepared", {
            "path": path, "file_bytes": os.path.getsize(path),
            "generation": generation, "selected_parameter_count": len(self.parameters),
            "clone_count": len(self.clone_plan), "pristine_cell_count": len(self.pristine_cells),
            "setup_identity": [self.workspace["primary_library"], self.workspace["setup_cell"],
                               self.workspace.get("setup_view") or "maestro"],
        })
        return path

    def _capture_process_group(self):
        deadline = self._clock() + 2.0
        last_group = None
        while self._clock() < deadline:
            try:
                last_group = self._getpgid(self.process.pid)
            except OSError:
                last_group = None
            if last_group == self.process.pid:
                return last_group
            if self.process.poll() is not None:
                break
            self._sleep(self.poll_interval)
        raise PersistentMaestroWorkerStartupError(
            "Ocean child did not establish its private process group: pid=%s pgid=%s" % (
                getattr(self.process, "pid", None), last_group,
            )
        )

    def _send_load(self, path):
        if not self.process or self.process.poll() is not None:
            raise PersistentMaestroWorkerProcessError("Ocean worker is not alive")
        try:
            self._log_event("maestro_load", "Loading native SKILL request", {
                "command": 'load("%s")' % _skill_quote(os.path.abspath(path)),
                "path": os.path.abspath(path), "file_bytes": os.path.getsize(path),
                "epoch": self.epoch, "sequence": self.sequence,
            })
            self.process.stdin.write('load("%s")\n' % _skill_quote(os.path.abspath(path)))
            self.process.stdin.flush()
        except Exception as error:
            raise PersistentMaestroWorkerProcessError(
                "failed to send SKILL request to Ocean: %s: %s" % (error.__class__.__name__, error)
            )

    def _wait_protocol(self, path, identity, terminal_states, timeout_seconds, operation,
                       wall_deadline=None, wall_timeout_seconds=None, abort_check=None):
        timeout_seconds = float(timeout_seconds)
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if wall_deadline is not None:
            wall_deadline = float(wall_deadline)
            wall_timeout_seconds = float(
                wall_timeout_seconds if wall_timeout_seconds is not None else timeout_seconds
            )
        progress_deadline = self._clock() + timeout_seconds
        observed = 0
        while True:
            if abort_check is not None:
                abort_check()
            rows = _matching_worker_protocol(path, identity)
            observer = getattr(self, '_live_progress', None)
            if observer is not None and identity.get('candidate_id') == observer.manifest.get('batch_id'):
                try:
                    live_rows = _matching_worker_protocol(observer.manifest['live_marker_path'], identity)
                    observed_states = [row['state'] for row in rows]
                    if live_rows and observed_states[-1:] == ['PATCHED']:
                        observed_states.append(live_rows[-1]['state'])
                    observer.poll(observed_states, force=bool(rows and rows[-1]['state'] in terminal_states))
                except Exception as progress_error:
                    if not getattr(self, '_live_progress_warned', False):
                        self._live_progress_warned = True
                        self._log_event('maestro_progress_unavailable', str(progress_error), level='WARNING')
            if len(rows) > observed:
                for row in rows[observed:]:
                    self._log_event("maestro_" + row["state"].lower(), row.get("message") or operation, {
                        "candidate_id": identity.get("candidate_id"), "epoch": identity.get("epoch"),
                        "sequence": identity.get("sequence"), "marker_path": path,
                    }, level="ERROR" if row["state"].endswith("ERROR") else "INFO")
                observed = len(rows)
                progress_deadline = self._clock() + timeout_seconds
            now = self._clock()
            if wall_deadline is not None and now >= wall_deadline:
                raise PersistentMaestroWorkerTimeout(
                    "%s exceeded its non-resettable total wall-clock deadline of %.3f seconds" % (
                        operation, wall_timeout_seconds,
                    )
                )
            if rows and rows[-1]["state"] in terminal_states:
                return rows
            returncode = self.process.poll() if self.process else None
            if returncode is not None:
                raise PersistentMaestroWorkerProcessError(
                    "%s Ocean worker exited with return code %s" % (operation, returncode)
                )
            if now >= progress_deadline:
                raise PersistentMaestroWorkerTimeout(
                    "%s made no fenced progress for %.3f seconds" % (operation, timeout_seconds)
                )
            self._sleep(self.poll_interval)

    def _candidate_supervision(self, candidate):
        supervision = candidate.get("candidate_supervision") or {}
        if supervision.get("deadline_monotonic") is None:
            return None
        return supervision

    def remaining_candidate_seconds(self, candidate):
        supervision = self._candidate_supervision(candidate)
        if supervision is None:
            return None
        return float(supervision["deadline_monotonic"]) - self._clock()

    def require_candidate_time(self, candidate, operation):
        supervision = self._candidate_supervision(candidate)
        if supervision is None:
            return None
        remaining = float(supervision["deadline_monotonic"]) - self._clock()
        if remaining <= 0:
            raise PersistentMaestroWorkerTimeout(
                "candidate %s exceeded its non-resettable total wall-clock deadline of %.3f "
                "seconds during %s" % (
                    candidate.get("candidate_id"), float(supervision["timeout_seconds"]), operation,
                )
            )
        return remaining

    def start(self, timeout_seconds=None):
        with self._operation_lock:
            if self.closed:
                raise PersistentMaestroWorkerError("closed worker cannot be started")
            if self.process and self.process.poll() is None:
                return self
            timeout_seconds = float(
                timeout_seconds if timeout_seconds is not None
                else self.config["runtime"].get("ocean_startup_timeout_seconds", 90)
            )
            lock_timeout = min(
                timeout_seconds,
                float(self.config["runtime"].get("command_timeout_seconds", 1800)),
            )
            acquired = CADENCE_STARTUP_LOCK.acquire(timeout=lock_timeout)
            if not acquired:
                raise PersistentMaestroWorkerStartupError("Cadence startup lock acquisition timed out")
            identity = {
                "epoch": self.epoch, "sequence": 0, "nonce": uuid.uuid4().hex,
                "candidate_id": "__startup__",
            }
            try:
                atomic_write_text(self.startup_marker, "")
                bootstrap = self._write_bootstrap(identity)
                self._stdout_handle = open(os.path.join(self.control_root, "ocean.stdout"), "ab")
                self._stderr_handle = open(os.path.join(self.control_root, "ocean.stderr"), "ab")
                if self._use_guardian:
                    guardian = guardian_script_path()
                    if not os.path.isfile(guardian):
                        raise PersistentMaestroWorkerStartupError(
                            "Ocean worker guardian is missing: %s" % guardian
                        )
                    if os.path.lexists(self.guardian_state_path):
                        raise PersistentMaestroWorkerStartupError(
                            "worker epoch already has guardian process state"
                        )
                    if os.path.lexists(self.guardian_launch_manifest_path):
                        raise PersistentMaestroWorkerStartupError(
                            "worker epoch already has guardian launch state"
                        )
                    ocean_command = [self.config["cadence"]["ocean_bin"], "-nograph"]
                    launch_lock_fd = None
                    try:
                        launch_lock_fd, launch_lock_path = acquire_legacy_guardian_launch_lock(
                            self.control_root,
                        )
                        parent_identity = legacy_guardian_parent_identity()
                        command = [
                            sys.executable, guardian,
                            "--state-path", self.guardian_state_path,
                            "--stdout-path", os.path.join(self.control_root, "ocean.stdout"),
                            "--stderr-path", os.path.join(self.control_root, "ocean.stderr"),
                            "--token", self.guardian_token,
                            "--cwd", self.workspace["root"],
                            "--terminate-grace-seconds", str(self._guardian_terminate_grace_seconds),
                            "--expected-parent-pid", str(parent_identity["pid"]),
                            "--launch-lock-fd", str(launch_lock_fd),
                            "--",
                        ]
                        command.extend(ocean_command)
                        guardian_environment = _guardian_environment(self._environment())
                        guardian_environment[GUARDIAN_LAUNCH_TOKEN_ENVIRONMENT] = self.guardian_token
                        write_legacy_guardian_manifest(
                            self.control_root, self.workspace["root"], self.guardian_state_path,
                            os.path.join(self.control_root, "ocean.stdout"),
                            os.path.join(self.control_root, "ocean.stderr"),
                            self.guardian_token, ocean_command, parent_identity["pid"], guardian,
                            launch_lock_path=launch_lock_path,
                            expected_parent_boot_id=parent_identity["boot_id"],
                            expected_parent_start_ticks=parent_identity["start_ticks"],
                            expected_uid=parent_identity["uid"], guardian_argv=command,
                            guardian_token_identity=self.guardian_token_identity,
                        )
                        self.state = "launching"
                        self.active_identity = identity
                        self._write_state("durable guardian launch intent recorded before Popen")
                        command_launch_lock_fd = launch_lock_fd
                        launch_lock_fd = None
                        try:
                            self._log_event("maestro_launch", "Launching native Ocean worker", {
                                "command": ocean_command, "guardian_command": command,
                                "cwd": self.workspace["root"], "bootstrap_path": bootstrap,
                            })
                            self.process = self._popen_factory(
                                command, cwd=self.workspace["root"], env=guardian_environment,
                                stdin=subprocess.PIPE, stdout=self._stdout_handle,
                                stderr=self._stderr_handle, start_new_session=True,
                                universal_newlines=True, bufsize=1, close_fds=True,
                                pass_fds=(command_launch_lock_fd,),
                            )
                        except Exception as error:
                            try:
                                self._write_guardian_launch_state(
                                    ocean_command, "launch_failed", error=error,
                                    parent_identity=parent_identity,
                                    launch_lock_fd=command_launch_lock_fd,
                                )
                            finally:
                                close_legacy_guardian_launch_lock(command_launch_lock_fd)
                                command_launch_lock_fd = None
                            raise
                        close_legacy_guardian_launch_lock(command_launch_lock_fd)
                        command_launch_lock_fd = None
                    finally:
                        if launch_lock_fd is not None:
                            close_legacy_guardian_launch_lock(launch_lock_fd)
                    self.state = "started"
                    self._write_state("guardian Popen returned; Ocean cleanup is not yet proven")
                else:
                    self._log_event("maestro_launch", "Launching native Ocean worker", {
                        "command": [self.config["cadence"]["ocean_bin"], "-nograph"],
                        "cwd": self.workspace["root"], "bootstrap_path": bootstrap,
                    })
                    self.process = self._popen_factory(
                        [self.config["cadence"]["ocean_bin"], "-nograph"],
                        cwd=self.workspace["root"], env=self._environment(), stdin=subprocess.PIPE,
                        stdout=self._stdout_handle, stderr=self._stderr_handle,
                        preexec_fn=os.setsid, universal_newlines=True, bufsize=1, close_fds=True,
                    )
                self._pgid = self._capture_process_group()
                self.state = "starting"
                self.active_identity = identity
                self._write_state()
                self._send_load(bootstrap)
                rows = self._wait_protocol(
                    self.startup_marker, identity, ("READY", "STARTUP_ERROR"), timeout_seconds, "startup",
                )
                terminal = _validate_worker_protocol(rows, ("STARTING", "READY"), "STARTUP_ERROR")
                if terminal == "STARTUP_ERROR":
                    raise PersistentMaestroWorkerStartupError(
                        rows[-1].get("message") or "Maestro worker bootstrap failed"
                    )
                validate_batch_clones({"clone_plan": self.clone_plan, "clone_records": self.clone_records,
                                       "config_binding_plan": private_config_plan(self.workspace, self.clone_plan),
                                       "config_verification_path": self.clone_records + ".config"})
                _validate_pristine_records(
                    self.pristine_cells, self.pristine_records, "PRISTINE",
                )
                if self.workspace.get("native_storage"):
                    load_native_storage_readback(self.workspace, required=self._use_guardian)
                expected_policy = getattr(self, 'expected_job_policy', {})
                if expected_policy.get('available') or bool(expected_policy.get('overrides')):
                    from .job_policy import parse_policy_records, verify_effective_job_policy
                    with open(self.job_policy_readback_path, 'r') as policy_stream:
                        policy_raw = policy_stream.read(512 * 1024 + 1)
                    if len(policy_raw) > 512 * 1024:
                        raise PersistentMaestroWorkerStartupError('Native job policy diagnostics exceed size limit')
                    readback = parse_policy_records(policy_raw, source='private_native_readback')
                    verified = verify_effective_job_policy(readback, self.expected_job_policy,
                        command_wrappers=self.job_observation.get('wrappers'))
                    verified['policy_provenance'] = {
                        'captured_available': bool(expected_policy.get('available')),
                        'capture_source': expected_policy.get('source'),
                        'validation_scope': ('captured_policy_and_overrides'
                                             if expected_policy.get('available') else 'explicit_overrides_only'),
                        'known_override_fields': sorted((expected_policy.get('overrides') or {}).keys()),
                    }
                    atomic_write_json(os.path.join(self.control_root, 'effective_job_policy.json'), verified)
                    self._log_event('maestro_effective_job_policy',
                        'Private Maestro Job Setup applied and read back', verified)
                    if not verified['ok']:
                        raise PersistentMaestroWorkerStartupError('Private Maestro Job Setup differs; no simulation submitted: %s' % verified['differences'])
                technology_manifest = self.workspace.get("technology") or {}
                if workspace_technology_validation_required(technology_manifest):
                    validate_workspace_technology(
                        technology_manifest, require_complete=True,
                    )
                schedule_path = os.path.join(self.control_root, "native_corner_schedule.tsv")
                if self._use_guardian or os.path.isfile(schedule_path):
                    from .private_netlist import _capture_setup_identity, parse_native_corner_schedule
                    snapshot = _capture_setup_identity(self.workspace, self.control_root, config=self.config)
                    schedule = parse_native_corner_schedule(schedule_path)
                    self.workspace["native_corner_identity"] = {"setup_snapshot": snapshot,
                                                                "schedule": schedule,
                                                                "point_parameters_required": True}
                    atomic_write_json(os.path.join(self.workspace["root"], "workspace.json"), self.workspace)
                    self._log_event("maestro_corner_identity", "Native source corner groups qualified before candidate tests", {
                        "schedule": schedule, "setup_snapshot_path": os.path.join(self.control_root, "corner_identity.json"),
                    })
                self.state = "ready"
                self.active_identity = None
                self._write_state()
                return self
            except Exception as error:
                self.tainted = True
                self.state = "failed"
                self._terminate_after_failure("startup failure", error,
                    request_native_stop=getattr(self, 'remote_session_possible', False))
                if isinstance(error, PersistentMaestroWorkerStartupError):
                    raise
                raise PersistentMaestroWorkerStartupError(
                    "%s: %s" % (error.__class__.__name__, error)
                )
            finally:
                CADENCE_STARTUP_LOCK.release()

    def _candidate_artifact(self, candidate_id, values, candidate_root, identity):
        candidate_root = ensure_dir(os.path.abspath(candidate_root))
        rows = _candidate_rows(self.workspace, self.parameters, dict(values or {}), self.clone_lookup)
        adapter_path = self._adapter_path()
        plans = _candidate_plans(rows, candidate_root, not adapter_path)
        history = _history_identity(self.workspace, candidate_root)
        marker_path = os.path.join(candidate_root, "persistent_worker_status.tsv")
        script_path = os.path.join(candidate_root, "persistent_worker_candidate.il")
        pristine_restore_path = os.path.join(candidate_root, "pristine_restore.records")
        atomic_write_text(marker_path, "")
        artifact = {
            "candidate_id": str(candidate_id), "candidate_root": candidate_root,
            "values": dict(values or {}), "history_name": history["history_name"],
            "requested_history_name": history["history_name"],
            "history_identity": history,
            "rdb_path": workspace_rdb_path(self.workspace, history["history_name"]),
            "marker_path": marker_path, "script_path": script_path,
            "patch_plan": plans, "protocol_identity": dict(identity),
            "worker_epoch": self.epoch, "worker_index": self.worker_index,
            "pristine_cells": list(self.pristine_cells),
            "pristine_restore_path": pristine_restore_path,
            "native_corner_identity": self.workspace.get("native_corner_identity"),
        }
        native_serial = bool(self.config.get("runtime", {}).get("native_cluster_stop_required"))
        if native_serial:
            artifact.update({
                "native_serial": True, "batch_id": str(candidate_id),
                "batch_root": candidate_root,
                "manifest_path": os.path.join(candidate_root, "persistent_worker_candidate.json"),
                "simulation_submitted": bool(getattr(self, 'remote_session_possible', False)),
                "native_completion_proven": False,
                "native_stop_proven": False,
                "no_remote_jobs_started": not bool(getattr(self, 'remote_session_possible', False)),
                "abort_requested": False,
                "abort_marker_path": os.path.join(candidate_root, "persistent_worker_batch_abort.tsv"),
                "abort_script_path": os.path.join(candidate_root, "persistent_worker_batch_abort.il"),
                "finish_script_path": os.path.join(candidate_root, "persistent_worker_candidate_finish.il"),
            })
            atomic_write_text(artifact["abort_marker_path"], "")
        status = (
            'crescWriteWorkerStatus("%s" "%s" %d "%s" "%s"' % (
                _skill_quote(marker_path), _skill_quote(identity["epoch"]), identity["sequence"],
                _skill_quote(identity["nonce"]), _skill_quote(identity["candidate_id"]),
            )
        )
        lines = [
            'unless(equal(crescWorkerEpoch "%s") error("Crescendo worker epoch mismatch\\n"))' % _skill_quote(self.epoch),
            'when(crescWorkerTainted error("Crescendo worker is tainted\\n"))',
            status + ' "ACCEPTED" "")',
            "candidateResult = errset(progn(",
            "  " + status + ' "BEGIN" "")',
        ]
        if native_serial:
            lines.append('  unless(isCallable(\'maeStopSimulation) && isCallable(\'maeWaitUntilDone) error("Crescendo cluster stop APIs unavailable; no simulation submitted\\n"))')
        lines.extend("  " + row for row in _pristine_restore_lines(
            self.pristine_cells, pristine_restore_path,
        ))
        lines.extend("  " + row for row in _snapshot_lines(
            plans["baseline"]["parameters"], plans["baseline"]["verification_path"], "BASELINE",
        ))
        lines.extend("  " + row for row in _patch_lines(
            plans["parameters"]["parameters"], plans["parameters"]["verification_path"], adapter_path,
        ))
        lines.extend("  " + row for row in _snapshot_lines(
            plans["verify"]["parameters"], plans["verify"]["verification_path"], "POST",
        ))
        from .job_policy import native_job_policy_assertion_skill, job_policy_readback_skill
        source_tests = [test['name'] for test in self.workspace.get('setup', {}).get('tests') or []
                        if test.get('enabled', True)]
        lines.extend('  ' + line for line in job_policy_readback_skill(
            os.path.join(candidate_root, 'effective_job_policy.tsv'), session_expr='session', tests=source_tests).splitlines())
        lines.extend('  ' + line for line in native_job_policy_assertion_skill(
            getattr(self, 'expected_job_policy', {}), session_expr='session',
            test_map=dict((name, name) for name in source_tests),
            command_wrappers=getattr(self, 'job_observation', {}).get('wrappers')).splitlines())
        result_lines = [
            "  runData = axlGetRunData(session runId)",
            '  unless(runData error("Crescendo cannot obtain run data\\n"))',
            "  actualHistoryName = axlGetHistoryName(runData)",
            '  unless(stringp(actualHistoryName) && actualHistoryName != "" error("Crescendo cannot identify completed history\\n"))',
            "  if(crescExpectedHistoryName then",
            '    unless(equal(actualHistoryName crescExpectedHistoryName) error("Crescendo native history overwrite did not reuse the expected history\\n"))',
            "  else",
            "    crescExpectedHistoryName = actualHistoryName",
            "  )",
            "  " + status + ' "RESULTS_READY" actualHistoryName)',
            "  crescLastRunId = runId",
            "  crescLastHistoryName = actualHistoryName",
            '  crescLastEpoch = "%s"' % _skill_quote(identity["epoch"]),
            "  crescLastSequence = %d" % identity["sequence"],
            '  crescLastNonce = "%s"' % _skill_quote(identity["nonce"]),
            '  crescLastCandidate = "%s"' % _skill_quote(identity["candidate_id"]),
            "  " + status + ' "DONE" actualHistoryName)',
        ]
        storage_line = native_history_readback_line(self.workspace, candidate_root)
        if storage_line:
            result_lines.insert(-1, "  " + storage_line)
        lines.append("  " + status + ' "PATCHED" "")')
        if native_serial:
            lines.extend([
                "  procedure(crescCandidateFinished()",
                "    " + status + ' "SIM_DONE" "")',
                "  )",
                '  runId = maeRunSimulation(?waitUntilDone nil ?returnRunId t ?callback "crescCandidateFinished()" ?session session)',
                '  unless(runId error("Crescendo candidate async submission rejected\\n"))',
            ])
            finish_lines = [
                'unless(equal(crescWorkerEpoch "%s") error("Crescendo candidate finish epoch mismatch\\n"))' % _skill_quote(self.epoch),
                "candidateFinishResult = errset(progn(",
                "  maeWaitUntilDone('All ?session session)",
            ] + result_lines + [
                "  t", ") t)",
                "unless(candidateFinishResult",
                "  crescWorkerTainted = t",
                "  " + status + ' "ERROR" "candidate result finalization failed")',
                ")",
            ]
            atomic_write_text(artifact["finish_script_path"], "\n".join(finish_lines) + "\n")
        else:
            lines.extend([
                "  runId = maeRunSimulation(?waitUntilDone t ?returnRunId t ?session session)",
                "  " + status + ' "SIM_DONE" "")',
            ] + result_lines)
        lines.extend([
            "  t",
            ") t)",
            "unless(candidateResult",
            "  crescWorkerTainted = t",
            "  " + status + ' "ERROR" "candidate failed")',
            ")",
        ])
        body_start = lines.index("candidateResult = errset(progn(")
        body_end = lines.index(") t)", body_start + 1)
        body = [row[2:] if row.startswith("  ") else row
                for row in lines[body_start + 1:body_end]]
        bounded, generation = bounded_skill_body_lines(body, "candidateResult")
        lines[body_start:body_end + 1] = bounded
        artifact["candidate_generation"] = generation
        atomic_write_text(script_path, "\n".join(lines) + "\n")
        atomic_write_json(os.path.join(candidate_root, "persistent_worker_candidate.json"), artifact)
        return artifact

    def evaluate(self, candidate_id, values, candidate_root, timeout_seconds=None):
        with self._operation_lock:
            native_serial = bool(self.config.get("runtime", {}).get("native_cluster_stop_required"))
            if native_serial:
                with self._batch_submit_lock:
                    if self._batch_abort_before_submit:
                        raise PersistentMaestroWorkerError("candidate session was aborted before submission")
            if self.closed:
                raise PersistentMaestroWorkerError("worker is closed")
            if self.tainted:
                raise PersistentMaestroWorkerTainted("worker epoch is tainted and must be replaced")
            if self.pending_cleanup is not None:
                raise PersistentMaestroWorkerError("previous candidate results must be cleaned before reuse")
            if not self.process or self.process.poll() is not None or self.state != "ready":
                raise PersistentMaestroWorkerProcessError("worker is not ready")
            timeout_seconds = float(
                timeout_seconds if timeout_seconds is not None
                else self.config["runtime"].get("candidate_timeout_seconds", 600)
            )
            if timeout_seconds <= 0:
                raise ValueError("candidate timeout_seconds must be positive")
            no_progress_timeout = float(
                self.config["runtime"].get(
                    "candidate_no_progress_timeout_seconds", timeout_seconds,
                )
            )
            if no_progress_timeout <= 0:
                raise ValueError("candidate_no_progress_timeout_seconds must be positive")
            started_monotonic = self._clock()
            deadline_monotonic = started_monotonic + timeout_seconds
            started = time.time()
            self.sequence += 1
            identity = {
                "epoch": self.epoch, "sequence": self.sequence, "nonce": uuid.uuid4().hex,
                "candidate_id": str(candidate_id),
            }
            self.active_identity = identity
            self.state = "busy"
            self._write_state()
            try:
                artifact = self._candidate_artifact(candidate_id, values, candidate_root, identity)
                from .maestro_progress import MaestroProgressObserver
                source_tests = [test['name'] for test in self.workspace.get('setup', {}).get('tests') or []
                                if test.get('enabled', True)]
                observation = dict(artifact, batch_id=str(candidate_id), batch_root=candidate_root,
                    live_marker_path=artifact['marker_path'], serial_observation=True,
                    candidates=[dict(artifact, source_test_map=dict((name, name) for name in source_tests))],
                    job_policy_readback_path=os.path.join(candidate_root, 'effective_job_policy.tsv'),
                    submission_log_path=self.config.get('runtime', {}).get('maestro_submission_log_path'))
                live_root = self.config.get('runtime', {}).get('run_root') or self.worker_root
                self._live_progress = MaestroProgressObserver(live_root, self.workspace, observation, self.control_root)
                self._live_progress.poll([], force=True)
                self._live_progress_warned = False
                artifact["candidate_supervision"] = {
                    "timeout_seconds": timeout_seconds,
                    "no_progress_timeout_seconds": no_progress_timeout,
                    "started_monotonic": started_monotonic,
                    "deadline_monotonic": deadline_monotonic,
                }
                if native_serial:
                    self._active_batch = artifact
                    self._write_state()
                    atomic_write_json(artifact["manifest_path"], artifact)
                def require_not_stopped():
                    if native_serial and artifact.get("abort_requested") and artifact.get("native_stop_proven"):
                        raise PersistentMaestroWorkerError(
                            "candidate was stopped by request; native cancellation acknowledged")
                self.require_candidate_time(artifact, "candidate patch and request preparation")
                atomic_write_text(
                    self.spectre_retain_pointer,
                    os.path.abspath(candidate_root) + "\n",
                )
                if native_serial:
                    with self._batch_submit_lock:
                        if self._batch_abort_before_submit or artifact.get("abort_requested"):
                            raise PersistentMaestroWorkerError("candidate aborted before submission")
                        artifact["simulation_submitted"] = True
                        artifact["no_remote_jobs_started"] = False
                        atomic_write_json(artifact["manifest_path"], artifact)
                        self._write_state()
                        self._send_load(artifact["script_path"])
                else:
                    self._send_load(artifact["script_path"])
                rows = self._wait_protocol(
                    artifact["marker_path"], identity,
                    ("SIM_DONE", "ERROR") if native_serial else ("DONE", "ERROR"), no_progress_timeout,
                    "candidate %s" % candidate_id,
                    wall_deadline=deadline_monotonic, wall_timeout_seconds=timeout_seconds,
                    abort_check=require_not_stopped if native_serial else None,
                )
                if native_serial and rows[-1]["state"] == "SIM_DONE":
                    self._send_load(artifact["finish_script_path"])
                    rows = self._wait_protocol(
                        artifact["marker_path"], identity, ("DONE", "ERROR"), no_progress_timeout,
                        "candidate %s result finalization" % candidate_id,
                        wall_deadline=deadline_monotonic, wall_timeout_seconds=timeout_seconds,
                        abort_check=require_not_stopped,
                    )
                terminal = _validate_worker_protocol(rows, self.CANDIDATE_STATES, "ERROR")
                self.require_candidate_time(artifact, "candidate protocol validation")
                if terminal == "DONE":
                    actual_history_name = str(rows[-1].get("message") or "").strip()
                    if (
                        not actual_history_name
                        or not re.match(r"^[A-Za-z0-9_.-]+$", actual_history_name)
                        or actual_history_name in (".", "..")
                    ):
                        raise PersistentMaestroWorkerProtocolError(
                            "worker returned an unsafe or empty completed history name"
                        )
                    artifact["history_name"] = actual_history_name
                    artifact["rdb_path"] = workspace_rdb_path(self.workspace, actual_history_name)
                    native_paths = qualify_native_history_paths(self.workspace, candidate_root, actual_history_name, started_epoch=started, required=self._use_guardian)
                    if native_paths:
                        artifact["native_storage_paths"] = native_paths
                        artifact["rdb_path"] = native_paths.get("rdb_path") or artifact["rdb_path"]
                    artifact["history_identity"] = dict(artifact["history_identity"])
                    artifact["history_identity"]["requested_history_name"] = artifact[
                        "requested_history_name"
                    ]
                    artifact["history_identity"]["history_name"] = actual_history_name
                    atomic_write_json(
                        os.path.join(candidate_root, "persistent_worker_candidate.json"), artifact,
                    )
                artifact["status"] = rows
                artifact["states"] = [row["state"] for row in rows]
                artifact["started_epoch"] = started
                artifact["finished_epoch"] = time.time()
                artifact["elapsed_seconds"] = max(0.0, artifact["finished_epoch"] - started)
                if terminal == "ERROR":
                    if (native_serial and "PATCHED" not in artifact["states"]
                            and not getattr(self, 'remote_session_possible', False)):
                        artifact["simulation_submitted"] = False
                        artifact["no_remote_jobs_started"] = True
                    self.tainted = True
                    self.state = "tainted"
                else:
                    if native_serial:
                        artifact["native_completion_proven"] = True
                    self.pending_cleanup = dict(identity)
                    self.state = "awaiting_cleanup"
                atomic_write_json(os.path.join(candidate_root, "persistent_worker_candidate.json"), artifact)
                self.active_identity = None
                self._write_state()
                return artifact
            except Exception as error:
                self.tainted = True
                self.state = "tainted"
                self._terminate_after_failure("candidate supervision failure", error,
                    request_native_stop=native_serial)
                raise

    def evaluate_batch(self, candidates, batch_root, timeout_seconds=None):
        """Run independently patched candidate designs as tests in one Maestro.

        One submit call permits Maestro's shared global policy to queue
        any number of candidate tests, irrespective of population vs max jobs.
        The complete shared history remains fenced until one batch cleanup ACK.
        """
        from .maestro_batch import (
            MaestroBatchUnsupported, prepare_snapshot_batch, persist_batch_artifacts,
            snapshot_preflight_lines, snapshot_test_lines,
        )
        from .job_policy import job_policy_override_definitions
        with self._operation_lock:
            with self._batch_submit_lock:
                if self._batch_abort_before_submit:
                    raise PersistentMaestroWorkerError("batch session was aborted before submission")
            if self.closed or self.tainted:
                raise PersistentMaestroWorkerTainted("batch worker is closed or tainted")
            if self.pending_cleanup is not None:
                raise PersistentMaestroWorkerError("previous results require cleanup before batch")
            if not self.process or self.process.poll() is not None or self.state != "ready":
                raise PersistentMaestroWorkerProcessError("batch worker is not ready")
            timeout_seconds = float(timeout_seconds if timeout_seconds is not None else
                                    self.config["runtime"].get("maestro_batch_timeout_seconds", 86400))
            if timeout_seconds <= 0:
                raise ValueError("batch timeout_seconds must be positive")
            self.sequence += 1
            identity = {"epoch": self.epoch, "sequence": self.sequence,
                        "nonce": uuid.uuid4().hex,
                        "candidate_id": "batch_%d_%s" % (self.sequence, uuid.uuid4().hex[:10])}
            # Pure capability/planning failures occur before mutation and are safe
            # for the controller to fall back to serial in the same session.
            manifest = prepare_snapshot_batch(self, candidates, batch_root, identity)
            manifest["candidate_id"] = identity["candidate_id"]
            manifest["simulation_submitted"] = bool(getattr(self, 'remote_session_possible', False))
            manifest["native_completion_proven"] = False
            manifest["native_stop_proven"] = False
            manifest["no_remote_jobs_started"] = not bool(getattr(self, 'remote_session_possible', False))
            manifest["abort_requested"] = False
            manifest["worker_epoch"] = self.epoch
            manifest["worker_index"] = self.worker_index
            manifest["candidate_root"] = manifest["batch_root"]
            manifest["pristine_cells"] = list(self.pristine_cells)
            manifest["pristine_restore_path"] = os.path.join(manifest["batch_root"], "batch_pristine_restore.records")
            manifest["abort_marker_path"] = os.path.join(manifest["batch_root"], "persistent_worker_batch_abort.tsv")
            manifest["abort_script_path"] = os.path.join(manifest["batch_root"], "persistent_worker_batch_abort.il")
            manifest['live_marker_path'] = os.path.join(manifest['batch_root'], 'native_live.tsv')
            atomic_write_text(manifest['live_marker_path'], '')
            manifest['job_policy_readback_path'] = os.path.join(manifest['batch_root'], 'persistent_worker_batch_job_policy.tsv')
            manifest['submission_log_path'] = self.config.get('runtime', {}).get('maestro_submission_log_path')
            status = ('crescWriteWorkerStatus("%s" "%s" %d "%s" "%s"' % (
                _skill_quote(manifest["marker_path"]), _skill_quote(identity["epoch"]),
                identity["sequence"], _skill_quote(identity["nonce"]),
                _skill_quote(identity["candidate_id"])))
            lines = job_policy_override_definitions(
                self.config.get("runtime", {}).get("maestro_job_overrides") or {}).splitlines()
            lines.extend([
                'unless(equal(crescWorkerEpoch "%s") error("Crescendo batch epoch mismatch\\n"))' % _skill_quote(self.epoch),
                'when(crescWorkerTainted error("Crescendo batch worker is tainted\\n"))',
                status + ' "ACCEPTED" "")',
            ])
            lines.extend(snapshot_preflight_lines(manifest))
            lines.extend([
                "if(crescBatchUnsupportedReason then",
                "  " + status + ' "BATCH_UNSUPPORTED" crescBatchUnsupportedReason)',
                "else",
                "  batchResult = errset(progn(",
                "    " + status + ' "BEGIN" "")',
                '    crescBatchSnapshotVerify = outfile("%s")' % _skill_quote(manifest["snapshot_records"]),
                '    unless(crescBatchSnapshotVerify error("Crescendo batch snapshot evidence unavailable\\n"))',
            ])
            lines.extend("    " + row for row in _pristine_restore_lines(
                self.pristine_cells, manifest["pristine_restore_path"]))
            lines.extend("    " + row for row in manifest["snapshot_lines"])
            lines.extend("    " + row for row in snapshot_test_lines(manifest))
            from .job_policy import (job_policy_observation_skill, job_policy_readback_skill,
                                     native_job_policy_assertion_skill)
            test_map = dict((test['name'], test['source']) for test in manifest['batch_tests'])
            wrappers = getattr(self, 'job_observation', {}).get('wrappers') or []
            lines.extend('    ' + row for row in job_policy_observation_skill(wrappers,
                session_expr='session', test_map=test_map).splitlines())
            lines.extend([
                '    unless(isCallable(\'axlSetRunDistributeOptions) && isCallable(\'axlGetRunDistributeOptions) error("Crescendo batch native parallel Run Options unavailable; no simulation submitted\\n"))',
                '    unless(axlSetRunDistributeOptions(activeSetup ?RunIn \'Parallel) error("Crescendo batch native parallel Run Options rejected; no simulation submitted\\n"))',
                '    unless(equal(get(axlGetRunDistributeOptions(activeSetup) \'RunIn) \'Parallel) error("Crescendo batch native parallel Run Options differ; no simulation submitted\\n"))',
            ])
            lines.extend('    ' + row for row in job_policy_readback_skill(manifest['job_policy_readback_path'],
                session_expr='session', tests=list(test_map)).splitlines())
            lines.extend('    ' + row for row in native_job_policy_assertion_skill(
                getattr(self, 'expected_job_policy', {}), session_expr='session', test_map=test_map,
                command_wrappers=wrappers, force_parallel=True).splitlines())
            lines.extend([
                "    close(crescBatchSnapshotVerify)",
                "    crescBatchSnapshotVerify = nil",
                "    " + status + ' "PATCHED" "")',
                "    procedure(crescBatchFinished()",
                "      " + status + ' "SIM_DONE" "")',
                "    )",
                '    runId = maeRunSimulation(?waitUntilDone nil ?returnRunId t ?callback "crescBatchFinished()" ?session session)',
                '    unless(runId error("Crescendo batch simulation submission rejected\\n"))',
                status.replace(_skill_quote(manifest['marker_path']), _skill_quote(manifest['live_marker_path'])) + ' "SUBMITTED" sprintf(nil "%L" runId))',
                "    t",
                "  ) t)",
                "  unless(batchResult",
                "    crescWorkerTainted = t",
                "    " + status + ' "ERROR" "batch preparation or submission failed")',
                "  )",
                ")",
            ])
            finish_path = os.path.join(manifest["batch_root"], "persistent_worker_batch_finish.il")
            finish_lines = [
                "batchFinishResult = errset(progn(",
                "  maeWaitUntilDone('All ?session session)",
                "  runData = axlGetRunData(session runId)",
                '  unless(runData error("Crescendo batch run data unavailable\\n"))',
                "  actualHistoryName = axlGetHistoryName(runData)",
                '  unless(stringp(actualHistoryName) && actualHistoryName != "" error("Crescendo batch history unavailable\\n"))',
                "  if(crescExpectedHistoryName then",
                '    unless(equal(actualHistoryName crescExpectedHistoryName) error("Crescendo batch native overwrite history mismatch\\n"))',
                "  else crescExpectedHistoryName = actualHistoryName)",
                "  " + status + ' "RESULTS_READY" actualHistoryName)',
                "  crescLastRunId = runId",
                "  crescLastHistoryName = actualHistoryName",
                '  crescLastEpoch = "%s"' % _skill_quote(identity["epoch"]),
                "  crescLastSequence = %d" % identity["sequence"],
                '  crescLastNonce = "%s"' % _skill_quote(identity["nonce"]),
                '  crescLastCandidate = "%s"' % _skill_quote(identity["candidate_id"]),
                "  " + status + ' "DONE" actualHistoryName)',
                "  t",
                ") t)",
                "unless(batchFinishResult",
                "  crescWorkerTainted = t",
                "  " + status + ' "ERROR" "batch result finalization failed")',
                ")",
            ]
            atomic_write_text(manifest["marker_path"], "")
            atomic_write_text(manifest["abort_marker_path"], "")
            guard_start = lines.index("if(crescBatchUnsupportedReason then")
            body_start = lines.index("  batchResult = errset(progn(", guard_start)
            body_end = lines.index("  ) t)", body_start + 1)
            body = [row[4:] if row.startswith("    ") else row
                    for row in lines[body_start + 1:body_end]]
            from .maestro_batch import shared_skill_expressions
            shared_definitions, body, shared_generation = shared_skill_expressions(body)
            bounded, generation = bounded_skill_body_lines(body, "batchResult")
            generation["shared_expressions"] = shared_generation
            bounded[0] = "batchResult = unless(crescBatchUnsupportedReason list(t))"
            lines = lines[:guard_start] + shared_definitions + bounded + [
                "if(crescBatchUnsupportedReason then",
                "  " + status + ' "BATCH_UNSUPPORTED" crescBatchUnsupportedReason)',
                "else",
                "  unless(batchResult",
                "    crescWorkerTainted = t",
                "    " + status + ' "ERROR" "batch preparation or submission failed")',
                "  )",
                ")",
            ]
            manifest["batch_generation"] = generation
            atomic_write_text(manifest["script_path"], "\n".join(lines) + "\n")
            storage_line = native_history_readback_line(self.workspace, manifest["batch_root"])
            if storage_line:
                done_index = next(index for index, line in enumerate(finish_lines) if ' "DONE" actualHistoryName)' in line)
                finish_lines.insert(done_index, "  " + storage_line)
            atomic_write_text(finish_path, "\n".join(finish_lines) + "\n")
            started = time.time()
            deadline = self._clock() + timeout_seconds
            supervision = {"timeout_seconds": timeout_seconds,
                           "no_progress_timeout_seconds": timeout_seconds,
                           "started_monotonic": self._clock(), "deadline_monotonic": deadline,
                           "kind": "maestro_batch_including_scheduler_queue"}
            manifest["candidate_supervision"] = supervision
            self.active_identity = identity
            self._active_batch = manifest
            from .maestro_progress import MaestroProgressObserver
            live_root = self.config.get('runtime', {}).get('run_root') or os.path.dirname(os.path.dirname(batch_root))
            self._live_progress = MaestroProgressObserver(live_root, self.workspace, manifest, self.control_root)
            self._live_progress.poll([], force=True)
            self._live_progress_warned = False
            self.state = "busy"
            self._write_state()
            persist_batch_artifacts(manifest)
            def require_not_stopped():
                if manifest.get("abort_requested") and manifest.get("native_stop_proven"):
                    raise PersistentMaestroWorkerError(
                        "batch was stopped by request; native cancellation acknowledged")
            try:
                atomic_write_text(self.spectre_retain_pointer, manifest["batch_root"] + "\n")
                with self._batch_submit_lock:
                    if self._batch_abort_before_submit or manifest.get("abort_requested"):
                        raise PersistentMaestroWorkerError("batch aborted before submission")
                    # Conservative ownership marker: once this load is sent it
                    # may submit remotely. Only native DONE/STOP_DONE clears it.
                    manifest["simulation_submitted"] = True
                    manifest["no_remote_jobs_started"] = False
                    persist_batch_artifacts(manifest)
                    self._write_state()
                    self._send_load(manifest["script_path"])
                rows = self._wait_protocol(
                    manifest["marker_path"], identity,
                    ("SIM_DONE", "ERROR", "BATCH_UNSUPPORTED"), timeout_seconds,
                    "batch %s" % identity["candidate_id"], wall_deadline=deadline,
                    wall_timeout_seconds=timeout_seconds,
                    abort_check=require_not_stopped,
                )
                if rows[-1]["state"] == "BATCH_UNSUPPORTED":
                    manifest["simulation_submitted"] = bool(getattr(self, 'remote_session_possible', False))
                    manifest["no_remote_jobs_started"] = not manifest["simulation_submitted"]
                    manifest["status"] = rows
                    persist_batch_artifacts(manifest)
                    self.active_identity = None
                    self._active_batch = getattr(self, '_startup_remote_scope', None)
                    self.state = "ready"
                    self._write_state(rows[-1].get("message"))
                    raise MaestroBatchUnsupported(rows[-1].get("message") or "native capability unavailable")
                if rows[-1]["state"] == "SIM_DONE":
                    self._send_load(finish_path)
                    rows = self._wait_protocol(
                        manifest["marker_path"], identity, ("DONE", "ERROR"), timeout_seconds,
                        "batch result finalization", wall_deadline=deadline,
                        wall_timeout_seconds=timeout_seconds,
                        abort_check=require_not_stopped,
                    )
                terminal = _validate_worker_protocol(rows, self.CANDIDATE_STATES, "ERROR")
                manifest["status"] = rows
                manifest["states"] = [row["state"] for row in rows]
                manifest["started_epoch"] = started
                manifest["finished_epoch"] = time.time()
                manifest["elapsed_seconds"] = max(0.0, manifest["finished_epoch"] - started)
                if terminal == "ERROR":
                    from .maestro_progress import tail_text
                    diagnostic_root = self.config.get('runtime', {}).get('run_root') or self.worker_root
                    manifest['native_error'] = '\n'.join(tail_text(os.path.join(self.control_root, name), diagnostic_root, 16384)
                                                         for name in ('ocean.stdout', 'ocean.stderr'))
                    self._log_event('maestro_native_batch_error', 'Native batch failed before result acceptance',
                        {'batch_id': manifest['batch_id'], 'native_error': manifest['native_error']}, level='ERROR')
                    if ("PATCHED" not in manifest["states"]
                            and not getattr(self, 'remote_session_possible', False)):
                        # The fenced script has not reached its sole submit call.
                        manifest["simulation_submitted"] = False
                        manifest["no_remote_jobs_started"] = True
                    self.tainted = True
                    self.state = "tainted"
                    for candidate in manifest["candidates"]:
                        candidate.update({"status": rows, "states": manifest["states"],
                                          "started_epoch": started, "finished_epoch": manifest["finished_epoch"],
                                          "elapsed_seconds": manifest["elapsed_seconds"]})
                else:
                    actual_history = str(rows[-1].get("message") or "").strip()
                    if not re.match(r"^[A-Za-z0-9_.-]+$", actual_history) or actual_history in (".", ".."):
                        raise PersistentMaestroWorkerProtocolError("unsafe batch history identity")
                    manifest["history_name"] = actual_history
                    manifest["rdb_path"] = workspace_rdb_path(self.workspace, actual_history)
                    native_paths = qualify_native_history_paths(self.workspace, manifest["batch_root"], actual_history, started_epoch=started, required=self._use_guardian)
                    if native_paths:
                        manifest["native_storage_paths"] = native_paths
                        manifest["rdb_path"] = native_paths.get("rdb_path") or manifest["rdb_path"]
                    for candidate in manifest["candidates"]:
                        candidate.update({"history_name": actual_history, "rdb_path": manifest["rdb_path"],
                                          "native_storage_paths": manifest.get("native_storage_paths"),
                                          "status": rows, "states": manifest["states"],
                                          "started_epoch": started, "finished_epoch": manifest["finished_epoch"],
                                          "elapsed_seconds": manifest["elapsed_seconds"],
                                          "candidate_supervision": dict(supervision)})
                    manifest["native_completion_proven"] = True
                    self.pending_cleanup = dict(identity)
                    self.state = "awaiting_cleanup"
                self.active_identity = None
                self._write_state()
                return persist_batch_artifacts(manifest)
            except MaestroBatchUnsupported:
                raise
            except Exception as error:
                self.tainted = True
                self.state = "tainted"
                # Ask the live owned Maestro to stop all of this batch's global
                # and per-test submissions before the local guardian exits.
                self._terminate_after_failure("batch supervision failure", error,
                    request_native_stop=True)
                raise

    def request_batch_abort(self, reason="external batch abort", timeout_seconds=None):
        """Best effort native cluster stop; acknowledgement is recorded distinctly.

        Local process death never counts as remote scheduler cancellation proof.
        This method deliberately avoids the evaluation operation lock.
        """
        with self._batch_submit_lock:
            self._batch_abort_before_submit = True
            batch = (getattr(self, "_active_batch", None)
                     or getattr(self, "_startup_remote_scope", None))
            if not batch:
                return {"requested": False, "remote_stop_proven": True,
                        "no_remote_jobs_started": True, "reason": reason}
            if (batch.get("native_stop_proven") or
                    (not getattr(self, 'remote_session_possible', False) and
                     (self.state == "awaiting_cleanup" or batch.get("native_completion_proven")))):
                return {"requested": False, "remote_stop_proven": True,
                        "batch_completed": bool(batch.get("native_completion_proven")),
                        "native_stop_proven": bool(batch.get("native_stop_proven")), "reason": reason}
            if (not batch.get("simulation_submitted")
                    and not getattr(self, 'remote_session_possible', False)):
                batch["abort_requested"] = True
                return {"requested": False, "remote_stop_proven": True,
                        "no_remote_jobs_started": True, "reason": reason}
            if not self.process or self.process.poll() is not None:
                return {"requested": False, "remote_stop_proven": False, "reason": reason}
            batch["abort_requested"] = True
        identity = batch["protocol_identity"]
        marker = batch["abort_marker_path"]
        script = batch["abort_script_path"]
        status = ('crescWriteWorkerStatus("%s" "%s" %d "%s" "%s"' % (
            _skill_quote(marker), _skill_quote(identity["epoch"]), identity["sequence"],
            _skill_quote(identity["nonce"]), _skill_quote(identity["candidate_id"])))
        lines = [
            'unless(equal(crescWorkerEpoch "%s") error("Crescendo batch stop epoch mismatch\\n"))' % _skill_quote(self.epoch),
            status + ' "STOP_BEGIN" "")',
            "batchStopResult = errset(progn(",
            '  unless(isCallable(\'maeStopSimulation) && isCallable(\'maeWaitUntilDone) error("Crescendo native stop APIs unavailable; remote cancellation unproven\\n"))',
            '  unless(boundp(\'session) && session error("Crescendo owned session is unavailable; remote cancellation unproven\\n"))',
            '  nativeStopAccepted = errset(maeStopSimulation(?session session) t)',
            '  unless(nativeStopAccepted && car(nativeStopAccepted) || %s error("Crescendo batch native stop rejected\\n"))' % ('t' if batch.get('startup_session') or batch.get('native_completion_proven') else 'nil'),
            "  " + status + ' "STOP_SENT" "owned Maestro accepted stop request")',
            "  maeWaitUntilDone('All ?session session)",
            '  unless(isCallable(\'maeCloseSession) error("Crescendo owned session close API unavailable; remote worker pool stop unproven\\n"))',
            '  ownedSessionClosed = errset(maeCloseSession(?session session ?forceClose t) t)',
            '  unless(ownedSessionClosed error("Crescendo owned session close failed; remote worker pool stop unproven\\n"))',
            "  " + status + ' "STOP_DONE" "owned Maestro reports simulation stopped and session closed")',
            "  t",
            ") t)",
            "unless(batchStopResult " + status + ' "STOP_ERROR" "native stop failed"))',
        ]
        atomic_write_text(script, "\n".join(lines) + "\n")
        result = {"requested": True, "remote_stop_proven": False, "reason": reason,
                  "protocol_identity": dict(identity), "marker_path": marker}
        timeout = float(timeout_seconds if timeout_seconds is not None else
                        self.config.get("runtime", {}).get("maestro_batch_stop_timeout_seconds", 10))
        try:
            self._send_load(script)
            rows = self._wait_protocol(marker, identity, ("STOP_DONE", "STOP_ERROR"), timeout,
                                       "owned batch stop")
            result["status"] = rows
            result["remote_stop_proven"] = rows[-1]["state"] == "STOP_DONE"
            batch["native_stop_proven"] = result["remote_stop_proven"]
            if result["remote_stop_proven"]:
                self.remote_session_possible = False
                scope = getattr(self, '_startup_remote_scope', None)
                if scope:
                    scope['native_stop_proven'] = True
                    scope['native_session_closed'] = True
                    atomic_write_json(scope['manifest_path'], scope)
            self._write_state()
            if batch.get("native_serial"):
                atomic_write_json(batch["manifest_path"], batch)
            else:
                from .maestro_batch import persist_batch_artifacts
                persist_batch_artifacts(batch)
        except Exception as error:
            result["error"] = "%s: %s" % (error.__class__.__name__, error)
        atomic_write_json(os.path.join(batch["batch_root"], "batch_stop_evidence.json"), result)
        return result

    def request_cleanup(self, candidate, timeout_seconds=None):
        with self._operation_lock:
            identity = dict(candidate.get("protocol_identity") or {})
            if self.tainted:
                raise PersistentMaestroWorkerTainted("tainted worker cannot clean results")
            if identity != self.pending_cleanup:
                raise PersistentMaestroWorkerProtocolError("cleanup does not match the pending candidate fence")
            if not self.process or self.process.poll() is not None:
                raise PersistentMaestroWorkerProcessError("worker died before result cleanup")
            marker_path = os.path.join(candidate["candidate_root"], "persistent_worker_cleanup.tsv")
            script_path = os.path.join(candidate["candidate_root"], "persistent_worker_cleanup.il")
            atomic_write_text(marker_path, "")
            status = (
                'crescWriteWorkerStatus("%s" "%s" %d "%s" "%s"' % (
                    _skill_quote(marker_path), _skill_quote(identity["epoch"]), identity["sequence"],
                    _skill_quote(identity["nonce"]), _skill_quote(identity["candidate_id"]),
                )
            )
            lines = [
                status + ' "CLEANUP_BEGIN" "")',
                "cleanupResult = errset(progn(",
                '  unless(equal(crescLastEpoch "%s") error("Crescendo cleanup epoch mismatch\\n"))' % _skill_quote(identity["epoch"]),
                "  unless(crescLastSequence == %d error(\"Crescendo cleanup sequence mismatch\\n\"))" % identity["sequence"],
                '  unless(equal(crescLastNonce "%s") error("Crescendo cleanup nonce mismatch\\n"))' % _skill_quote(identity["nonce"]),
                '  unless(equal(crescLastCandidate "%s") error("Crescendo cleanup candidate mismatch\\n"))' % _skill_quote(identity["candidate_id"]),
                "  unless(isCallable('axlRemoveSimulationResults) error(\"Crescendo result cleanup API is unavailable\\n\"))",
                "  historySDB = axlGetHistoryEntry(mainSDB crescLastHistoryName)",
                "  unless(historySDB error(\"Crescendo completed history entry is unavailable\\n\"))",
                "  removalResult = axlRemoveSimulationResults(historySDB)",
                "  unless(removalResult error(\"Crescendo simulator result cleanup was rejected\\n\"))",
                "  maeWaitUntilDone('All ?session session)",
                "  crescLastRunId = nil",
                "  crescLastHistoryName = nil",
                "  crescLastEpoch = nil",
                "  crescLastSequence = nil",
                "  crescLastNonce = nil",
                "  crescLastCandidate = nil",
                "  t",
                ") t)",
                "if(cleanupResult then",
                "  progn(",
                "    runData = nil",
                "    runId = nil",
                "    actualHistoryName = nil",
                "    historySDB = nil",
                "    removalResult = nil",
                "    candidateResult = nil",
                "    cv = nil",
                "    inst = nil",
                "    instances = nil",
                "    cdfgData = nil",
                "    cdfResult = nil",
                "    baseCdf = nil",
                "    cdfParam = nil",
                "    crescCallbackEffectiveValue = nil",
                "    crescCallbackDefaultValue = nil",
                "    crescCallbackDefaultPresent = nil",
                "    crescCallbackFreshCdf = nil",
                "    crescCallbackFreshParam = nil",
                "    crescCallbackOaProp = nil",
                "    existingProp = nil",
                "    schematicProp = nil",
                "    schematicValue = nil",
                "    snapshotInstances = nil",
                "    snapshotProp = nil",
                "    snapshotCdfParam = nil",
                "    snapshotValue = nil",
                "    snapshotBase = nil",
                "    snapshotSim = nil",
                "    snapshotSpec = nil",
                "    snapshotNames = nil",
                "    snapshotName = nil",
                "    snapshotSymbol = nil",
                "    effectiveParam = nil",
                "    effectiveFields = nil",
                "    callbackExpr = nil",
                "    callbackResult = nil",
                "    callbackStatus = nil",
                "    updateResult = nil",
                "    crescVerify = nil",
                "    cleanupResult = nil",
                "    gc()",
                "    " + status + ' "CLEANUP_DONE" "")',
                "  )",
                "else",
                "  crescWorkerTainted = t",
                "  " + status + ' "CLEANUP_ERROR" "result cleanup failed")',
                ")",
            ]
            if candidate.get("shared_history") and candidate.get("batch_tests"):
                from .maestro_batch import snapshot_cleanup_lines
                insertion = lines.index("  crescLastRunId = nil")
                lines[insertion:insertion] = ["  " + line for line in snapshot_cleanup_lines(candidate)]
            opening = lines.index("cleanupResult = errset(progn(")
            closing = lines.index(") t)", opening + 1)
            cleanup_lines, generation = bounded_skill_body_lines(
                lines[opening + 1:closing], "cleanupResult")
            lines[opening:closing + 1] = cleanup_lines
            atomic_write_text(script_path, "\n".join(lines) + "\n")
            timeout_seconds = float(
                timeout_seconds if timeout_seconds is not None
                else self.config["runtime"].get("persistent_cleanup_timeout_seconds", 20),
            )
            if timeout_seconds <= 0:
                raise ValueError("persistent cleanup timeout must be positive")
            cleanup_deadline = self._clock() + timeout_seconds
            self.state = "cleaning"
            self.active_identity = identity
            self._write_state()
            try:
                self._send_load(script_path)
                rows = self._wait_protocol(
                    marker_path, identity, ("CLEANUP_DONE", "CLEANUP_ERROR"), timeout_seconds,
                    "candidate %s cleanup" % identity["candidate_id"],
                    wall_deadline=cleanup_deadline,
                    wall_timeout_seconds=timeout_seconds,
                )
                terminal = _validate_worker_protocol(rows, self.CLEANUP_STATES, "CLEANUP_ERROR")
                if terminal == "CLEANUP_ERROR":
                    self.tainted = True
                    self.state = "tainted"
                else:
                    self.pending_cleanup = None
                    self._active_batch = None
                    self.state = "ready"
                self.active_identity = None
                result = {
                    "schema_version": WORKER_SCHEMA_VERSION,
                    "protocol_identity": identity, "marker_path": marker_path,
                    "script_path": script_path, "status": rows,
                    "cleanup_generation": generation,
                    "states": [row["state"] for row in rows],
                    "ok": terminal == "CLEANUP_DONE", "tainted": bool(self.tainted),
                }
                atomic_write_json(os.path.join(candidate["candidate_root"], "persistent_worker_cleanup.json"), result)
                self._write_state()
                return result
            except Exception as error:
                self.tainted = True
                self.state = "tainted"
                self._terminate_after_failure("result cleanup supervision failure", error,
                    request_native_stop=getattr(self, 'remote_session_possible', False))
                raise

    def _wait_exit(self, timeout_seconds):
        if not self.process:
            return None
        try:
            return self.process.wait(timeout=max(0.0, float(timeout_seconds)))
        except subprocess.TimeoutExpired:
            return None

    def _close_streams(self):
        for handle_name in ("_stdout_handle", "_stderr_handle"):
            handle = getattr(self, handle_name)
            if handle is not None:
                try:
                    handle.close()
                except Exception:
                    pass
                setattr(self, handle_name, None)

    def _close_stdin(self):
        process = self.process
        stream = getattr(process, "stdin", None) if process is not None else None
        if stream is not None:
            try:
                stream.close()
            except Exception:
                pass

    def _signal_supervisor(self, sig):
        process = self.process
        if not process or process.poll() is not None:
            return
        if self._pgid == process.pid:
            try:
                observed_pgid = self._getpgid(process.pid)
            except OSError:
                return
            if observed_pgid == self._pgid:
                self._killpg(self._pgid, sig)
                return
        if sig == signal.SIGTERM:
            process.terminate()
        else:
            process.kill()

    def request_abort(self, reason="external watchdog abort"):
        process = self.process
        if not process or process.poll() is not None:
            return {"requested": False, "reason": "worker supervisor is not alive"}
        self.tainted = True
        self.state = "abort_requested"
        try:
            self._write_state(reason)
        except Exception:
            pass
        try:
            self._close_stdin()
        except Exception:
            pass
        try:
            self._signal_supervisor(signal.SIGTERM)
        except OSError:
            pass
        return {
            "requested": True,
            "reason": reason,
            "pid": getattr(process, "pid", None),
            "pgid": self._pgid,
        }

    def _require_guardian_group_cleanup(self):
        if not self._use_guardian or self.process is None:
            return None

        def load_validated_state():
            try:
                with open(self.guardian_state_path, "r") as stream:
                    guardian_state = json.load(stream)
            except (IOError, OSError, ValueError) as error:
                raise PersistentMaestroWorkerProcessError(
                    "guardian final state is unavailable: %s: %s" % (
                        error.__class__.__name__, error,
                    )
                )
            if not isinstance(guardian_state, dict):
                raise PersistentMaestroWorkerProcessError(
                    "guardian final state must be a JSON object"
                )
            expected = {
                "token": self.guardian_token,
                "guardian_pid": self.process.pid,
                "cwd": os.path.realpath(os.path.abspath(self.workspace["root"])),
            }
            observed = {
                "token": guardian_state.get("token"),
                "guardian_pid": guardian_state.get("guardian_pid"),
                "cwd": os.path.realpath(os.path.abspath(guardian_state.get("cwd") or "")),
            }
            if observed != expected:
                raise PersistentMaestroWorkerProcessError(
                    "guardian final identity mismatch: expected=%s observed=%s" % (
                        expected, observed,
                    )
                )
            return guardian_state

        state = load_validated_state()
        cleanup_complete = bool(
            state.get("status") == "exited"
            and state.get("group_cleanup_complete") is True
        )
        orphan_recovery = state.get("orphan_recovery")
        if cleanup_complete and orphan_recovery is None:
            return state
        if self.process.poll() is None:
            raise PersistentMaestroWorkerProcessError(
                "guardian did not prove Ocean group cleanup: status=%s remaining=%s" % (
                    state.get("status"), state.get("remaining_group_pids"),
                )
            )
        recovery = quiesce_legacy_guardian_attempt(
            self.control_root,
            self.workspace["root"],
            timeout_seconds=max(
                1.0,
                self._guardian_terminate_grace_seconds
                + self._guardian_termination_margin_seconds,
            ),
            terminate_owned=not cleanup_complete,
            expected_token_identity=self.guardian_token_identity,
        )
        proof = recovery.get("termination_proof") or {}
        if recovery.get("state") != "dead" or proof.get("ok") is not True:
            raise PersistentMaestroWorkerProcessError(
                "guardian cleanup recovery was not proven: state=%s kind=%s reason=%s "
                "processes=%s" % (
                    recovery.get("state"), proof.get("kind"), recovery.get("reason"),
                    json.dumps(
                        recovery.get("processes") or [], sort_keys=True,
                        separators=(",", ":"),
                    ),
                )
            )
        state = load_validated_state()
        if state.get("status") != "exited" or state.get("group_cleanup_complete") is not True:
            raise PersistentMaestroWorkerProcessError(
                "guardian recovery proof was not persisted: status=%s remaining=%s" % (
                    state.get("status"), state.get("remaining_group_pids"),
                )
            )
        if state.get("orphan_recovery") is not None and proof.get("kind") != "orphaned_ocean_group_recovered":
            raise PersistentMaestroWorkerProcessError(
                "persisted orphan recovery proof was not validated: kind=%s" % proof.get("kind")
            )
        return state

    def terminate(self, reason="terminated", grace_seconds=5.0):
        with self._operation_lock:
            process = self.process
            if self.closed:
                self._close_stdin()
                self._close_streams()
                return getattr(process, "returncode", None)
            if process and process.poll() is None:
                if self._use_guardian:
                    self._close_stdin()
                    outer_grace = max(
                        float(grace_seconds),
                        self._guardian_terminate_grace_seconds
                        + self._guardian_termination_margin_seconds,
                    )
                else:
                    try:
                        self._signal_supervisor(signal.SIGTERM)
                    except OSError:
                        pass
                    outer_grace = max(0.0, float(grace_seconds))
                if self._wait_exit(outer_grace) is None and process.poll() is None:
                    try:
                        self._signal_supervisor(signal.SIGTERM)
                    except OSError:
                        pass
                    if self._wait_exit(max(1.0, float(grace_seconds))) is None and process.poll() is None:
                        try:
                            self._signal_supervisor(signal.SIGKILL)
                        except OSError:
                            pass
                        self._wait_exit(max(1.0, float(grace_seconds)))
            if process and process.poll() is None:
                self.tainted = True
                self.closed = False
                self.state = "termination_failed"
                self._close_stdin()
                self._close_streams()
                self._write_state(reason)
                raise PersistentMaestroWorkerProcessError(
                    "worker supervisor remained alive after TERM/KILL: pid=%s pgid=%s" % (
                        getattr(process, "pid", None), self._pgid,
                    )
                )
            if process and self._use_guardian:
                try:
                    self._require_guardian_group_cleanup()
                except PersistentMaestroWorkerProcessError:
                    self.tainted = True
                    self.closed = False
                    self.state = "termination_failed"
                    self._close_stdin()
                    self._close_streams()
                    self._write_state(reason)
                    raise
            if getattr(self, 'remote_session_possible', False):
                self.state = 'local_termination_proven_remote_unproven'
                self.closed = False
                self._close_stdin()
                self._close_streams()
                self._write_state('Owned remote worker pool stop unproven; private views retained')
                raise PersistentMaestroWorkerProcessError(
                    'local termination proven but owned remote worker pool stop is unproven; private views retained')
            from .same_library_views import cleanup_same_library_views
            view_cleanup = cleanup_same_library_views(self.workspace, process_cleanup_proven=True)
            if not view_cleanup["cleanup_proven"]:
                self.state = "termination_failed"
                raise PersistentMaestroWorkerProcessError("owned schematic views remain after worker termination")
            self.closed = True
            self.state = "terminated"
            self._close_stdin()
            self._close_streams()
            self._write_state(reason)
            return getattr(process, "returncode", None)

    def close(self, timeout_seconds=10.0):
        with self._operation_lock:
            if self.closed:
                return {"state": self.state, "returncode": getattr(self.process, "returncode", None)}
            if not self.process or self.process.poll() is not None:
                if self.process is not None and self._use_guardian:
                    try:
                        self._require_guardian_group_cleanup()
                    except PersistentMaestroWorkerProcessError:
                        self.tainted = True
                        self.closed = False
                        self.state = "termination_failed"
                        self._close_stdin()
                        self._close_streams()
                        self._write_state("guardian cleanup proof failed after process exit")
                        raise
                if getattr(self, 'remote_session_possible', False):
                    self._write_state('Owned remote worker pool stop unproven; private views retained')
                    raise PersistentMaestroWorkerProcessError(
                        'owned remote worker pool stop is unproven; private views retained')
                self.closed = True
                self.state = "closed"
                from .same_library_views import cleanup_same_library_views
                if not cleanup_same_library_views(self.workspace, process_cleanup_proven=True)["cleanup_proven"]:
                    self.closed = False
                    raise PersistentMaestroWorkerProcessError("owned schematic views remain after process exit")
                self._close_stdin()
                self._close_streams()
                self._write_state("process already exited")
                return {"state": self.state, "returncode": getattr(self.process, "returncode", None)}
            identity = {
                "epoch": self.epoch, "sequence": self.sequence + 1, "nonce": uuid.uuid4().hex,
                "candidate_id": "__shutdown__",
            }
            atomic_write_text(self.shutdown_marker, "")
            script_path = os.path.join(self.control_root, "shutdown.il")
            status = (
                'crescWriteWorkerStatus("%s" "%s" %d "%s" "%s"' % (
                    _skill_quote(self.shutdown_marker), _skill_quote(identity["epoch"]), identity["sequence"],
                    _skill_quote(identity["nonce"]), _skill_quote(identity["candidate_id"]),
                )
            )
            lines = [
                status + ' "SHUTDOWN_BEGIN" "")',
                "shutdownResult = errset(maeCloseSession(?session session ?forceClose t) t)",
                "if(shutdownResult then",
                "  " + status + ' "SHUTDOWN_DONE" "")',
                "else",
                "  " + status + ' "SHUTDOWN_ERROR" "session close failed")',
                ")",
                "exit()",
            ]
            atomic_write_text(script_path, "\n".join(lines) + "\n")
            try:
                self._send_load(script_path)
                rows = self._wait_protocol(
                    self.shutdown_marker, identity, ("SHUTDOWN_DONE", "SHUTDOWN_ERROR"),
                    timeout_seconds, "shutdown",
                )
                terminal = _validate_worker_protocol(rows, self.SHUTDOWN_STATES, "SHUTDOWN_ERROR")
                if terminal != "SHUTDOWN_DONE":
                    raise PersistentMaestroWorkerProtocolError("Maestro session close failed")
                self.remote_session_possible = False
                scope = getattr(self, '_startup_remote_scope', None)
                if scope:
                    scope['native_stop_proven'] = True
                    scope['native_session_closed'] = True
                    atomic_write_json(scope['manifest_path'], scope)
                    self._write_state()
                if self._wait_exit(timeout_seconds) is None and self.process.poll() is None:
                    raise PersistentMaestroWorkerTimeout("Ocean did not exit after graceful shutdown")
                self._require_guardian_group_cleanup()
                if self.process.returncode not in (None, 0):
                    raise PersistentMaestroWorkerProcessError(
                        "guardian exited nonzero after graceful shutdown: %s" % self.process.returncode
                    )
                from .same_library_views import cleanup_same_library_views
                if not cleanup_same_library_views(self.workspace, process_cleanup_proven=True)["cleanup_proven"]:
                    raise PersistentMaestroWorkerProcessError("owned schematic views remain after graceful shutdown")
                self.closed = True
                self.state = "closed"
                self._close_stdin()
                self._close_streams()
                self._write_state()
                return {"state": self.state, "returncode": self.process.returncode, "status": rows}
            except Exception as error:
                self.tainted = True
                self._terminate_after_failure("graceful close failed", error,
                    request_native_stop=getattr(self, 'remote_session_possible', False))
                raise

    def is_alive(self):
        return bool(self.process and self.process.poll() is None and not self.closed)
