"""Isolated candidate snapshots scheduled by one owned Maestro session.

A batch is a set of tests, not a set of concurrently patched OA databases.
Only changed occurrence branches and their top/config are copied. Candidate
values keep the serial adapter's CDF application and netlist verification.
"""
from __future__ import print_function

import ast
import copy
import hashlib
import json
import os
import re
import xml.etree.ElementTree as ET

from crescendo_engine.runtime import bind_text
_contract_text = bind_text()

from .config_binding import config_binding_lines, private_config_plan
from .utils import atomic_write_json, ensure_dir, is_under
from .workspace import (ParameterApplicationFailure, _skill_quote, private_library_name,
                        private_design_identity, private_library_is_owned)

SCHEMA_VERSION = _contract_text("crescendo.maestro_candidate_batch.v1")
MODE = "isolated_design_tests"


def shared_skill_expressions(lines):
    """Factor exact repeated, complete global expressions into bounded helpers.

    No record/occurrence is omitted. Control transfers and definitions remain
    inline. Helpers have no local bindings/arguments and return the original
    expression value, preserving assignment and error/callback semantics.
    """
    forms, pending, depth, quoted, escaped, comment = [], [], 0, False, False, False
    for raw in lines:
        pending.append(raw)
        index = 0
        while index < len(raw):
            char, pair = raw[index], raw[index:index + 2]
            if comment:
                if pair == "*/":
                    comment = False
                    index += 2
                    continue
            elif quoted:
                if escaped:
                    escaped = False
                elif char == "\\":
                    escaped = True
                elif char == '"':
                    quoted = False
            elif pair == "/*":
                comment = True
                index += 2
                continue
            elif char == ";" or pair == "//":
                break
            elif char == '"':
                quoted = True
            elif char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
                if depth < 0:
                    raise ValueError("shared batch expression is unbalanced")
            index += 1
        if depth == 0 and not quoted and not comment:
            forms.append(tuple(pending))
            pending = []
    if pending or depth or quoted or comment:
        raise ValueError("shared batch expression is incomplete")
    counts = {}
    for form in forms:
        counts[form] = counts.get(form, 0) + 1
    shared = {}
    definitions = []
    for form, count in sorted(counts.items()):
        text = "\n".join(form)
        size = len(text.encode("utf-8"))
        if (count < 2 or not 80 <= size <= 16384 or (count - 1) * size < 512
                or re.search(r"\b(?:procedure|defun|lambda|return|go|break|continue|prog)\s*\(", text)):
            continue
        name = _contract_text("crescBatchShared_") + hashlib.sha256(text.encode("utf-8")).hexdigest()[:20]
        shared[form] = name
        definitions.append("procedure(%s()" % name)
        definitions.extend(form)
        definitions.append(")")
    body = []
    for form in forms:
        if form in shared:
            body.append(shared[form] + "()")
        else:
            body.extend(form)
    return definitions, body, {"shared_expression_count": len(shared),
                               "original_body_bytes": len(("\n".join(lines) + "\n").encode("utf-8")),
                               "shared_body_bytes": len(("\n".join(definitions + body) + "\n").encode("utf-8")),
                               "complete_expression_count": len(forms)}


class MaestroBatchUnsupported(RuntimeError):
    """A capability boundary, before submission; serial evaluation is safe."""


def _key(row):
    return tuple(row.get(name) for name in ("library", "cell", "view"))


def _design_key(row):
    return tuple(row.get("design_" + name) for name in ("library", "cell", "view"))


def _name(epoch, sequence, slot, identity):
    # A private ownership prefix plus a collision-resistant complete identity.
    value = "\x1f".join(str(item) for item in (epoch, sequence, slot) + tuple(identity))
    return _contract_text("cr_b_%s_%s") % (str(epoch)[:8], hashlib.sha256(value.encode("utf-8")).hexdigest()[:17])


def qualify_snapshot_batch(workspace, parameters):
    reasons = []
    setup = workspace.get("setup") or {}
    tests = [row for row in setup.get("tests") or [] if row.get("enabled", True)]
    if not tests:
        reasons.append("no_enabled_tests")
    if any(row.get("backend", "schematic") != "schematic" for row in parameters):
        reasons.append("selected_backend_requires_serial_adapter")
    if any(not all(_key(row)) for row in tests):
        reasons.append("test_design_identity_incomplete")
    if any(row.get("simulator") not in (None, "", "spectre", "alps") for row in tests):
        reasons.append("saved_simulator_requires_completion_adapter")
    if setup.get("mode") and setup["mode"] not in (
            "Single Run, Sweeps and Corners", "Single Run, Sweeps, and Corners", "SRSC"):
        reasons.append("run_mode_requires_serial_adapter:%s" % setup["mode"])
    # Cross-test references must never silently keep referring to the source test.
    # A separate remapping adapter can extend this conservative boundary later.
    outputs = list(setup.get("outputs") or [])
    for test in tests:
        outputs.extend(test.get("outputs") or [])
    for row in outputs:
        expression = str(row.get("expression") or row.get("expr") or "")
        if re.search(r"\b(calcVal|getData|valueFromTest)\s*\(", expression):
            if "calcVal" in expression or "?test" in expression or "valueFromTest" in expression:
                reasons.append("cross_test_output_requires_remapping")
                break
    path = (setup.get("source") or {}).get("setup_db") or (setup.get("source") or {}).get("maestro_sdb")
    if path and os.path.isfile(path):
        try:
            root = ET.parse(path).getroot()
            active = next((child for child in root if str(child.tag).rsplit("}", 1)[-1].lower() == "active"), root)
            for child in active:
                tag = str(child.tag).rsplit("}", 1)[-1].lower()
                if tag in ("parameters", "params") and list(child):
                    # Existing device sweeps contain paths to the source design.
                    reasons.append("existing_device_sweeps_require_snapshot_remapping")
                if tag in ("prerunscript", "prerunscripts") and list(child):
                    reasons.append("custom_pre_run_script_requires_serial_adapter")
        except (IOError, OSError, ET.ParseError):
            reasons.append("private_setup_cannot_be_qualified")
    return {"supported": not reasons, "mode": MODE, "reasons": sorted(set(reasons)),
            "candidate_dimension": "tests", "changes_population": False,
            "max_jobs_controls": "maestro_icrp_jobs"}


def _copy_view_lines(source, target, view_type="schematic"):
    return [
        _contract_text('unless(ddGetObj("%s" "%s" "%s") == nil error("Crescendo batch destination already exists\\n"))') % tuple(_skill_quote(v) for v in target),
        'srcCv = dbOpenCellViewByType("%s" "%s" "%s" "%s" "r")' % tuple(_skill_quote(v) for v in source + (view_type,)),
        _contract_text('unless(srcCv error("Crescendo batch snapshot source could not open\\n"))'),
        'dstCv = dbCopyCellView(srcCv "%s" "%s" "%s")' % tuple(_skill_quote(v) for v in target),
        _contract_text('unless(dstCv error("Crescendo batch snapshot could not copy\\n"))'),
        'dbClose(srcCv)', 'dbClose(dstCv)',
    ]


def prepare_snapshot_batch(worker, candidates, batch_root, identity):
    """Render one genuinely parallel Maestro run without modifying source cells."""
    if worker.workspace.get("copy_policy") == "same_library_views":
        return _prepare_same_library_snapshot_batch(worker, candidates, batch_root, identity)
    from .persistent_maestro import (_candidate_rows, _candidate_plans, _clone_lines,
                                     _snapshot_lines, _patch_lines)
    candidates = list(candidates or [])
    if not candidates:
        raise ValueError("batch requires candidates")
    qualification = qualify_snapshot_batch(worker.workspace, worker.parameters)
    if not qualification["supported"]:
        raise MaestroBatchUnsupported(", ".join(qualification["reasons"]))
    batch_root = ensure_dir(os.path.abspath(batch_root))
    source_workspace = worker.workspace
    setup = source_workspace.get("setup") or {}
    original_tests = [copy.deepcopy(row) for row in setup.get("tests") or [] if row.get("enabled", True)]
    config_designs = dict((_key(row), row) for row in source_workspace.get("config_designs") or [])
    selected_designs = set(_design_key(row) for row in worker.parameters)
    missing = selected_designs - set(_key(row) for row in original_tests)
    if missing:
        raise MaestroBatchUnsupported("selected design has no enabled test: %r" % sorted(missing))
    manifest = {"schema_version": SCHEMA_VERSION, "mode": MODE,
                "batch_id": identity["candidate_id"], "batch_root": batch_root,
                "protocol_identity": dict(identity), "qualification": qualification,
                "shared_history": True, "candidates": [], "owned_cells": [],
                "original_enabled_tests": [row["name"] for row in original_tests],
                "batch_tests": [], "snapshot_records": os.path.join(batch_root, "snapshot.records"),
                "marker_path": os.path.join(batch_root, "persistent_worker_batch.tsv"),
                "script_path": os.path.join(batch_root, "persistent_worker_batch.il"),
                "manifest_path": os.path.join(batch_root, _contract_text("persistent_worker_batch.json"))}
    lines = []
    seen = set()
    for slot, requested in enumerate(candidates):
        candidate_id = str(requested["candidate_id"])
        if candidate_id in seen:
            raise ValueError("duplicate candidate_id: %s" % candidate_id)
        seen.add(candidate_id)
        root = os.path.abspath(requested.get("candidate_root") or os.path.join(batch_root, "candidates", "c%d" % slot))
        batch_parent = os.path.dirname(batch_root)
        product_candidates = os.path.join(os.path.dirname(batch_parent), "candidates")
        product_root = (os.path.basename(batch_parent) == "batches"
                        and os.path.dirname(root) == product_candidates
                        and os.path.basename(root) == candidate_id
                        and candidate_id not in (".", "..")
                        and re.match(r"^[A-Za-z0-9_.-]+$", candidate_id)
                        and os.path.realpath(root) == root
                        and os.path.realpath(batch_root) == batch_root)
        if not product_root and not is_under(root, batch_parent) and not is_under(root, worker.worker_root):
            raise ValueError("batch candidate root is outside owned run")
        root = ensure_dir(root)
        design_map = {}
        top_map = {}
        roots = []
        candidate_configs = []
        # Copy each changed top once, even if multiple tests reference it.
        for design in sorted(selected_designs):
            config = config_designs.get(design)
            top = _key(config["resolved_top"]) if config else design
            if top not in top_map:
                target = (private_library_name(source_workspace, top[0]), _name(worker.epoch, identity["sequence"], slot, top), top[2])
                if top[0] not in source_workspace["copied_libraries"]:
                    raise MaestroBatchUnsupported("selected top is not in working workspace")
                top_map[top] = target
                roots.append({"source": top, "target": target})
                lines.extend(_copy_view_lines(private_design_identity(source_workspace, top), target))
                manifest["owned_cells"].append({"library": target[0], "cell": target[1]})
            if config:
                cfg_target = (private_library_name(source_workspace, design[0]), _name(worker.epoch, identity["sequence"], slot, design), design[2])
                design_map[design] = cfg_target
                updated = copy.deepcopy(config)
                updated.update(dict(zip(("library", "cell", "view"), cfg_target)))
                updated["resolved_top"] = dict(config["resolved_top"])
                updated["resolved_top"].update(dict(zip(("library", "cell", "view"), top_map[top])))
                candidate_configs.append(updated)
                lines.extend([
                    _contract_text('unless(ddGetObj("%s" "%s") == nil error("Crescendo batch config cell exists\\n"))') % tuple(_skill_quote(v) for v in cfg_target[:2]),
                    _contract_text('crescSourceConfig = hdbOpen(%s "r")') % " ".join('"%s"' % _skill_quote(v) for v in private_design_identity(source_workspace, design)),
                    _contract_text('unless(crescSourceConfig error("Crescendo batch source config unavailable\\n"))'),
                    _contract_text('unless(hdbSaveACopy(crescSourceConfig %s) error("Crescendo batch config copy failed\\n"))') % " ".join('"%s"' % _skill_quote(v) for v in cfg_target),
                    _contract_text('hdbClose(crescSourceConfig)'),
                ])
                manifest["owned_cells"].append({"library": cfg_target[0], "cell": cfg_target[1]})
            else:
                design_map[design] = top_map[top]
        batch_clones = []
        current_clone_map = {}
        for original in worker.clone_plan:
            original_design = _design_key(original)
            row = copy.deepcopy(original)
            target_design = design_map[original_design]
            row.update(dict(zip(("design_library", "design_cell", "design_view"), target_design)))
            row["source_library"] = original["clone_library"]
            row["source_cell"] = original["clone_cell"]
            row["clone_cell"] = _name(worker.epoch, identity["sequence"], slot,
                                      original_design + (original["occurrence_path"],))
            row["clone_preexisting"] = False
            if row.get("config_bound"):
                # The private occurrence is already a complete schematic+symbol.
                row["symbol_source_library"] = original["clone_library"]
                row["symbol_source_cell"] = original["clone_cell"]
            if row["parent_occurrence_path"] == "/":
                top = _key(config_designs[original_design]["resolved_top"]) if original_design in config_designs else original_design
                row["parent_library"], row["parent_cell"] = top_map[top][:2]
            else:
                parent = current_clone_map[original_design + (row["parent_occurrence_path"],)]
                row["parent_library"], row["parent_cell"] = parent["clone_library"], parent["clone_cell"]
            current_clone_map[original_design + (row["occurrence_path"],)] = row
            batch_clones.append(row)
            manifest["owned_cells"].append({"library": row["clone_library"], "cell": row["clone_cell"]})
        candidate_workspace = dict((key, copy.deepcopy(source_workspace[key]))
                                   for key in ("root", "simulation_root", "setup_dir", "primary_library",
                                               "setup_cell", "setup_view", "setup_backend", "copied_libraries",
                                               "private_library_names", "copy_policy")
                                   if key in source_workspace)
        candidate_workspace["config_designs"] = candidate_configs
        clone_records = os.path.join(root, "batch_clone.records")
        lines.extend(_clone_lines(batch_clones, clone_records, candidate_workspace))
        # _candidate_rows expects source design keys; retain them in the lookup.
        rows = _candidate_rows(source_workspace, worker.parameters, requested["values"], current_clone_map)
        for row in rows:
            design = (row["design_library"], row["design_cell"], row["design_view"])
            original_top = _key(config_designs[design]["resolved_top"]) if design in config_designs else design
            target_top = top_map[original_top]
            if row["owner_occurrence_path"] == "/":
                row["library"], row["cell"] = target_top[:2]
            row["netlist_design_library"], row["netlist_design_cell"] = target_top[:2]
        plans = _candidate_plans(rows, root, not worker._adapter_path())
        lines.extend(_snapshot_lines(rows, plans["baseline"]["verification_path"], "BASELINE"))
        lines.extend(_patch_lines(rows, plans["parameters"]["verification_path"], worker._adapter_path()))
        lines.extend(_snapshot_lines(rows, plans["verify"]["verification_path"], "POST"))
        source_test_map = {}
        tests = []
        for source in original_tests:
            target = design_map.get(_key(source), _key(source))
            alias = _name(worker.epoch, identity["sequence"], slot, ("test", source["name"]))
            test = copy.deepcopy(source)
            test.update({"name": alias, "library": target[0], "cell": target[1], "view": target[2]})
            tests.append(test)
            source_test_map[alias] = source["name"]
            manifest["batch_tests"].append({"name": alias, "source": source["name"], "design": target,
                                            "simulator": source.get("simulator") or "spectre"})
        candidate_workspace["setup"] = copy.deepcopy(setup)
        candidate_workspace["setup"]["tests"] = tests
        artifact = {"candidate_id": candidate_id, "candidate_root": root,
                    "values": dict(requested["values"]), "patch_plan": plans,
                    "protocol_identity": dict(identity), "worker_epoch": worker.epoch,
                    "worker_index": worker.worker_index, "batch_id": identity["candidate_id"],
                    "mode": MODE, "shared_history": True, "source_test_map": source_test_map,
                    "verification_workspace": candidate_workspace,
                    "clone_plan": batch_clones, "clone_records": clone_records,
                    "config_binding_plan": private_config_plan(candidate_workspace, batch_clones),
                    "config_verification_path": clone_records + ".config", "snapshot_roots": roots}
        manifest["candidates"].append(artifact)
    manifest["snapshot_lines"] = lines
    return manifest


def _prepare_same_library_snapshot_batch(worker, candidates, batch_root, identity):
    from .persistent_maestro import _candidate_rows, _candidate_plans, _clone_lines, _snapshot_lines, _patch_lines
    from .same_library_views import register_view, ownership_marker_lines
    qualification = qualify_snapshot_batch(worker.workspace, worker.parameters)
    if not qualification["supported"]:
        raise MaestroBatchUnsupported(", ".join(qualification["reasons"]))
    candidates = list(candidates or [])
    if not candidates:
        raise ValueError("batch requires candidates")
    batch_root = ensure_dir(os.path.abspath(batch_root))
    source_workspace = worker.workspace
    setup = source_workspace.get("setup") or {}
    original_tests = [copy.deepcopy(row) for row in setup.get("tests") or [] if row.get("enabled", True)]
    config_designs = dict((_key(row), row) for row in source_workspace.get("config_designs") or [])
    selected_designs = set(_design_key(row) for row in worker.parameters)
    if selected_designs - set(_key(row) for row in original_tests):
        raise MaestroBatchUnsupported("selected design has no enabled test")
    manifest = {"schema_version": SCHEMA_VERSION, "mode": MODE,
                "batch_id": identity["candidate_id"], "batch_root": batch_root,
                "protocol_identity": dict(identity), "qualification": qualification,
                "shared_history": True, "candidates": [], "owned_cells": [], "owned_views": [],
                "original_enabled_tests": [row["name"] for row in original_tests], "batch_tests": [],
                "snapshot_records": os.path.join(batch_root, "snapshot.records"),
                "marker_path": os.path.join(batch_root, "persistent_worker_batch.tsv"),
                "script_path": os.path.join(batch_root, "persistent_worker_batch.il"),
                "manifest_path": os.path.join(batch_root, _contract_text("persistent_worker_batch.json"))}
    lines, seen = [], set()
    for slot, requested in enumerate(candidates):
        candidate_id = str(requested["candidate_id"])
        if candidate_id in seen:
            raise ValueError("duplicate candidate_id")
        seen.add(candidate_id)
        root = ensure_dir(os.path.abspath(requested.get("candidate_root") or os.path.join(batch_root, "candidates", "c%d" % slot)))
        if not is_under(root, os.path.dirname(batch_root)) and not is_under(root, worker.worker_root):
            # Product candidate roots are siblings of the batches directory.
            run_root = os.path.dirname(os.path.dirname(batch_root))
            if not (os.path.dirname(root) == os.path.join(run_root, "candidates") and os.path.basename(root) == candidate_id and re.match(r"^[A-Za-z0-9_.-]+$", candidate_id)):
                raise ValueError("batch candidate root is outside owned run")
        design_map, candidate_configs = {}, []
        for design in sorted(selected_designs):
            config = config_designs.get(design)
            if not config:
                raise MaestroBatchUnsupported("selected design lacks working config")
            source_config = private_design_identity(source_workspace, design)
            if source_workspace.get("same_library_objects"):
                owned_config = register_view(source_workspace, source_config,
                    (worker.epoch, identity["sequence"], slot, "config") + design, role="batch_config")
                target = tuple(owned_config["target"])
                manifest["owned_views"].append(owned_config)
            else:
                owned_config = None
                target = (source_config[0], _name(worker.epoch, identity["sequence"], slot, design), source_config[2])
            design_map[design] = target
            updated = copy.deepcopy(config)
            updated.update(dict(zip(("library", "cell", "view"), target)))
            updated["generated_config"] = bool(source_workspace.get("same_library_objects"))
            candidate_configs.append(updated)
            lines.extend([
                _contract_text('unless(ddGetObj(%s) == nil error("Crescendo owned batch config exists\\n"))') % " ".join('"%s"' % _skill_quote(v) for v in (target if owned_config else target[:2])),
                _contract_text('crescSourceConfig = hdbOpen(%s "r")') % " ".join('"%s"' % _skill_quote(v) for v in source_config),
                _contract_text('unless(crescSourceConfig error("Crescendo batch source config unavailable\\n"))'),
                _contract_text('unless(hdbSaveACopy(crescSourceConfig %s) error("Crescendo batch config copy failed\\n"))') % " ".join('"%s"' % _skill_quote(v) for v in target),
                _contract_text('hdbClose(crescSourceConfig)'),
            ])
            if owned_config:
                lines.extend(ownership_marker_lines(owned_config))
            else:
                manifest["owned_cells"].append({"library": target[0], "cell": target[1]})
        clones, lookup = [], {}
        for original in worker.clone_plan:
            source_design = _design_key(original)
            row = copy.deepcopy(original)
            row.update(dict(zip(("design_library", "design_cell", "design_view"), design_map[source_design])))
            source = (original["clone_library"], original["clone_cell"], original["target_view"])
            record = register_view(source_workspace, source,
                                   (worker.epoch, identity["sequence"], slot) + source_design + (row["occurrence_path"],), role="batch")
            row.update({"source_library": source[0], "source_cell": source[1], "source_view": source[2],
                        "target_view": record["target"][2], "owned_view": record, "clone_preexisting": False})
            clones.append(row)
            lookup[source_design + (row["occurrence_path"],)] = row
            manifest["owned_views"].append(record)
        candidate_workspace = copy.deepcopy(source_workspace)
        candidate_workspace["config_designs"] = candidate_configs
        candidate_workspace["private_design_identities"] = []
        clone_records = os.path.join(root, "batch_clone.records")
        lines.extend(_clone_lines(clones, clone_records, candidate_workspace))
        rows = _candidate_rows(source_workspace, worker.parameters, requested["values"], lookup)
        plans = _candidate_plans(rows, root, not worker._adapter_path())
        lines.extend(_snapshot_lines(rows, plans["baseline"]["verification_path"], "BASELINE"))
        lines.extend(_patch_lines(rows, plans["parameters"]["verification_path"], worker._adapter_path()))
        lines.extend(_snapshot_lines(rows, plans["verify"]["verification_path"], "POST"))
        tests, source_test_map = [], {}
        for source in original_tests:
            target = design_map.get(_key(source), _key(source))
            alias = _name(worker.epoch, identity["sequence"], slot, ("test", source["name"]))
            test = copy.deepcopy(source)
            test.update({"name": alias, "library": target[0], "cell": target[1], "view": target[2]})
            tests.append(test)
            source_test_map[alias] = source["name"]
            manifest["batch_tests"].append({"name": alias, "source": source["name"], "design": target,
                                            "simulator": source.get("simulator") or "spectre"})
        candidate_workspace["setup"] = copy.deepcopy(setup)
        candidate_workspace["setup"]["tests"] = tests
        artifact = {"candidate_id": candidate_id, "candidate_root": root, "values": dict(requested["values"]),
                    "patch_plan": plans, "protocol_identity": dict(identity), "worker_epoch": worker.epoch,
                    "worker_index": worker.worker_index, "batch_id": identity["candidate_id"], "mode": MODE,
                    "shared_history": True, "source_test_map": source_test_map,
                    "verification_workspace": candidate_workspace, "clone_plan": clones,
                    "clone_records": clone_records, "config_binding_plan": private_config_plan(candidate_workspace, clones),
                    "config_verification_path": clone_records + ".config", "snapshot_roots": []}
        manifest["candidates"].append(artifact)
    manifest["snapshot_lines"] = lines
    return manifest


def snapshot_preflight_lines(manifest):
    # All checks are read-only and precede any OA/test changes or submission.
    required = ("maeCreateTest", "axlSetTestToolArgs", "axlGetTestToolArgs", "maeGetEnabledAnalysis",
                "axlGetTest", "axlRemoveElement", "axlGetCornersForATest", "axlGetCorners",
                "maeSetSetup", "axlGetEnabled", "maeRunSimulation", "ddDeleteObj",
                "maeGetJobPolicy", "axlDetachJobPolicy", "maeGetSetup", "maeGetRunPlan")
    names = " ".join("'%s" % name for name in required)
    source_names = " ".join('"%s"' % _skill_quote(name) for name in manifest["original_enabled_tests"])
    return [
        _contract_text("crescBatchUnsupportedReason = nil"),
        _contract_text("foreach(api list(%s) unless(isCallable(api) crescBatchUnsupportedReason = sprintf(nil \"native_batch_api_unavailable:%%L\" api)))") % names,
        _contract_text("unless(crescBatchUnsupportedReason"),
        _contract_text("  crescNativeParameters = maeGetSetup(?typeName \"parameters\" ?session session)"),
        _contract_text("  when(crescNativeParameters crescBatchUnsupportedReason = \"existing_device_parameters_require_remapping\")"),
        _contract_text("  when(maeGetRunPlan(?session session) crescBatchUnsupportedReason = \"existing_run_plan_requires_candidate_remapping\")"),
        "  foreach(sourceName list(%s)" % source_names,
        "    sourceHandle = axlGetTest(mainSDB sourceName)",
        _contract_text("    unless(sourceHandle && axlGetEnabled(sourceHandle) crescBatchUnsupportedReason = \"source_test_enabled_identity_changed\")"),
        _contract_text("    unless(crescBatchSourceUsesGlobalPolicy(session sourceName) crescBatchUnsupportedReason = \"independent_test_job_policy_requires_serial_adapter\")"),
        "    when(isCallable('axlGetPreRunScript)",
        "      sourcePreRunScript = axlGetPreRunScript(session sourceName)",
        _contract_text("      when(sourcePreRunScript && sourcePreRunScript != \"\" crescBatchUnsupportedReason = \"custom_pre_run_script_requires_serial_adapter\")"),
        "    )",
        "  )",
        ")",
    ]


def snapshot_test_lines(manifest):
    workspace_root = manifest["candidates"][0]["verification_workspace"]["root"]
    scope_path = os.path.join(workspace_root, _contract_text("portability_settings.json"))
    manifest["native_scope_path"] = scope_path
    for candidate in manifest["candidates"]:
        candidate["verification_workspace"]["native_scope_path"] = scope_path
    lines = [
        _contract_text('crescBatchCornerScopePort=outfile("%s")') % _skill_quote(scope_path),
        _contract_text('unless(crescBatchCornerScopePort error("Crescendo batch native corner scope could not be written\\n"))'),
        _contract_text('fprintf(crescBatchCornerScopePort "{\\"schema_version\\":\\"crescendo.batch_native_scope.v1\\",\\"corner_copy_policy\\":\\"inherited_exact_native_readback\\",\\"records\\":[\\n")'),
        _contract_text('crescBatchCornerScopeFirst=t'),
        _contract_text("crescBatchCopiedCornerScopes=makeTable('crescBatchCopiedCornerScopes nil)"),
        _contract_text('procedure(crescRecordBatchCornerScope(stage source alias)'),
        _contract_text('fprintf(crescBatchCornerScopePort "%s{\\"stage\\":%L,\\"source_test\\":%L,\\"alias_test\\":%L,\\"source_corners_skill\\":%L,\\"alias_corners_skill\\":%L,\\"all_corner_groups_skill\\":%L}" if(crescBatchCornerScopeFirst then "" else ",\\n") stage source if(alias then alias else "not_created") sprintf(nil "%L" axlGetCornersForATest(session source)) if(alias then sprintf(nil "%L" axlGetCornersForATest(session alias)) else "not_created") sprintf(nil "%L" axlGetCorners(mainSDB)))'),
        _contract_text('crescBatchCornerScopeFirst=nil'),
        ')',
    ]
    for test in manifest["batch_tests"]:
        alias, source = test["name"], test["source"]
        lib, cell, view = test["design"]
        simulator = test.get("simulator") or "spectre"
        if simulator not in ("spectre", "alps"):
            raise MaestroBatchUnsupported("unqualified saved native simulator")
        lines.extend([
            _contract_text('crescRecordBatchCornerScope("before_copy" "%s" nil)') % _skill_quote(source),
            _contract_text('unless(maeCreateTest("%s" ?sourceTest "%s" ?lib "%s" ?cell "%s" ?view "%s" ?simulator "%s" ?session session) error("Crescendo batch test copy failed\\n"))') % tuple(_skill_quote(v) for v in (alias, source, lib, cell, view, simulator)),
            _contract_text('crescRecordBatchCornerScope("after_copy" "%s" "%s")') % (_skill_quote(source), _skill_quote(alias)),
            'sourceToolArgs = axlGetTestToolArgs(axlGetTest(mainSDB "%s"))' % _skill_quote(source),
            _contract_text('unless(sourceToolArgs error("Crescendo batch source tool options unavailable\\n"))'),
            _contract_text('unless(equal(cadr(assoc("sim" sourceToolArgs)) "%s") error("Crescendo saved batch simulator identity changed\\n"))') % _skill_quote(simulator),
            'batchTestHandle = axlGetTest(mainSDB "%s")' % _skill_quote(alias),
            'copiedToolArgs = axlGetTestToolArgs(batchTestHandle)',
            'batchBindingOptions = append(list(list("lib" "%s") list("cell" "%s") list("view" "%s")) setof(item copiedToolArgs !member(car(item) list("lib" "cell" "view"))))' % tuple(_skill_quote(v) for v in (lib, cell, view)),
            _contract_text('unless(axlSetTestToolArgs(batchTestHandle batchBindingOptions) error("Crescendo batch test binding failed\\n"))'),
            'batchToolArgs = axlGetTestToolArgs(batchTestHandle)',
            _contract_text('unless(equal(assoc("sim" sourceToolArgs) assoc("sim" batchToolArgs)) error("Crescendo batch simulator changed\\n"))'),
            'sourceAnalyses = maeGetEnabledAnalysis("%s" ?session session)' % _skill_quote(source),
            'batchAnalyses = maeGetEnabledAnalysis("%s" ?session session)' % _skill_quote(alias),
            _contract_text('unless(sourceAnalyses && equal(sort(copy(sourceAnalyses) \'alphalessp) sort(copy(batchAnalyses) \'alphalessp)) error("Crescendo batch copied analyses changed\\n"))'),
            _contract_text('unless(equal(cadr(assoc("lib" batchToolArgs)) "%s") && equal(cadr(assoc("cell" batchToolArgs)) "%s") && equal(cadr(assoc("view" batchToolArgs)) "%s") error("Crescendo batch test design readback mismatch\\n"))') % tuple(_skill_quote(v) for v in (lib, cell, view)),
            _contract_text('crescCopyEffectiveJobPolicy(session "%s" "%s")') % (_skill_quote(source), _skill_quote(alias)),
            # maeCreateTest inherits the native corner participation and
            # variables. axlGetCornersForATest returns expanded point names;
            # sending these to maeSetCorner creates additional source groups.
            _contract_text('unless(equal(axlGetCornersForATest(session "%s") axlGetCornersForATest(session "%s")) error("Crescendo batch inherited corner scope differs from source\\n"))') % (_skill_quote(source), _skill_quote(alias)),
            _contract_text('crescBatchCopiedCornerScopes["%s"]=sprintf(nil "%%L" axlGetCornersForATest(session "%s"))') % (_skill_quote(alias), _skill_quote(alias)),
            _contract_text('crescRecordBatchCornerScope("after_corner_verify" "%s" "%s")') % (_skill_quote(source), _skill_quote(alias)),
            _contract_text('fprintf(crescBatchSnapshotVerify "TEST\\t%%L\\t%%L\\t%%L\\t%%L\\t%%L\\n" "%s" "%s" "%s" "%s" "%s")') % tuple(_skill_quote(v) for v in (alias, source, lib, cell, view)),
        ])
    sources = " ".join('"%s"' % _skill_quote(name) for name in manifest["original_enabled_tests"])
    aliases = " ".join('"%s"' % _skill_quote(row["name"]) for row in manifest["batch_tests"])
    lines.extend([
        _contract_text('unless(maeSetSetup(?tests list(%s) ?enabled nil ?session session) error("Crescendo batch source tests could not disable\\n"))') % sources,
        _contract_text('unless(maeSetSetup(?tests list(%s) ?enabled t ?session session) error("Crescendo batch candidate tests could not enable\\n"))') % aliases,
        _contract_text('crescApplyJobOverrides(session list(%s))') % aliases,
        _contract_text('crescBatchTestNames = list(%s)') % aliases,
        _contract_text('crescBatchSourceTestNames = list(%s)') % sources,
    ])
    for test in manifest["batch_tests"]:
        lines.append(_contract_text('unless(equal(crescBatchCopiedCornerScopes["%s"] sprintf(nil "%%L" axlGetCornersForATest(session "%s"))) error("Crescendo batch corner scope changed while enabling candidate tests\\n"))') % (_skill_quote(test["name"]), _skill_quote(test["name"])))
        lines.append(_contract_text('crescRecordBatchCornerScope("after_test_enable" "%s" "%s")') % (_skill_quote(test["source"]), _skill_quote(test["name"])))
    lines.extend([_contract_text('fprintf(crescBatchCornerScopePort "\\n]}\\n")'), _contract_text('close(crescBatchCornerScopePort)')])
    return lines


def snapshot_cleanup_lines(manifest):
    lines = []
    prefix = _contract_text("cr_b_%s_") % str(manifest["worker_epoch"])[:8]
    for row in manifest["owned_cells"]:
        if not str(row.get("cell") or "").startswith(prefix):
            raise ValueError("batch cleanup cell lacks this worker's ownership prefix")
    for row in manifest["batch_tests"]:
        if not str(row.get("name") or "").startswith(prefix):
            raise ValueError("batch cleanup test lacks this worker's ownership prefix")
    for test in manifest["batch_tests"]:
        alias = _skill_quote(test["name"])
        lines.extend([
            'batchTestHandle = axlGetTest(mainSDB "%s")' % alias,
            _contract_text('unless(batchTestHandle error("Crescendo owned batch test disappeared before cleanup\\n"))'),
            _contract_text('unless(axlRemoveElement(batchTestHandle) error("Crescendo owned batch test cleanup rejected\\n"))'),
            _contract_text('when(member("%s" maeGetSetup(?typeName "tests" ?session session)) error("Crescendo owned batch test remains after cleanup\\n"))') % alias,
        ])
    sources = " ".join('"%s"' % _skill_quote(name) for name in manifest["original_enabled_tests"])
    lines.append(_contract_text('unless(maeSetSetup(?tests list(%s) ?enabled t ?session session) error("Crescendo source tests could not restore\\n"))') % sources)
    for row in manifest.get("owned_views") or []:
        target = row["target"]
        if not row.get("token") or row.get("existed_before") is not False or not target[2].startswith(_contract_text("cr_s_") + row["token"][:8] + "_"):
            raise ValueError("batch cleanup view lacks exact ownership")
        args = " ".join('"%s"' % _skill_quote(v) for v in target)
        lines.extend(['batchOwnedView = ddGetObj(%s)' % args,
                      _contract_text('unless(batchOwnedView error("Crescendo owned batch view disappeared\\n"))'),
                      _contract_text('unless(ddDeleteObj(batchOwnedView) error("Crescendo owned batch view cleanup rejected\\n"))'),
                      _contract_text('when(ddGetObj(%s) error("Crescendo owned batch view remains\\n"))') % args])
    # Owned identities are generated, below private copied libraries, and checked
    # before rendering. Never discover targets through a broad prefix scan.
    for cell in manifest["owned_cells"]:
        lib, name = _skill_quote(cell["library"]), _skill_quote(cell["cell"])
        lines.extend([
            'batchOwnedCell = ddGetObj("%s" "%s")' % (lib, name),
            _contract_text('unless(batchOwnedCell error("Crescendo owned batch cell disappeared before cleanup\\n"))'),
            _contract_text('unless(ddDeleteObj(batchOwnedCell) error("Crescendo owned batch cell cleanup rejected\\n"))'),
            _contract_text('when(ddGetObj("%s" "%s") error("Crescendo owned batch cell remains after cleanup\\n"))') % (lib, name),
        ])
    lines.extend([_contract_text('crescBatchTestNames = nil'), _contract_text('crescBatchSourceTestNames = nil'),
                  'batchTestHandle = nil', 'batchOwnedCell = nil', 'batchToolArgs = nil'])
    return lines


def persist_batch_artifacts(manifest):
    candidates = manifest["candidates"]
    # Exact shared keys only: occurrence-specific config, tests, ownership and
    # native corner readback remain distinct. The live native verifier receives
    # the complete in-memory manifest; durable diagnostics store one shared
    # context instead of multiplying the same library/source fingerprints.
    shared = {}
    if candidates:
        first = candidates[0].get("verification_workspace") or {}
        for key, value in first.items():
            if all((candidate.get("verification_workspace") or {}).get(key) == value
                   and key in (candidate.get("verification_workspace") or {}) for candidate in candidates):
                shared[key] = value
    context = {"schema_version": _contract_text("crescendo.maestro_batch_context.v1"), "verification_workspace": shared,
               "protocol_identity": manifest["protocol_identity"]}
    encoded = json.dumps(context, sort_keys=True, separators=(",", ":")).encode("utf-8")
    context_path = os.path.join(manifest["batch_root"], _contract_text("batch_context.json"))
    atomic_write_json(context_path, context)
    reference = {"path": context_path, "sha256": hashlib.sha256(encoded).hexdigest()}
    compact_candidates = []
    for candidate in candidates:
        compact = dict(candidate)
        compact["verification_workspace"] = dict((key, value) for key, value in
            (candidate.get("verification_workspace") or {}).items() if key not in shared)
        compact["shared_workspace_context"] = reference
        compact_candidates.append(compact)
        atomic_write_json(os.path.join(candidate["candidate_root"], _contract_text("persistent_worker_candidate.json")), compact)
    value = dict((key, row) for key, row in manifest.items() if key != "snapshot_lines")
    durable = dict(value)
    durable["candidates"] = compact_candidates
    durable["shared_workspace_context"] = reference
    atomic_write_json(manifest["manifest_path"], durable)
    return value


def expand_batch_artifact(artifact):
    """Read-only recovery/diagnostic helper for current and historical artifacts."""
    reference = artifact.get("shared_workspace_context")
    if not reference:
        return artifact
    from .utils import load_json
    path = reference.get("path")
    root = os.path.dirname(path or "")
    if (not path or os.path.basename(path) != _contract_text("batch_context.json") or os.path.islink(path)
            or not os.path.isfile(path) or os.stat(path).st_nlink != 1):
        raise ValueError("batch shared context is unavailable or unsafe")
    context = load_json(path)
    checksum = hashlib.sha256(json.dumps(context, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
    if checksum != reference.get("sha256") or context.get("protocol_identity") != artifact.get("protocol_identity"):
        raise ValueError("batch shared context identity/checksum changed")
    value = dict(artifact)
    workspace = dict(context.get("verification_workspace") or {})
    workspace.update(artifact.get("verification_workspace") or {})
    value["verification_workspace"] = workspace
    return value


def compact_committed_batch(run_root, batch_id, cleanup, batch_root=None):
    """Replace generated success infrastructure with one authenticated receipt."""
    from .utils import load_json, sha256_file, utc_timestamp
    from .retention import _reject_unsafe_regular_tree, _fsync_directory
    from .runner import validate_checkpoint_integrity
    from .run_data_store import dataset_from_reference
    from .persistent_maestro import (WORKER_SCHEMA_VERSION, _matching_worker_protocol,
                                     _validate_worker_protocol, PersistentMaestroWorker)
    if batch_root is None:
        return {"ok": True, "removed": [], "state": "no_batch_root"}
    run_root, batch_root = os.path.abspath(run_root), os.path.abspath(batch_root)
    if (os.path.dirname(batch_root) != os.path.join(run_root, "batches")
            or os.path.realpath(batch_root) != batch_root):
        raise ValueError("batch compaction escaped its owned run")
    path = os.path.join(batch_root, _contract_text("persistent_worker_batch.json"))
    if not os.path.isfile(path):
        return {"ok": True, "removed": [], "state": "no_generated_batch_manifest"}
    try:
        _reject_unsafe_regular_tree(batch_root, os.stat(run_root).st_dev, "committed native batch")
        manifest = load_json(path)
        identity = manifest.get("protocol_identity")
        if (manifest.get("batch_id") != batch_id or manifest.get("native_completion_proven") is not True
                or manifest.get("shared_history") is not True
                or cleanup.get("ok") is not True):
            raise ValueError("batch completion/cleanup identity is unproven")
        # The whole-run worker returns a public summary without fenced protocol
        # fields. Its successful summary alone is never deletion authority.
        # Re-read the complete native ACK and its marker from this exact batch.
        ack_path = os.path.join(batch_root, _contract_text("persistent_worker_cleanup.json"))
        marker_path = os.path.join(batch_root, "persistent_worker_cleanup.tsv")
        native_ack = load_json(ack_path)
        if (native_ack.get("schema_version") != WORKER_SCHEMA_VERSION
                or native_ack.get("ok") is not True or native_ack.get("tainted") is not False
                or native_ack.get("protocol_identity") != identity
                or native_ack.get("marker_path") != marker_path):
            raise ValueError("batch native cleanup ACK identity is unproven")
        native_rows = _matching_worker_protocol(marker_path, identity)
        terminal = _validate_worker_protocol(
            native_rows, PersistentMaestroWorker.CLEANUP_STATES, "CLEANUP_ERROR")
        if (terminal != "CLEANUP_DONE" or native_rows != native_ack.get("status")
                or native_ack.get("states") != [row["state"] for row in native_rows]):
            raise ValueError("batch native cleanup marker/ACK disagree")
        if cleanup.get("protocol_identity") is not None and cleanup.get("protocol_identity") != identity:
            raise ValueError("batch cleanup result refers to another request")
        checkpoint = load_json(os.path.join(run_root, _contract_text("checkpoint.json")))
        validate_checkpoint_integrity(checkpoint)
        reference = checkpoint.get("candidate_store")
        if not reference or os.path.dirname(reference.get("path") or "") != run_root:
            raise ValueError("batch compaction requires durable core data from this run")
        rows = dict((row["candidate_id"], row) for row in dataset_from_reference(reference)["candidates"])
        for artifact in manifest.get("candidates") or []:
            row = rows.get(artifact["candidate_id"]) or {}
            if row.get("status") not in ("completed", "failed", "infrastructure_failed") or row.get("values") != artifact.get("values"):
                raise ValueError("batch candidate is not durable with matching identity/parameters")
        files = []
        for name in sorted(os.listdir(batch_root)):
            item = os.path.join(batch_root, name)
            if not os.path.isfile(item) or os.stat(item).st_nlink != 1:
                raise ValueError("batch compaction contains pending or unsafe artifacts")
            files.append({"name": name, "bytes": os.path.getsize(item), "sha256": sha256_file(item)})
        receipt_path = os.path.join(batch_root, _contract_text("batch_receipt.json"))
        receipt = {"schema_version": _contract_text("crescendo.maestro_batch_receipt.v1"), "batch_id": batch_id,
                   "protocol_identity": identity, "process_termination_proven": True,
                   "remote_stop_proven": True, "native_completion_proven": True,
                   "proof_scope": "native_batch_jobs_and_snapshot_cleanup",
                   "native_cleanup_states": native_ack["states"],
                   "native_cleanup_status": native_rows,
                   "candidate_ids": [row["candidate_id"] for row in manifest["candidates"]],
                   "core_reference": reference, "checkpoint_sha256": checkpoint.get("payload_sha256"),
                   "generated_artifacts": files, "cleaned_at": utc_timestamp()}
        atomic_write_json(receipt_path, receipt)
        removed = []
        for item in files:
            artifact_path = os.path.join(batch_root, item["name"])
            if sha256_file(artifact_path) != item["sha256"]:
                raise ValueError("batch generated artifact changed after durable receipt")
            os.unlink(artifact_path)
            removed.append(item["name"])
        _fsync_directory(batch_root)
        return {"ok": True, "artifact_cleanup_complete": True, "removed": removed, "receipt": receipt_path}
    except Exception as error:
        return {"ok": False, "artifact_cleanup_complete": False, "removed": [],
                "batch_root": batch_root, "error": "%s: %s" % (error.__class__.__name__, error)}


def validate_snapshot_batch(manifest):
    """Validate each isolated clone/config and the effective test/design mapping."""
    from .persistent_maestro import validate_batch_clones, _validate_pristine_records
    restore = _validate_pristine_records(manifest.get("pristine_cells") or [],
                                        manifest.get("pristine_restore_path") or "", "RESTORED")
    clones = []
    for candidate in manifest["candidates"]:
        clones.append(validate_batch_clones(candidate))
    expected = [(row["name"], row["source"]) + tuple(row["design"]) for row in manifest["batch_tests"]]
    actual = []
    with open(manifest["snapshot_records"], "r") as stream:
        for line in stream:
            fields = line.rstrip("\n").split("\t")
            if fields[0] != "TEST" or len(fields) != 6:
                raise ParameterApplicationFailure("batch snapshot test evidence malformed")
            actual.append(tuple(ast.literal_eval(value) for value in fields[1:]))
    if expected != actual:
        raise ParameterApplicationFailure("batch snapshot test identity evidence mismatch")
    return {"ok": True, "candidate_count": len(manifest["candidates"]),
            "test_count": len(actual), "clone_verification": clones,
            "pristine_restore": restore, "mode": MODE}
