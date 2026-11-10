from __future__ import print_function

import copy
import hashlib
import json
import math
import os
import random
import re
import shutil
import stat
import sys
import time
import traceback
import threading
import queue
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

from .cadence import check_command_cancellation, detect_pdk_context
from .continuation import (
    ContinuationLease, RuntimeOptimizationLease, build_continuation_pending,
    checkpoint_continuation_id, clear_continuation_pending,
    continuation_checkpoint_path, continuation_pending_path,
    continuation_schema_sha256, new_continuation_id,
    persist_continuation_pending,
)
from .hierarchy import (cell_scope_records, numeric_candidate_expression,
                        numeric_parameter_semantic, parse_physical_number, probe_hierarchy)
from .legacy_guardian import MANIFEST_SCHEMA_VERSION as LEGACY_GUARDIAN_PROTOCOL
from .completion import validate_candidate_completion
from .engineering import merge_catalog_domain, normalize_choices, normalize_metric_definitions
from .environment_identity import (
    check_external_eda_environment_metadata, collect_external_eda_environment,
    validate_external_eda_environment_identity,
)
from .ade_setup import build_setup_reference, inspect_setup, normalize_setup_reference
from .adexl_runner import run_adexl
from .metrics import evaluate_metrics, nondominated_front, select_representative
from .netlist_optimizer import PrivateNetlistOptimizer, PrivateNetlistRunError
from .execution_commands import normalize_execution_commands
from .optimizers import PopulationGenerator, validate_candidate_values, validate_variables
from .pdk import (
    GENERIC_ADAPTER_ID, adapter_identity as pdk_adapter_identity,
    build_adapter_evidence, hierarchy_library_references, normalize_adapter_policy, select_adapter,
)
from .persistent_maestro import (
    PersistentMaestroWorker, PersistentMaestroWorkerError,
    PersistentMaestroWorkerProcessError, PersistentMaestroWorkerStartupError,
    PersistentMaestroWorkerTimeout,
    prepare_persistent_batch, read_candidate_status, run_persistent_batch, validate_batch_clones,
    validate_candidate_records, verify_candidate_netlist,
)
from .persistent_controller import (
    PersistentControllerProcessError, PersistentControllerProxy,
    PersistentControllerRemoteError,
)
from .rdb import parse_latest_rdb, parse_rdb, slice_candidate_rdb
from .cluster_execution import (
    batch_timeout_seconds, candidate_batches, candidate_execution_plan,
    slice_candidate_spectre_timing,
    serial_timeout_seconds,
)
from .retention import (
    cleanup_abandoned_nonterminal_candidate,
    cleanup_proven_terminal_infrastructure_candidate,
    lightweight_candidate_record, persist_lightweight_candidate,
    distribute_batch_spectre_logs,
    retained_spectre_paths, safe_delete_candidate_workspace,
    safe_delete_worker_epoch,
    summarize_successful_candidate_artifacts, compact_successful_candidate_artifacts,
)
from .simulators import selected_simulator
from .resources import submit_spectre_script_path
from .session_health import (
    PersistentSessionHealthGuard, PersistentSessionHealthLimit,
    process_identity, sample_owned_process_tree,
)
from .storage_preflight import (
    optimization_workspace_required, startup_storage_preflight,
)
from .utils import (
    atomic_write_json, atomic_write_text, ensure_dir, is_under, load_json, run_id,
    sha256_file, utc_timestamp,
)
from .runtime_layout import optimization_run_name
from .run_log import append_run_event
from .run_data_store import (CORE_NAME, RunDataStore, dataset_from_reference,
                             close_run_data, compact_result_summary,
                             TERMINAL_STATES as DATA_TERMINAL_STATES)
from .workspace import (
    ParameterApplicationFailure, ParameterApplicationInfrastructureFailure,
    WorkspaceBuildFailure,
    adapter_failure_code,
    apply_candidate, build_workspace, run_maestro,
    source_design_identity,
    summarize_spectre_timing,
    verify_parameter_application, verify_sources_unchanged,
)


_RUN_DISPLAY_NAME_MAX_LENGTH = 120


def _validated_run_display_name(value):
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("display_name must be a string")
    display_name = value.strip()
    if not display_name:
        raise ValueError("display_name must not be empty")
    if len(display_name) > _RUN_DISPLAY_NAME_MAX_LENGTH:
        raise ValueError(
            "display_name must be at most %d characters"
            % _RUN_DISPLAY_NAME_MAX_LENGTH
        )
    if any(not character.isprintable() for character in display_name):
        raise ValueError("display_name must not contain control characters")
    return display_name


class _PersistentIdentityGuardSlots(object):
    def __init__(self):
        self.boundary = {}
        self.active = {}

    def expect_replacement(self, worker_index, controller_identity, entry_factory):
        for guards in (self.boundary, self.active):
            guard_entry = entry_factory(None, controller_identity, None)
            guard_entry["expected_replacement"] = True
            guard_entry["initial_startup"] = False
            guards[int(worker_index)] = guard_entry

    def begin_expected_replacement(self, worker_index, observed_at):
        started = False
        for guards in (self.boundary, self.active):
            guard_entry = guards.get(int(worker_index))
            if (
                    guard_entry is not None
                    and guard_entry.get("expected_replacement") is True
                    and guard_entry.get("identity_pending_since") is None):
                guard_entry["identity_pending_since"] = observed_at
                started = True
        return started


def _semantic_parameter(parameter):
    return numeric_parameter_semantic(parameter)


def _parameter_value_type(parameter):
    declared = "".join(
        character for character in str(parameter.get("cdf_parameter_type") or parameter.get("cdf_type") or "").lower()
        if character.isalnum()
    )
    if declared in {"float", "real", "double"}:
        return "float"
    if declared in {
        "int", "integer", "natural", "naturalnumber", "positiveinteger", "long", "fixnum",
    }:
        return "integer"
    if parameter.get("semantic") == "turns":
        for key in ("current_value", "default_value", "numeric_value"):
            number = parse_physical_number(parameter.get(key))
            if number is not None and not number.is_integer():
                return "float"
        try:
            choices = normalize_choices(
                parameter.get("choices") if parameter.get("choices") is not None
                else parameter.get("choices_raw"),
            )
        except (TypeError, ValueError):
            choices = []
        numbers = [parse_physical_number(choice) for choice in choices]
        if (numbers and all(number is not None for number in numbers)
                and any(not number.is_integer() for number in numbers)):
            return "float"
        return "integer"
    if parameter.get("semantic") in ("fingers", "multiplier", "segments", "array"):
        return "integer"
    return "float"


def parameter_catalog(setup, hierarchy):
    check_command_cancellation()
    rows = []
    seen = set()
    library_roles = hierarchy_library_references(hierarchy)
    vendor_owners = (set(library_roles["master_libraries"])
                     - set(library_roles["design_libraries"]))
    for design in hierarchy.get("designs", []):
        check_command_cancellation()
        instances = dict(
            (instance.get("hierarchy_path"), instance)
            for instance in design.get("hierarchy", {}).get("instances", [])
        )
        for parameter_index, parameter in enumerate(design.get("hierarchy", {}).get("parameters", [])):
            if parameter_index % 256 == 0:
                check_command_cancellation()
            key = parameter["parameter_id"]
            if key not in seen:
                row = dict(parameter)
                row["backend"] = "schematic"
                row["semantic"] = _semantic_parameter(row)
                instance = instances.get(row.get("hierarchy_path"), {})
                is_top_analog_source = bool(
                    str(instance.get("master_library") or "").lower() == "analoglib"
                    and str(instance.get("master_cell") or "").lower() in ("vdc", "idc")
                    and str(row.get("hierarchy_path") or "").count("/") == 1
                    and str(row.get("name") or "").lower() == str(instance.get("master_cell") or "").lower()
                )
                if is_top_analog_source:
                    resolved = row.get("numeric_value")
                    row["numeric_value"] = resolved
                    row["candidate_expression"] = numeric_candidate_expression(row)
                    row["tunable"] = bool(
                        row.get("editable") and row.get("display", True) is not False
                        and (resolved is not None or row["candidate_expression"]))
                    row["semantic"] = "voltage" if str(instance.get("master_cell")).lower() == "vdc" else "current"
                    row["parameter_policy"] = "top_level_analog_source"
                    row["optimizable"] = bool(row.get("source") == "cdf" and row.get("tunable"))
                else:
                    row["parameter_policy"] = "device_semantic_filter"
                    row["candidate_expression"] = numeric_candidate_expression(row)
                    if row["candidate_expression"]:
                        row["tunable"] = True
                    row["optimizable"] = bool(
                        row.get("source") == "cdf"
                        and row.get("tunable")
                        and (row.get("numeric_value") is not None or row["candidate_expression"])
                        and row.get("semantic")
                    )
                if row.get("candidate_expression"):
                    row["baseline_expression"] = row.get("current_value")
                    row["requires_point_numeric_value"] = True
                    if (not is_top_analog_source
                            and str(instance.get("master_library") or "").lower() == "analoglib"
                            and str(instance.get("master_cell") or "").lower() in ("vdc", "idc")):
                        row["optimizable"] = False
                        row["optimizable_reason"] = "nested analog source expressions require their existing scope qualification"
                row["value_type"] = _parameter_value_type(row)
                if row.get("netlister_ignored") or instance.get("netlister_ignored"):
                    row["optimizable"] = False
                    row["parameter_policy"] = "netlister_ignored"
                if (row.get("symbol_masked") or instance.get("symbol_masked")
                        or row.get("resolved_view_type") == "unknown"):
                    row["optimizable"] = False
                    row["parameter_policy"] = "opaque_readonly"
                if row.get("owner_library") in vendor_owners:
                    row["optimizable"] = False
                    row["parameter_policy"] = "reference_library_internal"
                    row["optimizable_reason"] = "internal PDK cell is read-only; select its design-instance CDF control"
                row["default_selected"] = bool(row.get("optimizable") and row.get("semantic") in ("width", "length"))
                rows.append(row)
                seen.add(key)
    for test in setup.get("tests", []):
        for variable in test.get("variables", []):
            key = "design_variable:%s:%s:%s:%s:%s" % (
                test.get("library"), test.get("cell"), test.get("view") or "schematic",
                test["name"], variable["name"],
            )
            if key in seen:
                continue
            rows.append({
                "parameter_id": key,
                "backend": "design_variable",
                "name": variable["name"],
                "current_value": variable.get("value"),
                "default_value": variable.get("value"),
                "hierarchy_path": "$design_variables/%s" % test["name"],
                "owner_library": test.get("library"),
                "owner_cell": test.get("cell"),
                "tunable": False,
                "optimizable": False,
                "value_kind": "expression",
            })
            seen.add(key)
    rows.sort(key=lambda row: (row.get("hierarchy_path") or "", row.get("name") or ""))
    check_command_cancellation()
    return rows


def _optimization_output_name(name):
    """Keep named measurements; saved node paths are not optimization metrics."""
    return isinstance(name, str) and bool(name.strip()) and not name.strip().startswith("/")


def output_catalog(setup):
    """Build selectable metrics without changing raw ADE outputs or saved signals."""
    return [{
        "key": "%s::%s" % (test["name"], output["name"]),
        "test": test["name"],
        "name": output["name"],
        "expression": output.get("expression"),
    } for test in setup.get("tests", []) for output in test.get("outputs", [])
        if _optimization_output_name(output.get("name"))]


def cell_selection_scopes(selection):
    """Validate persisted occurrence choices; an empty choice never means full scan."""
    if selection is None:
        return None
    if not isinstance(selection, dict) or not isinstance(selection.get("scopes"), list) or not selection["scopes"]:
        raise ValueError("cell_selection must contain a nonempty scopes array")
    scopes = copy.deepcopy(selection["scopes"])
    for scope in scopes:
        if not isinstance(scope, dict) or not isinstance(scope.get("design"), dict):
            raise ValueError("cell_selection scope requires an exact design identity")
        if any(not isinstance(scope["design"].get(key), str) or not scope["design"][key]
               for key in ("library", "cell", "view")):
            raise ValueError("cell_selection scope has an incomplete design identity")
        path = scope.get("hierarchy_path")
        if not isinstance(path, str) or not path.startswith("/") or "//" in path:
            raise ValueError("cell_selection scope requires an absolute hierarchy_path")
    ids = selection.get("scope_ids")
    if ids is not None and (not isinstance(ids, list) or any(not isinstance(value, str) or not value for value in ids)):
        raise ValueError("cell_selection scope_ids must be an array of identifiers")
    if ids and ids != [scope.get("scope_id") for scope in scopes]:
        raise ValueError("cell_selection identifiers do not match its occurrences")
    return scopes


def prepare_setup(path, config, max_depth=32, backend="auto", selected_cell_paths=None, selected_test=None):
    check_command_cancellation()
    from .setup_baseline import load_bound_setup
    setup = load_bound_setup(path, config, backend=backend)
    from .single_test import project_setup
    if selected_test is None:
        rows = setup.get('_source_tests') or setup.get('tests') or []
        if len(rows) != 1:
            raise ValueError('Select exactly one Test before preparing.')
        selected_test = rows[0].get('name')
    setup = project_setup(setup, selected_test)
    check_command_cancellation()
    if selected_cell_paths is None:
        hierarchy = probe_hierarchy(setup, config, max_depth=max_depth)
    else:
        hierarchy = probe_hierarchy(
            setup, config, max_depth=max_depth, selected_cell_paths=selected_cell_paths,
        )
        cell_selection_scopes({"scopes": selected_cell_paths})
        allowed = dict((tuple(row["design"].get(key) for key in ("library", "cell", "view"))
                        + (row["hierarchy_path"],), row) for row in cell_scope_records(
            hierarchy, setup=setup, config=config,
        ) if row.get("selectable"))
        for selected in selected_cell_paths:
            key = tuple(selected["design"].get(name) for name in ("library", "cell", "view")) + (selected["hierarchy_path"],)
            actual = allowed.get(key)
            if actual is None or any(selected.get(name) is not None and selected[name] != actual.get(name)
                                     for name in ("library", "cell", "view", "scope_id")):
                raise ValueError("Selected cell is absent, changed, or belongs to a reference library")
    check_command_cancellation()
    cadence = (config or {}).get("cadence") or {}
    discovery = cadence.get("discovery") or {}
    pdk_context = detect_pdk_context(setup, hierarchy, cadence)
    check_command_cancellation()
    if pdk_context.get("blocking"):
        details = pdk_context.get("unresolved_libraries") or pdk_context.get(
            "ambiguous_profile_matches"
        ) or pdk_context.get("adapter_errors") or []
        raise ValueError(
            "PDK context cannot be resolved safely: %s. %s" % (
                json.dumps(details, sort_keys=True),
                " ".join(pdk_context.get("recommendations") or []),
            )
        )
    submission = _simulation_submission_metadata(
        config or {}, controller=setup.get("backend") or "maestro",
    )
    external_environment = collect_external_eda_environment(
        setup, cadence, pdk_context, submission=submission,
    )
    check_command_cancellation()
    setup_reference = build_setup_reference(setup)
    prepared = {
        "schema_version": "crescendo.prepared.v1",
        "setup_locator": setup_reference,
        "setup_path": setup_reference["path"],
        "setup": setup,
        "hierarchy": hierarchy,
        "parameters": parameter_catalog(setup, hierarchy),
        "outputs": output_catalog(setup),
        "source_identity": source_design_identity(setup, hierarchy, config=config),
        "eda_context": {
            "cadence": {
                "schema_version": "crescendo.eda_setup.v1",
                "version_checks": discovery.get("version_checks") or [],
            },
            "pdk": pdk_context,
            "submission": submission,
            "external_environment": external_environment["identity"],
            "external_environment_diagnostics": external_environment["diagnostics"],
        },
    }
    if setup_reference["backend"] == "maestro":
        prepared["maestro_path"] = setup_reference["path"]
    if selected_cell_paths is not None:
        prepared["cell_selection"] = {
            "scope_ids": [row["scope_id"] for row in selected_cell_paths if row.get("scope_id")],
            "scopes": copy.deepcopy(selected_cell_paths),
        }
    from .job_policy import capture_setup_job_policy
    prepared["job_policy"] = capture_setup_job_policy(setup, config)
    check_command_cancellation()
    return prepared


def prepare_maestro(path, config, max_depth=32):
    """Backward-compatible Maestro-only preparation entry point."""
    return prepare_setup(path, config, max_depth=max_depth, backend="maestro")


def prepared_setup_reference(prepared=None, spec=None, path_override=None):
    prepared = prepared or {}
    spec = spec or {}
    reference = (
        prepared.get("setup_locator")
        or spec.get("setup_locator")
        or prepared.get("setup_path")
        or spec.get("setup_path")
    )
    legacy_maestro_path = prepared.get("maestro_path") or spec.get("maestro_path")
    if reference is None and legacy_maestro_path:
        reference = {"maestro_path": legacy_maestro_path}
    if reference is None and prepared.get("setup"):
        source = (prepared["setup"].get("source") or {})
        if any(source.get(key) for key in ("setup_db", "maestro_sdb", "data_sdb")):
            reference = prepared["setup"]
    if reference is None and path_override is None:
        return {
            "schema_version": "crescendo.ade_setup_reference.v1",
            "backend": (prepared.get("setup") or {}).get("backend") or "maestro",
            "path": None,
            "sha256": ((prepared.get("setup") or {}).get("source") or {}).get("sha256"),
        }
    return normalize_setup_reference(reference, path_override=path_override)


def _selected_parameters(catalog, variables):
    by_id = dict((row["parameter_id"], row) for row in catalog)
    selected = []
    for variable in variables:
        parameter_id = variable["parameter_id"]
        if parameter_id not in by_id:
            raise ValueError("unknown parameter_id: %s" % parameter_id)
        if not by_id[parameter_id].get("optimizable", by_id[parameter_id].get("numeric_value") is not None):
            raise ValueError("parameter is visible but not numeric-optimizable: %s" % parameter_id)
        selected.append(by_id[parameter_id])
    return selected


def _enforce_catalog_variable_types(variables, selected):
    normalized = []
    for variable, parameter in zip(variables, selected):
        row = merge_catalog_domain(variable, parameter)
        row["value_type"] = row.get("value_type") or _parameter_value_type(parameter)
        if row["value_type"] == "integer":
            row["scale"] = "linear"
        normalized.append(row)
    return normalized


def _mock_parsed(values, objectives, constraints):
    outputs = {}
    base = sum((index + 1) * float(value) for index, value in enumerate(values.values()))
    definitions = list(objectives) + list(constraints)
    for index, definition in enumerate(definitions):
        value = (base - float(definition.get("mock_target", index + 1))) ** 2 + index * 0.01
        outputs[definition["output_key"]] = {"summary": dict((name, value) for name in ("min", "max", "mean", "first", "last"))}
    return {"output_map": outputs}


def _failure_metrics(spec, error):
    penalty_score = 1.0e30
    penalty_violation = 1.0e6
    objectives = [{
        "name": definition.get("name") or definition["output_key"],
        "output_key": definition["output_key"],
        "value": None,
        "score": penalty_score,
        "direction": definition.get("direction", "minimize"),
        "corners": list(definition.get("corners") or []),
        "penalized": True,
    } for definition in spec.get("objectives", [])]
    constraints = [{
        "name": definition.get("name") or definition["output_key"],
        "output_key": definition["output_key"],
        "value": None,
        "violation": penalty_violation,
        "satisfied": False,
        "corners": list(definition.get("corners") or []),
        "penalized": True,
    } for definition in spec.get("constraints", [])]
    return {
        "objectives": objectives,
        "constraints": constraints,
        "feasible": False,
        "total_violation": penalty_violation * max(1, len(constraints)),
        "merit": penalty_score * max(1, len(objectives)),
        "penalized": True,
        "failure_reason": "%s: %s" % (error.__class__.__name__, error),
    }


class InfrastructureFailure(RuntimeError):
    pass


class ExternalEdaEnvironmentChanged(RuntimeError):
    pass


def _select_candidate_pdk_fallback(prepared, error):
    code = adapter_failure_code(error)
    if not code:
        return {"activated": False, "reason": "failure_not_classified"}
    pdk = ((prepared or {}).get("eda_context") or {}).get("pdk") or {}
    contexts = list(pdk.get("contexts") or [])
    if not contexts:
        policy = pdk.get("pdk_adapter_policy")
        if policy is not None:
            context = {
                "id": "legacy", "name": pdk.get("pdk_name"),
                "package_root": pdk.get("pdk_dir"),
                "reference_libraries": pdk.get("candidate_libraries") or [],
                "technology_libraries": [], "adapter_policy": policy,
            }
            context["adapter_evidence"] = build_adapter_evidence(context)
            contexts = [context]
    if len(contexts) != 1:
        return {
            "activated": False, "failure_code": code,
            "reason": "multiple_pdk_contexts_require_explicit_composite_adapter"
            if contexts else "pdk_context_unavailable",
        }
    context = contexts[0]
    policy = context.get("adapter_policy") or pdk.get("pdk_adapter_policy")
    selection = select_adapter(
        policy, context.get("adapter_evidence") or build_adapter_evidence(context),
        generic_failure=code,
    )
    selected = selection.get("selected") or {}
    implementation = selected.get("implementation") or {}
    path = implementation.get("skill_path")
    activated = bool(selection.get("used_fallback") and path and os.path.isfile(path))
    result = {
        "activated": activated, "failure_code": code,
        "reason": selection.get("reason"),
        "context_id": context.get("id"),
        "adapter_id": selected.get("id") or GENERIC_ADAPTER_ID,
        "policy": _pdk_policy_identity(policy),
    }
    if activated:
        result["adapter_path"] = os.path.abspath(path)
        result["adapter_identity"] = pdk_adapter_identity(selected)
    return result


def _evaluate_native_baseline_candidate(run_root, index, generation, values, spec, prepared, selected, config, cancel_check, owned_roots=None):
    """Run one owned Maestro candidate; preserve raw evidence on every outcome."""
    candidate_id = "g%03d_c%04d" % (generation, index)
    candidate_root = ensure_dir(os.path.join(run_root, "candidates", candidate_id))
    if owned_roots is not None:
        owned_roots.add(os.path.abspath(candidate_root))
    started = time.time()
    row = {"candidate_id": candidate_id, "generation": generation, "index": index,
           "values": values, "status": "running", "started_at": utc_timestamp(),
           "execution_backend": "maestro_baseline", "cleanup_pending": True}
    atomic_write_json(os.path.join(candidate_root, "candidate.json"), row)
    try:
        from .native_baseline import evaluate_native_baseline
        result = evaluate_native_baseline(candidate_id, values, selected, spec, prepared, config,
                                          candidate_root, cancel_check=cancel_check)
        command = result.get("command") or {}
        completion = result.get("completion_evidence") or {}
        guard = result.get("source_guard") or {}
        application = result.get("parameter_application") or {}
        if (completion.get("schema_version") != "crescendo.completion_evidence.v1" or
                completion.get("ok") is not True or guard.get("ok") is not True or
                application.get("ok") is not True or command.get("cleanup_pending") is not False):
            raise RuntimeError("Maestro baseline parameter, completion, source or cleanup evidence is incomplete")
        row.update(parameter_application=application,
                   completion_evidence=completion, source_guard=guard, native_command=command)
        # A later metric failure does not revoke the native cleanup already
        # proved by this completed evaluator.
        row["cleanup_pending"] = False
        row["metrics"] = evaluate_metrics(result["parsed"], spec["objectives"], spec.get("constraints", []))
        row["status"] = "completed"
        atomic_write_json(os.path.join(candidate_root, "completion_evidence.json"), completion)
    except Exception as error:
        row.update(status="infrastructure_failed", failure_class="native_baseline",
                   error="%s: %s" % (error.__class__.__name__, error), traceback=traceback.format_exc())
        detail = getattr(error, "detail", None)
        if isinstance(detail, dict):
            row["native_failure_detail"] = copy.deepcopy(detail)
            # Keep the producer stage in lightweight progress/checkpoint rows.
            # The full native detail stays in the candidate evidence file.
            if detail.get("failure_stage"):
                row["failure_stage"] = detail["failure_stage"]
            if isinstance(detail.get("source_guard"), dict):
                row["source_guard"] = copy.deepcopy(detail["source_guard"])
            if (detail.get("cleanup_pending") is False and
                    (detail.get("native_baseline") or {}).get("cleanup_proven") is True):
                row["cleanup_pending"] = False
            if (cancel_check() and row.get("cleanup_pending") is False and
                    (detail.get("native_stop") or {}).get("remote_stop_proven") is True):
                row.update(status="cancelled", failure_class="user_stop", cleanup_pending=False)
    row["finished_at"] = utc_timestamp()
    row["timing"] = {"schema_version": "crescendo.candidate_timing.v1", "candidate_id": candidate_id,
                     "started_epoch": started, "finished_epoch": time.time(),
                     "elapsed_seconds": max(0.0, time.time() - started), "status": row["status"]}
    atomic_write_json(os.path.join(candidate_root, "native_baseline_evidence.json"), {
        "candidate_id": candidate_id, "status": row["status"],
        "command": row.get("native_command"), "failure_detail": row.get("native_failure_detail"),
        "parameter_application": row.get("parameter_application"),
        "source_guard": row.get("source_guard"), "cleanup_pending": row["cleanup_pending"]})
    atomic_write_json(os.path.join(candidate_root, "timing.json"), row["timing"])
    atomic_write_json(os.path.join(candidate_root, "candidate.json"), row)
    return row


def _evaluate_candidate(run_root, index, generation, values, spec, prepared, selected, config):
    candidate_id = "g%03d_c%04d" % (generation, index)
    candidate_root = ensure_dir(os.path.join(run_root, "candidates", candidate_id))
    timing_path = os.path.join(candidate_root, "timing.json")
    timing_started_epoch = time.time()
    timing = {
        "schema_version": "crescendo.candidate_timing.v1", "candidate_id": candidate_id,
        "started_epoch": timing_started_epoch,
        "phases": {
            "lock": {"virtuoso_seconds": 0.0, "maestro_seconds": 0.0, "total_seconds": 0.0},
            "active": {"virtuoso_seconds": 0.0, "maestro_seconds": 0.0, "total_seconds": 0.0},
            "spectre": {"run_count": 0, "completed_run_count": 0, "total_seconds": 0.0, "runs": []},
        },
    }
    atomic_write_json(timing_path, timing)
    row = {
        "candidate_id": candidate_id, "generation": generation, "index": index,
        "values": values, "status": "running", "started_at": utc_timestamp(),
        "execution_backend": (
            "adexl" if (prepared.get("setup") or {}).get("backend") == "adexl"
            else "legacy"
        ),
    }
    atomic_write_json(os.path.join(candidate_root, "candidate.json"), row)
    workspace = None
    try:
        if spec.get("evaluator", "real") == "mock":
            parsed = _mock_parsed(values, spec["objectives"], spec.get("constraints", []))
            row["source_guard"] = {"ok": True, "libraries": []}
        else:
            active_config = copy.deepcopy(config)
            row["simulation_submission"] = _simulation_submission_metadata(
                active_config,
                controller=(prepared.get("setup") or {}).get("backend") or "maestro",
            )
            candidate_timeout = int(spec.get("candidate_timeout_seconds", config["runtime"].get("candidate_timeout_seconds", 600)))
            deadline = time.monotonic() + candidate_timeout
            workspace_root = os.path.join(candidate_root, "workspace")
            workspace_started = time.monotonic()
            workspace = build_workspace(
                prepared["setup"], prepared["hierarchy"], active_config, workspace_root,
                expected_source_identity=prepared.get("source_identity"),
                parameters=selected,
            )
            build_timing = workspace.get("build_timing") or {}
            timing["phases"]["copy"] = {
                "elapsed_seconds": float(build_timing.get("copy_seconds", time.monotonic() - workspace_started)),
                "workspace_build_seconds": float(build_timing.get("elapsed_seconds", time.monotonic() - workspace_started)),
                "source_fingerprint_seconds": float(build_timing.get("source_fingerprint_seconds", 0.0)),
                "rewrite_seconds": float(build_timing.get("rewrite_seconds", 0.0)),
                "library_count": int(build_timing.get("library_count", 0)),
            }
            atomic_write_json(timing_path, timing)
            remaining = max(1, int(deadline - time.monotonic()))
            try:
                patch_result = apply_candidate(
                    workspace, active_config, selected, values, candidate_root,
                    timeout_seconds=remaining,
                )
            except ParameterApplicationFailure as error:
                fallback = _select_candidate_pdk_fallback(prepared, error)
                row["pdk_adapter_fallback"] = fallback
                if not fallback.get("activated"):
                    raise
                source_guard = verify_sources_unchanged(workspace)
                if not source_guard.get("ok"):
                    raise ParameterApplicationInfrastructureFailure(
                        "source library fingerprint changed before PDK adapter fallback"
                    )
                if os.path.isdir(workspace_root) and is_under(workspace_root, candidate_root):
                    shutil.rmtree(workspace_root)
                active_config["cadence"]["pdk_adapter"] = fallback["adapter_path"]
                workspace = build_workspace(
                    prepared["setup"], prepared["hierarchy"], active_config,
                    workspace_root, expected_source_identity=prepared.get("source_identity"),
                    parameters=selected,
                )
                remaining = max(1, int(deadline - time.monotonic()))
                patch_result = apply_candidate(
                    workspace, active_config, selected, values, candidate_root,
                    timeout_seconds=remaining,
                )
            deadline += float((patch_result or {}).get("startup_lock_wait_seconds", 0.0))
            cadence_timing = patch_result.get("cadence_timing") or {} if isinstance(patch_result, dict) else {}
            timing["phases"]["lock"]["virtuoso_seconds"] = float(cadence_timing.get("lock_wait_seconds", 0.0))
            timing["phases"]["active"]["virtuoso_seconds"] = float(cadence_timing.get("active_seconds", 0.0))
            timing["phases"]["cadence"] = cadence_timing
            atomic_write_json(timing_path, timing)
            retries = int(active_config["runtime"].get("infrastructure_retries", 1))
            setup_backend = (prepared.get("setup") or {}).get("backend") or "maestro"
            attempt_key = "adexl_attempts" if setup_backend == "adexl" else "maestro_attempts"
            row[attempt_key] = []
            maestro_timings = []
            for attempt in range(retries + 1):
                remaining = int(deadline - time.monotonic())
                if remaining < 1:
                    raise InfrastructureFailure(
                        "candidate timed out after %d seconds before %s completed"
                        % (candidate_timeout, setup_backend)
                    )
                attempt_root = ensure_dir(os.path.join(candidate_root, "attempts", "attempt_%d" % (attempt + 1)))
                if setup_backend == "adexl":
                    command = run_adexl(
                        workspace, active_config, attempt_root,
                        timeout_seconds=remaining,
                        retained_log_root=candidate_root,
                    )
                else:
                    command = run_maestro(
                        workspace, active_config, attempt_root,
                        timeout_seconds=remaining,
                        retained_log_root=candidate_root,
                    )
                deadline += float(command.get("lock_wait_seconds", 0.0))
                row[attempt_key].append(command)
                maestro_lock = float(command.get("lock_wait_seconds", 0.0))
                maestro_elapsed = float(command.get("elapsed_seconds", 0.0))
                maestro_timings.append({
                    "attempt": attempt + 1, "elapsed_seconds": maestro_elapsed,
                    "lock_wait_seconds": maestro_lock,
                    "active_seconds": max(0.0, maestro_elapsed - maestro_lock),
                    "returncode": command.get("returncode"),
                    "timed_out": bool(command.get("timed_out")),
                    "startup_timed_out": bool(command.get("startup_timed_out")),
                })
                spectre = command.get("spectre_timing") or {}
                timing["phases"]["spectre"]["run_count"] += int(spectre.get("run_count", 0))
                timing["phases"]["spectre"]["completed_run_count"] += int(spectre.get("completed_run_count", 0))
                timing["phases"]["spectre"]["total_seconds"] += float(spectre.get("total_seconds", 0.0))
                timing["phases"]["spectre"]["runs"].extend(spectre.get("runs") or [])
                atomic_write_json(os.path.join(candidate_root, "candidate.json"), row)
                if _retryable_setup_attempt(command, setup_backend) and attempt < retries:
                    continue
                break
            timing["phases"]["maestro"] = {
                "attempts": maestro_timings,
                "elapsed_seconds": sum(item["elapsed_seconds"] for item in maestro_timings),
                "lock_wait_seconds": sum(item["lock_wait_seconds"] for item in maestro_timings),
                "active_seconds": sum(item["active_seconds"] for item in maestro_timings),
            }
            timing["phases"]["lock"]["maestro_seconds"] = timing["phases"]["maestro"]["lock_wait_seconds"]
            timing["phases"]["active"]["maestro_seconds"] = timing["phases"]["maestro"]["active_seconds"]
            atomic_write_json(timing_path, timing)
            if command.get("termination_failed"):
                row["cleanup_pending"] = True
                row["process_cleanup"] = command.get("guardian_cleanup") or {
                    "state": "ambiguous", "reason": "Maestro process cleanup was not proven",
                }
                raise InfrastructureFailure(
                    "Maestro process cleanup could not be proven; raw candidate artifacts were retained"
                )
            if command.get("timed_out"):
                raise InfrastructureFailure(
                    "candidate timed out after %d seconds before %s completed"
                    % (candidate_timeout, setup_backend)
                )
            if command.get("infrastructure_failure"):
                raise InfrastructureFailure(
                    _adexl_infrastructure_failure_reason(command)
                )
            if command["returncode"] != 0:
                raise RuntimeError(
                    "%s batch failed with return code %s"
                    % (setup_backend, command["returncode"])
                )
            row["parameter_application"] = verify_parameter_application(
                workspace, patch_result, candidate_root,
                history_name=command.get("history_name"),
            )
            parse_started = time.monotonic()
            if setup_backend == "adexl":
                parsed = parse_rdb(command["rdb_path"])
            else:
                parsed = parse_latest_rdb(workspace_root)
            timing["phases"]["parse"] = {"elapsed_seconds": time.monotonic() - parse_started}
            atomic_write_json(timing_path, timing)
            row["rdb_path"] = parsed["path"]
            required_outputs = [
                definition["output_key"]
                for definition in spec.get("objectives", []) + spec.get("constraints", [])
            ]
            row["completion_evidence"] = validate_candidate_completion(
                workspace, candidate_root, command, parsed, setup=prepared.get("setup"),
                required_output_keys=required_outputs, config=config,
            )
        row["metrics"] = evaluate_metrics(parsed, spec["objectives"], spec.get("constraints", []))
        row["status"] = "completed"
    except WorkspaceBuildFailure as error:
        row["status"] = "infrastructure_failed"
        row["failure_class"] = "workspace_build"
        row["failure_stage"] = "workspace_build"
        row["spectre_state"] = "not_started"
        row["error"] = "%s: %s" % (error.__class__.__name__, error)
        row["traceback"] = traceback.format_exc()
    except (InfrastructureFailure, ParameterApplicationInfrastructureFailure) as error:
        row["status"] = "infrastructure_failed"
        row["failure_class"] = "infrastructure"
        row["error"] = "%s: %s" % (error.__class__.__name__, error)
        row["traceback"] = traceback.format_exc()
    except ParameterApplicationFailure as error:
        row["status"] = "failed"
        row["failure_class"] = "parameter_application"
        row["error"] = "%s: %s" % (error.__class__.__name__, error)
        row["traceback"] = traceback.format_exc()
        row["metrics"] = _failure_metrics(spec, error)
        if int((timing.get("phases", {}).get("spectre") or {}).get("run_count", 0) or 0):
            row["failure_stage"] = "parameter_verification"
            row["spectre_state"] = "started"
        else:
            row["failure_stage"] = "parameter_application"
            row["spectre_state"] = "not_started"
    except Exception as error:
        row["status"] = "failed"
        row["failure_class"] = "design_or_simulation"
        row["error"] = "%s: %s" % (error.__class__.__name__, error)
        row["traceback"] = traceback.format_exc()
        row["metrics"] = _failure_metrics(spec, error)
    finally:
        if workspace is not None:
            try:
                row["source_guard"] = verify_sources_unchanged(workspace)
            except Exception as error:
                row["source_guard"] = {"ok": False, "error": "%s: %s" % (error.__class__.__name__, error)}
            if not row["source_guard"].get("ok"):
                error = RuntimeError("source library fingerprint changed or could not be verified")
                row["status"] = "infrastructure_failed"
                row["failure_class"] = "source_integrity"
                row["failure_stage"] = "source_integrity"
                row["error"] = "%s: %s" % (error.__class__.__name__, error)
                row.pop("metrics", None)
    row["finished_at"] = utc_timestamp()
    timing["status"] = row["status"]
    timing["finished_epoch"] = time.time()
    timing["elapsed_seconds"] = max(0.0, timing["finished_epoch"] - timing_started_epoch)
    timing["phases"]["lock"]["total_seconds"] = (
        timing["phases"]["lock"]["virtuoso_seconds"] + timing["phases"]["lock"]["maestro_seconds"]
    )
    timing["phases"]["active"]["total_seconds"] = (
        timing["phases"]["active"]["virtuoso_seconds"] + timing["phases"]["active"]["maestro_seconds"]
    )
    atomic_write_json(timing_path, timing)
    atomic_write_json(os.path.join(candidate_root, "candidate.json"), row)
    return row


def _persistent_backend_selected(spec, selected, config, prepared=None):
    if spec.get("evaluator", "real") != "real":
        return False
    mode = str(spec.get("execution_backend") or config.get("runtime", {}).get("execution_backend", "legacy"))
    if mode not in ("auto", "legacy", "persistent_maestro", "adexl", "private_netlist"):
        raise ValueError("execution_backend must be auto, legacy, adexl, persistent_maestro, or private_netlist")
    setup_backend = ((prepared or {}).get("setup") or {}).get("backend") or "maestro"
    if mode == "private_netlist":
        if setup_backend != "maestro":
            raise ValueError("private_netlist currently requires a Maestro setup; select Maestro execution for ADE XL")
        if not selected or any(row.get("backend", "schematic") != "schematic" for row in selected):
            raise ValueError("private_netlist supports selected schematic device parameters; use Maestro for design variables")
        return False
    if setup_backend == "adexl":
        if mode == "persistent_maestro":
            raise ValueError("persistent_maestro cannot run an ADE XL setup")
        return False
    if mode == "adexl":
        raise ValueError("adexl execution requires an ADE XL setup")
    eligible = bool(selected) and all(row.get("backend", "schematic") == "schematic" for row in selected)
    if mode == "persistent_maestro" and not eligible:
        raise ValueError("persistent_maestro requires every selected parameter to use the schematic backend")
    return mode != "legacy" and eligible


def _retryable_setup_attempt(command, setup_backend):
    if command.get("termination_failed"):
        return False
    if command.get("startup_timed_out"):
        return True
    return bool(
        setup_backend == "adexl" and command.get("infrastructure_failure")
    )


def _adexl_infrastructure_failure_reason(command):
    finalization_error = str(command.get("finalization_error") or "").strip()
    if finalization_error:
        return "ADE XL infrastructure failure: %s" % finalization_error
    states = command.get("adexl_protocol_states") or []
    protocol_state = states[-1] if states else "none"
    return (
        "ADE XL process stopped before a terminal candidate protocol state "
        "(returncode=%r, protocol_state=%s)"
        % (command.get("returncode"), protocol_state)
    )


def _persistent_running_row(run_root, index, generation, values):
    candidate_id = "g%03d_c%04d" % (generation, index)
    candidate_root = ensure_dir(os.path.join(run_root, "candidates", candidate_id))
    started_epoch = time.time()
    timing = {
        "schema_version": "crescendo.candidate_timing.v1", "candidate_id": candidate_id,
        "started_epoch": started_epoch, "backend": "persistent_maestro",
        "phases": {
            "lock": {"virtuoso_seconds": 0.0, "maestro_seconds": 0.0, "total_seconds": 0.0},
            "active": {"virtuoso_seconds": 0.0, "maestro_seconds": 0.0, "total_seconds": 0.0},
            "spectre": {"run_count": 0, "completed_run_count": 0, "total_seconds": 0.0, "runs": []},
        },
    }
    row = {
        "candidate_id": candidate_id, "generation": generation, "index": index,
        "values": values, "status": "queued", "started_at": utc_timestamp(),
        "execution_backend": "persistent_maestro",
        "execution_progress": {"stage": "preparing", "tasks": []},
    }
    atomic_write_json(os.path.join(candidate_root, "timing.json"), timing)
    atomic_write_json(os.path.join(candidate_root, "candidate.json"), row)
    return {"row": row, "root": candidate_root, "timing": timing}


def _discard_unreturned_candidate_roots(candidates, returned, run_root, spec):
    returned_ids = set(item["row"]["candidate_id"] for item in returned)
    candidates_root = os.path.abspath(os.path.join(run_root, "candidates"))
    for item in candidates:
        if item["row"]["candidate_id"] in returned_ids:
            continue
        root = os.path.abspath(item["root"])
        try:
            if not is_under(root, candidates_root) or root == candidates_root or os.path.islink(root):
                raise RuntimeError("refusing to discard unsafe staged candidate root: %s" % root)
            if os.path.isdir(root):
                shutil.rmtree(root)
            elif os.path.lexists(root):
                os.unlink(root)
        except Exception as error:
            _persistent_failure(
                item["row"], spec, error, infrastructure=True, failure_class="cleanup",
            )
            returned.append(item)
            returned_ids.add(item["row"]["candidate_id"])


def _persistent_failure(row, spec, error, infrastructure=False, failure_class=None):
    row["status"] = "infrastructure_failed" if infrastructure else "failed"
    row["failure_class"] = failure_class or ("infrastructure" if infrastructure else "design_or_simulation")
    if row["failure_class"] == "source_integrity":
        row["failure_stage"] = "source_integrity"
    row["error"] = "%s: %s" % (error.__class__.__name__, error)
    row["traceback"] = traceback.format_exc()
    if not infrastructure:
        row["metrics"] = _failure_metrics(spec, error)
    else:
        row.pop("metrics", None)


def _simulation_submission_metadata(config, controller="maestro"):
    script = config.get("runtime", {}).get(
        "spectre_submit_script"
    ) or submit_spectre_script_path()
    result = {
        "schema_version": "crescendo.simulation_submission.v1",
        "controller": str(controller or "maestro"),
        "simulator": selected_simulator(config),
        "command_scheduler": config.get("runtime", {}).get("command_scheduler", "auto"),
        "submit_script": os.path.abspath(script),
    }
    runtime = config.get("runtime") or {}
    if runtime.get("maestro_job_overrides"):
        result["maestro_job_overrides"] = copy.deepcopy(runtime["maestro_job_overrides"])
    return result


def _reverify_prepared_source_identity(prepared, config):
    expected = prepared.get("source_identity") if isinstance(prepared, dict) else None
    if not isinstance(expected, dict) or not expected.get("fingerprint"):
        return {
            "ok": False,
            "error": "prepared source design identity is missing or malformed",
        }
    try:
        observed = source_design_identity(
            prepared["setup"], prepared["hierarchy"], config=config,
        )
    except Exception as error:
        return {
            "ok": False,
            "error": "%s: %s" % (error.__class__.__name__, error),
            "before": expected,
        }
    unchanged = bool(
        observed.get("schema_version") == expected.get("schema_version")
        and observed.get("fingerprint") == expected.get("fingerprint")
    )
    result = {"ok": unchanged, "before": expected, "after": observed}
    if not unchanged:
        result["error"] = "source design identity changed after controller retirement"
    return result


def _reverify_prepared_external_environment(prepared, config):
    context = (prepared or {}).get("eda_context") or {}
    expected = context.get("external_environment")
    if expected is None:
        return {"ok": True, "state": "not_recorded"}
    try:
        validate_external_eda_environment_identity(expected)
        observed = collect_external_eda_environment(
            prepared["setup"], (config or {}).get("cadence") or {},
            context.get("pdk") or {},
            submission=(context.get("submission") or _simulation_submission_metadata(
                config or {}, controller=(prepared.get("setup") or {}).get("backend") or "maestro",
            )),
        )
        validate_external_eda_environment_identity(observed["identity"])
    except Exception as error:
        return {
            "ok": False,
            "error": "%s: %s" % (error.__class__.__name__, error),
            "before": expected,
        }
    unchanged = bool(
        observed["identity"].get("schema_version") == expected.get("schema_version")
        and observed["identity"].get("fingerprint") == expected.get("fingerprint")
    )
    result = {
        "ok": unchanged, "before": expected, "after": observed["identity"],
    }
    if unchanged:
        context["external_environment_diagnostics"] = observed["diagnostics"]
    else:
        result["error"] = "external EDA environment changed after ADE preparation"
    return result


def _derived_parameter_cache(application, cache):
    for parameter in (application or {}).get("parameters") or []:
        names = set(cache.get(str(parameter.get("parameter_id"))) or [])
        for effective in parameter.get("effective_netlist") or []:
            for evidence in effective.get("evidence") or []:
                if evidence.get("name"):
                    names.add(str(evidence["name"]))
        if names:
            cache[str(parameter.get("parameter_id"))] = sorted(names)


def _private_netlist_environment_check(prepared, config):
    # Reuse the persistent adapter's verified path-watch/full-identity boundary.
    # The thin adapter serializes this callback; no Maestro controller is made.
    class Boundary(object):
        pass
    boundary = Boundary()
    boundary.prepared = prepared
    boundary.config = config
    boundary.external_environment_full_generation = None
    return lambda generation: _WholeRunPersistentWorker.verify_external_environment_boundary(boundary, generation)


def _process_health(pid):
    result = {"pid": pid, "alive": False, "rss_kib": None, "fd_count": None}
    if not pid:
        return result
    process_root = "/proc/%d" % int(pid)
    if not os.path.isdir(process_root):
        return result
    result["alive"] = True
    try:
        with open(os.path.join(process_root, "status"), "r") as stream:
            for line in stream:
                if line.startswith("VmRSS:"):
                    result["rss_kib"] = int(line.split()[1])
                    break
    except (IOError, OSError, ValueError, IndexError):
        pass
    try:
        result["fd_count"] = len(os.listdir(os.path.join(process_root, "fd")))
    except (IOError, OSError):
        pass
    return result


def _worker_diagnostic_tail(controller, byte_limit=4096):
    control_root = getattr(controller, "control_root", None) if controller is not None else None
    if not control_root:
        return {}
    control_root = os.path.abspath(control_root)
    worker_root = getattr(controller, "worker_root", None)
    if (os.path.realpath(control_root) != control_root or
            (worker_root and not is_under(control_root, os.path.abspath(worker_root)))):
        return {}
    result = {}
    remaining = max(0, int(byte_limit))
    for name in ("ocean.stderr", "ocean.stdout"):
        if remaining <= 0:
            break
        path = os.path.join(control_root, name)
        descriptor = None
        try:
            before = os.lstat(path)
            if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
                continue
            descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
            after = os.fstat(descriptor)
            if (not stat.S_ISREG(after.st_mode) or after.st_nlink != 1 or
                    (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino)):
                continue
            with os.fdopen(descriptor, "rb") as stream:
                descriptor = None
                stream.seek(0, os.SEEK_END)
                size = stream.tell()
                slot_limit = max(1, remaining // 2) if name == "ocean.stderr" else remaining
                read_size = min(size, remaining, slot_limit)
                stream.seek(max(0, size - read_size), os.SEEK_SET)
                text = stream.read(read_size).decode("utf-8", "replace")
                text = "".join(character for character in text
                               if ord(character) >= 32 or character in "\t\n\r")
                encoded = text.encode("utf-8")[-read_size:] if read_size else b""
                result[name] = encoded.decode("utf-8", "ignore")
                remaining -= len(encoded)
        except (IOError, OSError):
            continue
        finally:
            if descriptor is not None:
                os.close(descriptor)
    return result


def _persist_worker_startup_failure(run_root, worker_index, controller, error):
    """Keep bounded native error evidence outside the epoch before cleanup."""
    diagnostic = _worker_diagnostic_tail(controller, byte_limit=16384)
    native_error = _diagnostic_tail_reason(diagnostic)
    message = str(error)
    stage_match = re.search(r"bootstrap failed at ([A-Za-z0-9_]+)", message)
    stage = stage_match.group(1) if stage_match else "bootstrap"
    control_root = os.path.abspath(controller.control_root)
    document = {
        "schema_version": "crescendo.startup_failure.v1",
        "recorded_at": utc_timestamp(),
        "worker_index": worker_index,
        "epoch": getattr(controller, "epoch", None),
        "error_type": error.__class__.__name__, "message": message[:8192],
        "stage": stage, "native_error": native_error,
        "diagnostic": diagnostic,
        "log_paths": dict((name, os.path.join(control_root, name)) for name in
                          ("ocean.stderr", "ocean.stdout", "startup.tsv")),
        "marker": {
            "state": "STARTUP_ERROR" if "bootstrap failed" in message else "unknown",
            "message": message[:8192],
        },
    }
    # Malformed/control-rich native output cannot produce a large archive even
    # after JSON escapes. Only this bounded latest failure survives cleanup.
    while len(json.dumps(document, ensure_ascii=True).encode("utf-8")) > 60000:
        message = document["message"]
        document["message"] = message[len(message) // 2:]
        message = document["marker"]["message"]
        document["marker"]["message"] = message[len(message) // 2:]
        for name in list(document["diagnostic"]):
            text = document["diagnostic"][name]
            document["diagnostic"][name] = text[len(text) // 2:]
    path = os.path.join(run_root, "startup_failure.json")
    atomic_write_json(path, document)
    return path, document


def _diagnostic_tail_reason(diagnostic, limit=600):
    if not isinstance(diagnostic, dict):
        return ""
    lines = []
    for name in ("ocean.stderr", "ocean.stdout"):
        for raw_line in str(diagnostic.get(name) or "").splitlines():
            line = re.sub(r"\s+", " ", raw_line).strip()
            if line:
                lines.append(line)
    if not lines:
        return ""
    preferred = [
        line for line in lines
        if re.search(
            r"CRESCENDO_ADAPTER_ERROR|callback|out[ -]?of[ -]?range|invalid|error|fatal",
            line, re.IGNORECASE,
        )
    ]
    return (preferred[-1] if preferred else lines[-1])[-int(limit):]


def _persistent_protocol_failure_diagnostics(states, spectre_summary, diagnostic):
    states = list(states or [])
    spectre_summary = spectre_summary if isinstance(spectre_summary, dict) else {}
    if "PATCHED" not in states:
        diagnostic_text = json.dumps(diagnostic or {}, sort_keys=True).lower()
        stage = (
            "pdk_callback"
            if "cr_adapter_error" in diagnostic_text or "cdf callback" in diagnostic_text
            else "parameter_application"
        )
        return stage, "not_started"
    if "SIM_DONE" in states or "RESULTS_READY" in states:
        return "result_publication", "started"
    if int(spectre_summary.get("run_count", 0) or 0) <= 0:
        return "netlist_or_submission", "not_started"
    return "spectre", "started"


def _persistent_batch_protocol_failure_diagnostics(
        states, batch_spectre_summary, diagnostic, candidate_root):
    states = list(states or [])
    batch_spectre_summary = (
        batch_spectre_summary if isinstance(batch_spectre_summary, dict) else {}
    )
    if "PATCHED" not in states or "SIM_DONE" in states or "RESULTS_READY" in states:
        return _persistent_protocol_failure_diagnostics(
            states, batch_spectre_summary, diagnostic,
        )
    if int(batch_spectre_summary.get("run_count", 0) or 0) <= 0:
        return _persistent_protocol_failure_diagnostics(
            states, {"run_count": 0}, diagnostic,
        )
    try:
        retained_paths = retained_spectre_paths(candidate_root)
    except Exception:
        retained_paths = []
    if any(path.endswith(".log") for path in retained_paths):
        return _persistent_protocol_failure_diagnostics(
            states, {"run_count": 1}, diagnostic,
        )
    return "netlist_or_spectre", "unknown"


def _spectre_attempt_summary(before, after):
    before = before if isinstance(before, dict) else {}
    after = after if isinstance(after, dict) else {}
    completed_offset = max(0, int(before.get("completed_run_count", 0)))
    runs = list(after.get("runs") or [])[completed_offset:]
    started_count = max(
        0, int(after.get("run_count", 0)) - int(before.get("run_count", 0)),
    )
    return {
        "run_count": started_count,
        "completed_run_count": len([run for run in runs if run.get("returncode") == 0]),
        "reported_end_count": len(runs),
        "unclosed_start_count": max(0, started_count - len(runs)),
        "total_seconds": sum(float(run.get("elapsed_seconds", 0.0)) for run in runs),
        "runs": runs,
    }


def _trusted_candidate_infrastructure_evidence(worker, spectre_summary):
    evidence = []
    trusted_returncodes = {
        124, 126, 127,
        132, 133, 134, 135, 136, 137, 139, 143,
    }
    for run in spectre_summary.get("runs") or []:
        returncode = run.get("returncode")
        if returncode in trusted_returncodes:
            evidence.append({
                "kind": "spectre_submit_returncode", "returncode": returncode,
            })
    if spectre_summary.get("unclosed_start_count", 0) > 0:
        evidence.append({
            "kind": "spectre_submit_process_died",
            "count": int(spectre_summary["unclosed_start_count"]),
        })
    controller = getattr(worker, "controller", None)
    is_alive = getattr(controller, "is_alive", None)
    if callable(is_alive):
        try:
            if not is_alive():
                evidence.append({"kind": "persistent_session_process_died"})
        except Exception:
            pass
    return evidence


class _WholeRunPersistentWorker(object):
    def __init__(self, run_root, worker_index, prepared, selected, config,
                 epoch_namespace=None):
        self.run_root = os.path.abspath(run_root)
        self.worker_index = int(worker_index)
        self.epoch_namespace = (
            None if epoch_namespace is None else int(epoch_namespace)
        )
        if self.epoch_namespace is not None and self.epoch_namespace < 0:
            raise ValueError("worker epoch namespace must be non-negative")
        self.prepared = prepared
        self.selected = list(selected)
        self.config = copy.deepcopy(config)
        self.worker_root = ensure_dir(os.path.join(
            self.run_root, "workers", "worker_%03d" % self.worker_index,
        ))
        existing_epochs = []
        for name in os.listdir(self.worker_root):
            if self.epoch_namespace in (None, 0):
                match = re.match(r"^epoch_(\d+)$", name)
            else:
                prefix = "epoch_c%06d_l" % self.epoch_namespace
                match = re.match(r"^%s(\d+)$" % re.escape(prefix), name)
            if match is None:
                continue
            existing_epochs.append(int(match.group(1)))
        self.epoch_index = max(existing_epochs) + 1 if existing_epochs else 0
        self.start_count = 0
        self.workspace = None
        self.epoch_root = None
        self.controller = None
        self.pending_artifact = None
        self.deferred_artifacts = []
        self.retire_after_cleanup = False
        self.derived_parameter_cache = {}
        self.last_cleanup = None
        self.last_retirement = None
        self.external_environment_full_generation = None
        self.startup_phase_callback = None
        self.pdk_adapter_runtime = {
            "adapter_id": GENERIC_ADAPTER_ID,
            "used_fallback": False,
            "reason": "generic_first",
        }

    def activate_pdk_adapter_fallback(self, fallback):
        if self.controller is not None or self.epoch_root is not None:
            raise PersistentMaestroWorkerProcessError(
                "PDK adapter fallback requires a fully retired worker"
            )
        if not (fallback or {}).get("activated"):
            raise ValueError("PDK adapter fallback was not activated")
        self.config["cadence"]["pdk_adapter"] = fallback["adapter_path"]
        self.pdk_adapter_runtime = copy.deepcopy(fallback)
        self.pdk_adapter_runtime["used_fallback"] = True
        self.derived_parameter_cache = {}
        return copy.deepcopy(self.pdk_adapter_runtime)

    def verify_external_environment_boundary(self, generation):
        context = (self.prepared or {}).get("eda_context") or {}
        if context.get("external_environment") is None:
            return {"ok": True, "state": "not_recorded"}
        try:
            generation = int(generation)
        except (TypeError, ValueError, OverflowError):
            raise ExternalEdaEnvironmentChanged(
                "external EDA boundary generation is malformed"
            )
        diagnostics = context.get("external_environment_diagnostics") or {}
        metadata = check_external_eda_environment_metadata(diagnostics)
        require_full = self.external_environment_full_generation != generation
        if metadata.get("unsafe"):
            raise ExternalEdaEnvironmentChanged(
                "external EDA file became a symbolic link or non-regular file"
            )
        if not require_full and metadata.get("ok"):
            return {
                "ok": True, "state": "metadata_unchanged",
                "checked_files": metadata.get("checked_files", 0),
            }
        guard = _reverify_prepared_external_environment(
            self.prepared, self.config,
        )
        if not guard.get("ok"):
            raise ExternalEdaEnvironmentChanged(
                guard.get("error") or "external EDA environment changed"
            )
        refreshed = check_external_eda_environment_metadata(
            context.get("external_environment_diagnostics") or {},
        )
        if not refreshed.get("ok"):
            raise ExternalEdaEnvironmentChanged(
                "external EDA resolved-file metadata could not be refreshed safely"
            )
        self.external_environment_full_generation = generation
        return {
            "ok": True, "state": "identity_reverified",
            "checked_files": refreshed.get("checked_files", 0),
        }

    def _new_epoch(self):
        local_epoch = self.epoch_index
        self.epoch_index += 1
        if self.epoch_namespace in (None, 0):
            epoch_name = "epoch_%03d" % local_epoch
            epoch_identity = "worker-%03d-epoch-%03d" % (
                self.worker_index, local_epoch,
            )
        else:
            epoch_name = "epoch_c%06d_l%06d" % (
                self.epoch_namespace, local_epoch,
            )
            epoch_identity = "worker-%03d-controller-%06d-epoch-%06d" % (
                self.worker_index, self.epoch_namespace, local_epoch,
            )
        epoch_root = os.path.join(self.worker_root, epoch_name)
        if self.startup_phase_callback is not None:
            self.startup_phase_callback("workspace_prepare", epoch_identity)
        external_guard = _reverify_prepared_external_environment(
            self.prepared, self.config,
        )
        if not external_guard.get("ok"):
            raise ExternalEdaEnvironmentChanged(
                external_guard.get("error") or "external EDA environment changed"
            )
        workspace = build_workspace(
            self.prepared["setup"], self.prepared["hierarchy"], self.config,
            os.path.join(epoch_root, "workspace"),
            expected_source_identity=self.prepared.get("source_identity"),
            parameters=self.selected,
        )
        controller = PersistentMaestroWorker(
            workspace, self.config, self.selected, epoch_root,
            worker_index=self.worker_index,
            epoch=epoch_identity,
        )
        self.workspace = workspace
        self.epoch_root = epoch_root
        self.controller = controller
        if self.startup_phase_callback is not None:
            self.startup_phase_callback("ocean_starting", epoch_identity)
        started = time.monotonic()
        try:
            controller.start()
        except Exception as error:
            try:
                diagnostic_path, diagnostic = _persist_worker_startup_failure(
                    self.run_root, self.worker_index, controller, error,
                )
                error.startup_diagnostic_path = diagnostic_path
                error.startup_native_error = diagnostic["native_error"]
                error.startup_stage = diagnostic["stage"]
                if isinstance(error, PersistentMaestroWorkerStartupError):
                    detail = str(error)
                    if diagnostic["native_error"]:
                        detail += "; native error: " + diagnostic["native_error"]
                    error.args = (detail + "; startup diagnostics: " + diagnostic_path,)
            except Exception as diagnostic_error:
                # A full disk or unreadable diagnostic file must never replace
                # the native error or change the owned termination contract.
                error.startup_diagnostic_write_error = "%s: %s" % (
                    diagnostic_error.__class__.__name__, diagnostic_error,
                )
            raise
        if self.startup_phase_callback is not None:
            self.startup_phase_callback("session_ready", epoch_identity)
        self.start_count += 1
        return {
            "started": True, "startup_seconds": time.monotonic() - started,
            "workspace_build": workspace.get("build_timing") or {},
            "epoch": controller.epoch,
        }

    def ensure_started(self):
        if (
            self.controller is not None and self.controller.is_alive()
            and not self.controller.tainted and self.controller.state == "ready"
        ):
            return {
                "started": False, "startup_seconds": 0.0,
                "workspace_build": {}, "epoch": self.controller.epoch,
            }
        if self.controller is not None or self.epoch_root is not None:
            retirement = self.retire("worker was not reusable")
            if retirement.get("process_still_alive"):
                raise PersistentMaestroWorkerProcessError(
                    "refusing to start a replacement while the previous worker supervisor is alive"
                )
            retirement_cleanup = retirement.get("cleanup") or {}
            if (
                not retirement.get("termination_proven")
                or not retirement_cleanup.get("ok")
                or self.controller is not None
                or self.epoch_root is not None
            ):
                raise PersistentMaestroWorkerProcessError(
                    "refusing to start a replacement because the previous worker was not safely retired"
                )
        return self._new_epoch()

    def health(self):
        process = getattr(self.controller, "process", None) if self.controller else None
        identity = None
        if process is not None and process.poll() is None:
            identity = process_identity(process.pid)
        control_root = getattr(self.controller, "control_root", None) if self.controller else None
        log_paths = []
        if control_root:
            log_paths = [
                os.path.join(control_root, "ocean.stdout"),
                os.path.join(control_root, "ocean.stderr"),
                os.path.join(control_root, "spectre_timeout.log"),
            ]
        storage_paths = [self.run_root]
        if self.workspace and self.workspace.get("root"):
            storage_paths.append(self.workspace["root"])
        result = sample_owned_process_tree(
            [dict(identity, role="maestro_guardian_or_ocean")] if identity else [],
            storage_paths=storage_paths, log_paths=log_paths,
        )
        result.update({
            "worker_index": self.worker_index,
            "epoch": self.controller.epoch if self.controller else None,
            "start_count": self.start_count,
        })
        return result

    def request_abort(self, reason):
        controller = self.controller
        if controller is None:
            return {"requested": False, "reason": "worker has no active controller"}
        return controller.request_abort(reason)

    def _history_artifact_paths(self, artifact):
        if not self.workspace:
            return []
        history_name = artifact["history_name"]
        candidates = [artifact["rdb_path"]]
        for test in self.workspace.get("setup", {}).get("tests", []):
            candidates.append(os.path.join(
                self.workspace["simulation_root"], test["library"], test["cell"],
                "maestro", "results", "maestro", history_name,
            ))
        return candidates

    def set_pending(self, artifact, retire_after_cleanup=False):
        self.pending_artifact = artifact
        self.retire_after_cleanup = bool(retire_after_cleanup)

    def commit_candidate(self, candidate_id):
        artifact = self.pending_artifact
        if artifact is None:
            return {"ok": True, "state": "nothing_pending", "recycled": False}
        if artifact.get("candidate_id") != candidate_id:
            raise RuntimeError("worker cleanup candidate mismatch")
        try:
            protocol = self.controller.request_cleanup(artifact)
            if not protocol.get("ok"):
                diagnostic_tail = _worker_diagnostic_tail(self.controller)
                retirement = self.retire("Maestro result cleanup reported failure")
                result = {
                    "ok": False, "state": "cleanup_failed_recycled", "recycled": True,
                    "protocol": protocol, "diagnostic_tail": diagnostic_tail,
                    "retirement": retirement,
                }
                self.last_cleanup = result
                return result
            self.deferred_artifacts = [artifact]
            result = {
                "ok": bool(protocol.get("ok")), "state": "cleaned",
                "recycled": False, "raw_paths_removed": [],
                "raw_paths_deferred": sum(
                    [self._history_artifact_paths(row) for row in self.deferred_artifacts], [],
                ),
                "health": self.health(),
            }
            self.pending_artifact = None
            if self.retire_after_cleanup:
                self.retire_after_cleanup = False
                retirement = self.retire("result validation tainted the worker")
                result["retirement"] = retirement
                result["recycled"] = True
                retirement_cleanup = retirement.get("cleanup") or {}
                if (
                    retirement.get("process_still_alive")
                    or not (retirement.get("source_guard") or {}).get("ok", True)
                    or not retirement_cleanup.get("ok", True)
                ):
                    result["ok"] = False
                    result["state"] = "retirement_failed"
            self.last_cleanup = result
            return result
        except Exception as error:
            self.retire_after_cleanup = False
            diagnostic_tail = _worker_diagnostic_tail(self.controller)
            retirement = self.retire("result cleanup failed")
            if not (retirement.get("cleanup") or {}).get("ok"):
                self.pending_artifact = artifact
            result = {
                "ok": False, "state": "cleanup_failed", "recycled": True,
                "error": "%s: %s" % (error.__class__.__name__, error),
                "diagnostic_tail": diagnostic_tail, "retirement": retirement,
            }
            self.last_cleanup = result
            return result

    def retire(self, reason):
        controller = self.controller
        workspace = self.workspace
        epoch_root = self.epoch_root
        close_error = None
        source_guard = None
        cleanup = None
        process_still_alive = False
        controller_closed = controller is None
        controller_state = None
        termination_proven = controller is None
        process_termination_proven = controller is None
        remote_stop = {"remote_stop_proven": True, "no_remote_jobs_started": True} if controller is None else {}
        native_close_result = None
        diagnostic_tail = _worker_diagnostic_tail(controller)
        if controller is not None:
            abort = getattr(controller, "request_batch_abort", None)
            if callable(abort):
                try:
                    remote_stop = abort(reason=reason)
                except Exception as error:
                    remote_stop = {"remote_stop_proven": False,
                                   "error": "%s: %s" % (error.__class__.__name__, error)}
            try:
                if controller.is_alive() and not controller.tainted:
                    native_close_result = controller.close()
                else:
                    native_close_result = controller.terminate(reason=reason)
            except Exception as error:
                close_error = "%s: %s" % (error.__class__.__name__, error)
                try:
                    native_close_result = controller.terminate(reason=reason)
                except Exception:
                    pass
            process = getattr(controller, "process", None)
            process_still_alive = bool(process is not None and process.poll() is None)
            controller_closed = getattr(controller, "closed", None) is True
            controller_state = getattr(controller, "state", None)
            process_termination_proven = bool(
                controller_closed
                and controller_state in ("closed", "terminated")
                and not process_still_alive
            )
            if isinstance(native_close_result, dict) and "process_termination_proven" in native_close_result:
                process_termination_proven = bool(native_close_result["process_termination_proven"] is True
                                                  and not process_still_alive)
            elif not process_termination_proven and not process_still_alive and getattr(controller, "_use_guardian", False):
                # A view cleanup failure must not erase an independently
                # validated durable Guardian process-termination proof.
                from .persistent_controller import _worker_epoch_proof
                process_termination_proven = bool(epoch_root and _worker_epoch_proof(epoch_root).get("ok"))
            if isinstance(native_close_result, dict) and "remote_stop_proven" in native_close_result:
                remote_stop = dict(remote_stop or {}, remote_stop_proven=native_close_result["remote_stop_proven"])
            # Legacy adapters without a remote API retain their existing
            # process contract; do not fabricate a remote acknowledgement.
            termination_proven = bool(process_termination_proven and
                                      (remote_stop or {}).get("remote_stop_proven") is not False)
        if workspace is not None:
            try:
                source_guard = verify_sources_unchanged(workspace)
            except Exception as error:
                source_guard = {"ok": False, "error": "%s: %s" % (error.__class__.__name__, error)}
        if (
            termination_proven and epoch_root is not None and os.path.isdir(epoch_root)
            and source_guard and source_guard.get("ok")
        ):
            try:
                cleanup = safe_delete_worker_epoch(epoch_root, self.run_root, source_guard)
            except Exception as error:
                cleanup = {"ok": False, "error": "%s: %s" % (error.__class__.__name__, error)}
                if getattr(error, "detail", None):
                    cleanup["detail"] = error.detail
        elif process_still_alive:
            cleanup = {
                "ok": False, "removed": False,
                "error": "worker supervisor is still alive; epoch retained for safety",
            }
        elif not termination_proven:
            cleanup = {
                "ok": False, "removed": False,
                "error": (
                    "worker termination was not proven; epoch retained for safety "
                    "(closed=%r state=%r close_error=%r)"
                    % (controller_closed, controller_state, close_error)
                ),
            }
        elif epoch_root is not None and cleanup is None:
            cleanup = {
                "ok": False, "removed": False,
                "error": "worker epoch could not be safely verified and removed",
            }
        result = {
            "reason": reason, "close_error": close_error,
            "process_still_alive": process_still_alive,
            "controller_closed": controller_closed,
            "controller_state": controller_state,
            "termination_proven": termination_proven,
            "process_termination_proven": process_termination_proven,
            "remote_stop_proven": (remote_stop or {}).get("remote_stop_proven"),
            "remote_stop": remote_stop,
            "artifact_cleanup_complete": (cleanup or {}).get("ok") is True,
            "source_guard": source_guard, "cleanup": cleanup,
            "diagnostic_tail": diagnostic_tail,
        }
        if cleanup is not None:
            cleanup.setdefault("artifact_cleanup_complete", result["artifact_cleanup_complete"])
            cleanup.setdefault("state", "all_epochs_removed" if result["artifact_cleanup_complete"] else
                               ("artifact_cleanup_failed" if termination_proven else "unproven"))
            if not cleanup.get("ok"):
                result["cleanup_error"] = cleanup.get("error")
        self.last_retirement = copy.deepcopy(result)
        if epoch_root is not None and os.path.isdir(epoch_root):
            try:
                atomic_write_json(os.path.join(epoch_root, "retirement.json"), result)
            except Exception:
                pass
        if (
            not termination_proven
            or process_still_alive
            or not (cleanup or {}).get("ok")
        ):
            return result
        self.controller = None
        self.workspace = None
        self.epoch_root = None
        self.pending_artifact = None
        self.deferred_artifacts = []
        self.retire_after_cleanup = False
        self.derived_parameter_cache = {}
        return result

    def close(self):
        if self.controller is None and self.epoch_root is None:
            if self.last_retirement is not None and (self.last_retirement.get("cleanup") or {}).get("ok"):
                result = copy.deepcopy(self.last_retirement)
                result["prior_retirement_reason"] = result.get("reason")
                result["reason"] = "already_closed"
                return result
            return {"reason": "already_closed", "process_still_alive": False,
                    "termination_proven": True, "process_termination_proven": True,
                    "remote_stop_proven": True, "artifact_cleanup_complete": True,
                    "cleanup": {"ok": True, "removed": False, "state": "no_owned_epoch"}}
        return self.retire("optimization finished")


def _evaluate_persistent_candidate(worker, run_root, index, generation, values,
                                   spec, prepared, selected, config):
    item = _persistent_running_row(run_root, index, generation, values)
    row = item["row"]
    timing = item["timing"]
    attempts = []
    artifact = None
    source_guard = None
    protocol_completed = False
    parameter_application_verified = False
    retries = max(0, int(config["runtime"].get("infrastructure_retries", 1)))
    infrastructure_retries_used = 0
    fallback_used = False
    for attempt in range(retries + 2):
        try:
            if getattr(worker, "_native_stop_requested", False):
                raise InfrastructureFailure("serial cluster candidate stopped before submission")
            boundary_guard = getattr(
                worker, "verify_external_environment_boundary", None,
            )
            if callable(boundary_guard):
                boundary_guard(generation)
            startup = worker.ensure_started()
            if getattr(worker, "_native_stop_requested", False):
                raise InfrastructureFailure("serial cluster candidate stopped during preparation")
            before_spectre = summarize_spectre_timing(worker.controller.spectre_log, worker.workspace["root"])
            artifact = worker.controller.evaluate(
                row["candidate_id"], values, item["root"],
                timeout_seconds=serial_timeout_seconds(spec, config),
            )
            states = list(artifact.get("states") or [])
            protocol_completed = bool(states and states[-1] == "DONE")
            attempts.append({
                "attempt": attempt + 1, "epoch": worker.controller.epoch,
                "states": states, "startup": startup,
            })
            if states and states[-1] == "ERROR":
                after_spectre = summarize_spectre_timing(
                    worker.controller.spectre_log, worker.workspace["root"],
                )
                spectre_summary = _spectre_attempt_summary(before_spectre, after_spectre)
                timing["phases"]["spectre"] = spectre_summary
                infrastructure_evidence = _trusted_candidate_infrastructure_evidence(
                    worker, spectre_summary,
                )
                diagnostic_tail = _worker_diagnostic_tail(worker.controller)
                diagnostic_reason = _diagnostic_tail_reason(diagnostic_tail)
                failure_stage, spectre_state = _persistent_protocol_failure_diagnostics(
                    states, spectre_summary, diagnostic_tail,
                )
                error = RuntimeError(
                    "persistent Maestro candidate failed during %s%s" % (
                        failure_stage,
                        ": " + diagnostic_reason if diagnostic_reason else "",
                    )
                )
                attempts[-1]["diagnostic_tail"] = diagnostic_tail
                attempts[-1]["spectre"] = spectre_summary
                attempts[-1]["infrastructure_evidence"] = infrastructure_evidence
                fallback = (
                    _select_candidate_pdk_fallback(prepared, diagnostic_tail)
                    if "PATCHED" not in states and not fallback_used else None
                )
                if fallback is not None:
                    attempts[-1]["pdk_adapter_fallback"] = fallback
                retirement = worker.retire("candidate reported ERROR")
                attempts[-1]["retirement"] = retirement
                source_guard = retirement.get("source_guard") or source_guard
                artifact = None
                retirement_cleanup = retirement.get("cleanup") or {}
                safely_recycled = bool(
                    retirement.get("termination_proven")
                    and not retirement.get("process_still_alive")
                    and retirement_cleanup.get("ok")
                    and (retirement.get("source_guard") or {}).get("ok", True)
                )
                if source_guard is not None and not source_guard.get("ok"):
                    _persistent_failure(
                        row, spec,
                        RuntimeError("source library fingerprint changed or could not be verified"),
                        infrastructure=True, failure_class="source_integrity",
                    )
                    break
                if (
                        fallback and fallback.get("activated")
                        and not infrastructure_evidence and safely_recycled
                ):
                    worker.activate_pdk_adapter_fallback(fallback)
                    fallback_used = True
                    protocol_completed = False
                    parameter_application_verified = False
                    continue
                if infrastructure_evidence:
                    if infrastructure_retries_used < retries and safely_recycled:
                        infrastructure_retries_used += 1
                        protocol_completed = False
                        parameter_application_verified = False
                        continue
                    _persistent_failure(
                        row, spec, error, infrastructure=True,
                        failure_class="simulation_infrastructure",
                    )
                    row["failure_stage"] = failure_stage
                    row["spectre_state"] = spectre_state
                    break
                failure_class = (
                    "parameter_application" if "PATCHED" not in states
                    else "design_or_simulation"
                )
                _persistent_failure(row, spec, error, failure_class=failure_class)
                row["failure_stage"] = failure_stage
                row["spectre_state"] = spectre_state
                break
            worker.controller.require_candidate_time(artifact, "candidate patch record validation")
            patch_result = validate_candidate_records(artifact)
            worker.controller.require_candidate_time(artifact, "candidate patch record validation")
            worker.controller.require_candidate_time(artifact, "final Spectre netlist verification")
            row["parameter_application"] = verify_candidate_netlist(
                worker.workspace, artifact, patch_result=patch_result,
                derived_parameter_names=worker.derived_parameter_cache,
            )
            worker.controller.require_candidate_time(artifact, "final Spectre netlist verification")
            parameter_application_verified = True
            _derived_parameter_cache(row["parameter_application"], worker.derived_parameter_cache)
            worker.controller.require_candidate_time(artifact, "Maestro result parsing")
            parsed = parse_rdb(artifact["rdb_path"])
            worker.controller.require_candidate_time(artifact, "Maestro result parsing")
            candidate_command = {
                "returncode": 0, "timed_out": False, "startup_timed_out": False,
                "execution_simulator": selected_simulator(worker.config),
                "spectre_timing": (summarize_spectre_timing(worker.controller.spectre_log, worker.workspace["root"]) if selected_simulator(worker.config) == "alps" else None),
                "history_name": artifact["history_name"],
                "history_identity": artifact["history_identity"],
                "started_epoch": artifact.get("started_epoch"),
                "finished_epoch": artifact.get("finished_epoch"),
                "native_corner_identity": artifact.get("native_corner_identity"),
                "native_storage_paths": artifact.get("native_storage_paths"),
            }
            if artifact.get("native_storage_paths") is not None:
                row["native_storage_paths"] = copy.deepcopy(artifact["native_storage_paths"])
            row["maestro_attempts"] = [candidate_command]
            row["rdb_path"] = parsed["path"]
            required_outputs = [
                definition["output_key"]
                for definition in spec.get("objectives", []) + spec.get("constraints", [])
            ]
            worker.controller.require_candidate_time(artifact, "simulation completion validation")
            row["completion_evidence"] = validate_candidate_completion(
                worker.workspace, item["root"], candidate_command, parsed,
                setup=prepared.get("setup"), required_output_keys=required_outputs, config=worker.config,
            )
            if row.get("native_storage_paths") is not None:
                row["completion_evidence"]["native_storage_paths"] = row["native_storage_paths"]
            worker.controller.require_candidate_time(artifact, "simulation completion validation")
            worker.controller.require_candidate_time(artifact, "objective and constraint evaluation")
            row["metrics"] = evaluate_metrics(
                parsed, spec["objectives"], spec.get("constraints", []),
            )
            worker.controller.require_candidate_time(artifact, "objective and constraint evaluation")
            worker.controller.require_candidate_time(artifact, "source library integrity verification")
            source_guard = verify_sources_unchanged(worker.workspace)
            worker.controller.require_candidate_time(artifact, "source library integrity verification")
            row["source_guard"] = source_guard
            after_spectre = summarize_spectre_timing(worker.controller.spectre_log, worker.workspace["root"])
            spectre_summary = _spectre_attempt_summary(
                before_spectre, after_spectre,
            )
            timing["phases"]["spectre"] = spectre_summary
            infrastructure_evidence = _trusted_candidate_infrastructure_evidence(
                worker, spectre_summary,
            )
            attempts[-1]["spectre"] = spectre_summary
            attempts[-1]["infrastructure_evidence"] = infrastructure_evidence
            if infrastructure_evidence:
                error = InfrastructureFailure(
                    "trusted Spectre infrastructure failure after successful result validation"
                )
                worker.set_pending(artifact, retire_after_cleanup=True)
                cleanup_result = worker.commit_candidate(row["candidate_id"])
                retirement = cleanup_result.get("retirement") or {}
                attempts[-1]["cleanup"] = cleanup_result
                source_guard = retirement.get("source_guard") or source_guard
                retirement_cleanup = retirement.get("cleanup") or {}
                safely_recycled = bool(
                    cleanup_result.get("ok")
                    and retirement
                    and retirement.get("termination_proven")
                    and not retirement.get("process_still_alive")
                    and retirement_cleanup.get("ok")
                    and (retirement.get("source_guard") or {}).get("ok", True)
                )
                if source_guard is not None and not source_guard.get("ok"):
                    _persistent_failure(
                        row, spec,
                        RuntimeError("source library fingerprint changed or could not be verified"),
                        infrastructure=True, failure_class="source_integrity",
                    )
                    break
                if infrastructure_retries_used < retries and safely_recycled:
                    infrastructure_retries_used += 1
                    artifact = None
                    protocol_completed = False
                    parameter_application_verified = False
                    for key in (
                            "completion_evidence", "metrics", "rdb_path",
                            "parameter_application", "maestro_attempts"
                    ):
                        row.pop(key, None)
                    continue
                _persistent_failure(
                    row, spec, error, infrastructure=True,
                    failure_class="simulation_infrastructure",
                )
                break
            if callable(boundary_guard):
                boundary_guard(generation)
            row["status"] = "completed"
            worker.set_pending(artifact)
            break
        except ExternalEdaEnvironmentChanged:
            raise
        except WorkspaceBuildFailure as error:
            _persistent_failure(
                row, spec, error, infrastructure=True,
                failure_class="workspace_build",
            )
            row["failure_stage"] = "workspace_build"
            row["spectre_state"] = "not_started"
            break
        except ParameterApplicationInfrastructureFailure as error:
            if attempts:
                attempts[-1]["postprocess_error"] = "%s: %s" % (
                    error.__class__.__name__, error,
                )
                attempts[-1]["diagnostic_tail"] = _worker_diagnostic_tail(worker.controller)
            cleanup_result = None
            retirement = None
            if artifact is not None and worker.controller is not None and worker.controller.pending_cleanup:
                worker.set_pending(artifact, retire_after_cleanup=True)
                cleanup_result = worker.commit_candidate(row["candidate_id"])
                retirement = cleanup_result.get("retirement") or {}
                if attempts:
                    attempts[-1]["cleanup"] = cleanup_result
            else:
                retirement = worker.retire("result publication verification failed")
                if attempts:
                    attempts[-1]["retirement"] = retirement
            source_guard = (retirement or {}).get("source_guard") or source_guard
            retirement_cleanup = (retirement or {}).get("cleanup") or {}
            safely_recycled = bool(
                retirement
                and retirement.get("termination_proven")
                and not retirement.get("process_still_alive")
                and retirement_cleanup.get("ok")
                and (retirement.get("source_guard") or {}).get("ok", True)
            )
            if cleanup_result is not None:
                safely_recycled = bool(cleanup_result.get("ok") and safely_recycled)
            if source_guard is not None and not source_guard.get("ok"):
                _persistent_failure(
                    row, spec,
                    RuntimeError("source library fingerprint changed or could not be verified"),
                    infrastructure=True, failure_class="source_integrity",
                )
                break
            if infrastructure_retries_used < retries and safely_recycled:
                infrastructure_retries_used += 1
                artifact = None
                protocol_completed = False
                parameter_application_verified = False
                continue
            _persistent_failure(
                row, spec, error, infrastructure=True,
                failure_class="result_publication",
            )
            break
        except ParameterApplicationFailure as error:
            fallback = (
                _select_candidate_pdk_fallback(prepared, error)
                if not fallback_used else None
            )
            cleanup_result = None
            retirement = None
            if fallback is not None:
                if attempts:
                    attempts[-1]["pdk_adapter_fallback"] = fallback
                else:
                    row["pdk_adapter_fallback"] = fallback
            if fallback and fallback.get("activated"):
                if artifact is not None and worker.controller is not None and worker.controller.pending_cleanup:
                    worker.set_pending(artifact, retire_after_cleanup=True)
                    cleanup_result = worker.commit_candidate(row["candidate_id"])
                    retirement = cleanup_result.get("retirement") or {}
                else:
                    retirement = worker.retire("generic PDK adapter compatibility failure")
                source_guard = (retirement or {}).get("source_guard") or source_guard
                retirement_cleanup = (retirement or {}).get("cleanup") or {}
                safely_recycled = bool(
                    retirement
                    and retirement.get("termination_proven")
                    and not retirement.get("process_still_alive")
                    and retirement_cleanup.get("ok")
                    and (retirement.get("source_guard") or {}).get("ok", True)
                )
                if cleanup_result is not None:
                    safely_recycled = bool(cleanup_result.get("ok") and safely_recycled)
                fallback["retirement_safe"] = safely_recycled
                if attempts:
                    attempts[-1]["pdk_adapter_fallback"] = fallback
                if safely_recycled:
                    worker.activate_pdk_adapter_fallback(fallback)
                    fallback_used = True
                    artifact = None
                    protocol_completed = False
                    parameter_application_verified = False
                    for key in (
                            "completion_evidence", "metrics", "rdb_path",
                            "parameter_application", "maestro_attempts",
                    ):
                        row.pop(key, None)
                    continue
            _persistent_failure(row, spec, error, failure_class="parameter_application")
            spectre_runs = int(
                (timing.get("phases", {}).get("spectre") or {}).get("run_count", 0) or 0
            )
            if protocol_completed or spectre_runs:
                row["failure_stage"] = "parameter_verification"
                row["spectre_state"] = "started"
            else:
                failure_text = str(error).lower()
                row["failure_stage"] = (
                    "pdk_callback"
                    if adapter_failure_code(error) or "callback" in failure_text
                    else "parameter_application"
                )
                row["spectre_state"] = "not_started"
            if artifact is not None and worker.controller is not None and worker.controller.pending_cleanup:
                worker.set_pending(artifact, retire_after_cleanup=True)
            else:
                retirement = worker.retire("parameter verification failed")
                source_guard = retirement.get("source_guard") or source_guard
            break
        except PersistentMaestroWorkerError as error:
            startup_failure = isinstance(error, PersistentMaestroWorkerStartupError)
            attempts.append({
                "attempt": attempt + 1, "error": "%s: %s" % (error.__class__.__name__, error),
                "startup_failure": startup_failure,
                "diagnostic_tail": _worker_diagnostic_tail(worker.controller),
            })
            retirement = worker.retire("persistent infrastructure failure")
            source_guard = retirement.get("source_guard") or source_guard
            if source_guard is not None and not source_guard.get("ok"):
                _persistent_failure(
                    row, spec,
                    RuntimeError("source library fingerprint changed or could not be verified"),
                    infrastructure=True, failure_class="source_integrity",
                )
                break
            if infrastructure_retries_used < retries:
                infrastructure_retries_used += 1
                continue
            _persistent_failure(
                row, spec, error, infrastructure=True,
                failure_class="persistent_startup" if startup_failure else "infrastructure",
            )
            if startup_failure:
                row["failure_stage"] = "maestro_startup"
                row["spectre_state"] = "not_started"
            break
        except Exception as error:
            spectre_summary = None
            infrastructure_evidence = []
            if worker.controller is not None and worker.workspace is not None:
                try:
                    after_spectre = summarize_spectre_timing(
                        worker.controller.spectre_log, worker.workspace["root"],
                    )
                    spectre_summary = _spectre_attempt_summary(
                        before_spectre, after_spectre,
                    )
                    timing["phases"]["spectre"] = spectre_summary
                    infrastructure_evidence = _trusted_candidate_infrastructure_evidence(
                        worker, spectre_summary,
                    )
                except Exception as evidence_error:
                    if attempts:
                        attempts[-1]["spectre_evidence_error"] = "%s: %s" % (
                            evidence_error.__class__.__name__, evidence_error,
                        )
            if attempts and spectre_summary is not None:
                attempts[-1]["postprocess_error"] = "%s: %s" % (
                    error.__class__.__name__, error,
                )
                attempts[-1]["spectre"] = spectre_summary
                attempts[-1]["infrastructure_evidence"] = infrastructure_evidence
            if infrastructure_evidence:
                cleanup_result = None
                retirement = None
                if (
                        artifact is not None and worker.controller is not None
                        and worker.controller.pending_cleanup
                ):
                    worker.set_pending(artifact, retire_after_cleanup=True)
                    cleanup_result = worker.commit_candidate(row["candidate_id"])
                    retirement = cleanup_result.get("retirement") or {}
                    if attempts:
                        attempts[-1]["cleanup"] = cleanup_result
                else:
                    retirement = worker.retire(
                        "trusted Spectre infrastructure failure during result validation"
                    )
                    if attempts:
                        attempts[-1]["retirement"] = retirement
                source_guard = (retirement or {}).get("source_guard") or source_guard
                retirement_cleanup = (retirement or {}).get("cleanup") or {}
                safely_recycled = bool(
                    retirement
                    and retirement.get("termination_proven")
                    and not retirement.get("process_still_alive")
                    and retirement_cleanup.get("ok")
                    and (retirement.get("source_guard") or {}).get("ok", True)
                )
                if cleanup_result is not None:
                    safely_recycled = bool(cleanup_result.get("ok") and safely_recycled)
                if source_guard is not None and not source_guard.get("ok"):
                    _persistent_failure(
                        row, spec,
                        RuntimeError("source library fingerprint changed or could not be verified"),
                        infrastructure=True, failure_class="source_integrity",
                    )
                    break
                if infrastructure_retries_used < retries and safely_recycled:
                    infrastructure_retries_used += 1
                    artifact = None
                    protocol_completed = False
                    parameter_application_verified = False
                    continue
                _persistent_failure(
                    row, spec, error, infrastructure=True,
                    failure_class="simulation_infrastructure",
                )
                break
            _persistent_failure(row, spec, error, failure_class="design_or_simulation")
            if artifact is not None and worker.controller is not None and worker.controller.pending_cleanup:
                worker.set_pending(
                    artifact,
                    retire_after_cleanup=not (
                        protocol_completed and parameter_application_verified
                    ),
                )
            else:
                retirement = worker.retire("result evaluation failed")
                source_guard = retirement.get("source_guard") or source_guard
            break
    if source_guard is None and worker.workspace is not None:
        try:
            source_guard = verify_sources_unchanged(worker.workspace)
        except Exception as error:
            source_guard = {"ok": False, "error": "%s: %s" % (error.__class__.__name__, error)}
    if source_guard is None:
        source_guard = {"ok": True, "libraries": [], "workspace_created": False}
    row["source_guard"] = source_guard
    if not source_guard.get("ok"):
        error = RuntimeError("source library fingerprint changed or could not be verified")
        _persistent_failure(row, spec, error, infrastructure=True, failure_class="source_integrity")
        if artifact is not None and worker.pending_artifact is not None:
            worker.retire_after_cleanup = True
        else:
            retirement = worker.retire("source integrity failure")
            source_guard = retirement.get("source_guard") or source_guard
    row["simulation_submission"] = _simulation_submission_metadata(config)
    if config.get("runtime", {}).get("native_cluster_stop_required"):
        native_state = artifact or getattr(worker.controller, "_active_batch", None) or {}
        row["native_job_state"] = dict((key, native_state.get(key)) for key in (
            "simulation_submitted", "no_remote_jobs_started",
            "native_completion_proven", "native_stop_proven",
        ))
    row["persistent_worker"] = {
        "worker_index": worker.worker_index,
        "worker_lifetime": "optimization_run",
        "session_start_count": worker.start_count,
        "epoch": worker.controller.epoch if worker.controller else None,
        "attempts": attempts,
        "health": worker.health(),
        "cleanup_pending": worker.pending_artifact is not None,
        "pdk_adapter_runtime": copy.deepcopy(getattr(worker, "pdk_adapter_runtime", {
            "adapter_id": GENERIC_ADAPTER_ID, "used_fallback": False,
            "reason": "generic_first",
        })),
    }
    row["finished_at"] = utc_timestamp()
    timing["status"] = row["status"]
    timing["finished_epoch"] = time.time()
    timing["elapsed_seconds"] = max(0.0, timing["finished_epoch"] - timing["started_epoch"])
    timing["phases"]["maestro"] = {
        "attempts": attempts,
        "elapsed_seconds": timing["elapsed_seconds"],
        "session_start_count": worker.start_count,
    }
    atomic_write_json(os.path.join(item["root"], "timing.json"), timing)
    atomic_write_json(os.path.join(item["root"], "candidate.json"), row)
    return row


def _evaluate_run_persistent_batch(worker, run_root, planned, generation, spec,
                                   prepared, selected, config):
    """Run immutable candidate designs in one native Maestro scheduling pool."""
    from .maestro_batch import MaestroBatchUnsupported, validate_snapshot_batch

    planned = [(int(index), values) for index, values in planned]
    if not planned:
        raise ValueError("persistent candidate batch cannot be empty")
    boundary = getattr(worker, "verify_external_environment_boundary", None)
    if getattr(worker, "_native_stop_requested", False):
        raise InfrastructureFailure("batch stopped before submission")
    if callable(boundary):
        boundary(generation)
    worker.ensure_started()
    if getattr(worker, "_native_stop_requested", False):
        raise InfrastructureFailure("batch stopped during workspace preparation")
    items = [_persistent_running_row(run_root, index, generation, values)
             for index, values in planned]
    root = ensure_dir(os.path.join(run_root, "batches", "g%03d_%s" % (
        generation, items[0]["row"]["candidate_id"],
    )))
    before = summarize_spectre_timing(worker.controller.spectre_log, worker.workspace["root"])
    try:
        batch = worker.controller.evaluate_batch([
            {"candidate_id": item["row"]["candidate_id"], "values": item["row"]["values"],
             "candidate_root": item["root"]} for item in items
        ], root, timeout_seconds=batch_timeout_seconds(len(items), spec, config))
    except MaestroBatchUnsupported as error:
        # No simulation or mutation is permitted before this capability check.
        return {"rows": [], "fallback": str(error), "batch_id": None}
    if batch.get("states", [])[-1:] != ["DONE"]:
        if (batch.get("states", [])[-1:] == ["ERROR"]
                and "PATCHED" not in batch.get("states", [])
                and batch.get("simulation_submitted") is False
                and batch.get("no_remote_jobs_started") is True):
            native_error = str(batch.get('native_error') or '')
            if re.search(r'job policy|Job Setup|Run Options|observer command|effective.*policy', native_error, re.I):
                raise InfrastructureFailure('Native Maestro scheduling configuration was rejected before submission: %s' % native_error[-12000:])
            # One invalid CDF combination must not discard the whole population.
            # A fenced pre-submit ERROR permits the existing serial adapter to
            # classify each identical candidate independently after retirement.
            retirement = worker.retire("native batch preparation failed before submission")
            if (not retirement.get("termination_proven")
                    or not (retirement.get("cleanup") or {}).get("ok")
                    or not (retirement.get("source_guard") or {}).get("ok")):
                raise InfrastructureFailure("batch preparation retirement is unproven: %s" % retirement)
            return {"rows": [], "batch_id": None,
                    "fallback": "native batch preparation failed before submission; serial candidate isolation",
                    "retirement": retirement}
        raise InfrastructureFailure("native Maestro batch did not complete: %s" % batch.get("states"))
    validate_snapshot_batch(batch)
    source_guard = verify_sources_unchanged(worker.workspace)
    if not source_guard.get("ok"):
        raise ExternalEdaEnvironmentChanged("source libraries changed during native candidate batch")
    parsed_batch = parse_rdb(batch["rdb_path"])
    after = summarize_spectre_timing(worker.controller.spectre_log, worker.workspace["root"])
    spectre_summary = _spectre_attempt_summary(before, after)
    artifacts = dict((artifact["candidate_id"], artifact) for artifact in batch["candidates"])
    if set(artifacts) != set(item["row"]["candidate_id"] for item in items):
        raise InfrastructureFailure("native batch candidate identity mismatch")
    distribute_batch_spectre_logs(root, list(artifacts.values()), worker.workspace["root"])
    required_outputs = [definition["output_key"] for definition in
                        spec.get("objectives", []) + spec.get("constraints", [])]
    rows = []
    for item in items:
        row = item["row"]
        artifact = artifacts[row["candidate_id"]]
        if artifact.get("native_storage_paths") is not None:
            row["native_storage_paths"] = copy.deepcopy(artifact["native_storage_paths"])
        mapping = artifact["source_test_map"]
        candidate_spectre = slice_candidate_spectre_timing(spectre_summary, mapping)
        command = {
            "returncode": 0, "timed_out": False, "startup_timed_out": False,
            "execution_simulator": selected_simulator(config),
            "spectre_timing": candidate_spectre,
            "history_name": batch["history_name"],
            "started_epoch": batch.get("started_epoch"),
            "finished_epoch": batch.get("finished_epoch"),
            "native_corner_identity": (artifact.get("verification_workspace") or {}).get("native_corner_identity"),
            "native_storage_paths": artifact.get("native_storage_paths"),
        }
        row["batch_execution"] = {"batch_id": batch["batch_id"],
                                  "source_test_map": mapping, "shared_history": True}
        row["maestro_attempts"] = [command]
        row["rdb_path"] = batch["rdb_path"]
        try:
            patch = validate_candidate_records(artifact)
            verification_workspace = artifact["verification_workspace"]
            row["parameter_application"] = verify_candidate_netlist(
                verification_workspace, artifact, patch_result=patch,
                derived_parameter_names=worker.derived_parameter_cache,
            )
            _derived_parameter_cache(row["parameter_application"], worker.derived_parameter_cache)
            parsed = slice_candidate_rdb(parsed_batch, mapping)
            logical_workspace = dict(verification_workspace)
            logical_setup = copy.deepcopy(verification_workspace["setup"])
            for test in logical_setup.get("tests", []):
                test["name"] = mapping[test["name"]]
            logical_workspace["setup"] = logical_setup
            row["completion_evidence"] = validate_candidate_completion(
                logical_workspace, item["root"], command, parsed, setup=logical_setup,
                required_output_keys=required_outputs, config=config,
            )
            if row.get("native_storage_paths") is not None:
                row["completion_evidence"]["native_storage_paths"] = row["native_storage_paths"]
            row["metrics"] = evaluate_metrics(parsed, spec["objectives"], spec.get("constraints", []))
            row["status"] = "completed"
        except ParameterApplicationInfrastructureFailure as error:
            _persistent_failure(row, spec, error, infrastructure=True, failure_class="result_publication")
            row["failure_stage"] = "parameter_verification"
            row["spectre_state"] = "started"
        except ParameterApplicationFailure as error:
            _persistent_failure(row, spec, error, failure_class="parameter_application")
            row["failure_stage"] = "parameter_verification"
            row["spectre_state"] = "started"
        except Exception as error:
            infrastructure_evidence = _trusted_candidate_infrastructure_evidence(worker, candidate_spectre)
            command["infrastructure_evidence"] = infrastructure_evidence
            _persistent_failure(row, spec, error, infrastructure=bool(infrastructure_evidence),
                                failure_class=("simulation_infrastructure" if infrastructure_evidence
                                               else "design_or_simulation"))
            row["failure_stage"] = "simulation_or_results"
            row["spectre_state"] = "started"
        row["source_guard"] = source_guard
        row["simulation_submission"] = _simulation_submission_metadata(config)
        row["persistent_worker"] = {
            "worker_index": worker.worker_index, "worker_lifetime": "optimization_run",
            "session_start_count": worker.start_count, "epoch": worker.controller.epoch,
            "batch_id": batch["batch_id"], "cleanup_pending": True,
        }
        row["finished_at"] = utc_timestamp()
        timing = item["timing"]
        timing.update({"status": row["status"], "finished_epoch": time.time()})
        timing["elapsed_seconds"] = max(0.0, timing["finished_epoch"] - timing["started_epoch"])
        timing["phases"]["spectre"] = candidate_spectre
        timing["phases"]["maestro"] = {"batch_id": batch["batch_id"],
                                      "elapsed_seconds": batch.get("elapsed_seconds")}
        atomic_write_json(os.path.join(item["root"], "timing.json"), timing)
        atomic_write_json(os.path.join(item["root"], "candidate.json"), row)
        rows.append(row)
    if callable(boundary):
        boundary(generation)
    worker.set_pending(batch)
    return {"rows": rows, "batch_id": batch["batch_id"], "shared_history": True}


def _persistent_controller_failure_candidate(run_root, index, generation, values, spec,
                                             worker_index, error, termination,
                                             failure_class="persistent_controller"):
    item = _persistent_running_row(run_root, index, generation, values)
    row = item["row"]
    _persistent_failure(
        row, spec, error, infrastructure=True, failure_class=failure_class,
    )
    row["traceback"] = ""
    row["persistent_worker"] = {
        "worker_index": int(worker_index),
        "worker_lifetime": "optimization_run",
        "cleanup_pending": False,
        "controller_termination": termination,
    }
    row["finished_at"] = utc_timestamp()
    timing = item["timing"]
    timing["status"] = row["status"]
    timing["finished_epoch"] = time.time()
    timing["elapsed_seconds"] = max(0.0, timing["finished_epoch"] - timing["started_epoch"])
    timing["phases"]["maestro"] = {
        "attempts": [],
        "elapsed_seconds": timing["elapsed_seconds"],
        "controller_termination": termination,
    }
    atomic_write_json(os.path.join(item["root"], "timing.json"), timing)
    atomic_write_json(os.path.join(item["root"], "candidate.json"), row)
    return row


def _terminal_wal_json_exact(left, right):
    try:
        return json.dumps(
            left, sort_keys=True, separators=(",", ":"), allow_nan=False,
        ) == json.dumps(
            right, sort_keys=True, separators=(",", ":"), allow_nan=False,
        )
    except (TypeError, ValueError):
        return False


def _terminal_wal_finite_number(value):
    return (
        isinstance(value, (int, float)) and
        not isinstance(value, bool) and
        math.isfinite(float(value))
    )


def _terminal_wal_metrics_are_valid(metrics, spec, status):
    if not isinstance(metrics, dict):
        return False
    objectives = metrics.get("objectives")
    constraints = metrics.get("constraints")
    objective_definitions = list(spec.get("objectives") or [])
    constraint_definitions = list(spec.get("constraints") or [])
    if not isinstance(objectives, list) or len(objectives) != len(objective_definitions):
        return False
    if not isinstance(constraints, list) or len(constraints) != len(constraint_definitions):
        return False
    if not isinstance(metrics.get("feasible"), bool):
        return False
    if not _terminal_wal_finite_number(metrics.get("total_violation")):
        return False
    if not _terminal_wal_finite_number(metrics.get("merit")):
        return False
    penalized_failure = status == "failed"
    if penalized_failure:
        if metrics.get("penalized") is not True or metrics.get("feasible") is not False:
            return False
    elif metrics.get("penalized") is True:
        return False
    for row, definition in zip(objectives, objective_definitions):
        if not isinstance(row, dict) or row.get("output_key") != definition.get("output_key"):
            return False
        if not _terminal_wal_finite_number(row.get("score")):
            return False
        if penalized_failure:
            if row.get("penalized") is not True or row.get("value") is not None:
                return False
        elif not _terminal_wal_finite_number(row.get("value")):
            return False
    for row, definition in zip(constraints, constraint_definitions):
        if not isinstance(row, dict) or row.get("output_key") != definition.get("output_key"):
            return False
        if not _terminal_wal_finite_number(row.get("violation")):
            return False
        if not isinstance(row.get("satisfied"), bool):
            return False
        if penalized_failure:
            if row.get("penalized") is not True or row.get("value") is not None:
                return False
        elif not _terminal_wal_finite_number(row.get("value")):
            return False
    return True


def _terminal_wal_regular_file(path, direct_parent):
    path = os.path.abspath(path)
    direct_parent = os.path.abspath(direct_parent)
    return bool(
        os.path.dirname(path) == direct_parent and
        is_under(path, direct_parent) and
        not os.path.islink(path) and
        os.path.isfile(path) and
        os.path.getsize(path) > 0
    )


_TERMINAL_WAL_SPECTRE_COMPLETION = re.compile(
    r"spectre\s+completes\s+with\s+0\s+errors?\b",
    re.IGNORECASE,
)
_TERMINAL_WAL_DONE_MESSAGES = set(
    ("done", "completed", "complete", "passed", "pass", "success", "succeeded")
)


def _terminal_wal_nonempty_string(value):
    return isinstance(value, str) and bool(value.strip())


def _terminal_wal_relative_artifact(path):
    if not _terminal_wal_nonempty_string(path):
        return False
    if path != path.strip() or "\\" in path or "\x00" in path or os.path.isabs(path):
        return False
    parts = path.split("/")
    return bool(parts and all(part not in ("", ".", "..") for part in parts))


def _terminal_wal_string_list(value, allow_empty=False):
    return bool(
        isinstance(value, list) and
        (allow_empty or value) and
        all(_terminal_wal_nonempty_string(item) for item in value) and
        value == sorted(set(value))
    )


def _terminal_wal_completion_summary_matches(row, completion):
    bound = False
    inline = row.get("completion_evidence")
    if inline is not None:
        if not isinstance(inline, dict) or not _terminal_wal_json_exact(inline, completion):
            return False
        bound = True
    compact = row.get("completion")
    if compact is not None:
        if not isinstance(compact, dict):
            return False
        expected = {
            "ok": True,
            "adapter": completion["adapter"],
            "history_name": completion["history_name"],
            "tests": list(completion["tests"]),
            "corners": list(completion["corners"]),
            "point_count": len(completion["points"]),
            "output_count": len(completion["outputs"]),
        }
        if not _terminal_wal_json_exact(compact, expected):
            return False
        bound = True
    return bound


def _terminal_wal_completion_evidence_is_valid(completion, row, spec):
    if not isinstance(completion, dict):
        return False
    if completion.get("schema_version") != "crescendo.completion_evidence.v1":
        return False
    if completion.get("adapter") != "maestro-rdb-psf-v1" or completion.get("ok") is not True:
        return False
    if not re.match(
        r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$",
        str(completion.get("validated_at") or ""),
    ):
        return False
    history_name = completion.get("history_name")
    if (
        not _terminal_wal_nonempty_string(history_name) or
        "/" in history_name or "\\" in history_name or history_name in (".", "..")
    ):
        return False
    candidate_history = re.match(r"^CRESCENDO_(g\d{3}_c\d{4})(?:_|$)", history_name)
    if candidate_history and candidate_history.group(1) != row.get("candidate_id"):
        return False

    rdb = completion.get("rdb")
    if not isinstance(rdb, dict) or rdb.get("history_name") != history_name:
        return False
    rdb_path = rdb.get("path")
    if (
        not _terminal_wal_relative_artifact(rdb_path) or
        rdb_path.rsplit("/", 1)[-1] != history_name + ".rdb" or
        not isinstance(rdb.get("size_bytes"), int) or
        isinstance(rdb.get("size_bytes"), bool) or
        rdb.get("size_bytes") <= 0 or
        not _terminal_wal_finite_number(rdb.get("mtime_epoch")) or
        float(rdb.get("mtime_epoch")) <= 0.0
    ):
        return False

    tests = completion.get("tests")
    corners = completion.get("corners")
    if not _terminal_wal_string_list(tests):
        return False
    if not _terminal_wal_string_list(corners, allow_empty=True):
        return False
    physical_map = completion.get("candidate_test_name_map") or {}
    if physical_map and (not isinstance(physical_map, dict)
            or set(physical_map.values()) != set(tests)
            or len(physical_map) != len(tests)
            or any(not _terminal_wal_nonempty_string(name) or "/" in name or "\\" in name
                   or name in (".", "..") for name in physical_map)):
        return False
    physical_by_test = dict((logical, physical) for physical, logical in physical_map.items())
    points = completion.get("points")
    if not isinstance(points, list) or not points:
        return False
    point_map = {}
    observed_tests = set()
    observed_corners = set()
    for point in points:
        if not isinstance(point, dict):
            return False
        test_name = point.get("test")
        if point.get("artifact_test", test_name) != physical_by_test.get(test_name, test_name):
            return False
        point_id = point.get("point_id")
        corner = point.get("corner")
        if test_name not in tests:
            return False
        if not isinstance(point_id, int) or isinstance(point_id, bool) or point_id < 0:
            return False
        if corner is not None and not _terminal_wal_nonempty_string(corner):
            return False
        if str(point.get("status") or "").strip().lower() not in _TERMINAL_WAL_DONE_MESSAGES:
            return False
        status_code = point.get("status_code")
        if not isinstance(status_code, int) or isinstance(status_code, bool):
            return False
        identity = (test_name, point_id)
        if identity in point_map:
            return False
        point_map[identity] = point
        observed_tests.add(test_name)
        if corner is not None:
            observed_corners.add(corner)
    if observed_tests != set(tests) or observed_corners != set(corners):
        return False

    definitions = list(spec.get("objectives") or []) + list(spec.get("constraints") or [])
    expected_outputs = set()
    expected_corners = set()
    for definition in definitions:
        if not isinstance(definition, dict) or not _terminal_wal_nonempty_string(definition.get("output_key")):
            return False
        expected_outputs.add(definition["output_key"])
        selected_corners = definition.get("corners")
        if selected_corners is None:
            continue
        if not isinstance(selected_corners, list):
            return False
        if not all(_terminal_wal_nonempty_string(corner) for corner in selected_corners):
            return False
        expected_corners.update(selected_corners)
    if not expected_corners.issubset(observed_corners):
        return False

    outputs = completion.get("outputs")
    if not isinstance(outputs, list):
        return False
    observed_outputs = set()
    points_per_test = {}
    for test_name, _point_id in point_map:
        points_per_test[test_name] = points_per_test.get(test_name, 0) + 1
    for output in outputs:
        if not isinstance(output, dict):
            return False
        key = output.get("key")
        if not _terminal_wal_nonempty_string(key) or "::" not in key or key in observed_outputs:
            return False
        test_name = key.split("::", 1)[0]
        if test_name not in points_per_test:
            return False
        if not isinstance(output.get("scalar"), bool):
            return False
        point_count = output.get("point_count")
        if (
            not isinstance(point_count, int) or isinstance(point_count, bool) or
            point_count != points_per_test[test_name]
        ):
            return False
        observed_outputs.add(key)
    if observed_outputs != expected_outputs:
        return False

    # Old native WAL grammar only proves Spectre zero-error summaries.
    # ALPS candidates are safely re-evaluated after an uncheckpointed crash.
    if completion.get("execution_simulator", "spectre") != "spectre":
        return False
    spectre_runs = completion.get("spectre_runs")
    if not isinstance(spectre_runs, list) or len(spectre_runs) != len(point_map):
        return False
    observed_runs = set()
    for run in spectre_runs:
        if not isinstance(run, dict):
            return False
        identity = (run.get("test"), run.get("point_id"))
        point = point_map.get(identity)
        if point is None or identity in observed_runs or run.get("corner") != point.get("corner"):
            return False
        psf = run.get("psf")
        sim_done = run.get("sim_done")
        spectre = run.get("spectre")
        artifact_test = physical_by_test.get(identity[0], identity[0])
        if run.get("artifact_test", identity[0]) != artifact_test:
            return False
        suffix = "/%s/%s/psf" % (identity[1], artifact_test)
        if (
            not _terminal_wal_relative_artifact(psf) or
            history_name not in psf.split("/") or
            not psf.endswith(suffix) or
            sim_done != psf + "/.simDone" or
            not _terminal_wal_relative_artifact(sim_done) or
            not isinstance(spectre, dict) or
            spectre.get("path") != psf + "/spectre.out" or
            not _terminal_wal_relative_artifact(spectre.get("path")) or
            not isinstance(spectre.get("error_count"), int) or
            isinstance(spectre.get("error_count"), bool) or
            spectre.get("error_count") != 0 or
            not isinstance(spectre.get("size_bytes"), int) or
            isinstance(spectre.get("size_bytes"), bool) or
            spectre.get("size_bytes") <= 0 or
            not _terminal_wal_nonempty_string(spectre.get("completion")) or
            not _TERMINAL_WAL_SPECTRE_COMPLETION.search(spectre["completion"])
        ):
            return False
        observed_runs.add(identity)
    if observed_runs != set(point_map):
        return False
    return _terminal_wal_completion_summary_matches(row, completion)


def _recover_terminal_candidate_wal(run_root, index, generation, values, spec,
                                    worker_index, termination):
    if not isinstance(termination, dict) or termination.get("termination_proven") is not True:
        return None
    if termination.get("process_still_alive") is not False:
        return None
    run_root = os.path.abspath(run_root)
    candidates_root = os.path.abspath(os.path.join(run_root, "candidates"))
    candidate_id = "g%03d_c%04d" % (generation, index)
    candidate_root = os.path.abspath(os.path.join(candidates_root, candidate_id))
    if (
        os.path.dirname(candidates_root) != run_root or
        os.path.islink(candidates_root) or
        not os.path.isdir(candidates_root) or
        os.path.dirname(candidate_root) != candidates_root or
        os.path.basename(candidate_root) != candidate_id or
        not is_under(candidate_root, candidates_root) or
        os.path.islink(candidate_root) or
        not os.path.isdir(candidate_root)
    ):
        return None
    candidate_path = os.path.join(candidate_root, "candidate.json")
    if not _terminal_wal_regular_file(candidate_path, candidate_root):
        return None
    try:
        row = load_json(candidate_path)
    except Exception:
        return None
    if not isinstance(row, dict) or row.get("status") not in ("completed", "failed"):
        return None
    if row.get("candidate_id") != candidate_id:
        return None
    if not _terminal_wal_json_exact(row.get("generation"), generation):
        return None
    if not _terminal_wal_json_exact(row.get("index"), index):
        return None
    if not isinstance(row.get("values"), dict):
        return None
    if not _terminal_wal_json_exact(row.get("values"), values):
        return None
    source_guard = row.get("source_guard")
    if not isinstance(source_guard, dict) or source_guard.get("ok") is not True:
        return None
    if not _terminal_wal_metrics_are_valid(row.get("metrics"), spec, row["status"]):
        return None
    completion_path = None
    if row["status"] == "completed":
        completion_path = os.path.join(candidate_root, "completion_evidence.json")
        if not _terminal_wal_regular_file(completion_path, candidate_root):
            return None
        try:
            completion = load_json(completion_path)
        except Exception:
            return None
        if not _terminal_wal_completion_evidence_is_valid(completion, row, spec):
            return None
        row["completion_evidence"] = completion
    recovery = {
        "schema_version": "crescendo.terminal_candidate_wal_recovery.v1",
        "recovered": True,
        "candidate_id": candidate_id,
        "terminal_status": row["status"],
        "candidate_record": os.path.relpath(candidate_path, run_root),
        "completion_evidence": (
            os.path.relpath(completion_path, run_root) if completion_path else None
        ),
        "termination_proven": True,
        "recovered_at": utc_timestamp(),
    }
    row["wal_recovery"] = recovery
    row["persistent_worker"] = {
        "worker_index": int(worker_index),
        "worker_lifetime": "optimization_run",
        "cleanup_pending": False,
        "controller_termination": termination,
        "wal_recovery": recovery,
    }
    return row


def _evaluate_persistent_batch(run_root, planned, generation, spec, prepared, selected, config, worker_index=0):
    candidates = [_persistent_running_row(run_root, index, generation, values) for index, values in planned]
    if not candidates:
        return []
    owner = candidates[0]
    workspace = None
    returned = []
    command = None
    batch = None
    try:
        workspace_started = time.monotonic()
        workspace = build_workspace(
            prepared["setup"], prepared["hierarchy"], config,
            os.path.join(owner["root"], "workspace"),
            expected_source_identity=prepared.get("source_identity"),
            parameters=selected,
        )
        build_elapsed = time.monotonic() - workspace_started
        build_timing = workspace.get("build_timing") or {}
        for item in candidates:
            item["timing"]["phases"]["copy"] = {
                "elapsed_seconds": float(build_timing.get("copy_seconds", build_elapsed)),
                "workspace_build_seconds": float(build_timing.get("elapsed_seconds", build_elapsed)),
                "source_fingerprint_seconds": float(build_timing.get("source_fingerprint_seconds", 0.0)),
                "rewrite_seconds": float(build_timing.get("rewrite_seconds", 0.0)),
                "library_count": int(build_timing.get("library_count", 0)),
                "shared_worker": int(worker_index),
            }
        batch = prepare_persistent_batch(
            workspace, config, selected,
            [{
                "candidate_id": item["row"]["candidate_id"], "values": item["row"]["values"],
                "candidate_root": item["root"],
            } for item in candidates],
            os.path.join(owner["root"], "persistent_batch"),
        )
        command = run_persistent_batch(
            workspace, config, batch,
            candidate_timeout_seconds=spec["candidate_timeout_seconds"],
        )
        try:
            validate_batch_clones(batch)
        except ParameterApplicationFailure as error:
            if command.get("timed_out") or command.get("returncode") not in (0, None):
                raise InfrastructureFailure("persistent worker ended before clone verification: %s" % error)
            raise
        artifact_by_id = dict((row["candidate_id"], row) for row in batch["candidates"])
        command_by_id = dict((row["candidate_id"], row) for row in command.get("candidates") or [])
        required_outputs = [
            definition["output_key"]
            for definition in spec.get("objectives", []) + spec.get("constraints", [])
        ]
        derived_cache = {}
        for item in candidates:
            row = item["row"]
            artifact = artifact_by_id[row["candidate_id"]]
            status_rows = command_by_id.get(row["candidate_id"], {}).get("status") or []
            states = [status.get("state") for status in status_rows]
            if states == ["BEGIN", "PATCHED", "DONE"]:
                try:
                    patch_result = validate_candidate_records(artifact)
                    row["parameter_application"] = verify_candidate_netlist(
                        workspace, artifact, patch_result=patch_result,
                        derived_parameter_names=derived_cache,
                    )
                    _derived_parameter_cache(row["parameter_application"], derived_cache)
                    parsed = parse_rdb(artifact["rdb_path"])
                    candidate_command = {
                        "returncode": 0, "timed_out": False, "startup_timed_out": False,
                        "execution_simulator": selected_simulator(config),
                        "spectre_timing": command.get("spectre_timing"),
                        "history_name": artifact["history_name"],
                        "history_identity": artifact["history_identity"],
                        "started_epoch": command.get("started_epoch"),
                        "finished_epoch": command.get("finished_epoch"),
                    }
                    row["maestro_attempts"] = [candidate_command]
                    row["rdb_path"] = parsed["path"]
                    row["completion_evidence"] = validate_candidate_completion(
                        workspace, item["root"], candidate_command, parsed,
                        setup=prepared.get("setup"), required_output_keys=required_outputs, config=config,
                    )
                    row["metrics"] = evaluate_metrics(
                        parsed, spec["objectives"], spec.get("constraints", []),
                    )
                    row["status"] = "completed"
                except ParameterApplicationFailure as error:
                    _persistent_failure(
                        row, spec, error, failure_class="parameter_application",
                    )
                    row["failure_stage"] = "parameter_verification"
                    row["spectre_state"] = "started"
                except Exception as error:
                    _persistent_failure(row, spec, error)
                returned.append(item)
                continue
            if states in (["BEGIN", "ERROR"], ["BEGIN", "PATCHED", "ERROR"]):
                diagnostic = {
                    "ocean.stderr": (command or {}).get("stderr_tail") or "",
                    "ocean.stdout": (command or {}).get("stdout_tail") or "",
                }
                failure_stage, spectre_state = (
                    _persistent_batch_protocol_failure_diagnostics(
                        states, (command or {}).get("spectre_timing") or {},
                        diagnostic, item["root"],
                    )
                )
                diagnostic_reason = _diagnostic_tail_reason(diagnostic)
                error = RuntimeError(
                    "persistent Maestro candidate failed during %s%s" % (
                        failure_stage,
                        ": " + diagnostic_reason if diagnostic_reason else "",
                    )
                )
                _persistent_failure(
                    row, spec, error,
                    failure_class=(
                        "parameter_application"
                        if failure_stage in ("pdk_callback", "parameter_application")
                        else "design_or_simulation"
                    ),
                )
                row["failure_stage"] = failure_stage
                row["spectre_state"] = spectre_state
                returned.append(item)
                break
            if states in (["BEGIN"], ["BEGIN", "PATCHED"]):
                error = InfrastructureFailure(
                    "persistent Maestro worker stopped while candidate was in state %s" % states[-1]
                )
                _persistent_failure(
                    row, spec, error, infrastructure=True, failure_class="infrastructure",
                )
                returned.append(item)
                break
            if states:
                error = InfrastructureFailure(
                    "persistent Maestro status protocol is invalid: %s" % ",".join(str(state) for state in states)
                )
                _persistent_failure(row, spec, error, infrastructure=True)
                returned.append(item)
                break
            if not returned:
                detail = (command.get("stderr_tail") or command.get("stdout_tail") or "")[-2000:]
                error = InfrastructureFailure(
                    "persistent Maestro worker ended before the first candidate started%s" % (
                        ": " + detail if detail else "",
                    )
                )
                _persistent_failure(row, spec, error, infrastructure=True)
                returned.append(item)
            break
    except WorkspaceBuildFailure as error:
        _persistent_failure(
            owner["row"], spec, error, infrastructure=True,
            failure_class="workspace_build",
        )
        owner["row"]["failure_stage"] = "workspace_build"
        owner["row"]["spectre_state"] = "not_started"
        returned = [owner]
    except ParameterApplicationFailure as error:
        _persistent_failure(owner["row"], spec, error, failure_class="parameter_application")
        returned = [owner]
        owner["row"]["failure_stage"] = "parameter_application"
        owner["row"]["spectre_state"] = "not_started"
    except Exception as error:
        any_candidate_started = bool(batch and any(
            read_candidate_status(artifact) for artifact in batch.get("candidates") or []
        ))
        _persistent_failure(
            owner["row"], spec, error, infrastructure=True,
            failure_class="infrastructure" if any_candidate_started else "persistent_startup",
        )
        returned = [owner]
    finally:
        source_guard = None
        if workspace is not None:
            try:
                source_guard = verify_sources_unchanged(workspace)
            except Exception as error:
                source_guard = {"ok": False, "error": "%s: %s" % (error.__class__.__name__, error)}
        if source_guard is None:
            source_guard = {"ok": True, "libraries": [], "workspace_created": False}
        spectre_timing = (command or {}).get("spectre_timing") or {}
        batch_elapsed = float((command or {}).get("elapsed_seconds", 0.0))
        _discard_unreturned_candidate_roots(candidates, returned, run_root, spec)
        for item in returned:
            row = item["row"]
            timing = item["timing"]
            row["source_guard"] = source_guard
            row["simulation_submission"] = _simulation_submission_metadata(config)
            if not source_guard.get("ok"):
                error = RuntimeError("source library fingerprint changed or could not be verified")
                _persistent_failure(row, spec, error, infrastructure=True, failure_class="source_integrity")
            row["persistent_worker"] = {
                "worker_index": int(worker_index),
                "batch_candidate_count": len(candidates),
                "batch_elapsed_seconds": batch_elapsed,
                "command_returncode": (command or {}).get("returncode"),
                "timed_out": bool((command or {}).get("timed_out")),
                "progress_timed_out": bool((command or {}).get("progress_timed_out")),
            }
            row["finished_at"] = utc_timestamp()
            timing["status"] = row["status"]
            timing["finished_epoch"] = time.time()
            timing["elapsed_seconds"] = max(0.0, timing["finished_epoch"] - timing["started_epoch"])
            timing["phases"]["maestro"] = {
                "batch_elapsed_seconds": batch_elapsed,
                "lock_wait_seconds": float((command or {}).get("lock_wait_seconds", 0.0)),
                "shared_worker": int(worker_index),
            }
            timing["phases"]["spectre"] = spectre_timing
            timing["phases"]["lock"]["maestro_seconds"] = float((command or {}).get("lock_wait_seconds", 0.0))
            timing["phases"]["lock"]["total_seconds"] = timing["phases"]["lock"]["maestro_seconds"]
            timing["phases"]["active"]["maestro_seconds"] = max(
                0.0, batch_elapsed - timing["phases"]["lock"]["maestro_seconds"],
            )
            timing["phases"]["active"]["total_seconds"] = timing["phases"]["active"]["maestro_seconds"]
            atomic_write_json(os.path.join(item["root"], "timing.json"), timing)
            atomic_write_json(os.path.join(item["root"], "candidate.json"), row)
    return [item["row"] for item in returned]


def _automatic_population_size(variable_count):
    return max(20, min(100, int(round(5.0 * math.sqrt(float(variable_count))))))


def _infrastructure_failure_summary(rows):
    counts = {}
    for row in rows:
        key = "%s: %s" % (row.get("failure_class") or "infrastructure", row.get("error") or "unknown error")
        counts[key] = counts.get(key, 0) + 1
    ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:3]
    return "; ".join("%dx %s" % (count, message[:300]) for message, count in ranked)


def _workspace_build_failure_row(row):
    error = str(row.get("error") or "")
    return bool(
        row.get("failure_class") == "workspace_build"
        or row.get("failure_stage") == "workspace_build"
        or "WorkspaceBuildFailure" in error
        or "workspace build failed" in error.lower()
    )


def _raise_workspace_build_failure(generation, rows):
    failures = [row for row in rows if _workspace_build_failure_row(row)]
    if failures:
        raise RuntimeError(
            "private workspace build failed in generation %d; optimization stopped "
            "after the first observed failure, before Cadence or Spectre startup for "
            "that candidate and before submitting replacements or further queued "
            "work. Check source file readability for the optimizer OS user, then "
            "reload the setup: %s" % (
                generation + 1, _infrastructure_failure_summary(failures),
            )
        )


def _raise_source_integrity_failure(generation, rows):
    failures = [row for row in rows if row.get("failure_class") == "source_integrity"]
    if failures:
        raise RuntimeError(
            "source integrity failure in generation %d; optimization stopped before any "
            "pending candidates, replacements, or optimizer updates: %s" % (
                generation + 1, _infrastructure_failure_summary(failures),
            )
        )


def _raise_parameter_application_failure(generation, rows):
    failures = [row for row in rows if row.get("failure_class") == "parameter_application"]
    if failures:
        raise RuntimeError(
            "parameter range/application failure in generation %d; optimization stopped after "
            "the first PDK-adjusted candidate to avoid wasting simulations. Run Validate All "
            "Ranges and apply the reported legal bounds before starting again: %s" % (
                generation + 1, _infrastructure_failure_summary(failures),
            )
        )


def _raise_repeated_persistent_startup_failure(generation, rows, streak, limit):
    failures = [row for row in rows if row.get("failure_class") == "persistent_startup"]
    if failures and streak >= limit:
        raise RuntimeError(
            "persistent Maestro startup failed %d consecutive time(s) in generation %d; "
            "optimization stopped before retrying the whole population: %s" % (
                streak, generation + 1, _infrastructure_failure_summary(failures),
            )
        )


def _algorithm_defaults(algorithm, variable_count):
    variable_count = max(1, int(variable_count))
    population = _automatic_population_size(variable_count)
    common = {"population": population, "seed": 1}
    if algorithm == "nsga2":
        common.update({
            "crossover_probability": 0.9,
            "crossover_eta": round(15.0 + math.sqrt(variable_count), 3),
            "mutation_probability": 1.0 / float(variable_count),
            "mutation_eta": round(20.0 + 0.5 * math.sqrt(variable_count), 3),
        })
    elif algorithm == "differential_evolution":
        common.update({
            "differential_weight": round(max(0.5, 0.9 - 0.005 * variable_count), 3),
            "crossover_rate": round(min(0.95, 0.7 + 0.005 * variable_count), 3),
        })
    elif algorithm == "bayesian_optimization":
        common.update({
            "initial_samples": max(population, 2 * variable_count + 1),
            "acquisition_exploration": round(max(1.0, 2.5 - 0.02 * variable_count), 3),
            "candidate_pool": max(200, 10 * variable_count),
            "noise": 1.0e-6,
            "kernel_length_scale": round(max(0.08, 1.0 / math.sqrt(variable_count)), 6),
            "max_observations": max(80, 2 * variable_count + 1),
        })
    return common


def _environment_test_run(spec):
    """Recognize only the explicitly bounded diagnostic run contract.

    This flag does not relax source, parameter, result or cleanup validation.
    It prevents replacement sampling outside the shared bounded diagnostic plan.
    """
    if "environment_test" not in spec:
        return False
    marker = spec["environment_test"]
    if (not isinstance(marker, dict) or
            not {"schema_version", "test_id"}.issubset(marker) or
            set(marker) - {"schema_version", "test_id", "simulator", "frozen_candidate", "frozen_candidate_sha256", "frozen_plan", "frozen_plan_sha256"} or
            marker.get("schema_version") != "crescendo.environment_test.run.v1"):
        raise ValueError("environment_test must use crescendo.environment_test.run.v1")
    if "simulator" in marker and (marker["simulator"] not in ("spectre", "alps") or
            ("simulator" in spec and marker["simulator"] != spec["simulator"])):
        raise ValueError("environment_test simulator identity disagrees with the selected solver")
    test_id = marker.get("test_id")
    if (not isinstance(test_id, str) or not test_id.strip() or
            len(test_id) > 128 or any(ord(char) < 32 for char in test_id)):
        raise ValueError("environment_test test_id must be a nonempty bounded string")
    if 'frozen_candidate' in marker and (not isinstance(marker['frozen_candidate'], dict) or not marker['frozen_candidate']):
        raise ValueError('environment_test frozen_candidate must be a nonempty parameter mapping')
    if 'frozen_candidate_sha256' in marker and 'frozen_candidate' not in marker:
        raise ValueError('environment_test candidate digest requires a frozen candidate')
    parameters = spec.get("algorithm_parameters") or {}
    if not isinstance(parameters, dict):
        raise ValueError("environment_test algorithm_parameters must be an object")
    populations = [parameters.get("population", spec.get("population"))]
    if "population" in parameters and "population" in spec:
        populations.append(spec["population"])
    generations = spec.get("generations")
    if (spec.get("algorithm") != "random" or type(generations) is not int or
            not 1 <= generations <= 10 or
            any(type(value) is not int or not 1 <= value <= 16 for value in populations) or
            len(set(populations)) != 1 or generations * populations[0] > 64):
        raise ValueError("environment_test requires random and a bounded diagnostic shape")
    if generations * populations[0] > 1 and 'frozen_plan' not in marker:
        raise ValueError("multi-candidate environment_test requires a frozen plan")
    if 'frozen_plan_sha256' in marker and 'frozen_plan' not in marker:
        raise ValueError("environment_test plan digest requires a frozen plan")
    if str(spec.get("evaluator", "real")).strip().lower() != "real":
        raise ValueError("environment_test requires the real execution chain")
    return True


def _frozen_environment_candidate(spec, variables):
    """Freeze diagnostic values without relaxing the normal parameter contract."""
    if not _environment_test_run(spec):
        return None
    marker = spec['environment_test']
    if 'frozen_candidate' not in marker:
        return None  # Historical single-case diagnostic callers remain valid.
    values = validate_candidate_values(marker['frozen_candidate'], variables)
    if any(isinstance(value, bool) for value in marker['frozen_candidate'].values()):
        raise ValueError('environment_test candidate values cannot be booleans')
    digest = hashlib.sha256(json.dumps(values, sort_keys=True, separators=(',', ':'),
                                      ensure_ascii=True, allow_nan=False).encode('utf-8')).hexdigest()
    if marker.get('frozen_candidate_sha256') not in (None, digest):
        raise ValueError('environment_test frozen candidate digest does not match validated values')
    marker['frozen_candidate'] = copy.deepcopy(values)
    marker['frozen_candidate_sha256'] = digest
    return values


def _frozen_environment_plan(spec, variables):
    if not _environment_test_run(spec) or 'frozen_plan' not in spec['environment_test']:
        return None
    from .diagnostic_plan import validate_plan, digest
    marker = spec['environment_test']
    plan = validate_plan(marker['frozen_plan'], variables, spec['generations'], spec['population'])
    actual_digest = digest(plan)
    if marker.get('frozen_plan_sha256') not in (None, actual_digest):
        raise ValueError('environment_test frozen plan digest does not match validated values')
    if marker.get('frozen_candidate') is not None and plan[0]['values'] != marker['frozen_candidate']:
        raise ValueError('environment_test first planned candidate differs from frozen baseline')
    marker['frozen_plan'] = copy.deepcopy(plan)
    marker['frozen_plan_sha256'] = actual_digest
    return plan


def _normalize_algorithm_settings(spec, variable_count, objective_count):
    _environment_test_run(spec)
    aliases = {
        "nsga-ii": "nsga2", "nsga_ii": "nsga2", "differential": "differential_evolution",
        "de": "differential_evolution", "bayesian": "bayesian_optimization",
    }
    requested = spec.get("algorithm")
    if requested == "random" and spec.get("evaluator") == "mock":
        algorithm = "random"
        policy = "mock_random_baseline"
    elif requested:
        algorithm = aliases.get(str(requested).lower(), str(requested).lower())
        policy = "explicit_user_selection"
    else:
        algorithm = "differential_evolution" if objective_count == 1 else "nsga2"
        policy = "automatic_by_objective_count"
    if algorithm not in ("random", "nsga2", "differential_evolution", "bayesian_optimization"):
        raise ValueError("unsupported optimization algorithm: %s" % requested)
    if objective_count > 1 and algorithm in ("differential_evolution", "bayesian_optimization"):
        raise ValueError("%s supports one objective only; use NSGA-II for multi-objective optimization" % algorithm)
    defaults = _algorithm_defaults(algorithm, variable_count)
    supplied = dict(spec.get("algorithm_parameters") or {})
    for key in defaults:
        if key in spec:
            supplied.setdefault(key, spec[key])
    unknown = sorted(set(supplied) - set(defaults))
    if unknown:
        raise ValueError("unsupported %s parameter(s): %s" % (algorithm, ", ".join(unknown)))
    settings = dict(defaults)
    settings.update(supplied)
    settings["population"] = int(settings.get("population", defaults.get("population", 1)))
    settings["seed"] = int(settings.get("seed", defaults.get("seed", 1)))
    if settings["population"] < 1:
        raise ValueError("population must be at least 1")
    if algorithm != "random" and settings["population"] < 4:
        raise ValueError("population must be at least 4 for %s optimization" % algorithm)
    if algorithm == "nsga2":
        for key in ("crossover_probability", "mutation_probability"):
            settings[key] = float(settings[key])
            if not 0.0 <= settings[key] <= 1.0:
                raise ValueError("%s must be between 0 and 1" % key)
        for key in ("crossover_eta", "mutation_eta"):
            settings[key] = float(settings[key])
            if settings[key] <= 0.0:
                raise ValueError("%s must be positive" % key)
    elif algorithm == "differential_evolution":
        settings["differential_weight"] = float(settings["differential_weight"])
        settings["crossover_rate"] = float(settings["crossover_rate"])
        if not 0.0 < settings["differential_weight"] <= 2.0:
            raise ValueError("differential_weight must be in (0, 2]")
        if not 0.0 <= settings["crossover_rate"] <= 1.0:
            raise ValueError("crossover_rate must be between 0 and 1")
    elif algorithm == "bayesian_optimization":
        settings["initial_samples"] = int(settings["initial_samples"])
        settings["candidate_pool"] = int(settings["candidate_pool"])
        settings["max_observations"] = int(settings["max_observations"])
        settings["acquisition_exploration"] = float(settings["acquisition_exploration"])
        settings["noise"] = float(settings["noise"])
        settings["kernel_length_scale"] = float(settings["kernel_length_scale"])
        if settings["initial_samples"] < settings["population"]:
            raise ValueError("initial_samples must be at least the Bayesian batch population")
        if settings["candidate_pool"] < settings["population"]:
            raise ValueError("candidate_pool must be at least the Bayesian batch population")
        if settings["max_observations"] < settings["initial_samples"]:
            raise ValueError("max_observations must be at least initial_samples")
        if settings["acquisition_exploration"] < 0.0:
            raise ValueError("acquisition_exploration must be non-negative")
        if settings["noise"] <= 0.0:
            raise ValueError("noise must be positive")
        if settings["kernel_length_scale"] <= 0.0:
            raise ValueError("kernel_length_scale must be positive")
    return algorithm, policy, settings


def resolve_metric_bindings(prepared, objectives, constraints):
    """Report objective/constraint bindings missing from a prepared setup."""
    definitions = list(objectives or []) + list(constraints or [])
    output_keys = set(
        row.get("key") for row in prepared.get("outputs", [])
        if isinstance(row, dict) and isinstance(row.get("key"), str)
    )
    corner_names = set(
        row.get("name") for row in prepared.get("setup", {}).get("corners", [])
        if isinstance(row, dict) and isinstance(row.get("name"), str)
    )
    referenced_outputs = set()
    referenced_corners = set()
    for row in definitions:
        if not isinstance(row, dict):
            raise ValueError("objective and constraint definitions must be objects")
        output_key = row.get("output_key")
        if not isinstance(output_key, str) or not output_key.strip():
            raise ValueError("objective and constraint output_key must be a non-empty string")
        corners = row.get("corners") or []
        if isinstance(corners, str) or not isinstance(corners, (list, tuple)):
            raise ValueError("objective and constraint corners must be an array of names")
        if any(not isinstance(corner, str) or not corner.strip() for corner in corners):
            raise ValueError("objective and constraint corner names must be non-empty strings")
        referenced_outputs.add(output_key)
        referenced_corners.update(corners)
    return {
        "outputs": sorted(referenced_outputs - output_keys),
        "corners": sorted(referenced_corners - corner_names),
    }


def _optional_candidate_timeout(value):
    if value is None or (isinstance(value, str) and not value.strip()):
        return -1
    if isinstance(value, bool):
        raise ValueError("Candidate/measurement timeout must be positive or blank; 0 is not allowed")
    numeric = float(value)
    if not math.isfinite(numeric) or (numeric <= 0 and numeric != -1):
        raise ValueError("Candidate/measurement timeout must be positive or blank; 0 is not allowed")
    return numeric


def resolve_design_config(document, prepared):
    if not isinstance(document, dict) or document.get("schema_version") != "crescendo.design_config.v1":
        raise ValueError("unsupported design config; expected crescendo.design_config.v1")
    optimization = document.get("optimization") or {}
    variables = list(optimization.get("variables") or [])
    objectives = list(optimization.get("objectives") or [])
    constraints = list(optimization.get("constraints") or [])
    run_settings = dict(optimization.get("run") or {})
    parameter_ids = set(
        row.get("parameter_id") for row in prepared.get("parameters", []) if row.get("optimizable")
    )
    missing_parameters = sorted(set(
        row.get("parameter_id") for row in variables if row.get("parameter_id") not in parameter_ids
    ))
    metric_missing = resolve_metric_bindings(prepared, objectives, constraints)
    missing = {
        "parameters": missing_parameters,
        "outputs": metric_missing["outputs"],
        "corners": metric_missing["corners"],
    }
    spec = {
        "evaluator": "real",
        "generations": int(run_settings.get("generations", 20)),
        "jobs": int(run_settings.get("jobs", 1)),
        "candidate_timeout_seconds": _optional_candidate_timeout(run_settings.get("candidate_timeout_seconds", 600)),
        "algorithm": run_settings.get("algorithm"),
        "algorithm_parameters": dict(run_settings.get("algorithm_parameters") or {}),
        "variables": variables,
        "objectives": objectives,
        "constraints": constraints,
    }
    for field in ("maestro_job_overrides", "maestro_candidate_mode", "execution_backend", "simulator", "execution_commands", "command_scheduler"):
        if field in run_settings:
            spec[field] = copy.deepcopy(run_settings[field])
    return {"compatible": not any(missing.values()), "missing": missing, "spec": spec}


def _nested_tuple(value):
    if isinstance(value, list):
        return tuple(_nested_tuple(item) for item in value)
    return value


def _pack_metrics(metrics):
    if not metrics:
        return None
    return [
        metrics.get("feasible"), metrics.get("total_violation"), metrics.get("merit"),
        bool(metrics.get("penalized")), str(metrics.get("failure_reason") or "")[:2048],
        [[row.get("value"), row.get("score"), bool(row.get("penalized"))]
         for row in metrics.get("objectives", [])],
        [[row.get("value"), row.get("violation"), row.get("satisfied"), bool(row.get("penalized"))]
         for row in metrics.get("constraints", [])],
    ]


def _unpack_metrics(packed, spec):
    if packed is None:
        return None
    if not isinstance(packed, (list, tuple)) or len(packed) != 7:
        raise ValueError("checkpoint candidate metrics payload is malformed")
    objective_definitions = spec.get("objectives", [])
    constraint_definitions = spec.get("constraints", [])
    if not isinstance(objective_definitions, list) or not isinstance(constraint_definitions, list):
        raise ValueError("checkpoint metric definitions are malformed")
    if not isinstance(packed[5], (list, tuple)) or not isinstance(packed[6], (list, tuple)):
        raise ValueError("checkpoint candidate metric rows are malformed")
    objectives = []
    for index, values in enumerate(packed[5]):
        if not isinstance(values, (list, tuple)) or len(values) != 3:
            raise ValueError("checkpoint candidate objective payload is malformed")
        definition = objective_definitions[index] if index < len(objective_definitions) else {}
        if not isinstance(definition, dict):
            raise ValueError("checkpoint objective definition is malformed")
        objectives.append({
            "name": definition.get("name") or definition.get("output_key"),
            "output_key": definition.get("output_key"),
            "value": values[0], "score": values[1], "penalized": values[2],
            "direction": definition.get("direction", "minimize"),
            "corners": list(definition.get("corners") or []),
        })
    constraints = []
    for index, values in enumerate(packed[6]):
        if not isinstance(values, (list, tuple)) or len(values) != 4:
            raise ValueError("checkpoint candidate constraint payload is malformed")
        definition = constraint_definitions[index] if index < len(constraint_definitions) else {}
        if not isinstance(definition, dict):
            raise ValueError("checkpoint constraint definition is malformed")
        constraints.append({
            "name": definition.get("name") or definition.get("output_key"),
            "output_key": definition.get("output_key"),
            "value": values[0], "violation": values[1], "satisfied": values[2],
            "penalized": values[3], "corners": list(definition.get("corners") or []),
        })
    return {
        "feasible": packed[0], "total_violation": packed[1], "merit": packed[2],
        "penalized": packed[3], "failure_reason": packed[4],
        "objectives": objectives, "constraints": constraints,
    }


_CANDIDATE_LAYOUT_BASE = (
    "id", "generation", "index", "status", "values", "metrics",
    "failure_class", "error", "started_at", "finished_at",
)
_CANDIDATE_LAYOUT_DIAGNOSTICS = (
    _CANDIDATE_LAYOUT_BASE + ("failure_stage", "spectre_state")
)


def _checkpoint_candidate_layout(checkpoint_or_layout=None):
    if isinstance(checkpoint_or_layout, dict):
        if "candidate_layout" not in checkpoint_or_layout:
            raise ValueError("checkpoint candidate_layout is missing")
        layout = checkpoint_or_layout.get("candidate_layout")
        if not isinstance(layout, list):
            raise ValueError("checkpoint candidate_layout is malformed")
    else:
        layout = checkpoint_or_layout
        if layout is None:
            return list(_CANDIDATE_LAYOUT_DIAGNOSTICS)
    if tuple(layout) == _CANDIDATE_LAYOUT_BASE:
        return list(_CANDIDATE_LAYOUT_BASE)
    if tuple(layout) == _CANDIDATE_LAYOUT_DIAGNOSTICS:
        return list(_CANDIDATE_LAYOUT_DIAGNOSTICS)
    raise ValueError("checkpoint candidate_layout is not supported")


def _pack_candidate(row, variable_ids, candidate_layout=None):
    packed = [
        row.get("candidate_id"), int(row.get("generation", 0)), int(row.get("index", 0)),
        row.get("status"), [row.get("values", {}).get(parameter_id) for parameter_id in variable_ids],
        _pack_metrics(row.get("metrics")), row.get("failure_class"), str(row.get("error") or "")[:2048],
        row.get("started_at"), row.get("finished_at"),
    ]
    if tuple(_checkpoint_candidate_layout(candidate_layout)) == _CANDIDATE_LAYOUT_DIAGNOSTICS:
        packed.extend((row.get("failure_stage"), row.get("spectre_state")))
    return packed


def _unpack_candidate(packed, variable_ids, spec):
    if not isinstance(packed, (list, tuple)) or len(packed) not in (10, 12):
        raise ValueError("checkpoint candidate payload is malformed")
    if not isinstance(packed[0], str):
        raise ValueError("checkpoint candidate id is malformed")
    for field, value in (("generation", packed[1]), ("index", packed[2])):
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError("checkpoint candidate %s is malformed" % field)
    if packed[3] not in ("completed", "failed", "infrastructure_failed"):
        raise ValueError("checkpoint candidate status is malformed")
    if not isinstance(packed[4], (list, tuple)) or len(packed[4]) != len(variable_ids):
        raise ValueError("checkpoint candidate parameter payload is malformed")
    row = {
        "candidate_id": packed[0], "generation": packed[1], "index": packed[2], "status": packed[3],
        "values": dict(zip(variable_ids, packed[4])),
    }
    metrics = _unpack_metrics(packed[5], spec)
    if metrics is not None:
        row["metrics"] = metrics
    for key, value in zip(("failure_class", "error", "started_at", "finished_at"), packed[6:10]):
        if value:
            row[key] = value
    if len(packed) == 12:
        for key, value in zip(("failure_stage", "spectre_state"), packed[10:12]):
            if value:
                row[key] = value
    return row


def checkpoint_candidates(checkpoint):
    if not isinstance(checkpoint, dict) or checkpoint.get("schema_version") != "crescendo.checkpoint.v1":
        raise ValueError("unsupported checkpoint; expected crescendo.checkpoint.v1")
    spec = checkpoint.get("spec", {})
    parameter_ids = checkpoint.get("parameter_ids", [])
    packed_candidates = checkpoint.get("candidates", [])
    if spec is None:
        spec = {}
    if parameter_ids is None:
        parameter_ids = []
    if packed_candidates is None:
        packed_candidates = []
    if not isinstance(spec, dict):
        raise ValueError("checkpoint spec is malformed")
    if not isinstance(parameter_ids, list):
        raise ValueError("checkpoint parameter_ids is malformed")
    if not isinstance(packed_candidates, list):
        raise ValueError("checkpoint candidates payload is malformed")
    variable_ids = list(parameter_ids)
    reference = checkpoint.get("candidate_store")
    if reference is not None:
        if packed_candidates:
            raise ValueError("checkpoint has conflicting candidate history sources")
        try:
            dataset = dataset_from_reference(reference, checkpoint.get("candidate_store_embedded"))
        except Exception as error:
            raise ValueError("checkpoint candidate history is unavailable or invalid: %s" % error)
        metadata_spec = (dataset.get("metadata") or {}).get("spec") or {}
        if metadata_spec.get("variables") != spec.get("variables"):
            raise ValueError("checkpoint candidate history parameter schema differs")
        rows = sorted((row for row in dataset["candidates"] if row.get("status") in DATA_TERMINAL_STATES),
                      key=lambda row: (row["generation"], row["index"]))
        # Preserve the existing optimizer/checkpoint interpretation while the
        # display dataset retains complete effective values and native proofs.
        return [_unpack_candidate(_pack_candidate(row, variable_ids,
                                _checkpoint_candidate_layout(checkpoint)), variable_ids, spec)
                for row in rows]
    rows = []
    for position, packed in enumerate(packed_candidates):
        try:
            rows.append(_unpack_candidate(packed, variable_ids, spec))
        except ValueError as error:
            raise ValueError("checkpoint candidate %d is malformed: %s" % (position, error))
        except (IndexError, KeyError, TypeError, AttributeError, OverflowError) as error:
            raise ValueError(
                "checkpoint candidate %d is malformed: %s" % (position, error.__class__.__name__)
            )
    return rows


def checkpoint_with_candidates(checkpoint, rows):
    if not isinstance(checkpoint, dict):
        raise ValueError("unsupported checkpoint; expected crescendo.checkpoint.v1")
    if not isinstance(rows, (list, tuple)):
        raise ValueError("checkpoint candidate rows must be a list")
    if any(not isinstance(row, dict) for row in rows):
        raise ValueError("checkpoint candidate rows are malformed")
    document = copy.deepcopy(checkpoint)
    document.pop("candidate_store", None)
    document.pop("candidate_store_embedded", None)
    candidate_layout = _checkpoint_candidate_layout(document)
    document["candidate_layout"] = candidate_layout
    variable_ids = list(document.get("parameter_ids") or [])
    terminal = ("completed", "failed", "infrastructure_failed")
    try:
        terminal_rows = sorted(
            (row for row in rows if row.get("status") in terminal),
            key=lambda row: (
                int(row.get("generation", -1)), int(row.get("index", -1)),
                str(row.get("candidate_id") or ""),
            ),
        )
        document["candidates"] = [
            _pack_candidate(row, variable_ids, candidate_layout)
            for row in terminal_rows
        ]
    except (TypeError, ValueError, OverflowError) as error:
        raise ValueError("checkpoint candidate rows are malformed: %s" % error)
    state = dict(document.get("state") or {})
    state["completed_candidates"] = len([
        row for row in terminal_rows if row.get("status") == "completed"
    ])
    state["failed_candidates"] = len([
        row for row in terminal_rows if row.get("status") == "failed"
    ])
    state["infrastructure_failed_candidates"] = len([
        row for row in terminal_rows if row.get("status") == "infrastructure_failed"
    ])
    state["checkpoint_revision"] = _checkpoint_nonnegative_integer(
        state.get("checkpoint_revision", 0), "revision",
    ) + 1
    ancestry = list(state.get("ancestor_payload_sha256") or [])
    parent_checksum = checkpoint.get("payload_sha256")
    if parent_checksum and (not ancestry or ancestry[-1] != parent_checksum):
        ancestry.append(parent_checksum)
    state["ancestor_payload_sha256"] = ancestry
    document["state"] = state
    document["created_at"] = utc_timestamp()
    document["payload_sha256"] = _checkpoint_checksum(document)
    return document


def self_contained_checkpoint(checkpoint):
    """Export one portable file without changing origin checksum or ancestry."""
    validate_checkpoint_integrity(checkpoint)
    document = copy.deepcopy(checkpoint)
    reference = document.get("candidate_store")
    if reference is not None and "candidate_store_embedded" not in document:
        from .run_data_store import _open_read
        with _open_read(reference["path"]) as stream:
            raw = stream.read(reference["bytes"])
        if len(raw) != reference["bytes"] or hashlib.sha256(raw).hexdigest() != reference["prefix_sha256"]:
            raise ValueError("checkpoint history changed during portable export")
        document["candidate_store_embedded"] = raw.decode("utf-8")
    validate_checkpoint_integrity(document)
    return document


def _seed_checkpoint_candidates(run_root, rows):
    for row in rows:
        if row.get("status") not in ("completed", "failed", "infrastructure_failed"):
            continue
        candidate_id = row.get("candidate_id")
        if not candidate_id or candidate_id in (".", "..") or os.path.basename(candidate_id) != candidate_id:
            raise ValueError("checkpoint contains an invalid candidate id: %r" % candidate_id)
        candidate_root = ensure_dir(os.path.join(run_root, "candidates", candidate_id))
        persist_lightweight_candidate(candidate_root, row)


def _generator_state(generator):
    return {
        "rng_state": generator.rng.getstate(),
        "population_size": generator.population_size,
        "previous": [row.get("candidate_id") for row in generator.previous],
        "de_targets": [
            [list(key), [row.get("candidate_id") for row in rows]]
            for key, rows in generator._de_targets.items()
        ],
    }


def _restore_generator(generator, state, rows):
    by_id = dict((row.get("candidate_id"), row) for row in rows)
    generator.rng.setstate(_nested_tuple(state["rng_state"]))
    generator.population_size = state.get("population_size")
    generator.previous = [by_id[candidate_id] for candidate_id in state.get("previous", []) if candidate_id in by_id]
    generator._de_targets = {}
    for key, target_ids in state.get("de_targets", []):
        generator._de_targets[tuple(key)] = [by_id[candidate_id] for candidate_id in target_ids if candidate_id in by_id]


def _pack_plan(plan, variable_ids):
    if plan is None:
        return None
    return {
        "generation": plan["generation"], "next_index": plan["next_index"],
        "replacements": plan["replacements"],
        "planned": [[index, [values[parameter_id] for parameter_id in variable_ids]]
                    for index, values in plan["planned"]],
    }


def _unpack_plan(plan, variable_ids):
    if plan is None:
        return None
    return {
        "generation": int(plan["generation"]), "next_index": int(plan["next_index"]),
        "replacements": int(plan.get("replacements", 0)),
        "planned": [(int(row[0]), dict(zip(variable_ids, row[1]))) for row in plan.get("planned", [])],
    }


def _prepared_schema_fingerprint_v1(prepared):
    setup = prepared.get("setup") or {}
    try:
        setup_reference = prepared_setup_reference(prepared=prepared)
    except ValueError:
        setup_reference = {"path": prepared.get("maestro_path") or "", "backend": "maestro"}
    payload = {
        "maestro_path": os.path.realpath(setup_reference.get("path") or ""),
        "tests": [{
            "name": row.get("name"), "library": row.get("library"),
            "cell": row.get("cell"), "enabled": row.get("enabled", True),
            "outputs": [{"name": output.get("name"), "expression": output.get("expression")}
                        for output in row.get("outputs", [])],
        } for row in setup.get("tests", [])],
        "corners": [{"name": row.get("name"), "enabled": row.get("enabled", True)}
                    for row in setup.get("corners", [])],
        "parameters": [{
            "parameter_id": row.get("parameter_id"), "hierarchy_path": row.get("hierarchy_path"),
            "name": row.get("name"), "master_library": row.get("master_library"),
            "master_cell": row.get("master_cell"),
        } for row in prepared.get("parameters", []) if row.get("optimizable")],
    }
    encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _sorted_semantic_rows(rows):
    return sorted(
        rows,
        key=lambda row: json.dumps(row, separators=(",", ":"), sort_keys=True),
    )


def _design_identity(value):
    if isinstance(value, dict):
        return dict((key, value.get(key)) for key in ("library", "cell", "view"))
    values = list(value or []) + [None, None, None]
    return {"library": values[0], "cell": values[1], "view": values[2]}


def configuration_binding_identity(hierarchy):
    """Small, stable identity of every resolved config occurrence, including unselected ones."""
    designs = []
    fields = (
        "hierarchy_path", "name", "parent_path", "master_library", "master_cell", "master_view",
        "owner_library", "owner_cell", "owner_view", "bound_library", "bound_cell", "bound_view",
        "resolved_library", "resolved_cell", "resolved_view", "resolved_view_type", "stop",
        "fixed_library", "fixed_cell", "fixed_view", "config_context", "config_context_path",
        "config_local_path", "netlister_ignored", "scan_traversable", "symbol_masked",
    )
    for design in (hierarchy or {}).get("designs") or []:
        body = design.get("hierarchy") or {}
        top = design.get("resolved_top") or body.get("resolved_top")
        if top is None:
            continue
        dependencies = design.get("config_dependencies") or body.get("config_dependencies") or []
        bindings = design.get("config_bindings") or body.get("config_bindings") or []
        designs.append({
            "design": _design_identity(design.get("design")),
            "resolved_top": _design_identity(top),
            "config_dependencies": _sorted_semantic_rows([_design_identity(row) for row in dependencies]),
            "config_bindings": _sorted_semantic_rows([
                dict((field, row.get(field)) for field in fields) for row in bindings
            ]),
        })
    if not designs:
        return None
    payload = _sorted_semantic_rows(designs)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return {"schema_version": "crescendo.configuration_bindings.v1", "sha256": hashlib.sha256(encoded).hexdigest()}


def _prepared_schema_fingerprint_v2(prepared):
    setup = prepared.get("setup") or {}
    hierarchy = prepared.get("hierarchy") or {}
    tests = []
    for test in setup.get("tests", []):
        tests.append({
            "name": test.get("name"),
            "enabled": test.get("enabled", True),
            "library": test.get("library"),
            "cell": test.get("cell"),
            "view": test.get("view"),
            "simulator": test.get("simulator"),
            "state": test.get("state"),
            "variables": _sorted_semantic_rows([
                {"name": row.get("name"), "value": row.get("value")}
                for row in test.get("variables", [])
            ]),
            "analyses": _sorted_semantic_rows([
                {
                    "name": row.get("name"),
                    "enabled": row.get("enabled", True),
                    "fields": row.get("fields") or {},
                }
                for row in test.get("analyses", [])
            ]),
            "outputs": _sorted_semantic_rows([
                {
                    "name": row.get("name"),
                    "expression": row.get("expression"),
                    "eval_type": row.get("eval_type"),
                    "unit": row.get("unit"),
                }
                for row in test.get("outputs", [])
            ]),
        })
    corners = _sorted_semantic_rows([
        {
            "name": corner.get("name"),
            "enabled": corner.get("enabled", True),
            "variables": _sorted_semantic_rows([
                {"name": row.get("name"), "value": row.get("value")}
                for row in corner.get("variables", [])
            ]),
        }
        for corner in setup.get("corners", [])
    ])
    parameter_fields = (
        "parameter_id", "hierarchy_path", "owner_occurrence_path", "name", "prompt",
        "source", "backend", "semantic", "value_type", "value_kind", "cdf_type",
        "cdf_parameter_type", "current_value", "default_value", "numeric_value", "unit",
        "choices_raw", "minimum", "maximum", "editable", "tunable", "optimizable",
        "owner_library", "owner_cell", "master_library", "master_cell", "design_library",
        "design_cell", "design_view", "callback",
    )
    parameters = _sorted_semantic_rows([
        dict((field, row.get(field)) for field in parameter_fields)
        for row in prepared.get("parameters", []) if row.get("optimizable")
    ])
    config_parameter_fields = (
        "owner_view", "owner_view_type", "resolved_top", "bound_library", "bound_cell", "bound_view",
        "resolved_library", "resolved_cell", "resolved_view", "resolved_view_type", "stop",
        "config_context", "config_context_path", "config_local_path", "netlister_ignored",
        "scan_traversable", "symbol_masked",
    )
    config_parameters = _sorted_semantic_rows([
        dict([("parameter_id", row.get("parameter_id"))] + [
            (field, row.get(field)) for field in config_parameter_fields
        ]) for row in prepared.get("parameters", []) if row.get("optimizable") and row.get("resolved_top")
    ])
    designs = []
    for design in hierarchy.get("designs", []):
        probed = design.get("hierarchy") or {}
        designs.append({
            "design": _design_identity(design.get("design")),
            "cells": _sorted_semantic_rows([
                {
                    "hierarchy_path": row.get("hierarchy_path"),
                    "library": row.get("library"),
                    "cell": row.get("cell"),
                    "view": row.get("view"),
                    "depth": row.get("depth"),
                    "children": sorted(row.get("children") or []),
                }
                for row in probed.get("cells", [])
            ]),
            "instances": _sorted_semantic_rows([
                dict((field, row.get(field)) for field in (
                    "hierarchy_path", "name", "master_library", "master_cell", "master_view",
                    "master_view_type", "has_schematic", "parent_path", "depth", "kind",
                ))
                for row in probed.get("instances", [])
            ]),
        })
    payload = {
        "prepared_schema": prepared.get("schema_version"),
        "setup_schema": setup.get("schema_version"),
        "setup_backend": setup.get("backend") or "maestro",
        "setup_sha256": (setup.get("source") or {}).get("sha256"),
        "mode": setup.get("mode"),
        "tests": _sorted_semantic_rows(tests),
        "corners": corners,
        "outputs": _sorted_semantic_rows([
            {
                "key": row.get("key"), "test": row.get("test"), "name": row.get("name"),
                "expression": row.get("expression"),
            }
            for row in prepared.get("outputs", [])
        ]),
        "parameters": parameters,
        "hierarchy": _sorted_semantic_rows(designs),
    }
    config_identity = configuration_binding_identity(hierarchy)
    if config_identity is not None:
        payload["configuration_bindings"] = config_identity
        payload["configuration_parameters"] = config_parameters
    encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _prepared_schema_fingerprint(prepared):
    return _prepared_schema_fingerprint_v2(prepared)


def _pdk_policy_identity(value):
    policy = normalize_adapter_policy(value)
    policy_path = policy.pop("path", None)
    if policy_path and os.path.isfile(os.path.abspath(os.path.expanduser(str(policy_path)))):
        policy["custom_adapter_sha256"] = sha256_file(
            os.path.abspath(os.path.expanduser(str(policy_path)))
        )
    elif policy_path:
        policy["custom_adapter"] = {
            "kind": "missing", "name": os.path.basename(str(policy_path)),
        }
    return policy


def _prepared_eda_context_identity(prepared):
    context = prepared.get("eda_context") or {}
    cadence = context.get("cadence") or {}
    pdk = context.get("pdk") or {}
    version_checks = _sorted_semantic_rows([
        {
            "name": row.get("name"),
            "ok": row.get("ok"),
            "version": row.get("version"),
            "returncode": row.get("returncode"),
        }
        for row in cadence.get("version_checks") or []
    ])
    adapter = str(pdk.get("pdk_adapter") or GENERIC_ADAPTER_ID)
    adapter_path = os.path.abspath(os.path.expanduser(adapter))
    if adapter != GENERIC_ADAPTER_ID and os.path.isfile(adapter_path):
        legacy_adapter_identity = {
            "kind": "file", "sha256": sha256_file(adapter_path),
        }
    else:
        legacy_adapter_identity = {
            "kind": "builtin" if adapter == GENERIC_ADAPTER_ID else "named",
            "name": adapter if adapter == GENERIC_ADAPTER_ID else os.path.basename(adapter),
        }
    context_rows = []
    for context_row in pdk.get("contexts") or []:
        selection = context_row.get("adapter_selection") or {}
        context_rows.append({
            "id": context_row.get("id"), "name": context_row.get("name"),
            "profile_id": context_row.get("profile_id"),
            "reference_libraries": sorted(context_row.get("reference_libraries") or []),
            "technology_libraries": sorted(context_row.get("technology_libraries") or []),
            "policy": _pdk_policy_identity(context_row.get("adapter_policy")),
            "adapter": copy.deepcopy(selection.get("identity")),
            "adapter_id": selection.get("adapter_id") or GENERIC_ADAPTER_ID,
        })
    context_rows = _sorted_semantic_rows(context_rows)
    submission = context.get("submission") or {}
    submit_script = submission.get("submit_script")
    if isinstance(submit_script, str) and os.path.isfile(submit_script):
        submit_identity = {"kind": "file", "sha256": sha256_file(submit_script)}
    else:
        submit_identity = {"kind": "missing"}
    payload = {
        "schema_version": "crescendo.eda_context_identity.v2",
        "cadence_versions": version_checks,
        "pdk": {
            "ok": pdk.get("ok"),
            "name": pdk.get("pdk_name"),
            "candidate_libraries": sorted(pdk.get("candidate_libraries") or []),
            "adapter": legacy_adapter_identity,
            "adapter_policy": _pdk_policy_identity(pdk.get("pdk_adapter_policy")),
            "contexts": context_rows,
        },
        "submission": {
            "controller": submission.get("controller"),
            "simulator": submission.get("simulator"),
            "submit_script": submit_identity,
        },
    }
    external_environment = context.get("external_environment")
    if external_environment is not None:
        validate_external_eda_environment_identity(external_environment)
        payload["external_environment"] = copy.deepcopy(external_environment)
    encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    payload["fingerprint"] = hashlib.sha256(encoded).hexdigest()
    return payload


def _eda_context_identity_matches(expected, actual):
    if not isinstance(expected, dict) or not isinstance(actual, dict):
        return False
    for identity in (expected, actual):
        external_environment = identity.get("external_environment")
        if external_environment is not None:
            try:
                validate_external_eda_environment_identity(external_environment)
            except ValueError:
                return False
    if (
            expected.get("schema_version") == actual.get("schema_version")
            and expected.get("fingerprint") == actual.get("fingerprint")):
        return True
    if expected.get("external_environment") is not None:
        return False
    expected_environment = dict(expected)
    actual_environment = dict(actual)
    for payload in (expected_environment, actual_environment):
        payload.pop("fingerprint", None)
    actual_environment.pop("external_environment", None)
    if expected_environment == actual_environment:
        return True
    legacy_submission = {
        "controller": None,
        "simulator": None,
        "submit_script": {"kind": "missing"},
    }
    if expected_environment.get("submission") != legacy_submission:
        return False
    for payload in (expected_environment, actual_environment):
        payload.pop("submission", None)
    return expected_environment == actual_environment


def _checkpoint_checksum(document):
    payload = dict(document)
    payload.pop("payload_sha256", None)
    # Portable exports embed precisely the prefix already authenticated by
    # candidate_store. This transport field cannot alter the checkpoint's
    # origin checksum/ancestry; checkpoint_candidates checks its full hash.
    if payload.get("candidate_store") is not None:
        payload.pop("candidate_store_embedded", None)
    encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def checkpoint_continuation_schema(document):
    manifest = copy.deepcopy(document.get("manifest") or {})
    # Actual model qualification appears after the first native capture. Its
    # monotonic continuity is checked separately from the static run schema.
    manifest.pop("simulator_model_profile_identity", None)
    manifest.pop("origin_run_id", None)
    manifest.pop("origin_run_root", None)
    manifest.pop("maestro_path", None)
    setup_locator = manifest.get("setup_locator")
    if isinstance(setup_locator, dict):
        manifest["setup_locator"] = {
            "backend": setup_locator.get("backend") or "auto",
        }
    spec = copy.deepcopy(document.get("spec"))
    if isinstance(spec, dict):
        spec.pop("simulator_model_profile_identity", None)
        spec.pop("maestro_path", None)
        spec.pop("setup_path", None)
        setup_locator = spec.get("setup_locator")
        if isinstance(setup_locator, dict):
            spec["setup_locator"] = {
                "backend": setup_locator.get("backend") or "auto",
            }
    return {
        "schema_version": document.get("schema_version"),
        "manifest": manifest,
        "spec": spec,
        "parameter_ids": document.get("parameter_ids"),
        "candidate_layout": document.get("candidate_layout"),
    }


def checkpoint_model_profile_transition(older, newer):
    before = (older.get("spec") or {}).get("simulator_model_profile_identity")
    after = (newer.get("spec") or {}).get("simulator_model_profile_identity")
    if before == after:
        return True
    if before is not None or after is None:
        return False
    # First qualification may advance the already durable planned checkpoint;
    # an unqualified historical fitness can never acquire a new model identity.
    reference = older.get("candidate_store")
    rows = (dataset_from_reference(reference, older.get("candidate_store_embedded"))["candidates"]
            if reference is not None else checkpoint_candidates(older))
    return not any(row.get("status") == "completed" or row.get("metrics") for row in rows)


def _checkpoint_document(spec, prepared, generator, rows, selection_history, next_generation, plan,
                         status, ancestry=None, terminal_state=None,
                         terminal_reason=None, candidate_layout=None, candidate_store=None):
    spec = copy.deepcopy(dict(spec))
    variable_ids = [row["parameter_id"] for row in spec["variables"]]
    candidate_layout = _checkpoint_candidate_layout(candidate_layout)
    terminal = ("completed", "failed", "infrastructure_failed")
    checkpoint_rows = sorted(
        (row for row in rows if row.get("status") in terminal),
        key=lambda row: (row.get("generation", -1), row.get("index", -1), row.get("candidate_id", "")),
    )
    setup_reference = prepared_setup_reference(prepared=prepared, spec=spec)
    manifest = {
        "origin_run_id": status.get("run_id"),
        "origin_run_root": status.get("run_root"),
        "continuation_id": status.get("continuation_id"),
        "algorithm": spec.get("algorithm"),
        "algorithm_policy": spec.get("algorithm_policy"),
        "algorithm_parameters": spec.get("algorithm_parameters"),
        "prepared_schema_version": "crescendo.prepared_fingerprint.v2",
        "prepared_schema_sha256": _prepared_schema_fingerprint(prepared),
    }
    if spec.get("simulator_execution_identity") is not None:
        manifest["simulator_execution_identity"] = copy.deepcopy(spec["simulator_execution_identity"])
    if spec.get("execution_commands") is not None:
        manifest["execution_commands"] = copy.deepcopy(spec["execution_commands"])
    if spec.get("execution_protocol") is not None:
        manifest["execution_protocol"] = spec["execution_protocol"]
    for field in ("command_scheduler", "simulator_model_profile_identity", "netlist_generation_protocol",
                  "native_qualification_revision", "execution_selection"):
        if spec.get(field) is not None:
            manifest[field] = copy.deepcopy(spec[field])
    if spec.get("display_name") is not None:
        manifest["display_name"] = spec["display_name"]
    if setup_reference.get("path"):
        manifest["setup_locator"] = setup_reference
    if setup_reference.get("backend") == "maestro":
        manifest["maestro_path"] = setup_reference["path"]
    if prepared.get("cell_selection") is not None:
        cell_selection_scopes(prepared["cell_selection"])
        manifest["cell_selection"] = copy.deepcopy(prepared["cell_selection"])
    if (prepared.get("job_policy") or {}).get("available"):
        from .job_policy import job_policy_fingerprint
        manifest["maestro_job_policy_sha256"] = job_policy_fingerprint(
            prepared["job_policy"], spec.get("maestro_job_overrides"),
        )
    document = {
        "schema_version": "crescendo.checkpoint.v1",
        "created_at": utc_timestamp(),
        "manifest": manifest,
        "spec": spec,
        "parameter_ids": variable_ids,
        "candidate_layout": candidate_layout,
        "candidates": [] if candidate_store is not None else [
            _pack_candidate(row, variable_ids, candidate_layout)
            for row in checkpoint_rows
        ],
        "state": {
            "next_generation": int(next_generation),
            "selection_history": selection_history,
            "generator": _generator_state(generator),
            "active_plan": _pack_plan(plan, variable_ids),
            "checkpoint_revision": int(status.get("checkpoint_revision", 0)),
            "completed_candidates": status.get("completed_candidates", 0),
            "failed_candidates": status.get("failed_candidates", 0),
            "infrastructure_failed_candidates": status.get("infrastructure_failed_candidates", 0),
            "resource_guard_restart_count": status.get("resource_guard_restart_count", 0),
            "resource_guard_recycles": copy.deepcopy(
                status.get("resource_guard_recycles") or []
            )[-20:],
            "ancestor_payload_sha256": list(ancestry or []),
        },
    }
    if candidate_store is not None:
        document["candidate_store"] = candidate_store
    if terminal_state is not None:
        document["state"]["terminal_state"] = terminal_state
        document["state"]["terminal_reason"] = terminal_reason
    if status.get("persistent_session_health") is not None:
        document["state"]["persistent_session_health"] = status["persistent_session_health"]
    source_identity = prepared.get("source_identity")
    if source_identity is not None:
        document["manifest"]["source_design_identity"] = source_identity
    if prepared.get("setup_baseline_snapshot") is not None:
        document["manifest"]["setup_baseline_snapshot"] = copy.deepcopy(prepared["setup_baseline_snapshot"])
    from .private_suite_service import suite_reference
    suite = suite_reference(prepared)
    if suite:
        document["manifest"]["private_suite"] = suite
    if prepared.get("cell_selection") is not None:
        document["spec"] = dict(spec)
        document["spec"]["cell_selection"] = copy.deepcopy(prepared["cell_selection"])
    context_identity = _prepared_eda_context_identity(prepared)
    if context_identity is not None:
        document["manifest"]["eda_context_identity"] = context_identity
    document["payload_sha256"] = _checkpoint_checksum(document)
    return document


def _checkpoint_nonnegative_integer(value, description):
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("checkpoint %s must be a non-negative integer" % description)
    return value


def validate_checkpoint_integrity(checkpoint):
    if not isinstance(checkpoint, dict) or checkpoint.get("schema_version") != "crescendo.checkpoint.v1":
        raise ValueError("unsupported checkpoint; expected crescendo.checkpoint.v1")
    expected_checksum = checkpoint.get("payload_sha256")
    if expected_checksum is not None:
        try:
            actual_checksum = _checkpoint_checksum(checkpoint)
        except (TypeError, ValueError, OverflowError):
            raise ValueError("checkpoint payload cannot be checksummed")
        if not isinstance(expected_checksum, str) or expected_checksum != actual_checksum:
            raise ValueError("checkpoint payload checksum is invalid")
    rows = checkpoint_candidates(checkpoint)
    candidate_layout = _checkpoint_candidate_layout(checkpoint)
    for position, packed in enumerate(checkpoint.get("candidates") or []):
        if len(packed) != len(candidate_layout):
            raise ValueError(
                "checkpoint candidate %d width does not match candidate_layout"
                % position
            )
    spec = checkpoint.get("spec") or {}
    real_eda = str(spec.get("evaluator", "real")).strip().lower() != "mock"
    if expected_checksum is None and real_eda:
        raise ValueError("real EDA checkpoint payload checksum is missing")
    manifest = checkpoint.get("manifest")
    if manifest is not None and not isinstance(manifest, dict):
        raise ValueError("checkpoint manifest is malformed")
    manifest_scopes = cell_selection_scopes((manifest or {}).get("cell_selection"))
    spec_scopes = cell_selection_scopes(spec.get("cell_selection"))
    if ((manifest_scopes is None) != (spec_scopes is None) or
            (manifest_scopes is not None and manifest_scopes != spec_scopes)):
        raise ValueError("checkpoint manifest and spec cell selections disagree")
    if (manifest or {}).get("simulator_execution_identity") != spec.get("simulator_execution_identity"):
        raise ValueError("checkpoint simulator execution identities disagree")
    for field in ("execution_commands", "execution_protocol", "simulator_model_profile_identity", "netlist_generation_protocol",
                  "native_qualification_revision", "execution_selection"):
        if (manifest or {}).get(field) != spec.get(field):
            raise ValueError("checkpoint %s identities disagree" % field)
    if (manifest or {}).get("command_scheduler", "auto") != spec.get("command_scheduler", "auto"):
        raise ValueError("checkpoint command_scheduler identities disagree")
    if spec.get("command_scheduler", "auto") not in ("auto", "batch", "receipt"):
        raise ValueError("checkpoint command_scheduler is unsupported")
    if spec.get("execution_commands") is not None:
        from .execution_commands import validate_execution_commands
        validate_execution_commands(spec["execution_commands"])
    context_identity = (manifest or {}).get("eda_context_identity")
    if isinstance(context_identity, dict) and "external_environment" in context_identity:
        validate_external_eda_environment_identity(
            context_identity.get("external_environment")
        )
    expected_ids = list(checkpoint.get("parameter_ids") or [])
    if any(not isinstance(parameter_id, str) or not parameter_id for parameter_id in expected_ids):
        raise ValueError("checkpoint parameter_ids contains an invalid identifier")
    if len(expected_ids) != len(set(expected_ids)):
        raise ValueError("checkpoint parameter_ids contains duplicates")
    variables = spec.get("variables", [])
    if not isinstance(variables, list) or any(not isinstance(row, dict) for row in variables):
        raise ValueError("checkpoint variable schema is malformed")
    for field in ("objectives", "constraints"):
        definitions = spec.get(field, [])
        if not isinstance(definitions, list) or any(
            not isinstance(row, dict) for row in definitions
        ):
            raise ValueError("checkpoint %s schema is malformed" % field)
    actual_ids = [row.get("parameter_id") for row in variables]
    if expected_ids != actual_ids:
        raise ValueError("checkpoint parameter schema is inconsistent")
    seen_ids = set()
    for row in rows:
        candidate_id = row.get("candidate_id")
        generation = _checkpoint_nonnegative_integer(row.get("generation"), "candidate generation")
        index = _checkpoint_nonnegative_integer(row.get("index"), "candidate index")
        if row.get("status") not in ("completed", "failed", "infrastructure_failed"):
            raise ValueError("checkpoint candidate status is not terminal")
        expected_id = "g%03d_c%04d" % (generation, index)
        if candidate_id != expected_id:
            raise ValueError("checkpoint candidate identity is inconsistent: %r" % candidate_id)
        if candidate_id in seen_ids:
            raise ValueError("checkpoint contains duplicate candidate: %s" % candidate_id)
        seen_ids.add(candidate_id)
    state_value = checkpoint.get("state")
    state = {} if state_value is None else state_value
    if not isinstance(state, dict):
        raise ValueError("checkpoint state is malformed")
    if "checkpoint_revision" in state:
        _checkpoint_nonnegative_integer(
            state.get("checkpoint_revision"), "revision",
        )
    if "next_generation" in state:
        _checkpoint_nonnegative_integer(state.get("next_generation"), "next_generation")
    if "resource_guard_restart_count" in state:
        _checkpoint_nonnegative_integer(
            state.get("resource_guard_restart_count"),
            "resource_guard_restart_count",
        )
    resource_guard_recycles = state.get("resource_guard_recycles", [])
    if not isinstance(resource_guard_recycles, list) or any(
            not isinstance(event, dict) for event in resource_guard_recycles):
        raise ValueError("checkpoint resource_guard_recycles is malformed")
    ancestry = state.get("ancestor_payload_sha256", [])
    if not isinstance(ancestry, list) or any(
        not isinstance(checksum, str) or not re.match(r"^[0-9a-f]{64}$", checksum)
        for checksum in ancestry
    ):
        raise ValueError("checkpoint ancestor checksum chain is malformed")
    if len(ancestry) != len(set(ancestry)):
        raise ValueError("checkpoint ancestor checksum chain contains duplicates")
    terminal_state = state.get("terminal_state")
    terminal_reason = state.get("terminal_reason")
    if terminal_state not in (None, "completed"):
        raise ValueError("checkpoint terminal_state is not recognized")
    if terminal_state is None:
        if terminal_reason is not None:
            raise ValueError("checkpoint terminal_reason requires terminal_state")
    elif not isinstance(terminal_reason, str) or not terminal_reason.strip():
        raise ValueError("completed checkpoint requires a terminal_reason")
    raw_plan = state.get("active_plan")
    if terminal_state == "completed" and raw_plan is not None:
        raise ValueError("completed checkpoint cannot contain an active plan")
    if raw_plan is not None:
        if not isinstance(raw_plan, dict):
            raise ValueError("checkpoint active plan is malformed")
        for field in ("generation", "next_index"):
            if field not in raw_plan:
                raise ValueError("checkpoint active plan is missing %s" % field)
            _checkpoint_nonnegative_integer(raw_plan.get(field), "active plan %s" % field)
        _checkpoint_nonnegative_integer(
            raw_plan.get("replacements", 0), "active plan replacements",
        )
        raw_planned = raw_plan.get("planned", [])
        if not isinstance(raw_planned, list):
            raise ValueError("checkpoint active plan candidates are malformed")
        for position, packed in enumerate(raw_planned):
            if not isinstance(packed, (list, tuple)) or len(packed) != 2:
                raise ValueError("checkpoint active plan candidate %d is malformed" % position)
            _checkpoint_nonnegative_integer(
                packed[0], "active plan candidate %d index" % position,
            )
            if not isinstance(packed[1], (list, tuple)) or len(packed[1]) != len(expected_ids):
                raise ValueError(
                    "checkpoint active plan candidate %d values are malformed" % position
                )
    try:
        plan = _unpack_plan(raw_plan, expected_ids)
    except (IndexError, KeyError, TypeError, ValueError, OverflowError):
        raise ValueError("checkpoint active plan is malformed")
    if plan is not None:
        next_generation = _checkpoint_nonnegative_integer(
            state.get("next_generation"), "next_generation",
        )
        if plan.get("generation") != next_generation:
            raise ValueError("checkpoint active plan generation does not match next_generation")
        planned_indices = [index for index, _values in plan.get("planned", [])]
        if len(planned_indices) != len(set(planned_indices)):
            raise ValueError("checkpoint active plan contains duplicate candidate indices")
        next_index = plan.get("next_index", 0)
        replacements = plan.get("replacements", 0)
        if planned_indices != list(range(next_index)):
            raise ValueError("checkpoint active plan indices must be contiguous and match next_index")
        population = spec.get("population")
        if isinstance(population, bool) or not isinstance(population, int) or population < 1:
            raise ValueError("checkpoint population must be positive when an active plan exists")
        if replacements != max(0, next_index - population):
            raise ValueError("checkpoint active plan replacement count is inconsistent")
        planned_values = dict(plan.get("planned", []))
        if any(set(values) != set(expected_ids) for values in planned_values.values()):
            raise ValueError("checkpoint active plan parameter values are incomplete")
        for row in rows:
            if row.get("generation") != next_generation:
                continue
            index = row.get("index")
            if index not in planned_values:
                raise ValueError("checkpoint terminal candidate is missing from the active plan")
            if not _terminal_wal_json_exact(row.get("values"), planned_values[index]):
                raise ValueError("checkpoint terminal candidate values do not match the active plan")
    generator_value = state.get("generator")
    generator_state = {} if generator_value is None else generator_value
    if not isinstance(generator_state, dict):
        raise ValueError("checkpoint optimizer state is malformed")
    rng_state = generator_state.get("rng_state")
    modern_real_checkpoint = (
        real_eda
        and (manifest or {}).get("prepared_schema_version") == "crescendo.prepared_fingerprint.v2"
    )
    if rng_state is None and modern_real_checkpoint:
        raise ValueError("real EDA checkpoint optimizer rng_state is missing")
    if rng_state is not None:
        try:
            probe_rng = random.Random()
            probe_rng.setstate(_nested_tuple(rng_state))
        except (IndexError, TypeError, ValueError, OverflowError):
            raise ValueError("checkpoint optimizer rng_state is malformed")
    population_size = generator_state.get("population_size")
    if population_size is not None and (
        isinstance(population_size, bool) or not isinstance(population_size, int)
        or population_size < 1
    ):
        raise ValueError("checkpoint optimizer population_size is malformed")
    previous_value = generator_state.get("previous")
    de_targets_value = generator_state.get("de_targets")
    previous = [] if previous_value is None else previous_value
    de_targets = [] if de_targets_value is None else de_targets_value
    if not isinstance(previous, list) or not isinstance(de_targets, list):
        raise ValueError("checkpoint optimizer references are malformed")
    referenced = list(previous)
    for position, target in enumerate(de_targets):
        if not isinstance(target, (list, tuple)) or len(target) != 2:
            raise ValueError("checkpoint optimizer target %d is malformed" % position)
        target_ids = target[1]
        if not isinstance(target_ids, list):
            raise ValueError("checkpoint optimizer target references are malformed")
        referenced.extend(target_ids)
    if any(not isinstance(candidate_id, str) for candidate_id in referenced):
        raise ValueError("checkpoint optimizer candidate reference is malformed")
    missing_references = sorted(set(referenced) - seen_ids)
    if missing_references:
        raise ValueError("checkpoint optimizer state references missing candidate(s): %s" % ", ".join(missing_references[:10]))
    return rows


def validate_checkpoint(checkpoint, prepared):
    rows = validate_checkpoint_integrity(checkpoint)
    spec = checkpoint.get("spec") or {}
    if (spec.get("execution_backend") == "maestro_baseline" and
            spec.get("execution_protocol") != "crescendo.maestro_baseline.v1"):
        raise ValueError("Maestro baseline checkpoint protocol does not match its execution backend")
    if (spec.get("execution_protocol") == "crescendo.maestro_baseline.v1" and
            spec.get("execution_backend") != "maestro_baseline"):
        raise ValueError("Maestro baseline checkpoint cannot be resumed with another execution backend")
    if (str(spec.get("evaluator", "real")).lower() != "mock" and
            spec.get("execution_backend") in ("maestro_netlist", "private_netlist")):
        from .native_maestro_parameters import GENERATION_PROTOCOL
        if spec.get("netlist_generation_protocol") != GENERATION_PROTOCOL:
            raise ValueError("Checkpoint uses an older netlist-generation protocol; retain it for reading/export or resume with its original Crescendo version. Start a new run for native Device Parameters.")
        from .native_qualification import NATIVE_QUALIFICATION_REVISION
        if spec.get("native_qualification_revision") != NATIVE_QUALIFICATION_REVISION:
            raise ValueError("Checkpoint uses an older point-qualification revision; retain its results for reading/export. Start a new run so exact Test Local/RDB and generated-netlist values are qualified again.")
    from .test_selection import selection_plan
    selection_plan(prepared.get("setup") or {}, spec)
    expected_ids = list(checkpoint.get("parameter_ids") or [])
    available = set(
        row.get("parameter_id") for row in prepared.get("parameters", []) if row.get("optimizable")
    )
    missing = [parameter_id for parameter_id in expected_ids if parameter_id not in available]
    if missing:
        raise ValueError("checkpoint parameters are missing after Maestro reload: %s" % ", ".join(missing[:10]))
    outputs = set(row.get("key") for row in prepared.get("outputs", []))
    missing_outputs = sorted(set(
        row.get("output_key") for row in spec.get("objectives", []) + spec.get("constraints", [])
        if row.get("output_key") not in outputs
    ))
    if missing_outputs:
        raise ValueError("checkpoint outputs are missing after Maestro reload: %s" % ", ".join(missing_outputs))
    manifest = checkpoint.get("manifest") or {}
    if manifest.get("maestro_job_policy_sha256") is not None:
        from .job_policy import job_policy_fingerprint
        if manifest["maestro_job_policy_sha256"] != job_policy_fingerprint(
                prepared.get("job_policy"), spec.get("maestro_job_overrides")):
            raise ValueError("checkpoint original Maestro job policy changed after reload")
    if manifest.get("cell_selection") is not None:
        expected_scopes = cell_selection_scopes(manifest["cell_selection"])
        actual_scopes = cell_selection_scopes(prepared.get("cell_selection"))
        if actual_scopes != expected_scopes:
            raise ValueError("checkpoint selected cell occurrences changed after reload")
    real_eda = str(spec.get("evaluator", "real")).strip().lower() != "mock"
    expected_fingerprint = manifest.get("prepared_schema_sha256")
    fingerprint_version = manifest.get("prepared_schema_version")
    if fingerprint_version in (None, "crescendo.prepared_fingerprint.v1"):
        actual_fingerprint = _prepared_schema_fingerprint_v1(prepared)
    elif fingerprint_version == "crescendo.prepared_fingerprint.v2":
        actual_fingerprint = _prepared_schema_fingerprint_v2(prepared)
    else:
        raise ValueError("checkpoint prepared fingerprint version is not supported: %s" % fingerprint_version)
    if expected_fingerprint and expected_fingerprint != actual_fingerprint:
        raise ValueError("checkpoint Maestro setup, corner, output, or hierarchy schema has changed")
    expected_source_identity = manifest.get("source_design_identity")
    if expected_source_identity is None and real_eda:
        raise ValueError(
            "real EDA checkpoint lacks source design identity; start a fresh optimization"
        )
    if expected_source_identity is not None:
        if (
            not isinstance(expected_source_identity, dict)
            or not isinstance(expected_source_identity.get("schema_version"), str)
            or not isinstance(expected_source_identity.get("fingerprint"), str)
            or not expected_source_identity.get("fingerprint")
        ):
            raise ValueError("checkpoint source design identity is malformed")
        actual_source_identity = prepared.get("source_identity")
        if (
            not isinstance(actual_source_identity, dict)
            or not isinstance(actual_source_identity.get("schema_version"), str)
            or not isinstance(actual_source_identity.get("fingerprint"), str)
            or not actual_source_identity.get("fingerprint")
        ):
            raise ValueError("checkpoint source design identity cannot be verified after Maestro reload")
        if (
                expected_source_identity.get("schema_version") != actual_source_identity.get("schema_version")
                or expected_source_identity.get("fingerprint") != actual_source_identity.get("fingerprint")):
            raise ValueError("checkpoint source design content has changed after Maestro reload")
    expected_context_identity = manifest.get("eda_context_identity")
    if expected_context_identity is None and real_eda:
        raise ValueError(
            "real EDA checkpoint lacks Cadence/PDK/submission identity; start a fresh optimization"
        )
    if expected_context_identity is not None:
        if (
            not isinstance(expected_context_identity, dict)
            or not isinstance(expected_context_identity.get("schema_version"), str)
            or not isinstance(expected_context_identity.get("fingerprint"), str)
            or not expected_context_identity.get("fingerprint")
        ):
            raise ValueError("checkpoint Cadence/PDK/submission identity is malformed")
        actual_context_identity = _prepared_eda_context_identity(prepared)
        if not _eda_context_identity_matches(
                expected_context_identity, actual_context_identity):
            raise ValueError(
                "checkpoint Cadence, simulator, PDK, model, technology, startup, "
                "or submission identity has changed"
            )
    return rows



def _reconcile_baseline_shutdown(run_root, owned_roots):
    """Reconcile the exact roots created by this run; preserve native raw data.

    artifact_cleanup_complete means the requested retention policy is settled,
    not that retained candidate evidence or an owned suite has been deleted.
    Normal native completion and an acknowledged cancellation are separate
    terminal reasons. Local process exit alone never qualifies a remote Stop.
    """
    from .native_baseline import reconcile_native_baseline
    roots = sorted(set(os.path.abspath(path) for path in owned_roots))
    candidates_root = os.path.join(os.path.abspath(run_root), "candidates")
    observations, candidate_proofs = [], {}
    workspaces = set()
    for root in roots:
        try:
            if os.path.dirname(root) != candidates_root or os.path.islink(root):
                raise ValueError("Native shutdown root is outside this run's owned candidate set")
            native = reconcile_native_baseline(root, request_cancel=True, timeout_seconds=12)
            if not isinstance(native, dict):
                raise ValueError("Native shutdown reconciliation returned no structured proof")
            no_launch = native.get("launch_attempted") is False
            local = native.get("local_cleanup") or {}
            process_proven = bool((no_launch and native.get("cleanup_proven") is True) or
                (local.get("state") == "dead" and (local.get("termination_proof") or {}).get("ok") is True))
            stop = native.get("native_stop") or {}
            completed = (native.get("launch_attempted") is True and
                         native.get("native_session_closed") is True and
                         stop.get("requested") is False)
            stopped = (native.get("native_session_closed") is True and
                       stop.get("remote_stop_proven") is True)
            terminal = bool(native.get("cleanup_proven") is True and (no_launch or completed or stopped))
            kind = "not_launched" if no_launch else "cancelled" if stopped else "completed" if completed else "unknown"
            ok = process_proven and terminal
            workspace = native.get("workspace_root")
            if workspace:
                workspaces.add(workspace)
            proof = {"ok": ok, "cleanup_proven": ok, "process_termination_proven": process_proven,
                     "remote_stop_proven": terminal, "artifact_cleanup_complete": ok,
                     "native_terminal_state": kind, "raw_deleted": False,
                     "artifact_policy": "retain_owned_suite_and_candidate_evidence",
                     "candidate_root": root, "observed_at": utc_timestamp()}
            state_path = os.path.join(root, "native_baseline", "native_baseline_state.json")
            observations.append({"candidate_root": os.path.relpath(root, run_root), "ok": ok,
                                 "native_terminal_state": kind,
                                 "process_termination_proven": process_proven, "remote_stop_proven": terminal,
                                 "raw_cleanup_proof": os.path.relpath(os.path.join(root, "raw_cleanup_proof.json"), run_root),
                                 "native_state_path": os.path.relpath(state_path, run_root),
                                 "native_state_sha256": sha256_file(state_path) if os.path.isfile(state_path) else None})
        except Exception as error:
            proof = {"ok": False, "cleanup_proven": False, "process_termination_proven": False,
                     "remote_stop_proven": False, "artifact_cleanup_complete": False,
                     "native_terminal_state": "unknown", "raw_deleted": False,
                     "artifact_policy": "retain_owned_suite_and_candidate_evidence",
                     "candidate_root": root, "error": "%s: %s" % (type(error).__name__, error)}
            observations.append({"candidate_root": os.path.relpath(root, run_root), "ok": False,
                                 "process_termination_proven": False, "remote_stop_proven": False,
                                 "native_terminal_state": "unknown", "error": proof["error"][:1024]})
        candidate_proofs[root] = proof
        if os.path.dirname(root) == candidates_root and os.path.isdir(root) and not os.path.islink(root):
            atomic_write_json(os.path.join(root, "raw_cleanup_proof.json"), proof)
    process_proven = all(row["process_termination_proven"] for row in observations)
    terminal_proven = all(row["remote_stop_proven"] for row in observations)
    settled = all(row["ok"] for row in observations)
    index_path = os.path.join(run_root, "baseline_shutdown_roots.json")
    atomic_write_json(index_path, {"schema_version": "crescendo.maestro_baseline_shutdown_roots.v1", "roots": observations})
    root_index = {"path": "baseline_shutdown_roots.json", "count": len(roots), "sha256": sha256_file(index_path)}
    final = {"process_termination_proven": process_proven, "remote_stop_proven": terminal_proven,
             "artifact_cleanup_complete": settled, "raw_deleted": False,
             "artifact_policy": "retain_owned_suite_and_candidate_evidence",
             "root_index": root_index, "retained_roots_preview": [os.path.relpath(path, run_root) for path in roots[:16]],
             "retained_native_workspaces_preview": sorted(workspaces)[:8], "retained_native_workspace_count": len(workspaces),
             "native_request_count": len(roots), "no_native_requests_created": not roots,
             "observed_at": utc_timestamp()}
    result = dict(final, cleanup_proven=settled, candidates_preview=observations[:16])
    workers = [{"worker_index": "maestro_baseline", "ok": settled, "result": result}]
    return {"workers": workers, "final_cleanup": final, "candidate_proofs": candidate_proofs}


def _write_worker_shutdown(run_root, workers, final_cleanup=None):
    """Retain detailed diagnostics and publish a small shutdown proof."""
    finished_at = utc_timestamp()
    details = []
    compact = {"schema_version": "crescendo.worker_shutdown.v1", "workers": [],
               "finished_at": finished_at}
    for row in workers:
        item = {"worker_index": row.get("worker_index"), "ok": row.get("ok") is True}
        if row.get("error") is not None:
            item["error"] = str(row["error"])[:4096]
        result = row.get("result") or {}
        if row.get("worker_index") == "private_netlist" and result:
            item["details_path"] = "private_netlist/shutdown.json"
        elif result or row.get("error") is not None:
            details.append(row)
            item["details_path"] = "worker_shutdown_details.json"
        item["result"] = dict((key, result[key]) for key in (
            "cleanup_proven", "termination_proven", "process_still_alive",
            "process_termination_proven", "remote_stop_proven", "artifact_cleanup_complete")
            if isinstance(result.get(key), bool))
        compact["workers"].append(item)
    if details:
        atomic_write_json(os.path.join(run_root, "worker_shutdown_details.json"), {
            "schema_version": "crescendo.worker_shutdown.v1", "workers": details,
            "finished_at": finished_at})
    if final_cleanup is not None:
        compact["final_cleanup"] = dict((key, final_cleanup.get(key)) for key in
            ("process_termination_proven", "remote_stop_proven", "artifact_cleanup_complete"))
    atomic_write_json(os.path.join(run_root, "worker_shutdown.json"), compact)


def run_optimization(spec, config, prepared=None, run_root=None, resume_checkpoint=None,
                     continuation_lease=None, runtime_optimization_lease=None):
    if resume_checkpoint is not None:
        raise RuntimeError(
            "checkpoint resume is private and must use OptimizerService.resume "
            "or the public resume CLI"
        )
    return _run_optimization_with_leases(
        spec, config, prepared=prepared, run_root=run_root,
        continuation_lease=continuation_lease,
        runtime_optimization_lease=runtime_optimization_lease,
    )


def _run_coordinated_resume(spec, config, prepared, run_root, resume_checkpoint,
                            continuation_lease, runtime_optimization_lease=None):
    if (
        not isinstance(resume_checkpoint, dict) or
        resume_checkpoint.get("schema_version") != "crescendo.checkpoint.v1"
    ):
        raise ValueError("unsupported checkpoint; expected crescendo.checkpoint.v1")
    if continuation_lease is None or not continuation_lease.held:
        raise RuntimeError("coordinated checkpoint resume requires a held continuation lease")
    return _run_optimization_with_leases(
        spec, config, prepared=prepared, run_root=run_root,
        resume_checkpoint=resume_checkpoint,
        continuation_lease=continuation_lease,
        runtime_optimization_lease=runtime_optimization_lease,
    )


def _run_optimization_with_leases(spec, config, prepared=None, run_root=None,
                                  resume_checkpoint=None, continuation_lease=None,
                                  runtime_optimization_lease=None):
    continuation_id = (
        checkpoint_continuation_id(resume_checkpoint)
        if resume_checkpoint is not None else new_continuation_id()
    )
    configured_runtime_root = (config.get("runtime") or {}).get("run_root")
    if run_root:
        effective_run_root = os.path.abspath(run_root)
    elif configured_runtime_root:
        effective_run_root = os.path.abspath(os.path.join(configured_runtime_root, optimization_run_name(spec.get("display_name"))))
    else:
        return _run_optimization_unleased(
            spec, config, prepared=prepared, run_root=run_root,
            resume_checkpoint=resume_checkpoint, continuation_id=continuation_id,
            checkpoint_anchor_root=configured_runtime_root,
        )
    effective_spec = (resume_checkpoint or {}).get("spec") or spec
    if str(effective_spec.get("evaluator", "real")).strip().lower() != "mock":
        from .storage_policy import validate_native_data_path
        validate_native_data_path(effective_run_root)
    lease_runtime_root = configured_runtime_root or os.path.dirname(effective_run_root)
    startup_storage_preflight(
        effective_run_root, lease_runtime_root,
        ((config or {}).get("runtime") or {}).get("persistent_min_free_disk_mib", 1024),
        checkpoint=resume_checkpoint, workspace_required=False,
    )
    owns_lease = continuation_lease is None
    lease = continuation_lease or ContinuationLease(
        lease_runtime_root, continuation_id, run_root=effective_run_root,
    ).acquire()
    if not lease.held:
        raise RuntimeError("continuation lease must be acquired before optimization starts")
    if lease.continuation_id != continuation_id:
        if owns_lease:
            lease.release()
        raise ValueError("continuation lease does not match checkpoint continuation_id")
    effective_spec = (resume_checkpoint or {}).get("spec") or spec
    real_eda = str(effective_spec.get("evaluator", "real")).strip().lower() != "mock"
    owns_runtime_lease = bool(real_eda and runtime_optimization_lease is None)
    runtime_lease = runtime_optimization_lease
    if real_eda and runtime_lease is None:
        try:
            runtime_lease = RuntimeOptimizationLease(
                lease_runtime_root, run_root=effective_run_root,
            ).acquire()
        except Exception:
            if owns_lease:
                lease.release()
            raise
    if runtime_lease is not None and not runtime_lease.held:
        if owns_lease:
            lease.release()
        raise RuntimeError("runtime optimization lease must be acquired before real EDA starts")
    try:
        return _run_optimization_unleased(
            spec, config, prepared=prepared, run_root=effective_run_root,
            resume_checkpoint=resume_checkpoint, continuation_id=continuation_id,
            checkpoint_anchor_root=lease_runtime_root,
        )
    finally:
        close_run_data(os.path.join(effective_run_root, CORE_NAME))
        if owns_runtime_lease:
            runtime_lease.release()
        if owns_lease:
            lease.release()


def _run_optimization_unleased(spec, config, prepared=None, run_root=None,
                               resume_checkpoint=None, continuation_id=None,
                               checkpoint_anchor_root=None):
    if resume_checkpoint is not None:
        if not isinstance(resume_checkpoint, dict) or resume_checkpoint.get("schema_version") != "crescendo.checkpoint.v1":
            raise ValueError("unsupported checkpoint; expected crescendo.checkpoint.v1")
        spec = dict(resume_checkpoint.get("spec") or {})
    else:
        spec = dict(spec)
    evaluator = str(spec.get("evaluator", "real")).strip().lower()
    if evaluator not in ("real", "mock"):
        raise ValueError("unsupported optimization evaluator: %s" % spec.get("evaluator"))
    spec["evaluator"] = evaluator
    config = copy.deepcopy(config)
    spec["simulator"] = selected_simulator(config, spec)
    config.setdefault("cadence", {})["simulator"] = spec["simulator"]
    if (evaluator == "real" and spec["simulator"] == "alps" and
            (spec.get("execution_backend") or config.get("runtime", {}).get("execution_backend")) != "maestro_baseline"):
        from .simulators import simulator_configuration_identity
        simulator_identity = simulator_configuration_identity(config, spec)
        if resume_checkpoint is not None and spec.get("simulator_execution_identity") != simulator_identity:
            raise ValueError("ALPS checkpoint executable/model/section/thread/result-reader identity changed; start a fresh optimization")
        spec["simulator_execution_identity"] = simulator_identity

    display_name = _validated_run_display_name(spec.get("display_name"))
    if display_name is None:
        spec.pop("display_name", None)
    else:
        spec["display_name"] = display_name
    spec["objectives"], spec["constraints"] = normalize_metric_definitions(
        spec.get("objectives", []), spec.get("constraints", []),
    )
    candidate_timeout = _optional_candidate_timeout(spec.get("candidate_timeout_seconds", config["runtime"].get("candidate_timeout_seconds", 600)))
    spec["candidate_timeout_seconds"] = candidate_timeout
    try:
        cleanup_timeout = float(config["runtime"].get("persistent_cleanup_timeout_seconds", 20))
        commit_timeout = float(config["runtime"].get("controller_commit_timeout_seconds", 60))
        cleanup_termination_margin = float(config["runtime"].get(
            "persistent_cleanup_termination_margin_seconds", 20,
        ))
    except (TypeError, ValueError, OverflowError):
        raise ValueError("persistent cleanup and controller commit timeouts must be finite numbers")
    if not math.isfinite(cleanup_timeout) or cleanup_timeout <= 0:
        raise ValueError("persistent_cleanup_timeout_seconds must be finite and positive")
    if (
        not math.isfinite(cleanup_termination_margin)
        or cleanup_termination_margin < 0
    ):
        raise ValueError(
            "persistent_cleanup_termination_margin_seconds must be finite and non-negative"
        )
    if (
        not math.isfinite(commit_timeout)
        or commit_timeout <= cleanup_timeout + cleanup_termination_margin
    ):
        raise ValueError(
            "controller_commit_timeout_seconds must exceed the cleanup timeout plus termination margin"
        )
    variables = validate_variables(spec.get("variables", []))
    if not spec.get("objectives"):
        raise ValueError("at least one objective is required")
    objective_count = len(spec["objectives"])
    checkpoint_algorithm_policy = spec.get("algorithm_policy") if resume_checkpoint is not None else None
    algorithm, algorithm_policy, algorithm_parameters = _normalize_algorithm_settings(
        spec, len(variables), objective_count,
    )
    if checkpoint_algorithm_policy is not None:
        algorithm_policy = checkpoint_algorithm_policy
    generations = int(spec.get("generations", 10))
    population_explicit = "population" in spec or "population" in (spec.get("algorithm_parameters") or {})
    population = algorithm_parameters["population"]
    if generations < 1:
        raise ValueError("generations must be at least 1")
    spec["algorithm"] = algorithm
    spec["algorithm_policy"] = algorithm_policy
    spec["algorithm_parameters"] = algorithm_parameters
    spec["seed"] = algorithm_parameters["seed"]
    spec["population"] = population
    spec["population_policy"] = "explicit" if population_explicit else "automatic_by_variable_count"
    prepared_was_supplied = prepared is not None
    if prepared is None:
        setup_reference = normalize_setup_reference(
            spec.get("setup_locator") or spec.get("setup_path") or spec.get("maestro_path")
        )
        selection = ((resume_checkpoint or {}).get("manifest") or {}).get("cell_selection")
        if selection is None:
            selection = spec.get("cell_selection")
        scopes = cell_selection_scopes(selection)
        options = {"backend": setup_reference["backend"], "selected_test": spec.get("selected_test")}
        if scopes is not None:
            options["selected_cell_paths"] = scopes
        prepared = prepare_setup(setup_reference["path"], config, spec.get("max_depth", 32), **options)
    setup = prepared.get("setup") or {}
    rows = setup.get("tests") or []
    selected_test = setup.get("selected_test")
    if len(rows) != 1 or not selected_test or rows[0].get("name") != selected_test or not setup.get("_source_tests"):
        raise ValueError("Prepared setup lacks a qualified single Test; Load and Inspect again.")
    if spec.get("selected_test") not in (None, selected_test):
        raise ValueError("Requested Test differs from prepared Test.")
    spec["selected_test"] = selected_test
    if prepared_was_supplied:
        external_guard = _reverify_prepared_external_environment(prepared, config)
        if not external_guard.get("ok"):
            raise ValueError(external_guard.get("error") or "external EDA environment changed")
    if resume_checkpoint is not None:
        validate_checkpoint(resume_checkpoint, prepared)
    if prepared.get("cell_selection") is not None:
        spec["cell_selection"] = copy.deepcopy(prepared["cell_selection"])
    setup_reference = prepared_setup_reference(prepared=prepared, spec=spec)
    if setup_reference.get("path"):
        spec["setup_locator"] = setup_reference
        spec["setup_path"] = setup_reference["path"]
    if setup_reference.get("backend") == "maestro" and setup_reference.get("path"):
        spec["maestro_path"] = setup_reference["path"]
    elif setup_reference.get("backend") != "maestro":
        spec.pop("maestro_path", None)
    from .test_selection import selection_plan
    requested_selection = selection_plan(prepared.get("setup") or {}, spec)
    if requested_selection is not None:
        spec["execution_selection"] = {"tests": list(requested_selection["selected"]),
            "analyses": {name: list(requested_selection["analyses"][name])
                         for name in requested_selection["selected"]
                         if requested_selection["available_analyses"][name]}}
    selected = _selected_parameters(prepared["parameters"], variables)
    variables = validate_variables(_enforce_catalog_variable_types(variables, selected))
    frozen_diagnostic_candidate = _frozen_environment_candidate(spec, variables)
    frozen_diagnostic_plan = _frozen_environment_plan(spec, variables)
    requested_backend = str(spec.get("execution_backend") or config.get("runtime", {}).get("execution_backend", "auto"))
    if requested_backend not in ("auto", "maestro_netlist", "private_netlist", "maestro_baseline", "legacy", "persistent_maestro", "adexl"):
        raise ValueError("Unknown netlist source: %s" % requested_backend)
    # The two netlist sources share a solver engine; the explicit Maestro
    # baseline retains its own native lifecycle and checkpoint protocol.
    persistent_backend = False
    baseline_backend = evaluator == "real" and requested_backend == "maestro_baseline"
    netlist_backend = evaluator == "real" and not baseline_backend
    setup_backend = (prepared.get("setup") or {}).get("backend") or "maestro"
    spec["execution_backend"] = (
        "maestro_baseline" if baseline_backend else
        ("private_netlist" if requested_backend == "private_netlist" else "maestro_netlist") if netlist_backend
        else "adexl" if setup_backend == "adexl"
        else "legacy"
    )
    if baseline_backend:
        if resume_checkpoint is not None:
            raise ValueError("Maestro baseline checkpoint resume is unavailable until owned native stop and identity are verified; start a new run")
        if setup_backend != "maestro" or not selected or any(row.get("backend", "schematic") != "schematic" for row in selected):
            raise ValueError("Maestro baseline requires a Maestro setup and selected schematic device parameters")
        spec["execution_protocol"] = "crescendo.maestro_baseline.v1"
    if netlist_backend:
        from .native_maestro_parameters import GENERATION_PROTOCOL
        from .native_qualification import NATIVE_QUALIFICATION_REVISION
        spec["netlist_generation_protocol"] = GENERATION_PROTOCOL
        spec["native_qualification_revision"] = NATIVE_QUALIFICATION_REVISION
        if resume_checkpoint is not None and requested_backend not in ("private_netlist", "maestro_netlist"):
            raise ValueError("Historical native-ADE checkpoint cannot be resumed with the shared CLI solver; use its original Crescendo version or start a fresh Maestro Netlist run")
        commands = normalize_execution_commands(config, spec)
        scheduler = spec.get("command_scheduler", "auto" if resume_checkpoint is not None
                             else config.get("runtime", {}).get("command_scheduler", "auto"))
        if scheduler not in ("auto", "batch", "receipt"):
            raise ValueError("Command scheduler must be auto, batch or receipt")
        spec["command_scheduler"] = scheduler
        config.setdefault("runtime", {})["command_scheduler"] = scheduler
        if (resume_checkpoint is not None and checkpoint_candidates(resume_checkpoint)
                and not spec.get("simulator_model_profile_identity")):
            raise ValueError("Historical CLI checkpoint has no frozen model profile; use its original Crescendo version or start a fresh optimization")
        if resume_checkpoint is not None and spec.get("execution_commands") and spec["execution_commands"] != commands:
            raise ValueError("Checkpoint solver/submission command identity changed; start a fresh optimization")
        spec["execution_commands"] = commands
        spec["execution_protocol"] = "crescendo.unified_execution.v1"
        if setup_backend != "maestro":
            raise ValueError("Shared CLI netlist generation requires a Maestro setup; ADE XL remains readable in historical runs")
        if not selected or any(row.get("backend", "schematic") != "schematic" for row in selected):
            raise ValueError("Shared CLI netlist generation supports schematic device parameters; design variables are not qualified")
    if not persistent_backend and not netlist_backend and not baseline_backend:
        spec["legacy_guardian_protocol"] = LEGACY_GUARDIAN_PROTOCOL
    if resume_checkpoint is not None:
        for row in checkpoint_candidates(resume_checkpoint):
            validate_candidate_values(row.get("values") or {}, variables)
    run_root = os.path.abspath(run_root or os.path.join(config["runtime"]["run_root"], optimization_run_name(spec.get("display_name"))))
    if evaluator == "real":
        from .storage_policy import validate_native_data_path
        validate_native_data_path(run_root)
    startup_preflight = startup_storage_preflight(
        run_root, checkpoint_anchor_root or config["runtime"].get("run_root"),
        config["runtime"].get("persistent_min_free_disk_mib", 1024),
        setup=prepared.get("setup"), hierarchy=prepared.get("hierarchy"), config=config,
        checkpoint=resume_checkpoint,
        workspace_required=optimization_workspace_required(spec, resume_checkpoint),
        parameters=selected,
    )
    ensure_dir(run_root)
    config = copy.deepcopy(config)
    if _environment_test_run(spec):
        # Only this new diagnostic entry is bounded to a single attempt. The
        # user's normal runtime configuration is an independent deep copy.
        config.setdefault("runtime", {})["infrastructure_retries"] = 0
        config["_diagnostic_native_run_trace"] = True
    from .job_policy import normalize_job_overrides
    if "maestro_job_overrides" in spec:
        spec["maestro_job_overrides"] = normalize_job_overrides(spec["maestro_job_overrides"])
        config.setdefault("runtime", {})["maestro_job_overrides"] = copy.deepcopy(spec["maestro_job_overrides"])
    if persistent_backend:
        config.setdefault('runtime', {})['maestro_source_job_policy'] = copy.deepcopy(prepared.get('job_policy') or {})
    execution_plan = candidate_execution_plan(spec, prepared, config)
    config.setdefault("runtime", {})["native_cluster_stop_required"] = (
        execution_plan["native_cluster_stop_required"] or
        (persistent_backend and str(spec.get('evaluator') or '').lower() == 'real'
         and not (prepared.get('job_policy') or {}).get('available'))
    )
    batch_enabled = bool(persistent_backend and execution_plan["enabled"])
    config.setdefault("runtime", {})["run_root"] = run_root
    config["runtime"]["_run_display_name"] = display_name or os.path.basename(run_root)
    stop_path = os.path.join(run_root, "STOP")
    status_path = os.path.join(run_root, "status.json")
    spec["variables"] = variables
    spec["variable_labels"] = dict(
        (parameter["parameter_id"], "%s/%s" % (
            str(parameter.get("hierarchy_path") or "/").rstrip("/"),
            parameter.get("name") or parameter["parameter_id"],
        ))
        for parameter in selected
    )
    atomic_write_json(os.path.join(run_root, "spec.json"), spec)
    if spec.get("evaluator", "real") == "real":
        export_prepared = dict((key, copy.deepcopy(prepared[key])) for key in
                               ("setup", "hierarchy", "source_identity", "pdk") if key in prepared)
        export_prepared["parameters"] = copy.deepcopy(selected)
        atomic_write_json(os.path.join(run_root, "schematic_export_context.json"), {
            "schema_version": "crescendo.schematic_export_context.v1",
            "prepared": export_prepared, "config": config,
            "origin_run_id": os.path.basename(((resume_checkpoint or {}).get("manifest") or {}).get("origin_run_root") or "") or None,
        })
    all_rows = checkpoint_candidates(resume_checkpoint) if resume_checkpoint is not None else []
    if resume_checkpoint is not None and resume_checkpoint.get("candidate_store") is not None:
        resumed_dataset = dataset_from_reference(resume_checkpoint["candidate_store"],
                                                 resume_checkpoint.get("candidate_store_embedded"))
        all_rows = sorted([copy.deepcopy(row) for row in resumed_dataset["candidates"]
                           if row.get("status") in DATA_TERMINAL_STATES],
                          key=lambda row: (row["generation"], row["index"]))
    elif resume_checkpoint is not None:
        _seed_checkpoint_candidates(run_root, all_rows)
    data_store = RunDataStore(os.path.join(run_root, CORE_NAME), metadata={
        "run_id": os.path.basename(run_root), "display_name": display_name or os.path.basename(run_root),
        "spec": copy.deepcopy(spec), "parameters": copy.deepcopy(selected),
        "outputs": copy.deepcopy(prepared.get("outputs") or []),
        "source_identity": copy.deepcopy(prepared.get("source_identity")),
        "execution_backend": spec["execution_backend"],
        "simulator": spec["simulator"],
        "continuation_id": continuation_id,
    })
    if all_rows:
        data_store.commit_candidates(all_rows)
    planned_core_ids = set(row["candidate_id"] for row in all_rows)
    point_commit_queue = queue.Queue()
    status = {
        "schema_version": "crescendo.run_status.v1",
        "run_id": os.path.basename(run_root),
        "run_root": run_root,
        "display_name": display_name or os.path.basename(run_root),
        "continuation_id": continuation_id,
        "state": "running",
        "started_at": utc_timestamp(),
        "generation": 0,
        "total_generations": generations,
        "completed_candidates": len([row for row in all_rows if row["status"] == "completed"]),
        "failed_candidates": len([row for row in all_rows if row["status"] == "failed"]),
        "infrastructure_failed_candidates": len([row for row in all_rows if row["status"] == "infrastructure_failed"]),
        "total_candidates": generations * population + len([
            row for row in all_rows if row["status"] == "infrastructure_failed"
        ]),
        "message": "checkpoint restored" if resume_checkpoint is not None else "preparation complete",
        "main_log_file": os.path.join(run_root, "crescendo.log"),
        "algorithm": algorithm,
        "algorithm_parameters": algorithm_parameters,
        "execution_backend": spec["execution_backend"],
        "simulator": spec["simulator"],
        "legacy_guardian_protocol": spec.get("legacy_guardian_protocol"),
        **({"environment_test": spec["environment_test"]} if spec.get("environment_test") else {}),
        "checkpoint_revision": int(
            ((resume_checkpoint or {}).get("state") or {}).get("checkpoint_revision", 0)
        ),
        "startup_storage_preflight": startup_preflight,
        "resource_guard_restart_count": int(
            ((resume_checkpoint or {}).get("state") or {}).get(
                "resource_guard_restart_count", 0,
            )
        ),
        "resource_guard_recycles": copy.deepcopy(
            ((resume_checkpoint or {}).get("state") or {}).get(
                "resource_guard_recycles", [],
            )
        )[-20:],
    }
    if resume_checkpoint is not None:
        status["resumed_from"] = resume_checkpoint.get("created_at")
    atomic_write_json(status_path, status)
    generator = PopulationGenerator(
        variables, algorithm, algorithm_parameters["seed"], parameters=algorithm_parameters,
    )
    selection_history = []
    start_generation = 0
    active_plan = None
    checkpoint_ancestry = []
    checkpoint_parent_checksum = None
    resume_terminal_state = None
    resume_terminal_reason = None
    terminal_checkpoint_written = False
    if resume_checkpoint is not None:
        checkpoint_state = resume_checkpoint.get("state") or {}
        selection_history = list(checkpoint_state.get("selection_history") or [])
        start_generation = int(checkpoint_state.get("next_generation", 0))
        active_plan = _unpack_plan(checkpoint_state.get("active_plan"), [row["parameter_id"] for row in variables])
        generator_state = checkpoint_state.get("generator") or {}
        if generator_state.get("rng_state") is not None:
            _restore_generator(generator, generator_state, all_rows)
        checkpoint_ancestry = list(checkpoint_state.get("ancestor_payload_sha256") or [])
        checkpoint_parent_checksum = _checkpoint_checksum(resume_checkpoint)
        resume_terminal_state = checkpoint_state.get("terminal_state")
        resume_terminal_reason = checkpoint_state.get("terminal_reason")
        terminal_checkpoint_written = resume_terminal_state == "completed"
    stopped_reason = resume_terminal_reason if resume_terminal_state == "completed" else None
    execution_start_generation = generations if resume_terminal_state == "completed" else start_generation
    netlist_optimizer = None
    if netlist_backend:
        origin_root = ((resume_checkpoint or {}).get("manifest") or {}).get("origin_run_root")
        prior_roots = []
        if origin_root and os.path.abspath(origin_root) != run_root:
            if not os.path.isabs(origin_root) or os.path.realpath(origin_root) != origin_root:
                raise ValueError("private netlist checkpoint origin is not an independent absolute run directory")
            prior_roots.append({
                "owned_run_root": origin_root,
                "ownership_root": os.path.join(origin_root, "private_netlist", "execution_ownership"),
            })
        netlist_optimizer = PrivateNetlistOptimizer(
            run_root, spec, prepared, selected, config, _failure_metrics,
            cancel_check=lambda: os.path.exists(stop_path), prior_ownership_roots=prior_roots,
            environment_check=_private_netlist_environment_check(prepared, config),
        )
    jobs = (netlist_optimizer.parallel_candidates if netlist_optimizer is not None else
            min(max(1, int(spec.get("jobs", 1))), int(config["runtime"].get("max_parallel_jobs", 5))))
    requested_jobs = max(1, int(spec.get("jobs", 1))) if baseline_backend else jobs
    if baseline_backend:
        jobs = 1
    persistent_jobs = 1 if persistent_backend else jobs
    requested_persistent_worker_lifetime = str(
        config["runtime"].get("persistent_worker_lifetime", "run")
    ).strip().lower()
    if requested_persistent_worker_lifetime not in ("run", "batch"):
        raise ValueError("runtime.persistent_worker_lifetime must be run or batch")
    persistent_worker_lifetime = "run"
    persistent_startup_limit = persistent_jobs * (
        max(0, int(config["runtime"].get("infrastructure_retries", 1))) + 1
    )
    status["requested_parallel_jobs"] = requested_jobs
    status["effective_parallel_jobs"] = persistent_jobs if persistent_backend else jobs
    status["maestro_candidate_execution"] = copy.deepcopy(execution_plan)
    status["maestro_max_jobs"] = execution_plan["max_jobs"]
    if baseline_backend:
        status["execution_protocol"] = spec["execution_protocol"]
        status["parallel_execution_policy"] = "serialized_owned_maestro_baseline"
    if netlist_optimizer is not None:
        status["parallel_execution_policy"] = "shared_simulator_task_pool"
        status["spectre_task_policy"] = copy.deepcopy(netlist_optimizer.policy)
        status["simulator_task_policy"] = copy.deepcopy(netlist_optimizer.policy)
        status["maestro_max_jobs"] = netlist_optimizer.policy["max_jobs"]
        status["private_netlist_protocol"] = "crescendo.private_netlist_optimizer.v1"
        status["execution_protocol"] = "crescendo.unified_execution.v1"
        status["netlist_source"] = "template" if spec["execution_backend"] == "private_netlist" else "maestro"
    if persistent_backend:
        status["persistent_worker_lifetime"] = "optimization_run"
        status["persistent_worker_count"] = 1
        status["persistent_session_policy"] = "single_maestro_for_optimization_run"
        status["parallel_execution_policy"] = (
            "native_parallel_candidates_single_maestro" if batch_enabled else "serialized_single_maestro"
        )
        if jobs != 1:
            status["parallel_jobs_override"] = {
                "requested": jobs, "effective": 1,
                "reason": "real EDA uses one run-lifetime Maestro session",
            }
        if requested_persistent_worker_lifetime != "run":
            status["worker_lifetime_override"] = {
                "requested": requested_persistent_worker_lifetime,
                "effective": "run",
                "reason": "scheduled batch restarts are disabled",
            }
    atomic_write_json(status_path, status)
    configured_replacement_limit = int(
        config["runtime"].get("max_infrastructure_replacements", 0)
    )
    if configured_replacement_limit < 0:
        raise ValueError("runtime.max_infrastructure_replacements must be non-negative")
    replacement_limit = (0 if _environment_test_run(spec)
                         else configured_replacement_limit or population)
    scheduler_watchdog_grace = max(
        0.0, float(config["runtime"].get("scheduler_watchdog_grace_seconds", 30)),
    )
    persistent_workers = []
    baseline_candidate_roots = set()
    pending_result = None
    pending_assignment = None
    checkpoint_variable_ids = [row["parameter_id"] for row in variables]

    def publish_result_summary(result):
        try:
            reference = data_store.reference()
        except Exception:
            reference = None
        atomic_write_json(os.path.join(run_root, "result.json"), compact_result_summary(result, reference))
    identity_guard_slots = _PersistentIdentityGuardSlots()
    session_health_guards = identity_guard_slots.boundary
    active_session_health_guards = identity_guard_slots.active
    runtime_config = config.get("runtime") or {}
    try:
        active_health_interval = float(runtime_config.get(
            "persistent_active_health_interval_seconds", 2.0,
        ))
    except (TypeError, ValueError, OverflowError):
        raise ValueError("persistent_active_health_interval_seconds must be a finite number")
    if not math.isfinite(active_health_interval) or active_health_interval < 0.5:
        raise ValueError(
            "persistent_active_health_interval_seconds must be finite and at least 0.5"
        )
    try:
        resource_guard_restart_limit = int(runtime_config.get(
            "persistent_resource_guard_max_restarts", 8,
        ))
    except (TypeError, ValueError, OverflowError):
        raise ValueError("persistent_resource_guard_max_restarts must be a positive integer")
    if persistent_backend and resource_guard_restart_limit < 1:
        raise ValueError("persistent_resource_guard_max_restarts must be a positive integer")
    resource_guard_restart_limit = max(1, resource_guard_restart_limit)
    try:
        configured_identity_startup_timeout = float(runtime_config.get(
            "persistent_identity_startup_timeout_seconds",
            runtime_config.get("ocean_startup_timeout_seconds", 90),
        ))
        ocean_startup_timeout = float(runtime_config.get(
            "ocean_startup_timeout_seconds", 90,
        ))
    except (TypeError, ValueError, OverflowError):
        raise ValueError(
            "persistent_identity_startup_timeout_seconds must be a finite positive number"
        )
    if (
            not math.isfinite(configured_identity_startup_timeout)
            or configured_identity_startup_timeout <= 0
            or not math.isfinite(ocean_startup_timeout) or ocean_startup_timeout <= 0):
        raise ValueError(
            "persistent_identity_startup_timeout_seconds must be a finite positive number"
        )
    identity_startup_timeout = max(
        configured_identity_startup_timeout, ocean_startup_timeout,
    )
    try:
        workspace_prepare_timeout = float(runtime_config.get(
            "persistent_workspace_prepare_timeout_seconds", 1800,
        ))
        startup_total_timeout = float(runtime_config.get(
            "persistent_startup_total_timeout_seconds",
            workspace_prepare_timeout + identity_startup_timeout,
        ))
    except (TypeError, ValueError, OverflowError):
        raise ValueError("persistent startup phase timeouts must be finite positive numbers")
    if (
            not math.isfinite(workspace_prepare_timeout) or workspace_prepare_timeout <= 0
            or not math.isfinite(startup_total_timeout) or startup_total_timeout <= 0):
        raise ValueError("persistent startup phase timeouts must be finite positive numbers")

    def new_session_health_guard():
        try:
            return PersistentSessionHealthGuard(
                warmup_samples=runtime_config.get("persistent_health_warmup_samples", 5),
                baseline_samples=runtime_config.get("persistent_health_baseline_samples", 3),
                consecutive_limit=runtime_config.get("persistent_health_consecutive_limit", 3),
                rss_growth_limit_kib=int(float(
                    runtime_config.get("persistent_rss_growth_limit_mib", 512)
                ) * 1024),
                fd_growth_limit=runtime_config.get("persistent_fd_growth_limit", 128),
                min_free_disk_mib=runtime_config.get("persistent_min_free_disk_mib", 1024),
                log_growth_limit_bytes=int(float(
                    runtime_config.get("persistent_log_growth_limit_mib", 256)
                ) * 1024 * 1024),
            )
        except (TypeError, ValueError, OverflowError):
            raise ValueError("persistent Maestro health limits must be finite positive numbers")

    def controller_health_identity(worker, health):
        process = getattr(worker, "process", None)
        expected = (
            getattr(worker, "epoch_index", None),
            getattr(process, "pid", None),
            getattr(worker, "_start_ticks", None),
        )
        observed = (
            health.get("controller_epoch"), health.get("pid"),
            health.get("pid_start_ticks"),
        )
        if (
                any(isinstance(value, bool) or value is None for value in expected)
                or observed != expected):
            return None
        return expected

    def session_health_identity(worker, health):
        controller_identity = controller_health_identity(worker, health)
        session = health.get("session_identity")
        if (
                controller_identity is None
                or health.get("session_identity_proven") is not True
                or not isinstance(session, dict) or session.get("proven") is not True):
            return None
        expected = {
            "worker_index": worker.worker_index,
            "controller_epoch": controller_identity[0],
        }
        observed = {
            "worker_index": session.get("worker_index"),
            "controller_epoch": session.get("controller_epoch"),
        }
        epoch = session.get("epoch")
        session_pid = session.get("pid")
        session_start_ticks = session.get("pid_start_ticks")
        if (
                observed != expected or not isinstance(epoch, str) or not epoch
                or isinstance(session_pid, bool) or not isinstance(session_pid, int)
                or session_pid <= 1 or isinstance(session_start_ticks, bool)
                or not isinstance(session_start_ticks, int) or session_start_ticks <= 0):
            return None
        return {
            "key": controller_identity + (epoch, session_pid, session_start_ticks),
            "controller_key": controller_identity,
            "controller_epoch": controller_identity[0],
            "worker_epoch": epoch,
            "controller_pid": controller_identity[1],
            "controller_pid_start_ticks": controller_identity[2],
            "session_pid": session_pid,
            "session_pid_start_ticks": session_start_ticks,
        }

    def unproven_identity_health(health):
        observed = dict(health)
        observed.update({
            "session_identity_complete": False,
            "rss_kib": None, "rss_kb": None, "fd_count": None,
            "max_fd_count": None, "log_bytes": None,
        })
        return observed

    def pending_identity_health(health):
        observed = dict(health)
        observed.update({
            "session_identity_complete": True,
            "rss_kib": None, "rss_kb": None, "fd_count": None,
            "max_fd_count": None,
        })
        return observed

    def public_identity_failure_reason(health):
        session = health.get("session_identity") if isinstance(health, dict) else None
        raw = str((session or {}).get("reason") or "").lower()
        reasons = (
            ("outside the active controller namespace", "foreign_worker_epoch",
             "worker epochs outside the active controller namespace exist"),
            ("expected exactly one active worker epoch", "worker_epoch_not_materialized",
             "exactly one active worker epoch is not yet available"),
            ("state identity mismatch", "worker_state_identity_mismatch",
             "active worker state identity does not match its controller namespace"),
            ("session is not reusable", "worker_session_not_reusable",
             "active worker session is not in a reusable state"),
            ("process identity is incomplete", "worker_process_identity_incomplete",
             "active worker process identity is incomplete"),
            ("not in the owned controller tree", "worker_process_not_owned",
             "active worker process is not in the owned controller tree"),
            ("authenticated worker health did not match", "worker_health_identity_mismatch",
             "authenticated worker health does not match the active session"),
            ("no such file or directory", "worker_state_not_materialized",
             "active worker identity files are not yet available"),
        )
        for fragment, code, message in reasons:
            if fragment in raw:
                return {"code": code, "message": message}
        return {
            "code": "session_identity_unproven",
            "message": "active worker session identity could not be proven",
        }

    def new_identity_guard_entry(identity, controller_identity, observed_at,
                                 authorized=False):
        return {
            "identity": identity,
            "controller_identity": controller_identity,
            "guard": new_session_health_guard(),
            "authorized": bool(authorized),
            "identity_ever_proven": identity is not None,
            "identity_pending_since": None,
            "initial_startup": identity is None,
        }

    def expect_session_replacement(worker):
        process = getattr(worker, "process", None)
        controller_identity = (
            getattr(worker, "epoch_index", None),
            getattr(process, "pid", None),
            getattr(worker, "_start_ticks", None),
        )
        if any(isinstance(value, bool) or value is None for value in controller_identity):
            return False
        identity_guard_slots.expect_replacement(
            worker.worker_index, controller_identity, new_identity_guard_entry,
        )
        return True

    def begin_expected_session_replacement(worker, observed_at):
        return identity_guard_slots.begin_expected_replacement(
            worker.worker_index, observed_at,
        )

    def observe_identity_pending(guard_entry, health, observed_at,
                                 start_deadline=True):
        decision = guard_entry["guard"].observe(pending_identity_health(health))
        started_at = guard_entry.get("identity_pending_since")
        if started_at is None:
            if not start_deadline and guard_entry.get("expected_replacement") is True:
                decision.update({
                    "state": "replacement_waiting_for_evaluate",
                    "triggered": False,
                    "identity_pending_seconds": 0.0,
                    "identity_startup_timeout_seconds": identity_startup_timeout,
                    "identity_unproven_reason": public_identity_failure_reason(health),
                })
                return decision
            guard_entry["identity_pending_since"] = observed_at
            started_at = observed_at
        elapsed = max(0.0, observed_at - started_at)
        phase = health.get("startup_phase")
        phase_proven = (
            isinstance(phase, dict) and phase.get("proven") is True
            and phase.get("controller_epoch") == health.get("controller_epoch")
            and phase.get("controller_pid") == health.get("pid")
            and phase.get("controller_pid_start_ticks") == health.get("pid_start_ticks")
        )
        timeout_state = None
        timeout_violation = None
        public_phase = "identity_starting"
        phase_limit = identity_startup_timeout
        total_elapsed = elapsed
        if phase_proven:
            phase_key = (
                phase.get("controller_epoch"), phase.get("controller_pid"),
                phase.get("controller_pid_start_ticks"),
                phase.get("request_sequence"), phase.get("request_nonce"),
            )
            previous_key = guard_entry.get("startup_request_identity")
            if previous_key is not None and previous_key != phase_key:
                timeout_state = "startup_request_identity_changed"
                timeout_violation = "startup_request_identity_changed"
            else:
                guard_entry["startup_request_identity"] = phase_key
                total_started = min(
                    started_at, phase["request_started_monotonic"],
                    phase["started_monotonic"],
                )
                previous_total = guard_entry.get("startup_total_started")
                if previous_total is not None:
                    total_started = min(total_started, previous_total)
                guard_entry["startup_total_started"] = total_started
                total_elapsed = max(0.0, observed_at - total_started)
                first_ocean = guard_entry.get("startup_ocean_started")
                phase_ocean = phase.get("first_ocean_started_monotonic")
                if phase_ocean is not None:
                    first_ocean = min(first_ocean, phase_ocean) if first_ocean is not None else phase_ocean
                    guard_entry["startup_ocean_started"] = first_ocean
                public_phase = phase.get("stage")
                if first_ocean is None and public_phase == "workspace_prepare":
                    elapsed = total_elapsed
                    phase_limit = workspace_prepare_timeout
                    if elapsed >= phase_limit:
                        timeout_state = "workspace_prepare_timeout"
                        timeout_violation = "workspace_prepare_timeout"
                else:
                    # A repeated preparation marker cannot reset the first Ocean deadline.
                    first_ocean = first_ocean if first_ocean is not None else total_started
                    elapsed = max(0.0, observed_at - first_ocean)
                    if elapsed >= identity_startup_timeout:
                        timeout_state = "identity_startup_timeout"
                        timeout_violation = "session_identity_startup_timeout"
                if total_elapsed >= startup_total_timeout:
                    timeout_state = "startup_total_timeout"
                    timeout_violation = "persistent_startup_total_timeout"
        elif elapsed >= identity_startup_timeout:
            timeout_state = "identity_startup_timeout"
            timeout_violation = "session_identity_startup_timeout"
        if timeout_state is not None:
            violations = list(decision.get("violations") or [])
            if timeout_violation not in violations:
                violations.append(timeout_violation)
            decision.update({
                "state": timeout_state,
                "triggered": True,
                "violations": violations,
            })
        elif not decision.get("triggered"):
            decision.update({
                "state": "workspace_preparing" if public_phase == "workspace_prepare" else "identity_pending",
                "triggered": False,
            })
        decision.update({
            "identity_pending_seconds": elapsed,
            "identity_startup_timeout_seconds": identity_startup_timeout,
            "identity_unproven_reason": public_identity_failure_reason(health),
            "startup_phase": public_phase,
            "startup_phase_authenticated": phase_proven,
            "startup_phase_timeout_seconds": phase_limit,
            "startup_total_seconds": total_elapsed,
            "startup_total_timeout_seconds": startup_total_timeout,
        })
        return decision

    def observe_unexpected_identity_change(guard_entry, health, identity, violation):
        decision = guard_entry["guard"].observe(unproven_identity_health(health))
        violations = list(decision.get("violations") or [])
        if violation not in violations:
            violations.append(violation)
        decision.update({
            "state": violation,
            "triggered": True,
            "violations": violations,
            "identity_unproven_reason": {
                "code": violation,
                "message": "session identity changed without an authorized replacement",
            },
            "previous_session_identity": public_session_identity(
                guard_entry.get("identity")
            ),
            "observed_session_identity": public_session_identity(identity),
        })
        return decision

    def public_cleanup_result(cleanup_result):
        public = copy.deepcopy(cleanup_result)
        health = public.get("health") if isinstance(public, dict) else None
        session = health.get("session_identity") if isinstance(health, dict) else None
        if isinstance(session, dict) and session.get("reason"):
            reason = public_identity_failure_reason(health)
            session["reason"] = reason["message"]
            session["reason_code"] = reason["code"]
        return public

    def public_session_identity(identity):
        if identity is None:
            return None
        return dict((key, value) for key, value in identity.items() if key != "key")

    def observe_session_health(worker, cleanup_result):
        if runtime_config.get("persistent_health_enabled", True) is False:
            return {
                "schema_version": "crescendo.persistent_session_health.v1",
                "state": "disabled", "triggered": False,
            }
        health = (cleanup_result or {}).get("health")
        if not isinstance(health, dict):
            return None
        observed_at = time.monotonic()
        controller_identity = controller_health_identity(worker, health)
        identity = session_health_identity(worker, health)
        guard_entry = session_health_guards.get(worker.worker_index)
        transition = "same_session"
        expected_replacement_transition = False
        forced_decision = None
        if guard_entry is None:
            guard_entry = new_identity_guard_entry(
                identity, controller_identity, observed_at,
            )
            session_health_guards[worker.worker_index] = guard_entry
            transition = "initial_session" if identity is not None else "initial_startup"
        elif (
                controller_identity is not None
                and guard_entry.get("controller_identity") != controller_identity):
            transition = "unexpected_controller_identity_change"
            forced_decision = observe_unexpected_identity_change(
                guard_entry, health, identity, transition,
            )
        elif identity is not None and guard_entry.get("identity") is None:
            expected_replacement_transition = bool(guard_entry.get("expected_replacement"))
            if expected_replacement_transition or guard_entry.get("initial_startup") is True:
                transition = (
                    "expected_replacement_proven"
                    if expected_replacement_transition else "initial_session"
                )
                guard_entry = new_identity_guard_entry(
                    identity, controller_identity, observed_at,
                )
                session_health_guards[worker.worker_index] = guard_entry
            else:
                transition = "unexpected_session_identity_change"
                forced_decision = observe_unexpected_identity_change(
                    guard_entry, health, identity, transition,
                )
        elif identity is not None and (
                guard_entry["identity"].get("key") != identity.get("key")):
            transition = "unexpected_session_identity_change"
            forced_decision = observe_unexpected_identity_change(
                guard_entry, health, identity, transition,
            )
        if forced_decision is not None:
            decision = forced_decision
        elif identity is None and not guard_entry.get("identity_ever_proven"):
            decision = observe_identity_pending(
                guard_entry, health, observed_at, start_deadline=False,
            )
        elif identity is None:
            transition = "identity_unproven"
            decision = guard_entry["guard"].observe(unproven_identity_health(health))
            decision["identity_unproven_reason"] = public_identity_failure_reason(health)
        else:
            decision = guard_entry["guard"].observe(health)
        decision["controller_epoch"] = getattr(worker, "epoch_index", None)
        decision["worker_index"] = worker.worker_index
        decision["identity_proven"] = identity is not None
        decision["identity_transition"] = transition
        decision["session_identity"] = public_session_identity(identity)
        decision["expected_replacement"] = bool(
            guard_entry.get("expected_replacement") or expected_replacement_transition
        )
        return decision

    def authorize_active_session_health(worker, cleanup_result, boundary_decision):
        if runtime_config.get("persistent_health_enabled", True) is False:
            return
        health = (cleanup_result or {}).get("health")
        if not isinstance(health, dict):
            return
        if not (
            health.get("alive") is True and health.get("tree_complete") is not False
            and health.get("storage_complete") is not False
            and health.get("logs_complete") is not False
        ):
            return
        identity = session_health_identity(worker, health)
        if (
                identity is None or not isinstance(boundary_decision, dict)
                or boundary_decision.get("identity_proven") is not True
                or (boundary_decision.get("session_identity") or {}).get("worker_epoch")
                != identity.get("worker_epoch")):
            return
        guard_entry = active_session_health_guards.get(worker.worker_index)
        if (
                guard_entry is not None and guard_entry.get("authorized") is True
                and guard_entry.get("identity") is not None
                and guard_entry["identity"].get("key") == identity.get("key")):
            return
        guard = new_session_health_guard()
        seed_decision = guard.observe(health, comparable=False)
        guard_entry = new_identity_guard_entry(
            identity, identity.get("controller_key"), time.monotonic(), authorized=True,
        )
        guard_entry.update({"guard": guard, "seed_decision": seed_decision})
        active_session_health_guards[worker.worker_index] = guard_entry

    def observe_active_session_health(worker, health):
        if runtime_config.get("persistent_health_enabled", True) is False:
            return None
        controller_identity = controller_health_identity(worker, health)
        identity = session_health_identity(worker, health)
        observed_at = time.monotonic()
        guard_entry = active_session_health_guards.get(worker.worker_index)
        transition = "same_session"
        identity_unproven = False
        expected_replacement_transition = False
        forced_decision = None
        if guard_entry is None:
            guard_entry = new_identity_guard_entry(
                identity, controller_identity, observed_at,
            )
            active_session_health_guards[worker.worker_index] = guard_entry
            transition = "initial_session" if identity is not None else "initial_startup"
        elif (
                controller_identity is not None
                and guard_entry.get("controller_identity") != controller_identity):
            transition = "unexpected_controller_identity_change"
            forced_decision = observe_unexpected_identity_change(
                guard_entry, health, identity, transition,
            )
        elif identity is not None and (
                guard_entry.get("identity") is None
                or guard_entry["identity"].get("key") != identity.get("key")):
            prior_identity = guard_entry.get("identity")
            expected_replacement = guard_entry.get("expected_replacement") is True
            expected_replacement_transition = expected_replacement
            if expected_replacement or (
                    prior_identity is None and guard_entry.get("initial_startup") is True):
                guard_entry = new_identity_guard_entry(
                    identity, controller_identity, observed_at,
                )
                active_session_health_guards[worker.worker_index] = guard_entry
                transition = (
                    "expected_replacement_proven" if expected_replacement
                    else "initial_session_proven"
                )
            else:
                transition = "unexpected_session_identity_change"
                forced_decision = observe_unexpected_identity_change(
                    guard_entry, health, identity, transition,
                )
        elif identity is None and (
                guard_entry.get("identity") is not None
                or guard_entry.get("authorized") is True):
            transition = "identity_unproven"
            identity_unproven = True
        if forced_decision is not None:
            decision = forced_decision
        elif identity is None and not guard_entry.get("identity_ever_proven"):
            decision = observe_identity_pending(guard_entry, health, observed_at)
        else:
            observed_health = health
            if identity_unproven:
                observed_health = unproven_identity_health(health)
            elif not guard_entry["authorized"]:
                observed_health = pending_identity_health(health)
            decision = guard_entry["guard"].observe(
                observed_health,
                comparable=bool(identity_unproven or not guard_entry["authorized"]),
            )
            if identity_unproven:
                decision["identity_unproven_reason"] = public_identity_failure_reason(health)
        decision.update({
            "controller_epoch": (
                controller_identity[0] if controller_identity is not None else None
            ),
            "worker_index": worker.worker_index,
            "phase": "active_evaluate",
            "resource_limits_authorized": bool(guard_entry["authorized"]),
            "identity_proven": identity is not None,
            "identity_transition": transition,
            "session_identity": public_session_identity(identity),
            "expected_replacement": bool(
                guard_entry.get("expected_replacement") or expected_replacement_transition
            ),
        })
        if health.get("sampling_error"):
            decision["sampling_error"] = health["sampling_error"]
        return decision

    anchor_root = checkpoint_anchor_root or os.path.dirname(run_root)

    def persist_checkpoint(path, document, description):
        if os.path.lexists(path) and (os.path.islink(path) or not os.path.isfile(path)):
            raise RuntimeError("%s is not a safe regular file" % description)
        validate_checkpoint_integrity(document)
        atomic_write_text(
            path, json.dumps(document, separators=(",", ":"), sort_keys=True) + "\n",
        )
        if os.path.islink(path) or not os.path.isfile(path):
            raise RuntimeError("%s was not persisted safely" % description)
        persisted = load_json(path)
        validate_checkpoint_integrity(persisted)
        if persisted.get("payload_sha256") != document.get("payload_sha256"):
            raise RuntimeError("%s verification failed" % description)

    def begin_resource_guard_recycle(generation, plan, worker, candidate_id,
                                     phase, decision):
        restart_count = int(status.get("resource_guard_restart_count", 0))
        exhausted = restart_count >= resource_guard_restart_limit
        if not exhausted:
            restart_count += 1
            status["resource_guard_restart_count"] = restart_count
        event = {
            "candidate_id": candidate_id,
            "worker_index": worker.worker_index,
            "phase": phase,
            "decision": decision,
            "requested_at": utc_timestamp(),
            "restart_number": restart_count,
            "restart_limit": resource_guard_restart_limit,
            "restart_budget_exhausted": exhausted,
            "state": "restart_limit_exhausted" if exhausted else "checkpointed",
        }
        status.setdefault("resource_guard_recycles", []).append(event)
        status["resource_guard_recycles"] = status["resource_guard_recycles"][-20:]
        if phase == "active_candidate":
            status.setdefault("active_resource_guard_terminations", []).append(event)
            status["active_resource_guard_terminations"] = (
                status["active_resource_guard_terminations"][-20:]
            )
        status["persistent_session_health"] = decision
        status["message"] = (
            "persistent Maestro resource guard restart limit exhausted; "
            "checkpoint saved without failing the candidate"
            if exhausted else
            "persistent Maestro resource guard exceeded; checkpoint saved before clean restart"
        )
        atomic_write_json(status_path, status)
        save_checkpoint(generation, plan)
        return event, exhausted

    def resource_guard_is_recyclable(decision, phase):
        violations = set((decision or {}).get("violations") or [])
        recyclable = set((
            "rss_growth", "fd_growth", "fd_per_process_limit", "log_growth",
        ))
        if not violations or not violations.issubset(recyclable):
            return False
        if (decision or {}).get("identity_proven") is not True:
            return False
        if phase == "active_candidate" and (
                decision or {}).get("resource_limits_authorized") is not True:
            return False
        return True

    def terminate_resource_guard_epoch(worker, event):
        termination = worker.terminate_and_prove(
            "persistent Maestro resource guard recycle for %s" % event["candidate_id"]
        )
        event["termination"] = termination
        event["terminated_at"] = utc_timestamp()
        if not (
                termination.get("termination_proven") is True
                and termination.get("process_still_alive") is False
                and (termination.get("cleanup") or {}).get("ok") is True):
            event["state"] = "termination_unproven"
            atomic_write_json(status_path, status)
            raise RuntimeError(
                "resource guard could not prove owned controller cleanup: %s" % termination
            )
        source_guard = (
            _reverify_prepared_source_identity(prepared, config)
            if prepared.get("source_identity") is not None else
            {"ok": True, "state": "source_identity_not_recorded"}
        )
        event["source_guard"] = source_guard
        if not source_guard.get("ok"):
            event["state"] = "source_integrity_failed"
            atomic_write_json(status_path, status)
            raise RuntimeError(
                source_guard.get("error") or
                "source identity changed during resource guard recycle"
            )
        event["state"] = (
            "terminated_restart_limit_exhausted"
            if event.get("restart_budget_exhausted") else "terminated"
        )
        atomic_write_json(status_path, status)
        return termination

    def restart_resource_guard_epoch(generation, plan, worker, event):
        if os.path.exists(stop_path):
            event["state"] = "stopped_after_termination"
            status["message"] = "user_stop"
            atomic_write_json(status_path, status)
            save_checkpoint(generation, plan)
            return False
        worker.restart()
        if not expect_session_replacement(worker):
            event["state"] = "replacement_identity_unproven"
            atomic_write_json(status_path, status)
            raise RuntimeError(
                "resource guard replacement controller identity could not be established"
            )
        event["state"] = "restarted"
        event["restarted_at"] = utc_timestamp()
        event["controller_epoch"] = getattr(worker, "epoch_index", None)
        status["message"] = (
            "persistent Maestro restarted after resource guard; continuing optimization"
        )
        atomic_write_json(status_path, status)
        save_checkpoint(generation, plan)
        return True

    def save_checkpoint(next_generation, plan, terminal_state=None, terminal_reason=None):
        nonlocal checkpoint_ancestry, checkpoint_parent_checksum, terminal_checkpoint_written
        status["checkpoint_revision"] = int(status.get("checkpoint_revision", 0)) + 1
        ancestry = list(checkpoint_ancestry)
        if checkpoint_parent_checksum and (
            not ancestry or ancestry[-1] != checkpoint_parent_checksum
        ):
            ancestry.append(checkpoint_parent_checksum)
        if plan is not None:
            planned_rows = []
            for index, values in plan.get("planned") or []:
                identity = "g%03d_c%04d" % (plan["generation"], index)
                if identity not in planned_core_ids:
                    planned_rows.append({"candidate_id": identity, "generation": plan["generation"],
                                         "index": index, "values": values, "status": "planned",
                                         "execution_backend": spec["execution_backend"]})
                    planned_core_ids.add(identity)
            if planned_rows:
                data_store.commit_candidates(planned_rows)
        document = _checkpoint_document(
            spec, prepared, generator, all_rows, selection_history, next_generation, plan, status,
            ancestry=ancestry, terminal_state=terminal_state,
            terminal_reason=terminal_reason,
            candidate_layout=(resume_checkpoint or {}).get("candidate_layout"),
            candidate_store=data_store.reference(),
        )
        startup_storage_preflight(
            run_root, anchor_root,
            runtime_config.get("persistent_min_free_disk_mib", 1024),
            checkpoint=document, workspace_required=False,
        )
        anchor_path = continuation_checkpoint_path(
            anchor_root, continuation_id, create=True,
        )
        anchored_checksum = None
        if os.path.lexists(anchor_path):
            if os.path.islink(anchor_path) or not os.path.isfile(anchor_path):
                raise RuntimeError("continuation checkpoint anchor is not a safe regular file")
            anchored = load_json(anchor_path)
            validate_checkpoint_integrity(anchored)
            if (
                checkpoint_continuation_id(anchored) != continuation_id
                or checkpoint_continuation_schema(anchored)
                != checkpoint_continuation_schema(document)
                or not checkpoint_model_profile_transition(anchored, document)
            ):
                raise RuntimeError(
                    "continuation checkpoint anchor belongs to a different optimization"
                )
            anchored_checksum = _checkpoint_checksum(anchored)
            if checkpoint_parent_checksum is None:
                if anchored_checksum != document.get("payload_sha256"):
                    raise RuntimeError("continuation checkpoint anchor appeared unexpectedly")
            elif anchored_checksum != checkpoint_parent_checksum:
                raise RuntimeError(
                    "continuation checkpoint anchor changed outside the held lease"
                )
        pending_path = continuation_pending_path(
            anchor_root, continuation_id, create=True,
        )
        if os.path.lexists(pending_path):
            raise RuntimeError(
                "an unresolved continuation pending journal must be recovered before optimization"
            )
        pending = build_continuation_pending(
            document, continuation_id,
            int((document.get("state") or {}).get("checkpoint_revision", 0)),
            anchored_checksum,
            continuation_schema_sha256(checkpoint_continuation_schema(document)),
        )
        persist_continuation_pending(anchor_root, continuation_id, pending)
        persist_checkpoint(
            os.path.join(run_root, "checkpoint.json"), document, "run checkpoint",
        )
        persist_checkpoint(anchor_path, document, "continuation checkpoint anchor")
        clear_continuation_pending(anchor_root, continuation_id)
        checkpoint_ancestry = ancestry
        checkpoint_parent_checksum = document["payload_sha256"]
        if terminal_state == "completed":
            terminal_checkpoint_written = True

    live_candidate_rows = {}
    live_batch_snapshots = {}

    def update_live_batch(planned, generation, initializing=False, serial=False):
        from .maestro_progress import read_batch_progress
        ids = ['g%03d_c%04d' % (generation, index) for index, _values in planned]
        path = (os.path.join(run_root, 'candidates', ids[0], 'execution_progress.json') if serial else
                os.path.join(run_root, 'batches', 'g%03d_%s' % (generation, ids[0]), 'execution_progress.json'))
        snapshot = read_batch_progress(path, run_root, ids)
        key = json.dumps(snapshot, sort_keys=True) if snapshot else None
        if not initializing and (snapshot is None or live_batch_snapshots.get(path) == key):
            return
        supplied = dict((row['candidate_id'], row.get('execution_progress') or {})
                        for row in (snapshot or {}).get('candidates') or [])
        rows = []
        for index, values in planned:
            cid = 'g%03d_c%04d' % (generation, index)
            progress = copy.deepcopy(supplied.get(cid) or {'stage': 'preparing', 'tasks': []})
            previous = live_candidate_rows.get(cid)
            row = {'candidate_id': cid, 'generation': generation, 'index': index,
                   'values': copy.deepcopy(values), 'status': 'running' if progress.get('stage') in
                   ('running', 'evaluating') else 'queued', 'execution_backend': 'persistent_maestro',
                   'execution_progress': progress, 'source_test_map': progress.get('source_test_map') or {},
                   'spectre_state': 'started' if progress.get('stage') in ('running', 'evaluating') or any(task.get('state') in ('running', 'finished') for task in progress.get('tasks') or []) else 'not_started'}
            if previous is not None and previous == row:
                continue
            live_candidate_rows[cid] = row
            rows.append(row)
            append_run_event(run_root, 'candidate', progress.get('stage') or row['status'],
                {'candidate_id': cid, 'execution_backend': 'persistent_maestro',
                 'status': row['status'], 'execution_progress': progress})
        if rows:
            data_store.commit_candidates(rows)
        if snapshot:
            live_batch_snapshots[path] = key
            status['maestro_live_progress'] = {'batch_id': snapshot['batch_id'],
                'observed_at': snapshot.get('observed_at'), 'progress_path': path,
                'shared_submissions': snapshot.get('shared_submissions') or [],
                'candidates': dict((row['candidate_id'], (row.get('execution_progress') or {}).get('stage'))
                                   for row in snapshot['candidates'])}
            atomic_write_json(status_path, status)

    def request_point_commit(point, cancel_check):
        request = {"point": point, "ready": threading.Event(), "result": None, "error": None}
        point_commit_queue.put(request)
        deadline = time.monotonic() + max(30.0, float(runtime_config.get("point_commit_timeout_seconds", 120)))
        while not request["ready"].wait(0.1):
            if cancel_check() or time.monotonic() >= deadline:
                return {"ok": False, "state": "cancelled_or_timeout_before_point_cleanup"}
        if request["error"] is not None:
            raise request["error"]
        return request["result"]

    def drain_point_commits(generation):
        requests = []
        while True:
            try:
                requests.append(point_commit_queue.get_nowait())
            except queue.Empty:
                break
        if not requests:
            return
        try:
            for request in requests:
                request["revision"] = data_store.append_event("point", request["point"])
            # One checkpoint ACK covers all points currently waiting. Candidate
            # status and the plan/RNG stay unchanged; a partial point is never
            # reported completed or skipped on a fresh Resume.
            save_checkpoint(generation, active_plan)
            for request in requests:
                request["result"] = {"ok": True, "revision": request["revision"],
                                     "checkpoint_sha256": checkpoint_parent_checksum,
                                     "candidate_completed": False}
        except Exception as error:
            for request in requests:
                request["error"] = error
            raise
        finally:
            for request in requests:
                request["ready"].set()

    if netlist_optimizer is not None:
        netlist_optimizer.point_commit_callback = request_point_commit

    def fold_successful_candidate(row, native_cleanup_proven):
        if row.get("status") != "completed" or not native_cleanup_proven:
            return None
        candidate_root = os.path.join(run_root, "candidates", row["candidate_id"])
        if not os.path.isdir(candidate_root):
            return None
        for key, name in (("timing", "timing.json"), ("completion_evidence", "completion_evidence.json")):
            path = os.path.join(candidate_root, name)
            if os.path.isfile(path) and not os.path.islink(path):
                row[key] = load_json(path)
        summary = summarize_successful_candidate_artifacts(candidate_root, run_root)
        if not summary:
            return None
        row["artifact_summary"] = summary
        row["raw_cleanup_proof"] = {"ok": True, "process_termination_proven": True,
                                    "remote_stop_proven": True, "native_cleanup_proven": native_cleanup_proven}
        data_store.commit_candidates([row])
        save_checkpoint(generation, active_plan)
        try:
            result = compact_successful_candidate_artifacts(candidate_root, run_root)
        except Exception as error:
            # A stopped task's small summary deletion is housekeeping. The
            # durable core/checkpoint remain valid and the exact files stay
            # available; an artifact error must not impersonate native failure.
            result = {"ok": False, "artifact_cleanup_complete": False,
                      "candidate_id": row["candidate_id"], "error": "%s: %s" % (error.__class__.__name__, error)}
        row["artifact_compaction"] = result
        data_store.commit_candidates([row])
        return result

    try:
        save_checkpoint(
            start_generation, active_plan, terminal_state=resume_terminal_state,
            terminal_reason=resume_terminal_reason,
        )
        if (
            persistent_backend and persistent_worker_lifetime == "run"
            and execution_start_generation < generations
        ):
            for worker_index in range(persistent_jobs):
                controller = PersistentControllerProxy(
                    run_root, worker_index, prepared, selected, config,
                )
                controller.start()
                persistent_workers.append(controller)
        for generation in range(execution_start_generation, generations):
            if os.path.exists(stop_path):
                stopped_reason = "user_stop"
                break
            status.update({"generation": generation + 1, "message": "evaluating generation %d" % (generation + 1)})
            atomic_write_json(status_path, status)
            generation_rows = [
                row for row in all_rows
                if row.get("generation") == generation and row.get("status") != "infrastructure_failed"
            ]
            infrastructure_rows = [
                row for row in all_rows
                if row.get("generation") == generation and row.get("status") == "infrastructure_failed"
            ]
            persistent_startup_streak = 0
            _raise_workspace_build_failure(
                generation, generation_rows + infrastructure_rows,
            )
            _raise_source_integrity_failure(generation, infrastructure_rows)
            if active_plan is None or active_plan.get("generation") != generation:
                if frozen_diagnostic_plan is not None:
                    generator.population_size = population
                    initial_values = [copy.deepcopy(item['values']) for item in frozen_diagnostic_plan
                                      if item['generation'] == generation]
                elif frozen_diagnostic_candidate is not None:
                    # ask() normally establishes this size; a fixed diagnostic
                    # plan must establish it without drawing another random point.
                    generator.population_size = 1
                    initial_values = [copy.deepcopy(frozen_diagnostic_candidate)]
                else:
                    initial_values = generator.ask(population)
                active_plan = {
                    "generation": generation,
                    "next_index": len(initial_values),
                    "replacements": 0,
                    "planned": list(enumerate(initial_values)),
                }
                save_checkpoint(generation, active_plan)

            def record_future_rows(future_rows, cleanup_worker, startup_streak):
                if not isinstance(future_rows, list):
                    future_rows = [future_rows]
                for row in future_rows:
                    if spec.get("evaluator", "real") == "real":
                        row.setdefault("execution_backend", spec["execution_backend"])
                        row["execution_simulator"] = spec["simulator"]
                    live_row = live_candidate_rows.get(row['candidate_id']) or {}
                    for live_key in ('execution_progress', 'source_test_map'):
                        if live_row.get(live_key):
                            row[live_key] = copy.deepcopy(live_row[live_key])
                    if row.get('execution_progress'):
                        progress = row['execution_progress']
                        progress['stage'] = row.get('status')
                        points = (row.get('completion_evidence') or {}).get('points') or []
                        for task in progress.get('tasks') or []:
                            matching = [point for point in points if
                                point.get('artifact_test') == task.get('artifact_test') and
                                str(point.get('point_id')) == str(task.get('point_id'))]
                            if len(matching) == 1:
                                task['corner'] = matching[0].get('source_corner') or matching[0].get('corner')
                    evaluated_row = row
                    candidate_root = os.path.join(run_root, "candidates", row["candidate_id"])
                    candidate_path = os.path.join(candidate_root, "candidate.json")
                    cleanup_request = None
                    process_cleanup_unproven = bool(evaluated_row.get("cleanup_pending"))
                    candidate_is_persisted = (
                        spec.get("evaluator", "real") == "real" and os.path.isfile(candidate_path)
                    )
                    if candidate_is_persisted:
                        persist_lightweight_candidate(candidate_root, row)
                        required_paths = [
                            candidate_path,
                            os.path.join(candidate_root, "timing.json"),
                        ]
                        if row.get("status") == "completed":
                            required_paths.append(os.path.join(candidate_root, "completion_evidence.json"))
                        if row.get("execution_backend") in ("private_netlist", "maestro_netlist"):
                            diagnostic_path = os.path.join(candidate_root, "execution_diagnostics.json")
                            if os.path.isfile(diagnostic_path):
                                required_paths.append(diagnostic_path)
                        if (row.get("source_guard", {}).get("ok") and not process_cleanup_unproven
                                and row.get("execution_backend") != "maestro_baseline"):
                            cleanup_request = (candidate_root, row["source_guard"], required_paths)
                        row = lightweight_candidate_record(row)
                        for key, name in (("timing", "timing.json"), ("completion_evidence", "completion_evidence.json")):
                            path = os.path.join(candidate_root, name)
                            if os.path.isfile(path) and not os.path.islink(path):
                                row[key] = load_json(path)
                    data_store.commit_candidates([row])
                    all_rows.append(row)
                    if row["status"] == "infrastructure_failed":
                        infrastructure_rows.append(row)
                    else:
                        generation_rows.append(row)
                    status["completed_candidates"] = len([
                        item for item in all_rows if item["status"] == "completed"
                    ])
                    status["failed_candidates"] = len([
                        item for item in all_rows if item["status"] == "failed"
                    ])
                    status["infrastructure_failed_candidates"] = len([
                        item for item in all_rows if item["status"] == "infrastructure_failed"
                    ])
                    status["message"] = "%s %s" % (row["candidate_id"], row["status"])
                    atomic_write_json(status_path, status)

                    # The terminal candidate must be durable before the shared Maestro
                    # session discards its only raw result/history binding.
                    save_checkpoint(generation, active_plan)
                    compact_before_cleanup = json.dumps({
                        "candidate": _pack_candidate(row, checkpoint_variable_ids),
                        "counts": [
                            status.get("completed_candidates", 0),
                            status.get("failed_candidates", 0),
                            status.get("infrastructure_failed_candidates", 0),
                        ],
                    }, separators=(",", ":"), sort_keys=True)
                    cleanup_changed = False
                    health_limit_decision = None
                    fatal_cleanup_error = (
                        "owned simulation cleanup was not proven; resume must quiesce the "
                        "recorded tasks before continuing"
                        if process_cleanup_unproven else None
                    )
                    if cleanup_worker is not None:
                        persistent_state = row.setdefault("persistent_worker", {})
                        try:
                            cleanup_result = cleanup_worker.commit_candidate(row["candidate_id"])
                            if not isinstance(cleanup_result, dict):
                                raise RuntimeError("persistent worker returned an invalid cleanup result")
                        except Exception as error:
                            cleanup_result = {
                                "ok": False, "state": "cleanup_exception",
                                "error": "%s: %s" % (error.__class__.__name__, error),
                            }
                            persistent_state["cleanup"] = public_cleanup_result(cleanup_result)
                            persistent_state["cleanup_pending"] = True
                            cleanup_request = None
                            cleanup_changed = True
                            fatal_cleanup_error = "persistent worker candidate cleanup raised an exception"
                        else:
                            retirement = cleanup_result.get("retirement") or {}
                            retired_guard = retirement.get("source_guard") or {}
                            retirement_cleanup = retirement.get("cleanup") or {}
                            if retirement and not retired_guard:
                                retired_guard = _reverify_prepared_source_identity(
                                    prepared, config,
                                )
                                retirement["source_guard"] = retired_guard
                            safe_retirement = bool(
                                retirement
                                and retirement.get("termination_proven") is True
                                and retirement.get("process_still_alive") is False
                                and retired_guard.get("ok") is True
                                and retirement_cleanup.get("ok") is True
                            )
                            cleanup_complete = cleanup_result.get("ok") is True or safe_retirement
                            persistent_state["cleanup_pending"] = not cleanup_complete
                            if not cleanup_complete:
                                cleanup_request = None
                            expected_replacement = bool(
                                cleanup_result.get("recycled") is True and safe_retirement
                                and expect_session_replacement(cleanup_worker)
                            )
                            if expected_replacement:
                                persistent_state["expected_session_replacement"] = True
                            if cleanup_complete:
                                health_decision = observe_session_health(
                                    cleanup_worker, cleanup_result,
                                )
                                if health_decision is not None:
                                    persistent_state["session_health"] = health_decision
                                    cleanup_changed = True
                                    if health_decision.get("triggered"):
                                        health_limit_decision = health_decision
                                    else:
                                        authorize_active_session_health(
                                            cleanup_worker, cleanup_result,
                                            health_decision,
                                        )
                            if retired_guard and not retired_guard.get("ok"):
                                cleanup_request = None
                                for candidate_row in (row, evaluated_row):
                                    candidate_row["source_guard"] = retired_guard
                                    candidate_row["status"] = "infrastructure_failed"
                                    candidate_row["failure_class"] = "source_integrity"
                                    candidate_row["failure_stage"] = "source_integrity"
                                    candidate_row["error"] = retired_guard.get("error") or (
                                        "source library fingerprint changed or could not be verified"
                                    )
                                    candidate_row.pop("metrics", None)
                                if row in generation_rows:
                                    generation_rows.remove(row)
                                if row not in infrastructure_rows:
                                    infrastructure_rows.append(row)
                                status["completed_candidates"] = len([
                                    item for item in all_rows if item["status"] == "completed"
                                ])
                                status["failed_candidates"] = len([
                                    item for item in all_rows if item["status"] == "failed"
                                ])
                                status["infrastructure_failed_candidates"] = len([
                                    item for item in all_rows if item["status"] == "infrastructure_failed"
                                ])
                                atomic_write_json(status_path, status)
                            if retirement.get("process_still_alive"):
                                fatal_cleanup_error = (
                                    "persistent worker supervisor remained alive after cleanup failure"
                                )
                            elif retirement and retirement.get("termination_proven") is not True:
                                fatal_cleanup_error = (
                                    "persistent worker termination was not proven after cleanup failure"
                                )
                            elif (
                                retirement and retired_guard.get("ok", True)
                                and not retirement_cleanup.get("ok", True)
                            ):
                                fatal_cleanup_error = (
                                    "persistent worker epoch could not be safely removed"
                                )
                            elif (
                                cleanup_result.get("ok") is not True
                                and not safe_retirement
                                and not (retired_guard and retired_guard.get("ok") is False)
                            ):
                                fatal_cleanup_error = (
                                    "persistent worker candidate cleanup was not proven"
                                )
                            persistent_state["cleanup"] = public_cleanup_result(cleanup_result)
                            cleanup_changed = True
                    controller_termination = (
                        (row.get("persistent_worker") or {}).get("controller_termination") or {}
                    )
                    if (
                        candidate_is_persisted and row.get("status") == "infrastructure_failed"
                        and controller_termination.get("termination_proven") is True
                        and controller_termination.get("process_still_alive") is False
                    ):
                        persistent_state = row.setdefault("persistent_worker", {})
                        try:
                            candidate_cleanup = cleanup_proven_terminal_infrastructure_candidate(
                                candidate_root, run_root,
                                required_lightweight_paths=[candidate_path, os.path.join(
                                    candidate_root, "timing.json",
                                )],
                            )
                        except Exception as error:
                            persistent_state["candidate_cleanup"] = {
                                "ok": False, "state": "cleanup_exception",
                                "error": "%s: %s" % (error.__class__.__name__, error),
                            }
                            cleanup_changed = True
                            fatal_cleanup_error = (
                                "proven-dead controller candidate artifacts could not be safely removed"
                            )
                        else:
                            persistent_state["candidate_cleanup"] = candidate_cleanup
                            cleanup_request = None
                            cleanup_changed = True
                    if cleanup_request is not None:
                        candidate_cleanup = safe_delete_candidate_workspace(
                            cleanup_request[0], run_root, cleanup_request[1],
                            required_lightweight_paths=cleanup_request[2],
                        )
                        if cleanup_worker is not None and isinstance(candidate_cleanup, dict):
                            row.setdefault("persistent_worker", {})["candidate_cleanup"] = candidate_cleanup
                            cleanup_changed = True
                    if cleanup_changed:
                        data_store.commit_candidates([row])
                        if candidate_is_persisted:
                            persist_lightweight_candidate(candidate_root, row)
                        compact_after_cleanup = json.dumps({
                            "candidate": _pack_candidate(row, checkpoint_variable_ids),
                            "counts": [
                                status.get("completed_candidates", 0),
                                status.get("failed_candidates", 0),
                                status.get("infrastructure_failed_candidates", 0),
                            ],
                        }, separators=(",", ":"), sort_keys=True)
                        if compact_after_cleanup != compact_before_cleanup:
                            save_checkpoint(generation, active_plan)
                    if fatal_cleanup_error is not None:
                        raise RuntimeError(fatal_cleanup_error)
                    if row.get("status") == "completed":
                        native_ack = bool(cleanup_worker is not None and
                            (row.get("persistent_worker") or {}).get("cleanup_pending") is False)
                        cli_ack = bool(row.get("execution_backend") in ("private_netlist", "maestro_netlist")
                            and not row.get("cleanup_pending")
                            and (row.get("native_job_state") or {}).get("native_completion_proven") is True)
                        legacy_ack = bool(row.get("execution_backend") in ("legacy", "adexl")
                            and not process_cleanup_unproven and cleanup_request is not None)
                        if native_ack or cli_ack or legacy_ack:
                            fold_successful_candidate(row, "native_candidate_cleanup_ack" if native_ack else "owned_task_completion")
                    if health_limit_decision is not None:
                        if not resource_guard_is_recyclable(
                                health_limit_decision, "candidate_boundary"):
                            status["message"] = "%s; checkpoint saved for resume" % (
                                PersistentSessionHealthLimit(health_limit_decision),
                            )
                            status["persistent_session_health"] = health_limit_decision
                            atomic_write_json(status_path, status)
                            save_checkpoint(generation, active_plan)
                            raise PersistentSessionHealthLimit(health_limit_decision)
                        finished_after_row = set(
                            item.get("candidate_id") for item in all_rows
                        )
                        future_work = bool(
                            generation + 1 < generations or any(
                                "g%03d_c%04d" % (generation, index)
                                not in finished_after_row
                                for index, _values in active_plan["planned"]
                            )
                        )
                        if not future_work:
                            status["message"] = (
                                "persistent Maestro resource limit reached after final candidate; "
                                "normal final shutdown will reclaim the session"
                            )
                            status["persistent_session_health"] = health_limit_decision
                            atomic_write_json(status_path, status)
                            save_checkpoint(generation, active_plan)
                            continue
                        event, exhausted = begin_resource_guard_recycle(
                            generation, active_plan, cleanup_worker,
                            row["candidate_id"], "candidate_boundary",
                            health_limit_decision,
                        )
                        terminate_resource_guard_epoch(cleanup_worker, event)
                        if exhausted:
                            save_checkpoint(generation, active_plan)
                            raise PersistentSessionHealthLimit({
                                "state": "restart_limit_exhausted",
                                "restart_limit": resource_guard_restart_limit,
                                "candidate_id": row["candidate_id"],
                                "decision": health_limit_decision,
                            })
                        restart_resource_guard_epoch(
                            generation, active_plan, cleanup_worker, event,
                        )

                _raise_workspace_build_failure(generation, future_rows)
                _raise_source_integrity_failure(generation, future_rows)
                _raise_parameter_application_failure(generation, future_rows)
                startup_only = bool(future_rows) and all(
                    row.get("status") == "infrastructure_failed"
                    and row.get("failure_class") == "persistent_startup"
                    for row in future_rows
                )
                startup_streak = startup_streak + 1 if startup_only else 0
                _raise_repeated_persistent_startup_failure(
                    generation, future_rows, startup_streak, persistent_startup_limit,
                )
                return startup_streak

            while True:
                finished_ids = set(row.get("candidate_id") for row in all_rows)
                pending = [
                    (index, values) for index, values in active_plan["planned"]
                    if "g%03d_c%04d" % (generation, index) not in finished_ids
                ]
                if pending and batch_enabled:
                    worker = persistent_workers[0]
                    batch_fallback = False
                    for planned_batch in candidate_batches(pending, execution_plan["candidate_limit"]):
                        update_live_batch(planned_batch, generation, initializing=True)
                        request = worker.submit_evaluate_batch(planned_batch, generation, spec)
                        submitted_at = time.monotonic()
                        begin_expected_session_replacement(worker, submitted_at)
                        deadline = submitted_at + batch_timeout_seconds(len(planned_batch), spec, config)
                        next_health_sample = submitted_at + active_health_interval
                        status["active_batch_candidates"] = [
                            "g%03d_c%04d" % (generation, index) for index, _values in planned_batch
                        ]
                        status["message"] = "evaluating %d candidates in one Maestro batch" % len(planned_batch)
                        atomic_write_json(status_path, status)
                        result = None
                        next_live_sample = 0.0
                        while result is None:
                            if time.monotonic() >= next_live_sample:
                                next_live_sample = time.monotonic() + 1.0
                                update_live_batch(planned_batch, generation)
                            if os.path.exists(stop_path):
                                native_stop = getattr(worker, 'request_native_batch_stop', None)
                                if callable(native_stop):
                                    status['native_stop_before_log_retention'] = native_stop('user stop before diagnostic retention')
                                from .maestro_progress import preserve_live_logs
                                try:
                                    retained = preserve_live_logs(run_root, [live_candidate_rows[cid] for cid in
                                        status['active_batch_candidates'] if cid in live_candidate_rows])
                                    for retained_row in retained:
                                        retained_row['execution_progress']['stage'] = 'stopping'
                                        live_candidate_rows[retained_row['candidate_id']] = retained_row
                                    if retained:
                                        data_store.commit_candidates(retained)
                                except Exception as retention_error:
                                    status['live_log_retention_error'] = str(retention_error)
                                termination = worker.terminate_and_prove("user stop during Maestro candidate batch")
                                if termination.get('termination_proven'):
                                    cancelled = []
                                    for cid in status['active_batch_candidates']:
                                        live_row = live_candidate_rows.get(cid)
                                        if live_row:
                                            live_row['execution_progress']['stage'] = 'cancelled'
                                            for task in live_row['execution_progress'].get('tasks') or []:
                                                if task.get('state') not in ('finished', 'submission_failed'):
                                                    task['state'] = 'cancelled'
                                            cancelled.append(live_row)
                                    if cancelled:
                                        data_store.commit_candidates(cancelled)
                                status.setdefault("batch_stop_terminations", []).append(termination)
                                for index, values in planned_batch:
                                    recovered = _recover_terminal_candidate_wal(
                                        run_root, index, generation, values, spec,
                                        worker.worker_index, termination,
                                    )
                                    if recovered is not None:
                                        persistent_startup_streak = record_future_rows(
                                            recovered, None, persistent_startup_streak,
                                        )
                                stopped_reason = "user_stop"
                                status["message"] = "user_stop"
                                save_checkpoint(generation, active_plan)
                                atomic_write_json(status_path, status)
                                if not termination.get("termination_proven"):
                                    raise RuntimeError("batch stop was not proven; workspace retained: %s" % termination)
                                break
                            try:
                                result = worker.poll(request)
                                now = time.monotonic()
                                if result is None and now >= next_health_sample:
                                    next_health_sample = now + active_health_interval
                                    decision = observe_active_session_health(worker, worker.sample_health())
                                    if decision and decision.get("triggered"):
                                        status["persistent_session_health"] = decision
                                        raise PersistentSessionHealthLimit(decision)
                                if result is None and now >= deadline:
                                    raise PersistentControllerProcessError("bounded Maestro batch deadline exceeded")
                            except Exception as error:
                                from .maestro_progress import preserve_live_logs
                                try:
                                    retained = preserve_live_logs(run_root, [live_candidate_rows[cid] for cid in
                                        status['active_batch_candidates'] if cid in live_candidate_rows])
                                    for retained_row in retained:
                                        retained_row['execution_progress']['stage'] = 'infrastructure_failed'
                                        live_candidate_rows[retained_row['candidate_id']] = retained_row
                                    if retained:
                                        data_store.commit_candidates(retained)
                                except Exception as retention_error:
                                    status['live_log_retention_error'] = str(retention_error)
                                termination = worker.terminate_and_prove(str(error))
                                status.setdefault("batch_failure_terminations", []).append(termination)
                                status["message"] = str(error)
                                for index, values in planned_batch:
                                    recovered = _recover_terminal_candidate_wal(
                                        run_root, index, generation, values, spec,
                                        worker.worker_index, termination,
                                    )
                                    if recovered is not None:
                                        persistent_startup_streak = record_future_rows(
                                            recovered, None, persistent_startup_streak,
                                        )
                                save_checkpoint(generation, active_plan)
                                atomic_write_json(status_path, status)
                                # Never resubmit an ambiguous remote job or erase
                                # its private files merely because Ocean died.
                                raise
                            if result is None:
                                time.sleep(0.05)
                        if stopped_reason == "user_stop":
                            break
                        if not isinstance(result, dict):
                            raise RuntimeError("Maestro batch controller returned an invalid result")
                        if result.get("fallback"):
                            batch_enabled = False
                            batch_fallback = True
                            execution_plan.update({"effective_mode": "serial", "fallback": result["fallback"]})
                            status["maestro_candidate_execution"] = copy.deepcopy(execution_plan)
                            status["parallel_execution_policy"] = "serialized_single_maestro"
                            status["message"] = "batch unavailable; preserving serial execution: %s" % result["fallback"]
                            atomic_write_json(status_path, status)
                            break
                        update_live_batch(planned_batch, generation)
                        rows = result.get("rows")
                        expected_ids = set("g%03d_c%04d" % (generation, index) for index, _values in planned_batch)
                        if (not isinstance(rows, list) or len(rows) != len(expected_ids)
                                or any(not isinstance(row, dict) for row in rows)
                                or set(row.get("candidate_id") for row in rows) != expected_ids):
                            raise RuntimeError("Maestro batch returned an incomplete candidate set")
                        requested_by_id = dict(
                            ("g%03d_c%04d" % (generation, index), (index, values))
                            for index, values in planned_batch
                        )
                        for row in rows:
                            requested_index, requested_values = requested_by_id[row["candidate_id"]]
                            if (type(row.get("index")) is not int
                                    or row["index"] != requested_index
                                    or type(row.get("generation")) is not int
                                    or row["generation"] != generation
                                    or row.get("values") != requested_values):
                                raise RuntimeError("Maestro batch candidate identity/values changed: %s" % row["candidate_id"])
                        # Every candidate and the authoritative checkpoint must
                        # survive before discarding their shared native history.
                        persistent_startup_streak = record_future_rows(rows, None, persistent_startup_streak)
                        save_checkpoint(generation, active_plan)
                        cleanup = worker.commit_candidate(result["batch_id"])
                        status["last_batch_cleanup"] = cleanup
                        if not isinstance(cleanup, dict) or not cleanup.get("ok"):
                            save_checkpoint(generation, active_plan)
                            raise RuntimeError("Maestro batch cleanup could not be proven: %s" % cleanup)
                        for row in all_rows:
                            if (row.get("batch_execution") or {}).get("batch_id") == result["batch_id"]:
                                row.setdefault("persistent_worker", {})["cleanup_pending"] = False
                                fold_successful_candidate(row, "native_batch_cleanup_ack")
                        from .maestro_batch import compact_committed_batch
                        status["last_batch_artifact_compaction"] = compact_committed_batch(
                            run_root, result["batch_id"], cleanup,
                            batch_root=os.path.join(run_root, "batches", "g%03d_g%03d_c%04d" %
                                                    (generation, generation, planned_batch[0][0])))
                        decision = observe_session_health(worker, cleanup)
                        if decision and decision.get("triggered"):
                            status["persistent_session_health"] = decision
                            save_checkpoint(generation, active_plan)
                            raise PersistentSessionHealthLimit(decision)
                        authorize_active_session_health(worker, cleanup, decision)
                        save_checkpoint(generation, active_plan)
                        status.pop("active_batch_candidates", None)
                        atomic_write_json(status_path, status)
                    if stopped_reason == "user_stop":
                        break
                    if batch_fallback:
                        continue
                    # Re-evaluate pending/replacement planning with this same
                    # generation; no population padding to match maxjobs.
                    continue
                elif pending and persistent_backend and persistent_worker_lifetime == "run":
                    pending_iter = iter(pending)
                    active_requests = {}
                    retries = max(0, int(config["runtime"].get("infrastructure_retries", 1))) + 1
                    hard_seconds = (
                        retries * (
                            float(config["runtime"].get("persistent_workspace_prepare_timeout_seconds", 1800))
                            +
                            float(config["runtime"].get("ocean_startup_timeout_seconds", 90))
                            + serial_timeout_seconds(spec, config)
                        )
                        + scheduler_watchdog_grace
                    )

                    def submit_controller_request(worker, item):
                        index, values = item
                        update_live_batch([(index, values)], generation, initializing=True, serial=True)
                        request = worker.submit_evaluate(index, generation, values, spec)
                        submitted_at = time.monotonic()
                        begin_expected_session_replacement(worker, submitted_at)
                        active_requests[worker] = {
                            "request": request,
                            "index": index,
                            "values": values,
                            "deadline": submitted_at + hard_seconds,
                            "next_health_sample": submitted_at + active_health_interval,
                        }

                    for worker in persistent_workers[:min(len(pending), len(persistent_workers))]:
                        submit_controller_request(worker, next(pending_iter))
                    while active_requests:
                        if os.path.exists(stop_path):
                            stop_terminations = []
                            stop_recovered_rows = []
                            for worker, watched in list(active_requests.items()):
                                live_id = "g%03d_c%04d" % (generation, watched["index"])
                                native_stop = worker.request_native_batch_stop('user stop during Maestro serial candidate')
                                update_live_batch([(watched['index'], watched['values'])], generation, serial=True)
                                retained_live = None
                                if live_id in live_candidate_rows:
                                    from .maestro_progress import preserve_live_logs
                                    retained_live = preserve_live_logs(run_root, [live_candidate_rows[live_id]])[0]
                                    retained_live['execution_progress']['stage'] = 'stopping'
                                    live_candidate_rows[live_id] = retained_live
                                    data_store.commit_candidates([retained_live])
                                has_retained_candidate_log = bool(retained_live and any(
                                    task.get('retained_snapshot') for task in
                                    retained_live.get('execution_progress', {}).get('tasks') or []))
                                termination = worker.terminate_and_prove(
                                    "user stop requested during g%03d_c%04d"
                                    % (generation, watched["index"])
                                )
                                stop_termination = {
                                    "candidate_id": "g%03d_c%04d" % (generation, watched["index"]),
                                    "terminated_at": utc_timestamp(),
                                    "result": termination,
                                    "native_stop": native_stop,
                                }
                                stop_terminations.append(stop_termination)
                                status.setdefault("controller_stop_terminations", []).append(stop_termination)
                                status["controller_stop_terminations"] = status["controller_stop_terminations"][-20:]
                                atomic_write_json(status_path, status)
                                if not termination.get("termination_proven"):
                                    raise RuntimeError(
                                        "persistent controller termination was not proven during stop: %s"
                                        % termination
                                    )
                                if retained_live is not None:
                                    retained_live['execution_progress']['stage'] = 'cancelled'
                                    live_candidate_rows[live_id] = retained_live
                                    data_store.commit_candidates([retained_live])
                                recovered = _recover_terminal_candidate_wal(
                                    run_root, watched["index"], generation, watched["values"],
                                    spec, worker.worker_index, termination,
                                )
                                if recovered is not None:
                                    stop_termination["terminal_wal_recovery"] = recovered["wal_recovery"]
                                    stop_recovered_rows.append(recovered)
                                else:
                                    candidate_root = os.path.join(
                                        run_root, "candidates", stop_termination["candidate_id"],
                                    )
                                    try:
                                        candidate_cleanup = (
                                            cleanup_abandoned_nonterminal_candidate(
                                                candidate_root, run_root,
                                                verify_sources_unchanged,
                                            )
                                            if os.path.lexists(candidate_root) and not has_retained_candidate_log
                                            else {
                                                "ok": True, "removed": False,
                                                "state": "bounded_live_diagnostics_retained" if has_retained_candidate_log else "candidate_root_absent",
                                            }
                                        )
                                    except Exception as cleanup_error:
                                        stop_termination["candidate_cleanup"] = {
                                            "ok": False,
                                            "error": "%s: %s" % (
                                                cleanup_error.__class__.__name__, cleanup_error,
                                            ),
                                        }
                                        status["controller_stop_terminations"] = (
                                            status["controller_stop_terminations"][-20:]
                                        )
                                        atomic_write_json(status_path, status)
                                        raise RuntimeError(
                                            "user stop could not safely reclaim active candidate: %s"
                                            % cleanup_error
                                        )
                                    stop_termination["candidate_cleanup"] = candidate_cleanup
                            status["controller_stop_terminations"] = (
                                status["controller_stop_terminations"][-20:]
                            )
                            atomic_write_json(status_path, status)
                            for recovered in stop_recovered_rows:
                                persistent_startup_streak = record_future_rows(
                                    recovered, None, persistent_startup_streak,
                                )
                            active_requests.clear()
                            break
                        completed_requests = []
                        now = time.monotonic()
                        for worker, watched in list(active_requests.items()):
                            if now >= watched.get('next_live_sample', 0):
                                watched['next_live_sample'] = now + 1.
                                update_live_batch([(watched['index'], watched['values'])], generation, serial=True)
                            result = None
                            failure = None
                            termination = None
                            resource_decision = None
                            try:
                                result = worker.poll(watched["request"])
                            except Exception as error:
                                failure = error
                            if (
                                result is None and failure is None
                                and now >= watched["next_health_sample"]
                            ):
                                watched["next_health_sample"] = now + active_health_interval
                                sampler = getattr(worker, "sample_health", None)
                                if callable(sampler):
                                    try:
                                        active_health = sampler()
                                        if not isinstance(active_health, dict):
                                            raise RuntimeError(
                                                "active health sampler returned a non-object"
                                            )
                                    except Exception as health_error:
                                        active_health = {
                                            "alive": True,
                                            "tree_complete": False,
                                            "storage_complete": False,
                                            "logs_complete": False,
                                            "sampling_error": "%s: %s" % (
                                                health_error.__class__.__name__, health_error,
                                            ),
                                        }
                                    resource_decision = observe_active_session_health(
                                        worker, active_health,
                                    )
                                    watched["last_health_decision"] = resource_decision
                                    if not (
                                        resource_decision
                                        and resource_decision.get("triggered")
                                    ):
                                        resource_decision = None
                            if resource_decision is not None:
                                resource_error = PersistentSessionHealthLimit(resource_decision)
                                candidate_id = "g%03d_c%04d" % (
                                    generation, watched["index"],
                                )
                                if not resource_guard_is_recyclable(
                                        resource_decision, "active_candidate"):
                                    termination = worker.terminate_and_prove(str(resource_error))
                                    resource_record = {
                                        "candidate_id": candidate_id,
                                        "requested_at": utc_timestamp(),
                                        "decision": resource_decision,
                                        "result": termination,
                                    }
                                    status["message"] = (
                                        "persistent Maestro health guard exceeded; "
                                        "checkpoint saved for resume"
                                    )
                                    status["persistent_session_health"] = resource_decision
                                    status.setdefault(
                                        "active_resource_guard_terminations", [],
                                    ).append(resource_record)
                                    status["active_resource_guard_terminations"] = (
                                        status["active_resource_guard_terminations"][-20:]
                                    )
                                    atomic_write_json(status_path, status)
                                    recovered = None
                                    if termination.get("termination_proven"):
                                        recovered = _recover_terminal_candidate_wal(
                                            run_root, watched["index"], generation,
                                            watched["values"], spec, worker.worker_index,
                                            termination,
                                        )
                                    if recovered is not None:
                                        resource_record["terminal_wal_recovery"] = (
                                            recovered["wal_recovery"]
                                        )
                                    resource_row = recovered or _persistent_controller_failure_candidate(
                                        run_root, watched["index"], generation,
                                        watched["values"], spec, worker.worker_index,
                                        resource_error, termination,
                                        failure_class="resource_guard",
                                    )
                                    resource_row.setdefault("persistent_worker", {}).update({
                                        "active_session_health": resource_decision,
                                        "controller_termination": termination,
                                    })
                                    active_requests.pop(worker, None)
                                    persistent_startup_streak = record_future_rows(
                                        resource_row, None, persistent_startup_streak,
                                    )
                                    save_checkpoint(generation, active_plan)
                                    if not termination.get("termination_proven"):
                                        raise RuntimeError(
                                            "active health guard could not prove controller termination: %s"
                                            % termination
                                        )
                                    raise resource_error
                                resource_record, exhausted = begin_resource_guard_recycle(
                                    generation, active_plan, worker, candidate_id,
                                    "active_candidate", resource_decision,
                                )
                                termination = terminate_resource_guard_epoch(
                                    worker, resource_record,
                                )
                                recovered = None
                                recovered = _recover_terminal_candidate_wal(
                                    run_root, watched["index"], generation,
                                    watched["values"], spec, worker.worker_index,
                                    termination,
                                )
                                if recovered is not None:
                                    resource_record["terminal_wal_recovery"] = recovered["wal_recovery"]
                                    active_requests.pop(worker, None)
                                    persistent_startup_streak = record_future_rows(
                                        recovered, None, persistent_startup_streak,
                                    )
                                else:
                                    candidate_root = os.path.join(
                                        run_root, "candidates", candidate_id,
                                    )
                                    try:
                                        candidate_cleanup = (
                                            cleanup_abandoned_nonterminal_candidate(
                                                candidate_root, run_root,
                                                verify_sources_unchanged,
                                            )
                                            if os.path.lexists(candidate_root)
                                            else {
                                                "ok": True, "removed": False,
                                                "state": "candidate_root_absent",
                                            }
                                        )
                                    except Exception as cleanup_error:
                                        resource_record["candidate_cleanup"] = {
                                            "ok": False,
                                            "error": "%s: %s" % (
                                                cleanup_error.__class__.__name__, cleanup_error,
                                            ),
                                        }
                                        atomic_write_json(status_path, status)
                                        raise RuntimeError(
                                            "resource guard could not safely reclaim active candidate: %s"
                                            % cleanup_error
                                        )
                                    resource_record["candidate_cleanup"] = candidate_cleanup
                                    active_requests.pop(worker, None)
                                atomic_write_json(status_path, status)
                                save_checkpoint(generation, active_plan)
                                if exhausted:
                                    raise PersistentSessionHealthLimit({
                                        "state": "restart_limit_exhausted",
                                        "restart_limit": resource_guard_restart_limit,
                                        "candidate_id": candidate_id,
                                        "decision": resource_decision,
                                    })
                                restarted = restart_resource_guard_epoch(
                                    generation, active_plan, worker, resource_record,
                                )
                                if not restarted:
                                    continue
                                if recovered is None:
                                    submit_controller_request(
                                        worker, (watched["index"], watched["values"]),
                                    )
                                else:
                                    try:
                                        item = next(pending_iter)
                                    except StopIteration:
                                        pass
                                    else:
                                        submit_controller_request(worker, item)
                                continue
                            if result is None and failure is None and now >= watched["deadline"]:
                                failure = PersistentControllerProcessError(
                                    "controller hard deadline exceeded after %.1f seconds" % hard_seconds
                                )
                            if failure is not None:
                                fatal_remote = bool(
                                    isinstance(failure, PersistentControllerRemoteError)
                                    and failure.fatal_run
                                )
                                termination = worker.terminate_and_prove(str(failure))
                                candidate_id = "g%03d_c%04d" % (
                                    generation, watched["index"],
                                )
                                termination_record = {
                                    "candidate_id": candidate_id,
                                    "requested_at": utc_timestamp(),
                                    "error": "%s: %s" % (failure.__class__.__name__, failure),
                                    "result": termination,
                                }
                                if fatal_remote:
                                    termination_record.update({
                                        "remote_error_type": failure.remote_error_type,
                                        "failure_class": failure.failure_class,
                                    })
                                    status["message"] = (
                                        "external EDA environment changed; checkpoint saved for resume"
                                    )
                                    status.setdefault(
                                        "external_environment_terminations", [],
                                    ).append(termination_record)
                                    status["external_environment_terminations"] = (
                                        status["external_environment_terminations"][-20:]
                                    )
                                else:
                                    status["message"] = (
                                        "watchdog terminated controller for %s" % candidate_id
                                    )
                                    status.setdefault(
                                        "scheduler_watchdog_terminations", [],
                                    ).append(termination_record)
                                    status["scheduler_watchdog_terminations"] = (
                                        status["scheduler_watchdog_terminations"][-20:]
                                    )
                                atomic_write_json(status_path, status)
                                if not termination.get("termination_proven"):
                                    raise RuntimeError(
                                        "persistent controller termination was not proven: %s" % termination
                                    )
                                if fatal_remote:
                                    active_requests.pop(worker, None)
                                    save_checkpoint(generation, active_plan)
                                    candidate_root = os.path.join(
                                        run_root, "candidates", candidate_id,
                                    )
                                    try:
                                        candidate_cleanup = (
                                            cleanup_abandoned_nonterminal_candidate(
                                                candidate_root, run_root,
                                                verify_sources_unchanged,
                                            )
                                            if os.path.lexists(candidate_root)
                                            else {
                                                "ok": True, "removed": False,
                                                "state": "candidate_root_absent",
                                            }
                                        )
                                    except Exception as cleanup_error:
                                        termination_record["candidate_cleanup"] = {
                                            "ok": False,
                                            "error": "%s: %s" % (
                                                cleanup_error.__class__.__name__, cleanup_error,
                                            ),
                                        }
                                        atomic_write_json(status_path, status)
                                        raise RuntimeError(
                                            "external EDA environment changed and the active "
                                            "candidate could not be safely reclaimed: %s"
                                            % cleanup_error
                                        )
                                    termination_record["candidate_cleanup"] = candidate_cleanup
                                    atomic_write_json(status_path, status)
                                    raise ExternalEdaEnvironmentChanged(str(failure))
                                result = _recover_terminal_candidate_wal(
                                    run_root, watched["index"], generation, watched["values"],
                                    spec, worker.worker_index, termination,
                                )
                                if result is not None:
                                    termination_record["terminal_wal_recovery"] = result["wal_recovery"]
                                    status["message"] = "recovered terminal WAL for %s" % result["candidate_id"]
                                    atomic_write_json(status_path, status)
                                else:
                                    result = _persistent_controller_failure_candidate(
                                        run_root, watched["index"], generation, watched["values"], spec,
                                        worker.worker_index, failure, termination,
                                    )
                            if result is not None:
                                completed_requests.append((watched["index"], worker, result, failure))
                        if not completed_requests:
                            time.sleep(0.05)
                            continue
                        for _index, worker, result, failure in sorted(completed_requests):
                            active_requests.pop(worker, None)
                            persistent_startup_streak = record_future_rows(
                                result, None if failure is not None else worker,
                                persistent_startup_streak,
                            )
                            if failure is not None and not os.path.exists(stop_path):
                                worker.restart()
                                expect_session_replacement(worker)
                            if os.path.exists(stop_path):
                                continue
                            try:
                                item = next(pending_iter)
                            except StopIteration:
                                continue
                            submit_controller_request(worker, item)
                elif pending:
                    with ThreadPoolExecutor(max_workers=jobs) as executor:
                        pending_iter = iter(pending)
                        active_futures = {}

                        def submit_next_candidate():
                            try:
                                index, values = next(pending_iter)
                            except StopIteration:
                                return False
                            if netlist_optimizer is not None:
                                future = executor.submit(netlist_optimizer.evaluate, index, generation, values)
                            elif baseline_backend:
                                future = executor.submit(_evaluate_native_baseline_candidate,
                                    run_root, index, generation, values, spec, prepared, selected, config,
                                    lambda: os.path.exists(stop_path), owned_roots=baseline_candidate_roots)
                            else:
                                future = executor.submit(
                                    _evaluate_candidate, run_root, index, generation, values,
                                    spec, prepared, selected, config,
                                )
                            active_futures[future] = (index, values)
                            return True

                        for _unused in range(min(jobs, len(pending))):
                            submit_next_candidate()
                        try:
                            while active_futures:
                                wait_options = {"return_when": FIRST_COMPLETED}
                                if netlist_optimizer is not None:
                                    wait_options["timeout"] = 0.2
                                completed, _waiting = wait(
                                    set(active_futures), **wait_options
                                )
                                if netlist_optimizer is not None:
                                    drain_point_commits(generation)
                                    fallback = getattr(netlist_optimizer, "native_fallback", None)
                                    if fallback and not status.get("native_generation_required"):
                                        status.update(netlist_source="maestro", native_generation_required=True,
                                                      template_qualified=False, template_fallback=copy.deepcopy(fallback))
                                        status["message"] = "Numeric derived DPAR requires fresh native netlist generation per candidate"
                                        atomic_write_json(status_path, status)
                                if netlist_optimizer is not None and os.path.exists(stop_path):
                                    netlist_optimizer.cancel()
                                completed_count = 0
                                for future in sorted(
                                        completed,
                                        key=lambda item: active_futures[item][0]):
                                    active_futures.pop(future)
                                    result = future.result()
                                    persistent_startup_streak = record_future_rows(
                                        result, None,
                                        persistent_startup_streak,
                                    )
                                    if (baseline_backend and result.get("status") != "completed" and
                                            not (result.get("status") == "cancelled" and os.path.exists(stop_path))):
                                        raise RuntimeError("Maestro baseline candidate failed; evidence retained: " + str(result.get("error")))
                                    completed_count += 1
                                if (netlist_optimizer is not None and netlist_optimizer.fatal_error
                                        and not os.path.exists(stop_path)):
                                    netlist_optimizer.cancel()
                                    # Persist every already-created candidate before failing the run.
                                    for future in sorted(active_futures, key=lambda item: active_futures[item][0]):
                                        record_future_rows(future.result(), None, persistent_startup_streak)
                                    active_futures.clear()
                                    raise PrivateNetlistRunError(netlist_optimizer.fatal_error)
                                if not os.path.exists(stop_path):
                                    for _unused in range(completed_count):
                                        submit_next_candidate()
                        except Exception:
                            if netlist_optimizer is not None:
                                netlist_optimizer.cancel()
                            for future in active_futures:
                                future.cancel()
                            raise
                if os.path.exists(stop_path):
                    stopped_reason = "user_stop"
                    status["message"] = "user_stop"
                    atomic_write_json(status_path, status)
                    save_checkpoint(generation, active_plan)
                    break
                finished_ids = set(row.get("candidate_id") for row in all_rows)
                remaining_planned = [
                    (index, values) for index, values in active_plan["planned"]
                    if "g%03d_c%04d" % (generation, index) not in finished_ids
                ]
                if remaining_planned and not os.path.exists(stop_path):
                    continue
                missing = population - len(generation_rows)
                allowance = replacement_limit - active_plan["replacements"]
                if missing > 0 and allowance > 0 and not os.path.exists(stop_path):
                    count = min(missing, allowance)
                    replacement_values = generator.ask(count)
                    first_index = active_plan["next_index"]
                    active_plan["planned"].extend(
                        (first_index + offset, values) for offset, values in enumerate(replacement_values)
                    )
                    active_plan["next_index"] += count
                    active_plan["replacements"] += count
                    status["total_candidates"] += count
                    status["message"] = "resampling %d infrastructure-failed candidate(s)" % count
                    atomic_write_json(status_path, status)
                    save_checkpoint(generation, active_plan)
                    continue
                break
            if netlist_optimizer is not None:
                netlist_optimizer.verify_sources()
            if stopped_reason == "user_stop":
                break
            if not generation_rows and infrastructure_rows:
                raise RuntimeError(
                    "systemic infrastructure failure in generation %d; optimization stopped instead of "
                    "replacing candidates indefinitely: %s" % (
                        generation + 1, _infrastructure_failure_summary(infrastructure_rows),
                    )
                )
            generation_rows.sort(key=lambda row: (
                int(row.get("index", 0)), str(row.get("candidate_id") or ""),
            ))
            generation_accepted = generator.tell(generation_rows)
            completed_generation = [row for row in generation_rows if row["status"] == "completed"]
            if not completed_generation:
                status["message"] = "generation %d had no valid candidate; retained prior population" % (generation + 1)
                atomic_write_json(status_path, status)
                active_plan = None
                save_checkpoint(generation + 1, active_plan)
                continue
            survivor_population = [row for row in generator.previous if row.get("status") == "completed"]
            representative, ideal_distance = select_representative(survivor_population or completed_generation)
            selection_history.append({
                "generation": generation,
                "candidate_id": representative["candidate_id"],
                "role": "single_objective_best" if objective_count == 1 else "pareto_knee_representative",
                "total_violation": representative["metrics"]["total_violation"],
                "objective_scores": [row["score"] for row in representative["metrics"]["objectives"]],
                "merit": representative["metrics"]["merit"] if objective_count == 1 else None,
                "ideal_distance": ideal_distance if objective_count > 1 else None,
            })
            auto_stop = spec.get("auto_stop", {}) if generation_accepted and objective_count == 1 else {}
            patience = int(auto_stop.get("patience", 0))
            tolerance = float(auto_stop.get("tolerance", 0.0))
            if patience > 0 and len(selection_history) > patience:
                recent = selection_history[-patience - 1:]
                improvement = recent[0]["merit"] - min(row["merit"] for row in recent[1:])
                if improvement <= tolerance and all(row["total_violation"] <= 0 for row in recent):
                    stopped_reason = "auto_stop_no_improvement"
                    active_plan = None
                    save_checkpoint(
                        generation + 1, active_plan, terminal_state="completed",
                        terminal_reason=stopped_reason,
                    )
                    break
            active_plan = None
            if generation + 1 >= generations:
                stopped_reason = "generation_limit"
                save_checkpoint(
                    generation + 1, active_plan, terminal_state="completed",
                    terminal_reason=stopped_reason,
                )
            else:
                save_checkpoint(generation + 1, active_plan)
        completed = [row for row in all_rows if row["status"] == "completed"]
        if not completed and stopped_reason != "user_stop":
            raise RuntimeError("all %d candidates failed across %d generations; inspect candidate errors" % (len(all_rows), generations))
        if stopped_reason != "user_stop" and not terminal_checkpoint_written:
            stopped_reason = "generation_limit"
            save_checkpoint(
                generations, None, terminal_state="completed",
                terminal_reason=stopped_reason,
            )
        front = nondominated_front(completed) if completed else []
        representative, ideal_distance = select_representative(completed) if completed else (None, None)
        result_state = "stopped" if stopped_reason == "user_stop" else "completed"
        result = {
            "schema_version": "crescendo.run_result.v2",
            "run_id": status["run_id"],
            "run_root": run_root,
            "display_name": status["display_name"],
            "state": result_state,
            "stopped_reason": stopped_reason or "generation_limit",
            "algorithm": algorithm,
            "algorithm_policy": algorithm_policy,
            "algorithm_parameters": algorithm_parameters,
            "representative": representative,
            "representative_role": "single_objective_best" if objective_count == 1 else "pareto_knee_representative",
            "representative_ideal_distance": ideal_distance if objective_count > 1 else None,
            "pareto_front": front,
            "candidates": all_rows,
            "selection_history": selection_history,
            "raw_cleanup": {"policy": "zero_raw_retention", "kept": []},
            "finished_at": utc_timestamp(),
        }
        if objective_count == 1 and representative is not None:
            result["best"] = representative
        if representative is not None:
            assignment_name = "best_assignment.json" if objective_count == 1 else "representative_assignment.json"
            pending_assignment = (assignment_name, {
                "candidate_id": representative["candidate_id"],
                "role": result["representative_role"],
                "values": representative["values"],
                "metrics": representative["metrics"],
            })
        status.update({"state": "finalizing", "message": (
            "closing owned simulator tasks" if netlist_optimizer is not None else "closing persistent Maestro session"
        )})
        atomic_write_json(status_path, status)
        pending_result = result
    except Exception as error:
        message = "%s: %s" % (error.__class__.__name__, error)
        status.setdefault("primary_failure", {
            "error_type": error.__class__.__name__, "message": str(error),
            "failure_stage": getattr(error, "startup_stage", None) or "optimization",
            "diagnostic_path": getattr(error, "startup_diagnostic_path", None),
        })
        shared_failure = getattr(netlist_optimizer, "preparation_failure", None)
        if isinstance(shared_failure, dict):
            status["shared_preparation_failure"] = copy.deepcopy(shared_failure)
            status["primary_failure"]["failure_stage"] = shared_failure.get("failure_stage") or "native_template_preparation"
            status["primary_failure"]["diagnostic_path"] = shared_failure.get("diagnostic_path")
        status.update({"state": "failed", "message": message, "finished_at": utc_timestamp()})
        try:
            data_store.append_event("status", status)
        except Exception:
            pass  # Preserve the first error; failed writes never allow raw deletion.
        atomic_write_json(status_path, status)
        raise
    finally:
        active_exception = sys.exc_info()[0] is not None
        shutdown = []
        if netlist_optimizer is not None:
            try:
                close_result = netlist_optimizer.close(cancel=active_exception or stopped_reason == "user_stop")
                shutdown.append({"worker_index": "private_netlist", "ok": close_result.get("cleanup_proven") is True,
                                 "result": close_result})
                if pending_result is not None:
                    pending_result["raw_cleanup"] = {
                        "policy": "candidate_raw_removed_after_checkpoint; netlist_generation_evidence_retained",
                        "kept": [os.path.relpath(netlist_optimizer.root, run_root)],
                    }
            except Exception as error:
                shutdown.append({"worker_index": "private_netlist", "ok": False,
                                 "error": "%s: %s" % (error.__class__.__name__, error)})
        for worker in persistent_workers:
            try:
                close_result = worker.close()
                cleanup_result = close_result.get("cleanup") or {}
                shutdown.append({
                    "worker_index": worker.worker_index,
                    "ok": bool(
                        close_result.get("termination_proven") is True
                        and close_result.get("process_still_alive") is False
                        and cleanup_result.get("ok") is True
                        and (cleanup_result.get("removed") is True
                             or cleanup_result.get("state") in ("already_removed", "no_owned_epoch"))
                    ),
                    "result": close_result,
                })
            except Exception as error:
                shutdown.append({
                    "worker_index": worker.worker_index,
                    "ok": False,
                    "error": "%s: %s" % (error.__class__.__name__, error),
                })
        audit_error = None
        if baseline_backend:
            try:
                baseline_shutdown = _reconcile_baseline_shutdown(run_root, baseline_candidate_roots)
                shutdown.extend(baseline_shutdown["workers"])
                status["final_cleanup"] = baseline_shutdown["final_cleanup"]
                for row in all_rows:
                    root = os.path.join(run_root, "candidates", row["candidate_id"])
                    proof = baseline_shutdown["candidate_proofs"].get(root)
                    if proof is not None:
                        row["raw_cleanup_proof"] = proof
                        persist_lightweight_candidate(root, row)
                if all_rows:
                    data_store.commit_candidates(all_rows)
                if pending_result is not None:
                    pending_result["raw_cleanup"] = {
                        "policy": "retain_owned_suite_and_candidate_evidence", "raw_deleted": False,
                        "root_index": copy.deepcopy(status["final_cleanup"]["root_index"]),
                        "kept_preview": list(status["final_cleanup"]["retained_roots_preview"])}
                atomic_write_json(status_path, status)
                _write_worker_shutdown(run_root, shutdown, status["final_cleanup"])
            except Exception as final_audit_error:
                audit_error = "%s: %s" % (type(final_audit_error).__name__, final_audit_error)
        if persistent_workers or netlist_optimizer is not None:
            try:
                shutdown_document = {
                    "schema_version": "crescendo.worker_shutdown.v1",
                    "workers": shutdown,
                    "finished_at": utc_timestamp(),
                }
            except Exception as error:
                audit_error = "%s: %s" % (error.__class__.__name__, error)
        if persistent_workers:
            from .retention import inspect_run_cleanup_scope
            scope = inspect_run_cleanup_scope(run_root)
            process_proven = all((item.get('result') or {}).get('process_termination_proven') is True
                                 for item in shutdown)
            remote_proven = all((item.get('result') or {}).get('remote_stop_proven') is True
                                for item in shutdown)
            final_artifacts_proven = bool(scope.get('artifact_cleanup_complete') is True
                and process_proven and remote_proven and all(item.get('ok') is True for item in shutdown))
            status['final_cleanup'] = {'process_termination_proven': process_proven,
                'remote_stop_proven': remote_proven,
                'artifact_cleanup_complete': final_artifacts_proven,
                'run_cleanup_scope': scope, 'observed_at': utc_timestamp()}
            earlier_attempts = list(status.get('batch_stop_terminations') or []) + list(status.get('batch_failure_terminations') or [])
            earlier_attempts.extend(item.get('result') or {} for item in status.get('controller_stop_terminations') or [])
            for earlier in earlier_attempts:
                if earlier.get('artifact_cleanup_complete') is False:
                    earlier['historical_attempt'] = True
                    earlier['final_artifact_cleanup_complete'] = final_artifacts_proven
                    earlier['earlier_failure_recovered'] = final_artifacts_proven
                    earlier['resolved_by_final_cleanup'] = {'ok': final_artifacts_proven,
                        'observed_at': status['final_cleanup']['observed_at']}
            if scope.get('artifact_cleanup_complete') is not True:
                shutdown.append({'worker_index': 'run_artifact_scope', 'ok': False,
                    'error': 'owned run artifacts remain; earlier cleanup proof does not cover them',
                    'result': scope})
            try:
                atomic_write_json(status_path, status)
                _write_worker_shutdown(run_root, shutdown, status['final_cleanup'])
            except Exception as final_audit_error:
                audit_error = str(final_audit_error)
        elif netlist_optimizer is not None:
            try:
                _write_worker_shutdown(run_root, shutdown)
            except Exception as final_audit_error:
                audit_error = str(final_audit_error)
        shutdown_failures = [row for row in shutdown if not row.get("ok")]
        if shutdown_failures or audit_error:
            message = "owned simulation worker shutdown/cleanup failed"
            if audit_error:
                message += "; audit write failed: %s" % audit_error
            failed_result_error = None
            if pending_result is not None:
                failed_result = copy.deepcopy(pending_result)
                failed_result["state"] = "failed"
                failed_result["stopped_reason"] = "persistent_worker_shutdown_failure"
                failed_result["worker_shutdown_failures"] = shutdown_failures
                if audit_error:
                    failed_result["worker_shutdown_audit_error"] = audit_error
                try:
                    try:
                        data_store.commit_result(failed_result)
                    except Exception:
                        pass
                    publish_result_summary(failed_result)
                except Exception as error:
                    failed_result_error = "%s: %s" % (error.__class__.__name__, error)
                    message += "; failed result write failed: %s" % failed_result_error
            status["cleanup_failure"] = {
                "message": message, "workers": shutdown_failures,
                "audit_error": audit_error, "failed_result_error": failed_result_error,
                "recovery_required": True,
            }
            status["secondary_cleanup_error"] = message
            if not active_exception:
                status.setdefault("primary_failure", {
                    "error_type": "RuntimeError", "message": message,
                    "failure_stage": "worker_cleanup",
                })
                status["message"] = message
            status.update({"state": "failed", "finished_at": utc_timestamp()})
            append_run_event(run_root, "cleanup_failed", message, status["cleanup_failure"], level="ERROR")
            try:
                atomic_write_json(status_path, status)
            except Exception:
                pass
            if not active_exception:
                raise RuntimeError(message)

    if pending_result is None:
        raise RuntimeError("optimization finalization produced no terminal result")
    try:
        data_store.commit_result(pending_result)
        if pending_assignment is not None:
            atomic_write_json(
                os.path.join(run_root, pending_assignment[0]), pending_assignment[1],
            )
        publish_result_summary(pending_result)
        status.update({
            "state": pending_result["state"],
            "message": pending_result["stopped_reason"],
            "finished_at": pending_result["finished_at"],
        })
        atomic_write_json(status_path, status)
    except Exception as error:
        message = "terminal result publication failed: %s: %s" % (
            error.__class__.__name__, error,
        )
        failed_result = copy.deepcopy(pending_result)
        failed_result["state"] = "failed"
        failed_result["stopped_reason"] = "terminal_result_publication_failure"
        failed_result["publication_error"] = message
        try:
            publish_result_summary(failed_result)
        except Exception:
            pass
        status.update({"state": "failed", "message": message, "finished_at": utc_timestamp()})
        try:
            atomic_write_json(status_path, status)
        except Exception:
            pass
        raise
    return pending_result
