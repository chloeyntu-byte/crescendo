from __future__ import print_function
from crescendo_engine.runtime import bind_text
_contract_text = bind_text()


import copy
import math
import os
import shutil
import threading
import traceback

from .hierarchy import parse_physical_number
from .engineering import merge_catalog_domain
from .optimizers import validate_variables
from .utils import atomic_write_json, ensure_dir, require_under, utc_timestamp
from .workspace import ParameterApplicationFailure, WorkspaceBuildFailure, apply_candidate, build_workspace, verify_sources_unchanged


TERMINAL_STATES = ("completed", "failed", "cancelled")


class VariableRangeValidationError(ValueError):
    def __init__(self, report):
        self.report = report
        self.issues = list(report.get("issues") or [])
        self.normalized_variables = list(report.get("variables") or [])
        message = "; ".join(issue.get("message") or "invalid parameter range"
                            for issue in self.issues)
        ValueError.__init__(self, message or "invalid parameter ranges")


def _same_value(requested, effective):
    requested_number = parse_physical_number(requested)
    effective_number = parse_physical_number(effective)
    if requested_number is None or effective_number is None:
        return str(requested) == str(effective)
    tolerance = max(1.0e-18, 1.0e-9 * max(abs(requested_number), abs(effective_number), 1.0e-30))
    return abs(requested_number - effective_number) <= tolerance


def _endpoint_rows(parameters, endpoint, requested, patch_result):
    verification = patch_result.get("schematic_verification") or {}
    by_id = dict((str(row["parameter_id"]), row) for row in verification.get("parameters") or [])
    rows = []
    for parameter in parameters:
        parameter_id = str(parameter["parameter_id"])
        requested_value = requested[parameter_id]
        verified = by_id.get(parameter_id)
        if parameter.get("backend") == "design_variable":
            effective = requested_value
            evidence = {"mode": "design_variable", "changes": patch_result.get("design_variable_changes") or []}
        elif verified:
            effective = verified.get("persisted_cdf_value", verified.get("cdf_value"))
            evidence = {
                "mode": "cdf_callback",
                "schematic_value": verified.get("persisted_schematic_value", verified.get("schematic_value")),
                "cdf_value": effective,
                "callback_status": verified.get("callback_status"),
                "callback_expression": verified.get("callback_expression"),
            }
        else:
            rows.append({
                "parameter_id": parameter_id, "endpoint": endpoint,
                "requested": requested_value, "effective": None,
                "status": "validation_failed", "error": "no callback verification record was produced",
            })
            continue
        rows.append({
            "parameter_id": parameter_id, "endpoint": endpoint,
            "requested": requested_value, "effective": effective,
            "status": "valid" if _same_value(requested_value, effective) else "pdk_adjusted",
            "adjustment": None if _same_value(requested_value, effective) else "quantized_or_clamped",
            "evidence": evidence,
        })
    return rows


def _safe_remove_workspace(workspace, job_root):
    root = os.path.abspath(workspace["root"])
    require_under(root, os.path.abspath(job_root), "range validation workspace")
    if os.path.islink(root):
        raise RuntimeError("range validation workspace unexpectedly became a symbolic link")
    source_check = verify_sources_unchanged(workspace)
    if not source_check["ok"]:
        raise RuntimeError("source library changed during range validation; private workspace preserved")
    from .same_library_views import cleanup_same_library_views
    cleanup = cleanup_same_library_views(workspace, process_cleanup_proven=True)
    if not cleanup["cleanup_proven"]:
        raise RuntimeError("range validation owned views remain; workspace preserved")
    shutil.rmtree(root)
    return source_check


def _native_endpoint_rows(parameters, endpoint, requested, manifest):
    application = manifest.get("parameter_application") or {}
    verified = application.get("proof_kind") == "native_run_device_parameters" and application.get("ok") is True
    by_id = {}
    for row in application.get("parameters") or []:
        key = str(row.get("parameter_id"))
        if key in by_id:
            raise ValueError("Duplicate native parameter validation evidence")
        by_id[key] = row
    result = []
    for parameter in parameters:
        key = str(parameter["parameter_id"])
        row = by_id.get(key) or {}
        points = row.get("points") or []
        numbers = [parse_physical_number(point.get("effective")) for point in points]
        readable = bool(points) and all(value is not None and math.isfinite(value) for value in numbers)
        adjustment_proven = bool(points) and all(point.get("callback_adjustment_proven") is True for point in points)
        consistent = readable and all(_same_value(numbers[0], value) for value in numbers[1:])
        effective = numbers[0] if verified and consistent and adjustment_proven else None
        item = {"parameter_id": key, "endpoint": endpoint, "requested": requested[key],
                "effective": effective,
                "evidence": {"mode": "native_run_device_parameters", "points": copy.deepcopy(points),
                             "private_oa_backannotation_proven": False}}
        if effective is None:
            item.update(status="validation_failed", error=(
                "Native PDK values differ between Test/corner points; a single adjusted bound cannot be applied."
                if verified and readable and not consistent else
                "Native Run bound the requested DPAR value, but the PDK-adjusted device field could not be confirmed; inspect retained SCS."
                if verified and readable and not adjustment_proven else
                "Native Run did not provide complete selected-device values; inspect the retained SCS and capture log."))
        else:
            same = _same_value(requested[key], effective)
            item.update(status="valid" if same else "pdk_adjusted",
                        adjustment=None if same else "quantized_or_clamped")
        result.append(item)
    return result


def run_range_validation(job, prepared, variables, config, cancel_event, update,
                         resident_session=None):
    """Validate endpoints through the candidate's native Run generation path.

    No solver or scheduler is started.  SCS and capture evidence are retained;
    only owned private OA views are cleaned after native-process termination.
    """
    from .netlist_providers import prepare_maestro_netlists
    from .private_netlist import reconcile_preparation
    from .variable_scope_contract import require_unique_run_variables
    parameters_by_id = {str(row["parameter_id"]): row for row in prepared.get("parameters") or []
                        if row.get("optimizable")}
    parameters = [parameters_by_id[str(row["parameter_id"])] for row in variables]
    all_rows = []
    try:
        require_unique_run_variables(prepared.get("setup") or {},
                                     action="validate native parameter ranges", multiplicity_only=True)
        if resident_session is not None:
            raise RuntimeError("Native Run range validation cannot use a legacy direct-CDF session")
        update(state="running", message="Generating native endpoint netlists", started_at=utc_timestamp(),
               validation_method="native_run_device_parameters", solver_invocations=0)
        for endpoint in ("minimum", "maximum"):
            if cancel_event.is_set():
                break
            root = os.path.join(job["job_root"], endpoint)
            requested = {str(variable["parameter_id"]): variable[endpoint] for variable in variables}
            for variable, parameter in zip(variables, parameters):
                domain = merge_catalog_domain(variable, parameter)
                if domain.get("numeric_choices"):
                    domain = validate_variables([domain])[0]
                    requested[str(variable["parameter_id"])] = (
                        min if endpoint == "minimum" else max
                    )(domain["choices"], key=parse_physical_number)
            update(active_endpoint=endpoint, message="Generating %s endpoint with native Maestro Run" % endpoint)
            error = None
            try:
                manifest = prepare_maestro_netlists(
                    prepared, parameters, requested, config, root, "range_" + endpoint,
                    cancel_check=cancel_event.is_set, generation_only=True,
                    allow_effective_adjustment=True,
                )
                all_rows.extend(_native_endpoint_rows(parameters, endpoint, requested, manifest))
            except Exception as caught:
                error = caught
                if not cancel_event.is_set():
                    all_rows.extend({"parameter_id": str(parameter["parameter_id"]), "endpoint": endpoint,
                                     "requested": requested[str(parameter["parameter_id"])], "effective": None,
                                     "status": "validation_failed", "error": str(caught),
                                     "evidence": {"mode": "native_run_device_parameters", "capture_root": root}}
                                    for parameter in parameters)
            finally:
                try:
                    cleanup = reconcile_preparation(root, request_cancel=True,
                        timeout_seconds=float(config.get("runtime", {}).get("range_validation_shutdown_timeout_seconds", 15)))
                except Exception as cleanup_error:
                    cleanup = {"cleanup_proven": False, "reason": str(cleanup_error)}
                atomic_write_json(os.path.join(root, _contract_text("range_cleanup.json")), cleanup)
            if not cleanup.get("cleanup_proven") or bool(error and (
                    getattr(error, "unsafe_to_optimize", False) or getattr(error, "cleanup_failed", False))):
                update(state="failed", message="Native validation process cleanup could not be proven; evidence retained.",
                       active_endpoint=None, results=all_rows, unsafe_to_optimize=True,
                       failure_stage="native_generation_termination",
                       unsafe_cleanup_scope="native_range_capture", cleanup_root=root,
                       cleanup_error=str(error or cleanup.get("reason") or cleanup), finished_at=utc_timestamp())
                return
            update(results=all_rows, retained_capture_roots=[
                os.path.join(job["job_root"], name) for name in ("minimum", "maximum")
                if os.path.isdir(os.path.join(job["job_root"], name))])
            # A native generation failure has one useful first cause; do not
            # repeat the same failure for the maximum endpoint.
            if error is not None:
                if cancel_event.is_set():
                    break
                update(state="failed", message="Native endpoint generation failed; inspect retained SCS/capture evidence.",
                       detail=str(error), active_endpoint=None, results=all_rows,
                       failure_stage="workspace_build" if isinstance(error, WorkspaceBuildFailure) else
                                     (getattr(error, "detail", {}) or {}).get("failure_stage", "native_parameter_generation"),
                       finished_at=utc_timestamp())
                return
        cancelled = cancel_event.is_set()
        update(state="cancelled" if cancelled else "completed",
               message="Native range validation cancelled" if cancelled else "Native range validation completed",
               active_endpoint=None, results=all_rows, finished_at=utc_timestamp())
    except Exception as error:
        update(state="cancelled" if cancel_event.is_set() else "failed", message=str(error),
               active_endpoint=None, results=all_rows, traceback=traceback.format_exc(),
               unsafe_to_optimize=bool(getattr(error, "unsafe_to_optimize", False)),
               finished_at=utc_timestamp())



def _parameter_context(parameter_id, parameter):
    parameter = parameter or {}
    hierarchy_path = parameter.get("hierarchy_path")
    instance_name = parameter.get("instance_name")
    if not instance_name and hierarchy_path and not str(hierarchy_path).startswith("$"):
        instance_name = str(hierarchy_path).rstrip("/").rsplit("/", 1)[-1] or None
    return {
        "parameter_id": parameter_id or None,
        "hierarchy_path": hierarchy_path,
        "instance_name": instance_name,
        "master_library": parameter.get("master_library"),
        "master_cell": parameter.get("master_cell"),
        "parameter_name": parameter.get("name"),
        "prompt": parameter.get("prompt") or parameter.get("name"),
    }


def _parameter_label(context):
    parts = []
    if context.get("hierarchy_path"):
        parts.append(str(context["hierarchy_path"]))
    master_library = context.get("master_library")
    master_cell = context.get("master_cell")
    if master_library or master_cell:
        parts.append("%s/%s" % (master_library or "?", master_cell or "?"))
    prompt = context.get("prompt")
    parameter_name = context.get("parameter_name")
    if prompt and parameter_name and prompt != parameter_name:
        parts.append("%s [%s]" % (prompt, parameter_name))
    elif prompt or parameter_name:
        parts.append(str(prompt or parameter_name))
    return " / ".join(parts) or "selected parameter"


def _bound_state(raw_value):
    if raw_value is None or (isinstance(raw_value, str) and not raw_value.strip()):
        return "missing", None
    try:
        normalized = parse_physical_number(raw_value)
    except (TypeError, ValueError, OverflowError):
        normalized = None
    if normalized is not None:
        return "valid", normalized
    if isinstance(raw_value, float) and not math.isfinite(raw_value):
        return "non_finite", None
    if isinstance(raw_value, int) and not isinstance(raw_value, bool):
        # Huge integers overflow float; str() of them is also refused on
        # Python 3.11+ (integer string conversion limit), so test directly.
        try:
            numeric = float(raw_value)
        except OverflowError:
            return "non_finite", None
        return ("non_finite", None) if not math.isfinite(numeric) else ("malformed", None)
    try:
        numeric = float(str(raw_value).strip().strip('"'))
    except (TypeError, ValueError, OverflowError):
        numeric = None
    if numeric is not None and not math.isfinite(numeric):
        return "non_finite", None
    return "malformed", None


def _validation_issue(code, parameter_id, parameter, field, row_index,
                      raw_minimum, raw_maximum, normalized_minimum,
                      normalized_maximum, message):
    issue = _parameter_context(parameter_id, parameter)
    if field == "minimum":
        raw_value = raw_minimum
        normalized_value = normalized_minimum
    elif field == "maximum":
        raw_value = raw_maximum
        normalized_value = normalized_maximum
    elif field == "range":
        raw_value = {"minimum": raw_minimum, "maximum": raw_maximum}
        normalized_value = {
            "minimum": normalized_minimum, "maximum": normalized_maximum,
        }
    else:
        raw_value = None
        normalized_value = None
    issue.update({
        "code": code,
        "field": field,
        "row_index": row_index,
        "raw_value": raw_value,
        "normalized_value": normalized_value,
        "raw_minimum": raw_minimum,
        "raw_maximum": raw_maximum,
        "normalized_minimum": normalized_minimum,
        "normalized_maximum": normalized_maximum,
        "message": message,
    })
    return issue


def validate_variable_request_report(prepared, variables):
    """Return all static range issues without starting Cadence or mutating input."""
    prepared = prepared or {}
    available = dict(
        (str(row["parameter_id"]), row) for row in prepared.get("parameters") or []
        if row.get("optimizable")
    )
    issues = []
    normalized = []
    seen = set()
    if not isinstance(variables, (list, tuple)) or not variables:
        issues.append(_validation_issue(
            "no_variables", None, None, "selection", None,
            None, None, None, None,
            "select at least one optimizable parameter",
        ))
        return {"valid": False, "variables": normalized, "issues": issues}

    for row_index, row in enumerate(variables):
        if not isinstance(row, dict):
            issues.append(_validation_issue(
                "invalid_variable", None, None, "selection", row_index,
                None, None, None, None,
                "variable entry %d must be an object" % (row_index + 1),
            ))
            continue
        parameter_id = str(row.get("parameter_id") or "")
        parameter = available.get(parameter_id)
        minimum = row.get("minimum")
        maximum = row.get("maximum")
        minimum_state, normalized_minimum = _bound_state(minimum)
        maximum_state, normalized_maximum = _bound_state(maximum)
        context = _parameter_context(parameter_id, parameter)
        label = _parameter_label(context)
        row_issues = []

        if parameter is None:
            row_issues.append(_validation_issue(
                "parameter_unavailable", parameter_id, None, "parameter", row_index,
                minimum, maximum, normalized_minimum, normalized_maximum,
                "selected parameter is unavailable or not optimizable",
            ))
        elif parameter_id in seen:
            row_issues.append(_validation_issue(
                "duplicate_parameter", parameter_id, parameter, "parameter", row_index,
                minimum, maximum, normalized_minimum, normalized_maximum,
                "duplicate parameter selection for %s" % label,
            ))
        else:
            seen.add(parameter_id)

        for field, state, raw_value in (
                ("minimum", minimum_state, minimum),
                ("maximum", maximum_state, maximum)):
            if state == "valid":
                continue
            if state == "missing":
                code = "missing_value"
                message = "%s is required for %s" % (field, label)
            elif state == "non_finite":
                code = "non_finite_value"
                message = "%s must be finite for %s" % (field, label)
            else:
                code = "malformed_value"
                message = "%s must be a numeric physical value for %s" % (field, label)
            row_issues.append(_validation_issue(
                code, parameter_id, parameter, field, row_index,
                minimum, maximum, normalized_minimum, normalized_maximum, message,
            ))

        if minimum_state == "valid" and maximum_state == "valid":
            if normalized_minimum > normalized_maximum:
                row_issues.append(_validation_issue(
                    "invalid_range_order", parameter_id, parameter, "range", row_index,
                    minimum, maximum, normalized_minimum, normalized_maximum,
                    "minimum exceeds maximum for %s; minimum must be less than maximum" % label,
                ))
            elif normalized_minimum == normalized_maximum:
                row_issues.append(_validation_issue(
                    "invalid_range_order", parameter_id, parameter, "range", row_index,
                    minimum, maximum, normalized_minimum, normalized_maximum,
                    "minimum must be less than maximum for %s; equal bounds are not allowed" % label,
                ))

        if not row_issues and parameter is not None:
            try:
                validate_variables([merge_catalog_domain(row, parameter)])
            except (TypeError, ValueError, OverflowError) as error:
                row_issues.append(_validation_issue(
                    "invalid_parameter_domain", parameter_id, parameter, "range", row_index,
                    minimum, maximum, normalized_minimum, normalized_maximum,
                    "invalid optimization range for %s: %s" % (label, error),
                ))

        issues.extend(row_issues)
        if not row_issues:
            normalized.append({
                "parameter_id": parameter_id, "minimum": minimum, "maximum": maximum,
            })
    return {"valid": not issues, "variables": normalized, "issues": issues}


def validate_variable_request(prepared, variables):
    report = validate_variable_request_report(prepared, variables)
    if not report["valid"]:
        raise VariableRangeValidationError(report)
    return report["variables"]
