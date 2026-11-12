from __future__ import print_function

import hashlib
import os
import threading

from crescendo_engine.runtime import bind_text
_contract_text = bind_text()

from .hierarchy import parse_physical_number
from .utils import atomic_write_json, ensure_dir, utc_timestamp


RUNTIME_DESIGN_VAR = "runtime_design_var"
PARAMETERIZED_INSTANCE_SCALAR = "parameterized_instance_scalar"
REQUIRES_RECOMPILE = "requires_recompile"
UNSUPPORTED = "unsupported"

PARAMETER_CLASSES = (
    RUNTIME_DESIGN_VAR,
    PARAMETERIZED_INSTANCE_SCALAR,
    REQUIRES_RECOMPILE,
    UNSUPPORTED,
)

_SCALAR_SEMANTICS = set((
    "width", "length", "multiplier", "resistance", "capacitance", "inductance",
    "voltage", "current", "amplitude", "phase", "frequency", "delay", "impedance",
))
_TOPOLOGY_SEMANTICS = set(("fingers", "segments", "array", "turns"))
_RECOMPILE_FLAGS = (
    "topology_changing", "requires_callback_per_value", "cdf_callback_dynamic",
    "derived_parameters_dynamic", "requires_recompile",
)


class FastWorkerError(RuntimeError):
    pass


class FastWorkerFallbackRequired(FastWorkerError):
    pass


class FastWorkerInitializationError(FastWorkerError):
    pass


def _normalized_text(value):
    return "".join(character for character in str(value or "").lower() if character.isalnum())


def _numeric_value(parameter):
    value = parameter.get("numeric_value")
    if value is not None:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
    return parse_physical_number(parameter.get("current_value"))


def classify_parameter(parameter):
    """Classify one catalog parameter without claiming it passed OA/netlist validation."""
    parameter_id = parameter.get("parameter_id")
    backend = parameter.get("backend") or "schematic"
    explicit = parameter.get("fast_worker_class") or parameter.get("parametric_class")
    if explicit and explicit not in PARAMETER_CLASSES:
        return {
            "parameter_id": parameter_id,
            "classification": UNSUPPORTED,
            "reason": "invalid explicit fast-worker classification: %s" % explicit,
            "requires_validation": False,
        }

    if backend == "design_variable":
        if not parameter.get("name"):
            classification = UNSUPPORTED
            reason = "design variable has no name"
        else:
            classification = explicit or RUNTIME_DESIGN_VAR
            reason = "existing Maestro design variable"
        return {
            "parameter_id": parameter_id,
            "classification": classification,
            "reason": reason,
            "requires_validation": False,
        }

    if backend != "schematic":
        return {
            "parameter_id": parameter_id,
            "classification": UNSUPPORTED,
            "reason": "unsupported parameter backend: %s" % backend,
            "requires_validation": False,
        }

    required = ("parameter_id", "name", "hierarchy_path", "owner_library", "owner_cell")
    missing = [name for name in required if not parameter.get(name)]
    if missing:
        return {
            "parameter_id": parameter_id,
            "classification": UNSUPPORTED,
            "reason": "missing schematic identity: %s" % ", ".join(missing),
            "requires_validation": False,
        }
    if parameter.get("editable") is False or parameter.get("tunable") is False:
        return {
            "parameter_id": parameter_id,
            "classification": UNSUPPORTED,
            "reason": "schematic parameter is not editable/tunable",
            "requires_validation": False,
        }
    if _numeric_value(parameter) is None:
        return {
            "parameter_id": parameter_id,
            "classification": UNSUPPORTED,
            "reason": "schematic parameter has no numeric seed value",
            "requires_validation": False,
        }

    if explicit:
        classification = explicit
        reason = "explicit fast-worker classification"
    elif any(bool(parameter.get(flag)) for flag in _RECOMPILE_FLAGS):
        classification = REQUIRES_RECOMPILE
        reason = "parameter metadata requires per-candidate OA/CDF recompilation"
    elif parameter.get("parametric_safe") is False:
        classification = REQUIRES_RECOMPILE
        reason = "parameter is explicitly unsafe for symbolic OA parameterization"
    else:
        semantic = parameter.get("semantic") or _normalized_text(parameter.get("name"))
        if semantic in _TOPOLOGY_SEMANTICS:
            classification = REQUIRES_RECOMPILE
            reason = "parameter may alter PCell topology or derived CDF values"
        elif semantic in _SCALAR_SEMANTICS or parameter.get("parametric_safe") is True:
            classification = PARAMETERIZED_INSTANCE_SCALAR
            reason = "scalar candidate; OA/netlist sentinel validation is still required"
        else:
            classification = REQUIRES_RECOMPILE
            reason = "unknown CDF semantics are not enabled for the fast path"
    return {
        "parameter_id": parameter_id,
        "classification": classification,
        "reason": reason,
        "requires_validation": classification == PARAMETERIZED_INSTANCE_SCALAR,
    }


def _generated_design_variable(parameter):
    identity = "\x1f".join(str(parameter.get(name) or "") for name in (
        "design_library", "design_cell", "design_view", "owner_occurrence_path",
        "hierarchy_path", "owner_library", "owner_cell", "name", "parameter_id",
    ))
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:20]
    return _contract_text("CRESCENDOFW_%s") % digest


def _plan_entry(parameter):
    classified = classify_parameter(parameter)
    entry = dict(classified)
    entry.update({
        "backend": parameter.get("backend") or "schematic",
        "name": parameter.get("name"),
        "hierarchy_path": parameter.get("hierarchy_path"),
        "owner_occurrence_path": parameter.get("owner_occurrence_path") or "/",
        "owner_library": parameter.get("owner_library"),
        "owner_cell": parameter.get("owner_cell"),
        "property": parameter.get("name"),
    })
    if entry["classification"] == RUNTIME_DESIGN_VAR:
        entry["design_variable"] = parameter.get("name")
    elif entry["classification"] == PARAMETERIZED_INSTANCE_SCALAR:
        entry["design_variable"] = _generated_design_variable(parameter)
    else:
        entry["design_variable"] = None
    return entry


def build_parametric_plan(parameters):
    entries = [_plan_entry(parameter) for parameter in parameters]
    runtime_names = {}
    generated_names = {}
    for entry in entries:
        name = entry.get("design_variable")
        if not name:
            continue
        collection = runtime_names if entry["classification"] == RUNTIME_DESIGN_VAR else generated_names
        collection.setdefault(name, []).append(entry)

    for name, duplicates in runtime_names.items():
        if len(duplicates) < 2:
            continue
        for entry in duplicates:
            entry.update({
                "classification": UNSUPPORTED,
                "reason": "ambiguous repeated Maestro design variable name: %s" % name,
                "requires_validation": False,
                "design_variable": None,
            })
    collisions = [name for name, rows in generated_names.items() if len(rows) > 1]
    if collisions:
        raise FastWorkerError("generated design-variable collision: %s" % ", ".join(sorted(collisions)))

    counts = dict((name, 0) for name in PARAMETER_CLASSES)
    for entry in entries:
        counts[entry["classification"]] += 1
    blocking = [entry for entry in entries if entry["classification"] in (REQUIRES_RECOMPILE, UNSUPPORTED)]
    return {
        "schema_version": _contract_text("crescendo.fast_worker.plan.v1"),
        "created_at": utc_timestamp(),
        "eligible": bool(entries) and not blocking,
        "counts": counts,
        "parameters": entries,
        "blocking_parameter_ids": [entry.get("parameter_id") for entry in blocking],
    }


def candidate_design_variables(plan, values):
    assignments = {}
    missing = []
    for entry in plan.get("parameters", []):
        if entry["classification"] not in (RUNTIME_DESIGN_VAR, PARAMETERIZED_INSTANCE_SCALAR):
            continue
        parameter_id = entry["parameter_id"]
        if parameter_id not in values:
            missing.append(parameter_id)
            continue
        name = entry["design_variable"]
        if name in assignments and assignments[name] != values[parameter_id]:
            raise FastWorkerError("conflicting candidate values for design variable %s" % name)
        assignments[name] = values[parameter_id]
    if missing:
        raise FastWorkerError("candidate is missing parameter values: %s" % ", ".join(sorted(missing)))
    return assignments


def validate_initialization_evidence(plan, evidence):
    if not isinstance(evidence, dict):
        raise FastWorkerInitializationError("OA initializer returned no validation evidence")
    rows = evidence.get("parameters") or []
    by_id = {}
    for row in rows:
        parameter_id = row.get("parameter_id")
        if parameter_id in by_id:
            raise FastWorkerInitializationError("duplicate initialization evidence for %s" % parameter_id)
        by_id[parameter_id] = row
    verified = []
    for entry in plan.get("parameters", []):
        if entry["classification"] != PARAMETERIZED_INSTANCE_SCALAR:
            continue
        row = by_id.get(entry["parameter_id"])
        if not row or row.get("status") != "validated":
            raise FastWorkerInitializationError(
                "symbolic OA/netlist validation did not pass for %s" % entry["parameter_id"]
            )
        if row.get("design_variable") != entry["design_variable"]:
            raise FastWorkerInitializationError(
                "initialization evidence design variable mismatch for %s" % entry["parameter_id"]
            )
        if (row.get("owner_occurrence_path") or "/") != entry["owner_occurrence_path"]:
            raise FastWorkerInitializationError(
                "initialization evidence occurrence mismatch for %s" % entry["parameter_id"]
            )
        verified.append(entry["parameter_id"])
    return {
        "schema_version": _contract_text("crescendo.fast_worker.initialization_evidence.v1"),
        "validated_parameter_ids": sorted(verified),
        "raw": evidence,
    }


def patch_private_design_variables(workspace, assignments):
    """Update existing variables only inside a private worker setup."""
    from .workspace import _patch_design_vars

    setup_dir = workspace.get("setup_dir")
    root = workspace.get("root")
    if not setup_dir or not root:
        raise FastWorkerError("workspace is missing root/setup_dir")
    setup_dir = os.path.realpath(setup_dir)
    root = os.path.realpath(root)
    if setup_dir != root and not setup_dir.startswith(root + os.sep):
        raise FastWorkerError("worker setup escapes working workspace")
    changes = []
    for current, _directories, files in os.walk(setup_dir):
        for name in files:
            if name == "maestro.sdb" or name.endswith(".state"):
                changes.extend(_patch_design_vars(os.path.join(current, name), assignments))
    changed_names = set(row["name"] for row in changes)
    missing = sorted(set(assignments) - changed_names)
    if missing:
        raise FastWorkerError(
            "Maestro working copy setup does not define design variable(s): %s" % ", ".join(missing)
        )
    return {"changes": changes, "design_variables": sorted(changed_names)}


class ParametricWorker(object):
    """One working OA workspace with fresh Maestro execution per candidate.

    ``oa_initializer`` owns the Cadence-specific one-time operation: occurrence cloning,
    symbolic CDF assignment, callbacks, and two-sentinel netlist validation. It must
    return evidence accepted by ``validate_initialization_evidence``. This class never
    treats missing evidence as eligibility.
    """

    def __init__(self, setup, hierarchy, config, parameters, worker_root,
                 oa_initializer=None, fallback_evaluator=None, workspace_builder=None,
                 design_variable_updater=None, maestro_runner=None):
        if workspace_builder is None:
            from .workspace import build_workspace
            workspace_builder = build_workspace
        if design_variable_updater is None:
            design_variable_updater = patch_private_design_variables
        if maestro_runner is None:
            from .workspace import run_maestro
            maestro_runner = run_maestro
        self.setup = setup
        self.hierarchy = hierarchy
        self.config = config
        self.parameters = list(parameters)
        self.worker_root = os.path.abspath(worker_root)
        self.oa_initializer = oa_initializer
        self.fallback_evaluator = fallback_evaluator
        self.workspace_builder = workspace_builder
        self.design_variable_updater = design_variable_updater
        self.maestro_runner = maestro_runner
        self.plan = build_parametric_plan(self.parameters)
        self.workspace = None
        self.initialization_evidence = None
        self.state = "new"
        self.reason = None
        self._lock = threading.Lock()

    def _write_state(self):
        ensure_dir(self.worker_root)
        atomic_write_json(os.path.join(self.worker_root, _contract_text("fast_worker_state.json")), {
            "schema_version": _contract_text("crescendo.fast_worker.state.v1"),
            "state": self.state,
            "reason": self.reason,
            "plan": self.plan,
            "workspace_root": self.workspace.get("root") if self.workspace else None,
            "initialization_evidence": self.initialization_evidence,
            "updated_at": utc_timestamp(),
        })

    def _disable(self, reason):
        self.state = "fallback"
        self.reason = str(reason)
        self._write_state()
        return False

    def initialize(self, seed_values=None):
        if self.state == "ready":
            return True
        if self.state == "fallback":
            return False
        if not self.plan.get("eligible"):
            return self._disable(
                "fast path blocked by parameter classification: %s" %
                ", ".join(self.plan.get("blocking_parameter_ids") or [])
            )
        if self.oa_initializer is None:
            return self._disable("OA parametric initializer hook is not configured")
        seed_values = seed_values or dict(
            (parameter["parameter_id"], _numeric_value(parameter))
            for parameter in self.parameters if parameter.get("parameter_id")
        )
        try:
            workspace_root = os.path.join(self.worker_root, "workspace")
            self.workspace = self.workspace_builder(
                self.setup, self.hierarchy, self.config, workspace_root,
            )
            raw_evidence = self.oa_initializer(
                self.workspace, self.plan, seed_values, self.worker_root,
            )
            self.initialization_evidence = validate_initialization_evidence(self.plan, raw_evidence)
        except Exception as error:
            return self._disable("fast-worker initialization failed: %s: %s" % (
                error.__class__.__name__, error,
            ))
        self.state = "ready"
        self.reason = None
        self._write_state()
        return True

    def _fallback(self, values, candidate_root, reason):
        if self.fallback_evaluator is None:
            raise FastWorkerFallbackRequired(reason)
        return {
            "mode": "fallback",
            "reason": reason,
            "result": self.fallback_evaluator(values, candidate_root, reason),
        }

    def evaluate(self, values, candidate_root):
        with self._lock:
            if not self.initialize(seed_values=values):
                return self._fallback(values, candidate_root, self.reason)
            try:
                assignments = candidate_design_variables(self.plan, values)
                update = self.design_variable_updater(self.workspace, assignments)
            except Exception as error:
                self._disable("candidate design-variable update failed: %s: %s" % (
                    error.__class__.__name__, error,
                ))
                return self._fallback(values, candidate_root, self.reason)
            command = self.maestro_runner(
                self.workspace, self.config, candidate_root,
            )
            return {
                "mode": "parametric_worker",
                "design_variables": assignments,
                "update": update,
                "command": command,
            }
