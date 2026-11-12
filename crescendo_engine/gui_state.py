from __future__ import print_function
from crescendo_engine.runtime import bind_text
_contract_text = bind_text()


import math
import os
import queue
import threading
import time

from .engineering import normalize_choices, try_parse_engineering_number


PHYSICAL_SCALES = {
    "t": 1e12,
    "g": 1e9,
    "meg": 1e6,
    "k": 1e3,
    "m": 1e-3,
    "u": 1e-6,
    "n": 1e-9,
    "p": 1e-12,
    "f": 1e-15,
    "a": 1e-18,
}

ALGORITHM_NAMES = {
    "nsga2": "NSGA-II",
    "differential_evolution": "Differential Evolution",
    "bayesian_optimization": "Bayesian Optimization",
}

ALGORITHM_FIELDS = {
    "nsga2": (
        "population", "seed", "crossover_probability", "crossover_eta",
        "mutation_probability", "mutation_eta",
    ),
    "differential_evolution": (
        "population", "seed", "differential_weight", "crossover_rate",
    ),
    "bayesian_optimization": (
        "population", "seed", "initial_samples", "acquisition_exploration",
        "candidate_pool", "noise", "kernel_length_scale", "max_observations",
    ),
}

TERMINAL_CANDIDATE_STATES = frozenset((
    "completed", "failed", "infrastructure_failed", "stopped",
))


def finite_number(value):
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return numeric if math.isfinite(numeric) else None


def physical_unit(parameter):
    raw = str(parameter.get("current_value", parameter.get("default_value", "")) or "")
    raw = raw.strip().strip('"')
    suffix = []
    for character in reversed(raw):
        if character.isalpha():
            suffix.append(character)
        else:
            break
    unit = "".join(reversed(suffix))
    return unit if unit.lower() in PHYSICAL_SCALES else ""


def parse_physical_input(value, fallback_unit=""):
    text = str(value if value is not None else "").strip()
    if not text:
        return None
    index = len(text)
    while index and text[index - 1].isalpha():
        index -= 1
    number_text = text[:index]
    unit = text[index:] or fallback_unit
    numeric = finite_number(number_text)
    if numeric is None:
        return None
    if unit:
        scale = PHYSICAL_SCALES.get(unit.lower())
        if scale is None:
            return None
        numeric *= scale
    return numeric if math.isfinite(numeric) else None


def format_physical(value, unit=""):
    numeric = finite_number(value)
    if numeric is None:
        return ""
    scale = PHYSICAL_SCALES.get(unit.lower(), 1.0) if unit else 1.0
    adjusted = numeric / scale
    return "{:.8g}".format(adjusted)


def bound_default(parameter, side, unit=""):
    value = finite_number(parameter.get("numeric_value"))
    if value is None:
        return ""
    integer = parameter.get("value_type") == "integer"
    choices = normalize_choices(parameter.get("choices", parameter.get("choices_raw")))
    numbers = [try_parse_engineering_number(choice) for choice in choices]
    if numbers and all(number is not None for number in numbers):
        bound = (min if side == "minimum" else max)(numbers)
        return format_physical(bound, unit)
    defined = parameter.get("minimum" if side == "minimum" else "maximum")
    if defined not in (None, ""):
        parsed = parse_physical_input(defined, unit)
        if parsed is not None:
            return str(int(round(parsed))) if integer else format_physical(parsed, unit)
    if integer:
        center = int(round(value))
        minimum = max(1, int(math.floor(center * 0.5)))
        maximum = max(2, int(math.ceil(center * 2.5)), minimum + 1)
        return str(minimum if side == "minimum" else maximum)
    if value > 0:
        bound = value * (0.5 if side == "minimum" else 2.5)
    elif value < 0:
        bound = value * (2.5 if side == "minimum" else 0.5)
    else:
        bound = 0.0 if side == "minimum" else 1.0
    return format_physical(bound, unit)


def parameter_label(parameter):
    path = (
        parameter.get("hierarchy_path") or parameter.get("instance_path") or
        parameter.get("path") or "/"
    )
    instance_name = parameter.get("instance_name") or os.path.basename(path.rstrip("/"))
    master_library = parameter.get("master_library") or parameter.get("library") or ""
    master_cell = parameter.get("master_cell") or parameter.get("cell") or ""
    name = parameter.get("name") or parameter.get("cdf_name") or "parameter"
    prompt = parameter.get("prompt") or ""
    owner = instance_name or path
    master = "/".join(item for item in (master_library, master_cell) if item)
    parameter_text = "{} [{}]".format(prompt, name) if prompt and prompt != name else name
    pieces = [path]
    if owner and owner != path:
        pieces.append(owner)
    if master:
        pieces.append("({})".format(master))
    pieces.append(parameter_text)
    return " / ".join(pieces)


def parameter_group_name(parameter):
    name = parameter.get("name") or parameter.get("cdf_name") or "parameter"
    return str(name).strip() or "parameter"


def parameter_owner_label(parameter):
    path = (
        parameter.get("hierarchy_path") or parameter.get("instance_path") or
        parameter.get("path") or "/"
    )
    instance_name = parameter.get("instance_name") or os.path.basename(path.rstrip("/"))
    master_library = parameter.get("master_library") or parameter.get("library") or ""
    master_cell = parameter.get("master_cell") or parameter.get("cell") or ""
    master = "/".join(item for item in (master_library, master_cell) if item)
    pieces = [path]
    if instance_name and instance_name not in (path, os.path.basename(path.rstrip("/"))):
        pieces.append(instance_name)
    if master:
        pieces.append("({})".format(master))
    return " / ".join(str(item) for item in pieces if item) or "/"


def validated_run_display_name(value, maximum_length=120):
    if not isinstance(value, str):
        raise ValueError("Run name must be text.")
    display_name = value.strip()
    if not display_name:
        raise ValueError("Run name is required.")
    if len(display_name) > int(maximum_length):
        raise ValueError("Run name must be at most {} characters.".format(maximum_length))
    if any(not character.isprintable() for character in display_name):
        raise ValueError("Run name must not contain control characters.")
    return display_name


def group_parameters_by_name(parameters):
    groups = {}
    for parameter in parameters:
        display_name = parameter_group_name(parameter)
        key = display_name.lower()
        group = groups.setdefault(key, {
            "key": key, "name": display_name, "parameters": [],
        })
        group["parameters"].append(parameter)
    result = []
    for key in sorted(groups):
        group = groups[key]
        group["parameters"].sort(key=lambda row: (
            parameter_owner_label(row).lower(),
            str(row.get("parameter_id") or ""),
        ))
        result.append(group)
    return result


def issue_display_name(issue):
    if issue.get("display_name"):
        return str(issue["display_name"])
    path = issue.get("hierarchy_path")
    instance = issue.get("instance_name")
    library = issue.get("master_library")
    cell = issue.get("master_cell")
    name = issue.get("parameter_name") or issue.get("parameter_id")
    prompt = issue.get("prompt")
    pieces = []
    if path:
        pieces.append(str(path))
    elif instance:
        pieces.append(str(instance))
    master = "/".join(str(item) for item in (library, cell) if item)
    if master:
        pieces.append("({})".format(master))
    if prompt and name and prompt != name:
        pieces.append("{} [{}]".format(prompt, name))
    elif prompt or name:
        pieces.append(str(prompt or name))
    return " / ".join(pieces) or "Parameter selection"


def initial_parameter_config(parameter):
    unit = physical_unit(parameter)
    semantic = str(parameter.get("semantic") or "").lower()
    selected = bool(parameter.get("default_selected")) or semantic in ("width", "length")
    value_type = parameter.get("value_type") or "float"
    return {
        "selected": selected,
        "unit": unit,
        "minimum": bound_default(parameter, "minimum", unit),
        "maximum": bound_default(parameter, "maximum", unit),
        "scale": "linear",
        "value_type": value_type,
    }


def validate_variable_configs(parameters, configs, require_selected=True):
    by_id = dict((row.get("parameter_id"), row) for row in parameters)
    variables = []
    issues = []
    selected_ids = [parameter_id for parameter_id, config in configs.items()
                    if config.get("selected")]
    if require_selected and not selected_ids:
        issues.append({
            "code": "no_parameters",
            "parameter_id": None,
            "field": "selection",
            "message": "Select at least one optimizable parameter.",
            "display_name": "Parameter selection",
        })
    for parameter_id in selected_ids:
        config = configs[parameter_id]
        parameter = by_id.get(parameter_id, {"parameter_id": parameter_id, "name": parameter_id})
        label = parameter_label(parameter)
        unit = config.get("unit") or ""
        minimum = parse_physical_input(config.get("minimum"), unit)
        maximum = parse_physical_input(config.get("maximum"), unit)
        if minimum is None:
            issues.append({
                "code": "invalid_minimum", "parameter_id": parameter_id,
                "field": "minimum", "message": "Minimum must be a finite numeric value.",
                "display_name": label, "raw_value": config.get("minimum"),
            })
        if maximum is None:
            issues.append({
                "code": "invalid_maximum", "parameter_id": parameter_id,
                "field": "maximum", "message": "Maximum must be a finite numeric value.",
                "display_name": label, "raw_value": config.get("maximum"),
            })
        if minimum is not None and maximum is not None and minimum >= maximum:
            issues.append({
                "code": "invalid_order", "parameter_id": parameter_id,
                "field": "range",
                "message": "Minimum must be strictly smaller than maximum.",
                "display_name": label, "minimum": minimum, "maximum": maximum,
            })
        value_type = config.get("value_type") or parameter.get("value_type") or "float"
        if value_type == "integer":
            for field, value in (("minimum", minimum), ("maximum", maximum)):
                if value is not None and int(round(value)) != value:
                    issues.append({
                        "code": "non_integer_bound", "parameter_id": parameter_id,
                        "field": field, "message": "Integer parameters require integer bounds.",
                        "display_name": label, "raw_value": config.get(field),
                    })
        scale = config.get("scale") or "linear"
        if scale == "log" and minimum is not None and minimum <= 0:
            issues.append({
                "code": "invalid_log_minimum", "parameter_id": parameter_id,
                "field": "minimum", "message": "Log-scale minimum must be positive.",
                "display_name": label, "minimum": minimum,
            })
        if minimum is not None and maximum is not None:
            variables.append({
                "parameter_id": parameter_id,
                "minimum": int(round(minimum)) if value_type == "integer" else minimum,
                "maximum": int(round(maximum)) if value_type == "integer" else maximum,
                "scale": "linear" if value_type == "integer" else scale,
                "value_type": value_type,
            })
    return {"variables": variables, "issues": issues}


def automatic_population(parameter_count):
    count = max(1, int(parameter_count))
    return max(20, min(100, int(round(5.0 * math.sqrt(count)))))


def calculated_algorithm_defaults(algorithm, parameter_count):
    count = max(1, int(parameter_count))
    population = automatic_population(count)
    if algorithm == "nsga2":
        return {
            "population": population,
            "seed": 1,
            "crossover_probability": 0.9,
            "crossover_eta": round(15.0 + math.sqrt(count), 3),
            "mutation_probability": 1.0 / count,
            "mutation_eta": round(20.0 + 0.5 * math.sqrt(count), 3),
        }
    if algorithm == "differential_evolution":
        return {
            "population": population,
            "seed": 1,
            "differential_weight": round(max(0.5, 0.9 - 0.005 * count), 3),
            "crossover_rate": round(min(0.95, 0.7 + 0.005 * count), 3),
        }
    if algorithm == "bayesian_optimization":
        return {
            "population": population,
            "seed": 1,
            "initial_samples": max(population, 2 * count + 1),
            "acquisition_exploration": round(max(1.0, 2.5 - 0.02 * count), 3),
            "candidate_pool": max(200, 10 * count),
            "noise": 1e-6,
            "kernel_length_scale": round(max(0.08, 1.0 / math.sqrt(count)), 6),
            "max_observations": max(80, 2 * count + 1),
        }
    raise ValueError("unsupported optimization algorithm: {}".format(algorithm))


def algorithm_for_objective_count(requested, objective_count, prior_single=None):
    if int(objective_count) >= 2:
        return "nsga2", requested if requested != "nsga2" else prior_single
    restored = prior_single if requested == "nsga2" and prior_single else requested
    return restored if restored in ALGORITHM_NAMES else "nsga2", None


def objective_pairs(objectives):
    return [
        (left, right)
        for left in range(len(objectives))
        for right in range(left + 1, len(objectives))
    ]


def metric_definition_label(definition):
    output = str(definition.get("output_key") or "Output")
    aggregation = str(definition.get("aggregation") or "mean")
    corners = definition.get("corners") or []
    direction = str(definition.get("direction") or "minimize")
    corner_text = ", ".join(str(item) for item in corners) or "all corners"
    return "{} · {} · {} · {}".format(output, aggregation, corner_text, direction)


_FAILURE_STAGE_LABELS = {
    "workspace_build": "Workspace copy build",
    "pdk_callback": "PDK / CDF callback",
    "parameter_application": "Parameter application",
    "parameter_verification": "Parameter verification after simulation",
    "netlist_or_submission": "Netlisting / Spectre submission",
    "netlist_or_spectre": "Netlisting / Spectre boundary",
    "maestro_startup": "Maestro / Ocean startup",
    "persistent_startup": "Maestro / Ocean startup",
    "spectre": "Spectre simulation",
    "result_publication": "Result publication",
    "simulation_infrastructure": "Spectre infrastructure",
    "source_integrity": "Source integrity verification",
    "design_or_simulation": "Simulation / result evaluation",
    "infrastructure": "EDA infrastructure",
}


def candidate_failure_stage(candidate):
    stage = str(candidate.get("failure_stage") or "").strip()
    if stage:
        return stage
    error = str(candidate.get("error") or "")
    if "WorkspaceBuildFailure" in error or "workspace build failed" in error.lower():
        return "workspace_build"
    failure_class = str(candidate.get("failure_class") or "").strip()
    return {
        "persistent_startup": "maestro_startup",
        "parameter_application": "parameter_application",
        "source_integrity": "source_integrity",
        "simulation_infrastructure": "simulation_infrastructure",
        "result_publication": "result_publication",
        "workspace_build": "workspace_build",
    }.get(failure_class, failure_class)


def candidate_failure_stage_label(candidate):
    if candidate.get("shared_preparation_failed"):
        return "Shared template preparation"
    stage = candidate_failure_stage(candidate)
    return _FAILURE_STAGE_LABELS.get(
        stage, stage.replace("_", " ").title() if stage else "Unknown",
    )


def candidate_spectre_state(candidate):
    if candidate.get("execution_started") is False:
        return "not_started"
    state = str(candidate.get("spectre_state") or "").strip()
    if state:
        return state
    if candidate.get("completion_evidence") or candidate.get("completion"):
        return "started"
    explicit_stage = str(candidate.get("failure_stage") or "").strip()
    stage = candidate_failure_stage(candidate)
    if stage in (
            "workspace_build", "pdk_callback", "parameter_application",
            "netlist_or_submission", "maestro_startup", "persistent_startup") and (
                stage != "parameter_application" or explicit_stage):
        return "not_started"
    return "unknown"


def candidate_status(candidate):
    if candidate.get("shared_preparation_failed"):
        return "Shared Preparation Failed · Not Executed"
    state = str(candidate.get("status") or "running")
    metrics = candidate.get("metrics") or {}
    stage = candidate_failure_stage(candidate)
    if stage == "source_integrity":
        return "Source Changed · Run Aborted"
    if stage == "workspace_build":
        return "Workspace Build Failed · Spectre Not Started"
    if state in ("failed", "infrastructure_failed"):
        if stage == "pdk_callback":
            return "PDK Callback Failed · Spectre Not Started"
        if stage == "parameter_application":
            return "Parameter Application Failed"
        if stage == "parameter_verification":
            return "Parameter Verification Failed"
        if stage == "netlist_or_submission":
            return "Netlist / Submission Failed"
        if stage == "netlist_or_spectre":
            return "Netlist / Spectre Boundary Failed"
        if stage in ("maestro_startup", "persistent_startup"):
            return "Maestro Startup Failed"
        if stage == "spectre":
            return "Spectre Simulation Failed"
    if state == "infrastructure_failed":
        return "Infrastructure Failed · Replacement Sampled"
    if state == "failed":
        return "Simulation / Evaluation Failed"
    if state == "completed" and metrics.get("feasible") is False:
        return "Completed · Constraints Not Met"
    if state == "completed":
        return "Completed"
    progress = candidate.get("execution_progress") or {}
    stage = str(progress.get("stage") or "").strip().lower()
    if state not in ("failed", "infrastructure_failed", "completed", "stopped", "cancelled"):
        label = {
            "preparing": "Preparing · Not Submitted",
            "patching": "Applying Parameters · Not Submitted",
            "netlisting": "Netlisting · Not Submitted",
            "starting": "Native Task Starting",
            "dispatching": "Dispatching · Awaiting Task Evidence",
            "submitted": "Submitted",
            "queued": "Queued",
            "running": "Running",
            "evaluating": "Evaluating Results",
            "cancelled": "Cancelled",
            "stopped": "Stopped",
            "submission_failed": "Submission Failed",
        }.get(stage)
        if label:
            return label
    if state == "queued":
        return "Queued"
    if state == "planned":
        return "Planned · Not Submitted"
    if state == "stopped":
        return "Stopped"
    if state == "cancelled":
        return "Cancelled"
    return "Running"


def metric_value(candidate, objective_index):
    metrics = (candidate.get("metrics") or {}).get("objectives") or []
    if objective_index >= len(metrics):
        return None
    return finite_number(metrics[objective_index].get("value"))


def _objective_cost(value, definition):
    direction = definition.get("direction") or "minimize"
    if direction == "maximize":
        return -value
    if direction == "target":
        target = finite_number(definition.get("target"))
        return abs(value - target) if target is not None else value
    return value


def fallback_pareto_ids(candidates, objectives):
    completed = []
    for candidate in candidates:
        if candidate.get("status") != "completed":
            continue
        if (candidate.get("metrics") or {}).get("feasible") is not True:
            continue
        values = [metric_value(candidate, index) for index in range(len(objectives))]
        if any(value is None for value in values):
            continue
        completed.append((candidate, [
            _objective_cost(value, objectives[index])
            for index, value in enumerate(values)
        ]))
    nondominated = set()
    for index, (candidate, costs) in enumerate(completed):
        dominated = False
        for other_index, (_other, other_costs) in enumerate(completed):
            if index == other_index:
                continue
            if all(left <= right for left, right in zip(other_costs, costs)) and any(
                    left < right for left, right in zip(other_costs, costs)):
                dominated = True
                break
        if not dominated:
            nondominated.add(candidate.get("candidate_id"))
    return nondominated


def pairwise_points(candidates, objectives, left_index, right_index, pareto_ids=None):
    pareto_ids = set(pareto_ids or fallback_pareto_ids(candidates, objectives))
    points = []
    for candidate in candidates:
        if candidate.get("status") != "completed":
            continue
        left = metric_value(candidate, left_index)
        right = metric_value(candidate, right_index)
        if left is None or right is None:
            continue
        metrics = candidate.get("metrics") or {}
        points.append({
            "candidate_id": candidate.get("candidate_id"),
            "x": left,
            "y": right,
            "generation": int(candidate.get("generation") or 0),
            "feasible": metrics.get("feasible") is True,
            "pareto": candidate.get("candidate_id") in pareto_ids,
        })
    return points


def compact_metric_values(candidate, kind):
    metrics = (candidate.get("metrics") or {}).get(kind) or []
    values = []
    for row in metrics:
        numeric = finite_number(row.get("value"))
        if numeric is None:
            values.append("Failed" if candidate.get("status") in TERMINAL_CANDIDATE_STATES else "Pending")
        else:
            values.append("{:.6g}".format(numeric))
    return ", ".join(values) if values else "None"


class RunUiState(object):
    def __init__(self):
        self.run_id = None
        self.state = "idle"
        self.stop_requested = False

    def started(self, run_id):
        self.run_id = run_id
        self.state = "running"
        self.stop_requested = False

    def request_stop(self):
        if not self.run_id or self.state not in ("queued", "running", "stopping"):
            return False
        self.state = "stopping"
        self.stop_requested = True
        return True

    def stop_failed(self):
        if self.run_id and self.state == "stopping":
            self.state = "running"
            self.stop_requested = False

    def update(self, state):
        if state in ("completed", "failed", "stopped"):
            self.state = state
            self.stop_requested = False
        elif self.stop_requested:
            self.state = "stopping"
        else:
            self.state = state or self.state


class BackgroundTaskQueue(object):
    def __init__(self, root, interval_ms=50, observer=None):
        self.root = root
        self.interval_ms = interval_ms
        self.observer = observer
        self.activity_sequence = 0
        self.tasks = queue.Queue()
        self.control_tasks = queue.Queue()
        self.history_tasks = queue.Queue()
        self.optional_tasks = queue.Queue()
        self.export_tasks = queue.Queue()
        self.monitor_tasks = queue.Queue()
        self.results = queue.Queue()
        self.exit_results = queue.Queue()
        self.closed = False
        self.shutting_down = False
        self.exit_worker = None
        self.state_lock = threading.Lock()
        self.worker = threading.Thread(
            target=self._work, args=(self.tasks,), name=_contract_text("crescendo-gui-worker"),
        )
        self.worker.daemon = True
        self.worker.start()
        self.control_worker = threading.Thread(
            target=self._work, args=(self.control_tasks,), name=_contract_text("crescendo-gui-control"),
        )
        self.control_worker.daemon = True
        self.control_worker.start()
        self.history_worker = threading.Thread(target=self._work, args=(self.history_tasks,), name=_contract_text("crescendo-gui-history"))
        self.history_worker.daemon = True
        self.history_worker.start()
        self.optional_worker = threading.Thread(
            target=self._work, args=(self.optional_tasks,), name=_contract_text("crescendo-gui-optional"),
        )
        self.optional_worker.daemon = True
        self.optional_worker.start()
        self.export_worker = threading.Thread(
            target=self._work, args=(self.export_tasks,), name=_contract_text("crescendo-gui-schematic-export"),
        )
        self.export_worker.daemon = True
        self.export_worker.start()
        self.monitor_worker = threading.Thread(
            target=self._work, args=(self.monitor_tasks,), name=_contract_text("crescendo-gui-live-monitor"),
        )
        self.monitor_worker.daemon = True
        self.monitor_worker.start()
        self.drain_job = self.root.after(self.interval_ms, self._drain)

    def _activity_item(self, function, on_success, on_error, lane):
        self.activity_sequence += 1
        metadata = {"id": self.activity_sequence, "name": getattr(function, "__qualname__", getattr(function, "__name__", "task")), "lane": lane,
                    "queued_at": time.time()}
        if self.observer is not None:
            self.results.put((True, dict(metadata, state="queued"), self._notify_activity))
        return function, on_success, on_error, metadata

    def _notify_activity(self, event):
        # Called by the Tk drain, never by producer threads. Telemetry must not
        # disable a callback or the independent Stop/Exit lanes.
        if self.observer is not None:
            try:
                self.observer(event)
            except Exception:
                pass

    def submit(self, function, on_success=None, on_error=None):
        with self.state_lock:
            if self.closed or self.shutting_down:
                raise RuntimeError("GUI task queue is closed")
            self.tasks.put(self._activity_item(function, on_success, on_error, "normal"))

    def submit_control(self, function, on_success=None, on_error=None):
        with self.state_lock:
            if self.closed or self.shutting_down:
                raise RuntimeError("GUI task queue is closed")
            self.control_tasks.put(self._activity_item(function, on_success, on_error, "control"))

    def submit_history(self, function, on_success=None, on_error=None):
        with self.state_lock:
            if self.closed or self.shutting_down:
                raise RuntimeError("GUI task queue is closing")
            self.history_tasks.put(self._activity_item(function, on_success, on_error, "history"))

    def submit_optional(self, function, on_success=None, on_error=None):
        """Keep optional callback work outside optimization's task lane."""
        with self.state_lock:
            if self.closed or self.shutting_down:
                raise RuntimeError("GUI task queue is closing")
            self.optional_tasks.put(self._activity_item(function, on_success, on_error, "optional"))

    @staticmethod
    def _discard_pending(task_queue):
        while True:
            try:
                task_queue.get_nowait()
            except queue.Empty:
                return

    def submit_export(self, function, on_success=None, on_error=None):
        with self.state_lock:
            if self.closed or self.shutting_down:
                raise RuntimeError("GUI task queue is closed")
            self.export_tasks.put(self._activity_item(function, on_success, on_error, "export"))

    def submit_monitor(self, function, on_success=None, on_error=None):
        """Bounded live reads continue while setup/native operations are busy."""
        with self.state_lock:
            if self.closed or self.shutting_down:
                raise RuntimeError("GUI task queue is closed")
            self.monitor_tasks.put(self._activity_item(function, on_success, on_error, "monitor"))

    def begin_shutdown(self, function, on_success=None, on_error=None):
        """Prioritize exit even when ordinary or control work is blocked."""
        with self.state_lock:
            if self.closed:
                return
            if self.exit_worker is not None and self.exit_worker.is_alive():
                return
            self.shutting_down = True
            if self.observer is not None:
                self.exit_results.put((True, {"state": "shutdown"}, self._notify_activity))
            for pending in (self.tasks, self.control_tasks, self.history_tasks, self.optional_tasks, self.export_tasks, self.monitor_tasks):
                self._discard_pending(pending)
            def exit_work():
                try:
                    value = function()
                except BaseException as error:
                    self.exit_results.put((False, error, on_error))
                else:
                    self.exit_results.put((True, value, on_success))
            self.exit_worker = threading.Thread(target=exit_work, name=_contract_text("crescendo-gui-exit"))
            self.exit_worker.daemon = True
            self.exit_worker.start()

    def abandon(self):
        """Close callbacks without acquiring a possibly blocked worker lock.

        Used only for explicit UI detachment. Native work and durable resource
        state remain the service's responsibility; no cleanup is implied.
        """
        self.closed = True
        self.shutting_down = True
        identifier = getattr(self, "drain_job", None)
        if identifier is not None:
            try:
                self.root.after_cancel(identifier)
            except Exception:
                pass

    def close(self):
        with self.state_lock:
            if self.closed:
                return
            self.closed = True
            for pending in (self.tasks, self.control_tasks, self.history_tasks, self.optional_tasks, self.export_tasks, self.monitor_tasks):
                self._discard_pending(pending)
            self.tasks.put(None)
            self.control_tasks.put(None)
            self.history_tasks.put(None)
            self.optional_tasks.put(None)
            self.export_tasks.put(None)
            self.monitor_tasks.put(None)
        identifier = getattr(self, "drain_job", None)
        if identifier is not None and hasattr(self.root, "after_cancel"):
            try:
                self.root.after_cancel(identifier)
            except Exception:
                pass

    def close_without_wait(self):
        """Stop callbacks now and retire worker queues without blocking Tk."""
        self.abandon()
        def retire_workers():
            with self.state_lock:
                for pending in (self.tasks, self.control_tasks, self.history_tasks,
                                self.optional_tasks, self.export_tasks, self.monitor_tasks):
                    self._discard_pending(pending)
                    pending.put(None)
        threading.Thread(target=retire_workers, name=_contract_text("crescendo-gui-queue-close"), daemon=True).start()

    def _work(self, task_queue):
        while True:
            item = task_queue.get()
            if item is None:
                return
            with self.state_lock:
                if self.closed or self.shutting_down:
                    continue
            function, on_success, on_error = item[:3]
            metadata = item[3] if len(item) > 3 else {}
            if self.observer is not None:
                self.results.put((True, dict(metadata, state="running", started_at=time.time()), self._notify_activity))
            try:
                result = function()
            except BaseException as error:
                if self.observer is not None:
                    self.results.put((True, dict(metadata, state="failed", error=str(error)[:2048]), self._notify_activity))
                self.results.put((False, error, on_error))
            else:
                if self.observer is not None:
                    self.results.put((True, dict(metadata, state="completed"), self._notify_activity))
                self.results.put((True, result, on_success))

    def _drain(self):
        try:
            started = time.monotonic()
            while True:
                if not self.state_lock.acquire(False):
                    return
                try:
                    if self.closed:
                        return
                finally:
                    self.state_lock.release()
                try:
                    success, value, callback = self.exit_results.get_nowait()
                except queue.Empty:
                    try:
                        success, value, callback = self.results.get_nowait()
                    except queue.Empty:
                        break
                    if self.shutting_down:
                        continue
                if callback:
                    callback(value)
                if time.monotonic() - started >= 0.012:
                    break
        finally:
            if not self.closed:
                self.drain_job = self.root.after(self.interval_ms, self._drain)
