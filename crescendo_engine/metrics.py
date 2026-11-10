from __future__ import print_function

import math

from .engineering import (
    normalize_constraint_definition, normalize_objective_definition,
    parse_engineering_number,
)
from .rdb import aggregate_output, normalize_corner_name, selected_corner_rows


def _selected_output_statistics(parsed, definitions):
    """Keep real selected-point statistics before native data is reclaimed."""
    statistics, seen = [], set()
    for definition in definitions:
        key = definition["output_key"]
        corners = sorted(set(normalize_corner_name(value) for value in definition.get("corners") or []))
        identity = (key, tuple(corners))
        if identity in seen:
            continue
        seen.add(identity)
        output = parsed.get("output_map", {}).get(key) or {}
        values = output.get("values")
        if values is not None and corners:
            values, _ = selected_corner_rows(values, corners, allow_native_alias=False)
        row = {"output_key": key, "corners": corners,
               "point_count": len(values) if values is not None else None,
               "finite_count": sum(isinstance(item.get("numeric"), (int, float))
                                   and math.isfinite(float(item["numeric"])) for item in values)
                               if values is not None else None,
               "source": "native_selected_output_points"}
        unavailable = []
        for aggregation in ("min", "mean", "max"):
            try:
                value = aggregate_output(parsed, key, aggregation, corners, allow_native_alias=False)
                row[aggregation] = value if math.isfinite(float(value)) else None
            except (ValueError, TypeError, OverflowError):
                row[aggregation] = None
            if row[aggregation] is None:
                unavailable.append(aggregation)
        row["status"] = "AVAILABLE" if not unavailable else "PARTIAL"
        if unavailable:
            row["unavailable_statistics"] = unavailable
        statistics.append(row)
    return statistics


def _configured_scale(definition):
    configured = definition.get("scale")
    if configured is not None:
        scale = parse_engineering_number(configured, "metric scale")
        if scale <= 0.0:
            raise ValueError("metric scale must be positive")
        return scale
    return None


def _constraint_scale(definition):
    configured = _configured_scale(definition)
    if configured is not None:
        return configured
    operator = definition.get("operator", "<=")
    if operator in ("<=", ">="):
        limit = abs(parse_engineering_number(definition["limit"], "constraint limit"))
        return limit if limit > 0.0 else 1.0
    if operator == "between":
        lower = parse_engineering_number(definition["lower"], "constraint lower bound")
        upper = parse_engineering_number(definition["upper"], "constraint upper bound")
        scale = max(abs(lower), abs(upper), abs(upper - lower))
        return scale if scale > 0.0 else 1.0
    if operator == "target":
        target = abs(parse_engineering_number(definition["target"], "constraint target"))
        return target if target > 0.0 else 1.0
    return 1.0


def objective_score(value, definition):
    definition = normalize_objective_definition(definition)
    direction = definition.get("direction", "minimize")
    scale = _configured_scale(definition) or 1.0
    if direction == "minimize":
        return float(value) / scale
    if direction == "maximize":
        return -float(value) / scale
    if direction == "target":
        if _configured_scale(definition) is None:
            target = abs(definition["target"])
            scale = target if target > 0.0 else 1.0
        return abs(float(value) - definition["target"]) / scale
    raise ValueError("unsupported objective direction: %s" % direction)


def constraint_violation(value, definition):
    definition = normalize_constraint_definition(definition)
    operator = definition.get("operator", "<=")
    scale = _constraint_scale(definition)
    if operator == "<=":
        return max(0.0, float(value) - definition["limit"]) / scale
    if operator == ">=":
        return max(0.0, definition["limit"] - float(value)) / scale
    if operator == "between":
        lower = definition["lower"]
        upper = definition["upper"]
        return (max(0.0, lower - float(value)) + max(0.0, float(value) - upper)) / scale
    if operator == "target":
        tolerance = abs(definition.get("tolerance", 0.0))
        return max(0.0, abs(float(value) - definition["target"]) - tolerance) / scale
    raise ValueError("unsupported constraint operator: %s" % operator)


def evaluate_metrics(parsed, objectives, constraints):
    objective_rows = []
    for definition in objectives:
        value = aggregate_output(parsed, definition["output_key"], definition.get("aggregation", "mean"), definition.get("corners"), allow_native_alias=False)
        objective_rows.append({
            "name": definition.get("name") or definition["output_key"],
            "output_key": definition["output_key"],
            "value": value,
            "score": objective_score(value, definition),
            "direction": definition.get("direction", "minimize"),
            "aggregation": definition.get("aggregation", "mean"),
            "corners": list(definition.get("corners") or []),
        })
    constraint_rows = []
    for definition in constraints:
        value = aggregate_output(parsed, definition["output_key"], definition.get("aggregation", "mean"), definition.get("corners"), allow_native_alias=False)
        violation = constraint_violation(value, definition)
        constraint_rows.append({
            "name": definition.get("name") or definition["output_key"],
            "output_key": definition["output_key"],
            "value": value,
            "violation": violation,
            "satisfied": violation <= 0.0,
            "aggregation": definition.get("aggregation", "mean"),
            "corners": list(definition.get("corners") or []),
        })
    total_violation = sum(row["violation"] for row in constraint_rows)
    merit = sum(row["score"] for row in objective_rows)
    if not math.isfinite(merit):
        merit = float("inf")
    return {
        "objectives": objective_rows,
        "constraints": constraint_rows,
        "selected_output_statistics": _selected_output_statistics(parsed, list(objectives) + list(constraints)),
        "feasible": total_violation <= 0.0,
        "total_violation": total_violation,
        "merit": merit,
    }


def dominates(left, right):
    if left["total_violation"] != right["total_violation"]:
        return left["total_violation"] < right["total_violation"]
    left_scores = [row["score"] for row in left["objectives"]]
    right_scores = [row["score"] for row in right["objectives"]]
    return all(a <= b for a, b in zip(left_scores, right_scores)) and any(a < b for a, b in zip(left_scores, right_scores))


def nondominated_front(rows):
    return [row for row in rows if not any(dominates(other["metrics"], row["metrics"]) for other in rows if other is not row)]


def select_representative(rows):
    if not rows:
        raise ValueError("cannot select a representative from an empty population")
    front = nondominated_front(rows)
    objective_count = len(front[0]["metrics"].get("objectives", []))
    if objective_count <= 1:
        row = min(front, key=lambda item: (
            item["metrics"]["total_violation"],
            float(item["metrics"]["objectives"][0]["score"]) if objective_count else 0.0,
            str(item.get("candidate_id") or ""),
        ))
        return row, 0.0
    minimums = []
    maximums = []
    for objective_index in range(objective_count):
        values = [float(row["metrics"]["objectives"][objective_index]["score"]) for row in front]
        minimums.append(min(values))
        maximums.append(max(values))

    def ideal_distance(row):
        squared = 0.0
        for objective_index in range(objective_count):
            value = float(row["metrics"]["objectives"][objective_index]["score"])
            span = maximums[objective_index] - minimums[objective_index]
            normalized = 0.0 if span <= 0.0 else (value - minimums[objective_index]) / span
            squared += normalized * normalized
        return math.sqrt(squared / float(objective_count))

    row = min(front, key=lambda item: (ideal_distance(item), str(item.get("candidate_id") or "")))
    return row, ideal_distance(row)
