from __future__ import print_function

import hashlib
import json
import math
from bisect import bisect_left, bisect_right


SINGLE_OBJECTIVE_ALGORITHMS = (
    "nsga2",
    "differential_evolution",
    "bayesian_optimization",
)
MULTI_OBJECTIVE_ALGORITHMS = ("nsga2",)

_ALGORITHM_ALIASES = {
    "nsga2": "nsga2",
    "nsga-ii": "nsga2",
    "nsga_ii": "nsga2",
    "differential": "differential_evolution",
    "differential_evolution": "differential_evolution",
    "de": "differential_evolution",
    "bayesian": "bayesian_optimization",
    "bayesian_optimization": "bayesian_optimization",
}


def _objective_count(value):
    try:
        count = int(value)
    except (TypeError, ValueError, OverflowError):
        raise ValueError("objective count must be a positive integer")
    if isinstance(value, float) and value != count:
        raise ValueError("objective count must be a positive integer")
    if count < 1:
        raise ValueError("objective count must be a positive integer")
    return count


def canonical_algorithm_name(algorithm):
    if algorithm is None or not str(algorithm).strip():
        return None
    name = str(algorithm).strip().lower()
    if name not in _ALGORITHM_ALIASES:
        raise ValueError("unsupported algorithm: %s" % algorithm)
    return _ALGORITHM_ALIASES[name]


def resolve_algorithm_policy(algorithm, objective_count, coerce=False):
    """Resolve visible optimizer policy without silently changing strict callers.

    ``coerce=False`` is intended for CLI/API validation and raises when a
    multiobjective configuration requests a single-objective algorithm.
    ``coerce=True`` is intended for an interactive GUI and deterministically
    selects NSGA-II while reporting that change to the presenter.
    """
    count = _objective_count(objective_count)
    requested = canonical_algorithm_name(algorithm)
    allowed = SINGLE_OBJECTIVE_ALGORITHMS if count == 1 else MULTI_OBJECTIVE_ALGORITHMS
    default = "differential_evolution" if count == 1 else "nsga2"
    selected = requested or default
    coerced = False
    notice = ""
    if selected not in allowed:
        if count > 1 and coerce:
            selected = "nsga2"
            coerced = True
            notice = "Multiple objectives require NSGA-II; the algorithm was changed to NSGA-II."
        else:
            raise ValueError(
                "algorithm %s is incompatible with %d objectives; allowed algorithms: %s" % (
                    selected, count, ", ".join(allowed),
                )
            )
    if coerced:
        policy = "gui_coerced_multiobjective"
    elif requested is None:
        policy = "automatic_by_objective_count"
    else:
        policy = "explicit"
    return {
        "objective_count": count,
        "requested_algorithm": requested,
        "algorithm": selected,
        "allowed_algorithms": list(allowed),
        "locked": count > 1,
        "coerced": coerced,
        "policy": policy,
        "notice": notice,
    }


def _corners(definition):
    corners = definition.get("corners")
    if corners is None:
        return []
    if isinstance(corners, str):
        return [corners]
    return [str(corner) for corner in corners]


def _objective_identity(index, output_key, aggregation, corners, direction):
    payload = json.dumps(
        [index, output_key, aggregation, corners, direction],
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return "objective:%d:%s" % (index, hashlib.sha1(payload).hexdigest()[:12])


def objective_descriptors(objectives):
    descriptors = []
    for index, definition in enumerate(objectives or []):
        if not isinstance(definition, dict):
            raise ValueError("objective %d must be an object" % (index + 1))
        output_key = str(definition.get("output_key") or "").strip()
        if not output_key:
            raise ValueError("objective %d requires output_key" % (index + 1))
        aggregation = str(definition.get("aggregation") or "mean")
        direction = str(definition.get("direction") or "minimize")
        corners = _corners(definition)
        name = str(definition.get("name") or output_key)
        metric_label = output_key if name == output_key else "%s [%s]" % (name, output_key)
        corner_label = ", ".join(corners) if corners else "all corners"
        descriptor = {
            "objective_id": _objective_identity(
                index, output_key, aggregation, corners, direction,
            ),
            "index": index,
            "number": index + 1,
            "name": name,
            "output_key": output_key,
            "aggregation": aggregation,
            "corners": corners,
            "direction": direction,
            "label": "Objective %d | %s | %s | %s | %s" % (
                index + 1, metric_label, direction, aggregation, corner_label,
            ),
        }
        descriptors.append(descriptor)
    if not descriptors:
        raise ValueError("at least one objective is required")
    return descriptors


def objective_pair_descriptors(objectives):
    descriptors = objective_descriptors(objectives)
    pairs = []
    for left_index in range(len(descriptors)):
        for right_index in range(left_index + 1, len(descriptors)):
            left = descriptors[left_index]
            right = descriptors[right_index]
            pairs.append({
                "pair_id": "pair:%s:%s" % (
                    left["objective_id"], right["objective_id"],
                ),
                "x_index": left_index,
                "y_index": right_index,
                "x_objective_id": left["objective_id"],
                "y_objective_id": right["objective_id"],
                "x_label": left["label"],
                "y_label": right["label"],
                "label": "%s vs %s" % (left["label"], right["label"]),
            })
    return pairs


def _eligible_candidates(rows, objective_count):
    eligible = []
    seen = set()
    for row in rows or []:
        if row.get("status") != "completed":
            continue
        candidate_id = str(row.get("candidate_id") or "")
        if not candidate_id or candidate_id in seen:
            continue
        metrics = row.get("metrics") or {}
        objectives = metrics.get("objectives") or []
        if len(objectives) != objective_count:
            continue
        try:
            violation = float(metrics.get("total_violation", 0.0))
            scores = [float(objective["score"]) for objective in objectives]
        except (KeyError, TypeError, ValueError, OverflowError):
            continue
        if not math.isfinite(violation) or not all(math.isfinite(score) for score in scores):
            continue
        seen.add(candidate_id)
        eligible.append(row)
    return eligible


class _FenwickMinimum(object):
    def __init__(self, size):
        self.values = [float("inf")] * (int(size) + 1)

    def update(self, index, value):
        index = int(index)
        while index < len(self.values):
            if value < self.values[index]:
                self.values[index] = value
            index += index & -index

    def query(self, index):
        result = float("inf")
        index = int(index)
        while index > 0:
            if self.values[index] < result:
                result = self.values[index]
            index -= index & -index
        return result


class _Fenwick2DMinimum(object):
    def __init__(self, vectors):
        self.y_values = sorted(set(vector[1] for vector in vectors))
        self.y_index = dict(
            (value, index + 1) for index, value in enumerate(self.y_values)
        )
        self.z_values = [[] for _unused in range(len(self.y_values) + 1)]
        for vector in vectors:
            index = self.y_index[vector[1]]
            while index < len(self.z_values):
                self.z_values[index].append(vector[2])
                index += index & -index
        self.trees = [[] for _unused in range(len(self.z_values))]
        for index in range(1, len(self.z_values)):
            values = sorted(set(self.z_values[index]))
            self.z_values[index] = values
            self.trees[index] = [float("inf")] * (len(values) + 1)

    def update(self, vector):
        y_index = self.y_index[vector[1]]
        while y_index < len(self.z_values):
            z_index = bisect_left(self.z_values[y_index], vector[2]) + 1
            tree = self.trees[y_index]
            while z_index < len(tree):
                if vector[3] < tree[z_index]:
                    tree[z_index] = vector[3]
                z_index += z_index & -z_index
            y_index += y_index & -y_index

    def query(self, vector):
        y_index = bisect_right(self.y_values, vector[1])
        result = float("inf")
        while y_index > 0:
            z_index = bisect_right(self.z_values[y_index], vector[2])
            tree = self.trees[y_index]
            while z_index > 0:
                if tree[z_index] < result:
                    result = tree[z_index]
                z_index -= z_index & -z_index
            y_index -= y_index & -y_index
        return result


def _nondominated_score_vectors(vectors):
    unique = sorted(set(vectors))
    if not unique:
        return set()
    dimensions = len(unique[0])
    if dimensions == 1:
        return set([unique[0]])
    if dimensions == 2:
        result = set()
        best_second = float("inf")
        for vector in unique:
            if vector[1] < best_second:
                result.add(vector)
                best_second = vector[1]
        return result
    if dimensions == 3:
        second_values = sorted(set(vector[1] for vector in unique))
        second_index = dict(
            (value, index + 1) for index, value in enumerate(second_values)
        )
        tree = _FenwickMinimum(len(second_values))
        result = set()
        for vector in unique:
            index = second_index[vector[1]]
            if tree.query(index) > vector[2]:
                result.add(vector)
            tree.update(index, vector[2])
        return result
    if dimensions == 4:
        tree = _Fenwick2DMinimum(unique)
        result = set()
        for vector in unique:
            if tree.query(vector) > vector[3]:
                result.add(vector)
            tree.update(vector)
        return result
    result = []
    for vector in unique:
        if not any(
                all(left <= right for left, right in zip(other, vector))
                for other in result):
            result.append(vector)
    return set(result)


def _fast_nondominated_rows(rows):
    if not rows:
        return []
    minimum_violation = min(
        float(row["metrics"].get("total_violation", 0.0)) for row in rows
    )
    tier = [
        row for row in rows
        if float(row["metrics"].get("total_violation", 0.0)) == minimum_violation
    ]
    vectors = [
        tuple(float(objective["score"])
              for objective in row["metrics"]["objectives"])
        for row in tier
    ]
    nondominated = _nondominated_score_vectors(vectors)
    return [row for row, vector in zip(tier, vectors) if vector in nondominated]


def global_nondominated_candidate_ids(rows, objective_count):
    """Return one constrained, full-dimensional Pareto front.

    Pairwise views consume these IDs as projections; they must not calculate
    independent two-dimensional fronts because that changes Pareto semantics.
    """
    count = _objective_count(objective_count)
    eligible = _eligible_candidates(rows, count)
    return [row["candidate_id"] for row in _fast_nondominated_rows(eligible)]


def build_objective_presentation(rows, objectives):
    """Build compact, dependency-free data for live result and Pareto views."""
    descriptors = objective_descriptors(objectives)
    eligible = _eligible_candidates(rows, len(descriptors))
    pareto_ids = [row["candidate_id"] for row in _fast_nondominated_rows(eligible)]
    pareto_set = set(pareto_ids)
    points = []
    for row in eligible:
        metrics = row["metrics"]
        metric_objectives = metrics["objectives"]
        points.append({
            "candidate_id": row["candidate_id"],
            "generation": row.get("generation"),
            "index": row.get("index"),
            "status": row.get("status"),
            "feasible": bool(metrics.get(
                "feasible", float(metrics.get("total_violation", 0.0)) <= 0.0,
            )),
            "total_violation": float(metrics.get("total_violation", 0.0)),
            "pareto": row["candidate_id"] in pareto_set,
            "objective_values": [objective.get("value") for objective in metric_objectives],
            "objective_scores": [float(objective["score"]) for objective in metric_objectives],
        })
    return {
        "schema_version": "crescendo.objective_presentation.v1",
        "objective_count": len(descriptors),
        "objectives": descriptors,
        "pairs": objective_pair_descriptors(objectives),
        "pareto_candidate_ids": pareto_ids,
        "points": points,
    }
