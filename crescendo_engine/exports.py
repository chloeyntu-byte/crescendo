from __future__ import print_function

import csv
import io
import os


BASE_COLUMNS = (
    "candidate",
    "generation",
    "index",
    "status",
    "failure_class",
    "feasible",
)


def _text(value):
    if value is None:
        return ""
    if value is True:
        return "yes"
    if value is False:
        return "no"
    return str(value)


def _metric_key(metric, index):
    return (
        index,
        str(metric.get("name") or metric.get("output_key") or "metric_%d" % (index + 1)),
        str(metric.get("output_key") or ""),
        str(metric.get("aggregation") or "mean"),
        tuple(str(corner) for corner in (metric.get("corners") or [])),
    )


def _metric_label(identity, repeated):
    index, name, output_key, aggregation, corners = identity
    if not repeated:
        return name
    details = []
    if output_key and output_key != name:
        details.append(output_key)
    if aggregation:
        details.append(aggregation)
    if corners:
        details.append("corners=%s" % "+".join(corners))
    details.append("#%d" % (index + 1))
    return "%s [%s]" % (name, "; ".join(details))


def _collect_columns(candidates, variable_labels):
    variable_names = set()
    objective_names = []
    constraint_names = []
    seen_objectives = set()
    seen_constraints = set()
    for candidate in candidates:
        variable_names.update((candidate.get("values") or {}).keys())
        metrics = candidate.get("metrics") or {}
        for index, metric in enumerate(metrics.get("objectives") or []):
            identity = _metric_key(metric, index)
            if identity not in seen_objectives:
                seen_objectives.add(identity)
                objective_names.append(identity)
        for index, metric in enumerate(metrics.get("constraints") or []):
            identity = _metric_key(metric, index)
            if identity not in seen_constraints:
                seen_constraints.add(identity)
                constraint_names.append(identity)
    variable_names = sorted(variable_names, key=lambda key: str(variable_labels.get(key, key)))
    return variable_names, objective_names, constraint_names


def _metric_values(metrics):
    return dict((_metric_key(metric, index), metric.get("value")) for index, metric in enumerate(metrics or []))


def _labels_for_variables(variable_names, variable_labels):
    base_labels = [str(variable_labels.get(name, name)) for name in variable_names]
    counts = dict((label, base_labels.count(label)) for label in set(base_labels))
    return [
        label if counts[label] == 1 else "%s [%s]" % (label, name)
        for name, label in zip(variable_names, base_labels)
    ]


def _labels_for_metrics(identities):
    names = [identity[1] for identity in identities]
    counts = dict((name, names.count(name)) for name in set(names))
    return [_metric_label(identity, counts[identity[1]] > 1) for identity in identities]


def _display_generation(value):
    if value is None or value == "":
        return value
    try:
        return int(value) + 1
    except (TypeError, ValueError):
        return value


def write_candidates_csv(candidates, stream, variable_labels=None):
    variable_labels = dict(variable_labels or {})
    variable_names, objective_names, constraint_names = _collect_columns(candidates, variable_labels)
    objective_labels = _labels_for_metrics(objective_names)
    constraint_labels = _labels_for_metrics(constraint_names)
    display_variable_labels = _labels_for_variables(variable_names, variable_labels)
    headers = list(BASE_COLUMNS)
    headers.extend("objective:%s" % label for label in objective_labels)
    headers.extend("constraint:%s" % label for label in constraint_labels)
    headers.extend("variable:%s" % label for label in display_variable_labels)
    include_actual = any(candidate.get("execution_backend") in ("maestro_netlist", "private_netlist") for candidate in candidates)
    if include_actual:
        headers.extend("actual_variable:%s" % label for label in display_variable_labels)
    writer = csv.writer(stream, lineterminator="\n")
    writer.writerow(headers)
    for candidate in candidates:
        metrics = candidate.get("metrics") or {}
        objective_values = _metric_values(metrics.get("objectives"))
        constraint_values = _metric_values(metrics.get("constraints"))
        values = candidate.get("values") or {}
        row = [
            candidate.get("candidate_id"),
            _display_generation(candidate.get("generation")),
            candidate.get("index"),
            candidate.get("status"),
            candidate.get("failure_class"),
            metrics.get("feasible") if "feasible" in metrics else candidate.get("feasible"),
        ]
        row.extend(objective_values.get(name) for name in objective_names)
        row.extend(constraint_values.get(name) for name in constraint_names)
        row.extend(values.get(name) for name in variable_names)
        if include_actual:
            completion = candidate.get("completion_evidence") or candidate.get("completion") or {}
            actual = completion.get("parameters") or {} if completion.get("ok") else {}
            row.extend(actual.get(name) for name in variable_names)
        writer.writerow([_text(value) for value in row])
    return {
        "rows": len(candidates),
        "columns": len(headers),
        "headers": headers,
    }


def candidates_csv_text(candidates, variable_labels=None):
    stream = io.StringIO()
    write_candidates_csv(candidates, stream, variable_labels=variable_labels)
    return stream.getvalue()


def export_candidates_csv(candidates, path, variable_labels=None):
    target = os.path.abspath(os.path.expanduser(path))
    parent = os.path.dirname(target)
    if parent and not os.path.isdir(parent):
        raise ValueError("CSV output directory does not exist: %s" % parent)
    temporary = target + ".tmp"
    try:
        with io.open(temporary, "w", encoding="utf-8", newline="") as stream:
            summary = write_candidates_csv(candidates, stream, variable_labels=variable_labels)
        os.replace(temporary, target)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    summary["path"] = target
    summary["bytes"] = os.path.getsize(target)
    return summary
