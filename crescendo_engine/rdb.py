from __future__ import print_function

import math
import copy
import os
import sqlite3
from urllib.parse import quote


DEFAULT_CORNER_NAME = "_default"


def normalize_corner_name(value):
    if value is None:
        return DEFAULT_CORNER_NAME
    text = str(value).strip()
    return text or DEFAULT_CORNER_NAME


def find_rdb_files(root):
    paths = []
    for current, _directories, files in os.walk(root):
        for name in files:
            if name.endswith(".rdb"):
                path = os.path.join(current, name)
                if os.path.getsize(path) > 0:
                    paths.append(path)
    return sorted(paths, key=lambda path: (os.path.getmtime(path), path))


def _numeric(value):
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        number = float(value)
        return number if math.isfinite(number) else None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _table_exists(connection, name):
    return connection.execute("select count(*) from sqlite_master where name=?", (name,)).fetchone()[0] > 0


class RdbReadCancelled(RuntimeError):
    """A caller cancelled an optional history metadata read."""


def parse_rdb_summary(path, cancel_event=None):
    """Read exact history counts without materializing sweep values.

    This is an optional UI attachment summary, not a result/completion check.
    Candidate extraction must continue using ``parse_rdb``.  In particular, no
    scalar, parameter, status or point values are sampled or silently omitted
    from a result document: this API returns a distinct metadata-only schema.
    """
    def check_cancelled():
        if cancel_event is not None and cancel_event.is_set():
            raise RdbReadCancelled("history read cancelled")

    check_cancelled()
    path = os.path.abspath(path)
    # Keep the exact, read-only filename behavior of the full result reader.
    connection = sqlite3.connect("file:%s?mode=ro" % quote(path, safe="/"), uri=True)
    try:
        if cancel_event is not None:
            connection.set_progress_handler(lambda: int(cancel_event.is_set()), 1000)
        # Keep SQLite's page cache bounded even for a multi-gigabyte history.
        connection.execute("PRAGMA cache_size=-2048")
        connection.execute("PRAGMA query_only=ON")
        for table in ("result", "test", "point"):
            check_cancelled()
            if not _table_exists(connection, table):
                raise ValueError("history RDB has no %s table: %s" % (table, path))

        # Full parsing merges duplicate result definitions by this exact key.
        # Count only those definitions, never the potentially enormous
        # resultValue/parameterValue/testStatus tables or the full sweep join.
        output_keys = set()
        for test_name, output_name in connection.execute('''
            SELECT t.name, r.name
            FROM result r JOIN test t ON t.testID = r.testID
        '''):
            check_cancelled()
            output_keys.add("%s::%s" % (test_name, output_name))
        counts = {}
        for table in ("point", "test", "corner"):
            check_cancelled()
            counts[table] = (connection.execute("SELECT COUNT(*) FROM %s" % table).fetchone()[0]
                             if _table_exists(connection, table) else 0)
        check_cancelled()
        return {
            "schema_version": "crescendo.rdb.summary.v1",
            "path": path,
            "point_count": counts["point"],
            "output_count": len(output_keys),
            "test_count": counts["test"],
            "corner_count": counts["corner"],
            "metadata_only": True,
        }
    except sqlite3.OperationalError:
        # SQLite interrupts long VM operations through the progress handler.
        # Distinguish cancellation from schema/path/read failures for the UI.
        check_cancelled()
        raise
    finally:
        connection.close()


def parse_rdb(path):
    path = os.path.abspath(path)
    # Native history names may contain URI delimiters or literal percent signs.
    # Encode only the path so SQLite opens exactly that file in read-only mode.
    connection = sqlite3.connect("file:%s?mode=ro" % quote(path, safe="/"), uri=True)
    connection.row_factory = sqlite3.Row
    try:
        outputs = {}
        query = '''
            SELECT r.resultID, r.name AS output_name, r.expression, r.type AS output_type,
                   t.name AS test_name, p.pointID, p.designPointNumber,
                   c.name AS corner_name, rv.value, e.message AS error_message
            FROM result r
            JOIN test t ON t.testID = r.testID
            LEFT JOIN resultValue rv ON rv.resultID = r.resultID
            LEFT JOIN point p ON p.pointID = rv.pointID
            LEFT JOIN corner c ON c.cornerID = p.cornerID
            LEFT JOIN error e ON e.errorID = rv.errorID
            ORDER BY t.name, r.resultOrder, p.pointID
        '''
        for row in connection.execute(query):
            key = "%s::%s" % (row["test_name"], row["output_name"])
            output = outputs.setdefault(key, {
                "key": key,
                "test": row["test_name"],
                "name": row["output_name"],
                "expression": row["expression"],
                "output_type": row["output_type"],
                "values": [],
            })
            if row["pointID"] is not None:
                numeric = _numeric(row["value"])
                output["values"].append({
                    "point_id": row["pointID"],
                    "design_point": row["designPointNumber"],
                    "corner": normalize_corner_name(row["corner_name"]),
                    "value": row["value"] if not isinstance(row["value"], bytes) else None,
                    "numeric": numeric,
                    "scalar": numeric is not None,
                    "error": row["error_message"],
                })
        for output in outputs.values():
            values = [row["numeric"] for row in output["values"] if row["numeric"] is not None]
            output["scalar"] = bool(values) and len(values) == len(output["values"])
            output["summary"] = {
                "count": len(values),
                "min": min(values) if values else None,
                "max": max(values) if values else None,
                "mean": sum(values) / float(len(values)) if values else None,
                "first": values[0] if values else None,
                "last": values[-1] if values else None,
            }
        parameters = []
        if _table_exists(connection, "parameter") and _table_exists(connection, "parameterValue"):
            param_query = '''
                SELECT p.pointID, p.designPointNumber, c.name AS corner_name,
                       prm.name, prm.type, prm.defaultValue, pv.value
                FROM parameter prm
                JOIN parameterValue pv ON pv.parameterID = prm.parameterID
                JOIN point p ON p.pointID = pv.pointID
                LEFT JOIN corner c ON c.cornerID = p.cornerID
                ORDER BY p.pointID, prm.name
            '''
            parameters = []
            for row in connection.execute(param_query):
                value = dict(row)
                value["corner_name"] = normalize_corner_name(value.get("corner_name"))
                parameters.append(value)
        statuses = []
        if _table_exists(connection, "testStatus"):
            status_query = '''
                SELECT ts.pointID, t.name AS test_name, ts.statusCode,
                       ts.startTime, ts.stopTime, e.message AS status_message
                FROM testStatus ts
                JOIN test t ON t.testID = ts.testID
                LEFT JOIN error e ON e.errorID = ts.errorID
                ORDER BY ts.pointID, t.name
            '''
            statuses = [dict(row) for row in connection.execute(status_query)]
        points = []
        if _table_exists(connection, "point"):
            if _table_exists(connection, "corner"):
                point_query = '''
                    SELECT p.pointID, p.designPointNumber, c.name AS corner_name
                    FROM point p
                    LEFT JOIN corner c ON c.cornerID = p.cornerID
                    ORDER BY p.pointID
                '''
            else:
                point_query = '''
                    SELECT p.pointID, p.designPointNumber, NULL AS corner_name
                    FROM point p
                    ORDER BY p.pointID
                '''
            points = []
            for row in connection.execute(point_query):
                value = dict(row)
                value["corner_name"] = normalize_corner_name(value.get("corner_name"))
                points.append(value)
        return {
            "schema_version": "crescendo.rdb.v1",
            "path": path,
            "outputs": [outputs[key] for key in sorted(outputs)],
            "output_map": outputs,
            "parameters": parameters,
            "test_statuses": statuses,
            "points": points,
        }
    finally:
        connection.close()


def parse_latest_rdb(root):
    paths = find_rdb_files(root)
    if not paths:
        raise ValueError("no non-empty Maestro RDB found under %s" % root)
    return parse_rdb(paths[-1])


def slice_candidate_rdb(parsed, test_name_map, point_ids=None):
    """Select one immutable candidate from a shared native batch history.

    The test map is the manifest's physical-test -> original-test mapping.
    Point IDs may be shared by multiple tests; test identity is always required.
    Summaries must be recomputed after slicing, never reused from the batch.
    """
    if not isinstance(test_name_map, dict) or not test_name_map:
        raise ValueError("candidate RDB requires a non-empty test identity map")
    if any(not isinstance(k, str) or not k or not isinstance(v, str) or not v
           for k, v in test_name_map.items()):
        raise ValueError("candidate RDB test identity map is malformed")
    if len(set(test_name_map.values())) != len(test_name_map):
        raise ValueError("candidate RDB maps multiple tests onto one original test")
    selected_ids = None if point_ids is None else set(point_ids)
    if selected_ids is not None and (not selected_ids or any(
            isinstance(i, bool) or not isinstance(i, int) or i < 0 for i in selected_ids)):
        raise ValueError("candidate RDB point identities are malformed")
    result = dict(parsed)
    outputs = {}
    used_points = set()
    for output in parsed.get("outputs", []):
        physical_test = output.get("test")
        if physical_test not in test_name_map:
            continue
        values = [copy.deepcopy(row) for row in output.get("values", [])
                  if selected_ids is None or row.get("point_id") in selected_ids]
        if not values:
            continue
        original_test = test_name_map[physical_test]
        key = "%s::%s" % (original_test, output["name"])
        if key in outputs:
            raise ValueError("candidate RDB output identity is ambiguous: %s" % key)
        entry = dict(output, key=key, test=original_test, physical_test=physical_test,
                     values=values)
        numbers = [row["numeric"] for row in values if row.get("numeric") is not None]
        entry["scalar"] = bool(numbers) and len(numbers) == len(values)
        entry["summary"] = {
            "count": len(numbers), "min": min(numbers) if numbers else None,
            "max": max(numbers) if numbers else None,
            "mean": sum(numbers) / float(len(numbers)) if numbers else None,
            "first": numbers[0] if numbers else None,
            "last": numbers[-1] if numbers else None,
        }
        outputs[key] = entry
        used_points.update(row["point_id"] for row in values if row.get("point_id") is not None)
    statuses = []
    for status in parsed.get("test_statuses", []):
        physical_test = status.get("test_name")
        if physical_test not in test_name_map or (
                selected_ids is not None and status.get("pointID") not in selected_ids):
            continue
        row = dict(status, physical_test=physical_test,
                   test_name=test_name_map[physical_test])
        statuses.append(row)
        used_points.add(row["pointID"])
    result.update({
        "outputs": [outputs[key] for key in sorted(outputs)], "output_map": outputs,
        "test_statuses": statuses,
        "points": [copy.deepcopy(row) for row in parsed.get("points", [])
                   if row.get("pointID") in used_points],
        "parameters": [copy.deepcopy(row) for row in parsed.get("parameters", [])
                       if row.get("pointID") in used_points],
        "candidate_test_name_map": dict(test_name_map),
    })
    return result


def selected_corner_rows(values, corners, allow_native_alias=True):
    """Prefer source group identities; retain explicit native point selection."""
    selected = set(normalize_corner_name(value) for value in corners)
    source_names = set(normalize_corner_name(row['source_corner']) for row in values
                       if row.get('source_corner') is not None)
    matches, covered = [], set()
    for row in values:
        native = normalize_corner_name(row.get('corner'))
        source = normalize_corner_name(row['source_corner']) if row.get('source_corner') is not None else native
        names = set()
        if source in selected:
            names.add(source)
        if native in selected and (native == source or (allow_native_alias and native not in source_names)):
            names.add(native)
        if names:
            matches.append(row)
            covered.update(names)
    return matches, covered


def aggregate_output(parsed, key, aggregation, corners=None, allow_native_alias=True):
    output = parsed.get("output_map", {}).get(key)
    if not output:
        raise ValueError("output not found: %s" % key)
    if aggregation not in ("min", "max", "mean", "first", "last"):
        raise ValueError("unsupported aggregation: %s" % aggregation)
    selected_corners = [normalize_corner_name(value) for value in (corners or [])]
    if selected_corners:
        selected, covered = selected_corner_rows(output.get("values", []), selected_corners, allow_native_alias)
        if not selected:
            raise ValueError("output has no points for selected corners %s: %s" % (", ".join(selected_corners), key))
        missing = sorted(set(selected_corners) - covered)
        if missing:
            raise ValueError("output is missing selected corners %s: %s" % (", ".join(missing), key))
        values = [row.get("numeric") for row in selected]
        if any(value is None for value in values):
            raise ValueError("output is not scalar numeric for every selected corner: %s" % key)
        summary = {
            "min": min(values),
            "max": max(values),
            "mean": sum(values) / float(len(values)),
            "first": values[0],
            "last": values[-1],
        }
    else:
        summary = output.get("summary", {})
    value = summary.get(aggregation)
    if value is None:
        raise ValueError("output is not a fully scalar numeric result: %s" % key)
    return value
