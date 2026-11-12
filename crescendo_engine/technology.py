from __future__ import print_function
from crescendo_engine.runtime import bind_text
_contract_text = bind_text()


import ast
import json
import os


TECHNOLOGY_SCHEMA_VERSION = _contract_text("crescendo.workspace_technology.v1")


class TechnologyBindingError(RuntimeError):
    pass


def _skill_quote(value):
    return str(value).replace("\\", "\\\\").replace('"', '\\"')


def _skill_value(value):
    return '"%s"' % _skill_quote(value) if value not in (None, "") else "nil"


def hierarchy_technology_bindings(hierarchy):
    by_library = {}
    for design in (hierarchy or {}).get("designs") or []:
        body = design.get("hierarchy") or {}
        for row in body.get("technology_bindings") or []:
            library = row.get("library")
            if not library:
                continue
            normalized = {
                "library": str(library),
                "technology_library": row.get("technology_library"),
                "technology_path": row.get("technology_path"),
                "source_probe": True,
            }
            previous = by_library.get(str(library))
            if previous and previous.get("technology_library") != normalized.get("technology_library"):
                raise TechnologyBindingError(
                    "source hierarchy reported conflicting technology bindings for %s: %r and %r"
                    % (
                        library, previous.get("technology_library"),
                        normalized.get("technology_library"),
                    )
                )
            by_library[str(library)] = normalized
    return dict((key, by_library[key]) for key in sorted(by_library))


def workspace_technology_manifest(hierarchy, copied_libraries, output_path):
    source_bindings = hierarchy_technology_bindings(hierarchy)
    bindings = []
    for library in sorted(copied_libraries or {}):
        source = source_bindings.get(library) or {}
        bindings.append({
            "library": library,
            "technology_library": source.get("technology_library"),
            "source_technology_path": source.get("technology_path"),
            "source_probe_available": bool(source),
        })
    return {
        "schema_version": TECHNOLOGY_SCHEMA_VERSION,
        "source_probe_read_only": True,
        "private_probe_read_only": True,
        "probe_path": os.path.abspath(output_path),
        "bindings": bindings,
        "status": "pending",
    }


def technology_probe_skill(manifest):
    if not isinstance(manifest, dict) or manifest.get("schema_version") != TECHNOLOGY_SCHEMA_VERSION:
        raise TechnologyBindingError("workspace technology manifest schema is not supported")
    output_path = manifest.get("probe_path")
    if not output_path:
        raise TechnologyBindingError("workspace technology probe path is missing")
    lines = [
        _contract_text('crescTechOut = outfile("%s")') % _skill_quote(output_path),
        _contract_text("crescTechFailed = nil"),
        "if(isCallable('techGetTechFile) then",
    ]
    for row in manifest.get("bindings") or []:
        library = str(row.get("library") or "")
        expected = row.get("technology_library")
        lines.extend([
            _contract_text('  crescTechLib = ddGetObj("%s")') % _skill_quote(library),
            _contract_text("  crescTechFile = nil"),
            _contract_text("  crescActualTechLib = nil"),
            _contract_text("  crescActualTechPath = nil"),
            _contract_text("  if(ddIsId(crescTechLib) then"),
            _contract_text("    crescTechResult = errset(techGetTechFile(crescTechLib) t)"),
            _contract_text("    when(crescTechResult crescTechFile = car(crescTechResult))"),
            _contract_text("    when(crescTechFile"),
            _contract_text("      crescActualTechLib = crescTechFile~>libName"),
            _contract_text("      crescActualTechPath = crescTechFile~>path"),
            "    )",
            "  else",
            _contract_text("    crescTechFailed = t"),
            "  )",
            _contract_text('  fprintf(crescTechOut "TECH\\t%%L\\t%%L\\t%%L\\t%%L\\n" "%s" %s crescActualTechLib crescActualTechPath)')
            % (_skill_quote(library), _skill_value(expected)),
        ])
        if expected:
            lines.extend([
                _contract_text("  unless(crescActualTechLib && equal(crescActualTechLib %s)")
                % _skill_value(expected),
                _contract_text("    crescTechFailed = t"),
                "  )",
            ])
    lines.extend([
        "else",
        _contract_text('  fprintf(crescTechOut "ERROR\\t%L\\t%L\\n" "technology_api_unavailable" "techGetTechFile")'),
        _contract_text("  crescTechFailed = t"),
        ")",
        _contract_text("close(crescTechOut)"),
        _contract_text('when(crescTechFailed error("Crescendo working workspace technology binding validation failed; inspect %s\\n"))')
        % _skill_quote(output_path),
    ])
    return lines


def _literal(value):
    value = str(value).strip()
    if value == "nil":
        return None
    if value == "t":
        return True
    try:
        return ast.literal_eval(value)
    except (SyntaxError, ValueError):
        return value


def parse_technology_records(path):
    rows = []
    errors = []
    with open(path, "r") as stream:
        for line_number, raw in enumerate(stream, 1):
            values = raw.rstrip("\n").split("\t")
            if not values or not values[0]:
                continue
            if values[0] == "TECH" and len(values) >= 5:
                rows.append({
                    "library": _literal(values[1]),
                    "expected_technology_library": _literal(values[2]),
                    "technology_library": _literal(values[3]),
                    "technology_path": _literal(values[4]),
                    "line": line_number,
                })
            else:
                errors.append({
                    "kind": values[0], "values": [_literal(value) for value in values[1:]],
                    "line": line_number,
                })
    return {"bindings": rows, "errors": errors}


def validate_workspace_technology(manifest, require_complete=True):
    path = (manifest or {}).get("probe_path")
    if not path or not os.path.isfile(path):
        result = {
            "ok": not require_complete,
            "status": "pending",
            "error": "working technology probe has not run",
            "probe_path": path,
            "bindings": [],
        }
        if require_complete:
            raise TechnologyBindingError(result["error"] + ": %s" % path)
        return result
    parsed = parse_technology_records(path)
    expected = dict(
        (row.get("library"), row) for row in (manifest or {}).get("bindings") or []
    )
    actual = dict((row.get("library"), row) for row in parsed["bindings"])
    failures = list(parsed["errors"])
    for library, expectation in sorted(expected.items()):
        observed = actual.get(library)
        expected_technology = expectation.get("technology_library")
        if observed is None:
            failures.append({"library": library, "reason": "probe_record_missing"})
            continue
        if expected_technology and observed.get("technology_library") != expected_technology:
            failures.append({
                "library": library, "reason": "technology_library_mismatch",
                "expected": expected_technology,
                "observed": observed.get("technology_library"),
            })
        technology_path = observed.get("technology_path")
        if observed.get("technology_library") and (
                not technology_path or not os.path.isfile(technology_path)):
            failures.append({
                "library": library, "reason": "technology_file_missing",
                "technology_path": technology_path,
            })
    result = {
        "ok": not failures,
        "status": "verified" if not failures else "failed",
        "probe_path": path,
        "bindings": parsed["bindings"],
        "failures": failures,
    }
    if failures:
        raise TechnologyBindingError(
            "working workspace technology binding validation failed: %s"
            % json.dumps(failures, sort_keys=True)
        )
    return result


def workspace_technology_validation_required(manifest):
    return any(
        row.get("source_probe_available") and row.get("technology_library")
        for row in (manifest or {}).get("bindings") or []
    )
