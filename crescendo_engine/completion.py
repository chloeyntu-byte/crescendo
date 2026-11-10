from __future__ import print_function

import os
import hashlib
import re
import stat

from .rdb import normalize_corner_name
from .utils import atomic_write_json, is_under, utc_timestamp
from .owned_solver_logs import owned_solver_log_reader, OwnedSolverLogError


class CompletionEvidenceError(RuntimeError):
    pass


class UnsupportedCompletionEvidence(CompletionEvidenceError):
    pass


_SPECTRE_COMPLETION = re.compile(
    r"spectre\s+completes\s+with\s+(\d+)\s+errors?\b[^\r\n]*",
    re.IGNORECASE,
)
_DONE_MESSAGES = set(("done", "completed", "complete", "passed", "pass", "success", "succeeded"))


def _relative_artifact(path, workspace_root, label):
    absolute = os.path.abspath(path)
    if not is_under(absolute, workspace_root):
        raise CompletionEvidenceError("%s escapes the private candidate workspace: %s" % (label, absolute))
    real = os.path.realpath(absolute)
    if not is_under(real, workspace_root):
        raise CompletionEvidenceError("%s resolves outside the private candidate workspace: %s" % (label, absolute))
    return os.path.relpath(absolute, workspace_root).replace(os.sep, "/")


def _owned_setup_results_root(workspace):
    if not workspace.get("same_library_objects"):
        return None
    from .same_library_views import owns_view
    target = tuple(workspace.get(key) for key in ("primary_library", "setup_cell", "setup_view"))
    if not all(target) or not owns_view(workspace, target, role="maestro", require_marker=True):
        raise CompletionEvidenceError("private Maestro result ownership was not proven")
    setup_dir = workspace.get("setup_dir")
    expected = os.path.join(os.path.realpath(workspace["source_libraries"][target[0]]), target[1], target[2])
    if not setup_dir or os.path.abspath(setup_dir) != expected:
        raise CompletionEvidenceError("private Maestro setup does not match its exact owned view")
    return os.path.join(expected, "results", "maestro")


def _artifact_reference(path, workspace, label):
    if is_under(os.path.abspath(path), workspace["root"]):
        return _relative_artifact(path, workspace["root"], label)
    results_root = _owned_setup_results_root(workspace)
    if results_root is None:
        return _relative_artifact(path, workspace["root"], label)
    relative = _relative_artifact(path, results_root, label)
    return "owned_setup_results/" + relative


def _tail_text(path, byte_limit=65536):
    with open(path, "rb") as stream:
        stream.seek(0, os.SEEK_END)
        size = stream.tell()
        stream.seek(max(0, size - int(byte_limit)), os.SEEK_SET)
        data = stream.read(int(byte_limit))
    return data.decode("utf-8", "replace")


def _spectre_log_evidence(path, workspace_root):
    if not os.path.isfile(path):
        raise UnsupportedCompletionEvidence("Spectre completion log is missing: %s" % path)
    text = _tail_text(path)
    matches = list(_SPECTRE_COMPLETION.finditer(text))
    if not matches:
        raise UnsupportedCompletionEvidence(
            "Spectre log has no recognized completion summary; add a completion adapter for this simulator/version: %s" % path
        )
    match = matches[-1]
    error_count = int(match.group(1))
    if error_count != 0:
        raise CompletionEvidenceError("Spectre reported %d error(s): %s" % (error_count, path))
    return {
        "path": _relative_artifact(path, workspace_root, "Spectre log"),
        "size_bytes": os.path.getsize(path),
        "completion": match.group(0).strip()[:512],
        "error_count": error_count,
    }


def _alps_log_evidence(task, owned_root, launch_epoch):
    """Select a fresh exact-task ALPS log without assuming Spectre logFile.

    A warning file is diagnostic only. An unsafe/stale primary companion is
    never hidden by falling back to another log. Neither log proves completion;
    owned terminal receipts and actual result/parameter checks remain required.
    """
    if task.get("execution_simulator") != "alps":
        raise CompletionEvidenceError("ALPS log selection requires its task identity")
    psf = task["psf_dir"]
    for kind, path in (("alps_psf_log", psf + ".log"), ("psf_log", os.path.join(psf, "logFile"))):
        source_present = os.path.lexists(path)
        try:
            with owned_solver_log_reader(path, owned_root, launch_epoch, task) as (descriptor, metadata):
                if metadata["bytes"] == 0:
                    raise CompletionEvidenceError("ALPS fresh owned PSF result log is missing, empty or stale: " + path)
                data = os.pread(descriptor, 65536, max(0, metadata["bytes"] - 65536))
                if _SPECTRE_COMPLETION.search(data.decode("utf-8", "replace")):
                    raise CompletionEvidenceError("ALPS result log contains conflicting Spectre completion evidence")
                result = dict(metadata, kind=kind, execution_simulator="alps", fresh=True)
            return result
        except FileNotFoundError:
            if source_present or os.path.lexists(path):
                raise CompletionEvidenceError("ALPS preferred PSF result log is dangling or changed; unsafe log rejected: " + path)
            continue
        except OwnedSolverLogError as error:
            raise CompletionEvidenceError("ALPS fresh owned PSF result log is missing, empty or stale; unsafe log rejected (%s): %s" % (error.reason, path))
    raise UnsupportedCompletionEvidence("ALPS fresh owned PSF result log is missing, empty or stale: " + psf)


def _shared_spectre_log_binding(path, psf_dir, history_root, test_name):
    if not os.path.islink(path):
        return None
    real_log = os.path.realpath(path)
    _relative_artifact(real_log, history_root, "shared Spectre log")
    components = os.path.relpath(real_log, history_root).split(os.sep)
    if (len(components) != 4 or not components[0].isdigit()
            or components[1:] != [test_name, "psf", "spectre.out"]):
        raise CompletionEvidenceError(
            "shared Spectre log is not bound to the same history/test: %s" % path
        )
    expected_psf = os.path.realpath(psf_dir)
    opening = re.compile(r"^\s*Opening the PSF(?:ASCII)? file (.+?)\s+\.\.\.\s*$")
    with open(real_log, "r", errors="replace") as stream:
        for line in stream:
            match = opening.match(line)
            if not match:
                continue
            reference = match.group(1).strip().strip('"')
            output_path = os.path.realpath(os.path.join(os.path.dirname(real_log), reference))
            if os.path.dirname(output_path) == expected_psf:
                return {
                    "mode": "native_incremental_shared_log",
                    "point_psf_output": reference,
                    "real_log": os.path.relpath(real_log, history_root).replace(os.sep, "/"),
                }
    raise CompletionEvidenceError(
        "shared Spectre log has no output binding for the completed point PSF: %s" % psf_dir
    )


def _enabled_tests(setup):
    return [row for row in setup.get("tests", []) if row.get("enabled", True)]


def _enabled_corner_names(setup):
    return [
        normalize_corner_name(row.get("name"))
        for row in setup.get("corners", [])
        if row.get("enabled", True) and row.get("name") is not None
    ]


def _expected_output_keys(setup):
    keys = []
    for test in _enabled_tests(setup):
        for output in test.get("outputs", []):
            if output.get("name"):
                keys.append("%s::%s" % (test["name"], output["name"]))
    return sorted(set(keys))


def _point_map(parsed):
    points = {}
    for row in parsed.get("points", []):
        points[row.get("pointID")] = row
    if not points:
        for output in parsed.get("outputs", []):
            for value in output.get("values", []):
                point_id = value.get("point_id")
                if point_id is not None:
                    points.setdefault(point_id, {
                        "pointID": point_id,
                        "designPointNumber": value.get("design_point"),
                        "corner_name": value.get("corner"),
                    })
    return points


def _validate_command(command):
    if not command:
        raise CompletionEvidenceError("ADE command evidence is missing")
    if command.get("startup_timed_out"):
        raise CompletionEvidenceError("ADE startup timed out")
    if command.get("timed_out"):
        raise CompletionEvidenceError("ADE command timed out")
    if command.get('schema_version') == 'crescendo.maestro_baseline.v1':
        if command.get('guardian_returncode') != 0 or command.get('cleanup_pending') is not False:
            raise CompletionEvidenceError('Native Maestro guardian termination is unproven')
    elif command.get("returncode") != 0:
        raise CompletionEvidenceError("ADE command returned %r" % command.get("returncode"))
    history_name = command.get("history_name")
    if not history_name:
        raise UnsupportedCompletionEvidence("ADE command did not record a unique history identity")
    return history_name


def _validate_rdb_identity(workspace, command, parsed, history_name):
    workspace_root = workspace["root"]
    path = parsed.get("path")
    if not path or not os.path.isfile(path):
        raise CompletionEvidenceError("the bound ADE RDB is missing")
    if (command.get('schema_version') == 'crescendo.maestro_baseline.v1' and
            (not command.get('rdb_path') or os.path.abspath(path) != os.path.abspath(command['rdb_path']))):
        raise CompletionEvidenceError('Native Maestro baseline RDB differs from the bound history path')
    relative = _artifact_reference(path, workspace, "ADE RDB")
    info = os.lstat(path)
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise CompletionEvidenceError("ADE RDB is not a regular unique history file")
    expected_name = history_name + ".rdb"
    if os.path.basename(path) != expected_name:
        raise CompletionEvidenceError(
            "ADE RDB history mismatch: expected %s, found %s" % (expected_name, os.path.basename(path))
        )
    started_epoch = command.get("started_epoch")
    if started_epoch is not None and os.path.getmtime(path) + 2 < float(started_epoch):
        raise CompletionEvidenceError("ADE RDB predates the current candidate run: %s" % path)
    return {
        "path": relative,
        "history_name": history_name,
        "size_bytes": os.path.getsize(path),
        "mtime_epoch": os.path.getmtime(path),
    }


def _completed_point_statuses(setup, parsed, point_map, native_corner_identity=None):
    """Collect completed RDB points; corner/parameter proof is a later gate."""
    statuses = parsed.get("test_statuses") or []
    if not statuses:
        raise UnsupportedCompletionEvidence("ADE RDB has no testStatus evidence")
    expected_tests = [row["name"] for row in _enabled_tests(setup)]
    by_test = dict((name, []) for name in expected_tests)
    for status in statuses:
        if status.get("test_name") in by_test:
            by_test[status["test_name"]].append(status)
    missing_tests = sorted(name for name, rows in by_test.items() if not rows)
    if missing_tests:
        raise CompletionEvidenceError("ADE RDB is missing completed points for test(s): %s" % ", ".join(missing_tests))
    expected_corners = set(_enabled_corner_names(setup))
    results = []
    for test_name in expected_tests:
        observed_corners = set()
        for status in by_test[test_name]:
            message = str(status.get("status_message") or "").strip().lower()
            status_code = status.get("statusCode")
            if status_code not in (None, ""):
                try:
                    completed_status = int(status_code) == 3
                except (TypeError, ValueError):
                    completed_status = False
                if not completed_status:
                    raise CompletionEvidenceError(
                        "ADE point %s/%s has non-complete statusCode=%r" % (
                            test_name, status.get("pointID"), status_code,
                        )
                    )
            elif message not in _DONE_MESSAGES:
                raise CompletionEvidenceError(
                    "ADE point %s/%s has no statusCode and is not complete: message=%r" % (
                        test_name, status.get("pointID"), status.get("status_message"),
                    )
                )
            point = point_map.get(status.get("pointID"), {})
            corner = normalize_corner_name(point.get("corner_name"))
            observed_corners.add(corner)
            result = {
                "test": test_name,
                "point_id": status.get("pointID"),
                "design_point": point.get("designPointNumber"),
                "corner": corner,
                "status_code": status.get("statusCode"),
                "status": status.get("status_message"),
            }
            physical_test = status.get("physical_test")
            if physical_test is not None:
                expected = (parsed.get("candidate_test_name_map") or {}).get(physical_test)
                if expected != test_name:
                    raise CompletionEvidenceError("candidate physical test identity mismatch")
                result["artifact_test"] = physical_test
            results.append(result)
        if native_corner_identity is None and expected_corners and not expected_corners.issubset(observed_corners):
            missing = sorted(expected_corners - observed_corners)
            raise CompletionEvidenceError(
                "ADE RDB is missing corner(s) for test %s: %s" % (test_name, ", ".join(missing))
            )
    return results


def _validate_statuses(setup, parsed, point_map, native_corner_identity=None, point_identity_loader=None):
    results = _completed_point_statuses(setup, parsed, point_map, native_corner_identity)
    if native_corner_identity is not None:
        from .private_netlist import qualify_native_corner_mapping, PrivateNetlistError
        if not isinstance(native_corner_identity, dict):
            raise CompletionEvidenceError("native corner identity is unavailable")
        snapshot = native_corner_identity.get("setup_snapshot")
        schedule = native_corner_identity.get("schedule")
        if not isinstance(snapshot, dict) or not isinstance(schedule, dict) or snapshot.get("ok") is not True or not (
                (snapshot.get("saved_source") or {}).get("corner_read_ok") is True and
                (snapshot.get("private_setup") or {}).get("corner_read_ok") is True):
            raise CompletionEvidenceError("native saved/private corner snapshot is not proven")
        if point_identity_loader is not None:
            point_identity_loader(results)
        try:
            coverage = qualify_native_corner_mapping(setup, results, parsed, snapshot, schedule)
        except PrivateNetlistError as error:
            raise CompletionEvidenceError("native Maestro corner/point identity mismatch: %s" % error)
        parsed["native_corner_coverage"] = coverage
        if coverage.get("ok") is not True:
            mismatch = [row for row in coverage.get("test_differences") or []
                        if row.get("missing") or row.get("unexpected") or row.get("mapping_error")]
            model_mismatch = any(failure.get('model_path_errors') or
                any(identity.get('missing_models') for identity in failure.get('closest_identities') or [])
                for row in mismatch for failure in (row.get('corner_expansion') or {}).get('identity_failures') or [])
            summary = '; '.join('%s: %s' % (row.get('test'), row.get('mapping_error') or 'point membership mismatch')
                                for row in mismatch[:4]) or 'native group enablement differs'
            if model_mismatch:
                summary = 'model path/section identity mismatch; ' + summary
            error = CompletionEvidenceError("native Maestro corner/point coverage differs: " + summary)
            error.detail = {'failure_stage': 'native_corner_coverage', 'execution_started': True,
                            'phase': 'completed_native_point_identity', 'native_coverage': coverage}
            raise error
        if native_corner_identity.get('point_parameters_required') is True:
            _validate_native_effective_scope(setup, results, parsed, snapshot)
        aliases = dict(((row["test"], row["point_id"]), row["source_corner"]) for row in results)
        actual = dict(((row["test"], row["point_id"]), row["corner"]) for row in results)
        # Keep actual native names and point IDs. Source-group aliases serve
        # only the user's corner selection before min/mean/max reduction.
        outputs = list((parsed.get("output_map") or {}).values()) + list(parsed.get("outputs") or [])
        for output in outputs:
            for value in output.get("values") or []:
                key = (output.get("test"), value.get("point_id"))
                if key in aliases:
                    if "corner" in value and normalize_corner_name(value["corner"]) != actual[key]:
                        raise CompletionEvidenceError("output corner does not match its native test/point")
                    value["source_corner"] = aliases[key]
    return results


def _validate_native_effective_scope(setup, statuses, parsed, snapshot):
    """Modern exact-PSF contract proves saved used scope and full sweep grid."""
    from .native_identity import (
        effective_variables, execution_plan_variables, point_variable_checks, sweep_coverage,
    )
    tests = dict((row['name'], row) for row in _enabled_tests(setup))
    corners = dict((row['name'], row) for row in setup.get('corners') or [])
    mapping = dict((row['corner'], row.get('source_corner')) for row in statuses)
    all_checks, all_sweeps, all_differences, unused = [], [], [], []
    for test_name, test in sorted(tests.items()):
        rows = [row for row in statuses if row['test'] == test_name]
        plan = execution_plan_variables(setup, test_name)
        if plan['errors']:
            parsed['native_effective_scope'] = {'ok': False, 'plan_errors': plan['errors']}
            raise UnsupportedCompletionEvidence('Native saved execution-plan scope is unresolved: %s' % plan['errors'])
        used_names = set(item['name'] for row in rows for item in row.get('native_parameters') or [])
        used_names.update(item['name'] for item in parsed.get('parameters') or []
                          if item.get('pointID') in set(row['point_id'] for row in rows) and item.get('name'))
        if any(row.get('temperature') for row in rows):
            used_names.add('temperature')
        for row in rows:
            corner = corners.get(row.get('source_corner'))
            expected = effective_variables(setup, test, corner)
            unused.extend({'test': test_name, 'variable': name, 'source': item.get('source'),
                           'reason': 'not_in_native_netlist_or_point_rdb'}
                          for name, item in expected.items() if name not in used_names)
            expected = dict((name, item) for name, item in expected.items() if name in used_names)
            differences, checks = point_variable_checks(setup, test, corner, row, parsed, snapshot,
                                                       effective_scope=expected)
            all_differences.extend(dict(item, test=test_name) for item in differences)
            all_checks.extend(dict(item, test=test_name) for item in checks)
        all_sweeps.extend(dict(item, test=test_name) for item in
                          sweep_coverage(setup, test, corners, rows, mapping, used_names=used_names,
                                         strict_multiplicity=True))
    proof = {'schema_version': 'crescendo.native_effective_scope.v1',
             'ok': not all_differences and not all_sweeps,
             'point_checks': all_checks, 'sweep_differences': all_sweeps,
             'variable_differences': all_differences, 'unused_saved_variables': unused,
             'param_source': 'current_psf_designParamVals_with_preserved_native_declarations'}
    parsed['native_effective_scope'] = proof
    if not proof['ok']:
        if any(row.get('reason') == 'native_point_repeat_dimension_unqualified' for row in all_sweeps):
            raise UnsupportedCompletionEvidence('Native repeated points have no proven source repeat dimension: %s' % all_sweeps[:4])
        raise CompletionEvidenceError('native Maestro effective variable/sweep identity mismatch: %s' %
                                      (all_differences[:4] or all_sweeps[:4]))


def _validate_outputs(setup, parsed, status_rows, required_output_keys=None):
    output_map = parsed.get("output_map") or {}
    expected = sorted(set(required_output_keys if required_output_keys is not None else _expected_output_keys(setup)))
    missing = [key for key in expected if key not in output_map]
    if missing:
        raise CompletionEvidenceError("ADE RDB is missing configured output(s): %s" % ", ".join(missing))
    points_by_test = {}
    for status in status_rows:
        points_by_test.setdefault(status["test"], set()).add(status["point_id"])
    summaries = []
    for key in expected:
        output = output_map[key]
        observed = set()
        errors = []
        for value in output.get("values", []):
            if value.get("point_id") is not None:
                observed.add(value["point_id"])
            if value.get("error"):
                errors.append(str(value["error"]))
        test_name = output.get("test")
        missing_points = sorted(points_by_test.get(test_name, set()) - observed)
        if missing_points:
            raise CompletionEvidenceError("output %s is missing point(s): %s" % (key, missing_points))
        if errors:
            raise CompletionEvidenceError("output %s has evaluation error(s): %s" % (key, "; ".join(errors[:3])))
        summaries.append({"key": key, "point_count": len(observed), "scalar": bool(output.get("scalar"))})
    return summaries


def _history_root(workspace, test, history_name):
    # Maestro owns history below the opened setup, even when its test targets
    # another cell or library. Historical callers may lack setup metadata.
    setup_library = workspace.get("primary_library") or test["library"]
    setup_cell = workspace.get("setup_cell") or test["cell"]
    return os.path.join(
        workspace["simulation_root"], setup_library, setup_cell,
        workspace.get("setup_view") or "maestro", "results", "maestro", history_name,
    )


def _completed_model_resolution(workspace, setup, command, status, config, snapshot, path_cache):
    """Share capture's model identity rules with the completed native point.

    runSimulation is retained native argv evidence, never a script to execute.
    Accept one literal solver command and the observed Cadence LD_PRELOAD
    preamble, without applying it. Other shell setup remains unknown.
    Physical paths and verified remaps retain model authority.
    """
    import shlex
    from .native_identity import resolve_task_model_identity
    from .native_log_io import native_log_read_scope
    task = dict(status)
    path = os.path.join(status['native_cwd'], 'runSimulation')
    evidence = {'path': path, 'basis': 'native_runSimulation_unavailable'}
    try:
        if os.path.lexists(path):
            _artifact_reference(path, workspace, 'native point solver command')
            info = os.lstat(path)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > 256 * 1024:
                raise ValueError('Native point runSimulation is not a bounded regular file')
            with native_log_read_scope():
                descriptor = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0))
                with os.fdopen(descriptor, 'rb') as stream:
                    if not os.path.samestat(info, os.fstat(stream.fileno())):
                        raise ValueError('Native point runSimulation changed during open')
                    raw = stream.read(256 * 1024 + 1)
            if len(raw) > 256 * 1024:
                raise ValueError('Native point runSimulation exceeded its size bound')
            evidence.update(sha256=hashlib.sha256(raw).hexdigest(), size_bytes=len(raw))
            text = raw.decode('utf-8').replace('\\\n', '')
            lines = [line.strip() for line in text.splitlines() if line.strip() and not line.lstrip().startswith('#')]
            ignored_exports = []
            for line in lines[:-1]:
                # IC618 interactive runSimulation prepends its C++ loader.
                # This does not set model search or cwd. Do not interpret any
                # other export, shell command, or command substitution.
                words = shlex.split(line, comments=True)
                if (len(words) != 2 or words[0] != 'export' or
                        not words[1].startswith('LD_PRELOAD=') or
                        any(char in line for char in (';', '|', '&', '<', '>', '`', '$(', '\x00'))):
                    raise ValueError('Native point runSimulation has unsupported shell setup')
                value = words[1].split('=', 1)[1]
                parts = value.split(':')
                if (not parts or any(not part.startswith('/') or any(char in part for char in '$*?[]~')
                        for part in parts[:-1]) or not (parts[-1] in ('$LD_PRELOAD', '${LD_PRELOAD}') or
                        (parts[-1].startswith('/') and not any(char in parts[-1] for char in '$*?[]~')))):
                    raise ValueError('Native point runSimulation has unsupported LD_PRELOAD setup')
                ignored_exports.append({'name': 'LD_PRELOAD', 'assignment': words[1],
                                        'policy': 'loader_preamble_recorded_not_executed'})
            if ignored_exports:
                references = [row.get('file') or '' for row in status.get('native_models') or []]
                references.extend(row.get('file') or '' for group in setup.get('model_groups') or []
                                  for row in group.get('models') or [])
                references.append(((config or {}).get('cadence') or {}).get(
                    (command.get('execution_simulator') or 'spectre') + '_model_dir') or '')
                if any(re.search(r'\$(?:LD_PRELOAD\b|\{LD_PRELOAD\})', value) for value in references):
                    raise ValueError('Native model search depends on an uninterpreted loader export')
                evidence['ignored_environment_exports'] = ignored_exports
            if not lines or any(char in lines[-1] for char in (';', '|', '&', '<', '>', '`', '$', '*', '?', '[', ']', '~', '\x00')):
                raise ValueError('Native point runSimulation is not one literal solver command')
            argv = shlex.split(lines[-1], comments=True)
            if argv and argv[0] == 'exec':
                argv = argv[1:]
            simulator = command.get('execution_simulator') or 'spectre'
            if (not argv or os.path.basename(argv[0]) != simulator or
                    sum(os.path.abspath(os.path.join(status['native_cwd'], value)) == status['netlist_path']
                        for value in argv[1:]) != 1):
                raise ValueError('Native point runSimulation does not bind the selected solver and exact input.scs')
            task['argv'] = argv
            evidence.update(basis='exact_point_native_runSimulation', argv=argv)
        result = resolve_task_model_identity(task, config, command.get('execution_simulator') or 'spectre',
                                             setup, snapshot, path_cache)
    except (OSError, ValueError, CompletionEvidenceError) as error:
        result = {'schema_version': 'crescendo.native_model_identity.v1', 'native_cwd': status['native_cwd'],
                  'include_dirs': [], 'references': [], 'source_references': [], 'errors': [str(error)]}
    result['argv_evidence'] = evidence
    return result


def _prepare_native_point_identity(workspace, setup, command, history_name, statuses, diagnostic_path,
                                   config=None, parsed=None):
    """Derive exact PSF requests from owned RDB points and native netlists."""
    from .private_netlist import top_level_native_parameters, native_netlist_identity, MAX_NETLIST_BYTES
    from .native_log_io import native_log_read_scope
    tests = dict((row["name"], row) for row in _enabled_tests(setup))
    points = []
    requests = []
    model_resolution_cache = {}
    native_identity = command.get("native_corner_identity", workspace.get("native_corner_identity")) or {}
    snapshot = native_identity.get("setup_snapshot") or {}
    from .native_identity import numeric_domain, effective_variables
    corner_names = set(variable['name'] for corner in setup.get('corners') or [] if corner.get('enabled', True)
                       for variable in corner.get('variables') or []
                       if numeric_domain(variable.get('value')) is not None)
    for status in statuses:
        test = tests[status["test"]]
        artifact_test = status.get("artifact_test") or status["test"]
        if not isinstance(artifact_test, str) or artifact_test in (".", "..") or os.path.basename(artifact_test) != artifact_test:
            raise CompletionEvidenceError("unsafe physical test identity for native corner proof")
        history_root = command.get("results_dir") if (command.get("backend") or workspace.get("setup_backend")) == "adexl" else _history_root(workspace, test, history_name)
        if not history_root:
            raise UnsupportedCompletionEvidence("native corner netlist history is unavailable")
        directory = os.path.join(history_root, str(status["point_id"]), artifact_test, "netlist")
        path = os.path.join(directory, "input.scs")
        _artifact_reference(path, workspace, "native corner netlist")
        info = os.lstat(path)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > MAX_NETLIST_BYTES:
            raise UnsupportedCompletionEvidence("native corner netlist is not a bounded regular file: %s" % path)
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        with native_log_read_scope():
            descriptor = os.open(path, flags)
            with os.fdopen(descriptor, "rb") as stream:
                if not os.path.samestat(info, os.fstat(stream.fileno())):
                    raise CompletionEvidenceError("native corner netlist changed during open")
                text = stream.read(MAX_NETLIST_BYTES + 1)
        if len(text) > MAX_NETLIST_BYTES:
            raise UnsupportedCompletionEvidence("native corner netlist exceeds qualification bound")
        digest = hashlib.sha256(text).hexdigest()
        text = text.decode("utf-8")
        status.update(native_parameters=top_level_native_parameters(text), netlist_path=path, native_cwd=directory)
        status.update(native_netlist_identity(text))
        if status.get("native_models"):
            status["native_model_resolution"] = _completed_model_resolution(
                workspace, setup, command, status, config, snapshot, model_resolution_cache)
        status['native_declared_parameters'] = list(status['native_parameters'])
        status['native_declared_temperature'] = status.get('temperature')
        status['param_source'] = 'legacy_static_native_netlist'
        used_names = set(row['name'] for row in status['native_parameters'])
        if status.get('temperature'):
            used_names.add('temperature')
        used_names.update(row['name'] for row in (parsed or {}).get('parameters') or []
                          if row.get('pointID') == status['point_id'] and row.get('name'))
        names = corner_names.intersection(used_names)
        if 'temperature' in corner_names and status.get('temperature'):
            names.add('temperature')
        if command.get("execution_simulator") == "alps":
            names.update(row["name"] for row in status["native_declared_parameters"] if isinstance(row.get("value"), (int, float)))
            names.add("temperature")
        dynamic_scope = {}
        for name, variable in effective_variables(setup, test).items():
            domain = numeric_domain(variable.get('value'))
            if name in used_names and domain is not None and len(domain) > 1 and variable.get('sweep_enabled') is not False:
                names.add(name)
                dynamic_scope[name] = variable.get('source')
        # Shared input.scs contains the final point's dependent expressions.
        # Read their actual PSF values; Python never fabricates point values.
        import re
        declarations = dict((row['name'], row.get('expression') or '')
                            for row in status['native_declared_parameters'])
        dependencies = {}
        changed = True
        while changed:
            changed = False
            for name, expression in declarations.items():
                if name in names:
                    continue
                references = set(re.findall(r'\b[A-Za-z_][A-Za-z0-9_]*\b', expression))
                parents = sorted(references.intersection(names))
                if parents:
                    names.add(name)
                    dependencies[name] = parents
                    changed = True
        names = sorted(names)
        status['psf_required_variables'] = names
        status['psf_dynamic_variable_scopes'] = dynamic_scope
        status['psf_declared_dynamic_dependencies'] = dependencies
        points.append(dict(status, netlist_sha256=digest))
        psf_dir = os.path.join(os.path.dirname(directory), 'psf')
        _artifact_reference(psf_dir, workspace, 'exact point PSF parameter source')
        if not os.path.isdir(psf_dir):
            raise UnsupportedCompletionEvidence('Exact point PSF directory is unavailable')
        requests.append({'point_id': status['point_id'], 'test': status['test'], 'psf_dir': psf_dir, 'names': names})
    document = {"schema_version": "crescendo.native_point_identity.v1", "points": points,
                "history_name": history_name, "source": "exact_owned_native_point_netlists"}
    scope_path = workspace.get("native_scope_path")
    if scope_path:
        import json
        _artifact_reference(scope_path, workspace, "native batch scope metadata")
        with native_log_read_scope():
            descriptor = os.open(scope_path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
            with os.fdopen(descriptor, "rb") as stream:
                info = os.fstat(stream.fileno())
                if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > 2 * 1024 * 1024:
                    raise UnsupportedCompletionEvidence("native batch scope metadata is not a bounded regular file")
                raw = stream.read(2 * 1024 * 1024 + 1)
        if len(raw) > 2 * 1024 * 1024:
            raise UnsupportedCompletionEvidence("native batch scope metadata exceeded size bound")
        document["native_batch_scope"] = json.loads(raw.decode("utf-8"))
    # Persist declaration/scope evidence even if exact PSF readback fails.
    atomic_write_json(diagnostic_path, document)
    return document, requests


def prepare_native_point_parameter_requests(workspace, setup, command, parsed, candidate_root, config=None):
    """Prepare a read while Ocean is alive; this does not prove completion/cleanup."""
    history = command['history_name']
    _validate_rdb_identity(workspace, command, parsed, history)
    statuses = _completed_point_statuses(setup, parsed, _point_map(parsed),
                                         command.get('native_corner_identity', workspace.get('native_corner_identity')))
    unused, requests = _prepare_native_point_identity(
        workspace, setup, command, history, statuses, os.path.join(candidate_root, 'point_identity.json'),
        config=config, parsed=parsed)
    return requests


def _load_native_point_identity(workspace, setup, command, history_name, statuses, diagnostic_path,
                                config=None, parameter_reader=None, parsed=None, reader_deadline=None, reader_cancel_check=None):
    """Bind expanded corners to these verified native point netlists."""
    document, requests = _prepare_native_point_identity(
        workspace, setup, command, history_name, statuses, diagnostic_path, config=config, parsed=parsed)
    points = document['points']
    required_psf = (workspace.get('native_corner_identity') or {}).get('point_parameters_required') is True
    if parameter_reader is None and config is not None:
        from .native_point_parameters import read_point_parameters
        parameter_reader = lambda requested: read_point_parameters(requested, config, os.path.dirname(diagnostic_path),
            deadline=reader_deadline, cancel_check=reader_cancel_check)
    if required_psf and parameter_reader is None:
        raise UnsupportedCompletionEvidence('Exact point PSF parameter reader is unavailable for this native identity contract')
    if parameter_reader is not None and any(request['names'] for request in requests):
        try:
            values = parameter_reader(requests)
        except Exception as error:
            document['psf_parameter_error'] = {'message': '%s: %s' % (type(error).__name__, error),
                                                'detail': getattr(error, 'detail', {})}
            atomic_write_json(diagnostic_path, document)
            failure = UnsupportedCompletionEvidence(str(error))
            failure.cancelled = bool(getattr(error, 'cancelled', False))
            failure.detail = {'failure_stage': 'native_point_parameter_readback', 'diagnostic_path': diagnostic_path,
                              'reader': getattr(error, 'detail', {}) or {},
                              'command': (getattr(error, 'detail', {}) or {}).get('command')}
            raise failure
        if not isinstance(values, list) or len(values) != len(statuses):
            raise CompletionEvidenceError('Exact point PSF parameter reader returned the wrong point count')
        import math
        for status, point, request, variables in zip(statuses, points, requests, values):
            if not isinstance(variables, dict) or set(variables) != set(request['names']) or any(
                    isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value))
                    for value in variables.values()):
                raise CompletionEvidenceError('Exact point PSF parameter reader returned incomplete/invalid variable identity')
            if command.get("execution_simulator") == "alps":
                expected = dict((row["name"], row["value"]) for row in status["native_declared_parameters"] if isinstance(row.get("value"), (int, float)) and not isinstance(row.get("value"), bool))
                temperature = (status.get("temperature") or {}).get("value")
                if isinstance(temperature, (int, float)) and not isinstance(temperature, bool):
                    expected["temperature"] = temperature
                for name, planned in expected.items():
                    actual = variables.get(name)
                    if actual is None or abs(actual - planned) > max(abs(planned), 1e-300) * 1e-9:
                        raise CompletionEvidenceError("ALPS actual point parameter differs from native candidate: " + name)
            replacements = dict((name, {'name': name, 'value': float(value), 'expression': '%.17g' % value,
                                        'source': 'current_psf_designParamVals'}) for name, value in variables.items())
            status['native_parameters'] = [replacements.pop(row['name'], row) for row in status['native_parameters']]
            status['native_parameters'].extend(replacements.values())
            status['param_source'] = 'current_psf_designParamVals'
            status['psf_parameters'] = variables
            if 'temperature' in variables:
                status['temperature'] = {'value': variables['temperature'], 'expression': str(variables['temperature']),
                                         'source': 'current_psf_designParamVals'}
            point.update(native_parameters=status['native_parameters'], param_source=status['param_source'],
                         psf_parameters=variables, temperature=status.get('temperature'),
                         psf_dir=os.path.join(os.path.dirname(status['native_cwd']), 'psf'))
        document['param_source'] = 'current_psf_designParamVals'
        atomic_write_json(diagnostic_path, document)


def _alps_native_receipt(workspace, command, psf_dir):
    expected = os.path.realpath(os.path.join(os.path.dirname(psf_dir), "netlist"))
    matches = []
    for receipt in (command.get("spectre_timing") or {}).get("runs") or []:
        cwd = receipt.get("cwd")
        if not cwd:
            continue
        if not os.path.isabs(cwd):
            cwd = os.path.join(workspace["root"], cwd)
        if os.path.realpath(cwd) == expected:
            matches.append(receipt)
    if len(matches) != 1 or matches[0].get("returncode") != 0:
        raise UnsupportedCompletionEvidence("ALPS exact point owned zero-exit receipt is unavailable or ambiguous")
    receipt = matches[0]
    if not receipt.get("started_epoch") or not receipt.get("finished_epoch") or receipt["finished_epoch"] < receipt["started_epoch"]:
        raise CompletionEvidenceError("ALPS owned receipt has invalid times")
    return dict(receipt)


def _baseline_history_protocol(command, candidate_root, history_name):
    """Qualify an actual owned Maestro Run/Wait/Close transcript, not a CLI receipt."""
    import math
    record = command.get('native_baseline')
    if command.get('schema_version') != 'crescendo.maestro_baseline.v1' or not isinstance(record, dict):
        raise UnsupportedCompletionEvidence('Native Maestro baseline protocol evidence is unavailable')
    if (record.get('schema_version') != 'crescendo.maestro_baseline.v1' or
            not isinstance(record.get('candidate_id'), str) or not record.get('candidate_id') or
            record.get('candidate_id') != command.get('candidate_id') or
            record.get('request_root') != os.path.abspath(candidate_root) or
            record.get('history_name') != history_name or record.get('run_id') != command.get('run_id') or
            not isinstance(record.get('run_id'), str) or not record.get('run_id') or
            record.get('native_session_closed') is not True or record.get('local_cleanup_proven') is not True):
        raise CompletionEvidenceError('Native Maestro baseline run, history or cleanup identity is unproven')
    started, finished = record.get('started_epoch'), record.get('finished_epoch')
    if (isinstance(started, bool) or isinstance(finished, bool) or
            not isinstance(started, (int, float)) or not isinstance(finished, (int, float)) or
            not math.isfinite(started) or not math.isfinite(finished) or started <= 0 or finished < started or
            command.get('started_epoch') != started or command.get('finished_epoch') != finished):
        raise CompletionEvidenceError('Native Maestro baseline timestamps are invalid')
    nonce, path = record.get('nonce'), record.get('protocol_path')
    if (not isinstance(nonce, str) or not re.fullmatch(r'[a-f0-9]{32}', nonce) or
            not isinstance(path, str) or os.path.abspath(path) != os.path.join(os.path.abspath(candidate_root), 'native_baseline', 'native_baseline_protocol.tsv')):
        raise CompletionEvidenceError('Native Maestro baseline protocol path or nonce is invalid')
    _relative_artifact(path, os.path.abspath(candidate_root), 'native baseline protocol')
    try:
        info = os.lstat(path)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > 1024 * 1024 or info.st_mtime + 2 < started:
            raise CompletionEvidenceError('Native Maestro baseline protocol is not a fresh owned regular file')
        descriptor = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
        with os.fdopen(descriptor, 'rb') as stream:
            if not os.path.samestat(info, os.fstat(stream.fileno())):
                raise CompletionEvidenceError('Native Maestro baseline protocol changed during open')
            raw = stream.read(1024 * 1024 + 1)
    except OSError as error:
        raise UnsupportedCompletionEvidence('Native Maestro baseline protocol could not be read: %s' % error)
    if len(raw) > 1024 * 1024 or hashlib.sha256(raw).hexdigest() != record.get('protocol_sha256'):
        raise CompletionEvidenceError('Native Maestro baseline protocol hash differs')
    try:
        rows = [line.split('\t') for line in raw.decode('utf-8').splitlines()]
    except UnicodeError:
        raise CompletionEvidenceError('Native Maestro baseline protocol encoding is invalid')
    if any(len(row) != 3 or row[0] != nonce for row in rows):
        raise CompletionEvidenceError('Native Maestro baseline protocol record identity differs')
    observed = [{'state': row[1], 'value': row[2], 'sequence': index} for index, row in enumerate(rows)]
    if observed != record.get('records'):
        raise CompletionEvidenceError('Native Maestro baseline protocol records differ')
    required = ['RUN_BEGIN', 'RUN_ID', 'SIM_DONE', 'WAIT_RETURN', 'HISTORY', 'CLOSED']
    selected = [row for row in observed if row['state'] in required]
    if ([row['state'] for row in selected] != required or
            any(row['state'] in ('ERROR', 'STOP_ERROR', 'STOP_BEGIN', 'STOP_DONE') for row in observed) or
            selected[1]['value'] != record['run_id'] or selected[4]['value'] != history_name):
        raise CompletionEvidenceError('Native Maestro baseline Run/Wait/Close protocol is incomplete or ambiguous')
    return {'kind': 'native_maestro_history_point', 'schema_version': 'crescendo.maestro_baseline.v1',
            'candidate_id': record['candidate_id'], 'run_id': record['run_id'], 'history_name': history_name,
            'protocol_sha256': record['protocol_sha256'], 'started_epoch': started, 'finished_epoch': finished}


def _validate_psf(workspace, setup, command, history_name, status_rows, candidate_root=None):
    workspace_root = workspace["root"]
    tests = dict((row["name"], row) for row in _enabled_tests(setup))
    backend = command.get("backend") or workspace.get("setup_backend") or "maestro"
    adexl_results_dir = command.get("results_dir") if backend == "adexl" else None
    if backend == "adexl":
        if not adexl_results_dir or not os.path.isdir(adexl_results_dir):
            raise UnsupportedCompletionEvidence(
                "ADE XL command did not bind an existing point-results directory"
            )
        _relative_artifact(adexl_results_dir, workspace_root, "ADE XL point-results directory")
        if os.path.basename(os.path.normpath(adexl_results_dir)) != history_name:
            raise CompletionEvidenceError(
                "ADE XL point-results history does not match the completed RDB"
            )
    artifacts = []
    baseline = (command.get('execution_simulator') == 'alps' and
                command.get('schema_version') == 'crescendo.maestro_baseline.v1')
    baseline_receipt = _baseline_history_protocol(command, candidate_root, history_name) if baseline else None
    for status in status_rows:
        test = tests[status["test"]]
        point_id = status["point_id"]
        artifact_test = status.get("artifact_test") or status["test"]
        if (not isinstance(artifact_test, str) or artifact_test in (".", "..")
                or os.path.basename(artifact_test) != artifact_test):
            raise CompletionEvidenceError("unsafe physical test identity")
        history_root = (
            adexl_results_dir if backend == "adexl"
            else _history_root(workspace, test, history_name)
        )
        psf_dir = os.path.join(
            history_root, str(point_id), artifact_test, "psf",
        )
        if not os.path.isdir(psf_dir):
            raise UnsupportedCompletionEvidence(
                "point PSF layout is not recognized for %s point %s; add an ADE completion adapter: %s" % (
                    status["test"], point_id, psf_dir,
                )
            )
        _relative_artifact(psf_dir, workspace_root, "point PSF directory")
        sim_done = os.path.join(psf_dir, ".simDone")
        if not os.path.isfile(sim_done):
            raise CompletionEvidenceError("Simulator .simDone is missing: %s" % sim_done)
        if command.get("execution_simulator") == "alps":
            if baseline:
                marker = os.lstat(sim_done)
                if (os.path.realpath(psf_dir) != os.path.abspath(psf_dir) or
                        not stat.S_ISREG(marker.st_mode) or marker.st_nlink != 1 or
                        marker.st_mtime + 2 < baseline_receipt['started_epoch'] or
                        os.stat(psf_dir).st_mtime + 2 < baseline_receipt['started_epoch']):
                    raise CompletionEvidenceError('ALPS native point PSF or .simDone is not a fresh owned artifact')
                receipt = dict(baseline_receipt, test=status['test'], point_id=point_id,
                               corner=status.get('corner'), psf=_relative_artifact(psf_dir, workspace_root, 'ALPS point PSF'))
            else:
                receipt = _alps_native_receipt(workspace, command, psf_dir)
            log_evidence = _alps_log_evidence({"execution_simulator": "alps", "psf_dir": psf_dir},
                                             workspace_root, receipt["started_epoch"])
            if status.get("param_source") != "current_psf_designParamVals" or not status.get("psf_parameters"):
                raise UnsupportedCompletionEvidence("ALPS actual point PSF parameter readback is unproven")
            stale_spectre = os.path.join(psf_dir, "spectre.out")
            if os.path.isfile(stale_spectre) and _SPECTRE_COMPLETION.search(_tail_text(stale_spectre)):
                raise CompletionEvidenceError("ALPS point contains conflicting Spectre completion evidence")
            artifact = {"test": status["test"], "point_id": point_id, "corner": status.get("corner"),
                        "execution_simulator": "alps", "psf": _relative_artifact(psf_dir, workspace_root, "ALPS point PSF"),
                        "sim_done": _relative_artifact(sim_done, workspace_root, ".simDone marker"),
                        "receipt": receipt, "native_parameter_readback_verified": True,
                        "result_reader": command.get("result_reader", "spectre"), "solver_log": log_evidence}
            if status.get("artifact_test") is not None:
                artifact["artifact_test"] = artifact_test
            artifacts.append(artifact)
            continue
        log = os.path.join(psf_dir, "spectre.out")
        shared_binding = _shared_spectre_log_binding(
            log, psf_dir, history_root, artifact_test,
        )
        log_evidence = _spectre_log_evidence(log, workspace_root)
        if shared_binding is not None:
            log_evidence["point_binding"] = shared_binding
        artifact = {
            "test": status["test"],
            "point_id": point_id,
            "corner": status.get("corner"),
            "psf": _relative_artifact(psf_dir, workspace_root, "point PSF directory"),
            "sim_done": _relative_artifact(sim_done, workspace_root, ".simDone marker"),
            "spectre": log_evidence,
        }
        if status.get("artifact_test") is not None:
            artifact["artifact_test"] = artifact_test
        artifacts.append(artifact)
    return artifacts


def validate_candidate_completion(workspace, candidate_root, command, parsed, setup=None, output_path=None,
                                  required_output_keys=None, config=None, native_parameter_reader=None,
                                  reader_deadline=None, reader_cancel_check=None):
    setup = setup or workspace.get("setup") or {}
    from .simulators import selected_simulator
    simulator = selected_simulator(config)
    native_simulators = set((row.get("simulator") or "spectre").lower() for row in _enabled_tests(setup))
    if native_simulators and native_simulators != {simulator}:
        raise CompletionEvidenceError("Saved native simulator differs from selected execution simulator")
    if command.get("execution_simulator") not in (None, simulator):
        raise CompletionEvidenceError("Native command simulator differs from selected simulator")
    command = dict(command, execution_simulator=simulator, result_reader=(config or {}).get("cadence", {}).get("simulator_result_reader") or "spectre")
    history_name = _validate_command(command)
    if command.get('schema_version') == 'crescendo.maestro_baseline.v1':
        _baseline_history_protocol(command, candidate_root, history_name)
    point_map = _point_map(parsed)
    rdb = _validate_rdb_identity(workspace, command, parsed, history_name)
    native_identity = command.get("native_corner_identity", workspace.get("native_corner_identity"))
    if (simulator == 'alps' and command.get('schema_version') == 'crescendo.maestro_baseline.v1' and
            (not isinstance(native_identity, dict) or native_identity.get('point_parameters_required') is not True)):
        raise UnsupportedCompletionEvidence('Native Maestro baseline requires exact corner identity and point parameter readback')
    parsed.pop('native_effective_scope', None)
    def point_identity_loader(statuses):
        _load_native_point_identity(workspace, setup, command, history_name, statuses,
                                    os.path.join(candidate_root, "point_identity.json"), config=config,
                                    parameter_reader=native_parameter_reader, parsed=parsed, reader_deadline=reader_deadline,
                                    reader_cancel_check=reader_cancel_check)
    try:
        statuses = _validate_statuses(setup, parsed, point_map, native_corner_identity=native_identity,
                                     point_identity_loader=point_identity_loader if native_identity is not None else None)
    finally:
        if parsed.get('native_effective_scope'):
            atomic_write_json(os.path.join(candidate_root, 'portability_settings.json'),
                              parsed['native_effective_scope'])
    if simulator == "alps" and native_identity is None:
        point_identity_loader(statuses)
    outputs = _validate_outputs(setup, parsed, statuses, required_output_keys=required_output_keys)
    psf = _validate_psf(workspace, setup, command, history_name, statuses, candidate_root=candidate_root)
    backend = command.get("backend") or workspace.get("setup_backend") or "maestro"
    evidence = {
        "schema_version": "crescendo.completion_evidence.v1",
        "ok": True,
        "adapter": "%s-rdb-psf-v1" % backend,
        "backend": backend,
        "validated_at": utc_timestamp(),
        "history_name": history_name,
        "rdb": rdb,
        "tests": sorted(set(row["test"] for row in statuses)),
        "corners": sorted(set(row["corner"] for row in statuses if row.get("corner"))),
        "points": statuses,
        "outputs": outputs,
        "execution_simulator": simulator,
        "simulator": simulator,
        "result_reader": command["result_reader"],
        "simulator_runs": psf,
        "native_parameter_readback_verified": simulator == "alps" and all(row.get("native_parameter_readback_verified") is True for row in psf),
        "spectre_runs": psf if simulator == "spectre" else [],
    }
    if parsed.get("native_corner_coverage"):
        evidence["native_corner_coverage"] = parsed["native_corner_coverage"]
    if parsed.get('native_effective_scope'):
        evidence['native_effective_scope'] = parsed['native_effective_scope']
    if parsed.get("candidate_test_name_map"):
        evidence["candidate_test_name_map"] = dict(parsed["candidate_test_name_map"])
    path = output_path or os.path.join(candidate_root, "completion_evidence.json")
    if is_under(path, workspace["root"]):
        raise CompletionEvidenceError("completion evidence must survive outside the disposable workspace")
    atomic_write_json(path, evidence)
    return evidence
