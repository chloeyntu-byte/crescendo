from __future__ import print_function

import math
import os
import re
import uuid

from crescendo_engine.runtime import bind_text
_contract_text = bind_text()

from .utils import atomic_write_json, atomic_write_text, ensure_dir, is_under


SESSION_SCHEMA_VERSION = _contract_text("crescendo.adexl_session.v1")
CANDIDATE_SCHEMA_VERSION = _contract_text("crescendo.adexl_candidate.v1")
PROTOCOL_SCHEMA_VERSION = _contract_text("crescendo.adexl_protocol.v1")
RESULT_SCHEMA_VERSION = _contract_text("crescendo.adexl_result.v2")

REQUIRED_AXL_APIS = (
    "axlCreateSession",
    "axlSetMainSetupDBLCV",
    "axlGetMainSetupDB",
    "axlSDBHp",
    "axlRunSimulation",
    "axlGetRunData",
    "axlGetHistoryName",
    "axlGetHistoryEntry",
    "axlGetHistoryResults",
    "axlGetHistoryResultsDir",
    "axlReadHistoryResDB",
    "axlCloseSession",
)

OPTIONAL_AXL_APIS = (
    "axlGetAllFinishedRuns",
    "axlGetRunStatus",
    "axlGetResultsLocation",
    "axlStop",
    "axlStopAll",
)

STARTUP_STATES = ("PROBE_BEGIN", "PROBE_OK", "SESSION_READY")
CANDIDATE_STATES = (
    "ACCEPTED", "BEGIN", "RUN_SUBMITTED", "SIM_DONE", "RESULTS_READY", "DONE",
)
SHUTDOWN_STATES = ("CLOSE_BEGIN", "CLOSE_DONE")


class AdeXLRuntimeError(RuntimeError):
    pass


class AdeXLUnsupportedMode(AdeXLRuntimeError):
    pass


class AdeXLProtocolError(AdeXLRuntimeError):
    pass


class AdeXLResultError(AdeXLRuntimeError):
    pass


def normalize_adexl_workspace_binding(setup, binding=None, require_private_workspace=False):
    locator = dict((setup or {}).get("setup_locator") or {})
    source_path = locator.get("path")
    if not binding:
        if require_private_workspace:
            raise ValueError("ADE XL runtime requires an explicit workspace copy binding")
        return {
            "strict_private": False,
            "workspace_root": None,
            "simulation_root": None,
            "setup_path": source_path,
            "view_dir": locator.get("view_dir"),
            "library_name": locator.get("library_name"),
            "cell_name": locator.get("cell_name"),
            "view_name": locator.get("view_name"),
        }

    source = dict(binding)
    workspace_root = source.get("workspace_root") or source.get("root")
    simulation_root = source.get("simulation_root")
    setup_path = source.get("setup_path") or source.get("setup_sdb")
    library_name = source.get("library_name") or source.get("primary_library")
    cell_name = source.get("cell_name") or source.get("setup_cell")
    view_name = source.get("view_name") or source.get("setup_view")
    required = {
        "workspace_root": workspace_root,
        "simulation_root": simulation_root,
        "setup_path": setup_path,
        "library_name": library_name,
        "cell_name": cell_name,
        "view_name": view_name,
    }
    missing = sorted(name for name, value in required.items() if value in (None, ""))
    if missing:
        raise ValueError("ADE XL workspace copy binding is missing %s" % ", ".join(missing))

    workspace_root = os.path.abspath(os.path.expanduser(str(workspace_root)))
    simulation_root = os.path.abspath(os.path.expanduser(str(simulation_root)))
    setup_path = os.path.abspath(os.path.expanduser(str(setup_path)))
    if not os.path.isdir(workspace_root):
        raise ValueError("ADE XL workspace copy root does not exist: %s" % workspace_root)
    if not os.path.isfile(setup_path):
        raise ValueError("ADE XL working SetupDB does not exist: %s" % setup_path)
    if os.path.basename(setup_path).lower() != "data.sdb":
        raise ValueError("ADE XL working SetupDB must use the canonical data.sdb filename")
    if not is_under(setup_path, workspace_root):
        raise ValueError("ADE XL working SetupDB must be below workspace_root")
    if not is_under(simulation_root, workspace_root):
        raise ValueError("ADE XL projectDir must be below workspace_root")
    ensure_dir(simulation_root)

    view_dir = os.path.dirname(setup_path)
    declared_view_dir = source.get("view_dir") or source.get("setup_dir")
    if declared_view_dir and os.path.realpath(os.path.abspath(declared_view_dir)) != os.path.realpath(view_dir):
        raise ValueError("ADE XL working SetupDB view_dir does not match setup_path")
    return {
        "strict_private": True,
        "workspace_root": workspace_root,
        "simulation_root": simulation_root,
        "setup_path": setup_path,
        "view_dir": view_dir,
        "library_name": _protocol_value(library_name, "ADE XL logical library name"),
        "cell_name": _protocol_value(cell_name, "ADE XL setup cell name"),
        "view_name": _protocol_value(view_name, "ADE XL setup view name"),
    }


def _safe_token(value, default="item", maximum=48):
    token = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value or "")).strip("_.-") or default
    return token[:maximum]


def _skill_quote(value):
    value = str(value)
    if "\x00" in value or "\n" in value or "\r" in value:
        raise ValueError("SKILL string contains a forbidden control character")
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _protocol_value(value, label):
    value = str(value)
    if any(character in value for character in ("\x00", "\t", "\n", "\r")):
        raise ValueError("%s contains a forbidden protocol character" % label)
    return value


def _boolean(value):
    if isinstance(value, str):
        return value.strip().lower() not in ("", "0", "false", "no", "off", "nil")
    return bool(value)


def normalize_adexl_runtime_options(options=None):
    source = dict(options or {})
    execution_mode = str(source.get("execution_mode") or "local_serial").strip().lower().replace("-", "_")
    scheduler = str(source.get("scheduler") or "local").strip().lower()
    parallel_jobs = int(source.get("parallel_jobs", 1))
    lscs_enabled = _boolean(source.get("lscs_enabled", False))
    if lscs_enabled or scheduler in ("lscs", "distributed", "remote") or "lscs" in execution_mode:
        raise AdeXLUnsupportedMode("ADE XL LSCS/distributed execution is not supported")
    if execution_mode not in ("local", "serial", "local_serial"):
        raise AdeXLUnsupportedMode("ADE XL runtime supports local serial execution only")
    if parallel_jobs != 1:
        raise AdeXLUnsupportedMode("ADE XL runtime requires parallel_jobs=1")
    timeout_seconds = float(source.get("candidate_timeout_seconds", 600.0))
    poll_interval_seconds = int(source.get("poll_interval_seconds", 2))
    if timeout_seconds <= 0:
        raise ValueError("candidate_timeout_seconds must be positive")
    if poll_interval_seconds <= 0:
        raise ValueError("poll_interval_seconds must be a positive integer")
    return {
        "execution_mode": "local_serial",
        "scheduler": "local",
        "parallel_jobs": 1,
        "lscs_supported": False,
        "close_after_candidate": _boolean(source.get("close_after_candidate", False)),
        "candidate_timeout_seconds": timeout_seconds,
        "poll_interval_seconds": poll_interval_seconds,
    }


def _status_call(path, identity, state, message='""'):
    return _contract_text('crescAxlWriteStatus("%s" "%s" %d "%s" "%s" "%s" %s)') % (
        _skill_quote(path),
        _skill_quote(identity["epoch"]),
        int(identity["sequence"]),
        _skill_quote(identity["nonce"]),
        _skill_quote(identity["candidate_id"]),
        _skill_quote(state),
        message,
    )


def _status_procedure_lines():
    return [
        _contract_text("procedure(crescAxlSafeString(value)"),
        "  if(stringp(value) then value else sprintf(nil \"%L\" value))",
        ")",
        _contract_text("procedure(crescAxlFieldString(value)"),
        _contract_text("  if(value then crescAxlSafeString(value) else \"\"))"),
        _contract_text("procedure(crescAxlWriteStatus(path epoch sequence nonce candidate state message)"),
        "  let((port)",
        "    port = outfile(path \"a\")",
        "    when(port",
        _contract_text('      fprintf(port "%s\\t%L\\t%s\\t%s\\t%s\\t%s\\t%s\\n" epoch sequence nonce candidate state getCurrentTime() crescAxlSafeString(message))'),
        "      close(port)",
        "    )",
        "  )",
        ")",
    ]


def _safe_close_procedure_lines():
    return [
        _contract_text("procedure(crescAxlSafeClose()"),
        "  let((stopResult closeResult)",
        _contract_text("    when(crescAxlSession"),
        "      when(isCallable('axlStopAll)",
        _contract_text("        stopResult = errset(axlStopAll(crescAxlSession) t)"),
        "      )",
        "      when(isCallable('axlCloseSession)",
        _contract_text("        closeResult = errset(axlCloseSession(crescAxlSession) t)"),
        "      )",
        _contract_text("      crescAxlSession = nil"),
        "    )",
        "    t",
        "  )",
        ")",
    ]


def _reset_runtime_output(path):
    if os.path.lexists(path):
        if not os.path.isfile(path) or os.path.islink(path):
            raise AdeXLRuntimeError("refusing to replace unsafe ADE XL runtime output: %s" % path)
        os.unlink(path)


def render_adexl_capability_probe(output_path, exit_when_done=True):
    output_path = os.path.abspath(output_path)
    lines = [
        _contract_text('crescAxlCapabilityPort = outfile("%s" "w")') % _skill_quote(output_path),
        _contract_text('unless(crescAxlCapabilityPort error("Crescendo cannot open ADE XL capability report\\n"))'),
    ]
    for api_name in REQUIRED_AXL_APIS:
        lines.append(
            _contract_text('fprintf(crescAxlCapabilityPort "API\\t%s\\t%%L\\trequired\\n" isCallable(\'%s))')
            % (_skill_quote(api_name), api_name)
        )
    for api_name in OPTIONAL_AXL_APIS:
        lines.append(
            _contract_text('fprintf(crescAxlCapabilityPort "API\\t%s\\t%%L\\toptional\\n" isCallable(\'%s))')
            % (_skill_quote(api_name), api_name)
        )
    lines.append(_contract_text("close(crescAxlCapabilityPort)"))
    if exit_when_done:
        lines.append("exit()")
    return "\n".join(lines) + "\n"


def parse_adexl_capabilities(path):
    rows = []
    if not os.path.isfile(path):
        return rows
    with open(path, "r") as stream:
        for raw in stream:
            if not raw.endswith("\n"):
                continue
            fields = raw.rstrip("\n").split("\t")
            if len(fields) != 4 or fields[0] != "API":
                continue
            rows.append({
                "name": fields[1],
                "available": fields[2].strip().lower() in ("t", "true", "1"),
                "requirement": fields[3],
            })
    return rows


def read_adexl_protocol(path):
    rows = []
    if not os.path.isfile(path):
        return rows
    with open(path, "r") as stream:
        for raw in stream:
            if not raw.endswith("\n"):
                continue
            fields = raw.rstrip("\n").split("\t", 6)
            if len(fields) != 7:
                continue
            try:
                sequence = int(fields[1])
            except ValueError:
                continue
            rows.append({
                "epoch": fields[0],
                "sequence": sequence,
                "nonce": fields[2],
                "candidate_id": fields[3],
                "state": fields[4],
                "timestamp": fields[5],
                "message": fields[6],
            })
    return rows


def matching_adexl_protocol(path, identity):
    return [
        row for row in read_adexl_protocol(path)
        if row["epoch"] == identity["epoch"]
        and row["sequence"] == int(identity["sequence"])
        and row["nonce"] == identity["nonce"]
        and row["candidate_id"] == identity["candidate_id"]
    ]


def validate_adexl_protocol(path, identity, successful_states, error_state="ERROR"):
    rows = matching_adexl_protocol(path, identity)
    states = [row["state"] for row in rows]
    if not states:
        raise AdeXLProtocolError("ADE XL runtime returned no fenced protocol states")
    if states[-1] == successful_states[-1]:
        if states != list(successful_states):
            raise AdeXLProtocolError("invalid successful ADE XL protocol: %s" % ",".join(states))
        return rows
    if states[-1] == error_state:
        prefix = states[:-1]
        if not prefix or prefix != list(successful_states[:len(prefix)]):
            raise AdeXLProtocolError("invalid failed ADE XL protocol: %s" % ",".join(states))
        return rows
    raise AdeXLProtocolError("ADE XL protocol has no recognized terminal state: %s" % ",".join(states))


def read_adexl_result_manifest(path, identity):
    if not os.path.isfile(path):
        raise AdeXLResultError("ADE XL result manifest was not generated: %s" % path)
    rows = []
    with open(path, "r") as stream:
        for raw in stream:
            if not raw.endswith("\n"):
                continue
            fields = raw.rstrip("\n").split("\t")
            if len(fields) not in (10, 11) or fields[0] != "RESULT":
                continue
            try:
                sequence = int(fields[2])
            except ValueError:
                continue
            legacy = len(fields) == 10
            rows.append({
                "schema_version": RESULT_SCHEMA_VERSION,
                "epoch": fields[1],
                "sequence": sequence,
                "nonce": fields[3],
                "candidate_id": fields[4],
                "history_name": fields[5],
                "results_dir": fields[6] or None,
                "results_location": fields[7] or None,
                "rdb_path": None if legacy else (fields[8] or None),
                "run_id": fields[8 if legacy else 9] or None,
                "run_status": fields[9 if legacy else 10] or None,
                "legacy_manifest": legacy,
            })
    matching = [
        row for row in rows
        if row["epoch"] == identity["epoch"]
        and row["sequence"] == int(identity["sequence"])
        and row["nonce"] == identity["nonce"]
        and row["candidate_id"] == identity["candidate_id"]
    ]
    if len(matching) != 1:
        raise AdeXLResultError("ADE XL result manifest does not contain exactly one fenced result")
    return matching[0]


def _candidate_rdb_paths(candidate, result):
    history_name = result["history_name"]
    expected_name = history_name + ".rdb"
    direct_path = result.get("rdb_path")
    if direct_path:
        direct_path = os.path.abspath(os.path.expanduser(direct_path))
        if os.path.basename(direct_path) != expected_name:
            raise AdeXLResultError(
                "ADE XL direct RDB identity mismatch: expected=%s actual=%s"
                % (expected_name, os.path.basename(direct_path))
            )
        if not is_under(direct_path, candidate["setup_dir"]):
            raise AdeXLResultError("ADE XL direct RDB is outside the working setup view")
        if os.path.isfile(direct_path) and os.path.getsize(direct_path) > 0:
            return [os.path.realpath(direct_path)]
        return []

    candidates = []
    for raw in (result.get("results_dir"), result.get("results_location")):
        if not raw:
            continue
        path = os.path.abspath(os.path.expanduser(raw))
        if os.path.isfile(path) and os.path.basename(path) == expected_name:
            candidates.append(path)
        elif os.path.isdir(path):
            direct = os.path.join(path, expected_name)
            if os.path.isfile(direct):
                candidates.append(direct)
    setup_results = os.path.join(candidate["setup_dir"], "results")
    if os.path.isdir(setup_results):
        for current, directories, files in os.walk(setup_results):
            relative = os.path.relpath(current, setup_results)
            if relative != "." and relative.count(os.sep) >= 5:
                directories[:] = []
            if expected_name in files:
                candidates.append(os.path.join(current, expected_name))
    return sorted(set(os.path.realpath(path) for path in candidates if os.path.getsize(path) > 0))


def locate_adexl_point_result(result, point_id, test_name, require_complete=False):
    results_dir = result.get("results_dir")
    if not results_dir:
        raise AdeXLResultError("ADE XL result does not contain a point results directory")
    point_token = str(point_id)
    test_token = str(test_name)
    for label, token in (("point_id", point_token), ("test_name", test_token)):
        if token in ("", ".", "..") or os.sep in token or (os.altsep and os.altsep in token):
            raise AdeXLResultError("invalid ADE XL %s path segment: %r" % (label, token))
    results_dir = os.path.abspath(os.path.expanduser(results_dir))
    point_root = os.path.join(results_dir, point_token, test_token)
    psf_dir = os.path.join(point_root, "psf")
    paths = {
        "schema_version": _contract_text("crescendo.adexl_point_result.v1"),
        "backend": "adexl",
        "history_name": result.get("history_name"),
        "point_id": point_token,
        "test_name": test_token,
        "point_root": point_root,
        "netlist_path": os.path.join(point_root, "netlist", _contract_text("input.scs")),
        "psf_dir": psf_dir,
        "spectre_out_path": os.path.join(psf_dir, "spectre.out"),
        "sim_done_path": os.path.join(psf_dir, ".simDone"),
    }
    if not all(is_under(path, results_dir) for path in (
            paths["point_root"], paths["netlist_path"], paths["psf_dir"],
            paths["spectre_out_path"], paths["sim_done_path"])):
        raise AdeXLResultError("ADE XL point result escaped the history results directory")
    if require_complete:
        missing = []
        for name in ("netlist_path", "spectre_out_path"):
            path = paths[name]
            if not os.path.isfile(path) or os.path.getsize(path) <= 0:
                missing.append(name)
        if not os.path.isfile(paths["sim_done_path"]):
            missing.append("sim_done_path")
        if missing:
            raise AdeXLResultError(
                "ADE XL point result is incomplete for point=%s test=%s: %s"
                % (point_token, test_token, ", ".join(missing))
            )
    return paths


def validate_adexl_rdb_completion(parsed, expected_tests=None, expected_corners=None):
    expected_tests = sorted(set(str(value) for value in (expected_tests or []) if value))
    expected_corners = sorted(set(str(value) for value in (expected_corners or []) if value))
    points = dict((row.get("pointID"), row) for row in parsed.get("points") or [])
    statuses = list(parsed.get("test_statuses") or [])
    if not points:
        raise AdeXLResultError("ADE XL RDB contains no design points")
    if not statuses:
        raise AdeXLResultError("ADE XL RDB contains no per-test completion statuses")

    actual_corners = set(
        str(row.get("corner_name")) for row in points.values()
        if row.get("corner_name") not in (None, "")
    )
    missing_corners = sorted(set(expected_corners) - actual_corners)
    if missing_corners:
        raise AdeXLResultError(
            "ADE XL RDB is missing enabled corners: %s" % ", ".join(missing_corners)
        )
    actual_tests = set(str(row.get("test_name")) for row in statuses if row.get("test_name"))
    missing_tests = sorted(set(expected_tests) - actual_tests)
    if missing_tests:
        raise AdeXLResultError(
            "ADE XL RDB is missing enabled tests: %s" % ", ".join(missing_tests)
        )

    failed = []
    status_by_pair = {}
    for row in statuses:
        pair = (row.get("pointID"), str(row.get("test_name")))
        status_by_pair[pair] = row
        try:
            status_code = int(row.get("statusCode"))
        except (TypeError, ValueError):
            status_code = None
        if status_code != 3:
            failed.append({
                "point_id": row.get("pointID"),
                "test": row.get("test_name"),
                "status_code": row.get("statusCode"),
                "message": row.get("status_message"),
            })
    if failed:
        raise AdeXLResultError("ADE XL RDB contains failed or incomplete test points: %r" % failed)

    missing_pairs = []
    required_tests = expected_tests or sorted(actual_tests)
    for point_id, point in points.items():
        corner = point.get("corner_name")
        if expected_corners and str(corner) not in expected_corners:
            continue
        for test_name in required_tests:
            if (point_id, test_name) not in status_by_pair:
                missing_pairs.append((point_id, corner, test_name))
    if missing_pairs:
        raise AdeXLResultError("ADE XL RDB is missing per-point test statuses: %r" % missing_pairs)
    return {
        "ok": True,
        "point_count": len(points),
        "test_count": len(actual_tests),
        "corners": sorted(actual_corners),
        "tests": sorted(actual_tests),
    }


def finalize_adexl_candidate(candidate, require_rdb=True):
    identity = dict(candidate.get("protocol_identity") or {})
    rows = validate_adexl_protocol(candidate["marker_path"], identity, CANDIDATE_STATES)
    if rows[-1]["state"] == "ERROR":
        raise AdeXLRuntimeError("ADE XL candidate failed: %s" % rows[-1]["message"])
    result = read_adexl_result_manifest(candidate["result_manifest_path"], identity)
    if not result.get("history_name"):
        raise AdeXLResultError("ADE XL completed result has no history identity")
    submitted = [row for row in rows if row["state"] == "RUN_SUBMITTED"]
    if len(submitted) != 1:
        raise AdeXLResultError("ADE XL candidate does not contain exactly one submitted run identity")
    if result.get("run_id") is not None and submitted[0]["message"] != str(result["run_id"]):
        raise AdeXLResultError(
            "ADE XL completed run does not match submitted run identity: expected=%s actual=%s"
            % (submitted[0]["message"], result["run_id"])
        )
    simulation_root = candidate.get("simulation_root")
    if simulation_root:
        for name in ("results_dir", "results_location"):
            path = result.get(name)
            if path and not is_under(path, simulation_root):
                raise AdeXLResultError("ADE XL %s is outside working projectDir" % name)
    rdb_paths = _candidate_rdb_paths(candidate, result)
    if len(rdb_paths) > 1:
        raise AdeXLResultError("multiple ADE XL RDB files match completed history: %s" % rdb_paths)
    if require_rdb and not rdb_paths:
        raise AdeXLResultError(
            "completed ADE XL history has no non-empty RDB file: %s"
            % result["history_name"]
        )
    finalized = dict(candidate)
    finalized["history_name"] = result["history_name"]
    finalized["history_identity"] = dict(candidate.get("history_identity") or {})
    finalized["history_identity"]["history_name"] = result["history_name"]
    finalized["rdb_path"] = rdb_paths[0] if rdb_paths else None
    finalized["results_dir"] = result.get("results_dir")
    finalized["results_location"] = result.get("results_location")
    finalized["result_manifest_rdb_path"] = result.get("rdb_path")
    finalized["run_id"] = result.get("run_id")
    finalized["run_status"] = result.get("run_status")
    finalized["status"] = rows
    finalized["states"] = [row["state"] for row in rows]
    return finalized


class AdeXLRuntimeBuilder(object):
    """Build fenced SKILL operations for one persistent local ADE XL session."""

    def __init__(self, setup, runtime_root, worker_index=0, epoch=None, options=None,
                 workspace_binding=None, require_private_workspace=False):
        self.setup = dict(setup or {})
        if self.setup.get("backend") != "adexl":
            raise ValueError("ADE XL runtime requires an inspected adexl setup")
        source_locator = dict(self.setup.get("setup_locator") or {})
        self.workspace_binding = normalize_adexl_workspace_binding(
            self.setup, binding=workspace_binding,
            require_private_workspace=require_private_workspace,
        )
        self.locator = dict(source_locator)
        self.locator.update({
            "path": self.workspace_binding["setup_path"],
            "view_dir": self.workspace_binding["view_dir"],
            "view_name": self.workspace_binding["view_name"],
            "cell_name": self.workspace_binding["cell_name"],
            "library_name": self.workspace_binding["library_name"],
        })
        self.locator["cell_dir"] = os.path.dirname(self.locator["view_dir"])
        self.locator["library_root"] = os.path.dirname(self.locator["cell_dir"])
        for name in ("path", "view_dir", "view_name", "cell_name", "library_name"):
            if not self.locator.get(name):
                raise ValueError("ADE XL setup locator is missing %s" % name)
        if not os.path.isfile(self.locator["path"]):
            raise ValueError("ADE XL SetupDB does not exist: %s" % self.locator["path"])
        self.runtime_root = ensure_dir(runtime_root)
        if is_under(self.runtime_root, self.locator["view_dir"]):
            raise ValueError("ADE XL runtime root must not be inside the setup view")
        self.worker_index = int(worker_index)
        self.epoch = _protocol_value(epoch or uuid.uuid4().hex, "worker epoch")
        self.options = normalize_adexl_runtime_options(options)
        self.session_id = _safe_token(
            _contract_text("CRESCENDO_adexl_w%d_%s") % (self.worker_index, self.epoch[:12]),
            default=_contract_text("CRESCENDO_adexl"),
            maximum=63,
        )
        self.sequence = 0
        self._session = None
        self._shutdown_built = False

    def _identity(self, candidate_id):
        self.sequence += 1
        return {
            "epoch": self.epoch,
            "sequence": self.sequence,
            "nonce": uuid.uuid4().hex,
            "candidate_id": _protocol_value(candidate_id, "candidate_id"),
        }

    def _history_identity(self, candidate_id, candidate_root, sequence, requested=None):
        requested_name = _safe_token(
            requested or _contract_text("CRESCENDO_adexl_%s_%d") % (
                _safe_token(candidate_id, default="candidate", maximum=32),
                int(sequence),
            ),
            default=_contract_text("CRESCENDO_adexl_history"),
            maximum=63,
        )
        return {
            "schema_version": _contract_text("crescendo.history_identity.v1"),
            "backend": "adexl",
            "session_id": self.session_id,
            "history_name": None,
            "requested_history_name": requested_name,
            "naming_policy": "native_adexl_history_name",
            "binding_policy": "callback_run_id_and_exact_rdb",
        }

    def build_session(self):
        if self._session is not None:
            return dict(self._session)
        identity = {
            "epoch": self.epoch,
            "sequence": 0,
            "nonce": uuid.uuid4().hex,
            "candidate_id": "__startup__",
        }
        startup_marker = os.path.join(self.runtime_root, "adexl_startup.tsv")
        capabilities_path = os.path.join(self.runtime_root, "adexl_capabilities.tsv")
        startup_script = os.path.join(self.runtime_root, "adexl_startup.il")
        manifest_path = os.path.join(self.runtime_root, _contract_text("adexl_session.json"))
        _reset_runtime_output(startup_marker)
        _reset_runtime_output(capabilities_path)

        lines = _status_procedure_lines() + [
            _contract_text('crescAxlWorkerEpoch = "%s"') % _skill_quote(self.epoch),
            _contract_text('crescAxlSessionId = "%s"') % _skill_quote(self.session_id),
            _contract_text("crescAxlSession = nil"),
            _contract_text("crescAxlMainSetupDB = nil"),
            _contract_text("crescAxlSequence = 0"),
            _contract_text("crescAxlBusy = nil"),
            _contract_text("crescAxlTainted = nil"),
            _contract_text("crescAxlLastRunId = nil"),
            _contract_text("crescAxlLastHistoryName = nil"),
        ] + _safe_close_procedure_lines() + [
            _status_call(startup_marker, identity, "PROBE_BEGIN"),
            _contract_text("crescAxlStartupResult = errset(progn("),
            _contract_text('  crescAxlCapabilityPort = outfile("%s" "w")') % _skill_quote(capabilities_path),
            _contract_text('  unless(crescAxlCapabilityPort error("Crescendo cannot open ADE XL capability report\\n"))'),
            _contract_text("  crescAxlMissingRequired = 0"),
        ]
        for api_name in REQUIRED_AXL_APIS:
            lines.extend([
                _contract_text("  crescAxlApiAvailable = isCallable('%s)") % api_name,
                _contract_text('  fprintf(crescAxlCapabilityPort "API\\t%s\\t%%L\\trequired\\n" crescAxlApiAvailable)') % _skill_quote(api_name),
                _contract_text("  unless(crescAxlApiAvailable crescAxlMissingRequired = crescAxlMissingRequired + 1)"),
            ])
        for api_name in OPTIONAL_AXL_APIS:
            lines.extend([
                _contract_text("  crescAxlApiAvailable = isCallable('%s)") % api_name,
                _contract_text('  fprintf(crescAxlCapabilityPort "API\\t%s\\t%%L\\toptional\\n" crescAxlApiAvailable)') % _skill_quote(api_name),
            ])
        lines.extend([
            _contract_text("  close(crescAxlCapabilityPort)"),
            _contract_text('  unless(crescAxlMissingRequired == 0 error("Crescendo required ADE XL public APIs are unavailable\\n"))'),
            "  " + _status_call(startup_marker, identity, "PROBE_OK"),
        ])
        if self.workspace_binding.get("simulation_root"):
            lines.append(
                '  envSetVal("asimenv.startup" "projectDir" \'string "%s")'
                % _skill_quote(self.workspace_binding["simulation_root"])
            )
        lines.extend([
            _contract_text('  crescAxlSession = axlCreateSession("%s")') % _skill_quote(self.session_id),
            _contract_text('  unless(crescAxlSession error("Crescendo cannot create ADE XL session\\n"))'),
            _contract_text('  crescAxlBindResult = errset(axlSetMainSetupDBLCV(crescAxlSession "%s" "%s" "%s" ?mode "a") t)') % (
                _skill_quote(self.locator["library_name"]),
                _skill_quote(self.locator["cell_name"]),
                _skill_quote(self.locator["view_name"]),
            ),
            _contract_text('  unless(crescAxlBindResult && car(crescAxlBindResult) error("Crescendo cannot bind ADE XL SetupDB LCV\\n"))'),
            _contract_text("  crescAxlMainSetupDB = car(crescAxlBindResult)"),
            _contract_text('  unless(axlSDBHp(crescAxlMainSetupDB) error("Crescendo cannot obtain a valid ADE XL SetupDB handle\\n"))'),
            _contract_text("  crescAxlResolvedSetupDB = axlGetMainSetupDB(crescAxlSession)"),
            _contract_text('  unless(equal(crescAxlResolvedSetupDB crescAxlMainSetupDB) error("Crescendo ADE XL main SetupDB identity mismatch\\n"))'),
            "  " + _status_call(startup_marker, identity, "SESSION_READY", _contract_text('crescAxlSessionId')),
            "  t",
            ") t)",
            _contract_text("unless(crescAxlStartupResult"),
            _contract_text("  crescAxlTainted = t"),
            _contract_text("  crescAxlSafeClose()"),
            "  " + _status_call(startup_marker, identity, "ERROR", '"startup failed"'),
            ")",
        ])
        atomic_write_text(startup_script, "\n".join(lines) + "\n")
        session = {
            "schema_version": SESSION_SCHEMA_VERSION,
            "backend": "adexl",
            "session_id": self.session_id,
            "worker_epoch": self.epoch,
            "worker_index": self.worker_index,
            "runtime_root": self.runtime_root,
            "setup_dir": self.locator["view_dir"],
            "setup_locator": dict(self.locator),
            "workspace_binding": dict(self.workspace_binding),
            "simulation_root": self.workspace_binding.get("simulation_root"),
            "execution": dict(self.options),
            "startup_marker_path": startup_marker,
            "capabilities_path": capabilities_path,
            "startup_script_path": startup_script,
            "manifest_path": manifest_path,
            "protocol_identity": dict(identity),
        }
        atomic_write_json(manifest_path, session)
        self._session = dict(session)
        return dict(session)

    def build_candidate(self, candidate_id, candidate_root, values=None, history_name=None,
                        pre_run_skill_lines=None, timeout_seconds=None):
        if self._shutdown_built:
            raise AdeXLRuntimeError("cannot build a candidate after ADE XL shutdown")
        self.build_session()
        candidate_root = ensure_dir(candidate_root)
        if not is_under(candidate_root, self.runtime_root):
            raise ValueError("ADE XL candidate root must be below runtime_root")
        identity = self._identity(str(candidate_id))
        history = self._history_identity(
            candidate_id, candidate_root, identity["sequence"], requested=history_name,
        )
        timeout = float(
            timeout_seconds if timeout_seconds is not None
            else self.options["candidate_timeout_seconds"]
        )
        if timeout <= 0:
            raise ValueError("candidate timeout_seconds must be positive")
        poll_seconds = self.options["poll_interval_seconds"]
        poll_count = max(1, int(math.ceil(timeout / float(poll_seconds))))
        callback_token = re.sub(
            r"[^A-Za-z0-9_]", "_",
            "%s_%d_%s" % (self.epoch, identity["sequence"], identity["nonce"][:12]),
        )
        callback_name = _contract_text("crescAxlCandidateDone_%s") % callback_token
        callback_flag = _contract_text("crescAxlCandidateDoneFlag_%s") % callback_token
        callback_result = _contract_text("crescAxlCandidateDoneResult_%s") % callback_token
        marker_path = os.path.join(candidate_root, "adexl_status.tsv")
        progress_path = os.path.join(candidate_root, "adexl_progress.tsv")
        script_path = os.path.join(candidate_root, "adexl_candidate.il")
        result_manifest_path = os.path.join(candidate_root, "adexl_result.tsv")
        artifact_path = os.path.join(candidate_root, _contract_text("adexl_candidate.json"))
        _reset_runtime_output(marker_path)
        _reset_runtime_output(progress_path)
        _reset_runtime_output(result_manifest_path)
        pre_run_skill_lines = list(pre_run_skill_lines or [])
        for line in pre_run_skill_lines:
            if "\x00" in str(line):
                raise ValueError("pre_run_skill_lines contains NUL")

        status = lambda state, message='""': _status_call(marker_path, identity, state, message)
        lines = [
            "%s = nil" % callback_flag,
            "procedure(%s()" % callback_name,
            "  unless(%s" % callback_flag,
            "    %s = t" % callback_flag,
            "    " + _status_call(progress_path, identity, "CALLBACK_BEGIN", '"run finished"'),
            "    %s = errset(progn(" % callback_result,
            _contract_text('      unless(equal(crescAxlWorkerEpoch "%s") error("Crescendo ADE XL callback epoch mismatch\\n"))') % _skill_quote(self.epoch),
            _contract_text('      unless(equal(crescAxlSessionId "%s") error("Crescendo ADE XL callback session mismatch\\n"))') % _skill_quote(self.session_id),
            _contract_text('      unless(crescAxlSession error("Crescendo ADE XL callback session is closed\\n"))'),
            _contract_text('      unless(crescAxlBusy error("Crescendo ADE XL callback has no active candidate\\n"))'),
            "      when(isCallable('axlGetAllFinishedRuns)",
            _contract_text("        crescAxlFinishedRuns = axlGetAllFinishedRuns(crescAxlSession)"),
            _contract_text('        unless(member(crescAxlRunId crescAxlFinishedRuns) error("Crescendo ADE XL callback run identity mismatch\\n"))'),
            "      )",
            "      " + status("SIM_DONE"),
            _contract_text("      crescAxlRunData = axlGetRunData(crescAxlSession crescAxlRunId)"),
            _contract_text('      unless(crescAxlRunData error("Crescendo cannot obtain ADE XL run data\\n"))'),
            _contract_text("      crescAxlActualHistoryName = axlGetHistoryName(crescAxlRunData)"),
            _contract_text('      unless(stringp(crescAxlActualHistoryName) error("Crescendo cannot obtain ADE XL history identity\\n"))'),
            _contract_text('      when(crescAxlActualHistoryName == "" error("Crescendo obtained an empty ADE XL history identity\\n"))'),
            _contract_text("      crescAxlHistoryEntry = axlGetHistoryEntry(crescAxlMainSetupDB crescAxlActualHistoryName)"),
            _contract_text('      unless(crescAxlHistoryEntry error("Crescendo cannot obtain ADE XL history entry\\n"))'),
            _contract_text("      crescAxlRDBPath = axlGetHistoryResults(crescAxlRunData)"),
            _contract_text('      unless(stringp(crescAxlRDBPath) error("Crescendo cannot obtain ADE XL history RDB path\\n"))'),
            _contract_text("      crescAxlResDBResult = errset(axlReadHistoryResDB(crescAxlActualHistoryName ?session crescAxlSession) t)"),
            _contract_text('      unless(crescAxlResDBResult && car(crescAxlResDBResult) error("Crescendo cannot read ADE XL history result DB\\n"))'),
            _contract_text("      crescAxlResultsDir = axlGetHistoryResultsDir(crescAxlRunData)"),
            _contract_text("      crescAxlEntryResultsDir = axlGetHistoryResultsDir(crescAxlHistoryEntry)"),
            _contract_text('      unless(equal(crescAxlResultsDir crescAxlEntryResultsDir) error("Crescendo ADE XL history results directory mismatch\\n"))'),
            _contract_text("      crescAxlResultsLocation = if(isCallable('axlGetResultsLocation) then axlGetResultsLocation(crescAxlHistoryEntry) else nil)"),
            _contract_text("      crescAxlRunStatus = if(isCallable('axlGetRunStatus) then axlGetRunStatus(crescAxlSession ?optionName \"all\" ?historyName crescAxlActualHistoryName) else nil)"),
            _contract_text('      crescAxlResultPort = outfile("%s" "w")') % _skill_quote(result_manifest_path),
            _contract_text('      unless(crescAxlResultPort error("Crescendo cannot open ADE XL result manifest\\n"))'),
            _contract_text('      fprintf(crescAxlResultPort "RESULT\\t%s\\t%d\\t%s\\t%s\\t%%s\\t%%s\\t%%s\\t%%s\\t%%s\\t%%s\\n"') % (
                _skill_quote(identity["epoch"]), identity["sequence"],
                _skill_quote(identity["nonce"]), _skill_quote(identity["candidate_id"]),
            ),
            _contract_text("        crescAxlFieldString(crescAxlActualHistoryName) crescAxlFieldString(crescAxlResultsDir)"),
            _contract_text("        crescAxlFieldString(crescAxlResultsLocation) crescAxlFieldString(crescAxlRDBPath)"),
            _contract_text("        crescAxlFieldString(crescAxlRunId) crescAxlFieldString(crescAxlRunStatus))"),
            _contract_text("      close(crescAxlResultPort)"),
            "      " + status("RESULTS_READY", _contract_text("crescAxlActualHistoryName")),
            _contract_text("      crescAxlLastRunId = crescAxlRunId"),
            _contract_text("      crescAxlLastHistoryName = crescAxlActualHistoryName"),
            _contract_text("      crescAxlBusy = nil"),
            "      " + status("DONE", _contract_text("crescAxlActualHistoryName")),
        ]
        if self.options["close_after_candidate"]:
            lines.extend([
                _contract_text("      crescAxlSafeClose()"),
                "      exit()",
            ])
        lines.extend([
            "      t",
            "    ) t)",
            "    unless(%s" % callback_result,
            _contract_text("      crescAxlBusy = nil"),
            _contract_text("      crescAxlTainted = t"),
            "      " + status("ERROR", '"candidate callback failed"'),
            _contract_text("      crescAxlSafeClose()"),
        ])
        if self.options["close_after_candidate"]:
            lines.append("      exit()")
        lines.extend([
            "    )",
            "  )",
            ")",
            status("ACCEPTED"),
            _contract_text("crescAxlCandidateResult = errset(progn("),
            _contract_text('  unless(equal(crescAxlWorkerEpoch "%s") error("Crescendo ADE XL worker epoch mismatch\\n"))') % _skill_quote(self.epoch),
            _contract_text('  unless(equal(crescAxlSessionId "%s") error("Crescendo ADE XL session identity mismatch\\n"))') % _skill_quote(self.session_id),
            _contract_text('  unless(crescAxlSession error("Crescendo ADE XL session is closed\\n"))'),
            _contract_text('  when(crescAxlTainted error("Crescendo ADE XL session is tainted\\n"))'),
            _contract_text('  when(crescAxlBusy error("Crescendo ADE XL session only supports serial candidates\\n"))'),
            _contract_text('  unless(crescAxlSequence == %d error("Crescendo ADE XL candidate sequence mismatch\\n"))') % (identity["sequence"] - 1),
            _contract_text("  crescAxlBusy = t"),
            _contract_text("  crescAxlSequence = %d") % identity["sequence"],
            "  " + status("BEGIN"),
        ])
        lines.extend("  " + str(line) for line in pre_run_skill_lines)
        lines.extend([
            _contract_text('  crescAxlRunId = axlRunSimulation(?session crescAxlSession ?callback "%s()")') % callback_name,
            _contract_text('  unless(crescAxlRunId error("Crescendo ADE XL run submission failed\\n"))'),
            "  " + status("RUN_SUBMITTED", _contract_text("crescAxlSafeString(crescAxlRunId)")),
            "  " + _status_call(progress_path, identity, "ASYNC_WAIT", '"callback pending"'),
            "  t",
            ") t)",
            _contract_text("unless(crescAxlCandidateResult"),
            _contract_text("  crescAxlBusy = nil"),
            _contract_text("  crescAxlTainted = t"),
            "  " + status("ERROR", '"candidate submission failed"'),
            _contract_text("  crescAxlSafeClose()"),
        ])
        if self.options["close_after_candidate"]:
            lines.append("  exit()")
        lines.extend([
            ")",
        ])
        atomic_write_text(script_path, "\n".join(lines) + "\n")
        artifact = {
            "schema_version": CANDIDATE_SCHEMA_VERSION,
            "backend": "adexl",
            "candidate_id": str(candidate_id),
            "candidate_root": candidate_root,
            "values": dict(values or {}),
            "history_name": history["history_name"],
            "requested_history_name": history["requested_history_name"],
            "history_identity": history,
            "rdb_path": None,
            "results_dir": None,
            "setup_dir": self.locator["view_dir"],
            "simulation_root": self.workspace_binding.get("simulation_root"),
            "workspace_binding": dict(self.workspace_binding),
            "marker_path": marker_path,
            "progress_path": progress_path,
            "script_path": script_path,
            "result_manifest_path": result_manifest_path,
            "artifact_path": artifact_path,
            "protocol_identity": dict(identity),
            "worker_epoch": self.epoch,
            "worker_index": self.worker_index,
            "session_id": self.session_id,
            "candidate_timeout_seconds": timeout,
            "poll_interval_seconds": poll_seconds,
            "poll_count": poll_count,
            "completion_mode": "callback",
            "close_after_candidate": self.options["close_after_candidate"],
            "callback_name": callback_name,
            "pre_run_skill_line_count": len(pre_run_skill_lines),
        }
        atomic_write_json(artifact_path, artifact)
        return artifact

    def build_shutdown(self):
        if self._shutdown_built:
            raise AdeXLRuntimeError("ADE XL shutdown was already built")
        self.build_session()
        identity = self._identity("__shutdown__")
        marker_path = os.path.join(self.runtime_root, "adexl_shutdown.tsv")
        script_path = os.path.join(self.runtime_root, "adexl_shutdown.il")
        _reset_runtime_output(marker_path)
        lines = [
            _status_call(marker_path, identity, "CLOSE_BEGIN"),
            _contract_text("crescAxlCloseResult = errset(progn("),
            _contract_text('  unless(equal(crescAxlWorkerEpoch "%s") error("Crescendo ADE XL shutdown epoch mismatch\\n"))') % _skill_quote(self.epoch),
            _contract_text('  unless(equal(crescAxlSessionId "%s") error("Crescendo ADE XL shutdown session mismatch\\n"))') % _skill_quote(self.session_id),
            _contract_text('  when(crescAxlBusy error("Crescendo cannot close a busy ADE XL session\\n"))'),
            _contract_text("  crescAxlSafeClose()"),
            "  " + _status_call(marker_path, identity, "CLOSE_DONE"),
            "  t",
            ") t)",
            _contract_text("unless(crescAxlCloseResult"),
            _contract_text("  crescAxlTainted = t"),
            _contract_text("  crescAxlSafeClose()"),
            "  " + _status_call(marker_path, identity, "ERROR", '"shutdown failed"'),
            ")",
        ]
        atomic_write_text(script_path, "\n".join(lines) + "\n")
        self._shutdown_built = True
        return {
            "schema_version": PROTOCOL_SCHEMA_VERSION,
            "backend": "adexl",
            "operation": "shutdown",
            "session_id": self.session_id,
            "worker_epoch": self.epoch,
            "worker_index": self.worker_index,
            "marker_path": marker_path,
            "script_path": script_path,
            "protocol_identity": dict(identity),
        }
