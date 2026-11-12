"""Scheduler key-value protocol shared by submission observers and owned queries.

These parsers identify explicit records, never authorize a scheduler command.
In particular a missing job, a cancellation acknowledgement or an unrelated
integer cannot prove remote termination or simulator completion.
"""
from __future__ import print_function
from crescendo_engine.runtime import bind_text
_contract_text = bind_text()


import json
import re

MAX_PROTOCOL_TEXT = 256 * 1024
TERMINAL_STATES = frozenset(("CANCELLED", "CANCELED", "DONE", "FINISHED",
    "FAILED", "EXIT", "TERMINATED", "SUCCEEDED", "SUCCESS"))
ACTIVE_STATES = frozenset(("RUNNING", "PENDING", "QUEUED", "SUBMITTED",
    "WAITING", "SUSPENDED", "SCHEDULED", "HELD", "STARTING", "COMPLETING",
    "FINISHING", "KILLING", "STOPPING", "CANCELLING", "CANCELING", "READY"))
_NOT_FOUND = re.compile(r"\b(?:job\s+(?:[0-9]+\s+)?(?:not\s+found|does\s+not\s+exist)|"
    r"no\s+(?:matching\s+)?jobs?(?:\s+(?:found|exist|records?))?|"
    r"(?:not\s+found|does\s+not\s+exist).{0,40}\bjob)\b", re.I)
_ERROR = re.compile(r"(?im)^\s*(?:ERROR\b|FAILED\b|permission\s+denied\b)")


class _Pairs(list):
    """Retain JSON duplicates instead of silently accepting last-value wins."""


def valid_job_id(value):
    return (not isinstance(value, bool) and isinstance(value, (str, int))
            and re.fullmatch(r"[0-9]{1,32}", str(value)) is not None)


def _name(value):
    return re.sub(r"[_\s-]", "", str(value)).lower()


def _document(text):
    try:
        return json.loads(text, object_pairs_hook=_Pairs), False
    except (ValueError, TypeError):
        return None, text.lstrip().startswith(("{", "["))


def parse_submit_observation(text):
    result = {"schema_version": _contract_text("crescendo.scheduler_submission_observation.v1"),
        "job_id": None, "observed_job_ids": [], "accepted": False,
        "rejected": False, "ambiguous": False, "invalid": False,
        "format": "unknown", "message": None}
    if not isinstance(text, str) or len(text) > MAX_PROTOCOL_TEXT:
        result["invalid"] = True
        return result
    document, malformed = _document(text)
    found = set()
    if _ERROR.search(text):
        result["invalid"] = True

    def identifier(value):
        if valid_job_id(value):
            found.add(str(value))
        else:
            result["invalid"] = True

    if document is not None:
        result["format"] = "json"
        def inspect(value, depth=0):
            if depth > 32:
                result["invalid"] = True
                return
            if isinstance(value, _Pairs):
                seen = set()
                for key, child in value:
                    if key in seen:
                        result["invalid"] = True
                    seen.add(key)
                    if _name(key) == "jobid":
                        identifier(child)
                    elif _name(key) in ("error", "errors", "errormessage", "errmsg") and child not in (None, False, "", 0, [], {}):
                        result["invalid"] = True
                    inspect(child, depth + 1)
            elif isinstance(value, list):
                for child in value:
                    inspect(child, depth + 1)
        inspect(document)
    elif malformed:
        result["format"] = "json"
        result["invalid"] = True
    else:
        header = False
        for raw in text.splitlines(keepends=True):
            words = raw.strip().split()
            if len(words) == 2 and [_name(word) for word in words] == ["jobid", "message"]:
                header = True
                result["format"] = "table"
                continue
            if not header or not words:
                continue
            if words[0] == "-":
                result["rejected"] = True
                result["message"] = " ".join(words[1:])[:2048]
            elif valid_job_id(words[0]):
                # Do not publish a numeric token split across stdout chunks.
                if len(words) > 1 or raw.endswith(("\n", "\r")):
                    identifier(words[0])
                    result["message"] = " ".join(words[1:])[:2048]
            elif re.match(r"^[0-9]", words[0]):
                result["invalid"] = True
            else:
                # A CPU/memory header or another message ends this table.
                header = False
        patterns = (r"\bJob\s*<([0-9]+)>\s+(?:is\s+)?submitted\b",
            r"\bSubmitted\s+(?:batch\s+)?job\s+([0-9]+)(?![0-9A-Za-z_.-])",
            r"\bJob\s+([0-9]+)\s+(?:is\s+)?submitted\b",
            r"\bjob(?:Id|_id|\s+id)\s*[:=]\s*[\"']?([0-9]+)(?![0-9A-Za-z_.-])")
        for pattern in patterns:
            for match in re.findall(pattern, text, re.I):
                identifier(match)
                if result["format"] == "unknown":
                    result["format"] = "explicit_submission"
    result["observed_job_ids"] = sorted(found)
    result["ambiguous"] = len(found) > 1
    if len(found) == 1 and not any(result[key] for key in ("invalid", "rejected", "ambiguous")):
        result["job_id"] = next(iter(found))
        result["accepted"] = True
    return result


def parse_submit_job_id(text):
    return parse_submit_observation(text)["job_id"]


def parse_submit_streams(stdout, stderr):
    """Join completed streams without corrupting JSON or completing partial IDs."""
    streams = [parse_submit_observation(value or '') for value in (stdout, stderr)]
    result = {"schema_version": _contract_text("crescendo.scheduler_submission_observation.v1"), "format": "streams",
        "job_id": None, "observed_job_ids": sorted(set(identifier for stream in streams
            for identifier in stream["observed_job_ids"])), "accepted": False,
        "invalid": any(stream["invalid"] for stream in streams),
        "rejected": any(stream["rejected"] for stream in streams), "streams": streams}
    result["ambiguous"] = len(result["observed_job_ids"]) > 1
    if len(result["observed_job_ids"]) == 1 and not any(result[key] for key in ("invalid", "rejected", "ambiguous")):
        result["accepted"] = True
        result["job_id"] = result["observed_job_ids"][0]
    return result


def parse_scheduler_observation(text, expected_id):
    expected = str(expected_id)
    result = {"schema_version": _contract_text("crescendo.scheduler_observation.v1"), "expected_job_id": expected,
        "job_id": None, "observed_job_ids": [], "state": None, "terminal": False,
        "not_found": False, "invalid": False, "format": "unknown", "reason": None,
        "task_states": {}, "job_exit_code": None, "metadata": {}}
    if not valid_job_id(expected_id) or not isinstance(text, str) or len(text) > MAX_PROTOCOL_TEXT:
        result.update(invalid=True, reason="invalid_id_or_output_limit")
        return result
    identities, job_states, task_states, exit_codes = set(), set(), {}, set()
    metadata_values = {}
    result["not_found"] = bool(_NOT_FOUND.search(text))
    if _ERROR.search(text):
        result["invalid"] = True

    def collect(key, value, task_scope=None):
        name = _name(key)
        if name == "jobid":
            if valid_job_id(value):
                identities.add(str(value))
            else:
                result["invalid"] = True
        elif name in ("state", "jobstate", "status", "jobstatus", "taskstate", "taskstatus"):
            if not isinstance(value, str) or not value.strip():
                result["invalid"] = True
                return
            state = value.strip().upper()
            if state not in TERMINAL_STATES | ACTIVE_STATES:
                result["invalid"] = True
            if task_scope is not None or name.startswith("task"):
                task_states.setdefault(task_scope or "unspecified_task", set()).add(state)
            else:
                job_states.add(state)
        elif name.endswith(("state", "status")):
            result["invalid"] = True
        elif name in ("exitcode", "jobexitcode", "returncode") and task_scope is None:
            if value in (None, "", "-", "N/A"):
                return
            if isinstance(value, bool) or not isinstance(value, (int, str)) or not re.fullmatch(r"-?[0-9]{1,10}", str(value)):
                result["invalid"] = True
            else:
                exit_codes.add(int(value))
        elif name in ("error", "errors", "errormessage", "errmsg") and value not in (None, False, "", 0, [], {}):
            result["invalid"] = True
        elif name in ("execnode", "executionnode", "submitnode", "jobname", "queue", "account",
                      "reqcpu", "reqmem", "jobcreatetime", "exitmessage"):
            metadata_key = {"execnode": "exec_node", "executionnode": "exec_node",
                "submitnode": "submit_node", "jobname": "job_name", "reqcpu": "req_cpu",
                "reqmem": "req_mem", "jobcreatetime": "job_create_time", "exitmessage": "exit_message"}.get(name, name)
            if value not in (None, ""):
                metadata_values.setdefault(metadata_key, set()).add(str(value).strip()[:2048])

    document, malformed = _document(text)
    if document is not None:
        result["format"] = "json"
        def walk(value, path=(), task_scope=None, depth=0):
            if depth > 32:
                result["invalid"] = True
                return
            if isinstance(value, _Pairs):
                seen = set()
                for key, child in value:
                    if key in seen or not isinstance(key, str):
                        result["invalid"] = True
                    seen.add(key)
                    collect(key, child, task_scope)
                    scope = task_scope
                    if re.match(r"^tasks?(?:$|[\[A-Z_])", key, re.I):
                        scope = "/".join(path + (key,))
                    walk(child, path + (key,), scope, depth + 1)
            elif isinstance(value, list):
                for index, child in enumerate(value):
                    child_path = path + (str(index),)
                    scope = "/".join(child_path) if task_scope is not None else None
                    walk(child, child_path, scope, depth + 1)
        walk(document)
    elif malformed:
        result.update(format="json", invalid=True)
    else:
        header = None
        rows = []
        for line in text.splitlines():
            words = line.strip().split()
            names = [_name(word) for word in words]
            if "jobid" in names and "jobstate" in names:
                if names.count("jobid") != 1 or names.count("jobstate") != 1:
                    result["invalid"] = True
                header = names
                result["format"] = "table"
                continue
            if header is not None and words and valid_job_id(words[0]):
                if header[0] != "jobid" or len(words) <= header.index("jobstate"):
                    result["invalid"] = True
                    continue
                collect("jobid", words[0])
                collect("jobstate", words[header.index("jobstate")])
                for index, name in enumerate(header):
                    if index >= len(words) or name in ("jobid", "jobstate"):
                        continue
                    if name.startswith("task") and name.endswith(("state", "status")):
                        collect(name, words[index], "table_task_%d" % len(rows))
                    else:
                        collect(name, words[index])
                rows.append({"job_id": words[0], "columns": words[:32]})
                continue
            if header is not None and words and re.match(r"^[0-9]", words[0]):
                result["invalid"] = True
            # Restrict the field token before splitting: a date/time colon is
            # value data, and the scheduler key-value protocol uses whitespace.
            match = re.match(r"\s*([A-Za-z_][A-Za-z0-9_.\[\] -]*?)\s*[:=]\s*(.*?)\s*$", line)
            if match is None:
                match = re.match(r"\s*([A-Za-z_][A-Za-z0-9_.\[\]-]*)\s+(.*?)\s*$", line)
            if match:
                key, value = match.groups()
                parts = key.strip().split(".")
                scope = ".".join(parts[:-1]) if any(re.match(r"^TASK(?:\[|$)", part, re.I) for part in parts[:-1]) else None
                collect(parts[-1], value, scope)
                if result["format"] == "unknown":
                    result["format"] = "key_value"
        if header is not None:
            result["metadata"] = {"headers": header, "rows": rows[:32]}
            if not rows:
                result["not_found"] = True
    for key, values in metadata_values.items():
        result["metadata"][key] = next(iter(values)) if len(values) == 1 else sorted(values)
    result["observed_job_ids"] = sorted(identities)
    result["task_states"] = {key: sorted(values) for key, values in task_states.items()}
    if result["not_found"]:
        if identities or job_states:
            result["invalid"] = True
        result["format"] = "not_found" if result["format"] == "unknown" else result["format"]
        result["reason"] = "job_not_found_is_not_stop_proof"
        return result
    if identities != {expected} or len(job_states) != 1 or len(exit_codes) > 1:
        result["invalid"] = True
    if any(len(states) != 1 for states in task_states.values()):
        result["invalid"] = True
    if identities == {expected} and len(job_states) == 1:
        result["job_id"] = expected
        result["state"] = next(iter(job_states))
        if exit_codes:
            result["job_exit_code"] = next(iter(exit_codes)) if len(exit_codes) == 1 else None
        if result["state"] in TERMINAL_STATES and any(not states <= TERMINAL_STATES for states in task_states.values()):
            result["invalid"] = True
    result["terminal"] = bool(not result["invalid"] and result["state"] in TERMINAL_STATES
        and all(states <= TERMINAL_STATES for states in task_states.values()))
    result["reason"] = "invalid_or_mixed_records" if result["invalid"] else (
        "exact_job_terminal" if result["terminal"] else "exact_job_nonterminal")
    return result
