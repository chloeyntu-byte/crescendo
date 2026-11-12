"""Read Maestro submission policy and override it only in an owned setup.

Commands are opaque Cadence policy values. This module never runs a command,
guesses scheduler flags, or changes the invoking Maestro's saved setup.
"""
from __future__ import print_function

import copy
import hashlib
import json
import math
import os
import re
import tempfile
import shlex
import sys

from crescendo_engine.runtime import bind_text
_contract_text = bind_text()

from .utils import atomic_write_text, ensure_dir

SCHEMA = _contract_text("crescendo.maestro_job_policy.v1")
RECORD_SCHEMA = _contract_text("CRESCENDO_JOB_POLICY_1")
MAX_VALUE_CHARS = 16384
JOB_TIMEOUT_PROPERTIES = {
    "start_seconds": "starttimeout", "configure_seconds": "configuretimeout",
    "run_seconds": "runtimeout", "linger_seconds": "lingertimeout",
}


def timeout_override_properties(overrides):
    """Native private-policy fields; never infer a differently named property."""
    timeouts = normalize_job_overrides(overrides).get("timeouts") or {}
    return dict((JOB_TIMEOUT_PROPERTIES[key], str(value)) for key, value in timeouts.items())


def timeout_override_skill_arguments(overrides):
    """Append to one private maeSetJobPolicy call; pairs require readback."""
    return "".join(" ?%s %s" % (key, json.dumps(value))
                   for key, value in sorted(timeout_override_properties(overrides).items()))


def timeout_override_skill_readback(overrides, policy_expr="policy"):
    """Strict equality for the same private policy returned after setting it."""
    return " && ".join("equal(getq(%s %s) %s)" % (policy_expr, key, json.dumps(value))
                       for key, value in sorted(timeout_override_properties(overrides).items())) or "t"


def job_timeout_policy(properties, overrides=None, fallbacks=None, scope=None):
    """Read seconds and disabled values without inventing zero's semantics.

    The source ICRP Run limit covers preparation/evaluation as well as solve.
    The common CLI layer declares its own phase mapping separately.
    """
    properties, fallbacks = properties or {}, fallbacks or {}
    timeouts = normalize_job_overrides(overrides).get("timeouts") or {}
    result = {"unit": "seconds", "fields": {}, "unqualified_properties": {}}
    for field, native in JOB_TIMEOUT_PROPERTIES.items():
        if field in timeouts:
            raw, source = timeouts[field], "manual_override"
        elif native in properties:
            raw, source = properties[native], "maestro_job_setup"
        elif field in fallbacks:
            raw, source = fallbacks[field], _contract_text("cr_runtime_fallback")
        else:
            raw, source = None, "unavailable"
        seconds, status = None, "unknown"
        if source != "unavailable" and (raw is None or (isinstance(raw, str) and not raw.strip())):
            status = "disabled"
        elif not isinstance(raw, bool):
            try:
                numeric = float(raw)
                if math.isfinite(numeric):
                    if numeric == -1:
                        status = "disabled"
                    elif numeric > 0:
                        seconds, status = numeric, "enabled"
            except (ValueError, TypeError):
                pass
        result["fields"][field] = {"source_property": native, "raw_value": raw,
            "source": source, "seconds": seconds, "enabled": status == "enabled",
            "status": status, "scope": scope}
    if "configuredtimeout" in properties:
        result["unqualified_properties"]["configuredtimeout"] = properties["configuredtimeout"]
    return result
# Public maeSetJobPolicy keywords, IC6.1.8. Named policies retain additional
# native properties. An unnamed policy with unknown fields cannot be cloned.
POLICY_PROPERTIES = (
    "blockemail", "configuretimeout", "configuredtimeout", "distributionmethod",
    "lingertimeout", "interfacename", "jobhostname", "jobqueue", "joboutfilename",
    "joberrfilename", "jobjobname", "loginshell", "jobresourcerequirements", "jobprojectname",
    "jobsubmitcommand", "jobusergroup", "maxjobs", "parallelnumprocs",
    "olvjobresourcerequirements", "olvjobprojectname", "olvjobusergroup",
    "olvpriority", "olvJobGroup", "preemptivestart", "reconfigureimmediately", "runtimeout", "sgehardresource",
    "sgesoftresource", "sgepriority", "sgeparallelenv", "showerrorwhenretrying",
    "showoutputlogerror", "startmaxjobsimmed", "starttimeout", "usesameprocess",
)


def normalize_job_overrides(value):
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError("Maestro job overrides must be an object")
    unknown = sorted(set(value) - {"command", "max_jobs", "timeouts"})
    if unknown:
        raise ValueError("Unsupported Maestro job override fields: %s" % ", ".join(unknown))
    result = {}
    if "command" in value:
        command = value["command"]
        if not isinstance(command, str) or len(command) > MAX_VALUE_CHARS:
            raise ValueError("Maestro command must be a string of at most %s characters" % MAX_VALUE_CHARS)
        if any(ord(char) < 32 for char in command):
            raise ValueError("Maestro command cannot contain control characters")
        result["command"] = command
    if "max_jobs" in value:
        count = value["max_jobs"]
        if isinstance(count, bool) or not isinstance(count, int) or count < 1:
            raise ValueError("Maestro max jobs must be a positive integer")
        result["max_jobs"] = count
    if "timeouts" in value:
        timeouts = value["timeouts"]
        if not isinstance(timeouts, dict) or set(timeouts) - set(JOB_TIMEOUT_PROPERTIES):
            raise ValueError("Maestro timeouts must use the supported seconds fields")
        normalized = {}
        for field, seconds in timeouts.items():
            if seconds is None or (isinstance(seconds, str) and not seconds.strip()):
                seconds = -1
            if (isinstance(seconds, bool) or not isinstance(seconds, int)
                    or (seconds != -1 and seconds <= 0)):
                raise ValueError("%s must be positive integer seconds or blank for no limit; 0 is invalid" % field)
            normalized[field] = seconds
        result["timeouts"] = normalized
    return result


def unavailable_job_policy(source="unknown", message=None):
    return {"schema_version": SCHEMA, "source": source, "available": False,
            "global": {"available": False, "properties": {}}, "tests": [],
            "run_mode": None, "run_options": {"available": False}, "warnings": [message] if message else []}


def job_policy_fingerprint(policy, overrides=None):
    original = copy.deepcopy(policy or unavailable_job_policy())
    original.pop("probe_root", None)
    original.pop("save_options", None)
    payload = {"original": original,
               "overrides": normalize_job_overrides(overrides)}
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def policy_from_records(rows, source="invoking_session"):
    """Decode bounded native records; missing values stay unknown, never Local/1."""
    result = unavailable_job_policy(source)
    by_test, seen, equal_global = {}, set(), set()
    storage_rows = []
    for tag, fields in rows:
        if tag == "V":
            storage_rows.append((tag, fields))
            continue
        if tag == "U":
            if len(fields) != 3 or not re.match(r"^(RunIn|DivideJobs|JobLimit(?::[A-Za-z][A-Za-z0-9_]{0,63})?)$", str(fields[0])):
                raise ValueError("Invalid Maestro run distribution record")
            key, available, value = fields
            if ("run_options", key) in seen or available not in (None, True):
                raise ValueError("Duplicate or invalid Maestro run distribution record")
            if value is not None and not isinstance(value, (str, int, bool)):
                raise ValueError("Invalid Maestro run distribution value")
            if key == "RunIn" and available and value not in ("Parallel", "Series"):
                raise ValueError("Invalid Maestro RunIn value")
            if key.startswith("JobLimit:"):
                if available:
                    result["run_options"].setdefault("JobLimit", []).append([key.split(":", 1)[1], value])
            else:
                result["run_options"][key] = value if available else None
            result["run_options"]["available"] = bool(available) if key == "RunIn" else result["run_options"]["available"]
            seen.add(("run_options", key))
            continue
        if tag == "Q":
            if len(fields) != 1 or "run_mode" in seen:
                raise ValueError("Duplicate or invalid Maestro run mode record")
            value = fields[0]
            if value is not None and not isinstance(value, str):
                raise ValueError("Invalid Maestro run mode")
            result["run_mode"] = value
            seen.add("run_mode")
            continue
        if tag not in ("P", "J", "D", "O") or len(fields) not in {"P": (3,), "J": (4,), "D": (2,), "O": (3, 4)}[tag]:
            raise ValueError("Invalid Maestro job policy record")
        scope, name = fields[:2]
        if scope not in ("global", "test") or (scope == "global" and name is not None):
            raise ValueError("Invalid Maestro job policy scope")
        if scope == "test":
            if not isinstance(name, str) or not name or len(name) > MAX_VALUE_CHARS or any(ord(c) < 32 for c in name):
                raise ValueError("Invalid Maestro policy test name")
            if name not in by_test:
                if len(by_test) >= 4096:
                    raise ValueError("Too many Maestro test policies")
                by_test[name] = {"name": name, "available": False, "properties": {}}
            target = by_test[name]
        else:
            target = result["global"]
        if tag == "D":
            if scope != "test" or name in equal_global or target["properties"] or target.get("opaque_properties"):
                raise ValueError("Invalid duplicate effective Maestro policy reference")
            equal_global.add(name)
        elif tag == "P":
            key = (scope, name, "__available__")
            if key in seen or fields[2] not in (None, True):
                raise ValueError("Duplicate or invalid Maestro policy availability")
            target["available"] = fields[2] is True
            seen.add(key)
        elif tag == "O":
            if scope == "test" and name in equal_global:
                raise ValueError("Conflicting effective Maestro policy records")
            key = fields[2]
            kind = fields[3] if len(fields) == 4 else "non_scalar"
            identity = (scope, name, key)
            if (not isinstance(key, str) or not re.match(r"^[A-Za-z][A-Za-z0-9_]*$", key) or identity in seen
                    or kind not in ("list", "symbol", "native_object", "non_scalar")):
                raise ValueError("Duplicate or invalid opaque Maestro policy property")
            target.setdefault("opaque_properties", {})[key] = kind
            seen.add(identity)
        else:
            if scope == "test" and name in equal_global:
                raise ValueError("Conflicting effective Maestro policy records")
            key, value = fields[2:]
            identity = (scope, name, key)
            if not isinstance(key, str) or not re.match(r"^[A-Za-z][A-Za-z0-9_]*$", key) or identity in seen:
                raise ValueError("Duplicate or invalid Maestro policy property")
            if value is not None and not isinstance(value, (str, int, bool)):
                raise ValueError("Unsupported Maestro policy property value")
            if isinstance(value, str) and (len(value) > MAX_VALUE_CHARS or any(ord(c) < 32 for c in value)):
                raise ValueError("Invalid Maestro policy property value")
            target["properties"][key] = value
            seen.add(identity)
    for name in equal_global:
        if not result["global"]["available"] or not by_test[name]["available"]:
            raise ValueError("Effective Maestro policy reference has no available global evidence")
        by_test[name]["properties"] = copy.deepcopy(result["global"]["properties"])
        if result["global"].get("opaque_properties"):
            by_test[name]["opaque_properties"] = copy.deepcopy(result["global"]["opaque_properties"])
    result["tests"] = list(by_test.values())
    for scope, row in [("global", result["global"])] + [("test " + name, row) for name, row in by_test.items()]:
        opaque = row.get("opaque_properties") or {}
        if opaque:
            result["warnings"].append(_contract_text("Maestro Job Setup %s has native properties not interpreted by Crescendo CLI: %s. No CLI translation is applied.") %
                (scope, ", ".join("%s (%s)" % item for item in sorted(opaque.items()))))
    result["available"] = bool(result["global"]["available"])
    if not result["available"]:
        result["warnings"].append("Original Maestro job policy was not available; submission settings remain unknown.")
    if storage_rows:
        from .storage_policy import save_options_from_records
        result["save_options"] = save_options_from_records(storage_rows, source=source)
    return result


def parse_policy_records(raw, source="saved_setup_native"):
    # Import lazily: launch-context parsing also uses policy_from_records.
    from .maestro_context import MAX_CONTEXT_BYTES, _fields
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    if len(raw.encode("utf-8")) > MAX_CONTEXT_BYTES:
        raise ValueError("Maestro job policy records exceed size limit")
    lines = raw.splitlines()
    if not lines or lines[0] != RECORD_SCHEMA:
        raise ValueError("Unsupported Maestro job policy record schema")
    rows = []
    for line in lines[1:]:
        if line.strip():
            tag, separator, fields = line.partition("\t")
            if not separator:
                raise ValueError("Invalid Maestro job policy record")
            rows.append((tag, _fields(fields)))
    return policy_from_records(rows, source=source)


def job_policy_capture_definitions():
    return _contract_text(r'''
procedure(crescWriteOneJobPolicy(port sess scope testName @optional identicalPolicy)
  let((result policy cursor key value)
    result = nil
    when(isCallable('maeGetJobPolicy)
      result = errset(if(testName then maeGetJobPolicy(?session sess ?testName testName)
                                 else maeGetJobPolicy(?session sess)) nil)
    )
    policy = if(result then car(result) else nil)
    fprintf(port "P\t%L\t%L\t%L\n" scope testName if(policy then t else nil))
    if(policy && identicalPolicy && equal(policy identicalPolicy) then
      ; This is equality of actual returned values, not an inheritance claim.
      fprintf(port "D\t%L\t%L\n" scope testName)
    else when(policy
      cursor = cdr(policy)
      while(cursor && cdr(cursor)
        key = car(cursor)
        value = cadr(cursor)
        when(symbolp(key)
          if(stringp(value) || numberp(value) || value == nil || value == t then
            fprintf(port "J\t%L\t%L\t%L\t%L\n" scope testName symbolToString(key)
              if(numberp(value) then sprintf(nil "%L" value) else value))
          else
            fprintf(port "O\t%L\t%L\t%L\t%L\n" scope testName symbolToString(key)
              if(listp(value) then "list" else if(symbolp(value) then "symbol" else "native_object")))
          )
        )
        cursor = cddr(cursor)
      )
    ))
    policy
  )
)
procedure(crescWriteJobPolicyRecords(port sess testNames)
  let((result globalPolicy value item)
    globalPolicy = crescWriteOneJobPolicy(port sess "global" nil)
    foreach(name testNames crescWriteOneJobPolicy(port sess "test" name globalPolicy))
    result = nil
    when(isCallable('maeGetCurrentRunMode)
      result = errset(maeGetCurrentRunMode(?session sess) nil)
    )
    fprintf(port "Q\t%L\n" if(result && stringp(car(result)) then car(result) else nil))
    result = nil
    when(isCallable('axlGetRunDistributeOptions)
      result = errset(axlGetRunDistributeOptions(axlGetActiveSetup(axlGetMainSetupDB(sess))) nil)
    )
    foreach(key '(RunIn DivideJobs)
      value = if(result && car(result) then get(car(result) key) else nil)
      when(symbolp(value) && value != nil && value != t value = symbolToString(value))
      fprintf(port "U\t%L\t%L\t%L\n" symbolToString(key)
        if(result && car(result) then t else nil) value)
    )
    when(result && car(result)
      foreach(item car(result)->JobLimit
        fprintf(port "U\t%L\tt\t%L\n" strcat("JobLimit:" car(item)) cadr(item))
      )
    )
  )
)
''')



def _bounded_test_list(values, variable):
    """Keep large source/batch test tables out of a single SKILL argument list."""
    values = list(values or [])
    expression = "list(%s)" % " ".join(json.dumps(name, ensure_ascii=False) for name in values)
    if len(values) <= 128 and len(expression.encode("utf-8")) <= 32768:
        return [], expression
    lines = [variable + " = nil"]
    chunk, size = [], 0
    for name in values:
        literal = json.dumps(name, ensure_ascii=False)
        if chunk and (len(chunk) >= 128 or size + len(literal.encode("utf-8")) > 32768):
            lines.append("foreach(name list(%s) %s = cons(name %s))" % (" ".join(chunk), variable, variable))
            chunk, size = [], 0
        chunk.append(literal)
        size += len(literal.encode("utf-8")) + 1
    if chunk:
        lines.append("foreach(name list(%s) %s = cons(name %s))" % (" ".join(chunk), variable, variable))
    lines.append(variable + " = reverse(" + variable + ")")
    return lines, variable


def job_policy_capture_skill(library, cell, view, output_path, tests):
    strings = lambda value: json.dumps(value, ensure_ascii=False)
    test_lines, test_list = _bounded_test_list(tests, _contract_text("crescCapturePolicyTests"))
    from .storage_policy import native_storage_capture_definitions
    return job_policy_capture_definitions() + native_storage_capture_definitions() + "\n".join(test_lines) + "\n" + _contract_text('''
let((sess port result)
  result = errset(progn(
    sess = maeOpenSetup(%s %s %s ?mode "r")
    unless(sess error("Crescendo: read-only Maestro policy setup could not open.\\n"))
    port = outfile(%s)
    unless(port error("Crescendo: policy records could not be created.\\n"))
    fprintf(port "CRESCENDO_JOB_POLICY_1\\n")
    crescWriteJobPolicyRecords(port sess %s)
    crescWriteSaveOptionsRecords(port axlGetMainSetupDB(sess))
    close(port) port = nil
    t
  ) t)
  when(port close(port))
  when(sess maeCloseSession(?session sess ?forceClose t))
  unless(result && car(result) exit(1))
)
exit(0)
''') % (strings(library), strings(cell), strings(view), strings(output_path), test_list)


def _qualify_saved_job_policy(policy, references):
    """Reject a saved named policy silently replaced by a query default.

    Policy names are references, not enough information to recreate commands.
    Capture from an invoking session remains authoritative and bypasses this
    saved-setup-only check. Opaque native policy contents are never invented.
    """
    references = references or {}
    differences = []
    actual_global = (policy.get("global") or {}).get("properties") or {}
    expected_name = references.get("global")
    if expected_name and actual_global.get("name") != expected_name:
        differences.append({"scope": "global", "test": None,
                            "expected": expected_name, "actual": actual_global.get("name")})
    actual_tests = dict((row.get("name"), row) for row in policy.get("tests") or [])
    for row in references.get("tests") or []:
        expected_name = row.get("policy_name")
        actual_name = ((actual_tests.get(row.get("test")) or {}).get("properties") or {}).get("name")
        if expected_name and actual_name != expected_name:
            differences.append({"scope": "test", "test": row.get("test"),
                                "expected": expected_name, "actual": actual_name})
    if not differences:
        return policy
    result = unavailable_job_policy(policy.get("source") or "saved_setup_native",
        _contract_text("Saved JobPolicy could not be resolved in the read-only query; open Crescendo from this Maestro or override Command."))
    result["name_reference_validation"] = {
        "ok": False, "saved_references": copy.deepcopy(references),
        "differences": differences, "observed_policy": copy.deepcopy(policy),
        "environment": "isolated_readonly_query", "local_default_accepted": False,
    }
    if policy.get("save_options"):
        result["save_options"] = copy.deepcopy(policy["save_options"])
    return result


def capture_setup_job_policy(setup, config):
    """Prefer the invoking session; query another saved setup in read-only mode."""
    if setup.get("baseline"):
        from .setup_baseline import verify_setup_baseline
        verify_setup_baseline(setup)
        if "baseline_job_policy" in setup:
            return copy.deepcopy(setup["baseline_job_policy"])
    reference = setup.get("setup_locator") or {}
    source = setup.get("source") or {}
    path = reference.get("path") or source.get("setup_db")
    context = config.get("_maestro_launch_context") or {}
    bound_path = (context.get("source") or {}).get("setup_path")
    snapshot_checksum = (context.get("source") or {}).get("setup_content_sha256")
    snapshot_matches = True
    if snapshot_checksum and path:
        from .utils import sha256_file
        try:
            snapshot_matches = sha256_file(path) == snapshot_checksum
        except OSError:
            snapshot_matches = False
    snapshot_source = context.get("source") or {}
    if snapshot_matches and "active_state_sha256" in snapshot_source and path:
        from .utils import sha256_file
        state_path = os.path.join(os.path.dirname(path), "active.state")
        try:
            state_checksum = sha256_file(state_path) if os.path.isfile(state_path) else None
            snapshot_matches = state_checksum == snapshot_source["active_state_sha256"]
        except OSError:
            snapshot_matches = False
    if snapshot_matches and bound_path and path and os.path.realpath(bound_path) == os.path.realpath(path) and (context.get("job_policy") or context.get("save_options")):
        result = copy.deepcopy(context.get("job_policy") or unavailable_job_policy("invoking_session"))
        if context.get("save_options"):
            setup["save_options"] = copy.deepcopy(context["save_options"])
            result["save_options"] = copy.deepcopy(context["save_options"])
        return result
    if setup.get("backend") != "maestro":
        return unavailable_job_policy("unsupported_backend", "Job policy overrides require a Maestro setup.")
    from .cadence import CommandCancelled, child_environment, run_command, write_readonly_cds_context
    from .workspace import parse_cds_defines
    cadence = config.get("cadence") or {}
    if not cadence.get("ocean_bin"):
        return unavailable_job_policy("saved_setup_native", "Ocean is unavailable; original job policy could not be read.")
    runtime = config.setdefault("runtime", {})
    probe_root = runtime.get("hierarchy_probe_root") or runtime.get("run_root")
    if not probe_root:
        from .config import default_run_root
        probe_root = default_run_root()
    try:
        parent = ensure_dir(os.path.join(probe_root, "job_policy"))
        root = tempfile.mkdtemp(prefix="job_policy_", dir=parent)
        from .scratch import register_scratch
        scratch_ownership = register_scratch(root, kind="job_policy_probe")
        runtime.setdefault("scratch_ownership", []).append(scratch_ownership)
        library_root = source.get("library_root") or reference.get("library_root")
        override = cadence.get("source_cds_lib_override")
        mappings = parse_cds_defines(override or cadence.get("cds_lib")) if (override or cadence.get("cds_lib")) else {}
        names = sorted(name for name, value in mappings.items()
                       if library_root and os.path.realpath(value) == os.path.realpath(library_root))
        library = names[0] if names else reference.get("library_name")
        if not library or not library_root:
            raise ValueError("Saved setup has no authoritative library mapping")
        write_readonly_cds_context(root, library, library_root, cadence.get("cds_lib"), source_cds_lib_override=override)
        output = os.path.join(root, "job_policy.records")
        script = os.path.join(root, "read_job_policy.il")
        atomic_write_text(script, job_policy_capture_skill(
            library, reference.get("cell_name"), reference.get("view_name"), output,
            [row["name"] for row in setup.get("tests") or [] if row.get("name")],
        ))
        command = run_command([cadence["ocean_bin"], "-nograph", "-restore", script], cwd=root,
                              env=child_environment(config, ensure_dir(os.path.join(root, "home"))),
                              timeout_seconds=min(float(runtime.get("command_timeout_seconds", 1800)), 90),
                              stdout_path=os.path.join(root, _contract_text("stdout.log")), stderr_path=os.path.join(root, _contract_text("stderr.log")))
        if command.get("returncode") != 0 or not os.path.isfile(output):
            raise RuntimeError("Read-only Maestro policy query failed (see %s)" % root)
        with open(output, "rb") as stream:
            result = parse_policy_records(stream.read(1024 * 1024 + 1))
        if result.get("save_options"):
            # A separate saved setup query captures its native effective values,
            # not the invoking window's settings. Include exact LCV for public
            # path normalization; never derive it from an old history.
            from .storage_policy import save_options_from_records
            from .maestro_context import _fields
            rows = []
            with open(output, encoding="utf-8") as stream:
                for line in stream:
                    if line.startswith("V\t"):
                        rows.append(("V", _fields(line.partition("\t")[2].strip())))
            identity = dict(library=library, cell=reference.get("cell_name"), view=reference.get("view_name"),
                            view_dir=reference.get("view_dir"), work_dir=(config.get("cadence") or {}).get("project_dir"))
            result["save_options"] = save_options_from_records(rows, source="saved_setup_native_environment", identity=identity)
            result["save_options"]["warnings"].append(_contract_text("Saved setup query uses its native environment defaults; open Crescendo from that Maestro to capture its current Save Options, or override Data Root."))
            # The isolated query HOME/workdir must never become the user's
            # persistent default. Preserve observed paths for diagnosis only.
            observed = result["save_options"]["effective"]
            observed["query_simulation_root"] = observed.pop("simulation_root", None)
            observed.pop("simulation_root_evidence", None)
            result["save_options"]["available"] = False
            setup["save_options"] = copy.deepcopy(result["save_options"])
        if not command.get("termination_failed") and command.get("returncode") == 0:
            from .scratch import cleanup_scratch
            cleanup = cleanup_scratch(root, token=scratch_ownership.get("token"), producers_stopped=True)
            if not cleanup.get("ok"):
                result.setdefault("warnings", []).append("Read-only query temporary files retained: %s" % root)
        return _qualify_saved_job_policy(result, setup.get("job_policy_references"))
    except CommandCancelled:
        raise
    except Exception as error:
        return unavailable_job_policy("saved_setup_native", "Original Maestro job policy could not be read: %s" % error)


def job_policy_override_definitions(overrides=None):
    """Generate public-API clone/apply/readback helpers for an owned session.

    The caller guarantees the session uses a private setup. Copying an effective
    per-test policy avoids assuming that newly cloned tests inherit it. Batch
    callers must qualify source policies before cloning: dedicated test policies
    can start separate ICRPs even when their policy names are the same.
    """
    overrides = normalize_job_overrides(overrides)
    known = " ".join(POLICY_PROPERTIES)
    copy_lines = []
    for key in POLICY_PROPERTIES:
        copy_lines.append("    when(memq('%s cdr(original)) args = append(args list(?%s getq(original %s))))" % (key, key, key))
    override_lines = []
    if "command" in overrides:
        value = json.dumps(overrides["command"], ensure_ascii=False)
        override_lines.extend(["    original->jobsubmitcommand = %s" % value])
        if overrides["command"]:
            override_lines.append('    original->distributionmethod = "Command"')
        else:
            override_lines.append(_contract_text('    when(original->distributionmethod == "Command" error("Crescendo: Command distribution requires a nonempty submission command.\\n"))'))
    if "max_jobs" in overrides:
        override_lines.extend(["    original->maxjobs = %s" % json.dumps(str(overrides["max_jobs"]))])
    for key, value in sorted(timeout_override_properties(overrides).items()):
        override_lines.append("    original->%s = %s" % (key, json.dumps(value)))
    # Always define the copier even when there are no user overrides.
    return _contract_text('''
procedure(crescJobPolicyKeys(policy)
  let((cursor keys valid)
    valid = policy && listp(policy) && cdr(policy)
    when(valid
      cursor = cdr(policy)
      while(cursor && valid
        if(listp(cursor) && cdr(cursor) && listp(cdr(cursor)) &&
           symbolp(car(cursor)) && !memq(car(cursor) keys) then
          keys = cons(car(cursor) keys)
          cursor = cddr(cursor)
        else valid = nil)
      )
    )
    if(valid then cons(t keys) else nil)
  )
)
procedure(crescJobPolicyPropertiesEqual(left right @optional ignoreName)
  let((leftKeys rightKeys valid)
    leftKeys = crescJobPolicyKeys(left)
    rightKeys = crescJobPolicyKeys(right)
    when(leftKeys && rightKeys
      valid = t
      foreach(key cdr(leftKeys)
        unless(ignoreName && key == 'name
          unless(memq(key cdr(rightKeys)) && equal(get(left key) get(right key)) valid = nil)
        )
      )
      foreach(key cdr(rightKeys)
        unless(ignoreName && key == 'name
          unless(memq(key cdr(leftKeys)) valid = nil)
        )
      )
    )
    valid
  )
)
procedure(crescJobPolicyHasJobBudget(policy)
  let((keys maxJobs method)
    keys = crescJobPolicyKeys(policy)
    when(keys && memq('maxjobs cdr(keys)) && memq('distributionmethod cdr(keys))
      maxJobs = getq(policy maxjobs)
      method = getq(policy distributionmethod)
      when(stringp(maxJobs)
        maxJobs = if(rexMatchp("^[0-9][0-9]*$" maxJobs) then atoi(maxJobs) else nil))
      stringp(method) && method != "" && numberp(maxJobs) && maxJobs > 0
    )
  )
)
procedure(crescBatchSourceUsesGlobalPolicy(sess sourceTest)
  let((globalResult sourceResult)
    ; Only actual effective settings count. Missing/error evidence never means
    ; that a test inherits global; explicit overrides must already be applied.
    when(isCallable('maeGetJobPolicy)
      globalResult = errset(maeGetJobPolicy(?session sess) nil)
      sourceResult = errset(maeGetJobPolicy(?session sess ?testName sourceTest) nil)
    )
    globalResult && sourceResult && crescJobPolicyHasJobBudget(car(globalResult)) &&
      crescJobPolicyPropertiesEqual(car(globalResult) car(sourceResult) t)
  )
)
procedure(crescRequireJobPolicyMatch(original readback)
  let((cursor key oldValue newValue)
    unless(readback error("Crescendo: effective job policy readback unavailable.\\n"))
    cursor = cdr(original)
    while(cursor && cdr(cursor)
      key = car(cursor) oldValue = cadr(cursor) newValue = get(readback key)
      unless(equal(oldValue newValue)
        error("Crescendo: private job policy readback mismatch for %%L (%%L / %%L).\\n" key oldValue newValue)
      )
      cursor = cddr(cursor)
    )
    readback
  )
)
procedure(crescCloneJobPolicy(sess original targetTest name)
  let((args accepted readback cursor key oldValue newValue)
    unless(original error("Crescendo: original effective job policy unavailable.\\n"))
    ; Use the already registered policy in THIS private session. Updating the
    ; same name preserves native fields beyond the public setter's keywords.
    if(stringp(original->name) && original->name != "" then
      name = original->name
    else
      cursor = cdr(original)
      while(cursor && cdr(cursor)
        unless(memq(car(cursor) '(%s))
          error("Crescendo: unnamed job policy property %%L cannot be preserved by this API; no simulation submitted.\\n" car(cursor))
        )
        cursor = cddr(cursor)
      )
    )
    args = list(name ?session sess)
    when(targetTest args = append(args list(?tests list(targetTest))))
%s
    accepted = apply('maeSetJobPolicy args)
    unless(accepted error("Crescendo: private job policy update was rejected.\\n"))
    readback = if(targetTest then maeGetJobPolicy(?session sess ?testName targetTest)
                            else maeGetJobPolicy(?session sess))
    ; Public generic property setter covers native fields not listed in
    ; maeSetJobPolicy keywords (e.g. CPU/memory estimator settings). Only
    ; differing captured fields are set; exact readback remains mandatory.
    cursor = cdr(original)
    while(cursor && cdr(cursor)
      key = car(cursor) oldValue = cadr(cursor)
      when(key != 'name && !memq(key '(%s)) && !equal(oldValue get(readback key))
        unless(isCallable('axlSetJobPolicyProperty)
          error("Crescendo: captured job property %%L cannot be applied; no simulation submitted.\\n" key))
        unless(axlSetJobPolicyProperty(name symbolToString(key) oldValue)
          error("Crescendo: captured job property %%L was rejected; no simulation submitted.\\n" key))
      )
      cursor = cddr(cursor)
    )
    readback = if(targetTest then maeGetJobPolicy(?session sess ?testName targetTest)
                            else maeGetJobPolicy(?session sess))
    crescRequireJobPolicyMatch(original readback)
  )
)
procedure(crescCopyEffectiveJobPolicy(sess sourceTest targetTest)
  let((original globalPolicy readback accepted)
    original = maeGetJobPolicy(?session sess ?testName sourceTest)
    globalPolicy = copy(maeGetJobPolicy(?session sess))
    unless(crescJobPolicyKeys(original) && crescJobPolicyKeys(globalPolicy)
      error("Crescendo: original effective job policy unavailable or malformed; no simulation submitted.\\n"))
    if(crescJobPolicyPropertiesEqual(original globalPolicy t) then
      ; Source copies can retain an explicit binding. Equal values/names alone
      ; do not prove a shared ICRP; detach only the owned alias using public API.
      unless(isCallable('axlDetachJobPolicy)
        error("Crescendo: shared batch job policy detach API unavailable; no simulation submitted.\\n"))
      ; Detach returns nil when there was no explicit binding. First bind the
      ; existing private global policy and verify it, so the subsequent detach
      ; has an unambiguous success result; no simulation has started here.
      crescCloneJobPolicy(sess copy(globalPolicy) targetTest "CRESCENDO_shared_global")
      accepted = axlDetachJobPolicy(sess "ICRP" targetTest)
      unless(accepted
        error("Crescendo: shared batch job policy detach was rejected; no simulation submitted.\\n"))
      readback = maeGetJobPolicy(?session sess ?testName targetTest)
      unless(crescJobPolicyPropertiesEqual(globalPolicy readback t)
        error("Crescendo: shared batch job policy readback unavailable or differs; no simulation submitted.\\n"))
      readback
    else
      crescCloneJobPolicy(sess copy(original) targetTest strcat("CRESCENDO_copy_" targetTest))
    )
  )
)
procedure(crescApplyJobOverrides(sess @optional testNames)
  let((original globalPolicy allPolicies testsResult)
%s
  )
)
''') % (known, "\n".join(copy_lines), known, (
        _contract_text('''    unless(isCallable('maeGetJobPolicy) && isCallable('maeSetJobPolicy)
      error("Crescendo: public job policy APIs are unavailable; explicit override cannot be applied.\\n"))
    globalPolicy = copy(maeGetJobPolicy(?session sess))
    unless(globalPolicy error("Crescendo: original global job policy unavailable.\\n"))
    unless(testNames
      testsResult = maeGetSetup(?typeName "tests" ?session sess)
      testNames = testsResult
    )
    ; Capture every effective test policy BEFORE changing the global one.
    allPolicies = nil
    foreach(name testNames
      original = maeGetJobPolicy(?session sess ?testName name)
      unless(original original = globalPolicy)
      allPolicies = append(allPolicies list(list(name copy(original))))
    )
    original = copy(globalPolicy)
%s
    crescCloneJobPolicy(sess original nil "CRESCENDO_override_global")
    foreach(item allPolicies
      original = cadr(item)
      ; Updating a shared global policy must not add test-specific bindings.
      testsResult = equal(original globalPolicy)
%s
      if(testsResult then
        crescRequireJobPolicyMatch(original maeGetJobPolicy(?session sess ?testName car(item)))
      else
        crescCloneJobPolicy(sess original car(item) strcat("CRESCENDO_override_" car(item)))
      )
    )
    t''') % ("\n".join(override_lines), "\n".join(override_lines))
        if overrides else "    t ; No explicit override: leave every original submission setting unchanged."
    ))


def job_policy_override_skill(overrides=None, session_expr="sess", tests=None):
    if not re.match(r"^[A-Za-z][A-Za-z0-9_]*$", session_expr):
        raise ValueError("Invalid SKILL session expression")
    test_expr = "" if tests is None else " list(%s)" % " ".join(json.dumps(name, ensure_ascii=False) for name in tests)
    return job_policy_override_definitions(overrides) + _contract_text("\ncrescApplyJobOverrides(%s%s)\n") % (session_expr, test_expr)


def effective_job_policy(policy, overrides=None):
    """Expected native settings from this invocation, never a historical run."""
    result = copy.deepcopy(policy or unavailable_job_policy())
    overrides = normalize_job_overrides(overrides)
    name_validation = result.get("name_reference_validation") or {}
    if (result.get("available") is not True and name_validation.get("ok") is False
            and not str(overrides.get("command") or "").strip()):
        differences = name_validation.get("differences") or []
        detail = "; ".join("%s%s expected %r, actual %r" % (
            row.get("scope") or "policy", "/" + str(row["test"]) if row.get("test") else "",
            row.get("expected"), row.get("actual")) for row in differences)
        raise ValueError(_contract_text("Saved JobPolicy name mismatch (%s); open Crescendo from the source Maestro or provide an explicit Command override. Local fallback is blocked.") % detail)
    for scope in [result.get("global") or {}] + list(result.get("tests") or []):
        props = scope.setdefault("properties", {})
        if "command" in overrides:
            props["jobsubmitcommand"] = overrides["command"]
            if overrides["command"]:
                props["distributionmethod"] = "Command"
            elif str(props.get("distributionmethod") or "").lower() == "command":
                raise ValueError("Command distribution requires a nonempty submission command")
        if "max_jobs" in overrides:
            props["maxjobs"] = str(overrides["max_jobs"])
        props.update(timeout_override_properties(overrides))
        if str(props.get("distributionmethod") or "").lower() == "command" and not str(props.get("jobsubmitcommand") or "").strip():
            raise ValueError("Captured Command distribution has no submission command; no simulation submitted")
    result["overrides"] = overrides
    return result


def job_input_defaults(policy, tests=None):
    """Values for the editable fields; no dependency on a named .jp file."""
    policy = policy or {}
    props = dict((policy.get("global") or {}).get("properties") or {})
    rows = [row for row in policy.get("tests") or []
            if tests is None or row.get("name") in tests]
    if len(rows) == 1:
        props.update(rows[0].get("properties") or {})
    command = props.get("jobsubmitcommand")
    command = command if isinstance(command, str) else ""
    try:
        count = int(str(props.get("maxjobs", "1")))
        if count < 1: count = 1
    except (TypeError, ValueError):
        count = 1
    fields = job_timeout_policy(props)["fields"]
    timeouts = {key: int(value["seconds"]) if value["status"] == "enabled" else -1
                for key, value in fields.items()}
    return {"command": command, "max_jobs": count, "timeouts": timeouts}


def command_job_policy(policy, overrides=None, tests=None):
    """Build a private Command policy from visible settings, not source names."""
    values = job_input_defaults(policy, tests)
    requested = normalize_job_overrides(overrides)
    values.update({key: value for key, value in requested.items() if key != "timeouts"})
    values["timeouts"].update(requested.get("timeouts") or {})
    if not values["command"].strip():
        raise ValueError("Command was not captured; enter a Command in Job Setup before running")
    values = normalize_job_overrides(values)
    props = {"distributionmethod": "Command", "jobsubmitcommand": values["command"],
             "maxjobs": str(values["max_jobs"])}
    props.update(timeout_override_properties(values))
    names = list(tests) if tests is not None else [r["name"] for r in (policy or {}).get("tests") or [] if r.get("name")]
    return {"schema_version": SCHEMA, "source": _contract_text("cr_job_input_fields"), "available": True,
            "global": {"available": True, "properties": copy.deepcopy(props)},
            "tests": [{"name": name, "available": True, "properties": copy.deepcopy(props)} for name in names],
            "run_options": copy.deepcopy((policy or {}).get("run_options") or {}),
            "overrides": values, "warnings": []}


def _skill_scalar(value):
    if value is None or value is False:
        return "nil"
    if value is True:
        return "t"
    if isinstance(value, list):
        return "list(%s)" % " ".join(_skill_scalar(item) for item in value)
    if isinstance(value, (str, int, float)):
        return json.dumps(value, ensure_ascii=False)
    raise ValueError("Unsupported native policy value")


def _skill_policy(properties):
    fields = ["nil"]
    for key in sorted(properties):
        if not re.match(r"^[A-Za-z][A-Za-z0-9_]*$", key):
            raise ValueError("Invalid native job policy key")
        fields.extend(["'" + key, _skill_scalar(properties[key])])
    return "list(%s)" % " ".join(fields)


def job_policy_application_skill(policy=None, overrides=None, session_expr="session",
                                 output_path=None, tests=None, force_parallel=False,
                                 command_wrappers=None):
    """Apply the captured policy to an owned setup, then explicit overrides.

    Named native policy fields not exposed by the public setter must already be
    preserved by the private clone, or the exact readback fails before running.
    No mismatch is silently converted into a local submission.
    """
    if not re.match(r"^[A-Za-z][A-Za-z0-9_]*$", session_expr):
        raise ValueError("Invalid SKILL session expression")
    policy = copy.deepcopy(policy or unavailable_job_policy())
    expected = effective_job_policy(policy, overrides)
    lines = job_policy_override_definitions(overrides).splitlines()
    sess = session_expr
    captured_global = policy.get("global") or {}
    global_props = captured_global.get("properties") or {}
    test_names = list(tests) if tests is not None else [row["name"] for row in policy.get("tests") or [] if row.get("name")]
    if policy.get("available") and captured_global.get("available"):
        lines.append(_contract_text('unless(isCallable(\'maeGetJobPolicy) && isCallable(\'maeSetJobPolicy) error("Crescendo: captured job policy cannot be applied; no simulation submitted.\\n"))'))
        lines.append(_contract_text('crescCloneJobPolicy(%s %s nil "CRESCENDO_captured_global")') % (sess, _skill_policy(global_props)))
        for row in policy.get("tests") or []:
            name = row.get("name")
            if name not in test_names:
                continue
            if not row.get("available"):
                raise ValueError("Captured effective policy unavailable for test %s" % name)
            props = row.get("properties") or {}
            compare = lambda values: dict((key, value) for key, value in values.items() if key != "name")
            if compare(props) == compare(global_props):
                lines.append(_contract_text('crescRequireJobPolicyMatch(%s maeGetJobPolicy(?session %s ?testName %s))') % (_skill_policy(props), sess, _skill_scalar(name)))
            else:
                lines.append(_contract_text('crescCloneJobPolicy(%s %s %s %s)') % (sess, _skill_policy(props), _skill_scalar(name), _skill_scalar(_contract_text("CRESCENDO_captured_") + name)))
    # Existing saved policies remain authoritative when capture is unavailable.
    # Explicit overrides still require public APIs and exact native readback.
    test_lines, test_expr = _bounded_test_list(test_names, _contract_text("crescApplyPolicyTests"))
    lines.extend(test_lines)
    lines.append(_contract_text('crescApplyJobOverrides(%s%s)') % (sess, " " + test_expr if tests is not None else ""))
    options = policy.get("run_options") or {}
    option_values = dict((key, options[key]) for key in ("RunIn", "DivideJobs", "JobLimit") if key in options and options.get("available"))
    if force_parallel:
        option_values["RunIn"] = "Parallel"
    if option_values:
        args = []
        for key in ("RunIn", "DivideJobs", "JobLimit"):
            if key in option_values:
                value = option_values[key]
                args.extend(["?" + key, "stringToSymbol(%s)" % _skill_scalar(value) if key in ("RunIn", "DivideJobs") and isinstance(value, str) else _skill_scalar(value)])
        lines.extend([
            _contract_text('unless(isCallable(\'axlSetRunDistributeOptions) && isCallable(\'axlGetRunDistributeOptions) error("Crescendo: native Run Options cannot be applied/read back; no simulation submitted.\\n"))'),
            _contract_text('crescRunOptionsSetup = axlGetActiveSetup(axlGetMainSetupDB(%s))') % sess,
            _contract_text('unless(axlSetRunDistributeOptions(crescRunOptionsSetup %s) error("Crescendo: private Run Options were rejected; no simulation submitted.\\n"))') % " ".join(args),
            _contract_text('crescRunOptionsReadback = axlGetRunDistributeOptions(crescRunOptionsSetup)'),
        ])
        for key, value in option_values.items():
            lines.append(_contract_text('unless(equal(get(crescRunOptionsReadback \'%s) %s) error("Crescendo: private Run Options readback differs for %s; no simulation submitted.\\n"))') % (key, "stringToSymbol(%s)" % _skill_scalar(value) if key in ("RunIn", "DivideJobs") and isinstance(value, str) else _skill_scalar(value), key))
    # Wrappers are an exact argv observer only; native policy still supplies the
    # ICRP/simulation arguments. Unsupported shell syntax stays native unchanged.
    lines.extend(job_policy_observation_skill(command_wrappers, session_expr=sess).splitlines())
    if output_path:
        lines.extend(job_policy_readback_skill(output_path, session_expr=sess, tests=test_names).splitlines())
    return "\n".join(lines) + "\n"


def job_policy_readback_skill(output_path, session_expr="session", tests=None):
    if not re.match(r"^[A-Za-z][A-Za-z0-9_]*$", session_expr):
        raise ValueError("Invalid SKILL session expression")
    test_lines, expression = _bounded_test_list(tests or [], _contract_text("crescReadbackPolicyTests"))
    if tests is None:
        expression = 'maeGetSetup(?typeName "tests" ?session %s)' % session_expr
    return job_policy_capture_definitions() + "\n".join(test_lines) + "\n" + _contract_text('''
crescJobPolicyReadbackPort = outfile(%s)
unless(crescJobPolicyReadbackPort error("Crescendo: effective Job Setup diagnostics could not be written.\\n"))
fprintf(crescJobPolicyReadbackPort "CRESCENDO_JOB_POLICY_1\\n")
crescWriteJobPolicyRecords(crescJobPolicyReadbackPort %s %s)
close(crescJobPolicyReadbackPort)
crescJobPolicyReadbackPort = nil
''') % (_skill_scalar(output_path), session_expr, expression)


def verify_effective_job_policy(readback, expected, test_map=None, command_wrappers=None,
                                force_parallel=False):
    """Structured differences between captured/overridden and native settings."""
    expected = copy.deepcopy(expected or unavailable_job_policy())
    aliases = dict(test_map or {})
    originals = dict((row["name"], row) for row in expected.get("tests") or [])
    wrappers = dict((row.get("test"), row) for row in command_wrappers or [])
    differences = []
    wanted_tests = set(aliases) if test_map is not None else set(originals)
    observed_tests = set(row.get("name") for row in readback.get("tests") or [])
    if wanted_tests:
        for name in sorted(wanted_tests - observed_tests):
            differences.append({"test": name, "field": "test_policy_missing", "expected": True, "actual": False})
        for name in sorted(observed_tests - wanted_tests):
            differences.append({"test": name, "field": "test_policy_unexpected", "expected": False, "actual": True})
    scopes = [(None, readback.get("global") or {}, expected.get("global") or {})]
    scopes.extend((row.get("name"), row, originals.get(aliases.get(row.get("name"), row.get("name"))) or expected.get("global") or {}) for row in readback.get("tests") or [])
    for name, actual, original in scopes:
        if not actual.get("available"):
            differences.append({"test": name, "field": "availability", "expected": True, "actual": False})
            continue
        actual_props = actual.get("properties") or {}
        props = copy.deepcopy(original.get("properties") or {})
        if not original.get("available"):
            props = {}
            overrides = expected.get("overrides") or {}
            if "command" in overrides:
                props["jobsubmitcommand"] = overrides["command"]
                if overrides["command"]:
                    props["distributionmethod"] = "Command"
            if "max_jobs" in overrides:
                props["maxjobs"] = str(overrides["max_jobs"])
            props.update(timeout_override_properties(overrides))
        wrapper = wrappers.get(aliases.get(name, name)) or wrappers.get(None)
        if wrapper and props.get("jobsubmitcommand") == wrapper.get("original_command"):
            props["jobsubmitcommand"] = wrapper["effective_command"]
        for key, value in props.items():
            # Alias inherits the native global identity; name may be canonicalized.
            if key != "name" and actual_props.get(key) != value:
                differences.append({"test": name, "source_test": aliases.get(name, name), "field": key, "expected": value, "actual": actual_props.get(key)})
    expected_options = expected.get("run_options") or {}
    run_values = dict((key, expected_options[key]) for key in ("RunIn", "DivideJobs", "JobLimit") if expected_options.get("available") and key in expected_options)
    if force_parallel:
        run_values["RunIn"] = "Parallel"
    for key, value in run_values.items():
        actual = (readback.get("run_options") or {}).get(key)
        if actual != value:
            differences.append({"field": key, "expected": value, "actual": actual})
    return {"ok": not differences, "source": expected.get("source"),
            "overrides": copy.deepcopy(expected.get("overrides") or {}),
            "differences": differences, "global": copy.deepcopy(readback.get("global") or {}),
            "tests": copy.deepcopy(readback.get("tests") or []),
            "run_options": copy.deepcopy(readback.get("run_options") or {}),
            "command_wrappers": copy.deepcopy(command_wrappers or [])}


def _observable_command_argv(command):
    """Accept only literal native argv; shell expressions keep native semantics."""
    normalize_job_overrides({"command": command})
    if not command.strip():
        raise ValueError("Submission command is empty")
    quoted, escaped = None, False
    for char in command:
        if escaped:
            escaped = False
            continue
        if char == "\\" and quoted != "'":
            escaped = True
            continue
        if char in "'\"":
            if quoted is None:
                quoted = char
            elif quoted == char:
                quoted = None
            continue
        if (char in "$`" and quoted != "'") or (quoted is None and char in ";&|<>()*?[]{}~"):
            raise ValueError("Shell syntax is retained unchanged; observation uses native logs")
    argv = shlex.split(command)
    if not argv or re.match(r"^[A-Za-z_][A-Za-z0-9_]*=", argv[0]):
        raise ValueError("Shell assignments are retained unchanged; observation uses native logs")
    return argv


def prepare_job_command_observers(policy, overrides, owned_root, log_path,
                                  python_bin=None, helper_path=None):
    """Create task-owned command observers; never execute a scheduler here."""
    from .native_environment import GUI_ENV_NAMES
    from .utils import atomic_write_json
    expected = effective_job_policy(policy, overrides)
    root = ensure_dir(os.path.abspath(owned_root))
    python_bin = os.path.abspath(python_bin or sys.executable)
    if helper_path is None:
        from .resources import tool_path
        helper_path = tool_path("maestro_job_observer.py")
    if not os.path.isfile(helper_path):
        raise ValueError("Native command observer helper is unavailable")
    if not os.path.isabs(log_path):
        raise ValueError("Native command observer log must be an absolute owned path")
    commands, rows, unavailable = {}, [], []
    global_props = (expected.get("global") or {}).get("properties") or {}
    scopes = [(None, global_props)]
    scopes.extend((row.get("name"), row.get("properties") or {}) for row in expected.get("tests") or [])
    for test, props in scopes:
        if str(props.get("distributionmethod") or "").lower() != "command":
            continue
        command = props.get("jobsubmitcommand") or ""
        try:
            argv = _observable_command_argv(command)
        except ValueError as error:
            unavailable.append({"test": test, "command": command, "reason": str(error), "native_command_preserved": True})
            continue
        key = hashlib.sha256(command.encode("utf-8")).hexdigest()[:20]
        if command not in commands:
            config_path = os.path.join(root, "job_observer_%s.json" % key)
            shell_path = os.path.join(root, "job_observer_%s.sh" % key)
            atomic_write_json(config_path, {"schema_version": _contract_text("crescendo.maestro_job_observer.v1"),
                "command": command, "argv": argv, "log_path": log_path})
            os.chmod(config_path, 0o600)
            lines = ["#!/bin/sh", "# Owned submission observer. Restore these native states only in the real command."]
            for name in GUI_ENV_NAMES:
                lines.extend([_contract_text('CRESCENDO_JOB_ENV_%s_SET=${%s+x}') % (name, name),
                              _contract_text('CRESCENDO_JOB_ENV_%s_VALUE=${%s-}') % (name, name),
                              _contract_text('export CRESCENDO_JOB_ENV_%s_SET CRESCENDO_JOB_ENV_%s_VALUE') % (name, name),
                              'unset %s' % name])
            lines.append('exec %s %s --config %s -- "$@"' % (shlex.quote(python_bin), shlex.quote(os.path.abspath(helper_path)), shlex.quote(config_path)))
            atomic_write_text(shell_path, "\n".join(lines) + "\n")
            os.chmod(shell_path, 0o700)
            commands[command] = {"original_command": command, "submit_argv": argv,
                                 "effective_command": shlex.quote(shell_path),
                                 "observer_config": config_path, "observer_script": shell_path,
                                 "submission_log": log_path}
        row = copy.deepcopy(commands[command])
        row["test"] = test
        rows.append(row)
    return {"schema_version": _contract_text("crescendo.maestro_command_observation.v1"), "source": expected.get("source"),
            "overrides": normalize_job_overrides(overrides), "wrappers": rows,
            "unavailable": unavailable, "log_path": log_path,
            "configured_submission_proves_execution": False}


def job_policy_observation_skill(command_wrappers, session_expr="session", test_map=None):
    """Bind/rebind approved observers after native test copies and overrides."""
    if not re.match(r"^[A-Za-z][A-Za-z0-9_]*$", session_expr):
        raise ValueError("Invalid SKILL session expression")
    lines = []
    aliases = dict(test_map or {})
    for row in command_wrappers or []:
        source = row.get("test")
        targets = [None] if source is None else [name for name, original in aliases.items() if original == source]
        if source is not None and not aliases:
            targets = [source]
        for target in targets:
            target_arg = ' ?testName %s' % _skill_scalar(target) if target else ""
            lines.extend([
                _contract_text('crescObservedPolicy = copy(maeGetJobPolicy(?session %s%s))') % (session_expr, target_arg),
                _contract_text('unless(crescObservedPolicy && member(crescObservedPolicy->jobsubmitcommand list(%s %s)) error("Crescendo: observer command identity differs; no simulation submitted.\\n"))') % (_skill_scalar(row["original_command"]), _skill_scalar(row["effective_command"])),
                _contract_text('unless(equal(crescObservedPolicy->jobsubmitcommand %s)') % _skill_scalar(row["effective_command"]),
                _contract_text('  crescObservedPolicy->jobsubmitcommand = %s') % _skill_scalar(row["effective_command"]),
                _contract_text('  crescCloneJobPolicy(%s crescObservedPolicy %s "CRESCENDO_observed_command")') % (session_expr, _skill_scalar(target)),
                ')',
            ])
    return "\n".join(lines) + "\n"


def native_job_policy_assertion_skill(expected, session_expr="session", test_map=None,
                                      command_wrappers=None, force_parallel=False):
    """Assert exact expected policies in the execution session BEFORE submit."""
    if not re.match(r"^[A-Za-z][A-Za-z0-9_]*$", session_expr):
        raise ValueError("Invalid SKILL session expression")
    expected = copy.deepcopy(expected or unavailable_job_policy())
    originals = dict((row["name"], row) for row in expected.get("tests") or [])
    wrappers = dict((row.get("test"), row) for row in command_wrappers or [])
    aliases = dict(test_map or {})
    if not aliases:
        aliases = dict((name, name) for name in originals)
    scopes = [(None, None, expected.get("global") or {})]
    for target, source in aliases.items():
        original = originals.get(source)
        if original is None and expected.get("available"):
            raise ValueError("Captured job policy has no source test evidence for %s" % source)
        scopes.append((target, source, original or {}))
    lines = []
    for target, source, scope in scopes:
        props = dict((key, value) for key, value in (scope.get("properties") or {}).items() if key != "name")
        if not scope.get("available"):
            # An unavailable snapshot cannot authorize a fallback. At least the
            # explicit fields must be proved against native execution settings.
            overrides = expected.get("overrides") or {}
            props = {}
            if "command" in overrides:
                props["jobsubmitcommand"] = overrides["command"]
                if overrides["command"]:
                    props["distributionmethod"] = "Command"
            if "max_jobs" in overrides:
                props["maxjobs"] = str(overrides["max_jobs"])
            props.update(timeout_override_properties(overrides))
        wrapper = wrappers.get(source) or wrappers.get(None)
        if wrapper and props.get("jobsubmitcommand") == wrapper.get("original_command"):
            props["jobsubmitcommand"] = wrapper["effective_command"]
        target_arg = ' ?testName %s' % _skill_scalar(target) if target else ""
        if props:
            lines.append(_contract_text('crescRequireJobPolicyMatch(%s maeGetJobPolicy(?session %s%s))') % (_skill_policy(props), session_expr, target_arg))
    options = expected.get("run_options") or {}
    wanted = dict((key, options[key]) for key in ("RunIn", "DivideJobs", "JobLimit") if options.get("available") and key in options)
    if force_parallel:
        wanted["RunIn"] = "Parallel"
    if wanted:
        lines.append(_contract_text('crescFinalRunOptions = axlGetRunDistributeOptions(axlGetActiveSetup(axlGetMainSetupDB(%s)))') % session_expr)
        for key, value in wanted.items():
            literal = 'stringToSymbol(%s)' % _skill_scalar(value) if key in ("RunIn", "DivideJobs") and isinstance(value, str) else _skill_scalar(value)
            lines.append(_contract_text('unless(crescFinalRunOptions && equal(get(crescFinalRunOptions \'%s) %s) error("Crescendo: final effective Run Options differ for %s; no simulation submitted.\\n"))') % (key, literal, key))
    return "\n".join(lines) + "\n"
