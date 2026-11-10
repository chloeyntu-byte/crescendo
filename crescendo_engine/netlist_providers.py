"""Private netlist producers for the common solver/submission pipeline.

Maestro produces a fresh numeric netlist for each candidate. The template
producer qualifies its symbolic mapping once and only writes candidate numbers.
Native invocation capture is confined to netlist production: its owned wrapper
records argv and exits before a solver or scheduler can run.
"""
from __future__ import print_function

import copy
import hashlib
import math
import os
import re
import shutil
import time

from .execution_commands import (build_solver_command_plan, normalize_execution_commands,
                                 submission_cpu_hint)
from .private_netlist import (SCHEMA, PrivateNetlistError, PrivateNetlistUnsupported,
    _capture_phase, _check_cancel, _digest, _manifest_digest, _qualify_setup_capture,
    _text, _regular, native_netlist_identity, top_level_native_parameters, validate_manifest)
from .simulators import execution_contract, rewrite_model_references
from .native_includes import (NativeIncludeError, freeze_native_includes, resolve_include_file,
                              native_include_environment)
from .utils import atomic_write_json, atomic_write_text, ensure_dir, is_under, utc_timestamp
from .workspace import build_workspace, verify_sources_unchanged
from .native_maestro_parameters import GENERATION_PROTOCOL
from .native_qualification import NATIVE_QUALIFICATION_REVISION
from .variable_scope_contract import require_unique_run_variables


# Consume comments and quoted strings as whole tokens so text such as
# ``description="file=old_name"`` never becomes an input declaration.
_INPUT_STRING = r'"(?:\\.|[^"\\\n])*"'
_INPUT_TOKENS = re.compile(
    r'(?P<comment>//[^\n]*|/\*.*?\*/|^[ \t]*\*[^\n]*)'
    r'|(?P<include>^[ \t]*(?:include|ahdl_include|`include)\b[ \t]*)'
    r'(?P<include_literal>' + _INPUT_STRING + r')?'
    r'|(?P<file>\b(?P<parameter>file|filename|datafile|wavefile|stimulusfile)\s*=\s*)'
    r'(?:(?P<file_literal>' + _INPUT_STRING + r')|(?P<file_identifier>[A-Za-z_][A-Za-z0-9_]*))?'
    r'|(?P<string>' + _INPUT_STRING + r')', re.M | re.S | re.I)
_INPUT_IDENTIFIER_END = re.compile(r'[ \t]*(?:$|\n|//|[A-Za-z_][A-Za-z0-9_]*\s*=)')


def validate_provider_request(prepared, parameters, config, spec=None, source_mode="maestro_netlist",
                              generation_only=False):
    """Read-only checks before copying OA, running callbacks or starting a job."""
    if source_mode not in ("maestro_netlist", "private_netlist"):
        raise PrivateNetlistUnsupported("unknown private netlist source mode")
    from .test_selection import selection_plan, effective_setup
    setup = effective_setup(prepared.get("setup") or {}, selection_plan(prepared.get("setup") or {}, spec))
    variable_scope = require_unique_run_variables(setup, action="generate private SCS", multiplicity_only=True)
    if (setup.get("backend") or "maestro") != "maestro":
        raise PrivateNetlistUnsupported("the common CLI netlist producer currently requires a saved Maestro setup")
    if not parameters or len({row.get("parameter_id") for row in parameters}) != len(parameters):
        raise PrivateNetlistUnsupported("selected parameter identities are missing or duplicated")
    if any(not row.get("parameter_id") for row in parameters):
        raise PrivateNetlistUnsupported("selected parameter identity is missing")
    if any(row.get("backend", "schematic") != "schematic" for row in parameters):
        raise PrivateNetlistUnsupported("test-scoped design-variable optimization has no qualified common CLI netlist adapter")
    simulators = {(test.get("simulator") or "spectre") for test in setup.get("tests") or [] if test.get("enabled", True)}
    if not simulators:
        raise PrivateNetlistUnsupported("the saved Maestro has no enabled tests")
    if len(simulators) != 1:
        raise PrivateNetlistUnsupported("mixed saved simulators require separate qualified native netlist adapters")
    capture = next(iter(simulators))
    _qualify_setup_capture(setup, capture)
    if generation_only:
        return {"source_mode": source_mode, "capture_source_simulator": capture,
                "generation_only": True, "solver_qualification_required": False,
                "measurement_qualification_required": False}
    commands = normalize_execution_commands(config, spec)
    from .netlist_execution import execution_policy
    policy = execution_policy(dict(prepared, setup=setup), spec or {}, config)
    resource_cpu = submission_cpu_hint(commands, policy["global"])
    execution_contract(config, capture, resource_cpu=resource_cpu, spec=spec)
    from .netlist_measurement import qualify_expressions
    enabled = {test["name"] for test in setup.get("tests") or [] if test.get("enabled", True)}
    outputs = setup.get("outputs") or []
    tasks = [{"test": test, "outputs": [dict(row, key=row.get("key") or "%s::%s" % (test, row.get("name")))
              for row in outputs if row.get("test") == test and row.get("name") and not row["name"].startswith("/")]}
             for test in sorted(enabled)]
    required = sorted({row["output_key"] for row in
                       (spec or {}).get("objectives", []) + (spec or {}).get("constraints", [])
                       if row.get("output_key")})
    qualification = qualify_expressions(tasks, required if required else None)
    if not qualification["supported"]:
        error = PrivateNetlistUnsupported("common CLI measurement cannot reproduce the saved outputs: " + "; ".join(qualification["reasons"]))
        error.detail = {"failure_stage": "measurement_preflight", "qualification": qualification,
                        "execution_started": False, "cleanup_proven": True}
        raise error
    return {"source_mode": source_mode, "capture_source_simulator": capture,
            "execution_commands": commands, "resource_cpu": resource_cpu,
            "measurement_qualification": qualification, "execution_policy": policy,
            "variable_scope": {key: variable_scope[key] for key in (
                "source", "multiplicity_ok", "native_pending", "effective_point_values_proven")}}



def _freeze_generated_dependencies(text, task, config, mutable_roots, cancel_check=None,
                                   allow_candidate_parameters=False):
    """Resolve native file inputs; snapshot only reused generation content."""
    roots = [os.path.realpath(path) for path in mutable_roots or [] if path]
    environ = native_include_environment(config)
    directories = (task.get("include_resolution") or {}).get("include_dirs") or []
    native_cwd = task.get("native_cwd") or task["cwd"]
    target_root = os.path.join(task["cwd"], "cr_generated_inputs")
    copied, files, total = {}, [], [0]
    static_inputs = task.setdefault("static_file_inputs",[])
    evidence = {"schema_version": "crescendo.native_file_inputs.v1", "deferred": [],
                "deferred_count": 0, "deferred_omitted": 0,
                "resolved_parameters": [], "resolved_parameter_count": 0,
                "resolved_inputs": [], "resolved_input_count": 0, "resolved_inputs_omitted": 0,
                "task_id": task.get("task_id"), "test": task.get("test"),
                "corner": task.get("source_corner") or task.get("corner"),
                "native_corner": task.get("corner"), "point_id": task.get("point_id"),
                "interpretation": "simulator", "native_cwd": native_cwd,
                "snapshot_cwd": task["cwd"], "include_dirs": list(directories),
                "path_policy": "literal_inputs_use_captured_search_order; expressions_preserved"}
    task["native_file_inputs"] = evidence
    main_source = task.get("netlist_path") or os.path.join(task["cwd"], "input.scs")
    parameter_literals = [None]
    parameter_bindings = {}
    subckt_names = set()
    static_locations = {(row["path"], row.get("reference_path") or row["path"])
                        for row in static_inputs}

    def context(value, match, source):
        start = value.rfind("\n", 0, match.start()) + 1
        end = value.find("\n", match.end())
        if end < 0:
            end = len(value)
        return {"source": source, "line": value.count("\n", 0, match.start()) + 1,
                "statement": value[start:min(end, start + 512)],
                "parameter": match.group("parameter"),
                "expression": value[match.end("file"):min(end, match.end("file") + 256)]
                    if match.group("file") else match.group(0)[:256]}

    def deferred(value, match, source, reason):
        evidence["deferred_count"] += 1
        if len(evidence["deferred"]) < 32:
            evidence["deferred"].append(dict(context(value, match, source), reason=reason))
        else:
            evidence["deferred_omitted"] += 1

    def unquote(value):
        # Decode only quote/backslash escapes; ordinary simulator strings are
        # neither Python expressions nor shell expansion requests.
        return re.sub(r'\\(["\\])', r'\1', value[1:-1])

    def quoted(path):
        return '"' + path.replace('\\', '\\\\').replace('"', '\\"') + '"'

    def complete_value(value, end):
        if not _INPUT_IDENTIFIER_END.match(value, end):
            return False
        # A leading-plus continuation can append an operator to this value.
        # Keep the complete expression opaque rather than replace its prefix.
        line_end = value.find('\n', end)
        for _ in range(16):
            if line_end < 0:
                return True
            next_end = value.find('\n', line_end + 1)
            line = value[line_end + 1:next_end if next_end >= 0 else len(value)].strip()
            if line and not line.startswith('//'):
                return not line.startswith(('+', '/*'))
            line_end = next_end
        return False

    def parameter_reference(name):
        # A sole literal top-level declaration is enough to locate an exact
        # dependency. Any local/instance assignment of that name makes the
        # binding ambiguous, so leave it for the simulator. No expression or
        # alias evaluation and no search based on filename suffixes is used.
        if name in parameter_bindings:
            return parameter_bindings[name]
        if parameter_literals[0] is None:
            from .spectre_parameter_syntax import parameter_statement_spans, parse_parameter_declarations
            # The usual model-backed netlist needs no extra declaration pass:
            # a binding may be shadowed by included content, so defer it.
            if re.search(r'(?mi)^[ \t]*(?:include|ahdl_include|`include)\b', text):
                parameter_literals[0] = {}
                return None
            analysis_text = _INPUT_TOKENS.sub(
                lambda match: (' ' + '\n' * match.group(0).count('\n'))
                if match.group('comment') else match.group(0), text)
            analysis_text = re.sub(r'(?mi)^([ \t]*)(parameters|subckt|ends)\b',
                                   lambda match: match.group(1) + match.group(2).lower(), analysis_text)
            # Included Spectre/AHDL content may shadow a name. We do not parse
            # models to prove optional string bindings; the simulator resolves
            # those expressions using the preserved native include paths.
            if re.search(r'(?mi)^[ \t]*(?:include|ahdl_include|`include)\b', analysis_text):
                parameter_literals[0] = {}
                return None
            rows = task.get("native_parameters") if analysis_text == text else None
            if rows is None:
                try:
                    rows = top_level_native_parameters(analysis_text)
                except PrivateNetlistError:
                    rows = []  # Optional dependency discovery is not a new precondition.
            declarations = {}
            for _start, _end, statement in parameter_statement_spans(analysis_text):
                for key, _value in parse_parameter_declarations(statement):
                    declarations[key] = declarations.get(key, 0) + 1
            header, continued = False, False
            for line in analysis_text.splitlines():
                stripped = line.strip()
                header = bool(re.match(r'^subckt\b', stripped) or
                              (header and (continued or stripped.startswith('+'))))
                if header:
                    subckt_names.update(re.findall(r'\b[A-Za-z_][A-Za-z0-9_]*\b', stripped))
                continued = stripped.endswith('\\')
            parameter_literals[0] = {
                row["name"]: unquote(row["expression"])
                for row in rows if declarations.get(row["name"]) == 1 and
                re.fullmatch(_INPUT_STRING, row.get("expression") or "")}
        parameter_bindings[name] = None
        if name not in parameter_literals[0]:
            return None
        # Inline subckt formals may be bare rather than ``name=value``.
        # Conservatively retain the expression if the name occurs in one.
        if name in subckt_names:
            return None
        assignments = re.finditer(r'\b' + re.escape(name) + r'\s*=', text)
        if next(assignments, None) is None or next(assignments, None) is not None:
            return None
        parameter_bindings[name] = parameter_literals[0][name]
        return parameter_bindings[name]

    def mutable(path):
        return any(is_under(path, root) for root in roots)
    def freeze_file(path, parse_text):
        _check_cancel(cancel_check)
        path = os.path.abspath(path)
        if path in copied:
            return copied[path]
        if len(copied) >= 512 or total[0] >= 64*1024*1024:
            raise NativeIncludeError("Generated input dependency snapshot exceeds its qualification bound")
        owner = next((root for root in roots if is_under(path,root)), None)
        _regular(path, owner)
        size = os.path.getsize(path)
        total[0] += size
        if total[0] > 64*1024*1024:
            raise NativeIncludeError("Generated input dependency snapshot exceeds its byte bound")
        destination = os.path.join(target_root, hashlib.sha256(path.encode("utf-8")).hexdigest()[:20], os.path.basename(path))
        copied[path] = destination
        ensure_dir(os.path.dirname(destination))
        chunks = []
        with open(path,"rb") as stream:
            while True:
                _check_cancel(cancel_check)
                block = stream.read(256*1024)
                if not block:
                    break
                chunks.append(block)
        raw = b"".join(chunks)
        if parse_text:
            try:
                value = raw.decode("utf-8")
            except UnicodeError:
                raise NativeIncludeError("Generated include is not a qualified text dependency: " + path)
            value = rewrite(value, os.path.dirname(path), path)
            raw = value.encode("utf-8")
        with open(destination,"wb") as stream:
            stream.write(raw)
        files.append({"path":destination,"relative_path":os.path.relpath(destination,task["cwd"]),
                      "sha256":_digest(destination),"source":path,"metadata":True,
                      "snapshot_reason":"referenced_reused_generation_input"})
        return destination
    def rewrite(value, include_cwd, source):
        _check_cancel(cancel_check)
        def replace(match):
            if match.group("comment"):
                return match.group(0)
            if match.group("include"):
                try:
                    if not match.group("include_literal"):
                        raise NativeIncludeError("Nonliteral native include needs explicit generated-input qualification")
                    reference = unquote(match.group("include_literal"))
                    if allow_candidate_parameters and reference == "cr_candidate_parameters.scs":
                        return match.group(0)
                    path = resolve_include_file(reference, include_cwd, [native_cwd]+directories, environ)
                    target = freeze_file(path, True) if mutable(path) else path
                    return match.group("include") + quoted(target)
                except NativeIncludeError as error:
                    error.detail.update(input_context=context(value, match, source))
                    raise
            if match.group("file"):
                literal = match.group("file_literal")
                identifier = match.group("file_identifier")
                reference = unquote(literal) if literal else None
                if (identifier and source == main_source and
                        complete_value(value, match.end())):
                    reference = parameter_reference(identifier)
                # A quoted prefix followed by an operator is still a dynamic
                # expression, not a filename to rewrite or prevalidate.
                if reference is None or not complete_value(value, match.end()):
                    deferred(value, match, source, "expression_preserved_for_simulator")
                    return match.group(0)
                try:
                    path = resolve_include_file(reference, native_cwd, directories, environ)
                except NativeIncludeError:
                    # These names can be ordinary strings, output filenames,
                    # or optional/unused inputs. Only the simulator knows their
                    # meaning; a missing literal must not block preparation.
                    deferred(value, match, source, "literal_unresolved_preserved_for_simulator")
                    return match.group(0)
                resolved_path = path
                handling = "existing_task_input"
                if mutable(path):
                    path = freeze_file(path, False)
                    handling = "generated_input_snapshot"
                elif not is_under(path, task["cwd"]):
                    handling = "shared_static_input"
                    real = os.path.realpath(path)
                    if (real, path) not in static_locations:
                        _regular(real)
                        static_inputs.append({"path":real,"reference_path":path,
                                              "bytes":os.path.getsize(real),"sha256":_digest(real)})
                        static_locations.add((real, path))
                evidence["resolved_input_count"] += 1
                if len(evidence["resolved_inputs"]) < 32:
                    evidence["resolved_inputs"].append(dict(
                        context(value, match, source), reference=reference,
                        resolved_path=resolved_path, execution_path=path, handling=handling))
                else:
                    evidence["resolved_inputs_omitted"] += 1
                if identifier:
                    evidence["resolved_parameter_count"] += 1
                    if len(evidence["resolved_parameters"]) < 32:
                        evidence["resolved_parameters"].append(dict(
                            context(value, match, source), binding=identifier,
                            reference=reference, resolved_path=path,
                            reason="unique_literal_parameter_dependency"))
                return match.group("file") + quoted(path)
            # Explicit absolute strings below reused generation roots retain
            # the existing snapshot protection without guessing file types.
            path = unquote(match.group("string"))
            if (complete_value(value, match.end()) and os.path.isabs(path) and
                    mutable(os.path.abspath(path)) and os.path.isfile(path)):
                return quoted(freeze_file(path, False))
            return match.group(0)
        return _INPUT_TOKENS.sub(replace, value)
    result = rewrite(text, native_cwd, main_source)
    evidence.update(snapshot_count=len(files), static_identity_count=len(static_inputs))
    atomic_write_json(os.path.join(task["cwd"], "native_file_inputs.json"), evidence)
    return result, files


def _freeze_task(task, config, capture, commands, resource_cpu, spec, candidate_root, mutable_roots=None, cancel_check=None):
    # Both producers use root/tasks/<id>. The common point-commit cleanup gate
    # must not need a separate native-capture layout or a broader deletion rule.
    if not re.match(r"^[A-Za-z0-9_.-]{1,128}$", task.get("task_id") or "") or task["task_id"] in (".", ".."):
        raise PrivateNetlistError("numeric native task identity is unsafe")
    task_root = ensure_dir(os.path.join(candidate_root, "tasks", task["task_id"]))
    cwd = ensure_dir(os.path.join(task_root, "netlist"))
    files, seen = [], set()
    for row in task["template_files"]:
        relative = row["relative_path"]
        path = os.path.abspath(os.path.join(cwd, relative))
        if relative in seen or not is_under(path, cwd):
            raise PrivateNetlistError("numeric native snapshot relative path is unsafe or duplicated")
        seen.add(relative)
        _regular(row["path"], root=candidate_root)
        ensure_dir(os.path.dirname(path))
        shutil.copyfile(row["path"], path)
        files.append(dict(row, path=path))
    task["capture_snapshot_root"] = task["cwd"]
    task.update(cwd=cwd, netlist_path=os.path.join(cwd, "input.scs"),
                psf_dir=os.path.join(task_root, "psf"), template_files=files)
    text = _text(task["netlist_path"])
    try:
        text, task["argv"], task["include_resolution"] = freeze_native_includes(text, task, config, capture)
    except NativeIncludeError as error:
        failure = PrivateNetlistUnsupported(str(error))
        failure.detail = {"failure_stage": "model_include_resolution", "execution_started": False,
                          "capture_source_simulator": capture}
        raise failure
    text, generated = _freeze_generated_dependencies(text, task, config, mutable_roots, cancel_check)
    files.extend(generated)
    task["generated_input_snapshots"] = generated
    captured = native_netlist_identity(text)
    contract = execution_contract(config, capture, captured["native_models"],
                                  default_executable=task["argv"][0], spec=spec,
                                  template_argv=task["argv"], resource_cpu=resource_cpu)
    text = rewrite_model_references(text, contract)
    execution_identity = native_netlist_identity(text)
    atomic_write_text(task["netlist_path"], text)
    simulator = contract["execution_simulator"]
    task["spectre_log"] = os.path.join(task["psf_dir"], simulator + ".out")
    task["simulator_log"] = task["spectre_log"]
    task["ahdl_dir"] = os.path.join(os.path.dirname(task["psf_dir"]), "ahdl")
    task["execution_simulator"] = simulator
    task["capture_source_simulator"] = capture
    task["simulator_contract"] = contract
    task["execution_commands"] = copy.deepcopy(commands)
    task["solver_command_plan"] = build_solver_command_plan(
        contract, task["argv"], task["netlist_path"], task["psf_dir"], task["spectre_log"],
        task["ahdl_dir"], commands=commands)
    task["argv"] = list(task["solver_command_plan"]["argv"])
    task["capture_native_identity"] = captured
    task["execution_native_identity"] = execution_identity
    task["execution_model_identity"] = copy.deepcopy(contract["models"])
    task["models"] = copy.deepcopy(execution_identity["native_models"])
    task["native_parameters"] = top_level_native_parameters(text)
    names = {row["name"] for row in task["native_parameters"]}
    task["variables"] = [copy.deepcopy(row) for row in task.get("variables") or [] if row.get("name") not in names] + [
        {"name": row["name"], "value": row["value"]} for row in task["native_parameters"] if row["value"] is not None]
    for row in task["template_files"]:
        row["sha256"] = _digest(row["path"])
    task["artifact_sha256"] = {row["path"]: row["sha256"] for row in task["template_files"]}
    task["netlist_sha256"] = _digest(task["netlist_path"])
    task["metadata_files"] = [row["path"] for row in task["template_files"] if row.get("metadata")]
    task.pop("template_files", None)
    # native_cwd remains provenance: dependencies can still live below the
    # owned capture workspace, which is retained until all tasks are stopped.
    return task


def prepare_maestro_netlists(prepared, parameters, values, config, root, candidate_id,
                             spec=None, cancel_check=None, progress_callback=None, shared_state=None,
                             generation_only=False, allow_effective_adjustment=False):
    from .setup_private_session import borrow_setup_private_session
    with borrow_setup_private_session(prepared, config, root, cancel_check) as session_workspace:
        return _prepare_maestro_netlists(prepared, parameters, values, config, root, candidate_id,
            spec=spec, cancel_check=cancel_check, progress_callback=progress_callback,
            shared_state=shared_state, generation_only=generation_only,
            allow_effective_adjustment=allow_effective_adjustment, session_workspace=session_workspace)


def _prepare_maestro_netlists(prepared, parameters, values, config, root, candidate_id,
                             spec=None, cancel_check=None, progress_callback=None, shared_state=None,
                             generation_only=False, allow_effective_adjustment=False, session_workspace=None):
    """Return a candidate manifest accepted by NetlistExecutionContext."""
    from .private_netlist import _capture_request_config
    config = _capture_request_config(prepared, config, spec)
    request = validate_provider_request(prepared, parameters, config, spec,
                                        generation_only=generation_only)
    if set(values) != {row["parameter_id"] for row in parameters}:
        raise PrivateNetlistError("candidate parameter identities do not match the selected private schematic")
    if any(isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)) for value in values.values()):
        raise PrivateNetlistError("candidate parameter values must be finite numbers")
    root = os.path.abspath(root)
    if os.path.lexists(root):
        raise PrivateNetlistError("numeric Maestro netlist generation requires a fresh root")
    _check_cancel(cancel_check)
    config = copy.deepcopy(config)
    config["_capture_source_simulator"] = request["capture_source_simulator"]
    started = time.monotonic()
    ensure_dir(root)
    if session_workspace is not None:
        from .setup_private_session import write_session_reference
        workspace = session_workspace
        write_session_reference(root, prepared["setup_private_session"])
    elif shared_state is None:
        workspace = build_workspace(prepared["setup"], prepared["hierarchy"], config,
            os.path.join(root, "workspace"), expected_source_identity=prepared.get("source_identity"), parameters=parameters)
    else:
        from .shared_maestro import write_reference
        workspace = shared_state.get("workspace")
        if workspace is None:
            workspace = build_workspace(prepared["setup"], prepared["hierarchy"], config,
                shared_state["owner"]["workspace_root"], expected_source_identity=prepared.get("source_identity"), parameters=parameters)
            shared_state["workspace"] = workspace
        write_reference(root,candidate_id,shared_state["owner"])
    if progress_callback:
        progress_callback({"stage": "netlist_generation", "candidate_id": str(candidate_id),
                           "source_mode": "maestro_netlist", "netlist_method": "capture_wrapper", "method": "capture_wrapper",
                           "message": "Generating numeric netlists from the private Maestro and selected private schematic views"})
    initialized = session_workspace is not None or bool(shared_state and shared_state.get("initialized"))
    capture_options = {"private_config_initialized":True} if initialized and session_workspace is None else {}
    if allow_effective_adjustment:
        capture_options["allow_effective_adjustment"] = True
    phase = _capture_phase(workspace, parameters, values, {}, {}, config, os.path.join(root, "numeric"),
                           emit_clones=not initialized, symbolic=False, cancel_check=cancel_check,
                           progress_callback=progress_callback, verify_numeric_application=True, **capture_options)
    mutable_roots = None
    if shared_state is not None or session_workspace is not None:
        from .private_netlist import reconcile_preparation
        capture_stop = reconcile_preparation(root,request_cancel=False)
        if not capture_stop.get("cleanup_proven"):
            error = PrivateNetlistError("Shared Maestro capture has not stopped before generated input snapshot")
            error.detail = {"failure_stage":"native_generation_termination","termination_failed":True,
                            "preparation_cleanup":capture_stop}
            raise error
        mutable_roots = [workspace.get("simulation_root")]
        storage = workspace.get("native_storage") or {}
        mutable_roots.extend(storage.get(key) for key in ("save_dir","save_res_dir","local_psf_dir","local_rdb_dir","scratch_root"))
        # The actual captured cwd is native generation output even when its
        # configured staging field is absent in older workspace manifests.
        mutable_roots.extend(task.get("native_cwd") for task in phase["tasks"]
                             if task.get("native_cwd") and is_under(task["native_cwd"],workspace["root"]))
        mutable_roots = [path for path in mutable_roots if path]
    if generation_only:
        guard = verify_sources_unchanged(workspace)
        if not guard.get("ok"):
            raise PrivateNetlistError("source design changed during native parameter generation")
        result = {"schema_version": "crescendo.native_parameter_capture.v1", "root": root,
                  "candidate_id": str(candidate_id), "workspace": workspace,
                  "execution_selection": copy.deepcopy(config.get("_capture_execution_selection")),
                  "tasks": phase["tasks"], "requested_values": dict(values),
                  "applied_values": phase["applied_values"],
                  "parameter_application": phase["parameter_application"],
                  "native_variable_scope": phase.get("native_variable_scope"),
                  "native_variable_scope_path": phase.get("native_variable_scope_path"),
                  "native_worker_diagnostics": phase.get("native_worker_diagnostics"),
                  "source_guard": guard, "generation_only": True,
                  "preparation": {"phases": [{"name": "numeric", "command": phase["command"],
                                                "corner_identity": phase["corner_identity"]}]},
                  "netlist_generation": {"source_mode": "maestro_netlist", "method": "native_Run_device_parameters",
                    "generation_protocol": GENERATION_PROTOCOL,
                    "execution_selection": copy.deepcopy(config.get("_capture_execution_selection")),
                    "qualification_revision": NATIVE_QUALIFICATION_REVISION,
                    "parameter_application": phase["parameter_application"], "point_count": len(phase["tasks"]),
                    "solver_invocations": 0, "remote_submissions": 0}}
        atomic_write_json(os.path.join(root, "native_parameter_capture.json"), result)
        return result
    tasks = []
    for task in phase["tasks"]:
        task_policy = request["execution_policy"]["tests"].get(task["test"], request["execution_policy"]["global"])
        cpu = submission_cpu_hint(request["execution_commands"], task_policy)
        tasks.append(_freeze_task(task, config, request["capture_source_simulator"],
                                  request["execution_commands"], cpu, spec, root, mutable_roots=mutable_roots, cancel_check=cancel_check))
    guard = verify_sources_unchanged(workspace)
    if not guard.get("ok"):
        raise PrivateNetlistError("source design changed during numeric Maestro netlist generation")
    _check_cancel(cancel_check)
    private_identity = {key: workspace.get(key) for key in ("root", "primary_library", "setup_cell", "setup_view", "copy_policy")}
    if workspace.get("setup_sdb"):
        private_identity["setup_sha256"] = _digest(workspace["setup_sdb"])
    private_identity["views"] = [{key: row.get(key) for key in ("source", "target", "role")}
                                 for row in workspace.get("same_library_registry") or []]
    parameter_application = copy.deepcopy(phase["parameter_application"])
    parameter_application.update(requested=dict(values), applied=phase["applied_values"])
    generation = {"source_mode": "maestro_netlist", "source": "maestro", "netlist_method": "capture_wrapper", "method": "capture_wrapper",
                  "generation_protocol": GENERATION_PROTOCOL,
                    "qualification_revision": NATIVE_QUALIFICATION_REVISION,
                  "source_identity": prepared.get("source_identity"),
                  "execution_selection": copy.deepcopy(config.get("_capture_execution_selection")),
                  "private_identity": private_identity,
                  "parameter_application": parameter_application,
                  "native_worker_diagnostics": phase.get("native_worker_diagnostics"),
                  "capture_timeout_profiles": copy.deepcopy(phase.get("capture_timeout_profiles")),
                  "capture_outer_budget": copy.deepcopy(phase.get("capture_outer_budget")),
                  "point_count": len(tasks), "source_guard": guard,
                  "solver_invocations": 0, "remote_submissions": 0,
                  "capture_only_wrapper_exit": 1}
    manifest = {"schema_version": SCHEMA, "root": root, "candidate_id": str(candidate_id),
                "created_at": utc_timestamp(), "values": dict(values),
                "requested_values": dict(values), "applied_values": phase["applied_values"],
                "execution_commands": request["execution_commands"],
                "execution_selection": copy.deepcopy(config.get("_capture_execution_selection")),
                "capture_source_simulator": request["capture_source_simulator"],
                "execution_simulator": tasks[0]["execution_simulator"],
                "parameter_symbols": {}, "tasks": tasks, "workspace": copy.deepcopy(workspace),
                "source_identity": prepared.get("source_identity"), "source_guard": guard,
                "parameter_application": phase["parameter_application"],
                "export_snapshot": phase.get("export_snapshot"), "netlist_generation": generation,
                "qualification": {"ok": True, "revision": NATIVE_QUALIFICATION_REVISION, "scope": "numeric_native_CDF_and_generated_netlist_readback", "callback_legality_per_candidate": True},
                "preparation": {"elapsed_seconds": time.monotonic() - started,
                                "phases": [{"name": "numeric", "command": phase["command"], "corner_identity": phase["corner_identity"]}],
                                "solver_invocations": 0, "spectre_simulations": 0, "remote_submissions": 0}}
    manifest["manifest_sha256"] = _manifest_digest(manifest)
    validate_manifest(manifest)
    atomic_write_json(os.path.join(root, "private_netlist.json"), manifest)
    atomic_write_json(os.path.join(root, "private_netlist_candidate.json"), manifest)
    if progress_callback:
        progress_callback({"stage": "netlist_ready", "candidate_id": str(candidate_id),
                           "netlist_generation": generation, "task_count": len(tasks),
                           "manifest_sha256": manifest["manifest_sha256"]})
    return manifest
