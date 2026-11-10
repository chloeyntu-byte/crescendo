"""Frozen execution identities for Spectre and explicitly selected ALPS.

Solver commands use explicit simulator contracts. Results are
qualified by the owned receipt and the native reader, never an invented log
summary. A model-format bridge is a new execution identity, not relocation.
"""
import copy
import hashlib
import json
import math
import os
import re
import shlex
import stat


SCHEMA = "crescendo.simulator_execution.v1"


class SimulatorConfigurationError(ValueError):
    pass


def submission_cpu(command):
    """Read an explicit -R cpu resource value from the selected argv."""
    if isinstance(command, str):
        try:
            argv = shlex.split(command)
        except ValueError:
            raise SimulatorConfigurationError("Submission resource command is malformed")
    else:
        argv = list(command or [])
    if not argv or os.path.basename(argv[0]) in ("sh", "bash", "dash", "tcsh", "csh", "zsh", "ksh"):
        return None
    if any(argument in ("|", "||", "&&", ";", "&", ">", ">>", "<") for argument in argv):
        return None
    found = set()
    for index, argument in enumerate(argv):
        resource = argv[index + 1] if argument == "-R" and index + 1 < len(argv) else (argument[2:] if argument.startswith("-R") and len(argument) > 2 else None)
        if resource is None:
            continue
        for field in resource.split(";"):
            match = re.match(r"^\s*cpu\s*=\s*([0-9]+)\s*$", field)
            if match:
                found.add(int(match.group(1)))
            elif re.match(r"^\s*cpu\s*=", field):
                raise SimulatorConfigurationError("scheduler cpu must be an exact positive integer")
    if len(found) > 1 or any(not 1 <= value <= 4096 for value in found):
        raise SimulatorConfigurationError("scheduler cpu resource is conflicting or invalid")
    return next(iter(found)) if found else None


def selected_simulator(config=None, spec=None):
    value = (spec or {}).get("simulator")
    if value is None:
        value = ((config or {}).get("cadence") or {}).get("simulator", "spectre")
    if not isinstance(value, str) or value.strip().lower() not in ("spectre", "alps"):
        raise SimulatorConfigurationError("Simulator must be Spectre or ALPS")
    return value.strip().lower()


def _fingerprint(value):
    value = dict(value)
    value.pop("fingerprint", None)
    return hashlib.sha256(json.dumps(value, sort_keys=True,
                                     separators=(",", ":")).encode("utf-8")).hexdigest()


def _file_identity(path, executable=False):
    if not isinstance(path, str) or not os.path.isabs(path) or "\x00" in path:
        raise SimulatorConfigurationError("Simulator/model path must be an explicit absolute path")
    resolved = os.path.realpath(path)
    try:
        info = os.stat(resolved)
    except OSError:
        raise SimulatorConfigurationError("Simulator/model file is unavailable: " + path)
    if not stat.S_ISREG(info.st_mode) or (executable and not os.access(resolved, os.X_OK)):
        raise SimulatorConfigurationError("Simulator/model file is not usable: " + path)
    digest = hashlib.sha256()
    with open(resolved, "rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return {"path": path, "realpath": resolved, "size": info.st_size,
            "sha256": digest.hexdigest(), "executable": bool(executable)}


def execution_contract(config, capture_source_simulator="spectre", model_references=None,
                       default_executable=None, spec=None, template_argv=None, resource_cpu=None):
    """Freeze exact solver/model files; automatic bridges require interface evidence."""
    config = config or {}
    cadence = config.get("cadence") or {}
    simulator = selected_simulator(config, spec)
    if capture_source_simulator not in ("spectre", "alps"):
        raise SimulatorConfigurationError("Saved capture simulator is unsupported")
    if capture_source_simulator == "alps" and simulator == "spectre":
        raise SimulatorConfigurationError("ALPS capture to Spectre CLI is unqualified; use a saved Spectre source setup")
    binary = cadence.get(simulator + "_bin")
    if not binary and simulator == "alps":
        check = next((row for row in (cadence.get("discovery") or {}).get("checks") or []
                      if isinstance(row, dict) and row.get("name") == "alps_bin"), {})
        resolution = check.get("resolution") or {}
        if resolution.get("status") in ("invalid_explicit_binary", "ambiguous", "incomplete", "unavailable"):
            raise SimulatorConfigurationError(resolution.get("message") or "ALPS executable discovery did not select a unique executable")
    if not binary and simulator == capture_source_simulator:
        binary = default_executable
    if not binary and simulator == "alps" and cadence.get("alps_root"):
        binary = os.path.join(cadence["alps_root"], "bin", "alps")
    if not binary:
        raise SimulatorConfigurationError(simulator.upper() + " executable is missing")
    requested_threads = cadence.get("simulator_threads", cadence.get("threads"))
    from .execution_commands import normalize_execution_commands, solver_thread_hint
    commands = normalize_execution_commands(config, spec)
    edited_template_threads = solver_thread_hint(commands, simulator)
    resource_cpu = resource_cpu if resource_cpu is not None else config.get("_simulator_submission_cpu")
    template_threads = None
    for index, argument in enumerate(template_argv or []):
        match = re.match(r"^\+mt=([0-9]+)$", argument)
        if match:
            template_threads = int(match.group(1))
        elif argument == "-mt" and index + 1 < len(template_argv) and re.match(r"^[0-9]+$", template_argv[index + 1]):
            template_threads = int(template_argv[index + 1])
    for value in (requested_threads, resource_cpu, template_threads, edited_template_threads):
        if value is not None and (isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or int(value) != value or not 1 <= value <= 4096):
            raise SimulatorConfigurationError("Simulator threads/resource CPU must be a positive integer")
    if requested_threads is not None and resource_cpu is not None and requested_threads != resource_cpu:
        raise SimulatorConfigurationError("Simulator threads differ from the selected scheduler cpu resource")
    if edited_template_threads is not None and any(value is not None and value != edited_template_threads for value in (requested_threads, resource_cpu)):
        raise SimulatorConfigurationError("Edited solver threads differ from the selected thread/scheduler cpu setting")
    threads = requested_threads if requested_threads is not None else (resource_cpu if resource_cpu is not None else (edited_template_threads if edited_template_threads is not None else (template_threads or 1)))
    threads_source = "explicit_user" if requested_threads is not None else ("selected_submission_cpu" if resource_cpu is not None else ("selected_solver_template" if edited_template_threads is not None else ("qualified_template" if template_threads else "default_one")))
    from .simulator_profiles import (ModelProfileError, model_directory,
                                     resolve_model_mapping, selected_result_reader)
    try:
        reader = selected_result_reader(cadence, simulator)
    except ModelProfileError as error:
        raise SimulatorConfigurationError(str(error))
    directory, directory_origin = model_directory(cadence, simulator, capture_source_simulator, model_references)
    if (simulator == "alps" or simulator != capture_source_simulator) and not directory:
        raise SimulatorConfigurationError(simulator.upper() + " model directory is missing; select its actual model directory")
    if directory:
        if not isinstance(directory, str) or not os.path.isabs(directory) or not os.path.isdir(directory) or not os.access(directory, os.R_OK | os.X_OK):
            raise SimulatorConfigurationError(simulator.upper() + " model directory is unavailable")
        directory = os.path.realpath(directory)
    mapping = cadence.get(simulator + "_model_file_map") or {}
    if not isinstance(mapping, dict):
        raise SimulatorConfigurationError("Simulator model mapping must be an explicit source-to-file object")
    models = []
    for reference in model_references or []:
        source = reference.get("file")
        if source == "cr_candidate_parameters.scs":
            continue
        source_identity = _file_identity(source)
        source_section = reference.get("section")
        try:
            resolved = resolve_model_mapping(cadence, simulator, capture_source_simulator, reference, directory)
        except ModelProfileError as error:
            raise SimulatorConfigurationError(simulator.upper() + " model mapping is missing or unqualified for source model: " + source + "; " + str(error))
        target, target_section = resolved["path"], resolved["section"]
        target_identity = _file_identity(target)
        if (simulator == "alps" or simulator != capture_source_simulator) and os.path.commonpath([target_identity["realpath"], directory]) != directory:
            raise SimulatorConfigurationError("Mapped model is outside the selected simulator model directory: " + target)
        if target_section is not None and (not isinstance(target_section, str) or not re.match(r"^[A-Za-z0-9_.-]{1,128}$", target_section)):
            raise SimulatorConfigurationError("Mapped model section is invalid")
        models.append({"source": source_identity, "target": target_identity,
                       "source_section": source_section, "section": target_section,
                       "mapping_origin": resolved["mapping_origin"],
                       "mapping_evidence": resolved["mapping_evidence"]})
    if any((row.get('mapping_evidence') or {}).get('interface_kind') == 'parameter_only' for row in models):
        if not any(row.get('mapping_origin') == 'manual' or
                   (row.get('mapping_evidence') or {}).get('interface_kind') == 'model_subckt'
                   for row in models):
            raise SimulatorConfigurationError("Auxiliary parameter sections alone cannot qualify a model bundle; a model/subckt interface or explicit model mapping is required")
    contract = {"schema_version": SCHEMA, "execution_simulator": simulator,
                "capture_source_simulator": capture_source_simulator,
                "executable": _file_identity(binary, executable=True),
                "model_dir": directory, "model_dir_origin": directory_origin,
                "models": models, "threads": int(threads),
                "threads_source": threads_source, "template_threads": template_threads,
                "edited_template_threads": edited_template_threads,
                "result_reader": reader, "native_result_qualification_required": True,
                "model_equivalence_claimed": False}
    contract["fingerprint"] = _fingerprint(contract)
    return contract


def validate_execution_contract(contract, verify_files=True):
    if not isinstance(contract, dict) or contract.get("schema_version") != SCHEMA or contract.get("fingerprint") != _fingerprint(contract):
        raise SimulatorConfigurationError("Frozen simulator execution identity changed")
    if contract.get("execution_simulator") not in ("spectre", "alps") or contract.get("capture_source_simulator") not in ("spectre", "alps") or contract.get("result_reader") not in ("spectre", "alps"):
        raise SimulatorConfigurationError("Frozen simulator adapter is unsupported")
    if contract["capture_source_simulator"] == "alps" and contract["execution_simulator"] == "spectre":
        raise SimulatorConfigurationError("Frozen ALPS capture to Spectre CLI adapter is unqualified")
    threads = contract.get("threads")
    if isinstance(threads, bool) or not isinstance(threads, int) or not 1 <= threads <= 4096 or contract.get("native_result_qualification_required") is not True or contract.get("model_equivalence_claimed") is not False:
        raise SimulatorConfigurationError("Frozen simulator qualification/threads contract is invalid")
    if contract["execution_simulator"] == "alps" and not contract.get("model_dir"):
        raise SimulatorConfigurationError("Frozen ALPS model directory is missing")
    if verify_files:
        identities = [contract["executable"]]
        identities += [row[role] for row in contract.get("models") or [] for role in ("source", "target")]
        for identity in identities:
            if _file_identity(identity["path"], identity.get("executable", False)) != identity:
                raise SimulatorConfigurationError("Frozen simulator/model content changed: " + identity["path"])
        for row in contract.get("models") or []:
            evidence = row.get("mapping_evidence") or {}
            for dependency in evidence.get("source_dependencies", []) + evidence.get("target_dependencies", []):
                current = _file_identity(dependency["path"])
                if current["size"] != dependency["size"] or current["sha256"] != dependency["sha256"]:
                    raise SimulatorConfigurationError("Frozen automatic model dependency content changed: " + dependency["path"])
    return True


def inherit_native_include_dirs(argv, native_argv, simulator, model_dir=None):
    """Keep explicit paths, then append this task's missing captured -I paths.

    Captured directories were made absolute against the native task's cwd.
    They are ordinary search directories, never model-format mappings. Exact
    duplicates are removed without resolving aliases or changing their order.
    """
    result, directories = [], set()
    def append(directory):
        if directory not in directories:
            result.extend(["-I", directory] if simulator == "alps" else ["-I" + directory])
            directories.add(directory)
    for arguments, keep_other in ((argv, True), (native_argv, False)):
        original, index = list(arguments or []), 0
        while index < len(original):
            argument = original[index]
            if argument == "-I":
                if index + 1 >= len(original) or not original[index + 1]:
                    raise SimulatorConfigurationError("%s -I has no model include directory" %
                                                      ("ALPS" if simulator == "alps" else "Spectre"))
                append(original[index + 1])
                index += 2
            elif argument.startswith("-I") and len(argument) > 2:
                append(argument[2:])
                index += 1
            else:
                if keep_other:
                    result.append(argument)
                index += 1
    if model_dir:
        append(model_dir)
    return result


def ensure_model_include(argv, model_dir):
    """Spectre 18 requires -Ipath; a separate directory becomes another input."""
    return inherit_native_include_dirs(argv, [], "spectre", model_dir)


def build_simulator_argv(contract, template_argv, netlist, psf, log, ahdl):
    validate_execution_contract(contract)
    if contract["execution_simulator"] == "alps":
        # Keep ALPS's selected model directory first, then retain this Test's
        # native data/search paths. Source Spectre paths cannot replace it.
        result = [contract["executable"]["path"], netlist, "-format", "ps", "-o", psf,
                  "-I", contract["model_dir"], "-mt", str(contract["threads"])]
        return inherit_native_include_dirs(result, template_argv, "alps")
    original = list(template_argv)
    result, index = [contract["executable"]["path"], netlist], 2
    paired_drop = {"-plugin", "-ahdllibdir", "+log", "-raw"}
    while index < len(original):
        value = original[index]
        if value == "-I" and (index + 1 >= len(original) or not original[index + 1]):
            raise SimulatorConfigurationError("Spectre -I has no model include directory")
        if value in paired_drop:
            index += 2
            continue
        if value == "-inter" or value.startswith(("+mpssession=", "+mpshost=", "-raw=", "+log=")):
            index += 1
            continue
        if contract.get("threads_source") in ("explicit_user", "selected_submission_cpu") and (value == "+mt" or value.startswith("+mt=")):
            index += 2 if value == "+mt" else 1
            continue
        result.append(value)
        index += 1
    result.extend(["+log", log, "-raw", psf, "-ahdllibdir", ahdl])
    if contract.get("threads_source") in ("explicit_user", "selected_submission_cpu"):
        result.append("+mt=" + str(contract["threads"]))
    return ensure_model_include(result, contract.get("model_dir"))


def rewrite_model_references(text, contract):
    """Rewrite in one pass; all other circuit/variable bytes remain identical."""
    validate_execution_contract(contract)
    mapping, changed = {}, set()
    for row in contract.get("models") or []:
        key = (row["source"]["path"], row.get("source_section"))
        if key in mapping and mapping[key] != row:
            raise SimulatorConfigurationError("Conflicting model mapping for one source section")
        mapping[key] = row
    pattern = re.compile(r'(?m)^(\s*include\s+)"([^"\n]+)"(?:\s+section\s*=\s*("[^"\n]+"|[^\s]+))?')
    edits = []
    def replace(match):
        key = (match.group(2), (match.group(3) or "").strip('"') or None)
        row = mapping.get(key)
        if row is None:
            return match.group(0)
        changed.add(key)
        target, section = row["target"]["path"], row.get("section")
        if key == (target, section):
            return match.group(0)
        replacement = match.group(1) + '"' + target.replace('"', '\\"') + '"' + (" section=" + section if section else "")
        edits.append((match.start(), match.end(), replacement))
        return replacement
    rewritten = pattern.sub(replace, text)
    for key, row in mapping.items():
        if key not in changed and key != (row["target"]["path"], row.get("section")):
            raise SimulatorConfigurationError("Selected model reference is absent from the native template: " + key[0])
    # Applying only these exact include spans is also a byte-for-byte proof
    # that design variables, device parameters, analyses and circuit are kept.
    check, cursor = [], 0
    for start, end, replacement in edits:
        check.extend((text[cursor:start], replacement))
        cursor = end
    check.append(text[cursor:])
    if "".join(check) != rewritten:
        raise SimulatorConfigurationError("Model adaptation changed non-include netlist content")
    return rewritten


def simulator_readiness(config, spec=None, capture_source_simulator="spectre"):
    """Read-only executable/model readiness, not native acceptance or a probe."""
    try:
        contract = execution_contract(config, capture_source_simulator, spec=spec)
    except (SimulatorConfigurationError, OSError) as error:
        result = {"ready": False, "simulator": selected_simulator(config, spec), "reason": str(error)}
        if result["simulator"] == "alps":
            discovery = ((config.get("cadence") or {}).get("discovery") or {})
            check = next((row for row in discovery.get("checks") or []
                          if isinstance(row, dict) and row.get("name") == "alps_bin"), None)
            if check is not None:
                result["alps_executable_discovery"] = copy.deepcopy(check)
                result["discovery_scope"] = "path candidates only; not native solver/submission qualification"
        return result
    directory_found = bool(contract.get("model_dir"))
    message = ("Executable and selected model search directory available; exact native file/section points and PSF results still require qualification"
               if directory_found else
               "Executable available; model search directory is not detected. Actual native model files/sections and PSF results must be qualified during capture")
    return {"ready": True, "simulator": contract["execution_simulator"],
            "contract": copy.deepcopy(contract), "native_result_qualification": "required",
            "executable_ready": True, "model_directory_discovered": directory_found,
            "exact_point_models_qualified": False, "message": message}


def simulator_configuration_identity(config, spec=None):
    """Content/semantic resume guard, independent of relocatable host paths."""
    cadence = config.get("cadence") or {}
    from .simulator_profiles import selected_result_reader
    simulator = selected_simulator(config, spec)
    executable = _file_identity(cadence.get(simulator + "_bin"), executable=True)
    models = []
    mapping = cadence.get(simulator + "_model_file_map") or {}
    if not isinstance(mapping, dict):
        raise SimulatorConfigurationError("Simulator model mapping must be an object")
    for source, target in sorted(mapping.items()):
        section = target.get("section") if isinstance(target, dict) else None
        target = target.get("path") if isinstance(target, dict) else target
        model = {"source_sha256": _file_identity(source)["sha256"],
                 "target_sha256": _file_identity(target)["sha256"], "section": section}
        sections = mapping[source].get("sections") if isinstance(mapping[source], dict) else None
        if sections is not None:
            model["sections"] = copy.deepcopy(sections)
        models.append(model)
    models.sort(key=lambda row: json.dumps(row, sort_keys=True))
    payload = {"schema_version": "crescendo.simulator_configuration_identity.v1",
               "simulator": simulator, "executable_sha256": executable["sha256"],
               "threads": cadence.get("simulator_threads"),
               "result_reader": selected_result_reader(cadence, simulator),
               "models": models}
    payload["fingerprint"] = _fingerprint(payload)
    return payload
