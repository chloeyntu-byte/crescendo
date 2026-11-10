"""Frozen, shell-free solver and submission command plans.

Templates are split before their fields are expanded. A path containing spaces
therefore stays one argument, and a value containing braces is never expanded
again. Shell logic belongs in an explicitly selected executable wrapper.
"""
import copy
import hashlib
import json
import os
import re
import shlex
import string

from .simulators import (build_simulator_argv, ensure_model_include, inherit_native_include_dirs,
                         submission_cpu, validate_execution_contract, SimulatorConfigurationError)


SCHEMA = "crescendo.execution_commands.v1"
PLAN_SCHEMA = "crescendo.execution_command_plan.v1"
FIELDS = frozenset(("simulator", "netlist", "psf", "log", "model_dir", "threads",
                    "ahdl", "job_script", "cwd", "test", "corner", "point_id",
                    "job_id", "scheduler_stdout", "scheduler_stderr"))
SOLVER_FIELDS = frozenset(("simulator", "netlist", "psf", "log", "model_dir", "threads", "ahdl"))
DEFAULT_SOLVER_TEMPLATES = {
    "spectre": "{simulator} {netlist} +aps +mt={threads} -format psfxl -raw {psf} +log {log} -ahdllibdir {ahdl}",
    "alps": "{simulator} {netlist} -format ps -o {psf} -I {model_dir} -mt {threads}"}
_CWD_VARIABLE = re.compile(r"\$(?:\{(?:cwd|PWD)\}|(?:cwd|PWD)(?![A-Za-z0-9_]))")


class ExecutionCommandError(ValueError):
    def __init__(self, message, failure_stage=None, detail=None):
        super(ExecutionCommandError, self).__init__(message)
        self.detail = dict(detail or {})
        if failure_stage is not None:
            self.detail.setdefault("failure_stage", failure_stage)


def _fingerprint(value):
    value = copy.deepcopy(value)
    value.pop("fingerprint", None)
    return hashlib.sha256(json.dumps(value, sort_keys=True,
                                     separators=(",", ":")).encode("utf-8")).hexdigest()


def _template(value, label):
    if value is None:
        return ""
    if not isinstance(value, str) or "\x00" in value or "\n" in value or "\r" in value:
        raise ExecutionCommandError(label + " must be a single command template")
    if len(value) > 16384:
        raise ExecutionCommandError(label + " is too long")
    return value.strip()


def normalize_execution_commands(config=None, spec=None):
    """Missing fields inherit config; empty scheduler fields stay unconfigured."""
    configured = ((config or {}).get("runtime") or {}).get("execution_commands") or {}
    override = (spec or {}).get("execution_commands")
    for value in (configured, override):
        if value is not None and not isinstance(value, dict):
            raise ExecutionCommandError("Execution commands must be an object")
        if isinstance(value, dict):
            if set(value) - {"schema_version", "fingerprint", "solver_templates", "submission_template",
                             "scheduler_query_template", "scheduler_cancel_template"}:
                raise ExecutionCommandError("Execution command settings contain unknown fields")
            if value.get("schema_version") not in (None, SCHEMA):
                raise ExecutionCommandError("Execution command settings use an unsupported schema")
    if override and override.get("schema_version") == SCHEMA:
        validate_execution_commands(override)
        return copy.deepcopy(override)
    result = {"schema_version": SCHEMA, "solver_templates": {}, "submission_template": ""}
    configured_solvers = configured.get("solver_templates") or {}
    override_solvers = (override or {}).get("solver_templates") or {}
    if not isinstance(configured_solvers, dict) or not isinstance(override_solvers, dict):
        raise ExecutionCommandError("Solver templates must be an object")
    if set(configured_solvers) - {"spectre", "alps"} or set(override_solvers) - {"spectre", "alps"}:
        raise ExecutionCommandError("Solver templates support Spectre and ALPS")
    for simulator in ("spectre", "alps"):
        value = override_solvers[simulator] if simulator in override_solvers else configured_solvers.get(simulator)
        result["solver_templates"][simulator] = _template(value, simulator.upper() + " solver")
    value = (override or {})["submission_template"] if "submission_template" in (override or {}) else configured.get("submission_template")
    result["submission_template"] = _template(value, "Submission")
    for key in ("scheduler_query_template", "scheduler_cancel_template"):
        value = (override or {})[key] if key in (override or {}) else configured.get(key)
        result[key] = _template(value, key)
        if result[key]:
            scheduler_template_argv(result[key], key)
    # Validate syntax at freeze time, before an expensive native netlist capture.
    for simulator, template in result["solver_templates"].items():
        if template:
            unused, fields = _split_template(template)
            required = {"simulator", "netlist", "psf"} | ({"log"} if simulator == "spectre" else {"model_dir"})
            if not required.issubset(fields):
                raise ExecutionCommandError(simulator.upper() + " solver template needs " + ", ".join("{" + field + "}" for field in sorted(required)))
            if "job_script" in fields:
                raise ExecutionCommandError("Solver command cannot invoke a submission job script")
            unbound = fields - SOLVER_FIELDS
            if unbound:
                raise ExecutionCommandError("Solver template cannot use submission-only placeholders: " +
                    ", ".join("{" + field + "}" for field in sorted(unbound)), "solver_command")
    if result["submission_template"]:
        unused, fields = _split_template(result["submission_template"])
        if "job_id" in fields:
            raise ExecutionCommandError("Submission template cannot use {job_id} before submission")
        if "job_script" not in fields:
            raise ExecutionCommandError("Submission template needs {job_script}")
    result["fingerprint"] = _fingerprint(result)
    return result


def validate_execution_commands(commands):
    if not isinstance(commands, dict) or commands.get("schema_version") != SCHEMA or commands.get("fingerprint") != _fingerprint(commands):
        raise ExecutionCommandError("Frozen execution command settings changed")
    templates = commands.get("solver_templates")
    if not isinstance(templates, dict) or set(templates) != {"spectre", "alps"}:
        raise ExecutionCommandError("Frozen solver command settings are incomplete")
    # Re-normalizing the plain data also checks syntax on restored snapshots.
    plain = {"solver_templates": templates, "submission_template": commands.get("submission_template")}
    for key in ("scheduler_query_template", "scheduler_cancel_template"):
        plain[key] = commands.get(key)
    normalized = normalize_execution_commands({}, {"execution_commands": plain})
    if normalized != commands:
        raise ExecutionCommandError("Frozen command settings are not canonical")
    return True


def _reject_shell(argv):
    controls = {"|", "||", "&&", ";", "&", ">", ">>", "<", "<<"}
    if not argv or any(value in controls or re.match(r"^[0-9]*[<>]", value) for value in argv):
        raise ExecutionCommandError("Command templates accept arguments; use an executable wrapper for shell logic")
    if os.path.basename(argv[0]) in ("sh", "bash", "dash", "tcsh", "csh", "zsh", "ksh") and "-c" in argv[1:]:
        raise ExecutionCommandError("Inline shell commands are unsupported; select a wrapper file")


def _split_template(template):
    try:
        argv = shlex.split(template)
        _reject_shell(argv)
        # The target login shell uses $cwd. Bind the equivalent task directory
        # without invoking a shell; expansion still happens after tokenization.
        argv = [_CWD_VARIABLE.sub("{cwd}", argument) for argument in argv]
        fields = set()
        for argument in argv:
            for literal, field, format_spec, conversion in string.Formatter().parse(argument):
                if field is None:
                    continue
                if field not in FIELDS or format_spec or conversion:
                    raise ExecutionCommandError("Unknown or formatted command placeholder: " + str(field))
                fields.add(field)
    except ValueError as error:
        if isinstance(error, ExecutionCommandError):
            raise
        raise ExecutionCommandError("Command template quoting/braces are invalid: " + str(error))
    return argv, fields


def scheduler_template_argv(template, label="Scheduler template"):
    argv, fields = _split_template(template)
    formatter = string.Formatter()
    count = sum(field == "job_id" for argument in argv
                for literal, field, format_spec, conversion in formatter.parse(argument))
    executable_fields = [field for literal, field, format_spec, conversion in formatter.parse(argv[0])
                         if field is not None]
    if count != 1 or executable_fields:
        raise ExecutionCommandError(label + " must bind exactly one {job_id} and a literal executable")
    if fields - {"job_id", "cwd", "job_script", "test", "corner", "point_id", "threads",
                 "scheduler_stdout", "scheduler_stderr"}:
        raise ExecutionCommandError(label + " contains unsupported scheduler placeholders")
    return argv


def _render(template, values):
    argv, fields = _split_template(template)
    for field in fields:
        if values.get(field) is None:
            raise ExecutionCommandError("Command placeholder has no value: {" + field + "}")
    result = [argument.format(**values) for argument in argv]
    if any(not isinstance(argument, str) or "\x00" in argument or "\n" in argument or "\r" in argument for argument in result):
        raise ExecutionCommandError("Expanded command argument contains an invalid character")
    # Expanded values are data. They are intentionally not parsed as shell.
    return result


def solver_threads(argv, simulator):
    values = []
    for index, argument in enumerate(argv):
        if simulator == "spectre":
            match = re.match(r"^\+mt=([0-9]+)$", argument)
            flag = "+mt"
        else:
            match = re.match(r"^-mt=([0-9]+)$", argument)
            flag = "-mt"
        if match:
            values.append(int(match.group(1)))
        elif argument == flag:
            if index + 1 >= len(argv) or not re.match(r"^[0-9]+$", argv[index + 1]):
                raise ExecutionCommandError("Simulator thread argument must be a positive integer")
            values.append(int(argv[index + 1]))
        elif argument.startswith(flag + "="):
            raise ExecutionCommandError("Simulator thread argument must be a positive integer")
    if len(values) > 1 or any(not 1 <= value <= 4096 for value in values):
        raise ExecutionCommandError("Simulator thread argument is duplicated or invalid")
    return values[0] if values else None


def solver_thread_hint(commands, simulator):
    """A literal edited solver thread count takes precedence over capture Auto."""
    template = commands["solver_templates"][simulator]
    if not template:
        return None
    argv, unused = _split_template(template)
    flag = "+mt" if simulator == "spectre" else "-mt"
    for index, argument in enumerate(argv):
        value = argv[index + 1] if argument == flag and index + 1 < len(argv) else (argument[len(flag) + 1:] if argument.startswith(flag + "=") else None)
        if value == "{threads}":
            return None
    return solver_threads(argv, simulator)


def _flag_values(argv, flag, attached=False):
    result = []
    for index, argument in enumerate(argv):
        if argument == flag:
            if index + 1 >= len(argv):
                raise ExecutionCommandError("Missing argument after " + flag)
            result.append(argv[index + 1])
        elif argument.startswith(flag + "="):
            result.append(argument[len(flag) + 1:])
        elif attached and argument.startswith(flag) and len(argument) > len(flag):
            result.append(argument[len(flag):])
    return result


def solver_bindings(contract, netlist, psf, log, ahdl, **extra):
    values = {"simulator": contract["executable"]["path"], "netlist": netlist,
              "psf": psf, "log": log, "ahdl": ahdl,
              "model_dir": contract.get("model_dir"), "threads": str(contract["threads"])}
    values.update(extra)
    return values


def build_solver_command(contract, template_argv, netlist, psf, log, ahdl, commands=None):
    try:
        return _build_solver_command(contract, template_argv, netlist, psf, log, ahdl, commands)
    except ExecutionCommandError as error:
        error.detail.setdefault("failure_stage", "solver_command")
        raise
    except SimulatorConfigurationError as error:
        raise ExecutionCommandError(str(error), "solver_command")


def _build_solver_command(contract, template_argv, netlist, psf, log, ahdl, commands=None):
    validate_execution_contract(contract)
    commands = commands or normalize_execution_commands()
    validate_execution_commands(commands)
    simulator = contract["execution_simulator"]
    template = commands["solver_templates"][simulator]
    if not template:
        argv = build_simulator_argv(contract, template_argv, netlist, psf, log, ahdl)
    else:
        argv = _render(template, solver_bindings(contract, netlist, psf, log, ahdl))
        if argv[0] != contract["executable"]["path"] or argv[1:].count(netlist) != 1:
            raise ExecutionCommandError("Solver command must invoke the frozen simulator with exactly this private netlist")
        output_flag = "-raw" if simulator == "spectre" else "-o"
        if _flag_values(argv, output_flag) != [psf]:
            raise ExecutionCommandError("Solver output flag must bind exactly the owned {psf} directory")
        if simulator == "spectre":
            if _flag_values(argv, "+log") != [log]:
                raise ExecutionCommandError("Spectre +log must bind exactly the owned {log}")
            ahdl_values = _flag_values(argv, "-ahdllibdir")
            if ahdl_values and ahdl_values != [ahdl]:
                raise ExecutionCommandError("Spectre -ahdllibdir must bind the owned {ahdl}")
            if "-inter" in argv or "-plugin" in argv or any(value.startswith(("+mpssession=", "+mpshost=")) for value in argv):
                raise ExecutionCommandError("Standalone solver templates cannot use native interactive session flags")
        else:
            if _flag_values(argv, "-format") != ["ps"]:
                raise ExecutionCommandError("ALPS result qualification currently requires -format ps")
            if contract["model_dir"] not in _flag_values(argv, "-I", attached=True):
                raise ExecutionCommandError("ALPS -I must include its independently selected {model_dir}")
        # A custom command controls solver flags, but still runs this Test's
        # native circuit. Automatic builders already inherit these paths.
        argv = inherit_native_include_dirs(argv, template_argv, simulator)
    if simulator == "spectre":
        argv = ensure_model_include(argv, contract["model_dir"])
    actual_threads = solver_threads(argv, simulator)
    if actual_threads is not None and actual_threads != contract["threads"]:
        raise ExecutionCommandError("Actual solver threads differ from the frozen simulator thread setting")
    if template and actual_threads is None:
        raise ExecutionCommandError("Solver template needs an explicit thread argument (+mt= or -mt)")
    return argv


def build_solver_command_plan(contract, template_argv, netlist, psf, log, ahdl, commands=None):
    commands = copy.deepcopy(commands or normalize_execution_commands())
    argv = build_solver_command(contract, template_argv, netlist, psf, log, ahdl, commands)
    plan = {"schema_version": PLAN_SCHEMA, "layer": "solve", "argv": argv,
            "execution_simulator": contract["execution_simulator"],
            "simulator_contract_fingerprint": contract["fingerprint"],
            "commands": commands, "template": commands["solver_templates"][contract["execution_simulator"]],
            "native_argv": list(template_argv or []),
            "bindings": solver_bindings(contract, netlist, psf, log, ahdl)}
    if contract["execution_simulator"] == "spectre":
        plan["argument_encoding"] = {"include": "-I<directory>", "paths_and_search_order_preserved": True,
                                     "reason": "Spectre 18 treats a separate -I directory as a second input; joined encoding is required"}
    plan["fingerprint"] = _fingerprint(plan)
    return plan


def validate_solver_command_plan(plan, contract, task):
    if not isinstance(plan, dict) or plan.get("schema_version") != PLAN_SCHEMA or plan.get("layer") != "solve" or plan.get("fingerprint") != _fingerprint(plan):
        raise ExecutionCommandError("Frozen solver command plan changed")
    expected = build_solver_command_plan(contract, plan.get("native_argv"), task["netlist_path"],
                                         task["psf_dir"], task["spectre_log"], task.get("ahdl_dir"), plan.get("commands"))
    if expected != plan or task.get("argv") != plan["argv"]:
        raise ExecutionCommandError("Solver command plan differs from its owned task bindings")
    if task.get("execution_commands") is not None and task["execution_commands"] != plan["commands"]:
        raise ExecutionCommandError("Task command settings conflict with its solver command plan")
    return True


def build_submission_command(policy, job_script, commands=None, values=None):
    try:
        return _build_submission_command(policy, job_script, commands, values)
    except ExecutionCommandError as error:
        error.detail.setdefault("failure_stage", "submit")
        raise


def _build_submission_command(policy, job_script, commands=None, values=None):
    commands = commands or normalize_execution_commands()
    validate_execution_commands(commands)
    bindings = dict(values or {}, job_script=job_script,
                    scheduler_stdout=os.path.join(os.path.dirname(job_script), "scheduler.stdout"),
                    scheduler_stderr=os.path.join(os.path.dirname(job_script), "scheduler.stderr"))
    template = commands["submission_template"]
    remote = bool(template) or policy.get("distribution") == "command"
    if template:
        argv = _render(template, bindings)
        if argv.count(job_script) != 1:
            raise ExecutionCommandError("Submission command must invoke exactly the owned {job_script}")
    elif remote:
        prefix = list(policy.get("submit_argv") or [])
        _reject_shell(prefix)
        if any(not os.path.isabs(argument) and _CWD_VARIABLE.search(argument) for argument in prefix):
            cwd = bindings.get("cwd")
            if not isinstance(cwd, str) or not os.path.isabs(cwd):
                raise ExecutionCommandError("Submission $cwd/$PWD needs the owned task directory")
            # Absolute Job Setup paths are already concrete data; a literal
            # dollar in their filename must not trigger a second expansion.
            prefix = [argument if os.path.isabs(argument) else
                      _CWD_VARIABLE.sub(lambda match: cwd, argument) for argument in prefix]
        argv = prefix + [job_script]
    else:
        argv = ["/bin/sh", job_script]
    # Log destinations are explicit template bindings, never injected CLI flags.
    scheduler_io = None
    if remote:
        for key in ("scheduler_stdout", "scheduler_stderr"):
            if sum(argument.count(bindings[key]) for argument in argv) > 1:
                raise ExecutionCommandError("Scheduler log binding must identify one task path")
        scheduler_io = {"workdir": bindings.get("cwd")}
        for kind in ("stdout", "stderr"):
            path = bindings["scheduler_" + kind]
            bound = any(argument == path or argument.endswith("=" + path) for argument in argv)
            scheduler_io[kind] = path if bound else None
    plan = {"schema_version": PLAN_SCHEMA, "layer": "submit", "argv": argv,
            "remote": remote, "template": template, "commands_fingerprint": commands["fingerprint"],
            "bindings": bindings, "resource_cpu": submission_cpu(argv) if remote else None}
    if scheduler_io is not None:
        plan["scheduler_io"] = scheduler_io
    plan["fingerprint"] = _fingerprint(plan)
    return plan


def validate_submission_resources(plan, solver_argv, contract):
    actual_threads = solver_threads(solver_argv, contract["execution_simulator"])
    actual_threads = contract["threads"] if actual_threads is None else actual_threads
    cpu = plan.get("resource_cpu")
    if cpu is not None and cpu != actual_threads:
        raise ExecutionCommandError("Frozen simulator threads differ from actual scheduler cpu resource", "resource_contract",
                                    {"threads": actual_threads, "cpu": cpu})
    return True


def resolve_submission_command(plan, executable):
    """Freeze the executable selected by the scheduler's bounded resolver."""
    if plan.get("fingerprint") != _fingerprint(plan) or not isinstance(executable, str) or not os.path.isabs(executable):
        raise ExecutionCommandError("Submission executable resolution is invalid")
    result = copy.deepcopy(plan)
    result["requested_executable"] = result["argv"][0]
    result["argv"][0] = executable
    result["fingerprint"] = _fingerprint(result)
    return result


def command_display(argv):
    """Readable copy of argv; execution always uses the original argument list."""
    return " ".join(shlex.quote(value) for value in argv)


def submission_cpu_hint(commands, policy):
    """Use a literal selected resource to freeze Auto threads, never guess."""
    validate_execution_commands(commands)
    if not commands["submission_template"]:
        return submission_cpu(policy.get("submit_argv") or []) if policy.get("distribution") == "command" else None
    argv, unused = _split_template(commands["submission_template"])
    if not argv:
        return None
    for index, argument in enumerate(argv):
        resource = argv[index + 1] if argument == "-R" and index + 1 < len(argv) else (argument[2:] if argument.startswith("-R") and len(argument) > 2 else None)
        if resource is not None and re.search(r"(?:^|;)\s*cpu\s*=\s*\{threads\}\s*(?:;|$)", resource):
            return None
    return submission_cpu(argv)
