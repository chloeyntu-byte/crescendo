"""Native netlist capture and qualified templates for the common CLI pipeline.

A private Spectre Maestro exports netlists through an owned invocation wrapper,
which records the command and exits before solving or submitting a job. ALPS
native-addon capture remains blocked until its interception is qualified. The
numeric producer runs CDF callbacks for each candidate; the template producer
qualifies a symbolic mapping against numeric references once per run.
"""
from __future__ import print_function

import ast
import copy
import hashlib
import json
import math
import os
import re
import shlex
import shutil
import stat
import time
import uuid
import xml.etree.ElementTree as ET

from crescendo_engine.runtime import bind_text
_contract_text = bind_text()

from .cadence import child_environment, run_command
from .hierarchy import parse_physical_number
from .spectre_values import parse_spectre_number
from .rdb import parse_latest_rdb
from .variable_scope_contract import require_unique_run_variables
from .utils import atomic_write_json, atomic_write_text, ensure_dir, is_under, utc_timestamp
from .workspace import (
    CADENCE_STARTUP_LOCK, _skill_quote, _workspace_startup_load_lines,
    build_workspace, private_library_name, reset_results,
    verify_sources_unchanged, compact_source_guard, source_change_summary,
    workspace_test_design_lines, write_schematic_patch,
)

SCHEMA = _contract_text("crescendo.private_netlist.v1")
MAX_NETLIST_BYTES = 64 * 1024 * 1024
MAX_METADATA_BYTES = 32 * 1024 * 1024
SYMBOL_PREFIX = _contract_text("CRESCENDO_NL_")
PREPARATION_PHASES = ("baseline", "baseline_fallback", "symbolic", "reference_low", "reference_high", "numeric")


class PrivateNetlistError(RuntimeError):
    pass


class PrivateNetlistUnsupported(PrivateNetlistError):
    """Template reuse is unsupported; only explicitly classified native evidence permits fallback."""
    def __init__(self, message, reason="binding_unresolved", **details):
        super(PrivateNetlistUnsupported, self).__init__(message)
        self.detail = dict(details, reason=reason)


def reconcile_preparation(root, request_cancel=True, timeout_seconds=12):
    """Prove preparation captures quiescent before any resume/template reuse.

    Only known durable launch manifests establish ownership. Missing or unsafe
    evidence is retained and reported; it never licenses a broad process kill.
    """
    from .legacy_guardian import quiesce_legacy_guardian_attempt
    root = os.path.abspath(root)
    result = {"cleanup_proven": True, "root": root, "phases": []}
    if not os.path.lexists(root):
        return result
    if not os.path.isdir(root) or os.path.realpath(root) != root:
        return dict(result, cleanup_proven=False, reason="unsafe preparation root")
    try:
        from .setup_private_session import preparation_session_reference
        session_owner = preparation_session_reference(root)
        from .shared_maestro import candidate_reference
        shared = candidate_reference(root)
    except (OSError,ValueError) as error:
        return dict(result,cleanup_proven=False,reason=str(error))
    if shared:
        result["shared_workspace"] = {"root":shared["workspace_root"],"token":shared["token"],
                                      "view_cleanup":"retained_until_run_close"}
    if session_owner:
        result["setup_private_session"] = {"root":session_owner["workspace_root"],
            "session_id":session_owner["session_id"], "view_cleanup":"retained_until_ui_close"}
    for name in PREPARATION_PHASES:
        phase = os.path.join(root, name)
        if not os.path.lexists(phase):
            continue
        try:
            if not os.path.isdir(phase) or os.path.realpath(phase) != phase:
                raise PrivateNetlistError("unsafe preparation phase root")
            manifest_path = os.path.join(phase, _contract_text("legacy_guardian_launch.json"))
            if not os.path.lexists(manifest_path):
                # No launch ever occurred only if there is no process evidence.
                if any(os.path.lexists(os.path.join(phase, entry)) for entry in
                       (_contract_text("guardian_state.json"), _contract_text("command.json"), "ocean.stdout", "native_capture_ready")):
                    raise PrivateNetlistError("capture launch ownership evidence is missing")
                result["phases"].append({"name": name, "state": "not_launched"})
                continue
            _regular(manifest_path, phase, MAX_METADATA_BYTES)
            with open(manifest_path, "r", encoding="utf-8") as stream:
                manifest = json.load(stream)
            token = manifest.get("token") or ""
            if not re.match(r"^netlist-capture-[a-f0-9]{32}$", token):
                raise PrivateNetlistError("capture ownership token is invalid")
            argv = manifest.get("argv") or []
            if len(argv) != 4 or argv[1:3] != ["-nograph", "-restore"] or argv[3] != os.path.join(phase, "capture.il"):
                raise PrivateNetlistError("capture command ownership is invalid")
            expected_workspace = manifest.get("workspace_root")
            allowed_workspaces = ({session_owner["workspace_root"]} if session_owner else
                                  {shared["workspace_root"]} if shared and name == "numeric" else
                                  {os.path.join(root, "workspace"), os.path.join(root, "workspace_fallback")})
            if expected_workspace not in allowed_workspaces or (name == "baseline" and not session_owner and expected_workspace != os.path.join(root, "workspace")):
                raise PrivateNetlistError("capture workspace ownership is invalid")
            inspection = quiesce_legacy_guardian_attempt(
                phase, expected_workspace_root=expected_workspace,
                timeout_seconds=timeout_seconds, terminate_owned=bool(request_cancel))
            row = {"name": name, "state": inspection.get("state"),
                   "reason": inspection.get("reason"),
                   "termination_requested": inspection.get("termination_requested", False)}
            result["phases"].append(row)
            if inspection.get("state") != "dead":
                result["cleanup_proven"] = False
        except (OSError, ValueError, PrivateNetlistError) as error:
            result["cleanup_proven"] = False
            result["phases"].append({"name": name, "state": "ambiguous", "reason": str(error)})
    if result["cleanup_proven"] and not session_owner:
        from .same_library_views import cleanup_same_library_views
        result["view_cleanup"] = []
        for workspace_name in ("workspace", "workspace_fallback"):
            path = os.path.join(root, workspace_name, _contract_text("workspace.json"))
            if not os.path.lexists(path):
                continue
            try:
                _regular(path, root, MAX_METADATA_BYTES)
                with open(path, "r", encoding="utf-8") as stream:
                    workspace = json.load(stream)
                if workspace.get("root") != os.path.join(root, workspace_name):
                    raise PrivateNetlistError("capture workspace identity is invalid")
                cleanup = cleanup_same_library_views(workspace, process_cleanup_proven=True)
                result["view_cleanup"].append(cleanup)
                if not cleanup.get("cleanup_proven"):
                    result["cleanup_proven"] = False
            except (OSError, ValueError, PrivateNetlistError) as error:
                result["cleanup_proven"] = False
                result["view_cleanup"].append({"cleanup_proven": False, "reason": str(error)})
    return result


def _digest(path):
    result = hashlib.sha256()
    with open(path, "rb") as stream:
        while True:
            block = stream.read(1024 * 1024)
            if not block:
                break
            result.update(block)
    return result.hexdigest()


def _manifest_digest(manifest):
    payload = dict(manifest)
    payload.pop("manifest_sha256", None)
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def _regular(path, root=None, maximum=MAX_NETLIST_BYTES):
    path = os.path.abspath(path)
    if root and not is_under(os.path.realpath(path), os.path.realpath(root)):
        raise PrivateNetlistError("netlist artifact is outside its owned root")
    info = os.lstat(path)
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > maximum:
        raise PrivateNetlistError("netlist artifact is not a bounded independent regular file: %s" % path)
    if os.path.realpath(path) != path:
        raise PrivateNetlistError("netlist artifact has a symbolic-link ancestor: %s" % path)
    return path


def _text(path):
    _regular(path)
    with open(path, "r", encoding="utf-8", errors="strict") as stream:
        return stream.read()


def _retain_failed_capture_inputs(workspace, root, captures, invocation_prefix):
    """Retain bounded current-wrapper inputs as evidence, never qualification."""
    summary = {"schema_version": _contract_text("crescendo.failed_capture_inputs.v1"), "status": "unavailable",
               "qualification": "diagnostic_only", "retained_scs_paths": [],
               "retained_file_count": 0, "bytes": 0, "reasons": [],
               "include_search_contexts": [], "dependency_samples": []}
    records, files = [], []
    def reason(message):
        if len(summary["reasons"]) < 16:
            summary["reasons"].append(str(message)[:512])
    try:
        if not re.match(r"^capture\.[0-9a-f]{32}\.$", invocation_prefix):
            raise ValueError("current wrapper invocation identity is unavailable")
        root, captures = os.path.abspath(root), os.path.abspath(captures)
        if (os.path.realpath(root) != root or os.path.realpath(captures) != captures
                or os.path.dirname(captures) != root):
            raise ValueError("capture evidence directory is unsafe")
        owned = os.path.abspath(workspace["root"])
        if os.path.realpath(owned) != owned or not os.path.isdir(owned):
            raise ValueError("capture workspace directory is unsafe")
        allowed = [owned]
        try:
            native_results = os.path.abspath(capture_rdb_root(workspace))
            if os.path.realpath(native_results) == native_results:
                allowed.append(native_results)
        except (KeyError, OSError, ValueError, PrivateNetlistError) as error:
            reason("Owned native result directory unavailable: %s" % error)
        failure_root = os.path.join(root, "failure_inputs")
        if not os.path.lexists(failure_root):
            os.mkdir(failure_root, 0o700)
        if not os.path.isdir(failure_root) or os.path.realpath(failure_root) != failure_root:
            raise ValueError("failure input destination is unsafe")
        attempt = os.path.join(failure_root, invocation_prefix.split(".")[1])
        os.mkdir(attempt, 0o700)
        summary["manifest_path"] = os.path.join(attempt, _contract_text("manifest.json"))
        total_limit, file_limit, count_limit = 32 * 1024 * 1024, 8 * 1024 * 1024, 64
        def read_owned(path):
            path = os.path.abspath(path)
            if not any(is_under(path, boundary) for boundary in allowed):
                raise ValueError("outside owned input directories")
            _regular(path, maximum=file_limit)
            descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | os.O_NONBLOCK)
            try:
                before = os.fstat(descriptor)
                if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1
                        or before.st_size > file_limit):
                    raise ValueError("not a bounded independent regular input")
                if summary["bytes"] + before.st_size > total_limit:
                    raise ValueError("failure input byte budget reached")
                with os.fdopen(descriptor, "rb") as stream:
                    descriptor = None
                    data = stream.read(file_limit + 1)
                    after = os.fstat(stream.fileno())
                identity = lambda info: (info.st_dev, info.st_ino, info.st_size,
                                         getattr(info, "st_mtime_ns", info.st_mtime))
                if len(data) > file_limit or identity(before) != identity(after):
                    raise ValueError("input changed while reading")
                _regular(path, maximum=file_limit)
                if identity(os.stat(path)) != identity(after):
                    raise ValueError("input identity changed after reading")
                return data
            finally:
                if descriptor is not None:
                    os.close(descriptor)
        inspected = 0
        with os.scandir(captures) as entries:
            for entry in entries:
                inspected += 1
                if inspected > 256:
                    reason("Invocation inventory limit reached")
                    break
                if not entry.name.startswith(invocation_prefix):
                    continue
                if not re.match(re.escape(invocation_prefix) + r"[0-9]+$", entry.name):
                    reason("Malformed current-wrapper invocation filename")
                    continue
                if len(records) >= 32:
                    reason("Current-wrapper record limit reached")
                    break
                record = {"invocation": entry.name, "dependencies": []}
                records.append(record)
                try:
                    _regular(entry.path, captures, maximum=256 * 1024)
                    descriptor = os.open(entry.path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | os.O_NONBLOCK)
                    with os.fdopen(descriptor, "rb") as stream:
                        info = os.fstat(stream.fileno())
                        if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                                or info.st_size > 256 * 1024):
                            raise ValueError("wrapper record identity is unsafe")
                        raw = stream.read(256 * 1024 + 1)
                    fields = raw.split(b"\0")
                    if len(raw) > 256 * 1024 or not raw.endswith(b"\0"):
                        raise ValueError("incomplete or oversized wrapper record")
                    values = [field.decode("utf-8") for field in fields[:-1]]
                    if len(values) < 2 or not os.path.isabs(values[0]) or not values[1].endswith(".scs"):
                        raise ValueError("wrapper record lacks an absolute cwd and SCS argument")
                    cwd = os.path.abspath(values[0])
                    source = os.path.abspath(os.path.join(cwd, values[1]))
                    record.update(native_cwd=cwd, source_scs=source)
                    # These are locations from the current wrapper, not a
                    # reconstructed login environment or a model qualification.
                    directories, index, complete = [], 2, True
                    while index < len(values):
                        argument = values[index]
                        if argument == "-I":
                            index += 1
                            if index == len(values) or not values[index]:
                                complete = False
                                break
                            directory = values[index]
                        elif argument.startswith("-I") and len(argument) > 2:
                            directory = argument[2:]
                        else:
                            index += 1
                            continue
                        if len(directories) == 32:
                            complete = False
                            break
                        # Native shell-expanded arguments normally contain no
                        # variables. Never substitute this process's environment
                        # for a literal still present in the recorded argv.
                        if len(directory) > 2048 or "$" in directory or directory.startswith("~"):
                            complete = False
                        elif not any(char in directory for char in ("\r", "\n")):
                            directory = os.path.abspath(os.path.join(cwd, directory))
                            if directory not in directories:
                                directories.append(directory)
                        else:
                            complete = False
                        index += 1
                    include_context = {"native_cwd": cwd[:2048], "include_dirs": directories,
                        "search_order": "captured_cwd_then_captured_I",
                        "complete": complete and len(cwd) <= 2048,
                        "qualification": "location_evidence_only",
                        "external_files_inspected": False}
                    record["include_search_context"] = include_context
                    if len(summary["include_search_contexts"]) < 2:
                        summary["include_search_contexts"].append(include_context)
                    destination_root = os.path.join(attempt, "input_%02d" % len(records))
                    os.mkdir(destination_root, 0o700)
                    pending, seen = [(source, True)], set()
                    while pending and len(files) < count_limit:
                        path, primary = pending.pop(0)
                        if path in seen:
                            continue
                        seen.add(path)
                        try:
                            data = read_owned(path)
                            name = _contract_text("input.scs") if primary else (hashlib.sha256(path.encode("utf-8")).hexdigest()[:12]
                                    + "_" + re.sub(r"[^A-Za-z0-9_.-]", "_", os.path.basename(path))[:100])
                            destination = os.path.join(destination_root, name)
                            partial = destination + ".partial"
                            descriptor = os.open(partial, os.O_WRONLY | os.O_CREAT | os.O_EXCL
                                                 | getattr(os, "O_NOFOLLOW", 0), 0o600)
                            with os.fdopen(descriptor, "wb") as stream:
                                stream.write(data)
                                stream.flush()
                                os.fsync(stream.fileno())
                            os.rename(partial, destination)
                            files.append({"source": path, "path": destination, "bytes": len(data),
                                          "sha256": hashlib.sha256(data).hexdigest(), "primary_scs": primary})
                            summary["bytes"] += len(data)
                            if primary:
                                record["retained_scs"] = destination
                                if len(summary["retained_scs_paths"]) < 4:
                                    summary["retained_scs_paths"].append(destination)
                            text = data.decode("utf-8", "replace")
                            dependencies = re.findall(r'(?im)\b(?:include|ahdl_include)\s+"([^"\r\n]+)"|\bfile\s*=\s*"([^"\r\n]+)"', text)
                            for pair in dependencies[:32]:
                                reference = pair[0] or pair[1]
                                if len(record["dependencies"]) >= 128:
                                    reason("Failure input dependency inventory limit reached")
                                    break
                                literal = "$" not in reference and not reference.startswith("~")
                                # include/ahdl_include use the captured cwd/-I
                                # context. Retain the old source-relative hint for
                                # file= stimulus references without asserting it
                                # is an include search rule.
                                bases = ([os.path.dirname(path)] if pair[1] else [cwd] + directories)
                                candidates = ([os.path.abspath(reference)] if os.path.isabs(reference) else
                                              [os.path.abspath(os.path.join(base, reference)) for base in bases])
                                candidates = list(dict.fromkeys(candidates))
                                dependency = {"reference": reference[:512], "source": path[:2048],
                                    "path": candidates[0][:2048], "status": "external_or_unresolved_not_copied",
                                    "qualification": "diagnostic_only", "search_candidates": [],
                                    "search_complete": complete and len(candidates) <= 16}
                                selected, unresolved_predecessor = None, (not literal or
                                    (not complete and not os.path.isabs(reference)))
                                for target in candidates[:16]:
                                    # Classify lexically first: is_under() resolves
                                    # symlinks and would inspect an external PDK
                                    # candidate. read_owned/_regular still prove
                                    # the real boundary before any owned copy.
                                    internal = literal and any(target == boundary or
                                        target.startswith(boundary + os.sep) for boundary in allowed)
                                    observation = "external_not_inspected" if literal else "unexpanded_literal"
                                    if internal:
                                        try:
                                            _regular(target, maximum=file_limit)
                                            observation = "owned_regular_candidate"
                                            if selected is None and not unresolved_predecessor:
                                                selected = target
                                        except FileNotFoundError:
                                            observation = "owned_candidate_not_present"
                                        except (OSError, PrivateNetlistError):
                                            observation = "owned_candidate_not_retained"
                                            unresolved_predecessor = True
                                    else:
                                        unresolved_predecessor = True
                                    dependency["search_candidates"].append({"path": target[:2048],
                                        "scope": "owned" if internal else "external_or_unresolved",
                                        "observation": observation})
                                    if selected is not None:
                                        break
                                if selected is not None:
                                    dependency.update(path=selected[:2048], status="owned_input")
                                    if len(pending) < count_limit:
                                        pending.append((selected, False))
                                else:
                                    dependency["resolution_status"] = ("external_or_literal_search_unverified"
                                        if unresolved_predecessor or not complete else "owned_search_unresolved")
                                    if len(summary["dependency_samples"]) < 4:
                                        summary["dependency_samples"].append(dependency)
                                record["dependencies"].append(dependency)
                        except (OSError, ValueError, PrivateNetlistError) as error:
                            reason("Input not retained: %s: %s" % (path, error))
                    if pending:
                        reason("Failure input file limit reached")
                except (OSError, ValueError, UnicodeError, PrivateNetlistError) as error:
                    record["reason"] = str(error)[:512]
                    reason("Current-wrapper record skipped: %s" % error)
        summary["retained_file_count"] = len(files)
        summary["status"] = "retained" if summary["retained_scs_paths"] else "unavailable"
        atomic_write_json(summary["manifest_path"], dict(summary, records=records, files=files))
    except Exception as error:
        reason("Failure input collection incomplete: %s" % error)
    return summary


def parameter_symbols(parameters):
    result = {}
    for parameter in parameters or []:
        key = parameter.get("parameter_id")
        if not isinstance(key, str) or not key or key in result:
            raise PrivateNetlistUnsupported("selected parameter identity is missing or duplicated")
        result[key] = SYMBOL_PREFIX + hashlib.sha256(key.encode("utf-8")).hexdigest()[:20]
    if len(set(result.values())) != len(result):
        raise PrivateNetlistError("private netlist parameter symbol collision")
    return result


def _logical_statements(text, include_view=False):
    """Join native Spectre continuations, retaining library/cell evidence."""
    result, buffer, library, cell, view = [], "", None, None, None
    pending = None
    def scoped(statement):
        return (library, cell, statement) + ((view,) if include_view else ())
    lines = text.splitlines()
    for index, raw in enumerate(lines):
        line = raw.strip()
        identity = re.match(r"//\s*(Library|Cell|View) name:\s*(.*?)\s*$", line)
        if pending:
            continuation = re.match(r"//\s*([^\s:]+)\s*$", line)
            candidate = continuation.group(1) if continuation else None
            # Native wrapped identities occupy exactly the next comment line;
            # section labels and free-standing simulator comments are not names.
            following = lines[index + 1].strip() if index + 1 < len(lines) else ""
            continuation_boundary = {"Library": r"//\s*Cell name:",
                                     "Cell": r"//\s*View name:",
                                     "View": r"//\s*Inherited view list:"}
            if (candidate and not identity and candidate.lower() not in ("spectre", "simulator")
                    and re.match(continuation_boundary[pending], following)):
                if pending == "Library":
                    library = candidate
                elif pending == "Cell":
                    cell = candidate
                else:
                    view = candidate
                pending = None
                continue
            pending = None
        if identity:
            kind, value = identity.groups()
            value = value.strip() or None
            if kind == "Library":
                if buffer:
                    result.append(scoped(buffer.strip()))
                    buffer = ""
                library, cell, view = value, None, None
            elif kind == "Cell":
                cell, view = value, None
            else:
                view = value
            if value is None:
                pending = kind
            continue
        if not line or line.startswith("//"):
            continue
        if line.startswith("+"):
            if buffer:
                buffer += " " + line[1:].strip()
            elif result:
                old = result[-1]
                result[-1] = (old[0], old[1], old[2] + " " + line[1:].strip()) + old[3:]
            else:
                raise PrivateNetlistUnsupported("orphan native Spectre continuation")
            continue
        if buffer:
            buffer += " " + line
        else:
            buffer = line
        if buffer.endswith("\\"):
            buffer = buffer[:-1].rstrip()
        else:
            result.append(scoped(buffer.strip()))
            buffer = ""
    if buffer:
        result.append(scoped(buffer.strip()))
    return result


def _assignments(statement):
    """Read balanced assignment values rather than splitting expressions."""
    starts, quote, escape, depth = [], None, False, 0
    index = 0
    while index < len(statement):
        char = statement[index]
        if quote:
            if escape:
                escape = False
            elif char == "\\":
                escape = True
            elif char == quote:
                quote = None
            index += 1
            continue
        if char in ('"', "'"):
            quote = char
        elif char in "([{":
            depth += 1
        elif char in ")]}" and depth:
            depth -= 1
        elif depth == 0 and (index == 0 or statement[index - 1].isspace()):
            match = re.match(r"([A-Za-z_][A-Za-z0-9_]*)\s*=\s*", statement[index:])
            if match:
                starts.append((match.group(1), index, index + match.end()))
                index += match.end()
                continue
        index += 1
    result = []
    for position, (name, start, value_start) in enumerate(starts):
        end = starts[position + 1][1] if position + 1 < len(starts) else len(statement)
        result.append((name, statement[value_start:end].strip(), start, end))
    return result


def _space(value):
    return re.sub(r"\s+", " ", value.strip())


def _expression_value(expression, variables):
    """Bounded arithmetic only, for qualification; never Python eval()."""
    expression = expression.strip().strip('"')
    numeric = parse_spectre_number(expression)
    if numeric is not None:
        return float(numeric)
    # Spectre engineering suffixes are numbers even inside a native formula.
    pattern = r"(?<![A-Za-z0-9_\.])(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?[A-Za-z]+"
    def replace(match):
        number = parse_spectre_number(match.group(0))
        if number is None:
            raise ValueError("unknown native numeric suffix")
        return repr(float(number))
    expression = re.sub(pattern, replace, expression).replace("^", "**")
    if len(expression) > 4096:
        raise ValueError("native expression exceeds qualification limit")
    node = ast.parse(expression, mode="eval")
    count = [0]
    def visit(item):
        count[0] += 1
        if count[0] > 256:
            raise ValueError("native expression is too complex")
        if isinstance(item, ast.Expression):
            return visit(item.body)
        if isinstance(item, ast.Num) and not isinstance(item.n, bool):
            return float(item.n)
        if isinstance(item, ast.Name) and item.id in variables:
            return float(variables[item.id])
        if isinstance(item, ast.UnaryOp) and isinstance(item.op, (ast.UAdd, ast.USub)):
            value = visit(item.operand)
            return value if isinstance(item.op, ast.UAdd) else -value
        if isinstance(item, ast.BinOp):
            left, right = visit(item.left), visit(item.right)
            if isinstance(item.op, ast.Add): return left + right
            if isinstance(item.op, ast.Sub): return left - right
            if isinstance(item.op, ast.Mult): return left * right
            if isinstance(item.op, ast.Div): return left / right
            if isinstance(item.op, ast.Pow) and abs(right) <= 8: return left ** right
        if isinstance(item, ast.Call) and isinstance(item.func, ast.Name) and not item.keywords:
            functions = {"abs": abs, "sqrt": math.sqrt, "min": min, "max": max,
                         "floor": math.floor, "ceil": math.ceil}
            if item.func.id in functions and 1 <= len(item.args) <= 4:
                return functions[item.func.id](*[visit(arg) for arg in item.args])
        raise ValueError("unsupported native arithmetic expression")
    value = visit(node)
    if not math.isfinite(value):
        raise ValueError("non-finite native arithmetic expression")
    return value


def _equal_values(symbolic, reference, variables):
    if _space(symbolic) == _space(reference):
        return True
    try:
        left = _expression_value(symbolic, variables)
        right = _expression_value(reference, variables)
        return abs(left - right) <= max(abs(left), abs(right), 1e-300) * 1e-8
    except (ValueError, SyntaxError, TypeError, ZeroDivisionError, OverflowError):
        return False


def resolve_native_parameters(rows, overrides=None):
    """Resolve bounded native arithmetic, preserving unknown expressions."""
    variables = dict(overrides or {})
    result = copy.deepcopy(rows)
    pending = dict((row["name"], row["expression"]) for row in result)
    for name in list(pending):
        if name in variables:
            pending.pop(name)
    for _ in range(min(len(pending) + 1, 64)):
        advanced = False
        for name in list(pending):
            try:
                variables[name] = _expression_value(pending[name], variables)
                pending.pop(name)
                advanced = True
            except (ValueError, SyntaxError, TypeError, ZeroDivisionError, OverflowError):
                pass
        if not advanced:
            break
    for row in result:
        row["value"] = variables.get(row["name"])
        if row["value"] is None:
            row["resolution_reason"] = "unsupported_expression"
        if row["name"] in (overrides or {}):
            row["expression"] = "%.17g" % float(overrides[row["name"]])
    return result


def top_level_native_parameters(text):
    """Read this point/test's declarations; subcircuit locals never leak."""
    from .spectre_parameter_syntax import parse_parameter_declarations
    rows, scope = {}, 0
    for _library, _cell, statement in _logical_statements(text):
        if re.match(r"^subckt\s", statement):
            scope += 1
            continue
        if re.match(r"^ends(?:\s|$)", statement):
            scope = max(0, scope - 1)
            continue
        if not scope and statement.startswith("parameters "):
            for name, value in parse_parameter_declarations(statement):
                rows[name] = {"name": name, "expression": value}
    return resolve_native_parameters([rows[name] for name in sorted(rows)])


def native_netlist_identity(text):
    """Read simulator temperature and model sections from this exact netlist."""
    temperature, models, scope = None, [], 0
    for _library, _cell, statement in _logical_statements(text):
        if re.match(r"^subckt\s", statement):
            scope += 1
            continue
        if re.match(r"^ends(?:\s|$)", statement):
            scope = max(0, scope - 1)
            continue
        if scope:
            continue
        if re.match(r"^\S+\s+options\b", statement):
            for name, expression, _a, _b in _assignments(statement):
                if name == "temp":
                    temperature = {"expression": expression, "value": parse_spectre_number(expression),
                                   "source": "simulator_options"}
        match = re.match(r'^include\s+"([^"\n]+)"(?:\s+section\s*=\s*("[^"\n]+"|[^\s]+))?', statement)
        if match:
            models.append({"file": match.group(1), "section": (match.group(2) or "").strip('"') or None})
    return {"temperature": temperature, "native_models": models}


def native_capture_point_coverage(tasks, parsed, tests, schedule=None):
    """An intentional simulator exit never excuses a missing native point."""
    expected = set((row["pointID"], row["test_name"]) for row in parsed.get("test_statuses") or []
                   if row["test_name"] in tests)
    observed = set((row["point_id"], row["test"]) for row in tasks)
    points = dict((row["pointID"], row) for row in parsed.get("points") or [])
    masked = set()
    if schedule and schedule.get("read_ok"):
        # RDB contains placeholder status rows for explicitly masked pairs.
        # Exclude only a proved source group and its native per-Test mask;
        # an error status or an unknown expanded identity never excuses loss.
        source_groups = {}
        for task in tasks:
            if task.get("source_corner"):
                source_groups.setdefault(task["point_id"], set()).add(task["source_corner"])
        groups = schedule.get("groups") or {}
        native_groups = {}
        if schedule.get("members_state") == "available":
            for name, group in groups.items():
                for labels in (group.get("test_members") or {}).values():
                    for label in labels:
                        native_groups.setdefault(label, set()).add(name)
        for point, test in expected:
            names = set(source_groups.get(point, set()))
            raw = points.get(point, {}).get("corner_name")
            # Native membership is independent of later parameter checks. It
            # can identify a masked placeholder even if another point failed.
            names.update(native_groups.get(raw, set()))
            if not names and (raw in groups or raw == "_default"):
                names = {raw}
            if len(names) != 1:
                continue
            name = next(iter(names))
            group = groups.get(name)
            if ((name == "_default" and schedule.get("nominal_tests", {}).get(test) is False) or
                    (group and test in group.get("disabled_tests", []))):
                masked.add((point, test))
        expected -= masked
    counts = {}
    for row in tasks:
        key = (row["point_id"], row["test"])
        counts[key] = counts.get(key, 0) + 1

    def rows(keys):
        return [{"point_id": point, "test": test, "corner": points.get(point, {}).get("corner_name"),
                 "design_point": points.get(point, {}).get("designPointNumber")} for point, test in sorted(keys)]
    duplicates = [key for key, count in counts.items() if count > 1]
    return {"schema_version": _contract_text("crescendo.native_capture_point_coverage.v1"),
            "ok": bool(observed) and expected == observed and not duplicates,
            "expected_test_point_count": len(expected), "captured_test_point_count": len(observed),
            "missing_test_points": rows(expected - observed), "unexpected_test_points": rows(observed - expected),
            "duplicate_test_points": rows(duplicates), "masked_test_points": rows(masked), "rdb_path": parsed.get("path"),
            "scope": "all enabled native test/points; capture-only exit is expected but full coverage is mandatory"}


def qualify_symbolic_netlist(symbolic_text, references, symbols, expected_targets=None):
    """Check native structure and every assignment, including derived fields.

    This certifies the exported expression mapping and the recorded reference
    points. It does not infer arbitrary callback formulas or prove undocumented
    conditional branches throughout a PDK's full legal range.
    """
    from .spectre_parameter_syntax import parameter_declaration_spans
    def statement_pairs(statement):
        return (parameter_declaration_spans(statement) if statement.startswith("parameters ")
                else _assignments(statement))
    statements = _logical_statements(symbolic_text, include_view=True)
    symbol_names = set(symbols.values())
    if not symbol_names or not references:
        raise PrivateNetlistUnsupported("native parameterization has no identity/reference evidence")
    uses = dict((name, []) for name in symbol_names)
    from .netlist_scope import NetlistScope, BindingError
    try:
        lexical = NetlistScope(symbolic_text, symbol_names)
        all_bindings = set()
        for index, (library, cell, text, view) in enumerate(statements):
            for name, value, _a, _b in statement_pairs(text):
                number, dependencies, bindings = lexical.trace(value, lexical.scopes[index])
                if dependencies:
                    all_bindings.update(lexical.last_references)
                if text.startswith(("parameters ", "subckt ")):
                    continue
                for symbol in dependencies:
                    uses[symbol].append({"library": library, "cell": cell, "view": view,
                        "instance": text.split(None, 1)[0], "property": name, "expression": value})
        lexical.validate_instance_bindings(all_bindings)
    except BindingError as error:
        raise PrivateNetlistUnsupported(str(error), **error.detail)
    for key, symbol in symbols.items():
        if not uses[symbol]:
            raise PrivateNetlistUnsupported("native netlist does not use selected parameter %s" % key)
        target = (expected_targets or {}).get(key)
        fields = ("library", "cell", "view", "instance") if target and len(target) == 4 else ("library", "cell", "instance")
        if target and any(tuple(row[field] for field in fields) != tuple(target)
                          for row in uses[symbol]):
            raise PrivateNetlistUnsupported("native symbol escaped its selected occurrence: %s" % key, "scope_shadow")
    checks = []
    numeric_companions = []
    for reference in references:
        native = _logical_statements(reference["text"], include_view=True)
        if len(native) != len(statements):
            raise PrivateNetlistUnsupported("native callback changes model/topology/statement count", "topology_change")
        variables = dict((symbols[key], float(value)) for key, value in reference["values"].items())
        try:
            observed_scope = NetlistScope(reference["text"])
        except BindingError as error:
            raise PrivateNetlistUnsupported(str(error), **error.detail)
        assignment_count = 0
        for index, (symbolic, observed) in enumerate(zip(statements, native)):
            if symbolic[:2] != observed[:2] or symbolic[3] != observed[3]:
                raise PrivateNetlistUnsupported("native callback changes the owning cell/view scope", "scope_shadow")
            left, right = symbolic[2], observed[2]
            left_pairs, right_pairs = statement_pairs(left), statement_pairs(right)
            if left.startswith("parameters "):
                left_pairs = [row for row in left_pairs if row[0] not in symbol_names]
                right_pairs = [row for row in right_pairs if row[0] not in symbol_names]
            left_prefix = _space(left[:statement_pairs(left)[0][2]]) if statement_pairs(left) else _space(left)
            right_prefix = _space(right[:statement_pairs(right)[0][2]]) if statement_pairs(right) else _space(right)
            if left_prefix != right_prefix or [row[0] for row in left_pairs] != [row[0] for row in right_pairs]:
                raise PrivateNetlistUnsupported("native callback changes model/topology/parameter fields", "topology_change")
            for before, after in zip(left_pairs, right_pairs):
                try:
                    left_value, dependencies, bindings = lexical.trace(before[1], lexical.scopes[index], variables)
                    right_value, unused, unused_bindings = observed_scope.trace(after[1], observed_scope.scopes[index])
                except BindingError as error:
                    raise PrivateNetlistUnsupported(str(error), **error.detail)
                equal = ((not dependencies or left_value is not None and right_value is not None) and
                         (_space(before[1]) == _space(after[1]) or
                         left_value is not None and right_value is not None and
                         abs(left_value - right_value) <= max(abs(left_value), abs(right_value), 1e-300) * 1e-8))
                if not equal:
                    # Some PDK callbacks export numeric companion DPARs, not
                    # expressions. Never fit a formula or freeze the seed.
                    # Finish checking every statement/reference before granting
                    # this narrowly identified native-generation fallback.
                    numeric = False
                    try:
                        numeric = all(math.isfinite(_expression_value(value, {}))
                                      for value in (before[1], after[1]))
                    except (ValueError, SyntaxError, TypeError, ZeroDivisionError, OverflowError):
                        pass
                    if (left.startswith("parameters ") and re.fullmatch(r"DPAR_[0-9]+", before[0])
                            and not dependencies and numeric):
                        numeric_companions.append({"parameter": before[0], "reference": reference.get("name"),
                                                   "template_value": before[1], "native_value": after[1]})
                        continue
                    raise PrivateNetlistUnsupported("native derived parameter is not represented: %s/%s/%s %s" %
                                                  (symbolic[0], symbolic[1], left.split(None, 1)[0], before[0]),
                        "unsupported_expression" if left_value is None or right_value is None else "binding_unresolved",
                        expression=before[1], native_expression=after[1])
                assignment_count += 1
        checks.append({"reference": reference.get("name"), "assignment_count": assignment_count})
    if numeric_companions:
        error = PrivateNetlistUnsupported("native derived parameter is not represented: parameters %s; native generation required per candidate" %
                                          numeric_companions[0]["parameter"])
        error.detail = {"reason": "numeric_companion", "failure_stage": "native_numeric_derived_parameters", "native_generation_required": True,
                        "template_qualified": False, "numeric_derived_parameters": numeric_companions}
        raise error
    from .native_qualification import NATIVE_QUALIFICATION_REVISION
    return {"ok": True, "symbols": uses, "references": checks,
            "revision": NATIVE_QUALIFICATION_REVISION,
            "scope": "native_expression_mapping_and_recorded_reference_points",
            "range_validation_required": False}


def _without_symbol_declarations(text, symbols):
    from .spectre_parameter_syntax import parameter_declaration_spans
    names = set(symbols.values())
    result = []
    for _lib, _cell, statement in _logical_statements(text):
        if statement.startswith("parameters "):
            pairs = parameter_declaration_spans(statement)
            kept = [statement[start:end] for name, _value, start, end in pairs if name not in names]
            if kept:
                result.append("parameters " + " ".join(kept))
        else:
            result.append(statement)
    # Native comments are preserved elsewhere as qualification metadata; their
    # removal here cannot alter instance, node or subcircuit names / amap.
    text = "\n".join(result) + "\n"
    marker = "simulator lang=spectre\n"
    if marker not in text:
        raise PrivateNetlistUnsupported("native netlist has no Spectre language header")
    return text.replace(marker, marker + _contract_text('include "cr_candidate_parameters.scs"\n'), 1)


def validate_manifest(manifest, verify_files=True, require_current_qualification=True):
    if not isinstance(manifest, dict) or manifest.get("schema_version") != SCHEMA:
        raise PrivateNetlistError("private netlist manifest schema is invalid")
    root = os.path.abspath(manifest.get("root") or "")
    if not os.path.isdir(root) or os.path.realpath(root) != root:
        raise PrivateNetlistError("private netlist root is not an owned independent directory")
    if not manifest.get("qualification", {}).get("ok"):
        raise PrivateNetlistUnsupported("private netlist mapping has not been qualified")
    if require_current_qualification:
        from .native_qualification import require_current_native_qualification
        try:
            require_current_native_qualification(manifest)
        except ValueError as error:
            raise PrivateNetlistUnsupported(str(error))
    if manifest.get("manifest_sha256") != _manifest_digest(manifest):
        raise PrivateNetlistError("private netlist manifest identity changed")
    tasks = manifest.get("tasks") or []
    if not tasks or len(set(row.get("task_id") for row in tasks)) != len(tasks):
        raise PrivateNetlistError("private netlist task identities are missing or duplicated")
    for row in tasks:
        if row.get("simulator_contract"):
            from .simulators import validate_execution_contract
            validate_execution_contract(row["simulator_contract"], verify_files=verify_files)
            if row.get("solver_command_plan"):
                from .execution_commands import validate_solver_command_plan
                validate_solver_command_plan(row["solver_command_plan"], row["simulator_contract"], row)
        if not isinstance(row.get("task_id"), str) or not re.match(r"^[A-Za-z0-9_.-]{1,128}$", row["task_id"]) or row["task_id"] in (".", ".."):
            raise PrivateNetlistError("private netlist task identity is unsafe")
        for name in ("netlist_path", "cwd", "psf_dir", "spectre_log"):
            if not row.get(name) or not is_under(os.path.abspath(row[name]), root):
                raise PrivateNetlistError("private netlist task path is outside its root")
        if verify_files:
            from .shared_maestro import validate_static_file_inputs
            try:
                validate_static_file_inputs(row)
            except (OSError,ValueError) as error:
                raise PrivateNetlistError(str(error))
            for path, digest in row.get("artifact_sha256", {}).items():
                _regular(path, root=root)
                if _digest(path) != digest:
                    raise PrivateNetlistError("private netlist artifact changed: %s" % path)
            _regular(row["netlist_path"], root=root)
            if _digest(row["netlist_path"]) != row["netlist_sha256"]:
                raise PrivateNetlistError("private netlist text changed")
    return True


def compact_preparation_artifacts(manifest):
    """Remove only qualified, stopped capture workspace payloads.

    Frozen templates, numeric references, corner/CDF/readback records and all
    guardian manifests remain intact. A native include with an unresolved or
    bounded-out dependency conservatively retains payloads rather than making
    execution qualification less strict.
    """
    from .retention import _reject_unsafe_regular_tree, _fsync_directory
    root = os.path.abspath(manifest.get("root") or "")
    result = {"schema_version": _contract_text("crescendo.netlist_preparation_compaction.v1"),
              "ok": False, "artifact_cleanup_complete": False,
              "root": root, "removed": [], "retained": []}
    try:
        validate_manifest(manifest, require_current_qualification=False)
        persisted = os.path.join(root, _contract_text("private_netlist.json"))
        _regular(persisted, root, MAX_METADATA_BYTES)
        with open(persisted, "r", encoding="utf-8") as stream:
            if json.load(stream) != manifest:
                raise PrivateNetlistError("preparation manifest is not durably identical")
        proof = reconcile_preparation(root, request_cancel=False)
        result["preparation_cleanup"] = proof
        if not proof.get("cleanup_proven"):
            raise PrivateNetlistError("native preparation termination/view cleanup is unproven")
        roots = []
        for workspace_name in ("workspace", "workspace_fallback"):
            workspace_root = os.path.join(root, workspace_name)
            if not os.path.isdir(workspace_root):
                continue
            for name in ("simulation", "logs_root", "_scratch", "bin"):
                path = os.path.join(workspace_root, name)
                if os.path.lexists(path):
                    roots.append(path)
        dependencies, pending, visited, unresolved = set(), [], set(), []
        for task in manifest["tasks"]:
            pending.append((task["netlist_path"], task.get("native_cwd") or task["cwd"]))
            dependencies.update(os.path.realpath(path) for path in task.get("artifact_sha256", {}))
        include_pattern = re.compile(r'(?m)^\s*(?:include|ahdl_include|`include)\s+"([^"\n]+)"')
        include_start = re.compile(r'(?m)^\s*(?:include|ahdl_include|`include)\b')
        while pending:
            filename, native_cwd = pending.pop()
            filename = os.path.realpath(filename)
            if filename in visited:
                continue
            if len(visited) >= 4096:
                unresolved.append("native include dependency inspection exceeded its bound")
                break
            visited.add(filename)
            dependencies.add(filename)
            try:
                if os.path.getsize(filename) > MAX_NETLIST_BYTES:
                    raise PrivateNetlistError("native include is too large for bounded dependency inspection")
                with open(filename, "r", encoding="utf-8", errors="strict") as stream:
                    text = stream.read()
            except (OSError, ValueError, PrivateNetlistError) as error:
                unresolved.append("%s: %s" % (filename, error))
                continue
            # Any explicit absolute quoted template path is an owned dependency,
            # even when it is not an include statement (e.g. stimulus data).
            for quoted in re.findall(r'"([^"\n]+)"', text):
                if os.path.isabs(quoted):
                    dependencies.add(os.path.realpath(quoted))
            includes = include_pattern.findall(text)
            if len(includes) != len(include_start.findall(text)):
                unresolved.append("native include syntax could not be inspected: %s" % filename)
            for name in includes:
                if name == _contract_text("cr_candidate_parameters.scs"):
                    continue
                choices = [name] if os.path.isabs(name) else [
                    os.path.join(os.path.dirname(filename), name), os.path.join(native_cwd, name)]
                existing = [os.path.realpath(path) for path in choices if os.path.isfile(path)]
                if not existing:
                    unresolved.append("native include dependency could not be located: %s" % name)
                for dependency in existing:
                    dependencies.add(dependency)
                    pending.append((dependency, native_cwd))
        planned = []
        for path in roots:
            if unresolved or any(is_under(dependency, path) or dependency == path for dependency in dependencies):
                result["retained"].append({"path": path, "reason":
                    "native template dependency" if not unresolved else "include dependency inspection incomplete"})
                continue
            if os.path.realpath(path) != path or not is_under(path, root):
                raise PrivateNetlistError("preparation payload boundary is unsafe")
            _reject_unsafe_regular_tree(path, os.stat(root).st_dev, "native preparation payload")
            files = []
            for current, _directories, names in os.walk(path):
                for name in names:
                    artifact = os.path.join(current, name)
                    files.append({"path": os.path.relpath(artifact, root),
                                  "bytes": os.path.getsize(artifact), "sha256": _digest(artifact)})
            planned.append({"path": path, "files": files})
        result["dependency_warnings"] = unresolved[:20]
        receipt = dict(result, ok=True, manifest_sha256=manifest["manifest_sha256"],
                       planned_payloads=planned, recorded_at=utc_timestamp())
        receipt_path = os.path.join(root, _contract_text("preparation_cleanup.json"))
        atomic_write_json(receipt_path, receipt)
        _fsync_directory(root)
        for item in planned:
            for artifact in item["files"]:
                if _digest(os.path.join(root, artifact["path"])) != artifact["sha256"]:
                    raise PrivateNetlistError("preparation payload changed after durable receipt")
            shutil.rmtree(item["path"])
            _fsync_directory(os.path.dirname(item["path"]))
            result["removed"].append({"path": item["path"], "files": len(item["files"]),
                                      "bytes": sum(row["bytes"] for row in item["files"])})
        validate_manifest(manifest, require_current_qualification=False)
        result.update(ok=True, artifact_cleanup_complete=True, receipt=receipt_path)
        atomic_write_json(receipt_path, dict(receipt, outcome=result))
    except Exception as error:
        result["error"] = "%s: %s" % (error.__class__.__name__, error)
    return result


def _candidate_argv(template, netlist, psf, log, ahdl):
    if template.get("simulator_contract"):
        from .simulators import build_simulator_argv
        return build_simulator_argv(template["simulator_contract"], template["argv"], netlist, psf, log, ahdl)
    original = list(template["argv"])
    result, index = [original[0], netlist], 2
    # These are native Maestro IPC hooks, not simulator analysis settings. A
    # standalone CLI child must not attach to the previous Maestro session.
    paired_drop = {"-plugin", "-ahdllibdir", "+log", "-raw"}
    while index < len(original):
        value = original[index]
        if value in paired_drop:
            index += 2
            continue
        if value == "-inter" or value.startswith(("+mpssession=", "+mpshost=")):
            index += 1
            continue
        if value.startswith(("-raw=", "+log=")):
            index += 1
            continue
        result.append(value)
        index += 1
    result.extend(["+log", log, "-raw", psf, "-ahdllibdir", ahdl])
    return result


def materialize_candidate(manifest, values, candidate_root, candidate_id):
    validate_manifest(manifest)
    symbols = manifest["parameter_symbols"]
    if set(values) != set(symbols):
        raise PrivateNetlistError("candidate parameter identities do not match the frozen template")
    for value in values.values():
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
            raise PrivateNetlistError("candidate parameter values must be finite numbers")
    root = os.path.abspath(candidate_root)
    if os.path.lexists(root):
        if os.path.islink(root) or os.path.realpath(root) != root or os.listdir(root):
            raise PrivateNetlistError("candidate root must be a fresh independent directory")
    if is_under(root, manifest["root"]) or is_under(manifest["root"], root):
        raise PrivateNetlistError("candidate root overlaps the frozen template")
    ensure_dir(root)
    # Full OA/source guards and four native reference transcripts belong to the
    # one frozen template, never to every population member.
    result = {"schema_version": SCHEMA, "root": root, "candidate_id": str(candidate_id),
              "values": dict(values), "template_root": manifest["root"],
              "template_sha256": manifest.get("manifest_sha256"),
        "execution_selection": copy.deepcopy(manifest.get("execution_selection")),
              "parameter_symbols": dict(symbols),
              "source_fingerprint": (manifest.get("source_identity") or {}).get("fingerprint"),
              "qualification": {"ok": True, "scope": manifest.get("qualification", {}).get("scope")},
              "tasks": []}
    if manifest.get("execution_commands"):
        result["execution_commands"] = copy.deepcopy(manifest["execution_commands"])
    result["netlist_generation"] = {
        "source_mode": "private_netlist", "source": "netlist", "netlist_method": "qualified_symbolic_template", "method": "qualified_symbolic_template",
        "generation_protocol": manifest.get("netlist_generation", {}).get("generation_protocol"),
        "execution_selection": copy.deepcopy(manifest.get("execution_selection")),
        "template_sha256": manifest.get("manifest_sha256"),
        "private_identity": copy.deepcopy((manifest.get("netlist_generation") or {}).get("private_identity") or {
            key: (manifest.get("workspace") or {}).get(key) for key in
            ("root", "primary_library", "setup_cell", "setup_view", "copy_policy")}),
        "source_identity": copy.deepcopy(manifest.get("source_identity")),
        "point_count": len(manifest["tasks"]), "source_guard": copy.deepcopy(manifest.get("source_guard")),
        "parameter_application": {"ok": True, "requested": dict(values), "applied": dict(values)},
        "solver_invocations": 0, "remote_submissions": 0}
    result["netlist_generation"]["private_identity"]["template_manifest_sha256"] = manifest.get("manifest_sha256")
    if "revision" in manifest.get("qualification", {}):
        result["qualification"]["revision"] = manifest["qualification"]["revision"]
    if "qualification_revision" in manifest.get("netlist_generation", {}):
        result["netlist_generation"]["qualification_revision"] = manifest["netlist_generation"]["qualification_revision"]
    for template in manifest["tasks"]:
        task_root = ensure_dir(os.path.join(root, "tasks", template["task_id"]))
        cwd = ensure_dir(os.path.join(task_root, "netlist"))
        psf = ensure_dir(os.path.join(task_root, "psf"))
        task = copy.deepcopy(template)
        artifacts = {}
        for row in template["template_files"]:
            destination = os.path.abspath(os.path.join(cwd, row["relative_path"]))
            if not is_under(destination, cwd):
                raise PrivateNetlistError("template file relative path is unsafe")
            ensure_dir(os.path.dirname(destination))
            shutil.copyfile(row["path"], destination)
            artifacts[destination] = row["sha256"]
        parameters_path = os.path.join(cwd, _contract_text("cr_candidate_parameters.scs"))
        payload = "simulator lang=spectre\nparameters " + " ".join(
            "%s=%.17g" % (symbols[key], float(values[key])) for key in sorted(symbols)) + "\n"
        atomic_write_text(parameters_path, payload)
        artifacts[parameters_path] = _digest(parameters_path)
        netlist = os.path.join(cwd, _contract_text("input.scs"))
        simulator = (template.get("simulator_contract") or {}).get("execution_simulator", "spectre")
        log = os.path.join(psf, simulator + ".out")
        task.update(netlist_path=netlist, cwd=cwd, psf_dir=psf, spectre_log=log,
                    argv=_candidate_argv(template, netlist, psf, log, os.path.join(task_root, "ahdl")),
                    artifact_sha256=artifacts, netlist_sha256=_digest(netlist),
                    parameter_file=parameters_path, parameter_sha256=artifacts[parameters_path],
                    metadata_files=[os.path.join(cwd, row["relative_path"])
                                    for row in template["template_files"] if row.get("metadata")])
        task["execution_simulator"] = simulator
        task["capture_source_simulator"] = (template.get("simulator_contract") or {}).get("capture_source_simulator", "spectre")
        task["simulator_log"] = log
        if template.get("simulator_contract"):
            from .execution_commands import build_solver_command_plan, normalize_execution_commands
            commands = template.get("execution_commands") or manifest.get("execution_commands") or normalize_execution_commands()
            task["execution_commands"] = copy.deepcopy(commands)
            task["ahdl_dir"] = os.path.join(task_root, "ahdl")
            task["solver_command_plan"] = build_solver_command_plan(
                template["simulator_contract"], template.get("native_solver_argv") or template["argv"],
                netlist, psf, log, task["ahdl_dir"], commands=commands)
            task["argv"] = list(task["solver_command_plan"]["argv"])
        task.pop("template_files", None)
        task.pop("native_cwd", None)
        # Relative netlist/amap-to-PSF topology is retained. Old binary
        # runObjFile files are evidence only, never parsed or loaded as setup.
        task["native_parameters"] = resolve_native_parameters(template.get("native_parameters") or [],
            {symbols[key]: float(values[key]) for key in symbols})
        native_names = set(row["name"] for row in task["native_parameters"])
        task["variables"] = [copy.deepcopy(row) for row in template.get("variables") or []
                             if row.get("name") not in (set(symbols.values()) | native_names)] + [
            {"name": row["name"], "value": row["value"]} for row in task["native_parameters"] if row["value"] is not None] + [
            {"name": symbols[key], "value": float(values[key])} for key in sorted(symbols)
            if symbols[key] not in native_names]
        result["tasks"].append(task)
    result["manifest_sha256"] = _manifest_digest(result)
    atomic_write_json(os.path.join(root, _contract_text("private_netlist_candidate.json")), result)
    validate_manifest(result)
    return result


def _seeds(parameters):
    result = {}
    for parameter in parameters:
        number = parameter.get("numeric_value")
        if number is None:
            number = parse_physical_number(parameter.get("current_value"))
        if number is None or not math.isfinite(float(number)):
            raise PrivateNetlistUnsupported("selected parameter has no numeric native seed: %s" % parameter["parameter_id"])
        if any(parameter.get(name) for name in ("topology_changing", "requires_recompile", "model_selecting")):
            raise PrivateNetlistUnsupported("selected parameter changes native topology/model: %s" % parameter["parameter_id"])
        result[parameter["parameter_id"]] = float(number)
    return result


def _probe_values(parameters, seeds, sign):
    result = {}
    for index, parameter in enumerate(parameters):
        key, seed = parameter["parameter_id"], seeds[parameter["parameter_id"]]
        if parameter.get("value_type") == "integer":
            value = seed + sign * max(1, round(abs(seed) * 0.05))
        else:
            value = seed * (1 + sign * (0.05 + (index % 5) * 0.01)) if seed else sign * 0.01
        # References are not min/max legality endpoints. Keep positive-valued
        # seeded parts near their seed; candidate values remain unrestricted.
        result[key] = seed if seed > 0 and value <= 0 else value
    return result


def _native_qualification_values(parameters, spec, baseline, workspace):
    """Explicit user domain probes, otherwise proven equal native point values."""
    from .engineering import merge_catalog_domain
    from .optimizers import validate_variables, _decode
    from .workspace import _schematic_parameter_rows, _physical_values_match
    from .native_maestro_parameters import read_native_parameter_points
    declarations = dict((row["parameter_id"], row) for row in (spec or {}).get("variables") or [])
    _clones, rows = _schematic_parameter_rows(workspace, parameters,
        dict((row["parameter_id"], None) for row in parameters), clone_state="existing", render_values=False)
    for row in rows:
        row.setdefault("view", "schematic")
    observed = None
    seed, low, high, provenance = {}, {}, {}, {}
    for parameter in parameters:
        key = parameter["parameter_id"]
        if key in declarations:
            domain = validate_variables([merge_catalog_domain(declarations[key], parameter)])[0]
            values = [parse_physical_number(_decode(point, domain)) for point in (0.5, 0.0, 1.0)]
            if any(value is None for value in values):
                raise PrivateNetlistUnsupported("template qualification needs numeric user domain: " + key)
            seed[key], low[key], high[key] = values
            provenance[key] = {"source": "explicit_user_domain", "role": "qualification_probe", "baseline_value_claimed": False}
        else:
            if observed is None:
                observed = dict((row["parameter_id"], row) for row in read_native_parameter_points(rows, baseline["tasks"], workspace["setup"]))
            values = [point["effective"] for point in observed[key]["points"]]
            if not values or any(not _physical_values_match(values[0], value) for value in values):
                raise PrivateNetlistUnsupported("selected parameter differs across native points; provide one explicit qualification domain: " + key)
            seed[key] = values[0]
            provenance[key] = {"source": "all_native_points_same_value", "role": "qualification_probe", "point_count": len(values)}
    if set(low) != set(seed):
        inferred_low, inferred_high = _probe_values(parameters, seed, -1), _probe_values(parameters, seed, 1)
        for key in seed:
            low.setdefault(key, inferred_low[key])
            high.setdefault(key, inferred_high[key])
    return seed, low, high, provenance


def _native_unresolved_binding_fallback(references, symbols):
    """Recognize proven native inputs whose CDF fields have no SCS alias.

    This grants no template binding and infers no CDF formula. The exact native
    input proof must hold at every reference point; only numeric DPAR header
    values may differ. Candidate generation still uses native Run each time.
    """
    from .spectre_parameter_syntax import parameter_declaration_spans
    from .workspace import _physical_values_match
    if not references:
        return None
    first = references[0][2]
    unresolved = set(row["parameter_id"] for row in first["parameter_application"]["parameters"]
                     if any(point.get("dpar_status") == "selected_field_DPAR_binding_unresolved"
                            for point in row["points"]))
    if not unresolved:
        return None

    def reject(message):
        raise PrivateNetlistUnsupported("native selected parameter fallback is unqualified: " + message)

    if [name for name, _values, _phase in references] != ["numeric", "reference_low", "reference_high"]:
        reject("complete numeric/low/high references required")
    identities, task_identity, point_identity, normalized = None, None, None, None
    bindings, checks = [], []
    for name, values, phase in references:
        rows = phase.get("native_parameter_rows") or []
        owners = dict((row["parameter_id"], {key: row.get(key) for key in
                       ("library", "cell", "view", "instance", "property")}) for row in rows)
        if len(owners) != len(rows) or set(owners) != set(symbols):
            reject("selected native owners differ")
        if identities is not None and owners != identities:
            reject("selected native owner identity changes")
        identities = owners
        tasks = phase.get("tasks") or []
        current_tasks = dict((task["task_id"], tuple(task.get(key) for key in
            ("point_id", "design_point", "corner", "source_corner", "test"))) for task in tasks)
        if not tasks or len(current_tasks) != len(tasks) or (task_identity is not None and current_tasks != task_identity):
            reject("test/corner/point identity changes")
        task_identity = current_tasks
        task_points = set((task["point_id"], task["test"]) for task in tasks)
        if len(task_points) != len(tasks):
            reject("duplicate native test/point")
        application = phase.get("parameter_application") or {}
        parameters = application.get("parameters") or []
        if (application.get("ok") is not True or application.get("proof_kind") != "native_run_device_parameters"
                or len(parameters) != len(symbols) or set(row["parameter_id"] for row in parameters) != set(symbols)
                or set(values) != set(symbols) or set(phase.get("applied_values") or {}) != set(symbols)):
            reject("selected parameter application evidence incomplete")
        current_points, occupied, current_unresolved = {}, set(), set()
        for row in parameters:
            key = row["parameter_id"]
            points = row.get("points") or []
            seen = set()
            for point in points:
                identity = (point.get("point_id"), point.get("test"))
                if identity in seen or identity not in task_points:
                    reject("selected parameter point is missing/duplicated")
                seen.add(identity)
                if (point.get("matches_requested") is not True
                        or not _physical_values_match(values[key], point.get("effective"))
                        or not _physical_values_match(values[key], phase["applied_values"][key])):
                    reject("selected native input differs from request")
                dpars = point.get("dpar_values") or {}
                if point.get("dpar_status") == "selected_field_DPAR_binding_unresolved":
                    proof = point.get("native_input_proof") or {}
                    if (dpars or point.get("proof_source") != "full_identity_native_RDB_input"
                            or point.get("effective_kind") != "native_device_parameter_input"
                            or point.get("netlist_property") != "$native_device_input"
                            or proof.get("proof_kind") != "native_Run_selected_device_input_only"
                            or proof.get("identity") != owners[key]
                            or (proof.get("point_id"), proof.get("test")) != identity
                            or not proof.get("rdb_path")
                            or not _physical_values_match(values[key], proof.get("value"))):
                        reject("unresolved field lacks exact native RDB input proof")
                    current_unresolved.add(key)
                    bindings.append({"parameter_id": key, "reference": name,
                        "point_id": identity[0], "test": identity[1],
                        "instance": owners[key]["instance"], "name": owners[key]["property"],
                        "proof_source": point["proof_source"], "netlist_property": point["netlist_property"],
                        "dpar_names": [], "native_input_proof": copy.deepcopy(proof)})
                else:
                    if len(dpars) != 1:
                        reject("another selected field lacks a unique DPAR")
                    binding = identity + (next(iter(dpars)),)
                    if binding in occupied:
                        reject("selected DPAR binding is shared")
                    occupied.add(binding)
                current_points[(key,) + identity] = (point.get("dpar_status"), tuple(sorted(dpars)))
            if not seen:
                reject("selected parameter has no native points")
        if current_unresolved != unresolved or (point_identity is not None and current_points != point_identity):
            reject("selected parameter reference coverage/binding changes")
        point_identity = current_points
        selected_task_points = set(identity[1:] for identity in current_points)
        current_normalized = {}
        for task in tasks:
            statements, scope, declarations = [], 0, set()
            for library, cell, statement, view in _logical_statements(_text(task["netlist_path"]), include_view=True):
                if re.match(r"^subckt\s", statement):
                    scope += 1
                elif re.match(r"^ends(?:\s|$)", statement):
                    scope = max(0, scope - 1)
                # Only top-level numeric DPAR declarations are opaque CDF
                # results. Model, scope, topology and all other text stay exact.
                if (not scope and statement.startswith("parameters ")
                        and (task["point_id"], task["test"]) in selected_task_points):
                    edits = []
                    for parameter, value, start, end in parameter_declaration_spans(statement):
                        if re.fullmatch(r"DPAR_[0-9]+", parameter):
                            if parameter in declarations:
                                reject("duplicate top-level DPAR declaration")
                            declarations.add(parameter)
                            number = parse_spectre_number(value)
                            if number is None or not math.isfinite(number):
                                reject("DPAR declaration is not numeric")
                            edits.append((start, end, parameter + "=NATIVE_NUMERIC_DPAR"))
                    for start, end, replacement in reversed(edits):
                        statement = statement[:start] + replacement + statement[end:]
                statements.append((library, cell, _space(statement), view))
            current_normalized[task["task_id"]] = statements
        if normalized is not None and current_normalized != normalized:
            reject("native model/topology/non-DPAR fields change")
        normalized = current_normalized
        checks.append({"reference": name, "task_count": len(tasks), "parameter_point_count": len(current_points),
                       "scope": "exact_structure_except_top_level_numeric_DPAR_values"})
    return {"reason": "binding_unresolved", "failure_stage": "native_selected_parameter_binding", "native_generation_required": True,
            "template_qualified": False, "selected_parameter_bindings": bindings, "reference_checks": checks}


def _symbolize_native_dpar_phase(numeric, symbols, root):
    """Replace only owned DPAR declaration values; keep native PDK formulas."""
    from .spectre_parameter_syntax import parameter_declaration_spans, parameter_statement_spans
    phase = copy.deepcopy(numeric)
    by_id = dict((row["parameter_id"], row) for row in numeric["parameter_application"]["parameters"])
    for task in phase["tasks"]:
        binding = {}
        for key, symbol in symbols.items():
            points = [row for row in by_id[key]["points"]
                      if row["point_id"] == task["point_id"] and row["test"] == task["test"]]
            if not points:
                continue
            names = list(points[0]["dpar_values"])
            if len(names) != 1 or any(name == names[0] for scope, name in binding):
                raise PrivateNetlistUnsupported("native selected parameter has no unique DPAR template binding: " + key)
            evidence = points[0].get("dpar_bindings") or [{"name": names[0], "scope": []}]
            binding[(tuple(evidence[0]["scope"]), names[0])] = symbol
        target = ensure_dir(os.path.join(root, "tasks", task["task_id"], "netlist"))
        for row in task["template_files"]:
            destination = os.path.join(target, row["relative_path"])
            ensure_dir(os.path.dirname(destination))
            shutil.copyfile(row["path"], destination)
            row["path"] = destination
        text = _text(task["netlist_path"])
        replaced, edits = set(), []
        from .netlist_scope import NetlistScope
        lexical = NetlistScope(text)
        parameter_scopes = iter(lexical.scopes[index] for index, row in enumerate(lexical.statements)
                                if row[2].startswith("parameters "))
        for statement_start, _statement_end, statement in parameter_statement_spans(text):
            scope = next(parameter_scopes)
            for name, _value, start, end in parameter_declaration_spans(statement):
                key = (scope, name)
                if key in binding:
                    if key in replaced:
                        raise PrivateNetlistUnsupported("duplicate owned DPAR declaration")
                    edits.append((statement_start + start, statement_start + end,
                                  name + "=" + binding[key]))
                    replaced.add(key)
        for start, end, replacement in reversed(edits):
            text = text[:start] + replacement + text[end:]
        if replaced != set(binding):
            raise PrivateNetlistUnsupported("owned native DPAR declaration not found")
        task.update(cwd=target, netlist_path=os.path.join(target, _contract_text("input.scs")),
                    psf_dir=os.path.join(os.path.dirname(target), "psf"))
        task["spectre_log"] = os.path.join(task["psf_dir"], "spectre.out")
        atomic_write_text(task["netlist_path"], text)
        task["native_parameters"] = top_level_native_parameters(_text(task["netlist_path"]))
        task["native_dpar_template_bindings"] = {"/".join(scope + (name,)): symbol for (scope, name), symbol in binding.items()}
    return phase


def _check_cancel(cancel_check):
    if cancel_check and cancel_check():
        raise PrivateNetlistError("private netlist preparation was cancelled")


def _saved_corner_contract(path):
    """Read saved, complete corner content; a read failure is never nominal."""
    from .ade_setup import (_active_node, _children, _child, _corner_rows, _name, _enabled,
                            _variables, _option_map, _global_variables, _model_groups, _execution_settings,
                            _analysis_rows, _state_details, _merge_analyses, _disabled_test_names)
    # This is a read-only source snapshot, not an independent writable
    # candidate file. Project-library symlinks and read-only SVN source files
    # are valid; source identity guards verify their actual content.
    path = os.path.realpath(os.path.abspath(path))
    info = os.stat(path)
    if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_NETLIST_BYTES:
        raise PrivateNetlistUnsupported("saved corner setup is not a bounded regular file: %s" % path)
    try:
        root = ET.parse(path).getroot()
    except (OSError, ET.ParseError) as error:
        raise PrivateNetlistUnsupported("saved corner setup could not be read: %s: %s" % (path, error))
    active = _active_node(root)

    def content(node):
        # Retain model sections, temperature, per-corner test selection and
        # unknown native fields. Only whitespace is insignificant here.
        return {"tag": str(node.tag).rsplit("}", 1)[-1],
                "attributes": dict(sorted(node.attrib.items())),
                "text": (node.text or "").strip(),
                "children": [content(child) for child in node]}

    corners = _corner_rows(root)
    nodes = _children(_child(active, "corners"), "corner")
    for row, node in zip(corners, nodes):
        row["native_content"] = content(node)
    tests = []
    state = _state_details(os.path.join(os.path.dirname(path), "active.state"))
    disabled_tests = _disabled_test_names(root)
    for node in _children(_child(active, "tests"), "test"):
        options = _option_map(node)
        tests.append({"name": _name(node, "unnamed"), "enabled": _enabled(node) and _name(node, "unnamed") not in disabled_tests,
                      "simulator": options.get("sim") or options.get("simulator"),
                      "variables": _variables(node),
                      "analyses": sorted(_merge_analyses(_analysis_rows(node), (state.get(_name(node, "unnamed")) or {}).get("analyses", [])), key=lambda row: row["name"])})
    return {"source": "saved_setupdb_xml", "path": os.path.abspath(path),
            "sha256": _digest(path), "corner_read_ok": True,
            "mode": (_child(active, "currentmode").text or "").strip() if _child(active, "currentmode") is not None else None,
            "corners": corners, "tests": tests, "global_variables": _global_variables(root),
            "model_groups": _model_groups(root), "execution_settings": _execution_settings(root)}


def _named_rows(rows, fields):
    return sorted([{key: (sorted(row.get(key) or [], key=lambda item: item["name"]) if key == "analyses" else row.get(key)) for key in fields} for row in rows or []],
                  key=lambda row: str(row.get("name")))


def _normalize_global_dependency_defaults(rows):
    """Native save may materialize dependenttest's default enabled=1.

    Compare that one documented parser default semantically; preserve every
    explicit disabled flag, value, omission flag and unknown native field.
    """
    rows = copy.deepcopy(rows)
    for row in rows:
        content = row.get("native_content") or {}
        for group in content.get("children") or []:
            if group.get("tag") != "dependenttests":
                continue
            for child in group.get("children") or []:
                if child.get("tag") == "dependenttest":
                    child.setdefault("attributes", {}).setdefault("enabled", "1")
    return rows


def _normalize_owned_corner_parameters(rows, workspace):
    """Remove only the preceding proven Crescendo override from clone comparison."""
    from .workspace import _physical_values_match
    rows, proofs = copy.deepcopy(rows), []
    paths = set(workspace.get("native_parameter_paths") or [])
    values = workspace.get("native_parameter_last_values") or {}
    for corner in rows:
        content = corner.get("native_content") or {}
        groups = []
        for group in content.get("children") or []:
            if group.get("tag") != "parameters" or group.get("attributes") or group.get("text"):
                groups.append(group)
                continue
            locations = []
            group_changed = False
            for location in group.get("children") or []:
                if location.get("tag") != "location" or location.get("attributes"):
                    locations.append(location)
                    continue
                retained = []
                location_changed = False
                for parameter in location.get("children") or []:
                    path = str(location.get("text") or "") + "/" + str(parameter.get("text") or "")
                    children = parameter.get("children") or []
                    is_owned = (path in paths and path in values and parameter.get("tag") == "parameter"
                        and not parameter.get("attributes") and len(children) == 1
                        and children[0].get("tag") == "value" and not children[0].get("attributes")
                        and not children[0].get("children")
                        and _physical_values_match(values[path], children[0].get("text")))
                    if is_owned:
                        location_changed = True
                        group_changed = True
                        proofs.append({"corner": corner["name"], "path": path,
                                       "previous_verified_value": values[path]})
                    else:
                        retained.append(parameter)
                location["children"] = retained
                if retained or not location_changed:
                    locations.append(location)
            group["children"] = locations
            if locations or not group_changed:
                groups.append(group)
        content["children"] = groups
    return rows, proofs


def _normalize_private_corner_paths(contract, workspace, source_work_dir=None):
    """Reverse only content-verified model remaps recorded for this clone."""
    from .model_paths import _replace_path_token
    normalized, proofs = copy.deepcopy(contract), []
    setup_path = os.path.realpath(workspace.get("setup_sdb") or "")
    replacements = {}

    semantic_fields = ("corners", "tests", "global_variables", "model_groups", "execution_settings")

    def references_path(value, path):
        if isinstance(value, str):
            return _replace_path_token(value, path, "__cr_corner_path_reference__") != value
        if isinstance(value, dict):
            return any(references_path(item, path) for item in value.values())
        if isinstance(value, list):
            return any(references_path(item, path) for item in value)
        return False

    for row in (workspace.get("model_paths") or {}).get("references") or []:
        if not row.get("changed") or os.path.realpath(row.get("file") or "") != setup_path:
            continue
        original, resolved = row.get("original"), row.get("resolved")
        # Global includePath and other ADE references do not belong to the
        # corner contract. Do not require a file hash for an unrelated valid
        # directory remap merely because it shares this SetupDB file.
        nodes = [normalized.get(field) or [] for field in semantic_fields]
        if not resolved or not any(references_path(node, resolved) for node in nodes):
            continue
        if row.get("directory"):
            raise PrivateNetlistUnsupported("corner-referenced directory remap has no proven directory content identity")
        identity = row.get("target_identity") or {}
        if (not original or
                not identity.get("sha256") or not os.path.isfile(resolved) or
                os.path.getsize(resolved) != identity.get("size") or _digest(resolved) != identity["sha256"]):
            raise PrivateNetlistUnsupported("recorded private model path remap lost its content identity")
        if resolved in replacements and replacements[resolved] != original:
            raise PrivateNetlistUnsupported("private model path remap has ambiguous source identity")
        replacements[resolved] = original
        source_identity = None
        original_file = original if os.path.isabs(original) else (os.path.join(source_work_dir, original) if source_work_dir else None)
        if original_file and os.path.isfile(original_file):
            source_identity = {"size": os.path.getsize(original_file), "sha256": _digest(original_file)}
            if source_identity != {"size": identity.get("size"), "sha256": identity["sha256"]}:
                raise PrivateNetlistUnsupported("approved model path remap changes existing source file content")
        proofs.append({"original": original, "resolved": resolved, "target_identity": identity,
                       "source_identity": source_identity,
                       "source_identity_status": "verified_bytes_equal" if source_identity else "source_unavailable_approved_target_identity"})

    def normalize(value):
        if isinstance(value, str):
            for resolved, original in sorted(replacements.items(), key=lambda item: -len(item[0])):
                value = _replace_path_token(value, resolved, original)
            return value
        if isinstance(value, dict):
            return dict((key, normalize(item)) for key, item in value.items())
        if isinstance(value, list):
            return [normalize(item) for item in value]
        return value
    for field in semantic_fields:
        normalized[field] = normalize(normalized.get(field) or [])
    return normalized, proofs


def native_corner_schedule_skill(setup, path, main_sdb="mainSDB", include_disabled_tests=False, include_members=False, masks_only=False):
    """Read native group counts and test masks in the already open session."""
    lines = [
        _contract_text('crescCornerPort=outfile("%s")') % _skill_quote(path),
        _contract_text('unless(crescCornerPort error("Crescendo native corner metadata could not be written\\n"))'),
        _contract_text('foreach(crescCornerName cadr(axlGetCorners(%s))') % main_sdb,
        _contract_text('crescCornerHandle=axlGetCorner(%s crescCornerName)') % main_sdb,
        (_contract_text('crescCornerCount=nil') if masks_only else _contract_text('crescCornerCount=axlGetCornerCountForName(%s crescCornerName)') % main_sdb),
        _contract_text('printf("CRESCENDO_CORNER_OBS name=%L handle=%L count=%L\\n" crescCornerName crescCornerHandle crescCornerCount)'),
        (_contract_text('unless(crescCornerHandle && crescCornerHandle!=0 && integerp(crescCornerCount) && crescCornerCount>0 error("Crescendo native corner group could not be qualified\\n"))') if not masks_only else _contract_text('unless(crescCornerHandle && crescCornerHandle!=0 error(\"Crescendo native corner group unavailable\\n\"))')),
        (_contract_text('fprintf(crescCornerPort "GROUP\\t%L\\t%d\\t%d\\n" crescCornerName if(axlGetEnabled(crescCornerHandle) then 1 else 0) crescCornerCount)') if not masks_only else _contract_text('fprintf(crescCornerPort \"GROUP_META\\t%L\\t%d\\n\" crescCornerName if(axlGetEnabled(crescCornerHandle) then 1 else 0))')),
        _contract_text('foreach(crescDisabledTest cadr(axlGetCornerDisabledTests(crescCornerHandle))'),
        _contract_text('fprintf(crescCornerPort "DISABLED\\t%L\\t%L\\n" crescCornerName crescDisabledTest)'), ')', ')',
        _contract_text('fprintf(crescCornerPort "NOMINAL\\t%%d\\n" if(axlGetNominalCornerEnabled(%s) then 1 else 0))') % main_sdb,
    ]
    for test in setup.get("tests") or []:
        if include_disabled_tests or test.get("enabled", True):
            lines.extend([
                _contract_text('crescCornerTest=axlGetTest(%s "%s")') % (main_sdb, _skill_quote(test["name"])),
                _contract_text('unless(crescCornerTest && crescCornerTest!=0 error("Crescendo corner test identity unavailable\\n"))'),
                _contract_text('fprintf(crescCornerPort "NOMINAL_TEST\\t%%L\\t%%d\\n" "%s" if(axlGetNominalCornerTestEnabled(crescCornerTest) then 1 else 0))') % _skill_quote(test["name"]),
            ])
    if include_members:
        # Collect in memory first: incomplete optional queries never become a
        # partially trusted membership map. Restore and verify before Run.
        lines.extend([
            _contract_text("crescMembersOK=nil crescMemberRows=nil crescMemberPairs=nil"),
            "when(isCallable('axlGetCornersForATest) && isCallable('maeSetSetup)",
            _contract_text('crescMemberNames=cadr(axlGetCorners(%s))') % main_sdb,
            _contract_text('crescMemberFlags=mapcar(lambda((n) list(n axlGetEnabled(axlGetCorner(%s n)))) crescMemberNames)') % main_sdb,
            _contract_text('crescMemberNominal=axlGetNominalCornerEnabled(%s)') % main_sdb,
            _contract_text('crescMemberResult=errset(unwindProtect(progn('),
            _contract_text('foreach(crescMemberTarget crescMemberFlags'),
            _contract_text('when(cadr(crescMemberTarget)'),
            _contract_text('foreach(crescMemberRow crescMemberFlags'),
            _contract_text('unless(maeSetSetup(?corners list(car(crescMemberRow)) ?enabled equal(car(crescMemberRow) car(crescMemberTarget)) ?session session) error("Crescendo corner membership isolation failed\\n")))'),
            _contract_text('foreach(crescMemberRow crescMemberFlags unless(equal(equal(car(crescMemberRow) car(crescMemberTarget)) axlGetEnabled(axlGetCorner(%s car(crescMemberRow)))) error("Crescendo corner membership isolation readback differs\\n")))') % main_sdb,
            'axlSetNominalCornerEnabled(%s 0)' % main_sdb,
            _contract_text('when(axlGetNominalCornerEnabled(%s) error("Crescendo nominal corner isolation readback differs\\n"))') % main_sdb,
            _contract_text('crescMemberDisabled=cadr(axlGetCornerDisabledTests(axlGetCorner(%s car(crescMemberTarget))))') % main_sdb,
        ])
        for test in setup.get("tests") or []:
            if include_disabled_tests or test.get("enabled", True):
                name = _skill_quote(test["name"])
                lines.extend([
                    _contract_text('crescMemberTest="%s"') % name,
                    _contract_text('crescMemberPairs=cons(list(car(crescMemberTarget) crescMemberTest) crescMemberPairs)'),
                    _contract_text('unless(member(crescMemberTest crescMemberDisabled)'),
                    _contract_text('crescMemberQuery=errset(axlGetCornersForATest(session crescMemberTest) nil)'),
                    _contract_text('unless(crescMemberQuery && listp(car(crescMemberQuery)) && car(crescMemberQuery) error("Crescendo corner membership query unavailable\\n"))'),
                    _contract_text('foreach(crescMemberPoint car(crescMemberQuery)'),
                    _contract_text('unless(listp(crescMemberPoint) && stringp(car(crescMemberPoint)) && car(crescMemberPoint)!="" error("Crescendo corner membership entry invalid\\n"))'),
                    _contract_text('crescMemberRows=cons(list(car(crescMemberTarget) crescMemberTest car(crescMemberPoint)) crescMemberRows)))'),
                ])
        lines.extend([
            ')) t)',
            _contract_text('progn(foreach(crescMemberRow crescMemberFlags errset(maeSetSetup(?corners list(car(crescMemberRow)) ?enabled cadr(crescMemberRow) ?session session) nil))'),
            _contract_text('errset(axlSetNominalCornerEnabled(%s if(crescMemberNominal then 1 else 0)) nil))) nil)') % main_sdb,
            _contract_text('foreach(crescMemberRow crescMemberFlags unless(equal(cadr(crescMemberRow) axlGetEnabled(axlGetCorner(%s car(crescMemberRow)))) error("Crescendo corner membership restore failed; Run blocked\\n")))') % main_sdb,
            _contract_text('unless(equal(crescMemberNominal axlGetNominalCornerEnabled(%s)) error("Crescendo nominal corner restore failed; Run blocked\\n"))') % main_sdb,
            _contract_text('when(crescMemberResult crescMembersOK=t))'),
            _contract_text('if(crescMembersOK then'),
            _contract_text('foreach(crescMemberRow crescMemberPairs fprintf(crescCornerPort "MEMBER_TEST\\t%L\\t%L\\n" car(crescMemberRow) cadr(crescMemberRow)))'),
            _contract_text('foreach(crescMemberRow crescMemberRows fprintf(crescCornerPort "MEMBER\\t%L\\t%L\\t%L\\n" car(crescMemberRow) cadr(crescMemberRow) caddr(crescMemberRow)))'),
            _contract_text('fprintf(crescCornerPort "MEMBERS_DONE\\n")'),
            _contract_text('else fprintf(crescCornerPort "MEMBERS_UNAVAILABLE\\n"))'),
        ])
    lines.extend([_contract_text('fprintf(crescCornerPort "READ_OK\\n")'), _contract_text('close(crescCornerPort)')])
    return lines


def parse_native_corner_schedule(path):
    _regular(path, maximum=MAX_METADATA_BYTES)
    result = {"source": "native_setupdb_apis", "path": path, "read_ok": False,
              "groups": {}, "nominal_enabled": None, "nominal_tests": {}}
    pairs, members, member_state = set(), {}, "not_requested"
    with open(path, "r", encoding="utf-8") as stream:
        for line in stream:
            fields = line.rstrip("\n").split("\t")
            if fields[0] == "GROUP" and len(fields) == 4:
                name, enabled, count = json.loads(fields[1]), int(fields[2]), int(fields[3])
                if not isinstance(name, str) or name in result["groups"] or enabled not in (0, 1) or count < 1:
                    raise PrivateNetlistUnsupported("native corner group metadata is invalid")
                result["groups"][name] = {"enabled": bool(enabled), "count": count, "disabled_tests": []}
            elif fields[0] == "GROUP_META" and len(fields) == 3:
                name, enabled = json.loads(fields[1]), int(fields[2])
                if not isinstance(name, str) or name in result["groups"] or enabled not in (0, 1):
                    raise PrivateNetlistUnsupported("native corner group metadata is invalid")
                result["groups"][name] = {"enabled": bool(enabled), "count": None, "disabled_tests": []}
            elif fields[0] == "DISABLED" and len(fields) == 3:
                name, test = json.loads(fields[1]), json.loads(fields[2])
                if name not in result["groups"] or not isinstance(test, str):
                    raise PrivateNetlistUnsupported("native corner test mask is invalid")
                result["groups"][name]["disabled_tests"].append(test)
            elif fields[0] == "NOMINAL" and len(fields) == 2 and fields[1] in ("0", "1"):
                result["nominal_enabled"] = fields[1] == "1"
            elif fields[0] == "NOMINAL_TEST" and len(fields) == 3 and fields[2] in ("0", "1"):
                test = json.loads(fields[1])
                if not isinstance(test, str) or test in result["nominal_tests"]:
                    raise PrivateNetlistUnsupported("native nominal test metadata is invalid")
                result["nominal_tests"][test] = fields[2] == "1"
            elif fields[0] == "MEMBER_TEST" and len(fields) == 3:
                name, test = json.loads(fields[1]), json.loads(fields[2])
                if (member_state != "not_requested" or not isinstance(name, str) or name not in result["groups"] or
                        not result["groups"][name]["enabled"] or not isinstance(test, str) or not test or
                        (name, test) in pairs):
                    raise PrivateNetlistUnsupported("native corner membership test record is invalid")
                pairs.add((name, test))
            elif fields[0] == "MEMBER" and len(fields) == 4:
                name, test, label = (json.loads(field) for field in fields[1:])
                if (member_state != "not_requested" or not isinstance(name, str) or not isinstance(test, str) or (name, test) not in pairs or
                        not isinstance(label, str) or not label or (test, label) in members):
                    raise PrivateNetlistUnsupported("native corner membership is duplicated or ambiguous")
                members[(test, label)] = name
            elif fields == ["MEMBERS_DONE"] and member_state == "not_requested":
                member_state = "available"
            elif fields == ["MEMBERS_UNAVAILABLE"] and member_state == "not_requested" and not pairs and not members:
                member_state = "unavailable"
            elif fields == ["READ_OK"]:
                result["read_ok"] = True
            else:
                raise PrivateNetlistUnsupported("native corner metadata record is invalid")
    if not result["read_ok"] or result["nominal_enabled"] is None:
        raise PrivateNetlistUnsupported("native corner metadata did not complete")
    if member_state == "not_requested" and (pairs or members):
        raise PrivateNetlistUnsupported("native corner membership metadata did not complete")
    if member_state == "available":
        expected = set((name, test) for name, group in result["groups"].items() if group["enabled"]
                       for test in result["nominal_tests"])
        if pairs != expected:
            raise PrivateNetlistUnsupported("native corner membership test records are incomplete")
        for name, test in pairs:
            result["groups"][name].setdefault("test_members", {})[test] = []
        for (test, label), name in members.items():
            if test in result["groups"][name]["disabled_tests"]:
                raise PrivateNetlistUnsupported("disabled test has native corner members")
            result["groups"][name]["test_members"][test].append(label)
    result["members_state"] = member_state
    return result


def _normalize_projected_default_slot(original, full_original, private, workspace):
    """Ignore only an identical saved empty disabled nominal slot after projection."""
    selected = workspace.get(_contract_text("_cr_selected_corners"))
    if (not (workspace.get("setup") or {}).get("selected_test") or not isinstance(selected, list)
            or not selected or "_default" in selected):
        return private, None
    projected = original.get("corners") or []
    source = [row for row in full_original.get("corners") or [] if row.get("name") == "_default"]
    slots = [row for row in private.get("corners") or [] if row.get("name") == "_default"]
    if any(row.get("name") == "_default" for row in projected) or len(source) != 1 or len(slots) != 1:
        return private, None
    expected = {"name": "_default", "enabled": False, "variables": [], "model_groups": [],
                "native_content": {"tag": "corner", "text": "_default", "attributes": {"enabled": "0"}, "children": []}}
    if source[0] != expected or slots[0] != expected:
        return private, None
    without_slot = [row for row in private.get("corners") or [] if row.get("name") != "_default"]
    if sorted(without_slot, key=lambda row: row.get("name", "")) != sorted(projected, key=lambda row: row.get("name", "")):
        return private, None
    normalized = copy.deepcopy(private)
    normalized["corners"] = [row for row in normalized["corners"] if row.get("name") != "_default"]
    return normalized, {"reason": "saved empty disabled nominal slot restored by native setup after selected-corner projection",
                        "ignored_corner": copy.deepcopy(slots[0]),
                        "selected_corners": list(selected), "source_slot_identical": True,
                        "scope": "comparison only; raw source and private snapshots retained"}


def _capture_setup_identity(workspace, root, owned_symbols=None, config=None):
    """Compare the inspected snapshot, saved source and private clone."""
    setup = workspace["setup"]
    source = setup.get("source") or {}
    path = source.get("setup_db") or source.get("maestro_sdb")
    detail = {"schema_version": _contract_text("crescendo.native_corner_identity.v1"), "ok": False,
              "phase": os.path.basename(root), "inspected_source": source,
              "private_setup_path": workspace.get("setup_sdb"), "differences": []}
    selected = (config or {}).get("_selected_setup_source") or {}
    source_work_dir = None
    base_source = "unavailable"
    if selected.get("work_dir") and selected.get("setup_path"):
        if os.path.realpath(selected["setup_path"]) != os.path.realpath(path or ""):
            base_source = "selected_source_identity_mismatch"
        else:
            source_work_dir, base_source = selected["work_dir"], "captured_selected_source_work_dir"
    elif ((config or {}).get("cadence") or {}).get("project_dir"):
        source_work_dir = config["cadence"]["project_dir"]
        base_source = "explicit_cadence_project_dir"
    if source_work_dir and (not os.path.isabs(source_work_dir) or not os.path.isdir(source_work_dir)):
        source_work_dir, base_source = None, "invalid_explicit_source_work_dir"
    detail["model_path_context"] = {"source_work_dir": source_work_dir, "source": base_source,
                                    "native_capture_work_dir": workspace.get("root"),
                                    "relative_path_policy": "explicit_source_and_native_bases_with_captured_include_search"}
    diagnostic_path = os.path.join(root, _contract_text("corner_identity.json"))
    try:
        from .setup_baseline import effective_setup_path
        full_original = _saved_corner_contract(effective_setup_path(setup))
        original = full_original
        if setup.get("selected_test"):
            from .single_test import project_native_files
            projection_dir = ensure_dir(os.path.join(root, "source_projection"))
            projected_path = os.path.join(projection_dir, os.path.basename(effective_setup_path(setup)))
            shutil.copy2(effective_setup_path(setup), projected_path)
            state = os.path.join(os.path.dirname(effective_setup_path(setup)), "active.state")
            if os.path.isfile(state):
                shutil.copy2(state, os.path.join(projection_dir, "active.state"))
            manifest_path = os.path.join(projection_dir, _contract_text("manifest.json"))
            project_native_files(projected_path, setup["selected_test"], manifest_path,
                                 selected_corners=workspace.get(_contract_text("_cr_selected_corners")))
            original = _saved_corner_contract(projected_path)
            detail["source_projection_manifest"] = manifest_path
        private = _saved_corner_contract(workspace["setup_sdb"])
        detail.update(saved_source=full_original, projected_source=original, private_setup=private)
        private_comparison, remap_proofs = _normalize_private_corner_paths(private, workspace, source_work_dir=source_work_dir)
        detail["verified_model_path_normalizations"] = remap_proofs
        from .corner_selection import normalize_previous_selection
        private_comparison, selection_proof = normalize_previous_selection(
            original, private_comparison, workspace.get("_native_corner_selection_applied"))
        if selection_proof:
            detail["owned_corner_selection_normalization"] = selection_proof
        from .test_selection import normalize_previous_selection as normalize_test_selection
        private_comparison, execution_proof = normalize_test_selection(
            original, private_comparison, workspace.get("_native_execution_selection_applied"))
        if execution_proof:
            detail["owned_execution_selection_normalization"] = execution_proof
        private_comparison["corners"], owned_proofs = _normalize_owned_corner_parameters(private_comparison["corners"], workspace)
        detail["owned_native_parameter_normalizations"] = owned_proofs
        private_comparison, default_slot_proof = _normalize_projected_default_slot(
            original, full_original, private_comparison, workspace)
        if default_slot_proof:
            detail["projected_default_slot_normalization"] = default_slot_proof
        owned_symbols = set(owned_symbols or [])
        source_variables = set(row["name"] for test in full_original["tests"] for row in test["variables"])
        source_variables.update(row["name"] for corner in full_original["corners"] for row in corner["variables"])
        source_variables.update(row["name"] for row in full_original.get("global_variables") or [])
        if source_variables & owned_symbols:
            raise PrivateNetlistUnsupported("private template symbol collides with an original design variable")
        if source.get("sha256") and source["sha256"] != full_original["sha256"]:
            detail["differences"].append({"field": "source_sha256", "reason": "inspected_source_is_stale",
                                          "expected": source["sha256"], "actual": full_original["sha256"]})
        for field, keys in (("corners", ("name", "enabled", "variables")),
                            ("tests", ("name", "enabled", "simulator", "variables", "analyses"))):
            inspected = _named_rows(setup.get("_source_tests") if field == "tests" and setup.get("_source_tests") is not None else setup.get(field), keys)
            saved = _named_rows(full_original[field], keys)
            if inspected != saved:
                detail["differences"].append({"field": "inspected_" + field,
                                              "reason": "inspected_source_is_stale_or_incomplete",
                                              "expected": inspected, "actual": saved})
        for field in ("global_variables", "model_groups", "execution_settings"):
            # Older callers can omit empty new fields, never nonempty saved
            # scope/sweep/model/run-plan settings.
            inspected = (setup.get("_source_global_variables") if field == "global_variables"
                         and setup.get("_source_global_variables") is not None else setup.get(field)) or []
            if inspected != full_original[field]:
                detail["differences"].append({"field": "inspected_" + field,
                                              "reason": "inspected_source_is_stale_or_incomplete",
                                              "expected": inspected, "actual": full_original[field]})
        for field in ("mode", "tests", "corners", "global_variables", "model_groups", "execution_settings"):
            expected, actual = original[field], private_comparison[field]
            if field == "tests":
                actual = copy.deepcopy(actual)
                for row in actual:
                    # Native Maestro records variables discovered in the
                    # parameterized schematic. Ignore only this preparation's
                    # exact owned symbols, never a user-variable prefix.
                    row["variables"] = [variable for variable in row["variables"]
                                        if variable["name"] not in owned_symbols]
            if field == "global_variables":
                actual = [row for row in actual if row["name"] not in owned_symbols]
                expected = _normalize_global_dependency_defaults(expected)
                actual = _normalize_global_dependency_defaults(actual)
            if field in ("tests", "corners", "global_variables", "model_groups"):
                expected = sorted(expected, key=lambda row: row["name"])
                actual = sorted(actual, key=lambda row: row["name"])
            if expected != actual:
                detail["differences"].append({"field": "private_" + field,
                                              "reason": "private_setup_clone_changed_semantics",
                                              "expected": expected, "actual": actual})
        if detail["differences"]:
            raise PrivateNetlistUnsupported("saved/private corner setup differs from the inspected snapshot")
        detail["ok"] = True
    except (OSError, TypeError, ValueError, PrivateNetlistError) as error:
        detail.update(ok=False, error="%s: %s" % (error.__class__.__name__, error))
        atomic_write_json(diagnostic_path, detail)
        failure = PrivateNetlistUnsupported("native corner setup qualification failed; see %s" % diagnostic_path)
        failure.detail = {"failure_stage": "native_corner_setup_identity", "diagnostic_path": diagnostic_path,
                          "corner_identity": detail, "execution_started": False}
        raise failure
    atomic_write_json(diagnostic_path, detail)
    return detail


def validate_capture_corners(setup, tasks, parsed, snapshot=None, schedule=None, strict_sweep_multiplicity=None):
    """Check test-scoped coverage, preserving explicit nominal/default names."""
    from .rdb import DEFAULT_CORNER_NAME, normalize_corner_name
    from .native_identity import qualify_expansion, point_variable_checks, sweep_coverage, execution_plan_variables
    tests = [row["name"] for row in setup.get("tests") or [] if row.get("enabled", True)]
    proven_aliases = []
    enabled = [row for row in setup.get("corners") or [] if row.get("enabled", True)]
    expected_names = [normalize_corner_name(row.get("name")) for row in enabled]
    duplicates = sorted(set(name for name in expected_names if expected_names.count(name) > 1))
    detail = {"schema_version": _contract_text("crescendo.native_corner_coverage.v1"), "ok": True,
              "enabled_tests": tests, "explicit_enabled_corners": expected_names,
              "disabled_corners": [row.get("name") for row in setup.get("corners") or [] if not row.get("enabled", True)],
              "duplicate_source_corners": duplicates, "test_differences": [],
              "rdb_path": parsed.get("path"), "snapshot_path": (snapshot or {}).get("private_setup_path")}
    if duplicates or len(set(tests)) != len(tests):
        detail["ok"] = False
    if schedule is not None:
        source_enabled = sorted(set(name for name in expected_names if name != DEFAULT_CORNER_NAME))
        native_enabled = sorted(name for name, group in (schedule.get("groups") or {}).items()
                                if group.get("enabled") and name != DEFAULT_CORNER_NAME)
        detail["native_group_enabled_difference"] = {"expected": source_enabled, "actual": native_enabled}
        if source_enabled != native_enabled or not schedule.get("read_ok"):
            detail["ok"] = False
        default_row = next((row for row in setup.get("corners") or [] if row.get("name") == DEFAULT_CORNER_NAME), None)
        if default_row is not None and bool(default_row.get("enabled", True)) != bool(schedule.get("nominal_enabled")):
            detail["ok"] = False
            detail["native_nominal_difference"] = {"expected": bool(default_row.get("enabled", True)),
                                                     "actual": schedule.get("nominal_enabled")}
    for test in tests:
        rows = [row for row in tasks if row["test"] == test]
        actual = sorted(set(row["corner"] for row in rows))
        # Empty is accepted only after the saved source and clone were read
        # successfully. RDB NULL/empty names already normalize to _default;
        # named 'nominal' is never treated as this implicit point.
        implicit = not enabled
        expected = [DEFAULT_CORNER_NAME] if implicit else sorted(set(expected_names))
        mapping, mapping_error, member_difference = {}, None, None
        active_groups = None
        expansion = {"counts": {}, "proofs": [], "contracts": {}, "errors": []}
        if schedule is not None:
            if not schedule.get("read_ok"):
                mapping_error = "native_corner_schedule_read_failed"
            groups = schedule.get("groups") or {}
            active_groups = dict((name, row) for name, row in groups.items()
                                 if name != DEFAULT_CORNER_NAME and row.get("enabled") and test not in row.get("disabled_tests", []))
            expected = sorted(active_groups)
            if schedule.get("nominal_enabled") and schedule.get("nominal_tests", {}).get(test):
                expected.append(DEFAULT_CORNER_NAME)
            if not enabled and not groups and schedule.get("nominal_enabled") and schedule.get("nominal_tests", {}).get(test):
                implicit = True
            member_difference = None
            if schedule.get("members_state") == "available":
                native_members = {}
                for name, group in active_groups.items():
                    for label in group["test_members"][test]:
                        native_members[label] = name
                if DEFAULT_CORNER_NAME in expected:
                    native_members[DEFAULT_CORNER_NAME] = DEFAULT_CORNER_NAME
                member_difference = {"expected": sorted(native_members), "actual": actual,
                                     "missing": sorted(set(native_members) - set(actual)),
                                     "unexpected": sorted(set(actual) - set(native_members))}
                mapping = {label: native_members[label] for label in actual if label in native_members}
                if member_difference["missing"] or member_difference["unexpected"]:
                    mapping_error = "native_corner_member_set_mismatch"
            else:
                for name in actual:
                    if name in expected:
                        mapping[name] = name
            expansion = qualify_expansion(setup, rows, active_groups, mapping, snapshot)
            effective_counts = dict((name, expansion["counts"].get(name, group["count"])) for name, group in active_groups.items())
            unmatched = [name for name in actual if name not in mapping]
            expanded = [name for name, group in active_groups.items()
                        if effective_counts[name] > sum(value == name for value in mapping.values())]
            if unmatched:
                # Native group counts + a unique effective group establish
                # membership. Never strip an underscore or numeric suffix.
                # Multiple expanded groups need stronger native provenance.
                if len(expanded) == 1 and active_groups[expanded[0]]["count"] > 1 and expanded[0] not in expansion["contracts"]:
                    group = expanded[0]
                    missing_count = active_groups[group]["count"] - sum(value == group for value in mapping.values())
                    if len(unmatched) == missing_count:
                        mapping.update(dict((name, group) for name in unmatched))
                    else:
                        mapping_error = "native_expanded_corner_count_mismatch"
                else:
                    mapping_error = "native_expanded_corner_group_mapping_is_ambiguous"
            for name, group in active_groups.items():
                if sum(value == name for value in mapping.values()) != effective_counts[name]:
                    mapping_error = mapping_error or "native_expanded_corner_count_mismatch"
            if expansion["errors"]:
                mapping_error = mapping_error or expansion["errors"][0]
        else:
            mapping = dict((name, name) for name in actual if name in expected)
        mapped_actual = sorted(set(mapping.values()))
        missing = sorted(set(expected) - set(mapped_actual))
        unexpected = [name for name in actual if name not in mapping]
        point_keys = [(row["point_id"], row["test"]) for row in rows]
        repeated = sorted(set(point for point in point_keys if point_keys.count(point) > 1))
        if implicit and not ((snapshot or {}).get("ok") and
                             (snapshot.get("saved_source") or {}).get("corner_read_ok") and
                             (snapshot.get("private_setup") or {}).get("corner_read_ok")):
            missing = expected
        point_counts = dict((name, sum(row["corner"] == name for row in rows)) for name in actual)
        variable_mismatches, point_checks = [], []
        test_row = next(row for row in setup["tests"] if row["name"] == test)
        plan_details = execution_plan_variables(setup, test)
        for task in rows:
            if schedule is not None and task["corner"] not in mapping:
                point_checks.append({"point_id": task["point_id"], "corner": task["corner"],
                                     "evaluation": "blocked_by_corner_mapping"})
                continue
            source_corner = mapping.get(task["corner"], task["corner"])
            corner = next((row for row in enabled if normalize_corner_name(row.get("name")) == source_corner), {})
            mismatches, checks = point_variable_checks(setup, test_row, corner, task, parsed, snapshot)
            variable_mismatches.extend(mismatches)
            point_checks.extend(checks)
        modern_scope = all(key in setup for key in ('global_variables', 'model_groups', 'execution_settings'))
        strict_grid = modern_scope if strict_sweep_multiplicity is None else strict_sweep_multiplicity
        sweep_differences = sweep_coverage(setup, test_row, dict((row["name"], row) for row in enabled), rows, mapping,
                                           strict_multiplicity=strict_grid)
        detail["test_differences"].append({"test": test, "expected": expected, "actual": actual,
                                            "missing": missing, "unexpected": unexpected,
                                            "duplicate_points": repeated,
                                            "point_counts": point_counts,
                                            "native_to_source_corner": mapping,
                                            "mapping_error": mapping_error,
                                            "mapping_evidence": "native_test_corner_members" if member_difference is not None else "saved_identity_and_native_counts",
                                            "native_member_difference": member_difference,
                                            "variable_mismatches": variable_mismatches,
                                            "point_identity_checks": point_checks,
                                            "unverified_variable_check_count": sum(check.get("ok") is None and "variable" in check for check in point_checks),
                                            "execution_plan": plan_details,
                                            "corner_expansion": expansion,
                                            "sweep_differences": sweep_differences,
                                            "implicit_corner": implicit,
                                            "netlists": [row.get("netlist_path") for row in rows]})
        if missing or unexpected or repeated or variable_mismatches or mapping_error or sweep_differences or plan_details["errors"]:
            detail["ok"] = False
        if not (missing or unexpected or repeated or variable_mismatches or mapping_error or sweep_differences or plan_details["errors"]):
            for row in rows:
                proven_aliases.append((row, mapping[row["corner"]]))
    unexpected_tests = sorted(set(row["test"] for row in tasks) - set(tests))
    detail["unexpected_tests"] = unexpected_tests
    if unexpected_tests or not tests:
        detail["ok"] = False
    if detail["ok"]:
        for row, source_corner in proven_aliases:
            row["source_corner"] = source_corner
    return detail


def qualify_native_corner_mapping(setup, tasks, parsed, snapshot, schedule):
    """Prove native group/point coverage only, for an independent EDA verifier.

    This does not replace model, netlist, variable, PSF or output identity
    checks. Callers must already verify the saved/private setup snapshot and
    retain their native variable checks. Private-netlist capture uses the
    stronger ``validate_capture_corners`` including native constants.
    """
    groups = copy.deepcopy(setup)
    for row in groups.get("tests") or []:
        row.pop("variables", None)
    # Corner variables/model groups establish expanded membership. Removing
    # them turns a native identity proof back into a bare name/count guess.
    # Global/test constant validation remains with the independent verifier.
    groups["global_variables"] = []
    groups["execution_settings"] = []
    detail = validate_capture_corners(groups, tasks, parsed, snapshot, schedule, strict_sweep_multiplicity=False)
    detail["scope"] = "native_test_corner_group_and_actual_point_coverage"
    return detail


def _qualify_setup_capture(setup, capture_source_simulator="spectre"):
    if capture_source_simulator == "alps":
        error = PrivateNetlistUnsupported(
            "Saved ALPS Maestro netlist capture is unqualified: its native addon may bypass the owned capture wrapper; use a saved Spectre source with the ALPS CLI solver and a verified ALPS model profile")
        error.detail = {"failure_stage": "native_netlist_capture_adapter", "capture_source_simulator": "alps",
                        "execution_started": False, "cleanup_proven": True,
                        "reason": "unqualified_native_alps_capture_interception"}
        raise error
    supported_modes = ("Single Run, Sweeps and Corners", "Single Run, Sweeps, and Corners", "SRSC")
    if setup.get("mode") and setup["mode"] not in supported_modes:
        raise PrivateNetlistUnsupported("this Maestro run mode requires the original execution mode")
    for test in setup.get("tests") or []:
        if test.get("enabled", True) and (test.get("simulator") or "spectre") != capture_source_simulator:
            raise PrivateNetlistUnsupported("private netlist capture requires matching saved %s tests" % capture_source_simulator)
    if capture_source_simulator not in ("spectre", "alps"):
        raise PrivateNetlistUnsupported("saved simulator requires a qualified native capture adapter")
    source = setup.get("source") or {}
    from .setup_baseline import effective_setup_path
    path = effective_setup_path(setup)
    if not path or not os.path.isfile(path) or os.stat(path).st_size > MAX_NETLIST_BYTES:
        raise PrivateNetlistUnsupported("source Maestro setup cannot be qualified for native capture")
    try:
        with open(path, "rb") as stream:
            source_bytes = stream.read(MAX_NETLIST_BYTES + 1)
        if len(source_bytes) > MAX_NETLIST_BYTES:
            raise PrivateNetlistUnsupported("source Maestro setup cannot be read for native capture")
        tree = ET.fromstring(source_bytes)
    except (OSError, ET.ParseError):
        raise PrivateNetlistUnsupported("source Maestro setup cannot be read for native capture")
    from .setup_scripts import inspect_setup_script_hooks, is_native_disabled_prerun_switch
    scripts = inspect_setup_script_hooks(tree)
    active = next((child for child in tree if str(child.tag).rsplit("}", 1)[-1].lower() == "active"), tree)
    guard_hits = []

    def saved_guard(node, location):
        tag = str(node.tag).rsplit("}", 1)[-1].lower().replace("_", "")
        if (re.match(r"^(?:pre|post|user|eval).*script", tag)
                and not is_native_disabled_prerun_switch(node)
                and (list(node) or (node.text or "").strip())):
            if len(guard_hits) < 64:
                guard_hits.append({"xml_path": location, "tag": tag,
                                   "child_count": len(list(node)), "nonempty_text": bool((node.text or "").strip())})
        counts = {}
        for child in list(node):
            child_tag = str(child.tag).rsplit("}", 1)[-1].lower()
            counts[child_tag] = counts.get(child_tag, 0) + 1
            saved_guard(child, "%s/%s[%d]" % (location, child_tag, counts[child_tag]))

    saved_guard(active, "/" + str(active.tag).rsplit("}", 1)[-1].lower())
    # The observed leaf prerunscript=0 is ADE's separate disabled switch.
    # All file/code payloads and unknown structures retain the saved guard;
    # general empty/disabled observations do not grant qualification.
    if guard_hits or scripts["blocked"]:
        blocked = [row["xml_path"] for row in guard_hits] or [row["xml_path"] for row in scripts["checks"] if row["blocking"]]
        error = PrivateNetlistUnsupported(
            "Maestro user execution/evaluation scripts require current target pre-flight verification "
            "before common CLI capture; saved script settings: " + ", ".join(blocked[:8]))
        error.detail = {"failure_stage": "native_user_scripts",
                        "reason": "saved_script_settings_require_target_verification",
                        "qualification_policy": "preserved_guard_pending_site_evidence",
                        "legacy_guard_hits": guard_hits,
                        "source_setup_path": os.path.abspath(path),
                        "source_setup_sha256": hashlib.sha256(source_bytes).hexdigest(),
                        "script_checks": scripts["checks"], "script_hook_count": scripts["hook_count"],
                        "omitted_script_hooks": scripts["omitted_hooks"],
                        "execution_started": False, "cleanup_proven": True}
        raise error


def capture_rdb_root(workspace):
    """Use only this private Maestro view's results, including same-lib views."""
    if workspace.get("native_storage"):
        from .storage_policy import workspace_rdb_path
        directory = os.path.dirname(workspace_rdb_path(workspace, "__sentinel__"))
        if not is_under(os.path.realpath(directory), os.path.realpath(workspace["root"])):
            raise PrivateNetlistError("native capture results escaped the owned run workspace")
        return directory
    directory = os.path.abspath(workspace["setup_dir"])
    if workspace.get("same_library_objects"):
        from .same_library_views import owns_view
        target = tuple(workspace[name] for name in ("primary_library", "setup_cell", "setup_view"))
        if not owns_view(workspace, target, role="maestro", require_marker=True):
            raise PrivateNetlistError("native capture Maestro result directory lacks exact view ownership")
        registry = next(row for row in workspace["same_library_registry"] if tuple(row["target"]) == target and row["role"] == "maestro")
        if directory != registry["path"]:
            raise PrivateNetlistError("native capture Maestro result path does not match its owned view")
    elif not is_under(os.path.realpath(directory), os.path.realpath(workspace["root"])):
        raise PrivateNetlistError("native capture Maestro results are outside the private workspace")
    return os.path.join(directory, "results", "maestro")


def _capture_timeout_profiles(config, tests):
    """Record private native capture timeout overrides without changing scope.

    Zero is invalid. An explicit UI blank is normalized to the internal -1
    unlimited value by the common policy parser; native readback must prove it.
    The owned outer capture deadline and session/guardian close still apply.
    """
    from .job_policy import normalize_job_overrides, JOB_TIMEOUT_PROPERTIES
    requested = normalize_job_overrides(config.get("_capture_job_overrides"))
    source = config.get("_capture_source_job_policy") or {}
    global_properties = (source.get("global") or {}).get("properties") or {}
    source_tests = {row.get("name"): row.get("properties") or {} for row in source.get("tests") or []}
    def scoped(properties, scope):
        effective = copy.deepcopy(requested)
        supplied = requested.get("timeouts") or {}
        fields = {}
        for field, native in JOB_TIMEOUT_PROPERTIES.items():
            raw = supplied.get(field, properties.get(native))
            origin = "manual_override" if field in supplied else "source_maestro" if native in properties else "unavailable"
            blank = raw is None or (isinstance(raw, str) and not raw.strip())
            if blank and field not in supplied:
                # A nil native getter is missing evidence, unlike the user's
                # explicit blank override, which normalize_job_overrides maps
                # to the documented unlimited value -1.
                origin = "unavailable"
            if not blank:
                try:
                    number = float(raw)
                    valid = not isinstance(raw, bool) and math.isfinite(number) and (number > 0 or number == -1)
                except (ValueError, TypeError):
                    valid = False
                if not valid:
                    error = PrivateNetlistUnsupported("Native capture %s must be positive seconds or unlimited; zero is invalid" % native)
                    error.detail = {"failure_stage": "native_capture_job_policy", "scope": scope,
                                    "field": field, "source_property": native, "raw_value": raw,
                                    "execution_started": False, "cleanup_proven": True}
                    raise error
            status = "unknown" if origin == "unavailable" else "unlimited" if blank or raw in (-1, "-1") else "limited"
            fields[field] = {"requested": raw, "requested_source": origin,
                "effective": raw, "effective_source": origin, "status": status,
                "scope": "native_netlist_capture_only", "zero_allowed": False,
                "reason": ("Source timeout unavailable; native default is not claimed as inherited"
                           if origin == "unavailable" else
                           "Explicit source/override value is applied to the owned capture policy; blank means unlimited (-1)"),
                "outer_deadline_and_owned_close_unchanged": True}
        return {"scope": scope, "requested_overrides": copy.deepcopy(requested),
                "effective_overrides": effective, "fields": fields,
                "linger": copy.deepcopy(fields["linger_seconds"])}
    return {"schema_version": _contract_text("crescendo.native_capture_timeout_profiles.v1"),
            "global": scoped(global_properties, "global"),
            "tests": {name: scoped(dict(global_properties, **source_tests.get(name, {})), "test:" + name)
                      for name in tests}, "source_policy_modified": False,
            "cli_policy_modified": False}


def _capture_policy_names(workspace):
    """Only names saved in the inspected/private setup; never read user .jp."""
    references = [(workspace.get("setup") or {}).get("job_policy_references") or {}]
    private = workspace.get("setup_sdb")
    if private:
        from .ade_setup import inspect_setup
        references.append(inspect_setup(private, backend="maestro").get("job_policy_references") or {})
    names = set()
    for reference in references:
        for name in [reference.get("global")] + [row.get("policy_name") for row in reference.get("tests") or []]:
            if name is None or name == "":
                continue
            if (not isinstance(name, str) or len(name) > 255 or name in (".", "..")
                    or any(ord(char) < 32 or ord(char) == 127 for char in name)
                    or any(char in name for char in ("/", "\\"))):
                raise PrivateNetlistUnsupported("Saved native job policy name is unsafe")
            names.add(name)
    if len(names) > 1024:
        raise PrivateNetlistUnsupported("Saved native job policy reference count exceeds its bound")
    return sorted(names)


def _capture_known_timeout_overrides(profile):
    """A fresh policy needs explicit source timeouts; unknown remains unknown."""
    result = {}
    for field, row in (profile.get("fields") or {}).items():
        if row.get("effective_source") == "unavailable":
            continue
        raw = row.get("effective")
        if raw is None or (isinstance(raw, str) and not raw.strip()):
            result[field] = -1
        else:
            number = float(raw)
            if not math.isfinite(number) or number != int(number) or (number <= 0 and number != -1):
                raise PrivateNetlistUnsupported("Native capture timeout must be integer seconds or unlimited")
            result[field] = int(number)
    return {"timeouts": result} if result else {}


def _capture_setup_skill(workspace, tests, profiles, path):
    """Pre-open safe aliases, then independent owned policies before Test APIs."""
    directory = os.path.join(workspace["root"], ".cadence")
    if os.path.lexists(directory) and (os.path.islink(directory) or not os.path.isdir(directory)):
        raise PrivateNetlistUnsupported("Owned native job policy directory is unsafe")
    ensure_dir(directory)
    policy_dir = os.path.join(directory, "jobpolicy")
    if os.path.lexists(policy_dir) and (os.path.islink(policy_dir) or not os.path.isdir(policy_dir)):
        raise PrivateNetlistUnsupported("Owned native job policy file directory is unsafe")
    ensure_dir(policy_dir)
    names = _capture_policy_names(workspace)
    # A direct retry may reuse its phase directory. This attempt starts its
    # own evidence stream; prior successful rows cannot qualify an early fail.
    atomic_write_text(path, "")
    quote = lambda value: '"' + _skill_quote(value) + '"'
    # Every operation opens and closes the append-only record independently;
    # a native error cannot strand buffered evidence behind it.
    pre = [
        _contract_text('unless(equal(getShellEnvVar("HOME") %s) && equal(getWorkingDir() %s) error("Crescendo capture policy HOME/cwd escaped the owned workspace\\n"))') % (quote(workspace["root"]), quote(workspace["root"])),
        _contract_text('procedure(crescCaptureRecord(kind scope name operation succeeded value)'),
        '  let((port) port=outfile(%s "a")' % quote(path),
        _contract_text('    unless(port error("Crescendo capture policy/mode record unavailable\\n"))'),
        '    fprintf(port "%s\\t%L\\t%L\\t%L\\t%L\\t%L\\n" kind scope name operation succeeded value)',
        '    close(port)))',
        _contract_text("unless(isCallable('axlAdd" "JobPolicy) && isCallable('axlGetJobPolicy) error(\"Crescendo capture policy alias APIs unavailable\\n\"))")]
    def call(lines, expression, kind, scope, name, operation, message):
        lines.extend([
            _contract_text('crescCaptureResult=errset(%s t)') % expression,
            _contract_text('crescCaptureRecord("%s" %s %s "%s" if(crescCaptureResult then t else nil) if(crescCaptureResult then car(crescCaptureResult) else nil))') %
                (kind, quote(scope), quote(name), operation),
            _contract_text('unless(crescCaptureResult && car(crescCaptureResult) error("%s\\n"))') % message])
    def read_policy(lines, expression, scope, name, expected):
        lines.extend([_contract_text('crescCaptureResult=errset(%s t)') % expression,
            _contract_text('crescCaptureRecord("GET" %s %s "policy" if(crescCaptureResult then t else nil) if(crescCaptureResult then car(crescCaptureResult) else nil))') % (quote(scope), quote(name)),
            _contract_text('unless(crescCaptureResult && listp(car(crescCaptureResult)) && car(crescCaptureResult) error("Crescendo capture policy readback unavailable\\n"))'),
            _contract_text('crescCapturePolicy=car(crescCaptureResult)')])
        for field, value in sorted(expected.items()):
            lines.append(_contract_text('crescCaptureRecord("READ" %s %s "%s" t getq(crescCapturePolicy %s))') % (quote(scope), quote(name), field, field))
            lines.append(_contract_text('unless(equal(getq(crescCapturePolicy %s) %s) error("Crescendo capture policy readback differs: %s/%s\\n"))') % (field, quote(value), _skill_quote(scope), field))
    alias_expected = {"distributionmethod": "Local", "preemptivestart": "0", "maxjobs": "2", "usesameprocess": "0"}
    for name in names:
        target = os.path.join(policy_dir, name + ".jp")
        if os.path.lexists(target) and (os.path.islink(target) or not os.path.isfile(target)):
            raise PrivateNetlistUnsupported("Owned native job policy alias file is unsafe")
        expected = dict(alias_expected, name=name)
        props = 'list(nil ' + ' '.join("'%s %s" % (key, quote(value)) for key, value in sorted(expected.items())) + ')'
        call(pre, 'axlAdd' 'JobPolicy(%s %s %s)' % (quote(name), quote(directory), props),
             "SET", "preopen_alias", name, "axlAdd" "JobPolicy", _contract_text("Crescendo capture-only native policy alias could not be saved"))
        read_policy(pre, 'axlGetJobPolicy(%s)' % quote(name), "preopen_alias", name, expected)
    from .job_policy import timeout_override_skill_arguments, timeout_override_properties
    token = hashlib.sha256(workspace["root"].encode("utf-8")).hexdigest()[:12]
    post, final_names, final_expected = [], {}, {}
    for scope, test, profile in [("global", None, profiles["global"])] + [
            ("test:" + test, test, profiles["tests"][test]) for test in tests]:
        name = _contract_text("CRESCENDO_Capture_%s_%s") % (token, "global" if test is None else hashlib.sha256(test.encode("utf-8")).hexdigest()[:12])
        final_names[scope] = name
        overrides = _capture_known_timeout_overrides(profile)
        profile["effective_overrides"] = copy.deepcopy(overrides)
        args = timeout_override_skill_arguments(overrides)
        expected = dict(alias_expected, name=name, **timeout_override_properties(overrides))
        final_expected[scope] = expected
        expression = 'maeSetJobPolicy(%s ?distributionmethod "Local" ?maxjobs "2" ?usesameprocess "0" ?preemptivestart "0"%s%s ?session session)' % (
            quote(name), args, ' ?tests list(%s)' % quote(test) if test is not None else '')
        call(post, expression, "SET", scope, name, "maeSetJobPolicy", _contract_text("Crescendo cannot isolate native capture policy"))
        read_policy(post, 'maeGetJobPolicy(%s?session session)' % ('?testName %s ' % quote(test) if test is not None else ''), scope, name, expected)
    return pre, post, {"records_path": path, "alias_directory": directory,
        "alias_names": names, "final_policy_names": final_names,
        "expected_policy_readback": final_expected,
        "scope": "owned_capture_child_only", "user_policy_files_read_or_modified": False}


def _capture_mode_skill(tests):
    # controlMode is an Environment Option. TestEnvVar is a separate API;
    # requiring it first rejects supported setups before the actual setter.
    lines = []
    for test in tests:
        quoted = '"' + _skill_quote(test) + '"'
        for setter, getter, operation in [
            ('maeSetEnvOption(%s ?options list(list("controlMode" "batch")) ?session session)' % quoted,
             'maeGetEnvOption(%s ?option "controlMode" ?session session)' % quoted, "maeSetEnvOption")]:
            lines.extend([
                _contract_text('crescCaptureResult=errset(%s t)') % setter,
                _contract_text('crescCaptureRecord("SET" %s "controlMode" "%s" if(crescCaptureResult then t else nil) if(crescCaptureResult then car(crescCaptureResult) else nil))') % (quoted, operation),
                _contract_text('unless(crescCaptureResult && car(crescCaptureResult) error("Crescendo native test batch mode rejected: %s/%s\\n"))') % (_skill_quote(test), operation),
                _contract_text('crescCaptureResult=errset(%s t)') % getter,
                _contract_text('crescCaptureRecord("MODE" %s "controlMode" "%s" if(crescCaptureResult then t else nil) if(crescCaptureResult then car(crescCaptureResult) else nil))') % (quoted, operation),
                _contract_text('unless(crescCaptureResult && equal(car(crescCaptureResult) "batch") error("Crescendo native batch mode readback differs: %s/%s\\n"))') % (_skill_quote(test), operation)])
    return lines


def _capture_environment_observation_skill(tests):
    """Optional read-only diagnostics; never change Environment or qualify it.

    IC618 returns an alist of (name value), including empty values. Discover
    vendor keys from that readback rather than inventing an ALPS option name.
    Global error tracing must be disabled around this optional getter as an
    errset print flag alone does not silence its stack on this runtime.
    """
    lines = [
        _contract_text('procedure(crescObserveCaptureEnvironment(test)'),
        ' let((savedTrace savedStack observation options optionName literal matched shapeOK observationStatus)',
        '  savedTrace=status(errsetTrace) savedStack=status(stacktrace)',
        '  unwindProtect(',
        '   progn(sstatus(errsetTrace nil) sstatus(stacktrace 0)',
        '    errset(progn(',
        '     observation=errset(maeGetEnvOption(test ?includeEmpty t ?session session) nil)',
        '     observationStatus="UNAVAILABLE" matched=0',
        '     when(observation && listp(car(observation)) && car(observation)',
        '      options=car(observation) shapeOK=length(options)<=256',
        '      when(shapeOK foreach(row options',
        '       unless(listp(row) && length(row)==2 && stringp(car(row)) shapeOK=nil)))',
        '      when(shapeOK observationStatus="OBSERVED"',
        '       foreach(row options optionName=car(row)',
        '        when(member(optionName list("autoDisplay" "userCmdLineOption" "licQueueTimeOut")) || rexMatchp(".*alps.*" lowerCase(optionName))',
        '         if(matched<8 then',
        '          literal=sprintf(nil "%L" cadr(row))',
        '          when(strlen(literal)>2048 literal=strcat(substring(literal 1 2048) "...[truncated]") observationStatus="TRUNCATED")',
        _contract_text('          crescCaptureRecord("ENV" test optionName "maeGetEnvOption.diagnostic" t literal)'),
        '          matched=matched+1',
        '         else observationStatus="TRUNCATED")))))',
        _contract_text('     crescCaptureRecord("ENV" test "__CRESCENDO_ENV_DIAGNOSTIC_STATUS__" "maeGetEnvOption.diagnostic" t observationStatus)'),
        '    ) nil))',
        '   progn(sstatus(stacktrace savedStack) sstatus(errsetTrace savedTrace)))',
        ' ))']
    lines.extend(_contract_text('crescObserveCaptureEnvironment("%s")') % _skill_quote(test) for test in tests)
    return lines


def _capture_setup_readback(plan, tests):
    """Diagnostics report actual partial evidence, including pre-Run failures."""
    result = dict(plan, records=[], policy_confirmed=False, mode_confirmed=False,
                  interactiveA_confirmed=None, interactiveE_confirmed=None,
                  readback_status="not_available", environment_option_observations=[])
    try:
        path = plan["records_path"]
        if not os.path.isfile(path):
            return result
        if os.path.getsize(path) > 2 * 1024 * 1024:
            raise ValueError("capture policy/mode records exceed bound")
        with open(path, encoding="utf-8") as stream:
            lines = stream.readlines()
        for line in lines:
            parts = line.rstrip("\n").split("\t")
            if len(parts) != 6 or parts[0] not in ("SET", "GET", "READ", "MODE", "ENV"):
                raise ValueError("malformed capture policy/mode record")
            values = [None if value == "nil" else True if value == "t" else
                      ast.literal_eval(value) if not value.startswith('(') else value[:4096] for value in parts[1:]]
            result["records"].append(dict(zip(("kind", "scope", "name", "operation", "call_succeeded", "value"), [parts[0]] + values)))
        records = result["records"]
        observations = {}
        for row in records:
            if row["kind"] != "ENV" or row["operation"] != "maeGetEnvOption.diagnostic":
                continue
            observation = observations.setdefault(row["scope"], {"test": row["scope"],
                "status": "UNAVAILABLE", "fields": []})
            if row["name"] == _contract_text("__CRESCENDO_ENV_DIAGNOSTIC_STATUS__"):
                observation["status"] = row["value"]
            elif len(observation["fields"]) < 8:
                observation["fields"].append({"name": row["name"], "literal": row["value"]})
        result["environment_option_observations"] = list(observations.values())
        setters_confirmed = all(any(row["kind"] == "SET" and row["scope"] == scope and
            row["name"] == name and row["operation"] == "maeSetJobPolicy" and
            row["call_succeeded"] is True and bool(row["value"]) for row in records)
            for scope, name in plan["final_policy_names"].items())
        result["policy_confirmed"] = setters_confirmed and all(any(row["kind"] == "READ" and row["scope"] == scope and
            row["name"] == plan["final_policy_names"][scope] and row["operation"] == field and
            row["call_succeeded"] is True and row["value"] == expected for row in records)
            for scope, fields in plan["expected_policy_readback"].items()
            for field, expected in fields.items())
        result["mode_confirmed"] = all(any(row["kind"] == "SET" and row["scope"] == test and row["name"] == "controlMode" and
            row["operation"] == operation and row["call_succeeded"] is True and bool(row["value"]) for row in records)
            and any(row["kind"] == "MODE" and row["scope"] == test and row["name"] == "controlMode" and
            row["operation"] == operation and row["call_succeeded"] is True and row["value"] == "batch" for row in records)
            for test in tests for operation in ("maeSetEnvOption",))
        for field in ("interactiveA", "interactiveE"):
            actual = [row for row in records if row["kind"] == "ENV" and row["name"] == field and
                      row["call_succeeded"] is True]
            if actual:
                result[field + "_confirmed"] = False if actual[-1]["value"] is None else actual[-1]["value"]
        result["readback_status"] = "collected"
    except (OSError, ValueError, SyntaxError) as error:
        result["readback_status"] = "invalid"
        result["readback_error"] = str(error)
    return result


def _capture_request_config(prepared, config, spec):
    """Shared frontend freeze, including the explicit preparation UI budget."""
    from .simulator_profiles import apply_source_profile_defaults
    from .job_policy import normalize_job_overrides
    from .corner_selection import selection_plan, effective_setup
    setup = (prepared or {}).get("setup") or {}
    from .test_selection import selection_plan as test_plan, effective_setup as test_setup
    execution_selection = test_plan(setup, spec)
    selected_setup = test_setup(setup, execution_selection)
    plan = selection_plan(selected_setup, spec)
    selected_setup = effective_setup(selected_setup, plan)
    result = apply_source_profile_defaults(config, selected_setup)
    result["_capture_execution_selection"] = execution_selection
    result["_capture_corner_selection"] = plan
    result["_capture_job_overrides"] = normalize_job_overrides((spec or {}).get("maestro_job_overrides"))
    result["_capture_source_job_policy"] = copy.deepcopy((prepared or {}).get("job_policy") or {})
    runtime = result.setdefault("runtime", {})
    if "candidate_timeout_seconds" in (spec or {}):
        raw = spec["candidate_timeout_seconds"]
        origin = "explicit_preparation_measurement_budget"
    else:
        raw = runtime.get("private_netlist_prepare_timeout_seconds", 300)
        origin = "runtime_private_netlist_prepare" if "private_netlist_prepare_timeout_seconds" in runtime else "legacy_capture_default_300_seconds"
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        value = -1
    else:
        try:
            value = float(raw)
            valid = not isinstance(raw, bool) and math.isfinite(value) and (value > 0 or value == -1)
        except (ValueError, TypeError):
            valid = False
        if not valid:
            raise PrivateNetlistUnsupported("Preparation budget must be positive seconds or blank for no limit; zero is invalid")
    runtime["private_netlist_prepare_timeout_seconds"] = value
    result["_capture_outer_budget"] = {"requested": raw, "effective_seconds": value,
        "enabled": value > 0, "source": origin, "scope": "native_netlist_generation_only",
        "cancel_and_owned_close_remain_active": True}
    tests = [row["name"] for row in selected_setup.get("tests") or [] if row.get("enabled", True)]
    # Reject zero/invalid native settings before copying OA or creating workers.
    _capture_timeout_profiles(result, tests)
    return result


def _capture_worker_logs(workspace, before=None):
    """Read only bounded owned Job logs and distinguish wrapper aftermath.

    The simulator capture wrapper exits 1 deliberately.  ADE errors after the
    native 'Running simulation' boundary cannot prove a CDF/netlisting error.
    Existing unchanged bytes from a shared suite are never reported again.
    """
    owned = os.path.realpath(workspace["root"])
    logs_root = os.path.join(owned, "logs_root")
    result = {"schema_version": _contract_text("crescendo.native_worker_diagnostics.v1"), "owned_workspace": owned,
              "logs_root": logs_root, "logs": [], "ok": True,
              "expected_post_generation_wrapper_failure": True,
              "expected_wrapper_aftermath_samples": [], "parent_capture_failed": False}
    snapshot = {}
    # ADE names this directory after the executing user.  The local fixture
    # happened to be root; migrating that literal silently hid every Job log.
    roots = []
    for name in sorted(os.listdir(owned)):
        if not re.fullmatch(r"logs_[A-Za-z0-9_.-]+", name):
            continue
        candidate = os.path.join(owned, name)
        if os.path.islink(candidate) or os.path.realpath(candidate) != candidate:
            result.setdefault("omitted_log_roots", []).append({"path": candidate, "reason": "linked directory"})
            continue
        if os.path.isdir(candidate):
            roots.append(candidate)
    result["logs_roots"] = roots
    if len(roots) == 1:
        result["logs_root"] = roots[0]
    if len(roots) > 64:
        result.update(ok=False, qualification_error="Owned native worker log root inventory exceeds 64 directories")
        return result, snapshot
    paths = []
    for logs_root in roots:
        for directory in sorted(os.listdir(logs_root)):
            child = os.path.join(logs_root, directory)
            if not re.match(r"^logs[0-9]+$", directory) or not os.path.isdir(child) or os.path.islink(child):
                continue
            for name in sorted(os.listdir(child)):
                if re.match(r"^Job[0-9]+\.log$", name):
                    paths.append(os.path.join(child, name))
    if len(paths) > 4096:
        result.update(ok=False, qualification_error="Owned native worker log inventory exceeds 4096 files")
        return result, snapshot
    changed_count = 0
    for path in paths:
        prior = (before or {}).get(path) or {}
        info, identity = None, None
        try:
            info = os.lstat(path)
            identity = [info.st_dev, info.st_ino, info.st_size,
                        getattr(info, "st_mtime_ns", info.st_mtime), getattr(info, "st_ctime_ns", info.st_ctime)]
            if prior.get("identity") == identity:
                snapshot[path] = prior
                continue
            if before is not None:
                changed_count += 1
                if changed_count > 128:
                    raise PrivateNetlistUnsupported("More than 128 native worker logs changed in one capture")
            _regular(path, owned, maximum=MAX_METADATA_BYTES)
            with open(path, "rb") as stream:
                raw = stream.read(MAX_METADATA_BYTES + 1)
            if len(raw) > MAX_METADATA_BYTES:
                raise PrivateNetlistUnsupported("Native worker log grew beyond its byte bound")
        except (OSError, PrivateNetlistError) as error:
            if before is None and info is not None and stat.S_ISREG(info.st_mode):
                snapshot[path] = {"identity": identity, "bytes": info.st_size,
                                  "unreadable_before_capture": True}
            else:
                result["ok"] = False
                result.setdefault("unreadable_logs", []).append({"path": path, "error": str(error)})
            if changed_count > 128:
                break
            continue
        digest = hashlib.sha256(raw).hexdigest()
        snapshot[path] = {"identity": identity, "bytes": len(raw), "sha256": digest, "lines": raw.count(b"\n")}
        if before is None:
            continue
        offset, start_line = 0, 1
        if prior.get("sha256") and len(raw) >= prior["bytes"] and hashlib.sha256(raw[:prior["bytes"]]).hexdigest() == prior["sha256"]:
            offset, start_line = prior["bytes"], prior["lines"] + 1
        fresh = len(raw) > offset
        if not fresh:
            continue
        lines = raw[offset:].decode("utf-8", "replace").splitlines()
        pre_generation, errors, generation_errors = True, [], []
        for index, line in enumerate(lines):
            if "Run start for Point ID" in line:
                pre_generation = True
            if "Running simulation on testbench" in line:
                pre_generation = False
            if re.search(r"\*Error\*|\bERROR\s*\(", line):
                from .native_diagnostics import native_error_context
                evidence = {"line": start_line + index, "text": line[:1024],
                    "stage": "native_generation" if pre_generation else "capture_wrapper_aftermath",
                    "context": lines[max(0, index-2):min(len(lines), index+3)],
                    "trace_context": native_error_context(lines, index, start_line=start_line)}
                errors.append(evidence)
                if pre_generation:
                    generation_errors.append(evidence)
                elif len(result["expected_wrapper_aftermath_samples"]) < 2:
                    code = re.search(r"\bERROR\s*\(([A-Za-z0-9_-]+)\)", line)
                    result["expected_wrapper_aftermath_samples"].append({
                        "console_source": path, "line": start_line + index,
                        "code": code.group(1) if code else None,
                        "message": line[:1024], "stage": "capture_wrapper_aftermath",
                        "primary_failure": False})
        result["logs"].append({"path": path, "sha256": digest, "fresh": True,
            "new_bytes_from": offset, "error_lines": generation_errors[:32],
            "expected_wrapper_aftermath_error_count": len(errors)-len(generation_errors),
            "generation_error_count": len(generation_errors)})
        result["ok"] = result["ok"] and not generation_errors
    return result, snapshot


def _capture_child_environment(config, workspace_root, local_bin):
    env = child_environment(config, workspace_root)
    # The capture Ocean owns its workers and must not join the invoking
    # Virtuoso's MPS session, just as the independent baseline Ocean does.
    env.pop("CDS_MPS_SESSION", None)
    env["PATH"] = local_bin + os.pathsep + env.get("PATH", "")
    return env


def _capture_failure_text(native_diagnostics, worker_diagnostics):
    first_error = native_diagnostics.get("first_error") or {}
    primary = first_error.get("message") if isinstance(first_error, dict) else str(first_error)
    if primary:
        return str(primary)[:1024]
    for log in worker_diagnostics.get("logs") or []:
        for row in log.get("error_lines") or []:
            if row.get("stage") == "native_generation" and row.get("text"):
                return str(row["text"])[:1024]
    return "native netlist capture failed"


def _raise_capture_failure(cancelled, command, native_diagnostics, worker_diagnostics,
                           capture_diagnostics, failure_inputs, root):
    primary_text = _capture_failure_text(native_diagnostics, worker_diagnostics)
    error = (PrivateNetlistError("private netlist preparation was cancelled") if cancelled else
             PrivateNetlistUnsupported(primary_text + "; see " + os.path.join(root, "ocean.stdout")))
    error.native_diagnostic = (command.get("stdout_tail") or "") + "\n" + (command.get("stderr_tail") or "")
    error.detail = {"failure_stage": "native_netlist_cancelled" if cancelled else "native_capture_worker_generation_error" if not worker_diagnostics["ok"] else "native_capture_skill_error" if native_diagnostics.get("syntax_error") or native_diagnostics.get("native_error") else "native_netlist_preparation", "command": command,
                    "native_worker_diagnostics": worker_diagnostics,
                    "capture_diagnostics": capture_diagnostics,
                    "failure_inputs": failure_inputs,
                    "cleanup_proven": command.get("guardian_cleanup", {}).get("state") == "dead"}
    raise error


def _capture_phase(workspace, parameters, values, symbols, seeds, config, root,
                   emit_clones=False, symbolic=False, cancel_check=None, progress_callback=None,
                   verify_numeric_application=False, private_config_initialized=False,
                   baseline_only=False, allow_effective_adjustment=False, batch_plan=None):
    require_unique_run_variables(workspace.get("setup") or {},
                                 action="capture private SCS", multiplicity_only=True)
    if baseline_only and (values or symbols or seeds or symbolic or verify_numeric_application):
        raise PrivateNetlistError("unchanged baseline capture cannot apply candidate values or template symbols")
    ensure_dir(root)
    _check_cancel(cancel_check)
    if (workspace.get("setup") or {}).get("selected_test"):
        from .corner_selection import eligible_corner_names
        from .single_test import project_native_files
        setup = workspace["setup"]
        requested = (config.get("_capture_corner_selection") or {}).get("selected")
        wanted = list(requested if requested is not None else
                      eligible_corner_names(setup, setup["selected_test"]))
        if not wanted:
            raise PrivateNetlistUnsupported("No qualified corner selected for this Test")
        previous = workspace.get(_contract_text("_cr_selected_corners"))
        if previous is None:
            project_native_files(workspace["setup_sdb"], setup["selected_test"],
                                 os.path.join(root, _contract_text("native_capture_projection.json")),
                                 selected_corners=wanted)
            workspace[_contract_text("_cr_selected_corners")] = wanted
        elif previous != wanted:
            raise PrivateNetlistUnsupported("Corner selection changed without rebuilding the private setup")
    setup_identity = _capture_setup_identity(workspace, root, symbols.values(), config=config)
    from .corner_selection import effective_setup, selection_skill_lines, capture_selection_plan
    corner_selection = capture_selection_plan(workspace["setup"], config.get("_capture_corner_selection"),
                                              workspace.get("_native_corner_selection_applied"))
    from .test_selection import effective_setup as test_setup, selection_skill_lines as test_skill_lines
    from .test_selection import capture_selection_plan as capture_test_plan
    execution_selection = capture_test_plan(workspace["setup"], config.get("_capture_execution_selection"),
                                             workspace.get("_native_execution_selection_applied"))
    run_setup = test_setup(effective_setup(workspace["setup"], corner_selection), execution_selection)
    setup_identity["run_execution_selection"] = copy.deepcopy(execution_selection)
    setup_identity["run_corner_selection"] = copy.deepcopy(corner_selection)
    reset_results(workspace)
    records = os.path.join(root, "patch.records")
    patch = os.path.join(root, "patch.il")
    patch_parameters = copy.deepcopy(parameters)
    if symbolic:
        for parameter in patch_parameters:
            parameter["cdf_type"] = "instData"
    # No selected value is written into OA and no PDK callback runs here.
    # Native Parameters applies candidates later in Maestro's Run/point flow.
    clone_selection = dict((row["parameter_id"], None) for row in parameters)
    if len(clone_selection) != len(parameters):
        raise PrivateNetlistError("native capture parameter identities are duplicated")
    if not baseline_only and set(values) != set(clone_selection):
        raise PrivateNetlistError("native candidate differs from selected private parameter identities")
    plan = write_schematic_patch(workspace, patch_parameters, clone_selection, patch, records,
                                 pdk_adapter="generic-cdf", emit_parameters=False,
                                 emit_clones=emit_clones, exit_after=False,
                                 clone_state="absent" if emit_clones else "existing")
    native_rows, native_plan = [], None
    native_settings_path = os.path.join(root, "native_parameters.tsv")
    if not baseline_only and parameters:
        from .workspace import _schematic_parameter_rows
        from .native_maestro_parameters import build_native_parameter_plan
        _clones, native_rows = _schematic_parameter_rows(workspace, patch_parameters, values,
            clone_state="mixed" if emit_clones else "existing")
        for row in native_rows:
            row.setdefault("view", "schematic")
        native_plan = build_native_parameter_plan(native_rows, run_setup.get("corners"),
            owned_paths=workspace.get("native_parameter_paths") or [],
            allowed_symbols=symbols.values() if symbolic else [])
    if batch_plan is not None:
        from .generation_batch import build_native_columns
        build_native_columns(batch_plan, native_plan["selected"])
    captures = ensure_dir(os.path.join(root, "invocations"))
    invocation_prefix = "capture." + uuid.uuid4().hex + "."
    local_bin = ensure_dir(os.path.join(workspace["root"], "bin"))
    capture_simulator = config.get("_capture_source_simulator", "spectre")
    wrapper = os.path.join(local_bin, capture_simulator)
    ready_path = os.path.join(root, "native_capture_ready")
    atomic_write_text(wrapper, '#!/bin/sh\nprintf \'%s\\0\' "$PWD" "$@" > ' + shlex.quote(captures) +
                      '/' + invocation_prefix + '$$\nprintf \'capture_only\\n\' > ' + shlex.quote(ready_path) + '\nexit 1\n')
    os.chmod(wrapper, 0o700)
    tests = [row["name"] for row in run_setup.get("tests") or [] if row.get("enabled", True)]
    capture_timeout_profiles = _capture_timeout_profiles(config, tests)
    capture_policy_path = os.path.join(root, "capture_policy_mode.tsv")
    alias_lines, policy_lines, capture_policy_plan = _capture_setup_skill(
        workspace, tests, capture_timeout_profiles, capture_policy_path)
    from .workspace import bounded_skill_body_lines
    lines = ['sstatus(errsetTrace t)', 'sstatus(stacktrace 16)'] + _workspace_startup_load_lines(workspace)
    # Verified IC618 policy: disable interactive worker reuse in this owned
    # child before opening Maestro. Otherwise one invocation can serve several
    # points through simiADEPlugin and an exit-only capture misses their SCS.
    body = (['ocnSetXLMode("assembler")'] if batch_plan is not None else []) + ['envSetVal("maestro.simulation" "interactiveA" \'boolean nil)',
            'envSetVal("maestro.simulation" "interactiveE" \'boolean nil)',
            _contract_text('when(envGetVal("maestro.simulation" "interactiveA") || envGetVal("maestro.simulation" "interactiveE") error("Crescendo native noninteractive capture policy did not apply\\n"))')]
    body.extend(alias_lines)
    for field in ("interactiveA", "interactiveE"):
        body.append(_contract_text('crescCaptureRecord("ENV" "child" "%s" "envGetVal" t envGetVal("maestro.simulation" "%s"))') % (field, field))
    body.extend([
                  _contract_text('unless(errset(load("%s") t) error("Crescendo private netlist parameterization failed\\n"))') % _skill_quote(patch),
                  _contract_text('unless(crescSchematicPatchResult error("Crescendo private netlist parameter patch did not complete\\n"))'),
                  'session=maeOpenSetup("%s" "%s" "%s" ?mode "a")' % tuple(_skill_quote(workspace[key]) for key in ("primary_library", "setup_cell", "setup_view")),
                  _contract_text('unless(session error("Crescendo private Maestro could not open\\n"))'),
                  'mainSDB=axlGetMainSetupDB(session)'])
    if config.get("_diagnostic_native_run_trace") is True:
        body.extend(_capture_environment_observation_skill(tests))
    body.extend(policy_lines)
    from .storage_policy import native_storage_readback_lines, load_native_storage_readback
    body.extend(native_storage_readback_lines(workspace))
    body.extend(workspace_test_design_lines(workspace,read_only=True) if private_config_initialized
                else workspace_test_design_lines(workspace))
    from .native_variable_scope_readback import native_variable_scope_skill_lines
    native_scope_path = os.path.join(root, "native_variable_scope.tsv")
    body.extend(native_variable_scope_skill_lines(run_setup, native_scope_path))
    for test in tests:
        body.append(_contract_text('unless(equal(cadr(assoc("sim" axlGetTestToolArgs(axlGetTest(mainSDB "%s")))) "%s") error("Crescendo saved capture simulator identity changed\\n"))') % (_skill_quote(test), capture_simulator))
    body.extend(_capture_mode_skill(tests))
    from .single_test import native_selection, native_selection_lines
    native_execution_selection = native_selection(workspace["setup"])
    if native_execution_selection and execution_selection:
        for name in native_execution_selection["selected"]:
            native_execution_selection["analyses"][name] = list(execution_selection["analyses"][name])
    body.extend(native_selection_lines(native_execution_selection) if native_execution_selection else test_skill_lines(execution_selection))
    native_corner_selection = copy.deepcopy(corner_selection)
    if native_corner_selection and workspace.get(_contract_text("_cr_selected_corners")) is not None:
        native_corner_selection["available"] = [name for name in native_corner_selection["available"]
                                                if name in workspace[_contract_text("_cr_selected_corners")]]
    body.extend(selection_skill_lines(native_corner_selection))
    for key in sorted(symbols):
        body.append(_contract_text('unless(maeSetVar("%s" "%.17g" ?session session) error("Crescendo private netlist symbol seed failed\\n"))') % (symbols[key], seeds[key]))
    schedule_path = os.path.join(root, "native_corner_schedule.tsv")
    body.extend(native_corner_schedule_skill(run_setup, schedule_path, include_members=True))
    if native_plan:
        from .native_maestro_parameters import native_parameter_skill_lines
        if batch_plan is None:
            body.extend(native_parameter_skill_lines(native_plan, native_settings_path))
        else:
            from .generation_batch import native_batch_skill_lines
            body.extend(native_batch_skill_lines(batch_plan, native_plan,
                os.path.join(root, "native_batch_columns.tsv")))
    # A single Maestro worker can select interactive Spectre and abort the
    # remaining points when this capture-only entry intentionally exits 1.
    # Keep the established two-worker netlisting policy. This runs no solver;
    # the diagnostic candidate's real Spectre task pool still has capacity 1.
    capture_jobs = "2"
    from .native_diagnostics import native_run_trace_lines
    run_trace_requested = config.get("_diagnostic_native_run_trace") is True
    body.extend(native_run_trace_lines([
        'runId=maeRunSimulation(?waitUntilDone t ?returnRunId t ?session session)',
        "maeWaitUntilDone('All ?session session)"], enabled=run_trace_requested))
    from .storage_policy import native_history_readback_line, qualify_native_history_paths
    if workspace.get("native_storage"):
        body.extend([_contract_text('printf("CRESCENDO_NATIVE_RUN_STAGE HistoryLookup BEGIN\\n")'),
                     'runData=axlGetRunData(session runId)',
                     _contract_text('printf("CRESCENDO_NATIVE_RUN_VALUE history %L\\n" runData)'),
                     _contract_text('when(!runData || equal(runData 0) error("Crescendo native capture history unavailable\\n"))'),
                     _contract_text('printf("CRESCENDO_NATIVE_RUN_STAGE HistoryLookup RETURN\\n")'),
                     native_history_readback_line(workspace, root)])
    if batch_plan is not None:
        from .generation_batch import native_point_export_lines
        body.extend(['runData=axlGetRunData(session runId)',
                     'unless(runData && runData!=0 error("Exact capture Run missing\\n"))',
                     'historyName=axlGetHistoryName(runData)',
                     _contract_text('cr_captureRunOut=outfile(%s)') % ('"' + _skill_quote(os.path.join(root, 'native_batch_run.tsv')) + '"'),
                     _contract_text('fprintf(cr_captureRunOut "%L\\t%s\\n" runId historyName) close(cr_captureRunOut)')])
        body.extend(native_point_export_lines(os.path.join(root, "native_batch_points.tsv")))
    body.extend(['maeCloseSession(?session session ?forceClose t)',
                 _contract_text('printf("CRESCENDO_PRIVATE_NETLIST_CAPTURE_DONE\\n")')])
    bounded, generation = bounded_skill_body_lines(body, result_name=_contract_text("crescNetlistCaptureBodyResult"))
    generation["native_run_trace"] = {
        "requested": run_trace_requested,
        "scope": "diagnostic owned native Run/Wait only; no automatic replay",
        "availability_source": "native console trace markers",
        "reader_payload_guaranteed": False}
    generation["capture_timeout_profiles"] = capture_timeout_profiles
    generation["capture_outer_budget"] = copy.deepcopy(config.get("_capture_outer_budget") or {
        "effective_seconds": float(config.get("runtime", {}).get("private_netlist_prepare_timeout_seconds", 300)),
        "source": "direct_capture_runtime_or_legacy_default", "scope": "native_netlist_generation_only"})
    lines.extend(bounded)
    lines.append(_contract_text('if(crescNetlistCaptureBodyResult then exit(0) else exit(1))'))
    atomic_write_json(os.path.join(root, _contract_text("capture_generation.json")), generation)
    script = os.path.join(root, "capture.il")
    atomic_write_text(script, "\n".join(lines) + "\n")
    env = _capture_child_environment(config, workspace["root"], local_bin)
    timeout = float(config.get("runtime", {}).get("private_netlist_prepare_timeout_seconds", 300))
    from .netlist_measurement import run_owned_command
    def guarded_runner(argv, **kwargs):
        return run_command(argv, startup_lock=CADENCE_STARTUP_LOCK, ready_path=ready_path, **kwargs)
    native_argv = [config["cadence"]["ocean_bin"], "-nograph", "-restore", script]
    if progress_callback:
        progress_callback({"stage": "native_netlist_capture_command", "argv": native_argv, "cwd": workspace["root"]})
    _worker_unused, worker_before = _capture_worker_logs(workspace)
    command = run_owned_command(native_argv,
                                cwd=workspace["root"], env=env, timeout_seconds=timeout, control_root=root,
                                token="netlist-capture-" + uuid.uuid4().hex,
                                cancel_check=cancel_check, command_runner=guarded_runner)
    atomic_write_json(os.path.join(root, _contract_text("command.json")), command)
    from .native_diagnostics import skill_command_diagnostics
    native_diagnostics = skill_command_diagnostics(command, script, required_marker=_contract_text("CRESCENDO_PRIVATE_NETLIST_CAPTURE_DONE"), owned_root=root)
    worker_diagnostics, _worker_after = _capture_worker_logs(workspace, worker_before)
    atomic_write_json(os.path.join(root, _contract_text("native_worker_diagnostics.json")), worker_diagnostics)
    capture_setup_readback = _capture_setup_readback(capture_policy_plan, tests)
    capture_diagnostics = {"schema_version": _contract_text("crescendo.native_capture_diagnostics.v1"),
                           "capture_only": True, "real_spectre_execution_started": False,
                           "unchanged_baseline_only": bool(baseline_only),
                           "native_control_mode": {"api": "maeSetEnvOption/maeGetEnvOption",
                             "requested": {"mode": "batch", "interactiveA": False, "interactiveE": False},
                             "confirmed": {"test_batch_mode": capture_setup_readback["mode_confirmed"],
                               "interactiveA": capture_setup_readback["interactiveA_confirmed"],
                               "interactiveE": capture_setup_readback["interactiveE_confirmed"]},
                             "scope": "owned_private_capture_child_only"},
                           "native_capture_setup": capture_setup_readback,
                           "expected_wrapper_exit": 1, "wrapper_path": wrapper,
                           "capture_capacity": int(capture_jobs),
                           "capture_timeout_profiles": capture_timeout_profiles,
                           "capture_outer_budget": generation["capture_outer_budget"],
                           "capture_capacity_reason": "Two native netlisting workers avoid interactive simulator reuse after intentional wrapper exit; no real Spectre solver runs in this phase.",
                           "wrapper_sha256": _digest(wrapper), "script_path": script,
                           "generation": generation, "ocean": command,
                           "skill": native_diagnostics,
                           "native_worker_diagnostics": worker_diagnostics,
                           "recorded_invocation_count": len(os.listdir(captures)),
                           "assembler_failure_explanation": "The capture wrapper records each invocation and intentionally exits 1; ASSEMBLER-5031 alone is expected at this phase."}
    atomic_write_json(os.path.join(root, _contract_text("capture_diagnostics.json")), capture_diagnostics)
    cancelled = bool(command.get("cancelled") or (cancel_check and cancel_check()))
    if cancelled or command.get("returncode") != 0 or command.get("timed_out") or command.get("termination_failed") or not native_diagnostics["ok"] or not worker_diagnostics["ok"]:
        worker_diagnostics["parent_capture_failed"] = True
        try:
            atomic_write_json(os.path.join(root, _contract_text("native_worker_diagnostics.json")), worker_diagnostics)
        except Exception:
            pass  # Secondary evidence cannot replace the native failure.
        try:
            failure_inputs = _retain_failed_capture_inputs(workspace, root, captures, invocation_prefix)
        except Exception as collection_error:
            failure_inputs = {"status": "unavailable", "qualification": "diagnostic_only",
                              "retained_scs_paths": [], "reasons": [str(collection_error)[:512]]}
        capture_diagnostics["failure_inputs"] = failure_inputs
        try:
            atomic_write_json(os.path.join(root, _contract_text("capture_diagnostics.json")), capture_diagnostics)
        except Exception:
            pass  # Diagnostic persistence cannot replace the native failure.
        _raise_capture_failure(cancelled, command, native_diagnostics, worker_diagnostics,
                               capture_diagnostics, failure_inputs, root)
    _check_cancel(cancel_check)
    from .workspace import validate_schematic_application
    baseline_clone_verification = validate_schematic_application(plan)
    native_settings = None
    if native_plan:
        from .native_maestro_parameters import parse_native_parameter_settings
        if batch_plan is None:
            native_settings = parse_native_parameter_settings(native_settings_path, native_plan)
        else:
            from .generation_batch import read_native_batch_columns
            native_settings = read_native_batch_columns(
                os.path.join(root, "native_batch_columns.tsv"), batch_plan)
    from .native_variable_scope_readback import parse_native_variable_scope
    native_scope = parse_native_variable_scope(native_scope_path, run_setup)
    workspace["setup"] = copy.deepcopy(workspace["setup"])
    if execution_selection is None:
        workspace["setup"]["native_variable_scope"] = native_scope
    run_setup["native_variable_scope"] = native_scope
    require_unique_run_variables(run_setup, action="native SCS capture", multiplicity_only=True)
    load_native_storage_readback(workspace, required=bool(workspace.get("native_storage")))
    rdb_root = capture_rdb_root(workspace)
    if batch_plan is None:
        parsed = parse_latest_rdb(rdb_root)
    else:
        from .rdb import parse_rdb
        from .workspace import _record_literal
        with open(os.path.join(root, "native_batch_run.tsv"), encoding="utf-8") as stream:
            run_id, history_name = stream.read().strip().split("\t")
        batch_plan.update(run_id=_record_literal(run_id), history=history_name)
        parsed = parse_rdb(os.path.join(rdb_root, history_name + ".rdb"))
    _regular(parsed["path"], rdb_root, MAX_NETLIST_BYTES)
    if workspace.get("native_storage"):
        history_name = os.path.basename(parsed["path"])[:-4]
        try:
            native_storage_paths = qualify_native_history_paths(workspace, root, history_name, required=True)
        except (OSError, ValueError, UnicodeError) as cause:
            from .storage_policy import native_history_readback_path
            error = PrivateNetlistUnsupported("native history path readback failed: %s" % cause)
            error.detail = {"failure_stage": "native_history_path_readback",
                            "history_readback_path": native_history_readback_path(workspace, root),
                            "capture_diagnostics": capture_diagnostics}
            raise error from cause
        command["native_storage_paths"] = native_storage_paths
    points = dict((row["pointID"], row) for row in parsed["points"])
    tasks = []
    model_resolution_cache = {}
    for filename in sorted(os.listdir(captures)):
        path = _regular(os.path.join(captures, filename), captures, maximum=256 * 1024)
        with open(path, "rb") as stream:
            fields = stream.read().split(b"\0")
        arguments = [field.decode("utf-8") for field in fields if field]
        if len(arguments) < 2: continue
        cwd, argv = arguments[0], arguments[1:]
        netlist = os.path.abspath(os.path.join(cwd, argv[0]))
        if not os.path.isfile(netlist) or not netlist.endswith(".scs"): continue
        _regular(netlist, workspace["root"])
        test_name = os.path.basename(os.path.dirname(cwd))
        point_name = os.path.basename(os.path.dirname(os.path.dirname(cwd)))
        if not point_name.isdigit() or int(point_name) not in points or test_name not in tests:
            raise PrivateNetlistUnsupported("native test/point directory identity is not supported")
        point = points[int(point_name)]
        task_id = "p%s_%s" % (point_name, hashlib.sha256(test_name.encode("utf-8")).hexdigest()[:16])
        snapshot = ensure_dir(os.path.join(root, "tasks", task_id, "netlist"))
        # Freeze only netlist text and small native name-map metadata. OA,
        # layout, PSF, CMI binaries and histories never enter candidate copies.
        files = []
        for current, directories, names in os.walk(cwd):
            directories[:] = [name for name in directories if name == "amap"] if current == cwd else directories
            for name in names:
                if current == cwd and name not in (_contract_text("input.scs"), "netlist", "runObjFile", "designInfo"):
                    continue
                source = _regular(os.path.join(current, name), workspace["root"], maximum=MAX_METADATA_BYTES)
                relative = os.path.relpath(source, cwd)
                destination = os.path.join(snapshot, relative)
                ensure_dir(os.path.dirname(destination)); shutil.copyfile(source, destination)
                files.append({"path": destination, "relative_path": relative, "sha256": _digest(destination),
                              "metadata": relative != _contract_text("input.scs")})
        outputs = [dict(key="%s::%s" % (test_name, row["name"]), name=row["name"],
                        expression=row.get("expression"), output_type=row.get("output_type"))
                   for row in parsed["outputs"] if row["test"] == test_name and row.get("name") and not row["name"].startswith("/")]
        tasks.append({"task_id": task_id, "point_id": point["pointID"], "design_point": point["designPointNumber"],
                      "corner": point["corner_name"], "test": test_name, "netlist_path": os.path.join(snapshot, _contract_text("input.scs")),
                      "cwd": snapshot, "psf_dir": os.path.dirname(snapshot) + "/psf", "spectre_log": os.path.dirname(snapshot) + "/psf/spectre.out",
                      "argv": [config["cadence"].get(capture_simulator + "_bin") or wrapper] + argv, "native_cwd": cwd,
                      "capture_executable_provenance": "configured_source_simulator" if config["cadence"].get(capture_simulator + "_bin") else "owned_capture_wrapper_only",
                      "outputs": outputs, "variables": [], "template_files": files,
                      "native_parameters": top_level_native_parameters(_text(os.path.join(snapshot, _contract_text("input.scs"))))})
        tasks[-1].update(native_netlist_identity(_text(os.path.join(snapshot, _contract_text("input.scs")))))
        from .native_identity import resolve_task_model_identity
        tasks[-1]["native_model_resolution"] = resolve_task_model_identity(
            tasks[-1], config, capture_simulator, run_setup, setup_identity, model_resolution_cache)
    schedule = parse_native_corner_schedule(schedule_path)
    setup_identity["native_schedule"] = schedule
    partitions = None
    if batch_plan is None:
        corner_coverage = validate_capture_corners(run_setup, tasks, parsed, setup_identity, schedule)
    else:
        from .generation_batch import map_native_points, parse_native_rows
        from .rdb import slice_candidate_rdb
        mapped = map_native_points(batch_plan, batch_plan['run_id'], batch_plan['history'], parsed,
            parse_native_rows(os.path.join(root, "native_batch_points.tsv")))
        partitions, coverage_rows = {}, []
        for row in batch_plan['rows']:
            ids = {point['point_id'] for point in mapped[row['row_id']]}
            row_tasks = [task for task in tasks if task['point_id'] in ids]
            part = slice_candidate_rdb(parsed, {test: test for test in tests}, ids)
            coverage = validate_capture_corners(run_setup, row_tasks, part, setup_identity, schedule)
            partitions[row['row_id']] = {'tasks': row_tasks, 'parsed': part}
            coverage_rows.append(dict(coverage, row_id=row['row_id']))
        corner_coverage = {'ok': all(row['ok'] for row in coverage_rows),
                          'rows': coverage_rows, 'enabled_tests': tests,
                          'explicit_enabled_corners': native_plan['enabled_corners']}
        batch_plan['corner_coverage'] = corner_coverage
        batch_plan['native_schedule'] = schedule
        batch_plan['job_policy'] = capture_setup_readback
    point_coverage = native_capture_point_coverage(tasks, parsed, tests, schedule=schedule)
    capture_diagnostics["point_coverage"] = point_coverage
    # Publish both independent checks before either gate raises. Expanded
    # group mapping failures can also affect masked RDB placeholder counts;
    # losing the corner evidence here used to hide the more specific cause.
    setup_identity["native_coverage"] = corner_coverage
    identity_path = os.path.join(root, _contract_text("corner_identity.json"))
    atomic_write_json(identity_path, setup_identity)
    atomic_write_json(os.path.join(root, _contract_text("capture_diagnostics.json")), capture_diagnostics)
    if not point_coverage["ok"]:
        failure = PrivateNetlistUnsupported("native netlist capture did not cover every enabled test/point (%s/%s captured)" %
                                            (point_coverage["captured_test_point_count"], point_coverage["expected_test_point_count"]))
        failure.detail = {"failure_stage": "native_capture_point_coverage", "execution_started": False,
                          "capture_only": True, "capture_capacity": int(capture_jobs),
                          "diagnostic_path": os.path.join(root, _contract_text("capture_diagnostics.json")),
                          "corner_diagnostic_path": identity_path,
                          "corner_coverage_ok": corner_coverage["ok"],
                          "point_coverage": point_coverage}
        raise failure
    if not corner_coverage["ok"]:
        failure = PrivateNetlistUnsupported("native netlist test/corner coverage differs from the inspected snapshot; see %s" % identity_path)
        failure.detail = {"failure_stage": "native_corner_coverage", "diagnostic_path": identity_path,
                          "corner_identity": corner_coverage, "execution_started": False}
        raise failure
    from .corner_selection import missing_metric_corners
    missing_metrics = missing_metric_corners(tasks, corner_selection)
    if missing_metrics:
        failure = PrivateNetlistUnsupported("selected metric corners are disabled for their Maestro test: " + str(missing_metrics))
        failure.detail = {"failure_stage": "metric_corner_coverage", "missing": missing_metrics, "execution_started": False}
        raise failure
    if native_execution_selection or execution_selection:
        workspace["_native_execution_selection_applied"] = copy.deepcopy(native_execution_selection or execution_selection)
    if native_corner_selection:
        workspace["_native_corner_selection_applied"] = copy.deepcopy(native_corner_selection)
    for task in tasks:
        task["variables"] = [{"name": row["name"], "value": row["value"]} for row in parsed["parameters"] if row["pointID"] == task["point_id"]]
    applied_values = {}
    application = {"ok": True, "proof_kind": "unchanged_native_baseline", "parameters": [],
                   "private_oa_backannotation_proven": False}
    if native_rows:
        from .native_maestro_parameters import verify_native_parameter_application
        expected = dict(seeds) if symbolic else dict(values)
        try:
            if batch_plan is None:
                application = verify_native_parameter_application(native_rows, tasks, expected,
                    run_setup, parsed, allow_effective_adjustment=allow_effective_adjustment)
            else:
                for row in batch_plan['rows']:
                    part = partitions[row['row_id']]
                    app = verify_native_parameter_application(native_rows, part['tasks'], row['values'],
                        run_setup, part['parsed'])
                    part['parameter_application'] = app
                    part['applied_values'] = {item['parameter_id']: item['effective'] for item in app['parameters']}
                application = partitions[batch_plan['rows'][0]['row_id']]['parameter_application']
        except ValueError as error:
            failure = PrivateNetlistUnsupported("Native Run parameter evidence could not be verified: " + str(error))
            failure.detail = dict(getattr(error, "detail", {}), **{"failure_stage": "native_parameter_point_verification",
                "proof_kind": "native_run_device_parameters", "requested_values": dict(expected),
                "scs_paths": [task["netlist_path"] for task in tasks],
                "native_worker_diagnostics": worker_diagnostics,
                "capture_only": True, "solver_execution_started": False})
            failure.detail.setdefault("reason", "binding_unresolved")
            raise failure
        applied_values = dict((row["parameter_id"], row["effective"]) for row in application["parameters"])
        workspace["native_parameter_paths"] = [row["path"] for row in native_plan["selected"]]
        workspace["native_parameter_last_values"] = dict((row["path"], row["value"]) for row in native_plan["selected"])
    result = {"tasks": sorted(tasks, key=lambda row: row["task_id"]), "patch_plan": plan, "command": command,
              "capture_timeout_profiles": capture_timeout_profiles,
              "capture_outer_budget": generation["capture_outer_budget"],
              "corner_identity": {"ok": True, "path": identity_path, "source_sha256": setup_identity["saved_source"]["sha256"]},
              "applied_values": applied_values, "requested_values": dict(values),
              "native_parameter_rows": native_rows, "parameter_application": application,
              "native_parameter_settings": native_settings, "native_parameter_plan": native_plan,
              "native_variable_scope_path": native_scope_path, "native_variable_scope": native_scope,
              "clone_preparation": baseline_clone_verification}
    result["native_worker_diagnostics"] = worker_diagnostics
    if batch_plan is not None:
        for part in partitions.values():
            for task in part['tasks']:
                task['variables'] = [{"name": row['name'], "value": row['value']}
                                     for row in parsed['parameters'] if row['pointID'] == task['point_id']]
        result['partitions'] = partitions
        batch_plan['point_coverage'] = point_coverage
        batch_plan['capture_only_wrapper_exit'] = 1
        batch_plan['solver_invocations'] = 0
    if baseline_only:
        result["clone_preparation"] = baseline_clone_verification
        result["unchanged_baseline"] = {
            "selected_values_modified": False,
            _contract_text("cr_parameter_callbacks_invoked"): False,
            "native_device_parameters_added": False,
            "variable_resolution": "saved_Maestro_native_test_corner_point_generation",
            "solver_completion_proven": False}
    if progress_callback:
        progress_callback({"stage": "native_corner_qualified", "message": "Native test/corner setup and coverage verified",
                           "enabled_tests": corner_coverage["enabled_tests"],
                           "explicit_enabled_corners": corner_coverage["explicit_enabled_corners"],
                           "native_task_count": len(tasks), "diagnostic_path": identity_path,
                           "rdb_path": parsed["path"], "spectre_simulations": 0, "remote_submissions": 0})
    return result


def capture_unmodified_baseline(workspace, parameters, config, root,
                                emit_clones=True, cancel_check=None,
                                progress_callback=None, private_config_initialized=False):
    """Capture native point SCS without rewriting selected original values.

    This separate entry does not qualify numeric candidates or a template.
    It preserves the saved Maestro variables/Parameters and relies on the
    existing native Run capture, setup identity and point coverage gates.
    Callers retain the private workspace until owned capture is quiescent.
    """
    if os.path.lexists(root):
        raise PrivateNetlistError("unchanged baseline capture requires a fresh root")
    _qualify_setup_capture(workspace["setup"], config.get("_capture_source_simulator", "spectre"))
    result = _capture_phase(workspace, parameters, {}, {}, {}, config, root,
                            emit_clones=emit_clones, cancel_check=cancel_check,
                            progress_callback=progress_callback,
                            private_config_initialized=private_config_initialized,
                            baseline_only=True)
    result["source_guard"] = verify_sources_unchanged(workspace)
    if not result["source_guard"].get("ok"):
        error = PrivateNetlistError("source design changed during unchanged baseline generation: " +
                                    source_change_summary(result["source_guard"]))
        error.detail = {"failure_stage": "source_freshness", "capture_only": True,
                        "solver_execution_started": False,
                        "source_guard": compact_source_guard(result["source_guard"])}
        raise error
    return result


def prepare_private_netlist(prepared, parameters, config, root, cancel_check=None, progress_callback=None, spec=None):
    from .setup_private_session import borrow_setup_private_session
    with borrow_setup_private_session(prepared, config, root, cancel_check) as session_workspace:
        return _prepare_private_netlist(prepared, parameters, config, root, cancel_check,
            progress_callback, spec, session_workspace=session_workspace)


def _prepare_private_netlist(prepared, parameters, config, root, cancel_check=None, progress_callback=None,
                             spec=None, session_workspace=None):
    """Create one isolated, native-qualified immutable template for a run."""
    from .native_qualification import NATIVE_QUALIFICATION_REVISION
    require_unique_run_variables((prepared or {}).get("setup") or {},
                                 action="prepare private netlist template", multiplicity_only=True)
    root = os.path.abspath(root)
    if os.path.lexists(root):
        raise PrivateNetlistError("private netlist preparation requires a fresh root")
    ensure_dir(root)
    if session_workspace is not None:
        from .setup_private_session import write_session_reference
        write_session_reference(root, prepared["setup_private_session"])
    if any(row.get("backend", "schematic") != "schematic" for row in parameters or []):
        raise PrivateNetlistUnsupported("selected design variables require the original Maestro execution mode")
    symbols = parameter_symbols(parameters)
    if not symbols: raise PrivateNetlistUnsupported("no selected parameters")
    config = _capture_request_config(prepared, config, spec)
    # Capture cannot run user-side scripts or Monte Carlo reconstruction. Such
    # setups remain supported by the independently selectable original mode.
    from .test_selection import effective_setup as test_setup
    setup = test_setup(prepared.get("setup") or {}, config.get("_capture_execution_selection"))
    enabled_simulators = set((row.get("simulator") or "spectre") for row in setup.get("tests") or [] if row.get("enabled", True))
    if len(enabled_simulators) > 1:
        raise PrivateNetlistUnsupported("mixed saved simulators require separate qualified capture adapters")
    capture_simulator = next(iter(enabled_simulators)) if enabled_simulators else "spectre"
    _qualify_setup_capture(setup, capture_simulator)
    config["_capture_source_simulator"] = capture_simulator
    from .simulators import execution_contract, selected_simulator, submission_cpu
    if capture_simulator == "alps" and selected_simulator(config, spec) == "spectre":
        raise PrivateNetlistUnsupported("ALPS capture to Spectre CLI is unqualified; use a saved Spectre source setup")
    # The real executor is mandatory for both choices before native capture.
    # A capture wrapper can describe a Spectre source for an ALPS CLI bridge,
    # but can never be selected as the actual Spectre/ALPS solver.
    execution_contract(config, capture_simulator, spec=spec)
    from .job_policy import effective_job_policy
    from .execution_commands import normalize_execution_commands, submission_cpu_hint
    from .netlist_execution import execution_policy
    commands = normalize_execution_commands(config, spec)
    common_policy = execution_policy(prepared, spec or {}, config)
    effective_policy = effective_job_policy(prepared.get("job_policy") or {}, (spec or {}).get("maestro_job_overrides"))
    started = time.monotonic()
    workspace = session_workspace
    if workspace is None:
        workspace = build_workspace(prepared["setup"], prepared["hierarchy"], config, os.path.join(root, "workspace"),
                                    expected_source_identity=prepared.get("source_identity"), parameters=parameters)
        if prepared["setup"].get("selected_test"):
            from .single_test import project_native_files
            project_native_files(workspace["setup_sdb"], prepared["setup"]["selected_test"],
                                 os.path.join(root, _contract_text("native_test_projection.json")))
    phases = []
    # Obtain the actual untouched native SCS first. This is not a numeric CDF
    # reference and does not infer a value by choosing an arbitrary corner.
    original_baseline = capture_unmodified_baseline(workspace, parameters, config,
        os.path.join(root, "baseline"), emit_clones=session_workspace is None,
        cancel_check=cancel_check, progress_callback=progress_callback)
    phases.append({"name": "baseline", "command": original_baseline["command"],
                   "corner_identity": original_baseline["corner_identity"],
                   "role": "unmodified_native_baseline"})
    seeds, low_values, high_values, probe_origins = _native_qualification_values(
        parameters, spec, original_baseline, workspace)
    def capture(name, values):
        _check_cancel(cancel_check)
        if progress_callback:
            progress_callback({"stage": "native_netlist_" + name,
                "message": "Qualifying native Run device parameter netlists: " + name})
        phase = _capture_phase(workspace, parameters, values, {}, {}, config, os.path.join(root, name),
                               emit_clones=False, private_config_initialized=True,
                               cancel_check=cancel_check, progress_callback=progress_callback)
        phases.append({"name": name, "command": phase["command"],
                       "corner_identity": phase["corner_identity"], "role": "qualification_probe"})
        return phase
    adapter_fallback = None
    baseline = capture("numeric", seeds)
    references = [("numeric", seeds, baseline),
                  ("reference_low", low_values, capture("reference_low", low_values)),
                  ("reference_high", high_values, capture("reference_high", high_values))]
    for key in symbols:
        seed = baseline["applied_values"][key]
        if not any(abs(phase["applied_values"][key] - seed) > max(abs(seed), 1e-300) * 1e-9
                   for _name, _values, phase in references[1:]):
            raise PrivateNetlistUnsupported("native parameter references are inconclusive after CDF clipping: %s" % key)
    unresolved = _native_unresolved_binding_fallback(references, symbols)
    if unresolved:
        guard = verify_sources_unchanged(workspace)
        if not guard.get("ok"):
            failure = PrivateNetlistError("source changed during native template qualification: " + source_change_summary(guard))
            failure.detail = {"failure_stage": "source_freshness", "capture_only": True,
                              "execution_started": False, "solver_execution_started": False,
                              "source_guard": compact_source_guard(guard)}
            raise failure
        error = PrivateNetlistUnsupported("native selected CDF field has no proven DPAR template binding; native generation required per candidate")
        error.detail = dict(unresolved, source_guard=compact_source_guard(guard),
                            execution_started=False, template_root=root)
        raise error
    symbolic = _symbolize_native_dpar_phase(baseline, symbols, os.path.join(root, "symbolic"))
    targets = dict((row["parameter_id"], (row["library"], row["cell"], row.get("view") or "schematic", row["instance"]))
                   for row in symbolic.get("native_parameter_rows") or [])
    task_maps = [dict((row["task_id"], row) for row in phase["tasks"]) for _name, _values, phase in references]
    qualification = []
    for task in symbolic["tasks"]:
        relevant = [row for row in parameters if any(row.get("design_library") == test.get("library") and row.get("design_cell") == test.get("cell") and test["name"] == task["test"]
                        for test in setup.get("tests") or [])]
        selected_symbols = dict((row["parameter_id"], symbols[row["parameter_id"]]) for row in relevant)
        if not selected_symbols:
            selected_symbols = {}  # Unchanged tests still participate in output evaluation.
        comparisons = []
        for (name, values, _phase), mapping in zip(references, task_maps):
            if set(mapping) != set(task_maps[0]) or task["task_id"] not in mapping:
                raise PrivateNetlistUnsupported("native parameterization changes test/corner/point identity")
            native_task = mapping[task["task_id"]]
            if any(native_task.get(field) != task.get(field) for field in ("point_id", "design_point", "corner", "source_corner", "test")):
                raise PrivateNetlistUnsupported("native parameterization changes test/corner/point metadata")
            comparisons.append({"name": name, "values": {key: _phase["applied_values"][key] for key in selected_symbols},
                                "text": _text(mapping[task["task_id"]]["netlist_path"])})
        text = _text(task["netlist_path"])
        if selected_symbols:
            try:
                proof = qualify_symbolic_netlist(text, comparisons, selected_symbols, targets)
            except PrivateNetlistUnsupported as error:
                if getattr(error, "detail", {}).get("native_generation_required"):
                    guard = verify_sources_unchanged(workspace)
                    if not guard.get("ok"):
                        failure = PrivateNetlistError("source changed during native template qualification: " +
                                                      source_change_summary(guard))
                        failure.detail = {"failure_stage": "source_freshness", "capture_only": True,
                                          "execution_started": False, "solver_execution_started": False,
                                          "source_guard": compact_source_guard(guard)}
                        raise failure
                    error.detail.update(source_guard=compact_source_guard(guard), task_id=task["task_id"],
                                        execution_started=False, template_root=root)
                raise
        else:
            if any(_logical_statements(text) != _logical_statements(row["text"]) for row in comparisons):
                raise PrivateNetlistUnsupported("unselected test changes during native parameterization")
            proof = {"ok": True, "symbols": {}, "references": [], "unchanged_test": True}
        qualification.append(dict(task_id=task["task_id"], **proof))
        template_text = _without_symbol_declarations(text, symbols)
        from .native_includes import NativeIncludeError, freeze_native_includes
        try:
            template_text, task["argv"], task["include_resolution"] = freeze_native_includes(
                template_text, task, config, capture_simulator, allow_candidate_parameters=True)
            from .netlist_providers import _freeze_generated_dependencies
            template_text, generated = _freeze_generated_dependencies(
                template_text, task, config, [workspace["root"]] if session_workspace is not None else [],
                cancel_check, allow_candidate_parameters=True)
            task["template_files"].extend(generated)
            task["generated_input_snapshots"] = generated
        except NativeIncludeError as error:
            failure = PrivateNetlistUnsupported(str(error))
            failure.detail = {"failure_stage": "model_include_resolution", "execution_started": False,
                              "capture_source_simulator": capture_simulator}
            raise failure
        from .simulators import execution_contract, rewrite_model_references
        properties = ((effective_policy.get("global") or {}).get("properties") or {})
        for policy_row in effective_policy.get("tests") or []:
            if policy_row.get("name") == task["test"]:
                properties = dict(properties, **(policy_row.get("properties") or {}))
                break
        task_policy = common_policy["tests"].get(task["test"], common_policy["global"])
        resource_cpu = submission_cpu_hint(commands, task_policy)
        if resource_cpu is None and not commands.get("submission_template"):
            resource_cpu = submission_cpu(properties.get("jobsubmitcommand")) if str(properties.get("distributionmethod") or "").lower() == "command" else None
        contract = execution_contract(config, capture_simulator,
                                      native_netlist_identity(template_text)["native_models"],
                                      default_executable=task["argv"][0], template_argv=task["argv"],
                                      resource_cpu=resource_cpu, spec=spec)
        task["capture_native_identity"] = native_netlist_identity(template_text)
        template_text = rewrite_model_references(template_text, contract)
        task["execution_native_identity"] = native_netlist_identity(template_text)
        task["models"] = copy.deepcopy(task["execution_native_identity"]["native_models"])
        task["simulator_contract"] = contract
        task["capture_source_simulator"] = capture_simulator
        task["execution_simulator"] = contract["execution_simulator"]
        task["simulator_log"] = task["spectre_log"]
        task["execution_model_identity"] = copy.deepcopy(contract["models"])
        atomic_write_text(task["netlist_path"], template_text)
        for row in task["template_files"]:
            row["sha256"] = _digest(row["path"])
        task["netlist_sha256"] = _digest(task["netlist_path"])
        task["artifact_sha256"] = dict((row["path"], row["sha256"]) for row in task["template_files"])
        task["metadata_files"] = [row["path"] for row in task["template_files"] if row.get("metadata")]
        from .execution_commands import build_solver_command_plan
        task["native_solver_argv"] = list(task["argv"])
        task["execution_commands"] = copy.deepcopy(commands)
        task["ahdl_dir"] = os.path.join(os.path.dirname(task["psf_dir"]), "ahdl")
        task["solver_command_plan"] = build_solver_command_plan(
            contract, task["native_solver_argv"], task["netlist_path"], task["psf_dir"], task["spectre_log"],
            task["ahdl_dir"], commands=commands)
        task["argv"] = list(task["solver_command_plan"]["argv"])
    guard = verify_sources_unchanged(workspace)
    if not guard.get("ok"):
        error = PrivateNetlistError("source changed during private netlist preparation: " +
                                    source_change_summary(guard))
        error.detail = {"failure_stage": "source_freshness", "capture_only": True,
                        "solver_execution_started": False, "source_guard": compact_source_guard(guard)}
        raise error
    manifest = {"schema_version": SCHEMA, "root": root, "created_at": utc_timestamp(),
                "capture_source_simulator": capture_simulator,
                "execution_simulator": symbolic["tasks"][0]["execution_simulator"],
                "execution_commands": commands,
                "execution_selection": copy.deepcopy(config.get("_capture_execution_selection")),
                "parameter_symbols": symbols, "seed_values": seeds, "tasks": symbolic["tasks"],
                "qualification_probe_origins": probe_origins,
                "unmodified_baseline": {"point_count": len(original_baseline["tasks"]),
                    "scs_paths": [task["netlist_path"] for task in original_baseline["tasks"]],
                    "selected_values_modified": False, "solver_completion_proven": False},
                "source_identity": prepared.get("source_identity"), "workspace": copy.deepcopy(workspace),
                "export_snapshot": symbolic.get("export_snapshot"),
                "source_guard": guard, "qualification": {"ok": True, "tasks": qualification,
                    "revision": NATIVE_QUALIFICATION_REVISION,
                    "optional_range_validation_required": False, "callback_legality_per_candidate": False,
                    "scope": "native_expression_mapping_and_recorded_reference_points"},
                "preparation": {"elapsed_seconds": time.monotonic() - started, "phases": phases,
                                "spectre_simulations": 0, "remote_submissions": 0},
                "native_reference_values": [{"name": name, "requested": values,
                    "applied": phase["applied_values"]} for name, values, phase in references],
                "candidate_value_policy": "raw_finite_values_without_CDF_quantization_or_clipping"}
    if adapter_fallback:
        manifest["numeric_adapter_fallback"] = adapter_fallback
    private_identity = {key: workspace.get(key) for key in
                        ("root", "primary_library", "setup_cell", "setup_view", "copy_policy")}
    if workspace.get("setup_sdb"):
        private_identity["setup_sha256"] = _digest(workspace["setup_sdb"])
    private_identity["views"] = [{key: row.get(key) for key in ("source", "target", "role")}
                                 for row in workspace.get("same_library_registry") or []]
    manifest["netlist_generation"] = {
        "source_mode": "private_netlist", "source": "netlist", "netlist_method": "qualified_symbolic_template", "method": "qualified_symbolic_template",
        "generation_protocol": _contract_text("crescendo.native_run_device_parameters.v1"),
        "execution_selection": copy.deepcopy(config.get("_capture_execution_selection")),
        "qualification_revision": NATIVE_QUALIFICATION_REVISION,
        "source_identity": copy.deepcopy(prepared.get("source_identity")), "private_identity": private_identity,
        "source_guard": copy.deepcopy(guard), "point_count": len(manifest["tasks"]),
        "mapping_qualification": copy.deepcopy(manifest["qualification"]),
        "capture_timeout_profiles": copy.deepcopy(symbolic.get("capture_timeout_profiles")),
        "capture_outer_budget": copy.deepcopy(symbolic.get("capture_outer_budget")),
        "solver_invocations": 0, "remote_submissions": 0}
    manifest["manifest_sha256"] = _manifest_digest(manifest)
    validate_manifest(manifest)
    atomic_write_json(os.path.join(root, _contract_text("private_netlist.json")), manifest)
    return manifest
