"""Freeze native include paths without shell execution or basename searches.

Relative references follow the captured working directory and ordered -I
arguments. Environment expansion uses the native caller, not a login shell.
This resolves locations only; simulator model-format bridges remain explicit.
"""
import os
import re
import ast
import shlex

from crescendo_engine.runtime import bind_text
_contract_text = bind_text()

from .native_environment import restore_native_environment


class NativeIncludeError(ValueError):
    def __init__(self, message):
        ValueError.__init__(self, message)
        self.detail = {"failure_stage": "model_include_resolution", "execution_started": False}


_VARIABLE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}|\$([A-Za-z_][A-Za-z0-9_]*)")
_INCLUDE = re.compile(r'(?m)^(\s*(?:include|ahdl_include)\s+)"([^"\n]+)"')
_MODEL_STRING = r'"(?:\\.|[^"\\])*"'
_MODEL_ROW = re.compile(r'\(\s*(' + _MODEL_STRING + r')(?:\s+(' + _MODEL_STRING + r'|nil))?\s*\)')
_MODEL_SECTION = re.compile(r'^[A-Za-z0-9_.-]{1,128}$')


def saved_model_file_rows(value):
    """Read literal ADE modelFiles data, never evaluating SKILL expressions.

    Saved string lists retain exact filename/section pairs. The documented
    cdsenv ``#;filename;section`` marker disables a record only in that string
    syntax; a hash in a quoted list filename remains a literal character.
    Unsupported representations are deferred to the native netlister.
    """
    text = str(value or '').strip()
    if len(text) > 128 * 1024:
        raise NativeIncludeError('Saved modelFiles exceeds the bounded metadata size')
    if text in ('', 'nil', '()', '(nil)'):
        return []
    rows = []
    if text.startswith('('):
        sequence = text
        # A sequence of literal (file section) rows or one such row.
        if text.startswith('((') or re.match(r'^\(\s*\(', text):
            if not text.endswith(')'):
                raise NativeIncludeError('Saved modelFiles literal list is incomplete')
            sequence = text[1:-1]
        cursor = 0
        for match in _MODEL_ROW.finditer(sequence):
            if sequence[cursor:match.start()].strip():
                raise NativeIncludeError('Saved modelFiles uses an unsupported expression')
            filename = ast.literal_eval(match.group(1))
            section = ast.literal_eval(match.group(2)) if match.group(2) not in (None, 'nil') else None
            rows.append({'reference': filename, 'section': section or None, 'enabled': True,
                         'format': 'saved_literal_model_files_list'})
            cursor = match.end()
        if not rows or sequence[cursor:].strip():
            raise NativeIncludeError('Saved modelFiles uses an unsupported expression')
    else:
        if re.match(r'^[A-Za-z_][A-Za-z0-9_]*\s*\(', text):
            raise NativeIncludeError('Saved modelFiles uses an unsupported expression')
        # A single quoted filename may legitimately contain spaces or '#'.
        quoted = re.fullmatch(_MODEL_STRING, text)
        if quoted:
            text = ast.literal_eval(text)
        if not text.strip():
            return []
        if ';' not in text and (quoted or re.match(r'^#\s+', text)):
            tokens = [text]
        else:
            try:
                tokens = shlex.split(text, comments=False)
            except ValueError:
                raise NativeIncludeError('Saved modelFiles string has incomplete quoting')
        for token in tokens:
            if not quoted and (token.startswith('(') or re.match(r'^[A-Za-z_][A-Za-z0-9_]*\s*\(', token)):
                raise NativeIncludeError('Saved modelFiles uses an unsupported expression')
            fields = token.split(';')
            enabled = True
            if fields[0] == '#' and len(fields) >= 2:
                enabled, fields = False, fields[1:]
            if len(fields) not in (1, 2) or not fields[0]:
                raise NativeIncludeError('Saved modelFiles string has an unsupported record')
            if not quoted and len(fields) == 1:
                from .environment_identity import _looks_like_file_reference
                if not _looks_like_file_reference(fields[0]):
                    raise NativeIncludeError('Saved modelFiles contains an unsupported bare value')
            rows.append({'reference': fields[0], 'section': (fields[1] or None) if len(fields) == 2 else None,
                         'enabled': enabled, 'format': 'saved_model_files_string'})
    if len(rows) > 256:
        raise NativeIncludeError('Saved modelFiles exceeds 256 records')
    for row in rows:
        if not row['reference'] or any(c in row['reference'] for c in ('\x00', '\n', '\r')):
            raise NativeIncludeError('Saved modelFiles filename is empty or contains controls')
        if row['section'] is not None and not _MODEL_SECTION.fullmatch(row['section']):
            raise NativeIncludeError('Saved modelFiles section uses an unsupported expression')
    return rows


def expand_native_path(value, environ, reject_unset=True):
    if not isinstance(value, str) or not value or any(char in value for char in ("\x00", "\n", "\r")):
        raise NativeIncludeError("Native include path is empty or contains controls")
    missing = []
    def replace(match):
        name = match.group(1) or match.group(2)
        if name not in environ:
            missing.append(name)
            return match.group(0)
        return str(environ[name])
    expanded = _VARIABLE.sub(replace, value)
    if reject_unset and missing:
        raise NativeIncludeError("Native include path refers to unset variable: " + ", ".join(sorted(set(missing))))
    # Only the caller HOME is used for a bare tilde. Named-user expansion is a
    # normal read-only passwd lookup; no shell or command substitution occurs.
    if expanded == "~" or expanded.startswith("~/"):
        if environ.get("HOME"):
            expanded = environ["HOME"] + expanded[1:]
        elif reject_unset:
            raise NativeIncludeError("Native include path requires caller HOME")
    elif expanded.startswith("~"):
        expanded = os.path.expanduser(expanded)
    if any(char in expanded for char in ("\x00", "\n", "\r")):
        raise NativeIncludeError("Expanded native include path contains controls")
    return expanded


def native_include_environment(config):
    return restore_native_environment(os.environ, ((config or {}).get("cadence") or {}).get("launch_environment"))


def _absolute(value, cwd, environ):
    value = expand_native_path(value, environ)
    return os.path.abspath(value if os.path.isabs(value) else os.path.join(cwd, value))


def freeze_include_argv(argv, cwd, environ, model_dir=None):
    """Return copied argv and ordered exact directories from -I/-Ipath."""
    result, directories, index = [], [], 0
    original = list(argv or [])
    while index < len(original):
        argument = original[index]
        if argument == "-I":
            if index + 1 >= len(original) or not original[index + 1]:
                raise NativeIncludeError("Native solver -I has no directory")
            directory = _absolute(original[index + 1], cwd, environ)
            result.extend(("-I", directory))
            index += 2
        elif argument.startswith("-I") and len(argument) > 2:
            directory = _absolute(argument[2:], cwd, environ)
            result.append("-I" + directory)
            index += 1
        else:
            result.append(argument)
            index += 1
            continue
        if directory not in directories:
            directories.append(directory)
    if model_dir:
        directory = _absolute(model_dir, cwd, environ)
        if directory not in directories:
            result.extend(("-I", directory))
            directories.append(directory)
    # Missing -I roots can be intentional unused vendor search entries. Keep
    # them in the actual argv; a reference must still resolve to a readable
    # exact file before freezing. Never silently replace the search order.
    return result, directories


def resolve_include_file(reference, cwd, directories, environ):
    value = expand_native_path(reference, environ)
    candidates = ([value] if os.path.isabs(value) else
                  [os.path.join(base, value) for base in [cwd] + list(directories or [])])
    for candidate in candidates:
        path = os.path.abspath(candidate)
        if os.path.isfile(path) and os.access(path, os.R_OK):
            return path
    raise NativeIncludeError("Native include is unavailable in the captured cwd/-I search order: " + reference)


def freeze_native_includes(text, task, config, capture_simulator, allow_candidate_parameters=False):
    """Freeze text and source include argv; return compact location evidence.

    A Spectre capture used for ALPS execution is resolved against Spectre's
    directories. The later execution contract alone can authorize format/file
    changes through the explicit ALPS model mapping.
    """
    if capture_simulator not in ("spectre", "alps"):
        raise NativeIncludeError("Native capture simulator is unsupported")
    cwd = task.get("native_cwd") or task.get("cwd")
    if not isinstance(cwd, str) or not os.path.isabs(cwd):
        raise NativeIncludeError("Native include resolution needs its captured absolute cwd")
    environ = native_include_environment(config)
    model_dir = ((config or {}).get("cadence") or {}).get(capture_simulator + "_model_dir")
    argv, directories = freeze_include_argv(task.get("argv"), cwd, environ, model_dir)
    references = []
    def replace(match):
        reference = match.group(2)
        if allow_candidate_parameters and reference == _contract_text("cr_candidate_parameters.scs"):
            return match.group(0)
        path = resolve_include_file(reference, cwd, directories, environ)
        references.append({"reference": reference, "path": path})
        return match.group(1) + '"' + path.replace('"', '\\"') + '"'
    frozen = _INCLUDE.sub(replace, text)
    evidence = {"schema_version": _contract_text("crescendo.native_include_resolution.v1"), "capture_source_simulator": capture_simulator,
                "native_cwd": cwd, "include_dirs": directories, "references": references,
                "search_order": "captured_cwd_then_captured_I_then_source_model_dir",
                "model_format_equivalence_claimed": False}
    return frozen, argv, evidence
