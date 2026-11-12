"""Bounded native SKILL evidence; an exit code or DONE marker is not a proof."""
from __future__ import print_function

import os
import re
import stat
import json
import collections

from crescendo_engine.runtime import bind_text
_contract_text = bind_text()

from .maestro_progress import _owned_binary_stream

MAX_SCAN_BYTES = 64 * 1024 * 1024
MAX_LINE_BYTES = 1024 * 1024
MAX_ERRORS = 12
MAX_GUARD_SCRIPT_BYTES = 65536
NATIVE_ERROR_FOLLOWING_LINES = 24
MAX_NATIVE_ERROR_CONTEXTS = 4
MAX_RUN_PREERROR_LINES = 80
MAX_RUN_PREERROR_CHARS = 512


def native_run_trace_lines(run_lines, enabled=False):
    """Wrap the owned diagnostic child's Run/Wait once, preserving its errors.

    Cadence's documented status switches retain evaluated call arguments only
    when both debugMode and traceArgs are enabled. Error stacks are capped at
    32 frames without dumping locals. Two public readers are traced in this
    diagnostic child; successful argument text stays out of the compact report.
    Native readers may still hide their input or bypass these public APIs.
    Cadence's quiet internal readers can catch parse errors as normal control
    flow. Keep errsetTrace off inside Run/Wait in both normal and diagnostic
    modes; the caller's explicit printing errset still rejects escaping errors.
    Optional setup failures are silent native probes followed by an explicit
    unavailable marker; they must not become the capture's first native error.

    The caller supplies its original one-line Run and Wait forms.  No user
    expression is parsed or evaluated by this Python helper, and normal runs
    execute their original Run and Wait forms exactly once.
    """
    original = list(run_lines)
    if len(original) != 2 or any(not isinstance(line, str) or '\n' in line or
                                  '\r' in line for line in original):
        raise ValueError("native Run trace requires the original Run and Wait forms")
    checked_run = [original[0],
        _contract_text('printf("CRESCENDO_NATIVE_RUN_VALUE runId %L\\n" runId)'),
        _contract_text('printf("CRESCENDO_NATIVE_RUN_STAGE RunIdCheck BEGIN\\n")'),
        _contract_text('unless(runId error("Crescendo native Run returned invalid runId\\n"))'),
        _contract_text('printf("CRESCENDO_NATIVE_RUN_STAGE RunIdCheck RETURN\\n")')]
    if not enabled:
        return _native_run_error_boundary_lines(
            [_contract_text('printf("CRESCENDO_NATIVE_RUN_STAGE Run BEGIN\\n")'), original[0],
             _contract_text('printf("CRESCENDO_NATIVE_RUN_STAGE Run RETURN\\n")')] + checked_run[1:] +
            [
             _contract_text('printf("CRESCENDO_NATIVE_RUN_STAGE Wait BEGIN\\n")'), original[1],
             _contract_text('printf("CRESCENDO_NATIVE_RUN_STAGE Wait RETURN\\n")')])
    stack_lines = [
        _contract_text('let((crescNativeTraceBase crescNativeTraceOptions crescNativeTraceSetup crescNativeTraceRestore crescNativeTraceReturn)'),
        _contract_text('crescNativeTraceBase=list(status(errsetTrace) status(stacktrace))'),
        'unwindProtect(',
        'progn(',
        'sstatus(errsetTrace nil)',
        'sstatus(stacktrace 0)',
        _contract_text('crescNativeTraceSetup=errset(progn('),
        _contract_text('crescNativeTraceOptions=list(status(debugMode) status(traceArgs) status(stacktraceDump) status(stackTraceFormat))'),
        'sstatus(debugMode t)',
        'sstatus(traceArgs t)',
        'sstatus(stacktraceDump nil)',
        "sstatus(stackTraceFormat 'onlyCalls)",
        'unless(status(debugMode) && status(traceArgs) && !status(stacktraceDump) error("native diagnostic status switches unavailable"))',
        't) nil)',
        _contract_text('if(crescNativeTraceSetup then'),
        'sstatus(stacktrace 32)',
        'sstatus(errsetTrace nil)',
        _contract_text('printf("CRESCENDO_NATIVE_RUN_TRACE ENABLED frames=32 locals=off arguments=evaluated best_effort=t\\n")'),
        _contract_text('printf("CRESCENDO_NATIVE_RUN_TRACE STATUS debugMode=%L traceArgs=%L stacktrace=%L stacktraceDump=%L stackTraceFormat=%L\\n" status(debugMode) status(traceArgs) status(stacktrace) status(stacktraceDump) status(stackTraceFormat))'),
        'else',
        # Even partially applied optional switches must be restored before Run.
        _contract_text('when(crescNativeTraceOptions'),
        'errset(progn(',
        _contract_text('sstatus(debugMode car(crescNativeTraceOptions))'),
        _contract_text('sstatus(traceArgs cadr(crescNativeTraceOptions))'),
        _contract_text('sstatus(stacktraceDump caddr(crescNativeTraceOptions))'),
        _contract_text('sstatus(stackTraceFormat cadddr(crescNativeTraceOptions))) nil))'),
        _contract_text('sstatus(stacktrace cadr(crescNativeTraceBase))'),
        _contract_text('sstatus(errsetTrace car(crescNativeTraceBase))'),
        _contract_text('printf("CRESCENDO_NATIVE_RUN_TRACE UNAVAILABLE original_Run_will_execute_once\\n")'),
        ')',
        *_native_setup_observation_lines('before_run'),
        _contract_text('printf("CRESCENDO_NATIVE_RUN_STAGE Run BEGIN\\n")'),
        original[0],
        _contract_text('printf("CRESCENDO_NATIVE_RUN_STAGE Run RETURN\\n")'),
        *_native_setup_observation_lines('after_run'),
    ] + checked_run[1:] + [
        _contract_text('printf("CRESCENDO_NATIVE_RUN_STAGE Wait BEGIN\\n")'),
        _contract_text('crescNativeTraceReturn=progn(') + original[1] + ')',
        _contract_text('printf("CRESCENDO_NATIVE_RUN_STAGE Wait RETURN\\n")'),
        _contract_text('crescNativeTraceReturn'),
        ')',
        'progn(',
        'sstatus(errsetTrace nil)',
        'sstatus(stacktrace 0)',
        _contract_text('crescNativeTraceRestore=errset(progn('),
        _contract_text('when(crescNativeTraceOptions'),
        _contract_text('sstatus(debugMode car(crescNativeTraceOptions))'),
        _contract_text('sstatus(traceArgs cadr(crescNativeTraceOptions))'),
        _contract_text('sstatus(stacktraceDump caddr(crescNativeTraceOptions))'),
        _contract_text('sstatus(stackTraceFormat cadddr(crescNativeTraceOptions)))'),
        't) nil)',
        _contract_text('sstatus(stacktrace cadr(crescNativeTraceBase))'),
        _contract_text('sstatus(errsetTrace car(crescNativeTraceBase))'),
        _contract_text('printf("CRESCENDO_NATIVE_RUN_TRACE RESTORE %s\\n" if(crescNativeTraceRestore then "DONE" else "UNAVAILABLE"))'),
        ')',
        ')',
        ')',
    ]
    return _native_run_error_boundary_lines(_native_reader_trace_lines(stack_lines))


def _native_setup_observation_lines(stage):
    # Child-local optional probes: nil/0 are evidence, never new run gates.
    return [
        _contract_text('let((crescObsMain crescObsActive)'),
        _contract_text("crescObsMain=errset(axlGetMainSetupDB(session) nil)"),
        _contract_text("crescObsActive=if(crescObsMain && car(crescObsMain) && !equal(car(crescObsMain) 0) then errset(axlGetActiveSetup(car(crescObsMain)) nil) else nil)"),
        _contract_text('printf("CRESCENDO_NATIVE_SETUP_OBSERVATION %s session=%%L mainSDB=%%L resolved=%%L active=%%L resolved_ok=%%L active_ok=%%L\\n" session mainSDB if(crescObsMain then car(crescObsMain) else nil) if(crescObsActive then car(crescObsActive) else nil) if(crescObsMain then t else nil) if(crescObsActive then t else nil))') % stage,
        ')']


def _native_run_error_boundary_lines(body):
    """Preserve native caught-error semantics without waiving printed errors.

    The generated bounded body owns an explicit errset(... t) and success
    latch. An escaping Run/Wait error still fails that body and prints its
    native error; all existing process, worker and point checks remain active.
    """
    return [
        _contract_text('let((crescRunSavedErrsetTrace)'),
        _contract_text('crescRunSavedErrsetTrace=status(errsetTrace)'),
        'unwindProtect(progn(',
        'sstatus(errsetTrace nil)',
        _contract_text('printf("CRESCENDO_NATIVE_RUN_ERROR_POLICY caught=quiet escaping=fatal\\n")'),
    ] + list(body) + [
        _contract_text(') sstatus(errsetTrace crescRunSavedErrsetTrace))'),
        ')',
    ]


def _native_reader_trace_lines(body):
    """Trace just two public readers in this owned diagnostic child.

    A protected C parser can bypass these entries or abbreviate arguments.
    Keep the original Run, errors and inherited function traces unchanged.
    """
    restore = [
        _contract_text('when(crescReaderPortSaved ptport=crescReaderOldPort)'),
        _contract_text('when(crescReaderReadAdded untrace(readstring) crescReaderReadAdded=nil)'),
        _contract_text('when(crescReaderLineAdded untrace(linereadstring) crescReaderLineAdded=nil)'),
        _contract_text('when(crescMainAdded untrace(axlGetMainSetupDB) crescMainAdded=nil)'),
        _contract_text('when(crescActiveAdded untrace(axlGetActiveSetup) crescActiveAdded=nil)'),
        _contract_text('when(crescSimulationAdded untrace(maeRunSimulation) crescSimulationAdded=nil)'),
        _contract_text('when(crescReaderFunctionsSaved unless(equal(crescReaderOldFunctions tracef()) error("native reader trace restoration mismatch")))'),
        't']
    return [
        _contract_text('let((crescReaderBase crescReaderOldPort crescReaderOldFunctions crescReaderPortSaved crescReaderFunctionsSaved crescReaderReadAdded crescReaderLineAdded crescMainAdded crescActiveAdded crescSimulationAdded crescReaderSetup crescReaderRestore)'),
        _contract_text('crescReaderBase=list(status(errsetTrace) status(stacktrace))'),
        'unwindProtect(progn(',
        'sstatus(errsetTrace nil) sstatus(stacktrace 0)',
        _contract_text('crescReaderSetup=errset(progn('),
        _contract_text('crescReaderOldPort=ptport crescReaderPortSaved=t'),
        _contract_text('crescReaderOldFunctions=tracef() crescReaderFunctionsSaved=t'),
        'ptport=poport',
        _contract_text("unless(memq('readstring crescReaderOldFunctions) crescReaderReadAdded=t tracef(readstring))"),
        _contract_text("unless(memq('linereadstring crescReaderOldFunctions) crescReaderLineAdded=t tracef(linereadstring))"),
        _contract_text("when(isCallable('axlGetMainSetupDB) && !memq('axlGetMainSetupDB crescReaderOldFunctions) crescMainAdded=t tracef(axlGetMainSetupDB))"),
        _contract_text("when(isCallable('axlGetActiveSetup) && !memq('axlGetActiveSetup crescReaderOldFunctions) crescActiveAdded=t tracef(axlGetActiveSetup))"),
        _contract_text("when(isCallable('maeRunSimulation) && !memq('maeRunSimulation crescReaderOldFunctions) crescSimulationAdded=t tracef(maeRunSimulation))"),
        't) nil)',
        _contract_text('if(crescReaderSetup then'),
        _contract_text('printf("CRESCENDO_NATIVE_READER_TRACE ENABLED public=readstring,linereadstring private_parser_coverage=unknown\\n")'),
        _contract_text("printf(\"CRESCENDO_NATIVE_SETUP_TRACE main=%L active=%L run=%L\\n\" if(memq('axlGetMainSetupDB tracef()) then t else nil) if(memq('axlGetActiveSetup tracef()) then t else nil) if(memq('maeRunSimulation tracef()) then t else nil))"),
        'else',
        _contract_text('crescReaderRestore=errset(progn(')] + restore + [') nil)',
        _contract_text('printf("CRESCENDO_NATIVE_READER_TRACE UNAVAILABLE original_Run_will_execute_once\\n")'),
        ')',
        _contract_text('sstatus(stacktrace cadr(crescReaderBase)) sstatus(errsetTrace car(crescReaderBase))')] + list(body) + [
        ') progn(',
        'sstatus(errsetTrace nil) sstatus(stacktrace 0)',
        _contract_text('crescReaderRestore=errset(progn(')] + restore + [') nil)',
        _contract_text('sstatus(stacktrace cadr(crescReaderBase)) sstatus(errsetTrace car(crescReaderBase))'),
        _contract_text('printf("CRESCENDO_NATIVE_READER_TRACE RESTORE %s\\n" if(crescReaderRestore then "DONE" else "UNAVAILABLE"))'),
        '))',
        ')']


_READER_ENTRY = re.compile(r'^\s*\|+\s*\[(\d+)\]\s*(readstring|linereadstring)\(')
_READER_RETURN = re.compile(r'^\s*\|+\s*\[(\d+)\]\s*(readstring|linereadstring)\s+-->')
MAX_READER_ENTRY_CHARS = 2048
MAX_READER_ERROR_DISTANCE = 12

_CODED_ERROR = re.compile(r'^\s*ERROR\s*\(([A-Z][A-Z0-9_]*-\d+)\)\s*:', re.I)
_CRESCENDO_GUARD = re.compile(_contract_text(r'^\s*\*Error\*\s+(Crescendo [^\r\n]+)'))

_SYNTAX = re.compile(r'\*Error\*\s+(?:lineread/read|read|load|eval).*?syntax error', re.I)
_LOCATION = re.compile(r'line\s+(\d+)\s+column\s+(\d+)\s+in file\s+(.+?)\s*$', re.I)
_DISPLAY_FATAL = re.compile(r'^\s*Aborting due to fatal X IO error\.\s*$', re.I)
_BLOCK = re.compile(_contract_text(r'CRESCENDO_SKILL_BLOCK\s+(\S+)\s+(\d+)/(\d+)\s+lines=(\d+)\.\.(\d+)'))
_CDF_MARKER = re.compile(_contract_text(r'^\s*CRESCENDO_CDF_CALLBACK_INPUT\s+(.+)$'))
_CDF_FAILURE = re.compile(r'^\s*\*Error\*.*\bCDF callback failed for ([^:\s]+):')
_INT_ERROR = re.compile(r'^\s*\*Error\*\s+int:\s+can\'t handle int\(\s*("(?:\\.|[^"\\])*")\s*\)')


def native_error_context(lines, index, start_line=1, following_lines=NATIVE_ERROR_FOLLOWING_LINES):
    """Preserve a native error's printed frames as bounded raw evidence.

    Normal captures retain 16 frames; diagnostic Run can retain 32 with actual
    arguments. The default 24 following lines leave room for a trace header. We
    do not invent frames, infer a stack from generated guards, or read files.
    """
    first, last = max(0, index - 2), min(len(lines), index + 1 + following_lines)
    return {'error_line': start_line + index,
        'lines': [{'line': start_line + number, 'text': str(lines[number])[:1024]}
                  for number in range(first, last)],
        'following_line_limit': following_lines,
        'following_window_exhausted': last == index + 1 + following_lines,
        'scope': 'raw native error-adjacent console; not a generated guard or reconstructed traceback'}


def _cdf_marker_value(text):
    """Read a bounded printed alist, never evaluate native text or symbols."""
    if len(text) > 32768:
        raise ValueError("CDF marker exceeds parser bound")
    tokens = re.findall(r'"(?:\\.|[^"\\])*"|[()]|[^\s()]+', text)
    if len(tokens) > 2048:
        raise ValueError("CDF marker exceeds token bound")
    position = [0]
    def read(depth=0):
        if depth > 10 or position[0] >= len(tokens):
            raise ValueError("Incomplete or nested CDF marker")
        token = tokens[position[0]]
        position[0] += 1
        if token == '(':
            result = []
            while position[0] < len(tokens) and tokens[position[0]] != ')':
                result.append(read(depth + 1))
            if position[0] >= len(tokens):
                raise ValueError("Unclosed CDF marker")
            position[0] += 1
            return result
        if token == ')':
            raise ValueError("Unexpected CDF marker delimiter")
        if token.startswith('"'):
            try:
                return json.loads(token)
            except ValueError:
                raise ValueError("Unsupported CDF string literal")
        if token in ('nil', 't'):
            return token == 't'
        if re.match(r'^-?[0-9]+$', token):
            return int(token)
        if re.match(r'^-?(?:[0-9]+\.[0-9]*|[0-9]*\.[0-9]+)(?:[eE][+-]?[0-9]+)?$', token):
            return float(token)
        # Unsupported SKILL printer objects are opaque diagnostic literals.
        return token[:256]
    parsed = read()
    if position[0] != len(tokens) or not isinstance(parsed, list):
        raise ValueError("Trailing or non-list CDF marker")
    def alist(rows):
        if not isinstance(rows, list) or any(not isinstance(row, list) or len(row) != 2
                                            or not isinstance(row[0], str) for row in rows):
            raise ValueError("Invalid CDF marker fields")
        if len(set(row[0] for row in rows)) != len(rows):
            raise ValueError("Duplicate CDF marker field")
        return dict(rows)
    value = alist(parsed)
    if value.get('schema_version') != _contract_text('crescendo.cdf_callback_input.v1'):
        raise ValueError("Unrecognized CDF marker schema")
    fields = value.get('expression_fields') or []
    if not isinstance(fields, list) or len(fields) > 16:
        raise ValueError("Invalid bounded CDF expression fields")
    value['expression_fields'] = [alist(row) for row in fields]
    selected = value.get('selected_parameter_names')
    if selected is not None and (not isinstance(selected, list) or len(selected) > 32
                                 or any(not isinstance(name, str) or len(name) > 64
                                        for name in selected)):
        raise ValueError("Invalid selected CDF parameter names")
    for field in value['expression_fields']:
        for key, limit in (('name', 64), ('value', 192), ('type', 32),
                           ('parse_as_cel', 16), ('parse_as_number', 16)):
            item = field.get(key)
            if key in ('name', 'value') and not isinstance(item, str):
                raise ValueError("Invalid CDF candidate string")
            if item is not None and not isinstance(item, (str, bool)):
                raise ValueError("Invalid CDF candidate metadata")
            if isinstance(item, str) and len(item) > limit:
                raise ValueError("CDF candidate exceeds schema bound")
        for key in ('name_truncated', 'value_truncated', 'metadata_truncated'):
            if key in field and not isinstance(field[key], bool):
                raise ValueError("Invalid CDF candidate truncation flag")
    for key in ('property', 'property_type', 'parameter_id', 'hierarchy_path',
                'requested_value', 'db_property_value', 'cdf_value', 'callback',
                'cdf_parse_as_cel', 'cdf_parse_as_number', 'form_init_proc', 'done_proc'):
        if key in value and not isinstance(value[key], (str, int, float, bool, type(None))):
            raise ValueError("Invalid CDF scalar field")
    return value


def cdf_callback_failures(text, console_source=None):
    """Associate only actual same-console failure markers, without inference.

    Candidate strings are a failure-time observation. A matching int argument
    suggests an association; it never proves which field the vendor read.
    """
    lines, result = str(text)[:MAX_SCAN_BYTES].splitlines(), []
    previous_marker = -1
    for index, line in enumerate(lines):
        marker = _CDF_MARKER.match(line)
        if not marker:
            continue
        lower = max(previous_marker + 1, index - 16)
        lower = max([lower] + [number + 1 for number in range(lower, index)
                              if _CDF_FAILURE.match(lines[number])
                              or lines[number].startswith('[diagnostic byte gap:')])
        previous_marker = index
        try:
            value = _cdf_marker_value(marker.group(1))
        except (ValueError, TypeError, OverflowError):
            continue
        prop = value.get('property')
        if not isinstance(prop, str) or not prop:
            continue
        callback = None
        for number, candidate in enumerate(lines[index + 1:index + 13], index + 1):
            if _CDF_MARKER.match(candidate) or candidate.startswith('[diagnostic byte gap:'):
                break
            failure = _CDF_FAILURE.match(candidate)
            if failure:
                if failure.group(1) == prop:
                    callback = (number, candidate)
                break
        if callback is None:
            continue
        original = next(((number, candidate, _INT_ERROR.match(candidate)) for number, candidate in
                         reversed(list(enumerate(lines[lower:index], lower)))
                         if _INT_ERROR.match(candidate)), None)
        item = dict((key, value[key]) for key in (
            'schema_version', 'parameter_id', 'instance_lcv', 'hierarchy_path', 'property',
            'property_type', 'requested_value', 'db_property_present', 'db_property_value',
            'cdf_value', 'callback', 'callback_truncated', 'cdf_parse_as_cel',
            'cdf_parse_as_number', 'selected_cdf_metadata_truncated',
            'selected_parameter_names', 'selected_parameter_names_truncated', 'expression_fields',
            'expression_fields_status', 'expression_fields_truncated', 'expression_fields_scanned',
            'expression_fields_character_budget', 'remaining_cdf_fields_uninspected',
            'form_init_proc', 'form_init_proc_truncated', 'done_proc', 'done_proc_truncated',
            'capture_stage')
                    if key in value)
        item.update(schema_version=_contract_text('crescendo.cdf_callback_failure.v1'), console_source=console_source,
                    console_line=index + 1, callback_error=callback[1][:2048],
                    callback_error_line=callback[0] + 1, matching_expression_fields=[],
                    scope='same current console callback failure; candidate strings are not evaluated')
        if original:
            item.update(native_error=original[1][:2048], native_error_line=original[0] + 1)
            try:
                argument = json.loads(original[2].group(1))
            except ValueError:
                argument = None
            item['matching_expression_fields'] = [
                field.get('name') for field in item.get('expression_fields') or []
                if isinstance(field.get('name'), str) and field.get('value') == argument
                and not field.get('value_truncated') and not field.get('name_truncated')]
        result.append(item)
        if len(result) >= 8:
            break
    return result


def _script_context(path, root, line, column):
    """Treat impossible physical locations as an indirect reader location.

    Cadence can report line 1 for a parsed string rather than the loaded file.
    Do not invent a location or silently rewrite generated code based on it.
    """
    result = {"script_path": path, "reported_line": line, "reported_column": column}
    lines = []
    with _owned_binary_stream(path, root) as stream:
        if stream is None or os.fstat(stream.fileno()).st_size > MAX_SCAN_BYTES:
            result["context_status"] = "script_unavailable_or_over_limit"
            return result
        for number, raw in enumerate(stream, 1):
            if number > line + 2:
                break
            if number >= max(1, line - 2):
                text = raw.decode("utf-8", "replace").rstrip("\r\n")
                lines.append({"line": number, "text": text[:1000], "physical_line_characters": len(text)})
    actual = next((row for row in lines if row["line"] == line), None)
    if actual is None:
        result["context_status"] = "reported_line_outside_script"
    elif column < 1 or column > actual["physical_line_characters"] + 1:
        result["context_status"] = "reported_column_outside_physical_line_possible_indirect_reader"
        result["physical_line_characters"] = actual["physical_line_characters"]
    else:
        result["context_status"] = "physical_location_matches"
    result["source_context"] = lines
    return result


def generated_guard_context(text, message):
    """Associate a unique generated guard literal, never invent a native stack."""
    match = _CRESCENDO_GUARD.match(message)
    if not match:
        return None
    marker = match.group(1).strip()
    literal = json.dumps(marker, ensure_ascii=False)[:-1] + '\\n"'
    lines = text.splitlines()
    matches = [number for number, line in enumerate(lines, 1) if literal in line]
    result = {"association": _contract_text("unique generated Crescendo guard literal; not a native stack location"),
              "native_stack_location": False, "guard_message": marker,
              "matching_lines": matches[:8], "match_count": len(matches),
              "context_status": "unique_guard_match" if len(matches) == 1 else "guard_not_unique_or_absent"}
    if len(matches) == 1:
        number = matches[0]
        result["source_context"] = [{"line": i + 1, "text": lines[i][:1000]}
            for i in range(max(0, number - 4), min(len(lines), number + 3))]
    return result


def skill_command_diagnostics(command, script_path, required_marker=None, owned_root=None):
    """Scan this owned command's console, retaining only bounded useful evidence.

    Intentional capture-only simulator exits (ASSEMBLER-5031) are observations,
    not SKILL syntax errors. This helper never decides simulator completion.
    """
    root = os.path.abspath(owned_root or os.path.dirname(script_path))
    errors, seen = [], set()
    marker_seen = False
    last_block = None
    expected_capture_failures = 0
    inspected = []
    scan_complete = True
    recent_console, callback_failures = {}, []
    native_contexts, pending_context = [], {}
    native_run_traces = {}
    native_run_evidence = []
    reader_traces, reader_pending = {}, {}
    terminal_error = None
    run_prefixes, run_prefix_evidence, setup_observations = {}, [], []

    def inspect_line(raw, source, number):
        nonlocal marker_seen, last_block, expected_capture_failures, terminal_error
        text = raw.decode("utf-8", "replace") if isinstance(raw, bytes) else str(raw)
        if source in ('ocean.stdout', 'ocean.stderr'):
            marker = text.strip()
            if marker.startswith((_contract_text('CRESCENDO_NATIVE_SETUP_OBSERVATION '), _contract_text('CRESCENDO_NATIVE_SETUP_TRACE '))) and len(setup_observations) < 8:
                setup_observations.append({'source': source, 'line': number, 'text': marker[:1024]})
            evidence = re.match(_contract_text(r'^CRESCENDO_NATIVE_RUN_(STAGE (RunIdCheck|HistoryLookup|HistoryName) (BEGIN|RETURN)|VALUE (runId|history) (.*))$'), marker)
            if evidence and len(native_run_evidence) < 24:
                if evidence.group(2):
                    native_run_evidence.append({'source': source, 'line': number,
                        'stage': evidence.group(2), 'event': evidence.group(3)})
                else:
                    native_run_evidence.append({'source': source, 'line': number,
                        'value_name': evidence.group(4), 'value': evidence.group(5)[:1024]})
            if marker.startswith(_contract_text('CRESCENDO_NATIVE_READER_TRACE ')):
                reader = reader_traces.setdefault(source, {'console_source': source,
                    'entry_count': 0, 'return_count': 0, 'abbreviated_entry_count': 0,
                    'scope': 'two public reader APIs; private C parser coverage unknown'})
                if marker.startswith(_contract_text('CRESCENDO_NATIVE_READER_TRACE ENABLED ')):
                    reader.update(availability='ENABLED', active=True)
                elif marker.startswith(_contract_text('CRESCENDO_NATIVE_READER_TRACE UNAVAILABLE ')):
                    reader.update(availability='UNAVAILABLE', active=False)
                    reader_pending[source] = []
                elif marker.startswith(_contract_text('CRESCENDO_NATIVE_READER_TRACE RESTORE ')):
                    reader.update(active=False, restore_status=marker.rsplit(' ', 1)[-1])
                    reader_pending[source] = []
            if marker.startswith(_contract_text('CRESCENDO_NATIVE_RUN_TRACE ')) or marker.startswith(_contract_text('CRESCENDO_NATIVE_RUN_STAGE ')):
                trace = native_run_traces.setdefault(source, {
                    'console_source': source, 'arguments_enabled': False, 'stages': [],
                    'scope': 'observed diagnostic child markers; not solver or completion proof'})
                if marker.startswith(_contract_text('CRESCENDO_NATIVE_RUN_TRACE ENABLED ')):
                    trace['arguments_enabled'] = True
                    trace['availability'] = 'ENABLED'
                elif marker.startswith(_contract_text('CRESCENDO_NATIVE_RUN_TRACE UNAVAILABLE ')):
                    trace['availability'] = 'UNAVAILABLE'
                    trace['arguments_enabled'] = False
                elif marker.startswith(_contract_text('CRESCENDO_NATIVE_RUN_TRACE RESTORE ')):
                    trace['restore_status'] = marker.rsplit(' ', 1)[-1]
                    trace['active_stage'] = None
                    trace['stage_marker_line'] = None
                stage = re.match(_contract_text(r'^CRESCENDO_NATIVE_RUN_STAGE (Run|Wait) (BEGIN|RETURN)$'), marker)
                if stage:
                    if stage.group(1) == 'Run' and stage.group(2) == 'BEGIN':
                        run_prefixes[source] = {'lines': collections.deque(maxlen=MAX_RUN_PREERROR_LINES), 'seen': 0, 'filtered': 0, 'frozen': False, 'begin_line': number}
                    if stage.group(1) == 'Run' and stage.group(2) == 'RETURN':
                        run_prefixes.pop(source, None)
                    reader_pending[source] = []
                    event = {'stage': stage.group(1), 'event': stage.group(2), 'line': number}
                    if len(trace['stages']) < 8:
                        trace['stages'].append(event)
                    trace['active_stage'] = stage.group(1) if stage.group(2) == 'BEGIN' else None
                    trace['stage_marker_line'] = number
            reader = reader_traces.get(source) or {}
            if reader.get('active') and (native_run_traces.get(source) or {}).get('active_stage'):
                entry, returned = _READER_ENTRY.match(text), _READER_RETURN.match(text)
                if entry:
                    depth = int(entry.group(1))
                    reader['entry_count'] += 1
                    raw_entry = text.rstrip('\r\n')
                    abbreviated = bool(re.search(r'\(\s*\.\.\.\s*\)\s*$', raw_entry))
                    reader['abbreviated_entry_count'] += int(abbreviated)
                    pending_readers = [item for item in reader_pending.get(source, [])
                                       if item['stack_depth'] < depth]
                    pending_readers.append({'trace_line': number, 'function': entry.group(2),
                        'stack_depth': depth, 'raw_trace_entry': raw_entry[:MAX_READER_ENTRY_CHARS],
                        'truncated': len(raw_entry) > MAX_READER_ENTRY_CHARS,
                        'argument_abbreviated': abbreviated})
                    reader_pending[source] = pending_readers[-4:]
                elif returned:
                    reader['return_count'] += 1
                    reader_pending[source] = [item for item in reader_pending.get(source, [])
                        if item['stack_depth'] < int(returned.group(1))]
        prefix = run_prefixes.get(source)
        if prefix and not prefix['frozen']:
            if _READER_ENTRY.match(text) or _READER_RETURN.match(text):
                prefix['filtered'] += 1
            else:
                prefix['seen'] += 1
                prefix['lines'].append({'line': number, 'text': text.rstrip('\r\n')[:MAX_RUN_PREERROR_CHARS], 'text_truncated': len(text.rstrip('\r\n')) > MAX_RUN_PREERROR_CHARS})
        recent = recent_console.setdefault(source, collections.deque(maxlen=32))
        recent.append((number, text[:32768].rstrip('\r\n')))
        pending = pending_context.get(source)
        if pending and pending['error_line'] < number <= pending['error_line'] + pending['following_line_limit']:
            pending['lines'].append({'line': number, 'text': text.rstrip('\r\n')[:1024]})
            pending['following_window_exhausted'] = number == pending['error_line'] + pending['following_line_limit']
        if source in ('ocean.stdout', 'ocean.stderr') and _CDF_FAILURE.match(text) and len(callback_failures) < 8:
            found = cdf_callback_failures('\n'.join(row[1] for row in recent), source)
            for item in found:
                for field in ('console_line', 'callback_error_line', 'native_error_line'):
                    if field in item:
                        item[field] += recent[0][0] - 1
                if item not in callback_failures:
                    callback_failures.append(item)
        if required_marker and required_marker in text:
            marker_seen = True
        block = _BLOCK.search(text)
        if block:
            last_block = {"name": block.group(1), "block": int(block.group(2)),
                          "blocks": int(block.group(3)), "body_first_line": int(block.group(4)),
                          "body_last_line": int(block.group(5))}
        coded = _CODED_ERROR.match(text)
        display_fatal = bool(_DISPLAY_FATAL.match(text))
        # Keep the existing 5031-only exclusion. A required DONE marker is not
        # proof that a wrapper ran, so other coded errors are never waived here.
        capture_aftermath = bool(coded and coded.group(1).upper() == 'ASSEMBLER-5031')
        if "ASSEMBLER-5031" in text:
            expected_capture_failures += 1
        if capture_aftermath or (not display_fatal and "*Error*" not in text and not coded and not re.search(r'\bsyntax error\b', text, re.I)):
            return
        if prefix and not prefix['frozen'] and len(run_prefix_evidence) < MAX_NATIVE_ERROR_CONTEXTS:
            prefix['frozen'] = True
            rows = list(prefix['lines'])
            run_prefix_evidence.append({'source': source, 'begin_line': prefix['begin_line'], 'error_line': number, 'lines': rows, 'reader_trace_lines_filtered': prefix['filtered'], 'earlier_lines_omitted': max(0, prefix['seen'] - len(rows)), 'truncated': prefix['seen'] > len(rows) or any(row['text_truncated'] for row in rows), 'scope': 'same-console Run BEGIN through first reported error; reader trace lines filtered'})
        key = text.strip()
        terminal_error = {"message": key[:2048], "console_source": source, "console_line": number,
            "kind": "native_display_error" if display_fatal else "skill_syntax_error" if _SYNTAX.search(key) or re.search(r'\bsyntax error\b', key, re.I) else "native_error"}
        if coded:
            terminal_error['native_code'] = coded.group(1).upper()
        if key in seen:
            return
        seen.add(key)
        if (not pending or number > pending['error_line'] + pending['following_line_limit']) and len(native_contexts) < MAX_NATIVE_ERROR_CONTEXTS:
            trace = native_run_traces.get(source) or {}
            context = native_error_context([item[1] for item in recent], len(recent)-1,
                start_line=recent[0][0], following_lines=2 if display_fatal else
                40 if trace.get('arguments_enabled') and trace.get('active_stage') else NATIVE_ERROR_FOLLOWING_LINES)
            context['console_source'] = source
            native_contexts.append(context)
            pending_context[source] = context
        if len(errors) >= MAX_ERRORS:
            return
        location = _LOCATION.search(key)
        row = {"message": key[:2048], "console_source": source, "console_line": number,
               "kind": "native_display_error" if display_fatal else "skill_syntax_error" if _SYNTAX.search(key) or re.search(r'\bsyntax error\b', key, re.I) else "native_error",
               "last_successful_block": dict(last_block) if last_block else None}
        trace = native_run_traces.get(source) or {}
        if trace.get('active_stage'):
            row['native_run_boundary'] = {
                'stage': trace['active_stage'], 'marker_line': trace.get('stage_marker_line'),
                'arguments_enabled': trace.get('arguments_enabled', False),
                'scope': 'same-console Run/Wait marker before this error; reader input not inferred'}
            reader = reader_traces.get(source) or {}
            if row['kind'] == 'skill_syntax_error' and reader.get('active'):
                candidates = [item for item in reader_pending.get(source, [])
                    if 0 < number - item['trace_line'] <= MAX_READER_ERROR_DISTANCE]
                evidence = {'state': 'not_observed', 'console_source': source, 'error_line': number,
                    'qualification': 'diagnostic_only', 'private_parser_may_bypass': True,
                    'scope': 'nearest unreturned public reader entry in this Run/Wait window; not proof of a private C parser input'}
                if candidates:
                    evidence.update(candidates[-1])
                    evidence.update(state='preceding_unreturned_call',
                        distance_lines=number-evidence['trace_line'],
                        argument_completeness='abbreviated_or_truncated' if
                            evidence['truncated'] or evidence['argument_abbreviated'] else 'printed_line_only')
                row['native_reader_call'] = evidence
        if coded:
            row['native_code'] = coded.group(1).upper()
        if location:
            row["reported_file"] = location.group(3)[:2048]
            row["reported_line"] = int(location.group(1))
            row["reported_column"] = int(location.group(2))
            reported = os.path.abspath(location.group(3))
            if reported == os.path.abspath(script_path):
                row["script_context"] = _script_context(script_path, root, row["reported_line"], row["reported_column"])
        elif _CRESCENDO_GUARD.match(key):
            with _owned_binary_stream(script_path, root) as stream:
                if stream is not None and os.fstat(stream.fileno()).st_size <= MAX_GUARD_SCRIPT_BYTES:
                    context = generated_guard_context(stream.read(MAX_GUARD_SCRIPT_BYTES).decode('utf-8', 'replace'), key)
                    row['generated_guard_context'] = dict(context, script_path=script_path)
        if display_fatal:
            row["explanation"] = "Native Ocean lost its X display connection; generated files do not prove this command completed."
        errors.append(row)
        terminal_error = row

    for name in ("ocean.stdout", "ocean.stderr"):
        path = os.path.join(root, name)
        total = 0
        with _owned_binary_stream(path, root) as stream:
            if stream is None:
                continue
            inspected.append(name)
            for number in range(1, 10000000):
                raw = stream.readline(MAX_LINE_BYTES + 1)
                if not raw:
                    break
                total += len(raw)
                if len(raw) > MAX_LINE_BYTES or total > MAX_SCAN_BYTES:
                    scan_complete = False
                    break
                inspect_line(raw, name, number)
    if not inspected:
        # Injected runners may provide only tails. Keep this scope explicit.
        for name in ("stdout_tail", "stderr_tail"):
            for number, line in enumerate(str(command.get(name) or "").splitlines(), 1):
                inspect_line(line, name, number)
    syntax_errors = [row for row in errors if row["kind"] == "skill_syntax_error"]
    result = {"schema_version": _contract_text("crescendo.native_skill_diagnostics.v1"),
            "console_scope": "owned_full_logs" if inspected else "command_tails_only",
            "inspected_logs": inspected, "scan_complete": scan_complete,
            "syntax_error": bool(syntax_errors), "native_error": bool(errors),
            "errors": errors, "distinct_error_count": len(seen),
            "first_error": errors[0] if errors else None,
            "terminal_error": terminal_error,
            "error_order_scope": "per-console scan order; stdout/stderr ordering is not a global timestamp order",
            "omitted_error_count": max(0, len(seen) - len(errors)),
            "required_marker": required_marker, "required_marker_seen": marker_seen,
            "last_successful_block": last_block,
            "capture_wrapper_failure_observations": expected_capture_failures,
            "process_ok": command.get("returncode") == 0 and not any(command.get(key) for key in
                ("timed_out", "cancelled", "termination_failed")),
            "ok": command.get("returncode") == 0 and scan_complete and not errors
                and (not required_marker or marker_seen)
                and not any(command.get(key) for key in ("timed_out", "cancelled", "termination_failed"))}
    if setup_observations:
        result['native_setup_observations'] = setup_observations
    if run_prefix_evidence:
        result['native_run_preerror'] = run_prefix_evidence
    if callback_failures:
        result['cdf_callback_failures'] = callback_failures
        result['cdf_callback_failures_truncated'] = len(callback_failures) >= 8 or not scan_complete
    if native_contexts:
        result['native_error_contexts'] = native_contexts
        result['native_error_context_limit'] = MAX_NATIVE_ERROR_CONTEXTS
    if native_run_evidence:
        result['native_run_evidence'] = native_run_evidence
    if native_run_traces:
        result['native_run_trace'] = list(native_run_traces.values())
    if reader_traces:
        result['native_reader_trace'] = [dict((key, value) for key, value in row.items() if key != 'active')
                                         for row in reader_traces.values()]
    return result
