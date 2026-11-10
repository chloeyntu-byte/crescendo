"""Generate selected-property writes and read-only CDF callback evidence.

Additional failure metadata is bounded and printed only after a callback fails.
Successful callback value proofs go to internal verification records only.
These helpers never evaluate design variables or initialize a vendor form.
Related strings are possible expressions, not dependency proof.
"""
from __future__ import print_function


MAX_SCAN_FIELDS = 256
MAX_EXPRESSION_FIELDS = 16
MAX_VALUE_CHARS = 192
MAX_NAME_CHARS = 64
MAX_TYPE_CHARS = 32
MAX_FLAG_CHARS = 16
MAX_EXPRESSION_CHARS = 2048
MAX_SELECTED_NAMES = 32
MAX_CALLBACK_CHARS = 512
MAX_FORM_PROC_CHARS = 256


def _bounded_scalar(expression, limit):
    """Read only scalar metadata; never format arbitrary CDF/OA objects."""
    return ('let((crescDiagScalar) crescDiagScalar = {0} '
            'when(symbolp(crescDiagScalar) && crescDiagScalar && crescDiagScalar != t '
            'crescDiagScalar = symbolToString(crescDiagScalar)) '
            'if(stringp(crescDiagScalar) then '
            'if(strlen(crescDiagScalar) > {1} then substring(crescDiagScalar 1 {1}) else crescDiagScalar) '
            'else if(crescDiagScalar == nil || crescDiagScalar == t then crescDiagScalar else "<non-scalar>")))').format(
                expression, limit)


def _scalar_truncated(expression, limit):
    return ('let((crescDiagScalar) crescDiagScalar = {0} '
            'when(symbolp(crescDiagScalar) && crescDiagScalar && crescDiagScalar != t '
            'crescDiagScalar = symbolToString(crescDiagScalar)) '
            'stringp(crescDiagScalar) && strlen(crescDiagScalar) > {1})').format(expression, limit)


def selected_property_assignment_lines(row, literal, quote, generic=True):
    """Preassign all selected generic values, including CDF-only defaults.

    Custom adapters retain their original property/transaction responsibility.
    No unselected property is created or rewritten.
    """
    name = quote(row['property'])
    assignment = 'dbReplaceProp(inst "%s" "%s" %s)' % (name, row['prop_type'], literal)
    return ['existingProp = dbFindProp(inst "%s")' % name,
            assignment if generic else 'when(existingProp %s)' % assignment]


def callback_value_proof_lines(row, quote):
    """Compare a successful callback's scalar with fresh CDF and OA evidence.

    The caller snapshots the native default before the callback and its effective
    value before the optional update. This block only reads and records evidence;
    it never commits CDF.
    Keep the same callback ``when`` boundary so symbolic export removes it.
    """
    name = quote(row['property'])
    return [
        'when(callbackExpr && callbackExpr != ""',
        '  crescCallbackFreshCdf = cdfGetInstCDF(inst)',
        '  unless(crescCallbackFreshCdf error("Crescendo callback value proof cannot refresh CDF for %s\\n"))' % name,
        '  crescCallbackFreshParam = get(crescCallbackFreshCdf stringToSymbol("%s"))' % name,
        '  unless(crescCallbackFreshParam error("Crescendo callback value proof parameter missing: %s\\n"))' % name,
        '  crescCallbackOaProp = dbFindProp(inst "%s")' % name,
        '  fprintf(crescVerify "CDF_CALLBACK_EFFECTIVE\\t%%L\\t%%L\\t%%L\\t%%L\\t%%L\\t%%L\\t%%L\\n" "%s" crescCallbackEffectiveValue crescCallbackFreshParam~>value if(crescCallbackOaProp then t else nil) if(crescCallbackOaProp then crescCallbackOaProp~>value else nil) crescCallbackDefaultPresent if(crescCallbackDefaultPresent then crescCallbackDefaultValue else nil))' % quote(row['parameter_id']),
        ')',
    ]


def callback_failure_lines(row, literal, library, cell, view, instance,
                           selected_names, quote):
    """Keep entry evidence cheap; inspect related fields after failure only.

    The returned body stays inside the caller's existing callback ``when`` so
    symbolic export removes it with the callback. Evidence failure cannot turn
    a failed callback into success or replace the original adapter error.
    """
    names = sorted(set(str(name) for name in selected_names))
    names_literal = 'list(%s)' % ' '.join('"%s"' % quote(name) for name in names)
    reported_names = 'list(%s)' % ' '.join(
        '"%s"' % quote(name[:MAX_NAME_CHARS]) for name in names[:MAX_SELECTED_NAMES])
    identity = [quote(str(value or '')) for value in
                (row['property'], row['parameter_id'], library, cell, view, instance,
                 row.get('hierarchy_path') or '', row['property'], row['prop_type'])]
    entry = ('crescCallbackInput = errset(progn(crescCallbackInput = dbFindProp(inst "%s") '
             'list(list("schema_version" "crescendo.cdf_callback_input.v1") '
             'list("parameter_id" "%s") list("instance_lcv" list("%s" "%s" "%s" "%s")) '
             'list("hierarchy_path" "%s") list("property" "%s") list("property_type" "%s") '
             'list("requested_value" %s) list("db_property_present" if(crescCallbackInput then t else nil)) '
             'list("db_property_value" if(crescCallbackInput then crescCallbackInput~>value else nil)) '
             'list("cdf_value" cdfParam~>value) list("callback" %s) '
             'list("callback_truncated" %s) '
             'list("selected_parameter_names" %s) list("selected_parameter_names_truncated" %s) '
             'list("cdf_parse_as_cel" %s) list("cdf_parse_as_number" %s) '
             'list("selected_cdf_metadata_truncated" %s || %s))) nil)') % tuple(
                 identity + [literal, _bounded_scalar('callbackExpr', MAX_CALLBACK_CHARS),
                             _scalar_truncated('callbackExpr', MAX_CALLBACK_CHARS),
                             reported_names, 't' if (len(names) > MAX_SELECTED_NAMES or
                                                    any(len(name) > MAX_NAME_CHARS for name in names)) else 'nil',
                             _bounded_scalar("get(cdfParam 'parseAsCEL)", MAX_FLAG_CHARS),
                             _bounded_scalar("get(cdfParam 'parseAsNumber)", MAX_FLAG_CHARS),
                             _scalar_truncated("get(cdfParam 'parseAsCEL)", MAX_FLAG_CHARS),
                             _scalar_truncated("get(cdfParam 'parseAsNumber)", MAX_FLAG_CHARS)])
    fallback = ('list(list("schema_version" "crescendo.cdf_callback_input.v1") '
                'list("parameter_id" "%s") list("requested_value" %s) '
                'list("diagnostic_status" "unavailable"))') % (quote(row['parameter_id']), literal)
    lines = [
        '  let((crescCallbackInput crescCallbackExtra)',
        '    ' + entry,
        '    callbackResult = errset(evalstring(callbackExpr) t)',
        '    unless(callbackResult',
        '      crescCallbackExtra = errset(',
        '        let((crescDiagRemaining crescDiagParam crescDiagName crescDiagValue crescDiagType',
        '             crescDiagCel crescDiagNumber crescDiagFields crescDiagScanned crescDiagCount',
        '             crescDiagChars crescDiagCost crescDiagSelected crescDiagNameTruncated crescDiagMetadataTruncated)',
        '          crescDiagRemaining = cdfgData~>parameters',
        '          crescDiagSelected = ' + names_literal,
        '          crescDiagScanned = 0',
        '          crescDiagCount = 0',
        '          crescDiagChars = 0',
        '          while(crescDiagRemaining && crescDiagScanned < %d && crescDiagCount < %d && crescDiagChars < %d' % (
            MAX_SCAN_FIELDS, MAX_EXPRESSION_FIELDS, MAX_EXPRESSION_CHARS),
        '            crescDiagParam = car(crescDiagRemaining)',
        '            crescDiagRemaining = cdr(crescDiagRemaining)',
        '            crescDiagScanned = crescDiagScanned + 1',
        '            crescDiagName = crescDiagParam~>name',
        '            crescDiagValue = crescDiagParam~>value',
        '            when(stringp(crescDiagName) && !member(crescDiagName crescDiagSelected) &&',
        '                 stringp(crescDiagValue) && rexMatchp("[A-Za-z_]" if(strlen(crescDiagValue) > %d then substring(crescDiagValue 1 %d) else crescDiagValue))' % (
            MAX_VALUE_CHARS, MAX_VALUE_CHARS),
        '              crescDiagNameTruncated = strlen(crescDiagName) > %d' % MAX_NAME_CHARS,
        '              crescDiagMetadataTruncated = %s || %s || %s' % (
            _scalar_truncated("get(crescDiagParam 'type)", MAX_TYPE_CHARS),
            _scalar_truncated("get(crescDiagParam 'parseAsCEL)", MAX_FLAG_CHARS),
            _scalar_truncated("get(crescDiagParam 'parseAsNumber)", MAX_FLAG_CHARS)),
        '              crescDiagName = ' + _bounded_scalar('crescDiagName', MAX_NAME_CHARS),
        '              crescDiagType = ' + _bounded_scalar("get(crescDiagParam 'type)", MAX_TYPE_CHARS),
        '              crescDiagCel = ' + _bounded_scalar("get(crescDiagParam 'parseAsCEL)", MAX_FLAG_CHARS),
        '              crescDiagNumber = ' + _bounded_scalar("get(crescDiagParam 'parseAsNumber)", MAX_FLAG_CHARS),
        '              crescDiagCost = strlen(crescDiagName) + min(strlen(crescDiagValue) %d) + %d' % (
            MAX_VALUE_CHARS, MAX_TYPE_CHARS + 2 * MAX_FLAG_CHARS),
        '              if(crescDiagChars + crescDiagCost <= %d then' % MAX_EXPRESSION_CHARS,
        '                crescDiagFields = cons(list(list("name" crescDiagName)',
        '                  list("name_truncated" crescDiagNameTruncated)',
        '                  list("value" %s)' % _bounded_scalar('crescDiagValue', MAX_VALUE_CHARS),
        '                  list("type" crescDiagType) list("parse_as_cel" crescDiagCel)',
        '                  list("parse_as_number" crescDiagNumber)',
        '                  list("metadata_truncated" crescDiagMetadataTruncated)',
        '                  list("value_truncated" strlen(crescDiagValue) > %d)) crescDiagFields)' % MAX_VALUE_CHARS,
        '                crescDiagCount = crescDiagCount + 1',
        '                crescDiagChars = crescDiagChars + crescDiagCost',
        '              else',
        '                crescDiagRemaining = cons(crescDiagParam crescDiagRemaining)',
        '                crescDiagChars = %d' % MAX_EXPRESSION_CHARS,
        '              )',
        '            )',
        '          )',
        '          list(list("capture_stage" "callback_failure")',
        '            list("expression_fields_status" "bounded_candidate_strings")',
        '            list("expression_fields" reverse(crescDiagFields))',
        '            list("expression_fields_scanned" crescDiagScanned)',
        '            list("expression_fields_truncated" if(crescDiagRemaining then t else nil))',
        '            list("remaining_cdf_fields_uninspected" if(crescDiagRemaining then t else nil))',
        '            list("expression_fields_character_budget" %d)' % MAX_EXPRESSION_CHARS,
        '            list("form_init_proc" %s)' % _bounded_scalar("get(cdfgData 'formInitProc)", MAX_FORM_PROC_CHARS),
        '            list("form_init_proc_truncated" %s)' % _scalar_truncated("get(cdfgData 'formInitProc)", MAX_FORM_PROC_CHARS),
        '            list("done_proc" %s)' % _bounded_scalar("get(cdfgData 'doneProc)", MAX_FORM_PROC_CHARS),
        '            list("done_proc_truncated" %s))' % _scalar_truncated("get(cdfgData 'doneProc)", MAX_FORM_PROC_CHARS),
        '        ) nil)',
        '      errset(printf("CRESCENDO_CDF_CALLBACK_INPUT\\t%L\\n" append(',
        '        if(crescCallbackInput then car(crescCallbackInput) else ' + fallback + ')',
        '        if(crescCallbackExtra then car(crescCallbackExtra) else list(',
        '          list("capture_stage" "callback_failure") list("expression_fields_status" "unavailable"))))) nil)',
        '      error("CRESCENDO_ADAPTER_ERROR:cdf_callback_incompatible CDF callback failed for %s: %%L\\n" callbackExpr)' % quote(row['property']),
        '    )',
        '  )',
    ]
    # Symbolic export historically strips through the caller's standalone
    # closing parenthesis. Keep every internal close on its preceding line.
    compact = []
    for line in lines:
        if line.strip() == ")":
            compact[-1] += ")"
        else:
            compact.append(line)
    return compact
