"""Token boundaries for one native Spectre ``parameters`` declaration.

A bare name declares an unassigned parameter.  It is not part of the preceding
value and it has no invented numeric value.  Expressions and source text remain
unchanged; qualification/evaluation belongs to the existing native reader.
"""
import re

_IDENTIFIER = re.compile(r'[A-Za-z_][A-Za-z0-9_]*')
_NUMBER = re.compile(r'(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?[A-Za-z]*')
_OPERATORS = ('**', '==', '!=', '<=', '>=', '&&', '||', '<<', '>>')
_BINARY = frozenset(('+', '-', '*', '/', '^', '%', '**', '==', '!=', '<=', '>=',
                     '<', '>', '&&', '||', '&', '|', '<<', '>>', '?', ':'))
_UNARY = frozenset(('+', '-', '!', '~'))


def _quoted_end(text, start):
    quote, escaped = text[start], False
    index = start + 1
    while index < len(text):
        char = text[index]
        if escaped:
            escaped = False
        elif char == '\\':
            escaped = True
        elif char == quote:
            return index + 1
        index += 1
    return len(text)


def _group_end(text, start):
    closing = {'(': ')', '[': ']', '{': '}'}
    stack, index = [closing[text[start]]], start + 1
    while index < len(text):
        char = text[index]
        if char in ('"', "'"):
            index = _quoted_end(text, index)
            continue
        if char in closing:
            stack.append(closing[char])
        elif char == stack[-1]:
            stack.pop()
            if not stack:
                return index + 1
        index += 1
    return len(text)


def _tokens(text, start):
    result, index = [], start
    while index < len(text):
        char = text[index]
        if char.isspace():
            index += 1
            continue
        if char == "\\":
            continuation = re.match(r"\\[ \t]*\r?\n[ \t]*(?:\+[ \t]*)?", text[index:])
            if continuation:
                index += continuation.end()
                continue
        line_start = text.rfind("\n", 0, index) + 1
        if char == "+" and line_start > 0 and not text[line_start:index].strip():
            index += 1
            continue
        if text.startswith('//', index):
            break
        begin = index
        if char in ('"', "'"):
            index = _quoted_end(text, index)
            kind = 'quoted'
        elif char in '([{':
            index = _group_end(text, index)
            kind = 'group'
        else:
            identifier = _IDENTIFIER.match(text, index)
            number = _NUMBER.match(text, index)
            if identifier:
                index = identifier.end()
                kind = 'identifier'
            elif number:
                index = number.end()
                kind = 'number'
            else:
                operator = next((value for value in _OPERATORS if text.startswith(value, index)), char)
                index += len(operator)
                kind = 'operator'
        result.append((kind, text[begin:index], begin, index))
    return result


def parameter_declaration_spans(statement):
    """Return ``(name, expression, start, end)`` for exact declaration spans.

    Spans end at the expression's last token, never at the following bare name.
    They include the complete bare name for declarations without an assignment.

    Only top-level whitespace between complete expressions and the next bare
    identifier is a declaration boundary. Parentheses/braces/vectors and quoted
    strings are opaque balanced atoms, so their internal names never become
    declarations. This function does not evaluate expressions or rewrite SCS.
    """
    if not isinstance(statement, str):
        return []
    keyword = re.match(r'^\s*parameters\b', statement)
    if keyword is None:
        return []
    tokens = _tokens(statement, keyword.end())
    result, index = [], 0
    while index < len(tokens):
        token = tokens[index]
        if token[0] != 'identifier':
            index += 1
            continue
        name, name_start = token[1], token[2]
        index += 1
        if index >= len(tokens) or tokens[index][1] != '=':
            result.append((name, '', name_start, token[3]))
            continue
        index += 1
        begin = tokens[index][2] if index < len(tokens) else len(statement)
        end, expects_operand, previous = begin, True, None
        while index < len(tokens):
            token = tokens[index]
            kind, value, start, stop = token
            if kind == 'identifier' and index + 1 < len(tokens) and tokens[index + 1][1] == '=':
                break
            if kind in ('identifier', 'number', 'quoted', 'group'):
                # A function argument group may follow its function name. All
                # other adjacent complete atoms start a new bare declaration.
                function_call = kind == 'group' and value.startswith('(') and previous == 'identifier'
                if not expects_operand and not function_call:
                    break
                expects_operand = False
                previous = kind
            elif value in _UNARY and expects_operand:
                previous = 'operator'
            elif value in _BINARY:
                expects_operand = True
                previous = 'operator'
            else:
                break
            end = stop
            index += 1
        result.append((name, statement[begin:end].strip(), name_start, end))
    return result


def parse_parameter_declarations(statement):
    """Read header values without rewriting strings or inventing defaults."""
    return [(name, expression) for name, expression, _start, _end
            in parameter_declaration_spans(statement)]


def parameter_statement_spans(text):
    """Locate complete physical header blocks without discarding comments.

    The returned ``(start, end, statement)`` offsets address the original SCS.
    Only a parameters line and its explicit backslash/leading-plus continuation
    lines are included; following device statements never join the header.
    """
    if not isinstance(text, str):
        return []
    lines = text.splitlines(True)
    starts, offset = [], 0
    for line in lines:
        starts.append(offset)
        offset += len(line)
    result, index = [], 0
    while index < len(lines):
        if not re.match(r"^\s*parameters\b", lines[index]):
            index += 1
            continue
        begin = index
        index += 1
        while index < len(lines):
            continued = lines[index - 1].rstrip().endswith("\\")
            plus_line = lines[index].lstrip().startswith("+")
            if not continued and not plus_line:
                break
            index += 1
        start = starts[begin]
        end = starts[index] if index < len(starts) else len(text)
        # Trailing whitespace is outside declaration spans and stays byte-for-
        # byte intact when consumers replace only a selected declaration.
        result.append((start, end, text[start:end]))
    return result
