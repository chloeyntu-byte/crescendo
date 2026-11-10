"""Describe saved current script hooks without executing or logging their code.

Empty and explicitly disabled hooks do not impose an execution dependency.
Every other nonempty payload remains unqualified by the shared CLI pipeline.
"""
import hashlib
import re


_HOOK = re.compile(r'^(?:pre|post|user|eval).*script', re.I)
_FALSE = frozenset(('0', 'false', 'nil', '(nil)', 'no', 'off'))
_TRUE = frozenset(('1', 'true', 't', '(t)', 'yes', 'on'))
_EMPTY = frozenset(('', 'nil', '(nil)', '()', '""', "''"))
_HISTORY = frozenset(('history', 'historyentry', 'checkpoint', 'snapshots', 'snapshot'))
_METADATA = frozenset(('enabled', 'enable', 'disabled', 'name', 'language', 'type',
                      'count', 'num', 'number', 'numscript', 'numscripts', 'runorder'))
_PAYLOAD_ATTRIBUTES = frozenset(('file', 'filename', 'path', 'script', 'scriptfile',
                                'command', 'code', 'value', 'expression'))
_METADATA_ATTRIBUTES = frozenset(('enabled', 'enable', 'disabled', 'name', 'language',
                                 'type', 'id', 'version'))
MAX_HOOKS = 64
MAX_PAYLOAD_FIELDS = 32


def _tag(node):
    return str(node.tag).rsplit('}', 1)[-1].lower().replace('_', '').replace('-', '')


def is_native_disabled_prerun_switch(node):
    """Recognize only the native scalar switch, never a script/file payload.

    ADE stores prerunscript=0 separately from prerunscriptfile. Attributes,
    children, quoted strings and other hooks retain normal qualification.
    """
    return (_tag(node) == 'prerunscript' and not node.attrib and not list(node)
            and (node.text or '').strip() == '0')


def _control_tag(tag):
    return tag in _METADATA or bool(_HOOK.match(tag) and re.search(
        r'(?:enabled|enable|disabled|count|number|num|language|type)$', tag))


def _enable_state(node):
    """Contradictory or unknown flags cannot prove that a hook is disabled."""
    flags = [(str(key).lower().replace('_', '').replace('-', ''), str(value).strip().lower())
             for key, value in node.attrib.items()]
    for child in list(node):
        if _tag(child) in ('enabled', 'enable', 'disabled') and not list(child):
            flags.append((_tag(child), (child.text or '').strip().lower()))
    states = []
    for key, value in flags:
        if key not in ('enabled', 'enable', 'disabled'):
            continue
        if value not in _FALSE | _TRUE:
            return 'unknown_or_conflicting'
        enabled = value in _TRUE
        states.append(not enabled if key == 'disabled' else enabled)
    if not states:
        return None
    return states[0] if len(set(states)) == 1 else 'unknown_or_conflicting'


def _disabled(node):
    return _enable_state(node) is False


def _flag_metadata(node):
    flags = []
    for key, value in node.attrib.items():
        if str(key).lower().replace('_', '').replace('-', '') in ('enabled', 'enable', 'disabled'):
            normalized = str(value).strip().lower()
            flags.append({'source': 'attribute', 'name': key,
                          'value': normalized if normalized in _FALSE | _TRUE else '<unrecognized>'})
    for child in list(node):
        if _tag(child) in ('enabled', 'enable', 'disabled') and not list(child):
            normalized = (child.text or '').strip().lower()
            flags.append({'source': 'child', 'name': _tag(child),
                          'value': normalized if normalized in _FALSE | _TRUE else '<unrecognized>'})
    return flags


def _payload_summary(value, location, kind):
    raw = value.strip()
    if raw in _EMPTY:
        return None
    data = raw.encode('utf-8')
    row = {'xml_path': location, 'kind': kind, 'bytes': len(data),
           'sha256': hashlib.sha256(data).hexdigest(), 'text_included': False}
    # A literal file name is useful; arbitrary commands and script bodies are
    # represented by length/digest only, including potential credential text.
    if kind in ('file', 'filename', 'path', 'scriptfile'):
        candidate = raw[1:-1] if len(raw) >= 2 and raw[0] == raw[-1] == '"' else raw
        if (len(candidate) <= 512 and re.search(r'\.(?:il|ils|ocn|py|sh|csh|tcsh|tcl|skill)$', candidate, re.I)
                and not any(character in candidate for character in '\n\r\t;|&`"\'()=')):
            row['file_reference'] = candidate
    return row


def inspect_setup_script_hooks(root):
    """Return bounded current hook observations, excluding historical copies.

    This is a structural empty/disabled check, not a language interpreter or
    proof that an arbitrary user hook can be reproduced by the CLI pipeline.
    """
    active = next((child for child in list(root) if _tag(child) == 'active'), root)
    checks, total = [], [0]

    def payloads(node, path, inherited_disabled=False):
        disabled = inherited_disabled or _disabled(node)
        if disabled:
            return [], int(disabled)
        values, disabled_children = [], 0
        for key, value in node.attrib.items():
            kind = str(key).lower().replace('_', '').replace('-', '')
            if kind in _PAYLOAD_ATTRIBUTES or kind not in _METADATA_ATTRIBUTES:
                row = _payload_summary(str(value), path + '/@' + str(key), kind)
                if row:
                    values.append(row)
        if not _control_tag(_tag(node)) or list(node):
            row = _payload_summary(node.text or '', path, _tag(node))
            if row:
                values.append(row)
        counts = {}
        for child in list(node):
            tag = _tag(child)
            counts[tag] = counts.get(tag, 0) + 1
            nested, omitted = payloads(child, '%s/%s[%d]' % (path, tag, counts[tag]), disabled)
            values.extend(nested)
            disabled_children += omitted
            tail = _payload_summary(child.tail or '', '%s/%s[%d]/tail' % (path, tag, counts[tag]), 'mixed_xml_text')
            if tail:
                values.append(tail)
        return values, disabled_children

    def visit(node, path, inherited_disabled=False):
        tag = _tag(node)
        if tag in _HISTORY:
            return
        disabled = inherited_disabled or _disabled(node)
        if _HOOK.match(tag) and not _control_tag(tag):
            total[0] += 1
            if len(checks) < MAX_HOOKS:
                native_disabled = is_native_disabled_prerun_switch(node)
                values, disabled_children = ([], 0) if native_disabled else payloads(node, path, inherited_disabled)
                status = 'native_disabled_switch' if native_disabled else 'disabled' if disabled else 'unqualified' if values else 'empty'
                checks.append({'xml_path': path, 'tag': tag, 'enabled_state':
                    'native_switch_off' if native_disabled else 'explicitly_disabled' if disabled else 'unknown_or_conflicting' if
                    _enable_state(node) == 'unknown_or_conflicting' else 'enabled_or_unspecified',
                    'attribute_names': sorted(str(key) for key in node.attrib)[:16],
                    'attribute_count': len(node.attrib),
                    'enabled_flags': _flag_metadata(node), 'status': status,
                    'blocking': bool(values) and not disabled,
                    'child_tags': [_tag(child) for child in list(node)[:32]],
                    'disabled_descendants': disabled_children,
                    'payload_metadata': values[:MAX_PAYLOAD_FIELDS],
                    'payload_field_count': len(values),
                    'payload_fields_omitted': max(0, len(values) - MAX_PAYLOAD_FIELDS)})
            return
        counts = {}
        for child in list(node):
            child_tag = _tag(child)
            counts[child_tag] = counts.get(child_tag, 0) + 1
            visit(child, '%s/%s[%d]' % (path, child_tag, counts[child_tag]), disabled)

    visit(active, '/' + _tag(active))
    return {'schema_version': 'crescendo.saved_script_hooks.v1', 'scope': 'current_saved_active_setup',
            'checks': checks, 'hook_count': total[0], 'omitted_hooks': max(0, total[0] - len(checks)),
            'blocked': any(row['blocking'] for row in checks) or total[0] > MAX_HOOKS,
            'script_text_included': False, 'execution_authority': False}
