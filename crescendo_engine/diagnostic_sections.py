"""Read-only report contributions from explicitly registered trusted applications."""
import collections
import threading

from .diagnostic_actions import snapshot

SECTION_LIMIT = 8192
TOTAL_LIMIT = 32768
MAX_SECTIONS = 32
TITLE_LIMIT = 160
STATUSES = ('PASS', 'WARN', 'FAIL', 'NOT_TESTED')
_lock = threading.RLock()
_sections = collections.OrderedDict()


def register_diagnostic_section(name, contributor):
    """Register once, in report order. Callbacks receive immutable plain data."""
    if not isinstance(name, str) or not name or len(name) > 128 or not callable(contributor):
        raise ValueError('Diagnostic section requires a short name and callable')
    with _lock:
        if name in _sections or len(_sections) >= MAX_SECTIONS:
            raise ValueError('Duplicate diagnostic section or registration limit exceeded')
        _sections[name] = contributor


def registered_sections():
    with _lock:
        return tuple(_sections.items())


def _clip(value, limit):
    value = str(value)
    marker = '\n[truncated]'
    return value if len(value) <= limit else value[:max(0, limit - len(marker))] + marker[:limit]


def collect_sections(context, registrations=None, redactor=None):
    """Bounds apply to title + body characters, including truncation markers.

    Trusted callbacks must be bounded, read-only and must not mutate external
    runtime state. No service/process handles or gate setters are supplied.
    """
    context_error = None
    try:
        frozen = snapshot(context)
    except Exception as error:
        frozen, context_error = None, str(error)
    result, remaining = [], TOTAL_LIMIT
    registrations = tuple(registered_sections() if registrations is None else registrations)
    for index, (name, callback) in enumerate(registrations):
        if remaining <= 0:
            break
        try:
            if context_error is not None:
                raise ValueError('Invalid read-only snapshot: ' + context_error)
            value = callback(frozen)
            if not isinstance(value, dict) or value.get('status') not in STATUSES:
                raise ValueError('Expected title, status and body report fields')
            title, body, status = str(value.get('title', name)), str(value.get('body', '')), value['status']
        except Exception as error:
            title, body, status = name, 'Contributor error: ' + str(error), 'WARN'
        if redactor is not None:
            try:
                safe = redactor({'name': name, 'title': title, 'body': body, 'status': status})
                name, title, body, status = (str(safe['name']), str(safe['title']),
                                             str(safe['body']), safe['status'])
                if status not in STATUSES:
                    raise ValueError('Invalid redacted status')
            except Exception:
                name, title, body, status = 'contributor', 'Contributor warning', 'Contribution redaction failed', 'WARN'
        name = _clip(name, 128)
        budget = min(SECTION_LIMIT, remaining)
        original_title, original_body = title, body
        title = _clip(title, min(TITLE_LIMIT, budget))
        body = _clip(body, budget - len(title))
        omitted = 0
        if len(title) + len(body) == remaining and index < len(registrations) - 1:
            omitted = len(registrations) - index - 1
            marker = '\n[truncated: total section limit; later sections omitted]'
            body = _clip(body, max(0, budget - len(title) - len(marker))) + marker
            body = _clip(body, budget - len(title))
        remaining -= len(title) + len(body)
        result.append({'name': name, 'title': title, 'status': status, 'body': body,
                       'truncated': bool(title != original_title or body != original_body or omitted),
                       'omitted_sections': omitted})
    return result
