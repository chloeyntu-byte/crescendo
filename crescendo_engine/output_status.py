"""Data-only scalar readback states shared by Native and CLI readers."""
import math


def scalar_status(value=None, error=None, collected=True):
    if not collected:
        return 'not_collected'
    if error:
        return 'evaluation_failed'
    if value is None:
        return 'not_collected'
    if isinstance(value, bytes) or str(value).lower() in ('wave', 'waveform'):
        return 'waveform'
    try:
        number = float(value) if not isinstance(value, bool) else None
    except (TypeError, ValueError, OverflowError):
        return 'evaluation_failed'
    if number is None:
        return 'evaluation_failed'
    return 'available' if math.isfinite(number) else 'non_finite'


def point_status(point):
    # Older diagnostic points normalized every failure to None. That evidence
    # cannot distinguish missing collection from NaN or Calculator failure.
    if not point.get('output_status') and point.get('numeric', point.get('value')) is None and not point.get('error'):
        return 'unknown'
    return point.get('output_status') or scalar_status(
        point.get('numeric', point.get('value')), point.get('error'))


def output_statuses(parsed, outputs=()):
    """Keep exact output/point identities, including intentionally unread outputs."""
    catalogue = {row.get('key') or row.get('output_key'): row for row in outputs}
    catalogue.update(parsed.get('output_map') or {})
    rows = []
    for key, output in sorted(catalogue.items()):
        if not key:
            continue
        points = output.get('values') or []
        for point in points or [None]:
            status = point_status(point) if point is not None else 'not_collected'
            rows.append({'output': key, 'test': output.get('test') or key.rsplit('::', 1)[0],
                         'corner': point.get('corner') if point else None,
                         'point_id': point.get('point_id') if point else None,
                         'status': status,
                         'first_error': (point.get('first_error') or point.get('error') or
                                         ('Non-numeric scalar result' if status == 'evaluation_failed' else None)) if point else None})
    return rows
