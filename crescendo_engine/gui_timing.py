"""Small timing projections for the existing results UI; no I/O or probes."""
from .diagnostic_timing import (candidate_timing_summary, timing_seconds,
                               CANDIDATE_PHASE_LABELS, GENERATION_PHASE_LABELS)


def seconds_text(value):
    value = timing_seconds(value)
    return 'unknown' if value is None else '{:.3f}s'.format(value)


def phase_text(name, phase):
    text = '{}: {}'.format(CANDIDATE_PHASE_LABELS.get(name, name), seconds_text(phase.get('elapsed_seconds')))
    scope = phase.get('scope')
    if scope:
        text += ' [{}]'.format(scope)
    if 'total_count' in phase:
        text += ' · coverage {}/{}'.format(phase.get('count', 0), phase['total_count'])
    return text


def candidate_timing_lines(row, task_details=False):
    timing = candidate_timing_summary(row.get('timing'))
    if not timing:
        return ['Timing: unavailable for this candidate.']
    lines = ['Timing · elapsed {} [{}]'.format(seconds_text(timing.get('elapsed_seconds')),
                                              timing.get('elapsed_scope', 'candidate'))]
    lines.extend(phase_text(name, phase) for name, phase in timing['phases'].items())
    if task_details:
        for task in (row.get('timing') or {}).get('tasks') or []:
            if not isinstance(task, dict):
                continue
            lines.append('  Task {} / {} / {}'.format(task.get('task_id') or task.get('point_id') or '?',
                                                     task.get('test') or '?', task.get('corner') or '?'))
            for name, phase in (task.get('phases') or {}).items():
                if isinstance(phase, dict):
                    lines.append('    ' + phase_text(name, phase))
    lines.append('Parallel sums and nested phases overlap; do not add them to wall time.')
    return lines


def generation_timing_lines(rows):
    lines = []
    for row in rows:
        lines.append('Generation {} · wall {}{} · {}'.format(row.get('generation', '?'),
            seconds_text(row.get('total_seconds')), ' (resumed segment)' if row.get('resumed_segment') else '',
            row.get('status') or 'unknown'))
        phases = row.get('phases_seconds') or {}
        lines.append('  ' + ' | '.join('{} {}'.format(label, seconds_text(phases.get(name)))
                                      for name, label in GENERATION_PHASE_LABELS.items()))
        for phase in row.get('candidate_phase_sums') or []:
            lines.append('  ' + phase_text(phase.get('phase', '?'), phase))
    return lines or ['Generation timing: unavailable.']
