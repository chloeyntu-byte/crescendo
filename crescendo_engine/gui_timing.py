"""Small timing projections for the existing results UI; no I/O or probes."""
from .diagnostic_timing import (candidate_timing_summary, timing_seconds,
                               CANDIDATE_PHASE_LABELS, GENERATION_PHASE_LABELS, STORAGE_OPERATION_LABELS)


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
        operations = row.get('storage_operations') or {}
        if operations:
            lines.append('  Storage operations (nested within wall time)')
            for name, label in STORAGE_OPERATION_LABELS.items():
                if name not in operations:
                    continue
                item = operations[name]
                volume = ' · {:.2f} MiB'.format(item['bytes'] / (1024 * 1024)) if item['bytes'] else ''
                lines.append('    {} · {} · {} calls · max {}{}'.format(
                    label, seconds_text(item['seconds']), item['calls'], seconds_text(item['max_seconds']), volume))
        for phase in row.get('candidate_phase_sums') or []:
            lines.append('  ' + phase_text(phase.get('phase', '?'), phase))
    return lines or ['Generation timing: unavailable.']


class GuiHeartbeat(object):
    """Tk ticks on the main thread; workers read small synchronized windows.

    Pauses are actual gaps between Tk ticks; excess over the 50 ms interval
    is also retained. Load/Apply windows
    include their completion callbacks. No unbounded sample history is retained.
    """
    def __init__(self, root, clock=None):
        import threading
        import time
        self.clock = clock or time.monotonic
        self.lock = threading.RLock()
        self.interval = 0.05
        self.last = self.clock()
        self.active, self.latest = {}, {}
        self.root = root
        self.closed = False
        self.job = root.after(50, self.tick)
        if callable(getattr(root, 'bind', None)):
            root.bind('<Destroy>', self._destroyed, add='+')

    def begin(self, name):
        with self.lock:
            if self.closed:
                return
            self.active[name] = {'started': self.clock(), 'samples': 0,
                                 'max_pause_seconds': 0.0, 'max_excess_seconds': 0.0,
                                 'over_200ms_count': 0}

    def _sample(self, row, now):
        pause = max(0.0, now - max(self.last, row['started']))
        row['max_excess_seconds'] = max(row['max_excess_seconds'], max(0.0, pause - self.interval))
        row['samples'] += 1
        row['max_pause_seconds'] = max(row['max_pause_seconds'], pause)
        row['over_200ms_count'] += int(pause > .2)

    def tick(self):
        with self.lock:
            if self.closed:
                return
            now = self.clock()
            for row in self.active.values():
                self._sample(row, now)
            self.last = now
        try:
            self.job = self.root.after(50, self.tick)
        except Exception:
            self.job = None

    def end(self, name):
        with self.lock:
            row = self.active.pop(name, None)
            if row is not None:
                now = self.clock()
                # Include a pending delayed tick at the end of an operation.
                self._sample(row, now)
                row['elapsed_seconds'] = max(0.0, now - row.pop('started'))
                self.latest[name] = dict(row)
            return dict(self.latest.get(name) or {})

    def diagnostic_snapshot(self, identifier):
        with self.lock:
            diagnostic = self.end(identifier)
            # Do not retain previous diagnostic windows as history.
            self.latest.pop(identifier, None)
            return {'available': not self.closed, 'interval_seconds': self.interval,
                    'windows': dict({'diagnostic': diagnostic}, **{
                        name: dict(self.latest[name]) for name in ('load', 'apply') if name in self.latest})}

    def _destroyed(self, event):
        if event.widget is self.root:
            self.close()

    def close(self):
        """Called by Tk on root destruction; cancel only this observer's timer."""
        with self.lock:
            self.closed = True
            self.active.clear()
            job, self.job = self.job, None
        if job is not None:
            try:
                self.root.after_cancel(job)
            except Exception:
                pass


def observe_setup_action(name):
    """Include synchronous GUI work before submission in Load/Apply windows."""
    from functools import wraps
    def decorate(method):
        @wraps(method)
        def observed(app, *args, **kwargs):
            observer = getattr(app, 'diagnostic_heartbeat', None)
            if observer is None or observer.closed:
                return method(app, *args, **kwargs)
            observer.begin(name)
            try:
                return method(app, *args, **kwargs)
            finally:
                with observer.lock:
                    if not observer.active.get(name, {}).get('submitted'):
                        observer.end(name)
        return observed
    return decorate
