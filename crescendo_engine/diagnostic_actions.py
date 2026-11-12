"""Main-thread diagnostic actions over recursively immutable snapshots."""
import threading
from types import MappingProxyType


def snapshot(value):
    if isinstance(value, dict):
        return MappingProxyType({key: snapshot(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(snapshot(item) for item in value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise ValueError('Diagnostic payload must contain plain data')


class DiagnosticAction(object):
    def __init__(self, key, label, run, on_report_ready=None, on_progress=None, on_close=None):
        if not isinstance(key, str) or not key or not callable(run):
            raise ValueError('Diagnostic action requires an identity and callable')
        self.key, self.label, self.run = key, label, run
        self.on_report_ready = on_report_ready
        self.on_progress = on_progress
        self.on_close = on_close


class DiagnosticActions(object):
    def __init__(self, app, actions):
        self.app, self.actions = app, tuple(actions)
        if len(set(action.key for action in self.actions)) != len(self.actions):
            raise ValueError('Duplicate diagnostic action')
        self.thread = threading.current_thread()
        self.buttons, self.running, self.report_tokens = {}, set(), set()
        self.payload = snapshot({})
        self.closed = False

    def _main(self):
        if threading.current_thread() is not self.thread:
            raise RuntimeError('Diagnostic UI state belongs to the main thread')

    def bind(self, action, button):
        self._main()
        self.buttons[action.key] = button
        self._states()

    def _states(self):
        ready = self.payload.get('report_ready') is True
        for key, button in self.buttons.items():
            button.configure(state='normal' if ready and key not in self.running and not self.closed else 'disabled')

    def _event(self, action, name):
        callback = getattr(action, name)
        if callback is not None:
            try:
                callback(self.payload)
            except Exception as error:
                self.app._activity_event(str(error), 'WARN')

    def progress(self, payload):
        self._main()
        if self.closed:
            return
        self.payload = snapshot(payload)
        self._states()
        for action in self.actions:
            self._event(action, 'on_progress')
        token = (payload.get('test_id'), payload.get('main_log'))
        if payload.get('report_ready') is True and token not in self.report_tokens:
            self.report_tokens.add(token)
            for action in self.actions:
                self._event(action, 'on_report_ready')

    def invoke(self, action):
        self._main()
        if self.closed or action.key in self.running or self.payload.get('report_ready') is not True:
            return
        payload = self.payload
        self.running.add(action.key)
        self._states()
        activity_key = 'diagnostic-action:' + action.key
        self.app._activity_begin(activity_key, action.label, priority=10)
        def done(_result):
            self._main()
            self.running.discard(action.key)
            if not self.closed:
                self.app._activity_end(activity_key)
                self._states()
        def failed(error):
            done(None)
            if not self.closed:
                self.app._activity_event(str(error), 'WARN')
        try:
            self.app.tasks.submit_optional(lambda: action.run(payload), done, failed)
        except Exception:
            done(None)
            raise

    def close(self):
        self._main()
        if self.closed:
            return
        self.closed = True
        for key in self.running:
            self.app._activity_end('diagnostic-action:' + key)
        self._states()
        for action in self.actions:
            self._event(action, 'on_close')
