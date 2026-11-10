import os
import unittest
from unittest import mock

from crescendo_engine import gui
from crescendo_engine.gui_activity import ActivityJournal
from tests import test_gui_activity as activity_fixture


class ServiceActivityTimerTests(unittest.TestCase):
    def test_repeated_foreground_requests_keep_one_scheduled_poll(self):
        class Root(object):
            def __init__(self):
                self.timers, self.counter = {}, 0
            def after(self, delay, callback):
                self.counter += 1
                self.timers[self.counter] = (delay, callback)
                return self.counter
            def after_cancel(self, identifier):
                self.timers.pop(identifier, None)
        class Service(object):
            def activity_snapshot(self):
                return {'revision': 0, 'events': []}
        class Tasks(object):
            def submit_monitor(self, function, loaded, failed):
                loaded(function())
        app = gui.OptimizerGui.__new__(gui.OptimizerGui)
        app.root, app.service, app.tasks = Root(), Service(), Tasks()
        app.closing = False
        app.activity_journal = ActivityJournal()
        app.activity_journal.begin('task:1', 'Inspect')
        for _index in range(20):
            app._poll_service_activity()
        self.assertEqual(len(app.root.timers), 1)
        app.activity_journal.end('task:1')
        app._poll_service_activity()
        self.assertEqual(app.root.timers, {})


@unittest.skipUnless(os.environ.get('DISPLAY'), 'Existing DISPLAY required')
class ActivityLifecycleTkTests(unittest.TestCase):
    setUp = activity_fixture.ActivityGuiTests.setUp
    close = activity_fixture.ActivityGuiTests.close
    wait = activity_fixture.ActivityGuiTests.wait

    def test_exit_failure_is_logged_and_wait_bar_ends_while_retry_remains_possible(self):
        app = self.app
        self.service.exit_optimizer = mock.Mock(side_effect=RuntimeError('cleanup fixture failed'))
        with mock.patch.object(gui.messagebox, 'showerror') as shown:
            app.request_exit(confirm=False)
            self.wait(lambda: getattr(app, 'shutdown_failed', False))
            self.wait(lambda: '安全退出未完成：cleanup fixture failed' in app.activity_log.get('1.0', 'end'))
            self.assertIn('强制退出', app.activity_log.get('1.0', 'end'))
        self.assertTrue(app.closing)
        self.assertNotIn('exit', app.activity_journal.active)
        self.assertEqual(app.activity_wait_frame.winfo_manager(), '')
        self.assertFalse(shown.called)
        self.assertIn('cleanup fixture failed', app.exit_dialog_message.get())

    def test_synchronous_start_eda_and_checkpoint_errors_are_all_in_main_log(self):
        app = self.app
        with mock.patch.object(gui.messagebox, 'showerror'):
            app.start_run()
            with mock.patch.object(app, '_collect_eda_overrides', side_effect=ValueError('bad EDA override')):
                app.save_eda_setup()
            with mock.patch.object(gui.filedialog, 'askopenfilename', return_value='/fixture/checkpoint.json'), \
                    mock.patch.object(gui, '_read_json_file', side_effect=ValueError('bad checkpoint JSON')):
                app.load_checkpoint()
        self.wait(lambda: 'bad checkpoint JSON' in app.activity_log.get('1.0', 'end'))
        text = app.activity_log.get('1.0', 'end')
        for message in ('Load an ADE setup first', 'bad EDA override', 'bad checkpoint JSON'):
            self.assertIn(message, text)
        self.assertGreaterEqual(text.count('[ERROR]'), 3)

    def test_optional_range_job_keeps_progress_after_start_request_and_clears_on_terminal(self):
        app = self.app
        self.service.range_validation_status = mock.Mock(return_value={
            'state': 'running', 'phase': 'callback', 'completed': 1, 'total': 4,
            'parameter_count': 2, 'results': []})
        app.render_parameter_tree = mock.Mock()
        app.range_validation_job_id = 'range-fixture'
        app.range_validation_requested = {}
        app._activity_begin('range:range-fixture', 'Starting range check', priority=5)
        app._poll_range_validation()
        self.wait(lambda: app.activity_journal.active.get('range:range-fixture', {}).get('completed') == 1)
        self.assertEqual(app.activity_journal.active['range:range-fixture']['total'], 4)
        self.service.range_validation_status.return_value = {'state': 'completed', 'completed': 4, 'total': 4, 'results': []}
        app._poll_range_validation()
        self.wait(lambda: 'range:range-fixture' not in app.activity_journal.active)
        self.wait(lambda: '0 failed' in app.activity_log.get('1.0', 'end'))
        row = next(row for row in reversed(app.activity_journal.lines) if '0 failed' in row[1])
        self.assertEqual(row[2], 'INFO')

    def test_lost_range_job_and_setup_invalidation_clear_only_its_wait_token(self):
        app = self.app
        self.service.range_validation_status = mock.Mock(return_value=None)
        app.range_validation_job_id = 'lost'
        app._activity_begin('range:lost', 'Checking', priority=5)
        app._poll_range_validation()
        self.wait(lambda: 'range:lost' not in app.activity_journal.active)
        app.range_validation_job_id = 'old'
        app._activity_begin('range:old', 'Old range check', priority=5)
        app._activity_begin('another-task', 'Another operation', priority=30)
        app._invalidate_prepared('Saved setup changed')
        self.assertNotIn('range:old', app.activity_journal.active)
        self.assertIn('another-task', app.activity_journal.active)
