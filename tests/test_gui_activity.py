import gc
import os
import threading
import time
import unittest
from unittest import mock

from crescendo_engine.gui_activity import ActivityJournal
from crescendo_engine.gui_state import BackgroundTaskQueue
from crescendo_engine import gui
from tests.test_gui_control_queue import FakeRoot
from tests.test_gui_execution_progress import FixtureProgressService


class ActivityJournalTests(unittest.TestCase):
    def test_flood_is_bounded_and_repeated_poll_is_not_recorded(self):
        journal = ActivityJournal(max_lines=20, max_characters=1500, max_message=90)
        for index in range(10000):
            journal.append('event %d ' % index + 'x' * 100, key='stage')
        self.assertLessEqual(len(journal.lines), 20)
        self.assertLessEqual(journal.characters, 1500)
        self.assertLessEqual(len(journal.signatures), 20)
        self.assertIn('event 9999', journal.lines[-1][1])
        revision = journal.revision
        self.assertIsNone(journal.append('event 9999 ' + 'x' * 100, key='stage'))
        self.assertEqual(journal.revision, revision)
        journal.append('event 9999 ' + 'x' * 100, 'ERROR', key='stage')
        self.assertEqual(journal.lines[-1][2], 'ERROR')

    def test_parallel_tasks_finish_independently_and_priority_is_stable(self):
        journal = ActivityJournal()
        journal.begin('optimization', 'Optimize', total=20, completed=2, priority=10)
        journal.begin('load', 'Load saved setup', priority=30)
        journal.begin('stop', 'Stop', priority=50)
        self.assertEqual(journal.current()['label'], 'Stop')
        journal.end('load')
        self.assertEqual(journal.current()['label'], 'Stop')
        journal.end('stop')
        journal.update('optimization', completed=5)
        self.assertEqual(journal.current()['completed'], 5)
        journal.end('unknown')
        journal.end('optimization')
        self.assertIsNone(journal.current())

    def test_queue_observer_is_delivered_only_by_main_drain(self):
        root, events, threads = FakeRoot(), [], []
        owner = threading.current_thread().ident
        def observe(event):
            events.append(event)
            threads.append(threading.current_thread().ident)
        tasks = BackgroundTaskQueue(root, observer=observe)
        result = []
        try:
            tasks.submit(lambda: 42, result.append)
            deadline = time.monotonic() + 2
            while not result and time.monotonic() < deadline:
                root.step()
                time.sleep(.005)
            self.assertEqual(result, [42])
            self.assertEqual([item['state'] for item in events], ['queued', 'running', 'completed'])
            self.assertGreaterEqual(events[1]['started_at'], events[0]['queued_at'])
            self.assertEqual(threads, [owner, owner, owner])
        finally:
            tasks.close()

    def test_observer_failure_does_not_break_execution_or_callbacks(self):
        root, result = FakeRoot(), []
        def broken(_event):
            raise RuntimeError('display unavailable')
        tasks = BackgroundTaskQueue(root, observer=broken)
        try:
            tasks.submit_control(lambda: 'stopped', result.append)
            deadline = time.monotonic() + 2
            while not result and time.monotonic() < deadline:
                root.step()
                time.sleep(.005)
            self.assertEqual(result, ['stopped'])
        finally:
            tasks.close()


@unittest.skipUnless(os.environ.get('DISPLAY'), 'Existing DISPLAY required')
class ActivityGuiTests(unittest.TestCase):
    def setUp(self):
        self.root = gui.tk.Tk()
        self.root.withdraw()
        self.service = FixtureProgressService()
        self.errors = []
        self.app = gui.OptimizerGui(self.root, self.service)
        self.root.report_callback_exception = lambda kind, value, trace: self.errors.append(value)
        self.addCleanup(self.close)
        self.wait(lambda: 'EDA' in self.app.eda_status_var.get() or not self.app.activity_journal.active)
        self.root.update()

    def close(self):
        self.app.tasks.close()
        for name in ('worker', 'control_worker', 'history_worker', 'optional_worker', 'export_worker', 'monitor_worker'):
            getattr(self.app.tasks, name).join(1)
        for variable in gc.get_objects():
            if isinstance(variable, gui.tk.Variable) and variable._tk is self.root.tk:
                for modes, callback in variable.trace_info():
                    variable.trace_remove(modes, callback)
        for identifier in self.root.tk.splitlist(self.root.tk.call('after', 'info')):
            self.root.after_cancel(identifier)
        self.root.destroy()
        self.app = None
        gc.collect()

    def wait(self, predicate, timeout=3):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self.root.update()
            self.assertEqual(self.errors, [])
            if predicate():
                return
            time.sleep(.005)
        self.fail('UI did not reach expected state')

    def test_settings_reuses_old_key_and_preserves_algorithm_and_metrics_pages(self):
        app = self.app
        self.assertEqual(app.page_buttons['EDA Settings'].cget('text'), 'Settings')
        self.assertNotIn('Settings', app.pages)
        app.show_page('Settings')
        self.assertEqual(app.current_page, 'EDA Settings')
        self.assertTrue(app._widget_is_descendant(app.algorithm_parameter_frame, app.pages['Optimization']))
        self.assertTrue(app._widget_is_descendant(app.objective_container, app.pages['Objectives & Constraints']))
        for page in app.pages.values():
            self.assertFalse(app._widget_is_descendant(app.activity_log, page))
        self.assertTrue(app._widget_is_descendant(app.job_policy_tree, app.settings_sections['maestro']))
        self.assertTrue(app._widget_is_descendant(app.execution_console, app.settings_sections['crescendo']))

    def test_wait_bar_only_visible_for_waits_and_parallel_completion_keeps_it(self):
        app = self.app
        self.wait(lambda: not app.activity_journal.active)
        self.assertEqual(app.activity_wait_frame.winfo_manager(), '')
        app._activity_begin('load', 'Inspect saved setup', priority=30)
        self.assertEqual(app.activity_wait_mode, 'determinate')
        self.assertEqual(app.progress_var.get(), 0)
        self.assertEqual(app.activity_wait_frame.winfo_manager(), 'pack')
        app._activity_begin('run', 'Optimize', total=10, completed=3, priority=10)
        app._activity_end('load')
        self.assertEqual(app.activity_wait_mode, 'determinate')
        self.assertEqual(app.progress_var.get(), 3)
        app._activity_end('run')
        self.assertEqual(app.activity_wait_frame.winfo_manager(), '')

    def test_log_remains_bounded_readonly_and_keeps_warning_and_error(self):
        app = self.app
        for index in range(10000):
            app._activity_event('stage ' + str(index), key='flood')
        app._activity_event('missing optional setting', 'WARN', key='warning')
        app._activity_event('native capture failed', 'ERROR', key='failure')
        self.wait(lambda: 'native capture failed' in app.activity_log.get('1.0', 'end'))
        text = app.activity_log.get('1.0', 'end-1c')
        self.assertIn('[WARN] missing optional setting', text)
        self.assertIn('[ERROR] native capture failed', text)
        self.assertNotIn('stage 0\n', text)
        self.assertLessEqual(len(text), app.activity_journal.max_characters)
        self.assertEqual(str(app.activity_log.cget('state')), 'disabled')
        before = app.activity_journal.revision
        app._activity_event('native capture failed', 'ERROR', key='failure')
        self.assertEqual(app.activity_journal.revision, before)

    def test_diagnostic_stage_and_error_reach_main_log_without_rebuilding_unchanged_checks(self):
        app = self.app
        payload = {'test_id': 'fixture', 'state': 'running', 'stage': 'private_copy',
                   'message': 'Copying owned Maestro', 'checks_revision': 1,
                   'checks': [{'id': 'fixture.setting', 'check_id': 'setting', 'status': 'WARN',
                               'message': 'Current Maestro setting retained'}]}
        app._render_environment_test_progress(payload)
        with mock.patch.object(app.environment_test_tree, 'delete', wraps=app.environment_test_tree.delete) as deleted:
            app._render_environment_test_progress(dict(payload, stage='native_run'))
            deleted.assert_not_called()
        self.wait(lambda: 'native_run' in app.activity_log.get('1.0', 'end'))
        text = app.activity_log.get('1.0', 'end')
        self.assertIn('Current Maestro setting retained', text)
        app._render_environment_test_progress(dict(payload, state='completed'))
        self.wait(lambda: 'diagnostics' not in app.activity_journal.active)

    def test_real_gates_are_green_and_never_advance_with_time_or_duplicate_poll(self):
        app = self.app
        self.wait(lambda: not app.activity_journal.active)
        gates = {'scope': 'candidate_4', 'passed': 1, 'total': 5, 'current': 'candidate_4: netlist generation'}
        app._activity_begin('optimization', 'Optimize', gates=gates, priority=100)
        self.assertEqual(app.progress_var.get(), 1)
        self.assertEqual(app.activity_wait_mode, 'determinate')
        self.assertEqual(gui.ttk.Style(self.root).lookup('Crescendo.Horizontal.TProgressbar', 'background'), '#39e64d')
        deadline = time.monotonic() + .25
        while time.monotonic() < deadline:
            self.root.update()
            time.sleep(.005)
        self.assertEqual(app.progress_var.get(), 1)
        self.assertIsNone(getattr(app, 'activity_pulse_job', None))
        app._activity_update('optimization', gates=dict(gates, passed=3))
        app._activity_update('optimization', gates=dict(gates, passed=2))
        self.assertEqual(app.progress_var.get(), 3)
        self.assertIn('3/5', app.activity_wait_var.get())
        app._activity_finish('optimization')
        self.assertEqual(app.progress_var.get(), 3)
        self.wait(lambda: 'optimization' not in app.activity_journal.active)

    def test_diagnostic_failed_terminal_keeps_last_proven_gate_without_full_bar(self):
        app = self.app
        self.wait(lambda: not app.activity_journal.active)
        payload = {'test_id': 'failure', 'state': 'running', 'checks': [],
                   'gate_progress': {'scope': 'failure', 'passed': 2, 'total': 7, 'current': 'Native run'}}
        app._render_environment_test_progress(payload)
        self.assertEqual(app.progress_var.get(), 2)
        app._render_environment_test_progress(dict(payload, state='failed', message='corner alignment failed'))
        self.assertEqual(app.progress_var.get(), 2)
        self.assertEqual(float(app.progress_bar.cget('maximum')), 7)
        self.wait(lambda: 'diagnostics' not in app.activity_journal.active)

    def test_consecutive_loads_ignore_previous_terminal_events_and_queued_operations(self):
        app = self.app
        self.wait(lambda: not app.activity_journal.active)
        app.service_activity_revision = 0
        event = {'id': 9001, 'name': 'OptimizerGui.load_setup.<locals>.load', 'state': 'queued'}
        with mock.patch.object(app, '_poll_service_activity'):
            app._activity_task_event(event)
        task_key = 'task:9001'
        rows = [{'revision': 1, 'stage': 'inspection_complete', 'message': 'Previous load finished', 'time': 99},
                {'revision': 2, 'stage': 'saved_setup', 'message': 'Reading current setup', 'time': 101},
                {'revision': 3, 'stage': 'inspection_complete', 'message': 'Current Test catalogue ready', 'time': 102}]
        snapshot = lambda _service: {'events': rows}
        def immediate(function, loaded, failed):
            loaded(function())
        with mock.patch.object(type(app.service), 'activity_snapshot', snapshot, create=True), \
                mock.patch.object(app.tasks, 'submit_monitor', side_effect=immediate):
            # A waiting task cannot borrow gates from the currently running one.
            app._poll_service_activity()
            self.assertNotIn('gates', app.activity_journal.active[task_key])
            app.service_activity_revision = 0
            with mock.patch.object(app, '_poll_service_activity'):
                app._activity_task_event(dict(event, state='running', started_at=100))
            app._poll_service_activity()
            self.assertEqual(app.activity_journal.active[task_key]['gates']['passed'], 1)
            app._activity_end(task_key)
            event['id'] = 9002
            with mock.patch.object(app, '_poll_service_activity'):
                app._activity_task_event(event)
                app._activity_task_event(dict(event, state='running', started_at=200))
            rows[:] = [{'revision': 4, 'stage': 'inspection_complete', 'message': 'Old late terminal', 'time': 199},
                       {'revision': 5, 'stage': 'saved_setup', 'message': 'New load reading', 'time': 201}]
            app._poll_service_activity()
            self.assertEqual(app.activity_journal.active['task:9002']['gates']['passed'], 0)
            self.assertEqual(app.activity_journal.active['task:9002']['label'], 'New load reading')

    def test_candidate_failure_counts_cannot_fill_a_gate_bar_without_producer_evidence(self):
        app = self.app
        self.wait(lambda: not app.activity_journal.active)
        app._render_progress({'state': 'running', 'total_candidates': 2,
                              'failed_candidates': 1, 'infrastructure_failed_candidates': 1})
        row = app.activity_journal.active['optimization']
        self.assertIsNone(row['total'])
        self.assertEqual(row['completed'], 0)
        self.assertEqual(app.progress_var.get(), 0)
        app._render_progress({'state': 'failed', 'total_candidates': 2, 'failed_candidates': 2})
        self.assertEqual(app.progress_var.get(), 0)

    def test_new_run_clears_old_gate_and_cancels_prior_finish_timer(self):
        app = self.app
        self.wait(lambda: not app.activity_journal.active)
        app.run_ui.run_id = 'previous'
        app._render_progress({'state': 'running'})
        app._activity_update('optimization', gates={
            'scope': 'previous:c1', 'passed': 5, 'total': 5, 'current': 'Saved'})
        app._activity_finish('optimization')
        app.run_ui.run_id = 'current'
        app._render_progress({'state': 'running'})
        self.assertEqual(app.progress_var.get(), 0)
        self.assertNotIn('optimization', app.activity_finish_jobs)
        self.assertNotIn('gates', app.activity_journal.active['optimization'])
