import unittest
from unittest import mock
from crescendo_engine import gui
from crescendo_engine.gui_state import RunUiState


class DeferredTasks(object):
    shutting_down = False
    def __init__(self): self.calls = []
    def submit_monitor(self, function, loaded, failed): self.calls.append((function, loaded, failed))
    submit = submit_monitor


class ProgressOwnershipTests(unittest.TestCase):
    def test_success_and_failure_from_old_run_release_and_restart_polling(self):
        for failure in (False, True):
            app = gui.OptimizerGui.__new__(gui.OptimizerGui)
            app.closing = False; app.progress_inflight = False; app.setup_selection_epoch = 1
            app.run_ui = RunUiState(); app.run_ui.started('old')
            app.tasks = DeferredTasks(); app.root = mock.Mock()
            app.service = mock.Mock(); app.live_spec = {}
            app._render_progress = mock.Mock()
            app._poll_progress()
            app.run_ui.started('resumed')
            app._poll_progress()
            self.assertEqual(len(app.tasks.calls), 1)
            callback = app.tasks.calls[0][2 if failure else 1]
            callback(RuntimeError('old') if failure else {'status': {'state': 'stopped'}})
            self.assertFalse(app.progress_inflight)
            app._render_progress.assert_not_called()
            scheduled = app.root.after.call_args[0][1]
            scheduled()
            self.assertEqual(len(app.tasks.calls), 2)
            self.assertTrue(app.progress_inflight)
            callback(RuntimeError('duplicate') if failure else {})
            self.assertTrue(app.progress_inflight, 'Old replies cannot release a newer request')
