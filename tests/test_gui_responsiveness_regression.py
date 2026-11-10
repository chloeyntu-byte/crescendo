import os
import time
import unittest
from unittest import mock

from crescendo_engine import gui
from tests import test_gui_activity
from tests.test_gui_environment_test import EnvironmentTestInputTests, Value


class MetricDraftTests(unittest.TestCase):
    def test_invalid_corner_keeps_drafts_and_other_valid_metrics(self):
        app = EnvironmentTestInputTests().app()
        def row(kind, output, corners):
            result = mock.Mock()
            result.kind = kind
            result.output_var = Value(output)
            result.aggregation_var = Value('mean')
            result.corner_picker.selected.return_value = corners
            result.mode_var = Value('minimize' if kind == 'objective' else '<=')
            result.value_var = Value('1')
            result.definition.side_effect = (lambda label: {'output_key': output, 'corners': corners}
                if corners else (_ for _ in ()).throw(ValueError(label + ' has no corner selected.')))
            return result
        app.objective_rows = [row('objective', 'gain', []), row('objective', 'noise', ['tt'])]
        app.constraint_rows = [row('constraint', 'power', [])]
        spec = app._collect_environment_test_spec()
        self.assertFalse(spec['objectives_valid'])
        self.assertFalse(spec['constraints_valid'])
        self.assertEqual(spec['objectives'], [{'output_key': 'noise', 'corners': ['tt']}])
        self.assertEqual(spec['ui_metric_drafts']['objectives'][0]['output_key'], 'gain')
        self.assertEqual(spec['ui_metric_drafts']['constraints'][0]['corners'], [])
        self.assertEqual([(x['kind'], x['index'], x['output_key']) for x in spec['metric_definition_errors']],
                         [('objective', 1, 'gain'), ('constraint', 1, 'power')])
        self.assertIn('no corner selected', spec['metric_definition_errors'][0]['error'])

    def test_baseline_diagnostic_mode_preserved(self):
        app = EnvironmentTestInputTests().app()
        app.execution_backend_var = Value('maestro_baseline')
        self.assertEqual(app._collect_environment_test_spec()['execution_backend'], 'maestro_baseline')


@unittest.skipUnless(os.environ.get('DISPLAY'), 'DISPLAY required')
class ScrollTkTests(unittest.TestCase):
    setUp = test_gui_activity.ActivityGuiTests.setUp
    close = test_gui_activity.ActivityGuiTests.close
    wait = test_gui_activity.ActivityGuiTests.wait

    def test_scroll_skips_redundant_region_but_resize_updates_it(self):
        app = self.app
        app.show_page('Settings')
        app.settings_notebook.select(app.settings_sections['eda'])
        self.root.deiconify()
        self.root.geometry('1080x860')
        self.root.update()
        end = time.monotonic() + .3
        while time.monotonic() < end:
            self.root.update()
            time.sleep(.005)
        page = app.pages[app.current_page]
        with mock.patch.object(page.canvas, 'configure', wraps=page.canvas.configure) as configure:
            for i in range(30):
                page.scroll_units(1 if i < 15 else -1)
                self.root.update()
            region_calls = [call for call in configure.call_args_list if 'scrollregion' in call[1]]
            self.assertEqual(region_calls, [])
            initial_width = page.body.winfo_width()
            self.root.geometry('1200x860')
            self.wait(lambda: self.root.winfo_width() == 1200 and
                      page.body.winfo_width() != initial_width and
                      any('scrollregion' in call[1] for call in configure.call_args_list))
            self.assertTrue(any('scrollregion' in call[1] for call in configure.call_args_list))
        self.assertEqual(self.errors, [])

    def test_baseline_radio_and_settings_roundtrip(self):
        app = self.app
        app.simulator_var.set('alps')
        app.prepared_id = 'prepared-fixture'
        app.prepared = {'setup': {'tests': [{'name': 'Only', 'simulator': 'spectre'}]}}
        app.selected_test = 'Only'
        app.solver_command_vars['spectre'].set('unfinished CLI draft')
        app.job_command_var.set('unfinished job draft')
        app.job_command_override_var.set(True)
        app.execution_backend_var.set('maestro_baseline')
        self.assertEqual(app.simulator_var.get(), 'spectre')
        self.assertEqual(str(app.simulator_combo.cget('state')), 'disabled')
        self.assertTrue(all(str(w.cget('state')) == 'disabled' for w in app.cli_command_controls))
        self.assertTrue(all(str(w.cget('state')) == 'normal' for w in app.job_override_controls))
        with mock.patch.object(app.execution_selection_panel, 'collect', return_value=None), \
                mock.patch.object(app, '_collect_execution_commands', side_effect=AssertionError('CLI template checked')):
            settings = app._build_run_settings()
            self.assertEqual(settings['execution_backend'], 'maestro_baseline')
            self.assertNotIn('execution_commands', settings)
            self.assertEqual(settings['maestro_job_overrides']['command'], 'unfinished job draft')
        app.execution_backend_var.set('auto')
        self.assertEqual(app.simulator_var.get(), 'alps')
        self.assertEqual(app.solver_command_vars['spectre'].get(), 'unfinished CLI draft')
        self.assertEqual(app.job_command_var.get(), 'unfinished job draft')
        self.assertTrue(app.job_command_override_var.get())
        app._restore_job_settings({'execution_backend': 'maestro_baseline'})
        self.assertEqual(app.execution_backend_var.get(), 'maestro_baseline')

    def test_scrollbar_drag_and_metric_controls_remain_interactive(self):
        app = self.app
        app.show_page('Objectives & Constraints')
        self.root.deiconify()
        self.root.update()
        page = app.pages[app.current_page]
        for index in range(35):
            gui.ttk.Label(page.body, text='additional row %d' % index).pack()
        self.root.update()
        page.canvas.yview_moveto(1.0)
        self.root.update()
        self.assertGreater(page.canvas.yview()[0], 0)
        page.canvas.yview_moveto(0.0)
        self.root.update()
        self.assertAlmostEqual(page.canvas.yview()[0], 0.0)
        calls = []
        row = gui.MetricRow(page.body, 'constraint', ['power'], ['tt'],
                            lambda current: calls.append(current), None)
        row.pack()
        row.value_entry.insert(0, '12')
        self.assertEqual(row.value_var.get(), '12')
        self.assertEqual(row.definition('Constraint 1')['limit'], '12')
        buttons = [w for w in row.winfo_children() if isinstance(w, gui.ttk.Button)
                   and str(w.cget('text')) == 'Remove']
        self.assertEqual(len(buttons), 1)
        buttons[0].invoke()
        self.assertEqual(calls, [row])
        self.assertEqual(self.errors, [])

    def test_diagnostic_log_unchanged_poll_and_reader_position(self):
        app = self.app
        widget = gui.tk.Text(self.root, height=5)
        widget.pack()
        self.root.deiconify()
        self.root.update()
        app.environment_test_log_text = widget
        app.environment_test_log_engine = None
        payload = {'state': 'running', 'checks': [], 'main_log': '/tmp/example',
                   'log_tail': '\n'.join('line %d' % i for i in range(150))}
        app._render_environment_test_progress(payload)
        self.root.update()
        widget.yview_moveto(.25)
        self.root.update()
        before = widget.yview()[0]
        with mock.patch.object(widget, 'delete', wraps=widget.delete) as deleted:
            app._render_environment_test_progress(payload)
            deleted.assert_not_called()
            changed = dict(payload, log_tail=payload['log_tail'] + '\nnew line')
            app._render_environment_test_progress(changed)
            deleted.assert_called_once()
        self.root.update()
        self.assertAlmostEqual(widget.yview()[0], before, delta=.02)
        app.environment_test_log_text = None
        widget.destroy()
        self.assertEqual(self.errors, [])
