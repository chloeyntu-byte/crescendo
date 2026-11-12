"""Timing views keep native unknowns and diagnostic/formal identities separate."""
import os
import unittest
from crescendo_engine.gui_timing import candidate_timing_lines, generation_timing_lines
from tests import test_gui_light_loading as light


class TimingTextTests(unittest.TestCase):
    def test_unknown_solver_is_not_zero_and_parallel_scope_is_explicit(self):
        text = '\n'.join(candidate_timing_lines({'timing': {'elapsed_seconds': 10,
            'phases': {'solver': {'elapsed_seconds': None, 'count': 0, 'total_count': 2, 'scope': 'task_sum'},
                       'collection': {'elapsed_seconds': 1.25}}}}))
        self.assertIn('Solver: unknown [task_sum]', text)
        self.assertIn('coverage 0/2', text)
        self.assertIn('Collection: 1.250s', text)
        self.assertIn('do not add', text)

    def test_generation_has_settlement_and_persistence_separately(self):
        text = '\n'.join(generation_timing_lines([{'generation': 2, 'total_seconds': 10,
            'phases_seconds': {'settlement': 2, 'persist': 3}, 'resumed_segment': True}]))
        self.assertIn('Settlement 2.000s', text)
        self.assertIn('Checkpoint persistence 3.000s', text)
        self.assertIn('resumed segment', text)


@unittest.skipUnless(os.environ.get('DISPLAY'), 'DISPLAY required')
class TimingViewTests(unittest.TestCase):
    setUp = light.GuiLightLoadingTests.setUp
    _close_gui = light.GuiLightLoadingTests._close_gui
    _open = light.GuiLightLoadingTests._open
    _wait = light.GuiLightLoadingTests._wait
    _pump = light.GuiLightLoadingTests._pump

    def test_actual_timing_button_and_selected_candidate_display(self):
        app = self._open(parameter_count=2)
        app.root.geometry('1080x760')
        app.run_ui.started('formal')
        app._live_timing_snapshot = ('formal', [{'generation': 9, 'total_seconds': 999}])
        app.display_dataset = {'metadata': {'display_name': 'Diagnostic Private Netlist'},
            'generation_timings': [{'generation': 2, 'status': 'completed', 'total_seconds': 8,
                                   'phases_seconds': {'settlement': 0.4, 'persist': 0.1}}],
            'candidates': [{'candidate_id': 'g001_c0000', 'generation': 1, 'status': 'completed',
                'timing': {'elapsed_seconds': 6, 'phases': {'solver': {
                    'elapsed_seconds': 4, 'scope': 'task_sum', 'count': 2, 'total_count': 2}}}}]}
        app.selected_candidate_id = 'g001_c0000'
        app.show_page('Live Results')
        self._pump(.05)
        app.timing_details_button.invoke()
        self._pump(.05)
        text = app.timing_details_text.get('1.0', 'end')
        self.assertIn('Generation 2', text)
        self.assertNotIn('Generation 9', text)
        self.assertIn('Solver: 4.000s [task_sum]', text)
        self.assertIn('coverage 2/2', text)
        self.assertIn('Solver: 4.000s', app.candidate_detail.get('1.0','end'))
        self.assertEqual(app.run_ui.run_id, 'formal')
        self.assertTrue(app.timing_details_window.winfo_viewable())
        app.timing_details_window.destroy()
        app.display_dataset = None
        app.run_ui.started('other')
        app.show_timing_details()
        self.assertNotIn('Generation 9', app.timing_details_text.get('1.0', 'end'))
        app.timing_details_window.destroy()
