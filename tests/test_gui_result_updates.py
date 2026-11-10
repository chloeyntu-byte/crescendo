"""Large-project regressions without native EDA processes."""
import copy
import json
import os
import random
import tempfile
import time
import unittest
from unittest import mock

from crescendo_engine import gui
from crescendo_engine.gui_performance import ScopeIndex, snapshot_copy, parameter_defaults, json_detail_tail
from crescendo_engine.gui_state import RunUiState, initial_parameter_config
from crescendo_engine import settings_state, unified_configuration
from tests import test_gui_light_loading as light


@unittest.skipUnless(os.environ.get('DISPLAY'), 'Existing DISPLAY required')
class ResultUpdateTkTests(unittest.TestCase):
    setUp = light.GuiLightLoadingTests.setUp
    _close_gui = light.GuiLightLoadingTests._close_gui
    _open = light.GuiLightLoadingTests._open
    _wait = light.GuiLightLoadingTests._wait
    _pump = light.GuiLightLoadingTests._pump
    _load = light.GuiLightLoadingTests._load

    def prepared(self, count=100):
        app = self._open(parameter_count=count)
        self._load(); app.show_page('Parameters')
        self._wait(lambda: not app.parameter_tree_dirty)
        return app

    def test_results_pareto_and_trend_reuse_and_preserve_visible_values(self):
        app = self.prepared(2)
        app.live_spec = {'objectives': [{'output_key':'gain', 'direction':'maximize'}, {'output_key':'power','direction':'minimize'}]}
        def row(i, value):
            return {'candidate_id': 'c'+str(i), 'generation': i, 'status': 'completed',
                    'metrics': {'feasible': True, 'objectives': [{'value':value}, {'value':3.0}], 'constraints': []}}
        app.live_candidates = [row(0,1.0), row(1,2.0)]
        app.show_page('Live Results')
        original_rows = {i: app.result_tree.item(i) for i in app.result_tree.get_children('')}
        original_trend = app.result_trend.find_all()
        with mock.patch.object(app.result_tree, 'delete', wraps=app.result_tree.delete) as delete, \
                mock.patch.object(app.result_tree, 'insert', wraps=app.result_tree.insert) as insert, \
                mock.patch.object(app.result_tree, 'item', wraps=app.result_tree.item) as item:
            app.render_results()
            delete.assert_not_called(); insert.assert_not_called()
            self.assertEqual([c for c in item.call_args_list if c[1]], [])
            self.assertEqual({i: app.result_tree.item(i) for i in original_rows}, original_rows)
            self.assertEqual(app.result_trend.find_all(), original_trend)
            app.live_candidates[0] = row(0,4.0); app.render_results()
            delete.assert_not_called(); insert.assert_not_called()
            self.assertEqual(len([c for c in item.call_args_list if c[1]]), 1)
        app.show_page('Pareto Trade-offs')
        plots = list(app.pareto_plots); items = plots[0].canvas.find_all()
        app.render_pareto()
        self.assertEqual(app.pareto_plots, plots)
        self.assertEqual(plots[0].canvas.find_all(), items)
        app.live_candidates.append(row(2,5.0)); app.render_pareto()
        self.assertEqual(app.pareto_plots, plots)
        self.assertEqual(len(plots[0].canvas.find_withtag('point')), 3)
