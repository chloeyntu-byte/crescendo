# -*- coding: utf-8 -*-
import copy
import os
import unittest
from unittest import mock

from tests.test_gui_environment_test import EnvironmentTestTkTests


@unittest.skipUnless(os.environ.get('DISPLAY'), 'Real Tk checks require an existing DISPLAY')
class LadderTkTests(EnvironmentTestTkTests):
    def test_three_stages_and_baseline_keeps_cli_draft(self):
        app = self.app
        app.simulator_var.set('alps')
        app.prepared_id = 'prepared-fixture'
        app.prepared = {'setup': {'tests': [{'name': 'Only', 'simulator': 'spectre'}]}}
        app.selected_test = 'Only'
        app.solver_command_vars['alps'].set('custom {netlist}')
        app.execution_backend_var.set('maestro_baseline')
        with mock.patch.object(app, '_collect_execution_commands', side_effect=ValueError('bad CLI')), \
             mock.patch.object(app, '_collect_job_settings', return_value={'maestro_job_overrides': {'max_jobs': 3}}):
            spec = app._collect_environment_test_spec()
        self.assertEqual(spec['simulator'], 'alps')
        self.assertFalse(spec['command_settings_valid'])
        self.assertEqual(spec['execution_commands']['solver_templates']['alps'], 'custom {netlist}')
        self.assertEqual(spec['maestro_job_overrides']['max_jobs'], 3)
        self.assertEqual(app.simulator_var.get(), 'spectre')
        payload = copy.deepcopy(self.service.payload)
        payload['case_results'] = [
            {'engine': 'maestro_baseline', 'simulator': 'spectre', 'state': 'failed', 'first_cause': {'message': 'native failure'}},
            {'engine': 'private_netlist', 'simulator': 'alps', 'state': 'running'},
            {'engine': 'maestro_netlist', 'simulator': 'alps', 'state': 'skipped', 'skip_reason': 'stop unknown'},
        ]
        app._render_environment_test_progress(payload)
        text = app.environment_test_ladder_var.get()
        self.assertLess(text.index('原生 Baseline'), text.index('私有网表 CLI'))
        self.assertLess(text.index('私有网表 CLI'), text.index('每候选原生生成'))
        for token in ('失败', 'native failure', '运行中', '跳过', 'stop unknown', 'SPECTRE', 'ALPS'):
            self.assertIn(token, text)
        app.execution_backend_var.set('auto')
        self.assertEqual(app.simulator_var.get(), 'alps')
        self.assertEqual(app.solver_command_vars['alps'].get(), 'custom {netlist}')
