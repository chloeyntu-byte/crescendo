# -*- coding: utf-8 -*-
"""Regression for selected metrics rejected because their native corner is absent."""
import copy
import unittest
from unittest import mock
from crescendo_engine.environment_test import _request_blocking_reasons, _request_gate_failure, _case_capability
from crescendo_engine.environment_diagnostics import pipeline_qualification
from tests import test_environment_test as environment_fixture


class MetricInputDiagnosticsTests(unittest.TestCase):
    def test_baseline_survives_saved_settings_validation(self):
        from crescendo_engine.settings_state import SCHEMA, match_settings_state
        document = {'schema_version': SCHEMA, 'optimization': {'execution_backend': 'maestro_baseline', 'simulator': 'spectre'}}
        matched = match_settings_state(document, {})
        self.assertEqual(matched['apply']['optimization']['execution_backend'], 'maestro_baseline')


    def test_valid_subset_cannot_execute_when_another_selected_objective_is_invalid(self):
        row = {'prepared': {'setup': {}}, 'storage_error': None, 'write_error': None,
               'spec': {'variables': [{'parameter_id': 'p'}], 'objectives': [{'output_key': 'T::valid'}],
                        'objectives_valid': False, 'constraints_valid': True,
                        'ui_metric_drafts': {'objectives': [{'output_key': 'T::valid'}, {'output_key': 'T::gain'}]},
                        'metric_definition_errors': [{'kind': 'objective', 'index': 2,
                                                      'output_key': 'T::gain', 'error': 'Select at least one corner for T::gain'}]}}
        reasons = _request_blocking_reasons(row)
        self.assertEqual(reasons, ['objectives_valid'])
        failure = _request_gate_failure(row, reasons)
        self.assertIn('Select at least one corner', failure['message'])
        self.assertEqual(failure['metric_draft_counts']['objectives'], 2)
        self.assertFalse(failure['execution_started'])
        self.assertNotIn('选择至少一个优化目标。', failure['recovery_actions'])
        row['spec']['objectives'] = []
        self.assertEqual(_request_blocking_reasons(row), ['objectives_valid'])

    def test_native_readiness_preserves_saved_solver_without_cli_model_gate(self):
        prepared = {'setup': {'tests': [{'name': 'T', 'simulator': 'spectre'}]}}
        config = {'cadence': {'ocean_bin': '/native/ocean'}}
        with mock.patch('crescendo_engine.environment_test.shutil.which', return_value='/native/ocean'), \
             mock.patch('crescendo_engine.simulators.simulator_readiness', side_effect=AssertionError('CLI preflight called')):
            self.assertTrue(_case_capability(config, prepared, 'spectre', 'maestro_baseline')['ready'])
            self.assertFalse(_case_capability(config, prepared, 'alps', 'maestro_baseline')['ready'])

    def test_baseline_diagnostic_rejects_cli_or_incomplete_native_receipts(self):
        candidate = {'completion_evidence': {'ok': True, 'execution_simulator': 'spectre',
            'history_name': 'Interactive.3', 'points': [{'test': 'T', 'corner': 'TT', 'point_id': 1}],
            'simulator_runs': [{'test': 'T', 'error_count': 0}], 'execution_pipeline': {
                'schema_version': 'crescendo.maestro_baseline.v1', 'solver': 'native_maestro', 'submission': 'native_job_setup',
                'run_id': 'run3', 'history_name': 'Interactive.3', 'rdb_path': '/owned/Interactive.3.rdb',
                'native_run_invoked': True, 'native_wait_completed': True,
                'native_session_closed': True, 'solver_completion_proven': True}}}
        self.assertTrue(pipeline_qualification(candidate, 'maestro_baseline', 'spectre')['ok'])
        for key, value in [('solver', 'shared_cli'), ('native_wait_completed', False), ('history_name', 'Interactive.2')]:
            altered = copy.deepcopy(candidate)
            altered['completion_evidence']['execution_pipeline'][key] = value
            self.assertFalse(pipeline_qualification(altered, 'maestro_baseline', 'spectre')['ok'])


class MetricInputIntegrationTests(unittest.TestCase):
    finish_threads = environment_fixture.EnvironmentTestManagerTests.finish_threads
    setUp = environment_fixture.EnvironmentTestManagerTests.setUp
    tearDown = environment_fixture.EnvironmentTestManagerTests.tearDown
    fixture = environment_fixture.EnvironmentTestManagerTests.fixture
    finish = environment_fixture.EnvironmentTestManagerTests.finish
    checks = environment_fixture.EnvironmentTestManagerTests.checks

    def test_selected_invalid_row_is_logged_and_never_submitted(self):
        manager, service, spec = self.fixture()
        spec.update(objectives_valid=False, metric_definition_errors=[{
            'kind': 'objective', 'index': 1, 'output_key': 'main::gain', 'error': 'Corner TT is unavailable'}],
            ui_metric_drafts={'objectives': [{'output_key': 'main::gain', 'corners': ['TT']}]})
        payload = self.finish(manager, manager.start('prepared-fixture', spec)['test_id'])
        self.assertEqual(service.starts, [])
        check = self.checks(payload, 'execution')[0]
        self.assertEqual(check['status'], 'BLOCKED')
        self.assertIn('Corner TT is unavailable', check['message'])
        self.assertEqual(check['details']['first_cause']['blocking_reasons'], ['objectives_valid'])
