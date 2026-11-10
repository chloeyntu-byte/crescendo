import copy
import json
import unittest

from crescendo_engine.diagnosis_catalog import concise_multi_success
from crescendo_engine.environment_diagnostics import pipeline_summary


class MultiDiagnosticLogTests(unittest.TestCase):
    def test_actual_optimization_values_not_copied_to_success_log(self):
        value = {'ok': True, 'candidate_id': 'g001_c0002', 'parameters': [
            {'requested': 1.234567, 'effective': 1.234567}],
            'metrics': {'objectives': [{'value': 987.654321}], 'feasible': False},
            'timing': {'total_seconds': 2.5}, 'frozen_plan': [{'values': {'x': 1.234567}}]}
        before = copy.deepcopy(value)
        compact = concise_multi_success('metrics', value)
        self.assertEqual(value, before)
        self.assertNotIn('1.234567', json.dumps(compact))
        self.assertNotIn('987.654321', json.dumps(compact))
        self.assertFalse(compact['metrics']['feasible'])
        self.assertEqual(compact['timing']['total_seconds'], 2.5)
        self.assertEqual(compact['parameters_count'], 1)

    def test_large_fallback_guard_cannot_push_point_receipt_out_of_report(self):
        pipeline = {'solver': 'shared_cli', 'netlist_source': 'maestro',
            'netlist_generation': {'source_mode': 'maestro_netlist',
                'native_generation_required': True, 'template_qualified': False,
                'template_fallback': {'failure_stage': 'native_numeric_derived_parameters',
                    'source_guard': {'ok': True, 'libraries': ['x' * 1000] * 200}}},
            'points': [{'task_id': 'point1', 'terminal_receipt': {'returncode': 0, 'cleanup_proven': True}}]}
        before = copy.deepcopy(pipeline)
        result = pipeline_summary(pipeline)
        self.assertLess(len(json.dumps(result)), 2000)
        self.assertTrue(result['points'][0]['terminal_receipt']['cleanup_proven'])
        self.assertEqual(result['netlist_generation']['template_fallback']['source_guard'], {'ok': True})
        self.assertEqual(before, pipeline)
