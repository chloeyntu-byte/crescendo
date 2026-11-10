import os
import tempfile
import threading
import unittest
from unittest import mock

from crescendo_engine.range_validation import _native_endpoint_rows, run_range_validation


class NativeRangeValidationTests(unittest.TestCase):
    def prepared(self):
        return {'setup': {'tests': [{'name': 'T'}]}, 'parameters': [
            {'parameter_id': 'w', 'optimizable': True, 'name': 'w'}]}

    def manifest(self, value, second=None):
        points = [{'test': 'T', 'corner': 'C1', 'point_id': 1,
                   'effective': value, 'scs_path': '/retained/C1/input.scs', 'dpar_status': 'resolved',
                   'callback_adjustment_proven': True}]
        if second is not None:
            points.append(dict(points[0], corner='C2', point_id=2, effective=second))
        return {'parameter_application': {'proof_kind': 'native_run_device_parameters', 'ok': True,
                'parameters': [{'parameter_id': 'w', 'points': points}]}}

    def test_legal_symbolic_native_proof_retains_exact_point_evidence_and_adjustment(self):
        parameters = self.prepared()['parameters']
        result = _native_endpoint_rows(parameters, 'minimum', {'w': 1}, self.manifest(2))
        self.assertEqual(result[0]['status'], 'pdk_adjusted')
        self.assertEqual(result[0]['effective'], 2)
        self.assertEqual(result[0]['evidence']['points'][0]['scs_path'], '/retained/C1/input.scs')
        self.assertFalse(result[0]['evidence']['private_oa_backannotation_proven'])

    def test_point_dependent_adjustment_never_selects_a_corner_arbitrarily(self):
        result = _native_endpoint_rows(self.prepared()['parameters'], 'minimum', {'w': 1}, self.manifest(2, 3))
        self.assertEqual(result[0]['status'], 'validation_failed')
        self.assertIsNone(result[0]['effective'])
        self.assertEqual(len(result[0]['evidence']['points']), 2)

    def test_old_oa_cdf_or_marker_is_not_accepted_as_native_run_proof(self):
        for manifest in ({'schematic_verification': {'parameters': [{'parameter_id': 'w', 'cdf_value': 2}]}},
                         {'parameter_application': {'ok': True, 'parameters': []}}):
            result = _native_endpoint_rows(self.prepared()['parameters'], 'minimum', {'w': 1}, manifest)
            self.assertEqual(result[0]['status'], 'validation_failed')

    def test_requested_dpar_binding_is_not_pdk_adjustment_readback(self):
        manifest = self.manifest(1)
        manifest['parameter_application']['parameters'][0]['points'][0]['callback_adjustment_proven'] = False
        result = _native_endpoint_rows(self.prepared()['parameters'], 'minimum', {'w': 1}, manifest)
        self.assertEqual(result[0]['status'], 'validation_failed')
        self.assertIsNone(result[0]['effective'])
        self.assertIn('could not be confirmed', result[0]['error'])

    def test_endpoints_use_shared_native_generation_without_solver_or_direct_cdf(self):
        updates, calls = [], []
        with tempfile.TemporaryDirectory() as root:
            def generate(prepared, parameters, values, config, path, identifier, **kwargs):
                calls.append((dict(values), kwargs))
                os.makedirs(path)
                with open(os.path.join(path, 'input.scs'), 'w') as stream:
                    stream.write('parameters DPAR_1=%s\n' % values['w'])
                return self.manifest(values['w'])
            with mock.patch('crescendo_engine.netlist_providers.prepare_maestro_netlists', side_effect=generate), \
                    mock.patch('crescendo_engine.private_netlist.reconcile_preparation', return_value={'cleanup_proven': True}), \
                    mock.patch('crescendo_engine.range_validation.apply_candidate', side_effect=AssertionError('bare CDF forbidden')):
                run_range_validation({'job_root': root}, self.prepared(),
                    [{'parameter_id': 'w', 'minimum': 1, 'maximum': 2}], {'runtime': {}},
                    threading.Event(), lambda **row: updates.append(row))
            self.assertTrue(os.path.isfile(os.path.join(root, 'minimum', 'input.scs')))
            self.assertTrue(os.path.isfile(os.path.join(root, 'maximum', 'input.scs')))
        self.assertEqual(updates[-1]['state'], 'completed')
        self.assertEqual([row[0]['w'] for row in calls], [1, 2])
        self.assertTrue(all(row[1]['generation_only'] and row[1]['allow_effective_adjustment'] for row in calls))

    def test_first_native_error_does_not_repeat_maximum_or_hide_cleanup_failure(self):
        updates = []
        with tempfile.TemporaryDirectory() as root:
            with mock.patch('crescendo_engine.netlist_providers.prepare_maestro_netlists', side_effect=RuntimeError('PDK callback error')) as generate, \
                    mock.patch('crescendo_engine.private_netlist.reconcile_preparation', return_value={'cleanup_proven': False}):
                run_range_validation({'job_root': root}, self.prepared(),
                    [{'parameter_id': 'w', 'minimum': 1, 'maximum': 2}], {'runtime': {}},
                    threading.Event(), lambda **row: updates.append(row))
            self.assertEqual(generate.call_count, 1)
        self.assertEqual(updates[-1]['state'], 'failed')
        self.assertTrue(updates[-1]['unsafe_to_optimize'])

    def test_cancelled_generation_cleans_owned_capture_and_never_completes(self):
        event, updates = threading.Event(), []
        def generate(*args, **kwargs):
            self.assertTrue(callable(kwargs['cancel_check']))
            event.set()
            raise RuntimeError('cancelled')
        with tempfile.TemporaryDirectory() as root:
            with mock.patch('crescendo_engine.netlist_providers.prepare_maestro_netlists', side_effect=generate), \
                    mock.patch('crescendo_engine.private_netlist.reconcile_preparation', return_value={'cleanup_proven': True}) as cleanup:
                run_range_validation({'job_root': root}, self.prepared(),
                    [{'parameter_id': 'w', 'minimum': 1, 'maximum': 2}], {'runtime': {}},
                    event, lambda **row: updates.append(row))
            cleanup.assert_called_once()
        self.assertEqual(updates[-1]['state'], 'cancelled')
        self.assertNotIn('completed', [row.get('state') for row in updates])


if __name__ == '__main__':
    unittest.main()
