import os
import tempfile
import threading
import time
import unittest
from unittest import mock

from crescendo_engine.server import OptimizerService


class NativeValidationServiceTests(unittest.TestCase):
    def test_ready_legacy_capability_does_not_start_bare_cdf_session(self):
        with tempfile.TemporaryDirectory() as root:
            service = OptimizerService({'runtime': {'run_root': root}})
            service.prepared['P'] = {'setup': {'tests': [{'name': 'T', 'enabled': True}], 'selected_test': 'T', '_source_tests': [{'name': 'T', 'enabled': True}]}, 'parameters': [{'parameter_id': 'w', 'optimizable': True}],
                                     'resident_validation': {'state': 'ready'}}
            def native(_job, _prepared, _variables, _config, _cancel, update):
                update(state='completed', message='native complete')
            with mock.patch.object(service, '_start_resident_validation', side_effect=AssertionError('bare CDF forbidden')) as resident, \
                    mock.patch('crescendo_engine.server.run_range_validation', side_effect=native) as run:
                response = service.validate_ranges('P', [{'parameter_id': 'w', 'minimum': 1, 'maximum': 2}])
                service.range_validation_threads[response['job_id']].join(2)
            resident.assert_not_called()
            self.assertNotIn('resident_session', run.call_args[1])
            self.assertEqual(service.range_validation_status(response['job_id'])['state'], 'completed')

    def test_owned_native_cleanup_retry_releases_old_failure_for_override(self):
        with tempfile.TemporaryDirectory() as root:
            service = OptimizerService({'runtime': {'run_root': root}})
            job_id = 'range_validation_owned'
            job_root = os.path.join(root, 'range_validations', job_id)
            capture = os.path.join(job_root, 'minimum')
            os.makedirs(capture)
            thread = threading.Thread(target=lambda: None)
            thread.start()
            thread.join()
            service.range_validations[job_id] = {
                'job_root': job_root, 'state': 'failed', 'unsafe_to_optimize': True,
                'unsafe_cleanup_scope': 'native_range_capture', 'cleanup_root': capture,
            }
            service.range_validation_threads[job_id] = thread
            service.range_validation_cancels[job_id] = threading.Event()
            with mock.patch('crescendo_engine.private_netlist.reconcile_preparation', return_value={'cleanup_proven': True}) as cleanup:
                service._cancel_and_wait_range_validations(timeout_seconds=1, action='save EDA settings')
            cleanup.assert_called_once()
            self.assertFalse(service.range_validations[job_id]['unsafe_to_optimize'])

    def test_cleanup_retry_never_uses_unregistered_root_or_unknown_live_thread(self):
        with tempfile.TemporaryDirectory() as root:
            service = OptimizerService({'runtime': {'run_root': root}})
            service.range_validations['P'] = {
                'job_root': os.path.join(root, 'job'), 'state': 'failed', 'unsafe_to_optimize': True,
                'unsafe_cleanup_scope': 'native_range_capture', 'cleanup_root': '/unrelated',
            }
            with mock.patch('crescendo_engine.private_netlist.reconcile_preparation') as cleanup:
                service._retry_range_validation_native_cleanup(['P'], time.monotonic() + 1)
            cleanup.assert_not_called()
            self.assertTrue(service.range_validations['P']['unsafe_to_optimize'])


if __name__ == '__main__':
    unittest.main()
