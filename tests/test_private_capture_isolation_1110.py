"""Owned capture session isolation and generation-failure reporting."""
import os
import tempfile
import unittest
from contextlib import ExitStack
from unittest import mock

from crescendo_engine.private_netlist import (_capture_child_environment, _capture_failure_text,
                                                _raise_capture_failure, _capture_phase, PrivateNetlistUnsupported)


class CaptureIsolationTests(unittest.TestCase):
    def test_capture_phase_passes_isolated_environment_to_owned_command(self):
        with tempfile.TemporaryDirectory() as root:
            workspace = {'root': root, 'setup': {'tests': []}, 'primary_library': 'lib',
                         'setup_cell': 'cell', 'setup_view': 'maestro'}
            config = {'cadence': {'ocean_bin': '/cadence/ocean'}, 'runtime': {}}
            captured = {}
            def intercept(*args, **kwargs):
                captured.update(kwargs['env'])
                raise RuntimeError('intercepted capture launch')
            replacements = {
                'crescendo_engine.private_netlist.require_unique_run_variables': None,
                'crescendo_engine.private_netlist._capture_setup_identity': {},
                'crescendo_engine.private_netlist.reset_results': None,
                'crescendo_engine.private_netlist.write_schematic_patch': {},
                'crescendo_engine.private_netlist._capture_timeout_profiles': {},
                'crescendo_engine.private_netlist._capture_setup_skill': ([], [], {}),
                'crescendo_engine.private_netlist._workspace_startup_load_lines': [],
                'crescendo_engine.private_netlist.workspace_test_design_lines': [],
                'crescendo_engine.private_netlist._capture_mode_skill': [],
                'crescendo_engine.private_netlist.native_corner_schedule_skill': [],
                'crescendo_engine.private_netlist._capture_worker_logs': ({}, {}),
                'crescendo_engine.corner_selection.capture_selection_plan': None,
                'crescendo_engine.corner_selection.effective_setup': workspace['setup'],
                'crescendo_engine.test_selection.capture_selection_plan': None,
                'crescendo_engine.test_selection.effective_setup': workspace['setup'],
                'crescendo_engine.single_test.native_selection': None,
                'crescendo_engine.test_selection.selection_skill_lines': [],
                'crescendo_engine.corner_selection.selection_skill_lines': [],
                'crescendo_engine.storage_policy.native_storage_readback_lines': [],
                'crescendo_engine.native_variable_scope_readback.native_variable_scope_skill_lines': [],
                'crescendo_engine.workspace.bounded_skill_body_lines': ([], {}),
                'crescendo_engine.native_diagnostics.native_run_trace_lines': [],
            }
            with ExitStack() as stack:
                stack.enter_context(mock.patch.dict(os.environ, {'CDS_MPS_SESSION': 'virtuoso123',
                    'CDS_LIC_FILE': 'site-license', 'SITE_SCHEDULER_FLAG': 'preserved'}))
                for target, value in replacements.items():
                    stack.enter_context(mock.patch(target, return_value=value))
                stack.enter_context(mock.patch('crescendo_engine.netlist_measurement.run_owned_command', side_effect=intercept))
                with self.assertRaisesRegex(RuntimeError, 'intercepted capture launch'):
                    _capture_phase(workspace, [], {}, {}, {}, config, os.path.join(root, 'capture'), baseline_only=True)
                self.assertEqual(os.environ['CDS_MPS_SESSION'], 'virtuoso123')
            self.assertNotIn('CDS_MPS_SESSION', captured)
            self.assertEqual(captured['CDS_LIC_FILE'], 'site-license')
            self.assertEqual(captured['SITE_SCHEDULER_FLAG'], 'preserved')
            self.assertEqual(captured['PATH'].split(os.pathsep)[0], os.path.join(root, 'bin'))

    def test_owned_environment_drops_only_parent_session(self):
        with tempfile.TemporaryDirectory() as root:
            with mock.patch.dict(os.environ, {'CDS_MPS_SESSION': 'virtuoso123',
                                              'CDS_LIC_FILE': 'site-license',
                                              'SITE_SCHEDULER_FLAG': 'preserved'}):
                env = _capture_child_environment({'cadence': {}}, root, '/capture/bin')
                self.assertEqual(os.environ['CDS_MPS_SESSION'], 'virtuoso123')
            self.assertNotIn('CDS_MPS_SESSION', env)
            self.assertEqual(env['CDS_LIC_FILE'], 'site-license')
            self.assertEqual(env['SITE_SCHEDULER_FLAG'], 'preserved')
            self.assertEqual(env['PATH'].split(os.pathsep)[0], '/capture/bin')

    def test_failure_text_preserves_primary_and_skips_wrapper_aftermath(self):
        worker = {'logs': [{'error_lines': [
            {'stage': 'capture_wrapper_aftermath', 'text': 'expected wrapper exit'},
            {'stage': 'native_generation', 'text': '*Error* Could not create directory: /owned'},
        ]}]}
        self.assertEqual(_capture_failure_text({}, worker), '*Error* Could not create directory: /owned')
        self.assertEqual(_capture_failure_text({'first_error': {'message': 'Ocean fatal'}}, worker), 'Ocean fatal')
        worker['logs'][0]['error_lines'] = worker['logs'][0]['error_lines'][:1]
        self.assertEqual(_capture_failure_text({}, worker), 'native netlist capture failed')
        worker['logs'][0]['error_lines'] = [{'stage': 'native_generation', 'text': 'x' * 5000}]
        self.assertEqual(len(_capture_failure_text({}, worker)), 1024)

    def test_worker_failure_raises_with_original_evidence(self):
        row = {'stage': 'native_generation', 'line': 42,
               'text': '*Error* Could not create directory: /owned'}
        worker = {'ok': False, 'logs': [{'path': '/owned/Job0.log', 'error_lines': [row]}]}
        with self.assertRaises(PrivateNetlistUnsupported) as caught:
            _raise_capture_failure(False, {'guardian_cleanup': {'state': 'dead'}}, {}, worker,
                                   {}, {}, '/owned')
        self.assertIn(row['text'], str(caught.exception))
        self.assertEqual(caught.exception.detail['failure_stage'], 'native_capture_worker_generation_error')
        self.assertEqual(caught.exception.detail['native_worker_diagnostics']['logs'][0]['path'], '/owned/Job0.log')
        self.assertEqual(caught.exception.detail['native_worker_diagnostics']['logs'][0]['error_lines'][0]['line'], 42)
        self.assertTrue(caught.exception.detail['cleanup_proven'])


if __name__ == '__main__':
    unittest.main()
