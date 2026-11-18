import os
import sys
import tempfile
import types
import unittest
from unittest import mock

from crescendo_engine.runner import _evaluate_native_baseline_candidate


class BaselineDispatchTests(unittest.TestCase):
    def test_checkpoint_protocol_rejects_backend_mismatch_or_missing_backend(self):
        from crescendo_engine import runner
        with mock.patch.object(runner, 'validate_checkpoint_integrity', return_value=[]):
            for spec in ({'execution_protocol': 'crescendo.maestro_baseline.v1'},
                         {'execution_protocol': 'crescendo.maestro_baseline.v1', 'execution_backend': 'maestro_netlist'},
                         {'execution_backend': 'maestro_baseline', 'execution_protocol': 'crescendo.unified_execution.v1'}):
                with self.subTest(spec=spec):
                    with self.assertRaisesRegex(ValueError, 'Native Run checkpoint'):
                        runner.validate_checkpoint({'spec': spec}, {})

    def run_candidate(self, result=None, error=None):
        calls = []
        module = types.ModuleType('crescendo_engine.native_baseline')
        def evaluate(*args, **kwargs):
            calls.append((args, kwargs))
            if error:
                raise error
            return result
        module.evaluate_native_baseline = evaluate
        spec = {'objectives': [{'output_key': 'T::gain'}], 'constraints': []}
        with tempfile.TemporaryDirectory() as root:
            with mock.patch.dict(sys.modules, {'crescendo_engine.native_baseline': module}), \
                    mock.patch('crescendo_engine.runner.evaluate_metrics', return_value={'score': 1}):
                row = _evaluate_native_baseline_candidate(root, 2, 1, {'p': 3}, spec,
                    {'setup': {}}, [{'parameter_id': 'p'}], {}, lambda: True)
            files = os.listdir(os.path.join(root, 'candidates', 'g001_c0002'))
        return row, calls, files

    def test_complete_evidence_is_required_and_metrics_use_native_result(self):
        evidence = {'parsed': {'outputs': []}, 'parameter_application': {'ok': True},
            'completion_evidence': {'schema_version': 'crescendo.completion_evidence.v1', 'ok': True},
            'source_guard': {'ok': True}, 'command': {'cleanup_pending': False}}
        row, calls, files = self.run_candidate(evidence)
        self.assertEqual(row['status'], 'completed')
        self.assertEqual(row['execution_backend'], 'maestro_baseline')
        self.assertEqual(row['metrics'], {'score': 1})
        self.assertTrue(calls[0][1]['cancel_check']())
        self.assertIn('completion_evidence.json', files)

    def test_missing_cleanup_or_parameter_proof_never_completes(self):
        for command, application in (({}, {'ok': True}), ({'cleanup_pending': False}, {})):
            evidence = {'parsed': {}, 'parameter_application': application,
                'completion_evidence': {'schema_version': 'crescendo.completion_evidence.v1', 'ok': True},
                'source_guard': {'ok': True}, 'command': command}
            row, _, files = self.run_candidate(evidence)
            self.assertEqual(row['status'], 'infrastructure_failed')
            self.assertTrue(row['cleanup_pending'])
            self.assertNotIn('completion_evidence.json', files)

    def test_native_failure_retains_detail_and_does_not_score(self):
        error = RuntimeError('native stop uncertain')
        error.detail = {'cleanup_pending': True, 'native_stop': {'remote_stop_proven': False}}
        row, _, _ = self.run_candidate(error=error)
        self.assertEqual(row['status'], 'infrastructure_failed')
        self.assertTrue(row['cleanup_pending'])
        self.assertFalse(row['native_failure_detail']['native_stop']['remote_stop_proven'])
        self.assertNotIn('metrics', row)

    def test_reader_failure_stage_survives_lightweight_progress(self):
        from crescendo_engine.retention import lightweight_candidate_record
        from crescendo_engine.environment_diagnostics import failure_summary
        error = RuntimeError('Exact point PSF parameter readback timed out')
        error.detail = {'failure_stage': 'native_point_parameter_readback',
                        'cleanup_pending': False, 'native_baseline': {'cleanup_proven': True}}
        row, _, _ = self.run_candidate(error=error)
        lightweight = lightweight_candidate_record(row)
        summary = failure_summary({'state': 'failed', 'primary_failure': {
            'failure_stage': 'optimization', 'message': 'candidate failed'}}, [lightweight])
        self.assertEqual(summary['first_cause']['failure_stage'], 'native_point_parameter_readback')
        self.assertEqual(summary['first_cause_layer'], 'measurement')
        self.assertNotIn('native_failure_detail', lightweight)

    def test_failed_completion_with_proven_native_cleanup_retains_first_error(self):
        error = RuntimeError('required output missing')
        error.detail = {'cleanup_pending': False, 'native_baseline': {'cleanup_proven': True}}
        row, _, _ = self.run_candidate(error=error)
        self.assertEqual(row['status'], 'infrastructure_failed')
        self.assertFalse(row['cleanup_pending'])
        self.assertIn('required output missing', row['error'])
        self.assertNotIn('metrics', row)

    def test_failed_source_guard_survives_candidate_and_diagnostic_summary(self):
        from crescendo_engine.environment_diagnostics import failure_summary
        error = RuntimeError('Source changed during native baseline evaluation')
        error.detail = {'failure_stage': 'source_guard',
                        'source_guard': {'ok': False, 'changed': ['source.scs']},
                        'cleanup_pending': False, 'native_baseline': {'cleanup_proven': True}}
        row, _, _ = self.run_candidate(error=error)
        self.assertEqual(row['source_guard'], error.detail['source_guard'])
        self.assertFalse(row['cleanup_pending'])
        self.assertEqual(row['status'], 'infrastructure_failed')
        summary = failure_summary({'state': 'failed'}, [row])
        self.assertEqual(summary['first_cause']['failure_stage'], 'source_guard')
        self.assertNotIn('metrics', row)

    def test_native_corner_failure_details_reach_diagnostic_continuation_gate(self):
        from crescendo_engine.environment_diagnostics import failure_summary
        error = RuntimeError('Private corner identity differs')
        error.detail = {'failure_stage': 'native_corner_setup_identity',
                        'corner_identity': {'differences': [
                            {'reason': 'private_setup_clone_changed_semantics'}]},
                        'cleanup_pending': False, 'native_baseline': {'cleanup_proven': True}}
        row, _, _ = self.run_candidate(error=error)
        cause = failure_summary({'state': 'failed'}, [row])['first_cause']
        self.assertEqual(cause['failure_stage'], 'native_corner_setup_identity')
        self.assertEqual(cause['corner_identity'], error.detail['corner_identity'])

    def test_metric_failure_after_completed_native_cleanup_is_not_cleanup_failure(self):
        evidence = {'parsed': {}, 'parameter_application': {'ok': True},
            'completion_evidence': {'schema_version': 'crescendo.completion_evidence.v1', 'ok': True},
            'source_guard': {'ok': True}, 'command': {'cleanup_pending': False}}
        module = types.ModuleType('crescendo_engine.native_baseline')
        module.evaluate_native_baseline = lambda *args, **kwargs: evidence
        with tempfile.TemporaryDirectory() as root, \
                mock.patch.dict(sys.modules, {'crescendo_engine.native_baseline': module}), \
                mock.patch('crescendo_engine.runner.evaluate_metrics', side_effect=ValueError('metric missing')):
            row = _evaluate_native_baseline_candidate(root, 0, 0, {'p': 3},
                {'objectives': [{'output_key': 'T::gain'}]}, {'setup': {}}, [], {}, lambda: False)
        self.assertEqual(row['status'], 'infrastructure_failed')
        self.assertFalse(row['cleanup_pending'])
        self.assertIn('metric missing', row['error'])

    def test_cancelled_candidate_requires_remote_stop_proof(self):
        error = RuntimeError('cancelled')
        error.detail = {'cleanup_pending': False, 'native_baseline': {'cleanup_proven': True},
                        'native_stop': {'remote_stop_proven': True}}
        row, _, _ = self.run_candidate(error=error)
        self.assertEqual(row['status'], 'cancelled')
        self.assertFalse(row['cleanup_pending'])
        self.assertNotIn('metrics', row)


if __name__ == '__main__':
    unittest.main()

class BaselineLoopTests(unittest.TestCase):
    def test_single_test_candidates_run_serially_and_keep_protocol(self):
        self._check_loop()

    def test_failed_native_measurement_still_publishes_proven_shutdown(self):
        self._check_loop('failure')

    def test_cancelled_run_publishes_proven_native_shutdown(self):
        self._check_loop('stop')

    def test_exhausted_startup_crash_only_fails_its_candidate(self):
        self._check_loop('startup_crash')

    def test_unproven_startup_crash_aborts_before_next_candidate(self):
        self._check_loop('startup_cleanup_timeout')

    def test_completion_evidence_failure_only_fails_its_candidate(self):
        self._check_loop('completion')

    def test_completion_evidence_ownership_failure_aborts_before_next_candidate(self):
        self._check_loop('completion_ownership')

    def test_completion_evidence_cleanup_failure_aborts_before_next_candidate(self):
        self._check_loop('completion_cleanup')

    def _check_loop(self, fault=None):
        import copy
        from crescendo_engine import runner
        from crescendo_engine.config import DEFAULTS
        from crescendo_engine.utils import load_json
        from tests.test_netlist_optimizer import parsed
        with tempfile.TemporaryDirectory() as root:
            test = {'name': 'main', 'enabled': True, 'analyses': [{'name': 'dc', 'enabled': True}]}
            setup = {'backend': 'maestro', 'selected_test': 'main', 'tests': [test],
                '_source_tests': [copy.deepcopy(test)], 'corners': [{'name': 'nominal', 'enabled': True}],
                'test_corner_masks': {'main': {'known': True, 'disabled_corners': [], 'nominal_enabled': False}}}
            prepared = {'setup': setup, 'hierarchy': {'designs': []},
                'parameters': [{'parameter_id': 'x', 'backend': 'schematic', 'optimizable': True}],
                'outputs': [{'key': 'main::gain'}], 'eda_context': {'cadence': {'version_checks': []},
                'pdk': {'ok': True, 'pdk_name': 'FIXTURE', 'pdk_adapter': 'generic-cdf'},
                'submission': {'controller': 'maestro', 'simulator': 'spectre'}}}
            config = copy.deepcopy(DEFAULTS)
            config['runtime']['run_root'] = root
            spec = {'evaluator': 'real', 'execution_backend': 'maestro_baseline', 'selected_test': 'main',
                'execution_selection': {'tests': ['main'], 'analyses': {'main': ['dc']}},
                'native_candidate_batching': 'serial',
                'algorithm': 'random', 'generations': 1, 'population': 2, 'jobs': 4,
                'variables': [{'parameter_id': 'x', 'minimum': 0, 'maximum': 1}],
                'objectives': [{'output_key': 'main::gain', 'direction': 'maximize', 'corners': ['nominal']}],
                'constraints': []}
            calls = []
            module = types.ModuleType('crescendo_engine.native_baseline')
            def evaluate(candidate_id, values, parameters, active_spec, active_prepared, active_config, candidate_root, **kwargs):
                calls.append((candidate_id, active_spec['execution_backend']))
                if fault and len(calls) == 1:
                    if fault.startswith('startup_'):
                        proven = fault == 'startup_crash'
                        failure = RuntimeError('Native Ocean startup crash: cdsCrashReport: command not found')
                        failure.detail = {'failure_stage': 'native_startup_crash', 'cleanup_pending': not proven,
                                          'native_baseline': {'cleanup_proven': proven}}
                        raise failure
                    if fault.startswith('completion'):
                        from crescendo_engine.completion import CompletionEvidenceError, UnsafeCompletionEvidence
                        kind = UnsafeCompletionEvidence if fault == 'completion_ownership' else CompletionEvidenceError
                        proven = fault != 'completion_cleanup'
                        failure = kind('candidate point completion not proven')
                        failure.detail = {'failure_stage': 'native_completion_evidence', 'cleanup_pending': not proven,
                                          'native_baseline': {'cleanup_proven': proven}}
                        raise failure
                    if fault == 'stop':
                        with open(os.path.join(root, 'run', 'STOP'), 'w') as stream:
                            stream.write('stop')
                    failure = RuntimeError('native output missing' if fault == 'failure' else 'cancelled')
                    failure.detail = {'cleanup_pending': False, 'native_baseline': {'cleanup_proven': True},
                                      'native_stop': {'remote_stop_proven': fault == 'stop'}}
                    raise failure
                return {'parsed': parsed(values['x']), 'parameter_application': {'ok': True},
                    'completion_evidence': {'schema_version': 'crescendo.completion_evidence.v1', 'ok': True},
                    'source_guard': {'ok': True}, 'command': {'cleanup_pending': False}}
            module.evaluate_native_baseline = evaluate
            reconciled = []
            def reconcile(root, **kwargs):
                reconciled.append(root)
                if fault and fault in ('startup_crash', 'startup_cleanup_timeout', 'completion_cleanup') and root.endswith('g000_c0000'):
                    proven = fault == 'startup_crash'
                    return {'launch_attempted': True, 'submission_intent': False,
                            'cleanup_proven': proven, 'native_not_submitted': proven,
                            'local_cleanup': {'state': 'dead' if proven else 'ambiguous',
                                              'termination_proof': {'ok': proven}}}
                return {'launch_attempted': True, 'cleanup_proven': True, 'native_session_closed': True,
                        'native_stop': {'requested': fault == 'stop', 'remote_stop_proven': fault == 'stop'},
                        'local_cleanup': {'state': 'dead', 'termination_proof': {'ok': True}}}
            module.reconcile_native_baseline = reconcile
            with mock.patch.dict(sys.modules, {'crescendo_engine.native_baseline': module}), \
                    mock.patch.object(runner, '_reverify_prepared_external_environment', return_value={'ok': True}), \
                    mock.patch.object(runner, 'PrivateNetlistOptimizer') as cli:
                if fault in ('failure', 'startup_cleanup_timeout', 'completion_ownership', 'completion_cleanup'):
                    message = ('native output missing' if fault == 'failure' else
                               'owned simulation cleanup was not proven' if fault in ('startup_cleanup_timeout', 'completion_cleanup') else
                               'candidate point completion not proven')
                    with self.assertRaisesRegex(RuntimeError, message):
                        runner.run_optimization(spec, config, prepared=prepared, run_root=os.path.join(root, 'run'))
                    result = None
                else:
                    result = runner.run_optimization(spec, config, prepared=prepared, run_root=os.path.join(root, 'run'))
            cli.assert_not_called()
            if result:
                self.assertEqual(result['state'], 'stopped' if fault == 'stop' else 'completed')
            expected_count = 3 if fault in ('startup_crash', 'completion') else 1 if fault else 2
            self.assertEqual(len(calls), expected_count)
            self.assertTrue(all(backend == 'maestro_baseline' for _, backend in calls))
            status = load_json(os.path.join(root, 'run', 'status.json'))
            self.assertEqual(status['effective_parallel_jobs'], 1)
            self.assertEqual(status['requested_parallel_jobs'], 4)
            self.assertEqual(status['execution_protocol'], 'crescendo.maestro_baseline.v1')
            from crescendo_engine.shutdown_evidence import shutdown_proven
            shutdown = load_json(os.path.join(root, 'run', 'worker_shutdown.json'))
            self.assertEqual(shutdown_proven(shutdown), fault not in ('startup_cleanup_timeout', 'completion_cleanup'))
            self.assertEqual(len(reconciled), expected_count)
            self.assertFalse(status['final_cleanup']['raw_deleted'])
            self.assertEqual(status['final_cleanup']['root_index']['count'], expected_count)
            if fault in ('startup_crash', 'completion'):
                self.assertEqual(status['infrastructure_failed_candidates'], 1)
                self.assertEqual(status['completed_candidates'], 2)
                failed = load_json(os.path.join(root, 'run', 'candidates', 'g000_c0000', 'candidate.json'))
                self.assertEqual(failed['status'], 'infrastructure_failed')
                self.assertIn('cdsCrashReport: command not found' if fault == 'startup_crash' else
                              'candidate point completion not proven', failed['error'])
            if fault == 'failure':
                self.assertEqual(status['state'], 'failed')
                self.assertIn('native output missing', status['primary_failure']['message'])
                self.assertNotIn('cleanup_failure', status)
            for candidate_id, _ in calls:
                candidate_root = os.path.join(root, 'run', 'candidates', candidate_id)
                if not fault:
                    self.assertTrue(os.path.isfile(os.path.join(candidate_root, 'completion_evidence.json')))
                proof = load_json(os.path.join(candidate_root, 'raw_cleanup_proof.json'))
                self.assertEqual(proof['ok'], fault not in ('startup_cleanup_timeout', 'completion_cleanup'))
                self.assertFalse(proof['raw_deleted'])



class NativeDirectoryRetryTests(unittest.TestCase):
    def test_bounded_retries_use_unique_owned_requests_and_respect_stop_and_proof(self):
        from crescendo_engine.completion import NativeTemporaryDirectoryError
        from crescendo_engine.runner import _native_candidate_result

        error = NativeTemporaryDirectoryError("owned temp directory failure")
        error.detail = {"cleanup_pending": False, "source_guard": {"ok": True},
                        "native_stop": {"requested": False},
                        "native_baseline": {"cleanup_proven": True, "native_session_closed": True}}
        with tempfile.TemporaryDirectory() as root:
            existing = os.path.join(root, "candidates", "g000_c0000")
            for budget, cancel, proven, expected in ((1, False, True, 2), (0, False, True, 1),
                                                      (1, True, True, 1), (1, False, False, 1)):
                error.detail["native_baseline"]["cleanup_proven"] = proven
                owned = {existing}
                with self.subTest(budget=budget, cancel=cancel, proven=proven), mock.patch(
                        'crescendo_engine.native_baseline.evaluate_native_baseline', side_effect=error) as evaluate:
                    with self.assertRaises(NativeTemporaryDirectoryError):
                        _native_candidate_result("g000_c0000", {}, [], {}, {}, {}, existing,
                                                 lambda: cancel, owned, None, budget)
                    roots = [call[0][6] for call in evaluate.call_args_list]
                    self.assertEqual(len(roots), expected)
                    self.assertEqual(len(set(roots)), expected)
                    self.assertEqual(set(roots), owned)
                    self.assertTrue(all(os.path.dirname(path) == os.path.dirname(existing) for path in roots))

    def test_native_retry_configuration_freezes_only_on_new_or_explicit_runs(self):
        from crescendo_engine.runner import _freeze_native_directory_retries

        config = {"runtime": {"native_directory_retries": 0}}
        fresh, old, frozen = {}, {}, {"native_directory_retries": 2}
        _freeze_native_directory_retries(fresh, config, resuming=False)
        _freeze_native_directory_retries(old, config, resuming=True)
        _freeze_native_directory_retries(frozen, config, resuming=True)
        self.assertEqual(fresh["native_directory_retries"], 0)
        self.assertNotIn("native_directory_retries", old)
        self.assertEqual(frozen["native_directory_retries"], 2)
        with self.assertRaisesRegex(ValueError, "native_directory_retries"):
            _freeze_native_directory_retries({"native_directory_retries": 1.5}, config, resuming=False)
