import copy
import unittest
from unittest import mock

from tests import test_environment_test as fixtures


class MultiDiagnosticManagerTests(unittest.TestCase):
    setUp = fixtures.EnvironmentTestManagerTests.setUp
    fixture = fixtures.EnvironmentTestManagerTests.fixture
    finish = fixtures.EnvironmentTestManagerTests.finish
    checks = fixtures.EnvironmentTestManagerTests.checks
    finish_threads = fixtures.EnvironmentTestManagerTests.finish_threads

    def test_three_modes_execute_identical_six_candidates_and_retain_all_proofs(self):
        manager, service, spec = self.fixture()
        payload = self.finish(manager, manager.start('prepared-fixture', spec)['test_id'])
        self.assertEqual(payload['state'], 'completed')
        self.assertEqual(payload['diagnostic_shape']['distinct_candidates'], 6)
        sequences = [[(r['generation'], r['index'], r['values']) for r in snapshot['candidates']]
                     for snapshot in service.snapshots.values()]
        self.assertEqual(len(sequences), 3)
        self.assertTrue(all(seq == sequences[0] and len(seq) == 6 for seq in sequences))
        for proof in ('execution.pipeline', 'completion_evidence', 'parameter_application', 'source_guard', 'metrics'):
            checks = self.checks(payload, proof)
            self.assertEqual(len(checks), 18, proof)
            self.assertEqual(len({check['id'] for check in checks}), 18, proof)
            self.assertTrue(all(check['status'] == 'PASS' for check in checks), proof)
        self.assertEqual(payload['timing']['status'], 'completed')
        self.assertIn('report', payload['timing']['phases_seconds'])
        self.assertGreater(payload['timing']['total_seconds'], 0)
        self.assertTrue(all(case['timing']['total_seconds'] > 0 for case in payload['case_results']))
        with open(payload['main_log'], encoding='utf-8') as stream:
            self.assertIn('diagnostic_timing ', stream.read())

    def test_user_total_runs_exact_count_in_all_modes_and_reports_shape(self):
        manager, service, spec = self.fixture()
        spec.pop('diagnostic_samples', None)
        spec.update(diagnostic_generations=3, diagnostic_total_candidates=12)
        # Three existing 4 s engine budgets plus bounded report drain. The
        # larger sample still has all exact count/sequence assertions below.
        payload = self.finish(manager, manager.start('prepared-fixture', spec)['test_id'], timeout=60)
        self.assertEqual(payload['state'], 'completed')
        self.assertEqual(payload['diagnostic_shape']['total_candidates'], 12)
        self.assertEqual(payload['diagnostic_shape']['samples_per_generation'], 4)
        sequences = [[(r['generation'], r['index'], r['values']) for r in snapshot['candidates']]
                     for snapshot in service.snapshots.values()]
        self.assertEqual(len(sequences), 3)
        self.assertTrue(all(seq == sequences[0] and len(seq) == 12 for seq in sequences))
        self.assertEqual([(g, i) for g, i, values in sequences[0]],
                         [(g, i) for g in range(3) for i in range(4)])

    def test_first_candidate_failure_is_not_overwritten_by_five_later_successes(self):
        for proof in ('parameter_application', 'source_guard', 'pipeline', 'simulator'):
            with self.subTest(proof=proof):
                manager, service, spec = self.fixture()
                original_progress = service.progress
                def progress(name):
                    result = original_progress(name)
                    if (result['status'].get('state') == 'completed' and
                            result['candidates'][0]['execution_backend'] == 'private_netlist'):
                        first = result['candidates'][0]
                        if proof == 'pipeline':
                            first['completion_evidence']['execution_pipeline']['solver'] = 'wrong_solver'
                        elif proof == 'simulator':
                            first['completion_evidence']['execution_simulator'] = 'alps'
                        else:
                            first[proof] = {'ok': False, 'message': 'first candidate mismatch'}
                    return result
                service.progress = progress
                payload = self.finish(manager, manager.start('prepared-fixture', spec)['test_id'])
                self.assertEqual(payload['case_results'][1]['state'], 'failed')
                if proof == 'source_guard':
                    self.assertEqual(payload['case_results'][2]['state'], 'blocked')
                else:
                    self.assertEqual(payload['case_results'][2]['state'], 'completed')
                if proof in ('parameter_application', 'source_guard'):
                    checks = [c for c in self.checks(payload, proof) if c['engine'] == 'private_netlist']
                    self.assertEqual([c['status'] for c in checks], ['FAIL'] + ['PASS'] * 5)

    def test_actual_sequence_mismatch_fails_even_with_all_completion_proofs(self):
        manager, service, spec = self.fixture()
        original_progress = service.progress
        def progress(name):
            result = original_progress(name)
            if result['status'].get('state') == 'completed':
                result['candidates'][0]['values']['x'] = result['candidates'][-1]['values']['x']
            return result
        service.progress = progress
        payload = self.finish(manager, manager.start('prepared-fixture', spec)['test_id'])
        self.assertTrue(all(case['state'] == 'failed' for case in payload['case_results']))
        self.assertEqual(len(self.checks(payload, 'candidate_identity')), 3)

    def test_timer_failure_does_not_change_execution_or_stop(self):
        manager, service, spec = self.fixture()
        with mock.patch('crescendo_engine.environment_test.DiagnosticTiming.snapshot', side_effect=RuntimeError('clock unavailable')), \
             mock.patch('crescendo_engine.environment_test.DiagnosticTiming.transition', side_effect=RuntimeError('clock unavailable')):
            payload = self.finish(manager, manager.start('prepared-fixture', spec)['test_id'])
        self.assertEqual(payload['state'], 'completed')
        self.assertEqual(len(service.starts), 3)
        self.assertFalse(payload['timing']['available'])
        self.assertFalse(service.stops)

    def test_explicit_one_by_one_remains_available(self):
        manager, service, spec = self.fixture()
        spec.update(diagnostic_generations=1, diagnostic_samples=1)
        payload = self.finish(manager, manager.start('prepared-fixture', spec)['test_id'])
        self.assertEqual(payload['state'], 'completed')
        self.assertEqual(payload['diagnostic_shape']['total_candidates'], 1)
        self.assertTrue(all(len(snapshot['candidates']) == 1 for snapshot in service.snapshots.values()))
