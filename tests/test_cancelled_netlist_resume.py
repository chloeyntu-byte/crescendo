"""Real service origin reconciliation over durable control-flow checkpoints.

Native ownership is an explicit boundary fixture; no EDA/remote-stop claim.
The regression must exercise the service's actual cancelled-record branch.
"""
import copy
import os
import unittest
from contextlib import contextmanager
from unittest import mock

from crescendo_engine import runner
from crescendo_engine.server import OptimizerService
from crescendo_engine.utils import atomic_write_json, load_json
from tests import test_unified_optimizer as fixtures
from tests.test_stop_resume_shared_cli import stop_second_adapter
from tests.test_runner import _coordinated_runner_resume


class CancelledNetlistResumeTests(unittest.TestCase):
    def fixture(self, backend='private_netlist'):
        fixture = fixtures.UnifiedOptimizerTests()
        fixture.setUp()
        self.addCleanup(fixture.tearDown)
        spec = dict(fixture.spec, execution_backend=backend)
        origin = os.path.join(fixture.root, 'stopped')
        with mock.patch.object(runner, 'PrivateNetlistOptimizer', stop_second_adapter([])):
            stopped = runner.run_optimization(spec, fixture.config, prepared=fixture.prepared, run_root=origin)
        self.assertEqual(stopped['state'], 'stopped')
        checkpoint = load_json(os.path.join(origin, 'checkpoint.json'))
        candidate = os.path.join(origin, 'candidates', 'g000_c0001', 'candidate.json')
        return fixture, OptimizerService(fixture.config), checkpoint, candidate

    @contextmanager
    def ownership(self, solver=True, native=True):
        with mock.patch('crescendo_engine.netlist_execution.reconcile_previous_ownership',
                        return_value={'cleanup_proven': solver, 'fixture': 'owned solver boundary'}), \
                mock.patch('crescendo_engine.netlist_run_ownership.reconcile_run_preparations',
                           return_value={'cleanup_proven': native, 'fixture': 'owned native boundary'}):
            yield

    def test_cancelled_origin_is_retained_and_retried_without_reusing_fitness(self):
        for backend in ('private_netlist', 'maestro_netlist'):
            with self.subTest(backend=backend):
                fixture, service, checkpoint, path = self.fixture(backend)
                before = copy.deepcopy(checkpoint)
                cancelled = load_json(path)
                with self.ownership():
                    cleanup = service._cleanup_checkpoint_origin(checkpoint)
                self.assertIn('g000_c0001', cleanup['retained_lightweight'])
                self.assertEqual(cleanup['recovered_terminal_wal'], [])
                self.assertEqual(load_json(path), cancelled)
                self.assertEqual(checkpoint, before)
                self.assertEqual([row['candidate_id'] for row in runner.checkpoint_candidates(checkpoint)], ['g000_c0000'])
                instances = []
                with mock.patch.object(runner, 'PrivateNetlistOptimizer', fixtures.common_adapter(instances)):
                    resumed = _coordinated_runner_resume(checkpoint['spec'], fixture.config,
                        fixture.prepared, os.path.join(fixture.root, 'resumed'), checkpoint)
                self.assertEqual(resumed['state'], 'completed')
                self.assertEqual(instances[0].calls[0], (0, 1, cancelled['values']))
                self.assertEqual(len(instances[0].calls), 7)
                self.assertNotIn((0, 0), [(g, i) for g, i, values in instances[0].calls])

    def test_cancelled_origin_requires_exact_active_plan_and_user_stop_identity(self):
        mutations = {
            'wrong_values': lambda row: row.update(values={'x': -99}),
            'wrong_generation': lambda row: row.update(generation=1),
            'wrong_index': lambda row: row.update(index=2),
            'wrong_record_id': lambda row: row.update(candidate_id='g000_c0002'),
            'not_user_stop': lambda row: row.update(failure_class='solver_error'),
            'wrong_backend': lambda row: row.update(execution_backend='maestro_netlist'),
            'unrecognized_status': lambda row: row.update(status='arbitrary'),
        }
        for name, mutate in mutations.items():
            with self.subTest(case=name):
                fixture, service, checkpoint, path = self.fixture()
                row = load_json(path)
                mutate(row)
                atomic_write_json(path, row)
                with self.ownership(), self.assertRaises(RuntimeError):
                    service._cleanup_checkpoint_origin(checkpoint)
                self.assertEqual(load_json(path), row)

    def test_cancelled_namespace_not_in_plan_remains_rejected(self):
        fixture, service, checkpoint, path = self.fixture()
        row = load_json(path)
        other = os.path.join(os.path.dirname(os.path.dirname(path)), 'g000_c0099')
        os.rename(os.path.dirname(path), other)
        row.update(candidate_id='g000_c0099', index=99)
        atomic_write_json(os.path.join(other, 'candidate.json'), row)
        with self.ownership(), self.assertRaisesRegex(RuntimeError, 'active checkpoint plan'):
            service._cleanup_checkpoint_origin(checkpoint)

    def test_legacy_cancelled_record_without_optional_failure_class_is_retained(self):
        fixture, service, checkpoint, path = self.fixture()
        row = load_json(path)
        row.pop('failure_class')
        atomic_write_json(path, row)
        with self.ownership():
            cleanup = service._cleanup_checkpoint_origin(checkpoint)
        self.assertIn('g000_c0001', cleanup['retained_lightweight'])
        self.assertEqual(cleanup['recovered_terminal_wal'], [])
        self.assertEqual(load_json(path), row)

    def test_unknown_solver_or_native_stop_blocks_before_cancelled_record(self):
        for solver, native, message in ((False, True, 'jobs are not proven stopped'),
                                        (True, False, 'preparation is not proven stopped')):
            with self.subTest(solver=solver, native=native):
                fixture, service, checkpoint, path = self.fixture()
                before = load_json(path)
                with self.ownership(solver, native), self.assertRaisesRegex(RuntimeError, message):
                    service._cleanup_checkpoint_origin(checkpoint)
                self.assertEqual(load_json(path), before)


if __name__ == '__main__':
    unittest.main()
