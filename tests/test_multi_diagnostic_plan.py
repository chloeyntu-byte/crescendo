import copy
import tempfile
import unittest
from unittest import mock

from crescendo_engine import runner
from crescendo_engine.diagnostic_plan import freeze_plan, diagnostic_shape, validate_plan, digest
from crescendo_engine.environment_test import _engine_deadline_budget
from crescendo_engine.metrics import evaluate_metrics
from tests.test_environment_test_runner import EnvironmentTestRunnerTests


class MultiDiagnosticPlanTests(unittest.TestCase):
    _spec = EnvironmentTestRunnerTests._spec
    _run_with_fake_execution = EnvironmentTestRunnerTests._run_with_fake_execution

    def plan(self):
        spec = self._spec()
        plan, shape = freeze_plan(spec, {'x': .5})
        spec.update(generations=shape['generations'], population=shape['samples_per_generation'])
        spec['algorithm_parameters']['population'] = shape['samples_per_generation']
        spec['environment_test'].update(frozen_candidate={'x': .5}, frozen_plan=plan,
                                        frozen_plan_sha256=shape['plan_sha256'])
        return spec, plan, shape

    def test_deterministic_six_distinct_candidates_current_first_and_two_generations(self):
        spec, plan, shape = self.plan()
        self.assertEqual(shape['distinct_candidates'], 6)
        self.assertEqual(plan[0]['values'], {'x': .5})
        self.assertEqual([(r['generation'], r['index']) for r in plan],
                         [(g, i) for g in range(2) for i in range(3)])
        self.assertEqual(plan, freeze_plan(spec, {'x': .5})[0])
        self.assertEqual(plan, validate_plan(plan, spec['variables'], 2, 3))

    def test_small_grid_reports_real_distinct_count_and_repeats_only_when_exhausted(self):
        spec = {'variables': [{'parameter_id': 'x', 'minimum': 0, 'maximum': 1, 'step': 1}]}
        plan, shape = freeze_plan(spec, {'x': 0})
        self.assertEqual(shape['distinct_candidates'], 2)
        self.assertEqual(shape['repeated_candidates'], 4)
        self.assertEqual(len(plan), 6)
        self.assertEqual({r['values']['x'] for r in plan[:2]}, {0, 1})

    def test_shape_rejects_booleans_unbounded_and_nonpositive_values(self):
        for spec in ({'diagnostic_generations': True}, {'diagnostic_samples': 0},
                     {'diagnostic_generations': 20}, {'diagnostic_generations': 10, 'diagnostic_samples': 16}):
            with self.subTest(spec=spec), self.assertRaises(ValueError):
                diagnostic_shape(spec)
        self.assertEqual(diagnostic_shape({'diagnostic_generations': 1, 'diagnostic_samples': 1})['total_candidates'], 1)

    def test_explicit_total_keeps_exact_shape_and_legacy_input(self):
        shape = diagnostic_shape({'diagnostic_generations': 3, 'diagnostic_total_candidates': 12})
        self.assertEqual(shape, {'generations': 3, 'samples_per_generation': 4, 'total_candidates': 12})
        self.assertEqual(shape, diagnostic_shape({'diagnostic_generations': 3, 'diagnostic_samples': 4}))
        for total in (True, 0, 65, 7, 12.0):
            with self.subTest(total=total), self.assertRaises(ValueError):
                diagnostic_shape({'diagnostic_generations': 3, 'diagnostic_total_candidates': total})
        with self.assertRaises(ValueError):
            diagnostic_shape({'diagnostic_generations': 3, 'diagnostic_total_candidates': 12,
                              'diagnostic_samples': 3})

    def test_plan_order_identity_grid_and_digest_are_validated_before_execution(self):
        spec, plan, _ = self.plan()
        for alteration in ('order', 'missing', 'invalid', 'digest', 'baseline'):
            altered = copy.deepcopy(spec)
            marker = altered['environment_test']
            if alteration == 'order':
                marker['frozen_plan'][1]['index'] = 0
            elif alteration == 'missing':
                marker['frozen_plan'].pop()
            elif alteration == 'invalid':
                marker['frozen_plan'][2]['values']['x'] = 2
            elif alteration == 'baseline':
                marker['frozen_candidate']['x'] = .2
            else:
                marker['frozen_plan_sha256'] = '0' * 64
            with self.subTest(alteration=alteration), self.assertRaises(ValueError):
                runner._frozen_environment_plan(altered, spec['variables'])

    def test_runner_executes_whole_frozen_plan_without_resampling_and_retains_actual_data(self):
        spec, plan, _ = self.plan()
        seen = []
        def completed(_root, index, generation, values, active_spec, _prepared, _selected, _config):
            seen.append({'generation': generation, 'index': index, 'values': copy.deepcopy(values)})
            parsed = {'output_map': {'main::gain': {'summary': {'mean': values['x'] * 4}},
                                    'main::power': {'summary': {'mean': values['x'] + 1}}}}
            return {'candidate_id': 'g%03d_c%04d' % (generation, index), 'generation': generation,
                    'index': index, 'values': values, 'status': 'completed',
                    'metrics': evaluate_metrics(parsed, active_spec['objectives'], active_spec['constraints'])}
        with tempfile.TemporaryDirectory() as root, mock.patch.object(runner.PopulationGenerator, 'ask', side_effect=AssertionError('resampled')):
            result = self._run_with_fake_execution(root, spec, completed)
        self.assertEqual(seen, plan)
        self.assertEqual(len(result['candidates']), 6)
        self.assertEqual({row['generation'] for row in result['candidates']}, {0, 1})
        self.assertTrue(all(len(row['metrics']['objectives']) == 2 for row in result['candidates']))
        self.assertTrue(result['representative'])
        self.assertTrue(result['pareto_front'])

    def test_deadline_scales_total_plan_without_changing_candidate_budget(self):
        spec = {'candidate_timeout_seconds': 60, 'generations': 2, 'population': 3}
        self.assertEqual(_engine_deadline_budget({}, {}, spec), 540)
        self.assertEqual(_engine_deadline_budget({'engine_timeout_seconds': 77}, {}, spec), 77)
        self.assertEqual(_engine_deadline_budget({}, {'environment_test_engine_timeout_seconds': 88}, spec), 88)
        self.assertEqual(spec['candidate_timeout_seconds'], 60)
