import copy
import tempfile
import unittest
from unittest import mock
from crescendo_engine import runner
from crescendo_engine.metrics import evaluate_metrics
from tests import test_environment_test_runner as fixture


class FrozenDiagnosticCandidateTests(unittest.TestCase):
    _spec = fixture.EnvironmentTestRunnerTests._spec
    _run_with_fake_execution = fixture.EnvironmentTestRunnerTests._run_with_fake_execution

    def test_optimization_loop_uses_frozen_values_without_sampling(self):
        spec = self._spec()
        spec['environment_test']['frozen_candidate'] = {'x': .625}
        seen = []
        def completed(_root, index, generation, values, active_spec, _prepared, _selected, _config):
            seen.append(copy.deepcopy(values))
            parsed = {'output_map': {'main::gain': {'summary': {'mean': 4.0}},
                                      'main::power': {'summary': {'mean': 1.0}}}}
            return {'candidate_id':'g000_c0000','generation':generation,'index':index,
                    'values':values,'status':'completed','metrics':evaluate_metrics(parsed,active_spec['objectives'],active_spec['constraints'])}
        with tempfile.TemporaryDirectory() as root, mock.patch.object(runner.PopulationGenerator, 'ask', side_effect=AssertionError('resampled diagnostic')):
            result = self._run_with_fake_execution(root,spec,completed)
        self.assertEqual(seen,[{'x':.625}])
        self.assertEqual(result['candidates'][0]['values'],{'x':.625})

    def test_frozen_values_still_require_exact_ids_bounds_and_grid(self):
        variables=[{'parameter_id':'x','minimum':0,'maximum':1,'step':.25}]
        for values in ({'other':.5},{'x':1.5},{'x':.3},{'x':float('nan')},{'x':True}):
            spec=self._spec();spec['environment_test']['frozen_candidate']=values
            with self.subTest(values=values),self.assertRaises(ValueError):
                runner._frozen_environment_candidate(spec,variables)
        spec=self._spec();spec['environment_test']['frozen_candidate']={'x':.75}
        self.assertEqual(runner._frozen_environment_candidate(spec,variables),{'x':.75})
        digest=spec['environment_test']['frozen_candidate_sha256']
        self.assertEqual(len(digest),64)
        spec['environment_test']['frozen_candidate']['x']=.5
        with self.assertRaisesRegex(ValueError,'digest'):
            runner._frozen_environment_candidate(spec,variables)

    def test_normal_optimization_does_not_consume_diagnostic_override(self):
        spec=self._spec();spec.pop('environment_test')
        self.assertIsNone(runner._frozen_environment_candidate(spec,spec['variables']))
