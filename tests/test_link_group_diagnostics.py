"""Linked diagnostic baselines and exhaustive finite spaces, without EDA."""
import copy
import itertools
import unittest
from unittest import mock

from crescendo_engine.diagnostic_plan import freeze_plan, validate_plan
from crescendo_engine.environment_test import _freeze_diagnostic_candidate
from crescendo_engine.optimizers import PopulationGenerator, validate_candidate_values


class LinkedDiagnosticBaselineTests(unittest.TestCase):
    def variables(self):
        return [dict(parameter_id='a', minimum=1, maximum=5, step=1, link_group='pair'),
                dict(parameter_id='b', minimum=10, maximum=50, step=10, link_group='pair')]

    def freeze(self, variables, values):
        return _freeze_diagnostic_candidate(
            {'variables': variables}, {'parameters': [dict(parameter_id=key, numeric_value=value)
                                                      for key, value in values.items()]})

    def assert_seeded_group(self, variables, candidate, origin):
        sampled = PopulationGenerator(variables, 'random', 1,
                                      parameters={'population': 1, 'seed': 1}).ask(1)[0]
        self.assertEqual(candidate, sampled)
        self.assertEqual(candidate, validate_candidate_values(candidate, variables))
        for key in candidate:
            self.assertEqual(origin[key]['source'], 'seed1_sample')
            self.assertIn("link_group 'pair'", origin[key]['reason'])
        plan, _ = freeze_plan({'variables': variables}, candidate)
        self.assertEqual(plan[0]['values'], candidate)

    def test_individually_legal_but_inconsistent_current_grid_uses_one_seeded_group(self):
        variables = self.variables()
        candidate, origin = self.freeze(variables, {'a': 2, 'b': 40})
        self.assert_seeded_group(variables, candidate, origin)
        self.assertEqual(candidate['b'], candidate['a'] * 10)
        self.assertIn('member b', origin['a']['reason'])

    def test_missing_or_invalid_member_resamples_entire_group(self):
        variables = self.variables()
        for values in ({'a': 3}, {'b': 30}, {}, {'a': 3, 'b': None},
                       {'a': 3, 'b': 35}, {'a': 3, 'b': 60}, {'a': 3, 'b': float('nan')}):
            with self.subTest(values=values):
                candidate, origin = self.freeze(variables, values)
                self.assert_seeded_group(variables, candidate, origin)

    def test_consistent_current_values_retained_for_linear_log_integer_and_mixed_groups(self):
        cases = [
            (self.variables(), {'a': 3, 'b': 30}),
            ([dict(parameter_id='a', minimum=1, maximum=5, link_group='pair'),
              dict(parameter_id='b', minimum=10, maximum=50, link_group='pair')], {'a': 2, 'b': 20}),
            ([dict(parameter_id='a', minimum=1, maximum=100, scale='log', link_group='pair'),
              dict(parameter_id='b', minimum=10, maximum=1000, scale='log', link_group='pair')], {'a': 10, 'b': 100}),
            ([dict(parameter_id='a', minimum=1, maximum=5, value_type='integer', link_group='pair'),
              dict(parameter_id='b', minimum=10, maximum=50, step=10, value_type='integer', link_group='pair')], {'a': 3, 'b': 30}),
            ([dict(parameter_id='a', minimum=1, maximum=5, link_group='pair'),
              dict(parameter_id='b', minimum=10, maximum=50, step=10, link_group='pair')], {'a': 2.1, 'b': 20}),
        ]
        for variables, values in cases:
            with self.subTest(variables=variables):
                candidate, origin = self.freeze(variables, values)
                self.assertEqual(candidate, values)
                self.assertTrue(all(row['source'] == 'design_current_value' for row in origin.values()))
                self.assertEqual(freeze_plan({'variables': variables}, candidate)[0][0]['values'], values)

    def test_inconsistent_continuous_log_and_mixed_groups_are_legal_after_fallback(self):
        for scale in ('linear', 'log', 'mixed'):
            variables = [dict(parameter_id='a', minimum=1, maximum=100, link_group='pair'),
                         dict(parameter_id='b', minimum=10, maximum=1000, link_group='pair')]
            if scale == 'log':
                for row in variables:
                    row['scale'] = 'log'
            elif scale == 'mixed':
                variables[1]['step'] = 10
            with self.subTest(scale=scale):
                candidate, origin = self.freeze(variables, {'a': 20, 'b': 800})
                self.assert_seeded_group(variables, candidate, origin)

    def test_fallback_is_scoped_to_one_group_and_does_not_mutate_inputs(self):
        variables = self.variables() + [
            dict(parameter_id='c', minimum=0, maximum=1, link_group='other'),
            dict(parameter_id='d', minimum=10, maximum=20, link_group='other'),
            dict(parameter_id='e', value_type='enum', choices=['low', 'high']),
            dict(parameter_id='f', minimum=0, maximum=1)]
        values = {'a': 2, 'b': 40, 'c': .5, 'd': 15, 'e': 'high', 'f': .7}
        before = copy.deepcopy((variables, values))
        candidate, origin = self.freeze(variables, values)
        self.assertEqual((variables, values), before)
        self.assertEqual(candidate, validate_candidate_values(candidate, variables))
        for key in ('c', 'd', 'e', 'f'):
            self.assertEqual(candidate[key], values[key])
            self.assertEqual(origin[key]['source'], 'design_current_value')
        self.assertEqual(origin['a']['source'], 'seed1_sample')
        self.assertEqual(origin['b']['source'], 'seed1_sample')

    def test_unlinked_current_values_and_provenance_are_unchanged(self):
        variables = self.variables()
        for row in variables:
            row.pop('link_group')
        candidate, origin = self.freeze(variables, {'a': 2, 'b': 40})
        self.assertEqual(candidate, {'a': 2, 'b': 40})
        expected = {'source': 'design_current_value',
                    'reason': 'current value validated against selected range and declared grid/choices'}
        self.assertEqual(origin, {'a': expected, 'b': expected})
        candidate, origin = self.freeze(variables, {'a': 2})
        sampled = PopulationGenerator(variables, 'random', 1, parameters={'population': 1, 'seed': 1}).ask(1)[0]
        self.assertEqual(candidate, {'a': 2, 'b': sampled['b']})
        self.assertEqual(origin['a'], expected)
        self.assertEqual(origin['b'], {'source': 'seed1_sample', 'reason': 'design current value unavailable'})


class LinkedDiagnosticEnumerationTests(unittest.TestCase):
    def test_linked_grid_and_independent_enums_exhaust_legal_space_before_repeating(self):
        for counts in ((2, 3, 5), (5, 3, 2), (2, 4, 4)):
            variables = [dict(parameter_id='a', minimum=0, maximum=1, step=1, link_group='pair'),
                         dict(parameter_id='b', minimum=10, maximum=20, step=10, link_group='pair')]
            choices = [list('choice%d' % k for k in range(count)) for count in counts]
            for index, axis in enumerate(choices):
                variables.append(dict(parameter_id='e%d' % index, value_type='enum', choices=axis))
            baseline = dict(a=0, b=10, **{'e%d' % i: axis[0] for i, axis in enumerate(choices)})
            spec = dict(variables=variables, diagnostic_generations=4, diagnostic_samples=16)
            expected = set(itertools.product((0, 1), *choices))
            for force_enumeration in (False, True):
                with self.subTest(counts=counts, force_enumeration=force_enumeration):
                    def build():
                        return freeze_plan(spec, baseline)
                    if force_enumeration:
                        # Prove the enumerator alone covers the space; random
                        # samples must not be needed to hide missing points.
                        with mock.patch.object(PopulationGenerator, 'ask', return_value=[baseline]):
                            plan, shape = build()
                            self.assertEqual((plan, shape), build())
                    else:
                        plan, shape = build()
                        self.assertEqual((plan, shape), build())
                    observed = set(tuple(row['values'][key] for key in ('a', 'e0', 'e1', 'e2')) for row in plan)
                    self.assertEqual(observed, expected)
                    self.assertEqual(shape['distinct_candidates'], len(expected))
                    self.assertEqual(shape['repeated_candidates'], 64 - len(expected))
                    self.assertEqual(len(set(tuple(row['values'][key] for key in ('a', 'e0', 'e1', 'e2'))
                                             for row in plan[:len(expected)])), len(expected))
                    self.assertEqual(plan, validate_plan(plan, variables, 4, 16))
                    for row in plan:
                        self.assertEqual(row['values']['b'], 10 + 10 * row['values']['a'])

    def test_linked_and_unlinked_equivalent_spaces_have_identical_coverage(self):
        variables = [dict(parameter_id='a', minimum=0, maximum=1, value_type='integer', link_group='pair'),
                     dict(parameter_id='b', minimum=10, maximum=20, step=10, link_group='pair'),
                     dict(parameter_id='e', value_type='enum', choices=[1, 2, 3, 4, 5])]
        spec = dict(variables=variables, diagnostic_generations=2, diagnostic_samples=8)
        plan, shape = freeze_plan(spec, dict(a=0, b=10, e=1))
        control = copy.deepcopy(spec)
        control['variables'].pop(1)
        control['variables'][0].pop('link_group')
        unlinked, control_shape = freeze_plan(control, dict(a=0, e=1))
        self.assertEqual(shape['distinct_candidates'], 10)
        self.assertEqual(shape['distinct_candidates'], control_shape['distinct_candidates'])
        self.assertEqual({(row['values']['a'], row['values']['e']) for row in plan},
                         {(row['values']['a'], row['values']['e']) for row in unlinked})
