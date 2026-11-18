"""Linked search coordinates, validation, and persisted optimizer state."""
import copy
import csv
import io
import json
import math
import os
import tempfile
import unittest

from crescendo_engine.optimizers import (ALGORITHMS, PopulationGenerator,
                                         validate_candidate_values, validate_variables)


def linked_variables():
    return [
        {"parameter_id": "a", "minimum": -2, "maximum": 8, "link_group": "linear"},
        {"parameter_id": "b", "minimum": 100, "maximum": 200, "link_group": "linear"},
        {"parameter_id": "l", "minimum": 1, "maximum": 100, "scale": "log", "link_group": "log"},
        {"parameter_id": "m", "minimum": 10, "maximum": 100000, "scale": "log", "link_group": "log"},
        {"parameter_id": "n", "minimum": 1, "maximum": 5, "value_type": "integer", "link_group": "grid"},
        {"parameter_id": "w", "minimum": 0.1, "maximum": 0.5, "step": 0.1, "grid_origin": 0.1, "link_group": "grid"},
        {"parameter_id": "free", "minimum": 0, "maximum": 1},
    ]


def evaluate(values, generation):
    rows = []
    for index, value in enumerate(values):
        score = value['a'] ** 2 + value['l'] + value['n'] + value['free']
        rows.append({'candidate_id': 'g%03d_c%04d' % (generation, index),
                     'generation': generation, 'index': index,
                     'status': 'completed', 'values': value,
                     'metrics': {'merit': score, 'total_violation': 0.0,
                                 'objectives': [{'score': score}]}})
    return rows


class LinkGroupTests(unittest.TestCase):
    def assert_linked(self, value):
        self.assertEqual(set(value), {'a', 'b', 'l', 'm', 'n', 'w', 'free'})
        self.assertAlmostEqual((value['a'] + 2) / 10, (value['b'] - 100) / 100, places=12)
        self.assertAlmostEqual(math.log(value['l']) / math.log(100),
                               math.log(value['m'] / 10) / math.log(10000), places=12)
        self.assertIsInstance(value['n'], int)
        self.assertAlmostEqual(value['n'] - 1, (value['w'] - 0.1) / 0.1, places=12)
        validate_candidate_values(value, linked_variables())

    def test_all_algorithms_use_one_coordinate_per_group_through_ask_tell(self):
        for algorithm in ALGORITHMS:
            full = PopulationGenerator(linked_variables(), algorithm, seed=19)
            reduced = PopulationGenerator([dict(row, link_group='') for row in linked_variables()
                                           if row['parameter_id'] in ('a', 'l', 'n', 'free')], algorithm, seed=19)
            self.assertEqual(len(full._dimensions), 4)
            for generation in range(4):
                values = full.ask(8)
                reduced_values = reduced.ask(8)
                for value, small in zip(values, reduced_values):
                    self.assert_linked(value)
                    self.assertEqual({key: value[key] for key in small}, small)
                full.tell(evaluate(values, generation))
                reduced.tell(evaluate(reduced_values, generation))
            self.assertFalse(full.unmatched_trials)

    def test_mixed_groups_share_u_with_continuous_anchor_and_equal_discrete_indices(self):
        variables = [
            {'parameter_id': 'n', 'minimum': 1, 'maximum': 5, 'value_type': 'integer', 'link_group': 'mixed'},
            {'parameter_id': 'a', 'minimum': -2, 'maximum': 8, 'link_group': 'mixed'},
            {'parameter_id': 'w', 'minimum': 10, 'maximum': 50, 'step': 10, 'link_group': 'mixed'},
        ]
        for algorithm in ALGORITHMS:
            generator = PopulationGenerator(variables, algorithm, seed=13)
            independent = PopulationGenerator([dict(variables[1], link_group='')], algorithm, seed=13)
            for generation in range(4):
                values, small = generator.ask(8), independent.ask(8)
                rows, reduced_rows = [], []
                for index, (value, reference) in enumerate(zip(values, small)):
                    self.assertEqual(value['a'], reference['a'])
                    unit = (value['a'] + 2) / 10
                    k = int(round(unit * 4))
                    self.assertEqual(value['n'], 1 + k)
                    self.assertEqual(value['w'], 10 + 10 * k)
                    validate_candidate_values(value, variables)
                    score = value['a'] ** 2
                    metrics = {'total_violation': 0.0, 'merit': score, 'objectives': [{'score': score}]}
                    row = {'candidate_id': '%s-%s' % (generation, index), 'status': 'completed', 'values': value, 'metrics': metrics}
                    rows.append(row)
                    reduced_rows.append(dict(row, values=reference))
                generator.tell(rows); independent.tell(reduced_rows)
            invalid = dict(values[0], n=5 if values[0]['n'] != 5 else 1)
            with self.assertRaisesRegex(ValueError, "link_group 'mixed' member n"):
                validate_candidate_values(invalid, variables)

    def test_mixed_half_grid_roundoff_still_validates_asked_values(self):
        variables = [
            {'parameter_id': 'a', 'minimum': 0.1, 'maximum': 0.7, 'link_group': 'mixed'},
            {'parameter_id': 'n', 'minimum': 0, 'maximum': 4, 'value_type': 'integer', 'link_group': 'mixed'},
            {'parameter_id': 'm', 'minimum': 10, 'maximum': 50, 'step': 10, 'link_group': 'mixed'},
        ]
        generator = PopulationGenerator(variables)
        for unit in (0.125, 0.375, 0.625, 0.875):
            value = generator._decode_vector([unit])
            validate_candidate_values(value, variables)
            self.assertEqual(value['n'], round(unit * 4))
            invalid = dict(value, m=10 if value['m'] != 10 else 20)
            with self.assertRaisesRegex(ValueError, "link_group 'mixed' member m"):
                validate_candidate_values(invalid, variables)

    def test_mapping_uses_identical_grid_index_even_at_halfway_float_ties(self):
        generator = PopulationGenerator(linked_variables())
        for unit in (0, 0.125, 0.375, 0.625, 0.875, 1):
            value = generator._decode_vector([unit] * 4)
            self.assert_linked(value)
            index = int(round(unit * 4))
            self.assertEqual(value['n'], 1 + index)
            self.assertEqual(value['w'], 0.1 + index * 0.1)

    def test_seed_reproducibility_and_renaming_group_does_not_change_mapping(self):
        for algorithm in ALGORITHMS:
            a = PopulationGenerator(linked_variables(), algorithm, seed=91)
            variables = linked_variables()
            for row in variables:
                if row.get('link_group'):
                    row['link_group'] += '-renamed'
            b = PopulationGenerator(variables, algorithm, seed=91)
            for generation in range(3):
                left, right = a.ask(7), b.ask(7)
                self.assertEqual(json.dumps(left), json.dumps(right))
                a.tell(evaluate(left, generation)); b.tell(evaluate(right, generation))

    def test_group_validation_errors_name_group_and_member(self):
        base = [{'parameter_id': 'a', 'minimum': 1, 'maximum': 5, 'link_group': 'pair'},
                {'parameter_id': 'b', 'minimum': 10, 'maximum': 50, 'link_group': 'pair'}]
        for patch, message in (({'scale': 'log'}, 'scale must match'),
                               ({'value_type': 'enum', 'choices': ['x', 'y']}, 'enum linking is not supported'),
                               ({'scale': 'unknown'}, 'unsupported scale')):
            variables = copy.deepcopy(base)
            variables[1].update(patch)
            with self.assertRaisesRegex(ValueError, "link_group 'pair' member b: " + message):
                validate_variables(variables)
        variables = copy.deepcopy(base)
        variables[0]['step'] = 1
        variables[1]['step'] = 20
        with self.assertRaisesRegex(ValueError, "link_group 'pair' member b: step_count"):
            validate_variables(variables)
        for invalid in (None, 1, [], {}, '  '):
            with self.assertRaisesRegex(ValueError, 'link_group.*member a'):
                validate_variables([dict(base[0], link_group=invalid)])
        with self.assertRaisesRegex(ValueError, "link_group 'pair' member a: log scale requires positive bounds"):
            validate_variables([dict(base[0], minimum=0, scale='log')])

    def test_validation_is_idempotent_and_does_not_mutate_input(self):
        raw = linked_variables()
        before = copy.deepcopy(raw)
        normalized = validate_variables(raw)
        self.assertEqual(raw, before)
        self.assertEqual(validate_variables(normalized), normalized)
        value = PopulationGenerator(raw).ask(1)[0]
        value['b'] = 100 if value['a'] != -2 else 200
        with self.assertRaisesRegex(ValueError, "link_group 'linear' member b: candidate coordinate"):
            validate_candidate_values(value, raw)

    def test_checkpoint_restores_rng_selection_and_de_targets_for_every_algorithm(self):
        from crescendo_engine.runner import (_checkpoint_document, _restore_generator, checkpoint_candidates,
                                            checkpoint_continuation_schema, validate_checkpoint_integrity,
                                            _checkpoint_checksum)
        for algorithm in ALGORITHMS:
            variables = validate_variables(linked_variables())
            generator = PopulationGenerator(variables, algorithm, seed=7)
            rows = evaluate(generator.ask(8), 0)
            generator.tell(rows)
            # Persist a pending DE trial map as well as RNG/selection state.
            pending = generator.ask(8)
            spec = {'variables': variables, 'evaluator': 'mock', 'algorithm': algorithm,
                    'objectives': [], 'constraints': [], 'population': 8}
            checkpoint = _checkpoint_document(spec, {}, generator, rows, [], 1, None, {})
            self.assertEqual(checkpoint['schema_version'], 'crescendo.checkpoint.v2')
            restored_document = json.loads(json.dumps(checkpoint))
            restored_rows = validate_checkpoint_integrity(restored_document)
            restored = PopulationGenerator(variables, algorithm, seed=0)
            _restore_generator(restored, restored_document['state']['generator'], restored_rows)
            generator.tell(evaluate(pending, 1)); restored.tell(evaluate(pending, 1))
            self.assertEqual(json.dumps(generator.ask(8)), json.dumps(restored.ask(8)))
            changed = copy.deepcopy(checkpoint)
            changed['spec']['variables'][0]['link_group'] = 'changed'
            self.assertNotEqual(checkpoint_continuation_schema(changed), checkpoint_continuation_schema(checkpoint))
            changed['schema_version'] = 'crescendo.checkpoint.v1'
            changed['payload_sha256'] = _checkpoint_checksum(changed)
            with self.assertRaisesRegex(ValueError, 'link_group checkpoint requires'):
                validate_checkpoint_integrity(changed)
            self.assertEqual(len(checkpoint_candidates(checkpoint)), 8)

    def test_legacy_checkpoint_schema_remains_v1(self):
        from crescendo_engine.runner import _checkpoint_document, validate_checkpoint_integrity
        variables = [{'parameter_id': 'x', 'minimum': 0, 'maximum': 1}]
        generator = PopulationGenerator(variables)
        spec = {'variables': variables, 'evaluator': 'mock'}
        document = _checkpoint_document(spec, {}, generator, [], [], 0, None, {})
        self.assertEqual(document['schema_version'], 'crescendo.checkpoint.v1')
        self.assertEqual(validate_checkpoint_integrity(document), [])

    def test_csv_retains_group_for_each_member_and_legacy_bytes(self):
        from crescendo_engine.exports import candidates_csv_text, export_candidates_csv
        values = PopulationGenerator(linked_variables()).ask(3)
        rows = evaluate(values, 0)
        original = candidates_csv_text(rows)
        self.assertEqual(original, candidates_csv_text(rows, variables=[]))
        linked = candidates_csv_text(rows, variables=linked_variables())
        reader = list(csv.DictReader(io.StringIO(linked)))
        for row in reader:
            for member, group in [('a', 'linear'), ('b', 'linear'), ('l', 'log'), ('m', 'log'), ('n', 'grid'), ('w', 'grid')]:
                self.assertEqual(row['link_group:' + member], group)
                self.assertIn('variable:' + member, row)
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, 'candidates.csv')
            export_candidates_csv(rows, path, variables=linked_variables())
            with open(path) as stream:
                self.assertEqual(stream.read(), linked)
