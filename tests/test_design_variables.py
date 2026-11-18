import copy
import os
import tempfile
import unittest

from crescendo_engine.design_variables import (catalog, priority_report, read_design_points,
    require_no_corner_conflicts)
from crescendo_engine.native_maestro_parameters import build_native_parameter_plan
from crescendo_engine.generation_batch import build_generation_plan, build_native_columns, native_batch_skill_lines
from crescendo_engine.private_netlist import qualify_symbolic_netlist, PrivateNetlistUnsupported


class DesignVariablesTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.test = dict(name='dc', library='lib', cell='tb', view='schematic', variables=[dict(name='bias', value='2')])
        self.setup = dict(tests=[self.test], global_variables=[dict(name='bias', value='3')], corners=[dict(name='cold'), dict(name='hot')])
        self.control = catalog(self.setup, {})[0]
        self.device = dict(parameter_id='dev', library='lib', cell='tb', view='schematic', instance='R1', property='r', hierarchy_path='/R1')

    def task(self, text):
        path = os.path.join(self.temp.name, 'input.scs')
        with open(path, 'w') as stream:
            stream.write(text)
        return dict(test='dc', point_id=1, corner='cold', netlist_path=path)

    def text(self, declarations='parameters bias=3 alias=bias*1k', device='R1 (a 0) resistor r=alias'):
        return '// Library name: lib\n// Cell name: tb\n// View name: schematic\n'+declarations+'\n'+device+'\n'

    def test_global_precedes_test_and_preserves_source(self):
        self.assertEqual(self.control['source_kind'], 'global')
        self.assertEqual(self.control['source'], 'setup global')
        self.assertEqual(self.control['numeric_value'], 3)
        self.setup['global_variables'][0]['sweep_enabled'] = False
        row = catalog(self.setup, {})[0]
        self.assertEqual(row['source_kind'], 'top')
        self.assertEqual(row['source'], 'test')
        self.assertEqual(row['numeric_value'], 2)

    def test_single_test_projection_retains_implicit_global_participation(self):
        from crescendo_engine.single_test import project_setup
        setup = copy.deepcopy(self.setup)
        setup['global_variables'][0]['dependent_tests'] = None
        projected = project_setup(setup, 'dc')
        self.assertIsNone(projected['global_variables'][0]['dependent_tests'])
        self.assertEqual(catalog(projected, {})[0]['source_kind'], 'global')
        setup['global_variables'][0]['dependent_tests'] = []
        projected = project_setup(setup, 'dc')
        self.assertEqual(catalog(projected, {})[0]['source'], 'test')

    def test_saved_numeric_formula_is_selectable_without_claiming_native_seed(self):
        setup = dict(tests=[dict(self.test, variables=[dict(name='supply', value='2'), dict(name='bias', value='supply/2')])])
        rows = {r['name']:r for r in catalog(setup, {})}
        self.assertTrue(rows['bias']['optimizable'])
        self.assertTrue(rows['bias']['requires_point_numeric_value'])
        self.assertIsNone(rows['bias']['numeric_value'])
        self.assertEqual(rows['bias']['current_value'], 'supply/2')
        setup['tests'][0]['variables'][1]['value'] = 'unknown_function(supply)'
        self.assertFalse({r['name']:r for r in catalog(setup, {})}['bias']['optimizable'])

    def test_top_cell_and_test_precedence(self):
        hierarchy = dict(designs=[dict(design=['lib', 'tb', 'schematic'], hierarchy=dict(top_parameters=[dict(name='bias', value='1'), dict(name='extra', value='4')]))])
        rows = catalog(self.setup, hierarchy)
        self.assertEqual([(r['name'], r['source']) for r in rows], [('bias', 'setup global'), ('extra', 'top cell')])
        self.assertTrue(all(r['optimizable'] for r in rows))

    def test_corner_collision_names_both_identities_and_disabled_corner_allowed(self):
        self.setup['corners'][1]['variables'] = [dict(name='bias', value='8')]
        with self.assertRaisesRegex(ValueError, 'corner hot defines variable bias'):
            require_no_corner_conflicts(self.setup, [self.control])
        self.setup['corners'][1]['enabled'] = False
        require_no_corner_conflicts(self.setup, [self.control])

    def test_existing_noncorner_multiplicity_rule_still_blocks(self):
        self.setup['global_variables'][0]['value'] = '1 2'
        with self.assertRaisesRegex(ValueError, 'one value'):
            require_no_corner_conflicts(self.setup, [self.control])

    def test_scope_alias_transitive_dependency_locks_device(self):
        task = self.task(self.text())
        report = priority_report([self.control], [self.device], [task])
        self.assertEqual(report['locked_parameter_ids'], ['dev'])
        self.assertIn('bias -> /R1/r', report['warnings'][0])
        self.assertEqual(report['evidence'][0]['lexical_binding'], [[], 'bias'])

    def test_numeric_callback_export_retains_original_expression_dependency(self):
        task = self.task(self.text(device='R1 (a 0) resistor r=3000'))
        row = dict(self.device, baseline_expression='bias*1k')
        self.assertEqual(priority_report([self.control], [row], [task])['locked_parameter_ids'], ['dev'])
        row['baseline_expression'] = 'unknown_function(bias)'
        with self.assertRaisesRegex(ValueError, 'original device expression'):
            priority_report([self.control], [row], [task])

    def test_same_name_local_shadow_does_not_lock_device(self):
        task = self.task(self.text('parameters bias=3\nsubckt child (a)\nparameters bias=2 alias=bias*1k', 'R1 (a 0) resistor r=alias\nends child'))
        self.assertEqual(priority_report([self.control], [self.device], [task])['locked_parameter_ids'], [])

    def test_unknown_alias_or_instance_override_fails_closed(self):
        task = self.task(self.text(device='R1 (a 0) resistor unknown=alias'))
        with self.assertRaisesRegex(ValueError, 'alias'):
            priority_report([self.control], [self.device], [task])
        task = self.task(self.text('parameters bias=3\nsubckt child (a)\nparameters alias=bias*1k', 'R1 (a 0) resistor r=alias\nends child\nX1 (out) child alias=500'))
        with self.assertRaisesRegex(ValueError, 'shadows'):
            priority_report([self.control], [self.device], [task])

    def test_design_readback_is_top_only_and_duplicate_definition_rejected(self):
        row = read_design_points(self.control, [self.task(self.text())])
        self.assertEqual(row['points'][0]['effective'], 3)
        with self.assertRaises(ValueError):
            read_design_points(self.control, [self.task(self.text('parameters bias=3\nparameters bias=4'))])

    def test_plan_v2_and_mixed_batch_apis_are_explicit(self):
        row = dict(self.control, value='2')
        dev = dict(self.device, value='1k')
        plan = build_native_parameter_plan([row, dev], self.setup['corners'])
        self.assertEqual(plan['schema_version'], 'crescendo.native_parameter_plan.v2')
        batch = build_generation_plan(0, [(0, {row['parameter_id']:1, 'dev':1000}), (1, {row['parameter_id']:2, 'dev':2000})], [row, dev], 'epoch')
        build_native_columns(batch, plan['selected'])
        text = '\n'.join(native_batch_skill_lines(batch, plan, os.path.join(self.temp.name, 'audit')))
        self.assertIn('ocnxlSweepVar', text)
        self.assertIn('ocnxlSweepParam', text)
        self.assertIn('ocnxlParametricSet', text)
        self.assertEqual(batch['design_variables_protocol'], 'crescendo.design_variables.v1')

    def test_previous_reference_normalization_restores_only_proven_values(self):
        from crescendo_engine.design_variables import normalize_owned_globals
        def variable(value, enabled=True):
            return dict(name='bias', value=value, sweep_enabled=enabled,
                native_content=dict(tag='var', text='bias', attributes={}, children=[dict(tag='value', text=value, attributes={}, children=[])]))
        source = dict(global_variables=[variable('1')])
        private = dict(global_variables=[variable('2')])
        owned = [dict(name='bias', value='2', source_kind='global', variable_test='dc')]
        normalized, proofs = normalize_owned_globals(source, private, owned)
        self.assertEqual(normalized, source)
        self.assertEqual(private['global_variables'][0]['value'], '2')
        self.assertTrue(proofs)
        private['global_variables'][0]['sweep_enabled'] = False
        self.assertNotEqual(normalize_owned_globals(source, private, owned)[0], source)
        private['global_variables'][0]['value'] = '99'
        with self.assertRaisesRegex(ValueError, 'value changed'):
            normalize_owned_globals(source, private, owned)

    def test_single_global_axis_has_audited_constant_set_companion(self):
        row = dict(self.control, value='2')
        plan = build_native_parameter_plan([row], self.setup['corners'])
        batch = build_generation_plan(0, [(0, {row['parameter_id']:1}), (1, {row['parameter_id']:3})], [row], 'epoch')
        columns = build_native_columns(batch, plan['selected'])
        self.assertEqual(len(columns), 2)
        self.assertEqual(columns[1]['values'], ['0', '0'])
        self.assertTrue(columns[1]['auxiliary'])
        text = '\n'.join(native_batch_skill_lines(batch, plan, os.path.join(self.temp.name, 'audit')))
        self.assertIn('"variablesets"', text)
        self.assertIn('?name "var"', text)
        self.assertNotIn('crMembersRoot=', text)
        self.assertIn('crOwnedCheck!=0', text)

    def test_candidate_expectations_do_not_change_source_or_other_controls(self):
        from crescendo_engine.design_variables import candidate_setup
        before = copy.deepcopy(self.setup)
        effective = candidate_setup(self.setup, [self.control], {self.control['parameter_id']:7})
        self.assertEqual(effective['global_variables'][0]['value'], '7')
        self.assertEqual(self.setup, before)
        self.assertEqual(effective['corners'], before['corners'])

    def test_gui_variable_and_settings_evidence_retain_source_identity(self):
        from crescendo_engine.gui_state import initial_parameter_config, validate_variable_configs
        from crescendo_engine.settings_state import _parameter_evidence
        config = initial_parameter_config(self.control)
        config.update(selected=True, minimum='1', maximum='3', unit='')
        variable = validate_variable_configs([self.control], {self.control['parameter_id']:config})['variables'][0]
        self.assertEqual(variable['source_kind'], 'global')
        self.assertEqual(variable['variable_source'], 'setup global')
        evidence = _parameter_evidence(self.control)
        self.assertEqual(evidence['source_kind'], 'global')
        self.assertEqual(evidence['variable_test'], 'dc')

    def test_mock_checkpoint_preserves_and_checks_design_source_identity(self):
        import json
        from crescendo_engine.runner import run_optimization, validate_checkpoint
        setup = copy.deepcopy(self.setup)
        setup.update(selected_test='dc', _source_tests=copy.deepcopy(setup['tests']))
        prepared = dict(setup=setup, hierarchy=dict(designs=[]), parameters=[self.control], outputs=[dict(key='dc::out')])
        spec = dict(evaluator='mock', algorithm='random', generations=1, population=2,
                    variables=[dict(parameter_id=self.control['parameter_id'], minimum=1, maximum=3)],
                    objectives=[dict(output_key='dc::out', direction='minimize', aggregation='mean')], constraints=[])
        root = os.path.join(self.temp.name, 'run')
        result = run_optimization(spec, dict(runtime=dict(run_root=self.temp.name, max_parallel_jobs=1)), prepared=prepared, run_root=root)
        self.assertEqual(result['state'], 'completed')
        with open(os.path.join(root, 'checkpoint.json')) as stream:
            checkpoint = json.load(stream)
        self.assertEqual(checkpoint['spec']['variables'][0]['source_kind'], 'global')
        self.assertEqual(checkpoint['manifest']['design_variable_identity'][0]['source'], 'setup global')
        validate_checkpoint(checkpoint, prepared)
        changed = copy.deepcopy(prepared)
        changed['parameters'][0]['source'] = 'test'
        with self.assertRaisesRegex(ValueError, 'source or Test identity'):
            validate_checkpoint(checkpoint, changed)

    def test_old_device_fingerprints_do_not_gain_new_catalogue_axes(self):
        from crescendo_engine.runner import _prepared_schema_fingerprint_v1, _prepared_schema_fingerprint_v2
        prepared = dict(setup=self.setup, hierarchy=dict(designs=[]), parameters=[dict(parameter_id='d', optimizable=True)])
        expanded = copy.deepcopy(prepared)
        expanded['parameters'].append(self.control)
        for fingerprint in (_prepared_schema_fingerprint_v1, _prepared_schema_fingerprint_v2):
            self.assertEqual(fingerprint(prepared), fingerprint(expanded))

    def test_template_top_parameter_proof_covers_real_references_and_refuses_overrides(self):
        symbolic = self.text('parameters bias=CR_NL_bias alias=bias*1k')
        refs = [dict(name=str(v), values={'id':v}, text=self.text('parameters bias=%s alias=bias*1k' % v)) for v in (1, 2, 3)]
        self.assertTrue(qualify_symbolic_netlist(symbolic, refs, {'id':'CR_NL_bias'})['ok'])
        refs[1]['text'] = self.text('parameters bias=99 alias=bias*1k')
        with self.assertRaises(PrivateNetlistUnsupported):
            qualify_symbolic_netlist(symbolic, refs, {'id':'CR_NL_bias'})


if __name__ == '__main__':
    unittest.main()
