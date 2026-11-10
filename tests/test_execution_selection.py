import copy
import unittest
import xml.etree.ElementTree as ET
from crescendo_engine import test_selection as selection
from crescendo_engine.ade_setup import _merge_analyses, _test_corner_masks, test_corner_availability, apply_native_corner_schedule, _test_rows
from crescendo_engine.private_netlist import _capture_request_config, native_corner_schedule_skill


def setup():
    return {'tests': [
        {'name': 'A', 'enabled': True, 'analyses': [
            {'name': 'ac', 'enabled': True, 'fields': {'enable': '(t)', 'start': '1'}},
            {'name': 'dc', 'enabled': False, 'fields': {'enable': 'nil', 'saveOppoint': 't'}}]},
        {'name': 'B', 'enabled': False, 'analyses': [{'name': 'tran', 'enabled': True, 'fields': {'stop': '10n'}}]}],
        'corners': [{'name': 'TT', 'enabled': True}, {'name': 'SS', 'enabled': False}]}


class ExecutionSelectionTests(unittest.TestCase):
    def test_disabled_test_and_disabled_analysis_are_selectable_without_source_mutation(self):
        source = setup(); before = copy.deepcopy(source)
        plan = selection.selection_plan(source, {'execution_selection': {'tests': ['B', 'A'], 'analyses': {'A': ['dc']}}})
        result = selection.effective_setup(source, plan)
        self.assertEqual(plan['selected'], ['A', 'B'])
        self.assertEqual([row['enabled'] for row in result['tests']], [True, True])
        self.assertEqual([row['enabled'] for row in result['tests'][0]['analyses']], [False, True])
        self.assertEqual(result['tests'][0]['analyses'][1]['fields'], source['tests'][0]['analyses'][1]['fields'])
        self.assertEqual(source, before)
        self.assertIsNone(selection.selection_plan(source, {}))

    def test_explicit_empty_unknown_or_metric_wrong_test_fails_before_native_work(self):
        for raw in [{'tests': []}, {'tests': ['missing']}, {'tests': ['A'], 'analyses': {'A': []}},
                    {'tests': ['A'], 'analyses': {'A': ['missing']}}, {'tests': ['A', 'A']},
                    {'tests': ['A'], 'analyses': {'unknown': ['ac']}}]:
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                selection.selection_plan(setup(), {'execution_selection': raw})
        with self.assertRaisesRegex(ValueError, 'unselected Test'):
            selection.selection_plan(setup(), {'execution_selection': {'tests': ['B']},
                'objectives': [{'output_key': 'A::gain'}]})

    def test_normalize_only_proved_bits_not_parameter_or_analysis_options(self):
        source = setup()
        plan = selection.selection_plan(source, {'execution_selection': {'tests': ['B'], 'analyses': {'A': ['dc']}}})
        private = selection.effective_setup(source, plan)
        private['tests'][0]['analyses'][1]['fields']['saveOppoint'] = 'nil'
        normalized, proof = selection.normalize_previous_selection(source, private, plan)
        self.assertTrue(proof)
        self.assertEqual(normalized['tests'][0]['enabled'], True)
        self.assertFalse(normalized['tests'][0]['analyses'][1]['enabled'])
        self.assertEqual(normalized['tests'][0]['analyses'][1]['fields']['saveOppoint'], 'nil')
        private['tests'][0]['analyses'][0]['enabled'] = True
        untouched, proof = selection.normalize_previous_selection(source, private, plan)
        self.assertIsNone(proof)
        self.assertEqual(untouched, private)

    def test_mixed_source_and_applied_flags_are_not_an_owned_selection(self):
        source = setup()
        plan = selection.selection_plan(source, {'execution_selection': {'tests': ['B']}})
        private = selection.effective_setup(source, plan)
        private['tests'][0]['enabled'] = True
        normalized, proof = selection.normalize_previous_selection(source, private, plan)
        self.assertIsNone(proof)
        self.assertEqual(normalized, private)

    def test_repeat_capture_inheritance_restores_source(self):
        source = setup(); first = selection.selection_plan(source, {'execution_selection': {'tests': ['B']}})
        restore = selection.capture_selection_plan(source, None, first)
        self.assertEqual(restore['selected'], ['A'])
        self.assertEqual(restore['analyses'], {'A': ['ac'], 'B': ['tran']})

    def test_script_sets_and_reads_all_tests_and_analyses_including_disabled(self):
        plan = selection.selection_plan(setup(), {'execution_selection': {'tests': ['B']}})
        script = '\n'.join(selection.selection_skill_lines(plan))
        self.assertIn('maeSetSetup(?tests list("A") ?enabled nil', script)
        self.assertIn('maeSetSetup(?tests list("B") ?enabled t', script)
        self.assertIn('maeSetAnalysis("A" "dc" ?enable nil', script)
        self.assertIn('maeGetEnabledAnalysis("B"', script)
        self.assertIn('analysis options changed', script)
        self.assertLess(script.rfind('maeSetAnalysis('), script.find('maeSetSetup(?tests'))
        self.assertIn('Crescendo final enabled Test set differs', script)

    def test_analysis_catalogue_unions_embedded_and_state(self):
        rows = _merge_analyses([{'name': 'ac', 'enabled': True, 'fields': {'start': '1'}}],
            [{'name': 'ac', 'enabled': False, 'fields': {'stop': '1G'}}, {'name': 'tran', 'enabled': False}])
        self.assertEqual([row['name'] for row in rows], ['ac', 'tran'])
        self.assertFalse(rows[0]['enabled'])
        self.assertEqual(rows[0]['fields'], {'start': '1', 'stop': '1G'})

    def test_native_masks_distinguish_false_unknown_and_global_off(self):
        source = setup()
        self.assertIsNone(test_corner_availability(source, 'B')[0]['test_enabled'])
        apply_native_corner_schedule(source, {'read_ok': True, 'groups': {
            'TT': {'disabled_tests': ['B']}, 'SS': {'disabled_tests': []}},
            'nominal_tests': {'A': True, 'B': False}})
        rows = test_corner_availability(source, 'B')
        self.assertFalse(rows[0]['test_enabled'])
        self.assertTrue(rows[1]['test_enabled'])
        self.assertFalse(rows[1]['enabled'])
        self.assertIn('"B"', '\n'.join(native_corner_schedule_skill(source, '/tmp/corners', include_disabled_tests=True)))

    def test_saved_mask_missing_is_unknown_and_exact_known_container_is_parsed(self):
        root = ET.fromstring('<setupdb><active><tests><test>A</test><test>B<disabledcorners><disabledcorner>TT</disabledcorner></disabledcorners></test></tests></active></setupdb>')
        masks = _test_corner_masks(root)
        self.assertFalse(masks['A']['known'])
        self.assertEqual(masks['B']['disabled_corners'], ['TT'])
        self.assertTrue(masks['B']['known'])

    def test_native_disabledtests_overrides_per_test_attribute(self):
        root = ET.fromstring('<setupdb><active><tests><test enabled="1">A</test><test enabled="1">B</test></tests><disabledtests><test>B</test></disabledtests></active></setupdb>')
        rows = _test_rows(root, {'library_name':'L','cell_name':'C','path':'maestro.sdb'}, {})
        self.assertEqual([(row['name'],row['enabled']) for row in rows], [('A',True),('B',False)])

    def test_native_saved_corner_disabledtests_masks_cover_disabled_tests(self):
        root = ET.fromstring('<setupdb><active><corners><corner>_default</corner><corner>TT<disabledtests><test>B</test></disabledtests></corner><corner>FF<disabledtests><test>A</test></disabledtests></corner></corners><tests><test>A</test><test enabled="0">B</test></tests></active></setupdb>')
        masks = _test_corner_masks(root)
        self.assertEqual(masks['A']['disabled_corners'], ['FF'])
        self.assertEqual(masks['B']['disabled_corners'], ['TT'])
        self.assertTrue(masks['B']['known'])
        self.assertTrue(masks['B']['nominal_enabled'])

    def test_point_coverage_excludes_only_native_masked_placeholder_pairs(self):
        from crescendo_engine.private_netlist import native_capture_point_coverage
        tasks = [{'point_id':1,'test':'A','source_corner':'TT'}, {'point_id':2,'test':'B','source_corner':'FF'}]
        parsed = {'points':[{'pointID':1,'corner_name':'TT'}, {'pointID':2,'corner_name':'FF'}],
                  'test_statuses':[{'pointID':point,'test_name':test,'statusCode':10} for point in [1,2] for test in ['A','B']]}
        schedule = {'read_ok':True,'groups':{'TT':{'disabled_tests':['B']},'FF':{'disabled_tests':['A']}}}
        result = native_capture_point_coverage(tasks,parsed,['A','B'],schedule)
        self.assertTrue(result['ok'])
        self.assertEqual(result['expected_test_point_count'],2)
        self.assertEqual(len(result['masked_test_points']),2)
        self.assertFalse(native_capture_point_coverage(tasks[:1],parsed,['A','B'],schedule)['ok'])
        self.assertFalse(native_capture_point_coverage(tasks,parsed,['A','B'])['ok'])
        parsed['points'][1]['corner_name']='ambiguous_expanded_name'
        self.assertFalse(native_capture_point_coverage(tasks[:1],parsed,['A','B'],schedule)['ok'])

    def test_native_capture_config_carries_selection_to_all_phases(self):
        source = setup()
        config = _capture_request_config({'setup': source}, {'runtime': {}},
            {'execution_selection': {'tests': ['B']}})
        self.assertEqual(config['_capture_execution_selection']['selected'], ['B'])
        self.assertFalse(source['tests'][1]['enabled'])

    def test_saved_identity_accepts_analysis_reordering_but_rejects_option_changes(self):
        import tempfile
        from test_native_corner_identity import SavedCornerIdentityTests
        from crescendo_engine.private_netlist import _capture_setup_identity, PrivateNetlistUnsupported
        fixture = SavedCornerIdentityTests()
        ac = '<analysis name="ac"><field name="stop">"1G"</field></analysis>'
        dc = '<analysis name="dc"><field name="enable">nil</field></analysis>'
        fixture.SOURCE = fixture.SOURCE.replace('<test enabled="1">a', '<test enabled="1">a<analyses>'+dc+ac+'</analyses>')
        with tempfile.TemporaryDirectory() as root:
            workspace = fixture.fixture(root, fixture.SOURCE.replace(dc+ac, ac+dc))
            self.assertTrue(_capture_setup_identity(workspace, root)['ok'])
            with open(workspace['setup_sdb'], 'w') as stream:
                stream.write(fixture.SOURCE.replace(dc+ac, ac+dc).replace('1G','2G'))
            with self.assertRaises(PrivateNetlistUnsupported):
                _capture_setup_identity(workspace, root)

    def test_numeric_manifest_records_qualified_execution_selection(self):
        import os
        import tempfile
        from unittest import mock
        from test_netlist_providers import NumericMaestroProviderTest
        from crescendo_engine.netlist_providers import prepare_maestro_netlists
        fixture = NumericMaestroProviderTest()
        with tempfile.TemporaryDirectory() as root:
            prepared, parameters, config, spec, model = fixture.fixtures(root)
            spec['execution_selection'] = {'tests': ['tb']}
            with mock.patch('crescendo_engine.netlist_providers.build_workspace', side_effect=fixture.workspace), \
                 mock.patch('crescendo_engine.netlist_providers._capture_phase', side_effect=fixture.capture(model)), \
                 mock.patch('crescendo_engine.netlist_providers.verify_sources_unchanged', return_value={'ok': True}):
                manifest = prepare_maestro_netlists(prepared, parameters, {'p': 2e-6}, config,
                    os.path.join(root, 'simulation'), 'c1', spec=spec)
            self.assertEqual(manifest['execution_selection']['selected'], ['tb'])
            self.assertEqual(manifest['execution_selection'], manifest['netlist_generation']['execution_selection'])

    def test_materialized_template_keeps_selection_and_legacy_none(self):
        import os
        import tempfile
        from test_private_netlist import PrivateNetlistCandidateTest
        from crescendo_engine.private_netlist import materialize_candidate, _manifest_digest
        with tempfile.TemporaryDirectory() as root:
            template = PrivateNetlistCandidateTest().template(root)
            template['execution_selection'] = selection.selection_plan(setup(), {'execution_selection': {'tests': ['B']}})
            template['manifest_sha256'] = _manifest_digest(template)
            candidate = materialize_candidate(template, {'p': 1000}, os.path.join(root, 'candidate'), 'c1')
            self.assertEqual(candidate['execution_selection'], template['execution_selection'])
            self.assertEqual(candidate['netlist_generation']['execution_selection'], template['execution_selection'])
            candidate['execution_selection']['selected'].append('A')
            self.assertEqual(template['execution_selection']['selected'], ['B'])

    def test_template_manifest_scope_and_workspace_use_original_disabled_source(self):
        import os
        import tempfile
        from unittest import mock
        from test_netlist_providers import NumericMaestroProviderTest
        from crescendo_engine import private_netlist
        fixture = NumericMaestroProviderTest()
        with tempfile.TemporaryDirectory() as root:
            prepared, parameters, config, spec, model = fixture.fixtures(root)
            prepared['setup']['tests'][0].update(library='PRIVATE', cell='TB', view='schematic', enabled=False)
            parameters[0].update(numeric_value=2e-6, name='r', owner_library='PRIVATE', owner_cell='TB',
                owner_view='schematic', hierarchy_path='/R0', owner_occurrence_path='/',
                design_library='PRIVATE', design_cell='TB', design_view='schematic')
            spec['execution_selection'] = {'tests': ['tb']}
            source = copy.deepcopy(prepared['setup'])
            with mock.patch.object(private_netlist, 'build_workspace', side_effect=fixture.workspace) as builder, \
                 mock.patch.object(private_netlist, '_capture_phase', side_effect=fixture.native_capture(model)), \
                 mock.patch.object(private_netlist, 'verify_sources_unchanged', return_value={'ok': True}):
                template = private_netlist.prepare_private_netlist(prepared, parameters, config,
                    os.path.join(root, 'template'), spec=spec)
            self.assertEqual(builder.call_args[0][0], source)
            self.assertFalse(template['workspace']['setup']['tests'][0]['enabled'])
            self.assertEqual(template['execution_selection']['selected'], ['tb'])
            self.assertEqual(template['execution_selection'], template['netlist_generation']['execution_selection'])
            self.assertEqual(prepared['setup'], source)


if __name__ == '__main__':
    unittest.main()
