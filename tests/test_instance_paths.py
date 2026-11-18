"""Exact manual occurrence selection without a cells-only tree discovery."""
import copy
import json
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine import hierarchy, runner
from crescendo_engine.unified_configuration import build_configuration, normalize_configuration
from crescendo.demo_service import DemoService


DESIGN = {"library": "USER", "cell": "TOP", "view": "schematic"}


def native_body():
    records = [
        {"kind": "CELL", "values": ["/", "USER", "TOP", "schematic", 0]},
        {"kind": "INSTANCE", "values": ["/I0", "I0", "USER", "AMP", "symbol", True, "/", 0]},
        {"kind": "CELL", "values": ["/I0", "USER", "AMP", "schematic", 1]},
        {"kind": "INSTANCE", "values": ["/I0/R<3>", "R<3>", "analogLib", "res", "symbol", False, "/I0", 1]},
        {"kind": "PARAM", "values": ["/I0/R<3>", "r", "Resistance", "string", "1k", "1k", "Ohm",
            None, True, True, True, None, None, "USER", "AMP", None, "float"]}]
    return {"designs": [{"design": dict(DESIGN), "hierarchy": hierarchy.normalize_probe(records, DESIGN)}]}


class InstancePathTests(unittest.TestCase):
    def test_lines_preserve_numbers_and_never_accept_unsafe_or_iterated_forms(self):
        text = "\n/I0/R<3>\nUSER/TOP/schematic /I0\nUSER/TOP/schematic/I0\n/IARR<3:0>\n/I[3]\n/I*\n/I\\x\n/I0//R\n/I0/\nwrong\nOTHER/TOP/schematic /I0"
        rows = hierarchy.parse_instance_paths(text, [DESIGN])
        self.assertEqual([r['line'] for r in rows], list(range(2, 13)))
        self.assertEqual([r['hierarchy_path'] for r in rows if 'design' in r], ['/I0/R<3>', '/I0', '/I0'])
        self.assertTrue(all(r.get('reason') for r in rows[3:]))
        other = dict(DESIGN, view='config')
        rows = hierarchy.parse_instance_paths('/I0\nUSER/TOP/config /I0', [DESIGN, other])
        self.assertIn('ambiguous', rows[0]['reason'])
        self.assertEqual(rows[1]['design'], other)

    def test_mixed_valid_missing_reference_and_invalid_lines_are_independent(self):
        body = native_body()
        setup = {'tests': [DESIGN]}
        with mock.patch.object(hierarchy, 'probe_hierarchy', return_value=body) as probe, \
             mock.patch('crescendo_engine.workspace.resolve_copy_libraries', return_value=('USER', None, {'USER': '/source'})):
            result = hierarchy.resolve_instance_paths(setup, {}, '/I0\n/missing\n/I0/R<3>\n/I<3:0>\n/I0')
        self.assertEqual([r['valid'] for r in result['lines']], [True, False, True, False, True])
        self.assertEqual(len(result['scopes']), 2)
        self.assertIn('not found', result['lines'][1]['reason'])
        options = probe.call_args[1]
        self.assertTrue(options['targeted'] and options['cells_only'] and options['allow_missing'])
        self.assertEqual(len(options['selected_cell_paths']), 4)

    def test_no_valid_syntax_does_not_probe_and_does_not_fall_back_to_full(self):
        with mock.patch.object(hierarchy, 'probe_hierarchy') as probe:
            result = hierarchy.resolve_instance_paths({'tests': [DESIGN]}, {}, '/I<3:0>\nnot-a-path')
        probe.assert_not_called()
        self.assertEqual(result['scopes'], [])

    def test_targeted_prepare_cannot_fall_back_to_an_unscoped_scan(self):
        with mock.patch("crescendo_engine.setup_baseline.load_bound_setup") as load:
            with self.assertRaisesRegex(ValueError, "explicit selected"):
                runner.prepare_setup("unused", {}, targeted=True)
        load.assert_not_called()

    def test_generated_probe_gates_work_before_cdf_master_and_config_resolution(self):
        skill = hierarchy.probe_skill('USER', 'TOP', 'schematic', '/owned/records', 32,
                                     selected_cell_paths=['/I0/R<3>'], targeted=True)
        self.assertIn('cr_Targeted = t', skill)
        self.assertIn('inst = dbFindAnyInstByName(cv name)', skill)
        self.assertIn('foreach(inst cr_ProbeInstances(cv hierPath)', skill)
        gate = skill.index('when(cr_ProbeVisit(instPath)')
        self.assertLess(gate, skill.index('ignoreReason = crescConfigIgnoreReason(inst)'))
        self.assertLess(gate, skill.index('binding = crescConfigBind(bindingContext'))
        self.assertLess(gate, skill.index('instCdfResult = errset(cdfGetInstCDF(inst)'))
        self.assertIn('strcat(path "/")', skill)
        self.assertIn('strcat(scope "/")', skill)

    def test_manual_cell_has_identical_scope_catalog_and_checkpoint_identity_to_tree(self):
        source = native_body()
        with mock.patch('crescendo_engine.workspace.resolve_copy_libraries', return_value=('USER', None, {'USER': '/source'})):
            checked = next(r for r in hierarchy.cell_scope_records(source, setup={}, config={}) if r['hierarchy_path'] == '/I0')
            with mock.patch.object(hierarchy, 'probe_hierarchy', return_value=copy.deepcopy(source)):
                manual = hierarchy.resolve_instance_paths({'tests': [DESIGN]}, {}, '/I0')['scopes'][0]
        self.assertEqual(manual, checked)
        catalogs = [runner.parameter_catalog({}, copy.deepcopy(source)) for _ in (manual, checked)]
        self.assertTrue(catalogs[0] and catalogs[0][0]['optimizable'])
        self.assertEqual(catalogs[0], catalogs[1])
        from crescendo_engine.optimizers import PopulationGenerator
        spec = {'evaluator': 'mock', 'variables': [{'parameter_id': catalogs[0][0]['parameter_id'], 'minimum': 1, 'maximum': 2}],
                'objectives': [{'output_key': 'mock::cost'}], 'constraints': []}
        prepared = {'parameters': catalogs[0], 'outputs': [{'key': 'mock::cost'}], 'setup': {},
                    'cell_selection': {'scope_ids': [manual['scope_id']], 'scopes': [manual]}}
        checkpoint = runner._checkpoint_document(spec, prepared, PopulationGenerator(spec['variables']), [], [], 0, None, {})
        prepared['cell_selection'] = {'scope_ids': [checked['scope_id']], 'scopes': [checked]}
        runner.validate_checkpoint(json.loads(json.dumps(checkpoint)), prepared)
        self.assertEqual(checkpoint['manifest']['cell_selection'], prepared['cell_selection'])

    def test_versioned_configuration_roundtrip_keeps_exact_scopes_and_legacy_absence(self):
        paths = {'schema_version': 'crescendo.instance_paths.v1', 'scopes': [{'design': DESIGN, 'hierarchy_path': '/I0/R<3>'}]}
        document = build_configuration({'instance_path_selection': paths}, {}, {}, [])
        self.assertEqual(normalize_configuration(json.loads(json.dumps(document)))['document']['instance_path_selection'], paths)
        self.assertNotIn('instance_path_selection', build_configuration({}, {}, {}, []))
        for bad in ({'schema_version': 'v99', 'scopes': []}, {'schema_version': paths['schema_version'], 'scopes': []},
                    {'schema_version': paths['schema_version'], 'scopes': [{'design': DESIGN, 'hierarchy_path': '/I<3:0>'}]}):
            with self.assertRaises(ValueError):
                normalize_configuration(dict(document, instance_path_selection=bad))

    def test_cold_manual_demo_materializes_only_selected_parameters(self):
        with tempfile.TemporaryDirectory() as root:
            service = DemoService(scale='huge', latency=0, history_runs=0, workdir=root)
            try:
                # Expected fixture is separate from the service cache: this is a cold call.
                from crescendo.demo_service import _Hierarchy
                name = service.test_rows[0]['name']
                reference = _Hierarchy(service, 0, name)
                choices = reference.scope_rows[-3:]
                ids = [row['scope_id'] for row in choices]
                body, parameters = reference.selection_payload(ids)
                token, _ = service.inspect_tests(os.path.join(root, 'fixture.sdb'))
                with mock.patch.object(service, '_hierarchy', side_effect=AssertionError('full source requested')):
                    inspection, preview = service.inspect_instance_paths(token, name,
                        '\n'.join(row['hierarchy_path'] for row in choices))
                    _, prepared = service.prepare_cells(inspection, ids)
                self.assertEqual(preview['cell_scopes'], choices)
                self.assertEqual(prepared['hierarchy'], body)
                self.assertEqual(prepared['parameters'], parameters)
                source = service._inspections[inspection]['scoped_source']
                self.assertEqual(source.parameter_count, len(parameters))
                self.assertLess(source.parameter_count, reference.parameter_count // 100)
                with mock.patch('crescendo.demo_service._Hierarchy') as constructor:
                    token, preview = service.inspect_instance_paths(token, name, '/I<3:0>')
                    constructor.assert_not_called()
                    self.assertIsNone(token)
            finally:
                service.exit_optimizer()

    def test_huge_three_paths_skip_tree_and_match_checkbox_parameter_payload(self):
        with tempfile.TemporaryDirectory() as root:
            service = DemoService(scale='huge', latency=0, history_runs=0, workdir=root)
            try:
                token, catalog = service.inspect_tests(os.path.join(root, 'fixture.sdb'))
                name = service.test_rows[0]['name']
                source = service._hierarchy(name)
                choices = source.scope_rows[-3:]
                text = '\n'.join(row['hierarchy_path'] for row in choices)
                with mock.patch.object(service, 'inspect_test_cells', side_effect=AssertionError('full tree invoked')):
                    inspection, preview = service.inspect_instance_paths(token, name, text)
                    _pid, prepared = service.prepare_cells(inspection, [r['scope_id'] for r in choices])
                checked_body, checked_params = source.selection_payload([r['scope_id'] for r in choices])
                self.assertEqual(preview['cell_scopes'], choices)
                self.assertEqual(prepared['parameters'], checked_params)
                self.assertEqual(prepared['hierarchy'], checked_body)
                self.assertEqual(len(source.cells), 4000)
            finally:
                service.exit_optimizer()


if __name__ == '__main__':
    unittest.main()
