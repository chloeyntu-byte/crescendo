"""Saved model identity follows captured native include search, not basename."""
import copy
import json
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine.native_identity import resolve_task_model_identity
from crescendo_engine.private_netlist import native_netlist_identity, validate_capture_corners
from crescendo_engine.environment_diagnostics import _identity_coverage_summary, _intern_structures
from crescendo_engine.diagnosis_catalog import _problem_record


class CornerModelResolutionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='crescendo corner model ')
        self.addCleanup(self.temp.cleanup)
        self.root = self.temp.name
        self.source = self.root + '/workarea'
        self.native = self.root + '/native/netlist'
        self.pdk = self.root + '/PDK/spectre'
        for path in (self.source, self.native, self.pdk):
            os.makedirs(path)
        self.model = self.write(self.pdk + '/toplevel.scs')
        self.sections = ['Noise_Worst', 'Pre_Sim', 'TOP_TT_RFTYP']
        self.setup = {'tests': [{'name': 'a', 'enabled': True}],
            'corners': [{'name': 'PN_TT', 'enabled': True, 'model_groups': ['TTFW'],
                'variables': [{'name': 'temperature', 'value': '55'}, {'name': 'VDD', 'value': '.9'},
                              {'name': 'IND_5G', 'value': '"historical_string_not_a_file.s45p"'}]}],
            'model_groups': [{'name': 'TTFW', 'models': [
                {'file': 'toplevel.scs', 'section': section, 'test': 'All'} for section in self.sections]}]}
        self.task = {'test': 'a', 'corner': 'PN_TT', 'point_id': 1, 'native_cwd': self.native,
            'argv': ['spectre', 'input.scs', '-I' + self.pdk],
            'native_parameters': [{'name': 'VDD', 'value': .9}],
            'temperature': {'value': 55},
            'native_models': [{'file': 'toplevel.scs', 'section': section} for section in self.sections]}
        self.snapshot = {'ok': True, 'saved_source': {'corner_read_ok': True},
            'private_setup': {'corner_read_ok': True}, 'model_path_context': {'source_work_dir': self.source}}
        self.schedule = {'read_ok': True, 'groups': {'PN_TT': {'enabled': True, 'count': 1}},
                         'nominal_enabled': False, 'nominal_tests': {'a': False}}

    def write(self, path):
        with open(path, 'w') as stream:
            stream.write('// Exact fixture model; no solver needed\n')
        return path

    def validate(self, config=None):
        self.task['native_model_resolution'] = resolve_task_model_identity(
            self.task, config or {}, 'spectre', self.setup, self.snapshot)
        return validate_capture_corners(self.setup, [self.task], {}, self.snapshot, self.schedule)

    def test_log_shape_relative_saved_and_native_models_use_captured_I(self):
        result = self.validate()
        self.assertTrue(result['ok'], result)
        proof = result['test_differences'][0]['corner_expansion']['proofs'][0]
        self.assertEqual([row[0] for row in proof['identity']['models']], [self.model] * 3)
        self.assertEqual(self.task['source_corner'], 'PN_TT')
        self.assertFalse(os.path.exists(self.source + '/toplevel.scs'))
        self.assertEqual(len(self.task['native_model_resolution']['source_references']), 1)

    def test_existing_source_model_and_native_symlink_are_same_file(self):
        original = self.write(self.source + '/toplevel.scs')
        os.symlink(original, self.native + '/toplevel.scs')
        result = self.validate()
        self.assertTrue(result['ok'], result)
        actual = self.task['native_model_resolution']['references'][0]
        self.assertEqual(actual['resolved'], self.native + '/toplevel.scs')
        self.assertEqual(actual['physical_path'], original)

    def test_source_absolute_path_is_not_overridden_by_PDK(self):
        original = self.write(self.source + '/toplevel.scs')
        for model in self.setup['model_groups'][0]['models']:
            model['file'] = original
        result = self.validate({'cadence': {'spectre_model_dir': self.pdk}})
        self.assertFalse(result['ok'])
        difference = result['test_differences'][0]
        self.assertEqual(difference['mapping_error'], 'native_corner_identity_mismatch')
        detail = difference['corner_expansion']['identity_failures'][0]
        self.assertEqual(detail['closest_identities'][0]['missing_models'][0][0], original)
        self.assertEqual(detail['actual_models'][0][0], self.model)
        self.assertEqual(detail['closest_identities'][0]['variable_differences'], [])

    def test_same_basename_in_earlier_native_cwd_does_not_pass(self):
        self.write(self.native + '/toplevel.scs')
        result = self.validate()
        self.assertFalse(result['ok'])
        detail = result['test_differences'][0]['corner_expansion']['identity_failures'][0]
        self.assertEqual(detail['model_resolution']['references'][0]['resolved'], self.native + '/toplevel.scs')

    def test_wrong_section_is_still_rejected_with_precise_detail(self):
        self.task['native_models'][0]['section'] = 'Noise_Best'
        result = self.validate()
        self.assertFalse(result['ok'])
        closest = result['test_differences'][0]['corner_expansion']['identity_failures'][0]['closest_identities'][0]
        self.assertEqual(closest['missing_models'], [(self.model, 'Noise_Worst')])
        self.assertEqual(closest['variable_differences'], [])

    def test_existing_configured_source_model_directory_is_used(self):
        self.task['argv'] = ['spectre', 'input.scs']
        result = self.validate({'cadence': {'spectre_model_dir': self.pdk}})
        self.assertTrue(result['ok'], result)

    def test_native_environment_expansion_and_split_I_share_search_semantics(self):
        self.task['argv'] = ['spectre', 'input.scs', '-I', '$PDK/spectre']
        with mock.patch.dict(os.environ, {'PDK': self.root + '/PDK'}):
            result = self.validate()
        self.assertTrue(result['ok'], result)
        self.assertEqual(self.task['native_model_resolution']['include_dirs'], [self.pdk])

    def test_approved_remap_keeps_original_authority_when_original_is_unavailable(self):
        original = self.source + '/unavailable_model.scs'
        for model in self.setup['model_groups'][0]['models']:
            model['file'] = original
        self.snapshot['verified_model_path_normalizations'] = [{'original': original, 'resolved': self.model}]
        result = self.validate()
        self.assertTrue(result['ok'], result)

    def test_unresolved_model_retains_search_evidence_without_checking_variables(self):
        os.unlink(self.model)
        result = self.validate()
        self.assertFalse(result['ok'])
        detail = result['test_differences'][0]['corner_expansion']['identity_failures'][0]
        self.assertEqual(detail['model_resolution']['include_dirs'], [self.pdk])
        self.assertIn('toplevel.scs', detail['model_path_errors'][0])
        self.assertNotIn('historical_string', json.dumps(detail['model_resolution']))

    def test_failure_detail_is_bounded_for_many_points(self):
        self.task['native_models'][0]['section'] = 'WRONG'
        self.task['native_model_resolution'] = resolve_task_model_identity(
            self.task, {}, 'spectre', self.setup, self.snapshot)
        tasks = [dict(copy.deepcopy(self.task), point_id=index) for index in range(40)]
        result = validate_capture_corners(self.setup, tasks, {}, self.snapshot, self.schedule)
        expansion = result['test_differences'][0]['corner_expansion']
        self.assertEqual(len(expansion['identity_failures']), 32)
        self.assertEqual(expansion['identity_failures_omitted'], 8)

    def test_failure_summary_keeps_actual_expected_models_and_resolution(self):
        self.task['native_models'][0]['section'] = 'WRONG'
        result = self.validate()
        summary = _identity_coverage_summary({'numeric/corner_identity.json': {'ok': True, 'native_coverage': result}}, {})
        self.assertEqual(summary['status'], 'FAILED')
        self.assertEqual(summary['tests'][0]['corner_identity_errors'][0], 'native_corner_identity_mismatch')
        failure = summary['tests'][0]['corner_identity_failures'][0]
        self.assertEqual(failure['model_resolution']['source_work_dir'], self.source)
        self.assertIn((self.model, 'Noise_Worst'), failure['closest_identities'][0]['missing_models'])

    def test_compact_saved_setup_success_keeps_interned_failure_reference(self):
        failure = {'ok': False, 'test_differences': [{'test': 'a', 'native_corner': 'PN_TT',
                   'mapping_error': 'native_corner_identity_mismatch', 'detail': 'missing_models ' * 32}]}
        seen = {}
        _intern_structures(failure, seen, 'execution_diagnostics#data/detail/corner_identity')
        document = _intern_structures({'ok': True, 'native_coverage': failure,
            'successful_sibling': {'ok': True, 'parameters': list(range(64))}}, seen, 'corner_identity#data')
        compact = _problem_record(document)
        self.assertIn('same_as', compact['native_coverage'])
        self.assertNotIn('successful_subtree_summary', compact)
        self.assertTrue(compact['successful_sibling']['successful_subtree_summary'])


if __name__ == '__main__':
    unittest.main()
