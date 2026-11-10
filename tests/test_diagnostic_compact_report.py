import copy
import json
import unittest

from tests import test_environment_test as fixtures
from crescendo_engine.environment_test import (
    _compact_report_context, _compact_success_proof, _short_report_paths,
)
from crescendo_engine.diagnosis_catalog import read_section


class CompactReportUnitTests(unittest.TestCase):
    def test_paths_are_reversible_and_neighbour_prefix_is_not_replaced(self):
        for root in ('/a/run', '/a/run "quoted"'):
            value = {'path': root + '/candidate/psf', 'error': 'failed at ' + root + '/reader.il)',
                     'other': root + '_unrelated/file'}
            projected = json.loads(_short_report_paths(json.dumps(value), {'@run:case': root}))
            self.assertEqual(projected['path'], '@run:case/candidate/psf')
            self.assertEqual(projected['other'], value['other'])
            self.assertEqual(projected['error'].replace('@run:case', root), value['error'])

    def test_selected_corners_are_complete_and_source_context_is_not_mutated(self):
        context = {'run_root': '/run', 'execution_selection': {'requested_tests': ['T'], 'tests': [
            {'name': 'T', 'selected': True, 'requested_analyses': ['sp'], 'corners': [
                {'name': 'C%d' % i, 'enabled': True} for i in range(9)]}]},
            'artifacts': [{'path': '/run/%d' % i, 'status': 'generated'} for i in range(100)]}
        before = copy.deepcopy(context)
        compact = _compact_report_context(context)
        self.assertEqual(compact['execution_selection']['tests'][0]['enabled_corners'], ['C%d' % i for i in range(9)])
        self.assertEqual(compact['artifacts_count'], 100)
        self.assertEqual(len(compact['artifacts']), 2)
        self.assertEqual(context, before)

    def test_success_summary_preserves_counts_without_repeated_commands(self):
        proof = {'ok': True, 'parameters_count': 2, 'points': [{'argv': ['solver', 'large']}]*20,
                 'netlist_generation': {'source_mode': 'maestro_netlist', 'template_qualified': False,
                                        'native_generation_required': True, 'source_guard': ['large']*100}}
        before = copy.deepcopy(proof)
        compact = _compact_success_proof(proof)
        self.assertEqual(compact['parameters_count'], 2)
        self.assertEqual(compact['points_count'], 20)
        self.assertTrue(compact['netlist_generation']['native_generation_required'])
        self.assertNotIn('large', json.dumps(compact))
        self.assertEqual(proof, before)


class CompactReportIntegrationTests(unittest.TestCase):
    setUp = fixtures.EnvironmentTestManagerTests.setUp
    fixture = fixtures.EnvironmentTestManagerTests.fixture
    finish = fixtures.EnvironmentTestManagerTests.finish
    checks = fixtures.EnvironmentTestManagerTests.checks
    finish_threads = fixtures.EnvironmentTestManagerTests.finish_threads

    def run_report(self, full=False):
        manager, service, spec = self.fixture({'maestro_baseline': 'native_failure'})
        if full:
            service.config['runtime']['diagnostic_log_detail'] = 'full'
        payload = self.finish(manager, manager.start('prepared-fixture', spec)['test_id'])
        with open(payload['main_log'], encoding='utf-8') as stream:
            text = stream.read()
        overview = next(json.loads(line.split(' ', 1)[1]) for line in text.splitlines()
                        if line.startswith('diagnostic_overview '))
        checklist = next(json.loads(line.split(' ', 1)[1]) for line in text.splitlines()
                         if '"final_checklist":' in line)
        self.assertTrue(payload['report_ready'])
        self.assertEqual(len(overview['cases']), 3)
        self.assertLess(text.index('diagnostic_overview '), text.index('=== SECTION BEGIN'))
        self.assertEqual(overview['diagnostic_shape']['total_candidates'], 6)
        self.assertEqual([(x['case_id'], x['state'], x['stop_proven']) for x in overview['cases']],
                         [(x['case_id'], x['state'], x['stop_proven']) for x in payload['case_results']])
        self.assertEqual({x['id']: x['status'] for x in checklist['final_checklist']},
                         {x['id']: x['status'] for x in payload['checks']})
        self.assertIn('diagnostic_timing ', text)
        # Copy obtains this case's context and first cause from the same report.
        copied = read_section(payload['main_log'], 'spectre.maestro_baseline', 'request')
        self.assertTrue(copied)
        self.assertIn(payload['case_contexts']['spectre.maestro_baseline']['run_root'], copied)
        self.assertEqual(len(self.checks(payload, 'parameter_application')), 18)
        return payload, text

    def test_default_report_keeps_verdict_and_ui_checks(self):
        payload, text = self.run_report()
        self.assertIn('concise_case', text)
        self.assertIn('path_alias', text)
        self.assertIn('check_group ', text)
        self.assertNotIn('"component":', text)

    def test_full_mode_retains_original_detail_and_identical_check_contract(self):
        payload, text = self.run_report(full=True)
        self.assertNotIn('concise_case', text)
        self.assertNotIn('@run:', text)
        self.assertIn('"component":', text)


if __name__ == '__main__':
    unittest.main()
