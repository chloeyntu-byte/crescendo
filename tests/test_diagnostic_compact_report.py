import copy
import json
import inspect
import tempfile
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
        self.assertIn('sidecar_manifest', text)
        self.assertIn('focused_checks_summary', text)
        self.assertTrue(payload['report_sidecars'])
        self.assertEqual(payload['sidecar_integrity']['status'], 'PASS')
        self.assertIn('check_group ', text)
        self.assertNotIn('"component":', text)

    def test_full_mode_retains_original_detail_and_identical_check_contract(self):
        payload, text = self.run_report(full=True)
        self.assertNotIn('concise_case', text)
        self.assertNotIn('@run:', text)
        self.assertIn('"component":', text)


class CompactVerdictTests(unittest.TestCase):
    def render(self, mutate=None, full=False):
        from tests.test_diagnostic_log_sidecars import synthetic_row, synthetic_focus, render_source
        from crescendo_engine import environment_test as reports
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        row = synthetic_row(temp.name)
        if mutate:
            mutate(row)
        if full:
            row['diagnostic_log_detail'] = row['config']['runtime']['diagnostic_log_detail'] = 'full'
        text = render_source(inspect.getsource(reports), row, synthetic_focus(row))
        lines = text.splitlines()
        start = next(i for i, line in enumerate(lines) if line.startswith('1. '))
        verdicts = [line for line in lines[start:] if line.startswith(('1. ', '2. ', '3. '))][:3]
        return row, text, verdicts

    def test_verdict_contains_original_first_cause_and_reference(self):
        message = 'CompletionEvidenceError: ADE point vco_stress/1 has non-complete statusCode=0'
        def mutate(row):
            row['case_results']['spectre.maestro_baseline']['first_cause']['message'] = message
        row, text, verdicts = self.render(mutate)
        self.assertIn('[SPECTRE] — ' + message, verdicts[0])
        self.assertIn('first_cause_ref: spectre.maestro_baseline', text)
        self.assertNotIn('See first cause:', text)

    def test_long_multiline_cause_is_bounded_marked_and_lossless_in_sidecar(self):
        from crescendo_engine.diagnostic_log import read_evidence
        message = 'CompletionEvidenceError: ' + 'very long detail\n' * 50
        def mutate(row):
            row['case_results']['spectre.maestro_baseline']['first_cause']['message'] = message
        row, text, verdicts = self.render(mutate)
        self.assertEqual(len(verdicts[0]), 200)
        self.assertTrue(verdicts[0].endswith(' [truncated]'))
        self.assertIn('CompletionEvidenceError:', verdicts[0])
        evidence = read_evidence(row['report_sidecars'][0]['path'])
        self.assertEqual(evidence['case_results']['spectre.maestro_baseline']['first_cause']['message'], message)
        _row, _text, full_verdicts = self.render(mutate, full=True)
        self.assertNotIn('[truncated]', full_verdicts[0])
        self.assertGreater(len(full_verdicts[0]), 200)

    def test_verdict_uses_existing_environment_and_inline_secret_redaction(self):
        sensitive = 'fixture-sensitive-value'
        def mutate(row):
            row['config']['cadence'] = {'launch_environment': {'API_TOKEN': sensitive}}
            row['case_results']['spectre.maestro_baseline']['first_cause']['message'] = (
                'reader failed: ' + sensitive + ' password=' + 'fixture-password')
        _row, _text, verdicts = self.render(mutate)
        self.assertNotIn(sensitive, verdicts[0])
        self.assertNotIn('fixture-password', verdicts[0])
        self.assertIn('reader failed: <redacted> password=<redacted>', verdicts[0])

    def test_pass_has_no_first_cause_or_spurious_reference(self):
        def mutate(row):
            row['case_results']['spectre.maestro_baseline'].update(state='completed', first_cause=None)
        row, text, verdicts = self.render(mutate)
        self.assertEqual(len(verdicts), 3)
        self.assertTrue(all('[SPECTRE]' in line and ' — ' not in line for line in verdicts))
        self.assertNotIn('   first_cause_ref:', text)

    def test_each_mode_retains_its_reason_even_when_causes_share_a_reference(self):
        message = 'Cluster mode requires scheduler_query_template, scheduler_cancel_template'
        def mutate(row):
            cases = list(row['case_results'].values())
            for case in cases[:2]:
                case.update(state='blocked', first_cause={'message': message})
            cases[2].update(state='failed', first_cause={'error': 'Independent solver error'})
        row, text, verdicts = self.render(mutate)
        self.assertTrue(all(message in line for line in verdicts[:2]))
        self.assertIn('Independent solver error', verdicts[2])
        self.assertEqual(text.count('   first_cause_ref: spectre.maestro_baseline'), 2)
        self.assertTrue(all(len(line) <= 200 for line in verdicts))

    def test_verdict_uses_existing_secret_redaction(self):
        def mutate(row):
            row['config']['cadence'] = {'launch_environment': {'API_TOKEN': 'private-fixture-value'},
                                        'license_file': 'private-license-fixture'}
            row['case_results']['spectre.maestro_baseline']['first_cause']['message'] = (
                'Rejected private-fixture-value token=abc private-license-fixture')
        row, text, verdicts = self.render(mutate)
        self.assertIn('<redacted>', verdicts[0])
        for secret in ('private-fixture-value', 'token=abc', 'private-license-fixture'):
            self.assertNotIn(secret, verdicts[0])


if __name__ == '__main__':
    unittest.main()
