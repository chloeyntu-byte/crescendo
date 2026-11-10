import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine.diagnosis_catalog import describe_check
from crescendo_engine.environment_test import EnvironmentTestManager, _report_truncation_paths
from crescendo_engine.environment_diagnostics import collect_run_diagnostics
from test_environment_test import FixtureService


class ReportDeliveryTests(unittest.TestCase):
    def test_failed_baseline_reports_both_ocean_consoles_without_netlist_check(self):
        with tempfile.TemporaryDirectory() as root:
            service = FixtureService(root, {'maestro_baseline': 'native_failure'})
            manager = EnvironmentTestManager(service)
            service.manager = manager
            original = service.start

            def start(prepared_id, spec):
                launch = original(prepared_id, spec)
                if spec['execution_backend'] == 'maestro_baseline':
                    path = os.path.join(launch['run_root'], 'candidates', 'g000_c0000', 'native_baseline')
                    os.makedirs(path)
                    for name, content in (('ocean.stdout', 'ERROR stdout fixture evidence\n'),
                                          ('ocean.stderr', 'ERROR stderr fixture evidence\n')):
                        with open(os.path.join(path, name), 'w', encoding='utf-8') as stream:
                            stream.write(content)
                return launch

            spec = {'variables': [{'parameter_id': 'x', 'minimum': 1e-6, 'maximum': 2e-6}],
                    'objectives': [{'output_key': 'main::gain', 'direction': 'maximize'}],
                    'simulator': 'spectre'}
            with mock.patch.object(service, 'start', side_effect=start), \
                 mock.patch('crescendo_engine.environment_test._case_capability', return_value={'ready': True}), \
                 mock.patch('crescendo_engine.environment_test.run_environment_probes', return_value=[]):
                test_id = manager.start('prepared-fixture', spec)['test_id']
                self.assertTrue(manager.join(12))
                result = manager.progress(test_id)
                with open(result['main_log'], encoding='utf-8') as stream:
                    report = stream.read()
            self.assertIn('SECTION BEGIN spectre.maestro_baseline/netlist', report)
            self.assertIn('ERROR stdout fixture evidence', report)
            self.assertIn('ERROR stderr fixture evidence', report)
            self.assertIn('report_evidence_integrity', report)
            self.assertIn('collector only; writer completion uses report_ready', report)

    def test_truncation_and_failed_proof_are_explicit(self):
        self.assertTrue(_report_truncation_paths({'identity': {'read_truncated': True}}))
        check = {'id': 'spectre.maestro_baseline.raw_cleanup_proof', 'check_id': 'raw_cleanup_proof',
                 'status': 'FAIL', 'message': 'Native producer evidence'}
        self.assertIn('未通过', describe_check(check)['summary'])

    def test_bounded_ocean_read_is_a_collector_omission(self):
        with tempfile.TemporaryDirectory() as root:
            with open(os.path.join(root, 'ocean.stdout'), 'w', encoding='utf-8') as stream:
                stream.write('large fixture line\n' * 100)
            with mock.patch('crescendo_engine.environment_diagnostics.MAX_FILE', 64):
                records = collect_run_diagnostics(root, 'maestro_baseline')
            collection = next(record['payload'] for record in records
                              if record['observation'] == 'diagnostic.collection')
            self.assertEqual(collection['status'], 'PARTIAL')
            self.assertTrue(any(row.get('path') == 'ocean.stdout'
                                and 'bounded read' in row.get('reason', '')
                                for row in collection['omitted_or_truncated']))


if __name__ == '__main__':
    unittest.main()
