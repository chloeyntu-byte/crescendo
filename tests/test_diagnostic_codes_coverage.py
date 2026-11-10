# -*- coding: utf-8 -*-
import unittest
import os
import queue
import tempfile
import threading
from crescendo_engine.diagnosis_catalog import describe_check, diagnostic_code, resolve_diagnostic_code, _CODE_KEYS
from crescendo_engine.environment_diagnostics import _identity_coverage_summary
from crescendo_engine.environment_test import EnvironmentTestManager

class DiagnosticCodeCoverageTest(unittest.TestCase):
    def test_catalog_codes_are_unique_and_offline_resolvable(self):
        codes = [diagnostic_code(key, 'WARN') for key in _CODE_KEYS]
        self.assertEqual(len(set(codes)), len(codes))
        for key, code in zip(_CODE_KEYS, codes):
            self.assertEqual(resolve_diagnostic_code(code), key)
        self.assertIsNone(diagnostic_code('execution', 'PASS'))
        self.assertEqual(diagnostic_code('scheduler.job.123', 'FAIL'), diagnostic_code('scheduler.job.456', 'WARN'))

    def test_native_members_diff_and_fallback_are_bounded(self):
        labels = ['RANGE_%d' % i for i in range(20)]
        coverage = {'ok': False, 'test_differences': [{'test': 'PDK',
            'mapping_evidence': 'native_test_corner_members',
            'native_member_difference': {'expected': labels, 'actual': ['RANGE_0', 'BAD'],
                'missing': labels[1:], 'unexpected': ['BAD']}}]}
        native = {'read_ok': True, 'members_state': 'available', 'groups': {'RANGE': {
            'enabled': True, 'test_members': {'PDK': labels, 'Order_B': []}}}}
        summary = _identity_coverage_summary({'corner_identity.json': {
            'native_coverage': coverage, 'native_schedule': native}}, {})
        item = summary['tests'][0]
        self.assertEqual(item['mapping_evidence'], 'native_test_corner_members')
        self.assertEqual(len(item['native_member_difference']['missing']), 8)
        self.assertEqual(item['native_member_difference']['omitted_counts']['missing'], 11)
        samples = summary['contracts'][0]['native_schedule']['groups']['RANGE']['test_members']
        self.assertEqual(samples['PDK']['count'], 20)
        self.assertEqual(len(samples['PDK']['sample']), 8)
        self.assertEqual(samples['Order_B']['count'], 0)
        found = []
        class Collector(object):
            def _check(self, row, key, status, message, details, engine):
                found.append((key, status, message))
        EnvironmentTestManager._coverage_diagnostic_checks(Collector(), {}, 'maestro_netlist', summary)
        self.assertTrue(any(key == 'coverage.points.missing' and status == 'FAIL' and 'RANGE_1' in msg for key, status, msg in found))
        self.assertTrue(any(key == 'coverage.points.unexpected' and status == 'FAIL' and 'BAD' in msg for key, status, msg in found))
        self.assertFalse(any('members_unavailable' in key for key, status, msg in found))
        for state, should_warn in [('unavailable', True), ('not_requested', False), (None, False)]:
            found[:] = []
            summary['contracts'][0]['native_schedule']['members_state'] = state
            EnvironmentTestManager._coverage_diagnostic_checks(Collector(), {}, 'maestro_netlist', summary)
            warns = [(key, status) for key, status, msg in found if 'members_unavailable' in key]
            self.assertEqual(bool(warns), should_warn)
            if should_warn:
                self.assertEqual(warns[0][1], 'WARN')
                self.assertEqual(diagnostic_code(warns[0][0], 'WARN'), 'CRESCENDO-COV-602')
                self.assertEqual(resolve_diagnostic_code('CRESCENDO-COV-602'), warns[0][0])

    def test_live_main_log_keeps_short_code_and_reason(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, 'main.log')
            with open(path, 'w'):
                pass
            manager = EnvironmentTestManager(None)
            row = {'config': {}, 'checks': {}, 'main_log': path, 'simulator': 'spectre',
                   'case_id': 'spectre.maestro_netlist', 'test_id': 'fixture',
                   'log_bytes': {path: 0}, 'queued_log_bytes': {path: 0},
                   'physical_log_tails': {path: ''}, 'log_limited': set(),
                   'write_error': None, 'report_state': {}, 'log_queue': queue.Queue()}
            writer = threading.Thread(target=manager._log_writer, args=(row,))
            writer.start()
            try:
                manager._coverage_diagnostic_checks(row, 'maestro_netlist', {'tests': [
                    {'test': 'Order_B', 'missing': ['DISCRETE']}]})
                row['log_queue'].join()
            finally:
                row['log_queue'].put(None)
                writer.join(5)
            self.assertIsNone(row['write_error'])
            with open(path, encoding='utf-8') as stream:
                log = stream.read()
            for text in ('CRESCENDO-COV-101', 'Order_B', 'DISCRETE', 'missing'):
                self.assertIn(text, log)

    def test_primary_stage_does_not_replace_first_cause(self):
        primary = {'failure_stage': 'native_capture_point_coverage', 'message': 'first exact cause'}
        p = describe_check({'id': 'execution', 'status': 'FAIL', 'details': {'first_cause': primary}})
        self.assertEqual(resolve_diagnostic_code(p['diagnostic_code']), 'execution.native_capture_point_coverage')
        self.assertIn(p['diagnostic_code'], p['status_label'])
        self.assertIn('first exact cause', p['explanation'])

    def test_real_coverage_fields_and_unknown_not_pass(self):
        coverage = {'ok': False, 'enabled_tests': ['a'], 'unexpected_tests': ['b'],
                    'native_group_enabled_difference': {'expected': ['c'], 'actual': []},
                    'test_differences': [{'test': 'a', 'expected': ['c'], 'actual': [], 'missing': ['c'],
                         'duplicate_points': [[1, 'a']], 'point_counts': {}, 'native_to_source_corner': {},
                         'variable_mismatches': [{'variable': 'x'}], 'sweep_differences': [1],
                         'unverified_variable_check_count': 1, 'mapping_error': 'ambiguous'}]}
        summary = _identity_coverage_summary({'capture/corner_identity.json': {'native_coverage': coverage,
                    'native_schedule': {'read_ok': True}}}, {})
        self.assertEqual(summary['status'], 'FAILED')
        self.assertEqual(summary['tests'][0]['duplicate_points'], [[1, 'a']])
        self.assertEqual(summary['contracts'][0]['unexpected_tests'], ['b'])
        found = []
        class Collector(object):
            def _check(self, row, key, status, message, details, engine):
                found.append((key, status))
        EnvironmentTestManager._coverage_diagnostic_checks(Collector(), {}, 'maestro_netlist', summary)
        expected = ('test_selection.unexpected', 'corner_mask.group', 'identity.mapping',
                    'parameters.variable', 'parameters.sweep', 'points.missing',
                    'points.duplicate', 'unknown.numeric')
        self.assertEqual(set(x[0] for x in found), set('coverage.' + x for x in expected))
        self.assertEqual(dict(found)['coverage.unknown.numeric'], 'WARN')
        self.assertEqual(dict(found)['coverage.points.missing'], 'FAIL')
        self.assertEqual(diagnostic_code('coverage.points.missing', 'FAIL'), 'CRESCENDO-COV-101')
        self.assertEqual(resolve_diagnostic_code('CRESCENDO-COV-101'), 'coverage.points.missing')
        point_summary = _identity_coverage_summary({'capture/capture_diagnostics.json': {'point_coverage': {
            'ok': False, 'missing_test_points': [{'test': 'a', 'point_id': 3, 'corner': 'c'}],
            'masked_test_points': [{'test': 'a', 'point_id': 4, 'corner': 'c'}]}}}, {})
        found[:] = []
        EnvironmentTestManager._coverage_diagnostic_checks(Collector(), {}, 'maestro_netlist', point_summary)
        self.assertIn(('coverage.points.rdb_missing', 'FAIL'), found)
        self.assertEqual(point_summary['point_coverage'][0]['masked_test_points'][0]['point_id'], 4)
        self.assertFalse(any('masked' in key for key, status in found))
        empty = _identity_coverage_summary({}, {'setup': {'tests': [{'name': 'a'}]}})
        self.assertEqual(empty['status'], 'NOT_VERIFIED')
        self.assertIsNone(empty['unverified_count'])

if __name__ == '__main__':
    unittest.main()
