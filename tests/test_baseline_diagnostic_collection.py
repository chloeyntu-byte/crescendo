import unittest
import json
import os
import tempfile
from unittest import mock

from crescendo_engine.environment_diagnostics import _windows


class OceanDiagnosticWindowTest(unittest.TestCase):
    def test_runtime_events_without_error_and_coverage(self):
        lines = ['Loading module %d' % i for i in range(6806)]
        lines[130] = 'Submitted job ID: 48312 to queue short'
        lines[200] = 'ICRP worker starting on host node12'
        lines[201] = lines[200]
        lines[300] = 'job policy = 1'
        context = _windows('\n'.join(lines), console_source='ocean.stdout')
        retained = [row['text'] for row in context['lines']]
        self.assertIn(lines[130], retained)
        self.assertIn(lines[200], retained)
        self.assertEqual(retained.count(lines[200]), 1)
        self.assertNotIn(lines[300], retained)
        self.assertEqual(context['error_count'], 0)
        self.assertEqual(context['semantic_event_repeated'], 1)
        self.assertEqual(context['lines_total'], 6806)
        self.assertEqual(context['lines_retained'] + context['lines_omitted'], 6806)
        self.assertFalse(context['complete_text_included'])
        self.assertEqual(context['line_count_scope'], 'full_read_text')

    def test_bounded_first_error_and_tail_even_many_events(self):
        lines = ['startup worker event %d' % i for i in range(400)]
        lines[210] = '*Error* first actual error'
        context = _windows('\n'.join(lines), max_lines=48, console_source='ocean.stderr')
        numbers = [row['line'] for row in context['lines']]
        self.assertLessEqual(len(numbers), 48)
        self.assertIn(1, numbers)
        self.assertIn(211, numbers)
        self.assertIn(400, numbers)
        self.assertGreater(context['semantic_event_omitted'], 0)

    def test_partial_read_scope_does_not_claim_complete(self):
        context = _windows('head\n[diagnostic byte gap: middle omitted]\ntail', console_source='ocean.stdout')
        self.assertEqual(context['line_count_scope'], 'read_excerpt_only')
        self.assertFalse(context['complete_text_included'])


class NativeCollectorIntegrationTest(unittest.TestCase):
    def test_budget_identity_and_evidence_are_reported(self):
        from crescendo_engine.environment_diagnostics import collect_run_diagnostics
        with tempfile.TemporaryDirectory() as root:
            control = os.path.join(root, 'candidates', 'g000_c0000', 'native_baseline')
            os.makedirs(control)
            with open(os.path.join(control, 'native_baseline_state.json'), 'w') as stream:
                json.dump({'operation_budgets': {'candidate_seconds': 600}}, stream)
            with open(os.path.join(control, 'run.il'), 'w') as stream:
                stream.write('crescBaselineRecord("RUN_BEGIN" "")\n')
            result = {'status': 'partial', 'manifest': {'native_readback_verified': False},
                      'files_read': 2, 'read_bytes': 37,
                      'records': [{'path': 'native_storage/Job1/run.log', 'kind': 'text_log',
                                   'text': 'Submitted job ID: 48312\n', 'truncated': False}],
                      'omissions': [{'reason': 'unverified_readback'}]}
            with mock.patch('crescendo_engine.native_diagnostic_evidence.collect_native_storage_evidence',
                            return_value=result) as collect:
                records = collect_run_diagnostics(root, 'maestro_baseline')
            self.assertEqual(collect.call_args[0][0], os.path.dirname(control))
            self.assertGreater(collect.call_args[0][1], 37)
            by_kind = {}
            for record in records:
                by_kind.setdefault(record['observation'], []).append(record['payload'])
            self.assertEqual(by_kind['diagnostic.native_storage_collection'][0]['status'], 'partial')
            self.assertTrue(by_kind['diagnostic.native_storage_evidence'][0]['diagnostic_only'])
            self.assertEqual(by_kind['diagnostic.native_baseline_runtime'][0]['operation_budgets']['candidate_seconds'], 600)
            collection = by_kind['diagnostic.collection'][0]
            self.assertEqual(collection['status'], 'PARTIAL')
            self.assertGreaterEqual(collection['files_read'], 4)
            self.assertGreaterEqual(collection['read_bytes'], 37)
            self.assertTrue(any(x.get('reason') == 'unverified_readback' for x in collection['omitted_or_truncated']))
            self.assertTrue(any(x.get('path', '').endswith('run.il') for x in by_kind['diagnostic.script_context']))

    def test_native_samples_keep_offsets_and_missing_middle(self):
        from crescendo_engine.environment_diagnostics import collect_run_diagnostics
        with tempfile.TemporaryDirectory() as root:
            control = os.path.join(root, 'candidates', 'g000_c0000', 'native_baseline')
            os.makedirs(control)
            with open(os.path.join(control, 'native_baseline_state.json'), 'w') as stream:
                json.dump({}, stream)
            sample = {'path': 'native_storage/run.log', 'kind': 'text_log',
                'text': 'duplicated head', 'truncated': True, 'hash_scope': 'not_computed',
                'unscanned_ranges': [{'start': 20, 'end': 180}],
                'samples': [{'offset': 0, 'length': 20, 'text': 'ICRP worker started'},
                            {'offset': 180, 'length': 20, 'text': 'Job ID: 42 failed'}]}
            response = {'status': 'partial', 'manifest': {}, 'files_read': 6, 'read_bytes': 40,
                        'records': [sample], 'omissions': [{'reason': 'file_partially_scanned'}]}
            with mock.patch('crescendo_engine.native_diagnostic_evidence.collect_native_storage_evidence', return_value=response):
                result = collect_run_diagnostics(root, 'maestro_baseline')
            evidence = next(r['payload'] for r in result if r['observation'] == 'diagnostic.native_storage_evidence')
            self.assertNotIn('text', evidence)
            self.assertEqual([x['offset'] for x in evidence['sample_contexts']], [0, 180])
            self.assertEqual(evidence['unscanned_ranges'][0]['end'], 180)
            self.assertTrue(all(not x['context']['complete_text_included'] for x in evidence['sample_contexts']))
            self.assertIn('Job ID: 42 failed', str(evidence['sample_contexts'][1]))

    def test_large_native_and_multiple_candidates_reserve_ocean_first_error(self):
        from crescendo_engine.environment_diagnostics import collect_run_diagnostics
        with tempfile.TemporaryDirectory() as root:
            for name in ('g000_c0000', 'g000_c0001'):
                control = os.path.join(root, 'candidates', name, 'native_baseline')
                os.makedirs(control)
                with open(os.path.join(control, 'native_baseline_state.json'), 'w') as stream:
                    json.dump({}, stream)
                with open(os.path.join(control, 'ocean.stdout'), 'w') as stream:
                    stream.write('Loading startup\n' * 3000)
                    stream.write('prefix ' + ('x' * 1200) + ' *Error* decisive ocean cause\n')
                    stream.write('Loading end\n' * 3000)
            records = [{'path': 'native_storage/%d/run.log' % i, 'kind': 'text_log',
                        'text': ('ICRP worker startup %d ' % i + 'x' * 250 + '\n') * 20,
                        'truncated': False} for i in range(60)]
            calls = []
            def collector(candidate, max_bytes, max_files, deadline, cancelled=None):
                calls.append((max_bytes, max_files))
                return {'status': 'ok', 'manifest': {}, 'files_read': 32,
                        'read_bytes': 2 * 1024 * 1024, 'records': records if len(calls) == 2 else [], 'omissions': []}
            with mock.patch('crescendo_engine.native_diagnostic_evidence.collect_native_storage_evidence', side_effect=collector):
                result = collect_run_diagnostics(root, 'maestro_baseline')
            self.assertEqual(calls, [(4 * 1024 * 1024, 64), (2 * 1024 * 1024, 32)])
            contexts = [r['payload']['context'] for r in result if r['observation'] == 'diagnostic.log_context'
                        and r['payload'].get('path', '').endswith('ocean.stdout')]
            self.assertEqual(len(contexts), 2)
            for context in contexts:
                errors = [line for line in context['lines'] if 'decisive ocean cause' in line['text']]
                self.assertEqual(len(errors), 1)
                self.assertTrue(errors[0]['text_truncated'])
                self.assertGreater(context['lines_with_truncated_text'], 0)
            native = [r for r in result if r['observation'].startswith('diagnostic.native_storage_')]
            self.assertLessEqual(sum(len(json.dumps(r['payload'], ensure_ascii=True)) for r in native), 128 * 1024)


if __name__ == '__main__':
    unittest.main()
