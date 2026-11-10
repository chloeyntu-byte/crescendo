import copy
import json
import unittest

from crescendo_engine.environment_diagnostics import concise_case_records, concise_diagnostic_value


class CompactFailureTests(unittest.TestCase):
    def test_first_cause_and_independent_errors_keep_identity(self):
        records = [{'observation': 'diagnostic.first_cause', 'payload': {
            'first_cause': {'candidate_id': 'c1', 'error': 'readback timed out'},
            'secondary_cleanup': {'status': 'UNKNOWN', 'stop_proven': False}}},
            {'observation': 'execution.progress', 'payload': {'state': 'failed', 'candidates': [
                {'candidate_id': 'c1', 'tasks': [{'corner': 'TT', 'error': 'model missing', 'path': 'tt.log'}]},
                {'candidate_id': 'c2', 'tasks': [{'corner': 'SS', 'error': 'model missing', 'path': 'ss.log'}]}]}}]
        result = concise_case_records(records, {'state': 'failed', 'stop_proven': False})
        self.assertEqual(result[0], records[0])
        issues = result[1]['payload']['abnormal_context']
        self.assertEqual({(x['candidate_id'], x['corner'], x['path']) for x in issues if 'candidate_id' in x},
                         {('c1', 'TT', 'tt.log'), ('c2', 'SS', 'ss.log')})
        self.assertIs(result[-1]['payload']['stop_proven'], False)

    def test_unknown_stop_never_becomes_pass(self):
        record = {'observation': 'diagnostic.source_guard', 'payload': {'status': 'UNKNOWN', 'stop_proven': None}}
        result = concise_case_records([record], {'state': 'completed', 'stop_proven': None})
        self.assertEqual(result[0], record)
        self.assertIsNone(result[-1]['payload']['stop_proven'])
        self.assertNotIn('PASS', json.dumps(result))

    def test_nested_middle_failure_and_unknown_shape_survive(self):
        rows = [{'vendor': {'opaque': 'head'}}]
        rows.extend({'vendor': {'opaque': 'normal'}} for _ in range(20))
        rows.insert(10, {'vendor': {'failure': {'message': 'SFE-23 model section mismatch',
                                             'expected': 'TT', 'actual': 'FF', 'parameter': 'DTN_W'}}})
        rows.append({'vendor': {'opaque': 'tail'}})
        result = concise_diagnostic_value({'unfamiliar_records': rows})
        text = json.dumps(result)
        for required in ('head', 'tail', 'SFE-23', 'DTN_W', 'expected', 'TT', 'FF'):
            self.assertIn(required, text)
        self.assertIn('omitted', text)

    def test_unknown_error_dictionary_in_progress_is_not_path_only(self):
        records = [{'observation': 'execution.progress', 'payload': {'state': 'failed',
            'candidates': [{'candidate_id': 'C9', 'error': {'opaque_reason': 'new vendor condition',
                                                         'detail': {'binding': 'CLOAD'}}}]}}]
        text = json.dumps(concise_case_records(records, {'state': 'failed'}))
        for required in ('new vendor condition', 'CLOAD', 'C9'):
            self.assertIn(required, text)

    def test_same_as_is_original_evidence_reference_and_does_not_rebind(self):
        records = [{'observation': 'diagnostic.artifact', 'payload': {'path': 'c2/error.json',
            'data': {'candidate_id': 'c2', 'error': {'same_as': 'c1/error.json#data/error', 'sha256': 'abc'}}}}]
        text = json.dumps(concise_case_records(records, {'state': 'failed'}))
        self.assertNotIn('"same_as"', text)
        self.assertIn('original_evidence_reference', text)
        self.assertIn('c1/error.json#data/error', text)
        self.assertIn('c2/error.json', text)
        self.assertIn('abc', text)

    def test_full_is_exact_and_inputs_are_unchanged(self):
        records = [{'observation': 'execution.progress', 'payload': {'state': 'running',
            'candidates': [{'candidate_id': 'c1', 'tasks': [{'state': 'completed'}]}]}}] * 3
        case, checks = {'state': 'failed'}, [{'status': 'FAIL'}]
        before = copy.deepcopy((records, case, checks))
        concise_case_records(records, case, checks)
        self.assertEqual((records, case, checks), before)
        self.assertIs(concise_case_records(records, case, checks, full=True), records)

    def test_progress_repeats_compress_without_optimization_arrays(self):
        records = [{'observation': 'execution.progress', 'payload': {'state': 'running',
            'execution_stage': 'solve', 'message': 'running {}'.format(i),
            'candidates': [{'candidate_id': 'c1', 'tasks': [{'solver_command_plan': {'argv': ['spectre', '/long/' * 500]}}]}],
            'population': ['PRIVATE_OPTIMIZATION_VALUE'] * 30}} for i in range(40)]
        records.append({'observation': 'execution.progress', 'payload': {'state': 'failed', 'message': 'solver failed'}})
        result = concise_case_records(records, {'state': 'failed'})
        self.assertLess(len(json.dumps(result)), len(json.dumps(records)) * .1)
        self.assertNotIn('PRIVATE_OPTIMIZATION_VALUE', json.dumps(result))
        self.assertEqual(result[0]['repeated_observations'], 40)
        self.assertEqual(result[0]['last_message'], 'running 39')
        self.assertEqual(result[1]['payload']['state'], 'failed')

    def test_success_reuses_existing_success_projection(self):
        records = [{'observation': 'diagnostic.identity', 'payload': {'version': 'test'}},
                   {'observation': 'diagnostic.log_context', 'payload': {'text': 'Spectre fixture log beginning'}}]
        result = concise_case_records(records, {'state': 'completed', 'stop_proven': True})
        self.assertIn('concise_success', json.dumps(result))
        self.assertNotIn('Spectre fixture log beginning', json.dumps(result))

    def test_optimization_array_keeps_abnormal_context_without_normal_values(self):
        value = {'objectives': [{'value': 'PRIVATE_OPTIMIZATION_VALUE'},
                               {'output_key': 'Q_11G', 'error': 'missing_output', 'corner': 'SS'}]}
        result = json.dumps(concise_diagnostic_value(value))
        self.assertNotIn('PRIVATE_OPTIMIZATION_VALUE', result)
        for required in ('missing_output', 'Q_11G', 'SS'):
            self.assertIn(required, result)

    def test_failed_command_keeps_actual_argument_value(self):
        value = {'returncode': 2, 'error': 'invalid model section',
                 'argv': ['spectre', 'input.scs', '-section', 'nonexistent_TT']}
        result = concise_diagnostic_value(value)
        self.assertEqual(result['argv']['failure_arguments'], value['argv'])

    def test_completed_subtree_preserves_mixed_stop_and_source_flags(self):
        value = {'status': 'completed', 'points': [
            {'point_id': 1, 'stop_proven': False, 'source_unchanged': False},
            {'point_id': 2, 'stop_proven': True, 'source_unchanged': True}],
            'parsed': {'normal_output': ['large ordinary result'] * 100}}
        result = concise_diagnostic_value(value)
        flag = result['qualification_flags']['points.[].stop_proven']
        self.assertEqual(flag, {'values': [False, True], 'true_count': 1, 'false_count': 1})
        self.assertTrue(any(x.get('stop_proven') is False and x.get('point_id') == 1
                            for x in result['abnormal_context']))
        self.assertTrue(any(x.get('source_unchanged') is False for x in result['abnormal_context']))
        self.assertNotIn('large ordinary result', json.dumps(result))

    def test_completed_parent_keeps_false_and_unknown_nested_qualification(self):
        value = {'status': 'completed', 'proofs': [
            {'point_id': 1, 'ok': False, 'verified': None, 'process_termination_proven': False}],
            'parsed': ['ordinary output'] * 100}
        result = concise_diagnostic_value(value)
        issue = next(row for row in result['abnormal_context'] if row.get('point_id') == 1)
        self.assertIs(issue['ok'], False)
        self.assertIsNone(issue['verified'])
        self.assertIs(issue['process_termination_proven'], False)

    def test_failed_parent_is_not_success_despite_successful_child(self):
        value = {'status': 'failed', 'ok': False, 'error': 'source mismatch',
                 'completion': {'ok': True, 'points': [{'point_id': i} for i in range(100)]}}
        result = concise_diagnostic_value(value)
        self.assertEqual(result['status'], 'failed')
        self.assertIs(result['ok'], False)
        self.assertNotIn('successful_subtree_summary', result)
        self.assertTrue(result['completion']['successful_subtree_summary'])
        self.assertEqual(result['error'], 'source mismatch')

    def test_successful_candidate_inside_failed_case_compresses_proofs(self):
        records = [{'observation': 'diagnostic.artifact', 'payload': {
            'path': 'candidates/g000_c0000/point_identity.json', 'identity': {'sha256': 'rawhash'},
            'data': {'points': [{'point_id': i, 'corner': 'TT', 'test': 'T',
                                'parameters': ['PRIVATE_NORMAL_VALUES'] * 20} for i in range(10)]}}},
            {'observation': 'diagnostic.first_cause', 'payload': {
                'candidate_id': 'g000_c0001', 'error': 'startup deadline exceeded'}}]
        checks = [{'id': 'spectre.baseline.g000_c0000.completion_evidence', 'status': 'PASS'}]
        result = concise_case_records(records, {'state': 'failed', 'stop_proven': False}, checks)
        text = json.dumps(result)
        for required in ('startup deadline exceeded', 'rawhash', 'g000_c0000', 'TT'):
            self.assertIn(required, text)
        self.assertNotIn('PRIVATE_NORMAL_VALUES', text)
        self.assertEqual(result[-1]['payload']['state'], 'failed')

    def test_completed_candidate_console_error_is_kept(self):
        record = {'observation': 'diagnostic.log_context', 'payload': {
            'path': 'candidates/g000_c0000/ocean.stdout',
            'context': {'lines': [{'line': i, 'text': 'routine context' * 10} for i in range(20)] +
                                [{'line': 21, 'text': 'ERROR independent source violation'}]}}}
        checks = [{'id': 'spectre.baseline.g000_c0000.completion_evidence', 'status': 'PASS'}]
        text = json.dumps(concise_case_records([record], {'state': 'failed'}, checks))
        self.assertIn('ERROR independent source violation', text)
        self.assertIn('ocean.stdout', text)

    def test_long_unknown_text_retains_error_neighbors_and_boundaries(self):
        text = '\n'.join(['first context'] + ['routine'] * 250 +
                         ['before binding', 'ERROR vendor binding mismatch DTN_W', 'after binding'] +
                         ['routine'] * 250 + ['last context'])
        result = json.dumps(concise_diagnostic_value({'vendor_console': text}))
        for required in ('first context', 'last context', 'before binding', 'DTN_W', 'after binding', 'sha256'):
            self.assertIn(required, result)


if __name__ == '__main__':
    unittest.main()
