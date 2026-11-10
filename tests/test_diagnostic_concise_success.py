import copy
import unittest

from crescendo_engine.environment_diagnostics import concise_success_records


class ConciseSuccessTests(unittest.TestCase):
    def setUp(self):
        self.case = {'state': 'completed', 'stop_proven': True}
        self.records = [
            {'observation': 'diagnostic.identity', 'payload': {'version': '2.6.9'}},
            {'observation': 'diagnostic.settings', 'payload': {'rows': ['model'] * 300}},
            {'observation': 'diagnostic.collection', 'payload': {'status': 'PARTIAL',
                'files': ['large inventory'], 'omitted_or_truncated': [{'reason': 'read budget'}]}},
        ]

    def test_success_keeps_identity_and_omissions_without_mutating_evidence(self):
        before = copy.deepcopy(self.records)
        result = concise_success_records(self.records, self.case)
        self.assertEqual(self.records, before)
        self.assertEqual(result[0], self.records[0])
        self.assertEqual(result[1]['payload']['status'], 'PARTIAL')
        self.assertEqual(result[1]['payload']['omitted_or_truncated'],
                         self.records[2]['payload']['omitted_or_truncated'])
        self.assertNotIn('files', result[1]['payload'])
        self.assertEqual(result[-1]['payload']['suppressed_record_count'], 1)

    def test_failure_unknown_stop_and_first_cause_remain_full(self):
        for case in ({}, {'state': 'failed', 'stop_proven': True},
                     {'state': 'completed'}, dict(self.case, first_cause={'message': 'error'})):
            self.assertIs(concise_success_records(self.records, case), self.records)

    def test_failed_checks_and_actionable_warning_keep_full_evidence(self):
        for status in ('FAIL', 'BLOCKED', 'CANCELLED', 'WARN'):
            self.assertIs(concise_success_records(self.records, self.case,
                [{'id': 'x.execution', 'status': status}]), self.records)

    def test_collection_warning_still_allows_success_summary(self):
        result = concise_success_records(self.records, self.case,
            [{'id': 'x.diagnostics.collection', 'status': 'WARN'}])
        self.assertEqual(result[-1]['payload']['mode'], 'concise_success')

    def test_full_override_preserves_all(self):
        self.assertIs(concise_success_records(self.records, self.case, full=True), self.records)

    def test_interned_core_proof_never_becomes_dangling_reference(self):
        self.records[0]['payload'] = {'value': {'same_as': 'removed.json#data'}}
        self.assertIs(concise_success_records(self.records, self.case), self.records)

    def test_native_fallback_and_worker_failures_are_retained(self):
        for record in [
            {'observation': 'diagnostic.artifact', 'payload': {
                'path': 'private_netlist/native_generation_required.json',
                'data': {'template_qualified': False, 'selected_parameter_bindings': ['DTN_W']}}},
            {'observation': 'diagnostic.native_worker_diagnostics', 'payload': {'first_error': 'native error'}},
            {'observation': 'diagnostic.producer_failure', 'payload': {'failure_stage': 'native_selected_parameter_binding'}},
        ]:
            self.records.append(record)
            self.assertIn(record, concise_success_records(self.records, self.case))
