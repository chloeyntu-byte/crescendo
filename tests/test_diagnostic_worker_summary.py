# -*- coding: utf-8 -*-
import copy
import unittest
from crescendo_engine.diagnosis_catalog import worker_error_observation, format_ladder_summary
from crescendo_engine.gui import _diagnostic_ladder_text


def fixture():
    return {'native_worker_diagnostics': [{
        'schema_version': 'crescendo.native_worker_diagnostics.v1', 'ok': False,
        'owned_workspace': '/owned/session/workspace', 'parent_capture_failed': True,
        'expected_wrapper_aftermath_samples': [{'message': 'ASSEMBLER-5031'}],
        'logs': [{'path': '/owned/session/workspace/logs_root/logs0/Job1.log', 'fresh': True,
                  'generation_error_count': 2, 'error_lines': [
                      {'line': 181, 'text': r'\e *Error* Could not create directory: /owned/session/workspace/simulation/TB.'},
                      {'line': 183, 'text': r'\o ERROR (ADE-5036): Unable to create directory /owned/session/workspace/simulation/TB.'}]}]}]}


class WorkerSummaryTests(unittest.TestCase):
    def test_actual_structured_error_in_summary_and_gui_not_generic_replacement(self):
        context = fixture()
        case = {'case_id': 'spectre.maestro_netlist', 'engine': 'maestro_netlist', 'simulator': 'spectre',
                'state': 'failed', 'first_cause': {'failure_stage': 'native_capture_worker_generation_error',
                                                   'error': 'native capture failed; see ocean.stdout'}}
        contexts = {case['case_id']: context}
        original = copy.deepcopy(case)
        summary = format_ladder_summary([case], 'spectre', 'spectre', {'w': 1}, None, contexts)
        gui = _diagnostic_ladder_text({'case_results': [case], 'case_contexts': contexts})
        for result in (summary, gui):
            self.assertIn('native capture failed; see ocean.stdout', result)
            self.assertIn('Job1.log:181', result)
            self.assertIn('Could not create directory', result)
            self.assertIn('辅助证据', result)
            self.assertNotIn('ASSEMBLER-5031', result)
        self.assertEqual(case, original)

    def test_stale_wrong_schema_invalid_path_and_malformed_are_ignored(self):
        for change in ('stale', 'schema', 'outside', 'owner', 'line', 'ownership', 'logs'):
            context = fixture(); report = context['native_worker_diagnostics'][0]; log = report['logs'][0]
            if change == 'stale': log['fresh'] = False
            elif change == 'schema': report['schema_version'] = 'unrecognized'
            elif change == 'outside': log['path'] = '/unrelated/Job1.log'
            elif change == 'owner': report['owned_workspace'] = None
            elif change == 'line': log['error_lines'][0]['line'] = None; log['error_lines'][1]['line'] = None
            elif change == 'ownership': log['ownership_proven'] = False
            elif change == 'logs': report['logs'] = 'bad'
            self.assertIsNone(worker_error_observation(context), change)

    def test_wrapper_is_skipped_and_message_and_path_are_bounded(self):
        context = fixture(); log = context['native_worker_diagnostics'][0]['logs'][0]
        log['error_lines'].insert(0, {'line': 3, 'text': 'ERROR (ASSEMBLER-5031): expected wrapper'})
        log['error_lines'][1]['text'] = 'real error ' + 'x' * 3000
        log['path'] = '/owned/session/workspace/' + 'd' * 900 + '/Job1.log'
        result = worker_error_observation(context)
        self.assertEqual(result['line'], 181)
        self.assertLessEqual(len(result['path']), 600)
        self.assertLessEqual(len(result['text']), 321)
        self.assertTrue(result['path'].endswith('/Job1.log'))
        self.assertTrue(result['text'].endswith('…'))


if __name__ == '__main__':
    unittest.main()
