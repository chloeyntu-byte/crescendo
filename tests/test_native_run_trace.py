from __future__ import print_function

import os
import tempfile
import unittest

from crescendo_engine.native_diagnostics import native_run_trace_lines, skill_command_diagnostics
from crescendo_engine.workspace import bounded_skill_body_lines


class NativeRunTraceTest(unittest.TestCase):
    def setUp(self):
        self.original = [
            'runId=maeRunSimulation(?waitUntilDone t ?returnRunId t ?session session)',
            "maeWaitUntilDone('All ?session session)",
        ]

    def test_normal_run_keeps_native_caught_error_policy_without_diagnostic_traces(self):
        result = native_run_trace_lines(self.original)
        text = '\n'.join(result)
        for form in self.original:
            self.assertEqual(result.count(form), 1)
        self.assertIn('sstatus(errsetTrace nil)', result)
        self.assertIn(') sstatus(errsetTrace crescRunSavedErrsetTrace))', result)
        self.assertNotIn('tracef(', text)
        self.assertNotIn('sstatus(debugMode', text)
        rendered, metadata = bounded_skill_body_lines(result, max_forms=1)
        self.assertEqual(metadata['block_count'], 1)
        self.assertIn('  crescSkillBodyResult = errset(progn(', rendered)
        self.assertIn('  ) t)', rendered)
        self.assertIsNot(result, self.original)

    def test_original_run_and_wait_have_one_execution_site(self):
        result = '\n'.join(native_run_trace_lines(self.original, enabled=True))
        for form in self.original:
            self.assertEqual(result.count(form), 1)
        self.assertIn('tracef(readstring)', result)
        self.assertIn('tracef(linereadstring)', result)
        self.assertNotIn('tracef(t)', result)
        self.assertNotIn('untrace(t)', result)
        self.assertNotIn('status(tracelength)', result)
        self.assertIn("unless(memq('readstring crescReaderOldFunctions)", result)
        self.assertIn('ptport=crescReaderOldPort', result)
        self.assertNotIn('installDebugger', result)
        self.assertNotIn('sstatus(stacktraceDump t)', result)
        self.assertIn('sstatus(stacktrace 32)', result)
        self.assertNotIn('sstatus(errsetTrace t)', result)
        self.assertIn('crescRunSavedErrsetTrace=status(errsetTrace)', result)
        self.assertIn(') sstatus(errsetTrace crescRunSavedErrsetTrace))', result)
        self.assertIn('unwindProtect(', result)
        self.assertIn('CRESCENDO_NATIVE_RUN_STAGE Run BEGIN', result)
        self.assertIn('CRESCENDO_NATIVE_RUN_STAGE Wait RETURN', result)
        self.assertLess(result.index('sstatus(errsetTrace nil)'),
                        result.index('crescNativeTraceSetup=errset('))
        self.assertLess(result.index('crescNativeTraceSetup=errset('),
                        result.index(self.original[0]))

    def test_setup_probes_and_public_traces_are_diagnostic_only(self):
        normal = '\n'.join(native_run_trace_lines(self.original))
        diagnostic = '\n'.join(native_run_trace_lines(self.original, enabled=True))
        self.assertNotIn('CRESCENDO_NATIVE_SETUP_OBSERVATION', normal)
        for name in ('axlGetMainSetupDB', 'axlGetActiveSetup', 'maeRunSimulation'):
            self.assertIn('tracef(%s)' % name, diagnostic)
            self.assertIn('untrace(%s)' % name, diagnostic)
            self.assertIn("memq('%s crescReaderOldFunctions)" % name, diagnostic)
        self.assertLess(diagnostic.index('CRESCENDO_NATIVE_SETUP_OBSERVATION before_run'), diagnostic.index(self.original[0]))
        self.assertLess(diagnostic.index(self.original[0]), diagnostic.index('CRESCENDO_NATIVE_SETUP_OBSERVATION after_run'))
        self.assertLess(diagnostic.index('CRESCENDO_NATIVE_SETUP_OBSERVATION after_run'), diagnostic.index('RunIdCheck BEGIN'))

    def test_enabled_rejects_unknown_multiline_or_missing_stages(self):
        for invalid in ([], self.original[:1], self.original + ['other()'],
                        [self.original[0] + '\nother()', self.original[1]],
                        [None, self.original[1]]):
            with self.assertRaises(ValueError):
                native_run_trace_lines(invalid, enabled=True)

    def test_bounded_capture_does_not_split_the_protected_run_window(self):
        wrapped = native_run_trace_lines(self.original, enabled=True)
        rendered, metadata = bounded_skill_body_lines(wrapped, max_forms=1)
        self.assertEqual(metadata['expression_count'], 1)
        self.assertEqual(metadata['block_count'], 1)
        self.assertIn(self.original[0], rendered)


class NativeRunTraceEvidenceTest(unittest.TestCase):
    def collect(self, stdout, stderr=''):
        with tempfile.TemporaryDirectory() as root:
            script = os.path.join(root, 'capture.il')
            with open(script, 'w') as stream:
                stream.write('sstatus(errsetTrace t)\n')
            for name, text in [('ocean.stdout', stdout), ('ocean.stderr', stderr)]:
                with open(os.path.join(root, name), 'w') as stream:
                    stream.write(text)
            return skill_command_diagnostics({'returncode': 0}, script,
                required_marker='CAPTURE_DONE', owned_root=root)

    def test_run_preerror_context_is_bounded_filtered_and_fatal(self):
        lines = ['CRESCENDO_NATIVE_SETUP_OBSERVATION before_run session=1 mainSDB=2 resolved=2 active=3 resolved_ok=t active_ok=t',
                 'CRESCENDO_NATIVE_RUN_STAGE Run BEGIN']
        lines += ['line %d' % i for i in range(100)]
        lines += ['|[3]readstring("SECRET")', '|[3]readstring --> SECRET',
                  'ERROR (ASSEMBLER-2404): Cannot find a setup database entry for handle 0.',
                  'CRESCENDO_NATIVE_RUN_STAGE Run RETURN', 'CAPTURE_DONE']
        report = self.collect('\n'.join(lines))
        self.assertFalse(report['ok'])
        self.assertEqual(report['first_error']['native_code'], 'ASSEMBLER-2404')
        evidence = report['native_run_preerror'][0]
        self.assertEqual(len(evidence['lines']), 80)
        self.assertEqual(evidence['reader_trace_lines_filtered'], 2)
        self.assertTrue(evidence['truncated'])
        self.assertGreater(evidence['earlier_lines_omitted'], 0)
        self.assertNotIn('SECRET', str(evidence))
        self.assertEqual(report['native_setup_observations'][0]['line'], 1)

    def test_history_evidence_preserves_source_line_and_first_error(self):
        report = self.collect('\n'.join([
            '*Error* upstream Run failed',
            'CRESCENDO_NATIVE_RUN_VALUE runId 7',
            'CRESCENDO_NATIVE_RUN_STAGE HistoryLookup BEGIN',
            'CRESCENDO_NATIVE_RUN_VALUE history 0',
            '*Error* Crescendo native capture history unavailable',
            'CAPTURE_DONE']))
        evidence = report['native_run_evidence']
        self.assertEqual(evidence[0], {'source': 'ocean.stdout', 'line': 2,
                                      'value_name': 'runId', 'value': '7'})
        self.assertEqual(evidence[1]['stage'], 'HistoryLookup')
        self.assertEqual(evidence[2]['value'], '0')
        self.assertIn('upstream Run failed', report['first_error']['message'])

    def test_error_retains_actual_reader_argument_and_Run_boundary(self):
        report = self.collect('\n'.join([
            'CRESCENDO_NATIVE_RUN_TRACE ENABLED frames=32 locals=off arguments=evaluated best_effort=t',
            'CRESCENDO_NATIVE_RUN_STAGE Run BEGIN',
            '*Error* lineread/read: syntax error encountered in input',
            '<<< Stack Trace >>>', 'readstring("UNIQUE_FAILED_INPUT(")',
            '(... in maeRunSimulation ...)', 'CRESCENDO_NATIVE_RUN_TRACE RESTORE DONE', 'CAPTURE_DONE']))
        self.assertFalse(report['ok'])
        self.assertEqual(report['first_error']['native_run_boundary']['stage'], 'Run')
        self.assertIs(report['first_error']['native_run_boundary']['arguments_enabled'], True)
        context = report['native_error_contexts'][0]
        self.assertEqual(context['following_line_limit'], 40)
        self.assertTrue(any('UNIQUE_FAILED_INPUT(' in row['text'] for row in context['lines']))
        self.assertEqual(report['native_run_trace'][0]['restore_status'], 'DONE')

    def test_Wait_error_is_not_mislabeled_as_Run(self):
        report = self.collect('\n'.join([
            'CRESCENDO_NATIVE_RUN_TRACE ENABLED frames=32 locals=off arguments=evaluated best_effort=t',
            'CRESCENDO_NATIVE_RUN_STAGE Run BEGIN', 'CRESCENDO_NATIVE_RUN_STAGE Run RETURN',
            'CRESCENDO_NATIVE_RUN_STAGE Wait BEGIN', '*Error* native_wait_failure',
            'CRESCENDO_NATIVE_RUN_TRACE RESTORE DONE', 'CAPTURE_DONE']))
        self.assertEqual(report['first_error']['native_run_boundary']['stage'], 'Wait')
        self.assertFalse(report['ok'])

    def test_other_console_and_pre_Run_error_do_not_borrow_boundary(self):
        report = self.collect('CRESCENDO_NATIVE_RUN_STAGE Run BEGIN\nCAPTURE_DONE\n',
                              '*Error* independent_stderr_failure\n')
        self.assertNotIn('native_run_boundary', report['first_error'])
        report = self.collect('*Error* before_Run\nCRESCENDO_NATIVE_RUN_STAGE Run BEGIN\nCAPTURE_DONE\n')
        self.assertNotIn('native_run_boundary', report['first_error'])

    def test_trace_unavailable_is_visible_and_does_not_create_an_error(self):
        report = self.collect('\n'.join([
            'CRESCENDO_NATIVE_RUN_TRACE UNAVAILABLE original_Run_will_execute_once',
            'CRESCENDO_NATIVE_RUN_STAGE Run BEGIN', 'CRESCENDO_NATIVE_RUN_STAGE Run RETURN',
            'CRESCENDO_NATIVE_RUN_STAGE Wait BEGIN', 'CRESCENDO_NATIVE_RUN_STAGE Wait RETURN',
            'CRESCENDO_NATIVE_RUN_TRACE RESTORE DONE', 'CAPTURE_DONE']))
        self.assertTrue(report['ok'])
        self.assertIs(report['native_run_trace'][0]['arguments_enabled'], False)
        self.assertEqual(report['native_run_trace'][0]['availability'], 'UNAVAILABLE')

    def test_successful_trace_does_not_replace_required_completion_marker(self):
        report = self.collect('CRESCENDO_NATIVE_RUN_TRACE ENABLED frames=32 locals=off arguments=evaluated best_effort=t\n'
                              'CRESCENDO_NATIVE_RUN_STAGE Run BEGIN\nCRESCENDO_NATIVE_RUN_STAGE Run RETURN\n')
        self.assertFalse(report['ok'])
        self.assertFalse(report['native_error'])

    def test_cleanup_after_restore_does_not_borrow_failed_Run_boundary(self):
        report = self.collect('\n'.join([
            'CRESCENDO_NATIVE_RUN_TRACE ENABLED frames=32 locals=off arguments=evaluated best_effort=t',
            'CRESCENDO_NATIVE_RUN_STAGE Run BEGIN', '*Error* actual_Run_failure',
            'CRESCENDO_NATIVE_RUN_TRACE RESTORE DONE', '*Error* cleanup_failure', 'CAPTURE_DONE']))
        self.assertEqual(report['errors'][0]['native_run_boundary']['stage'], 'Run')
        self.assertNotIn('native_run_boundary', report['errors'][1])

    def reader_case(self, lines, stage='Run'):
        return self.collect('\n'.join([
            'CRESCENDO_NATIVE_READER_TRACE ENABLED public=readstring,linereadstring private_parser_coverage=unknown',
            'CRESCENDO_NATIVE_RUN_STAGE %s BEGIN' % stage] + lines + [
            'CRESCENDO_NATIVE_READER_TRACE RESTORE DONE', 'CAPTURE_DONE']))

    def test_public_reader_failed_entry_is_retained_before_protected_stack(self):
        report = self.reader_case(['|[9]linereadstring("FAILED_INPUT(")',
            '*Error* lineread/read: syntax error encountered in input',
            '(... in _axlRunAllTestsWithCallback ...)'])
        evidence = report['first_error']['native_reader_call']
        self.assertEqual(evidence['state'], 'preceding_unreturned_call')
        self.assertEqual(evidence['function'], 'linereadstring')
        self.assertIn('FAILED_INPUT(', evidence['raw_trace_entry'])
        self.assertEqual(evidence['distance_lines'], 1)
        self.assertEqual(evidence['qualification'], 'diagnostic_only')
        self.assertTrue(evidence['private_parser_may_bypass'])
        self.assertFalse(report['ok'])

    def test_successful_reader_values_are_counted_without_dump(self):
        report = self.reader_case(['|[3]readstring("PRIVATE_SUCCESS_VALUE")',
            '|[3]readstring --> PRIVATE_SUCCESS_VALUE'])
        self.assertTrue(report['ok'])
        self.assertEqual(report['native_reader_trace'][0]['entry_count'], 1)
        self.assertEqual(report['native_reader_trace'][0]['return_count'], 1)
        self.assertNotIn('PRIVATE_SUCCESS_VALUE', str(report))

    def test_successful_return_is_not_misattributed_to_later_parser_failure(self):
        report = self.reader_case(['|[3]readstring("SUCCESS")', '|[3]readstring --> SUCCESS',
            '*Error* lineread/read: syntax error encountered in input'])
        self.assertEqual(report['first_error']['native_reader_call']['state'], 'not_observed')

    def test_abbreviated_and_oversized_reader_arguments_are_explicit(self):
        for entry, field in [('|[3]readstring(...)', 'argument_abbreviated'),
                             ('|[3]readstring("' + 'x'*4096 + '")', 'truncated')]:
            report = self.reader_case([entry, '*Error* lineread/read: syntax error encountered in input'])
            evidence = report['first_error']['native_reader_call']
            self.assertTrue(evidence[field])
            self.assertEqual(evidence['argument_completeness'], 'abbreviated_or_truncated')
            self.assertLessEqual(len(evidence['raw_trace_entry']), 2048)

    def test_nested_readers_keep_nearest_unreturned_entry(self):
        report = self.reader_case(['|[3]readstring("OUTER")', '|[4]linereadstring("INNER(")',
            '*Error* lineread/read: syntax error encountered in input'])
        self.assertEqual(report['first_error']['native_reader_call']['stack_depth'], 4)

    def test_old_failed_call_is_replaced_by_next_call_at_same_depth(self):
        report = self.reader_case(['|[3]readstring("EARLIER(")', '|[3]readstring("CURRENT(")',
            '*Error* lineread/read: syntax error encountered in input'])
        self.assertIn('CURRENT(', report['first_error']['native_reader_call']['raw_trace_entry'])

    def test_wait_stage_and_console_boundaries_do_not_borrow_Run_entry(self):
        report = self.reader_case(['|[3]readstring("OLD_RUN(")',
            'CRESCENDO_NATIVE_RUN_STAGE Run RETURN', 'CRESCENDO_NATIVE_RUN_STAGE Wait BEGIN',
            '*Error* lineread/read: syntax error encountered in input'])
        self.assertEqual(report['first_error']['native_reader_call']['state'], 'not_observed')
        report = self.collect('CRESCENDO_NATIVE_READER_TRACE ENABLED public=readstring,linereadstring\n'
            'CRESCENDO_NATIVE_RUN_STAGE Run BEGIN\n|[3]readstring("STDOUT(")\nCAPTURE_DONE\n',
            '*Error* lineread/read: syntax error encountered in input\n')
        self.assertNotIn('native_reader_call', report['first_error'])

    def test_far_away_or_after_restore_entry_is_not_attributed(self):
        report = self.reader_case(['|[3]readstring("OLD(")'] + ['other']*12 + [
            '*Error* lineread/read: syntax error encountered in input'])
        self.assertEqual(report['first_error']['native_reader_call']['state'], 'not_observed')
        report = self.reader_case(['|[3]readstring("OLD(")',
            'CRESCENDO_NATIVE_READER_TRACE RESTORE DONE',
            '*Error* lineread/read: syntax error encountered in input'])
        self.assertNotIn('native_reader_call', report['first_error'])


if __name__ == '__main__':
    unittest.main()
