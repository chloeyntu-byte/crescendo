"""Guide and legacy scheduler records; parsing never runs a scheduler."""
import json
import unittest

from crescendo_engine.scheduler_protocol import (
    MAX_PROTOCOL_TEXT, parse_scheduler_observation, parse_submit_job_id,
    parse_submit_observation, parse_submit_streams, valid_job_id,
)
from crescendo_engine.scheduler_cli import scheduler_command_argv


class SubmissionRecordTests(unittest.TestCase):
    def test_completed_streams_preserve_json_banner_and_reject_foreign_or_partial_ids(self):
        observed = parse_submit_streams('{"jobId":"10000002"}', 'CPU 8 MEM 8000\n')
        self.assertTrue(observed['accepted'])
        self.assertEqual(observed['job_id'], '10000002')
        for stdout, stderr in (('JOBID MESSAGE\n10000002 accepted\n', 'Job <9999> submitted\n'),
                               ('JOBID MESSAGE\n10000002 accepted\n', 'ERROR permission denied\n'),
                               ('JOBID MESSAGE\n39', '097732 accepted\n')):
            observed = parse_submit_streams(stdout, stderr)
            self.assertIsNone(observed['job_id'])
            self.assertFalse(observed['accepted'])
    def test_guide_numeric_id_and_failed_dash_message(self):
        self.assertEqual(parse_submit_job_id('JOBID MESSAGE\n7391 submit job successfully\n'), '7391')
        failed = parse_submit_observation('JOBID MESSAGE\n- Submission failed\n')
        self.assertIsNone(failed['job_id'])
        self.assertTrue(failed['rejected'])
        self.assertEqual(failed['message'], 'Submission failed')

    def test_partial_rows_never_publish_a_partial_number(self):
        for text in ('JOBID MESSAGE', 'JOBID MESSAGE\n', 'JOBID MESSAGE\n7',
                     'JOBID MESSAGE\n7391'):
            self.assertIsNone(parse_submit_job_id(text))
        self.assertEqual(parse_submit_job_id('JOBID MESSAGE\n7391\n'), '7391')

    def test_only_explicit_receipts_not_cpu_memory_or_foreign_integers(self):
        for text in ('CPU MEM\n6 30000\n', 'cpu=6 mem=30000 total=7391',
                     'Job 7391 failed', 'Job <7391>', 'JOBID MESSAGE\nCPU MEM\n6 30000\n'):
            self.assertIsNone(parse_submit_job_id(text))
        text = 'JOBID MESSAGE\n7391 accepted\nCPU MEM\n6 30000\n'
        self.assertEqual(parse_submit_job_id(text), '7391')

    def test_legacy_lsf_and_json_receipts_remain_supported(self):
        for text in ('Job <7391> is submitted successfully.', 'Submitted batch job 7391',
                     'Job 7391 submitted', 'jobId: 7391', '{"jobId":7391}',
                     '{"jobId":7391,"job_id":7391}'):
            self.assertEqual(parse_submit_job_id(text), '7391')

    def test_conflicts_duplicate_json_errors_or_large_ids_are_rejected(self):
        for text in ('JOBID MESSAGE\n7391 accepted\n9999 accepted\n',
                     'JOBID MESSAGE\n7391 accepted\n- Submission failed\n',
                     'JOBID MESSAGE\n7391 accepted\nJob <9999> submitted\n',
                     '{"jobId":7391,"jobId":7391}', '{"jobId":7391,"job_id":9999}',
                     '{"jobId":7391,"error":"submit rejected"}', '{"jobId":true}',
                     '{"jobId":"' + '9' * 33 + '"}', 'Job <' + '9' * 33 + '> submitted',
                     'jobId: 7391abc', 'jobId: 7391.0'):
            self.assertIsNone(parse_submit_job_id(text), text)
        self.assertIsNone(parse_submit_job_id('x' * (MAX_PROTOCOL_TEXT + 1)))


class ExactSchedulerObservationTests(unittest.TestCase):
    header = 'JOB_ID JOB_NAME USER JOB_STATE EXIT_CODE\n'

    def test_verified_server_whitespace_detail_keeps_time_colons_as_value(self):
        text = ('JOB_ID     10000002\nJOB_NAME   probe_spectre\nUSER       user01\n'
            'JOB_STATE  SUCCEEDED\nQUEUE      short\nACCOUNT    current-account\n'
            'SUBMIT_NODE submit01\nEXEC_NODE node02\nEXIT_CODE 0\n'
            'JOB_CREATE_TIME 2026/10/13 19:13:16\n')
        observed = parse_scheduler_observation(text, '10000002')
        self.assertEqual(observed['job_id'], '10000002')
        self.assertEqual(observed['state'], 'SUCCEEDED')
        self.assertTrue(observed['terminal'])
        self.assertFalse(observed['invalid'])
        self.assertEqual(observed['job_exit_code'], 0)
        self.assertEqual(observed['metadata']['exec_node'], 'node02')
        self.assertEqual(observed['metadata']['submit_node'], 'submit01')
        self.assertEqual(observed['metadata']['job_create_time'], '2026/10/13 19:13:16')

    def test_whitespace_foreign_conflicting_and_active_tasks_cannot_hide(self):
        for text in ('JOB_ID 7391\nJOB_STATE CANCELLED\nJOB_ID 9999\n',
                     'JOB_ID 7391\nJOB_STATE RUNNING\nJOB_STATE SUCCEEDED\n',
                     'JOB_ID 7391\nJOB_STATE SUCCEEDED\nTASK[0].STATE RUNNING\n',
                     'JOB_ID 7391\nJOB_STATE SUCCEEDED\nTASK[0].RUN_STATE RUNNING\n',
                     'JOB_ID 7391abc\nJOB_STATE SUCCEEDED\n'):
            observed = parse_scheduler_observation(text, '7391')
            self.assertTrue(observed['invalid'])
            self.assertFalse(observed['terminal'])

    def test_node_and_exit_metadata_are_kept_for_table_and_json(self):
        table = 'JOB_ID JOB_NAME JOB_STATE EXEC_NODE SUBMIT_NODE EXIT_CODE\n7391 own SUCCEEDED remote login 0\n'
        document = json.dumps({'jobId': '7391', 'jobState': 'SUCCEEDED', 'execNode': 'remote',
                              'submitNode': 'login', 'exitCode': 0})
        for text in (table, document):
            observed = parse_scheduler_observation(text, '7391')
            self.assertTrue(observed['terminal'])
            self.assertEqual(observed['job_exit_code'], 0)
            self.assertEqual(observed['metadata']['exec_node'], 'remote')
            self.assertEqual(observed['metadata']['submit_node'], 'login')

    def test_fourth_header_column_binds_the_exact_job(self):
        running = parse_scheduler_observation(self.header + '7391 own user RUNNING -\n', '7391')
        self.assertEqual(running['job_id'], '7391')
        self.assertEqual(running['state'], 'RUNNING')
        self.assertFalse(running['invalid'])
        self.assertFalse(running['terminal'])
        self.assertIsNone(running['job_exit_code'])
        done = parse_scheduler_observation(self.header + '7391 own user CANCELLED 143\n', '7391')
        self.assertTrue(done['terminal'])
        self.assertEqual(done['job_exit_code'], 143)
        self.assertEqual(done['format'], 'table')

    def test_foreign_mixed_conflicting_or_malformed_table_never_proves_stop(self):
        for body in ('9999 other user CANCELLED 0\n',
                     '7391 own user CANCELLED 0\n9999 other user CANCELLED 0\n',
                     '7391 own user RUNNING -\n7391 own user CANCELLED 143\n',
                     '7391 own\n', '7391 own user UNKNOWN -\n',
                     '7391 own user CANCELLED 0\n' + '9' * 33 + ' bad user DONE 0\n'):
            observed = parse_scheduler_observation(self.header + body, '7391')
            self.assertFalse(observed['terminal'], body)
            self.assertTrue(observed['invalid'], body)

    def test_absence_and_zero_exit_error_are_not_stop_proof(self):
        for text in ('Job 7391 not found', 'No matching jobs found', self.header):
            observed = parse_scheduler_observation(text, '7391')
            self.assertTrue(observed['not_found'])
            self.assertFalse(observed['terminal'])
        observed = parse_scheduler_observation('ERROR: Job 7391 does not exist', '7391')
        self.assertFalse(observed['terminal'])
        self.assertTrue(observed['invalid'])

    def test_all_task_states_must_be_terminal_even_with_a_terminal_job(self):
        for text in ('JOB_ID:7391\nSTATE:CANCELLED\nTASK[0].STATE:RUNNING',
                     'JOB_ID:7391\nSTATE:CANCELLED\nTASK[0].EXECUTION_STATE:RUNNING',
                     'TASK[0].JOB_ID:7391\nTASK[0].STATE:CANCELLED',
                     json.dumps({'jobId':7391,'state':'CANCELLED','tasks':[{'jobId':7391,'state':'RUNNING'}]}),
                     self.header + '7391 own user CANCELLED 0\nTASK[0].STATE:RUNNING'):
            self.assertFalse(parse_scheduler_observation(text, '7391')['terminal'])
        good = {'jobId':7391,'state':'CANCELLED','tasks':[
            {'jobId':7391,'state':'SUCCEEDED'}, {'jobId':7391,'state':'CANCELLED'}]}
        self.assertTrue(parse_scheduler_observation(json.dumps(good), '7391')['terminal'])

    def test_duplicate_json_fields_and_conflicting_aliases_are_never_hidden(self):
        for text in ('{"jobId":7391,"state":"DONE","jobId":7391}',
                     '{"jobId":7391,"state":"DONE","state":"DONE"}',
                     '{"jobId":7391,"job_id":9999,"state":"DONE"}',
                     '{"jobId":7391,"state":"DONE","error":"permission denied"}'):
            observed = parse_scheduler_observation(text, '7391')
            self.assertTrue(observed['invalid'])
            self.assertFalse(observed['terminal'])


class ConfiguredArgvTests(unittest.TestCase):
    def test_explicit_position_preserves_exact_id_only(self):
        for template, expected in [('batch-query {job_id}', ['batch-query', '7391']),
                                   ('batch-query --id {job_id}', ['batch-query', '--id', '7391']),
                                   ('batch-cancel {job_id}', ['batch-cancel', '7391'])]:
            self.assertEqual(scheduler_command_argv(template, '7391'), expected)
        for template in ('batch-cancel --all', 'batch-cancel {job_id} {job_id}'):
            with self.assertRaises(ValueError):
                scheduler_command_argv(template, '7391')

    def test_every_renderer_rejects_noncanonical_numeric_ids(self):
        for identifier in ('7391\n', ' 7391', '7391 ', 'all', '-a', '1;touch bad', True,
                           '9' * 33, '\u0661\u0662\u0663'):
            self.assertFalse(valid_job_id(identifier))
            for template in ('batch-query {job_id}', 'batch-cancel {job_id}', 'wrapper --cancel {job_id}'):
                with self.assertRaises(ValueError):
                    scheduler_command_argv(template, identifier)


if __name__ == '__main__':
    unittest.main()
