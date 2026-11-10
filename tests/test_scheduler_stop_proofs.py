"""Exact Scheduler cancellation and terminal proofs; all commands are mocked."""
import json
import os
import tempfile
import unittest

from crescendo_engine.netlist_execution import NetlistExecutionContext, scheduler_job_id, scheduler_terminal_state
from crescendo_engine.scheduler_cli import scheduler_command_argv


class ExactCancelDialectTests(unittest.TestCase):
    def test_submission_json_conflicting_or_duplicate_ids_never_select_one_for_cancel(self):
        for text in ('{"jobId":7391,"job_id":9999}',
                     '{"jobId":7391,"jobId":9999}',
                     '{"jobId":7391,"tasks":[{"jobId":9999}]}',
                     '{"jobId":7391,"job_id":true}'):
            self.assertIsNone(scheduler_job_id(text))
        self.assertEqual(scheduler_job_id('{"jobId":7391,"job_id":7391}'), '7391')

    def test_only_explicit_exact_id_template_can_render_cancel(self):
        self.assertEqual(scheduler_command_argv('batch-cancel --id {job_id}', '7391'),
                         ['batch-cancel', '--id', '7391'])
        for template in ('batch-cancel --all', 'batch-cancel {job_id} {job_id}'):
            with self.assertRaises(ValueError):
                scheduler_command_argv(template, '7391')
        with self.assertRaises(ValueError):
            scheduler_command_argv('batch-cancel {job_id}', 'all')

    def _context(self, root, cancel_configured, state_text):
        import shlex
        submit, query = root + '/batch-submit', root + '/batch-query'
        calls = []
        def runner(argv, **kwargs):
            calls.append(argv)
            if argv[1] == '--cancel':
                return {'returncode': 0, 'stdout_tail': 'cancel request accepted'}
            return {'returncode': 0, 'stdout_tail': state_text}
        prepared = {'job_policy': {'global': {'properties': {'distributionmethod': 'Local'}}}}
        commands = {'scheduler_query_template': shlex.quote(query) + ' --id {job_id} --detail',
                    'scheduler_cancel_template': shlex.quote(query) + ' --cancel {job_id}' if cancel_configured else ''}
        context = NetlistExecutionContext({'cadence': {}, 'runtime': {'execution_commands': commands}},
                                         prepared, {}, command_runner=runner)
        return context, calls, submit, query

    def test_missing_cancel_template_never_cancels_or_queries(self):
        with tempfile.TemporaryDirectory() as root:
            context, calls, submit, query = self._context(root, False, 'JOB_ID: 7391\nSTATE: CANCELLED')
            try:
                proof = context._scheduler_cancel({'submit_argv': [submit]},
                    {'stdout_tail': 'Job <7391> submitted'}, root, dict(os.environ))
                self.assertFalse(proof['attempted'])
                self.assertFalse(proof['remote_stop_proven'])
                self.assertEqual(calls, [])
            finally:
                context.close(cancel=True)

    def test_exact_cancel_request_does_not_upgrade_mixed_records_to_stop_proof(self):
        mixed = 'JOB_ID: 7391\nSTATE: RUNNING\nJOB_ID: 9999\nSTATE: CANCELLED\nJOB_ID: 7391\n'
        with tempfile.TemporaryDirectory() as root:
            context, calls, submit, query = self._context(root, True, mixed)
            try:
                proof = context._scheduler_cancel({'submit_argv': [submit]},
                    {'stdout_tail': 'Job <7391> submitted'}, root, dict(os.environ))
                self.assertTrue(proof['attempted'])
                self.assertFalse(proof['remote_stop_proven'])
                self.assertIsNone(proof['terminal_state'])
                self.assertEqual(calls, [[query, '--cancel', '7391'], [query, '--id', '7391', '--detail']])
            finally:
                context.close(cancel=True)


class TerminalStateIdentityTests(unittest.TestCase):
    def test_foreign_and_conflicting_job_records_are_rejected(self):
        for text in ('JOB_ID: 7391\nSTATE: RUNNING\nJOB_ID: 9999\nSTATE: CANCELLED\nJOB_ID: 7391\n',
                     'JOB_ID: 7391\nSTATE: RUNNING\nJOB_ID: 7391\nSTATE: CANCELLED\n',
                     'JOB_ID: 7391\nSTATE: DONE\nJOB_ID: 7391\nSTATE: CANCELLED\n',
                     'JOB_ID: 7391\nSTATE: CANCELLED\nTASK[0].JOB_ID=9999\nTASK[0].STATE=CANCELLED'):
            self.assertIsNone(scheduler_terminal_state(text, '7391'))

    def test_verbose_same_owned_id_all_terminal_tasks_are_compatible(self):
        text = 'JOB_ID: 7391\nJOB_STATE: CANCELLED\nTASK[0].JOB_ID=7391\nTASK[0].STATE=SUCCEEDED\nTASK[1].JOB_ID=7391\nTASK[1].STATE=CANCELLED\n'
        self.assertEqual(scheduler_terminal_state(text, '7391'), 'CANCELLED')
        self.assertEqual(scheduler_terminal_state(text + 'JOB_ID: 7391\nSTATE: CANCELLED\n', '7391'), 'CANCELLED')

    def test_any_active_unknown_or_conflicting_task_prevents_stop_proof(self):
        prefix = 'JOB_ID: 7391\nSTATE: CANCELLED\nTASK[0].JOB_ID=7391\n'
        for suffix in ('TASK[0].STATE=RUNNING', 'TASK[0].STATE=PENDING',
                       'TASK[0].STATE=UNKNOWN', 'TASK[0].RUN_STATE=RUNNING', 'TASK[0].STATE=SUCCEEDED\nTASK[0].STATE=CANCELLED'):
            self.assertIsNone(scheduler_terminal_state(prefix + suffix, '7391'))
        self.assertIsNone(scheduler_terminal_state('TASK[0].JOB_ID=7391\nTASK[0].STATE=CANCELLED', '7391'))

    def test_json_duplicates_aliases_foreign_ids_and_active_tasks_are_rejected(self):
        for text in ('{"jobId":7391,"state":"CANCELLED","jobId":9999}',
                     '{"jobId":7391,"job_id":9999,"state":"CANCELLED"}',
                     '{"jobId":7391,"state":"RUNNING","state":"CANCELLED"}',
                     '{"jobId":7391,"state":"CANCELLED","tasks":[{"jobId":7391,"state":"RUNNING"}]}',
                     '{"jobId":7391,"state":"CANCELLED","tasks":[{"jobId":9999,"state":"CANCELLED"}]}',
                     '{"jobId":7391,"state":"CANCELLED","tasks":[{"jobId":7391,"executionState":"RUNNING"}]}'):
            self.assertIsNone(scheduler_terminal_state(text, '7391'))
        good = {'jobId': 7391, 'state': 'CANCELLED', 'tasks': [
            {'jobId': 7391, 'state': 'SUCCEEDED'}, {'jobId': 7391, 'state': 'CANCELLED'}]}
        self.assertEqual(scheduler_terminal_state(json.dumps(good), '7391'), 'CANCELLED')


if __name__ == '__main__':
    unittest.main()
