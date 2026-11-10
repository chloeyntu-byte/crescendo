"""Real local fake CLIs prove exact argv and observable terminal boundaries."""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

from crescendo_engine.netlist_execution import NetlistExecutionContext
from crescendo_engine.scheduler_cli import resolve_scheduler_binary


class GuideCancelCliTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = self.temp.name
        self.calls = os.path.join(self.root, 'calls.jsonl')
        self.mode = 'delayed'
        self.executables = set()
        self.batch_submit = self.executable('batch-submit', 'raise RuntimeError("must never submit in cancel tests")')
        self.batch_query = self.executable('batch-query', '''
mode = open(%r).read()
if sys.argv[1:] in (['--help'], ['-h']):
    if mode == 'failed_help':
        print('Usage: batch-query JOBID' if sys.argv[1:] == ['--help'] else 'version banner only')
        sys.exit(1 if sys.argv[1:] == ['--help'] else 0)
    print('Usage: batch-query JOBID')
    sys.exit(0)
if sys.argv[1:] != ['7391']: raise RuntimeError('foreign or all-account query')
count_path = %r
count = int(open(count_path).read()) if os.path.isfile(count_path) else 0
with open(count_path, 'w') as stream: stream.write(str(count + 1))
if mode == 'gone':
    print('No matching jobs found')
elif mode == 'mixed':
    print('JOB_ID JOB_NAME USER JOB_STATE\\n7391 own user CANCELLED\\n9999 other user CANCELLED')
else:
    state = 'RUNNING' if count == 0 or mode == 'ack_only' else 'CANCELLED'
    print('JOB_ID JOB_NAME USER JOB_STATE EXIT_CODE\\n7391 own user ' + state + ' -')
''' % (os.path.join(self.root, 'mode'), os.path.join(self.root, 'query_count')))
        self.batch_cancel = self.executable('batch-cancel', '''
if sys.argv[1:] in (['--help'], ['-h']):
    print('Usage: batch-cancel JOBID')
    sys.exit(0)
if sys.argv[1:] != ['7391']: raise RuntimeError('foreign or all-account cancellation')
print('cancel request accepted')
''')
        self.set_mode(self.mode)

    def tearDown(self):
        self.temp.cleanup()

    def set_mode(self, mode):
        with open(os.path.join(self.root, 'mode'), 'w') as stream:
            stream.write(mode)

    def executable(self, name, body):
        path = os.path.join(self.root, name)
        with open(path, 'w') as stream:
            stream.write('#!%s\nimport json,os,sys\n' % sys.executable)
            stream.write('with open(%r,"a") as stream: stream.write(json.dumps([os.path.basename(sys.argv[0])]+sys.argv[1:])+"\\n")\n' % self.calls)
            stream.write(body + '\n')
        os.chmod(path, 0o700)
        self.executables.add(path)
        return path

    def runner(self, argv, **kwargs):
        self.assertIn(argv[0], self.executables)
        process = subprocess.run(argv, cwd=kwargs.get('cwd'), env=kwargs.get('env'),
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=5)
        return {'returncode': process.returncode, 'stdout_tail': process.stdout.decode(),
                'stderr_tail': process.stderr.decode()}

    def cancellation(self, stop_timeout=None):
        prepared = {'job_policy': {'available': True, 'global': {'properties': {
            'distributionmethod': 'Local', 'maxjobs': '1'}}, 'tests': []}}
        runtime = {} if stop_timeout is None else {'cluster_stop_timeout_seconds': stop_timeout}
        import shlex
        runtime['execution_commands'] = {
            'scheduler_query_template': shlex.quote(self.batch_query) + ' {job_id}',
            'scheduler_cancel_template': shlex.quote(self.batch_cancel) + ' {job_id}' if self.mode != 'unconfigured' else ''}
        context = NetlistExecutionContext({'cadence': {}, 'runtime': runtime}, prepared, {},
                                         command_runner=self.runner)
        try:
            return context._scheduler_cancel({'submit_argv': [self.batch_submit]},
                {'stdout_tail': 'JOBID MESSAGE\n7391 submit job successfully\n'},
                self.root, {'PATH': '/usr/bin:/bin', 'HOME': self.root})
        finally:
            context.close(cancel=True)

    def records(self):
        if not os.path.exists(self.calls):
            return []
        with open(self.calls) as stream:
            return [json.loads(line) for line in stream]

    def test_adjacent_batch_cancel_and_delayed_exact_terminal_query_prove_stop(self):
        proof = self.cancellation()
        self.assertTrue(proof['attempted'])
        self.assertTrue(proof['remote_stop_proven'])
        self.assertEqual(proof['terminal_state'], 'CANCELLED')
        self.assertEqual(len(proof['queries']), 2)
        self.assertEqual(self.records(), [['batch-cancel', '7391'], ['batch-query', '7391'], ['batch-query', '7391']])

    def test_cancel_ack_without_terminal_is_bounded_and_never_proof(self):
        self.set_mode('ack_only')
        proof = self.cancellation(stop_timeout=1)
        self.assertEqual(proof['cancel']['returncode'], 0)
        self.assertFalse(proof['remote_stop_proven'])
        self.assertGreaterEqual(len(proof['queries']), 1)
        self.assertEqual(proof['timeout_seconds'], 1)
        self.assertTrue(proof['deadline_expired'])
        self.assertTrue(all(row[1:] == ['7391'] for row in self.records() if row[1] not in ('--help', '-h')))

    def test_disappearance_or_foreign_row_never_upgrades_to_stop_proof(self):
        for mode in ('gone', 'mixed'):
            with self.subTest(mode=mode):
                self.set_mode(mode)
                proof = self.cancellation()
                self.assertFalse(proof['remote_stop_proven'])
                self.assertIsNone(proof['terminal_state'])
                self.assertEqual(len(proof['queries']), 1)

    def test_missing_cancel_template_cannot_borrow_authority_from_adjacent_tools(self):
        self.mode = 'unconfigured'
        self.set_mode(self.mode)
        proof = self.cancellation()
        self.assertFalse(proof['attempted'])
        self.assertFalse(proof['remote_stop_proven'])
        self.assertEqual(self.records(), [])

    def test_resolver_uses_explicit_path_or_selected_path_environment(self):
        other = os.path.join(self.root, 'other')
        os.mkdir(other)
        shutil.copy2(self.batch_query, os.path.join(other, 'batch-query'))
        environment = {'PATH': other, 'HOME': 'captured-home'}
        before = dict(environment)
        self.assertEqual(resolve_scheduler_binary('batch-query', environment), os.path.join(other, 'batch-query'))
        self.assertEqual(resolve_scheduler_binary(self.batch_query, environment), self.batch_query)
        self.assertEqual(environment, before)


class GuideObserverCliTests(unittest.TestCase):
    def observer(self, root, body, wheel=False):
        project = os.path.dirname(os.path.dirname(__file__))
        original = os.path.join(project, 'tools', 'maestro_job_observer.py')
        script = original
        if wheel:
            package = os.path.join(root, 'site packages', 'crescendo_engine')
            helper = os.path.join(package, 'resources', 'tools')
            os.makedirs(helper)
            with open(os.path.join(package, '__init__.py'), 'w') as stream:
                stream.write('')
            shutil.copy2(os.path.join(project, 'crescendo_engine', 'scheduler_protocol.py'), package)
            script = os.path.join(helper, 'maestro_job_observer.py')
            shutil.copy2(original, script)
        batch_submit = os.path.join(root, 'batch-submit')
        with open(batch_submit, 'w') as stream:
            stream.write('#!%s\nimport os,time\n%s\n' % (sys.executable, body))
        os.chmod(batch_submit, 0o700)
        config = os.path.join(root, 'observer.json')
        log = os.path.join(root, 'dispatch.jsonl')
        with open(config, 'w') as stream:
            json.dump({'schema_version':'crescendo.maestro_job_observer.v1', 'argv':[batch_submit],
                       'command':batch_submit,'log_path':log}, stream)
        env = dict(os.environ)
        env.pop('PYTHONPATH', None)
        process = subprocess.run([sys.executable, '-B', script, '--config', config],
            cwd=root, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=8)
        self.assertEqual(process.returncode, 0, process.stderr)
        with open(log) as stream:
            events = [json.loads(line) for line in stream]
        return process, events

    def test_split_table_id_is_not_published_until_complete(self):
        with tempfile.TemporaryDirectory() as root:
            process, events = self.observer(root,
                'os.write(1,b"CPU MEM\\n6 30000\\nJOBID MESSAGE\\n73")\ntime.sleep(.15)\nos.write(1,b"91 accepted\\n")')
            outputs = [row for row in events if row['event']=='submit_output']
            self.assertEqual(outputs[0]['job_ids'], [])
            self.assertEqual(events[-1]['job_ids'], ['7391'])
            self.assertIn(b'7391 accepted', process.stdout)

    def test_dash_failure_and_later_ambiguity_leave_no_terminal_id(self):
        for body in ('os.write(1,b"JOBID MESSAGE\\n- submit failed\\n")',
                     'os.write(1,b"JOBID MESSAGE\\n7391 accepted\\n")\ntime.sleep(.1)\nos.write(2,b"Job <9999> submitted\\n")'):
            with self.subTest(body=body), tempfile.TemporaryDirectory() as root:
                unused, events = self.observer(root, body)
                self.assertEqual(events[-1]['job_ids'], [])

    def test_wheel_resource_observer_imports_its_shipped_shared_parser(self):
        with tempfile.TemporaryDirectory() as root:
            unused, events = self.observer(root, 'os.write(1,b"JOBID MESSAGE\\n7391 accepted\\n")', wheel=True)
            self.assertEqual(events[-1]['job_ids'], ['7391'])

    def test_eof_partial_id_and_other_stream_integer_do_not_complete_a_record(self):
        bodies = ('os.write(1,b"JOBID MESSAGE\\n73")',
                  'os.write(1,b"JOBID MESSAGE\\n")\nos.write(2,b"7391\\n")')
        for body in bodies:
            with self.subTest(body=body), tempfile.TemporaryDirectory() as root:
                unused, events = self.observer(root, body)
                self.assertEqual(events[-1]['job_ids'], [])
                self.assertTrue(all(row.get('job_ids', []) == [] for row in events))

    def test_complete_json_record_is_not_invalidated_by_other_stream_banner(self):
        with tempfile.TemporaryDirectory() as root:
            unused, events = self.observer(root,
                'os.write(1,b\'{"jobId":"7391"}\\n\')\nos.write(2,b"CPU 6 MEM 30000\\n")')
            self.assertEqual(events[-1]['job_ids'], ['7391'])


if __name__ == '__main__':
    unittest.main()
