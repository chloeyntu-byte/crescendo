"""Regression fixtures for caller auth, native parser errors and own NFS readers."""
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

from crescendo_engine.cadence import child_environment
from crescendo_engine.job_policy import prepare_job_command_observers
from crescendo_engine.maestro_progress import _owned_binary_stream
from crescendo_engine.native_diagnostics import skill_command_diagnostics
from crescendo_engine.native_environment import (
    LEGACY_NATIVE_ENV_NAMES, NATIVE_ENV_SCHEMA, SCHEDULER_HOME_SET,
    SCHEDULER_HOME_VALUE, parse_native_environment_snapshot, scheduler_environment,
)
from crescendo_engine.native_log_io import (
    OwnedLogReadersBusy, native_log_cleanup_scope, native_log_read_scope,
)
from crescendo_engine.retention import ArtifactCleanupError, cleanup_abandoned_controller_epoch


class SchedulerContextTests(unittest.TestCase):
    def test_private_ocean_and_caller_scheduler_home_preserve_all_states(self):
        with tempfile.TemporaryDirectory() as root:
            for value in (None, '', root + '/original caller'):
                snapshot = {'HOME': value, 'PATH': os.defpath}
                with self.subTest(value=value), mock.patch.dict(os.environ, {'HOME': root + '/gui'}, clear=True):
                    native = child_environment({'cadence': {'launch_environment': snapshot}}, root + '/private/home')
                    scheduler = scheduler_environment(native)
                self.assertEqual(native['HOME'], root + '/private/home')
                self.assertEqual(scheduler.get('HOME'), value)
                self.assertEqual('HOME' in scheduler, value is not None)
                self.assertNotIn(SCHEDULER_HOME_SET, scheduler)
                self.assertNotIn(SCHEDULER_HOME_VALUE, scheduler)
                self.assertEqual(native['PATH'], scheduler['PATH'])

    def test_existing_home_markers_survive_nested_native_launch(self):
        with tempfile.TemporaryDirectory() as root, mock.patch.dict(os.environ, {
                'HOME': root + '/first private', SCHEDULER_HOME_SET: 'x',
                SCHEDULER_HOME_VALUE: root + '/caller'}, clear=True):
            native = child_environment({'cadence': {}}, root + '/second private')
            self.assertEqual(scheduler_environment(native)['HOME'], root + '/caller')

    def test_compute_spectre_wrapper_restores_private_home_after_scheduler_auth(self):
        with tempfile.TemporaryDirectory() as root:
            simulator = root + '/spectre_fixture'
            output = root + '/compute_context.json'
            with open(simulator, 'w') as stream:
                stream.write('#!%s\nimport json,os,sys\nwith open(sys.argv[1],"w") as f: json.dump({"home":os.environ.get("HOME"),"simulation_markers":[x for x in os.environ if x.startswith("CRESCENDO_SIMULATION_HOME_")]},f)\n' % sys.executable)
            os.chmod(simulator, 0o700)
            wrapper = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'tools', 'submit_spectre_job.sh'))
            environment = dict(os.environ, HOME=root + '/caller',
                CRESCENDO_SIMULATION_HOME_SET='x', CRESCENDO_SIMULATION_HOME_VALUE=root + '/private',
                CRESCENDO_SIMULATION_TIMEOUT_SECONDS='3', CRESCENDO_SPECTRE_TIMEOUT_LOG=root + '/timing.log')
            result = subprocess.run(['/bin/sh', wrapper, simulator, output], cwd=root,
                env=environment, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=8)
            self.assertEqual(result.returncode, 0, result.stderr)
            with open(output) as stream:
                actual = json.load(stream)
            self.assertEqual(actual, {'home': root + '/private', 'simulation_markers': []})

    def test_private_remote_submission_uses_caller_home_and_compute_keeps_private_home(self):
        from crescendo_engine.netlist_execution import NetlistExecutionContext
        from crescendo_engine.utils import sha256_file
        with tempfile.TemporaryDirectory() as root:
            task_root = root + '/task'
            os.makedirs(task_root + '/psf')
            netlist, simulator = task_root + '/input.scs', task_root + '/simulator.py'
            with open(netlist, 'w') as stream:
                stream.write('simulator lang=spectre\n')
            with open(simulator, 'w') as stream:
                stream.write('import os\nopen(%r,"w").write(os.environ.get("HOME", "<unset>"))\nopen(%r,"w").write("spectre completes with 0 errors.\\n")\nopen(%r,"w").write("owned fixture")\n' % (
                    root + '/compute.home', task_root + '/psf/spectre.out', task_root + '/psf/logFile'))
            scheduler = root + '/batch-submit'
            with open(scheduler, 'w') as stream:
                stream.write('#!/bin/sh\nprintf "%%s\\n" "$HOME" > %s\nprintf "Job <7391> submitted\\n"\nfor cr_arg do cr_script=$cr_arg; done\n/bin/sh "$cr_script"\n' % (root + '/scheduler.home'))
            os.chmod(scheduler, 0o700)
            config = {'cadence': {'launch_environment': {'HOME': root + '/caller', 'PATH': os.defpath}},
                      'runtime': {'cluster_queue_timeout_seconds': 1, 'execution_commands': {
                          'scheduler_query_template': '/bin/echo {job_id}',
                          'scheduler_cancel_template': '/bin/echo {job_id}'}}}
            prepared = {'job_policy': {'available': True, 'global': {'properties': {
                'distributionmethod': 'Command', 'maxjobs': '1', 'jobsubmitcommand': scheduler}}, 'tests': []}}
            spec = {'candidate_timeout_seconds': 3, 'objectives': [{'output_key': 'rc::gain'}], 'constraints': []}
            task = {'task_id': 'owned', 'test': 'rc', 'corner': 'TT', 'point_id': 1,
                    'cwd': task_root, 'netlist_path': netlist, 'netlist_sha256': sha256_file(netlist),
                    'psf_dir': task_root + '/psf', 'spectre_log': task_root + '/psf/spectre.out',
                    'argv': [sys.executable, simulator, netlist],
                    'outputs': [{'key': 'rc::gain', 'name': 'gain', 'expression': '1.0'}]}
            context = NetlistExecutionContext(config, prepared, spec,
                measurement_evaluator=lambda task, *args, **kwargs: {
                    'outputs': [dict(task['outputs'][0], value=1.0, error=None)]})
            try:
                result = context.evaluate({'root': root, 'candidate_id': 'g000_c0000', 'tasks': [task]})
                self.assertTrue(result['task_results'][0]['receipt']['cleanup_proven'])
                self.assertEqual(result['task_results'][0]['receipt']['compute_hostname'], os.uname().nodename)
                self.assertEqual(result['task_results'][0]['receipt']['compute_uid'], os.getuid())
                with open(root + '/scheduler.home') as stream:
                    self.assertEqual(stream.read().strip(), root + '/caller')
                with open(root + '/compute.home') as stream:
                    self.assertEqual(stream.read(), task_root + '/home')
            finally:
                context.close(cancel=True)

    def test_scheduler_cancel_uses_proven_exact_selector_and_caller_home(self):
        from crescendo_engine.netlist_execution import NetlistExecutionContext
        with tempfile.TemporaryDirectory() as root:
            batch_submit, batch_query = root + '/batch-submit', root + '/batch-query'
            for path in (batch_submit, batch_query):
                with open(path, 'w') as stream:
                    stream.write('#!/bin/sh\nexit 0\n')
                os.chmod(path, 0o700)
            config = {'cadence': {}, 'runtime': {'execution_commands': {
                'scheduler_query_template': batch_query + ' -x -L {job_id}',
                'scheduler_cancel_template': batch_query + ' -T {job_id}'}}}
            prepared = {'job_policy': {'available': True, 'global': {'properties': {
                'distributionmethod': 'Local', 'maxjobs': '1'}}, 'tests': []}}
            help_text = 'Usage: batch-query -x -L JOB_ID\n-T JOB_ID Terminate a job\n-x Include expanded jobs\n-L JOB_ID List detailed job information\n'
            calls = []
            def runner(argv, **kwargs):
                calls.append((argv, kwargs['env']))
                if argv[1] == '--help':
                    return {'returncode': 0, 'stdout_tail': help_text}
                if argv[1] == '-T':
                    return {'returncode': 0, 'stdout_tail': 'cancel requested'}
                return {'returncode': 0, 'stdout_tail': 'JOB_ID: 7391\nSTATE: CANCELLED'}
            context = NetlistExecutionContext(config, prepared, {}, command_runner=runner)
            env = {'HOME': root + '/private', SCHEDULER_HOME_SET: 'x', SCHEDULER_HOME_VALUE: root + '/caller'}
            try:
                proof = context._scheduler_cancel({'submit_argv': [batch_submit]},
                    {'stdout_tail': 'Job <7391> submitted'}, root, env)
                self.assertTrue(proof['remote_stop_proven'])
                self.assertEqual([row[0] for row in calls], [
                    [batch_query, '-T', '7391'], [batch_query, '-x', '-L', '7391']])
                self.assertTrue(all(row[1]['HOME'] == root + '/caller' for row in calls))
                self.assertEqual(env['HOME'], root + '/private')
            finally:
                context.close(cancel=True)

    def test_generated_compute_wrapper_restores_home_before_custom_submit_script(self):
        from crescendo_engine.adexl_runner import _spectre_wrapper
        with tempfile.TemporaryDirectory() as root:
            simulator, submit = root + '/spectre_fixture', root + '/custom_submit'
            with open(simulator, 'w') as stream:
                stream.write('#!/bin/sh\nexit 0\n')
            with open(submit, 'w') as stream:
                stream.write('#!/bin/sh\nprintf "%%s\\n" "$HOME" > %s\nexec "$@"\n' % (root + '/custom.home'))
            for path in (simulator, submit):
                os.chmod(path, 0o700)
            wrapper, unused = _spectre_wrapper({'root': root}, {'cadence': {'spectre_bin': simulator},
                'runtime': {'spectre_submit_script': submit}})
            env = dict(os.environ, HOME=root + '/caller',
                CRESCENDO_SIMULATION_HOME_SET='x', CRESCENDO_SIMULATION_HOME_VALUE=root + '/private')
            process = subprocess.run([wrapper], env=env, cwd=root,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=5)
            self.assertEqual(process.returncode, 0, process.stderr)
            with open(root + '/custom.home') as stream:
                self.assertEqual(stream.read().strip(), root + '/private')

    @unittest.skipUnless(hasattr(os, 'fork'), 'POSIX fork fixture required')
    def test_observer_finishes_when_scheduler_grandchild_keeps_output_pipes(self):
        with tempfile.TemporaryDirectory() as root:
            pid_path = root + '/grandchild.pid'
            submit = root + '/forking_scheduler.py'
            with open(submit, 'w') as stream:
                stream.write('import os,sys,time\npid=os.fork()\nif pid==0:\n time.sleep(4)\n os._exit(0)\nwith open(sys.argv[1],"w") as f: f.write(str(pid))\nprint("Job <7391> submitted",flush=True)\nprint("scheduler exit detail",file=sys.stderr,flush=True)\nsys.exit(7)\n')
            command = '%s %s %s' % (sys.executable, submit, pid_path)
            policy = {'available': True, 'global': {'properties': {
                'name': 'fixture', 'distributionmethod': 'Command', 'jobsubmitcommand': command}}, 'tests': []}
            observed = prepare_job_command_observers(policy, {}, root, root + '/submission.jsonl')
            started = time.monotonic()
            process = subprocess.Popen([observed['wrappers'][0]['observer_script']],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = process.communicate(timeout=3)
            elapsed = time.monotonic() - started
            self.assertLess(elapsed, 2.5)
            self.assertEqual(process.returncode, 7, stderr)
            self.assertIn(b'Job <7391> submitted', stdout)
            self.assertIn(b'scheduler exit detail', stderr)
            with open(root + '/submission.jsonl') as stream:
                terminal = [json.loads(line) for line in stream][-1]
            self.assertEqual(terminal['job_ids'], ['7391'])
            self.assertEqual(terminal['returncode'], 7)
            self.assertTrue(terminal['output_incomplete'])
            self.assertEqual(terminal['child_pipes_retained'], ['stderr', 'stdout'])
            self.assertTrue(terminal['forward_threads_stopped'])
            with open(pid_path) as stream:
                grandchild = int(stream.read())
            # Read-only process existence check proves the observer did not
            # terminate the helper to force EOF. It expires naturally in 4s.
            os.kill(grandchild, 0)

    def test_old_complete_native_snapshot_is_accepted_without_forging_home(self):
        values = dict((name, None) for name in LEGACY_NATIVE_ENV_NAMES)
        self.assertEqual(parse_native_environment_snapshot(json.dumps({
            'schema': NATIVE_ENV_SCHEMA, 'values': values})), values)

    def test_real_submission_observer_uses_caller_home_and_keeps_native_loader_state(self):
        with tempfile.TemporaryDirectory() as root:
            output = root + '/submit_context.json'
            submit = root + '/scheduler_fixture.py'
            with open(submit, 'w') as stream:
                stream.write('import json,os,sys\nwith open(sys.argv[1],"w") as f: json.dump({"home":os.environ.get("HOME"),"tk":os.environ.get("TK_LIBRARY"),"markers":[x for x in os.environ if x.startswith("CRESCENDO_SCHEDULER_HOME_")]},f)\nprint("Job <7391> submitted")\nsys.exit(7)\n')
            command = '%s %s %s' % (sys.executable, submit, output)
            policy = {'available': True, 'global': {'properties': {
                'name': 'fixture', 'distributionmethod': 'Command', 'jobsubmitcommand': command}}, 'tests': []}
            observed = prepare_job_command_observers(policy, {}, root, root + '/submission.jsonl')
            env = dict(os.environ, HOME=root + '/private', TK_LIBRARY='',
                       CRESCENDO_SCHEDULER_HOME_SET='x', CRESCENDO_SCHEDULER_HOME_VALUE=root + '/caller')
            process = subprocess.Popen([observed['wrappers'][0]['observer_script']], env=env,
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = process.communicate(timeout=15)
            self.assertEqual(process.returncode, 7, stderr)
            with open(output) as stream:
                actual = json.load(stream)
            self.assertEqual(actual, {'home': root + '/caller', 'tk': '', 'markers': []})
            with open(root + '/submission.jsonl') as stream:
                events = [json.loads(line) for line in stream]
            self.assertTrue(events[0]['scheduler_context']['home_changed_from_ocean'])
            self.assertEqual(events[-1]['job_ids'], ['7391'])
            self.assertNotIn(root + '/caller', json.dumps(events))


class NativeDiagnosticsTests(unittest.TestCase):
    def test_native_error_following_frames_survive_without_becoming_generated_guard(self):
        from crescendo_engine.native_diagnostics import native_error_context
        with tempfile.TemporaryDirectory() as root:
            script=root+'/capture.il'
            with open(script,'w') as stream:
                stream.write('sstatus(stacktrace 16)\n')
            frames=['frame_%02d()' % number for number in range(16)]
            lines=['before_a','before_b','*Error* eval: unbound variable - schematic_custom',
                   '*Error* eval: unbound variable - schematic_custom','Stack Trace:','']+frames+['ordinary tail']*40
            with open(root+'/ocean.stdout','w') as stream:
                stream.write('\n'.join(lines))
            result=skill_command_diagnostics({'returncode':0},script,owned_root=root)
            self.assertFalse(result['ok'])
            self.assertEqual(result['distinct_error_count'],1)
            contexts=result['native_error_contexts']
            self.assertEqual(len(contexts),1)
            self.assertEqual(contexts[0]['error_line'],3)
            self.assertEqual(contexts[0]['console_source'],'ocean.stdout')
            retained=[row['text'] for row in contexts[0]['lines']]
            for frame in frames:
                self.assertIn(frame,retained)
            self.assertEqual(len(retained),27)
            self.assertNotIn('generated_guard_context',result['first_error'])
            self.assertNotIn('trace_frames',contexts[0])
            projected=native_error_context(lines,2,start_line=101)
            self.assertEqual(projected['error_line'],103)
            self.assertEqual(projected['lines'][0]['line'],101)
            self.assertEqual(projected['lines'][-1]['line'],127)

    def test_native_error_context_count_is_bounded_and_expected_capture_has_none(self):
        with tempfile.TemporaryDirectory() as root:
            script=root+'/capture.il'
            with open(script,'w') as stream:
                stream.write('exit(0)\n')
            with open(root+'/ocean.stdout','w') as stream:
                for number in range(8):
                    stream.write('*Error* native_%d\n' % number + 'following\n'*30)
            result=skill_command_diagnostics({'returncode':0},script,owned_root=root)
            self.assertEqual(len(result['native_error_contexts']),4)
            with open(root+'/ocean.stdout','w') as stream:
                stream.write('ERROR (ASSEMBLER-5031): Simulator failed\n')
            expected=skill_command_diagnostics({'returncode':0},script,owned_root=root)
            self.assertTrue(expected['ok'])
            self.assertNotIn('native_error_contexts',expected)

    def test_exit_zero_and_done_do_not_mask_reader_syntax_error(self):
        with tempfile.TemporaryDirectory() as root:
            script = root + '/capture.il'
            with open(script, 'w') as stream:
                stream.write('sstatus(errsetTrace t)\n')
            error = '*Error* lineread/read: syntax error encountered in input at line 1 column 64 in file ' + script
            with open(root + '/ocean.stdout', 'w') as stream:
                stream.write('CRESCENDO_SKILL_BLOCK captureResult 2/3 lines=129..256\n' + error + '\n' + error + '\nCRESCENDO_PRIVATE_NETLIST_CAPTURE_DONE\n')
            result = skill_command_diagnostics({'returncode': 0}, script,
                                               'CRESCENDO_PRIVATE_NETLIST_CAPTURE_DONE', root)
            self.assertFalse(result['ok'])
            self.assertTrue(result['syntax_error'])
            self.assertTrue(result['required_marker_seen'])
            self.assertEqual(result['distinct_error_count'], 1)
            self.assertIn('possible_indirect_reader', result['errors'][0]['script_context']['context_status'])
            self.assertEqual(result['last_successful_block']['block'], 2)

    def test_capture_only_assembler_failure_is_recorded_without_parser_failure(self):
        with tempfile.TemporaryDirectory() as root:
            script = root + '/capture.il'
            with open(script, 'w') as stream:
                stream.write('exit(0)\n')
            with open(root + '/ocean.stdout', 'w') as stream:
                stream.write('ERROR (ASSEMBLER-5031): Simulator failed\nCRESCENDO_PRIVATE_NETLIST_CAPTURE_DONE\n')
            result = skill_command_diagnostics({'returncode': 0}, script,
                                               'CRESCENDO_PRIVATE_NETLIST_CAPTURE_DONE', root)
            self.assertTrue(result['ok'])
            self.assertEqual(result['capture_wrapper_failure_observations'], 1)
            self.assertFalse(result['native_error'])

    def test_missing_done_and_over_limit_scan_cannot_pass(self):
        with tempfile.TemporaryDirectory() as root:
            script = root + '/capture.il'
            with open(script, 'w') as stream:
                stream.write('exit(0)\n')
            with open(root + '/ocean.stdout', 'w') as stream:
                stream.write('ordinary console output\n')
            self.assertFalse(skill_command_diagnostics({'returncode': 0}, script, 'REQUIRED', root)['ok'])
            with mock.patch('crescendo_engine.native_diagnostics.MAX_SCAN_BYTES', 4):
                result = skill_command_diagnostics({'returncode': 0}, script, None, root)
            self.assertFalse(result['scan_complete'])
            self.assertFalse(result['ok'])


class OwnedLogReaderTests(unittest.TestCase):
    def test_busy_reader_retains_artifacts_and_retry_waits_for_close(self):
        with tempfile.TemporaryDirectory() as root:
            epoch = root + '/controllers/slot_000/epoch_000'
            os.makedirs(epoch)
            with open(epoch + '/controller_liveness.lock', 'w') as stream:
                stream.write('released')
            log = epoch + '/ocean.stdout'
            with open(log, 'w') as stream:
                stream.write('owned log')
            entered, release = threading.Event(), threading.Event()
            state = {}
            def reader():
                with _owned_binary_stream(log, root) as stream:
                    state['stream'] = stream
                    entered.set()
                    release.wait(2)
                    state['text'] = stream.read()
            thread = threading.Thread(target=reader, name='owned-log-reader-fixture')
            thread.start()
            self.assertTrue(entered.wait(10))
            try:
                with mock.patch('crescendo_engine.retention.native_log_cleanup_scope',
                                side_effect=lambda: native_log_cleanup_scope(0.03)):
                    with self.assertRaises(ArtifactCleanupError) as raised:
                        cleanup_abandoned_controller_epoch(epoch, root,
                            {'ok': True, 'kind': 'recorded_pid_is_dead'})
                self.assertTrue(os.path.isdir(epoch))
                self.assertFalse(state['stream'].closed)
                self.assertEqual(raised.exception.detail['reader_barrier']['readers_after_wait']['active_reader_count'], 1)
                self.assertFalse(os.path.lexists(root + '/.cleanup_pending'))
            finally:
                release.set()
                thread.join(2)
            self.assertFalse(thread.is_alive())
            self.assertTrue(state['stream'].closed)
            result = cleanup_abandoned_controller_epoch(epoch, root,
                {'ok': True, 'kind': 'recorded_pid_is_dead'})
            self.assertTrue(result['removed'])
            self.assertTrue(result['reader_barrier']['readers_closed_before_delete'])
            self.assertFalse(os.path.lexists(epoch))

    def test_same_thread_reader_is_not_a_reentrant_cleanup_proof(self):
        with native_log_read_scope():
            with self.assertRaises(OwnedLogReadersBusy):
                with native_log_cleanup_scope(0):
                    self.fail('Must not delete artifacts while holding own reader')


if __name__ == '__main__':
    unittest.main()
