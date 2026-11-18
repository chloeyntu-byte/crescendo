"""Owned fake solver/scheduler recovery, without vendor tools or shared temp paths."""
import copy
import json
import os
import shlex
import subprocess
import sys
import threading
import unittest
from unittest import mock

from tests import test_netlist_execution as fixture
from crescendo_engine import netlist_execution as execution
from crescendo_engine import solver_transient as recovery
from crescendo_engine.utils import atomic_write_text

SIGNATURE = 'Internal error found in spectre during hierarchy flattening.\nFATAL (SPECTRE-18): Segmentation fault.\n'
INITIAL_SETUP_SIGNATURE = SIGNATURE.replace('during hierarchy flattening', 'during initial setup')


class SolverTransientTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixture.NetlistExecutionTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.f = self.fixture
        self.counter = os.path.join(self.f.root, 'attempt-count')

    def solver(self, failures=2, code=134, text=SIGNATURE, log=True, mutate=False):
        task = self.f.task
        script = task['argv'][1]
        atomic_write_text(script, '''import os,sys
counter={counter!r}
n=int(open(counter).read())+1 if os.path.exists(counter) else 1
open(counter,'w').write(str(n))
if n <= {failures!r}:
    if {log!r}: open({path!r},'w').write({text!r})
    if {mutate!r}: open({netlist!r},'a').write('// changed input\\n')
    sys.exit({code!r})
open({path!r},'w').write('spectre completes with 0 errors.\\n')
open({psflog!r},'w').write('owned fake result')
'''.format(counter=self.counter, failures=failures, log=log, path=task['spectre_log'],
           text=text, code=code, psflog=os.path.join(task['psf_dir'], 'logFile'),
           mutate=mutate, netlist=task['netlist_path']))

    def count(self):
        with open(self.counter) as stream:
            return int(stream.read())

    def test_two_crashes_then_success_keep_three_logs_tokens_and_times(self):
        self.solver()
        timing = {}
        result = self.f.context().evaluate(self.f.manifest, timing=timing)
        point = result['task_results'][0]['solver_transient']
        attempts = point['attempts']
        self.assertEqual(point['outcome'], 'completed')
        self.assertEqual(self.count(), 3)
        self.assertEqual(len(set(row['token'] for row in attempts)), 3)
        self.assertEqual([row['argv'] for row in attempts], [self.f.task['argv']] * 3)
        artifacts = result['task_results'][0]['solver_artifacts']['artifacts']
        stdout = next(row for row in artifacts if row['kind'] == 'solver_stdout')
        self.assertIn('attempt_003', stdout['path'])
        for number, row in enumerate(attempts, 1):
            self.assertTrue(row['termination_evidence']['cleanup_proven'])
            self.assertGreater(row['elapsed_seconds'], 0)
            with open(os.path.join(row['evidence']['path'], 'solver.log')) as stream:
                self.assertIn('hierarchy flattening' if number < 3 else '0 errors', stream.read())
            with open(os.path.join(row['evidence']['path'], 'attempt.json')) as stream:
                saved = json.load(stream)
            self.assertEqual(saved['token'], row['token'])
            self.assertEqual(bool(row.get('first_error')), number < 3)
        self.assertAlmostEqual(timing['tasks'][0]['phases']['local_command']['elapsed_seconds'],
                               sum(row['phases']['local_command']['elapsed_seconds'] for row in attempts))
        self.assertGreaterEqual(result['task_results'][0]['elapsed_seconds'], sum(row['elapsed_seconds'] for row in attempts))
        self.assertEqual(result['execution_pipeline']['points'][0]['solver_transient'], point)

    def assert_no_retry(self, **kwargs):
        self.solver(**kwargs)
        with self.assertRaises(execution.NetlistExecutionError) as caught:
            self.f.context().evaluate(self.f.manifest)
        self.assertEqual(self.count(), 1)
        self.assertEqual(len(caught.exception.detail['solver_transient']['attempts']), 1)
        self.assertNotEqual(caught.exception.detail.get('failure_reason'), 'transient_crash_exhausted')
        return caught.exception

    def test_single_keyword_is_not_a_signature(self):
        self.assert_no_retry(text='during hierarchy flattening\n')

    def test_other_single_keyword_is_not_a_signature(self):
        self.assert_no_retry(text='FATAL (SPECTRE-18): Segmentation fault\n')

    def test_initial_setup_crash_retries_then_succeeds(self):
        self.solver(failures=1, text=INITIAL_SETUP_SIGNATURE)
        result = self.f.context().evaluate(self.f.manifest)
        point = result['task_results'][0]['solver_transient']
        self.assertEqual(self.count(), 2)
        self.assertEqual(point['outcome'], 'completed')
        first, second = point['attempts']
        self.assertEqual(first['transient_crash']['signature'], 'initial_setup')
        self.assertEqual(first['first_error'], INITIAL_SETUP_SIGNATURE.splitlines()[0])
        self.assertNotEqual(first['token'], second['token'])
        self.assertTrue(first['termination_evidence']['cleanup_proven'])
        for attempt, expected in ((first, INITIAL_SETUP_SIGNATURE), (second, '0 errors')):
            with open(os.path.join(attempt['evidence']['path'], 'solver.log')) as stream:
                self.assertIn(expected, stream.read())

    def test_initial_setup_exhaustion_is_bounded_infrastructure_failure(self):
        self.solver(failures=20, text=INITIAL_SETUP_SIGNATURE)
        with self.assertRaises(execution.NetlistExecutionError) as caught:
            self.f.context().evaluate(self.f.manifest)
        self.assertEqual(self.count(), 3)
        self.assertTrue(caught.exception.infrastructure)
        self.assertEqual(caught.exception.detail['first_cause'], 'transient_crash_exhausted')
        attempts = caught.exception.detail['solver_transient']['attempts']
        self.assertTrue(all(row['transient_crash']['signature'] == 'initial_setup' for row in attempts))
        self.assertTrue(all(row['termination_evidence']['cleanup_proven'] for row in attempts))

    def test_flattening_without_internal_error_does_not_retry(self):
        self.assert_no_retry(text='during hierarchy flattening\nFATAL (SPECTRE-18): Segmentation fault.\n')

    def test_initial_setup_without_internal_error_does_not_retry(self):
        self.assert_no_retry(text='during initial setup\nFATAL (SPECTRE-18): Segmentation fault.\n')

    def test_internal_error_and_spectre_code_without_stage_do_not_retry(self):
        self.assert_no_retry(text='Internal error found in spectre\nFATAL (SPECTRE-18): Segmentation fault.\n')

    def test_initial_setup_without_spectre_code_does_not_retry(self):
        self.assert_no_retry(text='Internal error found in spectre during initial setup.\n')

    def test_initial_setup_with_ordinary_exit_code_does_not_retry(self):
        self.assert_no_retry(code=1, text=INITIAL_SETUP_SIGNATURE)

    def test_both_stage_signatures_require_all_current_keywords(self):
        task = dict(self.f.task)
        files = dict.fromkeys(('simulator.stdout', 'simulator.stderr'), os.path.join(self.f.root, 'missing'))
        for signature in (SIGNATURE, INITIAL_SETUP_SIGNATURE):
            for code in (134, -11, 139, 1, 0):
                with self.subTest(signature=signature, code=code):
                    atomic_write_text(task['spectre_log'], signature)
                    match = recovery.match_transient_crash(task, files, {'returncode': code}, self.f.root, 0)
                    self.assertEqual(bool(match), code in (134, -11, 139))
            for missing in ('Internal error found in spectre', 'SPECTRE-18',
                            'during hierarchy flattening' if signature == SIGNATURE else 'during initial setup'):
                with self.subTest(signature=signature, missing=missing):
                    atomic_write_text(task['spectre_log'], signature.replace(missing, ''))
                    self.assertIsNone(recovery.match_transient_crash(task, files, {'returncode': 134}, self.f.root, 0))

    def test_ordinary_nonzero_is_not_a_signature(self):
        self.assert_no_retry(code=1)

    def test_foreign_log_does_not_authorize_retry(self):
        atomic_write_text(os.path.join(self.f.root, 'foreign.log'), SIGNATURE)
        self.assert_no_retry(log=False)

    def test_unchanged_preexisting_psf_log_is_not_from_this_attempt(self):
        atomic_write_text(os.path.join(self.f.task['psf_dir'], 'log'), SIGNATURE)
        self.assert_no_retry(log=False)

    def appended_psf_log(self, old, current):
        self.solver(failures=1, log=False)
        path = os.path.join(self.f.task['psf_dir'], 'log')
        atomic_write_text(path, old)
        script = self.f.task['argv'][1]
        with open(script) as stream:
            source = stream.read()
        source = source.replace('    sys.exit(134)',
            "    open(%r, 'a').write(%r)\n    sys.exit(134)" % (path, current))
        atomic_write_text(script, source)
        return path

    def test_appended_unsigned_error_cannot_reuse_old_signature(self):
        self.appended_psf_log(SIGNATURE, 'current unsigned failure\n')
        with self.assertRaises(execution.NetlistExecutionError) as caught:
            self.f.context().evaluate(self.f.manifest)
        self.assertEqual(self.count(), 1)
        self.assertFalse(caught.exception.detail['solver_transient']['attempts'][0]['transient_crash'])

    def test_keywords_cannot_straddle_prelaunch_extent(self):
        for old, current in [('Internal error found in spectre during hierarchy flattening\n', 'SPECTRE-18\n'),
                             ('SPECTRE-18\n', 'Internal error found in spectre during hierarchy flattening\n'),
                             ('Internal error found in spectre during hierarchy flat', 'tening\nSPECTRE-18\n'),
                             ('Internal error found in spectre during initial setup\n', 'SPECTRE-18\n')]:
            with self.subTest(old=old):
                path = self.appended_psf_log(old, current)
                task = dict(self.f.task, solver_log_baseline=recovery.solver_log_baseline(self.f.task))
                with open(path, 'a') as stream:
                    stream.write(current)
                files = dict.fromkeys(('simulator.stdout', 'simulator.stderr'), os.path.join(self.f.root, 'missing'))
                self.assertIsNone(recovery.match_transient_crash(task, files, {'returncode': 134}, self.f.root, 0))

    def test_new_appended_signature_retries_and_preserves_full_log(self):
        old = 'previous unsigned failure\n'
        self.appended_psf_log(old, SIGNATURE)
        result = self.f.context().evaluate(self.f.manifest)
        attempts = result['task_results'][0]['solver_transient']['attempts']
        self.assertEqual(self.count(), 2)
        match = attempts[0]['transient_crash']
        self.assertEqual(match['source_identity']['attempt_start_offset'], len(old.encode('utf-8')))
        self.assertEqual(match['first_error'], SIGNATURE.splitlines()[0])
        with open(os.path.join(attempts[0]['evidence']['path'], 'solver_psf.log')) as stream:
            self.assertEqual(stream.read(), old + SIGNATURE)

    def test_inplace_rewrite_cannot_reuse_prelaunch_bytes(self):
        path = os.path.join(self.f.task['psf_dir'], 'log')
        atomic_write_text(path, SIGNATURE + 'old tail\n')
        task = dict(self.f.task, solver_log_baseline=recovery.solver_log_baseline(self.f.task))
        with open(path, 'w') as stream:
            stream.write(SIGNATURE)
        files = dict.fromkeys(('simulator.stdout', 'simulator.stderr'), os.path.join(self.f.root, 'missing'))
        self.assertIsNone(recovery.match_transient_crash(task, files, {'returncode': 134}, self.f.root, 0))

    def test_signature_split_across_logs_is_not_a_match(self):
        self.solver(text='Internal error found in spectre during hierarchy flattening\n')
        with open(self.f.task['argv'][1], 'a') as stream:
            # This line is intentionally unreachable after sys.exit: write a
            # separate pre-existing log to ensure cross-log merging is rejected.
            stream.write("\n")
        atomic_write_text(os.path.join(self.f.task['psf_dir'], 'log'), 'SPECTRE-18')
        with self.assertRaises(execution.NetlistExecutionError) as caught:
            self.f.context().evaluate(self.f.manifest)
        self.assertNotIn('transient_crash', caught.exception.detail)
        self.assertEqual(self.count(), 1)

    def test_exhaustion_is_infrastructure_with_original_first_errors(self):
        self.solver(failures=20)
        with self.assertRaises(execution.NetlistExecutionError) as caught:
            self.f.context().evaluate(self.f.manifest)
        error = caught.exception
        self.assertTrue(error.infrastructure)
        self.assertEqual(str(error), 'transient_crash_exhausted')
        self.assertEqual(error.detail['first_cause'], 'transient_crash_exhausted')
        self.assertEqual(self.count(), 3)
        self.assertTrue(all(row['first_error'] == SIGNATURE.splitlines()[0] for row in error.detail['solver_transient']['attempts']))

    def test_zero_disables_retry(self):
        self.f.config['runtime']['solver_transient_retries'] = 0
        self.solver(failures=20)
        with self.assertRaises(execution.NetlistExecutionError) as caught:
            self.f.context().evaluate(self.f.manifest)
        self.assertEqual(self.count(), 1)
        self.assertEqual(caught.exception.detail['first_cause'], 'transient_crash_exhausted')

    def test_stop_between_attempts_retains_clean_proof_and_never_resubmits(self):
        self.solver()
        context = self.f.context()
        def progress(event):
            if event.get('stage') == 'solver_transient_retry':
                context.cancel()
        context.progress_callback = progress
        with self.assertRaises(execution.NetlistExecutionError) as caught:
            context.evaluate(self.f.manifest)
        self.assertEqual(self.count(), 1)
        self.assertTrue(caught.exception.detail['cancelled'])
        self.assertTrue(context.close()['cleanup_proven'])
        point = caught.exception.detail['solver_transient']
        self.assertEqual(point['outcome'], 'cancelled')
        self.assertTrue(point['attempts'][0]['termination_evidence']['cleanup_proven'])

    def test_stop_during_second_attempt_proves_cleanup_and_blocks_third(self):
        self.solver()
        ready = os.path.join(self.f.root, 'second-started')
        script = self.f.task['argv'][1]
        with open(script, encoding='utf-8') as stream:
            source = stream.read()
        source = source.replace("open(counter,'w').write(str(n))", "open(counter,'w').write(str(n))\nif n == 2:\n    open(%r,'w').close()\n    __import__('time').sleep(30)" % ready)
        atomic_write_text(script, source)
        context = self.f.context()
        finished, seen = threading.Event(), []
        def stop():
            for _ in range(1000):
                if os.path.isfile(ready):
                    seen.append(True)
                    context.cancel()
                    return
                if finished.wait(0.01):
                    return
        worker = threading.Thread(target=stop)
        worker.start()
        try:
            with self.assertRaises(execution.NetlistExecutionError) as caught:
                context.evaluate(self.f.manifest, timeout_seconds=30)
        finally:
            finished.set()
            worker.join(12)
        self.assertEqual(seen, [True])
        self.assertEqual(self.count(), 2)
        self.assertTrue(caught.exception.detail['cancelled'])
        self.assertEqual(len(caught.exception.detail['solver_transient']['attempts']), 2)
        self.assertTrue(context.close()['cleanup_proven'])

    def test_unproven_cleanup_blocks_retry_and_keeps_psf(self):
        self.solver()
        context = self.f.context()
        with mock.patch.object(execution, 'task_termination_evidence', return_value={'cleanup_proven': False}), \
                self.assertRaises(execution.NetlistExecutionError) as caught:
            context.evaluate(self.f.manifest)
        self.assertEqual(self.count(), 1)
        self.assertTrue(caught.exception.detail['termination_failed'])
        self.assertTrue(os.path.isfile(self.f.task['spectre_log']))
        self.assertTrue(context.unsafe.is_set())
        self.assertTrue(context.close()['cleanup_proven'])

    def test_changed_netlist_is_never_retried_with_changed_input(self):
        self.solver(mutate=True)
        with self.assertRaises(execution.NetlistExecutionError) as caught:
            self.f.context().evaluate(self.f.manifest)
        self.assertEqual(self.count(), 1)
        self.assertIn('Frozen solver input changed', str(caught.exception))

    def test_last_retry_single_core_changes_only_execution_prefix(self):
        self.solver()
        self.f.config['runtime']['solver_transient_last_retry_single_core'] = True
        with mock.patch.object(recovery, 'last_retry_prefix', return_value=(['/usr/bin/env'], 'entire_solve_single_cpu')) as prefix:
            result = self.f.context().evaluate(self.f.manifest)
        prefix.assert_called_once_with(True)
        attempts = result['task_results'][0]['solver_transient']['attempts']
        self.assertEqual(attempts[-1]['argv'], ['/usr/bin/env'] + self.f.task['argv'])
        self.assertEqual(attempts[0]['argv'], self.f.task['argv'])
        self.assertEqual(result['task_results'][0]['task']['argv'], self.f.task['argv'])
        with open(os.path.join(attempts[-1]['control_path'], 'execute.sh')) as stream:
            self.assertIn('setsid /usr/bin/env ', stream.read())

    def test_missing_taskset_records_reason_then_retries_normally(self):
        self.solver()
        self.f.config['runtime']['solver_transient_last_retry_single_core'] = True
        with mock.patch.object(recovery.shutil, 'which', return_value=None):
            result = self.f.context().evaluate(self.f.manifest)
        self.assertEqual(result['task_results'][0]['solver_transient']['attempts'][-1]['single_core_reason'], 'taskset_unavailable')

    def scheduler(self, terminal=True, stop=False, reuse_id=False):
        binaries = self.f._positional_scheduler()
        calls = []
        context = [None]
        def run(argv, **kwargs):
            if argv[0] == binaries['batch-submit']:
                number = sum(row[0] == 'submit' for row in calls) + 1
                identifier = str(4001 if reuse_id else 4000 + number)
                if number > 1:
                    self.assertIn(('terminal', str(4000 + number - 1)), calls)
                calls.append(('submit', identifier))
                os.close(kwargs['guardian_launch_lock_fd'])
                completed = subprocess.run(['/bin/sh', argv[-1]], cwd=kwargs['cwd'], env=kwargs['env'],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10)
                self.assertIn(completed.returncode, (0, 134))
                return {'returncode': 0, 'stdout_tail': 'JOB_ID: %s\n' % identifier}
            self.assertEqual(argv[0], binaries['batch-query'])
            identifier = argv[-1]
            if terminal:
                calls.append(('terminal', identifier))
            if stop:
                context[0].cancel()
            return {'returncode': 0, 'stdout_tail': 'JOB_ID %s\nJOB_STATE %s\nEXIT_CODE %s\n' %
                    (identifier, 'FAILED' if self.count() < 3 else 'SUCCEEDED', 134 if self.count() < 3 else 0)} if terminal else {
                    'returncode': 0, 'stdout_tail': 'JOB_ID %s\nJOB_STATE RUNNING\n' % identifier}
        self.f.config['runtime']['execution_commands']['submission_template'] = shlex.quote(binaries['batch-submit']) + ' {job_script}'
        context[0] = self.f.context(command_runner=run)
        return context[0], calls

    def test_cluster_each_retry_gets_new_exact_job_id_token_and_terminal_proof(self):
        self.solver()
        self.f.config['runtime']['solver_transient_last_retry_single_core'] = True
        context, calls = self.scheduler()
        with mock.patch.object(recovery, 'last_retry_prefix', side_effect=AssertionError('Cluster must not resolve taskset')):
            result = context.evaluate(self.f.manifest)
        attempts = result['task_results'][0]['solver_transient']['attempts']
        self.assertEqual([row['scheduler_job_id'] for row in attempts], ['4001', '4002', '4003'])
        self.assertEqual(len(set(row['token'] for row in attempts)), 3)
        self.assertTrue(all(row['single_core_reason'] == 'ignored_cluster_mode' for row in attempts))
        self.assertTrue(all(row['scheduler_observation']['receipt_token'] == row['token'] for row in attempts))
        self.assertEqual([row for row in calls if row[0] == 'submit'], [('submit', '4001'), ('submit', '4002'), ('submit', '4003')])

    def test_cluster_reused_job_id_is_rejected_before_another_retry(self):
        self.solver()
        context, calls = self.scheduler(reuse_id=True)
        with self.assertRaises(execution.NetlistExecutionError) as caught:
            context.evaluate(self.f.manifest)
        self.assertEqual(self.count(), 2)
        self.assertTrue(caught.exception.detail['submission_identity_failed'])
        self.assertTrue(context.close()['cleanup_proven'])

    def test_cluster_without_scheduler_terminal_never_resubmits(self):
        self.solver()
        context, calls = self.scheduler(terminal=False)
        with self.assertRaises(execution.NetlistExecutionError):
            context.evaluate(self.f.manifest)
        self.assertEqual(self.count(), 1)
        self.assertEqual(len([row for row in calls if row[0] == 'submit']), 1)
        self.assertTrue(context.close()['cleanup_proven'])

    def test_cluster_stop_after_crash_never_resubmits(self):
        self.solver()
        context, calls = self.scheduler(stop=True)
        with self.assertRaises(execution.NetlistExecutionError):
            context.evaluate(self.f.manifest)
        self.assertEqual(self.count(), 1)
        self.assertTrue(context.close()['cleanup_proven'])

    def test_signal_code_and_alps_empty_profile(self):
        self.solver()
        task = dict(self.f.task)
        atomic_write_text(task['spectre_log'], SIGNATURE)
        files = {'simulator.stdout': os.path.join(self.f.root, 'missing'), 'simulator.stderr': os.path.join(self.f.root, 'missing')}
        self.assertIsNotNone(recovery.match_transient_crash(task, files, {'returncode': -11}, self.f.root, 0))
        task['execution_simulator'] = 'alps'
        self.assertIsNone(recovery.match_transient_crash(task, files, {'returncode': 134}, self.f.root, 0))


class RetryConfigurationTests(unittest.TestCase):
    def test_new_freeze_and_old_checkpoint_absence(self):
        from crescendo_engine.execution_settings import freeze_execution_settings
        spec, config = {}, {'runtime': {'solver_transient_retries': 4, 'solver_transient_last_retry_single_core': True}}
        freeze_execution_settings(spec, config)
        self.assertEqual(spec['solver_transient_retries'], 4)
        self.assertTrue(spec['solver_transient_last_retry_single_core'])
        config['runtime']['solver_transient_retries'] = 9
        freeze_execution_settings(spec, config, resuming=True)
        self.assertEqual(spec['solver_transient_retries'], 4)
        old = {}
        freeze_execution_settings(old, config, resuming=True)
        self.assertNotIn('solver_transient_retries', old)
        self.assertNotIn('solver_transient_last_retry_single_core', old)
        self.assertEqual(config['runtime']['solver_transient_retries'], 2)
        self.assertFalse(config['runtime']['solver_transient_last_retry_single_core'])

    def test_invalid_settings_rejected_and_defaults(self):
        self.assertEqual(recovery.normalize_retry_settings({}), {'solver_transient_retries': 2, 'solver_transient_last_retry_single_core': False})
        for value in (-1, True, 1.5, 'nan', None):
            with self.subTest(value=value), self.assertRaises(ValueError):
                recovery.normalize_retry_settings({'solver_transient_retries': value})
        with self.assertRaises(ValueError):
            recovery.normalize_retry_settings({'solver_transient_last_retry_single_core': 'false'})

    def test_taskset_uses_available_affinity_and_cluster_does_not_probe(self):
        with mock.patch.object(recovery.shutil, 'which', return_value='/bin/taskset'), \
                mock.patch.object(recovery.os, 'sched_getaffinity', return_value={7, 12}):
            self.assertEqual(recovery.last_retry_prefix(True)[0], ['/bin/taskset', '-c', '7'])
        with mock.patch.object(recovery.shutil, 'which', side_effect=AssertionError('No cluster probe')):
            self.assertEqual(recovery.last_retry_prefix(False), ([], 'ignored_cluster_mode'))

    def test_checkpoint_identity_rejects_one_sided_changes(self):
        from tests.test_run_profile_checkpoint import RunProfileCheckpointTests
        f = RunProfileCheckpointTests()
        for key, value in [('solver_transient_retries', 3), ('solver_transient_last_retry_single_core', True)]:
            checkpoint = f.checkpoint()
            checkpoint['spec'][key] = value
            with self.assertRaisesRegex(ValueError, key + ' identities disagree'):
                f.validate(checkpoint)
            checkpoint['manifest'][key] = value
            f.validate(checkpoint)

    def test_unified_configuration_roundtrip(self):
        from crescendo_engine.unified_configuration import build_configuration, normalize_configuration
        raw = {'optimization': {'solver_transient_retries': '5', 'solver_transient_last_retry_single_core': True}}
        document = build_configuration(raw, {})
        result = normalize_configuration(document, {})['settings']['optimization']
        self.assertEqual(result['solver_transient_retries'], '5')
        self.assertTrue(result['solver_transient_last_retry_single_core'])

    def test_diagnostic_observation_counts_without_deciding(self):
        from crescendo_engine.diagnostic_focus import focused_checks
        row = {'case_results': {'Local': {}, 'Cluster': {}}, 'focused_cases': {
            'Local': {'candidates': [{'solver_transient': {'points': [
                {'outcome': 'completed', 'attempts': [{'transient_crash': True}, {'transient_crash': True}, {}]},
                {'outcome': 'transient_crash_exhausted', 'attempts': [{'transient_crash': True}]},
                {'outcome': 'completed', 'attempts': [{}]}]}}]}}, 'prepared': {}, 'prepared_statistics': {}}
        checks = focused_checks(row)
        observed = checks.pop('solver_transient')
        self.assertTrue(observed['observation_only'])
        self.assertEqual(observed['modes']['Local'], {'transient_crashes': 3, 'retries': 2, 'finally_succeeded_points': 1,
            'exhausted_points': 1, 'observed_points': 3})
        before = copy.deepcopy(checks)
        row['focused_cases']['Local']['candidates'][0].pop('solver_transient')
        after = focused_checks(row)
        after.pop('solver_transient')
        self.assertEqual(before, after)

    def test_shared_candidate_statistics_count_physical_execution_once(self):
        import concurrent.futures
        import tempfile
        from crescendo_engine.netlist_optimizer import PrivateNetlistOptimizer
        point = {'task_id': 'a', 'outcome': 'completed', 'attempts': [
            {'token': 'crash1', 'transient_crash': True}, {'token': 'success2'}]}
        physical = {'candidate_id': 'g000_c0000', 'status': 'completed',
                    'solver_transient': {'points': [point]}}
        adapter = PrivateNetlistOptimizer.__new__(PrivateNetlistOptimizer)
        adapter.lock = threading.Lock()
        with tempfile.TemporaryDirectory() as root:
            adapter.run_root = root
            adapter._evaluate_candidate = lambda *args: copy.deepcopy(physical)
            generated = {'future': concurrent.futures.Future(),
                'manifest': {'candidate_id': 'g000_c0000'},
                'batch_reference': {'batch_epoch': 'epoch', 'physical_ids': ['row0']}}
            rows = [adapter.evaluate(index, 0, {'x': 0.5}, generated) for index in (0, 1)]
            self.assertEqual(rows[1]['shared_with'], rows[0]['candidate_id'])
            expected = {'transient_crashes': 1, 'retries': 1, 'finally_succeeded_points': 1,
                        'exhausted_points': 0, 'observed_points': 1}
            self.assertEqual(recovery.retry_observations(rows), expected)
            self.assertEqual(recovery.retry_observations(list(reversed(rows))), expected)
            for row in rows:
                row['solver_transient']['points'][0]['outcome'] = 'transient_crash_exhausted'
            expected.update(finally_succeeded_points=0, exhausted_points=1)
            self.assertEqual(recovery.retry_observations(rows), expected)

    def test_statistics_keep_independent_tokens_and_unidentified_legacy_points(self):
        point = {'task_id': 'same', 'outcome': 'completed', 'attempts': [{'token': 'one'}]}
        row = {'solver_transient': {'points': [point]}}
        other = copy.deepcopy(row)
        other['solver_transient']['points'][0]['attempts'][0]['token'] = 'two'
        self.assertEqual(recovery.retry_observations([row, other])['observed_points'], 2)
        for candidate in (row, other):
            candidate['solver_transient']['points'][0]['attempts'] = [{}]
        self.assertEqual(recovery.retry_observations([row, other])['observed_points'], 2)
        for candidate in (row, other):
            candidate.update(physical_execution_candidate='g000_c0000', batch_reference={'batch_epoch': 'epoch'})
        self.assertEqual(recovery.retry_observations([row, other])['observed_points'], 1)
        other['batch_reference']['batch_epoch'] = 'another'
        self.assertEqual(recovery.retry_observations([row, other])['observed_points'], 2)

    def test_exhaustion_candidate_has_no_design_penalty(self):
        from tests.test_netlist_optimizer import fixtures, model_task
        from crescendo_engine.netlist_optimizer import PrivateNetlistOptimizer
        from crescendo_engine import runner
        import tempfile
        with tempfile.TemporaryDirectory() as root:
            prepared, config, spec = fixtures(root)
            penalty = mock.Mock(side_effect=AssertionError('Infrastructure must not get design penalty'))
            adapter = PrivateNetlistOptimizer(root, spec, prepared, prepared['parameters'], config, penalty)
            adapter.preparing = True
            adapter.prepared_event.set()
            adapter.manifest = {'source_identity': {}}
            adapter.context = mock.Mock()
            adapter.context.unsafe.is_set.return_value = False
            point = {'outcome': 'transient_crash_exhausted', 'attempts': [{'transient_crash': True}]}
            def fail(*args, **kwargs):
                kwargs['timing']['tasks'] = [{'phases': {'solver_transient': point}}]
                raise execution.NetlistExecutionError('transient_crash_exhausted', {'failure_stage': 'solve', 'first_cause': 'transient_crash_exhausted'})
            adapter.context.evaluate.side_effect = fail
            with mock.patch('crescendo_engine.netlist_optimizer.materialize_candidate', return_value={'tasks': [model_task()]}):
                row = adapter.evaluate(0, 0, {'x': 0.5})
            self.assertEqual(row['status'], 'infrastructure_failed')
            self.assertEqual(row['failure_class'], 'private_netlist_infrastructure')
            self.assertEqual(row['first_cause'], 'transient_crash_exhausted')
            self.assertNotIn('metrics', row)
            penalty.assert_not_called()
