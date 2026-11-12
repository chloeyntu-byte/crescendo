"""Owned fake ALPS processes and synthetic PSF values; no native EDA claim."""
import copy
import json
import os
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

from crescendo_engine import netlist_execution as execution
from crescendo_engine import netlist_measurement as measurement
from crescendo_engine import simulators
from crescendo_engine import native_log_io
from crescendo_engine.owned_solver_logs import owned_solver_log_reader
from crescendo_engine.utils import atomic_write_text, sha256_file


class AlpsPrivateExecutionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="crescendo ALPS ' execution ")
        self.root = self.temp.name
        self.contexts = []
        self.models = os.path.join(self.root, 'models')
        os.makedirs(self.models)
        self.mode = os.path.join(self.root, 'mode')
        self.capture = os.path.join(self.root, 'launch-argv.json')
        self.executable = os.path.join(self.root, 'owned fake alps')
        atomic_write_text(self.mode, 'success')
        atomic_write_text(self.executable, '''#!%s
import json, os, sys
argv = sys.argv[1:]
json.dump(argv, open(%r, 'w'))
assert len(argv) == 9, argv
assert argv[1:3] == ['-format', 'ps'], argv
assert argv[3] == '-o' and argv[5] == '-I' and argv[7:9] == ['-mt', '6'], argv
assert argv[6] == %r and os.path.isfile(argv[0]), argv
mode = open(%r).read().strip()
if mode == 'spectre_stdout': print('spectre completes with 0 errors.')
if mode != 'missing_psf':
    psf = argv[4]
    if mode in ('companion_regular', 'companion_alias'):
        log = psf + '.log' if mode == 'companion_regular' else os.path.join(psf, 'native-alps.log')
        with open(log, 'w') as stream: stream.write('current owned ALPS fixture result log')
        if mode == 'companion_alias': os.symlink('psf/native-alps.log', psf + '.log')
    else:
        with open(os.path.join(psf, 'logFile'), 'w') as stream:
            stream.write('' if mode == 'empty_psf' else 'owned ALPS fixture PSF layout')
        if mode == 'stale_psf': os.utime(os.path.join(psf, 'logFile'), (1, 1))
        if mode == 'unsafe_primary':
            log = os.path.join(os.path.dirname(psf), 'other-task.log')
            with open(log, 'w') as stream: stream.write('outside current PSF')
            os.symlink(log, psf + '.log')
        if mode == 'stale_primary':
            with open(psf + '.log', 'w') as stream: stream.write('old result log')
            os.utime(psf + '.log', (1, 1))
        if mode == 'dangling_primary':
            os.symlink('psf/missing.log', psf + '.log')
    values = {} if mode == 'missing_parameters' else {'CRESCENDO_NL_p': 900 if mode == 'wrong_parameters' else 1000}
    json.dump(values, open(os.path.join(psf, 'fixture_parameters.json'), 'w'))
    if mode == 'spectre_log':
        with open(os.path.join(os.path.dirname(psf), 'alps.out'), 'w') as stream:
            stream.write('spectre completes with 0 errors.\\n')
print('owned fixture process exited')
''' % (sys.executable, self.capture, self.models, self.mode))
        os.chmod(self.executable, 0o700)
        self.config = {'cadence': {'simulator': 'alps', 'alps_bin': self.executable,
                                  'alps_model_dir': self.models, 'simulator_threads': 6},
                       'runtime': {}}
        self.prepared = {'job_policy': {'available': True, 'global': {'properties': {
            'distributionmethod': 'Local', 'maxjobs': '1'}}, 'tests': []}}
        self.spec = {'simulator': 'alps', 'candidate_timeout_seconds': 3,
                     'objectives': [{'output_key': 'rc::gain'}], 'constraints': []}
        task_root = os.path.join(self.root, 'tasks', 'rc_tt_1')
        os.makedirs(os.path.join(task_root, 'netlist'))
        self.psf = os.path.join(task_root, 'psf')
        os.mkdir(self.psf)
        netlist = os.path.join(task_root, 'netlist', 'input.scs')
        atomic_write_text(netlist, 'simulator lang=spectre\nparameters CRESCENDO_NL_p=1000\n')
        contract = simulators.execution_contract(self.config, 'spectre')
        log = os.path.join(task_root, 'alps.out')
        self.task = {'task_id': 'rc_tt_1', 'test': 'rc', 'corner': 'tt', 'point_id': 1,
                     'netlist_path': netlist, 'netlist_sha256': sha256_file(netlist),
                     'cwd': task_root, 'psf_dir': self.psf, 'spectre_log': log,
                     'simulator_log': log, 'execution_simulator': 'alps',
                     'simulator_contract': contract,
                     'variables': [{'name': 'CRESCENDO_NL_p', 'value': 1000}],
                     'native_parameters': [{'name': 'CRESCENDO_NL_p', 'value': 1000}],
                     'outputs': [{'key': 'rc::gain', 'name': 'gain', 'expression': '1.0'}]}
        self.task['argv'] = simulators.build_simulator_argv(contract, [], netlist, self.psf, log, None)
        self.manifest = {'schema_version': 'crescendo.private_netlist.v1', 'root': self.root,
                         'candidate_id': 'g000_c0000', 'template_sha256': 'fixture',
                         'tasks': [self.task]}
        self.measured = []

    def tearDown(self):
        for context in self.contexts:
            context.close(cancel=True)
        self.temp.cleanup()

    def set_mode(self, mode):
        atomic_write_text(self.mode, mode)

    def evaluator(self, task, config, root, **kwargs):
        self.measured.append(task['task_id'])
        # The reader is synthetic; the production parameter comparison is real.
        with open(os.path.join(task['psf_dir'], 'fixture_parameters.json')) as stream:
            values = json.load(stream)
        proof = measurement.verify_actual_parameters(task, values)
        return {'execution_simulator': 'alps', 'native_parameter_readback_verified': proof,
                'effective_variables': values,
                'outputs': [dict(task['outputs'][0], value=0.5, error=None)]}

    def context(self, **kwargs):
        context = execution.NetlistExecutionContext(self.config, self.prepared, self.spec,
            measurement_evaluator=kwargs.pop('measurement_evaluator', self.evaluator), **kwargs)
        self.contexts.append(context)
        return context

    def test_native_result_parameter_proof_is_explicit_in_alps_aggregate(self):
        result = self.context().evaluate(self.manifest)
        with open(self.capture) as stream:
            self.assertEqual(json.load(stream), self.task['argv'][1:])
        completion = result['completion']
        self.assertEqual(completion['execution_simulator'], 'alps')
        self.assertEqual(completion['spectre_runs'], [])
        self.assertTrue(completion['native_parameter_readback_verified'])
        self.assertEqual(completion['simulator_runs'][0]['execution_simulator'], 'alps')
        self.assertTrue(result['task_results'][0]['receipt']['cleanup_proven'])
        self.assertEqual(result['task_results'][0]['measurement']['native_parameter_readback_verified']['actual'],
                         {'CRESCENDO_NL_p': 1000})
        self.assertEqual(result['parsed']['output_map']['rc::gain']['summary']['mean'], 0.5)

    def test_zero_exit_without_psf_cannot_reach_measurement(self):
        self.set_mode('missing_psf')
        with self.assertRaises(execution.NetlistExecutionError):
            self.context().evaluate(self.manifest)
        self.assertEqual(self.measured, [])
        self.assertFalse(os.path.exists(os.path.join(self.root, 'cli_results.json')))

    def test_real_format_companion_log_can_qualify_without_spectre_logfile(self):
        self.set_mode('companion_regular')
        result = self.context().evaluate(self.manifest)
        self.assertFalse(os.path.exists(os.path.join(self.psf, 'logFile')))
        log = result['task_results'][0]['spectre']['solver_log']
        self.assertEqual(log['kind'], 'alps_psf_log')
        self.assertEqual(log['source_path'], self.psf + '.log')
        self.assertTrue(log['fresh'])
        self.assertTrue(result['completion']['native_parameter_readback_verified'])

    def test_exact_companion_alias_to_current_psf_is_accepted_as_log_only(self):
        self.set_mode('companion_alias')
        result = self.context().evaluate(self.manifest)
        log = result['task_results'][0]['spectre']['solver_log']
        self.assertTrue(log['leaf_link'])
        self.assertEqual(log['resolved_source_path'], os.path.join(self.psf, 'native-alps.log'))
        self.assertEqual(self.measured, ['rc_tt_1'])

    def assert_primary_rejected(self, mode):
        self.set_mode(mode)
        with self.assertRaisesRegex(execution.NetlistExecutionError, 'unsafe log rejected'):
            self.context().evaluate(self.manifest)
        self.assertTrue(os.path.isfile(os.path.join(self.psf, 'logFile')))
        self.assertEqual(self.measured, [])
        self.assertFalse(os.path.exists(os.path.join(self.root, 'cli_results.json')))

    def test_unsafe_primary_cannot_hide_behind_fresh_fallback(self):
        self.assert_primary_rejected('unsafe_primary')

    def test_stale_primary_cannot_hide_behind_fresh_fallback(self):
        self.assert_primary_rejected('stale_primary')

    def test_dangling_primary_cannot_hide_behind_fresh_fallback(self):
        self.assert_primary_rejected('dangling_primary')

    def test_missing_or_wrong_actual_parameters_cannot_complete(self):
        self.set_mode('wrong_parameters')
        with self.assertRaisesRegex(execution.NetlistExecutionError, 'actual PSF parameter differs'):
            self.context().evaluate(self.manifest)
        self.assertEqual(self.measured, ['rc_tt_1'])
        self.assertFalse(os.path.exists(os.path.join(self.root, 'cli_results.json')))

    def test_missing_actual_parameters_cannot_complete(self):
        self.set_mode('missing_parameters')
        with self.assertRaisesRegex(execution.NetlistExecutionError, 'actual PSF parameter differs'):
            self.context().evaluate(self.manifest)
        self.assertFalse(os.path.exists(os.path.join(self.root, 'cli_results.json')))

    def test_spectre_summary_in_stdout_is_rejected_before_measurement(self):
        self.set_mode('spectre_stdout')
        with self.assertRaisesRegex(execution.NetlistExecutionError, 'conflicting Spectre'):
            self.context().evaluate(self.manifest)
        self.assertEqual(self.measured, [])

    def test_spectre_summary_in_owned_simulator_log_is_rejected(self):
        self.set_mode('spectre_log')
        with self.assertRaisesRegex(execution.NetlistExecutionError, 'conflicting Spectre'):
            self.context().evaluate(self.manifest)
        self.assertEqual(self.measured, [])
        with open(self.task['simulator_log']) as stream:
            self.assertEqual(stream.read(), 'spectre completes with 0 errors.\n')
        self.assertFalse(os.path.exists(os.path.join(self.root, 'cli_results.json')))

    def test_empty_or_stale_psf_is_infrastructure_failure_without_design_penalty(self):
        for mode in ('empty_psf', 'stale_psf'):
            with self.subTest(mode=mode):
                manifest = copy.deepcopy(self.manifest)
                task = manifest['tasks'][0]
                task['task_id'] = mode
                task['cwd'] = os.path.join(self.root, 'tasks', mode)
                os.makedirs(os.path.join(task['cwd'], 'netlist'))
                task['psf_dir'] = os.path.join(task['cwd'], 'psf')
                os.mkdir(task['psf_dir'])
                task['netlist_path'] = os.path.join(task['cwd'], 'netlist', 'input.scs')
                atomic_write_text(task['netlist_path'], 'simulator lang=spectre\nparameters CRESCENDO_NL_p=1000\n')
                task['netlist_sha256'] = sha256_file(task['netlist_path'])
                task['spectre_log'] = task['simulator_log'] = os.path.join(task['cwd'], 'alps.out')
                task['argv'] = simulators.build_simulator_argv(task['simulator_contract'], [],
                    task['netlist_path'], task['psf_dir'], task['simulator_log'], None)
                self.set_mode(mode)
                with self.assertRaisesRegex(execution.NetlistExecutionError, 'empty or stale') as failure:
                    self.context().evaluate(manifest)
                self.assertTrue(failure.exception.infrastructure)
                self.assertTrue(os.path.isfile(os.path.join(task['psf_dir'], 'logFile')))
                self.assertEqual(self.measured, [])
                self.assertFalse(os.path.exists(os.path.join(self.root, 'cli_results.json')))

    def test_evaluator_without_native_parameter_proof_is_rejected(self):
        evaluator = lambda *args, **kwargs: {'execution_simulator': 'alps',
            'outputs': [dict(self.task['outputs'][0], value=0.5, error=None)]}
        with self.assertRaisesRegex(execution.NetlistExecutionError, 'qualification is unproven'):
            self.context(measurement_evaluator=evaluator).evaluate(self.manifest)

    def test_argv_or_selected_simulator_drift_is_rejected_without_launch(self):
        self.task['argv'].append('+aps')
        context = self.context()
        with mock.patch.object(context, 'command_runner') as runner:
            with self.assertRaisesRegex(execution.NetlistExecutionError, 'argv differs'):
                context.evaluate(self.manifest)
        runner.assert_not_called()
        self.task['argv'].pop()
        self.spec['simulator'] = 'spectre'
        context = self.context()
        with mock.patch.object(context, 'command_runner') as runner:
            with self.assertRaisesRegex(execution.NetlistExecutionError, 'Selected simulator differs'):
                context.evaluate(self.manifest)
        runner.assert_not_called()

    def test_actual_scheduler_cpu_mismatch_is_rejected_before_submit(self):
        import shlex
        submit = os.path.join(self.root, 'batch-submit')
        atomic_write_text(submit, '#!/bin/sh\nexit 0\n')
        os.chmod(submit, 0o700)
        self.config['runtime']['execution_commands'] = {
            'scheduler_query_template': 'batch-query {job_id}',
            'scheduler_cancel_template': 'batch-cancel {job_id}'}
        self.prepared['job_policy']['global']['properties'].update(
            distributionmethod='Command', jobsubmitcommand=shlex.quote(submit) + ' -R "cpu=8;mem=30000"')
        context = self.context()
        with mock.patch.object(context, 'command_runner') as runner:
            with self.assertRaisesRegex(execution.NetlistExecutionError, 'actual scheduler cpu'):
                context.evaluate(self.manifest)
        runner.assert_not_called()
        self.assertFalse(os.path.exists(self.capture))

    def test_point_compaction_retains_alps_identity_and_proof_before_raw_cleanup(self):
        committed = []
        def commit(row, cancelled):
            self.assertFalse(cancelled())
            self.assertTrue(os.path.isfile(os.path.join(self.psf, 'fixture_parameters.json')))
            committed.append(copy.deepcopy(row))
            return {'ok': True}
        result = self.context(point_commit_callback=commit).evaluate(self.manifest)
        self.assertEqual(len(committed), 1)
        row = committed[0]
        self.assertEqual(row['execution_simulator'], 'alps')
        self.assertEqual(row['simulator_contract_fingerprint'], self.task['simulator_contract']['fingerprint'])
        self.assertTrue(row['native_parameter_readback_verified']['ok'])
        self.assertEqual(row['native_parameter_readback_verified']['actual'], {'CRESCENDO_NL_p': 1000})
        self.assertFalse(row['candidate_completed'])
        self.assertTrue(row['terminal_receipt']['cleanup_proven'])
        self.assertFalse(os.path.exists(self.psf))
        self.assertTrue(result['completion']['native_parameter_readback_verified'])

    def test_companion_alias_is_retired_only_after_durable_point_ack(self):
        self.set_mode('companion_alias')
        committed = []
        events = []
        def commit(row, cancelled):
            self.assertTrue(os.path.islink(self.psf + '.log'))
            self.assertTrue(os.path.isdir(self.psf))
            self.assertTrue(row['terminal_receipt']['cleanup_proven'])
            committed.append(row)
            return {'ok': True, 'revision': 7}
        result = self.context(point_commit_callback=commit, progress_callback=events.append).evaluate(self.manifest)
        cleanup = result['task_results'][0]['point_raw_cleanup']
        self.assertTrue(cleanup['artifact_cleanup_complete'])
        self.assertIn('psf.log', cleanup['removed'])
        self.assertNotIn('progress_error', cleanup)
        self.assertFalse(os.path.lexists(self.psf + '.log'))
        self.assertFalse(os.path.exists(self.psf))
        self.assertEqual(len(committed), 1)
        self.assertTrue(any(row.get('stage') == 'point_raw_cleanup' for row in events))

    def test_alias_and_raw_results_remain_without_point_ack(self):
        self.set_mode('companion_alias')
        result = self.context(point_commit_callback=lambda row, cancelled: {'ok': False}).evaluate(self.manifest)
        self.assertNotIn('point_raw_cleanup', result['task_results'][0])
        self.assertTrue(os.path.islink(self.psf + '.log'))
        self.assertTrue(os.path.isdir(self.psf))

    def test_missing_actual_parameters_never_ack_a_point_or_create_fitness(self):
        self.set_mode('missing_parameters')
        acknowledged = []
        with self.assertRaises(execution.NetlistExecutionError):
            self.context(point_commit_callback=lambda row, cancelled: acknowledged.append(row)).evaluate(self.manifest)
        self.assertEqual(acknowledged, [])
        self.assertTrue(os.path.isdir(self.psf))
        self.assertFalse(os.path.exists(os.path.join(self.root, 'cli_results.json')))

    def test_unproven_stop_never_measures_acks_or_unlinks_alias(self):
        self.set_mode('companion_alias')
        acknowledged = []
        with mock.patch.object(execution, 'task_termination_evidence', return_value={'cleanup_proven': False}), \
             mock.patch.object(execution, '_unlink_verified_alps_companion_aliases', wraps=execution._unlink_verified_alps_companion_aliases) as unlink:
            with self.assertRaisesRegex(execution.NetlistExecutionError, 'cleanup is unproven'):
                self.context(point_commit_callback=lambda row, cancelled: acknowledged.append(row)).evaluate(self.manifest)
            unlink.assert_not_called()
        self.assertEqual(acknowledged, [])
        self.assertEqual(self.measured, [])
        self.assertTrue(os.path.islink(self.psf + '.log'))
        self.assertTrue(os.path.isdir(self.psf))
        self.assertFalse(os.path.exists(os.path.join(self.root, 'cli_results.json')))

    def test_changed_companion_target_after_ack_is_retained(self):
        self.set_mode('companion_alias')
        def commit(row, cancelled):
            target = os.path.join(self.psf, 'native-alps.log')
            replacement = os.path.join(self.psf, 'changed.log')
            atomic_write_text(replacement, 'different result writer')
            os.replace(replacement, target)
            return {'ok': True}
        result = self.context(point_commit_callback=commit).evaluate(self.manifest)
        cleanup = result['task_results'][0]['point_raw_cleanup']
        self.assertFalse(cleanup['artifact_cleanup_complete'])
        self.assertEqual(cleanup['removed'], [])
        self.assertTrue(os.path.islink(self.psf + '.log'))
        self.assertTrue(os.path.isdir(self.psf))

    def test_new_unsafe_warning_alias_is_not_unlinked_after_ack(self):
        self.set_mode('companion_alias')
        def commit(row, cancelled):
            os.symlink(os.path.join(self.root, 'mode'), self.psf + '.warn')
            return {'ok': True}
        result = self.context(point_commit_callback=commit).evaluate(self.manifest)
        cleanup = result['task_results'][0]['point_raw_cleanup']
        self.assertFalse(cleanup['artifact_cleanup_complete'])
        self.assertEqual(cleanup['removed'], [])
        self.assertTrue(os.path.islink(self.psf + '.log'))
        self.assertTrue(os.path.islink(self.psf + '.warn'))
        self.assertTrue(os.path.isdir(self.psf))

    def test_active_log_reader_prevents_alias_and_raw_cleanup(self):
        self.set_mode('companion_alias')
        held, release = threading.Event(), threading.Event()
        readers = []
        def reader():
            with owned_solver_log_reader(self.psf + '.log', self.root, time.time() - 5, self.task):
                held.set()
                release.wait(5)
        def commit(row, cancelled):
            thread = threading.Thread(target=reader)
            readers.append(thread)
            thread.start()
            self.assertTrue(held.wait(10))
            return {'ok': True}
        try:
            with mock.patch.object(execution, 'native_log_cleanup_scope',
                                   lambda: native_log_io.native_log_cleanup_scope(timeout=0.05)):
                result = self.context(point_commit_callback=commit).evaluate(self.manifest)
            cleanup = result['task_results'][0]['point_raw_cleanup']
            self.assertFalse(cleanup['artifact_cleanup_complete'])
            self.assertEqual(cleanup['reader_barrier']['state'], 'readers_busy')
            self.assertTrue(os.path.islink(self.psf + '.log'))
            self.assertTrue(os.path.isdir(self.psf))
        finally:
            release.set()
            for thread in readers:
                thread.join(2)


if __name__ == '__main__':
    unittest.main()
