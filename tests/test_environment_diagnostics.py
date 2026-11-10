"""Portable two-log evidence: retain causes/identity without secret/raw dumps."""
import json
import os
import tempfile
import sys
import unittest
from unittest import mock

from crescendo_engine import environment_diagnostics as diagnostics
from crescendo_engine import environment_probes as probes
from crescendo_engine.scheduler_cli import scheduler_command_argv


class DiagnosticEvidenceTests(unittest.TestCase):
    def test_native_completion_failure_projection_retains_phase_and_coverage(self):
        detail = {'failure_stage': 'native_corner_coverage', 'phase': 'completed_native_history',
                  'execution_started': True, 'native_coverage': {'ok': False, 'missing': ['TT']}}
        projected = diagnostics._producer_failure_index({'error': 'coverage failed', 'detail': detail})
        self.assertEqual(projected['detail'], detail)

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = self.temp.name

    def tearDown(self):
        self.temp.cleanup()

    def write(self, relative, value):
        path = os.path.join(self.root, relative)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w', encoding='utf-8') as stream:
            stream.write(json.dumps(value) if isinstance(value, dict) else value)
        return path

    def records(self, prepared=None, config=None):
        return diagnostics.collect_run_diagnostics(self.root, 'private_netlist',
            config=config, prepared=prepared, stop_proven=True)

    def test_native_stage_evidence_comes_from_owned_capture_artifact(self):
        from crescendo_engine.environment_test import _native_failure_observations
        self.write('native/capture_diagnostics.json', {'capture_only': True, 'skill': {'ok': False,
            'errors': [{'message': 'ERROR (ASSEMBLER-2404)', 'console_source': 'ocean.stdout',
                'console_line': 10, 'native_run_boundary': {'stage': 'Run', 'marker_line': 8}},
                {'message': '*Error* Crescendo native capture history unavailable',
                 'console_source': 'ocean.stderr', 'console_line': 4}],
            'native_run_evidence': [{'stage': 'HistoryLookup', 'event': 'BEGIN',
                                     'source': 'ocean.stdout', 'line': 30}],
            'error_order_scope': 'per-console scan order'}})
        record = next(row['payload'] for row in self.records()
                      if row['observation'] == 'diagnostic.native_stage_evidence')
        observations = _native_failure_observations({'capture_diagnostics': {'skill': record['skill']}})
        self.assertEqual([item[0] for item in observations], ['native.run_window', 'native.history_lookup'])
        self.assertEqual(observations[0][1]['error']['console_line'], 10)
        self.assertEqual(observations[1][1]['error']['console_source'], 'ocean.stderr')
        self.assertEqual(record['path'], 'native/capture_diagnostics.json')

    def test_small_causal_summary_keeps_stage_rc_and_safe_compute_context(self):
        self.write('native/submission.json', {'returncode': 0, 'job_id': '21', 'submission_mode': 'asynchronous'})
        self.write('native/compute_metadata.json', {'compute_hostname': 'compute-node', 'compute_uid': 1000,
            'submit_node': 'submission-node', 'cwd': '/owned/task', 'threads': 4, 'allocated_cpu': 4,
            'environment_presence': {'PATH': 'set', 'CDS_LIC_FILE': True, 'unexpected': 'PRIVATE_VALUE'}})
        self.write('native/started.receipt', 'v1\tprivate-token\tstarted\t123\tcompute-node\t1000\n')
        self.write('native/terminal.receipt', 'v1\tprivate-token\texited\t7\t1\tcompute-node\t1000\n')
        self.write('native/measurement_error.json', {'returncode': 2})
        records = self.records()
        summary = next(row['payload'] for row in records if row['observation'] == 'diagnostic.causal_summary')
        samples = {sample['stage']: sample for sample in summary['first_stage_samples']}
        self.assertEqual(samples['submission']['returncode'], 0)
        self.assertEqual(samples['submission']['submission_mode'], 'asynchronous')
        self.assertEqual(samples['solver_terminal']['receipt']['rows'][0]['returncode'], '7')
        self.assertEqual(samples['measurement']['returncode'], 2)
        self.assertEqual(samples['compute_context']['compute_hostname'], 'compute-node')
        self.assertNotIn('submit_node', samples['compute_context'])
        self.assertEqual(samples['compute_context']['environment_presence'], {'PATH': 'set', 'CDS_LIC_FILE': True})
        self.assertNotIn('PRIVATE_VALUE', json.dumps(records))
        self.assertNotIn('private-token', json.dumps(summary))
        self.assertTrue(summary['missing_stages_are_not_passes'])

    def test_owned_reader_observations_distinguish_missing_parameters_and_actual_scalars(self):
        observation = {'schema_version': 'crescendo.result_reader_capability.v1', 'task_id': 'main_TT_1',
            'test': 'main', 'corner': 'TT', 'point_id': 1, 'execution_simulator': 'alps', 'result_reader': 'spectre',
            'open_results_status': 'opened', 'analysis_names': ['sp'], 'analysis_inventory_status': 'available',
            'psf_dir': os.path.join(self.root, 'tasks', 'main_TT_1', 'psf'),
            'native_parameter_readback': {'ok': False, 'source': 'current_psf_designParamVals',
                'required': ['SW', 'VDD'], 'actual': {'VDD': 1.2}, 'missing': ['SW'],
                'missing_reasons': {'SW': 'unavailable_design_parameter'}},
            'output_diagnostics': [{'key': 'main::gain', 'status': 'PASS', 'value': 4.0, 'requires_variables': []},
                {'key': 'main::power', 'status': 'NOT_TESTED', 'value': None, 'requires_variables': ['SW']}],
            'candidate_qualification': {'ok': False, 'reason': 'required parameters unavailable'}}
        self.write('tasks/main_TT_1/measurement/result_reader_capability.json', observation)
        records = self.records()
        summary = next(row['payload'] for row in records if row['observation'] == 'diagnostic.result_reader_capability')
        self.assertEqual(summary['analysis_names'], ['sp'])
        self.assertEqual(summary['open_results_status'], 'opened')
        self.assertEqual(summary['analysis_inventory_status'], 'available')
        self.assertEqual(summary['native_parameter_readback']['actual'], {'VDD': 1.2})
        self.assertEqual(summary['native_parameter_readback']['missing_reasons'], {'SW': 'unavailable_design_parameter'})
        self.assertEqual(summary['native_parameter_readback']['missing'], ['SW'])
        self.assertEqual(summary['output_diagnostics'][0]['value'], 4.0)
        self.assertFalse(summary['candidate_qualification']['ok'])
        causal = next(row['payload'] for row in records if row['observation'] == 'diagnostic.causal_summary')
        self.assertEqual(causal['stage_counts']['result_reader'], 1)
        self.assertEqual(diagnostics.failure_layer({'failure_stage': 'result_reader_parameter_metadata'}), 'measurement')

    def test_reader_open_failure_is_explicit_and_remains_in_the_measurement_layer(self):
        for status in ('unavailable', 'not_recorded'):
            with self.subTest(status=status):
                self.write('measurement/result_reader_capability.json', {
                    'schema_version': 'crescendo.result_reader_capability.v1', 'task_id': 'main_TT_1',
                    'execution_simulator': 'alps', 'result_reader': 'spectre', 'open_results_status': status,
                    'analysis_names': [], 'analysis_inventory_status': 'unavailable',
                    'native_parameter_readback': {'ok': False, 'actual': {}, 'missing': ['SW']},
                    'candidate_qualification': {'ok': False, 'reason': 'openResults unavailable'}})
                summary = next(row['payload'] for row in self.records()
                    if row['observation'] == 'diagnostic.result_reader_capability')
                self.assertEqual(summary['open_results_status'], status)
                self.assertEqual(summary['analysis_names'], [])
                self.assertFalse(summary['candidate_qualification']['ok'])
        self.assertEqual(diagnostics.failure_layer({'failure_stage': 'result_reader_open_results'}), 'measurement')

    def test_unknown_reader_observation_schema_does_not_claim_capability(self):
        self.write('measurement/result_reader_capability.json', {'schema_version': 'other.v1',
            'task_id': 'first', 'execution_simulator': 'alps', 'native_parameter_readback': {'ok': True}})
        summary = next(row['payload'] for row in self.records()
            if row['observation'] == 'diagnostic.result_reader_capability')
        self.assertEqual(summary['status'], 'NOT_TESTED')
        self.assertNotIn('native_parameter_readback', summary)

    def test_repeated_startup_error_does_not_hide_later_distinct_measurement_error(self):
        text = ('*Error* repeated startup extension warning\n' + 'context padding\n' * 5) * 100
        text += '*Error* actual current PSF parameter metadata missing\n' + 'unrelated tail\n' * 300
        context = diagnostics._windows(text)
        retained = '\n'.join(row['text'] for row in context['lines'])
        self.assertIn('repeated startup extension warning', retained)
        self.assertIn('actual current PSF parameter metadata missing', retained)
        self.assertEqual(context['error_count'], 101)
        self.assertEqual(context['distinct_error_count'], 2)
        self.assertEqual(context['repeated_error_count'], 99)
        self.assertEqual(context['first_error_line'], 1)

    def pipeline(self, simulator='spectre', engine='maestro_netlist'):
        return {'schema_version': 'crescendo.execution_pipeline.v1', 'solver': 'shared_cli',
            'submission': 'shared_local_or_command', 'netlist_source': 'maestro' if engine == 'maestro_netlist' else 'template',
            'netlist_generation': {'source_mode': engine, 'method': 'native_invocation_capture',
                'private_identity': {'workspace_root': '/owned/private_maestro'}, 'point_count': 1,
                'source_guard': {'ok': True}, 'solver_invocations': 0, 'remote_submissions': 0},
            'tasks': [{'task_id': 'main_TT_1', 'test': 'main', 'point_id': 1, 'corner': 'TT',
                'execution_simulator': simulator,
                'solver_command_plan': {'schema_version': 'crescendo.execution_command_plan.v1', 'layer': 'solve',
                    'argv': ['/solver/' + simulator, '/owned/input.scs', '-o', '/owned/psf'],
                    'template': '{executable} {netlist} -o {psf}', 'fingerprint': 'solve-hash'},
                'submission_command_plan': {'schema_version': 'crescendo.execution_command_plan.v1', 'layer': 'submit',
                    'argv': ['/batch/batch-submit', '-q', 'chosen', '/owned/run.sh'], 'remote': True, 'resource_cpu': 8},
                'receipt': {'returncode': 0, 'cleanup_proven': True, 'compute_hostname': 'compute-node',
                    'compute_uid': 1000, 'token': 'owned-private-token'}}]}

    def test_shared_pipeline_retains_actual_layers_and_redacts_receipt_token(self):
        pipeline = self.pipeline()
        self.write('candidates/point/cli_results.json', {'candidate_id': 'p0',
            'completion': {'execution_pipeline': pipeline}, 'task_results': [{'large_raw_output': 'DO_NOT_REPEAT_OUTPUT'}]})
        records = self.records()
        summary = next(row['payload']['data'] for row in records if row['observation'] == 'diagnostic.artifact'
            and row['payload']['path'].endswith('cli_results.json'))
        self.assertEqual(summary['solver'], 'shared_cli')
        self.assertEqual(summary['netlist_generation']['source_mode'], 'maestro_netlist')
        self.assertEqual(summary['points'][0]['solver_command_plan']['argv'], pipeline['tasks'][0]['solver_command_plan']['argv'])
        self.assertEqual(summary['points'][0]['submission_command_plan']['argv'], pipeline['tasks'][0]['submission_command_plan']['argv'])
        self.assertEqual(summary['points'][0]['terminal_receipt']['compute_hostname'], 'compute-node')
        self.assertNotIn('owned-private-token', json.dumps(records))
        self.assertNotIn('DO_NOT_REPEAT_OUTPUT', json.dumps(records))

    def test_private_template_numeric_fallback_keeps_actual_native_source(self):
        pipeline = self.pipeline()
        generation = pipeline['netlist_generation']
        generation.update(requested_backend='private_netlist', native_generation_required=True,
            template_qualified=False, template_fallback={'failure_stage': 'native_numeric_derived_parameters',
                'native_generation_required': True, 'template_qualified': False,
                'numeric_derived_parameters': [{'parameter': 'DPAR_10'}],
                'source_guard': {'ok': True}, 'preparation_cleanup': {'cleanup_proven': True}})
        candidate = {'execution_backend': 'private_netlist', 'execution_pipeline': pipeline}
        proof = diagnostics.pipeline_qualification(candidate, 'private_netlist', 'spectre')
        self.assertTrue(proof['ok'], proof['problems'])
        self.assertTrue(proof['native_generation_required'])
        self.assertFalse(proof['template_qualified'])
        summary = diagnostics.pipeline_summary(proof['pipeline'])
        self.assertEqual(summary['netlist_source'], 'maestro')
        self.assertEqual(summary['netlist_generation']['source_mode'], 'maestro_netlist')
        self.assertFalse(summary['netlist_generation']['template_qualified'])
        generation['template_fallback']['preparation_cleanup']['cleanup_proven'] = False
        self.assertFalse(diagnostics.pipeline_qualification(candidate, 'private_netlist', 'spectre')['ok'])

    def test_unexplained_native_source_does_not_qualify_private_template_case(self):
        candidate = {'execution_pipeline': self.pipeline()}
        self.assertFalse(diagnostics.pipeline_qualification(candidate, 'private_netlist', 'spectre')['ok'])

    def test_selected_binding_fallback_is_native_and_keeps_binding_evidence(self):
        pipeline = self.pipeline()
        fallback = {'failure_stage': 'native_selected_parameter_binding',
            'native_generation_required': True, 'template_qualified': False,
            'selected_parameter_bindings': [{'parameter_id': 'width', 'name': 'DTN_W',
                'proof_source': 'full_identity_native_RDB_input', 'dpar_names': []}],
            'reference_checks': [{'reference': 'reference_low', 'ok': True}],
            'source_guard': {'ok': True}, 'preparation_cleanup': {'cleanup_proven': True}}
        pipeline['netlist_generation'].update(requested_backend='private_netlist',
            native_generation_required=True, template_qualified=False, template_fallback=fallback)
        candidate = {'execution_backend': 'private_netlist', 'execution_pipeline': pipeline}
        proof = diagnostics.pipeline_qualification(candidate, 'private_netlist', 'spectre')
        self.assertTrue(proof['ok'], proof['problems'])
        self.assertFalse(proof['template_qualified'])
        self.assertEqual(diagnostics.pipeline_summary(proof['pipeline'])['netlist_source'], 'maestro')
        projected = diagnostics._producer_failure_index({'message': 'native generation needed', 'detail': fallback})
        self.assertEqual(projected['detail']['selected_parameter_bindings'], fallback['selected_parameter_bindings'])
        self.assertEqual(projected['detail']['reference_checks'], fallback['reference_checks'])
        fallback['selected_parameter_bindings'] = []
        self.assertFalse(diagnostics.pipeline_qualification(candidate, 'private_netlist', 'spectre')['ok'])

    def test_fallback_diagnostic_rejects_contradictory_cleanup_flags(self):
        for field in ('cleanup_pending', 'termination_failed'):
            pipeline = self.pipeline()
            fallback = {'failure_stage': 'native_numeric_derived_parameters',
                'native_generation_required': True, 'template_qualified': False,
                'numeric_derived_parameters': [{'parameter': 'DPAR_10'}],
                'source_guard': {'ok': True}, 'preparation_cleanup': {'cleanup_proven': True}, field: True}
            pipeline['netlist_generation'].update(requested_backend='private_netlist',
                native_generation_required=True, template_qualified=False, template_fallback=fallback)
            self.assertFalse(diagnostics.pipeline_qualification({'execution_pipeline': pipeline},
                'private_netlist', 'spectre')['ok'])

    def test_actual_points_contract_uses_plan_simulator_and_completion_generation(self):
        pipeline = self.pipeline('alps')
        generation = pipeline.pop('netlist_generation')
        pipeline['points'] = pipeline.pop('tasks')
        simulator = pipeline['points'][0].pop('execution_simulator')
        pipeline['points'][0]['solver_command_plan']['execution_simulator'] = simulator
        proof = diagnostics.pipeline_qualification({'completion_evidence': {
            'execution_pipeline': pipeline, 'netlist_generation': generation}}, 'maestro_netlist', 'alps')
        self.assertTrue(proof['ok'], proof['problems'])
        summary = diagnostics.pipeline_summary(proof['pipeline'])
        self.assertEqual(summary['points'][0]['solver_command_plan']['execution_simulator'], 'alps')
        self.assertEqual(summary['netlist_generation']['point_count'], 1)

    def test_pipeline_keeps_scheduler_identity_and_alps_warning_snapshot_after_raw_retirement(self):
        from crescendo_engine.solver_artifacts import collect_solver_artifacts
        import time
        pipeline = self.pipeline('alps')
        psf = os.path.join(self.root, 'tasks', 'main_TT_1', 'psf')
        warning = self.write('tasks/main_TT_1/psf.warn', 'WARNING: model parameter ignored\n')
        snapshot = collect_solver_artifacts({'task_id': 'main_TT_1', 'test': 'main', 'corner': 'TT',
            'point_id': 1, 'execution_simulator': 'alps', 'psf_dir': psf}, self.root, time.time() - 1,
            job_id='10000001')
        point = pipeline['tasks'][0]
        point.update(scheduler_job_id='10000001', solver_artifacts=snapshot,
            scheduler_observation={'job_id': '10000001', 'state': 'SUCCEEDED',
                'native_completion_authority': False, 'metadata': {'exec_node': 'node01'}})
        os.unlink(warning)
        self.write('cli_results.json', {'completion': {'execution_pipeline': pipeline}})
        records = self.records()
        artifacts = [row['payload']['data'] for row in records
            if row['observation'] == 'diagnostic.artifact' and row['payload']['path'] == 'cli_results.json']
        artifact = artifacts[0] if artifacts else {row['payload']['field']: row['payload']['data'] for row in records
            if row['observation'] == 'diagnostic.artifact_section' and row['payload']['path'] == 'cli_results.json'}
        self.assertEqual(artifact['points'][0]['scheduler_job_id'], '10000001')
        self.assertEqual(artifact['points'][0]['scheduler_observation']['metadata']['exec_node'], 'node01')
        self.assertIn('model parameter ignored', json.dumps(artifact['points'][0]['solver_artifacts']))

    def test_companion_logs_and_task_info_before_long_statistical_tail_are_inlined(self):
        self.write('tasks/main_TT_1/psf.warn', 'header\n' + 'padding\n' * 30 + 'WARNING: unusual model parameter\n' + 'padding\n' * 30)
        self.write('tasks/main_TT_1/psf.log', 'ALPS owned solver log\n')
        self.write('tasks/main_TT_1/execution/started.receipt', 'v1\tprivate-token\tstarted\t123\tcompute\t1000\n')
        self.write('tasks/main_TT_1/execution/scheduler.stdout', 'TASK_INFO:\n JOB_ID: 10000001\n EXEC_NODE: node01\n EXIT_CODE: 0\n' + 'statistics\n' * 100)
        records = self.records()
        contexts = [row['payload'] for row in records if row['observation'] == 'diagnostic.log_context']
        self.assertEqual(len(contexts), 3)
        self.assertIn('unusual model parameter', json.dumps(contexts))
        self.assertIn('node01', json.dumps(contexts))
        self.assertIn('EXIT_CODE', json.dumps(contexts))

    def test_optional_raw_alps_sidecar_cannot_bypass_stale_snapshot_rejection(self):
        import time
        stale = self.write('tasks/main_TT_1/psf.warn', 'DO_NOT_INCLUDE_STALE_ALPS_WARNING\n')
        self.write('tasks/main_TT_1/execution/started.receipt', 'v1\tprivate-token\tstarted\t123\tcompute\t1000\n')
        os.utime(stale, (time.time() - 30, time.time() - 30))
        records = self.records()
        self.assertNotIn('DO_NOT_INCLUDE_STALE_ALPS_WARNING', json.dumps(records))
        collection = next(row['payload'] for row in records if row['observation'] == 'diagnostic.collection')
        self.assertTrue(any('stale' in item.get('reason', '') for item in collection['omitted_or_truncated']))

    def test_pipeline_gate_rejects_wrong_source_native_solver_partial_commands_and_receipts(self):
        import copy
        original = self.pipeline('alps')
        candidate = {'completion_evidence': {'execution_pipeline': original}}
        self.assertTrue(diagnostics.pipeline_qualification(candidate, 'maestro_netlist', 'alps')['ok'])
        for change in ('native', 'source', 'private', 'capture_solver', 'missing_submit', 'receipt', 'simulator', 'duplicate', 'malformed_id'):
            pipeline = copy.deepcopy(original)
            if change == 'native': pipeline['solver'] = 'native_ADE'
            elif change == 'source': pipeline['netlist_source'] = 'template'
            elif change == 'private': pipeline['netlist_generation'].pop('private_identity')
            elif change == 'capture_solver': pipeline['netlist_generation']['solver_invocations'] = 1
            elif change == 'missing_submit': pipeline['tasks'][0].pop('submission_command_plan')
            elif change == 'receipt': pipeline['tasks'][0]['receipt']['cleanup_proven'] = False
            elif change == 'simulator': pipeline['tasks'][0]['execution_simulator'] = 'spectre'
            elif change == 'duplicate':
                pipeline['tasks'].append(copy.deepcopy(pipeline['tasks'][0]));pipeline['netlist_generation']['point_count'] = 2
            elif change == 'malformed_id': pipeline['tasks'][0]['task_id'] = ['foreign']
            with self.subTest(change=change):
                proof = diagnostics.pipeline_qualification({'completion_evidence': {'execution_pipeline': pipeline}}, 'maestro_netlist', 'alps')
                self.assertFalse(proof['ok'])
                self.assertTrue(proof['problems'])

    def test_alps_loader_error_targets_alps_without_a_spectre_probe(self):
        with mock.patch.object(probes, 'bounded_command') as command:
            detail = probes.failure_environment_details({'cadence': {'alps_bin': '/missing/alps'}}, self.root,
                '/missing/alps: error while loading shared libraries: libalps.so: cannot open shared object file')
        self.assertEqual(detail['native_loader_failure']['tool'], 'alps_bin')
        self.assertEqual(detail['native_loader_failure']['status'], 'FAIL')
        command.assert_not_called()

    def test_old_loader_error_in_mixed_scheduler_text_is_unbound_and_does_not_probe_current_tool(self):
        old_error = '/software/alps: error while loading shared libraries: libold.so: GLIBC_2.28 not found'
        self.write('tasks/main_TT_1/execution/scheduler.stdout',
            'TASK_INFO:\n JOB_ID: 108\n' + old_error + '\nTASK_INFO:\n JOB_ID: 177\n EXIT_CODE: 0\n')
        self.write('tasks/main_TT_1/execution/scheduler_query_177.stderr', old_error + '\n')
        self.write('tasks/main_TT_1/solver_artifacts.json', {'schema_version': 'crescendo.solver_artifacts.v1',
            'task_id': 'main_TT_1', 'scheduler_job_id': '177', 'execution_simulator': 'alps',
            'artifacts': [{'kind': 'scheduler_stdout', 'scheduler_task_observation': {
                'owned_job_id': '177', 'ignored_job_ids': ['108'], 'current_records': [{'job_id': '177', 'exit_code': 0}]},
                'context': {'lines': [{'text': 'JOB_ID: 177'}, {'text': 'EXIT_CODE: 0'}]}}]})
        with mock.patch.object(probes, 'bounded_command') as command:
            records = self.records(config={'cadence': {'alps_bin': sys.executable}})
        command.assert_not_called()
        self.assertFalse(any(row['observation'] == 'diagnostic.failure_environment' for row in records))
        raw = [row['payload'] for row in records if row['observation'] == 'diagnostic.log_context'
               and row['payload'].get('attribution') == 'unbound_raw_scheduler_text']
        self.assertEqual(len(raw), 2)
        self.assertTrue(all(row['current_job_authority'] is False for row in raw))
        self.assertIn(old_error, json.dumps(raw))
        artifacts = [row for row in records if row['observation'].startswith('diagnostic.artifact')]
        self.assertIn('current_records', json.dumps(artifacts))
        self.assertIn('177', json.dumps(artifacts))

    def test_owned_solver_loader_error_still_runs_only_the_implicated_targeted_probe(self):
        self.write('tasks/main_TT_1/execution/simulator.stderr',
            '/software/alps: error while loading shared libraries: libalps.so: cannot open shared object file\n')
        with mock.patch.object(probes, '_native_context', return_value=(None, {'PATH': '/usr/bin:/bin'})), \
                mock.patch.object(probes.shutil, 'which', return_value='/usr/bin/ldd'), \
                mock.patch.object(probes, 'bounded_command', return_value={'returncode': 0}) as command:
            records = self.records(config={'cadence': {'alps_bin': sys.executable}})
        command.assert_called_once()
        self.assertEqual(command.call_args[0][0], ['/usr/bin/ldd', sys.executable])
        targeted = next(row['payload'] for row in records if row['observation'] == 'diagnostic.failure_environment')
        self.assertEqual(targeted['native_loader_failure']['tool'], 'alps_bin')
        self.assertEqual(targeted['native_loader_failure']['status'], 'PASS')

    def test_first_syntax_error_script_and_capture_semantics_are_inlined(self):
        script = self.write('baseline/capture.il', '\n'.join('line_%s()' % number for number in range(1, 80)))
        self.write('baseline/ocean.stdout', '*Error* syntax error at line 17 column 3 in file %s\n' % script +
                   'normal output\n' * 500 + 'ASSEMBLER-5031 Simulator failed\n')
        self.write('baseline/native_capture_ready', 'capture_only\n')
        records = self.records()
        capture = next(row['payload'] for row in records if row['observation'] == 'diagnostic.capture_semantics')
        self.assertTrue(capture['capture_only_detected'])
        self.assertEqual(capture['expected_wrapper_exit'], 1)
        context = next(row['payload']['context'] for row in records if row['observation'] == 'diagnostic.script_context')
        self.assertIn(17, context['reported_error_lines'])
        self.assertTrue(any(row['line'] == 17 for row in context['lines']))
        log = next(row['payload']['context'] for row in records if row['observation'] == 'diagnostic.log_context')
        self.assertEqual(log['first_error_line'], 1)
        self.assertIn('syntax error', log['lines'][0]['text'])

    def test_declared_worker_error_survives_without_reading_unbound_job_history(self):
        self.write('baseline/Job0.log', '*Error* OLD_UNBOUND_WORKER_HISTORY\n')
        report = {'schema_version': 'crescendo.native_worker_diagnostics.v1', 'ok': False,
                  'logs': [{'path': '/owned/workspace/logs_root/logs0/Job0.log', 'fresh': True,
                      'ownership_proven': True, 'error_lines': [{'line': 77, 'text': '*Error* CURRENT_WORKER_CALLBACK'}]}]}
        self.write('baseline/capture_diagnostics.json', json.dumps({
            'schema_version': 'crescendo.native_capture_diagnostics.v1', 'native_worker_diagnostics': report}))
        self.write('baseline/native_worker_diagnostics.json', json.dumps(report))
        self.write('preparation_failure.json', json.dumps({'failure_stage': 'native_run_parameter_error',
            'message': 'Native worker parameter failure', 'detail': {'native_worker_diagnostics': report}}))
        records = self.records()
        workers = [row for row in records if row['observation'] == 'diagnostic.native_worker_diagnostics']
        self.assertEqual(len(workers), 1)
        self.assertEqual(workers[0]['payload']['native_worker_diagnostics'], report)
        self.assertIn('CURRENT_WORKER_CALLBACK', json.dumps(workers))
        self.assertEqual(json.dumps(records).count('CURRENT_WORKER_CALLBACK'), 1)
        self.assertNotIn('OLD_UNBOUND_WORKER_HISTORY', json.dumps(records))
        self.assertIn('native_worker_diagnostics', json.dumps(next(
            row for row in records if row['observation'] == 'diagnostic.producer_failure')))

    def test_failed_capture_before_point_checks_is_explicitly_not_verified(self):
        self.write('numeric/capture_diagnostics.json', {'capture_only': True,
            'skill': {'ok': False, 'first_error': {'message': '*Error* reader failed'}}})
        self.write('numeric/corner_identity.json', {'ok': True, 'private_setup': {'path': '/owned/private'}})
        records = self.records(prepared={'setup': {'tests': [{'name': 'main', 'enabled': True}]}})
        coverage = next(row['payload'] for row in records if row['observation'] == 'diagnostic.identity_coverage')
        self.assertEqual(coverage['status'], 'NOT_VERIFIED')
        self.assertEqual(coverage['reason'], 'native_capture_failed_before_point_qualification')
        self.assertIsNone(coverage['unverified_count'])
        self.assertEqual(coverage['tests'][0]['test'], 'main')
        self.assertEqual(coverage['tests'][0]['status'], 'NOT_VERIFIED')
        self.assertNotIn('ok', coverage)

    def test_absent_identity_evidence_is_unknown_and_producer_result_is_not_upgraded(self):
        coverage = diagnostics._identity_coverage_summary({}, {})
        self.assertEqual(coverage['status'], 'NOT_VERIFIED')
        self.assertEqual(coverage['reason'], 'native_point_qualification_not_recorded')
        self.assertIsNone(coverage['unverified_count'])
        for qualified, count, status, expected_count in (
                (True, 0, 'VERIFIED', 0), (False, 0, 'FAILED', 0), (True, None, 'VERIFIED', None)):
            with self.subTest(qualified=qualified, count=count):
                coverage = diagnostics._identity_coverage_summary({'corner_identity.json': {
                    'native_coverage': {'ok': qualified, 'test_differences': [
                        {'test': 'main', 'unverified_variable_check_count': count}]}}}, {})
                self.assertEqual(coverage['status'], status)
                self.assertEqual(coverage['unverified_count'], expected_count)

    def test_earlier_phase_point_checks_do_not_verify_later_failed_capture(self):
        coverage = diagnostics._identity_coverage_summary({
            'baseline/corner_identity.json': {'native_coverage': {'ok': True, 'test_differences': [
                {'test': 'main', 'unverified_variable_check_count': 0}]}},
            'numeric/capture_diagnostics.json': {'skill': {'ok': False}}}, {})
        self.assertEqual(coverage['status'], 'NOT_VERIFIED')
        self.assertEqual(coverage['unqualified_failed_phases'], ['numeric'])
        self.assertIsNone(coverage['unverified_count'])

    def test_successful_native_setup_readback_summarizes_operations_but_keeps_modes(self):
        records = [{'kind': 'READ', 'scope': 'test:main', 'operation': 'runtimeout',
                    'value': '900', 'call_succeeded': True, 'name': 'UNIQUE_REPEATED_POLICY_RECORD'}] * 300
        records += [{'kind': 'MODE', 'scope': 'main', 'name': 'controlMode', 'value': 'batch'},
                    {'kind': 'ENV', 'scope': 'child', 'name': 'interactiveA', 'value': None}]
        setup = {'policy_confirmed': True, 'mode_confirmed': True, 'readback_status': 'collected',
            'interactiveA_confirmed': False, 'records_path': '/owned/capture_policy_mode.tsv', 'records': records,
            'final_policy_names': {'test:main': 'ownedPolicy'}, 'expected_policy_readback': {
                'test:main': {'distributionmethod': 'Local', 'maxjobs': '2', 'runtimeout': '900'}},
            'environment_option_observations': [{'test': 'main', 'status': 'OBSERVED', 'fields': [
                {'name': 'autoDisplay', 'literal': 't'}, {'name': 'useActualVendorAlpsFlag', 'literal': 'nil'}]}]}
        self.write('numeric/capture_diagnostics.json', {'capture_only': True, 'skill': {'ok': False},
            'native_capture_setup': setup})
        artifact = next(row['payload']['data'] for row in self.records()
                        if row['observation'] == 'diagnostic.artifact')
        summary = artifact['native_capture_setup']
        self.assertTrue(summary['successful_readback_summary'])
        self.assertEqual(summary['records_count'], 302)
        self.assertEqual(summary['actual_modes'][0]['value'], 'batch')
        self.assertEqual(summary['policy_scopes'][0]['confirmed_values']['runtimeout'], '900')
        self.assertEqual(summary['full_readback_path'], '/owned/capture_policy_mode.tsv')
        self.assertEqual(summary['environment_option_observations'], setup['environment_option_observations'])
        self.assertEqual(len(summary['records_sha256']), 64)
        self.assertNotIn('UNIQUE_REPEATED_POLICY_RECORD', json.dumps(artifact))
        self.assertLess(len(json.dumps(summary)), len(json.dumps(setup)) // 5)
        self.assertEqual(len(setup['records']), 302)

    def test_partial_native_setup_readback_retains_exact_failure_fields(self):
        setup = {'policy_confirmed': True, 'mode_confirmed': False, 'readback_status': 'collected',
            'records': [{'kind': 'MODE', 'scope': 'main', 'value': 'interactive', 'call_succeeded': True},
                        {'kind': 'SET', 'scope': 'main', 'value': None, 'call_succeeded': False}],
            'readback_error': 'expected batch, got interactive'}
        semantic = diagnostics._semantic_artifact({'native_capture_setup': setup})
        self.assertEqual(semantic['native_capture_setup'], setup)
        self.assertNotIn('successful_readback_summary', semantic['native_capture_setup'])

    def test_complete_clean_capture_scan_references_duplicate_tails_and_keeps_first_error(self):
        original = {'capture_only': True, 'skill': {'ok': False, 'process_ok': True,
            'scan_complete': True, 'console_scope': 'owned_full_logs',
            'inspected_logs': ['ocean.stdout', 'ocean.stderr'],
            'first_error': {'message': '*Error* current native reader failure',
                'native_reader_call': {'state': 'preceding_unreturned_call', 'raw_trace_entry': 'EXACT_FAILED_READER'}},
            'native_error_contexts': [{'console_source': 'ocean.stdout', 'error_line': 92, 'lines': [
                {'line': 92, 'text': '*Error* current native reader failure'}]}]},
            'ocean': {'returncode': 0, 'stdout_tail': '| [2] readstring("SUCCESSFUL_READER_INPUT")\n',
                'stderr_tail': '', 'guardian_cleanup': {'termination_proof': {'ok': True}}}}
        projected = diagnostics._semantic_artifact(original)
        self.assertNotIn('SUCCESSFUL_READER_INPUT', json.dumps(projected))
        self.assertEqual(projected['skill'], original['skill'])
        self.assertEqual(projected['ocean']['stdout_tail']['console_source'], 'ocean.stdout')
        self.assertEqual(len(projected['ocean']['stdout_tail']['sha256']), 64)
        self.assertIn('SUCCESSFUL_READER_INPUT', original['ocean']['stdout_tail'])

    def test_incomplete_unknown_or_unclean_capture_scan_preserves_bounded_tail(self):
        import copy
        original = {'capture_only': True, 'skill': {'ok': False, 'process_ok': True,
            'scan_complete': True, 'console_scope': 'owned_full_logs', 'inspected_logs': ['ocean.stdout'],
            'first_error': {'message': '*Error* known error'}, 'native_error_contexts': [{'error_line': 92}]},
            'ocean': {'returncode': 0, 'stdout_tail': 'IMPORTANT_UNSCANNED_TAIL',
                      'guardian_cleanup': {'termination_proof': {'ok': True}}}}
        cases = [('skill', 'scan_complete', False), ('skill', 'console_scope', 'command_tails_only'),
                 ('skill', 'process_ok', False), ('skill', 'first_error', None),
                 ('ocean', 'returncode', 1), ('ocean', 'returncode', None),
                 ('ocean', 'timed_out', True), ('ocean', 'cancelled', True),
                 ('ocean', 'termination_failed', True), ('ocean', 'guardian_cleanup', {'termination_proof': {'ok': False}})]
        for group, key, value in cases:
            with self.subTest(group=group, key=key, value=value):
                document = copy.deepcopy(original)
                document[group][key] = value
                projected = diagnostics._semantic_artifact(document)
                self.assertIn('IMPORTANT_UNSCANNED_TAIL', json.dumps(projected['ocean']['stdout_tail']))

    def test_wrapper_aftermath_is_bounded_noncause_evidence_only_for_failed_parent(self):
        report = {'schema_version': 'crescendo.native_worker_diagnostics.v1', 'ok': True,
            'parent_capture_failed': True, 'logs': [], 'expected_wrapper_aftermath_samples': [
                {'console_source': '/owned/logs_user/logs0/Job0.log', 'line': 12 + n,
                 'code': 'ADE-3036', 'message': 'EXPECTED_WRAPPER_%s' % n,
                 'stage': 'capture_wrapper_aftermath', 'primary_failure': False} for n in range(3)]}
        self.write('numeric/native_worker_diagnostics.json', report)
        self.write('numeric/capture_diagnostics.json', {'capture_only': True, 'skill': {'ok': False},
            'native_worker_diagnostics': report})
        records = self.records()
        raw = json.dumps(records)
        self.assertEqual(raw.count('EXPECTED_WRAPPER_0'), 1)
        self.assertEqual(raw.count('EXPECTED_WRAPPER_1'), 1)
        self.assertNotIn('EXPECTED_WRAPPER_2', raw)
        worker = next(row['payload']['native_worker_diagnostics'] for row in records
                      if row['observation'] == 'diagnostic.native_worker_diagnostics')
        self.assertTrue(worker['ok'])
        self.assertFalse(worker['expected_wrapper_aftermath_samples'][0]['primary_failure'])
        report['parent_capture_failed'] = False
        projected = diagnostics._worker_report_projection(report)
        self.assertNotIn('expected_wrapper_aftermath_samples', projected)
        self.assertEqual(projected['expected_wrapper_aftermath_sample_count'], 3)

    def test_native_run_error_indexes_exact_private_parameters_and_invoked_scs(self):
        script = self.write('numeric/capture.il', '\n'.join([
            'sstatus(errsetTrace t)',
            'crescNpRows=list(list("p1" "privateLib/privateCell/schematic_FTdiff/M1/nfin" "5"))',
            'runId=maeRunSimulation(?waitUntilDone t ?returnRunId t ?session session)',
            "maeWaitUntilDone('All ?session session)",
            'printf("CRESCENDO_PRIVATE_NETLIST_CAPTURE_DONE\\n")']))
        self.write('numeric/capture_diagnostics.json', {'schema_version':'crescendo.native_capture_diagnostics.v1',
            'capture_only':True, 'script_path':script, 'skill':{'ok':False,
                'first_error':{'message':'*Error* eval: unbound variable - schematic_FTdiff',
                    'last_successful_block':{'body_first_line':3,'body_last_line':5}}}})
        self.write('numeric/ocean.stdout', 'prefix context\n'*12 +
            '*Error* eval: unbound variable - schematic_FTdiff\n' +
            '*Error* lineread/read: syntax error encountered in input at line 1 column 64 in file %s\n' % script)
        cwd = os.path.join(self.root, 'workspace/current/netlist')
        self.write('numeric/invocations/capture.123', cwd+'\0input.scs\0+aps\0')
        scs = 'parameters FN_gm=20 DPAR_1=5\n// Library name: privateLib\n// Cell name: privateCell\n// View name: schematic_FTdiff\nM1 (d g s b) nch nfin=DPAR_1 nf=FN_gm\n'
        self.write('workspace/current/netlist/input.scs', scs)
        self.write('workspace/unbound-old/netlist/input.scs', scs.replace('nfin=DPAR_1','nfin=999'))
        prepared={'parameters':[{'parameter_id':'p1','hierarchy_path':'/I22/M1'}], 'hierarchy':{'designs':[
            {'design':{'library':'L','cell':'TB','view':'config'}, 'hierarchy':{
                'resolved_top':{'library':'L','cell':'TB','view':'schematic_custom'},
                'config_bindings':[{'hierarchy_path':'/I22','resolved_library':'L','resolved_cell':'sub',
                    'resolved_view':'schematic_FTdiff','resolved_view_type':'schematic','scan_traversable':True}]}}]}}
        records=self.records(prepared=prepared)
        payload=next(row['payload'] for row in records if row['observation']=='diagnostic.native_run_context')
        self.assertEqual(payload['selected_native_parameters'][0]['path'], 'privateLib/privateCell/schematic_FTdiff/M1/nfin')
        self.assertEqual(payload['selected_native_parameters'][0]['requested'], '5')
        self.assertIn('maeRunSimulation',json.dumps(payload['phase']))
        self.assertEqual(payload['source_config']['related_bindings'][0]['resolved_view'],'schematic_FTdiff')
        self.assertEqual(len(payload['generated_netlists']),1)
        device=payload['generated_netlists'][0]['selected_devices'][0]
        self.assertEqual(device['status'],'exact_private_CV_instance_observed')
        self.assertIn('nf=FN_gm',device['statement'])
        self.assertNotIn('999',json.dumps(payload))
        self.assertEqual(payload['generated_netlists'][0]['settings_reference'],
            'diagnostic.netlist_points: same netlist_sha256')
        context=next(row['payload']['context'] for row in records
            if row['observation']=='diagnostic.script_context' and row['payload']['path']=='numeric/capture.il')
        self.assertEqual(context['reported_error_lines'],[])
        self.assertEqual(context['unattributed_or_external_locations'][0]['physical_line_characters'],22)
        from crescendo_engine.diagnosis_catalog import observation_section
        self.assertEqual(observation_section({'observation':'diagnostic.native_run_context'}),'netlist')

    def test_successful_capture_adds_no_native_run_failure_dump(self):
        self.write('numeric/capture.il', 'runId=maeRunSimulation(?waitUntilDone t)')
        self.write('numeric/capture_diagnostics.json', {'capture_only':True,'skill':{'ok':True}})
        self.assertFalse(any(row['observation']=='diagnostic.native_run_context' for row in self.records()))

    def test_native_run_context_rejects_expressions_and_ambiguous_scs_identity(self):
        rows,reason=diagnostics._native_selected_rows('crescNpRows=list(list("p" evalstring("BAD") "5"))')
        self.assertEqual(rows,[])
        self.assertIn('nonliteral',reason)
        script='crescNpRows=list(list("p" "L/C/schematic_custom/M1/nfin" "5"))'
        text='// Library name: L\n// Cell name: C\n// View name: schematic_custom\nM1 (a b) n nfin=5\nM1 (c d) n nfin=7\n'
        payload=diagnostics._native_run_failure_context(script, {'skill':{'first_error':None}}, {},
            [{'path':'input.scs','sha256':'s','text':text,'read_truncated':False}])
        device=payload['generated_netlists'][0]['selected_devices'][0]
        self.assertEqual(device['match_count'],2)
        self.assertEqual(device['status'],'UNKNOWN')
        self.assertNotIn('statement',device)

    def test_native_run_failure_context_bounds_selected_rows_and_point_evidence(self):
        script='crescNpRows=list('+ ' '.join('list("p%d" "L/C/schematic_custom/M%d/nfin" "5")' % (n,n)
            for n in range(40)) + ')'
        netlists=[{'path':'input%d.scs'%n,'sha256':str(n),'text':'parameters a=1',
            'read_truncated':False} for n in range(6)]
        payload=diagnostics._native_run_failure_context(script,{'skill':{}},{},netlists)
        self.assertEqual(payload['selected_count'],40)
        self.assertEqual(len(payload['selected_native_parameters']),32)
        self.assertEqual(payload['selected_omitted'],8)
        self.assertEqual(len(payload['generated_netlists']),4)
        self.assertEqual(payload['unique_netlists_omitted'],2)
        self.assertEqual(len(payload['generated_netlists'][0]['selected_devices']),16)
        self.assertEqual(payload['generated_netlists'][0]['selected_devices_omitted'],24)

    def test_ocean_compaction_keeps_sixteen_printed_frames_after_native_error(self):
        frames=['native_function_%02d()' % number for number in range(16)]
        console='startup\n'*40 + '*Error* eval: unbound variable - schematic_custom\nStack Trace:\n\n' + \
            '\n'.join(frames) + '\n' + 'ordinary output\n'*500
        context=diagnostics._windows(console, console_source='numeric/ocean.stdout')
        retained=[row['text'] for row in context['lines']]
        for frame in frames:
            self.assertIn(frame,retained)
        self.assertLess(len(retained),100)
        self.assertEqual(context['error_count'],1)

    def test_setup_private_summary_is_once_and_keeps_identity_receipt_without_binding_dump(self):
        reference={'schema_version':'crescendo.setup_private_session.v1','session_id':'s','status':'ready',
            'diagnostic_log':'/owned/setup/environment_test.log',
            'maestro':{'library':'L','cell':'TB','view':'private_maestro','path':'/owned/TB/private_maestro'},
            'configs':[{'library':'L','cell':'TB','view':'private_config','path':'/owned/TB/private_config',
                'top':['L','TB','schematic_custom'],'bindings':[{'path':'/private-binding-do-not-dump'}]}],
            'views':[{'target':['L','TB','private_config']}],
            'binding_verification':{'ok':True,'config_records':'/owned/config.records','solver_invocations':0}}
        records=self.records(prepared={'setup_private_session':reference})
        summaries=[row for row in records if row['observation']=='diagnostic.setup_private_session']
        self.assertEqual(len(summaries),1)
        summary=summaries[0]['payload']
        self.assertEqual(summary['configs'][0]['view'],'private_config')
        self.assertEqual(summary['configs'][0]['binding_count'],1)
        self.assertTrue(summary['native_binding_receipt']['ok'])
        self.assertEqual(summary['diagnostic_log'],'/owned/setup/environment_test.log')
        self.assertEqual(summary['private_view_count'],1)
        self.assertNotIn('private-binding-do-not-dump',json.dumps(summary))

    def test_native_load_error_uses_owned_patch_callback_context_not_capture_line(self):
        rows = ['padding_%d()' % number for number in range(1,81)]
        rows[34] = 'cdfParam~>value = "5"'
        rows[39] = 'unless(callbackResult error("CDF callback failed for nfin: %%L" callbackExpr))'
        patch = self.write('numeric/patch.il','\n'.join(rows))
        self.write('numeric/capture.il','\n'.join('capture_only_%d()' % number for number in range(1,81)))
        self.write('numeric/ocean.stdout','*Error* int: can\'t handle int("FN_gm")\n' +
            '*Error* CRESCENDO_ADAPTER_ERROR CDF callback failed for nfin: "C710_coremos_CB( \'nfin )"\n' +
            '*Error* load: error while loading file - "%s" at line 75\n' % patch)
        records = self.records()
        contexts = {row['payload']['path']:row['payload'] for row in records if row['observation'] == 'diagnostic.script_context'}
        self.assertIn('numeric/patch.il',contexts)
        context = contexts['numeric/patch.il']['context']
        self.assertEqual(context['reported_error_lines'],[75])
        self.assertTrue(any(row['line'] == 35 and row['text'] == 'cdfParam~>value = "5"' for row in context['lines']))
        self.assertIn('CDF callback failed for nfin',json.dumps(context))
        self.assertEqual(contexts['numeric/capture.il']['context']['reported_error_lines'],[])
        self.assertEqual(context['reported_error_locations'][0]['reported_file'],patch)

    def test_large_referenced_patch_keeps_tail_callback_without_inventing_physical_lines(self):
        rows = ['long_unrelated_clone_form_%s()' % ('x'*60) for unused in range(500)]
        rows.extend(['cdfParam~>value = "5"','callbackExpr = cdfParam~>callback',
            'unless(callbackResult error("CDF callback failed for nfin: %%L" callbackExpr))',
            'close(crescVerify)','crescSchematicPatchResult = nil'])
        patch = self.write('baseline/patch.il','\n'.join(rows))
        self.write('baseline/ocean.stdout','*Error* CDF callback failed for nfin: "vendorCallback"\n' +
            '*Error* load: error while loading file - "%s" at line %d\n' % (patch,len(rows)))
        with mock.patch.object(diagnostics,'MAX_FILE',1024):
            records = self.records()
        payload = next(row['payload'] for row in records if row['observation'] == 'diagnostic.script_context' and row['payload']['path'] == 'baseline/patch.il')
        context = payload['context']
        self.assertTrue(payload['identity']['read_truncated'])
        self.assertEqual(payload['identity']['hash_scope'],'head_tail')
        self.assertEqual(context['reported_error_locations'][0]['line'],len(rows))
        self.assertEqual(context['reported_error_lines'],[])
        self.assertIn('head_tail_excerpt',context['line_number_scope'])
        self.assertIn('cdfParam~>value',json.dumps(context))
        self.assertTrue(all(row['line'] is None for row in context['lines']))
        collection = next(row['payload'] for row in records if row['observation'] == 'diagnostic.collection')
        self.assertLessEqual(collection['read_bytes'],diagnostics.MAX_READ_BYTES)

    def test_reported_patch_line_survives_many_same_property_candidate_callback_blocks(self):
        rows = ['padding_%d()' % number for number in range(1,1001)]
        for index in (49,149,249,349,449):
            rows[index] = 'unless(callbackResult error("CDF callback failed for nfin: %%L" callbackExpr))'
        rows[949] = 'reported_actual_load_failure()'
        patch = self.write('baseline/patch.il','\n'.join(rows))
        self.write('baseline/ocean.stdout','*Error* CDF callback failed for nfin: "vendorCallback"\n' +
            '*Error* load: error while loading file - "%s" at line 950\n' % patch)
        records = self.records()
        payload = next(row['payload'] for row in records if row['observation'] == 'diagnostic.script_context' and row['payload']['path'] == 'baseline/patch.il')
        context = payload['context']
        self.assertEqual(context['reported_error_lines'],[950])
        self.assertIn(950,[row['line'] for row in context['lines']])
        self.assertEqual(context['reported_error_lines_omitted_from_excerpt'],[])
        self.assertEqual(context['reported_error_window_lines_omitted'],0)
        self.assertLessEqual(len(context['lines']),160)
        self.assertIn('candidate callback blocks',context['callback_context_scope'])
        self.assertIn('does not identify the failing instance',context['callback_context_scope'])
        self.assertFalse(context['context_truncated'])
        self.assertLess(len(context['lines']), 32)

    def test_unreferenced_external_or_linked_patch_is_not_read(self):
        self.write('unreferenced/patch.il','UNREFERENCED_PRIVATE_CODE')
        with tempfile.TemporaryDirectory() as outside:
            external = os.path.join(outside,'patch.il')
            with open(external,'w') as stream: stream.write('EXTERNAL_PRIVATE_CODE')
            linked = os.path.join(self.root,'numeric','patch.il')
            os.makedirs(os.path.dirname(linked))
            os.symlink(external,linked)
            self.write('numeric/ocean.stdout','*Error* load: error while loading file - "%s" at line 1\n' % external +
                '*Error* load: error while loading file - "%s" at line 1\n' % linked)
            records = self.records()
        text = json.dumps(records)
        self.assertNotIn('UNREFERENCED_PRIVATE_CODE',text)
        self.assertNotIn('EXTERNAL_PRIVATE_CODE',text)
        self.assertFalse(any(row['observation'] == 'diagnostic.script_context' and row['payload']['path'].endswith('patch.il') for row in records))

    def test_callback_input_marker_survives_error_context_compaction(self):
        rows = ['*Error* unrelated startup %d' % number for number in range(60)]
        marker = 'CRESCENDO_CDF_CALLBACK_INPUT param_id=p requested=5 oa="5" cdf="5"'
        rows.insert(30,marker)
        rows.extend('ordinary final state %d' % number for number in range(500))
        context = diagnostics._windows('\n'.join(rows),max_lines=32)
        self.assertLessEqual(len(context['lines']),32)
        self.assertIn(marker,[row['text'] for row in context['lines']])
        self.assertEqual(context['callback_input_lines_retained'],1)
        self.assertEqual(context['callback_input_lines_omitted'],0)
        self.assertTrue(context['context_truncated'])

    def test_simulation_placeholder_does_not_mask_real_source_or_private_corruption(self):
        placeholder = self.write('workspace/simulation/lib/cell/view/maestro.sdb', '')
        os.makedirs(os.path.join(os.path.dirname(placeholder), 'results', 'maestro'))
        records = self.records()
        summary = next(row['payload'] for row in records if row['observation'] == 'diagnostic.collection')
        self.assertFalse(any('ParseError' in row.get('reason', '') for row in summary['omitted_or_truncated']))
        self.assertEqual(len(summary['excluded_non_contract_artifacts']), 1)
        declared = {'setup': {'source': {'maestro_sdb': placeholder}}}
        records = self.records(prepared=declared)
        summary = next(row['payload'] for row in records if row['observation'] == 'diagnostic.collection')
        self.assertTrue(any('ParseError' in row.get('reason', '') for row in summary['omitted_or_truncated']))
        self.assertEqual(summary['excluded_non_contract_artifacts'], [])
        self.write('corner_identity.json', {'private_setup': {'path': placeholder}})
        records = self.records()
        summary = next(row['payload'] for row in records if row['observation'] == 'diagnostic.collection')
        self.assertEqual(summary['excluded_non_contract_artifacts'], [])
        self.assertTrue(any('ParseError' in row.get('reason', '') for row in summary['omitted_or_truncated']))

    def test_system_warning_and_other_file_line_numbers_do_not_become_capture_error_locations(self):
        script = self.write('baseline/capture.il', 'sstatus(errsetTrace t)\nnext()\n')
        self.write('baseline/ocean.stdout', '*WARNING* cds.lib, Line 4, duplicate INCLUDE\n' +
            '*Error* syntax error line 3780 column 2 in file /opt/vendor/formatter.il\n' +
            '*Error* syntax error line 2 column 3 in file %s\n' % script +
            '*Error* syntax error line 1 column 64 in file %s\n' % script)
        records = self.records()
        context = next(row['payload']['context'] for row in records if row['observation'] == 'diagnostic.script_context')
        self.assertEqual(context['reported_error_lines'], [2])
        other = context['unattributed_or_external_locations']
        self.assertTrue(any(row['line'] == 3780 and row['attribution'] == 'external_file' for row in other))
        self.assertTrue(any(row['column'] == 64 and 'indirect_reader' in row['attribution'] for row in other))

    def test_all_points_and_simulator_options_temperature_survive_variant_compaction(self):
        for point in range(1, 37):
            self.write('tasks/p%s/netlist/input.scs' % point,
                'parameters Fref=64M N=%s trim=%s\n' % (62 + (point // 9) * 18, point % 2) +
                'include "model.scs" section=ff\nsimulatorOptions options temp=125\n')
        records = self.records()
        payload = next(row['payload'] for row in records if row['observation'] == 'diagnostic.netlist_points')
        self.assertEqual(len(payload['point_index']), 36)
        self.assertLess(len(payload['settings_variants']), 36)
        self.assertTrue(all(row['temperature_sources'] for row in payload['settings_variants']))
        self.assertIn('N=62', json.dumps(payload))

    def test_source_global_test_sweep_model_and_runplan_are_included_without_extra_material(self):
        source = self.write('source/maestro.sdb', '<Setup><global><variables><variable name="Fref">64M</variable>'
            '</variables></global><tests><test name="A"><variables><variable name="N">162</variable>'
            '</variables></test></tests><sweeps><sweep name="N">62 80 126 140</sweep></sweeps>'
            '<modelgroup name="PVT">ff ss</modelgroup><runplan name="all">enabled</runplan></Setup>')
        records = self.records(prepared={'setup': {'source': {'maestro_sdb': source}}})
        payload = next(row['payload'] for row in records if row['observation'] == 'diagnostic.source_settings')
        raw = json.dumps(payload)
        for value in ('64M', '162', '62 80 126 140', 'ff ss', 'enabled', '"name": "A"'):
            self.assertIn(value, raw)

    def test_native_vars_global_scope_is_not_lost_and_cancel_does_no_reads(self):
        source = self.write('maestro.sdb', '<Setup><active><vars><var name="Fref">64M</var></vars></active></Setup>')
        records = self.records(prepared={'setup': {'source': {'maestro_sdb': source}}})
        source_rows = next(row['payload']['settings_rows'] for row in records if row['observation'] == 'diagnostic.source_settings')
        self.assertTrue(any('/active/vars/var' in row['scope'] for row in source_rows))
        with mock.patch.object(diagnostics, '_read') as read:
            records = diagnostics.collect_run_diagnostics(self.root, 'private_netlist', cancel=lambda: True)
        read.assert_not_called()
        summary = next(row['payload'] for row in records if row['observation'] == 'diagnostic.collection')
        self.assertEqual(summary['status'], 'PARTIAL')
        self.assertIn('cancelled', json.dumps(summary))

    def test_large_identity_is_split_or_explicitly_incomplete_never_claimed_missing_source_inline(self):
        global_values = [{'name': 'x%s' % index, 'value': 'v%s_' % index + 'x' * 800} for index in range(420)]
        self.write('corner_identity.json', {'saved_source': {'global_variables': global_values,
            'execution_settings': [{'name': 'runplan', 'value': 'all'}], 'sha256': 'test-source'},
            'private_setup': {'global_variables': global_values, 'execution_settings': [{'name': 'runplan', 'value': 'all'}]}})
        records = self.records()
        self.assertTrue(any(row['observation'] == 'diagnostic.artifact_section' for row in records))
        with mock.patch.object(diagnostics, 'MAX_TOTAL', 220000):
            limited = self.records()
        source_claim = [row['payload'] for row in limited if row['observation'] == 'diagnostic.source_settings']
        self.assertFalse(any('already inlined once' in row.get('scope', '') for row in source_claim))
        summary = next(row['payload'] for row in limited if row['observation'] == 'diagnostic.collection')
        self.assertEqual(summary['status'], 'PARTIAL')

    def test_expensive_corner_expansion_cannot_prevent_owned_log_processing(self):
        self.write('corner_identity.json', {'slow_contract_marker': True, 'saved_source': {}})
        self.write('ocean.stdout', 'before\nERROR (ASSEMBLER-2404): handle 0\nafter\n')
        clock = [0]
        original = diagnostics._semantic_artifact
        def semantic(value):
            if isinstance(value, dict) and value.get('slow_contract_marker'):
                clock[0] = 20
            return original(value)
        with mock.patch.object(diagnostics.time, 'monotonic', side_effect=lambda: clock[0]), \
                mock.patch.object(diagnostics, '_semantic_artifact', side_effect=semantic):
            records = self.records()
        logs = [r['payload'] for r in records if r['observation'] == 'diagnostic.log_context']
        self.assertTrue(any(row['path'] == 'ocean.stdout' and 'ASSEMBLER-2404' in json.dumps(row)
                            for row in logs))
        summary = next(r['payload'] for r in records if r['observation'] == 'diagnostic.collection')
        self.assertEqual(summary['status'], 'PARTIAL')
        self.assertTrue(any(row.get('reason') == 'collection deadline'
                            for row in summary['omitted_or_truncated']))

    def test_deadline_lists_unprocessed_discovered_paths(self):
        self.write('a/ocean.stdout', 'first')
        self.write('b/ocean.stdout', 'second')
        clock = [0]
        original = diagnostics._read
        def read(*args, **kwargs):
            result = original(*args, **kwargs)
            clock[0] = 20
            return result
        with mock.patch.object(diagnostics.time, 'monotonic', side_effect=lambda: clock[0]), \
                mock.patch.object(diagnostics, '_read', side_effect=read):
            records = self.records()
        summary = next(r['payload'] for r in records if r['observation'] == 'diagnostic.collection')
        omission = next(row for row in summary['omitted_or_truncated'] if row.get('phase') == 'file reading')
        self.assertEqual(omission['unread_file_count'], 1)
        self.assertEqual(omission['unread_files'], ['b/ocean.stdout'])

    def test_native_log_is_read_before_unrelated_large_json(self):
        self.write('a/portability_settings.json', {'payload': 'x' * 200000})
        self.write('z/ocean.stdout', 'ERROR (ASSEMBLER-2404): synthetic\n')
        read_paths = []
        original = diagnostics._read
        def read(path, **kwargs):
            read_paths.append(os.path.relpath(path, self.root))
            return original(path, **kwargs)
        with mock.patch.object(diagnostics, '_read', side_effect=read):
            records = self.records()
        self.assertLess(read_paths.index('z/ocean.stdout'), read_paths.index('a/portability_settings.json'))
        self.assertTrue(any(r['observation'] == 'diagnostic.log_context' and
                            r['payload']['path'] == 'z/ocean.stdout' for r in records))

    def test_incomplete_deferred_contract_falls_back_to_saved_setup(self):
        source = self.write('source/maestro.sdb', '<Setup><active><vars><var name="ONLY_FALLBACK">42</var>'
                            '</vars></active></Setup>')
        rows = [{'name': 'x%s' % i, 'value': 'x' * 700} for i in range(500)]
        self.write('corner_identity.json', {'saved_source': {'global_variables': rows,
            'execution_settings': [{'name': 'runplan', 'value': 'all'}]}})
        with mock.patch.object(diagnostics, 'MAX_TOTAL', 220000):
            records = self.records(prepared={'setup': {'source': {'maestro_sdb': source}}})
        settings = [r['payload'] for r in records if r['observation'] == 'diagnostic.source_settings']
        self.assertTrue(any('ONLY_FALLBACK' in json.dumps(item) for item in settings))
        self.assertFalse(any(item.get('source') == 'corner_identity.saved_source' for item in settings))

    def test_split_boundary_keeps_secrets_redacted(self):
        sentinel = 'synthetic-sentinel-value-ABCD'
        rows = [{'name': 'row_%04d' % i, 'value': sentinel + ('x' * 450)} for i in range(400)]
        self.write('corner_identity.json', {'saved_source': {'global_variables': rows,
            'execution_settings': [{'name': 'runplan', 'value': 'all'}]}})
        with mock.patch.dict(os.environ, {'CRESCENDO_TEST_SECRET': sentinel}):
            records = self.records()
        payload = json.dumps(records)
        self.assertNotIn(sentinel, payload)
        self.assertIn('<redacted>', payload)
        self.assertTrue(any(row['observation'] == 'diagnostic.artifact_section' for row in records))
        self.assertTrue(all(len(diagnostics._serialized(row['payload']).encode('utf-8')) <= 192 * 1024
                            for row in records))

    def test_total_read_budget_limits_work_and_retains_final_collection_status(self):
        for number in range(20):
            self.write('tasks/%s/ocean.stdout' % number, 'header\n' + 'x' * 2000)
        with mock.patch.object(diagnostics, 'MAX_READ_BYTES', 4096):
            records = self.records()
        summary = next(row['payload'] for row in records if row['observation'] == 'diagnostic.collection')
        self.assertLessEqual(summary['read_bytes'], 4096)
        self.assertLess(summary['files_read'], 20)
        self.assertEqual(summary['status'], 'PARTIAL')

    def test_large_analysis_rows_cannot_displace_native_global_and_sweep_contract(self):
        source = self.write('maestro.sdb', '<Setup><active><analyses>' +
            ''.join('<analysis name="plot_%s">config</analysis>' % i for i in range(800)) +
            '</analyses><vars><var name="Fref">64M</var></vars><sweeps><sweep name="N">62 80</sweep>'
            '</sweeps></active></Setup>')
        result = diagnostics._settings_xml(source)
        raw = json.dumps(result['settings_rows'])
        self.assertIn('64M', raw)
        self.assertIn('62 80', raw)
        self.assertFalse(result['scope_coverage']['necessary_contract_incomplete'])
        self.assertGreater(result['scope_coverage']['omitted_rows']['analysis'], 0)

    def test_history_cannot_supply_or_displace_current_setup_settings(self):
        old_vars = ''.join('<var name="old_%s">HISTORY_VALUE_%s</var>' % (i, i) for i in range(800))
        old_settings = '<model section="ss">/old/models.scs</model><postRunScript>old_submit()</postRunScript>'
        source = self.write('maestro.sdb', '<setupdb><active><currentmode>SRSC</currentmode>'
            '<vars><var name="SW">1</var></vars><modelFiles><model section="TT">/current/models.scs</model>'
            '</modelFiles><tests><test name="C"><simulator>spectre</simulator></test></tests></active>'
            '<history><historyentry><checkpoint><vars>' + old_vars + '</vars>' + old_settings +
            '</checkpoint></historyentry></history></setupdb>')
        result = diagnostics._settings_xml(source)
        rows = json.dumps(result['settings_rows'])
        for value in ('SRSC', 'SW', '/current/models.scs', 'spectre'):
            self.assertIn(value, rows)
        for value in ('HISTORY_VALUE_', '/old/models.scs', 'old_submit()', '/history/'):
            self.assertNotIn(value, rows)
        self.assertEqual(result['excluded_noncurrent']['omitted_rows']['global_test_variables'], 800)
        self.assertGreater(result['excluded_noncurrent']['omitted_rows']['corner_model_groups'], 0)
        self.assertGreater(result['excluded_noncurrent']['omitted_rows']['user_scripts'], 0)
        self.assertEqual(result['settings_status'], 'READ')
        self.assertEqual(result['schema_version'], 'crescendo.current_setup_diagnostics.v1')
        self.assertFalse(result['scope_coverage']['necessary_contract_incomplete'])

    def test_large_current_other_settings_keep_model_job_script_analysis_and_output_quotas(self):
        models = ''.join('<model section="TT_%s">/pdk/spectre/model_%s.scs</model>' % (i, i) for i in range(105))
        noise = ''.join('<unused name="unused_%s">ordinary_setting</unused>' % i for i in range(2547))
        history = ''.join('<var name="history_%s">OLD_%s</var>' % (i, i) for i in range(1406))
        source = self.write('maestro.sdb', '<setupdb><active><currentmode>SRSC</currentmode><modelFiles>' + models +
            '</modelFiles><jobSetup><option name="jobsubmitcommand">batch-submit -q current</option>'
            '<option name="runtimeout">-1</option><option name="maxjobs">50</option></jobSetup>'
            '<saveOptions><projectDir>/shared/current</projectDir></saveOptions>'
            '<preRunScript><enabled>nil</enabled><value/></preRunScript><tests><test name="C" enabled="1">'
            '<design><library>LIB</library><cell>TB</cell><view>config</view></design>'
            '<simulator>spectre</simulator><includePath>/pdk/spectre</includePath><vars><var name="SW">1</var></vars>'
            '<sweeps><sweep name="CT">0 127</sweep></sweeps><analyses><analysis name="sp" enabled="1">start=1G</analysis>'
            '</analyses><outputs><output name="Q_10G">value(Q 10G)</output></outputs>' + noise +
            '</test></tests></active><history><historyentry><checkpoint><vars>' + history +
            '</vars></checkpoint></historyentry></history></setupdb>')
        result = diagnostics._settings_xml(source)
        rows = json.dumps(result['settings_rows'])
        self.assertLessEqual(len(result['settings_rows']), diagnostics.MAX_ROWS)
        for i in range(105):
            self.assertIn('/pdk/spectre/model_%s.scs' % i, rows)
        for value in ('SRSC', 'jobsubmitcommand', 'batch-submit -q current', 'runtimeout', 'maxjobs',
                      '/shared/current', 'prerunscript', 'LIB', 'TB', 'spectre', 'start=1G', 'Q_10G', 'SW', '0 127'):
            self.assertTrue(value in rows, value)
        self.assertNotIn('OLD_', rows)
        coverage = result['scope_coverage']
        self.assertNotIn('corner_model_groups', coverage['omitted_rows'])
        for category in ('setup_identity', 'job_policy', 'save_options', 'corner_model_groups',
                         'user_scripts', 'analysis', 'outputs', 'global_test_variables', 'sweep_runplan'):
            self.assertGreater(coverage['retained_rows'][category], 0)
            self.assertEqual(coverage['total_rows'][category], coverage['retained_rows'][category])
        self.assertEqual(result['settings_status'], 'PARTIAL')
        self.assertGreater(coverage['omitted_rows']['other_test_settings'], 0)
        self.assertEqual(coverage['retained_rows']['other_test_settings'], 32)
        self.assertEqual(coverage['omitted_rows']['other_test_settings'],
                         coverage['total_rows']['other_test_settings'] - 32)
        self.assertEqual(result['excluded_noncurrent']['omitted_rows']['global_test_variables'], 1406)

    def test_script_configuration_is_metadata_only_and_never_claimed_qualified(self):
        source = self.write('maestro.sdb', '<setupdb><active><preRunScript enabled="0">'
            '<body code="DO_NOT_COPY_ATTRIBUTE">DO_NOT_COPY_SCRIPT()</body></preRunScript>'
            '<postRunScript><value/></postRunScript>'
            '<userScript><enabled>unknown</enabled><value>DO_NOT_COPY_UNKNOWN()</value></userScript>'
            '</active></setupdb>')
        result = diagnostics._settings_xml(source)
        rows = [row for row in result['settings_rows'] if row['category'] == 'user_scripts']
        raw = json.dumps(result)
        self.assertEqual(len(rows), 3)
        self.assertNotIn('DO_NOT_COPY', raw)
        self.assertEqual(rows[0]['script_state']['enabled_state'], 'explicitly_disabled')
        self.assertEqual(rows[0]['script_state']['status'], 'disabled')
        self.assertFalse(rows[0]['script_state']['blocking'])
        self.assertEqual(rows[1]['script_state']['status'], 'empty')
        self.assertEqual(rows[2]['script_state']['enabled_flags'][0]['value'], '<unrecognized>')
        self.assertEqual(rows[2]['script_state']['status'], 'unqualified')
        self.assertTrue(rows[2]['script_state']['blocking'])
        self.assertTrue(all(row['xml_path'].startswith('/active/') for row in rows))
        self.assertFalse(result['script_hook_inventory']['execution_authority'])
        self.assertFalse(result['script_hook_inventory']['script_text_included'])

    def test_private_sdb_uses_same_current_only_bounded_settings_projection(self):
        self.write('private/maestro.sdb', '<setupdb><active><vars><var name="SW">1</var></vars>'
            '<modelFiles><model section="TT">/current/alps/toplevel.scs</model></modelFiles></active>'
            '<history><checkpoint><vars><var>OLD_VAR</var></vars></checkpoint></history></setupdb>')
        records = self.records()
        summary = next(row['payload'] for row in records if row['observation'] == 'diagnostic.settings')
        self.assertIn('/current/alps/toplevel.scs', json.dumps(summary['settings_rows']))
        self.assertNotIn('OLD_VAR', json.dumps(summary))
        self.assertEqual(summary['excluded_noncurrent']['omitted_rows']['global_test_variables'], 1)
        self.assertEqual(summary['schema_version'], 'crescendo.current_setup_diagnostics.v1')

    def test_named_sensitive_variable_values_are_redacted_with_identity_retained(self):
        source = self.write('maestro.sdb', '<setupdb><active><vars>'
            '<var>API_KEY<value>UNREPORTED_NATIVE_SECRET</value></var>'
            '<var name="PASSWORD" value="UNREPORTED_ATTRIBUTE_SECRET">UNREPORTED_TEXT_SECRET</var>'
            '<var name="SW">1</var></vars></active></setupdb>')
        result = diagnostics._settings_xml(source)
        raw = json.dumps(result)
        self.assertIn('API_KEY', raw)
        self.assertIn('PASSWORD', raw)
        self.assertIn('SW', raw)
        self.assertNotIn('UNREPORTED_', raw)
        self.assertIn('redacted', raw)

    def test_actual_current_quota_omissions_are_explicit_and_not_contract_complete(self):
        models = ''.join('<model section="TT">/models/%s.scs</model>' % i for i in range(800))
        source = self.write('maestro.sdb', '<setupdb><active><modelFiles>' + models + '</modelFiles>'
            '<vars><var name="SW">1</var></vars></active></setupdb>')
        result = diagnostics._settings_xml(source)
        coverage = result['scope_coverage']
        self.assertEqual(len(result['settings_rows']), diagnostics.MAX_ROWS)
        self.assertEqual(coverage['total_rows']['corner_model_groups'], 800)
        self.assertEqual(coverage['retained_rows']['corner_model_groups'] +
                         coverage['omitted_rows']['corner_model_groups'], 800)
        self.assertEqual(result['settings_status'], 'PARTIAL')
        self.assertTrue(coverage['necessary_contract_incomplete'])
        self.assertIn('SW', json.dumps(result['settings_rows']))

    def test_precise_script_producer_failure_is_retained_before_large_artifact_read(self):
        self.write('corner_identity.json', {'large': 'old_structure_' * 2000})
        self.write('private_netlist/preparation_failure.json', {'error_type': 'PrivateNetlistUnsupported',
            'message': 'unsupported saved script', 'detail': {'failure_stage': 'native_user_scripts',
                'source_setup_path': '/source/maestro.sdb', 'source_setup_sha256': 'source-hash',
                'execution_started': False, 'cleanup_proven': True,
                'script_checks': [{'xml_path': '/setupdb/active[1]/postRunScript[1]', 'tag': 'postRunScript',
                    'enabled_state': 'enabled', 'blocking': True,
                    'payload_metadata': {'status': 'present', 'sha256': 'payload-hash', 'bytes': 12,
                                         'kind': 'script_text'}}]}})
        with mock.patch.object(diagnostics, 'MAX_READ_BYTES', 4096):
            records = self.records()
        index = next(row['payload'] for row in records if row['observation'] == 'diagnostic.producer_failure')
        self.assertEqual(index['data']['detail']['failure_stage'], 'native_user_scripts')
        self.assertEqual(index['data']['detail']['source_setup_sha256'], 'source-hash')
        self.assertFalse(index['data']['detail']['execution_started'])
        self.assertTrue(index['data']['detail']['script_checks'][0]['blocking'])
        self.assertEqual(diagnostics.failure_layer(index['data']['detail']), 'netlist_generation')
        final = next(row['payload'] for row in records if row['observation'] == 'diagnostic.collection')
        self.assertLessEqual(final['read_bytes'], 4096)
        self.assertEqual(final['status'], 'PARTIAL')

    def test_shared_hook_payload_array_survives_precise_failure_index_without_code(self):
        metadata = {'xml_path': '/active/postrunscript[1]/scriptfile[1]', 'kind': 'scriptfile',
            'bytes': 16, 'sha256': 'payload-digest', 'text_included': False, 'file_reference': '/local/hook.il',
            'text': 'DO_NOT_INCLUDE_EXECUTABLE_TEXT'}
        failure = diagnostics._producer_failure_index({'error_type': 'PrivateNetlistUnsupported',
            'detail': {'script_checks': [{'tag': 'postrunscript', 'enabled_state': 'enabled_or_unspecified',
                'status': 'unqualified', 'blocking': True, 'enabled_flags': [],
                'payload_metadata': [metadata], 'payload_field_count': 1, 'payload_fields_omitted': 0}]}})
        check = failure['detail']['script_checks'][0]
        self.assertEqual(check['status'], 'unqualified')
        self.assertEqual(check['payload_metadata'][0]['sha256'], 'payload-digest')
        self.assertEqual(check['payload_metadata'][0]['file_reference'], '/local/hook.il')
        self.assertFalse(check['payload_metadata'][0]['text_included'])
        self.assertNotIn('DO_NOT_INCLUDE_EXECUTABLE_TEXT', json.dumps(failure))

    def test_package_identity_includes_application_and_shipped_release_version(self):
        from crescendo_engine import __version__
        self.write('crescendo_engine/module.py', 'value = 1\n')
        self.write('DELIVERY_MANIFEST.json', {'version': '1.1.1', 'source_commit': 'committed-source',
            'tag': 'v1.1.1', 'private_environment': 'DO_NOT_COPY_PRIVATE_ENVIRONMENT'})
        with mock.patch.object(diagnostics, '__file__', os.path.join(self.root, 'crescendo_engine', 'module.py')):
            identity = diagnostics.package_identity()
        self.assertEqual(identity['application_version'], __version__)
        self.assertEqual(identity['delivery_manifest']['metadata']['version'], '1.1.1')
        self.assertEqual(identity['delivery_manifest']['metadata']['source_commit'], 'committed-source')
        self.assertEqual(identity['delivery_manifest']['metadata']['tag'], 'v1.1.1')
        self.assertNotIn('DO_NOT_COPY_PRIVATE_ENVIRONMENT', json.dumps(identity))

    def test_report_redacts_unknown_inline_token_and_license_values(self):
        self.write('baseline/command.json', {'returncode': 1, 'stdout_tail': 'token=UNKNOWN_SECRET\n27000@private-license\n',
                                            'launch_environment': {'HOME': '/private/caller'}})
        self.write('baseline/ocean.stderr', 'API_KEY=another-secret\nAuthorization: Bearer abc.def.ghi\n')
        raw = json.dumps(self.records())
        for value in ('UNKNOWN_SECRET', 'private-license', 'another-secret', 'abc.def.ghi', '/private/caller'):
            self.assertNotIn(value, raw)
        self.assertIn('redacted', raw)

    def test_repeated_settings_are_interned_and_budget_omissions_are_explicit(self):
        value = {'global_variables': [{'name': 'x%s' % i, 'value': str(i)} for i in range(100)]}
        document = {'saved_source': value, 'private_setup': value}
        self.write('corner_identity.json', document)
        records = self.records()
        artifact = next(row['payload']['data'] for row in records if row['observation'] == 'diagnostic.artifact')
        self.assertEqual(artifact['private_setup']['same_as'], 'corner_identity.json#data/saved_source')
        with mock.patch.object(diagnostics, 'MAX_TOTAL', 65537):
            records = self.records()
        summary = next(row['payload'] for row in records if row['observation'] == 'diagnostic.collection')
        self.assertEqual(summary['status'], 'PARTIAL')
        self.assertTrue(summary['omitted_or_truncated'])

    def test_large_log_includes_file_head_and_tail_with_explicit_byte_gap(self):
        self.write('ocean.stdout', '*Error* FIRST_ERROR\n' + 'filler\n' * 200 + '*Error* LAST_ERROR\n')
        with mock.patch.object(diagnostics, 'MAX_FILE', 256):
            records = self.records()
        raw = json.dumps(records)
        self.assertIn('FIRST_ERROR', raw)
        self.assertIn('LAST_ERROR', raw)
        self.assertIn('read_truncated', raw)
        self.assertIn('head_tail', raw)

    def test_external_link_is_not_read_and_collector_has_no_open_self_readers(self):
        with tempfile.TemporaryDirectory() as other:
            target = os.path.join(other, 'ocean.stdout')
            with open(target, 'w') as stream:
                stream.write('foreign private evidence')
            os.symlink(target, os.path.join(self.root, 'ocean.stdout'))
            records = self.records()
        raw = json.dumps(records)
        self.assertNotIn('foreign private evidence', raw)
        self.assertIn('linked or non-regular', raw)
        evidence = next(row['payload'] for row in records if row['observation'] == 'diagnostic.cleanup_readers')
        self.assertEqual(evidence['open_run_readers'], [])
        self.assertTrue(evidence['collector_handles_closed'])

    def test_native_first_cause_stays_distinct_from_secondary_cleanup(self):
        primary = {'failure_stage': 'corner_identity', 'message': 'point map ambiguity'}
        cleanup = {'message': 'NFS busy', 'recovery_required': True}
        summary = diagnostics.failure_summary({'primary_failure': primary, 'cleanup_failure': cleanup}, [])
        self.assertEqual(summary['first_cause'], primary)
        self.assertEqual(summary['secondary_cleanup'], cleanup)

    def test_aggregate_lifecycle_cannot_hide_first_candidate_native_failure(self):
        lifecycle = {'error_type': 'RuntimeError', 'failure_stage': 'optimization',
            'message': 'all 1 candidates failed across 1 generations; inspect candidate errors'}
        error = "CompletionEvidenceError: native Maestro corner/point coverage differs: mapping_error=native_expanded_corner_count_mismatch"
        candidate = {'candidate_id': 'g000_c0000', 'status': 'failed', 'error': error,
                     'failure_stage': 'simulation_or_results', 'failure_class': 'design_or_simulation'}
        cleanup = {'message': 'NFS reader still busy', 'recovery_required': True}
        summary = diagnostics.failure_summary({'primary_failure': lifecycle, 'cleanup_failure': cleanup}, [candidate])
        first = summary['first_cause']
        self.assertEqual(first['error'], error)
        self.assertEqual(first['error_type'], 'CompletionEvidenceError')
        self.assertEqual(first['failure_stage'], candidate['failure_stage'])
        self.assertEqual(first['failure_class'], candidate['failure_class'])
        self.assertEqual(summary['secondary_lifecycle'], lifecycle)
        self.assertEqual(summary['secondary_cleanup'], cleanup)

    def test_specific_native_primary_is_preserved_and_candidate_type_is_not_guessed(self):
        primary = {'error_type': 'PrivateNetlistUnsupported', 'failure_stage': 'native_capture_point_coverage',
                   'message': 'capture count differs'}
        candidate = {'candidate_id': 'g000_c0000', 'status': 'failed', 'error': 'later cleanup failed',
                     'failure_stage': 'cleanup'}
        summary = diagnostics.failure_summary({'primary_failure': primary}, [candidate])
        self.assertEqual(summary['first_cause'], primary)
        self.assertIsNone(summary['secondary_lifecycle'])
        summary = diagnostics.failure_summary({}, [candidate])
        self.assertNotIn('error_type', summary['first_cause'])
        self.assertEqual(summary['first_cause']['failure_stage'], 'cleanup')

    def test_failed_execution_keeps_candidate_context_without_full_status_repetition(self):
        from crescendo_engine.environment_test import _failed_execution
        status = {'state': 'failed', 'message': 'aggregate failure', 'total_candidates': 1,
                  'startup_storage_preflight': {'large_internal_snapshot': True},
                  'primary_failure': {'message': 'see separate first cause'}}
        candidate = {'candidate_id': 'g000_c0000', 'status': 'failed', 'values': {'w': 3.9e-6},
                     'error': 'CompletionEvidenceError: complete actual context',
                     'failure_stage': 'simulation_or_results'}
        result = _failed_execution(status, [candidate])
        self.assertNotIn('startup_storage_preflight', result['status'])
        self.assertNotIn('primary_failure', result['status'])
        self.assertEqual(result['candidates'][0]['error'], candidate['error'])
        self.assertEqual(result['candidates'][0]['values'], candidate['values'])

    def test_subcircuit_parameters_do_not_enter_top_level_identity(self):
        result = diagnostics._netlist_identity('parameters Fref=64M\nsubckt a x y\n'
            'parameters Fref=2M\nends a\nsimulatorOptions options temp=-40\n')
        raw = json.dumps(result)
        self.assertIn('64M', raw)
        self.assertNotIn('2M', raw)
        self.assertIn('temp=-40', raw)

    def test_180_point_success_matrix_keeps_each_point_and_domains_once(self):
        checks = [{'point_id': point, 'corner': 'PVT_%s' % (point % 9), 'variable': variable,
                   'source': 'global_sweep', 'expected_expression': '62 80', 'expected_domain': [62, 80],
                   'actual': {'value': 62}, 'rdb_value': 62, 'ok': True}
                  for point in range(180) for variable in ('N', 'Fref', 'trim', 'finger')]
        self.write('corner_identity.json', {'native_coverage': {'test_differences': [
            {'test': 'main', 'point_identity_checks': checks}]}})
        records = self.records()
        artifact = next(row['payload']['data'] for row in records if row['observation'] == 'diagnostic.artifact')
        matrix = artifact['native_coverage']['test_differences'][0]['point_identity_checks']
        self.assertEqual(matrix['point_count'], 180)
        self.assertEqual(len(matrix['points']), 180)
        self.assertEqual(len(matrix['domains']), 4)
        self.assertEqual(matrix['failed_or_unverified'], [])

    def test_template_native_reference_proof_is_inlined_without_candidate_cdf_pass_or_oa_dump(self):
        manifest = {'schema_version': 'crescendo.private_netlist.v1', 'manifest_sha256': 'frozen-template-hash',
            'parameter_symbols': {'w': 'CRESCENDO_NL_w'}, 'seed_values': {'w': 4e-6},
            'native_reference_values': [{'name': 'baseline', 'requested': {'w': 4e-6}, 'applied': {'w': 4e-6}},
                                       {'name': 'reference_low', 'requested': {'w': 3.8e-6}, 'applied': {'w': 3.8e-6}}],
            'qualification': {'ok': True, 'callback_legality_per_candidate': False,
                'scope': 'native_expression_mapping_and_recorded_reference_points', 'tasks': [
                    {'task_id': 'p1', 'ok': True, 'symbols': {'CRESCENDO_NL_w': [{'instance': 'M%s' % i} for i in range(160)]},
                     'references': [{'reference': 'baseline', 'assignment_count': 160}]}]},
            'tasks': [{'task_id': 'p1', 'large_project_assignment_snapshot': 'do not dump raw OA project'}],
            'workspace': {'large_hierarchy_snapshot': 'unrelated full workspace'},
            'candidate_value_policy': 'raw_finite_values_without_CDF_quantization_or_clipping'}
        self.write('templates/private_netlist.json', manifest)
        records = self.records()
        proof = next(row['payload']['data'] for row in records if row['observation'] == 'diagnostic.artifact'
                     and row['payload']['path'] == 'templates/private_netlist.json')
        self.assertTrue(proof['qualification']['ok'])
        self.assertEqual(proof['candidate_cdf_callback_verification'], 'NOT_TESTED')
        self.assertEqual(proof['native_reference_values'][0]['applied'], {'w': 4e-6})
        self.assertEqual(proof['task_qualification'][0]['symbol_binding_count'], 160)
        raw = json.dumps(proof)
        self.assertNotIn('large_hierarchy_snapshot', raw)
        self.assertNotIn('do not dump raw OA project', raw)
        self.assertIn('assignment_count', raw)

    def test_candidate_point_identity_includes_all_native_batch_api_stages(self):
        stages = ('before_copy', 'after_copy', 'after_corner_enable', 'after_test_enable')
        records = [{'stage': stage, 'source_test': 'vco_nominal', 'alias_test': 'cr_alias',
                    'source_corners_skill': '((Fhigh t 62 80)) source_%s' % index,
                    'alias_corners_skill': '((Fhigh t 62 80)) alias_%s' % index}
                   for index, stage in enumerate(stages)]
        document = {'schema_version': 'crescendo.native_point_identity.v1', 'points': [
            {'point_id': i, 'test': 'vco_nominal', 'corner': 'Fhigh_%s' % i,
             'native_parameters': [{'name': 'N', 'value': 62 + i}], 'temperature': {'value': 27},
             'native_models': [{'section': 'tt'}], 'netlist_sha256': 'hash_%s' % i} for i in range(4)],
            'native_batch_scope': {'schema_version': 'crescendo.batch_native_scope.v1', 'records': records}}
        self.write('candidates/g000_c0000/point_identity.json', document)
        collected = self.records()
        artifact = next(row['payload']['data'] for row in collected if row['observation'] == 'diagnostic.artifact'
                        and row['payload']['path'].endswith('/point_identity.json'))
        self.assertEqual(artifact['native_batch_scope']['records'], records)
        self.assertEqual(len(artifact['points']), 4)


class LeanProbeTests(unittest.TestCase):
    def test_default_summary_never_runs_prior_host_dependency_battery_or_caches_old_results(self):
        with tempfile.TemporaryDirectory() as root, mock.patch.object(probes, 'bounded_command') as command, \
                mock.patch.object(probes, '_json_probe') as json_probe:
            first = probes.run_environment_probes({}, {}, root)
            second = probes.run_environment_probes({}, {}, root)
        self.assertEqual([row['id'] for row in first], ['runtime.summary'])
        self.assertEqual([row['id'] for row in second], ['runtime.summary'])
        command.assert_not_called()
        json_probe.assert_not_called()
        self.assertIn('source_tree_sha256', first[0]['details']['package'])

    def test_query_templates_require_explicit_exact_identity(self):
        self.assertEqual(scheduler_command_argv('/tool/batch-query --id {job_id} --detail', '10000001'),
                         ['/tool/batch-query', '--id', '10000001', '--detail'])
        for template in ('batch-query --all', 'batch-query {job_id} {job_id}'):
            with self.assertRaises(ValueError):
                scheduler_command_argv(template, '10000001')

    def test_only_actual_loader_or_storage_failure_triggers_targeted_observation(self):
        with tempfile.TemporaryDirectory() as root, mock.patch.object(probes, 'bounded_command') as command, \
                mock.patch.object(probes, '_native_context') as context:
            ordinary = probes.failure_environment_details({}, root, 'corner source mapping is ambiguous')
            self.assertEqual(ordinary, {})
            filesystem = probes.failure_environment_details({}, root, 'EDQUOT: Disk quota exceeded')
            self.assertIn('storage_failure', filesystem)
            missing = probes.failure_environment_details({'cadence': {'ocean_bin': '/missing/ocean'}}, root,
                'ocean: error while loading shared libraries: libfoo.so: cannot open shared object file')
            self.assertEqual(missing['native_loader_failure']['status'], 'FAIL')
        command.assert_not_called()
        context.assert_not_called()


class MissingOutputSourceEvidenceTests(unittest.TestCase):
    """Only actual missing-expression preflight failures read current state."""
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = self.temp.name
        self.run = os.path.join(self.root,'owned_run')
        os.makedirs(self.run)
        self.path = os.path.join(self.root,'active.state')
        self.prepared = {'setup':{'source':{'active_state':self.path}}}
        self.spec = {'objectives':[{'output_key':'C::gain'},{'output_key':'C::phase'}],
                     'constraints':[{'output_key':'C::power'}]}
        self.primary = {'first_cause':{'failure_stage':'measurement_preflight',
                        'message':'missing_output_expression:C::gain; missing_output_expression:C::power'}}

    def tearDown(self):
        self.temp.cleanup()

    def write(self,text):
        with open(self.path,'w',encoding='utf-8') as stream:
            stream.write(text)

    def collect(self,error=None,prepared=None):
        return diagnostics.collect_run_diagnostics(self.run,'maestro_netlist',
            prepared=self.prepared if prepared is None else prepared,spec=self.spec,
            error_text=json.dumps(self.primary if error is None else error))

    def targeted(self,records):
        return next(record['payload'] for record in records if record['observation'] == 'diagnostic.missing_output_source')

    def test_reads_only_current_requested_outputs_and_records_literal_identity(self):
        self.write('''<state><test Name="C">
          <field Name="outputList_1"><field Name="name">"gain"</field><field Name="expression">"db20(VF(\\\"/out\\\"))"</field></field>
          <field Name="outputList_2"><field Name="name">"power"</field><field Name="expr">"average(IT(\\\"/VDD\\\"))"</field></field>
          <field Name="outputList_3"><field Name="name">"phase"</field><field Name="expression">"not-requested-expression"</field></field>
          <history><field Name="outputList_4"><field Name="name">"gain"</field><field Name="expression">"historical-expression"</field></field></history>
          <field Name="history"><field Name="outputList_5"><field Name="name">"gain"</field><field Name="expression">"field-history-expression"</field></field></field>
        </test><test Name="other"><field Name="outputList_1"><field Name="name">"gain"</field><field Name="expression">"other-test-expression"</field></field></test></state>''')
        record = self.targeted(self.collect())
        self.assertEqual(record['status'],'READ')
        self.assertEqual(record['identity']['hash_scope'],'full')
        self.assertEqual(record['path'],self.path)
        rows = record['requested_outputs']
        self.assertEqual([row['output_key'] for row in rows],['C::gain','C::power'])
        self.assertEqual([row['output_match_count'] for row in rows],[1,1])
        self.assertIn('expression',rows[0]['matches'][0]['field_names'])
        self.assertTrue(rows[0]['matches'][0]['parsed_expression_present'])
        raw = json.dumps(record)
        self.assertNotIn('historical-expression',raw)
        self.assertNotIn('field-history-expression',raw)
        self.assertNotIn('not-requested-expression',raw)
        self.assertNotIn('other-test-expression',raw)

    def test_other_primary_stage_or_unselected_output_does_not_read_state(self):
        for error in ({'first_cause':{'failure_stage':'solve','message':'missing_output_expression:C::gain'}},
                      {'first_cause':{'failure_stage':'measurement_preflight','message':'unsupported analysis'}},
                      {'first_cause':{'failure_stage':'measurement_preflight','message':'missing_output_expression:C::unselected'}}):
            with mock.patch.object(diagnostics,'_read',side_effect=AssertionError('unexpected state read')):
                records = self.collect(error=error)
            self.assertFalse(any(row['observation'] == 'diagnostic.missing_output_source' for row in records))

    def test_missing_state_is_explicit_and_no_fallback_path_is_guessed(self):
        self.assertEqual(self.targeted(self.collect())['status'],'MISSING')
        record = self.targeted(self.collect(prepared={'setup':{'source':{}}}))
        self.assertEqual(record['status'],'NOT_CONFIGURED')
        self.assertIsNone(record['path'])

    def test_unknown_wrapper_and_absent_test_remain_explicit(self):
        self.write('<state><tests><test Name="C"/></tests></state>')
        record = self.targeted(self.collect())
        self.assertEqual(record['root_child_tags'],['tests'])
        self.assertEqual([row['status'] for row in record['requested_outputs']],['TEST_NOT_FOUND','TEST_NOT_FOUND'])

    def test_unknown_expression_field_and_duplicate_output_are_not_qualified(self):
        self.write('''<state><test Name="C"><field Name="outputList_1"><field Name="name">"gain"</field><field Name="expressionString">"some actual literal"</field></field>
        <field Name="outputList_2"><field Name="name">"gain"</field><field Name="expression">"2"</field></field></test></state>''')
        rows = self.targeted(self.collect())['requested_outputs']
        self.assertEqual(rows[0]['status'],'DUPLICATE_MATCHES')
        self.assertIn('expressionString',rows[0]['matches'][0]['field_names'])
        self.assertEqual(rows[1]['status'],'OUTPUT_NOT_FOUND')

    def test_state_read_counts_toward_existing_read_budget_and_truncated_xml_is_not_parsed(self):
        self.write('<state><test Name="C">' + ' ' * 10000 + '</test></state>')
        with mock.patch.object(diagnostics,'MAX_READ_BYTES',4096):
            records = self.collect()
        targeted = self.targeted(records)
        collection = next(row['payload'] for row in records if row['observation'] == 'diagnostic.collection')
        self.assertEqual(targeted['status'],'TRUNCATED')
        self.assertLessEqual(collection['read_bytes'],4096)
        self.assertNotIn('requested_outputs',targeted)
        self.assertTrue(targeted['identity']['read_truncated'])

    def test_expression_is_never_executed_and_literal_output_is_bounded(self):
        expression = '__import__("os").system("must-never-run")' + 'x' * 5000
        self.write('<state><test Name="C"><field Name="outputList_1"><field Name="name">"gain"</field><field Name="expression">'+expression+'</field></field></test></state>')
        with mock.patch('os.system',side_effect=AssertionError('expression evaluated')):
            record = self.targeted(self.collect())
        summary = record['requested_outputs'][0]['matches'][0]['parsed_expression_literal']
        self.assertTrue(summary['truncated'])
        self.assertLessEqual(len(summary['literal']),1024)
        self.assertIn('must-never-run',summary['literal'])

    def test_targeted_state_walk_obeys_existing_cancel_boundary(self):
        with self.assertRaisesRegex(ValueError,'cancelled or out of existing budget'):
            diagnostics._missing_output_state(b'<state><test Name="C"><field Name="outputList_1"/></test></state>',
                ['C::gain'],stop_check=lambda: 'cancelled')


class RetainedArtifactIndexBoundaryTests(unittest.TestCase):
    def test_empty_directory_tree_still_obeys_deadline(self):
        with tempfile.TemporaryDirectory() as root:
            with mock.patch.object(diagnostics.time,'monotonic',side_effect=[0,3]), \
                 mock.patch.object(diagnostics.os,'walk',return_value=iter([(root,['empty'],[])])):
                index = diagnostics.retained_artifact_index(root)
            self.assertFalse(index['index_complete'])
            self.assertEqual(index['retention_status'],'index_partial_original_files_retained')

    def test_walk_permission_error_is_explicit_partial_index(self):
        with tempfile.TemporaryDirectory() as root:
            denied = os.path.join(root,'denied')
            def walk(path,followlinks=False,onerror=None):
                onerror(PermissionError(13,'permission denied',denied))
                return iter([])
            with mock.patch.object(diagnostics.os,'walk',side_effect=walk):
                index = diagnostics.retained_artifact_index(root)
            self.assertFalse(index['index_complete'])
            self.assertEqual(index['artifacts'][0]['path'],denied)
            self.assertEqual(index['artifacts'][0]['status'],'unreadable_directory')
