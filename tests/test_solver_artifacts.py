"""Actual Scheduler TASK_INFO shape and owned ALPS companion log preservation."""
import json
import os
import tempfile
import time
import unittest

from crescendo_engine.solver_artifacts import collect_solver_artifacts, log_context, parse_scheduler_task_log


SCHEDULER = '''TASK_INFO:
    JOB_ID:         10000001
    JOB_NAME:       probe_alps
    TASK_NAME:      rg0.0
    REPLICA:        1
    TASKGROUP_NAME: rg0
    SUBMIT_NODE:    submit01
    EXEC_NODE:      node01
    EXIT_CODE:      0
    EXIT_MESSAGE:   Job execution succeeded

RESOURCE_USAGE_SUMMARY:
    REQ_CPU:              8
    REQ_MEM:              8000
    USER_CPU_TIME:        5
    SYSTEM_CPU_TIME:      0
    MEMSW_MAX:            94
    SWAP_MAX:             0
    MEM_MAX:              94
    MEM_AVG:              0
    MRUN_TIME:            6
    CPU_UTIL_AVG(%):      92
    CPU_UTIL_RECENT(%):   92
'''


class SolverArtifactsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = self.temp.name
        self.epoch = time.time() - 1
        point = os.path.join(self.root, 'tasks', 'main_TT_1')
        self.task = {'task_id': 'main_TT_1', 'test': 'main', 'corner': 'TT', 'point_id': 1,
            'execution_simulator': 'alps', 'psf_dir': os.path.join(point, 'psf'),
            'simulator_log': os.path.join(point, 'psf', 'alps.out')}

    def tearDown(self):
        self.temp.cleanup()

    def write(self, path, text):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w', encoding='utf-8') as stream:
            stream.write(text)
        return path

    def snapshot(self, **options):
        return collect_solver_artifacts(self.task, self.root, self.epoch, **options)

    def test_exact_success_log_indexes_remote_node_cpu_memory_and_exit_without_authority(self):
        parsed = parse_scheduler_task_log(SCHEDULER, '10000001')
        self.assertTrue(parsed['valid'])
        self.assertEqual(parsed['job_id'], '10000001')
        self.assertEqual(parsed['records'][0]['submit_node'], 'submit01')
        self.assertEqual(parsed['records'][0]['exec_node'], 'node01')
        self.assertEqual(parsed['records'][0]['exit_code'], 0)
        self.assertEqual(parsed['resource_usage']['req_cpu'], 8)
        self.assertEqual(parsed['resource_usage']['req_mem'], 8000)
        self.assertTrue(parsed['scheduler_task_exit_all_zero'])
        self.assertFalse(parsed['native_completion_authority'])
        self.assertFalse(parsed['remote_stop_authority'])

    def test_malformed_duplicate_missing_and_foreign_records_are_not_qualified(self):
        cases = [SCHEDULER.replace('10000001', '10000001x'),
            SCHEDULER.replace('    JOB_ID:', '    JOB_ID: 10000001\n    JOB_ID:'),
            SCHEDULER.replace('    JOB_ID:         10000001\n', ''),
            SCHEDULER.replace('    EXIT_CODE:      0\n', ''),
            SCHEDULER.replace('    REQ_CPU:              8', '    REQ_CPU:              bad')]
        for text in cases:
            with self.subTest(text=text[:100]):
                self.assertFalse(parse_scheduler_task_log(text, '10000001')['valid'])
        self.assertFalse(parse_scheduler_task_log(SCHEDULER, '10000002')['valid'])
        self.assertFalse(parse_scheduler_task_log(SCHEDULER, ';kill all')['valid'])
        self.assertFalse(parse_scheduler_task_log('Job submitted successfully', '10000001')['valid'])

    def test_appended_foreign_failure_never_hides_exact_owned_cancellation(self):
        owned = SCHEDULER.replace('10000001', '10000004').replace('node01', 'node03').replace(
            'EXIT_CODE:      0', 'EXIT_CODE:      10003').replace('Job execution succeeded', 'Job was terminated by user')
        foreign = 'ie: Command not found.\n' + SCHEDULER.replace('10000001', '10000003').replace(
            'EXIT_CODE:      0', 'EXIT_CODE:      10001').replace('REQ_CPU:              8', 'REQ_CPU:              1')
        for text in (foreign + owned, owned + foreign):
            with self.subTest(order=text[:60]):
                parsed = parse_scheduler_task_log(text, '10000004')
                self.assertTrue(parsed['valid'])
                self.assertEqual(parsed['job_id'], '10000004')
                self.assertEqual(parsed['observed_job_ids'], ['10000003', '10000004'])
                self.assertEqual(parsed['ignored_job_ids'], ['10000003'])
                self.assertEqual(parsed['foreign_record_count'], 1)
                self.assertEqual(len(parsed['records']), 1)
                self.assertEqual(parsed['records'][0]['exit_code'], 10003)
                self.assertEqual(parsed['resource_usage']['req_cpu'], 8)
                self.assertFalse(parsed['remote_stop_authority'])

    def test_foreign_success_cannot_override_owned_failure_or_supply_missing_fields(self):
        foreign = SCHEDULER.replace('10000001', '10000002')
        failed = SCHEDULER.replace('EXIT_CODE:      0', 'EXIT_CODE:      2')
        parsed = parse_scheduler_task_log(failed + foreign, '10000001')
        self.assertTrue(parsed['valid'])
        self.assertFalse(parsed['scheduler_task_exit_all_zero'])
        missing = failed.replace('    EXIT_CODE:      2\n', '')
        self.assertFalse(parse_scheduler_task_log(missing + foreign, '10000001')['valid'])
        self.assertEqual(parse_scheduler_task_log(foreign, '10000001')['records'], [])

    def test_multiple_owned_tasks_have_separate_usage_and_ambiguous_repeats_fail_closed(self):
        second = SCHEDULER.replace('rg0.0', 'rg0.1').replace('REQ_CPU:              8', 'REQ_CPU:              4')
        parsed = parse_scheduler_task_log(SCHEDULER + second, '10000001')
        self.assertTrue(parsed['valid'])
        self.assertEqual(len(parsed['records']), 2)
        self.assertNotIn('req_cpu', parsed['resource_usage'])
        self.assertEqual([row['values']['req_cpu'] for row in parsed['resource_usage_records']], [8, 4])
        self.assertFalse(parse_scheduler_task_log(SCHEDULER + SCHEDULER, '10000001')['valid'])
        self.assertFalse(parse_scheduler_task_log(SCHEDULER + SCHEDULER.replace('EXIT_CODE:      0', 'EXIT_CODE:      2'), '10000001')['valid'])

    def test_scheduler_context_contains_current_footer_without_old_unbound_error(self):
        owned = SCHEDULER.replace('10000001', '10000004').replace('EXIT_CODE:      0', 'EXIT_CODE:      10003')
        foreign = 'ie: Command not found.\n' + SCHEDULER.replace('10000001', '10000003')
        out = self.write(os.path.join(self.root, 'batch', 'append.out'), foreign + owned)
        row = self.snapshot(job_id='10000004', scheduler_stdout=out)['artifacts'][-1]
        self.assertTrue(row['scheduler_task_observation']['valid'])
        lines = '\n'.join(item['text'] for item in row['context']['lines'])
        self.assertIn('10000004', lines)
        self.assertIn('10003', lines)
        self.assertNotIn('10000003', lines)
        self.assertNotIn('Command not found', lines)
        self.assertFalse(row['context']['unbound_text_attributed'])

    def test_failed_task_record_keeps_native_exit_as_separate_scheduler_observation(self):
        parsed = parse_scheduler_task_log(SCHEDULER.replace('EXIT_CODE:      0', 'EXIT_CODE:      2'), '10000001')
        self.assertTrue(parsed['valid'])
        self.assertEqual(parsed['records'][0]['exit_code'], 2)
        self.assertFalse(parsed['scheduler_task_exit_all_zero'])

    def test_many_scheduler_records_cannot_evict_first_native_error_context(self):
        context = log_context(SCHEDULER * 16 + 'FATAL: first native failure\n' + 'tail\n' * 100)
        self.assertIn('first native failure', json.dumps(context))
        self.assertIn('EXEC_NODE', json.dumps(context))
        self.assertLessEqual(len(context['lines']), 80)
        self.assertTrue(context['context_truncated'])

    def test_warning_taxonomy_preserves_physical_options_and_observed_suppression(self):
        context = log_context('WARNING: ignored sensfile\nWARNING: ignored checklimitdest\n'
            'WARNING: no DC path to ground\nWARNING: no dc value, PORT0 resets\n'
            'WARNING: remaining warnings suppressed due to maxwarnstologfile\n')
        taxonomy = context['warning_taxonomy']
        self.assertEqual(taxonomy['categories']['ignored_solver_option']['count'], 2)
        self.assertEqual(taxonomy['categories']['physical_circuit']['count'], 2)
        self.assertTrue(taxonomy['warning_suppression_observed'])
        self.assertIsNone(taxonomy['suppressed_warning_count'])
        self.assertEqual(taxonomy['warning_line_count'], 5)
        self.assertIn('no DC path', json.dumps(context))

    def test_owned_fresh_alps_log_and_warning_are_captured_before_raw_files_disappear(self):
        self.write(self.task['psf_dir'] + '.log', 'ALPS completed\n')
        self.write(self.task['psf_dir'] + '.warn', 'header\n' + 'middle\n' * 40 + 'WARNING: ignored model parameter\n' + 'tail\n' * 30)
        out = self.write(os.path.join(self.root, 'batch', 'alps.out'), SCHEDULER + 'statistics\n' * 100)
        snapshot = self.snapshot(job_id='10000001', scheduler_stdout=out)
        rows = {row['kind']: row for row in snapshot['artifacts']}
        self.assertEqual(rows['alps_psf_log']['status'], 'RECORDED')
        self.assertIn('ignored model parameter', json.dumps(rows['alps_psf_warning']['context']))
        scheduler = rows['scheduler_stdout']
        self.assertTrue(scheduler['scheduler_task_observation']['valid'])
        self.assertIn('node01', json.dumps(scheduler['context']))
        self.assertIn('EXIT_CODE', json.dumps(scheduler['context']))
        os.unlink(self.task['psf_dir'] + '.log')
        self.assertIn('ALPS completed', json.dumps(snapshot))
        self.assertEqual(snapshot['task_id'], 'main_TT_1')
        self.assertEqual(snapshot['scheduler_job_id'], '10000001')

    def test_owned_alps_internal_companion_alias_records_target_identity_and_warning_hints(self):
        target = self.write(os.path.join(self.task['psf_dir'], 'alps.log'), 'ALPS completed\n')
        warning = self.write(os.path.join(self.task['psf_dir'], 'alps.warn'),
            'WARNING: unsupported info what=output\nWARNING: unsupported pivrel\nWARNING: unsupported annotate\n'
            + 'WARNING: PDK redefinedparams\n' * 5)
        os.symlink('psf/alps.log', self.task['psf_dir'] + '.log')
        os.symlink(warning, self.task['psf_dir'] + '.warn')
        rows = {row['kind']: row for row in self.snapshot()['artifacts']}
        self.assertEqual(rows['alps_psf_log']['status'], 'RECORDED')
        self.assertTrue(rows['alps_psf_log']['leaf_link'])
        self.assertEqual(rows['alps_psf_log']['resolved_path'], os.path.relpath(target, self.root))
        taxonomy = rows['alps_psf_warning']['context']['warning_taxonomy']
        self.assertEqual(taxonomy['warning_line_count'], 8)
        self.assertEqual(taxonomy['categories']['pdk_parameter_redefinition']['count'], 5)
        self.assertEqual(taxonomy['categories']['pdk_parameter_redefinition']['additional_examples_omitted'], 3)
        self.assertIn('numerical_option', taxonomy['categories'])
        self.assertIn('unsupported_metadata', taxonomy['categories'])
        self.assertIn('annotation_or_display', taxonomy['categories'])
        self.assertFalse(taxonomy['completion_authority'])

    def test_external_symlink_ancestor_hardlink_and_stale_logs_are_indexed_without_read(self):
        with tempfile.TemporaryDirectory() as outside:
            external = self.write(os.path.join(outside, 'solver.log'), 'DO_NOT_READ_EXTERNAL')
            linked = os.path.join(self.root, 'linked')
            os.symlink(outside, linked)
            rejected = self.snapshot(scheduler_stdout=os.path.join(linked, 'solver.log'))
            row = next(row for row in rejected['artifacts'] if row['kind'] == 'scheduler_stdout')
            self.assertEqual(row['status'], 'REJECTED')
            self.assertNotIn('DO_NOT_READ_EXTERNAL', json.dumps(rejected))
            rejected = self.snapshot(scheduler_stdout=external)
            self.assertEqual(rejected['artifacts'][-1]['status'], 'REJECTED')
            hard = os.path.join(self.root, 'hard.log')
            os.link(external, hard)
            rejected = self.snapshot(scheduler_stdout=hard)
            self.assertEqual(rejected['artifacts'][-1]['reason'], 'linked_or_nonregular_log')
        stale = self.write(self.task['psf_dir'] + '.warn', 'DO_NOT_READ_STALE')
        os.utime(stale, (self.epoch - 10, self.epoch - 10))
        rejected = self.snapshot()
        row = next(row for row in rejected['artifacts'] if row['kind'] == 'alps_psf_warning')
        self.assertEqual(row['reason'], 'stale_log')
        self.assertFalse(row['fresh'])
        self.assertNotIn('DO_NOT_READ_STALE', json.dumps(rejected))

    def test_bounded_partial_scheduler_log_keeps_context_but_never_certifies_all_job_records(self):
        out = self.write(os.path.join(self.root, 'batch', 'out'), SCHEDULER + 'padding\n' * 20000)
        row = self.snapshot(job_id='10000001', scheduler_stdout=out)['artifacts'][-1]
        self.assertTrue(row['read_truncated'])
        self.assertIn('node01', json.dumps(row['context']))
        self.assertFalse(row['scheduler_task_observation']['valid'])
        self.assertEqual(row['scheduler_task_observation']['reason'], 'partial_or_changing_scheduler_log')


if __name__ == '__main__':
    unittest.main()
