import copy
import unittest
from unittest import mock

from crescendo_engine import environment_probes as probes
from crescendo_engine.environment_test import _candidate_plan_observation
from tests import test_environment_probes as fixture


class CandidatePlanObservationTests(unittest.TestCase):
    def setUp(self):
        self.plan = [dict(generation=0, index=i, values={'x': i}) for i in range(6)]
        self.candidates = [dict(row, candidate_id='g000_c%04d' % row['index']) for row in self.plan]

    def test_aborted_valid_prefix_is_not_tested_and_completion_needs_all(self):
        self.assertEqual(_candidate_plan_observation(self.candidates[:3], self.plan, 'failed'), 'NOT_TESTED')
        self.assertEqual(_candidate_plan_observation(self.candidates[:3], self.plan, 'stopped'), 'NOT_TESTED')
        self.assertEqual(_candidate_plan_observation(self.candidates[:3], self.plan, 'completed'), 'FAIL')
        self.assertEqual(_candidate_plan_observation(self.candidates, self.plan, 'completed'), 'PASS')

    def test_mismatch_duplicate_gap_and_extra_are_failures_even_if_aborted(self):
        changed = copy.deepcopy(self.candidates[:3]); changed[1]['values']['x'] = 99
        for rows in (changed, [self.candidates[0], self.candidates[0]],
                     [self.candidates[0], self.candidates[2]], self.candidates + self.candidates[:1]):
            self.assertEqual(_candidate_plan_observation(rows, self.plan, 'failed'), 'FAIL')


class SchedulerObservationTests(unittest.TestCase):
    setUp = fixture.EnvironmentProbeTests.setUp
    tearDown = fixture.EnvironmentProbeTests.tearDown
    executable = fixture.EnvironmentProbeTests.executable
    command_policy = fixture.EnvironmentProbeTests.command_policy
    def test_unavailable_exact_query_is_warning_and_retains_evidence(self):
        self.executable('batch-submit', "print('Usage: batch-submit [options]')")
        self.executable('batch-query', "import sys\nif sys.argv[1:]==['--help']: print('Usage: batch-query JOBID')\nelse: print('temporary unavailable'); sys.exit(1)")
        self.command_policy()
        checks = probes.query_scheduler_jobs(self.config, self.prepared, self.root, ['21'])
        row = next(item for item in checks if item['id'] == 'scheduler.job.21')
        self.assertEqual(row['status'], 'WARN')
        self.assertEqual(row['details']['query_returncode'], 1)
        self.assertIn('temporary unavailable', row['details']['command']['stdout_tail'])
        self.assertFalse(row['details']['native_completion_authority'])
        self.assertFalse(row['details']['scheduler_observation']['terminal'])

    def test_empty_success_is_unknown_warning(self):
        self.executable('batch-submit', "print('Usage: batch-submit [options]')")
        self.executable('batch-query', "import sys\nif sys.argv[1:]==['--help']: print('Usage: batch-query JOBID')")
        self.command_policy()
        checks = probes.query_scheduler_jobs(self.config, self.prepared, self.root, ['21'])
        row = next(item for item in checks if item['id'] == 'scheduler.job.21')
        self.assertEqual(row['status'], 'WARN')
        self.assertIn('UNKNOWN', row['message'])
        self.assertFalse(row['details']['native_completion_authority'])

    def test_reported_failed_job_remains_visible_not_completion_proof(self):
        self.executable('batch-submit', "print('Usage: batch-submit [options]')")
        self.executable('batch-query', "import sys\nif sys.argv[1:]==['--help']: print('Usage: batch-query JOBID')\nelse: print('JOB_ID 21\\nJOB_STATE FAILED\\nEXIT_CODE 2')")
        self.command_policy()
        checks = probes.query_scheduler_jobs(self.config, self.prepared, self.root, ['21'])
        row = next(item for item in checks if item['id'] == 'scheduler.job.21')
        observed = row['details']['scheduler_observation']
        self.assertEqual(observed['state'], 'FAILED')
        self.assertEqual(observed['job_exit_code'], 2)
        self.assertFalse(row['details']['native_completion_authority'])
