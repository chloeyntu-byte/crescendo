# -*- coding: utf-8 -*-
import threading
import time
import unittest
from unittest import mock

from test_diagnostic_ladder import LadderTests
from crescendo_engine.ui_gates import ladder_gate_progress


class LadderProgressTests(LadderTests):
    def test_progress_only_counts_final_stage_including_cleanup(self):
        entered, release = threading.Event(), threading.Event()
        self.addCleanup(release.set)
        def engine(row, name):
            if name == 'private_netlist':
                entered.set()
                release.wait(5)
                self.manager._check(row, 'execution', 'PASS', 'solver done', engine=name)
                self.manager._check(row, 'termination_and_cleanup', 'FAIL', 'late cleanup failure', engine=name)
                return False
            self.manager._check(row, 'execution', 'PASS', 'stage done', engine=name)
            return True
        with mock.patch.object(self.manager, '_engine', side_effect=engine):
            result = self.manager.start('prepared-fixture', self.spec)
            self.assertTrue(entered.wait(10))
            middle = self.manager.progress(result['test_id'])
            self.assertEqual((middle['gate_progress']['passed'], middle['gate_progress']['total']), (1, 3))
            release.set()
            self.assertTrue(self.manager.join(30))
            end = self.manager.progress(result['test_id'])
        self.assertEqual([row['state'] for row in end['case_results']], ['completed', 'failed', 'blocked'])
        self.assertEqual(end['gate_progress']['passed'], 1)

    def test_native_subprogress_does_not_count_and_second_success_is_two(self):
        entered, release = threading.Event(), threading.Event()
        self.addCleanup(release.set)
        observed = []
        def engine(row, name):
            if name == 'maestro_baseline':
                self.manager._log(row, {'observation': 'execution.progress', 'payload': {
                    'gate_progress': {'scope': 'native', 'passed': 5, 'total': 5, 'current': 'native done'}}}, engine=name)
                observed.append(row['ui_progress']['gate_progress']['passed'])
            if name == 'maestro_netlist':
                entered.set()
                release.wait(5)
            self.manager._check(row, 'execution', 'PASS', 'done', engine=name)
            return True
        with mock.patch.object(self.manager, '_engine', side_effect=engine):
            result = self.manager.start('prepared-fixture', self.spec)
            self.assertTrue(entered.wait(10))
            self.assertEqual(self.manager.progress(result['test_id'])['gate_progress']['passed'], 2)
            release.set()
            self.assertTrue(self.manager.join(30))
            self.assertEqual(self.manager.progress(result['test_id'])['gate_progress']['passed'], 3)
        self.assertEqual(observed, [0])

    def test_execution_pass_without_cleanup_proof_does_not_count(self):
        def engine(row, name):
            self.manager._check(row, 'execution', 'PASS', 'solver done', engine=name)
            return False
        with mock.patch.object(self.manager, '_engine', side_effect=engine):
            result = self.manager.start('prepared-fixture', self.spec)
            self.assertTrue(self.manager.join(30))
            progress = self.manager.progress(result['test_id'])
        self.assertEqual(progress['gate_progress']['passed'], 0)
        self.assertEqual(progress['case_results'][0]['state'], 'failed')
        self.assertEqual(progress['case_results'][0]['first_cause']['failure_stage'], 'termination_and_cleanup')

    def test_independent_new_diagnostic_and_blocked_cancelled_do_not_count(self):
        cases = [{'engine': 'maestro_baseline', 'state': 'completed'},
                 {'engine': 'private_netlist', 'state': 'blocked'},
                 {'engine': 'maestro_netlist', 'state': 'cancelled'}]
        self.assertEqual(ladder_gate_progress(cases, 'one')['passed'], 1)
        self.assertEqual(ladder_gate_progress([], 'two')['passed'], 0)
        first = self.run_ladder()
        self.assertEqual(first['gate_progress']['passed'], 3)
        self.behavior['maestro_baseline'] = 'cancel'
        second = self.run_ladder()
        self.assertNotEqual(first['gate_progress']['scope'], second['gate_progress']['scope'])
        self.assertLess(second['gate_progress']['passed'], 3)

    def test_source_guard_registration_failure_is_not_a_stop_failure(self):
        class RegistrationError(Exception):
            def __init__(self, detail):
                self.detail = detail
                Exception.__init__(self, 'source changed')
        for guard in (None, {'ok': False, 'reason': 'changed'}):
            with self.subTest(guard=guard):
                error = RegistrationError({'failure_stage': 'source_freshness', 'source_guard': guard,
                                           'cleanup_proven': True})
                with mock.patch.object(self.manager, '_engine', wraps=self.manager.__class__._engine.__get__(self.manager)), \
                     mock.patch.object(self.manager, '_collect_inline'), \
                     mock.patch.object(self.service, 'start', side_effect=error):
                    result = self.manager.start('prepared-fixture', self.spec)
                    self.assertTrue(self.manager.join(30))
                    progress = self.manager.progress(result['test_id'])
                self.assertEqual(progress['case_results'][0]['first_cause']['failure_stage'], 'source_freshness')
                self.assertNotIn('Stop/cleanup unproven', progress['case_results'][1].get('skip_reason') or '')
                self.assertEqual(progress['gate_progress']['passed'], 0)

    def test_source_guard_object_blocks_later_stages_but_file_input_failure_does_not(self):
        class RegistrationError(Exception):
            def __init__(self, detail):
                self.detail = detail
                Exception.__init__(self, 'prelaunch failure')
        for detail, expected in (({'source_guard': {'ok': False}, 'cleanup_proven': True}, 'blocked'),
                                 ({'failure_stage': 'native_file_inputs', 'cleanup_proven': True}, 'failed')):
            with self.subTest(detail=detail):
                with mock.patch.object(self.manager, '_engine', wraps=self.manager.__class__._engine.__get__(self.manager)), \
                     mock.patch.object(self.manager, '_collect_inline'), \
                     mock.patch.object(self.service, 'start', side_effect=RegistrationError(detail)):
                    result = self.manager.start('prepared-fixture', self.spec)
                    self.assertTrue(self.manager.join(30))
                    progress = self.manager.progress(result['test_id'])
                self.assertEqual(progress['case_results'][1]['state'], expected)
                if expected == 'blocked':
                    self.assertEqual(progress['case_results'][0]['first_cause']['failure_stage'], 'source_guard')
                    self.assertNotIn('Stop/cleanup unproven', progress['case_results'][1]['skip_reason'])


if __name__ == '__main__':
    unittest.main()
