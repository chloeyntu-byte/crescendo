import unittest
from crescendo_engine.diagnosis_catalog import describe_check, stage_section


class CompletedCornerDiagnostics(unittest.TestCase):
    def test_completed_point_identity_does_not_claim_solver_was_not_run(self):
        primary = {'failure_stage': 'native_corner_coverage',
                   'phase': 'completed_native_point_identity', 'execution_started': True,
                   'message': 'native Maestro corner/point coverage differs: model identity mismatch'}
        check = {'id': 'execution', 'status': 'FAIL', 'engine': 'maestro_baseline'}
        row = describe_check(check, {'first_cause': primary})
        self.assertEqual(stage_section(primary), 'results')
        self.assertNotIn('尚未求解', row['summary'] + row['explanation'])
        self.assertIn('已进入原生结果核验', row['summary'])
        self.assertEqual(row['component']['file'], 'crescendo_engine/completion.py')

    def test_capture_failure_keeps_pre_solver_meaning(self):
        primary = {'failure_stage': 'native_corner_coverage', 'execution_started': False,
                   'message': 'native capture point coverage differs'}
        row = describe_check({'id': 'execution', 'status': 'FAIL'}, {'first_cause': primary})
        self.assertEqual(stage_section(primary), 'netlist')
        self.assertIn('尚未求解', row['summary'])


if __name__ == '__main__':
    unittest.main()
