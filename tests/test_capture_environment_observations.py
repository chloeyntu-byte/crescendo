"""Environment observations preserve native literals without setting options."""
import json
import os
import tempfile
import unittest

from crescendo_engine.private_netlist import (
    _capture_environment_observation_skill, _capture_setup_readback,
)


class CaptureEnvironmentObservationTests(unittest.TestCase):
    def test_generated_optional_getter_is_bounded_read_only_and_restores_global_error_status(self):
        script = '\n'.join(_capture_environment_observation_skill(['tb"quoted']))
        self.assertIn('maeGetEnvOption(test ?includeEmpty t ?session session)', script)
        self.assertIn('length(options)<=256', script)
        self.assertIn('matched<8', script)
        self.assertIn('strlen(literal)>2048', script)
        self.assertIn('rexMatchp(".*alps.*" lowerCase(optionName))', script)
        self.assertIn('crescObserveCaptureEnvironment("tb\\"quoted")', script)
        self.assertLess(script.index('sstatus(errsetTrace nil)'), script.index('maeGetEnvOption('))
        self.assertLess(script.index('sstatus(stacktrace 0)'), script.index('maeGetEnvOption('))
        self.assertIn('sstatus(stacktrace savedStack) sstatus(errsetTrace savedTrace)', script)
        for forbidden in ('maeSetEnvOption', 'maeSave', 'maeRun', 'eval(', 'readstring('):
            self.assertNotIn(forbidden, script)

    def test_existing_ENV_protocol_retains_empty_boolean_timeout_and_real_vendor_key_literals(self):
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, 'capture_policy_mode.tsv')
            fields = [('autoDisplay', 't'), ('userCmdLineOption', '""'),
                      ('licQueueTimeOut', '"90000"'), ('VendorALPSCS', 'nil')]
            with open(path, 'w') as stream:
                for name, literal in fields + [('__CRESCENDO_ENV_DIAGNOSTIC_STATUS__', 'OBSERVED')]:
                    stream.write('ENV\t' + '\t'.join(json.dumps(value) for value in
                        ('tb', name, 'maeGetEnvOption.diagnostic')) + '\tt\t' + json.dumps(literal) + '\n')
            report = _capture_setup_readback({'records_path': path, 'final_policy_names': {},
                'expected_policy_readback': {}}, [])
        self.assertEqual(report['readback_status'], 'collected')
        self.assertTrue(report['policy_confirmed'])
        self.assertTrue(report['mode_confirmed'])
        self.assertEqual(report['environment_option_observations'], [{'test': 'tb', 'status': 'OBSERVED',
            'fields': [{'name': name, 'literal': literal} for name, literal in fields]}])

    def test_unavailable_observation_stays_unknown_and_does_not_change_policy_confirmation(self):
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, 'records')
            with open(path, 'w') as stream:
                stream.write('ENV\t"tb"\t"__CRESCENDO_ENV_DIAGNOSTIC_STATUS__"\t'
                             '"maeGetEnvOption.diagnostic"\tt\t"UNAVAILABLE"\n')
            report = _capture_setup_readback({'records_path': path, 'final_policy_names': {},
                'expected_policy_readback': {}}, [])
        self.assertEqual(report['environment_option_observations'],
                         [{'test': 'tb', 'status': 'UNAVAILABLE', 'fields': []}])
        self.assertTrue(report['policy_confirmed'])
        self.assertTrue(report['mode_confirmed'])


if __name__ == '__main__':
    unittest.main()
