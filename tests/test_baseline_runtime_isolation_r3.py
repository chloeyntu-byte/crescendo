"""Baseline Ocean must allocate its own Cadence MPS session."""
import json
import os
import subprocess
import tempfile
import unittest
from unittest import mock

from crescendo_engine.native_baseline import _NativeSession


class BaselineRuntimeIsolationTests(unittest.TestCase):
    def test_guardian_receives_private_session_and_preserved_native_environment(self):
        with tempfile.TemporaryDirectory() as root:
            config = {'cadence': {'ocean_bin': '/cadence/ocean'}, 'runtime': {}}
            driver = _NativeSession(root, {'root': root}, config, 'one')
            captured = {}
            def intercept(*args, **kwargs):
                captured.update(kwargs['env'])
                raise RuntimeError('captured before process launch')
            with mock.patch.dict(os.environ, {'CDS_MPS_SESSION': 'virtuoso123',
                                             'CDS_LIC_FILE': 'site-license',
                                             'SITE_SCHEDULER_FLAG': 'preserved'}), \
                    mock.patch('subprocess.Popen', side_effect=intercept):
                with self.assertRaisesRegex(RuntimeError, 'captured before process launch'):
                    driver.launch('/unused/startup.il')
                self.assertEqual(os.environ['CDS_MPS_SESSION'], 'virtuoso123')
            self.assertNotIn('CDS_MPS_SESSION', captured)
            self.assertEqual(captured['CDS_LIC_FILE'], 'site-license')
            self.assertEqual(captured['SITE_SCHEDULER_FLAG'], 'preserved')

    def test_owned_command_observer_restores_scheduler_home_and_argv(self):
        from crescendo_engine.job_policy import prepare_job_command_observers
        with tempfile.TemporaryDirectory() as root:
            shim = os.path.join(root, 'scheduler')
            log = os.path.join(root, 'dispatch.jsonl')
            with open(shim, 'w') as f:
                f.write('#!/bin/sh\n[ "$HOME" = "$EXPECTED_HOME" ] || exit 91\nprintf "%s\\n" "$@"\n')
            os.chmod(shim, 0o700)
            props = {'distributionmethod': 'Command', 'jobsubmitcommand': shim + ' -q short'}
            policy = {'available': True, 'global': {'available': True, 'properties': props}, 'tests': []}
            observation = prepare_job_command_observers(policy, {}, os.path.join(root, 'observers'), log)
            wrapper = observation['wrappers'][0]['observer_script']
            env = dict(os.environ, HOME=os.path.join(root, 'private'), EXPECTED_HOME=root,
                       CRESCENDO_SCHEDULER_HOME_SET='x', CRESCENDO_SCHEDULER_HOME_VALUE=root)
            direct = subprocess.run([shim, '-q', 'short', 'worker argument'], env=env,
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            wrapped = subprocess.run([wrapper, 'worker argument'], env=env,
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            self.assertEqual(direct.returncode, 91)
            self.assertEqual(wrapped.returncode, 0, wrapped.stderr)
            self.assertEqual(wrapped.stdout.decode().splitlines(), ['-q', 'short', 'worker argument'])
            with open(log) as f:
                events = [json.loads(line) for line in f]
            self.assertEqual(events[0]['event'], 'submit_start')
            self.assertEqual(events[-1]['event'], 'submit_end')
            self.assertEqual(events[0]['argv'], [shim, '-q', 'short', 'worker argument'])
            self.assertTrue(events[0]['scheduler_context']['home_changed_from_ocean'])


if __name__ == '__main__':
    unittest.main()
