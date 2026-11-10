"""Moved installation accepts only identical helper with durable stopped identity."""
import json
import os
import shutil
import sys
import tempfile
import unittest
from unittest import mock

from crescendo_engine import legacy_guardian as guard
from crescendo_engine.netlist_measurement import run_owned_command


class MovedGuardianTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='crescendo-guard-moved-')
        self.addCleanup(self.temp.cleanup)
        self.root = self.temp.name
        self.workspace = os.path.join(self.root, 'workspace')
        self.attempt = os.path.join(self.root, 'attempt')
        os.makedirs(self.workspace)
        self.original = guard.guardian_script_path()
        self.new_helper = os.path.join(self.root, 'new-install-guardian.py')
        shutil.copyfile(self.original, self.new_helper)
        result = run_owned_command([sys.executable, '-c', 'print("ok")'], self.workspace,
            os.environ.copy(), 10, self.attempt, 'moved-install-test')
        self.assertEqual(result['returncode'], 0)
        self.assertTrue(result['guardian_cleanup']['state'] == 'dead')

    def inspect(self):
        with mock.patch.object(guard, 'guardian_script_path', return_value=self.new_helper):
            return guard.inspect_legacy_guardian_attempt(self.attempt, self.workspace)

    def test_real_terminal_guardian_survives_install_move(self):
        result = self.inspect()
        self.assertEqual(result['state'], 'dead', result)
        self.assertTrue(result['termination_proof']['ok'])

    def test_changed_or_missing_helper_cannot_qualify(self):
        with open(self.new_helper, 'a') as stream:
            stream.write('\n# different implementation\n')
        self.assertEqual(self.inspect()['state'], 'ambiguous')
        os.unlink(self.new_helper)
        self.assertEqual(self.inspect()['state'], 'ambiguous')

    def test_missing_or_nonterminal_state_is_never_accepted(self):
        state = os.path.join(self.attempt, 'guardian_state.json')
        with open(state) as stream:
            saved = json.load(stream)
        altered = dict(saved, status='running')
        with open(state, 'w') as stream:
            json.dump(altered, stream)
        self.assertEqual(self.inspect()['state'], 'ambiguous')
        os.unlink(state)
        self.assertEqual(self.inspect()['state'], 'ambiguous')

    def test_active_owned_process_never_signalled_by_moved_inspection(self):
        with mock.patch.object(guard, 'guardian_script_path', return_value=self.new_helper), \
                mock.patch.object(guard, '_process_snapshot', return_value={'alive': True, 'pid': 123}), \
                mock.patch.object(guard, '_guardian_identity_matches', return_value=True), \
                mock.patch.object(guard, '_ocean_identity_matches', return_value=True), \
                mock.patch.object(guard.os, 'kill') as kill:
            result = guard.quiesce_legacy_guardian_attempt(self.attempt, self.workspace,
                                                          timeout_seconds=1, terminate_owned=True)
        self.assertEqual(result['state'], 'ambiguous')
        self.assertFalse(result['termination_proof']['ok'])
        kill.assert_not_called()

    def test_boot_or_uid_mismatch_is_rejected(self):
        path = os.path.join(self.attempt, 'guardian_state.json')
        with open(path) as stream:
            state = json.load(stream)
        state['launch_boot_id'] = 'foreign-boot'
        with open(path, 'w') as stream:
            json.dump(state, stream)
        self.assertEqual(self.inspect()['state'], 'ambiguous')

        manifest_path = os.path.join(self.attempt, 'legacy_guardian_launch.json')
        with open(manifest_path) as stream:
            manifest = json.load(stream)
        state['launch_boot_id'] = manifest['expected_parent_boot_id']
        with open(path, 'w') as stream:
            json.dump(state, stream)
        manifest['expected_uid'] = os.getuid() + 1
        with open(manifest_path, 'w') as stream:
            json.dump(manifest, stream)
        self.assertEqual(self.inspect()['state'], 'ambiguous')


if __name__ == '__main__':
    unittest.main()
