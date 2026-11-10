from __future__ import print_function

import errno
import json
import os
import shutil
import tempfile
import unittest
from unittest import mock

from crescendo_engine import scratch


class ScratchLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.parent = tempfile.mkdtemp(prefix="Crescendo scratch test ' $")
        self.addCleanup(shutil.rmtree, self.parent, True)
        self.path = tempfile.mkdtemp(prefix='probe_', dir=self.parent)

    def body(self, name='hierarchy.json', value='selected module metadata'):
        path = os.path.join(self.path, name)
        with open(path, 'w') as stream:
            stream.write(value)
        return path

    def test_owner_death_is_not_a_native_producer_stop_proof(self):
        receipt = scratch.register_scratch(self.path)
        self.body()
        with mock.patch.object(scratch, '_process_start', return_value=None):
            result = scratch.cleanup_scratch(self.path)
        self.assertFalse(result['ok'])
        self.assertEqual(result['state'], 'unproven')
        self.assertTrue(os.path.exists(self.path))
        self.assertEqual(scratch.reap_sealed_scratch(self.parent), [])

    def test_ended_producers_allow_exact_cleanup_and_repeat_is_absent(self):
        receipt = scratch.register_scratch(self.path)
        self.body()
        result = scratch.cleanup_scratch(self.path, receipt['token'], producers_stopped=True)
        self.assertTrue(result['ok'])
        self.assertFalse(os.path.exists(self.path))
        self.assertEqual(scratch.cleanup_scratch(self.path)['state'], 'absent')

    def test_wrong_token_and_other_live_ui_are_preserved(self):
        receipt = scratch.register_scratch(self.path)
        self.body()
        result = scratch.cleanup_scratch(self.path, 'f' * 32, producers_stopped=True)
        self.assertFalse(result['ok'])
        self.assertTrue(os.path.exists(self.path))
        second = tempfile.mkdtemp(prefix='browse_', dir=self.parent)
        other = scratch.register_scratch(second)
        self.assertTrue(scratch.cleanup_scratch(self.path, receipt['token'], True)['ok'])
        self.assertTrue(os.path.isdir(second))
        self.assertTrue(scratch.cleanup_scratch(second, other['token'], True)['ok'])

    def test_source_symlink_and_hardlink_reject_cleanup_without_touching_source(self):
        receipt = scratch.register_scratch(self.path)
        source = os.path.join(self.parent, 'user_schematic.oa')
        with open(source, 'w') as stream:
            stream.write('protected source')
        for link in ('symlink', 'hardlink'):
            target = os.path.join(self.path, link)
            if link == 'symlink':
                os.symlink(source, target)
            else:
                os.link(source, target)
            result = scratch.cleanup_scratch(self.path, receipt['token'], True)
            self.assertFalse(result['ok'])
            with open(source) as stream:
                self.assertEqual(stream.read(), 'protected source')
            os.unlink(target)
        self.assertTrue(scratch.cleanup_scratch(self.path, receipt['token'], True)['ok'])

    def test_directory_replacement_and_non_temporary_root_are_rejected(self):
        receipt = scratch.register_scratch(self.path)
        original = self.path + '.original'
        os.rename(self.path, original)
        os.mkdir(self.path, 0o700)
        shutil.copyfile(os.path.join(original, scratch.OWNER_FILE), os.path.join(self.path, scratch.OWNER_FILE))
        self.assertFalse(scratch.cleanup_scratch(self.path, receipt['token'], True)['ok'])
        ordinary = os.path.join(self.parent, 'states')
        os.mkdir(ordinary, 0o700)
        with self.assertRaises(scratch.ScratchSafetyError):
            scratch.register_scratch(ordinary)

    def test_sealed_dead_bootstrap_is_reclaimed_but_active_sealed_owner_is_kept(self):
        receipt = scratch.register_scratch(self.path, 'bootstrap')
        self.body('launch.log')
        scratch.seal_scratch(self.path, receipt['token'])
        self.assertFalse(scratch.cleanup_scratch(self.path)['ok'])
        with mock.patch.object(scratch, '_process_start', return_value=None):
            results = scratch.reap_sealed_scratch(self.parent)
        self.assertEqual(len(results), 1)
        self.assertTrue(results[0]['ok'])

    def test_quota_and_busy_cleanup_preserve_accurate_retryable_receipt(self):
        receipt = scratch.register_scratch(self.path)
        self.body()
        with mock.patch.object(scratch.os, 'rmdir', side_effect=OSError(errno.ENOTEMPTY, 'directory not empty')):
            result = scratch.cleanup_scratch(self.path, receipt['token'], True)
        self.assertFalse(result['ok'])
        self.assertEqual(result['state'], 'artifact_cleanup_failed')
        self.assertTrue(os.path.isfile(os.path.join(self.path, scratch.OWNER_FILE)))
        self.assertTrue(scratch.cleanup_scratch(self.path, receipt['token'], True)['ok'])
        path = tempfile.mkdtemp(prefix='cr_tmp_', dir=self.parent)
        with mock.patch.object(scratch.os, 'open', side_effect=OSError(errno.EDQUOT, 'quota exceeded')):
            with self.assertRaises(OSError) as caught:
                scratch.register_scratch(path)
        self.assertEqual(caught.exception.errno, errno.EDQUOT)

    def test_budget_is_bounded_and_never_deletes_unknown_or_active_data(self):
        receipt = scratch.register_scratch(self.path)
        self.body(value='x' * 4096)
        unknown = os.path.join(self.parent, 'other_tool_large_result')
        os.mkdir(unknown)
        with self.assertRaises(scratch.ScratchSafetyError):
            scratch.enforce_scratch_budget(self.parent, budget_bytes=4096)
        self.assertTrue(os.path.isdir(unknown))
        self.assertTrue(os.path.isfile(os.path.join(self.path, 'hierarchy.json')))
        with self.assertRaises(scratch.ScratchSafetyError):
            scratch.enforce_scratch_budget(self.parent, max_files=1)
        usage = scratch.enforce_scratch_budget(self.parent)
        self.assertEqual(usage['directories'], 1)
        self.assertGreater(usage['bytes'], 4096)


if __name__ == '__main__':
    unittest.main()
