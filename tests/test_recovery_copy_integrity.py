import json
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine import same_library_views as views
from crescendo_engine import setup_private_session as session
from crescendo_engine import workspace


class RecoveryCopyIntegrityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.source = os.path.join(self.temp.name, 'source')
        self.target = os.path.join(self.temp.name, 'target')
        self.receipt = os.path.join(self.temp.name, 'move.json')
        os.mkdir(self.source)
        # These files are deliberately excluded by tree_content_fingerprint.
        for name, data in (('active.state', b'original state'),
                           ('settings.bak', b'original backup'),
                           ('maestro.sdb', b'data')):
            with open(os.path.join(self.source, name), 'wb') as stream:
                stream.write(data)
        with mock.patch.object(views, '_rename_noreplace',
                               side_effect=views.RenameNoReplaceUnavailable('unsupported')):
            with mock.patch.object(workspace, '_make_private_tree_owner_writable',
                                   side_effect=OSError('injected interruption')):
                with self.assertRaisesRegex(OSError, 'injected interruption'):
                    session._recovery_move(self.source, self.target, self.receipt)
        with open(self.receipt) as stream:
            self.assertEqual('copied', json.load(stream)['state'])
        self.assertTrue(os.path.isdir(self.source))
        self.assertTrue(os.path.isdir(self.target))

    def test_modified_active_state_is_rejected(self):
        with open(os.path.join(self.target, 'active.state'), 'wb') as stream:
            stream.write(b'changed')
        self.assert_changed_target_is_rejected()

    def test_deleted_backup_is_rejected(self):
        os.unlink(os.path.join(self.target, 'settings.bak'))
        self.assert_changed_target_is_rejected()

    def test_changed_mode_is_rejected(self):
        path = os.path.join(self.target, 'maestro.sdb')
        os.chmod(path, os.stat(path).st_mode ^ 0o100)
        self.assert_changed_target_is_rejected()

    def assert_changed_target_is_rejected(self):
        with self.assertRaisesRegex(ValueError, 'Completed recovery copy changed'):
            session._recovery_move(self.source, self.target, self.receipt)
        self.assertTrue(os.path.isdir(self.source))
        with open(os.path.join(self.source, 'active.state'), 'rb') as stream:
            self.assertEqual(b'original state', stream.read())
        with open(os.path.join(self.source, 'settings.bak'), 'rb') as stream:
            self.assertEqual(b'original backup', stream.read())
        with open(self.receipt) as stream:
            self.assertEqual('copied', json.load(stream)['state'])

    def test_unchanged_copy_completes(self):
        session._recovery_move(self.source, self.target, self.receipt)
        self.assertFalse(os.path.lexists(self.source))
        with open(os.path.join(self.target, 'active.state'), 'rb') as stream:
            self.assertEqual(b'original state', stream.read())
        with open(self.receipt) as stream:
            self.assertEqual('complete', json.load(stream)['state'])


class PartialCopyLinkTests(unittest.TestCase):
    def test_hard_link_replacing_partial_file_is_retained(self):
        with tempfile.TemporaryDirectory() as parent:
            source = os.path.join(parent, 'source')
            target = os.path.join(parent, 'target')
            receipt = os.path.join(parent, 'receipt.json')
            outside = os.path.join(parent, 'outside')
            os.mkdir(source)
            with open(os.path.join(source, 'data'), 'wb') as stream:
                stream.write(b'original source')
            with mock.patch.object(views.shutil, 'copyfileobj',
                                   side_effect=OSError('injected interruption')):
                with self.assertRaisesRegex(OSError, 'injected interruption'):
                    views.copy_tree_exclusive(source, target, receipt)
            partial = os.path.join(target, 'data')
            os.unlink(partial)
            with open(outside, 'wb') as stream:
                stream.write(b'outside content')
            os.link(outside, partial)
            with self.assertRaisesRegex(ValueError, 'hard-linked'):
                views.copy_tree_exclusive(source, target, receipt)
            self.assertEqual(os.stat(outside).st_ino, os.stat(partial).st_ino)
            with open(outside, 'rb') as stream:
                self.assertEqual(b'outside content', stream.read())
            with open(os.path.join(source, 'data'), 'rb') as stream:
                self.assertEqual(b'original source', stream.read())
            with open(receipt) as stream:
                self.assertEqual('copying', json.load(stream)['status'])


if __name__ == '__main__':
    unittest.main()
