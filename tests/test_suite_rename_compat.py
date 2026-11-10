import errno
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine import same_library_views as views


class ExclusiveCopyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.src = os.path.join(self.temp.name, 'stage')
        self.dst = os.path.join(self.temp.name, 'view')
        self.receipt = os.path.join(self.temp.name, 'receipt')
        os.mkdir(self.src)
        with open(os.path.join(self.src, 'data'), 'wb') as stream:
            stream.write(b'abc')
        with open(os.path.join(self.src, views.MARKER), 'wb') as stream:
            stream.write(b'owner')

    def test_complete_replay_and_changed_target(self):
        proof = views.copy_tree_exclusive(self.src, self.dst, self.receipt)
        self.assertEqual('complete', proof['status'])
        self.assertEqual(proof, views.copy_tree_exclusive(self.src, self.dst, self.receipt))
        with open(os.path.join(self.dst, 'data'), 'wb') as stream:
            stream.write(b'changed')
        with self.assertRaises(ValueError):
            views.copy_tree_exclusive(self.src, self.dst, self.receipt)

    def test_existing_targets_never_replaced(self):
        for kind in ('empty', 'nonempty', 'file', 'symlink'):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as parent:
                target = os.path.join(parent, 'target')
                source = os.path.join(parent, 'source')
                os.mkdir(source)
                if kind in ('empty', 'nonempty'):
                    os.mkdir(target)
                    if kind == 'nonempty':
                        open(os.path.join(target, 'keep'), 'w').close()
                elif kind == 'file':
                    open(target, 'w').close()
                else:
                    os.symlink('missing', target)
                before = os.lstat(target)
                with self.assertRaises(OSError):
                    views.copy_tree_exclusive(source, target, os.path.join(parent, 'receipt'))
                self.assertTrue(os.path.samestat(before, os.lstat(target)))
                self.assertTrue(os.path.isdir(source))

    def test_interrupted_copy_retains_evidence(self):
        with mock.patch.object(views.shutil, 'copyfileobj', side_effect=OSError('interrupted')):
            with self.assertRaisesRegex(OSError, 'interrupted'):
                views.copy_tree_exclusive(self.src, self.dst, self.receipt)
        self.assertTrue(os.path.isdir(self.src))
        self.assertTrue(os.path.isdir(self.dst))
        self.assertTrue(os.path.isfile(self.receipt))
        identity = os.lstat(self.dst)
        self.assertEqual('complete', views.copy_tree_exclusive(self.src, self.dst, self.receipt)['status'])
        self.assertTrue(os.path.samestat(identity, os.lstat(self.dst)))

    def test_partial_unknown_entry_refused(self):
        with mock.patch.object(views.shutil, 'copyfileobj', side_effect=OSError('interrupted')):
            with self.assertRaises(OSError):
                views.copy_tree_exclusive(self.src, self.dst, self.receipt)
        with open(os.path.join(self.dst, 'foreign'), 'w') as stream:
            stream.write('keep')
        with self.assertRaises(ValueError):
            views.copy_tree_exclusive(self.src, self.dst, self.receipt)
        self.assertTrue(os.path.exists(os.path.join(self.dst, 'foreign')))

    def test_source_symlink_refused(self):
        os.symlink('data', os.path.join(self.src, 'link'))
        with self.assertRaises(ValueError):
            views.copy_tree_exclusive(self.src, self.dst, self.receipt)
        self.assertFalse(os.path.lexists(self.dst))

    def test_exdev_not_unavailable(self):
        import ctypes
        with mock.patch.object(ctypes, 'CDLL') as libc, mock.patch.object(ctypes, 'get_errno', return_value=errno.EXDEV):
            libc.return_value.syscall.return_value = -1
            with self.assertRaises(OSError) as raised:
                views._rename_noreplace(self.src, self.dst)
        self.assertNotIsInstance(raised.exception, views.RenameNoReplaceUnavailable)
