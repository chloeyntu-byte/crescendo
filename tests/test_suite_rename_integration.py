"""Filesystem integration coverage when renameat2 is unavailable."""
import builtins
import errno
import os
import tempfile
import shutil
import unittest
from unittest import mock

from crescendo_engine import setup_private_session as session
from crescendo_engine import same_library_views as views
from tests import test_suite_transactions_1104 as transactions


class _UnavailableLibc(object):
    def __init__(self, error):
        self.error = error
        self.calls = 0

    def syscall(self, *args):
        self.calls += 1
        return -1


class _CtypesForViews(object):
    def __init__(self, real, libc):
        self.real = real
        self.libc = libc

    def CDLL(self, *args, **kwargs):
        return self.libc

    def get_errno(self):
        return self.libc.error

    def __getattr__(self, name):
        return getattr(self.real, name)


class RenameUnavailableIntegrationTests(transactions.RefreshRecoveryTests):
    def setUp(self):
        import ctypes
        self.libc = _UnavailableLibc(getattr(self, 'rename_errno', errno.EINVAL))
        self.ctypes_proxy = _CtypesForViews(ctypes, self.libc)
        original_import = builtins.__import__

        def isolated_import(name, globals=None, locals=None, fromlist=(), level=0):
            if name == 'ctypes' and globals and globals.get('__name__') == views.__name__:
                return self.ctypes_proxy
            return original_import(name, globals, locals, fromlist, level)

        patcher = mock.patch.object(builtins, '__import__', side_effect=isolated_import)
        patcher.start()
        self.addCleanup(patcher.stop)
        super(RenameUnavailableIntegrationTests, self).setUp()

    def cleanup(self):
        with session._REGISTRY_LOCK:
            owned = self.reference['session_id'] in session._SESSIONS
        if owned:
            super(RenameUnavailableIntegrationTests, self).cleanup()

    def _lifecycle(self):
        paths = [row['path'] for row in self.reference['views']]
        self.assertGreater(self.libc.calls, 0, 'creation must exercise unavailable syscall')
        self.assertTrue(session.close_setup_private_session(self.reference)['cleanup_proven'])
        reopened = session.reopen_setup_private_session(self.reference, self.config)
        self.assertEqual('ready', reopened['status'])
        refreshed = session.refresh_setup_private_session(reopened, self.prepared, self.config)
        self.assertEqual(paths, [row['path'] for row in refreshed['views']])
        deleted = session.delete_setup_private_session(reopened)
        self.assertTrue(deleted['cleanup_proven'], deleted)
        self.assertTrue(all(not os.path.lexists(path) for path in paths))
        self.assertTrue(os.path.isfile(self.fixture.schematic))

    def test_lifecycle_einval(self):
        self._lifecycle()

    def test_delete_retry_after_marker_removed(self):
        original = shutil.rmtree
        hit = [False]
        owned = set(row['path'] for row in self.reference['views'])

        def interrupted(path, *args, **kwargs):
            if not hit[0] and (path in owned or os.path.basename(path).startswith('.crescendo-delete-')):
                hit[0] = True
                marker = os.path.join(path, views.MARKER)
                if os.path.isfile(marker):
                    os.unlink(marker)
                raise OSError('injected partial removal')
            return original(path, *args, **kwargs)

        with mock.patch.object(views.shutil, 'rmtree', side_effect=interrupted):
            result = session.delete_setup_private_session(self.reference)
        self.assertTrue(hit[0], result)
        self.assertFalse(result['cleanup_proven'])
        self.assertTrue(result['retained'])
        result = session.delete_setup_private_session(self.reference)
        self.assertTrue(result['cleanup_proven'], result)
        self.assertTrue(os.path.isfile(self.fixture.schematic))

    def test_interrupted_refresh_recovery(self):
        self.interrupted_refresh()
        result = session.recover_setup_private_session(self.reference, self.config)
        self.assertTrue(result['recovered'], result)
        reopened = session.reopen_setup_private_session(self.reference, self.config)
        self.assertEqual('ready', reopened['status'])
        self.assertTrue(os.path.isfile(self.fixture.schematic))

    def test_copy_interruption_retries_with_same_receipt(self):
        with tempfile.TemporaryDirectory() as root:
            source = os.path.join(root, 'source')
            target = os.path.join(root, 'target')
            receipt = os.path.join(root, 'copy.json')
            os.mkdir(source)
            with open(os.path.join(source, 'content'), 'w') as stream:
                stream.write('complete source')
            def fail_copy(reader, writer, length):
                writer.write(b'partial')
                raise OSError('injected copy interruption')
            with mock.patch.object(views.shutil, 'copyfileobj', side_effect=fail_copy):
                with self.assertRaisesRegex(OSError, 'injected copy'):
                    views.copy_tree_exclusive(source, target, receipt)
            inode = os.stat(target).st_ino
            self.assertTrue(os.path.isfile(receipt))
            result = views.copy_tree_exclusive(source, target, receipt)
            self.assertEqual('complete', result['status'])
            self.assertEqual(inode, os.stat(target).st_ino)
            with open(os.path.join(target, 'content')) as stream:
                self.assertEqual('complete source', stream.read())
            self.assertTrue(os.path.isdir(source))

    def test_recover_after_source_deletion_loses_marker(self):
        self.interrupted_refresh()
        original = shutil.rmtree
        live = self.reference['views'][0]['path']
        hit = [False]
        def interrupted(path, *args, **kwargs):
            if path == live and not hit[0]:
                hit[0] = True
                os.unlink(os.path.join(path, views.MARKER))
                raise OSError('injected recovery removal interruption')
            return original(path, *args, **kwargs)
        with mock.patch.object(session.shutil, 'rmtree', side_effect=interrupted):
            with self.assertRaisesRegex(OSError, 'injected recovery'):
                session.recover_setup_private_session(self.reference, self.config)
        self.assertTrue(hit[0])
        self.assertFalse(os.path.exists(os.path.join(live, views.MARKER)))
        result = session.recover_setup_private_session(self.reference, self.config)
        self.assertTrue(result['recovered'], result)
        reopened = session.reopen_setup_private_session(self.reference, self.config)
        self.assertEqual('ready', reopened['status'])
        self.assertTrue(os.path.isfile(self.fixture.schematic))

    def test_partial_delete_retry_after_reopen_for_delete(self):
        original = shutil.rmtree
        owned = set(row['path'] for row in self.reference['views'])
        hit = [False]
        def interrupted(path, *args, **kwargs):
            if path in owned and not hit[0]:
                hit[0] = True
                os.unlink(os.path.join(path, views.MARKER))
                raise OSError('injected delete interruption')
            return original(path, *args, **kwargs)
        with mock.patch.object(views.shutil, 'rmtree', side_effect=interrupted):
            result = session.delete_setup_private_session(self.reference)
        self.assertTrue(hit[0], result)
        self.assertFalse(result['cleanup_proven'])
        controller = session._controller(self.reference)
        session._release_lease(controller.lease)
        controller.lease = None
        with session._REGISTRY_LOCK:
            session._SESSIONS.pop(self.reference['session_id'], None)
        reopened = session.reopen_setup_private_session(self.reference, self.config, for_delete=True)
        result = session.delete_setup_private_session(reopened)
        self.assertTrue(result['cleanup_proven'], result)
        self.assertTrue(all(not os.path.lexists(path) for path in owned))
        self.assertTrue(os.path.isfile(self.fixture.schematic))

    def test_unproven_stop_still_blocks_delete(self):
        root = self.capture('unproven-stop')
        with mock.patch('crescendo_engine.private_netlist.reconcile_preparation',
                        return_value={'cleanup_proven': False}):
            with session.borrow_setup_private_session(self.prepared, self.config, root):
                pass
            result = session.delete_setup_private_session(self.reference)
        self.assertFalse(result['cleanup_proven'], result)
        self.assertTrue(os.path.isdir(self.reference['maestro']['path']))
        self.assertTrue(os.path.isfile(self.fixture.schematic))


class RenameEnosysIntegrationTests(RenameUnavailableIntegrationTests):
    rename_errno = errno.ENOSYS

    def test_lifecycle_enosys(self):
        self._lifecycle()


def load_tests(loader, tests, pattern):
    # The inherited fixture supplies helpers, but its original tests are run by
    # their own module. Collect only newly defined cases to avoid duplicates.
    suite = unittest.TestSuite()
    for cls in (RenameUnavailableIntegrationTests, RenameEnosysIntegrationTests):
        for name in sorted(cls.__dict__):
            if name.startswith('test_'):
                suite.addTest(cls(name))
    return suite
