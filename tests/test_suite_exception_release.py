"""A failed suite initialization/refresh must not retain an orphan lease."""
import os
import unittest
from unittest import mock

from crescendo_engine import setup_private_session as session
from tests.test_setup_private_session import SetupPrivateSessionTests


class SuiteExceptionReleaseTests(unittest.TestCase):
    def fixture(self):
        fixture = SetupPrivateSessionTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        return fixture

    def test_failed_refresh_snapshot_keeps_suite_closeable(self):
        for error in (OSError(5, 'snapshot I/O error'), ValueError('malformed suite state')):
            with self.subTest(error=type(error).__name__):
                fixture = self.fixture()
                controller = session._controller(fixture.reference)
                before = session.setup_private_session_snapshot(fixture.reference)
                with mock.patch.object(session, '_read_suite', side_effect=error):
                    with self.assertRaises(type(error)) as caught:
                        session.refresh_setup_private_session(fixture.reference, fixture.prepared, fixture.config)
                self.assertIs(caught.exception, error)
                self.assertEqual(before, session.setup_private_session_snapshot(fixture.reference))
                acquired = controller.lock.acquire(blocking=False)
                if acquired:
                    controller.lock.release()
                else:
                    # Release this exact fixture lock before reporting failure.
                    controller.lock.release()
                self.assertTrue(acquired)
                self.assertTrue(session.close_setup_private_session(fixture.reference, timeout_seconds=0)['cleanup_proven'])

    def test_failed_constructor_releases_unregistered_lease_and_keeps_evidence(self):
        for filename in (session.OWNER, 'session_state.json'):
            with self.subTest(file=filename):
                fixture = self.fixture()
                leases = []
                acquire, write = session._acquire_lease, session.atomic_write_json
                def capture(root):
                    fd = acquire(root)
                    leases.append((root, fd))
                    return fd
                def fail(path, data, *args, **kwargs):
                    if leases and os.path.dirname(path) == leases[-1][0] and os.path.basename(path) == filename:
                        raise OSError(28, 'injected disk full', path)
                    return write(path, data, *args, **kwargs)
                with mock.patch.object(session, '_acquire_lease', side_effect=capture), \
                        mock.patch.object(session, 'atomic_write_json', side_effect=fail):
                    with self.assertRaises(OSError) as caught:
                        session.create_setup_private_session(fixture.prepared, fixture.config)
                self.assertEqual(caught.exception.errno, 28)
                root, fd = leases[-1]
                try:
                    os.fstat(fd)
                except OSError:
                    released = True
                else:
                    released = False
                    session._release_lease(fd)
                self.assertTrue(released)
                self.assertTrue(os.path.isdir(root))
                self.assertTrue(os.path.isfile(os.path.join(root, 'suite.lease')))
                self.assertNotIn(os.path.basename(root), session._SESSIONS)
                retry = acquire(root)
                session._release_lease(retry)
