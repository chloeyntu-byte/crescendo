"""Force-exit integration: actual Tk in disposable processes, no EDA tools."""
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

from crescendo_engine import ui_exit


def run_gui_child(case):
    from concurrent.futures import ThreadPoolExecutor
    from crescendo_engine import gui
    from tests.test_gui_execution_progress import FixtureProgressService
    never = threading.Event()
    service = FixtureProgressService()
    normal_started = threading.Event()
    def normal_exit():
        normal_started.set()
        os.write(1, b'NORMAL_BLOCKED\n')
        if case in ('failure', 'failed_state_hang'):
            raise RuntimeError('fixture cleanup failure')
        never.wait()
    def request_force():
        os.write(1, b'FORCE_REQUEST\n')
    def force_exit():
        if case == 'hang':
            os.write(1, b'FORCE_BLOCKED\n')
            never.wait()
        if case == 'disk_error':
            raise OSError('injected disk failure')
        os.write(1, b'FORCE_FALSE\n')
        return {'cleanup_confirmed': False, 'warnings': ['retained']}
    service.exit_optimizer = normal_exit
    service.request_force_exit = request_force
    service.force_exit_optimizer = force_exit
    real_class = gui.OptimizerGui
    class TestApp(real_class):
        def __init__(self, root, *args, **kwargs):
            real_class.__init__(self, root, *args, **kwargs)
            if case in ('closing_state_hang', 'failed_state_hang'):
                self.launch_context = {'state_path': '/tmp/crescendo-exit-fixture/state.json'}
                def write_state(snapshot):
                    state = snapshot[1]['state']
                    if ((case == 'closing_state_hang' and state == 'closing') or
                            (case == 'failed_state_hang' and state == 'exit_failed')):
                        os.write(1, b'STATE_BLOCKED\n')
                        never.wait()
                self._write_exit_launch_snapshot = write_state
            executor = ThreadPoolExecutor(max_workers=1)
            executor.submit(never.wait)
            root.bind('<Destroy>', lambda e: os.write(1, b'ROOT_DESTROYED\n') if e.widget is root else None, add='+')
            def start():
                queue_locked = threading.Event()
                def lock_queue():
                    self.tasks.state_lock.acquire()
                    queue_locked.set()
                    never.wait()
                lock_started = [False]
                if case == 'queue_prelocked':
                    thread = threading.Thread(target=lock_queue)
                    thread.daemon = True
                    thread.start()
                    queue_locked.wait(1)
                    os.write(1, b'QUEUE_LOCKED\n')
                assert not hasattr(self, 'force_exit_button')
                self.request_exit(confirm=False)
                def force_when_started():
                    if normal_started.is_set() or case in ('queue_prelocked', 'closing_state_hang'):
                        if case == 'queue_lock':
                            if not lock_started[0]:
                                lock_started[0] = True
                                thread = threading.Thread(target=lock_queue)
                                thread.daemon = True
                                thread.start()
                            if not queue_locked.is_set():
                                root.after(10, force_when_started)
                                return
                            os.write(1, b'QUEUE_LOCKED\n')
                        if not getattr(self, 'exit_dialog', None) or not self.exit_dialog.winfo_viewable():
                            root.after(10, force_when_started)
                            return
                        if case in ('failure', 'failed_state_hang'):
                            assert 'fixture cleanup failure' in self.exit_dialog_message.get()
                            assert str(self.exit_retry_button.cget('state')) == 'normal'
                        assert self.exit_force_button.cget('text') == '强制退出'
                        os.write(1, b'DIALOG_FORCE\n')
                        self.exit_force_button.invoke()
                    else:
                        root.after(10, force_when_started)
                root.after(10, force_when_started)
            root.after(100, start)
    gui.OptimizerGui = TestApp
    gui.launch_gui({}, service=service, force_exit_process=True)
    os.write(1, b'UNEXPECTED_RETURN\n')
    os._exit(9)


@unittest.skipUnless(os.environ.get('DISPLAY'), 'actual Tk display required')
class ForceExitProcessTests(unittest.TestCase):
    def test_blocked_exit_hung_force_disk_failure_and_false_cleanup(self):
        for case in ('hang', 'disk_error', 'false_cleanup', 'queue_lock', 'queue_prelocked', 'failure',
                     'closing_state_hang', 'failed_state_hang'):
            with self.subTest(case=case):
                start = time.monotonic()
                result = subprocess.run([sys.executable, '-m', 'tests.test_force_ui_exit', '--child', case],
                    env=dict(os.environ, PYTHONDONTWRITEBYTECODE='1'), stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE, timeout=10)
                self.assertEqual(result.returncode, 0, result.stderr.decode('utf-8', 'replace'))
                self.assertLess(time.monotonic() - start, 8)
                if case not in ('queue_prelocked', 'closing_state_hang'): self.assertIn(b'NORMAL_BLOCKED', result.stdout)
                if case in ('closing_state_hang', 'failed_state_hang'): self.assertIn(b'STATE_BLOCKED', result.stdout)
                self.assertIn(b'DIALOG_FORCE', result.stdout)
                self.assertIn(b'FORCE_REQUEST', result.stdout)
                self.assertIn(b'ROOT_DESTROYED', result.stdout)
                if case == 'hang': self.assertIn(b'FORCE_BLOCKED', result.stdout)
                if case == 'false_cleanup': self.assertIn(b'FORCE_FALSE', result.stdout)
                if case in ('queue_lock', 'queue_prelocked'): self.assertIn(b'QUEUE_LOCKED', result.stdout)


class DetachTests(unittest.TestCase):
    def test_exit_state_writer_does_not_recreate_removed_directory(self):
        from crescendo_engine.gui import OptimizerGui
        with tempfile.TemporaryDirectory() as directory:
            removed = os.path.join(directory, 'removed')
            OptimizerGui._write_exit_launch_snapshot((os.path.join(removed, 'state.json'), {'state': 'closed'}))
            self.assertFalse(os.path.exists(removed))

    def test_disk_write_failure_report_does_not_claim_cleanup(self):
        service = mock.Mock()
        service.prepare_cancel_event = threading.Event()
        service.range_validation_cancels = {}
        service.setup_private_sessions = {}
        service.session_run_names = set()
        service.config = {'runtime': {'run_root': '/tmp'}}
        with mock.patch('crescendo_engine.ui_exit.atomic_write_json', side_effect=OSError('disk full')):
            result = ui_exit.detach_service(service)
        self.assertFalse(result['cleanup_confirmed'])
        self.assertTrue(any('not durable' in warning for warning in result['warnings']))
        self.assertTrue(service.prepare_cancel_event.is_set())


class RestartIsolationTests(unittest.TestCase):
    def test_force_exit_releases_healthy_and_quarantines_only_unresolved_suite(self):
        import copy
        import shutil
        from crescendo_engine import setup_private_session as sessions
        from crescendo_engine.server import OptimizerService
        from tests.test_setup_private_ui import SetupPrivateServiceIntegrationTests
        fixture = SetupPrivateServiceIntegrationTests(methodName='runTest')
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        token, _preview = fixture.inspect()
        _prepared_id, prepared = fixture.apply(token)
        healthy = prepared['setup_private_session']
        unresolved = sessions.create_setup_private_session(fixture.fixture.prepared, fixture.service.config)
        fixture.service._remember_setup_private_session(unresolved)
        self.assertEqual(set(fixture.service.setup_private_sessions),
                         {healthy['session_id'], unresolved['session_id']})
        capture = os.path.join(fixture.fixture.temp.name, 'pending-capture')
        os.mkdir(capture)
        sessions.write_session_reference(capture, unresolved, kind='native_baseline')
        shutil.rmtree(capture)
        report = fixture.service.force_exit_optimizer()
        rows = {row['session_id']: row for row in report['suites']}
        self.assertEqual(len(rows), len(report['suites']))
        self.assertEqual(set(rows), {healthy['session_id'], unresolved['session_id']})
        self.assertEqual(rows[healthy['session_id']]['state'], 'released')
        self.assertFalse(rows[healthy['session_id']]['detached'])
        self.assertTrue(rows[unresolved['session_id']]['detached'])
        self.assertTrue(rows[unresolved['session_id']]['durable'])
        self.assertFalse(os.path.exists(os.path.join(healthy['root'], sessions.QUARANTINE)))
        self.assertTrue(os.path.exists(os.path.join(unresolved['root'], sessions.QUARANTINE)))
        # Simulate process death for the unresolved fixture without deleting it.
        controller = sessions._controller(unresolved)
        sessions._release_lease(controller.lease)
        controller.lease = None
        with sessions._REGISTRY_LOCK:
            sessions._SESSIONS.pop(unresolved['session_id'], None)
            sessions._QUARANTINED.discard(unresolved['session_id'])
            sessions._CLOSED.discard(healthy['session_id'])
        fixture.service.setup_private_sessions.clear()
        other = OptimizerService(copy.deepcopy(fixture.fixture.config))
        listed = other.list_private_suites()['suites']
        ids = [row['id'] for row in listed]
        self.assertEqual(len(ids), len(set(ids)))
        self.assertEqual(ids.count(healthy['session_id']), 1)
        self.assertEqual(ids.count(unresolved['session_id']), 1)
        with self.assertRaisesRegex(ValueError, 'isolated'):
            other.load_private_suite(unresolved['session_id'])
        with mock.patch('crescendo_engine.hierarchy.cell_scope_records', return_value=[fixture.scope]):
            loaded = other.load_private_suite(healthy['session_id'])
        self.assertEqual(loaded['suite']['id'], healthy['session_id'])
        self.assertTrue(sessions.close_setup_private_session(other.setup_private_sessions[healthy['session_id']])['cleanup_proven'])
        other.setup_private_sessions.clear()

    def test_failed_marker_with_durable_unresolved_capture_is_scoped(self):
        import copy
        import shutil
        from crescendo_engine import setup_private_session as sessions
        from crescendo_engine.server import OptimizerService
        from tests.test_setup_private_ui import SetupPrivateServiceIntegrationTests
        fixture = SetupPrivateServiceIntegrationTests(methodName='runTest')
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        token, preview = fixture.inspect()
        _prepared_id, healthy = fixture.apply(token)
        healthy = healthy['setup_private_session']
        self.assertTrue(sessions.close_setup_private_session(healthy)['cleanup_proven'])
        fixture.service.setup_private_sessions.pop(healthy['session_id'], None)
        token, preview = fixture.inspect()
        _prepared_id, pending = fixture.apply(token)
        pending = pending['setup_private_session']
        capture = os.path.join(fixture.fixture.temp.name, 'capture')
        os.mkdir(capture)
        sessions.write_session_reference(capture, pending, kind='native_baseline')
        shutil.rmtree(capture)  # missing capture proof models storage loss
        with mock.patch('crescendo_engine.setup_private_session.atomic_write_json', side_effect=OSError('disk full')):
            report = fixture.service.force_exit_optimizer()
        row = next(item for item in report['suites'] if item['session_id'] == pending['session_id'])
        self.assertFalse(row['durable'])
        self.assertFalse(report['cleanup_confirmed'])
        self.assertFalse(os.path.exists(os.path.join(pending['root'], sessions.QUARANTINE)))
        # Simulate process death: release only the fixture lease and process state.
        controller = sessions._controller(pending)
        sessions._release_lease(controller.lease)
        controller.lease = None
        with sessions._REGISTRY_LOCK:
            sessions._SESSIONS.pop(pending['session_id'], None)
            sessions._QUARANTINED.discard(pending['session_id'])
        fixture.service.setup_private_sessions.pop(pending['session_id'], None)
        other = OptimizerService(copy.deepcopy(fixture.fixture.config))
        with self.assertRaisesRegex(ValueError, 'evidence directory is missing'):
            other.load_private_suite(pending['session_id'])
        with mock.patch('crescendo_engine.hierarchy.cell_scope_records', return_value=[fixture.scope]):
            loaded = other.load_private_suite(healthy['session_id'])
        self.assertEqual(loaded['suite']['id'], healthy['session_id'])
        self.assertTrue(sessions.close_setup_private_session(other.setup_private_sessions[healthy['session_id']])['cleanup_proven'])
        other.setup_private_sessions.pop(healthy['session_id'], None)
        fixture.service = other
        token, preview = fixture.inspect()
        self.assertNotEqual(preview['setup_private_session']['session_id'], pending['session_id'])


if __name__ == '__main__':
    if len(sys.argv) == 3 and sys.argv[1] == '--child':
        run_gui_child(sys.argv[2])
    else:
        unittest.main()
