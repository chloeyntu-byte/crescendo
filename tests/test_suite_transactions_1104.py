"""Fault injection for durable suite isolation and exact-view deletion."""
import os
import shutil
import unittest
from unittest import mock

from crescendo_engine import setup_private_session as session
from crescendo_engine.same_library_views import MARKER
from tests.test_setup_private_session import SetupPrivateSessionTests


class SuiteTransactionTests(SetupPrivateSessionTests):
    def test_detach_does_not_wait_for_controller_lock(self):
        controller = session._controller(self.reference)
        controller.lock.acquire()
        try:
            result = session.detach_setup_private_session(self.reference, 'stop unknown')
        finally:
            controller.lock.release()
        self.assertTrue(result['durable'])
        self.assertEqual([], result['errors'])
        row = session.list_setup_private_sessions(self.config)[0]
        self.assertEqual('quarantined', row['status'])
        self.assertFalse(row['can_load'])
        self.assertFalse(row['can_delete'])
        with self.assertRaisesRegex(RuntimeError, 'closed, unsafe'):
            controller._lock()
        self.assertTrue(all(os.path.isdir(row['path']) for row in self.reference['views']))

    def test_detach_disk_failure_still_blocks_process(self):
        controller = session._controller(self.reference)
        with mock.patch.object(session, 'atomic_write_json', side_effect=OSError('disk full')):
            result = session.detach_setup_private_session(self.reference, 'stop unknown')
        self.assertFalse(result['durable'])
        self.assertTrue(result['errors'])
        with self.assertRaises(RuntimeError):
            controller._lock()

    def test_registry_half_write_is_reconciled_from_intent(self):
        from crescendo_engine.same_library_views import register_view
        from crescendo_engine.utils import atomic_write_json
        controller = session._controller(self.reference)
        workspace = controller.workspace
        path = os.path.join(workspace['root'], 'workspace.json')
        registry_path = os.path.join(workspace['root'], 'same_library_views.json')
        import json
        with open(path) as stream:
            old_workspace = json.load(stream)
        row = workspace['same_library_registry'][0]
        source = row['source']
        register_view(workspace, source, ('injected',), role='working')
        atomic_write_json(path, old_workspace)
        owner, state, repaired = session._read_suite(self.reference, deep=False, require_snapshot=False)
        self.assertEqual(repaired['same_library_registry'], workspace['same_library_registry'])
        self.assertEqual(len(repaired['same_library_registry']), len(old_workspace['same_library_registry']) + 1)
        atomic_write_json(path, workspace)
        with open(registry_path) as stream:
            self.assertEqual(json.load(stream)['views'], repaired['same_library_registry'])

    def test_partial_tombstone_deletion_retries_without_marker(self):
        original = shutil.rmtree
        once = [True]
        def interrupted(path, *args, **kwargs):
            if once[0] and os.path.basename(path).startswith('.crescendo-delete-'):
                once[0] = False
                marker = os.path.join(path, MARKER)
                if os.path.exists(marker):
                    os.unlink(marker)
                raise OSError('injected partial deletion')
            return original(path, *args, **kwargs)
        with mock.patch('crescendo_engine.same_library_views.shutil.rmtree', side_effect=interrupted):
            result = session.delete_setup_private_session(self.reference)
        self.assertFalse(result['cleanup_proven'])
        self.assertTrue(result['retained'])
        retry = session.delete_setup_private_session(self.reference)
        self.assertTrue(retry['cleanup_proven'], retry)
        self.assertTrue(all(not os.path.lexists(row['path']) for row in self.reference['views']))
        self.assertTrue(os.path.exists(self.fixture.schematic))

    def test_partial_deletion_recovers_after_process_restart(self):
        original = shutil.rmtree
        once = [True]
        def interrupted(path, *args, **kwargs):
            if once[0] and os.path.basename(path).startswith('.crescendo-delete-'):
                once[0] = False
                marker = os.path.join(path, MARKER)
                if os.path.isfile(marker):
                    os.unlink(marker)
                raise OSError('injected process interruption')
            return original(path, *args, **kwargs)
        with mock.patch('crescendo_engine.same_library_views.shutil.rmtree', side_effect=interrupted):
            result = session.delete_setup_private_session(self.reference)
        self.assertFalse(result['cleanup_proven'])
        controller = session._controller(self.reference)
        session._release_lease(controller.lease)
        controller.lease = None
        with session._REGISTRY_LOCK:
            session._SESSIONS.pop(self.reference['session_id'], None)
        reopened = session.reopen_setup_private_session(self.reference, self.config, for_delete=True)
        result = session.delete_setup_private_session(reopened)
        self.assertTrue(result['cleanup_proven'], result)
        self.assertTrue(os.path.exists(self.fixture.schematic))

    def test_receipt_rejects_replaced_tombstone(self):
        from crescendo_engine.same_library_views import _delete_transaction
        controller = session._controller(self.reference)
        row = controller.workspace['same_library_registry'][0]
        from crescendo_engine.private_view_access import restore_private_views
        restore_private_views(controller.workspace)
        with mock.patch('crescendo_engine.same_library_views.shutil.rmtree', side_effect=OSError('interrupted')):
            with self.assertRaises(OSError):
                _delete_transaction(controller.workspace, row)
        import hashlib
        tomb = os.path.join(os.path.dirname(row['path']), '.crescendo-delete-' + hashlib.sha256(row['path'].encode('utf-8')).hexdigest()[:32])
        old = tomb + '-old'
        os.rename(tomb, old)
        os.mkdir(tomb)
        try:
            with self.assertRaisesRegex(ValueError, 'identity changed'):
                _delete_transaction(controller.workspace, row)
        finally:
            os.rmdir(tomb)
            os.rename(old, tomb)
            _delete_transaction(controller.workspace, row)

if __name__ == '__main__':
    unittest.main()

class RefreshRecoveryTests(SetupPrivateSessionTests):
    def interrupted_refresh(self):
        controller = session._controller(self.reference)
        with mock.patch.object(controller, 'writable_views', side_effect=SystemExit('process interruption')):
            with self.assertRaises(SystemExit):
                session.refresh_setup_private_session(self.reference, self.prepared, self.config)
        self.assertIsNotNone(controller.refresh_pending)
        session._release_lease(controller.lease)
        controller.lease = None
        with session._REGISTRY_LOCK:
            session._SESSIONS.pop(self.reference['session_id'], None)
        return controller

    def test_refresh_recovery_replays_after_interrupted_swap(self):
        self.interrupted_refresh()
        from crescendo_engine import same_library_views
        original = same_library_views._rename_noreplace
        hit = [False]
        def interrupted(source, target):
            if '__cr_recover_' in source and not hit[0]:
                hit[0] = True
                raise OSError('injected swap interruption')
            return original(source, target)
        with mock.patch('crescendo_engine.same_library_views._rename_noreplace', side_effect=interrupted):
            with self.assertRaisesRegex(OSError, 'injected swap'):
                session.recover_setup_private_session(self.reference, self.config)
        result = session.recover_setup_private_session(self.reference, self.config)
        self.assertTrue(result['recovered'])
        reopened = session.reopen_setup_private_session(self.reference, self.config)
        self.assertEqual('ready', reopened['status'])
        self.assertTrue(os.path.exists(self.fixture.schematic))

    def test_old_refresh_record_is_retained_and_rejected(self):
        controller = self.interrupted_refresh()
        state_path = os.path.join(self.reference['root'], 'session_state.json')
        commit_path = os.path.join(self.reference['root'], 'suite_metadata_commit.json')
        import json
        from crescendo_engine.utils import atomic_write_json
        with open(state_path) as stream:
            state = json.load(stream)
        state['refresh_pending'] = {'backup_root': controller.refresh_pending['backup_root'], 'views': []}
        atomic_write_json(state_path, state)
        os.unlink(commit_path)
        with self.assertRaisesRegex(ValueError, 'Legacy or missing'):
            session.recover_setup_private_session(self.reference, self.config)
        self.assertTrue(os.path.isdir(controller.refresh_pending['backup_root']))
        # Test teardown should not assume a controller remains active.
        with session._REGISTRY_LOCK:
            session._SESSIONS[self.reference['session_id']] = controller
        controller.lease = session._acquire_lease(self.reference['root'])
        controller.unsafe = False

    def test_commit_repairs_mixed_state_workspace_revisions(self):
        controller = session._controller(self.reference)
        workspace_path = os.path.join(self.reference['workspace_root'], 'workspace.json')
        import json
        from crescendo_engine.utils import atomic_write_json
        with open(workspace_path) as stream:
            old_workspace = json.load(stream)
        controller.label = 'committed revision'
        controller.persist()
        atomic_write_json(workspace_path, old_workspace)
        owner, state, workspace = session._read_suite(self.reference, deep=False, require_snapshot=False)
        self.assertEqual('committed revision', state['label'])
        self.assertEqual(state['_suite_revision'], workspace['_suite_revision'])

class CreationAndCommitRecoveryTests(SetupPrivateSessionTests):
    def test_incomplete_copy_never_publishes_target(self):
        from crescendo_engine.same_library_views import copy_setup_view
        controller = session._controller(self.reference)
        workspace = controller.workspace
        source = workspace['same_library_registry'][0]['source']
        with mock.patch('crescendo_engine.workspace._copy_private_file', side_effect=OSError('copy interrupted')):
            with self.assertRaisesRegex(OSError, 'copy interrupted'):
                copy_setup_view(workspace, source, ('interrupted-create',))
        row = workspace['same_library_registry'][-1]
        self.assertFalse(os.path.lexists(row['path']))
        suite = session.list_setup_private_sessions(self.config)[0]
        self.assertEqual('recovery_required', suite['status'])
        self.assertFalse(suite['can_delete'])
        with self.assertRaisesRegex(ValueError, 'Incomplete creation staging'):
            session._read_suite(self.reference, require_snapshot=False)

    def test_committed_revision_survives_interrupted_first_mirror(self):
        controller = session._controller(self.reference)
        original = session.atomic_write_json
        state_path = os.path.join(self.reference['root'], 'session_state.json')
        controller.label = 'new committed state'
        def fail_state(path, value):
            if path == state_path:
                raise OSError('state mirror interrupted')
            return original(path, value)
        with mock.patch.object(session, 'atomic_write_json', side_effect=fail_state):
            with self.assertRaisesRegex(OSError, 'state mirror'):
                controller.persist()
        owner, state, workspace = session._read_suite(self.reference, deep=False, require_snapshot=False)
        self.assertEqual('new committed state', state['label'])
        self.assertEqual(state['_suite_revision'], workspace['_suite_revision'])

    def test_atomic_publication_refuses_occupied_destination(self):
        import tempfile
        from crescendo_engine.same_library_views import _rename_noreplace
        with tempfile.TemporaryDirectory() as root:
            source, destination = os.path.join(root, 'source'), os.path.join(root, 'destination')
            os.mkdir(source)
            os.mkdir(destination)
            with self.assertRaises(OSError):
                _rename_noreplace(source, destination)
            self.assertTrue(os.path.isdir(source))
            self.assertTrue(os.path.isdir(destination))

class RefreshSafetyGateTests(RefreshRecoveryTests):
    def test_unproven_native_stop_prevents_restore(self):
        self.interrupted_refresh()
        from crescendo_engine.utils import atomic_write_json
        import json
        root = os.path.join(self.reference['root'], 'synthetic-capture')
        os.mkdir(root)
        state_path = os.path.join(self.reference['root'], 'session_state.json')
        with open(state_path) as stream:
            state = json.load(stream)
        state['capture_roots'].append(root)
        atomic_write_json(state_path, state)
        with mock.patch.object(session, '_reconcile_owned_capture', return_value={'cleanup_proven': False}):
            with self.assertRaisesRegex(ValueError, 'not proven stopped'):
                session.recover_setup_private_session(self.reference, self.config)
        self.assertTrue(os.path.isdir(self.reference['views'][0]['path']))
        state['capture_roots'].remove(root)
        atomic_write_json(state_path, state)
        session.recover_setup_private_session(self.reference, self.config)
        session.reopen_setup_private_session(self.reference, self.config)

    def test_changed_backup_is_retained_and_refused(self):
        controller = self.interrupted_refresh()
        backup = controller.refresh_pending['views'][0]['backup']
        with open(os.path.join(backup, 'injected-change'), 'w') as stream:
            stream.write('modified')
        with self.assertRaisesRegex(ValueError, 'fingerprint changed'):
            session.recover_setup_private_session(self.reference, self.config)
        self.assertTrue(os.path.isdir(backup))
        os.unlink(os.path.join(backup, 'injected-change'))
        session.recover_setup_private_session(self.reference, self.config)
        session.reopen_setup_private_session(self.reference, self.config)

class PortabilityAndRecoveryOfferTests(RefreshRecoveryTests):
    def test_unsupported_kernel_or_filesystem_retains_both_objects(self):
        import ctypes
        import errno
        import tempfile
        from crescendo_engine.same_library_views import _rename_noreplace
        with tempfile.TemporaryDirectory() as root:
            source, target = os.path.join(root, 'source'), os.path.join(root, 'target')
            os.mkdir(source)
            for code in (errno.ENOSYS, errno.EINVAL, errno.EOPNOTSUPP, errno.EXDEV):
                with mock.patch.object(ctypes, 'CDLL') as library:
                    library.return_value.syscall.return_value = -1
                    with mock.patch.object(ctypes, 'get_errno', return_value=code):
                        with self.assertRaisesRegex(OSError, 'CRESCENDO-SUITE-RENAME'):
                            _rename_noreplace(source, target)
                self.assertTrue(os.path.isdir(source))
                self.assertFalse(os.path.lexists(target))

    def test_unknown_architecture_fails_closed(self):
        import platform
        from crescendo_engine.same_library_views import _rename_noreplace
        with mock.patch.object(platform, 'machine', return_value='unknown_arch'):
            with self.assertRaisesRegex(OSError, 'unsupported on architecture'):
                _rename_noreplace('source', 'target')

    def test_new_pending_advertises_recovery_without_claiming_stop(self):
        self.interrupted_refresh()
        item = session.list_setup_private_sessions(self.config)[0]
        self.assertTrue(item['can_recover'])
        self.assertFalse(item['can_load'])
        self.assertFalse(item['can_delete'])
        session.recover_setup_private_session(self.reference, self.config)
        session.reopen_setup_private_session(self.reference, self.config)

class LegacyMigrationTests(SetupPrivateSessionTests):
    def test_legacy_backup_is_complete_and_never_overwritten(self):
        import hashlib
        import json
        controller = session._controller(self.reference)
        backup = os.path.join(self.reference['root'], 'suite_legacy_metadata_backup.json')
        commit = os.path.join(self.reference['root'], 'suite_metadata_commit.json')
        os.unlink(backup)
        os.unlink(commit)
        with open(os.path.join(self.reference['root'], 'session_state.json')) as stream:
            state = json.load(stream)
        with open(os.path.join(self.reference['workspace_root'], 'workspace.json')) as stream:
            workspace = json.load(stream)
        controller.persist()
        with open(backup, 'rb') as stream:
            initial = stream.read()
        payload = json.loads(initial.decode('utf-8'))
        self.assertEqual(state, payload['state'])
        self.assertEqual(workspace, payload['workspace'])
        self.assertEqual(state['prepared_hash'], session._snapshot_hash(payload['prepared']))
        controller.label = 'later state'
        controller.persist()
        with open(backup, 'rb') as stream:
            self.assertEqual(hashlib.sha256(initial).digest(), hashlib.sha256(stream.read()).digest())

    def test_live_controller_blocks_recovery_with_release_explanation(self):
        controller = session._controller(self.reference)
        with mock.patch.object(controller, 'writable_views', side_effect=SystemExit('interrupt')):
            with self.assertRaises(SystemExit):
                session.refresh_setup_private_session(self.reference, self.prepared, self.config)
        listed = session.list_setup_private_sessions(self.config)[0]
        self.assertFalse(listed['can_recover'])
        self.assertIn('lease is still held', listed['can_recover_reason'])
        with self.assertRaisesRegex(RuntimeError, 'still owns the suite lease'):
            session.recover_setup_private_session(self.reference, self.config)
        self.assertIs(session._controller(self.reference), controller)
