"""Isolation checks never signal writers or touch native locks."""
import copy
import json
import os
import sys
import unittest
from unittest import mock

from crescendo_engine import setup_private_session as sessions
from crescendo_engine import suite_quarantine as quarantine
from crescendo_engine import legacy_guardian as guardian
from crescendo_engine.private_view_access import restore_private_views
from crescendo_engine.same_library_views import MARKER
from crescendo_engine.utils import atomic_write_json
from tests import test_setup_private_session as fixtures


def read_bytes(path):
    with open(path, 'rb') as stream:
        return stream.read()


class SuiteQuarantineTests(unittest.TestCase):
    setUp = fixtures.SetupPrivateSessionTests.setUp
    cleanup = fixtures.SetupPrivateSessionTests.cleanup

    def isolate_released(self):
        sessions.close_setup_private_session(self.reference)
        sessions.detach_setup_private_session(self.reference, 'forced exit; stop pending')
        return sessions._read_suite(self.reference, deep=False, require_snapshot=False)

    def row(self):
        return sessions.list_setup_private_sessions(self.config)[0]

    def test_release_archives_original_and_is_idempotent_and_loadable(self):
        self.isolate_released()
        original = read_bytes(os.path.join(self.reference['root'], sessions.QUARANTINE))
        row = self.row()
        self.assertEqual(row['status'], 'released')
        self.assertTrue(row['can_load'])
        self.assertTrue(row['can_delete'])
        receipt_path = os.path.join(self.reference['root'], quarantine.RECEIPT)
        receipt_bytes = read_bytes(receipt_path)
        receipt = json.loads(receipt_bytes.decode())
        self.assertEqual(set(receipt['checks']), {'native_processes', 'oa_locks', 'lease', 'ownership'})
        self.assertGreater(receipt['time'], 0)
        self.assertTrue(receipt['passed'])
        self.assertEqual(read_bytes(receipt['quarantine_archive']), original)
        self.row()
        self.assertEqual(read_bytes(receipt_path), receipt_bytes)
        sessions.reopen_setup_private_session(self.reference, self.config)
        self.assertTrue(sessions.delete_setup_private_session(self.reference)['cleanup_proven'])

    def test_legacy_record_without_new_fields_is_readable(self):
        owner, state, workspace = self.isolate_released()
        atomic_write_json(os.path.join(owner['root'], sessions.QUARANTINE), {
            'schema_version': 'crescendo.suite_quarantine.v1', 'root': owner['root'],
            'session_id': owner['session_id'], 'reason': 'old format', 'time': 1})
        self.assertEqual(self.row()['status'], 'released')

    def test_active_lease_keeps_isolation_and_allows_evidence_delete(self):
        sessions.detach_setup_private_session(self.reference, 'busy')
        row = self.row()
        self.assertEqual(row['status'], 'quarantined')
        self.assertIn('lease_active', [r['code'] for r in row['blocked_reasons']])
        self.assertTrue(row['can_delete'])
        result = sessions.delete_setup_private_session(self.reference)
        self.assertTrue(result['recycled'])
        self.assertEqual(set(result['retained']), {r['path'] for r in self.reference['views']})
        self.assertEqual(result['removed'], [])
        self.assertTrue(all(os.path.isdir(p) for p in result['retained']))
        self.assertEqual(self.row()['status'], 'deleted')
        with self.assertRaises(RuntimeError):
            sessions._controller(self.reference)._lock()

    def test_both_lock_kinds_retain_locked_views_and_receipt(self):
        owner, state, workspace = self.isolate_released()
        rows = workspace['same_library_registry']
        for name, row in zip(('master.cdslck', '.oa-lock'), rows):
            with open(os.path.join(row['path'], name), 'w') as stream:
                stream.write('owned by fixture editor')
        row = self.row()
        self.assertIn('oa_lock_held', [r['code'] for r in row['blocked_reasons']])
        result = quarantine.recycle(owner)
        self.assertTrue(result['recycled'])
        for name, member in zip(('master.cdslck', '.oa-lock'), rows):
            self.assertEqual(read_bytes(os.path.join(member['path'], name)).decode(), 'owned by fixture editor')
            self.assertIn(member['path'], result['retained'])
        self.assertEqual(set(result['removed']), {r['path'] for r in rows[2:]})
        self.assertTrue(all(not os.path.exists(p) for p in result['removed']))
        self.assertTrue(all(os.path.isdir(v['destination']) for v in result['views'] if v['action'] == 'moved'))
        path = os.path.join(result['recycle_root'], 'receipt.json')
        self.assertEqual(sessions._load(path), result)
        self.assertTrue(os.path.isfile(os.path.join(result['recycle_root'], 'records', sessions.QUARANTINE)))
        self.assertEqual(quarantine.recycle(owner), result)
        self.assertTrue(os.path.isfile(self.fixture.schematic))

    def test_marker_mismatch_retains_only_unproven_view(self):
        owner, state, workspace = self.isolate_released()
        member = workspace['same_library_registry'][0]
        atomic_write_json(os.path.join(member['path'], MARKER), {'token': 'someone-else', 'target': member['target']})
        row = self.row()
        self.assertIn('marker_mismatch', [r['code'] for r in row['blocked_reasons']])
        result = quarantine.recycle(owner)
        self.assertIn(member['path'], result['retained'])
        self.assertTrue(os.path.isdir(member['path']))
        self.assertEqual(len(result['removed']), len(workspace['same_library_registry']) - 1)

    def native_fixture(self):
        owner, state, workspace = self.isolate_released()
        control = os.path.join(owner['root'], 'operation')
        os.mkdir(control)
        path = os.path.join(control, guardian.STATE_NAME)
        script = os.path.realpath(os.path.join(os.path.dirname(__file__), '..', 'tools', 'ocean_worker_guardian.py'))
        argv = ['/bin/sleep', '30']
        guardian.write_legacy_guardian_manifest(control, owner['workspace_root'], path,
            os.path.join(control, 'ocean.stdout'), os.path.join(control, 'ocean.stderr'),
            'quarantine-fixture', argv, os.getpid(), script)
        atomic_write_json(path, {'schema_version': guardian.GUARDIAN_SCHEMA_VERSION,
            'cwd': owner['workspace_root'], 'argv': argv, 'token': 'quarantine-fixture',
            'expected_parent_pid': os.getpid(), 'guardian_pid': 9876, 'guardian_start_ticks': 111,
            'pid': None, 'status': 'running', 'boot_id': 'fixture-boot'})
        state['operations'] = [control]
        # Publish a coherent transaction just like the native controller.
        commit_path = os.path.join(owner['root'], 'suite_metadata_commit.json')
        commit = sessions._load(commit_path)
        commit['state'] = state
        atomic_write_json(commit_path, commit)
        atomic_write_json(os.path.join(owner['root'], 'session_state.json'), state)
        snapshot = {'alive': True, 'pid': 9876, 'pgrp': 9876, 'session': 9876,
            'start_ticks': 111, 'uid': os.getuid(), 'cwd': owner['workspace_root'],
            'argv': [sys.executable, script, '--state-path', path, '--token', 'quarantine-fixture',
                     '--cwd', owner['workspace_root'], '--expected-parent-pid', str(os.getpid())]}
        return owner, snapshot

    def test_native_process_alive_keeps_isolation_without_signals(self):
        owner, snapshot = self.native_fixture()
        with mock.patch.object(guardian, '_current_boot_id', return_value='fixture-boot'), \
                mock.patch.object(guardian, '_process_snapshot', side_effect=lambda pid, **kw: snapshot if pid == 9876 else {'alive': False}), \
                mock.patch.object(guardian.os, 'kill') as kill:
            row = self.row()
            self.assertEqual(row['status'], 'quarantined')
            self.assertIn('native_process_alive', [r['code'] for r in row['blocked_reasons']])
            kill.assert_not_called()

    def test_reused_pid_is_ambiguous_and_never_signalled(self):
        owner, snapshot = self.native_fixture()
        snapshot['start_ticks'] = 222
        with mock.patch.object(guardian, '_current_boot_id', return_value='fixture-boot'), \
                mock.patch.object(guardian, '_process_snapshot', side_effect=lambda pid, **kw: snapshot if pid == 9876 else {'alive': False}), \
                mock.patch.object(guardian.os, 'kill') as kill:
            row = self.row()
            self.assertEqual(row['status'], 'quarantined')
            self.assertIn('native_identity_unproven', [r['code'] for r in row['blocked_reasons']])
            result = quarantine.recycle(owner)
            self.assertEqual(result['removed'], [])
            kill.assert_not_called()

    def test_previous_boot_native_identity_releases(self):
        self.native_fixture()
        with mock.patch.object(guardian, '_current_boot_id', return_value='next-boot'):
            self.assertEqual(self.row()['status'], 'released')

    def test_unsafe_without_stop_evidence_stays_isolated(self):
        controller = sessions._controller(self.reference)
        controller.unsafe, controller.status = True, 'unsafe'
        controller.persist()
        sessions._release_lease(controller.lease)
        controller.lease, controller.closed = None, True
        row = self.row()
        self.assertEqual(row['status'], 'quarantined')
        self.assertIn('native_identity_unproven', [r['code'] for r in row['blocked_reasons']])

    def test_receipt_reconciles_crash_after_rename_and_retains_unvisited_members(self):
        owner, state, workspace = self.isolate_released()
        original = quarantine.os.rename
        def interrupted(source, target):
            original(source, target)
            raise KeyboardInterrupt('simulated process crash after rename')
        with mock.patch.object(quarantine.os, 'rename', side_effect=interrupted):
            with self.assertRaises(KeyboardInterrupt):
                quarantine.recycle(owner)
        result = quarantine.recycle(owner)
        self.assertTrue(result['complete'])
        self.assertEqual(len(result['removed']), 1)
        self.assertEqual(len(result['retained']), len(workspace['same_library_registry']) - 1)
        self.assertEqual({v['action'] for v in result['views']}, {'moved', 'retained'})
        self.assertTrue(all(os.path.isdir(path) for path in result['retained']))
        self.assertEqual(quarantine.recycle(owner), result)

    def test_cross_device_recycle_retains_original_and_reports_errno(self):
        import errno
        owner, state, workspace = self.isolate_released()
        with mock.patch.object(quarantine.os, 'rename', side_effect=OSError(errno.EXDEV, 'cross-device')):
            result = quarantine.recycle(owner)
        self.assertEqual(result['removed'], [])
        self.assertEqual(len(result['retained']), len(workspace['same_library_registry']))
        self.assertTrue(all(os.path.isdir(path) for path in result['retained']))
        self.assertTrue(all('cross-device' in v['reason'] for v in result['views']))

    def test_terminal_receipt_does_not_override_reused_pid(self):
        owner, snapshot = self.native_fixture()
        proof = {'state': 'dead', 'state_path': os.path.join(owner['root'], 'operation', guardian.STATE_NAME)}
        with mock.patch.object(guardian, 'inspect_legacy_guardian_attempt', return_value=proof), \
                mock.patch.object(guardian, '_current_boot_id', return_value='fixture-boot'), \
                mock.patch.object(guardian, '_process_stat', return_value={'start_ticks': 222, 'state': 'S'}):
            row = self.row()
        self.assertEqual(row['status'], 'quarantined')
        self.assertIn('native_identity_unproven', [r['code'] for r in row['blocked_reasons']])

    def test_unknown_capture_kind_remains_isolated(self):
        capture = os.path.join(self.config['runtime']['run_root'], 'bad-kind')
        os.mkdir(capture)
        sessions.write_session_reference(capture, self.reference)
        owner, state, workspace = self.isolate_released()
        state['capture_kinds'][capture] = 'unknown-kind'
        commit_path = os.path.join(owner['root'], 'suite_metadata_commit.json')
        commit = sessions._load(commit_path)
        commit['state'] = state
        atomic_write_json(commit_path, commit)
        atomic_write_json(os.path.join(owner['root'], 'session_state.json'), state)
        row = self.row()
        self.assertEqual(row['status'], 'quarantined')
        self.assertIn('native_identity_unproven', [r['code'] for r in row['blocked_reasons']])

    def test_receipt_write_failure_never_clears_original_quarantine(self):
        owner, state, workspace = self.isolate_released()
        path = os.path.join(owner['root'], sessions.QUARANTINE)
        before = read_bytes(path)
        with mock.patch.object(quarantine, 'atomic_write_json', side_effect=OSError('disk unavailable')):
            with self.assertRaises(OSError):
                quarantine.verify(owner)
        self.assertEqual(read_bytes(path), before)
        self.assertTrue(sessions._quarantine(owner))
        self.assertFalse(os.path.exists(os.path.join(owner['root'], quarantine.RECEIPT)))

    def test_service_startup_verifies_before_opening_list(self):
        self.isolate_released()
        from crescendo_engine.server import OptimizerService
        service = OptimizerService(self.config)
        self.assertEqual(service.startup_suite_checks[0]['status'], 'released')

    def test_service_evidence_delete_returns_retained_paths(self):
        sessions.detach_setup_private_session(self.reference, 'busy')
        from crescendo_engine.server import OptimizerService
        service = OptimizerService(self.config)
        result = service.delete_private_suite(self.reference['session_id'])
        self.assertTrue(result['recycled'])
        self.assertTrue(result['warnings'])
        self.assertEqual(service.list_private_suites()['suites'], [])

    def capture_fixture(self, kind=None):
        capture = os.path.join(self.config['runtime']['run_root'], 'path-evidence')
        os.mkdir(capture)
        sessions.write_session_reference(capture, self.reference, kind=kind)
        # Close while the evidence is valid; mutate only after releasing our lease.
        if kind == 'native_baseline':
            from crescendo_engine.native_baseline import SCHEMA_VERSION
            control = os.path.join(capture, 'native_baseline')
            os.mkdir(control)
            atomic_write_json(os.path.join(control, 'native_baseline_state.json'), {
                'schema_version': SCHEMA_VERSION, 'request_root': capture,
                'launch_attempted': False})
        owner, state, workspace = self.isolate_released()
        return capture, owner, state, workspace

    def assert_unsafe_capture_retains_views(self, owner, workspace):
        original = read_bytes(os.path.join(owner['root'], sessions.QUARANTINE))
        result = quarantine.verify(owner)
        self.assertFalse(result['passed'])
        self.assertIn('native_identity_unproven', [r['code'] for r in result['blocked_reasons']])
        row = self.row()
        self.assertEqual(row['status'], 'quarantined')
        self.assertFalse(row['can_load'])
        self.assertEqual(read_bytes(os.path.join(owner['root'], sessions.QUARANTINE)), original)
        self.assertFalse(os.path.lexists(os.path.join(owner['root'], quarantine.RECEIPT)))
        receipt = quarantine.recycle(owner)
        self.assertFalse(receipt['cleanup_proven'])
        self.assertEqual(receipt['removed'], [])
        self.assertEqual(set(receipt['retained']), {r['path'] for r in workspace['same_library_registry']})
        self.assertTrue(all(os.path.isdir(path) for path in receipt['retained']))

    def test_capture_phase_symlink_remains_isolated_and_retains_views(self):
        capture, owner, state, workspace = self.capture_fixture()
        destination = os.path.join(capture, 'other')
        os.mkdir(destination)
        phase = os.path.join(capture, 'baseline')
        os.symlink(destination, phase)
        from crescendo_engine.private_netlist import reconcile_preparation
        self.assertFalse(reconcile_preparation(capture, request_cancel=False)['cleanup_proven'])
        self.assert_unsafe_capture_retains_views(owner, workspace)
        self.assertTrue(os.path.islink(phase))

    def test_capture_phase_dangling_symlink_remains_isolated(self):
        capture, owner, state, workspace = self.capture_fixture()
        os.symlink(os.path.join(capture, 'absent'), os.path.join(capture, 'baseline'))
        self.assert_unsafe_capture_retains_views(owner, workspace)

    def test_capture_phase_file_remains_isolated(self):
        capture, owner, state, workspace = self.capture_fixture()
        from crescendo_engine.private_netlist import PREPARATION_PHASES
        for phase in PREPARATION_PHASES:
            with open(os.path.join(capture, phase), 'w') as stream:
                stream.write('invalid phase directory')
            with self.subTest(phase=phase):
                self.assertFalse(quarantine.verify(owner)['passed'])
            os.unlink(os.path.join(capture, phase))
        with open(os.path.join(capture, 'numeric'), 'w') as stream:
            stream.write('invalid phase directory')
        self.assert_unsafe_capture_retains_views(owner, workspace)

    def test_capture_root_symlink_remains_isolated(self):
        capture, owner, state, workspace = self.capture_fixture()
        os.rename(capture, capture + '-original')
        os.symlink(capture + '-original', capture)
        self.assert_unsafe_capture_retains_views(owner, workspace)

    def test_native_capture_control_symlink_remains_isolated(self):
        capture, owner, state, workspace = self.capture_fixture(kind='native_baseline')
        control = os.path.join(capture, 'native_baseline')
        os.rename(control, control + '-original')
        os.symlink(control + '-original', control)
        self.assert_unsafe_capture_retains_views(owner, workspace)

    def interrupt_service_recycle(self, service, boundary):
        # An interrupted update keeps the suite isolated without a live writer.
        # Otherwise the service's normal list check correctly releases it first.
        owner, state, workspace = sessions._read_suite(self.reference, deep=False, require_snapshot=False)
        state['refresh_pending'] = {'schema_version': 'crescendo.suite_refresh_pending.v1'}
        commit_path = os.path.join(owner['root'], 'suite_metadata_commit.json')
        commit = sessions._load(commit_path)
        commit['state'] = state
        atomic_write_json(commit_path, commit)
        atomic_write_json(os.path.join(owner['root'], 'session_state.json'), state)
        original_rename = quarantine.os.rename
        original_write = quarantine.atomic_write_json
        def rename(source, target):
            if boundary == 'before_rename':
                raise KeyboardInterrupt('crash before rename')
            original_rename(source, target)
            if boundary == 'after_rename':
                raise KeyboardInterrupt('crash after rename')
        def write(path, value):
            original_write(path, value)
            if (boundary == 'before_first_member' and path.endswith(quarantine.RECYCLE)):
                raise KeyboardInterrupt('crash after recycle pointer')
            if (boundary == 'before_complete' and path.endswith('receipt.json') and
                    value.get('views') and len(value['views']) == len(self.reference['views']) and
                    not value['complete'] and all(v['action'] == 'moved' for v in value['views'])):
                raise KeyboardInterrupt('crash after last member receipt')
        with mock.patch.object(quarantine.os, 'rename', side_effect=rename), \
                mock.patch.object(quarantine, 'atomic_write_json', side_effect=write):
            with self.assertRaises(KeyboardInterrupt):
                service.delete_private_suite(self.reference['session_id'])
        pointer = sessions._load(os.path.join(self.reference['root'], quarantine.RECYCLE))
        path = os.path.join(pointer['recycle_root'], 'receipt.json')
        self.assertFalse(sessions._load(path)['complete'])
        return path

    def assert_reconciled_receipt(self, path, moved):
        receipt = sessions._load(path)
        self.assertTrue(receipt['complete'])
        self.assertEqual(len(receipt['removed']), moved)
        self.assertEqual(len(receipt['retained']), len(self.reference['views']) - moved)
        self.assertEqual({v['path'] for v in receipt['views']}, {v['path'] for v in self.reference['views']})
        self.assertNotIn('move_pending', {v['action'] for v in receipt['views']})
        for entry in receipt['views']:
            if entry['action'] == 'moved':
                self.assertFalse(os.path.lexists(entry['path']))
                info = os.lstat(entry['destination'])
                self.assertEqual([info.st_dev, info.st_ino], entry['object_identity'])
            else:
                self.assertTrue(os.path.isdir(entry['path']))
        return receipt

    def test_service_startup_reconciles_crash_after_rename(self):
        from crescendo_engine.server import OptimizerService
        owner, state, workspace = self.isolate_released()
        service = OptimizerService(self.config)
        # Startup has safely released the suite; explicitly isolate for recycling.
        sessions.detach_setup_private_session(owner, 'test recycle interruption')
        path = self.interrupt_service_recycle(service, 'after_rename')
        restarted = OptimizerService(self.config)
        receipt = self.assert_reconciled_receipt(path, moved=1)
        self.assertTrue(restarted.startup_suite_checks[0]['recycle_complete'])
        before = read_bytes(path)
        self.assertEqual(restarted.list_private_suites()['suites'], [])
        self.assertEqual(read_bytes(path), before)
        self.assertEqual(restarted.delete_private_suite(owner['session_id'])['views'], receipt['views'])
        self.assertEqual(read_bytes(path), before)
        with self.assertRaises(ValueError):
            restarted._suite_reference_for_id(owner['session_id'])

    def test_service_refresh_reconciles_crash_before_rename(self):
        from crescendo_engine.server import OptimizerService
        owner, state, workspace = self.isolate_released()
        service = OptimizerService(self.config)
        sessions.detach_setup_private_session(owner, 'test recycle interruption')
        path = self.interrupt_service_recycle(service, 'before_rename')
        rows = service.list_private_suites()
        self.assertTrue(rows['deleted_suites'][0]['recycle_complete'])
        self.assert_reconciled_receipt(path, moved=0)
        before = read_bytes(path)
        service.list_private_suites()
        self.assertEqual(read_bytes(path), before)

    def test_service_delete_retry_reconciles_crash_before_first_member(self):
        from crescendo_engine.server import OptimizerService
        owner, state, workspace = self.isolate_released()
        service = OptimizerService(self.config)
        sessions.detach_setup_private_session(owner, 'test recycle interruption')
        path = self.interrupt_service_recycle(service, 'before_first_member')
        result = service.delete_private_suite(owner['session_id'])
        receipt = self.assert_reconciled_receipt(path, moved=0)
        self.assertEqual(result['views'], receipt['views'])
        self.assertTrue(result['complete'])

    def test_service_refresh_finishes_crash_after_last_member_receipt(self):
        from crescendo_engine.server import OptimizerService
        owner, state, workspace = self.isolate_released()
        service = OptimizerService(self.config)
        sessions.detach_setup_private_session(owner, 'test recycle interruption')
        path = self.interrupt_service_recycle(service, 'before_complete')
        rows = service.list_private_suites()
        self.assertTrue(rows['deleted_suites'][0]['recycle_complete'])
        self.assert_reconciled_receipt(path, moved=len(workspace['same_library_registry']))
        before = read_bytes(path)
        service.delete_private_suite(owner['session_id'])
        self.assertEqual(read_bytes(path), before)
