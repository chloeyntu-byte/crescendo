"""Ownership, recovery and lifetime fences for a UI's private OA suite."""
import copy
import contextlib
import json
import os
import stat
import threading
import unittest
from unittest import mock

from crescendo_engine import setup_private_session as session
from crescendo_engine.config_binding import private_config_plan
from crescendo_engine.private_netlist import reconcile_preparation
from crescendo_engine.private_view_access import (
    PrivateViewBusy, protect_private_views, write_private_views)
from crescendo_engine.same_library_views import MARKER, register_view
from crescendo_engine.utils import atomic_write_json
from tests import test_setup_baseline as fixtures


class SetupPrivateSessionTests(unittest.TestCase):
    def setUp(self):
        fixture = fixtures.SetupBaselineTests()
        fixture.setUp()
        self.addCleanup(fixture.temp.cleanup)
        fixture.freeze()
        self.fixture = fixture
        self.prepared, self.config = fixture.prepared, fixture.config
        self.prepared["setup"]["selected_test"] = self.prepared["setup"]["tests"][0]["name"]
        self.count = 0
        def native(controller, parameters, config, cancel_check):
            self.count += 1
            controller.binding_plan = private_config_plan(controller.workspace, [])
            for row in controller.workspace['same_library_registry']:
                if row['role'] == 'config' and not os.path.isdir(row['path']):
                    os.makedirs(row['path'])
                    atomic_write_json(os.path.join(row['path'], MARKER), {
                        'schema_version': 'crescendo.same_library_views.v1', 'token': row['token'], 'target': row['target']})
            controller.status = 'ready'
            controller.binding_verification = {'ok': True, 'native_run_invoked': False}
            controller.view_fingerprints = controller.view_identity()
            controller.persist()
        self.native = native
        self.patcher = mock.patch.object(session._PrivateSession, '_native_bind', native)
        self.patcher.start()
        self.addCleanup(self.patcher.stop)
        self.reference = session.create_setup_private_session(self.prepared, self.config)
        self.prepared['setup_private_session'] = self.reference
        self.addCleanup(self.cleanup)

    def cleanup(self):
        session.close_setup_private_session(self.reference)
        with session._REGISTRY_LOCK:
            session._SESSIONS.pop(self.reference['session_id'], None)

    def capture(self, name='candidate'):
        root = os.path.join(self.config['runtime']['run_root'], name)
        os.makedirs(root)
        session.write_session_reference(root, self.reference)
        return root

    def assert_private_access(self, writable):
        controller = session._controller(self.reference)
        for row in controller.workspace['same_library_registry']:
            if not os.path.isdir(row['path']):
                continue
            for current, dirs, files in os.walk(row['path']):
                mode = stat.S_IMODE(os.stat(current).st_mode)
                if writable:
                    self.assertTrue(mode & 0o200, current)
                    self.assertTrue(mode & 0o100, current)
                else:
                    self.assertEqual(mode & 0o222, 0, current)
                for name in files:
                    path = os.path.join(current, name)
                    mode = stat.S_IMODE(os.stat(path).st_mode)
                    if writable:
                        self.assertTrue(mode & 0o200, path)
                    else:
                        self.assertEqual(mode & 0o222, 0, path)

    @contextlib.contextmanager
    def external_edit_lock(self, path):
        """Model an editor that acquired a lock while the view was writable."""
        controller = session._controller(self.reference)
        lock = os.path.join(path, 'external.cdslck')
        with write_private_views(controller.workspace, restore_when=lambda: False):
            with open(lock, 'w') as stream:
                stream.write('unqualified external edit lock')
        try:
            yield lock
        finally:
            # Only the simulated external editor releases its own lock.
            if os.path.exists(lock):
                os.unlink(lock)
            if not controller.closed:
                protect_private_views(controller.workspace)

    def test_design_root_survives_run_root_change_and_discovers_legacy(self):
        config = copy.deepcopy(self.config)
        design_root = os.path.join(self.fixture.root, 'stable_design')
        config['runtime']['design_root'] = design_root
        newer = session.create_setup_private_session(self.prepared, config, label='stable')
        self.addCleanup(session.close_setup_private_session, newer)
        self.assertEqual(os.path.dirname(os.path.dirname(newer['root'])), design_root)
        listed = session.list_setup_private_sessions(config)
        self.assertEqual({row['suite_id'] for row in listed},
                         {newer['session_id'], self.reference['session_id']})
        self.assertTrue(session.close_setup_private_session(newer)['cleanup_proven'])
        changed = copy.deepcopy(config)
        changed['runtime']['run_root'] = os.path.join(self.fixture.root, 'other_run')
        rows = session.list_setup_private_sessions(changed)
        self.assertEqual([row['suite_id'] for row in rows], [newer['session_id']])
        reopened = session.reopen_setup_private_session(rows[0], changed)
        self.assertEqual(reopened['session_id'], newer['session_id'])
        self.assertTrue(session.delete_setup_private_session(reopened)['cleanup_proven'])
        self.assertTrue(os.path.isfile(self.fixture.schematic))

    def test_durable_release_reopen_and_cross_process_lease(self):
        import subprocess
        import sys
        rows = session.list_setup_private_sessions(self.config)
        self.assertTrue(rows[0]['busy'])
        command = ('from crescendo_engine.setup_private_session import _acquire_lease; '
                   '_acquire_lease(%r)' % self.reference['root'])
        child = subprocess.run([sys.executable, '-c', command], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.assertNotEqual(child.returncode, 0)
        before = session.setup_private_session_snapshot(self.reference)
        result = session.close_setup_private_session(self.reference)
        self.assertTrue(result['cleanup_proven'])
        self.assertFalse(result['warnings'])
        self.assertFalse(session.list_setup_private_sessions(self.config)[0]['busy'])
        reopened = session.reopen_setup_private_session(self.reference, self.config)
        self.assertEqual(reopened['suite_id'], self.reference['session_id'])
        self.assertEqual(reopened['prepared'], before)
        self.assert_private_access(False)

    def test_refresh_preserves_registered_identities(self):
        before = copy.deepcopy(self.reference['views'])
        info = session.refresh_setup_private_session(self.reference, self.prepared, self.config)
        self.assertEqual(before, info['views'])
        self.assertEqual(self.count, 2)
        self.assert_private_access(False)
        self.assertIsNone(session._controller(self.reference).refresh_pending)

    def test_failed_refresh_restores_exact_views_and_snapshot(self):
        controller = session._controller(self.reference)
        before = session.setup_private_session_snapshot(self.reference)
        path = controller.workspace['setup_sdb']
        with open(path, 'rb') as stream:
            initial = stream.read()
        def fail(current, parameters, config, cancel_check):
            with open(current.workspace['setup_sdb'], 'w') as stream:
                stream.write('partial refresh')
            raise RuntimeError('refresh failed')
        with mock.patch.object(session._PrivateSession, '_native_bind', fail):
            with self.assertRaisesRegex(RuntimeError, 'refresh failed'):
                session.refresh_setup_private_session(self.reference, self.prepared, self.config)
        with open(path, 'rb') as stream:
            self.assertEqual(stream.read(), initial)
        self.assertEqual(session.setup_private_session_snapshot(self.reference), before)
        self.assertFalse(controller.unsafe)
        self.assertIsNone(controller.refresh_pending)
        self.assert_private_access(False)

    def test_delete_removes_only_exact_registered_views(self):
        paths = [row['path'] for row in self.reference['views']]
        result = session.delete_setup_private_session(self.reference)
        self.assertTrue(result['cleanup_proven'])
        self.assertEqual(set(result['removed']), set(paths))
        self.assertTrue(all(not os.path.exists(path) for path in paths))
        self.assertTrue(os.path.exists(self.fixture.schematic))
        self.assertEqual(session.list_setup_private_sessions(self.config)[0]['status'], 'deleted')

    def test_delete_partially_created_suite_checks_and_removes_remaining_owned_views(self):
        import shutil
        paths = [row['path'] for row in self.reference['views']]
        session.close_setup_private_session(self.reference)
        shutil.rmtree(paths[0])
        with self.assertRaisesRegex(ValueError, 'registered view is missing'):
            session.reopen_setup_private_session(self.reference, self.config)
        deletion = session.reopen_setup_private_session(self.reference, self.config, for_delete=True)
        result = session.delete_setup_private_session(deletion)
        self.assertTrue(result['cleanup_proven'])
        self.assertEqual(set(result['removed']), set(paths[1:]))
        self.assertTrue(all(not os.path.lexists(path) for path in paths))
        self.assertTrue(os.path.exists(self.fixture.schematic))

    def test_rename_rebuilds_views_and_retires_old_suite(self):
        paths = [row['path'] for row in self.reference['views']]
        renamed = session.rename_setup_private_session(self.reference, 'new_name', self.config)
        self.addCleanup(session.close_setup_private_session, renamed)
        self.assertEqual(renamed['label'], 'new_name')
        self.assertNotEqual(renamed['session_id'], self.reference['session_id'])
        self.assertTrue(all('new_name' in row['target'][2] for row in renamed['views']))
        self.assertTrue(all(not os.path.exists(path) for path in paths))
        self.assertTrue(all(os.path.isdir(row['path']) for row in renamed['views']))

    def test_legacy_suite_is_visible_and_can_only_be_opened_for_delete(self):
        session.close_setup_private_session(self.reference)
        state_path = os.path.join(self.reference['root'], 'session_state.json')
        with open(state_path) as stream:
            state = json.load(stream)
        state.pop('prepared_hash', None)
        state.pop('prepared', None)
        atomic_write_json(state_path, state)
        item = session.list_setup_private_sessions(self.config)[0]
        self.assertFalse(item['can_load'])
        self.assertTrue(item['can_delete'])
        self.assertEqual(len(item['views']), len(self.reference['views']))
        with self.assertRaisesRegex(ValueError, 'no persisted inspected snapshot'):
            session.reopen_setup_private_session(self.reference, self.config)
        legacy = session.reopen_setup_private_session(self.reference, self.config, for_delete=True)
        result = session.delete_setup_private_session(legacy)
        self.assertTrue(result['cleanup_proven'])

    def test_tampered_registry_cannot_reopen(self):
        session.close_setup_private_session(self.reference)
        path = os.path.join(self.reference['workspace_root'], 'same_library_views.json')
        with open(path) as stream:
            data = json.load(stream)
        data['views'][0]['path'] += '-other'
        atomic_write_json(path, data)
        with self.assertRaisesRegex(ValueError, 'registry differs'):
            session.reopen_setup_private_session(self.reference, self.config)
        self.assertEqual(session.list_setup_private_sessions(self.config)[0]['status'], 'unavailable')

    def test_load_creates_real_file_view_and_all_configs_before_parameters(self):
        info = session.private_session_info(self.reference)
        self.assertEqual(info['baseline_id'], self.prepared['setup']['baseline']['baseline_id'])
        self.assertTrue(os.path.isfile(os.path.join(info['maestro']['path'], 'maestro.sdb')))
        self.assertEqual(len(info['configs']), 1)
        self.assertTrue(os.path.isdir(info['configs'][0]['path']))
        self.assertFalse(info['binding_verification']['native_run_invoked'])
        identity = session._controller(self.reference).workspace['source_identity']
        self.assertEqual(info['source_identity'], {key: identity[key] for key in ('schema_version', 'fingerprint')})
        self.assertNotIn('libraries', info['source_identity'])
        self.assert_private_access(False)

    def test_borrow_enables_private_writes_then_restores_idle_guard(self):
        controller = session._controller(self.reference)
        before = copy.deepcopy(self.prepared)
        with session.borrow_setup_private_session(self.prepared, self.config, 'not-created'):
            self.assert_private_access(True)
            with open(controller.workspace['setup_sdb'], 'w') as stream:
                stream.write(self.fixture.xml)
        self.assert_private_access(False)
        self.assertEqual(self.prepared, before)
        self.assertEqual(controller.status, 'ready')
        self.assertFalse(controller.unsafe)

    def test_apply_enables_native_writes_then_restores_idle_guard(self):
        controller = session._controller(self.reference)
        def native(current, parameters, config, cancel_check):
            self.assertIs(current, controller)
            self.assert_private_access(True)
            self.native(current, parameters, config, cancel_check)
        with mock.patch.object(session._PrivateSession, '_native_bind', native):
            info = session.apply_setup_private_cells(self.reference, self.prepared, self.config)
        self.assertEqual(info['status'], 'ready')
        self.assert_private_access(False)

    def test_apply_publishes_and_persists_latest_native_corner_masks(self):
        test = self.prepared['setup']['selected_test']
        self.prepared['setup']['test_corner_masks'] = {
            test: {'known': False, 'disabled_corners': [], 'nominal_enabled': None}}
        actual = {test: {'known': True, 'disabled_corners': ['slow'],
                         'nominal_enabled': False, 'source': 'native_setupdb_apis'}}
        def native(current, parameters, config, cancel_check):
            current.prepared['setup']['test_corner_masks'] = copy.deepcopy(actual)
            current.workspace['setup']['test_corner_masks'] = copy.deepcopy(actual)
            self.native(current, parameters, config, cancel_check)
        with mock.patch.object(session._PrivateSession, '_native_bind', native):
            info = session.apply_setup_private_cells(self.reference, self.prepared, self.config)
        self.assertEqual(info['test_corner_masks'], actual)
        self.assertEqual(session.setup_private_session_snapshot(self.reference)['setup']['test_corner_masks'], actual)
        self.assertFalse(self.prepared['setup']['test_corner_masks'][test]['known'])
        self.assertTrue(session.close_setup_private_session(self.reference)['cleanup_proven'])
        reopened = session.reopen_setup_private_session(self.reference, self.config)
        self.assertEqual(reopened['prepared']['setup']['test_corner_masks'], actual)

    def test_interrupted_apply_does_not_reopen_as_ready_and_can_be_deleted(self):
        def interrupted(current, parameters, config, cancel_check):
            state = session._load(os.path.join(current.owner['root'], 'session_state.json'), max_bytes=128 * 1024 * 1024)
            self.assertEqual(state['status'], 'applying')
            raise SystemExit('simulate interrupted Apply')
        with mock.patch.object(session._PrivateSession, '_native_bind', interrupted):
            with self.assertRaisesRegex(SystemExit, 'interrupted Apply'):
                session.apply_setup_private_cells(self.reference, self.prepared, self.config)
        self.assertTrue(session.close_setup_private_session(self.reference)['cleanup_proven'])
        with self.assertRaisesRegex(ValueError, 'incomplete setup'):
            session.reopen_setup_private_session(self.reference, self.config)
        reopened = session.reopen_setup_private_session(self.reference, self.config, for_delete=True)
        self.assertTrue(session.delete_setup_private_session(reopened)['cleanup_proven'])

    def test_failed_apply_remains_ineligible_after_release(self):
        with mock.patch.object(session._PrivateSession, '_native_bind', side_effect=RuntimeError('failed')):
            with self.assertRaisesRegex(RuntimeError, 'failed'):
                session.apply_setup_private_cells(self.reference, self.prepared, self.config)
        self.assertTrue(session.close_setup_private_session(self.reference)['cleanup_proven'])
        with self.assertRaisesRegex(ValueError, 'incomplete setup'):
            session.reopen_setup_private_session(self.reference, self.config)
        reopened = session.reopen_setup_private_session(self.reference, self.config, for_delete=True)
        self.assertTrue(session.delete_setup_private_session(reopened)['cleanup_proven'])

    def test_stopped_native_borrow_exception_restores_idle_permissions(self):
        controller = session._controller(self.reference)
        root = self.capture()
        with mock.patch('crescendo_engine.private_netlist.reconcile_preparation',
                        return_value={'cleanup_proven': True}):
            with self.assertRaisesRegex(RuntimeError, 'native fixture stopped'):
                with session.borrow_setup_private_session(self.prepared, self.config, root):
                    self.assert_private_access(True)
                    raise RuntimeError('native fixture stopped')
        self.assert_private_access(False)
        self.assertEqual(controller.status, 'ready')
        self.assertFalse(controller.unsafe)
        self.assertTrue(controller.private_views_read_only)

    def test_stopped_apply_exception_restores_idle_permissions(self):
        def stopped_native(current, parameters, config, cancel_check):
            self.assert_private_access(True)
            raise RuntimeError('native fixture stopped')
        with mock.patch.object(session._PrivateSession, '_native_bind', stopped_native):
            with self.assertRaisesRegex(RuntimeError, 'native fixture stopped'):
                session.apply_setup_private_cells(self.reference, self.prepared, self.config)
        self.assert_private_access(False)
        self.assertFalse(session._controller(self.reference).unsafe)
        self.assertTrue(session._controller(self.reference).private_views_read_only)

    def test_unproven_apply_native_stop_retains_writes_and_blocks_reuse(self):
        controller = session._controller(self.reference)
        def unproven_native(current, parameters, config, cancel_check):
            self.assert_private_access(True)
            current.unsafe = True
            raise RuntimeError('native stop unproven')
        with mock.patch.object(session._PrivateSession, '_native_bind', unproven_native):
            with self.assertRaisesRegex(RuntimeError, 'native stop unproven'):
                session.apply_setup_private_cells(self.reference, self.prepared, self.config)
        self.assertTrue(controller.unsafe)
        self.assert_private_access(True)
        self.assertFalse(controller.private_views_read_only)
        with self.assertRaisesRegex(RuntimeError, 'unsafe'):
            with session.borrow_setup_private_session(self.prepared, self.config, 'not-created'):
                self.fail('Unsafe native writer was reused')

    def test_capture_stop_proof_exception_never_reports_readonly(self):
        controller = session._controller(self.reference)
        root = self.capture()
        with mock.patch('crescendo_engine.private_netlist.reconcile_preparation',
                        side_effect=RuntimeError('stop proof unavailable')):
            with self.assertRaisesRegex(RuntimeError, 'stop proof unavailable'):
                with session.borrow_setup_private_session(self.prepared, self.config, root):
                    self.assert_private_access(True)
        self.assertTrue(controller.unsafe)
        self.assertFalse(controller.private_views_read_only)
        self.assert_private_access(True)

    def test_native_baseline_reconciler_requires_stop_proof(self):
        controller = session._controller(self.reference)
        root = os.path.join(self.fixture.root, 'baseline-candidate')
        reconciler = mock.Mock(return_value={'cleanup_proven': False})
        with self.assertRaisesRegex(RuntimeError, 'native stop is unproven'):
            with session.borrow_setup_private_session(self.prepared, self.config, root,
                                                      completion_reconciler=reconciler):
                self.assert_private_access(True)
        reconciler.assert_called_once_with(root, request_cancel=True, timeout_seconds=12)
        self.assertTrue(controller.unsafe)
        self.assert_private_access(True)
        self.assertFalse(session.close_setup_private_session(self.reference)['cleanup_proven'])
        with self.assertRaisesRegex(RuntimeError, 'unsafe'):
            with session.borrow_setup_private_session(self.prepared, self.config, root):
                self.fail('Native stop was not proven')

    def test_native_baseline_reconciler_proven_stop_restores_idle_permissions(self):
        reconciler = mock.Mock(return_value={'cleanup_proven': True})
        phases = {}
        with session.borrow_setup_private_session(self.prepared, self.config, 'baseline-candidate',
                                                  completion_reconciler=reconciler, phase_timings=phases):
            self.assert_private_access(True)
            for name in ('shared_setup_wait', 'shared_setup'):
                self.assertTrue(phases[name]['completed'])
                self.assertGreaterEqual(phases[name]['elapsed_seconds'], 0)
        self.assert_private_access(False)
        self.assertFalse(session._controller(self.reference).unsafe)

    def test_native_reconciliation_failure_preserves_original_error(self):
        original = RuntimeError('original native failure')
        original.detail = {'failure_stage': 'native_run', 'cleanup_pending': False}
        reconciler = mock.Mock(return_value={'cleanup_proven': False})
        with self.assertRaisesRegex(RuntimeError, 'original native failure') as raised:
            with session.borrow_setup_private_session(self.prepared, self.config, 'baseline-candidate',
                                                      completion_reconciler=reconciler):
                raise original
        self.assertIs(raised.exception, original)
        self.assertTrue(original.detail['cleanup_pending'])
        self.assertIn('native stop is unproven', original.detail['reconciliation_error'])
        self.assertTrue(session._controller(self.reference).unsafe)

    def test_durable_baseline_capture_uses_its_own_reconciler(self):
        root = os.path.join(self.config['runtime']['run_root'], 'baseline')
        os.makedirs(root)
        session.write_session_reference(root, self.reference, kind='native_baseline')
        with mock.patch('crescendo_engine.native_baseline.reconcile_native_baseline',
                        return_value={'cleanup_proven': True}) as reconcile, \
                mock.patch('crescendo_engine.private_netlist.reconcile_preparation') as cli:
            self.assertTrue(session.close_setup_private_session(self.reference)['cleanup_proven'])
            reconcile.assert_called_once()
            cli.assert_not_called()

    def test_missing_baseline_root_requires_durable_stop_proof(self):
        import shutil
        root = os.path.join(self.config['runtime']['run_root'], 'baseline-missing')
        os.makedirs(root)
        session.write_session_reference(root, self.reference, kind='native_baseline')
        shutil.rmtree(root)
        self.assertFalse(session.close_setup_private_session(self.reference)['cleanup_proven'])
        controller = session._controller(self.reference)
        controller.capture_proofs[root] = {'cleanup_proven': True, 'native_session_closed': True}
        controller.persist()
        self.assertTrue(session.close_setup_private_session(self.reference)['cleanup_proven'])
        reopened = session.reopen_setup_private_session(self.reference, self.config)
        self.assertEqual(reopened['status'], 'ready')

    def test_baseline_borrow_persists_stop_proof_before_release(self):
        root = os.path.join(self.config['runtime']['run_root'], 'baseline-proven')
        os.makedirs(root)
        session.write_session_reference(root, self.reference, kind='native_baseline')
        proof = {'cleanup_proven': True, 'native_session_closed': True}
        with session.borrow_setup_private_session(self.prepared, self.config, root,
                completion_reconciler=mock.Mock(return_value=proof)):
            pass
        state = session._load(os.path.join(self.reference['root'], 'session_state.json'), max_bytes=128 * 1024 * 1024)
        self.assertEqual(state['capture_proofs'][root], proof)
        import shutil
        shutil.rmtree(root)

    def test_idle_protection_failure_never_reports_readonly(self):
        controller = session._controller(self.reference)
        with mock.patch('crescendo_engine.private_view_access.protect_private_views',
                        side_effect=OSError('idle protection unavailable')):
            with self.assertRaisesRegex(OSError, 'idle protection unavailable'):
                with session.borrow_setup_private_session(self.prepared, self.config, 'not-created'):
                    self.assert_private_access(True)
        self.assertFalse(controller.private_views_read_only)
        self.assert_private_access(True)

    def test_run_cleanup_reconciles_capture_but_retains_exact_session_views(self):
        root = self.capture()
        paths = [row['path'] for row in self.reference['views']]
        proof = reconcile_preparation(root)
        self.assertTrue(proof['cleanup_proven'])
        self.assertEqual(proof['setup_private_session']['view_cleanup'], 'retained_until_ui_close')
        self.assertTrue(all(os.path.isdir(path) for path in paths))
        closed = session.close_setup_private_session(self.reference)
        self.assertTrue(closed['cleanup_proven'])
        self.assertTrue(all(os.path.isdir(path) for path in paths))
        for path in paths:
            self.assertTrue(os.stat(path).st_mode & stat.S_IWUSR)
        self.assertTrue(os.path.isfile(self.fixture.schematic))
        self.assertTrue(os.path.isfile(self.fixture.path))
        self.assertTrue(session.close_setup_private_session(self.reference)['cleanup_proven'])

    def test_wrong_owner_and_unregistered_capture_cannot_claim_retention(self):
        root = self.capture()
        reference_path = os.path.join(root, session.REFERENCE)
        with open(reference_path) as stream:
            reference = json.load(stream)
        reference['workspace_root'] = self.fixture.library
        atomic_write_json(reference_path, reference)
        self.assertFalse(reconcile_preparation(root)['cleanup_proven'])
        os.unlink(reference_path)
        reference = dict(self.reference, capture_root=root)
        atomic_write_json(reference_path, reference)
        controller = session._controller(self.reference)
        controller.capture_roots.remove(root)
        controller.persist()
        self.assertFalse(reconcile_preparation(root)['cleanup_proven'])

    def test_marker_link_does_not_grant_access_to_an_external_session(self):
        root = self.capture()
        reference_path = os.path.join(root, session.REFERENCE)
        os.rename(reference_path, reference_path + '.original')
        os.symlink(reference_path + '.original', reference_path)
        self.assertFalse(reconcile_preparation(root)['cleanup_proven'])
        os.unlink(reference_path)
        os.rename(reference_path + '.original', reference_path)

    def test_new_borrow_restores_original_maestro_but_reuses_config_identity(self):
        controller = session._controller(self.reference)
        path = controller.workspace['setup_sdb']
        with write_private_views(controller.workspace):
            with open(path, 'w') as stream:
                stream.write(self.fixture.xml.replace('<value>3</value>', '<value>99</value>'))
        controller.workspace['native_parameter_paths'] = ['owned/old/path']
        root = os.path.join(self.config['runtime']['run_root'], 'borrow')
        with session.borrow_setup_private_session(self.prepared, self.config, root) as workspace:
            with open(path) as stream:
                self.assertIn('<value>3</value>', stream.read())
            self.assertNotIn('native_parameter_paths', workspace)
            self.assertEqual(workspace['setup_dir'], self.reference['maestro']['path'])
            self.assertEqual(session.private_session_info(self.reference)['configs'], self.reference['configs'])
        with open(self.fixture.path) as stream:
            self.assertEqual(stream.read(), self.fixture.xml)

    def test_unproven_capture_prevents_reuse_and_final_view_removal(self):
        root = self.capture()
        with mock.patch('crescendo_engine.private_netlist.reconcile_preparation', return_value={'cleanup_proven': False}):
            with session.borrow_setup_private_session(self.prepared, self.config, root):
                pass
            self.assertTrue(session.private_session_info(self.reference)['unsafe'])
            self.assert_private_access(True)
            self.assertFalse(session._controller(self.reference).private_views_read_only)
            with self.assertRaisesRegex(RuntimeError, 'unsafe'):
                with session.borrow_setup_private_session(self.prepared, self.config, root):
                    self.fail('unsafe suite was borrowed')
            result = session.close_setup_private_session(self.reference)
            self.assertFalse(result['cleanup_proven'])
        self.assertTrue(os.path.isdir(self.reference['maestro']['path']))

    def test_close_during_generation_and_locked_private_maestro_are_retained(self):
        controller = session._controller(self.reference)
        controller.lock.acquire()
        try:
            self.assertFalse(session.close_setup_private_session(self.reference, timeout_seconds=0)['cleanup_proven'])
        finally:
            controller.lock.release()
        with self.external_edit_lock(self.reference['maestro']['path']) as lock:
            with self.assertRaises(PrivateViewBusy):
                with session.borrow_setup_private_session(self.prepared, self.config, 'not-created'):
                    self.fail('Locked private Maestro was borrowed')
            released = session.close_setup_private_session(self.reference)
            self.assertTrue(released['cleanup_proven'])
            self.assertTrue(released['warnings'])
            self.assertTrue(os.path.isfile(lock))
            self.assertEqual(controller.status, 'released')
            self.assertFalse(controller.unsafe)
        self.assertTrue(session.close_setup_private_session(self.reference)['cleanup_proven'])

    def test_every_private_view_lock_is_retryable_without_invalidating_settings(self):
        controller = session._controller(self.reference)
        # The basic setup fixture has no selected devices. Include an owned
        # private schematic too so the guard covers all three view roles.
        with write_private_views(controller.workspace):
            working = register_view(controller.workspace, ('design', 'TB', 'schematic'),
                                    ('private-schematic-fixture',), role='working')
            os.mkdir(working['path'])
            atomic_write_json(os.path.join(working['path'], MARKER), {
                'token': working['token'], 'target': working['target']})
            with open(os.path.join(working['path'], 'sch.oa'), 'w') as stream:
                stream.write('private schematic fixture')
            controller.view_fingerprints = controller.view_identity()
        before = copy.deepcopy(self.prepared)
        selection = copy.deepcopy(controller.applied_selection)
        fingerprints = copy.deepcopy(controller.view_fingerprints)
        for row in controller.workspace['same_library_registry']:
            with self.subTest(role=row['role']):
                with self.external_edit_lock(row['path']) as lock:
                    for action in ('borrow', 'apply'):
                        with self.subTest(action=action):
                            with self.assertRaises(PrivateViewBusy) as error:
                                if action == 'borrow':
                                    with session.borrow_setup_private_session(self.prepared, self.config, 'not-created'):
                                        self.fail('Locked private view was borrowed')
                                else:
                                    session.apply_setup_private_cells(self.reference, self.prepared, self.config)
                            self.assertTrue(error.exception.retryable)
                            self.assertEqual(error.exception.lock_paths, [lock])
                            self.assertIn(row['path'], error.exception.view_paths)
                            self.assertEqual(controller.status, 'ready')
                            self.assertFalse(controller.unsafe)
                            self.assertEqual(self.prepared, before)
                            self.assertEqual(controller.applied_selection, selection)
                            self.assertEqual(controller.view_fingerprints, fingerprints)
                            self.assertTrue(os.path.isfile(lock))
                with session.borrow_setup_private_session(self.prepared, self.config, 'not-created'):
                    self.assert_private_access(True)
                self.assert_private_access(False)
                self.assertEqual(controller.status, 'ready')

    def test_cancelled_wait_does_not_acquire_or_mutate_suite(self):
        controller = session._controller(self.reference)
        controller.lock.acquire()
        try:
            with self.assertRaisesRegex(RuntimeError, 'cancelled while waiting'):
                with session.borrow_setup_private_session(self.prepared, self.config, 'not-created', lambda: True):
                    pass
        finally:
            controller.lock.release()

    def test_history_removal_changes_only_copied_metadata(self):
        controller = session._controller(self.reference)
        path = controller.workspace['setup_sdb']
        with write_private_views(controller.workspace):
            with open(path, 'w') as stream:
                stream.write('<setupdb><active><historyentry><path>/external/psf</path></historyentry>'
                             '<vars><var Name="keep"/></vars></active></setupdb>')
            self.assertEqual(session._clear_inherited_history(controller.workspace), 1)
        with open(path) as stream:
            text = stream.read()
        self.assertNotIn('historyentry', text)
        self.assertIn('Name="keep"', text)
        with open(self.fixture.path) as stream:
            self.assertEqual(stream.read(), self.fixture.xml)

    def test_headless_provider_has_no_session_or_new_ownership_requirement(self):
        with session.borrow_setup_private_session({}, {}, 'not-created') as workspace:
            self.assertIsNone(workspace)

    def test_changed_private_config_blocks_borrow_until_reapplied(self):
        path = os.path.join(self.reference['configs'][0]['path'], 'expand.cfg')
        with write_private_views(session._controller(self.reference).workspace):
            with open(path, 'w') as stream:
                stream.write('externally changed binding')
        with self.assertRaisesRegex(ValueError, 'Private schematic/config changed'):
            with session.borrow_setup_private_session(self.prepared, self.config, 'not-created'):
                pass

    def test_source_drift_blocks_before_native_reset_and_keeps_compact_evidence(self):
        controller = session._controller(self.reference)
        with open(self.fixture.schematic, 'w') as stream:
            stream.write('saved changed electrical source')
        with mock.patch.object(controller, 'reset_maestro', side_effect=AssertionError('must not run')):
            with self.assertRaisesRegex(ValueError, 'source dependencies changed') as caught:
                with session.borrow_setup_private_session(self.prepared, self.config, 'not-created'):
                    self.fail('Changed source was allowed to enter native execution')
        detail = caught.exception.detail
        self.assertEqual(detail['failure_stage'], 'source_freshness')
        self.assertFalse(detail['execution_started'])
        self.assertFalse(detail['native_run_invoked'])
        guard = detail['source_guard']
        self.assertFalse(guard['ok'])
        self.assertEqual(guard['schema_version'], 'crescendo.source_guard_diagnostic.v1')
        self.assertEqual(guard['changed_libraries'], 1)
        files = [file_row for scope in guard['libraries'][0]['changes']['scopes']
                 for file_row in scope['file_changes']['files']]
        self.assertTrue(any(row['path'].endswith('/schematic/sch.oa') for row in files))
        self.assertLess(len(json.dumps(detail)), 12000)
        self.assert_private_access(False)

    def test_interrupted_restore_keeps_old_view_and_registered_staging_copy(self):
        controller = session._controller(self.reference)
        original = controller.workspace['setup_sdb']
        with open(original) as stream:
            before = stream.read()
        with mock.patch('crescendo_engine.workspace._copy_private_file', side_effect=RuntimeError('copy interrupted')):
            with self.assertRaisesRegex(RuntimeError, 'copy interrupted'):
                with write_private_views(controller.workspace):
                    controller.reset_maestro(self.prepared, self.config)
        with open(original) as stream:
            self.assertEqual(stream.read(), before)
        self.assertEqual(len(controller.restore_pending), 1)
        pending = controller.restore_pending[0]
        self.assertEqual(os.path.dirname(pending['stage']), os.path.dirname(pending['live']))
        self.assertTrue(os.path.isdir(pending['stage']))
        self.assertTrue(os.path.isfile(os.path.join(pending['stage'], MARKER)))

    def test_failed_swap_restores_old_view_without_deleting_staging_backup(self):
        controller = session._controller(self.reference)
        rename = os.rename
        def interrupted(source, target):
            if '__restore_' in source:
                raise OSError('swap interrupted')
            return rename(source, target)
        with mock.patch('crescendo_engine.setup_private_session.os.rename', side_effect=interrupted):
            with self.assertRaisesRegex(OSError, 'swap interrupted'):
                with write_private_views(controller.workspace):
                    controller.reset_maestro(self.prepared, self.config)
        self.assertTrue(os.path.isfile(controller.workspace['setup_sdb']))
        pending = controller.restore_pending[0]
        self.assertTrue(os.path.isdir(pending['stage']))
        self.assertFalse(os.path.exists(pending['previous']))

    def test_netlist_template_borrows_ready_suite_without_recloning_baseline(self):
        from crescendo_engine.private_netlist import prepare_private_netlist
        controller = session._controller(self.reference)
        root = os.path.join(self.config['runtime']['run_root'], 'template')
        parameter = {'parameter_id': 'p', 'backend': 'schematic'}
        with mock.patch('crescendo_engine.private_netlist._qualify_setup_capture'), \
             mock.patch('crescendo_engine.simulators.execution_contract'), \
             mock.patch('crescendo_engine.netlist_execution.execution_policy', return_value={'global': {}, 'tests': {}}), \
             mock.patch('crescendo_engine.private_netlist.build_workspace') as build, \
             mock.patch('crescendo_engine.private_netlist.capture_unmodified_baseline', side_effect=RuntimeError('baseline boundary')) as capture:
            with self.assertRaisesRegex(RuntimeError, 'baseline boundary'):
                prepare_private_netlist(self.prepared, [parameter], self.config, root)
        build.assert_not_called()
        self.assertIs(capture.call_args[0][0], controller.workspace)
        self.assertFalse(capture.call_args[1]['emit_clones'])
        self.assertEqual(session.preparation_session_reference(root)['session_id'], self.reference['session_id'])

    def test_frozen_template_input_survives_next_session_generation(self):
        from crescendo_engine.netlist_providers import _freeze_generated_dependencies
        controller = session._controller(self.reference)
        source = os.path.join(controller.workspace['root'], 'generated_model.scs')
        with open(source, 'w') as stream:
            stream.write('parameters generated_value=17\n')
        cwd = os.path.join(self.config['runtime']['run_root'], 'frozen_template')
        os.makedirs(cwd)
        task = {'cwd': cwd, 'native_cwd': controller.workspace['root']}
        text = 'include "cr_candidate_parameters.scs"\ninclude "generated_model.scs"\n'
        frozen, records = _freeze_generated_dependencies(text, task, self.config,
            [controller.workspace['root']], allow_candidate_parameters=True)
        self.assertIn('include "cr_candidate_parameters.scs"', frozen)
        self.assertEqual(len(records), 1)
        with open(source, 'w') as stream:
            stream.write('parameters generated_value=99\n')
        with open(records[0]['path']) as stream:
            self.assertEqual(stream.read(), 'parameters generated_value=17\n')
        self.assertIn(records[0]['path'], frozen)

    def test_candidate_include_exception_is_explicit_and_exact(self):
        from crescendo_engine.netlist_providers import _freeze_generated_dependencies
        from crescendo_engine.native_includes import NativeIncludeError
        controller = session._controller(self.reference)
        task = {'cwd': controller.owner['root'], 'native_cwd': controller.workspace['root']}
        for reference, permitted in [('cr_candidate_parameters.scs', False),
                                     ('other_missing.scs', True),
                                     ('/missing/cr_candidate_parameters.scs', True)]:
            with self.assertRaises(NativeIncludeError):
                _freeze_generated_dependencies('include "%s"\n' % reference,
                    copy.deepcopy(task), self.config, [controller.workspace['root']],
                    allow_candidate_parameters=permitted)
