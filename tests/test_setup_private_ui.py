"""Setup lifetime integration: native binding is mocked, registry/UI are real."""
import copy
import json
import os
import threading
import unittest
from unittest import mock

from crescendo_engine import gui, setup_private_session as sessions
from crescendo_engine.cadence import CommandCancelled
from crescendo_engine.config_binding import private_config_plan
from crescendo_engine.continuation import ContinuationLease
from crescendo_engine.same_library_views import MARKER
from crescendo_engine.server import OptimizerService
from crescendo_engine.utils import atomic_write_json
from tests import test_setup_baseline as baseline_fixtures
from tests import test_gui_cell_selection as tk_fixtures


class SetupPrivateServiceIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.fixture=baseline_fixtures.SetupBaselineTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.temp.cleanup)
        self.fixture.freeze()
        self.service=OptimizerService(copy.deepcopy(self.fixture.config))
        self.events=[]
        self.fail_binding=False
        def native(controller,parameters,config,cancel_check):
            self.events.append('bind')
            if self.fail_binding:
                raise RuntimeError('fixture binding failed')
            controller.binding_plan=private_config_plan(controller.workspace,parameters)
            for row in controller.workspace['same_library_registry']:
                if row['role']=='config' and not os.path.isdir(row['path']):
                    os.makedirs(row['path'])
                    atomic_write_json(os.path.join(row['path'],MARKER),{
                        'schema_version':'crescendo.same_library_views.v1','token':row['token'],'target':row['target']})
            controller.status='ready'
            test_name = controller.prepared['setup'].get('selected_test')
            if test_name:
                controller.prepared['setup']['test_corner_masks'] = {
                    test_name: {'known': True, 'disabled_corners': [], 'nominal_enabled': True,
                                'source': 'native_setupdb_apis'}}
            controller.binding_verification={'ok':True,'native_run_invoked':False}
            controller.view_fingerprints=controller.view_identity()
            controller.persist()
        self.native_patch=mock.patch.object(sessions._PrivateSession,'_native_bind',native)
        self.native_patch.start()
        self.addCleanup(self.native_patch.stop)
        self.addCleanup(self.cleanup)
        design=self.fixture.hierarchy['designs'][0]['design']
        self.scope={'scope_id':'root','design':design,'hierarchy_path':'/',
            'library':'design','cell':'TB','view':'schematic','depth':0,'selectable':True}

    def cleanup(self):
        for reference in list(self.service.setup_private_sessions.values()):
            sessions.close_setup_private_session(reference)
            with sessions._REGISTRY_LOCK:
                sessions._SESSIONS.pop(reference['session_id'],None)

    def inspect(self):
        with mock.patch('crescendo_engine.hierarchy.probe_hierarchy',return_value=self.fixture.hierarchy), \
                mock.patch('crescendo_engine.hierarchy.cell_scope_records',return_value=[self.scope]):
            return self.service.inspect_cells(self.fixture.path)

    def apply(self,inspection_id):
        from crescendo_engine.single_test import project_setup
        def prepared(*_args, **_kwargs):
            result = copy.deepcopy(self.fixture.prepared)
            result['setup'] = project_setup(result['setup'], 'C')
            return result
        with mock.patch('crescendo_engine.server.prepare_setup',side_effect=prepared), \
                mock.patch('crescendo_engine.hierarchy.cell_scope_records',return_value=[self.scope]):
            return self.service.prepare_cells(inspection_id,['root'])

    def test_inspect_creates_private_suite_before_apply_and_apply_reuses_identity(self):
        token,preview=self.inspect()
        reference=preview['setup_private_session']
        self.assertTrue(os.path.isfile(os.path.join(reference['maestro']['path'],'maestro.sdb')))
        self.assertTrue(os.path.isdir(reference['configs'][0]['path']))
        self.assertEqual(self.events,['bind'])
        self.assertEqual(self.service.prepared,{})
        prepared_id,prepared=self.apply(token)
        self.assertEqual(self.events,['bind','bind'])
        self.assertEqual(prepared['setup_private_session']['session_id'],reference['session_id'])
        self.assertEqual(self.service.setup_private_session_info(token,prepared_id)['maestro'],reference['maestro'])
        self.assertIn(prepared_id,self.service.prepared)
        self.assertTrue(os.path.isdir(reference['maestro']['path']))

    def test_suite_survives_release_and_cross_service_load_without_hierarchy_probe(self):
        token, preview = self.inspect()
        _prepared_id, prepared = self.apply(token)
        reference = prepared['setup_private_session']
        document = {'schema_version': 'crescendo.configuration.v1',
                    'source_setup': {'path': self.fixture.path}, 'settings': {},
                    'private_suite': {'suite_id': reference['session_id'],
                        'revision': reference.get('baseline_id'), 'source_path': self.fixture.path}}
        self.service.save_private_suite_configuration(reference['session_id'], document)
        result = sessions.close_setup_private_session(reference)
        self.assertTrue(result['cleanup_proven'])
        self.assertTrue(os.path.isdir(reference['maestro']['path']))
        other = OptimizerService(copy.deepcopy(self.fixture.config))
        with mock.patch('crescendo_engine.hierarchy.probe_hierarchy', side_effect=AssertionError('unexpected scan')), \
                mock.patch('crescendo_engine.hierarchy.cell_scope_records', return_value=[self.scope]):
            loaded = other.load_private_suite(reference['session_id'])
            restored = other.load_configuration(document, path=self.fixture.path)
        self.assertEqual(loaded['suite']['id'], reference['session_id'])
        self.assertTrue(restored['preserve_current_suite'])
        self.assertTrue(restored['ready_for_settings'])
        self.assertIn(reference['session_id'], other.setup_private_sessions)
        self.assertEqual(loaded['selected_cell_scope_ids'], ['root'])
        self.assertEqual(loaded['saved_configuration']['private_suite']['suite_id'], reference['session_id'])
        self.assertIsNotNone(loaded['prepared_id'])
        self.assertEqual(loaded['prepared']['parameters'], prepared['parameters'])
        for ref in list(other.setup_private_sessions.values()):
            sessions.close_setup_private_session(ref)

    def test_old_suite_unknown_mask_is_read_back_during_load(self):
        token, _preview = self.inspect()
        _prepared_id, prepared = self.apply(token)
        reference = prepared['setup_private_session']
        controller = sessions._controller(reference)
        controller.prepared['setup']['test_corner_masks'] = {
            'C': {'known': False, 'disabled_corners': [], 'nominal_enabled': None}}
        controller.persist()
        before = len(self.events)
        with mock.patch('crescendo_engine.hierarchy.probe_hierarchy', side_effect=AssertionError('unexpected scan')), \
                mock.patch('crescendo_engine.hierarchy.cell_scope_records', return_value=[self.scope]), \
                mock.patch('crescendo_engine.prepared_freshness.verify_prepared_freshness', return_value={'ok': True}):
            loaded = self.service.load_private_suite(reference['session_id'])
        self.assertEqual(len(self.events), before + 1)
        self.assertTrue(loaded['prepared']['setup']['test_corner_masks']['C']['nominal_enabled'])
        snapshot = sessions.setup_private_session_snapshot(reference)
        self.assertTrue(snapshot['setup']['test_corner_masks']['C']['nominal_enabled'])

    def test_known_nominal_mask_loads_without_native_refresh(self):
        token, _preview = self.inspect()
        _prepared_id, prepared = self.apply(token)
        reference = prepared['setup_private_session']
        controller = sessions._controller(reference)
        controller.prepared['setup']['test_corner_masks'] = {
            'C': {'known': False, 'disabled_corners': [], 'nominal_enabled': True}}
        controller.persist()
        before = len(self.events)
        with mock.patch('crescendo_engine.prepared_freshness.verify_prepared_freshness', return_value={'ok': True}), \
                mock.patch('crescendo_engine.hierarchy.cell_scope_records', return_value=[self.scope]):
            loaded = self.service.load_private_suite(reference['session_id'])
        self.assertEqual(len(self.events), before)
        self.assertTrue(loaded['prepared']['setup']['test_corner_masks']['C']['nominal_enabled'])

    def test_old_suite_failed_mask_readback_never_publishes_new_prepared(self):
        token, _preview = self.inspect()
        _prepared_id, prepared = self.apply(token)
        reference = prepared['setup_private_session']
        controller = sessions._controller(reference)
        controller.prepared['setup']['test_corner_masks'] = {
            'C': {'known': False, 'disabled_corners': [], 'nominal_enabled': None}}
        controller.persist()
        before = set(self.service.prepared)
        self.fail_binding = True
        with mock.patch('crescendo_engine.prepared_freshness.verify_prepared_freshness', return_value={'ok': True}):
            with self.assertRaisesRegex(RuntimeError, 'native Test corner readback failed.*fixture binding failed'):
                self.service.load_private_suite(reference['session_id'])
        self.assertEqual(set(self.service.prepared), before)

    def test_configuration_import_does_not_trigger_suite_corner_readback(self):
        token, _preview = self.inspect()
        _prepared_id, prepared = self.apply(token)
        reference = prepared['setup_private_session']
        controller = sessions._controller(reference)
        controller.prepared['setup']['test_corner_masks'] = {
            'C': {'known': False, 'disabled_corners': [], 'nominal_enabled': None}}
        controller.persist()
        self.fail_binding = True
        document = {'schema_version': 'crescendo.configuration.v1',
                    'source_setup': {'path': self.fixture.path}, 'settings': {},
                    'private_suite': {'suite_id': reference['session_id'], 'source_path': self.fixture.path}}
        with mock.patch('crescendo_engine.prepared_freshness.verify_prepared_freshness', return_value={'ok': True}), \
                mock.patch.object(self.service, '_inspect_tests_registered', side_effect=AssertionError('must not fallback')):
            result = self.service.load_configuration(document, path=self.fixture.path)
        self.assertIsNone(result['prepared_id'])
        self.assertTrue(result['preserve_current_suite'])
        self.assertTrue(result['ready_for_settings'])
        self.assertNotIn('preparation_error', result)
        self.assertIn(reference['session_id'], self.service.setup_private_sessions)
        self.assertEqual(result['document']['private_suite']['suite_id'], reference['session_id'])

    def test_selective_update_reuses_hierarchy_and_retains_old_baseline(self):
        from crescendo_engine.setup_baseline import verify_setup_baseline
        token, preview = self.inspect()
        _prepared_id, prepared = self.apply(token)
        reference = prepared['setup_private_session']
        old_setup = copy.deepcopy(prepared['setup'])
        old_baseline = verify_setup_baseline(old_setup)
        self.fixture.write(self.fixture.path, self.fixture.xml.replace('<value>3</value>', '<value>9</value>'))
        with mock.patch('crescendo_engine.hierarchy.probe_hierarchy', side_effect=AssertionError('unexpected hierarchy scan')), \
                mock.patch('crescendo_engine.hierarchy.cell_scope_records', return_value=[self.scope]):
            updated = self.service.update_private_suite(reference['session_id'], ['global_variables'])
        self.assertEqual(updated['prepared']['setup']['global_variables'][0]['value'], '9')
        from crescendo_engine.setup_baseline import effective_setup_path
        from crescendo_engine.utils import sha256_file
        self.assertEqual(updated['prepared']['setup_locator']['sha256'],
                         sha256_file(effective_setup_path(updated['prepared']['setup'])))
        self.assertEqual(updated['selected_cell_scope_ids'], ['root'])
        self.assertEqual(verify_setup_baseline(old_setup), old_baseline)
        self.assertTrue(updated['update_plan']['preserve_existing_checkpoints'])

    def test_same_baseline_reinspection_reuses_suite_without_rebinding(self):
        _first,preview=self.inspect()
        reference=preview['setup_private_session']
        controller = sessions._controller(reference)
        controller.prepared['setup']['test_corner_masks'] = {'C': {'known': True, 'disabled_corners': ['OFF']}}
        _second,repeated=self.inspect()
        self.assertEqual(repeated['setup_private_session']['session_id'],reference['session_id'])
        self.assertEqual(repeated['setup_private_session']['maestro'],reference['maestro'])
        self.assertEqual(len(self.service.setup_private_sessions),1)
        self.assertEqual(self.events,['bind'])
        self.assertEqual(repeated['setup']['test_corner_masks']['C']['disabled_corners'], ['OFF'])

    def test_same_design_different_test_preserves_old_suite(self):
        from xml.etree import ElementTree as ET
        tree = ET.parse(self.fixture.path)
        tests = tree.getroot().find('active/tests')
        other = copy.deepcopy(tests.find('test'))
        other.set('Name', 'D')
        tests.append(other)
        tree.write(self.fixture.path, encoding='utf-8', xml_declaration=True)
        self.service.select_setup(self.fixture.path)
        with mock.patch('crescendo_engine.hierarchy.probe_hierarchy', return_value=self.fixture.hierarchy), \
                mock.patch('crescendo_engine.hierarchy.cell_scope_records', return_value=[self.scope]):
            token, _catalogue = self.service.inspect_tests(self.fixture.path)
            first_id, first = self.service.inspect_test_cells(token, 'C')
            self.service.prepared['old'] = {'setup_private_session': first['setup_private_session']}
            second_id, second = self.service.inspect_test_cells(token, 'D')
        self.assertNotEqual(first['setup_private_session']['session_id'], second['setup_private_session']['session_id'])
        self.assertEqual(second['setup_private_session']['selected_test'], 'D')
        self.assertNotIn(first_id, self.service.inspections)
        self.assertNotIn('old', self.service.prepared)
        self.assertIn(second_id, self.service.inspections)
        self.assertTrue(all(os.path.isdir(row['path']) for row in first['setup_private_session']['views']))

    def test_cross_test_borrow_and_apply_are_rejected_before_private_write(self):
        from xml.etree import ElementTree as ET
        from crescendo_engine.single_test import project_setup
        tree = ET.parse(self.fixture.path)
        tests = tree.getroot().find('active/tests')
        other = copy.deepcopy(tests.find('test'))
        other.set('Name', 'D')
        tests.append(other)
        tree.write(self.fixture.path, encoding='utf-8', xml_declaration=True)
        self.service.select_setup(self.fixture.path)
        with mock.patch('crescendo_engine.hierarchy.probe_hierarchy', return_value=self.fixture.hierarchy), \
                mock.patch('crescendo_engine.hierarchy.cell_scope_records', return_value=[self.scope]):
            token, _catalogue = self.service.inspect_tests(self.fixture.path)
            _cell, first = self.service.inspect_test_cells(token, 'C')
        reference = first['setup_private_session']
        wrong = copy.deepcopy(self.fixture.prepared)
        wrong['setup'] = project_setup(first['setup'], 'D')
        wrong['setup_private_session'] = reference
        controller = sessions._controller(reference)
        with mock.patch.object(controller, 'reset_maestro') as reset, \
                mock.patch.object(controller, 'writable_views') as writable:
            with self.assertRaisesRegex(ValueError, 'different or unqualified Test'):
                with sessions.borrow_setup_private_session(wrong, self.service.config, os.path.join(self.fixture.root, 'bad')):
                    self.fail('wrong Test borrowed session')
            with self.assertRaisesRegex(ValueError, 'different or unqualified Test'):
                sessions.apply_setup_private_cells(reference, wrong, self.service.config)
            reset.assert_not_called()
            writable.assert_not_called()
        self.assertEqual(controller.status, 'ready')
        self.assertFalse(controller.unsafe)

    def test_explicit_reloads_preserve_old_versions(self):
        _token,preview=self.inspect()
        old_baseline=copy.deepcopy(preview['setup']['baseline'])
        old_setup=copy.deepcopy(preview['setup'])
        unknown=os.path.join(self.fixture.library,'TB','cr_s_unregistered_user_view')
        os.makedirs(unknown)
        self.fixture.write(os.path.join(unknown,'keep.txt'),'user view')
        references=[preview['setup_private_session']]
        for value in ('7','11','13'):
            self.fixture.write(self.fixture.path,self.fixture.xml.replace('<value>3</value>','<value>%s</value>'%value))
            self.service.select_setup(self.fixture.path)
            _token,current=self.inspect()
            reference=current['setup_private_session']
            self.assertEqual(current['setup']['global_variables'][0]['value'],value)
            self.assertEqual(list(self.service.setup_private_sessions),[reference['session_id']])
            self.assertTrue(all(os.path.isdir(row['path']) for old in references for row in old['views']))
            self.assertTrue(all(os.path.isdir(row['path']) for row in reference['views']))
            references.append(reference)
        self.assertEqual(len({row['session_id'] for row in references}),4)
        self.assertTrue(os.path.isfile(os.path.join(unknown,'keep.txt')))
        from crescendo_engine.setup_baseline import verify_setup_baseline
        self.assertEqual(verify_setup_baseline(old_setup),old_baseline)

    def test_replacement_cleanup_failure_retains_only_old_suite_until_retry(self):
        _token,preview=self.inspect()
        reference=preview['setup_private_session']
        self.service.select_setup(self.fixture.path)
        with mock.patch.object(sessions,'close_setup_private_session',return_value={
                'cleanup_proven':False,'state':'private_setup_native_stop_unproven'}), \
                mock.patch.object(sessions,'create_setup_private_session') as create:
            with self.assertRaisesRegex(RuntimeError,'no new private suite was created'):
                self.inspect()
            create.assert_not_called()
        self.assertEqual(list(self.service.setup_private_sessions),[reference['session_id']])
        self.assertTrue(all(os.path.isdir(row['path']) for row in reference['views']))
        _token,retried=self.inspect()
        self.assertNotEqual(retried['setup_private_session']['session_id'],reference['session_id'])
        self.assertEqual(len(self.service.setup_private_sessions),1)
        self.assertTrue(all(os.path.isdir(row['path']) for row in reference['views']))

    def test_cleanup_exception_or_wrong_owner_cannot_authorize_replacement(self):
        _token,preview=self.inspect()
        reference=preview['setup_private_session']
        self.service.select_setup(self.fixture.path)
        with mock.patch.object(sessions,'close_setup_private_session',side_effect=ValueError('ownership differs')), \
                mock.patch.object(sessions,'create_setup_private_session') as create:
            with self.assertRaisesRegex(RuntimeError,'no new private suite was created'):
                self.inspect()
            create.assert_not_called()
        wrong=copy.deepcopy(reference)
        wrong['owner_uid']=-1
        self.service.setup_private_sessions[reference['session_id']]=wrong
        try:
            with mock.patch.object(sessions,'create_setup_private_session') as create:
                with self.assertRaisesRegex(RuntimeError,'no new private suite was created'):
                    self.inspect()
                create.assert_not_called()
            self.assertTrue(all(os.path.isdir(row['path']) for row in reference['views']))
        finally:
            self.service.setup_private_sessions[reference['session_id']]=reference

    def test_failed_replacement_and_retry_never_accumulate_suites(self):
        _token,preview=self.inspect()
        original=preview['setup_private_session']
        self.service.select_setup(self.fixture.path)
        self.fail_binding=True
        with self.assertRaisesRegex(RuntimeError,'fixture binding failed'):
            self.inspect()
        failed=next(iter(self.service.setup_private_sessions.values()))
        self.assertEqual(len(self.service.setup_private_sessions),1)
        self.assertEqual(failed['status'],'failed')
        self.assertTrue(all(os.path.isdir(row['path']) for row in original['views']))
        failed_paths = [os.path.exists(row['path']) for row in failed['views']]
        self.fail_binding=False
        _token,retried=self.inspect()
        self.assertNotEqual(retried['setup_private_session']['session_id'],failed['session_id'])
        self.assertEqual(len(self.service.setup_private_sessions),1)
        self.assertEqual([os.path.exists(row['path']) for row in failed['views']], failed_paths)

    def test_retirement_invalidates_old_prepared_and_inspection_authority(self):
        old_inspection,preview=self.inspect()
        reference=preview['setup_private_session']
        self.service.prepared['old']={'setup_private_session':copy.deepcopy(reference)}
        self.service.config.pop('_setup_baseline')
        current_inspection,current=self.inspect()
        self.assertNotIn('old',self.service.prepared)
        self.assertNotIn(old_inspection,self.service.inspections)
        self.assertIn(current_inspection,self.service.inspections)
        self.assertNotEqual(current['setup_private_session']['session_id'],reference['session_id'])

    def test_reinspection_releases_but_preserves_surplus_suites(self):
        _token,preview=self.inspect()
        reference=preview['setup_private_session']
        surplus=sessions.create_setup_private_session(self.fixture.prepared,self.service.config)
        self.service._remember_setup_private_session(surplus)
        self.assertEqual(len(self.service.setup_private_sessions),2)
        _token,repeated=self.inspect()
        self.assertEqual(repeated['setup_private_session']['session_id'],reference['session_id'])
        self.assertEqual(list(self.service.setup_private_sessions),[reference['session_id']])
        self.assertTrue(all(os.path.isdir(row['path']) for row in surplus['views']))

    def test_external_editor_lock_preserves_old_suite_and_allows_new_version(self):
        _token, preview = self.inspect()
        reference = preview['setup_private_session']
        lock = os.path.join(reference['maestro']['path'], 'external.cdslck')
        self.fixture.write(lock, 'unknown editor lock')
        self.service.select_setup(self.fixture.path)
        close = sessions.close_setup_private_session
        releases = []
        def record_release(ref, **kwargs):
            result = close(ref, **kwargs)
            releases.append(result)
            return result
        with mock.patch.object(sessions, 'close_setup_private_session', side_effect=record_release):
            _token, replacement = self.inspect()
        self.assertNotEqual(replacement['setup_private_session']['session_id'], reference['session_id'])
        self.assertTrue(releases[0]['cleanup_proven'])
        self.assertTrue(releases[0]['warnings'])
        self.assertTrue(os.path.isfile(lock))
        self.assertTrue(os.path.isdir(reference['maestro']['path']))
        self.assertTrue(os.path.isfile(self.fixture.path))
        os.unlink(lock)

    def test_failed_or_unsafe_suite_cannot_be_bypassed_with_another_copy(self):
        _token,preview=self.inspect()
        reference=preview['setup_private_session']
        controller=sessions._controller(reference)
        for state in ('failed','unsafe'):
            with self.subTest(state=state):
                controller.status=state
                controller.unsafe=state=='unsafe'
                controller.persist()
                with mock.patch.object(sessions,'close_setup_private_session',return_value={
                        'cleanup_proven':False,'state':'private_setup_native_stop_unproven'}), \
                        mock.patch.object(sessions,'create_setup_private_session') as create:
                    with self.assertRaisesRegex(RuntimeError,'no new private suite was created'):
                        self.inspect()
                    create.assert_not_called()
                self.assertEqual(list(self.service.setup_private_sessions),[reference['session_id']])
        controller.status='ready'
        controller.unsafe=False

    def test_closed_registered_session_is_retired_before_new_suite(self):
        _token,preview=self.inspect()
        reference=preview['setup_private_session']
        self.assertTrue(sessions.close_setup_private_session(reference)['cleanup_proven'])
        _token,retried=self.inspect()
        self.assertNotEqual(retried['setup_private_session']['session_id'],reference['session_id'])
        self.assertEqual(len(self.service.setup_private_sessions),1)

    def test_active_run_or_range_cannot_retire_private_suite(self):
        _token,preview=self.inspect()
        reference=preview['setup_private_session']
        class Active(object):
            def is_alive(self):
                return True
        for active in ('run','range','range_finishing'):
            with self.subTest(active=active):
                if active=='run':
                    self.service.threads['active']=Active()
                elif active=='range':
                    self.service.range_validations['active']={'state':'running'}
                else:
                    self.service.range_validations['active']={'state':'completed'}
                    self.service.range_validation_threads['active']=Active()
                try:
                    with mock.patch.object(sessions,'close_setup_private_session') as close, \
                            mock.patch.object(sessions,'create_setup_private_session') as create:
                        with self.assertRaisesRegex(RuntimeError,'cannot'):
                            self.inspect()
                        close.assert_not_called()
                        create.assert_not_called()
                    self.assertEqual(list(self.service.setup_private_sessions),[reference['session_id']])
                finally:
                    self.service.threads.clear()
                    self.service.range_validations.clear()
                    self.service.range_validation_threads.clear()

    def test_cancellation_before_retirement_keeps_old_suite_and_does_not_clone(self):
        _token,preview=self.inspect()
        reference=preview['setup_private_session']
        controller=sessions._controller(reference)
        controller.status='failed'
        with mock.patch('crescendo_engine.server.check_command_cancellation',side_effect=CommandCancelled('Setup stopped')), \
                mock.patch.object(sessions,'close_setup_private_session') as close, \
                mock.patch.object(sessions,'create_setup_private_session') as create:
            with self.assertRaises(CommandCancelled):
                self.service._prepare_setup_private_session(copy.deepcopy(preview))
            close.assert_not_called()
            create.assert_not_called()
        self.assertEqual(list(self.service.setup_private_sessions),[reference['session_id']])
        self.assertTrue(all(os.path.isdir(row['path']) for row in reference['views']))

    def test_failed_apply_does_not_publish_new_prepared_id_and_retains_owned_failure(self):
        token,preview=self.inspect()
        first_id,first=self.apply(token)
        self.fail_binding=True
        with self.assertRaisesRegex(RuntimeError,'fixture binding failed'):
            self.apply(token)
        self.assertEqual(list(self.service.prepared),[first_id])
        reference=next(iter(self.service.setup_private_sessions.values()))
        self.assertEqual(reference['session_id'],preview['setup_private_session']['session_id'])
        self.assertEqual(reference['status'],'failed')
        self.assertTrue(os.path.isdir(reference['maestro']['path']))

    def test_service_exit_stops_runs_before_releasing_private_views(self):
        _token,preview=self.inspect()
        reference=preview['setup_private_session']
        events=self.events
        class Run(object):
            alive=True
            def is_alive(self):
                return self.alive
            def join(self,timeout=None):
                events.append('run_join')
                self.alive=False
        self.service.threads['run']=Run()
        close=sessions.close_setup_private_session
        def close_owned(ref,**kwargs):
            self.assertFalse(self.service.threads['run'].is_alive())
            events.append('private_close')
            return close(ref,**kwargs)
        with mock.patch.object(self.service,'stop',side_effect=lambda *_a:events.append('run_stop')), \
                mock.patch.object(self.service,'_recover_optimized_view_cleanup'), \
                mock.patch.object(self.service,'_cancel_and_wait_range_validations',side_effect=lambda **_k:events.append('range_stop')), \
                mock.patch.object(self.service,'_close_resident_validation',side_effect=lambda **_k:events.append('resident_close')), \
                mock.patch.object(self.service,'_retry_session_run_cleanup',return_value={}), \
                mock.patch.object(self.service,'_clear_retained_logs',side_effect=lambda **_k:events.append('logs_clear')), \
                mock.patch.object(self.service,'_cleanup_session_scratch',return_value={}), \
                mock.patch.object(sessions,'close_setup_private_session',side_effect=close_owned):
            result=self.service.exit_optimizer()
        self.assertTrue(result['setup_private_cleanup']['cleanup_proven'])
        self.assertLess(events.index('run_stop'),events.index('run_join'))
        self.assertLess(events.index('run_join'),events.index('private_close'))
        self.assertLess(events.index('resident_close'),events.index('private_close'))
        self.assertLess(events.index('private_close'),events.index('logs_clear'))
        self.assertTrue(os.path.isdir(reference['maestro']['path']))
        self.assertTrue(os.path.isfile(self.fixture.path))

    def test_exit_returns_editor_lock_warning_and_preserves_suite(self):
        _token, preview = self.inspect()
        reference = preview['setup_private_session']
        lock = os.path.join(reference['maestro']['path'], 'external.cdslck')
        self.fixture.write(lock, 'external editor')
        result = self.service.exit_optimizer()
        self.assertTrue(result['exiting'])
        self.assertTrue(result['warnings'])
        self.assertTrue(os.path.isfile(lock))
        self.assertTrue(os.path.isdir(reference['maestro']['path']))
        os.unlink(lock)

    def test_resume_attaches_only_live_suite_with_same_baseline_scope_and_parameter_catalog(self):
        _token,preview=self.inspect()
        reference=preview['setup_private_session']
        controller=sessions._controller(reference)
        controller.applied_selection={'scope_ids':['root'],'parameter_ids':['p1']}
        controller.persist()
        prepared=copy.deepcopy(self.fixture.prepared)
        prepared['setup']['selected_test'] = reference['selected_test']
        prepared['cell_selection']={'scope_ids':['root']}
        prepared['parameters']=[{'parameter_id':'p1','optimizable':True,'backend':'schematic'}]
        self.service._attach_live_setup_private_session(prepared)
        self.assertEqual(prepared['setup_private_session']['session_id'],reference['session_id'])
        for difference in ('baseline','scope','parameter','unsafe'):
            other=copy.deepcopy(prepared)
            other.pop('setup_private_session',None)
            if difference=='baseline':
                other['setup']['baseline']['baseline_id']='another'
            elif difference=='scope':
                other['cell_selection']['scope_ids']=['different']
            elif difference=='parameter':
                other['parameters'][0]['parameter_id']='different'
            else:
                controller.unsafe=True
            self.service._attach_live_setup_private_session(other)
            self.assertNotIn('setup_private_session',other,difference)
            controller.unsafe=False

    def test_global_stop_cancels_setup_binding_retains_views_and_next_load_can_restart(self):
        entered=threading.Event()
        release=threading.Event()
        errors=[]
        def waiting_native(controller,parameters,config,cancel_check):
            entered.set()
            release.wait(1)
            if cancel_check and cancel_check():
                raise CommandCancelled('fixture Setup stopped')
            self.fail('native operation did not receive cancellation')
        def inspect():
            try:
                self.inspect()
            except Exception as error:
                errors.append(error)
        with mock.patch.object(sessions._PrivateSession,'_native_bind',waiting_native):
            worker=threading.Thread(target=inspect)
            worker.start()
            self.assertTrue(entered.wait(1))
            result=self.service.stop_setup_preparation()
            self.assertTrue(result['stop_requested'])
            self.assertTrue(self.service.prepare_cancel_event.is_set())
            release.set()
            worker.join(1)
            self.assertFalse(worker.is_alive())
        self.assertEqual(len(errors),1)
        self.assertIsInstance(errors[0],CommandCancelled)
        failed=next(iter(self.service.setup_private_sessions.values()))
        self.assertTrue(os.path.isdir(failed['maestro']['path']))
        failed_paths = [os.path.exists(row['path']) for row in failed['views']]
        self.assertEqual(self.service.inspections,{})
        self.assertFalse(self.service.stop_setup_preparation()['stop_requested'])
        token,preview=self.inspect()
        self.assertFalse(self.service.prepare_cancel_event.is_set())
        self.assertIn(token,self.service.inspections)
        self.assertNotEqual(preview['setup_private_session']['session_id'],failed['session_id'])
        self.assertEqual(len(self.service.setup_private_sessions),1)
        self.assertEqual([os.path.exists(row['path']) for row in failed['views']], failed_paths)

    def test_range_sharing_live_setup_blocks_active_optimization_but_legacy_gate_stays_independent(self):
        _token,preview=self.inspect()
        self.service.prepared['prepared']=copy.deepcopy(self.fixture.prepared)
        from crescendo_engine.single_test import project_setup
        self.service.prepared['prepared']['setup'] = project_setup(self.service.prepared['prepared']['setup'], 'C')
        self.service.prepared['prepared']['setup_private_session']=preview['setup_private_session']
        self.service.optimization_launches=1
        with self.assertRaisesRegex(RuntimeError,'shares the Setup private views'):
            self.service._validate_ranges_registered('prepared',[])
        self.service.prepared['prepared'].pop('setup_private_session')
        with mock.patch('crescendo_engine.server.validate_variable_request',side_effect=ValueError('legacy variable gate reached')):
            with self.assertRaisesRegex(ValueError,'legacy variable gate reached'):
                self.service._validate_ranges_registered('prepared',[])
        self.assertEqual(self.service.range_validations,{})

    def test_start_hands_off_shared_setup_validation_while_real_legacy_lease_is_isolated(self):
        _token,preview=self.inspect()
        runtime=self.service.config['runtime']['run_root']
        job='range_validation_fixture'
        job_root=os.path.join(runtime,'range_validations',job)
        lease=ContinuationLease(runtime,'optional-pdk-callback-validation-v1',run_root=job_root,
            lease_kind='optional_pdk_callback_validation').acquire()
        self.addCleanup(lease.release)
        self.service.range_validations[job]={'state':'running','prepared_id':'prepared',
            'resource_scope':'isolated_optional_validation','job_root':job_root}
        self.service.range_validation_leases[job]=lease
        self.service.prepared['prepared']={'setup_private_session':preview['setup_private_session']}
        self.assertFalse(self.service._validation_resources_are_isolated())
        with mock.patch.object(self.service,'_cancel_and_wait_range_validations',return_value=['stopped']) as stop:
            self.assertEqual(self.service._wait_for_shared_validation_resources(),['stopped'])
            stop.assert_called_once_with()
        self.service.prepared['prepared'].pop('setup_private_session')
        self.assertTrue(self.service._validation_resources_are_isolated())
        with mock.patch.object(self.service,'_cancel_and_wait_range_validations') as stop:
            self.assertEqual(self.service._wait_for_shared_validation_resources(),[])
            stop.assert_not_called()
        self.service.range_validations[job]['resource_scope']='setup_private_session'
        self.assertFalse(self.service._validation_resources_are_isolated())

    def test_one_setup_log_has_small_success_and_failure_keeps_native_error_frames(self):
        from crescendo_engine.setup_private_diagnostics import write_setup_diagnostic_log
        _token,preview=self.inspect()
        reference=preview['setup_private_session']
        path=write_setup_diagnostic_log(reference)
        with open(path,encoding='utf-8') as stream:
            success=stream.read()
        self.assertNotIn('first_cause',success)
        self.assertLess(len(success),8192)
        controller=sessions._controller(reference)
        control=os.path.join(reference['root'],'operations','fixture')
        os.makedirs(control)
        controller.operations.append(control)
        controller.status='failed'
        controller.persist()
        script=os.path.join(control,'setup.il')
        with open(script,'w') as stream:
            stream.write('sstatus(stacktrace 16)\n')
        atomic_write_json(os.path.join(control,'command.json'),{'returncode':0,'cwd':reference['workspace_root']})
        with open(os.path.join(control,'ocean.stdout'),'w') as stream:
            stream.write('*Error* eval: unbound variable - schematic_custom\nStack Trace:\n'+
                '\n'.join('native_frame_%d()'%n for n in range(16)))
        self.assertEqual(write_setup_diagnostic_log(reference,RuntimeError('binding rejected')),path)
        with open(path,encoding='utf-8') as stream:
            failure=stream.read().splitlines()
        cause=json.loads(next(row.split(' ',1)[1] for row in failure if row.startswith('first_cause ')))
        evidence=json.loads(next(row.split(' ',1)[1] for row in failure if row.startswith('native_binding_failure ')))
        self.assertEqual(cause['message'],'binding rejected')
        self.assertFalse(evidence['native_error']['ok'])
        self.assertIn('native_frame_15()',json.dumps(evidence['native_error']['native_error_contexts']))
        self.assertEqual([name for name in os.listdir(reference['root']) if name.endswith('.log')],['environment_test.log'])


class _SetupService(tk_fixtures.FakeStagedService):
    def __init__(self,context):
        super(_SetupService,self).__init__(context)
        self.reference={'session_id':'session','status':'ready',
            'maestro':{'library':'lib_A','cell':'VCO','view':'cr_maestro','path':'/private/maestro'},
            'configs':[{'library':'lib_A','cell':'TB','view':'cr_config','path':'/private/config','bindings':[]}]}
        self.info_calls=[]
        self.exit_fail=False
        self.setup_stop_calls=0
    def inspect_cells(self,path,**options):
        token,preview=super(_SetupService,self).inspect_cells(path,**options)
        preview['setup_private_session']=copy.deepcopy(self.reference)
        self.inspections[token]=copy.deepcopy(preview)
        return token,preview
    def prepare_cells(self,inspection_id,scope_ids):
        token,prepared=super(_SetupService,self).prepare_cells(inspection_id,scope_ids)
        self.reference['configs'][0]['bindings']=[{'path':'/CORE_A','view':'private_schematic'}]
        prepared['setup_private_session']=copy.deepcopy(self.reference)
        return token,prepared
    def setup_private_session_info(self,**kwargs):
        self.info_calls.append(kwargs)
        return copy.deepcopy(self.reference)
    def exit_optimizer(self):
        self.exit_calls.append(True)
        if self.exit_fail:
            raise RuntimeError('fixture safe close unproven')
        self.reference['status']='closed'
        return {'closed':True}
    def private_suite_configuration_revision(self, suite_id):
        return None
    def save_private_suite_configuration(self, suite_id, document, expected_revision=None):
        return {'saved': True, 'revision': 'fixture-revision'}
    def stop_setup_preparation(self):
        self.setup_stop_calls+=1
        return {'stop_requested':True,'views_retained_until_ui_close':True}


@unittest.skipUnless(os.environ.get('DISPLAY'),'Setup private Tk tests require an existing DISPLAY')
class SetupPrivateTkIntegrationTests(unittest.TestCase):
    setUp=tk_fixtures.GuiCellSelectionTests.setUp
    _close=tk_fixtures.GuiCellSelectionTests._close
    _wait=tk_fixtures.GuiCellSelectionTests._wait
    _load_cells=tk_fixtures.GuiCellSelectionTests._load_cells
    _apply=tk_fixtures.GuiCellSelectionTests._apply
    _toggle=tk_fixtures.GuiCellSelectionTests._toggle
    def _open(self):
        self.service=_SetupService(self.context)
        self.app=gui.OptimizerGui(self.root,self.service,launch_context=self.context)
        self.root.report_callback_exception=lambda *args:self.callback_errors.append(args)
        self._wait(lambda:'/fixture/ocean' in self.app.eda_effective_vars['ocean_bin'].get())
        return self.app

    def test_load_and_use_invoking_display_private_identity_before_apply_then_open_fresh_binding(self):
        app=self._open()
        app.use_invoking_setup()
        self._wait(lambda:app.cell_inspection_id and not app.setup_loading)
        self.assertEqual(self.service.prepare_cells_calls,[])
        self.assertIn('/private/maestro',app.private_setup_var.get())
        self.assertEqual(str(app.open_config_button['state']),'normal')
        self._toggle('/CORE_A')
        self._apply()
        with mock.patch.object(app,'_show_private_config') as show:
            app.open_config_button.invoke()
            self._wait(lambda:show.called)
            self.assertEqual(show.call_args[0][0]['configs'][0]['bindings'][0]['view'],'private_schematic')
        self.assertEqual(self.service.info_calls[-1]['prepared_id'],app.prepared_id)
        self.assertEqual(self.service.info_calls[-1]['inspection_id'],app.cell_inspection_id)

    def test_failed_close_keeps_window_until_retry_proves_private_cleanup(self):
        app=self._open()
        self._load_cells()
        self.service.exit_fail=True
        app.request_exit(confirm=False)
        self._wait(lambda:app.shutdown_failed)
        self.assertFalse(self.destroyed)
        self.assertFalse(getattr(app,'cleanup_confirmed',False))
        self.assertEqual(self.service.reference['status'],'ready')
        self.service.exit_fail=False
        app.request_exit(confirm=False)
        self._wait(lambda:self.destroyed)
        self.assertTrue(app.cleanup_confirmed)
        self.assertEqual(self.service.reference['status'],'closed')
        self.assertEqual(len(self.service.exit_calls),2)

    def test_global_stop_routes_setup_operation_through_control_queue(self):
        app=self._open()
        self._load_cells()
        app.setup_loading=True
        app._set_setup_controls(False)
        self.assertEqual(str(app.stop_button['state']),'normal')
        app.stop_button.invoke()
        self._wait(lambda:self.service.setup_stop_calls==1)
        self.assertEqual(self.service.reference['status'],'ready')
        self.assertEqual(self.service.exit_calls,[])
        self.assertFalse(self.destroyed)
        app.setup_loading=False
