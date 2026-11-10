"""Loading refreshes changed source; an already running source guard stays strict."""
import copy
import os
import unittest
from unittest import mock

from crescendo_engine import setup_private_session as sessions
from crescendo_engine.setup_baseline import load_bound_setup
from crescendo_engine.workspace import source_design_identity, verify_sources_unchanged
from tests import test_setup_private_ui as private_ui_fixtures


class SetupSourceRefreshTests(unittest.TestCase):
    def setUp(self):
        self.case = private_ui_fixtures.SetupPrivateServiceIntegrationTests(
            'test_same_baseline_reinspection_reuses_suite_without_rebinding')
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        self.service, self.fixture = self.case.service, self.case.fixture

    def change_source(self):
        self.fixture.write(self.fixture.schematic, 'current user schematic after native cell discovery')

    def assert_ready_source(self, prepared):
        controller = sessions._controller(prepared['setup_private_session'])
        self.assertEqual(prepared['source_identity']['fingerprint'],
                         controller.workspace['source_identity']['fingerprint'])
        self.assertTrue(verify_sources_unchanged(controller.workspace)['ok'])
        with sessions.borrow_setup_private_session(prepared, self.service.config,
                os.path.join(self.fixture.root, 'candidate')) as workspace:
            self.assertEqual(workspace['source_identity']['fingerprint'], prepared['source_identity']['fingerprint'])

    def test_same_maestro_baseline_reinspection_refreshes_changed_schematic(self):
        token, first = self.case.inspect()
        previous = first['setup_private_session']
        self.change_source()
        _token, current = self.case.inspect()
        self.assertEqual(previous['baseline_id'], current['setup_private_session']['baseline_id'])
        self.assertNotEqual(previous['session_id'], current['setup_private_session']['session_id'])
        self.assertNotIn(token, self.service.inspections)
        self.assertTrue(all(os.path.isdir(row['path']) for row in previous['views']))
        self.assert_ready_source(current)

    def test_apply_refreshes_only_after_current_scope_and_hierarchy_checks(self):
        token, preview = self.case.inspect()
        previous = preview['setup_private_session']
        self.change_source()
        prepared_id, prepared = self.case.apply(token)
        self.assertNotEqual(previous['session_id'], prepared['setup_private_session']['session_id'])
        self.assertEqual(prepared['cell_selection']['scope_ids'], ['root'])
        self.assertIn(prepared_id, self.service.prepared)
        self.assertEqual(self.service.inspections[token]['preview']['setup_private_session']['session_id'],
                         prepared['setup_private_session']['session_id'])
        self.assertTrue(prepared['setup_refresh_warnings'])
        self.assert_ready_source(prepared)
        # No automatic repair is authorized once Load/Apply has completed.
        self.fixture.write(self.fixture.schematic, 'user changed design during execution')
        with self.assertRaisesRegex(ValueError, 'source dependencies changed'):
            with sessions.borrow_setup_private_session(prepared, self.service.config,
                    os.path.join(self.fixture.root, 'later-candidate')):
                self.fail('running source guard was bypassed')

    def test_changed_source_cannot_bypass_old_private_cleanup(self):
        token, preview = self.case.inspect()
        previous = preview['setup_private_session']
        self.change_source()
        with mock.patch.object(sessions, 'close_setup_private_session', return_value={
                'cleanup_proven': False, 'state': 'private_setup_native_stop_unproven'}), \
                mock.patch.object(sessions, 'create_setup_private_session') as create:
            with self.assertRaisesRegex(RuntimeError, 'no new private suite was created'):
                self.case.apply(token)
            create.assert_not_called()
        self.assertEqual(self.service.prepared, {})
        self.assertEqual(list(self.service.setup_private_sessions), [previous['session_id']])

    def test_failed_replacement_never_publishes_prepared(self):
        token, _preview = self.case.inspect()
        self.change_source()
        self.case.fail_binding = True
        with self.assertRaisesRegex(RuntimeError, 'fixture binding failed'):
            self.case.apply(token)
        self.assertEqual(self.service.prepared, {})
        self.assertEqual(len(self.service.setup_private_sessions), 1)
        self.assertEqual(next(iter(self.service.setup_private_sessions.values()))['status'], 'failed')

    def test_second_source_change_after_parameter_scan_is_not_silently_adopted(self):
        token, preview = self.case.inspect()
        previous = preview['setup_private_session']
        self.change_source()
        original = self.service._prepare_setup_private_session
        def changed_again(prepared, **options):
            self.fixture.write(self.fixture.schematic, 'a second edit after parameter scanning completed')
            return original(prepared, **options)
        with mock.patch.object(self.service, '_prepare_setup_private_session', side_effect=changed_again), \
                mock.patch.object(sessions, 'close_setup_private_session') as close:
            with self.assertRaisesRegex(ValueError, 'changed during parameter inspection'):
                self.case.apply(token)
            close.assert_not_called()
        self.assertEqual(self.service.prepared, {})
        self.assertEqual(list(self.service.setup_private_sessions), [previous['session_id']])

    def test_complete_configuration_load_uses_current_source_and_restores_other_settings(self):
        # This is one service transaction from a newly opened UI: old settings
        # are imported, native cells are inspected, then parameters are applied.
        self.service.config.pop('_setup_baseline', None)
        evidence = {key: copy.deepcopy(self.case.scope[key]) for key in
                    ('design', 'hierarchy_path', 'library', 'cell', 'view')}
        document = {'schema_version': 'crescendo.configuration.v1',
            'source_setup': {'path': self.fixture.path, 'backend': 'maestro',
                'identity': {'library': 'design', 'cell': 'TB', 'view': 'maestro'}},
            'settings': {'schema_version': 'crescendo.ui_settings.v1',
                'setup': {'path': self.fixture.path, 'backend': 'maestro',
                    'identity': {'library': 'design', 'cell': 'TB', 'view': 'maestro'}},
                'cells': [{'scope_id': 'old-scope', 'selected': True, 'evidence': evidence}],
                'parameters': [], 'outputs': [], 'objectives': [], 'constraints': [],
                'optimization': {'algorithm': 'differential_evolution', 'generations': '23'}},
            'selection': {'provided': True}, 'execution_selection': {'tests': ['C']}}
        document['settings']['cells'].append({'scope_id': 'removed', 'selected': True,
            'evidence': dict(evidence, hierarchy_path='/removed')})
        def current_prepared(*_args, **_kwargs):
            self.change_source()
            value = copy.deepcopy(self.fixture.prepared)
            value['setup'] = load_bound_setup(self.fixture.path, self.service.config)
            value['source_identity'] = source_design_identity(value['setup'], value['hierarchy'], self.service.config)
            return value
        with mock.patch('crescendo_engine.hierarchy.probe_hierarchy', return_value=self.fixture.hierarchy), \
                mock.patch('crescendo_engine.hierarchy.cell_scope_records', return_value=[self.case.scope]), \
                mock.patch('crescendo_engine.server.prepare_setup', side_effect=current_prepared):
            result = self.service.load_configuration(document)
        self.assertTrue(result['ready_for_settings'], result.get('preparation_error'))
        self.assertEqual(result['selected_cell_scope_ids'], ['root'])
        self.assertEqual(result['match']['apply']['optimization']['generations'], '23')
        self.assertGreater(result['cell_match']['skipped_count'], 0)
        self.assertTrue(any('rebuilt from the current design' in row for row in result['warnings']))
        self.assertEqual(result['inspection']['setup_private_session']['session_id'],
                         result['prepared']['setup_private_session']['session_id'])
        self.assertTrue(any(row['level'] == 'WARN' and row['stage'] == 'private_copy'
                            for row in self.service.activity_snapshot()['events']))
        self.assert_ready_source(result['prepared'])


if __name__ == '__main__':
    unittest.main()
