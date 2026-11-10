"""Native readback precedes publication; source and baseline changes stay fatal."""
import copy
import os
import unittest
from unittest import mock

from crescendo_engine import server, setup_private_session as sessions
from crescendo_engine.ade_setup import apply_native_corner_schedule
from crescendo_engine.prepared_freshness import require_prepared_freshness, verify_prepared_freshness
from crescendo_engine.setup_baseline import load_bound_setup, load_prepared_baseline
from crescendo_engine.unified_configuration import build_configuration
from tests import test_setup_private_ui as fixtures


class PreparedFinalizeLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.case = fixtures.SetupPrivateServiceIntegrationTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        self.fixture, self.service = self.case.fixture, self.case.service
        original_bind = sessions._PrivateSession._native_bind
        def native_readback(controller, *args):
            self.assertEqual(self.service.prepared, {}, 'draft published before native Apply completed')
            original_bind(controller, *args)
            schedule = {'read_ok': True, 'groups': {}, 'nominal_tests': {'C': True}}
            apply_native_corner_schedule(controller.prepared['setup'], schedule)
            apply_native_corner_schedule(controller.workspace['setup'], schedule)
        self.native = mock.patch.object(sessions._PrivateSession, '_native_bind', native_readback)
        self.native.start()
        self.addCleanup(self.native.stop)
        self.captures, self.persists = [], []
        capture, persist = server.capture_prepared_freshness, server.persist_prepared_baseline
        def capture_final(prepared):
            self.captures.append(len(self.case.events))
            return capture(prepared)
        def persist_final(prepared):
            self.persists.append(len(self.case.events))
            return persist(prepared)
        patches = [
            mock.patch('crescendo_engine.server.capture_prepared_freshness', side_effect=capture_final),
            mock.patch('crescendo_engine.server.persist_prepared_baseline', side_effect=persist_final),
            mock.patch('crescendo_engine.hierarchy.probe_hierarchy', return_value=self.fixture.hierarchy),
            mock.patch('crescendo_engine.hierarchy.cell_scope_records', return_value=[self.case.scope]),
            mock.patch('crescendo_engine.server.prepare_setup', side_effect=self.prepare_current),
        ]
        for patch in patches:
            patch.start()
            self.addCleanup(patch.stop)

    def prepare_current(self, *_args, **_kwargs):
        prepared = copy.deepcopy(self.fixture.prepared)
        prepared['setup'] = load_bound_setup(self.fixture.path, self.service.config)
        return prepared

    def inspect(self):
        self.service.select_setup(self.fixture.path)
        return self.service.inspect_cells(self.fixture.path)[0]

    def document(self):
        return build_configuration({'setup': {'path': self.fixture.path, 'backend': 'maestro',
                'identity': {'library': 'design', 'cell': 'TB', 'view': 'maestro'}},
            'selected_cell_scope_ids': ['root'], 'parameter_configs': {},
            'optimization': {'simulator': 'spectre', 'generations': '23'},
            'execution_selection': {'tests': ['C']}},
            parameter_by_id={}, cell_scopes=[self.case.scope], outputs=[])

    def assert_finalized(self, prepared_id, prepared):
        self.assertIn(prepared_id, self.service.prepared)
        self.assertTrue(prepared['setup']['test_corner_masks']['C']['known'])
        self.assertEqual(prepared['setup']['test_corner_masks'], prepared['setup_private_session']['test_corner_masks'])
        self.assertEqual(self.captures, [len(self.case.events)])
        self.assertEqual(self.persists, [len(self.case.events)])
        self.assertTrue(verify_prepared_freshness(prepared, self.service.config)['ok'])
        loaded = load_prepared_baseline(prepared['setup_baseline_snapshot'])
        self.assertNotIn('setup_private_session', loaded)
        self.assertNotIn('resident_validation', loaded)
        self.assertTrue(verify_prepared_freshness(loaded, self.service.config)['ok'])
        self.assertEqual(loaded['setup'], prepared['setup'])
        self.assertEqual(loaded['prepared_freshness'], prepared['prepared_freshness'])
        before = copy.deepcopy(prepared['prepared_freshness'])
        require_prepared_freshness(prepared, self.service.config, action='start optimization')
        require_prepared_freshness(prepared, self.service.config, action='validate parameter ranges')
        self.assertEqual(prepared['prepared_freshness'], before)

    def test_new_load_seals_native_masks_once_after_apply(self):
        token = self.inspect()
        self.assertEqual(self.captures, [])
        prepared_id, prepared = self.service.prepare_cells(token, ['root'])
        self.assert_finalized(prepared_id, prepared)

    def test_configuration_load_seals_same_final_catalogue_without_a_second_load(self):
        result = self.service.load_configuration(self.document(), path=self.fixture.path)
        self.assertTrue(result['ready_for_settings'], result.get('preparation_error'))
        self.assertEqual(result['match']['apply']['optimization']['generations'], '23')
        self.assert_finalized(result['prepared_id'], result['prepared'])

    def test_source_refresh_seals_after_native_mask_mutation(self):
        token = self.inspect()
        self.fixture.write(self.fixture.schematic, 'current user source saved before Apply')
        prepared_id, prepared = self.service.prepare_cells(token, ['root'])
        self.assertTrue(prepared['setup_refresh_warnings'])
        self.assert_finalized(prepared_id, prepared)

    def test_source_changed_during_native_apply_is_not_sealed_or_published(self):
        token = self.inspect()
        apply_native = self.service._apply_setup_private_cells
        def changed(prepared, reference):
            apply_native(prepared, reference)
            self.fixture.write(self.fixture.schematic, 'actual source edit during native Apply')
        with mock.patch.object(self.service, '_apply_setup_private_cells', side_effect=changed):
            with self.assertRaisesRegex(ValueError, 'Source design changed during private Apply'):
                self.service.prepare_cells(token, ['root'])
        self.assertEqual(self.captures, [])
        self.assertEqual(self.persists, [])
        self.assertEqual(self.service.prepared, {})

    def test_frozen_maestro_changed_during_native_apply_is_not_resealed(self):
        token = self.inspect()
        apply_native = self.service._apply_setup_private_cells
        changed_path, original = [], []
        def changed(prepared, reference):
            apply_native(prepared, reference)
            path = prepared['setup']['baseline']['setup_path']
            changed_path.append(path)
            with open(path, 'rb') as stream:
                original.append(stream.read())
            with open(path, 'ab') as stream:
                stream.write(b'changed native snapshot')
        try:
            with mock.patch.object(self.service, '_apply_setup_private_cells', side_effect=changed):
                with self.assertRaisesRegex(ValueError, 'baseline contents changed'):
                    self.service.prepare_cells(token, ['root'])
            self.assertEqual(self.captures, [])
            self.assertEqual(self.persists, [])
            self.assertEqual(self.service.prepared, {})
        finally:
            if changed_path:
                with open(changed_path[0], 'wb') as stream:
                    stream.write(original[0])

    def test_edit_after_apply_stays_stale_for_validate_start_and_native_borrow(self):
        token = self.inspect()
        prepared_id, prepared = self.service.prepare_cells(token, ['root'])
        self.assert_finalized(prepared_id, prepared)
        before = copy.deepcopy(prepared['prepared_freshness'])
        self.fixture.write(self.fixture.schematic, 'actual later source edit')
        for action in ('start optimization', 'validate parameter ranges'):
            with self.assertRaisesRegex(ValueError, 'loaded design is stale'):
                require_prepared_freshness(prepared, self.service.config, action=action)
        self.assertEqual(prepared['prepared_freshness'], before)
        with self.assertRaisesRegex(ValueError, 'source dependencies changed'):
            with sessions.borrow_setup_private_session(prepared, self.service.config,
                    os.path.join(self.fixture.root, 'candidate')):
                self.fail('changed source was borrowed')

    def test_direct_prepare_keeps_its_existing_finalize_and_publish_behavior(self):
        self.service.select_setup(self.fixture.path)
        prepared_id, prepared = self.service.prepare(self.fixture.path, backend='maestro',
            initialize_validation=False, selected_cell_paths=[self.case.scope])
        self.assertIn(prepared_id, self.service.prepared)
        self.assertEqual(self.captures, [0])
        self.assertEqual(self.persists, [0])
        self.assertTrue(verify_prepared_freshness(prepared, self.service.config)['ok'])


if __name__ == '__main__':
    unittest.main()
