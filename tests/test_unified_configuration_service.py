"""Unified configuration import: real matching, mocked native Setup boundaries."""
import copy
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine.cadence import CommandCancelled
from crescendo_engine.server import OptimizerService


class UnifiedConfigurationServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = os.path.join(self.temp.name, 'maestro.sdb')
        self.service = OptimizerService({'runtime': {'run_root': self.temp.name}})
        self.identity = {'library': 'design', 'cell': 'TB', 'view': 'maestro'}
        self.design = {'library': 'design', 'cell': 'TB', 'view': 'config'}
        self.test = dict(self.design, name='main')
        self.scope = {'scope_id': 'root', 'design': self.design, 'hierarchy_path': '/',
                      'library': 'design', 'cell': 'TB', 'view': 'schematic_custom',
                      'master_library': 'design', 'master_cell': 'TB',
                      'master_view': 'schematic_custom', 'selectable': True}
        self.evidence = {'design_library': 'design', 'design_cell': 'TB', 'design_view': 'config',
                         'hierarchy_path': '/M1', 'name': 'w', 'master_library': 'pdk',
                         'master_cell': 'nmos', 'owner_library': 'design', 'owner_cell': 'TB',
                         'owner_view': 'schematic_custom'}
        self.parameter = dict(self.evidence, parameter_id='p1', optimizable=True)
        self.output = {'key': 'main::gain', 'name': 'gain', 'test': 'main'}
        self.setup = {'backend': 'maestro', 'tests': [self.test], 'corners': [
            {'name': 'tt', 'enabled': True}], 'source': {'setup_db': self.path}}
        self.preview = {'setup': self.setup, 'setup_locator': dict(
            self.identity, path=self.path, backend='maestro', library_name='design',
            cell_name='TB', view_name='maestro'), 'cell_scopes': [self.scope],
            'setup_private_session': {'session_id': 'owned-suite'}}
        self.prepared = {'setup': self.setup, 'parameters': [self.parameter],
                         'outputs': [self.output], 'cell_selection': {'scope_ids': ['root']},
                         'setup_private_session': {'session_id': 'owned-suite'}}
        self.document = {'schema_version': 'crescendo.configuration.v1',
            'source_setup': dict(self.identity, path=self.path, backend='maestro',
                identity=self.identity, designs=[self.design], tests=[self.test]),
            'settings': {'schema_version': 'crescendo.ui_settings.v1',
                'setup': dict(identity=self.identity, path=self.path, backend='maestro',
                              designs=[self.design], tests=[self.test]),
                'parameters': [{'parameter_id': 'p1', 'evidence': self.evidence,
                                'config': {'selected': True, 'minimum': '1', 'maximum': '2'}}],
                'cells': [{'scope_id': 'old-root', 'selected': True, 'evidence': {
                    key: copy.deepcopy(self.scope[key]) for key in (
                        'design', 'hierarchy_path', 'library', 'cell', 'view',
                        'master_library', 'master_cell', 'master_view')}}],
                'outputs': [self.output],
                'objectives': [{'output_key': 'main::gain', 'direction': 'maximize',
                                'aggregation': 'mean', 'corners': ['tt']}],
                'constraints': [], 'optimization': {'algorithm': 'differential_evolution',
                    'generations': '10', 'simulator': 'alps', 'job_max_override': True,
                    'job_max_jobs': '3', 'job_timeout_override_run_seconds': True,
                    'job_timeout_run_seconds': '120'}},
            'selection': {'provided': True}, 'execution_selection': {'tests': ['main']}}
        self.calls = []
        self.select_patch = mock.patch.object(self.service, '_select_setup_registered', side_effect=self.select)
        self.inspect_patch = mock.patch.object(self.service, '_inspect_cells_registered', side_effect=self.inspect)
        def inspect_tests(path, backend=None):
            self.test_path, self.test_backend = path, backend
            return 'test-catalog', {'tests': copy.deepcopy(self.setup['tests']), 'setup': copy.deepcopy(self.setup)}
        def inspect_test_cells(token, name, max_depth=32):
            self.assertEqual((token, name), ('test-catalog', 'main'))
            return self.service._inspect_cells_registered(self.test_path, max_depth=max_depth, backend=self.test_backend)
        self.tests_patch = mock.patch.object(self.service, '_inspect_tests_registered', side_effect=inspect_tests)
        self.selected_patch = mock.patch.object(self.service, '_inspect_test_cells_registered', side_effect=inspect_test_cells)
        self.apply_patch = mock.patch.object(self.service, '_prepare_cells_registered', side_effect=self.apply)
        self.eda_patch = mock.patch.object(self.service, 'eda_setup', return_value={'effective': {}})
        for patch in (self.select_patch, self.inspect_patch, self.tests_patch, self.selected_patch, self.apply_patch, self.eda_patch):
            patch.start()
            self.addCleanup(patch.stop)

    def select(self, path):
        self.assertTrue(self.service.eda_reconfiguring)
        self.assertEqual(self.service.lifecycle_operations, 1)
        self.calls.append(('select', path))
        self.service.config['_selected_setup_path'] = path
        self.service.config['_selected_setup_source'] = dict(self.identity)
        return {'setup_path': path, 'source': self.identity, 'context': None}

    def inspect(self, path, **options):
        self.assertEqual(self.service.lifecycle_operations, 1)
        self.calls.append(('inspect', path, options))
        return 'inspection', copy.deepcopy(self.preview)

    def apply(self, token, scopes):
        self.assertEqual(self.service.lifecycle_operations, 1)
        self.calls.append(('apply', token, scopes))
        return 'prepared', copy.deepcopy(self.prepared)

    def test_one_load_owns_select_inspect_apply_and_restores_all_run_preferences(self):
        result = self.service.load_configuration(self.document)
        self.assertEqual([row[0] for row in self.calls], ['select', 'inspect', 'apply'])
        self.service._inspect_tests_registered.assert_called_once()
        self.service._inspect_test_cells_registered.assert_called_once_with('test-catalog', 'main', 32)
        self.assertEqual(result['selected_cell_scope_ids'], ['root'])
        self.assertEqual(result['prepared']['setup_private_session']['session_id'], 'owned-suite')
        self.assertTrue(result['ready_for_settings'])
        application = result['match']['apply']
        self.assertEqual(application['parameter_configs']['p1']['minimum'], '1')
        self.assertEqual(application['objectives'][0]['output_key'], 'main::gain')
        self.assertEqual(application['optimization']['simulator'], 'alps')
        self.assertEqual(application['optimization']['job_timeout_run_seconds'], '120')
        self.assertEqual(application['optimization']['job_max_jobs'], '3')
        self.assertFalse(self.service.eda_reconfiguring)
        self.assertEqual(self.service.lifecycle_operations, 0)

    def test_current_tb_retained_when_saved_source_differs(self):
        current = os.path.join(self.temp.name, 'current', 'maestro.sdb')
        self.service.config['_selected_setup_path'] = current
        result = self.service.load_configuration(self.document)
        self.assertEqual(self.calls[0], ('select', current))
        self.assertTrue(any('another setup' in item for item in result['warnings']))

    def test_explicit_selected_path_wins_over_current_and_saved_paths(self):
        self.service.config['_selected_setup_path'] = '/another/maestro.sdb'
        result = self.service.load_configuration(self.document, path=self.path)
        self.assertEqual(result['setup_path'], self.path)
        self.assertEqual(self.calls[0], ('select', self.path))

    def test_missing_saved_source_uses_current_selection(self):
        self.service.config['_selected_setup_path'] = self.path
        document = copy.deepcopy(self.document)
        document['source_setup'] = {}
        document['settings']['setup'] = {}
        result = self.service.load_configuration(document)
        self.assertEqual(self.calls[0], ('select', self.path))
        self.assertTrue(result['ready_for_settings'])

    def test_missing_any_source_imports_draft_without_opening_native_resources(self):
        document = copy.deepcopy(self.document)
        document['source_setup'] = {}
        document['settings']['setup'] = {}
        result = self.service.load_configuration(document)
        self.assertEqual(self.calls, [])
        self.assertFalse(result['ready_for_settings'])
        self.assertEqual(result['settings']['optimization']['generations'], '10')
        self.assertIn('match', result)

    def test_invoking_source_is_used_without_saved_source(self):
        document = copy.deepcopy(self.document)
        document['source_setup'] = {}
        document['settings']['setup'] = {}
        self.service.config['_maestro_launch_context'] = {'source': {'setup_path': self.path}}
        result = self.service.load_configuration(document)
        self.assertEqual(result['setup_path'], self.path)

    def test_missing_cell_selection_without_parameter_evidence_does_not_select_every_cell(self):
        document = copy.deepcopy(self.document)
        document['settings']['parameters'] = []
        document['settings']['cells'] = []
        document['selection']['provided'] = False
        result = self.service.load_configuration(document)
        self.assertEqual([row[0] for row in self.calls], ['select', 'inspect'])
        self.assertEqual(result['selected_cell_scope_ids'], [])
        self.assertEqual(result['inspection_id'], 'inspection')
        self.assertFalse(result['ready_for_settings'])

    def test_unmatched_cell_is_warning_and_preserves_draft(self):
        document = copy.deepcopy(self.document)
        document['settings']['cells'][0]['evidence']['view'] = 'old_view'
        document['settings']['cells'][0]['evidence']['master_view'] = 'old_view'
        result = self.service.load_configuration(document)
        self.assertFalse(result['ready_for_settings'])
        self.assertIsNone(result['prepared_id'])
        self.assertEqual(result['document']['settings']['cells'][0]['evidence']['view'], 'old_view')

    def test_partial_missing_cells_do_not_prevent_matching_cells_apply(self):
        document = copy.deepcopy(self.document)
        extra = copy.deepcopy(document['settings']['cells'][0])
        extra['scope_id'] = 'removed'
        extra['evidence']['hierarchy_path'] = '/removed'
        document['settings']['cells'].append(extra)
        result = self.service.load_configuration(document)
        self.assertTrue(result['ready_for_settings'])
        self.assertEqual(result['selected_cell_scope_ids'], ['root'])
        self.assertGreater(result['cell_match']['skipped_count'], 0)

    def test_missing_parameters_outputs_and_corners_do_not_block_preparation(self):
        self.prepared['parameters'] = []
        self.prepared['outputs'] = []
        self.prepared['setup']['corners'] = []
        result = self.service.load_configuration(self.document)
        self.assertTrue(result['ready_for_settings'])
        self.assertEqual(result['match']['apply']['parameter_configs'], {})
        self.assertEqual(result['match']['apply']['objectives'], [])
        self.assertTrue(result['warnings'])

    def test_ordinary_inspection_failure_returns_imported_draft_without_qualification(self):
        self.service._inspect_cells_registered.side_effect = RuntimeError('native unavailable')
        result = self.service.load_configuration(self.document)
        self.assertEqual(result['preparation_stage'], 'cell_inspection')
        self.assertIn('native unavailable', result['preparation_error'])
        self.assertFalse(result['ready_for_settings'])
        self.assertIsNone(result['prepared'])
        self.assertEqual(result['settings']['optimization']['simulator'], 'alps')

    def test_ordinary_apply_failure_keeps_inspection_and_draft(self):
        self.service._prepare_cells_registered.side_effect = ValueError('source changed')
        result = self.service.load_configuration(self.document)
        self.assertEqual(result['preparation_stage'], 'apply_selected_cells')
        self.assertEqual(result['inspection_id'], 'inspection')
        self.assertFalse(result['ready_for_settings'])

    def test_cancel_between_inspection_and_apply_stops_chain_with_partial_import(self):
        def stop_after_inspection(path, **options):
            result = self.inspect(path, **options)
            self.assertTrue(self.service.stop_setup_preparation()['stop_requested'])
            return result
        self.service._inspect_cells_registered.side_effect = stop_after_inspection
        with self.assertRaises(CommandCancelled) as raised:
            self.service.load_configuration(self.document)
        self.assertEqual([row[0] for row in self.calls], ['select', 'inspect'])
        self.assertFalse(raised.exception.configuration_import['ready_for_settings'])
        self.assertEqual(self.service.lifecycle_operations, 0)

    def test_unsafe_cleanup_and_session_failures_are_never_suppressed(self):
        for attr, value in (('unsafe_to_optimize', True), ('cleanup_failed', True),
                            ('setup_private_session', {'unsafe': True})):
            with self.subTest(attr=attr):
                error = RuntimeError('unsafe fixture')
                setattr(error, attr, value)
                self.service._inspect_cells_registered.side_effect = error
                with self.assertRaises(RuntimeError) as raised:
                    self.service.load_configuration(self.document)
                self.assertIs(raised.exception, error)
                self.assertIn('document', error.configuration_import)
                self.assertEqual(self.service.lifecycle_operations, 0)

    def test_metadata_display_failure_does_not_replace_original_cancellation(self):
        error = CommandCancelled('fixture cancellation')
        self.service._inspect_cells_registered.side_effect = error
        self.service.eda_setup.side_effect = ValueError('display failure')
        with self.assertRaises(CommandCancelled) as raised:
            self.service.load_configuration(self.document)
        self.assertIs(raised.exception, error)
        self.assertTrue(any('display' in item for item in error.configuration_import['warnings']))

    def test_busy_service_imports_draft_without_starting_setup(self):
        self.service._begin_lifecycle_operation('another operation', exclusive=True)
        try:
            result = self.service.load_configuration(self.document)
            self.assertFalse(result['ready_for_settings'])
            self.assertEqual(self.calls, [])
            self.assertEqual(self.service.lifecycle_operations, 1)
        finally:
            self.service._end_lifecycle_operation(exclusive=True)

    def test_legacy_design_config_without_cell_evidence_is_imported_as_draft(self):
        document = {'schema_version': 'crescendo.design_config.v1',
                    'ade_setup': {'path': self.path, 'backend': 'maestro'},
                    'optimization': {'variables': [{'parameter_id': 'p1', 'minimum': 1, 'maximum': 2}],
                        'objectives': [{'output_key': 'main::gain', 'direction': 'maximize'}],
                        'constraints': [], 'run': {'generations': 8}}}
        result = self.service.load_configuration(document)
        self.assertEqual([row[0] for row in self.calls], ['select', 'inspect'])
        self.assertEqual(result['test_inspection_id'], 'test-catalog')
        self.assertEqual(result['inspection_id'], 'inspection')
        self.assertTrue(result['normalized']['legacy'])
        self.assertFalse(result['ready_for_settings'])
        self.assertEqual(result['settings']['optimization']['generations'], 8)
        document['execution_selection'] = {'tests': ['main']}
        result = self.service.load_configuration(document)
        self.assertEqual(result['selected_test'], 'main')
        self.assertEqual(result['inspection_id'], 'inspection')
        self.assertFalse(result['ready_for_settings'])
        self.assertEqual([row[0] for row in self.calls], ['select', 'inspect', 'select', 'inspect'])

    def test_previous_ui_state_without_test_uses_single_available_test(self):
        result = self.service.load_configuration(copy.deepcopy(self.document['settings']))
        self.assertTrue(result['ready_for_settings'])
        self.assertEqual(result['selected_test'], 'main')
        self.assertEqual([row[0] for row in self.calls], ['select', 'inspect', 'apply'])

    def test_current_suite_matches_only_selected_cells_without_native_work(self):
        self.service.inspections['live'] = {'preview': copy.deepcopy(self.preview), 'prepared_ids': ['live-prepared']}
        self.service.prepared['live-prepared'] = copy.deepcopy(self.prepared)
        self.service.setup_private_sessions['owned-suite'] = {'session_id': 'owned-suite'}
        document = copy.deepcopy(self.document)
        document['private_suite'] = {'suite_id': 'a' * 32, 'source_path': self.path}
        before = copy.deepcopy((self.service.inspections, self.service.prepared, self.service.setup_private_sessions))
        with mock.patch.object(self.service, 'load_private_suite') as reopen:
            result = self.service.load_configuration(document, selected_scope_ids=['root'])
            reopen.assert_not_called()
        self.assertTrue(result['preserve_current_suite'])
        self.assertEqual(result['match']['apply']['parameter_configs']['p1']['minimum'], '1')
        self.assertFalse(result['match']['selection_present'])
        self.assertEqual(before, (self.service.inspections, self.service.prepared, self.service.setup_private_sessions))
        self.assertEqual(self.calls, [])
        result = self.service.load_configuration(document, selected_scope_ids=[])
        self.assertEqual(result['match']['apply']['parameter_configs'], {})
        self.assertTrue(any('cell' in warning.lower() for warning in result['warnings']))
        self.assertTrue(any('p1' in warning for warning in result['warnings']))

    def test_current_suite_cancel_leaves_ownership_and_settings_untouched(self):
        self.service.inspections['live'] = {'preview': copy.deepcopy(self.preview), 'prepared_ids': ['live-prepared']}
        self.service.prepared['live-prepared'] = copy.deepcopy(self.prepared)
        self.service.setup_private_sessions['owned-suite'] = {'session_id': 'owned-suite'}
        before = copy.deepcopy((self.service.inspections, self.service.prepared, self.service.setup_private_sessions))
        with mock.patch('crescendo_engine.server.check_command_cancellation', side_effect=CommandCancelled('stop')):
            with self.assertRaises(CommandCancelled) as raised:
                self.service.load_configuration(self.document, selected_scope_ids=['root'])
        self.assertTrue(raised.exception.configuration_import['preserve_current_suite'])
        self.assertEqual(before, (self.service.inspections, self.service.prepared, self.service.setup_private_sessions))
        self.assertEqual(self.service.lifecycle_operations, 0)

    def test_cancel_before_suite_creation_restores_source_and_registry(self):
        previous_config = copy.deepcopy(self.service.config)
        self.service.test_inspections['old'] = {'epoch': 0}
        previous_tests = copy.deepcopy(self.service.test_inspections)
        def cancel(path, **options):
            self.service.config.setdefault('cadence', {})['simulator_threads'] = 8
            self.service.test_inspections.clear()
            raise CommandCancelled('cancelled before creation')
        self.service._inspect_cells_registered.side_effect = cancel
        with self.assertRaises(CommandCancelled) as raised:
            self.service.load_configuration(self.document)
        self.assertEqual(self.service.config, previous_config)
        self.assertEqual(self.service.test_inspections, previous_tests)
        self.assertTrue(raised.exception.configuration_import['cancelled'])

    def test_cancel_after_creation_retains_suite_and_its_source_binding(self):
        def cancel(path, **options):
            self.service.setup_private_sessions['pending'] = {'session_id': 'pending', 'root': '/pending'}
            raise CommandCancelled('cancelled after creation')
        self.service._inspect_cells_registered.side_effect = cancel
        with self.assertRaises(CommandCancelled) as raised:
            self.service.load_configuration(self.document)
        self.assertIn('pending', self.service.setup_private_sessions)
        self.assertEqual(self.service.config['_selected_setup_path'], self.path)
        self.assertTrue(any('retained working-copy resources' in item for item in
                            raised.exception.configuration_import['warnings']))
        self.assertEqual(self.service.lifecycle_operations, 0)
        result = self.service.load_configuration(self.document)
        self.assertTrue(result['preserve_current_suite'])
        self.assertIn('pending', self.service.setup_private_sessions)
        self.assertEqual([row[0] for row in self.calls], ['select'])

    def test_adexl_source_retains_backend_through_inspection(self):
        document = copy.deepcopy(self.document)
        document['source_setup']['path'] = os.path.join(self.temp.name, 'data.sdb')
        document['source_setup']['backend'] = 'adexl'
        result = self.service.load_configuration(document, max_depth=17)
        self.assertTrue(result['ready_for_settings'])
        self.assertEqual(self.calls[1][2], {'max_depth': 17, 'backend': 'adexl'})

    def test_next_load_after_stop_can_finish_with_same_preferences(self):
        def stop_after_inspection(path, **options):
            result = self.inspect(path, **options)
            self.service.stop_setup_preparation()
            return result
        self.service._inspect_cells_registered.side_effect = stop_after_inspection
        with self.assertRaises(CommandCancelled):
            self.service.load_configuration(self.document)
        self.service._inspect_cells_registered.side_effect = self.inspect
        result = self.service.load_configuration(self.document)
        self.assertTrue(result['ready_for_settings'])
        self.assertFalse(self.service.prepare_cancel_event.is_set())
        self.assertEqual(result['match']['apply']['optimization']['job_max_jobs'], '3')

    def test_valid_solver_executable_requires_read_and_execute_permission(self):
        executable = os.path.join(self.temp.name, 'spectre-no-execute')
        with open(executable, 'w') as stream:
            stream.write('fixture')
        os.chmod(executable, 0o600)
        document = copy.deepcopy(self.document)
        document['environment_overrides'] = {'spectre_bin': executable}
        result = self.service.load_configuration(document)
        self.assertTrue(result['ready_for_settings'])
        self.assertEqual(result['restored_environment_overrides'], {})
        self.assertTrue(any('unavailable' in item for item in result['warnings']))

    def test_valid_solver_overrides_restore_without_persisting_or_full_eda_validation(self):
        executable = os.path.join(self.temp.name, 'alps')
        with open(executable, 'w') as stream:
            stream.write('#!/bin/sh\nexit 0\n')
        os.chmod(executable, 0o700)
        document = copy.deepcopy(self.document)
        document['environment_overrides'] = {'alps_bin': executable, 'alps_root': self.temp.name,
            'alps_model_dir': self.temp.name, 'simulator_threads': '8', 'alps_result_reader': 'spectre'}
        with mock.patch.object(self.service, 'update_eda_setup') as update, \
                mock.patch('crescendo_engine.server.persist_eda_setup') as persist:
            result = self.service.load_configuration(document)
        update.assert_not_called()
        persist.assert_not_called()
        self.assertTrue(result['ready_for_settings'])
        self.assertEqual(self.service.config['cadence']['alps_bin'], executable)
        self.assertEqual(self.service.config['cadence']['simulator_threads'], 8)
        self.assertEqual(self.service.config['cadence']['_manual_overrides']['alps_result_reader'], 'spectre')

    def test_stale_paths_and_protected_startup_fields_are_retained_as_draft_only(self):
        self.service.config['cadence'] = {'alps_bin': '/current/alps', 'cds_lib': '/current/cds.lib',
            'pdk_adapter': '/current/adapter', 'launch_environment': {'PATH': '/native/bin'}}
        before = copy.deepcopy(self.service.config['cadence'])
        document = copy.deepcopy(self.document)
        document['environment_overrides'] = {'alps_bin': '/missing/alps', 'alps_model_dir': '/missing/models',
            'simulator_threads': 0, 'alps_result_reader': 'unknown', 'cds_lib': '/foreign/cds.lib',
            'pdk_adapter': '/foreign/adapter', 'ocean_bin': '/foreign/ocean',
            'startup_extensions': ['/foreign/start.il'], 'alps_model_file_map': {'old': 'new'}}
        result = self.service.load_configuration(document)
        self.assertEqual(self.service.config['cadence'], before)
        self.assertTrue(result['ready_for_settings'])
        self.assertEqual(result['restored_environment_overrides'], {})
        self.assertEqual(result['document']['environment_overrides']['alps_bin'], '/missing/alps')
        self.assertTrue(result['warnings'])

    def test_thread_override_accepts_only_positive_whole_number(self):
        for value in (True, False, 0, -1, 1.5, '1.5', 'nan', 'inf', None, '', 4097):
            with self.subTest(value=value):
                document = {'environment_overrides': {'simulator_threads': value}}
                warnings = []
                accepted = self.service._restore_configuration_solver_overrides(document, warnings)
                self.assertEqual(accepted, {})
                self.assertTrue(warnings)

    def test_environment_values_already_in_current_environment_do_not_warn(self):
        self.service.config['cadence'] = {'cds_lib': '/current/cds.lib',
            '_manual_overrides': {'pdk_adapter': '/current/adapter'}}
        warnings = []
        accepted = self.service._restore_configuration_solver_overrides({
            'environment_overrides': {'cds_lib': '/current/cds.lib',
                'pdk_adapter': '/current/adapter'}}, warnings)
        self.assertEqual(accepted, {})
        self.assertEqual(warnings, [])

    def test_project_file_override_remains_authoritative_over_imported_manual_value(self):
        self.service.config['cadence'] = {'simulator_threads': 4,
            '_file_overrides': {'simulator_threads': 4}}
        warnings = []
        accepted = self.service._restore_configuration_solver_overrides({
            'environment_overrides': {'simulator_threads': 8}}, warnings)
        self.assertEqual(accepted, {})
        self.assertEqual(self.service.config['cadence']['simulator_threads'], 4)
        self.assertTrue(any('project override' in item for item in warnings))

    def test_loader_uses_real_setup_private_create_apply_registry_and_keeps_source_unchanged(self):
        from tests import test_setup_private_ui as fixtures
        from crescendo_engine.unified_configuration import build_configuration
        from crescendo_engine.utils import sha256_file
        fixture = fixtures.SetupPrivateServiceIntegrationTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        before = sha256_file(fixture.fixture.path)
        document = build_configuration({
            'setup': {'path': fixture.fixture.path, 'backend': 'maestro',
                      'identity': {'library': 'design', 'cell': 'TB', 'view': 'maestro'}},
            'selected_cell_scope_ids': ['root'], 'parameter_configs': {},
            'optimization': {'simulator': 'spectre'}, 'execution_selection': {'tests': ['C']}},
            parameter_by_id={}, cell_scopes=[fixture.scope], outputs=[])
        def prepare_current(*_args, **_kwargs):
            from crescendo_engine.setup_baseline import load_bound_setup
            value = copy.deepcopy(fixture.fixture.prepared)
            # A real prepare uses the newly selected baseline, not this
            # fixture's previously frozen setup from before Load Configuration.
            value['setup'] = load_bound_setup(fixture.fixture.path, fixture.service.config)
            return value
        with mock.patch('crescendo_engine.hierarchy.probe_hierarchy', return_value=fixture.fixture.hierarchy), \
                mock.patch('crescendo_engine.hierarchy.cell_scope_records', return_value=[fixture.scope]), \
                mock.patch('crescendo_engine.server.prepare_setup',
                    side_effect=prepare_current):
            result = fixture.service.load_configuration(document, path=fixture.fixture.path)
        self.assertTrue(result['ready_for_settings'], result.get('preparation_error'))
        self.assertEqual(fixture.events, ['bind', 'bind'])
        inspected = result['inspection']['setup_private_session']
        prepared = result['prepared']['setup_private_session']
        self.assertEqual(inspected['session_id'], prepared['session_id'])
        self.assertIn(result['prepared_id'], fixture.service.prepared)
        self.assertEqual(before, sha256_file(fixture.fixture.path))
        self.assertTrue(os.path.isdir(prepared['maestro']['path']))
        original_sessions = copy.deepcopy(fixture.service.setup_private_sessions)
        again = fixture.service.load_configuration(document, path='/unrelated/source', selected_scope_ids=['root'])
        self.assertTrue(again['preserve_current_suite'])
        self.assertEqual(again['cell_match']['matched_count'], 1)
        self.assertEqual(fixture.events, ['bind', 'bind'])
        self.assertEqual(original_sessions, fixture.service.setup_private_sessions)
        skipped = fixture.service.load_configuration(document, selected_scope_ids=[])
        self.assertEqual(skipped['cell_match']['matched_count'], 0)
        self.assertTrue(any('cell' in warning.lower() for warning in skipped['warnings']))
        self.assertEqual(original_sessions, fixture.service.setup_private_sessions)

    def test_numeric_text_thread_count_matches_current_project_without_warning(self):
        self.service.config['cadence'] = {'simulator_threads': 8,
            '_file_overrides': {'simulator_threads': 8}}
        warnings = []
        accepted = self.service._restore_configuration_solver_overrides({
            'environment_overrides': {'simulator_threads': '8'}}, warnings)
        self.assertEqual(accepted, {})
        self.assertEqual(warnings, [])
        self.assertEqual(self.service.config['cadence']['simulator_threads'], 8)

    def test_boolean_threads_cannot_borrow_current_integer_equality(self):
        self.service.config['cadence'] = {'simulator_threads': 1}
        warnings = []
        accepted = self.service._restore_configuration_solver_overrides({
            'environment_overrides': {'simulator_threads': True}}, warnings)
        self.assertEqual(accepted, {})
        self.assertTrue(warnings)
        self.assertEqual(self.service.config['cadence']['simulator_threads'], 1)


if __name__ == '__main__':
    unittest.main()
