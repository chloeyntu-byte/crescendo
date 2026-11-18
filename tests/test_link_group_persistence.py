"""Configuration fields and real runner/dataset round trips (no EDA)."""
import copy
import csv
import io
import json
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine import runner, settings_state, unified_configuration
from crescendo_engine.utils import load_json
from tests import test_unified_configuration as configuration_fixture
from tests import test_unified_optimizer as run_fixture
from tests.test_runner import _coordinated_runner_resume


class LinkGroupPersistenceTests(unittest.TestCase):
    def test_configuration_legacy_state_and_last_restore_preserve_all_optional_fields(self):
        fixture = configuration_fixture.UnifiedConfigurationTests()
        fixture.setUp()
        try:
            extras = {'step': '1', 'grid_origin': 0, 'link_group': 'nf-pair'}
            fixture.raw['parameter_configs']['opaque-id'].update(extras)
            document = fixture.document()
            unified_configuration.save_configuration(fixture.path, document)
            loaded = unified_configuration.read_configuration(fixture.path)
            matched = unified_configuration.match_configuration(loaded, fixture.current())
            actual = matched['apply']['parameter_configs']['opaque-id']
            self.assertEqual({key: actual[key] for key in extras}, extras)
            legacy = fixture.legacy()
            legacy['optimization']['variables'][0].update(extras)
            matched = unified_configuration.match_configuration(legacy, fixture.current())
            actual = matched['apply']['parameter_configs']['opaque-id']
            self.assertEqual({key: actual[key] for key in extras}, extras)
            state = settings_state.build_settings_state(fixture.raw, {'opaque-id': fixture.parameter})
            settings_state.save_settings_state(fixture.path, state)
            actual = settings_state.match_settings_state(settings_state.read_settings_state(fixture.path),
                                                        fixture.current())['apply']['parameter_configs']['opaque-id']
            self.assertEqual({key: actual[key] for key in extras}, extras)
            config = {'runtime': {'state_root': fixture.directory.name}}
            # Use the existing last-state path API; isolate the path explicitly.
            with mock.patch.object(settings_state, 'last_settings_path', return_value=fixture.path):
                settings_state.save_last_settings_state(state, config=config)
                restored = settings_state.read_last_settings_state(config=config, current_setup=fixture.setup)
                matched = settings_state.match_settings_state(restored, fixture.current())
                actual = matched['apply']['parameter_configs']['opaque-id']
                self.assertEqual({key: actual[key] for key in extras}, extras)
        finally:
            fixture.doCleanups()

    def test_working_copy_configuration_store_keeps_optional_parameter_fields(self):
        import threading
        from types import SimpleNamespace
        from crescendo_engine.server import OptimizerService
        fixture = configuration_fixture.UnifiedConfigurationTests()
        fixture.setUp()
        try:
            extras = {'step': '1', 'grid_origin': 0, 'link_group': 'paired'}
            fixture.raw['parameter_configs']['opaque-id'].update(extras)
            service = object.__new__(OptimizerService)
            controller = SimpleNamespace(lock=threading.Lock(), closed=False, unsafe=False, prepared={'setup': {}})
            with mock.patch.object(service, '_suite_reference_for_id', return_value={'root': fixture.directory.name}), mock.patch(
                    'crescendo_engine.private_suite_service.sessions._controller', return_value=controller), mock.patch(
                    'crescendo_engine.setup_baseline.origin_setup_path', return_value=fixture.setup['path']):
                result = service.save_private_suite_configuration('fixture', fixture.document())
            self.assertTrue(result['saved'])
            document = unified_configuration.read_configuration(os.path.join(fixture.directory.name, 'configuration.json'))
            actual = unified_configuration.match_configuration(document, fixture.current())['apply']['parameter_configs']['opaque-id']
            self.assertEqual({key: actual[key] for key in extras}, extras)
        finally:
            fixture.doCleanups()

    def test_old_configuration_has_no_new_fields_or_default_group(self):
        fixture = configuration_fixture.UnifiedConfigurationTests()
        fixture.setUp()
        try:
            for document in (fixture.document(), fixture.legacy()):
                matched = unified_configuration.match_configuration(document, fixture.current())
                actual = matched['apply']['parameter_configs']['opaque-id']
                self.assertFalse(set(actual) & {'step', 'grid_origin', 'link_group'})
        finally:
            fixture.doCleanups()

    def test_gui_variable_collection_and_static_range_validation_preserve_grid_and_group(self):
        from crescendo_engine.gui_state import validate_variable_configs
        from crescendo_engine.range_validation import validate_variable_request_report
        parameters = [{'parameter_id': key, 'optimizable': True, 'value_type': 'float'} for key in ('a', 'b')]
        configs = {'a': {'selected': True, 'minimum': '1', 'maximum': '5', 'step': '1', 'grid_origin': 0, 'link_group': 'pair'},
                   'b': {'selected': True, 'minimum': '10', 'maximum': '50', 'step': '10', 'grid_origin': 0, 'link_group': 'pair'}}
        report = validate_variable_configs(parameters, configs)
        self.assertFalse(report['issues'])
        validated = validate_variable_request_report({'parameters': parameters}, report['variables'])
        self.assertTrue(validated['valid'], validated)
        self.assertEqual(validated['variables'], report['variables'])
        configs['b']['step'] = '20'
        report = validate_variable_configs(parameters, configs)
        self.assertIn("link_group 'pair' member b", report['issues'][0]['message'])
        variables = copy.deepcopy(validated['variables']); variables[1]['step'] = 20
        validated = validate_variable_request_report({'parameters': parameters}, variables)
        self.assertFalse(validated['valid'])
        self.assertIn("link_group 'pair' member b", validated['issues'][0]['message'])

    def test_runner_checkpoint_dataset_export_service_csv_and_resume(self):
        from crescendo_engine.run_data_store import CORE_NAME, export_dataset, load_dataset
        from crescendo_engine.server import OptimizerService
        fixture = run_fixture.UnifiedOptimizerTests()
        fixture.setUp()
        try:
            fixture.prepared['parameters'].append({'parameter_id': 'y', 'backend': 'schematic', 'optimizable': True})
            fixture.spec['variables'] = [{'parameter_id': 'x', 'minimum': 0, 'maximum': 1, 'step': 0.25, 'grid_origin': 0, 'link_group': 'pair'},
                                         {'parameter_id': 'y', 'minimum': 10, 'maximum': 20, 'step': 2.5, 'grid_origin': 0, 'link_group': 'pair'}]
            stopped, _ = fixture.run_fixture(behavior='stop_after_first')
            self.assertEqual(stopped['state'], 'stopped')
            checkpoint = load_json(os.path.join(fixture.root, 'run', 'checkpoint.json'))
            self.assertEqual(checkpoint['schema_version'], 'crescendo.checkpoint.v2')
            instances = []
            with mock.patch.object(runner, 'PrivateNetlistOptimizer', run_fixture.common_adapter(instances)):
                result = _coordinated_runner_resume(checkpoint['spec'], fixture.config, fixture.prepared,
                                                   os.path.join(fixture.root, 'resumed'), checkpoint)
            self.assertEqual(result['state'], 'completed')
            self.assertEqual(len(result['candidates']), 8)
            for row in result['candidates']:
                self.assertEqual(set(row['values']), {'x', 'y'})
                self.assertEqual(row['values']['y'], 10 + 10 * row['values']['x'])
            exported = os.path.join(fixture.root, 'export.crescendo')
            export_dataset(os.path.join(fixture.root, 'resumed', CORE_NAME), exported)
            data = load_dataset(exported)
            self.assertEqual([row['link_group'] for row in data['metadata']['spec']['variables']], ['pair', 'pair'])
            self.assertEqual(data['metadata']['spec']['variables'][0]['step'], 0.25)
            self.assertEqual(data['metadata']['spec']['variables'][0]['grid_origin'], 0.0)
            service = object.__new__(OptimizerService)
            with mock.patch.object(service, 'progress', return_value={'spec': checkpoint['spec'], 'candidates': result['candidates']}):
                csv_rows = list(csv.DictReader(io.StringIO(service.candidates_csv('fixture'))))
            self.assertEqual(csv_rows[0]['link_group:x'], 'pair')
            self.assertEqual(csv_rows[0]['link_group:y'], 'pair')
            with mock.patch.object(service, '_resolve_run_checkpoint_snapshot', return_value={'checkpoint': checkpoint}):
                self.assertEqual(service.export_checkpoint('fixture')['schema_version'], 'crescendo.checkpoint.v2')
        finally:
            fixture.tearDown()

    def test_diagnostic_finite_grid_enumeration_keeps_members_linked(self):
        from crescendo_engine.diagnostic_plan import freeze_plan
        variables = [{'parameter_id': 'a', 'minimum': 0, 'maximum': 1, 'step': 1, 'link_group': 'pair'},
                     {'parameter_id': 'b', 'minimum': 10, 'maximum': 20, 'step': 10, 'link_group': 'pair'}]
        plan, shape = freeze_plan({'variables': variables, 'diagnostic_generations': 2,
                                   'diagnostic_samples': 3}, {'a': 0, 'b': 10})
        self.assertEqual(shape['distinct_candidates'], 2)
        for row in plan:
            self.assertEqual(row['values']['b'], 10 + row['values']['a'] * 10)
