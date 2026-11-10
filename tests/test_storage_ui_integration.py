"""Storage UI behavior, blocking failures and portable result authority."""
from __future__ import print_function

import copy
import errno
import json
import os
import shutil
import tempfile
import threading
import time
import unittest
from unittest import mock

from crescendo_engine import gui
from crescendo_engine.gui_state import RunUiState
from crescendo_engine.run_data_store import CORE_NAME, RunDataStore, load_dataset
from crescendo_engine.server import OptimizerService
from crescendo_engine import settings_state
from crescendo_engine.runtime_layout import design_storage_root
from tests import test_gui_optional_settings as _optional_fixtures


class StorageUiBackendTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='crescendo-storage-ui-')
        self.addCleanup(self.temp.cleanup)
        self.root = self.temp.name
        self.config = {'runtime': {'run_root': self.root}}
        self.service = OptimizerService(self.config)

    def run(self, *args, **kwargs):
        return super(StorageUiBackendTests, self).run(*args, **kwargs)

    def _run(self, name='large'):
        root = os.path.join(self.root, name)
        os.makedirs(root)
        with open(os.path.join(root, 'status.json'), 'w') as stream:
            json.dump({'schema_version': 'crescendo.run_status.v1', 'run_id': name,
                       'run_root': root, 'state': 'running'}, stream)
        store = RunDataStore(os.path.join(root, CORE_NAME), metadata={'spec': {'objectives': [], 'constraints': []}, 'run_id': name})
        self.addCleanup(store.close)
        return root, store

    @staticmethod
    def row(index, status='planned'):
        return {'candidate_id': 'g000_c%04d' % index, 'generation': 0, 'index': index,
                'status': status, 'values': {'width': index + 1}, 'metrics': {}}

    def test_thousand_candidates_incremental_poll_does_not_open_checkpoint_or_candidates(self):
        root, store = self._run()
        store.commit_candidates([self.row(index) for index in range(1000)])
        original = self.service._read_run_json_snapshot
        def read(name, relative, *args, **kwargs):
            self.assertNotEqual(relative, 'checkpoint.json')
            self.assertFalse(relative.startswith('candidates/'))
            return original(name, relative, *args, **kwargs)
        with mock.patch.object(self.service, '_read_run_json_snapshot', side_effect=read):
            first = self.service.progress_delta('large')
            self.assertEqual(len(first['candidates']), 1000)
            revision = first['revision']
            row = self.row(500, 'infrastructure_failed')
            row.update(error='shared template preparation failed; candidate not executed', spectre_state='not_started')
            store.commit_candidates([row])
            second = self.service.progress_delta('large', revision)
            self.assertNotIn('candidates', second)
            self.assertEqual([item['candidate_id'] for item in second['changed_candidates']], [row['candidate_id']])
            for _index in range(4):
                current = self.service.progress_delta('large', second['revision'])
                self.assertEqual(current['changed_candidates'], [])
                self.assertNotIn('candidates', current)
        self.assertFalse(os.path.exists(os.path.join(root, 'candidates')))

    def test_compacted_success_retains_native_candidate_and_removed_log_state(self):
        _root, store = self._run()
        row = self.row(0, 'completed')
        row['completion_evidence'] = {'ok': True, 'parameters': {'width': 1}}
        row['parameter_application'] = {'ok': True, 'parameters': []}
        store.commit_candidates([row])
        self.assertEqual(self.service._native_candidate_record('large', row['candidate_id']), row)
        payload = self.service.candidate_logs('large', row['candidate_id'])
        self.assertEqual(payload['logs'], [])
        self.assertEqual(payload['candidate_status'], 'completed')

    def test_storage_freeze_keeps_service_config_and_tracks_old_override_root(self):
        work = os.path.join(self.root, 'work')
        os.makedirs(work)
        data = os.path.join(self.root, 'data')
        config = {'cadence': {'project_dir': work}, '_selected_setup_source': {
                    'library': 'lib', 'cell': 'TB', 'view': 'maestro', 'work_dir': work},
                  'runtime': {'run_root': self.root, 'run_root_origin': 'derived', 'design_root': os.path.join(work, '.crescendo', 'TB')}}
        service = OptimizerService(config)
        frozen = service._freeze_run_config({'evaluator': 'real', 'data_root_override': data}, {'setup': {}})
        self.assertEqual(config['runtime']['run_root'], self.root)
        self.assertTrue(frozen['runtime']['run_root'].startswith(data + os.sep))
        os.makedirs(os.path.join(frozen['runtime']['run_root'], 'known'))
        warning = service._register_run_location('known', frozen['runtime']['run_root'], frozen)
        self.assertIsNone(warning)
        config['runtime']['run_root'] = os.path.join(self.root, 'new-default')
        self.assertEqual(service.run_path('known', ''), os.path.join(frozen['runtime']['run_root'], 'known'))
        recreated = OptimizerService(config)
        self.assertEqual(recreated.run_path('known', ''), service.run_path('known', ''))
        self.assertEqual(recreated._freeze_run_config({}, {}, checkpoint={'manifest': {'origin_run_id': 'known'}})['runtime']['run_root'], frozen['runtime']['run_root'])

    def test_same_tb_two_data_roots_cannot_bypass_work_lease(self):
        config = {'runtime': {'run_root': os.path.join(self.root, 'dataA'), 'design_root': os.path.join(self.root, 'work', 'TB')}}
        first = self.service._acquire_tb_lease(config)
        self.addCleanup(first.release)
        other = copy.deepcopy(config)
        other['runtime']['run_root'] = os.path.join(self.root, 'dataB')
        with self.assertRaises(RuntimeError):
            self.service._acquire_tb_lease(other)
        first.release()
        second = self.service._acquire_tb_lease(other)
        second.release()

    def test_unknown_save_options_falls_back_to_work_simulation_not_local(self):
        work = os.path.join(self.root, 'work')
        config = {'cadence': {'project_dir': work}, 'runtime': {'run_root': '/obsolete/.local/runs', 'run_root_origin': 'derived'}}
        service = OptimizerService(config)
        frozen = service._freeze_run_config({'evaluator': 'real', 'data_root_override': None}, {'setup': {}})
        self.assertTrue(frozen['runtime']['run_root'].startswith(os.path.join(work, 'simulation', 'crescendo') + os.sep))
        self.assertEqual(frozen['runtime']['storage_policy']['source'], 'working_directory_fallback')
        self.assertNotEqual(config['runtime']['run_root'], frozen['runtime']['run_root'])

    def test_last_snapshot_and_manual_backups_are_bounded(self):
        config = {'runtime': {'settings_root': os.path.join(self.root, 'states')}}
        document = settings_state.build_settings_state({'setup': {'identity': {'library': 'lib', 'cell': 'TB', 'view': 'maestro'}}})
        for _index in range(20):
            settings_state.save_last_settings_state(document, config)
        self.assertEqual(os.listdir(config['runtime']['settings_root']), ['last.json'])
        path = os.path.join(config['runtime']['settings_root'], 'preset.json')
        for _index in range(8):
            settings_state.save_settings_state(path, document)
        self.assertEqual(len([name for name in os.listdir(os.path.dirname(path)) if name.startswith('preset.json.bak-')]), 3)

    def test_last_migrated_namespace_matches_only_same_lcv(self):
        source = {'library': 'lib', 'cell': 'TB', 'view': 'maestro', 'work_dir': self.root,
                  'setup_path': os.path.join(self.root, 'lib', 'TB', 'maestro', 'maestro.sdb')}
        new_root = design_storage_root(source, {})
        old = os.path.join(self.root, '.crescendo', os.path.basename(new_root).rsplit('_', 1)[0] + '_0123456789abcdef', 'states')
        current_setup = {'identity': {key: source[key] for key in ('library', 'cell', 'view')}}
        doc = settings_state.build_settings_state({'setup': current_setup, 'optimization': {'generations': '7'}})
        settings_state.save_settings_state(os.path.join(old, 'last.json'), doc, retain_backups=0)
        config = {'cadence': {'project_dir': self.root}, '_selected_setup_source': source,
                  'runtime': {'design_root': new_root, 'settings_root': os.path.join(new_root, 'states')}}
        restored = settings_state.read_last_settings_state(config, current_setup=current_setup)
        self.assertEqual(restored['optimization']['generations'], '7')
        self.assertEqual(restored['_restore_source'], os.path.join(old, 'last.json'))
        other = copy.deepcopy(config)
        other['_selected_setup_source']['library'] = 'other'
        other['runtime']['settings_root'] = os.path.join(self.root, '.crescendo', 'other', 'states')
        with self.assertRaises(ValueError):
            settings_state.read_last_settings_state(other, current_setup={'identity': {'library': 'other', 'cell': 'TB', 'view': 'maestro'}})

    def test_relocated_absolute_data_override_is_not_automatically_enabled(self):
        identity = {'library': 'lib', 'cell': 'TB', 'view': 'maestro'}
        raw = {'setup': {'identity': identity, 'path': '/old/work/lib/TB/maestro'},
               'optimization': {'data_root_override_enabled': True, 'data_root_override': '/old/data',
                                'job_command_override': True, 'job_command': 'batch-submit -q original',
                                'job_max_override': True, 'job_max_jobs': '50'}}
        document = settings_state.build_settings_state(raw)
        current = {'setup': {'identity': identity, 'path': '/new/work/lib/TB/maestro'}}
        plan = settings_state.match_settings_state(document, current)
        self.assertFalse(plan['apply']['optimization']['data_root_override_enabled'])
        self.assertEqual(plan['apply']['optimization']['data_root_override'], '/old/data')
        self.assertTrue(plan['apply']['optimization']['job_command_override'])
        self.assertTrue(plan['apply']['optimization']['job_max_override'])
        self.assertIn('relocation', plan['storage_warning'])
        current['setup']['path'] = raw['setup']['path']
        unchanged = settings_state.match_settings_state(document, current)
        self.assertTrue(unchanged['apply']['optimization']['data_root_override_enabled'])
        self.assertNotIn('storage_warning', unchanged)

    def test_compact_result_reads_fixed_prefix_and_preserves_failed_summary(self):
        root, store = self._run()
        store.commit_candidates([self.row(0, 'completed')])
        reference = store.reference()
        # The final publication state can be failed despite some successful rows.
        with open(os.path.join(root, 'result.json'), 'w') as stream:
            json.dump({'schema_version': 'crescendo.run_result.v2', 'state': 'failed',
                       'candidate_store': reference, 'pareto_candidate_ids': ['g000_c0000'],
                       'representative_candidate_id': 'g000_c0000'}, stream)
        result = self.service.read('large', 'result.json')
        self.assertEqual(result['state'], 'failed')
        self.assertEqual(len(result['candidates']), 1)
        self.assertEqual(result['representative']['candidate_id'], 'g000_c0000')
        self.assertEqual(len(result['pareto_front']), 1)
        store.commit_candidates([self.row(1, 'completed')])
        self.assertEqual(len(self.service.read('large', 'result.json')['candidates']), 1)

    def test_native_export_cannot_use_display_core_without_fixed_checkpoint(self):
        _root, store = self._run()
        row = self.row(0, 'completed')
        row['completion_evidence'] = {'ok': True}
        store.commit_candidates([row])
        self.assertEqual(self.service._native_candidate_record('large', row['candidate_id']), row)
        with self.assertRaisesRegex(ValueError, 'fixed core checkpoint'):
            self.service._native_candidate_record('large', row['candidate_id'], strict=True)

    def test_two_ui_index_registrations_merge_without_losing_an_older_data_root(self):
        design = os.path.join(self.root, 'work', 'TB')
        config = {'runtime': {'run_root': self.root, 'design_root': design}}
        first, second = OptimizerService(config), OptimizerService(copy.deepcopy(config))
        a, b = os.path.join(self.root, 'data-A'), os.path.join(self.root, 'data-B')
        os.makedirs(os.path.join(a, 'round_A'))
        os.makedirs(os.path.join(b, 'round_B'))
        start = threading.Event()
        outcomes = []
        def register(service, name, path):
            start.wait(2)
            outcomes.append(service._register_run_location(name, path))
        threads = [threading.Thread(target=register, args=(first, 'round_A', a)),
                   threading.Thread(target=register, args=(second, 'round_B', b))]
        mkdir_gate = threading.Barrier(2)
        actual_makedirs = os.makedirs
        def simultaneous_mkdir(path, *args, **kwargs):
            if path == design:
                mkdir_gate.wait(2)
            return actual_makedirs(path, *args, **kwargs)
        with mock.patch('crescendo_engine.server.os.makedirs', side_effect=simultaneous_mkdir):
            for thread in threads:
                thread.start()
            start.set()
            for thread in threads:
                thread.join(3)
                self.assertFalse(thread.is_alive())
        self.assertEqual(outcomes, [None, None])
        recreated = OptimizerService(config)
        self.assertEqual(recreated.run_path('round_A', ''), os.path.join(a, 'round_A'))
        self.assertEqual(recreated.run_path('round_B', ''), os.path.join(b, 'round_B'))
        with self.assertRaises(RuntimeError):
            recreated._register_run_location('round_A', b)

    def test_historical_run_lease_uses_original_tb_scope(self):
        a, b = os.path.join(self.root, 'TB-A'), os.path.join(self.root, 'TB-B')
        config = {'runtime': {'run_root': self.root, 'design_root': a}}
        service = OptimizerService(config)
        service._register_run_location('round_A', self.root)
        config['runtime']['design_root'] = b
        frozen = service._config_for_run('round_A')
        self.assertEqual(frozen['runtime']['design_root'], a)
        self.assertEqual(service._optimized_cleanup_runtime('round_A'), a)

    def test_resume_rejects_unsafe_native_root_but_keeps_mock_portability(self):
        unsafe = os.path.join(self.root, 'simulation with spaces')
        self.service._run_locations['old-run'] = unsafe
        checkpoint = {'spec': {'evaluator': 'real'}, 'manifest': {'origin_run_id': 'old-run'}}
        with self.assertRaisesRegex(ValueError, 'Data Root Override'):
            self.service._freeze_run_config(checkpoint['spec'], {}, checkpoint)
        checkpoint['spec']['evaluator'] = 'mock'
        config = self.service._freeze_run_config(checkpoint['spec'], {}, checkpoint)
        self.assertEqual(config['runtime']['run_root'], unsafe)
        self.assertEqual(self.service.config['runtime']['run_root'], self.root)

    def test_corrupt_optional_index_does_not_block_registration(self):
        design = os.path.join(self.root, 'work', 'TB')
        os.makedirs(design)
        with open(os.path.join(design, 'index.json'), 'w') as stream:
            stream.write('[]')
        service = OptimizerService({'runtime': {'run_root': self.root, 'design_root': design}})
        warning = service._register_run_location('current', self.root)
        self.assertIn('index', warning.lower())
        self.assertEqual(service._runtime_for_run('current'), self.root)


@unittest.skipUnless(os.environ.get('DISPLAY'), 'Real Tk tests require DISPLAY')
class StorageUiNativeTkTests(unittest.TestCase):
    setUp = _optional_fixtures.GuiOptionalSettingsTkTests.setUp
    _close = _optional_fixtures.GuiOptionalSettingsTkTests._close
    _wait = _optional_fixtures.GuiOptionalSettingsTkTests._wait
    _prepared = _optional_fixtures.GuiOptionalSettingsTkTests._prepared
    _edit_draft = _optional_fixtures.GuiOptionalSettingsTkTests._edit_draft
    _save = _optional_fixtures.GuiOptionalSettingsTkTests._save

    def _dataset(self, name='results.crescendo'):
        path = os.path.join(self.directory.name, name)
        spec = {'evaluator': 'real', 'objectives': [{'output_key': 'other::gain', 'direction': 'maximize'},
                                                   {'output_key': 'other::power', 'direction': 'minimize'}], 'constraints': []}
        parameters = [{'parameter_id': 'width', 'name': 'w', 'hierarchy_path': '/Other/M1', 'master_library': 'otherPDK', 'master_cell': 'nmos'}]
        store = RunDataStore(path, metadata={'spec': spec, 'parameters': parameters, 'display_name': 'Other TB', 'run_id': 'foreign-run'})
        try:
            row = StorageUiBackendTests.row(0, 'completed')
            row['metrics'] = {'feasible': True, 'objectives': [{'value': 5., 'score': -5.}, {'value': 2., 'score': 2.}], 'constraints': []}
            row['completion_evidence'] = {'ok': True, 'parameters': {'width': 1}}
            store.commit_candidates([row])
        finally:
            store.close()
        return path

    def _load_data(self, path):
        with mock.patch.object(gui.filedialog, 'askopenfilename', return_value=path):
            self.app.load_results_data()
        self._wait(lambda: str(self.app.load_data_button.cget('state')) == 'normal')

    def test_load_results_during_active_run_does_not_grant_native_authority(self):
        self._prepared()
        self.app.run_ui.started('actual-live-run')
        self.app.stop_button.configure(state='normal')
        before_prepared, before_parameters = self.app.prepared, copy.deepcopy(self.app.parameter_configs)
        self.app.live_candidates = [StorageUiBackendTests.row(1)]
        self._load_data(self._dataset())
        self.assertEqual(self.app.run_ui.run_id, 'actual-live-run')
        self.assertEqual(self.app.run_ui.state, 'running')
        self.assertIs(self.app.prepared, before_prepared)
        self.assertEqual(self.app.parameter_configs, before_parameters)
        self.assertEqual(self.app.live_candidates[0]['candidate_id'], 'g000_c0001')
        self.assertEqual(list(self.app.result_tree.get_children('')), ['g000_c0000'])
        self.assertEqual(len(self.app.pareto_plots), 1)
        self.app.select_candidate('g000_c0000', navigate=False)
        self.assertEqual(str(self.app.optimized_schematic_button.cget('state')), 'disabled')
        self.assertEqual(str(self.app.stop_button.cget('state')), 'normal')
        self.assertIn('otherPDK', self.app.candidate_detail.get('1.0', 'end'))
        self.app.show_current_results()
        self.assertIsNone(self.app.display_dataset)
        self.assertEqual(list(self.app.result_tree.get_children('')), ['g000_c0001'])

    def test_bad_result_load_preserves_current_state_and_can_start(self):
        self._prepared()
        path = os.path.join(self.directory.name, 'broken.crescendo')
        with open(path, 'w') as stream:
            stream.write('{broken\n')
        before = copy.deepcopy(self.app.parameter_configs)
        self._load_data(path)
        self.assertIsNone(self.app.display_dataset)
        self.assertEqual(self.app.parameter_configs, before)
        self.assertFalse(self.app.start_request_pending)
        self.app.parameter_configs['/M1/w']['selected'] = False
        self.app.job_command_var.set('batch-submit -q fixture')
        self.app.execution_selection_panel.set_setup(self.app.prepared['setup'])
        with mock.patch.object(self.app, '_poll_progress'), mock.patch.object(self.app, '_request_run_display_name', return_value='After bad import'):
            self.app.start_run()
            self._wait(lambda: self.service.start.call_count == 1 and not self.app.start_request_pending)
        self.assertEqual(self.app.run_ui.run_id, 'fixture-run')

    def test_hung_result_import_cannot_block_start_stop_or_replace_new_run(self):
        self._prepared()
        self.app.parameter_configs['/M1/w']['selected'] = False
        self.app.job_command_var.set('batch-submit -q fixture')
        self.app.execution_selection_panel.set_setup(self.app.prepared['setup'])
        path = self._dataset('blocked.crescendo')
        entered, release, drained = threading.Event(), threading.Event(), threading.Event()
        self.releases.append(release)
        actual_load = load_dataset
        def blocked_load(value):
            entered.set()
            if not release.wait(10):
                raise RuntimeError('Fixture result import was not released')
            return actual_load(value)
        self.service.stop = mock.Mock(return_value={'stopped': True})
        with mock.patch('crescendo_engine.run_data_store.load_dataset', side_effect=blocked_load), \
                mock.patch.object(gui.filedialog, 'askopenfilename', return_value=path), \
                mock.patch.object(self.app, '_poll_progress'), \
                mock.patch.object(self.app, '_request_run_display_name', return_value='While import blocked'):
            self.app.load_results_data()
            self._wait(entered.is_set)
            self.assertEqual(str(self.app.load_data_button.cget('state')), 'disabled')
            self.app.start_run()
            self._wait(lambda: self.service.start.call_count == 1 and not self.app.start_request_pending)
            self.assertFalse(release.is_set())
            self.assertEqual(self.app.run_ui.run_id, 'fixture-run')
            self.assertEqual(str(self.app.load_data_button.cget('state')), 'normal')
            self.app.stop_run()
            self._wait(lambda: self.service.stop.call_count == 1)
            self.service.stop.assert_called_once_with('fixture-run')
            self.assertEqual(self.app.run_ui.state, 'stopping')
            release.set()
            self.app.tasks.submit_history(lambda: True, lambda value: drained.set())
            self._wait(drained.is_set)
        self.assertIsNone(self.app.display_dataset)
        self.assertEqual(self.app.run_ui.run_id, 'fixture-run')
        self.assertEqual(self.app.run_ui.state, 'stopping')

    def test_start_snapshot_quota_error_is_warning_only(self):
        self._prepared()
        self.app.parameter_configs['/M1/w']['selected'] = False
        self.app.job_command_var.set('batch-submit -q fixture')
        self.app.execution_selection_panel.set_setup(self.app.prepared['setup'])
        with mock.patch.object(gui, 'save_last_configuration', side_effect=OSError(errno.EDQUOT, 'Disk quota exceeded')), \
                mock.patch.object(self.app, '_poll_progress'), mock.patch.object(self.app, '_request_run_display_name', return_value='Quota warning'):
            self.app.start_run()
            self._wait(lambda: self.service.start.call_count == 1 and not self.app.start_request_pending)
        self.assertEqual(self.app.run_ui.run_id, 'fixture-run')
        self.assertIn('Last settings save not confirmed', self.app.settings_status_var.get())

    def test_restore_last_draft_and_unmatched_cell_remain_nonblocking(self):
        self._prepared()
        self._edit_draft()
        self._save()
        self.app.generations_var.set('2')
        self.app.restore_last_ui_state()
        self._wait(lambda: str(self.app.load_state_button.cget('state')) == 'normal')
        self.assertEqual(self.app.generations_var.get(), 'unfinished generations')
        self.assertEqual(self.app.job_max_var.get(), '19')
        self.assertFalse(self.app.start_request_pending)
        self.assertFalse(self.service.select_setup.called)

    def test_loaded_results_can_export_complete_file_without_original_run(self):
        self._load_data(self._dataset())
        target = os.path.join(self.directory.name, 'copied.crescendo')
        with mock.patch.object(gui.filedialog, 'asksaveasfilename', return_value=target):
            self.app.export_results_data()
        self._wait(lambda: os.path.isfile(target))
        document = load_dataset(target)
        self.assertEqual(document['metadata']['display_name'], 'Other TB')
        self.assertEqual(len(document['candidates']), 1)
        self.assertIsNone(self.app.run_ui.run_id)

    def test_closed_launch_telemetry_does_not_recreate_removed_context(self):
        context = os.path.join(self.directory.name, 'launch_removed')
        self.app.launch_context = {'state_path': os.path.join(context, 'state.json'), 'source': {}}
        self.app._write_launch_state('closed', cleanup_confirmed=True)
        self.assertFalse(os.path.exists(context))
        self.assertEqual(self.app.launch_state['state'], 'closed')
