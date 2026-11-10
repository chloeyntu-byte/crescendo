from __future__ import print_function

import copy
import json
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine.storage_policy import (
    load_native_storage_readback, native_storage_init_lines, native_storage_readback_lines,
    native_workspace_storage, require_data_root, resolve_source_path,
    resolve_storage_policy, save_options_from_records, unavailable_save_options,
    workspace_rdb_path, qualify_native_history_paths, native_history_readback_line,
    validate_native_data_path,
)
from crescendo_engine.cadence import child_environment


def record(key, value, available=True):
    return ('V', [key, True if available else None, value])


class StoragePolicyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = self.temp.name
        self.source = {'library': 'L', 'cell': 'TB', 'view': 'maestro',
                       'work_dir': self.root, 'view_dir': self.root+'/L/TB/maestro',
                       'setup_path': self.root+'/L/TB/maestro/maestro.sdb'}

    def options(self, *extra):
        rows = [record('source_work_dir', self.root), record('source_home', '/original/userhome'),
                record('save_res_dir', self.root+'/waves'), record('save_dir', self.root+'/db'),
                record('project_dir', '/original/userhome/simulation'),
                record('results_location', self.root+'/waves/L/TB/maestro/results/maestro')]
        return save_options_from_records(rows+list(extra), identity=self.source)

    def test_original_distinct_roots_and_actual_public_path_are_preserved(self):
        options = self.options()
        self.assertEqual(options['effective']['simulation_root'], self.root+'/waves')
        self.assertEqual(options['effective']['rdb_root'], self.root+'/db')
        self.assertEqual(options['effective']['simulation_root_evidence'], 'axlGetResultsLocation_exact_lcv')
        policy = resolve_storage_policy({'_selected_setup_source': self.source}, {'save_options': options})
        self.assertEqual(policy['data_root'], self.root+'/waves')
        self.assertTrue(policy['run_root'].startswith(self.root+'/waves/crescendo/TB'))
        self.assertTrue(policy['states_root'].startswith(self.root+'/.crescendo/TB'))
        self.assertEqual(policy['state_dir'], policy['states_root'])

    def test_missing_native_tool_keeps_settings_and_explicit_override_usable(self):
        config = {'_selected_setup_source': self.source, 'runtime': {'data_root_override': 'data_safe'}}
        policy = resolve_storage_policy(config, {'save_options': unavailable_save_options('missing_ocean')})
        self.assertEqual(policy['data_root'], self.root+'/data_safe')
        self.assertEqual(policy['source'], 'manual_override')
        self.assertTrue(require_data_root(policy)['available'])
        config['runtime']['data_root_override'] = None
        policy = resolve_storage_policy(config)
        self.assertTrue(policy['available'])
        self.assertFalse(policy['native_original_available'])
        self.assertEqual(policy['source'],'working_directory_fallback')
        self.assertEqual(policy['data_root'],self.root+'/simulation')
        self.assertIn('/states',policy['states_root'])
        self.assertTrue(require_data_root(policy)['available'])

    def test_legacy_explicit_override_is_not_moved_into_native_save_root(self):
        config = {'_selected_setup_source': self.source, 'runtime': {'run_root_override': self.root+'/legacy'}}
        with mock.patch.dict(os.environ, {}, clear=True):
            policy = resolve_storage_policy(config, {'save_options': self.options()})
        self.assertEqual(policy['run_root'], self.root+'/legacy')
        self.assertTrue(policy['states_root'].startswith(self.root+'/.crescendo'))
        with mock.patch.dict(os.environ, {'CRESCENDO_RUN_ROOT': self.root+'/explicit'}):
            self.assertEqual(resolve_storage_policy(config)['run_root'], self.root+'/explicit')

    def test_saved_reload_with_unconfirmed_root_never_silently_moves_to_working_directory(self):
        config = {'_maestro_launch_context': {'source': self.source,
                    'saved_setup_refresh': {'previous_simulation_root_hint': self.root+'/old-results'}},
                  'runtime': {'run_root': self.root+'/old-results/crescendo/TB', 'run_root_origin': 'derived'}}
        policy = resolve_storage_policy(config, {'save_options': unavailable_save_options('saved_query')})
        self.assertFalse(policy['available'])
        self.assertTrue(policy['saved_reload_root_unconfirmed'])
        self.assertIn('comparison only', policy['warnings'][1])
        with self.assertRaisesRegex(ValueError, 'Saved setup reload.*Data Root Override'):
            require_data_root(policy)
        # The pre-existing fallback remains available outside a changed-source
        # reload; this guard does not expand to other startup workflows.
        config['_maestro_launch_context'].pop('saved_setup_refresh')
        self.assertTrue(require_data_root(resolve_storage_policy(config))['available'])

    def test_saved_reload_accepts_current_root_evidence_and_explicit_user_override(self):
        config = {'_maestro_launch_context': {'source': self.source, 'saved_setup_refresh': {}},
                  'runtime': {'run_root_origin': 'derived'}}
        config['_maestro_launch_context']['saved_setup_refresh'] = {'source': 'explicit_saved_setup_reload'}
        policy = resolve_storage_policy(config, {'save_options': self.options()})
        self.assertEqual(require_data_root(policy)['data_root'], self.root+'/waves')
        for key, value in (('data_root_override', self.root+'/chosen'), ('run_root_override', self.root+'/explicit-run')):
            with self.subTest(key=key):
                config['runtime'] = {key: value, 'run_root_origin': 'derived'}
                policy = resolve_storage_policy(config, {'save_options': unavailable_save_options('saved_query')})
                self.assertTrue(require_data_root(policy)['available'])
                self.assertFalse(policy['saved_reload_root_unconfirmed'])

    def test_source_home_and_relative_paths_never_use_isolated_worker_home(self):
        self.assertEqual(resolve_source_path('~/results',self.root,'/original/userhome'),'/original/userhome/results')
        self.assertEqual(resolve_source_path('${HOME}/r',self.root,'/original/userhome'),'/original/userhome/r')
        self.assertEqual(resolve_source_path('sub/r',self.root,'/original/userhome'),self.root+'/sub/r')
        for value in ('${UNKNOWN}/x', '~someone/x', '\x00bad', '\n/r'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                resolve_source_path(value,self.root,'/original/userhome')
        value = self.root+'/$(keep-literal) `also-literal`'
        self.assertEqual(resolve_source_path(value,self.root,'/original/userhome'),value)

    def test_unknown_and_empty_are_distinct_and_no_history_can_supply_root(self):
        options = save_options_from_records([record('save_res_dir', None, False), record('save_dir',''), record('fallback',None,False)],identity=self.source)
        self.assertFalse(options['field_available']['save_res_dir'])
        self.assertTrue(options['field_available']['save_dir'])
        self.assertNotIn('simulation_root',options['effective'])
        policy = resolve_storage_policy({'_selected_setup_source': self.source, '_maestro_launch_context':{'histories':[{'results_dir':'/old/hist'}]}})
        self.assertFalse(policy['native_original_available'])

    def test_actual_native_lcv_suffix_does_not_guess_parent_or_match_name_only(self):
        options = save_options_from_records([record('results_location', self.root+'/foreign/TB/maestro/results/maestro')],identity=self.source)
        self.assertNotIn('simulation_root',options['effective'])
        self.assertTrue(options['warnings'])
        options = save_options_from_records([record('results_location', self.root+'/L/TB/maestro/results/maestro')],identity=self.source)
        self.assertEqual(options['effective']['simulation_root'],self.root)

    def test_malformed_or_duplicate_native_record_fails_without_forging_default(self):
        for rows in ([record('save_dir','/x'),record('save_dir','/y')], [('V',['bad',True,'x'])],
                     [('V',['save_dir',False,'/x'])],[record('save_dir','bad\npath')]):
            with self.subTest(rows=rows), self.assertRaises(ValueError):
                save_options_from_records(rows,identity=self.source)


class NativeExecutionPathTests(unittest.TestCase):
    setUp = StoragePolicyTests.setUp

    def test_only_native_data_qualification_rejects_special_execution_paths(self):
        for name in ('data with spaces', "data'quote", 'data"quote', 'data $ literal'):
            with self.subTest(name=name):
                config = {'_selected_setup_source': self.source,
                          'runtime': {'data_root_override': os.path.join(self.root, name)}}
                policy = resolve_storage_policy(config)
                self.assertTrue(policy['available'])  # UI/files remain readable.
                with self.assertRaisesRegex(ValueError, 'Cadence native.*Data Root Override'):
                    require_data_root(policy)
        self.assertEqual(validate_native_data_path(self.root+'/plain-root'),self.root+'/plain-root')

    def test_work_state_path_can_have_spaces_when_real_data_root_is_safe(self):
        source = dict(self.source,work_dir=self.root+'/Virtuoso working directory')
        policy = resolve_storage_policy({'_selected_setup_source':source,
                                         'runtime':{'data_root_override':self.root+'/large-safe-data'}})
        self.assertIn('Virtuoso working directory', policy['states_root'])
        self.assertTrue(require_data_root(policy)['available'])

    def test_real_start_qualifies_before_native_preflight_but_mock_remains_usable(self):
        from crescendo_engine.server import OptimizerService
        service = OptimizerService({'_selected_setup_source':self.source,
                                    'runtime':{'run_root':self.root+'/default','run_root_origin':'derived'}})
        spec = {'evaluator':'real','data_root_override':self.root+'/data $ literal'}
        with self.assertRaisesRegex(ValueError,'Cadence native'):
            service._freeze_run_config(spec, {'setup':{}})
        self.assertFalse(os.path.exists(spec['data_root_override']))
        spec['evaluator'] = 'mock'
        frozen = service._freeze_run_config(spec, {'setup':{}})
        self.assertIn('data $ literal',frozen['runtime']['run_root'])


class NativeStorageTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=self.temp.name
        self.workspace={'root':self.root,'primary_library':'L','setup_cell':'TB','setup_view':'maestro',
                        'setup_dir':self.root+'/original/L/TB/maestro', 'native_storage':native_workspace_storage(self.root)}

    def write_readback(self, replacement=None):
        storage=self.workspace['native_storage']
        fields=dict(save_dir=storage['save_dir'], save_res_dir=storage['save_res_dir'], project_dir=storage['project_dir'],
                    results_location=storage['save_res_dir']+'/L/TB/maestro/results/maestro',
                    source_home=self.root+'/home', source_work_dir=self.root)
        fields.update(replacement or {})
        with open(storage['readback_path'],'w') as stream:
            stream.write('CRESCENDO_NATIVE_STORAGE_1\n')
            for key,value in fields.items():stream.write('V\t'+json.dumps(key)+'\tt\t'+('t' if value is True else 'nil' if value is None or value is False else json.dumps(value))+'\n')

    def test_owned_roots_readback_is_qualified_and_original_layout_is_compatible(self):
        self.write_readback();self.assertIsNotNone(load_native_storage_readback(self.workspace,required=True))
        self.assertTrue(self.workspace['native_storage']['verified'])
        self.assertEqual(workspace_rdb_path(self.workspace,'H'), self.root+'/simulation/L/TB/maestro/results/maestro/H.rdb')
        old=dict(self.workspace);old.pop('native_storage')
        self.assertEqual(workspace_rdb_path(old,'H'), self.root+'/original/L/TB/maestro/results/maestro/H.rdb')
        lines='\n'.join(native_storage_init_lines(self.workspace['native_storage']))
        self.assertIn('"saveResDir"',lines);self.assertIn('"saveDir"',lines);self.assertIn('"projectDir"',lines)
        self.assertIn('crescWriteSaveOptionsRecords','\n'.join(native_storage_readback_lines(self.workspace)))

    def test_missing_or_foreign_actual_path_never_qualifies(self):
        self.assertIsNone(load_native_storage_readback(self.workspace))
        with self.assertRaisesRegex(ValueError,'qualification failed'):
            load_native_storage_readback(self.workspace,required=True)
        for values in ({'results_location':'/foreign/L/TB/maestro/results/maestro'}, {'save_dir':'/source'}, {'save_res_dir':'/source'}, {'project_dir':'/source'}):
            self.write_readback(values)
            with self.subTest(values=values), self.assertRaisesRegex(ValueError,'qualification failed'):
                load_native_storage_readback(self.workspace,required=True)
            self.assertFalse(self.workspace['native_storage']['verified'])

    def test_only_native_child_environment_receives_owned_temporary_root(self):
        original=dict(os.environ)
        config={'cadence':{'launch_environment':{}}}
        with mock.patch.dict(os.environ, {'TMPDIR':'/small/local','PYTHONPATH':'/gui_only'},clear=True):
            before=dict(os.environ)
            env=child_environment(config,self.root+'/home')
            self.assertEqual(env['TMPDIR'],self.root+'/_scratch')
            self.assertEqual(dict(os.environ),before)
            self.assertTrue(os.path.isdir(env['TMPDIR']))
        self.assertEqual(dict(os.environ),original)


class NativeLocalStagingTests(unittest.TestCase):
    setUp = NativeStorageTests.setUp
    write_readback = NativeStorageTests.write_readback

    def enable_local_options(self):
        storage = self.workspace['native_storage']
        storage['source_local_options'] = {
            'field_available': {key:True for key in ('use_local_psf_dir','use_local_rdb_dir','local_psf_dir','local_rdb_dir')},
            'raw': {'use_local_psf_dir':True,'use_local_rdb_dir':True,
                    'local_psf_dir':'/original/userhome/.local/large_psf','local_rdb_dir':'/original/userhome/.local/large_rdb'}}
        return storage

    def test_enabled_source_flags_keep_owned_local_directories_and_actual_readback(self):
        storage = self.enable_local_options()
        lines = '\n'.join(native_storage_init_lines(storage))
        self.assertIn("\"useLocalPsfDir\" 'boolean t",lines)
        self.assertIn("\"useLocalRdbDir\" 'boolean t",lines)
        self.assertNotIn('/original/userhome',lines)
        for key in ('local_psf_dir','local_rdb_dir'):
            self.assertTrue(storage[key].startswith(self.root+'/_scratch/'))
            self.assertTrue(os.path.isdir(storage[key]))
        self.write_readback({'use_local_psf_dir':True,'use_local_rdb_dir':True,
                             'local_psf_dir':storage['local_psf_dir'],'local_rdb_dir':storage['local_rdb_dir']})
        self.assertIsNotNone(load_native_storage_readback(self.workspace,required=True))

    def test_known_foreign_local_readback_is_rejected_but_optional_missing_field_is_not(self):
        storage = self.enable_local_options()
        self.write_readback({'use_local_psf_dir':True,'use_local_rdb_dir':True,
                             'local_psf_dir':'/original/userhome/.local/large_psf','local_rdb_dir':storage['local_rdb_dir']})
        with self.assertRaisesRegex(ValueError,'owned local staging'):
            load_native_storage_readback(self.workspace,required=True)
        self.write_readback()  # Optional native API unavailability never forges a success.
        self.assertIsNotNone(load_native_storage_readback(self.workspace,required=True))
        self.assertEqual(len(storage['local_readback_warnings']),4)

    def test_unknown_local_options_add_no_native_setting_or_files(self):
        lines = '\n'.join(native_storage_init_lines(self.workspace['native_storage']))
        self.assertNotIn('LocalPsfDir',lines)
        self.assertNotIn('LocalRdbDir',lines)
        self.assertFalse(os.path.exists(self.root+'/_scratch/local_psf'))


class NativeHistoryStorageTests(NativeStorageTests):
    def write_history(self, history='H', rdb=None, results=None):
        path=self.root+'/native_history_storage.records'
        rdb=rdb if rdb is not None else workspace_rdb_path(self.workspace,history)
        results=results if results is not None else self.root+'/simulation/L/TB/maestro/results/maestro/'+history
        with open(path,'w') as stream:
            stream.write('CRESCENDO_NATIVE_HISTORY_STORAGE_1\nH\t'+json.dumps(history)+'\t'+json.dumps(rdb)+'\t'+json.dumps(results)+'\n')
        return path

    def test_completed_history_public_paths_keep_context(self):
        self.write_history()
        result=qualify_native_history_paths(self.workspace,self.root,'H')
        self.assertTrue(result['available'])
        self.assertEqual(result['rdb_path'],workspace_rdb_path(self.workspace,'H'))
        self.assertIn('crescWriteNativeHistoryPaths',native_history_readback_line(self.workspace,self.root))

    def test_foreign_paths_and_wrong_history_are_rejected(self):
        for kwargs in ({'rdb':'/source/H.rdb'},{'results':'/source/H'},{'history':'Wrong'}):
            self.write_history(**kwargs)
            with self.subTest(kwargs=kwargs),self.assertRaises(ValueError):
                qualify_native_history_paths(self.workspace,self.root,'H')

    def test_missing_optional_public_setup_api_keeps_strict_completed_file_contract(self):
        self.write_readback({'results_location':None})
        result=load_native_storage_readback(self.workspace,required=True)
        self.assertIsNotNone(result)
        self.assertFalse(self.workspace['native_storage']['verified'])
        self.assertTrue(self.workspace['native_storage']['configured_roots_verified'])
        self.assertIn('completion',self.workspace['native_storage']['readback_warning'])
        self.assertIsNone(qualify_native_history_paths(self.workspace,self.root,'H',required=False))
        with self.assertRaises(OSError):
            qualify_native_history_paths(self.workspace,self.root,'H',required=True)

    def test_history_readback_is_unique_regular_and_bounded(self):
        path=self.write_history();os.link(path,path+'.alias')
        with self.assertRaisesRegex(ValueError,'unique regular'):
            qualify_native_history_paths(self.workspace,self.root,'H')
