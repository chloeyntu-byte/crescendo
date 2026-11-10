from __future__ import print_function

import json
import os
import shutil
import tempfile
import threading
import unittest
from unittest import mock

from crescendo_engine import config, run_log, runtime_layout, utils
from crescendo_engine.completion import CompletionEvidenceError, validate_candidate_completion
from crescendo_engine.same_library_views import register_view, MARKER
import test_completion as completion_fixture


class ProjectStorageTests(unittest.TestCase):
    def test_home_and_xdg_do_not_control_default_run_storage(self):
        with tempfile.TemporaryDirectory(prefix='Crescendo project ') as work:
            environment = {'HOME': '/never-write-home', 'XDG_STATE_HOME': '/never-write-xdg'}
            self.assertEqual(config.default_run_root(environment, work_dir=work), os.path.join(work, '.crescendo', 'runs'))

    def test_named_tb_root_is_stable_and_library_aliases_are_isolated(self):
        with tempfile.TemporaryDirectory() as work:
            source = {'work_dir': work, 'library': 'Design', 'cell': 'Big_TB', 'view': 'maestro',
                      'setup_path': '/lib/Big_TB/maestro/maestro.sdb', 'cds_lib': work+'/cds.lib'}
            first = runtime_layout.design_storage_root(source, {'Design': '/lib'})
            self.assertEqual(first, runtime_layout.design_storage_root(dict(source), {'Design': '/lib'}))
            self.assertTrue(first.startswith(work+'/.crescendo/Big_TB__Design_maestro_'))
            alias = dict(source, library='Alias')
            self.assertNotEqual(first, runtime_layout.design_storage_root(alias, {'Alias': '/lib'}))
            self.assertEqual(runtime_layout.settings_filename(source=source), 'CRESCENDO_Big_TB_Settings.json')

    def test_readable_run_names_preserve_user_name_without_path_escape(self):
        first = runtime_layout.optimization_run_name('FoM 优化 / 1')
        second = runtime_layout.optimization_run_name('FoM 优化 / 1')
        self.assertTrue(first.startswith('FoM_优化_1_'))
        self.assertNotEqual(first, second)
        self.assertEqual(os.path.basename(first), first)
        for value in ('../../escape', '.', '../', '甲'*100):
            component = runtime_layout.path_component(value)
            self.assertNotIn('/', component)
            self.assertNotIn(component, ('.', '..', ''))
            self.assertLessEqual(len(component.encode('utf-8')), 72)


class MainLogTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = self.temp.name

    def read(self):
        with open(os.path.join(self.root, 'crescendo.log'), encoding='utf-8') as stream:
            return [json.loads(line) for line in stream if line.strip()]

    def test_concurrent_candidates_and_primary_secondary_failures_share_one_log(self):
        utils.atomic_write_json(self.root+'/status.json', {'schema_version': 'crescendo.run_status.v1', 'state':'failed',
            'message':'startup_load: Value stack overflow', 'primary_failure': {'message':'Value stack overflow'}})
        threads=[threading.Thread(target=run_log.append_run_event, args=(self.root,'candidate','created'),
                                  kwargs={'details':{'index': i}}) for i in range(40)]
        for thread in threads: thread.start()
        for thread in threads: thread.join()
        utils.atomic_write_json(self.root+'/worker_shutdown.json', {'schema_version':'crescendo.worker_shutdown.v1',
            'workers':[{'worker_index':0,'ok':False,'result':{'termination_proven':True,
                'process_termination_proven':True,'remote_stop_proven':True,'artifact_cleanup_complete':False,
                'cleanup':{'ok':False,'state':'artifact_cleanup_failed','path':self.root+'/control',
                           'error':'Directory not empty'}}}]})
        rows=self.read()
        self.assertEqual(len(rows),42)
        self.assertIn('Value stack overflow',rows[0]['message'])
        self.assertEqual(rows[-1]['details']['workers'][0]['artifact_cleanup_complete'],False)
        self.assertEqual(rows[-1]['details']['workers'][0]['process_termination_proven'],True)
        self.assertEqual([name for name in os.listdir(self.root) if name.endswith('.log')],['crescendo.log'])

    def test_logging_failure_cannot_fail_durable_state_write(self):
        with mock.patch('crescendo_engine.run_log.observe_saved_json',side_effect=OSError(122,'Disk quota exceeded')):
            utils.atomic_write_json(self.root+'/status.json',{'state':'failed','message':'first error'})
        self.assertEqual(utils.load_json(self.root+'/status.json')['message'],'first error')

    def test_large_unicode_events_and_compaction_keep_complete_json_and_initial_error(self):
        with mock.patch.object(run_log,'MAX_LOG_BYTES',128*1024):
            self.assertTrue(run_log.append_run_event(self.root,'first','primary error'))
            for _ in range(16):
                self.assertTrue(run_log.append_run_event(self.root,'progress','甲'*40000,{'text':'乙'*100000}))
            self.assertTrue(run_log.append_run_event(self.root,'cleanup','completed'))
        rows=self.read()
        self.assertEqual(rows[0]['message'],'primary error')
        self.assertEqual(rows[-1]['message'],'completed')
        self.assertTrue(any(row['stage']=='log_compaction' for row in rows))
        self.assertLess(os.path.getsize(self.root+'/crescendo.log'),256*1024)
        self.assertEqual(os.listdir(self.root),['crescendo.log'])

    def test_symlink_or_shared_log_is_not_written(self):
        target=self.root+'/source'
        with open(target,'w') as stream: stream.write('unchanged')
        os.symlink(target,self.root+'/crescendo.log')
        self.assertFalse(run_log.append_run_event(self.root,'stage','event'))
        with open(target) as stream:self.assertEqual(stream.read(),'unchanged')


class SameLibraryCompletionTests(unittest.TestCase):
    def setUp(self):
        fixture=completion_fixture.CompletionTests()
        fixture.setUp()
        self.addCleanup(fixture.tearDown)
        self.fixture=fixture
        workspace=fixture.workspace
        source=os.path.join(fixture.run_root,'source_library')
        os.makedirs(source)
        workspace.update(same_library_objects=True,source_libraries={'D':source},run_display_name='FoM_Search')
        record=register_view(workspace,('D','TB','maestro'),('setup','D','TB','maestro'),role='maestro')
        os.makedirs(record['path'])
        utils.atomic_write_json(os.path.join(record['path'],MARKER),{'token':record['token'],'target':record['target']})
        results=os.path.join(record['path'],'results','maestro')
        os.makedirs(results)
        new_rdb=os.path.join(results,os.path.basename(fixture.rdb_path))
        shutil.copy2(fixture.rdb_path,new_rdb)
        fixture.parsed['path']=new_rdb
        original_history=os.path.join(fixture.simulation_root,'D','TB','maestro','results','maestro',fixture.history)
        new_history=os.path.join(fixture.simulation_root,'D','TB',record['target'][2],'results','maestro',fixture.history)
        shutil.copytree(original_history,new_history)
        workspace.update(setup_dir=record['path'],setup_view=record['target'][2])
        self.record=record

    def validate(self):
        f=self.fixture
        return validate_candidate_completion(f.workspace,f.candidate_root,f.command,f.parsed)

    def test_exact_owned_maestro_rdb_and_actual_view_psf_are_accepted(self):
        proof=self.validate()
        self.assertTrue(proof['ok'])
        self.assertTrue(proof['rdb']['path'].startswith('owned_setup_results/'))
        self.assertEqual(len(proof['spectre_runs']),2)
        self.assertTrue(all(self.record['target'][2] in row['psf'] for row in proof['spectre_runs']))

    def test_foreign_marker_rejects_same_library_history(self):
        utils.atomic_write_json(os.path.join(self.record['path'],MARKER),{'token':'other','target':self.record['target']})
        with self.assertRaisesRegex(CompletionEvidenceError,'ownership'):
            self.validate()

    def test_source_maestro_rdb_cannot_be_used_as_owned_history(self):
        f=self.fixture
        original=os.path.join(f.run_root,'source_library','TB','maestro','results','maestro')
        os.makedirs(original)
        original_rdb=os.path.join(original,f.history+'.rdb')
        shutil.copy2(f.parsed['path'],original_rdb)
        f.parsed['path']=original_rdb
        with self.assertRaisesRegex(CompletionEvidenceError,'escapes'):
            self.validate()

    def test_history_name_and_regular_file_proof_are_not_relaxed(self):
        f=self.fixture
        f.command['history_name']='other_history'
        with self.assertRaisesRegex(CompletionEvidenceError,'history mismatch'):
            self.validate()
        f.command['history_name']=f.history
        os.link(f.parsed['path'],f.parsed['path']+'.alias')
        with self.assertRaisesRegex(CompletionEvidenceError,'regular unique'):
            self.validate()


class SharedPreparationPresentationTests(unittest.TestCase):
    def test_shared_template_failure_is_shown_as_unexecuted_not_spectre_failure(self):
        from crescendo_engine.gui_state import candidate_status, candidate_spectre_state, candidate_failure_stage_label
        row={'status':'infrastructure_failed','execution_backend':'private_netlist',
             'execution_started':False,'shared_preparation_failed':True,'error':'corner mismatch'}
        self.assertEqual(candidate_status(row),'Shared Preparation Failed \u00b7 Not Executed')
        self.assertEqual(candidate_spectre_state(row),'not_started')
        self.assertEqual(candidate_failure_stage_label(row),'Shared template preparation')


class SameLibraryStoragePreflightTests(unittest.TestCase):
    def test_unmodified_large_layout_and_digital_views_are_not_sized_or_copied(self):
        from test_workspace_source_reference import fixture
        from crescendo_engine.storage_preflight import estimate_workspace_copy_bytes
        with tempfile.TemporaryDirectory() as root:
            setup,hierarchy,parameter,configuration,sources,mapping=fixture(root)
            configuration['cadence']['workspace_copy_strategy']='same_library_views'
            with mock.patch('crescendo_engine.storage_preflight.resolve_copy_libraries',return_value=('A',mapping,sources)):
                estimate=estimate_workspace_copy_bytes(setup,hierarchy,configuration,parameters=[parameter])
            expected=os.path.getsize(setup['source']['maestro_sdb'])+3*os.path.getsize(sources['B']+'/shared/schematic/sch.oa')
            self.assertEqual(estimate,expected)
            self.assertLess(estimate,1000)
            self.assertFalse(os.path.exists(configuration['runtime']['run_root']))


class OwnedSetupModelPathTests(unittest.TestCase):
    def test_relocation_updates_only_exact_owned_maestro_view(self):
        from crescendo_engine.model_paths import remap_workspace_setup_paths, ModelPathRemapError
        with tempfile.TemporaryDirectory() as root:
            source=root+'/source';os.makedirs(source+'/TB/maestro')
            model=root+'/pdk/device.scs';os.makedirs(os.path.dirname(model))
            with open(model,'w') as stream:stream.write('model fixture')
            original=source+'/TB/maestro/maestro.sdb'
            text='<Root><Field Name="modelFiles">"/old/device.scs"</Field></Root>'
            with open(original,'w') as stream:stream.write(text)
            workspace={'root':root+'/workspace','source_libraries':{'D':source},'same_library_objects':True,
                       'copied_libraries':{'D':source}}
            os.makedirs(workspace['root'])
            record=register_view(workspace,('D','TB','maestro'),('setup','D','TB','maestro'),role='maestro')
            os.makedirs(record['path']);utils.atomic_write_json(record['path']+'/'+MARKER,{'token':record['token'],'target':record['target']})
            owned_sdb=record['path']+'/maestro.sdb'
            with open(owned_sdb,'w') as stream:stream.write(text)
            workspace.update(setup_dir=record['path'],setup_sdb=owned_sdb)
            result=remap_workspace_setup_paths(workspace,{'cadence':{'pdk_dir':root+'/pdk'}})
            self.assertTrue(any(row.get('changed') for row in result['references']))
            with open(original) as stream:self.assertEqual(stream.read(),text)
            with open(owned_sdb) as stream:self.assertIn(model,stream.read())
            record['token']='forged'
            with self.assertRaises(ModelPathRemapError):
                remap_workspace_setup_paths(workspace,{'cadence':{'pdk_dir':root+'/pdk'}})


class MidRunErrorRetentionTests(unittest.TestCase):
    def test_non_object_log_record_cannot_block_later_progress_compaction(self):
        with tempfile.TemporaryDirectory() as root:
            with open(root+'/crescendo.log','w',encoding='utf-8') as stream:
                stream.write('[]\n'+('x'*70000)+'\n')
            with mock.patch.object(run_log,'MAX_LOG_BYTES',65536):
                self.assertTrue(run_log.append_run_event(root,'progress','continues'))
            with open(root+'/crescendo.log',encoding='utf-8') as stream:
                self.assertEqual(json.loads(stream.readlines()[-1])['message'],'continues')

    def test_compaction_pins_first_error_and_later_primary_cause_from_middle(self):
        with tempfile.TemporaryDirectory() as root, mock.patch.object(run_log,'MAX_LOG_BYTES',128*1024):
            for _ in range(12):run_log.append_run_event(root,'progress','before '*2000)
            run_log.append_run_event(root,'native_error','FIRST NATIVE ERROR',level='ERROR')
            run_log.append_run_event(root,'failed','PRIMARY CAUSE',details={'primary_failure':{'message':'native error'}},level='ERROR')
            for _ in range(160):run_log.append_run_event(root,'progress','after '*2000)
            run_log.append_run_event(root,'cleanup','last event')
            with open(root+'/crescendo.log',encoding='utf-8') as stream:rows=[json.loads(line) for line in stream if line.strip()]
            messages=[row['message'] for row in rows]
            self.assertIn('FIRST NATIVE ERROR',messages)
            self.assertIn('PRIMARY CAUSE',messages)
            self.assertEqual(messages[-1],'last event')
            self.assertEqual(os.listdir(root),['crescendo.log'])


class MaestroSweepCompletionTests(unittest.TestCase):
    def setUp(self):
        self.f=completion_fixture.CompletionTests();self.f.setUp();self.addCleanup(self.f.tearDown)
        f=self.f
        f.setup['corners']=[{'name':'Fhigh','enabled':True,
                             'variables':[{'name':'SW','value':'0 1'}]},
                            {'name':'Flow','enabled':False}]
        for index,row in enumerate(f.parsed['points']):row['corner_name']='Fhigh_'+str(index)
        for index,row in enumerate(f.parsed['output_map']['ac_test::gain']['values']):row['corner']='Fhigh_'+str(index)
        self.identity={'setup_snapshot':{'ok':True,'saved_source':{'corner_read_ok':True},'private_setup':{'corner_read_ok':True}},
            'schedule':{'read_ok':True,'nominal_enabled':False,'nominal_tests':{'ac_test':False},
                        'groups':{'Fhigh':{'enabled':True,'count':2,'disabled_tests':[]},'Flow':{'enabled':False,'count':1,'disabled_tests':[]}}}}
        self.identity['point_parameters_required']=True
        f.command['native_corner_identity']=self.identity
        f.workspace['native_corner_identity']=self.identity
        self.parameter_requests=[]
        for point in (1,2):
            directory=os.path.join(f.simulation_root,'D','TB','maestro','results','maestro',
                                   f.history,str(point),'ac_test','netlist')
            os.makedirs(directory)
            with open(os.path.join(directory,'input.scs'),'w') as stream:
                # Shared native input may contain the final sweep point. Exact
                # point PSF values, rather than this declaration, prove coverage.
                stream.write('parameters SW=1\n')

    def read_point_parameters(self, requests):
        self.assertEqual([row['point_id'] for row in requests],[1,2])
        self.assertEqual([row['test'] for row in requests],['ac_test','ac_test'])
        self.assertEqual([row['names'] for row in requests],[['SW'],['SW']])
        for point, request in enumerate(requests,1):
            expected=os.path.join(self.f.simulation_root,'D','TB','maestro','results','maestro',
                                  self.f.history,str(point),'ac_test','psf')
            self.assertEqual(request['psf_dir'],expected)
        self.parameter_requests.extend(requests)
        return [{'SW':0},{'SW':1}]

    def validate(self):
        return validate_candidate_completion(self.f.workspace,self.f.candidate_root,
            self.f.command,self.f.parsed,native_parameter_reader=self.read_point_parameters)

    def test_actual_point_names_and_source_group_reducers_are_both_preserved(self):
        from crescendo_engine.rdb import aggregate_output
        proof=self.validate()
        self.assertEqual(proof['corners'],['Fhigh_0','Fhigh_1'])
        self.assertEqual(len(self.parameter_requests),2)
        self.assertEqual([row['source_corner'] for row in proof['points']],['Fhigh','Fhigh'])
        self.assertEqual(aggregate_output(self.f.parsed,'ac_test::gain','min',['Fhigh']),8.0)
        self.assertEqual(aggregate_output(self.f.parsed,'ac_test::gain','mean',['Fhigh']),9.0)
        self.assertEqual(aggregate_output(self.f.parsed,'ac_test::gain','max',['Fhigh']),10.0)

    def test_missing_expansion_or_wrong_output_corner_stays_blocked(self):
        self.identity['schedule']['groups']['Fhigh']['count']=3
        with self.assertRaisesRegex(CompletionEvidenceError,'coverage differs'):
            self.validate()
        self.identity['schedule']['groups']['Fhigh']['count']=2
        self.f.parsed['output_map']['ac_test::gain']['values'][0]['corner']='other'
        with self.assertRaisesRegex(CompletionEvidenceError,'output corner'):
            self.validate()

    def test_failed_snapshot_cannot_be_treated_as_nominal(self):
        self.identity['setup_snapshot']['saved_source']['corner_read_ok']=False
        with self.assertRaisesRegex(CompletionEvidenceError,'not proven'):
            self.validate()


class ExplicitPluginStorageTests(unittest.TestCase):
    def test_explicit_base_controls_context_before_work_directory_writes(self):
        from test_maestro_context import MaestroContextTests
        from crescendo_engine.maestro_context import load_launch_context
        fixture=MaestroContextTests();fixture.setUp();self.addCleanup(fixture.doCleanups)
        path=fixture.root+'/native.context'
        with open(path,'w') as stream:stream.write('\n'.join(fixture.rows)+'\n')
        base=fixture.root+'/explicit store'
        with mock.patch.dict(os.environ,{},clear=True):
            first=load_launch_context(path,consume=False,storage_base_root=base)
            second=load_launch_context(path,consume=False,storage_base_root=base)
        self.assertTrue(first['runtime_root'].startswith(base+'/VCO__LogicalLib_maestro_'))
        self.assertEqual(first['runtime_root'],second['runtime_root'])
        self.assertEqual(first['storage_base_override'],base)
        self.assertNotEqual(first['context_path'],second['context_path'])
        self.assertFalse(os.path.exists(fixture.root+'/.crescendo'))
        override=fixture.root+'/env override'
        with mock.patch.dict(os.environ,{'CRESCENDO_RUN_ROOT':override}):
            environment_context=load_launch_context(path,consume=False,storage_base_root=base)
        self.assertTrue(environment_context['runtime_root'].startswith(override+'/'))
