"""Fictional legacy contracts cross actual producers and subprocess consumers."""
import os
import subprocess
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

SETUP = '''
import os, sys, json, hashlib, subprocess
from crescendo_engine.runtime import *
root = sys.argv[1]
os.environ['OLD_FABLE_EMPTY'] = ''
initialize(ApplicationIdentity(display_name='Fable', env_prefix='FABLE',
    env_aliases=('OLD_FABLE',), schema_namespace='fable', slug='fable',
    version='9.4.2',
    schema_aliases={'crescendo.generation_batch.v1':'fable.legacy_batch.v1',
                    'crescendo.optimization_record.v1':'fable.legacy_record.v1',
                    'crescendo.native_run_device_parameters.v3':'fable.old_device.v1'}),
    StorageLayout(config_home='fable-optimizer', state_directory='.fable-state',
        result_extension='.fres', log_filename='fable.log', owned_marker='.fable-owned.json',
        view_prefix='fb_s_', temporary_prefix='.fable-create-',
        leaf_aliases={'persistent_worker_batch.json':'legacy_batch.json'}),
    ProtocolDialect(procedure_prefix='fb', symbol_prefix='fb_', error_prefix='FABLE_ERR',
        guardian_token_prefix='fable2-',
        callback_aliases={'crescOwnedPort':'fbLegacyPort'},
        magic_aliases={'CR_BATCH_POINTS_1':'FABLE_LEGACY_POINTS_1',
                       'CRESCENDOFW_':'FABLE_LEGACY_FW_',
                       '_cr_selected_corners':'_fable_selected_corners',
                       'crescendo_engine.persistent_controller':'fable_controller'}))
assert os.environ['FABLE_EMPTY'] == ''
'''


class RuntimePairTests(unittest.TestCase):
    def isolated(self, source, setup=True):
        with tempfile.TemporaryDirectory() as directory:
            env = dict(os.environ, HOME=directory, TMPDIR=directory,
                       XDG_CACHE_HOME=directory, XDG_CONFIG_HOME=directory, PYTHONPATH=ROOT)
            env.pop('CRESCENDO_ENGINE_RUNTIME', None)
            env.pop('CRESCENDO_ENGINE_BOOTSTRAP', None)
            body = '\n'.join(line[8:] if line.startswith('        ') else line for line in source.splitlines())
            code = 'import os, sys, json, subprocess\n' + (SETUP if setup else '') + body
            result = subprocess.run([sys.executable, '-B', '-c', code, directory], cwd=ROOT,
                                    env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    universal_newlines=True, timeout=45)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_storage_annotations_and_controller_argv_share_runtime_identity(self):
        self.isolated('''
        import errno, inspect
        from crescendo_engine.storage_errors import annotate_storage_error, format_task_error
        error=annotate_storage_error(OSError(errno.ENOSPC,'full'),root+'/point.json','flush')
        from crescendo_engine.environment_diagnostics import package_identity
        assert package_identity()['application_version']=='9.4.2'
        assert package_identity()['engine_version']!='9.4.2'
        from crescendo_engine.private_netlist import _contract_text as workspace_text
        assert workspace_text('_cr_selected_corners')=='_fable_selected_corners'
        assert error.fb_storage_path == root+'/point.json'
        assert error.fb_storage_operation == 'flush'
        assert not hasattr(error,'cr_storage_path')
        assert root+'/point.json' in format_task_error(error,'fixture')
        from crescendo_engine import persistent_controller, server
        # Each producer and validator uses the same bound module token.
        for module in (persistent_controller,server):
            source=inspect.getsource(module)
            assert '\"crescendo_engine.persistent_controller\"' not in source.replace('_contract_text(\"crescendo_engine.persistent_controller\")','')
        assert server._contract_text('crescendo_engine.persistent_controller') == 'fable_controller'
        assert persistent_controller._contract_text('crescendo_engine.persistent_controller') == 'fable_controller'
        from crescendo_engine.diagnosis_catalog import format_section, read_section
        context={'case_id':'spectre.maestro_netlist','first_cause':{'failure_stage':'native_capture_skill_error','message':'owned failure'}}
        section=format_section([{'id':'execution.not_reached','check_id':'execution.not_reached','status':'NOT_TESTED','simulator':'spectre','engine':'maestro_netlist'}],context,compact=True)
        path=root+'/report.txt'
        with open(path,'w',encoding='utf-8') as stream:
            stream.write('run_context '+json.dumps({'scope':'spectre.maestro_netlist','context':context})+'\\n'+section)
        assert 'owned failure' in read_section(path,'spectre.maestro_netlist','request')
        ''')

    def test_dataset_batch_layout_and_log_roundtrip_preserve_user_bytes(self):
        self.isolated('''
        from crescendo_engine.config import user_config_path
        from crescendo_engine.runtime_layout import design_storage_root, settings_filename
        from crescendo_engine.run_data_store import RunDataStore, RunDataReader, CORE_NAME
        from crescendo_engine.run_log import append_run_event, LOG_NAME
        from crescendo_engine import generation_batch as batch
        from crescendo_engine.utils import atomic_write_json
        assert user_config_path() == os.path.join(root,'fable-optimizer','config.json')
        source = {'library':'L','cell':'C','view':'maestro','work_dir':root}
        assert os.path.dirname(design_storage_root(source)) == os.path.join(root,'.fable-state')
        assert settings_filename(source=source) == 'FABLE_C_Settings.json'
        assert CORE_NAME == 'optimization.fres' and LOG_NAME == 'fable.log'
        assert text('environment_export_probe.crescendo') == 'environment_export_probe.fres'
        from crescendo_engine.native_environment import native_snapshot_warnings
        assert 'Crescendo' not in native_snapshot_warnings({})[0]
        assert 'Fable' in native_snapshot_warnings({})[0]
        data = os.path.join(root,CORE_NAME)
        user = 'CRESCENDO_USER_LITERAL crescOwnedPort crescendo.user.v1'
        store = RunDataStore(data, {'spec':{},'user':user})
        store.commit_candidates([{'candidate_id':'g000_c0000','generation':0,'index':0,
                                  'status':'completed','values':{'x':1},'user':user}])
        reference = store.reference()
        store.close()
        before = hashlib.sha256(open(data,'rb').read()).hexdigest()
        assert append_run_event(root,'fixture',user)
        plan = batch.build_generation_plan(0,[(0,{'x':1})],[{'parameter_id':'x'}],{})
        assert plan['schema_version'] == 'fable.legacy_batch.v1'
        spec = {'execution_backend':'maestro_netlist'}
        batch.freeze_batching(spec,{'runtime':{'native_candidate_batching':'generation'}})
        assert spec['native_batching_protocol'] == 'fable.maestro_netlist_batch.v1'
        assert batch.generation_batching_enabled(spec)
        path = os.path.join(root,text('persistent_worker_batch.json'))
        atomic_write_json(path,plan)
        refs_path=os.path.join(root,'history_references.json')
        refs=batch.HistoryReferences(refs_path,plan)
        refs.commit('g000_c0000')
        child = r"""
import sys, os, json, hashlib
from crescendo_engine.run_data_store import RunDataReader, CORE_NAME
from crescendo_engine import generation_batch as batch
from crescendo_engine.run_log import LOG_NAME
from crescendo_engine.runtime import descriptor_json
root=sys.argv[1]
path=os.path.join(root,CORE_NAME)
data=RunDataReader(path).read(strict=True)
assert data['candidates'][0]['user'] == 'CRESCENDO_USER_LITERAL crescOwnedPort crescendo.user.v1'
assert data['metadata']['user'] == data['candidates'][0]['user']
assert json.loads(open(os.path.join(root,LOG_NAME)).read())['message'] == data['metadata']['user']
assert json.load(open(os.path.join(root,'legacy_batch.json')))['schema_version'] == batch.SCHEMA
assert json.load(open(os.path.join(root,'history_references.json')))['remaining'] == 0
print(hashlib.sha256(open(path,'rb').read()).hexdigest())
print(descriptor_json())
"""
        output = subprocess.check_output([sys.executable,'-B','-c',child,root], env=child_environment()).decode().splitlines()
        assert output == [before,descriptor_json()]
        records=[json.loads(line) for line in open(data)]
        assert all(r['schema_version'] == 'fable.legacy_record.v1' for r in records)
        ''')

    def test_skill_batch_magic_and_owned_view_producer_consumer_pairs(self):
        self.isolated('''
        from crescendo_engine import generation_batch as batch
        from crescendo_engine import same_library_views as views
        from crescendo_engine.fast_worker import _generated_design_variable
        assert _generated_design_variable({'parameter_id':'x'}).startswith('FABLE_LEGACY_FW_')
        from crescendo_engine.native_qualification import is_native_qualification, require_current_native_qualification
        old={'netlist_generation':{'generation_protocol':'fable.old_device.v1'}}
        assert is_native_qualification(old)
        try:
            require_current_native_qualification(old)
            raise AssertionError('aliased stale qualification accepted')
        except ValueError: pass
        assert batch.SCHEMA == 'fable.legacy_batch.v1'
        script='\\n'.join(batch.native_point_export_lines('/owned/CRESCENDO_USER_LITERAL'))
        assert 'FABLE_LEGACY_POINTS_1' in script and 'fbBatchOut' in script
        assert '/owned/CRESCENDO_USER_LITERAL' in script
        path=os.path.join(root,'points.txt')
        with open(path,'w',encoding='utf-8') as stream:
            stream.write('FABLE_LEGACY_POINTS_1\\nEND\\n')
        assert batch.parse_native_rows(path) == []
        with open(path,'w',encoding='utf-8') as stream: stream.write('CR_BATCH_POINTS_1\\nEND\\n')
        try:
            batch.parse_native_rows(path)
            raise AssertionError('wrong dialect accepted')
        except ValueError: pass
        lib=os.path.join(root,'library');os.makedirs(os.path.join(lib,'C'))
        workspace={'source_libraries':{'L':lib},'same_library_token':'a'*32}
        row=views.register_view(workspace,('L','C','schematic'),('fixture',),'schematic')
        assert row['target'][2].startswith('fb_s_aaaaaaaa_')
        os.mkdir(row['path'])
        payload={'schema_version':views.SCHEMA,'token':row['token'],'target':row['target']}
        with open(os.path.join(row['path'],views.MARKER),'w') as stream: json.dump(payload,stream)
        assert views.owns_view(workspace,row['target'],require_marker=True)
        marker_script='\\n'.join(views.ownership_marker_lines(row))
        assert '.fable-owned.json' in marker_script and 'fbLegacyPort' in marker_script
        assert 'fable.same_library_views.v1' in marker_script
        assert 'crescOwnedPort' not in marker_script
        os.remove(os.path.join(row['path'],views.MARKER))
        assert not views.owns_view(workspace,row['target'],require_marker=True)
        ''')

    def test_guardian_real_child_and_scoped_token_use_same_identity(self):
        self.isolated('''
        from crescendo_engine.legacy_guardian import build_guardian_token_identity, validate_guardian_token_identity
        from crescendo_engine.resources import guardian_script_path
        identity=build_guardian_token_identity('a'*32,'epoch',0)
        assert identity['token'].startswith('fable2-')
        assert validate_guardian_token_identity(identity) == identity
        state=os.path.join(root,'state.json');stdout=os.path.join(root,'child.out')
        stderr=os.path.join(root,'child.err')
        env=child_environment()
        env[text('CRESCENDO_LEGACY_GUARDIAN_TOKEN')]=identity['token']
        child='from crescendo_engine.runtime import descriptor_json; print(descriptor_json())'
        process=subprocess.Popen([sys.executable,'-B',guardian_script_path(),
            '--state-path',state,'--stdout-path',stdout,'--stderr-path',stderr,
            '--token',identity['token'],'--cwd',root,'--expected-parent-pid',str(os.getpid()),
            '--poll-expected-parent','--',sys.executable,'-B','-c',child],
            stdin=subprocess.PIPE,env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        try:
            status=process.wait(timeout=15)
            assert status == 0,(process.stderr.read(),open(stderr).read(),open(state).read())
        finally:
            if process.poll() is None:
                process.terminate();process.wait(timeout=5)
            process.stdin.close();process.stdout.close();process.stderr.close()
        document=json.load(open(state))
        assert document['schema_version'] == 'fable.ocean_worker_guardian.v1',document
        assert document['status'] == 'exited' and document['token'] == identity['token']
        assert open(stdout).read().strip() == descriptor_json()
        ''')

    def test_observer_config_and_events_use_parent_schema(self):
        self.isolated('''
        from crescendo_engine.resources import tool_path
        path=os.path.join(root,'observer.json');log=os.path.join(root,'events.jsonl')
        command=[sys.executable,'-B','-c','print("Submitted job 42")']
        with open(path,'w',encoding='utf-8') as stream:
            json.dump({'schema_version':'fable.maestro_job_observer.v1','argv':command,'command':'fictional-submit','log_path':log},stream)
        result=subprocess.run([sys.executable,'-B',tool_path('maestro_job_observer.py'),'--config',path],
            env=child_environment(),stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=15)
        assert result.returncode == 0,result.stderr
        rows=[json.loads(line) for line in open(log)]
        assert rows and all(r['schema_version']=='fable.maestro_submission_event.v1' for r in rows)
        assert b'Submitted job 42' in result.stdout
        ''')

    def test_shell_source_template_uses_legacy_environment_and_magic(self):
        self.isolated('''
        from crescendo_engine.resources import resource_path
        wrapper=resource_path('tools','submit_spectre_job.sh')
        fake=os.path.join(root,'fake_solver')
        with open(fake,'w') as stream: stream.write('#!/bin/sh\\nprintf "%s" "$HOME"\\n')
        os.chmod(fake,0o700)
        solver_log=os.path.join(root,'solver.log')
        with open(solver_log,'w') as stream: stream.write('CRESCENDO_USER_LITERAL')
        retained=os.path.join(root,'retained');os.mkdir(retained)
        env=child_environment()
        env.update(FABLE_SIMULATION_HOME_SET='x',FABLE_SIMULATION_HOME_VALUE='CRESCENDO_PRIVATE_HOME',
                   FABLE_SIMULATION_TIMEOUT_SECONDS='0',FABLE_SPECTRE_RETAIN_ROOT=retained,
                   FABLE_SPECTRE_TIMEOUT_LOG=os.path.join(root,'timing.log'))
        result=subprocess.run(['/bin/sh',wrapper,fake,'+log',solver_log],env=env,
            stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=10)
        assert result.returncode == 0,result.stderr
        assert result.stdout == b'CRESCENDO_PRIVATE_HOME'
        metadata=[name for name in os.listdir(retained) if name.endswith('.meta')]
        assert len(metadata)==1
        assert open(os.path.join(retained,metadata[0])).read().startswith('schema_version=fable.retained_spectre_log.v1\\n')
        assert open(solver_log).read() == 'CRESCENDO_USER_LITERAL'
        launcher=resource_path('cadence','crescendo_launch')
        assert subprocess.call(['/bin/bash','-n',launcher]) == 0
        assert os.path.dirname(os.path.dirname(os.path.dirname(wrapper))) != os.path.dirname(launcher)
        ''')

    def test_provider_bootstrap_content_is_pinned_and_never_selected_from_descriptor(self):
        self.isolated('''
        import importlib.util
        from crescendo_engine.runtime import *
        root=sys.argv[1]
        startup=os.path.join(root,'startup.py')
        source="from crescendo_engine.scheduler_providers import TemplateProvider,register_provider\\nclass Fable(TemplateProvider):\\n    version='fable-1'\\nregister_provider('fable',Fable())\\n"
        with open(startup,'w') as stream: stream.write(source)
        os.chmod(startup,0o600)
        spec=importlib.util.spec_from_file_location('fable_startup',startup)
        module=importlib.util.module_from_spec(spec);sys.modules[spec.name]=module
        spec.loader.exec_module(module)
        initialize(ApplicationIdentity(env_prefix='FABLE',schema_namespace='fable'),
                   scheduler='fable',worker_bootstrap=startup)
        from crescendo_engine.scheduler_providers import registered_provider_identity
        expected=registered_provider_identity('fable')
        assert contract()['provider_identity']==expected
        child="from crescendo_engine.scheduler_providers import provider_identity; print(provider_identity()['version'])"
        output=subprocess.check_output([sys.executable,'-B','-c',child],env=child_environment())
        assert output.strip()==b'fable-1'
        env=child_environment();env.pop(BOOTSTRAP_ENV)
        result=subprocess.run([sys.executable,'-B','-c',child],env=env,stderr=subprocess.PIPE)
        assert result.returncode != 0 and b'bootstrap' in result.stderr
        with open(startup,'a') as stream:stream.write("\\nraise AssertionError('must not execute changed code')\\n")
        result=subprocess.run([sys.executable,'-B','-c',child],env=child_environment(),stderr=subprocess.PIPE)
        assert result.returncode != 0 and b'content identity changed' in result.stderr
        ''', setup=False)

    def test_mock_run_checkpoint_is_validated_in_child_without_rewriting(self):
        self.isolated('''
        from tests.test_checkpoint_acceptance_matrix import _prepared, _spec
        from crescendo_engine.runner import run_optimization, validate_checkpoint_integrity
        from crescendo_engine.utils import load_json
        prepared=_prepared()
        prepared['source_identity']['schema_version']=text('crescendo.source_design_identity.v1')
        spec=_spec('random');spec['generations']=1;spec['algorithm_parameters']['population']=2
        run_root=os.path.join(root,'run')
        result=run_optimization(spec,{'runtime':{'run_root':root,'persistent_min_free_disk_mib':1}},
                                prepared=prepared,run_root=run_root)
        path=os.path.join(run_root,'checkpoint.json')
        document=load_json(path)
        assert document['schema_version']=='fable.checkpoint.v1'
        validate_checkpoint_integrity(document)
        before=hashlib.sha256(open(path,'rb').read()).hexdigest()
        child="import sys,hashlib,json; from crescendo_engine.runner import validate_checkpoint_integrity; path=sys.argv[1]; validate_checkpoint_integrity(json.load(open(path))); print(hashlib.sha256(open(path,'rb').read()).hexdigest())"
        output=subprocess.check_output([sys.executable,'-B','-c',child,path],env=child_environment()).decode().strip()
        assert output==before
        assert os.path.isfile(os.path.join(run_root,'optimization.fres'))
        assert os.path.isfile(os.path.join(run_root,'fable.log'))
        document['manifest']['algorithm']='changed'
        try:
            validate_checkpoint_integrity(document)
            raise AssertionError('tampered checkpoint accepted')
        except ValueError: pass
        ''')

    def test_injected_adapter_registry_rejects_changed_worker_resource(self):
        self.isolated("""
from crescendo_engine.runtime import *
root=sys.argv[1]
resource=os.path.join(root,'adapter.il')
with open(resource,'w') as stream:stream.write('procedure(cr_fableApply(inst name value) t)\\n')
manifest={'schema_version':'crescendo.pdk_adapter_manifest.v1','id':'custom.fable',
 'version':'1','abi':'crescendo.pdk_adapter.v1','label':'Fable','priority':0,
 'capabilities':{},'matcher':{'rules':[],'minimum_strong_matches':0},
 'implementation':{'type':'skill','skill_path':resource,'entry_symbol':'cr_fableApply'}}
initialize(adapter_manifests=[manifest])
from crescendo_engine.pdk import adapter_registry_identity
assert contract()['registry_sha256'] == adapter_registry_identity()['fingerprint']
child='from crescendo_engine.pdk import adapter_registry; adapter_registry()'
subprocess.check_call([sys.executable,'-B','-c',child],env=child_environment())
with open(resource,'a') as stream:stream.write('; changed')
result=subprocess.run([sys.executable,'-B','-c',child],env=child_environment(),stderr=subprocess.PIPE)
assert result.returncode != 0 and b'registry content identity changed' in result.stderr
""", setup=False)

    def test_late_initialization_and_unsafe_aliases_fail_before_consumers(self):
        self.isolated('''
        from crescendo_engine.runtime import *
        for constructor,kwargs in ((StorageLayout,{'leaf_aliases':{'safe.json':'../unsafe.json'}}),
                (ProtocolDialect,{'callback_aliases':{'crescCall':'bad()'}}),
                (ApplicationIdentity,{'schema_aliases':{'crescendo.test.v1':'bad/value'}})):
            try:
                constructor(**kwargs)
                raise AssertionError('unsafe alias accepted')
            except ValueError: pass
        from crescendo_engine import runtime_layout
        try:
            initialize()
            raise AssertionError('late initialization accepted')
        except RuntimeError: pass
        ''', setup=False)

    def test_default_literals_stay_byte_identical(self):
        # Scan actual wired source constants. Generated payloads/user data are
        # never codec inputs; existing golden protocol tests cover generation.
        import ast
        from crescendo_engine.runtime import text
        count=0
        for name in os.listdir(os.path.join(ROOT,'crescendo_engine')):
            if not name.endswith('.py') or name=='runtime.py':continue
            with open(os.path.join(ROOT,'crescendo_engine',name),encoding='utf-8') as stream:
                tree=ast.parse(stream.read())
            for node in ast.walk(tree):
                if (isinstance(node,ast.Call) and isinstance(node.func,ast.Name) and
                        node.func.id=='_contract_text' and node.args and isinstance(node.args[0],ast.Str)):
                    value=node.args[0].s
                    self.assertEqual(text(value).encode('utf-8'),value.encode('utf-8'),(name,node.lineno))
                    count+=1
        self.assertGreater(count,2600)


if __name__ == '__main__':
    unittest.main()

    def test_registered_application_code_root_is_canonical_and_version_is_data_only(self):
        self.isolated('''
        from crescendo_engine.runtime import ApplicationIdentity,initialize,application_root
        initialize(ApplicationIdentity(version='9.4.2'),resource_groups={
            'application_code':{'root':root,'files':{}}})
        assert application_root('/fallback')==root
        from crescendo_engine.environment_diagnostics import package_identity
        assert package_identity()['application_version']=='9.4.2'
        ''',setup=False)
