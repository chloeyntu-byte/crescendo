"""Startup contracts use isolated interpreters; no desktop or privileged tools."""
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from crescendo_engine.diagnostic_actions import DiagnosticAction, DiagnosticActions, snapshot

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class StartupContractTests(unittest.TestCase):
    def isolated(self, source, *args):
        environment = dict(os.environ)
        environment.pop('CRESCENDO_ENGINE_RUNTIME', None)
        environment['PYTHONPATH'] = ROOT
        result = subprocess.run([sys.executable, '-B', '-c', source] + list(args),
                                cwd=ROOT, env=environment, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, universal_newlines=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return result.stdout

    def test_provider_log_names_are_observational_and_keep_auxiliary_history(self):
        self.isolated('''
import os
from crescendo_engine.scheduler_providers import TemplateProvider,register_provider
from crescendo_engine.runtime import initialize
class Orbit(TemplateProvider):
    def log_artifacts(self,root):
        return {key:os.path.join(root,'orbit.'+key) for key in ('stdout','stderr')}
register_provider('orbit',Orbit())
initialize(scheduler='orbit')
from crescendo_engine import environment_diagnostics as d
assert d._diagnostic_text_name('orbit.stdout')
assert d._raw_scheduler_text_name('orbit_query_42.stderr')
assert d._raw_scheduler_text_name('scheduler_cancel.stderr')
assert not d._raw_scheduler_text_name('foreign.stdout')
''')

    def test_immutable_once_and_late_initialization_rejected(self):
        self.isolated('''
from crescendo_engine.runtime import *
i = ApplicationIdentity(display_name='Fable', env_prefix='FABLE', env_aliases=('OLD_FABLE',))
try:
    i.schema_aliases['x'] = 'y'
    raise AssertionError('mutable identity')
except TypeError:
    pass
os.environ['OLD_FABLE_EMPTY'] = ''
initialize(i)
assert os.environ['FABLE_EMPTY'] == ''
try:
    initialize(i)
    raise AssertionError('double initialization')
except RuntimeError:
    pass
''')
        self.isolated('''
from crescendo_engine import pdk
from crescendo_engine.runtime import initialize
try:
    initialize()
    raise AssertionError('late initialization')
except RuntimeError:
    pass
''')

    def test_data_only_descriptor_child_parity_and_tampering(self):
        self.isolated('''
import subprocess, sys
from crescendo_engine.runtime import *
initialize(ApplicationIdentity(display_name='Fable', env_prefix='FABLE', schema_namespace='fable'),
           StorageLayout(state_directory='.fable', result_extension='.fable'),
           ProtocolDialect(guardian_token_prefix='fable2-', adapter_entry='cr_fableApply'))
raw = descriptor_json()
assert validate_descriptor(raw) == contract()
child = subprocess.check_output([sys.executable, '-B', '-c',
    'from crescendo_engine.runtime import descriptor_json; print(descriptor_json())'], env=child_environment())
assert child.decode().strip() == raw
bad = json.loads(raw)
bad['runtime']['identity']['env_prefix'] = 'OTHER'
try:
    validate_descriptor(json.dumps(bad))
    raise AssertionError('tampered descriptor')
except ValueError:
    pass
bad = json.loads(raw)
bad['runtime']['module'] = 'untrusted.code'
try:
    validate_descriptor(json.dumps(bad))
    raise AssertionError('module path accepted')
except ValueError:
    pass
''')

    def test_literal_codec_aliases_before_user_interpolation(self):
        self.isolated('''
from crescendo_engine.runtime import *
initialize(ApplicationIdentity(display_name='Fable', env_prefix='FABLE', schema_namespace='fable',
                               schema_aliases={'crescendo.special.v1':'fable.old_special.v2'}),
           StorageLayout(state_directory='.fable', result_extension='.fable'),
           ProtocolDialect(procedure_prefix='fable', guardian_token_prefix='fable2-',
                           callback_aliases={'crescCallback':'cr_fableCallback'}))
assert text('crescendo.special.v1') == 'fable.old_special.v2'
assert text('crescendo.generation_batch.v1') == 'fable.generation_batch.v1'
assert text('optimization.crescendo') == 'optimization.fable'
assert text('crescCallback') == 'cr_fableCallback'
user = 'CRESCENDO_USER_LITERAL'
assert user in (text('crescEmit("%s")') % user)
assert text('^crescendo2-[0-9a-f]{64}$') == '^fable2-[0-9a-f]{64}$'
''')

    def test_public_registry_digest_unchanged(self):
        self.isolated('''
from crescendo_engine.pdk import adapter_registry_identity
assert adapter_registry_identity()['fingerprint'] == 'c00bd4b610960efc525ecc18f921fc91e1415b11c75bceff23582950742cb9ba'
''')

    def test_private_resources_registry_abi_and_entry_are_shared_with_child(self):
        with tempfile.TemporaryDirectory() as directory:
            resource = os.path.join(directory, 'adapter.il')
            with open(resource, 'w') as stream:
                stream.write('procedure(cr_fableApply(inst name value) t)\n')
            self.isolated('''
import subprocess, sys
from crescendo_engine.runtime import *
from crescendo_engine.resources import ResourceResolver
root = sys.argv[1]
group = ResourceResolver.group(root, ['adapter.il'])
manifest = {'schema_version':'crescendo.pdk_adapter_manifest.v1', 'id':'custom.fable',
 'version':'1', 'abi':'fable.adapter.v1', 'label':'Fable', 'priority':0,
 'capabilities':{}, 'matcher':{'rules':[], 'minimum_strong_matches':0},
 'implementation':{'type':'skill', 'resource_group':'pdk_adapters', 'resource_name':'adapter.il',
                   'entry_symbol':'cr_fableApply'}}
initialize(dialect=ProtocolDialect(adapter_abi='fable.adapter.v1'),
           adapter_manifests=[manifest], resource_groups={'pdk_adapters':group})
from crescendo_engine.pdk import adapter_registry, adapter_registry_identity, adapter_entry_symbol
from crescendo_engine.resources import resource_path
registry = adapter_registry()
assert list(registry) == ['custom.fable']
path = resource_path('pdk_adapters', 'adapter.il')
assert adapter_entry_symbol(path) == 'cr_fableApply'
identity = adapter_registry_identity()['fingerprint']
child = subprocess.check_output([sys.executable,'-B','-c',
 'from crescendo_engine.pdk import adapter_registry_identity; print(adapter_registry_identity()["fingerprint"])'],
 env=child_environment())
assert child.decode().strip() == identity
for name in ('../adapter.il', '/adapter.il', 'missing.il'):
    try:
        resource_path('pdk_adapters', name)
        raise AssertionError('unsafe resource')
    except ValueError:
        pass
with open(path, 'a') as stream:
    stream.write('; changed')
try:
    resource_path('pdk_adapters','adapter.il')
    raise AssertionError('changed resource')
except ValueError:
    pass
''', directory)

    def test_provider_contract_default_and_guarded_fake(self):
        self.isolated('''
from crescendo_engine.runtime import initialize
from crescendo_engine.scheduler_providers import *
class Fable(TemplateProvider):
    version = 'fable-1'
register_provider('fable', Fable())
initialize(scheduler='fable')
commands = {'scheduler_query_template':'query --id {job_id}',
            'scheduler_cancel_template':'cancel --id {job_id}'}
assert command('cancel',commands,'42') == ['cancel','--id','42']
assert submission('Submitted job 42\\n','')['job_id'] == '42'
assert observation('JOB_ID=42\\nJOB_STATE=DONE\\n','42')['terminal']
assert not observation('job not found','42')['terminal']
assert not observation('JOB_ID=43\\nJOB_STATE=DONE\\n','42')['terminal']
assert not capability({})['available']
assert resource_request(['submit','-R','cpu=2']) == {'cpu':2}
try:
    register_provider('late', Fable())
    raise AssertionError('late registration')
except RuntimeError:
    pass
''')

    def test_provider_cannot_invent_proof_or_widen_cancel(self):
        self.isolated('''
from crescendo_engine.runtime import initialize
from crescendo_engine.scheduler_providers import *
class Unsafe(TemplateProvider):
    def cancel_command(self,*args):
        return ['cancel','--all']
    def parse_observation(self,text,job_id):
        row = super(Unsafe,self).parse_observation(text,job_id)
        row['terminal'] = True
        return row
register_provider('unsafe', Unsafe())
initialize(scheduler='unsafe')
for work in (lambda: command('cancel', {'scheduler_cancel_template':'cancel {job_id}'}, '42'),
             lambda: observation('job not found','42')):
    try:
        work()
        raise AssertionError('provider escalated authority')
    except ValueError:
        pass
''')

    def test_provider_log_artifacts_bind_the_actual_submission_template(self):
        self.isolated('''
from crescendo_engine.execution_commands import build_submission_command
plan = build_submission_command({'distribution':'command','submit_argv':['submit']}, 'job.sh')
assert plan['argv'] == ['submit','job.sh']
assert plan['bindings']['scheduler_stdout'] == 'scheduler.stdout'
''')
        self.isolated('''
import os
from crescendo_engine.runtime import initialize
from crescendo_engine.scheduler_providers import TemplateProvider, register_provider
class Fable(TemplateProvider):
    def log_artifacts(self, root):
        return {'stdout':os.path.join(root,'fable.out'), 'stderr':os.path.join(root,'fable.err')}
register_provider('fable',Fable())
initialize(scheduler='fable')
from crescendo_engine.execution_commands import normalize_execution_commands, build_submission_command
settings = normalize_execution_commands({}, {'execution_commands':{
 'submission_template':'submit --out {scheduler_stdout} --err {scheduler_stderr} {job_script}'}})
plan = build_submission_command({'distribution':'command'},'/owned/job.sh',settings)
assert plan['argv'] == ['submit','--out','/owned/fable.out','--err','/owned/fable.err','/owned/job.sh']
assert plan['scheduler_io']['stdout'] == '/owned/fable.out'
assert plan['scheduler_io']['stderr'] == '/owned/fable.err'
''')

    def test_provider_adds_owned_arguments_before_engine_freezes_plan(self):
        self.isolated("""
from crescendo_engine.scheduler_providers import TemplateProvider,register_provider
from crescendo_engine.runtime import initialize
class Fable(TemplateProvider):
    def prepare_submission(self,plan):
        if plan['remote']:
            plan['argv'][1:1] = ['-R','cpu=2','--out',plan['bindings']['scheduler_stdout']]
        return plan
register_provider('fable',Fable())
initialize(scheduler='fable')
from crescendo_engine.execution_commands import build_submission_command,_fingerprint
plan=build_submission_command({'distribution':'command','submit_argv':['submit']},'/owned/job.sh')
assert plan['argv'] == ['submit','-R','cpu=2','--out','/owned/scheduler.stdout','/owned/job.sh']
assert plan['resource_cpu'] == 2
assert plan['scheduler_io']['stdout'] == '/owned/scheduler.stdout'
assert plan['fingerprint'] == _fingerprint(plan)
""")

    def test_provider_rejects_submission_plan_resource_and_log_escalation(self):
        self.isolated('''
from crescendo_engine.runtime import initialize
from crescendo_engine.scheduler_providers import *
class Unsafe(TemplateProvider):
    def parse_submission(self, stdout, stderr):
        row = super(Unsafe,self).parse_submission(stdout,stderr)
        row.update(accepted=True,job_id='42',observed_job_ids=['42'])
        return row
    def resource_request(self,argv):
        return {'cpu':True}
    def log_artifacts(self,root):
        return {'stdout':'/outside/stdout','stderr':'/outside/stderr'}
    def prepare_submission(self,plan):
        plan['argv'] = ['other-script']
        return plan
register_provider('unsafe',Unsafe())
initialize(scheduler='unsafe')
for work in (lambda: submission('',''), lambda: resource_request([]),
             lambda: log_artifacts('/owned/task'),
             lambda: prepare_submission({'argv':['owned-script'],'fingerprint':'bad'})):
    try:
        work()
        raise AssertionError('provider escalated authority')
    except ValueError:
        pass
''')

    def test_explicit_native_factory_survives_presentation_import(self):
        self.isolated('''
from crescendo_engine import gui
original = gui.OptimizerGui
from crescendo import app
assert gui.OptimizerGui is original
from crescendo_engine.application import RuntimeServices, launch_application
from unittest import mock
with mock.patch.object(gui,'launch_gui',return_value=17) as launch:
    assert launch_application(RuntimeServices({})) == 17
    assert launch.call_args[1]['gui_factory'] is gui.native_gui_factory
assert gui.NativeOptimizerGui is original
''')


class DiagnosticActionTests(unittest.TestCase):
    def test_snapshot_is_recursive_and_rejects_objects(self):
        value = {'items':[{'name':'before'}]}
        frozen = snapshot(value)
        value['items'][0]['name'] = 'after'
        self.assertEqual(frozen['items'][0]['name'], 'before')
        with self.assertRaises(TypeError):
            frozen['items'][0]['name'] = 'changed'
        with self.assertRaises(ValueError):
            snapshot({'handle':object()})

    def test_state_async_activity_events_and_close_are_main_thread_owned(self):
        app = mock.Mock()
        pending = []
        app.tasks.submit_optional.side_effect = lambda work, done, fail: pending.append((work,done,fail))
        received = []
        action = DiagnosticAction('fable','Fable',lambda payload: received.append(payload),
                                  on_report_ready=mock.Mock(),on_progress=mock.Mock(),on_close=mock.Mock())
        actions = DiagnosticActions(app,[action])
        button = mock.Mock()
        actions.bind(action,button)
        button.configure.assert_called_with(state='disabled')
        actions.progress({'test_id':'a','main_log':'report','report_ready':False})
        actions.invoke(action)
        self.assertEqual(pending, [])
        payload = {'test_id':'a','main_log':'report','report_ready':True,'items':[1]}
        actions.progress(payload)
        actions.progress(payload)
        action.on_report_ready.assert_called_once()
        actions.invoke(action)
        actions.invoke(action)
        self.assertEqual(len(pending),1)
        button.configure.assert_called_with(state='disabled')
        payload['items'].append(2)
        work,done,_fail = pending[0]
        import threading
        errors=[]
        def run():
            work()
            try:
                actions.progress({})
            except RuntimeError as error:
                errors.append(error)
        thread=threading.Thread(target=run)
        thread.start();thread.join()
        self.assertEqual(received[0]['items'],(1,))
        self.assertEqual(len(errors),1)
        done(None)
        button.configure.assert_called_with(state='normal')
        actions.close();actions.close()
        action.on_close.assert_called_once()
        button.configure.assert_called_with(state='disabled')
        app._activity_begin.assert_called_once()
        app._activity_end.assert_called_once()


if __name__ == '__main__':
    unittest.main()
