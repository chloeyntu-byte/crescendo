"""Legacy identities survive validation, planning and isolated restoration."""
import copy
import json
import os
import subprocess
import sys
import tempfile
import unittest

from crescendo_engine import execution_commands as commands

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class LegacyCommandTests(unittest.TestCase):
    def snapshot(self):
        value = commands.normalize_execution_commands({}, {'execution_commands': {
            'submission_template': 'fiction-submit {job_script}'}})
        value.pop('scheduler_query_template')
        value.pop('scheduler_cancel_template')
        value['fingerprint'] = commands._fingerprint(value)
        return value

    def test_four_and_six_key_snapshots_preserve_exact_identity(self):
        for value in (self.snapshot(), commands.normalize_execution_commands()):
            before = copy.deepcopy(value)
            self.assertTrue(commands.validate_execution_commands(value))
            restored = commands.normalize_execution_commands(
                {'runtime': {'execution_commands': {'submission_template': 'other {job_script}'}}},
                {'execution_commands': value})
            self.assertEqual(restored, before)
            self.assertEqual(commands.normalize_execution_commands(
                {'runtime': {'execution_commands': value}}), before)
            self.assertEqual(value, before)
            plan = commands.build_submission_command({'distribution': 'command', 'submit_argv': ['fiction-submit']}, '/owned/job.sh', value)
            self.assertEqual(plan['commands_fingerprint'], before['fingerprint'])

    def test_unknown_partial_corrupt_and_noncanonical_snapshots_rejected(self):
        original = self.snapshot()
        for update, rehash in (({'extra': ''}, True),
                               ({'scheduler_query_template': ''}, True),
                               ({'submission_template': 'changed {job_script}'}, False),
                               ({'submission_template': ' fiction-submit {job_script}'}, True),
                               ({'submission_template': None}, True),
                               ({'fingerprint': '0' * 64}, False)):
            value = dict(original, **update)
            if rehash:
                value['fingerprint'] = commands._fingerprint(value)
            with self.assertRaises(commands.ExecutionCommandError):
                commands.validate_execution_commands(value)
            with self.assertRaises(commands.ExecutionCommandError):
                commands.normalize_execution_commands({'runtime': {'execution_commands': value}})
        value = copy.deepcopy(original)
        value['solver_templates']['spectre'] = 'incomplete'
        value['fingerprint'] = commands._fingerprint(value)
        with self.assertRaises(commands.ExecutionCommandError):
            commands.validate_execution_commands(value)

    def test_legacy_solver_plan_rejects_rehashed_command_drift(self):
        from tests.test_execution_commands import ExecutionCommandTests, SPECTRE_TEMPLATE
        fixture = ExecutionCommandTests()
        fixture.setUp()
        self.addCleanup(fixture.tearDown)
        value = self.snapshot()
        value['solver_templates']['spectre'] = SPECTRE_TEMPLATE
        value['fingerprint'] = commands._fingerprint(value)
        plan = commands.build_solver_command_plan(fixture.contract, [], *fixture.paths, commands=value)
        task = dict(zip(('netlist_path', 'psf_dir', 'spectre_log', 'ahdl_dir'), fixture.paths))
        task.update(argv=plan['argv'], execution_commands=value)
        self.assertTrue(commands.validate_solver_command_plan(plan, fixture.contract, task))
        changed = copy.deepcopy(plan)
        changed['commands']['solver_templates']['spectre'] += ' +diagnose'
        changed['commands']['fingerprint'] = commands._fingerprint(changed['commands'])
        changed['fingerprint'] = commands._fingerprint(changed)
        with self.assertRaises(commands.ExecutionCommandError):
            commands.validate_solver_command_plan(changed, fixture.contract, task)

    def isolated(self, source, directory):
        env = dict(os.environ, PYTHONPATH=ROOT, HOME=directory, TMPDIR=directory)
        env.pop('CRESCENDO_ENGINE_RUNTIME', None)
        env.pop('CRESCENDO_ENGINE_BOOTSTRAP', None)
        result = subprocess.run([sys.executable, '-B', '-c', source, directory],
                                cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                universal_newlines=True, timeout=45)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_provider_supplies_only_absent_legacy_templates_across_processes(self):
        with tempfile.TemporaryDirectory() as directory:
            source = '''
import os, sys, json, subprocess, copy
from crescendo_engine.runtime import initialize, ProtocolDialect, child_environment, scheduler_mode
from crescendo_engine.scheduler_providers import TemplateProvider, register_provider
root = sys.argv[1]
bootstrap = os.path.join(root, 'startup.py')
with open(bootstrap, 'w') as stream:
    stream.write("from crescendo_engine.scheduler_providers import TemplateProvider, register_provider\\n"
        "class Fable(TemplateProvider):\\n"
        "    version = 'fable-1'\\n"
        "    def query_command(self, commands, job_id, values):\\n"
        "        from crescendo_engine.execution_commands import validate_execution_commands\\n"
        "        validate_execution_commands(commands)\\n"
        "        assert len(commands) == 4\\n"
        "        return super(Fable, self).query_command(commands, job_id, values)\\n"
        "    def probe(self, commands):\\n"
        "        if 'scheduler_query_template' not in commands and 'scheduler_cancel_template' not in commands:\\n"
        "            return {'available':True, 'query_template':'fiction-query {job_id}', 'cancel_template':'fiction-cancel {job_id}'}\\n"
        "        return super(Fable, self).probe(commands)\\n"
        "register_provider('fable', Fable())\\n")
os.chmod(bootstrap, 0o600)
import importlib.util
module_spec = importlib.util.spec_from_file_location('fable_startup', bootstrap)
module = importlib.util.module_from_spec(module_spec)
sys.modules[module_spec.name] = module
module_spec.loader.exec_module(module)
initialize(dialect=ProtocolDialect(scheduler_aliases={'fable_queue':'batch'}),
           scheduler='fable', worker_bootstrap=bootstrap)
from crescendo_engine.execution_commands import normalize_execution_commands, _fingerprint
from crescendo_engine.scheduler_providers import capability, command
modern = normalize_execution_commands()
legacy = {key:value for key,value in modern.items() if not key.startswith('scheduler_')}
legacy['fingerprint'] = _fingerprint(legacy)
raw = json.dumps(legacy, sort_keys=True)
assert capability(legacy)['available']
assert command('cancel', legacy, '42') == ['fiction-cancel', '42']
assert json.dumps(legacy, sort_keys=True) == raw
assert not capability(modern)['available']
assert scheduler_mode('fable_queue') == 'batch'
with open(os.path.join(root, 'commands.json'), 'w') as stream: stream.write(raw)
child = """
import os, sys, json
from crescendo_engine.execution_commands import validate_execution_commands, normalize_execution_commands
from crescendo_engine.scheduler_providers import capability, command
from crescendo_engine.runtime import scheduler_mode
value=json.load(open(sys.argv[1]))
assert validate_execution_commands(value)
assert normalize_execution_commands({}, {'execution_commands':value}) == value
assert capability(value)['available']
assert command('query',value,'42') == ['fiction-query','42']
assert scheduler_mode('fable_queue') == 'batch'
print(json.dumps(value,sort_keys=True))
"""
restored = subprocess.check_output([sys.executable,'-B','-c',child,os.path.join(root,'commands.json')],
                                   env=child_environment()).decode().strip()
assert restored == raw
'''
            self.isolated(source, directory)

    def test_legacy_provider_cannot_widen_templates_or_invent_terminal_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            self.isolated('''
import copy
from crescendo_engine.scheduler_providers import TemplateProvider, register_provider, capability, command, observation
from crescendo_engine.runtime import initialize
class Unsafe(TemplateProvider):
    version = 'fable-unsafe'
    def probe(self, commands):
        return {'available':True,'query_template':'query {job_id}', 'cancel_template':'cancel --all'}
register_provider('unsafe',Unsafe())
initialize(scheduler='unsafe')
from crescendo_engine.execution_commands import normalize_execution_commands, _fingerprint
value = normalize_execution_commands()
for key in ('scheduler_query_template','scheduler_cancel_template'): value.pop(key)
value['fingerprint'] = _fingerprint(value)
for work in (lambda:capability(value), lambda:command('cancel',value,'42')):
    try: work()
    except ValueError: pass
    else: raise AssertionError('wide cancellation accepted')
from crescendo_engine.netlist_execution import task_termination_evidence
row={'remote':True,'token':'a'*32,'scheduler_job_id':'42','task':{'task_id':'fixture'}}
assert not task_termination_evidence(row,None)['cleanup_proven']
assert not observation('job not found','42')['terminal']
assert not observation('JOB_ID=43\\nJOB_STATE=DONE\\n','42')['terminal']
''', directory)

    def test_public_defaults_and_alias_validation(self):
        with tempfile.TemporaryDirectory() as directory:
            self.isolated('''
from crescendo_engine.runtime import ProtocolDialect, scheduler_choices, scheduler_mode
assert scheduler_choices() == ('auto','batch','receipt')
for value in ('fable_queue', None, [], 'BATCH'):
    try: scheduler_mode(value)
    except ValueError: pass
    else: raise AssertionError('unsupported public value')
for aliases in ({'auto':'receipt'}, {'fable':'other'}, {'bad space':'batch'}):
    try: ProtocolDialect(scheduler_aliases=aliases)
    except ValueError: pass
    else: raise AssertionError('bad alias')
''', directory)

    def test_private_netlist_entry_and_resume_keep_legacy_command_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            self.isolated('''
import os, sys, copy
from unittest import mock
from crescendo_engine.runtime import initialize, ProtocolDialect
initialize(dialect=ProtocolDialect(scheduler_aliases={'fable_queue':'batch'}))
sys.path.insert(0, os.path.join(os.getcwd(),'tests'))
from crescendo_engine import runner
from crescendo_engine.execution_commands import normalize_execution_commands, _fingerprint
from crescendo_engine.utils import load_json
from tests import test_unified_optimizer as fixtures
from tests.test_stop_resume_shared_cli import stop_second_adapter
from tests.test_runner import _coordinated_runner_resume
fixture = fixtures.UnifiedOptimizerTests()
fixture.setUp()
try:
    commands = normalize_execution_commands()
    for key in ('scheduler_query_template','scheduler_cancel_template'): commands.pop(key)
    commands['fingerprint'] = _fingerprint(commands)
    spec = dict(fixture.spec, command_scheduler='fable_queue', execution_commands=commands)
    root = os.path.join(fixture.root,'stopped')
    with mock.patch.object(runner,'PrivateNetlistOptimizer',stop_second_adapter([])):
        stopped = runner.run_optimization(spec,fixture.config,prepared=fixture.prepared,run_root=root)
    assert stopped['state'] == 'stopped',stopped
    checkpoint = load_json(os.path.join(root,'checkpoint.json'))
    before = copy.deepcopy(checkpoint)
    assert checkpoint['spec']['execution_commands'] == commands
    assert checkpoint['spec']['command_scheduler'] == 'fable_queue'
    instances = []
    with mock.patch.object(runner,'PrivateNetlistOptimizer',fixtures.common_adapter(instances)):
        result = _coordinated_runner_resume(checkpoint['spec'],fixture.config,fixture.prepared,
                                            os.path.join(fixture.root,'resumed'),checkpoint)
    assert result['state'] == 'completed',result
    assert checkpoint == before
    assert len(instances[0].calls) == 7
    restored = load_json(os.path.join(fixture.root,'resumed','checkpoint.json'))
    assert restored['spec']['execution_commands'] == commands
    assert restored['spec']['command_scheduler'] == 'fable_queue'
finally:
    fixture.tearDown()
''', directory)

    def test_alias_checkpoint_settings_policy_continuation_and_runtime_semantics(self):
        with tempfile.TemporaryDirectory() as directory:
            self.isolated('''
import os, sys, copy, json
from crescendo_engine.runtime import *
aliases = {'fable_queue':'batch', 'fable_receipts':'receipt'}
dialect = ProtocolDialect(scheduler_aliases=aliases)
aliases['fable_queue'] = 'receipt'
initialize(dialect=dialect)
assert scheduler_mode('fable_queue') == 'batch'
try: dialect.scheduler_aliases['late'] = 'batch'
except TypeError: pass
else: raise AssertionError('mutable dialect')
sys.path.insert(0, os.path.join(os.getcwd(),'tests'))
from test_resume_preflight_hardening import _mock_config, _mock_prepared, _mock_spec
from crescendo_engine.runner import _run_optimization_with_leases, validate_checkpoint_integrity, validate_checkpoint
from crescendo_engine.execution_commands import normalize_execution_commands, _fingerprint
from crescendo_engine.continuation import checkpoint_continuation_id, continuation_schema_sha256
from crescendo_engine.settings_state import build_settings_state, match_settings_state
from crescendo_engine.netlist_execution import execution_policy, NetlistExecutionContext
root = sys.argv[1]
config = _mock_config(root)
spec = _mock_spec()
spec['command_scheduler'] = 'fable_queue'
value = normalize_execution_commands()
for key in ('scheduler_query_template','scheduler_cancel_template'): value.pop(key)
value['fingerprint'] = _fingerprint(value)
spec['execution_commands'] = value
runroot = os.path.join(root,'run')
prepared = _mock_prepared()
result = _run_optimization_with_leases(spec, config, prepared=prepared, run_root=runroot)
assert result['state'] == 'completed',result
checkpoint = json.load(open(os.path.join(runroot,'checkpoint.json')))
before = copy.deepcopy(checkpoint)
identity = checkpoint_continuation_id(checkpoint)
schema = continuation_schema_sha256(checkpoint['spec'])
assert validate_checkpoint(checkpoint, prepared) == validate_checkpoint_integrity(checkpoint)
assert checkpoint == before
assert checkpoint['spec']['execution_commands'] == value
assert checkpoint_continuation_id(checkpoint) == identity
assert continuation_schema_sha256(checkpoint['spec']) == schema
policy = execution_policy({}, {'command_scheduler':'fable_queue'}, {})
assert policy['command_scheduler'] == 'fable_queue'
context = object.__new__(NetlistExecutionContext)
context.command_scheduler = 'fable_receipts'
context.commands = normalize_execution_commands({}, {'execution_commands': {
 'scheduler_query_template':'query {job_id}', 'scheduler_cancel_template':'cancel {job_id}'}})
assert not context._scheduler_submission(['submit'])
context.command_scheduler = 'fable_queue'
assert context._scheduler_submission(['submit'])
settings = build_settings_state({'optimization':{'command_scheduler':'fable_queue'}})
restored = match_settings_state(settings,{})
assert restored['apply']['optimization']['command_scheduler'] == 'fable_queue'
''', directory)


if __name__ == '__main__':
    unittest.main()
