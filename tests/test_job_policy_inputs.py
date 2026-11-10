import copy, tempfile, unittest
from unittest import mock
from crescendo_engine.job_policy import command_job_policy, job_input_defaults, verify_effective_job_policy
from crescendo_engine.native_baseline import _native_policy_definitions
from crescendo_engine.environment_test import EnvironmentTestManager
from tests.test_gui_environment_test import EnvironmentTestTkTests

class CommandInputsTests(unittest.TestCase):
    def test_missing_custom_definition_uses_requested_fields_and_preserves_source(self):
        source={'available':False,'name_reference_validation':{'ok':False},'global':{'properties':{'name':'adexl-short'}}}
        before=copy.deepcopy(source)
        expected=command_job_policy(source,{'command':'batch-submit -q user','max_jobs':7,'timeouts':{'run_seconds':81}},['test'])
        self.assertEqual(expected['global']['properties']['distributionmethod'],'Command')
        self.assertEqual(expected['global']['properties']['runtimeout'],'81')
        self.assertEqual(source,before)
        with tempfile.TemporaryDirectory() as root:
            lines=_native_policy_definitions({'root':root,'setup':{'job_policy_references':{'global':'adexl-short'}}},expected)
            self.assertIn('"adexl-short"','\n'.join(lines)); self.assertIn('"batch-submit -q user"','\n'.join(lines))
        readback=copy.deepcopy(expected);readback['tests'][0]['properties']['maxjobs']='8'
        self.assertFalse(verify_effective_job_policy(readback,expected)['ok'])
    def test_captured_test_defaults_and_only_missing_command_blocks(self):
        source={'global':{'properties':{'name':'unknown'}},'tests':[{'name':'test','properties':{'jobsubmitcommand':'site_submit','maxjobs':'6','runtimeout':'90'}}]}
        result=command_job_policy(source,tests=['test'])
        self.assertEqual(result['global']['properties']['jobsubmitcommand'],'site_submit')
        self.assertEqual(result['global']['properties']['maxjobs'],'6')
        self.assertEqual(result['global']['properties']['runtimeout'],'90')
        self.assertEqual(command_job_policy({}, {'command':'user_submit'})['global']['properties']['maxjobs'],'1')
        with self.assertRaisesRegex(ValueError,'enter a Command'):command_job_policy({})
        with self.assertRaisesRegex(ValueError,'enter a Command'):command_job_policy(source,{'command':' '},['test'])

class JobInputTkTests(unittest.TestCase):
    setUp=EnvironmentTestTkTests.setUp
    close=EnvironmentTestTkTests.close
    wait=EnvironmentTestTkTests.wait
    def test_prefill_edit_refresh_and_baseline_all_use_visible_values(self):
        app=self.app
        source={'global':{'available':True,'properties':{'name':'adexl-short','jobsubmitcommand':'captured','maxjobs':'4','runtimeout':'80'}}}
        app._render_job_policy(source,reset=True)
        self.assertEqual(app.job_command_var.get(),'captured');self.assertEqual(app.job_max_var.get(),'4')
        app.job_command_var.set('edited');app.job_max_var.set('9');app.job_timeout_vars['run_seconds'].set('')
        app._render_job_policy(source)
        app.execution_backend_var.set('maestro_baseline')
        self.assertEqual(str(app.job_command_entry.cget('state')),'normal')
        self.assertEqual(app._collect_job_settings()['maestro_job_overrides']['command'],'edited')
        self.assertEqual(app._collect_job_settings()['maestro_job_overrides']['max_jobs'],9)
        self.assertEqual(app._collect_job_settings()['maestro_job_overrides']['timeouts']['run_seconds'],-1)
        self.assertFalse(any('Override command' == str(w.cget('text')) for w in app.job_command_entry.master.winfo_children() if 'text' in w.keys()))
        app.job_command_var.set('')
        with self.assertRaisesRegex(ValueError,'enter a Command'):app._collect_job_settings()
    def test_new_defaults_then_saved_settings_are_preserved(self):
        app=self.app;app._render_job_policy({},reset=True)
        self.assertEqual(app.job_max_var.get(),'1')
        app._restore_job_settings({'maestro_job_overrides':{'command':'saved','max_jobs':3,'timeouts':{'run_seconds':67}}})
        app._render_job_policy({'global':{'properties':{'jobsubmitcommand':'new source','maxjobs':'8'}}})
        values=app._collect_job_settings()['maestro_job_overrides']
        self.assertEqual(values['command'],'saved');self.assertEqual(values['max_jobs'],3);self.assertEqual(values['timeouts']['run_seconds'],67)
