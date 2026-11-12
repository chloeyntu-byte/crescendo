"""Contextual help is bounded by the engine and cannot run arbitrary argv."""
import copy
import unittest
from unittest import mock
from crescendo_engine import scheduler_providers as providers
from crescendo_engine.execution_commands import normalize_execution_commands, _fingerprint

class Fable(providers.TemplateProvider):
    defer_missing_templates = True
    def probe_context(self, commands, context, help_probe):
        proof = help_probe('/fictional/queue-info', '--help')
        return {'available':proof['returncode']==0, 'query_template':'queue-info {job_id}',
                'cancel_template':'queue-cancel {job_id}', 'help_attempts':[proof]}

class ProviderContextTests(unittest.TestCase):
    def test_missing_fields_defer_but_explicit_empty_fields_remain_empty(self):
        with mock.patch.object(providers,'get_provider',return_value=Fable()):
            old=normalize_execution_commands()
            self.assertNotIn('scheduler_query_template',old)
            context={'_help_runner':mock.Mock(return_value={'returncode':0})}
            self.assertTrue(providers.capability(old,context)['available'])
            self.assertEqual(providers.command('query',old,'42',context=context),['queue-info','42'])
            explicit=normalize_execution_commands({'runtime':{'execution_commands':{
                'scheduler_query_template':'','scheduler_cancel_template':''}}})
            self.assertFalse(providers.capability(explicit,context)['available'])
            self.assertEqual(old['fingerprint'],_fingerprint(old))
    def test_help_only_runner_and_deadline(self):
        import time
        runner=mock.Mock(return_value={'returncode':0})
        with mock.patch.object(providers,'get_provider',return_value=Fable()):
            commands=normalize_execution_commands()
            result=providers.capability(commands,{'_help_runner':runner,'deadline':time.monotonic()+0.25})
            self.assertTrue(result['available'])
            self.assertEqual(runner.call_args[0][0],['/fictional/queue-info','--help'])
            self.assertLessEqual(runner.call_args[1]['timeout_seconds'],0.25)
            runner.reset_mock()
            self.assertFalse(providers.capability(commands,{'_help_runner':runner,'deadline':0})['available'])
            runner.assert_not_called()
    def test_provider_cannot_use_probe_to_query_cancel_or_submit(self):
        provider=Fable()
        with mock.patch.object(providers,'get_provider',return_value=provider):
            commands=normalize_execution_commands()
            for option in ('42','--cancel','--submit'):
                def bad(commands,context,help_probe):return help_probe('/fictional/queue-info',option)
                with mock.patch.object(provider,'probe_context',side_effect=bad), self.assertRaises(ValueError):
                    providers.capability(commands,{})

    def test_receipt_only_tracking_still_requires_exact_submission_identity(self):
        from crescendo_engine.netlist_execution import NetlistExecutionContext
        context=object.__new__(NetlistExecutionContext)
        context.command_scheduler='auto'
        context.commands={}
        with mock.patch.object(context,'_provider_context',return_value=None), mock.patch(
                'crescendo_engine.netlist_execution.scheduler_capability',return_value={
                    'available':False,'receipt_only':True,'submit_identity_required':True}):
            self.assertTrue(context._scheduler_submission(['queue-submit']))
            context.command_scheduler='receipt'
            self.assertFalse(context._scheduler_submission(['queue-submit']))

    def test_query_only_capability_never_authorizes_cancel(self):
        provider=Fable()
        def query_only(commands,context,probe):
            return {'available':False,'query_available':True,'receipt_only':True,
                    'query_template':'/bin/echo {job_id}','cancel_template':''}
        with mock.patch.object(providers,'get_provider',return_value=provider), mock.patch.object(provider,'probe_context',side_effect=query_only):
            commands=normalize_execution_commands()
            capability=providers.capability(commands,{})
            self.assertFalse(capability['available'])
            self.assertTrue(capability['query_available'])
            self.assertEqual(providers.command('query',commands,'42',context={}),['/bin/echo','42'])
            with self.assertRaises(ValueError):
                providers.command('cancel',commands,'42',context={})

    def test_observational_help_cache_is_engine_owned_and_scoped(self):
        provider=Fable()
        provider.probe_context=lambda commands,context,probe: dict(available=False,
            receipt_only=True, query_template='',cancel_template='',proof=probe('/bin/true','--help'))
        cache={}
        with mock.patch.object(providers,'get_provider',return_value=provider), mock.patch(
                'crescendo_engine.environment_probes.bounded_command',return_value={'returncode':0}) as run:
            commands=normalize_execution_commands()
            for unused in range(2):providers.capability(commands,{'_help_cache':cache,'observation_only':True})
            self.assertEqual(run.call_count,1)
            providers.capability(commands,{'_help_cache':{},'observation_only':True})
            self.assertEqual(run.call_count,2)
