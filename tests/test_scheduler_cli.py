"""Configured scheduler argv contracts; no installation-specific inference."""
import copy
import os
import shlex
import tempfile
import unittest

from crescendo_engine.execution_commands import normalize_execution_commands, validate_execution_commands
from crescendo_engine.netlist_execution import execution_policy, NetlistExecutionError
from crescendo_engine.scheduler_cli import scheduler_capability, scheduler_command_argv, resolve_scheduler_binary


class SchedulerTemplateTests(unittest.TestCase):
    def settings(self, **values):
        commands = {'scheduler_query_template': 'batch-query {job_id}',
                    'scheduler_cancel_template': 'batch-cancel {job_id}'}
        commands.update(values)
        return normalize_execution_commands({}, {'execution_commands': commands})

    def test_defaults_have_no_commands_or_site_profile(self):
        self.assertFalse(scheduler_capability(normalize_execution_commands())['available'])
        self.assertIsNone(resolve_scheduler_binary('missing-batch-query', {'PATH': ''}))

    def test_user_templates_authorize_exact_queries_without_help(self):
        capability = scheduler_capability(self.settings())
        self.assertEqual(capability['source'], 'user_templates')
        self.assertTrue(capability['available'])
        self.assertEqual(scheduler_command_argv(capability['query_template'], '10000001'),
                         ['batch-query', '10000001'])

    def test_cancel_requires_its_own_explicit_template(self):
        result = scheduler_capability(self.settings(scheduler_cancel_template=''))
        self.assertFalse(result['available'])
        self.assertIn('scheduler_cancel_template', result['reason'])

    def test_numeric_scope_rejects_bulk_foreign_and_shell_ids(self):
        for identifier in ('all', '-a', '1 2', '1;bad', True, '', '1x'):
            for kind in ('query', 'cancel'):
                with self.subTest(kind=kind, identifier=identifier), self.assertRaises(ValueError):
                    scheduler_command_argv('batch-' + kind + ' {job_id}', identifier)

    def test_argument_order_is_explicit_and_never_inferred(self):
        for template, expected in [('batch-query --detail --id {job_id}', ['--detail', '--id', '21']),
                                   ('batch-query --id {job_id} --detail', ['--id', '21', '--detail']),
                                   ('batch-query --id={job_id}', ['--id=21'])]:
            self.assertEqual(scheduler_command_argv(template, '21')[1:], expected)

    def test_paths_quotes_and_braces_are_not_reexpanded(self):
        path = "/owned dir/{literal}'s"
        argv = scheduler_command_argv('batch-query --cwd {cwd} {job_id}', '21', {'cwd': path})
        self.assertEqual(argv, ['batch-query', '--cwd', path, '21'])

    def test_missing_or_duplicate_id_placeholders_fail_at_freeze(self):
        for template in ('batch-query --all', 'batch-query {job_id} {job_id}', '{job_id} query'):
            with self.assertRaises(ValueError):
                self.settings(scheduler_query_template=template)

    def test_unknown_formatted_and_shell_templates_fail_at_freeze(self):
        for template in ('batch-query {job_id!r}', 'batch-query {job_id.__class__}',
                         'batch-query {unknown} {job_id}', 'batch-query {job_id} | cat',
                         'sh -c "batch-query {job_id}"'):
            with self.assertRaises(ValueError):
                self.settings(scheduler_query_template=template)

    def test_missing_bindings_are_rejected_before_invocation(self):
        with self.assertRaises(ValueError):
            scheduler_command_argv('batch-query {cwd} {job_id}', '21')

    def test_config_inheritance_and_explicit_empty_override(self):
        config = {'runtime': {'execution_commands': self.settings()}}
        inherited = normalize_execution_commands(config)
        self.assertTrue(scheduler_capability(inherited)['available'])
        overridden = normalize_execution_commands(config, {'execution_commands': {'scheduler_query_template': ''}})
        self.assertFalse(scheduler_capability(overridden)['available'])

    def test_frozen_templates_reject_tampering(self):
        commands = self.settings()
        commands['scheduler_cancel_template'] = 'other {job_id}'
        with self.assertRaises(ValueError):
            validate_execution_commands(commands)

    def test_cluster_preparation_requires_both_templates_and_local_does_not(self):
        local = {'job_policy': {'global': {'properties': {'distributionmethod': 'Local'}}}}
        self.assertEqual(execution_policy(local, {}, {})['global']['distribution'], 'local')
        remote = copy.deepcopy(local)
        remote['job_policy']['global']['properties'].update(distributionmethod='Command', jobsubmitcommand='batch-submit')
        for key in ('scheduler_query_template', 'scheduler_cancel_template'):
            with self.assertRaisesRegex(NetlistExecutionError, key):
                execution_policy(remote, {'execution_commands': self.settings(**{key: ''})}, {})
        self.assertEqual(execution_policy(remote, {'execution_commands': self.settings()}, {})['global']['distribution'], 'command')

    def test_resolver_uses_only_configured_executable_and_preserves_environment(self):
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, 'unusual-user-wrapper')
            with open(path, 'w') as stream:
                stream.write('#!/bin/sh\nexit 0\n')
            os.chmod(path, 0o700)
            env = {'PATH': root}
            self.assertEqual(resolve_scheduler_binary('unusual-user-wrapper', env), path)
            self.assertEqual(resolve_scheduler_binary(path, {'PATH': ''}), path)
            self.assertEqual(env, {'PATH': root})

    def test_task_bindings_are_literal_and_cannot_invoke_a_shell(self):
        marker = '$(touch forbidden); `bad`'
        self.assertEqual(scheduler_command_argv('batch-query {test} {job_id}', '21', {'test': marker}),
                         ['batch-query', marker, '21'])

    def test_local_selected_test_ignores_inactive_global_cluster_policy(self):
        prepared = {'setup': {'tests': [{'name': 'local', 'enabled': True}, {'name': 'remote', 'enabled': False}]},
                    'job_policy': {'global': {'properties': {'distributionmethod': 'Command', 'jobsubmitcommand': 'batch-submit'}},
                                   'tests': [{'name': 'local', 'properties': {'distributionmethod': 'Local'}}]}}
        self.assertEqual(execution_policy(prepared, {}, {})['tests']['local']['distribution'], 'local')

    def test_arbitrary_wrapper_names_use_configured_templates(self):
        capability = scheduler_capability(self.settings(scheduler_query_template='my-status --task={job_id}',
                                                        scheduler_cancel_template='my-stop --task={job_id}'))
        self.assertEqual(scheduler_command_argv(capability['query_template'], '10000001'),
                         ['my-status', '--task=10000001'])

    def test_escaped_literal_id_is_not_counted_as_a_binding(self):
        template = 'batch-query --label={{job_id}} {job_id}'
        self.settings(scheduler_query_template=template)
        self.assertEqual(scheduler_command_argv(template, '21'), ['batch-query', '--label={job_id}', '21'])
