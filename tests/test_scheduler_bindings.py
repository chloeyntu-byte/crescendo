"""Argv-declared log bindings share the engine's frozen owned-point contract."""
import copy
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine import scheduler_providers as providers
from crescendo_engine.execution_commands import build_submission_command, normalize_execution_commands, _fingerprint


class Queue(providers.TemplateProvider):
    flags = {'workdir': '--workdir', 'stdout': '--output', 'stderr': '--error'}

    def prepare_submission(self, plan):
        if not plan['remote']:
            return plan
        additions = []
        for kind, option in sorted(self.flags.items()):
            if not any(arg == option or arg.startswith(option + '=') for arg in plan['argv']):
                path = plan['bindings'].get('cwd') if kind == 'workdir' else plan['bindings']['scheduler_' + kind]
                additions.extend([option, path])
        plan['argv'][1:1] = additions
        return plan

    def submission_bindings(self, plan):
        if not plan['remote']:
            return None
        result = {}
        for kind, option in self.flags.items():
            index = next(i for i, arg in enumerate(plan['argv']) if arg == option or arg.startswith(option + '='))
            result[kind] = {'option': option, 'index': index + (plan['argv'][index] == option)}
            result[kind]['index'] = int(result[kind]['index'])
        return result


class BindingTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = self.directory.name
        self.script = os.path.join(self.root, 'execution', 'job.sh')
        self.provider = Queue()
        self.patch = mock.patch.object(providers, 'get_provider', return_value=self.provider)
        self.patch.start()
        self.addCleanup(self.patch.stop)

    def plan(self, args=(), template=False, root=None):
        import shlex
        commands = normalize_execution_commands({}, {'execution_commands': {
            'submission_template': 'queue-submit ' + ' '.join(shlex.quote(x.replace('{', '{{').replace('}', '}}')) for x in args) + ' {job_script}' if template else ''}})
        return build_submission_command({'distribution': 'command', 'submit_argv': ['queue-submit'] + list(args)},
            self.script, commands, {'cwd': self.root, 'owned_task_root': root or self.root})

    def test_defaults_are_injected_once_and_fingerprinted(self):
        plan = self.plan()
        self.assertEqual(plan['scheduler_io'], {'workdir': self.root,
            'stdout': os.path.join(self.root, 'execution', 'scheduler.stdout'),
            'stderr': os.path.join(self.root, 'execution', 'scheduler.stderr')})
        self.assertEqual(plan['fingerprint'], _fingerprint(plan))
        changed = copy.deepcopy(plan)
        changed['scheduler_io']['stdout'] += '.other'
        self.assertNotEqual(plan['fingerprint'], _fingerprint(changed))
        self.assertEqual(plan['argv'].count('--output'), 1)

    def test_explicit_policy_and_template_keep_original_argv_and_literals(self):
        site = os.path.join(self.root, 'site space {literal} $cash')
        args = ['--workdir', site, '--output', site + '/queue.out', '--error', site + '/queue.err']
        for template in (False, True):
            plan = self.plan(args, template)
            self.assertEqual(plan['argv'], ['queue-submit'] + args + [self.script])
            self.assertEqual(plan['scheduler_io'], {'workdir': site, 'stdout': site + '/queue.out', 'stderr': site + '/queue.err'})
            self.assertEqual(plan['bindings']['scheduler_stdout'], site + '/queue.out')

    def test_relative_and_joined_paths_resolve_from_actual_workdir(self):
        plan = self.plan(['--workdir=site/../site', '--output=out file', '--error=err file'])
        self.assertEqual(plan['scheduler_io'], {'workdir': self.root + '/site',
            'stdout': self.root + '/site/out file', 'stderr': self.root + '/site/err file'})

    def test_each_task_has_independent_paths(self):
        first = self.plan()
        self.root = os.path.join(self.root, 'point-two')
        self.script = os.path.join(self.root, 'execution', 'job.sh')
        second = self.plan()
        self.assertNotEqual(first['scheduler_io'], second['scheduler_io'])
        self.assertNotEqual(first['fingerprint'], second['fingerprint'])

    def test_reject_duplicate_missing_option_and_stream_alias(self):
        for args in (['--output', 'one', '--output', 'two'],
                     ['--output=one', '--output=two'], ['--output'],
                     ['--output', 'same', '--error', 'same']):
            with self.subTest(args=args), self.assertRaises(ValueError):
                self.plan(args)

    def test_reject_path_escape_and_symlinks(self):
        os.symlink(self.root, self.root + '/link')
        for args in (['--workdir', '..'], ['--output', '../foreign'],
                     ['--output', '/foreign/log'], ['--workdir', 'link'],
                     ['--error', 'link/log'], ['--output', self.script]):
            with self.subTest(args=args), self.assertRaises(ValueError):
                self.plan(args)

    def test_require_owned_boundary_and_real_argv_position(self):
        for mutation in ('index', 'option', 'extra', 'field'):
            original = self.provider.submission_bindings
            def bad(plan):
                row = original(plan)
                if mutation == 'index':
                    row['stdout']['index'] = 0
                elif mutation == 'option':
                    row['stdout']['option'] = '--foreign'
                elif mutation == 'extra':
                    row['stdout']['path'] = self.root + '/invented'
                else:
                    row['completion'] = True
                return row
            with mock.patch.object(self.provider, 'submission_bindings', side_effect=bad), self.assertRaises(ValueError):
                self.plan()
        with self.assertRaises(ValueError):
            build_submission_command({'distribution': 'command', 'submit_argv': ['queue-submit']},
                self.script, values={'cwd': self.root})

    def test_declaration_cannot_modify_original_plan_or_evidence(self):
        def forged(plan):
            plan['bindings']['job_script'] = '/foreign/script'
            return {'workdir': None, 'stdout': None, 'stderr': None, 'terminal': True}
        with mock.patch.object(self.provider, 'submission_bindings', side_effect=forged), self.assertRaises(ValueError):
            self.plan()
        def changed(plan):
            plan['scheduler_io']['stdout'] = self.root + '/invented'
            return plan
        with mock.patch.object(self.provider, 'prepare_submission', side_effect=changed), self.assertRaises(ValueError):
            self.plan()

    def test_legacy_provider_without_hook_retains_template_behavior(self):
        with mock.patch.object(providers, 'get_provider', return_value=providers.TemplateProvider()):
            plan = self.plan()
        self.assertNotIn('scheduler_binding_declarations', plan)
        self.assertIsNone(plan['scheduler_io']['stdout'])

    def test_actual_fake_cli_logs_are_collected_from_frozen_bindings(self):
        import subprocess
        import sys
        import time
        from crescendo_engine.solver_artifacts import collect_solver_artifacts
        site = self.root + '/site space {literal} $cash'
        os.makedirs(site)
        os.makedirs(os.path.dirname(self.script))
        fake = self.root + '/queue-submit'
        with open(fake, 'w') as stream:
            stream.write('#!' + sys.executable + '\nimport sys\na=sys.argv\n')
            stream.write('open(a[a.index("--output")+1],"w").write(' + repr(
                'TASK_INFO:\n    JOB_ID: 42\n    TASK_NAME: point\n    EXIT_CODE: 2\n    EXIT_MESSAGE: owned failure\nRESOURCE_USAGE_SUMMARY:\n    REQ_CPU: 1\n') + ')\n')
            stream.write('open(a[a.index("--error")+1],"w").write("owned stderr")\n')
        os.chmod(fake, 0o700)
        epoch = time.time() - 1
        for template in (False, True):
            plan = self.plan(['--workdir', site, '--output', site + '/queue.out', '--error', site + '/queue.err'], template)
            plan['argv'][0] = fake
            subprocess.check_call(plan['argv'])
            paths = plan['scheduler_io']
            snapshot = collect_solver_artifacts({'task_id':'main_tt_1','test':'main','corner':'tt',
                'point_id':1,'execution_simulator':'spectre','psf_dir':self.root + '/psf',
                'simulator_log':self.root + '/solver.out'}, self.root, epoch, job_id='42',
                scheduler_stdout=paths['stdout'], scheduler_stderr=paths['stderr'])
            rows = {row['kind']:row for row in snapshot['artifacts']}
            self.assertTrue(rows['scheduler_stdout']['scheduler_task_observation']['valid'])
            self.assertEqual(rows['scheduler_stdout']['scheduler_task_observation']['job_id'], '42')
