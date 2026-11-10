"""Command binding and real owned-process fixtures; no native EDA claims."""
import copy
import json
import os
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from crescendo_engine import execution_commands as commands
from crescendo_engine import netlist_execution as execution
from crescendo_engine.simulators import execution_contract
from crescendo_engine.utils import atomic_write_text, sha256_file


SPECTRE_TEMPLATE = '{simulator} {netlist} +aps +mt={threads} -format psfxl -raw {psf} +log {log} -ahdllibdir {ahdl}'
ALPS_TEMPLATE = '{simulator} {netlist} -format ps -o {psf} -I {model_dir} -mt {threads}'


class ExecutionCommandTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="crescendo command's spaces ")
        self.root = self.temp.name
        self.models = os.path.join(self.root, "models with {literal} braces")
        os.makedirs(self.models)
        self.exe = os.path.join(self.root, "solver fixture")
        atomic_write_text(self.exe, "#!" + sys.executable + "\nimport sys,os,json\na=sys.argv[1:]\n"
            "p=a[a.index('-raw')+1] if '-raw' in a else a[a.index('-o')+1]\n"
            "os.makedirs(p,exist_ok=True)\nopen(os.path.join(p,'logFile'),'w').write('owned protocol fixture')\n"
            "if '+log' in a: open(a[a.index('+log')+1],'w').write('spectre completes with 0 errors.\\n')\n"
            "open(os.path.join(p,'fixture_argv.json'),'w').write(json.dumps(a))\n")
        os.chmod(self.exe, 0o700)
        self.config = {"cadence": {"simulator": "spectre", "spectre_bin": self.exe,
                                   "alps_bin": self.exe, "alps_model_dir": self.models,
                                   "simulator_threads": 8}, "runtime": {}}
        self.paths = [os.path.join(self.root, name) for name in ("input {literal}.scs", "private psf", "solver.out", "ahdl cache")]
        self.contract = execution_contract(self.config)
        self.settings = commands.normalize_execution_commands({}, {"execution_commands": {"solver_templates": {
            "spectre": SPECTRE_TEMPLATE, "alps": ALPS_TEMPLATE}}})

    def tearDown(self):
        self.temp.cleanup()

    def build(self, settings=None, contract=None):
        return commands.build_solver_command(contract or self.contract, [], *self.paths,
                                             commands=settings or self.settings)

    def test_spaces_quotes_and_braces_are_one_literal_argument(self):
        argv = self.build()
        self.assertEqual(argv[0], self.exe)
        self.assertEqual(argv[1], self.paths[0])
        self.assertEqual(argv[argv.index('-raw') + 1], self.paths[1])
        self.assertIn("{literal}", argv[1])
        self.assertEqual(commands.command_display(argv), " ".join(__import__('shlex').quote(value) for value in argv))

    def test_omitted_inherits_but_explicit_empty_resets_configured_templates(self):
        configured = {"runtime": {"execution_commands": {"solver_templates": {"spectre": SPECTRE_TEMPLATE},
                                                        "submission_template": 'batch-submit -q short {job_script}'}}}
        inherited = commands.normalize_execution_commands(configured, {})
        self.assertEqual(inherited['solver_templates']['spectre'], SPECTRE_TEMPLATE)
        explicit = commands.normalize_execution_commands(configured, {'execution_commands': {
            'solver_templates': {'spectre': ''}, 'submission_template': ''}})
        self.assertEqual(explicit['solver_templates']['spectre'], '')
        self.assertEqual(explicit['submission_template'], '')

    def test_frozen_spec_does_not_inherit_later_runtime_changes(self):
        spec = {'execution_commands': self.settings}
        configured = {'runtime': {'execution_commands': {'submission_template': 'other {job_script}'}}}
        self.assertEqual(commands.normalize_execution_commands(configured, spec), self.settings)

    def test_unknown_format_fields_shell_and_missing_bindings_rejected_at_freeze(self):
        for template in ('{simulator} {netlist} -raw {psf} +log {log} +mt={threads.__class__}',
                         '{simulator} {netlist} -raw {psf} +log {log} +mt={threads!r}',
                         '{simulator} {netlist} -raw {psf} +log {log} | tee output',
                         '{simulator} {netlist} -raw {psf} +mt=8',
                         '{simulator} {netlist} -raw {psf} +log {log} > output'):
            with self.subTest(template=template), self.assertRaises(commands.ExecutionCommandError):
                commands.normalize_execution_commands({}, {'execution_commands': {'solver_templates': {'spectre': template}}})

    def test_output_flags_cannot_escape_owned_results_or_duplicate_bindings(self):
        for template in (SPECTRE_TEMPLATE.replace('-raw {psf}', '-raw /old/history {psf}'),
                         SPECTRE_TEMPLATE + ' -raw {psf}',
                         SPECTRE_TEMPLATE.replace('+log {log}', '+log /old/log {log}'),
                         SPECTRE_TEMPLATE + ' {netlist}',
                         SPECTRE_TEMPLATE.replace('-ahdllibdir {ahdl}', '-ahdllibdir /shared/ahdl')):
            with self.subTest(template=template), self.assertRaises(commands.ExecutionCommandError):
                self.build(commands.normalize_execution_commands({}, {'execution_commands': {'solver_templates': {'spectre': template}}}))

    def test_interactive_native_flags_are_rejected_in_standalone_solver(self):
        for flag in ('-inter', '-plugin native.so', '+mpssession=foreign', '+mpshost=other'):
            with self.subTest(flag=flag), self.assertRaises(commands.ExecutionCommandError):
                self.build(commands.normalize_execution_commands({}, {'execution_commands': {'solver_templates': {'spectre': SPECTRE_TEMPLATE + ' ' + flag}}}))

    def test_actual_solver_threads_cannot_disagree_or_repeat(self):
        for template in (SPECTRE_TEMPLATE.replace('+mt={threads}', '+mt=6'),
                         SPECTRE_TEMPLATE + ' +mt=8', SPECTRE_TEMPLATE.replace('+mt={threads}', '+mt=auto'),
                         SPECTRE_TEMPLATE.replace('+mt={threads}', '')):
            with self.subTest(template=template), self.assertRaises(commands.ExecutionCommandError):
                self.build(commands.normalize_execution_commands({}, {'execution_commands': {'solver_templates': {'spectre': template}}}))

    def test_alps_has_independent_output_models_and_threads(self):
        config = copy.deepcopy(self.config)
        config['cadence']['simulator'] = 'alps'
        contract = execution_contract(config, 'alps')
        argv = self.build(contract=contract)
        self.assertEqual(argv[argv.index('-I') + 1], self.models)
        self.assertEqual(argv[argv.index('-mt') + 1], '8')
        self.assertNotIn('-raw', argv)
        for replacement in ('-format csv', '-I /spectre/models', '-mt 6'):
            original = '-format ps' if replacement.startswith('-format') else ('-I {model_dir}' if replacement.startswith('-I') else '-mt {threads}')
            bad = ALPS_TEMPLATE.replace(original, replacement)
            with self.subTest(template=bad), self.assertRaises(commands.ExecutionCommandError):
                self.build(commands.normalize_execution_commands({}, {'execution_commands': {'solver_templates': {'alps': bad}}}), contract)

    def test_spectre_split_include_template_is_normalized_and_recorded_in_actual_argv(self):
        config = copy.deepcopy(self.config)
        config["cadence"]["spectre_model_dir"] = self.models
        contract = execution_contract(config)
        settings = commands.normalize_execution_commands({}, {"execution_commands": {"solver_templates": {
            "spectre": SPECTRE_TEMPLATE + " -I {model_dir}"}}})
        plan = commands.build_solver_command_plan(contract, [], *self.paths, commands=settings)
        self.assertEqual(plan["template"], SPECTRE_TEMPLATE + " -I {model_dir}")
        self.assertIn("-I" + self.models, plan["argv"])
        self.assertNotIn("-I", plan["argv"])
        self.assertEqual(plan["argument_encoding"]["include"], "-I<directory>")
        subprocess.run(plan["argv"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        with open(os.path.join(self.paths[1], "fixture_argv.json")) as stream:
            actual = json.load(stream)
        self.assertIn("-I" + self.models, actual)
        self.assertNotIn(self.models, actual)

    def test_missing_spectre_include_directory_has_precise_prelaunch_stage(self):
        settings = commands.normalize_execution_commands({}, {"execution_commands": {"solver_templates": {
            "spectre": SPECTRE_TEMPLATE + " -I"}}})
        with self.assertRaisesRegex(commands.ExecutionCommandError, "no model include directory") as caught:
            self.build(settings)
        self.assertEqual(caught.exception.detail["failure_stage"], "solver_command")

    def test_submission_template_keeps_job_script_in_one_argument_and_checks_cpu(self):
        settings = commands.normalize_execution_commands({}, {'execution_commands': {
            'submission_template': 'batch-submit -A account -q short -R "cpu={threads};mem=30000" -EP {cwd} {job_script}'}})
        wrapper = os.path.join(self.root, 'owned job.sh')
        plan = commands.build_submission_command({'distribution': 'local'}, wrapper, settings,
                                                  {'threads': '8', 'cwd': self.root})
        self.assertTrue(plan['remote'])
        self.assertEqual(plan['argv'][-1], wrapper)
        self.assertIn('cpu=8;mem=30000', plan['argv'])
        commands.validate_submission_resources(plan, self.build(), self.contract)
        bad = copy.deepcopy(plan)
        bad['resource_cpu'] = 6
        with self.assertRaises(commands.ExecutionCommandError):
            commands.validate_submission_resources(bad, self.build(), self.contract)

    def test_submission_cannot_drop_or_duplicate_owned_wrapper(self):
        with self.assertRaises(commands.ExecutionCommandError):
            commands.normalize_execution_commands({}, {'execution_commands': {'submission_template': 'batch-submit input.scs'}})
        settings = commands.normalize_execution_commands({}, {'execution_commands': {'submission_template': 'batch-submit {job_script} {job_script}'}})
        with self.assertRaises(commands.ExecutionCommandError):
            commands.build_submission_command({'distribution': 'local'}, '/owned/job.sh', settings)

    def test_automatic_submission_preserves_source_flags_and_local_order(self):
        remote = commands.build_submission_command({'distribution': 'command', 'submit_argv': ['batch-submit', '-x', 'all', '-Kco']}, '/owned/job.sh')
        self.assertEqual(remote['argv'], ['batch-submit', '-x', 'all', '-Kco', '/owned/job.sh'])
        self.assertEqual(commands.build_submission_command({'distribution': 'local'}, '/owned/job.sh')['argv'], ['/bin/sh', '/owned/job.sh'])

    def test_literal_resource_hint_does_not_guess_dynamic_resource(self):
        literal = commands.normalize_execution_commands({}, {'execution_commands': {'submission_template': 'batch-submit -R "cpu=8;mem=2048" {job_script}'}})
        dynamic = commands.normalize_execution_commands({}, {'execution_commands': {'submission_template': 'batch-submit -R "cpu={threads};mem=2048" {job_script}'}})
        self.assertEqual(commands.submission_cpu_hint(literal, {'distribution': 'local'}), 8)
        self.assertIsNone(commands.submission_cpu_hint(dynamic, {'distribution': 'local'}))

    def test_edited_literal_threads_override_capture_auto_without_rewriting_user_command(self):
        config = copy.deepcopy(self.config)
        config['cadence']['simulator_threads'] = None
        settings = commands.normalize_execution_commands({}, {'execution_commands': {
            'solver_templates': {'spectre': SPECTRE_TEMPLATE.replace('+mt={threads}', '+mt=6')}}})
        contract = execution_contract(config, template_argv=[self.exe, 'old.scs', '+mt=64'],
                                      spec={'execution_commands': settings})
        self.assertEqual(contract['threads'], 6)
        self.assertEqual(contract['threads_source'], 'selected_solver_template')
        self.assertIn('+mt=6', self.build(settings, contract))
        with self.assertRaises(ValueError):
            execution_contract(config, resource_cpu=8, spec={'execution_commands': settings})

    def test_plan_is_bound_to_private_input_even_after_rehash(self):
        plan = commands.build_solver_command_plan(self.contract, [], *self.paths, commands=self.settings)
        task = {'netlist_path': self.paths[0], 'psf_dir': self.paths[1], 'spectre_log': self.paths[2], 'ahdl_dir': self.paths[3], 'argv': plan['argv']}
        commands.validate_solver_command_plan(plan, self.contract, task)
        altered = copy.deepcopy(plan)
        altered['argv'][1] = '/old/history/input.scs'
        altered['fingerprint'] = commands._fingerprint(altered)
        with self.assertRaises(commands.ExecutionCommandError):
            commands.validate_solver_command_plan(altered, self.contract, task)

    def test_point_generation_proofs_reference_global_snapshots_without_repeating_them(self):
        generation = {'source_mode': 'maestro_netlist', 'netlist_method': 'capture_wrapper',
                      'source_identity': {'fingerprint': 'original-design', 'files': ['large snapshot'] * 10000},
                      'private_identity': {'setup_sha256': 'private-setup', 'views': [{'target': 'private-schematic'}]},
                      'parameter_application': {'snapshot': ['full callback snapshot'] * 10000},
                      'solver_invocations': 0, 'remote_submissions': 0}
        compact = execution.compact_netlist_generation(generation, {'manifest_sha256': 'producer-proof'})
        self.assertLess(len(json.dumps(compact)), 1024)
        self.assertEqual(compact['mode'], 'maestro')
        self.assertEqual(compact['source_identity_fingerprint'], 'original-design')
        self.assertEqual(compact['producer_manifest_sha256'], 'producer-proof')
        self.assertEqual(compact['solver_invocations'], 0)
        changed = copy.deepcopy(generation)
        changed['private_identity']['views'][0]['target'] = 'different-private-config-binding'
        alternative = execution.compact_netlist_generation(changed, {'manifest_sha256': 'producer-proof'})
        self.assertNotEqual(compact['private_identity_fingerprint'], alternative['private_identity_fingerprint'])
        self.assertNotEqual(compact['generation_fingerprint'], alternative['generation_fingerprint'])

    def _task(self, mode):
        task_root = os.path.join(self.root, mode)
        os.makedirs(task_root)
        netlist = os.path.join(task_root, 'input.scs')
        atomic_write_text(netlist, 'simulator lang=spectre\n')
        return {'task_id': mode, 'test': 'test', 'corner': 'TT', 'point_id': 1,
                'netlist_path': netlist, 'netlist_sha256': sha256_file(netlist), 'cwd': task_root,
                'spectre_log': os.path.join(task_root, 'psf', 'spectre.out'),
                'psf_dir': os.path.join(task_root, 'psf'), 'ahdl_dir': os.path.join(task_root, 'ahdl'),
                'argv': [self.exe, netlist], 'simulator_contract': self.contract,
                'execution_simulator': 'spectre', 'netlist_generation': {'mode': mode if mode in ('maestro', 'template') else 'template'},
                'outputs': [{'key': 'test::gain', 'name': 'gain', 'expression': '1.0'}]}

    def test_maestro_and_template_producers_share_real_owned_solver_and_local_submit(self):
        events = []
        spec = {'execution_commands': self.settings, 'candidate_timeout_seconds': 3}
        prepared = {'job_policy': {'global': {'properties': {'distributionmethod': 'Local', 'maxjobs': 1}}}}
        context = execution.NetlistExecutionContext(self.config, prepared, spec, progress_callback=events.append,
            measurement_evaluator=lambda task, *args, **kwargs: {'outputs': [dict(task['outputs'][0], value=1.0, error=None)]})
        try:
            for mode in ('maestro', 'template'):
                task = self._task(mode)
                manifest = {'root': self.root, 'candidate_id': mode, 'tasks': [task]}
                result = context.evaluate(manifest)
                pipeline = result['completion']['execution_pipeline']
                self.assertEqual(pipeline['netlist_source'], mode)
                point = pipeline['points'][0]
                self.assertTrue(point['terminal_receipt']['cleanup_proven'])
                self.assertEqual(point['solver_command_plan']['argv'][0], self.exe)
                self.assertEqual(point['submission_command_plan']['argv'][0], '/bin/sh')
                with open(os.path.join(task['psf_dir'], 'fixture_argv.json')) as stream:
                    self.assertEqual(json.load(stream), point['solver_command_plan']['argv'][1:])
            self.assertEqual([event['stage'] for event in events].count('submit'), 2)
            self.assertEqual([event['stage'] for event in events].count('command_render'), 2)
        finally:
            self.assertTrue(context.close(cancel=True)['cleanup_proven'])

    def test_cpu_conflict_stops_before_any_submit_or_solver_launch(self):
        # Use a literal batch-submit executable identity so resource parsing is exact.
        batch_submit = os.path.join(self.root, 'batch-submit')
        atomic_write_text(batch_submit, '#!/bin/sh\nexit 0\n')
        os.chmod(batch_submit, 0o700)
        settings = commands.normalize_execution_commands({}, {'execution_commands': {
            'solver_templates': {'spectre': SPECTRE_TEMPLATE},
            'scheduler_query_template': 'batch-query {job_id}',
            'scheduler_cancel_template': 'batch-cancel {job_id}',
            'submission_template': __import__('shlex').quote(batch_submit) + ' -R "cpu=6;mem=2048" {job_script}'}})
        context = execution.NetlistExecutionContext(self.config, {}, {'execution_commands': settings})
        try:
            with mock.patch.object(context, 'command_runner') as runner, self.assertRaises(execution.NetlistExecutionError) as caught:
                context.evaluate({'root': self.root, 'candidate_id': 'conflict', 'tasks': [self._task('conflict')]})
            runner.assert_not_called()
            self.assertEqual(caught.exception.detail['failure_stage'], 'resource_contract')
            self.assertEqual(caught.exception.detail['cpu'], 6)
            self.assertEqual(caught.exception.detail['threads'], 8)
            self.assertIsNotNone(caught.exception.detail['solver_command_plan'])
            self.assertIsNotNone(caught.exception.detail['submission_command_plan'])
        finally:
            context.close(cancel=True)

    def test_missing_submit_binary_reports_both_expanded_layers_without_launch(self):
        settings = commands.normalize_execution_commands({}, {'execution_commands': {
            'solver_templates': {'spectre': SPECTRE_TEMPLATE},
            'scheduler_query_template': 'batch-query {job_id}',
            'scheduler_cancel_template': 'batch-cancel {job_id}',
            'submission_template': '/definitely/missing/batch-submit -q queue {job_script}'}})
        context = execution.NetlistExecutionContext(self.config, {}, {'execution_commands': settings})
        try:
            with mock.patch.object(context, 'command_runner') as runner, self.assertRaises(execution.NetlistExecutionError) as caught:
                context.evaluate({'root': self.root, 'candidate_id': 'missing_submit', 'tasks': [self._task('missing_submit')]})
            runner.assert_not_called()
            detail = caught.exception.detail
            self.assertEqual(detail['failure_stage'], 'submit')
            self.assertFalse(detail['execution_started'])
            self.assertEqual(detail['solver_command_plan']['argv'][0], self.exe)
            self.assertEqual(detail['submission_command_plan']['argv'][0], '/definitely/missing/batch-submit')
            self.assertTrue(detail['submission_command_plan']['argv'][-1].endswith('/execution/execute.sh'))
        finally:
            context.close(cancel=True)

    def test_edited_scheduler_submission_runs_owned_script_then_waits_for_its_terminal_receipt(self):
        batch_submit = os.path.join(self.root, 'batch-submit')
        atomic_write_text(batch_submit, '#!/bin/sh\nexit 0\n')
        os.chmod(batch_submit, 0o700)
        settings = commands.normalize_execution_commands({}, {'execution_commands': {
            'solver_templates': {'spectre': SPECTRE_TEMPLATE},
            'scheduler_query_template': 'batch-query {job_id}',
            'scheduler_cancel_template': 'batch-cancel {job_id}',
            'submission_template': __import__('shlex').quote(batch_submit) + ' -R "cpu={threads};mem=2048" -EP {cwd} {job_script}'}})
        processes, actual_submit = [], []

        def submit(argv, **kwargs):
            # Scheduler fixture performs only async submission. The solver is
            # started by the owned compute script, never separately by Crescendo.
            if 'guardian_launch_lock_fd' not in kwargs:
                return {'returncode': 0, 'stdout_tail': 'JOB_ID 10000005\nJOB_STATE RUNNING\n'}
            os.close(kwargs['guardian_launch_lock_fd'])
            actual_submit.append(argv)
            processes.append(subprocess.Popen(['/bin/sh', argv[-1]], cwd=kwargs['cwd'], env=kwargs['env']))
            return {'returncode': 0, 'stdout_tail': 'JOBID MESSAGE\n10000005 Submit job successfully.\n'}

        context = execution.NetlistExecutionContext(self.config, {}, {'execution_commands': settings}, command_runner=submit,
            measurement_evaluator=lambda task, *args, **kwargs: {'outputs': [dict(task['outputs'][0], value=1.0, error=None)]})
        try:
            task = self._task('async')
            result = context.evaluate({'root': self.root, 'candidate_id': 'async', 'tasks': [task]})
            point = result['execution_pipeline']['points'][0]
            self.assertEqual(actual_submit, [point['submission_command_plan']['argv']])
            self.assertEqual(point['scheduler_job_id'], '10000005')
            self.assertEqual(point['terminal_receipt']['returncode'], 0)
            self.assertTrue(point['terminal_receipt']['cleanup_proven'])
            self.assertTrue(point['submission_command_plan']['remote'])
        finally:
            for process in processes:
                process.wait(timeout=5)
            self.assertTrue(context.close(cancel=True)['cleanup_proven'])
