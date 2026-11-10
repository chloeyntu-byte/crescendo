"""Verify target tcsh paths through argv, without a shell or cluster job."""
import copy
import os
import tempfile
import unittest

from crescendo_engine.execution_commands import (ExecutionCommandError,
    build_solver_command, build_submission_command, normalize_execution_commands,
    resolve_submission_command)
from crescendo_engine.simulators import execution_contract


class ServerCommandPathTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="crescendo tcsh ' spaces ")
        self.root = self.temp.name
        self.cwd = os.path.join(self.root, "work {literal} $cwd")
        os.makedirs(self.cwd)
        self.script = os.path.join(self.cwd, "execution", "execute.sh")

    def tearDown(self):
        self.temp.cleanup()

    def test_tcsh_cwd_prefix_is_one_literal_task_directory(self):
        for variable in ("$cwd", "${cwd}", "$PWD", "${PWD}"):
            with self.subTest(variable=variable):
                policy = {"distribution": "command", "submit_argv": ["batch-submit",
                    "-A", "current-account", "-q", "short", "-R", "cpu=8;mem=8000",
                    "-x", "all", "-EP", variable, "-o", variable + "/out",
                    "-e", variable + "/err"]}
                plan = build_submission_command(policy, self.script, values={"cwd": self.cwd})
                self.assertEqual(plan["argv"][plan["argv"].index("-EP") + 1], self.cwd)
                self.assertIn(self.cwd + "/out", plan["argv"])
                self.assertEqual(plan["argv"].count(self.script), 1)
                self.assertEqual(plan["resource_cpu"], 8)

    def test_tcsh_cwd_templates_do_not_expand_values_twice(self):
        for variable in ("$cwd", "${cwd}", "$PWD", "${PWD}"):
            with self.subTest(variable=variable):
                commands = normalize_execution_commands({}, {"execution_commands": {
                    "submission_template": 'batch-submit -EP "' + variable + '" -o "' + variable + '/out" {job_script}'}})
                plan = build_submission_command({}, self.script, commands, {"cwd": self.cwd})
                self.assertEqual(plan["scheduler_io"]["workdir"], self.cwd)
                self.assertIn(self.cwd + "/out", plan["argv"])
                self.assertIn("{literal} $cwd", plan["argv"][plan["argv"].index("-o") + 1])

    def test_explicit_scheduler_log_bindings_receive_owned_paths(self):
        settings = normalize_execution_commands({}, {'execution_commands': {
            'submission_template': '/configured/bin/batch-submit --out={scheduler_stdout} --err {scheduler_stderr} {job_script}'}})
        plan = build_submission_command({}, self.script, settings, {'cwd': self.cwd})
        self.assertEqual(plan['scheduler_io'], {'workdir': self.cwd,
            'stdout': os.path.join(os.path.dirname(self.script), 'scheduler.stdout'),
            'stderr': os.path.join(os.path.dirname(self.script), 'scheduler.stderr')})
        self.assertEqual(plan['argv'][-1], self.script)
        self.assertEqual(resolve_submission_command(plan, '/configured/bin/batch-submit')['scheduler_io'], plan['scheduler_io'])

    def test_explicit_paths_and_flags_remain_without_injected_options(self):
        policy = {'distribution': 'command', 'submit_argv': ['batch-submit', '-EP', self.cwd,
            '-o', self.cwd + '/custom.out', '-e', self.cwd + '/custom.err']}
        plan = build_submission_command(policy, self.script, values={'cwd': self.cwd})
        self.assertEqual(plan['argv'], policy['submit_argv'] + [self.script])
        self.assertIsNone(plan['scheduler_io']['stdout'])
        settings = normalize_execution_commands({}, {'execution_commands': {
            'submission_template': 'batch-submit --out {scheduler_stdout} --out {scheduler_stdout} {job_script}'}})
        with self.assertRaises(ExecutionCommandError):
            build_submission_command({}, self.script, settings, {'cwd': self.cwd})

    def test_cwd_variable_without_owned_binding_fails_before_submit(self):
        with self.assertRaises(ExecutionCommandError):
            build_submission_command({"distribution": "command", "submit_argv": ["batch-submit", "-EP", "$cwd"]}, self.script)

    def test_spectre_custom_template_includes_selected_pdk_directory(self):
        exe = os.path.join(self.root, "spectre")
        with open(exe, "w") as stream:
            stream.write("#!/bin/sh\nexit 0\n")
        os.chmod(exe, 0o700)
        models = os.path.join(self.root, "spectre models")
        os.makedirs(models)
        config = {"cadence": {"simulator": "spectre", "spectre_bin": exe,
                  "spectre_model_dir": models, "simulator_threads": 8}}
        contract = execution_contract(config)
        settings = normalize_execution_commands({}, {"execution_commands": {"solver_templates": {
            "spectre": "{simulator} {netlist} +aps +mt={threads} -format psfxl -raw {psf} +log {log}"}}})
        paths = [os.path.join(self.cwd, name) for name in ("test.scs", "psf", "solver.log", "ahdl")]
        argv = build_solver_command(contract, [], *paths, commands=settings)
        self.assertEqual(argv.count("-I" + models), 1)
        self.assertNotIn("-I", argv)
        settings2 = normalize_execution_commands({}, {"execution_commands": {"solver_templates": {
            "spectre": settings["solver_templates"]["spectre"] + " -I{model_dir}"}}})
        argv2 = build_solver_command(contract, [], *paths, commands=settings2)
        self.assertEqual(argv2.count("-I" + models), 1)

    def test_solver_submission_fields_fail_before_native_capture(self):
        for field in ("$cwd", "${PWD}", "{test}", "{corner}", "{point_id}"):
            with self.subTest(field=field), self.assertRaises(ExecutionCommandError) as caught:
                normalize_execution_commands({}, {"execution_commands": {"solver_templates": {
                    "spectre": "{simulator} {netlist} +mt={threads} -raw {psf} +log {log} -I " + field}}})
            self.assertEqual(caught.exception.detail["failure_stage"], "solver_command")
