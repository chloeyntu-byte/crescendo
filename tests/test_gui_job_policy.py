import copy
import gc
import os
import unittest
from unittest import mock

from crescendo_engine import gui
from crescendo_engine.job_policy import policy_from_records


class Value(object):
    def __init__(self, value): self.value = value
    def get(self): return self.value
    def set(self, value): self.value = value


class GuiJobPolicyTests(unittest.TestCase):
    def app(self):
        app = gui.OptimizerGui.__new__(gui.OptimizerGui)
        app.job_command_override_var = Value(False)
        app.job_max_override_var = Value(False)
        app.job_command_var = Value('batch-submit -R "cpu=6;mem=30000"')
        app.job_max_var = Value("64")
        app.maestro_candidate_mode_var = Value("auto")
        return app

    def test_original_values_are_run_values(self):
        self.assertEqual(self.app()._collect_job_settings(), {"maestro_job_overrides": {"command": self.app().job_command_var.get(), "max_jobs": 64}, "maestro_candidate_mode": "auto"})

    def test_legacy_flags_do_not_disable_visible_fields(self):
        app = self.app()
        app.job_max_override_var.set(True)
        self.assertEqual(app._collect_job_settings()["maestro_job_overrides"], {"command": app.job_command_var.get(), "max_jobs": 64})
        app.job_command_override_var.set(True)
        self.assertEqual(app._collect_job_settings()["maestro_job_overrides"]["command"], 'batch-submit -R "cpu=6;mem=30000"')

    def test_small_and_large_job_limits_are_independent_of_algorithm(self):
        app = self.app()
        app.job_max_override_var.set(True)
        for value in ("1", "2", "256"):
            app.job_max_var.set(value)
            self.assertEqual(app._collect_job_settings()["maestro_job_overrides"]["max_jobs"], int(value))
        for value in ("", "0", "-1", "2.0", "nan"):
            app.job_max_var.set(value)
            with self.assertRaises(ValueError): app._collect_job_settings()

    def test_saved_settings_restore_both_explicit_fields_and_batch_choice(self):
        app = self.app()
        run = {"maestro_job_overrides": {"command": "batch-submit -q other", "max_jobs": 2}, "maestro_candidate_mode": "batch"}
        app._restore_job_settings(run)
        self.assertEqual(app._collect_job_settings(), run)
        app._restore_job_settings({})
        self.assertEqual(app._collect_job_settings(), {"maestro_job_overrides": {"command": "batch-submit -q other", "max_jobs": 2}, "maestro_candidate_mode": "auto"})

    def test_run_settings_include_submission_overrides_and_explicit_mode(self):
        app = self.app()
        app.generations_var, app.timeout_var = Value("20"), Value("600")
        app._collect_algorithm_settings = lambda: ("nsga2", {"population": 8})
        app.job_max_override_var.set(True)
        app.maestro_candidate_mode_var.set("batch")
        settings = app._build_run_settings()
        self.assertEqual(settings["algorithm_parameters"]["population"], 8)
        self.assertEqual(settings["maestro_job_overrides"], {"command": app.job_command_var.get(), "max_jobs": 64})
        self.assertEqual(settings["maestro_candidate_mode"], "batch")

    def test_checkpoint_to_ui_retains_submission_fields(self):
        app = self.app()
        document = app._checkpoint_design_config({"spec": {"maestro_job_overrides": {"max_jobs": 3}, "maestro_candidate_mode": "batch"}})
        run = document["optimization"]["run"]
        self.assertEqual(run["maestro_job_overrides"], {"max_jobs": 3})
        self.assertEqual(run["maestro_candidate_mode"], "batch")
    def command_app(self):
        app = self.app()
        app.service = mock.Mock(config={'runtime': {'execution_commands': {
            'solver_templates': {'spectre': 'old solver config'}, 'submission_template': 'old submit config'}}})
        app.solver_command_vars = {'spectre': Value(''), 'alps': Value('')}
        app.submission_command_var = Value('')
        app.execution_backend_var = Value('auto')
        app.generations_var, app.timeout_var = Value('20'), Value('600')
        app._collect_algorithm_settings = lambda: ('nsga2', {'population': 8})
        return app

    def test_empty_ui_commands_explicitly_clear_config_and_maestro_uses_common_cli(self):
        app = self.command_app()
        settings = app._build_run_settings()
        self.assertEqual(settings['execution_backend'], 'maestro_netlist')
        self.assertEqual(settings['execution_commands']['solver_templates'], {'spectre': '', 'alps': ''})
        self.assertEqual(settings['execution_commands']['submission_template'], '')
        for backend in ('auto', 'legacy', 'persistent_maestro', 'adexl', 'maestro_netlist'):
            app.execution_backend_var.set(backend)
            self.assertEqual(app._build_run_settings()['execution_backend'], 'maestro_netlist')

    def test_solver_submission_checkpoint_restore_preserves_both_simulators(self):
        app = self.command_app()
        commands = {'solver_templates': {'spectre': gui.DEFAULT_SOLVER_TEMPLATES['spectre'],
            'alps': gui.DEFAULT_SOLVER_TEMPLATES['alps']}, 'submission_template': 'batch-submit -q chosen {job_script}'}
        app._restore_job_settings({'execution_commands': commands, 'execution_backend': 'maestro_netlist'})
        settings = app._build_run_settings()
        self.assertEqual(settings['execution_commands']['solver_templates'], commands['solver_templates'])
        self.assertEqual(settings['execution_commands']['submission_template'], commands['submission_template'])
        document = app._checkpoint_design_config({'spec': settings})
        app.solver_command_vars['alps'].set('changed')
        app.submission_command_var.set('changed')
        app._restore_job_settings(document['optimization']['run'])
        self.assertEqual(app._build_run_settings()['execution_commands'], settings['execution_commands'])

    def test_invalid_template_is_rejected_before_start(self):
        app = self.command_app()
        for template in ('{simulator} {netlist} | tee log', '{simulator} {unknown} -raw {psf} +log {log}',
                         'spectre input.scs', '{simulator} {netlist} -raw {psf} +log {log}\nsecond'):
            app.solver_command_vars['spectre'].set(template)
            with self.subTest(template=template), self.assertRaises(ValueError):
                app._build_run_settings()



@unittest.skipUnless(os.environ.get("DISPLAY"), "Native Tk policy card requires DISPLAY")
class GuiPolicyCardTests(unittest.TestCase):
    def setUp(self):
        self.root = gui.tk.Tk()
        self.root.withdraw()
        self.addCleanup(self._close)
        self.app = gui.OptimizerGui.__new__(gui.OptimizerGui)
        self.app.launch_context = None
        self.app._card = lambda parent: gui.ttk.Frame(parent)
        self.app._build_job_policy_card(self.root)

    def _close(self):
        self.root.destroy()
        self.app = None
        gc.collect()

    def policy(self):
        return policy_from_records([
            ("P", ["global", None, True]), ("J", ["global", None, "name", "Global"]),
            ("J", ["global", None, "distributionmethod", "Command"]),
            ("J", ["global", None, "jobsubmitcommand", 'batch-submit -R "cpu=6;mem=30000"']),
            ("J", ["global", None, "maxjobs", "64"]),
            ("P", ["test", "TB", True]), ("D", ["test", "TB"]),
        ])

    def test_command_card_edits_are_shared_and_clear_does_not_restore_config(self):
        self.app.service = mock.Mock(config={'runtime': {'execution_commands': {
            'solver_templates': {'spectre': gui.DEFAULT_SOLVER_TEMPLATES['spectre']},
            'submission_template': 'batch-submit -q actual {job_script}',
            'scheduler_query_template': 'batch-query --id {job_id}',
            'scheduler_cancel_template': 'batch-cancel {job_id}'}}})
        self.app._build_execution_commands_card(self.root)
        self.assertEqual(self.app.solver_command_vars['spectre'].get(), gui.DEFAULT_SOLVER_TEMPLATES['spectre'])
        self.assertEqual(self.app.submission_command_var.get(), 'batch-submit -q actual {job_script}')
        self.assertEqual(self.app.scheduler_command_vars['query'].get(), 'batch-query --id {job_id}')
        self.app.scheduler_command_vars['cancel'].set('batch-cancel --job {job_id}')
        self.app.solver_command_vars['alps'].set(gui.DEFAULT_SOLVER_TEMPLATES['alps'])
        commands = self.app._collect_execution_commands()
        self.assertEqual(commands['scheduler_cancel_template'], 'batch-cancel --job {job_id}')
        self.assertEqual(commands['solver_templates']['alps'], gui.DEFAULT_SOLVER_TEMPLATES['alps'])
        self.app._restore_execution_commands({'solver_templates': {'spectre': '', 'alps': ''}, 'submission_template': ''})
        cleared = self.app._collect_execution_commands()
        self.assertEqual(cleared['solver_templates'], {'spectre': '', 'alps': ''})
        self.assertEqual(cleared['submission_template'], '')
        self.assertEqual(cleared['scheduler_query_template'], '')
        self.assertEqual(cleared['scheduler_cancel_template'], '')

    def test_original_values_are_visible_and_unchecked(self):
        self.app._render_job_policy(self.policy())
        self.assertEqual(self.app.job_max_var.get(), "64")
        self.assertEqual(self.app.job_command_var.get(), 'batch-submit -R "cpu=6;mem=30000"')
        self.assertEqual(len(self.app.job_policy_tree.get_children("")), 2)
        self.assertFalse(hasattr(self.app, "maestro_candidate_mode_combo"))
        self.assertEqual(self.app.shared_task_mode_var.get(), "Shared CLI tasks")
        self.assertEqual(self.app.shared_task_mode_var.get(), "Shared CLI tasks")
        self.assertEqual(self.app._collect_job_settings()["maestro_job_overrides"]["max_jobs"], 64)

    def test_refresh_preserves_explicit_edit_but_new_binding_resets_it(self):
        policy = self.policy()
        self.app._render_job_policy(policy)
        self.app.job_max_var.set("2")
        self.app.job_max_override_var.set(True)
        self.app._render_job_policy(copy.deepcopy(policy))
        self.assertEqual(self.app._collect_job_settings()["maestro_job_overrides"]["max_jobs"], 2)
        self.app._render_job_policy(policy, reset=True)
        self.assertEqual(self.app.job_max_var.get(), "64")
        self.assertFalse(self.app.job_max_override_var.get())

    def test_missing_original_policy_is_shown_unknown(self):
        self.app._render_job_policy(None, reset=True)
        self.assertEqual(self.app.job_max_var.get(), "1")
        self.assertEqual(self.app.job_policy_tree.item("policy_0", "values"), ("Unknown", "Unknown", "Unknown"))
