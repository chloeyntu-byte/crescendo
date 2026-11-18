"""Exercise the advisory through the real runner with synthetic native evidence."""
import json
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine import runner
from crescendo_engine.environment_diagnostics import collect_run_diagnostics, failure_summary
from crescendo_engine.run_log import LOG_NAME
from crescendo_engine.single_test import project_setup
from crescendo_engine.variable_selection_notice import VARIABLE_NOTICE_STAGE, variable_selection_notice
from crescendo_engine.utils import load_json
from tests.test_netlist_optimizer import fixtures
from crescendo_engine.private_netlist import _raise_capture_failure


class VariableSelectionNoticeTests(unittest.TestCase):
    def test_count_boundary_and_backend(self):
        for mode in ("private_netlist", "maestro_baseline", "maestro_netlist", "auto"):
            with self.subTest(mode=mode):
                self.assertEqual(variable_selection_notice(mode, 50), "")
                notice = variable_selection_notice(mode, 51)
                self.assertTrue(notice.startswith("Selected 51 variables."))
                if mode == "private_netlist":
                    self.assertIn("reference capture can cause Virtuoso to quit unexpectedly", notice)
                    self.assertIn("use Maestro Netlist or Native Run", notice)
                else:
                    self.assertIn("time per generation", notice)
                    self.assertIn("number of generations needed to converge", notice)
                    self.assertIn("narrowing the selection", notice)
                    self.assertNotIn("Virtuoso", notice)

    def test_native_and_maestro_log_effective_selection_at_runner_start(self):
        for mode in ("maestro_baseline", "maestro_netlist"):
            for locked in ([], ["x0"]):
                with self.subTest(mode=mode, locked=locked), tempfile.TemporaryDirectory() as root:
                    prepared, config, spec = self._selection(root, 51)
                    spec["execution_backend"] = mode
                    run_root = os.path.join(root, "run")
                    # Reaching dataset initialization means the common startup path
                    # accepted the selection and wrote the advisory before any EDA work.
                    with mock.patch("crescendo_engine.design_variables.qualify_priority",
                                    return_value={"locked_parameter_ids": locked, "warnings": []}), \
                            mock.patch.object(runner, "RunDataStore", side_effect=RuntimeError("fixture stop")):
                        with self.assertRaisesRegex(RuntimeError, "fixture stop"):
                            runner.run_optimization(spec, config, prepared=prepared, run_root=run_root)
                    effective = 51 - len(locked)
                    self._assert_notice(run_root, mode, effective)

    def test_run_warns_for_effective_selection_and_preserves_native_failure(self):
        for count, locked, returncode, crashed in (
                (50, [], 139, True), (51, ["x0"], 139, True), (51, [], 1, False),
                (51, [], 139, True), (51, [], 1, True)):
            with self.subTest(count=count, locked=locked, returncode=returncode, crashed=crashed), \
                    tempfile.TemporaryDirectory() as root:
                self._run(count, locked, returncode, crashed, root)

    def _selection(self, root, count):
        prepared, config, spec = fixtures(root, max_jobs=1, population=2)
        prepared["setup"]["tests"] = [{"name": "main", "enabled": True}]
        prepared["setup"] = project_setup(prepared["setup"], "main")
        prepared["parameters"] = [dict(parameter_id="x%d" % i, backend="schematic", optimizable=True)
                                  for i in range(count - 2)]
        for kind in ("global", "top"):
            prepared["parameters"].append(dict(parameter_id=kind, backend="design_variable", name=kind,
                source_kind=kind, source=kind, variable_test="main", optimizable=True))
        spec["variables"] = [dict(parameter_id=row["parameter_id"], minimum=1, maximum=2)
                             for row in prepared["parameters"]]
        for variable in spec["variables"][1:3]:
            variable["link_group"] = "paired"
        return prepared, config, spec

    def _run(self, count, locked, returncode, crashed, root):
        prepared, config, spec = self._selection(root, count)
        command = {"returncode": returncode, "stdout_tail": (
            "*WARNING* Process was terminated with SIGSEGV signal" if crashed else "")}

        def fail(_prepared, selected, _config, capture_root, **_kwargs):
            self.assertEqual(len(selected), count - len(locked))
            _raise_capture_failure(False, command, {"ok": False}, {"ok": True},
                                   {"ocean": command}, {}, capture_root)

        context = mock.Mock()
        context.unsafe.is_set.return_value = False
        context.close.return_value = {"cleanup_proven": True}
        run_root = os.path.join(root, "run")
        with mock.patch("crescendo_engine.design_variables.qualify_priority",
                        return_value={"locked_parameter_ids": locked, "warnings": []}), \
                mock.patch("crescendo_engine.netlist_optimizer.NetlistExecutionContext", return_value=context), \
                mock.patch("crescendo_engine.netlist_optimizer.prepare_private_netlist", side_effect=fail) as prepare:
            with self.assertRaisesRegex(RuntimeError, "native netlist"):
                runner.run_optimization(spec, config, prepared=prepared, run_root=run_root)
        prepare.assert_called_once()
        context.evaluate.assert_not_called()
        effective = count - len(locked)
        expected = self._assert_notice(run_root, "private_netlist", effective)
        status = load_json(os.path.join(run_root, "status.json"))
        self.assertEqual(status["state"], "failed")
        self.assertFalse(status["shared_preparation_failure"]["execution_started"])
        cause = failure_summary(status, [])["first_cause"]
        if expected and crashed:
            notice = variable_selection_notice("private_netlist", 51)
            self.assertEqual(cause["advisory"], notice)
            self.assertTrue(status["message"].endswith("\n" + notice))
        else:
            self.assertNotIn("advisory", cause)
            self.assertNotIn("select fewer variables", status["message"])

    def _assert_notice(self, run_root, mode, effective):
        path = os.path.join(run_root, LOG_NAME)
        warnings = []
        if os.path.isfile(path):
            with open(path, encoding="utf-8") as stream:
                warnings = [row for row in map(json.loads, stream) if row["stage"] == VARIABLE_NOTICE_STAGE]
        expected = int(effective > 50)
        self.assertEqual(len(warnings), expected)
        if expected:
            self.assertEqual(warnings[0]["level"], "WARN")
            self.assertEqual(warnings[0]["details"], {"selected_variables": effective, "threshold": 50})
            self.assertIn(variable_selection_notice(mode, effective), warnings[0]["message"])
        records = collect_run_diagnostics(run_root, mode)
        notices = [row["payload"] for row in records if row["observation"] == "diagnostic." + VARIABLE_NOTICE_STAGE]
        self.assertEqual(len(notices), expected)
        if expected:
            self.assertEqual(notices[0]["level"], "WARN")
            self.assertEqual(notices[0]["message"], warnings[0]["message"])
        return expected
