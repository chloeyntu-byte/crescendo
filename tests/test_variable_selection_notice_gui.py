"""Real Tk coverage of selection changes and Start with the advisory visible."""
import os
import unittest
from unittest import mock

from crescendo import app as frontend
from crescendo_engine import gui
from tests import test_gui_optional_settings as fixtures


@unittest.skipUnless(os.environ.get("DISPLAY"), "Real Tk requires DISPLAY")
class VariableSelectionNoticeGuiTests(unittest.TestCase):
    _close = fixtures.GuiOptionalSettingsTkTests._close
    _wait = fixtures.GuiOptionalSettingsTkTests._wait
    _prepared = fixtures.GuiOptionalSettingsTkTests._prepared

    def setUp(self):
        with mock.patch.object(gui, "OptimizerGui", frontend.CrescendoGui):
            fixtures.GuiOptionalSettingsTkTests.setUp(self)
        original = self.service.prepared_fixture

        def prepared():
            identifier, snapshot = original()
            snapshot["setup"]["tests"][0]["simulator"] = "spectre"
            for kind in ("global", "top"):
                snapshot["parameters"].append(dict(parameter_id=kind, backend="design_variable", source_kind=kind, source=kind,
                    variable_test="VCO", name=kind, numeric_value=1, current_value="1", optimizable=True))
            return identifier, snapshot

        self.service.parameter_count = 49
        self.service.prepared_fixture = prepared
        self._prepared()
        self.root.deiconify()
        self.app.show_page("Parameters")

    def test_notice_follows_selection_and_mode_in_simple_and_pro(self):
        app = self.app
        label = app.variable_selection_notice_label
        for pro in (False, True):
            app.pro_mode_var.set(pro)
            app._apply_mode(initial=True)
            app.parameter_all_button.invoke()
            self._wait(lambda: label.winfo_ismapped())
            for mode, wording in (("private_netlist", "reference capture"),
                                  ("maestro_baseline", "time per generation"),
                                  ("auto", "time per generation")):
                with self.subTest(pro=pro, mode=mode):
                    app.execution_backend_var.set(mode)
                    self.assertTrue(label.winfo_ismapped())
                    self.assertIn("Selected 51 variables.", label.cget("text"))
                    self.assertIn(wording, label.cget("text"))
            app.parameter_configs["top"]["selected"] = False
            app._refresh_parameter_selection_display()
            self.assertEqual(label.winfo_manager(), "")
            for mode in ("private_netlist", "maestro_baseline", "auto"):
                app.execution_backend_var.set(mode)
                self.assertEqual(label.winfo_manager(), "")
            app.parameter_all_button.invoke()
            self.assertEqual(label.winfo_manager(), "pack")
            app.parameter_none_button.invoke()
            self.assertEqual(label.winfo_manager(), "")

    def test_start_dispatches_template_without_warning_dialog(self):
        self._assert_start("private_netlist", "private_netlist")

    def test_start_dispatches_native_without_warning_dialog(self):
        self._assert_start("maestro_baseline", "maestro_baseline")

    def test_start_dispatches_maestro_without_warning_dialog(self):
        self._assert_start("auto", "maestro_netlist")

    def _assert_start(self, choice, backend):
        app = self.app
        app.execution_backend_var.set(choice)
        app.parameter_all_button.invoke()
        for key in ("/M0/w", "/M1/w"):
            app.parameter_configs[key]["link_group"] = "paired"
        self.assertEqual(app.variable_selection_notice_label.winfo_manager(), "pack")
        self.assertEqual(app._local_range_report()["issues"], [])
        self.assertNotIn("disabled", app.start_button.state())
        app.job_command_override_var.set(True)
        app.job_command_var.set("fixture-submit")
        with mock.patch.object(app, "_request_run_display_name", return_value="fixture optimization"), \
                mock.patch.object(app, "_poll_progress"), \
                mock.patch.object(gui.messagebox, "askyesno") as confirm, \
                mock.patch.object(gui.messagebox, "askokcancel") as ok_cancel, \
                mock.patch.object(gui.messagebox, "showwarning") as warning:
            app.start_button.invoke()
            self._wait(lambda: self.showerror.called or (self.service.start.called and app.run_ui.state == "running"))
            confirm.assert_not_called()
            ok_cancel.assert_not_called()
            warning.assert_not_called()
        self.assertFalse(self.showerror.called, self.showerror.call_args)
        spec = self.service.start.call_args[0][1]
        self.assertEqual(spec["execution_backend"], backend)
        self.assertEqual(len(spec["variables"]), 51)
        self.assertEqual(sum(row.get("link_group") == "paired" for row in spec["variables"]), 2)
