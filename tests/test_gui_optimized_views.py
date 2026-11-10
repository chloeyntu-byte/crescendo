"""Real Tk selection/export behavior with a fixture service, never EDA."""
import gc
import os
import threading
import unittest
from unittest import mock

from tests import test_gui_optional_settings as fixtures


@unittest.skipUnless(os.environ.get("DISPLAY"), "An existing display is required")
class GuiOptimizedViewsTkTests(unittest.TestCase):
    setUp = fixtures.GuiOptionalSettingsTkTests.setUp
    _wait = fixtures.GuiOptionalSettingsTkTests._wait
    _prepared = fixtures.GuiOptionalSettingsTkTests._prepared

    def _close(self):
        fixtures.GuiOptionalSettingsTkTests._close(self)
        self.app = None
        gc.collect()

    def _solution(self):
        self._prepared()
        self.app.live_spec = {"evaluator": "real"}
        self.app.live_candidates = [{"candidate_id": "g000_c0000", "status": "completed",
                                     "values": {"/M0/w": 3.96e-6}}]
        self.app.run_ui.started("finished-run")
        self.app.run_ui.update("completed")
        self.service.generate_optimized_schematic = mock.Mock(return_value={"ok": True, "views": [
            {"library": "fixture_lib", "cell": "B", "view": "schematic_Selected_Solution"}]})
        self.app.select_candidate("g000_c0000", navigate=False)

    def test_button_requires_selected_real_completed_candidate_and_terminal_run(self):
        self._solution()
        self.assertEqual(str(self.app.optimized_schematic_button.cget("state")), "normal")
        for state, candidate_state, evaluator in (("running", "completed", "real"),
                                                   ("stopping", "completed", "real"),
                                                   ("completed", "planned", "real"),
                                                   ("completed", "failed", "real"),
                                                   ("completed", "completed", "mock")):
            self.app.run_ui.update(state)
            self.app.live_candidates[0]["status"] = candidate_state
            self.app.live_spec["evaluator"] = evaluator
            self.app._render_candidate_detail()
            self.assertEqual(str(self.app.optimized_schematic_button.cget("state")), "disabled")
        self.service.generate_optimized_schematic.assert_not_called()

    def test_real_button_passes_selected_identity_and_displays_actual_created_view(self):
        self._solution()
        self.app.optimized_schematic_button.invoke()
        self._wait(lambda: self.service.generate_optimized_schematic.called)
        self._wait(lambda: not self.app.optimized_schematic_inflight)
        self.service.generate_optimized_schematic.assert_called_once_with("finished-run", "g000_c0000")
        self.assertIn("fixture_lib/B/schematic_Selected_Solution", self.app.optimized_schematic_status_var.get())
        self.assertIn("g000_c0000", self.app.candidate_detail.get("1.0", "end"))
        self.assertEqual(str(self.app.optimized_schematic_button.cget("state")), "normal")

    def test_export_runs_off_tk_thread_and_duplicate_clicks_do_not_write_twice(self):
        self._solution()
        entered, release = threading.Event(), threading.Event()
        self.releases.append(release)
        worker_threads = []
        owner_thread = threading.current_thread()

        def export(*args):
            worker_threads.append(threading.current_thread())
            entered.set()
            if not release.wait(2):
                raise AssertionError("Fixture export was not released")
            return {"ok": True, "views": [{"library": "fixture_lib", "cell": "B", "view": "schematic_Selected_Solution_1"}]}

        self.service.generate_optimized_schematic.side_effect = export
        self.app.optimized_schematic_button.invoke()
        self._wait(entered.is_set)
        self.assertTrue(self.app.optimized_schematic_inflight)
        self.assertEqual(str(self.app.optimized_schematic_button.cget("state")), "disabled")
        self.app.generate_optimized_schematic()
        self.root.update()
        self.assertIsNot(worker_threads[0], owner_thread)
        release.set()
        self._wait(lambda: not self.app.optimized_schematic_inflight)
        self.service.generate_optimized_schematic.assert_called_once_with("finished-run", "g000_c0000")
        self.assertIn("schematic_Selected_Solution_1", self.app.optimized_schematic_status_var.get())

    def test_failed_export_restores_button_and_keeps_selected_solution(self):
        self._solution()
        self.service.generate_optimized_schematic.side_effect = RuntimeError("Readback failed: owned diagnostic")
        self.app.optimized_schematic_button.invoke()
        self._wait(lambda: self.service.generate_optimized_schematic.called)
        self._wait(lambda: not self.app.optimized_schematic_inflight)
        self.assertEqual(self.app.selected_candidate_id, "g000_c0000")
        self.assertIn("Readback failed", self.app.optimized_schematic_status_var.get())
        self.assertEqual(str(self.app.optimized_schematic_button.cget("state")), "normal")
        self.assertTrue(self.showerror.called)

    def test_detail_shows_native_actual_value_and_requested_value_when_different(self):
        self._solution()
        self.app.parameter_configs["/M0/w"]["unit"] = "u"
        row = self.app.live_candidates[0]
        row["values"] = {"/M0/w": 4e-6}
        row["execution_backend"] = "persistent_maestro"
        row["parameter_application"] = {"ok": True, "parameters": [
            {"parameter_id": "/M0/w", "persisted_schematic_value": "3.96u"}]}
        self.app._render_candidate_detail()
        detail = self.app.candidate_detail.get("1.0", "end")
        self.assertIn("3.96 u", detail)
        self.assertIn("actual saved value; requested 4e-06", detail)
        row["execution_backend"] = "private_netlist"
        row.pop("parameter_application")
        row["completion_evidence"] = {"ok": True, "parameters": {"/M0/w": 3.95e-6}}
        self.app._render_candidate_detail()
        detail = self.app.candidate_detail.get("1.0", "end")
        self.assertIn("3.95 u", detail)
        self.assertIn("actual saved value; requested 4e-06", detail)


if __name__ == "__main__":
    unittest.main()
