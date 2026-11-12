"""Real Tk resource preferences through the configuration entry points."""
import os
import subprocess
import sys
import unittest
from unittest import mock
from tests import test_gui_optional_settings as fixture
from crescendo_engine.unified_configuration import normalize_configuration, build_configuration


class _ResourceSettingsFixture(object):
    def setUp(self):
        from crescendo import app
        preferences = mock.patch.object(app, "_prefs_path", side_effect=lambda: os.path.join(
            self.directory.name, "crescendo-ui.json"))
        preferences.start()
        self.addCleanup(preferences.stop)
        with mock.patch.object(fixture.gui, "OptimizerGui", app.CrescendoGui):
            fixture.GuiOptionalSettingsTkTests.setUp(self)
        self.assertIsInstance(self.app, app.CrescendoGui)

    _close = fixture.GuiOptionalSettingsTkTests._close
    _wait = fixture.GuiOptionalSettingsTkTests._wait

    def test_resource_fields_roundtrip_configuration_and_pro_visibility(self):
        values = {"native_candidate_batching": "generation", "local_solver_max_jobs": "3", "measurement_jobs": "2", "progress_flush_interval_seconds": "0.75"}
        self.app._restore_execution_resources(values)
        self.assertEqual(self.app._collect_execution_resources(), {
            "native_candidate_batching": "generation", "local_solver_max_jobs": 3, "measurement_jobs": 2, "progress_flush_interval_seconds": 0.75})
        raw, unused_context = self.app._snapshot_configuration()
        document = build_configuration(raw)
        normalized = normalize_configuration(document)["settings"]["optimization"]
        for key, value in values.items():
            self.assertEqual(normalized[key], value)
        self.app._restore_execution_resources(dict.fromkeys(values, "1"))
        self.app._restore_execution_resources(normalized)
        self.assertEqual(self.app._execution_resource_values(), values)
        self.root.deiconify()
        for pro in (False, True, False):
            self.app.pro_mode_var.set(pro)
            self.app._apply_mode()
            if pro:
                self.app.show_page("EDA Settings")
                self.app.settings_notebook.select(self.app.settings_sections["crescendo"])
            self.root.update()
            card = self.app.execution_resources_card
            self.assertEqual(card.winfo_manager(), "pack" if pro else "")
            self.assertEqual(bool(card.winfo_ismapped()), pro)
        self.assertFalse(self.errors)


@unittest.skipUnless(os.environ.get("DISPLAY"), "Real Tk requires DISPLAY")
class ResourceSettingsGuiTests(unittest.TestCase):
    def test_resource_fields_roundtrip_configuration_and_pro_visibility(self):
        # The presentation import installs process-wide hooks when launcher
        # tests later call install(). Give this real window its own process.
        original = fixture.gui.OptimizerGui
        imported = "crescendo.app" in sys.modules
        result = subprocess.run([sys.executable, "-B", "-m", "tests.test_accel_p0_gui", "--fixture"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=30)
        self.assertEqual(result.returncode, 0, result.stdout.decode("utf-8", "replace"))
        self.assertIs(fixture.gui.OptimizerGui, original)
        self.assertEqual("crescendo.app" in sys.modules, imported)
        print("P0_GUI real_Crescendo Simple=hidden Pro=mapped Simple=hidden")


if __name__ == "__main__":
    class ResourceSettingsWindowTests(_ResourceSettingsFixture, unittest.TestCase):
        pass
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(ResourceSettingsWindowTests)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
