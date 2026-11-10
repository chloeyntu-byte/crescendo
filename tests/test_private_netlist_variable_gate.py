import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine import private_netlist
from crescendo_engine.variable_scope_contract import VariableScopeContractError


class PrivateNetlistVariableGateTests(unittest.TestCase):
    def setup(self, value, global_scope=False):
        result = {"tests": [{"name": "T", "enabled": True, "variables": []}],
                  "global_variables": [], "corners": []}
        rows = (result["global_variables"] if global_scope else
                result["tests"][0]["variables"])
        rows.append({"name": "FN_gm", "value": value, "sweep_enabled": True})
        return result

    def test_template_rejects_known_multi_before_creating_root_or_cdf_workspace(self):
        for global_scope in (False, True):
            with self.subTest(global_scope=global_scope), tempfile.TemporaryDirectory() as parent:
                root = os.path.join(parent, "template")
                prepared = {"setup": self.setup("2 2", global_scope)}
                with mock.patch.object(private_netlist, "build_workspace") as build:
                    with self.assertRaises(VariableScopeContractError):
                        private_netlist.prepare_private_netlist(prepared, [], {}, root)
                    build.assert_not_called()
                self.assertFalse(os.path.lexists(root))

    def test_capture_rejects_multi_before_reset_patch_or_cadence(self):
        with tempfile.TemporaryDirectory() as parent:
            root = os.path.join(parent, "capture")
            workspace = {"setup": self.setup("1 2")}
            with mock.patch.object(private_netlist, "reset_results") as reset, \
                    mock.patch.object(private_netlist, "write_schematic_patch") as patch, \
                    mock.patch.object(private_netlist, "run_command") as run:
                with self.assertRaises(VariableScopeContractError):
                    private_netlist._capture_phase(workspace, [], {}, {}, {}, {}, root)
                reset.assert_not_called()
                patch.assert_not_called()
                run.assert_not_called()
            self.assertFalse(os.path.lexists(root))

    def test_scalar_expression_blank_and_corner_list_reach_existing_native_qualification(self):
        # Passing this source gate must not stand in for native qualification.
        for value in ("OTHER_VAR * 2", ""):
            with self.subTest(value=value), tempfile.TemporaryDirectory() as parent:
                root = os.path.join(parent, "capture")
                setup = self.setup(value)
                setup["corners"] = [{"name": "TT", "variables": [
                    {"name": "VDD", "value": "0.7 0.8"}]}]
                with mock.patch.object(private_netlist, "_capture_setup_identity",
                                       side_effect=RuntimeError("native identity pending")) as identity:
                    with self.assertRaisesRegex(RuntimeError, "native identity pending"):
                        private_netlist._capture_phase({"setup": setup}, [], {}, {}, {}, {}, root)
                    identity.assert_called_once()


if __name__ == "__main__":
    unittest.main()
