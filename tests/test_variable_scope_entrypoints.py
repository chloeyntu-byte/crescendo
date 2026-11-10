"""Known non-corner sweeps stop actual entrypoints before new EDA resources."""
import copy
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine.netlist_providers import prepare_maestro_netlists, validate_provider_request
from crescendo_engine.server import OptimizerService
from crescendo_engine.shared_maestro import SharedMaestroProducer
from crescendo_engine.variable_scope_contract import VariableScopeContractError


def prepared(global_value="6", local_value="2"):
    return {"setup": {"backend": "maestro", "mode": "SRSC",
                      "selected_test": "tb", "test_corner_masks": {"tb": {"known": True, "disabled_corners": []}}, "_source_tests": [{"name": "tb", "enabled": True}],
                      "tests": [{"name": "tb", "simulator": "spectre", "enabled": True,
                                 "variables": [{"name": "FN_gm", "value": local_value}]}],
                      "global_variables": [{"name": "FN_gm", "value": global_value,
                                            "dependent_tests": [{"test": "tb", "enabled": True}]}],
                      "corners": [{"name": "PVT", "enabled": True,
                                   "variables": [{"name": "FN_gm", "value": "8 10"}]}]},
            "parameters": [{"parameter_id": "nf", "backend": "schematic", "optimizable": True}],
            "outputs": [{"key": "tb::gain"}], "hierarchy": {}}


VARIABLES = [{"parameter_id": "nf", "minimum": 2, "maximum": 8}]
SPEC = {"evaluator": "real", "variables": VARIABLES,
        "objectives": [{"output_key": "tb::gain", "corners": ["PVT"]}], "constraints": []}


class VariableScopeEntrypointTests(unittest.TestCase):
    def test_real_start_rejects_global_or_local_sweep_before_launch_lease_or_cdf(self):
        for scope in ("global", "local"):
            with self.subTest(scope=scope), tempfile.TemporaryDirectory() as root:
                service = OptimizerService({"runtime": {"run_root": root}})
                service.prepared["p"] = prepared(global_value="6 6" if scope == "global" else "6",
                                                  local_value="1 2" if scope == "local" else "2")
                with mock.patch.object(service, "_launch") as launch, \
                        mock.patch.object(service, "_validate_live_test_bindings") as bindings, \
                        mock.patch("crescendo_engine.server.RuntimeOptimizationLease") as runtime_lease, \
                        mock.patch("crescendo_engine.server.ContinuationLease") as lease, \
                        mock.patch("crescendo_engine.server.run_range_validation") as cdf:
                    with self.assertRaises(VariableScopeContractError):
                        service.start("p", SPEC)
                    for boundary in (launch, bindings, runtime_lease, lease, cdf):
                        boundary.assert_not_called()
                self.assertEqual(service.lifecycle_operations, 0)
                self.assertEqual(os.listdir(root), [])

    def test_range_rejects_sweep_before_optional_capability_shortcut_or_resource_allocation(self):
        with tempfile.TemporaryDirectory() as root:
            service = OptimizerService({"runtime": {"run_root": root}})
            service.prepared["p"] = prepared(global_value="2 4")
            service.prepared["p"]["resident_validation"] = {"state": "unavailable"}
            with mock.patch("crescendo_engine.server.ContinuationLease") as lease, \
                    mock.patch.object(service, "_start_resident_validation") as resident, \
                    mock.patch.object(service, "_record_optional_range_validation_unavailable") as unavailable, \
                    mock.patch("crescendo_engine.server.run_range_validation") as cdf:
                with self.assertRaises(VariableScopeContractError):
                    service.validate_ranges("p", VARIABLES)
                for boundary in (lease, resident, unavailable, cdf):
                    boundary.assert_not_called()
            self.assertEqual(service.range_validations, {})
            self.assertEqual(service.lifecycle_operations, 0)
            self.assertEqual(os.listdir(root), [])

    def test_static_preview_and_mock_start_do_not_gain_new_sweep_restriction(self):
        with tempfile.TemporaryDirectory() as root:
            service = OptimizerService({"runtime": {"run_root": root}})
            service.prepared["p"] = prepared(global_value="2 4")
            spec = dict(SPEC, evaluator="mock")
            with mock.patch("crescendo_engine.server.require_unique_run_variables", side_effect=AssertionError("no real EDA guard")) as guard, \
                    mock.patch("crescendo_engine.server.require_prepared_freshness"), \
                    mock.patch.object(service, "_launch", return_value={"run_id": "mock"}):
                self.assertTrue(service.validate_static_ranges("p", VARIABLES)["valid"])
                self.assertEqual(service.start("p", spec)["run_id"], "mock")
                guard.assert_not_called()

    def test_scalar_expression_corner_list_and_blank_definition_are_not_new_ui_blocks(self):
        with tempfile.TemporaryDirectory() as root:
            service = OptimizerService({"runtime": {"run_root": root}})
            service.prepared["p"] = prepared(global_value="2 * BASE", local_value="")
            service.prepared["p"]["setup"]["corners"][0]["variables"].append({"name": "FROM_DEFS", "value": ""})
            service.prepared["p"]["resident_validation"] = {"state": "unavailable"}
            with mock.patch("crescendo_engine.server.require_prepared_freshness"), \
                    mock.patch.object(service, "_launch", return_value={"run_id": "accepted"}) as launch, \
                    mock.patch.object(service, "_record_optional_range_validation_unavailable", return_value={"state": "unavailable"}) as unavailable:
                self.assertEqual(service.start("p", SPEC)["run_id"], "accepted")
                self.assertEqual(service.validate_ranges("p", VARIABLES)["state"], "unavailable")
                launch.assert_called_once()
                unavailable.assert_called_once()

    def test_direct_provider_rejects_both_source_modes_before_solver_qualification(self):
        source = prepared(global_value="2 4")
        for mode in ("maestro_netlist", "private_netlist"):
            with self.subTest(mode=mode), \
                    mock.patch("crescendo_engine.netlist_providers._qualify_setup_capture") as qualification, \
                    mock.patch("crescendo_engine.netlist_providers.execution_contract") as solver:
                with self.assertRaises(VariableScopeContractError):
                    validate_provider_request(source, source["parameters"], {}, SPEC, source_mode=mode)
                qualification.assert_not_called()
                solver.assert_not_called()

    def test_actual_numeric_provider_rejects_before_copy_cdf_or_candidate_directory(self):
        source = prepared(local_value="2 4")
        with tempfile.TemporaryDirectory() as root:
            target = os.path.join(root, "candidate")
            with mock.patch("crescendo_engine.netlist_providers.build_workspace") as workspace, \
                    mock.patch("crescendo_engine.netlist_providers._capture_phase") as capture:
                with self.assertRaises(VariableScopeContractError):
                    prepare_maestro_netlists(source, source["parameters"], {"nf": 6}, {}, target, "c1", spec=SPEC)
                workspace.assert_not_called()
                capture.assert_not_called()
            self.assertFalse(os.path.exists(target))

    def test_shared_frontend_rejects_before_epoch_owner_or_native_work(self):
        source = prepared(global_value="2 4")
        with tempfile.TemporaryDirectory() as root:
            with self.assertRaises(VariableScopeContractError):
                SharedMaestroProducer(root, source, source["parameters"], {}, SPEC)
            self.assertEqual(os.listdir(root), [])

    def test_provider_pending_report_never_claims_cdf_or_point_success(self):
        source = prepared(global_value="2 * BASE")
        with mock.patch("crescendo_engine.netlist_providers._qualify_setup_capture"), \
                mock.patch("crescendo_engine.netlist_providers.normalize_execution_commands", return_value={}), \
                mock.patch("crescendo_engine.netlist_execution.execution_policy", return_value={"global": {}, "tests": {}}), \
                mock.patch("crescendo_engine.netlist_providers.submission_cpu_hint", return_value=None), \
                mock.patch("crescendo_engine.netlist_providers.execution_contract"), \
                mock.patch("crescendo_engine.netlist_measurement.qualify_expressions", return_value={"supported": True}):
            request = validate_provider_request(source, source["parameters"], {}, SPEC)
        evidence = request["variable_scope"]
        self.assertTrue(evidence["multiplicity_ok"])
        self.assertTrue(evidence["native_pending"])
        self.assertFalse(evidence["effective_point_values_proven"])
        self.assertEqual(evidence["source"], "saved_setup_only")


if __name__ == "__main__":
    unittest.main()
