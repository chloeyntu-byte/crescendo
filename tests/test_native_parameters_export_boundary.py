from __future__ import print_function

import copy
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine import optimized_schematic as export
from crescendo_engine.native_maestro_parameters import GENERATION_PROTOCOL
from tests.test_optimized_schematic import parameter


def native_candidate(backend="maestro_baseline"):
    completed = {"test": "dc", "point_id": 3, "corner": "nominal", "netlist_path": "/owned/3/input.scs",
                 "psf_parameters": {"DPAR_1": 3.5e-6}}
    point = dict(completed, effective=3.5e-6, scs_path=completed["netlist_path"],
                 proof_source="generated_point_scs_bound_parameters", callback_adjustment_proven=True,
                 statement_sha256="a" * 64, dpar_values={"DPAR_1": 3.5e-6})
    return {"status": "completed", "candidate_id": "g000_c0001", "execution_backend": backend,
            "values": {"p1": 4e-6},
            "completion_evidence": {"ok": True, "history_name": "Interactive.0", "points": [completed],
                                    "parameters": {"p1": 3.5e-6},
                                    "execution_pipeline": {"schema_version": export._contract_text("crescendo.maestro_baseline.v1"),
                                                           "history_name": "Interactive.0", "solver_completion_proven": True}},
            "parameter_application": {"ok": True, "proof_kind": "native_run_device_parameters",
                                      "generation_protocol": GENERATION_PROTOCOL, "solver_completion_proven": True,
                                      "parameters": [{"parameter_id": "p1", "requested": 4e-6, "points": [point]}]}}


class NativeParametersExportBoundaryTests(unittest.TestCase):
    def test_modes_use_observed_native_points_and_completed_netlist_values(self):
        for backend, expected in (("maestro_baseline", "native_run_point_parameters"),
                                  ("maestro_netlist", "maestro_netlist_point_parameters")):
            with self.subTest(backend=backend):
                candidate = native_candidate(backend)
                if backend == "maestro_netlist":
                    candidate["completion_evidence"]["netlist_generation"] = {
                        "parameter_application": candidate.pop("parameter_application")}
                evidence = export.build_export_evidence(candidate, [parameter()])
                self.assertEqual(evidence["values"], {"p1": 3.5e-6})
                self.assertEqual(evidence["proof"]["kind"], expected)
                self.assertEqual(evidence["proof"]["parameters"]["p1"][0]["point_id"], 3)

    def test_refuses_wrong_missing_or_input_only_native_point_proof(self):
        for mutation, reason in (
                ({"point_id": 4}, "coverage"),
                ({"scs_path": "/other/3/input.scs"}, "different Native Run netlist"),
                ({"effective": None}, "numeric parameter"),
                ({"proof_source": "full_identity_native_RDB_input"}, "effective instance/property readback"),
                ({"proof_source": "unique_selected_instance_DPAR"}, "effective instance/property readback"),
                ({"dpar_values": {"DPAR_1": 4e-6}}, "PSF and netlist")):
            with self.subTest(mutation=mutation):
                candidate = native_candidate()
                candidate["parameter_application"]["parameters"][0]["points"][0].update(mutation)
                with self.assertRaisesRegex(export.OptimizedSchematicError, reason):
                    export.build_export_evidence(candidate, [parameter()])

    def test_refuses_disagreeing_values_between_completed_points(self):
        candidate = native_candidate()
        completion = candidate["completion_evidence"]
        application = candidate["parameter_application"]["parameters"][0]
        completion["points"].append(dict(completion["points"][0], point_id=4))
        second = copy.deepcopy(application["points"][0])
        second.update(point_id=4, effective=3.7e-6)
        application["points"].append(second)
        with self.assertRaisesRegex(export.OptimizedSchematicError, "varies across"):
            export.build_export_evidence(candidate, [parameter()])

    def test_refuses_cached_snapshot_for_current_generation_without_completion_proof(self):
        with tempfile.TemporaryDirectory() as parent:
            target = os.path.join(parent, "export")
            candidate = {"status": "completed", "optimized_schematic_evidence": {"cached": True}}
            with mock.patch.object(export, "resolve_copy_libraries") as native:
                with self.assertRaisesRegex(export.OptimizedSchematicError, "completion evidence"):
                    export.export_optimized_schematic({"setup": {}}, {"netlist_generation_protocol": GENERATION_PROTOCOL},
                                                       candidate, {}, target)
                native.assert_not_called()
            self.assertFalse(os.path.exists(target))

    def test_refuses_requested_only_values_or_mismatched_candidate(self):
        candidate = native_candidate()
        candidate["values"]["p1"] = 5e-6
        with self.assertRaisesRegex(export.OptimizedSchematicError, "another candidate"):
            export.build_export_evidence(candidate, [parameter()])
        candidate["parameter_application"].pop("solver_completion_proven")
        with self.assertRaisesRegex(export.OptimizedSchematicError, "completed history"):
            export.build_export_evidence(candidate, [parameter()])
