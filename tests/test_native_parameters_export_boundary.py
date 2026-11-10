from __future__ import print_function

import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine import optimized_schematic as export
from crescendo_engine.native_maestro_parameters import GENERATION_PROTOCOL


class NativeParametersExportBoundaryTests(unittest.TestCase):
    def test_native_requested_values_cannot_be_misused_as_oa_snapshot(self):
        candidate = {"status": "completed", "execution_backend": "maestro_netlist",
                     "completion": {"ok": True}, "parameter_application": {
                         "proof_kind": "native_run_device_parameters", "ok": True,
                         "export_snapshot": {"instances": ["unverified"]}}}
        with self.assertRaises(export.OptimizedSchematicError) as caught:
            export.build_export_evidence(candidate, [])
        self.assertEqual(caught.exception.detail["capability"], "native_backannotation")

    def test_current_spec_cannot_bypass_guard_with_cached_old_evidence(self):
        with tempfile.TemporaryDirectory() as parent:
            target = os.path.join(parent, "export")
            candidate = {"status": "completed", "optimized_schematic_evidence": {"cached": True}}
            with mock.patch.object(export, "resolve_copy_libraries") as native:
                with self.assertRaises(export.OptimizedSchematicError):
                    export.export_optimized_schematic({}, {"netlist_generation_protocol": GENERATION_PROTOCOL},
                                                       candidate, {}, target)
                native.assert_not_called()
            self.assertFalse(os.path.exists(target))

    def test_old_result_remains_eligible_for_original_evidence_checks(self):
        candidate = {"status": "completed", "execution_backend": "persistent_maestro",
                     "completion": {"ok": True}}
        with self.assertRaises(export.OptimizedSchematicError) as caught:
            export.build_export_evidence(candidate, [])
        self.assertNotIn("backannotation", str(caught.exception))
