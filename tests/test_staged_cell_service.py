import copy
import json
import os
import tempfile
import threading
import unittest
from unittest import mock

from crescendo_engine.ade_setup import inspect_setup
from crescendo_engine.cadence import CommandCancelled
from crescendo_engine.config import DEFAULTS
from crescendo_engine.optimizers import PopulationGenerator
from crescendo_engine.server import OptimizerService
from crescendo_engine import hierarchy, runner


class StagedCellServiceTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = self.temporary.name
        view = os.path.join(self.root, "userLib", "TB", "maestro")
        os.makedirs(view)
        self.path = os.path.join(view, "maestro.sdb")
        with open(self.path, "w") as stream:
            stream.write('<setupdb Type="maestro"><active><tests><test Name="T">'
                         '<tooloptions><option Name="lib"><value>userLib</value></option>'
                         '<option Name="cell"><value>TB</value></option>'
                         '<option Name="view"><value>schematic</value></option>'
                         '</tooloptions></test></tests></active></setupdb>')
        self.config = copy.deepcopy(DEFAULTS)
        self.config["runtime"]["run_root"] = os.path.join(self.root, "runs")
        self.service = OptimizerService(self.config)
        # These fixtures exercise discovery/freshness gates with synthetic OA
        # identities. Real Setup private-view lifetime is covered separately.
        private_session = mock.patch.object(self.service, "_prepare_setup_private_session")
        private_session.start()
        self.addCleanup(private_session.stop)
        design = {"library": "userLib", "cell": "TB", "view": "schematic"}
        self.scopes = [{"scope_id": "scope-" + name, "design": design,
                        "hierarchy_path": "/" + name, "library": "userLib",
                        "cell": name, "view": "schematic", "depth": 1,
                        "selectable": True} for name in ("I1", "I10")]
        self.scopes.append(dict(self.scopes[0], scope_id="pdk-wrapper", selectable=False))
        self.hierarchy = {"designs": [{"design": design, "hierarchy": {
            "cells": [{"library": "userLib", "cell": "TB", "view": "schematic", "hierarchy_path": "/"},
                      {"library": "userLib", "cell": "I1", "view": "schematic", "hierarchy_path": "/I1"},
                      {"library": "userLib", "cell": "I10", "view": "schematic", "hierarchy_path": "/I10"}],
            "instances": [], "parameters": [],
        }}]}

    def tearDown(self):
        self.temporary.cleanup()

    def _inspect(self):
        with mock.patch.object(hierarchy, "probe_hierarchy", return_value=self.hierarchy), \
                mock.patch.object(hierarchy, "cell_scope_records", return_value=self.scopes, create=True):
            return self.service.inspect_cells(self.path)

    def _prepared(self):
        return {"setup": inspect_setup(self.path), "hierarchy": self.hierarchy,
                "parameters": [], "outputs": [], "eda_context": {"pdk": {}},
                "source_identity": {"schema_version": "crescendo.source_design_identity.v1", "fingerprint": "full-user-source"}}

    def test_preview_is_cells_only_and_does_not_register_or_deep_prepare(self):
        with mock.patch.object(hierarchy, "probe_hierarchy", return_value=self.hierarchy) as probe, \
                mock.patch.object(hierarchy, "cell_scope_records", return_value=self.scopes, create=True), \
                mock.patch("crescendo_engine.server.prepare_setup") as prepare, \
                mock.patch("crescendo_engine.server.capture_prepared_freshness") as freshness, \
                mock.patch.object(self.service, "_start_resident_validation") as resident:
            token, preview = self.service.inspect_cells(self.path, max_depth=20)
        self.assertIn(token, self.service.inspections)
        self.assertNotIn("hierarchy", self.service.inspections[token]["preview"])
        self.assertEqual(self.service.prepared, {})
        self.assertEqual(len(preview["cell_scopes"]), 2)
        self.assertNotIn("parameters", preview)
        self.assertNotIn("source_identity", preview)
        self.assertEqual(probe.call_args[1], {"max_depth": 20, "cells_only": True})
        prepare.assert_not_called()
        freshness.assert_not_called()
        resident.assert_not_called()

    def test_apply_selected_cells_keeps_freshness_and_defers_native_validation(self):
        token, preview = self._inspect()
        with mock.patch.object(hierarchy, "cell_scope_records", return_value=self.scopes, create=True), \
                mock.patch("crescendo_engine.server.prepare_setup", return_value=self._prepared()) as prepare, \
                mock.patch.object(self.service, "_start_resident_validation") as resident:
            prepared_id, prepared = self.service.prepare_cells(token, ["scope-I1", "scope-I1"])
        self.assertIn(prepared_id, self.service.prepared)
        self.assertEqual(prepare.call_args[1]["selected_cell_paths"], [preview["cell_scopes"][0]])
        self.assertEqual(prepare.call_args[1]["backend"], "maestro")
        self.assertEqual(prepared["cell_selection"]["scope_ids"], ["scope-I1"])
        from crescendo_engine.workspace import source_design_identity
        self.assertEqual(prepared["source_identity"], source_design_identity(
            prepared["setup"], prepared["hierarchy"], config=self.service.config))
        self.assertEqual(prepared["prepared_freshness"]["schema_version"], "crescendo.prepared_freshness.v1")
        self.assertEqual(prepared["resident_validation"]["state"], "deferred")
        resident.assert_not_called()

    def test_empty_unknown_reference_and_malformed_scope_lists_are_rejected_before_prepare(self):
        token, _preview = self._inspect()
        with mock.patch.object(hierarchy, "cell_scope_records", return_value=self.scopes, create=True), \
                mock.patch("crescendo_engine.server.prepare_setup") as prepare:
            for scopes in ([], None, "scope-I1", ["unknown"], ["pdk-wrapper"], [{}]):
                with self.subTest(scopes=scopes):
                    with self.assertRaises(ValueError):
                        self.service.prepare_cells(token, scopes)
            prepare.assert_not_called()

    def test_changed_setup_database_rejects_old_preview(self):
        token, _preview = self._inspect()
        with open(self.path, "a") as stream:
            stream.write('\n')
        with mock.patch.object(hierarchy, "cell_scope_records", return_value=self.scopes, create=True), \
                mock.patch("crescendo_engine.server.prepare_setup") as prepare:
            with self.assertRaisesRegex(ValueError, "changed since"):
                self.service.prepare_cells(token, ["scope-I1"])
            prepare.assert_not_called()

    def test_changed_active_state_rejects_old_preview(self):
        token, _preview = self._inspect()
        with open(os.path.join(os.path.dirname(self.path), "active.state"), "w") as stream:
            stream.write('new state')
        with mock.patch.object(hierarchy, "cell_scope_records", return_value=self.scopes, create=True), \
                mock.patch("crescendo_engine.server.prepare_setup") as prepare:
            with self.assertRaisesRegex(ValueError, "changed since"):
                self.service.prepare_cells(token, ["scope-I1"])
            prepare.assert_not_called()

    def test_new_preview_invalidates_previous_token_and_public_mutation_is_isolated(self):
        old, _old_preview = self._inspect()
        token, preview = self._inspect()
        preview["cell_scopes"][0]["hierarchy_path"] = "/wrong"
        self.assertNotIn(old, self.service.inspections)
        self.assertEqual(self.service.inspections[token]["preview"]["cell_scopes"][0]["hierarchy_path"], "/I1")

    def test_changed_cell_hierarchy_does_not_leave_runnable_prepared(self):
        token, _preview = self._inspect()
        with mock.patch.object(hierarchy, "cell_scope_records", return_value=[], create=True), \
                mock.patch("crescendo_engine.server.prepare_setup", return_value=self._prepared()):
            with self.assertRaisesRegex(ValueError, "hierarchy changed"):
                self.service.prepare_cells(token, ["scope-I1"])
        self.assertEqual(self.service.prepared, {})

    def test_replaced_master_at_same_occurrence_rejects_old_choice(self):
        token, _preview = self._inspect()
        observed = copy.deepcopy(self.scopes)
        observed[0]["cell"] = "different-master"
        with mock.patch.object(hierarchy, "cell_scope_records", return_value=observed, create=True), \
                mock.patch("crescendo_engine.server.prepare_setup", return_value=self._prepared()):
            with self.assertRaisesRegex(ValueError, "hierarchy changed"):
                self.service.prepare_cells(token, ["scope-I1"])
        self.assertEqual(self.service.prepared, {})

    def test_setup_change_during_apply_removes_the_new_runnable_registration(self):
        token, _preview = self._inspect()

        def changed_setup(*_args, **_kwargs):
            prepared = self._prepared()
            with open(self.path, "a") as stream:
                stream.write('\n')
            return prepared

        with mock.patch.object(hierarchy, "cell_scope_records", return_value=self.scopes, create=True), \
                mock.patch("crescendo_engine.server.prepare_setup", side_effect=changed_setup):
            with self.assertRaisesRegex(ValueError, "changed (?:during|before reference capture)"):
                self.service.prepare_cells(token, ["scope-I1"])
        self.assertEqual(self.service.prepared, {})

    def test_cancelled_inspection_does_not_publish_preview(self):
        def cancelled_probe(*_args, **_kwargs):
            self.service.prepare_cancel_event.set()
            return self.hierarchy
        with mock.patch.object(hierarchy, "probe_hierarchy", side_effect=cancelled_probe), \
                mock.patch.object(hierarchy, "cell_scope_records", return_value=self.scopes, create=True):
            with self.assertRaises(CommandCancelled):
                self.service.inspect_cells(self.path)
        self.assertEqual(self.service.inspections, {})
        self.assertEqual(self.service.lifecycle_operations, 0)

    def test_selection_and_eda_change_invalidate_preview_tokens(self):
        self._inspect()
        proposed = copy.deepcopy(self.config)
        proposed["_selected_setup_path"] = self.path
        with mock.patch("crescendo_engine.maestro_context.selected_setup_config", return_value=proposed), \
                mock.patch.object(self.service, "_cancel_and_wait_range_validations"), \
                mock.patch.object(self.service, "_close_resident_validation"):
            self.service.select_setup(self.path)
        self.assertEqual(self.service.inspections, {})
        self._inspect()
        with mock.patch("crescendo_engine.server.configure_eda", return_value=({"effective": {}, "ok": True}, "/unused")), \
                mock.patch("crescendo_engine.server.persist_eda_setup", return_value="/unused"), \
                mock.patch.object(self.service, "_cancel_and_wait_range_validations"), \
                mock.patch.object(self.service, "_close_resident_validation"):
            self.service.update_eda_setup({"mode": "auto"})
        self.assertEqual(self.service.inspections, {})

    def test_new_apply_retires_only_same_inspection_prepared_ids(self):
        token, _preview = self._inspect()
        self.service.prepared["legacy-full"] = {"legacy": True}
        with mock.patch.object(hierarchy, "cell_scope_records", return_value=self.scopes, create=True), \
                mock.patch("crescendo_engine.server.prepare_setup", side_effect=lambda *_a, **_k: self._prepared()):
            first, _prepared = self.service.prepare_cells(token, ["scope-I1"])
            second, _prepared = self.service.prepare_cells(token, ["scope-I10"])
        self.assertNotIn(first, self.service.prepared)
        self.assertIn(second, self.service.prepared)
        self.assertIn("legacy-full", self.service.prepared)

    def test_failed_apply_keeps_previously_valid_registration(self):
        token, _preview = self._inspect()
        with mock.patch.object(hierarchy, "cell_scope_records", return_value=self.scopes, create=True), \
                mock.patch("crescendo_engine.server.prepare_setup", side_effect=lambda *_a, **_k: self._prepared()):
            first, _prepared = self.service.prepare_cells(token, ["scope-I1"])
        observed = copy.deepcopy(self.scopes)
        observed[0]["cell"] = "replaced-master"
        with mock.patch.object(hierarchy, "cell_scope_records", return_value=observed, create=True), \
                mock.patch("crescendo_engine.server.prepare_setup", side_effect=lambda *_a, **_k: self._prepared()):
            with self.assertRaisesRegex(ValueError, "hierarchy changed"):
                self.service.prepare_cells(token, ["scope-I1"])
        self.assertEqual(list(self.service.prepared), [first])

    def _scoped_checkpoint(self):
        prepared = self._prepared()
        prepared["parameters"] = [{"parameter_id": "x", "optimizable": True}]
        prepared["outputs"] = [{"key": "mock::cost"}]
        prepared["cell_selection"] = {"scope_ids": ["scope-I1"], "scopes": [self.scopes[0]]}
        spec = {"evaluator": "mock", "variables": [{"parameter_id": "x", "minimum": 0, "maximum": 1}],
                "objectives": [{"output_key": "mock::cost"}], "constraints": []}
        generator = PopulationGenerator(spec["variables"])
        checkpoint = runner._checkpoint_document(spec, prepared, generator, [], [], 0, None, {})
        return json.loads(json.dumps(checkpoint)), prepared

    def test_scoped_checkpoint_persists_choices_and_retains_checksum_guard(self):
        checkpoint, prepared = self._scoped_checkpoint()
        self.assertEqual(checkpoint["manifest"]["cell_selection"], prepared["cell_selection"])
        self.assertEqual(checkpoint["spec"]["cell_selection"], prepared["cell_selection"])
        runner.validate_checkpoint(checkpoint, prepared)
        tampered = copy.deepcopy(checkpoint)
        tampered["manifest"]["cell_selection"]["scopes"][0]["hierarchy_path"] = "/I10"
        with self.assertRaisesRegex(ValueError, "checksum"):
            runner.validate_checkpoint_integrity(tampered)
        full = copy.deepcopy(prepared)
        full.pop("cell_selection")
        with self.assertRaisesRegex(ValueError, "selected cell"):
            runner.validate_checkpoint(checkpoint, full)

    def test_empty_persisted_scopes_cannot_fall_back_to_full_prepare(self):
        checkpoint, _prepared = self._scoped_checkpoint()
        checkpoint["manifest"]["cell_selection"]["scopes"] = []
        checkpoint["payload_sha256"] = runner._checkpoint_checksum(checkpoint)
        with self.assertRaisesRegex(ValueError, "nonempty scopes"):
            runner.validate_checkpoint_integrity(checkpoint)

    def test_service_resume_replays_scoped_prepare_before_original_guards(self):
        checkpoint, prepared = self._scoped_checkpoint()

        def launched(*_args, **kwargs):
            kwargs["continuation_lease"].release()
            return {"run_id": "resumed-scoped"}

        with mock.patch.object(self.service, "prepare", return_value=("prepared-scope", prepared)) as prepare, \
                mock.patch.object(self.service, "_cleanup_checkpoint_origin", return_value={"state": "skipped"}), \
                mock.patch.object(self.service, "_launch", side_effect=launched):
            result = self.service.resume(checkpoint)
        self.assertEqual(result["run_id"], "resumed-scoped")
        prepare.assert_called_once_with(self.path, backend="maestro", initialize_validation=False,
                                        selected_cell_paths=prepared["cell_selection"]["scopes"])

    def test_cli_without_prepared_replays_persisted_scopes(self):
        checkpoint, prepared = self._scoped_checkpoint()
        spec = copy.deepcopy(checkpoint["spec"])
        spec["setup_path"] = self.path
        with mock.patch("crescendo_engine.runner.prepare_setup", side_effect=RuntimeError("prepare sentinel")) as prepare:
            with self.assertRaisesRegex(RuntimeError, "prepare sentinel"):
                runner._run_optimization_unleased(spec, self.config, run_root=os.path.join(self.root, "cli"))
        self.assertEqual(prepare.call_args[1]["selected_cell_paths"], prepared["cell_selection"]["scopes"])

    def test_runner_scoped_prepare_still_calls_complete_identity_and_pdk_boundaries(self):
        setup = inspect_setup(self.path)
        selected = [self.scopes[0]]
        for scopes in (None, selected):
            with self.subTest(scopes=scopes), \
                    mock.patch("crescendo_engine.runner.inspect_setup", return_value=setup), \
                    mock.patch("crescendo_engine.runner.probe_hierarchy", return_value=self.hierarchy) as probe, \
                    mock.patch("crescendo_engine.runner.cell_scope_records", return_value=self.scopes), \
                    mock.patch("crescendo_engine.runner.detect_pdk_context", return_value={}) as pdk, \
                    mock.patch("crescendo_engine.runner._simulation_submission_metadata", return_value={}), \
                    mock.patch("crescendo_engine.runner.collect_external_eda_environment", return_value={"identity": {}, "diagnostics": {}}) as external, \
                    mock.patch("crescendo_engine.runner.source_design_identity", return_value={"fingerprint": "full"}) as source:
                runner.prepare_setup(self.path, self.config, selected_cell_paths=scopes)
            expected = {"max_depth": 32}
            if scopes is not None:
                expected["selected_cell_paths"] = selected
            self.assertEqual(probe.call_args[1], expected)
            from crescendo_engine.single_test import project_setup
            selected_setup = project_setup(setup, 'T')
            pdk.assert_called_once_with(selected_setup, self.hierarchy, self.config["cadence"])
            external.assert_called_once()
            source.assert_called_once_with(selected_setup, self.hierarchy, config=self.config)

    def test_runner_reference_scope_is_rejected_before_external_identity_scan(self):
        with mock.patch("crescendo_engine.runner.inspect_setup", return_value=inspect_setup(self.path)), \
                mock.patch("crescendo_engine.runner.probe_hierarchy", return_value=self.hierarchy), \
                mock.patch("crescendo_engine.runner.cell_scope_records", return_value=[dict(self.scopes[0], selectable=False)]), \
                mock.patch("crescendo_engine.runner.collect_external_eda_environment") as external:
            with self.assertRaisesRegex(ValueError, "reference library"):
                runner.prepare_setup(self.path, self.config, selected_cell_paths=[self.scopes[0]])
        external.assert_not_called()


if __name__ == "__main__":
    unittest.main()
