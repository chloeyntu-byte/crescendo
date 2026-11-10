import copy
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine.ade_setup import inspect_setup
from crescendo_engine.environment_identity import _ade_setup_references
from crescendo_engine.prepared_freshness import capture_prepared_freshness, require_prepared_freshness
from crescendo_engine.setup_baseline import (effective_setup_path, freeze_setup_baseline,
    load_bound_setup, load_prepared_baseline, persist_prepared_baseline, verify_setup_baseline)
from crescendo_engine.workspace import (source_design_identity, build_workspace,
    verify_sources_unchanged, workspace_test_design_lines)
from crescendo_engine.server import OptimizerService


class SetupBaselineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = self.temp.name
        self.library = os.path.join(self.root, "design")
        self.view = os.path.join(self.library, "TB", "maestro")
        os.makedirs(self.view)
        self.path = os.path.join(self.view, "maestro.sdb")
        self.xml = ('<setupdb><active><vars><var Name="FN_gm"><value>3</value></var></vars>'
            '<tests><test Name="C"><tooloptions><option Name="lib"><value>design</value></option>'
            '<option Name="cell"><value>TB</value></option><option Name="view"><value>schematic</value></option>'
            '</tooloptions><vars><var Name="local"><value>FN_gm*2</value></var></vars>'
            '<modelfiles><modelfile>../kit/toplevel.scs</modelfile></modelfiles></test></tests></active></setupdb>')
        self.write(self.path, self.xml)
        self.state = os.path.join(self.view, "active.state")
        self.write(self.state, '<state/>')
        self.schematic = os.path.join(self.library, "TB", "schematic", "sch.oa")
        os.makedirs(os.path.dirname(self.schematic))
        self.write(self.schematic, "original electrical view")
        self.cds = os.path.join(self.root, "cds.lib")
        self.write(self.cds, 'DEFINE design "%s"\n' % self.library)
        self.config = {"cadence": {"cds_lib": self.cds},
                       "runtime": {"run_root": os.path.join(self.root, "runtime")}}
        self.hierarchy = {"designs": [{"design": {"library": "design", "cell": "TB", "view": "schematic"},
            "hierarchy": {"cells": [{"library": "design", "cell": "TB", "view": "schematic", "hierarchy_path": "/"}]}}]}
        self.prepared = {"setup": inspect_setup(self.path), "hierarchy": self.hierarchy,
                         "setup_locator": {"path": self.path, "backend": "maestro"},
                         "parameters": [], "outputs": [],
                         "job_policy": {"available": True, "global": {"properties": {"maxjobs": "8"}}}}
        self.prepared["source_identity"] = source_design_identity(self.prepared["setup"], self.hierarchy, self.config)

    def write(self, path, text):
        with open(path, "w") as stream:
            stream.write(text)

    def freeze(self):
        baseline = freeze_setup_baseline(self.prepared, self.config)
        self.prepared["source_identity"] = source_design_identity(self.prepared["setup"], self.hierarchy, self.config)
        self.prepared["prepared_freshness"] = capture_prepared_freshness(self.prepared)
        self.config["_setup_baseline"] = {"origin_setup_path": self.path, "baseline": baseline}
        return baseline

    def test_original_setup_saves_do_not_change_applied_values_or_source_guard(self):
        baseline = self.freeze()
        before = self.prepared["source_identity"]
        self.write(self.path, self.xml.replace('<value>3</value>', '<value>9</value>'))
        self.write(self.state, '<state><new>saved</new></state>')
        loaded = load_bound_setup(self.path, self.config)
        self.assertEqual(loaded["global_variables"][0]["value"], "3")
        self.assertEqual(loaded["tests"][0]["variables"][0]["value"], "FN_gm*2")
        self.assertEqual(loaded["source"]["setup_db"], self.path)
        self.assertEqual(effective_setup_path(loaded), baseline["setup_path"])
        self.assertEqual(source_design_identity(loaded, self.hierarchy, self.config), before)
        self.assertTrue(require_prepared_freshness(self.prepared, self.config)["ok"])
        # Explicit Load drops only the binding and creates a different baseline.
        config = copy.deepcopy(self.config)
        config.pop("_setup_baseline")
        fresh = load_bound_setup(self.path, config)
        self.assertEqual(fresh["global_variables"][0]["value"], "9")

    def test_live_schematic_in_same_tb_cell_is_still_guarded(self):
        self.freeze()
        self.write(self.schematic, "changed electrical view")
        with self.assertRaisesRegex(ValueError, "source dependencies changed in library design"):
            require_prepared_freshness(self.prepared, self.config)

    def test_snapshot_tampering_and_linked_source_are_rejected(self):
        baseline = self.freeze()
        self.write(baseline["setup_path"], self.xml + "changed")
        with self.assertRaisesRegex(ValueError, "baseline contents changed"):
            verify_setup_baseline(self.prepared["setup"])
        fresh = copy.deepcopy(self.prepared)
        fresh["setup"].pop("baseline")
        os.link(self.path, os.path.join(self.view, "linked.sdb"))
        with self.assertRaisesRegex(ValueError, "link or special"):
            freeze_setup_baseline(fresh, self.config)

    def test_resume_snapshot_retains_same_baseline_without_reading_original(self):
        baseline = self.freeze()
        record = persist_prepared_baseline(self.prepared)
        self.write(self.path, self.xml.replace('<value>3</value>', '<value>9</value>'))
        with mock.patch("crescendo_engine.ade_setup.inspect_setup", side_effect=AssertionError("must not re-inspect original")):
            loaded = load_prepared_baseline(record)
            require_prepared_freshness(loaded, self.config, action="resume")
        self.assertEqual(loaded["setup"]["baseline"]["baseline_id"], baseline["baseline_id"])
        self.write(os.path.join(baseline["root"], "setup.json"), "{}")
        with self.assertRaisesRegex(ValueError, "settings snapshot changed"):
            load_prepared_baseline(record)

    def test_model_reference_scan_uses_baseline_but_original_relative_base(self):
        self.freeze()
        expected = _ade_setup_references(self.prepared["setup"])
        self.write(self.path, self.xml.replace("../kit/toplevel.scs", "/changed/model.scs"))
        self.assertEqual(_ade_setup_references(self.prepared["setup"]), expected)
        self.assertIn(("../kit/toplevel.scs", self.view), expected[0]["model"])

    def test_native_locator_defaults_keep_original_library_cell_and_view(self):
        self.xml = self.xml.replace('<option Name="lib"><value>design</value></option>', '').replace(
            '<option Name="cell"><value>TB</value></option>', '')
        self.write(self.path, self.xml)
        self.prepared["setup"] = inspect_setup(self.path)
        expected = copy.deepcopy(self.prepared["setup"]["tests"])
        self.freeze()
        loaded = load_bound_setup(self.path, self.config)
        self.assertEqual(loaded["tests"], expected)

    def test_symlinked_snapshot_directory_cannot_replace_owned_payload(self):
        baseline = self.freeze()
        directory = os.path.dirname(baseline["setup_path"])
        moved = os.path.join(self.root, "moved-baseline")
        os.rename(directory, moved)
        os.symlink(moved, directory)
        with self.assertRaisesRegex(ValueError, "real owned directory"):
            verify_setup_baseline(self.prepared["setup"])

    def test_existing_source_library_alias_is_materialized_without_snapshot_links(self):
        alias = os.path.join(self.root, "library-alias")
        os.symlink(self.library, alias)
        original = os.path.join(alias, "TB", "maestro", "maestro.sdb")
        self.prepared["setup"] = inspect_setup(original)
        baseline = freeze_setup_baseline(self.prepared, self.config)
        self.assertEqual(os.path.realpath(baseline["setup_path"]), baseline["setup_path"])
        self.assertEqual(self.prepared["setup"]["source"]["setup_db"], original)
        self.assertEqual(verify_setup_baseline(self.prepared["setup"])["baseline_id"], baseline["baseline_id"])

    def test_checkpoint_document_carries_exact_prepared_baseline_record(self):
        import random
        from crescendo_engine.runner import _checkpoint_document
        self.freeze()
        record = persist_prepared_baseline(self.prepared)
        class Generator(object):
            rng = random.Random(0)
            population_size = 1
            previous = []
            _de_targets = {}
        document = _checkpoint_document({"variables": [], "objectives": [], "constraints": []},
            self.prepared, Generator(), [], [], 0, None, {"run_id": "fixture", "run_root": self.root})
        self.assertEqual(document["manifest"]["setup_baseline_snapshot"], record)
        self.assertEqual(document["manifest"]["source_design_identity"], self.prepared["source_identity"])

    def test_stop_during_native_setup_copy_never_publishes_partial_baseline(self):
        from crescendo_engine.cadence import CommandCancelled
        from crescendo_engine.workspace import _copy_private_file
        copied = []
        def stop_after_copy(source, destination):
            _copy_private_file(source, destination)
            copied.append(destination)
            raise CommandCancelled("fixture Stop while sealing Maestro")
        with mock.patch("crescendo_engine.workspace._copy_private_file", side_effect=stop_after_copy):
            with self.assertRaisesRegex(CommandCancelled, "fixture Stop"):
                freeze_setup_baseline(self.prepared, self.config)
        self.assertTrue(copied)
        self.assertNotIn("baseline", self.prepared["setup"])
        self.assertEqual(os.listdir(os.path.join(self.config["runtime"]["run_root"], "setup_baselines")), [])
        with open(self.path) as stream:
            self.assertEqual(stream.read(), self.xml)

    def test_save_during_seal_is_rejected_without_publishing_mixed_settings(self):
        from crescendo_engine.workspace import _copy_private_file
        def save_during_copy(source, destination):
            result = _copy_private_file(source, destination)
            self.write(self.path, self.xml.replace('<value>3</value>', '<value>9</value>'))
            return result
        with mock.patch("crescendo_engine.workspace._copy_private_file", side_effect=save_during_copy):
            with self.assertRaisesRegex(ValueError, "changed during baseline capture"):
                freeze_setup_baseline(self.prepared, self.config)
        self.assertNotIn("baseline", self.prepared["setup"])
        self.assertEqual(os.listdir(os.path.join(self.config["runtime"]["run_root"], "setup_baselines")), [])

    def test_shared_test_binding_readback_has_no_config_write(self):
        workspace = {"setup": {"tests": [{"name": "C", "library": "design", "cell": "TB", "view": "config"}]},
            "private_design_identities": [{"source": ["design", "TB", "config"], "target": ["design", "TB", "cr_private"]}]}
        readonly = "\n".join(workspace_test_design_lines(workspace, read_only=True))
        self.assertNotIn("axlSetTestToolArgs", readonly)
        self.assertIn("cr_private", readonly)
        self.assertIn("Crescendo private test design readback mismatch", readonly)
        self.assertIn("axlSetTestToolArgs", "\n".join(workspace_test_design_lines(workspace)))

    def test_workspace_setup_copy_uses_baseline_after_original_save(self):
        self.freeze()
        self.write(self.path, self.xml.replace('<value>3</value>', '<value>9</value>'))
        for strategy, parameters in (("same_library_views", []), ("source_reference", None)):
            with self.subTest(strategy=strategy):
                config = copy.deepcopy(self.config)
                config["cadence"]["workspace_copy_strategy"] = strategy
                root = os.path.join(config["runtime"]["run_root"], strategy)
                identity = source_design_identity(self.prepared["setup"], self.hierarchy, config)
                with mock.patch("crescendo_engine.workspace.detect_pdk_context", return_value={}), mock.patch(
                        "crescendo_engine.workspace.remap_workspace_setup_paths", return_value={"changed_files": []}):
                    workspace = build_workspace(self.prepared["setup"], self.hierarchy, config, root,
                                                expected_source_identity=identity, parameters=parameters)
                with open(workspace["setup_sdb"]) as stream:
                    self.assertIn('<value>3</value>', stream.read())
                self.assertTrue(verify_sources_unchanged(workspace)["ok"])
                self.write(self.schematic, "electrical change")
                self.assertFalse(verify_sources_unchanged(workspace)["ok"])
                self.write(self.schematic, "original electrical view")

    def test_service_prepare_rechecks_same_baseline_after_environment_invalidation(self):
        service = OptimizerService(copy.deepcopy(self.config))
        def catalog(path, config, **options):
            setup = load_bound_setup(path, config)
            return {"setup": setup, "hierarchy": copy.deepcopy(self.hierarchy),
                    "parameters": [], "outputs": [], "job_policy": copy.deepcopy(self.prepared["job_policy"]),
                    "source_identity": source_design_identity(setup, self.hierarchy, config)}
        with mock.patch("crescendo_engine.server.prepare_setup", side_effect=catalog), mock.patch.object(
                service, "eda_setup", return_value={}), mock.patch("crescendo_engine.server.detect_pdk_context", return_value={}):
            first_id, first = service.prepare(self.path, initialize_validation=False)
            initial = first["setup"]["baseline"]["baseline_id"]
            # This is the same invalidation used by environment-only Override.
            service.prepared.clear()
            self.write(self.path, self.xml.replace('<value>3</value>', '<value>9</value>'))
            second_id, second = service.prepare(self.path, initialize_validation=False)
            service.select_setup(self.path)
            self.assertNotIn("_setup_baseline", service.config)
            third_id, third = service.prepare(self.path, initialize_validation=False)
        self.assertNotEqual(first_id, second_id)
        self.assertEqual(second["setup"]["baseline"]["baseline_id"], initial)
        self.assertEqual(second["setup"]["global_variables"][0]["value"], "3")
        self.assertNotEqual(third["setup"]["baseline"]["baseline_id"], initial)
        self.assertEqual(third["setup"]["global_variables"][0]["value"], "9")
        self.assertEqual(service._inspection_setup_identity(first["setup"]),
                         service._inspection_setup_identity(second["setup"]))

    def test_service_resume_uses_checkpoint_baseline_and_rejects_live_oa_drift(self):
        from crescendo_engine.runner import _checkpoint_checksum
        from tests.test_resume_preflight_hardening import _mock_checkpoint
        self.freeze()
        record = persist_prepared_baseline(self.prepared)
        checkpoint = _mock_checkpoint(self.root)
        checkpoint["manifest"].update(maestro_path=self.path, setup_baseline_snapshot=record)
        checkpoint["payload_sha256"] = _checkpoint_checksum(checkpoint)
        self.write(self.path, self.xml.replace('<value>3</value>', '<value>9</value>'))
        service = OptimizerService(copy.deepcopy(self.config))
        with mock.patch.object(service, "prepare", side_effect=AssertionError("must not prepare current original")) as prepare, \
                mock.patch.object(service, "_cleanup_checkpoint_origin", return_value={"state": "skipped"}), \
                mock.patch("crescendo_engine.server._reconcile_continuation_anchor", return_value={"state": "absent"}), \
                mock.patch("crescendo_engine.server.startup_storage_preflight", return_value={"ok": True}), \
                mock.patch.object(service, "_launch", return_value={"run_id": "resumed"}) as launch:
            result = service.resume(checkpoint)
            prepare.assert_not_called()
            self.assertEqual(result["prepared"]["setup"]["global_variables"][0]["value"], "3")
            self.assertEqual(launch.call_count, 1)
            launch.call_args[1]["continuation_lease"].release()
            self.write(self.schematic, "changed while stopped")
            with self.assertRaisesRegex(ValueError, "source dependencies changed in library design"):
                service.resume(checkpoint)
            self.assertEqual(launch.call_count, 1)

    def test_resume_keeps_shared_views_until_all_old_solver_ownership_is_stopped(self):
        origin = os.path.join(self.config["runtime"]["run_root"], "origin")
        os.makedirs(origin)
        checkpoint = {"manifest": {"origin_run_root": origin, "origin_run_id": "origin"},
                      "spec": {"execution_backend": "maestro_netlist", "evaluator": "real"}}
        service = OptimizerService(copy.deepcopy(self.config))
        order = []
        def stopped(*args, **options):
            order.append("solver_ownership")
            return {"cleanup_proven": True}
        def native(*args, **options):
            self.assertTrue(options["solver_cleanup_proven"])
            order.append("native_preparation")
            return {"cleanup_proven": True}
        with mock.patch("crescendo_engine.netlist_execution.reconcile_previous_ownership", return_value={"cleanup_proven": False}), \
                mock.patch("crescendo_engine.netlist_run_ownership.reconcile_run_preparations") as preparation:
            with self.assertRaisesRegex(RuntimeError, "jobs are not proven stopped"):
                service._cleanup_checkpoint_origin(checkpoint, terminate_owned=True, inspect_only=True)
            preparation.assert_not_called()
        with mock.patch("crescendo_engine.netlist_execution.reconcile_previous_ownership", side_effect=stopped), \
                mock.patch("crescendo_engine.netlist_run_ownership.reconcile_run_preparations", side_effect=native):
            result = service._cleanup_checkpoint_origin(checkpoint, terminate_owned=True, inspect_only=True)
        self.assertEqual(order, ["solver_ownership", "native_preparation"])


if __name__ == "__main__":
    unittest.main()
