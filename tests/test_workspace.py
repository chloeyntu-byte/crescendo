from __future__ import print_function

import json
import os
import re
import shutil
import stat
import tempfile
import unittest
from unittest import mock

from crescendo_engine.workspace import (
    _physical_values_match,
    ParameterApplicationFailure, ParameterApplicationInfrastructureFailure, WorkspaceBuildFailure,
    apply_candidate, build_occurrence_clone_plan,
    build_workspace, parse_cds_defines, render_physical_value, resolve_copy_libraries,
    run_maestro, source_design_identity, summarize_spectre_timing,
    tree_content_fingerprint, tree_fingerprint,
    merge_parameter_verification,
    validate_parameter_snapshot, validate_schematic_application,
    verify_parameter_application, verify_sources_unchanged, write_batch_files,
    write_combined_cadence_script, write_parameter_snapshot, write_schematic_patch,
    bounded_skill_body_lines,
)


class WorkspaceTest(unittest.TestCase):
    def test_batch_script_explicitly_loads_private_workspace_startup(self):
        with tempfile.TemporaryDirectory() as root:
            workspace = {
                "root": root,
                "primary_library": "DESIGN",
                "setup_cell": "TB",
            }
            config = {
                "cadence": {"spectre_bin": "/eda/spectre"},
                "runtime": {"spectre_submit_script": "/submit/spectre"},
            }
            generated = write_batch_files(workspace, config, history_name="CRESCENDO_test")
            with open(generated["ocean_script"], "r") as stream:
                script = stream.read()
            self.assertIn("crescWorkspaceStartupLoaded", script)
            self.assertIn('load("%s")' % os.path.join(root, ".cdsinit"), script)
            self.assertLess(script.index("crescWorkspaceStartupLoaded"), script.index("maeOpenSetup"))

    def test_legacy_maestro_writes_guardian_manifest_before_guarded_launch(self):
        with tempfile.TemporaryDirectory() as root:
            workspace_root = os.path.join(root, "workspace")
            candidate_root = os.path.join(root, "candidate", "attempts", "attempt_1")
            os.makedirs(workspace_root)
            workspace = {
                "root": workspace_root,
                "setup_dir": workspace_root,
                "simulation_root": os.path.join(workspace_root, "simulation"),
                "setup": {"tests": []},
            }
            os.makedirs(workspace["simulation_root"])
            spectre_wrapper = os.path.join(workspace_root, "bin", "spectre")
            ocean_script = os.path.join(workspace_root, "run_maestro.ocn")
            os.makedirs(os.path.dirname(spectre_wrapper))
            for path in (spectre_wrapper, ocean_script):
                with open(path, "w") as stream:
                    stream.write("test\n")
            config = {
                "cadence": {
                    "ocean_bin": "/eda/ocean", "spectre_bin": "/eda/spectre",
                    "cadence_root": "/eda", "license_file": None,
                    "extra_binary_paths": [], "extra_library_paths": [],
                },
                "runtime": {
                    "candidate_timeout_seconds": 60, "simulation_timeout_seconds": 30,
                    "ocean_startup_timeout_seconds": 10, "command_timeout_seconds": 60,
                    "spectre_submit_script": "/submit/spectre",
                },
            }
            command_result = {
                "returncode": 0, "timed_out": False, "startup_timed_out": False,
                "termination_failed": False, "lock_wait_seconds": 0.0,
                "elapsed_seconds": 0.1,
            }
            reused_descriptor = []

            def launch_with_transferred_descriptor(*_args, **kwargs):
                descriptor = kwargs["guardian_launch_lock_fd"]
                os.close(descriptor)
                probe = os.open(os.path.join(root, "fd-reuse-probe"), os.O_CREAT | os.O_RDWR, 0o600)
                reused_descriptor.append((descriptor, probe))
                return dict(command_result)

            with mock.patch("crescendo_engine.workspace.write_batch_files", return_value={
                "spectre_wrapper": spectre_wrapper, "ocean_script": ocean_script, "execution_simulator": "spectre",
            }), mock.patch(
                "crescendo_engine.workspace.run_command",
                side_effect=launch_with_transferred_descriptor,
            ) as launch:
                retained_log_root = os.path.join(root, "candidate")
                result = run_maestro(
                    workspace, config, candidate_root,
                    retained_log_root=retained_log_root,
                )
            manifest_path = os.path.join(candidate_root, "legacy_guardian_launch.json")
            with open(manifest_path) as stream:
                manifest = json.load(stream)
            self.assertEqual(manifest["schema_version"], "crescendo.legacy_maestro_guardian.v1")
            self.assertEqual(manifest["workspace_root"], os.path.realpath(workspace_root))
            self.assertEqual(manifest["state_path"], os.path.join(candidate_root, "guardian_state.json"))
            self.assertTrue(manifest["protocol"]["manifest_before_guardian_popen"])
            self.assertTrue(manifest["protocol"]["expected_parent_death_guard"])
            self.assertTrue(manifest["protocol"]["durable_launch_liveness_lock"])
            self.assertTrue(manifest["protocol"]["guardian_state_before_launch_lock_release"])
            self.assertEqual(manifest["expected_parent_pid"], os.getpid())
            self.assertIsInstance(manifest["expected_parent_start_ticks"], int)
            self.assertEqual(manifest["expected_uid"], os.getuid())
            self.assertEqual(manifest["guardian_argv"][-len(manifest["argv"]):], manifest["argv"])
            positional, keywords = launch.call_args
            self.assertEqual(positional[0], manifest["argv"])
            self.assertEqual(keywords["guardian_state_path"], manifest["state_path"])
            self.assertEqual(keywords["guardian_token"], manifest["token"])
            self.assertEqual(keywords["guardian_launch_lock_fd"], reused_descriptor[0][0])
            self.assertEqual(
                keywords["env"]["CRESCENDO_SPECTRE_RETAIN_ROOT"],
                retained_log_root,
            )
            self.assertEqual(reused_descriptor[0][0], reused_descriptor[0][1])
            os.fstat(reused_descriptor[0][1])
            os.close(reused_descriptor[0][1])
            self.assertEqual(result["guardian_manifest_path"], manifest_path)

    def test_spectre_wrapper_delegates_to_portable_submit_script(self):
        with tempfile.TemporaryDirectory() as root:
            workspace = {"root": root, "primary_library": "D", "setup_cell": "SETUP"}
            submit_script = os.path.join(root, "submit job.sh")
            config = {
                "cadence": {"spectre_bin": "/eda/spectre"},
                "runtime": {"spectre_submit_script": submit_script},
            }
            result = write_batch_files(workspace, config)
            with open(result["spectre_wrapper"]) as stream:
                wrapper = stream.read()
            self.assertIn('exec "$submit_script" "$real_spectre" "$@"', wrapper)
            self.assertIn(submit_script, wrapper)
            self.assertNotIn("/usr/bin/timeout", wrapper)

    def test_callback_validation_mode_reports_pdk_adjustment_without_weakening_normal_apply(self):
        with tempfile.TemporaryDirectory() as root:
            records = os.path.join(root, "records")
            with open(records, "w") as stream:
                stream.write('PARAM\tp1\t"20u"\t"8u"\t"8u"\t"ok"\t"callback"\n')
            plan = {
                "verification_path": records, "clones": [],
                "parameters": [{"parameter_id": "p1"}],
            }
            with self.assertRaisesRegex(ParameterApplicationFailure, "schematic property mismatch"):
                validate_schematic_application(plan)
            result = validate_schematic_application(plan, allow_effective_adjustment=True)
            self.assertEqual(result["parameters"][0]["requested"], "20u")
            self.assertEqual(result["parameters"][0]["cdf_value"], "8u")

    def _workspace_build_fixture(self, root):
        source = os.path.join(root, "source", "D")
        os.makedirs(os.path.join(source, "TOP", "maestro"))
        source_file = os.path.join(source, "TOP", "design.txt")
        with open(source_file, "w") as stream:
            stream.write("original")
        run_root = os.path.join(root, "runs", "run_1")
        workspace = os.path.join(run_root, "candidates", "g000_c0000", "workspace")
        setup = {"source": {"library_root": source, "maestro_sdb": os.path.join(source, "TOP", "maestro", "data.sdb")}}
        config = {"runtime": {"run_root": run_root}, "cadence": {}}
        return source, source_file, workspace, setup, config

    def test_partial_workspace_is_removed_after_copy_failure(self):
        with tempfile.TemporaryDirectory() as root:
            source, source_file, workspace, setup, config = self._workspace_build_fixture(root)

            def fail_copy(_source, destination, **_kwargs):
                os.makedirs(destination)
                with open(os.path.join(destination, "partial.txt"), "w") as stream:
                    stream.write("partial")
                raise IOError("injected copy failure")

            with mock.patch("crescendo_engine.workspace.resolve_copy_libraries", return_value=("D", None, {"D": source})), \
                    mock.patch("crescendo_engine.workspace.shutil.copytree", side_effect=fail_copy):
                with self.assertRaises(WorkspaceBuildFailure) as raised:
                    build_workspace(setup, {}, config, workspace)
            self.assertTrue(raised.exception.cleanup["ok"])
            self.assertTrue(raised.exception.cleanup["removed"])
            self.assertFalse(os.path.lexists(workspace))
            with open(source_file) as stream:
                self.assertEqual(stream.read(), "original")

    def test_partial_workspace_is_retained_when_source_changes(self):
        with tempfile.TemporaryDirectory() as root:
            source, source_file, workspace, setup, config = self._workspace_build_fixture(root)

            def fail_after_source_change(_source, destination, **_kwargs):
                os.makedirs(destination)
                with open(os.path.join(destination, "partial.txt"), "w") as stream:
                    stream.write("partial")
                with open(source_file, "w") as stream:
                    stream.write("changed by injected failure")
                raise IOError("injected copy failure")

            with mock.patch("crescendo_engine.workspace.resolve_copy_libraries", return_value=("D", None, {"D": source})), \
                    mock.patch("crescendo_engine.workspace.shutil.copytree", side_effect=fail_after_source_change):
                with self.assertRaises(WorkspaceBuildFailure) as raised:
                    build_workspace(setup, {}, config, workspace)
            self.assertFalse(raised.exception.cleanup["ok"])
            self.assertFalse(raised.exception.cleanup["removed"])
            self.assertIn("source fingerprint changed", raised.exception.cleanup["cleanup_error"])
            self.assertTrue(os.path.isdir(workspace))

    def test_partial_workspace_detects_same_size_source_change_with_restored_mtime(self):
        with tempfile.TemporaryDirectory() as root:
            source, source_file, workspace, setup, config = self._workspace_build_fixture(root)
            original_stat = os.stat(source_file)

            def fail_after_source_change(_source, destination, **_kwargs):
                os.makedirs(destination)
                with open(os.path.join(destination, "partial.txt"), "w") as stream:
                    stream.write("partial")
                with open(source_file, "w") as stream:
                    stream.write("mutated!")
                os.utime(source_file, ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns))
                raise IOError("injected copy failure")

            with mock.patch(
                    "crescendo_engine.workspace.resolve_copy_libraries",
                    return_value=("D", None, {"D": source})), mock.patch(
                        "crescendo_engine.workspace.shutil.copytree",
                        side_effect=fail_after_source_change):
                with self.assertRaises(WorkspaceBuildFailure) as raised:
                    build_workspace(setup, {}, config, workspace)
            self.assertFalse(raised.exception.cleanup["ok"])
            self.assertFalse(raised.exception.cleanup["removed"])
            self.assertIn("source fingerprint changed", raised.exception.cleanup["cleanup_error"])
            self.assertTrue(os.path.isdir(workspace))

    def test_workspace_materializes_source_symlinks_into_private_files(self):
        with tempfile.TemporaryDirectory() as root:
            source, source_file, workspace, setup, config = self._workspace_build_fixture(root)
            shared = os.path.join(root, "shared")
            os.makedirs(os.path.join(shared, "CELL"))
            with open(os.path.join(shared, "CELL", "data.txt"), "w") as stream:
                stream.write("shared")
            os.symlink(source_file, os.path.join(source, "linked_file.txt"))
            os.symlink(os.path.join(shared, "CELL"), os.path.join(source, "LINKED_CELL"))
            with mock.patch(
                    "crescendo_engine.workspace.resolve_copy_libraries",
                    return_value=("D", None, {"D": source})):
                built = build_workspace(setup, {}, config, workspace)
            copied = built["copied_libraries"]["D"]
            self.assertFalse(os.path.islink(os.path.join(copied, "linked_file.txt")))
            self.assertFalse(os.path.islink(os.path.join(copied, "LINKED_CELL")))
            with open(os.path.join(copied, "LINKED_CELL", "data.txt"), "w") as stream:
                stream.write("private")
            with open(os.path.join(shared, "CELL", "data.txt")) as stream:
                self.assertEqual(stream.read(), "shared")
            self.assertEqual(len(built["materialized_symlinks"]["D"]), 2)

    def test_workspace_makes_read_only_source_copy_owner_writable(self):
        with tempfile.TemporaryDirectory() as root:
            source, source_file, workspace, setup, config = self._workspace_build_fixture(root)
            source_directories = [
                source,
                os.path.join(source, "TOP"),
                os.path.join(source, "TOP", "maestro"),
            ]
            os.chmod(source_file, 0o444)
            for directory in reversed(source_directories):
                os.chmod(directory, 0o555)
            try:
                with mock.patch(
                        "crescendo_engine.workspace.resolve_copy_libraries",
                        return_value=("D", None, {"D": source})):
                    built = build_workspace(setup, {}, config, workspace)
                copied = built["copied_libraries"]["D"]
                copied_file = os.path.join(copied, "TOP", "design.txt")
                self.assertTrue(os.stat(copied_file).st_mode & 0o200)
                self.assertTrue(os.stat(os.path.dirname(copied_file)).st_mode & 0o300)
                with open(copied_file, "w") as stream:
                    stream.write("private")
                with open(source_file) as stream:
                    self.assertEqual(stream.read(), "original")
                self.assertEqual(stat.S_IMODE(os.stat(source_file).st_mode), 0o444)
                self.assertEqual(stat.S_IMODE(os.stat(source_directories[-1]).st_mode), 0o555)
            finally:
                os.chmod(source_file, 0o644)
                for directory in source_directories:
                    os.chmod(directory, 0o755)

    def test_partial_workspace_cleanup_handles_read_only_copy(self):
        with tempfile.TemporaryDirectory() as root:
            source, _source_file, workspace, setup, config = self._workspace_build_fixture(root)

            def fail_with_read_only_copy(_source, destination, **_kwargs):
                os.makedirs(os.path.join(destination, "CELL"))
                path = os.path.join(destination, "CELL", "data.oa")
                with open(path, "w") as stream:
                    stream.write("partial")
                os.chmod(path, 0o444)
                os.chmod(os.path.dirname(path), 0o555)
                os.chmod(destination, 0o555)
                raise IOError("injected read-only copy failure")

            with mock.patch(
                    "crescendo_engine.workspace.resolve_copy_libraries",
                    return_value=("D", None, {"D": source})), mock.patch(
                        "crescendo_engine.workspace.shutil.copytree",
                        side_effect=fail_with_read_only_copy):
                with self.assertRaises(WorkspaceBuildFailure) as raised:
                    build_workspace(setup, {}, config, workspace)
            self.assertTrue(raised.exception.cleanup["ok"])
            self.assertTrue(raised.exception.cleanup["removed"])
            self.assertFalse(os.path.lexists(workspace))

    def test_workspace_rejects_symbolic_link_directory_cycle_before_cadence(self):
        with tempfile.TemporaryDirectory() as root:
            source, _source_file, workspace, setup, config = self._workspace_build_fixture(root)
            os.symlink(source, os.path.join(source, "TOP", "cycle"))
            with mock.patch(
                    "crescendo_engine.workspace.resolve_copy_libraries",
                    return_value=("D", None, {"D": source})):
                with self.assertRaises(WorkspaceBuildFailure) as raised:
                    build_workspace(setup, {}, config, workspace)
            self.assertIn("symbolic-link directory cycle", str(raised.exception.cause))
            self.assertTrue(raised.exception.cleanup["ok"])
            self.assertFalse(os.path.lexists(workspace))

    def test_workspace_rejects_dangling_source_symlink_before_cadence(self):
        with tempfile.TemporaryDirectory() as root:
            source, _source_file, workspace, setup, config = self._workspace_build_fixture(root)
            os.symlink(os.path.join(root, "missing"), os.path.join(source, "BROKEN"))
            with mock.patch(
                    "crescendo_engine.workspace.resolve_copy_libraries",
                    return_value=("D", None, {"D": source})):
                with self.assertRaises(WorkspaceBuildFailure) as raised:
                    build_workspace(setup, {}, config, workspace)
            self.assertIn("dangling symbolic link", str(raised.exception.cause))
            self.assertTrue(raised.exception.cleanup["ok"])
            self.assertFalse(os.path.lexists(workspace))

    def test_failed_whole_run_workspace_build_is_cleaned_under_worker_root(self):
        with tempfile.TemporaryDirectory() as root:
            source, _source_file, _workspace, setup, config = self._workspace_build_fixture(root)
            workspace = os.path.join(
                config["runtime"]["run_root"], "workers", "worker_000", "epoch_000", "workspace",
            )
            os.symlink(os.path.join(root, "missing"), os.path.join(source, "BROKEN"))
            with mock.patch(
                    "crescendo_engine.workspace.resolve_copy_libraries",
                    return_value=("D", None, {"D": source})):
                with self.assertRaises(WorkspaceBuildFailure) as raised:
                    build_workspace(setup, {}, config, workspace)
            self.assertTrue(raised.exception.cleanup["ok"])
            self.assertFalse(os.path.lexists(workspace))

    def test_apply_candidate_runs_clone_then_parameters_with_one_deadline(self):
        with tempfile.TemporaryDirectory() as root:
            library = os.path.join(root, "library")
            os.makedirs(library)
            baseline_plan = {"clones": [], "parameters": [{"parameter_id": "p1"}], "verification_path": "baseline.records"}
            clone_plan = {"clones": [{"occurrence_path": "/X1"}], "parameters": [], "verification_path": "clone.records"}
            parameter_plan = {"clones": [], "parameters": [{"parameter_id": "p1"}], "verification_path": "parameter.records"}
            post_plan = {"clones": [], "parameters": [{"parameter_id": "p1"}], "verification_path": "post.records"}

            def write_plan(_workspace, _parameters, _values, _skill, _records, _adapter,
                           emit_clones=True, emit_parameters=True, **_kwargs):
                return clone_plan if emit_clones and not emit_parameters else parameter_plan

            commands = [{"returncode": 0, "timed_out": False} for _unused in range(2)]
            workspace = {"root": root, "setup_dir": root, "copied_libraries": {"D": library}}
            config = {"cadence": {"virtuoso_bin": "virtuoso", "pdk_adapter": "generic-cdf"},
                      "runtime": {"command_timeout_seconds": 10}}
            with mock.patch("crescendo_engine.workspace.write_schematic_patch", side_effect=write_plan), \
                    mock.patch("crescendo_engine.workspace.write_parameter_snapshot", side_effect=[baseline_plan, post_plan]), \
                    mock.patch("crescendo_engine.workspace.run_command", side_effect=commands) as run_mock, \
                    mock.patch("crescendo_engine.workspace.validate_schematic_application", side_effect=[
                        {"ok": True, "clones": clone_plan["clones"], "parameters": []},
                        {"ok": True, "clones": [], "parameters": parameter_plan["parameters"]},
                    ]), mock.patch("crescendo_engine.workspace.validate_parameter_snapshot", side_effect=[
                        {"ok": True, "clones": [], "parameters": baseline_plan["parameters"]},
                        {"ok": True, "clones": [], "parameters": post_plan["parameters"]},
                    ]), mock.patch("crescendo_engine.workspace.merge_parameter_verification", return_value={
                        "ok": True, "clones": clone_plan["clones"], "parameters": parameter_plan["parameters"],
                    }), mock.patch("crescendo_engine.workspace.time.monotonic", side_effect=[100.0, 101.0, 103.0, 106.0, 108.0]):
                result = apply_candidate(workspace, config, [], {}, root, timeout_seconds=10)
            self.assertEqual([row["name"] for row in result["schematic_patch_phases"]], ["baseline", "apply_verify"])
            self.assertEqual(
                [row["name"] for row in result["schematic_patch_phases"][1]["logical_phases"]],
                ["clone", "parameters", "verify"],
            )
            self.assertEqual([call[1]["timeout_seconds"] for call in run_mock.call_args_list], [9, 7])
            self.assertEqual(result["cadence_timing"]["process_count"], 2)
            with open(os.path.join(root, "patch_schematic_apply_verify.il")) as stream:
                combined = stream.read()
            self.assertLess(combined.index("patch_schematic_clone.il"), combined.index("patch_schematic.il"))
            self.assertLess(combined.index("patch_schematic.il"), combined.index("snapshot_schematic_post.il"))
            self.assertEqual(result["schematic_verification"]["clones"], clone_plan["clones"])
            self.assertEqual(result["schematic_verification"]["parameters"], parameter_plan["parameters"])

    def test_apply_candidate_cancellation_stops_before_next_cadence_phase(self):
        with tempfile.TemporaryDirectory() as root:
            library = os.path.join(root, "library")
            os.makedirs(library)
            baseline_plan = {"clones": [], "parameters": [{"parameter_id": "p1"}], "verification_path": "baseline.records"}
            parameter_plan = {"clones": [], "parameters": [{"parameter_id": "p1"}], "verification_path": "parameter.records"}
            post_plan = {"clones": [], "parameters": [{"parameter_id": "p1"}], "verification_path": "post.records"}
            workspace = {"root": root, "setup_dir": root, "copied_libraries": {"D": library}}
            config = {"cadence": {"virtuoso_bin": "virtuoso", "pdk_adapter": "generic-cdf"},
                      "runtime": {"command_timeout_seconds": 10}}
            with mock.patch("crescendo_engine.workspace.write_schematic_patch", return_value=parameter_plan), \
                    mock.patch("crescendo_engine.workspace.write_parameter_snapshot", side_effect=[baseline_plan, post_plan]), \
                    mock.patch("crescendo_engine.workspace.run_command", return_value={"returncode": 0, "timed_out": False}) as run_mock, \
                    mock.patch("crescendo_engine.workspace.validate_parameter_snapshot", return_value={
                        "ok": True, "clones": [], "parameters": baseline_plan["parameters"],
                    }), mock.patch("crescendo_engine.workspace.time.monotonic", side_effect=[100.0, 101.0]), \
                    mock.patch("crescendo_engine.workspace.tree_fingerprint", return_value={"fingerprint": "same"}):
                checks = iter((False, True))
                with self.assertRaisesRegex(ParameterApplicationInfrastructureFailure, "cancelled before apply_verify"):
                    apply_candidate(workspace, config, [], {}, root, timeout_seconds=10,
                                    cancel_check=lambda: next(checks))
            self.assertEqual(run_mock.call_count, 1)

    def test_apply_candidate_preserves_phase_artifacts_on_parameter_failure(self):
        with tempfile.TemporaryDirectory() as root:
            library = os.path.join(root, "library")
            os.makedirs(library)
            baseline_plan = {"clones": [], "parameters": [{"parameter_id": "p1"}], "verification_path": "baseline.records"}
            clone_plan = {"clones": [{"occurrence_path": "/X1"}], "parameters": [], "verification_path": "clone.records"}
            parameter_plan = {"clones": [], "parameters": [{"parameter_id": "p1"}], "verification_path": "parameter.records"}
            post_plan = {"clones": [], "parameters": [{"parameter_id": "p1"}], "verification_path": "post.records"}

            def write_plan(_workspace, _parameters, _values, _skill, _records, _adapter,
                           emit_clones=True, emit_parameters=True, **_kwargs):
                return clone_plan if emit_clones and not emit_parameters else parameter_plan

            workspace = {"root": root, "setup_dir": root, "copied_libraries": {"D": library}}
            config = {"cadence": {"virtuoso_bin": "virtuoso", "pdk_adapter": "generic-cdf"},
                      "runtime": {"command_timeout_seconds": 10}}
            with mock.patch("crescendo_engine.workspace.write_schematic_patch", side_effect=write_plan), \
                    mock.patch("crescendo_engine.workspace.write_parameter_snapshot", side_effect=[baseline_plan, post_plan]), \
                    mock.patch("crescendo_engine.workspace.run_command", side_effect=[
                        {"returncode": 0, "timed_out": False},
                        {"returncode": 2, "timed_out": False,
                         "stderr_tail": "CRESCENDO_ADAPTER_ERROR:cdf_callback_incompatible callback failed"},
                    ]), mock.patch("crescendo_engine.workspace.validate_schematic_application", return_value={
                        "ok": True, "clones": clone_plan["clones"], "parameters": [],
                    }), mock.patch("crescendo_engine.workspace.validate_parameter_snapshot", return_value={
                        "ok": True, "clones": [], "parameters": baseline_plan["parameters"],
                    }):
                with self.assertRaises(ParameterApplicationFailure) as raised:
                    apply_candidate(workspace, config, [], {}, root, timeout_seconds=10)
            self.assertEqual(raised.exception.code, "cdf_callback_incompatible")
            self.assertNotIsInstance(raised.exception, ParameterApplicationInfrastructureFailure)
            with open(os.path.join(root, "candidate_patch.json")) as stream:
                artifact = __import__("json").load(stream)
            self.assertEqual([row["name"] for row in artifact["schematic_patch_phases"]], ["baseline", "apply_verify"])
            self.assertEqual(artifact["schematic_patch_phases"][1]["command"]["returncode"], 2)

    def test_render_preserves_engineering_suffix(self):
        value, prop_type = render_physical_value(2.25e-6, {"cdf_type": "string", "current_value": "1.5u"})
        self.assertEqual(value, "2.25u")
        self.assertEqual(prop_type, "string")
        value, prop_type = render_physical_value(3.7, {"cdf_type": "int", "current_value": "1"})
        self.assertEqual(value, 4)
        self.assertEqual(prop_type, "int")

    def test_spectre_timing_pairs_parallel_wrapper_processes_by_pid(self):
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, "spectre_timeout.log")
            with open(path, "w") as stream:
                stream.write("START epoch=10 pid=101 cwd=%s/a argv=input.scs\n" % root)
                stream.write("START epoch=11 pid=202 cwd=%s/b argv=input.scs\n" % root)
                stream.write("END epoch=14 pid=202 returncode=0\n")
                stream.write("END epoch=16 pid=101 returncode=1\n")
            timing = summarize_spectre_timing(path, root)
        self.assertEqual(timing["run_count"], 2)
        self.assertEqual(timing["completed_run_count"], 2)
        self.assertEqual(timing["total_seconds"], 9.0)
        self.assertEqual(timing["span_seconds"], 6.0)
        self.assertEqual(sorted(row["cwd"] for row in timing["runs"]), ["a", "b"])

    def test_combined_phase_children_do_not_exit_cadence(self):
        with tempfile.TemporaryDirectory() as root:
            workspace = {"copied_libraries": {}}
            patch = os.path.join(root, "patch.il")
            snapshot = os.path.join(root, "snapshot.il")
            write_schematic_patch(workspace, [], {}, patch, exit_after=False)
            write_parameter_snapshot(
                workspace, [], {}, snapshot, os.path.join(root, "snapshot.records"),
                "POST", exit_after=False,
            )
            with open(patch) as stream:
                self.assertNotIn("exit()", stream.read())
            with open(snapshot) as stream:
                self.assertNotIn("exit()", stream.read())

    def test_standalone_combined_script_catches_failed_load_and_exits(self):
        with tempfile.TemporaryDirectory() as root:
            output = os.path.join(root, "combined.il")
            write_combined_cadence_script(output, [("parameters", "/private/parameters.il")])
            with open(output) as stream:
                script = stream.read()
        self.assertIn('errset(load("/private/parameters.il") t)', script)
        self.assertLess(script.index("CRESCENDO_PHASE_FAILED parameters"), script.index("exit(1)"))
        self.assertLess(script.index("exit(1)"), script.index("CRESCENDO_PHASE_DONE parameters"))
        self.assertTrue(script.endswith('printf("CRESCENDO_COMBINED_DONE\\n")\nexit()\n'))

    def test_persistent_combined_script_keeps_outer_error_contract(self):
        with tempfile.TemporaryDirectory() as root:
            output = os.path.join(root, "combined.il")
            write_combined_cadence_script(
                output, [("parameters", "/private/parameters.il")], exit_after=False,
            )
            with open(output) as stream:
                script = stream.read()
        self.assertEqual(script, '\n'.join([
            'printf("CRESCENDO_COMBINED_BEGIN\\n")',
            'printf("CRESCENDO_PHASE_BEGIN parameters\\n")',
            'load("/private/parameters.il")',
            'printf("CRESCENDO_PHASE_DONE parameters\\n")',
            'printf("CRESCENDO_COMBINED_DONE\\n")',
            '',
        ]))

    def test_tree_fingerprint_changes_with_content_metadata(self):
        root = tempfile.mkdtemp()
        path = os.path.join(root, "file")
        with open(path, "w") as stream:
            stream.write("a")
        before = tree_fingerprint(root)
        with open(path, "w") as stream:
            stream.write("bb")
        after = tree_fingerprint(root)
        self.assertNotEqual(before["fingerprint"], after["fingerprint"])

    def test_pdk_grid_quantization_is_accepted_but_large_clamping_is_rejected(self):
        self.assertTrue(_physical_values_match("20.1226975662u", "20.125u", allow_display_rounding=True))
        self.assertFalse(_physical_values_match("11.9269613935u", "8u", allow_display_rounding=True))

    def test_scoped_fingerprint_ignores_unrelated_cells(self):
        with tempfile.TemporaryDirectory() as root:
            os.makedirs(os.path.join(root, "USED"))
            os.makedirs(os.path.join(root, "UNRELATED"))
            used = os.path.join(root, "USED", "schematic")
            unrelated = os.path.join(root, "UNRELATED", "maestro.sdb")
            with open(used, "w") as stream:
                stream.write("used")
            with open(unrelated, "w") as stream:
                stream.write("before")
            before = tree_fingerprint(root, ["USED"])
            with open(unrelated, "w") as stream:
                stream.write("after")
            self.assertEqual(before["fingerprint"], tree_fingerprint(root, ["USED"])["fingerprint"])
            with open(used, "w") as stream:
                stream.write("changed")
            self.assertNotEqual(before["fingerprint"], tree_fingerprint(root, ["USED"])["fingerprint"])

    def test_content_fingerprint_is_path_and_timestamp_independent(self):
        with tempfile.TemporaryDirectory() as first, tempfile.TemporaryDirectory() as second:
            for root in (first, second):
                os.makedirs(os.path.join(root, "CELL"))
                with open(os.path.join(root, "CELL", "schematic.oa"), "wb") as stream:
                    stream.write(b"same oa bytes")
            os.utime(os.path.join(second, "CELL", "schematic.oa"), (1, 1))
            self.assertEqual(
                tree_content_fingerprint(first, ["CELL"])["fingerprint"],
                tree_content_fingerprint(second, ["CELL"])["fingerprint"],
            )

    def test_content_fingerprint_ignores_results_and_detects_scoped_content(self):
        with tempfile.TemporaryDirectory() as root:
            os.makedirs(os.path.join(root, "TB", "maestro", "results"))
            with open(os.path.join(root, "TB", "maestro", "data.oa"), "w") as stream:
                stream.write("setup")
            result = os.path.join(root, "TB", "maestro", "results", "run")
            with open(result, "w") as stream:
                stream.write("old")
            before = tree_content_fingerprint(root, ["TB"])
            with open(result, "w") as stream:
                stream.write("new")
            self.assertEqual(before["fingerprint"], tree_content_fingerprint(root, ["TB"])["fingerprint"])
            with open(os.path.join(root, "TB", "maestro", "data.oa"), "w") as stream:
                stream.write("changed")
            self.assertNotEqual(before["fingerprint"], tree_content_fingerprint(root, ["TB"])["fingerprint"])

    def test_content_fingerprint_ignores_maestro_runtime_and_backup_files(self):
        with tempfile.TemporaryDirectory() as root:
            maestro = os.path.join(root, "TB", "maestro")
            os.makedirs(os.path.join(maestro, "test_states"))
            with open(os.path.join(maestro, "maestro.sdb"), "w") as stream:
                stream.write("setup")
            before = tree_content_fingerprint(root, ["TB"])
            for relative, content in (
                ("active.state", "active"),
                ("maestro.sdb~", "editor backup"),
                ("maestro.sdb.bak-20260715-010101", "manual backup"),
                (os.path.join("test_states", "Interactive.1.state"), "runtime state"),
            ):
                with open(os.path.join(maestro, relative), "w") as stream:
                    stream.write(content)
            self.assertEqual(before["fingerprint"], tree_content_fingerprint(root, ["TB"])["fingerprint"])

    def test_content_fingerprint_compacts_overlapping_scopes_and_rejects_absolute_scope(self):
        with tempfile.TemporaryDirectory() as root:
            os.makedirs(os.path.join(root, "A", "B"))
            with open(os.path.join(root, "A", "B", "file"), "w") as stream:
                stream.write("content")
            parent = tree_content_fingerprint(root, ["A"])
            overlap = tree_content_fingerprint(root, ["A", os.path.join("A", "B")])
            self.assertEqual(parent, overlap)
            with self.assertRaisesRegex(ValueError, "must be relative"):
                tree_content_fingerprint(root, [os.path.join(root, "A")])

    def test_relative_define_is_resolved_from_declaring_cds_lib(self):
        with tempfile.TemporaryDirectory() as root:
            cds_dir = os.path.join(root, "project")
            library = os.path.join(cds_dir, "libraries", "SHARED")
            os.makedirs(library)
            cds_lib = os.path.join(cds_dir, "cds.lib")
            with open(cds_lib, "w") as stream:
                stream.write("DEFINE SHARED libraries/SHARED\n")
            self.assertEqual(parse_cds_defines(cds_lib)["SHARED"], os.path.realpath(library))

    def _library_resolution_fixture(self, root):
        project = os.path.join(root, "project")
        primary = os.path.join(project, "PRIMARY")
        shared = os.path.join(root, "user", "SHARED")
        cadence = os.path.join(root, "cadence")
        spectre = os.path.join(root, "spectre")
        pdk = os.path.join(root, "pdk")
        libraries = {
            "PRIMARY": primary,
            "SHARED": shared,
            "SYS_REF": os.path.join(cadence, "libraries", "SYS_REF"),
            "SIM_REF": os.path.join(spectre, "libraries", "SIM_REF"),
            "PDK_REF": os.path.join(pdk, "PDK_REF"),
        }
        for name, path in libraries.items():
            os.makedirs(os.path.join(path, "CELL"))
            with open(os.path.join(path, "CELL", "data.oa"), "w") as stream:
                stream.write(name)
        maestro = os.path.join(primary, "TB", "maestro", "maestro.sdb")
        os.makedirs(os.path.dirname(maestro))
        with open(maestro, "w") as stream:
            stream.write("setup")
        with open(os.path.join(project, "cds.lib"), "w") as stream:
            for name, path in sorted(libraries.items()):
                stream.write("DEFINE %s %s\n" % (name, os.path.relpath(path, project)))
        setup = {
            "source": {"library_root": primary, "maestro_sdb": maestro},
            "libraries": ["PRIMARY"],
        }
        hierarchy = {"designs": [{"hierarchy": {
            "cells": [
                {"library": name, "cell": "CELL"} for name in sorted(libraries)
            ],
            "instances": [{
                "master_library": "SHARED", "has_schematic": True,
            }, {
                "master_library": "PDK_REF", "has_schematic": False,
            }],
        }}]}
        config = {"cadence": {
            "cadence_root": cadence,
            "spectre_root": spectre,
            "pdk_name": "PDK_REF",
            "pdk_dir": pdk,
        }}
        return setup, hierarchy, config, libraries

    def test_resolution_excludes_system_and_pdk_dirs_but_copies_shared_user_library(self):
        with tempfile.TemporaryDirectory() as root:
            setup, hierarchy, config, libraries = self._library_resolution_fixture(root)
            primary, _cds_lib, resolved = resolve_copy_libraries(setup, hierarchy, config=config)
            self.assertEqual(primary, "PRIMARY")
            self.assertEqual(set(resolved), set(("PRIMARY", "SHARED")))
            self.assertEqual(resolved["SHARED"], os.path.realpath(libraries["SHARED"]))
            identity = source_design_identity(setup, hierarchy, config=config)
            self.assertEqual(
                [row["library"] for row in identity["libraries"]],
                ["PRIMARY", "SHARED"],
            )
            run_root = os.path.join(root, "runs")
            config["runtime"] = {"run_root": run_root}
            workspace_root = os.path.join(run_root, "candidates", "g0", "workspace")
            workspace = build_workspace(setup, hierarchy, config, workspace_root)
            self.assertEqual(set(workspace["copied_libraries"]), set(("PRIMARY", "SHARED")))

    def test_unresolved_required_user_hierarchy_library_fails_closed(self):
        with tempfile.TemporaryDirectory() as root:
            primary = os.path.join(root, "PRIMARY")
            maestro = os.path.join(primary, "TB", "maestro", "maestro.sdb")
            os.makedirs(os.path.dirname(maestro))
            with open(maestro, "w") as stream:
                stream.write("setup")
            with open(os.path.join(root, "cds.lib"), "w") as stream:
                stream.write("DEFINE PRIMARY PRIMARY\n")
            setup = {
                "source": {"library_root": primary, "maestro_sdb": maestro},
                "libraries": ["PRIMARY"],
            }
            hierarchy = {"designs": [{"hierarchy": {
                "cells": [{"library": "PRIMARY", "cell": "TB"},
                          {"library": "MISSING_SHARED", "cell": "CELL"}],
                "instances": [{
                    "master_library": "MISSING_SHARED", "has_schematic": True,
                }],
            }}]}
            with self.assertRaisesRegex(ValueError, "MISSING_SHARED"):
                resolve_copy_libraries(setup, hierarchy)

    def test_source_design_identity_contains_no_source_path(self):
        with tempfile.TemporaryDirectory() as root:
            library = os.path.join(root, "DESIGN")
            maestro = os.path.join(library, "TB", "maestro", "maestro.sdb")
            os.makedirs(os.path.dirname(maestro))
            with open(maestro, "w") as stream:
                stream.write("setup")
            setup = {
                "source": {"library_root": library, "maestro_sdb": maestro},
            }
            identity = source_design_identity(setup, {})
            self.assertEqual(identity["schema_version"], "crescendo.source_design_identity.v1")
            self.assertNotIn(root, json.dumps(identity, sort_keys=True))

    def test_workspace_rejects_source_changed_after_prepare(self):
        with tempfile.TemporaryDirectory() as root:
            library = os.path.join(root, "DESIGN")
            maestro = os.path.join(library, "TB", "maestro", "maestro.sdb")
            os.makedirs(os.path.dirname(maestro))
            with open(maestro, "w") as stream:
                stream.write("setup-a")
            setup = {
                "source": {"library_root": library, "maestro_sdb": maestro},
            }
            expected = source_design_identity(setup, {})
            with open(maestro, "w") as stream:
                stream.write("setup-b")
            config = {"runtime": {"run_root": os.path.join(root, "runs")}, "cadence": {}}
            workspace = os.path.join(root, "runs", "candidate", "workspace")
            with self.assertRaisesRegex(WorkspaceBuildFailure, "source design changed"):
                build_workspace(
                    setup, {}, config, workspace, expected_source_identity=expected,
                )

    def test_source_guard_detects_same_size_change_with_restored_mtime(self):
        with tempfile.TemporaryDirectory() as root:
            source, source_file, workspace_root, setup, config = self._workspace_build_fixture(root)
            workspace = build_workspace(setup, {}, config, workspace_root)
            original_stat = os.stat(source_file)
            with open(source_file, "w") as stream:
                stream.write("mutated!")
            os.utime(source_file, ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns))
            guard = verify_sources_unchanged(workspace)
            self.assertFalse(guard["ok"])
            self.assertFalse(guard["libraries"][0]["unchanged"])

    def test_workspace_rehashes_source_after_copy(self):
        with tempfile.TemporaryDirectory() as root:
            source, source_file, workspace, setup, config = self._workspace_build_fixture(root)
            original_stat = os.stat(source_file)
            copytree = shutil.copytree

            def copy_then_mutate(source_root, destination, **kwargs):
                with mock.patch.object(shutil, "copytree", copytree):
                    result = copytree(source_root, destination, **kwargs)
                with open(source_file, "w") as stream:
                    stream.write("mutated!")
                os.utime(source_file, ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns))
                return result

            with mock.patch(
                    "crescendo_engine.workspace.shutil.copytree", side_effect=copy_then_mutate):
                with self.assertRaises(WorkspaceBuildFailure) as raised:
                    build_workspace(setup, {}, config, workspace)
            self.assertIn("changed during private workspace copy", str(raised.exception.cause))
            self.assertFalse(raised.exception.cleanup["ok"])
            self.assertTrue(os.path.isdir(workspace))

    def test_build_workspace_accepts_omitted_config_under_candidate_root(self):
        with tempfile.TemporaryDirectory() as root:
            source, _source_file, _workspace, setup, _config = self._workspace_build_fixture(root)
            workspace = os.path.join(root, "runs", "run_2", "candidates", "g0", "workspace")
            built = build_workspace(setup, {}, root=workspace)
            self.assertEqual(built["source_libraries"], {"D": source})

    def test_workspace_ignores_source_cdsinit_and_loads_only_private_explicit_extensions(self):
        with tempfile.TemporaryDirectory() as root:
            source, _source_file, workspace, setup, config = self._workspace_build_fixture(root)
            project_root = os.path.dirname(source)
            with open(os.path.join(project_root, "cds.lib"), "w") as stream:
                stream.write("DEFINE D D\n")
            source_init = os.path.join(project_root, ".cdsinit")
            with open(source_init, "w") as stream:
                stream.write('load("vendor-specific.il")\n')
            extension = os.path.join(root, "portable_startup.il")
            with open(extension, "w") as stream:
                stream.write('procedure(crescPortableStartup() t)\n')
            config["cadence"]["startup_extensions"] = [extension]
            built = build_workspace(setup, {}, config, workspace)
            with open(os.path.join(workspace, ".cdsinit")) as stream:
                generated = stream.read()
            self.assertNotIn(source_init, generated)
            self.assertNotIn("vendor-specific.il", generated)
            self.assertEqual(built["source_cdsinit"]["path"], source_init)
            self.assertTrue(built["source_cdsinit"]["present"])
            self.assertFalse(built["source_cdsinit"]["loaded"])
            self.assertEqual(len(built["startup_extensions"]), 1)
            copied = built["startup_extensions"][0]["private_copy"]
            self.assertTrue(copied.startswith(workspace + os.sep))
            self.assertIn(copied, generated)
            self.assertLess(generated.index(copied), generated.index("techGetTechFile"))
            with open(copied) as stream:
                self.assertIn("crescPortableStartup", stream.read())

    def _nested_parameter(self, parameter_id, occurrence, instance, value="2u"):
        return {
            "parameter_id": parameter_id,
            "backend": "schematic",
            "design_library": "DESIGN",
            "design_cell": "TOP",
            "design_view": "schematic",
            "hierarchy_path": "%s/%s" % (occurrence, instance),
            "owner_occurrence_path": occurrence,
            "owner_library": "DESIGN",
            "owner_cell": "SHARED",
            "name": "w",
            "cdf_type": "string",
            "current_value": value,
            "occurrence_chain": [{
                "hierarchy_path": occurrence,
                "parent_path": "/",
                "name": occurrence.strip("/"),
                "master_library": "DESIGN",
                "master_cell": "SHARED",
                "master_view": "symbol",
                "master_view_type": "schematicSymbol",
                "has_schematic": True,
            }],
        }

    def test_reused_subcell_occurrences_get_distinct_deterministic_clones(self):
        with tempfile.TemporaryDirectory() as root:
            library = os.path.join(root, "DESIGN")
            os.makedirs(library)
            workspace = {"copied_libraries": {"DESIGN": library}}
            parameters = [
                self._nested_parameter("p1", "/X1", "M0"),
                self._nested_parameter("p2", "/X2", "M0"),
            ]
            first = build_occurrence_clone_plan(workspace, parameters, {"p1": 3e-6, "p2": 4e-6})
            second = build_occurrence_clone_plan(workspace, parameters, {"p1": 3e-6, "p2": 4e-6})
            self.assertEqual(first, second)
            self.assertEqual(len(first), 2)
            self.assertNotEqual(first[0]["clone_cell"], first[1]["clone_cell"])
            self.assertTrue(all(len(row["clone_cell"]) <= 32 for row in first))

    def test_existing_clone_policy_requires_prior_private_clone(self):
        with tempfile.TemporaryDirectory() as root:
            library = os.path.join(root, "DESIGN")
            os.makedirs(library)
            workspace = {"copied_libraries": {"DESIGN": library}}
            parameter = self._nested_parameter("p1", "/X1", "M0")
            initial = build_occurrence_clone_plan(workspace, [parameter], {"p1": 3e-6})
            clone_root = os.path.join(library, initial[0]["clone_cell"])

            with self.assertRaisesRegex(ParameterApplicationFailure, "expected clone cell is missing"):
                build_occurrence_clone_plan(
                    workspace, [parameter], {"p1": 3e-6}, clone_state="existing",
                )

            os.makedirs(clone_root)
            reused = build_occurrence_clone_plan(
                workspace, [parameter], {"p1": 4e-6}, clone_state="existing",
            )
            self.assertEqual(reused[0]["clone_cell"], initial[0]["clone_cell"])
            self.assertTrue(reused[0]["clone_preexisting"])
            with self.assertRaisesRegex(ParameterApplicationFailure, "clone cell already exists"):
                build_occurrence_clone_plan(workspace, [parameter], {"p1": 4e-6})

            plan = write_schematic_patch(
                workspace, [parameter], {"p1": 4e-6}, os.path.join(root, "reuse.il"),
                emit_clones=True, emit_parameters=False, clone_state="mixed",
            )
            self.assertEqual(plan["clones"], [])
            with open(os.path.join(root, "reuse.il")) as stream:
                self.assertNotIn("dbCopyCellView", stream.read())

    def test_patch_uses_single_instance_recreation_and_cdf_callback(self):
        with tempfile.TemporaryDirectory() as root:
            library = os.path.join(root, "DESIGN")
            os.makedirs(library)
            workspace = {"copied_libraries": {"DESIGN": library}}
            parameter = self._nested_parameter("p1", "/X1", "M0")
            skill = os.path.join(root, "patch.il")
            plan = write_schematic_patch(workspace, [parameter], {"p1": 3e-6}, skill)
            with open(skill) as stream:
                text = stream.read()
            self.assertIn("dbCreateParamInst", text)
            self.assertIn("cdfUpdateInstParam", text)
            self.assertIn("evalstring(callbackExpr)", text)
            self.assertIn('dbReplaceProp(inst "w" "string" "3u")', text)
            self.assertNotIn('when(existingProp dbReplaceProp(inst "w"', text)
            self.assertIn('cdfParam~>value = "3u"', text)
            self.assertIn('schematicValue = if(schematicProp then schematicProp~>value else cdfParam~>value)', text)
            self.assertNotIn("snapshotNames", text)
            self.assertNotIn("NETLIST_BEFORE", text)
            self.assertNotIn("NETLIST_PARAM", text)
            self.assertNotIn("dbSetInstHeaderMasterName", text)
            self.assertEqual(len(plan["clones"]), 1)
            parameter_only = write_schematic_patch(
                workspace, [parameter], {"p1": 3e-6}, os.path.join(root, "parameters.il"),
                emit_clones=False, emit_parameters=True,
            )
            self.assertEqual(parameter_only["clones"], [])
            self.assertEqual(parameter_only["parameters"][0]["cell"], plan["clones"][0]["clone_cell"])

    def test_cdf_failure_marker_preserves_typed_input_callback_order_and_failure_gate(self):
        with tempfile.TemporaryDirectory() as root:
            library = os.path.join(root, "DESIGN")
            os.makedirs(library)
            workspace = {"copied_libraries": {"DESIGN": library}}
            for cdf_type, value, literal in (("string", 3e-6, '"3u"'), ("int", 5, "5")):
                with self.subTest(cdf_type=cdf_type):
                    parameter = self._nested_parameter("p1", "/X1", "M0")
                    parameter["cdf_type"] = cdf_type
                    skill = os.path.join(root, cdf_type + ".il")
                    write_schematic_patch(workspace, [parameter], {"p1": value}, skill)
                    with open(skill, encoding="utf-8") as stream:
                        text = stream.read()
                    self.assertIn('let((crescCallbackInput crescCallbackExtra)', text)
                    self.assertIn('list("parameter_id" "p1")', text)
                    self.assertIn('list("requested_value" %s)' % literal, text)
                    self.assertIn('list("db_property_value" if(crescCallbackInput then crescCallbackInput~>value else nil))', text)
                    self.assertIn('list("cdf_value" cdfParam~>value)', text)
                    self.assertLess(text.index('crescCallbackInput = errset('), text.index('callbackResult = errset(evalstring(callbackExpr) t)'))
                    self.assertEqual(text.count('evalstring(callbackExpr)'), 1)
                    self.assertIn('unless(callbackResult\n', text)
                    self.assertIn('errset(printf("CRESCENDO_CDF_CALLBACK_INPUT', text)
                    failure = text.index('unless(callbackResult', text.index('evalstring(callbackExpr)'))
                    marker = text.index('CRESCENDO_CDF_CALLBACK_INPUT')
                    error = text.index('CRESCENDO_ADAPTER_ERROR:cdf_callback_incompatible')
                    self.assertLess(failure, marker)
                    self.assertLess(marker, error)
                    self.assertLess(error, text.index("cdfUpdateInstParam(inst)"))
                    self.assertNotIn('foreach(', text[text.index('let((crescCallbackInput crescCallbackExtra)'):error])
                    bounded_skill_body_lines(text.splitlines())
                    # The existing symbolic-export contract removes this whole
                    # when expression before it exports parameter expressions.
                    symbolic = re.sub(r'(?m)^\s*when\(callbackExpr && callbackExpr != ""\s*$.*?^\s*\)\s*$', "", text, flags=re.S | re.M)
                    self.assertNotIn('CRESCENDO_CDF_CALLBACK_INPUT', symbolic)
                    self.assertNotIn('crescCallbackInput', symbolic)
                    self.assertNotIn('evalstring(callbackExpr)', symbolic)
                    bounded_skill_body_lines(symbolic.splitlines())

    def test_custom_pdk_adapter_keeps_transaction_without_generic_failure_marker(self):
        with tempfile.TemporaryDirectory() as root:
            library = os.path.join(root, "DESIGN")
            os.makedirs(library)
            workspace = {"copied_libraries": {"DESIGN": library}}
            parameter = self._nested_parameter("p1", "/X1", "M0")
            adapter = os.path.join(root, "custom.il")
            with open(adapter, "w", encoding="utf-8") as stream:
                stream.write("; Fixture adapter is never executed.\n")
            skill = os.path.join(root, "patch.il")
            write_schematic_patch(workspace, [parameter], {"p1": 3e-6}, skill, pdk_adapter=adapter)
            with open(skill, encoding="utf-8") as stream:
                text = stream.read()
            self.assertIn('crescPdkApplyParameter(inst "w" "3u")', text)
            self.assertNotIn('CRESCENDO_CDF_CALLBACK_INPUT', text)
            self.assertNotIn('evalstring(callbackExpr)', text)
            bounded_skill_body_lines(text.splitlines())

    def test_baseline_snapshot_is_read_only_and_targets_uncloned_owner(self):
        with tempfile.TemporaryDirectory() as root:
            library = os.path.join(root, "DESIGN")
            os.makedirs(library)
            workspace = {"copied_libraries": {"DESIGN": library}}
            parameter = self._nested_parameter("p1", "/X1", "M0")
            skill = os.path.join(root, "baseline.il")
            records = os.path.join(root, "baseline.records")
            plan = write_parameter_snapshot(
                workspace, [parameter], {"p1": 3e-6}, skill, records, "BASELINE", "baseline",
            )
            with open(skill) as stream:
                text = stream.read()
            self.assertIn('dbOpenCellViewByType("DESIGN" "SHARED" "schematic" "schematic" "r")', text)
            self.assertIn("snapshotNames", text)
            self.assertIn("if(stringp(snapshotName) then stringToSymbol(snapshotName) else snapshotName)", text)
            self.assertIn("snapshotRawMappings = get(snapshotSpec 'propMapping)", text)
            self.assertIn("BASELINE_MAP", text)
            self.assertIn("errset(list(snapshotSymbol effectiveParam~>value))", text)
            self.assertIn("BASELINE_DIRECT", text)
            self.assertIn("snapshotValue = if(snapshotProp then snapshotProp~>value else snapshotCdfParam~>value)", text)
            self.assertIn("\n  )\n)\ndbClose(cv)", text)
            self.assertNotIn("cdfUpdateInstParam", text)
            self.assertNotIn("evalstring", text)
            self.assertNotIn("dbReplaceProp", text)
            self.assertNotIn("dbSave", text)
            self.assertEqual(plan["parameters"][0]["cell"], "SHARED")

    def test_post_snapshot_targets_occurrence_clone_without_writing(self):
        with tempfile.TemporaryDirectory() as root:
            library = os.path.join(root, "DESIGN")
            os.makedirs(library)
            workspace = {"copied_libraries": {"DESIGN": library}}
            parameter = self._nested_parameter("p1", "/X1", "M0")
            skill = os.path.join(root, "post.il")
            plan = write_parameter_snapshot(
                workspace, [parameter], {"p1": 3e-6}, skill, os.path.join(root, "post.records"),
                "POST", "patched",
            )
            with open(skill) as stream:
                text = stream.read()
            self.assertIn(plan["clones"][0]["clone_cell"], text)
            self.assertNotIn("cdfUpdateInstParam", text)
            self.assertNotIn("dbSave", text)

    def test_schematic_and_netlist_three_value_verification(self):
        with tempfile.TemporaryDirectory() as root:
            records = os.path.join(root, "schematic.records")
            with open(records, "w") as stream:
                stream.write('PARAM\t"p1"\t"3u"\t"3.0u"\t"3u"\t"executed"\t"callback()"\n')
            patch_plan = {"verification_path": records, "clones": [], "parameters": [{
                "parameter_id": "p1", "design_library": "DESIGN", "design_cell": "TOP",
                "library": "DESIGN", "cell": "TOP", "instance": "M0", "property": "w",
            }]}
            schematic = validate_schematic_application(patch_plan)
            simulation = os.path.join(root, "simulation", "DESIGN", "TOP", "maestro", "results", "maestro", "Interactive.1", "psf", "corner", "netlist")
            os.makedirs(simulation)
            with open(os.path.join(simulation, "netlist"), "w") as stream:
                stream.write("// Library name: DESIGN\n// Cell name: TOP\n// View name: schematic\nM0 (d g s b) nch w=3u l=180n\n")
            result = verify_parameter_application(
                {"simulation_root": os.path.join(root, "simulation")},
                {"schematic_verification": schematic}, root,
            )
            self.assertTrue(result["ok"])
            self.assertEqual(result["parameters"][0]["requested"], "3u")
            self.assertEqual(result["parameters"][0]["schematic_value"], "3.0u")
            self.assertEqual(result["parameters"][0]["effective_netlist"][0]["value"], "3u")

    def test_snapshot_resolves_unique_spectre_property_mapping(self):
        with tempfile.TemporaryDirectory() as root:
            records = os.path.join(root, "post.records")
            with open(records, "w") as stream:
                stream.write('POST_DIRECT\t"p1"\t"D"\t"TOP"\t"M0"\t"w"\t"1.55u"\t"1.55u"\n')
                stream.write('POST_MAP\t"p1"\t"w"\t"wr"\n')
                stream.write('POST\t"p1"\t"wr"\t"1.55u"\n')
            plan = {"verification_path": records, "record_kind": "POST", "parameters": [{
                "parameter_id": "p1", "library": "D", "cell": "TOP",
                "instance": "M0", "property": "w", "value": "1.55u",
            }]}
            result = validate_parameter_snapshot(plan)
            self.assertEqual(result["parameters"][0]["effective_aliases"], ["wr"])
            self.assertEqual(result["parameters"][0]["netlist_parameters"], {"wr": ["1.55u"]})

    def test_snapshot_rejects_ambiguous_spectre_property_mapping(self):
        with tempfile.TemporaryDirectory() as root:
            records = os.path.join(root, "post.records")
            with open(records, "w") as stream:
                stream.write('POST_DIRECT\t"p1"\t"D"\t"TOP"\t"M0"\t"w"\t"1.55u"\t"1.55u"\n')
                stream.write('POST_MAP\t"p1"\t"w"\t"wr"\n')
                stream.write('POST_MAP\t"p1"\t"w"\t"width"\n')
                stream.write('POST\t"p1"\t"wr"\t"1.55u"\n')
            plan = {"verification_path": records, "record_kind": "POST", "parameters": [{
                "parameter_id": "p1", "library": "D", "cell": "TOP",
                "instance": "M0", "property": "w", "value": "1.55u",
            }]}
            with self.assertRaisesRegex(ParameterApplicationFailure, "multiple netlist parameters"):
                validate_parameter_snapshot(plan)

    def test_merge_preserves_stable_discovered_effective_alias(self):
        application = {"clones": [], "parameters": [{
            "parameter_id": "p1", "property": "w", "requested": "20u",
        }]}
        baseline = {"parameters": [{
            "parameter_id": "p1", "library": "D", "cell": "TOP", "instance": "M0",
            "property": "w", "schematic_value": "1.5u", "cdf_value": "1.5u",
            "netlist_parameters": {"wr": ["1.5u"]}, "effective_aliases": ["wr"],
        }]}
        post = {"parameters": [{
            "parameter_id": "p1", "library": "D", "cell": "TOP", "instance": "M0",
            "property": "w", "schematic_value": "8u", "cdf_value": "8u",
            "netlist_parameters": {"wr": ["8u"]}, "effective_aliases": ["wr"],
        }]}
        merged = merge_parameter_verification(application, baseline, post)
        self.assertEqual(merged["parameters"][0]["effective_aliases"], ["wr"])
        self.assertEqual(merged["parameters"][0]["persisted_cdf_value"], "8u")

    def test_final_netlist_alias_matches_pdk_adjusted_cdf_value(self):
        with tempfile.TemporaryDirectory() as root:
            simulation = os.path.join(
                root, "simulation", "D", "TOP", "maestro", "results", "maestro",
                "Interactive.1", "psf", "corner", "netlist",
            )
            os.makedirs(simulation)
            with open(os.path.join(simulation, "netlist"), "w") as stream:
                stream.write("// Library name: D\n// Cell name: TOP\nM0 (d g s b) nch wr=8u\n")
            row = {
                "parameter_id": "w", "design_library": "D", "design_cell": "TOP",
                "library": "D", "cell": "TOP", "instance": "M0", "property": "w",
                "requested": "20u", "persisted_cdf_value": "8u",
                "effective_aliases": ["wr"], "instance_selected_count": 1,
            }
            result = verify_parameter_application(
                {"simulation_root": os.path.join(root, "simulation")},
                {"schematic_verification": {"parameters": [row]}}, root,
            )
            effective = result["parameters"][0]["effective_netlist"][0]
            self.assertEqual(effective["mode"], "direct_alias")
            self.assertEqual(effective["parameter"], "wr")

    def test_parameter_verification_can_bind_one_persistent_history(self):
        with tempfile.TemporaryDirectory() as root:
            simulation_root = os.path.join(root, "simulation")
            for history_name, width in (("CRESCENDO_first", "3u"), ("CRESCENDO_second", "7u")):
                netlist_root = os.path.join(
                    simulation_root, "D", "TOP", "maestro", "results", "maestro",
                    history_name, "psf", "corner", "netlist",
                )
                os.makedirs(netlist_root)
                with open(os.path.join(netlist_root, "netlist"), "w") as stream:
                    stream.write("// Library name: D\n// Cell name: TOP\nM0 (d g s b) nch w=%s\n" % width)
            row = {
                "parameter_id": "w", "design_library": "D", "design_cell": "TOP",
                "library": "D", "cell": "TOP", "instance": "M0", "property": "w",
                "requested": "7u", "instance_selected_count": 1,
            }
            result = verify_parameter_application(
                {"simulation_root": simulation_root},
                {"schematic_verification": {"parameters": [row]}}, root,
                history_name="CRESCENDO_second",
            )
            self.assertEqual(len(result["parameters"][0]["effective_netlist"]), 1)
            self.assertEqual(result["parameters"][0]["effective_netlist"][0]["value"], "7u")

    def test_parameter_verification_accepts_canonical_point_netlist(self):
        with tempfile.TemporaryDirectory() as root:
            simulation_root = os.path.join(root, "simulation")
            for history_name, width in (("CRESCENDO_first", "3u"), ("CRESCENDO_second", "7u")):
                netlist_root = os.path.join(
                    simulation_root, "D", "TOP", "maestro", "results", "maestro",
                    history_name, "1", "AC", "netlist",
                )
                os.makedirs(netlist_root)
                with open(os.path.join(netlist_root, "netlist"), "w") as stream:
                    stream.write("// Library name: D\n// Cell name: TOP\nM0 (d g s b) nch w=%s\n" % width)
            row = {
                "parameter_id": "w", "design_library": "D", "design_cell": "TOP",
                "library": "D", "cell": "TOP", "instance": "M0", "property": "w",
                "requested": "7u", "instance_selected_count": 1,
            }
            result = verify_parameter_application(
                {"simulation_root": simulation_root},
                {"schematic_verification": {"parameters": [row]}}, root,
                history_name="CRESCENDO_second",
            )
            effective = result["parameters"][0]["effective_netlist"]
            self.assertEqual(len(effective), 1)
            self.assertEqual(effective[0]["value"], "7u")
            self.assertIn("/CRESCENDO_second/1/AC/netlist/netlist", effective[0]["netlist"])

    def test_missing_final_netlist_is_infrastructure_failure(self):
        with tempfile.TemporaryDirectory() as root:
            row = {
                "parameter_id": "w", "design_library": "D", "design_cell": "TOP",
                "library": "D", "cell": "TOP", "instance": "M0", "property": "w",
                "requested": "3u", "instance_selected_count": 1,
            }
            with self.assertRaisesRegex(
                    ParameterApplicationInfrastructureFailure, "without a final Spectre netlist"):
                verify_parameter_application(
                    {"simulation_root": os.path.join(root, "simulation")},
                    {"schematic_verification": {"parameters": [row]}}, root,
                    history_name="CRESCENDO_missing",
                )

    def test_cdf_display_quantization_uses_reported_resolution(self):
        with tempfile.TemporaryDirectory() as root:
            records = os.path.join(root, "schematic.records")
            with open(records, "w") as stream:
                stream.write('PARAM\t"p1"\t"13.8958123247u"\t"13.895u"\t"13.895u"\t"executed"\t"callback()"\n')
            patch_plan = {"verification_path": records, "clones": [], "parameters": [{
                "parameter_id": "p1", "design_library": "D", "design_cell": "TOP",
                "library": "D", "cell": "TOP", "instance": "M0", "property": "w",
            }]}
            result = validate_schematic_application(patch_plan)
            self.assertTrue(result["ok"])

    def test_ambiguous_effective_parameter_is_rejected(self):
        with self.assertRaises(ParameterApplicationFailure):
            from crescendo_engine.workspace import _effective_netlist_value
            _effective_netlist_value({("D", "TOP"): ["M0 (d g s b) nch w=2u", "M0 (d g s b) nch w=2u"]}, "D", "TOP", "M0", "w")

    def test_unrelated_unchanged_cdf_value_is_not_effective_evidence(self):
        with tempfile.TemporaryDirectory() as root:
            simulation = os.path.join(root, "simulation", "D", "TOP", "maestro", "results", "maestro", "Interactive.1", "psf", "corner", "netlist")
            os.makedirs(simulation)
            with open(os.path.join(simulation, "netlist"), "w") as stream:
                stream.write("// Library name: D\n// Cell name: TOP\nM0 (d g s b) nch m=1\n")
            row = {
                "parameter_id": "finger", "design_library": "D", "design_cell": "TOP",
                "library": "D", "cell": "TOP", "instance": "M0", "property": "fingers",
                "requested": "2", "instance_selected_count": 1,
                "netlist_parameters": {"m": ["1"]},
                "netlist_parameters_before": {"m": ["1"]},
            }
            with self.assertRaisesRegex(ParameterApplicationFailure, "no uniquely attributable"):
                verify_parameter_application(
                    {"simulation_root": os.path.join(root, "simulation")},
                    {"schematic_verification": {"parameters": [row]}}, root,
                )

    def test_changed_cdf_value_present_in_netlist_is_derived_evidence(self):
        with tempfile.TemporaryDirectory() as root:
            simulation = os.path.join(root, "simulation", "D", "TOP", "maestro", "results", "maestro", "Interactive.1", "psf", "corner", "netlist")
            os.makedirs(simulation)
            with open(os.path.join(simulation, "netlist"), "w") as stream:
                stream.write("// Library name: D\n// Cell name: TOP\nM0 (d g s b) nch ad=2p m=1\n")
            row = {
                "parameter_id": "finger", "design_library": "D", "design_cell": "TOP",
                "library": "D", "cell": "TOP", "instance": "M0", "property": "fingers",
                "requested": "2", "instance_selected_count": 1,
                "netlist_parameters": {"ad": ["2p"], "m": ["1"]},
                "netlist_parameters_before": {"ad": ["1p"], "m": ["1"]},
            }
            result = verify_parameter_application(
                {"simulation_root": os.path.join(root, "simulation")},
                {"schematic_verification": {"parameters": [row]}}, root,
            )
            effective = result["parameters"][0]["effective_netlist"][0]
            self.assertEqual(effective["mode"], "cdf_callback_derived")
            self.assertEqual([item["name"] for item in effective["evidence"]], ["ad"])

    def test_cached_derived_mapping_accepts_repeated_effective_value(self):
        with tempfile.TemporaryDirectory() as root:
            simulation = os.path.join(
                root, "simulation", "D", "TOP", "maestro", "results", "maestro",
                "CRESCENDO_repeat", "psf", "corner", "netlist",
            )
            os.makedirs(simulation)
            with open(os.path.join(simulation, "netlist"), "w") as stream:
                stream.write("// Library name: D\n// Cell name: TOP\nM0 (d g s b) nch ad=2p m=1\n")
            row = {
                "parameter_id": "finger", "design_library": "D", "design_cell": "TOP",
                "library": "D", "cell": "TOP", "instance": "M0", "property": "fingers",
                "requested": "2", "instance_selected_count": 1,
                "netlist_parameters": {"ad": ["2p"], "m": ["1"]},
                "netlist_parameters_before": {"ad": ["2p"], "m": ["1"]},
                "derived_parameter_names": ["ad"],
            }
            result = verify_parameter_application(
                {"simulation_root": os.path.join(root, "simulation")},
                {"schematic_verification": {"parameters": [row]}}, root,
                history_name="CRESCENDO_repeat",
            )
            evidence = result["parameters"][0]["effective_netlist"][0]["evidence"]
            self.assertEqual(evidence[0]["mapping_source"], "cached")

    def test_analoglib_dc_sources_use_spectre_dc_alias(self):
        for master_cell, property_name, statement in (
            ("vdc", "vdc", "V0 (out 0) vsource dc=1.01 type=dc"),
            ("idc", "idc", "I0 (out 0) isource dc=100u type=dc"),
        ):
            with self.subTest(master_cell=master_cell), tempfile.TemporaryDirectory() as root:
                simulation = os.path.join(
                    root, "simulation", "D", "TOP", "maestro", "results", "maestro",
                    "Interactive.1", "psf", "corner", "netlist",
                )
                os.makedirs(simulation)
                with open(os.path.join(simulation, "netlist"), "w") as stream:
                    stream.write("// Library name: D\n// Cell name: TOP\n%s\n" % statement)
                requested = "1.01" if master_cell == "vdc" else "100u"
                row = {
                    "parameter_id": property_name, "design_library": "D", "design_cell": "TOP",
                    "library": "D", "cell": "TOP", "instance": "V0" if master_cell == "vdc" else "I0",
                    "master_library": "analogLib", "master_cell": master_cell,
                    "property": property_name, "requested": requested, "instance_selected_count": 1,
                }
                result = verify_parameter_application(
                    {"simulation_root": os.path.join(root, "simulation")},
                    {"schematic_verification": {"parameters": [row]}}, root,
                )
                effective = result["parameters"][0]["effective_netlist"][0]
                self.assertEqual(effective["mode"], "direct_alias")
                self.assertEqual(effective["parameter"], "dc")


    def test_partial_workspace_cleanup_allows_range_validation_namespaces(self):
        relatives = (
            os.path.join(
                "range_validations", "range_validation_test",
                "workspaces", "minimum", "workspace",
            ),
            os.path.join(
                "range_validation_resident", "prepared_test", "workspace",
            ),
        )
        for relative in relatives:
            with self.subTest(relative=relative), tempfile.TemporaryDirectory() as root:
                source, _source_file, _candidate_workspace, setup, config = (
                    self._workspace_build_fixture(root)
                )
                workspace = os.path.join(config["runtime"]["run_root"], relative)

                def fail_copy(_source, destination, **_kwargs):
                    os.makedirs(destination)
                    with open(os.path.join(destination, "partial.txt"), "w") as stream:
                        stream.write("partial")
                    raise IOError("injected validation copy failure")

                with mock.patch(
                        "crescendo_engine.workspace.resolve_copy_libraries",
                        return_value=("D", None, {"D": source})), mock.patch(
                            "crescendo_engine.workspace.shutil.copytree",
                            side_effect=fail_copy):
                    with self.assertRaises(WorkspaceBuildFailure) as raised:
                        build_workspace(setup, {}, config, workspace)
                self.assertTrue(raised.exception.cleanup["ok"])
                self.assertTrue(raised.exception.cleanup["removed"])
                self.assertFalse(os.path.lexists(workspace))


if __name__ == "__main__":
    unittest.main()
