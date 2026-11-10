from __future__ import print_function

import json
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine.adexl_runner import run_adexl
from crescendo_engine.adexl_runtime import CANDIDATE_STATES


class AdeXLRunnerTest(unittest.TestCase):
    def _workspace_and_config(self, root):
        workspace_root = os.path.join(root, "workspace")
        setup_dir = os.path.join(
            workspace_root, "libraries", "DESIGN", "TB", "custom_adexl",
        )
        simulation_root = os.path.join(workspace_root, "simulation")
        os.makedirs(setup_dir)
        os.makedirs(simulation_root)
        setup_path = os.path.join(setup_dir, "data.sdb")
        with open(setup_path, "w") as stream:
            stream.write('<?xml version="1.0"?><setupdb application="ADE XL"><active/></setupdb>\n')
        setup = {
            "schema_version": "crescendo.ade_setup.v1", "backend": "adexl",
            "setup_locator": {
                "path": setup_path, "view_dir": setup_dir,
                "view_name": "custom_adexl", "cell_name": "TB",
                "library_name": "DESIGN",
            },
            "source": {"setup_db": setup_path, "data_sdb": setup_path},
            "tests": [], "corners": [],
        }
        workspace = {
            "root": workspace_root, "simulation_root": simulation_root,
            "setup_sdb": setup_path, "setup_dir": setup_dir,
            "setup_backend": "adexl", "setup_view": "custom_adexl",
            "setup_cell": "TB", "primary_library": "DESIGN",
            "setup": setup, "technology": {},
        }
        config = {
            "cadence": {
                "virtuoso_bin": "/bin/true", "spectre_bin": "/bin/true",
                "cadence_root": "", "extra_binary_paths": [],
                "extra_library_paths": [], "license_file": None,
            },
            "runtime": {
                "candidate_timeout_seconds": 30,
                "simulation_timeout_seconds": 30,
                "ocean_startup_timeout_seconds": 10,
                "command_timeout_seconds": 30,
            },
        }
        return workspace, config

    def test_one_shot_runner_binds_callback_history_and_exact_rdb(self):
        with tempfile.TemporaryDirectory() as root:
            workspace, config = self._workspace_and_config(root)
            setup_dir = workspace["setup_dir"]
            simulation_root = workspace["simulation_root"]
            candidate_root = os.path.join(root, "candidate", "attempts", "attempt_1")

            def complete_callback(_argv, **kwargs):
                try:
                    os.close(kwargs["guardian_launch_lock_fd"])
                except OSError:
                    pass
                artifact_path = None
                for current, _directories, files in os.walk(candidate_root):
                    if "adexl_candidate.json" in files:
                        artifact_path = os.path.join(current, "adexl_candidate.json")
                        break
                with open(artifact_path) as stream:
                    artifact = json.load(stream)
                identity = artifact["protocol_identity"]
                rows = []
                for state in CANDIDATE_STATES:
                    message = "0" if state == "RUN_SUBMITTED" else "ok"
                    rows.append("%s\t%d\t%s\t%s\t%s\tnow\t%s" % (
                        identity["epoch"], identity["sequence"], identity["nonce"],
                        identity["candidate_id"], state, message,
                    ))
                with open(artifact["marker_path"], "w") as stream:
                    stream.write("\n".join(rows) + "\n")
                history = "Interactive.1"
                rdb_dir = os.path.join(setup_dir, "results", "data")
                os.makedirs(rdb_dir)
                rdb_path = os.path.join(rdb_dir, history + ".rdb")
                with open(rdb_path, "wb") as stream:
                    stream.write(b"sqlite")
                results_dir = os.path.join(
                    simulation_root, "DESIGN", "TB", "custom_adexl",
                    "results", "data", history,
                )
                os.makedirs(results_dir)
                with open(artifact["result_manifest_path"], "w") as stream:
                    stream.write(
                        "RESULT\t%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t0\tfinished\n" % (
                            identity["epoch"], identity["sequence"], identity["nonce"],
                            identity["candidate_id"], history, results_dir,
                            os.path.dirname(results_dir), rdb_path,
                        )
                    )
                return {
                    "returncode": 0, "timed_out": False,
                    "startup_timed_out": False, "lock_wait_seconds": 0.0,
                    "elapsed_seconds": 1.0,
                }

            with mock.patch(
                    "crescendo_engine.adexl_runner.run_command",
                    side_effect=complete_callback) as launch:
                retained_log_root = os.path.join(root, "candidate")
                command = run_adexl(
                    workspace, config, candidate_root, timeout_seconds=30,
                    retained_log_root=retained_log_root,
                )
            self.assertEqual(command["backend"], "adexl")
            self.assertEqual(
                command["candidate_artifact"]["protocol_identity"]["candidate_id"],
                "candidate",
            )
            self.assertEqual(command["history_name"], "Interactive.1")
            self.assertTrue(command["rdb_path"].endswith("Interactive.1.rdb"))
            self.assertEqual(command["adexl_protocol_states"], list(CANDIDATE_STATES))
            self.assertTrue(command["guardian_manifest_path"].endswith("legacy_guardian_launch.json"))
            self.assertEqual(
                launch.call_args[1]["env"]["CRESCENDO_SPECTRE_RETAIN_ROOT"],
                retained_log_root,
            )
            with open(command["guardian_manifest_path"]) as stream:
                guardian = json.load(stream)
            self.assertTrue(guardian["stdout_path"].endswith("ocean.stdout"))
            self.assertTrue(guardian["stderr_path"].endswith("ocean.stderr"))
            with open(command["candidate_artifact"]["script_path"]) as stream:
                script = stream.read()
            self.assertIn("crescAxlSafeClose()", script)
            self.assertIn("exit()", script)

    def test_process_crash_before_candidate_protocol_is_infrastructure_failure(self):
        with tempfile.TemporaryDirectory() as root:
            workspace, config = self._workspace_and_config(root)
            candidate_root = os.path.join(root, "candidate", "attempts", "attempt_1")

            def crash_callback(_argv, **kwargs):
                try:
                    os.close(kwargs["guardian_launch_lock_fd"])
                except OSError:
                    pass
                return {
                    "returncode": 134, "timed_out": False,
                    "startup_timed_out": False, "termination_failed": False,
                    "lock_wait_seconds": 0.0, "elapsed_seconds": 1.0,
                }

            with mock.patch(
                    "crescendo_engine.adexl_runner.run_command",
                    side_effect=crash_callback):
                command = run_adexl(
                    workspace, config, candidate_root, timeout_seconds=30,
                )
            self.assertTrue(command["infrastructure_failure"])
            self.assertEqual(command["adexl_protocol_states"], [])

    def test_terminal_protocol_error_is_infrastructure_failure_not_design_penalty(self):
        with tempfile.TemporaryDirectory() as root:
            workspace, config = self._workspace_and_config(root)
            candidate_root = os.path.join(root, "candidate", "attempts", "attempt_1")

            def callback_error(_argv, **kwargs):
                try:
                    os.close(kwargs["guardian_launch_lock_fd"])
                except OSError:
                    pass
                artifact_path = None
                for current, _directories, files in os.walk(candidate_root):
                    if "adexl_candidate.json" in files:
                        artifact_path = os.path.join(current, "adexl_candidate.json")
                        break
                with open(artifact_path) as stream:
                    artifact = json.load(stream)
                identity = artifact["protocol_identity"]
                states = ["ACCEPTED", "BEGIN", "RUN_SUBMITTED", "ERROR"]
                with open(artifact["marker_path"], "w") as stream:
                    for state in states:
                        message = "0" if state == "RUN_SUBMITTED" else "callback failed"
                        stream.write("%s\t%d\t%s\t%s\t%s\tnow\t%s\n" % (
                            identity["epoch"], identity["sequence"], identity["nonce"],
                            identity["candidate_id"], state, message,
                        ))
                return {
                    "returncode": 0, "timed_out": False,
                    "startup_timed_out": False, "lock_wait_seconds": 0.0,
                    "elapsed_seconds": 1.0,
                }

            with mock.patch(
                    "crescendo_engine.adexl_runner.run_command",
                    side_effect=callback_error):
                command = run_adexl(
                    workspace, config, candidate_root, timeout_seconds=30,
                )
            self.assertTrue(command["infrastructure_failure"])
            self.assertIn("AdeXLRuntimeError", command["finalization_error"])
            self.assertEqual(command["adexl_protocol_states"][-1], "ERROR")


if __name__ == "__main__":
    unittest.main()
