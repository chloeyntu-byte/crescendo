from __future__ import print_function

import json
import os
import tempfile
import unittest

from crescendo_engine.adexl_runtime import (
    CANDIDATE_STATES, REQUIRED_AXL_APIS, AdeXLProtocolError,
    AdeXLResultError, AdeXLRuntimeBuilder, AdeXLRuntimeError,
    AdeXLUnsupportedMode, finalize_adexl_candidate,
    locate_adexl_point_result, normalize_adexl_runtime_options,
    parse_adexl_capabilities, render_adexl_capability_probe,
    validate_adexl_protocol, validate_adexl_rdb_completion,
)


def _setup(root):
    view_dir = os.path.join(root, "LIB", "TB", "legacy_custom_view")
    os.makedirs(view_dir)
    setup_path = os.path.join(view_dir, "data.sdb")
    with open(setup_path, "w") as stream:
        stream.write("<?xml version=\"1.0\"?><setupdb application=\"ADE XL\"><active /></setupdb>\n")
    return {
        "schema_version": "crescendo.ade_setup.v1",
        "backend": "adexl",
        "setup_locator": {
            "backend": "adexl",
            "path": setup_path,
            "view_dir": view_dir,
            "view_name": "legacy_custom_view",
            "cell_dir": os.path.dirname(view_dir),
            "cell_name": "TB",
            "library_root": os.path.dirname(os.path.dirname(view_dir)),
            "library_name": "LIB",
        },
        "source": {"setup_db": setup_path, "data_sdb": setup_path},
    }


def _private_workspace_binding(root, logical_library="LIB_A"):
    workspace_root = os.path.join(root, "private-workspace")
    view_dir = os.path.join(
        workspace_root, "LIB_A_NATIVE_STAGE", "TB", "legacy_custom_view",
    )
    os.makedirs(view_dir)
    setup_path = os.path.join(view_dir, "data.sdb")
    with open(setup_path, "w") as stream:
        stream.write("<?xml version=\"1.0\"?><setupdb application=\"ADE XL\"><active /></setupdb>\n")
    return {
        "root": workspace_root,
        "simulation_root": os.path.join(workspace_root, "simulation"),
        "setup_sdb": setup_path,
        "primary_library": logical_library,
        "setup_cell": "TB",
        "setup_view": "legacy_custom_view",
    }


def _write_protocol(path, identity, states, stale=False, run_id="7"):
    rows = []
    if stale:
        rows.append("old\t99\tstale\tother\tDONE\tnow\tstale")
    for state in states:
        message = run_id if state == "RUN_SUBMITTED" else "message"
        rows.append("%s\t%d\t%s\t%s\t%s\tnow\t%s" % (
            identity["epoch"], identity["sequence"], identity["nonce"],
            identity["candidate_id"], state, message,
        ))
    with open(path, "w") as stream:
        stream.write("\n".join(rows) + "\n")


class AdeXLRuntimeTest(unittest.TestCase):
    def test_defaults_are_local_serial_and_lscs_is_rejected(self):
        options = normalize_adexl_runtime_options()
        self.assertEqual(options["execution_mode"], "local_serial")
        self.assertEqual(options["parallel_jobs"], 1)
        self.assertFalse(options["lscs_supported"])
        with self.assertRaisesRegex(AdeXLUnsupportedMode, "LSCS"):
            normalize_adexl_runtime_options({"scheduler": "LSCS"})
        with self.assertRaisesRegex(AdeXLUnsupportedMode, "parallel_jobs=1"):
            normalize_adexl_runtime_options({"parallel_jobs": 2})

    def test_session_script_probes_public_apis_and_binds_actual_view(self):
        with tempfile.TemporaryDirectory() as root:
            setup = _setup(root)
            runtime_root = os.path.join(root, "runtime")
            builder = AdeXLRuntimeBuilder(setup, runtime_root, epoch="epoch-a")
            session = builder.build_session()
            with open(session["startup_script_path"]) as stream:
                script = stream.read()
            for api_name in REQUIRED_AXL_APIS:
                self.assertIn("isCallable('%s)" % api_name, script)
            self.assertIn('axlCreateSession("CRESCENDO_adexl_w0_epoch-a")', script)
            self.assertIn('axlSetMainSetupDBLCV(crescAxlSession "LIB" "TB" "legacy_custom_view" ?mode "a")', script)
            self.assertIn("crescAxlMainSetupDB = car(crescAxlBindResult)", script)
            self.assertIn("axlSDBHp(crescAxlMainSetupDB)", script)
            self.assertIn("crescAxlResolvedSetupDB = axlGetMainSetupDB(crescAxlSession)", script)
            self.assertNotIn("axlGetSetupDB(crescAxlSession)", script)
            self.assertIn("crescAxlSafeClose", script)
            self.assertEqual(session["execution"]["parallel_jobs"], 1)
            with open(session["manifest_path"]) as stream:
                persisted = json.load(stream)
            self.assertEqual(persisted["session_id"], session["session_id"])

    def test_private_binding_uses_logical_library_and_isolated_project_dir(self):
        with tempfile.TemporaryDirectory() as root:
            setup = _setup(os.path.join(root, "source"))
            binding = _private_workspace_binding(root)
            runtime_root = os.path.join(binding["root"], "runtime")
            builder = AdeXLRuntimeBuilder(
                setup, runtime_root, epoch="private-a",
                workspace_binding=binding, require_private_workspace=True,
            )
            session = builder.build_session()
            with open(session["startup_script_path"]) as stream:
                script = stream.read()
            self.assertIn(
                'axlSetMainSetupDBLCV(crescAxlSession "LIB_A" "TB" "legacy_custom_view" ?mode "a")',
                script,
            )
            self.assertNotIn('axlSetMainSetupDBLCV(crescAxlSession "LIB_A_NATIVE_STAGE"', script)
            self.assertIn(
                'envSetVal("asimenv.startup" "projectDir" \'string "%s")'
                % binding["simulation_root"],
                script,
            )
            self.assertEqual(session["setup_locator"]["library_name"], "LIB_A")
            self.assertEqual(session["simulation_root"], binding["simulation_root"])

    def test_private_binding_rejects_unisolated_paths(self):
        with tempfile.TemporaryDirectory() as root:
            setup = _setup(os.path.join(root, "source"))
            runtime_root = os.path.join(root, "runtime")
            with self.assertRaisesRegex(ValueError, "explicit workspace copy"):
                AdeXLRuntimeBuilder(
                    setup, runtime_root, require_private_workspace=True,
                )
            binding = _private_workspace_binding(root)
            binding["simulation_root"] = os.path.join(root, "outside-simulation")
            with self.assertRaisesRegex(ValueError, "projectDir must be below"):
                AdeXLRuntimeBuilder(
                    setup, runtime_root, workspace_binding=binding,
                    require_private_workspace=True,
                )

    def test_candidate_contract_uses_async_fenced_callback_and_direct_rdb_manifest(self):
        with tempfile.TemporaryDirectory() as root:
            setup = _setup(root)
            runtime_root = os.path.join(root, "runtime")
            builder = AdeXLRuntimeBuilder(
                setup, runtime_root, epoch="epoch-b",
                options={"candidate_timeout_seconds": 9, "poll_interval_seconds": 2},
            )
            artifact = builder.build_candidate(
                "g001_c0001", os.path.join(runtime_root, "candidates", "g001_c0001"),
                values={"w": 1e-6}, pre_run_skill_lines=["crescPatched = t"],
            )
            with open(artifact["script_path"]) as stream:
                script = stream.read()
            self.assertIn("axlRunSimulation(?session crescAxlSession ?callback", script)
            self.assertIn("?callback \"crescAxlCandidateDone_", script)
            self.assertIn("ASYNC_WAIT", script)
            self.assertNotIn("ipcSleep", script)
            self.assertIn("member(crescAxlRunId crescAxlFinishedRuns)", script)
            self.assertIn(artifact["progress_path"], script)
            self.assertIn("axlReadHistoryResDB", script)
            self.assertIn("axlGetHistoryResults(crescAxlRunData)", script)
            self.assertNotIn("axlSetHistoryName", script)
            self.assertIn("crescAxlFieldString(crescAxlRDBPath)", script)
            self.assertIn("crescPatched = t", script)
            self.assertNotIn("LSCS", script)
            self.assertEqual(artifact["poll_count"], 5)
            self.assertEqual(artifact["completion_mode"], "callback")
            self.assertIsNone(artifact["history_name"])
            self.assertTrue(artifact["requested_history_name"])
            self.assertIsNone(artifact["rdb_path"])
            self.assertEqual(artifact["worker_epoch"], "epoch-b")
            self.assertIn("history_identity", artifact)

    def test_candidate_root_must_stay_below_runtime_root(self):
        with tempfile.TemporaryDirectory() as root:
            setup = _setup(root)
            builder = AdeXLRuntimeBuilder(setup, os.path.join(root, "runtime"))
            with self.assertRaisesRegex(ValueError, "below runtime_root"):
                builder.build_candidate("c1", os.path.join(root, "escaped"))

    def test_runtime_root_must_not_modify_setup_view(self):
        with tempfile.TemporaryDirectory() as root:
            setup = _setup(root)
            with self.assertRaisesRegex(ValueError, "must not be inside"):
                AdeXLRuntimeBuilder(setup, os.path.join(setup["setup_locator"]["view_dir"], "runtime"))

    def test_protocol_ignores_stale_rows_and_requires_exact_sequence(self):
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, "status.tsv")
            identity = {"epoch": "e", "sequence": 3, "nonce": "n", "candidate_id": "c"}
            _write_protocol(path, identity, CANDIDATE_STATES, stale=True)
            rows = validate_adexl_protocol(path, identity, CANDIDATE_STATES)
            self.assertEqual([row["state"] for row in rows], list(CANDIDATE_STATES))
            _write_protocol(path, identity, ("ACCEPTED", "SIM_DONE", "DONE"))
            with self.assertRaisesRegex(AdeXLProtocolError, "invalid successful"):
                validate_adexl_protocol(path, identity, CANDIDATE_STATES)

    def test_failed_protocol_must_be_a_valid_prefix(self):
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, "status.tsv")
            identity = {"epoch": "e", "sequence": 2, "nonce": "n", "candidate_id": "c"}
            _write_protocol(path, identity, ("ACCEPTED", "BEGIN", "ERROR"))
            rows = validate_adexl_protocol(path, identity, CANDIDATE_STATES)
            self.assertEqual(rows[-1]["state"], "ERROR")
            _write_protocol(path, identity, ("BEGIN", "ERROR"))
            with self.assertRaisesRegex(AdeXLProtocolError, "invalid failed"):
                validate_adexl_protocol(path, identity, CANDIDATE_STATES)

    def test_finalization_binds_only_fenced_history_rdb(self):
        with tempfile.TemporaryDirectory() as root:
            setup = _setup(root)
            runtime_root = os.path.join(root, "runtime")
            builder = AdeXLRuntimeBuilder(setup, runtime_root, epoch="epoch-c")
            artifact = builder.build_candidate("candidate", os.path.join(runtime_root, "candidate"))
            identity = artifact["protocol_identity"]
            _write_protocol(artifact["marker_path"], identity, CANDIDATE_STATES)
            results_dir = os.path.join(root, "point-results")
            os.makedirs(results_dir)
            history_name = "Interactive.9"
            rdb_dir = os.path.join(setup["setup_locator"]["view_dir"], "results", "data")
            os.makedirs(rdb_dir)
            rdb_path = os.path.join(rdb_dir, history_name + ".rdb")
            with open(rdb_path, "wb") as stream:
                stream.write(b"sqlite")
            with open(artifact["result_manifest_path"], "w") as stream:
                stream.write("RESULT\t%s\t%d\t%s\t%s\t%s\t%s\t\t%s\t7\tfinished\n" % (
                    identity["epoch"], identity["sequence"], identity["nonce"],
                    identity["candidate_id"], history_name, results_dir, rdb_path,
                ))
            finalized = finalize_adexl_candidate(artifact)
            self.assertEqual(finalized["rdb_path"], os.path.realpath(rdb_path))
            self.assertEqual(finalized["history_name"], history_name)
            self.assertEqual(finalized["result_manifest_rdb_path"], rdb_path)
            self.assertEqual(finalized["states"], list(CANDIDATE_STATES))
            self.assertEqual(finalized["run_id"], "7")

    def test_finalization_accepts_native_history_and_rejects_empty_or_wrong_run_identity(self):
        with tempfile.TemporaryDirectory() as root:
            setup = _setup(root)
            runtime_root = os.path.join(root, "runtime")
            builder = AdeXLRuntimeBuilder(setup, runtime_root)
            artifact = builder.build_candidate("candidate", os.path.join(runtime_root, "candidate"))
            identity = artifact["protocol_identity"]
            _write_protocol(artifact["marker_path"], identity, CANDIDATE_STATES)
            with open(artifact["result_manifest_path"], "w") as stream:
                stream.write("RESULT\t%s\t%d\t%s\t%s\tInteractive.12\t\t\t\t7\tfinished\n" % (
                    identity["epoch"], identity["sequence"], identity["nonce"], identity["candidate_id"],
                ))
            finalized = finalize_adexl_candidate(artifact, require_rdb=False)
            self.assertEqual(finalized["history_name"], "Interactive.12")
            with open(artifact["result_manifest_path"], "w") as stream:
                stream.write("RESULT\t%s\t%d\t%s\t%s\t\t\t\t\t7\tfinished\n" % (
                    identity["epoch"], identity["sequence"], identity["nonce"],
                    identity["candidate_id"],
                ))
            with self.assertRaisesRegex(AdeXLResultError, "no history identity"):
                finalize_adexl_candidate(artifact, require_rdb=False)
            with open(artifact["result_manifest_path"], "w") as stream:
                stream.write("RESULT\t%s\t%d\t%s\t%s\tInteractive.13\t\t\t\t8\tfinished\n" % (
                    identity["epoch"], identity["sequence"], identity["nonce"],
                    identity["candidate_id"],
                ))
            with self.assertRaisesRegex(AdeXLResultError, "submitted run identity"):
                finalize_adexl_candidate(artifact, require_rdb=False)

    def test_point_result_locator_binds_non_aggregate_psf_artifacts(self):
        with tempfile.TemporaryDirectory() as root:
            result = {"history_name": "CRESCENDO_h1", "results_dir": root}
            point_root = os.path.join(root, "1", "OPA_AC")
            netlist_dir = os.path.join(point_root, "netlist")
            psf_dir = os.path.join(point_root, "psf")
            os.makedirs(netlist_dir)
            os.makedirs(psf_dir)
            with open(os.path.join(netlist_dir, "input.scs"), "w") as stream:
                stream.write("simulator lang=spectre\n")
            with open(os.path.join(psf_dir, "spectre.out"), "w") as stream:
                stream.write("spectre completes with 0 errors\n")
            with open(os.path.join(psf_dir, ".simDone"), "w"):
                pass
            aggregate = os.path.join(root, "psf", "OPA_AC", "psf")
            os.makedirs(aggregate)
            paths = locate_adexl_point_result(result, 1, "OPA_AC", require_complete=True)
            self.assertEqual(paths["point_root"], point_root)
            self.assertEqual(paths["psf_dir"], psf_dir)
            self.assertNotEqual(paths["psf_dir"], aggregate)
            with self.assertRaisesRegex(AdeXLResultError, "invalid ADE XL test_name"):
                locate_adexl_point_result(result, 1, "../OPA_AC")

    def test_rdb_completion_requires_every_expected_corner_and_success_status(self):
        parsed = {
            "points": [
                {"pointID": 1, "corner_name": "TT"},
                {"pointID": 2, "corner_name": "FF"},
            ],
            "test_statuses": [
                {"pointID": 1, "test_name": "OPA_AC", "statusCode": 3, "status_message": "done"},
                {"pointID": 2, "test_name": "OPA_AC", "statusCode": 3, "status_message": "done"},
            ],
        }
        summary = validate_adexl_rdb_completion(
            parsed, expected_tests=["OPA_AC"], expected_corners=["TT", "FF"],
        )
        self.assertTrue(summary["ok"])
        with self.assertRaisesRegex(AdeXLResultError, "missing enabled corners"):
            validate_adexl_rdb_completion(
                parsed, expected_tests=["OPA_AC"], expected_corners=["TT", "FF", "SS"],
            )
        parsed["test_statuses"][1]["statusCode"] = 2
        with self.assertRaisesRegex(AdeXLResultError, "failed or incomplete"):
            validate_adexl_rdb_completion(
                parsed, expected_tests=["OPA_AC"], expected_corners=["TT", "FF"],
            )

    def test_shutdown_uses_safe_close_and_fences_future_candidates(self):
        with tempfile.TemporaryDirectory() as root:
            setup = _setup(root)
            runtime_root = os.path.join(root, "runtime")
            builder = AdeXLRuntimeBuilder(setup, runtime_root, epoch="epoch-d")
            shutdown = builder.build_shutdown()
            with open(shutdown["script_path"]) as stream:
                script = stream.read()
            self.assertIn("crescAxlSafeClose()", script)
            self.assertIn("CLOSE_DONE", script)
            with self.assertRaisesRegex(AdeXLRuntimeError, "after ADE XL shutdown"):
                builder.build_candidate("late", os.path.join(runtime_root, "late"))

    def test_capability_probe_and_parser(self):
        with tempfile.TemporaryDirectory() as root:
            report = os.path.join(root, "capabilities.tsv")
            script = render_adexl_capability_probe(report)
            self.assertIn("isCallable('axlCreateSession)", script)
            self.assertTrue(script.rstrip().endswith("exit()"))
            with open(report, "w") as stream:
                stream.write("API\taxlCreateSession\tt\trequired\n")
                stream.write("API\taxlGetRunStatus\tnil\toptional\n")
                stream.write("partial")
            self.assertEqual(parse_adexl_capabilities(report), [
                {"name": "axlCreateSession", "available": True, "requirement": "required"},
                {"name": "axlGetRunStatus", "available": False, "requirement": "optional"},
            ])


if __name__ == "__main__":
    unittest.main()
