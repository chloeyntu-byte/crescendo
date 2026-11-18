import hashlib
from pathlib import Path
import time
import unittest

from crescendo_engine import completion
from crescendo_engine.native_diagnostics import library_init_console_proofs, skill_command_diagnostics
from crescendo_engine.private_netlist import _completion_worker_log
from tests import test_completion


class NativeCompletionDiagnosticsTests(unittest.TestCase):
    def setUp(self):
        self.fixture = test_completion.CompletionTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.command = self.fixture.command
        self.control = Path(self.fixture.candidate_root, "native_baseline")
        self.control.mkdir()
        self.console = Path(__file__).with_name("fixtures").joinpath("library_init_native_completion.txt").read_text()
        self.error = self.console.splitlines()[4]
        self.states = [("RUN_BEGIN", ""), ("RUN_ID", "0"), ("SIM_DONE", ""), ("WAIT_RETURN", "nil"),
                       ("HISTORY", self.fixture.history), ("CLOSED", "")]
        self.write_protocol()

    def write_protocol(self):
        nonce = "a" * 32
        path = self.control / "native_baseline_protocol.tsv"
        raw = "".join("%s\t%s\t%s\n" % (nonce, *row) for row in self.states).encode()
        path.write_bytes(raw)
        now = time.time()
        self.command.update(schema_version="crescendo.maestro_baseline.v1", candidate_id="fixture",
                            execution_simulator="spectre", run_id="0", guardian_returncode=0,
                            cleanup_pending=False, rdb_path=self.fixture.rdb_path,
                            started_epoch=now - 1, finished_epoch=now)
        self.command["native_baseline"] = dict(
            schema_version=self.command["schema_version"], candidate_id="fixture",
            history_name=self.fixture.history, run_id="0", nonce=nonce, protocol_path=str(path),
            request_root=self.fixture.candidate_root, protocol_sha256=hashlib.sha256(raw).hexdigest(),
            records=[dict(state=s, value=v, sequence=i) for i, (s, v) in enumerate(self.states)],
            started_epoch=now - 1, finished_epoch=now, native_session_closed=True, local_cleanup_proven=True)

    def validate(self, worker_errors=()):
        (self.control / "ocean.stdout").write_text(self.console)
        script = str(self.control / "startup.il")
        diagnostics = skill_command_diagnostics({"returncode": 0}, script, simulators=("spectre",),
                                                native_completion=True)
        logs = [_completion_worker_log(list(worker_errors), "Job0.log", 0, True,
                                       library_init_console_proofs(diagnostics, exact=True), script, ("spectre",))]
        return completion.validate_candidate_completion(
            self.fixture.workspace, self.fixture.candidate_root, self.command, self.fixture.parsed,
            native_diagnostics=diagnostics, native_worker_diagnostics={"logs": logs})

    def test_proof_allows_exact_console_worker_and_rdb_echoes(self):
        (self.control / "ocean.stderr").write_text(self.error + "\n")
        self.fixture.parsed["test_statuses"][0]["status_message"] = self.error
        result = self.fixture.parsed["output_map"]["ac_test::gain"]["values"][0]
        result.update(error=self.error, first_error=self.error)
        evidence = self.validate([r"\e " + self.error])
        self.assertTrue(evidence["ok"])
        warnings = evidence["library_init_warnings"]
        self.assertEqual([w["source"] for w in warnings], ["ocean", "ocean", "worker", "rdb", "rdb"])
        self.assertEqual(warnings[0]["console_line"], 5)
        self.assertEqual(warnings[1]["related_console_line"], 5)
        self.assertEqual(warnings[2]["message"], r"\e " + self.error)
        self.assertIsNone(result["first_error"])

    def test_missing_stack_and_different_worker_error_are_fatal(self):
        with self.assertRaisesRegex(completion.CompletionEvidenceError, "Job0.log:1:.*asiEnvGetVar"):
            self.validate([r"\e " + self.error + " "])
        self.console = self.console.replace('loadi("fixture_library/libInit.il")', 'loadi("other.il")')
        with self.assertRaisesRegex(completion.CompletionEvidenceError, "ocean.stdout:5:.*asiEnvGetVar"):
            self.validate()

    def test_nonmatching_worker_stack_is_not_reclassified_by_exact_header(self):
        with self.assertRaisesRegex(completion.CompletionEvidenceError, "Job0.log:1:.*asiEnvGetVar"):
            self.validate([r"\e " + self.error, "", "<<< Stack Trace >>>", 'load("run.il")'])

    def test_missing_return_and_generated_skill_origin_are_fatal(self):
        original = self.console
        for console in (original.replace("Run RETURN", "Run ABORT"),
                        original.replace('loadi("fixture_library/libInit.il")',
                                         'load("run.il")\nloadi("fixture_library/libInit.il")')):
            with self.subTest(console=console):
                self.console = console
                with self.assertRaisesRegex(completion.CompletionEvidenceError, "asiEnvGetVar"):
                    self.validate()
        self.console = original
        self.states.pop(3)
        self.write_protocol()
        with self.assertRaisesRegex(completion.CompletionEvidenceError, "protocol is incomplete"):
            self.validate()

    def test_missing_history_and_incomplete_points_block_downgrade(self):
        self.command["history_name"] = None
        with self.assertRaisesRegex(completion.CompletionEvidenceError, "history identity"):
            self.validate()
        self.command["history_name"] = self.fixture.history
        self.fixture.parsed["test_statuses"][0].update(statusCode=0, status_message="Solver did not converge")
        with self.assertRaisesRegex(completion.CompletionEvidenceError, "statusCode=0: Solver did not converge"):
            self.validate()
        self.fixture.parsed["test_statuses"].pop(0)
        with self.assertRaisesRegex(completion.CompletionEvidenceError, "missing corner"):
            self.validate()

    def test_tool_name_controls_disposition(self):
        self.assertTrue(self.validate()["ok"])
        self.console = self.console.replace("fooS", "spectre")
        with self.assertRaisesRegex(completion.CompletionEvidenceError, "ADE-5066"):
            self.validate()

    def test_single_physical_point_error_does_not_poison_other_partition(self):
        self.command["native_batch_points"] = [("ac_test", 1), ("ac_test", 2), ("ac_test", 9)]
        self.console += ("> ERROR (ASSEMBLER-5011): While simulating run %s, point 9, "
                                                  "test ac_test, received error:\n" % self.fixture.history)
        self.console += ("Simulation Error:\n ------------------------------\n"
                         "ERROR (FIXTURE-1): point-local solver error\n ------------------------------\n")
        status = r"\o *Info* Run start for Point ID (0 9) on testbench [ ac_test ]."
        evidence = self.validate([status, r"\e *Error* point-only failure"])
        self.assertTrue(evidence["ok"])
        self.console = self.console.replace("point 9", "point 1")
        with self.assertRaisesRegex(completion.CompletionEvidenceError, "point 1"):
            self.validate()

    def calculator_block(self, teardown=False, point=1):
        rows = [r"\o *Info* Run start for Point ID (0 1) on testbench [ ac_test ]."]
        if teardown:
            rows.extend([r"\o *Info* Run complete for Point ID (0 1) on testbench [ ac_test ].",
                         r"\o *Info*    Received signal SIGTERM."])
        point_text = "point:'%d' " % point if point is not None else ""
        rows.extend([r"""\e *Error* ("difference" 0 t nil ("*Error* difference: can't handle (nil - nil)"))""",
                     r"\e", r"\e",
                     r"\o ERROR (ADE-2708): Evaluation of output 'gain' failed for " + point_text + "test:'ac_test'",
                     r"\o Expression:'(a - b)'",
                     r"\o because of the following error(s)",
                     r"\o *Error* difference: can't handle (nil - nil).",
                     r"\o Ensure that the definition of output is correct."])
        return rows

    def rdb_calculator_error(self):
        value = self.fixture.parsed["output_map"]["ac_test::gain"]["values"][0]
        value.update(value="", numeric=None, output_status="evaluation_failed",
                     error="Evaluation Error:\n ---\n *Error* Evaluating expression (a - b).")

    def test_calculator_error_requires_exact_rdb_output_and_successful_solver(self):
        self.rdb_calculator_error()
        evidence = self.validate(self.calculator_block())
        first, = evidence["measurement_failures"]
        self.assertEqual((first["output"], first["point_id"], first["corner"]), ("ac_test::gain", 1, "FF"))
        with self.assertRaisesRegex(completion.CompletionEvidenceError, r"nil - nil"):
            self.validate(self.calculator_block(point=2))
        with self.assertRaisesRegex(completion.CompletionEvidenceError, "separate infrastructure error"):
            self.validate(self.calculator_block() + [r"\e *Error* separate infrastructure error"])

    def test_post_signal_calculator_block_with_finite_point_is_teardown(self):
        value = self.fixture.parsed["output_map"]["ac_test::gain"]["values"][0]
        value.update(value=10.0)
        evidence = self.validate(self.calculator_block(teardown=True, point=None))
        self.assertNotIn("measurement_failures", evidence)
        self.assertEqual({w["output"] for w in evidence["measurement_teardown_warnings"]}, {"ac_test::gain"})
        with self.assertRaisesRegex(completion.CompletionEvidenceError, r"nil - nil"):
            self.validate(self.calculator_block(point=None))
        self.rdb_calculator_error()
        with self.assertRaisesRegex(completion.CompletionEvidenceError, r"nil - nil"):
            self.validate(self.calculator_block(teardown=True, point=None))

    def test_spectre_signal_is_fatal_even_with_calculator_errors_and_rdb_done(self):
        self.rdb_calculator_error()
        log = next(Path(self.fixture.simulation_root).glob("**/1/ac_test/psf/spectre.out"))
        log.write_text("Spectre received SIGTERM sent from PID: 312 with UID: 42.\n"
                       "    ERROR (SPECTRE-25): Killed by user.\n"
                       "spectre completes with 1 error, 0 warnings, and 0 notices.\n")
        with self.assertRaisesRegex(completion.CompletionEvidenceError,
                                    r"terminated externally \(SPECTRE-25\); SIGTERM from PID 312 UID 42"):
            self.validate(self.calculator_block())

    def test_retryable_directory_error_is_owned_and_has_no_competing_error(self):
        directory = str(Path(self.fixture.rdb_path).parent / ".tmpADEDir_tester/simulation/TB")
        error = r"\e *Error* Could not create directory: %s." % directory
        with self.assertRaises(completion.NativeTemporaryDirectoryError):
            self.validate([error])
        for errors in ([error, r"\e *Error* unrelated failure"],
                       [error.replace(directory, "/tmp/.tmpADEDir_tester/simulation/TB")]):
            with self.subTest(errors=errors), self.assertRaises(completion.CompletionEvidenceError) as failure:
                self.validate(errors)
            self.assertNotIsInstance(failure.exception, completion.NativeTemporaryDirectoryError)

    def test_native_close_waits_for_every_spectre_footer_without_accepting_solver_errors(self):
        from unittest import mock
        from crescendo_engine.native_baseline import _drain_native_spectre

        statuses = self.fixture.parsed["test_statuses"]
        log = next(Path(self.fixture.simulation_root).glob("**/2/ac_test/psf/spectre.out"))
        log.write_text("Spectre is closing files.\n")
        with mock.patch("crescendo_engine.rdb.read_rdb_test_statuses", return_value=statuses):
            evidence = _drain_native_spectre(self.fixture.workspace, self.fixture.rdb_path,
                                             self.fixture.history, None, timeout=0)
            self.assertEqual(evidence, {"terminal_logs_seen": False, "pending_logs": 1})
            with mock.patch("crescendo_engine.native_baseline.time.sleep",
                            side_effect=lambda _: log.write_text("spectre completes with 1 error.\n")):
                evidence = _drain_native_spectre(self.fixture.workspace, self.fixture.rdb_path,
                                                 self.fixture.history, None)
            self.assertEqual(evidence, {"terminal_logs_seen": True})
        with self.assertRaisesRegex(completion.CompletionEvidenceError, "Spectre reported 1 error"):
            self.validate()

    def test_explicit_nil_result_is_distinct_from_empty_value(self):
        value = self.fixture.parsed["output_map"]["ac_test::gain"]["values"][0]
        value.update(value="", numeric=None, output_status="evaluation_failed",
                     error="Evaluation Error:\n ---\n Expression returned nil.\n ---")
        worker = [r"\o *Info* Run start for Point ID (0 1) on testbench [ ac_test ].",
                  r"\o ERROR (ADE-2709): Evaluation of output 'gain' returned 'nil' for point:'1' test:'ac_test'",
                  r"\o Expression:'nil'.", r"\o Ensure that the definition of output is correct."]
        self.assertEqual(self.validate(worker)["measurement_failures"][0]["output"], "ac_test::gain")
        value.update(error=None, output_status="not_collected")
        with self.assertRaisesRegex(completion.CompletionEvidenceError, "ADE-2709"):
            self.validate(worker)

    def test_uncoded_termination_cannot_be_hidden_by_zero_error_footer(self):
        log = next(Path(self.fixture.simulation_root).glob("**/1/ac_test/psf/spectre.out"))
        log.write_text("Spectre received SIGTERM sent from PID: 312 with UID: 42.\n"
                       "Killed by user.\n"
                       "spectre completes with 0 errors, 0 warnings, and 0 notices.\n")
        with self.assertRaisesRegex(completion.CompletionEvidenceError,
                                    "terminated externally; SIGTERM from PID 312 UID 42"):
            self.validate()
