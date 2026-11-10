"""Phase/state fixtures and real owned local processes; no site qualification."""
import concurrent.futures
import copy
import json
import os
import re
import shlex
import threading
import time
import unittest
from unittest import mock

from crescendo_engine import netlist_execution as execution
from crescendo_engine.job_policy import (normalize_job_overrides, job_timeout_policy,
    timeout_override_properties, timeout_override_skill_arguments, timeout_override_skill_readback,
    effective_job_policy, job_policy_override_definitions)
import test_netlist_execution as fixtures


class TimeoutPolicyTests(unittest.TestCase):
    def test_overrides_are_seconds_and_disabled_is_explicit(self):
        settings = {"timeouts": {"start_seconds": 120, "configure_seconds": -1,
                                  "run_seconds": 7200, "linger_seconds": -1}}
        self.assertEqual(normalize_job_overrides(settings), settings)
        self.assertEqual(timeout_override_properties(settings), {
            "starttimeout": "120", "configuretimeout": "-1", "runtimeout": "7200", "lingertimeout": "-1"})
        for key, value in (("start_seconds", 0), ("run_seconds", 1.5), ("run_seconds", True),
                           ("configuredtimeout", 100), ("configure_seconds", -2), ("run_seconds", "12"), ("linger_seconds", 0)):
            with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                normalize_job_overrides({"timeouts": {key: value}})

    def test_manual_blank_is_unlimited_for_every_timeout_and_zero_never_works(self):
        for field in ("start_seconds", "configure_seconds", "run_seconds", "linger_seconds"):
            for blank in (None, "", "   "):
                self.assertEqual(normalize_job_overrides({"timeouts": {field: blank}}), {"timeouts": {field: -1}})
                policy = job_timeout_policy({}, {"timeouts": {field: blank}})
                self.assertFalse(policy["fields"][field]["enabled"])
                self.assertEqual(policy["fields"][field]["status"], "disabled")
            with self.assertRaises(ValueError):
                normalize_job_overrides({"timeouts": {field: 0}})
            native = {"start_seconds": "starttimeout", "configure_seconds": "configuretimeout",
                      "run_seconds": "runtimeout", "linger_seconds": "lingertimeout"}[field]
            self.assertEqual(job_timeout_policy({native: "0"})["fields"][field]["status"], "unknown")

    def test_source_disabled_unknown_and_unqualified_name_stay_distinct(self):
        policy = job_timeout_policy({"starttimeout": "0", "configuretimeout": None,
                                    "runtimeout": "-1", "lingertimeout": "300", "configuredtimeout": "11"})
        self.assertEqual(policy["fields"]["start_seconds"]["status"], "unknown")
        self.assertEqual(policy["fields"]["configure_seconds"]["status"], "disabled")
        self.assertEqual(policy["fields"]["run_seconds"]["status"], "disabled")
        self.assertEqual(policy["unqualified_properties"], {"configuredtimeout": "11"})
        missing = job_timeout_policy({"configuredtimeout": "11"})
        self.assertEqual(missing["fields"]["configure_seconds"]["status"], "unknown")

    def test_manual_disable_wins_over_a_positive_source_and_fallback(self):
        policy = job_timeout_policy({"runtimeout": "300"}, {"timeouts": {"run_seconds": -1}},
                                    {"run_seconds": 600})
        value = policy["fields"]["run_seconds"]
        self.assertFalse(value["enabled"])
        self.assertIsNone(value["seconds"])
        self.assertEqual(value["source"], "manual_override")

    def test_native_timeout_override_is_same_private_policy_and_read_back(self):
        overrides = {"timeouts": {"run_seconds": -1, "linger_seconds": None}}
        self.assertIn('?runtimeout "-1"', timeout_override_skill_arguments(overrides))
        self.assertIn('equal(getq(actual runtimeout) "-1")', timeout_override_skill_readback(overrides, "actual"))
        self.assertIn('original->runtimeout = "-1"', job_policy_override_definitions(overrides))
        source = {"global": {"available": True, "properties": {"runtimeout": "5"}}, "tests": []}
        before = copy.deepcopy(source)
        effective = effective_job_policy(source, overrides)
        self.assertEqual(effective["global"]["properties"]["runtimeout"], "-1")
        self.assertEqual(source, before)

    def clock(self, start=100, configure=20, run=10):
        policy = job_timeout_policy({}, fallbacks={"start_seconds": start,
            "configure_seconds": configure, "run_seconds": run, "linger_seconds": -1})
        return execution.TaskTimeoutClock(policy, 1000, job_id="10000002")

    def test_long_pending_is_unlimited_and_running_starts_run_budget(self):
        clock = self.clock()
        clock.observe({"job_id": "10000002", "state": "PENDING"}, 100000)
        self.assertIsNone(clock.expired(100000))
        self.assertFalse(clock.snapshot(100000)["enabled"])
        clock.observe({"job_id": "10000002", "state": "RUNNING"}, 100001)
        self.assertEqual(clock.snapshot(100005)["phase"], "run")
        self.assertIsNone(clock.expired(100010))
        self.assertEqual(clock.expired(100011)["phase"], "run")

    def test_requeued_pending_pauses_but_repeated_running_accumulates(self):
        clock = self.clock()
        clock.observe({"job_id": "10000002", "state": "RUNNING"}, 1010)
        clock.observe({"job_id": "10000002", "state": "PENDING"}, 1014)
        self.assertIsNone(clock.expired(10000))
        self.assertEqual(clock.snapshot(10000)["running_elapsed_seconds"], 4)
        clock.observe({"job_id": "10000002", "state": "RUNNING"}, 10001)
        clock.observe({"job_id": "10000002", "state": "RUNNING"}, 10004)
        self.assertIsNone(clock.expired(10006))
        self.assertEqual(clock.expired(10007)["running_elapsed_seconds"], 10)

    def test_wrong_id_and_failed_query_do_not_start_a_running_clock(self):
        clock = self.clock(start=30)
        for observation in ({"job_id": "10000001", "state": "RUNNING"},
                            {"job_id": "10000002", "state": "RUNNING", "reason": "scheduler_query_failed"},
                            {"job_id": "10000002", "state": "RUNNING", "invalid": True}):
            clock.observe(observation, 1020)
        self.assertIsNone(clock.expired(100000))
        self.assertEqual(clock.snapshot(100000)["phase"], "pending")

    def test_terminal_receipt_stops_clock_before_guardian_reaping(self):
        clock = self.clock(run=1)
        clock.started_receipt(1000.1)
        clock.terminal_receipt(1000.3)
        self.assertIsNone(clock.expired(2000))
        self.assertTrue(clock.snapshot(2000)["solver_finished"])
        self.assertAlmostEqual(clock.snapshot(2000)["phase_elapsed_seconds"], 0.2)

    def test_disabled_run_does_not_expire(self):
        clock = self.clock(run=-1)
        clock.started_receipt(1001)
        self.assertIsNone(clock.expired(99999999))
        self.assertFalse(clock.snapshot(99999999)["enabled"])


class ExecutionCapacityAndTimerTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.NetlistExecutionTests()
        self.fixture.setUp()

    def tearDown(self):
        self.fixture.tearDown()

    def test_cluster_uses_parallel_default_and_manual_one_is_respected(self):
        self.fixture.config['runtime']['execution_commands'] = {
            'scheduler_query_template': 'batch-query {job_id}',
            'scheduler_cancel_template': 'batch-cancel {job_id}'}
        original = {"distributionmethod": "Command", "jobsubmitcommand": "batch-submit", "maxjobs": "1"}
        self.fixture.prepared["job_policy"]["global"]["properties"] = original
        self.fixture.prepared["job_policy"]["tests"] = [{"name": "rc", "properties": dict(original)}]
        policy = execution.execution_policy(self.fixture.prepared, self.fixture.spec, self.fixture.config)
        self.assertEqual(policy["max_jobs"], 5)
        self.assertEqual(policy["tests"]["rc"]["effective_solver_max_jobs"], 5)
        self.assertEqual(policy["capacity_source"], "cr_cluster_parallel_default")
        manual = execution.execution_policy(self.fixture.prepared,
            dict(self.fixture.spec, maestro_job_overrides={"max_jobs": 1}), self.fixture.config)
        self.assertEqual(manual["max_jobs"], 1)
        self.assertEqual(manual["tests"]["rc"]["effective_solver_max_jobs"], 1)
        self.assertEqual(original["maxjobs"], "1")

    def test_cluster_capacity_is_shared_across_test_and_candidate_threads(self):
        self.fixture.config['runtime']['execution_commands'] = {
            'scheduler_query_template': 'batch-query {job_id}',
            'scheduler_cancel_template': 'batch-cancel {job_id}'}
        self.fixture.prepared["job_policy"]["global"]["properties"].update(
            distributionmethod="Command", jobsubmitcommand="batch-submit", maxjobs="3")
        self.fixture.prepared["job_policy"]["tests"] = [{"name": "limited", "properties": {"maxjobs": "2"}}]
        context = self.fixture.context()
        lock, active, peak, limited, limited_peak = threading.Lock(), [0], [0], [0], [0]
        def solve(manifest, task, timeout, cancelled):
            with lock:
                active[0] += 1; peak[0] = max(peak[0], active[0])
                if task["test"] == "limited":
                    limited[0] += 1; limited_peak[0] = max(limited_peak[0], limited[0])
            time.sleep(0.05)
            with lock:
                active[0] -= 1
                if task["test"] == "limited": limited[0] -= 1
        with mock.patch.object(context, "_execute_task_owned", side_effect=solve), \
                concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
            futures = [pool.submit(context._execute_task, {"candidate_id": "c%d" % i},
                {"test": "limited" if i % 2 else "other"}, 10, None) for i in range(12)]
            for future in futures: future.result()
        self.assertEqual(peak[0], 3)
        self.assertEqual(limited_peak[0], 2)

    def test_original_disabled_run_is_not_silently_replaced_by_candidate_budget(self):
        self.fixture.prepared["job_policy"]["global"]["properties"]["runtimeout"] = "-1"
        context = self.fixture.context()
        result = context.evaluate(self.fixture.manifest)
        state = result["task_results"][0]["timeout_state"]
        self.assertFalse(state["enabled"])
        self.assertIsNone(state["limit_seconds"])
        wrapper = os.path.join(self.fixture.task["cwd"], "execution", "execute.sh")
        with open(wrapper) as stream: text = stream.read()
        self.assertIn("solver run timeout disabled", text)
        self.assertIn("cr_heartbeat", text)
        self.assertIn("cr_cancel", text)

    def test_unknown_zero_blocks_before_task_launch_until_explicit_override(self):
        self.fixture.prepared["job_policy"]["global"]["properties"]["runtimeout"] = "0"
        with self.assertRaises(execution.NetlistExecutionError) as caught:
            self.fixture.context()
        self.assertIn("run_seconds", caught.exception.detail["unknown_timeout_fields"])
        self.fixture.spec["maestro_job_overrides"] = {"timeouts": {"run_seconds": -1}}
        context = self.fixture.context()
        self.assertFalse(context.policy["global"]["timeouts"]["fields"]["run_seconds"]["enabled"])

    def test_native_start_configure_linger_are_not_cli_queue_limits(self):
        self.fixture.prepared["job_policy"]["global"]["properties"].update(
            starttimeout="0", configuretimeout="0", lingertimeout="0", runtimeout="-1")
        context = self.fixture.context()
        fields = context.policy["global"]["timeouts"]["fields"]
        self.assertFalse(fields["start_seconds"]["applicable"])
        self.assertFalse(fields["configure_seconds"]["applicable"])
        self.assertFalse(fields["linger_seconds"]["applicable"])
        self.assertEqual(fields["start_seconds"]["status"], "unknown")

    def test_candidate_blank_disables_fallback_and_measurement_but_zero_is_rejected(self):
        self.fixture.spec["candidate_timeout_seconds"] = -1
        measured = []
        def evaluator(*args, **kwargs):
            measured.append(kwargs["timeout_seconds"])
            return self.fixture.evaluator(*args, **kwargs)
        context = self.fixture.context(measurement_evaluator=evaluator)
        result = context.evaluate(self.fixture.manifest)
        self.assertEqual(measured, [None])
        self.assertFalse(result["task_results"][0]["timeout_state"]["enabled"])
        with self.assertRaises(execution.NetlistExecutionError):
            context.evaluate(self.fixture.manifest, timeout_seconds=0)

    def test_remote_wrapper_uses_shared_run_clock_and_retains_heartbeat_cancel(self):
        self.fixture.config['runtime']['execution_commands'] = {
            'scheduler_query_template': 'batch-query {job_id}',
            'scheduler_cancel_template': 'batch-cancel {job_id}'}
        self.fixture.spec['command_scheduler'] = 'receipt'
        self.fixture.prepared["job_policy"]["global"]["properties"].update(
            distributionmethod="Command", jobsubmitcommand="/bin/sh", runtimeout="1")
        context = self.fixture.context()
        result = context.evaluate(self.fixture.manifest)
        self.assertTrue(result["task_results"][0]["submission_command_plan"]["remote"])
        wrapper = os.path.join(self.fixture.task["cwd"], "execution", "execute.sh")
        with open(wrapper) as stream: text = stream.read()
        self.assertIn("solver run timeout disabled", text)
        self.assertIn("cr_heartbeat", text)
        self.assertIn("cr_cancel", text)
        self.assertNotIn('cr_now-cr_start))" -ge', text)

    def test_local_finite_run_budget_terminates_owned_sleep(self):
        self.fixture.spec["maestro_job_overrides"] = {"timeouts": {"run_seconds": 1}}
        task = self.fixture.make_task("local_timeout", delay=30)
        context = self.fixture.context()
        with self.assertRaises(execution.NetlistExecutionError) as caught:
            context.evaluate(dict(self.fixture.manifest, tasks=[task]))
        self.assertTrue(caught.exception.detail["timed_out"])
        self.assertEqual(caught.exception.detail["timeout_state"]["expiration"]["phase"], "run")
        self.assertTrue(context.close()["cleanup_proven"])

    def test_explicit_scheduler_script_uses_owned_id_for_query_and_cancel(self):
        binaries = self.fixture._positional_scheduler()
        wrapper = os.path.join(self.fixture.root, "submit_wrapper")
        with open(wrapper, "w") as stream: stream.write("#!/bin/sh\nexit 0\n")
        os.chmod(wrapper, 0o700)
        self.fixture.spec["command_scheduler"] = "batch"
        calls = []
        def runner(argv, **kwargs):
            calls.append(argv)
            return {"returncode": 0, "stdout_tail": "JOB_ID 10000002\nJOB_STATE CANCELLED\n"}
        context = self.fixture.context(command_runner=runner)
        policy = {"submit_argv": [wrapper]}
        stopped = context._scheduler_cancel(policy,
            {"stdout_tail": "JOBID MESSAGE\n10000002 Submit job successfully.\n"}, self.fixture.root, os.environ)
        self.assertTrue(stopped["remote_stop_proven"])
        self.assertEqual(calls, [[binaries["batch-cancel"], "10000002"], [binaries["batch-query"], "10000002"]])
        self.assertFalse(context._scheduler_cancel(policy, {"stdout_tail": "accepted"}, self.fixture.root, os.environ)["attempted"])

    def test_receipt_backend_does_not_borrow_scheduler_profile_for_opaque_script(self):
        self.fixture._positional_scheduler()
        self.fixture.spec["command_scheduler"] = "receipt"
        context = self.fixture.context(command_runner=mock.Mock())
        stopped = context._scheduler_cancel({"submit_argv": ["/explicit/opaque/script"]},
            {"stdout_tail": "JOBID MESSAGE\n10000002 accepted\n"}, self.fixture.root, os.environ)
        self.assertFalse(stopped["attempted"])
        context.command_runner.assert_not_called()

    def test_pending_manual_stop_and_running_timeout_cancel_only_owned_id(self):
        binaries = self.fixture._positional_scheduler()
        for state in ("PENDING", "RUNNING"):
            with self.subTest(state=state):
                self.fixture.spec["maestro_job_overrides"] = {"command": shlex.quote(binaries["batch-submit"]),
                    "timeouts": {"start_seconds": 1, "configure_seconds": 1, "run_seconds": 1}}
                killed, calls = [False], []
                def runner(argv, **kwargs):
                    calls.append(argv)
                    if kwargs.get("guardian_launch_lock_fd") is not None:
                        os.close(kwargs["guardian_launch_lock_fd"])
                        return {"returncode": 0, "stdout_tail": "JOBID MESSAGE\n10000002 Submit job successfully.\n"}
                    if argv[0] == binaries["batch-cancel"]:
                        killed[0] = True
                        return {"returncode": 0, "stdout_tail": "cancel acknowledged"}
                    self.assertEqual(argv, [binaries["batch-query"], "10000002"])
                    observed = "CANCELLED" if killed[0] else state
                    return {"returncode": 0, "stdout_tail": "JOB_ID 10000002\nJOB_STATE " + observed + "\n"}
                context = self.fixture.context(command_runner=runner)
                task = self.fixture.make_task("pending" if state == "PENDING" else "running")
                manifest = dict(self.fixture.manifest, candidate_id=state.lower(), tasks=[task])
                ticks = [1000.0]
                def monotonic():
                    ticks[0] += 0.1
                    return ticks[0]
                with mock.patch.object(execution.time, "monotonic", side_effect=monotonic), \
                        mock.patch.object(execution.time, "sleep"), self.assertRaises(execution.NetlistExecutionError) as caught:
                    context.evaluate(manifest, cancel_check=lambda: state == "PENDING" and ticks[0] >= 1005)
                self.assertTrue(killed[0])
                if state == "PENDING":
                    self.assertIsNone(caught.exception.detail["timeout_state"]["expiration"])
                else:
                    self.assertEqual(caught.exception.detail["timeout_state"]["expiration"]["phase"], "run")
                self.assertTrue(caught.exception.detail["scheduler_stop"]["remote_stop_proven"])
                self.assertEqual([call for call in calls if call[0] == binaries["batch-cancel"]],
                                 [[binaries["batch-cancel"], "10000002"]])
                self.assertTrue(context.close()["cleanup_proven"])

    def test_scheduler_stop_waits_for_asynchronous_terminal_within_budget(self):
        binaries = self.fixture._positional_scheduler()
        self.fixture.config["runtime"]["cluster_stop_timeout_seconds"] = 8
        states, calls = iter(("KILLING", "STOPPING", "CANCELLED")), []
        def runner(argv, **kwargs):
            calls.append(argv)
            if argv[0] == binaries["batch-cancel"]:
                return {"returncode": 0, "stdout_tail": "cancel accepted"}
            return {"returncode": 0, "stdout_tail": "JOB_ID 10000002\nJOB_STATE " + next(states) + "\n"}
        context = self.fixture.context(command_runner=runner)
        with mock.patch.object(execution.time, "sleep"):
            proof = context._scheduler_cancel({"submit_argv": [binaries["batch-submit"]]},
                {"stdout_tail": "JOBID MESSAGE\n10000002 accepted\n"}, self.fixture.root, os.environ)
        self.assertTrue(proof["remote_stop_proven"])
        self.assertEqual(proof["query_count"], 3)
        self.assertEqual(proof["timeout_seconds"], 8)
        self.assertEqual(calls[0], [binaries["batch-cancel"], "10000002"])

    def test_running_timeout_without_solver_start_and_late_cancel_receipt_is_infrastructure_failure(self):
        binaries = self.fixture._positional_scheduler()
        for state, phase in (("RUNNING", "run"),):
            with self.subTest(state=state):
                task = self.fixture.make_task("race_" + state.lower())
                control = os.path.join(task["cwd"], "execution")
                self.fixture.spec["maestro_job_overrides"] = {"command": shlex.quote(binaries["batch-submit"]),
                    "timeouts": {"start_seconds": 1, "configure_seconds": 1, "run_seconds": 1}}
                killed = [False]
                def runner(argv, **kwargs):
                    if kwargs.get("guardian_launch_lock_fd") is not None:
                        os.close(kwargs["guardian_launch_lock_fd"])
                        return {"returncode": 0, "stdout_tail": "JOBID MESSAGE\n10000002 accepted\n"}
                    if argv[0] == binaries["batch-cancel"]:
                        killed[0] = True
                        with open(os.path.join(control, "execute.sh")) as stream:
                            token = re.search(r"(?m)^cr_token=([0-9a-f]{32})$", stream.read()).group(1)
                        with open(os.path.join(control, "terminal.receipt"), "w") as stream:
                            stream.write(execution.RECEIPT + "\t" + token + "\texited\t143\t1\towned_fixture_compute\t0\n")
                        return {"returncode": 0, "stdout_tail": "cancel acknowledged"}
                    return {"returncode": 0, "stdout_tail": "Job 10000002 not found\n" if killed[0] else "JOB_ID 10000002\nJOB_STATE " + state + "\n"}
                context = self.fixture.context(command_runner=runner)
                ticks = [1000.0]
                def monotonic():
                    ticks[0] += 0.1
                    return ticks[0]
                manifest = dict(self.fixture.manifest, candidate_id="race_" + state.lower(), tasks=[task])
                with mock.patch.object(execution.time, "monotonic", side_effect=monotonic), \
                        mock.patch.object(execution.time, "sleep"), self.assertRaises(execution.NetlistExecutionError) as caught:
                    context.evaluate(manifest)
                self.assertTrue(caught.exception.infrastructure)
                self.assertEqual(caught.exception.detail["timeout_state"]["expiration"]["phase"], phase)
                self.assertEqual(caught.exception.detail["receipt"]["returncode"], 143)
                self.assertTrue(context.close()["cleanup_proven"])

    def test_stop_deadline_and_disappearance_keep_remote_cleanup_unproven(self):
        binaries = self.fixture._positional_scheduler()
        self.fixture.config["runtime"]["cluster_stop_timeout_seconds"] = 2
        for query_text in ("JOB_ID 10000002\nJOB_STATE KILLING\n", "Job 10000002 not found\n"):
            def runner(argv, **kwargs):
                return {"returncode": 0, "stdout_tail": "cancel accepted" if argv[0] == binaries["batch-cancel"] else query_text}
            context = self.fixture.context(command_runner=runner)
            ticks = [1000.0]
            def monotonic():
                ticks[0] += 0.1
                return ticks[0]
            with mock.patch.object(execution.time, "monotonic", side_effect=monotonic), mock.patch.object(execution.time, "sleep"):
                proof = context._scheduler_cancel({"submit_argv": [binaries["batch-submit"]]},
                    {"stdout_tail": "JOBID MESSAGE\n10000002 accepted\n"}, self.fixture.root, os.environ)
            self.assertFalse(proof["remote_stop_proven"])
            row = {"remote": True, "token": "a" * 32, "scheduler_job_id": "10000002", "scheduler_stop": proof,
                   "submission_observation": {"accepted": True, "job_id": "10000002"},
                   "task": {"task_id": "a"}, "files": {}}
            self.assertFalse(execution.task_termination_evidence(row, None)["cleanup_proven"])


if __name__ == "__main__":
    unittest.main()
