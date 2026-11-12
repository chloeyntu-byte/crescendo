"""P0 synthetic binding, bounded resource and owned reader regressions."""
import concurrent.futures
import copy
import json
import os
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

from crescendo_engine.netlist_execution import NetlistExecutionContext, execution_policy
from crescendo_engine import netlist_measurement as reader
from crescendo_engine.execution_settings import freeze_execution_settings, normalize_execution_settings, READER_BATCH, READER_SERIAL
from crescendo_engine.netlist_optimizer import PrivateNetlistOptimizer
from crescendo_engine.utils import load_json


class ResourceSettingsTests(unittest.TestCase):
    def prepared(self, mode="Local", global_cap=4, test_cap=4):
        return {"job_policy": {"global": {"properties": {"distributionmethod": mode, "maxjobs": str(global_cap), "jobsubmitcommand": "/bin/sh"}}, "tests": [{"name": "T", "properties": {"maxjobs": str(test_cap)}}]}}

    def test_local_and_command_capacity_actual_fake_solver_peaks(self):
        for mode, requested, test_cap, expected in (("Local", 1, 4, 1), ("Local", 3, 4, 3), ("Local", 8, 2, 2), ("Command", 1, 4, 4), ("Command", 1, 2, 2)):
            context = NetlistExecutionContext({"runtime": {"execution_commands": {"scheduler_query_template": "query {job_id}", "scheduler_cancel_template": "cancel {job_id}"}}}, self.prepared(mode, 4, test_cap), {"local_solver_max_jobs": requested})
            try:
                lock, counts = threading.Lock(), [0, 0]
                def solve(manifest, task, *args):
                    with lock:
                        counts[0] += 1
                        counts[1] = max(counts[1], counts[0])
                    identity = {"candidate_id": "g000_c0000", "task_id": task["task_id"]}
                    context._emit(dict(identity, stage="solve"))
                    time.sleep(0.04)
                    context._emit(dict(identity, stage="solver_terminal"))
                    with lock:
                        counts[0] -= 1
                    return True
                context._execute_task_owned = solve
                with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
                    list(pool.map(lambda index: context._execute_task({}, {"test": "T", "task_id": str(index)}, 1, None), range(8)))
                self.assertEqual(counts[1], expected)
                summary = context.generation_capacity(0)
                self.assertEqual(summary["observed_peak"]["solver"], expected)
                self.assertEqual(summary["capacity"]["tests"]["T"]["effective"], expected)
                self.assertEqual(summary["capacity"]["tests"]["T"]["requested"], 4 if mode == "Command" else requested)
                print("P0_CAPACITY mode=%s requested=%d G=4 T=%d observed=%d" % (mode, requested, test_cap, counts[1]))
            finally:
                context.close()

    def test_peak_is_observed_when_work_is_below_capacity(self):
        context = NetlistExecutionContext({}, self.prepared(), {"local_solver_max_jobs": 4})
        try:
            self.assertIsNone(context.generation_capacity(0)["observed_peak"]["solver"])
            def one_point(*args):
                identity = {"candidate_id": "g000_c0000", "task_id": "only"}
                context._emit(dict(identity, stage="solve"))
                context._emit(dict(identity, stage="solver_terminal"))
            context._execute_task_owned = one_point
            context._execute_task({}, {"test": "T"}, 1, None)
            self.assertEqual(context.policy["max_jobs"], 4)
            self.assertEqual(context.generation_capacity(0)["observed_peak"]["solver"], 1)
        finally:
            context.close()

    def test_frozen_settings_and_old_checkpoint_semantics(self):
        config = {"runtime": {"local_solver_max_jobs": 5, "measurement_jobs": 3, "progress_flush_interval_seconds": 0.25}}
        spec = {}
        freeze_execution_settings(spec, copy.deepcopy(config))
        self.assertEqual(spec["reader_protocol"], READER_BATCH)
        from crescendo_engine.config import DEFAULTS
        fresh_cli = {}
        freeze_execution_settings(fresh_cli, copy.deepcopy(DEFAULTS))
        self.assertEqual(fresh_cli['native_batch_schema'], 'crescendo.generation_batch.v1')
        self.assertEqual(fresh_cli['native_candidate_batching'], 'generation')
        self.assertEqual(spec["local_solver_max_jobs"], 5)
        resumed = {}
        freeze_execution_settings(resumed, copy.deepcopy(config), resuming=True)
        self.assertEqual(resumed["reader_protocol"], READER_SERIAL)
        self.assertNotIn('native_candidate_batching', resumed)
        self.assertNotIn('native_batch_schema', resumed)
        self.assertEqual(resumed["local_solver_max_jobs"], 1)
        self.assertIsNone(resumed["measurement_jobs"])
        frozen = dict(spec)
        freeze_execution_settings(frozen, {}, resuming=True)
        self.assertEqual(frozen, spec)
        for key in ("local_solver_max_jobs", "measurement_jobs", "progress_flush_interval_seconds"):
            for value in (True, -1, float("inf")):
                with self.assertRaises(ValueError):
                    normalize_execution_settings({key: value})

    def test_explicit_measurement_capacity_obeys_global_and_reports_both_values(self):
        for settings in ({"measurement_jobs": 20}, {"runtime": {"measurement_jobs": 20}}):
            spec = settings if "measurement_jobs" in settings else {}
            config = settings if "runtime" in settings else {}
            context = NetlistExecutionContext(config, self.prepared(global_cap=3), spec)
            try:
                self.assertEqual(context.measurement_jobs, 3)
                capacity = context.generation_capacity(0)["capacity"]["reader"]
                self.assertEqual(capacity["requested"], 20)
                self.assertEqual(capacity["effective"], 3)
                self.assertEqual(capacity["reason"], "explicit_reader_jobs_capped_by_global")
            finally:
                context.close()

    def test_explicit_reader_reason_only_claims_an_actual_cap(self):
        for requested, reason in ((1, "explicit_reader_jobs"), (4, "explicit_reader_jobs"),
                                  (8, "explicit_reader_jobs_capped_by_global")):
            context = NetlistExecutionContext({}, self.prepared(), {"measurement_jobs": requested})
            try:
                self.assertEqual(context.generation_capacity(0)["capacity"]["reader"]["reason"], reason)
            finally:
                context.close()

    def test_progress_coalesces_snapshots_and_keeps_every_run_log_event(self):
        with tempfile.TemporaryDirectory() as root:
            optimizer = PrivateNetlistOptimizer(root, {"population": 1, "progress_flush_interval_seconds": 100}, {}, [], {}, lambda *args: {})
            path = os.path.join(optimizer.root, "progress.json")
            with mock.patch("crescendo_engine.netlist_optimizer.time.monotonic", return_value=0):
                for index in range(75):
                    optimizer._progress({"stage": "submit", "message": "event %d" % index})
            self.assertEqual(load_json(path)["sequence"], 1)
            self.assertEqual(optimizer.progress["sequence"], 75)
            with open(os.path.join(root, "crescendo.log")) as stream:
                events = [json.loads(line) for line in stream]
            self.assertEqual([row["message"] for row in events], ["event %d" % index for index in range(75)])

    def test_production_terminal_events_flush_without_waiting_for_interval(self):
        from tests.test_netlist_execution import NetlistExecutionTests
        fixture = NetlistExecutionTests()
        fixture.setUp()
        self.addCleanup(fixture.tearDown)
        os.makedirs(os.path.join(fixture.root, "candidates", fixture.manifest["candidate_id"]))
        optimizer = PrivateNetlistOptimizer(fixture.root,
            {"population": 1, "progress_flush_interval_seconds": 100}, {}, [], {}, lambda *args: {})
        observed = []
        terminal_stages = {"solver_terminal", "solve_completed", "measurement_terminal", "task_completed"}
        def progress(event):
            optimizer._progress(event)
            if event["stage"] in terminal_stages:
                observed.append(event["stage"])
                snapshot = load_json(os.path.join(optimizer.root, "progress.json"))
                self.assertEqual(snapshot["latest"]["stage"], event["stage"])
                candidate = load_json(os.path.join(fixture.root, "candidates",
                    fixture.manifest["candidate_id"], "cli_progress.json"))
                self.assertEqual(candidate["stage"], event["stage"])
        context = fixture.context(progress_callback=progress)
        context.evaluate(fixture.manifest)
        self.assertEqual(set(observed), terminal_stages)


class BatchedReaderTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = self.temp.name
        self.config = {"cadence": {"ocean_bin": sys.executable}}
        self.tasks = []
        for index in range(3):
            path = os.path.join(self.root, str(index))
            os.makedirs(path)
            self.tasks.append({"task_id": str(index), "test": "T", "corner": "C%d" % index, "point_id": index,
                "psf_dir": os.path.join(path, "psf"), "cwd": path,
                "outputs": [{"name": "gain", "key": "T::gain", "expression": 'VAR("R") + 1'}]})
        self.calls = []

    def command(self, argv, **kwargs):
        self.calls.append(argv)
        with open(argv[-1]) as stream:
            script = stream.read()
        self.assertEqual(script.count("exit(0)"), 1)
        self.assertNotIn("exit(4)", script)
        self.assertIn("crescBlockedVars gain)", script)
        self.assertIn("prog((crescPort", script)
        self.assertIn("closeResults(\"", script)
        metadata = load_json(argv[-1].replace(".il", ".json"))
        self.assertEqual(script.count("procedure(cr_read_point()"), len(metadata["points"]))
        self.assertEqual(kwargs["timeout_seconds"], metadata["batch_timeout_seconds"])
        # Deliberately write results in reverse completion order.
        for point in reversed(metadata["points"]):
            index = int(point["task_id"])
            output = "E\t0\texpression_error" if getattr(self, "fail_point", None) == index else "V\t0\t%d" % (index + 10)
            with open(point["records"], "w") as stream:
                stream.write(reader.PROTOCOL + "\t" + point["token"] + "\nP\t0\t%d\n%s\nEND\t1\n" % (index + 1, output))
        return {"returncode": 0}

    def read(self, **kwargs):
        return reader.evaluate_task_outputs_batch(self.tasks, self.config, self.root, 1, **kwargs)

    def test_prelaunch_render_and_write_failures_leave_no_reader_controls(self):
        for target in ("render_measurement_script", "atomic_write_text", "atomic_write_json"):
            original = getattr(reader, target)
            def fail_control(*args, **kwargs):
                if target == "render_measurement_script" or "reader_batches" in str(args[0]):
                    raise OSError("NFS write failed")
                return original(*args, **kwargs)
            with self.subTest(target=target), mock.patch.object(reader, target, side_effect=fail_control), mock.patch.object(reader, "run_owned_command") as launch:
                with self.assertRaises(OSError):
                    self.read()
                launch.assert_not_called()
            parent = os.path.join(self.root, "reader_batches")
            self.assertFalse(os.path.exists(parent) and os.listdir(parent))

    def test_guardian_prelaunch_failures_remove_unregistered_controls(self):
        for target in ("acquire_legacy_guardian_launch_lock", "write_legacy_guardian_manifest"):
            original = getattr(reader, target)
            def fail_after_write(*args, **kwargs):
                value = original(*args, **kwargs)
                if target == "acquire_legacy_guardian_launch_lock":
                    reader.close_legacy_guardian_launch_lock(value[0])
                raise OSError("NFS guardian preparation failed")
            with self.subTest(target=target), \
                    mock.patch.object(reader, target, side_effect=fail_after_write), \
                    mock.patch.object(reader, "run_command") as command:
                ownership = mock.Mock()
                with self.assertRaisesRegex(OSError, "NFS guardian preparation failed"):
                    self.read(ownership_callback=ownership)
                ownership.assert_not_called()
                command.assert_not_called()
            parent = os.path.join(self.root, "reader_batches")
            self.assertFalse(os.path.exists(parent) and os.listdir(parent))

    def test_ownership_or_runner_failures_preserve_guardian_controls(self):
        for failure in ("ownership", "runner"):
            with self.subTest(failure=failure):
                ownership = mock.Mock(side_effect=OSError("ownership ACK failed")
                                      if failure == "ownership" else None)
                command = mock.Mock(side_effect=OSError("runner failed"))
                with self.assertRaises(OSError):
                    self.read(ownership_callback=ownership, command_runner=command)
                ownership.assert_called_once()
                dependency = ownership.call_args[0][0]
                self.assertTrue(os.path.isdir(dependency["attempt_root"]))
                self.assertTrue(os.path.isfile(os.path.join(
                    dependency["attempt_root"], "legacy_guardian_launch.json")))
                self.assertEqual(command.call_count, int(failure == "runner"))

    def test_one_reader_out_of_order_results_and_variables_are_point_bound(self):
        with mock.patch.object(reader, "run_owned_command", side_effect=self.command):
            result = self.read()
        self.assertEqual(len(self.calls), 1)
        for index in range(3):
            self.assertEqual(result[str(index)]["outputs"][0]["value"], index + 10)
            self.assertEqual(result[str(index)]["effective_variables"], {"R": index + 1})

    def test_single_point_failure_keeps_others_and_restarts_fresh(self):
        self.fail_point = 1
        with mock.patch.object(reader, "run_owned_command", side_effect=self.command):
            first = self.read()
            self.assertIsInstance(first["1"], reader.NetlistMeasurementError)
            self.assertFalse(first["1"].infrastructure)
            self.assertEqual(first["2"]["outputs"][0]["value"], 12)
            self.fail_point = None
            second = self.read()
        self.assertNotEqual(first["0"]["token"], second["0"]["token"])
        self.assertEqual(len(self.calls), 2)

    def test_timeout_and_stop_cannot_ack_partial_records_and_next_reader_is_fresh(self):
        for interrupted in ({"timed_out": True}, {"cancelled": True}, {"termination_failed": True}):
            with mock.patch.object(reader, "run_owned_command", return_value=dict(interrupted, returncode=-1)):
                failed = self.read(cancel_check=lambda: bool(interrupted.get("cancelled")))
            self.assertTrue(all(isinstance(value, reader.NetlistMeasurementError) for value in failed.values()))
            with mock.patch.object(reader, "run_owned_command", side_effect=self.command):
                self.assertEqual(self.read()["0"]["outputs"][0]["value"], 10)

    def test_batch_rejects_colliding_point_roots_and_oversized_batches(self):
        self.tasks[1]["psf_dir"] = self.tasks[0]["psf_dir"]
        with self.assertRaises(reader.NetlistMeasurementError):
            self.read()
        with self.assertRaises(reader.NetlistMeasurementError):
            reader.evaluate_task_outputs_batch(self.tasks * 2, self.config, self.root, 1)

    def fake_reader(self):
        binary = os.path.join(self.root, "fake_ocean")
        behavior = os.path.join(self.root, "behavior")
        pid_path = os.path.join(self.root, "reader.pid")
        source = "#!%s\n" % sys.executable + "import json,os,sys,time\n"
        source += "open(%r,'w').write(str(os.getpid()))\n" % pid_path
        source += "behavior=open(%r).read()\n" % behavior
        source += "if behavior == 'wait': time.sleep(60)\n"
        source += "if behavior == 'invalid': sys.exit(4)\n"
        source += "script=sys.argv[sys.argv.index('-restore')+1]\n"
        source += "batch=json.load(open(script.replace('.il','.json')))\n"
        source += "for p in batch['points']:\n"
        source += " if behavior == 'slow': time.sleep(0.35)\n"
        source += " open(p['records'],'w').write('CRESCENDO_NETLIST_MEASURE_1\\t'+p['token']+'\\nP\\t0\\t1\\nV\\t0\\t7\\nEND\\t1\\n')\n"
        with open(binary, "w") as stream:
            stream.write(source)
        os.chmod(binary, 0o700)
        self.config["cadence"]["ocean_bin"] = binary
        return binary, behavior, pid_path

    def test_slow_points_keep_full_individual_timeout_budget(self):
        binary, behavior, pid_path = self.fake_reader()
        with open(behavior, "w") as stream:
            stream.write("slow")
        started = time.monotonic()
        result = reader.evaluate_task_outputs_batch(self.tasks, self.config, self.root, 0.75)
        duration = time.monotonic() - started
        self.assertGreater(duration, 0.75)
        self.assertLess(duration, 8)
        for measured in result.values():
            self.assertEqual(measured["outputs"][0]["value"], 7)
            self.assertFalse(measured["command"]["timed_out"])
        print("P0_SLOW_READER points=3 per_point_budget=0.75 batch_budget=2.25 elapsed=%.3f" % duration)

    def test_large_point_budgets_split_before_batch_ceiling(self):
        budgets = []
        def command(argv, **kwargs):
            budgets.append(kwargs["timeout_seconds"])
            return self.command(argv, **kwargs)
        with mock.patch.object(reader, "run_owned_command", side_effect=command):
            result = reader.evaluate_task_outputs_batch(self.tasks, self.config, self.root, 2000)
        self.assertEqual(budgets, [2000] * 3)
        self.assertEqual(len(result), 3)
        self.calls = []
        with mock.patch.object(reader, "run_owned_command", side_effect=command):
            result = reader.evaluate_task_outputs_batch(self.tasks[:1], self.config, self.root, 4000)
        self.assertEqual(budgets[-1], 4000)
        self.assertEqual(len(result), 1)

    def test_owned_reader_timeout_stop_cleanup_and_restart(self):
        binary, behavior, pid_path = self.fake_reader()
        for stop in (False, True):
            with open(behavior, "w") as stream:
                stream.write("wait")
            cancelled = threading.Event()
            timer = threading.Timer(0.5, cancelled.set) if stop else None
            if timer:
                timer.start()
            started = time.monotonic()
            try:
                result = reader.evaluate_task_outputs_batch(self.tasks, self.config, self.root,
                    10 if stop else 0.5, cancel_check=cancelled.is_set)
            finally:
                if timer:
                    timer.cancel()
            self.assertTrue(all(isinstance(value, reader.NetlistMeasurementError) for value in result.values()))
            self.assertLess(time.monotonic() - started, 10)
            for error in result.values():
                self.assertTrue(error.detail["command"]["cancelled" if stop else "timed_out"])
            with open(pid_path) as stream:
                pid = int(stream.read())
            self.assertFalse(os.path.exists("/proc/%d" % pid))
            with open(behavior, "w") as stream:
                stream.write("finish")
            restarted = self.read()
            self.assertEqual(restarted["0"]["outputs"][0]["value"], 7)
            self.assertFalse(restarted["0"]["command"].get("termination_failed"))

    def test_normal_entry_cleans_reader_controls_on_success_timeout_and_stop(self):
        from tests.test_netlist_execution import NetlistExecutionTests
        from crescendo_engine.netlist_execution import NetlistExecutionError
        binary, behavior, pid_path = self.fake_reader()
        for mode in ("finish", "timeout", "stop", "invalid"):
            with self.subTest(mode=mode):
                with open(behavior, "w") as stream:
                    stream.write("wait" if mode in ("timeout", "stop") else mode)
                fixture = NetlistExecutionTests()
                fixture.setUp()
                try:
                    fixture.config["cadence"]["ocean_bin"] = binary
                    fixture.spec.update(reader_protocol=READER_BATCH,
                        candidate_timeout_seconds=0.5 if mode == "timeout" else 10)
                    tasks = [fixture.make_task("reader%d" % index, point=index + 1) for index in range(3)]
                    for task in tasks:
                        task["outputs"][0]["expression"] = 'VAR("R")'
                    fixture.manifest["tasks"] = tasks
                    cancelled = threading.Event()
                    timers = []
                    def progress(event):
                        if mode == "stop" and event["stage"] == "measurement":
                            timer = threading.Timer(0.3, cancelled.set)
                            timers.append(timer)
                            timer.start()
                    context = fixture.context(measurement_evaluator=None,
                        cancel_check=cancelled.is_set, progress_callback=progress,
                        ownership_root=os.path.join(fixture.root, "ownership"), owned_run_root=fixture.root)
                    if mode == "finish":
                        context.evaluate(fixture.manifest)
                    else:
                        with self.assertRaises(NetlistExecutionError):
                            context.evaluate(fixture.manifest)
                    for timer in timers:
                        timer.cancel()
                    self.assertTrue(context.close()["cleanup_proven"])
                    self.assertFalse(os.path.exists(os.path.join(fixture.root, "reader_batches")))
                    ownership = load_json(context.ownership_path)
                    self.assertEqual(ownership["active"], {})
                    self.assertEqual(ownership["unresolved"], {})
                finally:
                    fixture.tearDown()

    def test_normal_entry_prelaunch_write_failure_cleans_or_retains_truthfully(self):
        from tests.test_netlist_execution import NetlistExecutionTests
        from crescendo_engine.netlist_execution import NetlistExecutionError
        for target in ("atomic_write_text", "acquire_legacy_guardian_launch_lock",
                       "write_legacy_guardian_manifest"):
            for cleanup_fails in (False, True):
                with self.subTest(target=target, cleanup_fails=cleanup_fails):
                    fixture = NetlistExecutionTests()
                    fixture.setUp()
                    try:
                        fixture.spec.update(reader_protocol=READER_BATCH, candidate_timeout_seconds=10)
                        fixture.config["cadence"]["ocean_bin"] = sys.executable
                        tasks = [fixture.make_task("guardian%d" % i, point=i + 1) for i in range(2)]
                        context = fixture.context(measurement_evaluator=None,
                            ownership_root=os.path.join(fixture.root, "ownership"), owned_run_root=fixture.root)
                        original = getattr(reader, target)
                        def fail_after_write(*args, **kwargs):
                            value = original(*args, **kwargs)
                            if "reader_batches" not in args[0]:
                                return value
                            if target == "acquire_legacy_guardian_launch_lock":
                                reader.close_legacy_guardian_launch_lock(value[0])
                            raise OSError("NFS guardian preparation failed")
                        original_remove = reader.shutil.rmtree
                        def remove(path, *args, **kwargs):
                            if cleanup_fails and "reader_batches" in path:
                                raise OSError("NFS cleanup failed")
                            return original_remove(path, *args, **kwargs)
                        cleanup = context._cleanup_reader_batches
                        observed = []
                        def record_cleanup(*args):
                            result = cleanup(*args)
                            observed.append(result)
                            return result
                        with mock.patch.object(reader, target, side_effect=fail_after_write), \
                                mock.patch.object(reader.shutil, "rmtree", side_effect=remove), \
                                mock.patch.object(context, "_cleanup_reader_batches", side_effect=record_cleanup), \
                                mock.patch.object(reader, "run_command") as command:
                            with self.assertRaisesRegex((NetlistExecutionError, OSError), "NFS guardian preparation failed"):
                                context.evaluate(dict(fixture.manifest, tasks=tasks))
                            shutdown = context.close()
                            command.assert_not_called()
                        self.assertTrue(observed)
                        self.assertTrue(all(row["artifact_cleanup_complete"] == (not cleanup_fails)
                                            for row in observed), observed)
                        self.assertEqual(shutdown["cleanup_proven"], not cleanup_fails)
                        parent = os.path.join(fixture.root, "reader_batches")
                        self.assertEqual(bool(os.path.exists(parent) and os.listdir(parent)), cleanup_fails)
                        ownership = load_json(context.ownership_path)
                        self.assertEqual(bool(ownership["unresolved"]), cleanup_fails)
                    finally:
                        fixture.tearDown()

    def test_point_acks_keep_shared_reader_proof_after_first_raw_cleanup(self):
        from tests.test_netlist_execution import NetlistExecutionTests
        binary, behavior, pid_path = self.fake_reader()
        with open(behavior, "w") as stream:
            stream.write("finish")
        fixture = NetlistExecutionTests()
        fixture.setUp()
        self.addCleanup(fixture.tearDown)
        fixture.config["cadence"]["ocean_bin"] = binary
        fixture.spec.update(reader_protocol=READER_BATCH, local_solver_max_jobs=2)
        tasks = []
        for index in range(3):
            task = fixture.make_task(os.path.join("tasks", "ack%d" % index), point=index + 1)
            task["task_id"] = "ack%d" % index
            task["outputs"][0]["expression"] = 'VAR("R")'
            tasks.append(task)
        committed = []
        def commit(point, cancelled):
            self.assertTrue(all(proof["state"] == "dead" for proof in point["measurement_dependencies"]))
            if committed:
                previous = next(task for task in tasks if task["task_id"] == committed[-1])
                self.assertFalse(os.path.exists(os.path.join(os.path.dirname(previous["psf_dir"]), "measurement")))
            committed.append(point["task_id"])
            return {"ok": True, "revision": len(committed), "checkpoint_sha256": "owned-fixture-checkpoint"}
        context = fixture.context(measurement_evaluator=None, point_commit_callback=commit,
            ownership_root=os.path.join(fixture.root, "ownership"), owned_run_root=fixture.root)
        result = context.evaluate(dict(fixture.manifest, tasks=tasks))
        self.assertEqual(len(committed), 3)
        self.assertTrue(all(row["point_raw_cleanup"]["artifact_cleanup_complete"] for row in result["task_results"]))
        self.assertTrue(context.close()["cleanup_proven"])
        self.assertFalse(os.path.exists(os.path.join(fixture.root, "reader_batches")))

    def test_sibling_batch_can_create_controls_after_first_batch_retires(self):
        from tests.test_netlist_execution import NetlistExecutionTests
        from crescendo_engine import netlist_execution as execution
        binary, behavior, pid_path = self.fake_reader()
        with open(behavior, "w") as stream:
            stream.write("finish")
        fixture = NetlistExecutionTests()
        fixture.setUp()
        self.addCleanup(fixture.tearDown)
        fixture.config["cadence"]["ocean_bin"] = binary
        fixture.prepared["job_policy"]["global"]["properties"]["maxjobs"] = "4"
        fixture.spec.update(reader_protocol=READER_BATCH, local_solver_max_jobs=4, measurement_jobs=2)
        tasks = [fixture.make_task("sibling%d" % index, point=index + 1) for index in range(8)]
        for task in tasks:
            task["outputs"][0]["expression"] = 'VAR("R")'
        fixture.manifest["tasks"] = tasks
        context = fixture.context(measurement_evaluator=None)
        first_retired, guard, calls = threading.Event(), threading.Lock(), []
        parent = os.path.join(fixture.root, "reader_batches")
        def read_batch(*args, **kwargs):
            with guard:
                calls.append(len(args[0]))
                ordinal = len(calls)
            if ordinal == 2:
                self.assertTrue(first_retired.wait(15))
                self.assertTrue(os.path.isdir(parent))
            return reader.evaluate_task_outputs_batch(*args, **kwargs)
        cleanup = context._cleanup_reader_batches
        def retire(*args):
            try:
                result = cleanup(*args)
                self.assertTrue(result["artifact_cleanup_complete"], result)
                self.assertTrue(os.path.isdir(parent))
                return result
            finally:
                first_retired.set()
        with mock.patch.object(execution, "evaluate_task_outputs_batch", side_effect=read_batch), \
                mock.patch.object(context, "_cleanup_reader_batches", side_effect=retire):
            result = context.evaluate(fixture.manifest)
        self.assertEqual(calls, [4, 4])
        self.assertEqual(len(result["task_results"]), 8)
        self.assertFalse(os.path.exists(parent))
        self.assertTrue(context.close()["cleanup_proven"])

    def test_reader_cleanup_preserves_unresolved_shared_owner(self):
        from tests.test_netlist_execution import NetlistExecutionTests
        binary, behavior, pid_path = self.fake_reader()
        with open(behavior, "w") as stream:
            stream.write("finish")
        fixture = NetlistExecutionTests()
        fixture.setUp()
        self.addCleanup(fixture.tearDown)
        fixture.config["cadence"]["ocean_bin"] = binary
        fixture.spec["reader_protocol"] = READER_BATCH
        fixture.task["outputs"][0]["expression"] = 'VAR("R")'
        retained = []
        context = fixture.context(measurement_evaluator=None,
            ownership_root=os.path.join(fixture.root, "ownership"), owned_run_root=fixture.root)
        def commit(point, cancelled):
            key = fixture.manifest["candidate_id"] + ":" + fixture.task["task_id"]
            with context.lock:
                owner = copy.deepcopy(context.active[key])
                context.unresolved[key] = owner
            dependency = owner["cleanup_dependencies"][-1]
            retained.append(dependency["attempt_root"])
            return {"ok": False}
        context.point_commit_callback = commit
        context.evaluate(fixture.manifest)
        self.assertEqual(len(retained), 1)
        self.assertTrue(os.path.isdir(retained[0]))
        with context.lock:
            dependency = copy.deepcopy(next(iter(context.unresolved.values()))["cleanup_dependencies"][-1])
            context.unresolved.clear()
        with mock.patch.object(context, "_persist_ownership_locked", side_effect=OSError("injected ownership write failure")):
            result = context._cleanup_reader_batches(fixture.root, [dependency])
        self.assertFalse(result["artifact_cleanup_complete"])
        self.assertTrue(os.path.isdir(retained[0]))
        self.assertTrue(context.close()["cleanup_proven"])

    def test_configured_reader_pool_peak_and_batched_normal_entry(self):
        from tests.test_netlist_execution import NetlistExecutionTests
        fixture = NetlistExecutionTests()
        fixture.setUp()
        self.addCleanup(fixture.tearDown)
        fixture.prepared["job_policy"]["global"]["properties"]["maxjobs"] = "4"
        fixture.spec.update(reader_protocol=READER_BATCH, measurement_jobs=2, local_solver_max_jobs=4)
        context = fixture.context(measurement_evaluator=None)
        manifests = []
        for index in range(4):
            tasks = [fixture.make_task("batch%d_%d" % (index, point), point=point + 1) for point in range(3)]
            manifests.append(dict(fixture.manifest, candidate_id="g000_c%04d" % index, tasks=tasks))
        counts, guard = [0, 0, 0], threading.Lock()
        reader_barrier = threading.Barrier(2)
        def batch(tasks, config, root, timeout, **kwargs):
            kwargs["progress_callback"]({"stage": "measurement"})
            with guard:
                counts[0] += 1
                counts[1] = max(counts[0], counts[1])
                counts[2] += 1
            reader_barrier.wait(timeout=15)
            time.sleep(0.1)
            with guard:
                counts[0] -= 1
            return {task["task_id"]: fixture.evaluator(task, config, root) for task in tasks}
        with mock.patch("crescendo_engine.netlist_execution.evaluate_task_outputs_batch", side_effect=batch):
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
                result = list(pool.map(context.evaluate, manifests))
        self.assertEqual([len(row["task_results"]) for row in result], [3] * 4)
        self.assertEqual(counts[1:], [2, 4])
        self.assertEqual(context.generation_capacity(0)["observed_peak"]["reader"], 2)
        self.assertFalse(context.active)
        print("P0_READER candidates=4 points=12 reader_processes=4 configured=2 observed=2")


if __name__ == "__main__":
    unittest.main()
