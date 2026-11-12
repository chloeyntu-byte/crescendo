"""Owned scheduler/process fixtures, separate from real Spectre qualification."""
import copy
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

from crescendo_engine import netlist_execution as execution
from crescendo_engine.utils import atomic_write_text, sha256_file


class NetlistExecutionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="crescendo-execution's tests-")
        self.root = self.temp.name
        self.config = {"cadence": {}, "runtime": {"cluster_queue_timeout_seconds": 0.1}}
        self.prepared = {"job_policy": {"available": True, "global": {"properties": {
            "distributionmethod": "Local", "maxjobs": "2"}}, "tests": []}}
        self.spec = {"candidate_timeout_seconds": 2, "objectives": [{"output_key": "rc::gain"}], "constraints": []}
        self.contexts = []
        self.task = self.make_task()
        self.manifest = {"schema_version": "crescendo.private_netlist.v1", "root": self.root,
                         "candidate_id": "g000_c0000", "template_sha256": "private", "tasks": [self.task]}

    def tearDown(self):
        for context in self.contexts:
            context.close(cancel=True)
        self.temp.cleanup()

    def make_task(self, suffix="a", point=1, delay=0.02):
        task_root = os.path.join(self.root, suffix)
        os.makedirs(os.path.join(task_root, "psf"))
        netlist = os.path.join(task_root, "input.scs")
        atomic_write_text(netlist, "simulator lang=spectre\n")
        log = os.path.join(task_root, "psf", "spectre.out")
        script = os.path.join(task_root, "owned_simulator_fixture.py")
        atomic_write_text(script, "import os,time\ntime.sleep(%r)\nopen(%r,'w').write('spectre completes with 0 errors.\\n')\nopen(%r,'w').write('owned protocol fixture, not native Spectre')\n" % (
            delay, log, os.path.join(task_root, "psf", "logFile")))
        return {"task_id": suffix, "test": "rc", "corner": "tt", "point_id": point,
                "netlist_path": netlist, "netlist_sha256": sha256_file(netlist), "cwd": task_root,
                "spectre_log": log, "psf_dir": os.path.join(task_root, "psf"),
                "argv": [sys.executable, script, netlist],
                "outputs": [{"key": "rc::gain", "name": "gain", "expression": "1.0"}]}

    def evaluator(self, task, config, root, **kwargs):
        return {"outputs": [dict(task["outputs"][0], value=0.5, error=None)]}

    def context(self, **kwargs):
        context = execution.NetlistExecutionContext(self.config, self.prepared, self.spec,
            measurement_evaluator=kwargs.pop("measurement_evaluator", self.evaluator), **kwargs)
        self.contexts.append(context)
        return context

    def test_local_owned_wrapper_completion_and_scalar_identity(self):
        context = self.context()
        timing = {"phases": {}}
        result = context.evaluate(self.manifest, timing=timing)
        self.assertEqual(result["candidate_id"], "g000_c0000")
        self.assertEqual(timing["phases"]["solver"]["count"], 0)
        self.assertIsNone(timing["phases"]["solver"]["elapsed_seconds"])
        self.assertIsNone(timing["phases"]["scheduler_queue"]["elapsed_seconds"])
        task = timing["tasks"][0]
        self.assertEqual(task["status"], "completed")
        self.assertEqual(task["phases"]["scheduler_queue"]["source"], "not_applicable")
        for phase in ("worker_queue", "capacity_wait", "execution_preparation", "local_command",
                      "solver_artifact_collection", "measurement_queue", "result_collection"):
            self.assertGreaterEqual(task["phases"][phase]["elapsed_seconds"], 0)
        self.assertEqual(result["parsed"]["output_map"]["rc::gain"]["summary"]["mean"], 0.5)
        self.assertFalse(result["parsed"]["native_rdb"])
        self.assertTrue(result["task_results"][0]["receipt"]["cleanup_proven"])
        self.assertTrue(context.close()["cleanup_proven"])

    def native_fallback_markers(self):
        fallback = {'failure_stage': 'native_numeric_derived_parameters',
            'native_generation_required': True, 'template_qualified': False,
            'numeric_derived_parameters': [{'parameter': 'DPAR_10'}],
            'source_guard': {'ok': True}, 'preparation_cleanup': {'cleanup_proven': True}}
        return {'requested_backend': 'private_netlist', 'native_generation_required': True,
                'template_qualified': False, 'template_fallback': fallback}

    def test_native_numeric_fallback_preserves_real_source_through_shared_execution(self):
        self.assert_native_fallback_source(self.native_fallback_markers())

    def selected_binding_markers(self):
        markers = self.native_fallback_markers()
        fallback = markers['template_fallback']
        fallback['failure_stage'] = 'native_selected_parameter_binding'
        fallback.pop('numeric_derived_parameters')
        fallback['selected_parameter_bindings'] = [{'parameter_id': 'width', 'name': 'DTN_W',
            'proof_source': 'full_identity_native_RDB_input', 'dpar_names': []}]
        return markers

    def test_native_selected_binding_fallback_preserves_persisted_actual_source(self):
        self.assert_native_fallback_source(self.selected_binding_markers())

    def assert_native_fallback_source(self, markers):
        self.spec['execution_backend'] = 'private_netlist'
        self.task = self.make_task(os.path.join('tasks', 'a'))
        self.task['task_id'] = 'a'
        self.manifest['tasks'] = [self.task]
        self.manifest['netlist_generation'] = dict(markers, source_mode='maestro_netlist')
        # Real native producer tasks originally have no source dict; optimizer
        # attaches fallback annotations before the shared execution boundary.
        self.task['netlist_generation'] = copy.deepcopy(markers)
        second = self.make_task(os.path.join('tasks', 'b'), point=2)
        second['task_id'] = 'b'
        second['netlist_generation'] = copy.deepcopy(markers)
        self.manifest['tasks'].append(second)
        points = []
        def commit(point, cancelled):
            points.append(copy.deepcopy(point))
            return {'ok': True, 'revision': len(points), 'checkpoint_sha256': 'owned-fixture-checkpoint'}
        context = self.context(point_commit_callback=commit)
        result = context.evaluate(self.manifest)
        pipeline = result['execution_pipeline']
        self.assertEqual(pipeline['netlist_source'], 'maestro')
        self.assertEqual(self.spec['execution_backend'], 'private_netlist')
        self.assertEqual(len(points), 2)
        self.assertTrue(all(point['netlist_generation']['mode'] == 'maestro' for point in points))
        with open(os.path.join(self.root, 'cli_results.json')) as stream:
            persisted = json.load(stream)
        self.assertEqual(persisted['execution_pipeline']['netlist_source'], 'maestro')
        self.assertEqual(persisted['completion']['execution_pipeline']['netlist_source'], 'maestro')
        for point in pipeline['points']:
            self.assertEqual(point['netlist_generation']['mode'], 'maestro')
            self.assertEqual(point['netlist_generation']['source_mode'], 'maestro_netlist')
            self.assertTrue(point['netlist_generation']['native_generation_required'])
            self.assertFalse(point['netlist_generation']['template_qualified'])
            self.assertTrue(point['terminal_receipt']['cleanup_proven'])
        self.assertTrue(context.close()['cleanup_proven'])

    def test_selected_binding_fallback_rejects_missing_proof_or_unknown_stage(self):
        self.spec['execution_backend'] = 'private_netlist'
        for fault in ('missing_bindings', 'unknown_stage', 'source', 'cleanup', 'termination_failed'):
            with self.subTest(fault=fault):
                markers = self.selected_binding_markers()
                fallback = markers['template_fallback']
                if fault == 'missing_bindings':
                    fallback['selected_parameter_bindings'] = []
                elif fault == 'unknown_stage':
                    fallback['failure_stage'] = 'any_parameter_error'
                elif fault == 'source':
                    fallback['source_guard']['ok'] = False
                elif fault == 'cleanup':
                    fallback['preparation_cleanup']['cleanup_proven'] = False
                else:
                    fallback['termination_failed'] = True
                self.manifest['netlist_generation'] = dict(markers, source_mode='maestro_netlist')
                context = self.context()
                with mock.patch.object(context, 'command_runner') as launch, self.assertRaises(execution.NetlistExecutionError):
                    context.evaluate(self.manifest)
                launch.assert_not_called()

    def test_shared_execution_rejects_unproven_native_fallback_before_launch(self):
        self.spec['execution_backend'] = 'private_netlist'
        for fault in ('missing_reason', 'source', 'cleanup', 'pending_cleanup', 'wrong_point_source'):
            with self.subTest(fault=fault):
                markers = self.native_fallback_markers()
                fallback = markers['template_fallback']
                if fault == 'missing_reason':
                    markers.pop('template_fallback')
                elif fault == 'source':
                    fallback['source_guard']['ok'] = False
                elif fault == 'cleanup':
                    fallback['preparation_cleanup']['cleanup_proven'] = False
                elif fault == 'pending_cleanup':
                    fallback['cleanup_pending'] = True
                self.manifest['netlist_generation'] = dict(markers, source_mode='maestro_netlist')
                self.task['netlist_generation'] = {'mode': 'template'} if fault == 'wrong_point_source' else {}
                context = self.context()
                with mock.patch.object(context, 'command_runner') as launch, self.assertRaises(execution.NetlistExecutionError):
                    context.evaluate(self.manifest)
                launch.assert_not_called()

    def test_local_zombie_receipt_joins_exact_guardian_before_point_commit_and_remote_never_reuses_it(self):
        self.task = self.make_task(os.path.join("tasks", "zombie"))
        self.task["task_id"] = "zombie"
        self.manifest["tasks"] = [self.task]
        script = self.task["argv"][1]
        with open(script, "r", encoding="utf-8") as stream:
            original = stream.read()
        # The simulator leaves one exited child unreaped. The shell's kill -0
        # sees its zombie group until the outer owned subreaper finishes.
        atomic_write_text(script, "import os,time\nif os.fork()==0: os._exit(0)\ntime.sleep(0.05)\n" + original)
        points = []

        def commit(point, cancelled):
            points.append(copy.deepcopy(point))
            return {"ok": True, "revision": 1, "checkpoint_sha256": "owned-fixture-checkpoint"}

        context = self.context(point_commit_callback=commit)
        result = context.evaluate(self.manifest)
        self.assertEqual(len(points), 1)
        point = points[0]
        self.assertFalse(point["terminal_receipt"]["cleanup_proven"])
        self.assertTrue(point["effective_terminal_receipt"]["cleanup_proven"])
        self.assertEqual(point["effective_terminal_receipt"]["cleanup_basis"], "owned_local_guardian")
        proof = point["termination_evidence"]
        self.assertTrue(proof["cleanup_proven"])
        self.assertFalse(proof["remote"])
        self.assertEqual(proof["guardian"]["termination_proof"]["kind"], "guardian_cleanup_proven")
        receipt_path = os.path.join(self.task["cwd"], "execution", "terminal.receipt")
        with open(receipt_path, "r", encoding="utf-8") as stream:
            self.assertEqual(stream.read().strip().split("\t")[4], "0")
        pipeline = result["execution_pipeline"]["points"][0]
        self.assertFalse(pipeline["raw_terminal_receipt"]["cleanup_proven"])
        self.assertTrue(pipeline["terminal_receipt"]["cleanup_proven"])
        self.assertTrue(result["task_results"][0]["point_raw_cleanup"]["artifact_cleanup_complete"])
        row = {"token": point["token"], "remote": False, "task": self.task,
               "files": {"terminal.receipt": receipt_path},
               "submission_command_plan": point["submission_command_plan"]}
        remote = dict(row, remote=True)
        self.assertFalse(execution.task_termination_evidence(remote, point["terminal_receipt"])["cleanup_proven"])
        wrong_token = dict(row, token="f" * 32)
        self.assertFalse(execution.task_termination_evidence(wrong_token, point["terminal_receipt"])["cleanup_proven"])
        wrong_command = copy.deepcopy(row)
        wrong_command["submission_command_plan"]["argv"] = ["/bin/sh", "/unowned/execute.sh"]
        self.assertFalse(execution.task_termination_evidence(wrong_command, point["terminal_receipt"])["cleanup_proven"])
        self.assertTrue(context.close()["cleanup_proven"])

    def test_default_and_override_preserve_original_command_and_resources(self):
        self.config['runtime']['execution_commands'] = {
            'scheduler_query_template': 'batch-query {job_id}',
            'scheduler_cancel_template': 'batch-cancel {job_id}'}
        self.prepared["job_policy"]["global"]["properties"].update(
            distributionmethod="Command", jobsubmitcommand='batch-submit -A account -q short -R "cpu=6;mem=30000"', maxjobs="64")
        policy = execution.execution_policy(self.prepared, self.spec, self.config)
        self.assertEqual(policy["max_jobs"], 64)
        self.assertEqual(policy["global"]["submit_argv"], ["batch-submit", "-A", "account", "-q", "short", "-R", "cpu=6;mem=30000"])
        override = dict(self.spec, maestro_job_overrides={"command": "batch-submit -q long", "max_jobs": 3})
        changed = execution.execution_policy(self.prepared, override, self.config)
        self.assertEqual(changed["max_jobs"], 3)
        self.assertEqual(changed["global"]["submit_argv"], ["batch-submit", "-q", "long"])
        self.assertEqual(self.prepared["job_policy"]["global"]["properties"]["maxjobs"], "64")

    def test_unknown_distribution_and_shell_redirection_rejected(self):
        self.config['runtime']['execution_commands'] = {
            'scheduler_query_template': 'batch-query {job_id}',
            'scheduler_cancel_template': 'batch-cancel {job_id}'}
        for distribution, command in (("lsf", ""), ("command", "batch-submit | other"), ("command", "")):
            self.prepared["job_policy"]["global"]["properties"].update(
                distributionmethod=distribution, jobsubmitcommand=command)
            with self.subTest(distribution=distribution, command=command), self.assertRaises(execution.NetlistExecutionError):
                self.context()

    def test_changed_netlist_and_parameter_include_rejected_before_native(self):
        context = self.context()
        params = os.path.join(self.task["cwd"], "params.scs")
        atomic_write_text(params, "parameters w=1u\n")
        self.task["artifact_sha256"] = {params: sha256_file(params)}
        atomic_write_text(params, "parameters w=2u\n")
        with mock.patch.object(context, "command_runner") as runner, self.assertRaises(execution.NetlistExecutionError):
            context.evaluate(self.manifest)
        runner.assert_not_called()

    def test_existing_history_log_cannot_certify_a_fresh_candidate(self):
        context = self.context()
        atomic_write_text(self.task["spectre_log"], "spectre completes with 0 errors.\n")
        with mock.patch.object(context, "command_runner") as runner, self.assertRaises(execution.NetlistExecutionError):
            context.evaluate(self.manifest)
        runner.assert_not_called()

    def test_source_history_or_symlink_psf_is_rejected(self):
        context = self.context()
        outside = tempfile.mkdtemp(prefix="crescendo-other-private-")
        try:
            self.task["psf_dir"] = outside
            with mock.patch.object(context, "command_runner") as runner, self.assertRaises(execution.NetlistExecutionError):
                context.evaluate(self.manifest)
            runner.assert_not_called()
        finally:
            os.rmdir(outside)

    def test_cancellation_stops_owned_local_sleep(self):
        self.task = self.make_task("sleep", delay=20)
        self.manifest["tasks"] = [self.task]
        context = self.context()
        timer = threading.Timer(0.3, context.cancel)
        timer.start()
        started = time.monotonic()
        try:
            with self.assertRaises(execution.NetlistExecutionError):
                context.evaluate(self.manifest, timeout_seconds=20)
        finally:
            timer.cancel()
        self.assertLess(time.monotonic() - started, 7)
        self.assertTrue(context.close()["cleanup_proven"])

    def test_capacity_shared_across_candidates_and_smaller_than_generation(self):
        context = self.context()
        manifests = []
        for index in range(5):
            task = self.make_task("parallel%d" % index, delay=0.1)
            manifests.append(dict(self.manifest, candidate_id="g000_c%04d" % index, tasks=[task]))
        active, peak = [0], [0]
        lock = threading.Lock()
        original = context._execute_task_owned

        def counted(*args, **kwargs):
            with lock:
                active[0] += 1
                peak[0] = max(peak[0], active[0])
            try:
                return original(*args, **kwargs)
            finally:
                with lock:
                    active[0] -= 1
        with mock.patch.object(context, "_execute_task_owned", side_effect=counted):
            with execution.concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
                results = list(pool.map(context.evaluate, manifests))
        self.assertEqual(len(results), 5)
        self.assertEqual(peak[0], 1)  # Local solvers are serialized across candidates.

    def test_receipt_wrong_attempt_and_nonzero_not_success(self):
        path = os.path.join(self.root, "receipt")
        atomic_write_text(path, execution.RECEIPT + "\told\texited\t0\t1\n")
        with self.assertRaises(execution.NetlistExecutionError):
            execution.read_task_receipt(path, "new", terminal=True)
        atomic_write_text(path, execution.RECEIPT + "\tnew\texited\t3\t1\n")
        self.assertEqual(execution.read_task_receipt(path, "new", terminal=True)["returncode"], 3)

    def test_scheduler_id_and_terminal_require_explicit_identity(self):
        self.assertEqual(execution.scheduler_job_id("Job <123> is submitted successfully."), "123")
        self.assertIsNone(execution.scheduler_job_id("cpu=6 mem=30000 total=123"))
        self.assertIsNone(execution.scheduler_job_id("Job <123> submitted\nJob <124> submitted"))
        self.assertEqual(execution.scheduler_terminal_state("JOB_ID: 123\nSTATE: CANCELLED", "123"), "CANCELLED")
        self.assertIsNone(execution.scheduler_terminal_state("JOB_ID: 124\nSTATE: CANCELLED", "123"))
        self.assertIsNone(execution.scheduler_terminal_state("cancel request sent to job 123", "123"))

    def test_missing_templates_never_guesses_cancel(self):
        context = self.context()
        batch_submit = os.path.join(self.root, "batch-submit")
        batch_query = os.path.join(self.root, "batch-query")
        for path in (batch_submit, batch_query):
            atomic_write_text(path, "#!/bin/sh\nexit 0\n")
            os.chmod(path, 0o700)
        policy = {"submit_argv": [batch_submit]}
        with mock.patch.object(context, "command_runner", return_value={"returncode": 0, "stdout_tail": "unknown CLI"}) as runner:
            result = context._scheduler_cancel(policy, {"stdout_tail": "Job <123> submitted"}, self.root, os.environ)
        self.assertFalse(result["attempted"])
        runner.assert_not_called()

    def test_async_submission_exit_is_not_simulation_completion(self):
        def submitted(*args, **kwargs):
            if kwargs.get("guardian_launch_lock_fd") is not None:
                os.close(kwargs["guardian_launch_lock_fd"])
            return {"returncode": 0, "stdout_tail": "Job <123> submitted"}
        context = self.context(command_runner=submitted)
        context.policy["global"].update({"distribution": "command", "submit_argv": [sys.executable], "command": sys.executable})
        with mock.patch("crescendo_engine.netlist_execution.time.sleep"), \
                mock.patch("crescendo_engine.netlist_execution.time.monotonic", side_effect=lambda: time.time() + 10000), \
                mock.patch.object(context, "_scheduler_cancel", return_value={"remote_stop_proven": False}):
            # Fast fake monotonic clock advancing on each read bounds the empty
            # queue/cleanup fixture without running any remote scheduler job.
            clock = iter(range(100, 100000, 100))
            polls = [0]
            def cancel_unproved_queue():
                polls[0] += 1
                return polls[0] > 10
            with mock.patch("crescendo_engine.netlist_execution.time.monotonic", side_effect=lambda: next(clock)), \
                    self.assertRaises(execution.NetlistExecutionError):
                context.evaluate(self.manifest, cancel_check=cancel_unproved_queue)
        self.assertTrue(context.unsafe.is_set())
        self.assertTrue(context.close()["retained"])
        self.assertTrue(os.path.isfile(os.path.join(self.task["cwd"], "execution", "error.json")))

    def test_command_async_owned_wrapper_finishes_before_accepting_scalar(self):
        wrappers = []

        def submit(argv, **kwargs):
            os.close(kwargs["guardian_launch_lock_fd"])
            wrappers.append(subprocess.Popen(["/bin/sh", argv[-1]], cwd=kwargs["cwd"],
                env=kwargs["env"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
            return {"returncode": 0, "stdout_tail": "Job <321> submitted"}
        context = self.context(command_runner=submit)
        context.policy["global"].update({"distribution": "command", "submit_argv": [sys.executable], "command": sys.executable})
        try:
            result = context.evaluate(self.manifest)
            self.assertTrue(result["task_results"][0]["receipt"]["cleanup_proven"])
            self.assertEqual(result["parsed"]["outputs"][0]["summary"]["first"], 0.5)
        finally:
            for process in wrappers:
                process.wait(timeout=5)

    def _positional_scheduler(self):
        binaries = {}
        for name in ('batch-submit', 'batch-query', 'batch-cancel'):
            binaries[name] = os.path.join(self.root, name)
            atomic_write_text(binaries[name], '#!/bin/sh\nexit 0\n')
            os.chmod(binaries[name], 0o700)
        self.config['runtime']['execution_commands'] = {
            'scheduler_query_template': __import__('shlex').quote(binaries['batch-query']) + ' {job_id}',
            'scheduler_cancel_template': __import__('shlex').quote(binaries['batch-cancel']) + ' {job_id}'}
        self.config['runtime']['cluster_queue_timeout_seconds'] = 3
        return binaries

    def test_selected_templates_cancel_only_owned_id_without_help(self):
        binaries = self._positional_scheduler()
        calls = []
        def runner(argv, **kwargs):
            calls.append(argv)
            if argv == [binaries['batch-cancel'], '10000002']:
                return {'returncode': 0, 'stdout_tail': 'cancel request accepted'}
            self.assertEqual(argv, [binaries['batch-query'], '10000002'])
            return {'returncode': 0, 'stdout_tail': 'JOB_ID 10000002\nJOB_STATE CANCELLED\n'}
        context = self.context(command_runner=runner)
        result = context._scheduler_cancel({'submit_argv': [binaries['batch-submit']]},
            {'stdout_tail': 'JOBID MESSAGE\n10000002 Submit job successfully.\n'}, self.root, os.environ)
        self.assertTrue(result['remote_stop_proven'])
        self.assertEqual(calls, [[binaries['batch-cancel'], '10000002'], [binaries['batch-query'], '10000002']])
        self.assertEqual(result['capability']['source'], 'user_templates')

    def test_scheduler_query_spawn_error_is_unknown_not_stop_or_solver_proof(self):
        binaries = self._positional_scheduler()
        def runner(argv, **kwargs):
            raise OSError('mocked CLI spawn failure')
        context = self.context(command_runner=runner)
        policy = {'submit_argv': [binaries['batch-submit']]}
        observed = context._scheduler_observe(policy, '10000002', self.root, os.environ, None,
            {'task_id': 'a', 'candidate_id': 'g000_c0000'}, final=True)
        self.assertFalse(observed['latest']['terminal'])
        self.assertEqual(observed['latest']['reason'], 'scheduler_query_failed')
        stopped = context._scheduler_cancel(policy,
            {'stdout_tail': 'JOBID MESSAGE\n10000002 accepted\n'}, self.root, os.environ)
        self.assertFalse(stopped['remote_stop_proven'])
        self.assertEqual(stopped['query_observation']['reason'], 'scheduler_query_failed')

    def test_missing_or_mixed_scheduler_id_fails_even_with_owned_zero_exit_and_solver_evidence(self):
        binaries = self._positional_scheduler()
        for index, submit_text in enumerate(('Submission accepted\n',
                'JOBID MESSAGE\n10000002 accepted\n10000001 accepted\n')):
            task = self.make_task('identity%d' % index)
            manifest = dict(self.manifest, candidate_id='identity%d' % index, tasks=[task])
            def runner(argv, **kwargs):
                if kwargs.get('guardian_launch_lock_fd') is not None:
                    os.close(kwargs['guardian_launch_lock_fd'])
                    completed = subprocess.run(['/bin/sh', argv[-1]], cwd=kwargs['cwd'], env=kwargs['env'],
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=5)
                    self.assertEqual(completed.returncode, 0)
                    return {'returncode': 0, 'stdout_tail': submit_text}
                self.fail('No query/cancel may guess an ID from missing or mixed submission records')
            context = self.context(command_runner=runner)
            context.policy['global'].update({'distribution': 'command', 'submit_argv': [binaries['batch-submit']], 'command': binaries['batch-submit']})
            with self.assertRaises(execution.NetlistExecutionError) as caught:
                context.evaluate(manifest)
            detail = caught.exception.detail
            self.assertEqual(detail['failure_stage'], 'submit')
            self.assertTrue(detail['submission_identity_failed'])
            self.assertFalse(detail['submission_observation']['accepted'])
            self.assertIsNone(detail['scheduler_job_id'])
            self.assertEqual(detail['receipt']['returncode'], 0)
            self.assertTrue(detail['receipt']['cleanup_proven'])
            self.assertFalse(context.unsafe.is_set())
            self.assertTrue(context.close()['cleanup_proven'])

    def test_scheduler_success_without_owned_solver_receipt_is_never_candidate_success(self):
        binaries = self._positional_scheduler()
        def runner(argv, **kwargs):
            if kwargs.get('guardian_launch_lock_fd') is not None:
                os.close(kwargs['guardian_launch_lock_fd'])
                return {'returncode': 0, 'stdout_tail': 'JOBID MESSAGE\n10000002 Submit job successfully.\n'}
            if argv[0] == binaries['batch-cancel']:
                return {'returncode': 0, 'stdout_tail': 'cancel accepted'}
            self.assertEqual(argv, [binaries['batch-query'], '10000002'])
            return {'returncode': 0, 'stdout_tail': 'JOB_ID 10000002\nJOB_STATE SUCCEEDED\nEXIT_CODE 0\n'}
        context = self.context(command_runner=runner)
        context.policy['global'].update({'distribution': 'command', 'submit_argv': [binaries['batch-submit']], 'command': binaries['batch-submit']})
        with mock.patch('crescendo_engine.netlist_execution.time.sleep'):
            clock = iter(range(100, 100000, 100))
            with mock.patch('crescendo_engine.netlist_execution.time.monotonic', side_effect=lambda: next(clock)):
                with self.assertRaises(execution.NetlistExecutionError) as caught:
                    context.evaluate(self.manifest)
        self.assertEqual(caught.exception.detail['scheduler_job_id'], '10000002')
        self.assertEqual(caught.exception.detail['scheduler_observation']['latest']['state'], 'SUCCEEDED')
        self.assertFalse(caught.exception.detail['scheduler_observation']['native_completion_authority'])
        self.assertEqual(caught.exception.detail['failure_stage'], 'submit')

    def test_exact_scheduler_failure_or_missing_receipt_stops_without_full_queue_wait(self):
        binaries = self._positional_scheduler()
        self.config['runtime']['cluster_queue_timeout_seconds'] = 3600
        self.config['runtime']['scheduler_receipt_grace_seconds'] = 0.05
        for index, state in enumerate(('FAILED', 'SUCCEEDED')):
            task = self.make_task('ended%d' % index)
            manifest = dict(self.manifest, candidate_id='ended%d' % index, tasks=[task])
            def runner(argv, **kwargs):
                if kwargs.get('guardian_launch_lock_fd') is not None:
                    os.close(kwargs['guardian_launch_lock_fd'])
                    return {'returncode': 0, 'stdout_tail': 'JOBID MESSAGE\n10000002 accepted\n'}
                self.assertEqual(argv, [binaries['batch-query'], '10000002'])
                return {'returncode': 0, 'stdout_tail': 'JOB_ID 10000002\nJOB_STATE %s\n' % state}
            context = self.context(command_runner=runner)
            context.policy['global'].update({'distribution': 'command', 'submit_argv': [binaries['batch-submit']], 'command': binaries['batch-submit']})
            started = time.monotonic()
            with self.assertRaises(execution.NetlistExecutionError) as caught:
                context.evaluate(manifest, timeout_seconds=600)
            self.assertLess(time.monotonic() - started, 2)
            self.assertTrue(caught.exception.detail['compute_receipt_missing'])
            self.assertEqual(caught.exception.detail['scheduler_terminal_state'], state)
            self.assertTrue(caught.exception.detail['scheduler_stop']['remote_stop_proven'])
            self.assertFalse(context.unsafe.is_set())
            self.assertTrue(context.close()['cleanup_proven'])

    def test_remote_exact_terminal_proof_joins_raw_unclean_receipt_through_point_commit(self):
        binaries = self._positional_scheduler()
        self.task = self.make_task(os.path.join('tasks', 'remoteproof'), delay=0.15)
        self.task['task_id'] = 'remoteproof'
        self.manifest['tasks'] = [self.task]
        wrappers, points = [], []
        original_render = execution.render_task_wrapper
        def render(*args, **kwargs):
            wrapper, files = original_render(*args, **kwargs)
            with open(wrapper) as stream:
                text = stream.read()
            atomic_write_text(wrapper, text.replace('  cr_clean=1', '  cr_clean=0'))
            return wrapper, files
        def runner(argv, **kwargs):
            if kwargs.get('guardian_launch_lock_fd') is not None:
                os.close(kwargs['guardian_launch_lock_fd'])
                wrappers.append(subprocess.Popen(['/bin/sh', argv[-1]], cwd=kwargs['cwd'], env=kwargs['env'],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True))
                return {'returncode': 0, 'stdout_tail': 'JOBID MESSAGE\n10000002 accepted\n'}
            self.assertEqual(argv, [binaries['batch-query'], '10000002'])
            state = 'SUCCEEDED' if os.path.exists(os.path.join(self.task['cwd'], 'execution', 'terminal.receipt')) else 'RUNNING'
            return {'returncode': 0, 'stdout_tail': 'JOB_ID 10000002\nJOB_STATE %s\nEXIT_CODE 0\n' % state}
        def commit(point, cancelled):
            points.append(copy.deepcopy(point))
            return {'ok': True, 'revision': 1, 'checkpoint_sha256': 'owned-fixture-checkpoint'}
        self.config['runtime']['execution_commands']['submission_template'] = (__import__('shlex').quote(binaries['batch-submit']) +
            ' -o {scheduler_stdout} -e {scheduler_stderr} {job_script}')
        context = self.context(command_runner=runner, point_commit_callback=commit)
        context.policy['global'].update({'distribution': 'command', 'submit_argv': [binaries['batch-submit']], 'command': binaries['batch-submit']})
        try:
            with mock.patch.object(execution, 'render_task_wrapper', side_effect=render):
                result = context.evaluate(self.manifest)
            row, point = result['task_results'][0], points[0]
            self.assertFalse(row['receipt']['cleanup_proven'])
            self.assertTrue(row['effective_terminal_receipt']['cleanup_proven'])
            self.assertEqual(row['termination_evidence']['source'], 'owned_scheduler_terminal')
            self.assertFalse(point['terminal_receipt']['cleanup_proven'])
            self.assertTrue(point['effective_terminal_receipt']['cleanup_proven'])
            self.assertTrue(row['point_raw_cleanup']['artifact_cleanup_complete'])
            tracking = row['scheduler_observation']
            fake = {'remote': True, 'token': row['token'], 'task': self.task,
                'scheduler_job_id': row['scheduler_job_id'], 'scheduler_observation': tracking,
                'submission_observation': row['submission_observation']}
            for changed in ({'token': 'f' * 32}, {'scheduler_job_id': '9999'},
                            {'task': dict(self.task, task_id='foreign')},
                            {'submission_observation': {'accepted': False, 'job_id': '10000002'}}):
                self.assertFalse(execution.task_termination_evidence(dict(fake, **changed), row['receipt'])['cleanup_proven'])
            self.assertTrue(context.close()['cleanup_proven'])
        finally:
            for process in wrappers:
                process.wait(timeout=5)

    def test_owner_heartbeat_tracks_current_active_stages_and_keeps_cancel_marker(self):
        context = execution.NetlistExecutionContext.__new__(execution.NetlistExecutionContext)
        context.lock, context.active, context.unsafe = threading.Lock(), {}, threading.Event()
        context.heartbeat_stop = mock.Mock()
        context.heartbeat_stop.wait.side_effect = [False, True]
        updated = []
        for stage in ('submit', 'solve', 'spectre', 'awaiting_measurement', 'measurement'):
            heartbeat = os.path.join(self.root, stage + '.heartbeat')
            cancel = os.path.join(self.root, stage + '.cancel')
            atomic_write_text(heartbeat, 'owned token\n')
            atomic_write_text(cancel, 'cancel requested\n')
            context.active[stage] = {'stage': stage, 'files': {'owner_heartbeat': heartbeat, 'cancel': cancel}}
        with mock.patch.object(execution.os, 'utime', side_effect=lambda path, value: updated.append(path)):
            context._heartbeat_loop()
        self.assertEqual(set(updated), set(os.path.join(self.root, stage + '.heartbeat') for stage in ('submit', 'solve', 'spectre')))
        self.assertFalse(context.unsafe.is_set())
        for row in context.active.values():
            with open(row['files']['cancel']) as stream:
                self.assertEqual(stream.read(), 'cancel requested\n')

    def test_remote_disappearance_keeps_receipt_completion_and_tracking_before_raw_retirement(self):
        binaries = self._positional_scheduler()
        self.task = self.make_task(os.path.join('tasks', 'remote'), delay=0.15)
        self.task['task_id'] = 'remote'
        self.manifest['tasks'] = [self.task]
        calls, wrappers, points = [], [], []
        scheduler_out = os.path.join(self.task['cwd'], 'execution', 'scheduler.stdout')
        scheduler_err = os.path.join(self.task['cwd'], 'execution', 'scheduler.stderr')
        def runner(argv, **kwargs):
            calls.append(argv)
            if kwargs.get('guardian_launch_lock_fd') is not None:
                os.close(kwargs['guardian_launch_lock_fd'])
                wrappers.append(subprocess.Popen(['/bin/sh', argv[-1]], cwd=kwargs['cwd'],
                    env=kwargs['env'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                    start_new_session=True))
                atomic_write_text(scheduler_out, 'TASK_INFO:\n JOB_ID: 10000002\n EXEC_NODE: fake-compute\n EXIT_CODE: 0\n')
                atomic_write_text(scheduler_err, '')
                return {'returncode': 0, 'stdout_tail': 'JOBID MESSAGE\n10000002 Submit job successfully.\n'}
            self.assertEqual(argv, [binaries['batch-query'], '10000002'])
            if sum(item[0] == binaries['batch-query'] for item in calls) == 1:
                return {'returncode': 0, 'stdout_tail': 'JOB_ID 10000002\nJOB_STATE RUNNING\nEXEC_NODE fake-compute\n'}
            return {'returncode': 1, 'stdout_tail': 'Job 10000002 not found\n'}
        def commit(point, cancelled):
            points.append(copy.deepcopy(point))
            return {'ok': True, 'revision': 1, 'checkpoint_sha256': 'owned-fixture-checkpoint'}
        self.config['runtime']['execution_commands']['submission_template'] = (__import__('shlex').quote(binaries['batch-submit']) +
            ' -o {scheduler_stdout} -e {scheduler_stderr} {job_script}')
        context = self.context(command_runner=runner, point_commit_callback=commit)
        context.policy['global'].update({'distribution': 'command', 'submit_argv':
            [binaries['batch-submit'], '-o', scheduler_out, '-e', scheduler_err], 'command': binaries['batch-submit']})
        try:
            result = context.evaluate(self.manifest)
            row, point = result['task_results'][0], points[0]
            self.assertEqual(row['scheduler_job_id'], '10000002')
            self.assertTrue(row['scheduler_observation']['latest']['not_found'])
            self.assertFalse(row['scheduler_observation']['latest']['terminal'])
            self.assertTrue(row['receipt']['cleanup_proven'])
            self.assertEqual(row['scheduler_observation']['task_id'], 'remote')
            self.assertEqual(row['scheduler_observation']['query_count'], 2)
            self.assertEqual(point['scheduler_job_id'], '10000002')
            self.assertEqual(point['solver_artifacts']['scheduler_job_id'], '10000002')
            self.assertEqual(result['execution_pipeline']['points'][0]['scheduler_observation'], row['scheduler_observation'])
            self.assertTrue(row['point_raw_cleanup']['artifact_cleanup_complete'])
            self.assertFalse(os.path.exists(self.task['psf_dir']))
            scheduler = [item for item in point['solver_artifacts']['artifacts'] if item['kind'] == 'scheduler_stdout'][0]
            self.assertEqual(scheduler['scheduler_task_observation']['job_id'], '10000002')
            self.assertFalse(scheduler['scheduler_task_observation']['native_completion_authority'])
        finally:
            for process in wrappers:
                process.wait(timeout=5)

    def test_persistent_unknown_queue_blocks_resume_until_same_token_cleanup(self):
        ownership = os.path.join(self.root, "private_netlist", "execution_ownership")
        control = os.path.join(self.task["cwd"], "execution")
        os.makedirs(control)
        token = "f" * 32
        files = dict((name, os.path.join(control, name)) for name in ("terminal.receipt", "cancel", "owner_heartbeat"))
        record = {"task": self.task, "files": files, "token": token, "remote": True}
        os.makedirs(ownership)
        path = os.path.join(ownership, "ownership.json")
        execution.atomic_write_json(path, {"schema_version": "crescendo.private_netlist_ownership.v1", "owned_run_root": self.root,
            "owner_token": "old", "active": {"candidate:task": record}, "unresolved": {}})
        original = sha256_file(path)
        proof = execution.reconcile_previous_ownership(ownership, self.root)
        self.assertFalse(proof["cleanup_proven"])
        self.assertTrue(os.path.isfile(files["cancel"]))
        self.assertEqual(sha256_file(path), original)
        with mock.patch("crescendo_engine.netlist_execution.run_command") as runner, self.assertRaises(execution.NetlistExecutionError):
            execution.NetlistExecutionContext(self.config, self.prepared, self.spec,
                ownership_root=ownership, owned_run_root=self.root)
        runner.assert_not_called()
        atomic_write_text(files["terminal.receipt"], execution.RECEIPT + "\t" + token + "\texited\t125\t1\n")
        self.assertTrue(execution.reconcile_previous_ownership(ownership, self.root)["cleanup_proven"])
        context = self.context(ownership_root=ownership, owned_run_root=self.root)
        self.assertFalse(context.unsafe.is_set())

    def test_prior_origin_root_reconcile_happens_before_new_context_submit(self):
        ownership = os.path.join(self.root, "prior", "execution_ownership")
        os.makedirs(ownership)
        control = os.path.join(self.task["cwd"], "execution")
        os.makedirs(control)
        record = {"task": self.task, "token": "a" * 32, "remote": True,
                  "files": dict((name, os.path.join(control, name)) for name in ("terminal.receipt", "cancel", "owner_heartbeat"))}
        execution.atomic_write_json(os.path.join(ownership, "ownership.json"), {
            "schema_version": "crescendo.private_netlist_ownership.v1", "owned_run_root": self.root,
            "active": {"old:task": record}, "unresolved": {}})
        with mock.patch("crescendo_engine.netlist_execution.run_command") as runner, self.assertRaises(execution.NetlistExecutionError):
            self.context(prior_ownership_roots=[{"ownership_root": ownership, "owned_run_root": self.root}])
        runner.assert_not_called()

    def test_stale_owner_heartbeat_prevents_queued_wrapper_launch(self):
        wrapper, files = execution.render_task_wrapper(self.task, self.root, "b" * 32, os.environ, 5)
        os.utime(files["owner_heartbeat"], (time.time() - 400, time.time() - 400))
        process = subprocess.Popen(["/bin/sh", wrapper], cwd=self.task["cwd"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        process.communicate(timeout=5)
        self.assertEqual(process.returncode, 125)
        terminal = execution.read_task_receipt(files["terminal.receipt"], "b" * 32, terminal=True)
        self.assertTrue(terminal["cleanup_proven"])
        self.assertFalse(os.path.exists(self.task["spectre_log"]))

    def test_completed_owner_record_is_bound_to_original_run_and_kept(self):
        context = self.context(ownership_root=os.path.join(self.root, "execution_ownership"), owned_run_root=self.root)
        context.evaluate(self.manifest)
        context.close()
        path = context.ownership_path
        with open(path, "r") as stream:
            doc = json.load(stream)
        self.assertEqual(doc["owned_run_root"], self.root)
        self.assertFalse(doc["active"])
        self.assertFalse(doc["unresolved"])
        self.assertTrue(execution.reconcile_previous_ownership(os.path.dirname(path), self.root)["cleanup_proven"])

    def test_large_output_catalog_does_not_bloat_ownership_or_block_resume(self):
        context = self.context(ownership_root=os.path.join(self.root, "execution_ownership"), owned_run_root=self.root)
        large_task = dict(self.task, outputs=[{"name": "output%d" % index, "expression": "1+" * 100 + "1"} for index in range(1000)],
                          variables=[{"name": "var%d" % index, "value": index} for index in range(1000)],
                          artifact_sha256={"unused%d" % index: "hash" for index in range(1000)})
        for index in range(100):
            control = os.path.join(self.root, "controls", str(index))
            os.makedirs(control)
            token = "%032x" % index
            files = dict((name, os.path.join(control, name)) for name in ("terminal.receipt", "cancel", "owner_heartbeat"))
            atomic_write_text(files["terminal.receipt"], execution.RECEIPT + "\t" + token + "\texited\t0\t1\n")
            context.active["large%d" % index] = {"task": large_task, "files": files, "token": token,
                                                "remote": True, "stage": "awaiting_measurement", "candidate_root": self.root}
        try:
            with context.lock:
                context._persist_ownership_locked()
            self.assertLess(os.path.getsize(context.ownership_path), 1024 * 1024)
            with open(context.ownership_path) as stream:
                document = json.load(stream)
            self.assertEqual(len(document["active"]), 100)
            self.assertNotIn("outputs", document["active"]["large0"]["task"])
            self.assertEqual(len(context.active["large0"]["task"]["outputs"]), 1000)
            self.assertTrue(execution.reconcile_previous_ownership(context.ownership_root, self.root)["cleanup_proven"])
            dependency = {"attempt_root": self.root, "cwd": self.root, "token": "measure", "extra_native_identity": "preserved"}
            context.active["large0"]["cleanup_dependencies"] = [dependency]
            with context.lock:
                context._persist_ownership_locked()
            with open(context.ownership_path) as stream:
                document = json.load(stream)
            self.assertEqual(document["active"]["large0"]["cleanup_dependencies"], [dependency])
        finally:
            context.active.clear()

    def test_local_measurement_pool_is_bounded_separately_from_spectre_capacity(self):
        self.prepared["job_policy"]["global"]["properties"]["maxjobs"] = "8"
        tasks = [self.make_task("meas%d" % index, point=index + 1) for index in range(8)]
        self.manifest["tasks"] = tasks
        active, peak = [0], [0]
        lock = threading.Lock()

        def measure(*args, **kwargs):
            with lock:
                active[0] += 1
                peak[0] = max(peak[0], active[0])
            try:
                time.sleep(0.1)
                return self.evaluator(*args, **kwargs)
            finally:
                with lock:
                    active[0] -= 1
        context = self.context(measurement_evaluator=measure)
        # Remote-command receipt fixture supplies concurrent solves; local
        # solver mode is intentionally serialized by the product policy.
        context.policy["global"].update(distribution="command", command="/bin/sh", submit_argv=["/bin/sh"])
        result = context.evaluate(self.manifest)
        self.assertEqual(len(result["task_results"]), 8)
        self.assertEqual(context.policy["max_jobs"], 8)
        self.assertEqual(peak[0], 2)
        self.assertEqual(context.policy["measurement_jobs"], 2)

    def test_numeric_output_failure_is_design_failure_and_keeps_cleanup_proven(self):
        def bad_output(*args, **kwargs):
            raise execution.NetlistMeasurementError("expression returned nil", infrastructure=False)
        context = self.context(measurement_evaluator=bad_output)
        timing = {"phases": {}}
        with self.assertRaises(execution.NetlistExecutionError) as caught:
            context.evaluate(self.manifest, timing=timing)
        self.assertEqual(timing["tasks"][0]["status"], "failed")
        self.assertIsNotNone(timing["phases"]["result_collection"]["elapsed_seconds"])
        self.assertFalse(caught.exception.infrastructure)
        self.assertFalse(context.unsafe.is_set())
        self.assertTrue(context.close()["cleanup_proven"])

    def test_native_failed_summary_is_design_but_license_and_unknown_exit_are_infrastructure(self):
        cases = (
            ("spectre completes with 4 errors, 1 warning.\\n", 1, False),
            ("lmStatus: ERROR (LMC-01902): License call failed.\\n", 2, True),
            ("launcher could not initialize\\n", 2, True),
        )
        for index, (text, code, infrastructure) in enumerate(cases):
            task = self.make_task("failure%d" % index)
            script = task["argv"][1]
            atomic_write_text(script, "import sys\nopen(%r,'w').write(%r)\nsys.exit(%d)\n" % (
                task["spectre_log"], text, code))
            context = self.context()
            manifest = dict(self.manifest, candidate_id="failure%d" % index, tasks=[task])
            with self.subTest(code=code, infrastructure=infrastructure), self.assertRaises(execution.NetlistExecutionError) as caught:
                context.evaluate(manifest)
            self.assertEqual(caught.exception.infrastructure, infrastructure)
            self.assertIn("native_failure_evidence", caught.exception.detail)
            self.assertTrue(context.close()["cleanup_proven"])

    def test_unproven_ocean_cleanup_is_retained_despite_spectre_finished_receipt(self):
        def uncertain_output(task, config, root, **kwargs):
            attempt = os.path.join(os.path.dirname(task["psf_dir"]), "measurement")
            os.makedirs(attempt)
            kwargs["ownership_callback"]({"attempt_root": attempt, "cwd": task["cwd"], "token": "native_measure_token"})
            raise execution.NetlistMeasurementError("Ocean cleanup unknown", {"command": {"termination_failed": True}})
        context = self.context(measurement_evaluator=uncertain_output)
        with self.assertRaises(execution.NetlistExecutionError):
            context.evaluate(self.manifest)
        self.assertTrue(context.unsafe.is_set())
        proof = context.close()
        self.assertFalse(proof["cleanup_proven"])
        self.assertTrue(proof["retained"])


if __name__ == "__main__":
    unittest.main()
