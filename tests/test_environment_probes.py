"""Only the additive qualification observations; no engine regression suite."""
import json
import os
import stat
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

from crescendo_engine import environment_probes as probes


class EnvironmentProbeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="crescendo-environment-probes-")
        self.root = self.temp.name
        self.config = {"cadence": {"project_dir": self.root}, "runtime": {
            "data_root_override": self.root, "run_root_origin": "derived"}}
        self.prepared = {"job_policy": {"available": True, "source": "invoking_session",
            "global": {"available": True, "properties": {"distributionmethod": "Local", "maxjobs": "2"}},
            "tests": []}}

    def tearDown(self):
        self.temp.cleanup()

    def executable(self, name, source):
        path = os.path.join(self.root, name)
        with open(path, "w", encoding="utf-8") as stream:
            stream.write("#!%s\n%s\n" % (sys.executable, source))
        os.chmod(path, 0o700)
        return path

    def command_policy(self):
        import shlex
        self.config['runtime']['execution_commands'] = {
            'scheduler_query_template': shlex.quote(os.path.join(self.root, 'batch-query')) + ' -l {job_id}',
            'scheduler_cancel_template': shlex.quote(self.executable('batch-cancel', 'raise RuntimeError("read-only probe must not cancel")')) + ' {job_id}'}
        self.prepared["job_policy"]["global"]["properties"].update(
            distributionmethod="Command", jobsubmitcommand=os.path.join(self.root, "batch-submit") + " -q short")

    def test_literal_argv_never_evaluates_shell_text(self):
        marker = os.path.join(self.root, "should-not-exist")
        argument = "$(touch %s); `touch %s`" % (marker, marker)
        result = probes.bounded_command([sys.executable, "-c", "import sys;print(sys.argv[1])", argument], timeout=1)
        self.assertEqual(result["returncode"], 0)
        self.assertIn(argument, result["stdout_tail"])
        self.assertFalse(os.path.exists(marker))

    def test_timeout_kills_only_owned_descendant_group_and_closes_pipes(self):
        script = ("import subprocess,sys,time; p=subprocess.Popen([sys.executable,'-c','import time;time.sleep(20)']);"
                  "print(p.pid,flush=True); time.sleep(20)")
        started = time.monotonic()
        result = probes.bounded_command([sys.executable, "-u", "-c", script], timeout=0.1)
        self.assertTrue(result["timed_out"])
        self.assertLess(time.monotonic() - started, 1.0)
        child_pid = int(result["stdout_tail"].strip())
        # Delivery to another process is asynchronous even after the parent
        # was reaped. Keep the command's existing <1 s budget above, then wait
        # briefly for the separately scheduled child to consume its signal.
        deadline = time.monotonic() + 0.5
        while True:
            try:
                with open("/proc/%d/stat" % child_pid) as stream:
                    state = stream.read().rsplit(")", 1)[1].split()[0]
            except (FileNotFoundError, ProcessLookupError):
                break
            if state == "Z":
                break
            if time.monotonic() >= deadline:
                self.fail("Owned descendant did not terminate: " + state)
            time.sleep(0.005)

    def test_output_flood_is_bounded_and_stopped(self):
        result = probes.bounded_command([sys.executable, "-c",
            "import os\nwhile True: os.write(1,b'x'*16384)"], timeout=2, output_limit=32768)
        self.assertTrue(result["output_limited"])
        self.assertLessEqual(len(result["stdout_tail"].encode("utf-8")), probes.OUTPUT_TAIL)
        self.assertIsNotNone(result["returncode"])

    def test_cancelled_probe_never_starts_a_command(self):
        event = threading.Event()
        event.set()
        with mock.patch.object(probes.subprocess, "Popen") as launch:
            result = probes.bounded_command(["anything"], cancel=event)
        self.assertTrue(result["cancelled"])
        launch.assert_not_called()

    def test_one_failed_or_slow_probe_does_not_hide_later_checks(self):
        reporter = probes._Reporter(None, None, {})
        order = []
        def slow():
            time.sleep(0.1)
            order.append("slow")
            return "FAIL", "owned timeout fixture", {}
        def failed():
            raise ValueError("independent failure")
        def fast():
            order.append("fast")
            return "PASS", "continued", {}
        probes._parallel_checks(reporter, [("slow", slow), ("exception", failed), ("fast", fast)])
        self.assertEqual(order, ["fast", "slow"])
        by_id = dict((row["id"], row) for row in reporter.checks)
        self.assertEqual(by_id["exception"]["status"], "FAIL")
        self.assertEqual(by_id["fast"]["status"], "PASS")

    def test_license_and_token_values_are_redacted_without_env_dump(self):
        reporter = probes._Reporter(None, None, {"CDS_LIC_FILE": "27000@secret-host", "API_TOKEN": "private-token"})
        result = reporter.add("fixture", "FAIL", "27000@secret-host private-token",
                              {"output": "private-token 27000@secret-host", "command": "batch-submit -q short"})
        text = json.dumps(result)
        self.assertNotIn("secret-host", text)
        self.assertNotIn("private-token", text)
        self.assertIn("batch-submit -q short", text)

    def test_structured_secrets_and_distinct_caller_license_are_redacted(self):
        command = 'batch-submit -A current-account -q short -R "cpu=6;mem=20000"'
        safe_sha = "ab" * 32
        # The actual caller/configured license is different from the GUI
        # process: the manager passes its merged private launch environment.
        caller_environment = {"CDS_LIC_FILE": "28000@caller-only-license",
                              "LM_LICENSE_FILE": "29000@configured-license"}
        record = {"nonce": "not-in-env-nonce", "worker_token": "not-in-env-token",
            "password": "p4ss", "credentials": {"private_key": "not-in-env-key"},
            "secret": "not-in-env-secret", "cookie": "session-cookie", "api_key": "not-in-env-api-key",
            "nonce_matched": True, "source_sha256": safe_sha, "command": command,
            "submit_argv": ["batch-submit", "-A", "current-account", "-R", "cpu=6;mem=20000"],
            "native_snapshot": {"schema": "crescendo.native_environment.v1", "values": {"PATH": "/private/caller/path"}},
            "env": {"values": {"PASSWORD": "should-never-be-collected"}},
            "launch_environment": {"LD_LIBRARY_PATH": "/private/cadence/library"},
            "error": "failed 28000@caller-only-license and 29000@configured-license"}
        result = probes._redact(record, caller_environment)
        serialized = json.dumps(result)
        for secret in ("not-in-env-nonce", "not-in-env-token", "not-in-env-key", "p4ss", "session-cookie",
                       "private/caller", "private/cadence", "should-never-be-collected", "caller-only-license", "configured-license"):
            self.assertNotIn(secret, serialized)
        self.assertEqual(result["source_sha256"], safe_sha)
        self.assertEqual(result["command"], command)
        self.assertEqual(result["submit_argv"], record["submit_argv"])
        self.assertTrue(result["nonce_matched"])

    def test_report_write_failure_does_not_prevent_independent_checks(self):
        def emit(_row):
            raise OSError(122, "report quota exceeded")
        reporter = probes._Reporter(emit, None, {})
        reporter.check("first", lambda: ("PASS", "first observation", {}))
        reporter.check("next", lambda: ("PASS", "independent observation", {}))
        self.assertEqual([row["id"] for row in reporter.checks], ["first", "next"])
        self.assertEqual(len(reporter.publish_errors), 2)
        self.assertTrue(all("report_publish_error" in row for row in reporter.checks))

    def test_invalid_native_snapshot_cleans_just_created_probe_home(self):
        self.config["cadence"]["launch_environment"] = {"unsupported": "fixture"}
        with self.assertRaises(ValueError):
            probes._native_context(self.config, self.root)
        self.assertFalse(any(name.startswith("environment_probe_") for name in os.listdir(self.root)))

    def test_optional_python_extension_missing_is_warning_not_engine_failure(self):
        original = probes._json_probe
        def json_probe(source, *args, **kwargs):
            if source == probes._PYTHON_PROBE:
                return "PASS", "fixture", {"imports": {"sqlite3": {"ok": False}, "json": {"ok": True}},
                    "tk": {"ok": True, "display_status": "PASS"}}
            return original(source, *args, **kwargs)
        with mock.patch.object(probes, "_json_probe", side_effect=json_probe):
            checks = probes._run_expanded_environment_probes(self.config, self.prepared, self.root)
        python_check = next(row for row in checks if row["id"] == "python.dependencies_and_tk")
        self.assertEqual(python_check["status"], "WARN")
        self.assertEqual(python_check["details"]["optional_extension_failures"], ["sqlite3"])

    def test_tiny_filesystem_write_exposes_edquot_not_free_space_as_quota(self):
        # Inject a real errno from the child; no large allocation is attempted.
        with mock.patch.object(probes, "_FILESYSTEM_PROBE", probes._FILESYSTEM_PROBE.replace(
                'temporary=tempfile.mkdtemp(prefix=".crescendo-qualification-",dir=existing)',
                'raise OSError(errno.EDQUOT,"Disk quota exceeded")')):
            _status, _message, details = probes._json_probe(probes._FILESYSTEM_PROBE,
                [self.root], self.root, os.environ, None)
        self.assertFalse(details["write_probe"]["ok"])
        self.assertEqual(details["write_probe"]["errno"], 122)
        self.assertEqual(details["quota_status"], "NOT_TESTED")

    def test_no_job_ids_never_runs_batch_query(self):
        with mock.patch.object(probes, "bounded_command") as command:
            checks = probes.query_scheduler_jobs(self.config, self.prepared, self.root, [])
        self.assertEqual(checks[0]["status"], "NOT_TESTED")
        command.assert_not_called()

    def test_configured_templates_issue_only_exact_owned_job_queries(self):
        submitted = os.path.join(self.root, "unexpected-submit")
        queried = os.path.join(self.root, "query-argv.jsonl")
        self.executable("batch-submit", "import sys\nif sys.argv[1:]==['--help']: print('Usage: batch-submit [options]\\n -x all Export environment\\n -EP directory Execution directory')\nelse: open(%r,'w').write('submitted')" % submitted)
        self.executable("batch-query", """import json,sys
with open(%r,'a') as stream: stream.write(json.dumps(sys.argv[1:])+'\\n')
if sys.argv[1:] in (['--help'],['-h']): print('Usage: batch-query job_id\\n -l list detailed Job ID information')
elif len(sys.argv)==3 and sys.argv[1]=='-l': print('Job ID: '+sys.argv[2]+'\\nState: RUNNING')
else: raise RuntimeError('Forbidden unscoped listing')""" % queried)
        self.command_policy()
        rows = [{"job_id": "21", "shared": True, "scope": "session_pool", "source": "current_owned_ledger"},
                {"job_id": "22", "candidate_id": "g000_c0000", "test": "testA", "corner": "tt"},
                {"job_id": "21"}, {"job_id": ";touch bad"}]
        checks = probes.query_scheduler_jobs(self.config, self.prepared, self.root, rows)
        by_id = dict((row["id"], row) for row in checks)
        self.assertEqual(by_id["scheduler.job.21"]["status"], "PASS")
        self.assertTrue(by_id["scheduler.job.21"]["details"]["shared"])
        self.assertIsNone(by_id["scheduler.job.21"]["details"]["candidate_id"])
        self.assertEqual(by_id["scheduler.job.22"]["details"]["corner"], "tt")
        self.assertEqual(by_id["scheduler.invalid_job_ids"]["status"], "WARN")
        self.assertFalse(os.path.exists(submitted))
        with open(queried) as stream:
            actual = [json.loads(line) for line in stream]
        self.assertEqual(set(tuple(row) for row in actual), {("-l", "21"), ("-l", "22")})
        self.assertFalse(any(name.startswith("environment_probe_") for name in os.listdir(self.root)))

    def test_unknown_batch_query_help_does_not_guess_query_syntax(self):
        self.executable("batch-submit", "raise RuntimeError('must never submit')")
        self.executable("batch-query", "print('Unsupported version: see admin')")
        self.command_policy()
        self.config["runtime"]["execution_commands"]["scheduler_query_template"] = ""
        checks = probes.query_scheduler_jobs(self.config, self.prepared, self.root, ["123"])
        self.assertFalse(any(row["id"].startswith("scheduler.job.") for row in checks))
        self.assertTrue(any(row["id"] == "scheduler.jobs" and row["status"] == "NOT_TESTED" for row in checks))

    def test_configured_positional_template_queries_saved_exact_task_without_help_dependency(self):
        self.executable('batch-submit', "import sys\nassert sys.argv[1:]==['--help']\nprint('optional transport help')")
        self.executable('batch-query', "import sys\nassert sys.argv[1:]==['10000001']\nprint('JOB_ID      10000001\\nJOB_STATE   SUCCEEDED\\nSUBMIT_NODE submit01')")
        self.command_policy()
        self.command_policy()
        config = __import__('copy').deepcopy(self.config)
        config['runtime']['execution_commands']['scheduler_query_template'] = __import__('shlex').quote(os.path.join(self.root, 'batch-query')) + ' {job_id}'
        checks = probes.query_scheduler_jobs(config, self.prepared, self.root,
            [{'job_id': '10000001', 'candidate_id': 'candidate', 'task_id': 'main_TT_1', 'point_id': 1}])
        capability = next(row for row in checks if row['id'] == 'scheduler.query_capability')
        self.assertEqual(capability['details']['source'], 'user_templates')
        self.assertTrue(capability['details']['submission_not_performed'])
        observed = next(row for row in checks if row['id'] == 'scheduler.job.10000001')
        self.assertEqual(observed['details']['scheduler_observation']['state'], 'SUCCEEDED')
        self.assertEqual(observed['details']['task_id'], 'main_TT_1')
        self.assertEqual(observed['details']['point_id'], 1)
        self.assertFalse(observed['details']['native_completion_authority'])

    def test_complete_submission_template_selects_actual_scheduler_over_saved_local_policy(self):
        self.executable('batch-submit', "import sys\nassert sys.argv[1:]==['--help']\nprint('help only')")
        self.executable('batch-query', "import sys\nassert sys.argv[1:]==['21']\nprint('JOB_ID 21\\nJOB_STATE RUNNING')")
        self.command_policy()
        config = __import__('copy').deepcopy(self.config)
        config['runtime']['execution_commands']['scheduler_query_template'] = __import__('shlex').quote(os.path.join(self.root, 'batch-query')) + ' {job_id}'
        spec = {'execution_commands': {'submission_template': os.path.join(self.root, 'batch-submit') + ' -A current-account -q short -R "cpu=8;mem=8000" -x all -EP {cwd} {job_script}'}}
        checks = probes.query_scheduler_jobs(config, self.prepared, self.root, ['21'], spec=spec)
        observed = next(row for row in checks if row['id'] == 'scheduler.job.21')
        self.assertEqual(observed['details']['scheduler_observation']['state'], 'RUNNING')

    def test_failed_help_cannot_authorize_a_successful_unrelated_later_attempt(self):
        self.executable("batch-submit", "print('Usage: batch-submit [options]')")
        self.executable("batch-query", "import sys\nif sys.argv[1:]==['--help']: print('Usage: batch-query JOBID'); sys.exit(7)\nelif sys.argv[1:]==['-h']: print('authentication status unavailable')\nelse: raise RuntimeError('no dialect was proved')")
        self.command_policy()
        self.config["runtime"]["execution_commands"]["scheduler_query_template"] = ""
        checks = probes.query_scheduler_jobs(self.config, self.prepared, self.root, ["21"])
        capability = next(row for row in checks if row["id"] == "scheduler.query_capability")
        self.assertEqual(capability["status"], "FAIL")
        self.assertIn("scheduler_query_template", capability["message"])
        self.assertFalse(any(row["id"].startswith("scheduler.job.") for row in checks))

    def test_job_state_and_query_rc_are_separate_and_transport_help_is_never_invoked(self):
        capture = os.path.join(self.root, "batch-submit-help-calls.txt")
        self.executable("batch-submit", "import sys\nassert sys.argv[1:]==['--help']\nwith open(%r,'a') as f: f.write('help\\n')\nprint('Usage: batch-submit [options]')" % capture)
        self.executable("batch-query", "import sys\nif sys.argv[1:]==['--help']: print('Usage: batch-query JOBID')\nelse: print('JOB_ID JOB_NAME USER JOB_STATE QUEUE\\n21 test user SUCCEEDED short')")
        self.command_policy()
        cache = {}
        for _ in range(2):
            checks = probes.query_scheduler_jobs(self.config, self.prepared, self.root, ["21"], capability_cache=cache)
            row = next(row for row in checks if row["id"] == "scheduler.job.21")
            self.assertEqual(row["details"]["query_returncode"], 0)
            self.assertEqual(row["details"]["scheduler_observation"]["state"], "SUCCEEDED")
            self.assertTrue(row["details"]["scheduler_observation"]["terminal"])
            self.assertFalse(row["details"]["native_completion_authority"])
        self.assertFalse(os.path.exists(capture))

    def test_missing_job_is_an_observation_and_never_a_completion_proof(self):
        self.executable("batch-submit", "print('Usage: batch-submit [options]')")
        self.executable("batch-query", "import sys\nif sys.argv[1:]==['--help']: print('Usage: batch-query JOBID')\nelse: print('Job 21 not found')")
        self.command_policy()
        checks = probes.query_scheduler_jobs(self.config, self.prepared, self.root, ["21"])
        row = next(row for row in checks if row["id"] == "scheduler.job.21")
        self.assertTrue(row["details"]["scheduler_observation"]["not_found"])
        self.assertFalse(row["details"]["scheduler_observation"]["terminal"])
        self.assertIn("not simulator completion", row["message"])

    def test_completed_job_disappearance_with_nonzero_query_is_warning_not_simulation_failure(self):
        self.executable('batch-submit', "print('Usage: batch-submit [options]')")
        self.executable('batch-query', "import sys\nif sys.argv[1:]==['--help']: print('Usage: batch-query JOBID')\nelse: print('Job 21 not found'); sys.exit(1)")
        self.command_policy()
        checks = probes.query_scheduler_jobs(self.config, self.prepared, self.root, ['21'])
        row = next(row for row in checks if row['id'] == 'scheduler.job.21')
        self.assertEqual(row['status'], 'WARN')
        self.assertEqual(row['details']['query_returncode'], 1)
        self.assertTrue(row['details']['scheduler_observation']['not_found'])
        self.assertFalse(row['details']['scheduler_observation']['terminal'])
        self.assertFalse(row['details']['native_completion_authority'])

    def test_all_invalid_job_ids_remain_explicitly_not_tested(self):
        self.executable("batch-submit", "raise RuntimeError('must never submit')")
        self.executable("batch-query", "print('Usage: batch-query job_id\\n -l list details')")
        self.command_policy()
        checks = probes.query_scheduler_jobs(self.config, self.prepared, self.root, ["all", "-a", "bad;id"])
        self.assertFalse(any(row["id"].startswith("scheduler.job.") for row in checks))
        self.assertTrue(any(row["id"] == "scheduler.jobs" and row["status"] == "NOT_TESTED" for row in checks))

    def test_probe_home_cleanup_is_run_after_cancellation(self):
        event = threading.Event()
        self.executable("batch-submit", "raise RuntimeError('must never submit')")
        self.executable("batch-query", "print('Usage: batch-query job_id\\n -l list details')")
        self.command_policy()
        def emit(row):
            if row["id"] == "scheduler.query_capability":
                event.set()
        checks = probes.query_scheduler_jobs(self.config, self.prepared, self.root, ["123"], emit, event)
        self.assertEqual(checks[-1]["id"], "scheduler.probe_home_cleanup")
        self.assertEqual(checks[-1]["status"], "PASS")
        self.assertFalse(any(name.startswith("environment_probe_") for name in os.listdir(self.root)))

    def test_current_overrides_win_and_callbacks_are_only_metadata(self):
        self.prepared["parameters"] = [
            {"parameter_id": "wanted", "name": "w", "callback": "dangerousCallback()", "minimum": "1u"},
            {"parameter_id": "unselected", "name": "length", "callback": "other()"}]
        captured = dict(self.prepared["job_policy"]["global"]["properties"])
        events = []
        checks = probes._run_expanded_environment_probes(self.config, self.prepared, self.root, events.append, spec={
            "maestro_job_overrides": {"command": "not_installed_scheduler -q current", "max_jobs": 7},
            "variables": [{"parameter_id": "wanted"}]})
        by_id = dict((row["id"], row) for row in checks)
        callback = by_id["pdk.cdf_metadata"]["details"]
        self.assertEqual(callback["selected_count"], 1)
        self.assertTrue(callback["parameters"][0]["callback_present"])
        self.assertEqual(callback["parameters"][0]["callback_execution"], "NOT_TESTED")
        self.assertNotIn("dangerousCallback", json.dumps(callback))
        policy = by_id["scheduler.current_policy"]["details"]["policies"][0]
        self.assertEqual(policy["max_jobs"], "7")
        self.assertEqual(policy["command"], "not_installed_scheduler -q current")
        self.assertEqual(self.prepared["job_policy"]["global"]["properties"], captured)
        self.assertEqual(by_id["native.version.virtuoso_bin"]["status"], "FAIL")
        self.assertEqual(by_id["storage.filesystem.simulation"]["status"], "PASS")
        self.assertEqual(by_id["native.probe_home_cleanup"]["status"], "PASS")
        self.assertEqual(len(events), len(checks))
        self.assertFalse(any(name.startswith(".crescendo-qualification-") or name.startswith("environment_probe_") for name in os.listdir(self.root)))

    def test_current_minimal_summary_uses_inspected_locator_names(self):
        prepared = dict(self.prepared, setup={'source': {'maestro_sdb': '/current/L/C/maestro/maestro.sdb'},
            'setup_locator': {'library_name': 'L', 'cell_name': 'C', 'view_name': 'maestro'}, 'tests': []})
        with mock.patch.object(probes, 'bounded_command') as command:
            checks = probes.run_environment_probes(self.config, prepared, self.root)
        self.assertEqual(len(checks), 1)
        source = checks[0]['details']['setup']
        self.assertEqual([source[key] for key in ('library', 'cell', 'view')], ['L', 'C', 'maestro'])
        self.assertEqual(source['maestro_sdb'], '/current/L/C/maestro/maestro.sdb')
        command.assert_not_called()

    def test_runtime_success_retains_only_cached_tool_paths_and_alps_discovery(self):
        from crescendo_engine.diagnosis_catalog import format_section
        cadence = self.config["cadence"]
        cadence.update(ocean_bin="/eda/ic/bin/ocean",spectre_bin="/eda/spectre/bin/spectre",alps_bin="/eda/alps/v/bin/alps",
            spectre_model_dir="/pdk/current/spectre",alps_model_dir="/pdk/current/alps",
            license_file="LICENSE_SENTINEL",launch_environment={"TOKEN":"ENV_SENTINEL"},discovery={
                "effective":{"alps_root":"/eda/alps/v","alps_bin":"/eda/alps/v/bin/alps","license_file":"LICENSE_SENTINEL"},
                "sources":{"alps_root":"environment:ALPS_HOME","alps_bin":"scoped_root:ALPS_HOME","license_file":"LICENSE_SENTINEL"},
                "checks":[{"name":"alps_bin","resolution":{"status":"selected","message":"unique cached launcher",
                    "candidates":["/eda/alps/v/bin/alps"],"launch_environment":{"TOKEN":"ENV_SENTINEL"}}}],
                "launch_environment":{"TOKEN":"ENV_SENTINEL"}})
        spec = {"_diagnostic_package_identity":{"application_version":"fixture"}, "execution_backend": "private_netlist", "simulator": "alps"}
        with mock.patch.object(probes,"bounded_command",side_effect=AssertionError("no native probe")), \
             mock.patch("crescendo_engine.environment_diagnostics.package_identity",side_effect=AssertionError("no new identity read")):
            checks = probes.run_environment_probes(self.config,self.prepared,self.root,spec=spec)
        self.assertEqual(len(checks),1)
        self.assertEqual(checks[0]["status"],"PASS")
        self.assertEqual(checks[0]["details"]["netlist_sources"], ["maestro_baseline", "private_netlist", "maestro_netlist"])
        self.assertEqual(checks[0]["details"]["selected_simulator"], "alps")
        body = format_section(checks)
        for value in ("/eda/ic/bin/ocean","/eda/spectre/bin/spectre","/eda/alps/v/bin/alps",
                      "/pdk/current/spectre", "/pdk/current/alps",
                      "environment:ALPS_HOME","scoped_root:ALPS_HOME","unique cached launcher"):
            self.assertIn(value,body)
        self.assertIn('"candidate_count": 1',body)
        self.assertNotIn("tools_sha256",body)
        self.assertNotIn("alps_discovery_sha256",body)
        self.assertNotIn("LICENSE_SENTINEL",body)
        self.assertNotIn("ENV_SENTINEL",body)
        self.assertNotIn("launch_environment",body)

    def test_runtime_discovery_summary_limits_candidates_and_marks_truncated_fields(self):
        from crescendo_engine.diagnosis_catalog import brief_success
        long_path = "/" + "x"*5000
        summary = probes._runtime_tool_summary({"ocean_bin":long_path,"spectre_bin":long_path,"alps_bin":long_path,
            "discovery":{"effective":{"alps_root":long_path,"alps_bin":long_path},
                "sources":{"alps_root":"source"*500,"alps_bin":"source"*500},
                "checks":[{"name":"alps_bin","resolution":{"status":"selected","message":"m"*5000,
                    "candidates":[long_path]*20}}]}})
        retained = brief_success(summary)
        alps = retained["alps_discovery"]
        self.assertEqual(alps["candidate_count"],20)
        self.assertEqual(alps["omitted_candidate_count"],16)
        self.assertEqual(len(alps["candidates"]),4)
        self.assertIn("ocean_bin",alps["truncated_fields"])
        self.assertIn("...[truncated]",retained["tools"]["ocean_bin"])
        self.assertLess(len(json.dumps(retained,ensure_ascii=False).encode("utf-8")),15000)
        forged = brief_success({"tools":{"ocean_bin":"/eda/ocean","license_file":"SECRET"},
            "alps_discovery":{"alps_root":"/eda/alps","launch_environment":{"TOKEN":"SECRET"}}})
        self.assertNotIn("SECRET",json.dumps(forged))

    def test_current_summary_keeps_unknown_identity_without_borrowing_history(self):
        prepared = dict(self.prepared, setup={'source': {}, 'histories': [{'library': 'old', 'cell': 'foreign'}]})
        checks = probes.run_environment_probes(self.config, prepared, self.root)
        self.assertIsNone(checks[0]['details']['setup']['library'])
        self.assertIsNone(checks[0]['details']['setup']['cell'])


if __name__ == "__main__":
    unittest.main()
