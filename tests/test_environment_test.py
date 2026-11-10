"""Only the additive diagnostic manager; no native simulator is launched here."""
import copy
import json
import os
import queue
import tarfile
import tempfile
import threading
import time
import unittest
from unittest import mock

from crescendo_engine.config import DEFAULTS
from crescendo_engine.diagnosis_catalog import read_section
from crescendo_engine.environment_test import (EnvironmentTestManager, compact_diagnostic_run, _schema_artifact_index,
                                                cleanup_test_runtime_lease, _request_gate_failure)
from crescendo_engine.continuation import (ContinuationLease, RuntimeOptimizationLease,
                                          continuation_checkpoint_path)
from crescendo_engine.metrics import evaluate_metrics
from crescendo_engine.optimizers import PopulationGenerator, validate_variables
from crescendo_engine.run_data_store import RunDataStore, export_dataset


class FixtureService(object):
    """Run-registration/thread contract with explicitly simulated evidence."""
    def __init__(self, root, behaviors=None):
        self.root = root
        self.lock = threading.RLock()
        self.config = copy.deepcopy(DEFAULTS)
        self.config["runtime"].update(
            run_root=os.path.join(root, "simulation", "crescendo", "fixture_TB"),
            design_root=os.path.join(root, "work", ".crescendo", "fixture_TB"),
            environment_test_stop_timeout_seconds=1)
        self.prepared = {"prepared-fixture": {
            "setup": {"tests": [{"name": "main", "enabled": True, "simulator": "spectre"}],
                      "corners": [{"name": "TT", "enabled": True},
                                  {"name": "disabled", "enabled": False}]},
            "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True,
                            "name": "w", "current_value": "1u", "callback": "fixtureCallback()"}],
            "outputs": [{"key": "main::gain"}, {"key": "main::power"}],
            "job_policy": {"available": True, "source": "invoking_session", "global": {
                "properties": {"distributionmethod": "Command", "maxjobs": "50",
                               "jobsubmitcommand": 'batch-submit -q current -R "cpu=6;mem=20000"'}}},
        }}
        self.threads = {}
        self.starts, self.stops, self.progress_calls, self.roots = [], [], [], {}
        self.exports = []
        self.datasets, self.snapshots, self.run_cancels = {}, {}, {}
        self.thread_errors = []
        self.behaviors = behaviors or {}
        self.start_entered, self.start_release = threading.Event(), threading.Event()
        self.start_release.set()
        self.manager = None
        self.closing = False
        self.formal_root = os.path.join(root, "formal_run")
        os.makedirs(self.formal_root)
        with open(os.path.join(self.formal_root, "unchanged.txt"), "w", encoding="utf-8") as stream:
            stream.write("Original run must not be edited.\n")

    def _freeze_run_config(self, spec, prepared):
        return copy.deepcopy(self.config)

    def start(self, prepared_id, spec):
        self.starts.append(copy.deepcopy(spec))
        engine = spec["execution_backend"]
        # Match service.start: destination is frozen before a native launch
        # may block; cancellation must also cover a run registered afterwards.
        parent = self.manager.run_root(spec["environment_test"], engine)
        self.start_entered.set()
        if not self.start_release.wait(5):
            raise RuntimeError("fixture launch gate was not released")
        behavior = self.behaviors.get(spec["simulator"] + "." + engine, self.behaviors.get(engine, "completed"))
        if behavior in ("launch_failure", "launch_cleanup_failed", "launch_unsafe_to_optimize"):
            error = RuntimeError("fixture private workspace preparation failed")
            if behavior != "launch_failure":
                setattr(error, behavior[len("launch_"):], True)
            raise error
        name = "fixture_owned_%03d" % len(self.starts)
        root = os.path.join(parent, name)
        os.makedirs(os.path.join(root, "native"))
        self.roots[name] = root
        cancel = threading.Event()
        self.run_cancels[name] = cancel
        values = copy.deepcopy(spec["environment_test"]["frozen_candidate"])
        parsed = {"output_map": {"main::gain": {"summary": {"mean": 4.0}},
                                 "main::power": {"summary": {"mean": 1.0}}}}
        candidate = {
            "candidate_id": "g000_c0000", "generation": 0, "index": 0,
            "values": values, "status": "running", "execution_backend": engine, "simulator": spec["simulator"],
            "execution_progress": {"stage": "running", "tasks": [{"test": "main", "corner": "TT", "point_id": 1,
                                                                        "state": "running", "job_id": "12345"}]},
            "source_guard": {"ok": True},
            "parameter_application": {"ok": True, "parameters": [{"parameter_id": "x", "requested": values["x"],
                                                                      "callback_status": "executed"}]},
            "metrics": evaluate_metrics(parsed, spec["objectives"], spec.get("constraints", [])),
        }
        status = {"schema_version": "crescendo.run_status.v1", "run_id": name, "run_root": root,
                  "state": "running", "message": "fixture native run", "total_candidates": spec["population"] * spec["generations"],
                  "completed_candidates": 0}
        self.snapshots[name] = {"status": status, "candidates": [candidate], "execution": {
            "commands": [{"argv": ["fixture_ocean", "-nograph"], "state": "running"}]}}
        if behavior == "shared_job_ids":
            candidate["execution_progress"]["tasks"][0].pop("job_id")
            self.snapshots[name]["execution"]["commands"].append({
                "kind": "native_submission", "argv": ["batch-submit", "fixture_icrp"],
                "job_ids": ["54321"], "returncode": 0})
        for relative, text in (("crescendo.log", "fixture engine progress\n"),
                               ("native/spectre.out", "Spectre fixture log beginning\n"),
                               ("native/protocol.tsv", "owned candidate protocol fixture\n")):
            with open(os.path.join(root, relative), "w", encoding="utf-8") as stream:
                stream.write(text)
        self._write(name, "spec.json", spec)
        self._write(name, "status.json", status)

        def target():
            if behavior in ("wait_for_stop", "launch_race"):
                cancel.wait(5)
            else:
                cancel.wait(.08)
            with self.lock:
                stopped = cancel.is_set()
                failed_behavior = behavior in ("native_failure", "reader_metadata_missing")
                status["state"] = "stopped" if stopped else ("failed" if failed_behavior else "completed")
                # The core schema retains nonterminal cancelled plans as queued;
                # their execution stage carries cancellation for Live Results.
                candidate["status"] = "queued" if stopped else ("failed" if failed_behavior else "completed")
                if stopped:
                    candidate["execution_progress"]["stage"] = "cancelled"
                if candidate["status"] == "completed":
                    candidate["completion_evidence"] = {
                        "schema_version": "crescendo.completion_evidence.v1", "ok": True,
                        "simulator": "spectre" if behavior == "wrong_simulator" else spec["simulator"],
                        "tests": ["main"], "corners": ["TT"], "points": [{"test": "main", "corner": "TT", "point_id": 1}],
                        "execution_simulator": spec["simulator"], "native_parameter_readback_verified": True,
                        "simulator_runs": [{"simulator": spec["simulator"], "returncode": 0}],
                        "spectre_runs": [] if spec["simulator"] == "alps" else [{"error_count": 0}], "fixture_only": True,
                        "execution_pipeline": {
                            "schema_version": "crescendo.execution_pipeline.v1", "solver": "shared_cli",
                            "submission": "shared_local_or_command",
                            "netlist_source": "maestro" if engine == "maestro_netlist" else "template",
                            "netlist_generation": {"source_mode": engine, "private_identity": {"workspace_root": root},
                                "method": "fixture", "point_count": 1, "solver_invocations": 0, "remote_submissions": 0},
                            "tasks": [{"task_id": "main_TT_1", "test": "main", "corner": "TT", "point_id": 1,
                                "execution_simulator": spec["simulator"],
                                "solver_command_plan": {"schema_version": "crescendo.execution_command_plan.v1", "layer": "solve",
                                    "argv": ["fixture_" + spec["simulator"], "input.scs"], "template": "{simulator} {netlist}"},
                                "submission_command_plan": {"schema_version": "crescendo.execution_command_plan.v1", "layer": "submit",
                                    "argv": ["/bin/sh", "fixture_owned_wrapper"], "remote": False},
                                "receipt": {"returncode": 0, "cleanup_proven": True}}]}}
                    if engine == "maestro_baseline":
                        candidate["completion_evidence"]["history_name"] = "Interactive.0"
                        candidate["completion_evidence"]["execution_pipeline"] = {
                            "schema_version": "crescendo.maestro_baseline.v1", "solver": "native_maestro",
                            "submission": "native_job_setup", "native_run_invoked": True,
                            "native_wait_completed": True, "native_session_closed": True,
                            "solver_completion_proven": True, "run_id": "1",
                            "history_name": "Interactive.0", "rdb_path": os.path.join(root, "fixture.rdb")}
                    status["completed_candidates"] = 1
                if behavior == "retired_native_solver" and candidate["status"] == "completed":
                    candidate["completion_evidence"]["execution_pipeline"]["solver"] = "native_ADE"
                if behavior == "missing_expanded_submission" and candidate["status"] == "completed":
                    candidate["completion_evidence"]["execution_pipeline"]["tasks"][0].pop("submission_command_plan")
                if behavior == "native_failure":
                    status["message"] = "fixture native startup failed"
                    status["primary_failure"] = {"message": status["message"], "failure_stage": "maestro_startup"}
                    candidate["error"] = status["message"]
                if behavior == "reader_metadata_missing":
                    status["message"] = "ALPS PSF design parameter metadata is unavailable"
                    status["primary_failure"] = {"message": status["message"], "failure_stage": "result_reader_parameter_metadata"}
                    candidate.update(error=status["message"], failure_stage="result_reader_parameter_metadata")
                    self._write(name, "native/result_reader_capability.json", {
                        "schema_version": "crescendo.result_reader_capability.v1", "fixture_only": True,
                        "task_id": "main_TT_1", "test": "main", "corner": "TT", "point_id": 1,
                        "execution_simulator": spec["simulator"], "result_reader": "spectre",
                        "psf_dir": os.path.join(root, "native", "psf"), "analysis_names": ["sp"],
                        "native_parameter_readback": {"source": "current_psf_designParamVals", "required": ["SW"],
                            "actual": {}, "missing": ["SW"], "ok": False},
                        "output_diagnostics": [{"key": "main::gain", "status": "PASS", "value": 4.0,
                            "requires_variables": []}, {"key": "main::power", "status": "NOT_TESTED", "value": None,
                            "error": "parameter unavailable", "requires_variables": ["SW"]}],
                        "candidate_qualification": {"ok": False, "reason": "required parameters unavailable"}})
                if behavior == "parameter_proof_failure":
                    candidate["parameter_application"] = {"ok": False, "message": "fixture readback mismatch"}
                if behavior == "native_run_points":
                    candidate["parameter_application"] = {'ok': True,
                        'proof_kind': 'native_run_device_parameters', 'private_oa_backannotation_proven': False,
                        'parameters': [{'parameter_id': 'x', 'requested': values['x'], 'effective': values['x'],
                            'points': [{'test': 'main', 'corner': corner, 'point_id': index,
                                'requested': values['x'], 'effective': values['x'], 'scs_expression': 'DPAR_0',
                                'dpar_status': 'resolved_from_native_run', 'proof_source': 'current_scs_and_dpar',
                                'scs_path': os.path.join(root, 'native', corner, 'input.scs')}
                                for index, corner in enumerate(('TT', 'SS'), 1)]}]}
                unsafe = behavior == "unproven_cleanup"
                status["final_cleanup"] = {"process_termination_proven": not unsafe,
                                           "remote_stop_proven": not unsafe,
                                           "artifact_cleanup_complete": not unsafe}
                if unsafe:
                    status["cleanup_failure"] = {"recovery_required": True}
                plan = spec['environment_test'].get('frozen_plan') or [
                    {'generation': 0, 'index': 0, 'values': values}]
                candidates = []
                for planned in plan:
                    item = copy.deepcopy(candidate)
                    item.update(generation=planned['generation'], index=planned['index'],
                                values=copy.deepcopy(planned['values']),
                                candidate_id='g%03d_c%04d' % (planned['generation'], planned['index']))
                    for parameter in (item.get('parameter_application') or {}).get('parameters') or []:
                        parameter['requested'] = planned['values'][parameter['parameter_id']]
                        if 'effective' in parameter:
                            parameter['effective'] = parameter['requested']
                        for point in parameter.get('points') or []:
                            point.update(requested=parameter['requested'], effective=parameter['requested'])
                    candidates.append(item)
                self.snapshots[name]['candidates'] = candidates
                status['completed_candidates'] = sum(item['status'] == 'completed' for item in candidates)
                self._write(name, "status.json", status)
                self._write(name, "worker_shutdown.json", {
                    "schema_version": "crescendo.worker_shutdown.v1", "workers": [{"worker_index": 0, "ok": not unsafe}],
                    "final_cleanup": status["final_cleanup"]})
                store = RunDataStore(os.path.join(root, "optimization.crescendo"), metadata={"run_id": name, "spec": spec})
                store.commit_candidates(copy.deepcopy(candidates))
                store.commit_result({"state": status["state"], "representative": candidate if candidate["status"] == "completed" else None,
                                     "pareto_front": [candidate] if candidate["status"] == "completed" else []})
                store.close()
                self._write(name, "result.json", {"state": status["state"], "fixture_only": True})
                with open(os.path.join(root, "native", "spectre.out"), "a", encoding="utf-8") as stream:
                    stream.write("spectre completes with 0 errors, 0 warnings, and 0 notices.\n")

        def guarded_target():
            try:
                target()
            except Exception as error:
                self.thread_errors.append(error)

        thread = threading.Thread(target=guarded_target, name=name, daemon=True)
        self.threads[name] = thread
        thread.start()
        return {"run_id": name, "run_root": root}

    def _write(self, name, relative, value):
        with open(os.path.join(self.roots[name], relative), "w", encoding="utf-8") as stream:
            json.dump(value, stream)

    def progress(self, name):
        self.progress_calls.append(name)
        with self.lock:
            return copy.deepcopy(self.snapshots[name])

    def stop(self, name):
        self.stops.append(name)
        self.run_cancels[name].set()
        return {"stop_requested": True}

    def read(self, name, relative):
        path = self.run_path(name, relative)
        if not os.path.isfile(path):
            return None
        with open(path, "r", encoding="utf-8") as stream:
            return json.load(stream)

    def run_path(self, name, relative):
        return os.path.join(self.roots[name], relative)

    def candidate_logs(self, name, candidate_id):
        engine = self.snapshots[name]["candidates"][0]["execution_backend"]
        return {"logs": [{"log_id": "a" * 24, "kind": "spectre", "path": self.run_path(name, "native/spectre.out"),
                          "test": "main", "corner": "TT", "point_id": 1}],
                "tasks": [] if self.behaviors.get(engine) == "shared_job_ids" else
                         [{"job_id": "12345", "test": "main", "corner": "TT"}]}

    def read_candidate_log_chunk(self, name, candidate_id, log_id, offset=0, max_bytes=16384):
        with open(self.run_path(name, "native/spectre.out"), "rb") as stream:
            stream.seek(offset)
            raw = stream.read(max_bytes)
        return {"text": raw.decode("utf-8"), "next_offset": offset + len(raw)}

    def list_runs(self):
        return [{"run_id": "formal_fixture", "state": "completed"}]

    def export_results(self, name, path):
        self.exports.append((name, path))
        export_dataset(self.run_path(name, "optimization.crescendo"), path)
        return {"path": path}


class EnvironmentTestManagerTests(unittest.TestCase):
    def test_repeated_command_poll_does_not_fill_cache_or_hide_new_terminal_state(self):
        manager = EnvironmentTestManager(None)
        row = {'config': {}, 'case_id': 'spectre.maestro_netlist', 'case_records': {},
               'case_record_bytes': {}, 'record_signatures': set(), 'selected_simulator': 'spectre'}
        command = {'observation': 'execution.command', 'payload': {
            'kind': 'solver', 'invocation_id': 'owned-1', 'argv': ['spectre', 'input.scs'], 'returncode': None}}
        for unused in range(100):
            manager._log(row, command, 'maestro_netlist')
        self.assertEqual(len(row['case_records']['spectre.maestro_netlist']), 1)
        terminal = copy.deepcopy(command)
        terminal['payload']['returncode'] = 0
        manager._log(row, terminal, 'maestro_netlist')
        self.assertEqual(len(row['case_records']['spectre.maestro_netlist']), 2)
        self.assertEqual(row['case_records']['spectre.maestro_netlist'][-1]['payload']['returncode'], 0)
        other = dict(row, case_id='alps.maestro_netlist')
        manager._log(other, command, 'maestro_netlist')
        self.assertEqual(len(row['case_records']['alps.maestro_netlist']), 1)

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.services, self.managers = [], []
        self.queries = []
        self.probe_status = "PASS"
        self.probe_entered, self.probe_release = threading.Event(), threading.Event()
        self.probe_release.set()
        self.scheduler_entered, self.scheduler_release = threading.Event(), threading.Event()
        self.scheduler_release.set()
        self.scheduler_threads = set()

        def probes(config, prepared, root, emit=None, cancel=None, spec=None):
            self.probe_entered.set()
            self.probe_release.wait(5)
            row = {"id": "fixture.independent", "status": self.probe_status,
                   "message": "fixture environment result", "details": {"independent": True}}
            if emit:
                emit(row)
            return [row]

        def scheduler(config, prepared, root, job_ids, emit=None, cancel=None, spec=None, capability_cache=None):
            self.scheduler_threads.add(threading.get_ident())
            self.queries.append(copy.deepcopy(job_ids))
            self.scheduler_entered.set()
            self.scheduler_release.wait(5)
            row = {"id": "scheduler.fixture", "status": "PASS", "message": "fixture exact owned query", "details": {"job_ids": job_ids}}
            if emit:
                emit(row)
            return [row]

        patch = mock.patch("crescendo_engine.environment_test.run_environment_probes", side_effect=probes)
        patch.start()
        self.addCleanup(patch.stop)
        patch = mock.patch("crescendo_engine.environment_test.query_scheduler_jobs", side_effect=scheduler)
        patch.start()
        self.addCleanup(patch.stop)
        self.available_simulators = {"spectre"}
        self.capability_calls = []
        def capability(config, prepared, simulator, engine):
            self.capability_calls.append((simulator, engine))
            self.assertEqual(config["cadence"]["simulator"], simulator)
            return {"ready": simulator in self.available_simulators, "simulator": simulator,
                    "reason": "fixture ALPS executable unavailable; no Spectre fallback"}
        patch = mock.patch("crescendo_engine.environment_test._case_capability", side_effect=capability)
        patch.start()
        self.addCleanup(patch.stop)
        self.addCleanup(self.finish_threads)

    def finish_threads(self):
        self.probe_release.set()
        self.scheduler_release.set()
        for service in self.services:
            service.start_release.set()
            for cancel in service.run_cancels.values():
                cancel.set()
        for manager in self.managers:
            manager.cancel_all()
            manager.join(3)
        for service in self.services:
            for thread in service.threads.values():
                thread.join(2)

    def test_empty_ui_execution_selection_blocks_native_start_without_source_fallback(self):
        manager, service, spec = self.fixture()
        spec.update(execution_selection_valid=False, execution_selection={"tests": [], "analyses": {}},
                    ui_input_warnings=["Select at least one Test to run."])
        payload = manager.start("prepared-fixture", spec)
        final = self.finish(manager, payload["test_id"])
        self.assertEqual(service.starts, [])
        check = self.checks(final, "execution")[0]
        self.assertEqual(check["status"], "BLOCKED")
        self.assertFalse(check["details"]["first_cause"]["input_flags"]["execution_selection_valid"])

    def fixture(self, behaviors=None):
        root = os.path.join(self.directory.name, "service_%d" % len(self.services))
        service = FixtureService(root, behaviors)
        manager = EnvironmentTestManager(service)
        service.manager = manager
        self.services.append(service)
        self.managers.append(manager)
        spec = {"variables": [{"parameter_id": "x", "minimum": 1e-6, "maximum": 2e-6}],
                "objectives": [{"output_key": "main::gain", "direction": "maximize"},
                               {"output_key": "main::power", "direction": "minimize"}],
                "constraints": [{"output_key": "main::power", "operator": "<=", "limit": 2.0}],
                "maestro_job_overrides": {"command": 'batch-submit -q current -R "cpu=6;mem=20000"', "max_jobs": 50},
                "candidate_timeout_seconds": 60, "engine_timeout_seconds": 4, "advanced_diagnostics": True}
        return manager, service, spec

    def finish(self, manager, test_id, timeout=8):
        self.assertTrue(manager.join(timeout), "Diagnostic manager did not reach its bounded terminal state")
        self.assertFalse([error for service in self.services for error in service.thread_errors],
                         "Simulated lifecycle writer failed; this cannot count as a passed manager test")
        return manager.progress(test_id)

    def checks(self, payload, identifier, simulator="spectre"):
        return [check for check in payload["checks"]
                if check.get("check_id", check.get("id")) == identifier and
                (simulator is None or check.get("simulator") in (None, simulator))]


    def test_selected_alps_source_shares_one_physical_report_and_one_run_directory(self):
        self.available_simulators = {"spectre", "alps"}
        manager, service, spec = self.fixture()
        original = copy.deepcopy(service.config)
        result = manager.start("prepared-fixture", dict(spec, simulator="alps", execution_backend="private_netlist"))
        payload = self.finish(manager, result["test_id"])
        self.assertEqual([(item["simulator"], item["execution_backend"]) for item in service.starts],
                         [("spectre", "maestro_baseline"), ("alps", "private_netlist"), ("alps", "maestro_netlist")])
        self.assertEqual(service.config, original)
        self.assertEqual(list(payload["run_ids"]), ["spectre.maestro_baseline", "alps.private_netlist", "alps.maestro_netlist"])
        self.assertEqual([item["state"] for item in payload["case_results"]], ["completed", "completed", "completed"])
        main = payload["main_log"]
        self.assertEqual(main, os.path.join(payload["report_root"], "environment_test.log"))
        self.assertEqual(payload["report_log_count"], 1)
        self.assertEqual(set(payload["engine_logs"].values()), {main})
        self.assertEqual(set(payload["simulator_logs"].values()), {main})
        self.assertEqual(payload["physical_log_bytes"][main], os.path.getsize(main))
        self.assertEqual(payload["pending_log_bytes"][main], 0)
        with open(main, encoding="utf-8") as stream:
            report = stream.read()
        self.assertEqual(payload["log_tail"], report[-65536:])
        self.assertEqual(report.count('"final_checklist":'), 1)
        self.assertEqual(len([line for line in report.splitlines() if line.startswith("run_context ")]), 4)
        self.assertIn("success_summary", report)
        self.assertNotIn("native.main_log_tail", report)
        self.assertIn("spectre.maestro_baseline", report)
        roots = [os.path.dirname(path) for path in service.roots.values()]
        self.assertEqual(roots, [os.path.join(payload["report_root"], "Spectre", "MaestroBaseline"), os.path.join(payload["report_root"], "ALPS", "PrivateNetlist"), os.path.join(payload["report_root"], "ALPS", "Maestro")])
        self.assertEqual(len(self.checks(payload, "fixture.independent", None)), 1)

    def test_single_report_quota_is_shared_by_both_simulator_aliases(self):
        manager = EnvironmentTestManager(None)
        path = '/owned/environment_test.log'
        row = {'config':{},'main_log':path,'selected_simulator':'spectre','write_error':None,
               'report_state':{'write_error':None},'log_bytes':{path:8*1024*1024},'queued_log_bytes':{path:0},
               'log_limited':set(),'record_signatures':set(),'log_queue':queue.Queue(maxsize=8),'checks':{}}
        for simulator in ('spectre','alps'):
            result = manager._log(dict(row,simulator=simulator),{'observation':'ordinary.'+simulator},'maestro_netlist')
            self.assertEqual(result,set())
        self.assertEqual(row['log_limited'],{path})
        warnings = [check for check in row['checks'].values() if check.get('check_id') == 'log.capacity']
        self.assertEqual(len(warnings),1)
        self.assertEqual(row['log_queue'].qsize(),1,'one quota warning, no simulator-specific duplicate writes')
        self.assertEqual(set(row['log_bytes']),{path})
        self.assertGreater(row['queued_log_bytes'][path],0)

    def test_writer_failure_is_shared_with_existing_case_copies_and_does_not_count_writes(self):
        manager = EnvironmentTestManager(None)
        path = os.path.join(self.directory.name,'not_a_log_file')
        os.makedirs(path)
        row = {'main_log':path,'write_error':None,'report_state':{'write_error':None},
               'log_bytes':{path:0},'queued_log_bytes':{path:0},'physical_log_tails':{path:''},
               'log_queue':queue.Queue(maxsize=8),'checks':{}}
        copied = dict(row,simulator='alps')
        self.assertTrue(manager._append_report(row,'first attempted write\n'))
        thread = threading.Thread(target=manager._log_writer,args=(row,),daemon=True)
        thread.start()
        self.wait(lambda: row['report_state']['write_error'] is not None)
        self.assertIsNone(copied['write_error'],'the case scalar is stale by design; shared error still prevents enqueue')
        self.assertFalse(manager._append_report(copied,'must not enqueue\n'))
        row['log_queue'].put(None)
        thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertEqual(row['log_bytes'][path],0)
        self.assertEqual(row['queued_log_bytes'][path],0)
        self.assertEqual(row['physical_log_tails'][path],'')
        self.assertEqual(row['checks']['report.write']['status'],'FAIL')


    def test_static_readiness_only_for_selected_solver_with_case_reference(self):
        self.available_simulators = {"spectre", "alps"}
        manager, service, spec = self.fixture()
        payload = self.finish(manager, manager.start("prepared-fixture", spec)["test_id"])
        self.assertEqual(self.capability_calls, [("spectre", "maestro_baseline"), ("spectre", "private_netlist"), ("spectre", "maestro_netlist")])
        self.assertEqual(len(service.starts), 3)
        self.assertEqual(set(payload["simulator_capabilities"]), {engine + ".spectre" for engine in ("maestro_baseline", "private_netlist", "maestro_netlist")})
        case_checks = [check for check in self.checks(payload, "simulator.readiness", "spectre") if check.get("engine")]
        self.assertEqual(len(case_checks), 3)
        for check in case_checks:
            entry = payload["simulator_capabilities"][check["engine"] + ".spectre"]
            self.assertEqual(check["details"]["sha256"], entry["sha256"])
            self.assertEqual(check["details"]["check_id"], entry["check_id"])


    def test_static_readiness_is_not_reused_from_a_prior_diagnostic(self):
        self.available_simulators = {"spectre", "alps"}
        manager, service, spec = self.fixture()
        spec["simulator"] = "alps"
        self.finish(manager, manager.start("prepared-fixture", spec)["test_id"])
        self.available_simulators = {"spectre"}
        payload = self.finish(manager, manager.start("prepared-fixture", spec)["test_id"])
        self.assertEqual(len(self.capability_calls), 6)
        self.assertFalse(payload["simulator_capabilities"]["private_netlist.alps"]["ready"])
        self.assertFalse(payload["simulator_capabilities"]["maestro_netlist.alps"]["ready"])
        self.assertEqual([case["state"] for case in payload["case_results"]], ["completed", "blocked", "blocked"])
        self.assertEqual(len(service.starts), 4)

    def test_explicit_scheduler_scheduler_survives_selected_case_request(self):
        self.available_simulators = {"spectre", "alps"}
        manager, service, spec = self.fixture()
        spec["command_scheduler"] = "batch"
        original = copy.deepcopy(spec)
        self.finish(manager, manager.start("prepared-fixture", spec)["test_id"])
        self.assertEqual(len(service.starts), 3)
        self.assertEqual([request.get("command_scheduler") for request in service.starts], [None, "batch", "batch"])
        self.assertEqual(spec, original)

    def test_selected_reader_failure_retains_waveform_observations_without_retry(self):
        self.available_simulators = {"spectre", "alps"}
        manager, service, spec = self.fixture({"alps.maestro_netlist": "reader_metadata_missing"})
        spec["simulator"] = "alps"
        payload = self.finish(manager, manager.start("prepared-fixture", spec)["test_id"])
        self.assertEqual(len(service.starts), 3)
        self.assertEqual([case["state"] for case in payload["case_results"]], ["completed", "completed", "failed"])
        case = payload["case_results"][2]
        self.assertEqual(case["first_cause"]["failure_stage"], "result_reader_parameter_metadata")
        self.assertEqual(case["first_cause_layer"], "measurement")
        checks = self.checks(payload, "reader.parameter_metadata.main_TT_1", "alps")
        self.assertEqual([check["status"] for check in checks], ["FAIL"])
        self.assertEqual(checks[0]["details"]["analysis_names"], ["sp"])
        observations = self.checks(payload, "reader.output_observations.main_TT_1", "alps")[0]
        self.assertEqual(observations["details"]["output_diagnostics"][0]["value"], 4.0)
        self.assertFalse(observations["details"]["candidate_qualification"]["ok"])
        with open(payload["simulator_logs"]["alps"], encoding="utf-8") as stream:
            self.assertIn("result_reader_parameter_metadata", stream.read())

    def test_schema_evidence_indices_preserve_changed_task_identity(self):
        document = {"schema_version": "fixture.schema.v1", "task_id": "first", "context": "x" * 1024}
        original = copy.deepcopy(document)
        index = {}
        first = _schema_artifact_index(document, index, "case1:artifact/data")
        repeated = _schema_artifact_index(document, index, "case2:artifact/data")
        changed = _schema_artifact_index(dict(document, task_id="second"), index, "case2:changed/data")
        self.assertEqual(first, original)
        self.assertEqual(repeated["same_as"], "case1:artifact/data")
        self.assertEqual(len(repeated["sha256"]), 64)
        self.assertEqual(changed["task_id"], "second")
        self.assertNotIn("same_as", changed)
        self.assertEqual(document, original)

    def test_identical_current_setup_is_indexed_only_within_one_solver_and_test(self):
        manager = EnvironmentTestManager(None)
        row = {'config': {}, 'prepared': {}, 'engine_outcomes': {}, 'cancel': threading.Event(),
               'selected_simulator': 'spectre', 'simulator': 'spectre', 'case_id': 'case1'}
        setup = {'schema_version': 'crescendo.current_setup_diagnostics.v1', 'path': '/source/maestro.sdb',
                 'sha256': 'source-hash', 'settings_status': 'READ',
                 'settings_rows': [{'scope': '/active/modelFiles/model', 'value': '/pdk/' + 'm' * 1024}]}
        captured = []
        def log(active, record, engine):
            captured.append(copy.deepcopy(record))
            return [active['simulator']]
        with mock.patch('crescendo_engine.environment_test.collect_run_diagnostics') as collect, \
                mock.patch.object(manager, '_log', side_effect=log):
            collect.return_value = [{'observation': 'diagnostic.source_settings', 'payload': setup}]
            manager._collect_inline(row, 'maestro_netlist', '/owned/first', {}, True)
            row['case_id'] = 'case2'
            manager._collect_inline(row, 'private_netlist', '/owned/second', {}, True)
            row['simulator'] = 'alps'
            manager._collect_inline(row, 'maestro_netlist', '/owned/alps', {}, True)
            row['simulator'] = 'spectre'
            private = dict(setup, path='private/maestro.sdb', sha256='private-hash')
            collect.return_value = [{'observation': 'diagnostic.settings', 'payload': private}]
            manager._collect_inline(row, 'private_netlist', '/owned/private', {}, True)
            collect.return_value = [{'observation': 'diagnostic.source_settings', 'payload': dict(setup, sha256='changed-hash')}]
            manager._collect_inline(row, 'private_netlist', '/owned/changed', {}, True)
            fresh = {'config': {}, 'prepared': {}, 'engine_outcomes': {}, 'cancel': threading.Event(),
                     'selected_simulator': 'spectre', 'simulator': 'spectre', 'case_id': 'new-test'}
            collect.return_value = [{'observation': 'diagnostic.source_settings', 'payload': setup}]
            manager._collect_inline(fresh, 'maestro_netlist', '/owned/new-test', {}, True)
            self.assertEqual(collect.call_count, 6)
        payloads = [record['payload'] for record in captured]
        self.assertEqual(payloads[0], setup)
        self.assertEqual(payloads[1]['same_as'], 'case1:/source/maestro.sdb')
        self.assertEqual(payloads[1]['schema_version'], 'crescendo.current_setup_diagnostics.v1')
        self.assertEqual(payloads[2], setup)
        self.assertEqual(payloads[3], private)
        self.assertEqual(payloads[4]['sha256'], 'changed-hash')
        self.assertEqual(payloads[5], setup)

    def test_unpersisted_current_setup_is_not_replaced_by_a_dangling_index(self):
        manager = EnvironmentTestManager(None)
        row = {'config': {}, 'prepared': {}, 'engine_outcomes': {}, 'cancel': threading.Event(),
               'selected_simulator': 'spectre', 'simulator': 'spectre', 'case_id': 'case1'}
        setup = {'schema_version': 'crescendo.current_setup_diagnostics.v1', 'path': '/source/maestro.sdb',
                 'settings_rows': [{'scope': '/active/vars/var', 'value': 'x' * 1024}]}
        with mock.patch('crescendo_engine.environment_test.collect_run_diagnostics', return_value=[
                {'observation': 'diagnostic.source_settings', 'payload': setup}]), \
                mock.patch.object(manager, '_log', side_effect=[[], ['spectre']]) as log:
            manager._collect_inline(row, 'maestro_netlist', '/owned/first', {}, True)
            manager._collect_inline(row, 'private_netlist', '/owned/second', {}, True)
        self.assertEqual(log.call_args_list[0][0][1]['payload'], setup)
        self.assertEqual(log.call_args_list[1][0][1]['payload'], setup)


    def test_missing_selected_alps_blocks_one_case_without_spectre_fallback(self):
        manager, service, spec = self.fixture()
        spec["simulator"] = "alps"
        payload = self.finish(manager, manager.start("prepared-fixture", spec)["test_id"])
        self.assertEqual([row["execution_backend"] for row in service.starts], ["maestro_baseline"])
        self.assertEqual([item["state"] for item in payload["case_results"]], ["completed", "blocked", "blocked"])
        self.assertEqual(set(payload["case_run_ids"]), {"spectre.maestro_baseline", "alps.private_netlist", "alps.maestro_netlist"})
        case = payload["case_results"][1]
        self.assertFalse(case["stop_required"])
        self.assertIsNone(case["stop_proven"], "Unlaunched case must not manufacture Stop proof")
        with open(payload["main_log"], encoding="utf-8") as stream:
            self.assertIn("no Spectre fallback", stream.read())

    def test_unknown_stop_keeps_failure_without_other_simulator_launch(self):
        self.available_simulators = {"spectre", "alps"}
        manager, service, spec = self.fixture({"spectre.maestro_baseline": "unproven_cleanup"})
        payload = self.finish(manager, manager.start("prepared-fixture", spec)["test_id"])
        self.assertEqual(len(service.starts), 1)
        self.assertEqual([item["state"] for item in payload["case_results"]], ["failed", "blocked", "blocked"])
        self.assertFalse(payload["case_results"][0]["stop_proven"])

    def test_launch_native_cleanup_failure_does_not_try_another_source(self):
        self.available_simulators = {"spectre", "alps"}
        for behavior in ("launch_cleanup_failed", "launch_unsafe_to_optimize"):
            with self.subTest(behavior=behavior):
                manager, service, spec = self.fixture({"spectre.maestro_baseline": behavior})
                payload = self.finish(manager, manager.start("prepared-fixture", spec)["test_id"])
                self.assertEqual(len(service.starts), 1)
                self.assertEqual([case["state"] for case in payload["case_results"]], ["failed", "blocked", "blocked"])
                first = payload["case_results"][0]
                self.assertTrue(first["stop_required"])
                self.assertFalse(first["stop_proven"])
                self.assertIsNone(first["run_id"])
                self.assertEqual(first["first_cause"]["failure_stage"], "run_registration")
                for later in payload["case_results"][1:]:
                    self.assertFalse(later["stop_required"])
                    self.assertIsNone(later["stop_proven"])

    def test_failed_case_with_proven_cleanup_does_not_repeat_shared_failure(self):
        self.available_simulators = {"spectre", "alps"}
        manager, service, spec = self.fixture({"spectre.maestro_baseline": "native_failure"})
        payload = self.finish(manager, manager.start("prepared-fixture", spec)["test_id"])
        self.assertEqual(len(service.starts), 3)
        self.assertEqual([item["state"] for item in payload["case_results"]], ["failed", "completed", "completed"])
        self.assertTrue(payload["case_results"][0]["stop_proven"])

    def test_alps_rejects_spectre_completion_identity(self):
        self.available_simulators = {"spectre", "alps"}
        manager, service, spec = self.fixture({"alps.private_netlist": "wrong_simulator"})
        spec.update(simulator="alps", execution_backend="private_netlist")
        payload = self.finish(manager, manager.start("prepared-fixture", spec)["test_id"])
        self.assertEqual(payload["case_results"][1]["state"], "failed")
        self.assertEqual(payload["case_results"][2]["state"], "completed")
        check = next(check for check in self.checks(payload, "execution", "alps") if check["engine"] == "private_netlist")
        self.assertEqual(check["status"], "FAIL")

    def test_cancellation_stops_only_selected_case_without_retry(self):
        self.available_simulators = {"spectre", "alps"}
        manager, service, spec = self.fixture({"spectre.maestro_baseline": "wait_for_stop"})
        result = manager.start("prepared-fixture", spec)
        self.wait(lambda: bool(manager.progress(result["test_id"])["active_runs"]))
        manager.stop(result["test_id"])
        payload = self.finish(manager, result["test_id"])
        self.assertEqual(len(service.starts), 1)
        self.assertEqual([item["state"] for item in payload["case_results"]], ["cancelled", "cancelled", "cancelled"])

    def test_environment_failure_keeps_selected_native_run_and_inputs_unchanged(self):
        self.probe_status = "FAIL"
        manager, service, spec = self.fixture()
        original_spec, original_config, original_prepared = copy.deepcopy(spec), copy.deepcopy(service.config), copy.deepcopy(service.prepared)
        result = manager.start("prepared-fixture", spec)
        payload = self.finish(manager, result["test_id"])
        self.assertEqual([item["execution_backend"] for item in service.starts], ["maestro_baseline", "private_netlist", "maestro_netlist"])
        self.assertEqual([item["status"] for item in self.checks(payload, "execution")], ["PASS", "PASS", "PASS"])
        self.assertEqual(spec, original_spec)
        self.assertEqual(service.config, original_config)
        self.assertEqual(service.prepared, original_prepared)
        self.assertEqual(payload["state"], "failed")
        self.assertTrue(self.queries)
        for requested in service.starts:
            self.assertEqual(requested["population"], 3)
            self.assertEqual(requested["generations"], 2)
            self.assertEqual(requested["algorithm"], "random")
            self.assertEqual(requested["maestro_job_overrides"], original_spec["maestro_job_overrides"])
            self.assertEqual(requested["objectives"], original_spec["objectives"])
            self.assertEqual(requested["constraints"], original_spec["constraints"])
        values = [snapshot["candidates"][0]["values"] for snapshot in service.snapshots.values()]
        self.assertEqual(len(values), 3)
        self.assertTrue(all(value == values[0] for value in values))
        with open(os.path.join(service.formal_root, "unchanged.txt"), "r", encoding="utf-8") as stream:
            self.assertEqual(stream.read(), "Original run must not be edited.\n")

    def test_blocked_prepared_gate_keeps_raw_ui_cause_in_each_case_and_total_log(self):
        self.available_simulators = {"spectre","alps"}
        manager,service,spec = self.fixture()
        warnings = ["Prepared selection: selected cells differ from the prepared cells",
                    "Parameters: previous user edits remain visible"]
        ui_state = {"prepared_id":"prepared-fixture","inspection_id":"inspection-old",
                    "cell_scope_dirty":True,"eda_settings_requires_revalidation":True,
                    "pending_restore_same_setup":True,"prepared_selection_matches":False,
                    "error":"selected cells differ from the prepared cells"}
        spec.update(prepared_selection_valid=False,ui_input_warnings=warnings,ui_prepared_state=ui_state)
        payload = self.finish(manager,manager.start("prepared-fixture",spec)["test_id"])
        self.assertEqual(service.starts,[])
        self.assertEqual(self.capability_calls,[])
        self.assertEqual(payload["run_ids"],{})
        execution_checks = [check for check in payload["checks"] if check.get("check_id") == "execution"]
        self.assertEqual(len(execution_checks),3)
        self.assertEqual([check["status"] for check in execution_checks],["BLOCKED"] * 3)
        for case in payload["case_results"]:
            cause = case["first_cause"]
            self.assertEqual(cause["ui_input_warnings"],warnings)
            self.assertEqual(cause["ui_prepared_state"],ui_state)
            self.assertFalse(cause["input_flags"]["prepared_selection_valid"])
            self.assertTrue(cause["prepared_registration"]["prepared_found"])
            self.assertFalse(cause["execution_started"])
            self.assertEqual(payload["case_contexts"][case["case_id"]]["first_cause"],cause)
            check = next(row for row in execution_checks if row["id"] == case["case_id"]+".execution")
            self.assertEqual(check["details"]["first_cause"],cause)
            self.assertEqual(check["presentation"]["component"]["function"],"EnvironmentTestManager._run")
            self.assertNotEqual(check["message"],"prepared_selection_valid")
            self.assertLess(len(check["message"]),100)
            body = read_section(payload["main_log"],case["case_id"],"request")
            self.assertIsNotNone(body)
            for warning in warnings:
                self.assertIn(warning,body)
            self.assertIn('"prepared_selection_valid": false',body)
            self.assertIn('"prepared_found": true',body)
            self.assertIn('"inspection_id": "inspection-old"',body)
            self.assertIsNone(case["run_id"])
            self.assertEqual(payload["case_contexts"][case["case_id"]]["netlists"],[])

    def test_request_warning_limits_are_explicit_and_do_not_hash_away_all_text(self):
        huge = "界面异常原文"*2000
        row = {"prepared_id":"prepared-old","prepared":{"setup":{}},"spec":{
            "prepared_selection_valid":False,"ui_input_warnings":[huge]*20,
            "ui_prepared_state":{"prepared_id":"prepared-old","error":huge,
                                 "launch_environment":{"SECRET":"must-not-copy"}}}}
        cause = _request_gate_failure(row,["prepared_selection_valid"])
        self.assertEqual(cause["warning_scope"]["total_count"],20)
        self.assertEqual(cause["warning_scope"]["retained_count"],8)
        self.assertEqual(cause["warning_scope"]["omitted_warning_count"],12)
        self.assertEqual(cause["warning_scope"]["truncated_warning_count"],8)
        self.assertTrue(cause["ui_input_warnings"][0]["truncated"])
        self.assertTrue(cause["ui_prepared_state"]["error"]["truncated"])
        self.assertIn("界面异常原文",cause["ui_input_warnings"][0]["text"])
        self.assertNotIn("must-not-copy",json.dumps(cause))
        self.assertLess(len(json.dumps(cause,ensure_ascii=True)),60000)

    def test_request_recovery_distinguishes_environment_refresh_from_new_cell_selection(self):
        row = {"prepared_id":"prepared-current","prepared":{"setup":{}},"spec":{
            "prepared_selection_valid":False,"variables":[{"parameter_id":"p"}],"objectives":[{"name":"gain"}]}}
        cases = [({"prepared_id":"prepared-current","eda_settings_requires_revalidation":True,
                   "cell_scope_dirty":True,"pending_restore_same_setup":True}, "自动重新校验", False, False),
                 ({"prepared_id":"prepared-current","pending_restore_same_setup":True}, "自动重新校验", False, False),
                 ({"prepared_id":"prepared-current","cell_scope_dirty":True}, "Apply", True, False),
                 ({"prepared_id":"prepared-current","prepared_selection_matches":False}, "Load & Inspect", False, True)]
        for state, expected, apply, load in cases:
            with self.subTest(state=state):
                row["spec"]["ui_prepared_state"] = state
                cause = _request_gate_failure(row,["prepared_selection_valid"])
                actions = " ".join(cause["recovery_actions"])
                self.assertIn(expected,actions)
                self.assertEqual("Apply" in actions,apply)
                self.assertEqual("Load & Inspect" in actions,load)
                self.assertIn("编辑",actions)
                self.assertFalse(cause["execution_started"])
        row["prepared"] = {}
        row["spec"]["ui_prepared_state"] = {}
        actions = " ".join(_request_gate_failure(row,["prepared_selection_valid"])["recovery_actions"])
        self.assertIn("Load & Inspect",actions)

    def test_incomplete_ui_flags_allow_environment_but_block_native_launch(self):
        for flag in ("prepared_selection_valid", "job_settings_valid", "storage_settings_valid"):
            with self.subTest(flag=flag):
                manager, service, spec = self.fixture()
                spec[flag] = False
                result = manager.start("prepared-fixture", spec)
                payload = self.finish(manager, result["test_id"])
                self.assertEqual([row["execution_backend"] for row in service.starts], [])
                self.assertEqual([item["status"] for item in self.checks(payload, "execution")], ["BLOCKED"] * 3)
                self.assertTrue(any(check["status"] == "PASS" and check["check_id"] == "fixture.independent" for check in payload["checks"]))

    def test_no_design_keeps_single_report_and_environment_observations(self):
        manager, service, spec = self.fixture()
        result = manager.start(None, {})
        payload = self.finish(manager, result["test_id"])
        self.assertEqual(service.starts, [])
        self.assertEqual(len(payload["engine_logs"]), 4)
        self.assertEqual([check["status"] for check in self.checks(payload, "execution")], ["BLOCKED"] * 3)
        for path in payload["engine_logs"].values():
            with open(path, "r", encoding="utf-8") as stream:
                text = stream.read()
                if path == payload["main_log"]:
                    self.assertIn("fixture.independent", text)
                else:
                    self.assertIn("shared_summary", text)

    def test_unproven_cleanup_retains_scene_without_retry(self):
        manager, service, spec = self.fixture({"maestro_baseline": "unproven_cleanup"})
        result = manager.start("prepared-fixture", spec)
        payload = self.finish(manager, result["test_id"])
        self.assertEqual(len(service.starts), 1)
        executions = self.checks(payload, "execution")
        self.assertEqual(executions[0]["status"], "PASS")
        self.assertEqual([case["state"] for case in payload["case_results"]], ["failed", "blocked", "blocked"])
        self.assertFalse(payload["case_results"][0]["stop_proven"])
        self.assertEqual(self.checks(payload, "termination_and_cleanup")[0]["status"], "FAIL")
        root = next(iter(service.roots.values()))
        self.assertTrue(os.path.isfile(os.path.join(root, "native", "protocol.tsv")))
        self.assertFalse(os.path.exists(os.path.join(root, "evidence.tar.gz")))

    def test_cancel_during_launch_registration_stops_created_run_without_launching_next(self):
        manager, service, spec = self.fixture({"maestro_baseline": "launch_race"})
        service.start_release.clear()
        result = manager.start("prepared-fixture", spec)
        self.assertTrue(service.start_entered.wait(2))
        manager.stop(result["test_id"])
        service.start_release.set()
        payload = self.finish(manager, result["test_id"])
        self.assertEqual(len(service.starts), 1)
        self.assertEqual(len(service.stops), 1)
        self.assertEqual(payload["state"], "cancelled")
        self.assertEqual(payload["active_runs"], [])

    def test_deadline_stops_only_current_owned_run_without_retry(self):
        manager, service, spec = self.fixture({"maestro_baseline": "wait_for_stop"})
        spec["engine_timeout_seconds"] = .03
        result = manager.start("prepared-fixture", spec)
        payload = self.finish(manager, result["test_id"])
        self.assertGreaterEqual(len(service.stops), 1)
        self.assertEqual(len(service.starts), 3)
        self.assertTrue(any(check["check_id"] == "stop_request" and check["engine"] == "maestro_baseline" for check in payload["checks"]))
        self.assertFalse(any(name == "formal_fixture" for name in service.stops))

    def test_session_summary_finishes_before_native_execution(self):
        manager, service, spec = self.fixture()
        self.probe_release.clear()
        result = manager.start("prepared-fixture", spec)
        self.assertTrue(self.probe_entered.wait(2))
        self.assertFalse(service.start_entered.is_set())
        self.probe_release.set()
        self.assertTrue(service.start_entered.wait(2))
        payload = self.finish(manager, result["test_id"])
        self.assertEqual(len(service.starts), 3)
        self.assertEqual([check["status"] for check in self.checks(payload, "execution")], ["PASS"] * 3)

    def test_runtime_summary_emitted_failure_is_warning_and_stages_continue(self):
        manager, service, spec = self.fixture()
        def failed_summary(config, prepared, root, emit=None, cancel=None, spec=None):
            emit({'id': 'runtime.summary', 'status': 'FAIL', 'message': 'summary error',
                  'details': {'error': 'original error'}})
        with mock.patch('crescendo_engine.environment_test.run_environment_probes', side_effect=failed_summary):
            payload = self.finish(manager, manager.start('prepared-fixture', spec)['test_id'])
        summary = self.checks(payload, 'runtime.summary', None)[0]
        self.assertEqual(summary['status'], 'WARN')
        self.assertEqual(summary['message'], 'summary error')
        self.assertEqual(summary['details']['observed_status'], 'FAIL')
        self.assertEqual(summary['details']['error'], 'original error')
        self.assertEqual(payload['state'], 'completed')
        self.assertEqual([case['state'] for case in payload['case_results']], ['completed'] * 3)

    def test_default_defers_optional_result_roundtrip_without_failing_stages(self):
        manager, service, spec = self.fixture()
        spec.pop('advanced_diagnostics')
        payload = self.finish(manager, manager.start('prepared-fixture', spec)['test_id'])
        self.assertEqual(service.exports, [])
        self.assertEqual([item['state'] for item in payload['case_results']], ['completed'] * 3)
        self.assertEqual([check['status'] for check in self.checks(payload, 'statistics_and_dataset')], ['NOT_TESTED'] * 3)
        self.assertEqual(self.checks(payload, 'test.summary', None)[0]['status'], 'WARN')
        self.assertEqual(payload['state'], 'completed')

    def test_advanced_export_failure_does_not_rewrite_successful_stages(self):
        manager, service, spec = self.fixture()
        with mock.patch.object(service, 'export_results', side_effect=RuntimeError('export failed')):
            payload = self.finish(manager, manager.start('prepared-fixture', spec)['test_id'])
        self.assertEqual(payload['state'], 'failed')
        self.assertEqual([check['status'] for check in self.checks(payload, 'execution')], ['PASS'] * 3)
        self.assertEqual([check['status'] for check in self.checks(payload, 'statistics_and_dataset')], ['FAIL'] * 3)
        self.assertEqual([case['state'] for case in payload['case_results']], ['completed'] * 3)
        self.assertTrue(all(case['metric_summary'] for case in payload['case_results']))
        self.assertTrue(all(case['first_cause'] is None for case in payload['case_results']))

    def test_completed_diagnostics_retain_original_files_and_only_summarize_success(self):
        manager, service, spec = self.fixture()
        payload = self.finish(manager, manager.start("prepared-fixture", spec)["test_id"])
        self.assertEqual(len(service.exports), 3)
        for case_id, name in payload["run_ids"].items():
            root = service.roots[name]
            self.assertTrue(os.path.isfile(os.path.join(root, "native", "protocol.tsv")))
            self.assertTrue(os.path.isfile(os.path.join(root, "spec.json")))
            self.assertFalse(os.path.exists(os.path.join(root, "evidence.tar.gz")))
            with open(payload["simulator_logs"][case_id.split(".")[0]], encoding="utf-8") as stream:
                report = stream.read()
            self.assertIn("run_context ", report)
            self.assertIn("success_summary", report)
            self.assertEqual(report.count("Spectre fixture log beginning"), 0)
            self.assertIn("concise_success", report)
            self.assertIn("report_evidence_integrity", report)
            self.assertIn("completion_evidence", report)
            self.assertIn("termination_and_cleanup", report)
            self.assertEqual(payload["case_contexts"][case_id]["run_root"], root)
            self.assertEqual(payload["case_contexts"][case_id]["netlists"], [])
        self.assertEqual([check["status"] for check in self.checks(payload, "statistics_and_dataset")], ["PASS"] * 3)

    def test_full_log_detail_expands_successful_stages(self):
        manager, service, spec = self.fixture()
        service.config['runtime']['diagnostic_log_detail'] = 'full'
        payload = self.finish(manager, manager.start('prepared-fixture', spec)['test_id'])
        with open(payload['main_log'], encoding='utf-8') as stream:
            report = stream.read()
        self.assertEqual(report.count('Spectre fixture log beginning'), 3)
        self.assertNotIn('concise_success', report)
        self.assertTrue(payload['report_ready'])

    def test_explicit_failed_parameter_evidence_is_not_certified_as_pass(self):
        manager, service, spec = self.fixture({"maestro_netlist": "parameter_proof_failure"})
        result = manager.start("prepared-fixture", spec)
        payload = self.finish(manager, result["test_id"])
        proof = next(check for check in self.checks(payload, "parameter_application") if check["category"] == "Maestro")
        self.assertEqual(proof["status"], "FAIL")
        self.assertFalse(proof["details"]["ok"])
        self.assertEqual(self.checks(payload, 'execution')[2]['status'], 'FAIL')
        self.assertEqual(payload['case_results'][2]['first_cause']['failure_stage'], 'parameter_application')
        self.assertIn('readback mismatch', payload['case_results'][2]['first_cause']['message'])
        self.assertEqual(len(service.starts), 3)

    def test_success_summary_indexes_proofs_and_ui_keeps_native_point_values(self):
        manager, service, spec = self.fixture({'maestro_netlist': 'native_run_points'})
        payload = self.finish(manager, manager.start('prepared-fixture', spec)['test_id'])
        with open(payload['main_log'], encoding='utf-8') as stream:
            lines = stream.read().splitlines()
        summaries = [json.loads(line[len('success_summary '):]) for line in lines if line.startswith('success_summary ')]
        proofs = [check['proof'] for summary in summaries for check in summary['checks']
                  if '.maestro_netlist.' in check['id'] and check['id'].endswith('.parameter_application')]
        self.assertEqual(len(proofs), 6)
        self.assertTrue(all(proof['parameters_count'] == 1 for proof in proofs))
        self.assertTrue(all('parameters' not in proof for proof in proofs))
        proof = next(check['details'] for check in self.checks(payload, 'parameter_application') if check['engine'] == 'maestro_netlist')
        self.assertEqual(proof['proof_kind'], 'native_run_device_parameters')
        self.assertFalse(proof['private_oa_backannotation_proven'])
        points = proof['parameters'][0]['points']
        self.assertEqual([point['corner'] for point in points], ['TT', 'SS'])
        self.assertTrue(all(point['scs_expression'] == 'DPAR_0' for point in points))
        self.assertTrue(all(os.path.isabs(point['scs_path']) for point in points))
        self.assertTrue(all(point['requested'] == point['effective'] for point in points))

    def test_shared_native_submission_ids_are_queried_without_candidate_association(self):
        manager, service, spec = self.fixture({"maestro_netlist": "shared_job_ids"})
        result = manager.start("prepared-fixture", spec)
        self.finish(manager, result["test_id"])
        observed = [row for query in self.queries for row in query if row["job_id"] == "54321"]
        self.assertTrue(observed, "Owned shared ICRP IDs must remain visible to exact scheduler queries")
        self.assertTrue(all(row.get("shared") is True for row in observed))
        self.assertTrue(all(row.get("candidate_id") is None for row in observed))

    def test_shared_cli_completed_pipeline_retains_exact_scheduler_point_identity(self):
        manager, service, spec = self.fixture()
        candidate = {'candidate_id': 'g000_c0000', 'execution_progress': {'tasks': []},
            'completion_evidence': {'execution_pipeline': {'points': [{'task_id': 'main_TT_1',
                'scheduler_job_id': '10000001', 'test': 'main', 'corner': 'TT', 'point_id': 1}]}}}
        owned = {}
        with mock.patch.object(service, 'candidate_logs', return_value={'tasks': [], 'logs': []}):
            manager._observe_candidate({}, 'maestro_netlist', 'owned-run', candidate, set(), {}, owned)
        self.assertEqual(owned['10000001']['candidate_id'], 'g000_c0000')
        self.assertEqual(owned['10000001']['task_id'], 'main_TT_1')
        self.assertEqual(owned['10000001']['point_id'], 1)

    def test_unknown_or_cancelled_marker_cannot_redirect_an_unrelated_run(self):
        manager, service, spec = self.fixture()
        with self.assertRaises(RuntimeError):
            manager.run_root({"schema_version": "crescendo.environment_test.run.v1", "test_id": "unregistered"}, "maestro_netlist")
        self.probe_release.clear()
        result = manager.start(None, {})
        manager.stop(result["test_id"])
        with self.assertRaises(RuntimeError):
            manager.run_root({"schema_version": "crescendo.environment_test.run.v1", "test_id": result["test_id"]}, "maestro_netlist")
        self.probe_release.set()
        self.finish(manager, result["test_id"])

    def test_linked_diagnostic_payload_is_retained_without_touching_its_target(self):
        root = os.path.join(self.directory.name, "owned_run")
        os.makedirs(root)
        target = os.path.join(self.directory.name, "original.oa")
        with open(target, "wb") as stream:
            stream.write(b"untouched source")
        os.symlink(target, os.path.join(root, "linked_payload"))
        with self.assertRaisesRegex(RuntimeError, "external linked|linked/special"):
            compact_diagnostic_run(root, "owned_run")
        self.assertTrue(os.path.islink(os.path.join(root, "linked_payload")))
        self.assertFalse(os.path.exists(os.path.join(root, "evidence.tar.gz")))
        with open(target, "rb") as stream:
            self.assertEqual(stream.read(), b"untouched source")

    def wait(self, predicate, timeout=2):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(.003)
        self.fail("Diagnostic fixture did not reach its expected asynchronous state")

    def test_slow_scheduler_is_bounded_and_does_not_block_selected_run_stop(self):
        manager, service, spec = self.fixture({"private_netlist": "wait_for_stop"})
        spec["execution_backend"] = "private_netlist"
        self.scheduler_release.clear()
        result = manager.start("prepared-fixture", spec)
        identifier = result["test_id"]
        self.assertTrue(self.scheduler_entered.wait(2))
        self.wait(lambda: len(service.starts) == 1)
        self.wait(lambda: bool(manager.progress(identifier)["active_runs"]))
        row = manager.tests[identifier]
        manager._scheduler(row, "maestro_netlist", [{"job_id": str(1000 + index)} for index in range(100)])
        self.assertFalse(self.scheduler_release.is_set())
        before = time.monotonic()
        payload = manager.progress(identifier)
        self.assertLess(time.monotonic() - before, .25)
        self.assertTrue(payload["active_runs"])
        before = time.monotonic()
        manager.stop(identifier)
        self.assertLess(time.monotonic() - before, .25)
        self.assertTrue(service.stops)
        self.scheduler_release.set()
        self.finish(manager, identifier)
        self.assertEqual(len(self.scheduler_threads), 1)
        self.assertTrue(all(len(query) <= 64 for query in self.queries))

    def test_blocked_report_io_does_not_hold_progress_or_stop_and_join_waits_for_writer(self):
        manager, service, spec = self.fixture({"maestro_netlist": "wait_for_stop"})
        blocked, release = threading.Event(), threading.Event()
        self.addCleanup(release.set)
        original_open = os.open

        def delayed_open(path, flags, *arguments, **keywords):
            if (str(path).endswith("environment_test.log") and
                    threading.current_thread().name.endswith("-report-writer")):
                blocked.set()
                release.wait(5)
            return original_open(path, flags, *arguments, **keywords)

        with mock.patch("crescendo_engine.environment_test.os.open", side_effect=delayed_open):
            result = manager.start("prepared-fixture", spec)
            identifier = result["test_id"]
            self.assertTrue(blocked.wait(2))
            self.wait(lambda: bool(manager.progress(identifier)["active_runs"]))
            before = time.monotonic()
            manager.progress(identifier)
            self.assertLess(time.monotonic() - before, .25)
            before = time.monotonic()
            manager.stop(identifier)
            self.assertLess(time.monotonic() - before, .25)
            self.assertFalse(manager.join(.01), "A blocked owned report writer must remain visible to Exit")
            release.set()
            payload = self.finish(manager, identifier)
        self.assertEqual(payload["state"], "cancelled")
        for path in payload["engine_logs"].values():
            with open(path, "r", encoding="utf-8") as stream:
                self.assertIn("test.summary", stream.read())

    def test_sampling_limit_warns_once_and_still_records_final_checks(self):
        manager, service, spec = self.fixture({"maestro_netlist": "wait_for_stop"})
        result = manager.start("prepared-fixture", spec)
        identifier = result["test_id"]
        self.wait(lambda: bool(manager.progress(identifier)["active_runs"]))
        row = manager.tests[identifier]
        with manager.lock:
            row["case_record_bytes"]["environment"] = 8 * 1024 * 1024
        for _attempt in range(2):
            manager._log(row, {"observation": "fixture.sample_should_not_persist"}, "maestro_netlist")
        manager._check(row, "fixture.final_proof", "FAIL", "Final proof is still recorded", engine="maestro_netlist")
        manager.stop(identifier)
        payload = self.finish(manager, identifier)
        capacity = [check for check in self.checks(payload, "log.capacity") if check["category"] == "Maestro"]
        self.assertEqual(len(capacity), 1)
        self.assertEqual(capacity[0]["status"], "WARN")
        with open(payload["engine_logs"]["maestro_netlist"], "r", encoding="utf-8") as stream:
            report = stream.read()
        self.assertNotIn("fixture.sample_should_not_persist", report)
        self.assertIn("Final proof is still recorded", report)
        self.assertIn("test.summary", report)

    def test_first_cause_has_reserved_observation_space_after_ordinary_capacity(self):
        manager, service, spec = self.fixture({"maestro_netlist": "wait_for_stop"})
        identifier = manager.start("prepared-fixture",spec)["test_id"]
        self.wait(lambda: bool(manager.progress(identifier)["active_runs"]))
        row = manager.tests[identifier]
        with manager.lock:
            row["case_record_bytes"]["environment"] = 6 * 1024 * 1024
        manager._log(row,{"observation":"fixture.ordinary_should_not_persist"},"maestro_netlist")
        manager._log(row,{"observation":"diagnostic.producer_failure","payload":{"error":"reserved exact producer cause"}},"maestro_netlist")
        self.assertTrue(any(item.get("payload",{}).get("error") == "reserved exact producer cause"
                            for item in row["case_records"].get("environment",[])))
        self.assertFalse(any(item.get("observation") == "fixture.ordinary_should_not_persist"
                             for item in row["case_records"].get("environment",[])))
        manager.stop(identifier)
        self.finish(manager,identifier)

    def test_slow_filesystem_queue_overflow_keeps_complete_final_checks_in_disk_report(self):
        manager, service, spec = self.fixture({"maestro_netlist": "wait_for_stop"})
        blocked, release = threading.Event(), threading.Event()
        self.addCleanup(release.set)
        original_open = os.open

        def delayed_open(path, flags, *arguments, **keywords):
            if (str(path).endswith("environment_test.log") and
                    threading.current_thread().name.endswith("-report-writer")):
                blocked.set()
                release.wait(5)
            return original_open(path, flags, *arguments, **keywords)

        with mock.patch("crescendo_engine.environment_test.os.open", side_effect=delayed_open):
            result = manager.start("prepared-fixture", spec)
            identifier = result["test_id"]
            self.assertTrue(blocked.wait(2))
            self.wait(lambda: bool(manager.progress(identifier)["active_runs"]))
            row = manager.tests[identifier]
            for index in range(160):
                manager._log(row, {"id": "fixture.queue_pressure", "status": "WARN", "message": str(index)}, "maestro_netlist")
            self.assertTrue(row["log_queue"].full())
            manager._check(row, "fixture.critical_final", "FAIL", "Critical final result survives log queue overflow",
                           engine="maestro_netlist")
            manager.stop(identifier)
            release.set()
            payload = self.finish(manager, identifier)
        self.assertTrue(any(check.get("check_id") == "fixture.critical_final" for check in payload["checks"]))
        with open(payload["engine_logs"]["maestro_netlist"], "r", encoding="utf-8") as stream:
            report = stream.read()
        self.assertIn("Critical final result survives log queue overflow", report)
        self.assertIn("test.summary", report)

    def test_join_does_not_miss_auxiliary_thread_registered_during_its_wait(self):
        manager, _service, _spec = self.fixture()

        class LateAuxiliary(object):
            def join(self, _timeout):
                pass

            def is_alive(self):
                return True

        class MainThread(object):
            def join(self, _timeout):
                if not manager.tests["late"]["aux_threads"]:
                    manager.tests["late"]["aux_threads"].append(LateAuxiliary())

            def is_alive(self):
                return False

        manager.tests["late"] = {"thread": MainThread(), "aux_threads": []}
        try:
            self.assertFalse(manager.join(.01), "Late probe/scheduler registration must not be certified as joined")
        finally:
            manager.tests.pop("late")

    def test_internal_psf_link_is_archived_without_following_or_modifying_kept_core_target(self):
        root = os.path.join(self.directory.name, "internal_link_run")
        os.makedirs(root)
        target = os.path.join(root, "optimization.crescendo")
        with open(target, "wb") as stream:
            stream.write(b"unchanged core target")
        os.symlink("optimization.crescendo", os.path.join(root, "native_psf_link"))
        result = compact_diagnostic_run(root, "internal_link_run")
        self.assertFalse(os.path.lexists(os.path.join(root, "native_psf_link")))
        with open(target, "rb") as stream:
            self.assertEqual(stream.read(), b"unchanged core target")
        with tarfile.open(result["archive"], "r:gz") as archive:
            member = archive.getmember("native_psf_link")
            self.assertTrue(member.issym())
            self.assertEqual(member.linkname, "optimization.crescendo")
            self.assertNotIn("optimization.crescendo", archive.getnames())

    def test_hardlinked_payload_is_retained_without_deleting_either_name(self):
        root = os.path.join(self.directory.name, "hardlink_run")
        os.makedirs(root)
        target = os.path.join(self.directory.name, "original-hardlinked.oa")
        with open(target, "wb") as stream:
            stream.write(b"source hardlink")
        os.link(target, os.path.join(root, "payload.oa"))
        with self.assertRaisesRegex(RuntimeError, "linked/special"):
            compact_diagnostic_run(root, "hardlink_run")
        self.assertTrue(os.path.exists(os.path.join(root, "payload.oa")))
        with open(target, "rb") as stream:
            self.assertEqual(stream.read(), b"source hardlink")

    def test_owned_parent_checkpoint_is_archived_and_released_locks_removed_to_five_files(self):
        parent = os.path.join(self.directory.name, "mode")
        root = os.path.join(parent, "checkpoint_run")
        os.makedirs(root)
        for name in ("status.json", "worker_shutdown.json", "optimization.crescendo"):
            with open(os.path.join(root, name), "wb") as stream:
                stream.write(b"fixture retained evidence")
        with open(os.path.join(parent, "environment_test.log"), "w", encoding="utf-8") as stream:
            stream.write("fixture core report")
        checkpoint = continuation_checkpoint_path(parent, "owned_continuation", create=True)
        with open(checkpoint, "w", encoding="utf-8") as stream:
            json.dump({"manifest": {"origin_run_id": "checkpoint_run", "origin_run_root": root}}, stream)
        runtime = RuntimeOptimizationLease(parent, run_root=root).acquire()
        runtime.release()
        continuation = ContinuationLease(parent, "owned_continuation", run_root=root).acquire()
        continuation.release()
        result = compact_diagnostic_run(root, "checkpoint_run")
        cleanup_test_runtime_lease(parent)
        self.assertFalse(os.path.exists(os.path.join(parent, ".continuation_checkpoints")))
        self.assertFalse(os.path.exists(os.path.join(parent, ".continuation_leases")))
        self.assertEqual(sum(len(names) for _directory, _children, names in os.walk(parent)), 5)
        with tarfile.open(result["archive"], "r:gz") as archive:
            snapshot = next(name for name in archive.getnames() if name.startswith("_runtime_checkpoints/"))
            document = json.loads(archive.extractfile(snapshot).read().decode("utf-8"))
            self.assertEqual(document["manifest"]["origin_run_root"], root)

    def test_foreign_parent_checkpoint_is_not_deleted_or_archived(self):
        parent = os.path.join(self.directory.name, "foreign_checkpoint_mode")
        root = os.path.join(parent, "owned_checkpoint_run")
        os.makedirs(root)
        checkpoint = continuation_checkpoint_path(parent, "foreign_continuation", create=True)
        with open(checkpoint, "w", encoding="utf-8") as stream:
            json.dump({"manifest": {"origin_run_id": "another_run", "origin_run_root": os.path.join(parent, "another_run")}}, stream)
        with self.assertRaisesRegex(RuntimeError, "origin differs"):
            compact_diagnostic_run(root, "owned_checkpoint_run")
        self.assertTrue(os.path.isfile(checkpoint))
        self.assertFalse(os.path.exists(os.path.join(root, "evidence.tar.gz")))

    def test_active_or_foreign_process_lease_is_retained(self):
        for state, pid in (("active", os.getpid()), ("released", os.getpid() + 100000)):
            with self.subTest(state=state, pid=pid):
                parent = os.path.join(self.directory.name, "lease-%s-%s" % (state, pid))
                namespace = os.path.join(parent, ".continuation_leases")
                os.makedirs(namespace)
                path = os.path.join(namespace, "a" * 64 + ".lock")
                with open(path, "w", encoding="utf-8") as stream:
                    json.dump({"schema_version": "crescendo.continuation_lease.v1", "lease_kind": "continuation",
                               "state": state, "pid": pid, "run_root": os.path.join(parent, "own_run")}, stream)
                with self.assertRaisesRegex(RuntimeError, "not released by this process"):
                    cleanup_test_runtime_lease(parent)
                self.assertTrue(os.path.isfile(path))


    def test_diagnostic_freezes_solver_and_submission_templates_without_mutation(self):
        manager, service, spec = self.fixture()
        spec['execution_commands'] = {'solver_templates': {'spectre': '{simulator} {netlist} -raw {psf}',
            'alps': '{simulator} {netlist} -format ps -o {psf}'},
            'submission_template': 'batch-submit -q chosen -R "cpu=6;mem=20000" {job_script}'}
        before = copy.deepcopy(spec)
        payload = self.finish(manager, manager.start('prepared-fixture', spec)['test_id'])
        self.assertEqual(len(service.starts), 3)
        for request in service.starts:
            if request['execution_backend'] == 'maestro_baseline':
                self.assertNotIn('execution_commands', request)
            else:
                self.assertEqual(request['execution_commands'], before['execution_commands'])
            self.assertIn(request['execution_backend'], ('maestro_baseline', 'maestro_netlist', 'private_netlist'))
            self.assertEqual(request['maestro_job_overrides'], before['maestro_job_overrides'])
        self.assertEqual(spec, before)

    def test_native_solver_or_missing_common_command_proof_cannot_pass(self):
        for behavior in ('retired_native_solver', 'missing_expanded_submission'):
            with self.subTest(behavior=behavior):
                manager, service, spec = self.fixture({'maestro_netlist': behavior})
                payload = self.finish(manager, manager.start('prepared-fixture', spec)['test_id'])
                self.assertEqual(payload['case_results'][2]['state'], 'failed')
                self.assertEqual(payload['case_results'][2]['first_cause']['failure_stage'], 'execution_pipeline')
                self.assertTrue(payload['case_results'][2]['stop_proven'])
                self.assertEqual(len(payload['case_results']), 3)
                self.assertTrue(all(check['status'] == 'FAIL' for check in self.checks(payload, 'execution.pipeline') if check['engine'] == 'maestro_netlist'))

    def test_legacy_log_marker_resolves_only_to_canonical_maestro_netlist_root(self):
        manager, service, spec = self.fixture({'maestro_netlist': 'wait_for_stop'})
        result = manager.start('prepared-fixture', spec)
        with self.assertRaises(ValueError):
            manager.run_root({'schema_version': 'crescendo.environment_test.run.v1',
                'test_id': result['test_id'], 'simulator': 'spectre'}, 'persistent_maestro')
        manager.stop(result['test_id'])
        self.finish(manager, result['test_id'])
        self.assertTrue(all(request['execution_backend'] != 'persistent_maestro' for request in service.starts))


class SimulatorDiagnosticCapabilityTests(unittest.TestCase):
    def test_shared_spectre_readiness_message_does_not_claim_undetected_model_root(self):
        import sys
        config = {'cadence': {'ocean_bin': sys.executable, 'spectre_bin': sys.executable}}
        prepared = {'setup': {'tests': [{'name': 'active', 'simulator': 'spectre'}]}}
        manager = EnvironmentTestManager(mock.Mock())
        manager._check = mock.Mock()
        row = {'prepared': prepared}
        capability, reference = manager._shared_case_capability(row, config, 'spectre', 'maestro_netlist')
        self.assertTrue(capability['ready'])
        self.assertIsNone(capability['contract']['model_dir'])
        self.assertFalse(capability['model_directory_discovered'])
        self.assertFalse(capability['exact_point_models_qualified'])
        message = manager._check.call_args[0][3]
        self.assertIn('model search directory is not detected', message)
        self.assertNotIn('directory available', message)
        details = manager._check.call_args[0][4]
        self.assertEqual(details['native_result_qualification'], 'required')

    def test_spectre_capture_source_can_qualify_both_alps_netlist_sources(self):
        import sys
        from crescendo_engine.environment_test import _case_capability
        with tempfile.TemporaryDirectory() as root:
            config = {'cadence': {'simulator': 'alps', 'ocean_bin': sys.executable,
                'alps_bin': sys.executable, 'alps_model_dir': root}}
            prepared = {'setup': {'tests': [{'name': 'active', 'simulator': 'spectre'},
                {'name': 'off', 'simulator': 'alps', 'enabled': False}]}}
            for engine in ('maestro_netlist', 'private_netlist'):
                result = _case_capability(config, prepared, 'alps', engine)
                self.assertTrue(result['ready'])
                self.assertEqual(result['contract']['capture_source_simulator'], 'spectre')
                self.assertEqual(result['contract']['execution_simulator'], 'alps')
                self.assertEqual(result['native_result_qualification'], 'required')

    def test_alps_saved_source_capture_is_blocked_before_executable_only_readiness(self):
        import sys
        from crescendo_engine.environment_test import _case_capability
        prepared = {'setup': {'tests': [{'name': 'tb', 'simulator': 'alps'}]}}
        config = {'cadence': {'ocean_bin': sys.executable, 'simulator': 'alps'}}
        with mock.patch('crescendo_engine.simulators.simulator_readiness') as readiness:
            result = _case_capability(config, prepared, 'alps', 'maestro_netlist')
        self.assertFalse(result['ready'])
        self.assertEqual(result['failure_stage'], 'netlist_generation_preflight')
        self.assertIn('ALPS-source capture', result['reason'])
        readiness.assert_not_called()

    def test_alps_completion_requires_adapter_and_native_readback_identity(self):
        from crescendo_engine.environment_test import _simulator_completion_matches
        evidence = {'simulator': 'alps', 'execution_simulator': 'alps', 'simulator_runs': [{'returncode': 0}],
                    'native_parameter_readback_verified': True, 'spectre_runs': []}
        self.assertTrue(_simulator_completion_matches(evidence, 'alps'))
        for field in ('execution_simulator', 'simulator_runs', 'native_parameter_readback_verified', 'spectre_runs'):
            missing = dict(evidence);missing.pop(field)
            self.assertFalse(_simulator_completion_matches(missing, 'alps'))
        wrong = dict(evidence, spectre_runs=[{'error_count': 0}])
        self.assertFalse(_simulator_completion_matches(wrong, 'alps'))
        self.assertTrue(_simulator_completion_matches({'ok': True}, 'spectre'))


if __name__ == "__main__":
    unittest.main()
