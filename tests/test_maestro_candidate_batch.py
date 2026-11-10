from __future__ import print_function

import copy
import json
import os
import tempfile
import unittest

from test_persistent_maestro import FakePopenFactory, PersistentMaestroTest
from crescendo_engine.maestro_batch import (
    MaestroBatchUnsupported, prepare_snapshot_batch, qualify_snapshot_batch,
    snapshot_cleanup_lines, validate_snapshot_batch,
)
from crescendo_engine.persistent_maestro import PersistentMaestroWorkerProtocolError


class BatchFactory(FakePopenFactory):
    def on_load(self, process, path):
        name = os.path.basename(path)
        if name == "bootstrap.il":
            result = super(BatchFactory, self).on_load(process, path)
            expected = self.worker.expected_job_policy
            if expected.get('overrides'):
                properties = dict(expected['global']['properties'])
                for wrapper in self.worker.job_observation.get('wrappers') or []:
                    if wrapper.get('test') is None:
                        properties['jobsubmitcommand'] = wrapper['effective_command']
                rows = ['CRESCENDO_JOB_POLICY_1', 'P\t"global"\tnil\tt']
                for key, value in properties.items():
                    rows.append('J\t"global"\tnil\t%s\t%s' % (json.dumps(key),json.dumps(value)))
                for test in self.worker.workspace['setup']['tests']:
                    rows.extend(['P\t"test"\t%s\tt' % json.dumps(test['name']),
                                 'D\t"test"\t%s' % json.dumps(test['name'])])
                with open(self.worker.job_policy_readback_path,'w') as stream:
                    stream.write('\n'.join(rows)+'\n')
            return result
        if name not in ("persistent_worker_batch.il", "persistent_worker_batch_finish.il",
                        "persistent_worker_batch_abort.il"):
            return super(BatchFactory, self).on_load(process, path)
        request = self._request(path)
        if name == "persistent_worker_batch.il":
            with open(os.path.join(os.path.dirname(path), "persistent_worker_batch.json")) as stream:
                self.batch = json.load(stream)
            self._append(request, "ACCEPTED")
            if self.mode == "batch_unsupported":
                self._append(request, "BATCH_UNSUPPORTED", message="native_batch_api_unavailable")
                return
            with open(self.batch["pristine_restore_path"], "w") as stream:
                for row in self.batch["pristine_cells"]:
                    stream.write('RESTORED\t"%s"\t"%s"\t"%s"\t"%s"\n' % (
                        row["library"], row["cell"], row["pristine_library"], row["pristine_cell"]))
            for candidate in self.batch["candidates"]:
                with open(candidate["clone_records"], "w") as stream:
                    for row in candidate["clone_plan"]:
                        stream.write('CLONE\t"%s"\t"%s"\t"%s"\t"%s"\n' % (
                            row["occurrence_path"], row["source_cell"], row["clone_cell"], row["parent_cell"]))
            with open(self.batch["snapshot_records"], "w") as stream:
                for row in self.batch["batch_tests"]:
                    stream.write("TEST\t" + "\t".join(json.dumps(v) for v in
                        (row["name"], row["source"]) + tuple(row["design"])) + "\n")
            states = ("BEGIN", "PATCHED") if self.mode == "batch_stopped" else ("BEGIN", "PATCHED", "SIM_DONE")
            for state in states:
                self._append(request, state)
            if self.mode == "batch_stopped":
                self.worker._active_batch["abort_requested"] = True
                self.worker._active_batch["native_stop_proven"] = True
        elif name == "persistent_worker_batch_finish.il":
            self._append(request, "RESULTS_READY", message=process.history_name)
            self._append(request, "DONE", message=process.history_name)
        else:
            for state in ("STOP_BEGIN", "STOP_SENT", "STOP_DONE"):
                self._append(request, state)


class CandidateBatchTest(unittest.TestCase):
    def worker(self, root, mode="success", nested=False, overrides=None):
        fixture = PersistentMaestroTest()
        parameters = ([fixture._parameter("p", "/IA/I0/M0", "/IA/I0", fixture._chain("/IA"))]
                      if nested else [fixture._parameter("p", "/M0", "/", [])])
        worker, _old, clock = fixture._worker(root, parameters=parameters)
        factory = BatchFactory(mode)
        worker._popen_factory = factory
        worker._killpg = factory.killpg
        worker.workspace["setup"] = {
            "tests": [{"name": "source", "enabled": True, "library": "D", "cell": "TOP",
                       "view": "schematic", "simulator": "spectre", "outputs": []}],
            "outputs": [], "corners": [],
        }
        if overrides:
            worker.config["runtime"]["maestro_job_overrides"] = overrides
        factory.worker = worker
        worker.start()
        factory.worker = worker
        self.addCleanup(worker._close_streams)
        return worker, factory, clock

    def requests(self, root, count=3):
        return [{"candidate_id": "c%d" % i, "values": {"p": (i + 1) * 1e-6},
                 "candidate_root": os.path.join(root, "c%d" % i)} for i in range(count)]

    def test_real_batch_script_submits_once_and_isolates_each_candidate(self):
        with tempfile.TemporaryDirectory() as root:
            worker, factory, _clock = self.worker(root)
            batch = worker.evaluate_batch(self.requests(root), os.path.join(root, "batch"))
            with open(batch["script_path"]) as stream:
                script = stream.read()
            self.assertEqual(script.count("runId = maeRunSimulation("), 1)
            self.assertIn("?waitUntilDone nil", script)
            self.assertIn('procedure(crescBatchFinished()', script)
            self.assertLess(script.index("crescBatchSourceUsesGlobalPolicy(session sourceName)"),
                            script.index(' "BEGIN" "")'))
            self.assertIn('independent_test_job_policy_requires_serial_adapter', script)
            cells = [c["patch_plan"]["parameters"]["parameters"][0]["cell"] for c in batch["candidates"]]
            self.assertEqual(len(set(cells)), 3)
            self.assertTrue(all(len(cell) <= 32 for cell in cells))
            self.assertEqual(len(factory.processes), 1)
            self.assertEqual(batch["history_name"], factory.processes[0].history_name)
            self.assertTrue(batch["shared_history"])
            self.assertEqual(worker.state, "awaiting_cleanup")
            self.assertTrue(validate_snapshot_batch(batch)["ok"])
            with self.assertRaises(Exception):
                worker.evaluate("other", {"p": 1e-6}, os.path.join(root, "other"))
            cleanup = worker.request_cleanup(batch)
            with open(cleanup["script_path"]) as stream:
                cleanup_script = stream.read()
            self.assertLess(cleanup_script.index("axlRemoveSimulationResults"), cleanup_script.index("axlRemoveElement"))
            self.assertIn("ddDeleteObj(batchOwnedCell)", cleanup_script)
            self.assertEqual(worker.state, "ready")
            self.assertIsNone(worker.pending_cleanup)

    def test_population_is_independent_of_maxjobs(self):
        for maxjobs, population in ((1, 4), (64, 3)):
            with tempfile.TemporaryDirectory() as root:
                worker, _factory, _clock = self.worker(root)
                worker.config["runtime"]["maestro_job_overrides"] = {"max_jobs": maxjobs}
                batch = worker.evaluate_batch(self.requests(root, population), os.path.join(root, "batch"))
                self.assertEqual(len(batch["candidates"]), population)
                with open(batch["script_path"]) as stream:
                    script = stream.read()
                self.assertIn('original->maxjobs = "%d"' % maxjobs, script)
                self.assertEqual(script.count("runId = maeRunSimulation("), 1)

    def test_candidate_setup_preserves_scope_contract_and_records_native_corner_copy_stages(self):
        with tempfile.TemporaryDirectory() as root:
            worker, _factory, _clock = self.worker(root)
            contract = {'global_variables': [{'name': 'Fref', 'value': '64M',
                                             'dependent_tests': [{'test': 'source', 'enabled': True}]}],
                        'model_groups': [{'name': 'TT', 'models': [{'file': '/pdk/model.scs', 'section': 'tt'}]}],
                        'execution_settings': [{'tag': 'native_scope_metadata', 'text': 'preserved'}]}
            worker.workspace['setup'].update(copy.deepcopy(contract))
            batch = worker.evaluate_batch(self.requests(root, 1), os.path.join(root, 'batch'))
            candidate = batch['candidates'][0]['verification_workspace']
            for key, value in contract.items():
                self.assertEqual(candidate['setup'][key], value)
                self.assertIsNot(candidate['setup'][key], worker.workspace['setup'][key])
            with open(batch['script_path']) as stream:
                script = stream.read()
            for stage in ('before_copy', 'after_copy', 'after_corner_verify', 'after_test_enable'):
                self.assertIn('crescRecordBatchCornerScope("%s"' % stage, script)
            self.assertLess(script.index('crescRecordBatchCornerScope("before_copy"'), script.index('unless(maeCreateTest('))
            self.assertLess(script.index('crescRecordBatchCornerScope("after_corner_verify"'), script.index('runId = maeRunSimulation('))
            self.assertNotIn('maeSetCorner(', script)
            self.assertIn('inherited corner scope differs from source', script)
            self.assertIn('corner scope changed while enabling candidate tests', script)
            self.assertIn('inherited_exact_native_readback', script)
            self.assertEqual(candidate['native_scope_path'], batch['native_scope_path'])
            self.assertEqual(os.path.basename(batch['native_scope_path']), 'portability_settings.json')

    def test_explicit_job_override_applies_to_private_session_before_candidates(self):
        with tempfile.TemporaryDirectory() as root:
            worker, _factory, _clock = self.worker(root, overrides={"max_jobs": 7,
                "command": 'batch-submit -q short -R "cpu=6;mem=30000"'})
            with open(os.path.join(worker.control_root, "bootstrap.il")) as stream:
                bootstrap = stream.read()
            self.assertLess(bootstrap.index('session = maeOpenSetup('),
                            bootstrap.index("crescApplyJobOverrides(session)"))
            self.assertIn('original->maxjobs = "7"', bootstrap)
            self.assertIn('original->distributionmethod = "Command"', bootstrap)

    def test_stop_before_planning_prevents_later_submission(self):
        with tempfile.TemporaryDirectory() as root:
            worker, factory, _clock = self.worker(root)
            stop = worker.request_batch_abort()
            self.assertTrue(stop["no_remote_jobs_started"])
            self.assertTrue(stop["remote_stop_proven"])
            with self.assertRaisesRegex(Exception, "aborted before submission"):
                worker.evaluate_batch(self.requests(root), os.path.join(root, "batch"))
            self.assertFalse(os.path.exists(os.path.join(root, "batch")))
            self.assertEqual(len(factory.processes), 1)

    def test_native_stop_releases_evaluation_without_completion_callback(self):
        with tempfile.TemporaryDirectory() as root:
            worker, factory, _clock = self.worker(root, mode="batch_stopped")
            with self.assertRaisesRegex(Exception, "native cancellation acknowledged"):
                worker.evaluate_batch(self.requests(root), os.path.join(root, "batch"))
            self.assertTrue(worker.closed)
            self.assertTrue(worker._active_batch["native_stop_proven"])
            self.assertEqual(len(factory.processes), 1)

    def test_ancestor_branches_are_isolated_not_just_top(self):
        with tempfile.TemporaryDirectory() as root:
            worker, _factory, _clock = self.worker(root, nested=True)
            manifest = prepare_snapshot_batch(worker, self.requests(root, 2), os.path.join(root, "batch"),
                {"epoch": worker.epoch, "sequence": 1, "nonce": "n", "candidate_id": "batch"})
            first, second = manifest["candidates"]
            cells1 = set(row["clone_cell"] for row in first["clone_plan"])
            cells2 = set(row["clone_cell"] for row in second["clone_plan"])
            self.assertFalse(cells1 & cells2)
            self.assertEqual(len(cells1), 2)
            for c in (first, second):
                parent, child = c["clone_plan"]
                self.assertEqual(child["parent_cell"], parent["clone_cell"])
                self.assertEqual(c["patch_plan"]["parameters"]["parameters"][0]["cell"], child["clone_cell"])
                self.assertEqual(parent["source_cell"], worker.clone_plan[0]["clone_cell"])

    def test_product_candidate_directories_and_escape_guard(self):
        with tempfile.TemporaryDirectory() as root:
            worker, _factory, _clock = self.worker(os.path.join(root, "workers", "epoch"))
            request = {"candidate_id": "g000_c0000", "values": {"p": 1e-6},
                       "candidate_root": os.path.join(root, "candidates", "g000_c0000")}
            batch = prepare_snapshot_batch(worker, [request], os.path.join(root, "batches", "g000"),
                {"epoch": worker.epoch, "sequence": 1, "nonce": "n", "candidate_id": "batch"})
            self.assertEqual(batch["candidates"][0]["candidate_root"], request["candidate_root"])
            bad = dict(request, candidate_root=os.path.join(root, "unowned", "g000_c0000"))
            with self.assertRaisesRegex(ValueError, "outside owned run"):
                prepare_snapshot_batch(worker, [bad], os.path.join(root, "batches", "g001"),
                    {"epoch": worker.epoch, "sequence": 2, "nonce": "n", "candidate_id": "batch"})
            self.assertFalse(os.path.exists(bad["candidate_root"]))
            alias = os.path.join(root, "candidates", "alias")
            os.symlink(os.path.join(root, "unowned"), alias)
            bad = dict(request, candidate_id="alias", candidate_root=alias)
            with self.assertRaisesRegex(ValueError, "outside owned run"):
                prepare_snapshot_batch(worker, [bad], os.path.join(root, "batches", "g002"),
                    {"epoch": worker.epoch, "sequence": 3, "nonce": "n", "candidate_id": "batch"})

    def test_config_snapshot_preserves_real_binding_and_opaque_stop(self):
        with tempfile.TemporaryDirectory() as root:
            worker, _factory, _clock = self.worker(root)
            worker.parameters[0]["design_view"] = "config"
            worker.parameters[0]["owner_view"] = "schematic"
            worker.parameters[0]["resolved_top"] = {"library": "D", "cell": "TOP", "view": "schematic"}
            worker.workspace["setup"]["tests"][0]["view"] = "config"
            worker.workspace["config_designs"] = [{"library": "D", "cell": "TOP", "view": "config",
                "resolved_top": {"library": "D", "cell": "TOP", "view": "schematic"},
                "config_bindings": [{"hierarchy_path": "/VA", "parent_path": "/", "name": "VA",
                    "master_library": "D", "master_cell": "opaque", "master_view": "symbol",
                    "resolved_library": "D", "resolved_cell": "opaque", "resolved_view": "veriloga",
                    "scan_traversable": False, "stop": True}]}]
            manifest = prepare_snapshot_batch(worker, self.requests(root, 2), os.path.join(root, "batch"),
                {"epoch": worker.epoch, "sequence": 1, "nonce": "n", "candidate_id": "batch"})
            configs = [c["config_binding_plan"][0] for c in manifest["candidates"]]
            self.assertNotEqual(configs[0]["config"], configs[1]["config"])
            self.assertNotEqual(configs[0]["top"], configs[1]["top"])
            for cfg in configs:
                self.assertEqual(cfg["bindings"][0]["target"], ("D", "opaque", "veriloga"))
                self.assertTrue(cfg["bindings"][0]["stop"])

    def test_native_unsupported_is_before_mutation_and_preserves_ready_worker(self):
        with tempfile.TemporaryDirectory() as root:
            worker, _factory, _clock = self.worker(root, mode="batch_unsupported")
            with self.assertRaises(MaestroBatchUnsupported):
                worker.evaluate_batch(self.requests(root), os.path.join(root, "batch"))
            self.assertEqual(worker.state, "ready")
            self.assertFalse(worker.tainted)
            self.assertIsNone(worker.pending_cleanup)
            self.assertTrue(worker.is_alive())

    def test_existing_device_sweep_or_cross_test_expression_has_explicit_boundary(self):
        with tempfile.TemporaryDirectory() as root:
            worker, _factory, _clock = self.worker(root)
            source = os.path.join(root, "maestro.sdb")
            with open(source, "w") as stream:
                stream.write('<setupdb><active><parameters><parameter Name="D/TOP/schematic/M0/w"/></parameters></active></setupdb>')
            worker.workspace["setup"]["source"] = {"setup_db": source}
            result = qualify_snapshot_batch(worker.workspace, worker.parameters)
            self.assertFalse(result["supported"])
            self.assertIn("existing_device_sweeps_require_snapshot_remapping", result["reasons"])
            worker.workspace["setup"]["source"] = {}
            worker.workspace["setup"]["outputs"] = [{"expression": 'calcVal("out" "source")'}]
            result = qualify_snapshot_batch(worker.workspace, worker.parameters)
            self.assertIn("cross_test_output_requires_remapping", result["reasons"])
            worker.workspace["setup"]["outputs"] = []
            worker.workspace["setup"]["tests"][0]["outputs"] = [{"expression": 'calcVal("out" "source")'}]
            result = qualify_snapshot_batch(worker.workspace, worker.parameters)
            self.assertIn("cross_test_output_requires_remapping", result["reasons"])

    def test_owned_cleanup_rejects_arbitrary_source_cell(self):
        with tempfile.TemporaryDirectory() as root:
            worker, _factory, _clock = self.worker(root)
            batch = worker.evaluate_batch(self.requests(root), os.path.join(root, "batch"))
            bad = copy.deepcopy(batch)
            bad["owned_cells"][0]["cell"] = "TOP"
            with self.assertRaises(ValueError):
                snapshot_cleanup_lines(bad)
            with self.assertRaises(PersistentMaestroWorkerProtocolError):
                wrong = dict(batch, protocol_identity=dict(batch["protocol_identity"], nonce="wrong"))
                worker.request_cleanup(wrong)

    def test_native_stop_evidence_does_not_equate_local_death_to_remote_stop(self):
        with tempfile.TemporaryDirectory() as root:
            worker, _factory, _clock = self.worker(root)
            batch = worker.evaluate_batch(self.requests(root), os.path.join(root, "batch"))
            completed = worker.request_batch_abort()
            self.assertTrue(completed["remote_stop_proven"])
            self.assertTrue(completed["batch_completed"])
            worker.state = "busy"
            worker._active_batch["native_completion_proven"] = False
            stop = worker.request_batch_abort()
            self.assertTrue(stop["remote_stop_proven"])
            with open(batch["abort_script_path"]) as stream:
                script = stream.read()
            self.assertIn("maeStopSimulation(?session session)", script)
            self.assertIn("maeWaitUntilDone('All ?session session)", script)
            worker._active_batch["native_stop_proven"] = False
            worker.process.returncode = 0
            stop = worker.request_batch_abort()
            self.assertFalse(stop["remote_stop_proven"])


if __name__ == "__main__":
    unittest.main()
