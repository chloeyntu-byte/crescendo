"""Shared-suite scheduling and durable snapshots; no native EDA process."""
import copy
import hashlib
import json
import os
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from unittest import mock

from crescendo_engine.netlist_providers import _freeze_generated_dependencies
from crescendo_engine.private_netlist import (PrivateNetlistError, _manifest_digest,
    validate_manifest, reconcile_preparation, SCHEMA)
from crescendo_engine.shared_maestro import (SharedMaestroProducer, owner_records,
    write_reference, REFERENCE)
from crescendo_engine.netlist_run_ownership import reconcile_run_preparations
from crescendo_engine.native_includes import NativeIncludeError
from crescendo_engine.utils import atomic_write_json
from tests import test_netlist_providers as provider_fixtures


class SharedMaestroTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = self.temp.name
        self.fixture = provider_fixtures.NumericMaestroProviderTest()
        self.prepared,self.parameters,self.config,self.spec,self.model = self.fixture.fixtures(self.root)
        self.config['runtime']['run_root'] = self.root
        self.options = []

    def tearDown(self):
        self.temp.cleanup()

    def suite(self, capture=None, proof=None):
        stack = ExitStack()
        self.addCleanup(stack.close)
        workspace = stack.enter_context(mock.patch('crescendo_engine.netlist_providers.build_workspace',side_effect=self.fixture.workspace))
        implementation = self.fixture.capture(self.model)
        def record(*args,**kwargs):
            self.options.append(dict(kwargs))
            result = implementation(*args,**kwargs)
            path = result['tasks'][0]['netlist_path']
            with open(path,'a') as stream: stream.write('parameters candidate_width=%.17g\n' % args[2]['p'])
            result['tasks'][0]['template_files'][0]['sha256'] = self.digest(path)
            return result
        captured = stack.enter_context(mock.patch('crescendo_engine.netlist_providers._capture_phase',side_effect=capture or record))
        stack.enter_context(mock.patch('crescendo_engine.netlist_providers.verify_sources_unchanged',return_value={'ok':True}))
        stack.enter_context(mock.patch('crescendo_engine.private_netlist.reconcile_preparation',side_effect=proof or (lambda *a,**k:{'cleanup_proven':True})))
        producer = SharedMaestroProducer(self.root,self.prepared,self.parameters,self.config,self.spec)
        return producer,workspace,captured

    def candidate(self,index):
        identifier = 'g000_c%04d' % index
        parent = os.path.join(self.root,'candidates',identifier)
        os.makedirs(parent)
        return os.path.join(parent,'simulation'),identifier

    @staticmethod
    def digest(path):
        with open(path,'rb') as stream: return hashlib.sha256(stream.read()).hexdigest()

    def test_a_b_a_reuses_one_suite_config_once_and_keeps_independent_manifests(self):
        producer,workspace,capture = self.suite()
        manifests, timings = [], []
        for index,value in enumerate((2e-6,4e-6,2e-6)):
            root,identifier = self.candidate(index)
            phases = {}
            timings.append(phases)
            manifests.append(producer.generate({'p':value},root,identifier,phase_timings=phases))
        self.assertEqual(sum('workspace_build' in phases for phases in timings),1)
        for phases in timings:
            self.assertGreaterEqual(phases['native_generation_lock_wait']['elapsed_seconds'],0)
            self.assertGreaterEqual(phases['native_generation_lock_held']['elapsed_seconds'],
                                    phases['native_capture']['elapsed_seconds'])
        self.assertEqual(workspace.call_count,1)
        self.assertEqual(capture.call_count,3)
        self.assertEqual([row['emit_clones'] for row in self.options],[True,False,False])
        self.assertEqual([row.get('private_config_initialized',False) for row in self.options],[False,True,True])
        self.assertEqual(len({row['workspace']['root'] for row in manifests}),1)
        self.assertEqual(len({row['tasks'][0]['cwd'] for row in manifests}),3)
        for row in manifests:
            self.assertTrue(validate_manifest(row))
            with open(os.path.join(row['root'],'private_netlist.json')) as stream:
                self.assertEqual(json.load(stream),row)
        with open(manifests[0]['tasks'][0]['netlist_path']) as stream: first = stream.read()
        with open(manifests[2]['tasks'][0]['netlist_path']) as stream: last = stream.read()
        self.assertEqual(first,last)
        self.assertEqual(manifests[0]['values'],manifests[2]['values'])

    def test_generation_and_native_dead_proof_serialized_but_solver_overlaps(self):
        trace,active = [],[0]
        lock = threading.Lock()
        implementation = self.fixture.capture(self.model)
        def capture(*args,**kwargs):
            with lock:
                active[0] += 1
                self.assertEqual(active[0],1)
                trace.append('capture')
            time.sleep(0.02)
            result = implementation(*args,**kwargs)
            with lock: active[0] -= 1
            return result
        def proof(*args,**kwargs):
            trace.append('proof_begin')
            time.sleep(0.02)
            trace.append('proof_end')
            return {'cleanup_proven':True}
        producer,workspace,unused = self.suite(capture,proof)
        entered,second = [],threading.Event()
        def solve(index):
            root,identifier = self.candidate(index)
            manifest = producer.generate({'p':index+1},root,identifier)
            with lock:
                entered.append(index)
                if len(entered)==2: second.set()
            self.assertTrue(second.wait(2),'solver was serialized inside generation lock')
            return manifest
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(solve,index) for index in range(2)]
            manifests = [future.result(timeout=4) for future in futures]
        self.assertEqual(trace,2*['capture','proof_begin','proof_end','proof_begin','proof_end'])
        self.assertEqual(workspace.call_count,1)
        self.assertTrue(all(validate_manifest(row) for row in manifests))

    def test_stop_waiting_for_generation_lock_never_starts_capture(self):
        producer,workspace,capture = self.suite()
        producer.lock.acquire()
        root,identifier = self.candidate(0)
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(producer.generate,{'p':1},root,identifier)
            time.sleep(0.06)
            producer.cancel()
            with self.assertRaisesRegex(PrivateNetlistError,'waiting'):
                future.result(timeout=1)
        producer.lock.release()
        capture.assert_not_called()
        workspace.assert_not_called()

    def test_unproven_native_stop_taints_suite_before_waiter_can_update_it(self):
        producer,workspace,capture = self.suite(proof=lambda *a,**k:{'cleanup_proven':False})
        root,identifier = self.candidate(0)
        with self.assertRaisesRegex(PrivateNetlistError,'not stopped'):
            producer.generate({'p':1},root,identifier)
        root,identifier = self.candidate(1)
        with self.assertRaisesRegex(PrivateNetlistError,'earlier generation failure'):
            producer.generate({'p':2},root,identifier)
        self.assertEqual(capture.call_count,1)
        self.assertFalse(producer.state['initialized'])

    def test_cdf_failure_taints_suite_before_next_waiter_and_never_skips_callback(self):
        entered = threading.Event()
        def failed_capture(*args,**kwargs):
            entered.set()
            time.sleep(0.04)
            raise PrivateNetlistError('CDF callback failed for nfin: FN_gm')
        producer,workspace,capture = self.suite(capture=failed_capture)
        first_root,first_id = self.candidate(0)
        second_root,second_id = self.candidate(1)
        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(producer.generate,{'p':1},first_root,first_id)
            self.assertTrue(entered.wait(10))
            second = pool.submit(producer.generate,{'p':2},second_root,second_id)
            with self.assertRaisesRegex(PrivateNetlistError,'CDF callback failed'):
                first.result(timeout=2)
            with self.assertRaisesRegex(PrivateNetlistError,'earlier generation failure'):
                second.result(timeout=2)
        self.assertEqual(capture.call_count,1)

    def test_optimizer_routes_maestro_through_one_producer_and_closes_it(self):
        from crescendo_engine.netlist_optimizer import PrivateNetlistOptimizer
        from tests import test_netlist_optimizer as optimizer_fixtures
        from crescendo_engine import runner
        prepared,config,spec = optimizer_fixtures.fixtures(self.root)
        spec['execution_backend'] = 'maestro_netlist'
        producer,context = mock.Mock(),mock.Mock()
        lifecycle = []
        context.unsafe.is_set.return_value = False
        context.evaluate.return_value = {'completion':{},'parsed':optimizer_fixtures.parsed(1.0),
            'native_job_state':{'native_completion_proven':True},'task_results':[]}
        context.close.return_value = {'cleanup_proven':True}
        producer.close.return_value = {'cleanup_proven':True}
        def generated(values,root,identifier,**kwargs):
            os.makedirs(root)
            return {'root':root,'candidate_id':identifier,'values':values,'workspace':{},'source_identity':{},
                'netlist_generation':{'parameter_application':{}},'tasks':[optimizer_fixtures.model_task()]}
        producer.generate.side_effect = generated
        def context_ready(*args,**kwargs):
            lifecycle.append('solver_reconciled')
            return context
        def native_ready(*args,**kwargs):
            self.assertTrue(kwargs['solver_cleanup_proven'])
            lifecycle.append('native_reconciled')
            return {'cleanup_proven':True}
        with mock.patch('crescendo_engine.netlist_optimizer.NetlistExecutionContext',side_effect=context_ready), \
             mock.patch('crescendo_engine.shared_maestro.SharedMaestroProducer',return_value=producer) as factory, \
             mock.patch('crescendo_engine.netlist_optimizer.reconcile_run_preparations',side_effect=native_ready), \
             mock.patch('crescendo_engine.netlist_optimizer.reconcile_preparation',return_value={'cleanup_proven':True}), \
             mock.patch('crescendo_engine.netlist_optimizer.verify_sources_unchanged',return_value={'ok':True}):
            adapter = PrivateNetlistOptimizer(self.root,spec,prepared,prepared['parameters'],config,runner._failure_metrics)
            rows = [adapter.evaluate(index,0,{'x':value}) for index,value in enumerate((0.3,0.6))]
            adapter.cancel()
            closed = adapter.close(cancel=True)
        self.assertEqual([row['status'] for row in rows],['completed','completed'])
        self.assertEqual(factory.call_count,1)
        self.assertEqual(lifecycle[0:2],['solver_reconciled','native_reconciled'])
        self.assertEqual(producer.generate.call_count,2)
        self.assertEqual(context.evaluate.call_count,2)
        producer.cancel.assert_called_once()
        producer.close.assert_called_once_with(cancel=True)
        self.assertTrue(closed['cleanup_proven'])

    def test_failed_context_constructor_close_retains_prior_shared_views(self):
        from crescendo_engine.netlist_optimizer import PrivateNetlistOptimizer
        from tests import test_netlist_optimizer as optimizer_fixtures
        from crescendo_engine import runner
        prepared,config,spec = optimizer_fixtures.fixtures(self.root)
        spec['execution_backend'] = 'maestro_netlist'
        # A registered previous epoch remains present when solver recovery
        # failed before the context could be assigned to the adapter.
        producer = SharedMaestroProducer(self.root,prepared,prepared['parameters'],config,spec)
        self.assertEqual(len(owner_records(self.root)),1)
        with mock.patch('crescendo_engine.netlist_optimizer.NetlistExecutionContext',
                        side_effect=RuntimeError('previous solver termination is unknown')), \
             mock.patch('crescendo_engine.netlist_optimizer.verify_sources_unchanged',return_value={'ok':True}), \
             mock.patch('crescendo_engine.same_library_views.cleanup_same_library_views') as cleanup:
            adapter = PrivateNetlistOptimizer(self.root,spec,prepared,prepared['parameters'],config,runner._failure_metrics)
            with self.assertRaisesRegex(RuntimeError,'termination is unknown'):
                adapter._prepare_once()
            self.assertIsNone(adapter.context)
            closed = adapter.close(cancel=True)
        self.assertFalse(closed['cleanup_proven'])
        self.assertFalse(closed['preparation_cleanup']['cleanup_proven'])
        self.assertEqual(closed['preparation_cleanup']['shared_maestro_epochs'][0]['state'],
                         'retained_until_solver_termination_proven')
        cleanup.assert_not_called()
        self.assertTrue(os.path.isfile(os.path.join(producer.state['owner']['root'],'owner.json')))

    def test_capture_baseline_bytes_keep_origin_selected_cwd_for_relative_models(self):
        from tests import test_setup_baseline as baseline_fixtures
        from crescendo_engine.private_netlist import _capture_setup_identity, _saved_corner_contract
        fixture = baseline_fixtures.SetupBaselineTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        baseline = fixture.freeze()
        private = os.path.join(self.root,'private_setup.sdb')
        with open(baseline['setup_path'],'rb') as source, open(private,'wb') as target:
            target.write(source.read())
        fixture.write(fixture.path,'original has changed and is not readable XML')
        fixture.config['_selected_setup_source'] = {'setup_path':fixture.path,'work_dir':fixture.root}
        workspace = {'root':self.root,'setup':fixture.prepared['setup'],'setup_sdb':private}
        with mock.patch('crescendo_engine.private_netlist._saved_corner_contract',wraps=_saved_corner_contract) as saved:
            detail = _capture_setup_identity(workspace,self.root,config=fixture.config)
        self.assertTrue(detail['ok'])
        self.assertEqual([call[0][0] for call in saved.call_args_list],[baseline['setup_path'],private])
        self.assertEqual(detail['model_path_context']['source_work_dir'],fixture.root)
        self.assertEqual(detail['model_path_context']['source'],'captured_selected_source_work_dir')
        self.assertEqual(detail['saved_source']['global_variables'][0]['value'],'3')

    def test_resume_epoch_is_fresh_but_same_frozen_setup_is_retained(self):
        first = SharedMaestroProducer(self.root,self.prepared,self.parameters,self.config,self.spec)
        first.close()
        second = SharedMaestroProducer(self.root,self.prepared,self.parameters,self.config,self.spec)
        self.assertNotEqual(first.state['owner']['workspace_root'],second.state['owner']['workspace_root'])
        self.assertIs(second.prepared,self.prepared)
        self.assertEqual(len(owner_records(self.root)),2)

    def test_candidate_reference_rejects_wrong_token_and_never_cleans_shared_views(self):
        producer = SharedMaestroProducer(self.root,self.prepared,self.parameters,self.config,self.spec)
        root,identifier = self.candidate(0)
        os.makedirs(os.path.join(root,'numeric'))
        write_reference(root,identifier,producer.state['owner'])
        with mock.patch('crescendo_engine.same_library_views.cleanup_same_library_views') as cleanup:
            self.assertTrue(reconcile_preparation(root)['cleanup_proven'])
        cleanup.assert_not_called()
        path = os.path.join(root,REFERENCE)
        with open(path) as stream: value = json.load(stream)
        value['token'] = 'a'*32
        atomic_write_json(path,value)
        with mock.patch('crescendo_engine.legacy_guardian.quiesce_legacy_guardian_attempt') as quiesce:
            self.assertFalse(reconcile_preparation(root)['cleanup_proven'])
        quiesce.assert_not_called()

    def test_run_close_cleans_only_registered_suite_after_all_native_phases_stop(self):
        producer = SharedMaestroProducer(self.root,self.prepared,self.parameters,self.config,self.spec)
        owner = producer.state['owner']
        os.makedirs(owner['workspace_root'])
        atomic_write_json(os.path.join(owner['workspace_root'],'workspace.json'),{'root':owner['workspace_root']})
        root,identifier = self.candidate(0)
        os.makedirs(os.path.join(root,'numeric'))
        write_reference(root,identifier,owner)
        with mock.patch('crescendo_engine.same_library_views.cleanup_same_library_views') as cleanup:
            unproven = reconcile_run_preparations(self.root)
        self.assertFalse(unproven['cleanup_proven'])
        self.assertEqual(unproven['shared_maestro_epochs'][0]['state'],'retained_until_solver_termination_proven')
        cleanup.assert_not_called()
        with mock.patch('crescendo_engine.same_library_views.cleanup_same_library_views',return_value={'cleanup_proven':True}) as cleanup:
            result = reconcile_run_preparations(self.root,solver_cleanup_proven=True)
        self.assertTrue(result['cleanup_proven'])
        cleanup.assert_called_once_with({'root':owner['workspace_root']},process_cleanup_proven=True)
        with open(os.path.join(root,'numeric','ocean.stdout'),'w') as stream: stream.write('launch evidence without manifest')
        with mock.patch('crescendo_engine.same_library_views.cleanup_same_library_views') as cleanup:
            result = reconcile_run_preparations(self.root,solver_cleanup_proven=True)
        self.assertFalse(result['cleanup_proven'])
        cleanup.assert_not_called()

    def test_only_referenced_generated_inputs_copied_static_pdk_and_ahdl_kept(self):
        native = os.path.join(self.root,'shared','_scratch','netlist')
        target = os.path.join(self.root,'candidate','netlist')
        os.makedirs(native); os.makedirs(target)
        static = os.path.join(self.root,'pdk.va')
        generated = os.path.join(native,'generated.scs')
        stimulus = os.path.join(native,'stim.tbl')
        for path,text in ((static,'module static; endmodule\n'),(stimulus,'0 0\n1 1\n')):
            with open(path,'w') as stream: stream.write(text)
        with open(generated,'w') as stream: stream.write('ahdl_include "'+static+'"\nparameters x=1\n')
        task = {'cwd':target,'native_cwd':native,'include_resolution':{'include_dirs':[]}}
        text,files = _freeze_generated_dependencies('include "generated.scs"\nV0 (p 0) vsource type=pwl file="stim.tbl"\nahdl_include "'+static+'"\n',task,self.config,[os.path.dirname(native)])
        self.assertEqual(len(files),2)
        self.assertIn(static,text)
        self.assertEqual({row['source'] for row in files},{generated,stimulus})
        for path in (generated,stimulus):
            with open(path,'w') as stream: stream.write('next candidate overwrote this')
        for row in files:
            with open(row['path']) as stream:
                self.assertNotIn('next candidate',stream.read())
        self.assertNotIn('file="stim.tbl"',text)
        self.assertEqual(task['static_file_inputs'],[])

    def test_static_relative_file_is_shared_with_identity_guard_and_expression_deferred(self):
        native = os.path.join(self.root,'native'); target = os.path.join(self.root,'candidate')
        os.makedirs(native); os.makedirs(target)
        static = os.path.join(native,'stim.tbl')
        with open(static,'w') as stream: stream.write('0 0\n')
        task = {'cwd':target,'native_cwd':native}
        text,files = _freeze_generated_dependencies('V0 (p 0) vsource file="stim.tbl"\n',task,self.config,[os.path.join(self.root,'other_mutable')])
        self.assertEqual(files,[])
        self.assertIn(static,text)
        self.assertEqual(task['static_file_inputs'][0]['sha256'],self.digest(static))
        netlist = os.path.join(target,'input.scs')
        with open(netlist,'w') as stream: stream.write(text)
        task.update(task_id='t',test='main',corner='TT',point_id=1,argv=['/bin/true',netlist],netlist_path=netlist,psf_dir=os.path.join(target,'psf'),spectre_log=os.path.join(target,'spectre.out'),netlist_sha256=self.digest(netlist))
        manifest = {'schema_version':SCHEMA,'root':target,'candidate_id':'g000_c0000','qualification':{'ok':True},'tasks':[task]}
        manifest['manifest_sha256'] = _manifest_digest(manifest)
        self.assertTrue(validate_manifest(manifest))
        from crescendo_engine.netlist_execution import validate_candidate_manifest, NetlistExecutionError
        self.assertEqual(validate_candidate_manifest(manifest),target)
        with open(static,'w') as stream: stream.write('changed')
        with self.assertRaisesRegex(PrivateNetlistError,'Static native file input changed'):
            validate_manifest(manifest)
        with self.assertRaisesRegex(NetlistExecutionError,'Static native file input changed'):
            validate_candidate_manifest(manifest)
        text = 'V0 (p 0) vsource file=VAR("name")\n'
        frozen, files = _freeze_generated_dependencies(text, task, self.config, [native])
        self.assertEqual(frozen, text)
        self.assertEqual(files, [])
        self.assertEqual(task['native_file_inputs']['deferred_count'], 1)


if __name__ == '__main__':
    unittest.main()
