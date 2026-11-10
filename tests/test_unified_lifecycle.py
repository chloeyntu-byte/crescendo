"""Actual producer/common CLI integration with fake native capture and solver.

The shell child and ownership receipts are real. This does not qualify a vendor
simulator, PDK, Ocean reader or cluster; those are explicit fixture boundaries.
"""
import copy
import os
import sys
import tempfile
from pathlib import Path
import unittest
from unittest import mock

from crescendo_engine.environment_diagnostics import pipeline_qualification
from crescendo_engine.execution_commands import normalize_execution_commands
from crescendo_engine.netlist_execution import NetlistExecutionContext
from crescendo_engine.netlist_optimizer import PrivateNetlistOptimizer
from crescendo_engine.netlist_providers import prepare_maestro_netlists
from crescendo_engine.private_netlist import materialize_candidate, _manifest_digest
from crescendo_engine.simulators import execution_contract
from crescendo_engine.utils import atomic_write_text, sha256_file
from tests import test_netlist_providers as provider_fixtures
from tests.test_netlist_optimizer import fixtures


class UnifiedProducerLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='crescendo-unified-lifecycle-')
        self.addCleanup(self.temp.cleanup)
        self.root = self.temp.name
        self.provider_fixture = provider_fixtures.NumericMaestroProviderTest()
        self.prepared, self.parameters, self.config, self.spec, self.model = self.provider_fixture.fixtures(self.root)
        self.config['runtime']['max_parallel_jobs'] = 1
        self.spec.update(simulator='spectre', candidate_timeout_seconds=30)
        self.contexts = []
        self.addCleanup(self.close_contexts)
        binary = self.config['cadence']['spectre_bin']
        atomic_write_text(binary, '#!' + sys.executable + '\n' +
            'import os,sys\n'
            'args=sys.argv[1:]\n'
            'psf=args[args.index("-raw")+1]\n'
            'log=args[args.index("+log")+1]\n'
            'os.makedirs(psf,exist_ok=True)\n'
            'open(log,"w").write("spectre completes with 0 errors.\\n")\n'
            'open(os.path.join(psf,"logFile"),"w").write("fake solver result fixture\\n")\n')
        os.chmod(binary, 0o700)

    def close_contexts(self):
        for context in self.contexts:
            context.close(cancel=True)

    def execute(self, manifest, engine):
        spec = dict(self.spec, execution_backend=engine)
        context = NetlistExecutionContext(self.config, self.prepared, spec,
            owned_run_root=self.root,
            measurement_evaluator=lambda task, _config, _root, **kwargs: {
                'outputs': [dict(task['outputs'][0], value=0.75, error=None)]})
        self.contexts.append(context)
        result = context.evaluate(manifest)
        completion = dict(result['completion'], ok=True, netlist_generation=manifest['netlist_generation'])
        candidate = {'completion_evidence': completion, 'netlist_generation': manifest['netlist_generation']}
        qualification = pipeline_qualification(candidate, engine, 'spectre')
        return result, qualification

    def test_numeric_provider_manifest_and_actual_common_receipts_qualify_diagnostics(self):
        with mock.patch('crescendo_engine.netlist_providers.build_workspace', side_effect=self.provider_fixture.workspace), \
                mock.patch('crescendo_engine.netlist_providers._capture_phase', side_effect=self.provider_fixture.capture(self.model)), \
                mock.patch('crescendo_engine.netlist_providers.verify_sources_unchanged', return_value={'ok': True}):
            manifest = prepare_maestro_netlists(self.prepared, self.parameters, {'p': 2e-6}, self.config,
                os.path.join(self.root, 'numeric'), 'numeric_candidate', spec=self.spec)
        result, qualification = self.execute(manifest, 'maestro_netlist')
        self.assertTrue(qualification['ok'], qualification['problems'])
        point = result['completion']['execution_pipeline']['points'][0]
        self.assertEqual(point['netlist_generation']['mode'], 'maestro')
        self.assertTrue(point['terminal_receipt']['cleanup_proven'])
        self.assertEqual(point['terminal_receipt']['returncode'], 0)
        self.assertEqual(point['solver_command_plan']['argv'], manifest['tasks'][0]['argv'])
        self.assertEqual(point['submission_command_plan']['argv'][0], '/bin/sh')
        self.assertTrue(self.contexts[-1].close()['cleanup_proven'])

    def test_template_materializer_and_actual_common_receipts_qualify_diagnostics(self):
        capture = self.provider_fixture.capture(self.model)({}, self.parameters, {'p': 1}, {}, {}, self.config,
            os.path.join(self.root, 'template'), symbolic=False, verify_numeric_application=True)
        task = capture['tasks'][0]
        with open(task['netlist_path'], 'a', encoding='utf-8') as stream:
            stream.write('include "cr_candidate_parameters.scs"\nR1 (out 0) resistor r=CRESCENDO_NL_p\n')
        task['netlist_sha256'] = sha256_file(task['netlist_path'])
        task['template_files'][0]['sha256'] = task['netlist_sha256']
        task['artifact_sha256'] = {task['netlist_path']: task['netlist_sha256']}
        task['simulator_contract'] = execution_contract(self.config)
        commands = normalize_execution_commands(self.config, self.spec)
        task['execution_commands'] = commands
        template = {'schema_version': 'crescendo.private_netlist.v1', 'root': os.path.join(self.root, 'template'),
            'parameter_symbols': {'p': 'CRESCENDO_NL_p'}, 'tasks': [task], 'execution_commands': commands,
            'qualification': {'ok': True}, 'source_guard': {'ok': True},
            'workspace': {'root': os.path.join(self.root, 'private_template'), 'primary_library': 'PRIVATE'}}
        template['manifest_sha256'] = _manifest_digest(template)
        manifest = materialize_candidate(template, {'p': 2000}, os.path.join(self.root, 'template_candidate'), 'template_candidate')
        result, qualification = self.execute(manifest, 'private_netlist')
        self.assertTrue(qualification['ok'], qualification['problems'])
        self.assertEqual(result['completion']['execution_pipeline']['netlist_source'], 'template')
        self.assertTrue(manifest['netlist_generation']['private_identity'])

    def test_actual_local_guardian_qualifies_zombie_raw_receipt_and_remote_cannot_reuse_it(self):
        from crescendo_engine import environment_diagnostics as diagnostics
        binary = self.config['cadence']['spectre_bin']
        original = Path(binary).read_text(encoding='utf-8')
        lines = original.splitlines(True)
        lines.insert(1, 'import os,time\nif os.fork()==0: os._exit(0)\ntime.sleep(0.05)\n')
        atomic_write_text(binary, ''.join(lines))
        os.chmod(binary, 0o700)
        with mock.patch('crescendo_engine.netlist_providers.build_workspace', side_effect=self.provider_fixture.workspace), \
                mock.patch('crescendo_engine.netlist_providers._capture_phase', side_effect=self.provider_fixture.capture(self.model)), \
                mock.patch('crescendo_engine.netlist_providers.verify_sources_unchanged', return_value={'ok': True}):
            manifest = prepare_maestro_netlists(self.prepared, self.parameters, {'p': 2e-6}, self.config,
                os.path.join(self.root, 'numeric_zombie'), 'numeric_zombie', spec=self.spec)
        result, qualification = self.execute(manifest, 'maestro_netlist')
        self.assertTrue(qualification['ok'], qualification['problems'])
        pipeline = result['completion']['execution_pipeline']
        point = pipeline['points'][0]
        self.assertIs(point['raw_terminal_receipt']['cleanup_proven'], False)
        self.assertIs(point['terminal_receipt']['cleanup_proven'], True)
        self.assertEqual(point['terminal_receipt']['cleanup_basis'], 'owned_local_guardian')
        self.assertTrue(diagnostics._point_termination_qualification(point)['ok'])
        for mutation in ('remote', 'token', 'argv', 'cwd', 'tick', 'manifest_hash', 'remaining', 'missing_guardian', 'basis'):
            altered = copy.deepcopy(point)
            evidence = altered['termination_evidence']
            if mutation == 'remote':
                altered['submission_command_plan']['remote'] = True;evidence['remote'] = True
            elif mutation == 'token': evidence['receipt_token'] = 'f' * 32
            elif mutation == 'argv': evidence['guardian']['argv'] = ['/bin/sh', '/foreign/execute.sh']
            elif mutation == 'cwd': evidence['guardian']['cwd'] = '/foreign'
            elif mutation == 'tick': evidence['guardian']['pid_start_ticks'] = True
            elif mutation == 'manifest_hash': evidence['guardian']['manifest_sha256'] = 'missing'
            elif mutation == 'remaining': evidence['guardian']['termination_proof']['remaining_group_pids'] = [123]
            elif mutation == 'missing_guardian': evidence.pop('guardian')
            elif mutation == 'basis': evidence['source'] = 'local_submit_rc_zero'
            with self.subTest(mutation=mutation):
                self.assertFalse(diagnostics._point_termination_qualification(altered)['ok'])
        summary = diagnostics.pipeline_summary(pipeline)['points'][0]
        self.assertFalse(summary['raw_terminal_receipt']['cleanup_proven'])
        self.assertTrue(summary['terminal_receipt']['cleanup_proven'])
        self.assertEqual(summary['termination_evidence']['source'], 'owned_local_guardian')
        self.assertNotIn(point['raw_terminal_receipt']['token'], repr(summary))

    def test_adapter_preserves_requested_and_applied_values_without_claiming_native_oa_export(self):
        from crescendo_engine.optimized_schematic import build_export_evidence, OptimizedSchematicError
        parameter = self.parameters[0]
        parameter.update(hierarchy_path='/M0', owner_library='Design', owner_cell='Core', owner_view='schematic',
            owner_occurrence_path='/', design_library='Design', design_cell='TB', design_view='config')
        spec = dict(self.spec, execution_backend='maestro_netlist', population=1)
        spec['objectives'] = [dict(spec['objectives'][0], direction='maximize')]
        capture_fixture = self.provider_fixture.capture(self.model)
        def capture(*args, **kwargs):
            result = capture_fixture(*args, **kwargs)
            result['applied_values'] = {'p': 1.9e-6}
            result['parameter_application'] = {'ok': True, 'parameters': [{
                'parameter_id': 'p', 'property': 'w', 'hierarchy_path': '/M0',
                'persisted_schematic_value': '1.9u', 'persisted_cdf_value': '1.9u',
                'requested': 2e-6}], 'export_snapshot': {'instances': []}}
            return result
        def context_factory(*args, **kwargs):
            context = NetlistExecutionContext(*args, measurement_evaluator=lambda task, _config, _root, **unused: {
                'outputs': [dict(task['outputs'][0], value=0.75, error=None)]}, **kwargs)
            self.contexts.append(context)
            return context
        adapter = PrivateNetlistOptimizer(self.root, spec, self.prepared, self.parameters, self.config,
            lambda *_args: {})
        with mock.patch('crescendo_engine.netlist_optimizer.NetlistExecutionContext', side_effect=context_factory), \
                mock.patch('crescendo_engine.netlist_providers.build_workspace', side_effect=self.provider_fixture.workspace), \
                mock.patch('crescendo_engine.netlist_providers._capture_phase', side_effect=capture), \
                mock.patch('crescendo_engine.netlist_providers.verify_sources_unchanged', return_value={'ok': True}), \
                mock.patch('crescendo_engine.netlist_optimizer.reconcile_preparation', return_value={'cleanup_proven': True}):
            candidate = adapter.evaluate(0, 0, {'p': 2e-6})
        self.assertEqual(candidate['status'], 'completed', candidate.get('error'))
        self.assertEqual(candidate['values'], {'p': 2e-6})
        self.assertEqual(candidate['completion_evidence']['parameters'], {'p': 1.9e-6})
        self.assertTrue(pipeline_qualification(candidate, 'maestro_netlist', 'spectre')['ok'])
        with self.assertRaisesRegex(OptimizedSchematicError, 'no verified OA/CDF backannotation snapshot'):
            build_export_evidence(candidate, self.parameters)

    def test_unproven_common_stop_does_not_reclaim_native_preparation_views(self):
        prepared, config, spec = fixtures(self.root)
        spec['execution_backend'] = 'maestro_netlist'
        adapter = PrivateNetlistOptimizer(self.root, spec, prepared, prepared['parameters'], config,
            lambda *_args: {})
        adapter.context = mock.Mock()
        adapter.context.close.return_value = {'cleanup_proven': False, 'unsafe': True,
            'retained': [{'identity': 'owned_pending_compute_task'}]}
        with mock.patch.object(adapter, 'verify_sources', return_value={'ok': True}), \
                mock.patch('crescendo_engine.netlist_optimizer.reconcile_run_preparations',
                    return_value={'cleanup_proven': True}) as preparation_cleanup:
            result = adapter.close(cancel=True)
        self.assertFalse(result['cleanup_proven'])
        preparation_cleanup.assert_not_called()
