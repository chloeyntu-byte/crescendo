import hashlib
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest

from crescendo_engine.gui_service import GuiServiceDelegate, GuiServiceError, SUMMARY, prepared_summary
from tests.gui_service_worker import FixtureService


class GuiServiceProcessTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.config = {'runtime': {'run_root': self.folder.name},
                       'shutdown_file': os.path.join(self.folder.name, 'shutdown.json')}
        self.parent = FixtureService(self.config)
        self.transport = GuiServiceDelegate(self.parent, [sys.executable, '-B', '-m', 'tests.gui_service_worker'])

    def tearDown(self):
        if not self.transport.closed and self.transport.failure is None:
            if self.transport.process is not None:
                self.transport.call('exit_optimizer')
            else:
                self.transport.close()
        if self.transport.process is not None:
            self.transport.process.wait(timeout=15)
        self.folder.cleanup()

    def test_summary_is_explicit_and_does_not_relabel_original(self):
        identifier, original = self.parent.prepare(100)
        before = json.dumps(original, sort_keys=True)
        summary = prepared_summary(original)
        self.assertEqual(summary['schema_version'], SUMMARY)
        self.assertEqual(summary['source_schema_version'], 'crescendo.prepared.v1')
        self.assertEqual(len(summary['parameters']), 5)
        self.assertEqual(set(summary['hierarchy']['designs'][0]['hierarchy']['parameters'][0]),
                         {'parameter_id', 'hierarchy_path'})
        self.assertEqual(json.dumps(original, sort_keys=True), before)

    def test_gui_summary_cannot_be_persisted_relabelled_or_fingerprinted_as_execution_data(self):
        from crescendo_engine import prepared_store
        from crescendo_engine.prepared_freshness import _loaded_identity
        summary = prepared_summary(self.parent.prepare(20)[1])
        for operation in (lambda: prepared_store.reference(summary, root=self.folder.name),
                          lambda: prepared_store.compact_catalog(summary), lambda: _loaded_identity(summary)):
            with self.assertRaisesRegex(ValueError, 'not an execution'):
                operation()
        self.assertEqual(summary['schema_version'], SUMMARY)

    def test_worker_preserves_original_identity_and_only_sends_summary(self):
        identifier, summary = self.transport.call('prepare', 2000)
        self.assertEqual(summary['schema_version'], SUMMARY)
        self.assertEqual(len(summary['parameters']), 100)
        self.assertIs(self.parent.prepared[identifier], summary)
        original = FixtureService(self.config).prepare(2000)[1]
        expected = hashlib.sha256(json.dumps(original, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        identity = self.transport.call('start', identifier)
        self.assertEqual(identity['sha256'], expected)
        self.assertEqual(identity['original_count'], 2000)
        self.assertEqual(identity['original_schema'], 'crescendo.prepared.v1')
        self.assertEqual(self.transport.catalogue_action(identifier, 'identity')['sha256'], expected)

    def test_suite_inspection_does_not_repeat_the_legacy_parameter_graph(self):
        result = self.transport.call('load_private_suite', 'synthetic')
        rows = result['inspection']['hierarchy']['designs'][0]['hierarchy']['parameters']
        self.assertEqual(len(rows), len(result['prepared']['parameters']))
        self.assertTrue(all(set(row) == {'parameter_id', 'hierarchy_path'} for row in rows))
        self.assertEqual(result['inspection']['hierarchy']['schema_version'], 'crescendo.hierarchy_gui_summary.v1')
        original = FixtureService(self.config).prepare()[1]
        self.assertLess(len(json.dumps(result)), len(json.dumps(original)) / 10)
        expected = hashlib.sha256(json.dumps(original, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        self.assertEqual(self.transport.catalogue_action(result['prepared_id'], 'identity')['sha256'], expected)

    def test_child_gil_and_gc_do_not_starve_parent_heartbeat(self):
        identifier, summary = self.transport.call('prepare')
        self.assertLess(len(summary['parameters']), 1000)
        previous = sys.getswitchinterval()
        done, errors = threading.Event(), []
        try:
            sys.setswitchinterval(.5)
            def run():
                try:
                    self.transport.call('start', identifier)
                except Exception as error:
                    errors.append(error)
                finally:
                    done.set()
            thread = threading.Thread(target=run)
            gaps = []
            last = time.monotonic()
            thread.start()
            while not done.wait(.005):
                now = time.monotonic()
                gaps.append(now-last)
                last = now
            thread.join(15)
            self.assertEqual(errors, [])
            self.assertGreater(len(gaps), 10)
            self.assertLess(max(gaps), .2)
        finally:
            sys.setswitchinterval(previous)

    def test_diagnostic_bridge_retains_tk_window_and_frozen_parent_contributors(self):
        from unittest import mock
        from crescendo_engine.diagnostic_sections import register_diagnostic_section
        from crescendo_engine.gui_timing import GuiHeartbeat
        class Root(object):
            def after(self, ms, callback):
                return 'tick'
        now = [0.]
        observer = GuiHeartbeat(Root(), clock=lambda: now[0])
        self.parent.diagnostic_gui_observer = observer
        seen = []
        def callback(context):
            seen.append(os.getpid())
            with self.assertRaises(TypeError):
                context['run_ids']['changed'] = True
            return {'title': 'Parent report', 'body': 'fixture-only section', 'status': 'PASS'}
        with mock.patch.dict('crescendo_engine.diagnostic_sections._sections', {}, clear=True):
            register_diagnostic_section('parent.fixture', callback)
            self.transport.call('diagnostic_begin', 'd1')
            register_diagnostic_section('late', lambda context: self.fail('must freeze before diagnostic'))
            now[0] = .25
            observer.tick()
            report = self.transport.call('diagnostic_report', 'd1')
        self.assertEqual(seen, [os.getpid()])
        self.assertEqual(report['F4']['status'], 'PASS')
        window = report['F4']['gui']['windows']['diagnostic']
        self.assertEqual(window['max_pause_seconds'], .25)
        self.assertEqual(window['over_200ms_count'], 1)
        self.assertEqual(report['sections'][0]['body'], 'fixture-only section')
        self.assertEqual(len(report['sections']), 1)
        self.assertEqual(self.transport.diagnostic_registrations, {})

    def test_diagnostic_report_ready_and_export_keep_parent_sections_and_tk(self):
        from unittest import mock
        from crescendo_engine.diagnostic_sections import register_diagnostic_section
        from crescendo_engine.gui_timing import GuiHeartbeat
        class Root(object):
            def after(self, ms, callback):
                return 'tick'
        self.parent.diagnostic_gui_observer = GuiHeartbeat(Root(), clock=lambda: 0.)
        with mock.patch.dict('crescendo_engine.diagnostic_sections._sections', {}, clear=True):
            register_diagnostic_section('export.fixture', lambda context: {
                'title': 'Parent export', 'status': 'WARN', 'body': 'fixture report contribution'})
            payload = self.transport.call('diagnostic_end_to_end')
        self.assertTrue(payload['report_ready'])
        self.assertEqual(payload['state'], 'completed')
        self.assertEqual(payload['focused_checks']['F4']['status'], 'PASS')
        self.assertEqual(payload['diagnostic_sections'][0]['name'], 'export.fixture')
        self.assertIn('fixture report contribution', payload['exported_report'])
        self.assertEqual(self.transport.diagnostic_registrations, {})

    def test_diagnostic_callbacks_do_not_block_cancellation_or_response_lane(self):
        from unittest import mock
        from crescendo_engine.diagnostic_sections import register_diagnostic_section
        entered, release = threading.Event(), threading.Event()
        def slow(context):
            entered.set()
            release.wait(10)
            return {'title': 'Released', 'body': '', 'status': 'PASS'}
        result, errors = [], []
        def report():
            try:
                result.append(self.transport.call('diagnostic_report', 'd2'))
            except Exception as error:
                errors.append(error)
        with mock.patch.dict('crescendo_engine.diagnostic_sections._sections', {}, clear=True):
            register_diagnostic_section('slow', slow)
            self.transport.call('diagnostic_begin', 'd2')
            thread = threading.Thread(target=report)
            thread.start()
            try:
                self.assertTrue(entered.wait(10))
                self.transport.call('stop_setup_preparation')
                self.assertTrue(self.transport.call('wait_for_cancel')['cancelled'])
            finally:
                release.set()
                thread.join(15)
        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(result[0]['F4']['status'], 'NOT_TESTED')
        self.assertEqual(result[0]['sections'][0]['status'], 'PASS')

    def test_control_is_concurrent_and_acknowledges_no_cleanup_proof(self):
        output = []
        thread = threading.Thread(target=lambda: output.append(self.transport.call('wait_for_cancel')))
        thread.start()
        until = time.monotonic() + 5
        while self.transport.process is None and time.monotonic() < until:
            time.sleep(.005)
        ack = self.transport.call('stop_setup_preparation')
        self.assertTrue(ack['delivery_pending'])
        self.assertFalse(ack['cleanup_confirmed'])
        thread.join(10)
        self.assertFalse(thread.is_alive())
        self.assertEqual(output, [{'cancelled': True}])

    def test_error_attributes_and_failure_evidence_survive_transport(self):
        identifier, _ = self.transport.call('prepare', 1000)
        with self.assertRaisesRegex(ValueError, 'synthetic commit failure') as raised:
            self.transport.call('fail')
        self.assertEqual(raised.exception.configuration_commit_state, {'phase': 'committed', 'revision': 'r1'})
        self.assertIn('synthetic commit failure', raised.exception.remote_traceback_tail)
        self.assertEqual(raised.exception.prepared['schema_version'], SUMMARY)
        self.assertEqual(len(raised.exception.prepared['parameters']), 50)
        self.assertEqual(self.transport.call('start', identifier)['original_count'], 1000)
        with self.assertRaisesRegex(ValueError, 'Unsupported'):
            self.transport.call('_private_method')

    def test_command_cancellation_preserves_type_cleanup_and_import_evidence(self):
        from crescendo_engine.cadence import CommandCancelled
        partial = {'document': {'settings': {'run': {'generations': 20}}},
                   'cancelled': True, 'warnings': ['retained working-copy resources']}
        for result in (None, {'termination_failed': False}, {'termination_failed': True}):
            with self.subTest(result=result):
                with self.assertRaises(CommandCancelled) as raised:
                    self.transport.call('cancel_configuration', partial, result)
                error = raised.exception
                self.assertEqual(str(error), 'synthetic configuration cancelled')
                self.assertEqual(error.result, result)
                self.assertEqual(error.cleanup_failed, bool((result or {}).get('termination_failed')))
                self.assertEqual(error.configuration_import, partial)
                self.assertIn('cancel_configuration', error.remote_traceback_tail)
        self.assertIsInstance(self.transport.call('contract'), dict)

    def test_parent_eof_closes_only_the_owned_worker_and_runs_cleanup(self):
        self.transport.call('contract')
        self.transport.close()
        self.assertEqual(self.transport.process.wait(timeout=15), 0)
        with open(self.config['shutdown_file']) as stream:
            self.assertEqual(json.load(stream)['state'], 'closed')

    def test_disconnect_cleanup_failure_retains_error_evidence(self):
        self.config['fail_cleanup'] = True
        self.transport.call('contract')
        self.transport.close()
        self.assertNotEqual(self.transport.process.wait(timeout=15), 0)
        with open(self.transport.stderr_path) as stream:
            self.assertIn('cleanup is unproven', stream.read())

    def test_failed_exit_keeps_transport_available_for_proven_retry(self):
        self.config['fail_cleanup'] = True
        with self.assertRaisesRegex(RuntimeError, 'cleanup unproven'):
            self.transport.call('exit_optimizer')
        self.assertFalse(self.transport.closed)
        self.assertTrue(self.transport.call('exit_optimizer')['cleanup_confirmed'])

    def test_serialization_failure_reports_error_without_stranding_the_task_lane(self):
        errors = []
        def invoke():
            try:
                self.transport.call('non_json_result')
            except Exception as error:
                errors.append(error)
        thread = threading.Thread(target=invoke)
        thread.start()
        thread.join(10)
        if thread.is_alive():
            self.transport.close()
            thread.join(10)
            self.fail('unserializable response stranded the request')
        self.assertIsInstance(errors[0], TypeError)
        self.assertIsInstance(self.transport.call('contract'), dict)
        with self.assertRaises(TypeError):
            self.transport.call('non_json_config')
        self.transport.call('repair_config')
        self.assertNotIn('invalid', self.transport.call('contract'))

    def test_worker_failure_is_reported_without_restart(self):
        with self.assertRaisesRegex(GuiServiceError, 'disconnected'):
            self.transport.call('crash')
        process = self.transport.process
        self.assertEqual(process.wait(timeout=15), 7)
        with self.assertRaises(GuiServiceError):
            self.transport.call('contract')
        self.assertIs(self.transport.process, process)

    def test_exit_is_idempotent_and_force_does_not_claim_cleanup(self):
        result = self.transport.call('force_exit_optimizer')
        self.assertFalse(result['cleanup_confirmed'])
        self.assertEqual(self.transport.call('exit_optimizer'), result)

    def test_headless_service_remains_inline_and_preloaded_service_is_not_transferred(self):
        from crescendo_engine.server import OptimizerService
        service = OptimizerService(self.config)
        self.assertNotIn('_gui_delegate', vars(service))
        service.prepared['owned'] = {'schema_version': 'crescendo.prepared.v1'}
        self.assertFalse(service.enable_gui_process())
        self.assertEqual(service.prepared['owned']['schema_version'], 'crescendo.prepared.v1')

    def test_startup_failure_retains_worker_error_evidence(self):
        self.config['fail_startup'] = True
        with self.assertRaises(GuiServiceError):
            self.transport.call('contract')
        self.transport.process.wait(timeout=15)
        with open(self.transport.stderr_path) as stream:
            self.assertIn('synthetic worker startup failure', stream.read())

    def test_validation_error_preserves_report_and_normalized_variables(self):
        from crescendo_engine.range_validation import VariableRangeValidationError
        with self.assertRaises(VariableRangeValidationError) as raised:
            self.transport.call('range_fail')
        self.assertEqual(raised.exception.report['issues'], [{'message': 'synthetic range failure'}])
        self.assertEqual(raised.exception.normalized_variables, [{'parameter_id': 'p1'}])

    def test_injected_contract_is_the_same_in_parent_and_worker(self):
        script = r"""
import tempfile,sys,hashlib,json
from crescendo_engine.runtime import initialize,ApplicationIdentity,ProtocolDialect,descriptor
initialize(identity=ApplicationIdentity(display_name='Legacy Test',env_prefix='LEGACY_TEST',schema_namespace='legacy_test',slug='legacy_test'),dialect=ProtocolDialect(procedure_prefix='lgc',symbol_prefix='lg_'))
from crescendo_engine.gui_service import GuiServiceDelegate
from tests.gui_service_worker import FixtureService
with tempfile.TemporaryDirectory() as root:
 parent=FixtureService({'runtime':{'run_root':root}})
 transport=GuiServiceDelegate(parent,[sys.executable,'-B','-m','tests.gui_service_worker'])
 try:
  assert transport.call('contract')==descriptor()['runtime']
  identifier,view=transport.call('prepare',40)
  assert view['schema_version']=='legacy_test.prepared_gui_summary.v1'
  original=FixtureService(parent.config).prepare(40)[1]
  actual=transport.call('start',identifier)
  assert actual['original_schema']=='legacy_test.prepared.v1'
  assert actual['sha256']==hashlib.sha256(json.dumps(original,sort_keys=True,separators=(',',':')).encode()).hexdigest()
 finally:
  transport.call('exit_optimizer');transport.process.wait(timeout=15)
"""
        environment = dict(os.environ)
        for key in ('CRESCENDO_ENGINE_RUNTIME', 'CRESCENDO_ENGINE_BOOTSTRAP'):
            environment.pop(key, None)
        result = subprocess.run([sys.executable, '-B', '-c', script], env=environment,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_fresh_native_service_keeps_object_identity_when_attached(self):
        from crescendo_engine.server import OptimizerService
        service = OptimizerService(self.config)
        original = id(service)
        self.assertTrue(service.enable_gui_process())
        self.assertEqual(id(service), original)
        self.assertIs(type(service), OptimizerService)
        service._activity_events = [{'revision': 99}]
        self.assertEqual(OptimizerService.activity_snapshot(service)['revision'], 0)
        self.assertTrue(service.exit_optimizer()['exiting'])
        service._gui_delegate.process.wait(timeout=15)


if __name__ == '__main__':
    unittest.main()
