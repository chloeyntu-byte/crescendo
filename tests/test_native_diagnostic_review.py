"""Integration of native diagnostic binding with a real private suite registry.

Native binding/readback are deliberately synthetic; this is not a Cadence test.
"""
import copy
import os
import time
import unittest
import uuid

from crescendo_engine import setup_private_session as session
from crescendo_engine.environment_diagnostics import collect_run_diagnostics
from crescendo_engine.native_baseline import _NativeSession, _record_storage_evidence
from crescendo_engine.native_diagnostic_evidence import collect_native_storage_evidence
from crescendo_engine.storage_policy import native_workspace_storage
from tests import test_setup_private_session as session_fixture


class NativeDiagnosticRegistryIntegrationTest(unittest.TestCase):
    def setUp(self):
        fixture = session_fixture.SetupPrivateSessionTests(methodName='runTest')
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        self.fixture = fixture

    def test_real_registry_producer_and_collector_chain(self):
        fixture = self.fixture
        run_root = os.path.join(fixture.config['runtime']['run_root'], 'diagnostic_run')
        candidate = os.path.join(run_root, 'candidates', 'g000_c0000')
        os.makedirs(candidate)
        session.write_session_reference(candidate, fixture.reference, kind='native_baseline')
        self.assertEqual(session.preparation_session_reference(candidate)['session_id'],
                         fixture.reference['session_id'])
        workspace = {'root': fixture.reference['workspace_root']}
        storage_root = os.path.join(workspace['root'], 'native_baseline_runs', uuid.uuid4().hex)
        os.makedirs(storage_root)
        workspace['native_storage'] = native_workspace_storage(storage_root)
        driver = _NativeSession(candidate, workspace, fixture.config, 'g000_c0000')
        with open(fixture.fixture.path, 'rb') as stream:
            source_before = stream.read()
        with open(fixture.fixture.schematic, 'rb') as stream:
            schematic_before = stream.read()
        with open(os.path.join(storage_root, 'run.log'), 'w') as stream:
            stream.write('Job ID: 12345\n*Error* synthetic solver failure\n')
        before = copy.deepcopy(driver.state)
        _record_storage_evidence(driver)
        self.assertNotIn('diagnostic_evidence_error', driver.state)
        result = collect_native_storage_evidence(candidate, 1024 * 1024, 64, time.monotonic() + 10)
        self.assertEqual(result['status'], 'partial', result)
        self.assertIn('native_storage_readback_unverified', [x['reason'] for x in result['omissions']])
        self.assertTrue(any('synthetic solver failure' in x.get('text', '') for x in result['records']))
        # This flag is synthetic; production sets it only after a native readback.
        workspace['native_storage']['configured_roots_verified'] = True
        _record_storage_evidence(driver)
        result = collect_native_storage_evidence(candidate, 1024 * 1024, 64, time.monotonic() + 10)
        self.assertEqual(result['status'], 'ok', result)
        self.assertTrue(result['manifest']['native_readback_verified'])
        records = collect_run_diagnostics(run_root, 'maestro_baseline', config=fixture.config)
        summaries = [x['payload'] for x in records if x['observation'] == 'diagnostic.native_storage_collection']
        logs = [x['payload'] for x in records if x['observation'] == 'diagnostic.native_storage_evidence']
        self.assertEqual(len(summaries), 1, records)
        self.assertEqual(summaries[0]['status'], 'ok')
        self.assertIn('synthetic solver failure', str(logs))
        self.assertTrue(all(x['diagnostic_only'] for x in logs))
        self.assertEqual(driver.state['native_stop'], before['native_stop'])
        self.assertFalse(driver.state['native_stop']['remote_stop_proven'])
        self.assertFalse(driver.state['cleanup_proven'])
        self.assertFalse(driver.state['native_session_closed'])
        with open(fixture.fixture.path, 'rb') as stream:
            self.assertEqual(stream.read(), source_before)
        with open(fixture.fixture.schematic, 'rb') as stream:
            self.assertEqual(stream.read(), schematic_before)


if __name__ == '__main__':
    unittest.main()
