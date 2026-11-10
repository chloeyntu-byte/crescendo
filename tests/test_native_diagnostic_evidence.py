from __future__ import print_function
import json
import os
import tempfile
import time
import unittest
from unittest import mock

from crescendo_engine.native_diagnostic_evidence import collect_native_storage_evidence


class NativeEvidenceTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.candidate = os.path.join(self.tmp.name, 'g000_c0000')
        self.control = os.path.join(self.candidate, 'native_baseline')
        self.session_root = os.path.join(self.tmp.name, 'setup_private_sessions', 'c' * 32)
        self.workspace = os.path.join(self.session_root, 'workspace')
        self.storage = os.path.join(self.workspace, 'native_baseline_runs', 'a' * 32)
        os.makedirs(self.control)
        os.makedirs(self.storage)
        st = os.stat(self.storage)
        self.manifest = dict(schema_version='crescendo.native_storage_evidence.v1',
            request_root=self.candidate, candidate_id='g000_c0000', workspace_root=self.workspace,
            nonce='b' * 32, storage_root=self.storage, native_readback_verified=True,
            storage_identity=dict(dev=st.st_dev, ino=st.st_ino, uid=st.st_uid), created_epoch=time.time())
        self.state = dict(self.manifest, schema_version='crescendo.maestro_baseline.v1', started_epoch=self.manifest['created_epoch'])
        self.write_identity()
        owner = dict(schema_version='crescendo.setup_private_session.v1', session_id='c' * 32,
                     root=self.session_root, workspace_root=self.workspace, owner_uid=os.getuid())
        reference = dict(owner, capture_root=self.candidate, capture_kind='native_baseline')
        for path, data in ((os.path.join(self.candidate, 'setup_private_session_reference.json'), reference),
                           (os.path.join(self.session_root, 'session_owner.json'), owner),
                           (os.path.join(self.session_root, 'session_state.json'),
                            {'session_id': 'c' * 32, 'capture_roots': [self.candidate]})):
            with open(path, 'w') as f: json.dump(data, f)

    def write_identity(self):
        for name, obj in (('native_storage_evidence.json', self.manifest), ('native_baseline_state.json', self.state)):
            with open(os.path.join(self.control, name), 'w') as stream:
                json.dump(obj, stream)

    def collect(self, size=100000, files=64, deadline=None, cancelled=None):
        return collect_native_storage_evidence(self.candidate, size, files,
            time.monotonic()+5 if deadline is None else deadline, cancelled)

    def test_log_index_and_excluded_content(self):
        os.mkdir(os.path.join(self.storage, 'psf'))
        with open(os.path.join(self.storage, 'psf', 'hidden.log'), 'w') as f: f.write('secret')
        with open(os.path.join(self.storage, 'run.log'), 'w') as f: f.write('run event')
        with open(os.path.join(self.storage, 'input.scs'), 'w') as f: f.write('netlist secret')
        with open(os.path.join(self.storage, 'design.sdb'), 'w') as f: f.write('sdb secret')
        r = self.collect()
        self.assertEqual(r['status'], 'ok')
        self.assertEqual(r['files_read'], 6)
        self.assertEqual({x['kind'] for x in r['records']}, {'text_log', 'netlist_index', 'directory_index'})
        self.assertTrue(all(x['diagnostic_only'] for x in r['records']))
        self.assertNotIn('secret', str(r['records']))

    def test_mismatch_and_owner_rejected(self):
        self.state['nonce'] = 'c' * 32
        self.write_identity()
        self.assertEqual(self.collect()['status'], 'unavailable')
        self.state['nonce'] = self.manifest['nonce']
        self.manifest['storage_identity']['ino'] += 1
        self.write_identity()
        self.assertEqual(self.collect()['status'], 'unavailable')

    def test_psf_solver_text_is_collected_without_waveform_or_arbitrary_logs(self):
        psf = os.path.join(self.storage, 'psf')
        os.mkdir(psf)
        for name, content in (('spectre.out', 'ERROR (SFE-23): model unavailable'),
                              ('hidden.log', 'unrelated private data'), ('tran.tran', 'binary-waveform')):
            with open(os.path.join(psf, name), 'w') as stream:
                stream.write(content)
        result = self.collect()
        logs = [item for item in result['records'] if item['kind'] == 'text_log']
        self.assertEqual([item['path'] for item in logs], ['native_storage/psf/spectre.out'])
        self.assertIn('SFE-23', str(logs))
        self.assertNotIn('unrelated private data', str(result['records']))
        self.assertNotIn('binary-waveform', str(result['records']))

    def test_si_debug_defaults_do_not_hide_real_error(self):
        from crescendo_engine.environment_diagnostics import _windows
        content = '\n'.join([
            'si: simSetDef().... simCheckTermMismatchAction is initialized to "error" .',
            '"detailed" "error" "euler" "fatal" "file"',
            'Begin Incremental Netlisting',
            'ERROR (OSSHNL-109): cell cannot be opened',
            'End netlisting'])
        context = _windows(content, console_source='native_storage/netlist/si.foregnd.log')
        self.assertEqual(1, context['error_count'])
        self.assertEqual(4, context['first_error_line'])
        self.assertIn('OSSHNL-109', str(context['lines']))

    def test_limits_links_stale_and_binary(self):
        with open(os.path.join(self.storage, 'run.log'), 'w') as f: f.write('abcdefghij')
        os.utime(os.path.join(self.storage, 'run.log'), (1, 1))
        os.symlink('/etc/passwd', os.path.join(self.storage, 'outside.log'))
        base = self.collect()
        metadata = base['read_bytes'] - 10
        r = self.collect(size=metadata + 4)
        self.assertEqual(r['read_bytes'], metadata + 4)
        entry = next(x for x in r['records'] if x['kind'] == 'text_log')
        self.assertTrue(entry['older_than_manifest'])
        self.assertTrue(entry['truncated'])
        self.assertEqual(r['status'], 'partial')
        os.unlink(os.path.join(self.storage, 'run.log'))
        with open(os.path.join(self.storage, 'binary.log'), 'wb') as f: f.write(b'abc\x00binary')
        r = self.collect()
        self.assertFalse(any(x['kind'] == 'text_log' for x in r['records']))
        self.assertEqual(r['read_bytes'], metadata + 10)

    def test_deadline_and_unverified_readback(self):
        self.assertEqual(self.collect(deadline=time.monotonic()-1)['status'], 'unavailable')
        self.assertEqual(self.collect(cancelled=lambda: True)['status'], 'unavailable')
        self.manifest['native_readback_verified'] = False
        self.write_identity()
        r = self.collect()
        self.assertEqual(r['status'], 'partial')
        self.assertIn('native_storage_readback_unverified', [x['reason'] for x in r['omissions']])

    def test_metadata_budget_and_wrong_owner(self):
        r = self.collect(files=2)
        self.assertEqual(r['status'], 'unavailable')
        self.assertLessEqual(r['files_read'], 2)
        r = self.collect(size=100)
        self.assertEqual(r['status'], 'unavailable')
        self.assertLessEqual(r['read_bytes'], 100)
        owner_path = os.path.join(self.session_root, 'session_owner.json')
        with open(owner_path) as f: owner = json.load(f)
        owner['workspace_root'] = self.tmp.name
        with open(owner_path, 'w') as f: json.dump(owner, f)
        self.assertEqual(self.collect()['status'], 'unavailable')

    def test_large_registry_and_epoch_validation(self):
        registry_path = os.path.join(self.session_root, 'session_state.json')
        with open(registry_path, 'w') as f:
            json.dump({'session_id': 'c' * 32, 'capture_roots': [self.candidate],
                       'bindings': ['x' * 700 for _ in range(192)]}, f)
        with open(os.path.join(self.storage, 'run.log'), 'w') as f: f.write('ok')
        r = self.collect(size=300000)
        self.assertEqual(r['status'], 'ok')
        self.assertGreater(r['read_bytes'], 65536)
        self.state['started_epoch'] += 1
        self.write_identity()
        self.assertEqual(self.collect(size=300000)['status'], 'unavailable')
        self.state['started_epoch'] = float('nan')
        self.manifest['created_epoch'] = float('nan')
        self.write_identity()
        self.assertEqual(self.collect(size=300000)['status'], 'unavailable')

    def test_prioritizes_logs_and_samples_head_and_tail(self):
        with open(os.path.join(self.storage, 'a.log'), 'w') as f: f.write('other')
        with open(os.path.join(self.storage, 'run.log'), 'w') as f:
            f.write('HEAD' + 'x' * 200000 + 'TAIL')
        r = self.collect(size=300000, files=6)
        text = [x for x in r['records'] if x['kind'] == 'text_log']
        self.assertEqual(len(text), 1)
        self.assertTrue(text[0]['path'].endswith('/run.log'))
        self.assertEqual(len(text[0]['samples']), 2)
        self.assertTrue(text[0]['samples'][0]['text'].startswith('HEAD'))
        self.assertTrue(text[0]['samples'][1]['text'].endswith('TAIL'))
        self.assertLessEqual(text[0]['read_bytes'], 128 * 1024)
        self.assertEqual(text[0]['unscanned_ranges'][0]['end'], text[0]['samples'][1]['offset'])
        self.assertEqual(text[0]['hash_scope'], 'not_computed')

    def test_rejects_foreign_owner(self):
        file_path = os.path.join(self.storage, 'run.log')
        with open(file_path, 'w') as f: f.write('secret')
        real_stat = os.stat
        def altered(path, *args, **kwargs):
            actual = real_stat(path, *args, **kwargs)
            if path == 'run.log' and kwargs.get('dir_fd') is not None:
                fields = list(actual)
                fields[4] = actual.st_uid + 1
                return os.stat_result(fields)
            return actual
        with mock.patch('crescendo_engine.native_diagnostic_evidence.os.stat', side_effect=altered):
            r = self.collect()
        self.assertFalse(any(x['kind'] == 'text_log' for x in r['records']))
        self.assertIn('different_device_or_owner', [x['reason'] for x in r['omissions']])

    def test_storage_symlink_rejected(self):
        os.rmdir(self.storage)
        os.symlink(self.tmp.name, self.storage)
        self.assertEqual(self.collect()['status'], 'unavailable')


if __name__ == '__main__':
    unittest.main()
