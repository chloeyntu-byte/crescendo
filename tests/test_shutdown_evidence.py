import json
import os
import tempfile
import time
import unittest
from unittest import mock

from crescendo_engine.runner import _write_worker_shutdown
from crescendo_engine.server import OptimizerService
from crescendo_engine.shutdown_evidence import read_shutdown, shutdown_proven


class ShutdownEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.run = os.path.join(self.temp.name, 'run')
        os.mkdir(self.run)
        self.path = os.path.join(self.run, 'worker_shutdown.json')
        self.service = OptimizerService({'runtime': {'run_root': self.temp.name}})
        self.service.session_run_names.add('run')
        with open(os.path.join(self.run, 'status.json'), 'w') as stream:
            json.dump({'schema_version': 'crescendo.run_status.v1', 'run_id': 'run',
                       'run_root': self.run, 'state': 'completed'}, stream)

    def write(self, ok=True, suffix=b'}'):
        with open(self.path, 'wb') as stream:
            stream.write(b'{"schema_version":"crescendo.worker_shutdown.v1","workers":[{"worker_index":0,"ok":')
            stream.write(b'true' if ok else b'false')
            stream.write(b',"result":{"source_guard":{"before":"')
            stream.write(b'x' * (16 * 1024 * 1024 + 1))
            stream.write(b'"}}}]' + suffix)

    def test_large_legacy_success_and_failure(self):
        self.write()
        evidence = self.service.read('run', 'worker_shutdown.json')
        self.assertTrue(shutdown_proven(evidence))
        self.assertEqual(self.service._retry_session_run_cleanup(time.monotonic() + 5)['state'], 'no_pending_cleanup')
        self.service._require_session_runs_clean()
        self.write(False)
        self.assertFalse(shutdown_proven(self.service.read('run', 'worker_shutdown.json')))
        with self.assertRaisesRegex(RuntimeError, 'cleanup remains incomplete'):
            self.service._require_session_runs_clean()

    def test_truncated_duplicate_and_missing_proof_fail(self):
        self.write(suffix=b'')
        with self.assertRaises(ValueError):
            self.service.read('run', 'worker_shutdown.json')
        for raw in (b'{"schema_version":"crescendo.worker_shutdown.v1","workers":[{"ok":true,"ok":false}]}',
                    b'{"schema_version":"crescendo.worker_shutdown.v1","workers":[{}]}',
                    b'{"schema_version":"crescendo.worker_shutdown.v1","workers":[]}',
                    b'{"schema_version":"crescendo.worker_shutdown.v1","workers":[{"ok":true}],"final_cleanup":{}}',
                    b'{"schema_version":"crescendo.worker_shutdown.v1","workers":[{"ok":true}],"final_cleanup":null}',
                    b'{"schema_version":"crescendo.worker_shutdown.v1","workers":[{"ok":true}],"final_cleanup":[]}',
                    b'{"schema_version":"crescendo.worker_shutdown.v1","workers":[{"ok":true,"result":null}]}',
                    b'{"schema_version":"crescendo.worker_shutdown.v1","workers":[{"ok":true,"result":{"cleanup_proven":false}}]}'):
            with open(self.path, 'wb') as stream:
                stream.write(raw)
            try:
                proof = self.service.read('run', 'worker_shutdown.json')
                self.assertFalse(shutdown_proven(proof))
            except ValueError:
                pass

    def test_links_and_changed_file_are_rejected(self):
        self.write()
        link = self.path + '.link'
        os.link(self.path, link)
        with self.assertRaises(ValueError):
            self.service.read('run', 'worker_shutdown.json')
        os.unlink(link)
        os.rename(self.path, link)
        os.symlink(link, self.path)
        with self.assertRaises(ValueError):
            self.service.read('run', 'worker_shutdown.json')
        os.unlink(self.path)
        os.rename(link, self.path)
        original = os.fstat
        calls = [0]
        def changed(fd):
            info = original(fd)
            calls[0] += 1
            if calls[0] == 2:
                with open(self.path, 'ab') as stream:
                    stream.write(b' ')
                info = original(fd)
            return info
        descriptor = os.open(self.path, os.O_RDONLY)
        try:
            with mock.patch('crescendo_engine.shutdown_evidence.os.fstat', side_effect=changed):
                with self.assertRaises(RuntimeError):
                    read_shutdown(descriptor, 'shutdown')
        finally:
            os.close(descriptor)

    def test_atomic_replacement_during_read_is_rejected(self):
        self.write()
        replacement = self.path + '.replacement'
        with open(replacement, 'w') as stream:
            json.dump({'schema_version': 'crescendo.worker_shutdown.v1',
                       'workers': [{'ok': True}]}, stream)
        original = self.service._open_run_relative
        count = [0]
        def replace_before_reopen(*args, **kwargs):
            count[0] += 1
            if count[0] == 2:
                os.replace(replacement, self.path)
            return original(*args, **kwargs)
        with mock.patch.object(self.service, '_open_run_relative', side_effect=replace_before_reopen):
            with self.assertRaisesRegex(RuntimeError, 'changed while being read'):
                self.service.read('run', 'worker_shutdown.json')

    def test_unicode_surrogate_escapes_and_invalid_tokens(self):
        raw = b'{"schema_version":"crescendo.worker_shutdown.v1","workers":[{"worker_index":"\\u4e2d\\ud83d\\ude00","ok":true}]}'
        with open(self.path, 'wb') as stream:
            stream.write(raw)
        descriptor = os.open(self.path, os.O_RDONLY)
        try:
            proof = read_shutdown(descriptor, 'shutdown')
            self.assertEqual(proof['workers'][0]['worker_index'], json.loads(raw.decode('ascii'))['workers'][0]['worker_index'])
        finally:
            os.close(descriptor)
        for bad in (b'NaN', b'01', b'1e', b'"bad\\q"'):
            with open(self.path, 'wb') as stream:
                stream.write(b'{"workers":[' + bad + b']}')
            with self.assertRaises(ValueError):
                self.service.read('run', 'worker_shutdown.json')

    def test_compact_write_preserves_full_worker_details(self):
        private = os.path.join(self.run, 'private_netlist')
        os.mkdir(private)
        full = {'cleanup_proven': True, 'source_guard': {'before': 'x' * 100000}}
        with open(os.path.join(private, 'shutdown.json'), 'w') as stream:
            json.dump(full, stream)
        other = {'termination_proven': True, 'cleanup': {'detail': 'retained'}}
        _write_worker_shutdown(self.run, [{'worker_index': 'private_netlist', 'ok': True, 'result': full},
                                         {'worker_index': 1, 'ok': False, 'result': other}])
        with open(self.path) as stream:
            proof = json.load(stream)
        self.assertLess(os.path.getsize(self.path), 4096)
        self.assertEqual(proof['workers'][0]['details_path'], 'private_netlist/shutdown.json')
        self.assertFalse(proof['workers'][1]['ok'])
        with open(os.path.join(self.run, 'worker_shutdown_details.json')) as stream:
            self.assertEqual(json.load(stream)['workers'][0]['result'], other)


if __name__ == '__main__':
    unittest.main()
