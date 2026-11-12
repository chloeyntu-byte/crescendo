"""Native/NFS source artifacts must not invalidate both capture and next Start."""
import copy
import hashlib
import json
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine import private_netlist, prepared_freshness
from crescendo_engine.workspace import copy_ignore, tree_content_fingerprint, verify_sources_unchanged
from tests import test_source_change_diagnostics as source_fixtures

NFS_NAME = '.nfs000000000000a12300000042'


class NativeSourceArtifactTests(unittest.TestCase):
    def setUp(self):
        self.fixture = source_fixtures.PreparedSourceAttributionTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.controller = self.fixture.attach_owned_snapshots()
        self.workspace = self.controller.workspace
        self.prepared, self.config = self.fixture.prepared, self.fixture.config
        self.view = os.path.dirname(self.fixture.fixture.schematic)
        self.before = copy.deepcopy(self.workspace['source_snapshots'])

    def capture(self, callback):
        with tempfile.TemporaryDirectory() as root:
            with mock.patch.object(private_netlist, '_qualify_setup_capture'), \
                    mock.patch.object(private_netlist, '_capture_phase', side_effect=callback):
                return private_netlist.capture_unmodified_baseline(
                    self.workspace, [], self.config, os.path.join(root, 'baseline'))

    def write(self, name, content='unlinked lock still open'):
        path = os.path.join(self.view, name)
        with open(path, 'w', encoding='utf-8') as stream:
            stream.write(content)
        return path

    def test_nfs_file_during_capture_and_next_start_preserves_loaded_identity(self):
        expected = copy.deepcopy(self.prepared['prepared_freshness'])
        def native(*args, **kwargs):
            self.write(NFS_NAME)
            return {'tasks': []}
        self.assertTrue(self.capture(native)['source_guard']['ok'])
        self.assertTrue(prepared_freshness.require_prepared_freshness(self.prepared, self.config)['ok'])
        self.assertEqual(self.workspace['source_snapshots'], self.before)
        self.assertEqual(self.prepared['prepared_freshness'], expected)
        # Its disappearance after the last native reader exits is benign too.
        os.unlink(os.path.join(self.view, NFS_NAME))
        self.assertTrue(verify_sources_unchanged(self.workspace)['ok'])

    def test_true_oa_change_during_capture_blocks_both_and_keeps_file_hashes(self):
        with open(self.fixture.fixture.schematic, 'rb') as stream:
            old = stream.read()
        def native(*args, **kwargs):
            self.write(NFS_NAME)
            self.write('sch.oa', 'actual changed OA content')
            return {'tasks': []}
        with self.assertRaises(private_netlist.PrivateNetlistError) as failure:
            self.capture(native)
        first = failure.exception
        with self.assertRaises(ValueError) as failure:
            prepared_freshness.require_prepared_freshness(self.prepared, self.config)
        second = failure.exception
        for error in (first, second):
            guard = error.detail['source_guard']
            self.assertFalse(guard['ok'])
            files = [f for lib in guard['libraries'] for scope in lib['changes']['scopes']
                     for f in scope['file_changes']['files']]
            row = next(f for f in files if f['path'].endswith('/sch.oa'))
            self.assertEqual(row['state'], 'modified')
            self.assertEqual(row['before']['sha256'], hashlib.sha256(old).hexdigest())
            self.assertEqual(row['after']['sha256'], hashlib.sha256(b'actual changed OA content').hexdigest())
            self.assertNotIn(NFS_NAME, json.dumps(guard))
        self.assertFalse(first.detail['solver_execution_started'])
        self.assertFalse(second.detail['native_run_invoked'])
        self.assertEqual(self.workspace['source_snapshots'], self.before)

    def test_oa_metadata_and_other_native_files_remain_guarded(self):
        for name in ('sch.oa', 'data.dm', 'master.tag', 'netlist.oa', '.nfs-circuit.scs',
                     '.nfs' + 'a' * 23, '.nfs' + 'a' * 25, '.nfs' + 'A' * 24,
                     NFS_NAME + '.scs', NFS_NAME + '\n'):
            with self.subTest(name=name):
                before = tree_content_fingerprint(self.view)
                path = self.write(name, 'changed bytes')
                after = tree_content_fingerprint(self.view)
                self.assertNotEqual(before['fingerprint'], after['fingerprint'])
                self.assertNotIn(name, copy_ignore(self.view, [name]))
                os.unlink(path)

    def test_matching_directory_and_symlink_remain_guarded_and_copied(self):
        for kind in ('directory', 'symlink'):
            with self.subTest(kind=kind):
                before = tree_content_fingerprint(self.view)
                path = os.path.join(self.view, NFS_NAME)
                if kind == 'directory':
                    os.mkdir(path)
                else:
                    os.symlink(self.fixture.metadata, path)
                after = tree_content_fingerprint(self.view)
                self.assertNotEqual(before['fingerprint'], after['fingerprint'])
                self.assertNotIn(NFS_NAME, copy_ignore(self.view, [NFS_NAME]))
                os.rmdir(path) if kind == 'directory' else os.unlink(path)

    def test_only_implicit_regular_nfs_file_is_ignored_explicit_scope_still_hashes(self):
        before = tree_content_fingerprint(self.view)
        path = self.write(NFS_NAME)
        self.assertEqual(before['fingerprint'], tree_content_fingerprint(self.view)['fingerprint'])
        self.assertIn(NFS_NAME, copy_ignore(self.view, [NFS_NAME]))
        direct = tree_content_fingerprint(self.view, [NFS_NAME])
        self.write(NFS_NAME, 'explicit requested file changed')
        self.assertNotEqual(direct['fingerprint'], tree_content_fingerprint(self.view, [NFS_NAME])['fingerprint'])
        os.unlink(path)
        self.assertNotEqual(direct['fingerprint'], tree_content_fingerprint(self.view, [NFS_NAME])['fingerprint'])

    def test_frozen_baseline_failure_is_not_misreported_as_schematic_change(self):
        proof = {'ok': False, 'libraries': [], 'setup_baseline': {'ok': False, 'error': 'baseline digest differs'}}
        with mock.patch.object(private_netlist, 'verify_sources_unchanged', return_value=proof):
            with self.assertRaises(private_netlist.PrivateNetlistError) as failure:
                self.capture(lambda *args, **kwargs: {'tasks': []})
        error = failure.exception
        self.assertFalse(error.detail['source_guard']['setup_baseline']['ok'])
        self.assertIn('frozen Maestro reference changed', str(error))


if __name__ == '__main__':
    unittest.main()
