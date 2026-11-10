"""OA runtime cache files are excluded only when tied to a real OA file."""
import copy
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine import prepared_freshness, private_netlist
from crescendo_engine.workspace import copy_ignore, tree_content_fingerprint, verify_sources_unchanged
from tests import test_source_change_diagnostics as fixtures

CACHE = 'sch.oa.submit01.example.invalid.3903.oacache'


class OACacheTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.PreparedSourceAttributionTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.controller = self.fixture.attach_owned_snapshots()
        self.workspace = self.controller.workspace
        self.view = os.path.dirname(self.fixture.fixture.schematic)

    def write(self, name, data=b'cache'):
        path = os.path.join(self.view, name)
        with open(path, 'wb') as stream:
            stream.write(data)
        return path

    def test_cache_added_during_capture_and_removed_after_snapshot(self):
        before = copy.deepcopy(self.workspace['source_snapshots'])
        def capture(*args, **kwargs):
            self.write(CACHE, b'x' * 147036)
            return {'tasks': []}
        with tempfile.TemporaryDirectory() as root:
            with mock.patch.object(private_netlist, '_qualify_setup_capture'), \
                    mock.patch.object(private_netlist, '_capture_phase', side_effect=capture):
                proof = private_netlist.capture_unmodified_baseline(
                    self.workspace, [], self.fixture.config, os.path.join(root, 'baseline'))
        self.assertTrue(proof['source_guard']['ok'])
        self.assertTrue(prepared_freshness.require_prepared_freshness(
            self.fixture.prepared, self.fixture.config)['ok'])
        self.assertEqual(before, self.workspace['source_snapshots'])
        self.assertIn(CACHE, copy_ignore(self.view, [CACHE]))
        with_cache = tree_content_fingerprint(self.view)
        os.unlink(os.path.join(self.view, CACHE))
        self.assertEqual(with_cache['fingerprint'], tree_content_fingerprint(self.view)['fingerprint'])
        self.assertTrue(verify_sources_unchanged(self.workspace)['ok'])

    def test_cache_deletion_preserves_identity(self):
        self.write(CACHE, b'x' * 147036)
        with_cache = tree_content_fingerprint(self.view)
        os.unlink(os.path.join(self.view, CACHE))
        self.assertEqual(with_cache['fingerprint'], tree_content_fingerprint(self.view)['fingerprint'])

    def test_real_design_changes_still_block(self):
        for name in ('sch.oa', 'data.dm'):
            with self.subTest(name=name):
                path = self.fixture.fixture.schematic if name == 'sch.oa' else self.fixture.metadata
                with open(path, 'rb') as stream:
                    original = stream.read()
                self.write(CACHE)
                with open(path, 'wb') as stream:
                    stream.write(b'real change')
                self.assertFalse(verify_sources_unchanged(self.workspace)['ok'])
                with open(path, 'wb') as stream:
                    stream.write(original)
                os.unlink(os.path.join(self.view, CACHE))

    def test_similar_names_and_missing_or_nonregular_base_are_guarded(self):
        names = ('sch.oa.host.oacache', 'sch.oa.host.x.oacache',
                 'sch.oa..3903.oacache', 'sch.oa.host.3903.oacache.extra',
                 'sch.oa.host.3903.oacache\n', 'other.oa.host.3903.oacache',
                 'sch.oax.host.3903.oacache')
        for name in names:
            with self.subTest(name=name):
                before = tree_content_fingerprint(self.view)
                path = self.write(name)
                self.assertNotEqual(before['fingerprint'], tree_content_fingerprint(self.view)['fingerprint'])
                self.assertNotIn(name, copy_ignore(self.view, [name]))
                os.unlink(path)
        base = os.path.join(self.view, 'other.oa')
        for kind in ('directory', 'symlink'):
            with self.subTest(base=kind):
                os.mkdir(base) if kind == 'directory' else os.symlink(self.fixture.fixture.schematic, base)
                name = 'other.oa.host.3903.oacache'
                before = tree_content_fingerprint(self.view)
                self.write(name)
                self.assertNotEqual(before['fingerprint'], tree_content_fingerprint(self.view)['fingerprint'])
                self.assertNotIn(name, copy_ignore(self.view, [name]))
                os.unlink(os.path.join(self.view, name))
                os.rmdir(base) if kind == 'directory' else os.unlink(base)

    def test_matching_directory_symlink_and_explicit_scope_are_guarded(self):
        for kind in ('directory', 'symlink'):
            with self.subTest(kind=kind):
                before = tree_content_fingerprint(self.view)
                path = os.path.join(self.view, CACHE)
                os.mkdir(path) if kind == 'directory' else os.symlink(self.fixture.fixture.schematic, path)
                self.assertNotEqual(before['fingerprint'], tree_content_fingerprint(self.view)['fingerprint'])
                self.assertNotIn(CACHE, copy_ignore(self.view, [CACHE]))
                os.rmdir(path) if kind == 'directory' else os.unlink(path)
        self.write(CACHE)
        before = tree_content_fingerprint(self.view, [CACHE])
        self.write(CACHE, b'explicit change')
        self.assertNotEqual(before['fingerprint'], tree_content_fingerprint(self.view, [CACHE])['fingerprint'])
        os.unlink(os.path.join(self.view, CACHE))
        self.assertNotEqual(before['fingerprint'], tree_content_fingerprint(self.view, [CACHE])['fingerprint'])


if __name__ == '__main__':
    unittest.main()
