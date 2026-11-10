"""Precise capture payload cleanup keeps frozen native evidence unchanged."""
import os
import tempfile
import unittest
from unittest import mock

from test_private_netlist import PrivateNetlistCandidateTest, digest
from crescendo_engine.private_netlist import (compact_preparation_artifacts,
    validate_manifest, materialize_candidate, _manifest_digest)
from crescendo_engine.utils import atomic_write_json, atomic_write_text
from crescendo_engine.runner import run_optimization, _run_coordinated_resume


class PreparationArtifactCompactionTest(unittest.TestCase):
    def fixture(self, parent):
        manifest = PrivateNetlistCandidateTest().template(parent)
        root = manifest['root']
        for name in ('simulation', 'logs_root', '_scratch', 'bin'):
            path = os.path.join(root, 'workspace', name)
            os.makedirs(path)
            atomic_write_text(os.path.join(path, 'raw'), 'capture-only payload')
        atomic_write_json(os.path.join(root, 'private_netlist.json'), manifest)
        return manifest

    def proof(self, proven=True):
        return mock.patch('crescendo_engine.private_netlist.reconcile_preparation',
            return_value={'cleanup_proven': proven, 'phases': [{'state': 'dead'}],
                          'view_cleanup': [{'cleanup_proven': proven}]})

    def test_stopped_capture_payloads_clean_without_changing_qualified_template(self):
        with tempfile.TemporaryDirectory() as root:
            manifest = self.fixture(root)
            checksum = manifest['manifest_sha256']
            with self.proof():
                result = compact_preparation_artifacts(manifest)
            self.assertTrue(result['artifact_cleanup_complete'], result)
            self.assertEqual(len(result['removed']), 4)
            self.assertEqual(manifest['manifest_sha256'], checksum)
            self.assertTrue(validate_manifest(manifest))
            candidate = materialize_candidate(manifest, {'p': 1000}, os.path.join(root, 'candidate'), 'g000_c0000')
            self.assertEqual(candidate['template_sha256'], checksum)

    def test_recursive_include_into_capture_payload_is_retained(self):
        with tempfile.TemporaryDirectory() as root:
            manifest = self.fixture(root)
            keep = os.path.join(manifest['root'], 'workspace', 'simulation', 'raw')
            model = os.path.join(root, 'model.scs')
            atomic_write_text(model, 'include "%s"\n' % keep)
            task = manifest['tasks'][0]
            with open(task['netlist_path'], 'a') as stream:
                stream.write('include "%s"\n' % model)
            task['netlist_sha256'] = digest(task['netlist_path'])
            task['artifact_sha256'][task['netlist_path']] = task['netlist_sha256']
            task['template_files'][0]['sha256'] = task['netlist_sha256']
            manifest['manifest_sha256'] = _manifest_digest(manifest)
            atomic_write_json(os.path.join(manifest['root'], 'private_netlist.json'), manifest)
            with self.proof():
                result = compact_preparation_artifacts(manifest)
            self.assertTrue(result['artifact_cleanup_complete'], result)
            self.assertTrue(os.path.isfile(keep))
            self.assertEqual(len(result['retained']), 1)
            self.assertTrue(validate_manifest(manifest))

    def test_unproven_stop_or_durable_receipt_quota_keeps_raw_payload(self):
        for quota in (False, True):
            with self.subTest(quota=quota), tempfile.TemporaryDirectory() as root:
                manifest = self.fixture(root)
                with self.proof(proven=quota):
                    if quota:
                        with mock.patch('crescendo_engine.private_netlist.atomic_write_json',
                                        side_effect=OSError(122, 'Disk quota exceeded')):
                            result = compact_preparation_artifacts(manifest)
                    else:
                        result = compact_preparation_artifacts(manifest)
                self.assertFalse(result['artifact_cleanup_complete'], result)
                self.assertTrue(os.path.isfile(os.path.join(manifest['root'], 'workspace', 'simulation', 'raw')))

    def test_unresolved_include_and_unsafe_tree_never_delete_payload(self):
        for unresolved in (False, True):
            with self.subTest(unresolved=unresolved), tempfile.TemporaryDirectory() as root:
                manifest = self.fixture(root)
                if unresolved:
                    task = manifest['tasks'][0]
                    with open(task['netlist_path'], 'a') as stream:
                        stream.write('include "missing-native-relative.scs"\n')
                    task['netlist_sha256'] = digest(task['netlist_path'])
                    task['artifact_sha256'][task['netlist_path']] = task['netlist_sha256']
                    manifest['manifest_sha256'] = _manifest_digest(manifest)
                    atomic_write_json(os.path.join(manifest['root'], 'private_netlist.json'), manifest)
                else:
                    os.symlink(manifest['tasks'][0]['netlist_path'],
                               os.path.join(manifest['root'], 'workspace', 'simulation', 'link'))
                with self.proof():
                    result = compact_preparation_artifacts(manifest)
                self.assertTrue(os.path.isfile(os.path.join(manifest['root'], 'workspace', 'simulation', 'raw')))
                self.assertTrue(result.get('retained') if unresolved else result.get('error'), result)

    def test_real_cli_and_resume_reject_native_data_path_before_setup_or_lease(self):
        for resume in (False, True):
            with self.subTest(resume=resume), tempfile.TemporaryDirectory() as root:
                spec = {'evaluator': 'real'}
                checkpoint = {'schema_version': 'crescendo.checkpoint.v1', 'spec': spec, 'continuation_id': 'c'}
                with mock.patch('crescendo_engine.runner.checkpoint_continuation_id', return_value='c'), \
                        mock.patch('crescendo_engine.runner.startup_storage_preflight') as preflight, \
                        mock.patch('crescendo_engine.runner.prepare_setup') as prepare:
                    with self.assertRaisesRegex(ValueError, 'no spaces'):
                        if resume:
                            _run_coordinated_resume(spec, {'runtime': {'run_root': root}}, None,
                                os.path.join(root, 'native data $ root'), checkpoint,
                                mock.Mock(held=True, continuation_id='c'))
                        else:
                            run_optimization(spec, {'runtime': {'run_root': root}},
                                             run_root=os.path.join(root, 'native data $ root'))
                    preflight.assert_not_called()
                    prepare.assert_not_called()


if __name__ == '__main__':
    unittest.main()
