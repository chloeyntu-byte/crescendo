import hashlib
import os
import tempfile
import threading
import time
import unittest
from unittest import mock

from crescendo_engine.run_data_store import RunDataStore, RunDataReader, RunDataError, dataset_from_reference


class RunDataStampReconciliationTest(unittest.TestCase):
    def read_bytes(self, path):
        with open(path, 'rb') as stream:
            return stream.read()

    def make(self, root):
        writer = RunDataStore(os.path.join(root, 'optimization.crescendo'), {'spec': {}})
        return writer, writer.reference()

    def drift(self, path):
        value = os.stat(path)
        os.utime(path, ns=(value.st_atime_ns, value.st_mtime_ns + 1000000000))

    def invoke(self, writer, reference, action):
        if action == 'append':
            return writer.append_event('point', {'point_id': 1})
        if action == 'reference':
            return writer.reference()
        return dataset_from_reference(reference)

    def test_metadata_only_change_recovers_all_entrypoints(self):
        for action in ('append', 'reference', 'cached_reference'):
            with self.subTest(action=action), tempfile.TemporaryDirectory() as root:
                writer, reference = self.make(root)
                try:
                    before = self.read_bytes(writer.path)
                    self.drift(writer.path)
                    self.invoke(writer, reference, action)
                    self.assertEqual(writer.expected_stamp, writer._file_stamp())
                    self.assertEqual(dataset_from_reference(reference)['prefix_sha256'], hashlib.sha256(before).hexdigest())
                finally:
                    writer.close()

    def test_same_mode_chmod_preserves_cached_reference(self):
        with tempfile.TemporaryDirectory() as root:
            writer, reference = self.make(root)
            try:
                before = self.read_bytes(writer.path)
                time.sleep(0.02)
                os.chmod(writer.path, 0o600)
                self.assertNotEqual(writer.expected_stamp, writer._file_stamp())
                self.assertEqual(dataset_from_reference(reference)['revision'], reference['revision'])
                self.assertEqual(before, self.read_bytes(writer.path))
            finally:
                writer.close()

    def test_all_entrypoints_reject_actual_changes(self):
        for action in ('append', 'reference', 'cached_reference'):
            for mutation in ('same_length', 'truncate', 'append', 'replace', 'hardlink', 'symlink'):
                with self.subTest(action=action, mutation=mutation), tempfile.TemporaryDirectory() as root:
                    writer, reference = self.make(root)
                    try:
                        if mutation == 'same_length':
                            with open(writer.path, 'r+b') as stream:
                                stream.write(b'!')
                            self.drift(writer.path)
                        elif mutation == 'truncate':
                            with open(writer.path, 'r+b') as stream:
                                stream.truncate(1)
                        elif mutation == 'append':
                            with open(writer.path, 'ab') as stream:
                                stream.write(b'!')
                        elif mutation in ('replace', 'symlink'):
                            alternate = writer.path + '.other'
                            with open(alternate, 'wb') as stream:
                                stream.write(self.read_bytes(writer.path))
                            if mutation == 'replace':
                                os.replace(alternate, writer.path)
                            else:
                                os.unlink(writer.path)
                                os.symlink(alternate, writer.path)
                        else:
                            os.link(writer.path, writer.path + '.link')
                        with self.assertRaises(RunDataError) as raised:
                            self.invoke(writer, reference, action)
                        self.assertEqual(raised.exception.detail['stage'], action)
                        self.assertIn('reason', raised.exception.detail)
                    finally:
                        writer.close()

    def test_old_reference_checks_full_current_content_when_stamp_drifts(self):
        with tempfile.TemporaryDirectory() as root:
            writer, reference = self.make(root)
            try:
                writer.append_event('point', {'point_id': 1})
                with open(writer.path, 'r+b') as stream:
                    stream.seek(reference['bytes'])
                    stream.write(b'!')
                self.drift(writer.path)
                with self.assertRaisesRegex(RunDataError, 'checksum'):
                    dataset_from_reference(reference)
            finally:
                writer.close()

    def test_normal_append_does_not_hash_whole_file(self):
        with tempfile.TemporaryDirectory() as root:
            writer, _ = self.make(root)
            try:
                with mock.patch.object(writer, '_disk_prefix_sha256', side_effect=AssertionError('full hash')):
                    writer.append_event('point', {'point_id': 1})
            finally:
                writer.close()

    def test_stamp_change_during_verification_is_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            writer, reference = self.make(root)
            try:
                self.drift(writer.path)
                original = writer._disk_prefix_sha256
                def changed(stop):
                    digest = original(stop)
                    self.drift(writer.path)
                    return digest
                with mock.patch.object(writer, '_disk_prefix_sha256', side_effect=changed):
                    with self.assertRaisesRegex(RunDataError, 'stamp_changed_during_verification'):
                        dataset_from_reference(reference)
            finally:
                writer.close()

    def test_bad_reference_hash_and_same_stamp_tampering_still_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            writer, reference = self.make(root)
            try:
                altered = dict(reference, prefix_sha256='0' * 64)
                with self.assertRaisesRegex(RunDataError, 'checksum'):
                    dataset_from_reference(altered)
                stamp = writer.expected_stamp
                with open(writer.path, 'r+b') as stream:
                    stream.write(b'!')
                with mock.patch.object(writer, '_file_stamp', return_value=stamp):
                    with self.assertRaisesRegex(RunDataError, 'checksum'):
                        dataset_from_reference(reference)
                    with self.assertRaisesRegex(RunDataError, 'checksum'):
                        writer.reference()
            finally:
                writer.close()

    def test_concurrent_append_checkpoint_and_display_reader(self):
        with tempfile.TemporaryDirectory() as root:
            writer, reference = self.make(root)
            errors = []
            def append():
                try:
                    for index in range(30):
                        writer.append_event('point', {'index': index})
                        writer.reference()
                except Exception as error:
                    errors.append(error)
            thread = threading.Thread(target=append)
            thread.start()
            try:
                reader = RunDataReader(writer.path)
                for _ in range(30):
                    dataset_from_reference(reference)
                    reader.read()
            finally:
                thread.join(10)
                writer.close()
            self.assertFalse(thread.is_alive())
            self.assertEqual(errors, [])


if __name__ == '__main__':
    unittest.main()
