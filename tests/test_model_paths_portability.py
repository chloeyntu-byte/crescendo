import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine.model_paths import ModelPathRemapError, resolve_relocated_reference


class PortableModelSearchTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = self.directory.name
        self.model = os.path.join(self.root, 'deep', 'first', 'device.scs')
        os.makedirs(os.path.dirname(self.model))
        with open(self.model, 'w') as stream:
            stream.write('parameters fixture=1\n')
        self.reference = '/previous-machine/vendor/device.scs'

    def test_complete_search_accepts_single_candidate(self):
        result = resolve_relocated_reference(self.reference, [self.root])
        self.assertEqual(result['path'], self.model)
        self.assertEqual(result['mode'], 'profile_search')
        self.assertFalse(result['search_limited'])

    def test_incomplete_search_rejects_partial_single_candidate(self):
        walked = [
            (os.path.dirname(self.model), [], ['device.scs']),
            (self.root, ['unvisited-%d' % index for index in range(30001)], []),
        ]
        with mock.patch('crescendo_engine.model_paths.os.walk', return_value=iter(walked)):
            with self.assertRaisesRegex(ModelPathRemapError, 'incomplete.*model_roots.*model_path_remaps'):
                resolve_relocated_reference(self.reference, [self.root])

    def test_incomplete_search_distinguishes_unknown_from_missing(self):
        walked = [(self.root, ['unvisited-%d' % index for index in range(30001)], [])]
        with mock.patch('crescendo_engine.model_paths.os.walk', return_value=iter(walked)):
            with self.assertRaisesRegex(ModelPathRemapError, 'search is incomplete'):
                resolve_relocated_reference(self.reference, [self.root])

    def test_explicit_remap_does_not_need_broad_basename_search(self):
        with mock.patch('crescendo_engine.model_paths._bounded_basename_search') as search:
            result = resolve_relocated_reference(
                self.reference, [self.root],
                remaps=[('/previous-machine/vendor', os.path.dirname(self.model))],
            )
        search.assert_not_called()
        self.assertEqual(result['path'], self.model)
        self.assertEqual(result['mode'], 'explicit_remap')


if __name__ == '__main__':
    unittest.main()
