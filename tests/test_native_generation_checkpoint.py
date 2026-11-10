import unittest
from unittest import mock

from crescendo_engine.native_maestro_parameters import GENERATION_PROTOCOL
from crescendo_engine.native_qualification import NATIVE_QUALIFICATION_REVISION
from crescendo_engine.runner import validate_checkpoint


class NativeGenerationCheckpointTests(unittest.TestCase):
    def test_old_completed_cli_cache_cannot_be_resumed_as_native_parameters(self):
        for backend in ('maestro_netlist', 'private_netlist'):
            checkpoint = {'spec': {'evaluator': 'real', 'execution_backend': backend}}
            with self.subTest(backend=backend), mock.patch(
                    'crescendo_engine.runner.validate_checkpoint_integrity', return_value=[{'status': 'completed', 'values': {'p': 5}}]):
                with self.assertRaisesRegex(ValueError, 'older netlist-generation protocol'):
                    validate_checkpoint(checkpoint, {})

    def test_new_protocol_reaches_existing_source_and_tool_identity_checks(self):
        checkpoint = {'spec': {'evaluator': 'real', 'execution_backend': 'maestro_netlist',
                              'netlist_generation_protocol': GENERATION_PROTOCOL,
                              'native_qualification_revision': NATIVE_QUALIFICATION_REVISION}}
        with mock.patch('crescendo_engine.runner.validate_checkpoint_integrity', return_value=[]):
            with self.assertRaisesRegex(ValueError, 'source design identity'):
                validate_checkpoint(checkpoint, {})

    def test_old_mock_checkpoint_reader_keeps_its_compatibility(self):
        with mock.patch('crescendo_engine.runner.validate_checkpoint_integrity', return_value=[]):
            self.assertEqual(validate_checkpoint({'spec': {'evaluator': 'mock'}}, {}), [])


if __name__ == '__main__':
    unittest.main()
