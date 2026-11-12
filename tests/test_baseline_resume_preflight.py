"""Baseline Resume reaches the common preflight without bypassing integrity."""
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine.runner import _checkpoint_checksum, validate_checkpoint_integrity
from crescendo_engine.server import OptimizerService
from tests.test_resume_preflight_hardening import _mock_checkpoint


class BaselineResumePreflightTests(unittest.TestCase):
    def checkpoint(self, root):
        checkpoint = _mock_checkpoint(root)
        checkpoint['spec'].update(evaluator='real', execution_backend='maestro_baseline',
                                  execution_protocol='crescendo.maestro_baseline.v1')
        checkpoint['manifest']['execution_protocol'] = 'crescendo.maestro_baseline.v1'
        checkpoint['payload_sha256'] = _checkpoint_checksum(checkpoint)
        validate_checkpoint_integrity(checkpoint)
        return checkpoint

    def test_valid_baseline_reaches_config_before_lease_cleanup_or_launch(self):
        with tempfile.TemporaryDirectory() as root:
            checkpoint = self.checkpoint(root)
            service = OptimizerService({'runtime': {'run_root': root}})
            before = os.listdir(root)
            with mock.patch.object(service, '_freeze_run_config', side_effect=ValueError('preflight sentinel')) as config, \
                    mock.patch.object(service, '_cleanup_checkpoint_origin') as cleanup, \
                    mock.patch.object(service, '_launch') as launch, \
                    mock.patch('crescendo_engine.server.ContinuationLease') as lease, \
                    mock.patch('subprocess.Popen') as spawn:
                with self.assertRaisesRegex(ValueError, 'preflight sentinel'):
                    service.resume(checkpoint)
            config.assert_called_once()
            for boundary in (cleanup, launch, lease, spawn):
                boundary.assert_not_called()
            self.assertEqual(os.listdir(root), before)
            self.assertFalse(service.threads)
            self.assertFalse(service.setup_private_sessions)

    def test_integrity_failure_still_precedes_unsupported_backend_message(self):
        with tempfile.TemporaryDirectory() as root:
            checkpoint = self.checkpoint(root)
            checkpoint['payload_sha256'] = '0' * 64
            service = OptimizerService({'runtime': {'run_root': root}})
            with mock.patch.object(service, '_cleanup_checkpoint_origin') as cleanup, \
                    mock.patch.object(service, '_launch') as launch:
                with self.assertRaisesRegex(ValueError, 'payload checksum is invalid'):
                    service.resume(checkpoint)
            cleanup.assert_not_called()
            launch.assert_not_called()


if __name__ == '__main__':
    unittest.main()
