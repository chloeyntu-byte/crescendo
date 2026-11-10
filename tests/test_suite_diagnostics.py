"""Stable suite diagnostics are presentation metadata, not safety gates."""
import unittest

from crescendo_engine.diagnosis_catalog import (
    describe_check, diagnostic_code, format_section, resolve_diagnostic_code)


class SuiteDiagnosticsTests(unittest.TestCase):
    def test_suite_codes_are_stable_and_classified_from_producer_messages(self):
        cases = [
            ('LEASE', 'lease', 'CRESCENDO-SUITE-LEASE: held'),
            ('LOCK', 'lock', 'CRESCENDO-SUITE-LOCK: editor owns view'),
            ('SNAPSHOT', 'snapshot', 'CRESCENDO-SUITE-SNAPSHOT: unavailable'),
            ('RECOVERY', 'recovery', 'CRESCENDO-SUITE-RECOVERY: incomplete'),
            ('ROLLBACK', 'rollback', 'CRESCENDO-SUITE-ROLLBACK: failed'),
            ('PERMISSION', 'permission', 'Private view permissions could not be restored: permission denied'),
            ('RELEASE', 'release', 'Suite lease could not be released: operation failed'),
        ]
        for suffix, key, message in cases:
            with self.subTest(key=key):
                code = 'CRESCENDO-SUITE-' + suffix
                check = {'id': 'execution', 'status': 'WARN', 'message': message}
                result = describe_check(check)
                self.assertEqual(result['diagnostic_code'], code)
                self.assertEqual(resolve_diagnostic_code(code), 'suite.' + key)
                self.assertEqual(diagnostic_code('suite.' + key, 'WARN'), code)
                self.assertEqual(check['status'], 'WARN')
                self.assertEqual(check['message'], message)
                self.assertTrue(result['explanation'])
        self.assertIsNone(diagnostic_code('suite.lock', 'PASS'))
        self.assertEqual(diagnostic_code('native.history_lookup', 'FAIL'), 'CRESCENDO-4AC0515DE9')

    def test_permission_warning_with_nested_lock_keeps_permission_category(self):
        message = 'Private view permissions could not be restored: CRESCENDO-SUITE-LOCK: locked'
        self.assertEqual(describe_check({'id': 'termination', 'status': 'WARN', 'message': message})['diagnostic_code'],
                         'CRESCENDO-SUITE-PERMISSION')

    def test_failure_section_never_dumps_embedded_prepared_snapshot(self):
        sentinel = 'FULL_SUITE_PREPARED_SENTINEL'
        check = {'id': 'suite.rollback', 'status': 'FAIL', 'message': 'CRESCENDO-SUITE-ROLLBACK: failed',
                 'details': {'error': 'failed', 'setup_private_session': {'prepared': {'private': sentinel}},
                             'prepared_snapshot': {'private': sentinel}}}
        context = {'first_cause': {'failure_stage': 'cleanup', 'message': 'failed',
                                   'prepared': {'private': sentinel}},
                   'records': [{'observation': 'diagnostic.producer_failure',
                                'payload': {'prepared': {'private': sentinel}, 'error': 'failed'}}]}
        for compact in (False, True):
            report = format_section([check], context, compact=compact)
            self.assertNotIn(sentinel, report)
            self.assertIn('prepared snapshot omitted', report)
            self.assertIn('CRESCENDO-SUITE-ROLLBACK', report)
