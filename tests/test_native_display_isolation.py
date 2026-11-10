"""The caller's display is reused only by isolated native children."""
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine.cadence import child_environment
from crescendo_engine.native_diagnostics import skill_command_diagnostics


class NativeDisplayIsolationTests(unittest.TestCase):
    def child(self, environment, extra=None):
        with tempfile.TemporaryDirectory() as root, mock.patch.dict(os.environ, environment, clear=True):
            before = dict(os.environ)
            result = child_environment({'cadence': {}}, root, extra=extra)
            self.assertEqual(dict(os.environ), before)
            return result

    def test_invoking_display_reused_without_parent_mutation(self):
        result = self.child({'DISPLAY': ':435'})
        self.assertEqual(result['CDS_NOGRAPH_DISPLAY'], ':435')

    def test_explicit_nograph_display_wins(self):
        result = self.child({'DISPLAY': ':435', 'CDS_NOGRAPH_DISPLAY': ':519'})
        self.assertEqual(result['CDS_NOGRAPH_DISPLAY'], ':519')

    def test_explicit_empty_nograph_selection_is_retained(self):
        result = self.child({'DISPLAY': ':435', 'CDS_NOGRAPH_DISPLAY': ''})
        self.assertEqual(result['CDS_NOGRAPH_DISPLAY'], '')

    def test_extra_display_is_used_after_native_environment_restoration(self):
        result = self.child({'DISPLAY': ':0'}, {'DISPLAY': ':435'})
        self.assertEqual(result['CDS_NOGRAPH_DISPLAY'], ':435')

    def test_extra_explicit_nograph_display_wins(self):
        result = self.child({'DISPLAY': ':0'}, {'DISPLAY': ':435', 'CDS_NOGRAPH_DISPLAY': ':519'})
        self.assertEqual(result['CDS_NOGRAPH_DISPLAY'], ':519')

    def test_no_display_does_not_invent_display_or_launch_x(self):
        for environment in ({}, {'DISPLAY': ''}):
            with self.subTest(environment=environment):
                self.assertNotIn('CDS_NOGRAPH_DISPLAY', self.child(environment))


class NativeDisplayDiagnosticsTests(unittest.TestCase):
    def diagnose(self, text, returncode=134):
        return skill_command_diagnostics(
            {'returncode': returncode, 'stderr_tail': text}, '/nonexistent/owned/capture.il')

    def test_real_fatal_x_signature_is_explained_and_failure_remains_failure(self):
        result = self.diagnose('IO Error 2 (No such file or directory) during exit\n'
                               'Aborting due to fatal X IO error.\n')
        self.assertFalse(result['ok'])
        self.assertEqual(result['first_error']['kind'], 'native_display_error')
        self.assertIn('X display connection', result['first_error']['explanation'])
        context = result['native_error_contexts'][0]
        self.assertIn('IO Error 2', context['lines'][0]['text'])

    def test_fatal_display_is_failure_even_with_zero_exit_and_done_marker(self):
        result = self.diagnose('DONE\nAborting due to fatal X IO error.\n', returncode=0)
        self.assertTrue(result['process_ok'])
        self.assertFalse(result['ok'])
        self.assertTrue(result['native_error'])

    def test_memory_map_after_fatal_display_does_not_flood_error_context(self):
        result = self.diagnose('IO Error 11 on Display "eda:82"\n'
                               'Aborting due to fatal X IO error.\n'
                               + '\n'.join('memory mapping %s' % index for index in range(1000)))
        self.assertEqual(result['distinct_error_count'], 1)
        self.assertEqual(result['native_error_contexts'][0]['following_line_limit'], 2)
        self.assertLessEqual(len(result['native_error_contexts'][0]['lines']), 5)

    def test_display_warning_or_quoted_documentation_is_not_fatal_signature(self):
        result = self.diagnose('Warning: DISPLAY is unset\n'
                               'documentation says: Aborting due to fatal X IO error.\n', returncode=0)
        self.assertTrue(result['ok'])


if __name__ == '__main__':
    unittest.main()
