"""Diagnostic detail controls are Pro-only and freeze into the diagnostic spec."""
import copy
import os
import unittest
from unittest import mock

from tests import test_gui_environment_test as fixtures
from crescendo_engine import gui
from crescendo import app


@unittest.skipUnless(os.environ.get('DISPLAY'), 'DISPLAY required')
class DiagnosticLogGuiTests(unittest.TestCase):
    close = fixtures.EnvironmentTestTkTests.close
    wait = fixtures.EnvironmentTestTkTests.wait

    def setUp(self):
        with mock.patch.object(gui, 'OptimizerGui', app.CrescendoGui):
            fixtures.EnvironmentTestTkTests.setUp(self)

    def test_sidecar_warning_visible_in_diagnostic_card_and_preserves_verdict(self):
        payload = copy.deepcopy(self.service.payload)
        payload.update(state='completed', report_ready=True)
        self.app._render_environment_test_progress(payload)
        original_status = self.app.environment_test_status_var.get()
        original_checks = dict(self.app.environment_test_checks)
        payload['sidecar_integrity'] = {'status': 'FAIL', 'sidecars': [{'path': '/fixture/evidence', 'reason': 'missing'}]}
        self.app._render_environment_test_progress(payload)
        status = self.app.environment_test_status_var.get()
        from crescendo.i18n import tr
        self.assertIn(tr('证据完整性告警：附属文件校验失败，诊断结论保持不变。'), tr(status))
        self.assertTrue(status.startswith(original_status))
        self.assertEqual(self.app.environment_test_state, 'completed')
        self.assertEqual(self.app.environment_test_checks, original_checks)
        self.assertTrue(self.app.environment_test_payload['report_ready'])
        payload['sidecar_integrity']['status'] = 'PASS'
        self.app._render_environment_test_progress(payload)
        self.assertEqual(self.app.environment_test_status_var.get(), original_status)

    def test_pro_control_labels_follow_presentation_translation(self):
        from crescendo.i18n import tr, ENABLED
        parent = self.app.diagnostic_log_detail_combo.master
        labels = [child.cget('text') for child in parent.winfo_children() if child.winfo_class() == 'TLabel']
        expected = ['诊断日志详略', '每项检查示例数（0–20）']
        if ENABLED:
            expected = [tr(text) for text in expected]
        self.assertEqual(labels, expected)

    def test_pro_controls_preserve_draft_when_hidden_and_freeze_into_spec(self):
        window = self.app
        parent = window.diagnostic_log_detail_combo.master
        self.assertIn(str(parent), window._simple_hidden)
        window.pro_mode_var.set(True)
        window._apply_mode(initial=True)
        self.assertEqual(parent.winfo_manager(), 'pack')
        window.show_page('EDA Settings')
        window.diagnostic_log_detail_var.set('full')
        window.diagnostic_log_examples_var.set('2')
        window.pro_mode_var.set(False)
        window._apply_mode(initial=True)
        self.assertEqual(window.nav_items['EDA Settings'].winfo_manager(), '')
        self.assertNotEqual(window.current_page, 'EDA Settings')
        self.assertEqual(window.diagnostic_log_detail_var.get(), 'full')
        self.assertEqual(window.diagnostic_log_examples_var.get(), '2')
        # A thin input fixture isolates these settings from unrelated form gates.
        draft = fixtures.EnvironmentTestInputTests().app()
        draft.diagnostic_log_detail_var = fixtures.Value('full')
        draft.diagnostic_log_examples_var = fixtures.Value('2')
        spec = draft._collect_environment_test_spec()
        self.assertEqual(spec['diagnostic_log_detail'], 'full')
        self.assertEqual(spec['diagnostic_log_examples'], 2)


class DiagnosticLogInputTests(unittest.TestCase):
    def test_gui_example_validation_has_clear_errors_and_accepts_endpoints(self):
        draft = fixtures.EnvironmentTestInputTests().app()
        for invalid in ('-1', '21', '1.5', '', 'abc'):
            draft.diagnostic_log_examples_var = fixtures.Value(invalid)
            with self.subTest(value=invalid), self.assertRaisesRegex(ValueError, r'diagnostic_log_examples.*0\.\.20'):
                draft._collect_environment_test_spec()
        for valid in ('0', '20'):
            draft.diagnostic_log_examples_var = fixtures.Value(valid)
            self.assertEqual(draft._collect_environment_test_spec()['diagnostic_log_examples'], int(valid))
