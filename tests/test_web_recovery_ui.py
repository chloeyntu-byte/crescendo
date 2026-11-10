import os
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class WebRecoveryUiTests(unittest.TestCase):
    def _read(self, relative_path):
        with open(os.path.join(ROOT, relative_path), encoding="utf-8") as stream:
            return stream.read()

    def test_recent_local_runs_controls_are_wired(self):
        html = self._read("web/index.html")
        javascript = self._read("web/app.js")

        for element_id in (
            "recentRunsSummary",
            "refreshRecentRuns",
            "recentRunsList",
        ):
            self.assertIn('id="%s"' % element_id, html)
        self.assertIn("api('/api/runs')", javascript)
        self.assertIn("/api/checkpoint?run_id=", javascript)
        self.assertIn("resumeCheckpoint(checkpoint,false)", javascript)
        self.assertIn("$('refreshRecentRuns').onclick=loadRecentRuns", javascript)

    def test_recovery_list_never_offers_active_run_for_resume(self):
        javascript = self._read("web/app.js")

        self.assertIn("const active=run.active===true", javascript)
        self.assertIn("${active?'disabled':''}", javascript)
        self.assertIn("active?'Active':'Resume Latest'", javascript)

    def test_range_validation_can_apply_legal_bounds_and_warns_about_runtime_stop(self):
        html = self._read("web/index.html")
        javascript = self._read("web/app.js")

        self.assertIn('id="applyValidatedRangesButton"', html)
        self.assertIn("function applyValidatedRanges()", javascript)
        self.assertIn("row.status='applied'", javascript)
        self.assertIn("first PDK-adjusted candidate stops the run", javascript)
        self.assertIn("$('applyValidatedRangesButton').onclick=applyValidatedRanges", javascript)

    def test_nonterminal_candidate_metrics_are_pending_not_failed(self):
        javascript = self._read("web/app.js")

        self.assertIn("const terminal=['completed','failed','infrastructure_failed'].includes(rowStatus)", javascript)
        self.assertIn("const pending=!terminal&&missing", javascript)
        self.assertIn("const failed=terminal&&(missing||metric.failed||metric.penalized)", javascript)
        self.assertIn("renderMetricStack(objectives,metrics.objectives||[],'objective',row.status)", javascript)
        self.assertIn("renderMetricStack(constraints,metrics.constraints||[],'constraint',row.status)", javascript)
        self.assertIn("if(row.status==='queued')return {label:'Queued',className:'pending'}", javascript)

    def test_exit_optimizer_control_is_explicit_and_wired(self):
        html = self._read("web/index.html")
        javascript = self._read("web/app.js")
        self.assertIn('id="exitOptimizerButton"', html)
        self.assertIn("api('/api/exit'", javascript)
        self.assertIn("$('exitOptimizerButton').onclick=exitOptimizer", javascript)


if __name__ == "__main__":
    unittest.main()
