# -*- coding: utf-8 -*-
import unittest

from crescendo_engine.diagnosis_catalog import format_ladder_summary


class LadderSummaryTests(unittest.TestCase):
    def test_reports_each_stage_and_skip_cause_with_solver_limit(self):
        cases = [
            {'engine': 'maestro_baseline', 'simulator': 'spectre', 'state': 'failed',
             'first_cause': {'message': 'native run error', 'recovery_actions': ['修复源 Test']}},
            {'engine': 'private_netlist', 'simulator': 'alps', 'state': 'completed'},
            {'engine': 'maestro_netlist', 'simulator': 'alps', 'state': 'blocked',
             'skip_reason': 'Previous native Stop/cleanup unproven'}]
        result = format_ladder_summary(cases, 'spectre', 'alps', {'width': 1})
        for token in ('native run error', '修复源 Test', '通过', '阻塞', 'Stop/cleanup unproven',
                      '核实本轮所属任务和集群作业已停止', '求解器不同', '不证明跨求解器数值等价'):
            self.assertIn(token, result)
        self.assertLess(result.index('1. 原生'), result.index('2. 私有'))
        self.assertLess(result.index('2. 私有'), result.index('3. 每候选'))

    def test_all_pass_does_not_claim_numeric_equivalence(self):
        cases = [{'engine': name, 'state': 'completed'} for name in
                 ('maestro_baseline', 'private_netlist', 'maestro_netlist')]
        result = format_ladder_summary(cases, 'spectre', 'spectre', {'width': 1})
        self.assertIn('实际结果及容差证据', result)
        self.assertNotIn('数值等价通过', result)

    def test_template_failure_localizes_and_retains_successful_paths(self):
        cases = [{'engine': 'maestro_baseline', 'state': 'completed'},
                 {'engine': 'private_netlist', 'state': 'failed', 'first_cause_layer': 'netlist_generation',
                  'first_cause': {'failure_stage': 'native_corner_coverage', 'message': 'corner missing'}},
                 {'engine': 'maestro_netlist', 'state': 'completed'}]
        result = format_ladder_summary(cases, 'spectre', 'spectre', {'width': 2.2e-7})
        for token in ('模板准备、捕获及改参链', '网表生成层', '不等于精确根因',
                      '原生 Baseline、每候选原生生成网表 CLI', '模板捕获的 Test/角点'):
            self.assertIn(token, result)

    def test_baseline_failure_and_all_pass_conclusions(self):
        cases = [{'engine': 'maestro_baseline', 'state': 'failed', 'first_cause_layer': 'measurement'},
                 {'engine': 'private_netlist', 'state': 'completed'},
                 {'engine': 'maestro_netlist', 'state': 'completed'}]
        result = format_ladder_summary(cases, 'spectre', 'spectre', {'width': 1})
        self.assertIn('不能据此判定整个环境不可用', result)
        self.assertIn('输出表达式', result)
        cases[0]['state'] = 'completed'
        result = format_ladder_summary(cases, 'spectre', 'spectre', {'width': 1})
        self.assertIn('小规模优化', result)
        self.assertIn('容差证据', result)

    def test_only_successful_real_finite_aggregates_are_shown_without_delta(self):
        cases = [{'engine': 'maestro_baseline', 'state': 'completed', 'metric_summary': [
            {'output_key': 'TB::gain', 'aggregation': 'mean', 'corners': ['tt'], 'value': 1.5},
            {'output_key': 'bad', 'aggregation': 'mean', 'corners': ['tt'], 'value': float('nan')}]},
            {'engine': 'private_netlist', 'state': 'failed', 'metric_summary': [
                {'output_key': 'must_not_show', 'aggregation': 'mean', 'corners': ['tt'], 'value': 9}]}]
        result = format_ladder_summary(cases, 'spectre', 'spectre', {'width': 1})
        self.assertNotIn('TB::gain', result)
        self.assertIn('Live Results', result)
        self.assertIn('日志不重复列出优化数据', result)
        self.assertNotIn('must_not_show', result)
        self.assertNotIn('bad ·', result)

    def test_unknown_identity_and_missing_candidate_not_inferred(self):
        result = format_ladder_summary([{'engine': 'maestro_baseline', 'simulator': 'alps', 'state': 'blocked'}], None, 'alps')
        self.assertIn('冻结候选：未知或未生成', result)
        self.assertIn('求解器身份未知', result)
        self.assertEqual(result.count('待执行'), 2)
        self.assertIn('[求解器未知]', result)
        self.assertNotIn('原生 Baseline：阻塞 [ALPS]', result)


if __name__ == '__main__':
    unittest.main()
