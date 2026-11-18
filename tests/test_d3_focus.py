"""Synthetic SP contracts, not a certification of a native solver or PSF."""
import copy
import json
import math
import os
import re
import sqlite3
import tempfile
import unittest
from unittest import mock

from crescendo_engine.diagnostic_focus import (consistency_check, focused_checks,
                                               _comparison_outputs, _frozen_outputs)
from crescendo_engine import netlist_measurement as reader
from crescendo_engine.output_status import output_statuses
from crescendo_engine.rdb import parse_rdb
from crescendo_engine.metrics import evaluate_metrics
import test_diagnostic_focus as focus_fixture
import test_rdb as rdb_fixture


class D3FocusTests(unittest.TestCase):
    def test_unknown_units_detect_small_nonzero_difference_and_count_all_candidates(self):
        cases = focus_fixture.FocusTests().cases(1.68e-14)
        for rows in cases.values():
            rows[0]['metrics']['diagnostic_raw_points'][0]['unit'] = ''
            rows.extend([dict(copy.deepcopy(rows[0]), candidate_id='g000_c%04d' % i) for i in range(1, 16)])
        cases['Maestro Netlist'][0]['metrics']['diagnostic_raw_points'][0]['value'] += 1e-19
        report = consistency_check(cases)
        self.assertEqual(report['status'], 'FAIL')
        self.assertEqual(report['compared_candidate_count'], 16)
        self.assertEqual(report['paired_items'], 16)
        self.assertIn('all completed frozen candidates', report['criterion'])
        policy = report['tolerances']['main::v']
        self.assertTrue(policy['unknown_unit_fallback'])
        self.assertEqual(policy['source'], 'fallback')
        self.assertLess(policy['actual_max'], 1.68e-14)
        self.assertAlmostEqual(policy['actual_min'] / 1.68e-14, 1e-6)
        self.assertEqual(report['outside_tolerance_count'], 2)
        for rows in cases.values():
            for row in rows:
                row['metrics']['diagnostic_raw_points'][0]['value'] = 0.
        self.assertEqual(consistency_check(cases)['tolerances']['main::v']['actual_max'], 0.)

    def test_required_scalar_closure_waveforms_idle_and_unit_provenance(self):
        setup = {'tests': [{'name': 'sp', 'outputs': [
            {'name': 'cap_high', 'expression': 'value(cap_wave 22e9)'},
            {'name': 'cap_wave', 'expression': 'cr_cap_wave()', 'unit': 'F'},
            {'name': 'quality_wave', 'expression': 'cr_quality_wave()', 'type': 'waveform'},
            {'name': 'margin', 'expression': 'cap_high + offset'},
            {'name': 'offset', 'expression': '1e-15', 'unit': 'F'},
            {'name': 'unused_probe', 'expression': 'value(cap_wave 1e11)'}]}]}
        spec = {'objectives': [{'output_key': 'sp::margin'}]}
        before = copy.deepcopy(setup)
        outputs, classification = _comparison_outputs(_frozen_outputs({'setup': setup}), spec, {})
        self.assertEqual(set(classification['required_scalars']), {'sp::margin', 'sp::cap_high', 'sp::offset'})
        self.assertEqual({o['output'] for o in classification['waveforms']}, {'sp::cap_wave', 'sp::quality_wave'})
        self.assertEqual([o['output'] for o in classification['idle']], ['sp::unused_probe'])
        self.assertTrue(all(o['status'] == 'not_compared' for o in classification['waveforms']))
        cap = next(o for o in outputs if o['key'] == 'sp::cap_high')
        self.assertEqual((cap['unit'], cap['unit_source']), ('F', 'inferred_value_dependency'))
        self.assertEqual(setup, before)
        cases = focus_fixture.FocusTests().cases()
        for rows in cases.values():
            points = rows[0]['metrics']['diagnostic_raw_points']
            points[:] = [dict(points[0], test='sp', output=key, unit='', value=1e-14)
                         for key in classification['required_scalars']]
            points.append(dict(points[0], output='sp::cap_wave', value=None, output_status='waveform'))
            points.append(dict(points[0], output='sp::unused_probe', value=None))
        report = consistency_check(cases, outputs, [('sp', 'TT')], classified=classification)
        self.assertEqual(report['status'], 'PASS', report)
        self.assertEqual(report['paired_items'], 3)
        self.assertFalse(report['unpaired'])
        self.assertEqual(report['tolerances']['sp::cap_high']['source'], 'inferred_value_dependency')
        self.assertEqual(report['tolerances']['sp::offset']['source'], 'frozen_setup')

    def test_unselected_catalogue_never_becomes_expected_points(self):
        helper = focus_fixture.FocusTests()
        setup = helper.scoped_setup(['FF'])
        setup['tests'][0]['outputs'] += [{'name': 'idle', 'expression': '1/0'},
                                        {'name': 'wave', 'type': 'waveform'}]
        row = helper.focused_row(setup, {'objectives': [{'output_key': 'main::v'}]})
        report = focused_checks(row)['F2']
        self.assertEqual(report['status'], 'PASS', report)
        self.assertEqual(report['output_classification']['required_scalars'], ['main::v'])
        self.assertEqual(report['unpaired'], [])

    def scalar_alias_row(self, selected_probe=False):
        helper = focus_fixture.FocusTests()
        setup = helper.scoped_setup(['FF'])
        setup['tests'][0]['outputs'] += [
            {'name': 'offset', 'expression': '1e-15', 'unit': 'F'},
            {'name': 'probe', 'expression': 'value(offset 1e11)'}]
        spec = {'objectives': [{'output_key': 'main::v'},
                               {'output_key': 'main::probe' if selected_probe else 'main::offset'}]}
        row = helper.focused_row(setup, spec)
        for mode, case in row['focused_cases'].items():
            points = case['candidates'][0]['metrics']['diagnostic_raw_points']
            points.append(dict(points[0], output='main::offset', unit='F',
                               output_status='available',
                               value=2e-15 if mode == 'Maestro Netlist' else 1e-15))
            if selected_probe:
                points.append(dict(points[0], output='main::probe', unit='F', value=1e-15))
        return row

    def test_idle_value_expression_cannot_hide_selected_scalar_difference(self):
        row = self.scalar_alias_row()
        before = copy.deepcopy(row)
        with_idle = focused_checks(row)['F2']
        self.assertEqual(row, before)
        row['prepared']['setup']['tests'][0]['outputs'].pop()
        row['prepared']['outputs'] = [o for o in row['prepared']['outputs']
                                      if o.get('name') != 'probe']
        without_idle = focused_checks(row)['F2']
        for report in (with_idle, without_idle):
            self.assertEqual(report['status'], 'FAIL', report)
            self.assertEqual(report['outside_tolerance_count'], 2)
            self.assertEqual(report['paired_items'], 2)
            self.assertIn('main::offset', report['output_classification']['required_scalars'])
            self.assertEqual(report['output_classification']['waveforms'], [])
        self.assertEqual(with_idle['outside_tolerance'], without_idle['outside_tolerance'])
        self.assertEqual([o['output'] for o in with_idle['output_classification']['idle']], ['main::probe'])

    def test_required_value_consumer_cannot_override_finite_scalar_dependency(self):
        report = focused_checks(self.scalar_alias_row(selected_probe=True))['F2']
        self.assertEqual(report['status'], 'FAIL', report)
        self.assertEqual(report['outside_tolerance_count'], 2)
        self.assertEqual(report['paired_items'], 3)
        self.assertEqual(report['output_classification']['waveforms'], [])
        self.assertIn('main::offset', report['output_classification']['required_scalars'])

    def test_one_finite_scalar_keeps_missing_dependency_in_expected_points(self):
        row = self.scalar_alias_row(selected_probe=True)
        for mode in ('Maestro Netlist', 'Template Netlist'):
            points = row['focused_cases'][mode]['candidates'][0]['metrics']['diagnostic_raw_points']
            points[:] = [p for p in points if p['output'] != 'main::offset']
        report = focused_checks(row)['F2']
        self.assertNotEqual(report['status'], 'PASS', report)
        self.assertEqual([p['output'] for p in report['unpaired']], ['main::offset'])
        self.assertEqual(set(report['unpaired'][0]['missing_modes']), {'Maestro Netlist', 'Template Netlist'})

    def test_idle_value_expression_cannot_infer_type_of_unobserved_scalar(self):
        row = self.scalar_alias_row()
        for case in row['focused_cases'].values():
            points = case['candidates'][0]['metrics']['diagnostic_raw_points']
            points[:] = [p for p in points if p['output'] != 'main::offset']
        report = focused_checks(row)['F2']
        self.assertNotEqual(report['status'], 'PASS', report)
        self.assertEqual([p['output'] for p in report['unpaired']], ['main::offset'])
        self.assertEqual(report['output_classification']['waveforms'], [])

    def test_finite_scalar_readback_takes_precedence_over_waveform_evidence(self):
        for evidence in ('metadata', 'other_mode'):
            with self.subTest(evidence=evidence):
                row = self.scalar_alias_row()
                if evidence == 'metadata':
                    row['prepared']['setup']['tests'][0]['outputs'][1]['type'] = 'waveform'
                else:
                    points = row['focused_cases']['Template Netlist']['candidates'][0]['metrics']['diagnostic_raw_points']
                    points[-1].update(value=None, output_status='waveform')
                report = focused_checks(row)['F2']
                self.assertEqual(report['status'], 'WARN' if evidence == 'other_mode' else 'FAIL', report)
                self.assertEqual(report['output_classification']['waveforms'], [])
                self.assertIn('main::offset', report['output_classification']['required_scalars'])
                if evidence == 'other_mode':
                    self.assertEqual(report['unpaired'][0]['output_states']['Template Netlist']['status'], 'waveform')

    def test_f2_unpaired_distinguishes_all_three_causes_and_first_error(self):
        cases = focus_fixture.FocusTests().cases(1.)
        for mode, status in zip(cases, ('not_collected', 'non_finite', 'evaluation_failed')):
            point = cases[mode][0]['metrics']['diagnostic_raw_points'][0]
            point.update(value=None, output_status=status, first_error='frequency outside sweep' if status == 'evaluation_failed' else None)
        report = consistency_check(cases)
        states = report['unpaired'][0]['output_states']
        self.assertEqual({s['status'] for s in states.values()}, {'not_collected', 'non_finite', 'evaluation_failed'})
        self.assertEqual(states['Maestro Netlist']['first_error'], 'frequency outside sweep')
        cases['Maestro Netlist'][0]['metrics']['diagnostic_raw_points'] = []
        cases['Maestro Netlist'][0]['output_statuses'] = [
            {'output': 'main::v', 'corner': 'TT', 'status': 'evaluation_failed', 'first_error': 'unbound dependency'}]
        self.assertEqual(consistency_check(cases)['unpaired'][0]['output_states']['Maestro Netlist']['first_error'], 'unbound dependency')


    def test_metric_corner_masks_propagate_to_scalar_dependencies(self):
        setup = {'tests': [{'name': 'main', 'outputs': [
            {'name': 'v', 'expression': 'offset + 1', 'unit': 'V'},
            {'name': 'offset', 'expression': '1', 'unit': 'V'},
            {'name': 'other', 'expression': '2', 'unit': 'V'}]}],
            'corners': [{'name': 'TT', 'enabled': True}, {'name': 'FF', 'enabled': True}],
            'test_corner_masks': {'main': {'known': True}}}
        spec = {'objectives': [{'output_key': 'main::v', 'corners': ['TT']}],
                'constraints': [{'output_key': 'main::other', 'corners': ['FF']}]}
        row = focus_fixture.FocusTests().focused_row(setup, spec)
        for case in row['focused_cases'].values():
            candidate = case['candidates'][0]
            points = candidate['metrics']['diagnostic_raw_points']
            points.append(dict(points[0], output='main::offset', value=1.))
            points.append(dict(points[0], output='main::other', corner='FF', value=2.))
        report = focused_checks(row)['F2']
        self.assertEqual(report['status'], 'PASS', report)
        self.assertEqual(report['paired_items'], 3)
        self.assertEqual(report['output_classification']['required_corners'],
                         {'main::v': ['TT'], 'main::offset': ['TT'], 'main::other': ['FF']})

    def test_legacy_null_readback_remains_unclassified_and_missing_policy_is_explicit(self):
        cases = focus_fixture.FocusTests().cases()
        for rows in cases.values():
            rows[0]['metrics']['diagnostic_raw_points'][0]['value'] = None
        report = consistency_check(cases, [{'key': 'main::v', 'unit': ''}], [('main', 'TT')],
                                   classified={'required_scalars': ['main::v']})
        self.assertEqual({s['status'] for s in report['unpaired'][0]['output_states'].values()}, {'unknown'})
        policy = report['tolerances']['main::v']
        self.assertTrue(policy['unknown_unit_fallback'])
        self.assertIsNone(policy['actual_min'])
        self.assertEqual(policy['not_applied_reason'], 'No finite comparable triplet')

if __name__ == '__main__':
    unittest.main()
