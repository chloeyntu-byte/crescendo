# -*- coding: utf-8 -*-
"""Failure-only CDF evidence; these fixtures never execute PDK callbacks."""
import json
import copy
import os
import tempfile
import threading
import unittest
from unittest import mock

from crescendo_engine import environment_diagnostics
from crescendo_engine.diagnosis_catalog import (
    _current_cdf_failures, _evidence, describe_check, format_section,
)
from crescendo_engine.environment_test import EnvironmentTestManager
from crescendo_engine.native_diagnostics import cdf_callback_failures, skill_command_diagnostics
from crescendo_engine.range_validation import _endpoint_rows, run_range_validation
from crescendo_engine.workspace import ParameterApplicationFailure, validate_schematic_application


def _literal(value):
    if isinstance(value, dict):
        return '(' + ' '.join('(%s %s)' % (_literal(key), _literal(item))
                              for key, item in value.items()) + ')'
    if isinstance(value, list):
        return '(' + ' '.join(_literal(item) for item in value) + ')'
    if value is None or value is False:
        return 'nil'
    if value is True:
        return 't'
    return json.dumps(value)


def _marker(**overrides):
    value = {'schema_version': 'crescendo.cdf_callback_input.v1', 'parameter_id': 'nfin-id',
        'property': 'nfin', 'requested_value': '5', 'db_property_present': True,
        'db_property_value': '5', 'cdf_value': '5', 'callback': 'fixture("nfin")',
        'instance_lcv': ['D', 'DUT', 'private_view', 'M0'], 'hierarchy_path': '/X1/M0',
        'selected_parameter_names': ['nfin'], 'capture_stage': 'callback_failure',
        'expression_fields': [{'name': 'nf', 'value': 'FN_gm', 'type': 'string',
            'parse_as_cel': 'yes', 'parse_as_number': 'yes', 'value_truncated': False}],
        'expression_fields_status': 'bounded_candidate_strings', 'form_init_proc': '',
        'done_proc': '', 'expression_fields_truncated': False}
    value.update(overrides)
    return 'CRESCENDO_CDF_CALLBACK_INPUT\t' + _literal(value)


def _failure(marker=None, argument='FN_gm', property_name='nfin'):
    return ('*Error* int: can\'t handle int(%s)\n' % json.dumps(argument) +
            (marker or _marker()) + '\n' +
            '*Error* CRESCENDO_ADAPTER_ERROR CDF callback failed for ' +
            property_name + ': "C710_coremos_CB( \'nfin )"\n')


def _native_capture(root, failures=1, startup_lines=7000):
    numeric = os.path.join(root, 'numeric')
    os.mkdir(numeric)
    script = os.path.join(numeric, 'capture.il')
    with open(script, 'w') as stream:
        stream.write('printf("DONE")\n')
    console = os.path.join(numeric, 'ocean.stdout')
    with open(console, 'w') as stream:
        stream.write(('startup ' + 'x' * 300 + '\n') * startup_lines + _failure() * failures)
    capture = {'schema_version': 'crescendo.native_capture_diagnostics.v1', 'capture_only': True,
               'script_path': script, 'skill': skill_command_diagnostics(
                   {'returncode': 1}, script, 'DONE', numeric)}
    path = os.path.join(numeric, 'capture_diagnostics.json')
    with open(path, 'w') as stream:
        json.dump(capture, stream)
    return capture, path, console


class CdfFailureDiagnosticsTests(unittest.TestCase):
    def _check(self):
        return {'id': 'spectre.maestro_netlist.execution', 'check_id': 'execution',
            'status': 'FAIL', 'message': 'raw failure',
            'details': {'first_cause': {'failure_stage': 'native_capture_skill_error',
                                      'message': 'native netlist capture failed'}}}

    def test_actual_same_console_error_distinguishes_selected_and_unselected_fields(self):
        rows = cdf_callback_failures(_failure(), 'numeric/ocean.stdout')
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row['property'], 'nfin')
        self.assertEqual(row['requested_value'], '5')
        self.assertEqual(row['expression_fields'][0]['name'], 'nf')
        self.assertEqual(row['matching_expression_fields'], ['nf'])
        self.assertEqual(row['console_source'], 'numeric/ocean.stdout')
        self.assertEqual(row['native_error_line'], 1)
        self.assertEqual(row['console_line'], 2)

    def test_no_association_when_actual_int_argument_differs_or_field_truncated(self):
        unrelated = cdf_callback_failures(_failure(argument='ANOTHER_VAR'))[0]
        self.assertEqual(unrelated['matching_expression_fields'], [])
        marker = _marker(expression_fields=[{'name': 'nf', 'value': 'FN_gm',
                                            'value_truncated': True}])
        truncated = cdf_callback_failures(_failure(marker))[0]
        self.assertEqual(truncated['matching_expression_fields'], [])

    def test_marker_alone_wrong_property_or_generated_source_is_not_a_failure(self):
        for text in (_marker(), _failure(property_name='w'),
                     'printf("%s")\n' % _marker(),
                     json.dumps({'previous_run': _failure()})):
            self.assertEqual(cdf_callback_failures(text), [])

    def test_association_stops_at_the_next_callback_failure_or_marker(self):
        wrong_first = _failure(property_name='w') + (
            '*Error* CRESCENDO_ADAPTER_ERROR CDF callback failed for nfin: "fixture()"\n')
        self.assertEqual(cdf_callback_failures(wrong_first), [])
        double_marker = (_failure().splitlines()[0] + '\n' + _marker() + '\n' +
                         _failure(_marker(property='w'), property_name='w'))
        rows = cdf_callback_failures(double_marker)
        self.assertEqual([row['property'] for row in rows], ['w'])
        other_int = _failure().replace("can't handle int", 'another int error')
        row = cdf_callback_failures(other_int)[0]
        self.assertNotIn('native_error', row)
        self.assertEqual(row['matching_expression_fields'], [])

    def test_old_marker_and_malformed_marker_remain_raw_without_invented_context(self):
        old = _marker(expression_fields=[], selected_parameter_names=['nfin'])
        row = cdf_callback_failures(_failure(old))[0]
        self.assertEqual(row['matching_expression_fields'], [])
        for marker in ('CRESCENDO_CDF_CALLBACK_INPUT param_id=p requested=5',
                       'CRESCENDO_CDF_CALLBACK_INPUT (("schema_version" "unknown"))',
                       'CRESCENDO_CDF_CALLBACK_INPUT (' + '(' * 15 + ')'*15 + ')',
                       'CRESCENDO_CDF_CALLBACK_INPUT ' + 'x' * 32769):
            self.assertEqual(cdf_callback_failures(_failure(marker)), [])

    def test_parser_is_bounded_and_never_evaluates_native_expression(self):
        marker = _marker(expression_fields=[{'name': 'nf', 'value': 'eval("hostile")',
                                             'value_truncated': False}])
        rows = cdf_callback_failures(_failure(marker, 'eval("hostile")') * 20)
        self.assertEqual(len(rows), 8)
        self.assertEqual(rows[0]['expression_fields'][0]['value'], 'eval("hostile")')

    def test_parser_rejects_out_of_schema_shapes_and_budgets(self):
        malformed = (
            _marker(selected_parameter_names=['nfin'] * 33),
            _marker(expression_fields=[{'name': ['nf'], 'value': 'FN_gm'}]),
            _marker(expression_fields=[{'name': 'nf', 'value': 'x' * 193}]),
            _marker(expression_fields=[{'name': 'nf', 'value': 'FN_gm',
                                       'value_truncated': 'nil'}]),
            _marker(requested_value={'nested': '5'}),
        )
        for marker in malformed:
            self.assertEqual(cdf_callback_failures(_failure(marker)), [])

    def test_owned_current_console_precedes_stale_command_tails(self):
        with tempfile.TemporaryDirectory() as root:
            script = os.path.join(root, 'capture.il')
            with open(script, 'w') as stream:
                stream.write('printf("DONE")\n')
            console = os.path.join(root, 'ocean.stdout')
            with open(console, 'w') as stream:
                stream.write('DONE\n')
            old = {'returncode': 0, 'stdout_tail': _failure()}
            result = skill_command_diagnostics(old, script, 'DONE', root)
            self.assertTrue(result['ok'])
            self.assertNotIn('cdf_callback_failures', result)
            with open(console, 'w') as stream:
                stream.write('ordinary startup\n' * 40 + _failure())
            result = skill_command_diagnostics({'returncode': 1}, script, 'DONE', root)
            self.assertFalse(result['ok'])
            self.assertEqual(result['cdf_callback_failures'][0]['console_line'], 42)
            self.assertEqual(result['cdf_callback_failures'][0]['native_error_line'], 41)

    def test_command_tails_and_different_console_streams_do_not_associate(self):
        with tempfile.TemporaryDirectory() as root:
            script = os.path.join(root, 'capture.il')
            with open(script, 'w') as stream:
                stream.write('printf("DONE")\n')
            result = skill_command_diagnostics({'returncode': 1, 'stdout_tail': _failure()},
                                               script, 'DONE', root)
            self.assertEqual(result['console_scope'], 'command_tails_only')
            self.assertNotIn('cdf_callback_failures', result)
            with open(os.path.join(root, 'ocean.stdout'), 'w') as stream:
                stream.write('\n'.join(_failure().splitlines()[:2]) + '\n')
            with open(os.path.join(root, 'ocean.stderr'), 'w') as stream:
                stream.write(_failure().splitlines()[2] + '\n')
            result = skill_command_diagnostics({'returncode': 1}, script, 'DONE', root)
            self.assertNotIn('cdf_callback_failures', result)

    def test_collector_and_ui_current_case_hook_keep_original_first_cause(self):
        with tempfile.TemporaryDirectory() as root:
            numeric = os.path.join(root, 'numeric')
            os.mkdir(numeric)
            with open(os.path.join(numeric, 'ocean.stdout'), 'w') as stream:
                stream.write(_failure())
            manager = EnvironmentTestManager.__new__(EnvironmentTestManager)
            manager.lock = threading.RLock()
            manager._log = mock.Mock(return_value={'spectre'})
            manager._check = mock.Mock()
            cause = {'failure_stage': 'native_capture_skill_error', 'message': 'original native cause'}
            row = {'case_id': 'spectre.maestro_netlist', 'simulator': 'spectre',
                'selected_simulator': 'spectre', 'test_id': 'current', 'config': {}, 'prepared': {},
                'cancel': threading.Event(), 'case_records': {}, 'engine_outcomes': {
                    'maestro_netlist': {'first_cause': cause}},
                'simulator_logs': {'spectre': os.path.join(root, 'environment_test.log')}}
            manager._collect_inline(row, 'maestro_netlist', root, {}, True)
            context = row['case_contexts']['spectre.maestro_netlist']
            self.assertEqual(context['first_cause'], cause)
            self.assertEqual(len(context['cdf_callback_failures']), 1)
            presentation = describe_check(self._check(), context)
            self.assertIn('选中参数 nfin', presentation['explanation'])
            self.assertIn('nf="FN_gm"', presentation['explanation'])
            self.assertIn('可能有关', presentation['explanation'])
            self.assertIn('尚未证明', presentation['explanation'])
            self.assertIn('original native cause', presentation['explanation'])
            self.assertEqual(presentation['summary'], 'original native cause')
            self.assertEqual(presentation['section_id'], 'netlist')
            self.assertTrue(any(item['path'] == os.path.join(root, 'numeric', 'ocean.stdout')
                                for item in presentation['evidence']))
            # The pre-existing missing-SetupDB collection warning is expected
            # from this deliberately small fixture; no CDF check was added.
            self.assertTrue(all(call[0][1] == 'diagnostics.collection'
                                for call in manager._check.call_args_list))
            records = [call[0][1] for call in manager._log.call_args_list]
            text = format_section([self._check()], dict(context, records=records))
            self.assertIn('diagnostic.cdf_callback_failure', text)
            self.assertIn('numeric/ocean.stdout', text)

    def test_current_case_hook_caps_multiple_console_failures_at_eight(self):
        with tempfile.TemporaryDirectory() as root:
            records = []
            for source in ('numeric/ocean.stdout', 'baseline/ocean.stdout'):
                records.append({'observation': 'diagnostic.cdf_callback_failure',
                    'payload': {'path': source, 'cdf_callback_failures':
                        cdf_callback_failures(_failure() * 8, source),
                        'cdf_callback_failures_truncated': True}})
            manager = EnvironmentTestManager.__new__(EnvironmentTestManager)
            manager.lock = threading.RLock()
            manager._log = mock.Mock()
            manager._check = mock.Mock()
            row = {'case_id': 'alps.maestro_netlist', 'simulator': 'alps', 'config': {},
                'prepared': {}, 'cancel': threading.Event(), 'engine_outcomes': {'maestro_netlist': {}}}
            with mock.patch('crescendo_engine.environment_test.collect_run_diagnostics', return_value=records):
                manager._collect_inline(row, 'maestro_netlist', root, {}, True)
            context = row['case_contexts']['alps.maestro_netlist']
            self.assertEqual(len(context['cdf_callback_failures']), 8)
            self.assertTrue(context['cdf_callback_failures_truncated'])
            self.assertFalse(manager._check.called)

    def test_context_without_exact_argument_match_does_not_claim_related_error(self):
        rows = cdf_callback_failures(_failure(argument='OTHER_VAR'))
        presentation = describe_check(self._check(), {'cdf_callback_failures': rows})
        self.assertNotIn('可能有关', presentation['explanation'])
        self.assertIn('不能据此认定', presentation['explanation'])

    def test_callback_context_never_replaces_an_authoritative_first_cause(self):
        context = {'cdf_callback_failures': cdf_callback_failures(_failure(), 'numeric/ocean.stdout'),
                   'first_cause': {'failure_stage': 'solve_failure', 'message': 'authoritative solver cause'}}
        presentation = describe_check(self._check(), context)
        self.assertEqual(presentation['summary'], 'authoritative solver cause')
        self.assertEqual(presentation['section_id'], 'solver')
        self.assertIn('nf="FN_gm"', presentation['explanation'])

    def test_non_ocean_logs_and_embedded_artifacts_do_not_become_ui_failures(self):
        for source in (None, 'scheduler.stdout', 'capture.il', 'spectre.out'):
            context = environment_diagnostics._windows(_failure(), console_source=source)
            self.assertNotIn('cdf_callback_failures', context)
        with tempfile.TemporaryDirectory() as root:
            with open(os.path.join(root, 'scheduler.stdout'), 'w') as stream:
                stream.write(_failure())
            records = environment_diagnostics.collect_run_diagnostics(
                root, 'maestro_netlist', {}, {}, {}, stop_proven=True)
            self.assertFalse(any(row['observation'] == 'diagnostic.cdf_callback_failure'
                                 for row in records))
            presentation = describe_check(self._check(), {'records': records, 'run_root': root})
            self.assertNotIn('FN_gm', presentation['explanation'])
        rows = cdf_callback_failures(_failure(), 'numeric/ocean.stdout')
        stale = {'records': [{'observation': 'diagnostic.artifact',
                             'payload': {'data': {'cdf_callback_failures': rows}}}]}
        self.assertNotIn('FN_gm', describe_check(self._check(), stale)['explanation'])

    def test_excerpt_byte_gaps_do_not_create_adjacency_or_physical_line_claims(self):
        gap = '[diagnostic byte gap: file head/tail included; middle omitted]\n'
        before, marker, after = _failure().splitlines()
        self.assertEqual(cdf_callback_failures(before + '\n' + marker + '\n' + gap + after), [])
        rows = cdf_callback_failures(before + '\n' + gap + marker + '\n' + after)
        self.assertEqual(rows[0]['matching_expression_fields'], [])
        context = environment_diagnostics._windows(gap + _failure(),
                                                   console_source='numeric/ocean.stdout')
        self.assertNotIn('cdf_callback_failures', context)

    def test_large_ocean_uses_current_native_scan_through_real_collector_and_ui(self):
        with tempfile.TemporaryDirectory() as root:
            capture, path, console = _native_capture(root)
            self.assertGreater(os.path.getsize(console), 2 * 1024 * 1024)
            self.assertEqual(capture['skill']['cdf_callback_failures'][0]['console_line'], 7002)
            manager = EnvironmentTestManager.__new__(EnvironmentTestManager)
            manager.lock = threading.RLock()
            manager._log = mock.Mock(return_value={'spectre'})
            manager._check = mock.Mock()
            cause = {'failure_stage': 'native_capture_skill_error', 'message': 'authoritative native cause'}
            row = {'case_id': 'spectre.maestro_netlist', 'simulator': 'spectre',
                   'selected_simulator': 'spectre', 'config': {}, 'prepared': {},
                   'cancel': threading.Event(), 'case_records': {},
                   'engine_outcomes': {'maestro_netlist': {'first_cause': cause}}}
            manager._collect_inline(row, 'maestro_netlist', root, {}, True)
            context = row['case_contexts']['spectre.maestro_netlist']
            self.assertEqual(context['first_cause'], cause)
            failure = context['cdf_callback_failures'][0]
            self.assertEqual(failure['console_source'], 'numeric/ocean.stdout')
            self.assertEqual(failure['console_line'], 7002)
            self.assertEqual(failure['native_error_line'], 7001)
            self.assertEqual(failure['diagnostic_path'], 'numeric/capture_diagnostics.json')
            presentation = describe_check(self._check(), context)
            self.assertEqual(presentation['summary'], 'authoritative native cause')
            self.assertIn('nf="FN_gm"', presentation['explanation'])
            self.assertIn('可能有关', presentation['explanation'])
            observations = [call[0][1] for call in manager._log.call_args_list
                            if call[0][1].get('observation') == 'diagnostic.cdf_callback_failure']
            self.assertEqual(len(observations), 1)
            self.assertTrue(observations[0]['payload']['native_full_console_scan'])
            self.assertTrue(observations[0]['payload']['identity']['read_truncated'])

    def test_large_ocean_native_summary_rejects_foreign_or_untrusted_producer_identity(self):
        with tempfile.TemporaryDirectory() as root:
            capture, path, console = _native_capture(root)
            cases = [
                (('schema_version',), 'unknown'),
                (('script_path',), '/old-run/numeric/capture.il'),
                (('skill', 'schema_version'), 'unknown'),
                (('skill', 'console_scope'), 'command_tails_only'),
                (('skill', 'inspected_logs'), []),
                (('skill', 'cdf_callback_failures', 0, 'schema_version'), 'unknown'),
                (('skill', 'cdf_callback_failures', 0, 'console_source'), '../old-run/ocean.stdout'),
            ]
            for keys, value in cases:
                document = copy.deepcopy(capture)
                cursor = document
                for key in keys[:-1]:
                    cursor = cursor[key]
                cursor[keys[-1]] = value
                with open(path, 'w') as stream:
                    json.dump(document, stream)
                records = environment_diagnostics.collect_run_diagnostics(
                    root, 'maestro_netlist', {}, {}, {}, stop_proven=True)
                self.assertFalse(any(record['observation'] == 'diagnostic.cdf_callback_failure'
                                     for record in records), keys)

    def test_reduced_console_read_limit_keeps_only_eight_native_physical_summaries(self):
        with tempfile.TemporaryDirectory() as root:
            capture, path, console = _native_capture(root, failures=9, startup_lines=500)
            with mock.patch.object(environment_diagnostics, 'MAX_FILE', 64 * 1024):
                records = environment_diagnostics.collect_run_diagnostics(
                    root, 'maestro_netlist', {}, {}, {}, stop_proven=True)
            observations = [record['payload'] for record in records
                            if record['observation'] == 'diagnostic.cdf_callback_failure']
            self.assertEqual(len(observations), 1)
            evidence = observations[0]
            self.assertTrue(evidence['identity']['read_truncated'])
            self.assertTrue(evidence['native_full_console_scan'])
            self.assertTrue(evidence['cdf_callback_failures_truncated'])
            self.assertEqual(len(evidence['cdf_callback_failures']), 8)
            self.assertEqual(evidence['cdf_callback_failures'][0]['console_line'], 502)
            self.assertEqual(evidence['cdf_callback_failures'][-1]['console_line'], 523)

    def test_ui_traversal_and_display_are_bounded_for_repeated_or_cyclic_values(self):
        cycle = {'children': []}
        cycle['children'].append(cycle)
        self.assertEqual(_current_cdf_failures(cycle), [])
        self.assertEqual(_evidence(cycle), [])
        rows = cdf_callback_failures(_failure() * 8, 'numeric/ocean.stdout')
        nested = {'cdf_callback_failures': rows * 50, 'path': 'numeric/ocean.stdout'}
        self.assertEqual(len(_current_cdf_failures(nested)), 8)
        presentation = describe_check(self._check(), nested)
        self.assertEqual(presentation['explanation'].count('失败现场的未选参数字符串'), 3)
        self.assertLess(len(presentation['explanation']), 10000)
        oversized = cdf_callback_failures(_failure(_marker(requested_value='x' * 10000)),
                                          'numeric/ocean.stdout')
        explanation = describe_check(self._check(), {'cdf_callback_failures': oversized})['explanation']
        self.assertIn('界面截断', explanation)
        self.assertNotIn('x' * 300, explanation)

    def test_success_and_unrelated_checks_do_not_expand_cdf_details(self):
        context = {'cdf_callback_failures': cdf_callback_failures(_failure())}
        check = self._check()
        check['status'] = 'PASS'
        self.assertNotIn('FN_gm', describe_check(check, context)['explanation'])
        check.update(status='NOT_TESTED', check_id='scheduler.jobs')
        self.assertNotIn('FN_gm', describe_check(check, context)['explanation'])
        context = {'previous_run': {'cdf_callback_failures': cdf_callback_failures(_failure())}}
        self.assertNotIn('FN_gm', describe_check(self._check(), context)['explanation'])

    def test_environment_windows_keep_structured_failure_after_compaction(self):
        text = 'startup\n' * 100 + _failure() + 'tail\n' * 300
        context = environment_diagnostics._windows(text, max_lines=24,
                                                   console_source='numeric/ocean.stdout')
        self.assertLessEqual(len(context['lines']), 24)
        self.assertEqual(context['cdf_callback_failures'][0]['matching_expression_fields'], ['nf'])

    def test_observed_requested_one_effective_two_is_range_adjustment_not_strict_success(self):
        # An observed callback result, not a hard-coded PDK range or algorithm.
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, 'application.records')
            with open(path, 'w') as stream:
                stream.write('PARAM\t"nfin-id"\t"1"\t"2"\t"2"\t"executed"\t"fixture()"\n')
            plan = {'verification_path': path, 'clones': [],
                    'parameters': [{'parameter_id': 'nfin-id'}]}
            with self.assertRaisesRegex(ParameterApplicationFailure, 'schematic property mismatch'):
                validate_schematic_application(plan)
            adjusted = validate_schematic_application(plan, allow_effective_adjustment=True)
            rows = _endpoint_rows([{'parameter_id': 'nfin-id', 'backend': 'schematic'}],
                'minimum', {'nfin-id': 1}, {'schematic_verification': adjusted})
            self.assertEqual(rows[0]['requested'], 1)
            self.assertEqual(rows[0]['effective'], '2')
            self.assertEqual(rows[0]['status'], 'pdk_adjusted')

    def test_range_flow_reports_observed_clamp_without_changing_the_requested_domain(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {'parameters': [{'parameter_id': 'nfin-id', 'backend': 'schematic',
                                         'optimizable': True}], 'setup': {}, 'hierarchy': {}}
            variables = [{'parameter_id': 'nfin-id', 'minimum': 1, 'maximum': 5}]
            calls, updates = [], []
            def observed_native(prepared, parameters, values, config, endpoint_root, candidate_id, **kwargs):
                os.makedirs(endpoint_root)
                requested = values['nfin-id']
                effective = 2 if requested == 1 else requested
                calls.append(dict(requested=values.copy(), allow=kwargs['allow_effective_adjustment'],
                                  generation_only=kwargs['generation_only']))
                return {'parameter_application': {'proof_kind': 'native_run_device_parameters', 'ok': True,
                    'parameters': [{'parameter_id': 'nfin-id', 'points': [
                        {'effective': effective, 'callback_adjustment_proven': True}]}]}}
            with mock.patch('crescendo_engine.netlist_providers.prepare_maestro_netlists', side_effect=observed_native), \
                    mock.patch('crescendo_engine.private_netlist.reconcile_preparation',
                               return_value={'cleanup_proven': True}):
                run_range_validation({'job_root': root}, prepared, variables, {'runtime': {}},
                                     threading.Event(), lambda **changes: updates.append(changes))
            self.assertEqual(updates[-1]['state'], 'completed')
            self.assertEqual([call['requested']['nfin-id'] for call in calls], [1, 5])
            self.assertTrue(all(call['allow'] for call in calls))
            self.assertTrue(all(call['generation_only'] for call in calls))
            self.assertEqual(updates[-1]['results'][0]['requested'], 1)
            self.assertEqual(updates[-1]['results'][0]['effective'], 2)
            self.assertEqual(updates[-1]['results'][0]['status'], 'pdk_adjusted')
            self.assertEqual(updates[-1]['results'][0]['adjustment'], 'quantized_or_clamped')
            self.assertEqual(variables[0]['minimum'], 1)


if __name__ == '__main__':
    unittest.main()
