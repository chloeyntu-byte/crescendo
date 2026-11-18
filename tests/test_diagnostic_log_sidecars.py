"""Reproducible large report receipts; no simulator or external services."""
import ast
import collections
import copy
import inspect
import json
import os
import queue
import tempfile
import threading
import unittest
from unittest import mock

from crescendo_engine import environment_test as reports
from crescendo_engine.diagnostic_log import (EvidenceStore, compact_report, encoded, grouped_checks,
    read_evidence, report_options, summarize, verify_sidecars)
from crescendo_engine.diagnostic_sections import collect_sections
from crescendo_engine.unified_configuration import build_configuration, normalize_configuration


def synthetic_row(root):
    modes = ['spectre.' + engine for engine in reports.ENGINES]
    cases, contexts, records, checks = collections.OrderedDict(), {}, {}, collections.OrderedDict()
    timings = []
    for generation in range(1, 5):
        timings.append({'generation': generation, 'status': 'completed', 'total_seconds': 12.25,
            'phases_seconds': {'ask': .1, 'evaluate': 11., 'tell': .15, 'persist': 1.},
            'candidate_count': 4, 'timed_candidate_count': 4, 'resumed_segment': False,
            'storage_operations': {'checkpoint_transaction': {'seconds': .5, 'calls': 1, 'max_seconds': .5, 'bytes': 4096},
                'dataset_prefix_scan': {'seconds': .1, 'calls': 1, 'max_seconds': .1, 'bytes': 4096}},
            'prefix_scans': [{'bytes': 4096, 'file_size': 4096}],
            'generation_commit': [{'verified': True, 'sha256': 'a' * 64}],
            'candidate_phase_sums': [{'phase': phase, 'scope': scope, 'elapsed_seconds': seconds, 'count': count, 'total_count': count}
                for phase, scope, seconds, count in [('build', 'candidate_sum', 2., 4), ('solver', 'task_sum', 7., 8), ('collection', 'batch_sum', 1., 1)]]})
    for mode in modes:
        cases[mode] = {'case_id': mode, 'simulator': 'spectre', 'engine': mode.split('.')[1],
            'state': 'completed', 'stop_required': True, 'stop_proven': True,
            'first_cause': None, 'generation_timings': copy.deepcopy(timings), 'run_id': mode + '_owned'}
        contexts[mode] = {'run_root': os.path.join(root, mode), 'first_cause': None, 'state': 'completed'}
        records[mode] = []
        for candidate in range(16):
            candidate_id = 'g%03d_c%04d' % (candidate // 4, candidate % 4)
            for corner in ('TT', 'SS'):
                identity = {'candidate_id': candidate_id, 'test': 'SP Test', 'corner': corner}
                payloads = {
                    'raw_points': {'diagnostic_raw_points': [dict(identity, output='SP Test::output_%02d' % i,
                        value=1.234 + i, unit='V', expression='value_at_frequency(' + 'frequency,' * 14 + ')') for i in range(16)]},
                    'cleanup': {'guardian_cleanup_proven': {'process_termination_proven': True, 'remote_stop_proven': True,
                        'proofs': [{'pid': 1000 + i, 'identity': 'owned-fixture-token-' + str(i), 'state': 'exited',
                                    'cleanup_proven': True} for i in range(26)]}},
                    'environment': {'userCmdLineOption': '-fixture +threads=2', 'ENV': {
                        'FIXTURE_PATH_%02d' % i: '/fixture/environment/library/' + str(i) * 12 for i in range(20)}},
                    'solver': {'argv': ['fixture-solver', '-64', 'input.scs'], 'stdout': 'solver observed status fixture\n' * 10},
                    'other': {'native_readback': [dict(identity, name='parameter_%02d' % i, source='current-run',
                        verified=True, expected=1.0, actual=1.0) for i in range(22)]}}
                for kind, payload in payloads.items():
                    records[mode].append({'observation': 'diagnostic.' + kind, 'candidate_id': candidate_id,
                                          'payload': dict(identity, **payload)})
            for check_id in ('completion_evidence', 'parameter_application', 'source_guard', 'raw_cleanup_proof', 'metrics', 'execution.pipeline'):
                identifier = mode + '.' + candidate_id + '.' + check_id
                checks[identifier] = {'id': identifier, 'check_id': check_id, 'status': 'PASS', 'message': 'Candidate proof verified',
                    'simulator': 'spectre', 'engine': cases[mode]['engine'], 'details': {'ok': True, 'count': 2}}
        for index in range(14):
            identifier = mode + '.scheduler.job.' + str(10000 + 14 * modes.index(mode) + index)
            checks[identifier] = {'id': identifier, 'check_id': identifier.split('.', 2)[2], 'status': 'WARN',
                'message': 'Exact owned Job ID query: UNKNOWN; query/queue state is not simulator completion or Stop proof',
                'simulator': 'spectre', 'engine': cases[mode]['engine'], 'details': {'job_id': str(10000 + 14 * modes.index(mode) + index)}}
    contexts['environment'] = {'package': {'application_version': 'fixture'}, 'execution_selection': {'tests': [
        {'name': 'SP Test', 'enabled_corners': ['TT', 'SS'], 'requested_analyses': ['sp'], 'selected': True}]}}
    # Include a failed first cause as well as successful modes and query warnings.
    cases[modes[0]].update(state='failed', first_cause={'failure_stage': 'readback', 'message': 'fixture scalar unavailable'})
    contexts[modes[0]]['first_cause'] = cases[modes[0]]['first_cause']
    return {'report_root': root, 'test_id': 'fixture_4x4x3x2', 'state': 'failed',
        'main_log': os.path.join(root, 'environment_test.log'), 'checks': checks,
        'diagnostic_log_detail': 'compact', 'diagnostic_log_examples': 3,
        'config': {'runtime': {'diagnostic_log_detail': 'compact'}}, 'spec': {},
        'case_results': cases, 'case_contexts': contexts, 'case_records': records,
        'case_timing_observers': {}, 'run_ids': {mode: mode + '_owned' for mode in modes},
        'diagnostic_shape': {'generations': 4, 'samples_per_generation': 4, 'total_candidates': 16},
        'source_simulator': 'spectre', 'selected_simulator': 'spectre', 'prepared': {},
        'section_registrations': [('fixture', lambda context: {'title': 'Fixture application summary', 'status': 'WARN',
            'body': 'Compared modes: %d; focused result: %s; context roots: %d' %
                (len(context['cases']), context['focused_checks']['F2']['status'], len(context['case_contexts']))})],
        'log_queue': queue.Queue(), 'report_state': {}, 'write_error': None}


def synthetic_focus(row):
    from crescendo_engine.diagnostic_focus import checkpoint_check
    return {'F1': {'status': 'PASS', 'modes': {mode: checkpoint_check(case['generation_timings']) for mode, case in row['case_results'].items()}},
        'F2': {'status': 'WARN', 'criterion': 'all mode pairs within scalar tolerance', 'paired_items': 128,
            'pair_count': 384, 'outside_tolerance_count': 0, 'unpaired': [
                {'output': 'SP Test::output_%02d' % (i % 8), 'reason': 'required scalar unavailable',
                 'candidate': 'g%03d_c%04d' % (i // 32, i % 4), 'corner': 'TT' if i % 2 else 'SS',
                 'output_states': {mode: {'status': 'unavailable', 'first_error': 'Missing fixture scalar readback at expected point ' + str(i)}
                                   for mode in row['case_results']}} for i in range(128)]},
        'F3': {'status': 'WARN', 'modes': {mode: {'status': 'WARN', 'criterion': 'bytes/point <= 262144', 'bytes_per_point': 300000,
            'scheduler_duplicates': [{'path': 'candidates/g%03d_c%04d/receipt/%d' % (i // 128, i % 4, i), 'occurrences': 42,
                'sha256': str(i % 10) * 64} for i in range(240)]} for mode in row['case_results']}},
        'F4': {'status': 'PASS', 'criterion': 'setup heartbeat within budget', 'logical_bytes': 1234567,
               'parameter_count': 2500, 'optimizable_count': 120, 'over_200ms': 0}}


def render_source(source, row, focus):
    """Exercise the actual final-drain renderer on identical frozen input fields."""
    tree = ast.parse(source)
    function = next(node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name == 'finish_reports')
    module = ast.Module(body=[function], type_ignores=[])
    namespace = dict(vars(reports))
    service = mock.Mock(config=row['config'], diagnostic_section_collector=None)
    manager = reports.EnvironmentTestManager(service)
    output = []
    manager._append_report = lambda row, raw, wait=False: output.append(raw)
    namespace.update(self=manager, row=row, _utc=lambda: '2026-01-01T00:00:00Z')
    exec(compile(module, '<frozen-report-renderer>', 'exec'), namespace)
    with mock.patch('crescendo_engine.diagnostic_focus.focused_checks', return_value=copy.deepcopy(focus)):
        namespace['finish_reports']()
    if row.get('write_error'):
        raise AssertionError(row['write_error'])
    return ''.join(output)


def structured(text, prefix):
    return [json.loads(line[len(prefix) + 1:]) for line in text.splitlines() if line.startswith(prefix + ' ')]


class DiagnosticSidecarTests(unittest.TestCase):
    def test_dedup_restores_types_and_verifies_tamper_missing_and_same_size_change(self):
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, 'evidence.jsonl')
            repeated = {'many': [1, None, True, 'abc'], 'nested': {'ref': 'literal-data-key', 'text': 'long shared proof ' * 100}}
            docs = {'a': [repeated] * 50, 'b': repeated}
            item = EvidenceStore(path).finish(docs)
            self.assertEqual(read_evidence(path), docs)
            self.assertLess(item['bytes'], len(encoded(docs)))
            self.assertEqual(verify_sidecars([item])['status'], 'PASS')
            with open(path, 'r+b') as stream:
                byte = stream.read(1); stream.seek(0); stream.write(b'!' if byte != b'!' else b'?')
            audit = verify_sidecars([item])
            self.assertEqual(audit['status'], 'FAIL')
            self.assertEqual(audit['sidecars'][0]['actual_bytes'], item['bytes'])
            os.unlink(path)
            self.assertEqual(verify_sidecars([item])['status'], 'FAIL')

    def test_unknown_queries_and_unpaired_groups_and_zero_examples(self):
        with tempfile.TemporaryDirectory() as root:
            row = synthetic_row(root)
            groups = grouped_checks(list(row['checks'].values()))
            self.assertEqual(len(groups), 1)
            self.assertEqual(groups[0]['count'], 42)
            self.assertEqual(len(groups[0]['modes']), 3)
            self.assertEqual(groups[0]['job_id_range'], ['10000', '10041'])
            self.assertEqual(len(groups[0]['examples']), 3)
            focus = synthetic_focus(row)
            summary = summarize(focus, 0)['F2']['unpaired']
            self.assertEqual(sum(g['count'] for g in summary['groups']), 128)
            self.assertEqual(len(summary['groups']), 8)
            self.assertEqual(summary['examples'], [])

    def test_frozen_full_and_compact_preserve_structured_verdict_timings_focus_contributors(self):
        with tempfile.TemporaryDirectory() as root:
            row = synthetic_row(root)
            focus = synthetic_focus(row)
            source = inspect.getsource(reports)
            row['diagnostic_log_detail'] = row['config']['runtime']['diagnostic_log_detail'] = 'full'
            full = render_source(source, row, focus)
            row['diagnostic_log_detail'] = row['config']['runtime']['diagnostic_log_detail'] = 'compact'
            compact = render_source(source, row, focus)
            self.assertLess(len(compact.encode('utf-8')), 100 * 1024)
            self.assertGreater(len(full.encode('utf-8')), 1500000)
            self.assertEqual(structured(full, 'diagnostic_overview'), structured(compact, 'diagnostic_overview'))
            self.assertEqual(structured(full, 'diagnostic_section'), structured(compact, 'diagnostic_section'))
            documents = read_evidence(row['report_sidecars'][0]['path'])
            self.assertEqual(documents['case_results'], row['case_results'])
            self.assertEqual(documents['focused_checks'], focus)
            self.assertEqual(documents['case_records'], row['case_records'])
            self.assertEqual(documents['checks'], list(row['checks'].values()))
            self.assertEqual(verify_sidecars(row['report_sidecars'])['status'], 'PASS')
            self.assertNotIn('guardian_cleanup_proven', compact)
            self.assertNotIn('userCmdLineOption', compact)
            self.assertEqual(compact.count('Exact owned Job ID query: UNKNOWN'), 1)
            self.assertEqual(len(structured(compact, 'diagnostic_section')), 1)
            self.assertIn('|spectre.maestro_netlist|4|solver|task_sum|7.0|8/8|', compact)

    def test_tool_warning_stays_visible_outside_compact_first_cause(self):
        from crescendo_engine.environment_diagnostics import collect_run_diagnostics
        from crescendo_engine.native_diagnostics import skill_command_diagnostics
        fixture = os.path.join(os.path.dirname(__file__), 'fixtures', 'library_init_ocean.txt')
        with open(fixture, encoding='utf-8') as stream:
            ocean = stream.read()
        error = "*Error* actual SKILL failure"
        with tempfile.TemporaryDirectory() as root:
            row = synthetic_row(root)
            scope = 'spectre.maestro_baseline'
            run_root = row['case_contexts'][scope]['run_root']
            os.mkdir(run_root)
            with open(os.path.join(run_root, 'ocean.stdout'), 'w') as stream:
                stream.write(ocean + error + '\n')
            skill = skill_command_diagnostics({'returncode': 0}, os.path.join(run_root, 'bootstrap.il'),
                                              owned_root=run_root, simulators=('spectre',))
            row['case_results'][scope]['first_cause']['message'] = skill['first_error']['message']
            row['case_records'][scope] = collect_run_diagnostics(run_root, 'maestro_baseline',
                spec={'simulator': 'spectre'})
            compact = render_source(inspect.getsource(reports), row, synthetic_focus(row))
            found = structured(compact, 'ocean_tool_warning')
            self.assertEqual([(item['severity'], item.get('native_code') or item.get('category')) for item in found],
                             [('WARN', 'library_init'), ('WARN', 'ADE-5066'), ('WARN', 'ADE-5067')])
            self.assertIn('<<< Stack Trace >>>', found[0]['message'])
            self.assertIn('asiEnvGetVar', found[0]['message'])
            verdict = next(line for line in compact.splitlines() if line.startswith('1. '))
            self.assertIn(error, verdict)
            self.assertNotIn('ADE-5066', verdict)
            self.assertNotIn('asiEnvGetVar', verdict)

    def test_template_selection_warning_and_crash_advice_in_both_report_formats(self):
        from crescendo_engine.environment_diagnostics import collect_run_diagnostics
        from crescendo_engine.run_log import append_run_event
        from crescendo_engine.variable_selection_notice import VARIABLE_NOTICE_STAGE, variable_selection_notice

        notice = variable_selection_notice("private_netlist", 51)
        original = "Virtuoso exited during native reference capture."
        for detail in ("compact", "full"):
            with self.subTest(detail=detail), tempfile.TemporaryDirectory() as root:
                row = synthetic_row(root)
                row["diagnostic_log_detail"] = detail
                scope = "spectre.private_netlist"
                cause = {"message": original, "advisory": notice, "failure_stage": "native_netlist_preparation"}
                row["case_results"][scope].update(state="failed", first_cause=cause)
                row["case_contexts"][scope].update(state="failed", first_cause=cause)
                for backend in reports.ENGINES:
                    current_scope = "spectre." + backend
                    run_root = row["case_contexts"][current_scope]["run_root"]
                    os.mkdir(run_root)
                    # A historical Template run log must still show up in a new report.
                    stage = "template_variable_notice" if backend == "private_netlist" else VARIABLE_NOTICE_STAGE
                    append_run_event(run_root, stage, variable_selection_notice(backend, 51),
                                     details={"selected_variables": 51, "threshold": 50}, level="WARN")
                    row["case_records"][current_scope] = collect_run_diagnostics(run_root, backend)
                report = render_source(inspect.getsource(reports), row, synthetic_focus(row))
                lines = report.splitlines()
                index = next(i for i, line in enumerate(lines) if line.startswith("2. ") and original in line)
                self.assertIn(notice, lines[index + 1:index + 3])
                self.assertRegex(report, r'variable_selection_notice[^\n]+"level"\s*:\s*"WARN"')
                self.assertRegex(report, r'"threshold"\s*:\s*50')
                for backend in reports.ENGINES:
                    self.assertIn(variable_selection_notice(backend, 51), report)

    def test_focused_summary_matches_every_sidecar_status_criterion_and_measurement(self):
        def compare(summary, complete, path, examples):
            with self.subTest(field=path):
                if isinstance(complete, dict):
                    self.assertEqual(set(summary), set(complete))
                    for key, value in complete.items():
                        if key == 'generations' and isinstance(value, list):
                            self.assertEqual(len(summary[key]), len(value))
                            for index, item in enumerate(value):
                                compare(summary[key][index], item, path + '.' + key + str(index), examples)
                        else:
                            compare(summary[key], value, path + '.' + key, examples)
                elif isinstance(complete, list):
                    self.assertEqual(summary['count'], len(complete))
                    self.assertEqual(len(summary['examples']), min(examples, len(complete)))
                    for index, item in enumerate(complete[:examples]):
                        compare(summary['examples'][index], item, path + str(index), examples)
                    if path.endswith('.unpaired'):
                        expected = collections.Counter((item['output'], item['reason']) for item in complete)
                        self.assertEqual({(g['output'], g['reason']): g['count'] for g in summary['groups']}, dict(expected))
                else:
                    self.assertEqual(summary, complete)

        for examples in (0, 3, 20):
            with tempfile.TemporaryDirectory() as root:
                row = synthetic_row(root)
                row['diagnostic_log_examples'] = examples
                text = render_source(inspect.getsource(reports), row, synthetic_focus(row))
                complete = read_evidence(row['report_sidecars'][0]['path'])['focused_checks']
                summary = structured(text, 'focused_checks_summary')[0]
                self.assertEqual(set(complete), {'F1', 'F2', 'F3', 'F4'})
                compare(summary, complete, 'focused_checks', examples)

    def test_example_range_rejected_by_config_save_load_and_spec_before_start(self):
        from crescendo_engine.config import load_config
        for invalid in (-1, 21, True, 1.5, 'bad', '2'):
            with self.subTest(runtime_or_spec=invalid), tempfile.TemporaryDirectory() as root:
                path = os.path.join(root, 'config.json')
                with open(path, 'w', encoding='utf-8') as stream:
                    json.dump({'runtime': {'diagnostic_log_examples': invalid}}, stream)
                with mock.patch('crescendo_engine.config.discover_eda_environment') as discover:
                    with self.assertRaisesRegex(ValueError, r'diagnostic_log_examples.*0\.\.20'):
                        load_config(path, persist_discovery=False)
                    discover.assert_not_called()
                manager = reports.EnvironmentTestManager(mock.Mock(config={'runtime': {}}))
                with self.assertRaisesRegex(ValueError, r'diagnostic_log_examples.*0\.\.20'):
                    manager.start('fixture', {'diagnostic_log_examples': invalid})
                self.assertEqual(manager.tests, {})
        for invalid in (-1, 21, '-1', '21', True, 1.5, 'bad', '2.0', ''):
            with self.subTest(configuration=invalid):
                with self.assertRaisesRegex(ValueError, r'diagnostic_log_examples.*0\.\.20'):
                    build_configuration({'optimization': {'diagnostic_log_examples': invalid}})
                document = build_configuration({})
                document['settings']['optimization']['diagnostic_log_examples'] = invalid
                with self.assertRaisesRegex(ValueError, r'diagnostic_log_examples.*0\.\.20'):
                    normalize_configuration(document)
        for valid in (0, 20):
            self.assertEqual(report_options({'diagnostic_log_examples': valid}), ('compact', valid))
            document = build_configuration({'optimization': {'diagnostic_log_examples': str(valid)}})
            self.assertEqual(normalize_configuration(document)['settings']['optimization']['diagnostic_log_examples'], str(valid))

    def test_duplicate_first_cause_shares_json_and_copy_ref_but_each_verdict_is_readable(self):
        from crescendo_engine.diagnosis_catalog import read_section
        with tempfile.TemporaryDirectory() as root:
            row = synthetic_row(root)
            cause = {'message': 'Shared frozen first cause fixture', 'failure_stage': 'readback'}
            for case in row['case_results'].values():
                case.update(state='failed', first_cause=copy.deepcopy(cause))
            text = render_source(inspect.getsource(reports), row, synthetic_focus(row))
            # One structured cause plus one readable verdict per mode.
            self.assertEqual(text.count(cause['message']), 4)
            cases = structured(text, 'diagnostic_overview')[0]['cases']
            self.assertEqual(cases[0]['first_cause'], cause)
            self.assertEqual(cases[1]['first_cause_ref'], cases[0]['case_id'])
            with open(row['main_log'], 'w', encoding='utf-8') as stream:
                stream.write(text)
            copied = read_section(row['main_log'], cases[1]['case_id'], 'scheduler')
            self.assertIn(cause['message'], copied)
            self.assertIn('sidecar_manifest ', copied)
            self.assertIn(row['report_sidecars'][0]['sha256'], copied)
            self.assertEqual(read_evidence(row['report_sidecars'][0]['path'])['case_results'], row['case_results'])

    def test_options_and_unified_configuration_round_trip(self):
        self.assertEqual(report_options({}), ('compact', 3))
        self.assertEqual(report_options({'diagnostic_log_detail': 'full'}, {'diagnostic_log_detail': 'compact', 'diagnostic_log_examples': 0}), ('compact', 0))
        for options in ({'diagnostic_log_detail': 'verbose'}, {'diagnostic_log_examples': True}, {'diagnostic_log_examples': -1}):
            with self.assertRaises(ValueError):
                report_options(options)
        document = build_configuration({'optimization': {'diagnostic_log_detail': 'full', 'diagnostic_log_examples': '2'}})
        normalized = normalize_configuration(document)
        optimization = normalized['settings']['optimization']
        self.assertEqual(optimization['diagnostic_log_detail'], 'full')
        self.assertEqual(optimization['diagnostic_log_examples'], '2')


class DiagnosticSidecarIntegrationTests(unittest.TestCase):
    def run_fixture(self, root, detail='compact'):
        from tests.test_environment_test import FixtureService
        service = FixtureService(root)
        manager = reports.EnvironmentTestManager(service)
        service.manager = manager
        spec = {'variables': [{'parameter_id': 'x', 'minimum': 1e-6, 'maximum': 2e-6}],
                'objectives': [{'output_key': 'main::gain', 'direction': 'maximize'}],
                'simulator': 'spectre', 'diagnostic_log_detail': detail}
        with mock.patch('crescendo_engine.environment_test._case_capability', return_value={'ready': True}), \
             mock.patch('crescendo_engine.environment_test.run_environment_probes', return_value=[]), \
             mock.patch('crescendo_engine.environment_test.query_scheduler_jobs', return_value=[]):
            identifier = manager.start('prepared-fixture', spec)['test_id']
            self.assertTrue(manager.join(30))
            self.assertFalse(service.thread_errors)
        return manager, identifier, manager.progress(identifier)

    def test_ready_payload_manifest_and_fresh_integrity_audit(self):
        with tempfile.TemporaryDirectory() as root:
            manager, identifier, payload = self.run_fixture(root)
            self.assertTrue(payload['report_ready'])
            self.assertEqual(payload['sidecar_integrity']['status'], 'PASS')
            self.assertEqual(len(payload['report_sidecars']), 1)
            item = payload['report_sidecars'][0]
            self.assertNotIn('roots', item)
            self.assertEqual(read_evidence(item['path'])['focused_checks'], json.loads(json.dumps(payload['focused_checks'])))
            with open(item['path'], 'ab') as stream:
                stream.write(b'\n')
            self.assertEqual(manager.verify_report_evidence(identifier)['status'], 'FAIL')
            audited = manager.progress(identifier)
            self.assertTrue(audited['report_ready'])
            self.assertEqual(audited['report_state']['status'], 'ready_with_warnings')
            self.assertEqual(audited['report_state']['warnings'][0]['code'], 'sidecar_integrity')
            for key in ('state', 'checks', 'summary', 'focused_checks', 'case_results', 'gate_progress'):
                self.assertEqual(audited[key], payload[key])
            os.unlink(item['path'])
            self.assertEqual(manager.verify_report_evidence(identifier)['status'], 'FAIL')

    def test_writer_integrity_failure_is_visible_without_changing_diagnostic_verdict(self):
        real_verify = verify_sidecars
        def corrupt_then_verify(manifest):
            with open(manifest[0]['path'], 'ab') as stream:
                stream.write(b'\n')
            return real_verify(manifest)
        with tempfile.TemporaryDirectory() as root, \
             mock.patch('crescendo_engine.diagnostic_log.verify_sidecars', side_effect=corrupt_then_verify):
            manager, identifier, payload = self.run_fixture(root)
            self.assertTrue(payload['report_ready'])
            self.assertEqual(payload['sidecar_integrity']['status'], 'FAIL')
            self.assertEqual(payload['report_state']['status'], 'ready_with_warnings')
            self.assertIsNone(payload['report_write_error'])
            self.assertFalse(any(check['id'] in ('report.write', 'report.sidecar_integrity') for check in payload['checks']))
            with open(payload['main_log'], encoding='utf-8') as stream:
                text = stream.read()
            delivery = structured(text, 'report_delivery_status')[0]
            self.assertEqual(delivery, payload['report_state'])
            self.assertEqual(structured(text, 'final_checklist')[0]['state'], payload['state'])
            self.assertEqual({c['id']: c['status'] for c in structured(text, 'final_checklist')[0]['final_checklist']},
                             {c['id']: c['status'] for c in payload['checks']})

    def test_storage_error_finishes_writer_without_report_ready(self):
        with tempfile.TemporaryDirectory() as root, \
             mock.patch('crescendo_engine.diagnostic_log.EvidenceStore', side_effect=OSError('fixture evidence write failure')):
            manager, identifier, payload = self.run_fixture(root)
            self.assertFalse(payload['report_ready'])
            self.assertIn('fixture evidence write failure', payload['report_write_error'])
            self.assertEqual(next(c for c in payload['checks'] if c['id'] == 'report.write')['status'], 'FAIL')

    def test_spec_full_override_does_not_create_sidecars(self):
        with tempfile.TemporaryDirectory() as root:
            manager, identifier, payload = self.run_fixture(root, 'full')
            self.assertTrue(payload['report_ready'])
            self.assertEqual(payload['report_sidecars'], [])
            with open(payload['main_log'], encoding='utf-8') as stream:
                text = stream.read()
            self.assertIn('evidence ', text)
            self.assertNotIn('sidecar_manifest ', text)
