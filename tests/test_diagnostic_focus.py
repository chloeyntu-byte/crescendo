"""Focused checks are passive and consume only the current diagnostic evidence."""
import copy
import json
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine.diagnostic_focus import (checkpoint_check, consistency_check,
    volume_check, prepared_statistics, setup_check, raw_output_points, encoded, focused_checks)
from crescendo_engine.diagnostic_sections import (collect_sections, register_diagnostic_section,
    SECTION_LIMIT, TOTAL_LIMIT)
from crescendo_engine.diagnostic_timing import DiagnosticTiming, generation_timing_summary
from crescendo_engine.gui_timing import GuiHeartbeat
from crescendo_engine.metrics import evaluate_metrics
from crescendo_engine.run_data_store import RunDataStore, RunDataError


class FocusTests(unittest.TestCase):
    def generation(self, scans=1, seconds=1., verified=True):
        return {'generation': 1, 'storage_operations': {
            'dataset_prefix_scan': {'calls': scans, 'bytes': 100 * scans},
            'checkpoint_transaction': {'seconds': seconds}},
            'prefix_scans': [{'bytes': 100, 'file_size': 100}] * scans,
            'generation_commit': [{'verified': verified}]}

    def test_checkpoint_status_boundaries(self):
        first = self.generation()
        self.assertEqual(checkpoint_check([])['status'], 'NOT_TESTED')
        self.assertEqual(checkpoint_check([first])['status'], 'PASS')
        report = checkpoint_check([first, self.generation(seconds=2)])
        self.assertEqual(report['status'], 'PASS')
        self.assertEqual(report['last_first_transaction_ratio'], 2)
        self.assertEqual(report['generations'][0]['scan_bytes_over_file_size'], [1.])
        self.assertEqual(checkpoint_check([first, self.generation(seconds=2.00001)])['status'], 'WARN')
        self.assertEqual(checkpoint_check([self.generation(scans=2)])['status'], 'WARN')
        self.assertEqual(checkpoint_check([self.generation(verified=False)])['status'], 'FAIL')
        self.assertEqual(checkpoint_check([self.generation(seconds=0), first])['status'], 'WARN')
        missing = self.generation(); missing['generation_commit'] = []
        self.assertEqual(checkpoint_check([missing])['status'], 'NOT_TESTED')

    def cases(self, value=0.):
        row = {'candidate_id': 'g000_c0000', 'status': 'completed', 'values': {'x': 1},
               'metrics': {'feasible': True, 'constraints': [], 'diagnostic_raw_points': [
                   {'test': 'main', 'corner': 'TT', 'output': 'main::v', 'value': value, 'unit': 'V', 'point_id': 1}]}}
        return {m: [copy.deepcopy(row)] for m in ('Native Run', 'Template Netlist', 'Maestro Netlist')}

    def test_consistency_pairing_absolute_and_relative_boundaries(self):
        cases = self.cases()
        before = copy.deepcopy(cases)
        report = consistency_check(cases)
        self.assertEqual(report['status'], 'PASS')
        self.assertEqual(report['pair_count'], 3)
        self.assertEqual(cases, before)
        cases['Maestro Netlist'][0]['metrics']['diagnostic_raw_points'][0].update(value=1e-9, point_id=99)
        report = consistency_check(cases)
        self.assertEqual(report['status'], 'PASS')
        self.assertEqual(report['tolerances']['main::v']['absolute'], 1e-9)
        cases['Maestro Netlist'][0]['metrics']['diagnostic_raw_points'][0]['value'] = 1.00001e-9
        self.assertEqual(consistency_check(cases)['status'], 'FAIL')
        cases = self.cases(100.)
        cases['Maestro Netlist'][0]['metrics']['diagnostic_raw_points'][0]['value'] = 100.000099
        self.assertEqual(consistency_check(cases)['status'], 'PASS')
        cases['Maestro Netlist'][0]['metrics']['diagnostic_raw_points'][0]['value'] = 100.000101
        self.assertEqual(consistency_check(cases)['status'], 'FAIL')

    def test_consistency_missing_failed_and_decisions(self):
        for missing in ('mode', 'point', 'failed'):
            cases = self.cases()
            if missing == 'mode':
                cases['Maestro Netlist'] = []
            elif missing == 'failed':
                cases['Maestro Netlist'][0]['status'] = 'failed'
            else:
                cases['Maestro Netlist'][0]['metrics']['diagnostic_raw_points'] = []
            report = consistency_check(cases)
            self.assertEqual(report['status'], 'NOT_TESTED')
            self.assertTrue(report['unpaired'])
            self.assertEqual(report['outside_tolerance_count'], 0)
        cases = self.cases()
        cases['Maestro Netlist'][0]['metrics']['feasible'] = False
        self.assertEqual(consistency_check(cases)['status'], 'FAIL')
        cases['Maestro Netlist'][0]['metrics']['constraints'] = [{'output_key': 'main::v', 'satisfied': False}]
        self.assertTrue(consistency_check(cases)['decision_mismatches'])
        cases = self.cases()
        extra = copy.deepcopy(cases['Native Run'][0]); extra['candidate_id'] = 'g000_c0001'
        cases['Native Run'].append(extra)
        self.assertEqual(consistency_check(cases)['status'], 'WARN')
        cases['Template Netlist'][0]['values'] = {'x': 2}
        self.assertEqual(consistency_check(cases)['status'], 'NOT_TESTED')

    def test_consistency_expected_missing_points_and_constraint_decisions(self):
        cases = self.cases()
        report = consistency_check(cases, [{'key': 'main::missing'}], [('main', 'SS')])
        self.assertEqual(report['status'], 'WARN')
        self.assertEqual(report['unpaired'][0]['missing_modes'], list(cases))
        report = consistency_check({m: [] for m in cases}, expected_candidates={'g000_c0000': {'x': 1}})
        self.assertEqual(report['status'], 'NOT_TESTED')
        self.assertEqual(report['unpaired'][0]['candidate'], 'g000_c0000')
        for mode in cases:
            cases[mode][0]['metrics']['constraints'] = [{'output_key': 'main::v', 'satisfied': True}]
        cases['Native Run'][0]['metrics']['constraints'][0]['satisfied'] = False
        report = consistency_check(cases)
        self.assertEqual(report['status'], 'FAIL')
        self.assertEqual(report['decision_mismatches'][0]['constraint'][0], 'main::v')
        cases['Native Run'][0]['metrics']['constraints'] = []
        report = consistency_check(cases)
        self.assertEqual(report['status'], 'WARN')
        self.assertFalse(report['decision_mismatches'])

    def focused_row(self, setup, spec=None):
        from crescendo_engine.runner import output_catalog
        from crescendo_engine.environment_test import _selection_context
        cases = self.cases()
        return {'prepared': {'setup': setup, 'outputs': output_catalog(setup)}, 'spec': spec or {},
                'case_results': {mode: {} for mode in cases},
                'case_contexts': {'environment': {'execution_selection': _selection_context(setup, spec or {})}},
                'frozen_plan': [{'generation': 0, 'index': 0, 'values': {'x': 1}}],
                'focused_cases': {mode: {'candidates': rows} for mode, rows in cases.items()}}

    def test_focused_production_units_from_frozen_setup(self):
        from crescendo_engine.netlist_measurement import parsed_results
        setup = {'tests': [{'name': 'main', 'outputs': [
            {'name': 'v', 'expression': 'fixture_voltage', 'unit': 'V'}]}]}
        row = self.focused_row(setup)
        # Exercise the real measurement -> metrics path, both of which currently
        # omit units, rather than handcrafting unit-bearing raw points.
        for mode, value in zip(row['focused_cases'], [0., 5e-10, 0.]):
            parsed = parsed_results([{
                'task': {'test': 'main', 'corner': 'TT', 'point_id': 1},
                'measurement': {'outputs': [{'key': 'main::v', 'name': 'v', 'value': value, 'unit': 'V'}]}}])
            metrics = evaluate_metrics(parsed, [{'output_key': 'main::v', 'direction': 'maximize'}], [], diagnostic=True)
            row['focused_cases'][mode]['candidates'][0]['metrics'] = metrics
        before = copy.deepcopy(row)
        report = focused_checks(row)['F2']
        self.assertEqual(row, before)
        self.assertEqual(report['status'], 'PASS')
        self.assertEqual(report['tolerances']['main::v'],
                         {'unit': 'V', 'absolute': 1e-9, 'relative': 1e-6, 'unknown_unit_fallback': False,
                          'source': 'frozen_setup', 'absolute_source': 'unit absolute tolerance',
                          'actual_min': 1e-9, 'actual_max': 1e-9})
        self.assertEqual(report['maximum_deviation']['deviation'], 5e-10)
        self.assertEqual(report['maximum_deviation']['tolerance'], 1e-9)
        points = next(iter(row['focused_cases'].values()))['candidates'][0]['metrics']['diagnostic_raw_points']
        points[0]['value'] = 1e-9
        self.assertEqual(focused_checks(row)['F2']['status'], 'PASS')
        points[0]['value'] = 1.00001e-9
        self.assertEqual(focused_checks(row)['F2']['status'], 'FAIL')
        # Flat saved Setup output metadata is another supported source.
        row['prepared']['setup'] = {'outputs': [{'test': 'main', 'name': 'v', 'units': 'V'}]}
        points[0]['value'] = 5e-10
        self.assertEqual(focused_checks(row)['F2']['tolerances']['main::v']['unit'], 'V')

    def test_focused_frozen_units_are_bound_to_exact_test_output(self):
        setup = {'tests': [{'name': 'main', 'outputs': [{'name': 'v', 'unit': 'V'}]},
                           {'name': 'other', 'outputs': [{'name': 'v', 'unit': 'A'}]}]}
        row = self.focused_row(setup)
        for index, case in enumerate(row['focused_cases'].values()):
            points = case['candidates'][0]['metrics']['diagnostic_raw_points']
            points[0].update(unit='', value=5e-10 if index else 0.)
            points.append(dict(points[0], test='other', output='other::v'))
        report = focused_checks(row)['F2']
        self.assertEqual(report['status'], 'FAIL')
        self.assertEqual(report['paired_items'], 2)
        self.assertEqual(report['tolerances']['main::v']['absolute'], 1e-9)
        self.assertEqual(report['tolerances']['other::v']['absolute'], 1e-12)
        self.assertEqual({item['output'] for item in report['outside_tolerance']}, {'other::v'})

    def scoped_setup(self, disabled=()):
        return {'selected_test': 'main', 'tests': [{'name': 'main', 'enabled': True,
                    'outputs': [{'name': 'v', 'unit': 'V'}]}],
                'corners': [{'name': 'TT', 'enabled': True}, {'name': 'FF', 'enabled': True}],
                'test_corner_masks': {'main': {'known': True, 'disabled_corners': list(disabled)}}}

    def test_focused_expected_points_respect_run_corners_and_test_mask(self):
        for disabled, metric_corners in (([], ['TT']), (['FF'], None)):
            setup = self.scoped_setup(disabled)
            spec = {'selected_test': 'main', 'objectives': [
                {'output_key': 'main::v', 'direction': 'maximize', 'corners': metric_corners}]}
            row = self.focused_row(setup, spec)
            before = copy.deepcopy(row)
            report = focused_checks(row)['F2']
            self.assertEqual(row, before)
            self.assertEqual(report['status'], 'PASS', report)
            self.assertEqual(report['paired_items'], 1)
            self.assertEqual(report['unpaired'], [])
        # FF is truly selected and available: absent from all three modes still
        # must be reported, rather than dropping all expected-point checks.
        row = self.focused_row(self.scoped_setup(), {'selected_test': 'main', 'objectives': [
            {'output_key': 'main::v', 'direction': 'maximize', 'corners': ['TT', 'FF']}]})
        report = focused_checks(row)['F2']
        self.assertEqual(report['status'], 'WARN')
        self.assertEqual(report['unpaired'][0]['corner'], 'FF')
        self.assertEqual(len(report['unpaired'][0]['missing_modes']), 3)
        self.assertEqual(report['outside_tolerance_count'], 0)

    def test_focused_expected_points_respect_test_selection_and_nominal_mask(self):
        setup = self.scoped_setup(['FF'])
        setup['tests'].append({'name': 'other', 'enabled': True, 'outputs': [{'name': 'v', 'unit': 'A'}]})
        setup['test_corner_masks']['other'] = {'known': True}
        spec = {'execution_selection': {'tests': ['main']}}
        row = self.focused_row(setup, spec)
        self.assertEqual(focused_checks(row)['F2']['status'], 'PASS')
        # Default source-disabled Tests are excluded too.
        setup['tests'][1]['enabled'] = False
        row = self.focused_row(setup)
        self.assertEqual(focused_checks(row)['F2']['status'], 'PASS')
        setup['corners'] = [{'name': 'TT', 'enabled': True}, {'name': '_default', 'enabled': True}]
        for nominal, status in ((False, 'PASS'), (True, 'WARN')):
            setup['test_corner_masks']['main']['nominal_enabled'] = nominal
            report = focused_checks(self.focused_row(setup))['F2']
            self.assertEqual(report['status'], status)
            if nominal:
                self.assertEqual(report['unpaired'][0]['corner'], '_default')

    def test_focused_invalid_selection_does_not_discard_other_checks(self):
        row = self.focused_row(self.scoped_setup(), {'execution_selection': {'tests': ['missing']}})
        report = focused_checks(row)
        self.assertEqual(report['F2']['status'], 'NOT_TESTED')
        self.assertIn('Unknown', report['F2']['reason'])
        self.assertEqual(list(report), ['F1', 'F2', 'F3', 'F4', 'netlist_preparation', 'solver_transient'])
        self.assertTrue(report['netlist_preparation']['observation_only'])
        self.assertTrue(report['solver_transient']['observation_only'])

    def test_consistency_mismatch_listing_bounded_and_duplicate_points(self):
        cases = self.cases()
        for mode, rows in cases.items():
            points = rows[0]['metrics']['diagnostic_raw_points']
            points[:] = [dict(points[0], corner='C' + str(i), value=1. if mode == 'Native Run' else 2.) for i in range(30)]
        report = consistency_check(cases)
        self.assertEqual(len(report['outside_tolerance']), 20)
        self.assertEqual(report['outside_tolerance_count'], 60)
        cases['Native Run'][0]['metrics']['diagnostic_raw_points'].append(copy.deepcopy(cases['Native Run'][0]['metrics']['diagnostic_raw_points'][0]))
        # Multiple points in one corner without unique point IDs are ambiguous.
        self.assertEqual(consistency_check(cases)['status'], 'FAIL')

    def test_raw_point_retention_is_diagnostic_only(self):
        parsed = {'output_map': {'main::v': {'test': 'main', 'unit': 'V',
            'values': [{'corner': 'TT', 'point_id': 1, 'numeric': 2.}], 'summary': {'mean': 2.}}}}
        definitions = [{'output_key': 'main::v', 'direction': 'maximize'}]
        standard = evaluate_metrics(parsed, definitions, [])
        diagnostic = evaluate_metrics(parsed, definitions, [], diagnostic=True)
        self.assertNotIn('diagnostic_raw_points', standard)
        self.assertEqual(diagnostic.pop('diagnostic_raw_points'), raw_output_points(parsed))
        self.assertEqual(standard, diagnostic)

    def write_records(self, root, padding=0):
        path = os.path.join(root, 'owned.jsonl')
        observation = {'job_id': '42', 'task_id': 'p1', 'query_count': 8,
                       'diagnostic_query_states': {'UNKNOWN': 7, 'DONE': 1},
                       'samples': [{'observation': {'state': 'DONE'}}]}
        records = [{'kind': 'point', 'revision': 1, 'payload': {'candidate_id': 'c', 'task_id': 'p1',
                    'scheduler_observation': observation, 'padding': 'x' * padding}},
                   {'kind': 'candidates', 'revision': 2, 'payload': [{'candidate_id': 'c',
                    'scheduler_observation': observation, 'alias': {'scheduler_observation': observation}}]}]
        with open(path, 'wb') as stream:
            for record in records:
                stream.write(encoded(record) + b'\n')
        return path

    def test_volume_exact_bytes_duplicates_unknown_and_statuses(self):
        with tempfile.TemporaryDirectory() as root:
            path = self.write_records(root)
            report = volume_check(path)
            self.assertEqual(report['status'], 'PASS')
            self.assertEqual(report['file_bytes'], os.path.getsize(path))
            self.assertEqual(sum(n for _, n in report['records_top10']), report['file_bytes'])
            self.assertEqual(report['point_count'], 1)
            self.assertEqual(report['scheduler_observation_count'], 3)
            self.assertEqual(report['scheduler_unknown_queries'], 7)
            self.assertEqual(report['scheduler_unknown_occurrences'], 21)
            self.assertEqual(report['scheduler_duplicates'][0]['record_count'], 2)
            self.assertEqual(report['scheduler_duplicates'][0]['occurrences'], 3)
            with mock.patch('crescendo_engine.diagnostic_focus.VOLUME_LIMIT', report['bytes_per_point']):
                self.assertEqual(volume_check(path)['status'], 'PASS')
            path = self.write_records(root, padding=256 * 1024)
            self.assertEqual(volume_check(path)['status'], 'WARN')
            with open(path, 'ab') as stream:
                stream.write(b'broken')
            self.assertEqual(volume_check(path)['status'], 'FAIL')
            self.assertEqual(volume_check(os.path.join(root, 'absent'))['status'], 'NOT_TESTED')
            with open(path, 'wb') as stream:
                stream.write(encoded({'kind': 'metadata', 'payload': {}}) + b'\n')
            self.assertEqual(volume_check(path)['status'], 'NOT_TESTED')

    def synthetic_query_run(self, root, version, missing_state=False, second_log=False):
        from crescendo_engine.scheduler_audit import append_query
        from tests.test_run_data_store import row
        task_root = os.path.join(root, 'candidates', 'g000_c0000', 'tasks', 'p1')
        os.makedirs(task_root)
        store = RunDataStore(os.path.join(root, 'data'), {'spec': {}}, format_version=version)
        tracking = {'schema_version': 'crescendo.scheduler_task_tracking.v%d' % version,
                    'candidate_id': 'g000_c0000', 'task_id': 'p1', 'job_id': '42',
                    'test': 'main', 'corner': 'TT', 'point_id': 1, 'query_count': 0}
        counts = {}
        try:
            for index, state in enumerate(('PENDING', 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', 'DONE', 'DONE'), 1):
                tracking['query_count'] = index
                counts[state] = counts.get(state, 0) + 1
                if version == 2:
                    append_query(tracking, {'stdout_tail': 'STATE: ' + state}, {'state': state}, task_root)
                else:
                    tracking['diagnostic_query_states'] = dict(counts)
                # Deliberately omit the middle UNKNOWN samples, just as v2 does.
                tracking['samples'] = [{'observation': {'state': 'PENDING'}}, {'observation': {'state': state}}]
                if missing_state and index == 6:
                    tracking['query_log']['entries'][1].pop('state')
                if index in (3, 6):
                    store.append_event('point', {'candidate_id': 'g000_c0000', 'task_id': 'p1',
                        'candidate_completed': False, 'scheduler_observation': copy.deepcopy(tracking)})
            candidate = row(status='completed')
            pipeline = {'points': [{'scheduler_observation': copy.deepcopy(tracking)}]}
            candidate.update(execution_pipeline=pipeline, completion={'execution_pipeline': pipeline},
                             completion_evidence={'execution_pipeline': pipeline})
            if second_log:
                # Identical originals in a distinct owned cancellation log are
                # separate queries; aliases of either log still count once.
                other = copy.deepcopy(tracking)
                target = os.path.join(task_root, 'cancel_scheduler_queries.jsonl')
                with open(tracking['query_log']['path'], 'rb') as source, open(target, 'wb') as dest:
                    dest.write(source.read())
                other['query_log']['path'] = target
                candidate['cancel'] = {'scheduler_observation': other}
            store.commit_candidates([candidate])
            candidate['artifact_summary'] = {'raw_deleted': True}
            store.commit_candidates([candidate])
            path = store.path
        finally:
            store.close()
        # F3 must consume the durable prefix after all query sidecars are gone.
        import shutil
        shutil.rmtree(os.path.join(root, 'candidates'))
        return volume_check(path)

    def test_volume_v1_v2_query_census_and_observation_occurrences_match(self):
        reports = []
        for version in (1, 2):
            with tempfile.TemporaryDirectory() as root:
                reports.append(self.synthetic_query_run(root, version))
        for report in reports:
            self.assertEqual(report['status'], 'PASS', report)
            self.assertEqual(report['scheduler_observation_count'], 8)
            self.assertEqual(report['scheduler_unknown_occurrences'], 23)
            self.assertEqual(report['scheduler_unknown_queries'], 3)
            self.assertTrue(report['scheduler_unknown_census_complete'])
        for key in ('scheduler_unknown_occurrences', 'scheduler_unknown_queries', 'scheduler_observation_count'):
            self.assertEqual(reports[0][key], reports[1][key])

    def test_volume_v2_distinct_query_logs_do_not_merge_identical_originals(self):
        with tempfile.TemporaryDirectory() as root:
            report = self.synthetic_query_run(root, 2, second_log=True)
            self.assertEqual(report['status'], 'PASS', report)
            self.assertEqual(report['scheduler_unknown_queries'], 6)
            self.assertTrue(report['scheduler_unknown_census_complete'])

    def test_volume_v2_missing_query_state_is_incomplete(self):
        with tempfile.TemporaryDirectory() as root:
            report = self.synthetic_query_run(root, 2, missing_state=True)
            self.assertEqual(report['status'], 'PASS', report)
            self.assertIsNone(report['scheduler_unknown_queries'])
            self.assertFalse(report['scheduler_unknown_census_complete'])

    def test_volume_legacy_bounded_samples_remain_incomplete(self):
        with tempfile.TemporaryDirectory() as root:
            path = self.write_records(root)
            with open(path, 'rb') as stream:
                records = [json.loads(line.decode('utf-8')) for line in stream]
            def remove_counts(value):
                if isinstance(value, dict):
                    value.pop('diagnostic_query_states', None)
                    for child in value.values():
                        remove_counts(child)
                elif isinstance(value, list):
                    for child in value:
                        remove_counts(child)
            remove_counts(records)
            with open(path, 'wb') as stream:
                for record in records:
                    stream.write(encoded(record) + b'\n')
            report = volume_check(path)
            self.assertEqual(report['scheduler_unknown_occurrences'], 0)
            self.assertIsNone(report['scheduler_unknown_queries'])
            self.assertFalse(report['scheduler_unknown_census_complete'])

    def test_store_generation_metadata_does_not_change_verification(self):
        with tempfile.TemporaryDirectory() as root:
            store = RunDataStore(os.path.join(root, 'data.jsonl'), metadata={'fixture': True})
            observer = DiagnosticTiming(); observer.start('evaluate')
            store.persistence_timing = observer
            store.append_event('note', {'v': 1})
            store.verify_generation(0)
            self.assertEqual(observer.generation_commit, [{'generation': 1, 'verified': True}])
            self.assertEqual(observer.prefix_scans[-1]['bytes'], os.path.getsize(store.path))
            with mock.patch.object(store, '_verify_disk_prefix', side_effect=RunDataError('fixture')):
                with self.assertRaises(RunDataError):
                    store.verify_generation(1)
            self.assertFalse(observer.generation_commit[-1]['verified'])
            observer.finish()
            summary = generation_timing_summary(0, observer, [])
            self.assertEqual(summary['prefix_scans'], observer.prefix_scans)
            store.close()

    def test_setup_headless_stalls_and_unknown_format(self):
        statistics = prepared_statistics({'parameters': [{'optimizable': True}, {'optimizable': False}]})
        self.assertEqual(statistics['copies'], 1)
        self.assertEqual(statistics['parameter_count'], 2)
        self.assertEqual(statistics['optimizable_count'], 1)
        self.assertEqual(setup_check(statistics)['status'], 'NOT_TESTED')
        def gui(seconds):
            return {'windows': {'diagnostic': {'samples': 5, 'max_pause_seconds': seconds, 'over_200ms_count': 1}}}
        self.assertEqual(setup_check(statistics, gui(1.))['status'], 'PASS')
        self.assertEqual(setup_check(statistics, gui(1.00001))['status'], 'WARN')
        self.assertEqual(setup_check(statistics, dict(gui(5.), available=False))['status'], 'NOT_TESTED')
        self.assertEqual(setup_check({'error': 'fixture'}, gui(0))['status'], 'FAIL')
        unknown = prepared_statistics({'schema_version': 'future.v99'})
        self.assertFalse(unknown['available'])
        self.assertEqual(setup_check(unknown, gui(0))['status'], 'NOT_TESTED')

    def test_prepared_counts_only_explicit_current_files(self):
        with tempfile.TemporaryDirectory() as root:
            current = os.path.join(root, 'current.json')
            historical = os.path.join(root, 'prepared_old.json')
            for path, size in ((current, 10), (historical, 500)):
                with open(path, 'w') as stream:
                    stream.write('x' * size)
            statistics = prepared_statistics({'setup_baseline_snapshot': {'path': current}})
            self.assertEqual(statistics['bytes'], 10)
            self.assertEqual(statistics['copies'], 1)
            self.assertNotIn(historical, json.dumps(statistics))

    def test_prepared_v2_shared_cas_counts_logical_copies_and_references(self):
        from crescendo_engine import catalog_json, prepared_store
        with tempfile.TemporaryDirectory() as root:
            value = {'schema_version': 'crescendo.prepared.v2', 'parameters': [
                {'parameter_id': 'x', 'optimizable': True}], 'hierarchy': {}, 'setup': {}}
            baseline = os.path.join(root, 'baseline.json')
            session = os.path.join(root, 'session')
            os.mkdir(session)
            digest = 'a' * 64
            snapshot = os.path.join(session, 'prepared-' + digest + '.json')
            for path in (baseline, snapshot):
                prepared_store.write(path, value, root=root)
            # An unrelated object and historical prepared file must not count.
            prepared_store.write(os.path.join(root, 'history.json'), dict(value, unused=True), root=root)
            with open(os.path.join(session, 'session_state.json'), 'w') as stream:
                json.dump({'prepared_hash': digest}, stream)
            current = dict(value, setup_baseline_snapshot={'path': baseline},
                           setup_private_session={'root': session})
            before = copy.deepcopy(current)
            statistics = prepared_statistics(current)
            self.assertEqual(current, before)
            self.assertTrue(statistics['available'])
            self.assertEqual(statistics['copies'], 2)
            self.assertEqual(statistics['reference_count'], 2)
            self.assertEqual(statistics['cas_reference_count'], 6)
            self.assertEqual(statistics['cas_object_count'], 3)
            self.assertEqual(statistics['logical_bytes'], 2 * len(''.join(catalog_json.chunks(value)).encode('utf-8')))
            self.assertEqual(statistics['cas_bytes'], sum(os.path.getsize(o['path']) for o in statistics['cas_objects']))
            self.assertEqual(statistics['physical_bytes'], statistics['cas_bytes'] + sum(os.path.getsize(p) for p in (baseline, snapshot)))
            self.assertEqual(statistics['parameter_count'], 1)
            self.assertEqual(statistics['optimizable_count'], 1)
            # Tampering must make the census incomplete, without publishing repairs.
            obj = statistics['cas_objects'][0]['path']
            with open(obj, 'a') as stream:
                stream.write(' ')
            statistics = prepared_statistics(current)
            self.assertFalse(statistics['available'])
            self.assertTrue(statistics['omissions'])
            self.assertEqual(statistics['copies'], 0)

    def test_prepared_v2_memory_inline_missing_and_unsafe_references(self):
        from crescendo_engine import catalog_json
        value = {'schema_version': 'crescendo.prepared.v2', 'parameters': [
            {'optimizable': True}, {'optimizable': False}]}
        statistics = prepared_statistics(value)
        self.assertTrue(statistics['available'])
        self.assertEqual(statistics['logical_bytes'], len(''.join(catalog_json.chunks(value)).encode('utf-8')))
        self.assertEqual(statistics['cas_bytes'], 0)
        self.assertEqual(statistics['reference_count'], 0)
        self.assertEqual(statistics['parameter_count'], 2)
        self.assertEqual(statistics['optimizable_count'], 1)
        with tempfile.TemporaryDirectory() as root:
            inline = os.path.join(root, 'inline.json')
            with open(inline, 'w') as stream:
                json.dump(value, stream)
            current = dict(value, setup_baseline_snapshot={'path': inline})
            self.assertTrue(prepared_statistics(current)['available'])
            link = os.path.join(root, 'link.json')
            os.symlink(inline, link)
            current['setup_baseline_snapshot']['path'] = link
            self.assertFalse(prepared_statistics(current)['available'])
            current['setup_baseline_snapshot']['path'] = os.path.join(root, 'missing.json')
            self.assertFalse(prepared_statistics(current)['available'])

    def test_prepared_oversized_session_metadata_is_explicitly_incomplete(self):
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, 'session_state.json')
            with open(path, 'w') as stream:
                stream.write('x' * (1024 * 1024 + 1))
            statistics = prepared_statistics({'setup_private_session': {'root': root}})
            self.assertFalse(statistics['available'])
            self.assertIn('1 MiB', statistics['omissions'][0])
            self.assertEqual(setup_check(statistics, {'windows': {'diagnostic': {
                'samples': 1, 'max_pause_seconds': 0.}}})['status'], 'NOT_TESTED')

    def test_gui_heartbeat_windows_and_pending_stall(self):
        class Root(object):
            def after(self, ms, callback):
                self.callback = callback
                return 'tick'
        now = [0.]
        observer = GuiHeartbeat(Root(), clock=lambda: now[0])
        observer.begin('load'); observer.begin('diag')
        now[0] = .2; observer.tick()  # exact 200ms gap: not counted
        now[0] = .401; observer.tick()
        now[0] = 1.501; observer.end('load')  # pending tick included
        report = observer.diagnostic_snapshot('diag')
        self.assertAlmostEqual(report['windows']['load']['max_pause_seconds'], 1.1)
        self.assertEqual(report['windows']['load']['over_200ms_count'], 2)
        self.assertAlmostEqual(report['windows']['load']['elapsed_seconds'], 1.501)
        self.assertNotIn('diag', observer.latest)

    def test_gui_heartbeat_closes_only_its_timer_and_reports_unavailable(self):
        root = mock.Mock()
        root.after.return_value = 'owned-timer'
        observer = GuiHeartbeat(root, clock=lambda: 0.)
        observer.begin('diag')
        observer._destroyed(mock.Mock(widget=object()))
        self.assertFalse(observer.closed)
        observer._destroyed(mock.Mock(widget=root))
        root.after_cancel.assert_called_once_with('owned-timer')
        observer.begin('after-close')
        observer.tick()
        self.assertFalse(observer.active)
        self.assertFalse(observer.diagnostic_snapshot('after-close')['available'])

    def test_setup_window_includes_sync_and_completion_work_and_failure(self):
        from crescendo_engine.gui import OptimizerGui
        from crescendo_engine.gui_timing import observe_setup_action
        class Root(object):
            def after(self, ms, callback):
                return 'tick'
        now = [0.]
        app = OptimizerGui.__new__(OptimizerGui)
        app.diagnostic_heartbeat = GuiHeartbeat(Root(), clock=lambda: now[0])
        app.tasks = mock.Mock()
        @observe_setup_action('load')
        def load(app):
            now[0] = .3  # synchronous GUI setup before task submission
            app._submit_setup_observed('load', lambda: None, lambda value: now.__setitem__(0, 1.7), lambda error: None)
        load(app)
        self.assertIn('load', app.diagnostic_heartbeat.active)
        now[0] = 1.5
        app.tasks.submit.call_args[0][1](None)
        window = app.diagnostic_heartbeat.latest['load']
        self.assertEqual(window['elapsed_seconds'], 1.7)
        self.assertAlmostEqual(window['max_pause_seconds'], 1.7)
        self.assertAlmostEqual(window['max_excess_seconds'], 1.65)
        app._submit_setup_observed('apply', lambda: None, lambda value: None, lambda error: None)
        app.tasks.submit.call_args[0][2](ValueError('fixture'))
        self.assertNotIn('apply', app.diagnostic_heartbeat.active)


class SectionTests(unittest.TestCase):
    def test_order_isolation_exceptions_and_limits(self):
        def first(context):
            with self.assertRaises(TypeError):
                context['x'] = 1
            with self.assertRaises(TypeError):
                context['nested']['x'] = 2
            return {'title': 'A', 'status': 'PASS', 'body': 'x' * 20000}
        def broken(context):
            raise ValueError('fixture failure')
        registrations = [('first', first), ('broken', broken)] + [
            ('last' + str(i), lambda context: {'title': 'Z' * 300, 'status': 'WARN', 'body': 'z' * 20000}) for i in range(8)]
        sections = collect_sections({'nested': {'x': 0}}, registrations)
        self.assertEqual([r['name'] for r in sections[:2]], ['first', 'broken'])
        self.assertEqual(sections[1]['status'], 'WARN')
        self.assertIn('fixture failure', sections[1]['body'])
        self.assertIn('[truncated]', sections[0]['body'])
        self.assertTrue(all(len(s['title']) + len(s['body']) <= SECTION_LIMIT for s in sections))
        self.assertLessEqual(sum(len(s['title']) + len(s['body']) for s in sections), TOTAL_LIMIT)
        self.assertIn('total section limit', sections[-1]['body'])

    def test_total_limit_smaller_than_marker_has_explicit_metadata(self):
        registrations = [(str(i), lambda context: {'title': 'Long title', 'status': 'PASS', 'body': 'Long body'})
                         for i in range(2)]
        with mock.patch('crescendo_engine.diagnostic_sections.TOTAL_LIMIT', 1):
            sections = collect_sections({}, registrations)
        self.assertEqual(len(sections), 1)
        self.assertTrue(sections[0]['truncated'])
        self.assertEqual(sections[0]['omitted_sections'], 1)
        self.assertLessEqual(len(sections[0]['title']) + len(sections[0]['body']), 1)

    def test_redaction_expansion_is_bounded_before_publication(self):
        from crescendo_engine.environment_probes import _redact
        registrations = [('fixture', lambda context: {'title': 'Receipt', 'status': 'PASS',
                         'body': '12@a ' * 10000})]
        sections = collect_sections({}, registrations, redactor=lambda value: _redact(value, {}))
        self.assertNotIn('12@a', sections[0]['body'])
        self.assertTrue(sections[0]['truncated'])
        self.assertLessEqual(len(sections[0]['title']) + len(sections[0]['body']), SECTION_LIMIT)

    def test_registration_duplicate_and_invalid_status(self):
        with mock.patch('crescendo_engine.diagnostic_sections._sections', new={}):
            callback = lambda context: {'title': 'x', 'status': 'INVALID', 'body': 'x'}
            register_diagnostic_section('fixture', callback)
            with self.assertRaises(ValueError):
                register_diagnostic_section('fixture', callback)
            self.assertEqual(collect_sections({})[0]['status'], 'WARN')
            with self.assertRaises(ValueError):
                register_diagnostic_section('', callback)


class FocusReportEndToEndTests(unittest.TestCase):
    def test_fake_diagnostic_exports_focus_and_sections_in_order_to_ready_event(self):
        from test_environment_test import FixtureService
        from crescendo_engine.environment_test import EnvironmentTestManager
        from crescendo_engine.diagnostic_actions import DiagnosticAction, DiagnosticActions
        # Add real synthetic point values to the fake parsed results (which
        # otherwise intentionally contain only means). No native EDA is launched.
        original_metrics = evaluate_metrics
        def metrics(parsed, objectives, constraints):
            parsed = copy.deepcopy(parsed)
            for key, output in parsed['output_map'].items():
                output['test'] = key.split('::')[0]
                output['values'] = [{'corner': 'TT', 'point_id': 1, 'numeric': output['summary']['mean']}]
            return original_metrics(parsed, objectives, constraints, diagnostic=True)
        def broken(context):
            raise ValueError('fixture section error')
        registrations = (('provider-one', lambda context: {'title': 'Fictional provider', 'status': 'FAIL', 'body': 'fixture-only section'}),
                         ('provider-two', broken))
        with tempfile.TemporaryDirectory() as root:
            service = FixtureService(root)
            manager = EnvironmentTestManager(service); service.manager = manager
            spec = {'variables': [{'parameter_id': 'x', 'minimum': 1e-6, 'maximum': 2e-6}],
                    'objectives': [{'output_key': 'main::gain', 'direction': 'maximize'}], 'simulator': 'spectre'}
            with mock.patch('test_environment_test.evaluate_metrics', side_effect=metrics), \
                 mock.patch('crescendo_engine.diagnostic_sections.registered_sections', return_value=registrations), \
                 mock.patch('crescendo_engine.environment_test._case_capability', return_value={'ready': True}), \
                 mock.patch('crescendo_engine.environment_test.run_environment_probes', return_value=[]), \
                 mock.patch('crescendo_engine.environment_test.query_scheduler_jobs', return_value=[]):
                identifier = manager.start('prepared-fixture', spec)['test_id']
                self.assertTrue(manager.join(30))
                payload = manager.progress(identifier)
            self.assertTrue(payload['report_ready'])
            self.assertEqual(payload['state'], 'completed', [(c['id'], c.get('message')) for c in payload['checks'] if c['status'] == 'FAIL'])
            self.assertEqual(payload['focused_checks']['F2']['status'], 'PASS')
            self.assertEqual(payload['focused_checks']['F3']['status'], 'PASS')
            self.assertEqual(payload['focused_checks']['F4']['status'], 'NOT_TESTED')
            self.assertEqual([s['name'] for s in payload['diagnostic_sections']], ['provider-one', 'provider-two'])
            self.assertEqual(payload['diagnostic_sections'][1]['status'], 'WARN')
            self.assertFalse(any('provider-one' in c['id'] for c in payload['checks']))
            with open(payload['main_log'], encoding='utf-8') as stream:
                exported = stream.read()
            overview = exported.index('diagnostic_overview ')
            focus = exported.index('SECTION BEGIN focused_checks')
            extension = exported.index('diagnostic_section ')
            detail = exported.index('SECTION BEGIN ', focus + 1)
            self.assertLess(overview, focus); self.assertLess(focus, extension); self.assertLess(extension, detail)
            self.assertIn('fixture-only section', exported)
            # The existing report-ready event carries the same frozen payload.
            app = mock.Mock()
            event = mock.Mock()
            action = DiagnosticAction('copy', 'Copy', lambda snapshot: None, on_report_ready=event)
            actions = DiagnosticActions(app, [action]); actions.progress(payload)
            received = event.call_args[0][0]
            self.assertEqual(received['diagnostic_sections'][0]['body'], 'fixture-only section')
            self.assertEqual(received['focused_checks']['F2']['status'], 'PASS')
            with self.assertRaises(TypeError):
                received['diagnostic_sections'][0]['body'] = 'changed'


if __name__ == '__main__':
    unittest.main()
