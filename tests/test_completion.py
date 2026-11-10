from __future__ import print_function

import json
import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from crescendo_engine.completion import (
    CompletionEvidenceError, UnsupportedCompletionEvidence, validate_candidate_completion,
    _validate_native_effective_scope, prepare_native_point_parameter_requests,
)
from crescendo_engine.retention import (
    UnsafeCleanupError, cleanup_abandoned_candidate, lightweight_candidate_record, persist_lightweight_candidate,
    safe_delete_candidate_workspace,
)
from crescendo_engine.workspace import _history_identity, write_batch_files


class CompletionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.run_root = self.temp.name
        self.candidate_root = os.path.join(self.run_root, "candidates", "g000_c0000")
        self.workspace_root = os.path.join(self.candidate_root, "workspace")
        self.simulation_root = os.path.join(self.workspace_root, "simulation")
        self.setup_dir = os.path.join(self.workspace_root, "libraries", "D", "TB", "maestro")
        os.makedirs(self.setup_dir)
        self.setup = {
            "tests": [{
                "name": "ac_test", "enabled": True, "library": "D", "cell": "TB",
                "outputs": [{"name": "gain"}],
            }],
            "corners": [{"name": "FF", "enabled": True}, {"name": "SS", "enabled": True}],
        }
        self.workspace = {
            "root": self.workspace_root,
            "simulation_root": self.simulation_root,
            "setup_dir": self.setup_dir,
            "setup": self.setup,
            "primary_library": "D",
            "setup_cell": "TB",
        }
        self.history = "CRESCENDO_g000_c0000_attempt_1_0123456789"
        self.rdb_path = os.path.join(self.setup_dir, "results", "maestro", self.history + ".rdb")
        os.makedirs(os.path.dirname(self.rdb_path))
        # Scope qualification reads the exact native point/Test metadata. Keep
        # this artifact a real read-only SQLite fixture, not an opaque token.
        with sqlite3.connect(self.rdb_path) as connection:
            connection.executescript('''
                CREATE TABLE test(testID INTEGER PRIMARY KEY, name TEXT);
                CREATE TABLE point(pointID INTEGER PRIMARY KEY, designPointNumber INTEGER);
                CREATE TABLE parameter(parameterID INTEGER PRIMARY KEY, name TEXT, type TEXT,
                                       testID INTEGER, hasLocal TEXT);
                CREATE TABLE parameterValue(pointID INTEGER, parameterID INTEGER, value NUMERIC);
                CREATE TABLE localParameterValue(pointID INTEGER, parameterID INTEGER,
                                                testID INTEGER, value NUMERIC);
                INSERT INTO test VALUES(1, 'ac_test');
                INSERT INTO point VALUES(1, 1), (2, 2);
            ''')
        self.parsed = {
            "path": self.rdb_path,
            "points": [
                {"pointID": 1, "designPointNumber": 1, "corner_name": "FF"},
                {"pointID": 2, "designPointNumber": 2, "corner_name": "SS"},
            ],
            "test_statuses": [
                {"pointID": 1, "test_name": "ac_test", "statusCode": 3, "status_message": "done"},
                {"pointID": 2, "test_name": "ac_test", "statusCode": 3, "status_message": "done"},
            ],
            "outputs": [],
            "output_map": {
                "ac_test::gain": {
                    "test": "ac_test", "name": "gain", "scalar": True,
                    "values": [
                        {"point_id": 1, "corner": "FF", "numeric": 10.0, "error": None},
                        {"point_id": 2, "corner": "SS", "numeric": 8.0, "error": None},
                    ],
                },
            },
        }
        for point in (1, 2):
            psf = os.path.join(
                self.simulation_root, "D", "TB", "maestro", "results", "maestro",
                self.history, str(point), "ac_test", "psf",
            )
            os.makedirs(psf)
            open(os.path.join(psf, ".simDone"), "w").close()
            with open(os.path.join(psf, "spectre.out"), "w") as stream:
                stream.write("spectre completes with 0 errors, 2 warnings, and 1 notices.\n")
        self.command = {
            "returncode": 0, "timed_out": False, "startup_timed_out": False,
            "history_name": self.history,
        }

    def tearDown(self):
        self.temp.cleanup()

    def _native_rdb_values(self, rows):
        """Write authoritative fixture values independently of the live SCS."""
        with sqlite3.connect(self.rdb_path) as connection:
            for row in rows:
                point, name = row['pointID'], row['name']
                connection.execute('INSERT OR IGNORE INTO point VALUES(?, ?)', (point, point))
                found = connection.execute('SELECT parameterID FROM parameter WHERE name=?', (name,)).fetchone()
                if found is None:
                    cursor = connection.execute("INSERT INTO parameter(name,type,testID,hasLocal) VALUES(?, 'desVar', '', 'false')", (name,))
                    parameter_id = cursor.lastrowid
                else:
                    parameter_id = found[0]
                connection.execute('DELETE FROM parameterValue WHERE pointID=? AND parameterID=?', (point, parameter_id))
                connection.execute('INSERT INTO parameterValue VALUES(?, ?, ?)', (point, parameter_id, row['value']))

    def test_validates_bound_history_points_corners_outputs_and_spectre(self):
        result = validate_candidate_completion(
            self.workspace, self.candidate_root, self.command, self.parsed,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["history_name"], self.history)
        self.assertEqual(result["corners"], ["FF", "SS"])
        self.assertEqual(len(result["spectre_runs"]), 2)
        self.assertFalse(os.path.isabs(result["rdb"]["path"]))
        with open(os.path.join(self.candidate_root, "completion_evidence.json")) as stream:
            persisted = json.load(stream)
        self.assertEqual(persisted["adapter"], "maestro-rdb-psf-v1")

    def _native_sweep_identity(self, values=(0, 1)):
        self.setup['corners'] = [{'name': 'Sweep', 'enabled': True,
                                 'variables': [{'name': 'SW', 'value': '0 1'}]}]
        self.workspace['native_corner_identity'] = {
            'setup_snapshot': {'ok': True, 'saved_source': {'corner_read_ok': True},
                               'private_setup': {'corner_read_ok': True}},
            'schedule': {'read_ok': True, 'groups': {'Sweep': {'enabled': True, 'count': 2}},
                         'nominal_enabled': False, 'nominal_tests': {'ac_test': False}}}
        # A shared live SCS can contain the final value while the current RDB
        # and exact point PSF still represent the original 0/1 expansion.
        self._native_rdb_values([{'pointID': point, 'name': 'SW', 'value': value}
                                 for point, value in enumerate((0, 1), 1)])
        for point, value in enumerate(values, 1):
            name = 'opaque_native_%s' % point
            self.parsed['points'][point - 1]['corner_name'] = name
            self.parsed['output_map']['ac_test::gain']['values'][point - 1]['corner'] = name
            directory = os.path.join(self.simulation_root, 'D', 'TB', 'maestro', 'results', 'maestro',
                                     self.history, str(point), 'ac_test', 'netlist')
            os.makedirs(directory)
            with open(os.path.join(directory, 'input.scs'), 'w') as stream:
                stream.write('parameters SW=%s Fref=64M\ninclude "/pdk/model.scs" section=tt\nsimulatorOptions options temp=27\n' % value)

    def _native_model_identity(self, mode='include_path'):
        self._native_sweep_identity()
        source = os.path.join(self.run_root, 'source project')
        pdk = os.path.join(self.run_root, 'PDK models')
        os.makedirs(source)
        os.makedirs(pdk)
        model = os.path.join(source if mode == 'symlink' else pdk, 'toplevel.scs')
        with open(model, 'w') as stream:
            stream.write('// Authoritative model fixture\nparameters model_revision=1\n')
        sections = ['TOP_TT_RFTYP', 'Pre_Sim', 'Noise_Worst']
        self.setup['corners'][0]['model_groups'] = ['TT']
        self.setup['model_groups'] = [{'name': 'TT', 'models': [
            {'file': 'toplevel.scs', 'section': section, 'test': 'All'} for section in sections]}]
        self.workspace['native_corner_identity']['setup_snapshot']['model_path_context'] = {'source_work_dir': source}
        directories = []
        for point in (1, 2):
            directory = os.path.join(self.simulation_root, 'D', 'TB', 'maestro', 'results', 'maestro',
                                     self.history, str(point), 'ac_test', 'netlist')
            directories.append(directory)
            with open(os.path.join(directory, 'input.scs'), 'w') as stream:
                stream.write('parameters SW=%s\nsimulatorOptions options temp=55\n' % (point - 1))
                stream.writelines('include "toplevel.scs" section=%s\n' % section for section in sections)
            with open(os.path.join(directory, 'runSimulation'), 'w') as stream:
                stream.write('spectre input.scs +log ../psf/spectre.out -I "%s"\n' % pdk)
            if mode == 'symlink':
                os.symlink(model, os.path.join(directory, 'toplevel.scs'))
        return source, pdk, model, directories

    def test_completed_models_follow_exact_point_native_include_search(self):
        source, pdk, model, directories = self._native_model_identity()
        result = validate_candidate_completion(self.workspace, self.candidate_root, self.command, self.parsed)
        self.assertTrue(result['ok'])
        with open(os.path.join(self.candidate_root, 'point_identity.json')) as stream:
            points = json.load(stream)['points']
        for point in points:
            resolution = point['native_model_resolution']
            self.assertEqual(resolution['include_dirs'], [pdk])
            self.assertEqual(resolution['source_references'][0]['physical_path'], model)
            self.assertEqual([row['physical_path'] for row in resolution['references']], [model] * 3)
            self.assertEqual(resolution['argv_evidence']['basis'], 'exact_point_native_runSimulation')
            self.assertEqual(len(resolution['argv_evidence']['sha256']), 64)

    def test_completed_native_IC618_loader_preamble_keeps_literal_model_search(self):
        source, pdk, model, directories = self._native_model_identity()
        for directory in directories:
            with open(os.path.join(directory, 'runSimulation'), 'w') as stream:
                stream.write('#!/bin/sh\n# runSimulation scripts generated for spectre interactive\n'
                    'export LD_PRELOAD=/opt/eda/cadence/IC618/tools.lnx86/dfII/../lib/64bit/libstdc++.so:$LD_PRELOAD\n'
                    ' spectre input.scs +escchars +log ../psf/spectre.out -maxnotestolog 1000 -64 '
                    '-plugin /opt/eda/cadence/IC618/lib/64bit/simiADEPlugin.so '
                    '+mpssession=spectre0_105769_1 -inter -format psfxl -raw ../psf '
                    '+mpshost=eda +lqtimeout 900 -maxw 5 -maxn 5 -I "%s" +logstatus\n' % pdk)
        before = os.environ.get('LD_PRELOAD')
        result = validate_candidate_completion(self.workspace, self.candidate_root, self.command, self.parsed)
        self.assertTrue(result['ok'])
        self.assertEqual(os.environ.get('LD_PRELOAD'), before)
        for point in result['points']:
            resolution = point['native_model_resolution']
            self.assertEqual(resolution['include_dirs'], [pdk])
            self.assertEqual(resolution['references'][0]['physical_path'], model)
            evidence = resolution['argv_evidence']
            self.assertEqual(evidence['basis'], 'exact_point_native_runSimulation')
            self.assertEqual(evidence['ignored_environment_exports'][0]['name'], 'LD_PRELOAD')

    def test_completed_native_preamble_does_not_authorize_shell_model_search(self):
        source, pdk, model, directories = self._native_model_identity()
        for script in ('export PDK="%s"\nspectre input.scs -I "$PDK"\n' % pdk,
                       'export LD_PRELOAD=/loader.so:$LD_PRELOAD\nspectre input.scs -I "$PDK"\n',
                       'export LD_PRELOAD=/loader.so:$LD_PRELOAD\nspectre input.scs -I "${PDK}"\n',
                       'export LD_PRELOAD=/loader.so:$LD_PRELOAD\nspectre input.scs -I /models/*\n',
                       'export LD_PRELOAD=/loader.so:$LD_PRELOAD\ncd /other\nspectre input.scs\n',
                       'export LD_PRELOAD=$(touch /not-executed)\nspectre input.scs\n',
                       'export LD_PRELOAD=/loader.so:$LD_PRELOAD\nsh -c "spectre input.scs"\n'):
            with self.subTest(script=script):
                with open(os.path.join(directories[0], 'runSimulation'), 'w') as stream:
                    stream.write(script)
                with self.assertRaisesRegex(CompletionEvidenceError, 'corner/point coverage'):
                    validate_candidate_completion(self.workspace, self.candidate_root, self.command, self.parsed)
                with open(os.path.join(self.candidate_root, 'point_identity.json')) as stream:
                    points = json.load(stream)['points']
                self.assertTrue(points[0]['native_model_resolution']['errors'])

    def test_completed_log_shape_TT_55_three_sections_uses_model_resolution(self):
        source, pdk, model, directories = self._native_model_identity()
        self.setup['corners'] = [{'name': 'TT', 'enabled': True, 'model_groups': ['TT'],
                                 'variables': [{'name': 'temperature', 'value': '55'}]}]
        self.workspace['native_corner_identity']['schedule']['groups'] = {'TT': {'enabled': True, 'count': 1}}
        self.workspace['native_corner_identity']['point_parameters_required'] = True
        self.parsed['points'] = [dict(self.parsed['points'][0], corner_name='TT')]
        self.parsed['test_statuses'] = self.parsed['test_statuses'][:1]
        self.parsed['output_map']['ac_test::gain']['values'] = [
            dict(self.parsed['output_map']['ac_test::gain']['values'][0], corner='TT')]
        result = validate_candidate_completion(self.workspace, self.candidate_root, self.command, self.parsed,
                                               native_parameter_reader=lambda requests: [{'temperature': 55}])
        self.assertTrue(result['ok'])
        point = result['points'][0]
        self.assertEqual(point['source_corner'], 'TT')
        self.assertEqual(point['temperature']['value'], 55)
        self.assertEqual(point['param_source'], 'current_psf_designParamVals')
        self.assertEqual([row['physical_path'] for row in point['native_model_resolution']['references']], [model] * 3)

    def test_completed_models_follow_source_symlink_with_no_run_script(self):
        source, pdk, model, directories = self._native_model_identity('symlink')
        for directory in directories:
            os.unlink(os.path.join(directory, 'runSimulation'))
        result = validate_candidate_completion(self.workspace, self.candidate_root, self.command, self.parsed)
        self.assertTrue(result['ok'])
        self.assertEqual(result['points'][0]['native_model_resolution']['references'][0]['physical_path'], model)

    def test_completed_model_search_keeps_configured_source_directory(self):
        source, pdk, model, directories = self._native_model_identity()
        for directory in directories:
            os.unlink(os.path.join(directory, 'runSimulation'))
        result = validate_candidate_completion(self.workspace, self.candidate_root, self.command, self.parsed,
                                               config={'cadence': {'spectre_model_dir': pdk}},
                                               native_parameter_reader=lambda requests: [{'SW': 0}, {'SW': 1}])
        self.assertTrue(result['ok'])

    def test_completed_model_shadow_and_unproved_copy_remain_rejected(self):
        source, pdk, model, directories = self._native_model_identity()
        for content in ('// Different model\n', '// Authoritative model fixture\nparameters model_revision=1\n'):
            with self.subTest(content=content):
                with open(os.path.join(directories[0], 'toplevel.scs'), 'w') as stream:
                    stream.write(content)
                with self.assertRaisesRegex(CompletionEvidenceError, 'corner/point coverage'):
                    validate_candidate_completion(self.workspace, self.candidate_root, self.command, self.parsed)
                with open(os.path.join(self.candidate_root, 'point_identity.json')) as stream:
                    points = json.load(stream)['points']
                self.assertEqual(points[0]['native_model_resolution']['references'][0]['physical_path'],
                                 os.path.join(directories[0], 'toplevel.scs'))

    def test_completed_models_wrong_section_remains_rejected(self):
        source, pdk, model, directories = self._native_model_identity()
        path = os.path.join(directories[0], 'input.scs')
        with open(path) as stream:
            text = stream.read()
        with open(path, 'w') as stream:
            stream.write(text.replace('Noise_Worst', 'Noise_Best'))
        with self.assertRaisesRegex(CompletionEvidenceError, 'model path/section identity mismatch') as raised:
            validate_candidate_completion(self.workspace, self.candidate_root, self.command, self.parsed)
        self.assertLess(len(str(raised.exception)), 512)
        detail = raised.exception.detail
        self.assertEqual(detail['failure_stage'], 'native_corner_coverage')
        self.assertTrue(detail['execution_started'])
        self.assertEqual(detail['phase'], 'completed_native_point_identity')
        failures = detail['native_coverage']['test_differences'][0]['corner_expansion']['identity_failures']
        self.assertEqual(failures[0]['closest_identities'][0]['missing_models'], [(model, 'Noise_Worst')])
        self.assertEqual(self.parsed['native_corner_coverage'], detail['native_coverage'])

    def test_completed_model_unresolved_search_keeps_diagnostics(self):
        source, pdk, model, directories = self._native_model_identity()
        os.unlink(model)
        with self.assertRaisesRegex(CompletionEvidenceError, 'corner/point coverage'):
            validate_candidate_completion(self.workspace, self.candidate_root, self.command, self.parsed)
        with open(os.path.join(self.candidate_root, 'point_identity.json')) as stream:
            points = json.load(stream)['points']
        resolution = points[0]['native_model_resolution']
        self.assertEqual(resolution['include_dirs'], [pdk])
        self.assertIn('error', resolution['references'][0])

    def test_completed_model_command_substitution_is_never_executed_or_trusted(self):
        source, pdk, model, directories = self._native_model_identity()
        marker = os.path.join(self.run_root, 'must-not-exist')
        with open(os.path.join(directories[0], 'runSimulation'), 'w') as stream:
            stream.write('spectre input.scs -I "$(touch %s)"\n' % marker)
        with self.assertRaisesRegex(CompletionEvidenceError, 'corner/point coverage'):
            validate_candidate_completion(self.workspace, self.candidate_root, self.command, self.parsed)
        self.assertFalse(os.path.exists(marker))
        with open(os.path.join(self.candidate_root, 'point_identity.json')) as stream:
            points = json.load(stream)['points']
        self.assertTrue(points[0]['native_model_resolution']['errors'])

    def test_native_sweep_consumer_receives_exact_netlist_variables_temperature_models(self):
        self._native_sweep_identity()
        result = validate_candidate_completion(self.workspace, self.candidate_root, self.command, self.parsed)
        self.assertTrue(result['ok'])
        self.assertEqual([row['source_corner'] for row in result['points']], ['Sweep', 'Sweep'])
        with open(os.path.join(self.candidate_root, 'point_identity.json')) as stream:
            points = json.load(stream)['points']
        self.assertEqual([row['temperature']['value'] for row in points], [27, 27])
        self.assertEqual(points[0]['native_models'], [{'file': '/pdk/model.scs', 'section': 'tt'}])
        self.assertEqual(next(row['value'] for row in points[0]['native_parameters'] if row['name'] == 'Fref'), 64e6)

    def test_native_sweep_duplicate_values_are_rejected_and_point_identity_is_retained(self):
        self._native_sweep_identity((0, 0))
        with self.assertRaisesRegex(CompletionEvidenceError, 'corner/point coverage'):
            validate_candidate_completion(self.workspace, self.candidate_root, self.command, self.parsed)
        with open(os.path.join(self.candidate_root, 'point_identity.json')) as stream:
            points = json.load(stream)['points']
        self.assertEqual([next(row['value'] for row in point['native_parameters'] if row['name'] == 'SW') for point in points], [0, 0])

    def test_shared_live_netlist_uses_exact_point_psf_not_last_declared_value(self):
        self._native_sweep_identity((1, 1))
        requested = []
        def read(requests):
            requested.extend(requests)
            return [{'SW': 0}, {'SW': 1}]
        result = validate_candidate_completion(self.workspace, self.candidate_root, self.command,
                                               self.parsed, native_parameter_reader=read)
        self.assertTrue(result['ok'])
        self.assertEqual([row['names'] for row in requested], [['SW'], ['SW']])
        with open(os.path.join(self.candidate_root, 'point_identity.json')) as stream:
            points = json.load(stream)['points']
        self.assertEqual([point['param_source'] for point in points], ['current_psf_designParamVals'] * 2)
        self.assertEqual([next(row['value'] for row in point['native_declared_parameters'] if row['name'] == 'SW') for point in points], [1, 1])
        self.assertEqual([next(row['value'] for row in point['native_parameters'] if row['name'] == 'SW') for point in points], [0, 1])

    def test_prefetch_derives_same_requests_without_claiming_completion(self):
        from crescendo_engine.native_point_parameters import prefetch_point_parameters
        self._native_sweep_identity(values=(1, 1))
        requests = prepare_native_point_parameter_requests(
            self.workspace, self.setup, self.command, self.parsed, self.candidate_root)
        self.assertEqual([row['names'] for row in requests], [['SW'], ['SW']])
        self.assertEqual([row['point_id'] for row in requests], [1, 2])
        self.assertFalse(os.path.exists(os.path.join(self.candidate_root, 'completion_evidence.json')))
        with patch('crescendo_engine.native_point_parameters.read_point_parameters',
                   return_value=[{'SW': 0}, {'SW': 1}]) as read:
            cached = prefetch_point_parameters(requests, {}, self.candidate_root, object())
            result = validate_candidate_completion(self.workspace, self.candidate_root, self.command,
                                                   self.parsed, native_parameter_reader=cached)
        self.assertEqual(read.call_count, 1)
        self.assertTrue(result['ok'])

    def test_prefetched_parameters_do_not_waive_final_corner_or_solver_gates(self):
        from crescendo_engine.native_point_parameters import prefetch_point_parameters
        self._native_sweep_identity(values=(1, 1))
        requests = prepare_native_point_parameter_requests(
            self.workspace, self.setup, self.command, self.parsed, self.candidate_root)
        with patch('crescendo_engine.native_point_parameters.read_point_parameters',
                   return_value=[{'SW': 1}, {'SW': 1}]):
            wrong = prefetch_point_parameters(requests, {}, self.candidate_root, object())
        with self.assertRaises(CompletionEvidenceError):
            validate_candidate_completion(self.workspace, self.candidate_root, self.command,
                                          self.parsed, native_parameter_reader=wrong)
        with patch('crescendo_engine.native_point_parameters.read_point_parameters',
                   return_value=[{'SW': 0}, {'SW': 1}]):
            correct = prefetch_point_parameters(requests, {}, self.candidate_root, object())
        with open(os.path.join(requests[0]['psf_dir'], 'spectre.out'), 'w') as stream:
            stream.write('spectre completes with 1 errors.\n')
        with self.assertRaises(CompletionEvidenceError):
            validate_candidate_completion(self.workspace, self.candidate_root, self.command,
                                          self.parsed, native_parameter_reader=correct)

    def test_psf_reader_cancellation_survives_completion_wrapper(self):
        from crescendo_engine.netlist_measurement import NetlistMeasurementError
        self._native_sweep_identity()
        def read(requests):
            error = NetlistMeasurementError('Exact point PSF parameter readback cancelled',
                                             {'command': {'cancelled': True}})
            error.cancelled = True
            raise error
        with self.assertRaises(UnsupportedCompletionEvidence) as raised:
            validate_candidate_completion(self.workspace, self.candidate_root, self.command,
                                          self.parsed, native_parameter_reader=read)
        self.assertTrue(raised.exception.cancelled)
        self.assertTrue(raised.exception.detail['command']['cancelled'])
        self.assertTrue(os.path.isfile(os.path.join(self.candidate_root, 'point_identity.json')))

    def test_psf_reader_covers_used_global_test_and_runplan_sweeps(self):
        self._native_sweep_identity((1, 1))
        self.setup['global_variables'] = [{'name': 'N', 'value': '62 140'},
                                          {'name': 'UNUSED', 'value': '1 2'}]
        self.setup['tests'][0]['variables'] = [{'name': 'TRIM', 'value': '4 6'}]
        self.setup['execution_settings'] = [{'tag': 'runplan', 'children': [
            {'tag': 'vars', 'children': [{'tag': 'var', 'attributes': {'name': 'PLAN'},
             'children': [{'tag': 'value', 'text': '2 3'}]}]}]}]
        for point in (1, 2):
            path = os.path.join(self.simulation_root, 'D', 'TB', 'maestro', 'results', 'maestro',
                                self.history, str(point), 'ac_test', 'netlist', 'input.scs')
            with open(path, 'a') as stream:
                stream.write('parameters N=140 TRIM=6 PLAN=3\n')
        requested = []
        def read(requests):
            requested.extend(requests)
            return [{'N': 62, 'TRIM': 4, 'PLAN': 2, 'SW': 0},
                    {'N': 140, 'TRIM': 6, 'PLAN': 3, 'SW': 1}]
        validate_candidate_completion(self.workspace, self.candidate_root, self.command,
                                      self.parsed, native_parameter_reader=read)
        self.assertEqual([row['names'] for row in requested], [['N', 'PLAN', 'SW', 'TRIM']] * 2)

    def test_required_psf_reader_missing_fails_closed(self):
        self._native_sweep_identity()
        self.workspace['native_corner_identity']['point_parameters_required'] = True
        with self.assertRaisesRegex(UnsupportedCompletionEvidence, 'PSF parameter reader is unavailable'):
            validate_candidate_completion(self.workspace, self.candidate_root, self.command, self.parsed)
        self.assertTrue(os.path.isfile(os.path.join(self.candidate_root, 'point_identity.json')))

    def test_psf_missing_variable_or_wrong_point_count_is_rejected(self):
        self._native_sweep_identity((1, 1))
        for values in ([{'SW': 0}], [{'SW': 0}, {}], [{'SW': float('nan')}, {'SW': 1}]):
            with self.assertRaises(CompletionEvidenceError):
                validate_candidate_completion(self.workspace, self.candidate_root, self.command,
                                              self.parsed, native_parameter_reader=lambda requests: values)

    def test_shared_dependent_declarations_are_read_from_exact_psf(self):
        self._native_sweep_identity((1, 1))
        self.workspace['native_corner_identity']['point_parameters_required'] = True
        self.setup['tests'][0]['variables'] = [{'name': 'DERIVED', 'value': '2*SW'},
                                              {'name': 'CHAIN', 'value': '3*DERIVED'}]
        for point in (1, 2):
            path = os.path.join(self.simulation_root, 'D', 'TB', 'maestro', 'results', 'maestro',
                                self.history, str(point), 'ac_test', 'netlist', 'input.scs')
            with open(path, 'a') as stream:
                stream.write('parameters DERIVED=2*SW CHAIN=3*DERIVED\n')
        requested = []
        def read(requests):
            requested.extend(requests)
            return [{'SW': 0, 'DERIVED': 0, 'CHAIN': 0}, {'SW': 1, 'DERIVED': 2, 'CHAIN': 6}]
        result = validate_candidate_completion(self.workspace, self.candidate_root, self.command,
                                               self.parsed, native_parameter_reader=read)
        self.assertTrue(result['ok'])
        self.assertEqual([row['names'] for row in requested], [['CHAIN', 'DERIVED', 'SW']] * 2)
        with open(os.path.join(self.candidate_root, 'point_identity.json')) as stream:
            points = json.load(stream)['points']
        self.assertEqual(points[0]['psf_declared_dynamic_dependencies'], {'DERIVED': ['SW'], 'CHAIN': ['DERIVED']})
        self.assertEqual(next(row['value'] for row in points[0]['native_declared_parameters'] if row['name'] == 'CHAIN'), 6)
        self.assertEqual(next(row['value'] for row in points[0]['native_parameters'] if row['name'] == 'CHAIN'), 0)

    def test_noncorner_temperature_sweep_is_read_from_exact_psf(self):
        self._native_sweep_identity((1, 1))
        self.setup['global_variables'] = [{'name': 'temperature', 'value': '0 50'}]
        requested = []
        def read(requests):
            requested.extend(requests)
            return [{'SW': 0, 'temperature': 0}, {'SW': 1, 'temperature': 50}]
        result = validate_candidate_completion(self.workspace, self.candidate_root, self.command,
                                               self.parsed, native_parameter_reader=read)
        self.assertEqual([row['names'] for row in requested], [['SW', 'temperature']] * 2)
        self.assertEqual([row['temperature']['value'] for row in result['points']], [0, 50])

    def _modern_scope_rows(self, pairs):
        self.setup['corners'] = [{'name': 'Sweep', 'variables': [{'name': 'SW', 'value': '0 1'}]}]
        self.setup['global_variables'] = [{'name': 'N', 'value': '62 80 126 140'}]
        self.setup['tests'][0]['variables'] = [{'name': 'VDD', 'value': '1.2'},
                                              {'name': 'UNUSED', 'value': '9'}]
        rows = []
        for index, (sw, n) in enumerate(pairs, 1):
            rows.append({'point_id': index, 'test': 'ac_test', 'corner': 'native_%s' % sw,
                         'source_corner': 'Sweep', 'param_source': 'current_psf_designParamVals',
                         'native_parameters': [{'name': name, 'value': value}
                                               for name, value in [('SW', sw), ('N', n), ('VDD', 1.2)]]})
        self._native_rdb_values([{'pointID': row['point_id'], 'name': item['name'], 'value': item['value']}
                                 for row in rows for item in row['native_parameters']])
        return rows

    def test_modern_contract_proves_used_scopes_and_complete_noncorner_grid(self):
        rows = self._modern_scope_rows([(sw, n) for sw in (0, 1) for n in (62, 80, 126, 140)])
        _validate_native_effective_scope(self.setup, rows, self.parsed, {})
        proof = self.parsed['native_effective_scope']
        self.assertTrue(proof['ok'])
        self.assertTrue(any(row['variable'] == 'N' and row['source'] == 'global' for row in proof['point_checks']))
        self.assertTrue(all(row['native_value_source'] == 'current_psf_designParamVals' for row in proof['point_checks']))
        self.assertTrue(any(row['variable'] == 'UNUSED' for row in proof['unused_saved_variables']))

    def test_modern_contract_rejects_missing_internal_N_value(self):
        rows = self._modern_scope_rows([(sw, n) for sw in (0, 1) for n in (62, 80, 140)])
        with self.assertRaisesRegex(CompletionEvidenceError, 'effective variable/sweep'):
            _validate_native_effective_scope(self.setup, rows, self.parsed, {})
        self.assertEqual(self.parsed['native_effective_scope']['sweep_differences'][0]['missing_values'], [126.0])

    def test_modern_contract_rejects_duplicate_full_grid_combination(self):
        rows = self._modern_scope_rows([(sw, n) for sw in (0, 1) for n in (62, 80, 126, 140)] + [(0, 62)])
        with self.assertRaisesRegex(CompletionEvidenceError, 'effective variable/sweep'):
            _validate_native_effective_scope(self.setup, rows, self.parsed, {})
        difference = self.parsed['native_effective_scope']['sweep_differences'][0]
        self.assertEqual(difference['reason'], 'design_sweep_point_multiplicity_mismatch')
        self.assertEqual(difference['combinations'][0]['observed_count'], 2)
        self.assertEqual(difference['combinations'][0]['expected_count'], 1)

    def test_modern_contract_rejects_large_grid_diagonal_without_enumeration_skip(self):
        rows = self._modern_scope_rows([(0, number) for number in range(317)])
        self.setup['global_variables'] = [{'name': name, 'value': ' '.join(str(number) for number in range(317))}
                                          for name in ('N', 'B')]
        for row in rows:
            row['native_parameters'].append({'name': 'B', 'value': row['native_parameters'][1]['value']})
        with self.assertRaisesRegex(CompletionEvidenceError, 'effective variable/sweep'):
            _validate_native_effective_scope(self.setup, rows, self.parsed, {})
        difference = self.parsed['native_effective_scope']['sweep_differences'][0]
        self.assertEqual(difference['reason'], 'design_sweep_combinations_missing')
        self.assertEqual(difference['expected_point_count'], 100489)
        self.assertEqual(difference['missing_combination_count'], 100172)

    def test_modern_contract_requires_explicit_repeat_dimension_for_nonsweep_points(self):
        rows = self._modern_scope_rows([(0, 62), (0, 62)])
        self.setup['global_variables'][0]['value'] = '62'
        with self.assertRaisesRegex(UnsupportedCompletionEvidence, 'no proven source repeat dimension'):
            _validate_native_effective_scope(self.setup, rows, self.parsed, {})

    def test_modern_contract_rejects_wrong_saved_constant_and_rdb_disagreement(self):
        rows = self._modern_scope_rows([(sw, n) for sw in (0, 1) for n in (62, 80, 126, 140)])
        rows[0]['native_parameters'][-1]['value'] = 1.1
        with self.assertRaisesRegex(CompletionEvidenceError, 'effective variable/sweep'):
            _validate_native_effective_scope(self.setup, rows, self.parsed, {})
        self.assertEqual(self.parsed['native_effective_scope']['variable_differences'][0]['variable'], 'VDD')
        rows[0]['native_parameters'][-1]['value'] = 1.2
        self.parsed['parameters'] = [{'pointID': 1, 'name': 'N', 'value': '140'}]
        self._native_rdb_values(self.parsed['parameters'])
        with self.assertRaisesRegex(CompletionEvidenceError, 'effective variable/sweep'):
            _validate_native_effective_scope(self.setup, rows, self.parsed, {})
        self.assertEqual(self.parsed['native_effective_scope']['variable_differences'][0]['variable'], 'N')

    def test_modern_contract_rejects_missing_runplan_cartesian_combinations(self):
        rows = self._modern_scope_rows([(sw, n) for sw in (0, 1) for n in (62, 140)])
        self.setup['global_variables'][0]['value'] = '62 140'
        self.setup['execution_settings'] = [{'tag': 'runplan', 'children': [
            {'tag': 'vars', 'children': [{'tag': 'var', 'attributes': {'name': 'PLAN'},
             'children': [{'tag': 'value', 'text': '2 3'}]}]}]}]
        for row in rows:
            row['native_parameters'].append({'name': 'PLAN', 'value': 2 if row['native_parameters'][1]['value'] == 62 else 3})
        with self.assertRaisesRegex(CompletionEvidenceError, 'effective variable/sweep'):
            _validate_native_effective_scope(self.setup, rows, self.parsed, {})
        self.assertEqual(self.parsed['native_effective_scope']['sweep_differences'][0]['reason'], 'design_sweep_combinations_missing')

    def test_modern_contract_rejects_unknown_execution_plan_scope(self):
        rows = self._modern_scope_rows([(sw, n) for sw in (0, 1) for n in (62, 80, 126, 140)])
        self.setup['execution_settings'] = [{'tag': 'runplan', 'children': [{'tag': 'unknown', 'text': 'opaque'}]}]
        with self.assertRaisesRegex(UnsupportedCompletionEvidence, 'execution-plan scope is unresolved'):
            _validate_native_effective_scope(self.setup, rows, self.parsed, {})

    def test_modern_completion_persists_source_variable_failure_before_cleanup(self):
        self._native_sweep_identity((1, 1))
        self.workspace['native_corner_identity']['point_parameters_required'] = True
        self.setup['tests'][0]['variables'] = [{'name': 'Fref', 'value': '32M'}]
        with self.assertRaisesRegex(CompletionEvidenceError, 'effective variable/sweep'):
            validate_candidate_completion(self.workspace, self.candidate_root, self.command,
                                          self.parsed, native_parameter_reader=lambda requests: [{'SW': 0}, {'SW': 1}])
        with open(os.path.join(self.candidate_root, 'portability_settings.json')) as stream:
            proof = json.load(stream)
        self.assertFalse(proof['ok'])
        self.assertEqual(proof['variable_differences'][0]['variable'], 'Fref')
        self.assertEqual(proof['variable_differences'][0]['expected_value'], 32e6)

    def test_maestro_history_uses_setup_binding_when_test_targets_another_design(self):
        self.setup["tests"][0]["library"] = "DEVICE_DESIGN"
        self.setup["tests"][0]["cell"] = "DUT"
        result = validate_candidate_completion(
            self.workspace, self.candidate_root, self.command, self.parsed,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(len(result["spectre_runs"]), 2)
        self.assertTrue(all(
            "/D/TB/maestro/results/maestro/" in row["psf"]
            for row in result["spectre_runs"]
        ))

    def test_maestro_does_not_accept_design_history_when_bound_setup_history_is_missing(self):
        self.workspace["setup_cell"] = "OTHER_SETUP"
        with self.assertRaisesRegex(UnsupportedCompletionEvidence, "OTHER_SETUP"):
            validate_candidate_completion(
                self.workspace, self.candidate_root, self.command, self.parsed,
            )

    def test_maestro_setup_binding_keeps_exact_history_and_point_marker_requirements(self):
        self.setup["tests"][0]["cell"] = "DUT"
        marker = os.path.join(
            self.simulation_root, "D", "TB", "maestro", "results", "maestro",
            self.history, "2", "ac_test", "psf", ".simDone",
        )
        os.unlink(marker)
        with self.assertRaisesRegex(CompletionEvidenceError, "simDone is missing"):
            validate_candidate_completion(
                self.workspace, self.candidate_root, self.command, self.parsed,
            )

    def test_native_shared_spectre_log_requires_output_binding_for_its_point(self):
        history_root = os.path.join(
            self.simulation_root, "D", "TB", "maestro", "results", "maestro", self.history,
        )
        first = os.path.join(history_root, "1", "ac_test", "psf", "spectre.out")
        second = os.path.join(history_root, "2", "ac_test", "psf", "spectre.out")
        with open(first, "w") as stream:
            stream.write("Opening the PSF file ../../../2/ac_test/psf/tran.tran ...\n")
            stream.write("spectre completes with 0 errors\n")
        os.unlink(second)
        os.symlink(first, second)
        result = validate_candidate_completion(
            self.workspace, self.candidate_root, self.command, self.parsed,
        )
        self.assertEqual(
            result["spectre_runs"][1]["spectre"]["point_binding"]["mode"],
            "native_incremental_shared_log",
        )

    def test_shared_spectre_log_without_matching_point_output_is_rejected(self):
        history_root = os.path.join(
            self.simulation_root, "D", "TB", "maestro", "results", "maestro", self.history,
        )
        first = os.path.join(history_root, "1", "ac_test", "psf", "spectre.out")
        second = os.path.join(history_root, "2", "ac_test", "psf", "spectre.out")
        os.unlink(second)
        os.symlink(first, second)
        with self.assertRaisesRegex(CompletionEvidenceError, "no output binding"):
            validate_candidate_completion(
                self.workspace, self.candidate_root, self.command, self.parsed,
            )

    def test_shared_spectre_log_cannot_reuse_another_history_inside_workspace(self):
        history_root = os.path.join(
            self.simulation_root, "D", "TB", "maestro", "results", "maestro", self.history,
        )
        second = os.path.join(history_root, "2", "ac_test", "psf", "spectre.out")
        other = os.path.join(self.workspace_root, "other_history_spectre.out")
        with open(other, "w") as stream:
            stream.write("spectre completes with 0 errors\n")
        os.unlink(second)
        os.symlink(other, second)
        with self.assertRaisesRegex(CompletionEvidenceError, "outside|escapes"):
            validate_candidate_completion(
                self.workspace, self.candidate_root, self.command, self.parsed,
            )

    def test_validates_adexl_native_history_and_point_layout(self):
        history = "Interactive.11"
        self.workspace["setup_backend"] = "adexl"
        self.workspace["setup_view"] = "custom_adexl"
        rdb_path = os.path.join(self.setup_dir, "results", "data", history + ".rdb")
        os.makedirs(os.path.dirname(rdb_path))
        with open(rdb_path, "wb") as stream:
            stream.write(b"rdb")
        self.parsed["path"] = rdb_path
        results_dir = os.path.join(
            self.simulation_root, "D", "TB", "custom_adexl", "results", "data", history,
        )
        for point in (1, 2):
            psf = os.path.join(results_dir, str(point), "ac_test", "psf")
            os.makedirs(psf)
            open(os.path.join(psf, ".simDone"), "w").close()
            with open(os.path.join(psf, "spectre.out"), "w") as stream:
                stream.write("spectre completes with 0 errors, 1 warnings, and 0 notices.\n")
        command = {
            "backend": "adexl", "returncode": 0, "timed_out": False,
            "startup_timed_out": False, "history_name": history,
            "results_dir": results_dir,
        }
        result = validate_candidate_completion(
            self.workspace, self.candidate_root, command, self.parsed,
        )
        self.assertEqual(result["adapter"], "adexl-rdb-psf-v1")
        self.assertEqual(result["backend"], "adexl")

    def test_rejects_history_mismatch(self):
        self.command["history_name"] = "different"
        with self.assertRaisesRegex(CompletionEvidenceError, "history mismatch"):
            validate_candidate_completion(self.workspace, self.candidate_root, self.command, self.parsed)

    def test_rejects_missing_corner(self):
        self.parsed["test_statuses"] = self.parsed["test_statuses"][:1]
        with self.assertRaisesRegex(CompletionEvidenceError, "missing corner"):
            validate_candidate_completion(self.workspace, self.candidate_root, self.command, self.parsed)

    def test_accepts_empty_rdb_corner_for_setup_default(self):
        self.setup["corners"] = [{"name": "_default", "enabled": True}]
        self.parsed["points"] = [{"pointID": 1, "designPointNumber": 1, "corner_name": ""}]
        self.parsed["test_statuses"] = self.parsed["test_statuses"][:1]
        self.parsed["output_map"]["ac_test::gain"]["values"] = [
            {"point_id": 1, "corner": "", "numeric": 10.0, "error": None},
        ]

        result = validate_candidate_completion(
            self.workspace, self.candidate_root, self.command, self.parsed,
        )
        self.assertEqual(result["corners"], ["_default"])
        self.assertEqual(result["points"][0]["corner"], "_default")

    def test_default_rdb_corner_cannot_satisfy_explicit_corner(self):
        self.setup["corners"] = [{"name": "TT", "enabled": True}]
        self.parsed["points"] = [{"pointID": 1, "designPointNumber": 1, "corner_name": ""}]
        self.parsed["test_statuses"] = self.parsed["test_statuses"][:1]
        with self.assertRaisesRegex(CompletionEvidenceError, "missing corner"):
            validate_candidate_completion(self.workspace, self.candidate_root, self.command, self.parsed)

    def test_rejects_non_complete_status_code_even_with_done_message(self):
        self.parsed["test_statuses"][0]["statusCode"] = 2
        with self.assertRaisesRegex(CompletionEvidenceError, "non-complete statusCode"):
            validate_candidate_completion(self.workspace, self.candidate_root, self.command, self.parsed)

    def test_status_code_three_is_authoritative_over_localized_message(self):
        self.parsed["test_statuses"][0]["status_message"] = "localized-success"
        result = validate_candidate_completion(
            self.workspace, self.candidate_root, self.command, self.parsed,
        )
        self.assertTrue(result["ok"])

    def test_status_message_is_fallback_when_status_code_is_missing(self):
        for status in self.parsed["test_statuses"]:
            status["statusCode"] = None
        result = validate_candidate_completion(
            self.workspace, self.candidate_root, self.command, self.parsed,
        )
        self.assertTrue(result["ok"])

    def test_rejects_unrecognized_status_message_when_status_code_is_missing(self):
        self.parsed["test_statuses"][0]["statusCode"] = None
        self.parsed["test_statuses"][0]["status_message"] = "localized-success"
        with self.assertRaisesRegex(CompletionEvidenceError, "has no statusCode"):
            validate_candidate_completion(self.workspace, self.candidate_root, self.command, self.parsed)

    def test_rejects_output_evaluation_error(self):
        self.parsed["output_map"]["ac_test::gain"]["values"][0]["error"] = "expression returned nil"
        with self.assertRaisesRegex(CompletionEvidenceError, "evaluation error"):
            validate_candidate_completion(self.workspace, self.candidate_root, self.command, self.parsed)

    def test_runner_can_validate_only_outputs_required_by_optimization_spec(self):
        self.setup["tests"][0]["outputs"].append({"name": "unneeded_broken_plot"})
        result = validate_candidate_completion(
            self.workspace, self.candidate_root, self.command, self.parsed,
            required_output_keys=["ac_test::gain"],
        )
        self.assertEqual([row["key"] for row in result["outputs"]], ["ac_test::gain"])

    def test_rejects_unrecognized_spectre_completion(self):
        log = os.path.join(
            self.simulation_root, "D", "TB", "maestro", "results", "maestro",
            self.history, "2", "ac_test", "psf", "spectre.out",
        )
        with open(log, "w") as stream:
            stream.write("simulator exited without a portable completion marker\n")
        with self.assertRaisesRegex(UnsupportedCompletionEvidence, "no recognized completion"):
            validate_candidate_completion(self.workspace, self.candidate_root, self.command, self.parsed)

    def test_rejects_psf_symlink_outside_workspace(self):
        log = os.path.join(
            self.simulation_root, "D", "TB", "maestro", "results", "maestro",
            self.history, "2", "ac_test", "psf", "spectre.out",
        )
        os.unlink(log)
        outside = os.path.join(self.run_root, "outside.log")
        with open(outside, "w") as stream:
            stream.write("spectre completes with 0 errors\n")
        os.symlink(outside, log)
        with self.assertRaisesRegex(CompletionEvidenceError, "outside|escapes"):
            validate_candidate_completion(self.workspace, self.candidate_root, self.command, self.parsed)

    def test_unique_history_is_embedded_and_verified_in_ocean(self):
        config = {
            "cadence": {"spectre_bin": "/bin/true", "cadence_root": ""},
            "runtime": {"simulation_timeout_seconds": 10},
        }
        identity = _history_identity(self.workspace, os.path.join(self.candidate_root, "attempts", "attempt_1"))
        second = _history_identity(self.workspace, os.path.join(self.candidate_root, "attempts", "attempt_2"))
        self.assertNotEqual(identity["history_name"], second["history_name"])
        batch = write_batch_files(self.workspace, config, identity["history_name"])
        with open(batch["ocean_script"]) as stream:
            text = stream.read()
        self.assertIn("axlSetHistoryName", text)
        self.assertIn("axlGetHistoryName", text)
        self.assertIn(identity["history_name"], text)


class RetentionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.run_root = self.temp.name
        self.candidate_root = os.path.join(self.run_root, "candidates", "g000_c0000")
        self.workspace = os.path.join(self.candidate_root, "workspace")
        self.source = os.path.join(self.run_root, "source")
        os.makedirs(self.workspace)
        os.makedirs(self.source)
        with open(os.path.join(self.candidate_root, "candidate.json"), "w") as stream:
            stream.write("{}\n")
        with open(os.path.join(self.candidate_root, "completion_evidence.json"), "w") as stream:
            stream.write("{}\n")
        with open(os.path.join(self.workspace, "workspace.json"), "w") as stream:
            json.dump({"root": self.workspace, "source_libraries": {"D": self.source}}, stream)
        with open(os.path.join(self.workspace, "large.psf"), "wb") as stream:
            stream.write(b"x" * 4096)
        attempts = os.path.join(self.candidate_root, "attempts", "attempt_1")
        os.makedirs(attempts)
        with open(os.path.join(attempts, "ocean.stdout"), "wb") as stream:
            stream.write(b"x" * 4096)
        with open(os.path.join(self.candidate_root, "patch_schematic.stdout"), "wb") as stream:
            stream.write(b"x" * 4096)
        with open(os.path.join(self.candidate_root, "unexpected-large-result.raw"), "wb") as stream:
            stream.write(b"x" * 4096)

    def tearDown(self):
        self.temp.cleanup()

    def test_deletes_only_private_workspace_after_lightweight_persistence(self):
        result = safe_delete_candidate_workspace(self.candidate_root, self.run_root, {"ok": True})
        self.assertTrue(result["removed"])
        self.assertFalse(os.path.exists(self.workspace))
        self.assertFalse(os.path.exists(os.path.join(self.candidate_root, "attempts")))
        self.assertFalse(os.path.exists(os.path.join(self.candidate_root, "patch_schematic.stdout")))
        self.assertFalse(os.path.exists(os.path.join(self.candidate_root, "unexpected-large-result.raw")))
        self.assertTrue(os.path.isfile(os.path.join(self.candidate_root, "candidate.json")))
        self.assertTrue(os.path.isdir(self.source))

    def test_cleanup_preserves_and_updates_lightweight_timing(self):
        timing_path = os.path.join(self.candidate_root, "timing.json")
        with open(timing_path, "w") as stream:
            json.dump({"schema_version": "crescendo.candidate_timing.v1", "phases": {}}, stream)
        safe_delete_candidate_workspace(self.candidate_root, self.run_root, {"ok": True})
        self.assertTrue(os.path.isfile(timing_path))
        with open(timing_path) as stream:
            timing = json.load(stream)
        self.assertTrue(timing["phases"]["cleanup"]["workspace_removed"])
        self.assertGreaterEqual(timing["phases"]["cleanup"]["elapsed_seconds"], 0.0)

    def test_candidate_record_drops_raw_commands_traceback_and_netlist_statements(self):
        row = {
            "candidate_id": "g000_c0000", "generation": 0, "index": 0,
            "values": {"w": 2e-6}, "status": "completed", "metrics": {"feasible": True},
            "execution_backend": "persistent_maestro",
            "persistent_worker": {"worker_index": 1, "batch_elapsed_seconds": 4.5},
            "traceback": "raw traceback", "rdb_path": "/large/result.rdb",
            "maestro_attempts": [{"stdout_tail": "large raw output"}],
            "parameter_application": {"ok": True, "parameters": [{
                "parameter_id": "p1", "hierarchy_path": "/X1/M0", "property": "w", "requested": "2u",
                "persisted_schematic_value": "2.0u", "persisted_cdf_value": "2.0u", "callback_status": "executed",
                "statement": "full netlist", "effective_netlist": [
                    {"mode": "direct", "value": "2.0u", "statement": "full statement", "netlist": "/large/netlist"},
                    {"mode": "direct", "value": "2.0u", "statement": "duplicate", "netlist": "/other/netlist"},
                ],
            }], "netlists": ["large"]},
            "completion_evidence": {"ok": True, "adapter": "a", "history_name": "h", "tests": ["t"], "corners": ["c"], "points": [1], "outputs": [1, 2]},
        }
        compact = lightweight_candidate_record(row)
        self.assertNotIn("traceback", compact)
        self.assertNotIn("rdb_path", compact)
        self.assertNotIn("maestro_attempts", compact)
        self.assertEqual(compact["execution_backend"], "persistent_maestro")
        self.assertEqual(compact["persistent_worker"]["worker_index"], 1)
        application = compact["parameter_application"]
        self.assertTrue(application["ok"])
        self.assertEqual(application["parameter_count"], 1)
        self.assertEqual(application["netlist_count"], 1)
        self.assertEqual(application["parameters"], [{
            "parameter_id": "p1", "hierarchy_path": "/X1/M0", "property": "w", "requested": "2u",
            "persisted_schematic_value": "2.0u", "persisted_cdf_value": "2.0u", "callback_status": "executed",
            "effective_netlist": [{"mode": "direct", "value": "2.0u"}],
        }])
        self.assertEqual(compact["completion"]["output_count"], 2)
        path = persist_lightweight_candidate(self.candidate_root, row)
        self.assertLess(os.path.getsize(path), 2048)

    def test_rejects_cleanup_without_source_guard(self):
        with self.assertRaisesRegex(UnsafeCleanupError, "source integrity"):
            safe_delete_candidate_workspace(self.candidate_root, self.run_root, {"ok": False})
        self.assertTrue(os.path.isdir(self.workspace))

    def test_rejects_symlink_workspace(self):
        os.unlink(os.path.join(self.workspace, "large.psf"))
        os.unlink(os.path.join(self.workspace, "workspace.json"))
        os.rmdir(self.workspace)
        outside = os.path.join(self.run_root, "outside")
        os.makedirs(outside)
        os.symlink(outside, self.workspace)
        with self.assertRaisesRegex(UnsafeCleanupError, "symbolic link"):
            safe_delete_candidate_workspace(self.candidate_root, self.run_root, {"ok": True})
        self.assertTrue(os.path.isdir(outside))

    def test_rejects_missing_lightweight_evidence(self):
        os.unlink(os.path.join(self.candidate_root, "completion_evidence.json"))
        with self.assertRaisesRegex(UnsafeCleanupError, "recovery artifact"):
            safe_delete_candidate_workspace(self.candidate_root, self.run_root, {"ok": True})
        self.assertTrue(os.path.isdir(self.workspace))

    def test_failed_candidate_can_keep_only_its_persisted_summary(self):
        os.unlink(os.path.join(self.candidate_root, "completion_evidence.json"))
        candidate_json = os.path.join(self.candidate_root, "candidate.json")
        result = safe_delete_candidate_workspace(
            self.candidate_root, self.run_root, {"ok": True},
            required_lightweight_paths=[candidate_json],
        )
        self.assertTrue(result["removed"])
        self.assertEqual(
            sorted(os.listdir(self.candidate_root)),
            ["candidate.json"],
        )

    def test_raw_symlink_is_unlinked_without_touching_target(self):
        outside = os.path.join(self.run_root, "outside-raw")
        with open(outside, "w") as stream:
            stream.write("keep me")
        os.symlink(outside, os.path.join(self.candidate_root, "raw-link"))
        safe_delete_candidate_workspace(self.candidate_root, self.run_root, {"ok": True})
        self.assertTrue(os.path.isfile(outside))
        self.assertFalse(os.path.lexists(os.path.join(self.candidate_root, "raw-link")))

    def test_abandoned_cleanup_revalidates_sources_before_deleting(self):
        candidate_json = os.path.join(self.candidate_root, "candidate.json")
        calls = []

        def verify(metadata):
            calls.append(metadata["root"])
            return {"ok": True}

        result = cleanup_abandoned_candidate(
            self.candidate_root, self.run_root, verify,
            required_lightweight_paths=[candidate_json],
        )
        self.assertTrue(result["removed"])
        self.assertEqual(calls, [self.workspace])
        self.assertEqual(
            sorted(os.listdir(self.candidate_root)),
            ["candidate.json", "completion_evidence.json"],
        )


if __name__ == "__main__":
    unittest.main()
