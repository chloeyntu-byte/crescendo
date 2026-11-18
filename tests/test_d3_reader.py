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


class D3ReaderTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = self.temp.name
        self.task = {'task_id': 'sp_tt', 'test': 'sp', 'corner': 'TT', 'point_id': 1,
                     'cwd': self.root, 'psf_dir': os.path.join(self.root, 'psf'),
                     'outputs': [{'name': 'cap_high', 'key': 'sp::cap_high', 'expression': 'value(cap_wave 22e9)', 'unit': 'F'},
                                 {'name': 'cap_wave', 'key': 'sp::cap_wave', 'expression': 'cr_cap_wave()'},
                                 {'name': 'idle', 'key': 'sp::idle', 'expression': 'value(cap_wave 100e9)'}]}
        self.records = os.path.join(self.root, 'records.tsv')
        self.config = {'cadence': {'ocean_bin': os.sys.executable}}

    def parse(self, body, capability):
        with open(self.records, 'w') as stream:
            stream.write(reader.PROTOCOL + '\towned\n' + body + 'END\t1\n')
        return reader.parse_measurement_records(self.records, 'owned', self.task['outputs'][:1], self.task, capability=capability)

    def test_nonfinite_and_missing_keep_capability_before_rejection(self):
        for body, expected in [('V\t0\tNaN\n', 'non_finite'), ('V\t0\tinf\n', 'non_finite'), ('', 'not_collected')]:
            capability = {}
            with self.subTest(expected=expected), self.assertRaises(reader.NetlistMeasurementError) as caught:
                self.parse(body, capability)
            self.assertEqual(capability['output_diagnostics'][0]['output_status'], expected)
            self.assertEqual(caught.exception.detail['result_reader_capability'], capability)
            json.dumps(capability, allow_nan=False)

    def test_evaluation_error_and_dependency_failure_keep_first_error(self):
        capability = {}
        rows = self.parse('E\t0\tdependency_evaluation_failed\n', capability)
        self.assertEqual(rows[0]['output_status'], 'evaluation_failed')
        self.assertEqual(rows[0]['first_error'], 'dependency_evaluation_failed')
        self.assertEqual(capability['output_diagnostics'][0]['output_dependencies'], ['cap_wave'])
        self.assertEqual(capability['evaluation_output_keys'], ['sp::cap_wave', 'sp::cap_high'])
        self.assertEqual(capability['requested_output_keys'], ['sp::cap_high'])

    def test_single_and_batch_scripts_clear_aliases_and_propagate_failures(self):
        for isolated in (False, True):
            script, outputs = reader.render_measurement_script(self.task, self.records, 'owned', ['sp::cap_high'], isolated)
            self.assertEqual([o['key'] for o in outputs], ['sp::cap_high'])
            self.assertNotIn('100e9', script)
            self.assertLess(script.index("set('cap_wave nil)"), script.index('evalstring("cr_cap_wave()")'))
            self.assertLess(script.index('evalstring("cr_cap_wave()")'), script.index('evalstring("value(cap_wave 22e9)")'))
            self.assertIn('member(crescVariable cr_failed_outputs)', script)
            self.assertIn('dependency_evaluation_failed', script)
            self.assertIn('cap_wave cap_high)', script)

    def synthetic_command(self, argv, **kwargs):
        """Bounded SP waveform response; checks actual rendered dependency order.

        Native expected values below are independently stored in an RDB fixture.
        This mock is deliberately limited to these two fixture expressions.
        """
        with open(argv[-1]) as stream:
            script = stream.read()
        blocks = re.split(r'procedure\((?:cr_read_point|crescEvaluatePrivateNetlist)\(\)', script)[1:]
        self.assertTrue(blocks)
        for block in blocks:
            exprs = [json.loads(m) for m in re.findall(r'evalstring\(("(?:\\.|[^"\\])*")\)', block)]
            self.assertEqual(exprs, ['cr_cap_wave()', 'value(cap_wave 22e9)'])
            # Synthetic SP capacitance wave has domain [1 GHz, 30 GHz].
            wave = {1e9: 1.5e-14, 11e9: 1.6e-14, 22e9: 1.68e-14, 30e9: 1.7e-14}
            frequency = float(re.fullmatch(r'value\(cap_wave ([^)]+)\)', exprs[-1]).group(1))
            filename = json.loads(re.search(r'crescPort = outfile\(("(?:\\.|[^"\\])*")\)', block).group(1))
            token = re.search(r'CRESCENDO_NETLIST_MEASURE_1\\t([^"\\]+)\\n', block).group(1)
            with open(filename, 'w') as stream:
                stream.write(reader.PROTOCOL + '\t' + token + '\nA\t0\tsp\nI\t0\tavailable\nV\t0\t%.17g\nEND\t1\n' % wave[frequency])
        return {'returncode': 0}

    def test_synthetic_sp_native_rdb_single_and_batch_paths_match(self):
        # SQLite schema fixture exercises the real Native result parser.
        fixture = rdb_fixture.RdbParserTest(); fixture.setUp()
        self.addCleanup(fixture.tearDown)
        connection = sqlite3.connect(fixture.path)
        connection.execute("UPDATE test SET name='sp'")
        connection.execute("UPDATE result SET name='cap_high', expression='value(cap_wave 22e9)' WHERE resultID=1")
        connection.execute('UPDATE resultValue SET value=1.68e-14 WHERE resultID=1')
        connection.commit(); connection.close()
        native = parse_rdb(fixture.path)
        with mock.patch.object(reader, 'run_owned_command', side_effect=self.synthetic_command):
            single = reader.evaluate_task_outputs(self.task, self.config, self.root, 10, ['sp::cap_high'])
            other_root = os.path.join(self.root, 'other'); os.mkdir(other_root)
            other = dict(self.task, task_id='sp_ff', point_id=2, corner='FF', cwd=other_root,
                         psf_dir=os.path.join(other_root, 'psf'))
            batch = reader.evaluate_task_outputs_batch([self.task, other], self.config, self.root, 10, ['sp::cap_high'])
        cli = reader.parsed_results([{'task': self.task, 'measurement': single},
                                     {'task': other, 'measurement': batch['sp_ff']}])
        definition = [{'output_key': 'sp::cap_high', 'direction': 'minimize'}]
        self.assertEqual(evaluate_metrics(native, definition, [])['objectives'], evaluate_metrics(cli, definition, [])['objectives'])
        self.assertEqual(batch['sp_tt']['outputs'][0]['value'], single['outputs'][0]['value'])
        self.assertEqual(cli['output_map']['sp::cap_high']['unit'], 'F')
        states = output_statuses(cli)
        self.assertEqual([s['status'] for s in states if s['output'] == 'sp::cap_high'], ['available', 'available'])
        self.assertTrue(all(s['status'] == 'not_collected' for s in states if s['output'] == 'sp::idle'))

    def test_native_parser_preserves_nonfinite_wave_and_first_evaluation_error(self):
        fixture = rdb_fixture.RdbParserTest(); fixture.setUp(); self.addCleanup(fixture.tearDown)
        connection = sqlite3.connect(fixture.path)
        connection.execute("UPDATE resultValue SET value='inf' WHERE pointID=1 AND resultID=1")
        connection.execute("INSERT INTO error VALUES(1, 'frequency outside SP sweep')")
        connection.execute('UPDATE resultValue SET value=NULL, errorID=1 WHERE pointID=2 AND resultID=1')
        connection.commit(); connection.close()
        states = output_statuses(parse_rdb(fixture.path))
        gain = [s for s in states if s['output'] == 'main::gain']
        self.assertEqual([s['status'] for s in gain], ['non_finite', 'evaluation_failed'])
        self.assertEqual(gain[1]['first_error'], 'frequency outside SP sweep')
        self.assertEqual({s['status'] for s in states if s['output'] == 'main::wave'}, {'waveform'})


    def test_scalar_dependency_observation_survives_into_raw_metrics(self):
        self.task['outputs'][1]['expression'] = '1.68e-14'
        self.task['outputs'][0]['expression'] = 'cap_wave + 1e-15'
        capability = {}
        rows = self.parse('D\t0\t1.68e-14\nV\t0\t1.78e-14\n', capability)
        self.assertEqual(capability['dependency_observation_schema'], 'crescendo.output_observations.v1')
        parsed = reader.parsed_results([{'task': self.task, 'measurement': {
            'outputs': rows, 'result_reader_capability': capability}}])
        metrics = evaluate_metrics(parsed, [{'output_key': 'sp::cap_high'}], [], diagnostic=True)
        dependency = next(p for p in metrics['diagnostic_raw_points'] if p['output'] == 'sp::cap_wave')
        self.assertEqual(dependency['value'], 1.68e-14)
        self.assertEqual(dependency['output_status'], 'available')
        for body in ('D\t1\t2\n', 'D\t0\t2\nD\t0\t3\n', 'D\t0\tgarbage\n'):
            with self.subTest(body=body), self.assertRaises(reader.NetlistMeasurementError):
                self.parse(body + 'V\t0\t1\n', {})

    def test_reader_first_error_is_retained_when_protocol_has_no_numeric_value(self):
        def command(argv, **kwargs):
            return dict(self.synthetic_command(argv, **kwargs), returncode=2,
                        stderr_tail='prefix\n*Error* value: frequency outside sweep\nsecond error')
        with mock.patch.object(reader, 'run_owned_command', side_effect=command):
            with self.assertRaises(reader.NetlistMeasurementError) as caught:
                reader.evaluate_task_outputs(self.task, self.config, self.root, 10, ['sp::cap_high'])
        self.assertEqual(caught.exception.detail['result_reader_capability']['reader_first_error'],
                         '*Error* value: frequency outside sweep')

    def test_failed_candidate_json_retains_output_state_and_reader_first_error(self):
        import test_netlist_optimizer as fixture
        from crescendo_engine.netlist_optimizer import PrivateNetlistOptimizer
        from crescendo_engine.netlist_execution import NetlistExecutionError
        from crescendo_engine import runner
        prepared, config, spec = fixture.fixtures(self.root)
        adapter = PrivateNetlistOptimizer(self.root, spec, prepared, prepared['parameters'], config, runner._failure_metrics)
        adapter.preparing = True; adapter.prepared_event.set()
        adapter.manifest = {'source_identity': {}}
        adapter.context = mock.Mock(); adapter.context.unsafe.is_set.return_value = False
        detail = {'result_reader_capability': {'test': 'main', 'corner': 'TT', 'point_id': 1,
                  'reader_first_error': '*Error* value: frequency outside sweep',
                  'output_diagnostics': [{'key': 'main::gain', 'output_status': 'evaluation_failed',
                                          'first_error': 'expression_failed'}]}}
        adapter.context.evaluate.side_effect = NetlistExecutionError('Missing scalar', detail, infrastructure=False)
        with mock.patch('crescendo_engine.netlist_optimizer.materialize_candidate', return_value={'tasks': [fixture.model_task()]}):
            row = adapter.evaluate(0, 0, {'x': 0.5})
        self.assertEqual(row['status'], 'failed')
        with open(os.path.join(self.root, 'candidates', row['candidate_id'], 'candidate.json')) as stream:
            saved = json.load(stream)
        self.assertEqual(saved['output_statuses'][0]['status'], 'evaluation_failed')
        self.assertEqual(saved['reader_first_error'], '*Error* value: frequency outside sweep')

if __name__ == '__main__':
    unittest.main()
