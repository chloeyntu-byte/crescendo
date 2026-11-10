import copy
import json
import os
import tempfile
import unittest
import time
from unittest.mock import Mock, patch

from crescendo_engine.native_point_parameters import (
    PROTOCOL, parse_point_parameter_records, render_point_parameter_script, read_point_parameters,
    prefetch_point_parameters,
)
from crescendo_engine.netlist_measurement import NetlistMeasurementError


class NativePointParameterTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.temp.name, 'parameters.tsv')
        self.requests = [{'point_id': 7, 'test': 'alias', 'psf_dir': '/owned/history/7/alias/psf',
                          'names': ['SW', 'temperature']}]

    def tearDown(self):
        self.temp.cleanup()

    def write(self, body, token='owned', end='END\t1'):
        with open(self.path, 'w') as stream:
            stream.write(PROTOCOL + '\t' + token + '\n' + body + '\n' + end + '\n')

    def test_exact_point_protocol_accepts_only_bound_complete_finite_values(self):
        self.write('P\t0\t0\t0\nP\t0\t1\t27')
        self.assertEqual(parse_point_parameter_records(self.path, 'owned', self.requests),
                         [{'SW': 0.0, 'temperature': 27.0}])

    def test_missing_duplicate_wrong_identity_and_nonfinite_values_fail(self):
        for body in ('P\t0\t0\t0', 'P\t0\t0\t0\nP\t0\t0\t1',
                     'P\t1\t0\t0', 'P\t0\t0\tnan\nP\t0\t1\t27',
                     'X\t0\t0\tunavailable\nP\t0\t1\t27'):
            self.write(body)
            with self.assertRaises(NetlistMeasurementError):
                parse_point_parameter_records(self.path, 'owned', self.requests)
        self.write('P\t0\t0\t0\nP\t0\t1\t27', token='other')
        with self.assertRaises(NetlistMeasurementError):
            parse_point_parameter_records(self.path, 'owned', self.requests)
        self.write('P\t0\t0\t0\nP\t0\t1\t27', end='END\t2')
        with self.assertRaises(NetlistMeasurementError):
            parse_point_parameter_records(self.path, 'owned', self.requests)

    def test_render_reads_exact_psf_pv_without_setup_writes_or_simulation(self):
        script = render_point_parameter_script(self.requests, self.path, 'owned')
        self.assertIn('openResults("/owned/history/7/alias/psf")', script)
        self.assertIn('pv("top-level" "SW" ?result \'designParamVals)', script)
        self.assertIn('pv("top-level" "temp" ?result \'designParamVals)', script)
        self.assertNotIn('maeSet', script)
        self.assertNotIn('run()', script)

    def test_owned_reader_timeout_preserves_stage_and_forwards_budget_cancel(self):
        config = {'cadence': {'ocean_bin': '/bin/true', 'startup_extensions': []}}
        cancel = lambda: False
        captured = {}
        def run(argv, **kwargs):
            captured.update(kwargs)
            root = kwargs['control_root']
            with open(os.path.join(root, 'reader_progress.tsv')) as f:
                token = f.read().split('\t')[0]
            with open(os.path.join(root, 'reader_progress.tsv'), 'w') as f:
                f.write(token + '\topen_results_0\n')
            return {'returncode': 143, 'timed_out': True}
        with patch('crescendo_engine.native_point_parameters.run_owned_command', side_effect=run):
            with self.assertRaises(NetlistMeasurementError) as raised:
                read_point_parameters(self.requests, config, self.temp.name,
                                      deadline=time.monotonic() + 20, cancel_check=cancel)
        self.assertIn('timed out', str(raised.exception))
        self.assertEqual(raised.exception.detail['last_stage'], 'open_results_0')
        self.assertFalse(raised.exception.detail['records_present'])
        self.assertIs(captured['cancel_check'], cancel)
        self.assertTrue(0 < captured['timeout_seconds'] <= 20)
        self.assertEqual(captured['cwd'], os.path.join(self.temp.name, 'native_point_parameters', 'work'))
        self.assertTrue(os.path.isfile(os.path.join(captured['control_root'], 'reader_status.json')))

    def test_explicit_extension_preserves_candidate_cwd(self):
        extension = os.path.join(self.temp.name, 'extension.il')
        with open(extension, 'w') as stream:
            stream.write('; extension with potential relative loads')
        config = {'cadence': {'ocean_bin': '/bin/true', 'startup_extensions': [extension]}}
        with patch('crescendo_engine.native_point_parameters.run_owned_command',
                   return_value={'returncode': 143, 'timed_out': True}) as run:
            with self.assertRaises(NetlistMeasurementError):
                read_point_parameters(self.requests, config, self.temp.name)
        self.assertEqual(run.call_args[1]['cwd'], self.temp.name)

    def test_cancelled_reader_is_classified_as_cancelled(self):
        config = {'cadence': {'ocean_bin': '/bin/true'}}
        with patch('crescendo_engine.native_point_parameters.run_owned_command',
                   return_value={'returncode': 143, 'cancelled': True}):
            with self.assertRaises(NetlistMeasurementError) as raised:
                read_point_parameters(self.requests, config, self.temp.name, cancel_check=lambda: True)
        self.assertTrue(raised.exception.cancelled)
        self.assertIn('cancelled', str(raised.exception))

    def test_expired_deadline_never_launches_reader(self):
        config = {'cadence': {'ocean_bin': '/bin/true'}}
        with patch('crescendo_engine.native_point_parameters.run_owned_command') as run:
            with self.assertRaisesRegex(NetlistMeasurementError, 'deadline exhausted'):
                read_point_parameters(self.requests, config, self.temp.name, deadline=time.monotonic() - 1)
            run.assert_not_called()

    def test_progress_precedes_extensions_and_parameter_calls(self):
        script = render_point_parameter_script(self.requests, self.path, 'owned',
                                                progress_path='/tmp/progress', startup_lines=['load("extension")'])
        self.assertLess(script.index('script_started'), script.index('load("extension")'))
        self.assertLess(script.index('open_results_0'), script.index('openResults('))
        self.assertLess(script.index('read_parameter_0_0'), script.index('pv('))

    def session(self, terminal='POINT_PARAMS_DONE', write_records=True):
        session = Mock()
        session.nonce = 'owned-session'
        session.control = self.temp.name
        session.workspace = {'root': self.temp.name}
        session.state = {'guardian_pid': 42}
        session.process.poll.return_value = None
        rows = [{'state': 'CLOSED', 'value': 'closed'}]
        session.status.side_effect = lambda: rows
        def send(script):
            with open(os.path.join(os.path.dirname(script), 'reader_request.json')) as stream:
                request = json.load(stream)
            token = request['token']
            if write_records:
                with open(request['paths']['records_path'], 'w') as stream:
                    stream.write(PROTOCOL + '\t' + token + '\nP\t0\t0\t0\nP\t0\t1\t27\nEND\t1\n')
                with open(request['paths']['progress_path'], 'w') as stream:
                    stream.write(token + '\tscript_complete\n')
            if terminal:
                rows.append({'state': terminal, 'value': token})
        session.send.side_effect = send
        return session

    def test_closed_owned_ocean_reads_without_spawn_exit_or_startup_replay(self):
        session = self.session()
        config = {'cadence': {'startup_extensions': ['/must/not/reload.il']}}
        with patch('crescendo_engine.native_point_parameters.run_owned_command') as launch:
            reader = prefetch_point_parameters(self.requests, config, self.temp.name, session)
        launch.assert_not_called()
        self.assertEqual(reader(self.requests), [{'SW': 0.0, 'temperature': 27.0}])
        session.cleanup.assert_not_called()
        with open(session.send.call_args[0][0]) as stream:
            script = stream.read()
        self.assertNotIn('exit(', script)
        self.assertNotIn('/must/not/reload.il', script)
        self.assertIn('pv("top-level" "SW"', script)
        with open(os.path.join(self.temp.name, 'native_point_parameters', 'reader_status.json')) as stream:
            status = json.load(stream)
        self.assertEqual(status['timeout_seconds'], 180)
        self.assertEqual(status['command']['execution_mode'], 'same_owned_ocean')
        self.assertFalse(status['command']['process_exit_observed'])
        self.assertEqual(status['stdout_path'], os.path.join(session.control, 'ocean.stdout'))

    def test_prefetched_values_reject_changed_point_test_psf_or_variable_order(self):
        reader = prefetch_point_parameters(self.requests, {}, self.temp.name, self.session())
        for field, value in [('point_id', 8), ('test', 'other'), ('psf_dir', '/other/psf'),
                             ('names', ['temperature', 'SW'])]:
            changed = copy.deepcopy(self.requests)
            changed[0][field] = value
            with self.assertRaisesRegex(NetlistMeasurementError, 'requests differ'):
                reader(changed)
        values = reader(self.requests)
        values[0]['SW'] = 100
        self.assertEqual(reader(self.requests)[0]['SW'], 0)

    def test_session_missing_records_and_native_script_error_fail_closed(self):
        for terminal in ('POINT_PARAMS_DONE', 'POINT_PARAMS_ERROR'):
            with self.subTest(terminal=terminal), self.assertRaises(NetlistMeasurementError):
                read_point_parameters(self.requests, {}, self.temp.name,
                                      native_session=self.session(terminal, write_records=False))

    def test_session_timeout_and_cancel_remain_owner_cleanup_responsibility(self):
        for cancelled in (False, True):
            session = self.session(terminal=None, write_records=False)
            with self.assertRaises(NetlistMeasurementError) as caught:
                read_point_parameters(self.requests, {}, self.temp.name, native_session=session,
                                      timeout_seconds=0.001, cancel_check=lambda: cancelled)
            detail = caught.exception.detail
            self.assertEqual(detail['last_stage'], 'script_not_observed')
            self.assertTrue(detail['command']['cancelled' if cancelled else 'timed_out'])
            session.cleanup.assert_not_called()

    def test_session_reader_requires_native_close(self):
        session = self.session()
        session.status.side_effect = lambda: [{'state': 'SIM_DONE', 'value': ''}]
        with self.assertRaisesRegex(NetlistMeasurementError, 'has not closed'):
            read_point_parameters(self.requests, {}, self.temp.name, native_session=session)
        session.send.assert_not_called()

    def test_session_transport_failure_persists_reader_receipt(self):
        session = self.session()
        session.send.side_effect = OSError('owned stdin unavailable')
        with self.assertRaisesRegex(NetlistMeasurementError, 'owned stdin unavailable'):
            read_point_parameters(self.requests, {}, self.temp.name, native_session=session)
        with open(os.path.join(self.temp.name, 'native_point_parameters', 'reader_status.json')) as stream:
            status = json.load(stream)
        self.assertIn('owned stdin unavailable', status['command']['reader_error'])
        self.assertEqual(status['last_stage'], 'script_not_observed')
        session.cleanup.assert_not_called()


if __name__ == '__main__':
    unittest.main()
