"""Fixture-only ALPS native-history completion; no ALPS runtime claim."""
import copy
import hashlib
import os
import time
import unittest

from crescendo_engine import completion
from tests import test_completion


class NativeBaselineAlpsCompletionTests(unittest.TestCase):
    def setUp(self):
        self.fixture = test_completion.CompletionTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.fixture._native_sweep_identity()
        self.fixture.workspace['native_corner_identity']['point_parameters_required'] = True
        self.fixture.setup['tests'][0]['simulator'] = 'alps'
        self.config = {'cadence': {'simulator': 'alps', 'simulator_result_reader': 'spectre'}}
        self.start = time.time() - 1
        self.end = time.time()
        self.nonce = 'a' * 32
        self.states = [('READY', ''), ('RUN_BEGIN', ''), ('RUN_ID', 'run-17'),
                       ('SIM_DONE', ''), ('WAIT_RETURN', 't'),
                       ('HISTORY', self.fixture.history), ('CLOSED', '')]
        self.path = os.path.join(self.fixture.candidate_root, 'native_baseline', 'native_baseline_protocol.tsv')
        os.makedirs(os.path.dirname(self.path))
        for point in (1, 2):
            psf = os.path.join(self.fixture.simulation_root, 'D', 'TB', 'maestro', 'results', 'maestro',
                               self.fixture.history, str(point), 'ac_test', 'psf')
            os.unlink(os.path.join(psf, 'spectre.out'))
            with open(os.path.join(psf, 'logFile'), 'w') as stream:
                stream.write('ALPS synthetic point result metadata\n')
        self.fixture.command.update(schema_version='crescendo.maestro_baseline.v1', backend='maestro',
            execution_simulator='alps', run_id='run-17', rdb_path=self.fixture.rdb_path,
            candidate_id='fixture-candidate', guardian_returncode=0, cleanup_pending=False,
            started_epoch=self.start, finished_epoch=self.end)
        self.fixture.command.pop('returncode')
        self.write_protocol()

    def write_protocol(self):
        raw = ''.join('%s\t%s\t%s\n' % (self.nonce, state, value) for state, value in self.states).encode()
        with open(self.path, 'wb') as stream:
            stream.write(raw)
        self.fixture.command['native_baseline'] = {'schema_version': 'crescendo.maestro_baseline.v1',
            'nonce': self.nonce, 'candidate_id': 'fixture-candidate', 'request_root': os.path.abspath(self.fixture.candidate_root),
            'protocol_path': self.path, 'protocol_sha256': hashlib.sha256(raw).hexdigest(),
            'records': [{'state': state, 'value': value, 'sequence': index}
                        for index, (state, value) in enumerate(self.states)],
            'run_id': 'run-17', 'history_name': self.fixture.history,
            'started_epoch': self.start, 'finished_epoch': self.end,
            'native_session_closed': True, 'local_cleanup_proven': True}

    def validate(self):
        def reader(requests):
            result = []
            for request in requests:
                values = {'SW': request['point_id'] - 1, 'Fref': 64000000, 'temperature': 27}
                result.append({name: values[name] for name in request['names']})
            return result
        return completion.validate_candidate_completion(self.fixture.workspace, self.fixture.candidate_root,
            self.fixture.command, self.fixture.parsed, config=self.config, native_parameter_reader=reader)

    def test_exact_native_run_and_all_current_points_pass_without_cli_receipt(self):
        result = self.validate()
        self.assertEqual(len(result['simulator_runs']), 2)
        self.assertTrue(all(row['receipt']['kind'] == 'native_maestro_history_point' for row in result['simulator_runs']))
        self.assertTrue(all('returncode' not in row['receipt'] for row in result['simulator_runs']))
        self.assertTrue(result['native_parameter_readback_verified'])

    def test_idle_native_wait_nil_retains_other_completion_gates(self):
        self.states[self.states.index(('WAIT_RETURN', 't'))] = ('WAIT_RETURN', 'nil')
        self.write_protocol()
        self.assertTrue(self.validate()['ok'])

    def test_bad_rdb_point_status_and_missing_output_are_rejected(self):
        self.fixture.parsed['test_statuses'][0]['statusCode'] = 2
        with self.assertRaisesRegex(completion.CompletionEvidenceError, 'statusCode'):
            self.validate()
        self.fixture.parsed['test_statuses'][0]['statusCode'] = 3
        self.fixture.parsed['output_map']['ac_test::gain']['values'].pop()
        with self.assertRaisesRegex(completion.CompletionEvidenceError, 'missing point'):
            self.validate()

    def test_missing_identity_failed_protocol_or_cross_history_rejected(self):
        original = copy.deepcopy(self.fixture.command['native_baseline'])
        for key, value in (('history_name', 'other'), ('candidate_id', 'other'), ('request_root', '/other'),
                           ('local_cleanup_proven', False), ('protocol_sha256', 'bad')):
            self.fixture.command['native_baseline'] = copy.deepcopy(original)
            self.fixture.command['native_baseline'][key] = value
            with self.subTest(key=key), self.assertRaises(completion.CompletionEvidenceError):
                self.validate()
        self.fixture.command['native_baseline'] = original
        self.states.remove(('SIM_DONE', ''))
        self.write_protocol()
        with self.assertRaisesRegex(completion.CompletionEvidenceError, 'protocol'):
            self.validate()

    def test_stale_marker_log_or_missing_actual_parameters_are_rejected(self):
        psf = os.path.join(self.fixture.simulation_root, 'D', 'TB', 'maestro', 'results', 'maestro',
                           self.fixture.history, '1', 'ac_test', 'psf')
        marker = os.path.join(psf, '.simDone')
        os.utime(marker, (self.start - 20, self.start - 20))
        with self.assertRaisesRegex(completion.CompletionEvidenceError, 'fresh'):
            self.validate()
        os.utime(marker, None)
        log = os.path.join(psf, 'logFile')
        os.utime(log, (self.start - 20, self.start - 20))
        with self.assertRaisesRegex(completion.CompletionEvidenceError, 'stale'):
            self.validate()
        os.utime(log, None)
        with self.assertRaises(completion.CompletionEvidenceError):
            completion.validate_candidate_completion(self.fixture.workspace, self.fixture.candidate_root,
                self.fixture.command, self.fixture.parsed, config=self.config,
                native_parameter_reader=lambda requests: [{} for _ in requests])

    def test_no_exact_native_corner_identity_is_rejected(self):
        self.fixture.workspace.pop('native_corner_identity')
        with self.assertRaisesRegex(completion.UnsupportedCompletionEvidence, 'exact corner identity'):
            self.validate()

    def test_guardian_failure_or_wrong_exact_rdb_rejected(self):
        self.fixture.command['guardian_returncode'] = 1
        with self.assertRaisesRegex(completion.CompletionEvidenceError, 'guardian termination'):
            self.validate()
        self.fixture.command['guardian_returncode'] = 0
        self.fixture.command['rdb_path'] = os.path.join(self.fixture.workspace_root, 'different.rdb')
        with self.assertRaisesRegex(completion.CompletionEvidenceError, 'bound history'):
            self.validate()

    def test_changed_protocol_file_is_rejected_even_if_metadata_is_unchanged(self):
        with open(self.path, 'ab') as stream:
            stream.write(('%s\tERROR\tlate failure\n' % self.nonce).encode())
        with self.assertRaisesRegex(completion.CompletionEvidenceError, 'hash differs'):
            self.validate()
