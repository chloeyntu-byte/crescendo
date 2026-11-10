"""Focused failed-capture evidence checks; no native EDA execution."""
import hashlib
import json
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine import environment_diagnostics as diagnostics


class FailedCaptureContextTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = self.temp.name

    def write(self, relative, value):
        path = os.path.join(self.root, relative)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w', encoding='utf-8') as stream:
            stream.write(json.dumps(value) if isinstance(value, dict) else value)
        return path

    def collect(self, prepared=None):
        return diagnostics.collect_run_diagnostics(self.root, 'maestro_netlist', prepared=prepared,
                                                   stop_proven=True)

    def observation(self, records, name):
        return next(row['payload'] for row in records if row['observation'] == 'diagnostic.' + name)

    def fixture(self, retained_path=None, retained_changes=None, console=None, first_error=None):
        script = self.write('numeric/capture.il', '\n'.join([
            'sstatus(errsetTrace t)',
            'crescNpRows=list(list("p1" "privateLib/privateCell/schematic_abc/M1/nfin" "5"))',
            'runId=maeRunSimulation(?waitUntilDone t ?returnRunId t ?session session)',
            "maeWaitUntilDone('All ?session session)",
            'printf("CRESCENDO_PRIVATE_NETLIST_CAPTURE_DONE\\n")']))
        default = os.path.join(self.root, 'numeric/failure_inputs/current/input_01/input.scs')
        retained = {'schema_version': 'crescendo.failed_capture_inputs.v1', 'status': 'retained',
                    'qualification': 'diagnostic_only', 'retained_scs_paths': [retained_path or default]}
        retained.update(retained_changes or {})
        error = {'message': '*Error* lineread/read: syntax error',
                 'last_successful_block': {'name': 'crescSchematicPatchResult',
                    'body_first_line': 1, 'body_last_line': 3}}
        if console is not None:
            console = console.replace('{script}', script)
            for number, line in enumerate(console.splitlines(), 1):
                if '*Error*' in line:
                    error.update(message=line.strip(), console_source='ocean.stdout', console_line=number)
                    break
        error.update(first_error or {})
        self.write('numeric/capture_diagnostics.json', {
            'schema_version': 'crescendo.native_capture_diagnostics.v1', 'capture_only': True,
            'script_path': script, 'failure_inputs': retained,
            'skill': {'ok': False, 'first_error': error}})
        if console is not None:
            self.write('numeric/ocean.stdout', console)
        return default

    def scs(self, value='DPAR_1', duplicate=False):
        text = ('parameters FN_gm=20 DPAR_1=5\n// Library name: privateLib\n'
                '// Cell name: privateCell\n// View name: schematic_abc\n'
                'M1 (d g s b) nch nfin=%s nf=FN_gm\n' % value)
        return text + ('M1 (a b c d) nch nfin=999\n' if duplicate else '')

    def test_retained_scs_survives_native_workspace_removal_and_keeps_exact_device(self):
        path = self.fixture()
        self.write(os.path.relpath(path, self.root), self.scs())
        self.write('numeric/invocations/capture.current.1',
                   '/missing/retired/native/netlist\0input.scs\0+aps\0')
        self.write('other-candidate/input.scs', self.scs('999'))
        payload = self.observation(self.collect(), 'native_run_context')
        self.assertEqual(len(payload['generated_netlists']), 1)
        observed = payload['generated_netlists'][0]
        self.assertIn('/failure_inputs/current/', observed['path'])
        self.assertEqual(observed['sha256'], hashlib.sha256(self.scs().encode('utf-8')).hexdigest())
        device = observed['selected_devices'][0]
        self.assertEqual(device['path'], 'privateLib/privateCell/schematic_abc/M1/nfin')
        self.assertEqual(device['status'], 'exact_private_CV_instance_observed')
        self.assertIn('nfin=DPAR_1 nf=FN_gm', device['statement'])
        self.assertNotIn('999', json.dumps(payload))
        self.assertIn('none proves', payload['scope'])

    def test_adjacent_phase_or_undeclared_retained_scs_has_no_current_identity(self):
        other = self.write('adjacent/failure_inputs/current/input.scs', self.scs())
        self.fixture(retained_path=other)
        self.write('numeric/failure_inputs/undeclared/input.scs', self.scs('777'))
        payload = self.observation(self.collect(), 'native_run_context')
        self.assertEqual(payload['generated_netlists'], [])

    def test_internal_non_retained_location_is_not_adopted_by_path_declaration(self):
        other = self.write('numeric/arbitrary/input.scs', self.scs())
        self.fixture(retained_path=other)
        self.assertEqual(self.observation(self.collect(), 'native_run_context')['generated_netlists'], [])

    def test_retained_contract_must_be_diagnostic_only_and_supported(self):
        for changes in ({'qualification': 'PASS'}, {'status': 'pending'}, {'schema_version': 'unknown'}):
            with self.subTest(changes=changes):
                path = self.fixture(retained_changes=changes)
                self.write(os.path.relpath(path, self.root), self.scs())
                self.assertEqual(self.observation(self.collect(), 'native_run_context')['generated_netlists'], [])

    def test_retained_symlink_and_hardlink_are_not_read_or_qualified(self):
        for hard in (False, True):
            with self.subTest(hard=hard):
                path = self.fixture()
                if os.path.lexists(path):
                    os.unlink(path)
                external = self.write('unbound-original/input.scs', self.scs())
                os.makedirs(os.path.dirname(path), exist_ok=True)
                (os.link if hard else os.symlink)(external, path)
                records = self.collect()
                self.assertEqual(self.observation(records, 'native_run_context')['generated_netlists'], [])
                self.assertEqual(self.observation(records, 'collection')['status'], 'PARTIAL')
                os.unlink(path)

    def test_retained_ambiguous_devices_remain_unknown(self):
        path = self.fixture()
        self.write(os.path.relpath(path, self.root), self.scs(duplicate=True))
        device = self.observation(self.collect(), 'native_run_context')['generated_netlists'][0]['selected_devices'][0]
        self.assertEqual(device['match_count'], 2)
        self.assertEqual(device['status'], 'UNKNOWN')
        self.assertNotIn('statement', device)

    def test_indirect_reader_uses_actual_Run_stack_and_call_instead_of_nested_patch(self):
        self.fixture(console='\n'.join([
            '*Error* lineread/read: syntax error encountered in input at line 1 column 64 in file {script}',
            '<<< Stack Trace >>>', '(... in _axlRunAllTestsWithCallback ...)',
            '(... in axlRunSimulation ...)', '(... in maeRunSimulation ...)',
            'maeRunSimulation(?waitUntilDone t ?returnRunId t ?session ... )']))
        payload = self.observation(self.collect(), 'native_run_context')
        phase = payload['phase']
        self.assertEqual(phase['attribution'], 'indirect_reader_during_native_Run')
        self.assertEqual(phase['block'], {})
        self.assertEqual(phase['lines'], [])
        self.assertEqual(phase['Run_wait_calls'][0]['line'], 3)
        self.assertEqual(phase['native_stack'][0]['line'], 3)
        self.assertIn('_axlRunAllTestsWithCallback', phase['native_stack'][0]['text'])
        self.assertEqual(phase['reported_locations'][0]['physical_line_characters'], 22)
        self.assertIn('payload not located', phase['source'])

    def test_indirect_column_without_Run_stack_does_not_invent_Run_attribution(self):
        self.fixture(console='*Error* lineread/read: syntax error encountered in input at line 1 column 64 in file {script}\n')
        phase = self.observation(self.collect(), 'native_run_context')['phase']
        self.assertNotIn('attribution', phase)
        self.assertEqual(phase['block'], {})
        self.assertIn('nested script marker', phase['source'])

    def test_native_Wait_boundary_is_not_explained_as_Run_or_patch_failure(self):
        self.fixture(console='CRESCENDO_NATIVE_RUN_STAGE Wait BEGIN\n*Error* wait_reader_failed\n',
            first_error={'native_run_boundary': {'stage': 'Wait', 'marker_line': 1,
                                                 'arguments_enabled': True}})
        phase = self.observation(self.collect(), 'native_run_context')['phase']
        self.assertEqual(phase['attribution'], 'native_Wait_boundary')
        self.assertEqual(phase['native_call_boundary']['stage'], 'Wait')
        self.assertEqual(phase['block'], {})

    def test_missing_native_stage_marker_is_not_invented(self):
        self.fixture(console='*Error* unrelated_failure\n',
            first_error={'native_run_boundary': {'stage': 'Run', 'arguments_enabled': True}})
        phase = self.observation(self.collect(), 'native_run_context')['phase']
        self.assertNotIn('native_call_boundary', phase)

    def test_indirect_reader_does_not_copy_or_execute_saved_output_expressions(self):
        self.fixture(console='*Error* lineread/read: syntax error encountered in input at line 1 column 64 in file {script}\n'
                     '(... in maeRunSimulation ...)\n')
        sentinel = os.path.join(self.root, 'must-not-execute')
        literal = 'system("touch %s") SAVED_OUTPUT_SHOULD_NOT_BE_DUMPED' % sentinel
        prepared = {'setup': {'tests': [{'name': 'current', 'enabled': True,
            'outputs': [{'name': 'out', 'expression': literal}]}]}}
        payload = self.observation(self.collect(prepared), 'native_run_context')
        self.assertNotIn('saved_Run_expressions', payload)
        self.assertNotIn('SAVED_OUTPUT_SHOULD_NOT_BE_DUMPED', json.dumps(payload))
        self.assertFalse(os.path.exists(sentinel))

    def test_preceding_Run_stack_cannot_identify_current_reader_error(self):
        self.fixture(console='(... in maeRunSimulation ...)\n'
            '*Error* lineread/read: syntax error encountered in input at line 1 column 64 in file {script}\n')
        self.assertNotIn('attribution', self.observation(self.collect(), 'native_run_context')['phase'])

    def test_Run_stack_outside_following_24_lines_is_not_associated(self):
        self.fixture(console='*Error* lineread/read: syntax error encountered in input at line 1 column 64 in file {script}\n'
            + 'ordinary output\n' * 24 + '(... in maeRunSimulation ...)\n')
        self.assertNotIn('attribution', self.observation(self.collect(), 'native_run_context')['phase'])

    def test_Run_stack_on_last_allowed_line_is_associated(self):
        self.fixture(console='*Error* lineread/read: syntax error encountered in input at line 1 column 64 in file {script}\n'
            + 'ordinary output\n' * 23 + '(... in maeRunSimulation ...)\n')
        phase = self.observation(self.collect(), 'native_run_context')['phase']
        self.assertEqual(phase['attribution'], 'indirect_reader_during_native_Run')
        self.assertEqual(phase['native_stack'][0]['line'], 25)

    def test_Run_stack_after_new_error_is_not_associated_with_first_reader(self):
        self.fixture(console='*Error* lineread/read: syntax error encountered in input at line 1 column 64 in file {script}\n'
            '*Error* unrelated subsequent failure\n(... in maeRunSimulation ...)\n')
        self.assertNotIn('attribution', self.observation(self.collect(), 'native_run_context')['phase'])

    def test_later_reader_with_Run_stack_cannot_replace_unrelated_first_error(self):
        self.fixture(console='*Error* unrelated original failure\n'
            '*Error* lineread/read: syntax error encountered in input at line 1 column 64 in file {script}\n'
            '(... in maeRunSimulation ...)\n')
        self.assertNotIn('attribution', self.observation(self.collect(), 'native_run_context')['phase'])

    def test_Run_stack_in_other_console_is_not_associated_with_first_reader(self):
        self.fixture(console='*Error* lineread/read: syntax error encountered in input at line 1 column 64 in file {script}\n')
        script = os.path.join(self.root, 'numeric/capture.il')
        self.write('numeric/ocean.stderr', '*Error* lineread/read: syntax error encountered in input at line 1 column 64 in file '
            + script + '\n(... in maeRunSimulation ...)\n')
        self.assertNotIn('attribution', self.observation(self.collect(), 'native_run_context')['phase'])

    def test_Run_stack_in_other_phase_is_not_associated_with_first_reader(self):
        self.fixture(console='*Error* lineread/read: syntax error encountered in input at line 1 column 64 in file {script}\n')
        script = os.path.join(self.root, 'numeric/capture.il')
        self.write('adjacent/ocean.stdout', '*Error* lineread/read: syntax error encountered in input at line 1 column 64 in file '
            + script + '\n(... in maeRunSimulation ...)\n')
        self.assertNotIn('attribution', self.observation(self.collect(), 'native_run_context')['phase'])

    def test_missing_or_mismatched_first_error_identity_does_not_attribute_Run(self):
        for overrides in ({'console_line': None}, {'console_source': None},
                          {'console_line': 2}, {'message': '*Error* different failure'}):
            with self.subTest(overrides=overrides):
                self.fixture(console='*Error* lineread/read: syntax error encountered in input at line 1 column 64 in file {script}\n'
                    '(... in maeRunSimulation ...)\n', first_error=overrides)
                self.assertNotIn('attribution', self.observation(self.collect(), 'native_run_context')['phase'])

    def test_large_JSON_prefix_is_not_parsed_or_misdiagnosed_as_malformed(self):
        self.write('worker_shutdown.json', {'state': 'dead', 'large': 'x' * 9000})
        real_loads = diagnostics.json.loads
        def reject_partial(value, *args, **kwargs):
            self.assertNotIn('"large"', value)
            return real_loads(value, *args, **kwargs)
        with mock.patch.object(diagnostics, 'MAX_FILE', 128), mock.patch.object(diagnostics.json, 'loads', side_effect=reject_partial):
            records = self.collect()
        index = self.observation(records, 'artifact_index')
        self.assertEqual(index['status'], 'STRUCTURED_JSON_NOT_PARSED')
        self.assertTrue(index['identity']['read_truncated'])
        self.assertLessEqual(index['identity']['read_bytes'], 129)
        self.assertEqual(index['identity']['hash_scope'], 'prefix')
        collection = self.observation(records, 'collection')
        self.assertEqual(collection['status'], 'PARTIAL')
        self.assertNotIn('JSONDecodeError', json.dumps(records))
        self.assertNotIn('"state": "dead"', json.dumps(index))

    def test_complete_malformed_JSON_still_reports_the_real_parse_error(self):
        self.write('worker_shutdown.json', '{not complete JSON}')
        self.assertIn('JSONDecodeError', json.dumps(self.collect()))

    def test_successful_capture_has_no_failure_expressions_or_Run_context(self):
        self.fixture()
        self.write('numeric/capture_diagnostics.json', {'capture_only': True, 'skill': {'ok': True}})
        records = self.collect({'setup': {'tests': [{'name': 'main', 'outputs': [{'expression': 'not-reported-on-success'}]}]}})
        self.assertFalse(any(row['observation'] == 'diagnostic.native_run_context' for row in records))
        self.assertNotIn('not-reported-on-success', json.dumps(records))


if __name__ == '__main__':
    unittest.main()
