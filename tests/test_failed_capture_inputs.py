"""Failed native inputs are retained as bounded evidence, never run authority."""
import json
import os
import re
import shutil
import tempfile
import unittest
from unittest import mock

from crescendo_engine import private_netlist as native


class FailedCaptureInputTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.workspace = os.path.join(self.temp.name, 'workspace')
        self.phase = os.path.join(self.temp.name, 'numeric')
        self.records = os.path.join(self.phase, 'invocations')
        self.netlist = os.path.join(self.workspace, 'simulation', '1', 'tb', 'netlist')
        os.makedirs(self.records)
        os.makedirs(self.netlist)
        self.prefix = 'capture.' + 'a' * 32 + '.'
        self.data = 'simulator lang=spectre\nparameters x=1\nR0 (1 0) resistor r=1k\n'
        self.write(os.path.join(self.netlist, 'input.scs'), self.data)
        self.description = {'root': self.workspace, 'setup_dir': os.path.join(self.workspace, 'setup')}

    def write(self, path, value):
        with open(path, 'w') as stream:
            stream.write(value)

    def record(self, name=None, cwd=None, argument='input.scs', arguments=None):
        path = os.path.join(self.records, name or self.prefix + '123')
        with open(path, 'wb') as stream:
            values = [cwd or self.netlist, argument] + (['+aps'] if arguments is None else arguments)
            stream.write(('\0'.join(values) + '\0').encode('utf-8'))
        return path

    def collect(self):
        return native._retain_failed_capture_inputs(self.description, self.phase, self.records, self.prefix)

    def test_scs_is_copied_independently_and_survives_owned_workspace_cleanup(self):
        self.record()
        result = self.collect()
        self.assertEqual(result['status'], 'retained')
        self.assertEqual(result['qualification'], 'diagnostic_only')
        self.assertEqual(result['retained_file_count'], 1)
        target = result['retained_scs_paths'][0]
        shutil.rmtree(self.workspace)
        with open(target) as stream:
            self.assertEqual(stream.read(), self.data)
        with open(result['manifest_path']) as stream:
            manifest = json.load(stream)
        self.assertNotIn('ready', manifest)
        self.assertNotIn('prepared_id', manifest)
        self.assertEqual(manifest['files'][0]['source'], os.path.join(self.netlist, 'input.scs'))

    def test_only_this_wrapper_prefix_is_collected(self):
        self.record(name='capture.' + 'b' * 32 + '.123')
        self.record(name='capture.123')
        self.assertEqual(self.collect()['retained_scs_paths'], [])
        self.prefix = 'capture.' + 'c' * 32 + '.'
        self.record()
        self.assertEqual(self.collect()['retained_file_count'], 1)

    def test_local_literal_include_ahdl_and_stimulus_dependencies_are_retained(self):
        self.write(os.path.join(self.netlist, 'input.scs'),
            'include "child.scs"\nahdl_include "device.va"\nV0 (1 0) vsource file="stim.txt"\n')
        for name in ('child.scs', 'device.va', 'stim.txt'):
            self.write(os.path.join(self.netlist, name), '// owned evidence\n')
        self.record()
        result = self.collect()
        self.assertEqual(result['retained_file_count'], 4)
        with open(result['manifest_path']) as stream:
            manifest = json.load(stream)
        self.assertEqual(len(manifest['records'][0]['dependencies']), 3)

    def test_external_pdk_reference_is_recorded_without_reading_or_copying_it(self):
        external = os.path.join(self.temp.name, 'pdk.scs')
        self.write(external, 'do not copy PDK model bytes')
        self.write(os.path.join(self.netlist, 'input.scs'), 'include "%s" section=tt\n' % external)
        self.record()
        result = self.collect()
        self.assertEqual(result['retained_file_count'], 1)
        with open(result['manifest_path']) as stream:
            detail = json.load(stream)
        dependency = detail['records'][0]['dependencies'][0]
        self.assertEqual(dependency['status'], 'external_or_unresolved_not_copied')
        self.assertEqual(dependency['path'], external)

    def test_recorded_I_context_does_not_call_missing_pdk_include_a_missing_local_input(self):
        external = os.path.join(self.temp.name, 'PDK', 'spectre')
        os.makedirs(external)
        model = os.path.join(external, 'toplevel.scs')
        self.write(model, 'external model bytes must stay external\n')
        self.write(os.path.join(self.netlist, 'input.scs'), 'include "toplevel.scs" section=TOP_TT_RFTYP\n')
        self.record(arguments=['+aps', '-I' + external])
        original = native.os.lstat
        def no_external_inspection(path, *args, **kwargs):
            self.assertNotEqual(os.path.abspath(path), model)
            return original(path, *args, **kwargs)
        with mock.patch.object(native.os, 'lstat', side_effect=no_external_inspection):
            result = self.collect()
        self.assertEqual(result['retained_file_count'], 1)
        self.assertEqual(result['reasons'], [])
        self.assertEqual(result['include_search_contexts'][0]['include_dirs'], [external])
        sample = result['dependency_samples'][0]
        self.assertEqual(sample['resolution_status'], 'external_or_literal_search_unverified')
        self.assertEqual(sample['search_candidates'][1], {'path': model,
            'scope': 'external_or_unresolved', 'observation': 'external_not_inspected'})
        self.assertEqual(sample['qualification'], 'diagnostic_only')

    def test_owned_include_search_uses_recorded_cwd_then_split_and_joined_I_order(self):
        first, second = [os.path.join(self.workspace, name) for name in ('first', 'second')]
        for directory in (first, second):
            os.makedirs(directory)
            self.write(os.path.join(directory, 'child.scs'), '// ' + directory + '\n')
        self.write(os.path.join(self.netlist, 'input.scs'), 'include "child.scs"\n')
        self.record(arguments=['-I', first, '-I' + second])
        result = self.collect()
        self.assertEqual(result['retained_file_count'], 2)
        self.assertEqual(result['reasons'], [])
        with open(result['manifest_path']) as stream:
            manifest = json.load(stream)
        self.assertEqual(manifest['records'][0]['include_search_context']['include_dirs'], [first, second])
        self.assertEqual(manifest['records'][0]['dependencies'][0]['path'], os.path.join(first, 'child.scs'))
        self.assertNotIn(os.path.join(second, 'child.scs'), [row['source'] for row in manifest['files']])

    def test_include_does_not_assume_source_directory_is_captured_cwd(self):
        child = os.path.join(self.netlist, 'nested')
        os.mkdir(child)
        self.write(os.path.join(child, 'input.scs'), 'include "child.scs"\n')
        self.write(os.path.join(self.netlist, 'child.scs'), '// actual capture cwd dependency\n')
        self.record(argument='nested/input.scs')
        result = self.collect()
        self.assertEqual(result['retained_file_count'], 2)
        self.assertEqual(result['reasons'], [])
        with open(result['manifest_path']) as stream:
            manifest = json.load(stream)
        self.assertEqual(manifest['records'][0]['dependencies'][0]['path'], os.path.join(self.netlist, 'child.scs'))

    def test_external_I_predecessor_prevents_claiming_a_later_owned_candidate_is_the_actual_model(self):
        external = os.path.join(self.temp.name, 'external')
        later = os.path.join(self.workspace, 'later')
        os.mkdir(later)
        self.write(os.path.join(later, 'child.scs'), '// not proven to be the selected model\n')
        self.write(os.path.join(self.netlist, 'input.scs'), 'include "child.scs"\n')
        self.record(arguments=['-I', external, '-I', later])
        result = self.collect()
        self.assertEqual(result['retained_file_count'], 1)
        self.assertEqual(result['reasons'], [])
        self.assertEqual(result['dependency_samples'][0]['resolution_status'], 'external_or_literal_search_unverified')
        self.assertEqual(result['dependency_samples'][0]['search_candidates'][-1]['observation'], 'owned_regular_candidate')

    def test_unexpanded_native_I_does_not_use_the_cr_process_environment(self):
        self.write(os.path.join(self.netlist, 'input.scs'), 'include "toplevel.scs"\n')
        self.record(arguments=['-I', '${PDK}/spectre'])
        with mock.patch.dict(os.environ, {'PDK': '/wrong/process/environment'}):
            result = self.collect()
        self.assertEqual(result['retained_file_count'], 1)
        self.assertFalse(result['include_search_contexts'][0]['complete'])
        self.assertEqual(result['include_search_contexts'][0]['include_dirs'], [])
        self.assertNotIn('/wrong/process/environment', json.dumps(result))

    def test_owned_dependency_link_is_recorded_but_never_copied(self):
        external = os.path.join(self.temp.name, 'external.scs')
        self.write(external, '// external model bytes\n')
        os.symlink(external, os.path.join(self.netlist, 'child.scs'))
        self.write(os.path.join(self.netlist, 'input.scs'), 'include "child.scs"\n')
        self.record()
        result = self.collect()
        self.assertEqual(result['retained_file_count'], 1)
        self.assertEqual(result['dependency_samples'][0]['search_candidates'][0]['observation'], 'owned_candidate_not_retained')

    def test_external_netlist_cannot_be_collected_from_an_owned_record(self):
        self.write(os.path.join(self.temp.name, 'input.scs'), self.data)
        self.record(cwd=self.temp.name)
        result = self.collect()
        self.assertEqual(result['retained_file_count'], 0)
        self.assertTrue(any('outside owned' in item for item in result['reasons']))

    def test_symlink_netlist_and_hardlink_netlist_are_not_copied(self):
        source = os.path.join(self.netlist, 'input.scs')
        original = source + '.original'
        os.rename(source, original)
        os.symlink(original, source)
        self.record()
        self.assertEqual(self.collect()['retained_file_count'], 0)
        os.unlink(source)
        os.link(original, source)
        self.prefix = 'capture.' + 'c' * 32 + '.'
        self.record()
        self.assertEqual(self.collect()['retained_file_count'], 0)

    def test_symlink_ancestor_netlist_cannot_escape_the_ownership_boundary(self):
        alias = os.path.join(self.workspace, 'alias')
        os.symlink(self.netlist, alias)
        self.record(cwd=alias)
        result = self.collect()
        self.assertEqual(result['retained_file_count'], 0)
        self.assertTrue(result['reasons'])

    def test_symlink_invocation_is_rejected(self):
        original = self.record(name='old')
        os.symlink(original, os.path.join(self.records, self.prefix + '123'))
        self.assertEqual(self.collect()['retained_file_count'], 0)

    def test_symlink_destination_is_never_followed(self):
        os.symlink(self.workspace, os.path.join(self.phase, 'failure_inputs'))
        self.record()
        result = self.collect()
        self.assertEqual(result['retained_file_count'], 0)
        self.assertFalse(os.path.exists(os.path.join(self.workspace, 'manifest.json')))

    def test_invalid_missing_and_oversized_invocations_return_bounded_reasons(self):
        for index, raw in enumerate((b'bad', b'\xff\0input.scs\0', b'x' * (256 * 1024 + 1))):
            with open(os.path.join(self.records, self.prefix + str(index)), 'wb') as stream:
                stream.write(raw)
        self.record(name=self.prefix + '33', argument='missing.scs')
        result = self.collect()
        self.assertEqual(result['retained_file_count'], 0)
        self.assertLessEqual(len(result['reasons']), 16)
        self.assertTrue(result['reasons'])

    def test_oversized_netlist_is_not_read(self):
        with open(os.path.join(self.netlist, 'input.scs'), 'wb') as stream:
            stream.truncate(8 * 1024 * 1024 + 1)
        self.record()
        result = self.collect()
        self.assertEqual(result['retained_file_count'], 0)
        self.assertTrue(any('bounded' in item for item in result['reasons']))

    def test_changed_input_is_rejected_without_retained_scs_authority(self):
        self.record()
        original = native._regular
        calls = [0]
        def changing(path, *args, **kwargs):
            result = original(path, *args, **kwargs)
            if path.endswith('input.scs'):
                calls[0] += 1
                if calls[0] == 2:
                    self.write(path, self.data + '// changed while inspecting\n')
            return result
        with mock.patch.object(native, '_regular', side_effect=changing):
            result = self.collect()
        self.assertEqual(result['retained_file_count'], 0)
        self.assertTrue(any('changed' in item for item in result['reasons']))

    def test_owned_native_results_are_allowed_only_after_native_root_qualification(self):
        external = os.path.join(self.temp.name, 'private_view', 'results', 'maestro', '1', 'tb', 'netlist')
        os.makedirs(external)
        self.write(os.path.join(external, 'input.scs'), self.data)
        self.record(cwd=external)
        with mock.patch.object(native, 'capture_rdb_root', return_value=os.path.dirname(os.path.dirname(os.path.dirname(external)))):
            result = self.collect()
        self.assertEqual(result['retained_file_count'], 1)

    def test_invalid_native_root_does_not_grant_access_outside_workspace(self):
        self.record(cwd=self.temp.name)
        self.write(os.path.join(self.temp.name, 'input.scs'), self.data)
        with mock.patch.object(native, 'capture_rdb_root', side_effect=native.PrivateNetlistError('ownership unproven')):
            result = self.collect()
        self.assertEqual(result['retained_file_count'], 0)

    def test_duplicate_dependencies_are_copied_once_per_record(self):
        self.write(os.path.join(self.netlist, 'input.scs'), 'include "child.scs"\ninclude "child.scs"\n')
        self.write(os.path.join(self.netlist, 'child.scs'), 'include "input.scs"\n')
        self.record()
        self.assertEqual(self.collect()['retained_file_count'], 2)

    def test_collection_failure_never_raises_or_replaces_a_caller_error(self):
        self.record()
        with mock.patch.object(native, 'atomic_write_json', side_effect=OSError('disk full')):
            result = self.collect()
        self.assertEqual(result['status'], 'retained')
        self.assertTrue(any('disk full' in item for item in result['reasons']))

    def test_retained_failed_scs_is_visible_in_the_single_log_and_gui_section_after_cleanup(self):
        from crescendo_engine.environment_diagnostics import retained_artifact_index
        from crescendo_engine.diagnosis_catalog import format_section, read_section, stage_section
        self.record()
        result = self.collect()
        target = result['retained_scs_paths'][0]
        shutil.rmtree(self.workspace)
        context = dict(retained_artifact_index(self.temp.name), case_id='spectre.maestro_netlist',
            first_cause={'failure_stage': 'native_netlist_preparation', 'message': 'Native command exited 134'})
        self.assertIn(target, [row['path'] for row in context['netlists']])
        check = {'id': 'spectre.maestro_netlist.execution', 'check_id': 'execution',
            'simulator': 'spectre', 'engine': 'maestro_netlist', 'status': 'FAIL'}
        section = format_section([check], context, compact=True)
        self.assertIn(target, section)
        log = os.path.join(self.temp.name, 'environment_test.log')
        with open(log, 'w', encoding='utf-8') as stream:
            stream.write('run_context ' + json.dumps({'scope': 'spectre.maestro_netlist', 'context': context}) + '\n' + section)
        # The GUI detail/copy action uses this same bounded section reader.
        detail = read_section(log, 'spectre.maestro_netlist', stage_section(context['first_cause']))
        self.assertIn(target, detail)
        self.assertIn('134', detail)
        self.assertTrue(os.path.isfile(target))


class FailedCaptureBoundaryTests(unittest.TestCase):
    def test_rc134_still_raises_and_keeps_scs_even_without_rdb_or_valid_parameters(self):
        self._capture_failure()

    def test_stop_still_cancels_and_retains_current_wrapper_inputs(self):
        self._capture_failure(cancelled=True)

    def test_optional_environment_sampling_is_generated_only_for_diagnostic_capture(self):
        self._capture_failure(diagnostic=True)

    def _capture_failure(self, cancelled=False, diagnostic=False):
        with tempfile.TemporaryDirectory() as root:
            workspace = {'root': os.path.join(root, 'workspace'), 'primary_library': 'PRIVATE',
                'setup_cell': 'TB', 'setup_view': 'maestro', 'setup': {'tests': [{'name': 'tb'}]}}
            os.mkdir(workspace['root'])
            phase = os.path.join(root, 'numeric')
            cwd = os.path.join(workspace['root'], 'simulation', '1', 'tb', 'netlist')
            os.makedirs(cwd)
            with open(os.path.join(cwd, 'input.scs'), 'w') as stream:
                stream.write('simulator lang=spectre\nparameters unassigned\n')
            command = {'returncode': 134, 'guardian_cleanup': {'state': 'dead'}}
            stopped = [False]
            def failed_command(*args, **kwargs):
                with open(os.path.join(workspace['root'], 'bin', 'spectre')) as stream:
                    wrapper = stream.read()
                prefix = re.search(r'/(capture\.[a-f0-9]{32}\.)\$\$', wrapper).group(1)
                with open(os.path.join(phase, 'invocations', prefix + '123'), 'wb') as stream:
                    stream.write((cwd + '\0input.scs\0').encode('utf-8'))
                stopped[0] = cancelled
                return command
            with mock.patch.object(native, '_capture_setup_identity', return_value={}), \
                    mock.patch.object(native, 'reset_results'), \
                    mock.patch.object(native, 'write_schematic_patch', return_value={'parameters': []}), \
                    mock.patch.object(native, '_workspace_startup_load_lines', return_value=[]), \
                    mock.patch.object(native, 'workspace_test_design_lines', return_value=[]), \
                    mock.patch.object(native, 'native_corner_schedule_skill', return_value=[]), \
                    mock.patch.object(native, 'child_environment', return_value=dict(os.environ)), \
                    mock.patch('crescendo_engine.netlist_measurement.run_owned_command', side_effect=failed_command), \
                    mock.patch('crescendo_engine.native_diagnostics.skill_command_diagnostics', return_value={'ok': False}), \
                    self.assertRaises(native.PrivateNetlistError if cancelled else native.PrivateNetlistUnsupported) as caught:
                native._capture_phase(workspace, [], {}, {}, {}, {'cadence': {'ocean_bin': '/bin/true'}, 'runtime': {},
                    '_diagnostic_native_run_trace': diagnostic}, phase,
                    cancel_check=lambda: stopped[0])
            detail = caught.exception.detail
            self.assertEqual(detail['command']['returncode'], 134)
            self.assertEqual(detail['failure_inputs']['qualification'], 'diagnostic_only')
            self.assertTrue(detail['native_worker_diagnostics']['parent_capture_failed'])
            with open(os.path.join(phase, 'native_worker_diagnostics.json')) as stream:
                self.assertTrue(json.load(stream)['parent_capture_failed'])
            self.assertTrue(os.path.isfile(detail['failure_inputs']['retained_scs_paths'][0]))
            self.assertNotIn('ready', detail)
            with open(os.path.join(phase, 'capture.il')) as stream:
                script = stream.read()
            self.assertEqual('procedure(crescObserveCaptureEnvironment(test)' in script, diagnostic)
            if diagnostic:
                self.assertLess(script.index('maeOpenSetup('), script.index('crescObserveCaptureEnvironment("tb")'))
                self.assertLess(script.index('crescObserveCaptureEnvironment("tb")'), script.index('maeRunSimulation('))
            if cancelled:
                self.assertIn('cancelled', str(caught.exception))
                self.assertEqual(detail['failure_stage'], 'native_netlist_cancelled')


if __name__ == '__main__':
    unittest.main()
