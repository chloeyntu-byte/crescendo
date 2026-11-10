"""Evidence needed to distinguish reader stalls from native solver failures."""
import json
import os
import tempfile
import unittest

from crescendo_engine.environment_diagnostics import collect_run_diagnostics, _windows, failure_layer
from crescendo_engine.diagnosis_catalog import observation_section, stage_section


class ReadbackDiagnosticTests(unittest.TestCase):
    def test_reader_failure_is_result_readback_not_solver_failure(self):
        failure = {'failure_stage': 'native_point_parameter_readback'}
        self.assertEqual(stage_section(failure), 'results')
        self.assertEqual(failure_layer(failure), 'measurement')
        self.assertEqual(stage_section({'failure_stage': 'source_guard'}), 'netlist')

    def test_readback_script_progress_and_missing_records_are_explicit(self):
        with tempfile.TemporaryDirectory() as root:
            relative = 'candidates/g000_c0000/native_point_parameters'
            folder = os.path.join(root, relative)
            os.makedirs(folder)
            script = '\n'.join('line_%d' % i for i in range(70))
            for name, text in [('read_point_parameters.il', script),
                               ('reader_progress.tsv', 'BEGIN\tstartup\nBEGIN\topen_results\n'),
                               ('reader_status.json', json.dumps({'status': 'timed_out', 'last_stage': 'open_results'}))]:
                with open(os.path.join(folder, name), 'w') as stream:
                    stream.write(text)
            records = collect_run_diagnostics(root, 'maestro_baseline')
            reader = [r for r in records if r['observation'] == 'diagnostic.point_parameter_reader']
            entry = next(r for r in reader if r['payload']['path'].endswith('.il'))
            self.assertEqual(len(entry['payload']['context']['lines']), 70)
            self.assertIn('line_35', str(entry))
            self.assertTrue(entry['payload']['context']['complete_text_included'])
            inventory = next(r['payload'] for r in records if r['observation'] == 'diagnostic.point_parameter_reader_files')
            self.assertEqual(next(x['status'] for x in inventory['files'] if x['name'] == 'parameters.tsv'), 'not_produced')
            self.assertTrue(any(r['payload'].get('path', '').endswith('reader_status.json') for r in records))
            self.assertEqual(observation_section(entry), 'results')

    def test_reader_artifacts_are_bound_to_reader_directory(self):
        with tempfile.TemporaryDirectory() as root:
            with open(os.path.join(root, 'parameters.tsv'), 'w') as stream:
                stream.write('unrelated private data')
            records = collect_run_diagnostics(root, 'maestro_baseline')
            self.assertNotIn('unrelated private data', json.dumps(records))

    def test_reader_symlink_content_is_not_followed(self):
        with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as outside:
            target = os.path.join(outside, 'parameters.tsv')
            with open(target, 'w') as stream:
                stream.write('external private data')
            folder = os.path.join(root, 'native_point_parameters')
            os.mkdir(folder)
            os.symlink(target, os.path.join(folder, 'parameters.tsv'))
            records = collect_run_diagnostics(root, 'maestro_baseline')
            self.assertNotIn('external private data', json.dumps(records))
            summary = next(r['payload'] for r in records if r['observation'] == 'diagnostic.collection')
            self.assertTrue(summary['omitted_or_truncated'])

    def test_native_warning_codes_do_not_become_solver_errors(self):
        text = 'WARNING (SFE-2322): duplicate model ignored\nWARNING (SPECTRE-17101): tolerance\nspectre completes with 0 errors, 2 warnings, and 0 notices.\n'
        context = _windows(text, console_source='native_storage/psf/spectre.out')
        self.assertEqual(context['error_count'], 0)
        self.assertEqual(context['warning_count'], 2)
        self.assertIsNone(context['first_error_line'])
        self.assertIn('duplicate model ignored', str(context['lines']))

    def test_actual_solver_error_survives_warning_classification(self):
        text = 'WARNING (SFE-2322): duplicate model ignored\nERROR (SFE-23): missing model\n*Error* readback failed\n'
        context = _windows(text, console_source='native_storage/psf/spectre.out')
        self.assertEqual(context['error_count'], 2)
        self.assertEqual(context['first_error_line'], 2)
        self.assertEqual(context['warning_count'], 1)


if __name__ == '__main__':
    unittest.main()
