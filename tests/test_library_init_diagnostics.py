"""Regression coverage for stage-scoped Cadence library initialization errors."""
import json
import os
from pathlib import Path
import tempfile
import time
import unittest
from datetime import datetime

from crescendo_engine.diagnostic_focus import consistency_check
from crescendo_engine.diagnostic_log import compact_report
from crescendo_engine.environment_diagnostics import _windows, collect_run_diagnostics
from crescendo_engine.native_diagnostics import skill_command_diagnostics
from crescendo_engine.netlist_measurement import _reader_console_diagnostics
from crescendo_engine.private_netlist import (
    _capture_failure_text, _capture_worker_logs, _worker_capture_invocations,
)
from crescendo_engine.runner import _diagnostic_tail_reason

FIXTURE = (Path(__file__).parent / 'fixtures' / 'library_init_ocean.txt').read_text(encoding='utf-8')
LIBRARY_LOOKUP = (Path(__file__).parent / 'fixtures' / 'library_init_library_lookup.txt').read_text(encoding='utf-8')
BEGIN = 'CRESCENDO_NATIVE_RUN_STAGE Run BEGIN'
RETURN = 'CRESCENDO_NATIVE_RUN_STAGE Run RETURN'
DONE = 'CRESCENDO_PRIVATE_NETLIST_CAPTURE_DONE'
FIRST = FIXTURE.splitlines()[4]
REAL_ERROR = '*Error* actual SKILL failure'


class LibraryInitDiagnosticsTests(unittest.TestCase):
    def scan(self, console, stderr=''):
        with tempfile.TemporaryDirectory() as root:
            with open(os.path.join(root, 'ocean.stdout'), 'w', encoding='utf-8') as stream:
                stream.write(console)
            with open(os.path.join(root, 'ocean.stderr'), 'w', encoding='utf-8') as stream:
                stream.write(stderr)
            return skill_command_diagnostics({'returncode': 0}, os.path.join(root, 'capture.il'),
                                             DONE, root, simulators=('spectre',))

    def test_caught_library_init_keeps_native_text_and_completion(self):
        report = self.scan(FIXTURE)
        self.assertTrue(report['ok'], report['first_error'])
        self.assertIsNone(report['terminal_error'])
        self.assertEqual([w['native_code'] for w in report['ocean_tool_warnings']], ['ADE-5066', 'ADE-5067'])
        warning, = report['library_init_warnings']
        self.assertEqual(warning['category'], 'library_init')
        self.assertEqual(warning['message'], FIXTURE[FIXTURE.index(FIRST):FIXTURE.index('Interactive.0')].rstrip())
        self.assertEqual(warning['completion_marker'], RETURN)
        self.assertEqual(warning['stage_begin_line'], 1)
        self.assertTrue(warning['stack_trace_truncated'])  # Cadence printed an ellipsis.
        self.assertTrue(self.scan(FIXTURE.replace('loadi(', 'load(').replace('libInit.il', 'libInit.ils'))['ok'])

    def test_library_lookup_requires_its_matching_return(self):
        lines = LIBRARY_LOOKUP.splitlines()
        returned = 'CRESCENDO_NATIVE_RUN_STAGE LibraryLookup RETURN'
        report = self.scan(LIBRARY_LOOKUP)
        self.assertTrue(report['ok'], report['first_error'])
        warning, = report['library_init_warnings']
        self.assertEqual(warning['stage_begin_line'], 1)
        self.assertEqual(warning['console_line'], 2)
        self.assertEqual(warning['completion_line'], lines.index(returned) + 1)
        self.assertEqual(warning['completion_marker'], returned)
        for replacement in ('', RETURN):
            with self.subTest(replacement=replacement):
                report = self.scan(LIBRARY_LOOKUP.replace(returned, replacement))
                self.assertFalse(report['ok'])
                self.assertEqual(report['first_error']['message'], lines[1])
                self.assertNotIn('library_init_warnings', report)

    def test_reader_retains_every_classified_library_init_warning(self):
        stage, _ = LIBRARY_LOOKUP.rsplit(DONE, 1)
        stage_lines = stage.splitlines()
        error = stage_lines[1]
        expected = ['%s Library %d.' % (error, i) for i in range(13)]
        console = ''.join(stage.replace(error, message, 1) for message in expected)
        with tempfile.TemporaryDirectory() as root:
            stdout = Path(root, 'ocean.stdout')
            stdout.write_text(console + REAL_ERROR + '\n', encoding='utf-8')
            reader = _reader_console_diagnostics({'stdout_path': str(stdout)}, ('spectre',))
        self.assertEqual(reader['reader_first_error'], REAL_ERROR)
        warnings = reader['library_init_warnings']
        self.assertEqual([w['message'].splitlines()[0] for w in warnings], expected)
        self.assertEqual([w['console_line'] for w in warnings],
                         [2 + i * len(stage_lines) for i in range(len(expected))])
        self.assertTrue(all(w['console_source'] == 'ocean.stdout' for w in warnings))

    def test_error_without_all_the_evidence_stays_fatal(self):
        for console, first, stderr in (
            (FIXTURE.replace(RETURN, ''), FIRST, ''),
            (FIXTURE.replace(RETURN, ''), FIRST, RETURN),
            (FIXTURE.replace(BEGIN, ''), FIRST, ''),
            (FIXTURE.replace('libInit.il', 'arbitrary.il'), FIRST, ''),
            (FIXTURE.replace('(... in _axlTraverse ...)', 'userFunction()'), FIRST, ''),
            (FIXTURE.replace('<<< Stack Trace >>>', 'no stack'), FIRST, ''),
            (FIXTURE.replace(FIRST, FIRST + ' line 1 column 2 in file "/run/capture.il"'),
             FIRST + ' line 1 column 2 in file "/run/capture.il"', ''),
            (FIXTURE.replace('loadi("/pdk/', 'load("/run/patch.il")\nloadi("/pdk/'), FIRST, ''),
            (FIXTURE.replace(RETURN, BEGIN + '\n' + RETURN), FIRST, ''),
            (FIXTURE.replace(RETURN, '[diagnostic byte gap: 12 bytes]\n' + RETURN), FIRST, ''),
            (FIXTURE.replace(RETURN, 'printf("' + RETURN + '\\n")'), FIRST, ''),
        ):
            with self.subTest(console=console, stderr=stderr):
                report = self.scan(console, stderr)
                self.assertFalse(report['ok'])
                self.assertEqual(report['first_error']['message'], first)
                self.assertNotIn('library_init_warnings', report)

    def test_mixed_errors_keep_the_real_failure(self):
        console = FIXTURE.replace(RETURN, REAL_ERROR + '\n' + RETURN)
        report = self.scan(console)
        self.assertFalse(report['ok'])
        self.assertEqual(report['first_error']['message'], REAL_ERROR)
        self.assertEqual(report['terminal_error']['message'], REAL_ERROR)
        self.assertEqual(len(report['library_init_warnings']), 1)
        context = _windows(console, console_source='ocean.stdout', simulators=('spectre',))
        self.assertEqual(context['first_error_line'], console.splitlines().index(REAL_ERROR) + 1)
        self.assertEqual(context['warning_count'], 3)
        self.assertEqual(_diagnostic_tail_reason({'ocean.stdout': console}, simulators=('spectre',)), REAL_ERROR)
        reader = _reader_console_diagnostics({'stdout_tail': console}, ('spectre',))
        self.assertEqual(reader['reader_first_error'], REAL_ERROR)
        self.assertEqual(reader['library_init_warnings'][0]['message'], report['library_init_warnings'][0]['message'])

    def test_focused_comparison_retains_actionable_first_errors(self):
        other = 'Required scalar unavailable'
        original = [FIXTURE, FIXTURE + REAL_ERROR, other + '\n' + FIXTURE]
        modes = ['spectre.maestro_baseline', 'spectre.maestro_netlist', 'spectre.private_netlist']
        cases = {
            mode: [{'candidate_id': 'g000_c0000', 'status': 'completed', 'values': {'x': 1},
                    'metrics': {'feasible': True, 'constraints': [], 'diagnostic_raw_points': [
                        {'test': 'test', 'corner': 'TT', 'output': 'test::v', 'value': None,
                         'first_error': first_error}]}}]
            for mode, first_error in zip(modes, original)
        }
        states = consistency_check(cases)['unpaired'][0]['output_states']
        self.assertEqual([states[mode]['first_error'] for mode in modes], [None, REAL_ERROR, original[-1]])

    def test_stack_backed_worker_stage_retains_its_own_warnings(self):
        with tempfile.TemporaryDirectory() as root:
            worker = Path(root) / 'logs_user' / 'logs0' / 'Job1.log'
            worker.parent.mkdir(parents=True)
            worker.write_text(''.join('\\o ' + line + '\n' for line in FIXTURE.splitlines()), encoding='utf-8')
            result, _ = _capture_worker_logs({'root': root}, {}, simulators=('spectre',))
            self.assertTrue(result['ok'], result)
            self.assertEqual(result['logs'][0]['library_init_warnings'][0]['category'], 'library_init')

    def test_capture_reports_publish_console_warnings(self):
        with tempfile.TemporaryDirectory() as root:
            Path(root, 'ocean.stdout').write_text(FIXTURE, encoding='utf-8')
            report = self.scan(FIXTURE)
            Path(root, 'capture_diagnostics.json').write_text(
                json.dumps({'capture_only': True, 'skill': report}), encoding='utf-8')
            records = collect_run_diagnostics(root, 'private_netlist', spec={'simulator': 'spectre'})
            warnings = [record['payload'] for record in records if record['observation'] == 'diagnostic.ocean_tool_warning']
            self.assertEqual([row.get('native_code') or row.get('category') for row in warnings],
                             ['ADE-5066', 'ADE-5067', 'library_init'])
            self.assertEqual(warnings[-1]['message'], report['library_init_warnings'][0]['message'])


class WorkerLibraryInitTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.workspace = {'root': temp.name}
        self.job = self.root / 'logs_example' / 'logs0' / 'Job1.log'
        self.job.parent.mkdir(parents=True)
        self.directory = self.root / 'simulation' / '1' / 'sample_test' / 'netlist'
        self.fixture = (Path(__file__).parent / 'fixtures' / 'library_init_worker.txt').read_text(
            encoding='utf-8').replace('@NETLIST@', str(self.directory))
        self.error = r'\e ' + FIRST
        self.capture = self.root / 'invocations'
        self.capture.mkdir()
        self.invocation = self.capture / 'capture.example.1'
        self.invocation.write_bytes(os.fsencode(str(self.directory)) + b'\0input.scs\0+escchars\0')
        self.script = str(self.root / 'capture.il')
        self.console = self.scan_console(FIXTURE)

    def scan_console(self, text):
        (self.root / 'ocean.stdout').write_text(text, encoding='utf-8')
        return skill_command_diagnostics({'returncode': 0}, self.script, DONE, str(self.root),
                                         simulators=('spectre',))

    def scan(self, text, console=None, before=None, capture_started_ns=None):
        self.job.write_text(text, encoding='utf-8')
        invocations = _worker_capture_invocations(str(self.capture), 'capture.example.', str(self.root))
        return _capture_worker_logs(self.workspace, before if before is not None else {},
            simulators=('spectre',), console=console if console is not None else self.console,
            script_path=self.script, invocations=invocations, capture_started_ns=capture_started_ns)[0]

    def test_matching_worker_warning_keeps_native_message_and_reaches_compact_report(self):
        result = self.scan(self.fixture.replace(self.error, self.error + '  '))
        self.assertTrue(result['ok'], result)
        log, = result['logs']
        self.assertEqual(log['generation_error_count'], 0)
        warning, = log['library_init_warnings']
        self.assertEqual(warning['message'], self.error + '  ')
        self.assertEqual(warning['source'], 'worker')
        self.assertEqual(warning['console_source'], str(self.job))
        self.assertEqual(warning['console_line'], 8)
        self.assertEqual(warning['capture_invocation'], str(self.invocation))
        self.assertEqual(warning['related_console_line'], 5)
        self.assertEqual(len(log['ocean_tool_warnings']), 2)

        # Two Job files with identical messages each retain their provenance.
        job2 = self.job.with_name('Job2.log')
        job2.write_text(self.fixture, encoding='utf-8')
        result = self.scan(self.fixture)
        (self.root / 'capture_diagnostics.json').write_text(json.dumps({
            'capture_only': True, 'skill': self.console, 'native_worker_diagnostics': result}),
            encoding='utf-8')
        records = collect_run_diagnostics(str(self.root), 'private_netlist', spec={'simulator': 'spectre'})
        scope = 'spectre.private_netlist'
        row = {'report_root': str(self.root), 'diagnostic_log_examples': 3,
               'case_results': {}, 'case_records': {scope: records}, 'case_contexts': {}, 'state': 'completed'}
        compact, _ = compact_report(row, [], {'cases': []}, {}, {}, [], '')
        worker_warnings = [json.loads(line.split(' ', 1)[1]) for line in compact.splitlines()
                           if line.startswith('ocean_tool_warning ')]
        worker_warnings = [w for w in worker_warnings if w.get('source') == 'worker'
                           and w.get('category') == 'library_init']
        self.assertEqual({w['console_source'] for w in worker_warnings}, {str(self.job), str(job2)})
        self.assertTrue(all(w['severity'] == 'WARN' and w['message'] == self.error for w in worker_warnings))

    def test_missing_or_nonidentical_console_evidence_keeps_worker_error(self):
        for console, text in (
            ({}, self.fixture),
            (self.console, self.fixture.replace(self.error, self.error + ' different argument')),
            (self.scan_console(FIXTURE + '[diagnostic byte gap: 15 bytes]\n'), self.fixture),
            (self.console, self.fixture.replace(self.error, self.error + '\n\\e <<< Stack Trace >>>\n\\e actualFailure()')),
        ):
            with self.subTest(console=console, text=text):
                result = self.scan(text, console=console)
                self.assertFalse(result['ok'])
                self.assertEqual(result['logs'][0]['generation_error_count'], 1)

    def test_generated_skill_error_stays_fatal_even_with_console_evidence(self):
        header = FIRST + ' in custom.il'
        console = self.scan_console(FIXTURE.replace(FIRST, header))
        self.assertTrue(console['library_init_warnings'])
        self.script = str(self.root / 'custom.il')
        result = self.scan(self.fixture.replace(FIRST, header), console=console)
        self.assertFalse(result['ok'])

    def test_missing_stage_or_completion_keeps_worker_error(self):
        lines = self.fixture.splitlines()
        end = next(line for line in lines if 'End netlisting' in line)
        stats = next(line for line in lines if 'Errors: 0' in line)
        wrapper_error = next(line for line in lines if 'ADE-3036' in line)
        for text in (
            self.fixture.replace(self.error, '').replace(end, end + '\n' + self.error),
            self.fixture.replace(self.error, '').replace(wrapper_error, wrapper_error + '\n' + self.error),
            self.fixture.replace(stats, stats.replace('Errors: 0', 'Errors: 1')),
            self.fixture.replace(stats + '\n', ''),
            self.fixture.replace(end, ''),
            self.fixture.replace(end, r'\o Begin Incremental Netlisting' + '\n' + end),
            self.fixture.replace(lines[3], ''),
            self.fixture.replace(lines[-2], ''),
            self.fixture + r'\e unexpected output after exit' + '\n',
            self.fixture.rstrip('\n'),
            self.fixture.replace(end, '[diagnostic byte gap: 15 bytes]\n' + end),
            self.fixture.replace(end, '\0' + end),
        ):
            with self.subTest(text=text):
                result = self.scan(text)
                self.assertFalse(result['ok'])
                self.assertNotIn('library_init_warnings', result['logs'][0])

    def test_wrapper_formatting_noise_stays_aftermath(self):
        line = r'\o *Error*   Error ID  = 5031'
        noise = r'\e *Error* eval: not a function - *WARNING* woport: bad port value -- , reset to stdout'
        result = self.scan(self.fixture.replace(line, line + '\n' + noise))
        self.assertTrue(result['ok'])
        self.assertEqual(result['logs'][0]['expected_wrapper_aftermath_error_count'], 5)

    def test_missing_stale_or_wrong_point_invocation_does_not_prove_completion(self):
        data = self.invocation.read_bytes()
        self.invocation.unlink()
        stale = self.invocation.with_name('capture.old.1')
        stale.write_bytes(data)
        self.assertFalse(self.scan(self.fixture)['ok'])
        self.invocation.write_bytes(data)
        self.assertFalse(self.scan(self.fixture.replace('(0 1)', '(0 2)'))['ok'])
        self.invocation.write_bytes(data[:-1])
        self.assertFalse(self.scan(self.fixture)['ok'])

    def test_only_fresh_netlist_can_replace_invocation_evidence(self):
        self.invocation.unlink()
        self.directory.mkdir(parents=True)
        netlist = self.directory / 'input.scs'
        netlist.write_text('simulator lang=spectre\n', encoding='utf-8')
        self.assertFalse(self.scan(self.fixture, capture_started_ns=int(time.time() * 1e9) + 1)['ok'])
        result = self.scan(self.fixture, capture_started_ns=netlist.stat().st_mtime_ns)
        self.assertTrue(result['ok'])
        self.assertEqual(result['logs'][0]['library_init_warnings'][0]['netlist_path'], str(netlist))

    def test_mixed_failure_keeps_actual_first_error(self):
        for failure in (r'\e ' + REAL_ERROR, r"\o ERROR (ADE-5066): Tool 'spectre' has not been registered."):
            result = self.scan(self.fixture.replace(self.error, self.error + '\n' + failure))
            log, = result['logs']
            self.assertFalse(result['ok'])
            self.assertEqual(log['generation_error_count'], 1)
            self.assertEqual(_capture_failure_text(self.console, result), failure)
            self.assertEqual(len(log['library_init_warnings']), 1)

    def test_appended_lines_use_prior_phase_and_keep_original_line_numbers(self):
        header, tail = self.fixture.split(self.error, 1)
        self.job.write_text(header, encoding='utf-8')
        _, before = _capture_worker_logs(self.workspace)
        result = self.scan(header + self.error + tail, before=before)
        self.assertTrue(result['ok'])
        warning, = result['logs'][0]['library_init_warnings']
        self.assertEqual(warning['console_line'], header.count('\n') + 1)
        self.assertEqual(warning['stage_begin_line'], 4)
        self.assertEqual(result['logs'][0]['new_bytes_from'], len(header.encode('utf-8')))

        self.job.write_text(header.replace('Jan 1', 'Jan 2'), encoding='utf-8')
        _, before = _capture_worker_logs(self.workspace)
        self.assertFalse(self.scan(self.fixture, before=before)['ok'])

    def test_reused_job_filename_requires_a_fresh_native_start_record(self):
        self.job.write_text('previous worker log\n', encoding='utf-8')
        _, before = _capture_worker_logs(self.workspace)
        stamp = datetime.utcfromtimestamp(time.time() - 2).strftime('%Y.%m.%d %H:%M:%S.%f')
        text = r'\# Program start time:     UTC ' + stamp + '\n' + self.fixture
        cutoff = int((time.time() - 60) * 1e9)
        self.assertTrue(self.scan(text, before=before, capture_started_ns=cutoff)['ok'])
        stale = text.replace(stamp, '2000.01.01 00:00:00.000')
        self.assertFalse(self.scan(stale, before=before, capture_started_ns=cutoff)['ok'])

    def test_more_than_twelve_worker_warnings_are_reported(self):
        result = self.scan(self.fixture.replace(self.error, '\n'.join([self.error] * 27)))
        self.assertTrue(result['ok'])
        self.assertEqual(len(result['logs'][0]['library_init_warnings']), 27)
