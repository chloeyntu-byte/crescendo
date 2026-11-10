"""Keep early source/file evidence visible without qualifying an unrun solver."""
import copy
import hashlib
import json
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine import environment_diagnostics as diagnostics
from crescendo_engine.diagnosis_catalog import observation_section, stage_section, format_section
from crescendo_engine.workspace import compact_source_guard, tree_content_fingerprint, verify_sources_unchanged


class SourceFileDiagnosticTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = self.temp.name

    def write(self, path, value):
        target = os.path.join(self.root, path)
        os.makedirs(os.path.dirname(target), exist_ok=True)
        with open(target, 'w', encoding='utf-8') as stream:
            stream.write(value if isinstance(value, str) else json.dumps(value))
        return target

    def collect(self):
        return diagnostics.collect_run_diagnostics(self.root, 'maestro_netlist', stop_proven=True)

    def observation(self, records, name):
        return [row['payload'] for row in records if row['observation'] == 'diagnostic.' + name]

    def source_failure(self):
        source = os.path.join(self.root, 'original')
        path = self.write('original/TB/schematic/sch.oa', 'source before')
        workspace = {'source_libraries': {'design': source}, 'source_snapshots': {
            'design': tree_content_fingerprint(source, ['TB/schematic'])}}
        with open(path, 'w') as stream:
            stream.write('source after!')
        guard = compact_source_guard(verify_sources_unchanged(workspace))
        failure = {'error': 'ValueError: source dependencies changed: design/TB/schematic',
            'candidate_id': 'g000_c0000', 'detail': {'failure_stage': 'source_freshness',
                'execution_started': False, 'native_run_invoked': False, 'source_guard': guard}}
        self.write('candidates/g000_c0000/execution_diagnostics.json', failure)
        return guard, failure

    def file_inputs(self):
        return {'schema_version': 'crescendo.native_file_inputs.v1', 'deferred_count': 1,
            'deferred_omitted': 0, 'deferred': [{'line': 321, 'source': '/owned/input.scs',
                'parameter': 'file', 'expression': 'strcat(BASE, "/rf.s45p")',
                'statement': 'N1 (a b) nport file=strcat(BASE, "/rf.s45p")',
                'reason': 'expression_deferred_to_simulator'}], 'resolved_parameters': [],
            'resolved_parameter_count': 0, 'interpretation': 'simulator',
            'native_cwd': '/native/netlist', 'execution_cwd': '/owned/task',
            'include_dirs': ['/native/models'], 'snapshot_count': 0, 'static_identity_count': 0,
            'path_policy': 'literal_inputs_use_captured_search_order; expressions_preserved'}

    def test_large_shutdown_does_not_hide_small_early_source_failure(self):
        guard, failure = self.source_failure()
        self.write('worker_shutdown.json', {'source_guard': guard, 'catalogue': 'x' * (8 * 1024 * 1024)})
        records = self.collect()
        evidence = self.observation(records, 'source_guard')
        self.assertEqual(len(evidence), 1)
        self.assertEqual(evidence[0]['source_guard'], guard)
        self.assertEqual(evidence[0]['qualification'], 'diagnostic_only')
        failure = self.observation(records, 'producer_failure')[0]['data']['detail']
        self.assertEqual(failure['failure_stage'], 'source_freshness')
        self.assertFalse(failure['native_run_invoked'])
        self.assertFalse(failure['execution_started'])
        self.assertFalse(failure['source_guard']['ok'])
        self.assertEqual(failure['source_guard']['sha256'], evidence[0]['sha256'])
        self.assertEqual(diagnostics.failure_layer(failure), 'netlist_generation')
        self.assertEqual(stage_section(failure), 'netlist')
        collection = self.observation(records, 'collection')[0]
        self.assertEqual(collection['status'], 'PARTIAL')
        self.assertTrue(any(row.get('path') == 'worker_shutdown.json'
            and row.get('status') == 'STRUCTURED_JSON_NOT_PARSED' for row in collection['omitted_or_truncated']))
        text = json.dumps(records)
        for content in (b'source before', b'source after!'):
            self.assertEqual(text.count(hashlib.sha256(content).hexdigest()), 1)
        self.assertLess(len(text), 35000)
        self.assertEqual(diagnostics.MAX_FILE, 2 * 1024 * 1024)
        self.assertEqual(diagnostics.MAX_READ_BYTES, 32 * 1024 * 1024)

    def test_source_and_native_file_records_precede_large_cleanup_and_do_not_duplicate(self):
        guard, failure = self.source_failure()
        inputs = self.file_inputs()
        failure['detail']['native_file_inputs'] = copy.deepcopy(inputs)
        self.write('candidates/g000_c0000/execution_diagnostics.json', failure)
        self.write('source_guard.json', guard)
        self.write('tasks/current/native_file_inputs.json', inputs)
        self.write('a_cleanup/worker_shutdown.json', {'padding': 'x' * (8 * 1024 * 1024)})
        reads = []
        original = diagnostics._read
        def observed(path, *args, **kwargs):
            reads.append(os.path.basename(path))
            return original(path, *args, **kwargs)
        with mock.patch.object(diagnostics, '_read', side_effect=observed):
            records = self.collect()
        self.assertLess(reads.index('source_guard.json'), reads.index('worker_shutdown.json'))
        self.assertLess(reads.index('native_file_inputs.json'), reads.index('worker_shutdown.json'))
        self.assertEqual(len(self.observation(records, 'source_guard')), 1)
        files = self.observation(records, 'native_file_inputs')
        self.assertEqual(len(files), 1)
        self.assertEqual(files[0]['native_file_inputs']['deferred'][0]['line'], 321)
        self.assertEqual(files[0]['native_file_inputs']['deferred'][0]['expression'], inputs['deferred'][0]['expression'])
        self.assertEqual(files[0]['native_file_inputs']['deferred_count'], 1)
        for row in records:
            if row['observation'] in ('diagnostic.source_guard', 'diagnostic.native_file_inputs'):
                self.assertEqual(observation_section(row), 'netlist')
        text = json.dumps(records)
        self.assertEqual(text.count('expression_deferred_to_simulator'), 1)

    def test_references_exist_only_after_full_record_is_retained(self):
        guard, failure = self.source_failure()
        guard['padding'] = 'evidence-within-file-bound-' * 400
        failure['detail']['source_guard'] = guard
        self.write('candidates/g000_c0000/execution_diagnostics.json', failure)
        with mock.patch.object(diagnostics, 'MAX_TOTAL', 200 * 1024):
            records = self.collect()
        self.assertFalse(self.observation(records, 'source_guard'))
        self.assertNotIn('"evidence_ref": "diagnostic.source_guard"', json.dumps(records))
        collection = self.observation(records, 'collection')[0]
        self.assertTrue(any(row.get('kind') == 'source_guard' for row in collection['omitted_or_truncated']))
        oversized = copy.deepcopy(guard)
        oversized['padding'] = 'x' * 17000
        failure['detail']['source_guard'] = oversized
        self.write('candidates/g000_c0000/execution_diagnostics.json', failure)
        records = self.collect()
        self.assertFalse(self.observation(records, 'source_guard'))
        self.assertNotIn('"evidence_ref": "diagnostic.source_guard"', json.dumps(records))

    def test_deferred_file_metadata_does_not_qualify_a_candidate_or_solver(self):
        inputs = self.file_inputs()
        self.write('tasks/current/native_file_inputs.json', inputs)
        records = self.collect()
        evidence = self.observation(records, 'native_file_inputs')[0]
        self.assertEqual(evidence['qualification'], 'diagnostic_only')
        self.assertFalse(diagnostics.pipeline_qualification(inputs, 'maestro_netlist', 'spectre')['ok'])
        identity = self.observation(records, 'identity_coverage')[0]
        self.assertEqual(identity['status'], 'NOT_VERIFIED')
        self.assertIsNone(identity['unverified_count'])
        self.assertEqual(diagnostics.failure_layer({'failure_stage': 'native_file_inputs'}), 'netlist_generation')
        self.assertEqual(stage_section({'failure_stage': 'native_file_inputs'}), 'netlist')
        self.assertEqual(observation_section({'observation': 'diagnostic.artifact',
            'payload': {'path': 'tasks/current/native_file_inputs.json'}}), 'netlist')

    def test_unknown_file_schema_remains_plain_artifact_without_dangling_reference(self):
        for schema in ('unknown', ['malformed']):
            with self.subTest(schema=schema):
                inputs = dict(self.file_inputs(), schema_version=schema)
                self.write('tasks/current/native_file_inputs.json', inputs)
                records = self.collect()
                self.assertFalse(self.observation(records, 'native_file_inputs'))
                self.assertTrue(self.observation(records, 'artifact'))
                self.assertNotIn('"evidence_ref": "diagnostic.native_file_inputs"', json.dumps(records))

    def test_compact_netlist_section_keeps_file_evidence_and_source_cause_once(self):
        guard, _failure = self.source_failure()
        self.write('source_guard.json', guard)
        self.write('tasks/current/native_file_inputs.json', self.file_inputs())
        records = self.collect()
        context = {'case_id': 'spectre.maestro_netlist', 'records': records,
            'first_cause': {'failure_stage': 'source_freshness', 'error': 'source dependencies changed'}}
        text = format_section([{'id': 'execution', 'status': 'FAIL'}], context, compact=True)
        self.assertIn('spectre.maestro_netlist/netlist', text)
        self.assertEqual(text.count(hashlib.sha256(b'source after!').hexdigest()), 1)
        self.assertIn('"deferred_count": 1', text)
        self.assertIn('"line": 321', text)
        self.assertIn('expression_deferred_to_simulator', text)


if __name__ == '__main__':
    unittest.main()
