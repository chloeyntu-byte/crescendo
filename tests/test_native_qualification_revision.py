"""Qualification cache compatibility; no native process or solver is launched."""
import copy
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine.native_maestro_parameters import GENERATION_PROTOCOL
from crescendo_engine.native_qualification import (
    NATIVE_QUALIFICATION_REVISION, is_native_qualification,
    require_current_native_qualification,
)
from crescendo_engine.netlist_execution import (
    NetlistExecutionError, compact_netlist_generation, validate_candidate_manifest,
)
from crescendo_engine.netlist_providers import prepare_maestro_netlists
from crescendo_engine.runner import validate_checkpoint
from crescendo_engine import runner
from tests import test_netlist_providers as provider_fixture
from tests import test_netlist_execution as execution_fixture
from tests import test_runner as checkpoint_fixture


class NativeQualificationRevisionTests(unittest.TestCase):
    def test_lexical_alias_rules_require_v5_authority(self):
        self.assertEqual(NATIVE_QUALIFICATION_REVISION, 'crescendo.native_point_qualification.v5')

    def manifest(self):
        return {'netlist_generation': {'generation_protocol': GENERATION_PROTOCOL,
                    'qualification_revision': NATIVE_QUALIFICATION_REVISION},
                'qualification': {'ok': True,
                    'scope': 'numeric_native_CDF_and_generated_netlist_readback',
                    'revision': NATIVE_QUALIFICATION_REVISION}}

    def test_current_native_proofs_and_non_native_legacy_remain_usable(self):
        native = self.manifest()
        self.assertTrue(is_native_qualification(native))
        require_current_native_qualification(native)
        for legacy in ({}, {'qualification': {'ok': True}},
                       {'netlist_generation': {'method': 'qualified_symbolic_template',
                                               'generation_protocol': None}},
                       {'qualification': {'scope': []}}):
            self.assertFalse(is_native_qualification(legacy))
            require_current_native_qualification(legacy)

    def test_each_native_evidence_requires_its_own_current_revision(self):
        for section, key in (('netlist_generation', 'qualification_revision'),
                             ('qualification', 'revision')):
            for revision in (None, 'crescendo.native_point_qualification.v1',
                             'crescendo.native_point_qualification.v4', ''):
                manifest = self.manifest()
                manifest[section][key] = revision
                with self.subTest(section=section, revision=revision):
                    with self.assertRaisesRegex(ValueError, 'older point-qualification'):
                        require_current_native_qualification(manifest)

    def test_pure_native_qualifier_needs_no_fabricated_generation(self):
        qualifier = {'qualification': {'ok': True,
            'scope': 'native_expression_mapping_and_recorded_reference_points',
            'revision': NATIVE_QUALIFICATION_REVISION}}
        require_current_native_qualification(qualifier)
        del qualifier['qualification']['revision']
        with self.assertRaisesRegex(ValueError, 'older point-qualification'):
            require_current_native_qualification(qualifier)

    def test_generation_only_evidence_checks_generation_revision(self):
        manifest = {'netlist_generation': self.manifest()['netlist_generation']}
        require_current_native_qualification(manifest)
        del manifest['netlist_generation']['qualification_revision']
        with self.assertRaisesRegex(ValueError, 'older point-qualification'):
            require_current_native_qualification(manifest)

    def test_checkpoint_rejects_old_completed_failed_and_penalty_history(self):
        for backend in ('maestro_netlist', 'private_netlist'):
            for status in ('completed', 'failed', 'infrastructure_failed'):
                for revision in (None, 'crescendo.native_point_qualification.v4'):
                    checkpoint = {'spec': {'evaluator': 'real', 'execution_backend': backend,
                        'netlist_generation_protocol': GENERATION_PROTOCOL,
                        'native_qualification_revision': revision}}
                    with self.subTest(backend=backend, status=status, revision=revision), mock.patch(
                            'crescendo_engine.runner.validate_checkpoint_integrity',
                            return_value=[{'status': status, 'metrics': {'merit': 100}}]):
                        with self.assertRaisesRegex(ValueError, 'older point-qualification'):
                            validate_checkpoint(checkpoint, {})

    def test_current_checkpoint_continues_to_source_checks_and_mock_stays_compatible(self):
        checkpoint = {'spec': {'evaluator': 'real', 'execution_backend': 'maestro_netlist',
            'netlist_generation_protocol': GENERATION_PROTOCOL,
            'native_qualification_revision': NATIVE_QUALIFICATION_REVISION}}
        with mock.patch('crescendo_engine.runner.validate_checkpoint_integrity', return_value=[]):
            with self.assertRaisesRegex(ValueError, 'source design identity'):
                validate_checkpoint(checkpoint, {})
            self.assertEqual(validate_checkpoint({'spec': {'evaluator': 'mock'}}, {}), [])

    def test_checkpoint_revision_is_serialized_and_integrity_bound(self):
        spec = {'variables': [], 'native_qualification_revision': NATIVE_QUALIFICATION_REVISION}
        with mock.patch('crescendo_engine.runner._generator_state', return_value={}):
            document = runner._checkpoint_document(spec, {}, None, [], [], 0, None, {})
        self.assertEqual(document['spec']['native_qualification_revision'], NATIVE_QUALIFICATION_REVISION)
        self.assertEqual(document['manifest']['native_qualification_revision'], NATIVE_QUALIFICATION_REVISION)
        self.assertEqual(document['payload_sha256'], runner._checkpoint_checksum(document))
        checkpoint = checkpoint_fixture._minimal_checkpoint()
        checkpoint['spec']['native_qualification_revision'] = NATIVE_QUALIFICATION_REVISION
        checkpoint['manifest'] = {'native_qualification_revision': NATIVE_QUALIFICATION_REVISION}
        runner.validate_checkpoint_integrity(checkpoint)
        checkpoint['manifest']['native_qualification_revision'] = 'crescendo.native_point_qualification.v1'
        with self.assertRaisesRegex(ValueError, 'native_qualification_revision identities disagree'):
            runner.validate_checkpoint_integrity(checkpoint)

    def test_v4_checkpoint_results_read_and_portable_export_without_resume_authority(self):
        from crescendo_engine.exports import candidates_csv_text
        from crescendo_engine.utils import atomic_write_json, load_json
        for backend in ('maestro_netlist', 'private_netlist'):
            checkpoint = checkpoint_fixture._minimal_checkpoint()
            checkpoint['spec'].update(evaluator='real', execution_backend=backend,
                netlist_generation_protocol=GENERATION_PROTOCOL,
                native_qualification_revision='crescendo.native_point_qualification.v4')
            checkpoint['manifest'] = {key: checkpoint['spec'][key] for key in
                ('netlist_generation_protocol', 'native_qualification_revision')}
            checkpoint['spec']['objectives'] = [{'output_key': 'T::gain'}]
            checkpoint['candidates'][0][5] = runner._pack_metrics({'objectives': [{'output_key': 'T::gain', 'value': 2.5}],
                                            'constraints': [], 'feasible': True})
            checkpoint['payload_sha256'] = runner._checkpoint_checksum(checkpoint)
            with self.subTest(backend=backend), tempfile.TemporaryDirectory() as root:
                path = os.path.join(root, 'old.json')
                atomic_write_json(path, checkpoint)
                loaded = load_json(path)
                rows = runner.checkpoint_candidates(loaded)
                self.assertEqual(rows[0]['metrics']['objectives'][0]['value'], 2.5)
                csv = candidates_csv_text(rows)
                self.assertIn('g000_c0000', csv)
                self.assertIn('2.5', csv)
                portable = runner.self_contained_checkpoint(loaded)
                atomic_write_json(os.path.join(root, 'portable.json'), portable)
                reloaded = load_json(os.path.join(root, 'portable.json'))
                self.assertEqual(runner.checkpoint_candidates(reloaded), rows)
                self.assertEqual(reloaded['payload_sha256'], loaded['payload_sha256'])
                with self.assertRaisesRegex(ValueError, 'older point-qualification'):
                    runner.validate_checkpoint(reloaded, {})

    def test_candidate_execution_rejects_old_native_proof_before_launch(self):
        fixture = execution_fixture.NetlistExecutionTests('test_local_owned_wrapper_completion_and_scalar_identity')
        fixture.setUp()
        try:
            manifest = copy.deepcopy(fixture.manifest)
            manifest.update(self.manifest())
            self.assertEqual(validate_candidate_manifest(manifest), fixture.root)
            del manifest['netlist_generation']['qualification_revision']
            with self.assertRaisesRegex(NetlistExecutionError, 'older point-qualification'):
                validate_candidate_manifest(manifest)
            self.assertFalse(os.path.exists(fixture.task['spectre_log']))
            self.assertEqual(validate_candidate_manifest(fixture.manifest), fixture.root)
        finally:
            fixture.tearDown()

    def test_point_summary_preserves_qualification_identity(self):
        generation = self.manifest()['netlist_generation']
        summary = compact_netlist_generation(generation, {})
        self.assertEqual(summary['generation_protocol'], GENERATION_PROTOCOL)
        self.assertEqual(summary['qualification_revision'], NATIVE_QUALIFICATION_REVISION)

    def test_real_proof_function_and_both_provider_paths_stamp_current_revision(self):
        fixture = provider_fixture.NumericMaestroProviderTest(
            'test_numeric_capture_is_once_and_frozen_for_common_executor')
        with tempfile.TemporaryDirectory() as root:
            prepared, parameters, config, spec, model = fixture.fixtures(root)
            with mock.patch('crescendo_engine.netlist_providers.build_workspace', side_effect=fixture.workspace), \
                    mock.patch('crescendo_engine.netlist_providers._capture_phase',
                               side_effect=fixture.native_capture(model)), \
                    mock.patch('crescendo_engine.netlist_providers.verify_sources_unchanged', return_value={'ok': True}):
                for generation_only in (False, True):
                    manifest = prepare_maestro_netlists(prepared, parameters, {'p': 2e-6}, config,
                        os.path.join(root, 'generation_' + str(generation_only)), 'c1',
                        spec=spec, generation_only=generation_only)
                    self.assertEqual(manifest['parameter_application']['qualification_revision'],
                                     NATIVE_QUALIFICATION_REVISION)
                    self.assertEqual(manifest['netlist_generation']['qualification_revision'],
                                     NATIVE_QUALIFICATION_REVISION)
                    if not generation_only:
                        self.assertEqual(manifest['qualification']['revision'], NATIVE_QUALIFICATION_REVISION)
                    require_current_native_qualification(manifest)


if __name__ == '__main__':
    unittest.main()
