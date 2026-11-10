"""Ordinary strings reach the simulator; proven generated inputs stay frozen."""
import json
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine.native_includes import NativeIncludeError
from crescendo_engine.netlist_providers import _freeze_generated_dependencies
from crescendo_engine.shared_maestro import validate_static_file_inputs


class NativeFileInputTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='crescendo native input ')
        self.addCleanup(self.temporary.cleanup)
        self.root = self.temporary.name
        self.native = os.path.join(self.root, 'native')
        self.target = os.path.join(self.root, 'candidate')
        os.makedirs(self.native)
        os.makedirs(self.target)
        self.task = {'cwd': self.target, 'native_cwd': self.native}

    def freeze(self, text):
        return _freeze_generated_dependencies(text, self.task, {}, [self.native])

    def test_parameter_reference_and_historical_unused_string_reach_simulator_unchanged(self):
        text = ('parameters IND_5G="v14v4_55.s45p" old="unused-file-name"\n'
                'N0 (p 0) nport file=IND_5G\n')
        with mock.patch('subprocess.Popen', side_effect=AssertionError('No evaluation')):
            frozen, files = self.freeze(text)
        self.assertEqual(frozen, text)
        self.assertEqual(files, [])
        self.assertTrue(validate_static_file_inputs(self.task))
        proof = self.task['native_file_inputs']
        self.assertEqual(proof['deferred_count'], 1)
        row = proof['deferred'][0]
        self.assertEqual(row['parameter'], 'file')
        self.assertEqual(row['expression'], 'IND_5G')
        self.assertEqual(row['line'], 2)
        self.assertEqual(row['statement'], 'N0 (p 0) nport file=IND_5G')
        with open(os.path.join(self.target, 'native_file_inputs.json')) as stream:
            self.assertEqual(json.load(stream), proof)

    def test_general_dynamic_expressions_and_non_nport_file_fields_are_opaque(self):
        for expression in ('VAR("name")', 'strcat("prefix", suffix)', '"base" + suffix',
                           '"base"+suffix', 'flag ? A : B', '0', '{a + b}'):
            with self.subTest(expression=expression):
                text = 'X0 (p 0) arbitrary filename=' + expression + '\n'
                frozen, files = self.freeze(text)
                self.assertEqual(frozen, text)
                self.assertEqual(files, [])
                self.assertEqual(self.task['native_file_inputs']['deferred_count'], 1)

    def test_unresolved_literal_and_empty_output_filename_are_not_preconditions(self):
        for literal in ('"historical-file"', '""', '"$CRESCENDO_NOT_SET_X/any-data"',
                        '"/not/present/filename"'):
            with self.subTest(literal=literal):
                text = 'X0 (p 0) arbitrary datafile=' + literal + '\n'
                frozen, files = self.freeze(text)
                self.assertEqual(frozen, text)
                self.assertEqual(files, [])
                self.assertEqual(self.task['static_file_inputs'], [])
                self.assertEqual(self.task['native_file_inputs']['deferred_count'], 1)

    def test_comments_and_quoted_descriptions_do_not_create_file_inputs(self):
        text = ('// historical file=DO_NOT_PARSE\n'
                '/* file="/absent"\n include ABSENT */\n'
                '* filename=OLD_SPICE_COMMENT\n'
                'parameters description="file=old_name"\n'
                'R0 (p 0) resistor r=50 // file=unused\n')
        frozen, files = self.freeze(text)
        self.assertEqual(frozen, text)
        self.assertEqual(files, [])
        self.assertEqual(self.task['native_file_inputs']['deferred_count'], 0)

    def test_known_mutable_input_is_independent_of_next_candidate(self):
        stimulus = os.path.join(self.native, 'input with spaces.tbl')
        with open(stimulus, 'w') as stream:
            stream.write('0 1\n1 2\n')
        text = 'V0 (p 0) vsource file="input with spaces.tbl"\n'
        frozen, files = self.freeze(text)
        self.assertEqual(len(files), 1)
        self.assertIn(files[0]['path'], frozen)
        self.assertEqual(self.task['native_file_inputs']['snapshot_count'], 1)
        with open(stimulus, 'w') as stream:
            stream.write('next candidate\n')
        with open(files[0]['path']) as stream:
            self.assertEqual(stream.read(), '0 1\n1 2\n')

    def test_literal_search_order_and_existing_static_identity_still_apply(self):
        static = os.path.join(self.root, 'static')
        os.makedirs(static)
        filename = os.path.join(static, 'shared.dat')
        with open(filename, 'w') as stream:
            stream.write('static data\n')
        self.task['include_resolution'] = {'include_dirs': [static]}
        frozen, files = self.freeze('X0 (p 0) arbitrary wavefile="shared.dat"\n')
        self.assertIn(filename, frozen)
        self.assertEqual(files, [])
        self.assertTrue(validate_static_file_inputs(self.task))
        with open(filename, 'w') as stream:
            stream.write('changed data\n')
        with self.assertRaises(ValueError):
            validate_static_file_inputs(self.task)

    def test_per_corner_literal_parameter_files_are_frozen_without_changing_variables(self):
        first_task = None
        for corner, filename in (('TT', 'tt.data'), ('SS', 'ss.data')):
            target = os.path.join(self.target, corner)
            os.makedirs(target)
            self.task = {'cwd': target, 'native_cwd': self.native}
            source = os.path.join(self.native, filename)
            with open(source, 'w') as stream:
                stream.write(corner + ' data\n')
            declaration = 'parameters INPUT="%s" UNUSED="historical-missing-file"\n' % filename
            frozen, files = self.freeze(declaration + 'X0 (p 0) arbitrary file=INPUT\n')
            self.assertTrue(frozen.startswith(declaration))
            self.assertEqual(len(files), 1)
            self.assertIn(files[0]['path'], frozen)
            self.assertEqual(self.task['native_file_inputs']['resolved_parameter_count'], 1)
            self.assertEqual(self.task['native_file_inputs']['deferred_count'], 0)
            with open(source, 'w') as stream:
                stream.write('next generation overwrote ' + corner)
            with open(files[0]['path']) as stream:
                self.assertEqual(stream.read(), corner + ' data\n')
            if first_task is None:
                first_task = files[0]['path']
        with open(first_task) as stream:
            self.assertEqual(stream.read(), 'TT data\n')

    def test_local_formals_and_instance_overrides_are_not_misbound_to_global(self):
        with open(os.path.join(self.native, 'global.data'), 'w') as stream:
            stream.write('wrong data if used by local input\n')
        variants = ('subckt S (p n)\nparameters INPUT="local.data"\n',
                    'subckt S (p n)\nparameters INPUT\n',
                    'SUBCKT S (p n)\nPARAMETERS INPUT\n',
                    'subckt S (p n) INPUT\n',
                    'subckt S (p n) \\\n INPUT\n',
                    'subckt S (p n) \\  \n INPUT\n',
                    'subckt S (p n)\n+ INPUT\n',
                    'subckt S (p n)\nX1 (p n) leaf INPUT="local.data"\n')
        for local in variants:
            with self.subTest(local=local):
                text = ('parameters INPUT="global.data"\n' + local +
                        'X0 (p n) arbitrary file=INPUT\nends S\n')
                frozen, files = self.freeze(text)
                self.assertEqual(frozen, text)
                self.assertEqual(files, [])
                self.assertEqual(self.task['native_file_inputs']['resolved_parameter_count'], 0)
                self.assertEqual(self.task['native_file_inputs']['deferred_count'], 1)

    def test_unique_continued_parameter_can_still_snapshot_exact_dependency(self):
        path = os.path.join(self.native, 'continued.data')
        with open(path, 'w') as stream:
            stream.write('continued input\n')
        text = ('parameters VDD=1 \\\n+ INPUT="continued.data"\n'
                'X0 (p 0) arbitrary file=INPUT\n')
        frozen, files = self.freeze(text)
        self.assertEqual(len(files), 1)
        self.assertIn(files[0]['path'], frozen)
        self.assertEqual(self.task['native_file_inputs']['resolved_parameter_count'], 1)

    def test_parameter_names_remain_case_sensitive_and_expressions_are_not_evaluated(self):
        with open(os.path.join(self.native, 'global.data'), 'w') as stream:
            stream.write('data\n')
        for expression in ('input', 'INPUT + suffix', 'INPUT+suffix', 'INPUT[0]'):
            with self.subTest(expression=expression):
                text = 'parameters INPUT="global.data"\nX0 (p 0) arbitrary file=' + expression + '\n'
                frozen, files = self.freeze(text)
                self.assertEqual(frozen, text)
                self.assertEqual(files, [])

    def test_continued_or_commented_expression_is_not_replaced_by_its_literal_prefix(self):
        with open(os.path.join(self.native, 'global.data'), 'w') as stream:
            stream.write('wrong prefix input\n')
        for expression in ('INPUT\n+ + SUFFIX', 'INPUT // comment\n+ + SUFFIX',
                           'INPUT\n// comment\n+ + SUFFIX',
                           '"global.data" /*separator*/ + SUFFIX',
                           '"global.data"\n+ + SUFFIX', '"global.data"[0]'):
            with self.subTest(expression=expression):
                text = 'parameters INPUT="global.data"\nX0 (p 0) arbitrary file=' + expression + '\n'
                frozen, files = self.freeze(text)
                self.assertEqual(frozen, text)
                self.assertEqual(files, [])
                self.assertEqual(self.task['native_file_inputs']['deferred_count'], 1)

    def test_included_parameter_overrides_prevent_optional_global_binding_guess(self):
        with open(os.path.join(self.native, 'global.data'), 'w') as stream:
            stream.write('wrong input if override ignored\n')
        override = os.path.join(self.root, 'override.scs')
        with open(override, 'w') as stream:
            stream.write('parameters INPUT="local.data"\n')
        text = ('parameters INPUT="global.data"\ninclude "%s"\n' % override +
                'X0 (p 0) arbitrary file=INPUT\n')
        frozen, files = self.freeze(text)
        self.assertEqual(frozen, text)
        self.assertEqual(files, [])
        self.assertEqual(self.task['native_file_inputs']['resolved_parameter_count'], 0)
        self.assertEqual(self.task['native_file_inputs']['deferred_count'], 1)

    def test_declarations_inside_block_comments_cannot_supply_a_file_binding(self):
        with open(os.path.join(self.native, 'global.data'), 'w') as stream:
            stream.write('comment-only filename\n')
        text = ('/* ignored declaration\nparameters INPUT="global.data"\n*/\n'
                'X0 (p 0) arbitrary file=INPUT\n')
        # Existing capture metadata is not authority for a comment-only name.
        self.task['native_parameters'] = [{'name': 'INPUT', 'expression': '"global.data"'}]
        frozen, files = self.freeze(text)
        self.assertEqual(frozen, text)
        self.assertEqual(files, [])
        self.assertEqual(self.task['native_file_inputs']['deferred_count'], 1)

    def test_absolute_string_inside_dynamic_function_is_not_a_standalone_file(self):
        path = os.path.join(self.native, 'prefix')
        with open(path, 'w') as stream:
            stream.write('not the intended combined filename\n')
        for expression in ('strcat("%s", SUFFIX)' % path, '"%s" + SUFFIX' % path,
                           'strcat(PREFIX, "%s")' % path):
            with self.subTest(expression=expression):
                text = 'X0 (p 0) arbitrary file=' + expression + '\n'
                frozen, files = self.freeze(text)
                self.assertEqual(frozen, text)
                self.assertEqual(files, [])

    def test_uppercase_include_keyword_also_prevents_unproven_binding(self):
        path = os.path.join(self.native, 'global.data')
        with open(path, 'w') as stream:
            stream.write('wrong if include is ignored\n')
        included = os.path.join(self.root, 'override.scs')
        with open(included, 'w') as stream:
            stream.write('parameters INPUT="local.data"\n')
        text = ('parameters INPUT="global.data"\nINCLUDE "%s"\n' % included +
                'X0 (p 0) arbitrary file=INPUT\n')
        frozen, files = self.freeze(text)
        self.assertEqual(frozen, text)
        self.assertEqual(files, [])

    def test_static_file_content_is_hashed_once_for_repeated_references(self):
        static = os.path.join(self.root, 'static.data')
        with open(static, 'w') as stream:
            stream.write('static input\n')
        text = ('parameters INPUT="%s"\n' % static +
                ''.join('X%d (p 0) arbitrary file=INPUT\n' % i for i in range(20)))
        from crescendo_engine import netlist_providers
        with mock.patch.object(netlist_providers, '_digest', wraps=netlist_providers._digest) as digest:
            frozen, files = self.freeze(text)
        self.assertEqual(files, [])
        self.assertEqual(digest.call_count, 1)
        self.assertEqual(self.task['native_file_inputs']['resolved_parameter_count'], 20)
        self.assertEqual(len(self.task['static_file_inputs']), 1)
        self.assertEqual(frozen.count('file="' + static + '"'), 20)

    def test_real_missing_include_remains_a_model_failure_with_location(self):
        for text in ('include "missing-model.scs"\n', 'include MODEL_EXPRESSION\n'):
            with self.subTest(text=text):
                with self.assertRaises(NativeIncludeError) as failure:
                    self.freeze(text)
                self.assertEqual(failure.exception.detail['failure_stage'], 'model_include_resolution')
                self.assertEqual(failure.exception.detail['input_context']['line'], 1)
                self.assertEqual(failure.exception.detail['input_context']['statement'], text.rstrip())

    def test_deferred_evidence_is_bounded_without_dropping_input_text(self):
        text = ''.join('X%d (p 0) arbitrary file=unknown%d\n' % (i, i) for i in range(200))
        frozen, files = self.freeze(text)
        self.assertEqual(frozen, text)
        self.assertEqual(files, [])
        proof = self.task['native_file_inputs']
        self.assertEqual(proof['deferred_count'], 200)
        self.assertEqual(len(proof['deferred']), 32)
        self.assertEqual(proof['deferred_omitted'], 168)
        self.assertLess(len(json.dumps(proof)), 32 * 1024)

    def test_without_mutable_roots_each_test_retains_its_ordered_search_and_identity(self):
        directories = []
        for name in ('A', 'B'):
            directory = os.path.join(self.root, name)
            os.makedirs(directory)
            with open(os.path.join(directory, 'ind.s10p'), 'w') as stream:
                stream.write(name + ' native file\n')
            directories.append(directory)
        for test, search in (('A', directories), ('B', list(reversed(directories)))):
            with self.subTest(test=test):
                task = {'cwd': self.target, 'native_cwd': self.native, 'test': test,
                        'corner': 'TT_0', 'source_corner': 'TT', 'point_id': 3,
                        'task_id': 'p3_' + test, 'include_resolution': {'include_dirs': search}}
                text = 'N0 (p 0) nport file="ind.s10p"\n'
                frozen, files = _freeze_generated_dependencies(text, task, {}, [])
                expected = os.path.join(search[0], 'ind.s10p')
                self.assertEqual(frozen, 'N0 (p 0) nport file="' + expected + '"\n')
                self.assertEqual(files, [])
                proof = task['native_file_inputs']
                self.assertEqual((proof['test'], proof['corner'], proof['native_corner'], proof['point_id']),
                                 (test, 'TT', 'TT_0', 3))
                self.assertEqual(proof['resolved_input_count'], 1)
                self.assertEqual(proof['resolved_inputs'][0]['resolved_path'], expected)
                self.assertEqual(proof['resolved_inputs'][0]['execution_path'], expected)
                self.assertEqual(proof['resolved_inputs'][0]['handling'], 'shared_static_input')
                self.assertEqual(task['static_file_inputs'][0]['path'], expected)

    def test_without_mutable_roots_unknowns_and_candidate_parameter_include_remain_valid(self):
        text = ('include "cr_candidate_parameters.scs"\n'
                'X0 (p 0) arbitrary file=UNKNOWN\n'
                'X1 (p 0) arbitrary file="missing-unused-file"\n')
        frozen, files = _freeze_generated_dependencies(
            text, self.task, {}, [], allow_candidate_parameters=True)
        self.assertEqual(frozen, text)
        self.assertEqual(files, [])
        self.assertEqual(self.task['native_file_inputs']['deferred_count'], 2)
        self.assertEqual(self.task['native_file_inputs']['resolved_input_count'], 0)

    def test_resolved_input_evidence_is_bounded_and_generated_input_reports_actual_snapshot(self):
        source = os.path.join(self.native, 'input.tbl')
        with open(source, 'w') as stream:
            stream.write('0 1\n')
        text = ''.join('X%d (p 0) arbitrary file="input.tbl"\n' % i for i in range(40))
        frozen, files = self.freeze(text)
        self.assertEqual(len(files), 1)
        proof = self.task['native_file_inputs']
        self.assertEqual(proof['resolved_input_count'], 40)
        self.assertEqual(len(proof['resolved_inputs']), 32)
        self.assertEqual(proof['resolved_inputs_omitted'], 8)
        self.assertEqual(proof['resolved_inputs'][0]['resolved_path'], source)
        self.assertEqual(proof['resolved_inputs'][0]['execution_path'], files[0]['path'])
        self.assertEqual(proof['resolved_inputs'][0]['handling'], 'generated_input_snapshot')
        self.assertEqual(frozen.count(files[0]['path']), 40)


if __name__ == '__main__':
    unittest.main()
