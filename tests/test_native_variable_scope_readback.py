import os
import tempfile
import unittest

from crescendo_engine.native_variable_scope_readback import (
    native_variable_scope_skill_lines, parse_native_variable_scope,
)
from crescendo_engine.native_identity import effective_variables


class NativeVariableScopeReadbackTests(unittest.TestCase):
    def setup(self):
        return {'global_variables': [{'name': 'FN_gm', 'value': '4'}],
                'tests': [{'name': 'A', 'variables': [{'name': 'FN_gm', 'value': '2'}]},
                          {'name': 'B', 'variables': [{'name': 'FN_gm', 'value': '3'}]},
                          {'name': 'Disabled', 'enabled': False}]}

    def parse(self, text, setup=None):
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, 'scope.tsv')
            with open(path, 'w') as stream:
                stream.write(text)
            return parse_native_variable_scope(path, setup or self.setup())

    def valid(self):
        return ('TOTAL\tt\nVALUE\t"FN_gm"\t"4"\nGLOBAL\t"FN_gm"\tt\n'
                'TEST\t"FN_gm"\t"A"\tnil\nTEST\t"FN_gm"\t"B"\tt\nDONE\n')

    def test_native_per_test_false_is_not_a_query_error_or_global_override(self):
        setup = self.setup()
        setup['native_variable_scope'] = self.parse(self.valid())
        self.assertEqual(effective_variables(setup, setup['tests'][0])['FN_gm']['value'], '2')
        self.assertEqual(effective_variables(setup, setup['tests'][1])['FN_gm']['value'], '4')
        self.assertFalse(setup['native_variable_scope']['effective_point_values_proven'])

    def test_native_total_off_preserves_locals_even_with_enabled_global_rows(self):
        setup = self.setup()
        setup['native_variable_scope'] = self.parse(self.valid().replace('TOTAL\tt', 'TOTAL\tnil'))
        self.assertEqual(effective_variables(setup, setup['tests'][1])['FN_gm']['value'], '3')

    def test_missing_test_query_done_or_value_never_becomes_false(self):
        for omitted in ('TEST\t"FN_gm"\t"A"\tnil\n', 'DONE\n', 'VALUE\t"FN_gm"\t"4"\n'):
            with self.subTest(omitted=omitted), self.assertRaises(ValueError):
                self.parse(self.valid().replace(omitted, ''))

    def test_extra_duplicate_unknown_flags_are_rejected(self):
        variants = [self.valid() + 'DONE\n',
                    self.valid().replace('DONE', 'TOTAL\tt\nDONE'),
                    self.valid().replace('TOTAL\tt', 'TOTAL\t1'),
                    self.valid().replace('"B"\tt', '"other"\tt')]
        for text in variants:
            with self.subTest(text=text), self.assertRaises(ValueError):
                self.parse(text)

    def test_empty_global_table_still_needs_actual_total_flag_and_done(self):
        result = self.parse('TOTAL\tnil\nDONE\n', {'tests': [{'name': 'T'}]})
        self.assertEqual(result['global_variables'], [])
        self.assertFalse(result['global_variables_enabled'])

    def test_script_is_one_read_only_native_form_and_keeps_flag_query_success_separate(self):
        from crescendo_engine.workspace import bounded_skill_body_lines
        lines = native_variable_scope_skill_lines(self.setup(), '/tmp/scope.tsv')
        _rendered, generation = bounded_skill_body_lines(lines)
        self.assertEqual(generation['expression_count'], 1)
        script = '\n'.join(lines)
        self.assertNotIn('maeSetVar', script)
        self.assertNotIn('axlSetEnabled', script)
        self.assertNotIn('evalstring', script)
        self.assertIn('unless(crescScopeResult error(', script)


if __name__ == '__main__':
    unittest.main()
