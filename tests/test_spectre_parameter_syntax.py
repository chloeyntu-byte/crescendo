"""Native header boundaries, including a real solver-accepted bare declaration."""
import unittest

from crescendo_engine.spectre_parameter_syntax import parse_parameter_declarations as parse


class SpectreParameterSyntaxTests(unittest.TestCase):
    def test_actual_maestro_unassigned_declaration_does_not_swallow_neighbor(self):
        text = 'parameters DPAR_1=220n L_G=180n MISSING_LENGTH W_L=1u FN_gm=4'
        self.assertEqual(parse(text), [('DPAR_1', '220n'), ('L_G', '180n'), ('MISSING_LENGTH', ''), ('W_L', '1u'), ('FN_gm', '4')])

    def test_bare_parameters_before_after_and_between_assignments(self):
        self.assertEqual(parse('parameters FIRST x=1 SECOND THIRD y=2 LAST'),
                         [('FIRST', ''), ('x', '1'), ('SECOND', ''), ('THIRD', ''), ('y', '2'), ('LAST', '')])

    def test_assigned_reference_and_unassigned_reference_are_distinct(self):
        self.assertEqual(parse('parameters LENGTH=UNKNOWN UNKNOWN'), [('LENGTH', 'UNKNOWN'), ('UNKNOWN', '')])

    def test_spaced_arithmetic_expression_is_preserved(self):
        self.assertEqual(parse('parameters a=2 * L_G + 1u MISSING b=3'), [('a', '2 * L_G + 1u'), ('MISSING', ''), ('b', '3')])

    def test_no_space_arithmetic_expression_is_preserved(self):
        self.assertEqual(parse('parameters a=2*L_G+1u MISSING b=3'), [('a', '2*L_G+1u'), ('MISSING', ''), ('b', '3')])

    def test_unary_sign_after_binary_operator(self):
        self.assertEqual(parse('parameters a=-2u + +3u MISSING b=+4u'), [('a', '-2u + +3u'), ('MISSING', ''), ('b', '+4u')])

    def test_scientific_and_case_sensitive_suffixes(self):
        self.assertEqual(parse('parameters a=1.8e-7 b=4M c=4m d=.5u EMPTY'),
                         [('a', '1.8e-7'), ('b', '4M'), ('c', '4m'), ('d', '.5u'), ('EMPTY', '')])

    def test_function_arguments_and_nested_functions(self):
        self.assertEqual(parse('parameters a=max(1u, sqrt(4 * 1u)) EMPTY b=2'),
                         [('a', 'max(1u, sqrt(4 * 1u))'), ('EMPTY', ''), ('b', '2')])

    def test_whitespace_between_function_name_and_call(self):
        self.assertEqual(parse('parameters a=max (1u, 2u) EMPTY b=2'), [('a', 'max (1u, 2u)'), ('EMPTY', ''), ('b', '2')])

    def test_parenthesized_expression_with_spaces(self):
        self.assertEqual(parse('parameters a=(BASE + 2u) / (1 + FACTOR) EMPTY'), [('a', '(BASE + 2u) / (1 + FACTOR)'), ('EMPTY', '')])

    def test_braced_expression_remains_one_unknown_native_expression(self):
        self.assertEqual(parse('parameters a={BASE + 2u * func(x, y)} EMPTY b=3'),
                         [('a', '{BASE + 2u * func(x, y)}'), ('EMPTY', ''), ('b', '3')])

    def test_vector_remains_one_native_expression(self):
        self.assertEqual(parse('parameters a=[1u 2u (BASE + 3u)] EMPTY b=3'),
                         [('a', '[1u 2u (BASE + 3u)]'), ('EMPTY', ''), ('b', '3')])

    def test_double_quoted_string_with_spaces_and_assignment(self):
        self.assertEqual(parse('parameters a="not a declaration b=7" EMPTY c=3'),
                         [('a', '"not a declaration b=7"'), ('EMPTY', ''), ('c', '3')])

    def test_single_quoted_expression_with_spaces(self):
        self.assertEqual(parse("parameters a='BASE + 2u' EMPTY c=3"), [('a', "'BASE + 2u'"), ('EMPTY', ''), ('c', '3')])

    def test_escaped_quote_does_not_split_string(self):
        text = r'parameters a="quoted \" token" EMPTY b=3'
        self.assertEqual(parse(text)[-2:], [('EMPTY', ''), ('b', '3')])

    def test_conditional_comparison_and_boolean_operators(self):
        self.assertEqual(parse('parameters a=FLAG > 0 && ENABLE ? BASE : 2u EMPTY b=3'),
                         [('a', 'FLAG > 0 && ENABLE ? BASE : 2u'), ('EMPTY', ''), ('b', '3')])

    def test_bitwise_shift_and_power(self):
        self.assertEqual(parse('parameters a=(1 << 2) | 3 ^ 2 EMPTY b=4**2'),
                         [('a', '(1 << 2) | 3 ^ 2'), ('EMPTY', ''), ('b', '4**2')])

    def test_empty_assignment_does_not_swallow_next_key(self):
        self.assertEqual(parse('parameters EMPTY= a=1'), [('EMPTY', ''), ('a', '1')])

    def test_inline_comment_is_not_a_declaration(self):
        self.assertEqual(parse('parameters a=1 EMPTY // ignored=3 commentary'), [('a', '1'), ('EMPTY', '')])

    def test_slashes_inside_quotes_are_not_comment(self):
        self.assertEqual(parse('parameters p="/path//name" EMPTY'), [('p', '"/path//name"'), ('EMPTY', '')])

    def test_duplicate_declarations_remain_in_input_order(self):
        self.assertEqual(parse('parameters a=1 a=2 EMPTY'), [('a', '1'), ('a', '2'), ('EMPTY', '')])

    def test_unterminated_native_group_remains_unresolved_text(self):
        self.assertEqual(parse('parameters a=(1 + UNKNOWN'), [('a', '(1 + UNKNOWN')])

    def test_unterminated_quote_remains_unresolved_text(self):
        self.assertEqual(parse('parameters a="unfinished UNKNOWN'), [('a', '"unfinished UNKNOWN')])

    def test_non_header_and_non_string_inputs_are_ignored(self):
        for value in ('M1 (a b c d) nmos l=1u', 'notparameters a=1', None, []):
            self.assertEqual(parse(value), [])

    def test_empty_or_bare_header(self):
        self.assertEqual(parse('parameters'), [])
        self.assertEqual(parse(' parameters EMPTY '), [('EMPTY', '')])

    def test_declaration_names_are_case_sensitive(self):
        self.assertEqual(parse('parameters A=1 a=2 Missing missing'), [('A', '1'), ('a', '2'), ('Missing', ''), ('missing', '')])


class SpectreHeaderSpanTests(unittest.TestCase):
    def test_bare_name_has_its_own_span(self):
        from crescendo_engine.spectre_parameter_syntax import parameter_declaration_spans
        text = 'parameters DPAR_1=220n MISSING_LENGTH W_L=(1u + 2u)'
        rows = parameter_declaration_spans(text)
        self.assertEqual([text[start:end] for _name, _value, start, end in rows],
                         ['DPAR_1=220n', 'MISSING_LENGTH', 'W_L=(1u + 2u)'])

    def test_dpar_on_backslash_continuation_uses_original_offsets(self):
        from crescendo_engine.spectre_parameter_syntax import parameter_declaration_spans
        text = 'parameters L_G=180n ' + '\\' + '\n    DPAR_1=220n MISSING_LENGTH W_L=1u\n'
        rows = parameter_declaration_spans(text)
        self.assertEqual([(name, value) for name, value, _a, _b in rows],
                         [('L_G', '180n'), ('DPAR_1', '220n'), ('MISSING_LENGTH', ''), ('W_L', '1u')])
        self.assertEqual(next(text[start:end] for name, _value, start, end in rows if name == 'DPAR_1'), 'DPAR_1=220n')

    def test_dpar_on_plus_continuation_uses_original_offsets(self):
        from crescendo_engine.spectre_parameter_syntax import parameter_declaration_spans
        text = 'parameters L_G=180n\n+ DPAR_1=220n MISSING_LENGTH\n'
        rows = parameter_declaration_spans(text)
        self.assertEqual([(name, value) for name, value, _a, _b in rows], [('L_G', '180n'), ('DPAR_1', '220n'), ('MISSING_LENGTH', '')])

    def test_multiword_expression_continuation_span_preserves_all_text(self):
        from crescendo_engine.spectre_parameter_syntax import parameter_declaration_spans
        expression = 'BASE + ' + '\\' + '\n    2u * FACTOR'
        text = 'parameters DPAR_1=' + expression + ' MISSING_LENGTH W_L=1u'
        rows = parameter_declaration_spans(text)
        self.assertEqual(rows[0][1], expression)
        self.assertEqual(text[rows[0][2]:rows[0][3]], 'DPAR_1=' + expression)
        self.assertEqual(rows[1][:2], ('MISSING_LENGTH', ''))

    def test_header_blocks_do_not_capture_following_device_or_comments(self):
        from crescendo_engine.spectre_parameter_syntax import parameter_statement_spans
        text = '// keep header evidence\nparameters A=1 ' + '\\' + '\n B=2 UNKNOWN\nR1 (a 0) resistor r=A\n// next\nparameters C=3\n+ OTHER\nV1 (a 0) vsource dc=C\n'
        blocks = parameter_statement_spans(text)
        self.assertEqual(len(blocks), 2)
        self.assertEqual(parse(blocks[0][2]), [('A', '1'), ('B', '2'), ('UNKNOWN', '')])
        self.assertEqual(parse(blocks[1][2]), [('C', '3'), ('OTHER', '')])
        self.assertTrue(all(text[start:end] == block for start, end, block in blocks))
        self.assertNotIn('R1', blocks[0][2])
        self.assertNotIn('// next', blocks[0][2])

    def test_crlf_continuation_preserves_original_span(self):
        from crescendo_engine.spectre_parameter_syntax import parameter_declaration_spans
        text = 'parameters A=1 ' + '\\' + '\r\n B=2 UNKNOWN\r\n'
        self.assertEqual(parse(text), [('A', '1'), ('B', '2'), ('UNKNOWN', '')])
        rows = parameter_declaration_spans(text)
        self.assertEqual(text[rows[1][2]:rows[1][3]], 'B=2')


class SpectreHeaderIntegrationTests(unittest.TestCase):
    def test_unassigned_header_value_stays_none_and_neighbor_resolves(self):
        from crescendo_engine.private_netlist import top_level_native_parameters
        rows = {row['name']: row for row in top_level_native_parameters(
            'simulator lang=spectre\nparameters DPAR_1=220n L_G=180n MISSING_LENGTH W_L=1u FN_gm=4\n')}
        self.assertAlmostEqual(rows['L_G']['value'], 180e-9)
        self.assertEqual(rows['MISSING_LENGTH']['expression'], '')
        self.assertIsNone(rows['MISSING_LENGTH']['value'])
        self.assertAlmostEqual(rows['DPAR_1']['value'], 220e-9)

    def test_expression_using_unknown_remains_unknown_not_zero(self):
        from crescendo_engine.private_netlist import top_level_native_parameters
        rows = {row['name']: row for row in top_level_native_parameters(
            'simulator lang=spectre\nparameters UNKNOWN L=(UNKNOWN * 2) SAFE=1u\n')}
        self.assertIsNone(rows['UNKNOWN']['value'])
        self.assertIsNone(rows['L']['value'])
        self.assertAlmostEqual(rows['SAFE']['value'], 1e-6)

    def test_subcircuit_unassigned_parameters_do_not_leak(self):
        from crescendo_engine.private_netlist import top_level_native_parameters
        text = 'simulator lang=spectre\nparameters GLOBAL=1 UNUSED\nsubckt inner A B\nparameters ONLY_LOCAL L=2\nends inner\n'
        rows = {row['name']: row for row in top_level_native_parameters(text)}
        self.assertEqual(set(rows), {'GLOBAL', 'UNUSED'})

    def test_symbol_removal_preserves_bare_name_and_expression_verbatim(self):
        from crescendo_engine.private_netlist import _without_symbol_declarations
        text = 'simulator lang=spectre\nparameters CRESCENDO_NL_p=220n EMPTY W_L=(1u + 2u) KEEP\n'
        result = _without_symbol_declarations(text, {'p': 'CRESCENDO_NL_p'})
        self.assertIn('parameters EMPTY W_L=(1u + 2u) KEEP', result)
        self.assertNotIn('EMPTY=', result)
        self.assertNotIn('KEEP=', result)
        self.assertNotIn('CRESCENDO_NL_p=', result)

    def _symbolize(self, header):
        import os
        import tempfile
        from crescendo_engine.private_netlist import _symbolize_native_dpar_phase
        directory = tempfile.TemporaryDirectory(prefix='crescendo-header-span-')
        self.addCleanup(directory.cleanup)
        original = os.path.join(directory.name, 'input.scs')
        text = 'simulator lang=spectre\n' + header + '\n// Library name: L\n// Cell name: C\n// View name: schematic_custom\nRSEL (a 0) resistor r=DPAR_1\n'
        with open(original, 'w') as stream:
            stream.write(text)
        task = {'task_id': 'p1', 'point_id': 1, 'test': 'T', 'netlist_path': original,
                'template_files': [{'path': original, 'relative_path': 'input.scs'}]}
        numeric = {'tasks': [task], 'parameter_application': {'parameters': [{'parameter_id': 'p', 'points': [
            {'point_id': 1, 'test': 'T', 'dpar_values': {'DPAR_1': 220e-9}}]}]}}
        phase = _symbolize_native_dpar_phase(numeric, {'p': 'CRESCENDO_NL_p'}, os.path.join(directory.name, 'symbolic'))
        with open(phase['tasks'][0]['netlist_path']) as stream:
            return stream.read()

    def test_dpar_symbol_replacement_keeps_immediately_following_bare(self):
        result = self._symbolize('parameters DPAR_1=220n MISSING_LENGTH W_L=1u')
        self.assertIn('DPAR_1=CRESCENDO_NL_p', result)
        self.assertIn('MISSING_LENGTH', result)
        self.assertNotIn('MISSING_LENGTH=', result)
        self.assertIn('W_L=1u', result)
        self.assertIn('// View name: schematic_custom', result)

    def test_dpar_symbol_replacement_preserves_next_spaced_expression(self):
        result = self._symbolize('parameters DPAR_1=220n EMPTY W_L=(BASE + 2u)')
        self.assertIn('EMPTY W_L=(BASE + 2u)', result)

    def test_dpar_symbol_replacement_on_backslash_continuation(self):
        header = 'parameters L_G=180n ' + '\\' + '\n    DPAR_1=220n EMPTY W_L=1u'
        result = self._symbolize(header)
        self.assertIn('DPAR_1=CRESCENDO_NL_p', result)
        self.assertIn('EMPTY W_L=1u', result)
        self.assertIn('L_G=180n', result)
        self.assertIn('// Library name: L', result)

    def test_dpar_symbol_replacement_on_plus_continuation(self):
        result = self._symbolize('parameters L_G=180n\n+ DPAR_1=220n EMPTY W_L=1u')
        self.assertIn('DPAR_1=CRESCENDO_NL_p', result)
        self.assertIn('EMPTY W_L=1u', result)

    def test_symbolic_mapping_keeps_unassigned_native_names(self):
        from crescendo_engine.private_netlist import qualify_symbolic_netlist, PrivateNetlistUnsupported
        def text(value):
            return 'simulator lang=spectre\nparameters DPAR_1=' + value + ' MISSING_LENGTH L_G=180n\n// Library name: L\n// Cell name: C\n// View name: schematic\nRSEL (a 0) resistor r=DPAR_1\n'
        symbolic = text('CRESCENDO_NL_p')
        references = [{'text': text('220n'), 'values': {'p': 220e-9}}]
        self.assertTrue(qualify_symbolic_netlist(symbolic, references, {'p': 'CRESCENDO_NL_p'})['ok'])
        references[0]['text'] = references[0]['text'].replace('MISSING_LENGTH ', '')
        with self.assertRaises(PrivateNetlistUnsupported):
            qualify_symbolic_netlist(symbolic, references, {'p': 'CRESCENDO_NL_p'})


if __name__ == '__main__':
    unittest.main()
