from __future__ import print_function

import unittest

from crescendo_engine.private_netlist import PrivateNetlistUnsupported, qualify_symbolic_netlist


class CapturedDparRegression(unittest.TestCase):
    # EnvTest, retained settings variants: numeric,
    # reference_low, reference_high and symbolic. Only the selected DPAR_1/2
    # are replaced by symbols; DPAR_10 is still the seed constant.
    def netlist(self, dpar, a, b):
        return ('simulator lang=spectre\n'
                'parameters DPAR_10=%s DPAR_2=%s DPAR_1=%s\n'
                '// Library name: L\n// Cell name: C\n// View name: schematic\n'
                'M0 (d g s b) mos nf=DPAR_1 m=DPAR_2 r=DPAR_10\n' % (dpar, a, b))

    def test_captured_constant_dpar_10_must_not_pass(self):
        symbolic = self.netlist('16.1858m', 'CRESCENDO_NL_a', 'CRESCENDO_NL_b')
        references = [
            {'name': 'numeric', 'values': {'a': 7, 'b': 7}, 'text': self.netlist('16.1858m', '7', '7')},
            {'name': 'reference_low', 'values': {'a': 4, 'b': 4}, 'text': self.netlist('55.2885m', '4', '4')},
            {'name': 'reference_high', 'values': {'a': 10, 'b': 10}, 'text': self.netlist('7.61589m', '10', '10')},
        ]
        with self.assertRaisesRegex(PrivateNetlistUnsupported, 'parameters DPAR_10') as raised:
            qualify_symbolic_netlist(symbolic, references, {'a': 'CRESCENDO_NL_a', 'b': 'CRESCENDO_NL_b'})
        self.assertTrue(raised.exception.detail['native_generation_required'])
        self.assertFalse(raised.exception.detail['template_qualified'])
        self.assertEqual([row['native_value'] for row in raised.exception.detail['numeric_derived_parameters']],
                         ['55.2885m', '7.61589m'])

    def test_numeric_companion_does_not_mask_later_topology_change(self):
        symbolic = self.netlist('16.1858m', 'CRESCENDO_NL_a', 'CRESCENDO_NL_b')
        references = [{'name': 'low', 'values': {'a': 4, 'b': 4},
                       'text': self.netlist('55.2885m', '4', '4').replace('mos nf', 'other_model nf')}]
        with self.assertRaisesRegex(PrivateNetlistUnsupported, 'model/topology') as raised:
            qualify_symbolic_netlist(symbolic, references, {'a': 'CRESCENDO_NL_a', 'b': 'CRESCENDO_NL_b'})
        self.assertFalse(getattr(raised.exception, 'detail', {}).get('native_generation_required', False))

    def test_wrong_selected_parameter_never_gets_numeric_companion_fallback(self):
        symbolic = self.netlist('16.1858m', 'CRESCENDO_NL_a', 'CRESCENDO_NL_b')
        references = [{'name': 'low', 'values': {'a': 4, 'b': 4},
                       'text': self.netlist('55.2885m', '5', '4')}]
        with self.assertRaises(PrivateNetlistUnsupported) as raised:
            qualify_symbolic_netlist(symbolic, references, {'a': 'CRESCENDO_NL_a', 'b': 'CRESCENDO_NL_b'})
        self.assertFalse(getattr(raised.exception, 'detail', {}).get('native_generation_required', False))

    def test_explicit_synthetic_expression_is_still_qualified(self):
        symbolic = self.netlist('(CRESCENDO_NL_a * 2m)', 'CRESCENDO_NL_a', 'CRESCENDO_NL_b')
        references = [
            {'name': 'numeric', 'values': {'a': 7, 'b': 7}, 'text': self.netlist('14m', '7', '7')},
            {'name': 'low', 'values': {'a': 4, 'b': 4}, 'text': self.netlist('8m', '4', '4')},
            {'name': 'high', 'values': {'a': 10, 'b': 10}, 'text': self.netlist('20m', '10', '10')},
        ]
        self.assertTrue(qualify_symbolic_netlist(symbolic, references,
                                               {'a': 'CRESCENDO_NL_a', 'b': 'CRESCENDO_NL_b'})['ok'])


if __name__ == '__main__':
    unittest.main()
