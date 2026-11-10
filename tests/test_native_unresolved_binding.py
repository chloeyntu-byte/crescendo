"""Reduced v3 DTN_W capture regression, with native RDB proof generated locally.

The DPAR headers and R0 statement are from EnvTest.
This is a reduced fixture, not the complete remote netlist or a remote run.
"""
import copy
import os
import sqlite3
import tempfile
import unittest

from crescendo_engine.native_maestro_parameters import verify_native_parameter_application
from crescendo_engine.private_netlist import (
    PrivateNetlistUnsupported, _native_unresolved_binding_fallback,
    _symbolize_native_dpar_phase,
)


WIDTH = 'd84d8f4cd052838cba5e'
SELECTED = [(WIDTH, 'R0', 'DTN_W'), ('7933b7f80fcf7536f54b', 'R0', 'segW'),
            ('d92d73e3f749b8a8add3', 'R1', 'DTN_W'), ('c385813836ba03624616', 'R1', 'segW'),
            ('68483239b31b587ac53a', 'M5', 'nf'), ('5d446bbec30ddcd02634', 'M5', 'nfin')]
SYMBOLS = dict((key, 'CRESCENDO_NL_' + key) for key, _instance, _property in SELECTED)
HEADERS = {
    'numeric': 'DPAR_14=16.1858m DPAR_13=16.1858m DPAR_12=3.984u DPAR_11=3.984u '
               'DPAR_10=149.408f DPAR_9=149.408f DPAR_8=7 DPAR_7=4.06e-07 DPAR_6=300n '
               'DPAR_5=300n DPAR_4=300n DPAR_3=300n DPAR_2=7 DPAR_1=7 CT_code=0',
    'reference_low': 'DPAR_14=55.2885m DPAR_13=55.2885m DPAR_12=1.2u DPAR_11=1.8u '
                     'DPAR_10=38.272f DPAR_9=57.408f DPAR_8=4 DPAR_7=2.08e-07 DPAR_6=0.2u '
                     'DPAR_5=198n DPAR_4=0.2u DPAR_3=198n DPAR_2=4 DPAR_1=4 CT_code=0',
    'reference_high': 'DPAR_14=7.61589m DPAR_13=7.61589m DPAR_12=6.96u DPAR_11=8.352u '
                      'DPAR_10=277.84f DPAR_9=333.408f DPAR_8=10 DPAR_7=6.04e-07 DPAR_6=500n '
                      'DPAR_5=500n DPAR_4=500n DPAR_3=500n DPAR_2=10 DPAR_1=10 CT_code=0',
}


def captured_binding_references(root):
    """Build native proof through the production SQLite reader; no mocked proof."""
    references = []
    # R0 has two selected CDF fields. Neither is named w in generated SCS;
    # equal numeric widths elsewhere do not establish a field-to-DPAR alias.
    rows = [dict(parameter_id=key, library='L', cell='C', view='private_view',
                 instance=instance, property=prop)
            for key, instance, prop in SELECTED]
    for name, width, count in [('numeric', 300e-9, 7), ('reference_low', 100e-9, 4),
                               ('reference_high', 500e-9, 10)]:
        folder = os.path.join(root, name)
        os.makedirs(folder)
        netlist = os.path.join(folder, 'input.scs')
        with open(netlist, 'w') as stream:
            stream.write('simulator lang=spectre\nparameters ' + HEADERS[name] + '\n'
                         'include "toplevel.scs" section=TOP_TT_RFTYP\n'
                         '// Library name: L\n// Cell name: C\n// View name: private_view\n'
                         'R0 (net028 R0_1_int VSS) rhir_3t l=2.5u w=DPAR_4 multi=1\n'
                         'R1 (net022 R1_1_int VSS) rhir_3t l=2.5u w=DPAR_6 multi=1\n'
                         'M5 (d g s b) nulvt_sct nf=DPAR_1 nfin=DPAR_2 w=DPAR_7\n')
        task = dict(task_id='p1', point_id=1, design_point=0, test='TB', corner='TT', source_corner='TT',
                    netlist_path=netlist, template_files=[dict(path=netlist, relative_path='input.scs')])
        rdb = os.path.join(folder, 'Interactive.0.rdb')
        db = sqlite3.connect(rdb)
        db.executescript('CREATE TABLE parameter(parameterID INTEGER,name TEXT,testID TEXT,lib TEXT,cell TEXT,view TEXT,instance TEXT,type TEXT);'
                         'CREATE TABLE parameterValue(parameterID INTEGER,pointID INTEGER,value NUMERIC);'
                         'CREATE TABLE test(testID INTEGER,name TEXT);')
        for index, (row, value) in enumerate(zip(rows, [width] * 4 + [count] * 2), 1):
            db.execute('INSERT INTO parameter VALUES(?,?,?,?,?,?,?,?)',
                       (index, row['property'], '', 'L', 'C', 'private_view', row['instance'], 'deviceParam'))
            db.execute('INSERT INTO parameterValue VALUES(?,?,?)', (index, 1, value))
        db.commit()
        db.close()
        values = dict((key, count if instance == 'M5' else width) for key, instance, _prop in SELECTED)
        application = verify_native_parameter_application(rows, [task], values, parsed={'path': rdb})
        phase = dict(tasks=[task], parameter_application=application, native_parameter_rows=copy.deepcopy(rows),
                     applied_values=values.copy())
        references.append((name, values, phase))
    return references


class UnresolvedBindingTests(unittest.TestCase):
    def test_capture_excerpt_reproduces_unique_binding_failure_then_classifies_native_fallback(self):
        with tempfile.TemporaryDirectory() as root:
            refs = captured_binding_references(root)
            with self.assertRaisesRegex(PrivateNetlistUnsupported, 'no unique DPAR template binding'):
                _symbolize_native_dpar_phase(refs[0][2], SYMBOLS, os.path.join(root, 'symbolic'))
            proof = _native_unresolved_binding_fallback(refs, SYMBOLS)
            self.assertEqual(proof['failure_stage'], 'native_selected_parameter_binding')
            self.assertTrue(proof['native_generation_required'])
            self.assertFalse(proof['template_qualified'])
            self.assertEqual(len(proof['selected_parameter_bindings']), 12)
            self.assertEqual(len(proof['reference_checks']), 3)
            self.assertTrue(all(row['dpar_names'] == [] for row in proof['selected_parameter_bindings']))
            width = proof['selected_parameter_bindings'][0]
            self.assertEqual(width['name'], 'DTN_W')
            self.assertEqual(width['native_input_proof']['identity']['instance'], 'R0')

    def test_missing_or_wrong_proof_never_gets_fallback(self):
        for mutation in ('requested', 'identity', 'rdb', 'point', 'effective', 'missing', 'duplicate', 'application', 'scope'):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as root:
                refs = captured_binding_references(root)
                phase = refs[2][2]
                point = phase['parameter_application']['parameters'][0]['points'][0]
                if mutation == 'requested': point['matches_requested'] = False
                elif mutation == 'identity': point['native_input_proof']['identity']['property'] = 'w'
                elif mutation == 'rdb': point.pop('native_input_proof')
                elif mutation == 'point': phase['tasks'][0]['point_id'] = 2
                elif mutation == 'effective': point['effective'] = 1
                elif mutation == 'missing': phase['parameter_application']['parameters'][0]['points'] = []
                elif mutation == 'duplicate': phase['parameter_application']['parameters'][0]['points'].append(copy.deepcopy(point))
                elif mutation == 'application': phase['parameter_application']['ok'] = False
                elif mutation == 'scope': phase['native_parameter_rows'][0]['view'] = 'wrong'
                with self.assertRaises(PrivateNetlistUnsupported) as raised:
                    _native_unresolved_binding_fallback(refs, SYMBOLS)
                self.assertFalse(getattr(raised.exception, 'detail', {}).get('native_generation_required'))

    def test_model_topology_non_dpar_expression_and_duplicate_dpar_remain_fatal(self):
        for old, new in [('rhir_3t', 'other_model'), ('net028', 'different_node'),
                         ('CT_code=0', 'CT_code=1'), ('l=2.5u', 'l=3u'),
                         ('DPAR_14=7.61589m', 'DPAR_14=(CT_code+1)'),
                         ('parameters ', 'parameters DPAR_14=1 '),
                         ('section=TOP_TT_RFTYP', 'section=TOP_FF_RFTYP')]:
            with self.subTest(new=new), tempfile.TemporaryDirectory() as root:
                refs = captured_binding_references(root)
                path = refs[2][2]['tasks'][0]['netlist_path']
                with open(path) as stream: text = stream.read()
                with open(path, 'w') as stream: stream.write(text.replace(old, new))
                with self.assertRaises(PrivateNetlistUnsupported) as raised:
                    _native_unresolved_binding_fallback(refs, SYMBOLS)
                self.assertFalse(getattr(raised.exception, 'detail', {}).get('native_generation_required'))

    def test_bound_fields_keep_old_template_path(self):
        phase = {'parameter_application': {'parameters': [{'parameter_id': 'x', 'points': [
            {'dpar_status': 'bound_in_generated_point_scs'}]}]}}
        self.assertIsNone(_native_unresolved_binding_fallback([('numeric', {}, phase)], {'x': 'CRESCENDO_NL_x'}))

    def test_numeric_reference_required(self):
        with tempfile.TemporaryDirectory() as root:
            refs = captured_binding_references(root)
            with self.assertRaises(PrivateNetlistUnsupported):
                _native_unresolved_binding_fallback(refs[:2], SYMBOLS)


if __name__ == '__main__':
    unittest.main()
