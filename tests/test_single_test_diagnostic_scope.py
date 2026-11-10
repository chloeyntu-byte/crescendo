import json
import os
import tempfile
import unittest
from crescendo_engine.environment_diagnostics import _settings_xml, _single_test_scope_summary, collect_run_diagnostics


class SingleTestDiagnosticsTest(unittest.TestCase):
    def test_scope_and_projection_receipt(self):
        setup = {'selected_test':'A', 'tests':[{'name':'A'}], '_source_tests':[{'name':'A'}, {'name':'B'}],
                 'corners':[{'name':'X', 'enabled':True}, {'name':'Y','enabled':False}, {'name':'Z','enabled':True}],
                 'test_corner_masks':{'A':{'known':True,'disabled_corners':['Z']}}}
        spec = {'selected_test':'A', 'objectives':[{'output_key':'A::PN','corners':['X']}], 'constraints':[]}
        with tempfile.TemporaryDirectory() as root:
            with open(os.path.join(root, 'native_capture_projection.json'), 'w') as f:
                json.dump({'schema_version':'crescendo.native_test_projection.v1', 'selected_test':'A', 'removed':[{'kind':'test'}], 'files':[]}, f)
            records = collect_run_diagnostics(root, 'maestro_netlist', prepared={'setup':setup}, spec=spec)
        scope = next(r['payload'] for r in records if r['observation']=='diagnostic.single_test_scope')
        self.assertEqual(scope['loaded_test_count'], 1)
        self.assertEqual(scope['source_test_names_sample'], ['A', 'B'])
        self.assertEqual(scope['eligible_corners'], ['X'])
        self.assertEqual(scope['metrics'][0]['corners'], ['X'])
        self.assertEqual(scope['private_projection_receipts'][0]['selected_test'], 'A')

    def test_xml_summary_does_not_traverse_unselected_test(self):
        data = b'<setupdb><active><tests><test>A<tooloptions><option>cell<value>chosen</value></option></tooloptions></test><test>B<tooloptions><option>cell<value>unselected_secret</value></option></tooloptions></test></tests><vars><var>g<value>1</value></var></vars></active></setupdb>'
        summary = _settings_xml('x', data, {'read_truncated':False}, selected_test='A')
        rows = str(summary['settings_rows'])
        self.assertIn('chosen', rows)
        self.assertNotIn('unselected_secret', rows)
        self.assertIn("'value': 'g'", rows)


if __name__ == '__main__':
    unittest.main()
