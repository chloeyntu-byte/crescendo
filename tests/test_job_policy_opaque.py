import copy
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine.job_policy import policy_from_records, parse_policy_records, job_policy_capture_definitions, RECORD_SCHEMA
from crescendo_engine.maestro_context import parse_launch_records, SCHEMA
from test_job_policy import record


class OpaqueJobPolicyTests(unittest.TestCase):
    def rows(self):
        return [('P',['global',None,True]), ('J',['global',None,'name','MyPolicy']),
                ('J',['global',None,'distributionmethod','Local']), ('J',['global',None,'maxjobs','2']),
                ('O',['global',None,'pluginBag','list']), ('O',['global',None,'pluginMode','symbol']),
                ('P',['test','B',True]), ('D',['test','B'])]

    def test_opaque_types_warn_without_entering_replayed_properties(self):
        result=policy_from_records(self.rows())
        self.assertTrue(result['available'])
        self.assertNotIn('pluginBag',result['global']['properties'])
        self.assertEqual(result['global']['opaque_properties'],{'pluginBag':'list','pluginMode':'symbol'})
        self.assertEqual(result['tests'][0]['opaque_properties'],result['global']['opaque_properties'])
        self.assertIsNot(result['tests'][0]['opaque_properties'],result['global']['opaque_properties'])
        self.assertTrue(any('global' in line and 'pluginBag (list)' in line for line in result['warnings']))
        self.assertTrue(any('test B' in line for line in result['warnings']))

    def test_legacy_no_opaque_and_short_metadata_rows_still_parse(self):
        legacy=[row for row in self.rows() if row[0]!='O']
        result=policy_from_records(legacy)
        self.assertFalse(result['warnings'])
        self.assertNotIn('opaque_properties',result['global'])
        raw=RECORD_SCHEMA+'\n'+record('P','global',None,True)+'\n'+record('O','global',None,'pluginBag')+'\n'
        self.assertEqual(parse_policy_records(raw)['global']['opaque_properties'],{'pluginBag':'non_scalar'})

    def test_conflicting_opaque_and_scalar_evidence_is_rejected(self):
        for extra in [('J',['global',None,'pluginBag','pretended-value']),
                      ('O',['global',None,'pluginBag','list']),
                      ('O',['test','B','another','list']),
                      ('O',['global',None,'invalid-name','list']),
                      ('O',['global',None,'another','arbitrary payload'])]:
            with self.subTest(extra=extra),self.assertRaises(ValueError):
                policy_from_records(self.rows()+[extra])

    def test_invoking_menu_context_accepts_optional_opaque_records(self):
        with tempfile.TemporaryDirectory() as root:
            view=os.path.join(root,'lib','cell','maestro');os.makedirs(view)
            setup=os.path.join(view,'maestro.sdb')
            with open(setup,'w') as stream:stream.write('<setupdb><active/></setupdb>')
            raw=SCHEMA+'\n'+record('C','lib','cell','maestro',root,view,None,'/opt/cadence/bin/ocean')+'\n'
            raw+='\n'.join(record(tag,*values) for tag,values in self.rows())+'\n'
            parsed=parse_launch_records(raw)
            self.assertEqual(parsed['job_policy']['global']['opaque_properties']['pluginBag'],'list')
            self.assertTrue(parsed['job_policy']['warnings'])

    def test_menu_and_native_capture_encode_names_and_types_without_values(self):
        definitions=job_policy_capture_definitions()
        path=os.path.join(os.path.dirname(os.path.dirname(__file__)),'cadence','crescendo_maestro_menu.il')
        with open(path,encoding='utf-8') as stream:menu=stream.read()
        start=definitions.index('procedure(crescWriteOneJobPolicy')
        end=definitions.index('procedure(crescWriteJobPolicyRecords')
        self.assertIn(definitions[start:end].strip(),menu)
        opaque=definitions[definitions.index('fprintf(port "O'):definitions.index('cursor = cddr(cursor)')]
        self.assertIn('symbolToString(key)',opaque)
        self.assertIn('listp(value)',opaque)
        self.assertNotIn('sprintf(nil "%L" value)',opaque)
