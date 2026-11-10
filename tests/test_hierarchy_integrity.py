"""Native hierarchy identity gates with owned records, no EDA invocation."""
import contextlib
import copy
import json
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine import hierarchy as hierarchy_module


class NativeHierarchyIntegrityTests(unittest.TestCase):
    def setUp(self):
        self.design={'library':'D','cell':'TOP','view':'schematic'}
        self.valid={'cells':[{'hierarchy_path':'/','library':'D','cell':'TOP','view':'schematic'}],
                    'instances':[{'hierarchy_path':'/M0','master_library':'genericDevice','master_cell':'nmos'}],
                    'parameters':[],'errors':[],'technology_bindings':[]}

    def test_generic_device_without_cdf_or_known_technology_remains_allowed(self):
        data=copy.deepcopy(self.valid)
        data['technology_bindings']=[{'library':'D','technology_library':None}]
        self.assertIsNone(hierarchy_module.validate_native_hierarchy(data,self.design))
        self.assertEqual(data['parameters'],[])

    def test_missing_root_cell_rejects_even_when_device_identities_exist(self):
        data=copy.deepcopy(self.valid);data['cells']=[]
        with self.assertRaisesRegex(RuntimeError,'root cell was not opened'):
            hierarchy_module.validate_native_hierarchy(data,self.design)

    def test_recorded_cell_open_failure_rejects_with_other_valid_records(self):
        data=copy.deepcopy(self.valid);data['errors']=[{'kind':'ERROR','hierarchy_path':'/I0'}]
        with self.assertRaisesRegex(RuntimeError,'cell could not be opened at /I0'):
            hierarchy_module.validate_native_hierarchy(data,self.design)

    def test_missing_master_library_or_cell_rejects_without_requiring_cdf(self):
        for key in ('master_library','master_cell'):
            for value in (None,''):
                data=copy.deepcopy(self.valid);data['instances'][0][key]=value
                with self.subTest(key=key,value=value),self.assertRaisesRegex(RuntimeError,'missing master identity at /M0'):
                    hierarchy_module.validate_native_hierarchy(data,self.design)

    def test_cycle_and_depth_limit_remain_visible_warnings(self):
        data=copy.deepcopy(self.valid)
        data['errors']=[{'kind':'CYCLE','hierarchy_path':'/I0'},{'kind':'DEPTH_LIMIT','hierarchy_path':'/I1'}]
        before=copy.deepcopy(data)
        self.assertIsNone(hierarchy_module.validate_native_hierarchy(data,self.design))
        self.assertEqual(data,before)

    def probe_records(self,root,records):
        library=os.path.join(root,'owned library');os.makedirs(library)
        config={'cadence':{'virtuoso_bin':'owned-fixture-never-executed'},'runtime':{'run_root':os.path.join(root,'runtime')}}
        setup={'source':{'library_root':library},'setup_locator':{'library_name':'D'},'tests':[dict(self.design,name='owned')]}
        @contextlib.contextmanager
        def display(environment,workdir):
            yield environment,{'state':'NOT TESTED owned fixture'}
        def fake_command(argv,cwd,**kwargs):
            self.assertEqual(argv[0],'owned-fixture-never-executed')
            with open(os.path.join(cwd,'hierarchy.records'),'w',encoding='utf-8') as stream:stream.write(records)
            return {'returncode':0,'fixture_only':True}
        stack=contextlib.ExitStack()
        stack.enter_context(mock.patch.object(hierarchy_module,'run_id',return_value='hierarchy_owned'))
        stack.enter_context(mock.patch.object(hierarchy_module,'child_environment',return_value={'PATH':os.defpath}))
        stack.enter_context(mock.patch.object(hierarchy_module,'owned_headless_display',side_effect=display))
        stack.enter_context(mock.patch.object(hierarchy_module,'run_command',side_effect=fake_command))
        return setup,config,stack,os.path.join(config['runtime']['run_root'],'probes','hierarchy_owned','hierarchy.json')

    def test_probe_saves_incomplete_evidence_before_rejecting_missing_master(self):
        with tempfile.TemporaryDirectory(prefix='crescendo hierarchy integrity ') as root:
            records='CELL\t"/"\t"D"\t"TOP"\t"schematic"\t0\nINSTANCE\t"/M0"\t"M0"\tnil\tnil\tnil\tnil\t"/"\t0\n'
            setup,config,stack,evidence=self.probe_records(root,records)
            with stack,self.assertRaisesRegex(RuntimeError,'missing master identity') as raised:
                hierarchy_module.probe_hierarchy(setup,config)
            self.assertIn(evidence,str(raised.exception))
            self.assertTrue(os.path.isfile(evidence))
            with open(evidence,encoding='utf-8') as stream:saved=json.load(stream)
            self.assertIsNone(saved['designs'][0]['hierarchy']['instances'][0]['master_library'])
            raw=os.path.join(os.path.dirname(evidence),'design_00','hierarchy.records')
            with open(raw,encoding='utf-8') as stream:self.assertEqual(stream.read(),records)

    def test_probe_accepts_generic_no_cdf_records_and_preserves_warning_evidence(self):
        with tempfile.TemporaryDirectory(prefix='crescendo hierarchy generic ') as root:
            records='CELL\t"/"\t"D"\t"TOP"\t"schematic"\t0\nINSTANCE\t"/M0"\t"M0"\t"GENERIC"\t"device"\t"symbol"\tnil\t"/"\t0\nDEPTH_LIMIT\t"/M0"\t"GENERIC"\t"device"\t"schematic"\n'
            setup,config,stack,evidence=self.probe_records(root,records)
            with stack:result=hierarchy_module.probe_hierarchy(setup,config)
            normalized=result['designs'][0]['hierarchy']
            self.assertEqual(normalized['parameters'],[])
            self.assertEqual(normalized['errors'][0]['kind'],'DEPTH_LIMIT')
            self.assertTrue(os.path.isfile(evidence))


if __name__=='__main__':unittest.main()
