from __future__ import print_function
import copy,json,os,shutil,tempfile,unittest
from crescendo_engine.config_binding import config_designs,private_config_plan,config_binding_lines,validate_config_binding_records
from crescendo_engine.workspace import build_occurrence_clone_plan,write_parameter_snapshot,write_schematic_patch,config_dependency_identity,source_design_identity,_hierarchy_libraries,_source_fingerprint_scopes
from crescendo_engine.persistent_maestro import _candidate_rows,_clone_lines,_snapshot_lines,_patch_lines,_pristine_cell_plan

def fixture(root):
    copied={'CFG':os.path.join(root,'CFG'),'DUT':os.path.join(root,'DUT')}
    for path in copied.values(): os.makedirs(path)
    binding={'hierarchy_path':'/I1','parent_path':'/','name':'I1','master_library':'CFG','master_cell':'raw','master_view':'symbol','owner_library':'DUT','owner_cell':'actual_top','owner_view':'schematic_alt','resolved_library':'DUT','resolved_cell':'actual_sub','resolved_view':'schematic_alt','resolved_view_type':'schematic','bound_library':'DUT','bound_cell':'actual_sub','bound_view':'config','stop':False,'fixed_cell':True,'fixed_library':True,'fixed_view':False}
    child=dict(binding,hierarchy_path='/I1/M0',parent_path='/I1',name='M0',master_library='PDK',master_cell='mos',owner_cell='actual_sub',resolved_library='PDK',resolved_cell='mos',resolved_view='spectre',bound_library='PDK',bound_cell='mos',bound_view='spectre',stop=True)
    design={'library':'CFG','cell':'TBconfig','view':'config_alt','resolved_top':{'library':'DUT','cell':'actual_top','view':'schematic_alt'},'config_dependencies':[{'library':'CFG','cell':'TBconfig','view':'config_alt'},{'library':'DUT','cell':'actual_sub','view':'config'}],'config_bindings':[binding,child]}
    w={'root':root,'copied_libraries':copied,'config_designs':[design]}
    p={'parameter_id':'p','backend':'schematic','name':'w','value':'1u','value_type':'string','design_library':'CFG','design_cell':'TBconfig','design_view':'config_alt','owner_library':'DUT','owner_cell':'actual_sub','owner_view':'schematic_alt','owner_view_type':'schematic','owner_occurrence_path':'/I1','hierarchy_path':'/I1/M0','occurrence_chain':[binding],'resolved_top':design['resolved_top']}
    return w,p,design

class ConfigBindingTest(unittest.TestCase):
    def test_requested_wrapper_and_actual_top_stay_distinct(self):
        with tempfile.TemporaryDirectory() as root:
            w,p,d=fixture(root)
            for wrapper in [dict(design={k:d[k] for k in ['library','cell','view']},hierarchy=d),dict(design=('CFG','TBconfig','config_alt'),hierarchy=d)]:
                actual=config_designs({'designs':[wrapper]})[0]
                self.assertEqual(actual['cell'],'TBconfig');self.assertEqual(actual['resolved_top']['cell'],'actual_top')
    def test_occurrence_clone_uses_actual_implementation_and_raw_symbol_source(self):
        with tempfile.TemporaryDirectory() as root:
            w,p,d=fixture(root);c=build_occurrence_clone_plan(w,[p],{'p':2e-6})[0]
            self.assertEqual((c['source_library'],c['source_cell'],c['source_view']),('DUT','actual_sub','schematic_alt'))
            self.assertEqual((c['symbol_source_library'],c['symbol_source_cell']),('CFG','raw'))
            self.assertEqual((c['parent_library'],c['parent_cell'],c['parent_view']),('DUT','actual_top','schematic_alt'))
    def test_clone_plan_maps_binding_and_child_owner_but_keeps_stop(self):
        with tempfile.TemporaryDirectory() as root:
            w,p,d=fixture(root);clones=build_occurrence_clone_plan(w,[p],{'p':2e-6});plan=private_config_plan(w,clones)[0]
            self.assertEqual(plan['bindings'][0]['target'],('DUT',clones[0]['clone_cell'],'schematic_alt'))
            self.assertEqual(plan['bindings'][1]['specifier'][1][1],clones[0]['clone_cell'])
            self.assertTrue(plan['bindings'][1]['stop'])
    def test_native_rules_verify_fixed_flags_and_stop_and_preserve_original(self):
        with tempfile.TemporaryDirectory() as root:
            w,p,d=fixture(root);clones=build_occurrence_clone_plan(w,[p],{'p':2e-6});lines,plan=config_binding_lines(w,clones,os.path.join(root,'records'));s='\n'.join(lines)
            self.assertIn('hdbSaveACopy',s);self.assertIn('hdbOpen("CFG" "TBconfig" "config_alt" "w")',s)
            self.assertIn('hdbBind(crescConfigPath "DUT" "%s" "symbol" "I1" t t nil)'%clones[0]['clone_cell'],s)
            self.assertIn('hdbIsAtStopPoint',s);self.assertIn('hdbSetObjBindRule',s)
            self.assertIn('hdbSetDefaultStopListString(crescPrivateConfig "spectre")',s)
            d['config_bindings'][1]['resolved_view']='schematic_alt'
            mixed_lines,_=config_binding_lines(w,clones,os.path.join(root,'records'))
            self.assertIn('hdbSetDefaultStopListString(crescPrivateConfig "cr_config_stop_sentinel")','\n'.join(mixed_lines))
    def test_ignored_bookkeeping_is_not_materialized(self):
        with tempfile.TemporaryDirectory() as root:
            w,p,d=fixture(root);d['config_bindings'].append(dict(d['config_bindings'][0],hierarchy_path='/PIN',name='PIN',netlister_ignored=True))
            self.assertEqual(len(private_config_plan(w,[])[0]['bindings']),2)
    def test_incomplete_or_duplicate_graph_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            w,p,d=fixture(root)
            d['config_bindings'].append(copy.deepcopy(d['config_bindings'][0]))
            with self.assertRaises(ValueError):private_config_plan(w,[])
    def test_stop_ancestor_cannot_be_crossed(self):
        with tempfile.TemporaryDirectory() as root:
            w,p,d=fixture(root);d['config_bindings'][0]['stop']=True
            with self.assertRaises(ValueError):private_config_plan(w,[])
    def test_config_evidence_exact_and_incomplete_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            w,p,d=fixture(root);plan=private_config_plan(w,[]);path=os.path.join(root,'records');rows=[]
            for item in plan:
                rows.append('CONFIG_TOP\t'+'\t'.join(json.dumps(v) for v in item['config']+item['top']))
                for node in item['bindings']:
                    rows.append('CONFIG_BINDING\t'+'\t'.join(json.dumps(v) for v in item['config']+(node['hierarchy_path'],)+node['target'])+'\t'+('t' if node['stop'] else 'nil'))
            with open(path,'w') as stream:stream.write('\n'.join(rows)+'\n')
            self.assertEqual(validate_config_binding_records(plan,path)['bindings'],2)
            self.assertEqual(validate_config_binding_records(json.loads(json.dumps(plan)),path)['bindings'],2)
            with open(path,'w') as stream:stream.write(rows[0]+'\n')
            with self.assertRaises(ValueError):validate_config_binding_records(plan,path)
    def test_persistent_rows_snapshots_and_pristine_use_actual_view(self):
        with tempfile.TemporaryDirectory() as root:
            w,p,d=fixture(root);clone=build_occurrence_clone_plan(w,[p],{'p':2e-6});lookup={('CFG','TBconfig','config_alt','/I1'):clone[0]}
            rows=_candidate_rows(w,[p],{'p':2e-6},lookup);row=rows[0]
            self.assertEqual(row['view'],'schematic_alt');self.assertEqual(row['netlist_design_cell'],'actual_top')
            for lines in [_snapshot_lines(rows,root+'/records','POST'),_patch_lines(rows,root+'/records',None)]:self.assertIn('"schematic_alt" "schematic"','\n'.join(lines))
            self.assertEqual(_pristine_cell_plan(rows,'epoch')[0]['view'],'schematic_alt')
    def test_top_parameter_target_is_actual_owner_not_requested_config_cell(self):
        with tempfile.TemporaryDirectory() as root:
            w,p,d=fixture(root);p.update(owner_cell='actual_top',owner_occurrence_path='/',hierarchy_path='/M0',occurrence_chain=[])
            row=_candidate_rows(w,[p],{'p':2e-6},{})[0]
            self.assertEqual((row['library'],row['cell']),('DUT','actual_top'))
            plan=write_parameter_snapshot(w,[p],{'p':2e-6},root+'/snap.il',root+'/records','POST')
            self.assertEqual(plan['parameters'][0]['cell'],'actual_top')
    def test_both_clone_paths_copy_actual_view_and_original_symbol_with_connection_checks(self):
        with tempfile.TemporaryDirectory() as root:
            w,p,d=fixture(root);clones=build_occurrence_clone_plan(w,[p],{'p':2e-6})
            plan=write_schematic_patch(w,[p],{'p':2e-6},root+'/clone.il',emit_parameters=False)
            with open(root+'/clone.il') as f:legacy=f.read()
            for text in [legacy,'\n'.join(_clone_lines(clones,root+'/clone.records',w))]:
                self.assertIn('dbOpenCellViewByType("CFG" "raw" "symbol"',text)
                self.assertIn('dbOpenCellViewByType("DUT" "actual_sub" "schematic_alt"',text)
                self.assertIn('dbOpenCellViewByType("DUT" "actual_top" "schematic_alt"',text)
                self.assertIn('connectivity mismatch',text);self.assertIn('hdbIsAtStopPoint',text)
    def test_config_dependency_hash_changes_on_config_content_only(self):
        with tempfile.TemporaryDirectory() as root:
            w,p,d=fixture(root)
            for dep in d['config_dependencies']:
                folder=os.path.join(w['copied_libraries'][dep['library']],dep['cell'],dep['view']);os.makedirs(folder)
                with open(os.path.join(folder,'expand.cfg'),'w') as f:f.write('original')
            setup={'source':{'library_root':w['copied_libraries']['CFG'],'setup_db':os.path.join(w['copied_libraries']['CFG'],'TB','maestro','maestro.sdb')},'tests':[{'library':'CFG'}]}
            with open(root+'/cds.lib','w') as f:f.write(''.join('DEFINE %s %s\n'%(lib,path) for lib,path in w['copied_libraries'].items()))
            hierarchy={'designs':[{'design':{k:d[k] for k in ['library','cell','view']},'hierarchy':d}]}
            # Remove a fake PDK/raw dependency; this test focuses on exact config files.
            hierarchy['designs'][0]['hierarchy']['config_bindings']=[]
            a=config_dependency_identity(setup,hierarchy)
            with open(os.path.join(w['copied_libraries']['CFG'],'TBconfig','config_alt','expand.cfg'),'w') as f:f.write('changed')
            b=config_dependency_identity(setup,hierarchy)
            self.assertNotEqual(a['fingerprint'],b['fingerprint'])
            self.assertEqual(len(a['dependencies']),2)
    def test_multiple_ancestor_clones_use_private_parent_and_exact_raw_binding(self):
        with tempfile.TemporaryDirectory() as root:
            w,p,d=fixture(root)
            first=d['config_bindings'][0]
            second=dict(first,hierarchy_path='/I1/I2',parent_path='/I1',name='I2',owner_cell='actual_sub',master_cell='raw2',resolved_cell='actual_nested')
            leaf=dict(d['config_bindings'][1],hierarchy_path='/I1/I2/M0',parent_path='/I1/I2',owner_cell='actual_nested')
            d['config_bindings']=[first,second,leaf]
            p.update(owner_cell='actual_nested',owner_occurrence_path='/I1/I2',hierarchy_path='/I1/I2/M0',occurrence_chain=[first,second])
            clones=build_occurrence_clone_plan(w,[p],{'p':2e-6})
            self.assertEqual(len(clones),2)
            self.assertEqual(clones[1]['parent_cell'],clones[0]['clone_cell'])
            plan=private_config_plan(w,clones)[0]
            self.assertEqual(plan['bindings'][2]['specifier'][1][1],clones[0]['clone_cell'])
            self.assertEqual(plan['bindings'][2]['specifier'][2][1],clones[1]['clone_cell'])
            lines=_clone_lines(clones,root+'/records',w)
            self.assertIn('dbOpenCellViewByType("DUT" "%s" "schematic_alt"'%clones[0]['clone_cell'],'\n'.join(lines))

    def test_clone_cdf_preserves_module_defaults_and_callbacks(self):
        with tempfile.TemporaryDirectory() as root:
            w,p,d=fixture(root);clones=build_occurrence_clone_plan(w,[p],{'p':2e-6})
            lines='\n'.join(_clone_lines(clones,root+'/records',w))
            self.assertIn('cdfGetBaseCellCDF(ddGetObj("CFG" "raw"))',lines)
            self.assertIn('cdfCopyCDF(crescCloneDestCell "baseCellData"',lines)
            self.assertIn('cdfSaveCDF(crescCloneDestCDF)',lines)
            self.assertIn('cdfFindParamByName(crescCloneDestCDF',lines)
            self.assertIn('crescSourceParam~>callback',lines)
            self.assertIn('crescCloneSourceCDF~>simInfo',lines)

    def test_readonly_sources_can_be_fingerprinted_without_changes(self):
        with tempfile.TemporaryDirectory() as root:
            w,p,d=fixture(root)
            for dep in d['config_dependencies']:
                folder=os.path.join(w['copied_libraries'][dep['library']],dep['cell'],dep['view']);os.makedirs(folder)
                path=os.path.join(folder,'expand.cfg')
                with open(path,'w') as f:f.write('readonly')
                os.chmod(path,0o444);os.chmod(folder,0o555)
            setup={'source':{'library_root':w['copied_libraries']['CFG'],'setup_db':w['copied_libraries']['CFG']+'/TB/maestro/maestro.sdb'},'tests':[{'library':'CFG'}]}
            with open(root+'/cds.lib','w') as f:f.write(''.join('DEFINE %s %s\n'%(lib,path) for lib,path in w['copied_libraries'].items()))
            d['config_bindings']=[]
            h={'designs':[{'design':d,'hierarchy':d}]}
            before={}
            for dep in d['config_dependencies']:
                path=os.path.join(w['copied_libraries'][dep['library']],dep['cell'],dep['view'],'expand.cfg')
                with open(path,'rb') as f:before[path]=(f.read(),os.stat(path).st_mode)
            self.assertIsNotNone(config_dependency_identity(setup,h))
            for path,expected in before.items():
                with open(path,'rb') as f:self.assertEqual((f.read(),os.stat(path).st_mode),expected)

    def test_source_scopes_cover_configs_raw_and_bound_implementations(self):
        with tempfile.TemporaryDirectory() as root:
            w,p,d=fixture(root);h={'designs':[{'design':d,'hierarchy':d}]}
            self.assertTrue({'CFG','DUT','PDK'}.issubset(_hierarchy_libraries(h)))
            setup={'source':{'library_root':w['copied_libraries']['CFG'],'setup_db':w['copied_libraries']['CFG']+'/TB/maestro/maestro.sdb'}}
            scopes=_source_fingerprint_scopes(setup,h,w['copied_libraries'])
            self.assertIn('TBconfig',scopes['CFG']);self.assertIn('raw',scopes['CFG']);self.assertIn('actual_sub',scopes['DUT'])

if __name__=='__main__':unittest.main()
