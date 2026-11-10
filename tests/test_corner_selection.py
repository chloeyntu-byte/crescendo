"""User corner choices and opaque strings across UI/native/result boundaries."""
import copy
import unittest
from types import SimpleNamespace
from crescendo_engine.corner_selection import catalogue, selection_plan, effective_setup, normalize_previous_selection
from crescendo_engine.native_identity import numeric_domain, literal_domain, point_variable_checks
from crescendo_engine.private_netlist import validate_capture_corners
from crescendo_engine.rdb import aggregate_output
from crescendo_engine.gui import OptimizerGui
from crescendo_engine.server import OptimizerService
from test_native_corner_identity import snapshot, task


def source():
    return {'tests':[{'name':'T','enabled':True}], 'corners':[
        {'name':'A','enabled':True,'variables':[{'name':'V','value':'1'}, {'name':'TEXT','value':'"one  name"'}]},
        {'name':'B','enabled':False,'variables':[{'name':'V','value':'2'}, {'name':'OTHER','value':'"old missing file"'}]},
        {'name':'_default','enabled':False,'variables':[]}]}


def plan(setup, names):
    return selection_plan(setup, {'objectives':[{'output_key':'T::gain','corners':names}]})


class CornerSelectionTests(unittest.TestCase):
    def test_disabled_catalogue_defaults_and_constraint_union(self):
        setup=source(); before=copy.deepcopy(setup)
        result=selection_plan(setup, {'objectives':[{'output_key':'T::gain'}],
            'constraints':[{'output_key':'T::power','corners':['B']}]})
        self.assertEqual(result['available'], ['A','B','_default'])
        self.assertEqual(result['source_enabled'], ['A'])
        self.assertEqual(result['selected'], ['A','B'])
        chosen=effective_setup(setup,result)
        self.assertEqual([r['name'] for r in chosen['corners'] if r['enabled']], ['A','B'])
        self.assertEqual(chosen['corners'][1]['variables'], setup['corners'][1]['variables'])
        self.assertEqual(setup,before)
        self.assertIsNone(selection_plan(setup,None))

    def test_implicit_nominal_and_all_off_are_distinct(self):
        self.assertEqual(plan({},None)['selected'], ['_default'])
        setup=source()
        for row in setup['corners']:row['enabled']=False
        with self.assertRaisesRegex(ValueError,'No enabled'):plan(setup,None)
        self.assertEqual(plan(setup,['B'])['selected'],['B'])

    def test_unknown_names_and_duplicate_catalogue_are_not_silently_replaced(self):
        with self.assertRaisesRegex(ValueError,'Unknown'):plan(source(),['missing'])
        with self.assertRaises(ValueError):plan(source(),'A')
        setup=source();setup['corners'].append(copy.deepcopy(setup['corners'][0]))
        with self.assertRaisesRegex(ValueError,'duplicate'):catalogue(setup)

    def test_owned_selection_normalizes_only_enabled_flags(self):
        setup=source(); selected=plan(setup,['B'])
        private=effective_setup(setup,selected)
        private['corners'][1]['variables'][0]['value']='9'
        normalized,proof=normalize_previous_selection(setup,private,selected)
        self.assertTrue(proof)
        self.assertEqual([r['enabled'] for r in normalized['corners']], [True,False,False])
        self.assertEqual(normalized['corners'][1]['variables'][0]['value'],'9')
        private['corners'][2]['enabled']=True
        normalized,proof=normalize_previous_selection(setup,private,selected)
        self.assertIsNone(proof)
        self.assertEqual(normalized,private)

    def test_gui_catalogue_uses_current_test_eligible_corners(self):
        setup=source()
        setup['selected_test']='T'
        setup['corners'].append({'name':'MASKED','enabled':True,'variables':[]})
        setup['test_corner_masks']={'T':{'known':True,'disabled_corners':['MASKED'], 'nominal_enabled':False}}
        app=SimpleNamespace(prepared={'setup':setup},selected_test='T')
        app._corner_names=lambda: OptimizerGui._corner_names(app)
        self.assertEqual(OptimizerGui._corner_names(app),['A'])
        self.assertEqual(OptimizerGui._default_corner_names(app),['A'])
        setup['test_corner_masks']['T']['known']=False
        self.assertEqual(OptimizerGui._corner_names(app),[])
        self.assertEqual(OptimizerGui._default_corner_names(app),[])
        setup['test_corner_masks']['T']['known']=True
        for row in setup['corners']:row['enabled']=False
        self.assertEqual(OptimizerGui._corner_names(app),[])
        self.assertEqual(OptimizerGui._default_corner_names(app),[])

    def test_missing_selected_result_and_expanded_source_alias(self):
        parsed={'output_map':{'T::gain':{'values':[{'corner':'A_0','source_corner':'A','numeric':1},
            {'corner':'A_1','source_corner':'A','numeric':3}]}}}
        self.assertEqual(aggregate_output(parsed,'T::gain','mean',['A']),2)
        with self.assertRaisesRegex(ValueError,'missing selected corners B'):
            aggregate_output(parsed,'T::gain','mean',['A','B'])
        parsed['output_map']['T::gain']['values'].append({'corner':'B','numeric':5})
        self.assertEqual(aggregate_output(parsed,'T::gain','mean',['A','B']),3)

    def test_strings_are_typed_and_preserve_spaces_and_escapes(self):
        self.assertIsNone(numeric_domain('"1 2"'))
        self.assertEqual(literal_domain('"1 2"'),['1 2'])
        self.assertEqual(literal_domain('"a  b" "c\\\\d"'),['a  b','c\\d'])
        self.assertEqual(literal_domain('1 2'),[1,2])

    def test_ordinary_strings_never_gate_missing_changed_or_native_expression(self):
        for value in ['"old nonexistent file.any"','"1 2"','strcat("prefix" suffix)','"native\\q"']:
            for actual in [[],[{'name':'TEXT','value':None,'expression':'"different"'}]]:
                with self.subTest(value=value, actual=actual):
                    setup={'tests':[{'name':'T','enabled':True}], 'corners':[
                        {'name':'A','enabled':True,'variables':[{'name':'TEXT','value':value}]}]}
                    row=task('T','A',1);row['native_parameters']=actual
                    schedule={'read_ok':True,'groups':{'A':{'enabled':True,'count':1,'disabled_tests':[]}},
                              'nominal_enabled':False,'nominal_tests':{'T':False}}
                    checked=validate_capture_corners(setup,[row],{},snapshot(),schedule)
                    self.assertTrue(checked['ok'],checked)

    def test_numeric_corner_override_still_rejects_wrong_value(self):
        setup=effective_setup(source(),plan(source(),['B']))
        row=task('T','B',1);row['native_parameters']=[{'name':'V','value':1,'expression':'1'}]
        result=validate_capture_corners(setup,[row],{},snapshot())
        self.assertFalse(result['ok'])

    def test_same_name_values_resolve_per_corner_without_leaking_overrides(self):
        setup=effective_setup(source(),plan(source(),['A','B']))
        rows=[]
        for name,value in [('A',1),('B',2)]:
            row=task('T',name,value);row['native_parameters']=[{'name':'V','value':value,'expression':str(value)}];rows.append(row)
        result=validate_capture_corners(setup,rows,{},snapshot())
        self.assertTrue(result['ok'],result)

    def test_metric_test_mask_is_reported_before_solver(self):
        from crescendo_engine.corner_selection import missing_metric_corners
        selected=plan(source(),['A','B'])
        missing=missing_metric_corners([{'test':'T','source_corner':'A'}, {'test':'Other','source_corner':'B'}],selected)
        self.assertEqual(missing,[{'output_key':'T::gain','test':'T','corners':['B']}])
        self.assertEqual(missing_metric_corners([{'test':'T','source_corner':'A'}, {'test':'T','source_corner':'B'}],selected),[])

    def test_model_projection_uses_current_corner_selection(self):
        from unittest import mock
        from crescendo_engine.private_netlist import _capture_request_config
        setup=source()
        def project(config, effective):
            self.assertEqual([r['name'] for r in effective['corners'] if r['enabled']], ['B'])
            return {'runtime':{}}
        with mock.patch('crescendo_engine.simulator_profiles.apply_source_profile_defaults',side_effect=project):
            result=_capture_request_config({'setup':setup},{},{'objectives':[{'output_key':'T::gain','corners':['B']}]})
        self.assertEqual(result['_capture_corner_selection']['selected'],['B'])
        self.assertTrue(setup['corners'][0]['enabled'])


import os
from tests import test_gui_optional_settings as tk_fixture


@unittest.skipUnless(os.environ.get('DISPLAY'), 'Real Tk requires DISPLAY')
class CornerSelectionTkTests(unittest.TestCase):
    setUp = tk_fixture.GuiOptionalSettingsTkTests.setUp
    _close = tk_fixture.GuiOptionalSettingsTkTests._close
    _wait = tk_fixture.GuiOptionalSettingsTkTests._wait
    _prepared = tk_fixture.GuiOptionalSettingsTkTests._prepared

    def test_eligible_corners_default_and_configuration_restoration(self):
        self._prepared()
        app=self.app
        setup=app.prepared['setup']
        app.selected_test=setup['selected_test']='VCO'
        setup['corners']=copy.deepcopy(source()['corners'])+[{'name':'MASKED','enabled':True}]
        setup['test_corner_masks']={'VCO':{'known':True,'disabled_corners':['MASKED'],'nominal_enabled':False}}
        app._reset_metric_rows();app.add_objective()
        row=app.objective_rows[0]
        self.assertEqual(row.corner_picker.corners,['A'])
        self.assertEqual(row.corner_picker.selected(),['A'])
        saved={'output_key':row.output_var.get(),'corners':['A','B','MASKED','_default']}
        app._reset_metric_rows();app.add_objective(saved)
        app.add_constraint({'output_key':saved['output_key'],'limit':'5','corners':['B','MASKED']})
        self.assertEqual(app.objective_rows[0].corner_picker.selected(),['A'])
        self.assertEqual(app.constraint_rows[0].corner_picker.selected(),[])
        self.assertEqual(app._settings_context()['corners'],['A'])
        app.add_objective({'corners':[]})
        self.assertEqual(app.objective_rows[-1].corner_picker.selected(),[])
        self.assertFalse(self.showerror.called)

    def test_all_off_and_unknown_mask_never_reenable_corners(self):
        self._prepared()
        app=self.app
        setup=app.prepared['setup']
        app.selected_test=setup['selected_test']='VCO'
        setup['test_corner_masks']={'VCO':{'known':True,'disabled_corners':[], 'nominal_enabled':False}}
        setup['corners']=[{'name':'A','enabled':False},{'name':'B','enabled':False}]
        app._reset_metric_rows();app.add_objective()
        self.assertEqual(app.objective_rows[0].corner_picker.selected(),[])
        setup['corners']=[{'name':'A','enabled':True}]
        setup['test_corner_masks']['VCO']['known']=False
        app._reset_metric_rows();app.add_objective({'corners':['A']})
        self.assertEqual(app.objective_rows[0].corner_picker.corners,[])
        self.assertEqual(app.objective_rows[0].corner_picker.selected(),[])
        setup['corners']=[]
        app._reset_metric_rows();app.add_objective()
        self.assertEqual(app.objective_rows[0].corner_picker.selected(),[])
        setup['test_corner_masks']['VCO']['nominal_enabled']=True
        app._reset_metric_rows();app.add_objective()
        self.assertEqual(app.objective_rows[0].corner_picker.selected(),['_default'])


class ExpandedStringIdentityTests(unittest.TestCase):
    def test_scalar_strings_can_disambiguate_equal_numeric_sweeps_without_becoming_file_checks(self):
        setup={'tests':[{'name':'T','enabled':True}], 'corners':[]}
        schedule={'read_ok':True,'groups':{},'nominal_enabled':False,'nominal_tests':{'T':False}}
        tasks=[]
        for group,literal in [('A','"first  unused.any"'),('B','"second unused.any"')]:
            setup['corners'].append({'name':group,'enabled':True,'variables':[
                {'name':'V','value':'1 2'},{'name':'TEXT','value':literal}]})
            schedule['groups'][group]={'enabled':True,'count':2,'disabled_tests':[]}
            for value in (1,2):
                row=task('T','opaque_%d'%len(tasks),len(tasks)+1)
                row['native_parameters']=[{'name':'V','value':value,'expression':str(value)},
                                          {'name':'TEXT','value':None,'expression':literal}]
                tasks.append(row)
        result=validate_capture_corners(setup,tasks,{},snapshot(),schedule)
        self.assertTrue(result['ok'],result)
        self.assertEqual([r['source_corner'] for r in tasks],['A','A','B','B'])
        # No independent evidence: report ambiguity, never guess from suffixes.
        for row in tasks:
            row.pop('source_corner',None);row['native_parameters']=row['native_parameters'][:1]
        self.assertFalse(validate_capture_corners(setup,tasks,{},snapshot(),schedule)['ok'])

    def test_quoted_argument_numeric_formula_retains_numeric_rdb_comparison(self):
        current={'name':'T','variables':[{'name':'V','value':'VAR("BASE")*2'}]}
        row=task('T','A',1);row['native_parameters']=[{'name':'V','value':4,'expression':'4'}]
        differences,checks=point_variable_checks({},current,{},row,{'parameters':[{'pointID':1,'name':'V','value':2}]})
        self.assertFalse(checks[0]['ok'])
        self.assertEqual(differences[0]['reason'],'native_rdb_netlist_value_mismatch')

    def test_string_sweep_preserves_every_literal_point(self):
        setup={'tests':[{'name':'T','enabled':True}], 'corners':[
            {'name':'S','enabled':True,'variables':[{'name':'TEXT','value':'"one  item" "2"'}]}]}
        schedule={'read_ok':True,'groups':{'S':{'enabled':True,'count':2,'disabled_tests':[]}},
            'nominal_enabled':False,'nominal_tests':{'T':False}}
        tasks=[]
        for literal in ['"one  item"','"2"']:
            row=task('T','opaque_%d'%len(tasks),len(tasks)+1)
            row['native_parameters']=[{'name':'TEXT','value':None,'expression':literal}];tasks.append(row)
        self.assertTrue(validate_capture_corners(setup,tasks,{},snapshot(),schedule)['ok'])
        self.assertFalse(validate_capture_corners(setup,tasks[:1],{},snapshot(),schedule)['ok'])

    def test_numeric_constant_cannot_turn_into_an_unchecked_string(self):
        current={'name':'T','variables':[{'name':'V','value':'1'}]}
        row=task('T','A',1);row['native_parameters']=[{'name':'V','value':None,'expression':'"1"'}]
        differences,checks=point_variable_checks({},current,{},row,{})
        self.assertFalse(checks[0]['ok']);self.assertTrue(differences)

    def test_reused_headless_capture_can_restore_source_defaults(self):
        from crescendo_engine.corner_selection import capture_selection_plan
        setup=source(); selected=plan(setup,['B'])
        self.assertIsNone(capture_selection_plan(setup,None,None))
        self.assertIs(capture_selection_plan(setup,selected,None),selected)
        restored=capture_selection_plan(setup,None,selected)
        self.assertEqual(restored['selected'],['A'])
        self.assertEqual(restored['metric_requirements'],[])
        self.assertEqual(effective_setup(setup,restored),setup)

    def test_numeric_rdb_expression_cannot_be_bypassed_by_quoted_native_value(self):
        current={'name':'T','variables':[{'name':'V','value':'VAR("BASE")*2'}]}
        row=task('T','A',1);row['native_parameters']=[{'name':'V','value':None,'expression':'"bad"'}]
        differences,checks=point_variable_checks({},current,{},row,{'parameters':[{'pointID':1,'name':'V','value':2}]})
        self.assertFalse(checks[0]['ok']);self.assertTrue(differences)
