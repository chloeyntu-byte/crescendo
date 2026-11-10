import unittest
from crescendo_engine.private_netlist import validate_capture_corners
from crescendo_engine.native_identity import point_variable_checks
from test_native_corner_identity import task, snapshot
import test_native_rdb_local_identity as native_rdb_fixture


class NativeStringExpressionTests(unittest.TestCase):
    def qualify(self, expression, emitted='"historical file.any"', native_value=None):
        setup={'tests':[{'name':'A','enabled':True}], 'corners':[{'name':'SS','enabled':True,
            'variables':[{'name':'FILE','value':expression}]}]}
        row=task('A','SS',1)
        row['native_parameters']=[{'name':'FILE','value':native_value,'expression':emitted}]
        schedule={'read_ok':True,'groups':{'SS':{'enabled':True,'count':1,'disabled_tests':[]}},
                  'nominal_enabled':False,'nominal_tests':{'A':False}}
        return validate_capture_corners(setup,[row],{},snapshot(),schedule)

    def test_emitted_scalar_type_not_function_name_or_file_existence_proves_string(self):
        for expression in ['sprintf(nil "%s" "historical file.any")', 'FILE_ALIAS', 'siteStringFunction(mode)']:
            with self.subTest(expression=expression):
                result=self.qualify(expression)
                self.assertTrue(result['ok'],result)
                opaque=result['test_differences'][0]['corner_expansion']['contracts']['SS']['opaque_variables']
                self.assertEqual(opaque[0]['evidence'],'native_emitted_scalar_string')

    def test_native_escape_syntax_need_not_be_json_string_syntax(self):
        self.assertTrue(self.qualify('siteStringFunction(mode)', '"directory\\q\\file.any"')['ok'])

    def test_numeric_literals_and_resolved_formulas_cannot_be_reclassified_by_quotes(self):
        for expression in ['4','2*2']:
            with self.subTest(expression=expression):
                self.assertFalse(self.qualify(expression, '"4"')['ok'])
                row=task('A','SS',1);row['native_parameters']=[{'name':'FILE','value':None,'expression':'"4"'}]
                differences,unused=point_variable_checks({}, {'name':'A','variables':[{'name':'FILE','value':expression}]}, {},row,{})
                self.assertTrue(differences)
        row=task('A','SS',1);row['native_parameters']=[{'name':'FILE','value':None,'expression':'"4"'},{'name':'BASE','value':2,'expression':'2'}]
        differences,unused=point_variable_checks({}, {'name':'A','variables':[{'name':'FILE','value':'BASE*2'}]}, {},row,{})
        self.assertTrue(differences)

    def test_numeric_native_rdb_value_still_rejects_quoted_capture(self):
        fixture=native_rdb_fixture.NativeRdbLocalIdentityTests();fixture.setUp();self.addCleanup(fixture.doCleanups)
        differences,checks=fixture.check(expected='siteStringFunction(mode)',value=None,expression='"not numeric"')
        self.assertTrue(differences)
        self.assertEqual(checks[0]['rdb_value'],0)

    def test_absent_or_multiple_literal_native_value_is_not_scalar_type_evidence(self):
        for actual in ['', '"a" "b"', 'siteStringFunction(mode)']:
            with self.subTest(actual=actual):
                self.assertFalse(self.qualify('siteStringFunction(mode)',actual)['ok'])

    def test_expanded_numeric_grid_keeps_count_and_ambiguity_guards(self):
        setup={'tests':[{'name':'A','enabled':True}], 'corners':[]}
        schedule={'read_ok':True,'groups':{},'nominal_enabled':False,'nominal_tests':{'A':False}}
        rows=[]
        for name in ['SS','FF']:
            setup['corners'].append({'name':name,'enabled':True,'variables':[
                {'name':'V','value':'1 2'},{'name':'FILE','value':'siteStringFunction(mode)'}]})
            schedule['groups'][name]={'enabled':True,'count':2,'disabled_tests':[]}
            for value in [1,2]:
                row=task('A','native_'+str(len(rows)),len(rows)+1)
                row['native_parameters']=[{'name':'V','value':value,'expression':str(value)},
                    {'name':'FILE','value':None,'expression':'"historical file.any"'}]
                rows.append(row)
        self.assertFalse(validate_capture_corners(setup,rows,{},snapshot(),schedule)['ok'])
        setup['corners']=setup['corners'][:1];schedule['groups'].pop('FF')
        self.assertTrue(validate_capture_corners(setup,rows[:2],{},snapshot(),schedule)['ok'])
        schedule['groups']['SS']['count']=3
        self.assertFalse(validate_capture_corners(setup,rows[:2],{},snapshot(),schedule)['ok'])
