import copy
import itertools
import os
import tempfile
import unittest

from crescendo_engine.ade_setup import inspect_setup
from crescendo_engine.hierarchy import parse_physical_number
from crescendo_engine.private_netlist import (
    _capture_setup_identity, native_netlist_identity, top_level_native_parameters,
    validate_capture_corners, PrivateNetlistUnsupported, native_capture_point_coverage,
)
from crescendo_engine.spectre_values import parse_spectre_number
from crescendo_engine.utils import sha256_file


def falcon_fixture():
    # Explicit reconstruction of the observed dimensions, not a claim that
    # the omitted target-machine global XML has already been obtained.
    test = {'name': 'a', 'enabled': True, 'variables': [
        {'name': 'Fref', 'value': '32M'}, {'name': 'N', 'value': '162'},
        {'name': 'finger1_1', 'value': '4'}, {'name': 'finger2_1', 'value': '4'},
        {'name': 'ldo_trim', 'value': '10'}]}
    current = {'tests': [test], 'global_variables': [
        {'name': 'Fref', 'value': '64M', 'dependent_tests': [{'test': 'a', 'enabled': True}]},
        {'name': 'N', 'value': '62 80 126 140', 'dependent_tests': [{'test': 'a', 'enabled': True}]},
        {'name': 'finger1_1', 'value': '6', 'dependent_tests': [{'test': 'a', 'enabled': True}]},
        {'name': 'finger2_1', 'value': '2', 'dependent_tests': [{'test': 'a', 'enabled': True}]}],
        'corners': [
            {'name': 'TT', 'enabled': True, 'variables': [{'name': 'temperature', 'value': '55'}], 'model_groups': ['TT']},
            {'name': 'PVT', 'enabled': True, 'variables': [{'name': 'temperature', 'value': '-40 125'},
                                                         {'name': 'ldo_trim', 'value': '5 15'}], 'model_groups': ['FF', 'SS']}],
        'model_groups': [{'name': name, 'models': [{'file': '/pdk/models.scs', 'section': name.lower(), 'test': 'All'}]}
                         for name in ('TT', 'FF', 'SS')]}
    schedule = {'read_ok': True, 'groups': {'TT': {'enabled': True, 'count': 1}, 'PVT': {'enabled': True, 'count': 4}},
                'nominal_enabled': False, 'nominal_tests': {'a': False}}
    tasks, params = [], []
    combinations = list(itertools.product(('ff', 'ss'), (5, 15), (-40, 125)))
    for sweep, n in enumerate((62, 80, 126, 140)):
        for index, (section, trim, temperature) in enumerate(combinations + [('tt', 10, 55)]):
            point = 9 * sweep + index + 1
            name = 'opaque_corner_%d' % index if index < 8 else 'TT'
            values = {'Fref': 64e6, 'N': n, 'finger1_1': 6, 'finger2_1': 2, 'ldo_trim': trim}
            tasks.append({'test': 'a', 'corner': name, 'point_id': point, 'netlist_path': '/owned/%s/input.scs' % point,
                          'native_parameters': [{'name': key, 'value': value, 'expression': str(value)} for key, value in values.items()],
                          'temperature': {'value': temperature, 'expression': str(temperature), 'source': 'simulator_options'},
                          'native_models': [{'file': '/pdk/models.scs', 'section': section}]})
            params.extend({'pointID': point, 'name': key, 'value': value} for key, value in values.items())
    snapshot = {'ok': True, 'saved_source': {'corner_read_ok': True}, 'private_setup': {'corner_read_ok': True}}
    return current, tasks, {'parameters': params}, snapshot, schedule


class EffectiveNativeIdentityTests(unittest.TestCase):
    def test_intentional_capture_exit_requires_every_native_test_point(self):
        parsed = {'path': '/owned/Interactive.rdb', 'test_statuses': [
            {'pointID': 1, 'test_name': 'a'}, {'pointID': 2, 'test_name': 'a'},
            {'pointID': 1, 'test_name': 'disabled'}],
            'points': [{'pointID': 1, 'corner_name': 'Flow', 'designPointNumber': 1},
                       {'pointID': 2, 'corner_name': 'Fhigh', 'designPointNumber': 1}]}
        tasks = [{'point_id': 1, 'test': 'a'}]
        failed = native_capture_point_coverage(tasks, parsed, ['a'])
        self.assertFalse(failed['ok'])
        self.assertEqual((failed['captured_test_point_count'], failed['expected_test_point_count']), (1, 2))
        self.assertEqual(failed['missing_test_points'], [{'point_id': 2, 'test': 'a', 'corner': 'Fhigh', 'design_point': 1}])
        tasks.append({'point_id': 2, 'test': 'a'})
        self.assertTrue(native_capture_point_coverage(tasks, parsed, ['a'])['ok'])
        tasks.append({'point_id': 2, 'test': 'a'})
        self.assertFalse(native_capture_point_coverage(tasks, parsed, ['a'])['ok'])

    def test_spectre_case_and_formula_preserve_spice_parser(self):
        self.assertEqual(parse_spectre_number('32M'), 32e6)
        self.assertEqual(parse_spectre_number('32m'), .032)
        self.assertEqual(parse_physical_number('32M'), .032)
        values = dict((row['name'], row['value']) for row in top_level_native_parameters('parameters Fref=64M N=62 fre=Fref*N\n'))
        self.assertEqual(values['fre'], 64e6 * 62)

    def test_options_temperature_and_models_exclude_subcircuit_locals(self):
        identity = native_netlist_identity('''simulator lang=spectre
include "/pdk/models.scs" section=ff
subckt nested n
wrong options temp=99
include "/local.scs" section=ss
ends nested
simulatorOptions options reltol=1e-3 temp=-40 tnom=27
''')
        self.assertEqual(identity['temperature']['value'], -40)
        self.assertEqual(identity['native_models'], [{'file': '/pdk/models.scs', 'section': 'ff'}])

    def test_model_temperature_trim_and_global_design_sweep_cover_36_points(self):
        data = falcon_fixture()
        result = validate_capture_corners(*data)
        self.assertTrue(result['ok'], result)
        detail = result['test_differences'][0]
        self.assertEqual(detail['corner_expansion']['counts'], {'TT': 1, 'PVT': 8})
        self.assertEqual(len(detail['corner_expansion']['proofs']), 9)
        self.assertEqual(len(data[1]), 36)
        self.assertTrue(all(row['source_corner'] == ('TT' if row['corner'] == 'TT' else 'PVT') for row in data[1]))

    def test_modern_private_grid_rejects_extra_duplicate_with_distinct_point_id(self):
        data = falcon_fixture()
        data[0]['execution_settings'] = []
        self.assertTrue(validate_capture_corners(*data)['ok'])
        duplicate = copy.deepcopy(data[1][0])
        duplicate['point_id'] = 999
        data[1].append(duplicate)
        result = validate_capture_corners(*data)
        self.assertFalse(result['ok'])
        self.assertTrue(any(row.get('reason') == 'design_sweep_point_multiplicity_mismatch'
                            for row in result['test_differences'][0]['sweep_differences']))

    def test_native_global_temperature_dimension_uses_simulator_options_value(self):
        from crescendo_engine.native_identity import sweep_coverage
        setup = {'global_variables': [{'name': 'temperature', 'value': '0 50'}]}
        test = {'name': 'a', 'variables': []}
        rows = [{'point_id': index + 1, 'corner': 'TT', 'native_parameters': [],
                 'temperature': {'value': value}} for index, value in enumerate((0, 50))]
        self.assertEqual(sweep_coverage(setup, test, {'TT': {}}, rows, {'TT': 'TT'},
                                        strict_multiplicity=True), [])

    def test_modern_private_grid_rejects_large_diagonal_legacy_contract_unchanged(self):
        data = falcon_fixture()
        data[0]['corners'] = [data[0]['corners'][0]]
        data[4]['groups'].pop('PVT')
        expression = ' '.join(str(number) for number in range(317))
        next(row for row in data[0]['global_variables'] if row['name'] == 'N')['value'] = expression
        data[0]['global_variables'].append({'name': 'B', 'value': expression})
        template = copy.deepcopy(data[1][8])
        data[1][:] = []
        data[2]['parameters'] = []
        for number in range(317):
            row = copy.deepcopy(template)
            row['point_id'] = number + 1
            next(item for item in row['native_parameters'] if item['name'] == 'N')['value'] = number
            row['native_parameters'].append({'name': 'B', 'value': number})
            data[1].append(row)
        self.assertTrue(validate_capture_corners(*data)['ok'])
        data[0]['execution_settings'] = []
        result = validate_capture_corners(*data)
        self.assertFalse(result['ok'])
        difference = result['test_differences'][0]['sweep_differences'][0]
        self.assertEqual(difference['missing_combination_count'], 100172)
        self.assertEqual(difference['expected_point_count'], 100489)

    def test_wrong_model_temperature_or_global_value_are_rejected(self):
        for field in ('model', 'temperature', 'global'):
            data = falcon_fixture()
            if field == 'model':
                data[1][0]['native_models'][0]['section'] = 'tt'
            elif field == 'temperature':
                data[1][0]['temperature']['value'] = 27
            else:
                next(row for row in data[1][8]['native_parameters'] if row['name'] == 'Fref')['value'] = 32e6
            result = validate_capture_corners(*data)
            self.assertFalse(result['ok'], field)
            self.assertTrue(all('source_corner' not in row for row in data[1]))

    def test_duplicate_model_combination_or_missing_design_sweep_value_are_rejected(self):
        data = falcon_fixture()
        for row in data[1]:
            if row['corner'] == 'opaque_corner_4':
                row['native_models'][0]['section'] = 'ff'
        self.assertFalse(validate_capture_corners(*data)['ok'])
        data = falcon_fixture()
        data[1][:] = [row for row in data[1] if row['point_id'] <= 27]
        result = validate_capture_corners(*data)
        self.assertFalse(result['ok'])
        self.assertEqual(len(result['test_differences'][0]['sweep_differences']), 9)

    def test_native_rdb_and_netlist_disagreement_is_rejected(self):
        data = falcon_fixture()
        next(row for row in data[2]['parameters'] if row['pointID'] == 1 and row['name'] == 'N')['value'] = 140
        detail = validate_capture_corners(*data)
        self.assertFalse(detail['ok'])
        self.assertEqual(detail['test_differences'][0]['variable_mismatches'][0]['reason'], 'native_rdb_netlist_value_mismatch')

    def test_unrelated_saved_settings_do_not_authorize_wrong_constants_or_missing_sweeps(self):
        for failure in ('global', 'missing_sweep'):
            data = falcon_fixture()
            data[0]['execution_settings'] = [{'tag': 'globaloptions', 'attributes': {}, 'text': '',
                                              'children': [{'tag': 'unrelated', 'attributes': {}, 'text': '1', 'children': []}]}]
            if failure == 'global':
                for row in data[1]:
                    next(item for item in row['native_parameters'] if item['name'] == 'Fref')['value'] = 32e6
                for row in data[2]['parameters']:
                    if row['name'] == 'Fref':row['value'] = 32e6
            else:
                data[1][:] = [row for row in data[1] if row['point_id'] <= 27]
            result = validate_capture_corners(*data)
            self.assertFalse(result['ok'])
            differences = result['test_differences'][0]
            self.assertTrue(differences['execution_plan']['errors'])
            self.assertTrue(differences['variable_mismatches'] if failure == 'global' else differences['sweep_differences'])

    def test_explicit_single_plan_overrides_only_its_saved_variable(self):
        data = falcon_fixture()
        data[0]['execution_settings'] = [{'tag': 'runplans', 'attributes': {}, 'text': '', 'children': [
            {'tag': 'runplan', 'attributes': {'enabled': '1'}, 'text': 'plan', 'children': [
                {'tag': 'vars', 'attributes': {}, 'text': '', 'children': [
                    {'tag': 'var', 'attributes': {}, 'text': 'Fref', 'children': [
                        {'tag': 'value', 'attributes': {}, 'text': '64M', 'children': []}]}]}]}]}]
        result = validate_capture_corners(*data)
        self.assertTrue(result['ok'], result)
        data[1][:] = [row for row in data[1] if row['point_id'] <= 27]
        self.assertFalse(validate_capture_corners(*data)['ok'])

    def test_per_test_global_participation_mask_is_respected(self):
        data = falcon_fixture()
        data[0]['global_variables'][0]['dependent_tests'][0]['enabled'] = False
        result = validate_capture_corners(*data)
        self.assertFalse(result['ok'])
        self.assertEqual(result['test_differences'][0]['variable_mismatches'][0]['expected_value'], 32e6)

    def test_unknown_model_group_cannot_be_replaced_by_a_count_multiplier(self):
        data = falcon_fixture()
        data[0]['model_groups'] = []
        result = validate_capture_corners(*data)
        self.assertFalse(result['ok'])
        self.assertIn('corner_identity_source_unresolved', result['test_differences'][0]['corner_expansion']['errors'])

    def test_saved_global_sweep_model_group_and_run_plan_changes_are_rejected(self):
        original = '''<setupdb><active><currentmode>SRSC</currentmode>
<vars><var>N<value>62 80 126 140</value><dependentTests><dependentTest enabled="1">a</dependentTest></dependentTests></var></vars>
<modelgroups><modelgroup>TT<models><model enabled="1">model<modeltest>All</modeltest><modelfile>/pdk/models.scs</modelfile><modelsection>"tt"</modelsection></model></models></modelgroup></modelgroups>
<runplans><runplan enabled="1">Nominal<vars><var>Fref<value>64M</value></var></vars></runplan></runplans>
<corners><corner enabled="1">TT<modelgroupname>"TT"</modelgroupname></corner></corners>
<tests><test enabled="1">a<tooloptions><option>sim<value>spectre</value></option></tooloptions><vars><var>N<value>162</value></var></vars></test></tests>
</active></setupdb>'''
        with tempfile.TemporaryDirectory() as root:
            source = os.path.join(root, 'maestro.sdb')
            private = os.path.join(root, 'private.sdb')
            with open(source, 'w') as stream:stream.write(original)
            setup = inspect_setup(source)
            workspace = {'setup': setup, 'setup_sdb': private}
            for before, after in [('62 80 126 140', '62 80 126'), ('enabled="1">a', 'enabled="0">a'),
                                  ('"tt"', '"ff"'), ('64M', '32M')]:
                with open(private, 'w') as stream:stream.write(original.replace(before, after))
                with self.assertRaises(PrivateNetlistUnsupported):_capture_setup_identity(workspace, root)
            with open(private, 'w') as stream:stream.write(original)
            self.assertTrue(_capture_setup_identity(workspace, root)['ok'])
            old = copy.deepcopy(setup)
            old.pop('global_variables')
            workspace['setup'] = old
            with self.assertRaises(PrivateNetlistUnsupported):_capture_setup_identity(workspace, root)

    def test_approved_same_content_model_remap_covers_model_groups_globals_and_plan(self):
        with tempfile.TemporaryDirectory() as root:
            source_model, target_model = root + '/original.scs', root + '/relocated.scs'
            for path in (source_model, target_model):
                with open(path, 'w') as stream:stream.write('library models\nsection tt\nendsection tt\nendlibrary models\n')
            source_xml = '''<setupdb><active><currentmode>SRSC</currentmode>
<vars><var>FILE<value>%s</value></var></vars>
<runplans><runplan enabled="1">only<vars><var>FILE<value>%s</value></var></vars></runplan></runplans>
<modelgroups><modelgroup>TT<models><model enabled="1">model<modeltest>All</modeltest><modelfile>%s</modelfile><modelsection>"tt"</modelsection></model></models></modelgroup></modelgroups>
<corners><corner enabled="1">TT<modelgroupname>"TT"</modelgroupname></corner></corners>
<tests><test enabled="1">a<tooloptions><option>sim<value>spectre</value></option></tooloptions></test></tests>
</active></setupdb>''' % (source_model, source_model, source_model)
            source, private = root + '/maestro.sdb', root + '/private.sdb'
            with open(source, 'w') as stream:stream.write(source_xml)
            with open(private, 'w') as stream:stream.write(source_xml.replace(source_model, target_model))
            setup = inspect_setup(source)
            reference = {'file': private, 'changed': True, 'directory': False, 'original': source_model,
                         'resolved': target_model, 'target_identity': {'size': os.path.getsize(target_model),
                          'sha256': sha256_file(target_model)}}
            workspace = {'setup': setup, 'setup_sdb': private, 'model_paths': {'references': [reference]}}
            identity = _capture_setup_identity(workspace, root)
            self.assertTrue(identity['ok'])
            self.assertEqual(identity['verified_model_path_normalizations'][0]['source_identity_status'], 'verified_bytes_equal')
            with open(private, 'w') as stream:stream.write(source_xml.replace(source_model, target_model).replace('"tt"', '"ff"'))
            with self.assertRaises(PrivateNetlistUnsupported):_capture_setup_identity(workspace, root)
            with open(private, 'w') as stream:stream.write(source_xml.replace(source_model, target_model))
            with open(target_model, 'a') as stream:stream.write('// changed\n')
            with self.assertRaises(PrivateNetlistUnsupported):_capture_setup_identity(workspace, root)
            reference['target_identity'] = {'size': os.path.getsize(target_model),
                                            'sha256': sha256_file(target_model)}
            with self.assertRaises(PrivateNetlistUnsupported):_capture_setup_identity(workspace, root)

    def test_relative_model_identity_requires_explicit_caller_and_native_bases(self):
        with tempfile.TemporaryDirectory() as root:
            caller, point = root + '/caller', root + '/caller/netlist'
            os.makedirs(point)
            with open(caller + '/models.scs', 'w') as stream:stream.write('library models\nendlibrary models\n')
            current = {'tests': [{'name': 'a', 'enabled': True}],
                       'corners': [{'name': 'TT', 'enabled': True, 'model_groups': ['TT']}],
                       'model_groups': [{'name': 'TT', 'models': [{'file': 'models.scs', 'section': 'tt', 'test': 'All'}]}]}
            schedule = {'read_ok': True, 'groups': {'TT': {'enabled': True, 'count': 1}},
                        'nominal_enabled': False, 'nominal_tests': {'a': False}}
            snapshot = {'ok': True, 'saved_source': {'corner_read_ok': True}, 'private_setup': {'corner_read_ok': True},
                        'model_path_context': {'source_work_dir': caller, 'source': 'captured_selected_source_work_dir'}}
            rows = [{'test': 'a', 'corner': 'TT', 'point_id': 1, 'native_cwd': point,
                     'native_models': [{'file': '../models.scs', 'section': 'tt'}]}]
            self.assertTrue(validate_capture_corners(current, rows, {}, snapshot, schedule)['ok'])
            unknown = copy.deepcopy(snapshot)
            unknown.pop('model_path_context')
            result = validate_capture_corners(current, rows, {}, unknown, schedule)
            self.assertFalse(result['ok'])
            self.assertIn('relative_model_path_base_unavailable', result['test_differences'][0]['corner_expansion']['errors'])
            rows[0].pop('native_cwd')
            result = validate_capture_corners(current, rows, {}, snapshot, schedule)
            self.assertFalse(result['ok'])
            self.assertIn('native_model_path_base_unavailable', result['test_differences'][0]['corner_expansion']['errors'])

    def test_relative_source_approved_remap_uses_captured_source_identity(self):
        with tempfile.TemporaryDirectory() as root:
            caller = root + '/caller'
            os.mkdir(caller)
            target_model = root + '/relocated.scs'
            for path in (caller + '/models.scs', target_model):
                with open(path, 'w') as stream:stream.write('library models\nsection tt\nendsection tt\nendlibrary models\n')
            xml = '''<setupdb><active><currentmode>SRSC</currentmode>
<modelgroups><modelgroup>TT<models><model enabled="1">model<modeltest>All</modeltest><modelfile>models.scs</modelfile><modelsection>"tt"</modelsection></model></models></modelgroup></modelgroups>
<corners><corner enabled="1">TT<modelgroupname>"TT"</modelgroupname></corner></corners>
<tests><test enabled="1">a<tooloptions><option>sim<value>spectre</value></option></tooloptions></test></tests>
</active></setupdb>'''
            source, private = root + '/maestro.sdb', root + '/private.sdb'
            with open(source, 'w') as stream:stream.write(xml)
            with open(private, 'w') as stream:stream.write(xml.replace('models.scs', target_model))
            setup = inspect_setup(source)
            workspace = {'setup': setup, 'setup_sdb': private, 'root': root, 'model_paths': {'references': [
                {'file': private, 'changed': True, 'directory': False, 'original': 'models.scs', 'resolved': target_model,
                 'target_identity': {'size': os.path.getsize(target_model), 'sha256': sha256_file(target_model)}}]}}
            config = {'_selected_setup_source': {'setup_path': source, 'work_dir': caller}, 'cadence': {'project_dir': caller}}
            snapshot = _capture_setup_identity(workspace, root, config=config)
            self.assertEqual(snapshot['model_path_context']['source_work_dir'], caller)
            self.assertEqual(snapshot['verified_model_path_normalizations'][0]['source_identity_status'], 'verified_bytes_equal')
            schedule = {'read_ok': True, 'groups': {'TT': {'enabled': True, 'count': 1}}, 'nominal_enabled': False, 'nominal_tests': {'a': False}}
            rows = [{'test': 'a', 'corner': 'TT', 'point_id': 1, 'native_cwd': root,
                     'native_models': [{'file': target_model, 'section': 'tt'}]}]
            self.assertTrue(validate_capture_corners(setup, rows, {}, snapshot, schedule)['ok'])
            config['_selected_setup_source']['setup_path'] = root + '/another.sdb'
            unknown = _capture_setup_identity(workspace, root, config=config)
            self.assertEqual(unknown['model_path_context']['source'], 'selected_source_identity_mismatch')
            self.assertFalse(validate_capture_corners(setup, rows, {}, unknown, schedule)['ok'])


if __name__ == '__main__':
    unittest.main()
