"""Mixed schematic/model config coverage, parameter safety and binding identity."""
import copy
import unittest

from crescendo_engine.config_binding import private_config_plan
from crescendo_engine.config_hierarchy import is_schematic_implementation
from crescendo_engine.hierarchy import normalize_probe, probe_skill, validate_native_hierarchy
from crescendo_engine.runner import configuration_binding_identity, parameter_catalog
from tests.test_config_hierarchy import REQUESTED, records, record, binding, parameter


def mixed_records():
    result = records()
    for row in result:
        if row['kind'] == 'CONFIG_BINDING':
            values = row['values']
            if not values[25] and values[24] is None:
                values[24] = 'schematicSymbol'
            values += [bool(not values[15] and not values[25] and values[24] == 'schematic'), False]
    top = ('USER', 'REAL_TOP', 'schematic_alt')
    for name, view, view_type, stop in (
            ('VA', 'veriloga', 'veriloga', False),
            ('VA_STOP', 'veriloga', 'veriloga', True),
            ('MASK', 'symbol', 'schematicSymbol', False),
            ('OTHER', 'extracted_alt', 'maskLayout', False),
            ('UNKNOWN', 'external_model', 'unknown', False),
            ('SPECTRE', 'spectre', 'schematicSymbol', True)):
        path = '/' + name
        target = ('MODEL', 'SHARED', view)
        row = binding(path, name, '/', ('MODEL', 'SHARED', 'symbol'), top, target, target, stop)
        row['values'][24] = view_type
        row['values'] += [False, view == 'symbol']
        result += [row, record('INSTANCE', path, name, 'MODEL', 'SHARED', 'symbol', False, '/', 0,
                              'schematicSymbol'), parameter(path, ('USER', 'REAL_TOP'))]
    return result


def hierarchy():
    return normalize_probe(mixed_records(), REQUESTED)


class ConfigLeafViewTests(unittest.TestCase):
    def test_source_schematic_names_also_require_native_database_type(self):
        for name in ('schematic', 'schematic_abc', 'schematic_FTdiff'):
            self.assertTrue(is_schematic_implementation(name, 'schematic'))
            self.assertFalse(is_schematic_implementation(name, 'veriloga'))
        for name in ('veriloga', 'ext_abc', 'postlayout_any_name'):
            self.assertFalse(is_schematic_implementation(name, 'schematic'))

    def test_extracted_schematic_database_is_opaque_without_changing_stop(self):
        source = mixed_records()
        row = next(item for item in source if item['kind'] == 'CONFIG_BINDING'
                   and item['values'][0] == '/OTHER')
        row['values'][11] = row['values'][14] = 'ext_abc'
        row['values'][24] = 'schematic'
        body = normalize_probe(source, REQUESTED)
        validate_native_hierarchy(body, REQUESTED)
        opaque = next(item for item in body['config_bindings'] if item['hierarchy_path'] == '/OTHER')
        self.assertFalse(opaque['scan_traversable'])
        self.assertFalse(opaque['stop'])
        workspace = {'copied_libraries': {'CFG': '/private/CFG', 'USER': '/private/USER'},
                     'config_designs': [dict(REQUESTED, resolved_top=body['resolved_top'],
                                             config_bindings=body['config_bindings'])]}
        bound = next(item for item in private_config_plan(workspace, [])[0]['bindings']
                     if item['hierarchy_path'] == '/OTHER')
        self.assertEqual(bound['target'], ('MODEL', 'SHARED', 'ext_abc'))
        self.assertFalse(bound['stop'])
        body['cells'].append(dict(hierarchy_path='/OTHER', library='MODEL',
                                  cell='SHARED', view='ext_abc', depth=1))
        with self.assertRaisesRegex(RuntimeError, 'unexpectedly expanded at /OTHER'):
            validate_native_hierarchy(body, REQUESTED)

    def test_plain_custom_schematic_parameters_keep_their_exact_owner_view(self):
        source = [record('DESIGN', 'USER', 'TB', 'schematic_FTdiff'),
                  record('CELL', '/', 'USER', 'TB', 'schematic_FTdiff', 0),
                  record('CELL_VIEW_TYPE', '/', 'schematic'),
                  record('INSTANCE', '/M0', 'M0', 'PDK', 'N', 'symbol', False, '/', 0,
                         'schematicSymbol'), parameter('/M0', ('USER', 'TB'))]
        body = normalize_probe(source)
        validate_native_hierarchy(body, dict(library='USER', cell='TB', view='schematic_FTdiff'))
        self.assertEqual(body['parameters'][0]['owner_view'], 'schematic_FTdiff')
        self.assertEqual(body['parameters'][0]['owner_view_type'], 'schematic')

    def test_mixed_model_views_do_not_require_schematic_internal_cells(self):
        body = hierarchy()
        validate_native_hierarchy(body, REQUESTED)
        self.assertEqual({row['hierarchy_path'] for row in body['cells']}, {'/', '/I1'})
        leaves = {row['hierarchy_path']: row for row in body['instances']}
        for path in ('/VA', '/VA_STOP', '/MASK', '/OTHER', '/UNKNOWN', '/SPECTRE'):
            self.assertFalse(leaves[path]['has_schematic'])
            self.assertFalse(leaves[path]['scan_traversable'])
        self.assertFalse(leaves['/VA']['stop'])
        self.assertTrue(leaves['/VA_STOP']['stop'])

    def test_opaque_leaf_does_not_disable_unrelated_schematic_parameters(self):
        body = hierarchy()
        rows = parameter_catalog({'tests': []}, {'designs': [{'design': REQUESTED, 'hierarchy': body}]})
        by_path = {row['hierarchy_path']: row for row in rows}
        self.assertTrue(by_path['/I1/M0']['optimizable'])
        self.assertTrue(by_path['/VA']['optimizable'])
        self.assertTrue(by_path['/SPECTRE']['optimizable'])
        self.assertFalse(by_path['/MASK']['optimizable'])
        self.assertFalse(by_path['/UNKNOWN']['optimizable'])

    def test_spectre_symbol_database_type_keeps_device_parameters(self):
        row = next(row for row in hierarchy()['parameters'] if row['hierarchy_path'] == '/SPECTRE')
        self.assertEqual(row['resolved_view_type'], 'schematicSymbol')
        self.assertFalse(row['symbol_masked'])
        self.assertTrue(row['tunable'])

    def test_symbol_and_unknown_models_keep_readonly_metadata_if_present(self):
        rows = {row['hierarchy_path']: row for row in hierarchy()['parameters']}
        self.assertFalse(rows['/MASK']['tunable'])
        self.assertFalse(rows['/UNKNOWN']['tunable'])
        self.assertTrue(rows['/MASK']['symbol_masked'])
        self.assertFalse(rows['/MASK']['netlister_ignored'])
        self.assertFalse(rows['/MASK']['stop'])

    def test_same_raw_master_keeps_distinct_actual_implementation_and_stop(self):
        rows = {row['hierarchy_path']: row for row in hierarchy()['instances']}
        for path in ('/VA', '/MASK', '/SPECTRE'):
            self.assertEqual(rows[path]['master_cell'], 'SHARED')
        self.assertEqual(rows['/VA']['resolved_view'], 'veriloga')
        self.assertEqual(rows['/MASK']['resolved_view'], 'symbol')
        self.assertEqual(rows['/SPECTRE']['resolved_view'], 'spectre')

    def test_real_missing_schematic_is_still_incomplete(self):
        body = hierarchy()
        body['cells'] = body['cells'][:1]
        with self.assertRaisesRegex(RuntimeError, 'implementation was not opened at /I1'):
            validate_native_hierarchy(body, REQUESTED)

    def test_forged_schematic_opaque_flag_cannot_hide_missing_cell(self):
        body = hierarchy()
        body['cells'] = body['cells'][:1]
        next(row for row in body['config_bindings'] if row['hierarchy_path'] == '/I1')['scan_traversable'] = False
        with self.assertRaisesRegex(RuntimeError, 'traversal evidence is inconsistent'):
            validate_native_hierarchy(body, REQUESTED)

    def test_opaque_body_cannot_be_synthetically_expanded(self):
        body = hierarchy()
        body['cells'].append(dict(hierarchy_path='/VA', library='MODEL', cell='SHARED', view='veriloga', depth=1))
        with self.assertRaisesRegex(RuntimeError, 'unexpectedly expanded at /VA'):
            validate_native_hierarchy(body, REQUESTED)

    def test_missing_actual_model_file_remains_fatal(self):
        body = normalize_probe(mixed_records() + [record('CONFIG_VIEW_MISSING', '/VA', 'no master file')], REQUESTED)
        with self.assertRaisesRegex(RuntimeError, 'CONFIG_VIEW_MISSING at /VA'):
            validate_native_hierarchy(body, REQUESTED)

    def test_optional_master_metadata_warning_preserves_parameters_and_binding(self):
        before = hierarchy()
        body = normalize_probe(mixed_records() + [record(
            'MASTER_SIMINFO_WARNING', None, 'MODEL', 'SHARED', 'optional metadata unavailable')], REQUESTED)
        validate_native_hierarchy(body, REQUESTED)
        self.assertEqual(body['parameters'], before['parameters'])
        self.assertEqual(body['config_bindings'], before['config_bindings'])
        self.assertEqual(body['errors'][-1]['kind'], 'MASTER_SIMINFO_WARNING')
        wrapped = lambda value: {'designs': [{'design': REQUESTED, 'hierarchy': value}]}
        self.assertEqual(configuration_binding_identity(wrapped(body)), configuration_binding_identity(wrapped(before)))

    def test_type_evidence_cannot_be_omitted_in_new_binding_record(self):
        body = hierarchy()
        next(row for row in body['config_bindings'] if row['hierarchy_path'] == '/VA')['resolved_view_type'] = None
        with self.assertRaisesRegex(RuntimeError, 'resolved view type is missing at /VA'):
            validate_native_hierarchy(body, REQUESTED)

    def test_private_config_preserves_models_and_true_stop_flags(self):
        body = hierarchy()
        workspace = {'copied_libraries': {'CFG': '/private/CFG', 'USER': '/private/USER'},
                     'config_designs': [dict(REQUESTED, resolved_top=body['resolved_top'],
                                             config_bindings=body['config_bindings'])]}
        plan = private_config_plan(workspace, [])
        leaves = {row['hierarchy_path']: row for row in plan[0]['bindings']}
        self.assertEqual(leaves['/VA']['target'], ('MODEL', 'SHARED', 'veriloga'))
        self.assertFalse(leaves['/VA']['stop'])
        self.assertTrue(leaves['/VA_STOP']['stop'])
        self.assertEqual(leaves['/MASK']['target'], ('MODEL', 'SHARED', 'symbol'))
        self.assertFalse(leaves['/MASK']['stop'])

    def test_clone_cannot_target_a_nonstop_model_implementation(self):
        body = hierarchy()
        workspace = {'copied_libraries': {'CFG': '/private/CFG', 'USER': '/private/USER'},
                     'config_designs': [dict(REQUESTED, resolved_top=body['resolved_top'],
                                             config_bindings=body['config_bindings'])]}
        clone = dict(design_library='CFG', design_cell='TB', design_view='config', occurrence_path='/VA')
        with self.assertRaisesRegex(ValueError, 'opaque view'):
            private_config_plan(workspace, [clone])

    def test_model_leaf_cannot_become_parent_of_a_fake_child_binding(self):
        body = hierarchy()
        parent = next(row for row in body['config_bindings'] if row['hierarchy_path'] == '/VA')
        child = dict(parent, hierarchy_path='/VA/FAKE', parent_path='/VA', name='FAKE')
        body['config_bindings'].append(child)
        workspace = {'copied_libraries': {'CFG': '/private/CFG', 'USER': '/private/USER'},
                     'config_designs': [dict(REQUESTED, resolved_top=body['resolved_top'],
                                             config_bindings=body['config_bindings'])]}
        with self.assertRaisesRegex(ValueError, 'missing or stopped ancestor'):
            private_config_plan(workspace, [])

    def test_view_policy_changes_invalidate_the_configuration_identity(self):
        body = hierarchy()
        wrapped = {'designs': [{'design': REQUESTED, 'hierarchy': body}]}
        before = configuration_binding_identity(wrapped)
        for field, value in (('scan_traversable', True), ('symbol_masked', True), ('resolved_view_type', 'schematic')):
            changed = copy.deepcopy(wrapped)
            row = next(row for row in changed['designs'][0]['hierarchy']['config_bindings'] if row['hierarchy_path'] == '/VA')
            row[field] = value
            self.assertNotEqual(before, configuration_binding_identity(changed))

    def test_probe_reads_actual_master_type_and_skips_masked_instance_cdf(self):
        skill = probe_skill('CFG', 'TB', 'config', '/private/records', 32, cells_only=True)
        self.assertIn('ddGetObj(libName cellName viewName "*")', skill)
        self.assertIn('ddMapGetFileViewType(masterFile)', skill)
        self.assertIn('crescConfigViewTypes[key]', skill)
        self.assertIn('crescConfigIsSchematic(resolvedView viewType)', skill)
        self.assertIn('childExists = binding && nth(9 binding)', skill)
        self.assertIn('binding && !nth(10 binding)', skill)
        self.assertNotIn('viewType = if(stop then nil else "schematic")', skill)


if __name__ == '__main__':
    unittest.main()
