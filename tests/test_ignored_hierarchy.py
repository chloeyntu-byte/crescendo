"""Saved schematic exclusion is distinct from a model/symbol binding."""
import copy
import unittest
from crescendo_engine.hierarchy import normalize_probe, probe_skill, validate_native_hierarchy
from crescendo_engine.runner import parameter_catalog
from test_config_hierarchy import record, parameter


class IgnoredHierarchyTests(unittest.TestCase):
    design = dict(library='USER', cell='TB', view='schematic')

    def records(self, ignored=True, reason='instance nlAction=ignore'):
        return [record('CELL', '/', 'USER', 'TB', 'schematic', 0),
                record('INSTANCE', '/OFF', 'OFF', 'USER', 'BLOCK', 'symbol', False,
                       '/', 0, 'schematicSymbol', ignored, reason),
                parameter('/OFF', ('USER', 'TB')),
                record('INSTANCE', '/ON', 'ON', 'PDK', 'N', 'symbol', False,
                       '/', 0, 'schematicSymbol', False, None),
                parameter('/ON', ('USER', 'TB'))]

    def test_plain_schematic_exclusion_is_readonly_metadata_not_an_optimizable_device(self):
        body = normalize_probe(self.records(), self.design)
        validate_native_hierarchy(body, self.design)
        instance = body['instances'][0]
        self.assertTrue(instance['netlister_ignored'])
        self.assertEqual(instance['ignore_reason'], 'instance nlAction=ignore')
        parameters = {row['hierarchy_path']: row for row in body['parameters']}
        self.assertFalse(parameters['/OFF']['tunable'])
        self.assertTrue(parameters['/ON']['tunable'])
        catalog = parameter_catalog({'tests': []}, {'designs': [{'design': self.design, 'hierarchy': body}]})
        by_path = {row['hierarchy_path']: row for row in catalog}
        self.assertFalse(by_path['/OFF']['optimizable'])
        self.assertTrue(by_path['/ON']['optimizable'])
        self.assertEqual(body['counts']['netlister_ignored_instances'], 1)

    def test_master_exclusion_and_marker_reason_are_preserved(self):
        for reason in ('master nlAction=ignore', 'schematic port marker', 'schematic ground marker'):
            body = normalize_probe(self.records(reason=reason), self.design)
            self.assertEqual(body['parameters'][0]['parameter_policy_reason'], reason)
            self.assertFalse(body['parameters'][0]['tunable'])

    def test_ignored_expression_never_becomes_numeric_candidate(self):
        records = self.records()
        records[2]['values'][4] = 'FN_width'
        row = normalize_probe(records, self.design)['parameters'][0]
        self.assertFalse(row['tunable'])
        self.assertFalse(row['candidate_expression'])
        self.assertFalse(row['requires_point_numeric_value'])

    def test_expanding_an_ignored_block_is_rejected(self):
        body = normalize_probe(self.records(), self.design)
        body['cells'].append(dict(hierarchy_path='/OFF', library='USER', cell='BLOCK', view='schematic'))
        with self.assertRaisesRegex(RuntimeError, 'netlister-ignored instance was unexpectedly expanded at /OFF'):
            validate_native_hierarchy(body, self.design)

    def test_exclusion_does_not_hide_a_genuine_other_cell_failure(self):
        body = normalize_probe(self.records() + [record('ERROR', '/ON/CHILD', 'USER', 'MISSING', 'schematic')], self.design)
        with self.assertRaisesRegex(RuntimeError, 'cell could not be opened at /ON/CHILD'):
            validate_native_hierarchy(body, self.design)

    def test_legacy_instance_record_without_exclusion_evidence_remains_compatible(self):
        records = self.records()
        records[1]['values'] = records[1]['values'][:9]
        body = normalize_probe(records, self.design)
        self.assertNotIn('netlister_ignored', body['instances'][0])
        self.assertTrue(body['parameters'][0]['tunable'])

    def test_probe_skips_ignored_child_and_cdf_in_both_scan_modes(self):
        skill = probe_skill('USER', 'TB', 'schematic', '/private/records', 32)
        self.assertIn('childExists = !ignored && crescProbeMasterSchematic', skill)
        self.assertIn('when(!ignored && crescProbeParameterScope(instPath)', skill)
        self.assertIn('when(!ignored && !crescCellsOnly && crescScoped', skill)
        self.assertIn('master~>cellViewType ignored ignoreReason', skill)
        self.assertIn('PCDB=%d OA=%d ignored=%L', skill)
        self.assertIn('PCDB=%L OA=%L nlAction=%L', skill)
        self.assertNotIn('ignored = nth(6 flags)', skill)


if __name__ == '__main__':
    unittest.main()
