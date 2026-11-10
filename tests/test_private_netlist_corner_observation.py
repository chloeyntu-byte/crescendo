import unittest
from crescendo_engine.private_netlist import native_corner_schedule_skill


class CornerObservationTests(unittest.TestCase):
    def test_observation_precedes_unchanged_strict_count_gate(self):
        lines = native_corner_schedule_skill({'tests': []}, '/tmp/corners.tsv')
        text = '\n'.join(lines)
        observation = text.index('CRESCENDO_CORNER_OBS name=%L handle=%L count=%L')
        gate = text.index('integerp(crescCornerCount) && crescCornerCount>0')
        group = text.index('fprintf(crescCornerPort "GROUP')
        self.assertLess(observation, gate)
        self.assertLess(gate, group)

    def test_masks_only_retains_handle_gate_and_observation(self):
        text = '\n'.join(native_corner_schedule_skill({'tests': []}, '/tmp/corners.tsv', masks_only=True))
        self.assertIn('CRESCENDO_CORNER_OBS name=%L handle=%L count=%L', text)
        self.assertIn('unless(crescCornerHandle && crescCornerHandle!=0', text)
        self.assertNotIn('integerp(crescCornerCount)', text)

class ProjectedDefaultSlotTests(unittest.TestCase):
    def setUp(self):
        import copy
        self.slot = {'name': '_default', 'enabled': False, 'variables': [], 'model_groups': [],
                     'native_content': {'tag': 'corner', 'text': '_default', 'attributes': {'enabled': '0'}, 'children': []}}
        self.original = {'corners': [{'name': 'PN_TT', 'enabled': True}]}
        self.source = {'corners': [copy.deepcopy(self.slot)]}
        self.private = {'corners': [{'name': 'PN_TT', 'enabled': True}, copy.deepcopy(self.slot)]}
        self.workspace = {'setup': {'selected_test': 'test'}, '_cr_selected_corners': ['PN_TT']}

    def apply(self):
        from crescendo_engine.private_netlist import _normalize_projected_default_slot
        return _normalize_projected_default_slot(self.original, self.source, self.private, self.workspace)

    def test_identical_saved_disabled_empty_slot_only_removed_from_comparison(self):
        normalized, proof = self.apply()
        self.assertEqual([row['name'] for row in normalized['corners']], ['PN_TT'])
        self.assertEqual(self.private['corners'][1], self.slot)
        self.assertTrue(proof['source_slot_identical'])

    def test_enabled_nonempty_changed_or_duplicate_slot_is_not_ignored(self):
        import copy
        for mutate in ('enabled', 'variable', 'model', 'native_child', 'source_mismatch', 'duplicate'):
            self.setUp()
            if mutate == 'enabled': self.private['corners'][1]['enabled'] = True
            if mutate == 'variable': self.private['corners'][1]['variables'] = [{'name': 'x', 'value': '1'}]
            if mutate == 'model': self.private['corners'][1]['model_groups'] = ['model']
            if mutate == 'native_child': self.private['corners'][1]['native_content']['children'] = [{'tag': 'other'}]
            if mutate == 'source_mismatch': self.source['corners'][0]['native_content']['attributes']['other'] = '1'
            if mutate == 'duplicate': self.private['corners'].append(copy.deepcopy(self.slot))
            normalized, proof = self.apply()
            self.assertIsNone(proof, mutate)
            self.assertEqual(normalized, self.private)

    def test_nominal_or_nonprojected_request_is_not_ignored(self):
        for selected in (['_default'], [], None):
            self.setUp()
            self.workspace['_cr_selected_corners'] = selected
            self.assertIsNone(self.apply()[1])
        self.setUp()
        self.workspace['setup']['selected_test'] = None
        self.assertIsNone(self.apply()[1])
        self.setUp()
        self.original['corners'].append(self.slot)
        self.assertIsNone(self.apply()[1])

    def test_missing_source_slot_and_extra_custom_corner_are_not_ignored(self):
        self.source['corners'] = []
        self.assertIsNone(self.apply()[1])
        self.setUp()
        self.private['corners'].append({'name': 'unexpected', 'enabled': False})
        self.assertIsNone(self.apply()[1])
