"""Large-project regressions without native EDA processes."""
import copy
import json
import os
import random
import tempfile
import time
import unittest
from unittest import mock

from crescendo_engine import gui
from crescendo_engine.gui_performance import ScopeIndex, snapshot_copy, parameter_defaults, json_detail_tail
from crescendo_engine.gui_state import RunUiState, initial_parameter_config
from crescendo_engine import settings_state, unified_configuration
from tests import test_gui_light_loading as light


class SnapshotTests(unittest.TestCase):
    def test_types_aliases_cycles_and_independent_mutations(self):
        shared = [{'value': 2.5, 'unicode': '\u7535\u8def', 'bytes': b'bytes', 'bool': True}]
        source = {'one': shared, 'two': shared, 'tuple': (2, ['a']), 'set': {1, 2}, 'frozen': frozenset([3])}
        result = snapshot_copy(source)
        self.assertEqual(result, copy.deepcopy(source))
        self.assertIs(result['one'], result['two'])
        self.assertIsNot(result['one'], shared)
        result['tuple'][1].append('b')
        self.assertEqual(source['tuple'][1], ['a'])
        recursive = []; recursive.append(recursive)
        cloned = snapshot_copy(recursive)
        self.assertIs(cloned[0], cloned)
        self.assertIsNot(cloned, recursive)

    def test_custom_copy_protocol_and_buffer_types_fall_back(self):
        class Special(dict):
            def __deepcopy__(self, memo): return {'copied': True}
        for value in [bytearray(b'abc'), {'custom': Special(a=1)}, {'bytes': bytearray(b'abc')}]:
            actual = snapshot_copy(value)
            self.assertEqual(actual, copy.deepcopy(value))
            self.assertIs(type(actual), type(copy.deepcopy(value)))
        self.assertIsInstance(snapshot_copy({'bytes': bytearray(b'x')})['bytes'], bytearray)

    def test_nonfinite_atomic_keys_keep_deepcopy_lookup_semantics(self):
        key = float('nan')
        source = {key: [key], 'complex': complex(float('nan'), 1)}
        result = snapshot_copy(source)
        self.assertIs(result[key][0], key)
        self.assertIs(result['complex'], source['complex'])
        member = snapshot_copy({key})
        self.assertIn(key, member)

    def test_default_config_memo_is_equivalent_and_rows_do_not_alias(self):
        rows = []
        for i, changes in enumerate([{}, {'current_value': None}, {'default_value': '2u'},
                {'value_type': 'integer', 'numeric_value': 3}, {'choices': ['1u', '3u']},
                {'choices_raw': '1 2 3'}, {'minimum': '0.5', 'maximum': '7'},
                {'numeric_value': -2}, {'numeric_value': 0}, {'semantic': 'other', 'default_selected': True}] * 3):
            row = {'parameter_id': str(i), 'numeric_value': 2e-6, 'semantic': 'width'}
            row.update(changes); rows.append(row)
        actual = parameter_defaults(rows, initial_parameter_config)
        self.assertEqual(actual, {row['parameter_id']: initial_parameter_config(row) for row in rows})
        actual['0']['selected'] = False
        self.assertTrue(actual['10']['selected'])

    def test_detail_suffix_is_exact_for_large_nested_unicode_and_short_reports(self):
        for count in (0, 2, 130, 500, 10000):
            document = {"state": "completed", "earlier": "x" * 4000,
                        "results": [{"id": i, "text": "\u7535\u8def {x}\\quote", "data": [None, True, 1e-9]} for i in range(count)]}
            for limit in (64, 4096, 65536):
                self.assertEqual(json_detail_tail(document, limit), json.dumps(document, indent=2, ensure_ascii=False)[-limit:])
        document = {"results": [1] * 5000, "last": "tail"}
        self.assertEqual(json_detail_tail(document), json.dumps(document, indent=2, ensure_ascii=False)[-65536:])

    def test_validation_work_freezes_edits_and_leaves_local_api_synchronous(self):
        app = gui.OptimizerGui.__new__(gui.OptimizerGui)
        app.parameter_by_id = {'p': {'parameter_id': 'p', 'name': 'w'}}
        app.parameter_configs = {'p': {'selected': True, 'minimum': '1', 'maximum': '2'}}
        work = app._range_report_work()
        expected = app._local_range_report()
        app.parameter_configs['p']['minimum'] = 'bad'
        self.assertEqual(work(), expected)
        self.assertTrue(app._local_range_report()['issues'])


@unittest.skipUnless(os.environ.get('DISPLAY'), 'Existing DISPLAY required')
class SnapshotPerformanceTkTests(unittest.TestCase):
    setUp = light.GuiLightLoadingTests.setUp
    _close_gui = light.GuiLightLoadingTests._close_gui
    _open = light.GuiLightLoadingTests._open
    _wait = light.GuiLightLoadingTests._wait
    _pump = light.GuiLightLoadingTests._pump
    _load = light.GuiLightLoadingTests._load

    def prepared(self, count=100):
        app = self._open(parameter_count=count)
        self._load(); app.show_page('Parameters')
        self._wait(lambda: not app.parameter_tree_dirty)
        return app

    def test_snapshot_result_matches_deepcopy_and_is_independent(self):
        app = self.prepared(10)
        expected = None
        with mock.patch.object(gui, 'snapshot_copy', copy.deepcopy): expected = app._snapshot_ui_state()
        actual = app._snapshot_ui_state()
        self.assertEqual(actual, expected)
        actual[0]['parameter_configs']['/M0/w']['minimum'] = 'changed'
        actual[1]['parameter_by_id']['/M0/w']['name'] = 'changed'
        self.assertNotEqual(app.parameter_configs['/M0/w']['minimum'], 'changed')
        self.assertEqual(app.parameter_by_id['/M0/w']['name'], 'w')

class LargeConfigurationTests(unittest.TestCase):
    def test_over_16_mib_roundtrip_keeps_all_identity_and_raw_edits(self):
        rows, configs = {}, {}
        for i in range(51000):
            key = '/module{}/M/w'.format(i)
            rows[key] = {'parameter_id': key, 'name': 'w', 'hierarchy_path': '/module{}/M'.format(i),
                         'design_library': 'design', 'design_cell': 'TB', 'design_view': 'schematic',
                         'master_library': 'device', 'master_cell': 'nmos', 'value_type': 'float',
                         'owner_library': 'design', 'owner_cell': 'module', 'owner_view': 'schematic'}
            configs[key] = {'selected': i%2 == 0, 'minimum': 'unfinished '+str(i), 'maximum': '', 'unit':'u', 'scale':'log'}
        raw = {'parameter_configs': configs, 'setup': {'path':'/design/TB/maestro/maestro.sdb'}}
        document = unified_configuration.build_configuration(raw, rows, [], [])
        size = len(settings_state._encoded(document))
        self.assertGreater(size, 16*1024*1024)
        with tempfile.TemporaryDirectory() as folder:
            path = os.path.join(folder, 'large.json')
            unified_configuration.save_configuration(path, document)
            restored = unified_configuration.read_configuration(path)
        expected = document['settings']['parameters']
        self.assertEqual(restored['settings']['parameters'], expected)
        self.assertEqual(len(expected), 51000)
        print("CONFIGURATION_ROUNDTRIP count=51000 bytes={}".format(size))
        self.assertTrue(all(row['config']['minimum'].startswith('unfinished ') for row in expected))
