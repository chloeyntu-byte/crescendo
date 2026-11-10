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


class ScopeEquivalenceTests(unittest.TestCase):
    @staticmethod
    def legacy(scopes, identity, path):
        for scope in scopes:
            owner = scope.get('design') or {}
            if all(owner.get(key) == identity.get(key) for key in ('library', 'cell', 'view')):
                prefix = scope.get('hierarchy_path')
                if prefix == '/' or path == prefix or path.startswith(prefix.rstrip('/') + '/'):
                    return True
        return False

    def test_slashes_empty_relative_unicode_and_unordered_scopes(self):
        identities = [{}, {'library': 'L', 'cell': 'C', 'view': 'schematic'},
                      {'library': 'L', 'cell': 'C', 'view': 'config'}]
        paths = ['', '/', '//', '///', '/A', '/A/', '/A//', '/A/B', '/AB',
                 '/A//B', 'A', 'A/', 'A/B', 'A//B', '/\u7535\u8def', '/\u7535\u8def/M']
        randomizer = random.Random(85)
        for prefix in paths:
            for owner in identities:
                scopes = [{'design': owner, 'hierarchy_path': prefix}]
                index = ScopeIndex(scopes)
                for identity in identities:
                    for path in paths:
                        self.assertEqual(index.contains(identity, path), self.legacy(scopes, identity, path),
                                         (scopes, identity, path))
        for _ in range(100):
            scopes = [{'design': randomizer.choice(identities), 'hierarchy_path': randomizer.choice(paths)}
                      for _ in range(randomizer.randrange(40))]
            randomizer.shuffle(scopes)
            index = ScopeIndex(scopes)
            for identity in identities:
                for path in paths:
                    self.assertEqual(index.contains(identity, path), self.legacy(scopes, identity, path))

    def test_forest_is_byte_identical_to_legacy_scope_predicate(self):
        identity = {'library': 'L', 'cell': 'C', 'view': 'schematic'}
        scopes = [{'design': identity, 'hierarchy_path': path} for path in ['/A/', '/B//', '/other']]
        instances = [{'hierarchy_path': p, 'parent_path': '/', 'kind': 'device', 'name': p, 'depth': 1}
                     for p in ['/A', '/A/M', '/B', '/B//M', '/outside']]
        prepared = {'cell_selection': {'scopes': scopes}, 'hierarchy': {'designs': [
            {'design': identity, 'hierarchy': {'cells': [{'hierarchy_path': '/', 'library': 'L', 'cell': 'C'}],
                                             'instances': instances}}]}}
        app = gui.OptimizerGui.__new__(gui.OptimizerGui)
        actual = app._build_parameter_forest(prepared=prepared, catalog={})
        legacy = self.legacy
        class LegacyIndex(object):
            def __init__(self, scopes): self.scopes = scopes
            def contains(self, identity, path): return legacy(self.scopes, identity, path)
        with mock.patch.object(gui, 'ScopeIndex', LegacyIndex):
            expected = app._build_parameter_forest(prepared=prepared, catalog={})
        encode = lambda v: json.dumps(v, sort_keys=True, ensure_ascii=False).encode('utf-8')
        self.assertEqual(encode(actual), encode(expected))


@unittest.skipUnless(os.environ.get('DISPLAY'), 'Existing DISPLAY required')
class LargeProjectTkTests(unittest.TestCase):
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

    def test_fast_tree_insert_matches_stock_ttk_including_numeric_strings(self):
        stock = gui.ttk.Treeview(self.root, columns=('a','b','c','d','e'))
        fast = gui.FastTreeview(self.root, columns=('a','b','c','d','e'))
        for i, values in enumerate([(2, 1.5, True, None, 'x y'),
                ('{a}', r'\backslash', '\u7535\u8def', '', 'a\nb'), ((1,2), [3,4], b'bytes', '-x', '"quote"')]):
            kw = dict(iid=str(i), text='text {x} \\ \u7535\u8def', values=values, tags=('one two', 'tag'), open=bool(i))
            stock.insert('', 'end', **kw); fast.insert('', 'end', **kw)
            self.assertEqual(stock.item(str(i)), fast.item(str(i)))
        self.assertIs(type(fast.item('0', 'values')[0]), str)
        self.root.update_idletasks()

    def test_page_reentry_and_cached_view_restore_do_not_rebuild(self):
        app = self.prepared(300)
        ids = dict(app.parameter_items)
        with mock.patch.object(app.tasks, 'submit', wraps=app.tasks.submit) as submit:
            app.show_page('Objectives & Constraints'); app.show_page('Parameters')
            submit.assert_not_called()
            app.parameter_filter_var.set('')
            self._pump(.22); submit.assert_not_called()
        app.parameter_view_var.set('name'); app._parameter_view_changed()
        self._wait(lambda: not app.parameter_tree_dirty)
        with mock.patch.object(app.tasks, 'submit', wraps=app.tasks.submit) as submit:
            app.parameter_view_var.set('hierarchy'); app._parameter_view_changed()
            self._wait(lambda: app.parameter_selection_job is None)
            submit.assert_not_called()
        self.assertEqual(app.parameter_items, ids)
        self.assertTrue(all(app.parameter_tree.exists(item) for item in ids.values()))

    def test_cached_switch_saves_expansion_before_search_rebuild(self):
        app = self.prepared(100)
        app.parameter_view_var.set('name'); app._parameter_view_changed()
        self._wait(lambda: not app.parameter_tree_dirty)
        group = app.parameter_tree.get_children('')[0]
        app.parameter_tree.item(group, open=True)
        app.parameter_view_var.set('hierarchy'); app._parameter_view_changed()
        self.assertIn(group, app.parameter_open_items_by_view['name'])
        app.parameter_filter_var.set('/M0')
        self._wait(lambda: not app.parameter_tree_dirty)
        app.parameter_view_var.set('name'); app._parameter_view_changed()
        self._wait(lambda: not app.parameter_tree_dirty)
        self.assertTrue(app.parameter_tree.item(app.parameter_tree.get_children('')[0], 'open'))

    def test_search_debounces_and_latest_query_keeps_focus_semantics(self):
        app = self.prepared(300)
        with mock.patch.object(app.tasks, 'submit', wraps=app.tasks.submit) as submit:
            app.parameter_filter_var.set('/M2'); app.parameter_filter_var.set('/M29'); app.parameter_filter_var.set('/M299 ')
            self._pump(.05); submit.assert_not_called()
            self._wait(lambda: not app.parameter_tree_dirty and set(app.parameter_items) == {'/M299/w'})
            self.assertEqual(submit.call_count, 1)
        app.focus_parameter('/M0/w')
        self._wait(lambda: not app.parameter_tree_dirty and app.pending_parameter_focus is None)
        self.assertEqual(app.parameter_tree.focus(), app.parameter_items['/M0/w'])
        self.assertEqual(len(app.parameter_items), 300)

    def test_unchanged_selection_writes_no_rows_single_edit_updates_leaf_and_ancestors(self):
        app = self.prepared(300)
        self._wait(lambda: app.parameter_selection_job is None)
        with mock.patch.object(app.parameter_tree, 'item', wraps=app.parameter_tree.item) as item:
            app._refresh_parameter_selection_display()
            self._wait(lambda: app.parameter_selection_job is None)
            self.assertEqual([c for c in item.call_args_list if c[1]], [])
            app.parameter_configs['/M0/w']['selected'] = False
            app._refresh_parameter_selection_display()
            self._wait(lambda: app.parameter_selection_job is None)
            writes = [c for c in item.call_args_list if c[1]]
            self.assertEqual(len(writes), 2)  # one leaf and the flat catalogue root
        self.assertTrue(app.parameter_tree.item(app.parameter_items['/M0/w'], 'text').startswith(gui.UNCHECKED_MARK))

    def test_invalid_view_normalizes_without_duplicate_cached_row_ids(self):
        app = self.prepared(100)
        app.parameter_view_var.set('unknown')
        app.render_parameter_tree()
        self._wait(lambda: not app.parameter_tree_dirty)
        self.assertEqual(app.parameter_view_var.get(), 'hierarchy')
        app.render_parameter_tree()
        self.assertEqual(len(app.parameter_items), 100)
        self.assertEqual(self.callback_errors, [])

    def test_hidden_edit_refreshes_cached_values_on_reentry(self):
        app = self.prepared(100)
        app.show_page('Objectives & Constraints')
        app.parameter_configs['/M0/w']['minimum'] = 'edited'
        app.render_parameter_tree()
        app.show_page('Parameters')
        self._wait(lambda: app.parameter_selection_job is None)
        self.assertEqual(app.parameter_tree.set(app.parameter_items['/M0/w'], 'minimum'), 'edited')

    def test_selection_refresh_tolerates_rows_arriving_from_render(self):
        app = self.prepared(2000)
        tick = [0.0]
        def clock():
            tick[0] += .001
            return tick[0]
        with mock.patch.object(gui.time, 'monotonic', side_effect=clock):
            app._refresh_parameter_selection_display()
            self.assertIsNotNone(app.parameter_selection_job)
        # Simulate insert_chunk adding a correctly rendered row between slices.
        parameter = dict(app.parameter_by_id['/M0/w'], parameter_id='late', name='late')
        app.parameter_by_id['late'] = parameter
        app.parameter_configs['late'] = dict(app.parameter_configs['/M0/w'])
        app.parameter_items['late'] = app.parameter_tree.insert('', 'end', iid='late', text='late')
        app.parameter_item_labels['late'] = 'late'
        self._wait(lambda: app.parameter_selection_job is None)
        self.assertEqual(self.callback_errors, [])

    def test_unchanged_scan_still_obeys_the_selection_time_slice(self):
        app = self.prepared(2000)
        self._wait(lambda: app.parameter_selection_job is None)
        tick = [0.0]
        def clock():
            tick[0] += .001
            return tick[0]
        with mock.patch.object(gui.time, 'monotonic', side_effect=clock), \
                mock.patch.object(app.parameter_tree, 'item', wraps=app.parameter_tree.item) as item:
            app._refresh_parameter_selection_display()
            self.assertIsNotNone(app.parameter_selection_job)
            self.assertEqual([c for c in item.call_args_list if c[1]], [])
        self._wait(lambda: app.parameter_selection_job is None)
