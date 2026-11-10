"""Real Tk identity rendering and redraw regression checks."""
import copy
import os
import tempfile
import unittest

from crescendo_engine import hierarchy
from tests import test_gui_cell_selection as cell_fixture
from tests import test_staged_hierarchy as hierarchy_fixture


@unittest.skipUnless(os.environ.get("DISPLAY"), "Real Tk tests require DISPLAY")
class HierarchyIdentityTkTests(unittest.TestCase):
    setUp = cell_fixture.GuiCellSelectionTests.setUp
    _close = cell_fixture.GuiCellSelectionTests._close
    _wait = cell_fixture.GuiCellSelectionTests._wait
    _open = cell_fixture.GuiCellSelectionTests._open
    _load_cells = cell_fixture.GuiCellSelectionTests._load_cells
    _apply = cell_fixture.GuiCellSelectionTests._apply
    _tree_items = staticmethod(cell_fixture.GuiCellSelectionTests._tree_items)

    def test_distinct_view_bindings_remain_distinguishable_and_selectable_after_redraw(self):
        app = self._open()
        original = cell_fixture.FakeStagedService._document
        def document(path):
            preview = original(path)
            base = preview['cell_scopes'][:2]
            rows = []
            for view in ('schematic', 'config', 'config_alt'):
                for source in base:
                    row = copy.deepcopy(source)
                    row['design']['view'] = view
                    row['scope_id'] += ':' + view
                    rows.append(row)
            preview['cell_scopes'] = rows
            preview['setup']['tests'] = [dict(name='VCO', library='lib_A', cell='TB', view='schematic')]
            return preview
        self.service._document = document
        self.context['live_tests'] = []
        self.service.context['live_tests'] = []
        app.active_launch_context['live_tests'] = []
        self._load_cells()
        roots = app.cell_scope_tree.get_children('')
        self.assertEqual(len(roots), 3)
        labels = [app.cell_scope_tree.item(item, 'text') for item in roots]
        self.assertTrue(all(any('[' + view + ']' in label for label in labels)
                            for view in ('schematic', 'config', 'config_alt')))
        selected = 'lib_A:/:config'
        app._toggle_cell_scope(selected)
        self.assertIn('[config]', app.cell_scope_tree.item(selected, 'text'))
        for _ in range(3):
            app._render_cell_scopes()
        self._wait(lambda: app.cell_render_job is None)
        self.assertEqual(len(self._tree_items(app.cell_scope_tree)), 6)
        self.assertEqual(app.selected_cell_scope_ids, {selected})
        self._apply()
        self.assertEqual(self.service.prepare_cells_calls[-1][1], [selected])


class SameDesignProbeTests(unittest.TestCase):
    def test_multiple_tests_same_design_probe_once_and_keep_distinct_view(self):
        fixture = hierarchy_fixture.StagedHierarchyTests()
        with tempfile.TemporaryDirectory() as root, fixture.fake_probe(root) as (setup, config, launch, _display):
            setup['tests'].append(dict(setup['tests'][0], name='B'))
            result = hierarchy.probe_hierarchy(setup, config, cells_only=True)
            self.assertEqual(len(result['designs']), 1)
            self.assertEqual(launch.call_count, 1)
            scope_rows = hierarchy.cell_scope_records(result)
            self.assertEqual(len(scope_rows), 2)
        # Distinct view identities produce independent scopes even with identical cells.
        first = result['designs'][0]
        second = copy.deepcopy(first)
        second['design']['view'] = 'config'
        rows = hierarchy.cell_scope_records({'designs': [first, second]})
        self.assertEqual(len(rows), 4)
        self.assertEqual(len(set(row['scope_id'] for row in rows)), 4)
