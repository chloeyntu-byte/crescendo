import copy
import os
import unittest
from types import SimpleNamespace

from crescendo_engine import gui
from crescendo_engine.gui_execution_selection import ExecutionSelectionPanel
from tests import test_gui_cell_selection as fixtures
FakeStagedService = fixtures.FakeStagedService


class SingleTestService(FakeStagedService):
    def __init__(self, context, count=2):
        super().__init__(context)
        self.count = count
        self.cell_calls = []

    def inspect_tests(self, path, **options):
        preview = self._document(path)
        first = preview['setup']['tests'][0]
        first['analyses'] = [{'name': 'tran', 'enabled': True}, {'name': 'ac', 'enabled': False}]
        if self.count == 2:
            preview['setup']['tests'].append(dict(first, name='OTHER', enabled=False,
                                                   analyses=[{'name': 'dc', 'enabled': True}]))
        preview['tests'] = preview['setup']['tests']
        self.preview = copy.deepcopy(preview)
        return 'catalog', preview

    def inspect_test_cells(self, catalog, name, **options):
        self.cell_calls.append(name)
        preview = copy.deepcopy(self.preview)
        preview['setup']['tests'] = [r for r in preview['setup']['tests'] if r['name'] == name]
        identifier = 'inspection-' + name
        self.inspections[identifier] = copy.deepcopy(preview)
        return identifier, preview


@unittest.skipUnless(os.environ.get('DISPLAY'), 'requires real Tk')
class SingleTestFlowTests(unittest.TestCase):
    setUp = fixtures.GuiCellSelectionTests.setUp
    _close = fixtures.GuiCellSelectionTests._close
    _wait = fixtures.GuiCellSelectionTests._wait
    def _open_count(self, count):
        self.service = SingleTestService(self.context, count)
        self.app = gui.OptimizerGui(self.root, self.service, launch_context=self.context)
        self.root.report_callback_exception = lambda exception, value, trace: self.callback_errors.append((exception, value))
        self._wait(lambda: '/fixture/ocean' in self.app.eda_effective_vars['ocean_bin'].get())
        self.app.setup_path_var.set(self.paths['lib_A'])
        self.app.load_setup()
        self._wait(lambda: self.app.test_inspection_id is not None)

    def test_multiple_tests_wait_for_selection_and_switch_clears_state(self):
        self._open_count(2)
        self.assertEqual(self.service.cell_calls, [])
        self.assertIn('OTHER', self.app.test_combo.cget('values'))
        self.assertFalse(self.app.test_catalog['tests'][1]['enabled'])
        self.assertIsNone(self.app.cell_inspection_id)
        self.app.test_choice_var.set('VCO'); self.app._test_selected()
        self._wait(lambda: self.app.cell_inspection_id == 'inspection-VCO')
        self.assertEqual(self.app.execution_selection_panel.collect()['tests'], ['VCO'])
        self.app.select_all_cells(True); self.app.apply_selected_cells()
        self._wait(lambda: bool(self.app.prepared_id))
        self.assertTrue(self.app.parameter_by_id)
        picker = self.app.objective_rows[0].corner_picker
        picker.invoke(); self.root.update()
        self.assertIsNotNone(picker.popup)
        self.app.test_choice_var.set('OTHER'); self.app._test_selected()
        self.assertIsNone(picker.popup)
        self.assertIsNone(self.app.prepared_id)
        self.assertFalse(self.app.parameter_by_id)
        self.assertFalse(self.app.objective_rows)
        self._wait(lambda: self.app.cell_inspection_id == 'inspection-OTHER')
        self.assertEqual(self.app.execution_selection_panel.collect(), {'tests': ['OTHER'], 'analyses': {'OTHER': ['dc']}})
        self.assertEqual(self.service.cell_calls, ['VCO', 'OTHER'])

    def test_stale_hierarchy_callback_is_ignored(self):
        self._open_count(2)
        pending = []
        original = self.app.tasks.submit
        self.app.tasks.submit = lambda work, done, fail: pending.append((work, done, fail))
        try:
            self.app.test_choice_var.set('VCO'); self.app._test_selected()
            work, done, fail = pending.pop()
            self.app.test_load_epoch += 1
            self.app.selected_test = 'OTHER'
            done(work())
            self.assertIsNone(self.app.cell_inspection_id)
            self.assertIsNone(self.app.prepared_id)
        finally:
            self.app.tasks.submit = original
            self.app.setup_loading = False

    def test_reload_after_eda_change_requires_new_cell_apply(self):
        self._open_count(1)
        self._wait(lambda: self.app.cell_inspection_id == 'inspection-VCO')
        self.app.select_all_cells(True); self.app.apply_selected_cells()
        self._wait(lambda: self.app.prepared_id is not None)
        self.app._invalidate_prepared('EDA changed', preserve_edits=True)
        self.assertTrue(self.app.eda_revalidation_required)
        self.app.load_setup()
        self._wait(lambda: self.app.cell_inspection_id == 'inspection-VCO' and not self.app.setup_loading)
        self.assertFalse(self.app.selected_cell_scope_ids)
        self.assertIsNone(self.app.prepared_id)
        self.assertFalse(self.app.eda_revalidation_required)
        self.app.select_all_cells(True); self.app.apply_selected_cells()
        self._wait(lambda: self.app.prepared_id is not None)
        self.app._require_prepared_selection()
        self.assertEqual(self.app.execution_selection_panel.collect()['tests'], ['VCO'])

    def test_single_test_auto_select(self):
        self._open_count(1)
        self._wait(lambda: self.app.cell_inspection_id == 'inspection-VCO')
        self.assertEqual(self.service.cell_calls, ['VCO'])


@unittest.skipUnless(os.environ.get('DISPLAY'), 'requires real Tk')
class CornerPopupTests(unittest.TestCase):
    def test_multiple_toggles_and_outside_dismiss(self):
        root = gui.tk.Tk()
        try:
            picker = gui.CornerPicker(root, ['TT', 'SS', 'FF'])
            picker.pack(); root.update()
            picker.invoke(); root.update()
            self.assertIsNotNone(picker.popup)
            first = root.focus_get()
            first.event_generate('<space>'); root.update()
            second = first.master.winfo_children()[1]
            second.focus_force(); second.event_generate('<space>'); root.update()
            self.assertIsNotNone(picker.popup)
            self.assertEqual(picker.selected(), ['FF'])
            picker._outside_click(SimpleNamespace(x_root=-100, y_root=-100))
            self.assertIsNone(picker.popup)
            picker.invoke(); root.update()
            root.focus_get().event_generate('<Escape>'); root.update()
            self.assertIsNone(picker.popup)
            picker.destroy()
        finally:
            root.destroy()

    def test_mouse_checks_external_click_and_button_close(self):
        root = gui.tk.Tk(); root.geometry('400x300+100+100')
        try:
            picker = gui.CornerPicker(root, ['TT', 'SS', 'FF'])
            picker.pack(anchor='nw'); root.update()
            picker.invoke(); root.update()
            first = root.focus_get(); second = first.master.winfo_children()[1]
            for check in (first, second):
                check.event_generate('<ButtonPress-1>', x=5, y=5)
                check.event_generate('<ButtonRelease-1>', x=5, y=5)
                root.update()
                self.assertIsNotNone(picker.popup)
            self.assertEqual(picker.selected(), ['FF'])
            popup = picker.popup
            popup.event_generate('<ButtonPress-1>', x=-10, y=-10,
                                 rootx=root.winfo_rootx()+350, rooty=root.winfo_rooty()+250)
            root.update()
            self.assertIsNone(picker.popup)
            picker.invoke(); root.update()
            popup = picker.popup
            popup.event_generate('<ButtonPress-1>', x=-10, y=-10,
                                 rootx=picker.winfo_rootx()+2, rooty=picker.winfo_rooty()+2)
            root.update()
            self.assertIsNone(picker.popup)
            picker.invoke(); root.update(); picker.destroy(); root.update()
            self.assertIsNone(picker.popup)
        finally:
            root.destroy()

    def test_only_current_test_analyses(self):
        root = gui.tk.Tk()
        try:
            panel = ExecutionSelectionPanel(root)
            panel.set_setup({'tests': [{'name': 'B', 'enabled': False, 'analyses': [{'name': 'dc', 'enabled': True}]}]})
            self.assertEqual(panel.collect(), {'tests': ['B'], 'analyses': {'B': ['dc']}})
            with self.assertRaises(ValueError):
                panel.restore({'tests': ['A', 'B']})
            panel.destroy()
        finally:
            root.destroy()
