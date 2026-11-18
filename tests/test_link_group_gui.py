"""Real Tk link/unlink and saved configuration behavior; no native EDA."""
import copy
import os
import unittest
from unittest import mock

from crescendo_engine import gui
from tests import test_gui_optional_settings as fixture
from tests import test_unified_configuration_gui as configuration_fixture


@unittest.skipUnless(os.environ.get('DISPLAY'), 'Real Tk requires DISPLAY')
class LinkGroupGuiTests(unittest.TestCase):
    setUp = fixture.GuiOptionalSettingsTkTests.setUp
    _close = fixture.GuiOptionalSettingsTkTests._close
    _wait = fixture.GuiOptionalSettingsTkTests._wait
    _prepared = fixture.GuiOptionalSettingsTkTests._prepared
    _install = configuration_fixture.UnifiedConfigurationGuiTests._install
    _save = configuration_fixture.UnifiedConfigurationGuiTests._save
    _load = configuration_fixture.UnifiedConfigurationGuiTests._load

    def _ready(self):
        self._install()
        self.app.show_page('Parameters')
        self.app.parameter_configs['/M0/w'].update(selected=True, minimum='1', maximum='5', unit='', scale='linear', step=1, grid_origin=0)
        self.app.parameter_configs['/M1/w'].update(selected=True, minimum='10', maximum='50', unit='', scale='linear', step=10, grid_origin=0)
        self.app.render_parameter_tree()
        self._wait(lambda: len(self.app.parameter_items) == 2 and not self.app.parameter_tree_dirty)
        self.app.parameter_tree.selection_set([self.app.parameter_items[key] for key in ('/M0/w', '/M1/w')])
        return self.app

    def test_link_unlink_buttons_validate_and_show_member_groups(self):
        app = self._ready()
        if hasattr(app, 'pro_mode_var'):
            app.pro_mode_var.set(True); app._apply_mode()
        with mock.patch.object(gui.simpledialog, 'askstring', return_value='widths'):
            app.parameter_link_button.invoke()
        for key in ('/M0/w', '/M1/w'):
            self.assertEqual(app.parameter_configs[key]['link_group'], 'widths')
            self.assertEqual(app.parameter_tree.set(app.parameter_items[key], 'link_group'), 'widths')
        self.root.update()
        self.assertTrue(app.parameter_link_button.winfo_ismapped())
        item = app.parameter_items['/M0/w']
        app.parameter_tree.see(item)
        self.root.update()
        self.assertTrue(app.parameter_tree.bbox(item))
        app.parameter_unlink_button.invoke()
        self.assertNotIn('link_group', app.parameter_configs['/M0/w'])
        app.parameter_configs['/M1/w']['step'] = 20
        before = copy.deepcopy(app.parameter_configs)
        with mock.patch.object(gui.simpledialog, 'askstring', return_value='bad'), mock.patch.object(app, '_set_error') as error:
            app.parameter_link_button.invoke()
        self.assertIn("link_group 'bad' member /M1/w", error.call_args[0][0])
        self.assertEqual(before, app.parameter_configs)

    def test_configuration_save_load_and_simple_mode_retain_optional_fields(self):
        app = self._ready()
        if hasattr(app, 'pro_mode_var'):
            app.pro_mode_var.set(True); app._apply_mode()
        with mock.patch.object(gui.simpledialog, 'askstring', return_value='widths'):
            app.parameter_link_button.invoke()
        path = self._save('linked.json')
        for config in app.parameter_configs.values():
            for key in ('step', 'grid_origin', 'link_group'):
                config.pop(key, None)
        self._load(path)
        self.assertEqual(app.parameter_configs['/M0/w']['step'], 1)
        self.assertEqual(app.parameter_configs['/M1/w']['step'], 10)
        self.assertEqual(app.parameter_configs['/M0/w']['grid_origin'], 0)
        self.assertEqual(app.parameter_configs['/M1/w']['link_group'], 'widths')
        if hasattr(app, 'pro_mode_var'):
            app.pro_mode_var.set(False); app._apply_mode(); self.root.update()
            self.assertFalse(app.parameter_link_controls.winfo_ismapped())
            self.assertNotIn('link_group', app.parameter_tree['displaycolumns'])
            self.assertEqual(app.parameter_configs['/M1/w']['link_group'], 'widths')
            app.pro_mode_var.set(True); app._apply_mode(); self.root.update()
            self.assertIn('link_group', app.parameter_tree['displaycolumns'])
            self.assertTrue(app.parameter_link_controls.winfo_ismapped())

    def test_ctrl_highlight_adds_rows_without_toggling_optimizer_selection(self):
        app = self._ready()
        if hasattr(app, 'pro_mode_var'):
            app.pro_mode_var.set(True); app._apply_mode()
        tree = app.parameter_tree
        first, second = [app.parameter_items[key] for key in ('/M0/w', '/M1/w')]
        tree.selection_set(first)
        tree.see(second); self.root.update()
        x, y, width, height = tree.bbox(second)
        before = copy.deepcopy(app.parameter_configs)
        tree.event_generate('<ButtonPress-1>', x=x + 20, y=y + height // 2, state=4)
        tree.event_generate('<ButtonRelease-1>', x=x + 20, y=y + height // 2, state=4)
        self.root.update()
        self.assertEqual(set(tree.selection()), {first, second})
        self.assertEqual(app.parameter_configs, before)

    def test_link_controls_and_column_remain_visible_at_minimum_size(self):
        app = self._ready()
        if hasattr(app, 'pro_mode_var'):
            app.pro_mode_var.set(True); app._apply_mode()
        width = max(1024, self.root.minsize()[0])
        self.root.geometry('%sx768' % width)
        self._wait(lambda: self.root.winfo_width() == width)
        for button in (app.parameter_link_button, app.parameter_unlink_button):
            self.assertTrue(button.winfo_ismapped())
            self.assertLessEqual(button.winfo_rootx() + button.winfo_width(),
                                 self.root.winfo_rootx() + self.root.winfo_width())
        item = app.parameter_items['/M0/w']
        app.parameter_tree.see(item)
        app.parameter_tree.xview_moveto(1)
        self.root.update()
        x, y, width, height = app.parameter_tree.bbox(item, 'link_group')
        self.assertGreater(height, 0)
        self.assertGreater(width, 0)
        self.assertGreater(x, 0)
        self.assertLessEqual(x + width, app.parameter_tree.winfo_width())
