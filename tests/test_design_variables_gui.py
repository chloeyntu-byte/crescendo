"""Visible native parameter grouping and source text; uses the real Tk page."""
import os
import unittest

from tests import test_gui_optional_settings as fixture
from crescendo_engine.design_variables import catalog
from crescendo_engine.gui_state import initial_parameter_config


@unittest.skipUnless(os.environ.get('DISPLAY'), 'Real Tk requires DISPLAY')
class DesignVariablesGuiTests(unittest.TestCase):
    setUp = fixture.GuiOptionalSettingsTkTests.setUp
    _close = fixture.GuiOptionalSettingsTkTests._close
    _wait = fixture.GuiOptionalSettingsTkTests._wait
    _prepared = fixture.GuiOptionalSettingsTkTests._prepared

    def test_visible_group_current_source_and_mode_retention(self):
        self._prepared()
        app = self.app
        row = catalog(dict(tests=[dict(name='VCO', library='fixture_lib', cell='TB', view='schematic')],
                           global_variables=[dict(name='bias', value='2')]), {})[0]
        key = row['parameter_id']
        app.parameter_by_id = dict(app.parameter_by_id, **{key:row})
        app.parameter_configs[key] = initial_parameter_config(row)
        app.parameter_configs[key]['selected'] = True
        if hasattr(app, 'pro_mode_var'):
            app.pro_mode_var.set(True)
            app._apply_mode()
        app.show_page('Parameters')
        app.render_parameter_tree()
        try:
            self._wait(lambda: key in app.parameter_items and not app.parameter_tree_dirty)
        except AssertionError:
            self.fail(repr(dict(catalog=list(app.parameter_by_id), items=list(app.parameter_items),
                page=app.current_page, dirty=app.parameter_tree_dirty,
                mode=app.pro_mode_var.get() if hasattr(app, 'pro_mode_var') else None,
                forest=app._build_parameter_forest())))
        tree = app.parameter_tree
        item = app.parameter_items[key]
        self.assertIn('setup global', tree.item(item, 'text'))
        self.assertEqual(str(tree.set(item, 'current')), '2')
        group = tree.parent(item)
        self.assertIn('Design variables', tree.item(group, 'text'))
        tree.item(group, open=True)
        tree.see(item)
        self.root.update()
        self.assertTrue(tree.winfo_ismapped())
        self.assertTrue(tree.bbox(item))
        app.parameter_view_var.set('name')
        app.render_parameter_tree()
        self._wait(lambda: key in app.parameter_items and not app.parameter_tree_dirty and app.rendered_parameter_view == 'name')
        self.assertIn('setup global', tree.item(app.parameter_items[key], 'text'))
        if hasattr(app, 'pro_mode_var'):
            app.pro_mode_var.set(False)
            app._apply_mode()
            self._wait(lambda: not app.parameter_tree_dirty and key not in app.parameter_items)
            self.assertTrue(app.parameter_configs[key]['selected'])
            app.pro_mode_var.set(True)
            app._apply_mode()
            self._wait(lambda: not app.parameter_tree_dirty and key in app.parameter_items)
