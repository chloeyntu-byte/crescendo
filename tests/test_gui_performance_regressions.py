"""Regressions for retained GUI rendering state; no EDA processes."""
import os
import unittest
from unittest import mock

from crescendo_engine import gui
from crescendo_engine.gui_performance import CachedCanvas
from tests import test_gui_light_loading as light


@unittest.skipUnless(os.environ.get("DISPLAY"), "Existing DISPLAY required")
class RetainedStateTkTests(unittest.TestCase):
    setUp = light.GuiLightLoadingTests.setUp
    _close_gui = light.GuiLightLoadingTests._close_gui
    _open = light.GuiLightLoadingTests._open
    _wait = light.GuiLightLoadingTests._wait
    _pump = light.GuiLightLoadingTests._pump
    _load = light.GuiLightLoadingTests._load

    def test_page_reentry_completes_cancelled_selection_refresh(self):
        app = self._open(parameter_count=1000)
        self._load()
        app.show_page("Parameters")
        self._wait(lambda: not app.parameter_tree_dirty and app.parameter_selection_job is None)
        tick = [0.0]
        def clock():
            tick[0] += .001
            return tick[0]
        with mock.patch.object(gui.time, "monotonic", side_effect=clock):
            app.select_all_parameters(False)
        self.assertIsNotNone(app.parameter_selection_job)
        self.assertTrue(any(app.parameter_tree.item(item, "text").startswith(gui.CHECKED_MARK)
                            for item in app.parameter_items.values()))
        app.show_page("Objectives & Constraints")
        app.show_page("Parameters")
        self._wait(lambda: not app.parameter_tree_dirty and app.parameter_selection_job is None)
        stale = [key for key, item in app.parameter_items.items()
                 if not app.parameter_tree.item(item, "text").startswith(gui.UNCHECKED_MARK)]
        self.assertEqual(stale, [])

    def test_cached_canvas_replacement_keeps_display_order(self):
        canvas = gui.tk.Canvas(self.root)
        draw = CachedCanvas(canvas)
        draw.create_line(1, 1, 20, 20)
        foreground = draw.create_oval(1, 1, 20, 20)
        draw.finish()
        draw = CachedCanvas(canvas)
        background = draw.create_text(10, 10, text="changed primitive")
        self.assertEqual(draw.create_oval(1, 1, 20, 20), foreground)
        draw.finish()
        self.assertEqual(canvas.find_all(), (background, foreground))
