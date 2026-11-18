"""Manual-path GUI and plot invalidation on real Tk without desktop helpers."""
import copy
import os
import time
import unittest
from types import SimpleNamespace
from unittest import mock

from crescendo_engine import gui
from tests import test_gui_cell_selection as fixtures
from tests import test_gui_single_test_flow as single


class PathService(single.SingleTestService):
    def inspect_instance_paths(self, catalog, test, text, **options):
        preview = copy.deepcopy(self.preview)
        scope = preview['cell_scopes'][1]
        preview['cell_scopes'] = [scope]
        preview['instance_path_validation'] = {'schema_version': 'crescendo.instance_paths.v1',
            'scopes': [scope], 'lines': [{'line': 1, 'valid': True, 'hierarchy_path': scope['hierarchy_path']},
                                      {'line': 2, 'valid': False, 'reason': 'path not found'}]}
        self.inspections['paths'] = copy.deepcopy(preview)
        return 'paths', preview


@unittest.skipUnless(os.environ.get('DISPLAY'), 'requires real Tk')
class InstancePathsGuiTests(unittest.TestCase):
    setUp = fixtures.GuiCellSelectionTests.setUp
    _close = fixtures.GuiCellSelectionTests._close
    _wait = fixtures.GuiCellSelectionTests._wait

    def test_manual_choice_skips_auto_tree_and_dialog_shows_partial_validation(self):
        self.service = PathService(self.context, 1)
        self.app = gui.OptimizerGui(self.root, self.service, launch_context=self.context)
        app = self.app
        if hasattr(app, 'pro_mode_var'):
            app.pro_mode_var.set(True); app._apply_mode()
        app.manual_instance_paths_var.set(True)
        app.setup_path_var.set(self.paths['lib_A'])
        app.load_setup()
        self._wait(lambda: app.test_inspection_id is not None and not app.setup_loading)
        self.assertEqual(self.service.cell_calls, [])
        self.assertIsNone(app.cell_inspection_id)
        dialog = app.enter_instance_paths()
        texts = [w for w in dialog.winfo_children() if isinstance(w, gui.tk.Text)]
        texts[0].insert('1.0', '/CORE_A\n/missing')
        button = next(w for w in dialog.winfo_children() if isinstance(w, gui.ttk.Button))
        button.invoke()
        self._wait(lambda: app.cell_inspection_id == 'paths')
        self.assertIn('Line 2: ERROR', texts[1].get('1.0', 'end'))
        self.assertEqual(len(app.selected_cell_scope_ids), 1)
        self.assertFalse(self.service.cell_calls)
        dialog.destroy()
        app.apply_selected_cells()
        self._wait(lambda: app.prepared_id is not None)
        raw, _context = app._snapshot_configuration()
        self.assertEqual(raw['instance_path_selection']['scopes'][0]['hierarchy_path'], '/CORE_A')

    def test_corner_names_include_single_available_and_single_selected(self):
        picker = gui.CornerPicker(self.root, ['tt', 'ss'])
        self.addCleanup(picker.destroy)
        picker.set_selected(['ss'])
        self.assertEqual(picker.label_var.get(), 'ss')
        picker.select_all()
        self.assertEqual(picker.label_var.get(), 'All corners (2)')
        picker.select_none()
        self.assertEqual(picker.label_var.get(), 'No corners')
        only = gui.CornerPicker(self.root, ['only'])
        self.addCleanup(only.destroy)
        self.assertEqual(only.label_var.get(), 'only')

    def test_root_cancelled_canvas_resize_tears_down_cleanly(self):
        from crescendo import app as presentation
        page = gui.ScrollableFrame(self.root)
        surface = presentation._ParetoSurface(page)
        surface._schedule()
        self.assertIsNotNone(surface.job)
        # GUI hosts cancel all interpreter timers before destroying child widgets.
        self.root.after_cancel(surface.job)
        page.destroy()
        self.assertIsNone(surface.job)

    def test_six_objective_surface_scroll_keeps_items_and_candidate_clicks(self):
        from crescendo import app as presentation
        presentation.T.init_fonts(self.root)
        self.root.deiconify()
        self.root.geometry('800x600')
        page = gui.ScrollableFrame(self.root)
        page.pack(fill='both', expand=True)
        gui.ttk.Label(page.body, text='Pareto Trade-offs').pack()
        surface = presentation._ParetoSurface(page)
        selected = []
        points = [{'candidate_id': 'candidate', 'x': 1, 'y': 2, 'feasible': True,
                   'pareto': True, 'generation': 1}]
        plots = []
        for index in range(15):
            plot = presentation.ParetoPlot(surface, SimpleNamespace(select_candidate=selected.append),
                                          {'output_key': 'x'}, {'output_key': 'y'})
            plot.pack(fill='x')
            plot.render(points)
            plots.append(plot)
        self.root.update()
        self._wait(lambda: surface.job is None)
        canvas = page.canvas
        self.assertTrue(all(plot.canvas.widget is canvas for plot in plots))
        self.assertTrue(all(not plot.winfo_ismapped() for plot in plots))
        before = canvas.find_all()
        view = canvas.yview()
        with mock.patch.object(canvas, 'delete', wraps=canvas.delete) as delete:
            for _ in range(10):
                page.scroll_units(1)
                self.root.update()
            self.assertNotEqual(view, canvas.yview())
            self.assertEqual(before, canvas.find_all())
            delete.assert_not_called()
        canvas.yview_moveto(0)
        self.root.update()
        item = canvas.find_withtag('pareto')[0]
        coords = canvas.coords(item)
        x, y = (coords[0] + coords[2]) / 2, (coords[1] + coords[3]) / 2
        canvas.event_generate('<Motion>', x=int(x), y=int(y - canvas.canvasy(0)))
        canvas.event_generate('<Button-1>', x=int(x), y=int(y - canvas.canvasy(0)))
        self.root.update()
        self.assertEqual(selected, ['candidate'])
        # A live value update affects only its plot's tagged items.
        untouched = canvas.find_withtag(plots[1].canvas.tag)
        points[0]['x'] = 3
        plots[0].render(points)
        self.assertEqual(untouched, canvas.find_withtag(plots[1].canvas.tag))
        for plot in list(plots):
            plot.destroy()
        self.assertEqual(surface.winfo_children(), [])
        self.assertFalse(canvas.find_withtag('point'))
        page.destroy()

    def test_pareto_same_size_scroll_and_same_data_preserve_items_resize_coalesces(self):
        from crescendo import app as presentation
        presentation.T.init_fonts(self.root)
        # The module can already be installed by --crescendo; the plot contract is identical.
        plot = presentation.ParetoPlot(self.root, SimpleNamespace(select_candidate=lambda _cid: None),
                                      {'output_key': 'x'}, {'output_key': 'y'})
        plot.pack(fill='x'); self.root.update()
        points = [{'candidate_id': 'one', 'x': 1, 'y': 2, 'feasible': True, 'pareto': True, 'generation': 1}]
        plot.render(points)
        with mock.patch.object(plot.canvas, 'delete', wraps=plot.canvas.delete) as delete:
            for _index in range(20):
                plot._configure_plot(SimpleNamespace(width=plot.canvas.winfo_width(), height=330))
                plot.render(points)
            self.assertEqual(delete.call_count, 0)
            job = plot._resize_job
            for width in (800, 801, 802):
                plot._configure_plot(SimpleNamespace(width=width, height=330))
            self.assertNotEqual(job, plot._resize_job)
            points[0]['x'] = 3
            plot.render(points)
            self.assertEqual(delete.call_count, 1)
            old_items = plot.canvas.find_all()
            plot._configure_plot(SimpleNamespace(width=802, height=330))
            self.root.update_idletasks()
            self.assertEqual(old_items, plot.canvas.find_all())
        plot.destroy()
        self.assertIsNone(plot._resize_job)


if __name__ == '__main__':
    unittest.main()
