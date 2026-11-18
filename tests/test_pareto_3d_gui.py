# -*- coding: utf-8 -*-
"""Three-dimensional result presentation; deterministic, no native solver."""
import json
import os
import time
import unittest
from types import SimpleNamespace
from unittest import mock

from crescendo.demo_service import three_objective_demo, three_objective_dataset
from crescendo_engine.pareto_3d import Pareto3DPlot, sample_points, triple_points
from crescendo_engine.gui_state import fallback_pareto_ids
from tests import test_gui_light_loading as light


class Pareto3DDataTests(unittest.TestCase):
    def test_demo_determinism_size_and_true_front(self):
        doc = three_objective_demo()
        self.assertEqual(doc, three_objective_demo())
        self.assertLess(len(json.dumps(doc).encode('utf-8')), 300000)
        for generation in (0, 5, 11):
            data = three_objective_dataset(doc, generation)
            actual = fallback_pareto_ids(data['candidates'], doc['objectives'])
            self.assertEqual(actual, set(doc['frames'][generation]['pareto_ids']))
        self.assertTrue(any(not c['feasible'] for c in doc['candidates']))
        self.assertGreater(len(doc['frames'][-1]['pareto_ids']), 40)

    def test_filter_nonfinite_incomplete_and_exact_objective_count(self):
        doc = three_objective_demo(generations=1, population=4)
        rows = three_objective_dataset(doc, 0)['candidates']
        rows[0]['metrics']['objectives'][2]['value'] = float('nan')
        rows[1]['status'] = 'running'
        self.assertEqual(len(triple_points(rows, doc['objectives'], set())), 2)
        self.assertEqual(triple_points(rows, doc['objectives'][:2], set()), [])

    def test_sampling_preserves_front_and_is_deterministic(self):
        points = [dict(candidate_id=str(i), pareto=i % 30 == 0) for i in range(6000)]
        sampled = sample_points(points)
        self.assertEqual(len(sampled), 1600)
        self.assertEqual(sampled, sample_points(points))
        self.assertEqual(sum(p['pareto'] for p in sampled), 200)
        self.assertEqual(sample_points(points, 1), [p for p in points if p['pareto']])


    def test_english_labels_have_translation_entries(self):
        from crescendo.i18n_en_data import PAIRS
        from crescendo_engine.pareto_3d import TITLE, HELP, LEGEND, SAMPLED
        english = set(en for zh, en in PAIRS)
        self.assertTrue(set((TITLE, HELP, LEGEND, SAMPLED, 'Auto rotate', 'Reset view', 'Front surface')) <= english)

    def test_bounds_ticks_mesh_and_convergence_export(self):
        from crescendo_engine.pareto_geometry import data_bounds, nice_ticks, front_triangles
        doc = three_objective_demo()
        points = doc['candidates']
        self.assertEqual(doc['axis_bounds'], data_bounds(points))
        for axis, (lo, hi) in enumerate(doc['axis_bounds']):
            values = [p['values'][axis] for p in points]
            span = max(values)-min(values)
            self.assertAlmostEqual((min(values)-lo)/span, .065)
            self.assertAlmostEqual((hi-max(values))/span, .065)
            ticks = nice_ticks(lo, hi)
            self.assertTrue(3 <= len(ticks) <= 8)
            self.assertTrue(all(lo <= t <= hi for t in ticks))
        for frame in doc['frames']:
            ids = set(frame['pareto_ids'])
            current = [dict(p, pareto=p['candidate_id'] in ids) for p in points if p['generation'] <= frame['generation']]
            exported = [tuple(points[i]['candidate_id'] for i in tri) for tri in frame['front_triangles']]
            self.assertEqual(exported, front_triangles(current, doc['axis_bounds']))
            for tri in frame['front_triangles']:
                self.assertEqual(len(set(tri)), 3)
                self.assertTrue(all(points[i]['candidate_id'] in ids and points[i]['feasible'] for i in tri))
        def radii(g):
            return [sum(v*v for v in ((p['values'][0]-35)/55, (2-p['values'][1])/1.65,
                                      (p['values'][2]-30)/60))**.5 for p in points if p['generation'] == g]
        early, late = radii(0), radii(11)
        self.assertLess(max(early), min(late))
        self.assertLess(max(late)-min(late), .02)
        self.assertGreater(max(early)-min(early), .1)

    def test_degenerate_mesh_is_empty_and_cap_is_bounded(self):
        from crescendo_engine.pareto_geometry import front_triangles
        points = [dict(candidate_id=str(i), values=[i, i, i], feasible=True, pareto=True) for i in range(12)]
        self.assertEqual(front_triangles(points, [(0, 12)]*3), [])
        self.assertEqual(front_triangles(points[:2], [(0, 12)]*3), [])
        doc = three_objective_demo()
        points = [dict(p, pareto=p['candidate_id'] in doc['frames'][-1]['pareto_ids']) for p in doc['candidates']]
        mesh = front_triangles(points, doc['axis_bounds'], limit=40)
        self.assertLessEqual(len(set(cid for t in mesh for cid in t)), 40)
        self.assertLessEqual(len(mesh), 2*40)

    def test_small_unit_ticks_and_noncollinear_projection(self):
        from crescendo_engine.pareto_geometry import nice_ticks, front_triangles
        ticks = nice_ticks(1e-15, 9e-15)
        self.assertGreater(len(ticks), 2)
        self.assertEqual(len(ticks), len(set(ticks)))
        self.assertTrue(all(1e-15 <= value <= 9e-15 for value in ticks))
        points = [dict(candidate_id=str(i), values=[i/30., i/30., (i%7)/70.],
                       feasible=True, pareto=True) for i in range(31)]
        self.assertTrue(front_triangles(points, [(0, 1)]*3))


@unittest.skipUnless(os.environ.get('DISPLAY'), 'Existing DISPLAY required')
class Pareto3DGuiTests(unittest.TestCase):
    setUp = light.GuiLightLoadingTests.setUp
    _close_gui = light.GuiLightLoadingTests._close_gui
    _open = light.GuiLightLoadingTests._open
    _wait = light.GuiLightLoadingTests._wait
    _pump = light.GuiLightLoadingTests._pump

    def prepare(self):
        app = self._open(parameter_count=2)
        doc = three_objective_demo(generations=2, population=16)
        app.display_dataset = three_objective_dataset(doc, 1)
        app.pareto_ids = set(doc['frames'][1]['pareto_ids'])
        app.render_results()
        app.show_page('Pareto Trade-offs')
        app.render_pareto()
        self._pump(.1)
        app.pareto_3d.redraw()
        return app

    def test_exactly_three_objectives_and_retained_pairwise_views(self):
        app = self.prepare()
        self.assertIsInstance(app.pareto_3d, Pareto3DPlot)
        self.assertEqual(len(app.pareto_plots), 3)
        plot = app.pareto_3d
        self.root.deiconify()
        self._pump(.08)
        self.assertTrue(plot.winfo_ismapped())
        app.render_pareto()
        self.assertIs(app.pareto_3d, plot)
        objectives = list(app._display_spec()['objectives'])
        for count in (2, 4, 3, 1):
            app._display_spec()['objectives'] = (objectives + [dict(output_key='fourth')])[:count]
            app.render_pareto()
            self.assertEqual(getattr(app, 'pareto_3d', None) is not None, count == 3)

    def test_rotate_zoom_reset_do_not_create_or_delete_items(self):
        app = self.prepare(); plot = app.pareto_3d; canvas = plot.canvas
        ids = set(canvas.find_all())
        item = next(iter(plot._items.values())); before = canvas.coords(item)
        with mock.patch.object(canvas, '_create', wraps=canvas._create) as create, \
                mock.patch.object(canvas, 'delete', wraps=canvas.delete) as delete:
            plot._down(SimpleNamespace(x=200, y=200))
            plot._drag(SimpleNamespace(x=260, y=220))
            plot._up(SimpleNamespace(x=260, y=220))
            plot._wheel(SimpleNamespace(num=4, delta=0))
            self._pump(.05)
            self.assertNotEqual(canvas.coords(item), before)
            self.assertEqual(set(canvas.find_all()), ids)
            create.assert_not_called(); delete.assert_not_called()
            plot.reset(); self._pump(.05)
            self.assertEqual(plot.zoom, 1.0)
            self.assertEqual(canvas.coords(item), before)

    def test_point_click_uses_existing_candidate_details_and_drag_does_not_select(self):
        app = self.prepare(); plot = app.pareto_3d
        cid = max(plot._projected, key=lambda c: plot._projected[c][3])
        x, y, _, _ = plot._projected[cid]
        event = SimpleNamespace(x=x, y=y)
        plot._down(event); plot._up(event)
        self.assertEqual(app.selected_candidate_id, cid)
        self.assertEqual(tuple(app.result_tree.selection()), (cid,))
        with mock.patch.object(app, 'select_candidate') as select:
            plot._down(event); plot._drag(SimpleNamespace(x=x+50, y=y)); plot._up(event)
            select.assert_not_called()
        # Existing 2D plots use exactly this callback too.
        self.assertIs(app.pareto_plots[0].app, plot.app)

    def test_large_scene_sampling_and_timer_cleanup(self):
        app = self.prepare(); plot = app.pareto_3d
        points = [dict(candidate_id=str(i), values=[i % 71, i % 37, i % 59],
                       generation=i//100, feasible=i%8 != 0, pareto=i%50 == 0) for i in range(6000)]
        plot.render(points)
        self.assertEqual(len(plot.points), 1600)
        self.assertIn('1600 of 6000', plot.notice.get())
        self.assertEqual(len(plot.canvas.find_withtag('point')), 1600)
        plot.spin.set(True); plot._toggle_spin(); plot.rotate(.1, .1)
        jobs = (plot._spin_job, plot._job)
        plot.destroy()
        pending = self.root.tk.call('after', 'info')
        for job in jobs:
            self.assertNotIn(job, pending)
        self._pump(.08)

    def test_fit_resize_default_camera_surface_and_labels(self):
        import itertools
        app = self.prepare()
        plot = app.pareto_3d
        self.assertEqual((plot.yaw, plot.pitch), (plot.DEFAULT_YAW, plot.DEFAULT_PITCH))
        self.assertAlmostEqual(plot.DEFAULT_YAW, .92)
        self.assertTrue(plot.surface.get())
        self.assertTrue(plot.triangles)
        for width, height in ((480, 360), (900, 510), (1633, 720), (1920, 1080)):
            for angle in (.92, 2.49, 4.06):
                plot.yaw = angle
                with mock.patch.object(plot.canvas, 'winfo_width', return_value=width), \
                     mock.patch.object(plot.canvas, 'winfo_height', return_value=height):
                    plot.redraw()
                corners = [plot.project(c) for c in itertools.product((-1, 1), repeat=3)]
                for axis, size in ((0, width), (1, height)):
                    extent = max(c[axis] for c in corners)-min(c[axis] for c in corners)
                    self.assertTrue(.75 <= extent/size <= .9)
                boxes = [plot.canvas.bbox(item) for item in plot._axis_labels]
                for box in boxes:
                    self.assertTrue(box[0] >= 0 and box[1] >= 0 and box[2] <= width and box[3] <= height)
                for item, _, _ in plot._ticks:
                    if plot.canvas.itemcget(item, 'state') != 'hidden':
                        a = plot.canvas.bbox(item)
                        for b in boxes:
                            self.assertFalse(a[0] < b[2] and a[2] > b[0] and a[1] < b[3] and a[3] > b[1])
        ids = plot.canvas.find_all()
        plot.surface.set(False); plot.redraw()
        self.assertTrue(all(plot.canvas.itemcget(i, 'state') == 'hidden' for i in plot._surface_items))
        plot.surface.set(True); plot.redraw()
        self.assertTrue(all(plot.canvas.itemcget(i, 'state') == 'normal' for i in plot._surface_items))
        self.assertEqual(ids, plot.canvas.find_all())
        plot.reset(); plot.redraw()
        self.assertEqual((plot.yaw, plot.pitch, plot.zoom), (plot.DEFAULT_YAW, plot.DEFAULT_PITCH, 1.0))
