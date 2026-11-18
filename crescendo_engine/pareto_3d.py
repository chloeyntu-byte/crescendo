# -*- coding: utf-8 -*-
"""Retained Tk 8.5 Canvas scene for exactly three objectives (no 3D dependency)."""
import math
import tkinter as tk
from tkinter import ttk

from .gui_state import metric_value
from .pareto_geometry import data_bounds, nice_ticks, front_triangles


TITLE = "3D Pareto trade-offs"
HELP = "Drag to rotate · Scroll to zoom · Double-click to reset · Click a candidate to inspect"
LEGEND = "Early → late generation     Orange outline: Pareto front     Grey: infeasible"
SAMPLED = "Showing {} of {} candidates; all {} Pareto candidates retained."


def triple_points(candidates, objectives, pareto_ids):
    if len(objectives) != 3:
        return []
    result = []
    for row in candidates:
        values = [metric_value(row, index) for index in range(3)]
        if row.get("status") != "completed" or any(value is None for value in values):
            continue
        result.append(dict(candidate_id=row.get("candidate_id"), values=values,
                           generation=int(row.get("generation") or 0),
                           feasible=(row.get("metrics") or {}).get("feasible") is True,
                           pareto=row.get("candidate_id") in pareto_ids))
    return result


def sample_points(points, limit=1600):
    """Keep every front point; deterministically spread the remaining budget."""
    front = [point for point in points if point["pareto"]]
    rest = [point for point in points if not point["pareto"]]
    budget = max(0, limit - len(front))
    if len(rest) > budget:
        rest = [rest[index * len(rest) // budget] for index in range(budget)]
    return front + rest


def blend(left, right, weight):
    return "#" + "".join("{:02x}".format(int(int(left[i:i+2], 16) * (1-weight) +
                                          int(right[i:i+2], 16) * weight)) for i in (1, 3, 5))


class Pareto3DPlot(ttk.Frame):
    """Rotate/zoom updates coordinates, styles and stacking, never scene items."""
    BACKGROUND = "#14161b"
    LIMIT = 1600
    DEFAULT_YAW = 0.92
    DEFAULT_PITCH = 0.48

    def __init__(self, parent, app, objectives, auto_rotate=False, bounds=None):
        ttk.Frame.__init__(self, parent, style="Card.TFrame", padding=12)
        self.app, self.objectives = app, objectives
        self.fixed_bounds = bounds
        self._owner = self.winfo_toplevel()
        self._job = None
        self._spin_job = None
        self._closed = False
        self._press = None
        self._dragged = False
        self.yaw, self.pitch, self.zoom = self.DEFAULT_YAW, self.DEFAULT_PITCH, 1.0
        self.points, self._items, self._scene = [], {}, []
        self._signature = None
        self._order = ()
        self._projected = {}
        self._text_sizes = {}
        self._text_styles = {}
        self._styles = {}
        self._glows, self._surface_items = {}, []
        self._surface_styles = {}
        self.surface = tk.BooleanVar(self, value=True)
        toolbar = ttk.Frame(self, style="Card.TFrame")
        toolbar.pack(fill="x")
        ttk.Label(toolbar, text=TITLE, style="CardTitle.TLabel").pack(side="left")
        self.spin = tk.BooleanVar(self, value=auto_rotate)
        ttk.Button(toolbar, text="Reset view", command=self.reset).pack(side="right")
        ttk.Checkbutton(toolbar, text="Auto rotate", variable=self.spin,
                        command=self._toggle_spin).pack(side="right", padx=12)
        ttk.Checkbutton(toolbar, text="Front surface", variable=self.surface,
                        command=self._schedule).pack(side="right", padx=8)
        ttk.Label(self, text=HELP, style="Muted.TLabel", wraplength=900).pack(anchor="w", pady=(5, 8))
        self.canvas = tk.Canvas(self, width=900, height=510, background=self.BACKGROUND,
                                highlightthickness=0, takefocus=True)
        self.canvas.pack(fill="both", expand=True)
        self.notice = tk.StringVar(self, value=LEGEND)
        ttk.Label(self, textvariable=self.notice, style="Muted.TLabel", wraplength=1100).pack(anchor="w", pady=(8, 0))
        self.canvas.bind("<Configure>", self._schedule)
        self.canvas.bind("<ButtonPress-1>", self._down)
        self.canvas.bind("<B1-Motion>", self._drag)
        self.canvas.bind("<ButtonRelease-1>", self._up)
        self.canvas.bind("<Double-Button-1>", self.reset)
        self.canvas.bind("<MouseWheel>", self._wheel)
        self.canvas.bind("<Button-4>", self._wheel)
        self.canvas.bind("<Button-5>", self._wheel)
        self.canvas.bind("<Left>", lambda e: self.rotate(-0.08, 0))
        self.canvas.bind("<Right>", lambda e: self.rotate(0.08, 0))
        self.canvas.bind("<Up>", lambda e: self.rotate(0, 0.06))
        self.canvas.bind("<Down>", lambda e: self.rotate(0, -0.06))
        self.canvas.bind("<Home>", self.reset)
        self.bind("<Destroy>", self._destroyed, add="+")
        self._build_scene()
        self._toggle_spin()

    def _build_scene(self):
        canvas = self.canvas
        # Ground and two walls. Shared world coordinates make depth readable.
        for index in range(9):
            t = -1 + 2 * index / 8.0
            for a, b in (((-1, t, -1), (1, t, -1)), ((t, -1, -1), (t, 1, -1)),
                         ((-1, 1, t), (1, 1, t)), ((t, 1, -1), (t, 1, 1))):
                self._scene.append((canvas.create_line(0, 0, 0, 0, fill="#303641"), (a, b), (0, 0)))
        self._ticks = []
        self._axes = []
        self._axis_labels = []
        for axis in range(3):
            self._axes.append((canvas.create_line(0, 0, 0, 0, fill="#8a909c", width=2), axis, None, False))
            for index in range(9):
                item = canvas.create_text(0, 0, text="", fill="#b6bdca", font=("TkDefaultFont", 9))
                self._axes.append((item, axis, index, False))
                self._ticks.append((item, axis, index))
            direction = self.objectives[axis].get("direction", "minimize")
            arrow = "↑" if direction == "maximize" else "↓" if direction == "minimize" else "→ target"
            name = self.objectives[axis].get("name") or self.objectives[axis].get("output_key") or "Output"
            label = "{} {}".format(name, arrow)
            item = canvas.create_text(0, 0, text=label, fill="#eceef2", width=180,
                                      font=("TkDefaultFont", 10, "bold"))
            self._axis_labels.append(item)
            self._axes.append((item, axis, 0.5, True))
        self._empty = canvas.create_text(450, 250, text="Waiting for completed candidates...", fill="#eceef2")

    def render(self, points):
        signature = tuple((p["candidate_id"], tuple(p["values"]), p["generation"], p["feasible"], p["pareto"]) for p in points)
        if signature == self._signature:
            self._schedule()
            return
        self._signature = signature
        self.points = sample_points(points, self.LIMIT)
        self.notice.set(LEGEND + ("\n" + SAMPLED.format(len(self.points), len(points),
                         sum(p["pareto"] for p in points)) if len(self.points) < len(points) else ""))
        self.bounds = list(self.fixed_bounds) if self.fixed_bounds else data_bounds(points)
        self._tick_values = [nice_ticks(*bound) for bound in self.bounds]
        for item, axis, index in self._ticks:
            ticks = self._tick_values[axis]
            self.canvas.itemconfigure(item, text="{:g}".format(ticks[index]) if index < len(ticks) else "")
        for i, (item, vertices, offset) in enumerate(self._scene):
            slot, kind = divmod(i, 4)
            axis = (1, 0, 2, 0)[kind]
            ticks = self._tick_values[axis]
            active = slot < len(ticks)
            self.canvas.itemconfigure(item, state="normal" if active else "hidden")
            if active:
                lo, hi = self.bounds[axis]
                t = 2*(ticks[slot]-lo)/(hi-lo)-1
                vertices = (((-1, t, -1), (1, t, -1)), ((t, -1, -1), (t, 1, -1)),
                            ((-1, 1, t), (1, 1, t)), ((t, 1, -1), (t, 1, 1)))[kind]
                self._scene[i] = (item, vertices, offset)
        for item in self._axis_labels + [entry[0] for entry in self._ticks]:
            self.canvas.itemconfigure(item, state="normal")
            box = self.canvas.bbox(item)
            self._text_sizes[item] = (box[2]-box[0], box[3]-box[1]) if box else (0, 0)
        self._text_styles.clear()
        wanted = set(p["candidate_id"] for p in self.points)
        for cid in set(self._items) - wanted:
            self.canvas.delete(self._items.pop(cid))
            self._styles.pop(cid, None)
        front_ids = set(p["candidate_id"] for p in self.points if p["pareto"] and p["feasible"])
        for cid in set(self._glows)-front_ids:
            self.canvas.delete(self._glows.pop(cid))
        for cid in front_ids-set(self._glows):
            self._glows[cid] = self.canvas.create_oval(0, 0, 0, 0, fill="#453027", outline="", tags=("glow",))
        last = max([p["generation"] for p in points] or [1]) or 1
        self._world = []
        for point in self.points:
            cid = point["candidate_id"]
            if cid not in self._items:
                self._items[cid] = self.canvas.create_oval(0, 0, 0, 0, tags=("point", "candidate:" + str(cid)))
            xyz = tuple(2*(v-low)/(high-low)-1 for v, (low, high) in zip(point["values"], self.bounds))
            age = point["generation"] / float(last)
            color = blend("#5988c9", "#ffc18b", age) if point["feasible"] else "#757d89"
            if point["pareto"] and point["feasible"]:
                color = blend(color, "#ffe5bb", .42)
                strength = .87
            else:
                strength = (.24 + .36*age) if point["feasible"] else .23
            # Quantize depth colors once per data update, avoiding per-frame RGB work.
            colors = [blend(self.BACKGROUND, color, strength*(.65+i*.035)) for i in range(11)]
            self._world.append((point, xyz, colors))
        self.triangles = front_triangles(self.points, self.bounds)
        while len(self._surface_items) > len(self.triangles):
            self.canvas.delete(self._surface_items.pop())
        while len(self._surface_items) < len(self.triangles):
            self._surface_items.append(self.canvas.create_polygon(0, 0, 0, 0, 0, 0,
                fill="#b77549", stipple="gray25", outline="#665346", width=1, tags=("front_surface",)))
        self._surface_styles.clear()
        self._surface_colors = [blend(self.BACKGROUND, "#d7996a", .23+i*.025) for i in range(11)]
        self._order = ()
        self.canvas.itemconfigure(self._empty, state="hidden" if points else "normal")
        self.redraw()

    def project(self, xyz):
        x, y, z = xyz
        cy, sy, cp, sp, sx, sy_scale, cx, center_y = self._camera
        u, depth = cy*x - sy*y, sy*x + cy*y
        v, depth = cp*z - sp*depth, cp*depth + sp*z
        return cx + sx*u, center_y - sy_scale*v, depth

    def redraw(self):
        if self._job is not None:
            self._owner.after_cancel(self._job)
        self._job = None
        if self._closed:
            return
        canvas = self.canvas
        width, height = max(1, canvas.winfo_width()), max(1, canvas.winfo_height())
        cy, sy, cp, sp = math.cos(self.yaw), math.sin(self.yaw), math.cos(self.pitch), math.sin(self.pitch)
        # Fit the projected box to an aspect-aware plot viewport. Separate
        # screen scales use wide canvases fully; objective units stay on axes.
        horizontal = 2*(abs(cy)+abs(sy))
        vertical = 2*(abs(cp)+abs(sp)*(abs(cy)+abs(sy)))
        self._camera = (cy, sy, cp, sp, width*.83/horizontal*self.zoom,
                        height*.81/vertical*self.zoom, width/2.0, height/2.0-4)
        commands = []
        widget = str(canvas)
        for item, vertices, offset in self._scene:
            coords = []
            for vertex in vertices:
                x, y, _ = self.project(vertex)
                coords.extend((x+offset[0], y+offset[1]))
            commands.append("%s coords %d %s" % (widget, item, " ".join("%.3f" % v for v in coords)))
        # Select silhouette edges and place text outward, away from data/ticks.
        edges = []
        for axis in range(3):
            options = []
            other = [i for i in range(3) if i != axis]
            for a in (-1, 1):
                for b in (-1, 1):
                    start = [-1, -1, -1]
                    start[other[0]], start[other[1]] = a, b
                    end = list(start); end[axis] = 1
                    p, q = self.project(start), self.project(end)
                    mid = ((p[0]+q[0])/2, (p[1]+q[1])/2)
                    rank = -mid[0] if axis == 2 else mid[1] - (10000 if start[2] != -1 else 0)
                    options.append((rank, p, q, mid))
            _, p, q, mid = max(options)
            nx, ny = -(q[1]-p[1]), q[0]-p[0]
            norm = math.hypot(nx, ny) or 1
            nx, ny = nx/norm, ny/norm
            if nx*(mid[0]-width/2) + ny*(mid[1]-height/2) < 0:
                nx, ny = -nx, -ny
            edges.append((p, q, nx, ny))
        text_boxes = []
        def overlaps(a, b):
            return a[0] < b[2]+5 and a[2]+5 > b[0] and a[1] < b[3]+4 and a[3]+4 > b[1]
        # Titles stay near an axis endpoint. Place titles before ticks so tick
        # density adapts to real font metrics and to narrow/rotated viewports.
        for item, axis, t, label in sorted(self._axes, key=lambda a: not a[3]):
            p, q, nx, ny = edges[axis]
            if t is None:
                commands.append("%s coords %d %.3f %.3f %.3f %.3f" % (widget, item, p[0], p[1], q[0], q[1]))
                continue
            if not label:
                ticks = self._tick_values[axis] if hasattr(self, "_tick_values") else []
                if t >= len(ticks):
                    commands.append("%s itemconfigure %d -state hidden" % (widget, item))
                    self._text_styles.pop(item, None)
                    continue
                lo, hi = self.bounds[axis]
                t = (ticks[t]-lo)/(hi-lo)
            else:
                t = .88
            gap = 30 if label else 12
            tx, ty = p[0]+(q[0]-p[0])*t+nx*gap, p[1]+(q[1]-p[1])*t+ny*gap
            anchor = ('w' if nx > 0 else 'e') if abs(nx) > .6 else ('n' if ny > 0 else 's')
            if label and axis == 2:
                # Vertical-axis title sits just beyond its upper endpoint;
                # long units no longer collide with the axis at the left edge.
                tx, ty = q[0], q[1]-18 if q[1] <= p[1] else q[1]+18
                anchor = 's' if q[1] <= p[1] else 'n'
            tw, th = self._text_sizes.get(item, (0, 0))
            def box_at(x, y):
                left = x if anchor == 'w' else x-tw if anchor == 'e' else x-tw/2
                top = y if anchor == 'n' else y-th if anchor == 's' else y-th/2
                return (left, top, left+tw, top+th)
            box = box_at(tx, ty)
            tx += max(6-box[0], 0)-max(box[2]-(width-6), 0)
            ty += max(6-box[1], 0)-max(box[3]-(height-6), 0)
            box = box_at(tx, ty)
            visible = True
            if any(overlaps(box, old) for old in text_boxes):
                if not label:
                    visible = False
                else:
                    tx += (p[0]-q[0])*.28
                    ty += (p[1]-q[1])*.28
                    box = box_at(tx, ty)
                    tx += max(6-box[0], 0)-max(box[2]-(width-6), 0)
                    ty += max(6-box[1], 0)-max(box[3]-(height-6), 0)
                    box = box_at(tx, ty)
            if visible:
                text_boxes.append(box)
            state = "normal" if visible else "hidden"
            commands.append("%s coords %d %.3f %.3f" % (widget, item, tx, ty))
            if self._text_styles.get(item) != (anchor, state):
                commands.append("%s itemconfigure %d -anchor %s -state %s" % (widget, item, anchor, state))
                self._text_styles[item] = (anchor, state)
        canvas.coords(self._empty, width/2, height/2)
        selected = getattr(self.app, "selected_candidate_id", None)
        projected = []
        self._projected = {}
        for point, xyz, colors in getattr(self, "_world", []):
            x, y, depth = self.project(xyz)
            cid = point["candidate_id"]
            item = self._items[cid]
            front = point["pareto"] and point["feasible"]
            radius = (5.8 if front else 2.3 if point["feasible"] else 1.8) * (0.83 + (depth+1.74)*0.09) * min(self.zoom, 1.4)
            if cid == selected:
                radius += 2
            commands.append("%s coords %d %.3f %.3f %.3f %.3f" %
                            (widget, item, x-radius, y-radius, x+radius, y+radius))
            color = colors[max(0, min(10, int((depth+1.74)*2.88)))]
            outline = "#ffffff" if cid == selected else "#FF7A3D" if front and point["feasible"] else color
            style = (color, outline, 2 if front or cid == selected else 1)
            if self._styles.get(cid) != style:
                commands.append("%s itemconfigure %d -fill %s -outline %s -width %d" % ((widget, item) + style))
                self._styles[cid] = style
            if cid in self._glows:
                glow = self._glows[cid]
                r = radius+3.5
                commands.append("%s coords %d %.3f %.3f %.3f %.3f" % (widget, glow, x-r, y-r, x+r, y+r))
                projected.append((depth-.001, glow))
            projected.append((depth, item))
            self._projected[cid] = (x, y, radius, depth)
        visible = self.surface.get()
        for item, triangle in zip(self._surface_items, self.triangles if hasattr(self, 'triangles') else []):
            vertices = [self._projected[cid] for cid in triangle]
            depth = sum(p[3] for p in vertices)/3.0
            commands.append("%s coords %d %s" % (widget, item, " ".join("%.3f %.3f" % p[:2] for p in vertices)))
            color = self._surface_colors[max(0, min(10, int((depth+1.74)*2.88)))]
            state = "normal" if visible else "hidden"
            if self._surface_styles.get(item) != (color, state):
                commands.append("%s itemconfigure %d -fill %s -state %s" % (widget, item, color, state))
                self._surface_styles[item] = (color, state)
            if visible:
                projected.append((depth-.02, item))
        order = tuple(item for depth, item in sorted(projected))
        if order != self._order:
            for item in order:
                commands.append("%s raise %d" % (widget, item))
            for item in self._axis_labels + [entry[0] for entry in self._ticks]:
                commands.append("%s raise %d" % (widget, item))
            self._order = order
        if commands:
            # One Python/Tcl boundary per frame. Only Tk-generated widget/item
            # IDs, numeric coordinates and internal hex colors enter this script;
            # candidate IDs, objective names and other user text never do.
            canvas.tk.call("eval", "\n".join(commands))

    def _schedule(self, event=None):
        if not self._closed and self._job is None:
            self._job = self._owner.after(8, self.redraw)

    def rotate(self, yaw, pitch):
        self.yaw += yaw
        self.pitch = max(-1.2, min(1.2, self.pitch + pitch))
        self._schedule()
        return "break"

    def _down(self, event):
        self.canvas.focus_set()
        self._press = (event.x, event.y)
        self._last = self._press
        self._dragged = False
        return "break"

    def _drag(self, event):
        if self._press:
            if abs(event.x-self._press[0]) + abs(event.y-self._press[1]) > 4:
                self._dragged = True
            self.rotate((event.x-self._last[0])*0.008, (event.y-self._last[1])*0.006)
            self._last = (event.x, event.y)
        return "break"

    def _up(self, event):
        if self._press and not self._dragged:
            hits = [(depth, cid) for cid, (x, y, radius, depth) in self._projected.items()
                    if (event.x-x)**2 + (event.y-y)**2 <= (radius+3)**2]
            if hits:
                self.app.select_candidate(max(hits)[1])
                self._schedule()
        self._press = None
        return "break"

    def _wheel(self, event):
        up = getattr(event, "num", None) == 4 or getattr(event, "delta", 0) > 0
        self.zoom = max(0.55, min(1.8, self.zoom * (1.1 if up else 1/1.1)))
        self._schedule()
        return "break"

    def reset(self, event=None):
        self.yaw, self.pitch, self.zoom = self.DEFAULT_YAW, self.DEFAULT_PITCH, 1.0
        self._press = None
        self._schedule()
        return "break"

    def _toggle_spin(self):
        if self._spin_job is not None:
            self._owner.after_cancel(self._spin_job)
            self._spin_job = None
        if self.spin.get() and not self._closed:
            self._spin_job = self._owner.after(40, self._spin)

    def _spin(self):
        self._spin_job = None
        if self.winfo_ismapped() and self._press is None:
            self.rotate(0.006, 0)
        self._toggle_spin()

    def _destroyed(self, event):
        if event.widget is self:
            self._closed = True
            for name in ("_job", "_spin_job"):
                job = getattr(self, name)
                if job is not None:
                    self._owner.after_cancel(job)
                    setattr(self, name, None)
