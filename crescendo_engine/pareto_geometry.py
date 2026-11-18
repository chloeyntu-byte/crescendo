"""Dependency-free plot geometry, shared by Tk and compact web exports."""
import math


def data_bounds(points):
    bounds = []
    for axis in range(3):
        values = [p["values"][axis] for p in points] or [0, 1]
        low, high = min(values), max(values)
        pad = (high-low or max(abs(low), 1.0)) * 0.065
        bounds.append((low-pad, high+pad))
    return bounds


def nice_ticks(low, high):
    raw = (high-low)/5.0
    magnitude = 10 ** math.floor(math.log10(raw))
    step = min((1, 2, 2.5, 5, 10), key=lambda n: abs(n*magnitude-raw))*magnitude
    first = math.ceil(low/step-1e-10)*step
    return [float("{:.12g}".format(first+i*step)) for i in range(9)
            if first+i*step <= high+step*1e-9]


def front_triangles(points, bounds, limit=220):
    """Delaunay on a normalized objective pair, capped for interactive redraw.

    Vertices are feasible front members only. Deduplicate projected positions,
    retain one sample by stable candidate-ID order, and reject long
    bridges across sparse regions. No interpolation outside the convex hull.
    Return candidate IDs; export translates these to global candidate indices.
    """
    front = sorted((p for p in points if p['pareto'] and p['feasible']),
                   key=lambda p: str(p['candidate_id']))
    if len(front) < 3:
        return []
    world = [tuple((v-lo)/(hi-lo) for v, (lo, hi) in zip(p['values'], bounds)) for p in front]
    # Covariance area rejects collinear pairs even when their spans are large.
    def area(ab):
        a, b = ab
        ma = sum(w[a] for w in world)/len(world)
        mb = sum(w[b] for w in world)/len(world)
        va = sum((w[a]-ma)**2 for w in world)
        vb = sum((w[b]-mb)**2 for w in world)
        cov = sum((w[a]-ma)*(w[b]-mb) for w in world)
        return max(0, va*vb-cov*cov)
    axes = max(((0, 1), (0, 2), (1, 2)), key=area)
    unique = {}
    for p, w in zip(front, world):
        key = (round(w[axes[0]], 9), round(w[axes[1]], 9))
        unique.setdefault(key, p['candidate_id'])
    entries = sorted(unique.items())
    if len(entries) > limit:
        entries = [entries[i*len(entries)//limit] for i in range(limit)]
    coords = [xy for xy, cid in entries]
    n = len(coords)
    if n < 3:
        return []
    coords += [(-16, -8), (16, -8), (0, 16)]
    def circle(triangle):
        (ax, ay), (bx, by), (cx, cy) = [coords[i] for i in triangle]
        d = 2*(ax*(by-cy)+bx*(cy-ay)+cx*(ay-by))
        if abs(d) < 1e-12:
            return None
        a, b, c = ax*ax+ay*ay, bx*bx+by*by, cx*cx+cy*cy
        x = (a*(by-cy)+b*(cy-ay)+c*(ay-by))/d
        y = (a*(cx-bx)+b*(ax-cx)+c*(bx-ax))/d
        return x, y, (x-ax)**2+(y-ay)**2
    mesh = {(n, n+1, n+2): circle((n, n+1, n+2))}
    for i, (x, y) in enumerate(coords[:n]):
        bad = [tri for tri, c in mesh.items() if (x-c[0])**2+(y-c[1])**2 <= c[2]+1e-12]
        edges = {}
        for tri in bad:
            del mesh[tri]
            for a, b in ((tri[0], tri[1]), (tri[1], tri[2]), (tri[2], tri[0])):
                edge = tuple(sorted((a, b)))
                edges[edge] = edges.get(edge, 0)+1
        for (a, b), count in sorted(edges.items()):
            if count == 1:
                tri = (a, b, i)
                c = circle(tri)
                if c is not None:
                    mesh[tri] = c
    result = []
    for tri in sorted(mesh):
        if max(tri) >= n:
            continue
        pairs = ((tri[0], tri[1]), (tri[1], tri[2]), (tri[2], tri[0]))
        if any(math.hypot(coords[a][0]-coords[b][0], coords[a][1]-coords[b][1]) > .48 for a, b in pairs):
            continue
        result.append(tuple(entries[i][1] for i in tri))
    return result
