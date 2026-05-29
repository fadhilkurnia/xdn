"""Shared plotting helpers for plot_bw_graph.py and plot_lat_graph.py.

Kept module-private (leading underscore) because these are internal
implementation details of the two scripts; nothing else in the repo
should import from here.
"""

import math

import networkx as nx
import numpy as np

CLIENT_PREFIX = "client-"  # mirrors trace_bw.CLIENT_PREFIX + "-"


# ----- arc3 / Bezier geometry ------------------------------------------------
#
# nx.draw_networkx_edges(connectionstyle="arc3,rad=R") draws each edge as a
# quadratic Bezier; reproducing that exact curve here lets us place edge
# labels along the same path the renderer uses.

def _arc3_control_point(p0, p1, rad):
    """Matplotlib's `arc3,rad=R` is a quadratic Bezier whose control point
    is at `midpoint + rad * (dy, -dx)` (matplotlib/patches.py
    ConnectionStyle.Arc3.connect): chord midpoint shifted by the
    *clockwise* perpendicular to the chord, scaled by `rad`. Reproducing
    that exactly here is what makes our label curve sit on the same arc
    that nx.draw_networkx_edges is drawing — so each label lands on its
    own edge instead of the opposite-direction edge's mirror.
    """
    chord = p1 - p0
    if float(np.linalg.norm(chord)) == 0.0:
        return p0.copy()
    perp_cw = np.array([chord[1], -chord[0]])
    return (p0 + p1) / 2.0 + rad * perp_cw


def _bezier_point(p0, c, p1, t):
    return (1.0 - t) ** 2 * p0 + 2.0 * (1.0 - t) * t * c + t ** 2 * p1


def _bezier_tangent(p0, c, p1, t):
    return 2.0 * (1.0 - t) * (c - p0) + 2.0 * t * (p1 - c)


def _t_at_distance_from_end(p0, c, p1, distance):
    """Find t in [0, 1] such that |B(t) - p1| ≈ `distance`.

    For an arc3 quadratic Bezier the distance from the endpoint is
    monotone in t over [0, 1], so a plain bisection converges fast.
    Returns 0.0 if the requested distance exceeds the curve length
    (i.e. the curve is shorter than the requested offset).
    """
    if distance <= 0.0:
        return 1.0
    if float(np.linalg.norm(p1 - p0)) <= distance:
        return 0.0
    lo, hi = 0.0, 1.0
    for _ in range(40):
        mid = (lo + hi) / 2.0
        if float(np.linalg.norm(_bezier_point(p0, c, p1, mid) - p1)) > distance:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2.0


def draw_edge_labels_at_fixed_offset(ax, graph, pos, edge_labels,
                                     rad, offset, **text_kwargs):
    """Draw each edge's label so that the label's *destination-facing
    edge* lands at a constant data-coordinate `offset` before the target
    node, measured along the edge's curved path. The rest of the label
    extends backward along the curve toward the source — which keeps the
    arrowhead end clear regardless of label width or edge length.

    Replaces `nx.draw_networkx_edge_labels(label_pos=…)`, which is
    proportional to edge length and therefore renders inconsistently
    across short and long edges.
    """
    bbox = text_kwargs.pop("bbox", None)
    fontsize = text_kwargs.pop("fontsize", 8)
    for (u, v), label in edge_labels.items():
        p0 = np.asarray(pos[u], dtype=float)
        p1 = np.asarray(pos[v], dtype=float)
        c = _arc3_control_point(p0, p1, rad)
        t = _t_at_distance_from_end(p0, c, p1, offset)
        xy = _bezier_point(p0, c, p1, t)
        tan = _bezier_tangent(p0, c, p1, t)
        angle = math.degrees(math.atan2(float(tan[1]), float(tan[0])))
        # Keep text upright on edges that would otherwise read upside
        # down. The flag remembers whether we flipped, because that
        # changes which side of the (rotated) text faces the destination.
        flipped = False
        if angle > 90.0:
            angle -= 180.0
            flipped = True
        elif angle < -90.0:
            angle += 180.0
            flipped = True
        # rotation_mode="anchor" anchors the alignment point first and
        # then rotates around it, so the label's chosen edge stays on
        # the curve at the anchor.
        #
        # No flip → text reads in the same direction as the edge tangent:
        #   the right side of the unrotated text faces the destination,
        #   so ha="right" parks the destination-facing edge at the
        #   anchor and the body extends back toward the source.
        # Flipped → text reads opposite to the edge tangent:
        #   the LEFT side of the unrotated text faces the destination
        #   instead, so ha="left" gives the same visual result.
        ha = "left" if flipped else "right"
        ax.text(float(xy[0]), float(xy[1]), label,
                rotation=angle, rotation_mode="anchor",
                ha=ha, va="center", fontsize=fontsize,
                bbox=bbox, **text_kwargs)


# ----- AR-on-circle layout ---------------------------------------------------

def replica_client_layout(graph, inner_radius=1.0, outer_radius=1.7):
    """Place AR vertices on a circle (inner ring) and each `client-<AR>`
    vertex on the outer ring at the same angle as its AR. Any leftover
    nodes (e.g. a bare 'client' fallback or 'unknown') are placed at the
    origin. If there are no AR vertices, fall back to a plain circular
    layout so this function stays a drop-in for `nx.circular_layout`.

    For graphs with no client- prefixed nodes (e.g. the latency graph)
    this simply puts every replica on the inner circle, which is the
    desired behaviour there too.
    """
    pos = {}
    nodes = list(graph.nodes)
    ar_nodes = sorted(n for n in nodes if not n.startswith(CLIENT_PREFIX)
                      and n != "client" and n != "unknown")
    if not ar_nodes:
        return nx.circular_layout(graph)

    # Match nx.circular_layout's convention: first node at angle 0, then
    # counterclockwise. This keeps AR ordering consistent with what readers
    # already saw in earlier renders.
    n_ar = len(ar_nodes)
    for i, ar in enumerate(ar_nodes):
        angle = 2.0 * math.pi * i / n_ar
        pos[ar] = (inner_radius * math.cos(angle),
                   inner_radius * math.sin(angle))

    for n in nodes:
        if n.startswith(CLIENT_PREFIX):
            ar = n[len(CLIENT_PREFIX):]
            if ar in pos:
                cx, cy = pos[ar]
                length = math.hypot(cx, cy) or 1.0
                pos[n] = (cx * outer_radius / length,
                          cy * outer_radius / length)
                continue
        if n not in pos:
            pos[n] = (0.0, 0.0)
    return pos
