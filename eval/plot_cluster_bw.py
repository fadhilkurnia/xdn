#!/usr/bin/env python3
"""Directed coordination-graph small multiples from measure_cluster_bw.py JSON.

One row per dataset, one panel per phase (idle / write / read). Nodes sit in
the same fixed layout in every panel (replicas in a triangle, the client at
the upper left) so the protocol's SHAPE is comparable across panels and
across services: a Raft star, a GR mesh, a chain path, and a client-driven
fan-out are distinguishable at a glance.

Encoding: edge width grows with log(rate); color carries the edge class
(blue = replica-to-replica coordination D^s, orange = client demand D^u);
arrowheads carry direction. Each directed edge is one curved arrow, its
reverse offset on the opposite arc. Panels print the peak edge rate so the
widths stay anchored to numbers; edges below a floor rate are dropped as
noise. Self-edges (in-pod control clients hairpinning the member's own
overlay address, e.g. corfu's embedded client talking to its local server)
are excluded, matching eval/analyze_cluster_bw.py.

Usage:
  python3 eval/plot_cluster_bw.py -o /tmp/coordination-graphs.png \
      eval/datasets/cluster-bw/proxied/bw2-*.json
  # optional: label rows with "Display Name=path", --pdf for vector output,
  # --floor 50 (B/s noise floor)
  python3 eval/plot_cluster_bw.py -o graphs.pdf --pdf \
      "etcd (Raft)=.../bw2-etcd.json" "Redis chain=.../bw2-redis.json"
"""

import argparse
import json
import math

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch

COLOR_COORD = "#2a78d6"  # replica-to-replica (D^s)
COLOR_CLIENT = "#eb6834"  # client demand (D^u)
INK = "#333333"
INK_MUTED = "#767676"
GRID = "#e5e5e2"

def make_pos(n):
    """Fixed layout for n replicas: a circle with replica-0 at the top,
    ordinals clockwise, and the client offset at the upper left. Identical
    across panels so protocol shape is comparable."""
    pos = {"client": (0.03, 0.90)}
    for i in range(n):
        theta = math.pi / 2 - 2 * math.pi * i / n
        pos[f"replica-{i}"] = (0.5 + 0.38 * math.cos(theta), 0.51 + 0.38 * math.sin(theta))
    return pos


def phase_edges(d, t0, t1, floor):
    """Directed edge rates (B/s) for one phase: {(src, dst): rate}.

    Each AR's own tx counters are authoritative for its outgoing edges (the
    peer's rx of the same flow is the mirror); client edges use the AR-side
    tx (replica->client) and rx (client->replica).
    """
    edges = {}
    dur = max(t1 - t0, 1e-9)
    for i, ar in enumerate(d["ars"]):
        me = f"replica-{i}"
        win = [s for s in d["samples"] if s["ar"] == ar and "edges" in s and t0 <= s["t"] <= t1]
        if len(win) < 2:
            continue

        def emap(s):
            return {e["peer"]: (e["txBytes"], e["rxBytes"]) for e in s["edges"]}

        first, last = emap(win[0]), emap(win[-1])
        for peer, (ltx, lrx) in last.items():
            ftx, frx = first.get(peer, (0, 0))
            tx, rx = (ltx - ftx) / dur, (lrx - frx) / dur
            if peer == me:
                continue  # self-edge: in-pod control client
            if peer == "client":
                if tx >= floor:
                    edges[(me, "client")] = tx
                if rx >= floor:
                    edges[("client", me)] = rx
            elif peer.startswith("replica"):
                if tx >= floor:
                    edges[(me, peer)] = tx
    return edges


def width_of(rate, floor):
    return 0.7 + 1.15 * math.log10(max(rate, floor) / floor)


def human(rate):
    if rate >= 1e6:
        return f"{rate / 1e6:.1f} MB/s"
    if rate >= 1e3:
        return f"{rate / 1e3:.1f} KB/s"
    return f"{rate:.0f} B/s"


def draw_panel(ax, edges, floor, pos):
    ax.set_xlim(-0.08, 1.08)
    ax.set_ylim(-0.06, 1.06)
    ax.set_aspect("equal")
    ax.axis("off")
    used = {n for e in edges for n in e}
    node_size = 340 if len(pos) <= 5 else 230
    node_font = 8 if len(pos) <= 5 else 6.5
    for name, (x, y) in pos.items():
        if name == "client" and "client" not in used:
            continue
        face = "#ffffff"
        edge_color = INK_MUTED if name != "client" else COLOR_CLIENT
        label = "C" if name == "client" else "R" + name.rsplit("-", 1)[1]
        ax.scatter([x], [y], s=node_size, zorder=3, facecolor=face,
                   edgecolor=edge_color, linewidth=1.2)
        ax.text(x, y, label, ha="center", va="center", fontsize=node_font,
                color=INK, zorder=4)
    peak = max(edges.values(), default=0)
    for (src, dst), rate in sorted(edges.items(), key=lambda kv: kv[1]):
        color = COLOR_CLIENT if "client" in (src, dst) else COLOR_COORD
        arrow = FancyArrowPatch(
            pos[src],
            pos[dst],
            connectionstyle="arc3,rad=0.14",
            arrowstyle="-|>,head_length=4,head_width=2.4",
            mutation_scale=1.0,
            shrinkA=11,
            shrinkB=11,
            linewidth=width_of(rate, floor),
            color=color,
            alpha=0.95,
            zorder=2,
        )
        ax.add_patch(arrow)
    if peak > 0:
        ax.text(0.98, -0.04, f"peak {human(peak)}", ha="right", va="bottom",
                fontsize=6.5, color=INK_MUTED, transform=ax.transAxes)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("datasets", nargs="+")
    ap.add_argument("-o", "--out", required=True)
    ap.add_argument("--floor", type=float, default=40.0, help="noise floor in B/s")
    ap.add_argument("--pdf", action="store_true")
    args = ap.parse_args()

    rows = []
    for spec in args.datasets:
        label, _, path = spec.rpartition("=")
        d = json.load(open(path or spec))
        d["_label"] = label if label else d["service"]
        rows.append(d)

    phases = ["idle", "write", "read"]
    fig_h = 1.85 * len(rows) + 0.7
    fig, axes = plt.subplots(len(rows), 3, figsize=(7.2, fig_h))
    if len(rows) == 1:
        axes = [axes]

    for r, d in enumerate(rows):
        by_name = {p["name"]: p for p in d["phases"]}
        pos = make_pos(len(d["ars"]))
        for c, phname in enumerate(phases):
            ax = axes[r][c]
            ph = by_name.get(phname)
            edges = phase_edges(d, ph["t0"], ph["t1"], args.floor) if ph else {}
            draw_panel(ax, edges, args.floor, pos)
            if r == 0:
                ax.set_title(phname, fontsize=9, color=INK, pad=6)
            if c == 0:
                ax.text(-0.16, 0.5, d["_label"], transform=ax.transAxes, rotation=90,
                        ha="center", va="center", fontsize=8, color=INK)

    handles = [
        plt.Line2D([], [], color=COLOR_COORD, linewidth=2.2, label="replica ↔ replica (coordination)"),
        plt.Line2D([], [], color=COLOR_CLIENT, linewidth=2.2, label="client ↔ replica (demand)"),
    ]
    fig.legend(handles=handles, loc="lower center", ncol=2, frameon=False,
               fontsize=8, bbox_to_anchor=(0.5, 0.002))
    fig.suptitle("Coordination graphs by phase (edge width ∝ log rate)",
                 fontsize=10, color=INK)
    fig.tight_layout(rect=(0.02, 0.025, 1, 0.975))
    out = args.out
    fig.savefig(out, dpi=180 if not args.pdf else None,
                format="pdf" if args.pdf else None, facecolor="#fcfcfb")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
