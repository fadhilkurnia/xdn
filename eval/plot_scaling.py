#!/usr/bin/env python3
"""Coordination-vs-cluster-size scaling figure from measure_cluster_bw.py
datasets at multiple N.

Two panels: (1) baseline-corrected coordination bytes per write op vs N;
(2) idle-phase peer-edge traffic (the protocol's standing mesh) vs N, log
scale. One line per protocol, drawn in a fixed categorical order with a
direct label at the last point. The families separate by growth: stars grow
O(N), meshes O(N^2), a relay chain grows per-hop, and the client-driven
chain grows with the client's fan-out.

Usage:
  python3 eval/plot_scaling.py -o scaling.png \
      --set etcd 3 bw2-etcd.json --set etcd 5 bwN5-etcd.json \
      --set mysql 3 bw2-mysql.json ...
"""

import argparse
import json

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

PALETTE = ["#2a78d6", "#008300", "#e87ba4", "#eda100",
           "#1baf7a", "#eb6834", "#4a3aa7", "#e34948"]
INK = "#333333"
INK_MUTED = "#767676"
GRID = "#e5e5e2"


def edge_deltas(d, ar, t0, t1):
    win = [s for s in d["samples"] if s["ar"] == ar and "edges" in s and t0 <= s["t"] <= t1]
    if len(win) < 2:
        return {}
    def emap(s):
        return {e["peer"]: (e["txBytes"], e["rxBytes"]) for e in s["edges"]}
    first, last = emap(win[0]), emap(win[-1])
    out = {}
    for peer, (ltx, lrx) in last.items():
        ftx, frx = first.get(peer, (0, 0))
        out[peer] = (ltx - ftx, lrx - frx)
    return out


def peer_tx_rate(d, t0, t1):
    """Total replica-to-replica tx across ARs, B/s; self-edges excluded."""
    tot = 0.0
    for i, ar in enumerate(d["ars"]):
        me = f"replica-{i}"
        for peer, (tx, _) in edge_deltas(d, ar, t0, t1).items():
            if peer.startswith("replica") and peer != me:
                tot += tx
    return tot / max(t1 - t0, 1e-9)


def metrics(path):
    d = json.load(open(path))
    ph = {p["name"]: p for p in d["phases"]}
    idle_rate = peer_tx_rate(d, ph["idle"]["t0"], ph["idle"]["t1"])
    w = ph["write"]
    w_rate = peer_tx_rate(d, w["t0"], w["t1"])
    per_op = max(w_rate - idle_rate, 0.0) * (w["t1"] - w["t0"]) / max(w["ops"], 1)
    return per_op, idle_rate


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-o", "--out", required=True)
    ap.add_argument("--set", nargs=3, action="append", required=True,
                    metavar=("PROTOCOL", "N", "PATH"))
    ap.add_argument("--pdf", action="store_true")
    args = ap.parse_args()

    series = {}
    order = []
    for proto, n, path in args.set:
        if proto not in series:
            series[proto] = []
            order.append(proto)
        per_op, idle = metrics(path)
        series[proto].append((int(n), per_op, idle))
    for proto in series:
        series[proto].sort()

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(7.2, 3.1))
    for ax, title, ylab, idx, logy in (
        (ax1, "coordination per write", "bytes / write (net of idle)", 1, False),
        (ax2, "standing mesh (idle)", "peer-edge B/s (log)", 2, True),
    ):
        for k, proto in enumerate(order):
            pts = series[proto]
            xs = [p[0] for p in pts]
            ys = [p[idx] for p in pts]
            color = PALETTE[k % len(PALETTE)]
            ax.plot(xs, ys, marker="o", markersize=5, linewidth=2, color=color)
            ax.annotate(proto, (xs[-1], ys[-1]), textcoords="offset points",
                        xytext=(6, 0), fontsize=6.5, color=INK, va="center")
        if logy:
            ax.set_yscale("log")
        ax.set_title(title, fontsize=9, color=INK)
        ax.set_xlabel("replicas (N)", fontsize=8, color=INK)
        ax.set_ylabel(ylab, fontsize=8, color=INK)
        all_ns = sorted({p[0] for pts in series.values() for p in pts})
        ax.set_xticks(all_ns)
        ax.margins(x=0.22)
        ax.tick_params(labelsize=7, colors=INK_MUTED)
        for s in ("top", "right"):
            ax.spines[s].set_visible(False)
        for s in ("left", "bottom"):
            ax.spines[s].set_color(GRID)
        ax.grid(axis="y", color=GRID, linewidth=0.6)
        ax.set_axisbelow(True)
    handles = [plt.Line2D([], [], color=PALETTE[k % len(PALETTE)], linewidth=2,
                          marker="o", markersize=4, label=p)
               for k, p in enumerate(order)]
    fig.legend(handles=handles, loc="lower center", ncol=min(len(order), 4),
               frameon=False, fontsize=7, bbox_to_anchor=(0.5, -0.02))
    fig.tight_layout(rect=(0, 0.08, 1, 1))
    fig.savefig(args.out, dpi=180 if not args.pdf else None,
                format="pdf" if args.pdf else None, facecolor="#fcfcfb",
                bbox_inches="tight")
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
