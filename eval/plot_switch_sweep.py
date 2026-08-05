#!/usr/bin/env python3
"""Latency vs request size crossover plot from protocol_switch_sweep.py output.

  python3 eval/plot_switch_sweep.py -o crossover.png sweep-paxos.json sweep-pb.json
"""
import argparse
import json

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

INK = "#333333"
INK_MUTED = "#767676"
GRID = "#e5e5e2"
COLORS = {"paxos": "#2a78d6", "primary-backup": "#eb6834", "pb": "#eb6834"}
NAMES = {"paxos": "active replication (Paxos)", "primary-backup": "primary-backup",
         "pb": "primary-backup"}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("datasets", nargs="+")
    ap.add_argument("-o", "--out", required=True)
    ap.add_argument("--pdf", action="store_true")
    args = ap.parse_args()

    fig, ax = plt.subplots(figsize=(7.0, 4.0))
    curves = {}
    for path in args.datasets:
        d = json.load(open(path))
        lab = d["label"]
        xs = [r["size"] / 1024 for r in d["results"] if r["median_ms"] is not None]
        ys = [r["median_ms"] for r in d["results"] if r["median_ms"] is not None]
        curves[lab] = (xs, ys)
        c = COLORS.get(lab, "#4a3aa7")
        ax.plot(xs, ys, marker="o", markersize=6, linewidth=2, color=c, label=NAMES.get(lab, lab))
        for x, y in zip(xs, ys):
            ax.annotate(f"{y:.0f}", (x, y), textcoords="offset points", xytext=(0, 7),
                        fontsize=6.5, color=INK_MUTED, ha="center")

    # crossover: first size where PB <= Paxos
    if "paxos" in curves and any(k in curves for k in ("primary-backup", "pb")):
        pk = "primary-backup" if "primary-backup" in curves else "pb"
        px, py = curves["paxos"]
        bx, by = curves[pk]
        # Meaningful crossover: first size where Paxos is >20% slower than PB
        # (beyond ~1ms measurement noise), not the first noise-level tie.
        common = sorted(set(px) & set(bx))
        cross = None
        for x in common:
            p, b = py[px.index(x)], by[bx.index(x)]
            if p > 1.2 * b and p - b > 1.5:
                cross = x
                break
        if cross is not None:
            ax.axvline(cross, color=INK_MUTED, linestyle=":", linewidth=1)
            ax.annotate(f"PB wins from ~{cross:.0f}KB", (cross, ax.get_ylim()[1]),
                        xytext=(4, -4), textcoords="offset points", fontsize=8,
                        color=INK_MUTED, va="top")

    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("request size (KB, log)", fontsize=9, color=INK)
    ax.set_ylabel("median latency (ms, log)", fontsize=9, color=INK)
    ax.set_title("bookcatalog: latency vs request size, Paxos vs primary-backup",
                 fontsize=10, color=INK)
    ax.tick_params(labelsize=8, colors=INK_MUTED)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(GRID)
    ax.grid(which="both", color=GRID, linewidth=0.6)
    ax.set_axisbelow(True)
    ax.legend(frameon=False, fontsize=8, loc="upper left")
    fig.tight_layout()
    fig.savefig(args.out, dpi=180 if not args.pdf else None,
                format="pdf" if args.pdf else None, facecolor="#fcfcfb")
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
