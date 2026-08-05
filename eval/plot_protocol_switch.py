#!/usr/bin/env python3
"""Plot median latency over time from protocol_switch_demo.py output, with the
Paxos->primary-backup switch annotated.

  python3 eval/plot_protocol_switch.py -o psw.png /tmp/psw-demo.json
"""

import argparse
import json
import statistics

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

INK = "#333333"
INK_MUTED = "#767676"
GRID = "#e5e5e2"
C_PAXOS = "#2a78d6"
C_PB = "#eb6834"


def sliding_median(samples, window_s):
    """Median latency of OK requests in a trailing window, sampled per second."""
    ok = [(s["t"], s["latency_ms"]) for s in samples if s["ok"]]
    if not ok:
        return [], []
    tmax = ok[-1][0]
    xs, ys = [], []
    t = window_s
    while t <= tmax:
        win = [lat for (ti, lat) in ok if t - window_s <= ti <= t]
        if win:
            xs.append(t)
            ys.append(statistics.median(win))
        t += 1.0
    return xs, ys


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("datasets", nargs="+")
    ap.add_argument("-o", "--out", required=True)
    ap.add_argument("--window", type=float, default=3.0, help="median window (s)")
    ap.add_argument("--pdf", action="store_true")
    args = ap.parse_args()

    fig, ax = plt.subplots(figsize=(7.2, 3.4))
    for path in args.datasets:
        d = json.load(open(path))
        samples = d["samples"]
        switch_t = d["switch_at_rel"]
        xs, ys = sliding_median(samples, args.window)
        # split the line at the switch so each regime carries its own color
        pre_x = [x for x in xs if x <= switch_t]
        pre_y = [ys[i] for i, x in enumerate(xs) if x <= switch_t]
        post_x = [x for x in xs if x > switch_t]
        post_y = [ys[i] for i, x in enumerate(xs) if x > switch_t]
        ax.plot(pre_x, pre_y, color=C_PAXOS, linewidth=2, label="active replication (Paxos)")
        ax.plot(post_x, post_y, color=C_PB, linewidth=2, label="primary-backup")
        ax.axvline(switch_t, color=INK_MUTED, linestyle="--", linewidth=1)
        ax.annotate("switch", (switch_t, ax.get_ylim()[1]), xytext=(4, -4),
                    textcoords="offset points", fontsize=8, color=INK_MUTED,
                    va="top")

        pre = [s["latency_ms"] for s in samples if s["ok"] and s["phase"] == "paxos"]
        post = [s["latency_ms"] for s in samples
                if s["ok"] and s["phase"] == "primary-backup" and s["t"] > switch_t + 5]
        parts = []
        if pre:
            parts.append(f"Paxos median {statistics.median(pre):.0f}ms")
        if post:
            parts.append(f"PB median {statistics.median(post):.0f}ms")
        if parts:
            ax.set_title(
                f"{d['service']}: {' -> '.join(parts)}"
                f"  (request {d['pad_bytes']//1000}KB, statediff tiny)",
                fontsize=9, color=INK)

    ax.set_ylim(bottom=0)
    ax.set_xlabel("time (s)", fontsize=9, color=INK)
    ax.set_ylabel(f"median latency (ms, {args.window:.0f}s window)", fontsize=9, color=INK)
    ax.tick_params(labelsize=8, colors=INK_MUTED)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(GRID)
    ax.grid(axis="y", color=GRID, linewidth=0.6)
    ax.set_axisbelow(True)
    # dedupe legend labels
    handles, labels = ax.get_legend_handles_labels()
    seen = {}
    for h, l in zip(handles, labels):
        seen.setdefault(l, h)
    ax.legend(seen.values(), seen.keys(), frameon=False, fontsize=8, loc="upper right")
    fig.tight_layout()
    fig.savefig(args.out, dpi=180 if not args.pdf else None,
                format="pdf" if args.pdf else None, facecolor="#fcfcfb")
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
