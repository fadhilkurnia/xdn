#!/usr/bin/env python3
"""
compare_summaries.py - Plots a side-by-side latency comparison between two
capture_bench.py summary.json files (e.g. --mode fuselog vs --mode none, or
two different apps/configs).

Produces one grouped bar chart per metric group (read latency, write latency
excluding capture overhead, write latency including capture overhead, and --
only if at least one side ran with --mode fuselog -- capture latency alone),
each showing p50/p95/p99 for both runs side by side.

Usage:
  python3 compare_summaries.py \
      --a runs/mysql_run1_fuselog/results/summary.json --label-a fuselog \
      --b runs/mysql_run1_none/results/summary.json --label-b none \
      --out comparison.png
"""

import argparse
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


METRIC_GROUPS = [
    ("read_latency_ms", "Read latency"),
    ("write_latency_excl_capture_ms", "Write latency (excl. capture)"),
    ("write_latency_incl_capture_ms", "Write latency (incl. capture)"),
    ("capture_latency_ms", "Capture latency (fuselog mode only)"),
]

PERCENTILE_KEYS = ["p50", "p95", "p99"]


def load_summary(path: Path) -> dict:
    with open(path) as f:
        return json.load(f)


def plot_comparison(summary_a: dict, label_a: str, summary_b: dict, label_b: str,
                     out_path: Path):
    # Only include metric groups where at least one side has non-null data.
    groups = [
        (key, title) for key, title in METRIC_GROUPS
        if summary_a.get(key) is not None or summary_b.get(key) is not None
    ]
    if not groups:
        raise ValueError("neither summary has any latency data to plot")

    fig, axes = plt.subplots(1, len(groups), figsize=(5 * len(groups), 5))
    if len(groups) == 1:
        axes = [axes]

    x = range(len(PERCENTILE_KEYS))
    width = 0.35

    for ax, (key, title) in zip(axes, groups):
        data_a = summary_a.get(key)
        data_b = summary_b.get(key)

        vals_a = [data_a[p] if data_a else None for p in PERCENTILE_KEYS]
        vals_b = [data_b[p] if data_b else None for p in PERCENTILE_KEYS]

        # Replace missing values with 0 for plotting, but annotate as "N/A".
        plot_vals_a = [v if v is not None else 0 for v in vals_a]
        plot_vals_b = [v if v is not None else 0 for v in vals_b]

        bars_a = ax.bar([i - width / 2 for i in x], plot_vals_a, width, label=label_a)
        bars_b = ax.bar([i + width / 2 for i in x], plot_vals_b, width, label=label_b)

        for bars, vals in [(bars_a, vals_a), (bars_b, vals_b)]:
            for bar, v in zip(bars, vals):
                label = f"{v:.1f}" if v is not None else "N/A"
                ax.annotate(label, (bar.get_x() + bar.get_width() / 2, bar.get_height()),
                            ha="center", va="bottom", fontsize=8)

        ax.set_xticks(list(x))
        ax.set_xticklabels(PERCENTILE_KEYS)
        ax.set_ylabel("ms")
        ax.set_title(title)
        ax.legend()
        ax.grid(axis="y", linestyle="--", alpha=0.4)

    fig.suptitle(
        f"{summary_a.get('app', '?')} -- {label_a} (n={summary_a.get('completed_count', '?')}) "
        f"vs {label_b} (n={summary_b.get('completed_count', '?')})"
    )
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    print(f"[compare_summaries] wrote {out_path}")


def print_text_summary(summary_a, label_a, summary_b, label_b):
    print(f"\n{'metric':<40} {label_a:>15} {label_b:>15}")
    print("-" * 72)
    for key, title in METRIC_GROUPS:
        data_a, data_b = summary_a.get(key), summary_b.get(key)
        if data_a is None and data_b is None:
            continue
        for p in PERCENTILE_KEYS:
            va = data_a[p] if data_a else None
            vb = data_b[p] if data_b else None
            va_s = f"{va:.2f}" if va is not None else "N/A"
            vb_s = f"{vb:.2f}" if vb is not None else "N/A"
            print(f"{title + ' ' + p:<40} {va_s:>15} {vb_s:>15}")

    sht_a = summary_a.get("state_container_health_time_s")
    sht_b = summary_b.get("state_container_health_time_s")
    print(f"{'stateful container health time (s)':<40} "
          f"{(f'{sht_a:.2f}' if sht_a is not None else 'N/A'):>15} "
          f"{(f'{sht_b:.2f}' if sht_b is not None else 'N/A'):>15}")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--a", required=True, type=Path, help="First summary.json")
    ap.add_argument("--label-a", default=None, help="Label for --a (default: its 'mode' field)")
    ap.add_argument("--b", required=True, type=Path, help="Second summary.json")
    ap.add_argument("--label-b", default=None, help="Label for --b (default: its 'mode' field)")
    ap.add_argument("--out", type=Path, default=Path("comparison.png"),
                     help="Output image path (default: comparison.png)")
    args = ap.parse_args()

    summary_a = load_summary(args.a)
    summary_b = load_summary(args.b)
    label_a = args.label_a or summary_a.get("mode", "a")
    label_b = args.label_b or summary_b.get("mode", "b")

    print_text_summary(summary_a, label_a, summary_b, label_b)
    plot_comparison(summary_a, label_a, summary_b, label_b, args.out)


if __name__ == "__main__":
    main()
