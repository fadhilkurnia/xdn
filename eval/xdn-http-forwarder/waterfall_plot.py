#!/usr/bin/env python3
"""
waterfall_plot.py — segment-breakdown waterfall chart from ForwarderFrontend's
forwarder-timings.log (tReceived, tBeforeExecute, tAfterExecute, tFlushed).

IMPORTANT CAVEAT, read before trusting this chart:
Each segment's percentile is computed INDEPENDENTLY across all requests, then
the three are stacked visually for comparison. This does NOT mean "the p50
request's total is the sum of these three bars" — the request that had the
slowest segment 2 is not necessarily the same request that had the slowest
segment 1 or 3. This is a standard, useful way to compare *where time tends to
go* in aggregate, but it is not a per-request decomposition of any single
request's actual total latency. Treat it as "which segment is typically
biggest," not as an exact accounting of any one request.

Usage:
    python3 waterfall_plot.py forwarder-timings.log --unit ns -o waterfall.png
    python3 waterfall_plot.py forwarder-timings.log --unit ms -o waterfall.png

--unit ns : timestamps came from System.nanoTime() (recommended — sub-ms precision)
--unit ms : timestamps came from System.currentTimeMillis() (legacy — will likely
            show segment 1 as ~0 for every request, since that gap is usually
            sub-millisecond and currentTimeMillis() can't resolve it)
"""

import argparse
import csv
import sys

import matplotlib.pyplot as plt
import numpy as np


def load_segments(path: str, unit: str):
    """Returns three numpy arrays of segment durations, in milliseconds."""
    divisor = 1_000_000.0 if unit == "ns" else 1.0  # ns -> ms, or already ms

    seg1, seg2, seg3 = [], [], []
    skipped = 0

    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                t_recv = int(row["tReceivedMs"])
                t_before = int(row["tBeforeExecuteMs"])
                t_after = int(row["tAfterExecuteMs"])
                t_flushed = int(row["tFlushedMs"])
                status = int(row["statusCode"])
            except (ValueError, KeyError):
                skipped += 1
                continue

            # -1 marks a failed/errored request in ForwarderFrontend's error path —
            # tFlushedMs is meaningless for these, so drop them rather than let a
            # negative duration corrupt the percentiles.
            if status == -1 or t_flushed == -1:
                skipped += 1
                continue

            seg1.append((t_before - t_recv) / divisor)
            seg2.append((t_after - t_before) / divisor)
            seg3.append((t_flushed - t_after) / divisor)

    if skipped:
        print(f"Skipped {skipped} rows (parse errors or failed requests).", file=sys.stderr)

    if not seg1:
        raise ValueError("No valid rows parsed — check --unit and the input file's columns.")

    return np.array(seg1), np.array(seg2), np.array(seg3)


def make_waterfall(seg1, seg2, seg3, percentiles, out_path):
    labels = [f"p{p}" for p in percentiles]

    s1 = [np.percentile(seg1, p) for p in percentiles]
    s2 = [np.percentile(seg2, p) for p in percentiles]
    s3 = [np.percentile(seg3, p) for p in percentiles]
    totals = [a + b + c for a, b, c in zip(s1, s2, s3)]

    fig, ax = plt.subplots(figsize=(8, 5))
    y = np.arange(len(labels))
    bar_height = 0.5

    ax.barh(y, s1, height=bar_height, label="receive \u2192 handoff (seg 1)", color="#4C72B0")
    ax.barh(y, s2, height=bar_height, left=s1, label="forwarder.execute (seg 2)", color="#DD8452")
    left2 = [a + b for a, b in zip(s1, s2)]
    ax.barh(y, s3, height=bar_height, left=left2, label="response \u2192 flush (seg 3)", color="#55A868")

    for i, total in enumerate(totals):
        ax.text(total + max(totals) * 0.01, y[i], f"{total:.3f} ms", va="center", fontsize=9)

    ax.set_yticks(y)
    ax.set_yticklabels(labels)
    ax.invert_yaxis()
    ax.set_xlabel("Duration (ms)")
    ax.set_title("Forwarder latency breakdown by segment (each segment's percentile computed independently)")
    ax.legend(loc="lower right", fontsize=8)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    print(f"Saved: {out_path}")

    print("\nPer-segment percentile summary (ms):")
    header = f"{'':6}" + "".join(f"{l:>12}" for l in labels)
    print(header)
    for name, arr in [("seg1", s1), ("seg2", s2), ("seg3", s3), ("total", totals)]:
        print(f"{name:6}" + "".join(f"{v:12.4f}" for v in arr))


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("input_csv", help="Path to forwarder-timings.log")
    parser.add_argument("--unit", choices=["ns", "ms"], default="ns",
                         help="Unit the timestamps were recorded in (default: ns)")
    parser.add_argument("-o", "--output", default="waterfall.png", help="Output image path")
    parser.add_argument("--percentiles", type=float, nargs="+", default=[50, 90, 95],
                         help="Percentiles to plot (default: 50 90 95)")
    args = parser.parse_args()

    seg1, seg2, seg3 = load_segments(args.input_csv, args.unit)
    make_waterfall(seg1, seg2, seg3, args.percentiles, args.output)


if __name__ == "__main__":
    main()
