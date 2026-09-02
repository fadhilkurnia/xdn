#!/usr/bin/env python3
"""
waterfall_plot.py — segment-breakdown waterfall chart from ForwarderFrontend's
timing logs. Supports BOTH log formats produced by ForwarderFrontend:

  1. "outer" (forwarder-timings.log), from TimingLogger:
       reqId,tReceivedMs,tBeforeExecuteMs,tAfterExecuteMs,tFlushedMs,statusCode
     -> segments: receive->handoff, forwarder.execute, response->flush

  2. "inner" (*-inner.log), from InnerTimingLogger:
       reqId,tBeforeExecuteNanos,tAcquireNanos,tWriteNanos,tRespRecvNanos
     -> segments: pool acquire, write request, backend wait

The format is auto-detected from the CSV header — no flag needed for the
common case. Despite the "Ms" suffix in the outer format's column names,
values are actually nanoseconds if you've made the System.nanoTime() switch
(see project notes) — pass --unit ms only if still on the old
System.currentTimeMillis() version.

IMPORTANT CAVEAT, read before trusting either chart:
Each segment's percentile is computed INDEPENDENTLY across all requests, then
stacked visually for comparison. This does NOT mean "the p50 request's total
is the sum of these bars" — the request with the slowest segment 2 is not
necessarily the same request with the slowest segment 1 or 3. This is a
standard, useful way to see *where time tends to go* in aggregate, but it is
not a per-request decomposition of any single request's actual total latency.

Usage:
    python3 waterfall_plot.py forwarder-timings.log -o outer.png
    python3 waterfall_plot.py forwarder-timings-inner.log -o inner.png
    python3 waterfall_plot.py forwarder-timings.log --unit ms -o outer.png   # legacy currentTimeMillis() logs
"""

import argparse
import csv
import sys

import matplotlib.pyplot as plt
import numpy as np

OUTER_COLUMNS = {"tReceivedMs", "tBeforeExecuteMs", "tAfterExecuteMs", "tFlushedMs", "statusCode"}
INNER_COLUMNS = {"tBeforeExecuteNanos", "tAcquireNanos", "tWriteNanos", "tRespRecvNanos"}


def detect_format(fieldnames):
    fields = set(fieldnames)
    if OUTER_COLUMNS.issubset(fields):
        return "outer"
    if INNER_COLUMNS.issubset(fields):
        return "inner"
    raise ValueError(
        f"Unrecognized column set: {fieldnames}\n"
        f"Expected either outer columns ({sorted(OUTER_COLUMNS)}) "
        f"or inner columns ({sorted(INNER_COLUMNS)})."
    )


def load_outer_segments(path: str, unit: str):
    """receive->handoff, forwarder.execute, response->flush — in ms."""
    divisor = 1_000_000.0 if unit == "ns" else 1.0
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
            if status == -1 or t_flushed == -1:
                skipped += 1
                continue
            seg1.append((t_before - t_recv) / divisor)
            seg2.append((t_after - t_before) / divisor)
            seg3.append((t_flushed - t_after) / divisor)

    if skipped:
        print(f"Skipped {skipped} rows (parse errors or failed requests).", file=sys.stderr)

    labels = ["receive \u2192 handoff", "forwarder.execute", "response \u2192 flush"]
    return labels, [np.array(seg1), np.array(seg2), np.array(seg3)]


def load_inner_segments(path: str):
    """pool acquire, write request, backend wait — always nanoseconds -> ms."""
    seg_acquire, seg_write, seg_wait = [], [], []
    skipped = 0

    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                t_before = int(row["tBeforeExecuteNanos"])
                t_acquire = int(row["tAcquireNanos"])
                t_write = int(row["tWriteNanos"])
                t_resp = int(row["tRespRecvNanos"])
            except (ValueError, KeyError):
                skipped += 1
                continue
            # tAcquireNanos == 0 means the request never even got scheduled
            # (e.g. acquire failed before ts[0] was set) — drop, not a real sample.
            if t_acquire == 0:
                skipped += 1
                continue
            seg_acquire.append((t_acquire - t_before) / 1_000_000.0)
            seg_write.append((t_write - t_acquire) / 1_000_000.0)
            seg_wait.append((t_resp - t_write) / 1_000_000.0)

    if skipped:
        print(f"Skipped {skipped} rows (parse errors or incomplete requests).", file=sys.stderr)

    labels = ["pool acquire", "write request", "backend wait"]
    return labels, [np.array(seg_acquire), np.array(seg_write), np.array(seg_wait)]


def make_waterfall(labels, segments, percentiles, out_path, title):
    pct_labels = [f"p{p}" for p in percentiles]
    colors = ["#4C72B0", "#DD8452", "#55A868", "#C44E52", "#8172B2"]

    per_segment_pcts = [[np.percentile(seg, p) for p in percentiles] for seg in segments]
    totals = [sum(vals) for vals in zip(*per_segment_pcts)]

    fig, ax = plt.subplots(figsize=(8, 5))
    y = np.arange(len(pct_labels))
    bar_height = 0.5
    left = np.zeros(len(pct_labels))

    for label, pcts, color in zip(labels, per_segment_pcts, colors):
        ax.barh(y, pcts, height=bar_height, left=left, label=label, color=color)
        left = left + np.array(pcts)

    for i, total in enumerate(totals):
        ax.text(total + max(totals) * 0.01, y[i], f"{total:.4f} ms", va="center", fontsize=9)

    ax.set_yticks(y)
    ax.set_yticklabels(pct_labels)
    ax.invert_yaxis()
    ax.set_xlabel("Duration (ms)")
    ax.set_title(title)
    ax.legend(loc="lower right", fontsize=8)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    print(f"Saved: {out_path}")

    print("\nPer-segment percentile summary (ms):")
    header = f"{'':16}" + "".join(f"{l:>12}" for l in pct_labels)
    print(header)
    for name, pcts in zip(labels, per_segment_pcts):
        print(f"{name:16}" + "".join(f"{v:12.4f}" for v in pcts))
    print(f"{'total':16}" + "".join(f"{v:12.4f}" for v in totals))


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("input_csv", help="Path to forwarder-timings.log or forwarder-timings-inner.log")
    parser.add_argument("--unit", choices=["ns", "ms"], default="ns",
                         help="Unit of the OUTER log's timestamps (default: ns). Ignored for inner logs, "
                              "which are always nanoseconds by construction.")
    parser.add_argument("-o", "--output", default="waterfall.png", help="Output image path")
    parser.add_argument("--percentiles", type=float, nargs="+", default=[50, 90, 95],
                         help="Percentiles to plot (default: 50 90 95)")
    args = parser.parse_args()

    with open(args.input_csv, newline="") as f:
        fieldnames = csv.DictReader(f).fieldnames

    fmt = detect_format(fieldnames)
    print(f"Detected format: {fmt}", file=sys.stderr)

    if fmt == "outer":
        labels, segments = load_outer_segments(args.input_csv, args.unit)
        title = "Forwarder latency breakdown by segment (outer: receive -> flush)"
    else:
        labels, segments = load_inner_segments(args.input_csv)
        title = "Forwarder latency breakdown by segment (inner: inside forwarder.execute)"

    if not segments[0].size:
        raise ValueError("No valid rows parsed — check the input file's contents.")

    make_waterfall(labels, segments, args.percentiles, args.output, title)


if __name__ == "__main__":
    main()
