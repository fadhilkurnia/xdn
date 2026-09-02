#!/usr/bin/env python3
"""
visualize-latency.py — render horizontal stacked-bar latency waterfalls
comparing arbitrary combinations of ping/curl baselines and k6+Java tier
breakdowns, one PNG per percentile (p50/p90/p95).

Usage (repeated --label groups, order preserved top-to-bottom):

    python3 visualize-latency.py \\
        --label ping --type ping --log ping_raw.txt \\
        --label cURL --type http --log http_raw.txt \\
        --label direct --type tier0 --k6 k6-direct.json \\
        --label sync-proxy --type tier1 --k6 k6-sync-proxy.json --log java-sync-proxy.log

--type ping / http : requires --log (raw ping/curl output), no --k6.
                      Uses the "excluding first sample" convention from
                      analyze.py (first sample can be skewed by ARP
                      resolution or the initial TCP handshake).
--type tier0        : requires --k6 only (direct-to-container, no Java
                      layer to correlate against). Hard errors if --log
                      is also given.
--type tier1        : requires both --k6 and --log. Reuses the join/
                      filter/derive pipeline from analyze-k6.py: match by
                      reqId, drop unmatched and non-200 status, then
                      break java_latency into three sub-stages.

Output: latency_waterfall_p50.png, latency_waterfall_p90.png,
latency_waterfall_p95.png (fixed filenames, written to the current
directory).

Notes:
  - Every stage (k6_latency, client_overhead, and the three sub-stages)
    is percentiled independently (Option A / per-stage order statistics).
    They are NOT expected to sum to k6_latency's own percentile at that
    same percentile -- the equation printed under each tier1 bar shows
    the real measured k6_latency percentile on the right of "=", so any
    gap between the sum of segments and that total is directly visible
    in the numbers rather than hidden.
  - client_overhead (k6_latency - java_latency, computed per-request
    before percentiling) is drawn as the leftmost segment of the tier1
    stack, but it is not really a contiguous interval -- it's the sum of
    time before netty received the request and time after netty flushed
    the response. See the footnote on the figure.
"""

import csv
import json
import re
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import numpy as np

PING_RE = re.compile(r"time=([\d.]+)\s*ms")
CURL_RE = re.compile(r"time_total=([\d.]+)")

TIER1_SEGMENTS = ["overhead", "dispatch", "roundtrip", "flush"]
TIER1_LABELS = {
    "overhead": "client_overhead",
    "dispatch": "netty <-> XdnHttpForwarder",
    "roundtrip": "XdnHttpForwarder <-> container",
    "flush": "netty flush response",
}
TIER1_COLORS = {
    "overhead": "#6250d6",
    "dispatch": "#2a78d6",
    "roundtrip": "#eb6834",
    "flush": "#1baf7a",
}
SINGLE_PALETTE = ["#eda100", "#e87ba4", "#008300", "#e34948", "#9085e9", "#199e70"]

TEXT_SECONDARY = "#52514e"
TEXT_PRIMARY = "#0b0b0b"


# ---------------------------------------------------------------------------
# CLI parsing (repeated --label groups)
# ---------------------------------------------------------------------------

def parse_args(argv):
    groups = []
    current = None
    i = 0
    while i < len(argv):
        tok = argv[i]
        if tok == "--label":
            if current is not None:
                groups.append(current)
            current = {"label": argv[i + 1]}
            i += 2
        elif tok in ("--type", "--k6", "--log"):
            if current is None:
                sys.exit(f"Error: {tok} given before any --label")
            current[tok[2:]] = argv[i + 1]
            i += 2
        else:
            sys.exit(f"Error: unknown argument {tok}")
    if current is not None:
        groups.append(current)

    if not groups:
        sys.exit("Error: at least one --label group is required")

    for g in groups:
        if "type" not in g:
            sys.exit(f"Error: --label {g['label']} is missing --type")
        t = g["type"]
        if t not in ("ping", "http", "tier0", "tier1"):
            sys.exit(f"Error: --label {g['label']} has unknown --type {t}")
        if t in ("ping", "http"):
            if "log" not in g:
                sys.exit(f"Error: --label {g['label']} (type {t}) requires --log")
            if "k6" in g:
                sys.exit(f"Error: --label {g['label']} (type {t}) must not have --k6")
        elif t == "tier0":
            if "k6" not in g:
                sys.exit(f"Error: --label {g['label']} (type tier0) requires --k6")
            if "log" in g:
                sys.exit(f"Error: --label {g['label']} (type tier0) must not have --log")
        elif t == "tier1":
            if "k6" not in g or "log" not in g:
                sys.exit(f"Error: --label {g['label']} (type tier1) requires both --k6 and --log")

    return groups


# ---------------------------------------------------------------------------
# Raw file parsing (duplicated from analyze.py / analyze-k6.py by design)
# ---------------------------------------------------------------------------

def extract(path, pattern, scale=1.0):
    values = []
    with open(path) as f:
        for line in f:
            m = pattern.search(line)
            if m:
                values.append(float(m.group(1)) * scale)
    return values


def parse_ping(path):
    values = extract(path, PING_RE)
    return values[1:] if len(values) > 1 else values


def parse_http(path):
    values = extract(path, CURL_RE, scale=1000.0)
    return values[1:] if len(values) > 1 else values


def parse_k6_file(path):
    values = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            if obj.get("metric") != "req_latency" or obj.get("type") != "Point":
                continue
            data = obj["data"]
            req_id = data["tags"]["reqId"]
            if req_id not in values:
                values[req_id] = data["value"]
    return values


def parse_java_log(path):
    rows = {}
    with open(path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            req_id = row["reqId"]
            if req_id not in rows:
                rows[req_id] = {
                    "tReceivedMs": int(row["tReceivedMs"]),
                    "tBeforeExecuteMs": int(row["tBeforeExecuteMs"]),
                    "tAfterExecuteMs": int(row["tAfterExecuteMs"]),
                    "tFlushedMs": int(row["tFlushedMs"]),
                    "statusCode": int(row["statusCode"]),
                }
    return rows


def ns_to_ms(ns_delta):
    return ns_delta / 1_000_000.0


def build_tier1_arrays(label, k6_path, log_path):
    k6_values = parse_k6_file(k6_path)
    java_rows = parse_java_log(log_path)

    arrays = {"k6_latency": [], "overhead": [], "dispatch": [], "roundtrip": [], "flush": []}
    unmatched = 0
    non_200 = 0

    for req_id, row in java_rows.items():
        if req_id not in k6_values:
            unmatched += 1
            continue
        if row["statusCode"] != 200:
            non_200 += 1
            continue

        k6_latency = k6_values[req_id]
        java_latency = ns_to_ms(row["tFlushedMs"] - row["tReceivedMs"])
        dispatch = ns_to_ms(row["tBeforeExecuteMs"] - row["tReceivedMs"])
        roundtrip = ns_to_ms(row["tAfterExecuteMs"] - row["tBeforeExecuteMs"])
        flush = ns_to_ms(row["tFlushedMs"] - row["tAfterExecuteMs"])
        overhead = k6_latency - java_latency

        arrays["k6_latency"].append(k6_latency)
        arrays["overhead"].append(overhead)
        arrays["dispatch"].append(dispatch)
        arrays["roundtrip"].append(roundtrip)
        arrays["flush"].append(flush)

    if unmatched or non_200:
        print(
            f"WARNING [{label}]: dropped {unmatched + non_200} "
            f"({unmatched} unmatched reqId, {non_200} non-200 status)",
            file=sys.stderr,
        )
    if not arrays["k6_latency"]:
        sys.exit(f"Error: no matching requests survived filtering for --label {label}")

    return arrays


# ---------------------------------------------------------------------------
# Load all group data once (percentiles computed later, per figure)
# ---------------------------------------------------------------------------

def load_groups(groups):
    loaded = []
    single_color_by_label = {}
    next_color_idx = 0

    for g in groups:
        label = g["label"]
        t = g["type"]

        if t == "ping":
            values = parse_ping(g["log"])
            entry = {"label": label, "type": "single", "values": values}
        elif t == "http":
            values = parse_http(g["log"])
            entry = {"label": label, "type": "single", "values": values}
        elif t == "tier0":
            values = list(parse_k6_file(g["k6"]).values())
            entry = {"label": label, "type": "single", "values": values}
        else:  # tier1
            arrays = build_tier1_arrays(label, g["k6"], g["log"])
            entry = {"label": label, "type": "tier1", "arrays": arrays}

        if entry["type"] == "single":
            if label not in single_color_by_label:
                single_color_by_label[label] = SINGLE_PALETTE[next_color_idx % len(SINGLE_PALETTE)]
                next_color_idx += 1
            entry["color"] = single_color_by_label[label]

        loaded.append(entry)

    return loaded


# ---------------------------------------------------------------------------
# Multi-color text (equation lines), measured after an initial draw
# ---------------------------------------------------------------------------

def place_multicolor_text(ax, fig, x0, y, parts, fontsize=11):
    texts = []
    for text, color in parts:
        t = ax.text(0, y, text, fontsize=fontsize, color=color, va="top", ha="left")
        texts.append(t)

    fig.canvas.draw()
    renderer = fig.canvas.get_renderer()

    x = x0
    for t in texts:
        bbox = t.get_window_extent(renderer=renderer)
        bbox_data = bbox.transformed(ax.transData.inverted())
        width = bbox_data.x1 - bbox_data.x0
        t.set_position((x, y))
        x += width


# ---------------------------------------------------------------------------
# Figure builder for a single percentile
# ---------------------------------------------------------------------------

def make_figure(loaded, percentile, filename):
    n = len(loaded)
    row_height_in = 1.00
    top_margin_in = 1.1
    bottom_margin_in = 0.85
    fig_width_in = 11
    fig_height_in = top_margin_in + bottom_margin_in + row_height_in * n

    fig, ax = plt.subplots(figsize=(fig_width_in, fig_height_in))
    fig.subplots_adjust(
        top=1 - top_margin_in / fig_height_in,
        bottom=bottom_margin_in / fig_height_in,
        left=0.03,
        right=0.97,
    )

    max_val = 0.0
    rendered = []  # (row_y, entry, bar_value_or_segments, total_for_equation)

    for i, entry in enumerate(loaded):
        row_y = n - 1 - i
        if entry["type"] == "single":
            val = float(np.percentile(entry["values"], percentile))
            max_val = max(max_val, val)
            rendered.append((row_y, entry, val, None))
        else:
            arrs = entry["arrays"]
            seg_vals = {seg: float(np.percentile(arrs[seg], percentile)) for seg in TIER1_SEGMENTS}
            total = float(np.percentile(arrs["k6_latency"], percentile))
            max_val = max(max_val, sum(seg_vals.values()), total)
            rendered.append((row_y, entry, seg_vals, total))

    x_max = max_val * 1.15 if max_val > 0 else 1.0
    ax.set_xlim(0, x_max)
    ax.set_ylim(-0.8, n - 0.3)

    for row_y, entry, val, total in rendered:
        label_y = row_y + 0.18
        eq_y = row_y - 0.20
        bar_y = row_y

        ax.text(0, label_y, entry["label"], fontsize=13, fontweight="medium",
                va="bottom", ha="left", color=TEXT_PRIMARY)

        if entry["type"] == "single":
            place_multicolor_text(ax, fig, 0, eq_y, [(f"{val:.3f} ms", TEXT_SECONDARY)], fontsize=11)
            ax.barh(bar_y, width=val, height=0.32, left=0, color=entry["color"])
        else:
            parts = []
            for idx, seg in enumerate(TIER1_SEGMENTS):
                parts.append((f"{val[seg]:.3f}", TIER1_COLORS[seg]))
                if idx < len(TIER1_SEGMENTS) - 1:
                    parts.append((" + ", TEXT_SECONDARY))
            parts.append((" = ", TEXT_SECONDARY))
            parts.append((f"{total:.3f} ms", TEXT_PRIMARY))
            place_multicolor_text(ax, fig, 0, eq_y, parts, fontsize=11)

            x_cursor = 0.0
            for seg in TIER1_SEGMENTS:
                w = val[seg]
                ax.barh(bar_y, width=w, height=0.32, left=x_cursor,
                        color=TIER1_COLORS[seg], edgecolor="white", linewidth=1.0)
                x_cursor += w

    ax.set_yticks([])
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_visible(False)
    ax.set_xlabel(f"Latency at p{percentile} (ms)")

    has_tier1 = any(e["type"] == "tier1" for e in loaded)

    legend_handles = []
    seen_single_labels = set()
    for entry in loaded:
        if entry["type"] == "single" and entry["label"] not in seen_single_labels:
            legend_handles.append(Patch(color=entry["color"], label=entry["label"]))
            seen_single_labels.add(entry["label"])
    if has_tier1:
        for seg in TIER1_SEGMENTS:
            legend_handles.append(Patch(color=TIER1_COLORS[seg], label=TIER1_LABELS[seg]))

    ncol = min(4, len(legend_handles)) or 1
    fig.legend(handles=legend_handles, loc="upper center", bbox_to_anchor=(0.5, 1.0),
               ncol=ncol, frameon=False, fontsize=10)

    if has_tier1:
        fig.text(
            0.01, 0.01,
            "*client_overhead spans both pre-receive and post-flush time; not a contiguous interval.",
            fontsize=8, color=TEXT_SECONDARY,
        )
        fig.text(
            0.01, 0.035,
            "*netty -> XdnHttpForwarder is blocking in sync and is non-blocking in async.",
            fontsize=8, color=TEXT_SECONDARY,
        )


    fig.savefig(filename, dpi=150)
    plt.close(fig)


def main():
    groups = parse_args(sys.argv[1:])
    loaded = load_groups(groups)

    for percentile, suffix in [(50, "p50"), (90, "p90"), (95, "p95")]:
        filename = f"latency_waterfall_{suffix}.png"
        make_figure(loaded, percentile, filename)
        print(f"wrote {filename}")


if __name__ == "__main__":
    main()
