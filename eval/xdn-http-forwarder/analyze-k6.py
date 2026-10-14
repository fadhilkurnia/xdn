#!/usr/bin/env python3
"""
analyze-k6.py — analyze a single k6 run (--out json=...) for either:

  tier0: direct-to-container, k6 latency only, no Java log involved.
  tier1: forwarder run, correlates k6's per-request "req_latency" metric
         (tagged with reqId) against the ForwarderFrontend Java timing log,
         and breaks total java-side latency into its three sub-stages.

Usage:
    python3 analyze-k6.py --type tier0 --k6 results/rs630/k6-direct.json
    python3 analyze-k6.py --type tier1 --k6 results/rs630/k6-sync-proxy.json \
                          --log results/rs630/java-sync-proxy.log

Notes:
  - k6's "req_latency" metric is a custom Trend (see write.js), tagged with
    reqId/tier per point. This is what carries the correlation key; the
    built-in http_req_duration metric does not carry reqId and is not used.
  - The Java log's tReceivedMs/tBeforeExecuteMs/tAfterExecuteMs/tFlushedMs
    columns are named "Ms" but are actually raw System.nanoTime() values
    (nanoseconds, arbitrary JVM-relative origin, not wall-clock). All
    durations derived from them are divided by 1e6 to get milliseconds.
    Because nanoTime() has no relation to wall-clock time, correlation
    across the k6 file and the Java log is done purely via reqId — never
    via timestamps.
  - Filtering to "successful" requests uses only the Java log's statusCode
    column (== 200). k6's own tags do not carry status on req_latency
    points in this data, so no k6-side agreement check is attempted.
  - Every tier1 percentile line (k6_latency, k6-java_overhead, java_latency,
    and the three sub-stages) is computed on the exact same filtered/
    matched cohort of requests, so the lines are directly comparable to
    each other.
  - Each of java_latency and the three sub-stages is percentiled
    independently (Option A / per-stage order statistics). Their
    percentiles are NOT expected to sum to java_latency's percentile —
    that's a property of order statistics, not a bug. A diagnostic line
    shows the sum of the three sub-stage percentiles next to java_latency
    purely for visual comparison; it is never used to adjust anything.
  - k6-java_overhead is computed per-request (k6_latency_i - java_latency_i
    for the same reqId) BEFORE percentiling — not
    percentile(k6_latency) - percentile(java_latency), which would subtract
    order statistics from two different requests and not correspond to any
    real request's actual overhead.
"""

import argparse
import csv
import json
import sys

import numpy as np


def parse_k6_file(path):
    """Return (reqid_to_value dict, total_points, duplicate_count)."""
    values = {}
    total_points = 0
    duplicates = 0

    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            if obj.get("metric") != "req_latency" or obj.get("type") != "Point":
                continue

            total_points += 1
            data = obj["data"]
            req_id = data["tags"]["reqId"]
            value = data["value"]

            if req_id in values:
                duplicates += 1
                continue
            values[req_id] = value

    return values, total_points, duplicates


def parse_java_log(path):
    """Return (reqid_to_row dict, duplicate_count).

    Each row is a dict with keys: tReceivedMs, tBeforeExecuteMs,
    tAfterExecuteMs, tFlushedMs, statusCode (raw values as read, still
    nanoseconds despite the column names).
    """
    rows = {}
    duplicates = 0

    with open(path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            req_id = row["reqId"]
            if req_id in rows:
                duplicates += 1
                continue
            rows[req_id] = {
                "tReceivedMs": int(row["tReceivedMs"]),
                "tBeforeExecuteMs": int(row["tBeforeExecuteMs"]),
                "tAfterExecuteMs": int(row["tAfterExecuteMs"]),
                "tFlushedMs": int(row["tFlushedMs"]),
                "statusCode": int(row["statusCode"]),
            }

    return rows, duplicates


def ns_to_ms(ns_delta):
    return ns_delta / 1_000_000.0


def pct(arr):
    p50, p90, p95 = np.percentile(arr, [50, 90, 95])
    return p50, p90, p95


def fmt_pct(label, arr, width=40):
    p50, p90, p95 = pct(arr)
    return f"{label:<{width}}p50={p50:8.3f} ms  p90={p90:8.3f} ms  p95={p95:8.3f} ms"


def main():
    parser = argparse.ArgumentParser(
        description="Analyze a single k6 run, optionally correlated with a ForwarderFrontend Java timing log"
    )
    parser.add_argument("--type", required=True, choices=["tier0", "tier1"])
    parser.add_argument("--k6", required=True, help="Path to k6 --out json=... file")
    parser.add_argument("--log", help="Path to ForwarderFrontend timing log (required for tier1)")
    args = parser.parse_args()

    if args.type == "tier0" and args.log:
        print("Error: --log must not be passed with --type tier0 (no Java layer to correlate against)", file=sys.stderr)
        sys.exit(1)
    if args.type == "tier1" and not args.log:
        print("Error: --log is required with --type tier1", file=sys.stderr)
        sys.exit(1)

    try:
        k6_values, total_requests, k6_dupes = parse_k6_file(args.k6)
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    if args.type == "tier0":
        print(f"total_requests={total_requests}")
        print(fmt_pct("k6_latency:", list(k6_values.values())))

        if k6_dupes:
            print(f"WARNING: {k6_dupes} duplicate reqId in k6 file (kept first occurrence)", file=sys.stderr)
        return

    # tier1
    try:
        java_rows, java_dupes = parse_java_log(args.log)
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    unmatched = 0
    non_200 = 0

    k6_latency_vals = []
    java_latency_vals = []
    overhead_vals = []
    stage1_vals = []  # netty -> XdnHttpForwarder
    stage2_vals = []  # XdnHttpForwarder <-> container
    stage3_vals = []  # netty flush response

    for req_id, row in java_rows.items():
        if req_id not in k6_values:
            unmatched += 1
            continue
        if row["statusCode"] != 200:
            non_200 += 1
            continue

        k6_latency = k6_values[req_id]
        java_latency = ns_to_ms(row["tFlushedMs"] - row["tReceivedMs"])
        stage1 = ns_to_ms(row["tBeforeExecuteMs"] - row["tReceivedMs"])
        stage2 = ns_to_ms(row["tAfterExecuteMs"] - row["tBeforeExecuteMs"])
        stage3 = ns_to_ms(row["tFlushedMs"] - row["tAfterExecuteMs"])
        overhead = k6_latency - java_latency

        k6_latency_vals.append(k6_latency)
        java_latency_vals.append(java_latency)
        overhead_vals.append(overhead)
        stage1_vals.append(stage1)
        stage2_vals.append(stage2)
        stage3_vals.append(stage3)

    matching_requests = len(k6_latency_vals)

    print(f"total_requests={total_requests}")
    print(f"matching_requests={matching_requests}")

    if matching_requests == 0:
        print("No matching requests survived filtering; nothing to percentile.", file=sys.stderr)
        sys.exit(1)

    print(fmt_pct("k6_latency:", k6_latency_vals))
    print(fmt_pct("k6-java_overhead:", overhead_vals))
    print(fmt_pct("java_latency:", java_latency_vals))

    s1_p50, s1_p90, s1_p95 = pct(stage1_vals)
    s2_p50, s2_p90, s2_p95 = pct(stage2_vals)
    s3_p50, s3_p90, s3_p95 = pct(stage3_vals)
    sum_p50 = s1_p50 + s2_p50 + s3_p50
    sum_p90 = s1_p90 + s2_p90 + s3_p90
    sum_p95 = s1_p95 + s2_p95 + s3_p95
    print(
        f"{'  (sum of sub-stage p-values:':<40}"
        f"p50={sum_p50:8.3f} ms  p90={sum_p90:8.3f} ms  p95={sum_p95:8.3f} ms)"
    )

    print(fmt_pct("netty -> XdnHttpForwarder:", stage1_vals))
    print(fmt_pct("XdnHttpForwarder <-> container:", stage2_vals))
    print(fmt_pct("netty flush response:", stage3_vals))

    dropped = unmatched + non_200
    warning_parts = []
    if dropped:
        warning_parts.append(f"{dropped} dropped ({unmatched} unmatched reqId, {non_200} non-200 status)")
    if k6_dupes:
        warning_parts.append(f"{k6_dupes} duplicate reqId in k6 file")
    if java_dupes:
        warning_parts.append(f"{java_dupes} duplicate reqId in java log")

    if warning_parts:
        print("WARNING: " + ", ".join(warning_parts) + " (duplicates: kept first occurrence)", file=sys.stderr)


if __name__ == "__main__":
    main()
