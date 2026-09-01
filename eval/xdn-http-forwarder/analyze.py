#!/usr/bin/env python3
"""
analyze.py — parse the raw sample files produced by benchmark.sh and report
p50/p90/p95 latency for each of the two measurement stages.

Usage:
    python3 analyze.py [--ping ping_raw.txt] [--http http_raw.txt]

Percentiles are computed with numpy.percentile (linear interpolation,
numpy's default), both including and excluding the first sample of each
stage. The first sample can be skewed by ARP cache population (ping) or
by the TCP handshake cost folded into curl's first keep-alive request;
see benchmark.sh's header comment for details.
"""

import argparse
import re
import sys

import numpy as np

PING_RE = re.compile(r"time=([\d.]+)\s*ms")
CURL_RE = re.compile(r"time_total=([\d.]+)")


def extract(path, pattern, scale=1.0):
    values = []
    with open(path) as f:
        for line in f:
            m = pattern.search(line)
            if m:
                values.append(float(m.group(1)) * scale)
    return values


def summarize(name, values):
    if not values:
        print(f"{name}: no samples found, skipping\n")
        return

    arr_all = np.array(values)
    arr_no_warmup = arr_all[1:] if len(arr_all) > 1 else arr_all

    def pct_line(arr):
        p50, p90, p95 = np.percentile(arr, [50, 90, 95])
        return f"n={len(arr):>6}  p50={p50:8.3f} ms  p90={p90:8.3f} ms  p95={p95:8.3f} ms"

    print(f"== {name} ==")
    print(f"  including first sample: {pct_line(arr_all)}")
    print(f"  excluding first sample: {pct_line(arr_no_warmup)}")
    print(f"  first sample value:     {arr_all[0]:.3f} ms")
    print()


def main():
    parser = argparse.ArgumentParser(
        description="Summarize benchmark.sh raw output into p50/p90/p95"
    )
    parser.add_argument("--ping", default="ping_raw.txt")
    parser.add_argument("--http", default="http_raw.txt")
    args = parser.parse_args()

    try:
        ping_vals = extract(args.ping, PING_RE)
        summarize("ICMP ping", ping_vals)

        # curl's time_total is reported in seconds; convert to ms for
        # consistency with the other two stages.
        http_vals = extract(args.http, CURL_RE, scale=1000.0)
        summarize("HTTP RTT (curl, keep-alive)", http_vals)
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
