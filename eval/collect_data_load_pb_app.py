#!/usr/bin/env python3
"""
collect_data_load_pb_app.py — Collect primary-backup benchmark results
from XDN, rqlite, OpenEBS, and CRIU into a single CSV.

Scans eval/results/ for the latest timestamped result directories for each
system, parses the per-rate latency .txt files (output of get_latency_at_rate.go),
and writes a combined CSV.

Usage:
    python3 collect_data_load_pb_app.py [--app bookcatalog] [--output results/eval_load_pb_bookcatalog.csv]
"""

import argparse
import csv
import re
import sys
from pathlib import Path

RESULTS_DIR = Path(__file__).resolve().parent / "results"

FIELDNAMES = [
    "system",
    "target_load_rps",
    "achieved_load_rps",
    "actual_throughput_rps",
    "total_requests_sent",
    "total_successful_responses",
    "min_latency_ms",
    "max_latency_ms",
    "average_latency_ms",
    "median_latency_ms",
    "p90_latency_ms",
    "p95_latency_ms",
    "p99_latency_ms",
]

# Mapping from metric keys in get_latency_at_rate.go output to CSV field names.
METRIC_KEY_MAP = {
    "actual_achieved_rate_rps": "achieved_load_rps",
    "actual_throughput_rps": "actual_throughput_rps",
    "total_requests_sent": "total_requests_sent",
    "total_successful_responses": "total_successful_responses",
    "min_latency_ms": "min_latency_ms",
    "max_latency_ms": "max_latency_ms",
    "average_latency_ms": "average_latency_ms",
    "median_latency_ms": "median_latency_ms",
    "p90_latency_ms": "p90_latency_ms",
    "p95_latency_ms": "p95_latency_ms",
    "p99_latency_ms": "p99_latency_ms",
}


def parse_latency_file(filepath):
    """Parse a get_latency_at_rate.go output file into a dict of metrics."""
    metrics = {}
    with open(filepath, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("---"):
                break
            if ":" not in line:
                continue
            key, value = line.split(":", 1)
            key = key.strip()
            value = value.strip()
            if key in METRIC_KEY_MAP:
                try:
                    metrics[METRIC_KEY_MAP[key]] = float(value)
                except ValueError:
                    pass
    return metrics


def find_latest_dir(pattern):
    """Find the latest timestamped directory matching a glob pattern."""
    matches = sorted(RESULTS_DIR.glob(pattern))
    return matches[-1] if matches else None


def extract_rate_from_filename(filename, patterns):
    """Extract the target rate from a result filename using multiple regex patterns."""
    for pat in patterns:
        m = re.search(pat, filename)
        if m:
            return int(m.group(1))
    return None


def collect_system(system_name, dir_pattern, file_glob, rate_patterns):
    """Collect all rate results for a system."""
    result_dir = find_latest_dir(dir_pattern)
    if result_dir is None:
        print(f"  {system_name}: no results found matching {dir_pattern}", file=sys.stderr)
        return []

    print(f"  {system_name}: {result_dir.name}")
    rows = []
    for filepath in sorted(result_dir.glob(file_glob)):
        rate = extract_rate_from_filename(filepath.name, rate_patterns)
        if rate is None:
            continue
        metrics = parse_latency_file(filepath)
        if not metrics:
            continue
        row = {"system": system_name, "target_load_rps": rate}
        for field in FIELDNAMES:
            if field not in row:
                row[field] = metrics.get(field, "")
        rows.append(row)

    rows.sort(key=lambda r: r["target_load_rps"])
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--app", default="bookcatalog",
                        help="Application name (default: bookcatalog)")
    parser.add_argument("--output", default=None,
                        help="Output CSV path (default: results/eval_load_pb_{app}.csv)")
    args = parser.parse_args()

    app = args.app
    output_path = Path(args.output) if args.output else RESULTS_DIR / f"eval_load_pb_{app}.csv"

    print(f"Collecting primary-backup results for '{app}':")

    all_rows = []

    # XDN PB (reflex)
    all_rows.extend(collect_system(
        "xdn_pb",
        f"load_pb_{app}_reflex_*",
        "rate*.txt",
        [r"rate(\d+)\.txt"],
    ))

    # rqlite
    all_rows.extend(collect_system(
        "rqlite",
        f"load_pb_{app}_rqlite_*",
        "rate*.txt",
        [r"rate(\d+)\.txt"],
    ))

    # OpenEBS
    all_rows.extend(collect_system(
        "openebs",
        f"load_pb_{app}_openebs_*",
        "rate*.txt",
        [r"rate(\d+)\.txt"],
    ))

    # CRIU
    all_rows.extend(collect_system(
        "criu",
        f"load_pb_{app}_criu_*",
        "rate*.txt",
        [r"rate(\d+)\.txt"],
    ))

    # MySQL semi-sync replication (WordPress)
    all_rows.extend(collect_system(
        "semisync",
        f"load_pb_{app}_semisync_*",
        "rate*.txt",
        [r"rate(\d+)\.txt"],
    ))

    if not all_rows:
        print("No results found!", file=sys.stderr)
        sys.exit(1)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDNAMES)
        writer.writeheader()
        for row in all_rows:
            writer.writerow(row)

    print(f"\nWritten {len(all_rows)} rows to {output_path}")
    for system in ["xdn_pb", "rqlite", "openebs", "criu", "semisync"]:
        count = sum(1 for r in all_rows if r["system"] == system)
        if count:
            print(f"  {system}: {count} rate points")


if __name__ == "__main__":
    main()
