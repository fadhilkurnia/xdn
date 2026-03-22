#!/usr/bin/env python3
"""Generate CSV files from Go load test rate files for each application."""

import os
import re
import csv
import glob

RESULTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")

# Fields to extract from rate files (in order)
FIELDS = [
    "total_requests_sent",
    "total_successful_responses",
    "actual_achieved_rate_rps",
    "actual_throughput_rps",
    "min_latency_ms",
    "max_latency_ms",
    "average_latency_ms",
    "median_latency_ms",
    "p90_latency_ms",
    "p95_latency_ms",
    "p99_latency_ms",
]

CSV_HEADER = [
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

# AR (deterministic apps) data sources: {app: [(system_name, directory_path), ...]}
AR_SOURCES = {
    "bookcatalog": [
        ("xdn_ar", os.path.join(RESULTS_DIR, "reflex_bookcatalog")),
        ("openebs", os.path.join(RESULTS_DIR, "oebs_bookcatalog")),
        ("rqlite", os.path.join(RESULTS_DIR, "distdb_bookcatalog")),
    ],
    "todo": [
        ("xdn_ar", os.path.join(RESULTS_DIR, "reflex_todogo")),
        ("openebs", os.path.join(RESULTS_DIR, "oebs_todo")),
        ("rqlite", os.path.join(RESULTS_DIR, "distdb_todogo")),
    ],
    "webkv": [
        ("xdn_ar", os.path.join(RESULTS_DIR, "reflex_webkv")),
        ("openebs", os.path.join(RESULTS_DIR, "oebs_webkv")),
        ("tikv", os.path.join(RESULTS_DIR, "distdb_webkv")),
    ],
}

# PB (non-deterministic apps) data sources
PB_SOURCES = {
    "bookcatalog-nd": [
        ("xdn_pb", os.path.join(RESULTS_DIR, "load_pb_bookcatalog_reflex")),
        ("openebs", os.path.join(RESULTS_DIR, "load_pb_bookcatalog_openebs")),
        ("rqlite", os.path.join(RESULTS_DIR, "load_pb_bookcatalog_rqlite")),
        ("criu", os.path.join(RESULTS_DIR, "load_pb_bookcatalog_criu")),
    ],
    "wordpress": [
        ("xdn_pb", os.path.join(RESULTS_DIR, "load_pb_wordpress_reflex")),
        ("openebs", os.path.join(RESULTS_DIR, "load_pb_wordpress_openebs")),
        ("semisync", os.path.join(RESULTS_DIR, "load_pb_wordpress_semisync")),
    ],
    "tpcc": [
        ("xdn_pb", os.path.join(RESULTS_DIR, "load_pb_tpcc_reflex")),
        ("openebs", os.path.join(RESULTS_DIR, "load_pb_tpcc_openebs")),
        ("pgsync", os.path.join(RESULTS_DIR, "load_pb_tpcc_pgsync")),
    ],
}


def parse_rate_file(filepath):
    """Parse a Go load test rate file and extract metrics."""
    with open(filepath, "r") as f:
        content = f.read()

    result = {}
    for field in FIELDS:
        pattern = rf"^{re.escape(field)}:\s*(.+)$"
        match = re.search(pattern, content, re.MULTILINE)
        if match:
            result[field] = match.group(1).strip()
        else:
            result[field] = ""

    return result


def extract_target_rate(filename):
    """Extract target rate from filename like rate100.txt -> 100."""
    match = re.search(r"rate(\d+)\.txt$", filename)
    if match:
        return int(match.group(1))
    return None


def collect_rows(system_name, directory):
    """Collect all rate file data from a directory for a given system."""
    rows = []
    if not os.path.exists(directory):
        print(f"  WARNING: directory not found: {directory}")
        return rows

    rate_files = glob.glob(os.path.join(directory, "rate*.txt"))
    for filepath in rate_files:
        filename = os.path.basename(filepath)
        # Skip warmup files
        if filename.startswith("warmup_"):
            continue
        target_rate = extract_target_rate(filename)
        if target_rate is None:
            continue

        metrics = parse_rate_file(filepath)
        row = {
            "system": system_name,
            "target_load_rps": target_rate,
            "achieved_load_rps": metrics.get("actual_achieved_rate_rps", ""),
            "actual_throughput_rps": metrics.get("actual_throughput_rps", ""),
            "total_requests_sent": metrics.get("total_requests_sent", ""),
            "total_successful_responses": metrics.get("total_successful_responses", ""),
            "min_latency_ms": metrics.get("min_latency_ms", ""),
            "max_latency_ms": metrics.get("max_latency_ms", ""),
            "average_latency_ms": metrics.get("average_latency_ms", ""),
            "median_latency_ms": metrics.get("median_latency_ms", ""),
            "p90_latency_ms": metrics.get("p90_latency_ms", ""),
            "p95_latency_ms": metrics.get("p95_latency_ms", ""),
            "p99_latency_ms": metrics.get("p99_latency_ms", ""),
        }
        rows.append(row)

    return rows


def write_csv(filepath, rows):
    """Write rows to a CSV file, sorted by system then target_load_rps."""
    rows.sort(key=lambda r: (r["system"], int(r["target_load_rps"])))
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADER)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Written {len(rows)} rows to {os.path.basename(filepath)}")


def generate_csvs(sources, prefix):
    """Generate CSVs for a set of app sources."""
    eval_dir = os.path.dirname(os.path.abspath(__file__))
    for app, system_dirs in sources.items():
        all_rows = []
        print(f"\n{prefix}_{app}:")
        for system_name, directory in system_dirs:
            rows = collect_rows(system_name, directory)
            if rows:
                print(f"  {system_name}: {len(rows)} rate points from {os.path.basename(directory)}")
            all_rows.extend(rows)

        if all_rows:
            csv_path = os.path.join(RESULTS_DIR, f"{prefix}_{app}.csv")
            write_csv(csv_path, all_rows)
        else:
            print(f"  WARNING: no data found for {app}")


def main():
    print("Generating AR CSVs (deterministic apps)...")
    generate_csvs(AR_SOURCES, "eval_load_ar")

    print("\nGenerating PB CSVs (non-deterministic apps)...")
    generate_csvs(PB_SOURCES, "eval_load_pb")


if __name__ == "__main__":
    main()
