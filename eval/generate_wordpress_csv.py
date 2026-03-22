"""
generate_wordpress_csv.py — Generate CSV comparison files from WordPress benchmark results.

Reads the 3 WordPress result symlink directories (load_pb_wordpress_reflex,
load_pb_wordpress_openebs, load_pb_wordpress_semisync) and produces CSV files
comparing throughput and latency percentiles.

Output files (in eval/results/):
    wordpress_comparison_throughput_rps.csv
    wordpress_comparison_avg_latency_ms.csv
    wordpress_comparison_p50_latency_ms.csv
    wordpress_comparison_p90_latency_ms.csv
    wordpress_comparison_p95_latency_ms.csv
    wordpress_comparison_p99_latency_ms.csv

Usage (run from eval/ directory):
    python3 generate_wordpress_csv.py [--results-base PATH]
"""

import argparse
import csv
from pathlib import Path

from plot_load_latency import discover_rates, load_all

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_RESULTS_BASE = SCRIPT_DIR / "results"

# (subdir, column_label)
SYSTEMS = [
    ("load_pb_wordpress_reflex",   "XDN PB"),
    ("load_pb_wordpress_openebs",  "OpenEBS"),
    ("load_pb_wordpress_semisync", "Semi-sync"),
]

# (metric_key, filename)
METRICS = [
    ("throughput_rps", "wordpress_comparison_throughput_rps.csv"),
    ("avg_ms",         "wordpress_comparison_avg_latency_ms.csv"),
    ("p50_ms",         "wordpress_comparison_p50_latency_ms.csv"),
    ("p90_ms",         "wordpress_comparison_p90_latency_ms.csv"),
    ("p95_ms",         "wordpress_comparison_p95_latency_ms.csv"),
    ("p99_ms",         "wordpress_comparison_p99_latency_ms.csv"),
]


def load_system_data(results_base: Path, subdir: str) -> dict:
    """Load rate→metrics dict for a system. Returns {rate: row_dict}."""
    d = results_base / subdir
    if not d.exists():
        return {}
    rates = discover_rates(d)
    if not rates:
        return {}
    rows = load_all(d, rates)
    return {row["offered_rps"]: row for row in rows}


def generate_csv(results_base: Path, metric_key: str, filename: str,
                 all_data: dict, all_rates: list) -> None:
    out_path = results_base / filename
    with open(out_path, "w", newline="") as f:
        writer = csv.writer(f)
        header = ["rate"] + [label for _, label in SYSTEMS]
        writer.writerow(header)
        for rate in all_rates:
            row = [rate]
            for subdir, _ in SYSTEMS:
                data = all_data.get(subdir, {})
                if rate in data:
                    row.append(f"{data[rate].get(metric_key, ''):.4f}")
                else:
                    row.append("")
            writer.writerow(row)
    print(f"  -> {out_path}")


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--results-base", type=Path, default=DEFAULT_RESULTS_BASE,
                   help="Base directory with result subdirs (default: eval/results/)")
    args = p.parse_args()

    print("Loading WordPress benchmark data ...")
    all_data = {}
    all_rates_set = set()
    for subdir, label in SYSTEMS:
        data = load_system_data(args.results_base, subdir)
        all_data[subdir] = data
        all_rates_set.update(data.keys())
        print(f"  {label:12s}: {len(data)} rate points from {subdir}/")

    if not all_rates_set:
        print("No data found. Make sure symlinks exist (e.g., results/load_pb_wordpress_reflex).")
        return

    all_rates = sorted(all_rates_set)

    print(f"\nGenerating CSV files for {len(all_rates)} rate points ...")
    for metric_key, filename in METRICS:
        generate_csv(args.results_base, metric_key, filename, all_data, all_rates)

    print("\nDone.")


if __name__ == "__main__":
    main()
