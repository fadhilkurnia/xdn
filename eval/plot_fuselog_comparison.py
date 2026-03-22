#!/usr/bin/env python3
"""
plot_fuselog_comparison.py - Generate comparison figures for C++ vs Rust fuselog.

Reads results from eval/results/fuselog_bench/fuselog_bench_results.json
and generates:
  - fuselog_write_throughput.png: Grouped bar chart per profile, C++ vs Rust
  - fuselog_mt_scaling.png: Throughput vs thread count (the key chart)
  - fuselog_capture_time.png: Statediff capture latency comparison
  - fuselog_sd_size.png: Statediff size distribution
  - fuselog_config_sweep.png: Impact of coalescing/pruning/compression

Usage:
    python3 plot_fuselog_comparison.py [--results-dir DIR] [--output-dir DIR]
"""

import argparse
import json
import os
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np


# Consistent colors for each implementation
COLORS = {
    "cpp": "#2196F3",      # Blue
    "rust-st": "#FF9800",  # Orange
    "rust-mt": "#4CAF50",  # Green
}

LABELS = {
    "cpp": "C++ (multi-threaded FUSE)",
    "rust-st": "Rust (single-threaded)",
    "rust-mt": "Rust (multi-threaded)",
}


def load_results(results_dir):
    """Load benchmark results from JSON."""
    json_path = Path(results_dir) / "fuselog_bench_results.json"
    if not json_path.exists():
        print(f"Error: {json_path} not found")
        sys.exit(1)
    with open(json_path) as f:
        return json.load(f)


def plot_write_throughput(results, output_dir):
    """Grouped bar chart: write throughput per profile."""
    profiles = sorted(set(r["profile"] for r in results))
    impls = sorted(set(r["impl"] for r in results))

    # Filter to threads=1 for fair single-threaded comparison
    st_results = [r for r in results if r["threads"] == 1 or r["profile"] == "mixed"]

    fig, ax = plt.subplots(figsize=(12, 6))

    x = np.arange(len(profiles))
    width = 0.25
    offsets = np.linspace(-width, width, len(impls))

    for i, impl in enumerate(impls):
        values = []
        for profile in profiles:
            matches = [r for r in st_results
                       if r["impl"] == impl and r["profile"] == profile]
            if matches:
                values.append(matches[0].get("w_throughput_mbps", 0))
            else:
                values.append(0)
        ax.bar(x + offsets[i], values, width * 0.9,
               label=LABELS.get(impl, impl), color=COLORS.get(impl, "#999"))

    ax.set_xlabel("Workload Profile")
    ax.set_ylabel("Write Throughput (MB/s)")
    ax.set_title("Fuselog Write Throughput by Profile (single-threaded)")
    ax.set_xticks(x)
    ax.set_xticklabels(profiles, rotation=15)
    ax.legend()
    ax.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    plt.savefig(Path(output_dir) / "fuselog_write_throughput.png", dpi=150)
    plt.close()
    print("  Saved fuselog_write_throughput.png")


def plot_mt_scaling(results, output_dir):
    """Line plot: throughput vs thread count — the key comparison chart."""
    impls = sorted(set(r["impl"] for r in results))
    # Focus on rand-small and seq-large for scaling comparison
    scaling_profiles = ["rand-small", "seq-large"]

    fig, axes = plt.subplots(1, len(scaling_profiles), figsize=(14, 6), sharey=False)
    if len(scaling_profiles) == 1:
        axes = [axes]

    for ax, profile in zip(axes, scaling_profiles):
        for impl in impls:
            matches = sorted(
                [r for r in results
                 if r["impl"] == impl and r["profile"] == profile],
                key=lambda r: r["threads"]
            )
            if not matches:
                continue
            threads = [r["threads"] for r in matches]
            throughput = [r.get("w_throughput_mbps", 0) for r in matches]
            ax.plot(threads, throughput, "o-",
                    label=LABELS.get(impl, impl),
                    color=COLORS.get(impl, "#999"),
                    linewidth=2, markersize=8)

        ax.set_xlabel("Writer Threads")
        ax.set_ylabel("Write Throughput (MB/s)")
        ax.set_title(f"Multi-Threaded Scaling: {profile}")
        ax.legend()
        ax.grid(alpha=0.3)
        ax.set_xticks(sorted(set(r["threads"] for r in results if r["profile"] != "mixed")))

    plt.tight_layout()
    plt.savefig(Path(output_dir) / "fuselog_mt_scaling.png", dpi=150)
    plt.close()
    print("  Saved fuselog_mt_scaling.png")


def plot_capture_time(results, output_dir):
    """Bar chart: statediff capture latency by implementation."""
    profiles = sorted(set(r["profile"] for r in results))
    impls = sorted(set(r["impl"] for r in results))

    # Use threads=1 for comparison
    st_results = [r for r in results if r["threads"] == 1 or r["profile"] == "mixed"]

    fig, ax = plt.subplots(figsize=(12, 6))

    x = np.arange(len(profiles))
    width = 0.25
    offsets = np.linspace(-width, width, len(impls))

    for i, impl in enumerate(impls):
        values = []
        for profile in profiles:
            matches = [r for r in st_results
                       if r["impl"] == impl and r["profile"] == profile]
            if matches:
                lat_us = matches[0].get("c_capture_lat_p50_us", 0)
                values.append(lat_us / 1000)  # Convert to ms
            else:
                values.append(0)
        ax.bar(x + offsets[i], values, width * 0.9,
               label=LABELS.get(impl, impl), color=COLORS.get(impl, "#999"))

    ax.set_xlabel("Workload Profile")
    ax.set_ylabel("Capture Latency p50 (ms)")
    ax.set_title("Statediff Capture Latency by Profile")
    ax.set_xticks(x)
    ax.set_xticklabels(profiles, rotation=15)
    ax.legend()
    ax.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    plt.savefig(Path(output_dir) / "fuselog_capture_time.png", dpi=150)
    plt.close()
    print("  Saved fuselog_capture_time.png")


def plot_sd_size(results, output_dir):
    """Bar chart: statediff payload size by profile."""
    profiles = sorted(set(r["profile"] for r in results))
    impls = sorted(set(r["impl"] for r in results))

    st_results = [r for r in results if r["threads"] == 1 or r["profile"] == "mixed"]

    fig, ax = plt.subplots(figsize=(12, 6))

    x = np.arange(len(profiles))
    width = 0.25
    offsets = np.linspace(-width, width, len(impls))

    for i, impl in enumerate(impls):
        values = []
        for profile in profiles:
            matches = [r for r in st_results
                       if r["impl"] == impl and r["profile"] == profile]
            if matches:
                avg_bytes = matches[0].get("c_avg_payload_bytes", 0)
                values.append(avg_bytes / 1024)  # Convert to KB
            else:
                values.append(0)
        ax.bar(x + offsets[i], values, width * 0.9,
               label=LABELS.get(impl, impl), color=COLORS.get(impl, "#999"))

    ax.set_xlabel("Workload Profile")
    ax.set_ylabel("Average Statediff Size (KB)")
    ax.set_title("Statediff Payload Size by Profile")
    ax.set_xticks(x)
    ax.set_xticklabels(profiles, rotation=15)
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    ax.set_yscale("log")

    plt.tight_layout()
    plt.savefig(Path(output_dir) / "fuselog_sd_size.png", dpi=150)
    plt.close()
    print("  Saved fuselog_sd_size.png")


def plot_ops_scaling(results, output_dir):
    """Line plot: ops/sec vs thread count for create-unlink and many-files."""
    impls = sorted(set(r["impl"] for r in results))
    scaling_profiles = ["create-unlink", "many-files"]

    available = [p for p in scaling_profiles
                 if any(r["profile"] == p for r in results)]
    if not available:
        return

    fig, axes = plt.subplots(1, len(available), figsize=(7 * len(available), 6))
    if len(available) == 1:
        axes = [axes]

    for ax, profile in zip(axes, available):
        for impl in impls:
            matches = sorted(
                [r for r in results
                 if r["impl"] == impl and r["profile"] == profile],
                key=lambda r: r["threads"]
            )
            if not matches:
                continue
            threads = [r["threads"] for r in matches]
            ops = [r.get("w_ops_per_sec", 0) for r in matches]
            ax.plot(threads, ops, "o-",
                    label=LABELS.get(impl, impl),
                    color=COLORS.get(impl, "#999"),
                    linewidth=2, markersize=8)

        ax.set_xlabel("Writer Threads")
        ax.set_ylabel("Operations / second")
        ax.set_title(f"Metadata Ops Scaling: {profile}")
        ax.legend()
        ax.grid(alpha=0.3)

    plt.tight_layout()
    plt.savefig(Path(output_dir) / "fuselog_ops_scaling.png", dpi=150)
    plt.close()
    print("  Saved fuselog_ops_scaling.png")


def main():
    parser = argparse.ArgumentParser(description="Plot fuselog benchmark comparison")
    parser.add_argument("--results-dir",
                        default=str(Path(__file__).parent / "results" / "fuselog_bench"),
                        help="Directory containing fuselog_bench_results.json")
    parser.add_argument("--output-dir", default=None,
                        help="Output directory for plots (default: same as results-dir)")
    args = parser.parse_args()

    output_dir = args.output_dir or args.results_dir
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    print("Loading results...")
    results = load_results(args.results_dir)
    print(f"  Loaded {len(results)} result entries")

    print("\nGenerating plots...")
    plot_write_throughput(results, output_dir)
    plot_mt_scaling(results, output_dir)
    plot_capture_time(results, output_dir)
    plot_sd_size(results, output_dir)
    plot_ops_scaling(results, output_dir)

    print(f"\nAll plots saved to {output_dir}/")


if __name__ == "__main__":
    main()
