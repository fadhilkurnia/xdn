"""
plot_tpcc_pb_investigation.py — 5-way TPC-C PB investigation comparison.

Reads rate*.txt files from five results directories and generates:
  1. Latency plot: mean latency vs offered load for all 5 configs
  2. Throughput plot: actual vs offered throughput with y=x reference

Datasets:
  - PB baseline (tpcc_pb/)
  - PG Sync-Rep baseline (tpcc_pgsync/)
  - PB unreplicated (tpcc_pb_unreplicated/)
  - PB RSYNC recorder (tpcc_pb_rsync/)
  - PB batched (tpcc_pb_batched/)

Usage (run from eval/ directory):
    python3 plot_tpcc_pb_investigation.py [--results-base PATH]
"""

import argparse
import datetime
import math
from pathlib import Path

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.ticker as ticker
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False

from plot_load_latency import discover_rates, load_all

# ── Constants ─────────────────────────────────────────────────────────────────

SCRIPT_DIR = Path(__file__).resolve().parent

DATASETS = [
    # (key, label, color, results_subdir)
    ("pb",           "XDN PB (baseline)",   "#4C72B0", "tpcc_pb"),
    ("pgsync",       "PG Sync-Rep",         "#55A868", "tpcc_pgsync"),
    ("unreplicated", "XDN Unreplicated",    "#C44E52", "tpcc_pb_unreplicated"),
    ("rsync",        "XDN PB (RSYNC)",      "#8172B2", "tpcc_pb_rsync"),
    ("batched",      "XDN PB (batched 10ms)", "#CCB974", "tpcc_pb_batched"),
]

# Also try fine-sweep version of baseline if available
FINESWEEP_SUBDIR = "tpcc_pb_finesweep"


# ── Helpers ───────────────────────────────────────────────────────────────────

def load_dataset(results_base: Path, subdir: str) -> list:
    d = results_base / subdir
    if not d.exists():
        return []
    rates = discover_rates(d)
    if not rates:
        return []
    return load_all(d, rates)


def union_rates(datasets: dict) -> list:
    """Return the sorted union of offered rates across all datasets."""
    all_rates = set()
    for rows in datasets.values():
        for row in rows:
            all_rates.add(row["offered_rps"])
    return sorted(all_rates)


# ── Plot 1: Latency ──────────────────────────────────────────────────────────

def plot_latency(datasets: dict, out: Path) -> None:
    if not HAS_MATPLOTLIB:
        print("WARNING: matplotlib not installed — cannot generate plot.")
        return

    markers = {
        "pb": "o", "pgsync": "^", "unreplicated": "D",
        "rsync": "s", "batched": "v", "finesweep": "p",
    }
    colors = {d[0]: d[2] for d in DATASETS}
    labels = {d[0]: d[1] for d in DATASETS}

    fig, ax = plt.subplots(figsize=(10, 5))

    for key, label, color, _ in DATASETS:
        rows = datasets.get(key, [])
        if not rows:
            continue
        offered = [r["offered_rps"] for r in rows]
        avg     = [r["avg_ms"]      for r in rows]
        marker  = markers.get(key, "o")

        ax.plot(offered, avg, f"{marker}-",
                color=color, linewidth=2,
                label=label, markersize=6)

    # Plot finesweep if available and different from baseline
    finesweep = datasets.get("finesweep", [])
    if finesweep:
        offered = [r["offered_rps"] for r in finesweep]
        avg     = [r["avg_ms"]      for r in finesweep]
        ax.plot(offered, avg, "p--",
                color="#4C72B0", linewidth=1.5, alpha=0.6,
                label="XDN PB (fine sweep)", markersize=5)

    ax.set_xlabel("Offered Load (req/s)", fontsize=12)
    ax.set_ylabel("Latency (ms)", fontsize=12)
    ax.set_yscale("linear")
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(
        lambda v, _: f"{v:.0f}"
    ))
    ax.set_ylim(bottom=0, top=5000)
    ax.grid(axis="both", alpha=0.25, linestyle="--")
    ax.legend(loc="upper left", fontsize=8, framealpha=0.9, ncol=1)
    ax.set_title(
        "TPC-C PB Investigation: Load vs Latency (mean)\n"
        "New Order via HTTP POST  ·  3-way CloudLab",
        fontsize=11,
    )

    plt.tight_layout()
    out.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out, dpi=150)
    plt.close(fig)
    print(f"   Latency plot saved -> {out}")


# ── Plot 2: Throughput ────────────────────────────────────────────────────────

def plot_throughput(datasets: dict, out: Path) -> None:
    if not HAS_MATPLOTLIB:
        print("WARNING: matplotlib not installed — cannot generate plot.")
        return

    rates = union_rates(datasets)
    if not rates:
        print("WARNING: no rate data available for throughput plot.")
        return

    markers = {
        "pb": "o", "pgsync": "^", "unreplicated": "D",
        "rsync": "s", "batched": "v", "finesweep": "p",
    }
    colors = {d[0]: d[2] for d in DATASETS}
    labels = {d[0]: d[1] for d in DATASETS}

    fig, ax = plt.subplots(figsize=(10, 5))

    active_keys = [key for key, _, _, _ in DATASETS if datasets.get(key)]

    for key in active_keys:
        rows = datasets[key]
        offered    = [r["offered_rps"]   for r in rows]
        throughput = [r["throughput_rps"] for r in rows]
        ax.plot(offered, throughput, f"{markers.get(key, 'o')}-",
                color=colors[key], linewidth=2, label=labels[key], markersize=6)

    # Plot finesweep if available
    finesweep = datasets.get("finesweep", [])
    if finesweep:
        offered    = [r["offered_rps"]   for r in finesweep]
        throughput = [r["throughput_rps"] for r in finesweep]
        ax.plot(offered, throughput, "p--",
                color="#4C72B0", linewidth=1.5, alpha=0.6,
                label="XDN PB (fine sweep)", markersize=5)

    # y=x reference line
    max_rate = max(rates)
    ax.plot([0, max_rate], [0, max_rate],
            "--", color="#aaaaaa", linewidth=1.5, label="perfect (y = x)", zorder=0)

    ax.set_xlabel("Offered Load (req/s)", fontsize=12)
    ax.set_ylabel("Actual Throughput (req/s)", fontsize=12)
    ax.set_xlim(left=0)
    ax.set_ylim(bottom=0)
    ax.grid(axis="both", alpha=0.25, linestyle="--")
    ax.legend(loc="upper left", fontsize=8, framealpha=0.9)
    ax.set_title(
        "TPC-C PB Investigation: Offered vs Actual Throughput\n"
        "New Order via HTTP POST  ·  3-way CloudLab",
        fontsize=11,
    )

    plt.tight_layout()
    out.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out, dpi=150)
    plt.close(fig)
    print(f"   Throughput plot saved -> {out}")


# ── Table ─────────────────────────────────────────────────────────────────────

def print_comparison_table(datasets: dict) -> None:
    rates = union_rates(datasets)
    active_keys = [key for key, _, _, _ in DATASETS if datasets.get(key)]
    labels = {d[0]: d[1] for d in DATASETS}

    def fmt(v):
        return f"{v:.1f}" if not math.isnan(v) else "  n/a"

    maps = {}
    for key in active_keys:
        maps[key] = {r["offered_rps"]: r for r in datasets[key]}

    col_w = 28
    header = f"  {'rate':>6}  " + "".join(f"{labels[k]:^{col_w}}" for k in active_keys)
    sub    = f"  {'':>6}  " + "".join(
        f"{'tput':>7}  {'p50':>8}  {'p95':>8}  " for _ in active_keys
    )
    sep    = "  " + "-" * (8 + col_w * len(active_keys))

    print()
    print(header)
    print(sub)
    print(sep)
    for rate in rates:
        line = f"  {int(rate):>6}  "
        for key in active_keys:
            row = maps[key].get(rate)
            if row:
                line += (
                    f"{row['throughput_rps']:>7.2f}  "
                    f"{fmt(row['p50_ms']):>8}  "
                    f"{fmt(row['p95_ms']):>8}  "
                )
            else:
                line += f"{'n/a':>7}  {'n/a':>8}  {'n/a':>8}  "
        print(line)
    print()

    print("  Peak throughput:")
    for key in active_keys:
        rows = datasets.get(key, [])
        if rows:
            peak = max(r["throughput_rps"] for r in rows)
            rate_at_peak = max(rows, key=lambda r: r["throughput_rps"])["offered_rps"]
            print(f"    {labels[key]:28s}: {peak:.2f} rps at {int(rate_at_peak)} rps offered")
    print()


# ── Report ────────────────────────────────────────────────────────────────────

def save_report(datasets: dict, out_path: Path) -> None:
    """Write a text report with peak throughput, knee analysis, and overhead breakdown."""
    active_keys = [key for key, _, _, _ in DATASETS if datasets.get(key)]
    labels = {d[0]: d[1] for d in DATASETS}

    lines = []
    lines.append("TPC-C PB Investigation — Final Report")
    lines.append(f"Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")

    # Peak throughput + knee per system
    lines.append("Peak Throughput and Knee:")
    lines.append("-" * 70)
    for key in active_keys:
        rows = datasets.get(key, [])
        if not rows:
            lines.append(f"  {labels[key]:28s}: no data")
            continue
        peak_row = max(rows, key=lambda r: r["throughput_rps"])
        peak = peak_row["throughput_rps"]
        peak_rate = peak_row["offered_rps"]

        knee_rate = None
        knee_p50 = math.nan
        knee_p99 = math.nan
        for r in sorted(rows, key=lambda x: x["offered_rps"]):
            if r["offered_rps"] > 0 and r["throughput_rps"] < 0.9 * r["offered_rps"]:
                knee_rate = r["offered_rps"]
                knee_p50 = r.get("p50_ms", math.nan)
                knee_p99 = r.get("p99_ms", math.nan)
                break

        knee_str = (
            f"knee at {int(knee_rate)} rps offered "
            f"(p50={knee_p50:.1f}ms, p99={knee_p99:.1f}ms)"
            if knee_rate is not None else "no saturation detected"
        )
        lines.append(
            f"  {labels[key]:28s}: peak={peak:.2f} rps at {int(peak_rate)} rps offered; {knee_str}"
        )
    lines.append("")

    # Overhead analysis: compare rate=1 latency across configs
    lines.append("Latency @ rate=1 (overhead analysis):")
    lines.append("-" * 70)
    for key in active_keys:
        rows = datasets.get(key, [])
        low_rate = [r for r in rows if r["offered_rps"] <= 1.5]
        if low_rate:
            r = low_rate[0]
            lines.append(
                f"  {labels[key]:28s}: avg={r['avg_ms']:.1f}ms  "
                f"p50={r.get('p50_ms', math.nan):.1f}ms  "
                f"p95={r.get('p95_ms', math.nan):.1f}ms"
            )
        else:
            lines.append(f"  {labels[key]:28s}: no rate=1 data")
    lines.append("")

    # Speedup analysis
    pb_rows = datasets.get("pb", [])
    if pb_rows:
        pb_peak = max(r["throughput_rps"] for r in pb_rows)
        lines.append("Speedup vs PB baseline (peak throughput):")
        lines.append("-" * 70)
        for key in active_keys:
            if key == "pb":
                continue
            rows = datasets.get(key, [])
            if rows:
                other_peak = max(r["throughput_rps"] for r in rows)
                ratio = other_peak / pb_peak if pb_peak > 0 else math.nan
                lines.append(f"  {labels[key]:28s}: {other_peak:.2f} rps ({ratio:.2f}x of PB)")
        lines.append("")

    # Full comparison table
    lines.append("Full Comparison Table:")
    lines.append("-" * 70)

    rates = union_rates(datasets)
    maps = {key: {r["offered_rps"]: r for r in datasets[key]} for key in active_keys}

    def fmt(v):
        return f"{v:.1f}" if not math.isnan(v) else "  n/a"

    col_w = 28
    header = f"  {'rate':>6}  " + "".join(f"{labels[k]:^{col_w}}" for k in active_keys)
    sub    = f"  {'':>6}  " + "".join(
        f"{'tput':>7}  {'p50':>8}  {'p95':>8}  " for _ in active_keys
    )
    sep    = "  " + "-" * (8 + col_w * len(active_keys))
    lines.append(header)
    lines.append(sub)
    lines.append(sep)
    for rate in rates:
        line = f"  {int(rate):>6}  "
        for key in active_keys:
            row = maps[key].get(rate)
            if row:
                line += (
                    f"{row['throughput_rps']:>7.2f}  "
                    f"{fmt(row['p50_ms']):>8}  "
                    f"{fmt(row['p95_ms']):>8}  "
                )
            else:
                line += f"{'n/a':>7}  {'n/a':>8}  {'n/a':>8}  "
        lines.append(line)
    lines.append("")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines) + "\n")
    print(f"   Report saved -> {out_path}")


# ── Main ──────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--results-base", type=Path,
        default=SCRIPT_DIR / "results",
        help="Base directory containing all tpcc_pb_* subdirs "
             "(default: eval/results/)",
    )
    p.add_argument(
        "--out", type=Path,
        default=None,
        help="Output PNG base path (default: <results-base>/tpcc_pb_investigation)",
    )
    return p.parse_args()


def main():
    args = parse_args()
    results_base = args.results_base
    out_base = args.out or (results_base / "tpcc_pb_investigation")

    print(f"Loading results from {results_base} ...")
    datasets = {}
    for key, label, _, subdir in DATASETS:
        rows = load_dataset(results_base, subdir)
        datasets[key] = rows
        if rows:
            print(f"  {label:28s}: {len(rows)} rate points from {results_base / subdir}")
        else:
            print(f"  {label:28s}: no data found in {results_base / subdir}")

    # Also load finesweep if available
    finesweep = load_dataset(results_base, FINESWEEP_SUBDIR)
    if finesweep:
        datasets["finesweep"] = finesweep
        print(f"  {'XDN PB (fine sweep)':28s}: {len(finesweep)} rate points from {results_base / FINESWEEP_SUBDIR}")

    if not any(datasets.values()):
        print("ERROR: no data found in any results directory.")
        raise SystemExit(1)

    print_comparison_table(datasets)
    plot_latency(datasets,    Path(str(out_base) + "_latency.png"))
    plot_throughput(datasets, Path(str(out_base) + "_throughput.png"))
    save_report(datasets, results_base / "tpcc_pb_investigation_report.txt")


if __name__ == "__main__":
    main()
