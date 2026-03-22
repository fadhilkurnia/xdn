"""
plot_triple_comparison.py — 3-way latency + throughput comparison.

Reads rate*.txt files from three results directories and generates:
  1. Latency (log scale): p50 and p95 lines for XDN PB, OpenEBS, semi-sync
  2. Throughput (grouped bars): actual rps per offered rate, with y=x reference

Also prints a side-by-side comparison table to stdout.

Usage (run from eval/ directory):
    python3 plot_triple_comparison.py [--results-base PATH]
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
    ("pb",       "XDN PB",      "#4C72B0", "final7_pb"),
    ("openebs",  "OpenEBS",     "#DD8452", "final2_openebs"),
    ("semisync", "Semi-sync",   "#55A868", "final2_semisync"),
]

# ── Helpers ────────────────────────────────────────────────────────────────────

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


# ── Plot 1: Latency (log scale) ────────────────────────────────────────────────

def plot_latency(datasets: dict, out: Path) -> None:
    if not HAS_MATPLOTLIB:
        print("WARNING: matplotlib not installed — cannot generate plot.")
        return

    # Color families per system; p50=solid, p95=dashed
    styles = {
        "pb":       ("#4C72B0", "#4C72B0"),  # blue
        "openebs":  ("#DD8452", "#DD8452"),  # orange
        "semisync": ("#55A868", "#55A868"),  # green
    }
    markers = {"pb": "o", "openebs": "s", "semisync": "^"}
    labels  = {"pb": "XDN PB", "openebs": "OpenEBS", "semisync": "Semi-sync"}

    fig, ax = plt.subplots(figsize=(10, 5))

    for key, _, _, _ in DATASETS:
        rows = datasets.get(key, [])
        if not rows:
            continue
        offered = [r["offered_rps"] for r in rows]
        avg     = [r["avg_ms"]      for r in rows]
        color   = styles[key][0]
        marker  = markers[key]
        label   = labels[key]

        ax.plot(offered, avg, f"{marker}-",
                color=color, linewidth=2,
                label=label)

    ax.set_xlabel("Offered Load (req/s)", fontsize=12)
    ax.set_ylabel("Latency (ms)", fontsize=12)
    ax.set_yscale("linear")
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(
        lambda v, _: f"{v:.0f}"
    ))
    ax.set_ylim(bottom=0, top=5000)
    ax.grid(axis="both", alpha=0.25, linestyle="--")
    ax.legend(loc="upper left", fontsize=9, framealpha=0.9, ncol=1)
    ax.set_title(
        "WordPress Replication Comparison: Load vs Latency (mean)\n"
        "wp.editPost via REST API  ·  3-way CloudLab",
        fontsize=11,
    )

    plt.tight_layout()
    out.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out, dpi=150)
    plt.close(fig)
    print(f"   Latency plot saved → {out}")


# ── Plot 2: Throughput (grouped bars) ─────────────────────────────────────────

def plot_throughput(datasets: dict, out: Path) -> None:
    if not HAS_MATPLOTLIB:
        print("WARNING: matplotlib not installed — cannot generate plot.")
        return

    rates = union_rates(datasets)
    if not rates:
        print("WARNING: no rate data available for throughput plot.")
        return

    colors  = {"pb": "#4C72B0", "openebs": "#DD8452", "semisync": "#55A868"}
    labels  = {"pb": "XDN PB",  "openebs": "OpenEBS",  "semisync": "Semi-sync"}
    markers = {"pb": "o",       "openebs": "s",         "semisync": "^"}

    fig, ax = plt.subplots(figsize=(10, 5))

    active_keys = [key for key, _, _, _ in DATASETS if datasets.get(key)]

    for key in active_keys:
        rows = datasets[key]
        offered    = [r["offered_rps"]   for r in rows]
        throughput = [r["throughput_rps"] for r in rows]
        ax.plot(offered, throughput, f"{markers[key]}-",
                color=colors[key], linewidth=2, label=labels[key])

    # y=x reference line
    max_rate = max(rates)
    ax.plot([0, max_rate], [0, max_rate],
            "--", color="#aaaaaa", linewidth=1.5, label="perfect (y = x)", zorder=0)

    ax.set_xlabel("Offered Load (req/s)", fontsize=12)
    ax.set_ylabel("Actual Throughput (req/s)", fontsize=12)
    ax.set_xlim(left=0)
    ax.set_ylim(bottom=0)
    ax.grid(axis="both", alpha=0.25, linestyle="--")
    ax.legend(loc="upper left", fontsize=10, framealpha=0.9)
    ax.set_title(
        "WordPress Replication Comparison: Offered vs Actual Throughput\n"
        "wp.editPost via REST API  ·  3-way CloudLab",
        fontsize=11,
    )

    plt.tight_layout()
    out.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out, dpi=150)
    plt.close(fig)
    print(f"   Throughput plot saved → {out}")


# ── Table ──────────────────────────────────────────────────────────────────────

def print_comparison_table(datasets: dict) -> None:
    rates = union_rates(datasets)
    active_keys = [key for key, _, _, _ in DATASETS if datasets.get(key)]
    labels = {"pb": "XDN PB", "openebs": "OpenEBS", "semisync": "Semi-sync"}

    def fmt(v):
        return f"{v:.1f}" if not math.isnan(v) else "  n/a"

    # Build per-key lookup tables
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

    # Peak throughput summary
    print("  Peak throughput:")
    for key in active_keys:
        rows = datasets.get(key, [])
        if rows:
            peak = max(r["throughput_rps"] for r in rows)
            rate_at_peak = max(rows, key=lambda r: r["throughput_rps"])["offered_rps"]
            print(f"    {labels[key]:12s}: {peak:.2f} rps at {int(rate_at_peak)} rps offered")
    print()


# ── Report ────────────────────────────────────────────────────────────────────

def save_report(datasets: dict, out_path: Path) -> None:
    """Write a text report: peak throughput, knee rate, latency at knee, full table."""
    active_keys = [key for key, _, _, _ in DATASETS if datasets.get(key)]
    labels = {"pb": "XDN PB", "openebs": "OpenEBS", "semisync": "Semi-sync"}

    lines = []
    lines.append("XDN Replication Comparison — Final Report")
    lines.append(f"Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")

    # Peak throughput + knee per system
    lines.append("Peak Throughput and Knee:")
    lines.append("-" * 60)
    for key in active_keys:
        rows = datasets.get(key, [])
        if not rows:
            lines.append(f"  {labels[key]:12s}: no data")
            continue
        peak_row = max(rows, key=lambda r: r["throughput_rps"])
        peak = peak_row["throughput_rps"]
        peak_rate = peak_row["offered_rps"]

        # Knee: first rate where actual_tput < 90% of offered rate
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
            f"  {labels[key]:12s}: peak={peak:.2f} rps at {int(peak_rate)} rps offered; {knee_str}"
        )
    lines.append("")

    # Full comparison table
    lines.append("Full Comparison Table:")
    lines.append("-" * 60)

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
    print(f"   Report saved → {out_path}")


# ── Main ──────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--results-base", type=Path,
        default=SCRIPT_DIR / "results",
        help="Base directory containing bottleneck/, openebs/, semisync/ subdirs "
             "(default: eval/results/)",
    )
    p.add_argument(
        "--out", type=Path,
        default=None,
        help="Output PNG path (default: <results-base>/triple_comparison.png)",
    )
    return p.parse_args()


def main():
    args = parse_args()
    results_base = args.results_base
    out_base = args.out or (results_base / "triple_comparison")

    print(f"Loading results from {results_base} ...")
    datasets = {}
    for key, label, _, subdir in DATASETS:
        rows = load_dataset(results_base, subdir)
        datasets[key] = rows
        if rows:
            print(f"  {label:12s}: {len(rows)} rate points from {results_base / subdir}")
        else:
            print(f"  {label:12s}: no data found in {results_base / subdir}")

    if not any(datasets.values()):
        print("ERROR: no data found in any results directory.")
        raise SystemExit(1)

    print_comparison_table(datasets)
    plot_latency(datasets,    Path(str(out_base) + "_latency.png"))
    plot_throughput(datasets, Path(str(out_base) + "_throughput.png"))
    save_report(datasets, results_base / "final_report.txt")


if __name__ == "__main__":
    main()
