"""
plot_synth_comparison.py — Generate comparison plots for synth-workload experiments.

Can be run standalone to generate all 5 plots from existing results, or called
from individual benchmark scripts for per-variant plots.

Usage:
    # Generate all comparison plots (requires all 3 variants' results)
    python3 plot_synth_comparison.py

    # Generate plots for a single variant
    python3 plot_synth_comparison.py --results-dir results/synth_pb --variant pb

Outputs:
    results/synth_exp1_txns.png
    results/synth_exp2_ops.png
    results/synth_exp3_writesize.png
    results/synth_exp4_combined_latency.png
    results/synth_exp4_combined_throughput.png
    results/synth_final_report.txt
"""

import argparse
import sys
from pathlib import Path

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ImportError:
    print("WARNING: matplotlib not available, skipping plots")
    sys.exit(0)


# ── Helpers ──────────────────────────────────────────────────────────────────


def parse_result_file(path):
    """Parse a go-client output file into a metrics dict."""
    metrics = {}
    try:
        for line in open(path):
            if ":" not in line:
                continue
            k, v = line.split(":", 1)
            try:
                metrics[k.strip()] = float(v.strip())
            except ValueError:
                pass
    except FileNotFoundError:
        return None
    if not metrics:
        return None
    return {
        "throughput_rps": metrics.get("actual_throughput_rps", 0.0),
        "actual_achieved_rps": metrics.get("actual_achieved_rate_rps", 0.0),
        "avg_ms": metrics.get("average_latency_ms", 0.0),
        "p50_ms": metrics.get("median_latency_ms", 0.0),
        "p90_ms": metrics.get("p90_latency_ms", 0.0),
        "p95_ms": metrics.get("p95_latency_ms", 0.0),
        "p99_ms": metrics.get("p99_latency_ms", 0.0),
    }


def load_exp1(results_dir):
    """Load experiment 1 results (vary txns)."""
    exp_dir = results_dir / "exp1_vary_txns"
    rows = []
    for txns in [1, 2, 5, 10, 20]:
        m = parse_result_file(exp_dir / f"txns{txns}.txt")
        if m:
            rows.append({"txns": txns, **m})
    return rows


def load_exp2(results_dir):
    """Load experiment 2 results (vary ops)."""
    exp_dir = results_dir / "exp2_vary_ops"
    rows = []
    for ops in [1, 5, 10, 20, 50]:
        m = parse_result_file(exp_dir / f"ops{ops}.txt")
        if m:
            rows.append({"ops": ops, **m})
    return rows


def load_exp3(results_dir):
    """Load experiment 3 results (vary write_size)."""
    exp_dir = results_dir / "exp3_vary_writesize"
    rows = []
    for ws in [100, 1000, 10000, 100000]:
        m = parse_result_file(exp_dir / f"write_size{ws}.txt")
        if m:
            rows.append({"write_size": ws, **m})
    return rows


def load_exp4(results_dir):
    """Load experiment 4 results (rate sweep)."""
    exp_dir = results_dir / "exp4_combined"
    rows = []
    for p in sorted(exp_dir.glob("rate*.txt")):
        if "warmup" in p.name:
            continue
        try:
            rate = int(p.stem.replace("rate", ""))
        except ValueError:
            continue
        m = parse_result_file(p)
        if m:
            rows.append({"rate_rps": rate, **m})
    return sorted(rows, key=lambda r: r["rate_rps"])


# ── Plot functions ──────────────────────────────────────────────────────────


VARIANTS = [
    ("pb",      "XDN PB",  "#4C72B0", "o"),
    ("openebs", "OpenEBS", "#DD8452", "s"),
    ("rqlite",  "rqlite",  "#55A868", "^"),
]

FIG_WIDTH = 5.0
FIG_HEIGHT = 3.5


def plot_parameter_sweep(datasets, x_key, x_label, title, out_path):
    """Generic parameter sweep plot: x_key vs avg latency for each variant."""
    fig, ax = plt.subplots(figsize=(FIG_WIDTH, FIG_HEIGHT))

    for variant_key, label, color, marker in VARIANTS:
        rows = datasets.get(variant_key, [])
        if not rows:
            continue
        xs = [r[x_key] for r in rows]
        ys = [r["avg_ms"] for r in rows]
        ax.plot(xs, ys, marker=marker, color=color, label=label,
                linewidth=2, markersize=6)

    ax.set_xlabel(x_label)
    ax.set_ylabel("Avg Latency (ms)")
    ax.set_title(title)
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"   Saved: {out_path}")


def plot_exp1(datasets, out_dir):
    plot_parameter_sweep(
        datasets, "txns", "Transactions per Request",
        "Latency vs Transactions/Request (@100 rps)",
        out_dir / "synth_exp1_txns.png",
    )


def plot_exp2(datasets, out_dir):
    plot_parameter_sweep(
        datasets, "ops", "Operations per Transaction",
        "Latency vs Operations/Transaction (@100 rps)",
        out_dir / "synth_exp2_ops.png",
    )


def plot_exp3(datasets, out_dir):
    """Write size plot with log-scale x-axis."""
    fig, ax = plt.subplots(figsize=(FIG_WIDTH, FIG_HEIGHT))
    for variant_key, label, color, marker in VARIANTS:
        rows = datasets.get(variant_key, [])
        if not rows:
            continue
        xs = [r["write_size"] for r in rows]
        ys = [r["avg_ms"] for r in rows]
        ax.plot(xs, ys, marker=marker, color=color, label=label,
                linewidth=2, markersize=6)
    ax.set_xscale("log")
    ax.set_xlabel("Write Size (bytes)")
    ax.set_ylabel("Avg Latency (ms)")
    ax.set_title("Latency vs Write Size (@100 rps)")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    out = out_dir / "synth_exp3_writesize.png"
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"   Saved: {out}")


def plot_exp4_latency(datasets, out_dir):
    """Standard load-latency curve (offered rate vs avg latency)."""
    fig, ax = plt.subplots(figsize=(FIG_WIDTH, FIG_HEIGHT))
    for variant_key, label, color, marker in VARIANTS:
        rows = datasets.get(variant_key, [])
        if not rows:
            continue
        xs = [r["rate_rps"] for r in rows]
        ys = [r["avg_ms"] for r in rows]
        ax.plot(xs, ys, marker=marker, color=color, label=label,
                linewidth=2, markersize=6)
    ax.set_xlabel("Offered Load (req/s)")
    ax.set_ylabel("Avg Latency (ms)")
    ax.set_title("Load-Latency (txns=5, ops=5, write_size=1K)")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    out = out_dir / "synth_exp4_combined_latency.png"
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"   Saved: {out}")


def plot_exp4_throughput(datasets, out_dir):
    """Offered vs actual throughput."""
    fig, ax = plt.subplots(figsize=(FIG_WIDTH, FIG_HEIGHT))
    max_rate = 0
    for variant_key, label, color, marker in VARIANTS:
        rows = datasets.get(variant_key, [])
        if not rows:
            continue
        xs = [r["rate_rps"] for r in rows]
        ys = [r["throughput_rps"] for r in rows]
        ax.plot(xs, ys, marker=marker, color=color, label=label,
                linewidth=2, markersize=6)
        if xs:
            max_rate = max(max_rate, max(xs))
    if max_rate > 0:
        ax.plot([0, max_rate], [0, max_rate], "k--", alpha=0.3, label="y=x")
    ax.set_xlabel("Offered Load (req/s)")
    ax.set_ylabel("Actual Throughput (req/s)")
    ax.set_title("Throughput (txns=5, ops=5, write_size=1K)")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    out = out_dir / "synth_exp4_combined_throughput.png"
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"   Saved: {out}")


def generate_report(all_datasets, out_dir):
    """Generate a text summary report."""
    out = out_dir / "synth_final_report.txt"
    lines = ["Synth-Workload Sync-Granularity Benchmark Report", "=" * 60, ""]

    for exp_name, x_key, x_label in [
        ("exp1", "txns", "txns"),
        ("exp2", "ops", "ops"),
        ("exp3", "write_size", "write_size"),
    ]:
        lines.append(f"--- {exp_name}: vary {x_label} ---")
        header = f"{'variant':<10} {x_label:>10} {'avg_ms':>10} {'p50_ms':>10} {'p95_ms':>10} {'tput':>10}"
        lines.append(header)
        lines.append("-" * len(header))
        for vkey, vlabel, _, _ in VARIANTS:
            rows = all_datasets.get(exp_name, {}).get(vkey, [])
            for r in rows:
                lines.append(
                    f"{vlabel:<10} {r[x_key]:>10} "
                    f"{r['avg_ms']:>10.1f} {r['p50_ms']:>10.1f} "
                    f"{r['p95_ms']:>10.1f} {r['throughput_rps']:>10.1f}"
                )
        lines.append("")

    # Exp4 summary
    lines.append("--- exp4: rate sweep ---")
    header = f"{'variant':<10} {'rate':>8} {'avg_ms':>10} {'p50_ms':>10} {'p95_ms':>10} {'tput':>10}"
    lines.append(header)
    lines.append("-" * len(header))
    for vkey, vlabel, _, _ in VARIANTS:
        rows = all_datasets.get("exp4", {}).get(vkey, [])
        for r in rows:
            lines.append(
                f"{vlabel:<10} {r['rate_rps']:>8} "
                f"{r['avg_ms']:>10.1f} {r['p50_ms']:>10.1f} "
                f"{r['p95_ms']:>10.1f} {r['throughput_rps']:>10.1f}"
            )
    lines.append("")

    with open(out, "w") as f:
        f.write("\n".join(lines))
    print(f"   Saved: {out}")


# ── Main ──────────────────────────────────────────────────────────────────────


def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--results-dir", type=str, default=None,
                   help="Results directory for a single variant")
    p.add_argument("--variant", type=str, default=None,
                   choices=["pb", "openebs", "rqlite"],
                   help="Which variant the results-dir belongs to")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    results_root = Path(__file__).resolve().parent / "results"

    if args.results_dir and args.variant:
        # Single-variant mode: just load and print summary
        results_dir = Path(args.results_dir)
        for exp_name, loader in [("exp1", load_exp1), ("exp2", load_exp2),
                                  ("exp3", load_exp3), ("exp4", load_exp4)]:
            rows = loader(results_dir)
            if rows:
                print(f"   {args.variant}/{exp_name}: {len(rows)} data points loaded")
        print("   (Full comparison plots require all 3 variants)")
        sys.exit(0)

    # Full comparison mode — load all variants
    variant_dirs = {
        "pb":      results_root / "synth_pb",
        "openebs": results_root / "synth_openebs",
        "rqlite":  results_root / "synth_rqlite",
    }

    all_datasets = {}
    for exp_name, loader in [("exp1", load_exp1), ("exp2", load_exp2),
                              ("exp3", load_exp3), ("exp4", load_exp4)]:
        all_datasets[exp_name] = {}
        for vkey, vdir in variant_dirs.items():
            rows = loader(vdir)
            if rows:
                all_datasets[exp_name][vkey] = rows
                print(f"   Loaded {vkey}/{exp_name}: {len(rows)} points")

    # Generate plots
    print("\nGenerating comparison plots ...")

    if all_datasets.get("exp1"):
        plot_exp1(all_datasets["exp1"], results_root)
    if all_datasets.get("exp2"):
        plot_exp2(all_datasets["exp2"], results_root)
    if all_datasets.get("exp3"):
        plot_exp3(all_datasets["exp3"], results_root)
    if all_datasets.get("exp4"):
        plot_exp4_latency(all_datasets["exp4"], results_root)
        plot_exp4_throughput(all_datasets["exp4"], results_root)

    # Generate report
    generate_report(all_datasets, results_root)

    print("\n[Done] All plots generated.")
