"""
plot_load_latency.py — Load-latency graph for the XDN WordPress PB benchmark.

Reads rate*.txt files produced by run_load_pb_wordpress_reflex.py and generates two
figures:
  1. load_latency.png              — latency (log scale) vs offered load
  2. offered_actual_throughput.png — actual vs offered throughput with y=x reference

Usage (run from eval/ directory):
    python3 plot_load_latency.py [--results-dir PATH]
"""

import argparse
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


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_RESULTS_DIR = SCRIPT_DIR / "results" / "bottleneck"


def discover_rates(results_dir: Path) -> list:
    """Auto-discover rate points from rate*.txt files in results_dir."""
    rates = []
    for p in sorted(results_dir.glob("rate*.txt")):
        try:
            rates.append(int(p.stem.replace("rate", "")))
        except ValueError:
            pass
    return sorted(rates)


def parse_rate_file(path: Path) -> dict | None:
    """Return metrics dict or None if the file is missing / contains errors."""
    if not path.exists():
        return None
    metrics = {}
    for line in path.read_text(errors="replace").splitlines():
        if ":" not in line:
            continue
        k, _, v = line.partition(":")
        try:
            metrics[k.strip()] = float(v.strip())
        except ValueError:
            pass
    if not metrics:
        return None
    return {
        "offered_rps":    metrics.get("actual_achieved_rate_rps",
                                      float(path.stem.replace("rate", ""))),
        "throughput_rps": metrics.get("actual_throughput_rps", math.nan),
        "avg_ms":         metrics.get("average_latency_ms",    math.nan),
        "p50_ms":         metrics.get("median_latency_ms",     math.nan),
        "p90_ms":         metrics.get("p90_latency_ms",        math.nan),
        "p95_ms":         metrics.get("p95_latency_ms",        math.nan),
        "p99_ms":         metrics.get("p99_latency_ms",        math.nan),
    }


def load_all(results_dir: Path, rates: list) -> list:
    rows = []
    for r in rates:
        row = parse_rate_file(results_dir / f"rate{r}.txt")
        if row is not None and not math.isnan(row["throughput_rps"]):
            rows.append(row)
    return rows


def plot_latency(rows: list, out: Path) -> None:
    """Figure 1: latency curves (log scale) vs offered load."""
    if not HAS_MATPLOTLIB:
        print("WARNING: matplotlib not installed — cannot generate plot.")
        print("         pip install matplotlib")
        return

    offered = [r["offered_rps"] for r in rows]
    p50     = [r["p50_ms"]      for r in rows]
    p90     = [r["p90_ms"]      for r in rows]
    p99     = [r["p99_ms"]      for r in rows]
    avg     = [r["avg_ms"]      for r in rows]

    fig, ax = plt.subplots(figsize=(9, 5))

    ax.plot(offered, avg, "x-",  color="#8172B2", linewidth=2.5,
            label="avg latency", zorder=5)
    ax.plot(offered, p50, "o--", color="#4C72B0", linewidth=1.5,
            label="p50 latency", zorder=4)
    ax.plot(offered, p90, "s--", color="#DD8452", linewidth=1.5,
            label="p90 latency", zorder=4)
    ax.plot(offered, p99, "^-.", color="#C44E52", linewidth=1.5,
            label="p99 latency", zorder=4)

    ax.set_xlabel("Offered Load (req/s)", fontsize=12)
    ax.set_ylabel("Latency (ms)", fontsize=12)
    ax.set_yscale("linear")
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(
        lambda v, _: f"{v:.0f}"
    ))
    ax.set_ylim(bottom=0)
    ax.set_xticks(offered)
    ax.grid(axis="both", alpha=0.25, linestyle="--")
    ax.legend(loc="upper left", fontsize=10, framealpha=0.9)
    ax.set_title(
        "XDN WordPress Primary-Backup: Load vs Latency\n"
        "wp.newPost via XML-RPC  ·  3-way CloudLab  ·  60 s per rate",
        fontsize=11,
    )

    plt.tight_layout()
    out.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out, dpi=150)
    plt.close(fig)
    print(f"   Graph saved → {out}")


def plot_throughput(rows: list, out: Path) -> None:
    """Figure 2: offered vs actual throughput with y=x reference line."""
    if not HAS_MATPLOTLIB:
        print("WARNING: matplotlib not installed — cannot generate plot.")
        return

    offered = [r["offered_rps"]    for r in rows]
    actual  = [r["throughput_rps"] for r in rows]

    fig, ax = plt.subplots(figsize=(7, 5))

    # y=x reference (perfect throughput)
    max_offered = max(offered) if offered else 20
    ax.plot([0, max_offered * 1.1], [0, max_offered * 1.1],
            "--", color="#aaaaaa", linewidth=1.5, label="perfect (y = x)")

    # Actual throughput curve
    ax.plot(offered, actual, "o-", color="#4C72B0", linewidth=2.5,
            markersize=7, label="actual throughput")

    # Annotate the saturation point: first rate where actual < 90% of offered
    for off, act in zip(offered, actual):
        if act < 0.9 * off:
            ax.annotate(
                f"saturation\n≈{act:.1f} rps",
                xy=(off, act),
                xytext=(off + 0.5, act * 0.75),
                arrowprops=dict(arrowstyle="->", color="#C44E52"),
                fontsize=9, color="#C44E52",
            )
            break

    ax.set_xlabel("Offered Load (req/s)", fontsize=12)
    ax.set_ylabel("Actual Throughput (req/s)", fontsize=12)
    ax.set_xlim(left=0, right=max_offered * 1.1)
    ax.set_ylim(bottom=0, top=max_offered * 1.1)
    ax.set_xticks(offered)
    ax.grid(alpha=0.25, linestyle="--")
    ax.legend(loc="upper left", fontsize=10, framealpha=0.9)
    ax.set_title(
        "XDN WordPress Primary-Backup: Offered vs Actual Throughput\n"
        "wp.newPost via XML-RPC  ·  3-way CloudLab",
        fontsize=11,
    )

    plt.tight_layout()
    out.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out, dpi=150)
    plt.close(fig)
    print(f"   Graph saved → {out}")


def print_table(rows: list) -> None:
    print()
    print(f"  {'offered':>8}  {'tput':>8}  {'avg':>8}  "
          f"{'p50':>8}  {'p90':>8}  {'p95':>8}  {'p99':>8}")
    print("  " + "-" * 72)
    for r in rows:
        def f(v):
            return f"{v:.1f}ms" if not math.isnan(v) else "   n/a"
        print(
            f"  {r['offered_rps']:>6.0f}  "
            f"{r['throughput_rps']:>7.2f}  "
            f"{f(r['avg_ms']):>8}  "
            f"{f(r['p50_ms']):>8}  "
            f"{f(r['p90_ms']):>8}  "
            f"{f(r['p95_ms']):>8}  "
            f"{f(r['p99_ms']):>8}"
        )
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results-dir", type=Path, default=DEFAULT_RESULTS_DIR)
    args = parser.parse_args()

    rates = discover_rates(args.results_dir)
    if not rates:
        print(f"ERROR: no rate*.txt files found in {args.results_dir}")
        print("       Run run_load_pb_wordpress_reflex.py first.")
        raise SystemExit(1)

    rows = load_all(args.results_dir, rates)
    if not rows:
        print(f"ERROR: no parseable rate*.txt files found in {args.results_dir}")
        raise SystemExit(1)

    print(f"Loaded {len(rows)} rate data points from {args.results_dir}")
    print_table(rows)

    plot_latency(rows, args.results_dir / "load_latency.png")
    plot_throughput(rows, args.results_dir / "offered_actual_throughput.png")


if __name__ == "__main__":
    main()
