"""
plot_comparison.py — Side-by-side comparison of Standalone vs XDN Primary-Backup.

Reads rate*.txt from results/bottleneck/ (XDN-PB) and results/standalone/
(standalone WordPress direct), then generates a two-subplot comparison figure.

Usage (run from eval/ directory):
    python3 plot_comparison.py [--out PATH]
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

from plot_load_latency import load_all

SCRIPT_DIR     = Path(__file__).resolve().parent
BOTTLENECK_DIR = SCRIPT_DIR / "results" / "bottleneck"
STANDALONE_DIR = SCRIPT_DIR / "results" / "standalone"
DEFAULT_OUT    = SCRIPT_DIR / "results" / "comparison.png"

RATES = [1, 5, 10, 20, 30, 50]


def plot_comparison(
    pb_rows: list[dict],
    standalone_rows: list[dict],
    out: Path,
    label1: str = "Standalone",
    label2: str = "XDN Primary-Backup",
) -> None:
    if not HAS_MATPLOTLIB:
        print("WARNING: matplotlib not installed — cannot generate plot.")
        print("         pip install matplotlib")
        return

    fig, (ax_lat, ax_tput) = plt.subplots(1, 2, figsize=(14, 5))
    fig.suptitle(
        f"{label1} vs {label2}: wp.newPost Throughput & Latency\n"
        "XML-RPC POST  ·  3-way CloudLab  ·  60 s per rate",
        fontsize=11,
    )

    # ── Left subplot: Latency (p50, p99) vs offered load ──────────────────────

    def plot_latency_group(rows, color_p50, color_p99, label_prefix):
        if not rows:
            return
        offered = [r["offered_rps"] for r in rows]
        p50 = [r["p50_ms"] for r in rows]
        p99 = [r["p99_ms"] for r in rows]
        ax_lat.plot(offered, p50, "o-",  color=color_p50, linewidth=2,
                    label=f"{label_prefix} p50")
        ax_lat.plot(offered, p99, "^--", color=color_p99, linewidth=1.5,
                    label=f"{label_prefix} p99")

    # dir1: blue tones; dir2: orange tones
    plot_latency_group(standalone_rows, "#4C72B0", "#1f3f6e", label1)
    plot_latency_group(pb_rows,         "#DD8452", "#8b3a0f", label2)

    ax_lat.set_xlabel("Offered Load (req/s)", fontsize=11)
    ax_lat.set_ylabel("Latency (ms)", fontsize=11)
    ax_lat.set_yscale("log")
    ax_lat.yaxis.set_major_formatter(ticker.FuncFormatter(
        lambda v, _: f"{v:.0f}" if v >= 10 else f"{v:.1f}"
    ))
    ax_lat.set_ylim(bottom=10)
    all_offered = sorted({r["offered_rps"] for r in pb_rows + standalone_rows})
    ax_lat.set_xticks(all_offered)
    ax_lat.grid(axis="both", alpha=0.25, linestyle="--")
    ax_lat.legend(fontsize=9, framealpha=0.9)
    ax_lat.set_title("Latency vs Offered Load", fontsize=10)

    # ── Right subplot: Throughput vs offered load (grouped bars) ──────────────

    pb_map = {r["offered_rps"]: r["throughput_rps"] for r in pb_rows}
    sl_map = {r["offered_rps"]: r["throughput_rps"] for r in standalone_rows}
    all_rates = sorted(set(list(pb_map.keys()) + list(sl_map.keys())))

    x     = list(range(len(all_rates)))
    bar_w = 0.35
    pb_y  = [pb_map.get(rate, math.nan) for rate in all_rates]
    sl_y  = [sl_map.get(rate, math.nan) for rate in all_rates]

    pb_bars = ax_tput.bar(
        [xi - bar_w / 2 for xi in x],
        [v if not math.isnan(v) else 0 for v in pb_y],
        width=bar_w, label=label2, color="#DD8452", alpha=0.8,
    )
    sl_bars = ax_tput.bar(
        [xi + bar_w / 2 for xi in x],
        [v if not math.isnan(v) else 0 for v in sl_y],
        width=bar_w, label=label1, color="#4C72B0", alpha=0.8,
    )

    # Annotate bar tops with actual rps value
    for bar, val in zip(pb_bars, pb_y):
        if not math.isnan(val):
            ax_tput.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 0.1,
                f"{val:.1f}",
                ha="center", va="bottom", fontsize=7, color="#8b3a0f",
            )
    for bar, val in zip(sl_bars, sl_y):
        if not math.isnan(val):
            ax_tput.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 0.1,
                f"{val:.1f}",
                ha="center", va="bottom", fontsize=7, color="#1f3f6e",
            )

    ax_tput.set_xlabel("Offered Load (req/s)", fontsize=11)
    ax_tput.set_ylabel("Actual Throughput (rps)", fontsize=11)
    ax_tput.set_xticks(x)
    ax_tput.set_xticklabels([str(int(r)) for r in all_rates])
    all_tput = [v for v in pb_y + sl_y if not math.isnan(v)]
    ax_tput.set_ylim(0, max(all_tput) * 1.3 if all_tput else 20)
    ax_tput.legend(fontsize=9, framealpha=0.9)
    ax_tput.grid(axis="y", alpha=0.25, linestyle="--")
    ax_tput.set_title("Throughput vs Offered Load", fontsize=10)

    plt.tight_layout()
    out.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out, dpi=150)
    plt.close(fig)
    print(f"   Comparison graph saved → {out}")


def print_comparison_table(
    pb_rows: list[dict],
    standalone_rows: list[dict],
    label1: str = "dir1",
    label2: str = "dir2",
) -> None:
    pb_map = {r["offered_rps"]: r for r in pb_rows}
    sl_map = {r["offered_rps"]: r for r in standalone_rows}
    all_rates = sorted(set(list(pb_map.keys()) + list(sl_map.keys())))

    def f(v):
        return f"{v:.1f}ms" if not math.isnan(v) else "   n/a"

    l1 = label1[:8]
    l2 = label2[:8]
    print()
    print(f"  {'rate':>6}  {l1+'_tput':>10}  {l1+'_p50':>10}  {l1+'_p99':>10}  "
          f"{l2+'_tput':>10}  {l2+'_p50':>10}  {l2+'_p99':>10}")
    print("  " + "-" * 80)
    for rate in all_rates:
        sl = sl_map.get(rate, {})
        pb = pb_map.get(rate, {})
        print(
            f"  {rate:>6.0f}  "
            f"{sl.get('throughput_rps', math.nan):>9.2f}  "
            f"{f(sl.get('p50_ms', math.nan)):>10}  "
            f"{f(sl.get('p99_ms', math.nan)):>10}  "
            f"{pb.get('throughput_rps', math.nan):>9.2f}  "
            f"{f(pb.get('p50_ms', math.nan)):>10}  "
            f"{f(pb.get('p99_ms', math.nan)):>10}"
        )
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dir1",   type=Path, default=STANDALONE_DIR,
                        help="First results directory (default: results/standalone)")
    parser.add_argument("--dir2",   type=Path, default=BOTTLENECK_DIR,
                        help="Second results directory (default: results/bottleneck)")
    parser.add_argument("--label1", default="Standalone",
                        help="Label for dir1 (default: Standalone)")
    parser.add_argument("--label2", default="XDN Primary-Backup",
                        help="Label for dir2 (default: XDN Primary-Backup)")
    parser.add_argument("--out",    type=Path, default=DEFAULT_OUT,
                        help=f"Output PNG path (default: {DEFAULT_OUT})")
    args = parser.parse_args()

    standalone_rows = load_all(args.dir1, RATES)
    pb_rows         = load_all(args.dir2, RATES)

    if not pb_rows and not standalone_rows:
        print("ERROR: no rate*.txt files found in either results directory.")
        print(f"       dir1: {args.dir1}")
        print(f"       dir2: {args.dir2}")
        raise SystemExit(1)

    if not standalone_rows:
        print(f"WARNING: no results found in {args.dir1}")
    else:
        print(f"{args.label1}: {len(standalone_rows)} rate points from {args.dir1}")

    if not pb_rows:
        print(f"WARNING: no results found in {args.dir2}")
    else:
        print(f"{args.label2}: {len(pb_rows)} rate points from {args.dir2}")

    print_comparison_table(standalone_rows, pb_rows, args.label1, args.label2)
    plot_comparison(pb_rows, standalone_rows, args.out, args.label1, args.label2)


if __name__ == "__main__":
    main()
