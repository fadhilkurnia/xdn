"""
plot_coordination_granularity.py — Plot coordination granularity microbenchmark.

Reads a combined CSV and produces a paper-ready figure showing per-request
latency vs SQL statements per request.

Usage:
    python3 plot_coordination_granularity.py \
        --csv ../reflex-paper-v2/data/eval_coordination_granularity.csv \
        --output ../reflex-paper-v2/figures/eval_coordination_granularity.pdf

    # Or from individual CSVs:
    python3 plot_coordination_granularity.py \
        --reflex results/microbench_coordination_reflex/coordination_granularity.csv \
        --pgsync-txn results/microbench_coordination_pgsync-txn/coordination_granularity.csv \
        --pgsync-stmt results/microbench_coordination_pgsync-stmt/coordination_granularity.csv \
        --openebs results/microbench_coordination_openebs/coordination_granularity.csv \
        --output ../reflex-paper-v2/figures/eval_coordination_granularity.pdf
"""

import argparse
import csv
import sys
from pathlib import Path

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.ticker as ticker
except ImportError:
    print("ERROR: matplotlib not installed. Install with: pip install matplotlib")
    sys.exit(1)


# Plot order controls which line is drawn on top (last = front).
# Coordination granularity from coarsest to finest:
#   reflex-txn (1 round/req) < reflex (1 round/req but more fsyncs)
#   < pgsync-txn (1 sync/commit) < pgsync-stmt (1 sync/SQL) < openebs (1 sync/fsync)
PLOT_ORDER = ["openebs", "pgsync-stmt", "pgsync-txn", "reflex", "reflex-txn"]

SYSTEM_STYLES = {
    "reflex":      {"label": "ReFlex (per request)",
                    "color": "#1f77b4", "marker": "o",  "linestyle": "-",  "linewidth": 2.2, "zorder": 5},
    "reflex-txn":  {"label": "ReFlex (per request, txn)",
                    "color": "#1f77b4", "marker": "D",  "linestyle": "--", "linewidth": 1.8, "zorder": 4},
    "pgsync-txn":  {"label": "PG Sync (per commit)",
                    "color": "#d62728", "marker": "^",  "linestyle": "--", "linewidth": 1.8, "zorder": 3},
    "pgsync-stmt": {"label": "PG Sync (per statement)",
                    "color": "#ff7f0e", "marker": "s",  "linestyle": "-.", "linewidth": 2.0, "zorder": 2},
    "openebs":     {"label": "OpenEBS (per fsync)",
                    "color": "#2ca02c", "marker": "v",  "linestyle": ":",  "linewidth": 2.0, "zorder": 1},
}


def read_csv(path):
    """Read coordination_granularity.csv and return list of dicts."""
    rows = []
    with open(path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append({
                "system": row["system"],
                "ol_cnt": int(row.get("ol_cnt") or 0),
                "sql_count": int(row["sql_count"]),
                "avg_ms": float(row["avg_ms"]),
                "p50_ms": float(row["p50_ms"]),
                "p95_ms": float(row["p95_ms"]),
            })
    return rows


def read_combined_csv(path):
    """Read a combined CSV with all systems and return dict of system -> rows."""
    all_rows = read_csv(path)
    data = {}
    for row in all_rows:
        sys = row["system"]
        data.setdefault(sys, []).append(row)
    return data


def plot(all_data, output_path):
    """Generate the coordination granularity figure."""
    fig, ax = plt.subplots(figsize=(4.5, 3.2))

    for system in PLOT_ORDER:
        if system not in all_data:
            continue
        rows = sorted(all_data[system], key=lambda r: r["sql_count"])
        style = SYSTEM_STYLES.get(system, {"label": system, "color": "gray",
                                            "marker": "x", "linestyle": "-",
                                            "linewidth": 1.5, "zorder": 0})
        sql_counts = [r["sql_count"] for r in rows]
        avg_latencies = [r["avg_ms"] for r in rows]
        ax.plot(sql_counts, avg_latencies,
                label=style["label"], color=style["color"],
                marker=style["marker"], linestyle=style["linestyle"],
                markersize=5, linewidth=style.get("linewidth", 2),
                zorder=style.get("zorder", 0))

    ax.set_xlabel("SQL statements per request", fontsize=10)
    ax.set_ylabel("Avg. latency (ms)", fontsize=10)
    ax.set_yscale("log")
    ax.yaxis.set_major_formatter(ticker.ScalarFormatter())
    ax.yaxis.set_minor_formatter(ticker.NullFormatter())
    ax.set_yticks([1, 2, 5, 10, 15])
    ax.set_ylim(bottom=0.8, top=20)
    ax.set_xlim(left=0, right=70)
    ax.legend(fontsize=7.5, loc="upper left")
    ax.grid(True, alpha=0.3, which="major")
    ax.tick_params(labelsize=9)
    fig.tight_layout()

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=300, bbox_inches="tight")
    print(f"   Plot saved: {output_path}")
    plt.close(fig)


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--csv", type=str, help="Combined CSV with all systems")
    p.add_argument("--reflex", type=str, help="Path to reflex CSV")
    p.add_argument("--reflex-txn", type=str, help="Path to reflex-txn CSV")
    p.add_argument("--pgsync-txn", type=str, help="Path to pgsync-txn CSV")
    p.add_argument("--pgsync-stmt", type=str, help="Path to pgsync-stmt CSV")
    p.add_argument("--openebs", type=str, help="Path to openebs CSV")
    p.add_argument("--output", type=str,
                   default="coordination_granularity.pdf",
                   help="Output figure path")
    args = p.parse_args()

    # Read from combined CSV or individual files
    if args.csv and Path(args.csv).exists():
        all_data = read_combined_csv(args.csv)
        for sys, rows in all_data.items():
            print(f"   Loaded {sys}: {len(rows)} points")
    else:
        all_data = {}
        for system, path in [("reflex", args.reflex),
                             ("reflex-txn", getattr(args, "reflex_txn")),
                             ("pgsync-txn", getattr(args, "pgsync_txn")),
                             ("pgsync-stmt", getattr(args, "pgsync_stmt")),
                             ("openebs", args.openebs)]:
            if path and Path(path).exists():
                all_data[system] = read_csv(path)
                print(f"   Loaded {system}: {len(all_data[system])} points from {path}")

    if not all_data:
        print("ERROR: no CSV files provided or found")
        sys.exit(1)

    plot(all_data, args.output)


if __name__ == "__main__":
    main()
