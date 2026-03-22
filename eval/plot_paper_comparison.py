"""
plot_paper_comparison.py — Paper-ready 3-way comparison figures.

For each application (WordPress, TPC-C, Hotel-Res), generates a single PDF
with two side-by-side subplots:
  Left:  Load vs Latency (mean)
  Right: Offered Load vs Actual Throughput

Output: 4-inch-wide PDFs in reflex-paper/figures/.

Usage (run from eval/ directory):
    python3 plot_paper_comparison.py [--results-base PATH] [--out-dir PATH]
"""

import argparse
import math
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

from plot_load_latency import discover_rates, load_all

# ── Constants ─────────────────────────────────────────────────────────────────

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_RESULTS_BASE = SCRIPT_DIR / "results"
DEFAULT_OUT_DIR = SCRIPT_DIR.parent / "reflex-paper" / "figures"

# Per-application configuration: (app_name, output_filename, datasets, y_limit)
# Each dataset: (key, label, color, marker, results_subdir)
APPS = [
    {
        "name": "WordPress",
        "filename": "eval_wordpress_pb_comparison.pdf",
        "datasets": [
            ("pb",       "XDN PB",    "#4C72B0", "o", "wordpress_pb"),
            ("openebs",  "OpenEBS",   "#DD8452", "s", "wordpress_openebs"),
            ("semisync", "Semi-sync", "#55A868", "^", "wordpress_semisync"),
        ],
        "lat_ylim": 200,
    },
    {
        "name": "TPC-C",
        "filename": "eval_tpcc_pb_comparison.pdf",
        "datasets": [
            ("pb",      "XDN PB",      "#4C72B0", "o", "tpcc_pb"),
            ("openebs", "OpenEBS",     "#DD8452", "s", "tpcc_openebs"),
            ("pgsync",  "PG Sync-Rep", "#55A868", "^", "tpcc_pgsync"),
        ],
        "lat_ylim": 5000,
    },
    {
        "name": "Hotel-Reservation",
        "filename": "eval_hotelres_pb_comparison.pdf",
        "datasets": [
            ("pb",        "XDN PB",        "#4C72B0", "o", "hotelres_pb"),
            ("replmongo", "Repl. MongoDB", "#DD8452", "s", "hotelres_replmongo"),
            ("openebs",   "OpenEBS",       "#55A868", "^", "hotelres_openebs"),
        ],
        "lat_ylim": 5000,
    },
    # ── Primary-Backup (non-deterministic, lightweight) ──
    {
        "name": "BookCatalog-ND (PB)",
        "filename": "eval_bookcatalog_nd_pb_comparison.pdf",
        "datasets": [
            ("pb",      "XDN PB",    "#4C72B0", "o", "bookcatalog_pb"),
            ("criu",    "CRIU",      "#DD8452", "s", "bookcatalog_criu"),
            ("rqlite",  "rqlite",    "#55A868", "^", "bookcatalog_rqlite"),
            ("openebs", "OpenEBS",   "#C44E52", "D", "bookcatalog_openebs"),
        ],
        "lat_ylim": 200,
    },
    # ── Active Replication (deterministic apps) ──
    {
        "name": "BookCatalog",
        "filename": "eval_bookcatalog_ar_comparison.pdf",
        "datasets": [
            ("reflex",  "XDN (Paxos RSM)", "#4C72B0", "o", "reflex_bookcatalog"),
            ("distdb",  "rqlite (Raft)",    "#DD8452", "s", "distdb_bookcatalog"),
            ("openebs", "OpenEBS",          "#55A868", "^", "oebs_bookcatalog"),
        ],
        "lat_ylim": 100,
    },
    {
        "name": "WebKV",
        "filename": "eval_webkv_ar_comparison.pdf",
        "datasets": [
            ("reflex",  "XDN (Paxos RSM)", "#4C72B0", "o", "reflex_webkv"),
            ("distdb",  "TiKV",            "#DD8452", "s", "distdb_webkv"),
            ("openebs", "OpenEBS",         "#55A868", "^", "oebs_webkv"),
        ],
        "lat_ylim": 10,
    },
    {
        "name": "Todo",
        "filename": "eval_todo_ar_comparison.pdf",
        "datasets": [
            ("reflex",  "XDN (Paxos RSM)", "#4C72B0", "o", "reflex_todogo"),
            ("distdb",  "rqlite (Raft)",    "#DD8452", "s", "distdb_todogo"),
            ("openebs", "OpenEBS",          "#55A868", "^", "oebs_todo"),
        ],
        "lat_ylim": 75,
    },
]

FIG_WIDTH = 4.0   # inches
FIG_HEIGHT = 1.6  # inches — compact for paper column


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
    all_rates = set()
    for rows in datasets.values():
        for row in rows:
            all_rates.add(row["offered_rps"])
    return sorted(all_rates)


# ── Plot Generation ──────────────────────────────────────────────────────────

def generate_figure(app_cfg: dict, results_base: Path, out_dir: Path) -> None:
    name = app_cfg["name"]
    ds_cfgs = app_cfg["datasets"]
    lat_ylim = app_cfg["lat_ylim"]

    # Load data
    datasets = {}
    for key, label, color, marker, subdir in ds_cfgs:
        rows = load_dataset(results_base, subdir)
        datasets[key] = rows
        n = len(rows)
        print(f"  {label:16s}: {n} rate points" + (f" from {subdir}/" if n else " (no data)"))

    if not any(datasets.values()):
        print(f"  SKIP {name}: no data found.")
        return

    rates = union_rates(datasets)
    max_rate = max(rates) if rates else 100

    # Paper-quality font sizes for 4-inch-wide figure
    LABEL_SIZE = 7
    TICK_SIZE = 6
    LEGEND_SIZE = 5.5
    TITLE_SIZE = 7.5
    MARKER_SIZE = 3.5
    LINE_WIDTH = 1.0

    fig, (ax_lat, ax_tput) = plt.subplots(
        1, 2, figsize=(FIG_WIDTH, FIG_HEIGHT),
        gridspec_kw={"wspace": 0.45},
    )

    # ── Left subplot: Load vs Latency (mean) ──
    for key, label, color, marker, _ in ds_cfgs:
        rows = datasets.get(key, [])
        if not rows:
            continue
        offered = [r["offered_rps"] for r in rows]
        avg = [r["avg_ms"] for r in rows]
        ax_lat.plot(offered, avg, marker=marker, linestyle="-",
                    color=color, linewidth=LINE_WIDTH, markersize=MARKER_SIZE,
                    label=label)

    ax_lat.set_xlabel("Offered Load (req/s)", fontsize=LABEL_SIZE)
    ax_lat.set_ylabel("Avg. Latency (ms)", fontsize=LABEL_SIZE)
    ax_lat.set_ylim(bottom=0, top=lat_ylim)
    ax_lat.set_xlim(left=0)
    ax_lat.tick_params(axis="both", labelsize=TICK_SIZE)
    ax_lat.grid(axis="both", alpha=0.2, linestyle="--", linewidth=0.5)
    ax_lat.set_title("(a) Load vs Latency", fontsize=TITLE_SIZE, pad=3)

    # ── Right subplot: Offered Load vs Actual Throughput ──
    for key, label, color, marker, _ in ds_cfgs:
        rows = datasets.get(key, [])
        if not rows:
            continue
        offered = [r["offered_rps"] for r in rows]
        throughput = [r["throughput_rps"] for r in rows]
        ax_tput.plot(offered, throughput, marker=marker, linestyle="-",
                     color=color, linewidth=LINE_WIDTH, markersize=MARKER_SIZE,
                     label=label)

    # y=x reference line
    ax_tput.plot([0, max_rate * 1.05], [0, max_rate * 1.05],
                 "--", color="#aaaaaa", linewidth=0.8, zorder=0)

    ax_tput.set_xlabel("Offered Load (req/s)", fontsize=LABEL_SIZE)
    ax_tput.set_ylabel("Actual Throughput (req/s)", fontsize=LABEL_SIZE)
    ax_tput.set_xlim(left=0)
    ax_tput.set_ylim(bottom=0)
    ax_tput.tick_params(axis="both", labelsize=TICK_SIZE)
    ax_tput.grid(axis="both", alpha=0.2, linestyle="--", linewidth=0.5)
    ax_tput.set_title("(b) Offered vs Actual Tput", fontsize=TITLE_SIZE, pad=3)

    # Single shared legend below the figure
    handles, labels = ax_lat.get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower center", ncol=len(ds_cfgs),
               fontsize=LEGEND_SIZE, frameon=False,
               bbox_to_anchor=(0.5, -0.08))

    fig.suptitle(name, fontsize=TITLE_SIZE + 1, fontweight="bold", y=1.02)

    out_path = out_dir / app_cfg["filename"]
    out_dir.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, format="pdf", bbox_inches="tight", pad_inches=0.02)
    plt.close(fig)
    print(f"  -> {out_path}")


# ── Main ──────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--results-base", type=Path, default=DEFAULT_RESULTS_BASE,
                   help="Base directory with result subdirs (default: eval/results/)")
    p.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR,
                   help="Output directory for PDFs (default: reflex-paper/figures/)")
    return p.parse_args()


def main():
    args = parse_args()
    print(f"Results: {args.results_base}")
    print(f"Output:  {args.out_dir}\n")

    for app_cfg in APPS:
        print(f"[{app_cfg['name']}]")
        generate_figure(app_cfg, args.results_base, args.out_dir)
        print()

    print("Done.")


if __name__ == "__main__":
    main()
