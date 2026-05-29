#!/usr/bin/env python3
"""plot_bw_graph.py — turn a trace_bw.py CSV into a directed bandwidth graph.

Reads the per-event CSV produced by `trace_bw.py` (columns
`local_id, peer_id, direction, bytes, ...`), aggregates bytes per
`(local_id, peer_id)` edge, and renders it as a NetworkX DiGraph with the
replicas arranged on a circle. The aggregated adjacency matrix is also
printed to stdout.

Usage:
    python3 xdn-bw-trace/plot_bw_graph.py xdn-bw-trace/results/<file>.csv
    python3 xdn-bw-trace/plot_bw_graph.py <file>.csv --output graph.png

The `out` direction is preferred for edge weights — it counts bytes once
at the sender, avoiding the double-count that would happen if we summed
both `out` (sender's tcp_sendmsg) and `in` (receiver's tcp_cleanup_rbuf)
for the same flow.
"""

import argparse
import csv
import json
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # headless render
import matplotlib.pyplot as plt
import networkx as nx

from _plotutil import (
    CLIENT_PREFIX, draw_edge_labels_at_fixed_offset, replica_client_layout,
)


def display_node_label(node):
    """Per-node display text for `nx.draw_networkx_labels`. Client
    vertices break onto two lines so 'client' and the AR id stack
    vertically inside the orange node square."""
    if node.startswith(CLIENT_PREFIX):
        return f"client\n{node[len(CLIENT_PREFIX):]}"
    return node


def load_meta(csv_path):
    """Return the workload-metadata dict that trace_bw.py writes
    alongside each CSV, or `None` if it isn't there (older CSVs, or
    user moved the file)."""
    meta_path = csv_path.with_name(csv_path.stem + ".meta.json")
    if not meta_path.exists():
        return None
    try:
        with open(meta_path) as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        print(f"warning: failed to read {meta_path}: {e}", file=sys.stderr)
        return None


def _humanize_consistency(s):
    """Match the Go CLI's display style for consistency model names.
    `LINEARIZABILITY` -> `Linearizability`, `READ_YOUR_WRITES` ->
    `ReadYourWrites`. Empty input passes through unchanged so callers
    can safely chain it.
    """
    if not s:
        return s
    return "".join(p.capitalize() for p in s.split("_"))


def format_workload_subtitle(meta):
    """Two-line workload summary built from the trace_bw.py metadata.
    Returns an empty string if `meta` is None so the caller can skip
    the subtitle entirely. Split across two lines so it fits inside
    the axes width without being clipped."""
    if not meta:
        return ""
    rr = meta.get("read_ratio")
    rate = meta.get("rate_per_replica_rps")
    n_t = meta.get("num_targets")
    dur = meta.get("duration_seconds")
    info = meta.get("service_info") or {}
    line1_parts = []
    if "service" in meta:
        line1_parts.append(f"service={meta['service']}")
    if info.get("protocol"):
        line1_parts.append(f"protocol={info['protocol']}")
    if info.get("consistency"):
        c = _humanize_consistency(info["consistency"])
        rc = _humanize_consistency(info.get("requested_consistency", ""))
        if rc and rc != c:
            line1_parts.append(f"consistency={c} (requested: {rc})")
        else:
            line1_parts.append(f"consistency={c}")
    if rr is not None:
        line1_parts.append(f"r/w={rr:.0%}/{1.0 - rr:.0%}")
    if rate is not None and n_t is not None:
        line1_parts.append(f"rate={rate:g} req/s × {n_t} replicas")
    if dur is not None:
        line1_parts.append(f"duration={dur:g}s")

    read = meta.get("read") or {}
    write = meta.get("write") or {}
    totals = meta.get("totals") or {}
    line2_parts = []
    if read.get("method") and read.get("path"):
        line2_parts.append(f"read={read['method']} {read['path']}")
    if write.get("method") and write.get("path"):
        line2_parts.append(f"write={write['method']} {write['path']}")
    if "reads" in totals and "writes" in totals:
        line2_parts.append(
            f"sent reads={totals['reads']} writes={totals['writes']}")

    sep = "  ·  "
    lines = []
    if line1_parts:
        lines.append(sep.join(line1_parts))
    if line2_parts:
        lines.append(sep.join(line2_parts))
    return "\n".join(lines)


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("csv", help="path to a trace_bw.py CSV")
    p.add_argument("--direction", choices=["out", "in", "both"], default="out",
                   help="which event direction(s) to sum into edge weight. "
                        "'out' (default) counts each flow once on the sender. "
                        "'in' counts on the receiver. 'both' double-counts "
                        "(useful only for sanity-checking symmetry).")
    p.add_argument("--include-unknown", action="store_true",
                   help="keep rows whose local_id or peer_id is 'unknown' "
                        "(default: drop them).")
    p.add_argument("--output",
                   help="output image path. Default: <csv-stem>.png next to "
                        "the CSV.")
    p.add_argument("--unit", choices=["B", "KB", "MB"], default="KB",
                   help="byte unit for edge labels and the printed matrix. "
                        "Default: KB.")
    return p.parse_args()


def read_edges(csv_path, direction, include_unknown):
    """Return a {(src, dst): bytes} dict aggregated from the CSV."""
    edges = defaultdict(int)
    nodes = set()
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        required = {"local_id", "peer_id", "direction", "bytes"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            sys.exit(f"error: CSV is missing columns: {sorted(missing)}. "
                     f"Did you regenerate it with the updated trace_bw.py?")
        for row in reader:
            if direction != "both" and row["direction"] != direction:
                continue
            src, dst = row["local_id"], row["peer_id"]
            if not include_unknown and ("unknown" in (src, dst)):
                continue
            try:
                nbytes = int(row["bytes"])
            except (ValueError, TypeError):
                continue
            edges[(src, dst)] += nbytes
            nodes.add(src)
            nodes.add(dst)
    return edges, nodes


def fmt_bytes(n, unit):
    if unit == "B":
        return f"{n} B"
    if unit == "KB":
        return f"{n / 1024:.1f} KB"
    return f"{n / (1024 * 1024):.2f} MB"


def print_matrix(nodes, edges, unit):
    """Print the adjacency matrix to stdout (rows = src, cols = dst)."""
    sorted_nodes = sorted(nodes)
    cell_strs = [
        fmt_bytes(edges.get((s, d), 0), unit)
        for s in sorted_nodes for d in sorted_nodes
    ]
    width = max(12,
                max((len(n) for n in sorted_nodes), default=0) + 2,
                max((len(c) for c in cell_strs), default=0) + 2)
    print("src \\ dst".ljust(width)
          + "".join(n.ljust(width) for n in sorted_nodes))
    for src in sorted_nodes:
        cells = [src.ljust(width)]
        for dst in sorted_nodes:
            v = edges.get((src, dst), 0)
            cells.append(("-" if src == dst and v == 0
                          else fmt_bytes(v, unit)).ljust(width))
        print("".join(cells))


def render_graph(nodes, edges, output_path, unit, meta=None):
    """Draw the DiGraph with replicas on an inner circle, per-replica
    `client-<AR>` vertices on an outer ring at the matching angle, and
    edge labels in `unit`. If `meta` is provided (the sidecar workload
    metadata from trace_bw.py), its summary is rendered as a subtitle
    beneath the main title."""
    g = nx.DiGraph()
    sorted_nodes = sorted(nodes)
    g.add_nodes_from(sorted_nodes)
    for (src, dst), w in edges.items():
        if src == dst:
            continue  # no self-loops in this view
        g.add_edge(src, dst, weight=w)

    pos = replica_client_layout(g)

    weights = [d["weight"] for _, _, d in g.edges(data=True)]
    if not weights:
        sys.exit("error: no edges to plot (every row was filtered out).")
    max_w = max(weights)
    # Map [0, max_w] -> [0.5, 5.0] line widths so small edges remain visible.
    widths = [0.5 + 4.5 * (w / max_w) for w in weights]

    ar_vs = [n for n in g.nodes
             if not n.startswith(CLIENT_PREFIX) and n not in ("client",
                                                              "unknown")]
    client_vs = [n for n in g.nodes if n.startswith(CLIENT_PREFIX)]
    other_vs = [n for n in g.nodes if n not in ar_vs and n not in client_vs]

    fig, ax = plt.subplots(figsize=(10, 10))
    nx.draw_networkx_nodes(g, pos, nodelist=ar_vs,
                           node_color="#cfe8ff", edgecolors="#1f6fb2",
                           node_size=1800, ax=ax)
    if client_vs:
        nx.draw_networkx_nodes(g, pos, nodelist=client_vs,
                               node_color="#ffe2c2", edgecolors="#c2691f",
                               node_shape="s", node_size=1500, ax=ax)
    if other_vs:
        nx.draw_networkx_nodes(g, pos, nodelist=other_vs,
                               node_color="#dddddd", edgecolors="#666666",
                               node_size=1200, ax=ax)
    display_labels = {n: display_node_label(n) for n in g.nodes}
    nx.draw_networkx_labels(g, pos, labels=display_labels,
                            font_size=10, font_weight="bold", ax=ax)
    edge_rad = 0.12
    edge_curve = f"arc3,rad={edge_rad}"
    nx.draw_networkx_edges(
        g, pos, width=widths, edge_color="#1f6fb2",
        arrows=True, arrowstyle="-|>", arrowsize=16,
        connectionstyle=edge_curve,
        node_size=1800, ax=ax,
    )
    edge_labels = {(u, v): fmt_bytes(d["weight"], unit)
                   for u, v, d in g.edges(data=True)}
    # Absolute offset (data coords) from the target node center to the
    # label's destination-facing edge. Has to clear: node disk radius
    # (~0.11 with node_size=1800 at this figsize/dpi) plus the
    # arrowhead's length (~0.08), plus a little breathing room so the
    # widest labels (e.g. "2966.6 KB" on the heaviest AR1->follower
    # edges) don't graze the arrow tip.
    draw_edge_labels_at_fixed_offset(
        ax, g, pos, edge_labels,
        rad=edge_rad, offset=0.28,
        fontsize=8,
        bbox={"boxstyle": "round,pad=0.1", "fc": "white",
              "ec": "none", "alpha": 0.75},
    )
    main_title = (f"Inter-replica bandwidth — {output_path.name} "
                  f"(edge weight = bytes, unit = {unit})")
    subtitle = format_workload_subtitle(meta)
    if subtitle:
        # fig.suptitle = main heading at top of figure;
        # ax.set_title = subheading directly above the axes.
        fig.suptitle(main_title, fontsize=11, y=0.97)
        ax.set_title(subtitle, fontsize=9, color="dimgray", pad=6)
    else:
        ax.set_title(main_title, fontsize=11)
    ax.set_axis_off()
    # A little padding so outer-ring labels aren't clipped.
    ax.margins(0.15)
    fig.tight_layout()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)


def main():
    args = parse_args()
    csv_path = Path(args.csv)
    if not csv_path.exists():
        sys.exit(f"error: {csv_path} does not exist")

    edges, nodes = read_edges(csv_path, args.direction, args.include_unknown)
    if not nodes:
        sys.exit("error: CSV produced no usable rows after filtering. "
                 "Try --include-unknown or check --direction.")

    print(f"# direction={args.direction}  nodes={len(nodes)}  "
          f"edges={len(edges)}  unit={args.unit}")
    print_matrix(nodes, edges, args.unit)

    output_path = (Path(args.output) if args.output
                   else csv_path.with_suffix(".png"))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    meta = load_meta(csv_path)
    render_graph(nodes, edges, output_path, args.unit, meta=meta)
    print(f"# wrote {output_path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
