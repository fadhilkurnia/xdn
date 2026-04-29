#!/usr/bin/env python3
"""plot_lat_graph.py — render an inter-replica latency graph computed
analytically from each replica's (lat, lon).

Latency is the great-circle distance via the Haversine formula divided
by an effective signal speed (default: c × 2/3, the typical refractive
index of fiber). This is a propagation-only lower bound; real RTT on
the public internet typically exceeds it by 1.5–2× because of routing
and queueing.

Usage:
    # Plot every active.<id>.geolocation in the gigapaxos config:
    python3 xdn-bw-trace/plot_lat_graph.py \\
        --config conf/gigapaxos.cloudlab.properties

    # Plot only the replicas hosting a service (queries the RC):
    python3 xdn-bw-trace/plot_lat_graph.py \\
        --config conf/gigapaxos.xdn.local.properties \\
        --service bookcatalog

    # Geographic layout (replicas placed at their actual (lon, lat)):
    python3 xdn-bw-trace/plot_lat_graph.py --config <...> --layout geo
"""

import argparse
import math
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import networkx as nx
import numpy as np

from _plotutil import replica_client_layout
from trace_bw import (
    HTTP_PORT_OFFSET, fetch_service_placement, parse_properties,
)

EARTH_RADIUS_KM = 6371.0
SPEED_OF_LIGHT_KMS = 299_792.0  # km/s


def parse_args():
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--config",
                   help="path to gigapaxos.properties. Used to read "
                        "active.<id>.geolocation if --service is not given, "
                        "and to derive the reconfigurator endpoint when "
                        "--service is given without --reconfigurator.")
    p.add_argument("--service",
                   help="XDN service name. If set, replicas are taken from "
                        "the reconfigurator's placement endpoint instead of "
                        "from the config file (so only the replicas hosting "
                        "this service appear in the graph).")
    p.add_argument("--reconfigurator",
                   help="reconfigurator HTTP endpoint as host[:port] "
                        "(default: derive host from the first reconfigurator "
                        "in --config, port = NIO port + 300).")
    p.add_argument("--output",
                   help="output PNG path. Default: <service-or-config>-"
                        "latency.png next to the input.")
    p.add_argument("--unit", choices=["ms", "us"], default="ms",
                   help="latency unit for labels and the printed matrix. "
                        "Default: ms.")
    p.add_argument("--rtt", action="store_true",
                   help="show round-trip latency (2 × one-way). Default: "
                        "one-way.")
    p.add_argument("--fiber-fraction", type=float, default=2.0 / 3.0,
                   help="effective signal speed as a fraction of c. Default: "
                        "0.667 (typical fiber refractive index ≈ 1.47).")
    p.add_argument("--layout", choices=["circle", "geo"], default="circle",
                   help="vertex layout. 'circle' (default) matches "
                        "plot_bw_graph.py; 'geo' places each replica at its "
                        "(longitude, latitude) via equirectangular "
                        "projection.")
    p.add_argument("--min-node-spacing", type=float, default=0.20,
                   help="(geo layout only) minimum distance, in normalized "
                        "data units, between any two replicas after the "
                        "projection. Replicas closer than this get pushed "
                        "apart along their connecting line, just enough to "
                        "stop their disks from overlapping. Smaller values "
                        "preserve geographic ratios more faithfully but may "
                        "leave dense clusters visually overlapping. "
                        "Default: 0.20 (just past 2× the default node-disk "
                        "radius).")
    return p.parse_args()


def haversine_km(lat1, lon1, lat2, lon2):
    """Great-circle distance in km, Haversine formula."""
    rlat1, rlat2 = math.radians(lat1), math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2) ** 2)
    return EARTH_RADIUS_KM * 2.0 * math.asin(min(1.0, math.sqrt(a)))


def latency_seconds(distance_km, fiber_fraction, rtt=False):
    """Propagation latency in seconds for a given distance over fiber."""
    if fiber_fraction <= 0.0:
        return float("inf")
    one_way = distance_km / (SPEED_OF_LIGHT_KMS * fiber_fraction)
    return 2.0 * one_way if rtt else one_way


def fmt_latency(seconds, unit):
    if unit == "us":
        return f"{seconds * 1e6:.1f} us"
    return f"{seconds * 1000.0:.2f} ms"


def load_replicas(args):
    """Return [(id, lat, lon), ...] in deterministic order.

    If --service is given, query the reconfigurator and use only the
    replicas hosting that service. Otherwise read every
    active.<id>.geolocation from --config.
    """
    if args.service:
        rc_host, rc_http_port = _resolve_rc_endpoint(args)
        nodes = fetch_service_placement(rc_host, rc_http_port, args.service)
        out = []
        skipped = []
        for n in nodes:
            geo = n.get("geolocation")
            if geo is None:
                skipped.append(n["id"])
                continue
            out.append((n["id"], geo[0], geo[1]))
        if skipped:
            print(f"warning: skipping {len(skipped)} replica(s) without "
                  f"geolocation in placement: {skipped}", file=sys.stderr)
        return out

    if not args.config:
        sys.exit("error: must pass --config (or --service to fetch from RC)")
    _, _, geolocations = parse_properties(args.config)
    if not geolocations:
        sys.exit(f"error: no active.<id>.geolocation entries in {args.config}")
    return [(name, lat, lon)
            for name, (lat, lon) in sorted(geolocations.items())]


def _resolve_rc_endpoint(args):
    if args.reconfigurator:
        endpoint = args.reconfigurator
        if ":" in endpoint:
            host, _, port_s = endpoint.rpartition(":")
            try:
                return host, int(port_s)
            except ValueError:
                sys.exit(f"error: invalid --reconfigurator '{endpoint}'")
        return endpoint, 3000 + HTTP_PORT_OFFSET
    if not args.config:
        sys.exit("error: --service requires either --reconfigurator or "
                 "--config (to derive the reconfigurator host).")
    _, reconfigurators, _ = parse_properties(args.config)
    if not reconfigurators:
        sys.exit(f"error: no reconfigurator.<name>=host:port in "
                 f"{args.config}; pass --reconfigurator host[:port]")
    name = sorted(reconfigurators.keys())[0]
    host, nio_port = reconfigurators[name]
    return host, nio_port + HTTP_PORT_OFFSET


def compute_latencies(replicas, fiber_fraction, rtt):
    """{(id_i, id_j): seconds} for every i<j (undirected)."""
    out = {}
    for i in range(len(replicas)):
        id_i, lat_i, lon_i = replicas[i]
        for j in range(i + 1, len(replicas)):
            id_j, lat_j, lon_j = replicas[j]
            d_km = haversine_km(lat_i, lon_i, lat_j, lon_j)
            out[(id_i, id_j)] = latency_seconds(d_km, fiber_fraction, rtt)
    return out


def geo_layout(replicas, min_distance=0.20, max_iter=80):
    """Equirectangular projection (lon → x, lat → y), then rescale into
    roughly [-1, 1] × [-1, 1] so the rest of the rendering pipeline (the
    same offset, font sizes, and figure size as the circle layout) keeps
    working without retuning.

    Replicas that would overlap their disks after the projection get
    iteratively pushed apart by `_spread_close_nodes` until every pair
    is at least `min_distance` away. This is intentionally just barely
    past 2× the node-disk radius (~0.16 in this frame at the default
    node_size) so the visual distance ratios stay as close to the real
    geographic ratios as possible — only pairs that would otherwise
    fully overlap get nudged, and only by the minimum amount.

    No latitude correction (would require cosine compression for high-
    latitude bands); for a small handful of replicas this isn't worth it.
    """
    if not replicas:
        return {}
    lats = [r[1] for r in replicas]
    lons = [r[2] for r in replicas]
    lon_mid = (min(lons) + max(lons)) / 2.0
    lat_mid = (min(lats) + max(lats)) / 2.0
    span = max(max(lons) - min(lons), max(lats) - min(lats), 1e-6)
    scale = 2.0 / span  # fit the bounding box of the replicas into [-1, 1]
    raw = {r_id: ((lon - lon_mid) * scale, (lat - lat_mid) * scale)
           for r_id, lat, lon in replicas}
    return _spread_close_nodes(raw, min_distance=min_distance,
                               max_iter=max_iter)


def _spread_close_nodes(pos, min_distance, max_iter):
    """Iteratively push apart any pair of nodes whose Euclidean distance
    is below `min_distance`. Each pass nudges every violating pair half-
    symmetrically along their connecting axis; for genuinely coincident
    nodes, the push direction is derived deterministically from the
    node-id pair so reruns produce the same layout. Converges in a
    handful of iterations for the small graphs this script is designed
    for; if `max_iter` is hit without convergence we still return the
    current positions (better than throwing).
    """
    if len(pos) < 2:
        return pos
    keys = sorted(pos.keys())  # deterministic iteration order
    p = {k: np.asarray(v, dtype=float).copy() for k, v in pos.items()}
    for _ in range(max_iter):
        moved = False
        for i in range(len(keys)):
            for j in range(i + 1, len(keys)):
                a, b = keys[i], keys[j]
                d_vec = p[b] - p[a]
                d = float(np.linalg.norm(d_vec))
                if d >= min_distance:
                    continue
                if d == 0.0:
                    # Coincident — pick a stable, content-derived
                    # direction so two identical inputs produce the same
                    # output across runs.
                    seed = (hash((a, b)) & 0xFFFF) / 0xFFFF
                    angle = 2.0 * math.pi * seed
                    unit = np.array([math.cos(angle), math.sin(angle)])
                else:
                    unit = d_vec / d
                violation = min_distance - d
                p[a] -= unit * violation / 2.0
                p[b] += unit * violation / 2.0
                moved = True
        if not moved:
            break
    return {k: tuple(v) for k, v in p.items()}


def print_matrix(replicas, latencies, unit):
    """Symmetric latency matrix to stdout."""
    ids = [r[0] for r in replicas]
    cell_strs = [
        fmt_latency(latencies.get(_undirected_key(a, b), 0.0), unit)
        for a in ids for b in ids
    ]
    width = max(12,
                max((len(i) for i in ids), default=0) + 2,
                max((len(c) for c in cell_strs), default=0) + 2)
    print("a \\ b".ljust(width)
          + "".join(i.ljust(width) for i in ids))
    for a in ids:
        row = [a.ljust(width)]
        for b in ids:
            if a == b:
                row.append("-".ljust(width))
            else:
                v = latencies.get(_undirected_key(a, b), 0.0)
                row.append(fmt_latency(v, unit).ljust(width))
        print("".join(row))


def _undirected_key(a, b):
    return (a, b) if a < b else (b, a)


def render_graph(replicas, latencies, output_path, args):
    """Undirected latency graph. Default circle layout matches the bw
    graph; --layout geo places replicas at their (lon, lat)."""
    g = nx.Graph()  # undirected
    g.add_nodes_from(r[0] for r in replicas)
    for (a, b), seconds in latencies.items():
        if a == b:
            continue
        g.add_edge(a, b, weight=seconds)

    if args.layout == "geo":
        pos = geo_layout(replicas, min_distance=args.min_node_spacing)
    else:
        pos = replica_client_layout(g)

    weights = [d["weight"] for _, _, d in g.edges(data=True)]
    if not weights:
        sys.exit("error: no edges to plot (need at least 2 replicas with "
                 "geolocations).")
    max_w = max(weights) or 1.0
    widths = [0.5 + 4.5 * (w / max_w) for w in weights]

    fig, ax = plt.subplots(figsize=(10, 10))
    nx.draw_networkx_nodes(g, pos, node_color="#cfe8ff",
                           edgecolors="#1f6fb2", node_size=1800, ax=ax)
    nx.draw_networkx_labels(g, pos, font_size=10, font_weight="bold", ax=ax)

    # Straight lines: latency is symmetric, so there's no need to fan
    # out two arcs per pair. networkx's undirected drawer uses a
    # LineCollection by default (no connectionstyle support); that's
    # exactly what we want here.
    edge_rad = 0.0
    nx.draw_networkx_edges(
        g, pos, width=widths, edge_color="#1f6fb2",
        node_size=1800, ax=ax,
    )
    edge_labels = {(u, v): fmt_latency(d["weight"], args.unit)
                   for u, v, d in g.edges(data=True)}
    # Latency edges are undirected and drawn straight, so the midpoint
    # is the natural place for the label. Plain networkx is fine here —
    # we don't need the curve-aware fixed-offset drawer used by the bw
    # graph (which exists specifically to handle two opposite-direction
    # arcs per pair).
    nx.draw_networkx_edge_labels(
        g, pos, edge_labels=edge_labels, font_size=8,
        label_pos=0.5,
        rotate=True,
        bbox={"boxstyle": "round,pad=0.1", "fc": "white",
              "ec": "none", "alpha": 0.75},
        ax=ax,
    )

    if args.layout == "geo":
        # Force equal data aspect so 1° of latitude renders the same
        # length as 1° of longitude — otherwise matplotlib auto-stretches
        # the narrower axis to fill the figure, which makes a 0.5° lat
        # gap look as big as a 30° lon gap. `adjustable="datalim"` keeps
        # the axes box at the requested figure size and instead expands
        # the data range in the narrower direction (so the replicas end
        # up correctly proportioned but with whitespace above and below
        # when the AR layout is wide-and-thin, e.g. our coast-to-coast
        # cluster).
        ax.set_aspect("equal", adjustable="datalim")
        # A faint graticule helps orient the viewer; otherwise the axes
        # are off and you're just looking at floating dots.
        ax.grid(True, color="#d0d0d0", linestyle=":", linewidth=0.5)
        # Restore tick labels in lat/lon (we projected to data coords,
        # but it's easy to invert for tick text).
        lats = [r[1] for r in replicas]
        lons = [r[2] for r in replicas]
        lon_mid = (min(lons) + max(lons)) / 2.0
        lat_mid = (min(lats) + max(lats)) / 2.0
        span = max(max(lons) - min(lons),
                   max(lats) - min(lats), 1e-6)
        scale = 2.0 / span

        def x_to_lon(x):
            return x / scale + lon_mid

        def y_to_lat(y):
            return y / scale + lat_mid

        ax.xaxis.set_major_formatter(
            plt.FuncFormatter(lambda x, _pos: f"{x_to_lon(x):.1f}°"))
        ax.yaxis.set_major_formatter(
            plt.FuncFormatter(lambda y, _pos: f"{y_to_lat(y):.1f}°"))
        ax.set_xlabel("longitude")
        ax.set_ylabel("latitude")
    else:
        ax.set_axis_off()
    ax.margins(0.18)

    main_title = (f"Inter-replica latency — {output_path.name} "
                  f"({'RTT' if args.rtt else 'one-way'}, "
                  f"fiber × {args.fiber_fraction:g})")
    subtitle = format_lat_subtitle(replicas, args)
    if subtitle:
        fig.suptitle(main_title, fontsize=11, y=0.97)
        ax.set_title(subtitle, fontsize=9, color="dimgray", pad=6)
    else:
        ax.set_title(main_title, fontsize=11)

    fig.tight_layout()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)


def format_lat_subtitle(replicas, args):
    locs = "  ·  ".join(
        f"{r_id}=({lat:g}, {lon:g})" for r_id, lat, lon in replicas
    )
    v_eff = SPEED_OF_LIGHT_KMS * args.fiber_fraction
    model = (f"model: c={SPEED_OF_LIGHT_KMS:.0f} km/s · "
             f"fiber_fraction={args.fiber_fraction:g} · "
             f"v_eff={v_eff:.0f} km/s · "
             f"propagation lower bound (real RTT typically 1.5–2× higher)")
    return locs + "\n" + model


def main():
    args = parse_args()
    replicas = load_replicas(args)
    if len(replicas) < 2:
        sys.exit(f"error: need at least 2 replicas with geolocation; "
                 f"got {len(replicas)}.")
    latencies = compute_latencies(replicas, args.fiber_fraction, args.rtt)

    print(f"# replicas={len(replicas)}  layout={args.layout}  "
          f"unit={args.unit}  "
          f"{'rtt' if args.rtt else 'one-way'}  "
          f"fiber_fraction={args.fiber_fraction:g}")
    print_matrix(replicas, latencies, args.unit)

    if args.output:
        out_path = Path(args.output)
    else:
        stem = args.service or (Path(args.config).stem if args.config
                                else "latency")
        parent = (Path(args.config).parent if args.config
                  else Path.cwd())
        out_path = parent / f"{stem}-latency.png"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    render_graph(replicas, latencies, out_path, args)
    print(f"# wrote {out_path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
