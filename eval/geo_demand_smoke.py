#!/usr/bin/env python3
"""
geo_demand_smoke.py — Send HTTP requests to the local XDN cluster with the
X-Client-Location header biased to one of the configured AR regions, so the
XdnGeoDemandProfiler will steer the replica group toward that region.

Usage:
    python3 geo_demand_smoke.py --region tokyo --service bookcatalog
    python3 geo_demand_smoke.py --region ohio --requests 500 --rate 30
    python3 geo_demand_smoke.py --lat 48.85 --lon 2.35 --requests 200

The script reads the AR HTTP frontend ports from defaults that match
conf/gigapaxos.xdn.local.properties (AR0..AR3 on 2300..2303). It rotates
across replicas to spread load. Each request adds a small lat/lon jitter so
the profiler sees several adjacent grid cells biased to one region rather
than a single cell, which exercises the centroid math more realistically.

Pre-flight:
  - Cluster running with DEMAND_PROFILE_TYPE=edu.umass.cs.xdn.XdnGeoDemandProfiler
  - Service deployed (e.g. xdn launch bookcatalog --image=fadhilkurnia/xdn-bookcatalog ...)

After the run, wait > MIN_DEMAND_REPORT_PERIOD_MS (10s) plus a reconfiguration
window, then check `xdn service info <name>` to see whether the active set
shifted toward the chosen region.
"""

import argparse
import random
import sys
import time

import requests

# Regions match conf/gigapaxos.xdn.local.properties active.AR*.geolocation.
# AR0, AR1 sit in us-east-1; AR2, AR3 sit in us-west-1.
REGIONS = {
    "us-east-1": (38.5400, -77.9500),  # midpoint of AR0 + AR1
    "us-west-1": (37.5700, -122.1700),  # midpoint of AR2 + AR3
}

# AR HTTP frontend port. The default targets one AR known to be in any service's
# initial group; pass --ar-ports to rotate. Sending to an AR that is NOT part of
# the service's replica group will hang since the local-cluster proxy has no peer
# to forward to.
DEFAULT_AR_PORTS = [2301]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--service",
        default="bookcatalog",
        help="Service name (also sent in the XDN header). Default: bookcatalog",
    )
    parser.add_argument(
        "--path",
        default="/api/books",
        help="HTTP request path on the service. Default: /api/books",
    )
    parser.add_argument(
        "--region",
        choices=sorted(REGIONS.keys()),
        help="Bias requests toward one of the configured AR regions.",
    )
    parser.add_argument("--lat", type=float, help="Override centroid latitude.")
    parser.add_argument("--lon", type=float, help="Override centroid longitude.")
    parser.add_argument(
        "--jitter-deg",
        type=float,
        default=2.0,
        help="Uniform +/- jitter in degrees added to each request's lat/lon. "
        "Default: 2.0 (covers ~220km — well within one AR region).",
    )
    parser.add_argument(
        "--requests",
        type=int,
        default=200,
        help="Total number of requests to send. Default: 200",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=20.0,
        help="Requests per second (best effort). Default: 20",
    )
    parser.add_argument(
        "--ar-ports",
        type=lambda s: [int(p) for p in s.split(",")],
        default=DEFAULT_AR_PORTS,
        help="Comma-separated AR HTTP frontend ports. Default: 2301",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="AR host. Default: 127.0.0.1",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=5.0,
        help="Per-request timeout in seconds. Default: 5",
    )
    return parser.parse_args()


def resolve_centroid(args: argparse.Namespace) -> tuple[float, float]:
    if args.lat is not None and args.lon is not None:
        return args.lat, args.lon
    if args.region is None:
        sys.exit("error: must specify --region or both --lat and --lon")
    return REGIONS[args.region]


def main() -> int:
    args = parse_args()
    centroid_lat, centroid_lon = resolve_centroid(args)
    period = 1.0 / args.rate if args.rate > 0 else 0.0

    print(
        f"target centroid=({centroid_lat:.4f},{centroid_lon:.4f}) "
        f"jitter=+/-{args.jitter_deg} requests={args.requests} rate={args.rate}/s "
        f"service={args.service} ports={args.ar_ports}"
    )

    sent = 0
    ok = 0
    failed = 0
    start = time.monotonic()
    for i in range(args.requests):
        port = args.ar_ports[i % len(args.ar_ports)]
        url = f"http://{args.host}:{port}{args.path}"

        # Clamp lat/lon to legal ranges; the profiler's parser rejects out-of-range
        # values and would silently drop the sample.
        lat = max(-90.0, min(90.0, centroid_lat + random.uniform(-args.jitter_deg, args.jitter_deg)))
        lon = max(-180.0, min(180.0, centroid_lon + random.uniform(-args.jitter_deg, args.jitter_deg)))
        headers = {
            "XDN": args.service,
            "X-Client-Location": f"{lat:.6f},{lon:.6f}",
        }

        try:
            resp = requests.get(url, headers=headers, timeout=args.timeout)
            sent += 1
            if 200 <= resp.status_code < 500:
                ok += 1
            else:
                failed += 1
        except requests.RequestException as e:
            sent += 1
            failed += 1
            print(f"  request {i} -> {url} failed: {e}", file=sys.stderr)

        if (i + 1) % 50 == 0:
            elapsed = time.monotonic() - start
            print(f"  sent={sent} ok={ok} failed={failed} elapsed={elapsed:.1f}s")

        if period > 0:
            target = start + (i + 1) * period
            sleep_for = target - time.monotonic()
            if sleep_for > 0:
                time.sleep(sleep_for)

    elapsed = time.monotonic() - start
    print(f"done. sent={sent} ok={ok} failed={failed} elapsed={elapsed:.1f}s")
    print(
        "Wait > 10s for the demand report to flush, then run "
        f"`xdn service info {args.service}` to check the new active set."
    )
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
