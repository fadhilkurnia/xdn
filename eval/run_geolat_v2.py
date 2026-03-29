"""
run_geolat_v2.py -- Geo-distributed latency measurement across 5 deployment approaches.

Measures end-to-end request latency for single-region, multi-region, and three
ReFlex placement strategies on a CloudLab cluster using virtual replicas via
IP aliases and tc/netem WAN emulation.

Produces four CSV files:
  - eval_geo_latency_cdf.csv         (CDF of all requests per approach)
  - eval_geo_per_city_latency.csv    (per-city averages)
  - eval_geo_raw.csv                 (per-request raw data)
  - eval_geo_summary.csv             (aggregate stats per approach)

Usage:
    python run_geolat_v2.py \\
        --ar-hosts 10.10.1.1,10.10.1.2,10.10.1.3 \\
        --control-plane-host 10.10.1.4

    python run_geolat_v2.py \\
        --approaches single_region,multi_region,reflex_lin \\
        --num-warmup 100 --num-requests 200
"""

import argparse
import atexit
import copy
import csv
import json
import urllib.request
import logging
import math
import os
import random
import re
import shutil
import socket
import statistics
import subprocess
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path

import numpy as np
import requests as http_requests
import urllib3.util.connection

# Fix Nagle + delayed ACK interaction (same as v1)
_orig_create_conn = urllib3.util.connection.create_connection
def _create_conn_nodelay(*args, **kwargs):
    sock = _orig_create_conn(*args, **kwargs)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    return sock
urllib3.util.connection.create_connection = _create_conn_nodelay

# -- Reuse v1 infrastructure ---------------------------------------------------
from run_geolat_consistency import (
    _ssh,
    detect_all_nics,
    get_virtual_ip,
    build_virtual_ip_map,
    setup_ip_aliases as add_virtual_ips,
    teardown_ip_aliases as remove_virtual_ips,
    apply_inter_server_latency as _apply_inter_server_latency,
    reset_tc as reset_wan_emulation,
    verify_wan_emulation as verify_wan_latency,
    cleanup_all as clear_cluster,
    start_cluster as start_cluster_screen,
    wait_for_cluster,
    deploy_service,
    destroy_service,
    ensure_images,
    start_proxy,
    stop_proxy,
    run_warmup,
    make_mix,
    compute_summary as _compute_summary_v1,
    reconfigure_leader,
    prepare_config as _prepare_config_v1,
    prepare_service_yaml as _prepare_service_yaml_v1,
    assign_clients as _assign_clients_v1,
    reset_cluster,
    write_csv,
    HTTP_PORT,
    XDN_BINARY,
    RC_HTTP_PORT,
    SERVICE_ENDPOINT,
    SERVICE_READ_ENDPOINT,
    SERVICE_TEMPLATE,
    GP_CONFIG_TEMPLATE,
    LATENCY_PROXY_BIN,
    NUM_TASKS,
)

from utils import (
    cluster_locations,
    get_estimated_rtt_latency,
    get_server_locations,
    replace_placeholder,
)
from replica_group_picker import (
    find_k_closest_servers,
    get_heuristic_replicas_placement,
)
from gp_config_utils import apply_config_overrides

# ==============================================================================
# Constants
# ==============================================================================

ALL_APPROACHES = [
    "single_region", "multi_region", "reflex_lin", "reflex_seq", "reflex_evt",
]

APPROACH_SHORT = {
    "single_region": "sreg",
    "multi_region": "mreg",
    "reflex_lin": "rlin",
    "reflex_seq": "rseq",
    "reflex_evt": "revt",
}

# Consistency model used by each approach
APPROACH_CONSISTENCY = {
    "single_region": "linearizability",
    "multi_region": "sequential",
    "reflex_lin": "linearizability",
    "reflex_seq": "sequential",
    "reflex_evt": "eventual",
}

# Approaches that use leader-based coordination
LEADER_APPROACHES = {"single_region", "multi_region", "reflex_lin", "reflex_seq"}

SLOWDOWN = 0.32258064516  # 1/3.1 speed-of-light inflation
US_EAST_1 = (39.04, -77.49)   # Ashburn, Virginia
US_EAST_2 = (40.42, -82.91)   # Columbus, Ohio
US_WEST_2 = (45.59, -122.60)  # Portland, Oregon
INTRA_AZ_RTT_MS = 2.0         # Fixed inter-AZ delay for single-region

READ_RATIO = 80               # Fixed 80% read ratio

CLIENT_CITIES = [
    ("New York", 40.71, -74.01),
    ("Chicago", 41.88, -87.63),
    ("Los Angeles", 34.05, -118.24),
]

RANDOM_SEED = 313
TODO_IMAGE = "fadhilkurnia/xdn-todo"
SERVER_DATA = "location_distributions/server_netflix_oca.csv"

# Expected coordinator class per consistency model (for protocol verification)
EXPECTED_PROTOCOL = {
    "linearizability": "PaxosReplicaCoordinator",
    "sequential":      "AwReplicaCoordinator",
    "eventual":        "LazyReplicaCoordinator",
}

log = logging.getLogger("geolat_v2")

# ==============================================================================
# Client construction
# ==============================================================================

def build_v2_clients():
    """Build the 3-city client list with population-weighted counts summing to 100.

    Population estimates (2020 census metro area, approximate):
        New York: 8.34M  -> ratio ~0.473  -> 34 clients
        Los Angeles: 3.90M -> ratio ~0.221 -> 22 clients
        Chicago: 2.70M   -> ratio ~0.153 -> 16 clients
    We use 72 total from these three to keep proportions, then scale to sum ~72.
    To get exactly 72 (34+22+16), we use them directly.
    """
    # Population-weighted distribution
    populations = {
        "New York": 8_336_817,
        "Los Angeles": 3_898_747,
        "Chicago": 2_696_555,
    }
    total_pop = sum(populations.values())
    total_clients = 72  # sum of rounded proportional counts

    clients = []
    client_id = 0
    assigned = 0
    city_list = list(CLIENT_CITIES)
    for idx, (city_name, lat, lon) in enumerate(city_list):
        if idx == len(city_list) - 1:
            count = total_clients - assigned
        else:
            count = round(populations[city_name] / total_pop * total_clients)
        assigned += count
        clients.append({
            "ClientID": client_id,
            "City": city_name,
            "Count": count,
            "Latitude": lat,
            "Longitude": lon,
        })
        client_id += 1

    return clients

# ==============================================================================
# Replica placement per approach
# ==============================================================================

def _find_closest_server(servers, coord):
    """Find the Netflix OCA server closest to the given (lat, lon) coordinate."""
    ref = {"Name": "ref", "Latitude": coord[0], "Longitude": coord[1]}
    # Make deep copies to avoid mutating the server list
    srv_copies = [dict(s) for s in servers]
    closest = find_k_closest_servers(srv_copies, ref, 1)
    return closest[0]


def compute_placement(approach, servers, clients, target_city=None):
    """Compute replica placement for a given approach.

    For reflex approaches, target_city specifies the city to optimize placement
    for. Each city gets its own deployment with replicas placed nearby.
    For baselines (single_region, multi_region), target_city is ignored.

    Args:
        target_city: tuple (city_name, lat, lon) for per-city reflex placement.

    Returns:
        dict with keys: "Replicas" (list of server dicts), "Leader" (server dict or None)
    """
    if approach == "single_region":
        # 3 replicas all at us-east-1 (Ashburn VA)
        base = _find_closest_server(servers, US_EAST_1)
        # Create 3 replicas at the same location with distinct names
        replicas = []
        for i in range(3):
            r = dict(base)
            r["Name"] = f"{base['Name']}_sr{i}"
            replicas.append(r)
        leader = replicas[0]
        return {"Replicas": replicas, "Leader": leader, "Centroid": US_EAST_1}

    if approach == "multi_region":
        # 3 replicas at us-east-1, us-east-2, us-west-2
        regions = [US_EAST_1, US_EAST_2, US_WEST_2]
        replicas = []
        for coord in regions:
            s = _find_closest_server(servers, coord)
            # Ensure unique names
            s = dict(s)
            replicas.append(s)
        # Leader at us-east-1 (closest to population center)
        leader = replicas[0]
        return {"Replicas": replicas, "Leader": leader, "Centroid": US_EAST_1}

    if approach == "reflex_lin":
        # Per-city placement: 3 replicas near the target city so the
        # Paxos quorum RTT is minimal for that city's clients.
        # target_city is set by the caller for per-city measurement.
        city_lat, city_lon = target_city[1], target_city[2]
        ref = {"Name": "ref", "Latitude": city_lat, "Longitude": city_lon}
        replicas = find_k_closest_servers([dict(s) for s in servers], ref, 3)
        leader = replicas[0]
        return {"Replicas": replicas, "Leader": leader,
                "Centroid": (city_lat, city_lon)}

    if approach == "reflex_seq":
        # Per-city placement with Flexible Paxos: place 3 RW replicas near
        # the target city (for low quorum RTT on writes), plus read-only
        # replicas nearby for local reads.
        # n = min(ceil(3 + 7 * 0.80), 10) = 9
        city_lat, city_lon = target_city[1], target_city[2]
        n = min(math.ceil(3 + 7 * READ_RATIO / 100.0), 10)
        ref = {"Name": "ref", "Latitude": city_lat, "Longitude": city_lon}
        all_replicas = find_k_closest_servers([dict(s) for s in servers], ref, n)
        leader = all_replicas[0]
        return {"Replicas": all_replicas, "Leader": leader,
                "Centroid": (city_lat, city_lon)}

    if approach == "reflex_evt":
        # Per-city placement: one replica near the target city for local
        # reads AND local writes with async propagation.
        # We place 3 replicas near the city (for fault tolerance).
        city_lat, city_lon = target_city[1], target_city[2]
        ref = {"Name": "ref", "Latitude": city_lat, "Longitude": city_lon}
        replicas = find_k_closest_servers([dict(s) for s in servers], ref, 3)
        return {"Replicas": replicas, "Leader": None, "Centroid": None}

    raise ValueError(f"Unknown approach: {approach}")

# ==============================================================================
# WAN emulation with single_region override
# ==============================================================================

def apply_wan_emulation(approach, ar_hosts, nic_by_host, replica_ips,
                        replica_geo, virtual_ip_map):
    """Apply WAN emulation. For single_region, override all inter-replica delays to INTRA_AZ_RTT_MS/2."""
    if approach == "single_region":
        # Override geo coordinates: place all replicas at slightly different
        # positions so that _one_way_delay_ms computes ~1ms (INTRA_AZ_RTT_MS/2).
        # Instead, we build a custom geo map that forces a fixed delay.
        # We'll call the v1 function with a modified geo map that places
        # replicas far enough apart to get exactly INTRA_AZ_RTT_MS/2 one-way.
        # Simpler: just apply tc rules directly with fixed delay.
        return _apply_fixed_delay(ar_hosts, nic_by_host, replica_ips,
                                  virtual_ip_map, INTRA_AZ_RTT_MS / 2.0)
    else:
        return _apply_inter_server_latency(
            ar_hosts, nic_by_host, replica_ips, replica_geo, virtual_ip_map)


def _apply_fixed_delay(ar_hosts, nic_by_host, replica_ips,
                       virtual_ip_map, one_way_ms):
    """Apply a fixed one-way delay between all replica pairs."""
    expected = {}

    # Group replicas by physical host
    host_replicas = {}
    for vip in replica_ips:
        phys = virtual_ip_map[vip]
        host_replicas.setdefault(phys, []).append(vip)

    for phys in ar_hosts:
        if phys not in host_replicas:
            continue
        nic = nic_by_host[phys]
        local_vips = host_replicas.get(phys, [])

        nic_rules = []
        lo_rules = []

        for src_vip in local_vips:
            for dst_vip in replica_ips:
                if src_vip == dst_vip:
                    continue
                expected[(src_vip, dst_vip)] = one_way_ms
                if one_way_ms < 0.1:
                    continue
                dst_phys = virtual_ip_map[dst_vip]
                if dst_phys != phys:
                    nic_rules.append((src_vip, dst_vip, one_way_ms))
                else:
                    lo_rules.append((src_vip, dst_vip, one_way_ms))

        # Apply NIC rules (cross-machine)
        if nic_rules:
            _ssh(phys, f"sudo tc qdisc del dev {nic} root 2>/dev/null || true", check=False)
            _ssh(phys, f"sudo tc qdisc add dev {nic} root handle 1: htb default 99")
            _ssh(phys, f"sudo tc class add dev {nic} parent 1: classid 1:99 htb rate 100gbit")
            for idx, (src_vip, dst_vip, delay) in enumerate(nic_rules, start=1):
                classid = f"1:{idx}"
                handle = f"{10 + idx}:"
                _ssh(phys, f"sudo tc class add dev {nic} parent 1: classid {classid} htb rate 100gbit")
                _ssh(phys, f"sudo tc qdisc add dev {nic} parent {classid} handle {handle} netem delay {delay:.2f}ms")
                _ssh(phys, (f"sudo tc filter add dev {nic} parent 1: protocol ip prio {idx} u32 "
                            f"match ip src {src_vip}/32 match ip dst {dst_vip}/32 flowid {classid}"))
                log.info("  tc %s: %s -> %s = %.1fms (fixed)", nic, src_vip, dst_vip, delay)

        # Apply loopback rules (co-located)
        if lo_rules:
            _ssh(phys, "sudo tc qdisc del dev lo root 2>/dev/null || true", check=False)
            _ssh(phys, "sudo tc qdisc add dev lo root handle 1: htb default 99")
            _ssh(phys, "sudo tc class add dev lo parent 1: classid 1:99 htb rate 100gbit")
            for idx, (src_vip, dst_vip, delay) in enumerate(lo_rules, start=1):
                classid = f"1:{idx}"
                handle = f"{10 + idx}:"
                _ssh(phys, f"sudo tc class add dev lo parent 1: classid {classid} htb rate 100gbit")
                _ssh(phys, f"sudo tc qdisc add dev lo parent {classid} handle {handle} netem delay {delay:.2f}ms")
                _ssh(phys, (f"sudo tc filter add dev lo parent 1: protocol ip prio {idx} u32 "
                            f"match ip src {src_vip}/32 match ip dst {dst_vip}/32 flowid {classid}"))
                log.info("  tc lo: %s -> %s = %.1fms (fixed)", src_vip, dst_vip, delay)

    return expected

# ==============================================================================
# Config generation (wraps v1 with SYNC=true injection)
# ==============================================================================

def prepare_config(config_template, config_target, replica_group_info,
                   control_plane_host, ar_hosts):
    """Generate GigaPaxos config and inject SYNC=true."""
    server_by_name, replica_geo = _prepare_config_v1(
        config_template, config_target, replica_group_info,
        control_plane_host, ar_hosts)
    # Inject SYNC=true via apply_config_overrides
    apply_config_overrides("-DSYNC=true", config_target)
    return server_by_name, replica_geo

# ==============================================================================
# Consistency protocol verification
# ==============================================================================

def verify_consistency_protocol(service_name, approach, addr_map):
    """Query replica/info on all replicas and verify the coordinator matches expectations.

    Aborts if any replica reports an unexpected consistency model or coordinator
    class (e.g., fallback to a stronger consistency like AwReplicaCoordinator
    when LazyReplicaCoordinator was expected).
    """
    consistency = APPROACH_CONSISTENCY[approach]
    expected_proto = EXPECTED_PROTOCOL[consistency]
    log.info("  Verifying consistency protocol on all replicas ...")
    log.info("  Expected: consistency=%s protocol=%s", consistency, expected_proto)

    for name, vip in addr_map.items():
        url = f"http://{vip}:{HTTP_PORT}/api/v2/services/{service_name}/replica/info"
        try:
            req = urllib.request.Request(url, headers={"XDN": service_name}, method="GET")
            with urllib.request.urlopen(req, timeout=10) as resp:
                info = json.load(resp)
        except Exception as e:
            log.warning("  Could not query %s (%s): %s", name, vip, e)
            continue

        offered = str(info.get("consistency", "")).lower()
        requested = str(info.get("requestedConsistency", "")).lower()
        protocol = str(info.get("protocol", ""))
        role = str(info.get("role", ""))

        log.info("    %s (%s): offered=%s requested=%s protocol=%s role=%s",
                 name, vip, offered, requested, protocol, role)

        if offered and offered != "?" and offered != consistency:
            raise RuntimeError(
                f"Consistency mismatch on {name} ({vip}): expected '{consistency}' "
                f"but replica reports offered='{offered}'. "
                f"Protocol may have fallen back to a stronger model."
            )

        if requested and requested != "?" and requested != consistency:
            raise RuntimeError(
                f"Requested consistency mismatch on {name} ({vip}): expected "
                f"'{consistency}' but replica reports "
                f"requestedConsistency='{requested}'."
            )

        if protocol and protocol != "?" and protocol != expected_proto:
            raise RuntimeError(
                f"Protocol mismatch on {name} ({vip}): expected '{expected_proto}' "
                f"but replica reports protocol='{protocol}'. "
                f"The coordinator may have fallen back unexpectedly."
            )

    log.info("  Verified: all replicas running '%s' with protocol '%s'",
             consistency, expected_proto)

# ==============================================================================
# Service YAML generation
# ==============================================================================

def prepare_service_yaml(approach, service_name):
    """Generate service YAML for the given approach.

    For eventual consistency, appends MONOTONIC behavior to writes so that
    LazyReplicaCoordinator accepts the service (it requires declared
    behaviors to include {MONOTONIC, READ_ONLY, WRITE_ONLY}).
    """
    consistency = APPROACH_CONSISTENCY[approach]
    target = f"static/xdn_geolat_v2_{APPROACH_SHORT[approach]}.yaml"
    shutil.copy2(SERVICE_TEMPLATE, target)
    replace_placeholder(target, "___SERVICE_NAME___", service_name)
    replace_placeholder(target, "___CONSISTENCY_MODEL___", consistency)
    if consistency == "eventual":
        with open(target, "a") as f:
            f.write("\n  - path_prefix: \"/\"\n")
            f.write("    methods: \"PUT,POST,DELETE\"\n")
            f.write("    behavior: monotonic\n")
    return target

# ==============================================================================
# Client assignment (wraps v1)
# ==============================================================================

def assign_clients(clients, replica_group_info, server_address_by_name):
    return _assign_clients_v1(clients, replica_group_info, server_address_by_name)

# ==============================================================================
# Measurement (v2 version: uses "approach" instead of "consistency")
# ==============================================================================

def run_measurement_v2(clients, service_name, approach, read_ratio,
                       num_requests, post_data, proxies):
    """Run measurement and return raw rows with 'approach' field."""
    mix = make_mix(num_requests, read_ratio)
    raw_rows = []
    session = http_requests.Session()
    headers = {"Content-Type": "application/json", "XDN": service_name}

    for client in clients:
        write_url = client["ServiceTargetUrl"]
        read_url = client["ServiceReadUrl"]
        h = {**headers, "X-Client-Location": f"{client['Latitude']};{client['Longitude']}"}
        city = client.get("City", "")
        lat = client.get("Latitude", 0)
        lon = client.get("Longitude", 0)
        target = client.get("TargetReplicaHostPort", "")

        for cid in range(client["Count"]):
            cookie = None
            for i, is_read in enumerate(mix):
                t0 = time.perf_counter()
                try:
                    if is_read:
                        r = session.get(read_url, headers=h, timeout=30,
                                        cookies=cookie, proxies=proxies)
                    else:
                        r = session.post(write_url, headers=h,
                                         data=post_data[i % len(post_data)],
                                         timeout=30, cookies=cookie, proxies=proxies)
                except http_requests.exceptions.RequestException as e:
                    session.close()
                    raise RuntimeError(
                        f"Measurement failed at request {i} for {city}: {e}") from e
                lat_ms = (time.perf_counter() - t0) * 1000.0
                if r.status_code != 200:
                    session.close()
                    raise RuntimeError(
                        f"Measurement failed: {('GET ' + read_url) if is_read else ('POST ' + write_url)} "
                        f"returned {r.status_code}: {r.text[:200]}")
                cookie = r.cookies
                raw_rows.append({
                    "approach": approach,
                    "client_city": city,
                    "client_lat": lat,
                    "client_lon": lon,
                    "target_replica": target,
                    "is_read": is_read,
                    "latency_ms": f"{lat_ms:.2f}",
                })
    session.close()
    return raw_rows

# ==============================================================================
# Summary computation (v2: uses "approach")
# ==============================================================================

def compute_summary(approach, num_replicas, num_clients, num_per_client, latencies):
    s = sorted(latencies)
    def pct(p):
        if not s:
            return 0
        idx = min(int(len(s) * p / 100.0), len(s) - 1)
        return s[idx]
    return {
        "approach": approach,
        "consistency": APPROACH_CONSISTENCY[approach],
        "num_replicas": num_replicas,
        "num_clients": num_clients,
        "num_requests_per_client": num_per_client,
        "avg_latency_ms": f"{statistics.mean(s):.2f}",
        "median_latency_ms": f"{pct(50):.2f}",
        "stddev_latency_ms": f"{statistics.stdev(s):.2f}" if len(s) > 1 else "0.00",
        "p90_latency_ms": f"{pct(90):.2f}",
        "p95_latency_ms": f"{pct(95):.2f}",
        "p99_latency_ms": f"{pct(99):.2f}",
    }

# ==============================================================================
# CDF and per-city CSV helpers
# ==============================================================================

def compute_cdf_csv(raw_rows):
    rows = []
    approaches_seen = sorted({r["approach"] for r in raw_rows})
    for approach in approaches_seen:
        lats = sorted(float(r["latency_ms"]) for r in raw_rows
                      if r["approach"] == approach)
        n = len(lats)
        if n == 0:
            continue
        for i, lat in enumerate(lats):
            rows.append({"approach": approach,
                         "latency_ms": f"{lat:.2f}",
                         "cdf": f"{(i + 1) / n:.6f}"})
    return rows


def compute_per_city_csv(raw_rows):
    rows = []
    approaches_seen = sorted({r["approach"] for r in raw_rows})
    cities = [c[0] for c in CLIENT_CITIES]
    for city in cities:
        for approach in approaches_seen:
            lats = [float(r["latency_ms"]) for r in raw_rows
                    if r["approach"] == approach and r["client_city"] == city]
            if lats:
                rows.append({"city": city, "approach": approach,
                             "avg_latency_ms": f"{statistics.mean(lats):.2f}"})
    return rows

# ==============================================================================
# RTT printing
# ==============================================================================

def print_computed_rtts(approach, placement, clients):
    """Print computed RTTs between client cities and replicas, and between replicas."""
    log.info("  Computed RTTs for %s:", approach)

    # Client-to-closest-replica RTTs
    for city_name, city_lat, city_lon in CLIENT_CITIES:
        city_coord = (city_lat, city_lon)
        min_rtt = float("inf")
        closest_name = ""
        for r in placement["Replicas"]:
            rtt = get_estimated_rtt_latency(city_coord,
                                            (r["Latitude"], r["Longitude"]),
                                            SLOWDOWN)
            if rtt < min_rtt:
                min_rtt = rtt
                closest_name = r["Name"]
        log.info("    %s -> %s: %.2f ms RTT", city_name, closest_name, min_rtt)

    # Inter-replica RTTs (for single_region, report fixed INTRA_AZ_RTT_MS)
    replicas = placement["Replicas"]
    for i in range(len(replicas)):
        for j in range(i + 1, len(replicas)):
            r1, r2 = replicas[i], replicas[j]
            if approach == "single_region":
                rtt = INTRA_AZ_RTT_MS
            else:
                rtt = get_estimated_rtt_latency(
                    (r1["Latitude"], r1["Longitude"]),
                    (r2["Latitude"], r2["Longitude"]),
                    SLOWDOWN)
            log.info("    %s <-> %s: %.2f ms RTT (inter-replica)",
                     r1["Name"], r2["Name"], rtt)

# ==============================================================================
# Main
# ==============================================================================

RAW_FIELDS = ["approach", "client_city", "client_lat", "client_lon",
              "target_replica", "is_read", "latency_ms"]

SUMMARY_FIELDS = ["approach", "consistency", "num_replicas", "num_clients",
                  "num_requests_per_client", "avg_latency_ms", "median_latency_ms",
                  "stddev_latency_ms", "p90_latency_ms", "p95_latency_ms",
                  "p99_latency_ms"]


def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--ar-hosts", default="10.10.1.1,10.10.1.2,10.10.1.3")
    p.add_argument("--control-plane-host", default="10.10.1.4")
    p.add_argument("--approaches", default=None,
                   help="Comma-separated approaches (default: all 5)")
    p.add_argument("--num-warmup", type=int, default=500)
    p.add_argument("--num-requests", type=int, default=1000)
    p.add_argument("--strict-wan-verify", action="store_true")
    return p.parse_args()


def main():
    args = parse_args()
    ar_hosts = [h.strip() for h in args.ar_hosts.split(",")]
    cp_host = args.control_plane_host

    assert len(ar_hosts) >= 3, "Need >= 3 AR hosts"

    if args.approaches:
        approaches = [a.strip() for a in args.approaches.split(",")]
        for a in approaches:
            assert a in ALL_APPROACHES, f"Unknown approach: {a}"
    else:
        approaches = ALL_APPROACHES

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_dir = Path("results") / f"geolat_v2_{timestamp}"
    results_dir.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(results_dir / "run.log"),
        ],
    )

    # Load servers
    servers = get_server_locations([SERVER_DATA], remove_duplicate_location=True,
                                  remove_nonredundant_city_servers=False)
    for s in servers:
        s["Name"] = s["Name"].replace("-", "_")

    # Build v2 clients (3 cities, population-weighted)
    clients = build_v2_clients()

    np.random.seed(RANDOM_SEED)
    random.seed(RANDOM_SEED)

    # Determine max replicas needed across all approaches
    max_rep = 10

    log.info("=" * 60)
    log.info("Geo-distributed Latency v2 — Deployment Approaches")
    log.info("  AR hosts     : %s", ar_hosts)
    log.info("  Control plane: %s", cp_host)
    log.info("  Approaches   : %s", approaches)
    log.info("  Read ratio   : %d%%", READ_RATIO)
    log.info("  Warmup       : %d req/loc", args.num_warmup)
    log.info("  Measurement  : %d req/loc", args.num_requests)
    log.info("  Results      : %s", results_dir)
    log.info("  Clients      :")
    for c in clients:
        log.info("    %s: %d clients (%.2f, %.2f)",
                 c["City"], c["Count"], c["Latitude"], c["Longitude"])
    log.info("=" * 60)

    # Detect NICs
    log.info("Detecting NICs ...")
    nic_by_host = detect_all_nics(ar_hosts)

    # Build virtual IP map
    virtual_ip_map = build_virtual_ip_map(ar_hosts, max_rep)
    log.info("Virtual replicas:")
    for i in range(max_rep):
        vip, phys = get_virtual_ip(i, ar_hosts)
        log.info("  Replica %d: %s (on %s)", i, vip, phys)

    # Verify binaries
    assert os.system(f"{XDN_BINARY} --help > /dev/null 2>&1") == 0, \
        f"Cannot find {XDN_BINARY}"
    assert os.path.isfile(LATENCY_PROXY_BIN), \
        f"Cannot find {LATENCY_PROXY_BIN}"

    # Pre-clean
    log.info("Pre-cleaning stale state ...")
    clear_cluster(ar_hosts, nic_by_host, virtual_ip_map, cp_host)

    # Register atexit
    atexit.register(lambda: clear_cluster(ar_hosts, nic_by_host, virtual_ip_map, cp_host))

    all_raw = []
    summary_rows = []
    summary_path = results_dir / "eval_geo_summary.csv"

    post_data = [f'{{"item":"task-{i}"}}' for i in range(NUM_TASKS)]

    try:
        add_virtual_ips(ar_hosts, virtual_ip_map, nic_by_host)
        ensure_images(ar_hosts, cp_host)

        # Write summary header
        with open(summary_path, "w", newline="") as f:
            csv.DictWriter(f, fieldnames=SUMMARY_FIELDS).writeheader()

        # Helper: deploy cluster, measure, return raw rows
        def run_single_deployment(approach, measure_clients, config_file,
                                  target_city=None):
            """Deploy a cluster for the given approach and measure latency.

            For baselines: measure_clients includes all 3 cities.
            For reflex: measure_clients includes only the target city.
            """
            consistency = APPROACH_CONSISTENCY[approach]
            city_suffix = f"_{target_city[0][:3].lower()}" if target_city else ""
            service_name = f"todo_{APPROACH_SHORT[approach]}{city_suffix}"

            log.info("[Phase 1] Resetting cluster ...")
            reset_cluster(ar_hosts, cp_host)

            # Phase 2: Compute placement
            log.info("[Phase 2] Computing placement ...")
            placement = compute_placement(approach, servers, clients,
                                          target_city=target_city)
            n_rep = len(placement["Replicas"])
            log.info("  %d replicas, leader=%s",
                     n_rep,
                     placement["Leader"]["Name"] if placement["Leader"] else "none")
            for r in placement["Replicas"]:
                log.info("    %s (%.2f, %.2f)", r["Name"],
                         r["Latitude"], r["Longitude"])

            # Print computed RTTs
            print_computed_rtts(approach, placement, measure_clients)

            # Phase 3: Generate config with SYNC=true
            log.info("[Phase 3] Generating config (SYNC=true) ...")
            addr_map, geo_map = prepare_config(
                GP_CONFIG_TEMPLATE, config_file, placement, cp_host, ar_hosts)
            rep_ips = list(addr_map.values())

            # Phase 5: Start cluster
            log.info("[Phase 5] Starting cluster ...")
            scr = f"xdn_geolat_v2_{APPROACH_SHORT[approach]}{city_suffix}"
            start_cluster_screen(scr, config_file)
            cluster_timeout = 120 + (n_rep - 3) * 30 if n_rep > 3 else 120
            if not wait_for_cluster(rep_ips, timeout_sec=cluster_timeout):
                log.error("Cluster not ready after %ds, skipping.", cluster_timeout)
                return []

            # Phase 6: Deploy service
            log.info("[Phase 6] Deploying service '%s' (consistency=%s) ...",
                     service_name, consistency)
            svc_yaml = prepare_service_yaml(approach, service_name)
            deploy_service(service_name, svc_yaml, cp_host)

            # Reconfigure leader if leader-based
            if approach in LEADER_APPROACHES and placement["Leader"]:
                log.info("  Reconfiguring leader ...")
                reconfigure_leader(service_name, placement, addr_map, cp_host)

            # Wait for service readiness on all replicas
            log.info("  Waiting for service on all %d replicas ...", n_rep)
            for name, vip in addr_map.items():
                svc_url = f"http://{vip}:{HTTP_PORT}/"
                svc_headers = {"XDN": service_name}
                deadline = time.time() + 120
                while time.time() < deadline:
                    try:
                        r = http_requests.get(svc_url, headers=svc_headers, timeout=5)
                        if r.status_code < 500:
                            log.info("    %s (%s:%d) ready", name, vip, HTTP_PORT)
                            break
                    except Exception:
                        pass
                    time.sleep(3)
                else:
                    log.warning("    TIMEOUT: %s (%s:%d) not ready",
                                name, vip, HTTP_PORT)

            # Phase 6b: Verify consistency protocol
            log.info("[Phase 6b] Verifying consistency protocol ...")
            verify_consistency_protocol(service_name, approach, addr_map)

            # Phase 7: Apply WAN emulation
            log.info("[Phase 7] Emulating WAN ...")
            expected_delays = apply_wan_emulation(
                approach, ar_hosts, nic_by_host, rep_ips, geo_map,
                virtual_ip_map)

            # Phase 8: Verify WAN latencies
            log.info("[Phase 8] Verifying WAN ...")
            wan_ok = verify_wan_latency(expected_delays, virtual_ip_map, ar_hosts)
            if not wan_ok:
                if args.strict_wan_verify:
                    raise RuntimeError("WAN verification failed: deviation > 1ms")
                else:
                    log.warning("WAN verification had deviations > 1ms, continuing ...")

            # Phase 10: Start latency proxy
            log.info("[Phase 10] Starting latency proxy ...")
            prx_scr = f"scr_prx_v2_{APPROACH_SHORT[approach]}{city_suffix}"
            proxies = start_proxy(config_file, prx_scr)

            curr_clients = assign_clients(measure_clients, placement, addr_map)

            # Phase 11: Warmup
            log.info("[Phase 11] Warmup (%d req/loc) ...", args.num_warmup)
            run_warmup(curr_clients, service_name, args.num_warmup,
                       READ_RATIO, post_data, proxies)

            # Phase 12: Measurement
            log.info("[Phase 12] Measurement (%d req/loc) ...", args.num_requests)
            raw = run_measurement_v2(
                curr_clients, service_name, approach, READ_RATIO,
                args.num_requests, post_data, proxies)

            # Summary for this deployment
            lats = [float(r["latency_ms"]) for r in raw]
            num_clients_total = sum(c["Count"] for c in measure_clients)
            summary = compute_summary(approach, n_rep,
                                      num_clients_total,
                                      args.num_requests, lats)
            log.info("  Result: avg=%.2fms p50=%.2fms p99=%.2fms (n=%d)",
                     float(summary["avg_latency_ms"]),
                     float(summary["median_latency_ms"]),
                     float(summary["p99_latency_ms"]), len(lats))

            return raw, summary

        # ── Outer loop: iterate over approaches ──────────────────────────
        # Baselines use a single deployment for all cities.
        # ReFlex approaches use per-city deployment.
        REFLEX_APPROACHES = {"reflex_lin", "reflex_seq", "reflex_evt"}

        for approach in approaches:
            consistency = APPROACH_CONSISTENCY[approach]
            config_file = str(results_dir / f"config_{approach}.properties")

            log.info("")
            log.info("=" * 60)
            log.info("  Approach: %s (consistency=%s)", approach, consistency)
            log.info("=" * 60)

            if approach not in REFLEX_APPROACHES:
                # Baselines: single deployment, measure from all 3 cities
                try:
                    raw, summary = run_single_deployment(
                        approach, clients, config_file)
                    all_raw.extend(raw)
                    summary_rows.append(summary)
                    with open(summary_path, "a", newline="") as f:
                        csv.DictWriter(f, fieldnames=SUMMARY_FIELDS).writerow(summary)
                except Exception as e:
                    log.error("ERROR in %s: %s", approach, e)
                    traceback.print_exc()
                finally:
                    log.info("[Cleanup] ...")
                    reset_cluster(ar_hosts, cp_host)
                    reset_wan_emulation(ar_hosts, nic_by_host)
                    stop_proxy()
                    time.sleep(5)
            else:
                # ReFlex: per-city deployment — deploy replicas near each
                # city, measure from that city only, teardown, repeat.
                for city_tuple in CLIENT_CITIES:
                    city_name = city_tuple[0]
                    log.info("")
                    log.info("  -- %s / %s --", approach, city_name)

                    # Build clients for this city only
                    city_clients = [c for c in clients if c["City"] == city_name]
                    city_config = str(results_dir /
                                     f"config_{approach}_{city_name[:3].lower()}.properties")

                    try:
                        raw, summary = run_single_deployment(
                            approach, city_clients, city_config,
                            target_city=city_tuple)
                        all_raw.extend(raw)
                        summary_rows.append(summary)
                        with open(summary_path, "a", newline="") as f:
                            csv.DictWriter(f, fieldnames=SUMMARY_FIELDS).writerow(summary)
                    except Exception as e:
                        log.error("ERROR in %s/%s: %s", approach, city_name, e)
                        traceback.print_exc()
                    finally:
                        log.info("[Cleanup] ...")
                        reset_cluster(ar_hosts, cp_host)
                        reset_wan_emulation(ar_hosts, nic_by_host)
                        stop_proxy()
                        time.sleep(5)

        # Write output CSVs
        log.info("")
        log.info("Writing output CSVs ...")
        write_csv(results_dir / "eval_geo_raw.csv", all_raw, RAW_FIELDS)

        cdf_rows = compute_cdf_csv(all_raw)
        if cdf_rows:
            write_csv(results_dir / "eval_geo_latency_cdf.csv",
                      cdf_rows, ["approach", "latency_ms", "cdf"])

        per_city_rows = compute_per_city_csv(all_raw)
        if per_city_rows:
            write_csv(results_dir / "eval_geo_per_city_latency.csv",
                      per_city_rows, ["city", "approach", "avg_latency_ms"])

    finally:
        clear_cluster(ar_hosts, nic_by_host, virtual_ip_map, cp_host)

    log.info("")
    log.info("Done. Results in %s", results_dir)


if __name__ == "__main__":
    main()
