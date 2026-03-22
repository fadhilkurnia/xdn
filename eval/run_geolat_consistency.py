"""
run_geolat_consistency.py -- Geo-distributed latency vs. consistency model evaluation.

Measures end-to-end request latency across consistency models on a small CloudLab
cluster (3 AR machines + 1 RC + 1 driver) using virtual replicas via IP aliases
and tc/netem WAN emulation on both physical NIC and loopback.

Produces four CSV files:
  - eval_consistency_latency_cdf.csv       (Figure 20a: CDF plot)
  - eval_consistency_top3_cities.csv       (Figure 20b: top-3 city bars)
  - eval_geolat_consistency_summary.csv    (aggregate stats)
  - eval_geolat_consistency_raw.csv        (per-request raw data)

Usage:
    python run_geolat_consistency.py \\
        --ar-hosts 10.10.1.1,10.10.1.2,10.10.1.3 \\
        --control-plane-host 10.10.1.4

    python run_geolat_consistency.py \\
        --consistency linearizability,sequential,eventual \\
        --num-warmup 100 --num-requests 200
"""

import argparse
import atexit
import copy
import csv
import json
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

# Fix Nagle + delayed ACK interaction: Python's requests/urllib3 sends POST
# headers and body as separate TCP segments. Without TCP_NODELAY, Nagle's
# algorithm delays the body by ~40ms (waiting for ACK of headers). This
# monkey-patch ensures all sockets set TCP_NODELAY.
_orig_create_conn = urllib3.util.connection.create_connection
def _create_conn_nodelay(*args, **kwargs):
    sock = _orig_create_conn(*args, **kwargs)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    return sock
urllib3.util.connection.create_connection = _create_conn_nodelay

from utils import (
    cluster_locations,
    get_client_count_per_city,
    get_estimated_rtt_latency,
    get_per_city_clients,
    get_population_ratio_per_city,
    get_server_locations,
    replace_placeholder,
)
from replica_group_picker import (
    find_k_closest_servers,
    get_client_locations,
    get_heuristic_replicas_placement,
)

# ==============================================================================
# Constants
# ==============================================================================

ALL_CONSISTENCY_MODELS = [
    "linearizability", "sequential", "causal", "pram",
    "monotonic_reads", "writes_follow_reads", "read_your_writes",
    "monotonic_writes", "eventual",
]

CONSISTENCY_SHORT = {
    "linearizability": "lin", "sequential": "seq", "causal": "cau",
    "pram": "prm", "monotonic_reads": "mrd", "writes_follow_reads": "wfr",
    "read_your_writes": "ryw", "monotonic_writes": "mwr", "eventual": "evt",
}

LEADER_MODELS = {"linearizability", "sequential"}
TOP_3_CITIES = ("New York", "Los Angeles", "Chicago")
CDF_SERIES = ("linearizability", "sequential", "eventual")

DEFAULT_READ_RATIOS = [0, 20, 40, 60, 80, 100]

POPULATION_DATA = "location_distributions/client_us_metro_population.csv"
SERVER_DATA = "location_distributions/server_netflix_oca.csv"
SERVICE_TEMPLATE = "static/xdn_consistency_template.yaml"
GP_CONFIG_TEMPLATE = "static/gigapaxos.xdn.cloudlab.template.properties"
SERVICE_ENDPOINT = "/api/todo/tasks"
SERVICE_READ_ENDPOINT = "/api/todo/tasks/task-8"
XDN_BINARY = "../bin/xdn"
RC_HTTP_PORT = "3300"
SLOWDOWN = 0.32258064516
RANDOM_SEED = 313
NUM_CLIENTS = 100
NUM_TASKS = 100
HTTP_PORT = 2300
TODO_IMAGE = "fadhilkurnia/xdn-todo"
LATENCY_PROXY_BIN = "xdn-latency-proxy/go/latency-injector"

log = logging.getLogger("geolat")

# ==============================================================================
# SSH helper
# ==============================================================================

def _ssh(host, cmd, check=True, capture=False, timeout=60):
    return subprocess.run(
        ["ssh", "-o", "StrictHostKeyChecking=no", host, cmd],
        check=check, capture_output=capture, text=True, timeout=timeout,
    )

# ==============================================================================
# NIC detection
# ==============================================================================

def detect_nic(host, peer_ip):
    result = subprocess.run(
        ["ssh", host, f"ip -o route get {peer_ip}"],
        capture_output=True, text=True, check=True,
    )
    parts = result.stdout.strip().split()
    return parts[parts.index("dev") + 1]

def detect_all_nics(ar_hosts):
    nic_by_host = {}
    for i, host in enumerate(ar_hosts):
        peer = ar_hosts[(i + 1) % len(ar_hosts)]
        nic = detect_nic(host, peer)
        nic_by_host[host] = nic
        log.info("  %s: NIC=%s", host, nic)
    return nic_by_host

# ==============================================================================
# Virtual IP management
# ==============================================================================

def get_virtual_ip(replica_index, ar_hosts):
    """Return (virtual_ip, physical_host) for a replica index.

    Round-robin across AR hosts. First len(ar_hosts) replicas use primary IPs;
    subsequent replicas use alias IPs in the 10.10.1.{10*slot + host_idx + 1} scheme.
    """
    host_idx = replica_index % len(ar_hosts)
    slot = replica_index // len(ar_hosts)
    physical_host = ar_hosts[host_idx]
    if slot == 0:
        return ar_hosts[host_idx], physical_host
    alias_ip = f"10.10.1.{10 * slot + host_idx + 1}"
    return alias_ip, physical_host

def build_virtual_ip_map(ar_hosts, max_replicas):
    """Build virtual_ip -> physical_host mapping for all replicas."""
    vmap = {}
    for i in range(max_replicas):
        vip, phys = get_virtual_ip(i, ar_hosts)
        vmap[vip] = phys
    return vmap

def ensure_ssh_config_for_aliases(ar_hosts, virtual_ip_map):
    """Ensure SSH can reach alias IPs using the same key as the primary AR hosts.

    Parses ~/.ssh/config to find the IdentityFile for the first AR host, then
    adds a wildcard entry (Host 10.10.1.*) if not already present.
    """
    ssh_config_path = Path.home() / ".ssh" / "config"
    if not ssh_config_path.exists():
        log.warning("  No ~/.ssh/config found; SSH to alias IPs may fail")
        return

    config_text = ssh_config_path.read_text()

    # Check if wildcard already covers alias IPs
    if "Host 10.10.1.*" in config_text:
        log.info("  SSH wildcard for 10.10.1.* already configured")
        return

    # Find IdentityFile for the first AR host
    identity_file = None
    user = None
    lines = config_text.splitlines()
    in_host_block = False
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("Host ") and ar_hosts[0] in stripped:
            in_host_block = True
            continue
        if stripped.startswith("Host ") and in_host_block:
            break
        if in_host_block:
            if stripped.startswith("IdentityFile"):
                identity_file = stripped.split(None, 1)[1]
            if stripped.startswith("User"):
                user = stripped.split(None, 1)[1]

    if not identity_file:
        log.warning("  Could not find IdentityFile for %s in SSH config", ar_hosts[0])
        return

    # Add wildcard entry
    entry = (
        f"\nHost 10.10.1.*\n"
        f"  User {user or 'root'}\n"
        f"  IdentityFile {identity_file}\n"
        f"  StrictHostKeyChecking no\n"
    )
    ssh_config_path.write_text(config_text + entry)
    log.info("  Added SSH wildcard: Host 10.10.1.* with IdentityFile=%s", identity_file)


def setup_ip_aliases(ar_hosts, virtual_ip_map, nic_by_host):
    log.info("Setting up IP aliases ...")
    ensure_ssh_config_for_aliases(ar_hosts, virtual_ip_map)
    for vip, phys in virtual_ip_map.items():
        if vip in ar_hosts:
            continue
        nic = nic_by_host[phys]
        _ssh(phys, f"sudo ip addr add {vip}/24 dev {nic} 2>/dev/null || true", check=False)
        log.info("  %s -> %s (on %s:%s)", vip, phys, phys, nic)

    # Verify SSH to one alias IP
    alias_ips = [vip for vip in virtual_ip_map if vip not in ar_hosts]
    if alias_ips:
        test_ip = alias_ips[0]
        try:
            result = _ssh(test_ip, "hostname", capture=True, timeout=10)
            log.info("  SSH to alias %s OK: %s", test_ip, result.stdout.strip())
        except Exception as e:
            raise RuntimeError(
                f"SSH to alias IP {test_ip} failed: {e}. "
                f"Ensure ~/.ssh/config has a wildcard entry for 10.10.1.* "
                f"with the correct IdentityFile.") from e

def teardown_ip_aliases(ar_hosts, virtual_ip_map, nic_by_host):
    for vip, phys in virtual_ip_map.items():
        if vip in ar_hosts:
            continue
        nic = nic_by_host.get(phys, "")
        if nic:
            _ssh(phys, f"sudo ip addr del {vip}/24 dev {nic} 2>/dev/null || true", check=False)

# ==============================================================================
# tc/netem WAN emulation
# ==============================================================================

def _one_way_delay_ms(coord1, coord2):
    rtt = get_estimated_rtt_latency(coord1, coord2, SLOWDOWN)
    return rtt / 2.0

def apply_inter_server_latency(ar_hosts, nic_by_host, replica_ips,
                                replica_geo, virtual_ip_map):
    """Apply tc/netem delay with per-pair (src, dst) filters.

    Each (source_vip, destination_vip) pair gets its own tc class with the exact
    geographic one-way delay. Filters match BOTH source and destination IPs.

    For cross-machine pairs: filters on the physical NIC.
    For co-located pairs: filters on loopback.

    Returns:
        dict of (src_ip, dst_ip) -> one_way_delay_ms for verification.
    """
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

        # Collect all per-pair rules for this machine's NIC
        nic_rules = []  # (src_vip, dst_vip, one_way_ms)
        lo_rules = []   # (src_vip, dst_vip, one_way_ms)

        for src_vip in local_vips:
            for dst_vip in replica_ips:
                if src_vip == dst_vip:
                    continue
                d = _one_way_delay_ms(replica_geo[src_vip], replica_geo[dst_vip])
                expected[(src_vip, dst_vip)] = d
                if d < 0.1:
                    continue
                dst_phys = virtual_ip_map[dst_vip]
                if dst_phys != phys:
                    nic_rules.append((src_vip, dst_vip, d))
                else:
                    lo_rules.append((src_vip, dst_vip, d))

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
                log.info("  tc %s: %s -> %s = %.1fms", nic, src_vip, dst_vip, delay)

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
                log.info("  tc lo: %s -> %s = %.1fms", src_vip, dst_vip, delay)

    return expected

def reset_tc(ar_hosts, nic_by_host):
    for host in ar_hosts:
        nic = nic_by_host.get(host, "")
        if nic:
            _ssh(host, f"sudo tc qdisc del dev {nic} root 2>/dev/null || true", check=False)
        _ssh(host, "sudo tc qdisc del dev lo root 2>/dev/null || true", check=False)

# ==============================================================================
# WAN verification
# ==============================================================================

def verify_wan_emulation(expected_delays, virtual_ip_map, ar_hosts, tolerance_ms=1.0):
    """Ping between replica pairs and verify RTT matches expected delay.

    With per-pair tc filters, every (src, dst) pair has its own exact delay.
    Samples cross-machine pairs for verification.
    """
    log.info("  Verifying WAN emulation (tolerance=%.1fms) ...", tolerance_ms)
    all_ok = True
    # Sample cross-machine pairs (up to 10)
    sampled = [(k, v) for k, v in expected_delays.items()
               if v >= 1.0 and virtual_ip_map.get(k[0]) != virtual_ip_map.get(k[1])][:10]
    for (src_ip, dst_ip), one_way_ms in sampled:
        phys = virtual_ip_map.get(src_ip, src_ip)
        try:
            result = _ssh(phys, f"ping -c 3 -W 2 -I {src_ip} {dst_ip} -q",
                         capture=True, check=False, timeout=15)
            m = re.search(r"rtt.*= [\d.]+/([\d.]+)/", result.stdout)
            if not m:
                log.warning("    FAIL: %s -> %s: no ping response", src_host, dst_host)
                all_ok = False
                continue
            actual_rtt = float(m.group(1))
            expected_rtt = 2 * one_way_ms
            dev = abs(actual_rtt - expected_rtt)
            ok = dev <= tolerance_ms
            log.info("    %s: %s -> %s  expect=%.1fms  actual=%.1fms  dev=%.1fms",
                     "OK" if ok else "FAIL", src_ip, dst_ip, expected_rtt, actual_rtt, dev)
            if not ok:
                all_ok = False
        except Exception as e:
            log.warning("    FAIL: %s -> %s: %s", src_ip, dst_ip, e)
            all_ok = False
    return all_ok

# ==============================================================================
# Cleanup
# ==============================================================================

def cleanup_all(ar_hosts, nic_by_host, virtual_ip_map, control_plane_host):
    log.info("Running full cleanup ...")
    reset_tc(ar_hosts, nic_by_host)
    teardown_ip_aliases(ar_hosts, virtual_ip_map, nic_by_host)
    for host in ar_hosts:
        _ssh(host, "pkill -f ReconfigurableNode 2>/dev/null || true", check=False)
        _ssh(host, "sudo fuser -k 2000/tcp 2>/dev/null || true", check=False)
        _ssh(host, "sudo fuser -k 2300/tcp 2>/dev/null || true", check=False)
        _ssh(host, "sudo df | grep xdn/state/fuselog | awk '{print $6}' | xargs -r sudo umount -l 2>/dev/null || true", check=False)
        _ssh(host, "sudo rm -rf /tmp/gigapaxos /tmp/xdn /dev/shm/xdn", check=False)
        _ssh(host, 'containers=$(docker ps -a -q); '
             'if [ -n "$containers" ]; then docker stop $containers && docker rm $containers; fi',
             check=False)
        _ssh(host, "docker network prune --force > /dev/null 2>&1", check=False)
    _ssh(control_plane_host, "sudo fuser -k 3000/tcp 2>/dev/null || true", check=False)
    _ssh(control_plane_host, "sudo rm -rf /tmp/gigapaxos", check=False)
    os.system("fuser -s -k 8080/tcp 2>/dev/null || true")

def reset_cluster(ar_hosts, control_plane_host):
    log.info("  Resetting XDN cluster ...")
    for host in ar_hosts:
        # Kill all Java/ReconfigurableNode processes on this host
        _ssh(host, "pkill -f ReconfigurableNode 2>/dev/null || true", check=False)
        _ssh(host, "sudo fuser -k 2000/tcp 2>/dev/null || true", check=False)
        _ssh(host, "sudo fuser -k 2300/tcp 2>/dev/null || true", check=False)
        _ssh(host, "sudo df | grep xdn/state/fuselog | awk '{print $6}' | xargs -r sudo umount -l 2>/dev/null || true", check=False)
        _ssh(host, "sudo rm -rf /tmp/gigapaxos /tmp/xdn /dev/shm/xdn", check=False)
        _ssh(host, 'containers=$(docker ps -a -q); '
             'if [ -n "$containers" ]; then docker stop $containers && docker rm $containers; fi',
             check=False)
        _ssh(host, "docker network prune --force > /dev/null 2>&1", check=False)
    _ssh(control_plane_host, "sudo fuser -k 3000/tcp 2>/dev/null || true", check=False)
    _ssh(control_plane_host, "sudo rm -rf /tmp/gigapaxos", check=False)

# ==============================================================================
# XDN cluster management
# ==============================================================================

def start_cluster(screen_name, config_file):
    os.makedirs("screen_logs", exist_ok=True)
    log_file = f"screen_logs/{screen_name}.log"
    os.system(f"rm -f {log_file}")
    os.system(f"screen -S {screen_name} -X quit > /dev/null 2>&1")
    cmd = (f"screen -L -Logfile {log_file} -S {screen_name} -d -m bash -c "
           f"'../bin/gpServer.sh -DgigapaxosConfig={config_file} start all; exec bash'")
    log.info("  %s", cmd)
    ret = os.system(cmd)
    assert ret == 0, "Failed to start cluster"
    time.sleep(20)

def wait_for_cluster(replica_ips, timeout_sec=120):
    """Wait for all replica HTTP endpoints to be reachable."""
    log.info("  Waiting for %d replicas to be ready ...", len(replica_ips))
    deadline = time.time() + timeout_sec
    for vip in replica_ips:
        host, port = vip, HTTP_PORT
        while time.time() < deadline:
            try:
                s = socket.create_connection((host, port), timeout=2)
                s.close()
                break
            except OSError:
                time.sleep(2)
        else:
            log.warning("  TIMEOUT: %s:%d not ready", host, port)
            return False
    log.info("  All replicas ready.")
    return True

# ==============================================================================
# Config generation
# ==============================================================================

def prepare_config(config_template, config_target, replica_group_info,
                   control_plane_host, ar_hosts):
    """Generate GigaPaxos config with IP-alias virtual replicas.

    Returns:
        server_by_name: dict of server_name -> virtual_ip
        replica_geo: dict of virtual_ip -> (lat, lon)
    """
    shutil.copy2(config_template, config_target)
    replicas = replica_group_info["Replicas"]
    server_by_name = {}
    replica_geo = {}
    actives_txt = ""
    geo_txt = ""
    for i, server in enumerate(replicas):
        name = server["Name"]
        vip, _ = get_virtual_ip(i, ar_hosts)
        server_by_name[name] = vip
        replica_geo[vip] = (server["Latitude"], server["Longitude"])
        actives_txt += f"active.{name}={vip}:2000\n"
        geo_txt += f'active.{name}.geolocation="{server["Latitude"]},{server["Longitude"]}"\n'

    replace_placeholder(config_target, "___________PLACEHOLDER_ACTIVES_ADDRESS___________", actives_txt)
    replace_placeholder(config_target, "___________PLACEHOLDER_ACTIVES_GEOLOCATION___________", geo_txt)
    replace_placeholder(config_target, "___________PLACEHOLDER_RC_HOST___________", control_plane_host)
    replace_placeholder(config_target, "REPLICATE_ALL=false", "REPLICATE_ALL=true")
    replace_placeholder(config_target, "#___________PLACEHOLDER_CONFIG_FLAGS___________",
                        "EXPERIMENTAL_MAX_PHASE2_QUORUM_SIZE=2")
    return server_by_name, replica_geo

def prepare_service_yaml(consistency, service_name):
    target = f"static/xdn_geolat_{CONSISTENCY_SHORT[consistency]}.yaml"
    shutil.copy2(SERVICE_TEMPLATE, target)
    replace_placeholder(target, "___SERVICE_NAME___", service_name)
    replace_placeholder(target, "___CONSITENCY_MODEL___", consistency)
    return target

# ==============================================================================
# Replica placement
# ==============================================================================

def get_placement(protocol_class, clients, servers, read_ratio, max_replicas):
    server_by_name = {s["Name"]: s for s in servers}

    if protocol_class == "linearizability":
        return get_heuristic_replicas_placement(servers, clients, 3, True)

    if protocol_class == "sequential":
        num = min(math.ceil(3 + 7 * read_ratio / 100.0), max_replicas)
        rw_info = get_heuristic_replicas_placement(servers, clients, 3, True)
        rw_names = {s["Name"] for s in rw_info["Replicas"]}
        if num <= 3:
            return rw_info
        ro_count = num - 3
        locs = []
        for c in clients:
            for _ in range(c["Count"]):
                locs.append((c["Latitude"], c["Longitude"]))
        _, centroids = cluster_locations(locs, ro_count)
        info = rw_info
        for center in centroids:
            ref = {"Name": "ref", "Latitude": center[0], "Longitude": center[1]}
            closest = find_k_closest_servers(servers, ref, 1)
            if closest[0]["Name"] not in rw_names:
                info["Replicas"].append(closest[0])
        return info

    if protocol_class == "eventual":
        num = min(10, max_replicas)
        locs = []
        for c in clients:
            for _ in range(c["Count"]):
                locs.append((c["Latitude"], c["Longitude"]))
        _, centroids = cluster_locations(locs, num)
        replica_names = set()
        info = {"Replicas": [], "Leader": None, "Centroid": None}
        for center in centroids:
            ref = {"Name": "ref", "Latitude": center[0], "Longitude": center[1]}
            closest = find_k_closest_servers(servers, ref, 1)
            if closest[0]["Name"] not in replica_names:
                replica_names.add(closest[0]["Name"])
                info["Replicas"].append(closest[0])
        return info

    raise ValueError(f"Unknown protocol class: {protocol_class}")

# ==============================================================================
# Client assignment
# ==============================================================================

def assign_clients(clients, replica_group_info, server_address_by_name):
    clients = copy.deepcopy(clients)
    for client in clients:
        closest = find_k_closest_servers(
            replica_group_info["Replicas"],
            {"Latitude": client["Latitude"], "Longitude": client["Longitude"]}, 1)
        name = closest[0]["Name"]
        addr = server_address_by_name[name]
        client["TargetReplicaHostPort"] = f"{addr}:{HTTP_PORT}"
        client["TargetReplicaName"] = name
        client["ServiceTargetUrl"] = f"http://{addr}:{HTTP_PORT}{SERVICE_ENDPOINT}"
        client["ServiceReadUrl"] = f"http://{addr}:{HTTP_PORT}{SERVICE_READ_ENDPOINT}"
    return clients

# ==============================================================================
# Service deployment
# ==============================================================================

def deploy_service(service_name, service_yaml, control_plane_host, max_attempts=5):
    cmd = f"XDN_CONTROL_PLANE={control_plane_host} {XDN_BINARY} launch {service_name} --file={service_yaml}"
    for attempt in range(max_attempts):
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, shell=True)
            if result.returncode == 0 and "Error" not in result.stdout:
                log.info("  Service '%s' deployed.", service_name)
                return
        except Exception:
            pass
        log.warning("  Deploy attempt %d failed, retrying ...", attempt + 1)
        time.sleep(min(10 ** attempt, 30))
    raise RuntimeError(f"Failed to deploy {service_name} after {max_attempts} attempts")

def destroy_service(service_name, control_plane_host):
    cmd = f"yes yes | XDN_CONTROL_PLANE={control_plane_host} {XDN_BINARY} service destroy {service_name}"
    try:
        subprocess.run(cmd, capture_output=True, text=True, shell=True)
    except Exception:
        pass

def reconfigure_leader(service_name, replica_group_info, server_address_by_name,
                       control_plane_host):
    leader_name = replica_group_info["Leader"]["Name"]
    replica_names = [r["Name"] for r in replica_group_info["Replicas"]]
    payload = json.dumps({"NODES": replica_names, "COORDINATOR": leader_name})
    url = f"http://{control_plane_host}:{RC_HTTP_PORT}/api/v2/services/{service_name}/placement"
    cmd = f'curl -s -X POST {url} -d \'{payload}\''
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    log.info("  Leader reconfiguration: %s", result.stdout.strip()[:200])
    time.sleep(90)

# ==============================================================================
# Docker image caching
# ==============================================================================

def ensure_images(ar_hosts, control_plane_host):
    log.info("Ensuring Docker images on all hosts ...")
    all_hosts = list(set(ar_hosts + [control_plane_host]))
    src = ar_hosts[0]
    for host in all_hosts:
        check = subprocess.run(
            ["ssh", host, f"docker image inspect {TODO_IMAGE}"],
            capture_output=True, check=False)
        if check.returncode == 0:
            continue
        log.info("  Transferring %s to %s ...", TODO_IMAGE, host)
        os.system(f"ssh {src} 'docker save {TODO_IMAGE}' | ssh {host} 'docker load'")

# ==============================================================================
# Latency proxy
# ==============================================================================

def start_proxy(config_file, screen_name):
    os.system("fuser -s -k 8080/tcp 2>/dev/null || true")
    log_file = f"screen_logs/{screen_name}.log"
    cmd = (f"screen -L -Logfile {log_file} -S {screen_name} -d -m "
           f"./{LATENCY_PROXY_BIN} -config={config_file}")
    log.info("  %s", cmd)
    os.system(cmd)
    time.sleep(5)
    return {"http": "http://127.0.0.1:8080", "https": "http://127.0.0.1:8080"}

def stop_proxy():
    os.system("fuser -s -k 8080/tcp 2>/dev/null || true")

# ==============================================================================
# Warmup and measurement
# ==============================================================================

def make_mix(n, read_ratio):
    count_read = int(n * read_ratio / 100.0)
    mix = [True] * count_read + [False] * (n - count_read)
    random.shuffle(mix)
    return mix

def run_warmup(clients, service_name, num_warmup, read_ratio, post_data, proxies):
    session = http_requests.Session()
    headers = {"Content-Type": "application/json", "XDN": service_name}

    # Phase 1: seed all tasks DIRECTLY to every unique replica (no proxy),
    # so reads for specific tasks (e.g., /api/todo/tasks/task-8) don't 404.
    # We seed all replicas because eventual consistency may not propagate
    # writes between replicas quickly (especially during experiments).
    unique_urls = list({c["ServiceTargetUrl"] for c in clients})
    log.info("  Seeding %d tasks on %d replicas (no proxy) ...", len(post_data), len(unique_urls))
    for seed_url in unique_urls:
        for payload in post_data:
            try:
                r = session.post(seed_url, headers=headers, data=payload, timeout=30)
                if r.status_code != 200:
                    session.close()
                    raise RuntimeError(f"Seed failed: POST {seed_url} returned {r.status_code}: {r.text[:200]}")
            except http_requests.exceptions.RequestException as e:
                session.close()
                raise RuntimeError(f"Seed failed: {e}") from e

    # Phase 2: mixed read/write warmup
    mix = make_mix(num_warmup, read_ratio)
    for client in clients:
        write_url = client["ServiceTargetUrl"]
        read_url = client["ServiceReadUrl"]
        h = {**headers, "X-Client-Location": f"{client['Latitude']};{client['Longitude']}"}
        cookie = None
        for i, is_read in enumerate(mix):
            try:
                if is_read:
                    r = session.get(read_url, headers=h, timeout=30, cookies=cookie, proxies=proxies)
                else:
                    r = session.post(write_url, headers=h, data=post_data[i % len(post_data)],
                                     timeout=30, cookies=cookie, proxies=proxies)
                if r.status_code != 200:
                    session.close()
                    raise RuntimeError(
                        f"Warmup failed: {('GET ' + read_url) if is_read else ('POST ' + write_url)} "
                        f"returned {r.status_code}: {r.text[:200]}")
                cookie = r.cookies
            except http_requests.exceptions.RequestException as e:
                session.close()
                raise RuntimeError(f"Warmup failed: {e}") from e
    session.close()

def run_measurement(clients, service_name, consistency, read_ratio,
                    num_requests, post_data, proxies):
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
                    "consistency": consistency,
                    "read_ratio": read_ratio,
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
# CSV output
# ==============================================================================

RAW_FIELDS = ["consistency", "read_ratio", "client_city", "client_lat",
              "client_lon", "target_replica", "is_read", "latency_ms"]

SUMMARY_FIELDS = ["consistency", "read_ratio", "num_replicas", "num_clients",
                  "num_requests_per_client", "avg_latency_ms", "median_latency_ms",
                  "stddev_latency_ms", "p90_latency_ms", "p95_latency_ms", "p99_latency_ms"]

def pct(sorted_vals, p):
    if not sorted_vals:
        return 0
    idx = min(int(len(sorted_vals) * p / 100.0), len(sorted_vals) - 1)
    return sorted_vals[idx]

def compute_summary(consistency, read_ratio, num_replicas, num_clients,
                    num_per_client, latencies):
    s = sorted(latencies)
    return {
        "consistency": consistency,
        "read_ratio": read_ratio,
        "num_replicas": num_replicas,
        "num_clients": num_clients,
        "num_requests_per_client": num_per_client,
        "avg_latency_ms": f"{statistics.mean(s):.2f}",
        "median_latency_ms": f"{pct(s, 50):.2f}",
        "stddev_latency_ms": f"{statistics.stdev(s):.2f}" if len(s) > 1 else "0.00",
        "p90_latency_ms": f"{pct(s, 90):.2f}",
        "p95_latency_ms": f"{pct(s, 95):.2f}",
        "p99_latency_ms": f"{pct(s, 99):.2f}",
    }

def compute_cdf_csv(raw_rows, target_rr=80):
    rows = []
    for cons in CDF_SERIES:
        lats = sorted(float(r["latency_ms"]) for r in raw_rows
                      if r["consistency"] == cons and int(r["read_ratio"]) == target_rr)
        n = len(lats)
        if n == 0:
            continue
        for i, lat in enumerate(lats):
            rows.append({"consistency": cons, "latency_ms": f"{lat:.2f}",
                         "cdf": f"{(i + 1) / n:.6f}"})
    return rows

def compute_top3_csv(raw_rows, target_rr=80):
    rows = []
    for city in TOP_3_CITIES:
        for cons in CDF_SERIES:
            lats = [float(r["latency_ms"]) for r in raw_rows
                    if r["consistency"] == cons
                    and int(r["read_ratio"]) == target_rr
                    and r["client_city"] == city]
            if lats:
                rows.append({"city": city, "consistency": cons,
                             "avg_latency_ms": f"{statistics.mean(lats):.2f}"})
    return rows

def write_csv(path, rows, fields):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for row in rows:
            w.writerow(row)
    log.info("  Written %d rows to %s", len(rows), path)

# ==============================================================================
# Main
# ==============================================================================

def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--ar-hosts", default="10.10.1.1,10.10.1.2,10.10.1.3")
    p.add_argument("--control-plane-host", default="10.10.1.4")
    p.add_argument("--max-replicas", type=int, default=10)
    p.add_argument("--consistency", default=None,
                   help="Comma-separated models (default: all 9)")
    p.add_argument("--num-warmup", type=int, default=500)
    p.add_argument("--num-requests", type=int, default=1000)
    p.add_argument("--read-ratios", default=None,
                   help="Comma-separated read ratios (default: 0,20,40,60,80,100)")
    p.add_argument("--strict-wan-verify", action="store_true")
    return p.parse_args()

def main():
    args = parse_args()
    ar_hosts = [h.strip() for h in args.ar_hosts.split(",")]
    cp_host = args.control_plane_host
    max_rep = args.max_replicas

    assert len(ar_hosts) >= 3, "Need >= 3 AR hosts"
    assert max_rep >= 3, "Need >= 3 max replicas"

    if args.consistency:
        models = [m.strip() for m in args.consistency.split(",")]
        for m in models:
            assert m in ALL_CONSISTENCY_MODELS, f"Unknown model: {m}"
    else:
        models = ALL_CONSISTENCY_MODELS

    read_ratios = ([int(r) for r in args.read_ratios.split(",")]
                   if args.read_ratios else DEFAULT_READ_RATIOS)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_dir = Path("results") / f"geolat_consistency_{timestamp}"
    results_dir.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(results_dir / "run.log"),
        ],
    )

    log.info("=" * 60)
    log.info("Geo-distributed Latency vs. Consistency Model")
    log.info("  AR hosts     : %s", ar_hosts)
    log.info("  Control plane: %s", cp_host)
    log.info("  Max replicas : %d", max_rep)
    log.info("  Models       : %s", models)
    log.info("  Read ratios  : %s", read_ratios)
    log.info("  Warmup       : %d req/loc", args.num_warmup)
    log.info("  Measurement  : %d req/loc", args.num_requests)
    log.info("  Results      : %s", results_dir)
    log.info("=" * 60)

    # Load data
    servers = get_server_locations([SERVER_DATA], remove_duplicate_location=True,
                                  remove_nonredundant_city_servers=False)
    for s in servers:
        s["Name"] = s["Name"].replace("-", "_")
    city_locs = get_client_locations(POPULATION_DATA)
    pop_ratio = get_population_ratio_per_city(city_locs)
    city_counts = get_client_count_per_city(pop_ratio, NUM_CLIENTS, 0.0, city_locs[0]["City"])
    clients = get_per_city_clients(city_counts, city_locs)

    np.random.seed(RANDOM_SEED)
    random.seed(RANDOM_SEED)

    # Detect NICs
    log.info("Detecting NICs ...")
    nic_by_host = detect_all_nics(ar_hosts)

    # Build virtual IP map
    # Build virtual IP map
    virtual_ip_map = build_virtual_ip_map(ar_hosts, max_rep)
    log.info("Virtual replicas:")
    for i in range(max_rep):
        vip, phys = get_virtual_ip(i, ar_hosts)
        log.info("  Replica %d: %s (on %s)", i, vip, phys)

    # Verify binaries
    assert os.system(f"{XDN_BINARY} --help > /dev/null 2>&1") == 0, f"Cannot find {XDN_BINARY}"
    assert os.path.isfile(LATENCY_PROXY_BIN), f"Cannot find {LATENCY_PROXY_BIN}"

    # Pre-clean
    log.info("Pre-cleaning stale state ...")
    cleanup_all(ar_hosts, nic_by_host, virtual_ip_map, cp_host)

    # Register atexit
    atexit.register(lambda: cleanup_all(ar_hosts, nic_by_host, virtual_ip_map, cp_host))

    all_raw = []
    summary_rows = []
    summary_path = results_dir / "eval_geolat_consistency_summary.csv"

    post_data = [f'{{"item":"task-{i}"}}' for i in range(NUM_TASKS)]

    try:
        setup_ip_aliases(ar_hosts, virtual_ip_map, nic_by_host)
        ensure_images(ar_hosts, cp_host)

        # Write summary header
        with open(summary_path, "w", newline="") as f:
            csv.DictWriter(f, fieldnames=SUMMARY_FIELDS).writeheader()

        for consistency in models:
            for read_ratio in read_ratios:
                service_name = f"todo_{CONSISTENCY_SHORT[consistency]}_{read_ratio}"
                config_file = str(results_dir / f"config_{consistency}_{read_ratio}.properties")

                log.info("")
                log.info("=" * 60)
                log.info("  %s | read_ratio=%d%%", consistency, read_ratio)
                log.info("=" * 60)

                try:
                    # Phase 1
                    log.info("[Phase 1] Resetting cluster ...")
                    reset_cluster(ar_hosts, cp_host)

                    # Phase 2
                    log.info("[Phase 2] Computing placement ...")
                    proto = consistency if consistency in ("linearizability", "sequential") else "eventual"
                    placement = get_placement(proto, clients, servers, read_ratio, max_rep)
                    n_rep = len(placement["Replicas"])
                    log.info("  %d replicas, leader=%s",
                             n_rep, placement["Leader"]["Name"] if placement["Leader"] else "none")

                    # Phase 3
                    log.info("[Phase 3] Generating config ...")
                    addr_map, geo_map = prepare_config(
                        GP_CONFIG_TEMPLATE, config_file, placement, cp_host, ar_hosts)
                    rep_ips = list(addr_map.values())

                    # Phase 4: Start cluster (no WAN emulation yet)
                    log.info("[Phase 4] Starting cluster ...")
                    scr = f"xdn_geolat_{CONSISTENCY_SHORT[consistency]}_{read_ratio}"
                    start_cluster(scr, config_file)
                    if not wait_for_cluster(rep_ips):
                        log.error("Cluster not ready, skipping.")
                        continue

                    # Phase 5: Deploy service BEFORE WAN emulation so Paxos
                    # reconfiguration uses fast LAN, not emulated WAN.
                    log.info("[Phase 5] Deploying service '%s' ...", service_name)
                    svc_yaml = prepare_service_yaml(consistency, service_name)
                    deploy_service(service_name, svc_yaml, cp_host)

                    if consistency in LEADER_MODELS and placement["Leader"]:
                        log.info("  Reconfiguring leader ...")
                        reconfigure_leader(service_name, placement, addr_map, cp_host)

                    # Wait for service to be ready on ALL replicas
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
                            log.warning("    TIMEOUT: %s (%s:%d) not ready", name, vip, HTTP_PORT)

                    # Phase 5b: Verify consistency protocol on all replicas
                    log.info("  Verifying consistency protocol on all replicas ...")
                    for name, vip in addr_map.items():
                        try:
                            info_url = f"http://{vip}:{HTTP_PORT}/api/v2/services/{service_name}/replica/info"
                            r = http_requests.get(info_url, headers={"XDN": service_name}, timeout=5)
                            if r.status_code == 200:
                                info = r.json()
                                offered = info.get("consistency", "?")
                                requested = info.get("requestedConsistency", "?")
                                protocol = info.get("protocol", "?")
                                role = info.get("role", "?")
                                behaviors = info.get("requestBehaviors", [])
                                log.info("    %s (%s): offered=%s requested=%s protocol=%s role=%s",
                                         name, vip, offered, requested, protocol, role)
                                if behaviors:
                                    for b in behaviors:
                                        log.info("      behavior: %s %s -> %s",
                                                 b.get("prefix", "?"),
                                                 b.get("methods", "?"),
                                                 b.get("behavior", "?"))
                                if offered.lower() != consistency.lower():
                                    log.warning("    MISMATCH: expected=%s offered=%s on %s",
                                                consistency, offered, name)
                            else:
                                log.warning("    %s (%s): replica/info returned %d", name, vip, r.status_code)
                        except Exception as e:
                            log.warning("    %s (%s): failed to query replica/info: %s", name, vip, e)

                    # Phase 6: Apply WAN emulation AFTER service is deployed
                    log.info("[Phase 6] Emulating WAN ...")
                    expected_delays = apply_inter_server_latency(
                        ar_hosts, nic_by_host, list(addr_map.values()), geo_map, virtual_ip_map)

                    # Phase 7: Verify WAN
                    log.info("[Phase 7] Verifying WAN ...")
                    wan_ok = verify_wan_emulation(expected_delays, virtual_ip_map, ar_hosts)
                    if not wan_ok:
                        raise RuntimeError("WAN verification failed: deviation > 1ms")

                    # Phase 8: Start latency proxy
                    log.info("[Phase 8] Starting latency proxy ...")
                    prx_scr = f"scr_prx_{CONSISTENCY_SHORT[consistency]}_{read_ratio}"
                    proxies = start_proxy(config_file, prx_scr)

                    curr_clients = assign_clients(clients, placement, addr_map)

                    # Phase 9
                    log.info("[Phase 9] Warmup (%d req/loc) ...", args.num_warmup)
                    run_warmup(curr_clients, service_name, args.num_warmup,
                               read_ratio, post_data, proxies)

                    # Phase 10
                    log.info("[Phase 10] Measurement (%d req/loc) ...", args.num_requests)
                    raw = run_measurement(curr_clients, service_name, consistency,
                                          read_ratio, args.num_requests, post_data, proxies)
                    all_raw.extend(raw)

                    # Phase 11
                    lats = [float(r["latency_ms"]) for r in raw]
                    summary = compute_summary(consistency, read_ratio, n_rep,
                                              NUM_CLIENTS, args.num_requests, lats)
                    summary_rows.append(summary)
                    log.info("  Result: avg=%.2fms p50=%.2fms p99=%.2fms (n=%d)",
                             float(summary["avg_latency_ms"]),
                             float(summary["median_latency_ms"]),
                             float(summary["p99_latency_ms"]), len(lats))

                    with open(summary_path, "a", newline="") as f:
                        csv.DictWriter(f, fieldnames=SUMMARY_FIELDS).writerow(summary)

                except Exception as e:
                    log.error("ERROR in %s/%d: %s", consistency, read_ratio, e)
                    traceback.print_exc()

                finally:
                    log.info("[Phase 12] Cleanup ...")
                    try:
                        destroy_service(service_name, cp_host)
                    except Exception:
                        pass
                    os.system(f"../bin/gpServer.sh -DgigapaxosConfig={config_file} forceclear all > /dev/null 2>&1")
                    reset_cluster(ar_hosts, cp_host)
                    reset_tc(ar_hosts, nic_by_host)
                    stop_proxy()
                    time.sleep(5)

        # Write output CSVs
        log.info("")
        log.info("Writing output CSVs ...")
        write_csv(results_dir / "eval_geolat_consistency_raw.csv", all_raw, RAW_FIELDS)
        cdf_rows = compute_cdf_csv(all_raw)
        if cdf_rows:
            write_csv(results_dir / "eval_consistency_latency_cdf.csv",
                      cdf_rows, ["consistency", "latency_ms", "cdf"])
        top3_rows = compute_top3_csv(all_raw)
        if top3_rows:
            write_csv(results_dir / "eval_consistency_top3_cities.csv",
                      top3_rows, ["city", "consistency", "avg_latency_ms"])

    finally:
        cleanup_all(ar_hosts, nic_by_host, virtual_ip_map, cp_host)

    log.info("")
    log.info("Done. Results in %s", results_dir)


if __name__ == "__main__":
    main()
