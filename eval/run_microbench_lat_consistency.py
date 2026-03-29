"""
run_microbench_lat_consistency.py — Per-request protocol overhead microbenchmark.

Measures raw per-request coordination cost of each consistency model on a LAN
cluster (no WAN emulation, no latency proxy). Tracks read and write latencies
separately because different consistency models handle reads and writes
differently (e.g., linearizability orders both via Paxos; eventual handles both
locally).

Uses the TodoApp service with sequential requests at low load to isolate
per-request coordination cost.

Prerequisites:
  - CloudLab 3-way cluster initialized via `xdnd dist-init`
  - Docker images pre-cached on all machines

Run from the eval/ directory:
    python run_microbench_lat_consistency.py
    python run_microbench_lat_consistency.py --consistency linearizability,eventual

Results go to eval/results/microbench_lat_consistency_<timestamp>/latency_consistency.csv
"""

import argparse
import json
import logging
import os
import shutil
import socket
import statistics
import subprocess
import sys
import time
import urllib.request
from urllib.error import HTTPError

import requests as http_requests
from datetime import datetime
from pathlib import Path

from utils import replace_placeholder

log = logging.getLogger("microbench-consistency")

# ==============================================================================
# Constants
# ==============================================================================

CONTROL_PLANE_HOST = "10.10.1.4"
AR_HOSTS = ["10.10.1.1", "10.10.1.2", "10.10.1.3"]
HTTP_PROXY_PORT = 2300
XDN_BINARY = "../bin/xdn"
GP_CONFIG_BASE = "../conf/gigapaxos.xdn.3way.cloudlab.properties"
SERVICE_PROP_TEMPLATE = "static/xdn_consistency_template.yaml"
SERVICE_ENDPOINT = "/api/todo/tasks"
SERVICE_READ_ENDPOINT = "/api/todo/tasks/warmup-8"
SCREEN_SESSION = "xdn_microbench_cons"
TODO_IMAGE = "fadhilkurnia/xdn-todo"

ALL_CONSISTENCY_MODELS = [
    "linearizability",
    "sequential",
    "causal",
    "pram",
    "monotonic_reads",
    "writes_follow_reads",
    "read_your_writes",
    "monotonic_writes",
    "eventual",
]

LEADER_CONSISTENCY_MODELS = {"linearizability", "sequential"}

# Short 3-letter service names to minimize coordination overhead (service name
# is included in every Paxos/replication message).
CONSISTENCY_SHORT_NAME = {
    "linearizability": "lin",
    "sequential":      "seq",
    "causal":          "cau",
    "pram":            "prm",
    "monotonic_reads": "mrd",
    "writes_follow_reads": "wfr",
    "read_your_writes": "ryw",
    "monotonic_writes": "mwr",
    "eventual":        "evt",
}

AR_BASE_PORT = 2000
HTTP_PORT_OFFSET = 300  # HTTP_PROXY_PORT = AR_BASE_PORT + HTTP_PORT_OFFSET

NUM_WARMUP_WRITES = 1000
NUM_WARMUP_READS = 1000
NUM_MEASURE_WRITES = 1000
NUM_MEASURE_READS = 1000

DEFAULT_INTER_REPLICA_RTT_MS = 2.0  # emulate inter-AZ latency (round-trip)

results_base = Path(__file__).resolve().parent / "results"


# ==============================================================================
# Inter-replica latency injection via tc/netem
# ==============================================================================


def _ssh(host, cmd, check=True, capture=False, timeout=60):
    return subprocess.run(
        ["ssh", "-o", "StrictHostKeyChecking=no", host, cmd],
        check=check, capture_output=capture, text=True, timeout=timeout,
    )


def _detect_nic(host, target_subnet="10.10.1.0"):
    """Detect the NIC used for inter-replica traffic (the one routing the AR subnet)."""
    # First try to find the NIC for the specific subnet
    r = _ssh(host, f"ip route show {target_subnet}/24 2>/dev/null | awk '{{print $3}}' | head -1",
             capture=True, check=False)
    nic = r.stdout.strip()
    if nic:
        return nic
    # Fallback: NIC with the host's own 10.10.1.x address
    r = _ssh(host, "ip -o addr show | grep '10\\.10\\.1\\.' | awk '{print $2}' | head -1",
             capture=True, check=False)
    nic = r.stdout.strip()
    if nic:
        return nic
    # Last resort: default route NIC
    r = _ssh(host, "ip route show default | awk '{print $5}' | head -1", capture=True)
    nic = r.stdout.strip()
    if not nic:
        raise RuntimeError(f"Could not detect NIC on {host}")
    return nic


def apply_inter_replica_latency(ar_hosts, rtt_ms):
    """Apply symmetric one-way delay between all AR pairs using tc/netem.

    rtt_ms is the desired round-trip time; each direction gets rtt_ms/2.
    Each (src, dst) pair on the physical NIC gets a dedicated tc class with
    netem delay. This emulates inter-availability-zone latency.
    """
    if rtt_ms <= 0:
        log.info("  Skipping latency injection (rtt_ms=0)")
        return

    delay_ms = rtt_ms / 2.0
    log.info("  Injecting %.1fms RTT inter-replica latency (%.1fms one-way) ...", rtt_ms, delay_ms)

    # Detect NICs
    nic_by_host = {}
    for host in ar_hosts:
        nic_by_host[host] = _detect_nic(host)
        log.info("    %s: NIC=%s", host, nic_by_host[host])

    # Build per-host rules: for each host, add delay to traffic destined for other ARs
    for host in ar_hosts:
        nic = nic_by_host[host]
        other_hosts = [h for h in ar_hosts if h != host]

        # Physical NIC: delay outgoing traffic to other ARs
        _ssh(host, f"sudo tc qdisc del dev {nic} root 2>/dev/null || true", check=False)
        _ssh(host, f"sudo tc qdisc add dev {nic} root handle 1: htb default 99")
        _ssh(host, f"sudo tc class add dev {nic} parent 1: classid 1:99 htb rate 100gbit")
        for idx, dst in enumerate(other_hosts, start=1):
            classid = f"1:{idx}"
            handle = f"{10 + idx}:"
            _ssh(host, f"sudo tc class add dev {nic} parent 1: classid {classid} htb rate 100gbit")
            _ssh(host, f"sudo tc qdisc add dev {nic} parent {classid} handle {handle} netem delay {delay_ms:.2f}ms")
            _ssh(host, (f"sudo tc filter add dev {nic} parent 1: protocol ip prio {idx} u32 "
                        f"match ip dst {dst}/32 flowid {classid}"))
            log.info("    tc %s on %s: -> %s = %.1fms", nic, host, dst, delay_ms)

        # Loopback: delay traffic between co-located replicas (same machine, different IPs)
        # Not needed here since all 3 ARs are on separate machines, but included for completeness
        # if any two AR_HOSTS share the same physical machine.

    log.info("  Latency injection complete.")


def clear_inter_replica_latency(ar_hosts):
    """Remove all tc/netem rules from AR hosts."""
    log.info("  Clearing inter-replica latency injection ...")
    for host in ar_hosts:
        try:
            nic = _detect_nic(host)
            _ssh(host, f"sudo tc qdisc del dev {nic} root 2>/dev/null || true", check=False)
        except Exception:
            pass
        _ssh(host, "sudo tc qdisc del dev lo root 2>/dev/null || true", check=False)
    log.info("  Latency injection cleared.")


def verify_inter_replica_latency(ar_hosts, rtt_ms, tolerance_ms=1.0):
    """Verify the injected latency by pinging between AR hosts."""
    if rtt_ms <= 0:
        return True
    log.info("  Verifying inter-replica latency (expected RTT=%.1fms) ...", rtt_ms)
    all_ok = True
    for src in ar_hosts:
        for dst in ar_hosts:
            if src == dst:
                continue
            r = _ssh(src, f"ping -c 3 -q {dst} | tail -1", capture=True, check=False)
            # Parse: rtt min/avg/max/mdev = 4.01/4.02/4.03/0.01 ms
            import re
            m = re.search(r"= [\d.]+/([\d.]+)/", r.stdout)
            if m:
                actual_rtt = float(m.group(1))
                expected_rtt = rtt_ms
                deviation = abs(actual_rtt - expected_rtt)
                status = "OK" if deviation <= tolerance_ms else "MISMATCH"
                log.info("    %s -> %s: RTT=%.2fms (expected=%.2fms, dev=%.2fms) %s",
                         src, dst, actual_rtt, expected_rtt, deviation, status)
                if deviation > tolerance_ms:
                    all_ok = False
            else:
                log.warning("    %s -> %s: could not parse ping output", src, dst)
                all_ok = False
    return all_ok


# ==============================================================================
# Cluster management (inlined from run_load_pb_bookcatalog_common.py)
# ==============================================================================


def ensure_docker_image_on_rc():
    """Ensure the todo Docker image is available on the RC node."""
    check = subprocess.run(
        ["ssh", CONTROL_PLANE_HOST, f"docker image inspect {TODO_IMAGE}"],
        capture_output=True,
    )
    if check.returncode == 0:
        log.info("  %s: already on RC", TODO_IMAGE)
        return
    src = AR_HOSTS[0]
    log.info("  %s: transferring %s -> %s ...", TODO_IMAGE, src, CONTROL_PLANE_HOST)
    ret = os.system(
        f"ssh {src} 'docker save {TODO_IMAGE}'"
        f" | ssh {CONTROL_PLANE_HOST} 'docker load'"
    )
    if ret != 0:
        log.error("Failed to transfer %s to RC", TODO_IMAGE)
        sys.exit(1)
    log.info("  %s: transferred to RC", TODO_IMAGE)


def clear_xdn_cluster():
    """Force-clear the XDN cluster: kill processes, remove state, stop containers."""
    log.info("Resetting the cluster:")
    for i in range(3):
        host = f"10.10.1.{i+1}"
        os.system(f"ssh {host} sudo pkill -f gigapaxos 2>/dev/null || true")
        os.system(f"ssh {host} sudo pkill -f ReconfigurableNode 2>/dev/null || true")
        os.system(f"ssh {host} sudo fuser -k 2000/tcp 2>/dev/null || true")
        os.system(f"ssh {host} sudo fuser -k 2300/tcp 2>/dev/null || true")
        os.system(
            f"ssh {host} \"sudo df | grep xdn/state/fuselog"
            f" | awk '{{print \\$6}}' | xargs -r sudo umount -l 2>/dev/null || true\""
        )
        os.system(f"ssh {host} sudo rm -rf /tmp/gigapaxos /tmp/xdn /dev/shm/xdn")
        os.system(
            f"ssh {host} 'containers=$(docker ps -a -q); "
            f"if [ -n \"$containers\" ]; then docker stop $containers && docker rm $containers; fi'"
        )
        os.system(f"ssh {host} docker network prune --force > /dev/null 2>&1")
    # RC node
    os.system(f"ssh {CONTROL_PLANE_HOST} sudo pkill -f gigapaxos 2>/dev/null || true")
    os.system(f"ssh {CONTROL_PLANE_HOST} sudo pkill -f ReconfigurableNode 2>/dev/null || true")
    os.system(f"ssh {CONTROL_PLANE_HOST} sudo fuser -k 3000/tcp 2>/dev/null || true")
    os.system(f"ssh {CONTROL_PLANE_HOST} sudo rm -rf /tmp/gigapaxos")
    log.info("  done.")


def wait_for_port(host, port, timeout_sec):
    """Wait for a TCP port to become reachable."""
    log.info("   Waiting for %s:%s ...", host, port)
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            s = socket.create_connection((host, port), timeout=2)
            s.close()
            log.info("   OK: %s:%s", host, port)
            return True
        except OSError:
            time.sleep(2)
    log.info("   TIMEOUT: %s:%s not open after %ss", host, port, timeout_sec)
    return False


def wait_for_service(host, port, service_name, timeout_sec):
    """Wait for the service to respond via XDN proxy."""
    log.info("   Waiting up to %ss for '%s' to respond ...", timeout_sec, service_name)
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            r = http_requests.get(
                f"http://{host}:{port}/",
                headers={"XDN": service_name},
                timeout=5,
            )
            if 200 <= r.status_code < 500:
                log.info("   READY: HTTP %s from %s", r.status_code, host)
                return True
        except Exception:
            pass
        time.sleep(5)
    log.info("   TIMEOUT: service not ready after %ss", timeout_sec)
    return False


# ==============================================================================
# Config generation
# ==============================================================================


def generate_config_no_batching(base_config, target_config):
    """Copy base config and disable batching for clean per-request measurements."""
    shutil.copy2(base_config, target_config)
    replace_placeholder(target_config, "BATCHING_ENABLED=true", "BATCHING_ENABLED=false")
    replace_placeholder(
        target_config,
        "HTTP_AR_FRONTEND_BATCH_ENABLED=true",
        "HTTP_AR_FRONTEND_BATCH_ENABLED=false",
    )


# ==============================================================================
# Leader detection
# ==============================================================================


def detect_leader_replica(service_name, retries=10, interval=3.0):
    """Query each AR's replica/info endpoint and return the leader host.

    Exits the script if multiple leaders are detected (split-brain).
    Raises RuntimeError if no leader is found after retries.
    """
    for attempt in range(1, retries + 1):
        leaders = []
        for host in AR_HOSTS:
            url = f"http://{host}:{HTTP_PROXY_PORT}/api/v2/services/{service_name}/replica/info"
            try:
                req = urllib.request.Request(url, headers={"XDN": service_name}, method="GET")
                with urllib.request.urlopen(req, timeout=5) as resp:
                    if resp.status >= 400:
                        continue
                    info = json.load(resp)
            except (HTTPError, Exception):
                continue

            role = str(info.get("role", "")).lower()
            if role in ("leader", "primary"):
                leaders.append(host)

        if len(leaders) == 1:
            log.info("   Detected leader at %s (attempt %d)", leaders[0], attempt)
            return leaders[0]

        if len(leaders) > 1:
            log.error(" multiple leaders detected: %s — aborting", leaders)
            sys.exit(1)

        log.info("   No leader found (attempt %d/%d), retrying ...", attempt, retries)
        if attempt < retries:
            time.sleep(interval)

    raise RuntimeError(
        f"Unable to detect leader for {service_name} after {retries} attempts"
    )


# ==============================================================================
# Consistency protocol verification
# ==============================================================================

# Expected coordinator class (SimpleName) for each consistency model.
# The service YAML is generated per-model: causal/pram get only read_only +
# write_only behaviors (no monotonic) so they use their native coordinators
# instead of falling back to AwReplicaCoordinator.
EXPECTED_PROTOCOL = {
    "linearizability": "PaxosReplicaCoordinator",
    "sequential":      "AwReplicaCoordinator",
    "causal":          "CausalReplicaCoordinator",
    "pram":            "PramReplicaCoordinator",
    "monotonic_reads":      "BayouReplicaCoordinator",
    "writes_follow_reads":  "BayouReplicaCoordinator",
    "read_your_writes":     "BayouReplicaCoordinator",
    "monotonic_writes":     "BayouReplicaCoordinator",
    "eventual":        "LazyReplicaCoordinator",
}


def verify_consistency_protocol(service_name, expected_consistency):
    """Query replica/info on all ARs and verify offered consistency and protocol.

    Aborts if any replica reports an unexpected consistency model or coordinator
    class (e.g., fallback to a stronger consistency).
    """
    expected_proto = EXPECTED_PROTOCOL[expected_consistency]
    log.info("   Verifying consistency protocol on all replicas ...")
    log.info("   Expected: consistency=%s protocol=%s", expected_consistency, expected_proto)
    for host in AR_HOSTS:
        url = f"http://{host}:{HTTP_PROXY_PORT}/api/v2/services/{service_name}/replica/info"
        try:
            req = urllib.request.Request(url, headers={"XDN": service_name}, method="GET")
            with urllib.request.urlopen(req, timeout=10) as resp:
                info = json.load(resp)
        except Exception as e:
            log.warning(" could not query %s: %s", host, e)
            continue

        offered = str(info.get("consistency", "")).lower()
        requested = str(info.get("requestedConsistency", "")).lower()
        protocol = str(info.get("protocol", ""))
        role = str(info.get("role", ""))

        log.info("   %s: offered=%s requested=%s protocol=%s role=%s",
              host, offered, requested, protocol, role)

        # Verify offered consistency
        if offered and offered != "?" and offered != expected_consistency:
            raise RuntimeError(
                f"Consistency mismatch on {host}: expected '{expected_consistency}' "
                f"but replica reports offered='{offered}'. "
                f"Protocol may have fallen back to a stronger model."
            )

        # Verify requested consistency
        if requested and requested != "?" and requested != expected_consistency:
            raise RuntimeError(
                f"Requested consistency mismatch on {host}: expected "
                f"'{expected_consistency}' but replica reports "
                f"requestedConsistency='{requested}'."
            )

        # Verify coordinator protocol class
        if protocol and protocol != "?" and protocol != expected_proto:
            raise RuntimeError(
                f"Protocol mismatch on {host}: expected '{expected_proto}' "
                f"but replica reports protocol='{protocol}'. "
                f"The coordinator may have fallen back unexpectedly."
            )

    log.info("   Verified: all replicas running '%s' with protocol '%s'",
          expected_consistency, expected_proto)


# ==============================================================================
# Latency statistics
# ==============================================================================


def compute_percentile(data, pct):
    """Compute the pct-th percentile of a sorted list."""
    if not data:
        return 0.0
    sorted_data = sorted(data)
    idx = int(len(sorted_data) * pct / 100.0)
    idx = min(idx, len(sorted_data) - 1)
    return sorted_data[idx]


def compute_stats(latencies):
    """Return dict with avg, stddev, p50, p90, p95, p99 in ms."""
    if not latencies:
        return {"avg": 0, "stddev": 0, "p50": 0, "p90": 0, "p95": 0, "p99": 0}
    return {
        "avg": statistics.mean(latencies),
        "stddev": statistics.stdev(latencies) if len(latencies) > 1 else 0,
        "p50": compute_percentile(latencies, 50),
        "p90": compute_percentile(latencies, 90),
        "p95": compute_percentile(latencies, 95),
        "p99": compute_percentile(latencies, 99),
    }


# ==============================================================================
# Main
# ==============================================================================


def run_measurement(consistency, result_file, inter_replica_rtt_ms=0.0):
    """Run a single measurement for one consistency model."""
    service_name = CONSISTENCY_SHORT_NAME[consistency]
    screen_session = f"{SCREEN_SESSION}_{consistency}"
    screen_log = f"screen_logs/{screen_session}.log"
    config_file = f"static/gigapaxos.microbench.{consistency}.properties"
    service_yaml = f"static/xdn_microbench_{consistency}.yaml"

    try:
        # (1) Clear cluster
        log.info("\n>> [%s] Clearing cluster ...", consistency)
        clear_xdn_cluster()

        # (2) Generate config with batching disabled
        log.info(">> [%s] Generating config ...", consistency)
        generate_config_no_batching(GP_CONFIG_BASE, config_file)

        # (3) Prepare service YAML
        log.info(">> [%s] Preparing service YAML ...", consistency)
        shutil.copy2(SERVICE_PROP_TEMPLATE, service_yaml)
        replace_placeholder(service_yaml, "___SERVICE_NAME___", service_name)
        replace_placeholder(service_yaml, "___CONSISTENCY_MODEL___", consistency)
        # Eventual consistency requires MONOTONIC behavior for writes.
        # LazyReplicaCoordinator checks that declared behaviors include
        # {MONOTONIC, READ_ONLY, WRITE_ONLY} before accepting the service.
        if consistency == "eventual":
            with open(service_yaml, "a") as f:
                f.write("\n  - path_prefix: \"/\"\n")
                f.write("    methods: \"PUT,POST,DELETE\"\n")
                f.write("    behavior: monotonic\n")

        # (4) Start cluster
        log.info(">> [%s] Starting cluster ...", consistency)
        os.makedirs("screen_logs", exist_ok=True)
        os.system(f"rm -f {screen_log}")
        os.system(f"screen -S {screen_session} -X quit > /dev/null 2>&1")
        cmd = (
            f"screen -L -Logfile {screen_log} -S {screen_session} -d -m bash -c "
            f"'../bin/gpServer.sh -DgigapaxosConfig={config_file} -DSYNC=true start all; exec bash'"
        )
        log.info("   %s", cmd)
        ret = os.system(cmd)
        assert ret == 0, "Failed to start cluster"

        # Wait for AR ports
        for host in AR_HOSTS:
            ok = wait_for_port(host, HTTP_PROXY_PORT, timeout_sec=60)
            if not ok:
                raise RuntimeError(f"AR {host}:{HTTP_PROXY_PORT} not reachable")

        # (5) Deploy service
        log.info(">> [%s] Deploying service ...", consistency)
        deploy_cmd = (
            f"XDN_CONTROL_PLANE={CONTROL_PLANE_HOST} {XDN_BINARY} launch "
            f"{service_name} --file={service_yaml}"
        )
        max_attempts = 5
        for attempt in range(max_attempts):
            try:
                log.info("   %s", deploy_cmd)
                result = subprocess.run(deploy_cmd, capture_output=True, text=True, shell=True)
                assert result.returncode == 0, f"returncode={result.returncode}"
                assert "Error" not in result.stdout, f"output: {result.stdout}"
                break
            except Exception as e:
                log.info("   Attempt %d failed: %s", attempt+1, e)
                time.sleep(10 ** attempt)
                if attempt == max_attempts - 1:
                    raise RuntimeError(f"Failed to deploy {service_name} after {max_attempts} attempts")

        # (6) Wait for service readiness (check on AR_HOSTS[0] first)
        log.info(">> [%s] Waiting for service readiness ...", consistency)
        ok = wait_for_service(AR_HOSTS[0], HTTP_PROXY_PORT, service_name, timeout_sec=120)
        if not ok:
            raise RuntimeError("Service not ready")

        # (6b) Verify the correct consistency protocol is active
        log.info(">> [%s] Verifying consistency protocol ...", consistency)
        verify_consistency_protocol(service_name, consistency)

        # (6c) For linearizability/sequential, detect the leader and send
        #      requests there so they go directly to the Paxos leader instead
        #      of being forwarded internally.
        if consistency in LEADER_CONSISTENCY_MODELS:
            log.info(">> [%s] Detecting leader replica ...", consistency)
            target_host = detect_leader_replica(service_name)
            log.info(">> [%s] Sending requests to leader %s", consistency, target_host)
        else:
            target_host = AR_HOSTS[0]

        target_url = f"http://{target_host}:{HTTP_PROXY_PORT}{SERVICE_ENDPOINT}"
        read_url = f"http://{target_host}:{HTTP_PROXY_PORT}{SERVICE_READ_ENDPOINT}"

        headers = {"Content-Type": "application/json", "XDN": service_name}

        # (6d) Inject inter-replica latency AFTER deployment (so Paxos
        #      reconfiguration uses fast LAN, but measurements use emulated WAN)
        if inter_replica_rtt_ms > 0:
            log.info(">> [%s] Injecting %.1fms RTT inter-replica latency ...",
                     consistency, inter_replica_rtt_ms)
            apply_inter_replica_latency(AR_HOSTS, inter_replica_rtt_ms)
            if not verify_inter_replica_latency(AR_HOSTS, inter_replica_rtt_ms):
                raise RuntimeError(
                    f"Inter-replica latency verification failed "
                    f"(expected {inter_replica_rtt_ms:.1f}ms RTT). "
                    f"Aborting measurement."
                )

        # (7) Warmup
        log.info(">> [%s] Warming up (%d writes + %d reads) ...", consistency, NUM_WARMUP_WRITES, NUM_WARMUP_READS)
        warmup_session = http_requests.Session()
        for i in range(NUM_WARMUP_WRITES):
            try:
                warmup_session.post(
                    target_url, headers=headers,
                    data=f'{{"item":"warmup-{i}"}}', timeout=10,
                )
            except Exception as e:
                log.info("   Warmup write %d error: %s", i, e)
        for i in range(NUM_WARMUP_READS):
            try:
                warmup_session.get(read_url, headers=headers, timeout=10)
            except Exception as e:
                log.info("   Warmup read %d error: %s", i, e)
        warmup_session.close()

        # (8) Measure writes
        log.info(">> [%s] Measuring %d sequential writes ...", consistency, NUM_MEASURE_WRITES)
        write_latencies = []
        write_session = http_requests.Session()
        for i in range(NUM_MEASURE_WRITES):
            start = time.perf_counter()
            r = write_session.post(
                target_url, headers=headers,
                data=f'{{"item":"measure-write-{i}"}}', timeout=10,
            )
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            if r.status_code != 200:
                write_session.close()
                raise RuntimeError(
                    f"Write {i} failed: POST {target_url} returned {r.status_code}: {r.text[:200]}")
            write_latencies.append(elapsed_ms)
        write_session.close()

        # (9) Measure reads
        log.info(">> [%s] Measuring %d sequential reads ...", consistency, NUM_MEASURE_READS)
        read_latencies = []
        read_session = http_requests.Session()
        for i in range(NUM_MEASURE_READS):
            start = time.perf_counter()
            r = read_session.get(read_url, headers=headers, timeout=10)
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            if r.status_code != 200:
                read_session.close()
                raise RuntimeError(
                    f"Read {i} failed: GET {read_url} returned {r.status_code}: {r.text[:200]}")
            read_latencies.append(elapsed_ms)
        read_session.close()

        # (10) Compute stats and write CSV row
        read_stats = compute_stats(read_latencies)
        write_stats = compute_stats(write_latencies)

        log.info(">> [%s] Results:", consistency)
        log.info("   Reads  — avg=%.2fms stddev=%.2fms p50=%.2fms p90=%.2fms p99=%.2fms (n=%d)",
              read_stats['avg'], read_stats['stddev'], read_stats['p50'],
              read_stats['p90'], read_stats['p99'], len(read_latencies))
        log.info("   Writes — avg=%.2fms stddev=%.2fms p50=%.2fms p90=%.2fms p99=%.2fms (n=%d)",
              write_stats['avg'], write_stats['stddev'], write_stats['p50'],
              write_stats['p90'], write_stats['p99'], len(write_latencies))

        row = (
            f"{consistency},"
            f"{read_stats['avg']:.3f},{read_stats['stddev']:.3f},"
            f"{read_stats['p50']:.3f},{read_stats['p90']:.3f},"
            f"{read_stats['p95']:.3f},{read_stats['p99']:.3f},"
            f"{write_stats['avg']:.3f},{write_stats['stddev']:.3f},"
            f"{write_stats['p50']:.3f},{write_stats['p90']:.3f},"
            f"{write_stats['p95']:.3f},{write_stats['p99']:.3f},"
            f"{len(read_latencies)},{len(write_latencies)}\n"
        )
        result_file.write(row)
        result_file.flush()

    finally:
        # Cleanup
        log.info(">> [%s] Cleaning up ...", consistency)

        # Always clear latency injection first, regardless of errors
        try:
            clear_inter_replica_latency(AR_HOSTS)
        except Exception:
            log.warning("Failed to clear inter-replica latency, continuing cleanup")

        # Destroy service
        destroy_cmd = (
            f"yes yes | XDN_CONTROL_PLANE={CONTROL_PLANE_HOST} "
            f"{XDN_BINARY} service destroy {service_name}"
        )
        try:
            subprocess.run(destroy_cmd, capture_output=True, text=True, shell=True)
        except Exception:
            pass

        # Forceclear cluster
        forceclear_cmd = f"../bin/gpServer.sh -DgigapaxosConfig={config_file} forceclear all > /dev/null 2>&1"
        os.system(forceclear_cmd)
        clear_xdn_cluster()

        # Clean up temp files
        for f in [config_file, service_yaml]:
            try:
                os.remove(f)
            except OSError:
                pass

        time.sleep(5)


def main():
    parser = argparse.ArgumentParser(
        description="Microbenchmark: per-request protocol overhead across consistency models"
    )
    parser.add_argument(
        "--consistency",
        type=str,
        default=None,
        help="Comma-separated list of consistency models to measure (default: all 9)",
    )
    parser.add_argument(
        "--inter-replica-rtt-ms",
        type=float,
        default=DEFAULT_INTER_REPLICA_RTT_MS,
        help="Round-trip inter-replica latency in ms (tc/netem), 0 to disable (default: %.1f)" % DEFAULT_INTER_REPLICA_RTT_MS,
    )
    args = parser.parse_args()

    # Prepare output directory first so we can log to it
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_dir = results_base / f"microbench_lat_consistency_{timestamp}"
    results_dir.mkdir(parents=True, exist_ok=True)
    csv_path = results_dir / "eval_latency_across_consistencies.csv"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(results_dir / "run.log"),
        ],
    )

    if args.consistency:
        models = [m.strip() for m in args.consistency.split(",")]
        for m in models:
            if m not in ALL_CONSISTENCY_MODELS:
                log.error("Unknown consistency model '%s'", m)
                log.error("Valid models: %s", ", ".join(ALL_CONSISTENCY_MODELS))
                sys.exit(1)
    else:
        models = ALL_CONSISTENCY_MODELS

    log.info("=" * 60)
    log.info("Microbenchmark: Per-Request Protocol Overhead")
    log.info("  Models   : %s", models)
    log.info("  Delay    : %.1fms RTT inter-replica (%.1fms one-way)", args.inter_replica_rtt_ms, args.inter_replica_rtt_ms / 2)
    log.info("  Warmup   : %d writes + %d reads", NUM_WARMUP_WRITES, NUM_WARMUP_READS)
    log.info("  Measure  : %d writes + %d reads", NUM_MEASURE_WRITES, NUM_MEASURE_READS)
    log.info("  Results  : %s", results_dir)
    log.info("=" * 60)

    # Verify xdn binary
    ret = os.system(f"{XDN_BINARY} --help > /dev/null 2>&1")
    assert ret == 0, f"Cannot find {XDN_BINARY}"

    # Ensure Docker image is on the RC node
    log.info("Ensuring Docker image is available on RC ...")
    ensure_docker_image_on_rc()

    csv_header = (
        "consistency,"
        "read_avg_ms,read_stddev_ms,read_p50_ms,read_p90_ms,read_p95_ms,read_p99_ms,"
        "write_avg_ms,write_stddev_ms,write_p50_ms,write_p90_ms,write_p95_ms,write_p99_ms,"
        "n_reads,n_writes\n"
    )
    result_file = open(csv_path, "w")
    result_file.write(csv_header)
    result_file.flush()

    for consistency in models:
        log.info("\n%s", "="*60)
        log.info("  Consistency model: %s", consistency)
        log.info("%s", "="*60)
        run_measurement(consistency, result_file,
                        inter_replica_rtt_ms=args.inter_replica_rtt_ms)

    result_file.close()
    log.info("\n[Done] Results saved to %s", csv_path)


if __name__ == "__main__":
    main()
