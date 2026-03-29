"""pb_common.py — Shared infrastructure for consolidated PB benchmark scripts.

Contains cluster management, load-test mechanics, and results-collection
helpers that were previously duplicated across run_load_pb_*_common.py and
run_load_pb_*_reflex.py scripts.

Usage:
    from pb_common import (
        clear_xdn_cluster, start_cluster, wait_for_port, wait_for_service,
        detect_primary_via_docker, ensure_docker_images,
        parse_go_output, run_load, print_metrics, check_saturation,
        copy_screen_log, save_effective_config,
    )
"""

import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import requests

# Re-export save_effective_config from gp_config_utils so callers can
# import it from this single module.
from gp_config_utils import save_effective_config


# ── Constants ─────────────────────────────────────────────────────────────────

CONTROL_PLANE_HOST = "10.10.1.4"
AR_HOSTS = ["10.10.1.1", "10.10.1.2", "10.10.1.3"]
HTTP_PROXY_PORT = 2300
XDN_BINARY = "../bin/xdn"

BOTTLENECK_RATES = [
    1, 100, 200, 300, 400, 500, 600, 700, 800, 900,
    1000, 1200, 1500, 2000, 2500, 3000, 3500, 4000,
    4500, 5000, 6000, 7000,
]

LOAD_DURATION_SEC = 60
WARMUP_DURATION_SEC = 15
INTER_RATE_PAUSE_SEC = 30
AVG_LATENCY_THRESHOLD_MS = 1_000
GO_LATENCY_CLIENT = Path(__file__).resolve().parent / "get_latency_at_rate.go"


# ── Cluster Management ───────────────────────────────────────────────────────


def clear_xdn_cluster():
    """Force-clear the XDN cluster: stop containers, kill processes, unmount
    FUSE, remove state dirs, and kill leftover k8s/OpenEBS agents.

    Uses /proc/mounts (not df) to find FUSE mounts, which avoids failures on
    stale mounts where df returns 'Transport endpoint is not connected'.
    """
    print(" > resetting the cluster:")
    for i in range(3):
        host = f"10.10.1.{i+1}"
        # 1. Stop Docker containers FIRST — they hold files open inside FUSE mounts
        os.system(
            f"ssh {host} 'containers=$(docker ps -a -q); "
            f"if [ -n \"$containers\" ]; then docker stop $containers && docker rm $containers; fi'"
        )
        os.system(f"ssh {host} docker network prune --force > /dev/null 2>&1")
        # 2. Kill Java (GigaPaxos) processes
        os.system(f"ssh {host} sudo fuser -k 2000/tcp 2>/dev/null || true")
        os.system(f"ssh {host} sudo fuser -k 2300/tcp 2>/dev/null || true")
        # 3. Kill fuselog FUSE processes (not bound to TCP ports)
        os.system(f"ssh {host} sudo pkill -9 -f fuselog 2>/dev/null || true")
        # 4. Unmount FUSE filesystems. Use /proc/mounts instead of df because
        #    df fails on stale FUSE mounts ("Transport endpoint is not connected").
        os.system(
            f"ssh {host} \"grep fuselog /proc/mounts"
            f" | awk '{{print \\$2}}' | xargs -r sudo umount 2>/dev/null;"
            f" grep fuselog /proc/mounts"
            f" | awk '{{print \\$2}}' | xargs -r sudo umount -l 2>/dev/null || true\""
        )
        # 5. Brief wait for unmount completion, then remove state dirs
        os.system(f"ssh {host} 'sleep 1 && sudo rm -rf /tmp/gigapaxos /tmp/xdn /dev/shm/xdn'")
    # RC node
    os.system(f"ssh {CONTROL_PLANE_HOST} sudo fuser -k 3000/tcp 2>/dev/null || true")
    os.system(f"ssh {CONTROL_PLANE_HOST} sudo rm -rf /tmp/gigapaxos")

    # Kill leftover k8s/OpenEBS agents (io-engine, kubelet, etc.) that steal
    # CPU from XDN benchmarks.  This is safe because the PB evaluation does
    # not use Kubernetes.
    # IMPORTANT: disconnect NVMe-oF targets BEFORE killing io-engine to prevent
    # the nvme_tcp kernel module from retrying stale connections indefinitely,
    # which triggers a kernel NULL pointer dereference panic on Linux 5.15.
    for i in range(3):
        host = f"10.10.1.{i+1}"
        os.system(
            f"ssh {host} '"
            f"sudo nvme disconnect-all 2>/dev/null;"
            f" sudo systemctl stop kubelet 2>/dev/null;"
            f" sudo pkill -9 io-engine 2>/dev/null;"
            f" sudo pkill -9 kube-proxy 2>/dev/null;"
            f" sudo pkill -9 minio 2>/dev/null;"
            f" sudo pkill -9 -f \"agent-ha|csi-node|openebs|obs-callhome"
            f"|operator-diskpool|metrics-exporter|nats-server\" 2>/dev/null;"
            f" sudo pkill -9 -f \"containerd-shim.*k8s.io\" 2>/dev/null;"
            f" true'"
        )
    print("   done.")


def start_cluster(gp_config, gp_jvm_args, screen_session, screen_log):
    """Start the XDN cluster in a detached screen session.

    Applies Config-framework overrides (SYNC, BATCHING_ENABLED, etc.) from
    gp_jvm_args into the .properties file before launching.

    Returns the cleaned gp_jvm_args string (with Config-framework flags removed).
    """
    from gp_config_utils import apply_config_overrides
    gp_jvm_args = apply_config_overrides(gp_jvm_args, gp_config)
    os.makedirs("screen_logs", exist_ok=True)
    os.system(f"rm -f {screen_log}")
    cmd = (
        f"screen -L -Logfile {screen_log} -S {screen_session} -d -m bash -c "
        f"'../bin/gpServer.sh -DgigapaxosConfig={gp_config} {gp_jvm_args} start all; exec bash'"
    )
    print(f"   {cmd}")
    ret = os.system(cmd)
    assert ret == 0, "Failed to start cluster in screen session"
    print(f"   Screen log: {screen_log}")
    time.sleep(20)
    return gp_jvm_args


# ── Readiness Checks ─────────────────────────────────────────────────────────


def wait_for_port(host, port, timeout_sec):
    """Wait for a TCP port to become reachable."""
    print(f"   Waiting for {host}:{port} ...")
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            s = socket.create_connection((host, port), timeout=2)
            s.close()
            print(f"   OK: {host}:{port}")
            return True
        except OSError:
            time.sleep(2)
    print(f"   TIMEOUT: {host}:{port} not open after {timeout_sec}s")
    return False


def wait_for_service(hosts, port, service, timeout_sec):
    """Wait for the named service to respond via XDN proxy on any host."""
    print(f"   Waiting up to {timeout_sec}s for '{service}' to respond ...")
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        for host in hosts:
            try:
                r = requests.get(
                    f"http://{host}:{port}/",
                    headers={"XDN": service},
                    timeout=5,
                )
                if 200 <= r.status_code < 500:
                    print(f"   READY: HTTP {r.status_code} from {host}")
                    return True, host
            except Exception:
                pass
        elapsed = int(timeout_sec - (deadline - time.time()))
        print(f"   ... {elapsed}s elapsed, still waiting ...")
        time.sleep(5)
    return False, None


# ── Primary Detection ────────────────────────────────────────────────────────


def detect_primary_via_docker(hosts):
    """Detect primary by finding which AR node has running Docker containers."""
    print("   Detecting primary via docker ps ...")
    for host in hosts:
        result = subprocess.run(
            ["ssh", host, "docker ps -q"],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0 and result.stdout.strip():
            containers = result.stdout.strip().splitlines()
            print(f"   {host} has {len(containers)} running container(s) -> PRIMARY")
            return host
        else:
            print(f"   {host}: no running containers")
    return None


# ── Docker Image Helpers ─────────────────────────────────────────────────────


def ensure_docker_images(images, target_hosts, source=None):
    """Ensure each image in `images` is present on every host in `target_hosts`.

    If an image is missing on a target host, it is pulled (if needed) on the
    source host and transferred via docker save|load.

    Args:
        images: list of Docker image names (e.g. ["mysql:8.4.0", "wordpress:6.5.4-apache"])
        target_hosts: list of hosts that need the images (e.g. [CONTROL_PLANE_HOST])
        source: host to pull/save images from (defaults to AR_HOSTS[0])
    """
    if source is None:
        source = AR_HOSTS[0]

    for img in images:
        for target in target_hosts:
            check = subprocess.run(
                ["ssh", target, f"docker image inspect {img}"],
                capture_output=True,
            )
            if check.returncode == 0:
                print(f"   {img}: already on {target}, skipping")
                continue

            # Ensure image is present on source (pull from Docker Hub if needed)
            src_check = subprocess.run(
                ["ssh", source, f"docker image inspect {img}"],
                capture_output=True,
            )
            if src_check.returncode != 0:
                print(f"   {img}: pulling on {source} ...")
                ret = os.system(f"ssh {source} 'docker pull {img}'")
                if ret != 0:
                    print(f"   ERROR: failed to pull {img} on {source}")
                    sys.exit(1)

            print(f"   {img}: transferring {source} -> {target} ...")
            ret = os.system(
                f"ssh {source} 'docker save {img}'"
                f" | ssh {target} 'docker load'"
            )
            if ret != 0:
                print(f"   ERROR: failed to transfer {img} to {target}")
                sys.exit(1)
            print(f"   {img}: done")


# ── Load-Test Mechanics ──────────────────────────────────────────────────────


def parse_go_output(path):
    """Parse output from get_latency_at_rate.go into a metrics dict.

    Returns a dict with keys: throughput_rps, actual_achieved_rps, avg_ms,
    p50_ms, p90_ms, p95_ms, p99_ms.
    """
    metrics = {}
    for line in open(path):
        if ":" not in line:
            continue
        k, v = line.split(":", 1)
        try:
            metrics[k.strip()] = float(v.strip())
        except ValueError:
            pass
    return {
        "throughput_rps":       metrics.get("actual_throughput_rps", 0.0),
        "actual_achieved_rps":  metrics.get("actual_achieved_rate_rps", 0.0),
        "avg_ms":               metrics.get("average_latency_ms", 0.0),
        "p50_ms":               metrics.get("median_latency_ms", 0.0),
        "p90_ms":               metrics.get("p90_latency_ms", 0.0),
        "p95_ms":               metrics.get("p95_latency_ms", 0.0),
        "p99_ms":               metrics.get("p99_latency_ms", 0.0),
    }


def run_load(go_args, output_file):
    """Run get_latency_at_rate.go with the given argument list.

    Args:
        go_args: list of CLI arguments to pass after 'go run get_latency_at_rate.go'
        output_file: path (str or Path) to write the go client's stdout

    Returns:
        Parsed metrics dict from parse_go_output().
    """
    output_file = Path(output_file)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    cmd = ["go", "run", str(GO_LATENCY_CLIENT)] + list(go_args)
    env = os.environ.copy()
    env["GO111MODULE"] = "off"
    env["GOWORK"] = "off"
    print(f"   Output -> {output_file}")
    with open(output_file, "w") as fh:
        result = subprocess.run(cmd, stdout=fh, stderr=subprocess.STDOUT,
                                env=env, text=True)
    if result.returncode != 0:
        print(f"   WARNING: go client exited {result.returncode}")
    return parse_go_output(output_file)


def print_metrics(m):
    """Print a one-liner summary of load-test metrics."""
    print(
        f"     tput={m['throughput_rps']:.2f} rps  "
        f"avg={m['avg_ms']:.1f}ms  "
        f"p50={m['p50_ms']:.1f}ms  "
        f"p95={m['p95_ms']:.1f}ms"
    )


def check_saturation(m, avg_threshold_ms=AVG_LATENCY_THRESHOLD_MS):
    """Check whether the system is saturated based on latency and throughput.

    Returns:
        (latency_saturated, throughput_saturated) — both are booleans.
        The system is considered saturated when BOTH are True.
    """
    latency_saturated = m["avg_ms"] > avg_threshold_ms
    throughput_saturated = (
        m["actual_achieved_rps"] > 0
        and m["throughput_rps"] < 0.75 * m["actual_achieved_rps"]
    )
    return latency_saturated, throughput_saturated


# ── Results Collection ───────────────────────────────────────────────────────


def copy_screen_log(screen_log_path, results_dir, rate):
    """Copy the running screen log snapshot into results dir, tagged by rate.

    Args:
        screen_log_path: path to the live screen log file
        results_dir: directory to copy into (str or Path)
        rate: rate label for the filename

    Returns:
        Path to the copied log file.
    """
    results_dir = Path(results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)
    dest = results_dir / f"screen_rate{rate}.log"
    os.system(f"cp {screen_log_path} {dest} 2>/dev/null || true")
    print(f"   Screen log saved -> {dest}")
    return dest
