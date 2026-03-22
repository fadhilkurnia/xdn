"""
run_load_pb_tpcc_reflex.py — TPC-C Primary-Backup throughput-ceiling investigation.

Spins up a fresh XDN cluster, deploys the TPC-C service (PostgreSQL + Flask app),
seeds the database, then runs get_latency_at_rate.go at increasing rates using
POST /orders (New Order transaction) to find the max throughput.

Also serves as the shared-helpers module for the other TPC-C evaluation scripts
(run_load_pb_tpcc_pgsync.py, run_load_pb_tpcc_openebs.py).

Run from the eval/ directory:
    python3 run_load_pb_tpcc_reflex.py

Outputs go to eval/results/load_pb_tpcc_reflex/:
    rate1.txt  rate100.txt  ...  — go-client output per rate
    screen.log                   — copy of the XDN screen log
"""

import argparse
import json
import os
import random
import socket
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import requests

# ── Constants ─────────────────────────────────────────────────────────────────

CONTROL_PLANE_HOST = "10.10.1.4"
AR_HOSTS = ["10.10.1.1", "10.10.1.2", "10.10.1.3"]
HTTP_PROXY_PORT = 2300
SERVICE_NAME = "tpcc"
TPCC_YAML = "../xdn-cli/examples/tpcc.yaml"
XDN_BINARY = "../bin/xdn"
GP_CONFIG = "../conf/gigapaxos.xdn.3way.cloudlab.properties"
GP_JVM_ARGS = "-DPB_N_PARALLEL_WORKERS=32 -DPB_CAPTURE_ACCUMULATION_MS=50"
SCREEN_SESSION = "xdn_tpcc_pb"
SCREEN_LOG = f"screen_logs/{SCREEN_SESSION}.log"

REQUIRED_IMAGES = ["postgres:17.4-bookworm", "fadhilkurnia/xdn-tpcc"]
IMAGE_SOURCE_HOST = AR_HOSTS[0]

NUM_WAREHOUSES = 10

# Timeouts
TIMEOUT_PORT_SEC = 60
TIMEOUT_SERVICE_SEC = 600
TIMEOUT_PRIMARY_SEC = 60

# Rate sweep config — same rates as WordPress for direct comparison
BOTTLENECK_RATES = [
    1, 100, 200, 300, 400, 500, 600, 700, 800, 900,
    1000, 1200, 1500, 2000, 2500, 3000, 3500, 4000,
    4500, 5000, 6000, 7000,
]

LOAD_DURATION_SEC = 90
WARMUP_DURATION_SEC = 15
INTER_RATE_PAUSE_SEC = 30
AVG_LATENCY_THRESHOLD_MS = 3_000

RESULTS_BASE = Path(__file__).resolve().parent / "results"
RESULTS_DIR = RESULTS_BASE / "load_pb_tpcc_reflex"
GO_LATENCY_CLIENT = Path(__file__).resolve().parent / "get_latency_at_rate.go"
PAYLOADS_FILE = RESULTS_DIR / "neworder_payloads.txt"


# ── Shared helpers (importable by other TPC-C scripts) ───────────────────────


def clear_xdn_cluster():
    """Force-clear XDN cluster: kill ports, unmount FUSE, rm state dirs, stop containers."""
    print(" > resetting the cluster:")
    for i in range(3):
        host = f"10.10.1.{i+1}"
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
    os.system(f"ssh {CONTROL_PLANE_HOST} sudo fuser -k 3000/tcp 2>/dev/null || true")
    os.system(f"ssh {CONTROL_PLANE_HOST} sudo rm -rf /tmp/gigapaxos")
    print("   done.")


def start_cluster():
    """Start XDN cluster in a screen session."""
    os.makedirs("screen_logs", exist_ok=True)
    os.system(f"rm -f {SCREEN_LOG}")
    cmd = (
        f"screen -L -Logfile {SCREEN_LOG} -S {SCREEN_SESSION} -d -m bash -c "
        f"'../bin/gpServer.sh -DgigapaxosConfig={GP_CONFIG} {GP_JVM_ARGS} start all; exec bash'"
    )
    print(f"   {cmd}")
    ret = os.system(cmd)
    assert ret == 0, "Failed to start cluster in screen session"
    print(f"   Screen log: {SCREEN_LOG}")
    time.sleep(20)


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
    """Wait for service to respond to HTTP requests via XDN header."""
    print(f"   Waiting up to {timeout_sec}s for '{service}' to respond ...")
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        for host in hosts:
            try:
                r = requests.get(
                    f"http://{host}:{port}/health",
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


def detect_primary(hosts, port, service, timeout_sec):
    """Detect primary node using XdnGetProtocolRoleRequest header."""
    print(f"   Detecting primary for '{service}' ...")
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        for host in hosts:
            try:
                r = requests.get(
                    f"http://{host}:{port}/",
                    headers={"XdnGetProtocolRoleRequest": "true", "XDN": service},
                    timeout=3,
                )
                data = json.loads(r.text)
                if data.get("role") == "primary":
                    return host
            except Exception:
                pass
        time.sleep(3)
    return None


def detect_primary_via_docker(hosts):
    """Detect primary by finding which AR node has running Docker containers."""
    print(f"   Detecting primary for '{SERVICE_NAME}' via docker ps ...")
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


def ensure_docker_images_on_rc():
    """Mirror required Docker images to the RC node."""
    for img in REQUIRED_IMAGES:
        check = subprocess.run(
            ["ssh", CONTROL_PLANE_HOST, f"docker image inspect {img}"],
            capture_output=True,
        )
        if check.returncode == 0:
            print(f"   {img}: already on RC, skipping")
            continue

        src_check = subprocess.run(
            ["ssh", IMAGE_SOURCE_HOST, f"docker image inspect {img}"],
            capture_output=True,
        )
        if src_check.returncode != 0:
            print(f"   {img}: pulling on {IMAGE_SOURCE_HOST} ...")
            ret = os.system(f"ssh {IMAGE_SOURCE_HOST} 'docker pull {img}'")
            if ret != 0:
                print(f"   ERROR: failed to pull {img} on {IMAGE_SOURCE_HOST}")
                sys.exit(1)

        print(f"   {img}: transferring {IMAGE_SOURCE_HOST} -> {CONTROL_PLANE_HOST} ...")
        ret = os.system(
            f"ssh {IMAGE_SOURCE_HOST} 'docker save {img}'"
            f" | ssh {CONTROL_PLANE_HOST} 'docker load'"
        )
        if ret != 0:
            print(f"   ERROR: failed to transfer {img} to RC")
            sys.exit(1)
        print(f"   {img}: done")


# ── TPC-C-specific helpers ────────────────────────────────────────────────────


def init_tpcc_db(host, port, service, warehouses=NUM_WAREHOUSES, timeout=300):
    """Initialize TPC-C database by POSTing to /init_db endpoint.

    Seeding warehouses can be slow, so we use a generous timeout with retries.
    """
    print(f"   Initializing TPC-C DB ({warehouses} warehouses) via {host}:{port} ...")
    deadline = time.time() + timeout
    last_err = None
    while time.time() < deadline:
        try:
            r = requests.post(
                f"http://{host}:{port}/init_db",
                headers={"XDN": service, "Content-Type": "application/json"},
                json={"warehouses": warehouses},
                timeout=timeout,
            )
            if 200 <= r.status_code < 300:
                print(f"   DB initialized (HTTP {r.status_code}): {r.text[:200]}")
                return True
            last_err = f"HTTP {r.status_code}: {r.text[:200]}"
            print(f"   init_db returned {r.status_code}, retrying ...")
        except Exception as e:
            last_err = str(e)
            print(f"   init_db error: {e}, retrying ...")
        time.sleep(10)
    print(f"   ERROR: init_db failed after {timeout}s: {last_err}")
    return False


def check_tpcc_health(host, port, service, timeout=120):
    """Check TPC-C app health via GET /health."""
    print(f"   Checking TPC-C health via {host}:{port}/health ...")
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(
                f"http://{host}:{port}/health",
                headers={"XDN": service},
                timeout=5,
            )
            if r.status_code == 200:
                print(f"   Health check OK (HTTP 200)")
                return True
        except Exception:
            pass
        time.sleep(5)
    print(f"   ERROR: health check failed after {timeout}s")
    return False


def generate_payloads_file(path, num_warehouses=NUM_WAREHOUSES, count=1000):
    """Generate a file of random New Order payloads for the Go load client.

    Each line is a JSON object: {"w_id": N, "c_id": M}
    - w_id: 1..num_warehouses
    - c_id: 1..num_warehouses*10
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as fh:
        for _ in range(count):
            w_id = random.randint(1, num_warehouses)
            c_id = random.randint(1, num_warehouses * 10)
            fh.write(json.dumps({"w_id": w_id, "c_id": c_id}) + "\n")
    print(f"   Generated {count} payloads -> {path}")


def _parse_go_output(path):
    """Parse key:value output from get_latency_at_rate.go."""
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
        "throughput_rps":      metrics.get("actual_throughput_rps", 0.0),
        "actual_achieved_rps": metrics.get("actual_achieved_rate_rps", 0.0),
        "avg_ms":              metrics.get("average_latency_ms", 0.0),
        "p50_ms":              metrics.get("median_latency_ms", 0.0),
        "p90_ms":              metrics.get("p90_latency_ms", 0.0),
        "p95_ms":              metrics.get("p95_latency_ms", 0.0),
        "p99_ms":              metrics.get("p99_latency_ms", 0.0),
    }


def run_load_point(primary, rate):
    """Run get_latency_at_rate.go at `rate` req/s for LOAD_DURATION_SEC seconds.

    Uses POST /orders with XDN header and payloads-file for varied New Order payloads.
    """
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    output_file = RESULTS_DIR / f"rate{rate}.txt"
    url = f"http://{primary}:{HTTP_PROXY_PORT}/orders"
    cmd = [
        "go", "run", str(GO_LATENCY_CLIENT),
        "-H", f"XDN: {SERVICE_NAME}",
        "-H", "Content-Type: application/json",
        "-X", "POST",
        "-payloads-file", str(PAYLOADS_FILE),
        url,
        '{"w_id": 1, "c_id": 1}',
        str(LOAD_DURATION_SEC),
        str(rate),
    ]
    env = os.environ.copy()
    env["GO111MODULE"] = "off"
    env["GOWORK"] = "off"
    print(f"   Output -> {output_file}")
    with open(output_file, "w") as fh:
        result = subprocess.run(cmd, stdout=fh, stderr=subprocess.STDOUT,
                                env=env, text=True)
    if result.returncode != 0:
        print(f"   WARNING: go client exited {result.returncode}")
    return _parse_go_output(output_file)


def warmup_rate_point(primary, rate):
    """Run a throwaway warmup load at `rate` for WARMUP_DURATION_SEC seconds."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    output_file = RESULTS_DIR / f"warmup_rate{rate}.txt"
    url = f"http://{primary}:{HTTP_PROXY_PORT}/orders"
    cmd = [
        "go", "run", str(GO_LATENCY_CLIENT),
        "-H", f"XDN: {SERVICE_NAME}",
        "-H", "Content-Type: application/json",
        "-X", "POST",
        "-payloads-file", str(PAYLOADS_FILE),
        url,
        '{"w_id": 1, "c_id": 1}',
        str(WARMUP_DURATION_SEC),
        str(rate),
    ]
    env = os.environ.copy()
    env["GO111MODULE"] = "off"
    env["GOWORK"] = "off"
    with open(output_file, "w") as fh:
        subprocess.run(cmd, stdout=fh, stderr=subprocess.STDOUT, env=env, text=True)
    m = _parse_go_output(output_file)
    print(f"     [warmup] tput={m['throughput_rps']:.2f} rps  avg={m['avg_ms']:.1f}ms")
    return m


def copy_screen_log():
    """Copy the running screen log snapshot into results dir."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    dest = RESULTS_DIR / "screen.log"
    os.system(f"cp {SCREEN_LOG} {dest} 2>/dev/null || true")
    print(f"   Screen log saved -> {dest}")


# ── Per-rate isolation helpers ────────────────────────────────────────────────


def setup_fresh_tpcc_cluster():
    """Forceclear, start XDN, deploy TPC-C, init DB.  Returns primary host."""
    # Kill any lingering screen session from a previous rate
    os.system(f"screen -S {SCREEN_SESSION} -X quit 2>/dev/null || true")

    print("   [setup] Force-clearing cluster ...")
    clear_xdn_cluster()

    print("   [setup] Starting XDN cluster ...")
    start_cluster()

    print("   [setup] Waiting for Active Replicas ...")
    for host in AR_HOSTS:
        if not wait_for_port(host, HTTP_PROXY_PORT, TIMEOUT_PORT_SEC):
            print(f"ERROR: AR at {host}:{HTTP_PROXY_PORT} not ready.")
            print(f"       Check: tail -f {SCREEN_LOG}")
            sys.exit(1)

    print("   [setup] Ensuring Docker images on RC ...")
    ensure_docker_images_on_rc()

    print("   [setup] Launching TPC-C service ...")
    cmd = (
        f"XDN_CONTROL_PLANE={CONTROL_PLANE_HOST} {XDN_BINARY} "
        f"launch {SERVICE_NAME} --file={TPCC_YAML}"
    )
    print(f"   {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    print(result.stdout.strip())
    if result.returncode != 0:
        print(f"ERROR: xdn launch failed:\n{result.stderr}")
        sys.exit(1)

    print("   [setup] Waiting for TPC-C HTTP readiness ...")
    ok, _ = wait_for_service(
        AR_HOSTS, HTTP_PROXY_PORT, SERVICE_NAME, TIMEOUT_SERVICE_SEC
    )
    if not ok:
        print(f"ERROR: TPC-C not ready after {TIMEOUT_SERVICE_SEC}s.")
        sys.exit(1)

    print("   [setup] Detecting primary ...")
    primary = detect_primary_via_docker(AR_HOSTS)
    if not primary:
        print("   WARNING: Could not detect primary via docker, falling back to first AR")
        primary = AR_HOSTS[0]
    print(f"   [setup] Primary: {primary}")

    print("   [setup] Initializing TPC-C database ...")
    if not init_tpcc_db(primary, HTTP_PROXY_PORT, SERVICE_NAME, NUM_WAREHOUSES):
        print("ERROR: TPC-C database initialization failed.")
        sys.exit(1)

    print("   [setup] Verifying TPC-C health ...")
    if not check_tpcc_health(primary, HTTP_PROXY_PORT, SERVICE_NAME):
        print("ERROR: TPC-C health check failed.")
        sys.exit(1)

    # Set PostgreSQL lock_timeout to prevent long lock waits that cause the convoy
    # effect. Without this, a few requests stuck on row-level locks for 120s (gunicorn
    # timeout) exhaust the worker pool and collapse throughput. lock_timeout is a
    # sighup-level parameter: pg_reload_conf() suffices (no container restart needed).
    print("   [setup] Setting PostgreSQL lock_timeout=2000ms ...")
    pg_container = "c0.e0.tpcc.ar0.xdn.io"
    tune_cmd = (
        f'ssh {primary} "docker exec {pg_container} psql -U postgres '
        f"-c \\\"ALTER SYSTEM SET lock_timeout = '2000';\\\" "
        f'-c \\\"SELECT pg_reload_conf();\\\"" '
    )
    result = subprocess.run(tune_cmd, shell=True, capture_output=True, text=True, timeout=15)
    if result.returncode != 0:
        print(f"   WARNING: lock_timeout tuning failed: {result.stderr.strip()}")
    else:
        verify_cmd = (
            f'ssh {primary} "docker exec {pg_container} psql -U postgres '
            f'-c \\\"SHOW lock_timeout;\\\""'
        )
        vr = subprocess.run(verify_cmd, shell=True, capture_output=True, text=True, timeout=15)
        print(f"   [setup] {vr.stdout.strip()}")

    # Wait for the DB init state diffs to be fully replicated via Paxos.
    # A single fast HTTP response is NOT enough — it only proves the PBM worker
    # can forward to the container. The Paxos pipeline may still be processing
    # thousands of init diffs, causing massive commit latency for new proposals.
    # Require CONSECUTIVE_OK fast probes to confirm the pipeline is drained.
    print(f"   [setup] Waiting for Paxos pipeline to drain (init diffs) ...")
    max_settle_sec = 300
    settle_deadline = time.time() + max_settle_sec
    CONSECUTIVE_OK = 10
    consecutive_ok = 0
    settled = False
    while time.time() < settle_deadline:
        try:
            t0 = time.time()
            r = requests.post(
                f"http://{primary}:{HTTP_PROXY_PORT}/orders",
                headers={"XDN": SERVICE_NAME, "Content-Type": "application/json"},
                json={"w_id": 1, "c_id": 1},
                timeout=60,
            )
            latency = time.time() - t0
            if r.status_code == 200 and latency < 1.0:
                consecutive_ok += 1
                print(f"   [setup] Probe OK ({consecutive_ok}/{CONSECUTIVE_OK}, latency={latency:.2f}s)")
                if consecutive_ok >= CONSECUTIVE_OK:
                    print(f"   [setup] Paxos pipeline drained — {CONSECUTIVE_OK} consecutive fast probes")
                    settled = True
                    break
                time.sleep(2)
            else:
                consecutive_ok = 0
                elapsed = int(max_settle_sec - (settle_deadline - time.time()))
                print(f"   [setup] Not settled (latency={latency:.1f}s, HTTP {r.status_code}), "
                      f"elapsed={elapsed}s ...")
                time.sleep(5)
        except Exception as e:
            consecutive_ok = 0
            print(f"   [setup] Probe failed ({e}), waiting ...")
            time.sleep(5)
    if not settled:
        print(f"   [setup] WARNING: pipeline did not drain within {max_settle_sec}s, proceeding anyway")

    # NOTE: Init-sync wait removed. The Paxos drain probes above naturally
    # block during init-sync stalls (high latency rejects the probe), which
    # delays measurement until BOTH init-sync AND PostgreSQL post-init
    # activity have completed. Explicit init-sync wait was counterproductive:
    # it let probes pass too early, then measurement hit PostgreSQL stalls.

    return primary


def copy_screen_log_for_rate(rate):
    """Copy the running screen log snapshot into results dir, tagged by rate."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    dest = RESULTS_DIR / f"screen_rate{rate}.log"
    os.system(f"cp {SCREEN_LOG} {dest} 2>/dev/null || true")
    print(f"   Screen log saved -> {dest}")
    return dest


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--rates", type=str, default=None,
                   help="Comma-separated rates to override default BOTTLENECK_RATES")
    p.add_argument("--skip-setup", action="store_true",
                   help="Skip cluster setup for ALL rates (assume already running)")
    return p.parse_args()


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    args = parse_args()

    # Compute timestamped results directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    RESULTS_DIR = RESULTS_BASE / f"tpcc_pb_{timestamp}"
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    PAYLOADS_FILE = RESULTS_DIR / "neworder_payloads.txt"

    # Determine rate list
    rates = [int(r) for r in args.rates.split(",")] if args.rates else BOTTLENECK_RATES

    print("=" * 60)
    print("TPC-C Primary-Backup Throughput Investigation")
    print(f"  Mode     : fresh cluster per rate (isolated)")
    print(f"  Workload : POST /orders (New Order)  XDN:{SERVICE_NAME}")
    print(f"  Rates    : {rates}")
    print(f"  Results  -> {RESULTS_DIR}")
    print("=" * 60)

    # Generate payloads (once, reused across rates)
    print(f"\n[Setup] Generating New Order payloads -> {PAYLOADS_FILE} ...")
    generate_payloads_file(PAYLOADS_FILE, NUM_WAREHOUSES)

    results = []
    screen_logs = []

    for i, rate in enumerate(rates):
        print(f"\n{'='*60}")
        print(f"  Rate point {i+1}/{len(rates)}: {rate} req/s")
        print(f"{'='*60}")

        # ── Per-rate fresh cluster setup ──────────────────────────────
        if not args.skip_setup:
            primary = setup_fresh_tpcc_cluster()
        else:
            print("   [setup] Skipped (--skip-setup)")
            primary = detect_primary_via_docker(AR_HOSTS)
            if not primary:
                primary = AR_HOSTS[0]
            print(f"   Primary: {primary}")

        # ── Warmup ────────────────────────────────────────────────────
        print(f"\n   -> rate={rate} req/s (warmup {WARMUP_DURATION_SEC}s) ...")
        warmup_result = warmup_rate_point(primary, rate)
        if warmup_result["throughput_rps"] < 0.1 * rate:
            print(
                f"   SKIP: warmup tput={warmup_result['throughput_rps']:.2f} rps "
                f"< 10% of {rate} rps -- system saturated"
            )
            log_path = copy_screen_log_for_rate(rate)
            screen_logs.append(log_path)
            continue
        print(f"   Settling 5s after warmup ...")
        time.sleep(5)

        # ── Measurement ───────────────────────────────────────────────
        print(f"   -> rate={rate} req/s (measuring {LOAD_DURATION_SEC}s) ...")
        m = run_load_point(primary, rate)
        row = {"rate_rps": rate, **m}
        results.append(row)
        print(
            f"     tput={m['throughput_rps']:.2f} rps  "
            f"avg={m['avg_ms']:.1f}ms  "
            f"p50={m['p50_ms']:.1f}ms  "
            f"p90={m['p90_ms']:.1f}ms  "
            f"p95={m['p95_ms']:.1f}ms"
        )

        # ── Per-rate log collection ───────────────────────────────────
        log_path = copy_screen_log_for_rate(rate)
        screen_logs.append(log_path)

        # ── Early stop on saturation ──────────────────────────────────
        latency_saturated = m["avg_ms"] > AVG_LATENCY_THRESHOLD_MS
        throughput_saturated = (
            m["actual_achieved_rps"] > 0
            and m["throughput_rps"] < 0.75 * m["actual_achieved_rps"]
        )
        if latency_saturated and throughput_saturated:
            print(
                f"   Avg latency {m['avg_ms']:.0f}ms > {AVG_LATENCY_THRESHOLD_MS}ms "
                f"AND throughput {m['throughput_rps']:.2f} rps < 75% of offered "
                f"{m['actual_achieved_rps']:.2f} rps -- stopping sweep early."
            )
            break

    # ── Concatenate per-rate screen logs for timing analysis ──────────
    combined_log = RESULTS_DIR / "screen.log"
    with open(combined_log, "w") as out:
        for log_path in screen_logs:
            if log_path.exists():
                out.write(log_path.read_text())
    print(f"\n   Combined screen log -> {combined_log}")

    # ── Create/update symlink ─────────────────────────────────────────
    symlink = RESULTS_BASE / "load_pb_tpcc_reflex"
    if symlink.is_symlink():
        symlink.unlink()
    elif symlink.is_dir():
        # Rename old non-symlink directory out of the way
        backup = RESULTS_BASE / f"tpcc_pb_old_{timestamp}"
        symlink.rename(backup)
        print(f"   Renamed old directory -> {backup.name}")
    symlink.symlink_to(RESULTS_DIR.name)
    print(f"   Symlink: {symlink} -> {RESULTS_DIR.name}")

    # ── Summary ───────────────────────────────────────────────────────
    print("\n[Summary]")
    print(f"   {'rate':>6}  {'tput':>8}  {'avg':>8}  {'p50':>8}  {'p95':>8}")
    print("   " + "-" * 48)
    for row in results:
        print(
            f"   {row['rate_rps']:>5.0f}  "
            f"{row['throughput_rps']:>7.2f}  "
            f"{row['avg_ms']:>7.1f}ms  "
            f"{row['p50_ms']:>7.1f}ms  "
            f"{row['p95_ms']:>7.1f}ms"
        )

    # ── Generate plots ────────────────────────────────────────────────
    print("\n[Plots] Generating plots ...")
    eval_dir = str(Path(__file__).resolve().parent)
    for script in ["investigate_parse_pb_timing.py", "plot_load_latency.py"]:
        cmd = ["python3", script, "--results-dir", str(RESULTS_DIR)]
        if script == "investigate_parse_pb_timing.py":
            cmd += ["--log", str(combined_log)]
        r = subprocess.run(
            cmd,
            capture_output=True, text=True,
            cwd=eval_dir,
        )
        print(r.stdout)
        if r.returncode != 0:
            print(f"   WARNING: {script} failed:\n{r.stderr}")

    print(f"\n[Done] Results in {RESULTS_DIR}/")
