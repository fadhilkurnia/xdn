import warnings as _w; _w.warn("DEPRECATED: Use run_load_pb_reflex_app.py --app tpcc instead.", DeprecationWarning, stacklevel=2)
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
GP_JVM_ARGS = (
    "-DSYNC=true "                        # fsync Paxos proposals to disk for durability
    "-DPB_N_PARALLEL_WORKERS=256 "
    "-DPB_CAPTURE_ACCUMULATION_US=100 "   # 0.1ms min, adaptive up to 5ms
    "-DHTTP_FORCE_KEEPALIVE=true "        # prevent Connection:close from killing pooled channels
    "-DFUSELOG_DISABLE_COALESCING=true "   # skip read-before-write in fuselog
    "-DXDN_MIGRATE_WAIT_SEC=5 "           # reduce PostgreSQL migration wait for benchmarks
    "-DXDN_SKIP_INIT_SYNC=true "          # skip rsync to backups; PB still works via statediffs
    "-DPB_INLINE_EXECUTE=true "           # execute on writePool thread, no worker queue
    "-DBATCHING_ENABLED=false"            # skip GigaPaxos RequestBatcher hop
)
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

LOAD_DURATION_SEC = 60
WARMUP_DURATION_SEC = 15
WARMUP_RATE = 100  # TPC-C NewOrder is heavier than WordPress editPost
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
    global GP_JVM_ARGS
    from gp_config_utils import apply_config_overrides
    GP_JVM_ARGS = apply_config_overrides(GP_JVM_ARGS, GP_CONFIG)
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


def _go_env():
    """Return an env dict with Go in PATH and module mode off."""
    env = os.environ.copy()
    env["GO111MODULE"] = "off"
    env["GOWORK"] = "off"
    # Ensure Go binary is in PATH (common install location)
    go_paths = ["/usr/local/go/bin", os.path.expanduser("~/go/bin")]
    for p in go_paths:
        if os.path.isdir(p) and p not in env.get("PATH", ""):
            env["PATH"] = p + ":" + env.get("PATH", "")
    return env


def run_load_point(primary, rate, duration_sec=LOAD_DURATION_SEC):
    """Run get_latency_at_rate.go at `rate` req/s for `duration_sec` seconds.

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
        str(duration_sec),
        str(rate),
    ]
    env = _go_env()
    print(f"   Output -> {output_file}")
    with open(output_file, "w") as fh:
        result = subprocess.run(cmd, stdout=fh, stderr=subprocess.STDOUT,
                                env=env, text=True)
    if result.returncode != 0:
        print(f"   WARNING: go client exited {result.returncode}")
    return _parse_go_output(output_file)


def warmup_rate_point(primary, rate, warmup_duration_sec=WARMUP_DURATION_SEC):
    """Run a throwaway warmup load at `rate` for `warmup_duration_sec` seconds."""
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
        str(warmup_duration_sec),
        str(rate),
    ]
    env = _go_env()
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
    # Collect PBM sample logs from the primary
    primary = detect_primary_via_docker(AR_HOSTS)
    if primary:
        sample_dest = RESULTS_DIR / f"pbm_samples_rate{rate}.log"
        os.system(f"scp {primary}:/tmp/pbm_samples.log {sample_dest} 2>/dev/null || true")
        os.system(f"ssh {primary} 'rm -f /tmp/pbm_samples.log' 2>/dev/null || true")
        if sample_dest.exists() and sample_dest.stat().st_size > 0:
            print(f"   PBM samples saved -> {sample_dest}")
    return dest


def _ssh_output(host, cmd):
    """Run SSH command and return stdout."""
    r = subprocess.run(
        ["ssh", "-o", "StrictHostKeyChecking=no", host, cmd],
        capture_output=True, text=True, timeout=15,
    )
    return r.stdout.strip()


def _get_proc_io(host, pid):
    """Read /proc/<pid>/io counters."""
    out = _ssh_output(host, f"sudo cat /proc/{pid}/io")
    counters = {}
    for line in out.splitlines():
        if ":" in line:
            k, v = line.split(":", 1)
            counters[k.strip()] = int(v.strip())
    return counters


def _get_jvm_pid(host):
    """Find the ReconfigurableNode JVM PID on a host."""
    return _ssh_output(host,
        "ps -ef | grep 'java.*ReconfigurableNode' | grep -v grep | grep -v sudo | awk '{print $2}' | head -1")


def _get_container_pid(host, name_pattern):
    """Find the main PID of a Docker container matching the pattern."""
    name = _ssh_output(host, f"docker ps --format '{{{{.Names}}}}' | grep '{name_pattern}' | head -1")
    if not name:
        return None, None
    pid = _ssh_output(host, f"docker inspect {name} --format '{{{{.State.Pid}}}}'")
    return name, pid


def _get_postgres_container_pid(host):
    """Find the PostgreSQL container and its main postgres PID (not init)."""
    name = _ssh_output(host, "docker ps --format '{{.Names}}' | grep postgres | head -1")
    if not name:
        # Try matching by image
        name = _ssh_output(host, "docker ps --format '{{.Names}} {{.Image}}' | grep postgres | awk '{print $1}' | head -1")
    if not name:
        return None, None
    # Get the actual postgres server PID inside the container (not the init PID)
    pid = _ssh_output(host,
        f"docker exec {name} bash -c 'pgrep -x postgres | head -1' 2>/dev/null")
    if not pid:
        # Fallback to container init PID
        pid = _ssh_output(host, f"docker inspect {name} --format '{{{{.State.Pid}}}}'")
    return name, pid


def _get_net_tx(host, iface="enp195s0np0"):
    """Get TX bytes from /proc/net/dev."""
    out = _ssh_output(host, f"cat /proc/net/dev | grep {iface}")
    parts = out.split()
    return int(parts[9]) if len(parts) >= 10 else 0


def _count_fsyncs(host, pid, tag=None):
    """Start strace on a PID to count fsyncs. Runs in background via setsid.

    Uses -f to follow forks (needed for PostgreSQL which forks worker processes,
    and JVM which has many threads).
    tag: optional string to distinguish strace output files when tracing multiple PIDs.
    Call _stop_strace_and_count() after sending requests to get the counts.
    """
    label = tag or pid
    subprocess.run(
        ["ssh", "-o", "StrictHostKeyChecking=no", host,
         f"sudo setsid strace -f -c -e fdatasync,fsync,sync_file_range -p {pid} "
         f"-o /tmp/strace_fsync_{label}.out </dev/null >/dev/null 2>&1 &"],
        capture_output=True, text=True, timeout=5,
    )


def _stop_strace_and_count(host, pid, tag=None):
    """Stop strace and parse fsync counts.

    With -f (follow forks), strace -c output may have multiple per-pid
    summaries. We sum across all of them.
    """
    label = tag or pid
    # Kill ALL strace processes tracing this PID
    _ssh_output(host, f"sudo pkill -INT -f 'strace.*-p {pid}' 2>/dev/null; sleep 1")
    out = _ssh_output(host, f"sudo cat /tmp/strace_fsync_{label}.out 2>/dev/null")
    _ssh_output(host, f"sudo rm -f /tmp/strace_fsync_{label}.out 2>/dev/null")

    fsyncs = 0
    fdatasyncs = 0
    sync_ranges = 0
    for line in out.splitlines():
        parts = line.split()
        if len(parts) < 5:
            continue
        # strace -c format: % time  seconds  usecs/call  calls  errors  syscall
        # The 'calls' column is typically at index 3 (0-indexed)
        syscall = parts[-1]
        try:
            calls = int(parts[3])
        except (ValueError, IndexError):
            continue
        if syscall == 'fsync':
            fsyncs += calls
        elif syscall == 'fdatasync':
            fdatasyncs += calls
        elif syscall == 'sync_file_range':
            sync_ranges += calls
    return fsyncs, fdatasyncs, sync_ranges


def _get_all_child_pids(host, parent_pid):
    """Get all descendant PIDs of a process (for tracing entire process trees)."""
    out = _ssh_output(host, f"sudo pstree -p {parent_pid} | grep -oP '\\(\\K[0-9]+' 2>/dev/null")
    return [p.strip() for p in out.splitlines() if p.strip() and p.strip() != str(parent_pid)]


def _get_fuselog_pid(host):
    """Find the fuselog FUSE daemon PID."""
    pid = _ssh_output(host, "pgrep -f '/usr/local/bin/fuselog' | head -1")
    return pid if pid else None


def _get_proc_io_tree(host, parent_pid):
    """Sum /proc/pid/io across a process and all its children."""
    pids = [str(parent_pid)] + _get_all_child_pids(host, parent_pid)
    total = {"write_bytes": 0, "syscw": 0, "read_bytes": 0, "syscr": 0}
    for pid in pids:
        io = _get_proc_io(host, pid)
        if io:
            for k in total:
                total[k] += io.get(k, 0)
    return total


def run_sanity_check(primary, backups, payload_file):
    """Send individual requests and measure per-request I/O on all components.

    Measures for each request:
      - Service container: write_bytes, syscw (write syscalls ≈ fsyncs)
      - GigaPaxos JVM (primary): write_bytes, syscw
      - GigaPaxos JVM (backup): write_bytes, syscw
      - Network: TX bytes from primary (includes Paxos proposal to backups)
      - Statediff size: from /tmp/pbm_samples.log on primary
    """
    print("\n" + "=" * 70)
    print("  SANITY CHECK: Per-request I/O measurement")
    print("=" * 70)

    # Find PIDs
    pg_name, pg_pid = _get_postgres_container_pid(primary)
    jvm_primary_pid = _get_jvm_pid(primary)
    fuselog_pid = _get_fuselog_pid(primary)
    backup_host = backups[0] if backups else None
    jvm_backup_pid = _get_jvm_pid(backup_host) if backup_host else None

    print(f"  Primary:   {primary}")
    print(f"  PostgreSQL:{pg_name} (PID={pg_pid})")
    print(f"  Fuselog:   PID={fuselog_pid}")
    print(f"  JVM (pri): PID={jvm_primary_pid}")
    if backup_host:
        print(f"  Backup:    {backup_host}")
        print(f"  JVM (bak): PID={jvm_backup_pid}")

    if not svc_pid or not jvm_primary_pid:
        print("  ERROR: could not find PIDs, skipping sanity check")
        return

    # Clear any stale PBM samples
    subprocess.run(["ssh", primary, "rm -f /tmp/pbm_samples.log"],
                   capture_output=True, timeout=5)

    # Read first payload for sending
    with open(payload_file, "r") as f:
        payload = f.readline().strip()
    if not payload:
        print("  ERROR: no payload in file")
        return

    # Detect network interface
    iface_out = _ssh_output(primary, f"ip route get {backup_host} | head -1") if backup_host else ""
    iface = "enp195s0np0"  # default
    if "dev " in iface_out:
        iface = iface_out.split("dev ")[1].split()[0]
    print(f"  Network:   {iface}")

    n_samples = 10
    http_payload_size = len(payload.encode())
    # Estimate full HTTP request size (method + URL + headers + body)
    url = f"http://{primary}:{HTTP_PROXY_PORT}/orders"
    request_line = f"POST /orders HTTP/1.1\r\n"
    headers_text = (
        f"Host: {primary}:{HTTP_PROXY_PORT}\r\n"
        f"Content-Type: application/json\r\n"
        f"XDN: {SERVICE_NAME}\r\n"
        f"Content-Length: {http_payload_size}\r\n"
        f"\r\n"
    )
    http_req_size = len(request_line.encode()) + len(headers_text.encode()) + http_payload_size
    print(f"\n  HTTP request: payload={http_payload_size} bytes, total≈{http_req_size} bytes")
    print(f"  Sending {n_samples} individual requests ...\n")

    header = (f"  {'#':>3}  {'http_req':>9}  {'svc_write':>10}  {'svc_fsync':>10}  {'fuse_write':>10}  "
              f"{'jvm_write':>10}  {'jvm_fsync':>10}  {'bak_write':>10}  {'bak_fsync':>10}  "
              f"{'net_tx':>10}  {'diff_size':>10}")
    units = (f"  {'':>3}  {'(bytes)':>9}  {'(bytes)':>10}  {'(count)':>10}  {'(bytes)':>10}  "
             f"{'(bytes)':>10}  {'(count)':>10}  {'(bytes)':>10}  {'(count)':>10}  "
             f"{'(bytes)':>10}  {'(bytes)':>10}")
    print(header)
    print(units)
    print("  " + "-" * 120)

    results = []
    import urllib.request

    for i in range(1, n_samples + 1):
        # Before — use process tree I/O to capture all children
        svc_io_before = _get_proc_io_tree(primary, pg_pid) if pg_pid else {}
        fuse_io_before = _get_proc_io(primary, fuselog_pid) if fuselog_pid else {}
        jvm_io_before = _get_proc_io_tree(primary, jvm_primary_pid)
        bak_io_before = _get_proc_io_tree(backup_host, jvm_backup_pid) if jvm_backup_pid else {}
        net_before = _get_net_tx(primary, iface) if backup_host else 0

        # Clear PBM samples
        subprocess.run(["ssh", primary, "rm -f /tmp/pbm_samples.log"],
                       capture_output=True, timeout=5)

        # Start strace on all processes to count fsyncs
        # Use docker exec nsenter for PostgreSQL to trace inside the container namespace
        if pg_pid:
            _count_fsyncs(primary, pg_pid, tag="pg")
        _count_fsyncs(primary, jvm_primary_pid, tag="jvm_pri")
        if jvm_backup_pid:
            _count_fsyncs(backup_host, jvm_backup_pid, tag="jvm_bak")

        time.sleep(0.3)  # let strace attach

        # Send ONE request
        url = f"http://{primary}:{HTTP_PROXY_PORT}/orders"
        req = urllib.request.Request(
            url, data=payload.encode(),
            headers={"Content-Type": "application/json", "XDN": SERVICE_NAME},
            method="POST",
        )
        try:
            resp = urllib.request.urlopen(req, timeout=30)
            resp.read()
        except Exception as e:
            print(f"  {i:>3}  ERROR: {e}")
            if pg_pid:
                _stop_strace_and_count(primary, pg_pid, tag="pg")
            _stop_strace_and_count(primary, jvm_primary_pid, tag="jvm_pri")
            if jvm_backup_pid:
                _stop_strace_and_count(backup_host, jvm_backup_pid, tag="jvm_bak")
            continue

        time.sleep(1.0)  # let writes and replication flush

        # Stop strace and get fsync counts
        if pg_pid:
            svc_fsync, svc_fdatasync, svc_syncrange = _stop_strace_and_count(primary, pg_pid, tag="pg")
        else:
            svc_fsync, svc_fdatasync, svc_syncrange = 0, 0, 0
        jvm_fsync, jvm_fdatasync, jvm_syncrange = _stop_strace_and_count(primary, jvm_primary_pid, tag="jvm_pri")
        if jvm_backup_pid:
            bak_fsync, bak_fdatasync, bak_syncrange = _stop_strace_and_count(backup_host, jvm_backup_pid, tag="jvm_bak")
        else:
            bak_fsync, bak_fdatasync, bak_syncrange = 0, 0, 0

        svc_total_fsync = svc_fsync + svc_fdatasync + svc_syncrange
        jvm_total_fsync = jvm_fsync + jvm_fdatasync + jvm_syncrange
        bak_total_fsync = bak_fsync + bak_fdatasync + bak_syncrange

        # After — use process tree I/O
        svc_io_after = _get_proc_io_tree(primary, pg_pid) if pg_pid else {}
        fuse_io_after = _get_proc_io(primary, fuselog_pid) if fuselog_pid else {}
        jvm_io_after = _get_proc_io_tree(primary, jvm_primary_pid)
        bak_io_after = _get_proc_io_tree(backup_host, jvm_backup_pid) if jvm_backup_pid else {}
        net_after = _get_net_tx(primary, iface) if backup_host else 0

        # Statediff size
        diff_out = _ssh_output(primary, "cat /tmp/pbm_samples.log 2>/dev/null | grep -oP 'diffSize=\\K[0-9]+' | tail -1")
        diff_size = int(diff_out) if diff_out else 0

        # Compute deltas
        svc_write = svc_io_after.get('write_bytes', 0) - svc_io_before.get('write_bytes', 0)
        fuse_write = fuse_io_after.get('write_bytes', 0) - fuse_io_before.get('write_bytes', 0) if fuse_io_before else 0
        jvm_write = jvm_io_after.get('write_bytes', 0) - jvm_io_before.get('write_bytes', 0)
        bak_write = bak_io_after.get('write_bytes', 0) - bak_io_before.get('write_bytes', 0) if bak_io_before else 0
        net_tx = net_after - net_before

        row = {
            "http_req_size": http_req_size,
            "svc_write": svc_write, "svc_fsync": svc_total_fsync,
            "fuse_write": fuse_write,
            "jvm_write": jvm_write, "jvm_fsync": jvm_total_fsync,
            "bak_write": bak_write, "bak_fsync": bak_total_fsync,
            "net_tx": net_tx, "diff_size": diff_size,
        }
        results.append(row)

        print(f"  {i:>3}  {http_req_size:>9}  {svc_write:>10}  {svc_total_fsync:>10}  {fuse_write:>10}  "
              f"{jvm_write:>10}  {jvm_total_fsync:>10}  "
              f"{bak_write:>10}  {bak_total_fsync:>10}  "
              f"{net_tx:>10}  {diff_size:>10}")

    if not results:
        return

    # Averages
    print("  " + "-" * 120)
    avg = {k: sum(r[k] for r in results) / len(results) for k in results[0]}
    print(f"  {'avg':>3}  {avg['http_req_size']:>9.0f}  {avg['svc_write']:>10.0f}  {avg['svc_fsync']:>10.1f}  {avg['fuse_write']:>10.0f}  "
          f"{avg['jvm_write']:>10.0f}  {avg['jvm_fsync']:>10.1f}  "
          f"{avg['bak_write']:>10.0f}  {avg['bak_fsync']:>10.1f}  "
          f"{avg['net_tx']:>10.0f}  {avg['diff_size']:>10.0f}")

    print(f"\n  Legend:")
    print(f"    http_req_size   = full HTTP request size (request line + headers + body)")
    print(f"    svc_write/fsync = PostgreSQL process tree: disk writes & fsync+fdatasync+sync_file_range")
    print(f"    fuse_write      = fuselog FUSE daemon: bytes written (captures all FUSE-intercepted writes)")
    print(f"    jvm_write/fsync = GigaPaxos JVM (primary) process tree: disk writes & fsync counts")
    print(f"    bak_write/fsync = GigaPaxos JVM (backup) process tree: disk writes & fsync counts")
    print(f"    net_tx          = primary network TX bytes (Paxos proposals to backups)")
    print(f"    diff_size       = statediff size captured by fuselog")

    # Save to results dir
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    import csv
    fieldnames = ["http_req_size", "svc_write", "svc_fsync", "fuse_write",
                  "jvm_write", "jvm_fsync", "bak_write", "bak_fsync",
                  "net_tx", "diff_size"]
    csv_path = RESULTS_DIR / "sanity_check.csv"
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    print(f"\n  Saved to {csv_path}")
    print("=" * 70)


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--rates", type=str, default=None,
                   help="Comma-separated rates to override default BOTTLENECK_RATES")
    p.add_argument("--duration", type=int, default=LOAD_DURATION_SEC,
                   help=f"Measurement duration per rate point in seconds (default: {LOAD_DURATION_SEC})")
    p.add_argument("--warmup-duration", type=int, default=WARMUP_DURATION_SEC,
                   help=f"Warmup duration per rate point in seconds (default: {WARMUP_DURATION_SEC})")
    p.add_argument("--skip-setup", action="store_true",
                   help="Skip cluster setup for ALL rates (assume already running)")
    p.add_argument("--settle", type=int, default=5,
                   help="Settle period in seconds between warmup and measurement (default: 5)")
    p.add_argument("--sample-latency", action="store_true",
                   help="Enable sampled pipeline latency breakdown (PB_SAMPLE_LATENCY)")
    p.add_argument("--sanity-check", action="store_true",
                   help="Run per-request I/O sanity check before measurements")
    return p.parse_args()


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    args = parse_args()

    # Compute timestamped results directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    RESULTS_DIR = RESULTS_BASE / f"load_pb_tpcc_reflex_{timestamp}"
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    PAYLOADS_FILE = RESULTS_DIR / "neworder_payloads.txt"

    # Determine rate list and durations
    rates = [int(r) for r in args.rates.split(",")] if args.rates else BOTTLENECK_RATES
    load_duration = args.duration
    warmup_duration = args.warmup_duration

    # Inject optional JVM flags
    # --sanity-check needs PB_SAMPLE_LATENCY to capture diffSize
    if args.sample_latency or args.sanity_check:
        GP_JVM_ARGS = GP_JVM_ARGS + " -DPB_SAMPLE_LATENCY=true -DXDN_TIMING_HEADERS=true"

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

    # ── Sanity check (optional) ─────────────────────────────────────
    if args.sanity_check:
        from coordination_size_sanity_check import SanityChecker

        print("\n[Sanity Check] Setting up cluster for I/O measurement ...")
        if not args.skip_setup:
            sc_primary = setup_fresh_tpcc_cluster()
        else:
            sc_primary = detect_primary_via_docker(AR_HOSTS)
            if not sc_primary:
                sc_primary = AR_HOSTS[0]

        # Warmup a few requests to stabilize
        print("   Warming up with 10 requests ...")
        import urllib.request as _ul
        for _ in range(10):
            try:
                _req = _ul.Request(
                    f"http://{sc_primary}:{HTTP_PROXY_PORT}/orders",
                    data=open(PAYLOADS_FILE).readline().strip().encode(),
                    headers={"Content-Type": "application/json", "XDN": SERVICE_NAME},
                )
                _ul.urlopen(_req, timeout=30).read()
            except Exception:
                pass
        time.sleep(3)

        # Run sanity check using the reusable module
        sc_backups = [h for h in AR_HOSTS if h != sc_primary]
        checker = SanityChecker(
            primary=sc_primary,
            backups=sc_backups,
            port=HTTP_PROXY_PORT,
            service_name=SERVICE_NAME,
            endpoint="/orders",
            db_container_pattern="postgres",
            db_process_name="postgres",
            n_samples=10,
        )
        checker.run(payload_file=PAYLOADS_FILE, results_dir=RESULTS_DIR)

        # Tear down the sanity check cluster — real measurements start fresh
        # without the sampling overhead
        print("\n[Sanity Check] Tearing down sanity check cluster ...")
        clear_xdn_cluster()

        # Restore GP_JVM_ARGS without sampling flags for real measurements
        if not args.sample_latency:
            GP_JVM_ARGS = GP_JVM_ARGS.replace(" -DPB_SAMPLE_LATENCY=true -DXDN_TIMING_HEADERS=true", "")

    # Save effective config for reproducibility
    from gp_config_utils import save_effective_config
    save_effective_config(GP_CONFIG, GP_JVM_ARGS, RESULTS_DIR)

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
        print(f"\n   -> warmup at {WARMUP_RATE} req/s for {warmup_duration}s ...")
        warmup_result = warmup_rate_point(primary, WARMUP_RATE, warmup_duration)
        if warmup_result["throughput_rps"] < 0.1 * WARMUP_RATE:
            print(
                f"   SKIP: warmup tput={warmup_result['throughput_rps']:.2f} rps "
                f"< 10% of {WARMUP_RATE} rps -- system not ready"
            )
            log_path = copy_screen_log_for_rate(rate)
            screen_logs.append(log_path)
            continue
        settle_sec = args.settle
        print(f"   Settling {settle_sec}s after warmup ...")
        time.sleep(settle_sec)

        # ── Measurement ───────────────────────────────────────────────
        print(f"   -> rate={rate} req/s (measuring {load_duration}s) ...")
        m = run_load_point(primary, rate, load_duration)
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
        backup = RESULTS_BASE / f"load_pb_tpcc_reflex_old_{timestamp}"
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
