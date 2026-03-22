#!/usr/bin/env python3
"""
bench_standalone_bookcatalog.py — Run bookcatalog-nd standalone (no XDN)
with fuselog mounted on the state directory, then load test at given rates.

This isolates the container + SQLite + fuselog overhead from XDN's replication
pipeline, to validate whether the same execution latency degradation occurs.

Usage:
    python3 bench_standalone_bookcatalog.py [--rates 100,300,600] [--host 10.10.1.3]

The script SSHes to the target host and:
  1. Stops any existing containers
  2. Creates a fuselog-mounted directory for /app/data/
  3. Starts bookcatalog-nd container with the fuselog mount
  4. Seeds 200 books
  5. Runs load at each rate point for 60s
  6. Collects results
"""

import argparse
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import requests

# ── Config ────────────────────────────────────────────────────────────────────

DEFAULT_HOST = "10.10.1.3"
CONTAINER_NAME = "bench-bookcatalog-nd"
CONTAINER_IMAGE = "fadhilkurnia/xdn-bookcatalog-nd"
CONTAINER_PORT = 8080  # host port mapped to container's 80
STATE_DIR_HOST = "/dev/shm/bench-fuselog/state"  # fuselog mount point (on tmpfs for speed)
STATE_DIR_CONTAINER = "/app/data/"
FUSELOG_BIN = "/usr/local/bin/fuselog"
FUSELOG_SOCKET = "/tmp/bench-fuselog.sock"

RESULTS_BASE = Path(__file__).resolve().parent / "results"
GO_LATENCY_CLIENT = Path(__file__).resolve().parent / "get_latency_at_rate.go"

DEFAULT_RATES = [100, 300, 600]
LOAD_DURATION_SEC = 60
WARMUP_DURATION_SEC = 15
SEED_COUNT = 200


# ── Remote command helpers ────────────────────────────────────────────────────


def ssh_run(host, cmd, check=True, timeout=30):
    """Run a command on the remote host via SSH."""
    result = subprocess.run(
        ["ssh", host, cmd],
        capture_output=True, text=True, timeout=timeout,
    )
    if check and result.returncode != 0:
        print(f"  SSH command failed: {cmd}")
        print(f"  stdout: {result.stdout.strip()}")
        print(f"  stderr: {result.stderr.strip()}")
    return result


def ssh_run_bg(host, cmd):
    """Run a command on the remote host in background via SSH (nohup)."""
    full_cmd = f"nohup {cmd} > /dev/null 2>&1 &"
    return ssh_run(host, full_cmd, check=False)


# ── Setup / Teardown ─────────────────────────────────────────────────────────


def cleanup(host):
    """Stop container, unmount fuselog, clean up state."""
    print("  [cleanup] Stopping container ...")
    ssh_run(host, f"docker rm -f {CONTAINER_NAME} 2>/dev/null || true", check=False)
    print("  [cleanup] Unmounting fuselog ...")
    ssh_run(host, f"sudo fusermount -u {STATE_DIR_HOST} 2>/dev/null || true", check=False)
    ssh_run(host, f"sudo umount -l {STATE_DIR_HOST} 2>/dev/null || true", check=False)
    # Kill any lingering fuselog process for our mount
    ssh_run(host, f"pkill -f 'fuselog.*{STATE_DIR_HOST}' 2>/dev/null || true", check=False)
    time.sleep(1)
    print("  [cleanup] Removing state directories ...")
    ssh_run(host, f"sudo rm -rf /dev/shm/bench-fuselog {FUSELOG_SOCKET}", check=False)


def setup_fuselog(host):
    """Create directories and mount fuselog on the state directory."""
    print("  [fuselog] Creating directories ...")
    ssh_run(host, f"mkdir -p {STATE_DIR_HOST}")

    print(f"  [fuselog] Mounting fuselog on {STATE_DIR_HOST} ...")
    env_vars = (
        f"FUSELOG_SOCKET_FILE={FUSELOG_SOCKET} "
        f"FUSELOG_DAEMON_LOGS=1 "
        f"FUSELOG_COMPRESSION=true "
        f"RUST_LOG=info"
    )
    mount_cmd = f"sudo {env_vars} {FUSELOG_BIN} -o allow_other {STATE_DIR_HOST}"
    result = ssh_run(host, mount_cmd, check=False, timeout=10)
    time.sleep(2)

    # Verify mount
    result = ssh_run(host, f"mountpoint -q {STATE_DIR_HOST} && echo MOUNTED || echo NOT_MOUNTED")
    if "MOUNTED" not in result.stdout:
        print(f"  ERROR: fuselog mount failed on {STATE_DIR_HOST}")
        print(f"  Trying without sudo ...")
        mount_cmd = f"{env_vars} {FUSELOG_BIN} -o allow_other {STATE_DIR_HOST}"
        ssh_run(host, mount_cmd, check=False, timeout=10)
        time.sleep(2)
        result = ssh_run(host, f"mountpoint -q {STATE_DIR_HOST} && echo MOUNTED || echo NOT_MOUNTED")
        if "MOUNTED" not in result.stdout:
            print(f"  ERROR: fuselog mount still failed")
            # Try to check if fuse module is loaded
            ssh_run(host, "lsmod | grep fuse", check=False)
            sys.exit(1)

    print(f"  [fuselog] Mount verified: {STATE_DIR_HOST}")


def start_container(host):
    """Start bookcatalog-nd container with fuselog-mounted state directory."""
    print(f"  [container] Starting {CONTAINER_IMAGE} ...")
    docker_cmd = (
        f"docker run -d --name {CONTAINER_NAME} "
        f"-p {CONTAINER_PORT}:80 "
        f"-e ENABLE_WAL=true "
        f"-v {STATE_DIR_HOST}:{STATE_DIR_CONTAINER} "
        f"{CONTAINER_IMAGE}"
    )
    result = ssh_run(host, docker_cmd)
    if result.returncode != 0:
        print("  ERROR: Failed to start container")
        sys.exit(1)
    print(f"  [container] Started, waiting for readiness ...")

    # Wait for HTTP readiness
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            r = requests.get(f"http://{host}:{CONTAINER_PORT}/", timeout=5)
            if r.status_code == 200:
                print(f"  [container] Ready (HTTP {r.status_code})")
                return
        except Exception:
            pass
        time.sleep(2)
    print("  ERROR: Container not ready after 60s")
    sys.exit(1)


def seed_books(host):
    """Seed the bookcatalog app with test books."""
    print(f"  [seed] Seeding {SEED_COUNT} books ...")
    created = 0
    for i in range(SEED_COUNT):
        try:
            r = requests.post(
                f"http://{host}:{CONTAINER_PORT}/api/books",
                json={"title": f"Book {i+1}", "author": f"Author {i+1}"},
                headers={"Content-Type": "application/json"},
                timeout=10,
            )
            if 200 <= r.status_code < 300:
                created += 1
        except Exception as e:
            if i == 0:
                print(f"  ERROR seeding first book: {e}")
    print(f"  [seed] Seeded {created}/{SEED_COUNT} books.")
    return created


# ── Load Test ─────────────────────────────────────────────────────────────────


def parse_go_output(output_file):
    """Parse latency stats from get_latency_at_rate.go output."""
    metrics = {
        "throughput_rps": 0.0, "avg_ms": 0.0, "p50_ms": 0.0,
        "p90_ms": 0.0, "p95_ms": 0.0, "p99_ms": 0.0,
        "min_ms": 0.0, "max_ms": 0.0, "actual_achieved_rps": 0.0,
    }
    try:
        text = output_file.read_text()
        for line in text.splitlines():
            line = line.strip()
            if line.startswith("avg:"):
                metrics["avg_ms"] = float(line.split(":")[1].strip().replace("ms", ""))
            elif line.startswith("p50:") or line.startswith("median:"):
                metrics["p50_ms"] = float(line.split(":")[1].strip().replace("ms", ""))
            elif line.startswith("p90:"):
                metrics["p90_ms"] = float(line.split(":")[1].strip().replace("ms", ""))
            elif line.startswith("p95:"):
                metrics["p95_ms"] = float(line.split(":")[1].strip().replace("ms", ""))
            elif line.startswith("p99:"):
                metrics["p99_ms"] = float(line.split(":")[1].strip().replace("ms", ""))
            elif line.startswith("min:"):
                metrics["min_ms"] = float(line.split(":")[1].strip().replace("ms", ""))
            elif line.startswith("max:"):
                metrics["max_ms"] = float(line.split(":")[1].strip().replace("ms", ""))
            elif "actual_achieved_rate" in line or "actual achieved rate" in line:
                val = line.split(":")[1].strip().split()[0]
                metrics["actual_achieved_rps"] = float(val)
            elif "actual_throughput" in line or "actual throughput" in line:
                val = line.split(":")[1].strip().split()[0]
                metrics["throughput_rps"] = float(val)
    except Exception as e:
        print(f"  WARNING: Failed to parse {output_file}: {e}")
    return metrics


def run_load(host, rate, output_file, duration_sec):
    """Run get_latency_at_rate.go against the standalone container."""
    output_file.parent.mkdir(parents=True, exist_ok=True)
    url = f"http://{host}:{CONTAINER_PORT}/api/books"
    cmd = [
        "go", "run", str(GO_LATENCY_CLIENT),
        "-H", "Content-Type: application/json",
        "-X", "POST",
        url,
        '{"title":"bench book","author":"bench author"}',
        str(duration_sec),
        str(rate),
    ]
    env = os.environ.copy()
    env["GO111MODULE"] = "off"
    env["GOWORK"] = "off"
    with open(output_file, "w") as fh:
        result = subprocess.run(cmd, stdout=fh, stderr=subprocess.STDOUT, env=env, text=True)
    if result.returncode != 0:
        print(f"  WARNING: go client exited {result.returncode}")
    return parse_go_output(output_file)


# ── Main ──────────────────────────────────────────────────────────────────────


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--host", type=str, default=DEFAULT_HOST,
                   help=f"Target host (default: {DEFAULT_HOST})")
    p.add_argument("--rates", type=str, default=None,
                   help="Comma-separated rates (default: 100,300,600)")
    p.add_argument("--no-fuselog", action="store_true",
                   help="Skip fuselog mount (use plain directory)")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    host = args.host

    if args.rates:
        rates = [int(r.strip()) for r in args.rates.split(",")]
    else:
        rates = DEFAULT_RATES

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    mode = "standalone_nofuselog" if args.no_fuselog else "standalone_fuselog"
    results_dir = RESULTS_BASE / f"bookcatalog_{mode}_{timestamp}"
    results_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print(f"Standalone BookCatalog-ND Benchmark ({mode})")
    print(f"  Host     : {host}")
    print(f"  Rates    : {rates}")
    print(f"  Duration : {LOAD_DURATION_SEC}s per rate")
    print(f"  Results  -> {results_dir}")
    print("=" * 60)

    # ── Setup ──
    print("\n[Setup]")
    cleanup(host)

    if not args.no_fuselog:
        setup_fuselog(host)
    else:
        print("  [no-fuselog] Using plain directory ...")
        ssh_run(host, f"mkdir -p {STATE_DIR_HOST}")

    start_container(host)
    seed_books(host)

    # ── Run load at each rate ──
    results = []
    for i, rate in enumerate(rates):
        print(f"\n{'='*60}")
        print(f"  Rate point {i+1}/{len(rates)}: {rate} req/s")
        print(f"{'='*60}")

        # Warmup
        print(f"  -> warmup at {rate} req/s ({WARMUP_DURATION_SEC}s) ...")
        warmup_file = results_dir / f"warmup_rate{rate}.txt"
        wm = run_load(host, rate, warmup_file, WARMUP_DURATION_SEC)
        print(f"     warmup: tput={wm['throughput_rps']:.1f} rps  avg={wm['avg_ms']:.1f}ms")

        time.sleep(5)

        # Measurement
        print(f"  -> measuring at {rate} req/s ({LOAD_DURATION_SEC}s) ...")
        measure_file = results_dir / f"rate{rate}.txt"
        m = run_load(host, rate, measure_file, LOAD_DURATION_SEC)
        row = {"rate_rps": rate, **m}
        results.append(row)
        print(f"     tput={m['throughput_rps']:.1f} rps  "
              f"avg={m['avg_ms']:.1f}ms  "
              f"p50={m['p50_ms']:.1f}ms  "
              f"p90={m['p90_ms']:.1f}ms  "
              f"p95={m['p95_ms']:.1f}ms  "
              f"p99={m['p99_ms']:.1f}ms")

    # ── Cleanup ──
    print("\n[Cleanup]")
    cleanup(host)

    # ── Summary ──
    print(f"\n{'='*60}")
    print("[Summary]")
    print(f"{'='*60}")
    print(f"  {'rate':>6}  {'tput':>8}  {'avg':>8}  {'p50':>8}  {'p90':>8}  {'p95':>8}  {'p99':>8}")
    print("  " + "-" * 62)
    for row in results:
        print(f"  {row['rate_rps']:>5.0f}  "
              f"{row['throughput_rps']:>7.1f}  "
              f"{row['avg_ms']:>7.1f}ms  "
              f"{row['p50_ms']:>7.1f}ms  "
              f"{row['p90_ms']:>7.1f}ms  "
              f"{row['p95_ms']:>7.1f}ms  "
              f"{row['p99_ms']:>7.1f}ms")

    print(f"\n[Done] Results in {results_dir}/")
