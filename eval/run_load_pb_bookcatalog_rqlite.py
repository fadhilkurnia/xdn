"""
run_load_pb_bookcatalog_rqlite.py — rqlite (Raft) baseline bookcatalog-nd benchmark.

Sets up a 3-node rqlite Raft cluster (using the rqlited binary) across CloudLab
nodes, runs bookcatalog-nd on the leader node with DB_TYPE=rqlite, and benchmarks
POST /api/books at increasing rates.  Each rate point gets a fresh cluster to
prevent state carryover.

Run from the eval/ directory:
    python3 run_load_pb_bookcatalog_rqlite.py [--rates 100,200,300]

Outputs go to eval/results/load_pb_bookcatalog_rqlite_<timestamp>/:
    rate100.txt  rate200.txt  ...  — go-client output per rate point
"""

import argparse
import concurrent.futures
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from run_load_pb_bookcatalog_common import (
    AR_HOSTS,
    BOOKCATALOG_IMAGE,
    check_app_ready,
    seed_books,
    wait_for_app_ready,
    wait_for_port,
)
from run_load_pb_wordpress_reflex import (
    AVG_LATENCY_THRESHOLD_MS,
    BOTTLENECK_RATES,
    GO_LATENCY_CLIENT,
    LOAD_DURATION_SEC,
    WARMUP_DURATION_SEC,
    _parse_go_output,
)

# ── Constants ─────────────────────────────────────────────────────────────────

LEADER_HOST = AR_HOSTS[0]  # 10.10.1.1
FOLLOWER_HOSTS = AR_HOSTS[1:]  # 10.10.1.2, 10.10.1.3
ALL_HOSTS = [LEADER_HOST] + FOLLOWER_HOSTS
APP_PORT = 80  # bookcatalog listens on port 80 with --network host
RQLITE_HTTP_PORT = 4001
RQLITE_RAFT_PORT = 4002
RQLITED_BIN = "rqlited"
DATA_DIR_PREFIX = "/tmp"

_TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
RESULTS_DIR = Path(__file__).resolve().parent / "results" / f"load_pb_bookcatalog_rqlite_{_TIMESTAMP}"
SCRIPT_DIR = Path(__file__).resolve().parent


# ── Shell helpers ─────────────────────────────────────────────────────────────


def _ssh(host, cmd, check=True, capture=False):
    return subprocess.run(
        ["ssh", "-o", "StrictHostKeyChecking=no", host, cmd],
        check=check,
        capture_output=capture,
        text=True,
    )


# ── rqlite cluster management (binary-based) ─────────────────────────────────


def start_rqlite_cluster():
    """Start a 3-node rqlite cluster using the rqlited binary."""
    print(f"   Starting rqlite cluster on {ALL_HOSTS} ...")
    leader_raft_addr = f"{LEADER_HOST}:{RQLITE_RAFT_PORT}"
    processes = []
    for idx, host in enumerate(ALL_HOSTS):
        node_id = idx + 1
        http_port = RQLITE_HTTP_PORT if idx == 0 else RQLITE_HTTP_PORT + (idx * 2)
        raft_port = RQLITE_RAFT_PORT if idx == 0 else RQLITE_RAFT_PORT + (idx * 2)
        data_dir = f"{DATA_DIR_PREFIX}/rqlite_data_{node_id}"
        log_file = f"{data_dir}/rqlite.log"
        _ssh(host, f"sudo rm -rf {data_dir}", check=False)
        cmd = (
            f"(sudo fuser -k {http_port}/tcp || true) && "
            f"(sudo fuser -k {raft_port}/tcp || true) && "
            f"sudo mkdir -p {data_dir} && "
            f"sudo chown -R $(id -un):$(id -gn) {data_dir} && "
            f"( nohup setsid {RQLITED_BIN} -node-id={node_id} "
            f"-http-addr={host}:{http_port} -raft-addr={host}:{raft_port} "
        )
        if idx > 0:
            cmd += f"-join={leader_raft_addr} "
        cmd += f"{data_dir} </dev/null >{log_file} 2>&1 & ) && echo $!"
        print(f"   Starting rqlite node {node_id} on {host} (http={http_port}, raft={raft_port})")
        result = _ssh(host, cmd, capture=True)
        pid = result.stdout.strip() if result.stdout else ""
        processes.append({
            "host": host, "pid": pid, "data_dir": data_dir,
            "http_port": http_port, "raft_port": raft_port,
        })
        time.sleep(1.5)
    print("   Waiting for rqlite nodes to stabilize ...")
    time.sleep(3)
    return processes


def stop_rqlite_cluster(processes):
    """Stop rqlite processes and clean up data directories."""
    for proc in processes:
        host = proc["host"]
        http_port = proc.get("http_port")
        raft_port = proc.get("raft_port")
        data_dir = proc.get("data_dir")
        cleanup_cmds = []
        if http_port:
            cleanup_cmds.append(f"sudo fuser -k {http_port}/tcp >/dev/null 2>&1 || true")
        if raft_port:
            cleanup_cmds.append(f"sudo fuser -k {raft_port}/tcp >/dev/null 2>&1 || true")
        cleanup_cmds.append("pkill -f rqlited >/dev/null 2>&1 || true")
        if data_dir:
            cleanup_cmds.append(f"sudo rm -rf {data_dir}")
        _ssh(host, " ; ".join(cleanup_cmds), check=False)


def teardown_all():
    """Kill any leftover rqlite processes and bookcatalog containers on all nodes."""
    print(f"   Tearing down on {ALL_HOSTS} ...")

    def teardown_node(host):
        _ssh(host,
             "pkill -f rqlited >/dev/null 2>&1 || true; "
             "sudo fuser -k 4001/tcp >/dev/null 2>&1 || true; "
             "sudo fuser -k 4002/tcp >/dev/null 2>&1 || true; "
             "sudo fuser -k 4003/tcp >/dev/null 2>&1 || true; "
             "sudo fuser -k 4004/tcp >/dev/null 2>&1 || true; "
             "sudo rm -rf /tmp/rqlite_data_* || true; "
             "docker rm -f bookcatalog-nd 2>/dev/null || true",
             check=False)

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        futs = [pool.submit(teardown_node, h) for h in ALL_HOSTS]
        for f in futs:
            f.result()


def wait_for_rqlite_ready(processes, timeout_sec=60):
    """Wait for all rqlite nodes to be reachable."""
    for proc in processes:
        host = proc["host"]
        http_port = proc["http_port"]
        print(f"   Waiting for rqlite on {host}:{http_port} ...")
        if not wait_for_port(host, http_port, timeout_sec=timeout_sec):
            raise RuntimeError(f"rqlite on {host}:{http_port} did not start in time.")


def wait_for_rqlite_cluster(timeout_sec=60):
    """Wait for rqlite cluster to have 3 voting nodes."""
    print("   Waiting for rqlite cluster convergence ...")
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        result = _ssh(
            LEADER_HOST,
            f"curl -s http://{LEADER_HOST}:{RQLITE_HTTP_PORT}/status",
            check=False, capture=True,
        )
        if result.returncode == 0 and result.stdout:
            try:
                status = json.loads(result.stdout)
                nodes = status.get("store", {}).get("raft", {}).get("num_peers", -1)
                if nodes == 2:  # 2 peers + self = 3 nodes
                    print("   rqlite cluster ready (3 nodes)")
                    return True
            except (json.JSONDecodeError, KeyError):
                pass
        time.sleep(3)
    print(f"   WARNING: rqlite cluster may not be fully converged after {timeout_sec}s")
    return False


# ── App management ────────────────────────────────────────────────────────────


def start_bookcatalog_app():
    """Start bookcatalog-nd on LEADER_HOST with DB_TYPE=rqlite."""
    print(f"   Starting bookcatalog-nd on {LEADER_HOST}:{APP_PORT} with rqlite backend ...")
    _ssh(LEADER_HOST, "docker rm -f bookcatalog-nd 2>/dev/null || true", check=False)
    cmd = (
        f"docker run -d --name bookcatalog-nd "
        f"--network host "
        f"-e DB_TYPE=rqlite "
        f"-e DB_HOST={LEADER_HOST} "
        f"{BOOKCATALOG_IMAGE}"
    )
    _ssh(LEADER_HOST, cmd)
    print(f"   Waiting for app port {LEADER_HOST}:{APP_PORT} ...")
    if not wait_for_port(LEADER_HOST, APP_PORT, timeout_sec=120):
        print("ERROR: bookcatalog-nd did not start in time.")
        result = _ssh(LEADER_HOST, "docker logs bookcatalog-nd --tail 30", check=False, capture=True)
        print(f"   App log:\n{result.stdout}")
        sys.exit(1)
    time.sleep(5)


def stop_bookcatalog_app():
    """Stop bookcatalog-nd container on LEADER_HOST."""
    _ssh(LEADER_HOST, "docker rm -f bookcatalog-nd 2>/dev/null || true", check=False)


# ── Load test helpers ────────────────────────────────────────────────────────


def _run_load(url, rate, output_file, duration_sec):
    output_file.parent.mkdir(parents=True, exist_ok=True)
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
    print(f"   Output -> {output_file}")
    with open(output_file, "w") as fh:
        result = subprocess.run(cmd, stdout=fh, stderr=subprocess.STDOUT,
                                env=env, text=True)
    if result.returncode != 0:
        print(f"   WARNING: go client exited {result.returncode}")
    return _parse_go_output(output_file)


def run_load_point(url, rate):
    return _run_load(url, rate, RESULTS_DIR / f"rate{rate}.txt", LOAD_DURATION_SEC)


def warmup_rate_point(url, rate):
    m = _run_load(url, rate, RESULTS_DIR / f"warmup_rate{rate}.txt", WARMUP_DURATION_SEC)
    print(f"     [warmup] tput={m['throughput_rps']:.2f} rps  avg={m['avg_ms']:.1f}ms")
    return m


# ── Main ──────────────────────────────────────────────────────────────────────


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--rates", type=str, default=None,
                   help="Comma-separated list of offered rates (req/s) to override default")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()

    print("=" * 60)
    print("BookCatalog-ND rqlite (Raft) Baseline Benchmark")
    print("  (fresh cluster per rate point, rqlited binary)")
    print("=" * 60)

    rates = [int(r.strip()) for r in args.rates.split(",")] if args.rates else BOTTLENECK_RATES
    app_url = f"http://{LEADER_HOST}:{APP_PORT}/api/books"
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    print(f"\n   URL      : {app_url}")
    print(f"   Rates    : {rates}")
    print(f"   Duration : {LOAD_DURATION_SEC}s per rate (warmup {WARMUP_DURATION_SEC}s)")

    results = []

    for i, rate in enumerate(rates):
        print(f"\n{'=' * 60}")
        print(f"  Rate point {i + 1}/{len(rates)}: {rate} req/s (fresh cluster)")
        print(f"{'=' * 60}")

        rqlite_processes = []

        try:
            # ── Tear down previous cluster ────────────────────────────
            print("\n   [1] Tearing down ...")
            teardown_all()

            # ── Start fresh rqlite cluster ────────────────────────────
            print("\n   [2] Starting rqlite cluster ...")
            rqlite_processes = start_rqlite_cluster()
            wait_for_rqlite_ready(rqlite_processes)
            wait_for_rqlite_cluster()

            # ── Start fresh app ───────────────────────────────────────
            print("\n   [3] Starting bookcatalog-nd app ...")
            start_bookcatalog_app()

            # ── Seed books ────────────────────────────────────────────
            print("\n   [4] Seeding books ...")
            if not wait_for_app_ready(LEADER_HOST, APP_PORT, timeout_sec=120):
                print("ERROR: App not ready.")
                continue
            seeded = False
            for attempt in range(1, 4):
                created = seed_books(LEADER_HOST, APP_PORT, count=200)
                if created > 0:
                    seeded = True
                    break
                print(f"   Attempt {attempt} failed, retrying in 10s ...")
                time.sleep(10)
            if not seeded:
                print("ERROR: Failed to seed books after 3 attempts, skipping rate.")
                continue

            if not check_app_ready(LEADER_HOST, APP_PORT):
                print("ERROR: App not ready after seeding, skipping rate.")
                continue

            # ── Warmup ────────────────────────────────────────────────
            print(f"\n   [5] Warmup ({WARMUP_DURATION_SEC}s at {rate} req/s) ...")
            wm = warmup_rate_point(app_url, rate)
            if wm["throughput_rps"] < 0.1 * rate:
                print(
                    f"   SKIP: warmup tput={wm['throughput_rps']:.2f} rps "
                    f"< 10% of {rate} rps — system saturated"
                )
                continue
            print("   Settling 5s after warmup ...")
            time.sleep(5)

            # ── Measurement ───────────────────────────────────────────
            print(f"\n   [6] Measuring ({LOAD_DURATION_SEC}s at {rate} req/s) ...")
            m = run_load_point(app_url, rate)
            row = {"rate_rps": rate, **m}
            results.append(row)
            print(
                f"     tput={m['throughput_rps']:.2f} rps  "
                f"avg={m['avg_ms']:.1f}ms  "
                f"p50={m['p50_ms']:.1f}ms  "
                f"p95={m['p95_ms']:.1f}ms"
            )

            # ── Early stop on saturation (both conditions must be true) ──
            avg_exceeded = m["avg_ms"] > AVG_LATENCY_THRESHOLD_MS
            tput_dropped = m["actual_achieved_rps"] > 0 and m["throughput_rps"] < 0.75 * m["actual_achieved_rps"]
            if avg_exceeded and tput_dropped:
                print(
                    f"   System saturated: avg {m['avg_ms']:.0f}ms > {AVG_LATENCY_THRESHOLD_MS}ms AND "
                    f"throughput {m['throughput_rps']:.2f} < 75% of {m['actual_achieved_rps']:.2f}; stopping."
                )
                break
            if avg_exceeded:
                print(f"   Avg latency {m['avg_ms']:.0f}ms > {AVG_LATENCY_THRESHOLD_MS}ms (throughput OK); continuing.")
            if tput_dropped:
                print(f"   Throughput {m['throughput_rps']:.2f} < 75% of {m['actual_achieved_rps']:.2f} (latency OK); continuing.")
        finally:
            # Always clean up this rate's cluster
            stop_bookcatalog_app()
            stop_rqlite_cluster(rqlite_processes)

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'=' * 60}")
    print("Summary")
    print(f"{'=' * 60}")
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

    # ── Plot ──────────────────────────────────────────────────────────────────
    print("\nGenerating plots ...")
    r = subprocess.run(
        ["python3", "plot_load_latency.py", "--results-dir", str(RESULTS_DIR)],
        capture_output=True, text=True, cwd=str(SCRIPT_DIR),
    )
    print(r.stdout)
    if r.returncode != 0:
        print(f"   WARNING: plot_load_latency.py failed:\n{r.stderr}")

    print(f"\n[Done] Results in {RESULTS_DIR}/")
