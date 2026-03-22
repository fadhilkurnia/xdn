"""
run_load_pb_synth_rqlite.py — rqlite (Raft) baseline synth-workload benchmark.

Sets up a 3-node rqlite Raft cluster (using the rqlited binary), runs
synth-workload on the leader with DB_TYPE=rqlite, and runs the same 4
experiments. Each SQL transaction in the synth-workload = separate HTTP call
to rqlite = separate Raft consensus round. Each experiment point gets a
fresh cluster to prevent state carryover.

Run from the eval/ directory:
    python3 run_load_pb_synth_rqlite.py [--experiments 1,2,3,4]

Outputs go to eval/results/load_pb_synth_rqlite/{exp1_vary_txns,...}/
"""

import argparse
import concurrent.futures
import json
import os
import subprocess
import sys
import time
from pathlib import Path

from run_load_pb_bookcatalog_common import (
    AR_HOSTS,
    wait_for_port,
)
from run_load_pb_synth_common import (
    SYNTH_IMAGE,
    ensure_synth_images,
    make_workload_payload,
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
FOLLOWER_HOSTS = AR_HOSTS[1:]
ALL_HOSTS = [LEADER_HOST] + FOLLOWER_HOSTS
APP_PORT = 80  # synth-workload listens on 80 with --network host
RQLITE_HTTP_PORT = 4001
RQLITE_RAFT_PORT = 4002
RQLITED_BIN = "rqlited"
DATA_DIR_PREFIX = "/tmp"

RESULTS_BASE = Path(__file__).resolve().parent / "results" / "load_pb_synth_rqlite"
SCRIPT_DIR = Path(__file__).resolve().parent

# Experiment parameters (same as PB benchmark)
EXP1_TXNS_VALUES = [1, 2, 5, 10, 20]
EXP2_OPS_VALUES = [1, 5, 10, 20, 50]
EXP3_WRITESIZE_VALUES = [100, 1000, 10000, 100000]
EXP_FIXED_RATE = 100
EXP_DURATION_SEC = 60
EXP4_TXNS = 5
EXP4_OPS = 5
EXP4_WRITESIZE = 1000


# ── Shell helpers ─────────────────────────────────────────────────────────────


def _ssh(host, cmd, check=True, capture=False):
    return subprocess.run(
        ["ssh", "-o", "StrictHostKeyChecking=no", host, cmd],
        check=check, capture_output=capture, text=True,
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
    """Kill any leftover rqlite processes and synth-workload containers on all nodes."""
    print(f"   Tearing down on {ALL_HOSTS} ...")

    def teardown_node(host):
        _ssh(host,
             "pkill -f rqlited >/dev/null 2>&1 || true; "
             "sudo fuser -k 4001/tcp >/dev/null 2>&1 || true; "
             "sudo fuser -k 4002/tcp >/dev/null 2>&1 || true; "
             "sudo fuser -k 4003/tcp >/dev/null 2>&1 || true; "
             "sudo fuser -k 4004/tcp >/dev/null 2>&1 || true; "
             "sudo rm -rf /tmp/rqlite_data_* || true; "
             "docker rm -f synth-workload 2>/dev/null || true",
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
    print(f"   WARNING: rqlite cluster may not be converged after {timeout_sec}s")
    return False


# ── App management ────────────────────────────────────────────────────────────


def start_synth_app():
    """Start synth-workload on LEADER_HOST with DB_TYPE=rqlite."""
    print(f"   Starting synth-workload on {LEADER_HOST}:{APP_PORT} with rqlite backend ...")
    _ssh(LEADER_HOST, "docker rm -f synth-workload 2>/dev/null || true", check=False)
    cmd = (
        f"docker run -d --name synth-workload "
        f"--network host "
        f"-e DB_TYPE=rqlite "
        f"-e RQLITE_URL=http://{LEADER_HOST}:{RQLITE_HTTP_PORT} "
        f"{SYNTH_IMAGE}"
    )
    _ssh(LEADER_HOST, cmd)
    print(f"   Waiting for app port {LEADER_HOST}:{APP_PORT} ...")
    if not wait_for_port(LEADER_HOST, APP_PORT, timeout_sec=120):
        print("ERROR: synth-workload did not start in time.")
        result = _ssh(LEADER_HOST, "docker logs synth-workload --tail 30",
                      check=False, capture=True)
        print(f"   App log:\n{result.stdout}")
        sys.exit(1)
    time.sleep(5)


def stop_synth_app():
    """Stop synth-workload container on LEADER_HOST."""
    _ssh(LEADER_HOST, "docker rm -f synth-workload 2>/dev/null || true", check=False)


# ── Fresh cluster setup/teardown wrapper ─────────────────────────────────────


def setup_fresh_cluster():
    """Teardown everything, start fresh rqlite + app. Returns rqlite processes."""
    teardown_all()
    processes = start_rqlite_cluster()
    wait_for_rqlite_ready(processes)
    wait_for_rqlite_cluster()
    start_synth_app()
    return processes


def cleanup_cluster(rqlite_processes):
    """Stop app and rqlite cluster."""
    stop_synth_app()
    stop_rqlite_cluster(rqlite_processes)


# ── Load test helpers ────────────────────────────────────────────────────────


def _run_load(url, rate, payload, output_file, duration_sec):
    output_file.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "go", "run", str(GO_LATENCY_CLIENT),
        "-H", "Content-Type: application/json",
        "-X", "POST",
        url,
        payload,
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


def run_experiment_point(url, rate, payload, output_file, duration_sec):
    m = _run_load(url, rate, payload, output_file, duration_sec)
    print(
        f"     tput={m['throughput_rps']:.2f} rps  "
        f"avg={m['avg_ms']:.1f}ms  "
        f"p50={m['p50_ms']:.1f}ms  "
        f"p95={m['p95_ms']:.1f}ms"
    )
    return m


def warmup_point(url, rate, payload, output_file):
    m = _run_load(url, rate, payload, output_file, WARMUP_DURATION_SEC)
    print(f"     [warmup] tput={m['throughput_rps']:.2f} rps  avg={m['avg_ms']:.1f}ms")
    return m


# ── Experiments ──────────────────────────────────────────────────────────────


def run_exp1(app_url):
    print("\n" + "=" * 60)
    print("Experiment 1: Vary txns/request (ops=1, write_size=100)")
    print("  (fresh cluster per parameter point)")
    print("=" * 60)
    exp_dir = RESULTS_BASE / "exp1_vary_txns"
    results = []
    for i, txns in enumerate(EXP1_TXNS_VALUES):
        print(f"\n   --- Point {i+1}/{len(EXP1_TXNS_VALUES)}: txns={txns} ---")
        rqlite_processes = []
        try:
            rqlite_processes = setup_fresh_cluster()
            payload = make_workload_payload(txns=txns, ops=1, write_size=100)
            print(f"\n   -> txns={txns} at {EXP_FIXED_RATE} rps ...")
            warmup_point(app_url, EXP_FIXED_RATE, payload, exp_dir / f"warmup_txns{txns}.txt")
            time.sleep(5)
            m = run_experiment_point(app_url, EXP_FIXED_RATE, payload,
                                     exp_dir / f"txns{txns}.txt", EXP_DURATION_SEC)
            results.append({"txns": txns, **m})
        finally:
            cleanup_cluster(rqlite_processes)
    return results


def run_exp2(app_url):
    print("\n" + "=" * 60)
    print("Experiment 2: Vary ops/txn (txns=1, write_size=100)")
    print("  (fresh cluster per parameter point)")
    print("=" * 60)
    exp_dir = RESULTS_BASE / "exp2_vary_ops"
    results = []
    for i, ops in enumerate(EXP2_OPS_VALUES):
        print(f"\n   --- Point {i+1}/{len(EXP2_OPS_VALUES)}: ops={ops} ---")
        rqlite_processes = []
        try:
            rqlite_processes = setup_fresh_cluster()
            payload = make_workload_payload(txns=1, ops=ops, write_size=100)
            print(f"\n   -> ops={ops} at {EXP_FIXED_RATE} rps ...")
            warmup_point(app_url, EXP_FIXED_RATE, payload, exp_dir / f"warmup_ops{ops}.txt")
            time.sleep(5)
            m = run_experiment_point(app_url, EXP_FIXED_RATE, payload,
                                     exp_dir / f"ops{ops}.txt", EXP_DURATION_SEC)
            results.append({"ops": ops, **m})
        finally:
            cleanup_cluster(rqlite_processes)
    return results


def run_exp3(app_url):
    print("\n" + "=" * 60)
    print("Experiment 3: Vary write_size (txns=1, ops=1)")
    print("  (fresh cluster per parameter point)")
    print("=" * 60)
    exp_dir = RESULTS_BASE / "exp3_vary_writesize"
    results = []
    for i, ws in enumerate(EXP3_WRITESIZE_VALUES):
        print(f"\n   --- Point {i+1}/{len(EXP3_WRITESIZE_VALUES)}: write_size={ws} ---")
        rqlite_processes = []
        try:
            rqlite_processes = setup_fresh_cluster()
            payload = make_workload_payload(txns=1, ops=1, write_size=ws)
            print(f"\n   -> write_size={ws} at {EXP_FIXED_RATE} rps ...")
            warmup_point(app_url, EXP_FIXED_RATE, payload, exp_dir / f"warmup_write_size{ws}.txt")
            time.sleep(5)
            m = run_experiment_point(app_url, EXP_FIXED_RATE, payload,
                                     exp_dir / f"write_size{ws}.txt", EXP_DURATION_SEC)
            results.append({"write_size": ws, **m})
        finally:
            cleanup_cluster(rqlite_processes)
    return results


def run_exp4(app_url, rates):
    print("\n" + "=" * 60)
    print(f"Experiment 4: Rate sweep (txns={EXP4_TXNS}, ops={EXP4_OPS}, write_size={EXP4_WRITESIZE})")
    print("  (fresh cluster per rate point)")
    print("=" * 60)
    exp_dir = RESULTS_BASE / "exp4_combined"
    payload = make_workload_payload(txns=EXP4_TXNS, ops=EXP4_OPS, write_size=EXP4_WRITESIZE)
    results = []

    for i, rate in enumerate(rates):
        print(f"\n   --- Rate point {i+1}/{len(rates)}: {rate} req/s ---")
        rqlite_processes = []
        try:
            rqlite_processes = setup_fresh_cluster()
            print(f"\n   -> rate={rate} req/s (warmup) ...")
            wm = warmup_point(app_url, rate, payload, exp_dir / f"warmup_rate{rate}.txt")
            if wm["throughput_rps"] < 0.1 * rate:
                print(f"   SKIP: warmup tput too low — saturated")
                continue
            time.sleep(5)
            print(f"   -> rate={rate} req/s (measuring {LOAD_DURATION_SEC}s) ...")
            m = run_experiment_point(app_url, rate, payload,
                                     exp_dir / f"rate{rate}.txt", LOAD_DURATION_SEC)
            results.append({"rate_rps": rate, **m})
            if m["avg_ms"] > AVG_LATENCY_THRESHOLD_MS:
                print(f"   Saturated, stopping.")
                break
            if m["actual_achieved_rps"] > 0 and m["throughput_rps"] < 0.8 * m["actual_achieved_rps"]:
                print(f"   Saturated (throughput < 80% offered), stopping.")
                break
        finally:
            cleanup_cluster(rqlite_processes)
    return results


# ── Main ──────────────────────────────────────────────────────────────────────


def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--experiments", type=str, default="1,2,3,4",
                   help="Comma-separated experiment numbers (default: 1,2,3,4)")
    p.add_argument("--rates", type=str, default=None,
                   help="Override rate list for exp4")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    experiments = [int(e.strip()) for e in args.experiments.split(",")]

    print("=" * 60)
    print("Synth-Workload rqlite (Raft) Baseline Benchmark")
    print("  (fresh cluster per point, rqlited binary)")
    print("=" * 60)

    # Ensure images are present
    print("\nEnsuring Docker images ...")
    ensure_synth_images(ALL_HOSTS)

    app_url = f"http://{LEADER_HOST}:{APP_PORT}/workload"
    print(f"\n   App URL: {app_url}")

    # Run experiments
    if 1 in experiments:
        run_exp1(app_url)
    if 2 in experiments:
        run_exp2(app_url)
    if 3 in experiments:
        run_exp3(app_url)
    if 4 in experiments:
        rates = [int(r.strip()) for r in args.rates.split(",")] if args.rates else BOTTLENECK_RATES
        run_exp4(app_url, rates)

    # Generate plots
    print("\nGenerating plots ...")
    r = subprocess.run(
        ["python3", "plot_synth_comparison.py",
         "--results-dir", str(RESULTS_BASE),
         "--variant", "rqlite"],
        capture_output=True, text=True, cwd=str(SCRIPT_DIR),
    )
    print(r.stdout)
    if r.returncode != 0:
        print(f"   WARNING: plot_synth_comparison.py failed:\n{r.stderr}")

    print(f"\n[Done] Results in {RESULTS_BASE}/")
