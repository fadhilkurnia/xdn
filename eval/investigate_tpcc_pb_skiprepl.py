"""
Experiment B: TPC-C PB_SKIP_REPLICATION — execution pipeline only.

Workers + doneQueue + capture thread run, but captureStateDiff and Paxos
propose are skipped. This isolates the worker → doneQueue → callback path.

Run from the eval/ directory:
    python3 investigate_tpcc_pb_skiprepl.py
"""

import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import run_load_pb_tpcc_reflex as base

RESULTS_BASE = base.RESULTS_BASE
EXPERIMENT_RATES = [1, 60, 80, 100, 120, 140, 160, 200, 250, 300, 400, 500]

# Override GP_JVM_ARGS to add PB_SKIP_REPLICATION
GP_JVM_ARGS_SKIPREPL = (
    "-DPB_N_PARALLEL_WORKERS=32 "
    "-DPB_CAPTURE_ACCUMULATION_MS=5 "
    "-DPB_SKIP_REPLICATION=true"
)
SCREEN_SESSION = "xdn_tpcc_pb_skiprepl"
SCREEN_LOG = f"screen_logs/{SCREEN_SESSION}.log"


def start_cluster_skiprepl():
    """Start XDN cluster with PB_SKIP_REPLICATION=true."""
    os.makedirs("screen_logs", exist_ok=True)
    os.system(f"rm -f {SCREEN_LOG}")
    cmd = (
        f"screen -L -Logfile {SCREEN_LOG} -S {SCREEN_SESSION} -d -m bash -c "
        f"'../bin/gpServer.sh -DgigapaxosConfig={base.GP_CONFIG} {GP_JVM_ARGS_SKIPREPL} start all; exec bash'"
    )
    print(f"   {cmd}")
    ret = os.system(cmd)
    assert ret == 0, "Failed to start cluster"
    print(f"   Screen log: {SCREEN_LOG}")
    time.sleep(20)


def setup_fresh_cluster():
    """Like base.setup_fresh_tpcc_cluster but uses PB_SKIP_REPLICATION.

    NOTE: With SKIP_REPLICATION, Paxos pipeline drain is not meaningful,
    but we still run the probes to ensure the container is warmed up.
    """
    os.system(f"screen -S {SCREEN_SESSION} -X quit 2>/dev/null || true")
    os.system(f"screen -S {base.SCREEN_SESSION} -X quit 2>/dev/null || true")

    print("   [setup] Force-clearing cluster ...")
    base.clear_xdn_cluster()

    print("   [setup] Starting XDN cluster (SKIP_REPLICATION) ...")
    start_cluster_skiprepl()

    print("   [setup] Waiting for Active Replicas ...")
    for host in base.AR_HOSTS:
        if not base.wait_for_port(host, base.HTTP_PROXY_PORT, base.TIMEOUT_PORT_SEC):
            print(f"ERROR: AR at {host}:{base.HTTP_PROXY_PORT} not ready.")
            sys.exit(1)

    print("   [setup] Ensuring Docker images on RC ...")
    base.ensure_docker_images_on_rc()

    print("   [setup] Launching TPC-C service ...")
    cmd = (
        f"XDN_CONTROL_PLANE={base.CONTROL_PLANE_HOST} {base.XDN_BINARY} "
        f"launch {base.SERVICE_NAME} --file={base.TPCC_YAML}"
    )
    print(f"   {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    print(result.stdout.strip())
    if result.returncode != 0:
        print(f"ERROR: xdn launch failed:\n{result.stderr}")
        sys.exit(1)

    print("   [setup] Waiting for TPC-C HTTP readiness ...")
    ok, _ = base.wait_for_service(
        base.AR_HOSTS, base.HTTP_PROXY_PORT, base.SERVICE_NAME, base.TIMEOUT_SERVICE_SEC
    )
    if not ok:
        print(f"ERROR: TPC-C not ready after {base.TIMEOUT_SERVICE_SEC}s.")
        sys.exit(1)

    print("   [setup] Detecting primary ...")
    primary = base.detect_primary_via_docker(base.AR_HOSTS)
    if not primary:
        primary = base.AR_HOSTS[0]
    print(f"   [setup] Primary: {primary}")

    print("   [setup] Initializing TPC-C database ...")
    if not base.init_tpcc_db(primary, base.HTTP_PROXY_PORT, base.SERVICE_NAME, base.NUM_WAREHOUSES):
        print("ERROR: TPC-C database initialization failed.")
        sys.exit(1)

    print("   [setup] Verifying TPC-C health ...")
    if not base.check_tpcc_health(primary, base.HTTP_PROXY_PORT, base.SERVICE_NAME):
        print("ERROR: TPC-C health check failed.")
        sys.exit(1)

    # With SKIP_REPLICATION, no Paxos pipeline to drain, but let container settle.
    print("   [setup] Settling 10s ...")
    time.sleep(10)

    return primary


if __name__ == "__main__":
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_dir = RESULTS_BASE / f"tpcc_pb_skiprepl_{timestamp}"
    results_dir.mkdir(parents=True, exist_ok=True)
    payloads_file = results_dir / "neworder_payloads.txt"

    print("=" * 60)
    print("Experiment B: TPC-C PB_SKIP_REPLICATION (pipeline only)")
    print(f"  JVM args : {GP_JVM_ARGS_SKIPREPL}")
    print(f"  Rates    : {EXPERIMENT_RATES}")
    print(f"  Results  -> {results_dir}")
    print("=" * 60)

    base.generate_payloads_file(payloads_file, base.NUM_WAREHOUSES)

    # Point base module to our results dir so run_load_point writes there
    base.RESULTS_DIR = results_dir
    base.PAYLOADS_FILE = payloads_file
    base.SCREEN_SESSION = SCREEN_SESSION
    base.SCREEN_LOG = SCREEN_LOG

    results = []
    screen_logs = []

    for i, rate in enumerate(EXPERIMENT_RATES):
        print(f"\n{'='*60}")
        print(f"  Rate point {i+1}/{len(EXPERIMENT_RATES)}: {rate} req/s (SKIP_REPLICATION)")
        print(f"{'='*60}")

        primary = setup_fresh_cluster()

        # Warmup
        print(f"\n   -> rate={rate} req/s (warmup {base.WARMUP_DURATION_SEC}s) ...")
        base.warmup_rate_point(primary, rate)
        print(f"   Settling 5s after warmup ...")
        time.sleep(5)

        # Measurement
        print(f"   -> rate={rate} req/s (measuring {base.LOAD_DURATION_SEC}s) ...")
        m = base.run_load_point(primary, rate)
        row = {"rate_rps": rate, **m}
        results.append(row)
        print(
            f"     tput={m['throughput_rps']:.2f} rps  "
            f"avg={m['avg_ms']:.1f}ms  "
            f"p50={m['p50_ms']:.1f}ms  "
            f"p95={m['p95_ms']:.1f}ms"
        )

        # Per-rate log
        dest = results_dir / f"screen_rate{rate}.log"
        os.system(f"cp {SCREEN_LOG} {dest} 2>/dev/null || true")
        screen_logs.append(dest)

        if m["avg_ms"] > base.AVG_LATENCY_THRESHOLD_MS and (
            m["actual_achieved_rps"] > 0
            and m["throughput_rps"] < 0.75 * m["actual_achieved_rps"]
        ):
            print(f"   Saturated — stopping early.")
            break

    # Symlink
    symlink = RESULTS_BASE / "tpcc_pb_skiprepl"
    if symlink.is_symlink():
        symlink.unlink()
    elif symlink.is_dir():
        symlink.rename(RESULTS_BASE / f"tpcc_pb_skiprepl_old_{timestamp}")
    symlink.symlink_to(results_dir.name)

    # Summary
    print("\n[Summary — SKIP_REPLICATION]")
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
    print(f"\n[Done] Results in {results_dir}/")
