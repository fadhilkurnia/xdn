"""
Experiment C: TPC-C vary CAPTURE_ACCUMULATION_MS.

Tests the effect of batching window on throughput by sweeping
CAPTURE_ACCUMULATION_MS across {5, 20, 50, 100}ms.

Hypothesis: larger windows → more requests per captureStateDiff → higher
throughput, IF captureStateDiff time grows sub-linearly with batch size.

Run from the eval/ directory:
    python3 investigate_tpcc_pb_accum.py
"""

import argparse
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import run_load_pb_tpcc_reflex as base

RESULTS_BASE = base.RESULTS_BASE
EXPERIMENT_RATES = [1, 60, 80, 100, 120, 140, 160, 200, 250, 300]
ACCUM_VALUES = [5, 20, 50, 100]


def start_cluster_with_accum(accum_ms):
    """Start XDN cluster with a specific CAPTURE_ACCUMULATION_MS."""
    jvm_args = f"-DPB_N_PARALLEL_WORKERS=32 -DPB_CAPTURE_ACCUMULATION_MS={accum_ms}"
    screen_session = f"xdn_tpcc_pb_accum{accum_ms}"
    screen_log = f"screen_logs/{screen_session}.log"

    os.makedirs("screen_logs", exist_ok=True)
    os.system(f"rm -f {screen_log}")
    cmd = (
        f"screen -L -Logfile {screen_log} -S {screen_session} -d -m bash -c "
        f"'../bin/gpServer.sh -DgigapaxosConfig={base.GP_CONFIG} {jvm_args} start all; exec bash'"
    )
    print(f"   {cmd}")
    ret = os.system(cmd)
    assert ret == 0, "Failed to start cluster"
    print(f"   Screen log: {screen_log}")
    time.sleep(20)
    return screen_session, screen_log


def setup_fresh_cluster(accum_ms):
    """Fresh cluster setup with specific CAPTURE_ACCUMULATION_MS."""
    screen_session = f"xdn_tpcc_pb_accum{accum_ms}"
    os.system(f"screen -S {screen_session} -X quit 2>/dev/null || true")
    os.system(f"screen -S {base.SCREEN_SESSION} -X quit 2>/dev/null || true")

    print("   [setup] Force-clearing cluster ...")
    base.clear_xdn_cluster()

    print(f"   [setup] Starting XDN cluster (ACCUM={accum_ms}ms) ...")
    screen_session, screen_log = start_cluster_with_accum(accum_ms)

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

    # Wait for Paxos pipeline to drain (same as base)
    import requests as req_lib
    print(f"   [setup] Waiting for Paxos pipeline to drain ...")
    max_settle_sec = 300
    settle_deadline = time.time() + max_settle_sec
    CONSECUTIVE_OK = 10
    consecutive_ok = 0
    settled = False
    while time.time() < settle_deadline:
        try:
            t0 = time.time()
            r = req_lib.post(
                f"http://{primary}:{base.HTTP_PROXY_PORT}/orders",
                headers={"XDN": base.SERVICE_NAME, "Content-Type": "application/json"},
                json={"w_id": 1, "c_id": 1},
                timeout=60,
            )
            latency = time.time() - t0
            if r.status_code == 200 and latency < 1.0:
                consecutive_ok += 1
                print(f"   [setup] Probe OK ({consecutive_ok}/{CONSECUTIVE_OK}, latency={latency:.2f}s)")
                if consecutive_ok >= CONSECUTIVE_OK:
                    print(f"   [setup] Pipeline drained")
                    settled = True
                    break
                time.sleep(2)
            else:
                consecutive_ok = 0
                time.sleep(5)
        except Exception as e:
            consecutive_ok = 0
            time.sleep(5)
    if not settled:
        print(f"   [setup] WARNING: pipeline did not drain within {max_settle_sec}s")

    return primary, screen_session, screen_log


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--accum-values", type=str, default=None,
                   help="Comma-separated accumulation values to test (default: 5,20,50,100)")
    p.add_argument("--rates", type=str, default=None,
                   help="Comma-separated rates to test")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    accum_values = [int(v) for v in args.accum_values.split(",")] if args.accum_values else ACCUM_VALUES
    rates = [int(r) for r in args.rates.split(",")] if args.rates else EXPERIMENT_RATES

    results_dir = RESULTS_BASE / f"tpcc_pb_accum_{timestamp}"
    results_dir.mkdir(parents=True, exist_ok=True)
    payloads_file = results_dir / "neworder_payloads.txt"

    print("=" * 60)
    print("Experiment C: TPC-C CAPTURE_ACCUMULATION_MS Sweep")
    print(f"  Accum values : {accum_values} ms")
    print(f"  Rates        : {rates}")
    print(f"  Results      -> {results_dir}")
    print("=" * 60)

    base.generate_payloads_file(payloads_file, base.NUM_WAREHOUSES)

    all_results = {}

    for accum_ms in accum_values:
        print(f"\n{'#'*60}")
        print(f"  ACCUMULATION_MS = {accum_ms}")
        print(f"{'#'*60}")

        accum_results = []
        accum_dir = results_dir / f"accum_{accum_ms}ms"
        accum_dir.mkdir(parents=True, exist_ok=True)

        # Point base module to this accum's subdir
        base.RESULTS_DIR = accum_dir
        base.PAYLOADS_FILE = payloads_file

        for i, rate in enumerate(rates):
            print(f"\n{'='*60}")
            print(f"  Accum={accum_ms}ms  Rate {i+1}/{len(rates)}: {rate} req/s")
            print(f"{'='*60}")

            primary, screen_session, screen_log = setup_fresh_cluster(accum_ms)

            # Warmup
            print(f"\n   -> rate={rate} req/s (warmup {base.WARMUP_DURATION_SEC}s) ...")
            base.warmup_rate_point(primary, rate)
            print(f"   Settling 5s after warmup ...")
            time.sleep(5)

            # Measurement
            print(f"   -> rate={rate} req/s (measuring {base.LOAD_DURATION_SEC}s) ...")
            m = base.run_load_point(primary, rate)
            row = {"rate_rps": rate, "accum_ms": accum_ms, **m}
            accum_results.append(row)
            print(
                f"     tput={m['throughput_rps']:.2f} rps  "
                f"avg={m['avg_ms']:.1f}ms  "
                f"p50={m['p50_ms']:.1f}ms  "
                f"p95={m['p95_ms']:.1f}ms"
            )

            # Per-rate log
            dest = accum_dir / f"screen_rate{rate}.log"
            os.system(f"cp {screen_log} {dest} 2>/dev/null || true")

            if m["avg_ms"] > base.AVG_LATENCY_THRESHOLD_MS and (
                m["actual_achieved_rps"] > 0
                and m["throughput_rps"] < 0.75 * m["actual_achieved_rps"]
            ):
                print(f"   Saturated — stopping this accum value early.")
                break

        all_results[accum_ms] = accum_results

    # Symlink
    symlink = RESULTS_BASE / "tpcc_pb_accum"
    if symlink.is_symlink():
        symlink.unlink()
    elif symlink.is_dir():
        symlink.rename(RESULTS_BASE / f"tpcc_pb_accum_old_{timestamp}")
    symlink.symlink_to(results_dir.name)

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY: CAPTURE_ACCUMULATION_MS Sweep")
    print("=" * 60)
    for accum_ms, rows in all_results.items():
        print(f"\n  ACCUM = {accum_ms}ms:")
        print(f"   {'rate':>6}  {'tput':>8}  {'avg':>8}  {'p50':>8}  {'p95':>8}")
        print("   " + "-" * 48)
        for row in rows:
            print(
                f"   {row['rate_rps']:>5.0f}  "
                f"{row['throughput_rps']:>7.2f}  "
                f"{row['avg_ms']:>7.1f}ms  "
                f"{row['p50_ms']:>7.1f}ms  "
                f"{row['p95_ms']:>7.1f}ms"
            )

    print(f"\n[Done] Results in {results_dir}/")
