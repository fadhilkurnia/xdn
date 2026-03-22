"""
Experiment A: TPC-C DDE bypass — measure container-only capacity.

Uses the ___DDE HTTP header to bypass PBReplicaCoordinator entirely,
measuring raw PostgreSQL + gunicorn throughput through FUSE.

Run from the eval/ directory:
    python3 investigate_tpcc_pb_dde.py
"""

import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import run_load_pb_tpcc_reflex as base

# Override results directory
RESULTS_BASE = base.RESULTS_BASE
EXPERIMENT_RATES = [100, 200, 250, 300, 400]


def run_load_point_dde(primary, rate, results_dir, payloads_file):
    """Like base.run_load_point but adds ___DDE header to bypass coordinator."""
    results_dir.mkdir(parents=True, exist_ok=True)
    output_file = results_dir / f"rate{rate}.txt"
    url = f"http://{primary}:{base.HTTP_PROXY_PORT}/orders"
    cmd = [
        "go", "run", str(base.GO_LATENCY_CLIENT),
        "-H", f"XDN: {base.SERVICE_NAME}",
        "-H", "Content-Type: application/json",
        "-H", "___DDE: true",
        "-X", "POST",
        "-payloads-file", str(payloads_file),
        url,
        '{"w_id": 1, "c_id": 1}',
        str(base.LOAD_DURATION_SEC),
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
    return base._parse_go_output(output_file)


def warmup_dde(primary, rate, results_dir, payloads_file):
    """Warmup with DDE header."""
    results_dir.mkdir(parents=True, exist_ok=True)
    output_file = results_dir / f"warmup_rate{rate}.txt"
    url = f"http://{primary}:{base.HTTP_PROXY_PORT}/orders"
    cmd = [
        "go", "run", str(base.GO_LATENCY_CLIENT),
        "-H", f"XDN: {base.SERVICE_NAME}",
        "-H", "Content-Type: application/json",
        "-H", "___DDE: true",
        "-X", "POST",
        "-payloads-file", str(payloads_file),
        url,
        '{"w_id": 1, "c_id": 1}',
        str(base.WARMUP_DURATION_SEC),
        str(rate),
    ]
    env = os.environ.copy()
    env["GO111MODULE"] = "off"
    env["GOWORK"] = "off"
    with open(output_file, "w") as fh:
        subprocess.run(cmd, stdout=fh, stderr=subprocess.STDOUT, env=env, text=True)
    m = base._parse_go_output(output_file)
    print(f"     [warmup] tput={m['throughput_rps']:.2f} rps  avg={m['avg_ms']:.1f}ms")
    return m


if __name__ == "__main__":
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_dir = RESULTS_BASE / f"tpcc_pb_dde_{timestamp}"
    results_dir.mkdir(parents=True, exist_ok=True)
    payloads_file = results_dir / "neworder_payloads.txt"

    print("=" * 60)
    print("Experiment A: TPC-C DDE Bypass (container-only capacity)")
    print(f"  Rates    : {EXPERIMENT_RATES}")
    print(f"  Results  -> {results_dir}")
    print("=" * 60)

    base.generate_payloads_file(payloads_file, base.NUM_WAREHOUSES)

    results = []
    screen_logs = []

    for i, rate in enumerate(EXPERIMENT_RATES):
        print(f"\n{'='*60}")
        print(f"  Rate point {i+1}/{len(EXPERIMENT_RATES)}: {rate} req/s (DDE bypass)")
        print(f"{'='*60}")

        # Use base module's RESULTS_DIR/PAYLOADS_FILE temporarily
        base.RESULTS_DIR = results_dir
        base.PAYLOADS_FILE = payloads_file
        primary = base.setup_fresh_tpcc_cluster()

        # Warmup
        print(f"\n   -> rate={rate} req/s (warmup {base.WARMUP_DURATION_SEC}s) ...")
        warmup_dde(primary, rate, results_dir, payloads_file)
        print(f"   Settling 5s after warmup ...")
        time.sleep(5)

        # Measurement
        print(f"   -> rate={rate} req/s (measuring {base.LOAD_DURATION_SEC}s) ...")
        m = run_load_point_dde(primary, rate, results_dir, payloads_file)
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
        os.system(f"cp {base.SCREEN_LOG} {dest} 2>/dev/null || true")
        screen_logs.append(dest)

        # Early stop
        if m["avg_ms"] > base.AVG_LATENCY_THRESHOLD_MS and (
            m["actual_achieved_rps"] > 0
            and m["throughput_rps"] < 0.75 * m["actual_achieved_rps"]
        ):
            print(f"   Saturated — stopping early.")
            break

    # Symlink
    symlink = RESULTS_BASE / "tpcc_pb_dde"
    if symlink.is_symlink():
        symlink.unlink()
    elif symlink.is_dir():
        symlink.rename(RESULTS_BASE / f"tpcc_pb_dde_old_{timestamp}")
    symlink.symlink_to(results_dir.name)

    # Summary
    print("\n[Summary — DDE Bypass]")
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
