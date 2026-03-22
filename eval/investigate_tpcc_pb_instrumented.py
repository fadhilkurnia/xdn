"""
Experiment D: TPC-C full pipeline with instrumentation.

Runs the full PB pipeline at target rate (200 rps) with INFO-level
capture thread logs to measure exact timing breakdown:
  - captureStateDiff time (ms)
  - batch size (requests per cycle)
  - cycle time (ms)
  - diff size (bytes)

Run from the eval/ directory:
    python3 investigate_tpcc_pb_instrumented.py
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
# Fewer rates — focus on the region around the target
EXPERIMENT_RATES = [1, 60, 80, 100, 120, 140, 160, 200, 250, 300]


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--rates", type=str, default=None,
                   help="Comma-separated rates (default: 1,60,80,100,120,140,160,200,250,300)")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    rates = [int(r) for r in args.rates.split(",")] if args.rates else EXPERIMENT_RATES

    results_dir = RESULTS_BASE / f"tpcc_pb_instrumented_{timestamp}"
    results_dir.mkdir(parents=True, exist_ok=True)
    payloads_file = results_dir / "neworder_payloads.txt"

    print("=" * 60)
    print("Experiment D: TPC-C Full Pipeline (Instrumented)")
    print(f"  GP_JVM_ARGS : {base.GP_JVM_ARGS}")
    print(f"  Rates       : {rates}")
    print(f"  Results     -> {results_dir}")
    print(f"  NOTE: Capture thread logs are at INFO level (Phase 1 instrumentation)")
    print("=" * 60)

    base.generate_payloads_file(payloads_file, base.NUM_WAREHOUSES)

    # Point base module to our results dir
    base.RESULTS_DIR = results_dir
    base.PAYLOADS_FILE = payloads_file

    results = []
    screen_logs = []

    for i, rate in enumerate(rates):
        print(f"\n{'='*60}")
        print(f"  Rate point {i+1}/{len(rates)}: {rate} req/s (instrumented)")
        print(f"{'='*60}")

        primary = base.setup_fresh_tpcc_cluster()

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

        # Per-rate log — important for timing analysis
        dest = results_dir / f"screen_rate{rate}.log"
        os.system(f"cp {base.SCREEN_LOG} {dest} 2>/dev/null || true")
        screen_logs.append(dest)

        if m["avg_ms"] > base.AVG_LATENCY_THRESHOLD_MS and (
            m["actual_achieved_rps"] > 0
            and m["throughput_rps"] < 0.75 * m["actual_achieved_rps"]
        ):
            print(f"   Saturated — stopping early.")
            break

    # Combined screen log for investigate_parse_pb_timing.py
    combined_log = results_dir / "screen.log"
    with open(combined_log, "w") as out:
        for log_path in screen_logs:
            if log_path.exists():
                out.write(log_path.read_text())
    print(f"\n   Combined screen log -> {combined_log}")

    # Symlink
    symlink = RESULTS_BASE / "tpcc_pb_instrumented"
    if symlink.is_symlink():
        symlink.unlink()
    elif symlink.is_dir():
        symlink.rename(RESULTS_BASE / f"tpcc_pb_instrumented_old_{timestamp}")
    symlink.symlink_to(results_dir.name)

    # Summary
    print("\n[Summary — Full Pipeline Instrumented]")
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

    # Run timing parser on the combined screen log
    print("\n[Timing Analysis] Parsing capture thread logs ...")
    eval_dir = str(Path(__file__).resolve().parent)
    r = subprocess.run(
        ["python3", "investigate_parse_pb_timing.py", "--log", str(combined_log),
         "--results-dir", str(results_dir)],
        capture_output=True, text=True, cwd=eval_dir,
    )
    print(r.stdout)
    if r.returncode != 0:
        print(f"   WARNING: investigate_parse_pb_timing.py failed:\n{r.stderr}")

    print(f"\n[Done] Results in {results_dir}/")
