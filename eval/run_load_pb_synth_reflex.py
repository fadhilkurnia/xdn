"""
run_load_pb_synth_reflex.py — XDN Primary-Backup synth-workload benchmark.

Runs 4 experiments to validate the sync-granularity hypothesis:
  exp1: vary txns/request  (XDN should be flat, rqlite linear)
  exp2: vary ops/txn       (XDN should be flat)
  exp3: vary write_size    (effect on state diff size)
  exp4: combined rate sweep (standard load-latency curve)

Run from the eval/ directory:
    python3 run_load_pb_synth_reflex.py [--skip-setup] [--experiments 1,2,3,4]

Outputs go to eval/results/load_pb_synth_reflex/{exp1_vary_txns,exp2_vary_ops,...}/
"""

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

from run_load_pb_synth_common import (
    AR_HOSTS,
    CONTROL_PLANE_HOST,
    HTTP_PROXY_PORT,
    SCREEN_LOG,
    SERVICE_NAME,
    SYNTH_IMAGE,
    SYNTH_YAML,
    TIMEOUT_APP_SEC,
    TIMEOUT_PORT_SEC,
    XDN_BINARY,
    check_synth_ready,
    clear_xdn_cluster,
    detect_primary_via_docker,
    ensure_synth_images,
    make_workload_payload,
    start_cluster,
    wait_for_port,
    wait_for_service,
    wait_for_synth_ready,
)
from run_load_pb_wordpress_reflex import (
    AVG_LATENCY_THRESHOLD_MS,
    BOTTLENECK_RATES,
    GO_LATENCY_CLIENT,
    INTER_RATE_PAUSE_SEC,
    LOAD_DURATION_SEC,
    WARMUP_DURATION_SEC,
    _parse_go_output,
)

# ── Config ────────────────────────────────────────────────────────────────────

RESULTS_BASE = Path(__file__).resolve().parent / "results" / "load_pb_synth_reflex"

# Experiment parameters
EXP1_TXNS_VALUES = [1, 2, 5, 10, 20]
EXP2_OPS_VALUES = [1, 5, 10, 20, 50]
EXP3_WRITESIZE_VALUES = [100, 1000, 10000, 100000]
EXP_FIXED_RATE = 100
EXP_DURATION_SEC = 60
EXP4_TXNS = 5
EXP4_OPS = 5
EXP4_WRITESIZE = 1000


# ── Load Helpers ──────────────────────────────────────────────────────────────


def _run_load(primary, rate, payload, output_file, duration_sec):
    """Run get_latency_at_rate.go with synth-workload payload."""
    output_file.parent.mkdir(parents=True, exist_ok=True)
    url = f"http://{primary}:{HTTP_PROXY_PORT}/workload"
    cmd = [
        "go", "run", str(GO_LATENCY_CLIENT),
        "-H", f"XDN: {SERVICE_NAME}",
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


def run_experiment_point(primary, rate, payload, output_file, duration_sec):
    """Run a single experiment measurement point."""
    m = _run_load(primary, rate, payload, output_file, duration_sec)
    print(
        f"     tput={m['throughput_rps']:.2f} rps  "
        f"avg={m['avg_ms']:.1f}ms  "
        f"p50={m['p50_ms']:.1f}ms  "
        f"p95={m['p95_ms']:.1f}ms"
    )
    return m


def warmup_point(primary, rate, payload, output_file):
    """Run a short warmup."""
    m = _run_load(primary, rate, payload, output_file, WARMUP_DURATION_SEC)
    print(f"     [warmup] tput={m['throughput_rps']:.2f} rps  avg={m['avg_ms']:.1f}ms")
    return m


def copy_screen_log():
    """Copy the running screen log snapshot into results dir."""
    RESULTS_BASE.mkdir(parents=True, exist_ok=True)
    dest = RESULTS_BASE / "screen.log"
    os.system(f"cp {SCREEN_LOG} {dest} 2>/dev/null || true")
    print(f"   Screen log saved -> {dest}")


# ── Experiments ──────────────────────────────────────────────────────────────


def run_exp1(primary):
    """Experiment 1: Vary transactions per request."""
    print("\n" + "=" * 60)
    print("Experiment 1: Vary txns/request (ops=1, write_size=100)")
    print("=" * 60)
    exp_dir = RESULTS_BASE / "exp1_vary_txns"
    results = []

    for txns in EXP1_TXNS_VALUES:
        payload = make_workload_payload(txns=txns, ops=1, write_size=100)
        print(f"\n   -> txns={txns} at {EXP_FIXED_RATE} rps ...")

        # Warmup
        warmup_point(primary, EXP_FIXED_RATE, payload,
                     exp_dir / f"warmup_txns{txns}.txt")
        time.sleep(5)

        # Measure
        m = run_experiment_point(primary, EXP_FIXED_RATE, payload,
                                 exp_dir / f"txns{txns}.txt", EXP_DURATION_SEC)
        results.append({"txns": txns, **m})
        time.sleep(INTER_RATE_PAUSE_SEC)

    print("\n   Summary (exp1: vary txns):")
    print(f"   {'txns':>6}  {'tput':>8}  {'avg':>8}  {'p50':>8}  {'p95':>8}")
    print("   " + "-" * 48)
    for row in results:
        print(
            f"   {row['txns']:>6}  "
            f"{row['throughput_rps']:>7.2f}  "
            f"{row['avg_ms']:>7.1f}ms  "
            f"{row['p50_ms']:>7.1f}ms  "
            f"{row['p95_ms']:>7.1f}ms"
        )
    return results


def run_exp2(primary):
    """Experiment 2: Vary operations per transaction."""
    print("\n" + "=" * 60)
    print("Experiment 2: Vary ops/txn (txns=1, write_size=100)")
    print("=" * 60)
    exp_dir = RESULTS_BASE / "exp2_vary_ops"
    results = []

    for ops in EXP2_OPS_VALUES:
        payload = make_workload_payload(txns=1, ops=ops, write_size=100)
        print(f"\n   -> ops={ops} at {EXP_FIXED_RATE} rps ...")

        warmup_point(primary, EXP_FIXED_RATE, payload,
                     exp_dir / f"warmup_ops{ops}.txt")
        time.sleep(5)

        m = run_experiment_point(primary, EXP_FIXED_RATE, payload,
                                 exp_dir / f"ops{ops}.txt", EXP_DURATION_SEC)
        results.append({"ops": ops, **m})
        time.sleep(INTER_RATE_PAUSE_SEC)

    print("\n   Summary (exp2: vary ops):")
    print(f"   {'ops':>6}  {'tput':>8}  {'avg':>8}  {'p50':>8}  {'p95':>8}")
    print("   " + "-" * 48)
    for row in results:
        print(
            f"   {row['ops']:>6}  "
            f"{row['throughput_rps']:>7.2f}  "
            f"{row['avg_ms']:>7.1f}ms  "
            f"{row['p50_ms']:>7.1f}ms  "
            f"{row['p95_ms']:>7.1f}ms"
        )
    return results


def run_exp3(primary):
    """Experiment 3: Vary write size."""
    print("\n" + "=" * 60)
    print("Experiment 3: Vary write_size (txns=1, ops=1)")
    print("=" * 60)
    exp_dir = RESULTS_BASE / "exp3_vary_writesize"
    results = []

    for ws in EXP3_WRITESIZE_VALUES:
        payload = make_workload_payload(txns=1, ops=1, write_size=ws)
        print(f"\n   -> write_size={ws} at {EXP_FIXED_RATE} rps ...")

        warmup_point(primary, EXP_FIXED_RATE, payload,
                     exp_dir / f"warmup_write_size{ws}.txt")
        time.sleep(5)

        m = run_experiment_point(primary, EXP_FIXED_RATE, payload,
                                 exp_dir / f"write_size{ws}.txt", EXP_DURATION_SEC)
        results.append({"write_size": ws, **m})
        time.sleep(INTER_RATE_PAUSE_SEC)

    print("\n   Summary (exp3: vary write_size):")
    print(f"   {'size':>8}  {'tput':>8}  {'avg':>8}  {'p50':>8}  {'p95':>8}")
    print("   " + "-" * 48)
    for row in results:
        print(
            f"   {row['write_size']:>8}  "
            f"{row['throughput_rps']:>7.2f}  "
            f"{row['avg_ms']:>7.1f}ms  "
            f"{row['p50_ms']:>7.1f}ms  "
            f"{row['p95_ms']:>7.1f}ms"
        )
    return results


def run_exp4(primary, rates):
    """Experiment 4: Combined rate sweep (txns=5, ops=5, write_size=1K)."""
    print("\n" + "=" * 60)
    print(f"Experiment 4: Rate sweep (txns={EXP4_TXNS}, ops={EXP4_OPS}, write_size={EXP4_WRITESIZE})")
    print("=" * 60)
    exp_dir = RESULTS_BASE / "exp4_combined"
    payload = make_workload_payload(txns=EXP4_TXNS, ops=EXP4_OPS, write_size=EXP4_WRITESIZE)
    results = []

    for i, rate in enumerate(rates):
        if i > 0 and INTER_RATE_PAUSE_SEC > 0:
            print(f"   Pausing {INTER_RATE_PAUSE_SEC}s for server drain ...")
            time.sleep(INTER_RATE_PAUSE_SEC)

        print(f"\n   -> rate={rate} req/s (warmup {WARMUP_DURATION_SEC}s) ...")
        wm = warmup_point(primary, rate, payload,
                          exp_dir / f"warmup_rate{rate}.txt")
        if wm["throughput_rps"] < 0.1 * rate:
            print(
                f"   SKIP: warmup tput={wm['throughput_rps']:.2f} rps "
                f"< 10% of {rate} rps — system saturated"
            )
            continue
        print("   Settling 5s after warmup ...")
        time.sleep(5)

        print(f"   -> rate={rate} req/s (measuring {LOAD_DURATION_SEC}s) ...")
        m = run_experiment_point(primary, rate, payload,
                                 exp_dir / f"rate{rate}.txt", LOAD_DURATION_SEC)
        row = {"rate_rps": rate, **m}
        results.append(row)

        if m["avg_ms"] > AVG_LATENCY_THRESHOLD_MS:
            print(
                f"   Avg latency {m['avg_ms']:.0f}ms > {AVG_LATENCY_THRESHOLD_MS}ms "
                f"— system saturated, stopping sweep early."
            )
            break
        if m["actual_achieved_rps"] > 0 and m["throughput_rps"] < 0.8 * m["actual_achieved_rps"]:
            print(
                f"   Throughput {m['throughput_rps']:.2f} rps < 80% of offered "
                f"{m['actual_achieved_rps']:.2f} rps — stopping."
            )
            break

    print("\n   Summary (exp4: rate sweep):")
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
    return results


# ── Main ──────────────────────────────────────────────────────────────────────


def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--skip-setup", action="store_true",
                   help="Skip cluster setup (assume already running)")
    p.add_argument("--experiments", type=str, default="1,2,3,4",
                   help="Comma-separated experiment numbers to run (default: 1,2,3,4)")
    p.add_argument("--rates", type=str, default=None,
                   help="Override rate list for exp4 (comma-separated)")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    experiments = [int(e.strip()) for e in args.experiments.split(",")]

    print("=" * 60)
    print("Synth-Workload PB Sync-Granularity Benchmark")
    print("=" * 60)

    if not args.skip_setup:
        # Phase 1 — Reset cluster
        print("\n[Phase 1] Force-clearing cluster ...")
        clear_xdn_cluster()

        # Phase 2 — Start cluster
        print("\n[Phase 2] Starting XDN cluster ...")
        start_cluster()

        # Phase 3 — Wait for ARs
        print("\n[Phase 3] Waiting for Active Replicas ...")
        for host in AR_HOSTS:
            if not wait_for_port(host, HTTP_PROXY_PORT, TIMEOUT_PORT_SEC):
                print(f"ERROR: AR at {host}:{HTTP_PROXY_PORT} not ready.")
                sys.exit(1)

        # Phase 3b — Ensure images
        print("\n[Phase 3b] Ensuring Docker images on all nodes ...")
        ensure_synth_images(AR_HOSTS + [CONTROL_PLANE_HOST])

        # Phase 4 — Launch synth service
        print("\n[Phase 4] Launching synth-workload service ...")
        cmd = (
            f"XDN_CONTROL_PLANE={CONTROL_PLANE_HOST} {XDN_BINARY} "
            f"launch {SERVICE_NAME} --file={SYNTH_YAML}"
        )
        print(f"   {cmd}")
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        print(result.stdout)
        if result.returncode != 0:
            print(f"ERROR: xdn launch failed:\n{result.stderr}")
            sys.exit(1)

        # Phase 5 — Wait for readiness
        print("\n[Phase 5] Waiting for synth-workload HTTP readiness ...")
        ok, _ = wait_for_service(AR_HOSTS, HTTP_PROXY_PORT, SERVICE_NAME, TIMEOUT_APP_SEC)
        if not ok:
            print(f"ERROR: synth-workload not ready after {TIMEOUT_APP_SEC}s.")
            copy_screen_log()
            sys.exit(1)
    else:
        print("\n[Phase 1-5] Skipping setup (--skip-setup)")

    # Phase 6 — Detect primary
    print("\n[Phase 6] Detecting primary node ...")
    primary = detect_primary_via_docker(AR_HOSTS)
    if not primary:
        print("   WARNING: Could not detect primary, falling back to first AR")
        primary = AR_HOSTS[0]
    print(f"   Primary: {primary}")

    # Phase 7 — Verify readiness
    print("\n[Phase 7] Verifying synth-workload readiness ...")
    if not wait_for_synth_ready(primary, HTTP_PROXY_PORT, SERVICE_NAME, timeout_sec=60):
        print("ERROR: synth-workload not ready.")
        copy_screen_log()
        sys.exit(1)

    # Phase 8 — Run experiments
    print("\n[Phase 8] Running experiments ...")
    all_results = {}

    if 1 in experiments:
        all_results["exp1"] = run_exp1(primary)

    if 2 in experiments:
        all_results["exp2"] = run_exp2(primary)

    if 3 in experiments:
        all_results["exp3"] = run_exp3(primary)

    if 4 in experiments:
        if args.rates:
            rates = [int(r.strip()) for r in args.rates.split(",")]
        else:
            rates = BOTTLENECK_RATES
        all_results["exp4"] = run_exp4(primary, rates)

    # Phase 9 — Save screen log
    print("\n[Phase 9] Saving screen log ...")
    copy_screen_log()

    # Phase 10 — Generate plots
    print("\n[Phase 10] Generating plots ...")
    eval_dir = str(Path(__file__).resolve().parent)
    r = subprocess.run(
        ["python3", "plot_synth_comparison.py",
         "--results-dir", str(RESULTS_BASE),
         "--variant", "pb"],
        capture_output=True, text=True, cwd=eval_dir,
    )
    print(r.stdout)
    if r.returncode != 0:
        print(f"   WARNING: plot_synth_comparison.py failed:\n{r.stderr}")

    print(f"\n[Done] Results in {RESULTS_BASE}/")
