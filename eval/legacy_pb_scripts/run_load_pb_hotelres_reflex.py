import warnings as _w; _w.warn("DEPRECATED: Use run_load_pb_reflex_app.py --app hotelres instead.", DeprecationWarning, stacklevel=2)
"""
run_load_pb_hotelres_reflex.py — XDN Primary-Backup hotel-reservation benchmark.

Spins up a fresh hotel-reservation PB cluster, generates dummy data,
then runs get_latency_at_rate.go with POST /reservation workload at
increasing rates to find the throughput ceiling.

Run from the eval/ directory:
    python3 run_load_pb_hotelres_reflex.py [--rates 1,100,200]

Outputs go to eval/results/load_pb_hotelres_reflex/:
    rate1.txt  rate100.txt  ...  — go-client output per rate point
"""

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

from run_load_pb_hotelres_common import (
    AR_HOSTS,
    CONTROL_PLANE_HOST,
    HOTEL_RES_YAML,
    HTTP_PROXY_PORT,
    SCREEN_LOG,
    SERVICE_NAME,
    TIMEOUT_APP_SEC,
    TIMEOUT_PORT_SEC,
    TIMEOUT_PRIMARY_SEC,
    XDN_BINARY,
    check_app_ready,
    clear_xdn_cluster,
    detect_primary_via_docker,
    ensure_docker_images_on_rc,
    generate_dummy_data,
    generate_reservation_urls,
    start_cluster,
    wait_for_app_ready,
    wait_for_port,
    wait_for_service,
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

RESULTS_DIR = Path(__file__).resolve().parent / "results" / "load_pb_hotelres_reflex"
URLS_FILE = RESULTS_DIR / "reservation_urls.txt"


# ── Load Helpers ──────────────────────────────────────────────────────────────


def _run_load(primary, rate, output_file, duration_sec):
    """Run get_latency_at_rate.go at `rate` req/s for `duration_sec` seconds."""
    output_file.parent.mkdir(parents=True, exist_ok=True)
    url = f"http://{primary}:{HTTP_PROXY_PORT}/reservation"
    cmd = [
        "go", "run", str(GO_LATENCY_CLIENT),
        "-H", f"XDN: {SERVICE_NAME}",
        "-X", "POST",
        "-urls-file", str(URLS_FILE),
        url,
        "",  # no JSON body — params are in query string
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


def run_load_point(primary, rate):
    return _run_load(primary, rate, RESULTS_DIR / f"rate{rate}.txt", LOAD_DURATION_SEC)


def warmup_rate_point(primary, rate):
    m = _run_load(primary, rate, RESULTS_DIR / f"warmup_rate{rate}.txt", WARMUP_DURATION_SEC)
    print(f"     [warmup] tput={m['throughput_rps']:.2f} rps  avg={m['avg_ms']:.1f}ms")
    return m


def copy_screen_log():
    """Copy the running screen log snapshot into results dir."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    dest = RESULTS_DIR / "screen.log"
    os.system(f"cp {SCREEN_LOG} {dest} 2>/dev/null || true")
    print(f"   Screen log saved -> {dest}")


# ── Main ──────────────────────────────────────────────────────────────────────


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--skip-setup", action="store_true",
                   help="Skip cluster setup (assume already running)")
    p.add_argument("--skip-seed", action="store_true",
                   help="Skip data generation (assume already seeded)")
    p.add_argument("--rates", type=str, default=None,
                   help="Comma-separated list of offered rates (req/s) to override default")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()

    print("=" * 60)
    print("Hotel-Reservation PB Throughput-Ceiling Investigation")
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
                print(f"       Check: tail -f {SCREEN_LOG}")
                sys.exit(1)

        # Phase 3b — Ensure images on RC
        print("\n[Phase 3b] Ensuring Docker images are present on RC ...")
        ensure_docker_images_on_rc()

        # Phase 4 — Launch hotel-reservation
        print("\n[Phase 4] Launching hotel-reservation service ...")
        cmd = (
            f"XDN_CONTROL_PLANE={CONTROL_PLANE_HOST} {XDN_BINARY} "
            f"launch {SERVICE_NAME} --file={HOTEL_RES_YAML}"
        )
        print(f"   {cmd}")
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        print(result.stdout)
        if result.returncode != 0:
            print(f"ERROR: xdn launch failed:\n{result.stderr}")
            sys.exit(1)

        # Phase 5 — Wait for HTTP readiness
        print("\n[Phase 5] Waiting for hotel-reservation HTTP readiness ...")
        ok, _ = wait_for_service(AR_HOSTS, HTTP_PROXY_PORT, SERVICE_NAME, TIMEOUT_APP_SEC)
        if not ok:
            print(f"ERROR: hotel-reservation not ready after {TIMEOUT_APP_SEC}s.")
            copy_screen_log()
            sys.exit(1)
    else:
        print("\n[Phase 1-5] Skipping setup (--skip-setup)")

    # Phase 6 — Detect primary
    print("\n[Phase 6] Detecting primary node ...")
    primary = detect_primary_via_docker(AR_HOSTS)
    if not primary:
        print("   WARNING: Could not detect primary via docker, falling back to first AR")
        primary = AR_HOSTS[0]
    print(f"   Primary: {primary}")

    if not args.skip_seed:
        # Phase 7 — Generate dummy data
        print("\n[Phase 7] Generating dummy data ...")
        for attempt in range(1, 4):
            if generate_dummy_data(primary, HTTP_PROXY_PORT, SERVICE_NAME):
                break
            print(f"   Attempt {attempt} failed, retrying in 10s ...")
            time.sleep(10)
        else:
            print("ERROR: Failed to generate dummy data after 3 attempts.")
            copy_screen_log()
            sys.exit(1)

        # Phase 8 — Verify app
        print("\n[Phase 8] Verifying app readiness ...")
        if not wait_for_app_ready(primary, HTTP_PROXY_PORT, SERVICE_NAME, timeout_sec=60):
            print("ERROR: App not ready after dummy data generation.")
            copy_screen_log()
            sys.exit(1)

        # Phase 9 — Generate reservation URLs
        print(f"\n[Phase 9] Generating reservation URLs -> {URLS_FILE} ...")
        RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        generate_reservation_urls(
            primary, HTTP_PROXY_PORT, str(URLS_FILE),
            count=500, service=SERVICE_NAME,
        )
    else:
        print("\n[Phase 7-9] Skipping data generation (--skip-seed)")
        print(f"   Assuming {URLS_FILE} already exists.")

    # Determine rate list
    if args.rates:
        rates = [int(r.strip()) for r in args.rates.split(",")]
    else:
        rates = BOTTLENECK_RATES

    # Phase 10 — Targeted load sweep
    print(f"\n[Phase 10] Targeted load sweep at {rates} req/s ...")
    print(f"   Workload  : POST /reservation (query params)  XDN:{SERVICE_NAME}")
    print(f"   Duration  : {LOAD_DURATION_SEC}s per rate")
    print(f"   Primary   : {primary}:{HTTP_PROXY_PORT}")

    results = []
    for i, rate in enumerate(rates):
        if i > 0 and INTER_RATE_PAUSE_SEC > 0:
            print(f"   Pausing {INTER_RATE_PAUSE_SEC}s for server drain ...")
            time.sleep(INTER_RATE_PAUSE_SEC)

        print(f"\n   -> rate={rate} req/s (warmup {WARMUP_DURATION_SEC}s) ...")
        warmup_result = warmup_rate_point(primary, rate)
        if warmup_result["throughput_rps"] < 0.1 * rate:
            print(
                f"   SKIP: warmup tput={warmup_result['throughput_rps']:.2f} rps "
                f"< 10% of {rate} rps — system saturated"
            )
            continue
        print(f"   Settling 5s after warmup ...")
        time.sleep(5)

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
        if m["avg_ms"] > AVG_LATENCY_THRESHOLD_MS:
            print(
                f"   Avg latency {m['avg_ms']:.0f}ms > {AVG_LATENCY_THRESHOLD_MS}ms "
                f"threshold — system saturated, stopping sweep early."
            )
            break
        if m["actual_achieved_rps"] > 0 and m["throughput_rps"] < 0.8 * m["actual_achieved_rps"]:
            print(
                f"   Throughput {m['throughput_rps']:.2f} rps < 80% of offered "
                f"{m['actual_achieved_rps']:.2f} rps — system saturated, stopping sweep early."
            )
            break

    # Phase 11 — Save screen log snapshot
    print("\n[Phase 11] Saving screen log snapshot ...")
    copy_screen_log()

    # Phase 12 — Summary
    print("\n[Phase 12] Summary")
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

    # Phase 13 — Generate plots and timing analysis
    print("\n[Phase 13] Generating plots ...")
    eval_dir = str(Path(__file__).resolve().parent)
    for script in ["investigate_parse_pb_timing.py", "plot_load_latency.py"]:
        cmd = ["python3", script, "--results-dir", str(RESULTS_DIR)]
        if script == "investigate_parse_pb_timing.py":
            cmd += ["--log", str(RESULTS_DIR / "screen.log")]
        r = subprocess.run(
            cmd,
            capture_output=True, text=True,
            cwd=eval_dir,
        )
        print(r.stdout)
        if r.returncode != 0:
            print(f"   WARNING: {script} failed:\n{r.stderr}")

    print(f"\n[Done] Results in {RESULTS_DIR}/")
