import warnings as _w; _w.warn("DEPRECATED: Use run_load_pb_reflex_app.py --app bookcatalog instead.", DeprecationWarning, stacklevel=2)
"""
run_load_pb_bookcatalog_reflex.py — XDN Primary-Backup bookcatalog-nd benchmark.

For each rate point, spins up a **fresh** bookcatalog-nd PB cluster (forceclear,
start, deploy, seed) so that SQLite WAL growth from earlier rates cannot
contaminate later measurements.

Run from the eval/ directory:
    python3 run_load_pb_bookcatalog_reflex.py [--rates 1,100,200]

Outputs go to eval/results/bookcatalog_pb_<timestamp>/:
    rate1.txt  rate100.txt  ...  — go-client output per rate point
    screen_rate<N>.log           — per-rate XDN screen log
    screen.log                   — concatenated screen log for timing analysis
    node_ar0.log ... node_ar2.log — per-node GigaPaxos logs (from last rate)
"""

import argparse
import os
import subprocess
import sys
import time
import urllib.request
from datetime import datetime
from pathlib import Path

from run_load_pb_bookcatalog_common import (
    AR_HOSTS,
    BOOKCATALOG_YAML,
    CONTROL_PLANE_HOST,
    HTTP_PROXY_PORT,
    SCREEN_LOG,
    SERVICE_NAME,
    TIMEOUT_APP_SEC,
    TIMEOUT_PORT_SEC,
    XDN_BINARY,
    check_app_ready,
    clear_xdn_cluster,
    detect_primary_via_docker,
    ensure_docker_images_on_rc,
    seed_books,
    start_cluster,
    wait_for_app_ready,
    wait_for_port,
    wait_for_service,
)
from run_load_pb_wordpress_reflex import (
    AVG_LATENCY_THRESHOLD_MS,
    BOTTLENECK_RATES,
    GO_LATENCY_CLIENT,
    LOAD_DURATION_SEC,
    WARMUP_DURATION_SEC,
    _parse_go_output,
)

# ── Config ────────────────────────────────────────────────────────────────────

RESULTS_BASE = Path(__file__).resolve().parent / "results"
RESULTS_DIR = None  # set in main after timestamp suffix is computed

# GigaPaxos log path on each AR node (relative to gpServer.sh working dir)
GIGAPAXOS_LOG_PATH = "/users/fadhil/XdnGigapaxosApp/output/gigapaxos.log"

# Node name mapping for log collection
AR_NODE_NAMES = {
    "10.10.1.1": "ar0",
    "10.10.1.2": "ar1",
    "10.10.1.3": "ar2",
}


# ── WAL Checkpoint ───────────────────────────────────────────────────────────


def trigger_wal_checkpoint(host, port, service_name):
    """Send POST /admin/checkpoint to truncate the SQLite WAL."""
    url = f"http://{host}:{port}/admin/checkpoint"
    req = urllib.request.Request(url, method="POST", data=b"",
                                headers={"XDN": service_name})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = resp.read().decode()
            print(f"     [checkpoint] {resp.status} {body}")
    except Exception as e:
        print(f"     [checkpoint] WARNING: {e}")


# ── Load Helpers ──────────────────────────────────────────────────────────────


def _run_load(primary, rate, output_file, duration_sec):
    """Run get_latency_at_rate.go at `rate` req/s for `duration_sec` seconds."""
    output_file.parent.mkdir(parents=True, exist_ok=True)
    url = f"http://{primary}:{HTTP_PROXY_PORT}/api/books"
    cmd = [
        "go", "run", str(GO_LATENCY_CLIENT),
        "-H", f"XDN: {SERVICE_NAME}",
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


def run_load_point(primary, rate):
    return _run_load(primary, rate, RESULTS_DIR / f"rate{rate}.txt", LOAD_DURATION_SEC)


def warmup_rate_point(primary, rate):
    m = _run_load(primary, rate, RESULTS_DIR / f"warmup_rate{rate}.txt", WARMUP_DURATION_SEC)
    print(f"     [warmup] tput={m['throughput_rps']:.2f} rps  avg={m['avg_ms']:.1f}ms")
    return m


def copy_screen_log_for_rate(rate):
    """Copy the running screen log snapshot into results dir, tagged by rate."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    dest = RESULTS_DIR / f"screen_rate{rate}.log"
    os.system(f"cp {SCREEN_LOG} {dest} 2>/dev/null || true")
    print(f"   Screen log saved -> {dest}")
    return dest


def collect_node_logs():
    """Collect GigaPaxos logs from all AR nodes into results dir."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    for host in AR_HOSTS:
        node_name = AR_NODE_NAMES.get(host, host)
        dest = RESULTS_DIR / f"node_{node_name}.log"
        print(f"   Collecting log from {host} ({node_name}) ...")
        ret = os.system(f"scp {host}:{GIGAPAXOS_LOG_PATH} {dest} 2>/dev/null")
        if ret == 0:
            size_mb = dest.stat().st_size / (1024 * 1024)
            print(f"   -> {dest} ({size_mb:.1f} MB)")
        else:
            print(f"   WARNING: failed to collect log from {host}")

    # Also collect GC log from primary if it exists
    for host in AR_HOSTS:
        node_name = AR_NODE_NAMES.get(host, host)
        gc_dest = RESULTS_DIR / f"gc_{node_name}.log"
        ret = os.system(f"scp {host}:/tmp/xdn_gc.log {gc_dest} 2>/dev/null")
        if ret == 0:
            print(f"   GC log from {host} -> {gc_dest}")


# ── Fresh Cluster Setup ──────────────────────────────────────────────────────


def setup_fresh_cluster():
    """Forceclear, start, deploy service, seed data. Returns primary host or exits."""
    print("   [setup] Force-clearing cluster ...")
    clear_xdn_cluster()

    print("   [setup] Starting XDN cluster ...")
    start_cluster()

    print("   [setup] Waiting for Active Replicas ...")
    for host in AR_HOSTS:
        if not wait_for_port(host, HTTP_PROXY_PORT, TIMEOUT_PORT_SEC):
            print(f"ERROR: AR at {host}:{HTTP_PROXY_PORT} not ready.")
            sys.exit(1)

    print("   [setup] Ensuring Docker images on RC ...")
    ensure_docker_images_on_rc()

    print("   [setup] Launching bookcatalog-nd service ...")
    cmd = (
        f"XDN_CONTROL_PLANE={CONTROL_PLANE_HOST} {XDN_BINARY} "
        f"launch {SERVICE_NAME} --file={BOOKCATALOG_YAML}"
    )
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    print(result.stdout.strip())
    if result.returncode != 0:
        print(f"ERROR: xdn launch failed:\n{result.stderr}")
        sys.exit(1)

    print("   [setup] Waiting for HTTP readiness ...")
    ok, _ = wait_for_service(AR_HOSTS, HTTP_PROXY_PORT, SERVICE_NAME, TIMEOUT_APP_SEC)
    if not ok:
        print(f"ERROR: bookcatalog-nd not ready after {TIMEOUT_APP_SEC}s.")
        sys.exit(1)

    print("   [setup] Detecting primary ...")
    primary = detect_primary_via_docker(AR_HOSTS)
    if not primary:
        print("ERROR: Could not detect primary via docker.")
        sys.exit(1)
    print(f"   [setup] Primary: {primary}")

    print("   [setup] Seeding books ...")
    for attempt in range(1, 4):
        created = seed_books(primary, HTTP_PROXY_PORT, SERVICE_NAME, count=200)
        if created > 0:
            break
        print(f"   Attempt {attempt} failed, retrying in 10s ...")
        time.sleep(10)
    else:
        print("ERROR: Failed to seed books after 3 attempts.")
        sys.exit(1)

    print("   [setup] Verifying app readiness ...")
    if not wait_for_app_ready(primary, HTTP_PROXY_PORT, SERVICE_NAME, timeout_sec=60):
        print("ERROR: App not ready after seeding.")
        sys.exit(1)

    return primary


# ── Main ──────────────────────────────────────────────────────────────────────


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--skip-setup", action="store_true",
                   help="Skip cluster setup for ALL rates (assume already running)")
    p.add_argument("--skip-seed", action="store_true",
                   help="(ignored, kept for CLI compat)")
    p.add_argument("--rates", type=str, default=None,
                   help="Comma-separated list of offered rates (req/s) to override default")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()

    # Create timestamped results directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    RESULTS_DIR = RESULTS_BASE / f"load_pb_bookcatalog_reflex_{timestamp}"
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    # Determine rate list
    if args.rates:
        rates = [int(r.strip()) for r in args.rates.split(",")]
    else:
        rates = BOTTLENECK_RATES

    print("=" * 60)
    print("BookCatalog-ND PB Throughput-Ceiling Investigation")
    print(f"  Mode    : fresh cluster per rate (isolated)")
    print(f"  Rates   : {rates}")
    print(f"  Results -> {RESULTS_DIR}")
    print("=" * 60)

    # Save effective config for reproducibility
    from gp_config_utils import save_effective_config
    import run_load_pb_bookcatalog_common as _bc
    save_effective_config(_bc.GP_CONFIG, _bc.GP_JVM_ARGS, RESULTS_DIR)

    results = []
    screen_logs = []  # paths of per-rate screen logs

    for i, rate in enumerate(rates):
        print(f"\n{'='*60}")
        print(f"  Rate point {i+1}/{len(rates)}: {rate} req/s")
        print(f"{'='*60}")

        # ── Per-rate fresh cluster setup ──────────────────────────────
        if not args.skip_setup:
            primary = setup_fresh_cluster()
        else:
            print("   [setup] Skipped (--skip-setup)")
            primary = detect_primary_via_docker(AR_HOSTS)
            if not primary:
                print("ERROR: Could not detect primary via docker.")
                sys.exit(1)
            print(f"   Primary: {primary}")

        # ── Warmup ────────────────────────────────────────────────────
        print(f"\n   -> rate={rate} req/s (warmup {WARMUP_DURATION_SEC}s) ...")
        warmup_result = warmup_rate_point(primary, rate)
        if warmup_result["throughput_rps"] < 0.1 * rate:
            print(
                f"   SKIP: warmup tput={warmup_result['throughput_rps']:.2f} rps "
                f"< 10% of {rate} rps — system saturated"
            )
            log_path = copy_screen_log_for_rate(rate)
            screen_logs.append(log_path)
            continue
        print(f"   Settling 5s after warmup ...")
        time.sleep(5)

        # ── Measurement ───────────────────────────────────────────────
        print(f"   Triggering WAL checkpoint ...")
        trigger_wal_checkpoint(primary, HTTP_PROXY_PORT, SERVICE_NAME)

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

        # ── Per-rate log collection ───────────────────────────────────
        log_path = copy_screen_log_for_rate(rate)
        screen_logs.append(log_path)

        # ── Early stop on saturation (both conditions must be true) ────
        avg_exceeded = m["avg_ms"] > AVG_LATENCY_THRESHOLD_MS
        tput_dropped = m["actual_achieved_rps"] > 0 and m["throughput_rps"] < 0.75 * m["actual_achieved_rps"]
        if avg_exceeded and tput_dropped:
            print(
                f"   System saturated at rate {rate} req/s: "
                f"avg latency {m['avg_ms']:.0f}ms > {AVG_LATENCY_THRESHOLD_MS}ms AND "
                f"throughput {m['throughput_rps']:.2f} rps < 75% of offered "
                f"{m['actual_achieved_rps']:.2f} rps; stopping."
            )
            break
        if avg_exceeded:
            print(
                f"   Avg latency {m['avg_ms']:.0f}ms > {AVG_LATENCY_THRESHOLD_MS}ms "
                f"(throughput still OK); continuing."
            )
        if tput_dropped:
            print(
                f"   Throughput {m['throughput_rps']:.2f} rps < 75% of offered "
                f"{m['actual_achieved_rps']:.2f} rps (latency still OK); continuing."
            )

    # ── Final log collection (node logs from last cluster) ────────────
    print("\n[Logs] Collecting node logs from last cluster ...")
    collect_node_logs()

    # ── Concatenate per-rate screen logs for timing analysis ──────────
    combined_log = RESULTS_DIR / "screen.log"
    with open(combined_log, "w") as out:
        for log_path in screen_logs:
            if log_path.exists():
                out.write(log_path.read_text())
    print(f"   Combined screen log -> {combined_log}")

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

    # ── Generate plots and timing analysis ────────────────────────────
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
