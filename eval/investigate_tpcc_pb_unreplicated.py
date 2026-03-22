"""
investigate_tpcc_pb_unreplicated.py — Unreplicated XDN baseline for TPC-C.

Runs the XDN cluster with EMULATE_UNREPLICATED=true, which skips Paxos
propose/commit. This isolates the HTTP proxy + FUSE + execute overhead
from the consensus overhead.

If unreplicated XDN matches PG sync-rep throughput (~53 rps), then Paxos
consensus is the primary bottleneck. If still lower, FUSE/proxy overhead
dominates.

Run from the eval/ directory:
    python3 investigate_tpcc_pb_unreplicated.py

Outputs go to eval/results/load_pb_tpcc_reflex_unreplicated/:
    rate1.txt  rate5.txt  ...  — go-client output per rate
    screen.log                 — copy of the XDN screen log
"""

import os
import subprocess
import sys
import time
from pathlib import Path

from run_load_pb_tpcc_reflex import (
    AR_HOSTS,
    AVG_LATENCY_THRESHOLD_MS,
    CONTROL_PLANE_HOST,
    GO_LATENCY_CLIENT,
    GP_CONFIG,
    HTTP_PROXY_PORT,
    INTER_RATE_PAUSE_SEC,
    LOAD_DURATION_SEC,
    NUM_WAREHOUSES,
    SERVICE_NAME,
    TIMEOUT_PORT_SEC,
    TIMEOUT_SERVICE_SEC,
    TPCC_YAML,
    WARMUP_DURATION_SEC,
    XDN_BINARY,
    _parse_go_output,
    check_tpcc_health,
    clear_xdn_cluster,
    detect_primary_via_docker,
    ensure_docker_images_on_rc,
    generate_payloads_file,
    init_tpcc_db,
    wait_for_port,
    wait_for_service,
)

# ── Override constants ────────────────────────────────────────────────────────

FINESWEEP_RATES = [1, 5, 10, 15, 20, 25, 30, 35, 40, 50, 60, 80, 100]

SCREEN_SESSION = "xdn_tpcc_pb_unreplicated"
SCREEN_LOG_OVERRIDE = f"screen_logs/{SCREEN_SESSION}.log"

RESULTS_DIR = Path(__file__).resolve().parent / "results" / "load_pb_tpcc_reflex_unreplicated"
PAYLOADS_FILE = RESULTS_DIR / "neworder_payloads.txt"

GP_CONFIG_PATH = Path(__file__).resolve().parent.parent / "conf" / "gigapaxos.xdn.3way.cloudlab.properties"

# ── Config helpers ───────────────────────────────────────────────────────────


def _ssh(host, cmd, check=True, capture=False):
    return subprocess.run(
        ["ssh", "-o", "StrictHostKeyChecking=no", host, cmd],
        check=check,
        capture_output=capture,
        text=True,
    )


def set_config(key, value):
    """Set a config property in the GigaPaxos config file using sed."""
    subprocess.run(
        ["sed", "-i", f"s/^{key}=.*/{key}={value}/", str(GP_CONFIG_PATH)],
        check=True,
    )
    print(f"   Config: {key}={value}")


def restore_config():
    """Restore config to default values after experiment."""
    set_config("EMULATE_UNREPLICATED", "false")
    print("   Config restored to defaults.")


# ── Load helpers (rewrite with custom RESULTS_DIR) ───────────────────────────

def run_load_point(primary, rate):
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    output_file = RESULTS_DIR / f"rate{rate}.txt"
    url = f"http://{primary}:{HTTP_PROXY_PORT}/orders"
    cmd = [
        "go", "run", str(GO_LATENCY_CLIENT),
        "-H", f"XDN: {SERVICE_NAME}",
        "-H", "Content-Type: application/json",
        "-X", "POST",
        "-payloads-file", str(PAYLOADS_FILE),
        url,
        '{"w_id": 1, "c_id": 1}',
        str(LOAD_DURATION_SEC),
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


def warmup_rate_point(primary, rate):
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    output_file = RESULTS_DIR / f"warmup_rate{rate}.txt"
    url = f"http://{primary}:{HTTP_PROXY_PORT}/orders"
    cmd = [
        "go", "run", str(GO_LATENCY_CLIENT),
        "-H", f"XDN: {SERVICE_NAME}",
        "-H", "Content-Type: application/json",
        "-X", "POST",
        "-payloads-file", str(PAYLOADS_FILE),
        url,
        '{"w_id": 1, "c_id": 1}',
        str(WARMUP_DURATION_SEC),
        str(rate),
    ]
    env = os.environ.copy()
    env["GO111MODULE"] = "off"
    env["GOWORK"] = "off"
    with open(output_file, "w") as fh:
        subprocess.run(cmd, stdout=fh, stderr=subprocess.STDOUT, env=env, text=True)
    m = _parse_go_output(output_file)
    print(f"     [warmup] tput={m['throughput_rps']:.2f} rps  avg={m['avg_ms']:.1f}ms")
    return m


def copy_screen_log():
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    dest = RESULTS_DIR / "screen.log"
    os.system(f"cp {SCREEN_LOG_OVERRIDE} {dest} 2>/dev/null || true")
    print(f"   Screen log saved -> {dest}")


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("TPC-C Primary-Backup UNREPLICATED Baseline")
    print("=" * 60)

    # Phase 1 — Reset cluster
    print("\n[Phase 1] Force-clearing cluster ...")
    clear_xdn_cluster()

    # Phase 1b — Modify config for unreplicated mode
    print("\n[Phase 1b] Setting EMULATE_UNREPLICATED=true ...")
    set_config("EMULATE_UNREPLICATED", "true")

    try:
        # Phase 2 — Start cluster
        print("\n[Phase 2] Starting XDN cluster (unreplicated mode) ...")
        os.makedirs("screen_logs", exist_ok=True)
        os.system(f"rm -f {SCREEN_LOG_OVERRIDE}")
        cmd = (
            f"screen -L -Logfile {SCREEN_LOG_OVERRIDE} -S {SCREEN_SESSION} -d -m bash -c "
            f"'../bin/gpServer.sh -DgigapaxosConfig={GP_CONFIG} start all; exec bash'"
        )
        print(f"   {cmd}")
        ret = os.system(cmd)
        assert ret == 0, "Failed to start cluster in screen session"
        print(f"   Screen log: {SCREEN_LOG_OVERRIDE}")
        time.sleep(20)

        # Phase 3 — Wait for ARs
        print("\n[Phase 3] Waiting for Active Replicas ...")
        for host in AR_HOSTS:
            if not wait_for_port(host, HTTP_PROXY_PORT, TIMEOUT_PORT_SEC):
                print(f"ERROR: AR at {host}:{HTTP_PROXY_PORT} not ready.")
                copy_screen_log()
                sys.exit(1)

        # Phase 3b — Ensure images on RC
        print("\n[Phase 3b] Ensuring Docker images are present on RC ...")
        ensure_docker_images_on_rc()

        # Phase 4 — Launch TPC-C service
        print("\n[Phase 4] Launching TPC-C service ...")
        cmd = (
            f"XDN_CONTROL_PLANE={CONTROL_PLANE_HOST} {XDN_BINARY} "
            f"launch {SERVICE_NAME} --file={TPCC_YAML}"
        )
        print(f"   {cmd}")
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        print(result.stdout)
        if result.returncode != 0:
            print(f"ERROR: xdn launch failed:\n{result.stderr}")
            sys.exit(1)

        # Phase 5 — Wait for TPC-C readiness
        print("\n[Phase 5] Waiting for TPC-C HTTP readiness ...")
        ok, responding_host = wait_for_service(
            AR_HOSTS, HTTP_PROXY_PORT, SERVICE_NAME, TIMEOUT_SERVICE_SEC
        )
        if not ok:
            print(f"ERROR: TPC-C not ready after {TIMEOUT_SERVICE_SEC}s.")
            copy_screen_log()
            sys.exit(1)

        # Phase 6 — Detect primary
        print("\n[Phase 6] Detecting primary node ...")
        primary = detect_primary_via_docker(AR_HOSTS)
        if not primary:
            print("   WARNING: Could not detect primary, falling back to first AR")
            primary = AR_HOSTS[0]
        print(f"   Primary: {primary}")

        # Phase 7 — Initialize TPC-C database
        print("\n[Phase 7] Initializing TPC-C database ...")
        if not init_tpcc_db(primary, HTTP_PROXY_PORT, SERVICE_NAME, NUM_WAREHOUSES):
            print("ERROR: TPC-C database initialization failed.")
            copy_screen_log()
            sys.exit(1)

        # Phase 7b — Verify health
        print("\n[Phase 7b] Verifying TPC-C health ...")
        if not check_tpcc_health(primary, HTTP_PROXY_PORT, SERVICE_NAME):
            print("ERROR: TPC-C health check failed after init.")
            copy_screen_log()
            sys.exit(1)

        # Phase 8 — Generate payloads file
        print(f"\n[Phase 8] Generating New Order payloads -> {PAYLOADS_FILE} ...")
        generate_payloads_file(PAYLOADS_FILE, NUM_WAREHOUSES)

        # Phase 9 — Load sweep (fine-grained)
        print(f"\n[Phase 9] Load sweep at {FINESWEEP_RATES} req/s (UNREPLICATED) ...")
        print(f"   Workload  : POST /orders (New Order)  XDN:{SERVICE_NAME}")
        print(f"   Duration  : {LOAD_DURATION_SEC}s per rate")
        print(f"   Primary   : {primary}:{HTTP_PROXY_PORT}")
        print(f"   Mode      : UNREPLICATED (no Paxos consensus)")

        results = []
        for i, rate in enumerate(FINESWEEP_RATES):
            if i > 0 and INTER_RATE_PAUSE_SEC > 0:
                print(f"   Pausing {INTER_RATE_PAUSE_SEC}s for server drain ...")
                time.sleep(INTER_RATE_PAUSE_SEC)

            print(f"\n   -> rate={rate} req/s (warmup {WARMUP_DURATION_SEC}s) ...")
            wm = warmup_rate_point(primary, rate)
            if wm["throughput_rps"] < 0.1 * rate:
                print(
                    f"   SKIP: warmup tput={wm['throughput_rps']:.2f} rps "
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
                    f"threshold — stopping sweep early."
                )
                break

        # Phase 10 — Save screen log snapshot
        print("\n[Phase 10] Saving screen log snapshot ...")
        copy_screen_log()

        # Phase 11 — Summary
        print("\n[Phase 11] Summary (UNREPLICATED)")
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

        # Phase 12 — Generate plots
        print("\n[Phase 12] Generating plots ...")
        eval_dir = str(Path(__file__).resolve().parent)
        for script in ["plot_load_latency.py"]:
            r = subprocess.run(
                ["python3", script, "--results-dir", str(RESULTS_DIR)],
                capture_output=True, text=True,
                cwd=eval_dir,
            )
            print(r.stdout)
            if r.returncode != 0:
                print(f"   WARNING: {script} failed:\n{r.stderr}")

    finally:
        # Always restore config
        print("\n[Cleanup] Restoring config ...")
        restore_config()

    print(f"\n[Done] Results in {RESULTS_DIR}/")
