"""
sweep_netty_threads.py — Parameter sweep for Netty boss/worker thread counts.

Tests different bossGroup and workerGroup thread counts to find optimal
configuration for HttpActiveReplica throughput.

IMPORTANT: Each (config, rate) pair gets a FRESH cluster (forceclear, start,
deploy, seed) to avoid residual state contamination.

Run from the eval/ directory:
    python3 -u sweep_netty_threads.py
"""

import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from run_load_pb_bookcatalog_common import (
    AR_HOSTS,
    BOOKCATALOG_YAML,
    CONTROL_PLANE_HOST,
    HTTP_PROXY_PORT,
    SERVICE_NAME,
    SCREEN_LOG,
    XDN_BINARY,
    clear_xdn_cluster,
    detect_primary_via_docker,
    ensure_docker_images_on_rc,
    seed_books,
    start_cluster,
    wait_for_app_ready,
    wait_for_port,
    wait_for_service,
    TIMEOUT_APP_SEC,
    TIMEOUT_PORT_SEC,
)
from run_load_pb_wordpress_reflex import (
    GO_LATENCY_CLIENT,
    LOAD_DURATION_SEC,
    WARMUP_DURATION_SEC,
    _parse_go_output,
)

# ── Config ────────────────────────────────────────────────────────────────────

GP_CONFIG = Path("../conf/gigapaxos.xdn.3way.cloudlab.properties")
RESULTS_BASE = Path(__file__).resolve().parent / "results"
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
RESULTS_DIR = RESULTS_BASE / f"sweep_netty_threads_{TIMESTAMP}"

# Test rates: moderate and high load
TEST_RATES = [2000, 2500]

# Configurations to sweep: (boss_threads, worker_threads, label)
CONFIGS = [
    (0, 0, "default"),          # Netty default: 2*cores = 128 each
    (2, 8, "b2_w8"),
    (2, 16, "b2_w16"),
    (2, 32, "b2_w32"),
    (2, 64, "b2_w64"),
    (1, 4, "b1_w4"),
]


def update_config(boss_threads, worker_threads):
    """Update the GigaPaxos config with boss/worker thread counts."""
    config_text = GP_CONFIG.read_text()

    # Remove old entries if present
    lines = []
    for line in config_text.splitlines():
        if line.startswith("HTTP_AR_BOSS_THREADS=") or line.startswith("HTTP_AR_WORKER_THREADS="):
            continue
        lines.append(line)

    # Add new entries
    lines.append(f"HTTP_AR_BOSS_THREADS={boss_threads}")
    lines.append(f"HTTP_AR_WORKER_THREADS={worker_threads}")

    GP_CONFIG.write_text("\n".join(lines) + "\n")
    print(f"   Config updated: boss={boss_threads}, worker={worker_threads}")


def distribute_jars_and_config():
    """Copy JARs and config to all nodes."""
    for host in AR_HOSTS + [CONTROL_PLANE_HOST]:
        os.system(f"rsync -a ../jars/ {host}:~/xdn/jars/ 2>/dev/null")
        os.system(f"rsync -a ../conf/ {host}:~/xdn/conf/ 2>/dev/null")
    print("   JARs and config distributed.")


def run_load(primary, rate, output_file, duration_sec):
    """Run get_latency_at_rate.go at given rate."""
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
    with open(output_file, "w") as fh:
        result = subprocess.run(cmd, stdout=fh, stderr=subprocess.STDOUT,
                                env=env, text=True)
    if result.returncode != 0:
        print(f"   WARNING: go client exited {result.returncode}")
    return _parse_go_output(output_file)


def setup_fresh_cluster():
    """Full cluster reset: clear, start, deploy, seed. Returns primary host or None."""
    clear_xdn_cluster()
    time.sleep(5)

    print(" > Starting cluster...")
    start_cluster()

    for host in AR_HOSTS:
        if not wait_for_port(host, HTTP_PROXY_PORT, TIMEOUT_PORT_SEC):
            print(f"   FAIL: {host}:{HTTP_PROXY_PORT} not reachable")
            return None

    ensure_docker_images_on_rc()
    time.sleep(5)

    print(" > Deploying service...")
    cmd = (
        f"XDN_CONTROL_PLANE={CONTROL_PLANE_HOST} {XDN_BINARY} "
        f"launch {SERVICE_NAME} --file={BOOKCATALOG_YAML}"
    )
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    print(f"   {result.stdout.strip()}")
    if result.returncode != 0:
        print(f"   FAIL: deploy failed: {result.stderr}")
        return None

    ok, _ = wait_for_service(AR_HOSTS, HTTP_PROXY_PORT, SERVICE_NAME, TIMEOUT_APP_SEC)
    if not ok:
        print("   FAIL: service not ready")
        return None

    primary = detect_primary_via_docker(AR_HOSTS)
    if not primary:
        print("   FAIL: cannot detect primary")
        return None
    print(f"   Primary: {primary}")

    if not wait_for_app_ready(primary, HTTP_PROXY_PORT, SERVICE_NAME, timeout_sec=60):
        print("   FAIL: app not ready")
        return None

    seed_books(primary, HTTP_PROXY_PORT, SERVICE_NAME, count=200)
    time.sleep(2)

    return primary


def run_single_point(boss, worker, label, rate):
    """Run one (config, rate) point with a FRESH cluster."""
    print(f"\n{'='*60}")
    print(f"CONFIG: {label} (boss={boss}, worker={worker}) @ rate={rate}")
    print(f"{'='*60}")

    # Update config and distribute
    update_config(boss, worker)
    distribute_jars_and_config()

    # Fresh cluster
    primary = setup_fresh_cluster()
    if not primary:
        return None

    # Warmup
    print(f" > Warmup at {rate} rps...")
    warmup_file = RESULTS_DIR / f"{label}_warmup_rate{rate}.txt"
    wm = run_load(primary, rate, warmup_file, WARMUP_DURATION_SEC)
    print(f"   [warmup] tput={wm['throughput_rps']:.1f} avg={wm['avg_ms']:.1f}ms")

    # Actual load
    print(f" > Measuring at {rate} rps for {LOAD_DURATION_SEC}s...")
    result_file = RESULTS_DIR / f"{label}_rate{rate}.txt"
    m = run_load(primary, rate, result_file, LOAD_DURATION_SEC)
    print(f"   tput={m['throughput_rps']:.1f} rps  avg={m['avg_ms']:.1f}ms  p99={m['p99_ms']:.1f}ms")

    return m


def main():
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Results: {RESULTS_DIR}")

    all_results = {}  # label -> {rate -> metrics}

    for boss, worker, label in CONFIGS:
        all_results[label] = {}
        for rate in TEST_RATES:
            m = run_single_point(boss, worker, label, rate)
            if m:
                all_results[label][rate] = m

    # Print summary
    print(f"\n{'='*80}")
    print("SUMMARY")
    print(f"{'='*80}")
    header = f"{'Config':<15}"
    for rate in TEST_RATES:
        header += f"  {'rate'+str(rate)+' tput':>14}  {'avg_ms':>8}  {'p99_ms':>8}"
    print(header)
    print("-" * len(header))

    for label, results in all_results.items():
        row = f"{label:<15}"
        for rate in TEST_RATES:
            if rate in results:
                m = results[rate]
                row += f"  {m['throughput_rps']:>14.1f}  {m['avg_ms']:>8.1f}  {m['p99_ms']:>8.1f}"
            else:
                row += f"  {'N/A':>14}  {'N/A':>8}  {'N/A':>8}"
        print(row)

    # Write summary to file
    summary_file = RESULTS_DIR / "summary.txt"
    with open(summary_file, "w") as f:
        f.write(f"Netty Thread Sweep - {TIMESTAMP}\n\n")
        f.write(header + "\n")
        f.write("-" * len(header) + "\n")
        for label, results in all_results.items():
            row = f"{label:<15}"
            for rate in TEST_RATES:
                if rate in results:
                    m = results[rate]
                    row += f"  {m['throughput_rps']:>14.1f}  {m['avg_ms']:>8.1f}  {m['p99_ms']:>8.1f}"
                else:
                    row += f"  {'N/A':>14}  {'N/A':>8}  {'N/A':>8}"
            f.write(row + "\n")
    print(f"\nSummary written to: {summary_file}")

    # Clean up: remove config overrides
    config_text = GP_CONFIG.read_text()
    lines = [l for l in config_text.splitlines()
             if not l.startswith("HTTP_AR_BOSS_THREADS=")
             and not l.startswith("HTTP_AR_WORKER_THREADS=")]
    GP_CONFIG.write_text("\n".join(lines) + "\n")
    print("Config cleaned up (removed thread overrides).")


if __name__ == "__main__":
    main()
