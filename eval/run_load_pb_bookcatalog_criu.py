"""
run_load_pb_bookcatalog_criu.py — CRIU baseline bookcatalog-nd benchmark.

Runs bookcatalog-nd container on AR0 with BaselineCriuReplica.java as a
batching HTTP proxy that checkpoints the container after each batch and
rsyncs to AR1/AR2.

Run from the eval/ directory:
    python3 run_load_pb_bookcatalog_criu.py [--skip-teardown] [--rates 1,100,200]

Outputs go to eval/results/bookcatalog_criu/:
    rate1.txt  rate100.txt  ...  — go-client output per rate point
"""

import argparse
import concurrent.futures
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from run_load_pb_bookcatalog_common import (
    AR_HOSTS,
    BOOKCATALOG_IMAGE,
    check_app_ready,
    seed_books,
    wait_for_app_ready,
    wait_for_port,
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

# ── Constants ─────────────────────────────────────────────────────────────────

PRIMARY_HOST = AR_HOSTS[0]  # 10.10.1.1
REPLICA_HOSTS = AR_HOSTS[1:]  # 10.10.1.2, 10.10.1.3
CONTAINER_NAME = "bookcatalog-nd"
CONTAINER_PORT = 8080  # host port mapped to container's port 80
CRIU_LISTEN_PORT = 2300

_TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
RESULTS_DIR = Path(__file__).resolve().parent / "results" / f"load_pb_bookcatalog_criu_{_TIMESTAMP}"
SCRIPT_DIR = Path(__file__).resolve().parent

# Java classpath for BaselineCriuReplica
JARS_DIR = Path(__file__).resolve().parent.parent / "jars"
CRIU_CLASSPATH = f"{JARS_DIR}/gigapaxos-1.0.10.jar:{JARS_DIR}/nio-1.2.1.jar"
CRIU_MAIN_CLASS = "edu.umass.cs.xdn.eval.BaselineCriuReplica"


# ── Shell helpers ─────────────────────────────────────────────────────────────


def _ssh(host, cmd, check=True, capture=False):
    return subprocess.run(
        ["ssh", "-o", "StrictHostKeyChecking=no", host, cmd],
        check=check,
        capture_output=capture,
        text=True,
    )


# ── Load test helpers ────────────────────────────────────────────────────────


def _run_load(url, rate, output_file, duration_sec):
    output_file.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "go", "run", str(GO_LATENCY_CLIENT),
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


def run_load_point(url, rate):
    return _run_load(url, rate, RESULTS_DIR / f"rate{rate}.txt", LOAD_DURATION_SEC)


def warmup_rate_point(url, rate):
    m = _run_load(url, rate, RESULTS_DIR / f"warmup_rate{rate}.txt", WARMUP_DURATION_SEC)
    print(f"     [warmup] tput={m['throughput_rps']:.2f} rps  avg={m['avg_ms']:.1f}ms")
    return m


# ── Phase helpers ─────────────────────────────────────────────────────────────


def teardown_all():
    """Stop and remove containers + CRIU proxy on all nodes."""
    all_hosts = [PRIMARY_HOST] + REPLICA_HOSTS
    print(f"   Stopping containers on {all_hosts} ...")

    def teardown_node(host):
        _ssh(host,
             "docker ps -q | xargs -r docker stop 2>/dev/null || true; "
             "docker ps -aq | xargs -r docker rm -f 2>/dev/null || true",
             check=False)
        if host == PRIMARY_HOST:
            _ssh(host,
                 f"PIDS=$(sudo lsof -ti :{CRIU_LISTEN_PORT} 2>/dev/null); "
                 f"[ -n \"$PIDS\" ] && sudo kill -9 $PIDS 2>/dev/null || true",
                 check=False)
            # Kill any Java BaselineCriuReplica process and clean CRIU dirs
            _ssh(host,
                 "sudo pkill -f BaselineCriuReplica 2>/dev/null || true; "
                 "sudo rm -rf /dev/shm/chk /dev/shm/criu-inc 2>/dev/null || true",
                 check=False)
        print(f"   Teardown complete on {host}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        futs = [pool.submit(teardown_node, h) for h in all_hosts]
        for f in futs:
            f.result()


def start_app_container():
    """Start bookcatalog-nd container on PRIMARY_HOST with CRIU-compatible flags."""
    print(f"   Starting {CONTAINER_NAME} on {PRIMARY_HOST}:{CONTAINER_PORT} (ENABLE_WAL=true) ...")
    _ssh(PRIMARY_HOST, f"docker rm -f {CONTAINER_NAME} 2>/dev/null || true", check=False)
    cmd = (
        f"docker run -d --name {CONTAINER_NAME} "
        f"--security-opt seccomp=unconfined "
        f"-e ENABLE_WAL=true "
        f"-p {CONTAINER_PORT}:80 "
        f"{BOOKCATALOG_IMAGE}"
    )
    _ssh(PRIMARY_HOST, cmd)
    print(f"   Waiting for container port {PRIMARY_HOST}:{CONTAINER_PORT} ...")
    if not wait_for_port(PRIMARY_HOST, CONTAINER_PORT, timeout_sec=120):
        print("ERROR: bookcatalog-nd container did not start in time.")
        sys.exit(1)
    time.sleep(5)


def start_criu_proxy():
    """Start BaselineCriuReplica Java process on PRIMARY_HOST with incremental CRIU."""
    print(f"   Starting CRIU proxy on {PRIMARY_HOST}:{CRIU_LISTEN_PORT} (incremental=true) ...")
    replicas = " ".join(REPLICA_HOSTS)
    java_cmd = (
        f"nohup sudo java "
        f"-Djdk.httpclient.allowRestrictedHeaders=content-length,connection,host "
        f"-Dcriu.incremental=true "
        f"-Dcriu.batch.size=8 "
        f"-Dcriu.batch.flush.ms=25 "
        f"-Dcriu.listen.port={CRIU_LISTEN_PORT} "
        f"-Dcriu.forward.port={CONTAINER_PORT} "
        f"-cp {CRIU_CLASSPATH} "
        f"{CRIU_MAIN_CLASS} {CONTAINER_PORT} {CONTAINER_NAME} {replicas} "
        f"> /tmp/criu_proxy.log 2>&1 &"
    )
    _ssh(PRIMARY_HOST, java_cmd)
    print(f"   Waiting for CRIU proxy port {PRIMARY_HOST}:{CRIU_LISTEN_PORT} ...")
    if not wait_for_port(PRIMARY_HOST, CRIU_LISTEN_PORT, timeout_sec=60):
        print("ERROR: CRIU proxy did not start in time.")
        # Print log for debugging
        result = _ssh(PRIMARY_HOST, "tail -50 /tmp/criu_proxy.log", check=False, capture=True)
        print(f"   CRIU log:\n{result.stdout}")
        sys.exit(1)
    time.sleep(3)


# ── Main ──────────────────────────────────────────────────────────────────────


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--skip-teardown", action="store_true",
                   help="Skip container teardown")
    p.add_argument("--skip-app", action="store_true",
                   help="Skip app + proxy start (assume already running)")
    p.add_argument("--skip-seed", action="store_true",
                   help="Skip data seeding (assume already seeded)")
    p.add_argument("--rates", type=str, default=None,
                   help="Comma-separated list of offered rates (req/s) to override default")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()

    # Determine rate list
    if args.rates:
        rates = [int(r.strip()) for r in args.rates.split(",")]
    else:
        rates = BOTTLENECK_RATES

    app_url = f"http://{PRIMARY_HOST}:{CRIU_LISTEN_PORT}/api/books"

    print("=" * 60)
    print("BookCatalog-ND CRIU Baseline Benchmark")
    print("=" * 60)
    print(f"  Primary     : {PRIMARY_HOST}")
    print(f"  Replicas    : {REPLICA_HOSTS}")
    print(f"  Container   : {CONTAINER_NAME} ({BOOKCATALOG_IMAGE})")
    print(f"  CRIU mode   : incremental")
    print(f"  ENABLE_WAL  : yes")
    print(f"  Rates       : {rates} req/s")
    print(f"  Duration    : {LOAD_DURATION_SEC}s per rate (warmup {WARMUP_DURATION_SEC}s)")
    print(f"  Fresh setup : per rate point")
    print(f"  Results     : {RESULTS_DIR}")
    print("=" * 60)

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    results = []

    for i, rate in enumerate(rates):
        print(f"\n{'=' * 60}")
        print(f"  Rate point {i+1}/{len(rates)}: {rate} req/s (fresh container)")
        print(f"{'=' * 60}")

        try:
            # Step 1 — Teardown
            print("\n  [Step 1] Tearing down previous state ...")
            teardown_all()

            # Step 2 — Start app container
            print("\n  [Step 2] Starting bookcatalog-nd container ...")
            start_app_container()

            # Step 3 — Start CRIU proxy (incremental)
            print("\n  [Step 3] Starting CRIU proxy (incremental=true) ...")
            start_criu_proxy()

            # Step 4 — Seed
            print("\n  [Step 4] Seeding books ...")
            if not wait_for_app_ready(PRIMARY_HOST, CRIU_LISTEN_PORT, timeout_sec=120):
                print("  ERROR: App not ready.")
                continue
            for attempt in range(1, 4):
                created = seed_books(PRIMARY_HOST, CRIU_LISTEN_PORT, count=200)
                if created > 0:
                    break
                print(f"  Attempt {attempt} failed, retrying in 10s ...")
                time.sleep(10)
            else:
                print("  ERROR: Failed to seed books after 3 attempts, skipping rate.")
                continue

            # Step 5 — Verify
            print("\n  [Step 5] Verifying app readiness ...")
            if not check_app_ready(PRIMARY_HOST, CRIU_LISTEN_PORT):
                print("  ERROR: App not ready after seeding, skipping rate.")
                continue

            # Step 6 — Warmup
            print(f"\n  [Step 6] Warming up at {rate} req/s for {WARMUP_DURATION_SEC}s ...")
            wm = warmup_rate_point(app_url, rate)
            if wm["throughput_rps"] < 0.1 * rate:
                print(
                    f"  SKIP: warmup tput={wm['throughput_rps']:.2f} rps "
                    f"< 10% of {rate} rps — system saturated"
                )
                continue
            print("  Settling 5s after warmup ...")
            time.sleep(5)

            # Step 7 — Measure
            print(f"\n  [Step 7] Measuring at {rate} req/s for {LOAD_DURATION_SEC}s ...")
            m = run_load_point(app_url, rate)
            row = {"rate_rps": rate, **m}
            results.append(row)
            print(
                f"    tput={m['throughput_rps']:.2f} rps  "
                f"avg={m['avg_ms']:.1f}ms  "
                f"p50={m['p50_ms']:.1f}ms  "
                f"p95={m['p95_ms']:.1f}ms"
            )

            avg_exceeded = m["avg_ms"] > AVG_LATENCY_THRESHOLD_MS
            tput_dropped = m.get("actual_achieved_rps", 0) > 0 and m["throughput_rps"] < 0.75 * m.get("actual_achieved_rps", rate)
            if avg_exceeded and tput_dropped:
                print(
                    f"  System saturated: avg {m['avg_ms']:.0f}ms > {AVG_LATENCY_THRESHOLD_MS}ms AND "
                    f"throughput {m['throughput_rps']:.2f} < 75% offered; stopping."
                )
                break
            if avg_exceeded:
                print(f"  Avg latency {m['avg_ms']:.0f}ms > {AVG_LATENCY_THRESHOLD_MS}ms (throughput OK); continuing.")

        except Exception as e:
            print(f"  ERROR: Rate {rate} failed: {e}")
        finally:
            # Step 8 — Cleanup
            print(f"\n  [Step 8] Cleaning up rate {rate} ...")
            teardown_all()

    # Summary
    print(f"\n{'=' * 60}")
    print("Summary")
    print(f"{'=' * 60}")
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

    # Plot
    print("\nGenerating plots ...")
    r = subprocess.run(
        ["python3", "plot_load_latency.py", "--results-dir", str(RESULTS_DIR)],
        capture_output=True, text=True, cwd=str(SCRIPT_DIR),
    )
    print(r.stdout)
    if r.returncode != 0:
        print(f"   WARNING: plot_load_latency.py failed:\n{r.stderr}")

    print(f"\n[Done] Results in {RESULTS_DIR}/")
