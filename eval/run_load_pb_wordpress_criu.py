"""
run_load_pb_wordpress_criu.py — WordPress PB CRIU baseline benchmark.

Runs WordPress (Apache + MySQL) on AR0 with BaselineCriuReplica.java as a
batching HTTP proxy. CRIU checkpoints the MySQL container (not Apache) after
each batch and rsyncs to AR1/AR2.

Uses incremental CRIU (soft-dirty page tracking) for lower checkpoint overhead.

Run from the eval/ directory:
    python3 run_load_pb_wordpress_criu.py [--rates 1,100,200]

Outputs go to eval/results/load_pb_wordpress_criu_<timestamp>/:
    rate<N>.txt  — go-client output per rate point
"""

import argparse
import concurrent.futures
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from run_load_pb_wordpress_common import (
    ADMIN_PASSWORD,
    ADMIN_USER,
    AR_HOSTS,
    REQUIRED_IMAGES,
    IMAGE_SOURCE_HOST,
    CONTROL_PLANE_HOST,
    HTTP_PROXY_PORT,
    check_rest_api,
    create_post,
    enable_rest_auth,
    install_wordpress,
    wait_for_port,
)
from run_load_pb_wordpress_reflex import (
    AVG_LATENCY_THRESHOLD_MS,
    BOTTLENECK_RATES,
    GO_LATENCY_CLIENT,
    LOAD_DURATION_SEC,
    WARMUP_DURATION_SEC,
    SEED_POST_COUNT,
    _parse_go_output,
    _xmlrpc_editpost_payload,
    ensure_docker_images_on_rc,
)

# ── Constants ─────────────────────────────────────────────────────────────────

PRIMARY_HOST = AR_HOSTS[0]
REPLICA_HOSTS = AR_HOSTS[1:]
CRIU_LISTEN_PORT = 2300

# WordPress Docker containers
WP_CONTAINER = "wordpress-criu"
MYSQL_CONTAINER = "wordpress-criu-mysql"
WP_PORT = 8080
MYSQL_PORT = 3306
MYSQL_ROOT_PASSWORD = "supersecret"
MYSQL_DATABASE = "wordpress"

_TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
RESULTS_DIR = Path(__file__).resolve().parent / "results" / f"load_pb_wordpress_criu_{_TIMESTAMP}"
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


# ── Teardown ──────────────────────────────────────────────────────────────────


def teardown_all():
    """Stop and remove containers + CRIU proxy on all nodes."""
    all_hosts = [PRIMARY_HOST] + REPLICA_HOSTS
    print(f"   Stopping containers on {all_hosts} ...")

    def teardown_node(host):
        _ssh(host,
             "docker ps -q | xargs -r docker stop 2>/dev/null || true; "
             "docker ps -aq | xargs -r docker rm -f 2>/dev/null || true; "
             "docker network prune --force > /dev/null 2>&1 || true",
             check=False)
        if host == PRIMARY_HOST:
            _ssh(host,
                 f"PIDS=$(sudo lsof -ti :{CRIU_LISTEN_PORT} 2>/dev/null); "
                 f"[ -n \"$PIDS\" ] && sudo kill -9 $PIDS 2>/dev/null || true",
                 check=False)
            _ssh(host,
                 "sudo pkill -f BaselineCriuReplica 2>/dev/null || true; "
                 "sudo rm -rf /dev/shm/chk /dev/shm/criu-inc 2>/dev/null || true",
                 check=False)
        print(f"   Teardown complete on {host}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        futs = [pool.submit(teardown_node, h) for h in all_hosts]
        for f in futs:
            f.result()


# ── Container setup ───────────────────────────────────────────────────────────


def start_wordpress_containers():
    """Start MySQL + WordPress containers on PRIMARY_HOST."""
    print(f"   Starting MySQL container ({MYSQL_CONTAINER}) ...")
    _ssh(PRIMARY_HOST, f"docker rm -f {MYSQL_CONTAINER} 2>/dev/null || true", check=False)
    mysql_cmd = (
        f"docker run -d --name {MYSQL_CONTAINER} "
        f"--security-opt seccomp=unconfined "
        f"-e MYSQL_ROOT_PASSWORD={MYSQL_ROOT_PASSWORD} "
        f"-e MYSQL_DATABASE={MYSQL_DATABASE} "
        f"-p {MYSQL_PORT}:3306 "
        f"mysql:8.4.0"
    )
    _ssh(PRIMARY_HOST, mysql_cmd)

    print(f"   Waiting for MySQL port {PRIMARY_HOST}:{MYSQL_PORT} ...")
    if not wait_for_port(PRIMARY_HOST, MYSQL_PORT, timeout_sec=120):
        print("ERROR: MySQL did not start in time.")
        sys.exit(1)
    # Wait for MySQL to be fully ready (accepts connections)
    print("   Waiting for MySQL to accept connections ...")
    deadline = time.time() + 120
    while time.time() < deadline:
        result = _ssh(PRIMARY_HOST,
                      f"docker exec {MYSQL_CONTAINER} mysqladmin ping -p{MYSQL_ROOT_PASSWORD} --silent",
                      check=False, capture=True)
        if result.returncode == 0:
            break
        time.sleep(2)
    else:
        print("ERROR: MySQL not accepting connections.")
        sys.exit(1)
    print("   MySQL ready.")

    print(f"   Starting WordPress container ({WP_CONTAINER}) ...")
    _ssh(PRIMARY_HOST, f"docker rm -f {WP_CONTAINER} 2>/dev/null || true", check=False)
    wp_cmd = (
        f"docker run -d --name {WP_CONTAINER} "
        f"--security-opt seccomp=unconfined "
        f"-e WORDPRESS_DB_HOST={PRIMARY_HOST}:{MYSQL_PORT} "
        f"-e WORDPRESS_DB_USER=root "
        f"-e WORDPRESS_DB_PASSWORD={MYSQL_ROOT_PASSWORD} "
        f"-e WORDPRESS_DB_NAME={MYSQL_DATABASE} "
        f"-p {WP_PORT}:80 "
        f"wordpress:6.5.4-apache"
    )
    _ssh(PRIMARY_HOST, wp_cmd)

    print(f"   Waiting for WordPress port {PRIMARY_HOST}:{WP_PORT} ...")
    if not wait_for_port(PRIMARY_HOST, WP_PORT, timeout_sec=120):
        print("ERROR: WordPress did not start in time.")
        sys.exit(1)
    time.sleep(5)
    print("   WordPress containers ready.")


def start_criu_proxy():
    """Start BaselineCriuReplica on PRIMARY_HOST.

    Forwards HTTP to WordPress (Apache) but checkpoints MySQL container.
    """
    print(f"   Starting CRIU proxy (incremental=true, checkpoint={MYSQL_CONTAINER}) ...")
    replicas = " ".join(REPLICA_HOSTS)
    java_cmd = (
        f"nohup sudo java "
        f"-Djdk.httpclient.allowRestrictedHeaders=content-length,connection,host "
        f"-Dcriu.incremental=true "
        f"-Dcriu.checkpoint.container={MYSQL_CONTAINER} "
        f"-Dcriu.batch.size=8 "
        f"-Dcriu.batch.flush.ms=25 "
        f"-Dcriu.listen.port={CRIU_LISTEN_PORT} "
        f"-Dcriu.forward.port={WP_PORT} "
        f"-cp {CRIU_CLASSPATH} "
        f"{CRIU_MAIN_CLASS} {WP_PORT} {WP_CONTAINER} {replicas} "
        f"> /tmp/criu_proxy.log 2>&1 &"
    )
    _ssh(PRIMARY_HOST, java_cmd)
    print(f"   Waiting for CRIU proxy port {PRIMARY_HOST}:{CRIU_LISTEN_PORT} ...")
    if not wait_for_port(PRIMARY_HOST, CRIU_LISTEN_PORT, timeout_sec=60):
        print("ERROR: CRIU proxy did not start in time.")
        result = _ssh(PRIMARY_HOST, "tail -50 /tmp/criu_proxy.log", check=False, capture=True)
        print(f"   CRIU log:\n{result.stdout}")
        sys.exit(1)
    time.sleep(3)
    print("   CRIU proxy ready.")


# ── WordPress setup ───────────────────────────────────────────────────────────


def setup_wordpress():
    """Install WordPress and seed posts, returning the payloads file path."""
    print("   Installing WordPress ...")
    ok = install_wordpress(PRIMARY_HOST, WP_PORT, "wordpress_criu")
    if not ok:
        raise RuntimeError("WordPress installation failed")
    # Allow WordPress to fully finish (auto-updates, DB writes)
    time.sleep(10)

    print("   Enabling REST auth ...")
    if not enable_rest_auth(PRIMARY_HOST):
        raise RuntimeError("REST API auth setup failed")

    print("   Verifying REST API works before seeding ...")
    if not check_rest_api(PRIMARY_HOST, WP_PORT, "wordpress_criu"):
        raise RuntimeError("REST API check failed before seeding")

    print(f"   Seeding {SEED_POST_COUNT} posts ...")
    for i in range(1, SEED_POST_COUNT + 1):
        pid = create_post(PRIMARY_HOST, WP_PORT, "wordpress_criu",
                          f"Seed Post {i}", f"Content for seed post {i}.")
        if not pid:
            print(f"   WARNING: Seed post {i} failed")
        if i % 50 == 0:
            print(f"     Seeded {i}/{SEED_POST_COUNT}")

    # Generate payloads file for wp.editPost
    payloads_file = RESULTS_DIR / "payloads.txt"
    payloads_file.parent.mkdir(parents=True, exist_ok=True)
    with open(payloads_file, "w") as f:
        for post_id in range(1, SEED_POST_COUNT + 1):
            f.write(_xmlrpc_editpost_payload(post_id) + "\n")
    print(f"   Generated {SEED_POST_COUNT} XML-RPC payloads → {payloads_file}")
    return payloads_file


# ── Load helpers ──────────────────────────────────────────────────────────────


def run_load_point(rate, payloads_file):
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    output_file = RESULTS_DIR / f"rate{rate}.txt"
    url = f"http://{PRIMARY_HOST}:{CRIU_LISTEN_PORT}/xmlrpc.php"
    cmd = [
        "go", "run", str(GO_LATENCY_CLIENT),
        "-H", "Content-Type: text/xml",
        "-X", "POST",
        "-payloads-file", str(payloads_file),
        url,
        "placeholder",
        str(LOAD_DURATION_SEC),
        str(rate),
    ]
    env = os.environ.copy()
    env["GO111MODULE"] = "off"
    env["GOWORK"] = "off"
    print(f"   Output → {output_file}")
    with open(output_file, "w") as fh:
        result = subprocess.run(cmd, stdout=fh, stderr=subprocess.STDOUT,
                                env=env, text=True)
    if result.returncode != 0:
        print(f"   WARNING: go client exited {result.returncode}")
    return _parse_go_output(output_file)


def warmup_rate_point(rate, payloads_file):
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    output_file = RESULTS_DIR / f"warmup_rate{rate}.txt"
    url = f"http://{PRIMARY_HOST}:{CRIU_LISTEN_PORT}/xmlrpc.php"
    cmd = [
        "go", "run", str(GO_LATENCY_CLIENT),
        "-H", "Content-Type: text/xml",
        "-X", "POST",
        "-payloads-file", str(payloads_file),
        url,
        "placeholder",
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


# ── Main ──────────────────────────────────────────────────────────────────────


def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--rates", type=str, default=None,
                   help="Comma-separated offered rates (default: BOTTLENECK_RATES)")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()

    if args.rates:
        rates = [int(r.strip()) for r in args.rates.split(",")]
    else:
        rates = BOTTLENECK_RATES

    print("=" * 60)
    print("WordPress CRIU Baseline Benchmark")
    print("=" * 60)
    print(f"  Primary          : {PRIMARY_HOST}")
    print(f"  Replicas         : {REPLICA_HOSTS}")
    print(f"  WordPress        : {WP_CONTAINER} (port {WP_PORT})")
    print(f"  MySQL            : {MYSQL_CONTAINER} (port {MYSQL_PORT})")
    print(f"  CRIU checkpoint  : {MYSQL_CONTAINER} (incremental)")
    print(f"  Rates            : {rates} req/s")
    print(f"  Duration         : {LOAD_DURATION_SEC}s per rate (warmup {WARMUP_DURATION_SEC}s)")
    print(f"  Fresh setup      : per rate point")
    print(f"  Results          : {RESULTS_DIR}")
    print("=" * 60)

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    results = []

    for i, rate in enumerate(rates):
        print(f"\n{'=' * 60}")
        print(f"  Rate point {i+1}/{len(rates)}: {rate} req/s (fresh containers)")
        print(f"{'=' * 60}")

        payloads_file = None
        try:
            # Step 1 — Teardown
            print("\n  [Step 1] Tearing down previous state ...")
            teardown_all()

            # Step 2 — Start WordPress + MySQL
            print("\n  [Step 2] Starting WordPress + MySQL containers ...")
            start_wordpress_containers()

            # Step 3 — Start CRIU proxy (checkpoints MySQL)
            print("\n  [Step 3] Starting CRIU proxy ...")
            start_criu_proxy()

            # Step 4 — Install WordPress + seed posts
            print("\n  [Step 4] Installing WordPress and seeding posts ...")
            payloads_file = setup_wordpress()

            # Step 5 — Warmup
            print(f"\n  [Step 5] Warming up at {rate} req/s for {WARMUP_DURATION_SEC}s ...")
            wm = warmup_rate_point(rate, payloads_file)
            if wm["throughput_rps"] < 0.1 * rate:
                print(
                    f"  SKIP: warmup tput={wm['throughput_rps']:.2f} rps "
                    f"< 10% of {rate} rps — system saturated"
                )
                continue
            print("  Settling 5s after warmup ...")
            time.sleep(5)

            # Step 6 — Measure
            print(f"\n  [Step 6] Measuring at {rate} req/s for {LOAD_DURATION_SEC}s ...")
            m = run_load_point(rate, payloads_file)
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
                print(f"  Avg latency {m['avg_ms']:.0f}ms > {AVG_LATENCY_THRESHOLD_MS}ms; continuing.")

        except Exception as e:
            print(f"  ERROR: Rate {rate} failed: {e}")
            import traceback
            traceback.print_exc()
        finally:
            print(f"\n  [Step 7] Cleaning up rate {rate} ...")
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

    print(f"\n[Done] Results in {RESULTS_DIR}/")
