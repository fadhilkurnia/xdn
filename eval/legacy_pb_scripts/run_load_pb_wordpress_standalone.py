import warnings as _w; _w.warn("DEPRECATED: Use run_load_pb_standalone_app.py --app wordpress instead.", DeprecationWarning, stacklevel=2)
"""
run_load_pb_wordpress_standalone.py — Standalone WordPress baseline measurement.

Deploys WordPress + MySQL directly on AR0 (10.10.1.1) at port 8080 (no XDN,
no Paxos, no state-diff capture), then runs the same load sweep as the XDN-PB
bottleneck test to establish a performance baseline.

Run from the eval/ directory (XDN cluster must NOT be running):
    python3 run_load_pb_wordpress_standalone.py

Outputs go to eval/results/standalone/:
    rate1.txt  rate5.txt  rate10.txt  ...  — go-client output per rate
"""

import os
import subprocess
import sys
import time
from pathlib import Path

import requests

from run_load_pb_wordpress_common import (
    ADMIN_PASSWORD,
    ADMIN_USER,
    TEST_POSTS,
    check_xmlrpc,
    create_post,
    install_wordpress,
    validate_posts,
    wait_for_port,
)
from run_load_pb_wordpress_reflex import (
    BOTTLENECK_RATES,
    INTER_RATE_PAUSE_SEC,
    LOAD_DURATION_SEC,
    MEDIAN_LATENCY_THRESHOLD_MS,
    WARMUP_POST_COUNT,
    XMLRPC_NEW_POST_PAYLOAD,
    _parse_go_output,
)

# ── Config ────────────────────────────────────────────────────────────────────

STANDALONE_HOST = "10.10.1.1"
STANDALONE_PORT = 8080
STANDALONE_DIR  = "/tmp/xdn-standalone"   # on AR0, created via SSH

RESULTS_DIR       = Path(__file__).resolve().parent / "results" / "standalone"
GO_LATENCY_CLIENT = Path(__file__).resolve().parent / "get_latency_at_rate.go"

TIMEOUT_WP_SEC = 300   # MySQL init + WP first-boot (no rsync needed)

# Docker Compose definition — same images and credentials as wordpress.yaml
COMPOSE_CONTENT = """\
services:
  database:
    image: mysql:8.4.0
    environment:
      MYSQL_ROOT_PASSWORD: supersecret
      MYSQL_DATABASE: wordpress
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "localhost", "-psupersecret"]
      interval: 5s
      timeout: 5s
      retries: 30
  wordpress:
    image: wordpress:6.5.4-apache
    ports:
      - "8080:80"
    depends_on:
      database:
        condition: service_healthy
    environment:
      WORDPRESS_DB_HOST: database
      WORDPRESS_DB_USER: root
      WORDPRESS_DB_PASSWORD: supersecret
      WORDPRESS_DB_NAME: wordpress
      WORDPRESS_CONFIG_EXTRA: |
        define('FORCE_SSL_ADMIN', false);
        define('FORCE_SSL_LOGIN', false);
"""


# ── Helpers ───────────────────────────────────────────────────────────────────

def clear_standalone():
    """Stop and remove any existing standalone containers on AR0 (idempotent)."""
    print(f"   Stopping any existing standalone containers on {STANDALONE_HOST} ...")
    os.system(
        f"ssh {STANDALONE_HOST} 'cd {STANDALONE_DIR} && "
        f"docker compose down -v --remove-orphans 2>/dev/null || true'"
    )
    os.system(f"ssh {STANDALONE_HOST} 'rm -rf {STANDALONE_DIR} 2>/dev/null || true'")
    # Also kill anything that might be holding port 8080
    os.system(f"ssh {STANDALONE_HOST} 'sudo fuser -k 8080/tcp 2>/dev/null || true'")
    print("   done.")


def start_standalone():
    """Write docker-compose.yml to AR0 via SSH and start containers."""
    os.system(f"ssh {STANDALONE_HOST} 'mkdir -p {STANDALONE_DIR}'")
    result = subprocess.run(
        f"ssh {STANDALONE_HOST} 'cat > {STANDALONE_DIR}/docker-compose.yml'",
        input=COMPOSE_CONTENT, shell=True, capture_output=True, text=True,
    )
    if result.returncode != 0:
        print(f"   ERROR writing compose file: {result.stderr}")
        return False
    ret = os.system(
        f"ssh {STANDALONE_HOST} 'cd {STANDALONE_DIR} && docker compose up -d'"
    )
    return ret == 0


def wait_for_standalone(timeout_sec=300):
    """Wait until port 8080 is open, then wait for HTTP response from WordPress."""
    if not wait_for_port(STANDALONE_HOST, STANDALONE_PORT, timeout_sec // 2):
        return False
    print(f"   Waiting up to {timeout_sec}s for WordPress HTTP readiness ...")
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            r = requests.get(
                f"http://{STANDALONE_HOST}:{STANDALONE_PORT}/",
                timeout=5,
                allow_redirects=True,
            )
            if 200 <= r.status_code < 500:
                print(f"   READY (HTTP {r.status_code})")
                return True
        except Exception:
            pass
        elapsed = int(timeout_sec - (deadline - time.time()))
        print(f"   ... {elapsed}s elapsed, still waiting ...")
        time.sleep(5)
    print("   TIMEOUT")
    return False


def run_standalone_load_point(rate):
    """Run get_latency_at_rate.go at `rate` req/s targeting standalone WordPress.

    Sends no XDN header — pure direct-to-WordPress load.
    Returns a dict with throughput_rps, avg_ms, p50_ms, p90_ms, p95_ms, p99_ms.
    """
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    output_file = RESULTS_DIR / f"rate{rate}.txt"
    url = f"http://{STANDALONE_HOST}:{STANDALONE_PORT}/xmlrpc.php"
    cmd = [
        "go", "run", str(GO_LATENCY_CLIENT),
        "-H", "Content-Type: text/xml",
        "-X", "POST",
        url,
        XMLRPC_NEW_POST_PAYLOAD,
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


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("WordPress Standalone Baseline Measurement")
    print("=" * 60)
    print(f"Target : {STANDALONE_HOST}:{STANDALONE_PORT}  (no XDN, no Paxos)")
    print(f"Results: {RESULTS_DIR}/")

    # Phase 1 — Clean up any leftover containers
    print("\n[Phase 1] Clearing any existing standalone containers ...")
    clear_standalone()

    # Phase 2 — Start WordPress + MySQL directly on AR0
    print("\n[Phase 2] Starting standalone WordPress + MySQL on AR0 ...")
    if not start_standalone():
        print("ERROR: docker compose up failed on AR0.")
        sys.exit(1)

    # Phase 3 — Wait for WordPress HTTP readiness
    print("\n[Phase 3] Waiting for WordPress HTTP readiness ...")
    if not wait_for_standalone(TIMEOUT_WP_SEC):
        print(f"ERROR: WordPress not ready after {TIMEOUT_WP_SEC}s.")
        print(f"       Check: ssh {STANDALONE_HOST} docker compose -f {STANDALONE_DIR}/docker-compose.yml logs")
        sys.exit(1)

    # Phase 4 — Install WordPress (service="" → sends XDN: header, WordPress ignores it)
    print("\n[Phase 4] Installing WordPress ...")
    if not install_wordpress(STANDALONE_HOST, STANDALONE_PORT, service=""):
        print("ERROR: WordPress installation failed.")
        sys.exit(1)
    if not check_xmlrpc(STANDALONE_HOST, STANDALONE_PORT, service=""):
        print("ERROR: XML-RPC endpoint not available.")
        sys.exit(1)

    # Phase 5 — Warm up with seed posts to prime MySQL caches
    print(f"\n[Phase 5] Warming up with {WARMUP_POST_COUNT} seed posts ...")
    warm_titles = [
        ("Warmup Post 1", "Seed post to prime MySQL caches."),
        ("Warmup Post 2", "Second seed post."),
        ("Warmup Post 3", "Third seed post."),
        ("Warmup Post 4", "Fourth seed post."),
        ("Warmup Post 5", "Fifth seed post."),
    ]
    seed_ids = []
    for title, content in warm_titles[:WARMUP_POST_COUNT]:
        pid = create_post(STANDALONE_HOST, STANDALONE_PORT, "", title, content)
        if pid:
            seed_ids.append(pid)
        else:
            print(f"   WARNING: seed post '{title}' failed, continuing anyway")
    print(f"   Warmup complete: {len(seed_ids)}/{WARMUP_POST_COUNT} posts created")
    if seed_ids:
        validate_posts(STANDALONE_HOST, STANDALONE_PORT, "", seed_ids)

    # Phase 6 — Load sweep (same rates/duration as XDN-PB bottleneck test)
    print(f"\n[Phase 6] Load sweep at {BOTTLENECK_RATES} req/s ...")
    print(f"   Workload : POST /xmlrpc.php (wp.newPost) — no XDN header")
    print(f"   Duration : {LOAD_DURATION_SEC}s per rate")
    print(f"   Target   : {STANDALONE_HOST}:{STANDALONE_PORT}")

    results = []
    for i, rate in enumerate(BOTTLENECK_RATES):
        if i > 0 and INTER_RATE_PAUSE_SEC > 0:
            print(f"   Pausing {INTER_RATE_PAUSE_SEC}s for server drain ...")
            time.sleep(INTER_RATE_PAUSE_SEC)
        print(f"\n   -> rate={rate} req/s (duration={LOAD_DURATION_SEC}s) ...")
        m = run_standalone_load_point(rate)
        row = {"rate_rps": rate, **m}
        results.append(row)
        print(
            f"     tput={m['throughput_rps']:.2f} rps  "
            f"avg={m['avg_ms']:.1f}ms  "
            f"p50={m['p50_ms']:.1f}ms  "
            f"p90={m['p90_ms']:.1f}ms  "
            f"p95={m['p95_ms']:.1f}ms"
        )
        if m["p50_ms"] > MEDIAN_LATENCY_THRESHOLD_MS:
            print(
                f"   Median latency {m['p50_ms']:.0f}ms > {MEDIAN_LATENCY_THRESHOLD_MS}ms "
                f"threshold — system saturated, stopping sweep early."
            )
            break

    # Phase 7 — Summary
    print("\n[Phase 7] Summary")
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
    print("       Run: python3 plot_comparison.py  to compare with XDN-PB results")
