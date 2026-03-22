"""
run_load_pb_wordpress_semisync.py — MySQL semi-sync replication WordPress benchmark.

Sets up MySQL primary on 10.10.1.1 with two semi-sync replicas on .2 and .3
using GTID-based replication (rpl_semi_sync_source_wait_for_replica_count=1).
WordPress runs on .1 connected to the local MySQL primary. Runs the same
wp.newPost XML-RPC workload as the XDN PB benchmark for direct comparison.

Run from the eval/ directory:
    python3 run_load_pb_wordpress_semisync.py [--skip-teardown] [--skip-mysql]

Outputs go to eval/results/semisync/:
    rate1.txt  rate5.txt  ...  — go-client output per rate point
"""

import argparse
import concurrent.futures
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import run_load_pb_wordpress_common as _wpmod
from run_load_pb_wordpress_common import (
    ADMIN_PASSWORD,
    ADMIN_USER,
    check_rest_api,
    create_post,
    enable_rest_auth,
    install_wordpress,
    validate_posts,
    wait_for_port,
)
from run_load_pb_wordpress_reflex import (
    AVG_LATENCY_THRESHOLD_MS,
    BOTTLENECK_RATES,
    GO_LATENCY_CLIENT,
    INTER_RATE_PAUSE_SEC,
    LOAD_DURATION_SEC,
    SEED_POST_COUNT,
    WARMUP_DURATION_SEC,
    _parse_go_output,
    _xmlrpc_editpost_payload,
)

# ── Constants ─────────────────────────────────────────────────────────────────

PRIMARY_HOST  = "10.10.1.1"
REPLICA_HOSTS = ["10.10.1.2", "10.10.1.3"]
WP_PORT       = 2300
MYSQL_PORT    = 3306

MYSQL_ROOT_PASS = "supersecret"
MYSQL_DB        = "wordpress"
REPL_USER       = "repl"
REPL_PASS       = "replpass"

# Semi-sync timeout: 30s prevents premature async fallback under load.
# If no replica ACKs within this window the primary falls back to async
# rather than blocking writes indefinitely.
SEMISYNC_TIMEOUT_MS = 30_000

RESULTS_BASE  = Path(__file__).resolve().parent / "results"
RESULTS_DIR   = None  # set dynamically in __main__ with timestamp
SCRIPT_DIR    = Path(__file__).resolve().parent
URLS_FILE     = None  # kept for backwards compat
PAYLOADS_FILE = None  # XML-RPC payloads file (set dynamically)

# ── Shell helpers ──────────────────────────────────────────────────────────────

def _ssh(host, cmd, check=True, capture=False):
    return subprocess.run(
        ["ssh", "-o", "StrictHostKeyChecking=no", host, cmd],
        check=check,
        capture_output=capture,
        text=True,
    )


def _mysql(host, sql, user="root", password=None):
    """Run SQL on the mysql-primary container on `host`."""
    if password is None:
        password = MYSQL_ROOT_PASS
    return _ssh(
        host,
        f'docker exec mysql-primary mysql -u{user} -p{password} -e "{sql}"',
        capture=True,
    )


def _mysql_replica(host, container, sql, user="root"):
    """Run SQL on a replica container on `host`."""
    return _ssh(
        host,
        f'docker exec {container} mysql -u{user} -p{MYSQL_ROOT_PASS} -e "{sql}"',
        capture=True,
    )


# ── Load test helpers ──────────────────────────────────────────────────────────

def _run_load(url, rate, output_file, duration_sec):
    output_file.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "go", "run", str(GO_LATENCY_CLIENT),
        "-H", "Content-Type: text/xml",
        "-X", "POST",
        "-payloads-file", str(PAYLOADS_FILE),
        url,
        "placeholder",
        str(duration_sec),
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


def run_load_point(url, rate, duration_sec=LOAD_DURATION_SEC):
    return _run_load(url, rate, RESULTS_DIR / f"rate{rate}.txt", duration_sec)


def warmup_rate_point(url, rate, warmup_duration_sec=WARMUP_DURATION_SEC):
    m = _run_load(url, rate, RESULTS_DIR / f"warmup_rate{rate}.txt", warmup_duration_sec)
    print(f"     [warmup] tput={m['throughput_rps']:.2f} rps  avg={m['avg_ms']:.1f}ms")
    return m


# ── Phase helpers ──────────────────────────────────────────────────────────────

def teardown_all():
    """Stop and remove MySQL + WordPress containers on all three nodes."""
    all_hosts = [PRIMARY_HOST] + REPLICA_HOSTS
    print(f"   Stopping containers on {all_hosts} ...")

    def teardown_node(host):
        # Stop ALL running Docker containers (catches XDN Docker Compose containers
        # that may still be bound to WP_PORT from a previous PB run).
        # Then kill any remaining non-Docker process occupying WP_PORT.
        _ssh(host,
             "docker ps -q | xargs -r docker stop 2>/dev/null || true; "
             "docker ps -aq | xargs -r docker rm -f 2>/dev/null || true",
             check=False)
        if host == PRIMARY_HOST:
            _ssh(host,
                 # Kill non-Docker processes (e.g. XDN/GigaPaxos Java running as root) on WP_PORT
                 f"PIDS=$(sudo lsof -ti :{WP_PORT} 2>/dev/null); "
                 f"[ -n \"$PIDS\" ] && sudo kill -9 $PIDS 2>/dev/null || true",
                 check=False)
        _ssh(host,
             "docker stop mysql-primary mysql-replica wordpress 2>/dev/null || true; "
             "docker rm -f mysql-primary mysql-replica wordpress 2>/dev/null || true",
             check=False)
        print(f"   Teardown complete on {host}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        futs = [pool.submit(teardown_node, h) for h in all_hosts]
        for f in futs:
            f.result()


def start_mysql_primary():
    """Start MySQL 8.4.0 primary with semi-sync source plugin on 10.10.1.1.

    Semi-sync variables use --loose- prefix so MySQL ignores them during the
    --initialize phase (when the plugin isn't loaded yet) but respects them
    at normal startup once the plugin is active.
    """
    print(f"   Starting MySQL primary on {PRIMARY_HOST} ...")
    cmd = (
        "docker run -d --name mysql-primary "
        f"-e MYSQL_ROOT_PASSWORD={MYSQL_ROOT_PASS} "
        f"-e MYSQL_DATABASE={MYSQL_DB} "
        f"-p {MYSQL_PORT}:{MYSQL_PORT} "
        "mysql:8.4.0 "
        "--server-id=1 "
        "--log-bin=mysql-bin "
        "--binlog-format=ROW "
        "--gtid-mode=ON "
        "--enforce-gtid-consistency=ON "
        "--plugin-load-add=semisync_source.so "
        "--loose-rpl_semi_sync_source_enabled=ON "
        "--loose-rpl_semi_sync_source_wait_for_replica_count=1 "
        f"--loose-rpl_semi_sync_source_timeout={SEMISYNC_TIMEOUT_MS}"
    )
    _ssh(PRIMARY_HOST, cmd)
    print(f"   Waiting for MySQL primary port {PRIMARY_HOST}:{MYSQL_PORT} ...")
    if not wait_for_port(PRIMARY_HOST, MYSQL_PORT, timeout_sec=240):
        print("ERROR: MySQL primary did not start in time.")
        sys.exit(1)
    # Allow MySQL to fully initialize (InnoDB recovery, grant tables)
    time.sleep(10)


def create_replication_user():
    """Create the replication user on the primary."""
    print("   Creating replication user ...")
    sqls = [
        f"CREATE USER IF NOT EXISTS '{REPL_USER}'@'%' IDENTIFIED BY '{REPL_PASS}'",
        f"GRANT REPLICATION SLAVE ON *.* TO '{REPL_USER}'@'%'",
        "FLUSH PRIVILEGES",
    ]
    for sql in sqls:
        result = _mysql(PRIMARY_HOST, sql)
        if result.returncode != 0:
            print(f"   ERROR running SQL: {sql}\n{result.stderr}")
            sys.exit(1)
    print("   Replication user created.")


def start_mysql_replicas():
    """Start MySQL replicas on .2 and .3 in parallel, then configure replication."""
    def start_replica(host, server_id):
        print(f"   Starting MySQL replica (server-id={server_id}) on {host} ...")
        cmd = (
            f"docker run -d --name mysql-replica "
            f"-e MYSQL_ROOT_PASSWORD={MYSQL_ROOT_PASS} "
            f"-p {MYSQL_PORT}:{MYSQL_PORT} "
            "mysql:8.4.0 "
            f"--server-id={server_id} "
            "--log-bin=mysql-bin "
            "--binlog-format=ROW "
            "--gtid-mode=ON "
            "--enforce-gtid-consistency=ON "
            "--read-only=ON "
            "--plugin-load-add=semisync_replica.so "
            "--loose-rpl_semi_sync_replica_enabled=ON"
        )
        _ssh(host, cmd)
        if not wait_for_port(host, MYSQL_PORT, timeout_sec=240):
            raise RuntimeError(f"MySQL replica on {host} did not start in time.")
        # Wait for InnoDB to fully initialize before connecting
        time.sleep(10)
        # Configure and start replication
        change_sql = (
            "CHANGE REPLICATION SOURCE TO "
            f"SOURCE_HOST='{PRIMARY_HOST}', "
            f"SOURCE_PORT={MYSQL_PORT}, "
            f"SOURCE_USER='{REPL_USER}', "
            f"SOURCE_PASSWORD='{REPL_PASS}', "
            "SOURCE_AUTO_POSITION=1, "
            "GET_SOURCE_PUBLIC_KEY=1"  # needed for caching_sha2_password without SSL
        )
        result = _ssh(
            host,
            f'docker exec mysql-replica mysql -uroot -p{MYSQL_ROOT_PASS} -e "{change_sql}"',
            capture=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"CHANGE REPLICATION SOURCE failed on {host}: {result.stderr}")
        result = _ssh(
            host,
            f"docker exec mysql-replica mysql -uroot -p{MYSQL_ROOT_PASS} -e 'START REPLICA'",
            capture=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"START REPLICA failed on {host}: {result.stderr}")
        print(f"   Replica started on {host}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        futs = [
            pool.submit(start_replica, REPLICA_HOSTS[0], 2),
            pool.submit(start_replica, REPLICA_HOSTS[1], 3),
        ]
        for f in futs:
            f.result()


def wait_for_replicas_sync(timeout_sec=120):
    """Poll SHOW REPLICA STATUS until IO and SQL threads are both running."""
    print("   Waiting for replicas to sync ...")
    deadline = time.time() + timeout_sec
    for host in REPLICA_HOSTS:
        while time.time() < deadline:
            result = _ssh(
                host,
                f"docker exec mysql-replica mysql -uroot -p{MYSQL_ROOT_PASS} "
                f"-e 'SHOW REPLICA STATUS\\G'",
                capture=True, check=False,
            )
            io_run  = "Replica_IO_Running: Yes"  in result.stdout
            sql_run = "Replica_SQL_Running: Yes" in result.stdout
            if io_run and sql_run:
                print(f"   {host}: Replica_IO_Running=Yes, Replica_SQL_Running=Yes")
                break
            print(f"   {host}: waiting for IO/SQL threads ...")
            time.sleep(5)
        else:
            print(f"   WARNING: replica on {host} may not be fully synced — check status")

    # Log semi-sync status on primary
    result = _mysql(
        PRIMARY_HOST,
        "SHOW STATUS LIKE 'Rpl_semi_sync_source%'",
    )
    if result.returncode == 0:
        print(f"   Semi-sync status:\n{result.stdout}")


def tune_mysql_semisync():
    """Tune MySQL InnoDB settings on the primary for write-heavy workloads.

    Increases buffer pool to 1GB (from default 128MB) to avoid page eviction
    during sustained high write rates, and reduces background I/O to minimize
    interference with foreground queries.
    """
    print("   Tuning MySQL InnoDB settings on primary ...")
    tune_sql = (
        "SET GLOBAL innodb_buffer_pool_size=1073741824; "       # 1 GB
        "SET GLOBAL innodb_change_buffer_max_size=50; "          # allow more change buffering
        "SET GLOBAL innodb_max_dirty_pages_pct=90; "             # delay page flush
        "SET GLOBAL innodb_max_dirty_pages_pct_lwm=80; "         # high-water mark before eager flush
        "SET GLOBAL innodb_io_capacity=200; "                    # limit background I/O
        "SET GLOBAL innodb_io_capacity_max=400; "                # cap burst I/O
        "SET GLOBAL innodb_lru_scan_depth=256; "                 # reduce per-second page cleaner work
    )
    result = _mysql(PRIMARY_HOST, tune_sql)
    if result.returncode == 0:
        print("   MySQL InnoDB tuned successfully")
    else:
        print(f"   WARNING: MySQL tuning failed: {result.stderr.strip()}")
    time.sleep(2)


def start_wordpress():
    """Start WordPress 6.5.4-apache on 10.10.1.1 connected to local MySQL primary."""
    print(f"   Starting WordPress on {PRIMARY_HOST}:{WP_PORT} ...")
    # Remove any previously exited container with the same name
    _ssh(PRIMARY_HOST, "docker rm -f wordpress 2>/dev/null || true", check=False)
    # Use PRIMARY_HOST IP (not 127.0.0.1) so the container can reach MySQL
    # published on the host's external interface.
    wp_config_extra = (
        "define('DISABLE_WP_CRON', true); "
        "define('AUTOMATIC_UPDATER_DISABLED', true); "
        "define('WP_AUTO_UPDATE_CORE', false);"
    )
    cmd = (
        "docker run -d --name wordpress "
        f"-e WORDPRESS_DB_HOST={PRIMARY_HOST}:{MYSQL_PORT} "
        "-e WORDPRESS_DB_USER=root "
        f"-e WORDPRESS_DB_PASSWORD={MYSQL_ROOT_PASS} "
        f"-e WORDPRESS_DB_NAME={MYSQL_DB} "
        f"-e WORDPRESS_CONFIG_EXTRA=\"{wp_config_extra}\" "
        f"-p {WP_PORT}:80 "
        "wordpress:6.5.4-apache"
    )
    _ssh(PRIMARY_HOST, cmd)
    print(f"   Waiting for WordPress port {PRIMARY_HOST}:{WP_PORT} ...")
    if not wait_for_port(PRIMARY_HOST, WP_PORT, timeout_sec=120):
        print("ERROR: WordPress did not start in time.")
        sys.exit(1)
    # Allow WordPress + Apache to fully initialize
    time.sleep(5)


# ── Main ──────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--skip-teardown", action="store_true",
                   help="Skip container teardown (assume clean state)")
    p.add_argument("--skip-mysql",    action="store_true",
                   help="Skip MySQL primary/replica setup (assume already running)")
    p.add_argument("--skip-wp",       action="store_true",
                   help="Skip WordPress container start (assume already running)")
    p.add_argument("--skip-install",  action="store_true",
                   help="Skip WordPress installation form submission (assume already installed)")
    p.add_argument("--skip-seed",     action="store_true",
                   help="Skip seeding warmup posts (assume already seeded)")
    p.add_argument("--rates", type=str, default=None,
                   help="Comma-separated list of offered rates (req/s) to override default")
    p.add_argument("--duration", type=int, default=LOAD_DURATION_SEC,
                   help=f"Measurement duration per rate point in seconds (default: {LOAD_DURATION_SEC})")
    p.add_argument("--warmup-duration", type=int, default=WARMUP_DURATION_SEC,
                   help=f"Warmup duration per rate point in seconds (default: {WARMUP_DURATION_SEC})")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()

    # Compute timestamped results directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    RESULTS_DIR = RESULTS_BASE / f"load_pb_wordpress_semisync_{timestamp}"
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    URLS_FILE = RESULTS_DIR / "editpost_urls.txt"
    PAYLOADS_FILE = RESULTS_DIR / "xmlrpc_payloads.txt"

    print("=" * 60)
    print("WordPress MySQL Semi-Sync Replication Benchmark")
    print("=" * 60)

    # Phase 1 — Teardown
    if args.skip_teardown:
        print("\n[Phase 1] Skipping teardown (--skip-teardown)")
    else:
        print("\n[Phase 1] Tearing down existing containers ...")
        teardown_all()

    # Phase 2 — MySQL primary
    if args.skip_mysql:
        print("\n[Phase 2] Skipping MySQL primary setup (--skip-mysql)")
    else:
        print("\n[Phase 2] Starting MySQL primary ...")
        start_mysql_primary()

        # Phase 3 — Replication user
        print("\n[Phase 3] Creating replication user on primary ...")
        create_replication_user()

        # Phase 4 — MySQL replicas
        print("\n[Phase 4] Starting MySQL replicas on .2 and .3 ...")
        start_mysql_replicas()

        # Phase 5 — Wait for sync
        print("\n[Phase 5] Waiting for replica sync ...")
        wait_for_replicas_sync()

    # Phase 6 — WordPress
    if args.skip_wp:
        print("\n[Phase 6] Skipping WordPress start (--skip-wp)")
    else:
        print("\n[Phase 6] Starting WordPress ...")
        start_wordpress()

    wp_url = f"http://{PRIMARY_HOST}:{WP_PORT}/xmlrpc.php"

    # Phase 7 — Install WordPress
    if args.skip_install:
        print("\n[Phase 7] Skipping WordPress install (--skip-install)")
    else:
        print("\n[Phase 7] Installing WordPress ...")
        ok = install_wordpress(PRIMARY_HOST, WP_PORT, "wordpress_semisync")
        if not ok:
            print("ERROR: WordPress installation failed.")
            sys.exit(1)

    # Phase 8 — Enable REST API auth + check availability
    print("\n[Phase 8] Enabling REST API auth ...")
    if not enable_rest_auth(PRIMARY_HOST):
        print("ERROR: REST API auth setup failed.")
        sys.exit(1)
    if not check_rest_api(PRIMARY_HOST, WP_PORT, "wordpress_semisync"):
        print("ERROR: REST API not available.")
        sys.exit(1)

    # Phase 8b — Tune MySQL InnoDB
    print("\n[Phase 8b] Tuning MySQL InnoDB settings ...")
    tune_mysql_semisync()

    # Phase 9 — Seed posts for editPost workload
    if args.skip_seed:
        print(f"\n[Phase 9] Skipping post seeding (--skip-seed)")
        print(f"   Assuming {PAYLOADS_FILE} already exists.")
    else:
        print(f"\n[Phase 9] Seeding {SEED_POST_COUNT} posts for editPost workload ...")
        seed_ids = []
        for i in range(SEED_POST_COUNT):
            title = f"Seed Post {i + 1}"
            content = f"Seed content {i + 1}."
            pid = None
            for attempt in range(1, 4):
                try:
                    pid = create_post(PRIMARY_HOST, WP_PORT, "wordpress_semisync",
                                      title, content)
                    if pid:
                        break
                except Exception as e:
                    print(f"   WARNING: attempt {attempt} failed ({e}), retrying ...")
                    time.sleep(5)
            if pid:
                seed_ids.append(pid)
            else:
                print(f"   WARNING: seed post '{title}' failed, continuing")
        print(f"   Seeded {len(seed_ids)}/{SEED_POST_COUNT} posts")

        # Phase 9b — Write XML-RPC payloads file
        print(f"\n[Phase 9b] Writing {len(seed_ids)} XML-RPC payloads → {PAYLOADS_FILE} ...")
        RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        with open(PAYLOADS_FILE, "w") as fh:
            for pid in seed_ids:
                fh.write(_xmlrpc_editpost_payload(pid) + "\n")
        print(f"   Done: {len(seed_ids)} payloads written.")

    # Determine rate list and durations
    if args.rates:
        rates = [int(r.strip()) for r in args.rates.split(",")]
    else:
        rates = BOTTLENECK_RATES
    load_duration = args.duration
    warmup_duration = args.warmup_duration

    # Phase 10 — Load sweep
    print(f"\n[Phase 10] Load sweep at {rates} req/s ...")
    print(f"   URL      : {wp_url}")
    print(f"   Duration : {load_duration}s per rate (warmup {warmup_duration}s)")
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    results = []
    for i, rate in enumerate(rates):
        if i > 0 and INTER_RATE_PAUSE_SEC > 0:
            print(f"   Pausing {INTER_RATE_PAUSE_SEC}s between rate points ...")
            time.sleep(INTER_RATE_PAUSE_SEC)

        print(f"\n   -> rate={rate} req/s (warmup {warmup_duration}s) ...")
        wm = warmup_rate_point(wp_url, rate, warmup_duration)
        if wm["throughput_rps"] < 0.1 * rate:
            print(
                f"   SKIP: warmup tput={wm['throughput_rps']:.2f} rps "
                f"< 10% of {rate} rps — system saturated"
            )
            continue
        print("   Settling 5s after warmup ...")
        time.sleep(5)

        # Log semi-sync status before each measurement
        status = _mysql(
            PRIMARY_HOST,
            "SHOW STATUS LIKE 'Rpl_semi_sync_source_status'",
        )
        if status.returncode == 0:
            line = [l for l in status.stdout.splitlines() if "Rpl_semi_sync" in l]
            if line:
                print(f"   Semi-sync: {line[0]}")

        print(f"   -> rate={rate} req/s (measuring {load_duration}s) ...")
        m = run_load_point(wp_url, rate, load_duration)
        row = {"rate_rps": rate, **m}
        results.append(row)
        print(
            f"     tput={m['throughput_rps']:.2f} rps  "
            f"avg={m['avg_ms']:.1f}ms  "
            f"p50={m['p50_ms']:.1f}ms  "
            f"p95={m['p95_ms']:.1f}ms"
        )
        if m["avg_ms"] > AVG_LATENCY_THRESHOLD_MS:
            print(
                f"   Avg latency {m['avg_ms']:.0f}ms > {AVG_LATENCY_THRESHOLD_MS}ms — stopping."
            )
            break
        if m["actual_achieved_rps"] > 0 and m["throughput_rps"] < 0.8 * m["actual_achieved_rps"]:
            print(
                f"   Throughput {m['throughput_rps']:.2f} rps < 80% of offered "
                f"{m['actual_achieved_rps']:.2f} rps — system saturated, stopping sweep early."
            )
            break

    # Write CSV
    import csv
    csv_path = RESULTS_DIR / "semisync_load_results.csv"
    fieldnames = ["rate_rps", "achieved_rps", "throughput_rps", "avg_ms", "p50_ms", "p90_ms", "p95_ms", "p99_ms"]
    with open(csv_path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in results:
            writer.writerow({
                "rate_rps": row["rate_rps"],
                "achieved_rps": row.get("actual_achieved_rps", ""),
                "throughput_rps": row.get("throughput_rps", ""),
                "avg_ms": row.get("avg_ms", ""),
                "p50_ms": row.get("p50_ms", ""),
                "p90_ms": row.get("p90_ms", ""),
                "p95_ms": row.get("p95_ms", ""),
                "p99_ms": row.get("p99_ms", ""),
            })
    print(f"\nCSV saved to {csv_path}")

    # Phase 11 — Summary
    print("\n[Phase 11] Summary")
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

    # Phase 12 — Plot
    print("\n[Phase 12] Generating plots ...")
    r = subprocess.run(
        ["python3", "plot_load_latency.py", "--results-dir", str(RESULTS_DIR)],
        capture_output=True, text=True, cwd=str(SCRIPT_DIR),
    )
    print(r.stdout)
    if r.returncode != 0:
        print(f"   WARNING: plot_load_latency.py failed:\n{r.stderr}")

    print(f"\n[Done] Results in {RESULTS_DIR}/")
