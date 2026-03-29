"""
run_load_pb_distdb_app.py — Consolidated native DB replication baseline benchmark.

Runs load benchmarks against applications backed by their native distributed
database replication (no XDN), for direct comparison with XDN PB throughput.

Supported apps and their auto-selected backends:
  wordpress  -> semisync   (MySQL semi-sync replication)
  tpcc       -> pgsync     (PostgreSQL synchronous streaming replication)
  hotelres   -> replmongo  (MongoDB replica set w:majority)
  bookcatalog -> rqlite    (rqlite Raft consensus)
  synth      -> rqlite     (rqlite Raft consensus)

Usage:
    python3 run_load_pb_distdb_app.py --app wordpress [--rates 100,200,300] [--duration 60]
    python3 run_load_pb_distdb_app.py --app tpcc --skip-db --skip-app
    python3 run_load_pb_distdb_app.py --app hotelres --skip-teardown
    python3 run_load_pb_distdb_app.py --app bookcatalog --rates 100,500,1000

Outputs go to eval/results/<app>_<backend>_<timestamp>/:
    rate100.txt  rate200.txt  ...  -- go-client output per rate point
"""

import argparse
import concurrent.futures
import csv
import json
import os
import random
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

# ── Imports from existing modules ────────────────────────────────────────────

from pb_app_configs import (
    APP_CONFIGS,
    ADMIN_PASSWORD,
    ADMIN_USER,
    _xmlrpc_editpost_payload,
    install_wordpress,
    enable_rest_auth,
    check_rest_api,
    disable_wp_cron,
    seed_bookcatalog,
    check_bookcatalog_ready,
    generate_dummy_data as _hotelres_generate_dummy_data,
    generate_reservation_urls as _hotelres_generate_reservation_urls,
    check_hotelres_ready,
    init_tpcc_db as _tpcc_init_db_via_xdn,
    generate_tpcc_payloads_file,
    check_tpcc_health as _tpcc_check_health_via_xdn,
    make_synth_workload_payload,
)

from pb_app_configs import (
    _wp_create_post,
    seed_bookcatalog as seed_books,
    check_bookcatalog_ready as _bookcatalog_check_app_ready,
)

from pb_common import (
    AR_HOSTS,
    wait_for_port,
)

BOOKCATALOG_IMAGE = "fadhilkurnia/xdn-bookcatalog-nd"

def _bookcatalog_wait_for_app_ready(host, port, service=None, timeout_sec=120):
    """Poll until bookcatalog app responds."""
    import time as _time
    deadline = _time.time() + timeout_sec
    while _time.time() < deadline:
        if _bookcatalog_check_app_ready(host, port, service):
            return True
        _time.sleep(5)
    return False

# Hotel-res constants (previously from run_load_pb_hotelres_common)
HOTELRES_APP_PORT = 5000
HOTELRES_FRONTEND_IMAGE = "fadhilkurnia/xdn-hotel-reservation:latest"
MONGO_IMAGE = "mongo:8.0.5-rc2-noble"
MONGO_PORT = 27017
MONGO_ROOT_PASS = "testing123"
MONGO_ROOT_USER = "root"

from pb_app_configs import (
    check_hotelres_ready as _hotelres_check_app_ready,
    generate_dummy_data as _hotelres_generate_dummy_data_common,
    generate_reservation_urls as _hotelres_gen_urls_common,
)

def _hotelres_wait_for_app_ready(host, port, service=None, timeout_sec=120):
    """Poll until hotelres app responds."""
    import time as _time
    deadline = _time.time() + timeout_sec
    while _time.time() < deadline:
        if _hotelres_check_app_ready(host, port, service):
            return True
        _time.sleep(5)
    return False

SYNTH_IMAGE = "fadhilkurnia/xdn-synth-workload"

# ── Constants ────────────────────────────────────────────────────────────────

PRIMARY_HOST = "10.10.1.1"
REPLICA_HOSTS = ["10.10.1.2", "10.10.1.3"]
ALL_HOSTS = [PRIMARY_HOST] + REPLICA_HOSTS

BOTTLENECK_RATES = [
    1, 100, 200, 300, 400, 500, 600, 700, 800, 900,
    1000, 1200, 1500, 2000, 2500, 3000, 3500, 4000,
    4500, 5000, 6000, 7000,
]

LOAD_DURATION_SEC = 60
WARMUP_DURATION_SEC = 15
INTER_RATE_PAUSE_SEC = 30
AVG_LATENCY_THRESHOLD_MS = 1_000
GO_LATENCY_CLIENT = Path(__file__).resolve().parent / "get_latency_at_rate.go"
RESULTS_BASE = Path(__file__).resolve().parent / "results"
SCRIPT_DIR = Path(__file__).resolve().parent

# App -> backend mapping
APP_BACKEND_MAP = {
    "wordpress": "semisync",
    "tpcc": "pgsync",
    "tpcc-java": "pgsync",
    "hotelres": "replmongo",
    "bookcatalog": "rqlite",
    "synth": "rqlite",
}

# ── semisync constants ───────────────────────────────────────────────────────

WP_PORT = 8094  # avoid port 2300 (XDN proxy)
MYSQL_PORT = 3306
DOCKER_NETWORK = "semisync-net"
MYSQL_ROOT_PASS = "supersecret"
MYSQL_DB = "wordpress"
REPL_USER_MYSQL = "repl"
REPL_PASS_MYSQL = "replpass"
SEMISYNC_TIMEOUT_MS = 30_000

# ── pgsync constants ─────────────────────────────────────────────────────────

PG_PORT = 5432
PG_PASSWORD = "benchpass"
PG_DB = "tpcc"
REPL_USER_PG = "repl"
REPL_PASS_PG = "replpass"
TPCC_APP_PORT = 2300
NUM_WAREHOUSES = 10

# ── replmongo constants ──────────────────────────────────────────────────────

APP_EXPOSED_PORT_MONGO = 2300
RS_NAME = "rs0"
KEYFILE_HOST_PATH = "/tmp/mongo-keyfile"
KEYFILE_CONTAINER_PATH = "/etc/mongo-keyfile"

# ── rqlite constants ─────────────────────────────────────────────────────────

RQLITE_HTTP_PORT = 4001
RQLITE_RAFT_PORT = 4002
RQLITED_BIN = "rqlited"
DATA_DIR_PREFIX = "/tmp"
RQLITE_APP_PORT = 80  # bookcatalog/synth listen on 80 with --network host

# ── Shell helpers ────────────────────────────────────────────────────────────


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


def _psql(host, sql, container="postgres-primary", user="postgres"):
    """Run SQL on a PostgreSQL container on `host`."""
    return _ssh(
        host,
        f'docker exec {container} psql -U {user} -d {PG_DB} -c "{sql}"',
        capture=True,
    )


def _mongosh(host, container, js, check=True):
    """Run JavaScript via mongosh inside a container."""
    return _ssh(
        host,
        f"docker exec {container} mongosh "
        f"--username {MONGO_ROOT_USER} --password {MONGO_ROOT_PASS} "
        f"--authenticationDatabase admin --eval '{js}'",
        capture=True,
        check=check,
    )


def _parse_go_output(path):
    """Parse key:value output from get_latency_at_rate.go."""
    metrics = {}
    for line in open(path):
        if ":" not in line:
            continue
        k, v = line.split(":", 1)
        try:
            metrics[k.strip()] = float(v.strip())
        except ValueError:
            pass
    return {
        "throughput_rps":      metrics.get("actual_throughput_rps", 0.0),
        "actual_achieved_rps": metrics.get("actual_achieved_rate_rps", 0.0),
        "avg_ms":              metrics.get("average_latency_ms", 0.0),
        "p50_ms":              metrics.get("median_latency_ms", 0.0),
        "p90_ms":              metrics.get("p90_latency_ms", 0.0),
        "p95_ms":              metrics.get("p95_latency_ms", 0.0),
        "p99_ms":              metrics.get("p99_latency_ms", 0.0),
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  Backend: semisync (MySQL semi-sync replication)
# ═══════════════════════════════════════════════════════════════════════════════


def teardown_semisync(primary=PRIMARY_HOST, replicas=REPLICA_HOSTS):
    """Stop and remove MySQL + WordPress containers on all three nodes."""
    all_hosts = [primary] + replicas
    print(f"   Stopping containers on {all_hosts} ...")

    def teardown_node(host):
        _ssh(host,
             "docker ps -q | xargs -r docker stop 2>/dev/null || true; "
             "docker ps -aq | xargs -r docker rm -f 2>/dev/null || true",
             check=False)
        if host == primary:
            _ssh(host,
                 f"PIDS=$(sudo lsof -ti :{WP_PORT} 2>/dev/null); "
                 f"[ -n \"$PIDS\" ] && sudo kill -9 $PIDS 2>/dev/null || true",
                 check=False)
        _ssh(host,
             "docker stop mysql-primary mysql-replica wordpress 2>/dev/null || true; "
             "docker rm -f mysql-primary mysql-replica wordpress 2>/dev/null || true; "
             f"docker network rm {DOCKER_NETWORK} 2>/dev/null || true",
             check=False)
        print(f"   Teardown complete on {host}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        futs = [pool.submit(teardown_node, h) for h in all_hosts]
        for f in futs:
            f.result()


def setup_semisync(primary=PRIMARY_HOST, replicas=REPLICA_HOSTS):
    """Start MySQL primary with semi-sync replication and two replicas."""
    # ── Start MySQL primary ──────────────────────────────────────────────
    print(f"   Starting MySQL primary on {primary} ...")
    _ssh(primary, f"docker network create {DOCKER_NETWORK} 2>/dev/null || true", check=False)
    cmd = (
        f"docker run -d --name mysql-primary --network {DOCKER_NETWORK} "
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
    _ssh(primary, cmd)
    print(f"   Waiting for MySQL primary port {primary}:{MYSQL_PORT} ...")
    if not wait_for_port(primary, MYSQL_PORT, timeout_sec=240):
        print("ERROR: MySQL primary did not start in time.")
        sys.exit(1)
    time.sleep(10)

    # ── Create replication user ──────────────────────────────────────────
    print("   Creating replication user ...")
    sqls = [
        f"CREATE USER IF NOT EXISTS '{REPL_USER_MYSQL}'@'%' IDENTIFIED BY '{REPL_PASS_MYSQL}'",
        f"GRANT REPLICATION SLAVE ON *.* TO '{REPL_USER_MYSQL}'@'%'",
        "FLUSH PRIVILEGES",
    ]
    for sql in sqls:
        result = _mysql(primary, sql)
        if result.returncode != 0:
            print(f"   ERROR running SQL: {sql}\n{result.stderr}")
            sys.exit(1)
    print("   Replication user created.")

    # ── Start MySQL replicas ─────────────────────────────────────────────
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
        time.sleep(10)
        change_sql = (
            "CHANGE REPLICATION SOURCE TO "
            f"SOURCE_HOST='{primary}', "
            f"SOURCE_PORT={MYSQL_PORT}, "
            f"SOURCE_USER='{REPL_USER_MYSQL}', "
            f"SOURCE_PASSWORD='{REPL_PASS_MYSQL}', "
            "SOURCE_AUTO_POSITION=1, "
            "GET_SOURCE_PUBLIC_KEY=1"
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
            pool.submit(start_replica, replicas[0], 2),
            pool.submit(start_replica, replicas[1], 3),
        ]
        for f in futs:
            f.result()

    # ── Wait for replicas to sync ────────────────────────────────────────
    print("   Waiting for replicas to sync ...")
    deadline = time.time() + 120
    for host in replicas:
        while time.time() < deadline:
            result = _ssh(
                host,
                f"docker exec mysql-replica mysql -uroot -p{MYSQL_ROOT_PASS} "
                f"-e 'SHOW REPLICA STATUS\\G'",
                capture=True, check=False,
            )
            io_run = "Replica_IO_Running: Yes" in result.stdout
            sql_run = "Replica_SQL_Running: Yes" in result.stdout
            if io_run and sql_run:
                print(f"   {host}: Replica_IO_Running=Yes, Replica_SQL_Running=Yes")
                break
            print(f"   {host}: waiting for IO/SQL threads ...")
            time.sleep(5)
        else:
            print(f"   WARNING: replica on {host} may not be fully synced")

    # Log semi-sync status on primary
    result = _mysql(primary, "SHOW STATUS LIKE 'Rpl_semi_sync_source%'")
    if result.returncode == 0:
        print(f"   Semi-sync status:\n{result.stdout}")


def start_app_semisync(primary=PRIMARY_HOST, app_config=None, app_name="wordpress"):
    """Start WordPress on PRIMARY_HOST connected to local MySQL primary."""
    print(f"   Starting WordPress on {primary}:{WP_PORT} ...")
    _ssh(primary, "docker rm -f wordpress 2>/dev/null || true", check=False)
    wp_config_extra = (
        "define('DISABLE_WP_CRON', true); "
        "define('AUTOMATIC_UPDATER_DISABLED', true); "
        "define('WP_AUTO_UPDATE_CORE', false);"
    )
    cmd = (
        f"docker run -d --name wordpress --network {DOCKER_NETWORK} "
        f"-e WORDPRESS_DB_HOST=mysql-primary:3306 "
        "-e WORDPRESS_DB_USER=root "
        f"-e WORDPRESS_DB_PASSWORD={MYSQL_ROOT_PASS} "
        f"-e WORDPRESS_DB_NAME={MYSQL_DB} "
        f"-e WORDPRESS_CONFIG_EXTRA=\"{wp_config_extra}\" "
        f"-p {WP_PORT}:80 "
        "wordpress:6.5.4-apache"
    )
    _ssh(primary, cmd)
    print(f"   Waiting for WordPress port {primary}:{WP_PORT} ...")
    if not wait_for_port(primary, WP_PORT, timeout_sec=120):
        print("ERROR: WordPress did not start in time.")
        sys.exit(1)
    time.sleep(5)


def tune_mysql_semisync():
    """Tune MySQL InnoDB settings on the primary for write-heavy workloads."""
    print("   Tuning MySQL InnoDB settings on primary ...")
    tune_sql = (
        "SET GLOBAL innodb_buffer_pool_size=1073741824; "
        "SET GLOBAL innodb_change_buffer_max_size=50; "
        "SET GLOBAL innodb_max_dirty_pages_pct=90; "
        "SET GLOBAL innodb_max_dirty_pages_pct_lwm=80; "
        "SET GLOBAL innodb_io_capacity=200; "
        "SET GLOBAL innodb_io_capacity_max=400; "
        "SET GLOBAL innodb_lru_scan_depth=256; "
    )
    result = _mysql(PRIMARY_HOST, tune_sql)
    if result.returncode == 0:
        print("   MySQL InnoDB tuned successfully")
    else:
        print(f"   WARNING: MySQL tuning failed: {result.stderr.strip()}")
    time.sleep(2)


def check_replication_semisync(primary=PRIMARY_HOST):
    """Log semi-sync replication status before measurement."""
    status = _mysql(
        primary,
        "SHOW STATUS LIKE 'Rpl_semi_sync_source_status'",
    )
    if status.returncode == 0:
        line = [l for l in status.stdout.splitlines() if "Rpl_semi_sync" in l]
        if line:
            print(f"   Semi-sync: {line[0]}")


# ═══════════════════════════════════════════════════════════════════════════════
#  Backend: pgsync (PostgreSQL synchronous streaming replication)
# ═══════════════════════════════════════════════════════════════════════════════


def teardown_pgsync(primary=PRIMARY_HOST, replicas=REPLICA_HOSTS):
    """Stop and remove PostgreSQL + TPC-C containers on all three nodes."""
    all_hosts = [primary] + replicas
    print(f"   Stopping containers on {all_hosts} ...")

    def teardown_node(host):
        _ssh(host,
             "docker ps -q | xargs -r docker stop 2>/dev/null || true; "
             "docker ps -aq | xargs -r docker rm -f 2>/dev/null || true",
             check=False)
        if host == primary:
            _ssh(host,
                 f"PIDS=$(sudo lsof -ti :{TPCC_APP_PORT} 2>/dev/null); "
                 f"[ -n \"$PIDS\" ] && sudo kill -9 $PIDS 2>/dev/null || true",
                 check=False)
        _ssh(host,
             "docker stop postgres-primary postgres-replica tpcc-app 2>/dev/null || true; "
             "docker rm -f postgres-primary postgres-replica tpcc-app 2>/dev/null || true; "
             "docker volume rm pgdata 2>/dev/null || true",
             check=False)
        print(f"   Teardown complete on {host}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        futs = [pool.submit(teardown_node, h) for h in all_hosts]
        for f in futs:
            f.result()


def setup_pgsync(primary=PRIMARY_HOST, replicas=REPLICA_HOSTS):
    """Start PostgreSQL primary with synchronous streaming replication."""
    # ── Start PostgreSQL primary ─────────────────────────────────────────
    print(f"   Starting PostgreSQL primary on {primary} ...")
    cmd = (
        "docker run -d --name postgres-primary "
        f"-e POSTGRES_PASSWORD={PG_PASSWORD} "
        f"-e POSTGRES_DB={PG_DB} "
        f"-p {PG_PORT}:{PG_PORT} "
        "--net=host "
        "postgres:17.4-bookworm "
        "-c wal_level=replica "
        "-c max_wal_senders=10 "
        "-c listen_addresses='*' "
        "-c max_connections=200 "
        "-c wal_keep_size=1024"
    )
    _ssh(primary, cmd)
    print(f"   Waiting for PostgreSQL primary port {primary}:{PG_PORT} ...")
    if not wait_for_port(primary, PG_PORT, timeout_sec=240):
        print("ERROR: PostgreSQL primary did not start in time.")
        sys.exit(1)
    time.sleep(10)

    # ── Create replication user ──────────────────────────────────────────
    print("   Creating replication user ...")
    sql = f"CREATE USER {REPL_USER_PG} WITH REPLICATION ENCRYPTED PASSWORD '{REPL_PASS_PG}'"
    result = _ssh(
        primary,
        f"docker exec postgres-primary psql -U postgres -c \"{sql}\"",
        capture=True, check=False,
    )
    if result.returncode != 0:
        if "already exists" in result.stderr or "already exists" in result.stdout:
            print(f"   Replication user already exists, continuing.")
        else:
            print(f"   ERROR running SQL: {sql}\n{result.stderr}")
            sys.exit(1)

    # Append replication entry to pg_hba.conf and reload
    _ssh(
        primary,
        "docker exec postgres-primary bash -c "
        "\"echo 'host replication repl 10.10.1.0/24 md5' >> "
        "/var/lib/postgresql/data/pg_hba.conf\"",
    )
    _ssh(
        primary,
        "docker exec -u postgres postgres-primary pg_ctl reload -D /var/lib/postgresql/data",
    )
    print("   Replication user created and pg_hba.conf updated.")

    # ── Start PostgreSQL replicas ────────────────────────────────────────
    def start_replica(host):
        print(f"   Starting PostgreSQL replica on {host} ...")
        _ssh(host, "docker volume rm pgdata 2>/dev/null || true", check=False)
        _ssh(host, "docker volume create pgdata")
        print(f"   [{host}] Running pg_basebackup from {primary} ...")
        _ssh(
            host,
            f"docker run --rm --net=host "
            f"-e PGPASSWORD={REPL_PASS_PG} "
            f"-v pgdata:/data "
            f"postgres:17.4-bookworm "
            f"pg_basebackup -h {primary} -U {REPL_USER_PG} -D /data -Fp -Xs -R",
        )
        print(f"   [{host}] Starting replica container ...")
        _ssh(
            host,
            f"docker run -d --name postgres-replica "
            f"-v pgdata:/var/lib/postgresql/data "
            f"-p {PG_PORT}:{PG_PORT} "
            f"--net=host "
            f"postgres:17.4-bookworm "
            f"-c max_connections=200",
        )
        if not wait_for_port(host, PG_PORT, timeout_sec=120):
            raise RuntimeError(f"PostgreSQL replica on {host} did not start in time.")
        print(f"   Replica started on {host}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        futs = [pool.submit(start_replica, h) for h in replicas]
        for f in futs:
            f.result()

    # ── Wait for replicas to sync ────────────────────────────────────────
    print("   Waiting for replicas to sync ...")
    deadline = time.time() + 120
    while time.time() < deadline:
        result = _ssh(
            primary,
            "docker exec postgres-primary psql -U postgres -c "
            "\"SELECT client_addr, state, sync_state FROM pg_stat_replication\"",
            capture=True, check=False,
        )
        if result.returncode == 0:
            streaming_count = result.stdout.count("streaming")
            print(f"   pg_stat_replication:\n{result.stdout.strip()}")
            if streaming_count >= 2:
                print(f"   Both replicas in streaming state.")
                break
        print(f"   Waiting for replicas to reach streaming state ...")
        time.sleep(5)
    else:
        print(f"   WARNING: replicas may not be fully synced after 120s")

    # ── Enable synchronous replication ───────────────────────────────────
    print("   Enabling synchronous replication on primary ...")
    sqls = [
        "ALTER SYSTEM SET synchronous_commit = on",
        "ALTER SYSTEM SET synchronous_standby_names = '*'",
    ]
    for sql in sqls:
        result = _ssh(
            primary,
            f"docker exec postgres-primary psql -U postgres -c \"{sql}\"",
            capture=True, check=False,
        )
        if result.returncode != 0:
            print(f"   ERROR running: {sql}\n{result.stderr}")
            sys.exit(1)
    _ssh(primary,
         "docker exec -u postgres postgres-primary pg_ctl reload -D /var/lib/postgresql/data")
    time.sleep(3)
    # Verify synchronous_commit is actually on
    verify = _ssh(
        primary,
        "docker exec postgres-primary psql -U postgres -c "
        "\"SHOW synchronous_commit\"",
        capture=True, check=False,
    )
    print(f"   synchronous_commit: {verify.stdout.strip()}")
    if "on" not in verify.stdout:
        raise RuntimeError("synchronous_commit is NOT 'on' after reload!")
    # Verify at least one sync replica
    check_replication_pgsync(primary)
    print("   Synchronous replication verified and active.")


def start_app_pgsync(primary=PRIMARY_HOST, app_config=None, app_name="tpcc"):
    """Start TPC-C app on PRIMARY_HOST connected to local PostgreSQL."""
    _ssh(primary, "docker rm -f tpcc-app 2>/dev/null || true", check=False)
    if app_name == "tpcc-java":
        image = "fadhilkurnia/xdn-tpcc-java"
        container_port = 80
        env_flags = (
            f"-e DATABASE_URL=postgresql://postgres:{PG_PASSWORD}@{primary}:{PG_PORT}/{PG_DB} "
            f"-e WAREHOUSES=10"
        )
    else:
        image = "fadhilkurnia/xdn-tpcc"
        container_port = 8000
        env_flags = (
            f"-e DATABASE_URL=postgresql://postgres:{PG_PASSWORD}@{primary}:{PG_PORT}/{PG_DB} "
            f'-e GUNICORN_CMD_ARGS="--workers 4 --timeout 120"'
        )
    print(f"   Starting {image} on {primary}:{TPCC_APP_PORT} ...")
    cmd = (
        f"docker run -d --name tpcc-app "
        f"{env_flags} "
        f"-p {TPCC_APP_PORT}:{container_port} "
        f"{image}"
    )
    _ssh(primary, cmd)
    print(f"   Waiting for TPC-C app port {primary}:{TPCC_APP_PORT} ...")
    if not wait_for_port(primary, TPCC_APP_PORT, timeout_sec=120):
        print("ERROR: TPC-C app did not start in time.")
        sys.exit(1)
    time.sleep(5)


def check_replication_pgsync(primary=PRIMARY_HOST):
    """Verify PostgreSQL synchronous replication is active.

    Checks both that replicas are streaming AND that at least one has
    sync_state='sync'. Aborts if replication is async (would produce
    misleadingly high throughput).
    """
    status = _ssh(
        primary,
        "docker exec postgres-primary psql -U postgres -c "
        "\"SELECT client_addr, state, sync_state FROM pg_stat_replication\"",
        capture=True, check=False,
    )
    if status.returncode != 0:
        print(f"   WARNING: could not query pg_stat_replication")
        return

    print(f"   pg_stat_replication:\n{status.stdout.strip()}")
    streaming = [l for l in status.stdout.splitlines() if "streaming" in l]
    sync_replicas = [l for l in status.stdout.splitlines() if "sync" in l and "async" not in l]

    if not streaming:
        raise RuntimeError("No streaming replicas found — replication not active")
    if not sync_replicas:
        raise RuntimeError(
            "No sync replicas found (all async) — synchronous_commit may not "
            "have taken effect. Writes are NOT waiting for replication. "
            "Check ALTER SYSTEM SET synchronous_standby_names and pg_ctl reload."
        )
    print(f"   PG replication: {len(streaming)} streaming, {len(sync_replicas)} sync")


# ═══════════════════════════════════════════════════════════════════════════════
#  Backend: replmongo (MongoDB replica set w:majority)
# ═══════════════════════════════════════════════════════════════════════════════


def teardown_replmongo(primary=PRIMARY_HOST, replicas=REPLICA_HOSTS):
    """Stop and remove MongoDB + hotel-reservation containers on all three nodes."""
    all_hosts = [primary] + replicas
    print(f"   Stopping containers on {all_hosts} ...")

    def teardown_node(host):
        _ssh(host,
             "docker ps -q | xargs -r docker stop 2>/dev/null || true; "
             "docker ps -aq | xargs -r docker rm -f 2>/dev/null || true",
             check=False)
        if host == primary:
            _ssh(host,
                 f"PIDS=$(sudo lsof -ti :{APP_EXPOSED_PORT_MONGO} 2>/dev/null); "
                 f"[ -n \"$PIDS\" ] && sudo kill -9 $PIDS 2>/dev/null || true",
                 check=False)
        _ssh(host,
             "docker stop mongo-primary mongo-secondary hotel-reservation 2>/dev/null || true; "
             "docker rm -f mongo-primary mongo-secondary hotel-reservation 2>/dev/null || true",
             check=False)
        print(f"   Teardown complete on {host}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        futs = [pool.submit(teardown_node, h) for h in all_hosts]
        for f in futs:
            f.result()


def setup_replmongo(primary=PRIMARY_HOST, replicas=REPLICA_HOSTS):
    """Start MongoDB replica set with keyFile auth across three nodes."""
    all_hosts = [primary] + replicas

    # ── Distribute keyFile ───────────────────────────────────────────────
    print("   Generating and distributing MongoDB keyFile ...")
    local_tmp = "/tmp/mongo-keyfile-local"
    subprocess.run(
        f"openssl rand -base64 756 > {local_tmp}",
        shell=True, check=True,
    )
    for host in all_hosts:
        _ssh(host, f"sudo rm -f {KEYFILE_HOST_PATH}", check=False)
        subprocess.run(
            ["scp", "-o", "StrictHostKeyChecking=no",
             local_tmp, f"{host}:{KEYFILE_HOST_PATH}"],
            check=True,
        )
        _ssh(host,
             f"sudo chown 999:999 {KEYFILE_HOST_PATH} && "
             f"sudo chmod 400 {KEYFILE_HOST_PATH}")
    os.remove(local_tmp)
    print("   KeyFile distributed to all nodes.")

    # ── Start MongoDB primary ────────────────────────────────────────────
    print(f"   Starting MongoDB primary on {primary} ...")
    cmd = (
        "docker run -d --name mongo-primary "
        f"-e MONGO_INITDB_ROOT_USERNAME={MONGO_ROOT_USER} "
        f"-e MONGO_INITDB_ROOT_PASSWORD={MONGO_ROOT_PASS} "
        f"-v {KEYFILE_HOST_PATH}:{KEYFILE_CONTAINER_PATH}:ro "
        f"-p {MONGO_PORT}:{MONGO_PORT} "
        f"{MONGO_IMAGE} "
        f"mongod --replSet {RS_NAME} --bind_ip_all "
        f"--keyFile {KEYFILE_CONTAINER_PATH}"
    )
    _ssh(primary, cmd)
    print(f"   Waiting for MongoDB primary port {primary}:{MONGO_PORT} ...")
    if not wait_for_port(primary, MONGO_PORT, timeout_sec=120):
        print("ERROR: MongoDB primary did not start in time.")
        sys.exit(1)
    time.sleep(10)

    # ── Start MongoDB secondaries ────────────────────────────────────────
    def start_secondary(host):
        print(f"   Starting MongoDB secondary on {host} ...")
        cmd = (
            "docker run -d --name mongo-secondary "
            f"-e MONGO_INITDB_ROOT_USERNAME={MONGO_ROOT_USER} "
            f"-e MONGO_INITDB_ROOT_PASSWORD={MONGO_ROOT_PASS} "
            f"-v {KEYFILE_HOST_PATH}:{KEYFILE_CONTAINER_PATH}:ro "
            f"-p {MONGO_PORT}:{MONGO_PORT} "
            f"{MONGO_IMAGE} "
            f"mongod --replSet {RS_NAME} --bind_ip_all "
            f"--keyFile {KEYFILE_CONTAINER_PATH}"
        )
        _ssh(host, cmd)
        if not wait_for_port(host, MONGO_PORT, timeout_sec=120):
            raise RuntimeError(f"MongoDB secondary on {host} did not start in time.")
        time.sleep(5)
        print(f"   MongoDB secondary started on {host}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        futs = [pool.submit(start_secondary, h) for h in replicas]
        for f in futs:
            f.result()

    # ── Initialize replica set ───────────────────────────────────────────
    print("   Initializing replica set ...")
    members = [
        f'{{_id: 0, host: "{primary}:{MONGO_PORT}"}}',
        f'{{_id: 1, host: "{replicas[0]}:{MONGO_PORT}"}}',
        f'{{_id: 2, host: "{replicas[1]}:{MONGO_PORT}"}}',
    ]
    members_str = ", ".join(members)
    js = f'rs.initiate({{_id: "{RS_NAME}", members: [{members_str}]}})'
    result = _mongosh(primary, "mongo-primary", js, check=False)
    print(f"   rs.initiate result: {result.stdout[:300]}")
    if result.returncode != 0:
        print(f"   WARNING: rs.initiate stderr: {result.stderr[:200]}")

    # ── Wait for replica set ready ───────────────────────────────────────
    print("   Waiting for replica set to be ready ...")
    deadline = time.time() + 120
    while time.time() < deadline:
        result = _mongosh(primary, "mongo-primary", "rs.status().ok", check=False)
        stdout = result.stdout.strip()
        if "1" in stdout:
            result2 = _mongosh(
                primary, "mongo-primary",
                "rs.status().members.filter(m => m.stateStr === \"PRIMARY\").length",
                check=False,
            )
            if "1" in result2.stdout.strip():
                print("   Replica set is ready (primary elected)")
                status = _mongosh(
                    primary, "mongo-primary",
                    "rs.status().members.map(m => m.name + \": \" + m.stateStr).join(\", \")",
                    check=False,
                )
                print(f"   Members: {status.stdout.strip()}")
                return
        print(f"   ... waiting for replica set election ...")
        time.sleep(5)
    print(f"   WARNING: replica set may not be fully ready after 120s")


def start_app_replmongo(primary=PRIMARY_HOST, app_config=None, app_name="hotelres"):
    """Start hotel-reservation app on PRIMARY_HOST connected to the replica set."""
    print(f"   Starting hotel-reservation app on {primary}:{APP_EXPOSED_PORT_MONGO} ...")
    _ssh(primary, "docker rm -f hotel-reservation 2>/dev/null || true", check=False)
    mongo_uri = (
        f"mongodb://{MONGO_ROOT_USER}:{MONGO_ROOT_PASS}@"
        f"{primary}:{MONGO_PORT},"
        f"{REPLICA_HOSTS[0]}:{MONGO_PORT},"
        f"{REPLICA_HOSTS[1]}:{MONGO_PORT}"
        f"/?replicaSet={RS_NAME}&w=majority&authSource=admin"
    )
    cmd = (
        "docker run -d --name hotel-reservation "
        f"-e MONGO_URI='{mongo_uri}' "
        f"-p {APP_EXPOSED_PORT_MONGO}:{HOTELRES_APP_PORT} "
        f"{HOTELRES_FRONTEND_IMAGE}"
    )
    _ssh(primary, cmd)
    print(f"   Waiting for app port {primary}:{APP_EXPOSED_PORT_MONGO} ...")
    if not wait_for_port(primary, APP_EXPOSED_PORT_MONGO, timeout_sec=120):
        print("ERROR: Hotel-reservation app did not start in time.")
        sys.exit(1)
    time.sleep(5)


def check_replication_replmongo(primary=PRIMARY_HOST):
    """Log MongoDB replica set status before measurement."""
    rs_status = _mongosh(
        primary, "mongo-primary",
        "rs.status().members.map(m => m.name + \": \" + m.stateStr).join(\", \")",
        check=False,
    )
    if rs_status.returncode == 0:
        print(f"   RS status: {rs_status.stdout.strip()[-80:]}")


# ═══════════════════════════════════════════════════════════════════════════════
#  Backend: rqlite (Raft consensus)
# ═══════════════════════════════════════════════════════════════════════════════

# Module-level state for rqlite processes (needed for per-rate-point teardown)
_rqlite_processes = []


def teardown_rqlite(primary=PRIMARY_HOST, replicas=REPLICA_HOSTS):
    """Kill any leftover rqlite processes and app containers on all nodes."""
    all_hosts = [primary] + replicas
    print(f"   Tearing down on {all_hosts} ...")

    def teardown_node(host):
        _ssh(host,
             "pkill -f rqlited >/dev/null 2>&1 || true; "
             "sudo fuser -k 4001/tcp >/dev/null 2>&1 || true; "
             "sudo fuser -k 4002/tcp >/dev/null 2>&1 || true; "
             "sudo fuser -k 4003/tcp >/dev/null 2>&1 || true; "
             "sudo fuser -k 4004/tcp >/dev/null 2>&1 || true; "
             "sudo rm -rf /tmp/rqlite_data_* || true; "
             "docker rm -f bookcatalog-nd synth-workload 2>/dev/null || true",
             check=False)

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        futs = [pool.submit(teardown_node, h) for h in all_hosts]
        for f in futs:
            f.result()


def setup_rqlite(primary=PRIMARY_HOST, replicas=REPLICA_HOSTS):
    """Start a 3-node rqlite cluster using the rqlited binary."""
    global _rqlite_processes
    all_hosts = [primary] + replicas
    print(f"   Starting rqlite cluster on {all_hosts} ...")
    leader_raft_addr = f"{primary}:{RQLITE_RAFT_PORT}"
    processes = []
    for idx, host in enumerate(all_hosts):
        node_id = idx + 1
        http_port = RQLITE_HTTP_PORT if idx == 0 else RQLITE_HTTP_PORT + (idx * 2)
        raft_port = RQLITE_RAFT_PORT if idx == 0 else RQLITE_RAFT_PORT + (idx * 2)
        data_dir = f"{DATA_DIR_PREFIX}/rqlite_data_{node_id}"
        log_file = f"{data_dir}/rqlite.log"
        _ssh(host, f"sudo rm -rf {data_dir}", check=False)
        cmd = (
            f"(sudo fuser -k {http_port}/tcp || true) && "
            f"(sudo fuser -k {raft_port}/tcp || true) && "
            f"sudo mkdir -p {data_dir} && "
            f"sudo chown -R $(id -un):$(id -gn) {data_dir} && "
            f"( nohup setsid {RQLITED_BIN} -node-id={node_id} "
            f"-http-addr={host}:{http_port} -raft-addr={host}:{raft_port} "
        )
        if idx > 0:
            cmd += f"-join={leader_raft_addr} "
        cmd += f"{data_dir} </dev/null >{log_file} 2>&1 & ) && echo $!"
        print(f"   Starting rqlite node {node_id} on {host} (http={http_port}, raft={raft_port})")
        result = _ssh(host, cmd, capture=True)
        pid = result.stdout.strip() if result.stdout else ""
        processes.append({
            "host": host, "pid": pid, "data_dir": data_dir,
            "http_port": http_port, "raft_port": raft_port,
        })
        time.sleep(1.5)
    print("   Waiting for rqlite nodes to stabilize ...")
    time.sleep(3)
    _rqlite_processes = processes

    # Wait for all nodes to be reachable
    for proc in processes:
        host = proc["host"]
        http_port = proc["http_port"]
        print(f"   Waiting for rqlite on {host}:{http_port} ...")
        if not wait_for_port(host, http_port, timeout_sec=60):
            raise RuntimeError(f"rqlite on {host}:{http_port} did not start in time.")

    # Wait for cluster convergence
    print("   Waiting for rqlite cluster convergence ...")
    deadline = time.time() + 60
    while time.time() < deadline:
        result = _ssh(
            primary,
            f"curl -s http://{primary}:{RQLITE_HTTP_PORT}/status",
            check=False, capture=True,
        )
        if result.returncode == 0 and result.stdout:
            try:
                status = json.loads(result.stdout)
                nodes = status.get("store", {}).get("raft", {}).get("num_peers", -1)
                if nodes == 2:  # 2 peers + self = 3 nodes
                    print("   rqlite cluster ready (3 nodes)")
                    return
            except (json.JSONDecodeError, KeyError):
                pass
        time.sleep(3)
    print(f"   WARNING: rqlite cluster may not be fully converged")


def _stop_rqlite_processes():
    """Stop rqlite processes and clean up data directories."""
    global _rqlite_processes
    for proc in _rqlite_processes:
        host = proc["host"]
        http_port = proc.get("http_port")
        raft_port = proc.get("raft_port")
        data_dir = proc.get("data_dir")
        cleanup_cmds = []
        if http_port:
            cleanup_cmds.append(f"sudo fuser -k {http_port}/tcp >/dev/null 2>&1 || true")
        if raft_port:
            cleanup_cmds.append(f"sudo fuser -k {raft_port}/tcp >/dev/null 2>&1 || true")
        cleanup_cmds.append("pkill -f rqlited >/dev/null 2>&1 || true")
        if data_dir:
            cleanup_cmds.append(f"sudo rm -rf {data_dir}")
        _ssh(host, " ; ".join(cleanup_cmds), check=False)
    _rqlite_processes = []


def start_app_rqlite(primary=PRIMARY_HOST, app_config=None, app_name="bookcatalog"):
    """Start bookcatalog-nd or synth-workload on the leader with DB_TYPE=rqlite."""
    app_name = app_config.get("_app_name", "bookcatalog") if app_config else "bookcatalog"

    if app_name == "synth":
        container_name = "synth-workload"
        image = SYNTH_IMAGE
        env_extra = f"-e RQLITE_URL=http://{primary}:{RQLITE_HTTP_PORT} "
        print(f"   Starting synth-workload on {primary}:{RQLITE_APP_PORT} with rqlite backend ...")
    else:
        container_name = "bookcatalog-nd"
        image = BOOKCATALOG_IMAGE
        env_extra = f"-e DB_HOST={primary} "
        print(f"   Starting bookcatalog-nd on {primary}:{RQLITE_APP_PORT} with rqlite backend ...")

    _ssh(primary, f"docker rm -f {container_name} 2>/dev/null || true", check=False)
    cmd = (
        f"docker run -d --name {container_name} "
        f"--network host "
        f"-e DB_TYPE=rqlite "
        f"{env_extra}"
        f"{image}"
    )
    _ssh(primary, cmd)
    print(f"   Waiting for app port {primary}:{RQLITE_APP_PORT} ...")
    if not wait_for_port(primary, RQLITE_APP_PORT, timeout_sec=120):
        print(f"ERROR: {container_name} did not start in time.")
        result = _ssh(primary, f"docker logs {container_name} --tail 30", check=False, capture=True)
        print(f"   App log:\n{result.stdout}")
        sys.exit(1)
    time.sleep(5)


def _stop_rqlite_app(app_name):
    """Stop the rqlite app container."""
    if app_name == "synth":
        _ssh(PRIMARY_HOST, "docker rm -f synth-workload 2>/dev/null || true", check=False)
    else:
        _ssh(PRIMARY_HOST, "docker rm -f bookcatalog-nd 2>/dev/null || true", check=False)


def check_replication_rqlite(primary=PRIMARY_HOST):
    """Log rqlite cluster status before measurement."""
    result = _ssh(
        primary,
        f"curl -s http://{primary}:{RQLITE_HTTP_PORT}/status",
        check=False, capture=True,
    )
    if result.returncode == 0 and result.stdout:
        try:
            status = json.loads(result.stdout)
            peers = status.get("store", {}).get("raft", {}).get("num_peers", "?")
            state = status.get("store", {}).get("raft", {}).get("state", "?")
            print(f"   rqlite: state={state}, peers={peers}")
        except (json.JSONDecodeError, KeyError):
            pass


# ═══════════════════════════════════════════════════════════════════════════════
#  Backend dispatch tables
# ═══════════════════════════════════════════════════════════════════════════════

BACKEND_FUNCS = {
    "semisync": {
        "setup": setup_semisync,
        "teardown": teardown_semisync,
        "start_app": start_app_semisync,
        "check_replication": check_replication_semisync,
    },
    "pgsync": {
        "setup": setup_pgsync,
        "teardown": teardown_pgsync,
        "start_app": start_app_pgsync,
        "check_replication": check_replication_pgsync,
    },
    "replmongo": {
        "setup": setup_replmongo,
        "teardown": teardown_replmongo,
        "start_app": start_app_replmongo,
        "check_replication": check_replication_replmongo,
    },
    "rqlite": {
        "setup": setup_rqlite,
        "teardown": teardown_rqlite,
        "start_app": start_app_rqlite,
        "check_replication": check_replication_rqlite,
    },
}


# ═══════════════════════════════════════════════════════════════════════════════
#  App-specific setup, seeding, and load helpers
# ═══════════════════════════════════════════════════════════════════════════════


def get_app_url(app_name):
    """Return the load test URL for the given app."""
    if app_name == "wordpress":
        return f"http://{PRIMARY_HOST}:{WP_PORT}/xmlrpc.php"
    elif app_name in ("tpcc", "tpcc-java"):
        return f"http://{PRIMARY_HOST}:{TPCC_APP_PORT}/orders"
    elif app_name == "hotelres":
        return f"http://{PRIMARY_HOST}:{APP_EXPOSED_PORT_MONGO}/reservation"
    elif app_name == "bookcatalog":
        return f"http://{PRIMARY_HOST}:{RQLITE_APP_PORT}/api/books"
    elif app_name == "synth":
        return f"http://{PRIMARY_HOST}:{RQLITE_APP_PORT}/workload"
    else:
        raise ValueError(f"Unknown app: {app_name}")


def get_app_port(app_name):
    """Return the port for the given app."""
    if app_name == "wordpress":
        return WP_PORT
    elif app_name in ("tpcc", "tpcc-java"):
        return TPCC_APP_PORT
    elif app_name == "hotelres":
        return APP_EXPOSED_PORT_MONGO
    elif app_name in ("bookcatalog", "synth"):
        return RQLITE_APP_PORT
    else:
        raise ValueError(f"Unknown app: {app_name}")


def setup_app(app_name, results_dir):
    """Run app-specific post-deploy setup (install, init DB, generate data)."""
    port = get_app_port(app_name)

    if app_name == "wordpress":
        # Install WordPress
        print("\n   Installing WordPress ...")
        ok = install_wordpress(PRIMARY_HOST, port, "wordpress_semisync")
        if not ok:
            print("ERROR: WordPress installation failed.")
            sys.exit(1)
        # Enable REST API auth
        print("   Enabling REST API auth ...")
        if not enable_rest_auth(PRIMARY_HOST):
            print("ERROR: REST API auth setup failed.")
            sys.exit(1)
        if not check_rest_api(PRIMARY_HOST, port, "wordpress_semisync"):
            print("ERROR: REST API not available.")
            sys.exit(1)
        # Tune MySQL InnoDB
        print("   Tuning MySQL InnoDB settings ...")
        tune_mysql_semisync()

    elif app_name in ("tpcc", "tpcc-java"):
        # Initialize TPC-C database
        import requests
        print(f"\n   Initializing TPC-C DB ({NUM_WAREHOUSES} warehouses) ...")
        deadline = time.time() + 300
        last_err = None
        initialized = False
        while time.time() < deadline:
            try:
                r = requests.post(
                    f"http://{PRIMARY_HOST}:{port}/init_db",
                    headers={"Content-Type": "application/json"},
                    json={"warehouses": NUM_WAREHOUSES},
                    timeout=300,
                )
                if 200 <= r.status_code < 300:
                    print(f"   DB initialized (HTTP {r.status_code}): {r.text[:200]}")
                    initialized = True
                    break
                last_err = f"HTTP {r.status_code}: {r.text[:200]}"
                print(f"   init_db returned {r.status_code}, retrying ...")
            except Exception as e:
                last_err = str(e)
                print(f"   init_db error: {e}, retrying ...")
            time.sleep(10)
        if not initialized:
            print(f"   ERROR: init_db failed after 300s: {last_err}")
            sys.exit(1)
        # Check health
        import requests
        print(f"   Checking TPC-C health ...")
        deadline = time.time() + 120
        while time.time() < deadline:
            try:
                r = requests.get(f"http://{PRIMARY_HOST}:{port}/health", timeout=5)
                if r.status_code == 200:
                    print(f"   Health check OK (HTTP 200)")
                    break
            except Exception:
                pass
            time.sleep(5)

    elif app_name == "hotelres":
        # Generate dummy data
        import requests
        print("\n   Generating dummy hotel data ...")
        if not _hotelres_wait_for_app_ready(PRIMARY_HOST, port, timeout_sec=120):
            print("ERROR: App not ready.")
            sys.exit(1)
        for attempt in range(1, 4):
            if _hotelres_generate_dummy_data_common(PRIMARY_HOST, port):
                break
            print(f"   Attempt {attempt} failed, retrying in 10s ...")
            time.sleep(10)
        else:
            print("ERROR: Failed to generate dummy data.")
            sys.exit(1)
        # Verify app
        if not _hotelres_check_app_ready(PRIMARY_HOST, port):
            print("ERROR: App not ready after data generation.")
            sys.exit(1)

    elif app_name == "bookcatalog":
        # No extra setup needed beyond seeding (handled in seed_app)
        pass

    elif app_name == "synth":
        # No extra setup needed
        pass


def seed_app(app_name, results_dir, seed_count):
    """Seed the app with test data and generate workload files."""
    port = get_app_port(app_name)

    if app_name == "wordpress":
        print(f"\n   Seeding {seed_count} posts for editPost workload ...")
        seed_ids = []
        for i in range(seed_count):
            title = f"Seed Post {i + 1}"
            content = f"Seed content {i + 1}."
            pid = None
            for attempt in range(1, 4):
                try:
                    pid = _wp_create_post(PRIMARY_HOST, port, "wordpress_semisync",
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
        print(f"   Seeded {len(seed_ids)}/{seed_count} posts")

        # Write XML-RPC payloads file
        payloads_file = results_dir / "xmlrpc_payloads.txt"
        results_dir.mkdir(parents=True, exist_ok=True)
        print(f"   Writing {len(seed_ids)} XML-RPC payloads -> {payloads_file} ...")
        with open(payloads_file, "w") as fh:
            for pid in seed_ids:
                fh.write(_xmlrpc_editpost_payload(pid) + "\n")
        print(f"   Done: {len(seed_ids)} payloads written.")
        return payloads_file

    elif app_name in ("tpcc", "tpcc-java"):
        payloads_file = results_dir / "neworder_payloads.txt"
        results_dir.mkdir(parents=True, exist_ok=True)
        print(f"\n   Generating New Order payloads -> {payloads_file} ...")
        generate_tpcc_payloads_file(str(payloads_file), num_warehouses=NUM_WAREHOUSES)
        return payloads_file

    elif app_name == "hotelres":
        urls_file = results_dir / "reservation_urls.txt"
        results_dir.mkdir(parents=True, exist_ok=True)
        print(f"\n   Generating reservation URLs -> {urls_file} ...")
        _hotelres_gen_urls_common(PRIMARY_HOST, APP_EXPOSED_PORT_MONGO,
                                  str(urls_file), count=seed_count)
        return urls_file

    elif app_name == "bookcatalog":
        # Seed books
        print(f"\n   Seeding {seed_count} books ...")
        if not _bookcatalog_wait_for_app_ready(PRIMARY_HOST, port, timeout_sec=120):
            print("ERROR: App not ready.")
            sys.exit(1)
        seeded = False
        for attempt in range(1, 4):
            created = seed_books(PRIMARY_HOST, port, count=seed_count)
            if created > 0:
                seeded = True
                break
            print(f"   Attempt {attempt} failed, retrying in 10s ...")
            time.sleep(10)
        if not seeded:
            print("ERROR: Failed to seed books after 3 attempts.")
            sys.exit(1)
        if not _bookcatalog_check_app_ready(PRIMARY_HOST, port):
            print("ERROR: App not ready after seeding.")
            sys.exit(1)
        return None  # no payloads file needed (inline body)

    elif app_name == "synth":
        # No seeding needed
        return None


def _build_load_cmd(app_name, url, rate, duration_sec, results_dir):
    """Build the go load client command for the given app."""
    cmd = ["go", "run", str(GO_LATENCY_CLIENT)]

    if app_name == "wordpress":
        payloads_file = results_dir / "xmlrpc_payloads.txt"
        cmd += [
            "-H", "Content-Type: text/xml",
            "-X", "POST",
            "-payloads-file", str(payloads_file),
            url,
            "placeholder",
            str(duration_sec),
            str(rate),
        ]
    elif app_name in ("tpcc", "tpcc-java"):
        payloads_file = results_dir / "neworder_payloads.txt"
        cmd += [
            "-H", "Content-Type: application/json",
            "-X", "POST",
            "-payloads-file", str(payloads_file),
            url,
            '{"w_id": 1, "c_id": 1}',
            str(duration_sec),
            str(rate),
        ]
    elif app_name == "hotelres":
        urls_file = results_dir / "reservation_urls.txt"
        cmd += [
            "-X", "POST",
            "-urls-file", str(urls_file),
            url,
            "",
            str(duration_sec),
            str(rate),
        ]
    elif app_name == "bookcatalog":
        cmd += [
            "-H", "Content-Type: application/json",
            "-X", "POST",
            url,
            '{"title":"bench book","author":"bench author"}',
            str(duration_sec),
            str(rate),
        ]
    elif app_name == "synth":
        payload = make_synth_workload_payload(txns=1, ops=1, write_size=100)
        cmd += [
            "-H", "Content-Type: application/json",
            "-X", "POST",
            url,
            payload,
            str(duration_sec),
            str(rate),
        ]

    return cmd


def _run_load(app_name, url, rate, output_file, duration_sec, results_dir):
    """Run the Go load client at `rate` req/s for `duration_sec` seconds."""
    output_file.parent.mkdir(parents=True, exist_ok=True)
    cmd = _build_load_cmd(app_name, url, rate, duration_sec, results_dir)
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


# ═══════════════════════════════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════════════════════════════


def parse_args():
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--app", required=True,
                   choices=list(APP_BACKEND_MAP.keys()),
                   help="Application to benchmark")
    p.add_argument("--rates", type=str, default=None,
                   help="Comma-separated list of offered rates (req/s)")
    p.add_argument("--duration", type=int, default=LOAD_DURATION_SEC,
                   help=f"Measurement duration per rate point (default: {LOAD_DURATION_SEC}s)")
    p.add_argument("--skip-teardown", action="store_true",
                   help="Skip container teardown (assume clean state)")
    p.add_argument("--skip-db", action="store_true",
                   help="Skip DB cluster setup (assume already running)")
    p.add_argument("--skip-app", action="store_true",
                   help="Skip app container start (assume already running)")
    p.add_argument("--skip-seed", action="store_true",
                   help="Skip app seeding and data generation")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    app_name = args.app
    backend = APP_BACKEND_MAP[app_name]
    app_config = APP_CONFIGS[app_name]
    seed_count = app_config.get("seed_count", 200)
    warmup_rate = app_config.get("warmup_rate", 1500)
    avg_lat_threshold = app_config.get("avg_latency_threshold_ms", AVG_LATENCY_THRESHOLD_MS)
    load_duration = args.duration

    # Compute timestamped results directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    RESULTS_DIR = RESULTS_BASE / f"{app_name}_{backend}_{timestamp}"
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    # Determine rate list
    if args.rates:
        rates = [int(r.strip()) for r in args.rates.split(",")]
    else:
        rates = BOTTLENECK_RATES

    backend_funcs = BACKEND_FUNCS[backend]

    # Inject app name into config for rqlite backend
    app_config_with_name = {**app_config, "_app_name": app_name}

    print("=" * 60)
    print(f"Distributed DB Replication Benchmark")
    print(f"  App      : {app_name}")
    print(f"  Backend  : {backend}")
    print(f"  Rates    : {rates}")
    print(f"  Duration : {load_duration}s per rate (warmup {WARMUP_DURATION_SEC}s)")
    print(f"  Results  : {RESULTS_DIR}")
    print("=" * 60)

    app_url = get_app_url(app_name)

    # ── For rqlite: fresh cluster per rate point ─────────────────────────
    if backend == "rqlite":
        results = []
        for i, rate in enumerate(rates):
            print(f"\n{'=' * 60}")
            print(f"  Rate point {i + 1}/{len(rates)}: {rate} req/s (fresh cluster)")
            print(f"{'=' * 60}")

            try:
                # Tear down previous cluster
                print("\n   [1] Tearing down ...")
                teardown_rqlite()

                # Start fresh rqlite cluster
                print("\n   [2] Starting rqlite cluster ...")
                setup_rqlite()

                # Start fresh app
                print("\n   [3] Starting app ...")
                start_app_rqlite(primary=PRIMARY_HOST, app_config=app_config_with_name,
                                 app_name=app_name)

                # Seed
                if not args.skip_seed:
                    print(f"\n   [4] Seeding ...")
                    seed_app(app_name, RESULTS_DIR, seed_count)

                # Warmup
                print(f"\n   [5] Warmup ({WARMUP_DURATION_SEC}s at {rate} req/s) ...")
                wm = _run_load(app_name, app_url, rate,
                               RESULTS_DIR / f"warmup_rate{rate}.txt",
                               WARMUP_DURATION_SEC, RESULTS_DIR)
                print(f"     [warmup] tput={wm['throughput_rps']:.2f} rps  avg={wm['avg_ms']:.1f}ms")
                if wm["throughput_rps"] < 0.1 * rate:
                    print(
                        f"   SKIP: warmup tput={wm['throughput_rps']:.2f} rps "
                        f"< 10% of {rate} rps -- system saturated"
                    )
                    continue
                print("   Settling 5s after warmup ...")
                time.sleep(5)

                # Check replication
                check_replication_rqlite()

                # Measurement
                print(f"\n   [6] Measuring ({load_duration}s at {rate} req/s) ...")
                m = _run_load(app_name, app_url, rate,
                              RESULTS_DIR / f"rate{rate}.txt",
                              load_duration, RESULTS_DIR)
                row = {"rate_rps": rate, **m}
                results.append(row)
                print(
                    f"     tput={m['throughput_rps']:.2f} rps  "
                    f"avg={m['avg_ms']:.1f}ms  "
                    f"p50={m['p50_ms']:.1f}ms  "
                    f"p95={m['p95_ms']:.1f}ms"
                )

                # Early stop on saturation
                avg_exceeded = m["avg_ms"] > avg_lat_threshold
                tput_dropped = (m["actual_achieved_rps"] > 0 and
                                m["throughput_rps"] < 0.75 * m["actual_achieved_rps"])
                if avg_exceeded and tput_dropped:
                    print(
                        f"   System saturated: avg {m['avg_ms']:.0f}ms > {avg_lat_threshold}ms AND "
                        f"throughput {m['throughput_rps']:.2f} < 75% of {m['actual_achieved_rps']:.2f}; stopping."
                    )
                    break
                if avg_exceeded:
                    print(f"   Avg latency {m['avg_ms']:.0f}ms > {avg_lat_threshold}ms (throughput OK); continuing.")
                if tput_dropped:
                    print(f"   Throughput {m['throughput_rps']:.2f} < 75% of {m['actual_achieved_rps']:.2f} (latency OK); continuing.")
            finally:
                # Always clean up this rate's cluster
                _stop_rqlite_app(app_name)
                _stop_rqlite_processes()

    else:
        # ── For non-rqlite backends: single cluster for all rate points ──

        # Phase 1 -- Teardown
        if args.skip_teardown:
            print("\n[Phase 1] Skipping teardown (--skip-teardown)")
        else:
            print("\n[Phase 1] Tearing down existing containers ...")
            backend_funcs["teardown"]()

        # Phase 2 -- DB setup
        if args.skip_db:
            print("\n[Phase 2] Skipping DB setup (--skip-db)")
        else:
            print(f"\n[Phase 2] Setting up {backend} cluster ...")
            backend_funcs["setup"]()

        # Phase 3 -- Start app
        if args.skip_app:
            print("\n[Phase 3] Skipping app start (--skip-app)")
        else:
            print(f"\n[Phase 3] Starting {app_name} app ...")
            backend_funcs["start_app"](primary=PRIMARY_HOST, app_config=app_config_with_name,
                                     app_name=app_name)

        # Phase 4 -- App-specific setup
        if not args.skip_app:
            print(f"\n[Phase 4] App-specific setup for {app_name} ...")
            setup_app(app_name, RESULTS_DIR)

        # Phase 5 -- Seed
        if args.skip_seed:
            print(f"\n[Phase 5] Skipping seed (--skip-seed)")
        else:
            print(f"\n[Phase 5] Seeding {app_name} ...")
            seed_app(app_name, RESULTS_DIR, seed_count)

        # Phase 6 -- Load sweep
        print(f"\n[Phase 6] Load sweep at {rates} req/s ...")
        print(f"   URL      : {app_url}")
        print(f"   Duration : {load_duration}s per rate (warmup {WARMUP_DURATION_SEC}s)")

        results = []
        for i, rate in enumerate(rates):
            if i > 0 and INTER_RATE_PAUSE_SEC > 0:
                print(f"   Pausing {INTER_RATE_PAUSE_SEC}s between rate points ...")
                time.sleep(INTER_RATE_PAUSE_SEC)

            print(f"\n   -> warmup at {warmup_rate} req/s for {WARMUP_DURATION_SEC}s ...")
            wm = _run_load(app_name, app_url, warmup_rate,
                           RESULTS_DIR / f"warmup_rate{rate}.txt",
                           WARMUP_DURATION_SEC, RESULTS_DIR)
            print(f"     [warmup] tput={wm['throughput_rps']:.2f} rps  avg={wm['avg_ms']:.1f}ms")
            if wm["throughput_rps"] < 0.1 * warmup_rate:
                print(
                    f"   SKIP: warmup tput={wm['throughput_rps']:.2f} rps "
                    f"< 10% of {warmup_rate} rps -- system not ready"
                )
                continue
            print("   Settling 5s after warmup ...")
            time.sleep(5)

            # Log replication status before each measurement
            backend_funcs["check_replication"]()

            print(f"   -> rate={rate} req/s (measuring {load_duration}s) ...")
            m = _run_load(app_name, app_url, rate,
                          RESULTS_DIR / f"rate{rate}.txt",
                          load_duration, RESULTS_DIR)
            row = {"rate_rps": rate, **m}
            results.append(row)
            print(
                f"     tput={m['throughput_rps']:.2f} rps  "
                f"avg={m['avg_ms']:.1f}ms  "
                f"p50={m['p50_ms']:.1f}ms  "
                f"p95={m['p95_ms']:.1f}ms"
            )
            if m["avg_ms"] > avg_lat_threshold:
                print(
                    f"   Avg latency {m['avg_ms']:.0f}ms > {avg_lat_threshold}ms -- stopping."
                )
                break
            if m["actual_achieved_rps"] > 0 and m["throughput_rps"] < 0.8 * m["actual_achieved_rps"]:
                print(
                    f"   Throughput {m['throughput_rps']:.2f} rps < 80% of offered "
                    f"{m['actual_achieved_rps']:.2f} rps -- system saturated, stopping sweep early."
                )
                break

    # ── Write CSV ────────────────────────────────────────────────────────
    csv_path = RESULTS_DIR / f"{app_name}_{backend}_load_results.csv"
    fieldnames = ["rate_rps", "achieved_rps", "throughput_rps",
                  "avg_ms", "p50_ms", "p90_ms", "p95_ms", "p99_ms"]
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

    # ── Summary ──────────────────────────────────────────────────────────
    print(f"\n{'=' * 60}")
    print(f"Summary: {app_name} / {backend}")
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

    # ── Plot ─────────────────────────────────────────────────────────────
    print("\nGenerating plots ...")
    r = subprocess.run(
        ["python3", "plot_load_latency.py", "--results-dir", str(RESULTS_DIR)],
        capture_output=True, text=True, cwd=str(SCRIPT_DIR),
    )
    print(r.stdout)
    if r.returncode != 0:
        print(f"   WARNING: plot_load_latency.py failed:\n{r.stderr}")

    # ── Symlink ──────────────────────────────────────────────────────────
    symlink = RESULTS_BASE / f"load_pb_{app_name}_{backend}"
    if symlink.is_symlink():
        symlink.unlink()
    elif symlink.is_dir():
        backup = RESULTS_BASE / f"{app_name}_{backend}_old_{timestamp}"
        symlink.rename(backup)
        print(f"   Renamed old directory -> {backup.name}")
    symlink.symlink_to(RESULTS_DIR.name)
    print(f"   Symlink: {symlink} -> {RESULTS_DIR.name}")

    print(f"\n[Done] Results in {RESULTS_DIR}/")
