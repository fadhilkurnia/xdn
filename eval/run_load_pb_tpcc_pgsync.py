"""
run_load_pb_tpcc_pgsync.py — PostgreSQL synchronous streaming replication benchmark.

Sets up PostgreSQL primary on 10.10.1.1 with two synchronous streaming replicas
on .2 and .3 using pg_basebackup -R. TPC-C Flask app connects directly to the
PG primary (no XDN). Runs the same POST /orders New Order workload as the XDN PB
benchmark for direct comparison.

Run from the eval/ directory:
    python3 run_load_pb_tpcc_pgsync.py [--skip-teardown] [--skip-postgres]

Outputs go to eval/results/load_pb_tpcc_pgsync/:
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

from run_load_pb_tpcc_reflex import (
    AVG_LATENCY_THRESHOLD_MS,
    BOTTLENECK_RATES,
    GO_LATENCY_CLIENT,
    INTER_RATE_PAUSE_SEC,
    LOAD_DURATION_SEC,
    NUM_WAREHOUSES,
    WARMUP_DURATION_SEC,
    _parse_go_output,
    generate_payloads_file,
    wait_for_port,
)

# ── Constants ─────────────────────────────────────────────────────────────────

PRIMARY_HOST = "10.10.1.1"
REPLICA_HOSTS = ["10.10.1.2", "10.10.1.3"]
TPCC_APP_PORT = 2300
PG_PORT = 5432

PG_PASSWORD = "benchpass"
PG_DB = "tpcc"
REPL_USER = "repl"
REPL_PASS = "replpass"

RESULTS_BASE = Path(__file__).resolve().parent / "results"
RESULTS_DIR = RESULTS_BASE / "load_pb_tpcc_pgsync"
SCRIPT_DIR = Path(__file__).resolve().parent
PAYLOADS_FILE = RESULTS_DIR / "neworder_payloads.txt"

# ── Shell helpers ─────────────────────────────────────────────────────────────


def _ssh(host, cmd, check=True, capture=False):
    return subprocess.run(
        ["ssh", "-o", "StrictHostKeyChecking=no", host, cmd],
        check=check,
        capture_output=capture,
        text=True,
    )


def _psql(host, sql, container="postgres-primary", user="postgres"):
    """Run SQL on a PostgreSQL container on `host`."""
    return _ssh(
        host,
        f'docker exec {container} psql -U {user} -d {PG_DB} -c "{sql}"',
        capture=True,
    )


# ── Load test helpers ─────────────────────────────────────────────────────────


def _run_load(url, rate, output_file, duration_sec):
    """Run get_latency_at_rate.go at `rate` req/s for `duration_sec` seconds.

    No XDN header, no auth — direct to TPC-C app.
    """
    output_file.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "go", "run", str(GO_LATENCY_CLIENT),
        "-H", "Content-Type: application/json",
        "-X", "POST",
        "-payloads-file", str(PAYLOADS_FILE),
        url,
        '{"w_id": 1, "c_id": 1}',
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
    """Stop and remove PostgreSQL + TPC-C containers on all three nodes."""
    all_hosts = [PRIMARY_HOST] + REPLICA_HOSTS
    print(f"   Stopping containers on {all_hosts} ...")

    def teardown_node(host):
        _ssh(host,
             "docker ps -q | xargs -r docker stop 2>/dev/null || true; "
             "docker ps -aq | xargs -r docker rm -f 2>/dev/null || true",
             check=False)
        if host == PRIMARY_HOST:
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


def start_postgres_primary():
    """Start PostgreSQL primary with synchronous replication on PRIMARY_HOST.

    Sync-rep parameters (synchronous_commit, synchronous_standby_names) are NOT
    passed via docker run because the PG Docker entrypoint applies ALL user args
    to its temporary initdb server. That server has no standbys, so sync_commit
    blocks forever on DB creation. Instead, we start with async and enable
    sync-rep after the container is fully initialized.
    """
    print(f"   Starting PostgreSQL primary on {PRIMARY_HOST} ...")
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
    _ssh(PRIMARY_HOST, cmd)
    print(f"   Waiting for PostgreSQL primary port {PRIMARY_HOST}:{PG_PORT} ...")
    if not wait_for_port(PRIMARY_HOST, PG_PORT, timeout_sec=240):
        print("ERROR: PostgreSQL primary did not start in time.")
        sys.exit(1)
    # Allow PostgreSQL to fully initialize
    time.sleep(10)


def enable_sync_replication():
    """Enable synchronous replication on the primary via ALTER SYSTEM SET.

    Called after replicas are connected so writes don't block waiting for
    non-existent standbys.
    """
    print("   Enabling synchronous replication on primary ...")
    sqls = [
        "ALTER SYSTEM SET synchronous_commit = on",
        "ALTER SYSTEM SET synchronous_standby_names = '*'",
    ]
    for sql in sqls:
        result = _ssh(
            PRIMARY_HOST,
            f"docker exec postgres-primary psql -U postgres -c \"{sql}\"",
            capture=True, check=False,
        )
        if result.returncode != 0:
            print(f"   ERROR running: {sql}\n{result.stderr}")
            sys.exit(1)
    # Reload config to apply
    _ssh(PRIMARY_HOST,
         "docker exec -u postgres postgres-primary pg_ctl reload -D /var/lib/postgresql/data")
    print("   Synchronous replication enabled and config reloaded.")


def create_replication_user():
    """Create the replication user on the primary."""
    print("   Creating replication user ...")
    sqls = [
        f"CREATE USER {REPL_USER} WITH REPLICATION ENCRYPTED PASSWORD '{REPL_PASS}'",
    ]
    for sql in sqls:
        result = _ssh(
            PRIMARY_HOST,
            f"docker exec postgres-primary psql -U postgres -c \"{sql}\"",
            capture=True,
            check=False,
        )
        if result.returncode != 0:
            # User may already exist
            if "already exists" in result.stderr or "already exists" in result.stdout:
                print(f"   Replication user already exists, continuing.")
            else:
                print(f"   ERROR running SQL: {sql}\n{result.stderr}")
                sys.exit(1)

    # Append replication entry to pg_hba.conf and reload
    _ssh(
        PRIMARY_HOST,
        "docker exec postgres-primary bash -c "
        "\"echo 'host replication repl 10.10.1.0/24 md5' >> "
        "/var/lib/postgresql/data/pg_hba.conf\"",
    )
    _ssh(
        PRIMARY_HOST,
        "docker exec -u postgres postgres-primary pg_ctl reload -D /var/lib/postgresql/data",
    )
    print("   Replication user created and pg_hba.conf updated.")


def start_postgres_replicas():
    """Start PostgreSQL replicas on .2 and .3 using pg_basebackup -R."""
    def start_replica(host):
        print(f"   Starting PostgreSQL replica on {host} ...")

        # Clean up any existing volume
        _ssh(host, "docker volume rm pgdata 2>/dev/null || true", check=False)
        _ssh(host, "docker volume create pgdata")

        # pg_basebackup from primary. -R creates standby.signal + primary_conninfo.
        print(f"   [{host}] Running pg_basebackup from {PRIMARY_HOST} ...")
        _ssh(
            host,
            f"docker run --rm --net=host "
            f"-e PGPASSWORD={REPL_PASS} "
            f"-v pgdata:/data "
            f"postgres:17.4-bookworm "
            f"pg_basebackup -h {PRIMARY_HOST} -U {REPL_USER} -D /data -Fp -Xs -R",
        )

        # Start replica using the basebackup data
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
        futs = [pool.submit(start_replica, h) for h in REPLICA_HOSTS]
        for f in futs:
            f.result()


def wait_for_replicas_sync(timeout_sec=120):
    """Poll pg_stat_replication until both replicas show streaming state."""
    print("   Waiting for replicas to sync ...")
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        result = _ssh(
            PRIMARY_HOST,
            "docker exec postgres-primary psql -U postgres -c "
            "\"SELECT client_addr, state, sync_state FROM pg_stat_replication\"",
            capture=True, check=False,
        )
        if result.returncode == 0:
            streaming_count = result.stdout.count("streaming")
            print(f"   pg_stat_replication:\n{result.stdout.strip()}")
            if streaming_count >= 2:
                print(f"   Both replicas in streaming state.")
                return True
        print(f"   Waiting for replicas to reach streaming state ...")
        time.sleep(5)
    print(f"   WARNING: replicas may not be fully synced after {timeout_sec}s")
    return False


def start_tpcc_app():
    """Start TPC-C Flask app on PRIMARY_HOST connected to local PostgreSQL."""
    print(f"   Starting TPC-C app on {PRIMARY_HOST}:{TPCC_APP_PORT} ...")
    _ssh(PRIMARY_HOST, "docker rm -f tpcc-app 2>/dev/null || true", check=False)
    cmd = (
        "docker run -d --name tpcc-app "
        f"-e DATABASE_URL=postgresql://postgres:{PG_PASSWORD}@{PRIMARY_HOST}:{PG_PORT}/{PG_DB} "
        f'-e GUNICORN_CMD_ARGS="--workers 4 --timeout 120" '
        f"-p {TPCC_APP_PORT}:8000 "
        "fadhilkurnia/xdn-tpcc"
    )
    _ssh(PRIMARY_HOST, cmd)
    print(f"   Waiting for TPC-C app port {PRIMARY_HOST}:{TPCC_APP_PORT} ...")
    if not wait_for_port(PRIMARY_HOST, TPCC_APP_PORT, timeout_sec=120):
        print("ERROR: TPC-C app did not start in time.")
        sys.exit(1)
    time.sleep(5)


def init_tpcc_direct(host, port, warehouses=NUM_WAREHOUSES, timeout=300):
    """Initialize TPC-C database via direct HTTP (no XDN header)."""
    import requests
    print(f"   Initializing TPC-C DB ({warehouses} warehouses) via {host}:{port} ...")
    deadline = time.time() + timeout
    last_err = None
    while time.time() < deadline:
        try:
            r = requests.post(
                f"http://{host}:{port}/init_db",
                headers={"Content-Type": "application/json"},
                json={"warehouses": warehouses},
                timeout=timeout,
            )
            if 200 <= r.status_code < 300:
                print(f"   DB initialized (HTTP {r.status_code}): {r.text[:200]}")
                return True
            last_err = f"HTTP {r.status_code}: {r.text[:200]}"
            print(f"   init_db returned {r.status_code}, retrying ...")
        except Exception as e:
            last_err = str(e)
            print(f"   init_db error: {e}, retrying ...")
        time.sleep(10)
    print(f"   ERROR: init_db failed after {timeout}s: {last_err}")
    return False


def check_tpcc_health_direct(host, port, timeout=120):
    """Check TPC-C app health via direct HTTP (no XDN header)."""
    import requests
    print(f"   Checking TPC-C health via {host}:{port}/health ...")
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(f"http://{host}:{port}/health", timeout=5)
            if r.status_code == 200:
                print(f"   Health check OK (HTTP 200)")
                return True
        except Exception:
            pass
        time.sleep(5)
    print(f"   ERROR: health check failed after {timeout}s")
    return False


# ── Main ──────────────────────────────────────────────────────────────────────


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--skip-teardown", action="store_true",
                   help="Skip container teardown (assume clean state)")
    p.add_argument("--skip-postgres", action="store_true",
                   help="Skip PostgreSQL primary/replica setup (assume already running)")
    p.add_argument("--skip-tpcc-app", action="store_true",
                   help="Skip TPC-C app container start (assume already running)")
    p.add_argument("--skip-init", action="store_true",
                   help="Skip TPC-C database initialization")
    p.add_argument("--skip-seed", action="store_true",
                   help="Skip generating payloads file (assume already exists)")
    p.add_argument("--rates", type=str, default=None,
                   help="Comma-separated list of offered rates (req/s) to override default")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()

    # Compute timestamped results directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    RESULTS_DIR = RESULTS_BASE / f"tpcc_pgsync_{timestamp}"
    PAYLOADS_FILE = RESULTS_DIR / "neworder_payloads.txt"

    print("=" * 60)
    print("TPC-C PostgreSQL Synchronous Replication Benchmark")
    print(f"  Results  -> {RESULTS_DIR}")
    print("=" * 60)

    # Phase 1 — Teardown
    if args.skip_teardown:
        print("\n[Phase 1] Skipping teardown (--skip-teardown)")
    else:
        print("\n[Phase 1] Tearing down existing containers ...")
        teardown_all()

    # Phase 2 — PostgreSQL primary
    if args.skip_postgres:
        print("\n[Phase 2-5] Skipping PostgreSQL setup (--skip-postgres)")
    else:
        print("\n[Phase 2] Starting PostgreSQL primary ...")
        start_postgres_primary()

        # Phase 3 — Replication user
        print("\n[Phase 3] Creating replication user on primary ...")
        create_replication_user()

        # Phase 4 — PostgreSQL replicas
        print("\n[Phase 4] Starting PostgreSQL replicas on .2 and .3 ...")
        start_postgres_replicas()

        # Phase 5 — Wait for replicas to connect, then enable sync-rep
        print("\n[Phase 5] Waiting for replicas to sync ...")
        wait_for_replicas_sync()

        # Phase 5b — Enable synchronous replication (after replicas are connected)
        print("\n[Phase 5b] Enabling synchronous replication ...")
        enable_sync_replication()

    # Phase 6 — TPC-C app
    if args.skip_tpcc_app:
        print("\n[Phase 6] Skipping TPC-C app start (--skip-tpcc-app)")
    else:
        print("\n[Phase 6] Starting TPC-C app ...")
        start_tpcc_app()

    tpcc_url = f"http://{PRIMARY_HOST}:{TPCC_APP_PORT}/orders"

    # Phase 7 — Initialize TPC-C database
    if args.skip_init:
        print("\n[Phase 7] Skipping TPC-C DB init (--skip-init)")
    else:
        print("\n[Phase 7] Initializing TPC-C database ...")
        if not init_tpcc_direct(PRIMARY_HOST, TPCC_APP_PORT, NUM_WAREHOUSES):
            print("ERROR: TPC-C database initialization failed.")
            sys.exit(1)

    # Phase 7b — Check health
    print("\n[Phase 7b] Verifying TPC-C health ...")
    if not check_tpcc_health_direct(PRIMARY_HOST, TPCC_APP_PORT):
        print("ERROR: TPC-C health check failed.")
        sys.exit(1)

    # Phase 8 — Generate payloads file
    if args.skip_seed:
        print(f"\n[Phase 8] Skipping payloads generation (--skip-seed)")
        print(f"   Assuming {PAYLOADS_FILE} already exists.")
    else:
        print(f"\n[Phase 8] Generating New Order payloads -> {PAYLOADS_FILE} ...")
        generate_payloads_file(PAYLOADS_FILE, NUM_WAREHOUSES)

    # Determine rate list
    if args.rates:
        rates = [int(r.strip()) for r in args.rates.split(",")]
    else:
        rates = BOTTLENECK_RATES

    # Phase 9 — Load sweep
    print(f"\n[Phase 9] Load sweep at {rates} req/s ...")
    print(f"   URL      : {tpcc_url}")
    print(f"   Duration : {LOAD_DURATION_SEC}s per rate (warmup {WARMUP_DURATION_SEC}s)")
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    results = []
    for i, rate in enumerate(rates):
        if i > 0 and INTER_RATE_PAUSE_SEC > 0:
            print(f"   Pausing {INTER_RATE_PAUSE_SEC}s between rate points ...")
            time.sleep(INTER_RATE_PAUSE_SEC)

        print(f"\n   -> rate={rate} req/s (warmup {WARMUP_DURATION_SEC}s) ...")
        wm = warmup_rate_point(tpcc_url, rate)
        if wm["throughput_rps"] < 0.1 * rate:
            print(
                f"   SKIP: warmup tput={wm['throughput_rps']:.2f} rps "
                f"< 10% of {rate} rps — system saturated"
            )
            continue
        print("   Settling 5s after warmup ...")
        time.sleep(5)

        # Log replication status before each measurement
        status = _ssh(
            PRIMARY_HOST,
            "docker exec postgres-primary psql -U postgres -c "
            "\"SELECT client_addr, state, sync_state FROM pg_stat_replication\"",
            capture=True, check=False,
        )
        if status.returncode == 0:
            streaming = [l for l in status.stdout.splitlines() if "streaming" in l]
            if streaming:
                print(f"   PG replication: {len(streaming)} replica(s) streaming")

        print(f"   -> rate={rate} req/s (measuring {LOAD_DURATION_SEC}s) ...")
        m = run_load_point(tpcc_url, rate)
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

    # Phase 10 — Summary
    print("\n[Phase 10] Summary")
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

    # Phase 11 — Plot
    print("\n[Phase 11] Generating plots ...")
    r = subprocess.run(
        ["python3", "plot_load_latency.py", "--results-dir", str(RESULTS_DIR)],
        capture_output=True, text=True, cwd=str(SCRIPT_DIR),
    )
    print(r.stdout)
    if r.returncode != 0:
        print(f"   WARNING: plot_load_latency.py failed:\n{r.stderr}")

    # ── Create/update symlink ─────────────────────────────────────────
    symlink = RESULTS_BASE / "load_pb_tpcc_pgsync"
    if symlink.is_symlink():
        symlink.unlink()
    elif symlink.is_dir():
        backup = RESULTS_BASE / f"tpcc_pgsync_old_{timestamp}"
        symlink.rename(backup)
        print(f"   Renamed old directory -> {backup.name}")
    symlink.symlink_to(RESULTS_DIR.name)
    print(f"   Symlink: {symlink} -> {RESULTS_DIR.name}")

    print(f"\n[Done] Results in {RESULTS_DIR}/")
