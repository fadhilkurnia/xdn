"""
run_microbench_coordination_granularity.py — Coordination granularity microbenchmark.

Measures per-request latency as a function of SQL statements per request,
demonstrating ReFlex's coordination granularity advantage. Uses the tpcc-java
service with a configurable `txns` parameter.

Systems:
  reflex      — XDN PB with tpcc-java on FUSE (1 Paxos round per HTTP request)
  pgsync-txn  — PG sync replication, autocommit=false (1 sync per txn commit)
  pgsync-stmt — PG sync replication, autocommit=true (1 sync per SQL statement)
  openebs     — OpenEBS Mayastor + tpcc-java on K8s (1 round per fsync)

Usage:
    python3 run_microbench_coordination_granularity.py --system reflex
    python3 run_microbench_coordination_granularity.py --system reflex \\
        --txns 1,2,3,5,8,10,15 --n-samples 100 --n-warmup 20 --sanity-check
"""

import argparse
import csv
import json
import os
import random
import shutil
import statistics
import subprocess
import sys
import time
import urllib.request
from datetime import datetime
from pathlib import Path

from pb_common import (
    AR_HOSTS,
    CONTROL_PLANE_HOST,
    HTTP_PROXY_PORT,
    start_cluster,
    wait_for_port,
    wait_for_service,
    detect_primary_via_docker,
    ensure_docker_images,
)


def clear_xdn_cluster():
    """Force-clear XDN cluster, tolerant of unreachable hosts.

    Uses ConnectTimeout=5 so unreachable hosts don't block for 60s each.
    """
    print(" > resetting the cluster:")
    ssh = "ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5"
    for host in AR_HOSTS:
        os.system(
            f"{ssh} {host} 'containers=$(docker ps -a -q); "
            f"if [ -n \"$containers\" ]; then docker stop $containers && docker rm $containers; fi' "
            f"2>/dev/null || true"
        )
        os.system(f"{ssh} {host} 'docker network prune --force' > /dev/null 2>&1 || true")
        os.system(f"{ssh} {host} 'sudo fuser -k 2000/tcp 2>/dev/null; sudo fuser -k 2300/tcp 2>/dev/null' 2>/dev/null || true")
        os.system(f"{ssh} {host} 'sudo pkill -9 -f fuselog 2>/dev/null' || true")
        os.system(
            f"{ssh} {host} \"grep fuselog /proc/mounts 2>/dev/null"
            f" | awk '{{print \\$2}}' | xargs -r sudo umount -l 2>/dev/null || true\""
        )
        os.system(f"{ssh} {host} 'sleep 1; sudo rm -rf /tmp/gigapaxos /tmp/xdn /dev/shm/xdn' 2>/dev/null || true")
    # RC node
    os.system(f"{ssh} {CONTROL_PLANE_HOST} 'sudo fuser -k 3000/tcp 2>/dev/null; sudo rm -rf /tmp/gigapaxos' 2>/dev/null || true")
    print("   done.")

# ── Constants ─────────────────────────────────────────────────────────────────

DEFAULT_TXNS = [1, 2, 3, 5, 8, 10, 15]
DEFAULT_OL_CNT = [1, 2, 3, 5, 8, 10, 15]
DEFAULT_N_SAMPLES = 50
DEFAULT_N_WARMUP = 10
NUM_WAREHOUSES = 10
SERVICE_NAME = "tpcc-java"
TPCC_JAVA_YAML = "../xdn-cli/examples/tpcc-java.yaml"
XDN_BINARY = "../bin/xdn"
GP_CONFIG = "../conf/gigapaxos.xdn.3way.cloudlab.properties"
GP_JVM_ARGS = (
    "-DSYNC=true "
    "-DPB_N_PARALLEL_WORKERS=256 "
    "-DPB_CAPTURE_ACCUMULATION_US=100 "
    "-DHTTP_FORCE_KEEPALIVE=true "
    "-DFUSELOG_DISABLE_COALESCING=true "
    "-DXDN_MIGRATE_WAIT_SEC=5 "
    "-DXDN_SKIP_INIT_SYNC=true "
    "-DPB_INLINE_EXECUTE=true "
    "-DBATCHING_ENABLED=false"
)
REQUIRED_IMAGES = ["postgres:17.4-bookworm", "fadhilkurnia/xdn-tpcc-java"]
# AR hosts matching the GP_CONFIG; overridden by --gp-config if needed
_AR_HOSTS = AR_HOSTS  # default from pb_common: [10.10.1.1, 10.10.1.2, 10.10.1.3]

SCREEN_SESSION = "xdn_coordination_bench"
SCREEN_LOG = "screen_logs/xdn_coordination_bench.log"

RESULTS_BASE = Path(__file__).resolve().parent / "results"


# ── Host reachability ─────────────────────────────────────────────────────────


def check_host_reachable(host, timeout=3):
    """Check if a host is reachable via SSH."""
    r = subprocess.run(
        ["ssh", "-o", "StrictHostKeyChecking=no", "-o", f"ConnectTimeout={timeout}",
         host, "true"],
        capture_output=True, timeout=timeout + 2,
    )
    return r.returncode == 0


def get_reachable_hosts():
    """Return list of reachable AR hosts and warn about unavailable ones."""
    reachable = []
    for host in AR_HOSTS:
        if check_host_reachable(host):
            reachable.append(host)
        else:
            print(f"   WARNING: {host} is unreachable, skipping")
    if len(reachable) < 2:
        print("   ERROR: need at least 2 reachable AR hosts for Paxos majority")
        sys.exit(1)
    return reachable


# ── Helpers ───────────────────────────────────────────────────────────────────


def make_payload(txns=1, autocommit=True, ol_cnt=0, num_warehouses=NUM_WAREHOUSES):
    """Build a random New Order JSON payload."""
    d = {
        "w_id": random.randint(1, num_warehouses),
        "c_id": random.randint(1, num_warehouses * 10),
        "txns": txns,
        "autocommit": autocommit,
    }
    if ol_cnt > 0:
        d["ol_cnt"] = ol_cnt
    return json.dumps(d)


def init_tpcc_db(host, port, service=None, warehouses=NUM_WAREHOUSES, timeout=300):
    """Initialize TPC-C Java database via POST /init_db."""
    print(f"   Initializing TPC-C Java DB ({warehouses} warehouses) ...")
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            headers = {"Content-Type": "application/json"}
            if service and service != "unused":
                headers["XDN"] = service
            req = urllib.request.Request(
                f"http://{host}:{port}/init_db",
                data=json.dumps({"warehouses": warehouses}).encode(),
                headers=headers,
            )
            resp = urllib.request.urlopen(req, timeout=timeout)
            body = resp.read().decode()
            print(f"   DB initialized: {body[:200]}")
            return True
        except Exception as e:
            print(f"   init_db error: {e}, retrying ...")
            time.sleep(5)
    print(f"   ERROR: init_db failed after {timeout}s")
    return False


def measure_latency(url, headers, payload_fn, n_warmup, n_samples):
    """Send sequential requests over a persistent HTTP connection.

    Uses http.client with keep-alive to avoid TCP handshake overhead
    per request (~8ms). payload_fn is called per-request for fresh payloads.
    """
    import http.client
    from urllib.parse import urlparse
    parsed = urlparse(url)
    conn = http.client.HTTPConnection(parsed.hostname, parsed.port or 80, timeout=60)

    latencies = []
    for i in range(n_warmup + n_samples):
        payload = payload_fn()
        t0 = time.time()
        try:
            conn.request("POST", parsed.path, payload, headers)
            resp = conn.getresponse()
            resp.read()
            elapsed_ms = (time.time() - t0) * 1000
            if i >= n_warmup:
                latencies.append(elapsed_ms)
        except Exception as e:
            print(f"     request {i+1} failed: {e}")
            if i >= n_warmup:
                latencies.append(60000.0)
            # Reconnect on failure
            try:
                conn.close()
                conn = http.client.HTTPConnection(parsed.hostname, parsed.port or 80, timeout=60)
            except Exception:
                pass
    conn.close()
    return latencies


def probe_sql_count(url, headers, txns=1, autocommit=True, ol_cnt=0):
    """Send one request and return sql_count from JSON response."""
    d = {"w_id": 1, "c_id": 1, "txns": txns, "autocommit": autocommit}
    if ol_cnt > 0:
        d["ol_cnt"] = ol_cnt
    payload = json.dumps(d)
    req = urllib.request.Request(
        url, data=payload.encode(), headers=headers, method="POST",
    )
    resp = urllib.request.urlopen(req, timeout=30)
    body = json.loads(resp.read().decode())
    return body.get("sql_count", 0)


def compute_stats(latencies):
    """Compute summary statistics from a list of latencies."""
    latencies.sort()
    n = len(latencies)
    return {
        "avg_ms": statistics.mean(latencies),
        "p50_ms": statistics.median(latencies),
        "p95_ms": latencies[int(n * 0.95)] if n > 0 else 0,
        "p99_ms": latencies[int(n * 0.99)] if n > 0 else 0,
    }


# ── ReFlex system setup/teardown ─────────────────────────────────────────────


def setup_reflex_cluster(extra_jvm_args=""):
    """Start XDN cluster and deploy tpcc-java service.

    Tolerates one unavailable AR node (Paxos needs majority = 2 of 3).
    """
    reachable = get_reachable_hosts()
    print(f"   [setup] Reachable hosts: {reachable} ({len(reachable)}/{len(AR_HOSTS)})")

    print("   [setup] Force-clearing cluster ...")
    clear_xdn_cluster()

    jvm_args = GP_JVM_ARGS
    if extra_jvm_args:
        jvm_args += " " + extra_jvm_args

    print("   [setup] Starting XDN cluster ...")
    start_cluster(GP_CONFIG, jvm_args, SCREEN_SESSION, SCREEN_LOG)

    print("   [setup] Waiting for Active Replicas (reachable only) ...")
    for host in reachable:
        wait_for_port(host, HTTP_PROXY_PORT, 60)

    print("   [setup] Ensuring Docker images on RC ...")
    ensure_docker_images(REQUIRED_IMAGES, [CONTROL_PLANE_HOST], source=reachable[0])

    print("   [setup] Launching tpcc-java service ...")
    cmd = (
        f"XDN_CONTROL_PLANE={CONTROL_PLANE_HOST} {XDN_BINARY} launch "
        f"{SERVICE_NAME} --file={TPCC_JAVA_YAML}"
    )
    print(f"   {cmd}")
    os.system(cmd)

    print("   [setup] Waiting for service readiness ...")
    wait_for_service(reachable, HTTP_PROXY_PORT, SERVICE_NAME, 600)

    print("   [setup] Detecting primary ...")
    primary = detect_primary_via_docker(reachable)
    if not primary:
        primary = reachable[0]
    print(f"   [setup] Primary: {primary}")

    print("   [setup] Initializing TPC-C database ...")
    init_tpcc_db(primary, HTTP_PROXY_PORT, SERVICE_NAME)

    # Disable TCP delayed ACK on Docker bridge routes to prevent 40ms
    # Nagle + delayed ACK penalty on the Netty HTTP forwarder.
    print("   [setup] Setting quickack on Docker bridge routes ...")
    for host in reachable:
        subprocess.run(
            ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=5", host,
             "for br in $(ip link show type bridge | awk -F: '/br-/{print $2}' | tr -d ' '); do "
             "sudo ip route change $(ip route | grep $br | head -1) quickack 1 2>/dev/null || true; "
             "done"],
            capture_output=True, timeout=10,
        )

    # Drain Paxos pipeline with probe requests
    print("   [setup] Draining Paxos pipeline ...")
    url = f"http://{primary}:{HTTP_PROXY_PORT}/orders"
    headers = {"Content-Type": "application/json", "XDN": SERVICE_NAME}
    for i in range(15):
        try:
            payload = make_payload(1)
            req = urllib.request.Request(url, data=payload.encode(),
                                         headers=headers, method="POST")
            urllib.request.urlopen(req, timeout=30).read()
        except Exception:
            pass
        time.sleep(0.5)

    return primary


def teardown_reflex_cluster():
    """Stop and clean up the XDN cluster."""
    try:
        subprocess.run(
            f"yes yes | XDN_CONTROL_PLANE={CONTROL_PLANE_HOST} {XDN_BINARY} "
            f"service destroy {SERVICE_NAME}",
            shell=True, capture_output=True, timeout=30,
        )
    except Exception:
        pass
    clear_xdn_cluster()


# ── Sanity check phase ────────────────────────────────────────────────────────


def run_sanity_check(results_dir, txns_values):
    """Run per-request I/O sanity check for selected txns values."""
    from coordination_size_sanity_check import SanityChecker

    print("\n" + "=" * 60)
    print("  Phase 1: Sanity Check (per-request I/O measurement)")
    print("=" * 60)

    # Setup cluster with sampling enabled
    primary = setup_reflex_cluster(
        extra_jvm_args="-DPB_SAMPLE_LATENCY=true -DXDN_TIMING_HEADERS=true"
    )
    reachable = get_reachable_hosts()
    backups = [h for h in reachable if h != primary]

    # Pick a subset of txns values for sanity check
    sanity_txns = [t for t in [1, 5, 15] if t in txns_values]
    if not sanity_txns:
        sanity_txns = [txns_values[0], txns_values[-1]]

    for txns in sanity_txns:
        print(f"\n   --- Sanity check: txns={txns} ---")

        # Write a single-line payload file for the SanityChecker
        payload_file = results_dir / f"sanity_payload_txns{txns}.txt"
        with open(payload_file, "w") as f:
            f.write(make_payload(txns, autocommit=True) + "\n")

        checker = SanityChecker(
            primary=primary,
            backups=backups,
            port=HTTP_PROXY_PORT,
            service_name=SERVICE_NAME,
            endpoint="/orders",
            db_container_pattern="postgres",
            db_process_name="postgres",
            n_samples=5,
        )
        checker.run(payload_file=payload_file, results_dir=results_dir)

        # Rename output to include txns value
        src = results_dir / "sanity_check.csv"
        dst = results_dir / f"sanity_check_txns{txns}.csv"
        if src.exists():
            shutil.move(str(src), str(dst))
            print(f"   Saved: {dst}")

    # Teardown sanity check cluster
    print("\n   [Sanity Check] Tearing down ...")
    teardown_reflex_cluster()


# ── Main measurement ─────────────────────────────────────────────────────────


def run_reflex_measurements(results_dir, ol_cnt_values, n_samples, n_warmup,
                             autocommit=True):
    """Run latency measurements for the ReFlex system, sweeping ol_cnt."""
    ac_label = "autocommit" if autocommit else "txn"
    print("\n" + "=" * 60)
    print(f"  Phase 2: Latency Measurements (ReFlex, {ac_label})")
    print("=" * 60)

    results = []
    screen_logs = []

    for ol_cnt in ol_cnt_values:
        sql_count = 4 + 4 * ol_cnt  # deterministic
        print(f"\n   --- ol_cnt={ol_cnt} (sql_count={sql_count}) ---")

        primary = setup_reflex_cluster()

        url = f"http://{primary}:{HTTP_PROXY_PORT}/orders"
        headers = {"Content-Type": "application/json", "XDN": SERVICE_NAME}

        # Probe to verify sql_count
        actual_sql = probe_sql_count(url, headers, txns=1, ol_cnt=ol_cnt,
                                      autocommit=autocommit)
        print(f"   verified sql_count={actual_sql} (autocommit={autocommit})")

        # Measure
        print(f"   Measuring: {n_warmup} warmup + {n_samples} samples ...")
        latencies = measure_latency(
            url, headers,
            payload_fn=lambda ol=ol_cnt, ac=autocommit: make_payload(
                txns=1, autocommit=ac, ol_cnt=ol),
            n_warmup=n_warmup,
            n_samples=n_samples,
        )

        stats = compute_stats(latencies)
        print(f"   avg={stats['avg_ms']:.1f}ms  "
              f"p50={stats['p50_ms']:.1f}ms  "
              f"p95={stats['p95_ms']:.1f}ms  "
              f"sql_count={actual_sql}")

        system_name = "reflex" if autocommit else "reflex-txn"
        results.append({
            "system": system_name,
            "ol_cnt": ol_cnt,
            "sql_count": actual_sql,
            **stats,
        })

        # Save screen log
        screen_log_dst = results_dir / f"screen_ol{ol_cnt}.log"
        try:
            if Path(SCREEN_LOG).exists():
                shutil.copy2(SCREEN_LOG, screen_log_dst)
                screen_logs.append(screen_log_dst)
        except Exception:
            pass

        teardown_reflex_cluster()

    # Combined screen log
    if screen_logs:
        combined = results_dir / "screen.log"
        with open(combined, "w") as out:
            for log_path in screen_logs:
                if log_path.exists():
                    out.write(f"\n{'='*60}\n  {log_path.name}\n{'='*60}\n")
                    out.write(log_path.read_text())

    return results


# ── PG Sync system setup/teardown ─────────────────────────────────────────────

# Lazy imports to avoid pulling in legacy modules when not needed.
_pgsync_imported = False
PG_PRIMARY = "10.10.1.1"
PG_REPLICAS = ["10.10.1.2", "10.10.1.3"]
PG_PORT = 5432
PG_PASSWORD = "benchpass"
PG_DB = "tpcc"
PGSYNC_APP_PORT = 8094  # avoid conflict with XDN proxy (2300)


def _import_pgsync():
    global _pgsync_imported, setup_pgsync, teardown_pgsync, check_replication_pgsync, distdb_ssh, distdb_wait_for_port
    if _pgsync_imported:
        return
    # Add legacy_pb_scripts to path so run_load_pb_distdb_app can find its imports
    legacy_dir = str(Path(__file__).resolve().parent / "legacy_pb_scripts")
    if legacy_dir not in sys.path:
        sys.path.insert(0, legacy_dir)
    from run_load_pb_distdb_app import (
        setup_pgsync as _sp,
        teardown_pgsync as _tp,
        check_replication_pgsync as _crp,
        _ssh as _ssh_fn,
        wait_for_port as _wfp,
    )
    setup_pgsync = _sp
    teardown_pgsync = _tp
    check_replication_pgsync = _crp
    distdb_ssh = _ssh_fn
    distdb_wait_for_port = _wfp
    _pgsync_imported = True


def verify_pgsync_is_truly_sync():
    """Verify sync replication by writing a row and checking replica flush_lsn.

    This catches the case where pg_stat_replication shows 'sync' but
    synchronous_commit hasn't actually taken effect (e.g., stale pg_ctl reload).

    Steps:
    1. Check synchronous_commit = 'on' in the running session
    2. Record flush_lsn on replica before write
    3. Write a test row via SQL (not the app)
    4. Confirm flush_lsn advanced on at least one replica
    5. Measure the write latency — if sync is real, it should be ≥1ms
    """
    _import_pgsync()
    print("   [verify] Strong sync replication verification ...")

    def _psql(sql, capture=True):
        return distdb_ssh(
            PG_PRIMARY,
            f'docker exec postgres-primary psql -U postgres -d {PG_DB} -t -A -c "{sql}"',
            capture=capture, check=False,
        )

    def _psql_replica(host, sql, capture=True):
        return distdb_ssh(
            host,
            f'docker exec postgres-replica psql -U postgres -d {PG_DB} -t -A -c "{sql}"',
            capture=capture, check=False,
        )

    # Step 1: Verify synchronous_commit is ON in the session
    result = _psql("SHOW synchronous_commit")
    sc_value = result.stdout.strip() if result.returncode == 0 else "UNKNOWN"
    print(f"   [verify] synchronous_commit = {sc_value}")
    if sc_value != "on":
        raise RuntimeError(
            f"synchronous_commit is '{sc_value}', not 'on'! "
            f"Sync replication is NOT active."
        )

    # Step 2: Get current flush_lsn on replicas
    result = _psql(
        "SELECT client_addr, flush_lsn, sync_state "
        "FROM pg_stat_replication"
    )
    print(f"   [verify] Before write: {result.stdout.strip()}")

    pre_lsns = {}
    for line in result.stdout.strip().splitlines():
        parts = line.split("|")
        if len(parts) >= 2:
            addr, lsn = parts[0].strip(), parts[1].strip()
            pre_lsns[addr] = lsn

    if not pre_lsns:
        raise RuntimeError("No replicas found in pg_stat_replication")

    # Step 3: Create a test table and write a row with timing
    _psql("CREATE TABLE IF NOT EXISTS _sync_verify (id serial, ts timestamptz default now())")
    import time as _time
    t0 = _time.time()
    _psql("INSERT INTO _sync_verify DEFAULT VALUES")
    write_ms = (_time.time() - t0) * 1000
    print(f"   [verify] Test write latency: {write_ms:.2f}ms")

    # Step 4: Check flush_lsn advanced on at least one replica
    _time.sleep(0.5)  # brief settle
    result = _psql(
        "SELECT client_addr, flush_lsn, sync_state "
        "FROM pg_stat_replication"
    )
    print(f"   [verify] After write:  {result.stdout.strip()}")

    advanced = False
    for line in result.stdout.strip().splitlines():
        parts = line.split("|")
        if len(parts) >= 2:
            addr, lsn = parts[0].strip(), parts[1].strip()
            if addr in pre_lsns and lsn != pre_lsns[addr]:
                print(f"   [verify] Replica {addr}: flush_lsn advanced {pre_lsns[addr]} -> {lsn}")
                advanced = True

    if not advanced:
        raise RuntimeError(
            "flush_lsn did NOT advance on any replica after write! "
            "Sync replication may not be working."
        )

    # Step 5: Sanity check — sync write should take at least ~0.5ms
    # (WAL flush + network + replica WAL flush)
    if write_ms < 0.3:
        raise RuntimeError(
            f"Test write completed in {write_ms:.2f}ms — suspiciously fast. "
            f"Sync replication may not be waiting for replica ACK."
        )

    # Also verify via replica that the row is visible
    for replica in PG_REPLICAS:
        try:
            result = _psql_replica(
                replica,
                "SELECT count(*) FROM _sync_verify"
            )
            count = result.stdout.strip()
            print(f"   [verify] Replica {replica}: _sync_verify count = {count}")
        except Exception as e:
            print(f"   [verify] WARNING: could not query replica {replica}: {e}")

    # Cleanup
    _psql("DROP TABLE IF EXISTS _sync_verify")
    print("   [verify] Sync replication verified: write flushed to replica.")


def setup_pgsync_cluster():
    """Start PG sync replication cluster and deploy tpcc-java app."""
    _import_pgsync()
    print("   [setup] Tearing down any existing PG sync cluster ...")
    teardown_pgsync_cluster()

    print("   [setup] Starting PG sync replication cluster ...")
    setup_pgsync()

    print("   [setup] Starting tpcc-java app on primary ...")
    distdb_ssh(PG_PRIMARY, "docker rm -f tpcc-java-app 2>/dev/null || true", check=False)
    cmd = (
        "docker run -d --name tpcc-java-app "
        f"-e DATABASE_URL=postgresql://postgres:{PG_PASSWORD}@{PG_PRIMARY}:{PG_PORT}/{PG_DB} "
        f"-e WAREHOUSES={NUM_WAREHOUSES} "
        f"-p {PGSYNC_APP_PORT}:80 "
        "fadhilkurnia/xdn-tpcc-java"
    )
    distdb_ssh(PG_PRIMARY, cmd)
    print(f"   [setup] Waiting for tpcc-java on {PG_PRIMARY}:{PGSYNC_APP_PORT} ...")
    if not distdb_wait_for_port(PG_PRIMARY, PGSYNC_APP_PORT, timeout_sec=60):
        print("   ERROR: tpcc-java app did not start in time")
        sys.exit(1)
    time.sleep(3)

    # Verify replication (structural check)
    check_replication_pgsync()

    # Strong verification: write a row and confirm it's flushed on a replica
    verify_pgsync_is_truly_sync()

    # Init DB
    print("   [setup] Initializing TPC-C database ...")
    init_tpcc_db(PG_PRIMARY, PGSYNC_APP_PORT, "unused")

    # Drain with probe requests
    print("   [setup] Draining with probe requests ...")
    url = f"http://{PG_PRIMARY}:{PGSYNC_APP_PORT}/orders"
    headers = {"Content-Type": "application/json"}
    for _ in range(10):
        try:
            req = urllib.request.Request(
                url, data=make_payload(1).encode(),
                headers=headers, method="POST")
            urllib.request.urlopen(req, timeout=30).read()
        except Exception:
            pass
        time.sleep(0.3)

    return PG_PRIMARY


def teardown_pgsync_cluster():
    """Tear down PG sync cluster and app."""
    _import_pgsync()
    distdb_ssh(PG_PRIMARY, "docker rm -f tpcc-java-app 2>/dev/null || true", check=False)
    teardown_pgsync()


def run_pgsync_measurements(results_dir, ol_cnt_values, n_samples, n_warmup,
                             autocommit, system_name):
    """Measure latency with PG sync replication, sweeping ol_cnt."""
    print(f"\n{'='*60}")
    print(f"  Phase 2: Latency Measurements ({system_name})")
    print(f"{'='*60}")

    results = []

    for ol_cnt in ol_cnt_values:
        sql_count = 4 + 4 * ol_cnt
        print(f"\n   --- ol_cnt={ol_cnt} (sql_count={sql_count}, autocommit={autocommit}) ---")

        primary = setup_pgsync_cluster()

        url = f"http://{primary}:{PGSYNC_APP_PORT}/orders"
        headers = {"Content-Type": "application/json"}

        # Probe to verify
        actual_sql = probe_sql_count(url, headers, txns=1, autocommit=autocommit, ol_cnt=ol_cnt)
        print(f"   verified sql_count={actual_sql}")

        # Measure
        print(f"   Measuring: {n_warmup} warmup + {n_samples} samples ...")
        latencies = measure_latency(
            url, headers,
            payload_fn=lambda ol=ol_cnt: make_payload(txns=1, autocommit=autocommit, ol_cnt=ol),
            n_warmup=n_warmup,
            n_samples=n_samples,
        )

        stats = compute_stats(latencies)
        print(f"   avg={stats['avg_ms']:.1f}ms  "
              f"p50={stats['p50_ms']:.1f}ms  "
              f"p95={stats['p95_ms']:.1f}ms  "
              f"sql_count={actual_sql}")

        results.append({
            "system": system_name,
            "ol_cnt": ol_cnt,
            "sql_count": actual_sql,
            **stats,
        })

        teardown_pgsync_cluster()

    return results


# ── OpenEBS system setup/teardown ─────────────────────────────────────────────

OPENEBS_K8S_CONTROL = "10.10.1.4"
OPENEBS_WORKER = "10.10.1.1"  # node where app pod runs
OPENEBS_NODEPORT = 30080


def _kubectl(args, manifest=None, check=True, capture=False):
    """Run kubectl on the K8s control plane via SSH."""
    cmd = "kubectl " + " ".join(args)
    if manifest:
        # Strip any trailing "-f -" from args to avoid duplication
        base = cmd.replace(" -f -", "").rstrip()
        full = ["ssh", "-o", "StrictHostKeyChecking=no",
                OPENEBS_K8S_CONTROL, f"cat | {base} -f -"]
        kw = {"check": check, "input": manifest, "text": True}
        if capture:
            kw["capture_output"] = True
        return subprocess.run(full, **kw)
    else:
        full = ["ssh", "-o", "StrictHostKeyChecking=no",
                OPENEBS_K8S_CONTROL, cmd]
        kw = {"check": check}
        if capture:
            kw["capture_output"] = True
        return subprocess.run(full, **kw)


TPCC_JAVA_OPENEBS_PVC = """
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: tpcc-pvc
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 5Gi
  storageClassName: openebs-3-replica
"""

TPCC_JAVA_OPENEBS_DEPLOY = f"""
apiVersion: apps/v1
kind: Deployment
metadata:
  name: tpcc-java
spec:
  replicas: 1
  selector:
    matchLabels:
      app: tpcc-java
  template:
    metadata:
      labels:
        app: tpcc-java
    spec:
      containers:
        - name: postgres
          image: postgres:17.4-bookworm
          imagePullPolicy: IfNotPresent
          env:
            - name: POSTGRES_PASSWORD
              value: benchpass
            - name: POSTGRES_DB
              value: tpcc
            - name: PGDATA
              value: /data/pgdata
          ports:
            - containerPort: 5432
          volumeMounts:
            - name: app-data
              mountPath: /data
        - name: tpcc-app
          image: fadhilkurnia/xdn-tpcc-java
          imagePullPolicy: Never
          env:
            - name: DATABASE_URL
              value: postgresql://postgres:benchpass@localhost:5432/tpcc
            - name: WAREHOUSES
              value: "10"
          ports:
            - containerPort: 80
      volumes:
        - name: app-data
          persistentVolumeClaim:
            claimName: tpcc-pvc
"""

TPCC_JAVA_OPENEBS_SVC = f"""
apiVersion: v1
kind: Service
metadata:
  name: tpcc-java-svc
spec:
  type: NodePort
  selector:
    app: tpcc-java
  ports:
    - port: 80
      targetPort: 80
      nodePort: {OPENEBS_NODEPORT}
"""


def setup_openebs_cluster():
    """Deploy tpcc-java on K8s with OpenEBS PVC."""
    print("   [setup] Cleaning up previous deployment ...")
    _kubectl(["delete", "deploy", "tpcc-java"], check=False)
    _kubectl(["delete", "svc", "tpcc-java-svc"], check=False)
    _kubectl(["delete", "pvc", "tpcc-pvc"], check=False)
    time.sleep(5)

    print("   [setup] Creating PVC ...")
    _kubectl(["apply", "-f", "-"], manifest=TPCC_JAVA_OPENEBS_PVC)

    print("   [setup] Creating Deployment ...")
    _kubectl(["apply", "-f", "-"], manifest=TPCC_JAVA_OPENEBS_DEPLOY)

    print("   [setup] Creating Service ...")
    _kubectl(["apply", "-f", "-"], manifest=TPCC_JAVA_OPENEBS_SVC)

    print("   [setup] Waiting for rollout ...")
    _kubectl(["rollout", "status", "deployment/tpcc-java", "--timeout=300s"])

    print(f"   [setup] Waiting for HTTP on {OPENEBS_WORKER}:{OPENEBS_NODEPORT} ...")
    deadline = time.time() + 300
    while time.time() < deadline:
        try:
            req = urllib.request.Request(
                f"http://{OPENEBS_WORKER}:{OPENEBS_NODEPORT}/health",
                method="GET")
            resp = urllib.request.urlopen(req, timeout=5)
            if resp.status < 500:
                print("   [setup] App ready.")
                break
        except Exception:
            pass
        time.sleep(5)

    print("   [setup] Initializing TPC-C database ...")
    init_tpcc_db(OPENEBS_WORKER, OPENEBS_NODEPORT)

    # Warmup
    url = f"http://{OPENEBS_WORKER}:{OPENEBS_NODEPORT}/orders"
    headers = {"Content-Type": "application/json"}
    for _ in range(10):
        try:
            req = urllib.request.Request(
                url, data=make_payload(1, ol_cnt=5).encode(),
                headers=headers, method="POST")
            urllib.request.urlopen(req, timeout=30).read()
        except Exception:
            pass


def teardown_openebs_cluster():
    """Remove tpcc-java K8s resources."""
    _kubectl(["delete", "deploy", "tpcc-java"], check=False)
    _kubectl(["delete", "svc", "tpcc-java-svc"], check=False)
    _kubectl(["delete", "pvc", "tpcc-pvc"], check=False)


def run_openebs_measurements(results_dir, ol_cnt_values, n_samples, n_warmup):
    """Measure latency with OpenEBS block-level replication, sweeping ol_cnt."""
    print(f"\n{'='*60}")
    print(f"  Phase 2: Latency Measurements (openebs)")
    print(f"{'='*60}")

    results = []

    for ol_cnt in ol_cnt_values:
        sql_count = 4 + 4 * ol_cnt
        print(f"\n   --- ol_cnt={ol_cnt} (sql_count={sql_count}) ---")

        setup_openebs_cluster()

        url = f"http://{OPENEBS_WORKER}:{OPENEBS_NODEPORT}/orders"
        headers = {"Content-Type": "application/json"}

        # Probe to verify
        actual_sql = probe_sql_count(url, headers, txns=1, ol_cnt=ol_cnt)
        print(f"   verified sql_count={actual_sql}")

        # Measure
        print(f"   Measuring: {n_warmup} warmup + {n_samples} samples ...")
        latencies = measure_latency(
            url, headers,
            payload_fn=lambda ol=ol_cnt: make_payload(txns=1, autocommit=True, ol_cnt=ol),
            n_warmup=n_warmup,
            n_samples=n_samples,
        )

        stats = compute_stats(latencies)
        print(f"   avg={stats['avg_ms']:.1f}ms  "
              f"p50={stats['p50_ms']:.1f}ms  "
              f"p95={stats['p95_ms']:.1f}ms  "
              f"sql_count={actual_sql}")

        results.append({
            "system": "openebs",
            "ol_cnt": ol_cnt,
            "sql_count": actual_sql,
            **stats,
        })

        teardown_openebs_cluster()

    return results


# ── CSV output ────────────────────────────────────────────────────────────────


def write_results_csv(results_dir, results, system):
    """Write results to CSV."""
    csv_path = results_dir / "coordination_granularity.csv"
    fieldnames = ["system", "ol_cnt", "sql_count", "avg_ms", "p50_ms", "p95_ms", "p99_ms"]
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in results:
            writer.writerow({k: row.get(k, "") for k in fieldnames})
    print(f"\n   Results saved to {csv_path}")

    # Print summary table
    print(f"\n   {'ol_cnt':>6}  {'sql_count':>10}  {'avg_ms':>8}  {'p50_ms':>8}  {'p95_ms':>8}")
    print("   " + "-" * 50)
    for row in results:
        print(f"   {row['ol_cnt']:>6}  {row['sql_count']:>10}  "
              f"{row['avg_ms']:>7.1f}  {row['p50_ms']:>7.1f}  {row['p95_ms']:>7.1f}")

    return csv_path


# ── Main ──────────────────────────────────────────────────────────────────────


def parse_args():
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--system", required=True,
                   choices=["reflex", "reflex-txn", "pgsync-txn", "pgsync-stmt", "openebs"],
                   help="System to benchmark")
    p.add_argument("--txns", type=str,
                   default=",".join(str(t) for t in DEFAULT_TXNS),
                   help="Comma-separated txns values (legacy, default: 1,2,3,5,8,10,15)")
    p.add_argument("--ol-cnt", type=str,
                   default=",".join(str(v) for v in DEFAULT_OL_CNT),
                   help="Comma-separated order line counts to sweep (default: 1,2,3,5,8,10,15)")
    p.add_argument("--n-samples", type=int, default=DEFAULT_N_SAMPLES,
                   help="Number of measurement samples per txns value (default: 50)")
    p.add_argument("--n-warmup", type=int, default=DEFAULT_N_WARMUP,
                   help="Number of warmup requests to discard (default: 10)")
    p.add_argument("--sanity-check", action="store_true",
                   help="Run per-request I/O sanity check before measurements")
    p.add_argument("--autocommit", type=str, default="true",
                   choices=["true", "false"],
                   help="JDBC autocommit mode (default: true)")
    p.add_argument("--gp-config", type=str, default=None,
                   help="Override GigaPaxos config file (e.g., for alternate node layout)")
    return p.parse_args()


def parse_ar_hosts_from_config(config_path):
    """Parse active replica hosts from a GigaPaxos properties file."""
    import re
    hosts = []
    with open(config_path) as f:
        for line in f:
            m = re.match(r"^\s*active\.\w+=\s*([^:\s]+):(\d+)", line)
            if m:
                hosts.append(m.group(1))
    return list(dict.fromkeys(hosts))  # deduplicate preserving order


if __name__ == "__main__":
    args = parse_args()
    ol_cnt_values = [int(v.strip()) for v in args.ol_cnt.split(",")]

    # Override config and AR hosts if specified
    if args.gp_config:
        globals()["GP_CONFIG"] = args.gp_config
        AR_HOSTS[:] = parse_ar_hosts_from_config(args.gp_config)
        print(f"  Using config: {args.gp_config}")
        print(f"  AR hosts:     {AR_HOSTS}")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_dir = RESULTS_BASE / f"microbench_coordination_{args.system}_{timestamp}"
    results_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("  Coordination Granularity Microbenchmark")
    print(f"  System:    {args.system}")
    print(f"  ol_cnt:    {ol_cnt_values}")
    print(f"  sql_count: {[4 + 4*v for v in ol_cnt_values]}")
    print(f"  Samples:   {args.n_samples} (warmup: {args.n_warmup})")
    print(f"  Results:   {results_dir}")
    print("=" * 60)

    # Phase 1: Sanity check (optional, only for reflex)
    if args.sanity_check and args.system == "reflex":
        run_sanity_check(results_dir, ol_cnt_values)

    # Phase 2: Latency measurements
    autocommit = args.autocommit == "true"
    if args.system == "reflex":
        results = run_reflex_measurements(
            results_dir, ol_cnt_values, args.n_samples, args.n_warmup,
            autocommit=True)
    elif args.system == "reflex-txn":
        results = run_reflex_measurements(
            results_dir, ol_cnt_values, args.n_samples, args.n_warmup,
            autocommit=False)
    elif args.system == "pgsync-txn":
        results = run_pgsync_measurements(
            results_dir, ol_cnt_values, args.n_samples, args.n_warmup,
            autocommit=False, system_name="pgsync-txn")
    elif args.system == "pgsync-stmt":
        results = run_pgsync_measurements(
            results_dir, ol_cnt_values, args.n_samples, args.n_warmup,
            autocommit=True, system_name="pgsync-stmt")
    elif args.system == "openebs":
        results = run_openebs_measurements(
            results_dir, ol_cnt_values, args.n_samples, args.n_warmup)

    if results:
        write_results_csv(results_dir, results, args.system)

    # Symlink for convenience
    symlink = RESULTS_BASE / f"microbench_coordination_{args.system}"
    symlink.unlink(missing_ok=True)
    symlink.symlink_to(results_dir.name)
    print(f"   Symlink: {symlink} -> {results_dir.name}")

    print(f"\n[Done] Results in {results_dir}/")
