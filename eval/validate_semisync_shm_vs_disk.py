"""
validate_semisync_shm_vs_disk.py — Validate whether /dev/shm vs disk storage
makes a significant difference for MySQL semi-sync replication throughput.

Runs the same WordPress editPost workload with semi-sync replication, once
with MySQL data on /dev/shm (tmpfs) and once on /tmp (disk), then compares.

Run from the eval/ directory:
    python3 validate_semisync_shm_vs_disk.py
    python3 validate_semisync_shm_vs_disk.py --rates 500,1000,1500 --duration 30
"""

import argparse
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from run_load_pb_wordpress_common import (
    ADMIN_PASSWORD,
    ADMIN_USER,
    install_wordpress,
    wait_for_port,
)
from run_load_pb_wordpress_reflex import (
    _parse_go_output,
    _xmlrpc_editpost_payload,
)

# ── Config ────────────────────────────────────────────────────────────────────

PRIMARY_HOST = "10.10.1.1"
REPLICA_HOST = "10.10.1.2"
MYSQL_PORT = 3307
WP_PORT = 8092
MYSQL_ROOT_PASS = "supersecret"
MYSQL_DB = "wordpress"
REPL_USER = "repl"
REPL_PASS = "replpass"
SEED_POST_COUNT = 200

DEFAULT_RATES = [500, 1000, 1500]
LOAD_DURATION_SEC = 30
WARMUP_DURATION_SEC = 10

MOUNT_CONFIGS = {
    "shm":  "/dev/shm/validate_semisync_mysql",
    "disk": "/tmp/validate_semisync_mysql",
}

RESULTS_BASE = Path(__file__).resolve().parent / "results"
GO_LATENCY_CLIENT = Path(__file__).resolve().parent / "get_latency_at_rate.go"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _ssh(host, cmd, check=True, capture=False):
    return subprocess.run(
        ["ssh", "-o", "StrictHostKeyChecking=no", host, cmd],
        check=check, capture_output=capture, text=True,
    )


def cleanup(hosts):
    print("   Cleaning up containers ...")
    for host in hosts:
        _ssh(host,
             "docker stop mysql-val-primary mysql-val-replica wordpress-val 2>/dev/null || true; "
             "docker rm -f mysql-val-primary mysql-val-replica wordpress-val 2>/dev/null || true; "
             "docker network rm val-net 2>/dev/null || true",
             check=False)


def setup_semisync(primary_data_dir, replica_data_dir):
    """Start MySQL primary+replica with semi-sync, WordPress on primary."""

    # Primary
    print(f"   Starting MySQL primary (data: {primary_data_dir}) ...")
    _ssh(PRIMARY_HOST, f"mkdir -p {primary_data_dir}")
    primary_cmd = (
        f"docker run -d --name mysql-val-primary "
        f"-e MYSQL_ROOT_PASSWORD={MYSQL_ROOT_PASS} "
        f"-e MYSQL_DATABASE={MYSQL_DB} "
        f"-v {primary_data_dir}:/var/lib/mysql "
        f"-p {MYSQL_PORT}:3306 "
        f"mysql:8.4.0 "
        f"--server-id=1 "
        f"--log-bin=mysql-bin "
        f"--binlog-format=ROW "
        f"--gtid-mode=ON "
        f"--enforce-gtid-consistency=ON "
        f"--plugin-load-add=semisync_source.so "
        f"--loose-rpl_semi_sync_source_enabled=ON "
        f"--loose-rpl_semi_sync_source_wait_for_replica_count=1 "
        f"--loose-rpl_semi_sync_source_timeout=30000"
    )
    _ssh(PRIMARY_HOST, primary_cmd)
    if not wait_for_port(PRIMARY_HOST, MYSQL_PORT, timeout_sec=120):
        print("ERROR: MySQL primary not ready")
        sys.exit(1)
    time.sleep(10)

    # Create replication user
    print("   Creating replication user ...")
    for sql in [
        f"CREATE USER IF NOT EXISTS '{REPL_USER}'@'%' IDENTIFIED BY '{REPL_PASS}'",
        f"GRANT REPLICATION SLAVE ON *.* TO '{REPL_USER}'@'%'",
        "FLUSH PRIVILEGES",
    ]:
        _ssh(PRIMARY_HOST,
             f"docker exec mysql-val-primary mysql -p{MYSQL_ROOT_PASS} -e \"{sql}\"",
             capture=True)

    # Replica
    print(f"   Starting MySQL replica (data: {replica_data_dir}) ...")
    _ssh(REPLICA_HOST, f"mkdir -p {replica_data_dir}")
    replica_cmd = (
        f"docker run -d --name mysql-val-replica "
        f"-e MYSQL_ROOT_PASSWORD={MYSQL_ROOT_PASS} "
        f"-v {replica_data_dir}:/var/lib/mysql "
        f"-p {MYSQL_PORT}:3306 "
        f"mysql:8.4.0 "
        f"--server-id=2 "
        f"--log-bin=mysql-bin "
        f"--binlog-format=ROW "
        f"--gtid-mode=ON "
        f"--enforce-gtid-consistency=ON "
        f"--read-only=ON "
        f"--plugin-load-add=semisync_replica.so "
        f"--loose-rpl_semi_sync_replica_enabled=ON"
    )
    _ssh(REPLICA_HOST, replica_cmd)
    if not wait_for_port(REPLICA_HOST, MYSQL_PORT, timeout_sec=120):
        print("ERROR: MySQL replica not ready")
        sys.exit(1)
    time.sleep(10)

    # Configure replication
    print("   Configuring replication ...")
    change_sql = (
        f"CHANGE REPLICATION SOURCE TO "
        f"SOURCE_HOST='{PRIMARY_HOST}', SOURCE_PORT={MYSQL_PORT}, "
        f"SOURCE_USER='{REPL_USER}', SOURCE_PASSWORD='{REPL_PASS}', "
        f"SOURCE_AUTO_POSITION=1, GET_SOURCE_PUBLIC_KEY=1"
    )
    _ssh(REPLICA_HOST,
         f'docker exec mysql-val-replica mysql -p{MYSQL_ROOT_PASS} -e "{change_sql}"',
         capture=True)
    _ssh(REPLICA_HOST,
         f"docker exec mysql-val-replica mysql -p{MYSQL_ROOT_PASS} -e 'START REPLICA'",
         capture=True)

    # Wait for replication to start
    print("   Waiting for replication sync ...")
    deadline = time.time() + 60
    while time.time() < deadline:
        result = _ssh(REPLICA_HOST,
                      f"docker exec mysql-val-replica mysql -p{MYSQL_ROOT_PASS} "
                      f"-e 'SHOW REPLICA STATUS\\G'",
                      check=False, capture=True)
        if "Replica_IO_Running: Yes" in result.stdout and "Replica_SQL_Running: Yes" in result.stdout:
            print("   Replication running")
            break
        time.sleep(2)
    else:
        print("WARNING: Replication may not be fully synced")

    # Verify semi-sync is active
    result = _ssh(PRIMARY_HOST,
                  f"docker exec mysql-val-primary mysql -p{MYSQL_ROOT_PASS} "
                  f"-e \"SHOW STATUS LIKE 'Rpl_semi_sync_source_status';\"",
                  capture=True)
    print(f"   Semi-sync status: {result.stdout.strip()}")

    # WordPress
    print("   Starting WordPress ...")
    wp_config_extra = (
        "define('DISABLE_WP_CRON', true); "
        "define('AUTOMATIC_UPDATER_DISABLED', true); "
        "define('WP_AUTO_UPDATE_CORE', false);"
    )
    wp_cmd = (
        f"docker run -d --name wordpress-val "
        f"-e WORDPRESS_DB_HOST={PRIMARY_HOST}:{MYSQL_PORT} "
        f"-e WORDPRESS_DB_USER=root "
        f"-e WORDPRESS_DB_PASSWORD={MYSQL_ROOT_PASS} "
        f"-e WORDPRESS_DB_NAME={MYSQL_DB} "
        f'-e WORDPRESS_CONFIG_EXTRA="{wp_config_extra}" '
        f"-p {WP_PORT}:80 "
        f"wordpress:6.5.4-apache"
    )
    _ssh(PRIMARY_HOST, wp_cmd)
    if not wait_for_port(PRIMARY_HOST, WP_PORT, timeout_sec=120):
        print("ERROR: WordPress not ready")
        sys.exit(1)
    time.sleep(5)

    # Install WordPress
    print("   Installing WordPress ...")
    ok = install_wordpress(PRIMARY_HOST, WP_PORT, "validate_semisync")
    if not ok:
        print("ERROR: WordPress install failed")
        sys.exit(1)

    # Verify WordPress is responding to XML-RPC
    print("   Checking WordPress XML-RPC readiness ...")
    import urllib.request
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            req = urllib.request.Request(
                f"http://{PRIMARY_HOST}:{WP_PORT}/xmlrpc.php",
                data=b'<?xml version="1.0"?><methodCall><methodName>system.listMethods</methodName></methodCall>',
                headers={"Content-Type": "text/xml"},
                method="POST",
            )
            resp = urllib.request.urlopen(req, timeout=5)
            if resp.status == 200:
                print("   WordPress XML-RPC ready")
                break
        except Exception:
            pass
        time.sleep(2)
    else:
        print("ERROR: WordPress XML-RPC not available")
        sys.exit(1)

    # Tune MySQL
    print("   Tuning MySQL ...")
    tune_sql = (
        "SET GLOBAL innodb_buffer_pool_size=1073741824; "
        "SET GLOBAL innodb_change_buffer_max_size=50; "
        "SET GLOBAL innodb_max_dirty_pages_pct=90; "
        "SET GLOBAL innodb_max_dirty_pages_pct_lwm=80; "
        "SET GLOBAL innodb_io_capacity=200; "
        "SET GLOBAL innodb_io_capacity_max=400; "
        "SET GLOBAL innodb_lru_scan_depth=256; "
    )
    _ssh(PRIMARY_HOST,
         f"docker exec mysql-val-primary mysql -p{MYSQL_ROOT_PASS} -e \"{tune_sql}\"",
         check=False, capture=True)
    time.sleep(2)


def run_load(host, port, rate, duration_sec, payloads_file, output_file):
    url = f"http://{host}:{port}/xmlrpc.php"
    cmd = [
        "go", "run", str(GO_LATENCY_CLIENT),
        "-H", "Content-Type: text/xml",
        "-X", "POST",
        "-payloads-file", str(payloads_file),
        url, "placeholder", str(duration_sec), str(rate),
    ]
    env = os.environ.copy()
    env["GO111MODULE"] = "off"
    env["GOWORK"] = "off"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w") as fh:
        subprocess.run(cmd, stdout=fh, stderr=subprocess.STDOUT,
                       env=env, text=True, cwd=str(GO_LATENCY_CLIENT.parent))
    return _parse_go_output(output_file)


# ── Main ──────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--rates", type=str, default=",".join(str(r) for r in DEFAULT_RATES),
                   help="Comma-separated rates (default: 500,1000,1500)")
    p.add_argument("--duration", type=int, default=LOAD_DURATION_SEC,
                   help=f"Measurement duration per rate (default: {LOAD_DURATION_SEC})")
    p.add_argument("--warmup-duration", type=int, default=WARMUP_DURATION_SEC,
                   help=f"Warmup duration per rate (default: {WARMUP_DURATION_SEC})")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    rates = [int(r.strip()) for r in args.rates.split(",")]
    load_duration = args.duration
    warmup_duration = args.warmup_duration

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_dir = RESULTS_BASE / f"validate_semisync_shm_vs_disk_{timestamp}"
    results_dir.mkdir(parents=True, exist_ok=True)

    all_hosts = [PRIMARY_HOST, REPLICA_HOST]

    print("=" * 60)
    print("Semi-Sync: /dev/shm vs /tmp Storage Comparison")
    print("=" * 60)
    print(f"  Primary  : {PRIMARY_HOST}")
    print(f"  Replica  : {REPLICA_HOST}")
    print(f"  Rates    : {rates}")
    print(f"  Duration : {load_duration}s (warmup: {warmup_duration}s)")
    print(f"  Results  : {results_dir}")
    print("=" * 60)

    all_results = {}

    for mount_name, data_dir in MOUNT_CONFIGS.items():
        primary_data = f"{data_dir}_primary"
        replica_data = f"{data_dir}_replica"

        print(f"\n{'='*60}")
        print(f"  Testing: {mount_name} ({data_dir})")
        print(f"{'='*60}")

        # Cleanup
        cleanup(all_hosts)
        _ssh(PRIMARY_HOST, f"sudo rm -rf {primary_data}", check=False)
        _ssh(REPLICA_HOST, f"sudo rm -rf {replica_data}", check=False)

        # Setup
        print("\n[1] Setting up semi-sync cluster ...")
        setup_semisync(primary_data, replica_data)

        # Seed posts via XML-RPC (no Application Password needed)
        print(f"\n[2] Seeding {SEED_POST_COUNT} posts via XML-RPC ...")
        import urllib.request
        seed_ids = []
        for i in range(SEED_POST_COUNT):
            payload = (
                '<?xml version="1.0"?>'
                '<methodCall><methodName>wp.newPost</methodName><params>'
                '<param><value><int>1</int></value></param>'
                f'<param><value><string>{ADMIN_USER}</string></value></param>'
                f'<param><value><string>{ADMIN_PASSWORD}</string></value></param>'
                '<param><value><struct>'
                f'<member><name>post_title</name><value><string>Seed Post {i+1}</string></value></member>'
                f'<member><name>post_content</name><value><string>Content {i+1}.</string></value></member>'
                '<member><name>post_status</name><value><string>publish</string></value></member>'
                '</struct></value></param>'
                '</params></methodCall>'
            )
            try:
                req = urllib.request.Request(
                    f"http://{PRIMARY_HOST}:{WP_PORT}/xmlrpc.php",
                    data=payload.encode(),
                    headers={"Content-Type": "text/xml"},
                )
                resp = urllib.request.urlopen(req, timeout=15)
                body = resp.read().decode()
                # Extract post ID from <int>N</int> or <string>N</string> response
                import re
                m = re.search(r'<(?:int|string)>(\d+)</(?:int|string)>', body)
                if m:
                    seed_ids.append(int(m.group(1)))
                elif i < 3:
                    # Log first few failures for debugging
                    snippet = body[:200] if 'faultString' in body else body[:100]
                    print(f"   WARNING: seed {i+1} unexpected response: {snippet}")
            except Exception as e:
                if i < 3:
                    print(f"   WARNING: seed post {i+1} failed: {e}")
        print(f"   Seeded {len(seed_ids)}/{SEED_POST_COUNT} posts")
        if len(seed_ids) == 0:
            print("ERROR: No posts seeded — cannot run load test. Check WordPress auth.")
            cleanup(all_hosts)
            continue

        # Write payloads
        payloads_file = results_dir / f"payloads_{mount_name}.txt"
        with open(payloads_file, "w") as fh:
            for pid in seed_ids:
                fh.write(_xmlrpc_editpost_payload(pid, ADMIN_PASSWORD) + "\n")

        # Verify semi-sync is still active after seeding
        result = _ssh(PRIMARY_HOST,
                      f"docker exec mysql-val-primary mysql -p{MYSQL_ROOT_PASS} "
                      f"-e \"SHOW STATUS LIKE 'Rpl_semi_sync_source%';\"",
                      capture=True)
        print(f"\n   Semi-sync after seeding:\n{result.stdout.strip()}")

        # Load sweep
        print(f"\n[3] Load sweep ...")
        mount_results = []
        for rate in rates:
            print(f"\n   -> rate={rate} (warmup {warmup_duration}s) ...")
            warmup_out = results_dir / f"{mount_name}_warmup_rate{rate}.txt"
            run_load(PRIMARY_HOST, WP_PORT, rate, warmup_duration,
                     payloads_file, warmup_out)
            time.sleep(5)

            print(f"   -> rate={rate} (measuring {load_duration}s) ...")
            output_file = results_dir / f"{mount_name}_rate{rate}.txt"
            m = run_load(PRIMARY_HOST, WP_PORT, rate, load_duration,
                         payloads_file, output_file)
            mount_results.append({"rate_rps": rate, **m})
            print(f"     tput={m['throughput_rps']:.1f} rps  "
                  f"avg={m['avg_ms']:.1f}ms  p50={m['p50_ms']:.1f}ms  "
                  f"p95={m['p95_ms']:.1f}ms")

            if m["avg_ms"] > 1000:
                print(f"     Saturated, stopping sweep")
                break
            time.sleep(3)

        all_results[mount_name] = mount_results

        # Cleanup
        cleanup(all_hosts)
        _ssh(PRIMARY_HOST, f"sudo rm -rf {primary_data}", check=False)
        _ssh(REPLICA_HOST, f"sudo rm -rf {replica_data}", check=False)

    # ── Summary ───────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print("Summary")
    print(f"{'='*60}")

    for mount_name, mount_results in all_results.items():
        print(f"\n  {mount_name}:")
        print(f"   {'rate':>6}  {'tput':>8}  {'avg':>8}  {'p50':>8}  {'p95':>8}  {'p99':>8}")
        print(f"   {'-'*56}")
        for row in mount_results:
            print(f"   {row['rate_rps']:>5}  "
                  f"{row['throughput_rps']:>7.1f}  "
                  f"{row['avg_ms']:>7.1f}ms  "
                  f"{row['p50_ms']:>7.1f}ms  "
                  f"{row['p95_ms']:>7.1f}ms  "
                  f"{row['p99_ms']:>7.1f}ms")

    if "shm" in all_results and "disk" in all_results:
        print(f"\n  Comparison (shm vs disk):")
        print(f"   {'rate':>6}  {'shm avg':>10}  {'disk avg':>10}  {'speedup':>8}")
        print(f"   {'-'*42}")
        shm_by_rate = {r["rate_rps"]: r for r in all_results["shm"]}
        disk_by_rate = {r["rate_rps"]: r for r in all_results["disk"]}
        for rate in rates:
            if rate in shm_by_rate and rate in disk_by_rate:
                s = shm_by_rate[rate]["avg_ms"]
                d = disk_by_rate[rate]["avg_ms"]
                speedup = d / s if s > 0 else float('inf')
                print(f"   {rate:>5}  {s:>9.1f}ms  {d:>9.1f}ms  {speedup:>7.2f}x")

    print(f"\n[Done] Results in {results_dir}/")
