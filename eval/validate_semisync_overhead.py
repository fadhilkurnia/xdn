"""
validate_semisync_overhead.py — Isolate MySQL semi-sync replication overhead
by comparing three configurations with identical WordPress setup:

  1. no_repl:  MySQL standalone (no replicas)
  2. async:    MySQL with 1 replica, async replication (semi-sync OFF)
  3. semisync: MySQL with 1 replica, semi-sync ON (wait for 1 ACK)

All three use the same Docker networking, same published ports, same MySQL
config, same WordPress version. Only the replication mode differs.

Run from the eval/ directory:
    python3 validate_semisync_overhead.py
    python3 validate_semisync_overhead.py --rates 500,1000,1500,2000 --duration 30
"""

import argparse
import os
import re
import subprocess
import sys
import time
import urllib.request
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
WP_PORT = 8093
MYSQL_ROOT_PASS = "supersecret"
MYSQL_DB = "wordpress"
REPL_USER = "repl"
REPL_PASS = "replpass"
SEED_POST_COUNT = 200
WARMUP_RATE = 1500

DEFAULT_RATES = [500, 1000, 1500, 2000]
LOAD_DURATION_SEC = 30
WARMUP_DURATION_SEC = 10

RESULTS_BASE = Path(__file__).resolve().parent / "results"
GO_LATENCY_CLIENT = Path(__file__).resolve().parent / "get_latency_at_rate.go"

# Three configurations to test
CONFIGS = {
    "no_repl": {
        "description": "MySQL standalone (no replication)",
        "semisync_source": False,
        "start_replica": False,
    },
    "async": {
        "description": "MySQL async replication (semi-sync OFF)",
        "semisync_source": False,
        "start_replica": True,
    },
    "semisync": {
        "description": "MySQL semi-sync replication (wait for 1 ACK)",
        "semisync_source": True,
        "start_replica": True,
    },
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _ssh(host, cmd, check=True, capture=False):
    return subprocess.run(
        ["ssh", "-o", "StrictHostKeyChecking=no", host, cmd],
        check=check, capture_output=capture, text=True,
    )


def cleanup():
    print("   Cleaning up ...")
    for host in [PRIMARY_HOST, REPLICA_HOST]:
        _ssh(host,
             "docker stop mysql-val-primary mysql-val-replica wp-val 2>/dev/null || true; "
             "docker rm -f mysql-val-primary mysql-val-replica wp-val 2>/dev/null || true; "
             "docker network rm val-net 2>/dev/null || true",
             check=False)


def setup_cluster(config_name):
    """Start MySQL primary (+ optional replica) and WordPress."""
    cfg = CONFIGS[config_name]
    print(f"\n   Config: {cfg['description']}")

    # Docker network for WordPress <-> MySQL
    _ssh(PRIMARY_HOST, "docker network create val-net 2>/dev/null || true", check=False)

    # Primary MySQL
    semisync_args = ""
    if cfg["semisync_source"]:
        semisync_args = (
            "--plugin-load-add=semisync_source.so "
            "--loose-rpl_semi_sync_source_enabled=ON "
            "--loose-rpl_semi_sync_source_wait_for_replica_count=1 "
            "--loose-rpl_semi_sync_source_timeout=30000 "
        )

    repl_args = ""
    if cfg["start_replica"]:
        repl_args = (
            "--log-bin=mysql-bin "
            "--binlog-format=ROW "
            "--gtid-mode=ON "
            "--enforce-gtid-consistency=ON "
        )

    primary_cmd = (
        f"docker run -d --name mysql-val-primary --network val-net "
        f"-e MYSQL_ROOT_PASSWORD={MYSQL_ROOT_PASS} "
        f"-e MYSQL_DATABASE={MYSQL_DB} "
        f"-p {MYSQL_PORT}:3306 "
        f"mysql:8.4.0 "
        f"--server-id=1 {repl_args}{semisync_args}"
    )
    print(f"   Starting MySQL primary ...")
    _ssh(PRIMARY_HOST, primary_cmd)
    if not wait_for_port(PRIMARY_HOST, MYSQL_PORT, timeout_sec=120):
        print("ERROR: MySQL primary not ready")
        sys.exit(1)
    time.sleep(10)

    # Create replication user if needed
    if cfg["start_replica"]:
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
    if cfg["start_replica"]:
        replica_semisync = ""
        if cfg["semisync_source"]:
            replica_semisync = (
                "--plugin-load-add=semisync_replica.so "
                "--loose-rpl_semi_sync_replica_enabled=ON "
            )

        replica_cmd = (
            f"docker run -d --name mysql-val-replica "
            f"-e MYSQL_ROOT_PASSWORD={MYSQL_ROOT_PASS} "
            f"-p {MYSQL_PORT}:3306 "
            f"mysql:8.4.0 "
            f"--server-id=2 "
            f"--log-bin=mysql-bin --binlog-format=ROW "
            f"--gtid-mode=ON --enforce-gtid-consistency=ON "
            f"--read-only=ON {replica_semisync}"
        )
        print(f"   Starting MySQL replica on {REPLICA_HOST} ...")
        _ssh(REPLICA_HOST, replica_cmd)
        if not wait_for_port(REPLICA_HOST, MYSQL_PORT, timeout_sec=120):
            print("ERROR: MySQL replica not ready")
            sys.exit(1)
        time.sleep(10)

        # Configure replication (use Docker network name, not published port)
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

        # Wait for replication
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

    # Verify semi-sync status
    if cfg["semisync_source"]:
        result = _ssh(PRIMARY_HOST,
                      f"docker exec mysql-val-primary mysql -p{MYSQL_ROOT_PASS} "
                      f"-e \"SHOW STATUS LIKE 'Rpl_semi_sync_source_status';\"",
                      capture=True)
        status_line = [l for l in result.stdout.splitlines() if "Rpl_semi_sync" in l]
        print(f"   Semi-sync: {status_line[0] if status_line else 'unknown'}")

    # WordPress (connected via Docker network to MySQL)
    wp_config_extra = (
        "define('DISABLE_WP_CRON', true); "
        "define('AUTOMATIC_UPDATER_DISABLED', true); "
        "define('WP_AUTO_UPDATE_CORE', false);"
    )
    wp_cmd = (
        f"docker run -d --name wp-val --network val-net "
        f"-e WORDPRESS_DB_HOST=mysql-val-primary:3306 "
        f"-e WORDPRESS_DB_USER=root "
        f"-e WORDPRESS_DB_PASSWORD={MYSQL_ROOT_PASS} "
        f"-e WORDPRESS_DB_NAME={MYSQL_DB} "
        f'-e WORDPRESS_CONFIG_EXTRA="{wp_config_extra}" '
        f"-p {WP_PORT}:80 "
        f"wordpress:6.5.4-apache"
    )
    print("   Starting WordPress ...")
    _ssh(PRIMARY_HOST, wp_cmd)
    if not wait_for_port(PRIMARY_HOST, WP_PORT, timeout_sec=120):
        print("ERROR: WordPress not ready")
        sys.exit(1)
    time.sleep(5)

    # Install WordPress
    print("   Installing WordPress ...")
    ok = install_wordpress(PRIMARY_HOST, WP_PORT, "validate_overhead")
    if not ok:
        print("ERROR: WordPress install failed")
        sys.exit(1)

    # XML-RPC readiness check
    print("   Checking XML-RPC readiness ...")
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            req = urllib.request.Request(
                f"http://{PRIMARY_HOST}:{WP_PORT}/xmlrpc.php",
                data=b'<?xml version="1.0"?><methodCall><methodName>system.listMethods</methodName></methodCall>',
                headers={"Content-Type": "text/xml"},
            )
            resp = urllib.request.urlopen(req, timeout=5)
            if resp.status == 200:
                print("   XML-RPC ready")
                break
        except Exception:
            pass
        time.sleep(2)

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


def seed_posts():
    """Seed posts via XML-RPC wp.newPost."""
    print(f"   Seeding {SEED_POST_COUNT} posts ...")
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
            m = re.search(r'<(?:int|string)>(\d+)</(?:int|string)>', body)
            if m:
                seed_ids.append(int(m.group(1)))
            elif i < 3:
                print(f"   WARNING: seed {i+1}: {body[:150]}")
        except Exception as e:
            if i < 3:
                print(f"   WARNING: seed {i+1} failed: {e}")
    print(f"   Seeded {len(seed_ids)}/{SEED_POST_COUNT}")
    return seed_ids


def run_load(rate, duration_sec, payloads_file, output_file):
    url = f"http://{PRIMARY_HOST}:{WP_PORT}/xmlrpc.php"
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
                   help="Comma-separated rates (default: 500,1000,1500,2000)")
    p.add_argument("--duration", type=int, default=LOAD_DURATION_SEC,
                   help=f"Measurement duration (default: {LOAD_DURATION_SEC})")
    p.add_argument("--warmup-duration", type=int, default=WARMUP_DURATION_SEC,
                   help=f"Warmup duration (default: {WARMUP_DURATION_SEC})")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    rates = [int(r.strip()) for r in args.rates.split(",")]
    load_duration = args.duration
    warmup_duration = args.warmup_duration

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_dir = RESULTS_BASE / f"validate_semisync_overhead_{timestamp}"
    results_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Semi-Sync Overhead Isolation")
    print("=" * 60)
    print(f"  Configs  : {', '.join(CONFIGS.keys())}")
    print(f"  Rates    : {rates}")
    print(f"  Duration : {load_duration}s (warmup: {warmup_duration}s)")
    print(f"  Results  : {results_dir}")
    print("=" * 60)

    all_results = {}

    for config_name in CONFIGS:
        print(f"\n{'='*60}")
        print(f"  [{config_name}] {CONFIGS[config_name]['description']}")
        print(f"{'='*60}")

        cleanup()

        print("\n[1] Setting up cluster ...")
        setup_cluster(config_name)

        print("\n[2] Seeding posts ...")
        seed_ids = seed_posts()
        if not seed_ids:
            print("ERROR: No posts seeded")
            cleanup()
            continue

        payloads_file = results_dir / f"payloads_{config_name}.txt"
        with open(payloads_file, "w") as fh:
            for pid in seed_ids:
                fh.write(_xmlrpc_editpost_payload(pid, ADMIN_PASSWORD) + "\n")

        # Verify semi-sync status before load
        if CONFIGS[config_name]["semisync_source"]:
            result = _ssh(PRIMARY_HOST,
                          f"docker exec mysql-val-primary mysql -p{MYSQL_ROOT_PASS} "
                          f"-e \"SHOW STATUS LIKE 'Rpl_semi_sync_source%';\"",
                          capture=True)
            print(f"\n   Semi-sync status:\n{result.stdout.strip()}")

        print(f"\n[3] Load sweep ...")
        config_results = []
        for rate in rates:
            # Warmup at fixed rate
            print(f"\n   -> warmup at {WARMUP_RATE} rps for {warmup_duration}s ...")
            warmup_out = results_dir / f"{config_name}_warmup_rate{rate}.txt"
            run_load(WARMUP_RATE, warmup_duration, payloads_file, warmup_out)
            time.sleep(5)

            # Measurement
            print(f"   -> rate={rate} (measuring {load_duration}s) ...")
            output_file = results_dir / f"{config_name}_rate{rate}.txt"
            m = run_load(rate, load_duration, payloads_file, output_file)
            config_results.append({"rate_rps": rate, **m})
            print(f"     tput={m['throughput_rps']:.1f} rps  "
                  f"avg={m['avg_ms']:.1f}ms  p50={m['p50_ms']:.1f}ms  "
                  f"p95={m['p95_ms']:.1f}ms  p99={m['p99_ms']:.1f}ms")

            # Verify semi-sync still active after load
            if CONFIGS[config_name]["semisync_source"]:
                result = _ssh(PRIMARY_HOST,
                              f"docker exec mysql-val-primary mysql -p{MYSQL_ROOT_PASS} "
                              f"-e \"SHOW STATUS LIKE 'Rpl_semi_sync_source_status';\"",
                              capture=True)
                status = [l for l in result.stdout.splitlines() if "Rpl_semi_sync" in l]
                if status:
                    print(f"     Semi-sync: {status[0].strip()}")

            if m["avg_ms"] > 2000:
                print("     Saturated, stopping")
                break
            time.sleep(3)

        all_results[config_name] = config_results
        cleanup()

    # ── Summary ───────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print("Summary")
    print(f"{'='*60}")

    for config_name, config_results in all_results.items():
        print(f"\n  {config_name} ({CONFIGS[config_name]['description']}):")
        print(f"   {'rate':>6}  {'tput':>8}  {'avg':>8}  {'p50':>8}  {'p95':>8}  {'p99':>8}")
        print(f"   {'-'*56}")
        for row in config_results:
            print(f"   {row['rate_rps']:>5}  "
                  f"{row['throughput_rps']:>7.1f}  "
                  f"{row['avg_ms']:>7.1f}ms  "
                  f"{row['p50_ms']:>7.1f}ms  "
                  f"{row['p95_ms']:>7.1f}ms  "
                  f"{row['p99_ms']:>7.1f}ms")

    # Comparison table
    configs_present = [c for c in ["no_repl", "async", "semisync"] if c in all_results]
    if len(configs_present) >= 2:
        print(f"\n  Comparison (avg latency):")
        header = f"   {'rate':>6}"
        for c in configs_present:
            header += f"  {c:>12}"
        print(header)
        print(f"   {'-'*(6 + 14 * len(configs_present))}")

        by_rate = {}
        for c in configs_present:
            by_rate[c] = {r["rate_rps"]: r for r in all_results[c]}

        for rate in rates:
            line = f"   {rate:>5}"
            for c in configs_present:
                if rate in by_rate[c]:
                    line += f"  {by_rate[c][rate]['avg_ms']:>10.1f}ms"
                else:
                    line += f"  {'N/A':>12}"
            print(line)

        print(f"\n  Comparison (throughput):")
        header = f"   {'rate':>6}"
        for c in configs_present:
            header += f"  {c:>12}"
        print(header)
        print(f"   {'-'*(6 + 14 * len(configs_present))}")
        for rate in rates:
            line = f"   {rate:>5}"
            for c in configs_present:
                if rate in by_rate[c]:
                    line += f"  {by_rate[c][rate]['throughput_rps']:>10.1f}"
                else:
                    line += f"  {'N/A':>12}"
            print(line)

    print(f"\n[Done] Results in {results_dir}/")
