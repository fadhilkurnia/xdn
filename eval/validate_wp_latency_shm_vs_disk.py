"""
validate_wp_latency_shm_vs_disk.py — Validate WordPress editPost latency
with MySQL state directory mounted on /dev/shm (tmpfs) vs /tmp (disk).

This isolates the effect of storage medium on WordPress write latency,
independent of XDN's replication pipeline. If /dev/shm latency matches
the ~25ms seen in XDN PB, it confirms the low latency is due to tmpfs-
backed state rather than missing replication overhead.

Run from the eval/ directory:
    python3 validate_wp_latency_shm_vs_disk.py
    python3 validate_wp_latency_shm_vs_disk.py --host 10.10.1.1 --rates 100,500,1000
    python3 validate_wp_latency_shm_vs_disk.py --duration 60 --warmup-duration 15

Output: eval/results/validate_wp_shm_vs_disk_<ts>/
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
    check_rest_api,
    create_post,
    install_wordpress,
)
from run_load_pb_wordpress_reflex import (
    _parse_go_output,
    _xmlrpc_editpost_payload,
)

# ── Config ────────────────────────────────────────────────────────────────────

TARGET_HOST = "10.10.1.1"
WP_PORT = 8090
MYSQL_PORT = 3307
MYSQL_ROOT_PASS = "supersecret"
MYSQL_DB = "wordpress"
SEED_POST_COUNT = 200

DEFAULT_RATES = [100, 500, 1000]
LOAD_DURATION_SEC = 10
WARMUP_DURATION_SEC = 5

# Configurations to compare:
#   shm:           MySQL data on tmpfs, Apache default (Connection: close)
#   disk:          MySQL data on disk,  Apache default (Connection: close)
#   shm_keepalive: MySQL data on tmpfs, Apache forced keep-alive (matches XDN behavior)
MOUNT_CONFIGS = {
    "shm":           {"data_dir": "/dev/shm/validate_wp_mysql",           "keepalive": False},
    "disk":          {"data_dir": "/tmp/validate_wp_mysql",               "keepalive": False},
    "shm_keepalive": {"data_dir": "/dev/shm/validate_wp_mysql_keepalive", "keepalive": True},
}

RESULTS_BASE = Path(__file__).resolve().parent / "results"
GO_LATENCY_CLIENT = Path(__file__).resolve().parent / "get_latency_at_rate.go"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _ssh(host, cmd, check=True, capture=False):
    return subprocess.run(
        ["ssh", "-o", "StrictHostKeyChecking=no", host, cmd],
        check=check, capture_output=capture, text=True,
    )


def enable_apache_keepalive(host):
    """Configure Apache in the WordPress container to use keep-alive connections.

    WordPress's Apache sends 'Connection: close' by default, forcing the Go
    client to create a new TCP connection per request. XDN's proxy overrides
    this header, so XDN clients get keep-alive for free. This function makes
    bare WordPress match XDN's behavior for a fair comparison.
    """
    print("   Enabling Apache keep-alive ...")
    cmd = (
        "docker exec wp-validate bash -c \""
        "echo 'KeepAlive On' >> /etc/apache2/apache2.conf && "
        "echo 'KeepAliveTimeout 60' >> /etc/apache2/apache2.conf && "
        "echo 'MaxKeepAliveRequests 10000' >> /etc/apache2/apache2.conf && "
        "apache2ctl graceful\""
    )
    result = _ssh(host, cmd, check=False, capture=True)
    if result.returncode == 0:
        print("   Apache keep-alive enabled")
    else:
        print(f"   WARNING: failed to enable keep-alive: {result.stderr.strip()}")
    time.sleep(2)


def cleanup_containers(host):
    """Stop and remove validation containers."""
    print(f"   Cleaning up containers on {host} ...")
    _ssh(host,
         "docker stop wp-validate mysql-validate 2>/dev/null || true; "
         "docker rm -f wp-validate mysql-validate 2>/dev/null || true; "
         "docker network rm wp-validate-net 2>/dev/null || true",
         check=False)


def setup_wordpress(host, mysql_data_dir):
    """Start MySQL + WordPress with MySQL data on the given directory."""
    print(f"   Creating data directory: {mysql_data_dir}")
    _ssh(host, f"mkdir -p {mysql_data_dir}")

    print(f"   Creating Docker network ...")
    _ssh(host, "docker network create wp-validate-net 2>/dev/null || true", check=False)

    print(f"   Starting MySQL (data on {mysql_data_dir}) ...")
    mysql_cmd = (
        f"docker run -d --name mysql-validate "
        f"--network wp-validate-net "
        f"-e MYSQL_ROOT_PASSWORD={MYSQL_ROOT_PASS} "
        f"-e MYSQL_DATABASE={MYSQL_DB} "
        f"-v {mysql_data_dir}:/var/lib/mysql "
        f"-p {MYSQL_PORT}:3306 "
        f"mysql:8.4.0"
    )
    _ssh(host, mysql_cmd)

    # Wait for MySQL to be ready
    print(f"   Waiting for MySQL readiness ...")
    deadline = time.time() + 240
    while time.time() < deadline:
        result = _ssh(host,
                      f"docker exec mysql-validate mysqladmin ping -p{MYSQL_ROOT_PASS} 2>/dev/null",
                      check=False, capture=True)
        if result.returncode == 0 and "alive" in result.stdout:
            break
        time.sleep(3)
    else:
        print("ERROR: MySQL did not become ready in 240s")
        sys.exit(1)
    print(f"   MySQL ready")

    print(f"   Starting WordPress ...")
    wp_config_extra = (
        "define('DISABLE_WP_CRON', true); "
        "define('AUTOMATIC_UPDATER_DISABLED', true); "
        "define('WP_AUTO_UPDATE_CORE', false);"
    )
    wp_cmd = (
        f"docker run -d --name wp-validate "
        f"--network wp-validate-net "
        f"-e WORDPRESS_DB_HOST=mysql-validate:3306 "
        f"-e WORDPRESS_DB_USER=root "
        f"-e WORDPRESS_DB_PASSWORD={MYSQL_ROOT_PASS} "
        f"-e WORDPRESS_DB_NAME={MYSQL_DB} "
        f'-e WORDPRESS_CONFIG_EXTRA="{wp_config_extra}" '
        f"-p {WP_PORT}:80 "
        f"wordpress:6.5.4-apache"
    )
    _ssh(host, wp_cmd)

    # Wait for WordPress HTTP
    print(f"   Waiting for WordPress HTTP readiness ...")
    from run_load_pb_wordpress_common import wait_for_port
    if not wait_for_port(host, WP_PORT, timeout_sec=120):
        print("ERROR: WordPress not ready")
        sys.exit(1)
    time.sleep(5)
    print(f"   WordPress ready on {host}:{WP_PORT}")


def enable_rest_auth_validate(host):
    """Enable WP Application Password for the validation containers."""
    import run_load_pb_wordpress_common as _wpmod
    _wpmod.ADMIN_APP_PASSWORD = None

    cid = "wp-validate"
    # mu-plugin
    mu_plugin = (
        "<?php\n"
        "add_filter('wp_is_application_passwords_available', '__return_true');\n"
    )
    subprocess.run(
        ["ssh", "-o", "StrictHostKeyChecking=no", host,
         f"docker exec -i {cid} bash -c "
         f"'mkdir -p /var/www/html/wp-content/mu-plugins && "
         f"cat > /var/www/html/wp-content/mu-plugins/xdn-app-passwords.php'"],
        input=mu_plugin, text=True,
    )

    # App password
    php_script = (
        "<?php "
        "require('/var/www/html/wp-load.php'); "
        "$user = get_user_by('login', 'admin'); "
        "if (!$user) { echo 'ERROR:no_admin'; exit(1); } "
        "$existing = WP_Application_Passwords::get_user_application_passwords($user->ID); "
        "foreach ($existing as $ap) { "
        "  if ($ap['name'] === 'xdn-bench') "
        "    WP_Application_Passwords::delete_application_password($user->ID, $ap['uuid']); "
        "} "
        "list($new_password) = WP_Application_Passwords::create_new_application_password("
        "  $user->ID, ['name' => 'xdn-bench']); "
        "echo $new_password;"
    )
    import re
    for attempt in range(1, 6):
        result = subprocess.run(
            ["ssh", "-o", "StrictHostKeyChecking=no", host,
             f"docker exec -i {cid} php"],
            input=php_script, text=True, capture_output=True,
        )
        pw = result.stdout.strip()
        if result.returncode == 0 and pw and len(pw) >= 10 and "<" not in pw and re.match(r'^[A-Za-z0-9 ]+$', pw):
            _wpmod.ADMIN_APP_PASSWORD = pw
            print(f"   App password created (first 5: {pw[:5]}...)")
            return True
        print(f"   Attempt {attempt}/5: invalid ({(pw or '(empty)')[:50]}), retrying ...")
        time.sleep(10)
    print("ERROR: Failed to create app password")
    return False


def disable_wp_cron_validate(host):
    """Disable WP-Cron in the validation WordPress container."""
    cmd = (
        "docker exec wp-validate bash -c "
        "\"grep -q DISABLE_WP_CRON /var/www/html/wp-config.php || "
        "sed -i '/That.*stop editing/i "
        "define('\\''DISABLE_WP_CRON'\\'', true);\\n"
        "define('\\''AUTOMATIC_UPDATER_DISABLED'\\'', true);\\n"
        "define('\\''WP_AUTO_UPDATE_CORE'\\'', false);' "
        "/var/www/html/wp-config.php\""
    )
    result = _ssh(host, cmd, check=False, capture=True)
    if result.returncode == 0:
        print("   WP-Cron disabled")
    else:
        print(f"   WARNING: failed to disable WP-Cron")


def tune_mysql_validate(host):
    """Apply same InnoDB tuning as the benchmark scripts."""
    print("   Tuning MySQL InnoDB settings ...")
    tune_sql = (
        "SET GLOBAL innodb_buffer_pool_size=1073741824; "
        "SET GLOBAL innodb_change_buffer_max_size=50; "
        "SET GLOBAL innodb_max_dirty_pages_pct=90; "
        "SET GLOBAL innodb_max_dirty_pages_pct_lwm=80; "
        "SET GLOBAL innodb_io_capacity=200; "
        "SET GLOBAL innodb_io_capacity_max=400; "
        "SET GLOBAL innodb_lru_scan_depth=256; "
    )
    result = _ssh(host,
                  f"docker exec mysql-validate mysql -p{MYSQL_ROOT_PASS} -e \"{tune_sql}\"",
                  check=False, capture=True)
    if result.returncode == 0:
        print("   MySQL tuned")
    else:
        print(f"   WARNING: MySQL tuning failed: {result.stderr.strip()}")
    time.sleep(2)


def run_load(host, rate, duration_sec, payloads_file, output_file):
    """Run the Go load generator."""
    url = f"http://{host}:{WP_PORT}/xmlrpc.php"
    cmd = [
        "go", "run", str(GO_LATENCY_CLIENT),
        "-H", "Content-Type: text/xml",
        "-X", "POST",
        "-payloads-file", str(payloads_file),
        url,
        "placeholder",
        str(duration_sec),
        str(rate),
    ]
    env = os.environ.copy()
    env["GO111MODULE"] = "off"
    env["GOWORK"] = "off"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w") as fh:
        result = subprocess.run(cmd, stdout=fh, stderr=subprocess.STDOUT,
                                env=env, text=True, cwd=str(GO_LATENCY_CLIENT.parent))
    if result.returncode != 0:
        print(f"   WARNING: go client exited {result.returncode}")
    return _parse_go_output(output_file)


# ── Main ──────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--host", type=str, default=TARGET_HOST,
                   help=f"Target host (default: {TARGET_HOST})")
    p.add_argument("--rates", type=str, default=",".join(str(r) for r in DEFAULT_RATES),
                   help="Comma-separated rates (default: 100,500,1000)")
    p.add_argument("--duration", type=int, default=LOAD_DURATION_SEC,
                   help=f"Measurement duration per rate in seconds (default: {LOAD_DURATION_SEC})")
    p.add_argument("--warmup-duration", type=int, default=WARMUP_DURATION_SEC,
                   help=f"Warmup duration per rate in seconds (default: {WARMUP_DURATION_SEC})")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    host = args.host
    rates = [int(r.strip()) for r in args.rates.split(",")]
    load_duration = args.duration
    warmup_duration = args.warmup_duration

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_dir = RESULTS_BASE / f"validate_wp_shm_vs_disk_{timestamp}"
    results_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("WordPress editPost Latency: /dev/shm vs /tmp vs keep-alive")
    print("=" * 60)
    print(f"  Host     : {host}")
    print(f"  Rates    : {rates}")
    print(f"  Duration : {load_duration}s (warmup: {warmup_duration}s)")
    print(f"  Results  : {results_dir}")
    print("=" * 60)

    all_results = {}

    for mount_name, cfg in MOUNT_CONFIGS.items():
        data_dir = cfg["data_dir"]
        use_keepalive = cfg["keepalive"]
        print(f"\n{'='*60}")
        print(f"  Testing: {mount_name} (dir={data_dir}, keepalive={use_keepalive})")
        print(f"{'='*60}")

        # Cleanup
        cleanup_containers(host)
        _ssh(host, f"sudo rm -rf {data_dir}", check=False)

        # Setup
        print("\n[1] Setting up WordPress + MySQL ...")
        setup_wordpress(host, data_dir)

        print("\n[2] Installing WordPress ...")
        ok = install_wordpress(host, WP_PORT, "validate")
        if not ok:
            print("ERROR: WordPress install failed")
            continue

        print("\n[3] Enabling REST API auth ...")
        if not enable_rest_auth_validate(host):
            continue

        print("\n[4] Disabling WP-Cron ...")
        disable_wp_cron_validate(host)

        if use_keepalive:
            print("\n[4b] Enabling Apache keep-alive ...")
            enable_apache_keepalive(host)

        print("\n[5] Checking REST API ...")
        if not check_rest_api(host, WP_PORT, "validate"):
            print("ERROR: REST API not available")
            continue

        print("\n[6] Tuning MySQL ...")
        tune_mysql_validate(host)

        print(f"\n[7] Seeding {SEED_POST_COUNT} posts ...")
        seed_ids = []
        for i in range(SEED_POST_COUNT):
            pid = None
            for attempt in range(1, 4):
                try:
                    pid = create_post(host, WP_PORT, "validate",
                                      f"Seed Post {i+1}", f"Content {i+1}.")
                    if pid:
                        break
                except Exception:
                    time.sleep(5)
            if pid:
                seed_ids.append(pid)
        print(f"   Seeded {len(seed_ids)}/{SEED_POST_COUNT} posts")

        # Write payloads file
        payloads_file = results_dir / f"payloads_{mount_name}.txt"
        with open(payloads_file, "w") as fh:
            for pid in seed_ids:
                fh.write(_xmlrpc_editpost_payload(pid, ADMIN_PASSWORD) + "\n")

        # Run load sweep
        print(f"\n[8] Load sweep ...")
        mount_results = []
        for rate in rates:
            # Warmup
            print(f"\n   -> rate={rate} (warmup {warmup_duration}s) ...")
            warmup_out = results_dir / f"{mount_name}_warmup_rate{rate}.txt"
            run_load(host, rate, warmup_duration, payloads_file, warmup_out)
            time.sleep(3)

            # Measurement
            print(f"   -> rate={rate} (measuring {load_duration}s) ...")
            output_file = results_dir / f"{mount_name}_rate{rate}.txt"
            m = run_load(host, rate, load_duration, payloads_file, output_file)
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
        cleanup_containers(host)
        _ssh(host, f"sudo rm -rf {data_dir}", check=False)

    # ── Summary ───────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print("Summary")
    print(f"{'='*60}")

    for mount_name, mount_results in all_results.items():
        data_dir = MOUNT_CONFIGS[mount_name]["data_dir"]
        print(f"\n  {mount_name} ({data_dir}):")
        print(f"   {'rate':>6}  {'tput':>8}  {'avg':>8}  {'p50':>8}  {'p95':>8}  {'p99':>8}")
        print(f"   {'-'*56}")
        for row in mount_results:
            print(f"   {row['rate_rps']:>5}  "
                  f"{row['throughput_rps']:>7.1f}  "
                  f"{row['avg_ms']:>7.1f}ms  "
                  f"{row['p50_ms']:>7.1f}ms  "
                  f"{row['p95_ms']:>7.1f}ms  "
                  f"{row['p99_ms']:>7.1f}ms")

    # ── Comparison ────────────────────────────────────────────────────
    by_rate = {}
    for name, rows in all_results.items():
        by_rate[name] = {r["rate_rps"]: r for r in rows}

    configs_present = [c for c in ["shm", "disk", "shm_keepalive"] if c in by_rate]
    if len(configs_present) >= 2:
        print(f"\n  Comparison:")
        header = f"   {'rate':>6}"
        for c in configs_present:
            header += f"  {c:>14}"
        print(header)
        print(f"   {'-'*(6 + 16 * len(configs_present))}")
        for rate in rates:
            line = f"   {rate:>5}"
            for c in configs_present:
                if rate in by_rate[c]:
                    line += f"  {by_rate[c][rate]['avg_ms']:>13.1f}ms"
                else:
                    line += f"  {'N/A':>14}"
            print(line)

    # Highlight the key finding
    if "shm" in by_rate and "shm_keepalive" in by_rate:
        print(f"\n  Keep-alive effect (shm vs shm_keepalive):")
        print(f"   {'rate':>6}  {'close':>10}  {'keepalive':>10}  {'speedup':>8}")
        print(f"   {'-'*42}")
        for rate in rates:
            if rate in by_rate["shm"] and rate in by_rate["shm_keepalive"]:
                close = by_rate["shm"][rate]["avg_ms"]
                ka = by_rate["shm_keepalive"][rate]["avg_ms"]
                speedup = close / ka if ka > 0 else float('inf')
                print(f"   {rate:>5}  {close:>9.1f}ms  {ka:>9.1f}ms  {speedup:>7.2f}x")

    print(f"\n[Done] Results in {results_dir}/")
