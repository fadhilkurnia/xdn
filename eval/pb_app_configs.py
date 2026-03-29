"""
pb_app_configs.py — Consolidated app configuration registry for PB benchmark scripts.

Maps short app names to their configuration (images, YAML, JVM args, workload
parameters) and provides app-specific setup/seed/readiness functions behind
generic dispatcher interfaces.

Replaces the per-app _common.py modules:
  - run_load_pb_wordpress_common.py
  - run_load_pb_bookcatalog_common.py
  - run_load_pb_hotelres_common.py
  - run_load_pb_synth_common.py
  - run_load_pb_tpcc_reflex.py (constants + helpers)
"""

import base64
import json
import os
import random
import re
import subprocess
import time

import requests

# ── Shared constants ─────────────────────────────────────────────────────────

ADMIN_USER = "admin"
ADMIN_PASSWORD = "AdminPass123!"
ADMIN_EMAIL = "admin@example.com"
SITE_TITLE = "WordPress XDN Test"

# Module-level state set by enable_rest_auth()
_wp_app_password = None


# ═══════════════════════════════════════════════════════════════════════════════
#  APP_CONFIGS — per-app configuration registry
# ═══════════════════════════════════════════════════════════════════════════════

APP_CONFIGS = {
    "wordpress": {
        "service_name": "wordpress",
        "yaml": "../xdn-cli/examples/wordpress.yaml",
        "images": ["mysql:8.4.0", "wordpress:6.5.4-apache"],
        "gp_config": "../conf/gigapaxos.xdn.wordpress-pb.cloudlab.properties",
        "gp_jvm_args": (
            "-DSYNC=true "
            "-DPB_N_PARALLEL_WORKERS=256 "
            "-DPB_CAPTURE_ACCUMULATION_US=100 "
            "-DHTTP_FORCE_KEEPALIVE=true "
            "-DFUSELOG_DISABLE_COALESCING=true "
            "-DXDN_MIGRATE_WAIT_SEC=5 "
            "-DXDN_SKIP_INIT_SYNC=true "
            "-DPB_INLINE_EXECUTE=true"
        ),
        "timeout_app_sec": 600,
        "warmup_rate": 1500,
        "avg_latency_threshold_ms": 1000,
        "seed_count": 200,
        "workload": {
            "url_path": "/xmlrpc.php",
            "method": "POST",
            "content_type": "text/xml",
            "uses_payloads_file": True,
        },
        # distdb
        "distdb_backend": "semisync",
        # criu
        "criu_checkpoint_container": "wordpress-criu-mysql",
        "criu_app_port": 8080,
    },
    "bookcatalog": {
        "service_name": "svc",
        "yaml": "../xdn-cli/examples/bob-book-catalog-nondeterministic-service.yaml",
        "images": ["fadhilkurnia/xdn-bookcatalog-nd"],
        "gp_config": "../conf/gigapaxos.xdn.3way.cloudlab.properties",
        "gp_jvm_args": "-DPB_N_PARALLEL_WORKERS=32 -DPB_CAPTURE_ACCUMULATION_MS=1",
        "timeout_app_sec": 300,
        "warmup_rate": 1500,
        "avg_latency_threshold_ms": 1000,
        "seed_count": 200,
        "workload": {
            "url_path": "/api/books",
            "method": "POST",
            "content_type": "application/json",
            "payload": '{"title":"bench book","author":"bench author"}',
        },
        "distdb_backend": "rqlite",
        "criu_checkpoint_container": None,  # checkpoint entire app container
        "criu_app_port": 8080,
    },
    "tpcc": {
        "service_name": "tpcc",
        "yaml": "../xdn-cli/examples/tpcc.yaml",
        "images": ["postgres:17.4-bookworm", "fadhilkurnia/xdn-tpcc"],
        "gp_config": "../conf/gigapaxos.xdn.3way.cloudlab.properties",
        "gp_jvm_args": (
            "-DSYNC=true "
            "-DPB_N_PARALLEL_WORKERS=256 "
            "-DPB_CAPTURE_ACCUMULATION_US=100 "
            "-DHTTP_FORCE_KEEPALIVE=true "
            "-DFUSELOG_DISABLE_COALESCING=true "
            "-DXDN_MIGRATE_WAIT_SEC=5 "
            "-DXDN_SKIP_INIT_SYNC=true "
            "-DPB_INLINE_EXECUTE=true "
            "-DBATCHING_ENABLED=false"
        ),
        "timeout_app_sec": 600,
        "warmup_rate": 100,
        "avg_latency_threshold_ms": 3000,
        "seed_count": 10,  # NUM_WAREHOUSES
        "workload": {
            "url_path": "/orders",
            "method": "POST",
            "content_type": "application/json",
            "uses_payloads_file": True,
        },
        "distdb_backend": "pgsync",
    },
    "tpcc-java": {
        "service_name": "tpcc-java",
        "yaml": "../xdn-cli/examples/tpcc-java.yaml",
        "images": ["postgres:17.4-bookworm", "fadhilkurnia/xdn-tpcc-java"],
        "gp_config": "../conf/gigapaxos.xdn.tpcc-java-pb.cloudlab.properties",
        "gp_jvm_args": (
            "-DSYNC=true "
            "-DPB_N_PARALLEL_WORKERS=256 "
            "-DPB_CAPTURE_ACCUMULATION_US=100 "
            "-DHTTP_FORCE_KEEPALIVE=true "
            "-DFUSELOG_DISABLE_COALESCING=true "
            "-DXDN_MIGRATE_WAIT_SEC=5 "
            "-DXDN_SKIP_INIT_SYNC=true "
            "-DPB_INLINE_EXECUTE=true "
            "-DBATCHING_ENABLED=false"
        ),
        "timeout_app_sec": 600,
        "warmup_rate": 100,
        "avg_latency_threshold_ms": 3000,
        "seed_count": 10,  # NUM_WAREHOUSES
        "workload": {
            "url_path": "/orders",
            "method": "POST",
            "content_type": "application/json",
            "uses_payloads_file": True,
        },
        "distdb_backend": "pgsync",
    },
    "hotelres": {
        "service_name": "hotel-reservation",
        "yaml": "../xdn-cli/examples/hotel-reservation.yaml",
        "images": ["mongo:8.0.5-rc2-noble", "fadhilkurnia/xdn-hotel-reservation:latest"],
        "gp_config": "../conf/gigapaxos.xdn.3way.cloudlab.properties",
        "gp_jvm_args": "",  # no special JVM args
        "timeout_app_sec": 300,
        "warmup_rate": 1500,
        "avg_latency_threshold_ms": 1000,
        "seed_count": 500,  # reservation URLs
        "workload": {
            "url_path": "/reservation",
            "method": "POST",
            "content_type": None,
            "uses_urls_file": True,
        },
        "distdb_backend": "replmongo",
    },
    "synth": {
        "service_name": "synth",
        "yaml": "../xdn-cli/examples/synth-workload.yaml",
        "images": ["fadhilkurnia/xdn-synth-workload"],
        "gp_config": "../conf/gigapaxos.xdn.3way.cloudlab.properties",
        "gp_jvm_args": (
            "-DSYNC=true "
            "-DPB_N_PARALLEL_WORKERS=256 "
            "-DPB_CAPTURE_ACCUMULATION_US=100 "
            "-DHTTP_FORCE_KEEPALIVE=true "
            "-DFUSELOG_DISABLE_COALESCING=true "
            "-DXDN_MIGRATE_WAIT_SEC=5 "
            "-DXDN_SKIP_INIT_SYNC=true "
            "-DPB_INLINE_EXECUTE=true "
            "-DBATCHING_ENABLED=false"
        ),
        "timeout_app_sec": 300,
        "warmup_rate": 1500,
        "avg_latency_threshold_ms": 1000,
        "seed_count": 0,
        "workload": {
            "url_path": "/workload",
            "method": "POST",
            "content_type": "application/json",
        },
        "distdb_backend": "rqlite",
    },
}


# ═══════════════════════════════════════════════════════════════════════════════
#  WordPress helpers
# ═══════════════════════════════════════════════════════════════════════════════


def _wp_rest_auth_header():
    """Return the Authorization header value for WordPress REST API calls."""
    password = _wp_app_password or ADMIN_PASSWORD
    return "Basic " + base64.b64encode(f"{ADMIN_USER}:{password}".encode()).decode()


def _wp_create_post(host, port, service, title, content):
    """Create a single WordPress post via REST API. Returns post id or None."""
    r = requests.post(
        f"http://{host}:{port}/?rest_route=/wp/v2/posts",
        headers={
            "XDN": service,
            "Content-Type": "application/json",
            "Authorization": _wp_rest_auth_header(),
        },
        data=json.dumps({"title": title, "content": content, "status": "publish"}),
        timeout=15,
    )
    if r.status_code not in (200, 201):
        print(f"   ERROR: HTTP {r.status_code} — {r.text[:200]}")
        return None
    post_id = r.json().get("id")
    if post_id:
        return post_id
    print(f"   ERROR: no 'id' in response: {r.text[:200]}")
    return None


def _xmlrpc_editpost_payload(post_id, password=ADMIN_PASSWORD):
    """Generate a single-line XML-RPC wp.editPost payload."""
    return (
        '<?xml version="1.0"?>'
        "<methodCall>"
        "<methodName>wp.editPost</methodName>"
        "<params>"
        "<param><value><int>1</int></value></param>"
        f"<param><value><string>{ADMIN_USER}</string></value></param>"
        f"<param><value><string>{password}</string></value></param>"
        f"<param><value><int>{post_id}</int></value></param>"
        "<param><value><struct>"
        "<member><name>post_content</name>"
        "<value><string>Edit workload test content.</string></value></member>"
        "</struct></value></param>"
        "</params>"
        "</methodCall>"
    )


def install_wordpress(host, port, service):
    """Submit the WordPress installation form."""
    print(f"   Submitting WordPress installation form to {host} ...")
    r = requests.post(
        f"http://{host}:{port}/wp-admin/install.php?step=2",
        data={
            "weblog_title":    SITE_TITLE,
            "user_name":       ADMIN_USER,
            "admin_password":  ADMIN_PASSWORD,
            "admin_password2": ADMIN_PASSWORD,
            "pw_weak":         "1",
            "admin_email":     ADMIN_EMAIL,
            "blog_public":     "1",
            "Submit":          "Install WordPress",
        },
        headers={"XDN": service},
        timeout=30,
        allow_redirects=True,
    )
    success = r.status_code == 200 and (
        "success" in r.text.lower() or "wp-login" in r.text.lower()
    )
    if success:
        print(f"   Installation succeeded (HTTP {r.status_code})")
    else:
        print(f"   Installation returned HTTP {r.status_code}")
        print(f"   Response snippet: {r.text[:500]}")
    return success


def enable_rest_auth(primary):
    """Enable WP REST API auth over HTTP using Application Passwords.

    Steps:
    1. Install a mu-plugin that sets wp_is_application_passwords_available=true
       (WP disables App Passwords on non-HTTPS by default).
    2. Create a WP Application Password for the admin user via PHP CLI.
    3. Store the generated password in module-level _wp_app_password.
    """
    global _wp_app_password
    # Get the WordPress web container ID
    r = subprocess.run(
        ["ssh", primary, "docker ps -q --filter ancestor=wordpress:6.5.4-apache"],
        capture_output=True, text=True,
    )
    cid = r.stdout.strip().split("\n")[0]
    if not cid:
        print(f"   ERROR: no running wordpress container on {primary}")
        return False

    # Step 1: install mu-plugin to allow Application Passwords on plain HTTP
    mu_plugin = (
        "<?php\n"
        "// Allow WP Application Passwords over plain HTTP (XDN benchmark).\n"
        "add_filter('wp_is_application_passwords_available', '__return_true');\n"
    )
    subprocess.run(
        ["ssh", primary,
         f"docker exec -i {cid} bash -c "
         f"'mkdir -p /var/www/html/wp-content/mu-plugins && "
         f"cat > /var/www/html/wp-content/mu-plugins/xdn-app-passwords.php'"],
        input=mu_plugin, text=True,
    )
    print(f"   mu-plugin installed: Application Passwords enabled on HTTP")

    # Step 2: create (or recreate) a WP Application Password named 'xdn-bench'
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

    def _is_valid_app_password(pw):
        """WP Application Passwords are 24 hex-like chars with spaces (e.g. 'abcd 1234 ...')."""
        if not pw or len(pw) < 10:
            return False
        # Must not contain HTML tags or common error markers
        if "<" in pw or "ERROR" in pw or "Fatal" in pw:
            return False
        # Should be alphanumeric + spaces only
        return bool(re.match(r'^[A-Za-z0-9 ]+$', pw))

    # Retry up to 5 times — WordPress may still be finishing installation
    for attempt in range(1, 6):
        result = subprocess.run(
            ["ssh", primary, f"docker exec -i {cid} php"],
            input=php_script, text=True, capture_output=True,
        )
        app_password = result.stdout.strip()
        if result.returncode == 0 and _is_valid_app_password(app_password):
            _wp_app_password = app_password
            print(f"   WP Application Password created (first 5 chars: {app_password[:5]}...)")
            return True
        snippet = (app_password or result.stderr or "(empty)")[:150]
        print(f"   Attempt {attempt}/5: invalid response: {snippet}")
        if attempt < 5:
            print(f"   Retrying in 10s ...")
            time.sleep(10)
    print(f"   ERROR creating app password after 5 attempts")
    return False


def disable_wp_cron(primary):
    """Disable WP-Cron and auto-updates to prevent 503 maintenance mode during benchmarks."""
    r = subprocess.run(
        ["ssh", primary, "docker ps -q --filter ancestor=wordpress:6.5.4-apache"],
        capture_output=True, text=True,
    )
    cid = r.stdout.strip().split("\n")[0]
    if not cid:
        print(f"   WARNING: no running wordpress container on {primary}, skipping WP cron disable")
        return

    # Insert before the "That's all, stop editing!" marker in wp-config.php
    cmd = (
        f"docker exec {cid} bash -c "
        "\"grep -q DISABLE_WP_CRON /var/www/html/wp-config.php || "
        "sed -i '/That.*stop editing/i "
        "define('\\''DISABLE_WP_CRON'\\'', true);\\n"
        "define('\\''AUTOMATIC_UPDATER_DISABLED'\\'', true);\\n"
        "define('\\''WP_AUTO_UPDATE_CORE'\\'', false);' "
        "/var/www/html/wp-config.php\""
    )
    result = subprocess.run(
        ["ssh", primary, cmd],
        capture_output=True, text=True,
    )
    if result.returncode == 0:
        print("   WP-Cron and auto-updates disabled in wp-config.php")
    else:
        print(f"   WARNING: failed to disable WP-Cron: {result.stderr.strip()}")


def check_rest_api(host, port, service, timeout_sec=120):
    """Verify REST API write access with Application Password (POST a draft post).

    Retries for up to timeout_sec seconds to handle WordPress maintenance mode
    (HTTP 503) that occurs briefly after initial installation while WP runs
    auto-update checks.
    """
    deadline = time.time() + timeout_sec
    last_status = None
    while time.time() < deadline:
        try:
            r = requests.post(
                f"http://{host}:{port}/?rest_route=/wp/v2/posts",
                headers={
                    "XDN": service,
                    "Content-Type": "application/json",
                    "Authorization": _wp_rest_auth_header(),
                },
                data=json.dumps({"title": "REST API check", "status": "draft"}),
                timeout=10,
            )
            last_status = r.status_code
            if r.status_code in (200, 201):
                print(f"   REST API available (HTTP {r.status_code})")
                return True
            if r.status_code in (500, 503):
                print(f"   REST API {r.status_code} (transient), retrying ...")
                time.sleep(5)
                continue
            # Any other non-success is a real failure
            print(f"   REST API NOT available (HTTP {r.status_code}): {r.text[:100]}")
            return False
        except Exception as e:
            print(f"   REST API check error: {e}, retrying ...")
            time.sleep(5)
    print(f"   REST API still unavailable after {timeout_sec}s (last HTTP {last_status})")
    return False


def tune_mysql(primary):
    """Tune MySQL InnoDB settings on the primary for write-heavy workloads.

    Increases buffer pool to 1GB (from default 128MB) to avoid page eviction
    during sustained high write rates, and reduces background I/O to minimize
    interference with foreground queries.
    """
    print("   [setup] Tuning MySQL InnoDB settings ...")
    # Find the MySQL container (the one running the mysql image)
    find_cmd = "docker ps --format '{{.Names}} {{.Image}}' | grep mysql | awk '{print $1}' | head -1"
    result = subprocess.run(
        ["ssh", primary, find_cmd],
        capture_output=True, text=True, timeout=10,
    )
    mysql_container = result.stdout.strip()
    if not mysql_container:
        print("   WARNING: Could not find MySQL container, skipping tuning")
        return

    tune_sql = (
        "SET GLOBAL innodb_buffer_pool_size=1073741824; "       # 1 GB
        "SET GLOBAL innodb_change_buffer_max_size=50; "          # allow more change buffering
        "SET GLOBAL innodb_max_dirty_pages_pct=90; "             # delay page flush (reduce background statediff)
        "SET GLOBAL innodb_max_dirty_pages_pct_lwm=80; "         # high-water mark before eager flush
        "SET GLOBAL innodb_io_capacity=200; "                    # limit background I/O (fewer flushes)
        "SET GLOBAL innodb_io_capacity_max=400; "                # cap burst I/O
        "SET GLOBAL innodb_lru_scan_depth=256; "                 # reduce per-second page cleaner work
    )
    tune_cmd = (
        f"docker exec {mysql_container} "
        f"mysql -psupersecret -e \"{tune_sql}\""
    )
    result = subprocess.run(
        ["ssh", primary, tune_cmd],
        capture_output=True, text=True, timeout=30,
    )
    if result.returncode == 0:
        print(f"   [setup] MySQL tuned on container {mysql_container}")
    else:
        print(f"   WARNING: MySQL tuning failed: {result.stderr.strip()}")
    # Give InnoDB a moment to resize the buffer pool
    time.sleep(2)


def setup_wordpress(primary, port, service):
    """Full WordPress setup: install, enable REST auth, disable cron, tune MySQL."""
    print("   [setup] Installing WordPress ...")
    if not install_wordpress(primary, port, service):
        print("ERROR: WordPress installation failed.")
        return False

    print("   [setup] Enabling REST API auth ...")
    if not enable_rest_auth(primary):
        print("ERROR: REST API auth setup failed.")
        return False

    print("   [setup] Disabling WP-Cron and auto-updates ...")
    disable_wp_cron(primary)

    print("   [setup] Checking REST API availability ...")
    if not check_rest_api(primary, port, service):
        print("ERROR: REST API not available.")
        return False

    tune_mysql(primary)
    return True


def seed_wordpress(primary, port, service, count, results_dir):
    """Create seed posts and write XML-RPC editPost payloads file.

    Returns the list of created post IDs.
    """
    print(f"   [setup] Creating {count} seed posts ...")
    seed_ids = []
    for i in range(count):
        title = f"Seed Post {i + 1}"
        content = f"Seed content {i + 1}."
        pid = None
        for attempt in range(1, 4):
            try:
                pid = _wp_create_post(primary, port, service, title, content)
                if pid:
                    break
            except Exception as e:
                print(f"   WARNING: '{title}' attempt {attempt} failed ({e}), retrying ...")
                time.sleep(5)
        if pid:
            seed_ids.append(pid)
        else:
            print(f"   WARNING: seed post '{title}' failed, continuing")
    print(f"   [setup] Seed complete: {len(seed_ids)}/{count} posts created")

    # Write XML-RPC editPost payloads file
    payloads_file = os.path.join(results_dir, "xmlrpc_payloads.txt")
    os.makedirs(results_dir, exist_ok=True)
    print(f"   [setup] Writing {len(seed_ids)} XML-RPC editPost payloads -> {payloads_file} ...")
    with open(payloads_file, "w") as fh:
        for pid in seed_ids:
            fh.write(_xmlrpc_editpost_payload(pid, ADMIN_PASSWORD) + "\n")
    print(f"   [setup] Done: {len(seed_ids)} payloads written.")
    return seed_ids


def check_wordpress_ready(host, port, service, timeout_sec=120):
    """Poll until WordPress REST API responds."""
    print(f"   Checking WordPress readiness on {host}:{port} ...")
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            r = requests.post(
                f"http://{host}:{port}/?rest_route=/wp/v2/posts",
                headers={
                    "XDN": service,
                    "Content-Type": "application/json",
                    "Authorization": _wp_rest_auth_header(),
                },
                data=json.dumps({"title": "health check", "status": "draft"}),
                timeout=10,
            )
            if r.status_code in (200, 201):
                print(f"   WordPress ready (HTTP {r.status_code})")
                return True
        except Exception:
            pass
        time.sleep(5)
    print(f"   TIMEOUT: WordPress not ready after {timeout_sec}s")
    return False


# ═══════════════════════════════════════════════════════════════════════════════
#  BookCatalog helpers
# ═══════════════════════════════════════════════════════════════════════════════


def setup_bookcatalog(primary, port, service):
    """No-op — bookcatalog is ready after deploy."""
    print("   [setup] BookCatalog: no setup needed (app ready after deploy)")
    return True


def seed_bookcatalog(primary, port, service, count, results_dir):
    """Seed the bookcatalog app with `count` books via POST /api/books."""
    print(f"   Seeding {count} books on {primary}:{port} ...")
    headers = {"Content-Type": "application/json"}
    if service:
        headers["XDN"] = service
    created = 0
    for i in range(count):
        try:
            r = requests.post(
                f"http://{primary}:{port}/api/books",
                json={"title": f"Book {i+1}", "author": f"Author {i+1}"},
                headers=headers,
                timeout=10,
            )
            if 200 <= r.status_code < 300:
                created += 1
            else:
                print(f"   WARNING: book {i+1} returned HTTP {r.status_code}")
        except Exception as e:
            print(f"   ERROR seeding book {i+1}: {e}")
    print(f"   Seeded {created}/{count} books.")
    return created


def check_bookcatalog_ready(host, port, service, timeout_sec=120):
    """Poll until bookcatalog responds on GET /api/books."""
    print(f"   Checking bookcatalog readiness on {host}:{port} ...")
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        headers = {}
        if service:
            headers["XDN"] = service
        try:
            r = requests.get(
                f"http://{host}:{port}/api/books",
                headers=headers,
                timeout=15,
            )
            if r.status_code == 200:
                print(f"   BookCatalog ready (HTTP {r.status_code})")
                return True
        except Exception:
            pass
        time.sleep(5)
    print(f"   TIMEOUT: bookcatalog not ready after {timeout_sec}s")
    return False


# ═══════════════════════════════════════════════════════════════════════════════
#  TPC-C helpers
# ═══════════════════════════════════════════════════════════════════════════════

# TPC-C constants
TPCC_NUM_WAREHOUSES = 10


def init_tpcc_db(host, port, service, warehouses=TPCC_NUM_WAREHOUSES, timeout=300):
    """Initialize TPC-C database by POSTing to /init_db endpoint.

    Seeding warehouses can be slow, so we use a generous timeout with retries.
    """
    print(f"   Initializing TPC-C DB ({warehouses} warehouses) via {host}:{port} ...")
    deadline = time.time() + timeout
    last_err = None
    while time.time() < deadline:
        try:
            r = requests.post(
                f"http://{host}:{port}/init_db",
                headers={"XDN": service, "Content-Type": "application/json"},
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


def generate_tpcc_payloads_file(path, num_warehouses=TPCC_NUM_WAREHOUSES, count=1000,
                                ol_cnt=5):
    """Generate a file of random New Order payloads for the Go load client.

    Each line is a JSON object: {"w_id": N, "c_id": M, "ol_cnt": K}
    - w_id: 1..num_warehouses
    - c_id: 1..num_warehouses*10
    - ol_cnt: fixed order line count (default 5, giving 4 + 4*5 = 24 SQL per request)
    """
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(path, "w") as fh:
        for _ in range(count):
            w_id = random.randint(1, num_warehouses)
            c_id = random.randint(1, num_warehouses * 10)
            fh.write(json.dumps({"w_id": w_id, "c_id": c_id, "ol_cnt": ol_cnt}) + "\n")
    print(f"   Generated {count} payloads (ol_cnt={ol_cnt}) -> {path}")


def check_tpcc_health(host, port, service, timeout_sec=120):
    """Poll until TPC-C app responds on GET /health."""
    print(f"   Checking TPC-C health via {host}:{port}/health ...")
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            r = requests.get(
                f"http://{host}:{port}/health",
                headers={"XDN": service},
                timeout=5,
            )
            if r.status_code == 200:
                print(f"   Health check OK (HTTP 200)")
                return True
        except Exception:
            pass
        time.sleep(5)
    print(f"   TIMEOUT: TPC-C health check failed after {timeout_sec}s")
    return False


def setup_tpcc(primary, port, service):
    """Initialize TPC-C database."""
    warehouses = APP_CONFIGS["tpcc"]["seed_count"]
    return init_tpcc_db(primary, port, service, warehouses=warehouses)


def seed_tpcc(primary, port, service, count, results_dir):
    """Generate New Order payloads file for load testing."""
    payloads_file = os.path.join(results_dir, "neworder_payloads.txt")
    os.makedirs(results_dir, exist_ok=True)
    generate_tpcc_payloads_file(payloads_file, num_warehouses=count, count=1000)
    return payloads_file


def check_tpcc_ready(host, port, service, timeout_sec=120):
    """Poll until TPC-C app responds on GET /health."""
    return check_tpcc_health(host, port, service, timeout_sec=timeout_sec)


# ═══════════════════════════════════════════════════════════════════════════════
#  HotelRes helpers
# ═══════════════════════════════════════════════════════════════════════════════


def generate_dummy_data(host, port, service=None):
    """Generate dummy hotel data by POSTing to /secret/generate."""
    print(f"   Generating dummy data on {host}:{port} ...")
    headers = {}
    if service:
        headers["XDN"] = service
    try:
        r = requests.post(
            f"http://{host}:{port}/secret/generate",
            headers=headers,
            timeout=60,
        )
        if r.status_code == 200:
            print(f"   Dummy data generated (HTTP {r.status_code})")
            return True
        else:
            print(f"   WARNING: /secret/generate returned HTTP {r.status_code}: {r.text[:200]}")
            return r.status_code < 500
    except Exception as e:
        print(f"   ERROR generating dummy data: {e}")
        return False


def generate_reservation_urls(host, port, output_file, count=500, service=None):
    """Generate a file with `count` randomized reservation POST URLs.

    Each URL has varied parameters to avoid row-lock contention:
    - hotelId: 1..80
    - inDate/outDate: random dates in 2015
    - customerName/username: Cornell_0..Cornell_500
    """
    print(f"   Generating {count} reservation URLs -> {output_file} ...")
    urls = []
    for i in range(count):
        hotel_id = random.randint(1, 80)
        # Random check-in date in 2015
        month = random.randint(4, 12)
        day_in = random.randint(1, 25)
        day_out = day_in + random.randint(1, 3)
        in_date = f"2015-{month:02d}-{day_in:02d}"
        out_date = f"2015-{month:02d}-{day_out:02d}"
        user_idx = random.randint(0, 500)
        username = f"Cornell_{user_idx}"

        url = (
            f"http://{host}:{port}/reservation"
            f"?inDate={in_date}&outDate={out_date}"
            f"&hotelId={hotel_id}&customerName={username}"
            f"&username={username}&password=1111111111&number=1"
        )
        urls.append(url)

    parent = os.path.dirname(output_file)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(output_file, "w") as fh:
        for url in urls:
            fh.write(url + "\n")
    print(f"   Done: {len(urls)} URLs written to {output_file}")
    return output_file


def setup_hotelres(primary, port, service):
    """Generate dummy hotel data."""
    return generate_dummy_data(primary, port, service)


def seed_hotelres(primary, port, service, count, results_dir):
    """Generate reservation URLs file for load testing."""
    urls_file = os.path.join(results_dir, "reservation_urls.txt")
    os.makedirs(results_dir, exist_ok=True)
    generate_reservation_urls(primary, port, urls_file, count=count, service=service)
    return urls_file


def check_hotelres_ready(host, port, service, timeout_sec=120):
    """Poll until hotel-reservation app responds on /hotels."""
    print(f"   Checking hotel-reservation readiness on {host}:{port} ...")
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        headers = {}
        if service:
            headers["XDN"] = service
        try:
            r = requests.get(
                f"http://{host}:{port}/hotels"
                f"?inDate=2015-04-09&outDate=2015-04-10&lat=38.0235&lon=-122.095",
                headers=headers,
                timeout=15,
            )
            if r.status_code == 200:
                print(f"   HotelRes ready (HTTP {r.status_code})")
                return True
        except Exception:
            pass
        time.sleep(5)
    print(f"   TIMEOUT: hotel-reservation not ready after {timeout_sec}s")
    return False


# ═══════════════════════════════════════════════════════════════════════════════
#  Synth helpers
# ═══════════════════════════════════════════════════════════════════════════════


def make_synth_workload_payload(txns=1, ops=1, write_size=100, autocommit=False):
    """Return a JSON payload string for POST /workload."""
    return json.dumps({
        "txns": txns, "ops": ops, "write_size": write_size,
        "autocommit": autocommit,
    })


def ensure_synth_images(hosts, ar_hosts=None):
    """Ensure the synth-workload Docker image is present on all given hosts."""
    synth_image = APP_CONFIGS["synth"]["images"][0]
    src = (ar_hosts or ["10.10.1.1"])[0]
    for host in hosts:
        check = subprocess.run(
            ["ssh", "-o", "StrictHostKeyChecking=no", host,
             f"docker image inspect {synth_image}"],
            capture_output=True,
        )
        if check.returncode == 0:
            print(f"   {synth_image}: already on {host}")
            continue

        # Try to transfer from first AR host
        print(f"   {synth_image}: transferring {src} -> {host} ...")
        ret = os.system(
            f"ssh {src} 'docker save {synth_image}'"
            f" | ssh {host} 'docker load'"
        )
        if ret != 0:
            print(f"   ERROR: failed to transfer {synth_image} to {host}")
            return False
        print(f"   {synth_image}: done on {host}")
    return True


def setup_synth(primary, port, service):
    """No-op — synth workload needs no setup beyond deploy."""
    print("   [setup] Synth: no setup needed (app ready after deploy)")
    return True


def seed_synth(primary, port, service, count, results_dir):
    """No-op — synth workload generates data inline."""
    print("   [setup] Synth: no seeding needed")
    return None


def check_synth_ready(host, port, service, timeout_sec=120):
    """Poll until synth-workload responds on GET /health."""
    print(f"   Checking synth readiness on {host}:{port} ...")
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        headers = {}
        if service:
            headers["XDN"] = service
        try:
            r = requests.get(f"http://{host}:{port}/health", headers=headers, timeout=15)
            if r.status_code == 200:
                print(f"   Synth ready (HTTP {r.status_code})")
                return True
        except Exception:
            pass
        time.sleep(5)
    print(f"   TIMEOUT: synth not ready after {timeout_sec}s")
    return False


# ═══════════════════════════════════════════════════════════════════════════════
#  Generic dispatcher functions
# ═══════════════════════════════════════════════════════════════════════════════

_SETUP_DISPATCH = {
    "wordpress":   setup_wordpress,
    "bookcatalog": setup_bookcatalog,
    "tpcc":        setup_tpcc,
    "tpcc-java":   setup_tpcc,
    "hotelres":    setup_hotelres,
    "synth":       setup_synth,
}

_SEED_DISPATCH = {
    "wordpress":   seed_wordpress,
    "bookcatalog": seed_bookcatalog,
    "tpcc":        seed_tpcc,
    "tpcc-java":   seed_tpcc,
    "hotelres":    seed_hotelres,
    "synth":       seed_synth,
}

_CHECK_DISPATCH = {
    "wordpress":   check_wordpress_ready,
    "bookcatalog": check_bookcatalog_ready,
    "tpcc":        check_tpcc_ready,
    "tpcc-java":   check_tpcc_ready,
    "hotelres":    check_hotelres_ready,
    "synth":       check_synth_ready,
}


def setup_app(app_name, primary, port, service):
    """Dispatch to app-specific setup."""
    fn = _SETUP_DISPATCH.get(app_name)
    if fn is None:
        raise ValueError(f"Unknown app: {app_name!r}. Known: {list(_SETUP_DISPATCH)}")
    return fn(primary, port, service)


def seed_app(app_name, primary, port, service, count, results_dir):
    """Dispatch to app-specific seeding."""
    fn = _SEED_DISPATCH.get(app_name)
    if fn is None:
        raise ValueError(f"Unknown app: {app_name!r}. Known: {list(_SEED_DISPATCH)}")
    return fn(primary, port, service, count, results_dir)


def check_app_ready(app_name, host, port, service, timeout_sec=120):
    """Poll until app-specific readiness check passes."""
    fn = _CHECK_DISPATCH.get(app_name)
    if fn is None:
        raise ValueError(f"Unknown app: {app_name!r}. Known: {list(_CHECK_DISPATCH)}")
    return fn(host, port, service, timeout_sec=timeout_sec)


def get_workload_args(app_name, primary, port, service, results_dir):
    """Return the Go load client args for this app.

    Returns a dict with keys:
      - url: full URL for the load client
      - headers: list of ["-H", "key: value"] pairs
      - method: HTTP method ("-X", "POST")
      - extra_args: additional Go client flags (e.g. -payloads-file, -urls-file)
    """
    cfg = APP_CONFIGS.get(app_name)
    if cfg is None:
        raise ValueError(f"Unknown app: {app_name!r}. Known: {list(APP_CONFIGS)}")

    wl = cfg["workload"]
    svc = cfg["service_name"]
    url = f"http://{primary}:{port}{wl['url_path']}"

    headers = ["-H", f"XDN: {svc}"]
    if wl.get("content_type"):
        headers += ["-H", f"Content-Type: {wl['content_type']}"]

    extra_args = []
    if wl.get("uses_payloads_file"):
        # Determine payloads file path based on app
        if app_name == "wordpress":
            payloads_path = os.path.join(results_dir, "xmlrpc_payloads.txt")
        elif app_name in ("tpcc", "tpcc-java"):
            payloads_path = os.path.join(results_dir, "neworder_payloads.txt")
        else:
            payloads_path = os.path.join(results_dir, "payloads.txt")
        extra_args += ["-payloads-file", payloads_path]

    if wl.get("uses_urls_file"):
        urls_path = os.path.join(results_dir, "reservation_urls.txt")
        extra_args += ["-urls-file", urls_path]

    # Default body placeholder (Go client needs a positional arg even with payloads-file)
    if wl.get("payload"):
        body = wl["payload"]
    elif app_name in ("tpcc", "tpcc-java"):
        body = '{"w_id": 1, "c_id": 1}'
    elif app_name == "synth":
        body = make_synth_workload_payload()
    else:
        body = "placeholder"

    return {
        "url": url,
        "headers": headers,
        "method": ["-X", wl["method"]],
        "extra_args": extra_args,
        "body": body,
    }
