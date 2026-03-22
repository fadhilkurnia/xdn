import base64
import json
import os
import socket
import subprocess
import sys
import time

import requests

ADMIN_USER     = "admin"
ADMIN_PASSWORD = "AdminPass123!"
# Set by enable_rest_auth() — WP Application Password for REST API calls.
# Overrides ADMIN_PASSWORD when set.
ADMIN_APP_PASSWORD = None
ADMIN_EMAIL    = "admin@example.com"
SITE_TITLE     = "WordPress XDN Test"

TEST_POSTS = [
    ("XDN Test Post 1", "This is the first test post created via XDN."),
    ("XDN Test Post 2", "Testing primary-backup replication with a second post."),
    ("XDN Test Post 3", "Verifying linearizability of WordPress writes through XDN."),
]

FUSELOG_BASE_DIR   = "/dev/shm/xdn/state/fuselog"
AR_HOST_TO_ID      = {"10.10.1.1": "ar0", "10.10.1.2": "ar1", "10.10.1.3": "ar2"}
STANDALONE_PORT    = 8080
STANDALONE_DIR     = "/tmp/xdn_backup_validation"

CONTROL_PLANE_HOST = "10.10.1.4"
AR_HOSTS = ["10.10.1.1", "10.10.1.2", "10.10.1.3"]
HTTP_PROXY_PORT = 2300
SERVICE_NAME = "wordpress"
WORDPRESS_YAML = "../xdn-cli/examples/wordpress.yaml"
XDN_BINARY = "../bin/xdn"
GP_CONFIG = "../conf/gigapaxos.xdn.wordpress-pb.cloudlab.properties"
GP_JVM_ARGS = (
    "-DSYNC=true "                        # fsync Paxos proposals to disk for durability
    "-DPB_N_PARALLEL_WORKERS=32 "
    "-DPB_CAPTURE_ACCUMULATION_US=100 "  # 0.1ms window: low-latency capture with minimal batching delay
    "-DHTTP_FORCE_KEEPALIVE=true "       # prevent Apache Connection:close from killing pooled channels
    "-DFUSELOG_DISABLE_COALESCING=true"   # skip read-before-write in fuselog (saves ~1ms per MySQL write)
)
SCREEN_SESSION = "xdn_wordpress_pb"
SCREEN_LOG = f"screen_logs/{SCREEN_SESSION}.log"

# Timeouts
TIMEOUT_PORT_SEC = 60     # AR JVM startup + port bind
TIMEOUT_WP_SEC = 600      # MySQL init (~90s) + WP first-boot init + rsync of state
TIMEOUT_PRIMARY_SEC = 60  # PB election after service init

REQUIRED_IMAGES = ["mysql:8.4.0", "wordpress:6.5.4-apache"]
IMAGE_SOURCE_HOST = AR_HOSTS[0]


# Phase 1 — Force-Clear Cluster

def clear_xdn_cluster():
    print(" > resetting the cluster:")
    for i in range(3):
        host = f"10.10.1.{i+1}"
        # 1. Stop Docker containers FIRST — they hold files open inside FUSE mounts
        os.system(
            f"ssh {host} 'containers=$(docker ps -a -q); "
            f"if [ -n \"$containers\" ]; then docker stop $containers && docker rm $containers; fi'"
        )
        os.system(f"ssh {host} docker network prune --force > /dev/null 2>&1")
        # 2. Kill Java (GigaPaxos) processes
        os.system(f"ssh {host} sudo fuser -k 2000/tcp 2>/dev/null || true")
        os.system(f"ssh {host} sudo fuser -k 2300/tcp 2>/dev/null || true")
        # 3. Kill fuselog FUSE processes (not bound to TCP ports)
        os.system(f"ssh {host} sudo pkill -9 -f fuselog 2>/dev/null || true")
        # 4. Unmount FUSE filesystems. Use /proc/mounts instead of df because
        #    df fails on stale FUSE mounts ("Transport endpoint is not connected").
        os.system(
            f"ssh {host} \"grep fuselog /proc/mounts"
            f" | awk '{{print \\$2}}' | xargs -r sudo umount 2>/dev/null;"
            f" grep fuselog /proc/mounts"
            f" | awk '{{print \\$2}}' | xargs -r sudo umount -l 2>/dev/null || true\""
        )
        # 5. Brief wait for unmount completion, then remove state dirs
        os.system(f"ssh {host} 'sleep 1 && sudo rm -rf /tmp/gigapaxos /tmp/xdn /dev/shm/xdn'")
    # RC node
    os.system(f"ssh {CONTROL_PLANE_HOST} sudo fuser -k 3000/tcp 2>/dev/null || true")
    os.system(f"ssh {CONTROL_PLANE_HOST} sudo rm -rf /tmp/gigapaxos")

    # Kill leftover k8s/OpenEBS agents (io-engine, kubelet, etc.) that steal
    # CPU from XDN benchmarks.  This is safe because the PB evaluation does
    # not use Kubernetes.
    for i in range(3):
        host = f"10.10.1.{i+1}"
        os.system(
            f"ssh {host} '"
            f"sudo systemctl stop kubelet 2>/dev/null;"
            f" sudo pkill -9 io-engine 2>/dev/null;"
            f" sudo pkill -9 kube-proxy 2>/dev/null;"
            f" sudo pkill -9 minio 2>/dev/null;"
            f" sudo pkill -9 -f \"agent-ha|csi-node|openebs|obs-callhome"
            f"|operator-diskpool|metrics-exporter|nats-server\" 2>/dev/null;"
            f" sudo pkill -9 -f \"containerd-shim.*k8s.io\" 2>/dev/null;"
            f" true'"
        )
    print("   done.")


# Phase 2 — Start Cluster

def start_cluster():
    os.makedirs("screen_logs", exist_ok=True)
    os.system(f"rm -f {SCREEN_LOG}")
    cmd = (
        f"screen -L -Logfile {SCREEN_LOG} -S {SCREEN_SESSION} -d -m bash -c "
        f"'../bin/gpServer.sh -DgigapaxosConfig={GP_CONFIG} {GP_JVM_ARGS} start all; exec bash'"
    )
    print(f"   {cmd}")
    ret = os.system(cmd)
    assert ret == 0, "Failed to start cluster in screen session"
    print(f"   Screen log: {SCREEN_LOG}")
    time.sleep(20)


# Phase 3 — Wait for TCP port

def wait_for_port(host, port, timeout_sec):
    print(f"   Waiting for {host}:{port} ...")
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            s = socket.create_connection((host, port), timeout=2)
            s.close()
            print(f"   OK: {host}:{port}")
            return True
        except OSError:
            time.sleep(2)
    print(f"   TIMEOUT: {host}:{port} not open after {timeout_sec}s")
    return False


# Phase 5 — Wait for WordPress HTTP readiness

def wait_for_service(hosts, port, service, timeout_sec):
    print(f"   Waiting up to {timeout_sec}s for '{service}' to respond ...")
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        for host in hosts:
            try:
                r = requests.get(
                    f"http://{host}:{port}/",
                    headers={"XDN": service},
                    timeout=5,
                )
                if 200 <= r.status_code < 500:
                    print(f"   READY: HTTP {r.status_code} from {host}")
                    return True, host
            except Exception:
                pass
        elapsed = int(timeout_sec - (deadline - time.time()))
        print(f"   ... {elapsed}s elapsed, still waiting ...")
        time.sleep(5)
    return False, None


# Phase 6 — Detect primary node

def detect_primary(hosts, port, service, timeout_sec):
    print(f"   Detecting primary for '{service}' ...")
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        for host in hosts:
            try:
                r = requests.get(
                    f"http://{host}:{port}/",
                    headers={"XdnGetProtocolRoleRequest": "true", "XDN": service},
                    timeout=3,
                )
                data = json.loads(r.text)
                if data.get("role") == "primary":
                    return host
            except Exception:
                pass
        time.sleep(3)
    return None


# Phase 8 — Complete WordPress Installation

def install_wordpress(host, port, service):
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


# Phase 8b — Enable REST API auth via WP Application Password (WP 5.6+, native Basic Auth)

def enable_rest_auth(primary):
    """Enable WP REST API auth over HTTP using Application Passwords.

    Steps:
    1. Install a mu-plugin that sets wp_is_application_passwords_available=true
       (WP disables App Passwords on non-HTTPS by default).
    2. Create a WP Application Password for the admin user via PHP CLI.
    3. Store the generated password in ADMIN_APP_PASSWORD module global.
    """
    global ADMIN_APP_PASSWORD
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
    import re as _re

    def _is_valid_app_password(pw):
        """WP Application Passwords are 24 hex-like chars with spaces (e.g. 'abcd 1234 ...')."""
        if not pw or len(pw) < 10:
            return False
        # Must not contain HTML tags or common error markers
        if "<" in pw or "ERROR" in pw or "Fatal" in pw:
            return False
        # Should be alphanumeric + spaces only
        return bool(_re.match(r'^[A-Za-z0-9 ]+$', pw))

    # Retry up to 5 times — WordPress may still be finishing installation
    for attempt in range(1, 6):
        result = subprocess.run(
            ["ssh", primary, f"docker exec -i {cid} php"],
            input=php_script, text=True, capture_output=True,
        )
        app_password = result.stdout.strip()
        if result.returncode == 0 and _is_valid_app_password(app_password):
            ADMIN_APP_PASSWORD = app_password
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

    php_snippet = (
        "define('DISABLE_WP_CRON', true);\n"
        "define('AUTOMATIC_UPDATER_DISABLED', true);\n"
        "define('WP_AUTO_UPDATE_CORE', false);\n"
    )
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


# Phase 9 — Check REST API Availability

def _rest_auth_header():
    """Return the Authorization header value for REST API calls."""
    password = ADMIN_APP_PASSWORD or ADMIN_PASSWORD
    return "Basic " + base64.b64encode(f"{ADMIN_USER}:{password}".encode()).decode()


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
                    "Authorization": _rest_auth_header(),
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


# Phase 10 — Create Blog Posts via REST API

def create_post(host, port, service, title, content):
    r = requests.post(
        f"http://{host}:{port}/?rest_route=/wp/v2/posts",
        headers={
            "XDN": service,
            "Content-Type": "application/json",
            "Authorization": _rest_auth_header(),
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


# Phase 11 — Validate Posts are Readable

def validate_posts(host, port, service, expected_ids):
    """Validate each post is publicly readable via GET /?p=<id>."""
    all_ok = True
    for pid in expected_ids:
        r = requests.get(
            f"http://{host}:{port}/?p={pid}",
            headers={"XDN": service},
            timeout=15,
            allow_redirects=True,
        )
        status = "OK" if r.status_code == 200 else "MISSING"
        print(f"   Post id={pid}: {status} (HTTP {r.status_code})")
        if r.status_code != 200:
            all_ok = False
    return all_ok


# Phase 12 — Locate backup node and verify state directory

def find_backup(primary):
    backups = [h for h in AR_HOSTS if h != primary]
    backup = backups[0]
    node_id = AR_HOST_TO_ID[backup]
    state_path = f"{FUSELOG_BASE_DIR}/{node_id}/mnt/{SERVICE_NAME}/e0/"
    print(f"   Backup node : {backup} (node ID: {node_id})")
    print(f"   State path  : {state_path}")
    result = subprocess.run(
        ["ssh", backup, f"ls {state_path}"],
        capture_output=True, text=True
    )
    if result.returncode != 0 or not result.stdout.strip():
        print(f"   ERROR: State dir empty or missing:\n{result.stderr}")
        return None, None
    print(f"   State dir contents: {result.stdout.strip()}")
    return backup, state_path


# Phase 13 — Launch standalone WordPress on backup using replicated state

COMPOSE_TEMPLATE = """\
services:
  database:
    image: mysql:8.4.0
    hostname: database
    environment:
      - MYSQL_ROOT_PASSWORD=supersecret
      - MYSQL_DATABASE=wordpress
    volumes:
      - type: bind
        source: {state_path}
        target: /var/lib/mysql/
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "localhost", "-psupersecret"]
      interval: 5s
      timeout: 3s
      retries: 30
      start_period: 15s
  wordpress:
    image: wordpress:6.5.4-apache
    hostname: wordpress
    ports:
      - "{port}:80"
    depends_on:
      database:
        condition: service_healthy
    environment:
      - WORDPRESS_DB_HOST=database
      - WORDPRESS_DB_USER=root
      - WORDPRESS_DB_PASSWORD=supersecret
      - WORDPRESS_DB_NAME=wordpress
"""


def launch_standalone(backup, state_path):
    compose_content = COMPOSE_TEMPLATE.format(
        state_path=state_path, port=STANDALONE_PORT
    )
    # Create dir on backup
    os.system(f"ssh {backup} 'mkdir -p {STANDALONE_DIR}'")
    # Write compose file via stdin pipe
    result = subprocess.run(
        f"ssh {backup} 'cat > {STANDALONE_DIR}/docker-compose.yml'",
        input=compose_content, shell=True, capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"   ERROR writing compose file: {result.stderr}")
        return False
    ret = os.system(
        f"ssh {backup} 'cd {STANDALONE_DIR} && docker compose up -d'"
    )
    return ret == 0


def wait_for_standalone(backup, port, timeout_sec):
    print(f"   Waiting up to {timeout_sec}s for standalone WordPress on {backup}:{port} ...")
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            r = requests.get(f"http://{backup}:{port}/", timeout=5,
                             allow_redirects=True)
            if r.status_code == 200:
                print(f"   READY (HTTP 200)")
                return True
        except Exception:
            pass
        time.sleep(5)
    print(f"   TIMEOUT")
    return False


# Phase 14 — Validate posts on backup's standalone WordPress

def validate_posts_standalone(backup, port, post_ids):
    all_ok = True
    for pid in post_ids:
        r = requests.get(f"http://{backup}:{port}/?p={pid}",
                         timeout=15, allow_redirects=True)
        status = "OK" if r.status_code == 200 else "MISSING"
        print(f"   Post id={pid}: {status} (HTTP {r.status_code})")
        if r.status_code != 200:
            all_ok = False
    return all_ok


# Phase 15 — Cleanup standalone containers

def cleanup_standalone(backup):
    os.system(
        f"ssh {backup} 'cd {STANDALONE_DIR} && docker compose down --volumes || true'"
    )
    os.system(f"ssh {backup} 'rm -rf {STANDALONE_DIR}'")
    print(f"   Cleaned up standalone containers on {backup}")


# ─── Main ────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    print("=" * 60)
    print("WordPress Primary-Backup Investigation")
    print("=" * 60)

    # Phase 1
    print("\n[Phase 1] Force-clearing cluster ...")
    clear_xdn_cluster()

    # Phase 2
    print("\n[Phase 2] Starting XDN cluster ...")
    start_cluster()

    # Phase 3
    print("\n[Phase 3] Waiting for Active Replicas ...")
    for host in AR_HOSTS:
        ok = wait_for_port(host, HTTP_PROXY_PORT, TIMEOUT_PORT_SEC)
        if not ok:
            print(f"ERROR: AR at {host}:{HTTP_PROXY_PORT} not ready.")
            print(f"       Check: tail -f {SCREEN_LOG}")
            sys.exit(1)

    # Phase 3b — Ensure Docker images are present on RC so the image validator passes.
    # The RC never runs containers so images may not be cached there and may not be
    # pullable (rate-limited). Pull on AR0 if needed, then transfer via save|load.
    print("\n[Phase 3b] Ensuring Docker images are present on RC ...")
    for img in REQUIRED_IMAGES:
        # Skip if already present on RC
        check = subprocess.run(
            ["ssh", CONTROL_PLANE_HOST, f"docker image inspect {img}"],
            capture_output=True
        )
        if check.returncode == 0:
            print(f"   {img}: already on RC, skipping")
            continue

        # Ensure image is present on source AR (pull from Docker Hub if needed)
        src_check = subprocess.run(
            ["ssh", IMAGE_SOURCE_HOST, f"docker image inspect {img}"],
            capture_output=True
        )
        if src_check.returncode != 0:
            print(f"   {img}: pulling on {IMAGE_SOURCE_HOST} ...")
            ret = os.system(f"ssh {IMAGE_SOURCE_HOST} 'docker pull {img}'")
            if ret != 0:
                print(f"   ERROR: failed to pull {img} on {IMAGE_SOURCE_HOST}")
                sys.exit(1)

        print(f"   {img}: transferring from {IMAGE_SOURCE_HOST} to {CONTROL_PLANE_HOST} ...")
        ret = os.system(
            f"ssh {IMAGE_SOURCE_HOST} 'docker save {img}'"
            f" | ssh {CONTROL_PLANE_HOST} 'docker load'"
        )
        if ret != 0:
            print(f"   ERROR: failed to transfer {img} to RC")
            sys.exit(1)
        print(f"   {img}: done")

    # Phase 4
    print("\n[Phase 4] Launching WordPress service ...")
    cmd = (
        f"XDN_CONTROL_PLANE={CONTROL_PLANE_HOST} {XDN_BINARY} "
        f"launch {SERVICE_NAME} --file={WORDPRESS_YAML}"
    )
    print(f"   {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        print(f"ERROR: xdn launch failed:\n{result.stderr}")
        sys.exit(1)

    # Phase 5
    print("\n[Phase 5] Waiting for WordPress HTTP readiness ...")
    ok, responding_host = wait_for_service(AR_HOSTS, HTTP_PROXY_PORT, SERVICE_NAME, TIMEOUT_WP_SEC)
    if not ok:
        print(f"ERROR: WordPress not ready after {TIMEOUT_WP_SEC}s.")
        print("Tips:  ssh 10.10.1.X docker ps -a    (check primary's containers)")
        print(f"       tail -f {SCREEN_LOG}           (check XDN cluster logs)")
        sys.exit(1)

    # Phase 6
    print("\n[Phase 6] Detecting primary node ...")
    primary = detect_primary(AR_HOSTS, HTTP_PROXY_PORT, SERVICE_NAME, TIMEOUT_PRIMARY_SEC)
    if primary:
        print(f"   Primary node: {primary}")
    else:
        print("   WARNING: Could not detect primary, falling back to first AR")
        primary = AR_HOSTS[0]

    # Phase 7
    print("\n[Phase 7] Sending test requests to primary ...")
    test_paths = [
        ("/",             "WordPress front page"),
        ("/wp-login.php", "WordPress login page"),
        ("/wp-json/",     "WordPress REST API"),
    ]
    for path, desc in test_paths:
        try:
            r = requests.get(
                f"http://{primary}:{HTTP_PROXY_PORT}{path}",
                headers={"XDN": SERVICE_NAME},
                timeout=15,
                allow_redirects=False,
            )
            print(f"   [{desc}] {path} -> HTTP {r.status_code}, {len(r.content)} bytes")
        except Exception as e:
            print(f"   [{desc}] {path} -> ERROR: {e}")

    # Phase 8
    print("\n[Phase 8] Installing WordPress ...")
    ok = install_wordpress(primary, HTTP_PROXY_PORT, SERVICE_NAME)
    if not ok:
        print("ERROR: WordPress installation failed.")
        sys.exit(1)
    # Phase 9
    print("\n[Phase 9] Enabling REST API Basic Auth and checking availability ...")
    if not enable_rest_auth(primary):
        print("ERROR: REST API auth setup failed.")
        sys.exit(1)
    disable_wp_cron(primary)
    ok = check_rest_api(primary, HTTP_PROXY_PORT, SERVICE_NAME)
    if not ok:
        print("ERROR: REST API not available.")
        sys.exit(1)

    # Phase 10
    print("\n[Phase 10] Creating blog posts via REST API ...")
    post_ids = []
    for title, content in TEST_POSTS:
        pid = create_post(primary, HTTP_PROXY_PORT, SERVICE_NAME, title, content)
        if pid:
            post_ids.append(pid)
    if len(post_ids) != len(TEST_POSTS):
        print(f"ERROR: Only {len(post_ids)}/{len(TEST_POSTS)} posts created.")
        sys.exit(1)

    # Phase 11
    print("\n[Phase 11] Validating created posts are readable ...")
    ok = validate_posts(primary, HTTP_PROXY_PORT, SERVICE_NAME, post_ids)
    if ok:
        print("   All posts validated successfully.")
    else:
        print("ERROR: Some posts not accessible via GET /?p=<id>.")
        sys.exit(1)

    print("\n[Done] WordPress REST API post creation validated.")

    # Phase 12
    print("\n[Phase 12] Locating backup node and verifying state directory ...")
    backup, backup_state_path = find_backup(primary)
    if not backup:
        print("ERROR: Backup state not found.")
        sys.exit(1)
    print(f"   Using backup: {backup}")

    # Phase 13
    print("\n[Phase 13] Launching standalone WordPress on backup using replicated state ...")
    ok = launch_standalone(backup, backup_state_path)
    if not ok:
        print("ERROR: docker compose up failed on backup.")
        sys.exit(1)
    ok = wait_for_standalone(backup, STANDALONE_PORT, 120)
    if not ok:
        print("ERROR: Standalone WordPress did not become ready.")
        sys.exit(1)

    # Phase 14
    print("\n[Phase 14] Validating posts on backup's standalone WordPress ...")
    ok = validate_posts_standalone(backup, STANDALONE_PORT, post_ids)
    if ok:
        print("   All posts visible on backup — state sync verified!")
    else:
        print("ERROR: Some posts missing from backup standalone instance.")
        sys.exit(1)

    # Phase 15
    print("\n[Phase 15] Cleaning up standalone containers ...")
    cleanup_standalone(backup)

    print("\n[Done] Backup state synchronization validated end-to-end.")
