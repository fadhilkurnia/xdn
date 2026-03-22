"""
validate_statediff_correctness.py — End-to-end correctness validation for
the byte[] state diff pipeline.

Validates that state diffs captured on the primary are faithfully replicated
and applied on backup nodes by:
  1. Creating data on the primary (books / blog posts)
  2. Waiting for state diff propagation
  3. Launching a standalone container on a backup node mounting the backup's
     replicated state directory
  4. Verifying the standalone container shows all the data

Uses two real applications: bookcatalog-nd and WordPress on the CloudLab
3-way cluster.

Usage:
    cd /users/fadhil/xdn/eval
    python3 -u validate_statediff_correctness.py
    python3 -u validate_statediff_correctness.py --skip-setup
    python3 -u validate_statediff_correctness.py --skip-wordpress
"""

import argparse
import os
import subprocess
import sys
import time

import requests

import run_load_pb_bookcatalog_common as bc_common
import run_load_pb_wordpress_common as wp


# ── Constants ────────────────────────────────────────────────────────────────

AR_HOSTS = bc_common.AR_HOSTS
CONTROL_PLANE_HOST = bc_common.CONTROL_PLANE_HOST
HTTP_PROXY_PORT = bc_common.HTTP_PROXY_PORT
XDN_BINARY = bc_common.XDN_BINARY

BC_SERVICE_NAME = bc_common.SERVICE_NAME  # "svc"
BC_YAML = bc_common.BOOKCATALOG_YAML
BC_IMAGE = bc_common.BOOKCATALOG_IMAGE
BC_BOOK_COUNT = 50
BC_STANDALONE_PORT = 8081
BC_STANDALONE_NAME = "xdn-bc-validate"

WP_SERVICE_NAME = wp.SERVICE_NAME  # "wordpress"
WP_YAML = wp.WORDPRESS_YAML
WP_POST_COUNT = 30
WP_PROPAGATION_WAIT = 15

FUSELOG_BASE_DIR = "/dev/shm/xdn/state/fuselog"
AR_HOST_TO_ID = {"10.10.1.1": "ar0", "10.10.1.2": "ar1", "10.10.1.3": "ar2"}


# ── ValidationResult ─────────────────────────────────────────────────────────


class ValidationResult:
    """Tracks and reports pass/fail for each validation check."""

    def __init__(self):
        self.results = []

    def record(self, name, passed, detail=""):
        status = "PASS" if passed else "FAIL"
        self.results.append((name, passed, detail))
        print(f"   [{status}] {name}" + (f" — {detail}" if detail else ""))

    def all_passed(self):
        return all(passed for _, passed, _ in self.results)

    def print_summary(self):
        print("\n" + "=" * 70)
        print("VALIDATION SUMMARY")
        print("=" * 70)
        for i, (name, passed, detail) in enumerate(self.results, 1):
            status = "PASS" if passed else "FAIL"
            line = f"  {i:2d}. [{status}] {name}"
            if detail:
                line += f"  ({detail})"
            print(line)
        total = len(self.results)
        passed = sum(1 for _, p, _ in self.results if p)
        print("-" * 70)
        print(f"  {passed}/{total} checks passed.")
        if self.all_passed():
            print("  State diff pipeline: CORRECT")
        else:
            print("  State diff pipeline: ISSUES DETECTED")
        print("=" * 70)


# ── Bookcatalog Helpers ──────────────────────────────────────────────────────


def find_bc_backup(primary):
    """Find a backup node and its FUSELOG state path for bookcatalog."""
    backups = [h for h in AR_HOSTS if h != primary]
    backup = backups[0]
    node_id = AR_HOST_TO_ID[backup]
    state_path = f"{FUSELOG_BASE_DIR}/{node_id}/mnt/{BC_SERVICE_NAME}/e0/"
    print(f"   Backup node : {backup} (node ID: {node_id})")
    print(f"   State path  : {state_path}")
    result = subprocess.run(
        ["ssh", backup, f"ls {state_path}"],
        capture_output=True, text=True,
    )
    if result.returncode != 0 or not result.stdout.strip():
        print(f"   ERROR: State dir empty or missing:\n{result.stderr}")
        return None, None
    print(f"   State dir contents: {result.stdout.strip()}")
    return backup, state_path


def launch_bc_standalone(backup, state_path):
    """Launch a standalone bookcatalog container on the backup node."""
    cmd = (
        f"ssh {backup} 'docker run -d --name {BC_STANDALONE_NAME} "
        f"--mount type=bind,source={state_path},target=/app/data/ "
        f"-e ENABLE_WAL=true "
        f"-p {BC_STANDALONE_PORT}:80 "
        f"{BC_IMAGE}'"
    )
    print(f"   {cmd}")
    ret = os.system(cmd)
    return ret == 0


def wait_for_bc_standalone(backup, timeout_sec=60):
    """Wait for the standalone bookcatalog to respond."""
    print(f"   Waiting up to {timeout_sec}s for standalone on {backup}:{BC_STANDALONE_PORT} ...")
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            r = requests.get(
                f"http://{backup}:{BC_STANDALONE_PORT}/api/books",
                timeout=5,
            )
            if r.status_code == 200:
                print(f"   READY (HTTP 200)")
                return True
        except Exception:
            pass
        time.sleep(3)
    print(f"   TIMEOUT")
    return False


def validate_bc_books(host, port, expected_count, use_xdn_header=False, service=None):
    """GET /api/books and verify at least expected_count books are present."""
    headers = {}
    if use_xdn_header and service:
        headers["XDN"] = service
    try:
        r = requests.get(
            f"http://{host}:{port}/api/books",
            headers=headers,
            timeout=15,
        )
        if r.status_code != 200:
            return False, f"HTTP {r.status_code}"
        books = r.json()
        count = len(books)
        if count >= expected_count:
            return True, f"{count} books found (expected >= {expected_count})"
        else:
            return False, f"only {count} books found (expected >= {expected_count})"
    except Exception as e:
        return False, f"error: {e}"


def cleanup_bc_standalone(backup):
    """Stop and remove the standalone bookcatalog container."""
    os.system(
        f"ssh {backup} 'docker stop {BC_STANDALONE_NAME} && "
        f"docker rm {BC_STANDALONE_NAME}' 2>/dev/null || true"
    )
    print(f"   Cleaned up standalone container on {backup}")


# ── Ensure Docker Images on RC ───────────────────────────────────────────────


def ensure_images_on_rc(images):
    """Ensure all required Docker images are present on the RC node."""
    print(" > Ensuring Docker images on RC ...")
    for img in images:
        check = subprocess.run(
            ["ssh", CONTROL_PLANE_HOST, f"docker image inspect {img}"],
            capture_output=True,
        )
        if check.returncode == 0:
            print(f"   {img}: already on RC")
            continue

        src = AR_HOSTS[0]
        src_check = subprocess.run(
            ["ssh", src, f"docker image inspect {img}"],
            capture_output=True,
        )
        if src_check.returncode != 0:
            print(f"   {img}: pulling on {src} ...")
            ret = os.system(f"ssh {src} 'docker pull {img}'")
            if ret != 0:
                print(f"   ERROR: failed to pull {img}")
                return False

        print(f"   {img}: transferring {src} -> {CONTROL_PLANE_HOST} ...")
        ret = os.system(
            f"ssh {src} 'docker save {img}'"
            f" | ssh {CONTROL_PLANE_HOST} 'docker load'"
        )
        if ret != 0:
            print(f"   ERROR: failed to transfer {img}")
            return False
        print(f"   {img}: done")
    return True


# ── Destroy a Service ────────────────────────────────────────────────────────


def destroy_service(service_name):
    """Delete a deployed service via xdn delete."""
    cmd = (
        f"XDN_CONTROL_PLANE={CONTROL_PLANE_HOST} {XDN_BINARY} "
        f"delete {service_name}"
    )
    print(f"   {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    print(f"   {result.stdout.strip()}")
    if result.returncode != 0:
        print(f"   WARNING: delete returned non-zero: {result.stderr.strip()}")
    # Wait for containers to stop
    time.sleep(10)


# ── Phase: Bookcatalog ───────────────────────────────────────────────────────


def run_bookcatalog_validation(vr):
    """Run bookcatalog-nd correctness validation."""
    print("\n" + "=" * 60)
    print("BOOKCATALOG-ND VALIDATION")
    print("=" * 60)

    # Deploy
    print("\n[BC-1] Deploying bookcatalog-nd service ...")
    cmd = (
        f"XDN_CONTROL_PLANE={CONTROL_PLANE_HOST} {XDN_BINARY} "
        f"launch {BC_SERVICE_NAME} --file={BC_YAML}"
    )
    print(f"   {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    print(f"   {result.stdout.strip()}")
    deploy_ok = result.returncode == 0
    vr.record("BC: Service deployed", deploy_ok,
              result.stderr.strip().split("\n")[0] if not deploy_ok else "")
    if not deploy_ok:
        return

    # Wait for readiness
    print("\n[BC-2] Waiting for service readiness ...")
    ok, _ = bc_common.wait_for_service(
        AR_HOSTS, HTTP_PROXY_PORT, BC_SERVICE_NAME, bc_common.TIMEOUT_APP_SEC
    )
    if not ok:
        vr.record("BC: Service ready", False, "timeout")
        return

    # Detect primary
    print("\n[BC-3] Detecting primary ...")
    primary = bc_common.detect_primary_via_docker(AR_HOSTS)
    if not primary:
        vr.record("BC: Primary detected", False)
        return
    print(f"   Primary: {primary}")

    # Seed books
    print(f"\n[BC-4] Seeding {BC_BOOK_COUNT} books ...")
    created = bc_common.seed_books(
        primary, HTTP_PROXY_PORT, service=BC_SERVICE_NAME, count=BC_BOOK_COUNT
    )
    seed_ok = created == BC_BOOK_COUNT
    vr.record("BC: Books seeded", seed_ok, f"{created}/{BC_BOOK_COUNT}")
    if not seed_ok:
        return

    # Wait for propagation
    print(f"\n[BC-5] Waiting 10s for state diff propagation ...")
    time.sleep(10)

    # Validate A: read via XDN on backup
    print("\n[BC-6] Validating books readable via XDN on backup ...")
    backups = [h for h in AR_HOSTS if h != primary]
    backup_host = backups[0]
    passed, detail = validate_bc_books(
        backup_host, HTTP_PROXY_PORT, BC_BOOK_COUNT,
        use_xdn_header=True, service=BC_SERVICE_NAME,
    )
    vr.record("BC: Books readable via XDN on backup", passed, detail)

    # Validate B: standalone backup state
    print("\n[BC-7] Locating backup state directory ...")
    backup, state_path = find_bc_backup(primary)
    state_exists = backup is not None
    vr.record("BC: Backup state dir exists", state_exists,
              state_path if state_exists else "not found")
    if not state_exists:
        return

    print("\n[BC-8] Launching standalone container on backup ...")
    launched = launch_bc_standalone(backup, state_path)
    if not launched:
        vr.record("BC: Standalone reads all books", False, "container launch failed")
        return

    ready = wait_for_bc_standalone(backup)
    if not ready:
        vr.record("BC: Standalone reads all books", False, "container not ready")
        cleanup_bc_standalone(backup)
        return

    passed, detail = validate_bc_books(
        backup, BC_STANDALONE_PORT, BC_BOOK_COUNT,
    )
    vr.record("BC: Standalone reads all books", passed, detail)

    # Cleanup
    print("\n[BC-9] Cleaning up standalone container ...")
    cleanup_bc_standalone(backup)

    # Destroy service
    print("\n[BC-10] Destroying bookcatalog service ...")
    destroy_service(BC_SERVICE_NAME)


# ── Phase: WordPress ─────────────────────────────────────────────────────────


def run_wordpress_validation(vr):
    """Run WordPress correctness validation."""
    print("\n" + "=" * 60)
    print("WORDPRESS VALIDATION")
    print("=" * 60)

    # Deploy
    print("\n[WP-1] Deploying WordPress service ...")
    cmd = (
        f"XDN_CONTROL_PLANE={CONTROL_PLANE_HOST} {XDN_BINARY} "
        f"launch {WP_SERVICE_NAME} --file={WP_YAML}"
    )
    print(f"   {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    print(f"   {result.stdout.strip()}")
    deploy_ok = result.returncode == 0
    vr.record("WP: Service deployed", deploy_ok,
              result.stderr.strip().split("\n")[0] if not deploy_ok else "")
    if not deploy_ok:
        return

    # Wait for readiness (WordPress + MySQL startup can be slow)
    print("\n[WP-2] Waiting for WordPress readiness (up to 600s) ...")
    ok, _ = wp.wait_for_service(
        AR_HOSTS, HTTP_PROXY_PORT, WP_SERVICE_NAME, wp.TIMEOUT_WP_SEC
    )
    if not ok:
        vr.record("WP: Service ready", False, "timeout")
        return

    # Detect primary (try XDN protocol role header first, then docker ps fallback)
    print("\n[WP-3] Detecting primary ...")
    primary = wp.detect_primary(
        AR_HOSTS, HTTP_PROXY_PORT, WP_SERVICE_NAME, wp.TIMEOUT_PRIMARY_SEC
    )
    if not primary:
        print("   XDN role header detection failed, trying docker ps ...")
        primary = bc_common.detect_primary_via_docker(AR_HOSTS)
    if not primary:
        print("   ERROR: Could not detect primary via any method")
        vr.record("WP: Primary detected", False)
        return
    print(f"   Primary: {primary}")

    # Install WordPress
    print("\n[WP-4] Installing WordPress ...")
    ok = wp.install_wordpress(primary, HTTP_PROXY_PORT, WP_SERVICE_NAME)
    if not ok:
        vr.record("WP: WordPress installed", False)
        return

    # Enable REST auth
    print("\n[WP-5] Enabling REST API auth ...")
    ok = wp.enable_rest_auth(primary)
    if not ok:
        vr.record("WP: REST auth enabled", False)
        return

    # Check REST API
    print("\n[WP-6] Checking REST API availability ...")
    ok = wp.check_rest_api(primary, HTTP_PROXY_PORT, WP_SERVICE_NAME)
    if not ok:
        vr.record("WP: REST API available", False)
        return

    # Create posts
    print(f"\n[WP-7] Creating {WP_POST_COUNT} blog posts ...")
    post_ids = []
    for i in range(WP_POST_COUNT):
        title = f"Validation Post {i+1}"
        content = f"State diff correctness test post number {i+1}."
        pid = wp.create_post(primary, HTTP_PROXY_PORT, WP_SERVICE_NAME, title, content)
        if pid:
            post_ids.append(pid)
    posts_ok = len(post_ids) == WP_POST_COUNT
    vr.record("WP: Posts created", posts_ok,
              f"{len(post_ids)}/{WP_POST_COUNT}")
    if not posts_ok:
        return

    # Wait for propagation
    print(f"\n[WP-8] Waiting {WP_PROPAGATION_WAIT}s for state diff propagation ...")
    time.sleep(WP_PROPAGATION_WAIT)

    # Validate A: posts readable on primary
    print("\n[WP-9] Validating posts on primary ...")
    primary_ok = wp.validate_posts(
        primary, HTTP_PROXY_PORT, WP_SERVICE_NAME, post_ids
    )
    vr.record("WP: Posts readable on primary", primary_ok)

    # Validate B: standalone backup state
    print("\n[WP-10] Locating backup state directory ...")
    backup, state_path = wp.find_backup(primary)
    state_exists = backup is not None
    vr.record("WP: Backup state dir exists", state_exists,
              state_path if state_exists else "not found")
    if not state_exists:
        return

    print("\n[WP-11] Launching standalone WordPress on backup ...")
    launched = wp.launch_standalone(backup, state_path)
    if not launched:
        vr.record("WP: Standalone reads all posts", False, "compose up failed")
        return

    ready = wp.wait_for_standalone(backup, wp.STANDALONE_PORT, 120)
    if not ready:
        vr.record("WP: Standalone reads all posts", False, "not ready")
        wp.cleanup_standalone(backup)
        return

    standalone_ok = wp.validate_posts_standalone(
        backup, wp.STANDALONE_PORT, post_ids
    )
    vr.record("WP: Standalone reads all posts", standalone_ok)

    # Cleanup
    print("\n[WP-12] Cleaning up standalone containers ...")
    wp.cleanup_standalone(backup)


# ── Main ─────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="Validate state diff correctness after byte[] pipeline change"
    )
    parser.add_argument("--skip-setup", action="store_true",
                        help="Skip cluster clear and start")
    parser.add_argument("--skip-bookcatalog", action="store_true",
                        help="Skip bookcatalog-nd validation")
    parser.add_argument("--skip-wordpress", action="store_true",
                        help="Skip WordPress validation")
    args = parser.parse_args()

    vr = ValidationResult()

    print("=" * 60)
    print("STATE DIFF CORRECTNESS VALIDATION")
    print("byte[] pipeline end-to-end test")
    print("=" * 60)

    # Phase 1: Cluster setup
    if not args.skip_setup:
        print("\n[Setup-1] Force-clearing cluster ...")
        bc_common.clear_xdn_cluster()

        print("\n[Setup-2] Starting XDN cluster ...")
        bc_common.start_cluster()

        print("\n[Setup-3] Waiting for Active Replicas ...")
        for host in AR_HOSTS:
            ok = bc_common.wait_for_port(host, HTTP_PROXY_PORT, bc_common.TIMEOUT_PORT_SEC)
            if not ok:
                print(f"ERROR: AR at {host}:{HTTP_PROXY_PORT} not ready.")
                sys.exit(1)

        print("\n[Setup-4] Ensuring Docker images on RC ...")
        all_images = list(set(
            bc_common.REQUIRED_IMAGES + wp.REQUIRED_IMAGES
        ))
        if not ensure_images_on_rc(all_images):
            print("ERROR: Failed to ensure Docker images on RC.")
            sys.exit(1)
    else:
        print("\n[Setup] Skipped (--skip-setup)")

    # Phase 2: Bookcatalog validation
    if not args.skip_bookcatalog:
        run_bookcatalog_validation(vr)
    else:
        print("\n[Bookcatalog] Skipped (--skip-bookcatalog)")

    # Phase 3: WordPress validation
    if not args.skip_wordpress:
        run_wordpress_validation(vr)
    else:
        print("\n[WordPress] Skipped (--skip-wordpress)")

    # Summary
    vr.print_summary()
    sys.exit(0 if vr.all_passed() else 1)


if __name__ == "__main__":
    main()
