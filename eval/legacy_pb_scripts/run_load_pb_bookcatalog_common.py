import warnings as _w; _w.warn("DEPRECATED: Use pb_app_configs and pb_common instead.", DeprecationWarning, stacklevel=2)
"""
run_load_pb_bookcatalog_common.py — Shared helpers for bookcatalog-nd benchmarks.

Contains constants, cluster management utilities, and bookcatalog-specific
helper functions used by run_load_pb_bookcatalog_reflex.py,
run_load_pb_bookcatalog_criu.py, run_load_pb_bookcatalog_rqlite.py,
and run_load_pb_bookcatalog_openebs.py.
"""

import os
import socket
import subprocess
import sys
import time

import requests

# ── Constants ─────────────────────────────────────────────────────────────────

CONTROL_PLANE_HOST = "10.10.1.4"
AR_HOSTS = ["10.10.1.1", "10.10.1.2", "10.10.1.3"]
HTTP_PROXY_PORT = 2300
SERVICE_NAME = "svc"
APP_PORT = 80

BOOKCATALOG_YAML = "../xdn-cli/examples/bob-book-catalog-nondeterministic-service.yaml"
XDN_BINARY = "../bin/xdn"
GP_CONFIG = "../conf/gigapaxos.xdn.3way.cloudlab.properties"
GP_JVM_ARGS = "-DPB_N_PARALLEL_WORKERS=32 -DPB_CAPTURE_ACCUMULATION_MS=1"
SCREEN_SESSION = "xdn_bookcatalog_pb"
SCREEN_LOG = f"screen_logs/{SCREEN_SESSION}.log"

BOOKCATALOG_IMAGE = "fadhilkurnia/xdn-bookcatalog-nd"
REQUIRED_IMAGES = [BOOKCATALOG_IMAGE]
IMAGE_SOURCE_HOST = AR_HOSTS[0]

# Timeouts
TIMEOUT_PORT_SEC = 60
TIMEOUT_APP_SEC = 300
TIMEOUT_PRIMARY_SEC = 60


# ── Cluster Management ───────────────────────────────────────────────────────


def clear_xdn_cluster():
    """Force-clear the XDN cluster: kill processes, remove state, stop containers."""
    print(" > resetting the cluster:")
    for i in range(3):
        host = f"10.10.1.{i+1}"
        os.system(f"ssh {host} sudo fuser -k 2000/tcp 2>/dev/null || true")
        os.system(f"ssh {host} sudo fuser -k 2300/tcp 2>/dev/null || true")
        os.system(
            f"ssh {host} \"sudo df | grep xdn/state/fuselog"
            f" | awk '{{print \\$6}}' | xargs -r sudo umount -l 2>/dev/null || true\""
        )
        os.system(f"ssh {host} sudo rm -rf /tmp/gigapaxos /tmp/xdn /dev/shm/xdn")
        os.system(
            f"ssh {host} 'containers=$(docker ps -a -q); "
            f"if [ -n \"$containers\" ]; then docker stop $containers && docker rm $containers; fi'"
        )
        os.system(f"ssh {host} docker network prune --force > /dev/null 2>&1")
    # RC node
    os.system(f"ssh {CONTROL_PLANE_HOST} sudo fuser -k 3000/tcp 2>/dev/null || true")
    os.system(f"ssh {CONTROL_PLANE_HOST} sudo rm -rf /tmp/gigapaxos")
    print("   done.")


def start_cluster():
    """Start the XDN cluster in a screen session."""
    global GP_JVM_ARGS
    from gp_config_utils import apply_config_overrides
    GP_JVM_ARGS = apply_config_overrides(GP_JVM_ARGS, GP_CONFIG)
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


# ── Readiness Checks ─────────────────────────────────────────────────────────


def wait_for_port(host, port, timeout_sec):
    """Wait for a TCP port to become reachable."""
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


def wait_for_service(hosts, port, service, timeout_sec):
    """Wait for the bookcatalog service to respond via XDN proxy."""
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


# ── Primary Detection ─────────────────────────────────────────────────────────


def detect_primary_via_docker(hosts):
    """Detect primary by finding which AR node has running Docker containers."""
    print("   Detecting primary for 'svc' via docker ps ...")
    for host in hosts:
        result = subprocess.run(
            ["ssh", host, "docker ps -q"],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0 and result.stdout.strip():
            containers = result.stdout.strip().splitlines()
            print(f"   {host} has {len(containers)} running container(s) -> PRIMARY")
            return host
        else:
            print(f"   {host}: no running containers")
    return None


# ── App Data Generation ───────────────────────────────────────────────────────


def seed_books(host, port, service=None, count=200):
    """Seed the bookcatalog app with `count` books via POST /api/books."""
    print(f"   Seeding {count} books on {host}:{port} ...")
    headers = {"Content-Type": "application/json"}
    if service:
        headers["XDN"] = service
    created = 0
    for i in range(count):
        try:
            r = requests.post(
                f"http://{host}:{port}/api/books",
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


def check_app_ready(host, port, service=None):
    """Verify the bookcatalog app is ready by querying GET /api/books."""
    print(f"   Checking app readiness on {host}:{port} ...")
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
            print(f"   App ready (HTTP {r.status_code})")
            return True
        else:
            print(f"   App check returned HTTP {r.status_code}: {r.text[:200]}")
            return False
    except Exception as e:
        print(f"   ERROR checking app: {e}")
        return False


def wait_for_app_ready(host, port, service=None, timeout_sec=120):
    """Poll until the bookcatalog app responds on /api/books."""
    print(f"   Waiting up to {timeout_sec}s for app readiness on {host}:{port} ...")
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        if check_app_ready(host, port, service):
            return True
        time.sleep(5)
    print(f"   TIMEOUT: app not ready after {timeout_sec}s")
    return False


# ── Docker Image Helpers ──────────────────────────────────────────────────────


def ensure_docker_images_on_rc(images=None):
    """Mirror required Docker images to the RC node."""
    for img in (images or REQUIRED_IMAGES):
        check = subprocess.run(
            ["ssh", CONTROL_PLANE_HOST, f"docker image inspect {img}"],
            capture_output=True,
        )
        if check.returncode == 0:
            print(f"   {img}: already on RC, skipping")
            continue

        src_check = subprocess.run(
            ["ssh", IMAGE_SOURCE_HOST, f"docker image inspect {img}"],
            capture_output=True,
        )
        if src_check.returncode != 0:
            print(f"   {img}: pulling on {IMAGE_SOURCE_HOST} ...")
            ret = os.system(f"ssh {IMAGE_SOURCE_HOST} 'docker pull {img}'")
            if ret != 0:
                print(f"   ERROR: failed to pull {img} on {IMAGE_SOURCE_HOST}")
                sys.exit(1)

        print(f"   {img}: transferring {IMAGE_SOURCE_HOST} -> {CONTROL_PLANE_HOST} ...")
        ret = os.system(
            f"ssh {IMAGE_SOURCE_HOST} 'docker save {img}'"
            f" | ssh {CONTROL_PLANE_HOST} 'docker load'"
        )
        if ret != 0:
            print(f"   ERROR: failed to transfer {img} to RC")
            sys.exit(1)
        print(f"   {img}: done")
