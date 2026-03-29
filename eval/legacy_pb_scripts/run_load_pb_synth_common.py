import warnings as _w; _w.warn("DEPRECATED: Use pb_app_configs and pb_common instead.", DeprecationWarning, stacklevel=2)
"""
run_load_pb_synth_common.py — Shared helpers for synth-workload benchmarks.

Reuses cluster management from run_load_pb_bookcatalog_common.py.
Provides synth-workload-specific constants and helpers.
"""

import json
import os
import subprocess
import sys

import requests

import run_load_pb_bookcatalog_common as _bc_common

from run_load_pb_bookcatalog_common import (  # noqa: F401 — re-exported
    AR_HOSTS,
    CONTROL_PLANE_HOST,
    HTTP_PROXY_PORT,
    SCREEN_LOG,
    TIMEOUT_APP_SEC,
    TIMEOUT_PORT_SEC,
    clear_xdn_cluster,
    detect_primary_via_docker,
    ensure_docker_images_on_rc,
    start_cluster,
    wait_for_port,
    wait_for_service,
)

# ── Synth-workload constants ─────────────────────────────────────────────────

SERVICE_NAME = "synth"
SYNTH_YAML = "../xdn-cli/examples/synth-workload.yaml"
SYNTH_IMAGE = "fadhilkurnia/xdn-synth-workload"
XDN_BINARY = "../bin/xdn"
GP_CONFIG = "../conf/gigapaxos.xdn.3way.cloudlab.properties"
# SCREEN_LOG is re-exported from run_load_pb_bookcatalog_common (same cluster config)

# Override JVM args with the same tuning used in WordPress PB benchmarks.
# This mutates the bookcatalog_common module variable so start_cluster() picks it up.
_bc_common.GP_JVM_ARGS = (
    "-DSYNC=true "                        # fsync Paxos proposals to disk for durability
    "-DPB_N_PARALLEL_WORKERS=256 "
    "-DPB_CAPTURE_ACCUMULATION_US=100 "   # 0.1ms min, adaptive up to 5ms
    "-DHTTP_FORCE_KEEPALIVE=true "        # prevent Connection:close from killing pooled channels
    "-DFUSELOG_DISABLE_COALESCING=true "   # skip read-before-write in fuselog
    "-DXDN_MIGRATE_WAIT_SEC=5 "           # reduce MySQL migration wait for benchmarks
    "-DXDN_SKIP_INIT_SYNC=true "          # skip rsync to backups; PB still works via statediffs
    "-DPB_INLINE_EXECUTE=true "            # execute on writePool thread, no worker queue
    "-DBATCHING_ENABLED=false"             # skip GigaPaxos RequestBatcher hop
)


# ── Synth-workload helpers ──────────────────────────────────────────────────


def make_workload_payload(txns=1, ops=1, write_size=100, autocommit=False):
    """Return a JSON payload string for POST /workload."""
    return json.dumps({
        "txns": txns, "ops": ops, "write_size": write_size,
        "autocommit": autocommit,
    })


def check_synth_ready(host, port, service=None):
    """Verify the synth-workload app responds on GET /health."""
    headers = {}
    if service:
        headers["XDN"] = service
    try:
        r = requests.get(f"http://{host}:{port}/health", headers=headers, timeout=15)
        if r.status_code == 200:
            return True
    except Exception:
        pass
    return False


def wait_for_synth_ready(host, port, service=None, timeout_sec=120):
    """Poll until synth-workload responds on /health."""
    import time
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        if check_synth_ready(host, port, service):
            print(f"   Synth-workload ready on {host}:{port}")
            return True
        time.sleep(5)
    print(f"   TIMEOUT: synth-workload not ready after {timeout_sec}s")
    return False


def ensure_synth_images(hosts):
    """Ensure the synth-workload Docker image is present on all given hosts."""
    for host in hosts:
        check = subprocess.run(
            ["ssh", "-o", "StrictHostKeyChecking=no", host,
             f"docker image inspect {SYNTH_IMAGE}"],
            capture_output=True,
        )
        if check.returncode == 0:
            print(f"   {SYNTH_IMAGE}: already on {host}")
            continue

        # Try to transfer from first AR host
        src = AR_HOSTS[0]
        print(f"   {SYNTH_IMAGE}: transferring {src} -> {host} ...")
        ret = os.system(
            f"ssh {src} 'docker save {SYNTH_IMAGE}'"
            f" | ssh {host} 'docker load'"
        )
        if ret != 0:
            print(f"   ERROR: failed to transfer {SYNTH_IMAGE} to {host}")
            sys.exit(1)
        print(f"   {SYNTH_IMAGE}: done on {host}")
