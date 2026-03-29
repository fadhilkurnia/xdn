"""
probe_xdn_timing.py — Start an XDN cluster with timing instrumentation enabled,
deploy WordPress, and probe the per-request latency breakdown.

Automatically adds -DXDN_TIMING_HEADERS=true to the JVM args so the cluster
emits X-XDN-Forward, X-XDN-Timing, and X-XDN-Pipeline response headers.

Run from the eval/ directory:
    python3 probe_xdn_timing.py
    python3 probe_xdn_timing.py --n 50 --host 10.10.1.1
    python3 probe_xdn_timing.py --skip-setup   # reuse running cluster
"""

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

import run_load_pb_wordpress_common as _wpmod
from run_load_pb_wordpress_common import (
    ADMIN_PASSWORD,
    ADMIN_USER,
    AR_HOSTS,
    CONTROL_PLANE_HOST,
    GP_CONFIG,
    GP_JVM_ARGS,
    HTTP_PROXY_PORT,
    IMAGE_SOURCE_HOST,
    REQUIRED_IMAGES,
    SCREEN_LOG,
    SCREEN_SESSION,
    SERVICE_NAME,
    TIMEOUT_PORT_SEC,
    TIMEOUT_WP_SEC,
    WORDPRESS_YAML,
    XDN_BINARY,
    check_rest_api,
    clear_xdn_cluster,
    create_post,
    disable_wp_cron,
    enable_rest_auth,
    install_wordpress,
    wait_for_port,
    wait_for_service,
)
from run_load_pb_wordpress_reflex import (
    detect_primary_via_docker,
    ensure_docker_images_on_rc,
    _xmlrpc_editpost_payload,
)

SEED_POST_COUNT = 20  # small count, enough for probing


def start_cluster_with_timing():
    """Start XDN cluster with XDN_TIMING_HEADERS=true."""
    jvm_args = f"-DXDN_TIMING_HEADERS=true {GP_JVM_ARGS}"
    os.makedirs("screen_logs", exist_ok=True)
    os.system(f"rm -f {SCREEN_LOG}")
    cmd = (
        f"screen -L -Logfile {SCREEN_LOG} -S {SCREEN_SESSION} -d -m bash -c "
        f"'../bin/gpServer.sh -DgigapaxosConfig={GP_CONFIG} {jvm_args} start all; exec bash'"
    )
    print(f"   {cmd}")
    ret = os.system(cmd)
    assert ret == 0, "Failed to start cluster"
    print(f"   Screen log: {SCREEN_LOG}")
    time.sleep(20)


def setup_cluster():
    """Full cluster setup: clear, start, deploy, install, seed."""
    print("\n[1] Clearing cluster ...")
    clear_xdn_cluster()

    print("\n[2] Starting cluster with XDN_TIMING_HEADERS=true ...")
    start_cluster_with_timing()

    print("\n[3] Waiting for Active Replicas ...")
    for host in AR_HOSTS:
        if not wait_for_port(host, HTTP_PROXY_PORT, TIMEOUT_PORT_SEC):
            print(f"ERROR: AR at {host}:{HTTP_PROXY_PORT} not ready")
            sys.exit(1)
        print(f"   {host}:{HTTP_PROXY_PORT} ready")

    print("\n[4] Ensuring Docker images on RC ...")
    ensure_docker_images_on_rc()

    print("\n[5] Deploying WordPress ...")
    deploy_cmd = (
        f"XDN_CONTROL_PLANE={CONTROL_PLANE_HOST} {XDN_BINARY} "
        f"launch {SERVICE_NAME} --file={WORDPRESS_YAML}"
    )
    for attempt in range(5):
        result = subprocess.run(deploy_cmd, capture_output=True, text=True, shell=True)
        if result.returncode == 0 and "Error" not in result.stdout:
            break
        print(f"   Attempt {attempt+1} failed, retrying ...")
        time.sleep(10)
    else:
        print("ERROR: Failed to deploy service")
        sys.exit(1)

    print("\n[6] Waiting for service readiness ...")
    ok, _ = wait_for_service(AR_HOSTS, HTTP_PROXY_PORT, SERVICE_NAME, TIMEOUT_WP_SEC)
    if not ok:
        print("ERROR: Service not ready")
        sys.exit(1)

    print("\n[7] Waiting for initContainerSync ...")
    deadline = time.time() + 600
    while time.time() < deadline:
        try:
            with open(SCREEN_LOG, "r") as f:
                if "Completed initContainerSync" in f.read():
                    print("   initContainerSync completed")
                    break
        except FileNotFoundError:
            pass
        elapsed = int(time.time() - (deadline - 600))
        if elapsed % 30 == 0:
            print(f"   ... {elapsed}s elapsed ...")
        time.sleep(5)
    else:
        print("   WARNING: initContainerSync not detected after 600s")
    time.sleep(10)

    print("\n[8] Detecting primary ...")
    primary = detect_primary_via_docker(AR_HOSTS)
    if not primary:
        primary = AR_HOSTS[0]
    print(f"   Primary: {primary}")

    print("\n[9] Installing WordPress ...")
    if not install_wordpress(primary, HTTP_PROXY_PORT, SERVICE_NAME):
        print("ERROR: WordPress install failed")
        sys.exit(1)

    print("\n[10] Enabling REST API auth ...")
    if not enable_rest_auth(primary):
        print("ERROR: REST API auth failed")
        sys.exit(1)

    disable_wp_cron(primary)

    if not check_rest_api(primary, HTTP_PROXY_PORT, SERVICE_NAME):
        print("ERROR: REST API not available")
        sys.exit(1)

    print(f"\n[11] Seeding {SEED_POST_COUNT} posts ...")
    import requests as http_requests
    headers = {"Content-Type": "application/json", "XDN": SERVICE_NAME}
    for i in range(SEED_POST_COUNT):
        try:
            http_requests.post(
                f"http://{primary}:{HTTP_PROXY_PORT}/api/todo/tasks",
                headers=headers,
                data=f'{{"item":"warmup-{i}"}}',
                timeout=10,
            )
        except Exception:
            pass

    seed_ids = []
    for i in range(SEED_POST_COUNT):
        try:
            pid = create_post(primary, HTTP_PROXY_PORT, SERVICE_NAME,
                              f"Probe Post {i+1}", f"Content {i+1}.")
            if pid:
                seed_ids.append(pid)
        except Exception:
            pass
    print(f"   Seeded {len(seed_ids)}/{SEED_POST_COUNT} posts")
    return primary, seed_ids


def probe_timing(host, port, service, seed_ids, n_requests):
    """Send editPost requests and capture timing headers."""
    import urllib.request

    print(f"\nProbing {n_requests} requests to {host}:{port} service={service}")
    print("=" * 100)
    print(f"{'#':>4}  {'container':>10}  {'proxy':>10}  {'copyreq':>10}  "
          f"{'exec':>10}  {'fwd':>10}  {'callback':>10}  {'qdelay':>10}  {'total':>10}")
    print("-" * 100)

    import re
    totals = {"container": [], "proxy": [], "exec": [], "fwd": [], "callback": [], "qdelay": []}

    for i in range(1, n_requests + 1):
        post_id = seed_ids[(i - 1) % len(seed_ids)] if seed_ids else 1
        payload = _xmlrpc_editpost_payload(post_id, ADMIN_PASSWORD)

        req = urllib.request.Request(
            f"http://{host}:{port}/xmlrpc.php",
            data=payload.encode(),
            headers={"Content-Type": "text/xml", "XDN": service},
            method="POST",
        )

        t0 = time.time()
        try:
            resp = urllib.request.urlopen(req, timeout=30)
            body = resp.read().decode("utf-8", errors="replace")
            total_ms = (time.time() - t0) * 1000
        except Exception as e:
            print(f"{i:>4}  ERROR: {e}")
            continue

        # Verify response is a successful editPost, not a fault
        if "faultString" in body:
            import re as _re
            fault = _re.search(r'<faultString><value><string>(.*?)</string>', body)
            fault_msg = fault.group(1) if fault else "unknown"
            print(f"{i:>4}  FAULT: {fault_msg}  (total={total_ms:.1f}ms)")
            continue
        if "<boolean>1</boolean>" not in body:
            print(f"{i:>4}  UNEXPECTED: {body[:100]}  (total={total_ms:.1f}ms)")
            continue

        fwd_hdr = resp.headers.get("X-XDN-Forward", "")
        tmg_hdr = resp.headers.get("X-XDN-Timing", "")
        pip_hdr = resp.headers.get("X-XDN-Pipeline", "")

        def extract(hdr, key):
            m = re.search(rf'{key}=([0-9.]+)', hdr)
            return float(m.group(1)) if m else None

        container = extract(fwd_hdr, "container")
        proxy = extract(fwd_hdr, "proxy")
        copyreq = extract(fwd_hdr, "copyreq")
        exec_t = extract(tmg_hdr, "exec")
        fwd_t = extract(tmg_hdr, "fwd")
        callback = extract(pip_hdr, "callback")
        qdelay = extract(pip_hdr, "qdelay")

        def fmt(v):
            return f"{v:>8.2f}ms" if v is not None else f"{'N/A':>10}"

        print(f"{i:>4}  {fmt(container)}  {fmt(proxy)}  {fmt(copyreq)}  "
              f"{fmt(exec_t)}  {fmt(fwd_t)}  {fmt(callback)}  {fmt(qdelay)}  "
              f"{total_ms:>8.2f}ms")

        for key, val in [("container", container), ("proxy", proxy),
                         ("exec", exec_t), ("fwd", fwd_t),
                         ("callback", callback), ("qdelay", qdelay)]:
            if val is not None:
                totals[key].append(val)

    print("=" * 100)
    print("\nAverages:")
    for key, vals in totals.items():
        if vals:
            avg = sum(vals) / len(vals)
            print(f"  {key:>12}: {avg:.2f}ms  (n={len(vals)})")

    print(f"\nLegend:")
    print(f"  container = time inside httpForwarderClient.execute() (WordPress+MySQL processing)")
    print(f"  proxy     = total forwarding overhead (validation + copy + container + store)")
    print(f"  copyreq   = time to copy HTTP request before forwarding")
    print(f"  exec      = total XdnGigapaxosApp.execute() time")
    print(f"  fwd       = forward-to-container time within execute()")
    print(f"  callback  = time from HTTP receive to PB callback (includes exec + PBM pipeline)")
    print(f"  qdelay    = time waiting in Netty EventLoop queue before writing response")
    print(f"  total     = end-to-end time measured by this probe script")

    # Parse PBM_TIMING from screen log
    try:
        with open(SCREEN_LOG, "r") as f:
            pbm_lines = [l.strip() for l in f if l.startswith("PBM_TIMING:")]
        if pbm_lines:
            print(f"\n{'='*100}")
            print(f"PBM Pipeline Breakdown (from screen log, last {min(n_requests, len(pbm_lines))} entries):")
            print(f"{'='*100}")
            for line in pbm_lines[-n_requests:]:
                print(f"  {line}")

            # Parse and average
            import re as _re2
            pbm_keys = ["queueWait", "exec", "captureWait", "capture", "propose", "total"]
            pbm_totals = {k: [] for k in pbm_keys}
            for line in pbm_lines[-n_requests:]:
                for key in pbm_keys:
                    m = _re2.search(rf'{key}=([0-9.]+)', line)
                    if m:
                        pbm_totals[key].append(float(m.group(1)))

            print(f"\nPBM Averages:")
            for key in pbm_keys:
                vals = pbm_totals[key]
                if vals:
                    print(f"  {key:>14}: {sum(vals)/len(vals):.2f}ms  (n={len(vals)})")

            print(f"\nPBM Legend:")
            print(f"  queueWait   = time waiting in batch worker queue")
            print(f"  exec        = time in execute() (WordPress forwarding)")
            print(f"  captureWait = time waiting for capture thread to pick up from doneQueue")
            print(f"  capture     = captureStateDiff() duration")
            print(f"  propose     = Paxos propose to commit callback duration")
            print(f"  total       = PBM entry to Paxos commit callback")
    except FileNotFoundError:
        pass


def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--host", type=str, default=None,
                   help="Target AR host (default: auto-detect primary)")
    p.add_argument("--n", type=int, default=20,
                   help="Number of probe requests (default: 20)")
    p.add_argument("--skip-setup", action="store_true",
                   help="Skip cluster setup (assume already running with XDN_TIMING_HEADERS=true)")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()

    if args.skip_setup:
        print("Skipping setup, using existing cluster ...")
        primary = args.host
        if not primary:
            primary = detect_primary_via_docker(AR_HOSTS)
            if not primary:
                primary = AR_HOSTS[0]
        print(f"Primary: {primary}")
        seed_ids = list(range(1, SEED_POST_COUNT + 1))
    else:
        primary, seed_ids = setup_cluster()

    if args.host:
        primary = args.host

    if not seed_ids:
        seed_ids = list(range(1, SEED_POST_COUNT + 1))

    probe_timing(primary, HTTP_PROXY_PORT, SERVICE_NAME, seed_ids, args.n)
