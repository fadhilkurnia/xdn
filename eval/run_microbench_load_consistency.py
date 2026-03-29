#!/usr/bin/env python3
"""
run_microbench_load_consistency.py — Load-latency curves across consistency models.

Measures throughput and avg latency at varying offered load for each consistency
model, producing data for the "Performance benefits of flexible consistency"
figure. Uses the TodoApp service on a 3-node XDN cluster with open-loop Poisson
clients.

Three read ratios are tested: 0% (write-only), 80% (mixed), 100% (read-only).

Run from the eval/ directory:
    python3 run_microbench_load_consistency.py
    python3 run_microbench_load_consistency.py --consistency linearizability,eventual
    python3 run_microbench_load_consistency.py --read-ratios 0,80 --rates 250,500,1000
    python3 run_microbench_load_consistency.py --local   # single-machine mode (no SSH)

Output CSV: eval/results/microbench_load_consistency_<ts>/eval_consistency_load_latency.csv
"""

import argparse
import csv
import logging
import os
import shutil
import socket
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from utils import replace_placeholder

log = logging.getLogger("load-consistency")

# ==============================================================================
# Constants
# ==============================================================================

# Defaults for cloudlab mode (overridden by --local)
CONTROL_PLANE_HOST = "10.10.1.4"
AR_HOSTS = ["10.10.1.1", "10.10.1.2", "10.10.1.3"]
HTTP_PROXY_PORT = 2300
XDN_BINARY = "../bin/xdn"
GP_CONFIG_BASE = "../conf/gigapaxos.xdn.3way.cloudlab.properties"

# Local mode constants
LOCAL_CONTROL_PLANE_HOST = "127.0.0.1"
LOCAL_AR_HOSTS = ["127.0.0.1"]
LOCAL_HTTP_PROXY_PORTS = [2300, 2301, 2302]  # one per AR
SERVICE_PROP_TEMPLATE = "static/xdn_consistency_template.yaml"
SERVICE_WRITE_ENDPOINT = "/api/todo/tasks"
SERVICE_READ_ENDPOINT = "/api/todo/tasks/warmup-8"
SCREEN_SESSION = "xdn_load_cons"
TODO_IMAGE = "fadhilkurnia/xdn-todo"

ALL_CONSISTENCY_MODELS = [
    "linearizability",
    "sequential",
    "causal",
    "pram",
    "monotonic_reads",
    "writes_follow_reads",
    "read_your_writes",
    "monotonic_writes",
    "eventual",
]

LEADER_CONSISTENCY_MODELS = {"linearizability", "sequential"}

CONSISTENCY_SHORT_NAME = {
    "linearizability": "lin",
    "sequential":      "seq",
    "causal":          "cau",
    "pram":            "prm",
    "monotonic_reads": "mrd",
    "writes_follow_reads": "wfr",
    "read_your_writes": "ryw",
    "monotonic_writes": "mwr",
    "eventual":        "evt",
}

DEFAULT_RATES = [250, 500, 750, 1000, 1250, 1500]
DEFAULT_READ_RATIOS = [0, 80, 100]
LOAD_DURATION_SEC = 60
WARMUP_DURATION_SEC = 10
WARMUP_RATE = 100
AVG_LATENCY_THRESHOLD_MS = 100.0

GO_CLIENT = Path(__file__).resolve().parent / "get_latency_at_rate.go"
results_base = Path(__file__).resolve().parent / "results"

# Global flag set by --local CLI argument
LOCAL_MODE = False

CSV_FIELDNAMES = [
    "consistency", "read_ratio", "target_rate_rps",
    "achieved_rate_rps", "throughput_rps",
    "avg_ms", "p50_ms", "p90_ms", "p95_ms", "p99_ms",
]


# ==============================================================================
# Cluster management
# ==============================================================================


def clear_xdn_cluster_remote():
    log.info("  Resetting the remote cluster ...")
    for i in range(3):
        host = f"10.10.1.{i+1}"
        os.system(f"ssh {host} sudo pkill -f gigapaxos 2>/dev/null || true")
        os.system(f"ssh {host} sudo pkill -f ReconfigurableNode 2>/dev/null || true")
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
    os.system(f"ssh {CONTROL_PLANE_HOST} sudo pkill -f gigapaxos 2>/dev/null || true")
    os.system(f"ssh {CONTROL_PLANE_HOST} sudo pkill -f ReconfigurableNode 2>/dev/null || true")
    os.system(f"ssh {CONTROL_PLANE_HOST} sudo fuser -k 3000/tcp 2>/dev/null || true")
    os.system(f"ssh {CONTROL_PLANE_HOST} sudo rm -rf /tmp/gigapaxos")
    log.info("  Remote cluster reset complete.")


def clear_xdn_cluster_local():
    log.info("  Resetting the local cluster ...")
    os.system("sudo pkill -f gigapaxos 2>/dev/null || true")
    os.system("sudo pkill -f ReconfigurableNode 2>/dev/null || true")
    for port in [2000, 2001, 2002, 2300, 2301, 2302, 3000]:
        os.system(f"sudo fuser -k {port}/tcp 2>/dev/null || true")
    os.system(
        "sudo df | grep xdn/state/fuselog"
        " | awk '{print $6}' | xargs -r sudo umount -l 2>/dev/null || true"
    )
    os.system("sudo rm -rf /tmp/gigapaxos /tmp/xdn /dev/shm/xdn")
    containers = subprocess.run(
        ["docker", "ps", "-a", "-q"], capture_output=True, text=True
    ).stdout.strip()
    if containers:
        os.system(f"docker stop {containers} && docker rm {containers}")
    os.system("docker network prune --force > /dev/null 2>&1")
    log.info("  Local cluster reset complete.")


def clear_xdn_cluster():
    if LOCAL_MODE:
        clear_xdn_cluster_local()
    else:
        clear_xdn_cluster_remote()


# Client-centric models use BayouReplicaCoordinator which requires
# TimestampedRequest/TimestampedResponse. XdnHttpRequestBatch does not
# implement these interfaces, so HTTP frontend batching must be disabled
# for these models (individual XdnHttpRequest does implement them).
CLIENT_CENTRIC_MODELS = {
    "monotonic_reads", "writes_follow_reads",
    "read_your_writes", "monotonic_writes",
}


def generate_config(base_config, target_config, disable_batching=False,
                    localize=False):
    """Generate a GigaPaxos config file from a base config.

    If localize=True, rewrites active.* and reconfigurator.* lines so that
    all nodes bind to 127.0.0.1 with distinct ports (AR0=:2000, AR1=:2001,
    AR2=:2002, RC0=:3000), keeping every other setting from the base config.
    """
    import re

    with open(base_config, "r", encoding="utf-8") as f:
        lines = f.readlines()

    if localize:
        out_lines = []
        # Strip node definitions; keep everything else as-is
        for line in lines:
            stripped = line.strip()
            if re.match(r"^(active|reconfigurator)\.", stripped):
                continue
            out_lines.append(line)

        # Append local node definitions
        out_lines.append("\n# --- local mode (auto-generated) ---\n")
        for i in range(3):
            out_lines.append(f"active.AR{i}=127.0.0.1:{2000 + i}\n")
        # Preserve geolocations from the base config (order matches AR0-2)
        geolocations = []
        for line in lines:
            m = re.match(r"^active\.AR(\d+)\.geolocation\s*=\s*(.*)", line.strip())
            if m:
                geolocations.append((int(m.group(1)), m.group(2)))
        for idx, geo in sorted(geolocations):
            out_lines.append(f"active.AR{idx}.geolocation={geo}\n")
        out_lines.append("reconfigurator.RC0=127.0.0.1:3000\n")

        with open(target_config, "w", encoding="utf-8") as f:
            f.writelines(out_lines)
    else:
        shutil.copy2(base_config, target_config)

    if disable_batching:
        with open(target_config, "a") as f:
            f.write("\nHTTP_AR_FRONTEND_BATCH_ENABLED=false\n")


def wait_for_port(host, port, timeout_sec=60):
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            s = socket.create_connection((host, port), timeout=2)
            s.close()
            return True
        except OSError:
            time.sleep(2)
    return False


def wait_for_service(host, port, service_name, timeout_sec=120):
    import urllib.request
    from urllib.error import HTTPError
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            url = f"http://{host}:{port}/api/todo/tasks"
            req = urllib.request.Request(url, headers={"XDN": service_name})
            r = urllib.request.urlopen(req, timeout=5)
            if r.status < 500:
                return True
        except (HTTPError, Exception):
            pass
        time.sleep(2)
    return False


def detect_leader(service_name, retries=15):
    """Detect the leader replica. Returns (host, port) tuple."""
    import json
    import urllib.request
    for attempt in range(1, retries + 1):
        if LOCAL_MODE:
            endpoints = [("127.0.0.1", port) for port in LOCAL_HTTP_PROXY_PORTS]
        else:
            endpoints = [(host, HTTP_PROXY_PORT) for host in AR_HOSTS]

        for host, port in endpoints:
            body = b""
            try:
                url = f"http://{host}:{port}/api/v2/services/{service_name}/replica/info"
                req = urllib.request.Request(url, headers={"XDN": service_name})
                r = urllib.request.urlopen(req, timeout=5)
                body = r.read()
                data = json.loads(body)
                role = data.get("role", "").lower()
                if role == "leader":
                    return (host, port)
                log.info("    %s:%d role=%s (attempt %d/%d)",
                         host, port, role, attempt, retries)
            except Exception as e:
                log.info("    %s:%d error=%s body=%s (attempt %d/%d)",
                         host, port, e, body[:200], attempt, retries)
        time.sleep(2)
    queried = LOCAL_HTTP_PROXY_PORTS if LOCAL_MODE else AR_HOSTS
    raise RuntimeError(
        f"Failed to detect leader for '{service_name}' after {retries} attempts. "
        f"Queried: {queried}"
    )


# ==============================================================================
# Consistency protocol verification
# ==============================================================================

# Expected coordinator class (SimpleName) for each consistency model.
EXPECTED_PROTOCOL = {
    "linearizability": "PaxosReplicaCoordinator",
    "sequential":      "AwReplicaCoordinator",
    "causal":          "CausalReplicaCoordinator",
    "pram":            "PramReplicaCoordinator",
    "monotonic_reads":      "BayouReplicaCoordinator",
    "writes_follow_reads":  "BayouReplicaCoordinator",
    "read_your_writes":     "BayouReplicaCoordinator",
    "monotonic_writes":     "BayouReplicaCoordinator",
    "eventual":        "LazyReplicaCoordinator",
}


def verify_consistency_protocol(service_name, expected_consistency):
    """Query replica/info on all ARs and verify offered consistency and protocol.

    Aborts if any replica reports an unexpected consistency model or coordinator
    class (e.g., fallback to a stronger consistency).
    """
    import json
    import urllib.request

    expected_proto = EXPECTED_PROTOCOL[expected_consistency]
    log.info("    Verifying consistency protocol on all replicas ...")
    log.info("    Expected: consistency=%s protocol=%s", expected_consistency, expected_proto)

    if LOCAL_MODE:
        endpoints = [("127.0.0.1", port) for port in LOCAL_HTTP_PROXY_PORTS]
    else:
        endpoints = [(host, HTTP_PROXY_PORT) for host in AR_HOSTS]

    for host, port in endpoints:
        url = f"http://{host}:{port}/api/v2/services/{service_name}/replica/info"
        try:
            req = urllib.request.Request(url, headers={"XDN": service_name}, method="GET")
            with urllib.request.urlopen(req, timeout=10) as resp:
                info = json.load(resp)
        except Exception as e:
            log.warning("    Could not query %s:%d: %s", host, port, e)
            continue

        offered = str(info.get("consistency", "")).lower()
        requested = str(info.get("requestedConsistency", "")).lower()
        protocol = str(info.get("protocol", ""))
        role = str(info.get("role", ""))

        log.info("    %s:%d: offered=%s requested=%s protocol=%s role=%s",
                 host, port, offered, requested, protocol, role)

        endpoint = f"{host}:{port}"
        if offered and offered != "?" and offered != expected_consistency:
            raise RuntimeError(
                f"Consistency mismatch on {endpoint}: expected '{expected_consistency}' "
                f"but replica reports offered='{offered}'."
            )

        if requested and requested != "?" and requested != expected_consistency:
            raise RuntimeError(
                f"Requested consistency mismatch on {endpoint}: expected "
                f"'{expected_consistency}' but replica reports "
                f"requestedConsistency='{requested}'."
            )

        if protocol and protocol != "?" and protocol != expected_proto:
            raise RuntimeError(
                f"Protocol mismatch on {endpoint}: expected '{expected_proto}' "
                f"but replica reports protocol='{protocol}'. "
                f"The coordinator may have fallen back unexpectedly."
            )

    log.info("    Verified: all replicas running '%s' with protocol '%s'",
             expected_consistency, expected_proto)


def prepare_service_yaml(consistency, service_yaml):
    service_name = CONSISTENCY_SHORT_NAME[consistency]
    shutil.copy2(SERVICE_PROP_TEMPLATE, service_yaml)
    replace_placeholder(service_yaml, "___SERVICE_NAME___", service_name)
    replace_placeholder(service_yaml, "___CONSISTENCY_MODEL___", consistency)
    if consistency == "eventual":
        with open(service_yaml, "a") as f:
            f.write("\n  - path_prefix: \"/\"\n")
            f.write("    methods: \"PUT,POST,DELETE\"\n")
            f.write("    behavior: monotonic\n")


# ==============================================================================
# Load generator
# ==============================================================================


def run_load_client(target_host, service_name, rate, duration_sec, read_ratio,
                    output_path, target_port=None, enable_cookies=False):
    """Run get_latency_at_rate.go with optional read-ratio."""
    port = target_port if target_port is not None else HTTP_PROXY_PORT
    write_url = f"http://{target_host}:{port}{SERVICE_WRITE_ENDPOINT}"
    read_url = f"http://{target_host}:{port}{SERVICE_READ_ENDPOINT}"
    payload = '{"item":"bench"}'

    env = os.environ.copy()
    # Ensure Go is on PATH (e.g., /usr/local/go/bin may not be in the default PATH)
    go_extra = "/usr/local/go/bin"
    if go_extra not in env.get("PATH", ""):
        env["PATH"] = go_extra + ":" + env.get("PATH", "")
    env["GO111MODULE"] = "off"
    env["GOWORK"] = "off"

    cmd = [
        "go", "run", str(GO_CLIENT),
        "-H", f"XDN: {service_name}",
        "-H", "Content-Type: application/json",
    ]
    if enable_cookies:
        cmd.append("-enable-cookies")
    if read_ratio > 0:
        cmd.extend(["-read-ratio", f"{read_ratio / 100.0:.2f}"])
        cmd.extend(["-read-url", read_url])
    cmd.extend([write_url, payload, str(duration_sec), str(rate)])

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as fh:
        result = subprocess.run(
            cmd, stdout=fh, stderr=subprocess.STDOUT,
            env=env, text=True, cwd=str(GO_CLIENT.parent),
        )
    if result.returncode != 0:
        log.warning("  Go client exited %d for rate=%d", result.returncode, rate)


def parse_go_output(output_path):
    metrics = {}
    try:
        with open(output_path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith("---"):
                    break
                if ":" not in line:
                    continue
                key, value = line.split(":", 1)
                try:
                    metrics[key.strip()] = float(value.strip())
                except ValueError:
                    pass
    except OSError:
        pass
    return {
        "achieved_rate_rps": metrics.get("actual_achieved_rate_rps", 0.0),
        "throughput_rps": metrics.get("actual_throughput_rps", 0.0),
        "avg_ms": metrics.get("average_latency_ms", 0.0),
        "p50_ms": metrics.get("median_latency_ms", 0.0),
        "p90_ms": metrics.get("p90_latency_ms", 0.0),
        "p95_ms": metrics.get("p95_latency_ms", 0.0),
        "p99_ms": metrics.get("p99_latency_ms", 0.0),
    }


# ==============================================================================
# Main measurement
# ==============================================================================


def run_consistency_sweep(consistency, read_ratios, rates, results_dir, csv_writer,
                          load_duration=LOAD_DURATION_SEC,
                          warmup_duration=WARMUP_DURATION_SEC,
                          extra_gp_properties=None):
    """Run all (read_ratio, rate) combinations for one consistency model."""
    service_name = CONSISTENCY_SHORT_NAME[consistency]
    screen_session = f"{SCREEN_SESSION}_{consistency}"
    screen_log = f"screen_logs/{screen_session}.log"
    config_file = f"static/gigapaxos.load.{consistency}.properties"
    service_yaml = f"static/xdn_load_{consistency}.yaml"

    try:
        # Steps 1-6: setup cluster and deploy service.
        # If the service fails to become ready, restart from step 1 (up to 3 times).
        max_setup_attempts = 3
        for setup_attempt in range(1, max_setup_attempts + 1):
            # (1) Clear cluster
            log.info("  [Step 1] Clearing cluster ... (setup attempt %d/%d)",
                     setup_attempt, max_setup_attempts)
            clear_xdn_cluster()

            # (2) Generate config.
            # Disable HTTP frontend batching for all consistency models so that
            # each request is handled individually — fairer comparison across models.
            log.info("  [Step 2] Generating config ...")
            generate_config(GP_CONFIG_BASE, config_file,
                            disable_batching=True,
                            localize=LOCAL_MODE)
            # Append any extra properties for Paxos tuning experiments
            if extra_gp_properties:
                with open(config_file, "a") as f:
                    for k, v in extra_gp_properties.items():
                        f.write(f"\n{k}={v}\n")

            # (3) Prepare YAML
            log.info("  [Step 3] Preparing service YAML ...")
            prepare_service_yaml(consistency, service_yaml)

            # (4) Start cluster
            log.info("  [Step 4] Starting cluster ...")
            os.makedirs("screen_logs", exist_ok=True)
            os.system(f"rm -f {screen_log}")
            os.system(f"screen -S {screen_session} -X quit > /dev/null 2>&1")
            cmd = (
                f"screen -L -Logfile {screen_log} -S {screen_session} -d -m bash -c "
                f"'../bin/gpServer.sh -DgigapaxosConfig={config_file} -DSYNC=true start all; exec bash'"
            )
            log.info("    %s", cmd)
            os.system(cmd)

            if LOCAL_MODE:
                for port in LOCAL_HTTP_PROXY_PORTS:
                    if not wait_for_port("127.0.0.1", port, timeout_sec=60):
                        raise RuntimeError(f"AR 127.0.0.1:{port} not reachable")
                    log.info("    127.0.0.1:%d ready", port)
            else:
                for host in AR_HOSTS:
                    if not wait_for_port(host, HTTP_PROXY_PORT, timeout_sec=60):
                        raise RuntimeError(f"AR {host}:{HTTP_PROXY_PORT} not reachable")
                    log.info("    %s:%d ready", host, HTTP_PROXY_PORT)

            # (5) Deploy service
            log.info("  [Step 5] Deploying service ...")
            cp_host = LOCAL_CONTROL_PLANE_HOST if LOCAL_MODE else CONTROL_PLANE_HOST
            deploy_cmd = (
                f"XDN_CONTROL_PLANE={cp_host} {XDN_BINARY} launch "
                f"{service_name} --file={service_yaml}"
            )
            for attempt in range(5):
                log.info("    %s", deploy_cmd)
                result = subprocess.run(deploy_cmd, capture_output=True, text=True, shell=True)
                if result.returncode == 0 and "Error" not in result.stdout:
                    break
                log.info("    Attempt %d failed, retrying ...", attempt + 1)
                time.sleep(10)
            else:
                raise RuntimeError("Failed to deploy service after 5 attempts")

            # (6) Wait for readiness
            log.info("  [Step 6] Waiting for service readiness ...")
            readiness_host = "127.0.0.1" if LOCAL_MODE else AR_HOSTS[0]
            readiness_port = LOCAL_HTTP_PROXY_PORTS[0] if LOCAL_MODE else HTTP_PROXY_PORT
            if wait_for_service(readiness_host, readiness_port, service_name):
                break
            log.warning("  Service not ready, restarting from step 1 ...")
        else:
            raise RuntimeError(
                f"Service not ready after {max_setup_attempts} full setup attempts")

        # (6b) Verify the correct consistency protocol is active
        log.info("  [Step 6b] Verifying consistency protocol ...")
        verify_consistency_protocol(service_name, consistency)

        # (7) Detect leader and determine default target node.
        #     For sequential/linearizability with writes, must use the leader.
        #     For read-only workloads, use AR_HOSTS[0] for all models to
        #     eliminate per-node variance (target node is overridden per
        #     read_ratio in the loop below).
        leader_host, leader_port = None, None
        if consistency in LEADER_CONSISTENCY_MODELS:
            leader_host, leader_port = detect_leader(service_name)
            log.info("  [Step 7] Leader detected at %s:%d", leader_host, leader_port)
            target_host, target_port = leader_host, leader_port
        else:
            if LOCAL_MODE:
                target_host, target_port = "127.0.0.1", LOCAL_HTTP_PROXY_PORTS[0]
            else:
                target_host, target_port = AR_HOSTS[0], HTTP_PROXY_PORT
            log.info("  [Step 7] Using %s:%d (non-leader model)", target_host, target_port)

        # (8) Seed some data for read requests
        log.info("  [Step 8] Seeding data for read requests ...")
        import requests as http_requests
        headers = {"Content-Type": "application/json", "XDN": service_name}
        for i in range(20):
            try:
                http_requests.post(
                    f"http://{target_host}:{target_port}{SERVICE_WRITE_ENDPOINT}",
                    headers=headers, data=f'{{"item":"warmup-{i}"}}', timeout=10,
                )
            except Exception:
                pass
        time.sleep(2)

        # (9) Run load sweep for each read ratio
        for read_ratio in read_ratios:
            # For read-only workloads (100% reads), sequential reads can go to
            # any replica — use AR_HOSTS[0] for all models to eliminate per-node
            # variance. For write-containing workloads, sequential/linearizability
            # must use the leader.
            if read_ratio == 100 and consistency in LEADER_CONSISTENCY_MODELS:
                if LOCAL_MODE:
                    target_host, target_port = "127.0.0.1", LOCAL_HTTP_PROXY_PORTS[0]
                else:
                    target_host, target_port = AR_HOSTS[0], HTTP_PROXY_PORT
                log.info("  >> read-only: targeting %s:%d (same as non-leader models)",
                         target_host, target_port)
            elif consistency in LEADER_CONSISTENCY_MODELS and leader_host:
                target_host, target_port = leader_host, leader_port

            log.info("")
            log.info("  >> [%s] read_ratio=%d%% — rates: %s", consistency, read_ratio, rates)

            # Brief warmup
            log.info("    Warming up at %d rps for %ds ...", WARMUP_RATE, warmup_duration)
            warmup_out = results_dir / f"{consistency}_r{read_ratio}_warmup.txt"
            use_cookies = consistency in CLIENT_CENTRIC_MODELS
            run_load_client(target_host, service_name, WARMUP_RATE, warmup_duration,
                            read_ratio, warmup_out, target_port=target_port,
                            enable_cookies=use_cookies)
            time.sleep(3)

            for rate in rates:
                log.info("    [%s r=%d%%] rate=%d rps (measuring %ds) ...",
                         consistency, read_ratio, rate, load_duration)

                output_file = results_dir / f"{consistency}_r{read_ratio}_rate{rate}.txt"
                run_load_client(target_host, service_name, rate, load_duration,
                                read_ratio, output_file, target_port=target_port,
                                enable_cookies=use_cookies)

                metrics = parse_go_output(output_file)
                row = {
                    "consistency": consistency,
                    "read_ratio": read_ratio,
                    "target_rate_rps": rate,
                    **metrics,
                }
                csv_writer.writerow(row)

                log.info("      achieved=%.1f tput=%.1f avg=%.1fms p50=%.1fms p95=%.1fms",
                         metrics["achieved_rate_rps"], metrics["throughput_rps"],
                         metrics["avg_ms"], metrics["p50_ms"], metrics["p95_ms"])

                if metrics["avg_ms"] > AVG_LATENCY_THRESHOLD_MS:
                    log.info("      Avg latency %.1fms > threshold %.1fms — stopping this sweep",
                             metrics["avg_ms"], AVG_LATENCY_THRESHOLD_MS)
                    break

                time.sleep(3)

    finally:
        log.info("  [Cleanup] Destroying service and clearing cluster ...")
        cp_host = LOCAL_CONTROL_PLANE_HOST if LOCAL_MODE else CONTROL_PLANE_HOST
        destroy_cmd = (
            f"yes yes | XDN_CONTROL_PLANE={cp_host} "
            f"{XDN_BINARY} service destroy {service_name}"
        )
        try:
            subprocess.run(destroy_cmd, capture_output=True, text=True, shell=True)
        except Exception:
            pass
        forceclear_cmd = f"../bin/gpServer.sh -DgigapaxosConfig={config_file} forceclear all > /dev/null 2>&1"
        os.system(forceclear_cmd)
        clear_xdn_cluster()
        for f in [config_file, service_yaml]:
            try:
                os.remove(f)
            except OSError:
                pass
        time.sleep(5)


# ==============================================================================
# CLI and main
# ==============================================================================


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--consistency", type=str, default=None,
                        help="Comma-separated consistency models (default: all 9)")
    parser.add_argument("--read-ratios", type=str, default=",".join(str(r) for r in DEFAULT_READ_RATIOS),
                        help="Comma-separated read ratios in %% (default: 0,80,100)")
    parser.add_argument("--rates", type=str, default=",".join(str(r) for r in DEFAULT_RATES),
                        help="Comma-separated offered rates (default: 250,500,750,1000,1250,1500)")
    parser.add_argument("--duration", type=int, default=LOAD_DURATION_SEC,
                        help="Duration per rate point in seconds (default: 60)")
    parser.add_argument("--warmup-duration", type=int, default=WARMUP_DURATION_SEC,
                        help=f"Warmup duration per read-ratio sweep in seconds (default: {WARMUP_DURATION_SEC})")
    parser.add_argument("--local", action="store_true",
                        help="Run on a single local machine (no SSH). Rewrites the cloudlab "
                             "base config to bind all nodes to 127.0.0.1 with distinct ports, "
                             "keeping all other settings (NIO size, pool sizes, etc.) intact.")
    args = parser.parse_args()

    # Apply local mode
    global LOCAL_MODE
    if args.local:
        LOCAL_MODE = True

    # Parse arguments
    if args.consistency:
        models = [m.strip() for m in args.consistency.split(",")]
        for m in models:
            if m not in ALL_CONSISTENCY_MODELS:
                print(f"Unknown model: {m}. Valid: {ALL_CONSISTENCY_MODELS}", file=sys.stderr)
                sys.exit(1)
    else:
        models = ALL_CONSISTENCY_MODELS

    read_ratios = [int(r.strip()) for r in args.read_ratios.split(",")]
    rates = [int(r.strip()) for r in args.rates.split(",")]
    load_duration = args.duration
    warmup_duration = args.warmup_duration

    # Output directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_dir = results_base / f"microbench_load_consistency_{timestamp}"
    results_dir.mkdir(parents=True, exist_ok=True)
    csv_path = results_dir / "eval_consistency_load_latency.csv"

    # Logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(results_dir / "run.log"),
        ],
    )

    total_experiments = len(models) * len(read_ratios) * len(rates)

    # Plan
    log.info("=" * 72)
    log.info("Load-Latency Curves Across Consistency Models")
    log.info("=" * 72)
    log.info("  Mode        : %s", "local (single machine)" if LOCAL_MODE else "cloudlab (remote SSH)")
    log.info("  Config      : %s", GP_CONFIG_BASE)
    log.info("  Models      : %s", ", ".join(models))
    log.info("  Read ratios : %s%%", ", ".join(str(r) for r in read_ratios))
    log.info("  Rates       : %s req/s", ", ".join(str(r) for r in rates))
    log.info("  Duration    : %ds per rate point", load_duration)
    log.info("  Warmup      : %ds at %d rps", warmup_duration, WARMUP_RATE)
    log.info("  Max points  : %d (%d models x %d ratios x %d rates)",
             total_experiments, len(models), len(read_ratios), len(rates))
    log.info("  Results     : %s", results_dir)
    log.info("  CSV         : %s", csv_path)
    log.info("=" * 72)

    # Verify prerequisites
    ret = os.system(f"{XDN_BINARY} --help > /dev/null 2>&1")
    assert ret == 0, f"Cannot find {XDN_BINARY}"

    if LOCAL_MODE:
        # Check that the state diff recorder binary required by the base config
        # is present on this machine.
        recorder_binary = {
            "FUSELOG": "/usr/local/bin/fuselog",
            "FUSERUST": "/usr/local/bin/fuserust",
        }
        with open(GP_CONFIG_BASE, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip().startswith("XDN_PB_STATEDIFF_RECORDER_TYPE="):
                    recorder_type = line.strip().split("=", 1)[1]
                    binary = recorder_binary.get(recorder_type)
                    if binary and not os.path.isfile(binary):
                        log.error(
                            "Local mode requires '%s' recorder but binary not found at %s. "
                            "Build it with: ./bin/build_xdn_fuselog.sh",
                            recorder_type, binary,
                        )
                        sys.exit(1)
                    break

    # Ensure the Docker image is present on all nodes to avoid pulling from
    # Docker Hub on every run (which hits rate limits).
    if LOCAL_MODE:
        log.info("  Ensuring Docker image '%s' is present locally ...", TODO_IMAGE)
        check = subprocess.run(
            ["docker", "image", "inspect", TODO_IMAGE],
            capture_output=True,
        )
        if check.returncode != 0:
            log.info("    Pulling '%s' locally ...", TODO_IMAGE)
            pull = subprocess.run(
                ["docker", "pull", TODO_IMAGE],
                capture_output=True, text=True,
            )
            if pull.returncode != 0:
                log.error("    Failed to pull image: %s", pull.stderr.strip())
                sys.exit(1)
            log.info("    Pulled successfully")
        else:
            log.info("    Image already present locally")
    else:
        all_hosts = AR_HOSTS + [CONTROL_PLANE_HOST]
        log.info("  Ensuring Docker image '%s' is present on all nodes ...", TODO_IMAGE)
        for host in all_hosts:
            check = subprocess.run(
                ["ssh", host, "docker", "image", "inspect", TODO_IMAGE],
                capture_output=True,
            )
            if check.returncode != 0:
                log.info("    Pulling '%s' on %s ...", TODO_IMAGE, host)
                pull = subprocess.run(
                    ["ssh", host, "docker", "pull", TODO_IMAGE],
                    capture_output=True, text=True,
                )
                if pull.returncode != 0:
                    log.error("    Failed to pull image on %s: %s", host, pull.stderr.strip())
                    sys.exit(1)
                log.info("    Pulled successfully on %s", host)
            else:
                log.info("    Image already present on %s", host)

    # Open CSV
    csv_fh = open(csv_path, "w", newline="", encoding="utf-8")
    csv_writer = csv.DictWriter(csv_fh, fieldnames=CSV_FIELDNAMES)
    csv_writer.writeheader()
    csv_fh.flush()

    # Run measurements
    for i, consistency in enumerate(models, start=1):
        log.info("")
        log.info("=" * 72)
        log.info("[%d/%d] Consistency: %s", i, len(models), consistency)
        log.info("=" * 72)

        try:
            run_consistency_sweep(consistency, read_ratios, rates, results_dir, csv_writer, load_duration, warmup_duration)
            csv_fh.flush()
        except Exception:
            log.exception("[%d/%d] FAILED: %s", i, len(models), consistency)

    csv_fh.close()

    # Summary
    log.info("")
    log.info("=" * 72)
    log.info("[Done] Results saved to %s", csv_path)
    log.info("=" * 72)


if __name__ == "__main__":
    main()
