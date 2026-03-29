#!/usr/bin/env python3
"""
validate_linearizability_batching.py — Validate whether HTTP frontend batching
explains the high read-only throughput for linearizability.

Runs linearizability with 100% read-only workload at increasing rates, first
with HTTP_AR_FRONTEND_BATCH_ENABLED=true, then with it set to false. Compares
the throughput and latency to determine if batching is the key factor.

Run from the eval/ directory:
    python3 validate_linearizability_batching.py --local
    python3 validate_linearizability_batching.py --local --rates 500,2000,4000,8000,12000,16000
"""

import argparse
import csv
import logging
import os
import re
import shutil
import socket
import subprocess
import sys
import time
import json
import urllib.request
from datetime import datetime
from pathlib import Path

from utils import replace_placeholder

log = logging.getLogger("validate-batching")

# ==============================================================================
# Constants (same as run_microbench_load_consistency.py)
# ==============================================================================

CONTROL_PLANE_HOST = "10.10.1.4"
AR_HOSTS = ["10.10.1.1", "10.10.1.2", "10.10.1.3"]
HTTP_PROXY_PORT = 2300
XDN_BINARY = "../bin/xdn"
GP_CONFIG_BASE = "../conf/gigapaxos.xdn.3way.cloudlab.properties"

LOCAL_CONTROL_PLANE_HOST = "127.0.0.1"
LOCAL_AR_HOSTS = ["127.0.0.1"]
LOCAL_HTTP_PROXY_PORTS = [2300, 2301, 2302]

SERVICE_PROP_TEMPLATE = "static/xdn_consistency_template.yaml"
SERVICE_WRITE_ENDPOINT = "/api/todo/tasks"
SERVICE_READ_ENDPOINT = "/api/todo/tasks/warmup-8"
SCREEN_SESSION = "xdn_val_batch"
TODO_IMAGE = "fadhilkurnia/xdn-todo"
GO_CLIENT = Path(__file__).resolve().parent / "get_latency_at_rate.go"
results_base = Path(__file__).resolve().parent / "results"

LOCAL_MODE = False

DEFAULT_RATES = [500, 1000, 2000, 4000, 6000, 8000, 10000, 12000, 16000]
LOAD_DURATION_SEC = 10
WARMUP_DURATION_SEC = 3
WARMUP_RATE = 100

CSV_FIELDNAMES = [
    "batching", "target_rate_rps",
    "achieved_rate_rps", "throughput_rps",
    "avg_ms", "p50_ms", "p90_ms", "p95_ms", "p99_ms",
]


# ==============================================================================
# Cluster management (reused from main benchmark script)
# ==============================================================================


def clear_xdn_cluster():
    log.info("  Resetting cluster ...")
    if LOCAL_MODE:
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
    else:
        for i in range(3):
            host = f"10.10.1.{i+1}"
            os.system(f"ssh {host} sudo pkill -f gigapaxos 2>/dev/null || true")
            os.system(f"ssh {host} sudo pkill -f ReconfigurableNode 2>/dev/null || true")
            os.system(f"ssh {host} sudo fuser -k 2000/tcp 2>/dev/null || true")
            os.system(f"ssh {host} sudo fuser -k 2300/tcp 2>/dev/null || true")
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
    log.info("  Cluster reset complete.")


def generate_config(base_config, target_config, batching_enabled=True):
    """Generate config, optionally localizing and setting batching."""
    with open(base_config, "r", encoding="utf-8") as f:
        lines = f.readlines()

    if LOCAL_MODE:
        out_lines = []
        for line in lines:
            stripped = line.strip()
            if re.match(r"^(active|reconfigurator)\.", stripped):
                continue
            out_lines.append(line)
        out_lines.append("\n# --- local mode (auto-generated) ---\n")
        for i in range(3):
            out_lines.append(f"active.AR{i}=127.0.0.1:{2000 + i}\n")
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

    # Explicitly set batching
    with open(target_config, "a") as f:
        val = "true" if batching_enabled else "false"
        f.write(f"\nHTTP_AR_FRONTEND_BATCH_ENABLED={val}\n")


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
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            url = f"http://{host}:{port}/api/todo/tasks"
            req = urllib.request.Request(url, headers={"XDN": service_name})
            r = urllib.request.urlopen(req, timeout=5)
            if r.status < 500:
                return True
        except Exception:
            pass
        time.sleep(2)
    return False


def detect_leader(service_name, retries=15):
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
    raise RuntimeError(
        f"Failed to detect leader for '{service_name}' after {retries} attempts."
    )


# ==============================================================================
# Load generator (reused)
# ==============================================================================


def run_load_client(target_host, target_port, service_name, rate, duration_sec,
                    read_ratio, output_path):
    read_url = f"http://{target_host}:{target_port}{SERVICE_READ_ENDPOINT}"
    write_url = f"http://{target_host}:{target_port}{SERVICE_WRITE_ENDPOINT}"
    payload = '{"item":"bench"}'

    env = os.environ.copy()
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
# Single experiment run
# ==============================================================================


def run_experiment(batching_enabled, rates, results_dir, csv_writer,
                   load_duration, warmup_duration):
    """Run linearizability read-only sweep with given batching setting."""
    label = "batch_on" if batching_enabled else "batch_off"
    service_name = "lin"
    screen_session = f"{SCREEN_SESSION}_{label}"
    screen_log = f"screen_logs/{screen_session}.log"
    config_file = f"static/gigapaxos.val_batch.{label}.properties"
    service_yaml = f"static/xdn_val_batch_{label}.yaml"

    try:
        # (1) Clear
        log.info("  [%s] Clearing cluster ...", label)
        clear_xdn_cluster()

        # (2) Generate config
        log.info("  [%s] Generating config (batching=%s) ...", label, batching_enabled)
        generate_config(GP_CONFIG_BASE, config_file, batching_enabled=batching_enabled)

        # (3) Prepare service YAML
        shutil.copy2(SERVICE_PROP_TEMPLATE, service_yaml)
        replace_placeholder(service_yaml, "___SERVICE_NAME___", service_name)
        replace_placeholder(service_yaml, "___CONSISTENCY_MODEL___", "linearizability")

        # (4) Start cluster
        log.info("  [%s] Starting cluster ...", label)
        os.makedirs("screen_logs", exist_ok=True)
        os.system(f"rm -f {screen_log}")
        os.system(f"screen -S {screen_session} -X quit > /dev/null 2>&1")
        cmd = (
            f"screen -L -Logfile {screen_log} -S {screen_session} -d -m bash -c "
            f"'../bin/gpServer.sh -DgigapaxosConfig={config_file} -DSYNC=true start all; exec bash'"
        )
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

        # (5) Deploy
        log.info("  [%s] Deploying service ...", label)
        cp_host = LOCAL_CONTROL_PLANE_HOST if LOCAL_MODE else CONTROL_PLANE_HOST
        deploy_cmd = f"XDN_CONTROL_PLANE={cp_host} {XDN_BINARY} launch {service_name} --file={service_yaml}"
        for attempt in range(5):
            result = subprocess.run(deploy_cmd, capture_output=True, text=True, shell=True)
            if result.returncode == 0 and "Error" not in result.stdout:
                break
            log.info("    Attempt %d failed, retrying ...", attempt + 1)
            time.sleep(10)
        else:
            raise RuntimeError("Failed to deploy service after 5 attempts")

        # (6) Wait for readiness
        log.info("  [%s] Waiting for service readiness ...", label)
        readiness_host = "127.0.0.1" if LOCAL_MODE else AR_HOSTS[0]
        readiness_port = LOCAL_HTTP_PROXY_PORTS[0] if LOCAL_MODE else HTTP_PROXY_PORT
        if not wait_for_service(readiness_host, readiness_port, service_name):
            raise RuntimeError("Service not ready")

        # (7) Detect leader
        log.info("  [%s] Detecting leader ...", label)
        target_host, target_port = detect_leader(service_name)
        log.info("  [%s] Leader at %s:%d", label, target_host, target_port)

        # (8) Seed data
        log.info("  [%s] Seeding data ...", label)
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

        # (9) Run read-only sweep
        read_ratio = 100
        log.info("")
        log.info("  >> [%s] read_ratio=100%% — rates: %s", label, rates)

        # Warmup
        warmup_out = results_dir / f"{label}_warmup.txt"
        run_load_client(target_host, target_port, service_name, WARMUP_RATE,
                        warmup_duration, read_ratio, warmup_out)
        time.sleep(3)

        for rate in rates:
            log.info("    [%s] rate=%d rps (%ds) ...", label, rate, load_duration)
            output_file = results_dir / f"{label}_rate{rate}.txt"
            run_load_client(target_host, target_port, service_name, rate,
                            load_duration, read_ratio, output_file)

            metrics = parse_go_output(output_file)
            row = {
                "batching": label,
                "target_rate_rps": rate,
                **metrics,
            }
            csv_writer.writerow(row)

            log.info("      achieved=%.1f tput=%.1f avg=%.1fms p50=%.1fms p99=%.1fms",
                     metrics["achieved_rate_rps"], metrics["throughput_rps"],
                     metrics["avg_ms"], metrics["p50_ms"], metrics["p99_ms"])

            if metrics["avg_ms"] > 100.0:
                log.info("      Avg latency %.1fms > 100ms — stopping sweep", metrics["avg_ms"])
                break

            time.sleep(3)

    finally:
        log.info("  [%s] Cleanup ...", label)
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
# Main
# ==============================================================================


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rates", type=str,
                        default=",".join(str(r) for r in DEFAULT_RATES),
                        help="Comma-separated offered rates")
    parser.add_argument("--duration", type=int, default=LOAD_DURATION_SEC,
                        help="Duration per rate point in seconds (default: 10)")
    parser.add_argument("--warmup-duration", type=int, default=WARMUP_DURATION_SEC)
    parser.add_argument("--local", action="store_true",
                        help="Run on a single local machine (no SSH)")
    args = parser.parse_args()

    global LOCAL_MODE
    if args.local:
        LOCAL_MODE = True

    rates = [int(r.strip()) for r in args.rates.split(",")]
    load_duration = args.duration
    warmup_duration = args.warmup_duration

    # Output directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_dir = results_base / f"validate_lin_batching_{timestamp}"
    results_dir.mkdir(parents=True, exist_ok=True)
    csv_path = results_dir / "validate_lin_batching.csv"

    # Logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(results_dir / "run.log"),
        ],
    )

    log.info("=" * 72)
    log.info("Validate: Linearizability Read Throughput — Batching On vs Off")
    log.info("=" * 72)
    log.info("  Mode        : %s", "local" if LOCAL_MODE else "cloudlab")
    log.info("  Rates       : %s", rates)
    log.info("  Duration    : %ds per point", load_duration)
    log.info("  Read ratio  : 100%% (read-only)")
    log.info("  Results     : %s", results_dir)
    log.info("=" * 72)

    # Verify prerequisites
    ret = os.system(f"{XDN_BINARY} --help > /dev/null 2>&1")
    assert ret == 0, f"Cannot find {XDN_BINARY}"

    if LOCAL_MODE:
        check = subprocess.run(["docker", "image", "inspect", TODO_IMAGE],
                               capture_output=True)
        if check.returncode != 0:
            log.info("  Pulling '%s' locally ...", TODO_IMAGE)
            subprocess.run(["docker", "pull", TODO_IMAGE], check=True)

    # Open CSV
    csv_fh = open(csv_path, "w", newline="", encoding="utf-8")
    csv_writer = csv.DictWriter(csv_fh, fieldnames=CSV_FIELDNAMES)
    csv_writer.writeheader()
    csv_fh.flush()

    # Run both experiments
    for batching in [True, False]:
        label = "ENABLED" if batching else "DISABLED"
        log.info("")
        log.info("=" * 72)
        log.info("  Batching: %s", label)
        log.info("=" * 72)

        try:
            run_experiment(batching, rates, results_dir, csv_writer,
                           load_duration, warmup_duration)
            csv_fh.flush()
        except Exception:
            log.exception("FAILED: batching=%s", label)

    csv_fh.close()

    # Print summary
    log.info("")
    log.info("=" * 72)
    log.info("Results saved to %s", csv_path)
    log.info("=" * 72)


if __name__ == "__main__":
    main()
