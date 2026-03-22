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

CONTROL_PLANE_HOST = "10.10.1.4"
AR_HOSTS = ["10.10.1.1", "10.10.1.2", "10.10.1.3"]
HTTP_PROXY_PORT = 2300
XDN_BINARY = "../bin/xdn"
GP_CONFIG_BASE = "../conf/gigapaxos.xdn.3way.cloudlab.properties"
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

CSV_FIELDNAMES = [
    "consistency", "read_ratio", "target_rate_rps",
    "achieved_rate_rps", "throughput_rps",
    "avg_ms", "p50_ms", "p90_ms", "p95_ms", "p99_ms",
]


# ==============================================================================
# Cluster management
# ==============================================================================


def clear_xdn_cluster():
    log.info("  Resetting the cluster ...")
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
    log.info("  Cluster reset complete.")


def generate_config(base_config, target_config):
    shutil.copy2(base_config, target_config)
    # Keep batching ENABLED for throughput measurements
    # (unlike the per-request microbenchmark which disables it)


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


def detect_leader(service_name, retries=5):
    import json
    for attempt in range(1, retries + 1):
        for host in AR_HOSTS:
            try:
                url = f"http://{host}:{HTTP_PROXY_PORT}/api/v2/services/{service_name}/replica/info"
                import urllib.request
                r = urllib.request.urlopen(url, timeout=5)
                data = json.loads(r.read())
                if data.get("role", "").lower() == "leader":
                    return host
            except Exception:
                pass
        time.sleep(2)
    return AR_HOSTS[0]  # fallback


def prepare_service_yaml(consistency, service_yaml):
    service_name = CONSISTENCY_SHORT_NAME[consistency]
    shutil.copy2(SERVICE_PROP_TEMPLATE, service_yaml)
    replace_placeholder(service_yaml, "___SERVICE_NAME___", service_name)
    replace_placeholder(service_yaml, "___CONSITENCY_MODEL___", consistency)
    if consistency == "eventual":
        with open(service_yaml, "a") as f:
            f.write("\n  - path_prefix: \"/\"\n")
            f.write("    methods: \"PUT,POST,DELETE\"\n")
            f.write("    behavior: monotonic\n")


# ==============================================================================
# Load generator
# ==============================================================================


def run_load_client(target_host, service_name, rate, duration_sec, read_ratio,
                    output_path):
    """Run get_latency_at_rate.go with optional read-ratio."""
    write_url = f"http://{target_host}:{HTTP_PROXY_PORT}{SERVICE_WRITE_ENDPOINT}"
    read_url = f"http://{target_host}:{HTTP_PROXY_PORT}{SERVICE_READ_ENDPOINT}"
    payload = '{"item":"bench"}'

    env = os.environ.copy()
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
# Main measurement
# ==============================================================================


def run_consistency_sweep(consistency, read_ratios, rates, results_dir, csv_writer,
                          load_duration=LOAD_DURATION_SEC):
    """Run all (read_ratio, rate) combinations for one consistency model."""
    service_name = CONSISTENCY_SHORT_NAME[consistency]
    screen_session = f"{SCREEN_SESSION}_{consistency}"
    screen_log = f"screen_logs/{screen_session}.log"
    config_file = f"static/gigapaxos.load.{consistency}.properties"
    service_yaml = f"static/xdn_load_{consistency}.yaml"

    try:
        # (1) Clear cluster
        log.info("  [Step 1] Clearing cluster ...")
        clear_xdn_cluster()

        # (2) Generate config (batching enabled for throughput)
        log.info("  [Step 2] Generating config ...")
        generate_config(GP_CONFIG_BASE, config_file)

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

        for host in AR_HOSTS:
            if not wait_for_port(host, HTTP_PROXY_PORT, timeout_sec=60):
                raise RuntimeError(f"AR {host}:{HTTP_PROXY_PORT} not reachable")
            log.info("    %s:%d ready", host, HTTP_PROXY_PORT)

        # (5) Deploy service
        log.info("  [Step 5] Deploying service ...")
        deploy_cmd = (
            f"XDN_CONTROL_PLANE={CONTROL_PLANE_HOST} {XDN_BINARY} launch "
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
        if not wait_for_service(AR_HOSTS[0], HTTP_PROXY_PORT, service_name):
            raise RuntimeError("Service not ready")

        # (7) Detect leader for linearizability/sequential
        if consistency in LEADER_CONSISTENCY_MODELS:
            target_host = detect_leader(service_name)
            log.info("  [Step 7] Leader detected at %s", target_host)
        else:
            target_host = AR_HOSTS[0]
            log.info("  [Step 7] Using %s (non-leader model)", target_host)

        # (8) Seed some data for read requests
        log.info("  [Step 8] Seeding data for read requests ...")
        import requests as http_requests
        headers = {"Content-Type": "application/json", "XDN": service_name}
        for i in range(20):
            try:
                http_requests.post(
                    f"http://{target_host}:{HTTP_PROXY_PORT}{SERVICE_WRITE_ENDPOINT}",
                    headers=headers, data=f'{{"item":"warmup-{i}"}}', timeout=10,
                )
            except Exception:
                pass
        time.sleep(2)

        # (9) Run load sweep for each read ratio
        for read_ratio in read_ratios:
            log.info("")
            log.info("  >> [%s] read_ratio=%d%% — rates: %s", consistency, read_ratio, rates)

            # Brief warmup
            log.info("    Warming up at %d rps for %ds ...", WARMUP_RATE, WARMUP_DURATION_SEC)
            warmup_out = results_dir / f"{consistency}_r{read_ratio}_warmup.txt"
            run_load_client(target_host, service_name, WARMUP_RATE, WARMUP_DURATION_SEC,
                            read_ratio, warmup_out)
            time.sleep(3)

            for rate in rates:
                log.info("    [%s r=%d%%] rate=%d rps (measuring %ds) ...",
                         consistency, read_ratio, rate, load_duration)

                output_file = results_dir / f"{consistency}_r{read_ratio}_rate{rate}.txt"
                run_load_client(target_host, service_name, rate, load_duration,
                                read_ratio, output_file)

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
        destroy_cmd = (
            f"yes yes | XDN_CONTROL_PLANE={CONTROL_PLANE_HOST} "
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
    args = parser.parse_args()

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
    log.info("  Models      : %s", ", ".join(models))
    log.info("  Read ratios : %s%%", ", ".join(str(r) for r in read_ratios))
    log.info("  Rates       : %s req/s", ", ".join(str(r) for r in rates))
    log.info("  Duration    : %ds per rate point", load_duration)
    log.info("  Warmup      : %ds at %d rps", WARMUP_DURATION_SEC, WARMUP_RATE)
    log.info("  Max points  : %d (%d models x %d ratios x %d rates)",
             total_experiments, len(models), len(read_ratios), len(rates))
    log.info("  Results     : %s", results_dir)
    log.info("  CSV         : %s", csv_path)
    log.info("=" * 72)

    # Verify prerequisites
    ret = os.system(f"{XDN_BINARY} --help > /dev/null 2>&1")
    assert ret == 0, f"Cannot find {XDN_BINARY}"

    # Ensure the Docker image is present on all nodes to avoid pulling from
    # Docker Hub on every run (which hits rate limits).
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
            run_consistency_sweep(consistency, read_ratios, rates, results_dir, csv_writer, load_duration)
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
