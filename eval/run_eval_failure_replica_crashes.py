#!/usr/bin/env python3
"""
run_eval_failure_replica_crashes.py — Reproduce Figure 19a: non-leader replica failures.

Measures per-second throughput of SQLite-backed TodoApp (80% reads) under
three consistency models (linearizability, sequential, eventual) while
gradually crashing non-leader replicas at scheduled times.

Run from the eval/ directory:
    python3 run_eval_failure_replica_crashes.py
    python3 run_eval_failure_replica_crashes.py --consistency linearizability,eventual
    python3 run_eval_failure_replica_crashes.py --crash-times 60,120 --duration 180
    python3 run_eval_failure_replica_crashes.py --local

Output: eval/results/eval_failure_replica_crashes_<timestamp>/
Post-processing copies CSVs to reflex-paper/data/ and runs combine + plot scripts.
"""

import argparse
import logging
import os
import re
import shutil
import socket
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from utils import replace_placeholder

log = logging.getLogger("failure-replica-crashes")

# ==============================================================================
# Constants
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
SCREEN_SESSION = "xdn_failure"
TODO_IMAGE = "fadhilkurnia/xdn-todo"

CONSISTENCY_MODELS = ["linearizability", "sequential", "eventual"]

CONSISTENCY_SHORT_NAME = {
    "linearizability": "lin",
    "sequential":      "seq",
    "eventual":        "evt",
}

OUTPUT_FILENAME = {
    "linearizability": "eval_failure_linearizable.csv",
    "sequential":      "eval_failure_sequential.csv",
    "eventual":        "eval_failure_eventual.csv",
}

DEFAULT_RATE = 1000
DEFAULT_DURATION = 180
DEFAULT_CRASH_TIMES = [60, 120]
DEFAULT_READ_RATIO = 80
WARMUP_DURATION_SEC = 10
WARMUP_RATE = 500

GO_CLIENT = Path(__file__).resolve().parent / "get_latency_at_rate.go"
RESULTS_BASE = Path(__file__).resolve().parent / "results"
PAPER_DATA_DIR = Path(__file__).resolve().parent.parent / "reflex-paper" / "data"

LOCAL_MODE = False


# ==============================================================================
# Cluster management (same patterns as run_microbench_load_consistency.py)
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


def generate_config(base_config, target_config, disable_batching=False,
                    localize=False):
    with open(base_config, "r", encoding="utf-8") as f:
        lines = f.readlines()

    if localize:
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
            if 200 <= r.status < 300:
                return True
        except HTTPError as e:
            if 200 <= e.code < 300:
                return True
        except Exception:
            pass
        time.sleep(2)
    return False


def detect_leader(service_name, retries=15):
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
# Replica crash
# ==============================================================================


def crash_replica_remote(host):
    """Kill the AR JVM on a remote host."""
    log.info("    Crashing replica on %s ...", host)
    os.system(f"ssh {host} sudo pkill -9 -f gigapaxos 2>/dev/null || true")


def crash_replica_local(port):
    """Kill the AR process bound to a specific local port."""
    log.info("    Crashing local replica on port %d ...", port)
    os.system(f"sudo fuser -k {port}/tcp 2>/dev/null || true")


# ==============================================================================
# Core experiment
# ==============================================================================


def run_failure_experiment(consistency, results_dir, rate, duration, crash_times,
                           read_ratio):
    """Run one failure experiment for a single consistency model."""
    service_name = CONSISTENCY_SHORT_NAME[consistency]
    screen_session = f"{SCREEN_SESSION}_{consistency}"
    screen_log = f"screen_logs/{screen_session}.log"
    config_file = f"static/gigapaxos.failure.{consistency}.properties"
    service_yaml = f"static/xdn_failure_{consistency}.yaml"
    output_csv = results_dir / OUTPUT_FILENAME[consistency]

    try:
        # Setup with retry
        max_setup_attempts = 3
        for setup_attempt in range(1, max_setup_attempts + 1):
            # (1) Clear cluster
            log.info("  [Step 1] Clearing cluster ... (attempt %d/%d)",
                     setup_attempt, max_setup_attempts)
            clear_xdn_cluster()

            # (2) Generate config
            log.info("  [Step 2] Generating config ...")
            generate_config(GP_CONFIG_BASE, config_file,
                            disable_batching=True,
                            localize=LOCAL_MODE)

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
                result = subprocess.run(deploy_cmd, capture_output=True, text=True,
                                        shell=True)
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

        # (7) Detect leader and identify non-leader replicas
        log.info("  [Step 7] Detecting leader ...")
        leader_host, leader_port = detect_leader(service_name)
        log.info("    Leader: %s:%d", leader_host, leader_port)

        if LOCAL_MODE:
            all_ports = LOCAL_HTTP_PROXY_PORTS
            non_leader_ports = [p for p in all_ports if p != leader_port]
            log.info("    Non-leader ports: %s", non_leader_ports)
            target_host, target_port = "127.0.0.1", leader_port
        else:
            non_leader_hosts = [h for h in AR_HOSTS if h != leader_host]
            log.info("    Non-leader hosts: %s", non_leader_hosts)
            target_host, target_port = leader_host, HTTP_PROXY_PORT

        # (8) Seed data for read requests
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

        # (9) Warmup
        log.info("  [Step 9] Warming up (%ds at %d rps) ...",
                 WARMUP_DURATION_SEC, WARMUP_RATE)
        warmup_out = results_dir / f"{consistency}_warmup.txt"
        _run_go_client(target_host, target_port, service_name, WARMUP_RATE,
                       WARMUP_DURATION_SEC, read_ratio, warmup_out)
        time.sleep(3)

        # (10) Launch Go load client as background subprocess
        log.info("  [Step 10] Starting load client (%ds at %d rps, %d%% reads) ...",
                 duration, rate, read_ratio)
        go_stdout = results_dir / f"{consistency}_client.txt"
        proc = _start_go_client_bg(target_host, target_port, service_name, rate,
                                    duration, read_ratio, output_csv, go_stdout)

        # (11-12) Wait and crash replicas at scheduled times
        start_time = time.time()
        for crash_idx, crash_time in enumerate(crash_times):
            while time.time() - start_time < crash_time:
                time.sleep(0.5)
            elapsed = time.time() - start_time
            if LOCAL_MODE:
                if crash_idx < len(non_leader_ports):
                    crash_replica_local(non_leader_ports[crash_idx])
                    log.info("  Crashed non-leader port %d at t=%.1fs",
                             non_leader_ports[crash_idx], elapsed)
            else:
                if crash_idx < len(non_leader_hosts):
                    crash_replica_remote(non_leader_hosts[crash_idx])
                    log.info("  Crashed non-leader %s at t=%.1fs",
                             non_leader_hosts[crash_idx], elapsed)

        # (13) Wait for Go client to finish
        log.info("  [Step 13] Waiting for load client to finish ...")
        proc.wait()
        log.info("  Load client exited with code %d", proc.returncode)

        if output_csv.exists():
            log.info("  Per-second CSV written: %s", output_csv)
        else:
            log.warning("  Per-second CSV NOT found: %s", output_csv)

    finally:
        # (14) Cleanup
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
        forceclear_cmd = (
            f"../bin/gpServer.sh -DgigapaxosConfig={config_file} forceclear all "
            f"> /dev/null 2>&1"
        )
        os.system(forceclear_cmd)
        os.system(f"screen -S {screen_session} -X quit > /dev/null 2>&1")
        clear_xdn_cluster()
        for f in [config_file, service_yaml]:
            try:
                os.remove(f)
            except OSError:
                pass
        time.sleep(5)


# ==============================================================================
# Go client helpers
# ==============================================================================


def _go_env():
    env = os.environ.copy()
    go_extra = "/usr/local/go/bin"
    if go_extra not in env.get("PATH", ""):
        env["PATH"] = go_extra + ":" + env.get("PATH", "")
    env["GO111MODULE"] = "off"
    env["GOWORK"] = "off"
    return env


def _run_go_client(host, port, service_name, rate, duration, read_ratio,
                   output_path, per_second_csv=None):
    """Run the Go load client synchronously (blocking)."""
    cmd = _build_go_cmd(host, port, service_name, rate, duration, read_ratio,
                        per_second_csv)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as fh:
        subprocess.run(cmd, stdout=fh, stderr=subprocess.STDOUT,
                       env=_go_env(), text=True, cwd=str(GO_CLIENT.parent))


def _start_go_client_bg(host, port, service_name, rate, duration, read_ratio,
                         per_second_csv, stdout_path):
    """Start the Go load client as a background subprocess."""
    cmd = _build_go_cmd(host, port, service_name, rate, duration, read_ratio,
                        per_second_csv)
    stdout_path.parent.mkdir(parents=True, exist_ok=True)
    fh = open(stdout_path, "w", encoding="utf-8")
    proc = subprocess.Popen(cmd, stdout=fh, stderr=subprocess.STDOUT,
                            env=_go_env(), text=True, cwd=str(GO_CLIENT.parent))
    return proc


def _build_go_cmd(host, port, service_name, rate, duration, read_ratio,
                  per_second_csv=None):
    write_url = f"http://{host}:{port}{SERVICE_WRITE_ENDPOINT}"
    read_url = f"http://{host}:{port}{SERVICE_READ_ENDPOINT}"
    payload = '{"item":"bench"}'

    cmd = [
        "go", "run", str(GO_CLIENT),
        "-H", f"XDN: {service_name}",
        "-H", "Content-Type: application/json",
    ]
    if read_ratio > 0:
        cmd.extend(["-read-ratio", f"{read_ratio / 100.0:.2f}"])
        cmd.extend(["-read-url", read_url])
    if per_second_csv is not None:
        cmd.extend(["-per-second-output", str(per_second_csv)])
    cmd.extend([write_url, payload, str(duration), str(rate)])
    return cmd


# ==============================================================================
# CLI and main
# ==============================================================================


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--consistency", type=str, default=None,
                        help="Comma-separated consistency models "
                             "(default: linearizability,sequential,eventual)")
    parser.add_argument("--rate", type=int, default=DEFAULT_RATE,
                        help=f"Offered load in rps (default: {DEFAULT_RATE})")
    parser.add_argument("--duration", type=int, default=DEFAULT_DURATION,
                        help=f"Total experiment duration in seconds (default: {DEFAULT_DURATION})")
    parser.add_argument("--crash-times", type=str,
                        default=",".join(str(t) for t in DEFAULT_CRASH_TIMES),
                        help="Comma-separated seconds at which to crash non-leader "
                             "replicas (default: 60,120)")
    parser.add_argument("--read-ratio", type=int, default=DEFAULT_READ_RATIO,
                        help=f"Read ratio in %% (default: {DEFAULT_READ_RATIO})")
    parser.add_argument("--skip-plot", action="store_true",
                        help="Skip running the plot script after measurements")
    parser.add_argument("--local", action="store_true",
                        help="Run on a single local machine (no SSH)")
    args = parser.parse_args()

    global LOCAL_MODE
    if args.local:
        LOCAL_MODE = True

    # Parse arguments
    if args.consistency:
        models = [m.strip() for m in args.consistency.split(",")]
        for m in models:
            if m not in CONSISTENCY_MODELS:
                print(f"Unknown model: {m}. Valid: {CONSISTENCY_MODELS}",
                      file=sys.stderr)
                sys.exit(1)
    else:
        models = CONSISTENCY_MODELS

    crash_times = [int(t.strip()) for t in args.crash_times.split(",")]

    # Output directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_dir = RESULTS_BASE / f"eval_failure_replica_crashes_{timestamp}"
    results_dir.mkdir(parents=True, exist_ok=True)

    # Logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(results_dir / "run.log"),
        ],
    )

    # Plan
    log.info("=" * 72)
    log.info("Figure 19a: Non-Leader Replica Failure Experiment")
    log.info("=" * 72)
    log.info("  Mode        : %s", "local" if LOCAL_MODE else "cloudlab (remote SSH)")
    log.info("  Models      : %s", ", ".join(models))
    log.info("  Rate        : %d rps", args.rate)
    log.info("  Duration    : %ds", args.duration)
    log.info("  Crash times : %s s", crash_times)
    log.info("  Read ratio  : %d%%", args.read_ratio)
    log.info("  Results     : %s", results_dir)
    log.info("=" * 72)

    # Verify prerequisites
    ret = os.system(f"{XDN_BINARY} --help > /dev/null 2>&1")
    assert ret == 0, f"Cannot find {XDN_BINARY}"

    # Ensure Docker image is present
    if LOCAL_MODE:
        log.info("  Ensuring Docker image '%s' is present locally ...", TODO_IMAGE)
        check = subprocess.run(
            ["docker", "image", "inspect", TODO_IMAGE], capture_output=True,
        )
        if check.returncode != 0:
            log.info("    Pulling '%s' locally ...", TODO_IMAGE)
            pull = subprocess.run(
                ["docker", "pull", TODO_IMAGE], capture_output=True, text=True,
            )
            if pull.returncode != 0:
                log.error("    Failed to pull image: %s", pull.stderr.strip())
                sys.exit(1)
    else:
        all_hosts = AR_HOSTS + [CONTROL_PLANE_HOST]
        log.info("  Ensuring Docker image '%s' is present on all nodes ...",
                 TODO_IMAGE)
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
                    log.error("    Failed to pull on %s: %s",
                              host, pull.stderr.strip())
                    sys.exit(1)

    # Run experiments
    for i, consistency in enumerate(models, start=1):
        log.info("")
        log.info("=" * 72)
        log.info("[%d/%d] Consistency: %s", i, len(models), consistency)
        log.info("=" * 72)

        try:
            run_failure_experiment(
                consistency, results_dir, args.rate, args.duration,
                crash_times, args.read_ratio,
            )
        except Exception:
            log.exception("[%d/%d] FAILED: %s", i, len(models), consistency)

    # Post-processing: copy CSVs to reflex-paper/data/
    log.info("")
    log.info("=" * 72)
    log.info("Post-processing")
    log.info("=" * 72)

    if PAPER_DATA_DIR.exists():
        for consistency in models:
            src = results_dir / OUTPUT_FILENAME[consistency]
            dst = PAPER_DATA_DIR / OUTPUT_FILENAME[consistency]
            if src.exists():
                shutil.copy2(src, dst)
                log.info("  Copied %s -> %s", src.name, dst)
            else:
                log.warning("  Missing: %s", src)

        # Run combine script
        combine_script = PAPER_DATA_DIR / "combine_failure_csvs.py"
        if combine_script.exists():
            log.info("  Running combine_failure_csvs.py ...")
            subprocess.run(
                [sys.executable, str(combine_script)],
                cwd=str(PAPER_DATA_DIR),
            )

        # Run plot script
        if not args.skip_plot:
            plot_script = PAPER_DATA_DIR / "plot_eval_failure_figures.py"
            figures_dir = PAPER_DATA_DIR.parent / "figures"
            if plot_script.exists():
                log.info("  Running plot_eval_failure_figures.py ...")
                subprocess.run([
                    sys.executable, str(plot_script),
                    "--data-dir", str(PAPER_DATA_DIR),
                    "--figures-dir", str(figures_dir),
                ])
    else:
        log.warning("  Paper data dir not found: %s", PAPER_DATA_DIR)

    log.info("")
    log.info("=" * 72)
    log.info("[Done] Results saved to %s", results_dir)
    log.info("=" * 72)


if __name__ == "__main__":
    main()
