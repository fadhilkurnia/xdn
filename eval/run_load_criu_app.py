import argparse
import csv
import getpass
import json
import logging
import os
import re
import shlex
import socket
import subprocess
import tempfile
import time
from pathlib import Path
from typing import List, Optional, Tuple

ACTIVE_REPLICA_RE = re.compile(r"^\s*active\.[^=]+=\s*([^:\s]+):(\d+)", re.IGNORECASE)
DEFAULT_VUS = [1, 5, 10, 20, 50]
logger = logging.getLogger(__name__)


def parse_active_replicas(config_path: Path) -> List[Tuple[str, int]]:
    # Extract all active replicas (host, port) lines from the Gigapaxos config.
    actives: List[Tuple[str, int]] = []
    with open(config_path, "r", encoding="utf-8") as fh:
        for line in fh:
            match = ACTIVE_REPLICA_RE.match(line)
            if match:
                host, port = match.group(1), int(match.group(2))
                actives.append((host, port))
    if not actives:
        raise ValueError(f"No active replicas found in {config_path}")
    return actives


def normalize_container_name(image_name: str) -> str:
    # Convert image name to a filesystem-safe container name.
    base = image_name.split("/")[-1].replace(":", "_")
    base = re.sub(r"[^A-Za-z0-9_.-]", "_", base)
    return f"criu_{base or 'app'}"


def is_local_host(host: str) -> bool:
    # Fast-path: avoid ssh if the target is local.
    local_hosts = {"127.0.0.1", "localhost", socket.gethostname()}
    try:
        local_hosts.add(socket.gethostbyname(socket.gethostname()))
    except socket.error:
        pass
    return host in local_hosts


def run_remote(
    host: str, command: str, capture_output: bool = False, timeout: Optional[int] = None
) -> subprocess.CompletedProcess:
    # Run a command locally or via SSH depending on the host.
    kwargs = {"check": True, "text": True, "stdin": subprocess.DEVNULL}
    is_nohup = "nohup" in command
    if capture_output:
        kwargs["capture_output"] = True
    if timeout is not None:
        kwargs["timeout"] = timeout
    if is_local_host(host):
        return subprocess.run(command, shell=True, **kwargs)
    if is_nohup:
        # Wrap in bash to ensure nohup background PIDs propagate correctly over SSH.
        command = f"bash -lc {shlex.quote(command)}"
    return subprocess.run(["ssh", host, command], **kwargs)

def start_remote_long_running(
    user: str,
    host: str,
    command: str,
    identity_file: Optional[str] = None,
    port: int = 22,
    log_file: str = "remote_command.log",
) -> str:
    """
    Start a long-running command on a remote machine via SSH.

    The command will be started under nohup, output redirected to log_file,
    and the SSH session will return immediately.

    Args:
        user: Username for SSH (e.g., 'ubuntu').
        host: Hostname or IP (e.g., 'example.com' or '1.2.3.4').
        command: The long-running command to execute remotely.
        identity_file: Path to SSH private key, if needed.
        port: SSH port (default 22).
        log_file: Remote log file path to capture stdout/stderr.
    """

    # Wrap the command to run under nohup in the background, with I/O redirected.
    # - `nohup` ignores HUP signals (logout)
    # - Redirect stdout and stderr to log_file
    # - `< /dev/null` to detach stdin
    # - `& echo $!` prints the PID of the background process
    remote_cmd = f"nohup {command} > {shlex.quote(log_file)} 2>&1 < /dev/null & echo $!"

    # Build base ssh command
    ssh_cmd = ["ssh", "-p", str(port)]

    if identity_file:
        ssh_cmd.extend(["-i", identity_file])

    # Disable host key checking if you want (optional, but less secure)
    # ssh_cmd.extend(["-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null"])

    ssh_cmd.append(f"{user}@{host}")
    ssh_cmd.append(remote_cmd)

    print("Running SSH command:")
    print(" ".join(shlex.quote(part) for part in ssh_cmd))
    print()

    # Execute the SSH command and capture the PID of the background process
    try:
        result = subprocess.run(
            ssh_cmd,
            check=True,
            text=True,
            capture_output=True,
        )
    except subprocess.CalledProcessError as e:
        stdout = e.stdout or ""
        stderr = e.stderr or ""
        raise RuntimeError(
            f"Failed to start remote command on {host}: {stderr or stdout or e}"
        ) from e

    pid = result.stdout.strip().splitlines()[-1]
    print("Remote command started.")
    print(f"Remote PID: {pid}")
    print(f"Output is being written to: {log_file} (on {host})")
    return pid

def start_container(host: str, container_name: str, image: str):
    # Restart the target container and expose port 80 -> 8080.
    cmd = (
        f"docker rm -f {shlex.quote(container_name)} >/dev/null 2>&1 || true; "
        f"docker run -d --rm -p 8080:80 --name {shlex.quote(container_name)} {shlex.quote(image)}"
    )
    try:
        result = run_remote(host, cmd, capture_output=True)
    except subprocess.CalledProcessError as exc:
        stderr = exc.stderr.strip() if exc.stderr else ""
        stdout = exc.stdout.strip() if exc.stdout else ""
        msg = stderr or stdout or str(exc)
        raise RuntimeError(f"Failed to start container {container_name}: {msg}") from exc
    try:
        state_res = run_remote(
            host,
            f"docker inspect -f '{{{{.State.Running}}}}' {shlex.quote(container_name)}",
            capture_output=True,
            timeout=5,
        )
    except subprocess.CalledProcessError as exc:
        stderr = exc.stderr.strip() if exc.stderr else ""
        stdout = exc.stdout.strip() if exc.stdout else ""
        msg = stderr or stdout or str(exc)
        raise RuntimeError(f"Failed to verify container state for {container_name}: {msg}") from exc
    if state_res.stdout.strip().lower() != "true":
        raise RuntimeError(f"Container {container_name} is not running after start.")

    port_check_cmd = """python3 - <<'PY'
import socket
import sys
import time

addr = ("127.0.0.1", 8080)
deadline = time.time() + 5
err = None

while time.time() < deadline:
    with socket.socket() as s:
        s.settimeout(1)
        try:
            s.connect(addr)
            sys.exit(0)
        except OSError as exc:
            err = exc
            time.sleep(0.2)

print(err or "port 8080 not listening")
sys.exit(1)
PY"""
    try:
        run_remote(host, port_check_cmd, capture_output=True, timeout=7)
    except subprocess.CalledProcessError as exc:
        stderr = exc.stderr.strip() if exc.stderr else ""
        stdout = exc.stdout.strip() if exc.stdout else ""
        msg = stderr or stdout or str(exc)
        raise RuntimeError(
            f"Container {container_name} is not listening on port 8080: {msg}"
        ) from exc

    return result


def init_criu(host: str, repo_root: Path):
    init_script = repo_root / "eval" / "init_criu.sh"
    init_cmd = f"cd {shlex.quote(str(init_script.parent))} && sudo ./init_criu.sh"
    logger.info("Initializing CRIU on %s", host)
    try:
        run_remote(host, init_cmd)
    except subprocess.CalledProcessError as exc:
        stderr = exc.stderr.strip() if exc.stderr else ""
        stdout = exc.stdout.strip() if exc.stdout else ""
        msg = stderr or stdout or str(exc)
        raise RuntimeError(f"Failed to initialize CRIU on {host}: {msg}") from exc


def start_baseline_replica(host: str, container_name: str, repo_root: Path) -> str:
    # Launch the BaselineCriuReplica in the background; return its PID.

    classpath = ":".join(
        [
            str(repo_root / "jars" / "gigapaxos-1.0.10.jar"),
            str(repo_root / "jars" / "gigapaxos-nio-src.jar"),
            str(repo_root / "jars" / "nio-1.2.1.jar"),
        ]
    )
    pattern = rf"edu\.umass\.cs\.xdn\.eval\.BaselineCriuReplica.*{re.escape(container_name)}"
    kill_cmd = f"sudo pkill -f {shlex.quote(pattern)} >/dev/null 2>&1 || true"

    # Kill any existing replica.
    run_remote(host, kill_cmd)

    # Start the primary replica.
    start_cmd = (
        f"java -cp {shlex.quote(classpath)} -Djdk.httpclient.allowRestrictedHeaders=content-length "
        f"edu.umass.cs.xdn.eval.BaselineCriuReplica 8080 {shlex.quote(container_name)}"
    )
    start_cmd_local = f"nohup {start_cmd} > baseline_criu_replica.log 2>&1 & echo $!"
    logger.info("BaselineCriuReplica repo root: %s", repo_root)
    logger.info("BaselineCriuReplica kill command: %s", kill_cmd)
    logger.info("BaselineCriuReplica start command: %s", start_cmd)
    try:
        if is_local_host(host):
            res = run_remote(host, start_cmd_local, capture_output=True, timeout=15)
            pid = res.stdout.strip().splitlines()[-1]
        else:
            ssh_user, ssh_host = (
                host.split("@", 1) if "@" in host else (getpass.getuser(), host)
            )
            pid = start_remote_long_running(
                ssh_user,
                ssh_host,
                start_cmd,
                log_file=str(repo_root / "baseline_criu_replica.log"),
            )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(
            f"Launching BaselineCriuReplica hung on host {host}. "
            f"Check Docker/CRIU availability or failing command: {start_cmd}"
        ) from exc

    wait_host = host.split("@", 1)[-1] if "@" in host else host
    logger.info("Waiting for BaselineCriuReplica to listen on %s:2300", wait_host)
    deadline = time.time() + 20
    last_error: Optional[BaseException] = None
    while time.time() < deadline:
        try:
            with socket.create_connection((wait_host, 2300), timeout=1.0):
                break
        except OSError as exc:
            last_error = exc
            time.sleep(0.25)
    else:
        raise RuntimeError(
            f"BaselineCriuReplica on {wait_host} did not open port 2300 within timeout"
        ) from last_error

    return pid


def stop_services(host: Optional[str], container_name: str, baseline_pid: Optional[str]):
    # Best-effort cleanup for replica and container.
    pieces = []
    if baseline_pid:
        pieces.append(f"kill {shlex.quote(str(baseline_pid))} >/dev/null 2>&1 || true")
    pieces.append(f"docker rm -f {shlex.quote(container_name)} >/dev/null 2>&1 || true")
    if host:
        run_remote(host, " && ".join(pieces))


def build_k6_script(target_host: str) -> str:
    # Minimal JS script: send GETs to the replica endpoint with small sleep.
    return f"""
import http from 'k6/http';
import {{ sleep }} from 'k6';

export default function () {{
  http.get('http://{target_host}:2300/api/books');
  sleep(0.1);
}}
"""


def run_k6(script_path: str, vus: int, duration: str, summary_path: Path):
    # Execute k6 and export summary to JSON for later parsing.
    cmd = [
        "k6",
        "run",
        "--vus",
        str(vus),
        "--duration",
        duration,
        "--summary-export",
        str(summary_path),
        script_path,
    ]
    subprocess.run(cmd, check=True)


def parse_k6_summary(summary_path: Path):
    # Pull throughput and latency percentiles from the k6 JSON summary.
    with open(summary_path, "r", encoding="utf-8") as fh:
        data = json.load(fh)

    def extract_metrics(section: str) -> dict:
        try:
            metric = data["metrics"][section]
        except KeyError as exc:
            raise ValueError(f"k6 summary missing metrics.{section}") from exc
        return metric.get("values", metric)

    durations = extract_metrics("http_req_duration")
    reqs = extract_metrics("http_reqs")

    for key in ("p(95)", "med", "avg"):
        if key not in durations:
            raise ValueError(f"k6 summary missing http_req_duration.{key}")

    try:
        throughput = reqs["rate"]
    except KeyError:
        if "count" not in reqs:
            raise ValueError("k6 summary missing http_reqs.rate and count")
        duration_ms = data.get("state", {}).get("testRunDurationMs")
        if duration_ms is None:
            raise ValueError("k6 summary missing state.testRunDurationMs")
        throughput = reqs["count"] / (duration_ms / 1000.0)

    return {
        "throughput_rps": throughput,
        "p95_ms": durations["p(95)"],
        "p90_ms": durations["p(90)"],
        "p50_ms": durations["med"],
        "avg_ms": durations["avg"],
    }


def write_results_csv(results_dir: Path, rows):
    # Persist collected metrics to CSV.
    csv_path = results_dir / "criu_baseline_results.csv"
    fieldnames = [
        "virtual_users",
        "throughput_rps",
        "p95_ms",
        "p90_ms",
        "p50_ms",
        "avg_ms",
    ]
    with open(csv_path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return csv_path


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(
        description="Run BaselineCriuReplica and measure load with k6."
    )
    parser.add_argument("xdnConfigFile", help="Path to gigapaxos.xdn.*.properties file")
    parser.add_argument("dockerImageName", help="Docker image for the target service")
    parser.add_argument(
        "--vus",
        help="Comma-separated list of virtual users to test",
        default=",".join(str(v) for v in DEFAULT_VUS),
    )
    parser.add_argument("--duration", default="30s", help="Duration for each k6 run (e.g., 30s)")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent.parent
    results_dir = Path(__file__).resolve().parent / "results"
    results_dir.mkdir(parents=True, exist_ok=True)

    active_replicas = parse_active_replicas(Path(args.xdnConfigFile))
    logger.info("Loaded active replicas from %s", args.xdnConfigFile)
    target_host, _ = active_replicas[0]
    vus_list = [int(v.strip()) for v in args.vus.split(",") if v.strip()]
    container_name = normalize_container_name(args.dockerImageName)
    logger.info(
        "Using host %s with container %s for image %s; VUs=%s",
        target_host,
        container_name,
        args.dockerImageName,
        vus_list,
    )

    baseline_pid = None
    tmp_script = None
    try:
        init_criu(target_host, repo_root)
        
        logger.info("Starting container on %s", target_host)
        start_container(target_host, container_name, args.dockerImageName)
        logger.info("Starting BaselineCriuReplica")
        baseline_pid = start_baseline_replica(target_host, container_name, repo_root)
        logger.info("BaselineCriuReplica started with PID %s", baseline_pid)
        time.sleep(3)  # brief pause to let the replica warm up

        script_content = build_k6_script(target_host)
        with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False) as tf:
            tf.write(script_content)
            tmp_script = tf.name

        results = []
        for vu in vus_list:
            summary_file = results_dir / f"k6_summary_vu{vu}.json"
            logger.info("Running k6 with %s virtual users", vu)
            run_k6(tmp_script, vu, args.duration, summary_file)
            metrics = parse_k6_summary(summary_file)
            logger.info("Results for VUs=%s: %s", vu, metrics)
            results.append(
                {
                    "virtual_users": vu,
                    **metrics,
                }
            )

        csv_path = write_results_csv(results_dir, results)
        print(f"Results saved to {csv_path}")
    finally:
        logger.info("Cleaning up temporary files and services")
        if tmp_script and os.path.exists(tmp_script):
            os.remove(tmp_script)
        stop_services(target_host if "target_host" in locals() else None, container_name if "container_name" in locals() else "", baseline_pid)


if __name__ == "__main__":
    main()
