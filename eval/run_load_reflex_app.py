import argparse
import csv
import json
import logging
import os
import re
import shlex
import subprocess
import tempfile
import time
import urllib.request
from urllib.error import HTTPError
from pathlib import Path
from typing import List, Tuple

# Script to start an XDN cluster, deploy a Dockerized service, hammer it with k6,
# and persist throughput/latency metrics for quick load comparisons.

ACTIVE_REPLICA_RE = re.compile(r"^\s*active\.[^=]+=\s*([^:\s]+):(\d+)", re.IGNORECASE)
RECONFIGURATOR_RE = re.compile(r"^\s*reconfigurator\.[^=]+=\s*([^:\s]+):(\d+)", re.IGNORECASE)
DEFAULT_VUS = [1, 5, 10, 20, 50]
logger = logging.getLogger(__name__)


def parse_active_replicas(config_path: Path) -> List[Tuple[str, int]]:
    actives: List[Tuple[str, int]] = []
    with open(config_path, "r", encoding="utf-8") as fh:
        for line in fh:
            match = ACTIVE_REPLICA_RE.match(line)
            if match:
                host, port = match.group(1), int(match.group(2))
                actives.append((host, port))
    if not actives:
        raise ValueError(f"No active replicas found in {config_path}")
    logger.info("Parsed %s active replicas from %s: %s", len(actives), config_path, actives)
    return actives


def parse_control_plane(config_path: Path) -> Tuple[str, int]:
    with open(config_path, "r", encoding="utf-8") as fh:
        for line in fh:
            match = RECONFIGURATOR_RE.match(line)
            if match:
                host, port = match.group(1), int(match.group(2))
                logger.info("Parsed control plane %s:%s from %s", host, port, config_path)
                return host, port
    raise ValueError(f"No reconfigurator entry found in {config_path}")


def normalize_service_name(image_name: str) -> str:
    base = image_name.split("/")[-1].replace(":", "_")
    base = re.sub(r"[^A-Za-z0-9_.-]", "_", base)
    return f"svc_{base or 'app'}"


def start_cluster(config_path: Path, repo_root: Path):
    gp_server = repo_root / "bin" / "gpServer.sh"
    cmd = [str(gp_server), f"-DgigapaxosConfig={config_path}", "start", "all"]
    logger.info("Starting XDN cluster with %s", config_path)
    subprocess.run(cmd, check=True)
    time.sleep(5)


def stop_cluster(config_path: Path, repo_root: Path):
    gp_server = repo_root / "bin" / "gpServer.sh"
    clear_cmd = [str(gp_server), f"-DgigapaxosConfig={config_path}", "forceclear", "all"]
    try:
        subprocess.run(clear_cmd, check=True)
    except subprocess.CalledProcessError:
        logger.warning("Cluster cleanup failed for command: %s", " ".join(clear_cmd))


def deploy_service(control_plane_host: str, image: str, service_name: str):
    # Launch the target image as an XDN service via CLI; assume control-plane host resolves.
    cmd = (
        f"XDN_CONTROL_PLANE={shlex.quote(control_plane_host)} "
        f"xdn launch {shlex.quote(service_name)} "
        f"--image={shlex.quote(image)} --deterministic=true "
        f"--consistency=linearizability --state=/app/data/"
    )
    logger.info("Launching XDN service %s with image %s", service_name, image)
    subprocess.run(cmd, shell=True, check=True)
    time.sleep(5)


def destroy_service(control_plane_host: str, service_name: str):
    cmd = (
        f"yes yes | XDN_CONTROL_PLANE={shlex.quote(control_plane_host)} "
        f"xdn service destroy {shlex.quote(service_name)}"
    )
    try:
        subprocess.run(cmd, shell=True, check=True)
    except subprocess.CalledProcessError:
        logger.warning("Failed to destroy service %s; manual cleanup may be needed", service_name)


def build_k6_script(host: str, port: int, service_name: str) -> str:
    # Keep the load script minimal to reduce JS build overhead.
    return f"""
import http from 'k6/http';
import {{ sleep }} from 'k6';

const HEADERS = {{ 'XDN': '{service_name}' }};

export default function () {{
  http.get('http://{host}:{port}/api/books', {{ headers: HEADERS }});
  sleep(0.1);
}}
"""


def verify_service(host: str, port: int, service_name: str, timeout: int = 60, interval: float = 2.0):
    """Poll the service endpoint until it responds or timeout expires."""
    url = f"http://{host}:{port}/"
    headers = {"XDN": service_name}
    deadline = time.time() + timeout
    last_error = None
    attempt = 0
    logger.info("Waiting for service %s at %s (timeout=%ss, interval=%ss)", service_name, url, timeout, interval)
    while time.time() < deadline:
        attempt += 1
        try:
            req = urllib.request.Request(url, headers=headers, method="GET")
            with urllib.request.urlopen(req, timeout=5) as resp:
                if resp.status < 500:
                    logger.info("Service %s is up at %s (status %s, attempts=%s)", service_name, url, resp.status, attempt)
                    return
        except HTTPError as exc:
            if exc.code < 500:
                logger.info("Service %s is up at %s (status %s, attempts=%s)", service_name, url, exc.code, attempt)
                return
            last_error = exc
        except Exception as exc:
            last_error = exc
            logger.debug("Service probe attempt %s failed: %s", attempt, exc)
        time.sleep(interval)
    raise TimeoutError(f"Service {service_name} not ready at {url}; last error: {last_error}")


def run_k6(script_path: str, vus: int, duration: str, summary_path: Path):
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
    csv_path = results_dir / "reflex_load_results.csv"
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
    log_level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_name, logging.INFO)
    logging.basicConfig(level=log_level, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(
        description="Launch an XDN app and measure load with k6."
    )
    parser.add_argument("xdnConfigFile", help="Path to gigapaxos.xdn.*.properties file")
    parser.add_argument("dockerImageName", help="Docker image for the XDN service")
    parser.add_argument(
        "--vus",
        help="Comma-separated list of virtual users to test",
        default=",".join(str(v) for v in DEFAULT_VUS),
    )
    parser.add_argument("--duration", default="30s", help="Duration for each k6 run (e.g., 30s)")
    parser.add_argument(
        "--service-name",
        default=None,
        help="Optional XDN service name (default: 'svc')",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent.parent
    results_dir = Path(__file__).resolve().parent / "results"
    results_dir.mkdir(parents=True, exist_ok=True)

    config_path = Path(args.xdnConfigFile).resolve()
    active_replicas = parse_active_replicas(config_path)
    control_plane_host, _ = parse_control_plane(config_path)
    target_host, target_port = active_replicas[0]
    http_target_port = target_port + 300  # Assuming HTTP port offset
    vus_list = [int(v.strip()) for v in args.vus.split(",") if v.strip()]
    service_name = args.service_name or "svc"
    logger.info(
        "Using service name=%s, target=%s:%s (http port %s), VUs=%s",
        service_name,
        target_host,
        target_port,
        http_target_port,
        vus_list,
    )

    tmp_script = None
    try:
        start_cluster(config_path, repo_root)
        deploy_service(control_plane_host, args.dockerImageName, service_name)
        verify_service(target_host, http_target_port, service_name)

        script_content = build_k6_script(target_host, http_target_port, service_name)
        with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False) as tf:
            tf.write(script_content)
            tmp_script = tf.name

        results = []
        for vu in vus_list:
            summary_file = results_dir / f"reflex_k6_summary_vu{vu}.json"
            logger.info("Running k6 with %s virtual users", vu)
            run_k6(tmp_script, vu, args.duration, summary_file)
            metrics = parse_k6_summary(summary_file)
            logger.info("Results for VUs=%s: %s", vu, metrics)
            results.append({"virtual_users": vu, **metrics})

        csv_path = write_results_csv(results_dir, results)
        print(f"Results saved to {csv_path}")
    finally:
        destroy_service(control_plane_host, service_name)
        stop_cluster(config_path, repo_root)
        if tmp_script:
            Path(tmp_script).unlink(missing_ok=True)


if __name__ == "__main__":
    main()
