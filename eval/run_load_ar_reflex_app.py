import argparse
import csv
import json
import logging
import os
import re
import resource
import shlex
import subprocess
import sys
import tempfile
import time
import urllib.request
from datetime import datetime
from urllib.error import HTTPError
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Script to start an XDN cluster, deploy a Dockerized service, hammer it with k6 or a Go client,
# and persist throughput/latency metrics for quick load comparisons.

ACTIVE_REPLICA_RE = re.compile(r"^\s*active\.[^=]+=\s*([^:\s]+):(\d+)", re.IGNORECASE)
RECONFIGURATOR_RE = re.compile(r"^\s*reconfigurator\.[^=]+=\s*([^:\s]+):(\d+)", re.IGNORECASE)
HTTP_PORT_OFFSET_RE = re.compile(r"^\s*HTTP_PORT_OFFSET\s*=\s*(\d+)\s*$", re.IGNORECASE)
ENABLE_RECONFIGURATOR_HTTP_RE = re.compile(r"^\s*ENABLE_RECONFIGURATOR_HTTP\s*=\s*(\S+)\s*$", re.IGNORECASE)
ENABLE_ACTIVE_REPLICA_HTTP_RE = re.compile(r"^\s*ENABLE_ACTIVE_REPLICA_HTTP\s*=\s*(\S+)\s*$", re.IGNORECASE)
ENABLE_ACTIVE_REPLICA_HTTP_PORT_80_RE = re.compile(
    r"^\s*ENABLE_ACTIVE_REPLICA_HTTP_PORT_80\s*=\s*(\S+)\s*$", re.IGNORECASE
)
DEFAULT_RATES = [100, 200, 300, 400, 500, 800, 1000, 1200, 1500, 2000, 2500, 3000, 4000, 5000]
DEFAULT_LOAD_GENERATOR = "go"
HTTP_PORT_OFFSET = 300
VALID_APPS = {
    "fadhilkurnia/xdn-bookcatalog",
    "fadhilkurnia/xdn-webkv",
    "fadhilkurnia/xdn-todo",
    "fadhilkurnia/xdn-todo-go",
}
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


def parse_reconfigurators(config_path: Path) -> List[Tuple[str, int]]:
    reconfigurators: List[Tuple[str, int]] = []
    with open(config_path, "r", encoding="utf-8") as fh:
        for line in fh:
            match = RECONFIGURATOR_RE.match(line)
            if match:
                host, port = match.group(1), int(match.group(2))
                reconfigurators.append((host, port))
    if not reconfigurators:
        raise ValueError(f"No reconfigurators found in {config_path}")
    logger.info("Parsed %s reconfigurators from %s: %s", len(reconfigurators), config_path, reconfigurators)
    return reconfigurators


def parse_http_settings(config_path: Path) -> Tuple[int, Optional[bool], Optional[bool], bool]:
    http_offset = HTTP_PORT_OFFSET
    enable_rc_http: Optional[bool] = None
    enable_active_http: Optional[bool] = None
    active_http_port_80 = False
    with open(config_path, "r", encoding="utf-8") as fh:
        for line in fh:
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            match = HTTP_PORT_OFFSET_RE.match(stripped)
            if match:
                http_offset = int(match.group(1))
                continue
            match = ENABLE_RECONFIGURATOR_HTTP_RE.match(stripped)
            if match:
                enable_rc_http = parse_bool(match.group(1))
                continue
            match = ENABLE_ACTIVE_REPLICA_HTTP_RE.match(stripped)
            if match:
                enable_active_http = parse_bool(match.group(1))
                continue
            match = ENABLE_ACTIVE_REPLICA_HTTP_PORT_80_RE.match(stripped)
            if match:
                active_http_port_80 = parse_bool(match.group(1)) is True
                continue
    return http_offset, enable_rc_http, enable_active_http, active_http_port_80


def parse_bool(raw: str) -> Optional[bool]:
    value = raw.strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    return None


def wait_for_http_ready(
    targets: List[Tuple[str, int]],
    label: str,
    timeout: int = 90,
    interval: float = 0.5,
    request_timeout: float = 2.0,
) -> None:
    if not targets:
        return
    pending = {(host, port): None for host, port in targets}
    deadline = time.time() + timeout
    logger.info(
        "Waiting for %s HTTP readiness across %s endpoints (timeout=%ss, interval=%ss)",
        label,
        len(pending),
        timeout,
        interval,
    )
    while pending and time.time() < deadline:
        ready_now = []
        for host, port in pending:
            url = f"http://{host}:{port}/"
            try:
                req = urllib.request.Request(url, method="GET")
                with urllib.request.urlopen(req, timeout=request_timeout) as resp:
                    if resp.status < 500:
                        ready_now.append((host, port))
                        continue
            except HTTPError as exc:
                if exc.code < 500:
                    ready_now.append((host, port))
                    continue
                pending[(host, port)] = exc
            except Exception as exc:
                pending[(host, port)] = exc
        for target in ready_now:
            pending.pop(target, None)
        if pending:
            time.sleep(interval)
    if pending:
        pending_desc = ", ".join(f"{host}:{port}" for host, port in pending)
        last_error = next(iter(pending.values()))
        raise TimeoutError(
            f"{label} HTTP endpoints not ready after {timeout}s: {pending_desc}; last error: {last_error}"
        )
    logger.info("%s HTTP endpoints ready.", label)


def normalize_service_name(image_name: str) -> str:
    base = image_name.split("/")[-1].replace(":", "_")
    base = re.sub(r"[^A-Za-z0-9_.-]", "_", base)
    return f"svc_{base or 'app'}"


def get_app_request_config(image: str) -> Tuple[str, Dict[str, str], str, Dict[str, str]]:
    if image not in VALID_APPS:
        raise ValueError(f"Unsupported application image: {image}")

    if image == "fadhilkurnia/xdn-bookcatalog":
        return "/api/books", {"author": "abc", "title": "xyz"}, "bookcatalog", {"ENABLE_WAL": "true"}
    if image == "fadhilkurnia/xdn-webkv":
        return "/api/kv/abc", {"key": "abc", "value": "xyz"}, "webkv", {}
    if image == "fadhilkurnia/xdn-todo":
        return "/api/todo/tasks", {"item": "task"}, "todo", {"ENABLE_WAL": "true"}
    if image == "fadhilkurnia/xdn-todo-go":
        return "/api/todo/tasks", {"item": "task"}, "todogo", {"ENABLE_WAL": "true"}

    raise ValueError(f"No request payload defined for image: {image}")


def start_cluster(config_path: Path, repo_root: Path, extra_jvm_args: str = ""):
    gp_server = repo_root / "bin" / "gpServer.sh"
    cmd = [str(gp_server), f"-DgigapaxosConfig={config_path}"]
    if extra_jvm_args:
        cmd.extend(extra_jvm_args.split())
    cmd.extend(["start", "all"])
    logger.info("Starting XDN cluster with %s (extra JVM args: %s)", config_path, extra_jvm_args or "(none)")
    subprocess.run(cmd, check=True)
    
    logger.info("Waiting for control plane and active replica HTTP endpoints to be ready...")
    http_offset, rc_http_enabled, active_http_enabled, active_http_port_80 = parse_http_settings(config_path)
    if rc_http_enabled is False:
        raise ValueError("ENABLE_RECONFIGURATOR_HTTP=false; control plane HTTP is disabled.")
    if active_http_enabled is False:
        raise ValueError("ENABLE_ACTIVE_REPLICA_HTTP=false; active replica HTTP is disabled.")
    reconfigurators = parse_reconfigurators(config_path)
    actives = parse_active_replicas(config_path)
    rc_targets = [(host, port + http_offset) for host, port in reconfigurators]
    if active_http_port_80:
        active_targets = [(host, 80) for host, _ in actives]
    else:
        active_targets = [(host, port + http_offset) for host, port in actives]
    wait_for_http_ready(rc_targets, "reconfigurator")
    wait_for_http_ready(active_targets, "active replica")


def stop_cluster(config_path: Path, repo_root: Path):
    gp_server = repo_root / "bin" / "gpServer.sh"
    clear_cmd = [str(gp_server), f"-DgigapaxosConfig={config_path}", "forceclear", "all"]
    try:
        subprocess.run(clear_cmd, check=True)
    except subprocess.CalledProcessError:
        logger.warning("Cluster cleanup failed for command: %s", " ".join(clear_cmd))


def deploy_service(control_plane_host: str, image: str, service_name: str, env_vars: Optional[Dict[str, str]] = None):
    # Launch the target image as an XDN service via CLI; assume control-plane host resolves.
    env_parts = []
    for key, value in (env_vars or {}).items():
        env_parts.append(f"--env {shlex.quote(f'{key}={value}')}")
    env_arg = " ".join(env_parts)
    cmd = (
        f"XDN_CONTROL_PLANE={shlex.quote(control_plane_host)} "
        f"xdn launch {shlex.quote(service_name)} "
        f"--image={shlex.quote(image)} --deterministic=true "
        f"--consistency=linearizability --state=/app/data/ {env_arg}"
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


def build_k6_script(
    host: str,
    port: int,
    service_name: str,
    rate: int,
    duration: str,
    post_endpoint: str,
    payload_data: Dict[str, str],
) -> str:
    # Keep the load script minimal to reduce JS build overhead.
    pre_alloc_vus = max(1, rate * 2)
    max_vus = pre_alloc_vus * 2
    payload = json.dumps(payload_data)
    return f"""
import http from 'k6/http';

export const options = {{
  scenarios: {{
    constant_rate: {{
      executor: 'constant-arrival-rate',
      rate: {rate},
      timeUnit: '1s',
      duration: '{duration}',
      preAllocatedVUs: {pre_alloc_vus},
      maxVUs: {max_vus},
    }},
  }},
}};

const HEADERS = {{ 'XDN': '{service_name}', 'Content-Type': 'application/json' }};
const PAYLOAD = {payload};

export default function () {{
  http.post('http://{host}:{port}{post_endpoint}', PAYLOAD, {{ headers: HEADERS }});
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


def detect_leader_replica(
    active_replicas: List[Tuple[str, int]],
    service_name: str,
    retries: int = 5,
    interval: float = 1.0,
) -> Tuple[str, int]:
    """
    Query each active replica's replica/info endpoint and return the leader/primary.
    Exits the script if multiple leaders are detected (split-brain).
    Raises a RuntimeError if no leader is found after retries.
    """
    headers = {"XDN": service_name}

    for attempt in range(1, retries + 1):
        leaders: List[Tuple[str, int]] = []
        for host, port in active_replicas:
            http_port = port + HTTP_PORT_OFFSET
            url = f"http://{host}:{http_port}/api/v2/services/{service_name}/replica/info"
            try:
                req = urllib.request.Request(url, headers=headers, method="GET")
                with urllib.request.urlopen(req, timeout=5) as resp:
                    if resp.status >= 400:
                        continue
                    info = json.load(resp)
            except Exception as exc:
                logger.debug("Replica info probe failed for %s (attempt %s): %s", url, attempt, exc)
                continue

            role = str(info.get("role", "")).lower()
            if role in ("leader", "primary"):
                leaders.append((host, port))

        if len(leaders) == 1:
            host, port = leaders[0]
            logger.info("Detected leader replica at %s:%s (attempt %s)", host, port, attempt)
            return host, port

        if len(leaders) > 1:
            leaders_desc = ", ".join(f"{h}:{p}" for h, p in leaders)
            logger.error(
                "Multiple leaders detected for %s: %s — aborting to avoid inconsistent measurements.",
                service_name,
                leaders_desc,
            )
            sys.exit(1)

        logger.debug("No leader found for %s (attempt %s/%s)", service_name, attempt, retries)
        if attempt < retries:
            time.sleep(interval)

    raise RuntimeError(
        f"Unable to determine leader/primary replica for {service_name} after {retries} attempts"
    )


def run_k6(script_path: str, summary_path: Path):
    cmd = ["k6", "run", "--summary-export", str(summary_path), script_path]
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


def parse_duration_seconds(duration: str) -> float:
    match = re.match(r"^\s*(\d+(?:\.\d+)?)(ms|s|m|h)?\s*$", duration)
    if not match:
        raise ValueError("Duration must be a number optionally followed by ms, s, m, or h")
    value = float(match.group(1))
    unit = match.group(2) or "s"
    if unit == "ms":
        return value / 1000.0
    if unit == "s":
        return value
    if unit == "m":
        return value * 60.0
    if unit == "h":
        return value * 3600.0
    raise ValueError("Unsupported duration unit")


def raise_fd_limit(target: int = 65536) -> None:
    """Raise the file descriptor soft limit for this process and its children."""
    try:
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        new_soft = min(target, hard)
        if soft < new_soft:
            resource.setrlimit(resource.RLIMIT_NOFILE, (new_soft, hard))
            logger.info("Raised fd soft limit from %d to %d (hard=%d)", soft, new_soft, hard)
    except (ValueError, OSError) as exc:
        logger.warning("Failed to raise fd limit: %s", exc)


def run_latency_client(
    source_path: Path,
    service_name: str,
    url: str,
    payload: str,
    duration_seconds: float,
    rate: int,
    output_path: Path,
) -> None:
    raise_fd_limit()
    duration_arg = f"{duration_seconds:.0f}" if duration_seconds.is_integer() else f"{duration_seconds}"
    env = os.environ.copy()
    env["GO111MODULE"] = "off"
    env["GOWORK"] = "off"
    cmd = [
        "go",
        "run",
        source_path.name,
        "-H",
        f"XDN: {service_name}",
        "-H",
        "Content-Type: application/json",
        url,
        payload,
        duration_arg,
        f"{rate}",
    ]
    with open(output_path, "w", encoding="utf-8") as fh:
        result = subprocess.run(
            cmd,
            check=False,
            stdout=fh,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
            cwd=str(source_path.parent),
        )
    if result.returncode != 0:
        try:
            with open(output_path, "r", encoding="utf-8") as fh:
                error_output = fh.read().strip()
        except OSError:
            error_output = ""
        if error_output:
            print("Latency client failed output:\n" + error_output, file=sys.stderr)
        else:
            print("Latency client failed with no output.", file=sys.stderr)
        raise subprocess.CalledProcessError(result.returncode, cmd)


def parse_latency_output(output_path: Path) -> Dict[str, float]:
    try:
        with open(output_path, "r", encoding="utf-8") as fh:
            lines = fh.readlines()
    except OSError as exc:
        raise ValueError(f"Failed to read latency output {output_path}: {exc}") from exc

    metrics: Dict[str, float] = {}
    for line in lines:
        if ":" not in line:
            continue
        key, raw_value = line.split(":", 1)
        key = key.strip()
        raw_value = raw_value.strip()
        try:
            metrics[key] = float(raw_value)
        except ValueError:
            continue

    if not metrics:
        raise ValueError(f"Latency output missing metrics in {output_path}")

    return {
        "throughput_rps": float(metrics.get("actual_throughput_rps", 0.0)),
        "p95_ms": float(metrics.get("p95_latency_ms", 0.0)),
        "p90_ms": float(metrics.get("p90_latency_ms", 0.0)),
        "p50_ms": float(metrics.get("median_latency_ms", 0.0)),
        "avg_ms": float(metrics.get("average_latency_ms", 0.0)),
    }


def write_results_csv(results_dir: Path, rows):
    csv_path = results_dir / "reflex_load_results.csv"
    fieldnames = [
        "rate_rps",
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
        "--rates",
        help="Comma-separated list of request rates (per second) to test",
        default=",".join(str(v) for v in DEFAULT_RATES),
    )
    parser.add_argument("--duration", default="30s", help="Duration for each k6 run (e.g., 30s)")
    parser.add_argument(
        "--service-name",
        default=None,
        help="Optional XDN service name (default: 'svc')",
    )
    parser.add_argument(
        "--median-latency-threshold-ms",
        type=float,
        default=1000.0,
        help="Stop load tests when median latency exceeds this threshold (default: 1000ms)",
    )
    parser.add_argument(
        "--load-generator",
        choices=("k6", "go"),
        default=DEFAULT_LOAD_GENERATOR,
        help="Load generator to use (k6 or get_latency_at_rate.go)",
    )
    parser.add_argument(
        "--jvm-args",
        default="-DSYNC=true",
        help="Extra JVM args to pass to gpServer.sh (e.g. '-DSYNC=true')",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent.parent
    results_base = Path(__file__).resolve().parent / "results"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    config_path = Path(args.xdnConfigFile).resolve()
    active_replicas = parse_active_replicas(config_path)
    control_plane_host, _ = parse_control_plane(config_path)
    target_host, target_port = active_replicas[0]
    http_target_port = target_port + HTTP_PORT_OFFSET
    rates_list = [int(v.strip()) for v in args.rates.split(",") if v.strip()]
    service_name = args.service_name or "svc"
    latency_threshold_ms = args.median_latency_threshold_ms
    post_endpoint, payload_data, app_name, env_vars = get_app_request_config(args.dockerImageName)

    # Create timestamped results directory
    results_dir = results_base / f"load_ar_{app_name}_reflex_{timestamp}"
    results_dir.mkdir(parents=True, exist_ok=True)
    logger.info(
        "Using service name=%s, target=%s:%s (http port %s), rates=%s req/s",
        service_name,
        target_host,
        target_port,
        http_target_port,
        rates_list,
    )
    logger.info("Using request endpoint %s with payload %s", post_endpoint, payload_data)

    go_source: Optional[Path] = None
    if args.load_generator == "go":
        go_source = Path(__file__).resolve().parent / "get_latency_at_rate.go"

    results = []

    for i, rate in enumerate(rates_list):
        logger.info("=" * 60)
        logger.info("  Rate point %s/%s: %s req/s (fresh cluster)", i + 1, len(rates_list), rate)
        logger.info("=" * 60)

        # ── Fresh cluster per rate point ──────────────────────────
        try:
            logger.info("Force-clearing cluster ...")
            stop_cluster(config_path, repo_root)

            logger.info("Starting fresh cluster ...")
            start_cluster(config_path, repo_root, extra_jvm_args=args.jvm_args)
            deploy_service(control_plane_host, args.dockerImageName, service_name, env_vars)
            verify_service(target_host, http_target_port, service_name)

            leader_host, leader_port = detect_leader_replica(active_replicas, service_name)
            leader_http_port = leader_port + HTTP_PORT_OFFSET
            logger.info(
                "Using leader/primary replica %s:%s (http port %s) for load testing",
                leader_host,
                leader_port,
                leader_http_port,
            )

            # ── Load test ─────────────────────────────────────────
            script_path = None
            summary_file = results_dir / f"reflex_k6_summary_rate_{app_name}_{rate}.json"
            latency_output = results_dir / f"rate{rate}.txt"
            try:
                if args.load_generator == "k6":
                    logger.info("Running k6 with target rate %s req/s", rate)
                    script_content = build_k6_script(
                        leader_host,
                        leader_http_port,
                        service_name,
                        rate,
                        args.duration,
                        post_endpoint,
                        payload_data,
                    )
                    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False) as tf:
                        tf.write(script_content)
                        script_path = tf.name
                    run_k6(script_path, summary_file)
                    metrics = parse_k6_summary(summary_file)
                else:
                    duration_seconds = parse_duration_seconds(args.duration)
                    payload_json = json.dumps(payload_data)
                    logger.info("Running Go latency client with target rate %s req/s", rate)
                    run_latency_client(
                        go_source,
                        service_name,
                        f"http://{leader_host}:{leader_http_port}{post_endpoint}",
                        payload_json,
                        duration_seconds,
                        rate,
                        latency_output,
                    )
                    metrics = parse_latency_output(latency_output)
                logger.info("Results for rate=%s: %s", rate, metrics)
                results.append({"rate_rps": rate, **metrics})
            finally:
                if script_path:
                    Path(script_path).unlink(missing_ok=True)

            # ── Early stop on saturation ──────────────────────────
            avg_exceeded = metrics["avg_ms"] > latency_threshold_ms
            tput_dropped = rate > 0 and metrics["throughput_rps"] < 0.75 * rate
            if avg_exceeded and tput_dropped:
                logger.warning(
                    "System saturated at rate %s req/s: "
                    "avg latency %.2f ms > %.0f ms threshold AND "
                    "throughput %.2f rps < 75%% of offered %s rps; stopping.",
                    rate,
                    metrics["avg_ms"],
                    latency_threshold_ms,
                    metrics["throughput_rps"],
                    rate,
                )
                break
            if avg_exceeded:
                logger.warning(
                    "Avg latency %.2f ms > %.0f ms at rate %s req/s (throughput still OK); continuing.",
                    metrics["avg_ms"], latency_threshold_ms, rate,
                )
            if tput_dropped:
                logger.warning(
                    "Throughput %.2f rps < 75%% of offered %s rps (latency still OK); continuing.",
                    metrics["throughput_rps"], rate,
                )
        finally:
            # Always clean up this rate's cluster
            try:
                destroy_service(control_plane_host, service_name)
            except Exception:
                pass
            stop_cluster(config_path, repo_root)

    csv_path = write_results_csv(results_dir, results)
    print(f"Results saved to {csv_path}")


if __name__ == "__main__":
    main()
