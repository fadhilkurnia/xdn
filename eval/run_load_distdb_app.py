import argparse
import concurrent.futures
import csv
import json
import logging
import os
import re
import shlex
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError

ACTIVE_REPLICA_RE = re.compile(r"^\s*active\.[^=]+=\s*([^:\s]+):(\d+)", re.IGNORECASE)
RECONFIGURATOR_RE = re.compile(r"^\s*reconfigurator\.[^=]+=\s*([^:\s]+):(\d+)", re.IGNORECASE)
DEFAULT_RATES = [100, 200, 300, 400, 500, 800, 1000]
DEFAULT_LOAD_GENERATOR = "go"
HTTP_PORT_OFFSET = 300
RQLITE_HTTP_PORT = 4001
RQLITE_RAFT_PORT = 4002
PD_CLIENT_PORT = 2379
PD_PEER_PORT = 2380
TIKV_PORT = 20160
TIKV_STATUS_PORT = 20180
DATA_DIR_PREFIX = "/tmp"
DEFAULT_PD_IMAGE = "pingcap/pd:latest"
DEFAULT_TIKV_IMAGE = "pingcap/tikv:latest"
RQLITED_BIN = "rqlited"
VALID_APPS = {
    "fadhilkurnia/xdn-bookcatalog",
    "fadhilkurnia/xdn-webkv",
    "fadhilkurnia/xdn-todo",
}
logger = logging.getLogger(__name__)


def sanitize_name(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", value)


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


def normalize_service_name(image_name: str, service_name: Optional[str] = None) -> str:
    if service_name:
        return f"rqlite_{sanitize_name(service_name)}"
    base = image_name.split("/")[-1].replace(":", "_")
    base = sanitize_name(base)
    return f"rqlite_{base or 'app'}"


def get_app_request_config(image: str) -> Dict[str, object]:
    if image == "fadhilkurnia/xdn-bookcatalog":
        return {
            "backend": "rqlite",
            "endpoint": "/api/books",
            "payload": {"author": "abc", "title": "xyz"},
            "service": "bookcatalog",
            "env": {"ENABLE_WAL": "true"},
        }
    if image == "fadhilkurnia/xdn-webkv":
        return {
            "backend": "tikv",
            "endpoint": "/api/kv/abc",
            "payload": {"key": "abc", "value": "xyz"},
            "service": "webkv",
            "env": {},
        }
    if image == "fadhilkurnia/xdn-todo":
        return {
            "backend": "rqlite",
            "endpoint": "/api/todo/tasks",
            "payload": {"item": "task"},
            "service": "todo",
            "env": {},
        }

    valid = ", ".join(sorted(VALID_APPS))
    raise ValueError(f"Unsupported application image: {image}; valid options: {valid}")


def is_local_host(host: str) -> bool:
    local_hosts = {"127.0.0.1", "localhost", socket.gethostname()}
    try:
        local_hosts.add(socket.getfqdn())
        local_hosts.add(socket.gethostbyname(socket.gethostname()))
    except socket.error:
        pass
    return host in local_hosts


def run_remote(
    host: str,
    command: str,
    capture_output: bool = False,
    timeout: Optional[int] = None,
    check: bool = True,
):
    kwargs: Dict = {"check": True, "text": True, "stdin": subprocess.DEVNULL}
    if capture_output:
        kwargs["capture_output"] = True
    if timeout is not None:
        kwargs["timeout"] = timeout
    kwargs["check"] = check
    if is_local_host(host):
        return subprocess.run(command, shell=True, **kwargs)
    return subprocess.run(["ssh", host, command], **kwargs)


def parse_control_plane(config_path: Path) -> Tuple[str, int]:
    with open(config_path, "r", encoding="utf-8") as fh:
        for line in fh:
            match = RECONFIGURATOR_RE.match(line)
            if match:
                host, port = match.group(1), int(match.group(2))
                logger.info("Parsed control plane %s:%s from %s", host, port, config_path)
                return host, port
    raise ValueError(f"No reconfigurator entry found in {config_path}")


def start_rqlite_cluster(active_replicas: List[Tuple[str, int]]):
    if len(active_replicas) < 3:
        raise ValueError("At least 3 active replicas are required to start rqlite")
    logger.info("Starting rqlite cluster on replicas: %s", active_replicas[:3])
    leader_host, _ = active_replicas[0]
    processes = []
    leader_raft_port = RQLITE_RAFT_PORT
    for idx, (host, _) in enumerate(active_replicas[:3]):
        node_id = idx + 1
        http_port = RQLITE_HTTP_PORT if idx == 0 else RQLITE_HTTP_PORT + (idx * 2)
        raft_port = RQLITE_RAFT_PORT if idx == 0 else RQLITE_RAFT_PORT + (idx * 2)
        data_dir = f"{DATA_DIR_PREFIX}/rqlite_data_{node_id}"
        log_file = f"{data_dir}/rqlite.log"
        run_remote(host, f"sudo rm -rf {shlex.quote(data_dir)}")
        cmd = (
            f"(sudo fuser -k {RQLITE_HTTP_PORT}/tcp || true) && "
            f"sudo mkdir -p {shlex.quote(data_dir)} && "
            f"sudo chown -R $(id -un):$(id -gn) {shlex.quote(data_dir)} && "
            f"( nohup setsid {RQLITED_BIN} -node-id={node_id} "
            f"-http-addr={host}:{http_port} -raft-addr={host}:{raft_port} "
        )
        if idx > 0:
            cmd += f"-join={leader_host}:{leader_raft_port} "
        cmd += f"{shlex.quote(data_dir)} </dev/null >{shlex.quote(log_file)} 2>&1 & ) && echo $!"
        logger.info("Starting rqlite on %s (http port %s, raft port %s)", host, http_port, raft_port)
        result = run_remote(host, cmd, capture_output=True)
        pid = result.stdout.strip() if result.stdout else ""
        processes.append(
            {"host": host, "pid": pid, "data_dir": data_dir, "http_port": http_port, "raft_port": raft_port}
        )
        time.sleep(1.5)
    logger.info("Waiting for rqlite cluster to stabilize...")
    time.sleep(3)
    return processes


def stop_rqlite_cluster(processes):
    for proc in processes:
        host = proc["host"]
        data_dir = proc.get("data_dir")
        http_port = proc.get("http_port")
        raft_port = proc.get("raft_port")
        cleanup_cmds = []
        if http_port:
            cleanup_cmds.append(f"sudo fuser -k {http_port}/tcp >/dev/null 2>&1 || true")
        if raft_port:
            cleanup_cmds.append(f"sudo fuser -k {raft_port}/tcp >/dev/null 2>&1 || true")
        cleanup_cmds.append("pkill -f rqlited >/dev/null 2>&1 || true")
        if data_dir:
            cleanup_cmds.append(f"sudo rm -rf {shlex.quote(data_dir)}")
        run_remote(host, " ; ".join(cleanup_cmds), check=False)


def start_tikv_cluster(
    reconfigurator_host: str, active_replicas: List[Tuple[str, int]], service_label: str
):
    if len(active_replicas) < 3:
        raise ValueError("At least 3 active replicas are required to start TiKV")
    label = sanitize_name(service_label or "svc")
    pd_data_dir = f"{DATA_DIR_PREFIX}/pd_data_{label}"
    pd_log_file = f"{DATA_DIR_PREFIX}/tikv_pd_{label}.log"
    pd_container = f"tikv_pd_{label}"
    pd_cmd = (
        f"(sudo fuser -k {PD_CLIENT_PORT}/tcp >/dev/null 2>&1 || true); "
        f"(sudo fuser -k {PD_PEER_PORT}/tcp >/dev/null 2>&1 || true); "
        f"docker rm -f {shlex.quote(pd_container)} >/dev/null 2>&1 || true; "
        f"mkdir -p {shlex.quote(pd_data_dir)}; "
        f"docker run -d "
        f"-p {PD_CLIENT_PORT}:{PD_CLIENT_PORT} -p {PD_PEER_PORT}:{PD_PEER_PORT} "
        f"--name {shlex.quote(pd_container)} "
        f"-v {shlex.quote(pd_data_dir)}:/data "
        f"{shlex.quote(DEFAULT_PD_IMAGE)} "
        f"--name=pd --data-dir=/data "
        f"--client-urls=http://0.0.0.0:{PD_CLIENT_PORT} "
        f"--peer-urls=http://0.0.0.0:{PD_PEER_PORT} "
        f"--advertise-client-urls=http://{reconfigurator_host}:{PD_CLIENT_PORT} "
        f"--advertise-peer-urls=http://{reconfigurator_host}:{PD_PEER_PORT} "
        f"--initial-cluster=pd=http://{reconfigurator_host}:{PD_PEER_PORT} "
        f"</dev/null >{shlex.quote(pd_log_file)} 2>&1"
    )
    logger.info("Starting PD on %s", reconfigurator_host)
    run_remote(reconfigurator_host, pd_cmd)

    tikv_nodes = []
    for idx, (host, _) in enumerate(active_replicas[:3], start=1):
        data_dir = f"{DATA_DIR_PREFIX}/tikv_data_{label}_{idx}"
        log_file = f"{DATA_DIR_PREFIX}/tikv_{label}_{idx}.log"
        container_name = f"tikv_{label}_{idx}"
        cmd = (
            f"(sudo fuser -k {TIKV_PORT}/tcp >/dev/null 2>&1 || true); "
            f"(sudo fuser -k {TIKV_STATUS_PORT}/tcp >/dev/null 2>&1 || true); "
            f"docker rm -f {shlex.quote(container_name)} >/dev/null 2>&1 || true; "
            f"mkdir -p {shlex.quote(data_dir)}; "
            f"docker run -d "
            f"-p {TIKV_PORT}:{TIKV_PORT} -p {TIKV_STATUS_PORT}:{TIKV_STATUS_PORT} "
            f"--name {shlex.quote(container_name)} "
            f"-v {shlex.quote(data_dir)}:/data "
            f"{shlex.quote(DEFAULT_TIKV_IMAGE)} "
            f"--addr=0.0.0.0:{TIKV_PORT} "
            f"--advertise-addr={host}:{TIKV_PORT} "
            f"--status-addr=0.0.0.0:{TIKV_STATUS_PORT} "
            f"--pd={reconfigurator_host}:{PD_CLIENT_PORT} "
            f"--data-dir=/data "
            f"</dev/null >{shlex.quote(log_file)} 2>&1"
        )
        logger.info("Starting TiKV node %s on %s", idx, host)
        run_remote(host, cmd)
        tikv_nodes.append(
            {
                "host": host,
                "container": container_name,
                "data_dir": data_dir,
                "log_file": log_file,
            }
        )
        time.sleep(1)

    return {
        "pd_host": reconfigurator_host,
        "pd_container": pd_container,
        "pd_data_dir": pd_data_dir,
        "tikv_nodes": tikv_nodes,
    }


def wait_for_pd(host: str, timeout: int = 180, interval: float = 2.0):
    url = f"http://{host}:{PD_CLIENT_PORT}/pd/api/v1/health"
    deadline = time.time() + timeout
    last_error: Optional[Exception] = None
    logger.info("Waiting for PD at %s", url)
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                data = json.load(resp)
                health_entries = data if isinstance(data, list) else data.get("health") if isinstance(data, dict) else []
                if isinstance(health_entries, dict):
                    health_entries = [health_entries]
                healthy = any(entry.get("health") for entry in health_entries) if health_entries else False
                if healthy:
                    logger.info("PD is healthy at %s", url)
                    return
        except Exception as exc:
            last_error = exc
        time.sleep(interval)
    raise TimeoutError(f"PD not ready at {url}; last error: {last_error}")


def wait_for_tikv(pd_host: str, expected_nodes: int, timeout: int = 600, interval: float = 3.0):
    url = f"http://{pd_host}:{PD_CLIENT_PORT}/pd/api/v1/stores"
    deadline = time.time() + timeout
    last_error: Optional[Exception] = None

    def store_up(store_entry: dict) -> bool:
        candidates = []
        for container in ("status", "store"):
            section = store_entry.get(container, {})
            for key in ("state_name", "stateName"):
                if key in section:
                    candidates.append(str(section.get(key)).lower())
        return any(val == "up" for val in candidates)

    logger.info("Waiting for %s TiKV nodes to be up via %s", expected_nodes, url)
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                data = json.load(resp)
            stores = data.get("stores", [])
            up_count = sum(1 for store in stores if store_up(store))
            if up_count >= expected_nodes:
                logger.info("Detected %s/%s TiKV nodes up", up_count, expected_nodes)
                return
        except Exception as exc:
            last_error = exc
        time.sleep(interval)
    raise TimeoutError(f"TiKV nodes not ready via {url}; last error: {last_error}")


def stop_tikv_cluster(cluster):
    if not cluster:
        return
    tikv_nodes = cluster.get("tikv_nodes", [])
    for node in tikv_nodes:
        host = node.get("host")
        if not host:
            continue
        container = node.get("container")
        data_dir = node.get("data_dir")
        cleanup_cmds = []
        if container:
            cleanup_cmds.append(f"docker rm -f {shlex.quote(container)} >/dev/null 2>&1 || true")
        cleanup_cmds.append(f"sudo fuser -k {TIKV_PORT}/tcp >/dev/null 2>&1 || true")
        cleanup_cmds.append(f"sudo fuser -k {TIKV_STATUS_PORT}/tcp >/dev/null 2>&1 || true")
        if data_dir:
            cleanup_cmds.append(f"sudo rm -rf {shlex.quote(data_dir)}")
        try:
            run_remote(host, " ; ".join(cleanup_cmds), check=False)
        except subprocess.CalledProcessError as exc:
            logger.warning("Failed to clean up TiKV on %s: %s", host, exc)

    pd_host = cluster.get("pd_host")
    pd_container = cluster.get("pd_container")
    pd_data_dir = cluster.get("pd_data_dir")
    if pd_host:
        cleanup_cmds = []
        if pd_container:
            cleanup_cmds.append(f"docker rm -f {shlex.quote(pd_container)} >/dev/null 2>&1 || true")
        cleanup_cmds.append(f"sudo fuser -k {PD_CLIENT_PORT}/tcp >/dev/null 2>&1 || true")
        cleanup_cmds.append(f"sudo fuser -k {PD_PEER_PORT}/tcp >/dev/null 2>&1 || true")
        if pd_data_dir:
            cleanup_cmds.append(f"sudo rm -rf {shlex.quote(pd_data_dir)}")
        try:
            run_remote(pd_host, " ; ".join(cleanup_cmds), check=False)
        except subprocess.CalledProcessError as exc:
            logger.warning("Failed to clean up PD on %s: %s", pd_host, exc)


def wait_for_rqlite(host: str, port: int, timeout: int = 30, interval: float = 1.0):
    logger.info("Waiting for rqlite at %s:%s", host, port)
    url = f"http://{host}:{port}/status"
    deadline = time.time() + timeout
    last_error: Optional[Exception] = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                if resp.status < 500:
                    return
        except Exception as exc:
            last_error = exc
        time.sleep(interval)
    raise TimeoutError(f"rqlite not ready at {url}; last error: {last_error}")


def start_app_container(
    host: str, image: str, container_name: str, http_port: int, env_vars: Optional[Dict[str, str]] = None
):
    logger.info("Starting app container on %s", host)
    env_parts = []
    for key, value in (env_vars or {}).items():
        env_parts.append(f"-e {shlex.quote(f'{key}={value}')}")
    env_arg = " ".join(env_parts)
    cmd = (
        f"docker rm -f {shlex.quote(container_name)} >/dev/null 2>&1 || true; "
        f"docker run -d --rm "
        f"{env_arg} "
        f"-p {http_port}:80 --name {shlex.quote(container_name)} {shlex.quote(image)}"
    )
    run_remote(host, cmd)


def stop_app_container(host: str, container_name: str):
    try:
        run_remote(host, f"docker rm -f {shlex.quote(container_name)} >/dev/null 2>&1 || true")
    except subprocess.CalledProcessError as exc:
        logger.warning("Failed to stop container %s on %s: %s", container_name, host, exc)


def build_k6_script(
    host: str, port: int, rate: int, duration: str, endpoint: str, payload_data: Dict[str, str]
) -> str:
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

const PAYLOAD = {payload};

export default function () {{
  http.post('http://{host}:{port}{endpoint}', PAYLOAD);
}}
"""


def verify_service(host: str, port: int, timeout: int = 60, interval: float = 2.0):
    url = f"http://{host}:{port}/"
    deadline = time.time() + timeout
    last_error: Optional[Exception] = None
    attempt = 0
    logger.info("Waiting for service at %s (timeout=%ss, interval=%ss)", url, timeout, interval)
    while time.time() < deadline:
        attempt += 1
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                if resp.status < 500:
                    logger.info("Service is up at %s (status %s, attempts=%s)", url, resp.status, attempt)
                    return
        except HTTPError as exc:
            if exc.code < 500:
                logger.info("Service is up at %s (status %s, attempts=%s)", url, exc.code, attempt)
                return
            last_error = exc
        except URLError as exc:
            last_error = exc
        except Exception as exc:
            last_error = exc
        time.sleep(interval)
    raise TimeoutError(f"Service not ready at {url}; last error: {last_error}")


def run_k6(script_path: str, summary_path: Path):
    cmd = ["k6", "run", "--summary-export", str(summary_path), script_path]
    subprocess.run(cmd, check=True)


def parse_k6_summary(summary_path: Path):
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


def run_latency_client(
    source_path: Path,
    url: str,
    payload: str,
    duration_seconds: float,
    rate: int,
    output_path: Path,
) -> None:
    duration_arg = f"{duration_seconds:.0f}" if duration_seconds.is_integer() else f"{duration_seconds}"
    env = os.environ.copy()
    env["GO111MODULE"] = "off"
    env["GOWORK"] = "off"
    cmd = ["go", "run", source_path.name, url, payload, duration_arg, f"{rate}"]
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


def write_results_csv(results_dir: Path, rows, backend: str):
    csv_path = results_dir / f"{backend}_load_results.csv"
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
        description="Launch an app backed by a distributed DB (rqlite or TiKV) and measure load with k6 or Go.",
    )
    parser.add_argument("xdnConfigFile", help="Path to gigapaxos.xdn.*.properties file")
    parser.add_argument("dockerImageName", help="Docker image for the service")
    parser.add_argument(
        "--rates",
        help="Comma-separated list of request rates (per second) to test",
        default=",".join(str(v) for v in DEFAULT_RATES),
    )
    parser.add_argument("--duration", default="30s", help="Duration for each k6 run (e.g., 30s)")
    parser.add_argument(
        "--service-name",
        default=None,
        help="Optional service name (used for container naming)",
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
    args = parser.parse_args()

    results_dir = Path(__file__).resolve().parent / "results"
    results_dir.mkdir(parents=True, exist_ok=True)

    config_path = Path(args.xdnConfigFile).resolve()
    app_config = get_app_request_config(args.dockerImageName)
    backend = str(app_config.get("backend", "rqlite"))
    endpoint = str(app_config.get("endpoint", "/"))
    payload_data = dict(app_config.get("payload", {}))
    default_service = str(app_config.get("service") or "svc")
    app_env = dict(app_config.get("env", {}))
    service_name = args.service_name or default_service
    service_label = sanitize_name(service_name)
    container_name = normalize_service_name(args.dockerImageName, service_name)

    active_replicas = parse_active_replicas(config_path)
    target_host, target_port = active_replicas[0]
    http_target_port = target_port + HTTP_PORT_OFFSET
    rates_list = [int(v.strip()) for v in args.rates.split(",") if v.strip()]
    latency_threshold_ms = args.median_latency_threshold_ms
    go_source: Optional[Path] = None
    if args.load_generator == "go":
        go_source = Path(__file__).resolve().parent / "get_latency_at_rate.go"
    request_url = f"http://{target_host}:{http_target_port}{endpoint}"

    logger.info(
        "Using service name=%s (backend=%s), target=%s:%s (http port %s), endpoint=%s, rates=%s req/s",
        service_name,
        backend,
        target_host,
        target_port,
        http_target_port,
        endpoint,
        rates_list,
    )

    rqlite_processes = []
    tikv_cluster = {}
    pd_host: Optional[str] = None
    try:
        db_env: Dict[str, str] = {}
        if backend == "rqlite":
            rqlite_processes = start_rqlite_cluster(active_replicas)
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(rqlite_processes)) as executor:
                futures = [
                    executor.submit(wait_for_rqlite, proc["host"], proc["http_port"])
                    for proc in rqlite_processes
                ]
                for future in concurrent.futures.as_completed(futures):
                    future.result()
            db_env = {"DB_TYPE": "rqlite", "DB_HOST": rqlite_processes[0]["host"]}
        elif backend == "tikv":
            pd_host, _ = parse_control_plane(config_path)
            tikv_cluster = start_tikv_cluster(pd_host, active_replicas, service_label)
            wait_for_pd(pd_host)
            wait_for_tikv(pd_host, expected_nodes=min(len(active_replicas), 3))
            pd_addr = f"{pd_host}:{PD_CLIENT_PORT}"
            db_env = {"DB_TYPE": "tikv", "DB_HOST": pd_addr, "TIKV_PD_ADDR": pd_addr}
        else:
            raise ValueError(f"Unsupported backend: {backend}")

        combined_env = {**app_env, **db_env}
        start_app_container(
            target_host,
            args.dockerImageName,
            container_name,
            http_target_port,
            combined_env,
        )
        verify_service(target_host, http_target_port)

        results = []
        for rate in rates_list:
            script_path = None
            summary_file = results_dir / f"{backend}_k6_summary_rate{rate}.json"
            latency_output = results_dir / f"{backend}_go_latency_rate{rate}.txt"
            try:
                if args.load_generator == "k6":
                    logger.info("Running k6 with target rate %s req/s", rate)
                    script_content = build_k6_script(
                        target_host, http_target_port, rate, args.duration, endpoint, payload_data
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
                        request_url,
                        payload_json,
                        duration_seconds,
                        rate,
                        latency_output,
                    )
                    metrics = parse_latency_output(latency_output)
                logger.info("Results for rate=%s: %s", rate, metrics)
                results.append({"rate_rps": rate, **metrics})
                if metrics["p50_ms"] > latency_threshold_ms:
                    logger.warning(
                        "Median latency %.2f ms exceeded threshold %.2f ms at rate %s req/s; stopping further tests",
                        metrics["p50_ms"],
                        latency_threshold_ms,
                        rate,
                    )
                    break
            finally:
                if script_path:
                    Path(script_path).unlink(missing_ok=True)

        csv_path = write_results_csv(results_dir, results, backend)
        print(f"Results saved to {csv_path}")
    finally:
        stop_app_container(target_host, container_name)
        if backend == "rqlite":
            stop_rqlite_cluster(rqlite_processes)
        else:
            stop_tikv_cluster(tikv_cluster)


if __name__ == "__main__":
    main()
