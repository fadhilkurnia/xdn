import argparse
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
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError

ACTIVE_REPLICA_RE = re.compile(r"^\s*active\.[^=]+=\s*([^:\s]+):(\d+)", re.IGNORECASE)
DEFAULT_RATES = [100, 200, 300, 400, 500, 800, 1000]
DEFAULT_LOAD_GENERATOR = "go"
DEFAULT_DRBD_PORT = 7789
HTTP_PORT_OFFSET = 300
VALID_APPS = {
    "fadhilkurnia/xdn-bookcatalog",
    "fadhilkurnia/xdn-webkv",
    "fadhilkurnia/xdn-todo",
}
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DrbdNode:
    name: str
    address: str
    node_id: int


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
    if len(actives) < 3:
        raise ValueError(f"Expected at least 3 active replicas in {config_path}")
    logger.info("Parsed %s active replicas from %s", len(actives), config_path)
    return actives


def normalize_container_name(image_name: str, service_name: Optional[str] = None) -> str:
    if service_name:
        return f"drbd_{sanitize_name(service_name)}"
    base = image_name.split("/")[-1].replace(":", "_")
    base = sanitize_name(base)
    return f"drbd_{base or 'app'}"


def get_app_config(image: str) -> Dict[str, object]:
    if image not in VALID_APPS:
        valid = ", ".join(sorted(VALID_APPS))
        raise ValueError(f"Unsupported application image: {image}; valid options: {valid}")
    if image == "fadhilkurnia/xdn-bookcatalog":
        return {
            "endpoint": "/api/books",
            "payload": {"author": "abc", "title": "xyz"},
            "service": "bookcatalog",
            "env": {"ENABLE_WAL": "true"},
            "container_mount": "/app/data",
        }
    if image == "fadhilkurnia/xdn-webkv":
        return {
            "endpoint": "/api/kv/abc",
            "payload": {"key": "abc", "value": "xyz"},
            "service": "webkv",
            "env": {},
            "container_mount": "/data",
        }
    return {
        "endpoint": "/api/todo/tasks",
        "payload": {"item": "task"},
        "service": "todo",
        "env": {},
        "container_mount": "/app/data",
    }


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
    input_text: Optional[str] = None,
) -> subprocess.CompletedProcess:
    kwargs: Dict[str, object] = {"check": check, "text": True, "stdin": subprocess.DEVNULL}
    if capture_output:
        kwargs["capture_output"] = True
    if timeout is not None:
        kwargs["timeout"] = timeout
    if input_text is not None:
        kwargs["input"] = input_text
        kwargs["stdin"] = subprocess.PIPE
    if is_local_host(host):
        return subprocess.run(command, shell=True, **kwargs)
    return subprocess.run(["ssh", host, command], **kwargs)


def get_remote_hostname(host: str) -> str:
    try:
        result = run_remote(host, "hostname -s", capture_output=True, timeout=5)
        name = (result.stdout or "").strip()
        if name:
            return name
    except subprocess.CalledProcessError:
        pass
    return sanitize_name(host)


def build_drbd_resource_config(
    resource_name: str,
    nodes: List[DrbdNode],
    drbd_device: str,
    backing_device: str,
    drbd_port: int,
) -> str:
    lines = [f"resource {resource_name} {{"]
    lines.append("  net {")
    lines.append("    protocol C;")
    lines.append("  }")
    lines.append("  options {")
    lines.append("    auto-promote no;")
    lines.append("  }")
    lines.append("  volume 0 {")
    lines.append(f"    device {drbd_device};")
    lines.append(f"    disk {backing_device};")
    lines.append("    meta-disk internal;")
    lines.append("  }")
    lines.append("  connection-mesh {")
    lines.append(f"    hosts {' '.join(node.name for node in nodes)};")
    lines.append("  }")
    for node in nodes:
        lines.append(f"  on {node.name} {{")
        lines.append(f"    node-id {node.node_id};")
        lines.append(f"    address {node.address}:{drbd_port};")
        lines.append("  }")
    lines.append("}")
    return "\n".join(lines) + "\n"


def write_drbd_config(host: str, resource_name: str, contents: str) -> None:
    config_path = f"/etc/drbd.d/{resource_name}.res"
    run_remote(host, "sudo mkdir -p /etc/drbd.d")
    run_remote(host, f"sudo tee {shlex.quote(config_path)} >/dev/null", input_text=contents)


def wait_for_block_device(host: str, device_path: str, timeout: int = 60, interval: float = 2.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = run_remote(host, f"test -b {shlex.quote(device_path)}", check=False)
        if result.returncode == 0:
            return
        time.sleep(interval)
    raise TimeoutError(f"Timed out waiting for block device {device_path} on {host}")


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


def write_results_csv(results_dir: Path, rows) -> Path:
    csv_path = results_dir / "drbd_load_results.csv"
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


def start_app_container(
    host: str,
    image: str,
    container_name: str,
    http_port: int,
    host_mount: str,
    container_mount: str,
    env_vars: Optional[Dict[str, str]] = None,
) -> None:
    env_parts = []
    for key, value in (env_vars or {}).items():
        env_parts.append(f"-e {shlex.quote(f'{key}={value}')}")
    env_arg = " ".join(env_parts)
    volume_arg = f"-v {shlex.quote(host_mount)}:{shlex.quote(container_mount)}"
    cmd = (
        f"docker rm -f {shlex.quote(container_name)} >/dev/null 2>&1 || true; "
        f"docker run -d --rm "
        f"{env_arg} "
        f"{volume_arg} "
        f"-p {http_port}:80 --name {shlex.quote(container_name)} {shlex.quote(image)}"
    )
    run_remote(host, cmd)


def stop_app_container(host: str, container_name: str):
    try:
        run_remote(host, f"docker rm -f {shlex.quote(container_name)} >/dev/null 2>&1 || true")
    except subprocess.CalledProcessError as exc:
        logger.warning("Failed to stop container %s on %s: %s", container_name, host, exc)


def main():
    log_level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_name, logging.INFO)
    logging.basicConfig(level=log_level, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(
        description=(
            "Create a 3-node DRBD cluster, mount the replicated block device, "
            "run an app container on the primary node, and optionally generate load."
        ),
    )
    parser.add_argument("xdnConfigFile", help="Path to gigapaxos.xdn.*.properties file")
    parser.add_argument("dockerImageName", help="Docker image for the service")
    parser.add_argument(
        "--resource-name",
        default="xdn_drbd",
        help="DRBD resource name (default: xdn_drbd)",
    )
    parser.add_argument(
        "--backing-device",
        required=True,
        help="Block device on each host to back DRBD (e.g., /dev/sdb)",
    )
    parser.add_argument(
        "--drbd-device",
        default="/dev/drbd0",
        help="DRBD device path to create (default: /dev/drbd0)",
    )
    parser.add_argument(
        "--drbd-port",
        type=int,
        default=DEFAULT_DRBD_PORT,
        help=f"DRBD replication port (default: {DEFAULT_DRBD_PORT})",
    )
    parser.add_argument(
        "--primary-host",
        default=None,
        help="Host to promote to primary (default: first active replica)",
    )
    parser.add_argument(
        "--mountpoint",
        default=None,
        help="Mount point on the primary host (default: /mnt/drbd/<resource-name>)",
    )
    parser.add_argument(
        "--fs-type",
        default="ext4",
        help="Filesystem type for the DRBD device (default: ext4)",
    )
    parser.add_argument(
        "--skip-format",
        action="store_true",
        help="Skip mkfs on the DRBD device (use if it already has a filesystem)",
    )
    parser.add_argument(
        "--force-create-md",
        action="store_true",
        help="Pass --force to drbdadm create-md (may destroy existing metadata)",
    )
    parser.add_argument(
        "--force-primary",
        action="store_true",
        help="Pass --force when promoting primary (needed on first initialization)",
    )
    parser.add_argument(
        "--rates",
        help="Comma-separated list of request rates (per second) to test",
        default=",".join(str(v) for v in DEFAULT_RATES),
    )
    parser.add_argument("--duration", default="30s", help="Duration for each load run (e.g., 30s)")
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
    parser.add_argument(
        "--skip-load",
        action="store_true",
        help="Skip the load test phase",
    )
    parser.add_argument(
        "--keep-drbd",
        action="store_true",
        help="Skip DRBD teardown and unmount on exit",
    )
    parser.add_argument(
        "--http-port",
        type=int,
        default=None,
        help="HTTP port to expose the app (default: active port + 300)",
    )
    args = parser.parse_args()

    results_dir = Path(__file__).resolve().parent / "results"
    results_dir.mkdir(parents=True, exist_ok=True)

    config_path = Path(args.xdnConfigFile).resolve()
    app_config = get_app_config(args.dockerImageName)
    endpoint = str(app_config.get("endpoint", "/"))
    payload_data = dict(app_config.get("payload", {}))
    default_service = str(app_config.get("service") or "svc")
    app_env = dict(app_config.get("env", {}))
    container_mount = str(app_config.get("container_mount", "/data"))
    service_name = args.service_name or default_service
    container_name = normalize_container_name(args.dockerImageName, service_name)

    active_replicas = parse_active_replicas(config_path)
    drbd_hosts = [host for host, _ in active_replicas[:3]]
    primary_host = args.primary_host or drbd_hosts[0]
    if primary_host not in drbd_hosts:
        raise ValueError(f"primary host {primary_host} is not one of the first three active replicas")
    primary_port = active_replicas[drbd_hosts.index(primary_host)][1]
    http_port = args.http_port or (primary_port + HTTP_PORT_OFFSET)
    mountpoint = args.mountpoint or f"/mnt/drbd/{sanitize_name(args.resource_name)}"
    rates_list = [int(v.strip()) for v in args.rates.split(",") if v.strip()]
    latency_threshold_ms = args.median_latency_threshold_ms
    go_source: Optional[Path] = None
    if args.load_generator == "go":
        go_source = Path(__file__).resolve().parent / "get_latency_at_rate.go"
    request_url = f"http://{primary_host}:{http_port}{endpoint}"

    logger.info(
        "Using DRBD resource=%s, backing=%s, device=%s, primary=%s, mountpoint=%s",
        args.resource_name,
        args.backing_device,
        args.drbd_device,
        primary_host,
        mountpoint,
    )
    logger.info(
        "Running app %s on %s:%s (endpoint=%s, rates=%s req/s)",
        args.dockerImageName,
        primary_host,
        http_port,
        endpoint,
        rates_list,
    )

    nodes: List[DrbdNode] = []
    for idx, host in enumerate(drbd_hosts):
        node_name = get_remote_hostname(host)
        nodes.append(DrbdNode(name=node_name, address=host, node_id=idx))

    resource_config = build_drbd_resource_config(
        args.resource_name,
        nodes,
        args.drbd_device,
        args.backing_device,
        args.drbd_port,
    )

    for host in drbd_hosts:
        write_drbd_config(host, args.resource_name, resource_config)

    create_md_cmd = f"sudo drbdadm create-md {shlex.quote(args.resource_name)}"
    if args.force_create_md:
        create_md_cmd = f"sudo drbdadm create-md --force {shlex.quote(args.resource_name)}"

    try:
        for host in drbd_hosts:
            run_remote(host, create_md_cmd)
        for host in drbd_hosts:
            run_remote(host, f"sudo drbdadm up {shlex.quote(args.resource_name)}")

        primary_cmd = f"sudo drbdadm primary {shlex.quote(args.resource_name)}"
        if args.force_primary:
            primary_cmd = f"sudo drbdadm primary --force {shlex.quote(args.resource_name)}"
        run_remote(primary_host, primary_cmd)

        wait_for_block_device(primary_host, args.drbd_device, timeout=90)

        if not args.skip_format:
            run_remote(primary_host, f"sudo mkfs -t {shlex.quote(args.fs_type)} {shlex.quote(args.drbd_device)}")

        run_remote(primary_host, f"sudo mkdir -p {shlex.quote(mountpoint)}")
        run_remote(primary_host, f"sudo mount {shlex.quote(args.drbd_device)} {shlex.quote(mountpoint)}")

        start_app_container(
            primary_host,
            args.dockerImageName,
            container_name,
            http_port,
            mountpoint,
            container_mount,
            app_env,
        )
        verify_service(primary_host, http_port)

        if args.skip_load:
            logger.info("Skipping load generation (--skip-load).")
            return

        results = []
        for rate in rates_list:
            script_path = None
            summary_file = results_dir / f"drbd_k6_summary_rate{rate}.json"
            latency_output = results_dir / f"drbd_go_latency_rate{rate}.txt"
            try:
                if args.load_generator == "k6":
                    logger.info("Running k6 with target rate %s req/s", rate)
                    script_content = build_k6_script(primary_host, http_port, rate, args.duration, endpoint, payload_data)
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
                        "Median latency %.2f ms exceeded threshold %.2f ms at rate %s req/s; stopping",
                        metrics["p50_ms"],
                        latency_threshold_ms,
                        rate,
                    )
                    break
            finally:
                if script_path:
                    Path(script_path).unlink(missing_ok=True)

        csv_path = write_results_csv(results_dir, results)
        print(f"Results saved to {csv_path}")
    finally:
        stop_app_container(primary_host, container_name)
        if args.keep_drbd:
            logger.info("Keeping DRBD resources and mountpoints (--keep-drbd).")
            return
        try:
            run_remote(primary_host, f"sudo umount {shlex.quote(mountpoint)}", check=False)
        except subprocess.CalledProcessError:
            logger.warning("Failed to unmount %s on %s", mountpoint, primary_host)
        for host in drbd_hosts:
            run_remote(host, f"sudo drbdadm secondary {shlex.quote(args.resource_name)}", check=False)
            run_remote(host, f"sudo drbdadm down {shlex.quote(args.resource_name)}", check=False)


if __name__ == "__main__":
    main()
