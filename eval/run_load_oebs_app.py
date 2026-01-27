import argparse
import csv
import json
import logging
import math
import os
import re
import socket
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Dict, List, Optional
import pandas as pd

DEFAULT_RATE_RPS = 300
APP_LABEL = "app"
K8S_STORAGE_CLASS = "openebs-3-replica"
logger = logging.getLogger(__name__)

def normalize_name(image: str) -> str:
    base = image.split("/")[-1].split(":")[0].lower()
    base = re.sub(r"[^a-z0-9-]", "-", base) or "app"
    return f"oebs-{base}"


def run_cmd(cmd: List[str], manifest: str = "") -> None:
    kwargs: Dict[str, object] = {"check": True}
    if manifest:
        kwargs.update({"text": True, "input": manifest})
    logger.debug("Running command: %s", " ".join(cmd))
    subprocess.run(cmd, **kwargs)


def parse_duration_seconds(duration: str) -> float:
    match = re.fullmatch(r"(?i)(\d+(?:\.\d+)?)([smh]?)", duration.strip())
    if not match:
        raise ValueError(f"Unsupported duration format: {duration}")
    value = float(match.group(1))
    unit = match.group(2).lower()
    if unit == "m":
        value *= 60
    elif unit == "h":
        value *= 3600
    if value <= 0:
        raise ValueError("Duration must be positive")
    return value


def percentile(values: List[float], p: float) -> float:
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    idx = (len(sorted_vals) - 1) * p
    lower = math.floor(idx)
    upper = math.ceil(idx)
    if lower == upper:
        return sorted_vals[int(idx)]
    return sorted_vals[lower] + (sorted_vals[upper] - sorted_vals[lower]) * (idx - lower)


def create_pvc(name: str, namespace: str, storage: str) -> None:
    manifest = f"""
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: {name}
  namespace: {namespace}
spec:
  accessModes:
  - ReadWriteOnce
  resources:
    requests:
      storage: {storage}
  storageClassName: {K8S_STORAGE_CLASS}
"""
    run_cmd(["kubectl", "apply", "-f", "-"], manifest)


# TODO: create deployment for another app based on the image name, add another one for fadhilkurnia/xdn-restkv
def create_deployment(name: str, image: str, pvc_name: str, namespace: str) -> None:
    manifest = f"""
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {name}
  namespace: {namespace}
spec:
  replicas: 1
  selector:
    matchLabels:
      {APP_LABEL}: {name}
  template:
    metadata:
      labels:
        {APP_LABEL}: {name}
    spec:
      containers:
      - name: {name}
        image: {image}
        ports:
        - containerPort: 80
        volumeMounts:
        - name: data
          mountPath: /app/data
      volumes:
      - name: data
        persistentVolumeClaim:
          claimName: {pvc_name}
"""
    run_cmd(["kubectl", "apply", "-f", "-"], manifest)
    run_cmd(["kubectl", "-n", namespace, "rollout", "status", f"deployment/{name}", "--timeout=120s"])


def create_service(name: str, namespace: str) -> None:
    manifest = f"""
apiVersion: v1
kind: Service
metadata:
  name: {name}
  namespace: {namespace}
spec:
  selector:
    {APP_LABEL}: {name}
  ports:
  - port: 80
    targetPort: 80
"""
    run_cmd(["kubectl", "apply", "-f", "-"], manifest)


def delete_resource(kind: str, name: str, namespace: str) -> None:
    try:
        run_cmd(["kubectl", "-n", namespace, "delete", kind, name, "--ignore-not-found=true"])
    except subprocess.CalledProcessError:
        logger.warning("Failed to delete %s/%s in namespace %s", kind, name, namespace)


def start_port_forward(service: str, namespace: str, local_port: int) -> subprocess.Popen:
    cmd = ["kubectl", "-n", namespace, "port-forward", f"svc/{service}", f"{local_port}:80"]
    logger.info("Port-forwarding svc/%s to localhost:%s", service, local_port)
    return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def wait_for_port(host: str, port: int, timeout: int = 60) -> None:
    deadline = time.time() + timeout
    last_error: Optional[Exception] = None
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1.0):
                return
        except OSError as exc:
            last_error = exc
            time.sleep(0.3)
    raise TimeoutError(f"Port {host}:{port} not ready: {last_error}")


def generate_ramp_rates(warmup_rate: int, peak_rate: int, ramp_steps: int) -> List[int]:
    if ramp_steps < 2:
        raise ValueError("ramp_steps must be at least 2")
    increment = max(1, (peak_rate - warmup_rate) // (ramp_steps - 1))
    rates = [warmup_rate + i * increment for i in range(ramp_steps)]
    rates[-1] = peak_rate
    return rates

def build_k6_vu_script(
    url: str, stage_duration: str, median_threshold_ms: float, warmup_rate: int, peak_rate: int, ramp_steps: int
) -> str:
    max_vus = peak_rate
    start_vus = max(1, warmup_rate)
    return f"""
import http from 'k6/http';

export const options = {{
  scenarios: {{
    smooth_ramp: {{
      executor: 'ramping-vus',
      startVUs: {start_vus},
      stages: [
        {{ target: {max_vus}, duration: '30m' }},
        {{ target: {max_vus}, duration: '1m' }},
      ],      
      gracefulRampDown: '{stage_duration}',
    }},
  }},
  thresholds: {{
    http_req_duration: [
      {{ threshold: 'p(50)<{median_threshold_ms}', abortOnFail: true, delayAbortEval: '{stage_duration}' }},
      'p(95)<500',
    ],
  }},
}};

const HEADERS = {{ 'XDN': 'bookcatalog' }};
const PAYLOAD = {{ 'author': 'abc', 'title': 'xyz' }};

export default function () {{
  http.post('{url}api/books', PAYLOAD, {{ headers: HEADERS }});
}}
"""

def build_k6_constant_rate_script(url: str, stage_duration: str, median_threshold_ms: float, rate: int) -> str:
    pre_allocated = max(50, rate)
    max_vus = max(pre_allocated * 2, rate * 2)
    return f"""
import http from 'k6/http';

export const options = {{
  scenarios: {{
    fixed_rate: {{
      executor: 'constant-arrival-rate',
      rate: {rate},
      timeUnit: '1s',
      duration: '{stage_duration}',
      preAllocatedVUs: {pre_allocated},
      maxVUs: {max_vus},
    }},
  }},
  thresholds: {{
    http_req_duration: [
      {{ threshold: 'p(50)<{median_threshold_ms}', abortOnFail: true, delayAbortEval: '{stage_duration}' }},
      'p(95)<500',
    ],
  }},
}};

const HEADERS = {{ 'XDN': 'bookcatalog' }};
const PAYLOAD = {{ 'author': 'abc', 'title': 'xyz' }};

export default function () {{
  http.post('{url}api/books', PAYLOAD, {{ headers: HEADERS }});
}}
"""


def run_k6(script_path: str, summary_path: Path, timeseries_path: Path) -> None:
    cmd = [
        "k6",
        "run",
        "--summary-export",
        str(summary_path),
        "--out",
        f"csv={timeseries_path}",
        script_path,
    ]
    result = subprocess.run(cmd, check=False)
    if result.returncode not in (0, 99):
        result.check_returncode()
    elif result.returncode != 0:
        logger.warning("k6 exited with code %s (threshold breach); continuing", result.returncode)



def parse_k6_summary(summary_path: Path) -> List[Dict[str, float]]:
    try:
        with open(summary_path, "r", encoding="utf-8") as fh:
            summary = json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Failed to load k6 summary %s: %s", summary_path, exc)
        return []

    metrics = summary.get("metrics")
    if metrics is None and isinstance(summary.get("data"), dict):
        metrics = summary["data"].get("metrics")
    if not isinstance(metrics, dict):
        logger.warning("k6 summary missing metrics section")
        return []

    def metric_values(metric_name: str) -> Dict[str, float]:
        metric = metrics.get(metric_name, {})
        if not isinstance(metric, dict):
            return {}
        values = metric.get("values")
        if isinstance(values, dict):
            return values
        return metric

    duration_metrics = metric_values("http_req_duration")
    throughput_metrics = metric_values("http_reqs")
    if not duration_metrics or not throughput_metrics:
        logger.warning("k6 summary missing required metrics")
        return []

    throughput = throughput_metrics.get("rate") or 0.0
    row = {
        "throughput_rps": float(throughput),
        "p95_ms": float(duration_metrics.get("p(95)", duration_metrics.get("p95", 0.0))),
        "p90_ms": float(duration_metrics.get("p(90)", duration_metrics.get("p90", 0.0))),
        "p50_ms": float(duration_metrics.get("med", duration_metrics.get("p(50)", 0.0))),
        "avg_ms": float(duration_metrics.get("avg", 0.0)),
    }
    return [row]

def parse_k6_timeseries(timeseries_path: Path) -> List[Dict[str, float]]:
    df = pd.read_csv(timeseries_path)

    metric_col = next((col for col in ("metric_name", "metric", "name") if col in df.columns), None)
    time_col = "timestamp" if "timestamp" in df.columns else ("time" if "time" in df.columns else None)
    value_col = "metric_value" if "metric_value" in df.columns else ("value" if "value" in df.columns else None)
    if not metric_col or not time_col or not value_col:
        logger.warning("k6 timeseries data missing required columns")
        return []

    df = df[df[metric_col] == "http_req_duration"]
    if df.empty:
        logger.warning("k6 timeseries data missing http_req_duration metric")
        return []

    df[time_col] = pd.to_datetime(df[time_col], unit="s", errors="coerce")
    df[value_col] = pd.to_numeric(df[value_col], errors="coerce")
    df = df.dropna(subset=[time_col, value_col])
    if df.empty:
        logger.warning("k6 timeseries data has no valid timestamp or value entries")
        return []

    df = df.set_index(time_col)
    grouped = df[value_col].resample("10s")
    summary = pd.DataFrame(
        {
            "throughput_rps": grouped.count() / 10.0,
            "p50_ms": grouped.quantile(0.5),
            "p90_ms": grouped.quantile(0.9),
            "p95_ms": grouped.quantile(0.95),
            "avg_ms": grouped.mean(),
        }
    )
    summary = summary.fillna(0)
    if summary.empty:
        logger.warning("k6 timeseries data produced no resampled metrics")
        return []

    rows: List[Dict[str, float]] = []
    for row in summary.itertuples():
        throughput = float(row.throughput_rps)
        rows.append(
            {
                "throughput_rps": throughput,
                "p95_ms": float(row.p95_ms),
                "p90_ms": float(row.p90_ms),
                "p50_ms": float(row.p50_ms),
                "avg_ms": float(row.avg_ms),
            }
        )

    return rows


def write_results(results_dir: Path, rows: List[Dict[str, float]]) -> Path:
    csv_path = results_dir / "oebs_load_results.csv"
    fieldnames = ["app", "rate_rps", "throughput_rps", "p95_ms", "p90_ms", "p50_ms", "avg_ms"]
    with open(csv_path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return csv_path


def main() -> None:
    log_level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_name, logging.INFO)
    logging.basicConfig(level=log_level, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(
        description="Deploy Docker images to Kubernetes with PVCs and run k6 load tests."
    )
    parser.add_argument("dockerImages", nargs="+", help="Docker images to deploy")
    parser.add_argument("--namespace", default="default", help="Kubernetes namespace")
    parser.add_argument("--storage", default="8Gi", help="PVC storage request")
    parser.add_argument(
        "--median-latency-threshold-s",
        type=float,
        default=1.0,
        help="Stop running additional load tests when median latency exceeds this threshold (seconds)",
    )
    parser.add_argument("--duration", default="1m", help="Duration for each k6 constant-rate run")
    parser.add_argument("--port-start", type=int, default=18080, help="Starting local port for port-forward")
    parser.add_argument("--data-parsing-only", action="store_true", help="Only parse existing k6 result files")
    parser.add_argument(
        "--rates",
        type=str,
        default=str(DEFAULT_RATE_RPS),
        help="Comma-separated list of constant arrival rates (req/s); each rate triggers a separate k6 run",
    )
    args = parser.parse_args()

    try:
        parsed_rates = []
        for part in args.rates.split(","):
            part = part.strip()
            if not part:
                continue
            parsed_rates.append(int(part))
    except ValueError as exc:
        raise ValueError("Rates must be comma-separated integers (example: --rates 50,100,150)") from exc

    rates = [r for r in parsed_rates if r > 0]
    if not rates:
        raise ValueError("At least one positive rate is required (example: --rates 50,100,150)")

    results_dir = Path(__file__).resolve().parent / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    median_threshold_ms = args.median_latency_threshold_s * 1000.0

    all_rows: List[Dict[str, float]] = []
    for idx, image in enumerate(args.dockerImages):
        app_name = normalize_name(image)
        pvc_name = f"{app_name}-pvc"
        local_port = args.port_start + idx
        tmp_script = None
        pf_proc: Optional[subprocess.Popen] = None
        logger.info("Deploying app %s with image %s", app_name, image)
        try:
            if not args.data_parsing_only:
                # Create storage and workload resources.
                create_pvc(pvc_name, args.namespace, args.storage)
                create_deployment(app_name, image, pvc_name, args.namespace)
                create_service(app_name, args.namespace)

                # Expose the service locally for the load generator.
                pf_proc = start_port_forward(app_name, args.namespace, local_port)
                wait_for_port("127.0.0.1", local_port)

                logger.info("Port-forward ready, starting k6 runs for rates: %s", rates)

            for rate in rates:
                summary_file = results_dir / f"{app_name}_{rate}rps_summary.json"
                timeseries_file = results_dir / f"{app_name}_{rate}rps_timeseries.csv"

                if not args.data_parsing_only:
                    script_content = build_k6_constant_rate_script(
                        f"http://127.0.0.1:{local_port}/", args.duration, median_threshold_ms, rate
                    )
                    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False) as tf:
                        tf.write(script_content)
                        tmp_script = tf.name

                    logger.info("Running k6 constant-rate test (%srps) against %s", rate, app_name)
                    run_k6(tmp_script, summary_file, timeseries_file)
                    Path(tmp_script).unlink(missing_ok=True)
                    tmp_script = None

                logger.info("Parsing k6 summary results for %s at target %srps", app_name, rate)
                stage_metrics = parse_k6_summary(summary_file)
                for metrics in stage_metrics:
                    throughput = metrics["throughput_rps"]
                    logger.info("Metrics for %s rps on %s: %s", throughput, app_name, metrics)
                    all_rows.append({"app": app_name, "rate_rps": rate, **metrics})
        finally:
            # Always tear down the port-forward and Kubernetes objects.
            if pf_proc and pf_proc.poll() is None:
                pf_proc.terminate()
                try:
                    pf_proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    pf_proc.kill()
            if tmp_script:
                Path(tmp_script).unlink(missing_ok=True)
            delete_resource("service", app_name, args.namespace)
            delete_resource("deployment", app_name, args.namespace)
            delete_resource("pvc", pvc_name, args.namespace)

    csv_path = write_results(results_dir, all_rows)
    print(f"Results saved to {csv_path}")


if __name__ == "__main__":
    main()
