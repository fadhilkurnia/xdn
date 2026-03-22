"""
run_microbench_breakdown_bookcatalog.py — Bookcatalog optimization breakdown benchmark.

Measures the contribution of each optimization level to max throughput with
the deterministic bookcatalog app (active Paxos replication):

  Level 0: no_optimization   — disk-backed state, no batching
  Level 1: in_memory_state   — state dir on /dev/shm (ramdisk)
  Level 2: paxos_batching    — + BATCHING_ENABLED=true
  Level 3: exec_batching     — + HTTP_AR_FRONTEND_BATCH_ENABLED=true with commutative matchers

Workload: PUT /api/books/{bookId} with round-robin over 2000 pre-seeded books.

Run from the eval/ directory:
    python3 run_microbench_breakdown_bookcatalog.py [--rates 100,200,...] [--levels 0,1,2,3]
"""

import argparse
import csv
import logging
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

log = logging.getLogger("microbench-breakdown")

from run_load_pb_bookcatalog_common import (
    AR_HOSTS,
    CONTROL_PLANE_HOST,
    HTTP_PROXY_PORT,
    SERVICE_NAME,
    TIMEOUT_PORT_SEC,
    XDN_BINARY,
    clear_xdn_cluster,
    seed_books,
    wait_for_port,
    wait_for_service,
)

# This benchmark measures deterministic bookcatalog (active replication).
BOOKCATALOG_DET_IMAGE = "fadhilkurnia/xdn-bookcatalog"

# ── Config ────────────────────────────────────────────────────────────────────

# Rates to sweep (req/s)
DEFAULT_RATES = [
    1, 100, 200, 300, 400, 500, 600, 700, 800, 900,
    1000, 1200, 1500, 2000, 2500, 3000, 3500, 4000,
    4500, 5000, 6000, 7000,
]

LOAD_DURATION_SEC = 60
WARMUP_DURATION_SEC = 15
AVG_LATENCY_THRESHOLD_MS = 3_000
SEED_BOOK_COUNT = 2000

GP_BASE_CONFIG = "../conf/gigapaxos.xdn.3way.cloudlab.properties"
GO_LATENCY_CLIENT = Path(__file__).resolve().parent / "get_latency_at_rate.go"
RESULTS_BASE = Path(__file__).resolve().parent / "results"
SCREEN_SESSION = "xdn_optbreak"
SCREEN_LOG = f"screen_logs/{SCREEN_SESSION}.log"

BOOKCATALOG_DET_YAML = "../xdn-cli/examples/bookcatalog-deterministic.yaml"
BOOKCATALOG_COMM_YAML = "../xdn-cli/examples/bookcatalog-commutative.yaml"

# ── Optimization Level Definitions ────────────────────────────────────────────

LEVEL_CONFIGS = {
    0: {
        "name": "no_optimization",
        "XDN_FUSELOG_BASE_DIR": "/tmp/xdn/state/fuselog/",
        "BATCHING_ENABLED": "false",
        "HTTP_AR_FRONTEND_BATCH_ENABLED": "false",
        "yaml": BOOKCATALOG_DET_YAML,
    },
    1: {
        "name": "in_memory_state",
        "XDN_FUSELOG_BASE_DIR": "/dev/shm/xdn/state/fuselog/",
        "BATCHING_ENABLED": "false",
        "HTTP_AR_FRONTEND_BATCH_ENABLED": "false",
        "yaml": BOOKCATALOG_DET_YAML,
    },
    2: {
        "name": "paxos_batching",
        "XDN_FUSELOG_BASE_DIR": "/dev/shm/xdn/state/fuselog/",
        "BATCHING_ENABLED": "true",
        "HTTP_AR_FRONTEND_BATCH_ENABLED": "false",
        "yaml": BOOKCATALOG_DET_YAML,
    },
    3: {
        "name": "exec_batching",
        "XDN_FUSELOG_BASE_DIR": "/dev/shm/xdn/state/fuselog/",
        "BATCHING_ENABLED": "true",
        "HTTP_AR_FRONTEND_BATCH_ENABLED": "true",
        "yaml": BOOKCATALOG_COMM_YAML,
    },
}


# ── Helpers ───────────────────────────────────────────────────────────────────


def generate_config(level):
    """Read base config, override properties per level, write temp config file."""
    cfg = LEVEL_CONFIGS[level]
    overrides = {
        "XDN_FUSELOG_BASE_DIR": cfg["XDN_FUSELOG_BASE_DIR"],
        "BATCHING_ENABLED": cfg["BATCHING_ENABLED"],
        "HTTP_AR_FRONTEND_BATCH_ENABLED": cfg["HTTP_AR_FRONTEND_BATCH_ENABLED"],
        "SYNC": "true",
    }

    lines = []
    with open(GP_BASE_CONFIG) as f:
        for line in f:
            stripped = line.strip()
            # Skip comments and empty lines — pass through
            if stripped.startswith("#") or "=" not in stripped:
                lines.append(line)
                continue
            key = stripped.split("=", 1)[0].strip()
            if key in overrides:
                lines.append(f"{key}={overrides[key]}\n")
                del overrides[key]
            else:
                lines.append(line)

    # Append any overrides not found in the base config
    for key, val in overrides.items():
        lines.append(f"{key}={val}\n")

    out_path = f"/tmp/xdn_optbreak_level{level}.properties"
    with open(out_path, "w") as f:
        f.writelines(lines)
    log.info("   Config written to %s", out_path)
    return out_path


def start_cluster_with_config(config_path):
    """Start the XDN cluster in a screen session with the given config."""
    os.makedirs("screen_logs", exist_ok=True)
    os.system(f"rm -f {SCREEN_LOG}")
    cmd = (
        f"screen -L -Logfile {SCREEN_LOG} -S {SCREEN_SESSION} -d -m bash -c "
        f"'../bin/gpServer.sh -DgigapaxosConfig={config_path} start all; exec bash'"
    )
    log.info("   %s", cmd)
    ret = os.system(cmd)
    assert ret == 0, "Failed to start cluster in screen session"
    log.info("   Screen log: %s", SCREEN_LOG)
    time.sleep(20)


def generate_urls_file(host, results_dir, count=2000):
    """Write urls.txt with round-robin PUT URLs."""
    urls_path = results_dir / "urls.txt"
    with open(urls_path, "w") as f:
        for i in range(1, count + 1):
            f.write(f"http://{host}:{HTTP_PROXY_PORT}/api/books/{i}\n")
    log.info("   URLs file written to %s (%s URLs)", urls_path, count)
    return urls_path


def setup_fresh_cluster(level, results_dir):
    """Clear -> gen config -> start -> wait -> deploy -> seed -> gen urls file.

    Returns (target_host, urls_file_path) or exits on failure.
    """
    cfg = LEVEL_CONFIGS[level]
    log.info("\n   [setup] Level %s (%s)", level, cfg['name'])

    log.info("   [setup] Force-clearing cluster ...")
    clear_xdn_cluster()

    log.info("   [setup] Generating config ...")
    config_path = generate_config(level)

    log.info("   [setup] Starting XDN cluster ...")
    start_cluster_with_config(config_path)

    log.info("   [setup] Waiting for Active Replicas ...")
    for host in AR_HOSTS:
        if not wait_for_port(host, HTTP_PROXY_PORT, TIMEOUT_PORT_SEC):
            log.error(" AR at %s:%s not ready.", host, HTTP_PROXY_PORT)
            log.info("       Check: tail -f %s", SCREEN_LOG)
            sys.exit(1)

    log.info("   [setup] Ensuring Docker images on RC ...")
    ensure_docker_images_on_rc(images=["fadhilkurnia/xdn-bookcatalog:latest"])

    log.info("   [setup] Launching bookcatalog service (yaml=%s) ...", cfg['yaml'])
    cmd = (
        f"XDN_CONTROL_PLANE={CONTROL_PLANE_HOST} {XDN_BINARY} "
        f"launch {SERVICE_NAME} --file={cfg['yaml']}"
    )
    log.info("   %s", cmd)
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    log.info("  %s", result.stdout.strip())
    if result.returncode != 0:
        log.error(" xdn launch failed:\n%s", result.stderr)
        sys.exit(1)

    log.info("   [setup] Waiting for service readiness ...")
    ok, ready_host = wait_for_service(AR_HOSTS, HTTP_PROXY_PORT, SERVICE_NAME, 120)
    if not ok:
        log.info("ERROR: bookcatalog service not ready after 120s.")
        sys.exit(1)

    # For deterministic bookcatalog with active Paxos, all replicas run
    # the container. Use the first AR host for load generation.
    target_host = AR_HOSTS[0]

    log.info("   [setup] Seeding %s books on %s ...", SEED_BOOK_COUNT, target_host)
    created = seed_books(target_host, HTTP_PROXY_PORT, SERVICE_NAME, count=SEED_BOOK_COUNT)
    if created < SEED_BOOK_COUNT * 0.9:
        log.info("WARNING: only %s/%s books seeded", created, SEED_BOOK_COUNT)

    urls_file = generate_urls_file(target_host, results_dir, count=SEED_BOOK_COUNT)

    return target_host, urls_file


def _parse_go_output(path):
    """Parse the Go load generator output file."""
    metrics = {}
    for line in open(path):
        if ":" not in line:
            continue
        k, v = line.split(":", 1)
        try:
            metrics[k.strip()] = float(v.strip())
        except ValueError:
            pass
    return {
        "throughput_rps": metrics.get("actual_throughput_rps", 0.0),
        "actual_achieved_rps": metrics.get("actual_achieved_rate_rps", 0.0),
        "avg_ms": metrics.get("average_latency_ms", 0.0),
        "p50_ms": metrics.get("median_latency_ms", 0.0),
        "p90_ms": metrics.get("p90_latency_ms", 0.0),
        "p95_ms": metrics.get("p95_latency_ms", 0.0),
        "p99_ms": metrics.get("p99_latency_ms", 0.0),
    }


def run_load(host, rate, output_file, urls_file, duration_sec):
    """Run Go load generator with PUT requests using round-robin URLs."""
    payload = '{"title":"bench","author":"bench"}'
    cmd = [
        "go", "run", str(GO_LATENCY_CLIENT),
        "-H", f"XDN: {SERVICE_NAME}",
        "-H", "Content-Type: application/json",
        "-X", "PUT",
        "-urls-file", str(urls_file),
        f"http://{host}:{HTTP_PROXY_PORT}/api/books/1",
        payload,
        str(duration_sec),
        str(rate),
    ]
    env = os.environ.copy()
    env["GO111MODULE"] = "off"
    env["GOWORK"] = "off"
    log.info("   Output -> %s", output_file)
    with open(output_file, "w") as fh:
        result = subprocess.run(cmd, stdout=fh, stderr=subprocess.STDOUT,
                                env=env, text=True)
    if result.returncode != 0:
        log.warning(" go client exited %s", result.returncode)
    return _parse_go_output(output_file)


def warmup(host, rate, results_dir, urls_file):
    """Run a throwaway warmup load."""
    output_file = results_dir / f"warmup_rate{rate}.txt"
    m = run_load(host, rate, output_file, urls_file, WARMUP_DURATION_SEC)
    log.info("     [warmup] tput=%.2f rps  avg=%.1fms", m['throughput_rps'], m['avg_ms'])
    return m


# ── CLI ───────────────────────────────────────────────────────────────────────


def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--rates", type=str, default=None,
                   help="Comma-separated rates to override default rates")
    p.add_argument("--levels", type=str, default=None,
                   help="Comma-separated optimization levels (0-3) to run")
    return p.parse_args()


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    args = parse_args()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_dir = RESULTS_BASE / f"bookcatalog_optbreakdown_{timestamp}"
    results_dir.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(results_dir / "run.log"),
        ],
    )

    rates = [int(r) for r in args.rates.split(",")] if args.rates else DEFAULT_RATES
    levels = [int(l) for l in args.levels.split(",")] if args.levels else [0, 1, 2, 3]

    log.info("=" * 60)
    log.info("Bookcatalog Optimization Breakdown Benchmark")
    log.info("  Levels   : %s", levels)
    log.info("  Rates    : %s", rates)
    log.info("  Duration : %ds per rate point", LOAD_DURATION_SEC)
    log.info("  Warmup   : %ds", WARMUP_DURATION_SEC)
    log.info("  Books    : %d", SEED_BOOK_COUNT)
    log.info("  Results  -> %s", results_dir)
    log.info("=" * 60)

    all_results = []
    summary_file = results_dir / "eval_bookcatalog_optimization_load_latency.csv"

    for level in levels:
        cfg = LEVEL_CONFIGS[level]
        log.info("\n%s", '='*60)
        log.info("  Level %s: %s", level, cfg['name'])
        log.info("%s", '='*60)

        for i, rate in enumerate(rates):
            log.info("\n  --- Level %s, Rate %s/%s: %s req/s ---", level, i+1, len(rates), rate)

            # Fresh cluster per rate point
            target_host, urls_file = setup_fresh_cluster(level, results_dir)

            # Warmup
            log.info("\n   -> warmup at %s req/s for %ss ...", rate, WARMUP_DURATION_SEC)
            wm = warmup(target_host, rate, results_dir, urls_file)
            if wm["throughput_rps"] < 0.1 * rate and rate > 1:
                log.info(
                    f"   SKIP: warmup tput={wm['throughput_rps']:.2f} rps "
                    f"< 10% of {rate} rps — system not responding"
                )
                continue
            log.info("   Settling 5s after warmup ...")
            time.sleep(5)

            # Measurement
            output_file = results_dir / f"opt{level}_rate{rate}.txt"
            log.info("   -> measuring at %s req/s for %ss ...", rate, LOAD_DURATION_SEC)
            m = run_load(target_host, rate, output_file, urls_file, LOAD_DURATION_SEC)
            row = {
                "level": level,
                "level_name": cfg["name"],
                "rate_rps": rate,
                "throughput_rps": m["throughput_rps"],
                "avg_ms": m["avg_ms"],
                "p50_ms": m["p50_ms"],
                "p90_ms": m["p90_ms"],
                "p95_ms": m["p95_ms"],
            }
            all_results.append(row)
            log.info(
                    f"     tput={m['throughput_rps']:.2f} rps  "
                f"avg={m['avg_ms']:.1f}ms  "
                f"p50={m['p50_ms']:.1f}ms  "
                f"p90={m['p90_ms']:.1f}ms  "
                f"p95={m['p95_ms']:.1f}ms"
            )

            # Write summary incrementally
            with open(summary_file, "w", newline="") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=["level", "level_name", "rate_rps",
                                "throughput_rps", "avg_ms", "p50_ms",
                                "p90_ms", "p95_ms"],
                )
                writer.writeheader()
                writer.writerows(all_results)

            # Early stop on saturation
            latency_saturated = m["avg_ms"] > AVG_LATENCY_THRESHOLD_MS
            throughput_saturated = (
                m["actual_achieved_rps"] > 0
                and m["throughput_rps"] < 0.75 * m["actual_achieved_rps"]
            ) if "actual_achieved_rps" in m else False
            if latency_saturated and throughput_saturated:
                log.info(
                    f"   Saturated at level {level}, rate {rate} — "
                    f"stopping this level early."
                )
                break

    # ── Final Summary ─────────────────────────────────────────────────────
    log.info("\n%s", '='*60)
    log.info("[Summary]")
    log.info("   %3s  %-20s  %6s  %8s  %8s  %8s  %8s", "lvl", "name", "rate", "tput", "avg", "p50", "p95")
    log.info("   " + "-" * 78)
    for row in all_results:
        log.info(
                    f"   {row['level']:>3}  {row['level_name']:<20}  "
            f"{row['rate_rps']:>5.0f}  "
            f"{row['throughput_rps']:>7.2f}  "
            f"{row['avg_ms']:>7.1f}ms  "
            f"{row['p50_ms']:>7.1f}ms  "
            f"{row['p95_ms']:>7.1f}ms"
        )

    log.info("\n[Done] Results in %s/", results_dir)
    log.info("       Summary CSV: %s", summary_file)
