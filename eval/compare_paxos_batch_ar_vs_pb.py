"""
compare_paxos_batch_ar_vs_pb.py — Compare Paxos-level batch sizes between
Active Replication (deterministic) and Primary-Backup (non-deterministic)
for the bookcatalog workload.

Runs both configurations at a moderate load rate, captures the screen logs,
and reports the PaxosSlotBatch distribution for each.

Run from the eval/ directory:
    python3 -u compare_paxos_batch_ar_vs_pb.py [--rates 100,200,300,400]
"""

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

from run_load_pb_bookcatalog_common import (
    AR_HOSTS,
    CONTROL_PLANE_HOST,
    HTTP_PROXY_PORT,
    TIMEOUT_APP_SEC,
    TIMEOUT_PORT_SEC,
    XDN_BINARY,
    clear_xdn_cluster,
    detect_primary_via_docker,
    ensure_docker_images_on_rc,
    seed_books,
    start_cluster,
    wait_for_app_ready,
    wait_for_port,
    wait_for_service,
)

# ── Config ────────────────────────────────────────────────────────────────────

GP_CONFIG = "../conf/gigapaxos.xdn.3way.cloudlab.properties"
SCREEN_SESSION_AR = "xdn_batch_ar"
SCREEN_SESSION_PB = "xdn_batch_pb"
SCREEN_LOG_AR = "screen_logs/xdn_batch_ar.log"
SCREEN_LOG_PB = "screen_logs/xdn_batch_pb.log"

BOOKCATALOG_ND_YAML = "../xdn-cli/examples/bob-book-catalog-nondeterministic-service.yaml"
# We'll create a deterministic version inline
BOOKCATALOG_DET_YAML = "/tmp/bookcatalog-det-svc.yaml"

RESULTS_DIR = Path(__file__).resolve().parent / "results" / "paxos_batch_comparison"

GO_LATENCY_CLIENT = Path(__file__).resolve().parent / "get_latency_at_rate.go"

SERVICE_NAME = "svc"
LOAD_DURATION_SEC = 60
WARMUP_DURATION_SEC = 15
INTER_RATE_PAUSE_SEC = 10
DEFAULT_RATES = [100, 200, 300, 400, 500, 600, 800, 1000, 1500, 2000, 2500]


# ── Helpers ──────────────────────────────────────────────────────────────────

def create_det_yaml():
    """Create a deterministic single-container bookcatalog YAML."""
    content = """---
name: "svc"
image: "fadhilkurnia/xdn-bookcatalog-nd"
port: 80
state: /app/data/
consistency: linearizability
deterministic: true
environments:
  - ENABLE_WAL: "true"
"""
    with open(BOOKCATALOG_DET_YAML, "w") as f:
        f.write(content)
    print(f"   Created {BOOKCATALOG_DET_YAML}")


def ensure_images():
    """Make sure bookcatalog-nd image is on all nodes + RC."""
    img = "fadhilkurnia/xdn-bookcatalog-nd"
    for host in AR_HOSTS + [CONTROL_PLANE_HOST]:
        check = subprocess.run(
            ["ssh", host, f"docker image inspect {img}"],
            capture_output=True,
        )
        if check.returncode != 0:
            print(f"   Pulling {img} on {host} ...")
            os.system(f"ssh {host} 'docker pull {img}'")


def start_cluster_with_log(screen_session, screen_log):
    """Start the XDN cluster in a screen session with a specific log file."""
    os.makedirs("screen_logs", exist_ok=True)
    os.system(f"rm -f {screen_log}")
    cmd = (
        f"screen -L -Logfile {screen_log} -S {screen_session} -d -m bash -c "
        f"'../bin/gpServer.sh -DgigapaxosConfig={GP_CONFIG} start all; exec bash'"
    )
    print(f"   {cmd}")
    ret = os.system(cmd)
    assert ret == 0, "Failed to start cluster"
    print(f"   Screen log: {screen_log}")
    time.sleep(20)


def launch_service(yaml_file):
    """Launch a service using xdn CLI."""
    cmd = (
        f"XDN_CONTROL_PLANE={CONTROL_PLANE_HOST} {XDN_BINARY} "
        f"launch {SERVICE_NAME} --file={yaml_file}"
    )
    print(f"   {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        print(f"ERROR: xdn launch failed:\n{result.stderr}")
        return False
    return True


def run_load(target_host, rate, output_file, duration_sec):
    """Run get_latency_at_rate.go."""
    output_file.parent.mkdir(parents=True, exist_ok=True)
    url = f"http://{target_host}:{HTTP_PROXY_PORT}/api/books"
    cmd = [
        "go", "run", str(GO_LATENCY_CLIENT),
        "-H", f"XDN: {SERVICE_NAME}",
        "-H", "Content-Type: application/json",
        "-X", "POST",
        url,
        '{"title":"bench book","author":"bench author"}',
        str(duration_sec),
        str(rate),
    ]
    env = os.environ.copy()
    env["GO111MODULE"] = "off"
    env["GOWORK"] = "off"
    with open(output_file, "w") as fh:
        result = subprocess.run(cmd, stdout=fh, stderr=subprocess.STDOUT,
                                env=env, text=True)
    if result.returncode != 0:
        print(f"   WARNING: go client exited {result.returncode}")


def parse_go_output(output_file):
    """Parse throughput from go client output (key: value format)."""
    metrics = {}
    try:
        for line in open(output_file):
            if ":" not in line:
                continue
            k, v = line.split(":", 1)
            try:
                metrics[k.strip()] = float(v.strip())
            except ValueError:
                pass
    except Exception:
        pass
    tput = metrics.get("actual_throughput_rps", 0.0)
    avg = metrics.get("average_latency_ms", 0.0)
    return tput, avg


def run_benchmark(mode, yaml_file, screen_session, screen_log, rates, results_subdir):
    """Run a full benchmark for one mode (AR or PB)."""
    print(f"\n{'=' * 60}")
    print(f"  Benchmark: {mode}")
    print(f"{'=' * 60}")

    # Phase 1 — Reset
    print(f"\n[{mode}] Phase 1 — Force-clearing cluster ...")
    clear_xdn_cluster()

    # Phase 2 — Start
    print(f"\n[{mode}] Phase 2 — Starting cluster ...")
    start_cluster_with_log(screen_session, screen_log)

    # Phase 3 — Wait for ARs
    print(f"\n[{mode}] Phase 3 — Waiting for Active Replicas ...")
    for host in AR_HOSTS:
        if not wait_for_port(host, HTTP_PROXY_PORT, TIMEOUT_PORT_SEC):
            print(f"ERROR: AR at {host}:{HTTP_PROXY_PORT} not ready.")
            return []

    # Phase 3b — Ensure images
    print(f"\n[{mode}] Phase 3b — Ensuring Docker images ...")
    ensure_images()

    # Phase 4 — Launch service
    print(f"\n[{mode}] Phase 4 — Launching service ({mode}) ...")
    if not launch_service(yaml_file):
        return []

    # Phase 5 — Wait for readiness
    print(f"\n[{mode}] Phase 5 — Waiting for HTTP readiness ...")
    ok, _ = wait_for_service(AR_HOSTS, HTTP_PROXY_PORT, SERVICE_NAME, TIMEOUT_APP_SEC)
    if not ok:
        print(f"ERROR: service not ready.")
        return []

    # Phase 6 — Detect target (primary for PB, any AR for AR)
    if mode == "PB":
        print(f"\n[{mode}] Phase 6 — Detecting primary ...")
        target = detect_primary_via_docker(AR_HOSTS)
        if not target:
            target = AR_HOSTS[0]
    else:
        target = AR_HOSTS[0]
    print(f"   Target: {target}")

    # Phase 7 — Seed
    print(f"\n[{mode}] Phase 7 — Seeding books ...")
    for attempt in range(1, 4):
        created = seed_books(target, HTTP_PROXY_PORT, SERVICE_NAME, count=200)
        if created > 0:
            break
        time.sleep(10)

    # Phase 8 — Verify
    print(f"\n[{mode}] Phase 8 — Verifying app ...")
    wait_for_app_ready(target, HTTP_PROXY_PORT, SERVICE_NAME, timeout_sec=60)

    # Phase 9 — Load sweep
    out_dir = RESULTS_DIR / results_subdir
    print(f"\n[{mode}] Phase 9 — Load sweep at {rates} req/s ...")
    results = []
    for i, rate in enumerate(rates):
        if i > 0:
            print(f"   Pausing {INTER_RATE_PAUSE_SEC}s ...")
            time.sleep(INTER_RATE_PAUSE_SEC)

        # Warmup
        print(f"\n   [{mode}] rate={rate} warmup ({WARMUP_DURATION_SEC}s) ...")
        run_load(target, rate, out_dir / f"warmup_rate{rate}.txt", WARMUP_DURATION_SEC)
        time.sleep(5)

        # Measure
        print(f"   [{mode}] rate={rate} measuring ({LOAD_DURATION_SEC}s) ...")
        out_file = out_dir / f"rate{rate}.txt"
        run_load(target, rate, out_file, LOAD_DURATION_SEC)
        tput, avg = parse_go_output(out_file)
        results.append((rate, tput, avg))
        print(f"     tput={tput:.2f} rps  avg={avg:.1f}ms")

        if avg > 3000:
            print(f"   System saturated (avg={avg:.0f}ms), stopping.")
            break

    # Save screen log copy
    out_dir.mkdir(parents=True, exist_ok=True)
    os.system(f"cp {screen_log} {out_dir}/screen.log 2>/dev/null || true")

    return results


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--rates", type=str, default=None,
                   help="Comma-separated rates (default: 100,200,...,2500)")
    p.add_argument("--skip-ar", action="store_true", help="Skip AR benchmark")
    p.add_argument("--skip-pb", action="store_true", help="Skip PB benchmark")
    args = p.parse_args()

    rates = [int(r.strip()) for r in args.rates.split(",")] if args.rates else DEFAULT_RATES

    create_det_yaml()

    ar_results = []
    pb_results = []

    # Run AR benchmark
    if not args.skip_ar:
        ar_results = run_benchmark(
            "AR", BOOKCATALOG_DET_YAML,
            SCREEN_SESSION_AR, SCREEN_LOG_AR,
            rates, "ar",
        )

    # Run PB benchmark
    if not args.skip_pb:
        pb_results = run_benchmark(
            "PB", BOOKCATALOG_ND_YAML,
            SCREEN_SESSION_PB, SCREEN_LOG_PB,
            rates, "pb",
        )

    # Parse Paxos batch sizes
    print(f"\n{'=' * 60}")
    print("  Paxos-Level Batch Size Comparison")
    print(f"{'=' * 60}")

    parse_script = Path(__file__).resolve().parent / "investigate_parse_paxos_batch_size.py"

    if not args.skip_ar:
        ar_log = RESULTS_DIR / "ar" / "screen.log"
        if ar_log.exists():
            print(f"\n--- Active Replication (AR) ---")
            subprocess.run(["python3", str(parse_script), "--log", str(ar_log),
                            "--group", SERVICE_NAME])

    if not args.skip_pb:
        pb_log = RESULTS_DIR / "pb" / "screen.log"
        if pb_log.exists():
            print(f"\n--- Primary-Backup (PB) ---")
            subprocess.run(["python3", str(parse_script), "--log", str(pb_log),
                            "--group", SERVICE_NAME])

    # Summary table
    print(f"\n{'=' * 60}")
    print("  Throughput Summary")
    print(f"{'=' * 60}")
    print(f"  {'rate':>6}  {'AR tput':>10}  {'AR avg':>10}  {'PB tput':>10}  {'PB avg':>10}")
    print("  " + "-" * 50)

    ar_dict = {r: (t, a) for r, t, a in ar_results}
    pb_dict = {r: (t, a) for r, t, a in pb_results}
    all_rates = sorted(set(list(ar_dict.keys()) + list(pb_dict.keys())))
    for rate in all_rates:
        ar_t, ar_a = ar_dict.get(rate, (0, 0))
        pb_t, pb_a = pb_dict.get(rate, (0, 0))
        ar_t_s = f"{ar_t:.1f}" if ar_t > 0 else "—"
        ar_a_s = f"{ar_a:.1f}ms" if ar_a > 0 else "—"
        pb_t_s = f"{pb_t:.1f}" if pb_t > 0 else "—"
        pb_a_s = f"{pb_a:.1f}ms" if pb_a > 0 else "—"
        print(f"  {rate:>6}  {ar_t_s:>10}  {ar_a_s:>10}  {pb_t_s:>10}  {pb_a_s:>10}")

    print(f"\nResults saved in {RESULTS_DIR}/")


if __name__ == "__main__":
    main()
