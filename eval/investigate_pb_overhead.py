"""
investigate_pb_overhead.py — Layer-by-layer overhead decomposition for XDN PB.

Measures per-layer overhead using debug headers to isolate:
  Layer 0 (___DDR): Netty HTTP server only (dummy response)
  Layer 1 (___DBC): Netty proxy → container (bypass coordination)
  Layer 2 (___DDE): XdnGigapaxosApp.execute() directly (no PBM/Paxos)
  Layer 4 (normal): Full PB pipeline (already measured separately)

Layer 3 (EMULATE_UNREPLICATED) requires a config change + rebuild, so it is
run as a separate invocation with --layer3-only.

Run from the eval/ directory:
    python3 investigate_pb_overhead.py                       # Layers 0-2
    python3 investigate_pb_overhead.py --skip-setup          # Skip cluster setup
    python3 investigate_pb_overhead.py --layer3-only         # Layer 3 only (requires EMULATE_UNREPLICATED=true)
    python3 investigate_pb_overhead.py --rates 100,500,1000  # Custom rates

Outputs go to eval/results/pb_overhead/:
    layer0_ddr/rate{R}.txt
    layer1_dbc/rate{R}.txt
    layer2_dde/rate{R}.txt
    layer3_unreplicated/rate{R}.txt   (--layer3-only)
"""

import argparse
import os
import re
import subprocess
import sys
import time
from pathlib import Path

from run_load_pb_bookcatalog_common import (
    AR_HOSTS,
    BOOKCATALOG_YAML,
    CONTROL_PLANE_HOST,
    HTTP_PROXY_PORT,
    SCREEN_LOG,
    SERVICE_NAME,
    TIMEOUT_APP_SEC,
    TIMEOUT_PORT_SEC,
    XDN_BINARY,
    check_app_ready,
    clear_xdn_cluster,
    detect_primary_via_docker,
    ensure_docker_images_on_rc,
    seed_books,
    start_cluster,
    wait_for_app_ready,
    wait_for_port,
    wait_for_service,
)
from run_load_pb_wordpress_reflex import (
    AVG_LATENCY_THRESHOLD_MS,
    GO_LATENCY_CLIENT,
    INTER_RATE_PAUSE_SEC,
    LOAD_DURATION_SEC,
    WARMUP_DURATION_SEC,
    _parse_go_output,
)

# ── Config ────────────────────────────────────────────────────────────────────

RESULTS_BASE = Path(__file__).resolve().parent / "results" / "pb_overhead"

DEFAULT_RATES = [100, 500, 1000, 1500, 2000, 2500, 3000, 4000, 5000]


# ── Container Port Detection ─────────────────────────────────────────────────


def detect_container_port(primary):
    """Detect the host-mapped port for the bookcatalog-nd container on the primary."""
    print(f"   Detecting container port on {primary} ...")
    result = subprocess.run(
        ["ssh", primary, "docker ps --format '{{.Ports}}'"],
        capture_output=True, text=True, timeout=10,
    )
    if result.returncode != 0 or not result.stdout.strip():
        print(f"   ERROR: docker ps failed on {primary}")
        return None

    # Parse port mapping like "0.0.0.0:32768->80/tcp"
    for line in result.stdout.strip().splitlines():
        match = re.search(r"0\.0\.0\.0:(\d+)->80/tcp", line)
        if match:
            port = int(match.group(1))
            print(f"   Detected container port: {port}")
            return port

    # Try any port mapping
    for line in result.stdout.strip().splitlines():
        match = re.search(r"0\.0\.0\.0:(\d+)->", line)
        if match:
            port = int(match.group(1))
            print(f"   Detected container port: {port} (non-80 target)")
            return port

    print(f"   ERROR: No port mapping found in docker ps output")
    print(f"   Output: {result.stdout.strip()}")
    return None


# ── Load Helpers ──────────────────────────────────────────────────────────────


def _run_load(primary, rate, output_file, duration_sec, extra_headers=None):
    """Run get_latency_at_rate.go at `rate` req/s for `duration_sec` seconds."""
    output_file.parent.mkdir(parents=True, exist_ok=True)
    url = f"http://{primary}:{HTTP_PROXY_PORT}/api/books"
    cmd = [
        "go", "run", str(GO_LATENCY_CLIENT),
        "-H", f"XDN: {SERVICE_NAME}",
        "-H", "Content-Type: application/json",
        "-X", "POST",
    ]
    if extra_headers:
        for hdr in extra_headers:
            cmd.extend(["-H", hdr])
    cmd.extend([
        url,
        '{"title":"bench book","author":"bench author"}',
        str(duration_sec),
        str(rate),
    ])
    env = os.environ.copy()
    env["GO111MODULE"] = "off"
    env["GOWORK"] = "off"
    print(f"   Output -> {output_file}")
    with open(output_file, "w") as fh:
        result = subprocess.run(cmd, stdout=fh, stderr=subprocess.STDOUT,
                                env=env, text=True)
    if result.returncode != 0:
        print(f"   WARNING: go client exited {result.returncode}")
    return _parse_go_output(output_file)


def run_layer_sweep(primary, layer_name, layer_dir, rates, extra_headers=None):
    """Run a full rate sweep for one layer, returning list of result dicts."""
    print(f"\n   Layer: {layer_name}")
    print(f"   Headers: {extra_headers or '(none)'}")
    results = []
    for i, rate in enumerate(rates):
        if i > 0 and INTER_RATE_PAUSE_SEC > 0:
            pause = min(INTER_RATE_PAUSE_SEC, 10)  # shorter pause for debug layers
            print(f"   Pausing {pause}s ...")
            time.sleep(pause)

        # Warmup
        print(f"\n   -> rate={rate} req/s (warmup {WARMUP_DURATION_SEC}s) ...")
        warmup_file = layer_dir / f"warmup_rate{rate}.txt"
        wm = _run_load(primary, rate, warmup_file, WARMUP_DURATION_SEC, extra_headers)
        print(f"     [warmup] tput={wm['throughput_rps']:.2f} rps  avg={wm['avg_ms']:.1f}ms")

        if wm["throughput_rps"] < 0.1 * rate and rate > 100:
            print(f"   SKIP: warmup tput too low — system saturated")
            continue

        print(f"   Settling 5s after warmup ...")
        time.sleep(5)

        # Measurement
        print(f"   -> rate={rate} req/s (measuring {LOAD_DURATION_SEC}s) ...")
        output_file = layer_dir / f"rate{rate}.txt"
        m = _run_load(primary, rate, output_file, LOAD_DURATION_SEC, extra_headers)
        row = {"rate_rps": rate, **m}
        results.append(row)
        print(
            f"     tput={m['throughput_rps']:.2f} rps  "
            f"avg={m['avg_ms']:.1f}ms  "
            f"p50={m['p50_ms']:.1f}ms  "
            f"p90={m['p90_ms']:.1f}ms  "
            f"p95={m['p95_ms']:.1f}ms"
        )
        if m["avg_ms"] > AVG_LATENCY_THRESHOLD_MS:
            print(f"   Avg latency {m['avg_ms']:.0f}ms > threshold — stopping sweep early.")
            break

    return results


def print_summary_table(label, results):
    """Print a nicely formatted summary table for one layer."""
    print(f"\n   {label}:")
    print(f"   {'rate':>6}  {'tput':>8}  {'avg':>8}  {'p50':>8}  {'p95':>8}")
    print("   " + "-" * 48)
    for row in results:
        print(
            f"   {row['rate_rps']:>5.0f}  "
            f"{row['throughput_rps']:>7.2f}  "
            f"{row['avg_ms']:>7.1f}ms  "
            f"{row['p50_ms']:>7.1f}ms  "
            f"{row['p95_ms']:>7.1f}ms"
        )


def print_decomposition(all_layers):
    """Print overhead decomposition table comparing layers."""
    print("\n" + "=" * 70)
    print("Overhead Decomposition (avg latency at each rate)")
    print("=" * 70)

    # Get all rates that appear across layers
    all_rates = sorted(set(
        row["rate_rps"]
        for results in all_layers.values()
        for row in results
    ))

    # Build a lookup: layer_name -> rate -> metrics
    lookup = {}
    for layer_name, results in all_layers.items():
        lookup[layer_name] = {row["rate_rps"]: row for row in results}

    layer_names = list(all_layers.keys())
    header = f"{'rate':>6}"
    for name in layer_names:
        header += f"  {name:>12}"
    print(header)
    print("-" * len(header))

    for rate in all_rates:
        line = f"{rate:>5.0f}"
        for name in layer_names:
            if rate in lookup[name]:
                avg = lookup[name][rate]["avg_ms"]
                line += f"  {avg:>11.2f}ms"
            else:
                line += f"  {'—':>12}"
        print(line)

    # Decomposition: compute differences between adjacent layers
    if len(layer_names) >= 2:
        print(f"\nPer-layer overhead (delta avg_ms):")
        header = f"{'rate':>6}"
        for i in range(1, len(layer_names)):
            label = f"{layer_names[i]}-{layer_names[i-1]}"
            header += f"  {label:>16}"
        print(header)
        print("-" * len(header))

        for rate in all_rates:
            line = f"{rate:>5.0f}"
            for i in range(1, len(layer_names)):
                prev = layer_names[i - 1]
                curr = layer_names[i]
                if rate in lookup[prev] and rate in lookup[curr]:
                    delta = lookup[curr][rate]["avg_ms"] - lookup[prev][rate]["avg_ms"]
                    line += f"  {delta:>15.2f}ms"
                else:
                    line += f"  {'—':>16}"
            print(line)


def copy_screen_log(results_dir):
    """Copy the running screen log snapshot into results dir."""
    results_dir.mkdir(parents=True, exist_ok=True)
    dest = results_dir / "screen.log"
    os.system(f"cp {SCREEN_LOG} {dest} 2>/dev/null || true")
    print(f"   Screen log saved -> {dest}")


# ── Main ──────────────────────────────────────────────────────────────────────


def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--skip-setup", action="store_true",
                   help="Skip cluster setup (assume already running)")
    p.add_argument("--skip-seed", action="store_true",
                   help="Skip data seeding (assume already seeded)")
    p.add_argument("--layer3-only", action="store_true",
                   help="Run only Layer 3 (EMULATE_UNREPLICATED mode)")
    p.add_argument("--rates", type=str, default=None,
                   help="Comma-separated list of offered rates (req/s)")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()

    print("=" * 60)
    print("PB Overhead Layer-by-Layer Decomposition")
    print("=" * 60)

    # Determine rates
    if args.rates:
        rates = [int(r.strip()) for r in args.rates.split(",")]
    else:
        rates = DEFAULT_RATES

    if not args.skip_setup:
        # Phase 1 — Reset cluster
        print("\n[Phase 1] Force-clearing cluster ...")
        clear_xdn_cluster()

        # Phase 2 — Start cluster
        print("\n[Phase 2] Starting XDN cluster ...")
        start_cluster()

        # Phase 3 — Wait for ARs
        print("\n[Phase 3] Waiting for Active Replicas ...")
        for host in AR_HOSTS:
            if not wait_for_port(host, HTTP_PROXY_PORT, TIMEOUT_PORT_SEC):
                print(f"ERROR: AR at {host}:{HTTP_PROXY_PORT} not ready.")
                print(f"       Check: tail -f {SCREEN_LOG}")
                sys.exit(1)

        # Phase 3b — Ensure images on RC
        print("\n[Phase 3b] Ensuring Docker images are present on RC ...")
        ensure_docker_images_on_rc()

        # Phase 4 — Launch bookcatalog-nd
        print("\n[Phase 4] Launching bookcatalog-nd service ...")
        cmd = (
            f"XDN_CONTROL_PLANE={CONTROL_PLANE_HOST} {XDN_BINARY} "
            f"launch {SERVICE_NAME} --file={BOOKCATALOG_YAML}"
        )
        print(f"   {cmd}")
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        print(result.stdout)
        if result.returncode != 0:
            print(f"ERROR: xdn launch failed:\n{result.stderr}")
            sys.exit(1)

        # Phase 5 — Wait for HTTP readiness
        print("\n[Phase 5] Waiting for bookcatalog-nd HTTP readiness ...")
        ok, _ = wait_for_service(AR_HOSTS, HTTP_PROXY_PORT, SERVICE_NAME, TIMEOUT_APP_SEC)
        if not ok:
            print(f"ERROR: bookcatalog-nd not ready after {TIMEOUT_APP_SEC}s.")
            copy_screen_log(RESULTS_BASE)
            sys.exit(1)
    else:
        print("\n[Phase 1-5] Skipping setup (--skip-setup)")

    # Phase 6 — Detect primary
    print("\n[Phase 6] Detecting primary node ...")
    primary = detect_primary_via_docker(AR_HOSTS)
    if not primary:
        print("   WARNING: Could not detect primary via docker, falling back to first AR")
        primary = AR_HOSTS[0]
    print(f"   Primary: {primary}")

    if not args.skip_seed:
        # Phase 7 — Seed books
        print("\n[Phase 7] Seeding books ...")
        for attempt in range(1, 4):
            created = seed_books(primary, HTTP_PROXY_PORT, SERVICE_NAME, count=200)
            if created > 0:
                break
            print(f"   Attempt {attempt} failed, retrying in 10s ...")
            time.sleep(10)
        else:
            print("ERROR: Failed to seed books after 3 attempts.")
            copy_screen_log(RESULTS_BASE)
            sys.exit(1)

        # Phase 8 — Verify app
        print("\n[Phase 8] Verifying app readiness ...")
        if not wait_for_app_ready(primary, HTTP_PROXY_PORT, SERVICE_NAME, timeout_sec=60):
            print("ERROR: App not ready after seeding.")
            copy_screen_log(RESULTS_BASE)
            sys.exit(1)
    else:
        print("\n[Phase 7-8] Skipping data seeding (--skip-seed)")

    all_layers = {}

    if args.layer3_only:
        # ── Layer 3: Full PB without Paxos consensus ─────────────────────
        print("\n" + "=" * 60)
        print("[Layer 3] Full PB without Paxos (EMULATE_UNREPLICATED=true)")
        print("=" * 60)
        print("   Ensure EMULATE_UNREPLICATED=true in config, rebuild, and redeploy.")

        layer3_dir = RESULTS_BASE / "layer3_unreplicated"
        layer3_results = run_layer_sweep(
            primary, "L3-unrepl", layer3_dir, rates,
            extra_headers=None,  # normal request path
        )
        all_layers["L3-unrepl"] = layer3_results
        print_summary_table("Layer 3: Full PB (EMULATE_UNREPLICATED)", layer3_results)

    else:
        # ── Layer 0: Pure Netty HTTP Server (___DDR) ─────────────────────
        print("\n" + "=" * 60)
        print("[Layer 0] Pure Netty HTTP Server (___DDR dummy response)")
        print("=" * 60)

        layer0_dir = RESULTS_BASE / "layer0_ddr"
        layer0_results = run_layer_sweep(
            primary, "L0-DDR", layer0_dir, rates,
            extra_headers=["___DDR: true"],
        )
        all_layers["L0-DDR"] = layer0_results

        # ── Layer 1: Netty Proxy Forwarding (___DBC) ─────────────────────
        print("\n" + "=" * 60)
        print("[Layer 1] Netty Proxy → Container (___DBC bypass coordination)")
        print("=" * 60)

        container_port = detect_container_port(primary)
        if container_port is None:
            print("ERROR: Cannot detect container port. Skipping Layer 1.")
            layer1_results = []
        else:
            layer1_dir = RESULTS_BASE / "layer1_dbc"
            layer1_results = run_layer_sweep(
                primary, "L1-DBC", layer1_dir, rates,
                extra_headers=[
                    "___DBC: true",
                    f"___DBCP: {container_port}",
                ],
            )
        all_layers["L1-DBC"] = layer1_results

        # ── Layer 2: Direct App Execute (___DDE) ─────────────────────────
        print("\n" + "=" * 60)
        print("[Layer 2] Direct App Execute (___DDE skip coordinator)")
        print("=" * 60)

        layer2_dir = RESULTS_BASE / "layer2_dde"
        layer2_results = run_layer_sweep(
            primary, "L2-DDE", layer2_dir, rates,
            extra_headers=["___DDE: true"],
        )
        all_layers["L2-DDE"] = layer2_results

    # ── Summary ──────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)

    for layer_name, results in all_layers.items():
        print_summary_table(layer_name, results)

    if len(all_layers) > 1:
        print_decomposition(all_layers)

    # Save screen log
    copy_screen_log(RESULTS_BASE)

    print(f"\n[Done] Results in {RESULTS_BASE}/")
