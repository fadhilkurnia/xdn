"""
run_load_pb_wordpress_reflex.py — WordPress PB per-rate isolation benchmark.

For each rate point, spins up a **fresh** WordPress PB cluster (forceclear,
start, deploy, install, seed 200 posts) so that MySQL InnoDB WAL growth and
statediff accumulation from earlier rates cannot contaminate later measurements.

Workload: XML-RPC wp.editPost — edits existing posts via /xmlrpc.php.

Run from the eval/ directory:
    python3 run_load_pb_wordpress_reflex.py [--rates 100,200,300,400,500]

Outputs go to eval/results/load_pb_wordpress_reflex_<timestamp>/:
    rate100.txt  rate200.txt  ...  — go-client output per rate point
    screen_rate<N>.log             — per-rate XDN screen log
    screen.log                     — concatenated screen log for timing analysis
"""

import argparse
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import run_load_pb_wordpress_common as _wpmod
from run_load_pb_wordpress_common import (
    ADMIN_PASSWORD,
    ADMIN_USER,
    AR_HOSTS,
    CONTROL_PLANE_HOST,
    HTTP_PROXY_PORT,
    IMAGE_SOURCE_HOST,
    REQUIRED_IMAGES,
    SCREEN_LOG,
    SERVICE_NAME,
    TIMEOUT_PORT_SEC,
    TIMEOUT_WP_SEC,
    WORDPRESS_YAML,
    XDN_BINARY,
    check_rest_api,
    clear_xdn_cluster,
    create_post,
    disable_wp_cron,
    enable_rest_auth,
    install_wordpress,
    start_cluster,
    wait_for_port,
    wait_for_service,
)

# ── Config ────────────────────────────────────────────────────────────────────

# Rates to sweep (req/s) — extended sweep to find XDN PB saturation knee
BOTTLENECK_RATES = [
    1, 100, 200, 300, 400, 500, 600, 700, 800, 900,
    1000, 1200, 1500, 2000, 2500, 3000, 3500, 4000,
    4500, 5000, 6000, 7000
]

# Duration per rate point (seconds)
LOAD_DURATION_SEC = 60

# Warmup duration before each rate measurement to prime MySQL and stabilize statediffs.
WARMUP_DURATION_SEC = 15

# Seconds to pause between rate points (kept for backwards compat with other scripts
# that import this constant; this script uses per-rate isolation instead).
INTER_RATE_PAUSE_SEC = 30

# Average latency threshold above which we consider the system saturated and stop early
AVG_LATENCY_THRESHOLD_MS = 1_000

# Seed post count — need enough distinct rows to avoid row-lock collisions across a batch
SEED_POST_COUNT = 200

# Output directory for XDN PB results (set dynamically in __main__ with timestamp)
RESULTS_BASE = Path(__file__).resolve().parent / "results"
RESULTS_DIR = None  # set in __main__ after timestamp computation
GO_LATENCY_CLIENT = Path(__file__).resolve().parent / "get_latency_at_rate.go"

# Path to pre-generated files (set dynamically alongside RESULTS_DIR)
URLS_FILE = None        # kept for backwards compat
PAYLOADS_FILE = None    # XML-RPC payloads file (one per line)


# ── XML-RPC Helpers ──────────────────────────────────────────────────────────


def _xmlrpc_editpost_payload(post_id, password=ADMIN_PASSWORD):
    """Generate a single-line XML-RPC wp.editPost payload."""
    return (
        '<?xml version="1.0"?>'
        "<methodCall>"
        "<methodName>wp.editPost</methodName>"
        "<params>"
        "<param><value><int>1</int></value></param>"
        f"<param><value><string>{ADMIN_USER}</string></value></param>"
        f"<param><value><string>{password}</string></value></param>"
        f"<param><value><int>{post_id}</int></value></param>"
        "<param><value><struct>"
        "<member><name>post_content</name>"
        "<value><string>Edit workload test content.</string></value></member>"
        "</struct></value></param>"
        "</params>"
        "</methodCall>"
    )


# ── Helpers ───────────────────────────────────────────────────────────────────


def ensure_docker_images_on_rc():
    """Mirror required Docker images to the RC node (same logic as other scripts)."""
    for img in REQUIRED_IMAGES:
        check = subprocess.run(
            ["ssh", CONTROL_PLANE_HOST, f"docker image inspect {img}"],
            capture_output=True,
        )
        if check.returncode == 0:
            print(f"   {img}: already on RC, skipping")
            continue

        src_check = subprocess.run(
            ["ssh", IMAGE_SOURCE_HOST, f"docker image inspect {img}"],
            capture_output=True,
        )
        if src_check.returncode != 0:
            print(f"   {img}: pulling on {IMAGE_SOURCE_HOST} ...")
            ret = os.system(f"ssh {IMAGE_SOURCE_HOST} 'docker pull {img}'")
            if ret != 0:
                print(f"   ERROR: failed to pull {img} on {IMAGE_SOURCE_HOST}")
                sys.exit(1)

        print(f"   {img}: transferring {IMAGE_SOURCE_HOST} → {CONTROL_PLANE_HOST} ...")
        ret = os.system(
            f"ssh {IMAGE_SOURCE_HOST} 'docker save {img}'"
            f" | ssh {CONTROL_PLANE_HOST} 'docker load'"
        )
        if ret != 0:
            print(f"   ERROR: failed to transfer {img} to RC")
            sys.exit(1)
        print(f"   {img}: done")


def run_load_point(primary, rate, duration_sec=LOAD_DURATION_SEC):
    """Run get_latency_at_rate.go at `rate` req/s for `duration_sec` seconds.

    Uses XML-RPC wp.editPost via -payloads-file for round-robin post editing.
    Returns a dict with throughput_rps, avg_ms, p50_ms, p90_ms, p95_ms, p99_ms.
    """
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    output_file = RESULTS_DIR / f"rate{rate}.txt"
    url = f"http://{primary}:{HTTP_PROXY_PORT}/xmlrpc.php"
    cmd = [
        "go", "run", str(GO_LATENCY_CLIENT),
        "-H", f"XDN: {SERVICE_NAME}",
        "-H", "Content-Type: text/xml",
        "-X", "POST",
        "-payloads-file", str(PAYLOADS_FILE),
        url,
        "placeholder",
        str(duration_sec),
        str(rate),
    ]
    env = os.environ.copy()
    env["GO111MODULE"] = "off"
    env["GOWORK"] = "off"
    print(f"   Output → {output_file}")
    with open(output_file, "w") as fh:
        result = subprocess.run(cmd, stdout=fh, stderr=subprocess.STDOUT,
                                env=env, text=True)
    if result.returncode != 0:
        print(f"   WARNING: go client exited {result.returncode}")
    return _parse_go_output(output_file)


def _parse_go_output(path):
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
        "throughput_rps":        metrics.get("actual_throughput_rps", 0.0),
        "actual_achieved_rps":   metrics.get("actual_achieved_rate_rps", 0.0),
        "avg_ms":                metrics.get("average_latency_ms", 0.0),
        "p50_ms":                metrics.get("median_latency_ms", 0.0),
        "p90_ms":                metrics.get("p90_latency_ms", 0.0),
        "p95_ms":                metrics.get("p95_latency_ms", 0.0),
        "p99_ms":                metrics.get("p99_latency_ms", 0.0),
    }


def detect_primary_via_docker(hosts):
    """Detect primary by finding which AR node has running Docker containers."""
    print("   Detecting primary for 'wordpress' via docker ps ...")
    for host in hosts:
        result = subprocess.run(
            ["ssh", host, "docker ps -q"],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0 and result.stdout.strip():
            containers = result.stdout.strip().splitlines()
            print(f"   {host} has {len(containers)} running container(s) → PRIMARY")
            return host
        else:
            print(f"   {host}: no running containers")
    return None


def tune_mysql(primary):
    """Tune MySQL InnoDB settings on the primary for write-heavy workloads.

    Increases buffer pool to 1GB (from default 128MB) to avoid page eviction
    during sustained high write rates, and sets flush mode to async to reduce
    fsync overhead.
    """
    print("   [setup] Tuning MySQL InnoDB settings ...")
    # Find the MySQL container (the one running the mysql image)
    find_cmd = "docker ps --format '{{.Names}} {{.Image}}' | grep mysql | awk '{print $1}' | head -1"
    result = subprocess.run(
        ["ssh", primary, find_cmd],
        capture_output=True, text=True, timeout=10,
    )
    mysql_container = result.stdout.strip()
    if not mysql_container:
        print("   WARNING: Could not find MySQL container, skipping tuning")
        return

    tune_sql = (
        "SET GLOBAL innodb_buffer_pool_size=1073741824; "       # 1 GB
        "SET GLOBAL innodb_flush_log_at_trx_commit=2; "         # async flush to OS buffer
        "SET GLOBAL innodb_change_buffer_max_size=50; "          # allow more change buffering
        "SET GLOBAL innodb_max_dirty_pages_pct=90; "             # delay page flush (reduce background statediff)
        "SET GLOBAL innodb_max_dirty_pages_pct_lwm=80; "         # high-water mark before eager flush
        "SET GLOBAL innodb_io_capacity=200; "                    # limit background I/O (fewer flushes)
        "SET GLOBAL innodb_io_capacity_max=400; "                # cap burst I/O
        "SET GLOBAL innodb_lru_scan_depth=256; "                 # reduce per-second page cleaner work
    )
    tune_cmd = (
        f"docker exec {mysql_container} "
        f"mysql -psupersecret -e \"{tune_sql}\""
    )
    result = subprocess.run(
        ["ssh", primary, tune_cmd],
        capture_output=True, text=True, timeout=30,
    )
    if result.returncode == 0:
        print(f"   [setup] MySQL tuned on container {mysql_container}")
    else:
        print(f"   WARNING: MySQL tuning failed: {result.stderr.strip()}")
    # Give InnoDB a moment to resize the buffer pool
    time.sleep(2)


def warmup_rate_point(primary, rate, warmup_duration_sec=WARMUP_DURATION_SEC):
    """Run a throwaway warmup load at `rate` for `warmup_duration_sec` seconds."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    output_file = RESULTS_DIR / f"warmup_rate{rate}.txt"
    url = f"http://{primary}:{HTTP_PROXY_PORT}/xmlrpc.php"
    cmd = [
        "go", "run", str(GO_LATENCY_CLIENT),
        "-H", f"XDN: {SERVICE_NAME}",
        "-H", "Content-Type: text/xml",
        "-X", "POST",
        "-payloads-file", str(PAYLOADS_FILE),
        url,
        "placeholder",
        str(warmup_duration_sec),
        str(rate),
    ]
    env = os.environ.copy()
    env["GO111MODULE"] = "off"
    env["GOWORK"] = "off"
    with open(output_file, "w") as fh:
        subprocess.run(cmd, stdout=fh, stderr=subprocess.STDOUT, env=env, text=True)
    m = _parse_go_output(output_file)
    print(f"     [warmup] tput={m['throughput_rps']:.2f} rps  avg={m['avg_ms']:.1f}ms")
    return m


def copy_screen_log_for_rate(rate):
    """Copy the running screen log snapshot into results dir, tagged by rate."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    dest = RESULTS_DIR / f"screen_rate{rate}.log"
    os.system(f"cp {SCREEN_LOG} {dest} 2>/dev/null || true")
    print(f"   Screen log saved → {dest}")
    return dest


# ── Fresh Cluster Setup ──────────────────────────────────────────────────────


def setup_fresh_wordpress_cluster():
    """Forceclear, start, deploy WordPress, install, seed posts, write payloads file.

    Returns the primary host or exits on failure.
    """
    print("   [setup] Force-clearing cluster ...")
    clear_xdn_cluster()

    print("   [setup] Starting XDN cluster ...")
    start_cluster()

    print("   [setup] Waiting for Active Replicas ...")
    for host in AR_HOSTS:
        if not wait_for_port(host, HTTP_PROXY_PORT, TIMEOUT_PORT_SEC):
            print(f"ERROR: AR at {host}:{HTTP_PROXY_PORT} not ready.")
            print(f"       Check: tail -f {SCREEN_LOG}")
            sys.exit(1)

    print("   [setup] Ensuring Docker images on RC ...")
    ensure_docker_images_on_rc()

    print("   [setup] Launching WordPress service ...")
    cmd = (
        f"XDN_CONTROL_PLANE={CONTROL_PLANE_HOST} {XDN_BINARY} "
        f"launch {SERVICE_NAME} --file={WORDPRESS_YAML}"
    )
    print(f"   {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    print(result.stdout.strip())
    if result.returncode != 0:
        print(f"ERROR: xdn launch failed:\n{result.stderr}")
        sys.exit(1)

    print("   [setup] Waiting for WordPress HTTP readiness ...")
    ok, _ = wait_for_service(AR_HOSTS, HTTP_PROXY_PORT, SERVICE_NAME, TIMEOUT_WP_SEC)
    if not ok:
        print(f"ERROR: WordPress not ready after {TIMEOUT_WP_SEC}s.")
        sys.exit(1)

    # Wait for PB init sync (rsync of MySQL data to backup replicas).
    # WordPress MySQL state can be 100-200MB; rsync takes ~60-120s.
    # Without this wait, statediff captures are skipped and requests stall.
    print("   [setup] Waiting for PB init sync to complete ...")
    init_sync_deadline = time.time() + 300
    while time.time() < init_sync_deadline:
        # Check if the screen log shows postInitialization on backup replicas
        try:
            with open(SCREEN_LOG, "r") as f:
                log_content = f.read()
            backup_init_count = log_content.count("Running postInitialization() in backup")
            if backup_init_count >= 2:
                print(f"   [setup] PB init sync complete ({backup_init_count} backups initialized)")
                break
        except FileNotFoundError:
            pass
        elapsed = int(time.time() - (init_sync_deadline - 300))
        if elapsed % 30 == 0:
            print(f"   [setup] ... {elapsed}s elapsed, waiting for backup init sync ...")
        time.sleep(5)
    else:
        print("   WARNING: PB init sync may not have completed after 300s — proceeding anyway")
    # Extra settle time after init sync
    time.sleep(10)

    print("   [setup] Detecting primary ...")
    primary = detect_primary_via_docker(AR_HOSTS)
    if not primary:
        print("   WARNING: Could not detect primary via docker, falling back to first AR")
        primary = AR_HOSTS[0]
    print(f"   [setup] Primary: {primary}")

    print("   [setup] Installing WordPress ...")
    if not install_wordpress(primary, HTTP_PROXY_PORT, SERVICE_NAME):
        print("ERROR: WordPress installation failed.")
        sys.exit(1)

    print("   [setup] Enabling REST API auth ...")
    if not enable_rest_auth(primary):
        print("ERROR: REST API auth setup failed.")
        sys.exit(1)

    print("   [setup] Disabling WP-Cron and auto-updates ...")
    disable_wp_cron(primary)

    print("   [setup] Checking REST API availability ...")
    if not check_rest_api(primary, HTTP_PROXY_PORT, SERVICE_NAME):
        print("ERROR: REST API not available.")
        sys.exit(1)

    tune_mysql(primary)

    print(f"   [setup] Creating {SEED_POST_COUNT} seed posts ...")
    seed_ids = []
    for i in range(SEED_POST_COUNT):
        title = f"Seed Post {i + 1}"
        content = f"Seed content {i + 1}."
        pid = None
        for attempt in range(1, 4):
            try:
                pid = create_post(primary, HTTP_PROXY_PORT, SERVICE_NAME, title, content)
                if pid:
                    break
            except Exception as e:
                print(f"   WARNING: '{title}' attempt {attempt} failed ({e}), retrying ...")
                time.sleep(5)
        if pid:
            seed_ids.append(pid)
        else:
            print(f"   WARNING: seed post '{title}' failed, continuing")
    print(f"   [setup] Seed complete: {len(seed_ids)}/{SEED_POST_COUNT} posts created")

    print(f"   [setup] Writing {len(seed_ids)} XML-RPC editPost payloads → {PAYLOADS_FILE} ...")
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    with open(PAYLOADS_FILE, "w") as fh:
        for pid in seed_ids:
            fh.write(_xmlrpc_editpost_payload(pid, ADMIN_PASSWORD) + "\n")
    print(f"   [setup] Done: {len(seed_ids)} payloads written.")

    return primary


# ── CLI ───────────────────────────────────────────────────────────────────────


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--rates", type=str, default=None,
                   help="Comma-separated rates to override default BOTTLENECK_RATES")
    p.add_argument("--duration", type=int, default=LOAD_DURATION_SEC,
                   help=f"Measurement duration per rate point in seconds (default: {LOAD_DURATION_SEC})")
    p.add_argument("--warmup-duration", type=int, default=WARMUP_DURATION_SEC,
                   help=f"Warmup duration per rate point in seconds (default: {WARMUP_DURATION_SEC})")
    p.add_argument("--skip-setup", action="store_true",
                   help="Skip cluster setup for ALL rates (assume already running)")
    return p.parse_args()


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    args = parse_args()

    # Compute timestamped results directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    RESULTS_DIR = RESULTS_BASE / f"load_pb_wordpress_reflex_{timestamp}"
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    PAYLOADS_FILE = RESULTS_DIR / "xmlrpc_payloads.txt"

    # Determine rate list and durations
    rates = [int(r) for r in args.rates.split(",")] if args.rates else BOTTLENECK_RATES
    load_duration = args.duration
    warmup_duration = args.warmup_duration

    print("=" * 60)
    print("WordPress PB Throughput-Ceiling Investigation")
    print(f"  Mode     : fresh cluster per rate (isolated)")
    print(f"  Workload : XML-RPC wp.editPost")
    print(f"  Rates    : {rates}")
    print(f"  Duration : {load_duration}s per rate (warmup: {warmup_duration}s)")
    print(f"  Results  → {RESULTS_DIR}")
    print("=" * 60)

    results = []
    screen_logs = []  # paths of per-rate screen logs

    for i, rate in enumerate(rates):
        print(f"\n{'='*60}")
        print(f"  Rate point {i+1}/{len(rates)}: {rate} req/s")
        print(f"{'='*60}")

        # ── Per-rate fresh cluster setup ──────────────────────────────
        if not args.skip_setup:
            primary = setup_fresh_wordpress_cluster()
        else:
            print("   [setup] Skipped (--skip-setup)")
            primary = detect_primary_via_docker(AR_HOSTS)
            if not primary:
                print("ERROR: Could not detect primary via docker.")
                sys.exit(1)
            print(f"   Primary: {primary}")

        # ── Warmup ────────────────────────────────────────────────────
        print(f"\n   -> rate={rate} req/s (warmup {warmup_duration}s) ...")
        warmup_result = warmup_rate_point(primary, rate, warmup_duration)
        if warmup_result["throughput_rps"] < 0.1 * rate:
            print(
                f"   SKIP: warmup tput={warmup_result['throughput_rps']:.2f} rps "
                f"< 10% of {rate} rps — system saturated"
            )
            log_path = copy_screen_log_for_rate(rate)
            screen_logs.append(log_path)
            continue
        print(f"   Settling 5s after warmup ...")
        time.sleep(5)

        # ── Measurement ───────────────────────────────────────────────
        print(f"   -> rate={rate} req/s (measuring {load_duration}s) ...")
        m = run_load_point(primary, rate, load_duration)
        row = {"rate_rps": rate, **m}
        results.append(row)
        print(
            f"     tput={m['throughput_rps']:.2f} rps  "
            f"avg={m['avg_ms']:.1f}ms  "
            f"p50={m['p50_ms']:.1f}ms  "
            f"p90={m['p90_ms']:.1f}ms  "
            f"p95={m['p95_ms']:.1f}ms"
        )

        # ── Per-rate log collection ───────────────────────────────────
        log_path = copy_screen_log_for_rate(rate)
        screen_logs.append(log_path)

        # ── Early stop on saturation ──────────────────────────────────
        # Stop only when BOTH conditions are met so that both load-latency
        # and throughput graphs show a clear hockey-stick shape.
        latency_saturated = m["avg_ms"] > AVG_LATENCY_THRESHOLD_MS
        throughput_saturated = (
            m["actual_achieved_rps"] > 0
            and m["throughput_rps"] < 0.75 * m["actual_achieved_rps"]
        )
        if latency_saturated and throughput_saturated:
            print(
                f"   Avg latency {m['avg_ms']:.0f}ms > {AVG_LATENCY_THRESHOLD_MS}ms "
                f"AND throughput {m['throughput_rps']:.2f} rps < 75% of offered "
                f"{m['actual_achieved_rps']:.2f} rps — stopping sweep early."
            )
            break

    # ── Concatenate per-rate screen logs for timing analysis ──────────
    combined_log = RESULTS_DIR / "screen.log"
    with open(combined_log, "w") as out:
        for log_path in screen_logs:
            if log_path.exists():
                out.write(log_path.read_text())
    print(f"\n   Combined screen log → {combined_log}")

    # ── Summary ───────────────────────────────────────────────────────
    print("\n[Summary]")
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

    # ── Generate plots and timing analysis ────────────────────────────
    print("\n[Plots] Generating plots ...")
    eval_dir = str(Path(__file__).resolve().parent)
    for script in ["investigate_parse_pb_timing.py", "plot_load_latency.py"]:
        cmd = ["python3", script, "--results-dir", str(RESULTS_DIR)]
        if script == "investigate_parse_pb_timing.py":
            cmd += ["--log", str(combined_log)]
        r = subprocess.run(
            cmd,
            capture_output=True, text=True,
            cwd=eval_dir,
        )
        print(r.stdout)
        if r.returncode != 0:
            print(f"   WARNING: {script} failed:\n{r.stderr}")

    # ── Compare against run9 newPost baseline ─────────────────────────
    print("\n[Comparison] Generating comparison plot ...")
    baseline_dir = Path(__file__).resolve().parent / "results" / "bottleneck"
    if baseline_dir.exists():
        r = subprocess.run(
            [
                "python3", "plot_comparison.py",
                "--dir1", str(baseline_dir),
                "--label1", "newPost (run9)",
                "--dir2", str(RESULTS_DIR),
                "--label2", "xmlrpc editPost (isolated)",
                "--out", str(RESULTS_DIR / "comparison_xmlrpc.png"),
            ],
            capture_output=True, text=True,
            cwd=eval_dir,
        )
        print(r.stdout)
        if r.returncode != 0:
            print(f"   WARNING: plot_comparison.py failed:\n{r.stderr}")
    else:
        print(f"   Skipping: baseline dir not found at {baseline_dir}")

    print(f"\n[Done] Results in {RESULTS_DIR}/")
