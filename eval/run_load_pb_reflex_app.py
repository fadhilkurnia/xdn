"""
run_load_pb_reflex_app.py — Consolidated XDN Primary-Backup benchmark script.

Supports multiple applications (wordpress, bookcatalog, tpcc, hotelres) via the
pb_app_configs registry.  For each rate point, spins up a **fresh** cluster
(forceclear, start, deploy, setup, seed) so that state growth from earlier rates
cannot contaminate later measurements.

Run from the eval/ directory:
    python3 run_load_pb_reflex_app.py --app wordpress [--rates 100,200,...] \
        [--duration 60] [--skip-setup] [--sanity-check] [--sample-latency] \
        [--fixed-accumulation] [--jfr] [--apache-timing]

Valid apps: wordpress, bookcatalog, tpcc, hotelres (NOT synth — stays separate)

Outputs go to eval/results/load_pb_<app>_reflex_<timestamp>/:
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

from pb_common import (
    AR_HOSTS,
    BOTTLENECK_RATES,
    CONTROL_PLANE_HOST,
    HTTP_PROXY_PORT,
    LOAD_DURATION_SEC,
    WARMUP_DURATION_SEC,
    XDN_BINARY,
    check_saturation,
    clear_xdn_cluster,
    copy_screen_log,
    detect_primary_via_docker,
    ensure_docker_images,
    parse_go_output,
    print_metrics,
    run_load,
    save_effective_config,
    start_cluster,
    wait_for_port,
    wait_for_service,
)

from pb_app_configs import (
    APP_CONFIGS,
    check_app_ready,
    get_workload_args,
    seed_app,
    setup_app,
)

# ── Constants ────────────────────────────────────────────────────────────────

VALID_APPS = ["wordpress", "bookcatalog", "tpcc", "tpcc-java", "hotelres"]
TIMEOUT_PORT_SEC = 60
INIT_SYNC_TIMEOUT = 90
RESULTS_BASE = Path(__file__).resolve().parent / "results"
GO_LATENCY_CLIENT = Path(__file__).resolve().parent / "get_latency_at_rate.go"


# ── Fresh Cluster Setup ─────────────────────────────────────────────────────


def setup_fresh_cluster(app_name, cfg, results_dir, screen_session, screen_log,
                        enable_apache_timing=False):
    """Forceclear, start, deploy, setup, and seed a fresh PB cluster.

    Returns the primary host or exits on failure.
    """
    service = cfg["service_name"]
    gp_jvm_args = cfg["gp_jvm_args"]

    print("   [setup] Force-clearing cluster ...")
    clear_xdn_cluster()

    print("   [setup] Starting XDN cluster ...")
    gp_jvm_args = start_cluster(
        cfg["gp_config"], gp_jvm_args, screen_session, screen_log
    )

    print("   [setup] Waiting for Active Replicas ...")
    for host in AR_HOSTS:
        if not wait_for_port(host, HTTP_PROXY_PORT, TIMEOUT_PORT_SEC):
            print(f"ERROR: AR at {host}:{HTTP_PROXY_PORT} not ready.")
            print(f"       Check: tail -f {screen_log}")
            sys.exit(1)

    print("   [setup] Ensuring Docker images on RC ...")
    ensure_docker_images(cfg["images"], [CONTROL_PLANE_HOST])

    print(f"   [setup] Launching '{service}' service ...")
    cmd = (
        f"XDN_CONTROL_PLANE={CONTROL_PLANE_HOST} {XDN_BINARY} "
        f"launch {service} --file={cfg['yaml']}"
    )
    print(f"   {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    print(result.stdout.strip())
    if result.returncode != 0:
        print(f"ERROR: xdn launch failed:\n{result.stderr}")
        sys.exit(1)

    print(f"   [setup] Waiting for '{service}' HTTP readiness ...")
    ok, _ = wait_for_service(
        AR_HOSTS, HTTP_PROXY_PORT, service, cfg["timeout_app_sec"]
    )
    if not ok:
        print(f"ERROR: '{service}' not ready after {cfg['timeout_app_sec']}s.")
        sys.exit(1)

    # Wait for PB init sync to complete on the primary.
    # Look for "Completed initContainerSync" in the screen log.
    print("   [setup] Waiting for PB init sync to complete ...")
    init_sync_deadline = time.time() + INIT_SYNC_TIMEOUT
    while time.time() < init_sync_deadline:
        try:
            with open(screen_log, "r") as f:
                log_content = f.read()
            if "Completed initContainerSync" in log_content:
                print("   [setup] initContainerSync completed on primary")
                break
        except FileNotFoundError:
            pass
        elapsed = int(time.time() - (init_sync_deadline - INIT_SYNC_TIMEOUT))
        if elapsed % 15 == 0:
            print(f"   [setup] ... {elapsed}s elapsed, waiting for initContainerSync ...")
        time.sleep(3)
    else:
        print(
            f"   WARNING: initContainerSync not detected after "
            f"{INIT_SYNC_TIMEOUT}s -- proceeding anyway"
        )
    # Brief settle time after init sync
    time.sleep(5)

    print("   [setup] Detecting primary ...")
    primary = detect_primary_via_docker(AR_HOSTS)
    if not primary:
        print("   WARNING: Could not detect primary via docker, falling back to first AR")
        primary = AR_HOSTS[0]
    print(f"   [setup] Primary: {primary}")

    print(f"   [setup] Running app-specific setup for '{app_name}' ...")
    if not setup_app(app_name, primary, HTTP_PROXY_PORT, service):
        print(f"ERROR: setup_app({app_name!r}) failed.")
        sys.exit(1)

    # Enable Apache %D timing header for WordPress (if requested)
    if app_name == "wordpress" and enable_apache_timing:
        print("   [setup] Enabling Apache timing header ...")
        r = subprocess.run(
            ["ssh", primary, "docker ps -q --filter ancestor=wordpress:6.5.4-apache"],
            capture_output=True, text=True,
        )
        wp_cid = r.stdout.strip().split("\n")[0]
        if wp_cid:
            subprocess.run(
                ["ssh", primary,
                 f"docker exec {wp_cid} bash -c "
                 "'a2enmod headers > /dev/null 2>&1; "
                 "echo '\"'\"'Header set X-Request-Time \"%D\"'\"'\"' "
                 ">> /etc/apache2/apache2.conf; apache2ctl graceful'"],
                capture_output=True, text=True,
            )
            print("   [setup] Apache %D header enabled")
        else:
            print("   WARNING: could not find WordPress container for Apache timing")

    print(f"   [setup] Seeding '{app_name}' ...")
    seed_app(
        app_name, primary, HTTP_PROXY_PORT, service,
        cfg["seed_count"], str(results_dir),
    )

    return primary


# ── Load Helpers ─────────────────────────────────────────────────────────────


def run_load_point(app_name, primary, rate, duration_sec, results_dir):
    """Run get_latency_at_rate.go at `rate` req/s for `duration_sec` seconds.

    Uses get_workload_args() to build the correct CLI arguments for the app.
    Returns a parsed metrics dict.
    """
    wl = get_workload_args(
        app_name, primary, HTTP_PROXY_PORT,
        APP_CONFIGS[app_name]["service_name"], str(results_dir),
    )
    output_file = results_dir / f"rate{rate}.txt"

    go_args = (
        wl["headers"]
        + wl["method"]
        + wl["extra_args"]
        + [wl["url"], wl["body"], str(duration_sec), str(rate)]
    )
    return run_load(go_args, output_file)


def warmup_load(app_name, primary, warmup_rate, warmup_duration, results_dir):
    """Run a throwaway warmup load at `warmup_rate` for `warmup_duration` seconds."""
    wl = get_workload_args(
        app_name, primary, HTTP_PROXY_PORT,
        APP_CONFIGS[app_name]["service_name"], str(results_dir),
    )
    output_file = results_dir / f"warmup_rate{warmup_rate}.txt"

    go_args = (
        wl["headers"]
        + wl["method"]
        + wl["extra_args"]
        + [wl["url"], wl["body"], str(warmup_duration), str(warmup_rate)]
    )
    m = run_load(go_args, output_file)
    print(f"     [warmup] tput={m['throughput_rps']:.2f} rps  avg={m['avg_ms']:.1f}ms")
    return m


# ── CLI ──────────────────────────────────────────────────────────────────────


def parse_args():
    p = argparse.ArgumentParser(
        description="Consolidated XDN Primary-Backup benchmark script."
    )
    p.add_argument(
        "--app", type=str, required=True, choices=VALID_APPS,
        help=f"Application to benchmark ({', '.join(VALID_APPS)})",
    )
    p.add_argument(
        "--rates", type=str, default=None,
        help="Comma-separated rates to override default BOTTLENECK_RATES",
    )
    p.add_argument(
        "--duration", type=int, default=LOAD_DURATION_SEC,
        help=f"Measurement duration per rate point in seconds (default: {LOAD_DURATION_SEC})",
    )
    p.add_argument(
        "--warmup-duration", type=int, default=WARMUP_DURATION_SEC,
        help=f"Warmup duration per rate point in seconds (default: {WARMUP_DURATION_SEC})",
    )
    p.add_argument(
        "--settle", type=int, default=5,
        help="Settle period in seconds between warmup and measurement (default: 5)",
    )
    p.add_argument(
        "--skip-setup", action="store_true",
        help="Skip cluster setup for ALL rates (assume already running)",
    )
    p.add_argument(
        "--sanity-check", action="store_true",
        help="Run per-request I/O sanity check before measurements",
    )
    p.add_argument(
        "--sample-latency", action="store_true",
        help="Enable sampled pipeline latency breakdown (PB_SAMPLE_LATENCY)",
    )
    p.add_argument(
        "--fixed-accumulation", action="store_true",
        help="Disable adaptive capture accumulation (fix window at 100us)",
    )
    p.add_argument(
        "--jfr", action="store_true",
        help="Enable Java Flight Recorder profiling during measurement",
    )
    p.add_argument(
        "--apache-timing", action="store_true",
        help="Enable Apache %%D response header for per-request timing (WordPress only)",
    )
    return p.parse_args()


# ── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    args = parse_args()
    app_name = args.app
    cfg = APP_CONFIGS[app_name]
    service = cfg["service_name"]

    # Compute timestamped results directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_dir = RESULTS_BASE / f"load_pb_{app_name}_reflex_{timestamp}"
    results_dir.mkdir(parents=True, exist_ok=True)

    # Screen session naming
    screen_session = f"xdn_{app_name}_pb"
    screen_log = f"screen_logs/{screen_session}.log"

    # Determine rate list and durations
    rates = [int(r) for r in args.rates.split(",")] if args.rates else BOTTLENECK_RATES
    load_duration = args.duration
    warmup_duration = args.warmup_duration
    warmup_rate = cfg["warmup_rate"]
    avg_latency_threshold_ms = cfg["avg_latency_threshold_ms"]

    # Inject optional JVM flags
    if args.sample_latency or args.sanity_check:
        cfg["gp_jvm_args"] += " -DPB_SAMPLE_LATENCY=true -DXDN_TIMING_HEADERS=true"
    if args.fixed_accumulation:
        cfg["gp_jvm_args"] += " -DPB_CAPTURE_ACCUMULATION_MAX_US=100"

    # ── Banner ────────────────────────────────────────────────────────
    print("=" * 60)
    print(f"{app_name.upper()} Primary-Backup Throughput Investigation")
    print(f"  Mode     : fresh cluster per rate (isolated)")
    print(f"  Service  : {service}")
    print(f"  Rates    : {rates}")
    print(f"  Duration : {load_duration}s per rate "
          f"(warmup: {warmup_duration}s at {warmup_rate} rps, settle: {args.settle}s)")
    print(f"  Results  -> {results_dir}")
    print("=" * 60)

    # ── Sanity check (optional) ───────────────────────────────────────
    if args.sanity_check:
        from coordination_size_sanity_check import SanityChecker

        print("\n[Sanity Check] Setting up cluster for I/O measurement ...")
        if not args.skip_setup:
            sc_primary = setup_fresh_cluster(
                app_name, cfg, results_dir, screen_session, screen_log
            )
        else:
            sc_primary = detect_primary_via_docker(AR_HOSTS)
            if not sc_primary:
                sc_primary = AR_HOSTS[0]

        # Warmup a few requests to stabilize
        wl = get_workload_args(
            app_name, sc_primary, HTTP_PROXY_PORT, service, str(results_dir)
        )
        print("   Warming up with 10 requests ...")
        import urllib.request as _ul
        for _ in range(10):
            try:
                headers_dict = {}
                for i in range(0, len(wl["headers"]), 2):
                    key, val = wl["headers"][i + 1].split(": ", 1)
                    headers_dict[key] = val
                data = wl.get("body", "placeholder")
                if isinstance(data, str):
                    data = data.encode()
                _req = _ul.Request(
                    wl["url"],
                    data=data,
                    headers=headers_dict,
                )
                _ul.urlopen(_req, timeout=30).read()
            except Exception:
                pass
        time.sleep(3)

        # Determine sanity check parameters based on app
        wl_cfg = cfg["workload"]
        sc_backups = [h for h in AR_HOSTS if h != sc_primary]

        # Find the DB container pattern for the app
        db_patterns = {
            "wordpress": ("mysql", "mysqld"),
            "tpcc":      ("postgres", "postgres"),
            "bookcatalog": (None, None),
            "hotelres":  ("mongo", "mongod"),
        }
        db_container_pattern, db_process_name = db_patterns.get(
            app_name, (None, None)
        )

        # Determine the payloads file for the sanity checker
        if wl_cfg.get("uses_payloads_file"):
            if app_name == "wordpress":
                sc_payload_file = str(results_dir / "xmlrpc_payloads.txt")
            elif app_name == "tpcc":
                sc_payload_file = str(results_dir / "neworder_payloads.txt")
            else:
                sc_payload_file = str(results_dir / "payloads.txt")
        else:
            # Create a temporary payloads file with the default body
            sc_payload_file = str(results_dir / "sanity_payloads.txt")
            with open(sc_payload_file, "w") as fh:
                body = wl.get("body", "placeholder")
                fh.write(body + "\n")

        checker = SanityChecker(
            primary=sc_primary,
            backups=sc_backups,
            port=HTTP_PROXY_PORT,
            service_name=service,
            endpoint=wl_cfg["url_path"],
            db_container_pattern=db_container_pattern or "none",
            db_process_name=db_process_name or "none",
            n_samples=10,
        )
        checker.run(payload_file=sc_payload_file, results_dir=str(results_dir))

        # Tear down the sanity check cluster -- real measurements start fresh
        print("\n[Sanity Check] Tearing down sanity check cluster ...")
        clear_xdn_cluster()

        # Restore JVM args without sampling flags for real measurements
        if not args.sample_latency:
            cfg["gp_jvm_args"] = cfg["gp_jvm_args"].replace(
                " -DPB_SAMPLE_LATENCY=true -DXDN_TIMING_HEADERS=true", ""
            )

    # ── Save effective config ─────────────────────────────────────────
    save_effective_config(cfg["gp_config"], cfg["gp_jvm_args"], str(results_dir))

    # ── Rate sweep ────────────────────────────────────────────────────
    results = []
    screen_logs = []

    for i, rate in enumerate(rates):
        print(f"\n{'='*60}")
        print(f"  Rate point {i+1}/{len(rates)}: {rate} req/s")
        print(f"{'='*60}")

        # ── Per-rate fresh cluster setup ──────────────────────────────
        if not args.skip_setup:
            primary = setup_fresh_cluster(
                app_name, cfg, results_dir, screen_session, screen_log,
                enable_apache_timing=args.apache_timing,
            )
        else:
            print("   [setup] Skipped (--skip-setup)")
            primary = detect_primary_via_docker(AR_HOSTS)
            if not primary:
                print("ERROR: Could not detect primary via docker.")
                sys.exit(1)
            print(f"   Primary: {primary}")

        # ── Warmup ────────────────────────────────────────────────────
        print(f"\n   -> warmup at {warmup_rate} req/s for {warmup_duration}s ...")
        warmup_result = warmup_load(
            app_name, primary, warmup_rate, warmup_duration, results_dir
        )
        if warmup_result["throughput_rps"] < 0.1 * warmup_rate:
            print(
                f"   SKIP: warmup tput={warmup_result['throughput_rps']:.2f} rps "
                f"< 10% of {warmup_rate} rps -- system not ready"
            )
            log_path = copy_screen_log(screen_log, results_dir, rate)
            screen_logs.append(log_path)
            continue
        settle_sec = args.settle
        print(f"   Settling {settle_sec}s after warmup ...")
        time.sleep(settle_sec)

        # ── JFR start (before measurement) ────────────────────────────
        jfr_file = None
        if args.jfr:
            jvm_pid = subprocess.run(
                ["ssh", primary,
                 "ps -ef | grep 'java.*ReconfigurableNode' | grep -v grep "
                 "| grep -v sudo | awk '{print $2}' | head -1"],
                capture_output=True, text=True,
            ).stdout.strip()
            if jvm_pid:
                jfr_remote = f"/tmp/xdn_jfr_rate{rate}.jfr"
                jfr_duration = load_duration + 5
                result = subprocess.run(
                    ["ssh", primary,
                     f"sudo jcmd {jvm_pid} JFR.start name=xdn_rate{rate} "
                     f"duration={jfr_duration}s filename={jfr_remote} settings=profile"],
                    capture_output=True, text=True,
                )
                if "Started recording" in result.stdout:
                    jfr_file = jfr_remote
                    print(f"   JFR started (pid={jvm_pid}, duration={jfr_duration}s)")
                else:
                    print(f"   WARNING: JFR start failed: {result.stdout.strip()}")

        # ── Measurement ───────────────────────────────────────────────
        print(f"   -> rate={rate} req/s (measuring {load_duration}s) ...")
        m = run_load_point(app_name, primary, rate, load_duration, results_dir)
        row = {"rate_rps": rate, **m}
        results.append(row)
        print_metrics(m)

        # ── JFR collect ───────────────────────────────────────────────
        if jfr_file:
            time.sleep(3)  # wait for JFR to flush
            jfr_dest = results_dir / f"jfr_rate{rate}.jfr"
            os.system(f"scp {primary}:{jfr_file} {jfr_dest} 2>/dev/null")
            if jfr_dest.exists():
                print(f"   JFR saved -> {jfr_dest}")
                # Print hot methods summary
                result = subprocess.run(
                    ["ssh", primary, f"jfr view hot-methods {jfr_file}"],
                    capture_output=True, text=True,
                )
                if result.stdout.strip():
                    print("   JFR Hot Methods:")
                    for line in result.stdout.strip().splitlines()[:15]:
                        print(f"     {line}")
                # Print GC summary
                result = subprocess.run(
                    ["ssh", primary, f"jfr view gc {jfr_file}"],
                    capture_output=True, text=True,
                )
                if result.stdout.strip():
                    print("   JFR GC:")
                    for line in result.stdout.strip().splitlines()[:10]:
                        print(f"     {line}")
                os.system(f"ssh {primary} rm -f {jfr_file} 2>/dev/null")
            else:
                print("   WARNING: JFR download failed")

        # ── Per-rate log collection ───────────────────────────────────
        log_path = copy_screen_log(screen_log, results_dir, rate)
        screen_logs.append(log_path)

        # ── Early stop on saturation ──────────────────────────────────
        lat_sat, tput_sat = check_saturation(m, avg_latency_threshold_ms)
        if lat_sat and tput_sat:
            print(
                f"   Avg latency {m['avg_ms']:.0f}ms > {avg_latency_threshold_ms}ms "
                f"AND throughput {m['throughput_rps']:.2f} rps < 75% of offered "
                f"{m['actual_achieved_rps']:.2f} rps -- stopping sweep early."
            )
            break

    # ── Concatenate per-rate screen logs for timing analysis ──────────
    combined_log = results_dir / "screen.log"
    with open(combined_log, "w") as out:
        for log_path in screen_logs:
            if log_path.exists():
                out.write(log_path.read_text())
    print(f"\n   Combined screen log -> {combined_log}")

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
        cmd = ["python3", script, "--results-dir", str(results_dir)]
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

    # ── Create/update symlink ─────────────────────────────────────────
    symlink = RESULTS_BASE / f"load_pb_{app_name}_reflex"
    if symlink.is_symlink():
        symlink.unlink()
    elif symlink.is_dir():
        backup = RESULTS_BASE / f"load_pb_{app_name}_reflex_old_{timestamp}"
        symlink.rename(backup)
        print(f"   Renamed old directory -> {backup.name}")
    symlink.symlink_to(results_dir.name)
    print(f"   Symlink: {symlink} -> {results_dir.name}")

    print(f"\n[Done] Results in {results_dir}/")
