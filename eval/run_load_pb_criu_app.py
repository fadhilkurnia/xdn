"""
run_load_pb_criu_app.py — Consolidated CRIU checkpoint/restore baseline benchmark.

Runs a stateful app on AR0 with BaselineCriuReplica.java as a batching HTTP
proxy. CRIU checkpoints the designated container after each batch and rsyncs
to replica nodes.

Supported apps:
  - wordpress: Two containers (MySQL + Apache). CRIU checkpoints MySQL only.
  - bookcatalog: One container (bookcatalog-nd). CRIU checkpoints the entire container.

Run from the eval/ directory:
    python3 run_load_pb_criu_app.py --app wordpress [--rates 1,100,200] [--duration 60]
    python3 run_load_pb_criu_app.py --app bookcatalog [--rates 1,100,200]

Outputs go to eval/results/load_pb_<app>_criu_<timestamp>/:
    rate<N>.txt  — go-client output per rate point
"""

import argparse
import concurrent.futures
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from pb_common import (
    AR_HOSTS,
    AVG_LATENCY_THRESHOLD_MS,
    BOTTLENECK_RATES,
    GO_LATENCY_CLIENT,
    LOAD_DURATION_SEC,
    WARMUP_DURATION_SEC,
    parse_go_output,
    wait_for_port,
)
from pb_app_configs import (
    install_wordpress,
    enable_rest_auth,
    disable_wp_cron,
    check_rest_api,
    _wp_create_post,
    _xmlrpc_editpost_payload,
    ADMIN_PASSWORD,
    ADMIN_USER,
    ADMIN_EMAIL,
    SITE_TITLE,
    APP_CONFIGS,
)

# ── Constants ─────────────────────────────────────────────────────────────────

PRIMARY_HOST = AR_HOSTS[0]
REPLICA_HOSTS = AR_HOSTS[1:]
CRIU_LISTEN_PORT = 2300

SCRIPT_DIR = Path(__file__).resolve().parent
JARS_DIR = Path(__file__).resolve().parent.parent / "jars"
CRIU_CLASSPATH = f"{JARS_DIR}/gigapaxos-1.0.10.jar:{JARS_DIR}/nio-1.2.1.jar"
CRIU_MAIN_CLASS = "edu.umass.cs.xdn.eval.BaselineCriuReplica"

# ── Per-app CRIU configuration ───────────────────────────────────────────────

CRIU_APP_CONFIGS = {
    "wordpress": {
        "containers": [
            {
                "name": "wordpress-criu-mysql",
                "image": "mysql:8.4.0",
                "port": 3306,
                "checkpoint": True,
                "env": {
                    "MYSQL_ROOT_PASSWORD": "supersecret",
                    "MYSQL_DATABASE": "wordpress",
                },
            },
            {
                "name": "wordpress-criu",
                "image": "wordpress:6.5.4-apache",
                "port": 8080,
                "checkpoint": False,
                "env": {
                    "WORDPRESS_DB_HOST": f"{PRIMARY_HOST}:3306",
                    "WORDPRESS_DB_USER": "root",
                    "WORDPRESS_DB_PASSWORD": "supersecret",
                    "WORDPRESS_DB_NAME": "wordpress",
                },
            },
        ],
        "criu_forward_port": 8080,
    },
    "bookcatalog": {
        "containers": [
            {
                "name": "bookcatalog-nd",
                "image": "fadhilkurnia/xdn-bookcatalog-nd",
                "port": 8080,
                "checkpoint": True,
                "env": {"ENABLE_WAL": "true"},
            },
        ],
        "criu_forward_port": 8080,
    },
}

VALID_APPS = list(CRIU_APP_CONFIGS.keys())


# ── Shell helpers ─────────────────────────────────────────────────────────────


def _ssh(host, cmd, check=True, capture=False):
    return subprocess.run(
        ["ssh", "-o", "StrictHostKeyChecking=no", host, cmd],
        check=check,
        capture_output=capture,
        text=True,
    )


# ── Teardown ──────────────────────────────────────────────────────────────────


def teardown_all():
    """Stop and remove containers + CRIU proxy on all nodes."""
    all_hosts = [PRIMARY_HOST] + REPLICA_HOSTS
    print(f"   Stopping containers on {all_hosts} ...")

    def teardown_node(host):
        _ssh(host,
             "docker ps -q | xargs -r docker stop 2>/dev/null || true; "
             "docker ps -aq | xargs -r docker rm -f 2>/dev/null || true; "
             "docker network prune --force > /dev/null 2>&1 || true",
             check=False)
        if host == PRIMARY_HOST:
            _ssh(host,
                 f"PIDS=$(sudo lsof -ti :{CRIU_LISTEN_PORT} 2>/dev/null); "
                 f"[ -n \"$PIDS\" ] && sudo kill -9 $PIDS 2>/dev/null || true",
                 check=False)
            _ssh(host,
                 "sudo pkill -f BaselineCriuReplica 2>/dev/null || true; "
                 "sudo rm -rf /dev/shm/chk /dev/shm/criu-inc 2>/dev/null || true",
                 check=False)
        print(f"   Teardown complete on {host}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        futs = [pool.submit(teardown_node, h) for h in all_hosts]
        for f in futs:
            f.result()


# ── Container setup ───────────────────────────────────────────────────────────


def _start_single_container(container_cfg):
    """Start a single container on PRIMARY_HOST with CRIU-compatible flags."""
    name = container_cfg["name"]
    image = container_cfg["image"]
    port = container_cfg["port"]
    env = container_cfg.get("env", {})

    print(f"   Starting container {name} on {PRIMARY_HOST}:{port} ...")
    _ssh(PRIMARY_HOST, f"docker rm -f {name} 2>/dev/null || true", check=False)

    env_flags = " ".join(f"-e {k}={v}" for k, v in env.items())
    # Map container port: for WordPress Apache, internal port is 80; for MySQL, 3306;
    # for bookcatalog, internal port is 80.
    if "mysql" in image:
        internal_port = 3306
    elif "wordpress" in image:
        internal_port = 80
    else:
        internal_port = 80

    cmd = (
        f"docker run -d --name {name} "
        f"--security-opt seccomp=unconfined "
        f"{env_flags} "
        f"-p {port}:{internal_port} "
        f"{image}"
    )
    _ssh(PRIMARY_HOST, cmd)


def _wait_mysql_ready(container_name, password, timeout_sec=120):
    """Wait for MySQL to be fully ready (accepts connections)."""
    print(f"   Waiting for MySQL ({container_name}) to accept connections ...")
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        result = _ssh(PRIMARY_HOST,
                      f"docker exec {container_name} mysqladmin ping -p{password} --silent",
                      check=False, capture=True)
        if result.returncode == 0:
            print("   MySQL ready.")
            return True
        time.sleep(2)
    print("   ERROR: MySQL not accepting connections.")
    return False


def start_containers(app_name):
    """Start all containers for the given app on PRIMARY_HOST.

    WordPress: starts MySQL first, waits for it to be healthy, then starts Apache.
    BookCatalog: starts single container.
    """
    criu_cfg = CRIU_APP_CONFIGS[app_name]
    containers = criu_cfg["containers"]

    if app_name == "wordpress":
        # MySQL first
        mysql_cfg = containers[0]  # wordpress-criu-mysql
        wp_cfg = containers[1]     # wordpress-criu

        _start_single_container(mysql_cfg)
        if not wait_for_port(PRIMARY_HOST, mysql_cfg["port"], timeout_sec=120):
            print("ERROR: MySQL did not start in time.")
            sys.exit(1)
        if not _wait_mysql_ready(mysql_cfg["name"],
                                 mysql_cfg["env"]["MYSQL_ROOT_PASSWORD"]):
            sys.exit(1)

        # Then WordPress Apache
        _start_single_container(wp_cfg)
        if not wait_for_port(PRIMARY_HOST, wp_cfg["port"], timeout_sec=120):
            print("ERROR: WordPress did not start in time.")
            sys.exit(1)
        time.sleep(5)
        print("   WordPress containers ready.")
    else:
        # Single container app (bookcatalog)
        for c in containers:
            _start_single_container(c)
            if not wait_for_port(PRIMARY_HOST, c["port"], timeout_sec=120):
                print(f"ERROR: Container {c['name']} did not start in time.")
                sys.exit(1)
            time.sleep(5)
            print(f"   Container {c['name']} ready.")


# ── CRIU proxy ────────────────────────────────────────────────────────────────


def start_criu_proxy(app_name):
    """Start BaselineCriuReplica on PRIMARY_HOST.

    Determines the checkpoint container and forward port from CRIU_APP_CONFIGS.
    """
    criu_cfg = CRIU_APP_CONFIGS[app_name]
    forward_port = criu_cfg["criu_forward_port"]

    # Find the checkpoint container
    checkpoint_containers = [c for c in criu_cfg["containers"] if c["checkpoint"]]
    checkpoint_container = checkpoint_containers[0]["name"]

    # Find the "app" container name (the one receiving forwarded requests)
    # For WordPress: Apache container. For bookcatalog: same as checkpoint container.
    if app_name == "wordpress":
        app_container = next(c["name"] for c in criu_cfg["containers"] if not c["checkpoint"])
    else:
        app_container = checkpoint_container

    print(f"   Starting CRIU proxy (incremental=true, checkpoint={checkpoint_container}) ...")
    replicas = " ".join(REPLICA_HOSTS)
    java_cmd = (
        f"nohup sudo java "
        f"-Djdk.httpclient.allowRestrictedHeaders=content-length,connection,host "
        f"-Dcriu.incremental=true "
        f"-Dcriu.checkpoint.container={checkpoint_container} "
        f"-Dcriu.batch.size=8 "
        f"-Dcriu.batch.flush.ms=25 "
        f"-Dcriu.listen.port={CRIU_LISTEN_PORT} "
        f"-Dcriu.forward.port={forward_port} "
        f"-cp {CRIU_CLASSPATH} "
        f"{CRIU_MAIN_CLASS} {forward_port} {app_container} {replicas} "
        f"> /tmp/criu_proxy.log 2>&1 &"
    )
    _ssh(PRIMARY_HOST, java_cmd)
    print(f"   Waiting for CRIU proxy port {PRIMARY_HOST}:{CRIU_LISTEN_PORT} ...")
    if not wait_for_port(PRIMARY_HOST, CRIU_LISTEN_PORT, timeout_sec=60):
        print("ERROR: CRIU proxy did not start in time.")
        result = _ssh(PRIMARY_HOST, "tail -50 /tmp/criu_proxy.log", check=False, capture=True)
        print(f"   CRIU log:\n{result.stdout}")
        sys.exit(1)
    time.sleep(3)
    print("   CRIU proxy ready.")


# ── App setup and seeding ─────────────────────────────────────────────────────


def setup_app(app_name, results_dir):
    """App-specific setup. Returns payloads_file (Path) or None."""
    if app_name == "wordpress":
        return _setup_wordpress(results_dir)
    elif app_name == "bookcatalog":
        # No setup needed for bookcatalog
        print("   [setup] BookCatalog: no setup needed.")
        return None
    else:
        raise ValueError(f"Unknown app: {app_name}")


def _setup_wordpress(results_dir):
    """Install WordPress, enable REST auth, disable cron, seed posts, generate payloads."""
    seed_count = APP_CONFIGS["wordpress"].get("seed_count", 200)

    print("   Installing WordPress ...")
    ok = install_wordpress(PRIMARY_HOST, CRIU_APP_CONFIGS["wordpress"]["criu_forward_port"],
                           "wordpress_criu")
    if not ok:
        raise RuntimeError("WordPress installation failed")
    time.sleep(10)

    print("   Enabling REST auth ...")
    if not enable_rest_auth(PRIMARY_HOST):
        raise RuntimeError("REST API auth setup failed")

    print("   Disabling WP-Cron ...")
    disable_wp_cron(PRIMARY_HOST)

    print("   Verifying REST API works before seeding ...")
    if not check_rest_api(PRIMARY_HOST,
                          CRIU_APP_CONFIGS["wordpress"]["criu_forward_port"],
                          "wordpress_criu"):
        raise RuntimeError("REST API check failed before seeding")

    print(f"   Seeding {seed_count} posts ...")
    for i in range(1, seed_count + 1):
        pid = _wp_create_post(PRIMARY_HOST,
                              CRIU_APP_CONFIGS["wordpress"]["criu_forward_port"],
                              "wordpress_criu",
                              f"Seed Post {i}",
                              f"Content for seed post {i}.")
        if not pid:
            print(f"   WARNING: Seed post {i} failed")
        if i % 50 == 0:
            print(f"     Seeded {i}/{seed_count}")

    # Generate payloads file for wp.editPost
    payloads_file = results_dir / "payloads.txt"
    payloads_file.parent.mkdir(parents=True, exist_ok=True)
    with open(payloads_file, "w") as f:
        for post_id in range(1, seed_count + 1):
            f.write(_xmlrpc_editpost_payload(post_id) + "\n")
    print(f"   Generated {seed_count} XML-RPC payloads -> {payloads_file}")
    return payloads_file


def seed_bookcatalog():
    """Seed bookcatalog app via CRIU proxy."""
    import requests as req

    print("   Waiting for bookcatalog readiness ...")
    deadline = time.time() + 120
    while time.time() < deadline:
        try:
            r = req.get(f"http://{PRIMARY_HOST}:{CRIU_LISTEN_PORT}/api/books", timeout=5)
            if r.status_code == 200:
                break
        except Exception:
            pass
        time.sleep(2)
    else:
        print("   ERROR: bookcatalog not ready.")
        return False

    seed_count = APP_CONFIGS["bookcatalog"].get("seed_count", 200)
    print(f"   Seeding {seed_count} books ...")
    created = 0
    for i in range(1, seed_count + 1):
        try:
            r = req.post(
                f"http://{PRIMARY_HOST}:{CRIU_LISTEN_PORT}/api/books",
                headers={"Content-Type": "application/json"},
                data='{"title":"seed book","author":"seed author"}',
                timeout=10,
            )
            if r.status_code in (200, 201):
                created += 1
        except Exception as e:
            print(f"   ERROR seeding book {i}: {e}")
        if i % 50 == 0:
            print(f"     Seeded {i}/{seed_count}")
    print(f"   Seeded {created}/{seed_count} books.")
    return created > 0


# ── Load helpers ──────────────────────────────────────────────────────────────


def _build_go_cmd(app_name, url, duration_sec, rate, payloads_file=None):
    """Build the go load client command for the given app."""
    if app_name == "wordpress":
        cmd = [
            "go", "run", str(GO_LATENCY_CLIENT),
            "-H", "Content-Type: text/xml",
            "-X", "POST",
        ]
        if payloads_file:
            cmd += ["-payloads-file", str(payloads_file)]
        cmd += [url, "placeholder", str(duration_sec), str(rate)]
    elif app_name == "bookcatalog":
        cmd = [
            "go", "run", str(GO_LATENCY_CLIENT),
            "-H", "Content-Type: application/json",
            "-X", "POST",
            url,
            '{"title":"bench book","author":"bench author"}',
            str(duration_sec),
            str(rate),
        ]
    else:
        raise ValueError(f"Unknown app: {app_name}")
    return cmd


def _get_app_url(app_name):
    """Get the load test URL for the given app."""
    if app_name == "wordpress":
        return f"http://{PRIMARY_HOST}:{CRIU_LISTEN_PORT}/xmlrpc.php"
    elif app_name == "bookcatalog":
        return f"http://{PRIMARY_HOST}:{CRIU_LISTEN_PORT}/api/books"
    else:
        raise ValueError(f"Unknown app: {app_name}")


def run_load_point(app_name, rate, results_dir, payloads_file=None):
    """Run a single load measurement at the given rate."""
    results_dir.mkdir(parents=True, exist_ok=True)
    output_file = results_dir / f"rate{rate}.txt"
    url = _get_app_url(app_name)
    cmd = _build_go_cmd(app_name, url, LOAD_DURATION_SEC, rate, payloads_file)
    env = os.environ.copy()
    env["GO111MODULE"] = "off"
    env["GOWORK"] = "off"
    print(f"   Output -> {output_file}")
    with open(output_file, "w") as fh:
        result = subprocess.run(cmd, stdout=fh, stderr=subprocess.STDOUT,
                                env=env, text=True)
    if result.returncode != 0:
        print(f"   WARNING: go client exited {result.returncode}")
    return parse_go_output(output_file)


def warmup_rate_point(app_name, rate, results_dir, payloads_file=None):
    """Run a warmup load at the given rate."""
    results_dir.mkdir(parents=True, exist_ok=True)
    output_file = results_dir / f"warmup_rate{rate}.txt"
    url = _get_app_url(app_name)
    cmd = _build_go_cmd(app_name, url, WARMUP_DURATION_SEC, rate, payloads_file)
    env = os.environ.copy()
    env["GO111MODULE"] = "off"
    env["GOWORK"] = "off"
    with open(output_file, "w") as fh:
        subprocess.run(cmd, stdout=fh, stderr=subprocess.STDOUT, env=env, text=True)
    m = parse_go_output(output_file)
    print(f"     [warmup] tput={m['throughput_rps']:.2f} rps  avg={m['avg_ms']:.1f}ms")
    return m


# ── Main ──────────────────────────────────────────────────────────────────────


def parse_args():
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--app", type=str, required=True, choices=VALID_APPS,
                   help=f"Application to benchmark ({', '.join(VALID_APPS)})")
    p.add_argument("--rates", type=str, default=None,
                   help="Comma-separated offered rates (default: BOTTLENECK_RATES)")
    p.add_argument("--duration", type=int, default=LOAD_DURATION_SEC,
                   help=f"Load duration in seconds (default: {LOAD_DURATION_SEC})")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    app_name = args.app

    # Override global duration if specified
    if args.duration != LOAD_DURATION_SEC:
        LOAD_DURATION_SEC = args.duration

    if args.rates:
        rates = [int(r.strip()) for r in args.rates.split(",")]
    else:
        rates = BOTTLENECK_RATES

    criu_cfg = CRIU_APP_CONFIGS[app_name]
    checkpoint_container = next(c["name"] for c in criu_cfg["containers"] if c["checkpoint"])

    _TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
    RESULTS_DIR = SCRIPT_DIR / "results" / f"load_pb_{app_name}_criu_{_TIMESTAMP}"

    print("=" * 60)
    print(f"{app_name.upper()} CRIU Baseline Benchmark")
    print("=" * 60)
    print(f"  Primary          : {PRIMARY_HOST}")
    print(f"  Replicas         : {REPLICA_HOSTS}")
    for c in criu_cfg["containers"]:
        chk = " (checkpoint)" if c["checkpoint"] else ""
        print(f"  Container        : {c['name']} [{c['image']}] port={c['port']}{chk}")
    print(f"  CRIU checkpoint  : {checkpoint_container} (incremental)")
    print(f"  CRIU forward     : port {criu_cfg['criu_forward_port']}")
    print(f"  Rates            : {rates} req/s")
    print(f"  Duration         : {args.duration}s per rate (warmup {WARMUP_DURATION_SEC}s)")
    print(f"  Fresh setup      : per rate point")
    print(f"  Results          : {RESULTS_DIR}")
    print("=" * 60)

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    results = []

    for i, rate in enumerate(rates):
        print(f"\n{'=' * 60}")
        print(f"  Rate point {i+1}/{len(rates)}: {rate} req/s (fresh containers)")
        print(f"{'=' * 60}")

        payloads_file = None
        try:
            # Step 1 — Teardown
            print("\n  [Step 1] Tearing down previous state ...")
            teardown_all()

            # Step 2 — Start containers
            print(f"\n  [Step 2] Starting {app_name} containers ...")
            start_containers(app_name)

            # Step 3 — Start CRIU proxy
            print("\n  [Step 3] Starting CRIU proxy ...")
            start_criu_proxy(app_name)

            # Step 4 — Setup + seed
            print(f"\n  [Step 4] Setting up and seeding {app_name} ...")
            if app_name == "wordpress":
                payloads_file = setup_app(app_name, RESULTS_DIR)
            elif app_name == "bookcatalog":
                setup_app(app_name, RESULTS_DIR)
                for attempt in range(1, 4):
                    ok = seed_bookcatalog()
                    if ok:
                        break
                    print(f"  Attempt {attempt} failed, retrying in 10s ...")
                    time.sleep(10)
                else:
                    print("  ERROR: Failed to seed books after 3 attempts, skipping rate.")
                    continue

            # Step 5 — Warmup
            print(f"\n  [Step 5] Warming up at {rate} req/s for {WARMUP_DURATION_SEC}s ...")
            wm = warmup_rate_point(app_name, rate, RESULTS_DIR, payloads_file)
            if wm["throughput_rps"] < 0.1 * rate:
                print(
                    f"  SKIP: warmup tput={wm['throughput_rps']:.2f} rps "
                    f"< 10% of {rate} rps — system saturated"
                )
                continue
            print("  Settling 5s after warmup ...")
            time.sleep(5)

            # Step 6 — Measure
            print(f"\n  [Step 6] Measuring at {rate} req/s for {args.duration}s ...")
            m = run_load_point(app_name, rate, RESULTS_DIR, payloads_file)
            row = {"rate_rps": rate, **m}
            results.append(row)
            print(
                f"    tput={m['throughput_rps']:.2f} rps  "
                f"avg={m['avg_ms']:.1f}ms  "
                f"p50={m['p50_ms']:.1f}ms  "
                f"p95={m['p95_ms']:.1f}ms"
            )

            # Check saturation (both conditions)
            avg_exceeded = m["avg_ms"] > AVG_LATENCY_THRESHOLD_MS
            tput_dropped = (
                m.get("actual_achieved_rps", 0) > 0
                and m["throughput_rps"] < 0.75 * m.get("actual_achieved_rps", rate)
            )
            if avg_exceeded and tput_dropped:
                print(
                    f"  System saturated: avg {m['avg_ms']:.0f}ms > {AVG_LATENCY_THRESHOLD_MS}ms AND "
                    f"throughput {m['throughput_rps']:.2f} < 75% offered; stopping."
                )
                break
            if avg_exceeded:
                print(f"  Avg latency {m['avg_ms']:.0f}ms > {AVG_LATENCY_THRESHOLD_MS}ms; continuing.")

        except Exception as e:
            print(f"  ERROR: Rate {rate} failed: {e}")
            import traceback
            traceback.print_exc()
        finally:
            print(f"\n  [Step 7] Cleaning up rate {rate} ...")
            teardown_all()

    # Summary
    print(f"\n{'=' * 60}")
    print("Summary")
    print(f"{'=' * 60}")
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

    # Plot
    print("\nGenerating plots ...")
    r = subprocess.run(
        ["python3", "plot_load_latency.py", "--results-dir", str(RESULTS_DIR)],
        capture_output=True, text=True, cwd=str(SCRIPT_DIR),
    )
    print(r.stdout)
    if r.returncode != 0:
        print(f"   WARNING: plot_load_latency.py failed:\n{r.stderr}")

    print(f"\n[Done] Results in {RESULTS_DIR}/")
