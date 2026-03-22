"""
run_load_pb_hotelres_replmongo.py — Replicated MongoDB hotel-reservation benchmark.

Sets up a MongoDB replica set (w:majority) across three CloudLab nodes:
  primary on 10.10.1.1, secondaries on 10.10.1.2 and 10.10.1.3.
Hotel-reservation app runs on 10.10.1.1 connected to the replica set.
Runs the same POST /reservation workload as the XDN PB benchmark.

Run from the eval/ directory:
    python3 run_load_pb_hotelres_replmongo.py [--skip-teardown] [--skip-mongo]

Outputs go to eval/results/load_pb_hotelres_replmongo/:
    rate1.txt  rate100.txt  ...  — go-client output per rate point
"""

import argparse
import concurrent.futures
import os
import subprocess
import sys
import time
from pathlib import Path

from run_load_pb_hotelres_common import (
    APP_PORT,
    FRONTEND_IMAGE,
    MONGO_IMAGE,
    MONGO_PORT,
    MONGO_ROOT_PASS,
    MONGO_ROOT_USER,
    SERVICE_NAME,
    check_app_ready,
    generate_dummy_data,
    generate_reservation_urls,
    wait_for_app_ready,
    wait_for_port,
)
from run_load_pb_wordpress_reflex import (
    AVG_LATENCY_THRESHOLD_MS,
    BOTTLENECK_RATES,
    GO_LATENCY_CLIENT,
    INTER_RATE_PAUSE_SEC,
    LOAD_DURATION_SEC,
    WARMUP_DURATION_SEC,
    _parse_go_output,
)

# ── Constants ─────────────────────────────────────────────────────────────────

PRIMARY_HOST  = "10.10.1.1"
REPLICA_HOSTS = ["10.10.1.2", "10.10.1.3"]
APP_EXPOSED_PORT = 2300  # match XDN proxy port for consistency

RESULTS_DIR = Path(__file__).resolve().parent / "results" / "load_pb_hotelres_replmongo"
SCRIPT_DIR  = Path(__file__).resolve().parent
URLS_FILE   = RESULTS_DIR / "reservation_urls.txt"

# MongoDB replica set name
RS_NAME = "rs0"

# Shared keyFile path inside containers (required for auth + replica set)
KEYFILE_HOST_PATH = "/tmp/mongo-keyfile"
KEYFILE_CONTAINER_PATH = "/etc/mongo-keyfile"

# ── Shell helpers ─────────────────────────────────────────────────────────────


def _ssh(host, cmd, check=True, capture=False):
    return subprocess.run(
        ["ssh", "-o", "StrictHostKeyChecking=no", host, cmd],
        check=check,
        capture_output=capture,
        text=True,
    )


def _mongosh(host, container, js, check=True):
    """Run JavaScript via mongosh inside a container."""
    return _ssh(
        host,
        f"docker exec {container} mongosh "
        f"--username {MONGO_ROOT_USER} --password {MONGO_ROOT_PASS} "
        f"--authenticationDatabase admin --eval '{js}'",
        capture=True,
        check=check,
    )


# ── Load test helpers ────────────────────────────────────────────────────────


def _run_load(url, rate, output_file, duration_sec):
    output_file.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "go", "run", str(GO_LATENCY_CLIENT),
        "-X", "POST",
        "-urls-file", str(URLS_FILE),
        url,
        "",  # no JSON body
        str(duration_sec),
        str(rate),
    ]
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


def run_load_point(url, rate):
    return _run_load(url, rate, RESULTS_DIR / f"rate{rate}.txt", LOAD_DURATION_SEC)


def warmup_rate_point(url, rate):
    m = _run_load(url, rate, RESULTS_DIR / f"warmup_rate{rate}.txt", WARMUP_DURATION_SEC)
    print(f"     [warmup] tput={m['throughput_rps']:.2f} rps  avg={m['avg_ms']:.1f}ms")
    return m


# ── Phase helpers ─────────────────────────────────────────────────────────────


def teardown_all():
    """Stop and remove MongoDB + hotel-reservation containers on all three nodes."""
    all_hosts = [PRIMARY_HOST] + REPLICA_HOSTS
    print(f"   Stopping containers on {all_hosts} ...")

    def teardown_node(host):
        _ssh(host,
             "docker ps -q | xargs -r docker stop 2>/dev/null || true; "
             "docker ps -aq | xargs -r docker rm -f 2>/dev/null || true",
             check=False)
        if host == PRIMARY_HOST:
            _ssh(host,
                 f"PIDS=$(sudo lsof -ti :{APP_EXPOSED_PORT} 2>/dev/null); "
                 f"[ -n \"$PIDS\" ] && sudo kill -9 $PIDS 2>/dev/null || true",
                 check=False)
        _ssh(host,
             "docker stop mongo-primary mongo-secondary hotel-reservation 2>/dev/null || true; "
             "docker rm -f mongo-primary mongo-secondary hotel-reservation 2>/dev/null || true",
             check=False)
        print(f"   Teardown complete on {host}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        futs = [pool.submit(teardown_node, h) for h in all_hosts]
        for f in futs:
            f.result()


def distribute_keyfile():
    """Generate a shared MongoDB replica set keyFile and distribute to all nodes.

    MongoDB requires a keyFile for internal authentication between replica set
    members when authorization is enabled (MONGO_INITDB_ROOT_USERNAME is set).
    The mongo container runs as uid 999 (mongodb), so the keyFile must be
    readable by that user with mode 400.
    """
    print("   Generating and distributing MongoDB keyFile ...")
    all_hosts = [PRIMARY_HOST] + REPLICA_HOSTS
    local_tmp = "/tmp/mongo-keyfile-local"
    # Generate keyFile locally and distribute to all nodes
    subprocess.run(
        f"openssl rand -base64 756 > {local_tmp}",
        shell=True, check=True,
    )
    for host in all_hosts:
        # Remove any stale keyFile (may be owned by uid 999 with 400 perms)
        _ssh(host, f"sudo rm -f {KEYFILE_HOST_PATH}", check=False)
        subprocess.run(
            ["scp", "-o", "StrictHostKeyChecking=no",
             local_tmp, f"{host}:{KEYFILE_HOST_PATH}"],
            check=True,
        )
        _ssh(host,
             f"sudo chown 999:999 {KEYFILE_HOST_PATH} && "
             f"sudo chmod 400 {KEYFILE_HOST_PATH}")
    os.remove(local_tmp)
    print("   KeyFile distributed to all nodes.")


def start_mongo_primary():
    """Start MongoDB primary with replica set on 10.10.1.1."""
    print(f"   Starting MongoDB primary on {PRIMARY_HOST} ...")
    cmd = (
        "docker run -d --name mongo-primary "
        f"-e MONGO_INITDB_ROOT_USERNAME={MONGO_ROOT_USER} "
        f"-e MONGO_INITDB_ROOT_PASSWORD={MONGO_ROOT_PASS} "
        f"-v {KEYFILE_HOST_PATH}:{KEYFILE_CONTAINER_PATH}:ro "
        f"-p {MONGO_PORT}:{MONGO_PORT} "
        f"{MONGO_IMAGE} "
        f"mongod --replSet {RS_NAME} --bind_ip_all "
        f"--keyFile {KEYFILE_CONTAINER_PATH}"
    )
    _ssh(PRIMARY_HOST, cmd)
    print(f"   Waiting for MongoDB primary port {PRIMARY_HOST}:{MONGO_PORT} ...")
    if not wait_for_port(PRIMARY_HOST, MONGO_PORT, timeout_sec=120):
        print("ERROR: MongoDB primary did not start in time.")
        sys.exit(1)
    time.sleep(10)


def start_mongo_secondaries():
    """Start MongoDB secondaries on .2 and .3 in parallel."""
    def start_secondary(host):
        print(f"   Starting MongoDB secondary on {host} ...")
        cmd = (
            "docker run -d --name mongo-secondary "
            f"-e MONGO_INITDB_ROOT_USERNAME={MONGO_ROOT_USER} "
            f"-e MONGO_INITDB_ROOT_PASSWORD={MONGO_ROOT_PASS} "
            f"-v {KEYFILE_HOST_PATH}:{KEYFILE_CONTAINER_PATH}:ro "
            f"-p {MONGO_PORT}:{MONGO_PORT} "
            f"{MONGO_IMAGE} "
            f"mongod --replSet {RS_NAME} --bind_ip_all "
            f"--keyFile {KEYFILE_CONTAINER_PATH}"
        )
        _ssh(host, cmd)
        if not wait_for_port(host, MONGO_PORT, timeout_sec=120):
            raise RuntimeError(f"MongoDB secondary on {host} did not start in time.")
        time.sleep(5)
        print(f"   MongoDB secondary started on {host}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        futs = [pool.submit(start_secondary, h) for h in REPLICA_HOSTS]
        for f in futs:
            f.result()


def init_replica_set():
    """Initialize the MongoDB replica set on the primary."""
    print("   Initializing replica set ...")
    members = [
        f'{{_id: 0, host: "{PRIMARY_HOST}:{MONGO_PORT}"}}',
        f'{{_id: 1, host: "{REPLICA_HOSTS[0]}:{MONGO_PORT}"}}',
        f'{{_id: 2, host: "{REPLICA_HOSTS[1]}:{MONGO_PORT}"}}',
    ]
    members_str = ", ".join(members)
    js = f'rs.initiate({{_id: "{RS_NAME}", members: [{members_str}]}})'
    result = _mongosh(PRIMARY_HOST, "mongo-primary", js, check=False)
    print(f"   rs.initiate result: {result.stdout[:300]}")
    if result.returncode != 0:
        print(f"   WARNING: rs.initiate stderr: {result.stderr[:200]}")


def wait_for_replica_set_ready(timeout_sec=120):
    """Poll rs.status() until primary is elected and secondaries are syncing."""
    print("   Waiting for replica set to be ready ...")
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        result = _mongosh(PRIMARY_HOST, "mongo-primary", "rs.status().ok", check=False)
        stdout = result.stdout.strip()
        if "1" in stdout:
            # Check that primary has been elected
            result2 = _mongosh(
                PRIMARY_HOST, "mongo-primary",
                "rs.status().members.filter(m => m.stateStr === \"PRIMARY\").length",
                check=False,
            )
            if "1" in result2.stdout.strip():
                print("   Replica set is ready (primary elected)")
                # Print full status
                status = _mongosh(
                    PRIMARY_HOST, "mongo-primary",
                    "rs.status().members.map(m => m.name + \": \" + m.stateStr).join(\", \")",
                    check=False,
                )
                print(f"   Members: {status.stdout.strip()}")
                return True
        print(f"   ... waiting for replica set election ...")
        time.sleep(5)
    print(f"   WARNING: replica set may not be fully ready after {timeout_sec}s")
    return False


def start_hotel_reservation_app():
    """Start hotel-reservation app on PRIMARY_HOST connected to the replica set."""
    print(f"   Starting hotel-reservation app on {PRIMARY_HOST}:{APP_EXPOSED_PORT} ...")
    _ssh(PRIMARY_HOST, "docker rm -f hotel-reservation 2>/dev/null || true", check=False)
    mongo_uri = (
        f"mongodb://{MONGO_ROOT_USER}:{MONGO_ROOT_PASS}@"
        f"{PRIMARY_HOST}:{MONGO_PORT},"
        f"{REPLICA_HOSTS[0]}:{MONGO_PORT},"
        f"{REPLICA_HOSTS[1]}:{MONGO_PORT}"
        f"/?replicaSet={RS_NAME}&w=majority&authSource=admin"
    )
    cmd = (
        "docker run -d --name hotel-reservation "
        f"-e MONGO_URI='{mongo_uri}' "
        f"-p {APP_EXPOSED_PORT}:{APP_PORT} "
        f"{FRONTEND_IMAGE}"
    )
    _ssh(PRIMARY_HOST, cmd)
    print(f"   Waiting for app port {PRIMARY_HOST}:{APP_EXPOSED_PORT} ...")
    if not wait_for_port(PRIMARY_HOST, APP_EXPOSED_PORT, timeout_sec=120):
        print("ERROR: Hotel-reservation app did not start in time.")
        sys.exit(1)
    time.sleep(5)


# ── Main ──────────────────────────────────────────────────────────────────────


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--skip-teardown", action="store_true",
                   help="Skip container teardown")
    p.add_argument("--skip-mongo", action="store_true",
                   help="Skip MongoDB setup (assume already running)")
    p.add_argument("--skip-app", action="store_true",
                   help="Skip app container start (assume already running)")
    p.add_argument("--skip-seed", action="store_true",
                   help="Skip data generation (assume already seeded)")
    p.add_argument("--rates", type=str, default=None,
                   help="Comma-separated list of offered rates (req/s) to override default")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()

    print("=" * 60)
    print("Hotel-Reservation Replicated MongoDB Benchmark")
    print("=" * 60)

    # Phase 1 — Teardown
    if args.skip_teardown:
        print("\n[Phase 1] Skipping teardown (--skip-teardown)")
    else:
        print("\n[Phase 1] Tearing down existing containers ...")
        teardown_all()

    # Phase 2 — MongoDB primary
    if args.skip_mongo:
        print("\n[Phase 2-5] Skipping MongoDB setup (--skip-mongo)")
    else:
        print("\n[Phase 2] Distributing keyFile and starting MongoDB primary ...")
        distribute_keyfile()
        start_mongo_primary()

        # Phase 3 — MongoDB secondaries
        print("\n[Phase 3] Starting MongoDB secondaries ...")
        start_mongo_secondaries()

        # Phase 4 — Initialize replica set
        print("\n[Phase 4] Initializing replica set ...")
        init_replica_set()

        # Phase 5 — Wait for replica set
        print("\n[Phase 5] Waiting for replica set to be ready ...")
        if not wait_for_replica_set_ready():
            print("ERROR: Replica set not ready.")
            sys.exit(1)

    # Phase 6 — Start app
    if args.skip_app:
        print("\n[Phase 6] Skipping app start (--skip-app)")
    else:
        print("\n[Phase 6] Starting hotel-reservation app ...")
        start_hotel_reservation_app()

    app_url = f"http://{PRIMARY_HOST}:{APP_EXPOSED_PORT}/reservation"

    if not args.skip_seed:
        # Phase 7 — Generate dummy data
        print("\n[Phase 7] Generating dummy data ...")
        if not wait_for_app_ready(PRIMARY_HOST, APP_EXPOSED_PORT, timeout_sec=120):
            print("ERROR: App not ready.")
            sys.exit(1)
        for attempt in range(1, 4):
            if generate_dummy_data(PRIMARY_HOST, APP_EXPOSED_PORT):
                break
            print(f"   Attempt {attempt} failed, retrying in 10s ...")
            time.sleep(10)
        else:
            print("ERROR: Failed to generate dummy data.")
            sys.exit(1)

        # Phase 8 — Verify app
        print("\n[Phase 8] Verifying app readiness ...")
        if not check_app_ready(PRIMARY_HOST, APP_EXPOSED_PORT):
            print("ERROR: App not ready after data generation.")
            sys.exit(1)

        # Phase 9 — Generate reservation URLs
        print(f"\n[Phase 9] Generating reservation URLs -> {URLS_FILE} ...")
        RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        generate_reservation_urls(
            PRIMARY_HOST, APP_EXPOSED_PORT, str(URLS_FILE), count=500,
        )
    else:
        print("\n[Phase 7-9] Skipping data generation (--skip-seed)")
        print(f"   Assuming {URLS_FILE} already exists.")

    # Determine rate list
    if args.rates:
        rates = [int(r.strip()) for r in args.rates.split(",")]
    else:
        rates = BOTTLENECK_RATES

    # Phase 10 — Load sweep
    print(f"\n[Phase 10] Load sweep at {rates} req/s ...")
    print(f"   URL      : {app_url}")
    print(f"   Duration : {LOAD_DURATION_SEC}s per rate (warmup {WARMUP_DURATION_SEC}s)")
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    results = []
    for i, rate in enumerate(rates):
        if i > 0 and INTER_RATE_PAUSE_SEC > 0:
            print(f"   Pausing {INTER_RATE_PAUSE_SEC}s between rate points ...")
            time.sleep(INTER_RATE_PAUSE_SEC)

        print(f"\n   -> rate={rate} req/s (warmup {WARMUP_DURATION_SEC}s) ...")
        wm = warmup_rate_point(app_url, rate)
        if wm["throughput_rps"] < 0.1 * rate:
            print(
                f"   SKIP: warmup tput={wm['throughput_rps']:.2f} rps "
                f"< 10% of {rate} rps — system saturated"
            )
            continue
        print("   Settling 5s after warmup ...")
        time.sleep(5)

        # Log replica set status before each measurement
        rs_status = _mongosh(
            PRIMARY_HOST, "mongo-primary",
            "rs.status().members.map(m => m.name + \": \" + m.stateStr).join(\", \")",
            check=False,
        )
        if rs_status.returncode == 0:
            print(f"   RS status: {rs_status.stdout.strip()[-80:]}")

        print(f"   -> rate={rate} req/s (measuring {LOAD_DURATION_SEC}s) ...")
        m = run_load_point(app_url, rate)
        row = {"rate_rps": rate, **m}
        results.append(row)
        print(
            f"     tput={m['throughput_rps']:.2f} rps  "
            f"avg={m['avg_ms']:.1f}ms  "
            f"p50={m['p50_ms']:.1f}ms  "
            f"p95={m['p95_ms']:.1f}ms"
        )
        if m["avg_ms"] > AVG_LATENCY_THRESHOLD_MS:
            print(
                f"   Avg latency {m['avg_ms']:.0f}ms > {AVG_LATENCY_THRESHOLD_MS}ms — stopping."
            )
            break
        if m["actual_achieved_rps"] > 0 and m["throughput_rps"] < 0.8 * m["actual_achieved_rps"]:
            print(
                f"   Throughput {m['throughput_rps']:.2f} rps < 80% of offered "
                f"{m['actual_achieved_rps']:.2f} rps — system saturated, stopping sweep early."
            )
            break

    # Phase 11 — Summary
    print("\n[Phase 11] Summary")
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

    # Phase 12 — Plot
    print("\n[Phase 12] Generating plots ...")
    r = subprocess.run(
        ["python3", "plot_load_latency.py", "--results-dir", str(RESULTS_DIR)],
        capture_output=True, text=True, cwd=str(SCRIPT_DIR),
    )
    print(r.stdout)
    if r.returncode != 0:
        print(f"   WARNING: plot_load_latency.py failed:\n{r.stderr}")

    print(f"\n[Done] Results in {RESULTS_DIR}/")
