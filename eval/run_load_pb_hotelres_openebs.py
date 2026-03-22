"""
run_load_pb_hotelres_openebs.py — OpenEBS Mayastor 3-replica hotel-reservation benchmark.

Sets up a K8s cluster on CloudLab with OpenEBS Mayastor block-level NVMe-oF
replication (3 replicas), deploys MongoDB + hotel-reservation, and runs the
same POST /reservation workload used by the XDN PB benchmark.

Run from the eval/ directory:
    python3 run_load_pb_hotelres_openebs.py [--skip-k8s] [--skip-disk-prep]

Outputs go to eval/results/load_pb_hotelres_openebs/:
    rate1.txt  rate100.txt  ...  — go-client output per rate point
"""

import argparse
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
from run_load_pb_wordpress_openebs import (
    K8S_CONTROL_PLANE,
    WORKER_NODES,
    _kubectl,
    _ssh,
    detect_nvme_disk,
    prep_disks,
    setup_k8s_cluster,
    setup_openebs,
    wait_for_http,
)

# ── Constants ─────────────────────────────────────────────────────────────────

APP_NODEPORT = 30080

RESULTS_DIR = Path(__file__).resolve().parent / "results" / "load_pb_hotelres_openebs"
SCRIPT_DIR  = Path(__file__).resolve().parent
URLS_FILE   = RESULTS_DIR / "reservation_urls.txt"

# ── K8s manifests ────────────────────────────────────────────────────────────

MONGO_PVC_YAML = """
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: mongo-pvc
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 20Gi
  storageClassName: openebs-3-replica
"""

MONGO_STATEFULSET_YAML = f"""
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: mongo
spec:
  serviceName: mongo-svc
  replicas: 1
  selector:
    matchLabels:
      app: mongo
  template:
    metadata:
      labels:
        app: mongo
    spec:
      containers:
        - name: mongo
          image: {MONGO_IMAGE}
          env:
            - name: MONGO_INITDB_ROOT_USERNAME
              value: "{MONGO_ROOT_USER}"
            - name: MONGO_INITDB_ROOT_PASSWORD
              value: "{MONGO_ROOT_PASS}"
          ports:
            - containerPort: {MONGO_PORT}
          volumeMounts:
            - name: mongo-data
              mountPath: /data/db
      volumes:
        - name: mongo-data
          persistentVolumeClaim:
            claimName: mongo-pvc
"""

MONGO_SVC_YAML = f"""
apiVersion: v1
kind: Service
metadata:
  name: mongo-svc
spec:
  selector:
    app: mongo
  ports:
    - port: {MONGO_PORT}
      targetPort: {MONGO_PORT}
  clusterIP: None
"""

HOTELRES_DEPLOY_YAML = f"""
apiVersion: apps/v1
kind: Deployment
metadata:
  name: hotel-reservation
spec:
  replicas: 1
  selector:
    matchLabels:
      app: hotel-reservation
  template:
    metadata:
      labels:
        app: hotel-reservation
    spec:
      containers:
        - name: hotel-reservation
          image: {FRONTEND_IMAGE}
          env:
            - name: MONGO_URI
              value: "mongodb://{MONGO_ROOT_USER}:{MONGO_ROOT_PASS}@mongo-svc:{MONGO_PORT}/?authSource=admin"
          ports:
            - containerPort: {APP_PORT}
"""

HOTELRES_SVC_YAML = f"""
apiVersion: v1
kind: Service
metadata:
  name: hotel-reservation-svc
spec:
  type: NodePort
  selector:
    app: hotel-reservation
  ports:
    - port: {APP_PORT}
      targetPort: {APP_PORT}
      nodePort: {APP_NODEPORT}
"""

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


# ── Phase 4: Deploy MongoDB + hotel-reservation on K8s ───────────────────────


def deploy_hotelres_k8s():
    """Apply K8s manifests for MongoDB + hotel-reservation and wait for rollout."""
    print("   Applying K8s manifests ...")
    for name, yaml in [
        ("mongo-pvc",             MONGO_PVC_YAML),
        ("mongo-statefulset",     MONGO_STATEFULSET_YAML),
        ("mongo-svc",             MONGO_SVC_YAML),
        ("hotel-reservation-deploy", HOTELRES_DEPLOY_YAML),
        ("hotel-reservation-svc",    HOTELRES_SVC_YAML),
    ]:
        print(f"     Applying {name} ...")
        _kubectl(["apply", "-f", "-"], manifest=yaml)

    print("   Waiting for MongoDB StatefulSet rollout ...")
    _kubectl(["rollout", "status", "statefulset/mongo", "--timeout=300s"])

    print("   Waiting for hotel-reservation Deployment rollout ...")
    _kubectl(["rollout", "status", "deployment/hotel-reservation", "--timeout=300s"])

    print("   Waiting for hotel-reservation to be HTTP-ready ...")
    ok = wait_for_http(WORKER_NODES[0], APP_NODEPORT, "/", timeout_sec=300)
    if not ok:
        print("ERROR: hotel-reservation not ready after 300s — check: kubectl get pods")
        sys.exit(1)


# ── Main ──────────────────────────────────────────────────────────────────────


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--skip-k8s",       action="store_true",
                   help="Skip K8s cluster setup")
    p.add_argument("--skip-disk-prep", action="store_true",
                   help="Skip LVM disk prep")
    p.add_argument("--skip-openebs",   action="store_true",
                   help="Skip OpenEBS install")
    p.add_argument("--skip-deploy",    action="store_true",
                   help="Skip K8s deployment")
    p.add_argument("--skip-seed",      action="store_true",
                   help="Skip data generation")
    p.add_argument("--rates", type=str, default=None,
                   help="Comma-separated list of offered rates (req/s) to override default")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()

    print("=" * 60)
    print("Hotel-Reservation OpenEBS 3-Replica Benchmark")
    print("=" * 60)

    # Phase 1 — K8s cluster setup
    if args.skip_k8s:
        print("\n[Phase 1] Skipping K8s cluster setup (--skip-k8s)")
    else:
        print("\n[Phase 1] Setting up K8s cluster ...")
        setup_k8s_cluster()

    # Phase 2 — Disk prep
    if args.skip_disk_prep:
        print("\n[Phase 2] Skipping disk prep (--skip-disk-prep)")
    else:
        print("\n[Phase 2] Prepping NVMe disks for OpenEBS ...")
        prep_disks()

    # Phase 3 — OpenEBS Mayastor
    if args.skip_openebs:
        print("\n[Phase 3] Skipping OpenEBS install (--skip-openebs)")
    else:
        print("\n[Phase 3] Installing OpenEBS Mayastor ...")
        setup_openebs()

    # Phase 4 — Deploy MongoDB + hotel-reservation
    if args.skip_deploy:
        print("\n[Phase 4] Skipping deployment (--skip-deploy)")
    else:
        print("\n[Phase 4] Deploying MongoDB + hotel-reservation on K8s ...")
        deploy_hotelres_k8s()

    app_host = WORKER_NODES[0]
    app_url  = f"http://{app_host}:{APP_NODEPORT}/reservation"

    if not args.skip_seed:
        # Phase 5 — Generate dummy data
        print("\n[Phase 5] Generating dummy data ...")
        if not wait_for_app_ready(app_host, APP_NODEPORT, timeout_sec=120):
            print("ERROR: App not ready.")
            sys.exit(1)
        for attempt in range(1, 4):
            if generate_dummy_data(app_host, APP_NODEPORT):
                break
            print(f"   Attempt {attempt} failed, retrying in 10s ...")
            time.sleep(10)
        else:
            print("ERROR: Failed to generate dummy data.")
            sys.exit(1)

        # Phase 6 — Verify app
        print("\n[Phase 6] Verifying app readiness ...")
        if not check_app_ready(app_host, APP_NODEPORT):
            print("ERROR: App not ready after data generation.")
            sys.exit(1)

        # Phase 7 — Generate reservation URLs
        print(f"\n[Phase 7] Generating reservation URLs -> {URLS_FILE} ...")
        RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        generate_reservation_urls(
            app_host, APP_NODEPORT, str(URLS_FILE), count=500,
        )
    else:
        print("\n[Phase 5-7] Skipping data generation (--skip-seed)")
        print(f"   Assuming {URLS_FILE} already exists.")

    # Determine rate list
    if args.rates:
        rates = [int(r.strip()) for r in args.rates.split(",")]
    else:
        rates = BOTTLENECK_RATES

    # Phase 8 — Load sweep
    print(f"\n[Phase 8] Load sweep at {rates} req/s ...")
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

    # Phase 9 — Summary
    print("\n[Phase 9] Summary")
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

    # Phase 10 — Plot
    print("\n[Phase 10] Generating plots ...")
    r = subprocess.run(
        ["python3", "plot_load_latency.py", "--results-dir", str(RESULTS_DIR)],
        capture_output=True, text=True, cwd=str(SCRIPT_DIR),
    )
    print(r.stdout)
    if r.returncode != 0:
        print(f"   WARNING: plot_load_latency.py failed:\n{r.stderr}")

    print(f"\n[Done] Results in {RESULTS_DIR}/")
