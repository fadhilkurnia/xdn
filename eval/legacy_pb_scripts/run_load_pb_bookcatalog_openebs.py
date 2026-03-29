import warnings as _w; _w.warn("DEPRECATED: Use run_load_pb_openebs_app.py --app bookcatalog instead.", DeprecationWarning, stacklevel=2)
"""
run_load_pb_bookcatalog_openebs.py — OpenEBS Mayastor 3-replica bookcatalog-nd benchmark.

Sets up a K8s cluster on CloudLab with OpenEBS Mayastor block-level NVMe-oF
replication (3 replicas), deploys bookcatalog-nd with SQLite WAL, and runs
the same POST /api/books workload used by the XDN PB benchmark.

Run from the eval/ directory:
    python3 run_load_pb_bookcatalog_openebs.py [--skip-k8s] [--skip-disk-prep]

Outputs go to eval/results/bookcatalog_openebs/:
    rate1.txt  rate100.txt  ...  — go-client output per rate point
"""

import argparse
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from run_load_pb_bookcatalog_common import (
    BOOKCATALOG_IMAGE,
    check_app_ready,
    seed_books,
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

_TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
RESULTS_DIR = Path(__file__).resolve().parent / "results" / f"load_pb_bookcatalog_openebs_{_TIMESTAMP}"
SCRIPT_DIR = Path(__file__).resolve().parent

# ── K8s manifests ────────────────────────────────────────────────────────────

BOOKCATALOG_PVC_YAML = """
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: bookcatalog-pvc
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 8Gi
  storageClassName: openebs-3-replica
"""

BOOKCATALOG_DEPLOY_YAML = f"""
apiVersion: apps/v1
kind: Deployment
metadata:
  name: bookcatalog-nd
spec:
  replicas: 1
  selector:
    matchLabels:
      app: bookcatalog-nd
  template:
    metadata:
      labels:
        app: bookcatalog-nd
    spec:
      containers:
        - name: bookcatalog-nd
          image: {BOOKCATALOG_IMAGE}
          env:
            - name: ENABLE_WAL
              value: "true"
            - name: DISABLE_WAL_CHECKPOINT
              value: "true"
          ports:
            - containerPort: 80
          volumeMounts:
            - name: app-data
              mountPath: /app/data
      volumes:
        - name: app-data
          persistentVolumeClaim:
            claimName: bookcatalog-pvc
"""

BOOKCATALOG_SVC_YAML = f"""
apiVersion: v1
kind: Service
metadata:
  name: bookcatalog-svc
spec:
  type: NodePort
  selector:
    app: bookcatalog-nd
  ports:
    - port: 80
      targetPort: 80
      nodePort: {APP_NODEPORT}
"""

# ── Load test helpers ────────────────────────────────────────────────────────


def _run_load(url, rate, output_file, duration_sec):
    output_file.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "go", "run", str(GO_LATENCY_CLIENT),
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


# ── Phase 4: Deploy bookcatalog on K8s ───────────────────────────────────────


def deploy_bookcatalog_k8s():
    """Apply K8s manifests for bookcatalog-nd and wait for rollout."""
    print("   Applying K8s manifests ...")
    for name, yaml in [
        ("bookcatalog-pvc",    BOOKCATALOG_PVC_YAML),
        ("bookcatalog-deploy", BOOKCATALOG_DEPLOY_YAML),
        ("bookcatalog-svc",    BOOKCATALOG_SVC_YAML),
    ]:
        print(f"     Applying {name} ...")
        _kubectl(["apply", "-f", "-"], manifest=yaml)

    print("   Waiting for bookcatalog-nd Deployment rollout ...")
    _kubectl(["rollout", "status", "deployment/bookcatalog-nd", "--timeout=300s"])

    print("   Waiting for bookcatalog-nd to be HTTP-ready ...")
    ok = wait_for_http(WORKER_NODES[0], APP_NODEPORT, "/api/books", timeout_sec=300)
    if not ok:
        print("ERROR: bookcatalog-nd not ready after 300s — check: kubectl get pods")
        sys.exit(1)


# ── Main ──────────────────────────────────────────────────────────────────────


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--skip-k8s", action="store_true",
                   help="Skip K8s cluster setup")
    p.add_argument("--skip-disk-prep", action="store_true",
                   help="Skip LVM disk prep")
    p.add_argument("--skip-openebs", action="store_true",
                   help="Skip OpenEBS install")
    # --skip-deploy and --skip-seed removed: fresh deploy + seed per rate point
    p.add_argument("--rates", type=str, default=None,
                   help="Comma-separated list of offered rates (req/s) to override default")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()

    print("=" * 60)
    print("BookCatalog-ND OpenEBS 3-Replica Benchmark")
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

    app_host = WORKER_NODES[0]
    app_url = f"http://{app_host}:{APP_NODEPORT}/api/books"

    # Determine rate list
    if args.rates:
        rates = [int(r.strip()) for r in args.rates.split(",")]
    else:
        rates = BOTTLENECK_RATES

    # Phase 7 — Load sweep (fresh deployment per rate point)
    print(f"\n[Phase 7] Load sweep at {rates} req/s (fresh deploy per rate) ...")
    print(f"   Duration : {LOAD_DURATION_SEC}s per rate (warmup {WARMUP_DURATION_SEC}s)")
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    results = []
    for i, rate in enumerate(rates):
        print(f"\n{'=' * 60}")
        print(f"  Rate point {i + 1}/{len(rates)}: {rate} req/s (fresh deploy)")
        print(f"{'=' * 60}")

        try:
            # Tear down previous deployment
            _kubectl(["delete", "deployment", "bookcatalog-nd", "--ignore-not-found=true"])
            _kubectl(["delete", "svc", "bookcatalog-svc", "--ignore-not-found=true"])
            _kubectl(["delete", "pvc", "bookcatalog-pvc", "--ignore-not-found=true"])
            time.sleep(10)

            # Fresh deploy
            deploy_bookcatalog_k8s()

            # Seed books
            if not wait_for_app_ready(app_host, APP_NODEPORT, timeout_sec=120):
                print("ERROR: App not ready, skipping rate.")
                continue
            seeded = False
            for attempt in range(1, 4):
                created = seed_books(app_host, APP_NODEPORT, count=200)
                if created > 0:
                    seeded = True
                    break
                print(f"   Attempt {attempt} failed, retrying in 10s ...")
                time.sleep(10)
            if not seeded:
                print("ERROR: Failed to seed, skipping rate.")
                continue
            if not check_app_ready(app_host, APP_NODEPORT):
                print("ERROR: App not ready after seeding, skipping rate.")
                continue

            # Warmup
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

            # Measurement
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

            # Early stop on saturation (both conditions must be true)
            avg_exceeded = m["avg_ms"] > AVG_LATENCY_THRESHOLD_MS
            tput_dropped = m["actual_achieved_rps"] > 0 and m["throughput_rps"] < 0.75 * m["actual_achieved_rps"]
            if avg_exceeded and tput_dropped:
                print(
                    f"   System saturated: avg {m['avg_ms']:.0f}ms > {AVG_LATENCY_THRESHOLD_MS}ms AND "
                    f"throughput {m['throughput_rps']:.2f} < 75% of {m['actual_achieved_rps']:.2f}; stopping."
                )
                break
            if avg_exceeded:
                print(f"   Avg latency {m['avg_ms']:.0f}ms > {AVG_LATENCY_THRESHOLD_MS}ms (throughput OK); continuing.")
            if tput_dropped:
                print(f"   Throughput {m['throughput_rps']:.2f} < 75% of {m['actual_achieved_rps']:.2f} (latency OK); continuing.")
        finally:
            # Clean up this rate's deployment
            _kubectl(["delete", "deployment", "bookcatalog-nd", "--ignore-not-found=true"], check=False)
            _kubectl(["delete", "svc", "bookcatalog-svc", "--ignore-not-found=true"], check=False)
            _kubectl(["delete", "pvc", "bookcatalog-pvc", "--ignore-not-found=true"], check=False)

    # Phase 8 — Summary
    print("\n[Phase 8] Summary")
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

    # Phase 9 — Plot
    print("\n[Phase 9] Generating plots ...")
    r = subprocess.run(
        ["python3", "plot_load_latency.py", "--results-dir", str(RESULTS_DIR)],
        capture_output=True, text=True, cwd=str(SCRIPT_DIR),
    )
    print(r.stdout)
    if r.returncode != 0:
        print(f"   WARNING: plot_load_latency.py failed:\n{r.stderr}")

    print(f"\n[Done] Results in {RESULTS_DIR}/")
