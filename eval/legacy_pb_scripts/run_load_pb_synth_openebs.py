import warnings as _w; _w.warn("DEPRECATED: Use run_load_pb_openebs_app.py --app synth instead.", DeprecationWarning, stacklevel=2)
"""
run_load_pb_synth_openebs.py — OpenEBS Mayastor 3-replica synth-workload benchmark.

Sets up a K8s cluster with OpenEBS Mayastor block-level NVMe-oF replication
(3 replicas), deploys synth-workload with SQLite WAL, and runs the same
4 experiments as the PB benchmark.

Run from the eval/ directory:
    python3 run_load_pb_synth_openebs.py [--skip-k8s] [--experiments 1,2,3,4]

Outputs go to eval/results/load_pb_synth_openebs/{exp1_vary_txns,...}/
"""

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

from run_load_pb_synth_common import (
    SYNTH_IMAGE,
    make_workload_payload,
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
RESULTS_BASE = Path(__file__).resolve().parent / "results" / "load_pb_synth_openebs"
SCRIPT_DIR = Path(__file__).resolve().parent

# Experiment parameters (same as PB benchmark)
EXP1_TXNS_VALUES = [1, 2, 5, 10, 20]
EXP2_OPS_VALUES = [1, 5, 10, 20, 50]
EXP3_WRITESIZE_VALUES = [100, 1000, 10000, 100000]
EXP_FIXED_RATE = 50
EXP_DURATION_SEC = 30
EXP4_TXNS = 5
EXP4_OPS = 5
EXP4_WRITESIZE = 1000
EXP5_AUTOCOMMIT_OPS = [1, 2, 5, 10, 20]

# ── K8s manifests ────────────────────────────────────────────────────────────

SYNTH_PVC_YAML = """
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: synth-pvc
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 5Gi
  storageClassName: openebs-3-replica
"""

SYNTH_DEPLOY_YAML = f"""
apiVersion: apps/v1
kind: Deployment
metadata:
  name: synth-workload
spec:
  replicas: 1
  selector:
    matchLabels:
      app: synth-workload
  template:
    metadata:
      labels:
        app: synth-workload
    spec:
      containers:
        - name: synth-workload
          image: {SYNTH_IMAGE}
          imagePullPolicy: Never
          env:
            - name: ENABLE_WAL
              value: "true"
            - name: DB_TYPE
              value: "sqlite"
          ports:
            - containerPort: 80
          volumeMounts:
            - name: app-data
              mountPath: /data
      volumes:
        - name: app-data
          persistentVolumeClaim:
            claimName: synth-pvc
"""

SYNTH_SVC_YAML = f"""
apiVersion: v1
kind: Service
metadata:
  name: synth-svc
spec:
  type: NodePort
  selector:
    app: synth-workload
  ports:
    - port: 80
      targetPort: 80
      nodePort: {APP_NODEPORT}
"""


# ── Load test helpers ────────────────────────────────────────────────────────


def _run_load(url, rate, payload, output_file, duration_sec):
    output_file.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "go", "run", str(GO_LATENCY_CLIENT),
        "-H", "Content-Type: application/json",
        "-X", "POST",
        url,
        payload,
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


def run_experiment_point(url, rate, payload, output_file, duration_sec):
    m = _run_load(url, rate, payload, output_file, duration_sec)
    print(
        f"     tput={m['throughput_rps']:.2f} rps  "
        f"avg={m['avg_ms']:.1f}ms  "
        f"p50={m['p50_ms']:.1f}ms  "
        f"p95={m['p95_ms']:.1f}ms"
    )
    return m


def warmup_point(url, rate, payload, output_file):
    m = _run_load(url, rate, payload, output_file, WARMUP_DURATION_SEC)
    print(f"     [warmup] tput={m['throughput_rps']:.2f} rps  avg={m['avg_ms']:.1f}ms")
    return m


# ── Deploy ───────────────────────────────────────────────────────────────────


def deploy_synth_k8s():
    """Apply K8s manifests for synth-workload and wait for rollout."""
    print("   Applying K8s manifests ...")
    for name, yaml in [
        ("synth-pvc",    SYNTH_PVC_YAML),
        ("synth-deploy", SYNTH_DEPLOY_YAML),
        ("synth-svc",    SYNTH_SVC_YAML),
    ]:
        print(f"     Applying {name} ...")
        _kubectl(["apply", "-f", "-"], manifest=yaml)

    print("   Waiting for synth-workload Deployment rollout ...")
    _kubectl(["rollout", "status", "deployment/synth-workload", "--timeout=300s"])

    print("   Waiting for synth-workload to be HTTP-ready ...")
    ok = wait_for_http(WORKER_NODES[0], APP_NODEPORT, "/health", timeout_sec=300)
    if not ok:
        print("ERROR: synth-workload not ready after 300s — check: kubectl get pods")
        sys.exit(1)


# ── Experiments (same structure as PB) ──────────────────────────────────────


def run_exp1(app_url):
    print("\n" + "=" * 60)
    print("Experiment 1: Vary txns/request (ops=1, write_size=100)")
    print("=" * 60)
    exp_dir = RESULTS_BASE / "exp1_vary_txns"
    results = []
    for txns in EXP1_TXNS_VALUES:
        payload = make_workload_payload(txns=txns, ops=1, write_size=100)
        print(f"\n   -> txns={txns} at {EXP_FIXED_RATE} rps ...")
        warmup_point(app_url, EXP_FIXED_RATE, payload, exp_dir / f"warmup_txns{txns}.txt")
        time.sleep(5)
        m = run_experiment_point(app_url, EXP_FIXED_RATE, payload,
                                 exp_dir / f"txns{txns}.txt", EXP_DURATION_SEC)
        results.append({"txns": txns, **m})
        time.sleep(INTER_RATE_PAUSE_SEC)
    return results


def run_exp2(app_url):
    print("\n" + "=" * 60)
    print("Experiment 2: Vary ops/txn (txns=1, write_size=100)")
    print("=" * 60)
    exp_dir = RESULTS_BASE / "exp2_vary_ops"
    results = []
    for ops in EXP2_OPS_VALUES:
        payload = make_workload_payload(txns=1, ops=ops, write_size=100)
        print(f"\n   -> ops={ops} at {EXP_FIXED_RATE} rps ...")
        warmup_point(app_url, EXP_FIXED_RATE, payload, exp_dir / f"warmup_ops{ops}.txt")
        time.sleep(5)
        m = run_experiment_point(app_url, EXP_FIXED_RATE, payload,
                                 exp_dir / f"ops{ops}.txt", EXP_DURATION_SEC)
        results.append({"ops": ops, **m})
        time.sleep(INTER_RATE_PAUSE_SEC)
    return results


def run_exp3(app_url):
    print("\n" + "=" * 60)
    print("Experiment 3: Vary write_size (txns=1, ops=1)")
    print("=" * 60)
    exp_dir = RESULTS_BASE / "exp3_vary_writesize"
    results = []
    for ws in EXP3_WRITESIZE_VALUES:
        payload = make_workload_payload(txns=1, ops=1, write_size=ws)
        print(f"\n   -> write_size={ws} at {EXP_FIXED_RATE} rps ...")
        warmup_point(app_url, EXP_FIXED_RATE, payload, exp_dir / f"warmup_write_size{ws}.txt")
        time.sleep(5)
        m = run_experiment_point(app_url, EXP_FIXED_RATE, payload,
                                 exp_dir / f"write_size{ws}.txt", EXP_DURATION_SEC)
        results.append({"write_size": ws, **m})
        time.sleep(INTER_RATE_PAUSE_SEC)
    return results


def run_exp5(app_url):
    print("\n" + "=" * 60)
    print("Experiment 5: Vary auto-committed ops/request (autocommit=true)")
    print("=" * 60)
    exp_dir = RESULTS_BASE / "exp5_autocommit_ops"
    results = []
    for ops in EXP5_AUTOCOMMIT_OPS:
        payload = make_workload_payload(txns=1, ops=ops, write_size=100, autocommit=True)
        print(f"\n   -> ops={ops} (autocommit) at {EXP_FIXED_RATE} rps ...")
        warmup_point(app_url, EXP_FIXED_RATE, payload, exp_dir / f"warmup_ops{ops}.txt")
        time.sleep(5)
        m = run_experiment_point(app_url, EXP_FIXED_RATE, payload,
                                 exp_dir / f"ops{ops}.txt", EXP_DURATION_SEC)
        results.append({"ops": ops, **m})
        time.sleep(3)
    print("\n   Summary (exp5: autocommit ops):")
    print(f"   {'ops':>6}  {'tput':>8}  {'avg':>8}  {'p50':>8}  {'p95':>8}")
    print("   " + "-" * 48)
    for row in results:
        print(
            f"   {row['ops']:>6}  "
            f"{row['throughput_rps']:>7.02f}  "
            f"{row['avg_ms']:>7.1f}ms  "
            f"{row['p50_ms']:>7.1f}ms  "
            f"{row['p95_ms']:>7.1f}ms"
        )
    return results


def run_exp4(app_url, rates):
    print("\n" + "=" * 60)
    print(f"Experiment 4: Rate sweep (txns={EXP4_TXNS}, ops={EXP4_OPS}, write_size={EXP4_WRITESIZE})")
    print("=" * 60)
    exp_dir = RESULTS_BASE / "exp4_combined"
    payload = make_workload_payload(txns=EXP4_TXNS, ops=EXP4_OPS, write_size=EXP4_WRITESIZE)
    results = []

    for i, rate in enumerate(rates):
        if i > 0 and INTER_RATE_PAUSE_SEC > 0:
            print(f"   Pausing {INTER_RATE_PAUSE_SEC}s ...")
            time.sleep(INTER_RATE_PAUSE_SEC)
        print(f"\n   -> rate={rate} req/s (warmup) ...")
        wm = warmup_point(app_url, rate, payload, exp_dir / f"warmup_rate{rate}.txt")
        if wm["throughput_rps"] < 0.1 * rate:
            print(f"   SKIP: warmup tput too low — saturated")
            continue
        time.sleep(5)
        print(f"   -> rate={rate} req/s (measuring {LOAD_DURATION_SEC}s) ...")
        m = run_experiment_point(app_url, rate, payload,
                                 exp_dir / f"rate{rate}.txt", LOAD_DURATION_SEC)
        results.append({"rate_rps": rate, **m})
        if m["avg_ms"] > AVG_LATENCY_THRESHOLD_MS:
            print(f"   Saturated (avg latency > {AVG_LATENCY_THRESHOLD_MS}ms), stopping.")
            break
        if m["actual_achieved_rps"] > 0 and m["throughput_rps"] < 0.8 * m["actual_achieved_rps"]:
            print(f"   Saturated (throughput < 80% offered), stopping.")
            break
    return results


# ── Main ──────────────────────────────────────────────────────────────────────


def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--skip-k8s", action="store_true", help="Skip K8s cluster setup")
    p.add_argument("--skip-disk-prep", action="store_true", help="Skip LVM disk prep")
    p.add_argument("--skip-openebs", action="store_true", help="Skip OpenEBS install")
    p.add_argument("--skip-deploy", action="store_true", help="Skip K8s deployment")
    p.add_argument("--experiments", type=str, default="1,2,3,4",
                   help="Comma-separated experiment numbers (default: 1,2,3,4)")
    p.add_argument("--rates", type=str, default=None,
                   help="Override rate list for exp4")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    experiments = [int(e.strip()) for e in args.experiments.split(",")]

    print("=" * 60)
    print("Synth-Workload OpenEBS 3-Replica Benchmark")
    print("=" * 60)

    if not args.skip_k8s:
        print("\n[Phase 1] Setting up K8s cluster ...")
        setup_k8s_cluster()
    else:
        print("\n[Phase 1] Skipping K8s setup (--skip-k8s)")

    if not args.skip_disk_prep:
        print("\n[Phase 2] Prepping NVMe disks ...")
        prep_disks()
    else:
        print("\n[Phase 2] Skipping disk prep (--skip-disk-prep)")

    if not args.skip_openebs:
        print("\n[Phase 3] Installing OpenEBS Mayastor ...")
        setup_openebs()
    else:
        print("\n[Phase 3] Skipping OpenEBS (--skip-openebs)")

    if not args.skip_deploy:
        print("\n[Phase 4] Deploying synth-workload on K8s ...")
        deploy_synth_k8s()
    else:
        print("\n[Phase 4] Skipping deployment (--skip-deploy)")

    app_host = WORKER_NODES[0]
    app_url = f"http://{app_host}:{APP_NODEPORT}/workload"

    print(f"\n   App URL: {app_url}")

    # Run experiments
    if 1 in experiments:
        run_exp1(app_url)
    if 2 in experiments:
        run_exp2(app_url)
    if 3 in experiments:
        run_exp3(app_url)
    if 4 in experiments:
        rates = [int(r.strip()) for r in args.rates.split(",")] if args.rates else BOTTLENECK_RATES
        run_exp4(app_url, rates)
    if 5 in experiments:
        run_exp5(app_url)

    # Generate plots
    print("\n[Phase 9] Generating plots ...")
    r = subprocess.run(
        ["python3", "plot_synth_comparison.py",
         "--results-dir", str(RESULTS_BASE),
         "--variant", "openebs"],
        capture_output=True, text=True, cwd=str(SCRIPT_DIR),
    )
    print(r.stdout)
    if r.returncode != 0:
        print(f"   WARNING: plot_synth_comparison.py failed:\n{r.stderr}")

    print(f"\n[Done] Results in {RESULTS_BASE}/")
