"""
run_load_pb_openebs_app.py — Consolidated OpenEBS Mayastor 3-replica benchmark.

Supports all five app workloads (wordpress, bookcatalog, tpcc, hotelres, synth)
on a K8s cluster with OpenEBS Mayastor block-level NVMe-oF replication (3 replicas).

Generates K8s manifests from per-app config instead of inline YAML per script.

Usage:
    python3 run_load_pb_openebs_app.py --app wordpress [--skip-k8s] [--skip-disk-prep] \
        [--skip-openebs] [--skip-deploy] [--rates 100,200,500] [--duration 60]

Valid apps: wordpress, bookcatalog, tpcc, hotelres, synth
"""

import argparse
import concurrent.futures
import csv
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path

from init_openebs import run_node_prep
from pb_app_configs import (
    ADMIN_PASSWORD,
    ADMIN_USER,
    APP_CONFIGS,
    install_wordpress,
    make_synth_workload_payload,
)
from pb_common import (
    AVG_LATENCY_THRESHOLD_MS,
    BOTTLENECK_RATES,
    GO_LATENCY_CLIENT,
    INTER_RATE_PAUSE_SEC,
    LOAD_DURATION_SEC,
    WARMUP_DURATION_SEC,
)

# ── Constants ─────────────────────────────────────────────────────────────────

K8S_CONTROL_PLANE = "10.10.1.4"
WORKER_NODES = ["10.10.1.1", "10.10.1.2", "10.10.1.3"]
APP_NODEPORT = 30080

RESULTS_BASE = Path(__file__).resolve().parent / "results"
SCRIPT_DIR = Path(__file__).resolve().parent

# ── Per-app OpenEBS configuration ─────────────────────────────────────────────

OPENEBS_APP_CONFIGS = {
    "wordpress": {
        "pvc_size": "20Gi",
        "db": {
            "name": "mysql",
            "image": "mysql:8.4.0",
            "port": 3306,
            "env": {
                "MYSQL_ROOT_PASSWORD": "supersecret",
                "MYSQL_DATABASE": "wordpress",
            },
            "mount": "/var/lib/mysql",
            "healthcheck": "mysqladmin ping -h localhost -psupersecret",
        },
        "app": {
            "name": "wordpress",
            "image": "wordpress:6.5.4-apache",
            "port": 80,
            "env": {
                "WORDPRESS_DB_HOST": "db-svc:3306",
                "WORDPRESS_DB_USER": "root",
                "WORDPRESS_DB_PASSWORD": "supersecret",
                "WORDPRESS_DB_NAME": "wordpress",
                "WORDPRESS_CONFIG_EXTRA": (
                    "define('DISABLE_WP_CRON', true); "
                    "define('AUTOMATIC_UPDATER_DISABLED', true); "
                    "define('WP_AUTO_UPDATE_CORE', false);"
                ),
            },
            "mount": None,  # wordpress doesn't mount the PVC directly
        },
        "readiness_path": "/",
        "seed_count": 200,
    },
    "bookcatalog": {
        "pvc_size": "8Gi",
        "db": None,  # SQLite, no separate DB
        "app": {
            "name": "bookcatalog-nd",
            "image": "fadhilkurnia/xdn-bookcatalog-nd",
            "port": 80,
            "env": {
                "ENABLE_WAL": "true",
                "DISABLE_WAL_CHECKPOINT": "true",
            },
            "mount": "/app/data",
        },
        "readiness_path": "/api/books",
        "seed_count": 200,
    },
    "tpcc": {
        "pvc_size": "20Gi",
        "db": {
            "name": "postgres",
            "image": "postgres:17.4-bookworm",
            "port": 5432,
            "env": {
                "POSTGRES_PASSWORD": "benchpass",
                "POSTGRES_DB": "tpcc",
                "PGDATA": "/var/lib/postgresql/data/pgdata",
            },
            "mount": "/var/lib/postgresql/data",
            "healthcheck": None,
        },
        "app": {
            "name": "tpcc-app",
            "image": "fadhilkurnia/xdn-tpcc",
            "port": 8000,
            "env": {
                "DATABASE_URL": "postgresql://postgres:benchpass@db-svc:5432/tpcc",
                "GUNICORN_CMD_ARGS": "--workers 4 --timeout 120",
            },
            "mount": None,
        },
        "readiness_path": "/orders",
        "seed_count": 10,  # NUM_WAREHOUSES
    },
    "tpcc-java": {
        "pvc_size": "20Gi",
        "db": {
            "name": "postgres",
            "image": "postgres:17.4-bookworm",
            "port": 5432,
            "env": {
                "POSTGRES_PASSWORD": "benchpass",
                "POSTGRES_DB": "tpcc",
                "PGDATA": "/var/lib/postgresql/data/pgdata",
            },
            "mount": "/var/lib/postgresql/data",
            "healthcheck": None,
        },
        "app": {
            "name": "tpcc-java-app",
            "image": "fadhilkurnia/xdn-tpcc-java",
            "port": 80,
            "env": {
                "DATABASE_URL": "postgresql://postgres:benchpass@db-svc:5432/tpcc",
                "WAREHOUSES": "10",
            },
            "mount": None,
        },
        "readiness_path": "/orders",
        "seed_count": 10,  # NUM_WAREHOUSES
    },
    "hotelres": {
        "pvc_size": "20Gi",
        "db": {
            "name": "mongo",
            "image": "mongo:8.0.5-rc2-noble",
            "port": 27017,
            "env": {
                "MONGO_INITDB_ROOT_USERNAME": "root",
                "MONGO_INITDB_ROOT_PASSWORD": "testing123",
            },
            "mount": "/data/db",
            "healthcheck": None,
        },
        "app": {
            "name": "hotel-reservation",
            "image": "fadhilkurnia/xdn-hotel-reservation:latest",
            "port": 5000,
            "env": {
                "MONGO_URI": "mongodb://root:testing123@db-svc:27017/?authSource=admin",
            },
            "mount": None,
        },
        "readiness_path": "/",
        "seed_count": 500,  # reservation URLs
    },
    "synth": {
        "pvc_size": "8Gi",
        "db": None,
        "app": {
            "name": "synth-workload",
            "image": "fadhilkurnia/xdn-synth-workload",
            "port": 80,
            "env": {},
            "mount": "/app/data",
        },
        "readiness_path": "/health",
        "seed_count": 0,
    },
}

# ── SSH / kubectl helpers (from run_load_pb_wordpress_openebs.py) ─────────────


def _ssh(host, cmd, check=True, capture=False):
    return subprocess.run(
        ["ssh", "-o", "StrictHostKeyChecking=no", host, cmd],
        check=check,
        capture_output=capture,
        text=True,
    )


def _ssh_with_retry(host, cmd, check=True, capture=False, retries=4, retry_delay=15):
    """SSH with automatic retry for transient connection failures (exit code 255)."""
    for attempt in range(retries):
        try:
            return _ssh(host, cmd, check=check, capture=capture)
        except subprocess.CalledProcessError as e:
            if e.returncode == 255 and attempt < retries - 1:
                print(f"   [retry {attempt + 1}/{retries - 1}] SSH to {host} lost "
                      f"(exit 255), waiting {retry_delay}s ...")
                time.sleep(retry_delay)
            else:
                raise


def _kubectl(args, manifest=None, check=True, capture=False):
    """Run kubectl on the control-plane node via SSH."""
    kubectl_cmd = "kubectl " + " ".join(
        f"'{a}'" if " " in a else a for a in args
    )
    if manifest is not None:
        base_cmd = kubectl_cmd
        if base_cmd.endswith(" -f -"):
            base_cmd = base_cmd[: -len(" -f -")]
        full_cmd = ["ssh", "-o", "StrictHostKeyChecking=no",
                    K8S_CONTROL_PLANE, f"cat | {base_cmd} -f -"]
        kw = {"check": check, "input": manifest, "text": True}
        if capture:
            kw["capture_output"] = True
        return subprocess.run(full_cmd, **kw)
    else:
        full_cmd = ["ssh", "-o", "StrictHostKeyChecking=no",
                    K8S_CONTROL_PLANE, kubectl_cmd]
        kw = {"check": check}
        if capture:
            kw["capture_output"] = True
            kw["text"] = True
        return subprocess.run(full_cmd, **kw)


# ── Cluster helpers ───────────────────────────────────────────────────────────


def _get_kube_node_name(worker_host):
    """Return the K8s node name for a worker identified by its 10.10.1.x IP."""
    result = _ssh(
        K8S_CONTROL_PLANE,
        "kubectl get nodes -o json",
        capture=True,
    )
    data = json.loads(result.stdout)
    for item in data["items"]:
        node_name = item["metadata"]["name"]
        for addr in item["status"].get("addresses", []):
            if addr["address"] == worker_host:
                return node_name
    # Fallback: resolve by external IP
    ext_ip = _ssh(worker_host,
                  "hostname -I | tr ' ' '\\n' | grep -v '^10\\.' | grep -v '^172\\.' | head -1",
                  capture=True).stdout.strip()
    if ext_ip:
        for item in data["items"]:
            node_name = item["metadata"]["name"]
            for addr in item["status"].get("addresses", []):
                if addr["address"] == ext_ip:
                    return node_name
    raise RuntimeError(f"Cannot find K8s node name for {worker_host}")


def _ensure_helm_on_cp():
    """Install helm on the K8s control-plane node if not already present."""
    r = _ssh(K8S_CONTROL_PLANE, "which helm 2>/dev/null || true", capture=True)
    if "/helm" in r.stdout:
        return
    print("   Installing helm on control-plane ...")
    _ssh(K8S_CONTROL_PLANE,
         "curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 "
         "| bash")


def _wait_for_hugepages(kube_node_name, timeout_seconds=300):
    """Poll K8s until hugepages are reported for the given node."""
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        r = _kubectl(
            ["get", "node", kube_node_name,
             "-o", "jsonpath={.status.allocatable.hugepages-2Mi}"],
            capture=True, check=False,
        )
        value = r.stdout.strip() if hasattr(r, "stdout") and r.stdout else ""
        if value and value != "0":
            return
        time.sleep(5)
    raise RuntimeError(f"HugePages not reported for {kube_node_name} within {timeout_seconds}s")


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Handler that treats redirects as successful responses (don't follow)."""
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def wait_for_http(host, port, path="/", timeout_sec=600, header=None):
    """Poll http://host:port/path until HTTP < 500 or timeout."""
    opener = urllib.request.build_opener(_NoRedirect)
    deadline = time.time() + timeout_sec
    url = f"http://{host}:{port}{path}"
    last_err = None
    while time.time() < deadline:
        try:
            req = urllib.request.Request(url)
            if header:
                req.add_header(*header.split(": ", 1))
            with opener.open(req, timeout=5) as r:
                if r.status < 500:
                    print(f"   READY: HTTP {r.status} from {url}")
                    return True
        except urllib.error.HTTPError as e:
            if e.code < 500:
                print(f"   READY: HTTP {e.code} from {url}")
                return True
            last_err = e
        except Exception as e:
            last_err = e
        elapsed = int(time.time() - (deadline - timeout_sec))
        if elapsed % 30 == 0 or elapsed < 5:
            print(f"   ... {elapsed}s elapsed, still waiting for {url} ... (last error: {last_err})")
        time.sleep(5)
    print(f"   TIMEOUT: {url} not ready after {timeout_sec}s (last error: {last_err})")
    return False


def detect_nvme_disk(host):
    """Return the NVMe data disk to use for OpenEBS (not the OS disk)."""
    result = _ssh(
        host,
        "lsblk -nd -o NAME,SIZE,MODEL | awk '$1~/nvme/ && !/Mayastor/{print $1}'",
        capture=True,
    )
    candidates = [n.strip() for n in result.stdout.strip().splitlines() if n.strip()]
    for name in candidates:
        part_check = _ssh(
            host,
            f"lsblk -n -o TYPE /dev/{name} | grep -c part",
            capture=True, check=False,
        )
        nparts = int(part_check.stdout.strip() or "0")
        if nparts == 0:
            return f"/dev/{name}"
    return f"/dev/{candidates[0]}" if candidates else "/dev/nvme0n1"


# ── Phase 1: K8s cluster setup ────────────────────────────────────────────────


def setup_k8s_cluster():
    """Install K8s on all nodes and initialize the cluster via kubeadm."""
    all_nodes = [K8S_CONTROL_PLANE] + WORKER_NODES

    print("   Pre-flight: verifying all nodes reachable ...")
    for host in all_nodes:
        _ssh(host, "echo ok")
    print("   All nodes reachable.")

    print("   Stopping any running XDN cluster processes ...")
    for host in all_nodes:
        _ssh(host,
             "sudo fuser -k 2000/tcp 2>/dev/null || true; "
             "sudo fuser -k 2300/tcp 2>/dev/null || true; "
             "sudo fuser -k 3000/tcp 2>/dev/null || true; "
             "sudo rm -rf /tmp/gigapaxos /tmp/xdn 2>/dev/null || true; "
             "containers=$(docker ps -a -q); "
             "if [ -n \"$containers\" ]; then docker stop $containers && docker rm $containers; fi",
             check=False)

    print("   Installing K8s dependencies on all nodes ...")
    install_steps = [
        "sudo swapoff -a",
        "sudo sed -i.bak '/\\sswap\\s/s/^/#/' /etc/fstab",
        "sudo apt-get update -y -qq",
        "sudo apt-get install -y -qq apt-transport-https ca-certificates curl gnupg lsb-release conntrack",
        "sudo install -m 0755 -d /etc/apt/keyrings",
        "curl -fsSL https://pkgs.k8s.io/core:/stable:/v1.31/deb/Release.key "
        "| sudo gpg --batch --yes --dearmor -o /etc/apt/keyrings/kubernetes-apt-keyring.gpg",
        'echo "deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] '
        'https://pkgs.k8s.io/core:/stable:/v1.31/deb/ /" '
        "| sudo tee /etc/apt/sources.list.d/kubernetes.list",
        "sudo apt-get update -y -qq",
        "sudo apt-get install -y kubelet kubeadm kubectl",
        "sudo apt-mark hold kubelet kubeadm kubectl",
        "sudo mkdir -p /etc/containerd",
        "sudo bash -lc 'containerd config default "
        "| sed \"s/SystemdCgroup = false/SystemdCgroup = true/\" "
        "> /etc/containerd/config.toml'",
        "sudo systemctl enable --now containerd",
        "sudo systemctl restart containerd",
        "echo 'KUBELET_EXTRA_ARGS=--container-runtime-endpoint=unix:///run/containerd/containerd.sock' "
        "| sudo tee /etc/default/kubelet",
        "sudo systemctl enable --now kubelet",
    ]
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(all_nodes)) as pool:
        def install_on(host):
            for step in install_steps:
                _ssh_with_retry(host, step)
        futs = [pool.submit(install_on, h) for h in all_nodes]
        for f in futs:
            f.result()

    # Disconnect any leftover Mayastor NVMe-oF controllers before reset
    print("   Disconnecting leftover Mayastor NVMe controllers ...")
    for host in all_nodes:
        _ssh(host,
             "which nvme >/dev/null 2>&1 || "
             "sudo apt-get install -y -qq nvme-cli >/dev/null 2>&1; "
             "sudo nvme disconnect-all 2>/dev/null || true",
             check=False)

    print("   Resetting any previous kubeadm state ...")
    for host in all_nodes:
        _ssh(host,
             "sudo kubeadm reset -f 2>/dev/null || true && "
             "sudo rm -rf /etc/kubernetes /var/lib/etcd "
             "/var/lib/kubelet/pki /var/lib/kubelet/config.yaml && "
             "sudo ip link delete cni0 2>/dev/null || true && "
             "sudo ip link delete flannel.1 2>/dev/null || true && "
             "sudo rm -rf /etc/cni/net.d/* /run/flannel",
             check=False)

    print("   Restarting Docker on all nodes to restore iptables chains ...")
    for host in all_nodes:
        _ssh(host, "sudo systemctl restart docker", check=False)
    time.sleep(3)

    print("   Loading br_netfilter and overlay kernel modules ...")
    for host in all_nodes:
        _ssh(host,
             "sudo modprobe br_netfilter && sudo modprobe overlay && "
             "sudo sysctl -w net.bridge.bridge-nf-call-iptables=1 "
             "net.bridge.bridge-nf-call-ip6tables=1 "
             "net.ipv4.ip_forward=1 > /dev/null",
             check=False)

    print(f"   Running kubeadm init on {K8S_CONTROL_PLANE} ...")
    _ssh(K8S_CONTROL_PLANE,
         f"sudo kubeadm init "
         f"--apiserver-advertise-address={K8S_CONTROL_PLANE} "
         f"--pod-network-cidr=10.244.0.0/16")

    print("   Setting up kubeconfig on control plane ...")
    _ssh(K8S_CONTROL_PLANE,
         "mkdir -p $HOME/.kube && "
         "sudo cp /etc/kubernetes/admin.conf $HOME/.kube/config && "
         "sudo chown $(id -u):$(id -g) $HOME/.kube/config")

    print("   Copying kubeconfig to local ~/.kube/config ...")
    _ssh(K8S_CONTROL_PLANE,
         "sudo cp /etc/kubernetes/admin.conf /tmp/admin.conf "
         "&& sudo chmod 644 /tmp/admin.conf")
    os.makedirs(os.path.expanduser("~/.kube"), exist_ok=True)
    subprocess.run(
        ["scp", "-o", "StrictHostKeyChecking=no",
         f"{K8S_CONTROL_PLANE}:/tmp/admin.conf",
         os.path.expanduser("~/.kube/config")],
        check=True,
    )
    _ssh(K8S_CONTROL_PLANE, "rm -f /tmp/admin.conf", check=False)
    kubeconfig_path = os.path.expanduser("~/.kube/config")
    with open(kubeconfig_path) as f:
        cfg = f.read()
    cfg = cfg.replace("https://127.0.0.1:6443", f"https://{K8S_CONTROL_PLANE}:6443")
    cfg = cfg.replace("https://0.0.0.0:6443", f"https://{K8S_CONTROL_PLANE}:6443")
    with open(kubeconfig_path, "w") as f:
        f.write(cfg)

    print("   Applying Flannel CNI ...")
    _kubectl(["apply", "-f",
              "https://raw.githubusercontent.com/flannel-io/flannel/master/"
              "Documentation/kube-flannel.yml"])

    print("   Getting worker join command ...")
    result = _ssh(K8S_CONTROL_PLANE,
                  "sudo kubeadm token create --print-join-command",
                  capture=True)
    join_cmd = result.stdout.strip()
    print(f"   Join command: {join_cmd[:80]}...")

    print("   Joining worker nodes ...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(WORKER_NODES)) as pool:
        futs = [pool.submit(_ssh, host, f"sudo {join_cmd}") for host in WORKER_NODES]
        for f in futs:
            f.result()

    print("   Waiting for all nodes to become Ready ...")
    deadline = time.time() + 120
    while time.time() < deadline:
        result = _kubectl(
            ["get", "nodes", "--no-headers"],
            capture=True, check=False,
        )
        out = result.stdout if hasattr(result, "stdout") and result.stdout else ""
        lines = [l for l in out.strip().splitlines() if l]
        ready = sum(1 for l in lines if "Ready" in l and "NotReady" not in l)
        if ready == len(all_nodes):
            print(f"   All {ready} nodes Ready.")
            break
        print(f"   {ready}/{len(all_nodes)} nodes Ready, waiting ...")
        time.sleep(10)
    _kubectl(["get", "nodes"], check=False)

    # Wait for Flannel DaemonSet pods to be Running on all nodes
    print("   Waiting for Flannel pods to be Running ...")
    deadline = time.time() + 120
    while time.time() < deadline:
        result = _kubectl(
            ["get", "pods", "-n", "kube-flannel", "--no-headers"],
            capture=True, check=False,
        )
        out = result.stdout if hasattr(result, "stdout") and result.stdout else ""
        lines = [l for l in out.strip().splitlines() if l]
        running = sum(1 for l in lines if "Running" in l)
        if running == len(all_nodes):
            print(f"   All {running} Flannel pods Running.")
            break
        print(f"   {running}/{len(all_nodes)} Flannel pods Running, waiting ...")
        time.sleep(10)
    else:
        _kubectl(["get", "pods", "-n", "kube-flannel"], check=False)
        raise RuntimeError("Flannel pods did not become Running within 120s")

    # Verify /run/flannel/subnet.env exists on every node
    print("   Verifying /run/flannel/subnet.env on all nodes ...")
    for host in all_nodes:
        deadline_env = time.time() + 30
        while time.time() < deadline_env:
            r = _ssh(host, "test -f /run/flannel/subnet.env && echo OK",
                     capture=True, check=False)
            if r.stdout and "OK" in r.stdout:
                break
            time.sleep(5)
        else:
            raise RuntimeError(f"/run/flannel/subnet.env missing on {host}")
    print("   Flannel networking verified on all nodes.")


# ── Phase 2: Disk prep ─────────────────────────────────────────────────────────


def prep_disk_one(host):
    """Shrink the emulab VG by removing the NVMe PV, keeping /mydata alive."""
    nvme = detect_nvme_disk(host)
    print(f"   [{host}] NVMe disk to free: {nvme}")

    pv_check = _ssh(
        host,
        f"timeout 10 sudo pvs --noheadings -o vg_name {nvme} 2>/dev/null | tr -d ' '",
        capture=True, check=False,
    )
    in_emulab = pv_check.stdout.strip() == "emulab"

    if in_emulab:
        print(f"   [{host}] Moving extents off {nvme} ...")
        _ssh(host, f"sudo pvmove {nvme}", check=False)
        print(f"   [{host}] Removing {nvme} from emulab VG ...")
        _ssh(host, f"sudo vgreduce emulab {nvme}", check=False)
        _ssh(host, f"sudo pvremove {nvme}", check=False)
        print(f"   [{host}] emulab VG shrunk, /mydata preserved")
    else:
        print(f"   [{host}] {nvme} not in emulab VG (already freed)")

    print(f"   [{host}] Wiping {nvme} ...")
    _ssh(host, f"sudo wipefs -a {nvme} 2>/dev/null || true", check=False)
    _ssh(host, f"sudo dd if=/dev/zero of={nvme} bs=1M count=100 status=none",
         check=False)

    r = _ssh(host, "timeout 5 sudo pvs 2>/dev/null", capture=True)
    print(f"   [{host}] pvs after prep:\n{r.stdout.rstrip()}")


def prep_disks():
    """Free NVMe disk from LVM on all workers in parallel."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(WORKER_NODES)) as pool:
        futs = [pool.submit(prep_disk_one, host) for host in WORKER_NODES]
        for f in futs:
            f.result()
    print("   Disk prep complete.")


# ── Phase 3: OpenEBS Mayastor ──────────────────────────────────────────────────


def setup_openebs():
    """Node prep + Helm install of OpenEBS Mayastor + DiskPools + StorageClass."""
    print("   Running node prep (hugepages, nvme-tcp) ...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(WORKER_NODES)) as pool:
        futs = [pool.submit(run_node_prep, host) for host in WORKER_NODES]
        for f in futs:
            f.result()

    # Label K8s nodes for Mayastor scheduling
    kube_nodes = [_get_kube_node_name(h) for h in WORKER_NODES]
    print(f"   K8s worker node names: {kube_nodes}")
    for kn in kube_nodes:
        _kubectl(["label", "nodes", kn, "openebs.io/engine=mayastor", "--overwrite"])

    # Restart kubelet on workers so hugepage allocation is reflected
    for host in WORKER_NODES:
        _ssh(host, "sudo systemctl restart kubelet")

    print("   Waiting for hugepages to be reported by K8s ...")
    for kn in kube_nodes:
        _wait_for_hugepages(kn, timeout_seconds=300)

    # Install OpenEBS via Helm
    print("   Installing OpenEBS Mayastor via Helm ...")
    _ensure_helm_on_cp()
    _ssh(K8S_CONTROL_PLANE,
         "helm repo add openebs https://openebs.github.io/openebs --force-update")
    _ssh(K8S_CONTROL_PLANE, "helm repo update")

    release_check = _ssh(
        K8S_CONTROL_PLANE,
        "helm status openebs --namespace openebs 2>/dev/null",
        check=False, capture=True,
    )
    if release_check.returncode != 0:
        _ssh(K8S_CONTROL_PLANE,
             "helm upgrade --install openebs --namespace openebs openebs/openebs "
             "--create-namespace --set obs.callhome.enabled=false",
             check=False)

    # Wait for diskpools CRD
    print("   Waiting for diskpools.openebs.io CRD to appear ...")
    crd_deadline = time.time() + 300
    while time.time() < crd_deadline:
        r = _kubectl(["get", "crd", "diskpools.openebs.io"],
                     capture=True, check=False)
        if r.returncode == 0:
            break
        time.sleep(10)
    else:
        _kubectl(["get", "pods", "-n", "openebs"], check=False)
        raise RuntimeError("diskpools.openebs.io CRD did not appear within 300s")

    _kubectl(["wait", "--for=condition=Established",
              "crd/diskpools.openebs.io", "--timeout=120s"])

    # Create one DiskPool per worker using the correct NVMe disk
    for ssh_target, kube_node_name in zip(WORKER_NODES, kube_nodes):
        nvme_disk = detect_nvme_disk(ssh_target)
        print(f"   Creating DiskPool on {kube_node_name} using {nvme_disk} ...")

        disk_leaf = nvme_disk.split("/")[-1]
        id_result = _ssh(
            ssh_target,
            f"ls -l /dev/disk/by-id/ | grep nvme-eui | grep '{disk_leaf}' "
            f"| awk '{{print $9; exit}}'",
            capture=True,
        )
        disk_by_id = id_result.stdout.strip()
        if not disk_by_id:
            openebs_disk_uri = f"aio://{nvme_disk}"
            print(f"   WARNING: no by-id path found, using raw path: {openebs_disk_uri}")
        else:
            openebs_disk_uri = f"aio:///dev/disk/by-id/{disk_by_id}"

        pool_name = f"disk-pool-{kube_node_name.replace('.', '-')}"
        disk_pool_yaml = f"""
apiVersion: openebs.io/v1beta3
kind: DiskPool
metadata:
  name: {pool_name}
  namespace: openebs
spec:
  node: {kube_node_name}
  disks: ["{openebs_disk_uri}"]
"""
        print(f"   Applying DiskPool: {pool_name}")
        _kubectl(["apply", "-f", "-"], manifest=disk_pool_yaml)

    # Create StorageClass (3-replica NVMe-oF)
    storage_class_yaml = """
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: openebs-3-replica
parameters:
  protocol: nvmf
  repl: "3"
provisioner: io.openebs.csi-mayastor
reclaimPolicy: Delete
volumeBindingMode: Immediate
"""
    _kubectl(["apply", "-f", "-"], manifest=storage_class_yaml)

    # Wait for DiskPools to go Online
    print("   Waiting for DiskPools to become Online (up to 5 min) ...")
    deadline = time.time() + 300
    while time.time() < deadline:
        result = _kubectl(
            ["get", "diskpools", "-n", "openebs", "--no-headers"],
            capture=True, check=False,
        )
        out = result.stdout if hasattr(result, "stdout") and result.stdout else ""
        lines = [l for l in out.strip().splitlines() if l]
        online = sum(1 for l in lines if "Online" in l)
        print(f"   {online}/{len(WORKER_NODES)} DiskPools Online ...")
        if online == len(WORKER_NODES):
            break
        time.sleep(15)
    _kubectl(["get", "diskpools", "-n", "openebs"], check=False)


# ── Parameterized K8s manifest generation ─────────────────────────────────────


def generate_pvc_yaml(app_name, storage_size):
    """Generate PVC manifest for OpenEBS 3-replica storage."""
    return f"""
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: {app_name}-pvc
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: {storage_size}
  storageClassName: openebs-3-replica
"""


def _env_list_yaml(env_dict, indent=12):
    """Convert a dict to K8s env YAML list entries."""
    lines = []
    prefix = " " * indent
    for k, v in env_dict.items():
        # Quote the value to handle special chars
        escaped = str(v).replace('"', '\\"')
        lines.append(f'{prefix}- name: {k}')
        lines.append(f'{prefix}  value: "{escaped}"')
    return "\n".join(lines)


def generate_db_manifests(app_name):
    """Return list of (name, yaml) tuples for DB resources, or empty list if no DB."""
    cfg = OPENEBS_APP_CONFIGS[app_name]
    db = cfg.get("db")
    if db is None:
        return []

    db_name = db["name"]
    pvc_name = f"{app_name}-pvc"
    manifests = []

    # DB StatefulSet
    env_yaml = _env_list_yaml(db["env"])
    statefulset_yaml = f"""
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: {db_name}
spec:
  serviceName: db-svc
  replicas: 1
  selector:
    matchLabels:
      app: {db_name}
  template:
    metadata:
      labels:
        app: {db_name}
    spec:
      containers:
        - name: {db_name}
          image: {db["image"]}
          imagePullPolicy: IfNotPresent
          env:
{env_yaml}
          ports:
            - containerPort: {db["port"]}
          volumeMounts:
            - name: db-data
              mountPath: {db["mount"]}
      volumes:
        - name: db-data
          persistentVolumeClaim:
            claimName: {pvc_name}
"""
    manifests.append((f"{db_name}-statefulset", statefulset_yaml))

    # DB Service (headless ClusterIP for StatefulSets)
    db_svc_yaml = f"""
apiVersion: v1
kind: Service
metadata:
  name: db-svc
spec:
  selector:
    app: {db_name}
  ports:
    - port: {db["port"]}
      targetPort: {db["port"]}
  clusterIP: None
"""
    manifests.append((f"{db_name}-svc", db_svc_yaml))

    return manifests


def generate_app_manifests(app_name):
    """Return list of (name, yaml) tuples for the app Deployment + NodePort Service."""
    cfg = OPENEBS_APP_CONFIGS[app_name]
    app = cfg["app"]
    app_label = app["name"]
    manifests = []

    # Volume mount section (only if app has a mount point)
    volume_mounts = ""
    volumes = ""
    if app.get("mount"):
        pvc_name = f"{app_name}-pvc"
        volume_mounts = f"""          volumeMounts:
            - name: app-data
              mountPath: {app["mount"]}"""
        volumes = f"""      volumes:
        - name: app-data
          persistentVolumeClaim:
            claimName: {pvc_name}"""

    # Pod affinity to colocate with DB if present
    affinity = ""
    db = cfg.get("db")
    if db is not None:
        affinity = f"""      affinity:
        podAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            - labelSelector:
                matchLabels:
                  app: {db["name"]}
              topologyKey: kubernetes.io/hostname"""

    # Env vars
    env_yaml = _env_list_yaml(app.get("env", {}))
    env_section = ""
    if app.get("env"):
        env_section = f"""          env:
{env_yaml}"""

    deploy_yaml = f"""
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {app_label}
spec:
  replicas: 1
  selector:
    matchLabels:
      app: {app_label}
  template:
    metadata:
      labels:
        app: {app_label}
    spec:
{affinity}
      containers:
        - name: {app_label}
          image: {app["image"]}
          imagePullPolicy: Never
{env_section}
          ports:
            - containerPort: {app["port"]}
{volume_mounts}
{volumes}
"""
    manifests.append((f"{app_label}-deploy", deploy_yaml))

    svc_yaml = f"""
apiVersion: v1
kind: Service
metadata:
  name: {app_label}-svc
spec:
  type: NodePort
  selector:
    app: {app_label}
  ports:
    - port: {app["port"]}
      targetPort: {app["port"]}
      nodePort: {APP_NODEPORT}
"""
    manifests.append((f"{app_label}-svc", svc_yaml))

    return manifests


# ── App-specific setup helpers ────────────────────────────────────────────────


def _parse_go_output(path):
    """Parse key:value output from get_latency_at_rate.go."""
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
        "throughput_rps":      metrics.get("actual_throughput_rps", 0.0),
        "actual_achieved_rps": metrics.get("actual_achieved_rate_rps", 0.0),
        "avg_ms":              metrics.get("average_latency_ms", 0.0),
        "p50_ms":              metrics.get("median_latency_ms", 0.0),
        "p90_ms":              metrics.get("p90_latency_ms", 0.0),
        "p95_ms":              metrics.get("p95_latency_ms", 0.0),
        "p99_ms":              metrics.get("p99_latency_ms", 0.0),
    }


def disable_wp_cron_k8s():
    """Disable WP-Cron and auto-updates in the K8s WordPress pod."""
    cmd = (
        "kubectl exec deploy/wordpress -- bash -c "
        "\"grep -q DISABLE_WP_CRON /var/www/html/wp-config.php || "
        "sed -i '/That.*stop editing/i "
        "define('\\''DISABLE_WP_CRON'\\'', true);\\n"
        "define('\\''AUTOMATIC_UPDATER_DISABLED'\\'', true);\\n"
        "define('\\''WP_AUTO_UPDATE_CORE'\\'', false);' "
        "/var/www/html/wp-config.php\""
    )
    result = subprocess.run(
        ["ssh", "-o", "StrictHostKeyChecking=no", K8S_CONTROL_PLANE, cmd],
        capture_output=True, text=True,
    )
    if result.returncode == 0:
        print("   WP-Cron and auto-updates disabled in wp-config.php")
    else:
        print(f"   WARNING: failed to disable WP-Cron: {result.stderr.strip()}")


def tune_mysql_k8s():
    """Tune MySQL InnoDB settings in the K8s MySQL pod for write-heavy workloads."""
    print("   Tuning MySQL InnoDB settings ...")
    tune_sql = (
        "SET GLOBAL innodb_buffer_pool_size=1073741824; "
        "SET GLOBAL innodb_change_buffer_max_size=50; "
        "SET GLOBAL innodb_max_dirty_pages_pct=90; "
        "SET GLOBAL innodb_max_dirty_pages_pct_lwm=80; "
        "SET GLOBAL innodb_io_capacity=200; "
        "SET GLOBAL innodb_io_capacity_max=400; "
        "SET GLOBAL innodb_lru_scan_depth=256; "
    )
    cmd = f"kubectl exec statefulset/mysql -- mysql -psupersecret -e \"{tune_sql}\""
    result = subprocess.run(
        ["ssh", "-o", "StrictHostKeyChecking=no", K8S_CONTROL_PLANE, cmd],
        capture_output=True, text=True, timeout=30,
    )
    if result.returncode == 0:
        print("   MySQL InnoDB tuned successfully")
    else:
        print(f"   WARNING: MySQL tuning failed: {result.stderr.strip()}")
    time.sleep(2)


def enable_rest_auth_k8s():
    """Install mu-plugin + create WP Application Password in the K8s WordPress pod."""
    import pb_app_configs as _app_cfg_mod

    mu_plugin = (
        "<?php\n"
        "// Allow WP Application Passwords over plain HTTP (XDN benchmark).\n"
        "add_filter('wp_is_application_passwords_available', '__return_true');\n"
    )
    install_cmd = (
        "kubectl exec -i deploy/wordpress -- bash -c "
        "'mkdir -p /var/www/html/wp-content/mu-plugins && "
        "cat > /var/www/html/wp-content/mu-plugins/xdn-app-passwords.php'"
    )
    subprocess.run(
        ["ssh", "-o", "StrictHostKeyChecking=no", K8S_CONTROL_PLANE, install_cmd],
        input=mu_plugin, text=True,
    )
    print("   mu-plugin installed: Application Passwords enabled on HTTP")

    php_script = (
        "<?php "
        "require('/var/www/html/wp-load.php'); "
        "$user = get_user_by('login', 'admin'); "
        "if (!$user) { echo 'ERROR:no_admin'; exit(1); } "
        "$existing = WP_Application_Passwords::get_user_application_passwords($user->ID); "
        "foreach ($existing as $ap) { "
        "  if ($ap['name'] === 'xdn-bench') "
        "    WP_Application_Passwords::delete_application_password($user->ID, $ap['uuid']); "
        "} "
        "list($new_password) = WP_Application_Passwords::create_new_application_password("
        "  $user->ID, ['name' => 'xdn-bench']); "
        "echo $new_password;"
    )

    def _is_valid_app_password(pw):
        if not pw or len(pw) < 10:
            return False
        if "<" in pw or "ERROR" in pw or "Fatal" in pw:
            return False
        return bool(re.match(r'^[A-Za-z0-9 ]+$', pw))

    for attempt in range(1, 6):
        result = subprocess.run(
            ["ssh", "-o", "StrictHostKeyChecking=no", K8S_CONTROL_PLANE,
             "kubectl exec -i deploy/wordpress -- php"],
            input=php_script, text=True, capture_output=True,
        )
        app_password = result.stdout.strip()
        if result.returncode == 0 and _is_valid_app_password(app_password):
            _app_cfg_mod._wp_app_password = app_password
            print(f"   WP Application Password created (first 5 chars: {app_password[:5]}...)")
            return True
        snippet = (app_password or result.stderr or "(empty)")[:150]
        print(f"   Attempt {attempt}/5: invalid response: {snippet}")
        if attempt < 5:
            print(f"   Retrying in 10s ...")
            time.sleep(10)
    print(f"   ERROR creating app password after 5 attempts")
    return False


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


def _wp_create_post_k8s(host, port, title, content):
    """Create a single WordPress post via REST API (no XDN header, direct K8s access)."""
    import base64
    import pb_app_configs as _app_cfg_mod

    password = _app_cfg_mod._wp_app_password or ADMIN_PASSWORD
    auth = "Basic " + base64.b64encode(f"{ADMIN_USER}:{password}".encode()).decode()

    import requests
    r = requests.post(
        f"http://{host}:{port}/?rest_route=/wp/v2/posts",
        headers={
            "Content-Type": "application/json",
            "Authorization": auth,
        },
        data=json.dumps({"title": title, "content": content, "status": "publish"}),
        timeout=15,
    )
    if r.status_code not in (200, 201):
        print(f"   ERROR: HTTP {r.status_code} -- {r.text[:200]}")
        return None
    return r.json().get("id")


def seed_wordpress_k8s(host, port, count, results_dir):
    """Seed WordPress posts and write XML-RPC payloads file."""
    seed_ids = []
    for i in range(count):
        title = f"Seed Post {i + 1}"
        content = f"Seed content {i + 1}."
        pid = None
        for attempt in range(1, 4):
            try:
                pid = _wp_create_post_k8s(host, port, title, content)
                if pid:
                    break
            except Exception as e:
                print(f"   WARNING: attempt {attempt} failed ({e}), retrying ...")
                time.sleep(5)
        if pid:
            seed_ids.append(pid)
        else:
            print(f"   WARNING: seed post '{title}' failed, continuing")
    print(f"   Seeded {len(seed_ids)}/{count} posts")

    # Write XML-RPC payloads file
    payloads_file = results_dir / "xmlrpc_payloads.txt"
    with open(payloads_file, "w") as fh:
        for pid in seed_ids:
            fh.write(_xmlrpc_editpost_payload(pid) + "\n")
    print(f"   Done: {len(seed_ids)} payloads written to {payloads_file}")
    return payloads_file


def seed_bookcatalog_k8s(host, port, count):
    """Seed bookcatalog with dummy books via POST /api/books."""
    import requests
    created = 0
    for i in range(count):
        try:
            r = requests.post(
                f"http://{host}:{port}/api/books",
                json={"title": f"bench book {i}", "author": f"author {i}"},
                timeout=10,
            )
            if r.status_code in (200, 201):
                created += 1
        except Exception:
            pass
    print(f"   Seeded {created}/{count} books")
    return created


def seed_tpcc_k8s(host, port, num_warehouses, results_dir):
    """Initialize TPC-C schema and generate payloads file.

    Supports both Python TPC-C (POST /init) and Java TPC-C (POST /init_db).
    """
    import requests

    # Try Java endpoint first, then Python
    print(f"   Initializing TPC-C with {num_warehouses} warehouses ...")
    for attempt in range(1, 6):
        try:
            # Try Java tpcc-java endpoint
            r = requests.post(
                f"http://{host}:{port}/init_db",
                json={"warehouses": num_warehouses},
                timeout=300,
            )
            if r.status_code == 200:
                print(f"   TPC-C initialized (Java): {r.text[:200]}")
                break
        except Exception:
            pass
        try:
            # Try Python tpcc endpoint
            r = requests.post(
                f"http://{host}:{port}/init",
                json={"num_warehouses": num_warehouses},
                timeout=120,
            )
            if r.status_code == 200:
                print(f"   TPC-C initialized (Python): {r.text[:200]}")
                break
        except Exception as e:
            print(f"   Attempt {attempt} failed: {e}")
            time.sleep(10)
    else:
        print("   WARNING: TPC-C init failed after 5 attempts")
        return None

    # Generate payloads file — simple format compatible with both services
    import random
    payloads_file = results_dir / "neworder_payloads.txt"
    with open(payloads_file, "w") as fh:
        for _ in range(1000):
            w_id = random.randint(1, num_warehouses)
            c_id = random.randint(1, num_warehouses * 10)
            fh.write(json.dumps({"w_id": w_id, "c_id": c_id}) + "\n")
    print(f"   Generated 1000 payloads to {payloads_file}")
    return payloads_file


def seed_hotelres_k8s(host, port, count, results_dir):
    """Generate dummy hotel data and reservation URLs."""
    import requests

    # Generate hotels + rooms
    print("   Generating hotel dummy data ...")
    for attempt in range(1, 4):
        try:
            r = requests.post(
                f"http://{host}:{port}/populate",
                json={"num_hotels": 20, "rooms_per_hotel": 10},
                timeout=30,
            )
            if r.status_code == 200:
                print(f"   Hotel data populated: {r.text[:200]}")
                break
        except Exception as e:
            print(f"   Attempt {attempt} failed: {e}")
            time.sleep(10)
    else:
        print("   WARNING: hotel data population failed")

    # Generate reservation URLs
    urls_file = results_dir / "reservation_urls.txt"
    with open(urls_file, "w") as fh:
        for i in range(count):
            hotel_id = (i % 20) + 1
            fh.write(f"http://{host}:{port}/reservation?hotelId={hotel_id}\n")
    print(f"   Generated {count} reservation URLs to {urls_file}")
    return urls_file


# ── Deploy + cleanup ──────────────────────────────────────────────────────────


def cleanup_app_k8s(app_name):
    """Delete all K8s resources for the given app. Safe to call if nothing exists."""
    cfg = OPENEBS_APP_CONFIGS[app_name]
    app_label = cfg["app"]["name"]
    print(f"   Cleaning up {app_name} K8s resources ...")

    resources = [f"deployment/{app_label}", f"service/{app_label}-svc"]
    db = cfg.get("db")
    if db:
        resources += [f"statefulset/{db['name']}", "service/db-svc"]
    resources.append(f"pvc/{app_name}-pvc")

    for resource in resources:
        try:
            _kubectl(["delete", resource, "--ignore-not-found", "--timeout=30s"],
                     check=False)
        except Exception:
            pass

    # Wait for pods to terminate
    label_selector = f"app in ({app_label}"
    if db:
        label_selector += f",{db['name']}"
    label_selector += ")"
    deadline = time.time() + 60
    while time.time() < deadline:
        result = _kubectl(
            ["get", "pods", "-l", label_selector, "--no-headers"],
            check=False, capture=True,
        )
        if not result.stdout.strip():
            break
        time.sleep(3)
    print("   Cleanup complete.")


def deploy_app_k8s(app_name):
    """Apply K8s manifests for an app and wait for rollout."""
    cfg = OPENEBS_APP_CONFIGS[app_name]
    app_label = cfg["app"]["name"]
    db = cfg.get("db")

    # Apply PVC
    pvc_yaml = generate_pvc_yaml(app_name, cfg["pvc_size"])
    print(f"     Applying {app_name}-pvc ...")
    _kubectl(["apply", "-f", "-"], manifest=pvc_yaml)

    # Apply DB manifests if present
    if db:
        db_manifests = generate_db_manifests(app_name)
        for name, yaml in db_manifests:
            print(f"     Applying {name} ...")
            _kubectl(["apply", "-f", "-"], manifest=yaml)
        print(f"   Waiting for {db['name']} StatefulSet rollout ...")
        _kubectl(["rollout", "status", f"statefulset/{db['name']}", "--timeout=300s"])

    # Apply app manifests
    app_manifests = generate_app_manifests(app_name)
    for name, yaml in app_manifests:
        print(f"     Applying {name} ...")
        _kubectl(["apply", "-f", "-"], manifest=yaml)
    print(f"   Waiting for {app_label} Deployment rollout ...")
    _kubectl(["rollout", "status", f"deployment/{app_label}", "--timeout=300s"])

    # Wait for HTTP readiness
    readiness_path = cfg.get("readiness_path", "/")
    print(f"   Waiting for {app_label} to be HTTP-ready at {readiness_path} ...")
    ok = wait_for_http(WORKER_NODES[0], APP_NODEPORT, readiness_path, timeout_sec=600)
    if not ok:
        print(f"ERROR: {app_label} not ready after 600s -- check: kubectl get pods")
        sys.exit(1)


# ── Load test helpers ─────────────────────────────────────────────────────────


def _build_load_cmd(app_name, url, rate, output_file, duration_sec, results_dir):
    """Build the go load client command for a given app."""
    cfg = OPENEBS_APP_CONFIGS[app_name]
    app_cfg_entry = APP_CONFIGS.get(app_name, {})
    wl = app_cfg_entry.get("workload", {})
    app_port = cfg["app"]["port"]

    cmd = ["go", "run", str(GO_LATENCY_CLIENT)]

    # Content-Type header
    content_type = wl.get("content_type")
    if content_type:
        cmd += ["-H", f"Content-Type: {content_type}"]

    # HTTP method
    method = wl.get("method", "POST")
    cmd += ["-X", method]

    # Payloads file or URLs file
    if wl.get("uses_payloads_file"):
        if app_name == "wordpress":
            payloads_path = str(results_dir / "xmlrpc_payloads.txt")
        elif app_name in ("tpcc", "tpcc-java"):
            payloads_path = str(results_dir / "neworder_payloads.txt")
        else:
            payloads_path = str(results_dir / "payloads.txt")
        cmd += ["-payloads-file", payloads_path]

    if wl.get("uses_urls_file"):
        urls_path = str(results_dir / "reservation_urls.txt")
        cmd += ["-urls-file", urls_path]

    # URL
    cmd.append(url)

    # Body placeholder
    if wl.get("payload"):
        cmd.append(wl["payload"])
    elif app_name in ("tpcc", "tpcc-java"):
        cmd.append('{"w_id": 1, "c_id": 1}')
    elif app_name == "synth":
        cmd.append(make_synth_workload_payload())
    else:
        cmd.append("placeholder")

    cmd += [str(duration_sec), str(rate)]
    return cmd


def run_load(app_name, url, rate, output_file, duration_sec, results_dir):
    """Run get_latency_at_rate.go at `rate` req/s for `duration_sec` seconds."""
    output_file.parent.mkdir(parents=True, exist_ok=True)
    cmd = _build_load_cmd(app_name, url, rate, output_file, duration_sec, results_dir)
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


# ── Main ──────────────────────────────────────────────────────────────────────


def parse_args():
    p = argparse.ArgumentParser(
        description="Consolidated OpenEBS Mayastor 3-replica benchmark for all apps.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--app", type=str, required=True,
                   choices=list(OPENEBS_APP_CONFIGS.keys()),
                   help="Application to benchmark")
    p.add_argument("--skip-k8s", action="store_true",
                   help="Skip K8s cluster setup (assumes cluster already running)")
    p.add_argument("--skip-disk-prep", action="store_true",
                   help="Skip LVM disk prep; assumes disks already freed")
    p.add_argument("--skip-openebs", action="store_true",
                   help="Skip OpenEBS install (assumes StorageClass already present)")
    p.add_argument("--skip-deploy", action="store_true",
                   help="Skip K8s app deployment (assumes pods already running)")
    p.add_argument("--rates", type=str, default=None,
                   help="Comma-separated list of offered rates (req/s) to override default")
    p.add_argument("--duration", type=int, default=LOAD_DURATION_SEC,
                   help=f"Measurement duration per rate point in seconds (default: {LOAD_DURATION_SEC})")
    p.add_argument("--warmup-duration", type=int, default=WARMUP_DURATION_SEC,
                   help=f"Warmup duration per rate point in seconds (default: {WARMUP_DURATION_SEC})")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    app_name = args.app
    cfg = OPENEBS_APP_CONFIGS[app_name]
    app_label = cfg["app"]["name"]

    # Timestamped results directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_dir = RESULTS_BASE / f"load_pb_{app_name}_openebs_{timestamp}"
    results_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print(f"{app_name.upper()} OpenEBS 3-Replica Benchmark")
    print("=" * 60)

    # Phase 0 -- Restart kubelet on all nodes and verify readiness
    all_k8s_nodes = WORKER_NODES + [K8S_CONTROL_PLANE]
    if not args.skip_k8s:
        print(f"\n[Phase 0] Restarting kubelet on all {len(all_k8s_nodes)} nodes ...")
        for host in all_k8s_nodes:
            result = subprocess.run(
                ["ssh", "-o", "StrictHostKeyChecking=no", host,
                 "sudo systemctl restart kubelet"],
                capture_output=True, text=True, timeout=30,
            )
            if result.returncode != 0:
                print(f"   ERROR: failed to restart kubelet on {host}: {result.stderr.strip()}")
                sys.exit(1)
            print(f"   Restarted kubelet on {host}")

        print("   Waiting for all nodes to become Ready ...")
        deadline = time.time() + 120
        while time.time() < deadline:
            result = subprocess.run(
                ["ssh", "-o", "StrictHostKeyChecking=no", K8S_CONTROL_PLANE,
                 "kubectl get nodes --no-headers"],
                capture_output=True, text=True, timeout=15,
            )
            if result.returncode == 0:
                lines = [l for l in result.stdout.strip().splitlines() if l.strip()]
                not_ready = [l for l in lines if " Ready " not in l]
                if len(lines) == len(all_k8s_nodes) and not not_ready:
                    print(f"   All {len(lines)} nodes are Ready")
                    break
                ready_count = len(lines) - len(not_ready)
                print(f"   {ready_count}/{len(all_k8s_nodes)} nodes Ready, waiting ...")
            time.sleep(5)
        else:
            print("ERROR: not all K8s nodes became Ready within 120s")
            sys.exit(1)

    # Phase 1 -- K8s cluster setup
    if args.skip_k8s:
        print("\n[Phase 1] Skipping K8s cluster setup (--skip-k8s)")
    else:
        print("\n[Phase 1] Setting up K8s cluster ...")
        setup_k8s_cluster()

    # Phase 2 -- Disk prep
    if args.skip_disk_prep:
        print("\n[Phase 2] Skipping disk prep (--skip-disk-prep)")
    else:
        print("\n[Phase 2] Prepping NVMe disks for OpenEBS ...")
        prep_disks()

    # Phase 3 -- OpenEBS Mayastor
    if args.skip_openebs:
        print("\n[Phase 3] Skipping OpenEBS install (--skip-openebs)")
    else:
        print("\n[Phase 3] Installing OpenEBS Mayastor ...")
        setup_openebs()

    # Determine rate list and durations
    if args.rates:
        rates = [int(r.strip()) for r in args.rates.split(",")]
    else:
        rates = BOTTLENECK_RATES
    load_duration = args.duration
    warmup_duration = args.warmup_duration

    app_host = WORKER_NODES[0]
    wl = APP_CONFIGS.get(app_name, {}).get("workload", {})
    url_path = wl.get("url_path", cfg.get("readiness_path", "/"))
    app_url = f"http://{app_host}:{APP_NODEPORT}{url_path}"

    print(f"\n  App         : {app_name}")
    print(f"  Rates       : {rates} req/s")
    print(f"  Duration    : {load_duration}s per rate (warmup {warmup_duration}s)")
    print(f"  Results     : {results_dir}")

    results = []

    try:
        # Phase 4 -- Clean + Deploy app
        print(f"\n[Phase 4] Cleaning up previous {app_name} K8s resources ...")
        cleanup_app_k8s(app_name)

        if args.skip_deploy:
            print(f"\n[Phase 4] Skipping {app_name} deployment (--skip-deploy)")
        else:
            print(f"\n[Phase 4] Deploying {app_name} on K8s ...")
            deploy_app_k8s(app_name)

        # Phase 5 -- App-specific setup and seeding
        print(f"\n[Phase 5] App-specific setup for {app_name} ...")

        if app_name == "wordpress":
            # Install WordPress
            print("   Installing WordPress ...")
            ok = install_wordpress(app_host, APP_NODEPORT, None)
            if not ok:
                print("ERROR: WordPress installation failed.")
                sys.exit(1)

            # Enable REST API auth
            print("   Enabling REST API auth ...")
            if not enable_rest_auth_k8s():
                print("ERROR: REST API auth setup failed.")
                sys.exit(1)
            disable_wp_cron_k8s()
            tune_mysql_k8s()

            # Seed posts
            seed_count = cfg.get("seed_count", 200)
            print(f"   Seeding {seed_count} posts ...")
            payloads_file = seed_wordpress_k8s(app_host, APP_NODEPORT, seed_count, results_dir)

        elif app_name == "bookcatalog":
            seed_count = cfg.get("seed_count", 200)
            print(f"   Seeding {seed_count} books ...")
            seed_bookcatalog_k8s(app_host, APP_NODEPORT, seed_count)

        elif app_name in ("tpcc", "tpcc-java"):
            num_warehouses = cfg.get("seed_count", 10)
            print(f"   Initializing TPC-C with {num_warehouses} warehouses ...")
            seed_tpcc_k8s(app_host, APP_NODEPORT, num_warehouses, results_dir)

        elif app_name == "hotelres":
            seed_count = cfg.get("seed_count", 500)
            print(f"   Seeding hotel reservation data ...")
            seed_hotelres_k8s(app_host, APP_NODEPORT, seed_count, results_dir)

        elif app_name == "synth":
            print("   Synth: no seeding needed")

        # Phase 6 -- Load sweep
        print(f"\n[Phase 6] Load sweep at {rates} req/s ...")
        print(f"   URL      : {app_url}")
        print(f"   Duration : {load_duration}s per rate (warmup {warmup_duration}s)")

        warmup_rate = APP_CONFIGS.get(app_name, {}).get("warmup_rate", 1500)
        threshold = APP_CONFIGS.get(app_name, {}).get("avg_latency_threshold_ms",
                                                       AVG_LATENCY_THRESHOLD_MS)

        for i, rate in enumerate(rates):
            if i > 0 and INTER_RATE_PAUSE_SEC > 0:
                print(f"   Pausing {INTER_RATE_PAUSE_SEC}s between rate points ...")
                time.sleep(INTER_RATE_PAUSE_SEC)

            # Warmup
            print(f"\n   -> warmup at {warmup_rate} req/s for {warmup_duration}s ...")
            wm = run_load(app_name, app_url, warmup_rate,
                          results_dir / f"warmup_rate{rate}.txt",
                          warmup_duration, results_dir)
            print(f"     [warmup] tput={wm['throughput_rps']:.2f} rps  avg={wm['avg_ms']:.1f}ms")
            if wm["throughput_rps"] < 0.1 * warmup_rate:
                print(
                    f"   SKIP: warmup tput={wm['throughput_rps']:.2f} rps "
                    f"< 10% of {warmup_rate} rps -- system not ready"
                )
                continue
            print("   Settling 5s after warmup ...")
            time.sleep(5)

            # Measurement
            print(f"   -> rate={rate} req/s (measuring {load_duration}s) ...")
            m = run_load(app_name, app_url, rate,
                         results_dir / f"rate{rate}.txt",
                         load_duration, results_dir)
            row = {"rate_rps": rate, **m}
            results.append(row)
            print(
                f"     tput={m['throughput_rps']:.2f} rps  "
                f"avg={m['avg_ms']:.1f}ms  "
                f"p50={m['p50_ms']:.1f}ms  "
                f"p95={m['p95_ms']:.1f}ms"
            )

            # Saturation checks
            avg_exceeded = m["avg_ms"] > threshold
            tput_dropped = (m["actual_achieved_rps"] > 0 and
                            m["throughput_rps"] < 0.75 * m["actual_achieved_rps"])
            if avg_exceeded and tput_dropped:
                print(
                    f"   System saturated: avg {m['avg_ms']:.0f}ms > {threshold}ms AND "
                    f"throughput {m['throughput_rps']:.2f} < 75% of "
                    f"{m['actual_achieved_rps']:.2f}; stopping."
                )
                break
            if avg_exceeded:
                print(f"   Avg latency {m['avg_ms']:.0f}ms > {threshold}ms "
                      f"(throughput OK); continuing.")
            if tput_dropped:
                print(f"   Throughput {m['throughput_rps']:.2f} < 75% of "
                      f"{m['actual_achieved_rps']:.2f} (latency OK); continuing.")

    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Always clean up K8s resources
        print(f"\n[Cleanup] Removing {app_name} K8s resources ...")
        cleanup_app_k8s(app_name)

    # Write CSV
    csv_path = results_dir / f"{app_name}_openebs_results.csv"
    fieldnames = ["rate_rps", "achieved_rps", "throughput_rps", "avg_ms",
                  "p50_ms", "p90_ms", "p95_ms", "p99_ms"]
    with open(csv_path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in results:
            writer.writerow({
                "rate_rps": row["rate_rps"],
                "achieved_rps": row.get("actual_achieved_rps", ""),
                "throughput_rps": row.get("throughput_rps", ""),
                "avg_ms": row.get("avg_ms", ""),
                "p50_ms": row.get("p50_ms", ""),
                "p90_ms": row.get("p90_ms", ""),
                "p95_ms": row.get("p95_ms", ""),
                "p99_ms": row.get("p99_ms", ""),
            })
    print(f"\nCSV saved to {csv_path}")

    # Summary
    print(f"\n{'=' * 60}")
    print(f"Summary ({app_name} OpenEBS)")
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
        ["python3", "plot_load_latency.py", "--results-dir", str(results_dir)],
        capture_output=True, text=True, cwd=str(SCRIPT_DIR),
    )
    print(r.stdout)
    if r.returncode != 0:
        print(f"   WARNING: plot_load_latency.py failed:\n{r.stderr}")

    print(f"\n[Done] Results in {results_dir}/")
