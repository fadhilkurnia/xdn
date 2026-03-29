import warnings as _w; _w.warn("DEPRECATED: Use run_load_pb_openebs_app.py --app tpcc instead.", DeprecationWarning, stacklevel=2)
"""
run_load_pb_tpcc_openebs.py — OpenEBS Mayastor 3-replica TPC-C benchmark.

Sets up a K8s cluster on CloudLab with OpenEBS Mayastor block-level NVMe-oF
replication (3 replicas), deploys PostgreSQL + TPC-C Flask app, and runs the
same POST /orders New Order workload used by the XDN PB benchmark for direct
comparison.

Run from the eval/ directory:
    python3 run_load_pb_tpcc_openebs.py [--skip-k8s] [--skip-disk-prep]

Outputs go to eval/results/load_pb_tpcc_openebs/:
    rate1.txt  rate100.txt  ...  — go-client output per rate point
"""

import argparse
import concurrent.futures
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from run_load_pb_tpcc_reflex import (
    AVG_LATENCY_THRESHOLD_MS,
    BOTTLENECK_RATES,
    GO_LATENCY_CLIENT,
    INTER_RATE_PAUSE_SEC,
    LOAD_DURATION_SEC,
    NUM_WAREHOUSES,
    WARMUP_DURATION_SEC,
    _parse_go_output,
    generate_payloads_file,
    wait_for_port,
)
from init_openebs import run_node_prep

# ── Constants ─────────────────────────────────────────────────────────────────

K8S_CONTROL_PLANE = "10.10.1.4"
WORKER_NODES = ["10.10.1.1", "10.10.1.2", "10.10.1.3"]
TPCC_NODEPORT = 30080
PG_PASSWORD = "benchpass"
PG_DB = "tpcc"

RESULTS_BASE = Path(__file__).resolve().parent / "results"
RESULTS_DIR = RESULTS_BASE / "load_pb_tpcc_openebs"
SCRIPT_DIR = Path(__file__).resolve().parent
PAYLOADS_FILE = RESULTS_DIR / "neworder_payloads.txt"

# ── K8s manifests ─────────────────────────────────────────────────────────────

PG_PVC_YAML = """
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: postgres-pvc
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 20Gi
  storageClassName: openebs-3-replica
"""

PG_STATEFULSET_YAML = f"""
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: postgres
spec:
  serviceName: postgres-svc
  replicas: 1
  selector:
    matchLabels:
      app: postgres
  template:
    metadata:
      labels:
        app: postgres
    spec:
      containers:
        - name: postgres
          image: postgres:17.4-bookworm
          env:
            - name: POSTGRES_PASSWORD
              value: "{PG_PASSWORD}"
            - name: POSTGRES_DB
              value: "{PG_DB}"
            - name: PGDATA
              value: /var/lib/postgresql/data/pgdata
          ports:
            - containerPort: 5432
          volumeMounts:
            - name: postgres-data
              mountPath: /var/lib/postgresql/data
      volumes:
        - name: postgres-data
          persistentVolumeClaim:
            claimName: postgres-pvc
"""

PG_SVC_YAML = """
apiVersion: v1
kind: Service
metadata:
  name: postgres-svc
spec:
  selector:
    app: postgres
  ports:
    - port: 5432
      targetPort: 5432
  clusterIP: None
"""

TPCC_DEPLOY_YAML = f"""
apiVersion: apps/v1
kind: Deployment
metadata:
  name: tpcc-app
spec:
  replicas: 1
  selector:
    matchLabels:
      app: tpcc-app
  template:
    metadata:
      labels:
        app: tpcc-app
    spec:
      containers:
        - name: tpcc-app
          image: fadhilkurnia/xdn-tpcc
          env:
            - name: DATABASE_URL
              value: "postgresql://postgres:{PG_PASSWORD}@postgres-svc:5432/{PG_DB}"
            - name: GUNICORN_CMD_ARGS
              value: "--workers 4 --timeout 120"
          ports:
            - containerPort: 8000
"""

TPCC_SVC_YAML = f"""
apiVersion: v1
kind: Service
metadata:
  name: tpcc-svc
spec:
  type: NodePort
  selector:
    app: tpcc-app
  ports:
    - port: 8000
      targetPort: 8000
      nodePort: {TPCC_NODEPORT}
"""

# ── Load test helpers ─────────────────────────────────────────────────────────


def _run_load(url, rate, output_file, duration_sec):
    """Run get_latency_at_rate.go at `rate` req/s for `duration_sec` seconds.

    No XDN header, no auth — direct to TPC-C app via NodePort.
    """
    output_file.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "go", "run", str(GO_LATENCY_CLIENT),
        "-H", "Content-Type: application/json",
        "-X", "POST",
        "-payloads-file", str(PAYLOADS_FILE),
        url,
        '{"w_id": 1, "c_id": 1}',
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


# ── Cluster helpers ───────────────────────────────────────────────────────────


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


def _get_kube_node_name(worker_host: str) -> str:
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


def _wait_for_hugepages(kube_node_name: str, timeout_seconds: int = 300):
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


def wait_for_http(host, port, path="/", timeout_sec=300):
    """Poll http://host:port/path until HTTP 200 or timeout."""
    import urllib.request
    deadline = time.time() + timeout_sec
    url = f"http://{host}:{port}{path}"
    while time.time() < deadline:
        try:
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=5) as r:
                if r.status < 500:
                    print(f"   READY: HTTP {r.status} from {url}")
                    return True
        except Exception:
            pass
        elapsed = int(time.time() - (deadline - timeout_sec))
        if elapsed % 30 == 0 or elapsed < 5:
            print(f"   ... {elapsed}s elapsed, still waiting for {url} ...")
        time.sleep(5)
    print(f"   TIMEOUT: {url} not ready after {timeout_sec}s")
    return False


# ── Phase 1: K8s cluster setup ───────────────────────────────────────────────


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

    # Disconnect any leftover Mayastor NVMe-oF controllers before reset.
    # These virtual NVMe devices cause LVM commands to hang indefinitely.
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

    # Load br_netfilter and overlay kernel modules on every node.
    # Flannel (and kube-proxy) require bridge-nf-call-iptables which needs
    # br_netfilter; without it Flannel pods CrashLoopBackOff.
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

    # Wait for Flannel DaemonSet pods to be Running on all nodes.
    # Without this, pods on workers fail sandbox creation because
    # /run/flannel/subnet.env hasn't been written yet.
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

    # Verify /run/flannel/subnet.env exists on every node.
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


# ── Phase 2: Disk prep ───────────────────────────────────────────────────────


def detect_nvme_disk(host):
    """Return the NVMe data disk to use for OpenEBS (not the OS disk).

    Avoids LVM commands (pvs/vgs) because leftover Mayastor NVMe controllers
    can cause them to hang indefinitely.  Uses lsblk only.
    """
    # List all real NVMe disks, excluding Mayastor virtual controllers.
    # lsblk -nd gives whole-disk entries; we filter by MODEL to skip Mayastor.
    result = _ssh(
        host,
        "lsblk -nd -o NAME,SIZE,MODEL | awk '$1~/nvme/ && !/Mayastor/{print $1}'",
        capture=True,
    )
    candidates = [n.strip() for n in result.stdout.strip().splitlines() if n.strip()]

    # Prefer a disk with NO partitions (the data disk).
    for name in candidates:
        part_check = _ssh(
            host,
            f"lsblk -n -o TYPE /dev/{name} | grep -c part",
            capture=True, check=False,
        )
        nparts = int(part_check.stdout.strip() or "0")
        if nparts == 0:
            return f"/dev/{name}"

    # All disks have partitions — return the largest one without a root mount.
    return f"/dev/{candidates[0]}" if candidates else "/dev/nvme0n1"


def prep_disk_one(host):
    """Shrink the emulab VG by removing the NVMe PV, keeping /mydata alive.

    Instead of destroying the VG (which breaks /mydata and causes emergency
    mode on reboot), we use pvmove to migrate extents off the NVMe onto the
    remaining PV, then vgreduce to remove the NVMe from the VG.  The LV and
    /mydata mount stay intact on the other PV (typically nvme0n1p4).
    """
    nvme = detect_nvme_disk(host)
    print(f"   [{host}] NVMe disk to free: {nvme}")

    # Check whether the NVMe is currently a PV in the emulab VG.
    pv_check = _ssh(
        host,
        f"timeout 10 sudo pvs --noheadings -o vg_name {nvme} 2>/dev/null | tr -d ' '",
        capture=True, check=False,
    )
    in_emulab = pv_check.stdout.strip() == "emulab"

    if in_emulab:
        # Migrate any allocated extents off the NVMe onto the remaining PV(s).
        print(f"   [{host}] Moving extents off {nvme} ...")
        _ssh(host, f"sudo pvmove {nvme}", check=False)

        # Shrink the VG — remove the NVMe PV while keeping VG/LV intact.
        print(f"   [{host}] Removing {nvme} from emulab VG ...")
        _ssh(host, f"sudo vgreduce emulab {nvme}", check=False)
        _ssh(host, f"sudo pvremove {nvme}", check=False)
        print(f"   [{host}] emulab VG shrunk, /mydata preserved")
    else:
        print(f"   [{host}] {nvme} not in emulab VG (already freed)")

    # Wipe the NVMe so OpenEBS Mayastor can claim it as a raw DiskPool.
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


# ── Phase 3: OpenEBS Mayastor ────────────────────────────────────────────────


def setup_openebs():
    """Node prep + Helm install of OpenEBS Mayastor + DiskPools + StorageClass."""
    print("   Running node prep (hugepages, nvme-tcp) ...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(WORKER_NODES)) as pool:
        futs = [pool.submit(run_node_prep, host) for host in WORKER_NODES]
        for f in futs:
            f.result()

    kube_nodes = [_get_kube_node_name(h) for h in WORKER_NODES]
    print(f"   K8s worker node names: {kube_nodes}")
    for kn in kube_nodes:
        _kubectl(["label", "nodes", kn, "openebs.io/engine=mayastor", "--overwrite"])

    for host in WORKER_NODES:
        _ssh(host, "sudo systemctl restart kubelet")

    print("   Waiting for hugepages to be reported by K8s ...")
    for kn in kube_nodes:
        _wait_for_hugepages(kn, timeout_seconds=300)

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

    # The diskpools CRD is registered by the openebs-operator-diskpool pod,
    # not the Helm chart directly.  kubectl wait fails immediately if the CRD
    # doesn't exist yet, so poll until it appears before waiting on Established.
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


# ── Phase 4: Deploy PostgreSQL + TPC-C on K8s ────────────────────────────────


def deploy_tpcc_k8s():
    """Apply K8s manifests for PostgreSQL + TPC-C and wait for rollout."""
    print("   Applying K8s manifests ...")
    for name, yaml in [
        ("postgres-pvc",        PG_PVC_YAML),
        ("postgres-statefulset", PG_STATEFULSET_YAML),
        ("postgres-svc",         PG_SVC_YAML),
        ("tpcc-deploy",          TPCC_DEPLOY_YAML),
        ("tpcc-svc",             TPCC_SVC_YAML),
    ]:
        print(f"     Applying {name} ...")
        _kubectl(["apply", "-f", "-"], manifest=yaml)

    print("   Waiting for PostgreSQL StatefulSet rollout ...")
    _kubectl(["rollout", "status", "statefulset/postgres", "--timeout=300s"])

    print("   Waiting for TPC-C Deployment rollout ...")
    _kubectl(["rollout", "status", "deployment/tpcc-app", "--timeout=300s"])

    print("   Waiting for TPC-C to be HTTP-ready ...")
    ok = wait_for_http(WORKER_NODES[0], TPCC_NODEPORT, "/health", timeout_sec=300)
    if not ok:
        print("ERROR: TPC-C not ready after 300s — check: kubectl get pods")
        sys.exit(1)


def init_tpcc_via_k8s(host, port, warehouses=NUM_WAREHOUSES, timeout=300):
    """Initialize TPC-C database via NodePort (no XDN header)."""
    import requests
    print(f"   Initializing TPC-C DB ({warehouses} warehouses) via {host}:{port} ...")
    deadline = time.time() + timeout
    last_err = None
    while time.time() < deadline:
        try:
            r = requests.post(
                f"http://{host}:{port}/init_db",
                headers={"Content-Type": "application/json"},
                json={"warehouses": warehouses},
                timeout=timeout,
            )
            if 200 <= r.status_code < 300:
                print(f"   DB initialized (HTTP {r.status_code}): {r.text[:200]}")
                return True
            last_err = f"HTTP {r.status_code}: {r.text[:200]}"
            print(f"   init_db returned {r.status_code}, retrying ...")
        except Exception as e:
            last_err = str(e)
            print(f"   init_db error: {e}, retrying ...")
        time.sleep(10)
    print(f"   ERROR: init_db failed after {timeout}s: {last_err}")
    return False


# ── Main ──────────────────────────────────────────────────────────────────────


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--skip-k8s", action="store_true",
                   help="Skip K8s cluster setup (assumes cluster already running)")
    p.add_argument("--skip-disk-prep", action="store_true",
                   help="Skip LVM disk prep; assumes disks already freed")
    p.add_argument("--skip-openebs", action="store_true",
                   help="Skip OpenEBS install (assumes StorageClass already present)")
    p.add_argument("--skip-deploy", action="store_true",
                   help="Skip K8s TPC-C deployment (assumes pods already running)")
    p.add_argument("--skip-init", action="store_true",
                   help="Skip TPC-C database initialization")
    p.add_argument("--skip-seed", action="store_true",
                   help="Skip generating payloads file (assume already exists)")
    p.add_argument("--rates", type=str, default=None,
                   help="Comma-separated list of offered rates (req/s) to override default")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()

    # Compute timestamped results directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    RESULTS_DIR = RESULTS_BASE / f"tpcc_openebs_{timestamp}"
    PAYLOADS_FILE = RESULTS_DIR / "neworder_payloads.txt"

    print("=" * 60)
    print("TPC-C OpenEBS 3-Replica Benchmark")
    print(f"  Results  -> {RESULTS_DIR}")
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

    # Phase 4 — Deploy PostgreSQL + TPC-C
    if args.skip_deploy:
        print("\n[Phase 4] Skipping TPC-C deployment (--skip-deploy)")
    else:
        print("\n[Phase 4] Deploying PostgreSQL + TPC-C on K8s ...")
        deploy_tpcc_k8s()

    tpcc_host = WORKER_NODES[0]
    tpcc_url = f"http://{tpcc_host}:{TPCC_NODEPORT}/orders"

    # Phase 5 — Initialize TPC-C database
    if args.skip_init:
        print("\n[Phase 5] Skipping TPC-C DB init (--skip-init)")
    else:
        print("\n[Phase 5] Initializing TPC-C database ...")
        if not init_tpcc_via_k8s(tpcc_host, TPCC_NODEPORT, NUM_WAREHOUSES):
            print("ERROR: TPC-C database initialization failed.")
            sys.exit(1)

    # Phase 6 — Generate payloads file
    if args.skip_seed:
        print(f"\n[Phase 6] Skipping payloads generation (--skip-seed)")
        print(f"   Assuming {PAYLOADS_FILE} already exists.")
    else:
        print(f"\n[Phase 6] Generating New Order payloads -> {PAYLOADS_FILE} ...")
        generate_payloads_file(PAYLOADS_FILE, NUM_WAREHOUSES)

    # Determine rate list
    if args.rates:
        rates = [int(r.strip()) for r in args.rates.split(",")]
    else:
        rates = BOTTLENECK_RATES

    # Phase 7 — Load sweep
    print(f"\n[Phase 7] Load sweep at {rates} req/s ...")
    print(f"   URL      : {tpcc_url}")
    print(f"   Duration : {LOAD_DURATION_SEC}s per rate (warmup {WARMUP_DURATION_SEC}s)")
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    results = []
    for i, rate in enumerate(rates):
        if i > 0 and INTER_RATE_PAUSE_SEC > 0:
            print(f"   Pausing {INTER_RATE_PAUSE_SEC}s between rate points ...")
            time.sleep(INTER_RATE_PAUSE_SEC)

        print(f"\n   -> rate={rate} req/s (warmup {WARMUP_DURATION_SEC}s) ...")
        wm = warmup_rate_point(tpcc_url, rate)
        if wm["throughput_rps"] < 0.1 * rate:
            print(
                f"   SKIP: warmup tput={wm['throughput_rps']:.2f} rps "
                f"< 10% of {rate} rps — system saturated"
            )
            continue
        print("   Settling 5s after warmup ...")
        time.sleep(5)

        print(f"   -> rate={rate} req/s (measuring {LOAD_DURATION_SEC}s) ...")
        m = run_load_point(tpcc_url, rate)
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

    # ── Create/update symlink ─────────────────────────────────────────
    symlink = RESULTS_BASE / "load_pb_tpcc_openebs"
    if symlink.is_symlink():
        symlink.unlink()
    elif symlink.is_dir():
        backup = RESULTS_BASE / f"tpcc_openebs_old_{timestamp}"
        symlink.rename(backup)
        print(f"   Renamed old directory -> {backup.name}")
    symlink.symlink_to(RESULTS_DIR.name)
    print(f"   Symlink: {symlink} -> {RESULTS_DIR.name}")

    print(f"\n[Done] Results in {RESULTS_DIR}/")
