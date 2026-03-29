import warnings as _w; _w.warn("DEPRECATED: Use run_load_pb_openebs_app.py --app wordpress instead.", DeprecationWarning, stacklevel=2)
"""
run_load_pb_wordpress_openebs.py — OpenEBS Mayastor 3-replica WordPress benchmark.

Sets up a K8s cluster on CloudLab with OpenEBS Mayastor block-level NVMe-oF
replication (3 replicas), deploys WordPress + MySQL, and runs the same
wp.newPost XML-RPC workload used by the XDN PB benchmark for direct comparison.

Run from the eval/ directory:
    python3 run_load_pb_wordpress_openebs.py [--skip-k8s] [--skip-disk-prep]

Outputs go to eval/results/openebs/:
    rate1.txt  rate5.txt  ...  — go-client output per rate point
"""

import argparse
import concurrent.futures
import os
import subprocess
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime
from pathlib import Path

import run_load_pb_wordpress_common as _wpmod
from run_load_pb_wordpress_common import (
    ADMIN_PASSWORD,
    ADMIN_USER,
    check_rest_api,
    create_post,
    install_wordpress,
    validate_posts,
    wait_for_port,
)
from run_load_pb_wordpress_reflex import (
    AVG_LATENCY_THRESHOLD_MS,
    BOTTLENECK_RATES,
    GO_LATENCY_CLIENT,
    INTER_RATE_PAUSE_SEC,
    LOAD_DURATION_SEC,
    SEED_POST_COUNT,
    WARMUP_DURATION_SEC,
    _parse_go_output,
    _xmlrpc_editpost_payload,
)
from init_openebs import (
    run_node_prep,
)

# NOTE: init_openebs.get_openebs_disk / run_disk_prep / install_openebs_mayastor
# are NOT used here because they assume nvme1n1 as the OpenEBS disk, but on
# this CloudLab config only nvme0n1 exists (it is the NVMe PV in the emulab VG).
# We implement custom disk detection + disk prep + DiskPool creation below.

# ── Constants ─────────────────────────────────────────────────────────────────

K8S_CONTROL_PLANE = "10.10.1.4"
WORKER_NODES      = ["10.10.1.1", "10.10.1.2", "10.10.1.3"]
WP_NODEPORT       = 30080
MYSQL_DB          = "wordpress"
MYSQL_ROOT_PASS   = "supersecret"

RESULTS_BASE = Path(__file__).resolve().parent / "results"
RESULTS_DIR  = None  # set dynamically in __main__ with timestamp
SCRIPT_DIR   = Path(__file__).resolve().parent
URLS_FILE    = None  # kept for backwards compat
PAYLOADS_FILE = None  # XML-RPC payloads file (set dynamically)

# ── K8s manifests ─────────────────────────────────────────────────────────────

MYSQL_PVC_YAML = """
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: mysql-pvc
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 20Gi
  storageClassName: openebs-3-replica
"""

def _mysql_statefulset_yaml(kube_node_name=None):
    """Generate MySQL StatefulSet YAML, optionally pinned to a specific K8s node."""
    node_selector = ""
    if kube_node_name:
        node_selector = f"""      nodeSelector:
        kubernetes.io/hostname: {kube_node_name}"""
    return f"""
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: mysql
spec:
  serviceName: mysql-svc
  replicas: 1
  selector:
    matchLabels:
      app: mysql
  template:
    metadata:
      labels:
        app: mysql
    spec:
{node_selector}
      containers:
        - name: mysql
          image: mysql:8.4.0
          env:
            - name: MYSQL_ROOT_PASSWORD
              value: "{MYSQL_ROOT_PASS}"
            - name: MYSQL_DATABASE
              value: "{MYSQL_DB}"
          ports:
            - containerPort: 3306
          volumeMounts:
            - name: mysql-data
              mountPath: /var/lib/mysql
      volumes:
        - name: mysql-data
          persistentVolumeClaim:
            claimName: mysql-pvc
"""

MYSQL_SVC_YAML = """
apiVersion: v1
kind: Service
metadata:
  name: mysql-svc
spec:
  selector:
    app: mysql
  ports:
    - port: 3306
      targetPort: 3306
  clusterIP: None
"""

WORDPRESS_DEPLOY_YAML = f"""
apiVersion: apps/v1
kind: Deployment
metadata:
  name: wordpress
spec:
  replicas: 1
  selector:
    matchLabels:
      app: wordpress
  template:
    metadata:
      labels:
        app: wordpress
    spec:
      affinity:
        podAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            - labelSelector:
                matchLabels:
                  app: mysql
              topologyKey: kubernetes.io/hostname
      containers:
        - name: wordpress
          image: wordpress:6.5.4-apache
          env:
            - name: WORDPRESS_DB_HOST
              value: "mysql-svc:3306"
            - name: WORDPRESS_DB_USER
              value: "root"
            - name: WORDPRESS_DB_PASSWORD
              value: "{MYSQL_ROOT_PASS}"
            - name: WORDPRESS_DB_NAME
              value: "{MYSQL_DB}"
            - name: WORDPRESS_CONFIG_EXTRA
              value: "define('DISABLE_WP_CRON', true); define('AUTOMATIC_UPDATER_DISABLED', true); define('WP_AUTO_UPDATE_CORE', false);"
          ports:
            - containerPort: 80
"""

WORDPRESS_SVC_YAML = f"""
apiVersion: v1
kind: Service
metadata:
  name: wordpress-svc
spec:
  type: NodePort
  selector:
    app: wordpress
  ports:
    - port: 80
      targetPort: 80
      nodePort: {WP_NODEPORT}
"""

# ── REST API auth (K8s) ────────────────────────────────────────────────────────

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
    """Tune MySQL InnoDB settings in the K8s MySQL pod for write-heavy workloads.

    Increases buffer pool to 1GB (from default 128MB) to avoid page eviction
    during sustained high write rates, and reduces background I/O to minimize
    interference with foreground queries.
    """
    print("   Tuning MySQL InnoDB settings ...")
    tune_sql = (
        "SET GLOBAL innodb_buffer_pool_size=1073741824; "       # 1 GB
        "SET GLOBAL innodb_change_buffer_max_size=50; "          # allow more change buffering
        "SET GLOBAL innodb_max_dirty_pages_pct=90; "             # delay page flush
        "SET GLOBAL innodb_max_dirty_pages_pct_lwm=80; "         # high-water mark before eager flush
        "SET GLOBAL innodb_io_capacity=200; "                    # limit background I/O
        "SET GLOBAL innodb_io_capacity_max=400; "                # cap burst I/O
        "SET GLOBAL innodb_lru_scan_depth=256; "                 # reduce per-second page cleaner work
    )
    cmd = f"kubectl exec statefulset/mysql -- mysql -p{MYSQL_ROOT_PASS} -e \"{tune_sql}\""
    result = subprocess.run(
        ["ssh", "-o", "StrictHostKeyChecking=no", K8S_CONTROL_PLANE, cmd],
        capture_output=True, text=True, timeout=30,
    )
    if result.returncode == 0:
        print("   MySQL InnoDB tuned successfully")
    else:
        print(f"   WARNING: MySQL tuning failed: {result.stderr.strip()}")
    # Give InnoDB a moment to resize the buffer pool
    time.sleep(2)


def enable_rest_auth_k8s():
    """Install mu-plugin + create WP Application Password in the K8s WordPress pod."""
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
    import re as _re

    def _is_valid_app_password(pw):
        if not pw or len(pw) < 10:
            return False
        if "<" in pw or "ERROR" in pw or "Fatal" in pw:
            return False
        return bool(_re.match(r'^[A-Za-z0-9 ]+$', pw))

    for attempt in range(1, 6):
        result = subprocess.run(
            ["ssh", "-o", "StrictHostKeyChecking=no", K8S_CONTROL_PLANE,
             "kubectl exec -i deploy/wordpress -- php"],
            input=php_script, text=True, capture_output=True,
        )
        app_password = result.stdout.strip()
        if result.returncode == 0 and _is_valid_app_password(app_password):
            _wpmod.ADMIN_APP_PASSWORD = app_password
            print(f"   WP Application Password created (first 5 chars: {app_password[:5]}...)")
            return True
        snippet = (app_password or result.stderr or "(empty)")[:150]
        print(f"   Attempt {attempt}/5: invalid response: {snippet}")
        if attempt < 5:
            print(f"   Retrying in 10s ...")
            time.sleep(10)
    print(f"   ERROR creating app password after 5 attempts")
    return False


# ── Load test helpers ──────────────────────────────────────────────────────────

def _run_load(url, rate, output_file, duration_sec):
    """Run get_latency_at_rate.go at `rate` req/s for `duration_sec` seconds."""
    output_file.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "go", "run", str(GO_LATENCY_CLIENT),
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


def run_load_point(url, rate, duration_sec=LOAD_DURATION_SEC):
    return _run_load(url, rate, RESULTS_DIR / f"rate{rate}.txt", duration_sec)


def warmup_rate_point(url, rate, warmup_duration_sec=WARMUP_DURATION_SEC):
    m = _run_load(url, rate, RESULTS_DIR / f"warmup_rate{rate}.txt", warmup_duration_sec)
    print(f"     [warmup] tput={m['throughput_rps']:.2f} rps  avg={m['avg_ms']:.1f}ms")
    return m


# ── Cluster helpers ────────────────────────────────────────────────────────────

def _ssh(host, cmd, check=True, capture=False):
    return subprocess.run(
        ["ssh", "-o", "StrictHostKeyChecking=no", host, cmd],
        check=check,
        capture_output=capture,
        text=True,
    )


def _ssh_with_retry(host, cmd, check=True, capture=False, retries=4, retry_delay=15):
    """SSH with automatic retry for transient connection failures (exit code 255).

    kubelet startup can briefly disrupt the node's network interface, causing
    SSH to return 255 (connection refused / no route to host). Retrying after a
    short delay recovers without aborting the whole setup.
    """
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
        # Pipe manifest via stdin. Strip any trailing -f - from args to
        # avoid duplicating it, then append exactly one -f - to read stdin.
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
    """Return the K8s node name for a worker identified by its 10.10.1.x IP.

    Queries K8s for nodes and matches by InternalIP → ExternalIP → Hostname.
    """
    result = _ssh(
        K8S_CONTROL_PLANE,
        "kubectl get nodes -o json",
        capture=True,
    )
    import json
    data = json.loads(result.stdout)
    # Build map from IP → node name
    for item in data["items"]:
        node_name = item["metadata"]["name"]
        for addr in item["status"].get("addresses", []):
            if addr["address"] == worker_host:
                return node_name
    # Fallback: resolve by external IP (each worker has both a 10.10.1.x and 130.127.x.x IP)
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


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Handler that treats redirects as successful responses (don't follow)."""
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def wait_for_http(host, port, path="/", timeout_sec=600, header=None):
    """Poll http://host:port/path until HTTP < 500 or timeout.

    Does NOT follow redirects — a 302 means the server is alive.
    """
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
            # HTTPError is raised for 3xx/4xx when not following redirects
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
             # Clean up stale CNI network interfaces so Flannel starts fresh.
             # Without this, old cni0/flannel.1 bridges from a previous cluster
             # cause 'cni0 already has an IP different from ...' sandbox failures.
             "sudo ip link delete cni0 2>/dev/null || true && "
             "sudo ip link delete flannel.1 2>/dev/null || true && "
             "sudo rm -rf /etc/cni/net.d/* /run/flannel",
             check=False)

    # kubeadm reset may leave iptables in a state that prevents Docker from
    # creating bridge networks (DOCKER-FORWARD chain missing). Restarting
    # Docker lets it recreate its own iptables chains cleanly.
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
    # admin.conf is root-owned; stage a world-readable copy in /tmp first
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
    # Fix server address to control plane IP if it says 127.0.0.1
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


# ── Phase 2: Disk prep ─────────────────────────────────────────────────────────

def detect_nvme_disk(host):
    """Return the NVMe data disk to use for OpenEBS (not the OS disk).

    Avoids LVM commands (pvs/vgs) because leftover Mayastor NVMe controllers
    can cause them to hang indefinitely.  Uses lsblk only.
    """
    # List all real NVMe disks, excluding Mayastor virtual controllers.
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

    # Verify
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
    """
    Node prep + Helm install of OpenEBS Mayastor + DiskPools + StorageClass.

    Implements the same logic as init_openebs.install_openebs_mayastor but
    uses detect_nvme_disk() to find the correct OpenEBS disk (nvme0n1 on this
    CloudLab config) instead of init_openebs.get_openebs_disk() which wrongly
    returns nvme1n1 when root is on sda.
    """
    # Node prep: hugepages + nvme-tcp module
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

    # Wait for hugepages on each K8s node
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
        # Use upgrade --install so a previously-failed release is re-attempted.
        # --set obs.callhome.enabled=false disables the minio/loki observability
        # stack whose post-install job tends to fail on clean clusters because
        # its PVCs are not yet provisioned when the hook fires.
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

    # Create one DiskPool per worker using the correct NVMe disk
    for ssh_target, kube_node_name in zip(WORKER_NODES, kube_nodes):
        nvme_disk = detect_nvme_disk(ssh_target)
        print(f"   Creating DiskPool on {kube_node_name} using {nvme_disk} ...")

        # Get stable /dev/disk/by-id path for OpenEBS
        disk_leaf = nvme_disk.split("/")[-1]
        id_result = _ssh(
            ssh_target,
            f"ls -l /dev/disk/by-id/ | grep nvme-eui | grep '{disk_leaf}' "
            f"| awk '{{print $9; exit}}'",
            capture=True,
        )
        disk_by_id = id_result.stdout.strip()
        if not disk_by_id:
            # Fallback: use raw device path
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


# ── Phase 4: Deploy WordPress + MySQL on K8s ──────────────────────────────────

def cleanup_wordpress_k8s():
    """Delete all WordPress + MySQL K8s resources. Safe to call even if nothing exists."""
    print("   Cleaning up WordPress K8s resources ...")
    for resource in [
        "deployment/wordpress",
        "service/wordpress-svc",
        "statefulset/mysql",
        "service/mysql-svc",
        "pvc/mysql-pvc",
    ]:
        try:
            _kubectl(["delete", resource, "--ignore-not-found", "--timeout=30s"])
        except Exception:
            pass
    # Wait for pods to terminate
    deadline = time.time() + 60
    while time.time() < deadline:
        result = _kubectl(
            ["get", "pods", "-l", "app in (wordpress,mysql)", "--no-headers"],
            check=False, capture=True,
        )
        if not result.stdout.strip():
            break
        time.sleep(3)
    print("   Cleanup complete.")


def deploy_wordpress_k8s():
    """Apply K8s manifests for MySQL + WordPress and wait for rollout."""
    # Pin MySQL to WORKER_NODES[0] so it colocates with Mayastor nexus
    kube_node = _get_kube_node_name(WORKER_NODES[0])
    print(f"   Pinning MySQL to node {kube_node} ({WORKER_NODES[0]})")
    print("   Applying MySQL manifests ...")
    for name, yaml in [
        ("mysql-pvc",         MYSQL_PVC_YAML),
        ("mysql-statefulset", _mysql_statefulset_yaml(kube_node)),
        ("mysql-svc",         MYSQL_SVC_YAML),
    ]:
        print(f"     Applying {name} ...")
        _kubectl(["apply", "-f", "-"], manifest=yaml)

    print("   Waiting for MySQL StatefulSet rollout ...")
    _kubectl(["rollout", "status", "statefulset/mysql", "--timeout=300s"])

    print("   Applying WordPress manifests ...")
    for name, yaml in [
        ("wordpress-deploy",  WORDPRESS_DEPLOY_YAML),
        ("wordpress-svc",     WORDPRESS_SVC_YAML),
    ]:
        print(f"     Applying {name} ...")
        _kubectl(["apply", "-f", "-"], manifest=yaml)

    print("   Waiting for WordPress Deployment rollout ...")
    _kubectl(["rollout", "status", "deployment/wordpress", "--timeout=300s"])

    print("   Waiting for WordPress to be HTTP-ready ...")
    ok = wait_for_http(WORKER_NODES[0], WP_NODEPORT, "/", timeout_sec=600)
    if not ok:
        print("ERROR: WordPress not ready — check: kubectl get pods")
        sys.exit(1)


# ── Main ──────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--skip-k8s",       action="store_true",
                   help="Skip K8s cluster setup (assumes cluster already running)")
    p.add_argument("--skip-disk-prep", action="store_true",
                   help="Skip LVM disk prep; assumes disks already freed")
    p.add_argument("--skip-openebs",   action="store_true",
                   help="Skip OpenEBS install (assumes StorageClass already present)")
    p.add_argument("--skip-deploy",    action="store_true",
                   help="Skip K8s WordPress deployment (assumes pods already running)")
    p.add_argument("--skip-install",   action="store_true",
                   help="Skip WordPress installation form submission (assume already installed)")
    p.add_argument("--skip-seed",      action="store_true",
                   help="Skip seeding warmup posts (assume already seeded)")
    p.add_argument("--rates", type=str, default=None,
                   help="Comma-separated list of offered rates (req/s) to override default")
    p.add_argument("--duration", type=int, default=LOAD_DURATION_SEC,
                   help=f"Measurement duration per rate point in seconds (default: {LOAD_DURATION_SEC})")
    p.add_argument("--warmup-duration", type=int, default=WARMUP_DURATION_SEC,
                   help=f"Warmup duration per rate point in seconds (default: {WARMUP_DURATION_SEC})")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()

    # Compute timestamped results directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    RESULTS_DIR = RESULTS_BASE / f"load_pb_wordpress_openebs_{timestamp}"
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    URLS_FILE = RESULTS_DIR / "editpost_urls.txt"
    PAYLOADS_FILE = RESULTS_DIR / "xmlrpc_payloads.txt"

    print("=" * 60)
    print("WordPress OpenEBS 3-Replica Benchmark")
    print("=" * 60)

    # Phase 0 — Restart kubelet on all nodes and verify readiness
    all_k8s_nodes = WORKER_NODES + [K8S_CONTROL_PLANE]
    print(f"\n[Phase 0] Restarting kubelet on all {len(all_k8s_nodes)} nodes ...")
    for host in all_k8s_nodes:
        result = subprocess.run(
            ["ssh", "-o", "StrictHostKeyChecking=no", host, "sudo systemctl restart kubelet"],
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
        result = subprocess.run(
            ["ssh", "-o", "StrictHostKeyChecking=no", K8S_CONTROL_PLANE,
             "kubectl get nodes -o wide"],
            capture_output=True, text=True, timeout=15,
        )
        print(result.stdout)
        sys.exit(1)

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

    # Determine rate list and durations
    if args.rates:
        rates = [int(r.strip()) for r in args.rates.split(",")]
    else:
        rates = BOTTLENECK_RATES
    load_duration = args.duration
    warmup_duration = args.warmup_duration

    wp_host = WORKER_NODES[0]
    wp_url  = f"http://{wp_host}:{WP_NODEPORT}/xmlrpc.php"

    print(f"  Rates       : {rates} req/s")
    print(f"  Duration    : {load_duration}s per rate (warmup {warmup_duration}s)")
    print(f"  Results     : {RESULTS_DIR}")

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    results = []

    try:
        # Phase 4 — Clean + Deploy WordPress + MySQL
        print("\n[Phase 4] Cleaning up previous K8s resources ...")
        cleanup_wordpress_k8s()

        if args.skip_deploy:
            print("\n[Phase 4] Skipping WordPress deployment (--skip-deploy)")
        else:
            print("\n[Phase 4] Deploying WordPress + MySQL on K8s ...")
            deploy_wordpress_k8s()

        # Phase 5 — Install WordPress
        if args.skip_install:
            print("\n[Phase 5] Skipping WordPress install (--skip-install)")
        else:
            print("\n[Phase 5] Installing WordPress ...")
            ok = install_wordpress(wp_host, WP_NODEPORT, "wordpress")
            if not ok:
                print("ERROR: WordPress installation failed.")
                sys.exit(1)

        # Phase 6 — Enable REST API auth + check availability
        print("\n[Phase 6] Enabling REST API auth ...")
        if not enable_rest_auth_k8s():
            print("ERROR: REST API auth setup failed.")
            sys.exit(1)
        print("\n[Phase 6b] Disabling WP-Cron and auto-updates ...")
        disable_wp_cron_k8s()
        if not check_rest_api(wp_host, WP_NODEPORT, "wordpress"):
            print("ERROR: REST API not available.")
            sys.exit(1)

        # Phase 6c — Tune MySQL InnoDB
        print("\n[Phase 6c] Tuning MySQL InnoDB settings ...")
        tune_mysql_k8s()

        # Phase 7 — Seed posts for editPost workload
        if args.skip_seed:
            print(f"\n[Phase 7] Skipping post seeding (--skip-seed)")
            print(f"   Assuming {PAYLOADS_FILE} already exists.")
        else:
            print(f"\n[Phase 7] Seeding {SEED_POST_COUNT} posts for editPost workload ...")
            seed_ids = []
            for i in range(SEED_POST_COUNT):
                title = f"Seed Post {i + 1}"
                content = f"Seed content {i + 1}."
                pid = None
                for attempt in range(1, 4):
                    try:
                        pid = create_post(wp_host, WP_NODEPORT, "wordpress",
                                          title, content)
                        if pid:
                            break
                    except Exception as e:
                        print(f"   WARNING: attempt {attempt} failed ({e}), retrying ...")
                        time.sleep(5)
                if pid:
                    seed_ids.append(pid)
                else:
                    print(f"   WARNING: seed post '{title}' failed, continuing")
            print(f"   Seeded {len(seed_ids)}/{SEED_POST_COUNT} posts")

            # Phase 7b — Write XML-RPC payloads file
            print(f"\n[Phase 7b] Writing {len(seed_ids)} XML-RPC payloads → {PAYLOADS_FILE} ...")
            RESULTS_DIR.mkdir(parents=True, exist_ok=True)
            with open(PAYLOADS_FILE, "w") as fh:
                for pid in seed_ids:
                    fh.write(_xmlrpc_editpost_payload(pid) + "\n")
            print(f"   Done: {len(seed_ids)} payloads written.")

        # Phase 8 — Load sweep
        print(f"\n[Phase 8] Load sweep at {rates} req/s ...")
        print(f"   URL      : {wp_url}")
        print(f"   Duration : {load_duration}s per rate (warmup {warmup_duration}s)")

        for i, rate in enumerate(rates):
            if i > 0 and INTER_RATE_PAUSE_SEC > 0:
                print(f"   Pausing {INTER_RATE_PAUSE_SEC}s between rate points ...")
                time.sleep(INTER_RATE_PAUSE_SEC)

            WARMUP_RATE = 1500
            print(f"\n   -> warmup at {WARMUP_RATE} req/s for {warmup_duration}s ...")
            wm = warmup_rate_point(wp_url, WARMUP_RATE, warmup_duration)
            if wm["throughput_rps"] < 0.1 * WARMUP_RATE:
                print(
                    f"   SKIP: warmup tput={wm['throughput_rps']:.2f} rps "
                    f"< 10% of {WARMUP_RATE} rps — system not ready"
                )
                continue
            print("   Settling 5s after warmup ...")
            time.sleep(5)

            print(f"   -> rate={rate} req/s (measuring {load_duration}s) ...")
            m = run_load_point(wp_url, rate, load_duration)
            row = {"rate_rps": rate, **m}
            results.append(row)
            print(
                f"     tput={m['throughput_rps']:.2f} rps  "
                f"avg={m['avg_ms']:.1f}ms  "
                f"p50={m['p50_ms']:.1f}ms  "
                f"p95={m['p95_ms']:.1f}ms"
            )
            if m["avg_ms"] > AVG_LATENCY_THRESHOLD_MS:
                print(f"   Avg latency {m['avg_ms']:.0f}ms > {AVG_LATENCY_THRESHOLD_MS}ms — stopping.")
                break
            if m.get("actual_achieved_rps", 0) > 0 and m["throughput_rps"] < 0.8 * m["actual_achieved_rps"]:
                print(f"   Throughput < 80% of offered — stopping.")
                break

    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Always clean up K8s resources
        print("\n[Cleanup] Removing WordPress + MySQL K8s resources ...")
        cleanup_wordpress_k8s()

    # Write CSV
    import csv
    csv_path = RESULTS_DIR / "openebs_load_results.csv"
    fieldnames = ["rate_rps", "achieved_rps", "throughput_rps", "avg_ms", "p50_ms", "p90_ms", "p95_ms", "p99_ms"]
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

    print(f"\n[Done] Results in {RESULTS_DIR}/")
