#!/usr/bin/env python3
"""
Statediff Capture Baseline Comparison Microbenchmark

Compares statediff capture latency and artifact size across capture mechanisms:
  - fuselog : FUSE-based filesystem logging (XDN's approach)
  - rsync   : rsync --write-batch between live and backup copy
  - btrfs   : btrfs send -p between snapshots

Uses a synthetic filewriter app that writes a fixed-size random payload at a
random offset into a pre-allocated state file (default: 50MB state, 8-byte writes).
This matches the experiment described in the paper's statediff evaluation.

Output CSVs:
  - eval_statediff_capture_baselines.csv      (per-request raw data)
  - eval_statediff_capture_baselines_summary.csv (aggregated per baseline)

Typical usage:
    python3 eval/run_microbench_sdcap_baselines.py

    python3 eval/run_microbench_sdcap_baselines.py \\
        --baselines fuselog btrfs rsync \\
        --state-mb 50 --write-size 8 --requests 100
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import shlex
import shutil
import socket
import struct
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path
from statistics import mean, median

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths and constants
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parent if SCRIPT_DIR.name == "eval" else SCRIPT_DIR
RESULTS_BASE = (ROOT_DIR / "eval" / "results") if (ROOT_DIR / "eval").exists() else (SCRIPT_DIR / "results")

WORKDIR = Path("/tmp/xdn-statediff")
FUSELOG_SHM_WORKDIR = Path("/dev/shm/xdn-statediff")

FILEWRITER_IMAGE = "xdn-filewriter-bench"
FILEWRITER_PORT = 18081
CONTAINER_NAME = "sdcap-baseline"

DEFAULT_BASELINES = ["fuselog", "btrfs", "rsync", "kvm-qcow", "criu", "criu-inc"]
DEFAULT_STATE_MB = 50
DEFAULT_WRITE_SIZE = 8
DEFAULT_REQUESTS = 100

DEFAULT_REMOTE_HOST = "10.10.1.4"
DEFAULT_REMOTE_DIR = "~/xdn-sdcap-baselines"
DEFAULT_BACKUP_HOSTS = ["10.10.1.1", "10.10.1.2"]

# KVM-QCOW constants
KVM_VM_NAME = "sdcap-kvm-vm"
KVM_DISK_DIR = Path("/tmp/xdn-kvm-workdir")
KVM_VM_IMAGE = KVM_DISK_DIR / f"{KVM_VM_NAME}.qcow2"
KVM_STATE_DISK = KVM_DISK_DIR / f"{KVM_VM_NAME}-state.qcow2"
KVM_SNAP_DIR = KVM_DISK_DIR / "snapshots"


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def configure_logging(log_path: Path | None, verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(level)

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(level)
    console.setFormatter(formatter)
    root.addHandler(console)

    if log_path is not None:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(log_path)
        fh.setLevel(level)
        fh.setFormatter(formatter)
        root.addHandler(fh)


# ---------------------------------------------------------------------------
# Shell helpers
# ---------------------------------------------------------------------------

def run(cmd: str, check: bool = True, capture: bool = False) -> subprocess.CompletedProcess:
    log.debug("$ %s", cmd)
    return subprocess.run(cmd, shell=True, check=check, text=True, capture_output=capture)


def sudo(cmd: str, check: bool = True) -> subprocess.CompletedProcess:
    return run(f"sudo -n bash -lc {json.dumps(cmd)}", check=check)


def ssh(host: str, cmd: str, check: bool = True) -> subprocess.CompletedProcess:
    full = ["ssh", "-o", "StrictHostKeyChecking=no", host, f"bash -lc {shlex.quote(cmd)}"]
    log.debug("[ssh %s] %s", host, cmd)
    return subprocess.run(full, check=check, text=True)


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def reset_dir(path: Path) -> None:
    shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True, exist_ok=True)


def find_existing(paths: list[Path]) -> Path:
    for path in paths:
        if path.exists():
            return path
    raise FileNotFoundError(f"none of these paths exist: {paths}")


def docker_rm(name: str) -> None:
    run(f"docker rm -f {name} >/dev/null 2>&1 || true", check=False)


def bytes_of(path: Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        return path.stat().st_size
    total = 0
    for root_dir, _, files in os.walk(path):
        for filename in files:
            total += (Path(root_dir) / filename).stat().st_size
    return total


def measure_coordination(data: bytes, backup_hosts: list[str]) -> float:
    """Broadcast statediff bytes to backup hosts over TCP and measure total time.

    Opens a TCP connection to each backup host on a fixed port, sends the data,
    and waits for all to complete. Returns elapsed time in ms.
    """
    if not backup_hosts or len(data) == 0:
        return 0.0
    coord_port = 19999
    start = time.perf_counter()
    socks = []
    for host in backup_hosts:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5)
        try:
            s.connect((host, coord_port))
            s.sendall(data)
            socks.append(s)
        except (ConnectionRefusedError, OSError) as exc:
            log.debug("Coordination to %s:%d failed: %s", host, coord_port, exc)
            s.close()
    for s in socks:
        s.close()
    return (time.perf_counter() - start) * 1000.0


COORD_PORT = 19876


def start_coordination_receivers(backup_hosts: list[str]) -> None:
    """Start TCP receivers on backup hosts (via SSH, once at setup time)."""
    for host in backup_hosts:
        # Kill any existing receiver, then start a new one that loops forever
        # The receiver accepts a connection, reads all data, closes, and loops
        subprocess.Popen(
            ["ssh", "-o", "StrictHostKeyChecking=no", host,
             f"pkill -f 'nc -lk {COORD_PORT}' 2>/dev/null; "
             f"nohup bash -c 'while true; do nc -l {COORD_PORT} > /dev/null; done' &"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, stdin=subprocess.DEVNULL
        )
    time.sleep(0.5)  # let receivers start


def stop_coordination_receivers(backup_hosts: list[str]) -> None:
    """Stop TCP receivers on backup hosts."""
    for host in backup_hosts:
        run(f"ssh -o StrictHostKeyChecking=no {host} 'pkill -f \"nc -l {COORD_PORT}\" 2>/dev/null || true'",
            check=False)


def measure_coordination(data: bytes, backup_hosts: list[str]) -> float:
    """Broadcast statediff bytes to backup hosts over raw TCP.

    Sends data to each backup host in parallel via pre-started TCP receivers.
    Returns elapsed time in ms (measures only network transfer, no SSH overhead).
    """
    if not backup_hosts or len(data) == 0:
        return 0.0
    start = time.perf_counter()
    socks = []
    for host in backup_hosts:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(5)
            s.connect((host, COORD_PORT))
            s.sendall(data)
            socks.append(s)
        except (ConnectionRefusedError, OSError) as exc:
            log.debug("Coordination to %s:%d failed: %s", host, COORD_PORT, exc)
            s.close()
    for s in socks:
        s.shutdown(socket.SHUT_WR)
        s.close()
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    return elapsed_ms


def measure_coordination_via_netcat(artifact_bytes: int, backup_hosts: list[str]) -> float:
    """Measure coordination by sending artifact_bytes over raw TCP to backup hosts."""
    if not backup_hosts or artifact_bytes <= 0:
        return 0.0
    data = os.urandom(artifact_bytes)
    return measure_coordination(data, backup_hosts)


def http_post(url: str, payload: bytes, content_type: str) -> float:
    req = urllib.request.Request(url, data=payload, method="POST")
    req.add_header("Content-Type", content_type)
    start = time.perf_counter()
    with urllib.request.urlopen(req, timeout=30) as resp:
        resp.read()
    return (time.perf_counter() - start) * 1000.0


def wait_http(url: str, timeout_sec: int = 120) -> None:
    deadline = time.time() + timeout_sec
    last_err = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                if resp.status < 500:
                    return
        except urllib.error.HTTPError as exc:
            if exc.code < 500:
                return
            last_err = exc
        except Exception as exc:
            last_err = exc
        time.sleep(1)
    raise RuntimeError(f"timeout waiting for {url}: {last_err}")


def create_state_file(path: Path, size_mb: int) -> None:
    ensure_dir(path.parent)
    log.info("  Creating %d MB state file at %s ...", size_mb, path)
    run(f"dd if=/dev/urandom of={path} bs=1M count={size_mb} status=none")


def parse_int_list(value: str) -> list[int]:
    return [int(x.strip()) for x in value.split(",") if x.strip()]


# ---------------------------------------------------------------------------
# Fuselog collector
# ---------------------------------------------------------------------------

class FuselogCollector:
    def __init__(self, socket_path: Path):
        self.socket_path = socket_path
        self.conn: socket.socket | None = None

    def connect(self) -> None:
        self.conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.conn.connect(str(self.socket_path))

    def close(self) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def drain(self) -> dict:
        if self.conn is None:
            raise RuntimeError("collector not connected")
        start = time.perf_counter()
        self.conn.sendall(b"g")
        size_buf = self._read_exact(8)
        payload_size = struct.unpack("<Q", size_buf)[0]
        payload = self._read_exact(payload_size)
        elapsed_ms = (time.perf_counter() - start) * 1000.0

        num_actions = -1
        if len(payload) >= 16:
            try:
                pos = 0
                num_file = struct.unpack_from("<Q", payload, pos)[0]
                pos += 8
                for _ in range(num_file):
                    _, path_len = struct.unpack_from("<QQ", payload, pos)
                    pos += 16 + path_len
                if pos + 8 <= len(payload):
                    num_actions = struct.unpack_from("<Q", payload, pos)[0]
            except (struct.error, OverflowError, ValueError):
                num_actions = -1

        return {
            "capture_ms": elapsed_ms,
            "artifact_bytes": payload_size + 8,
            "num_actions": num_actions,
        }

    def _read_exact(self, n: int) -> bytes:
        chunks = []
        got = 0
        while got < n:
            chunk = self.conn.recv(n - got)
            if not chunk:
                raise RuntimeError("socket closed while reading fuselog payload")
            chunks.append(chunk)
            got += len(chunk)
        return b"".join(chunks)


# ---------------------------------------------------------------------------
# Baseline harnesses
# ---------------------------------------------------------------------------

class BaselineHarness:
    def __init__(self, baseline: str, root: Path):
        self.baseline = baseline
        self.root = root
        self.live_dir = root / baseline / "live"
        self.meta_dir = root / baseline / "meta"
        ensure_dir(self.live_dir)
        ensure_dir(self.meta_dir)

    def start(self) -> None:
        raise NotImplementedError

    def stop(self) -> None:
        raise NotImplementedError

    def initialize_after_seed(self) -> None:
        raise NotImplementedError

    def capture(self, iteration: int | str) -> dict:
        raise NotImplementedError

    def mount_path(self) -> Path:
        return self.live_dir


class FuselogHarness(BaselineHarness):
    def __init__(self, root: Path):
        super().__init__("fuselog", root)
        self.socket_path = self.meta_dir / "fuselog.sock"
        self.proc: subprocess.Popen | None = None
        self.collector: FuselogCollector | None = None
        self.fuselog_bin = find_existing([
            Path("/usr/local/bin/fuselog"),
            Path(__file__).resolve().parent.parent / "xdn-fs" / "cpp" / "fuselog",
            Path.cwd() / "xdn-fs" / "cpp" / "fuselog",
            Path.cwd() / "cpp" / "fuselog",
        ])

    def start(self) -> None:
        ensure_dir(self.live_dir)
        reset_dir(self.meta_dir)
        env = {
            "FUSELOG_SOCKET_FILE": str(self.socket_path),
            "FUSELOG_DAEMON_LOGS": "1",
            "RUST_LOG": "info",
        }
        cmd = ["sudo", "-n", "env"]
        for key, value in env.items():
            cmd.append(f"{key}={value}")
        cmd.extend([str(self.fuselog_bin), "-o", "allow_other", str(self.live_dir)])
        self.proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = time.time() + 15
        while time.time() < deadline:
            if self.socket_path.exists():
                break
            if self.proc.poll() is not None:
                time.sleep(0.2)
                if self.socket_path.exists():
                    break
                raise RuntimeError(f"fuselog exited (code={self.proc.returncode})")
            time.sleep(0.1)
        else:
            raise RuntimeError(f"timed out waiting for fuselog socket {self.socket_path}")
        sudo(f"chmod 777 {self.socket_path}", check=False)
        self.collector = FuselogCollector(self.socket_path)
        self.collector.connect()

    def stop(self) -> None:
        if self.collector is not None:
            self.collector.close()
            self.collector = None
        sudo(f"fusermount -u {self.live_dir} >/dev/null 2>&1 || true", check=False)
        sudo(f"umount -l {self.live_dir} >/dev/null 2>&1 || true", check=False)
        if self.proc is not None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
            self.proc = None

    def initialize_after_seed(self) -> None:
        if self.collector is None:
            raise RuntimeError("collector not connected")
        self.collector.drain()

    def capture(self, iteration: int | str) -> dict:
        if self.collector is None:
            raise RuntimeError("collector not connected")
        return self.collector.drain()


class RsyncHarness(BaselineHarness):
    def __init__(self, root: Path):
        super().__init__("rsync", root)
        self.backup_dir = self.meta_dir / "backup"

    def start(self) -> None:
        reset_dir(self.live_dir)
        reset_dir(self.meta_dir)
        reset_dir(self.backup_dir)

    def stop(self) -> None:
        return

    def initialize_after_seed(self) -> None:
        reset_dir(self.backup_dir)
        run(f"rsync -a --delete {self.live_dir}/ {self.backup_dir}/")

    def capture(self, iteration: int | str) -> dict:
        batch_prefix = self.meta_dir / f"batch_{iteration}"
        for suffix in ("", ".sh"):
            try:
                Path(str(batch_prefix) + suffix).unlink()
            except FileNotFoundError:
                pass
        start = time.perf_counter()
        run(f"rsync -a --delete --write-batch={batch_prefix} "
            f"{self.live_dir}/ {self.backup_dir}/ >/dev/null")
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        artifact_bytes = bytes_of(batch_prefix) + bytes_of(Path(str(batch_prefix) + ".sh"))
        return {
            "capture_ms": elapsed_ms,
            "artifact_bytes": artifact_bytes,
            "num_actions": -1,
        }


class BtrfsHarness(BaselineHarness):
    def __init__(self, root: Path):
        super().__init__("btrfs", root)
        self.img = self.meta_dir / "btrfs.img"
        self.mount_dir = self.meta_dir / "mnt"
        self.snapshots_dir = self.mount_dir / ".snapshots"
        self.prev_snap = self.snapshots_dir / "prev_snap"

    def mount_path(self) -> Path:
        return self.mount_dir / "live"

    def start(self) -> None:
        reset_dir(self.meta_dir)
        ensure_dir(self.mount_dir)
        sudo(f"truncate -s 4G {self.img}")
        sudo(f"mkfs.btrfs -f {self.img} >/dev/null 2>&1")
        sudo(f"mount -o loop {self.img} {self.mount_dir}")
        sudo(f"btrfs -q subvolume create {self.mount_dir / 'live'}")
        sudo(f"mkdir -p {self.snapshots_dir}")
        run(f"sudo -n chown -R $(id -u):$(id -g) {self.mount_dir}", check=False)

    def stop(self) -> None:
        sudo(f"umount {self.mount_dir} >/dev/null 2>&1 || true", check=False)

    def initialize_after_seed(self) -> None:
        sudo(f"btrfs -q subvolume delete {self.prev_snap} >/dev/null 2>&1 || true", check=False)
        sudo(f"btrfs -q subvolume snapshot -r {self.mount_dir / 'live'} {self.prev_snap}")

    def capture(self, iteration: int | str) -> dict:
        curr_snap = self.snapshots_dir / f"curr_snap_{iteration}"
        diff_path = self.meta_dir / f"diff_{iteration}.bin"
        sudo(f"btrfs -q subvolume delete {curr_snap} >/dev/null 2>&1 || true", check=False)
        run(f"rm -f {diff_path}", check=False)
        start = time.perf_counter()
        sudo(
            f"btrfs -q subvolume snapshot -r {self.mount_dir / 'live'} {curr_snap} && "
            f"btrfs -q send -p {self.prev_snap} -f {diff_path} {curr_snap}"
        )
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        artifact_bytes = bytes_of(diff_path)
        sudo(f"btrfs -q subvolume delete {self.prev_snap} >/dev/null 2>&1 || true", check=False)
        self.prev_snap = curr_snap
        return {
            "capture_ms": elapsed_ms,
            "artifact_bytes": artifact_bytes,
            "num_actions": -1,
        }


class KvmQcowHarness(BaselineHarness):
    """KVM/QEMU with qcow2 incremental disk snapshots.

    Setup based on eval/kvm-approach/:
    1. Build a Debian 12 VM image with virt-builder
    2. Launch VM with virt-install --import
    3. Attach a separate qcow2 state disk (vdb), format + mount inside VM
    4. Copy the filewriter binary into the VM, run it
    5. For each capture: virsh snapshot-create-as --disk-only on vdb
       The new snapshot file IS the statediff artifact.
    """

    def __init__(self, root: Path, state_mb: int = 50):
        super().__init__("kvm-qcow", root)
        self.state_mb = state_mb
        self.vm_ip: str | None = None
        self.snap_idx = 0
        ensure_dir(KVM_DISK_DIR)
        ensure_dir(KVM_SNAP_DIR)
        self.filewriter_bin = find_existing([
            Path(__file__).resolve().parent.parent / "services" / "filewriter",
            Path.cwd() / "services" / "filewriter",
            Path.cwd() / "filewriter",
        ])

    def start(self) -> None:
        log.info("  [KVM] Cleaning up previous VM ...")
        sudo(f"virsh destroy {KVM_VM_NAME} 2>/dev/null || true", check=False)
        sudo(f"virsh undefine {KVM_VM_NAME} --remove-all-storage --snapshots-metadata 2>/dev/null || true", check=False)
        sudo(f"rm -rf {KVM_SNAP_DIR}/*", check=False)

        if not KVM_VM_IMAGE.exists():
            log.info("  [KVM] Building Debian 12 VM image with virt-builder ...")
            sudo(
                f"virt-builder debian-12 "
                f"-o {KVM_VM_IMAGE} "
                f"--format qcow2 "
                f"--size 6G "
                f"--hostname vm.sdcap.xdn "
                f"--root-password password:12345678 "
                f"--edit \"/etc/ssh/sshd_config: s/#PermitRootLogin.*/PermitRootLogin yes/\" "
                f"--run-command 'apt-get install -y qemu-guest-agent screen openssh-server' "
                f"--run-command 'systemctl enable qemu-guest-agent ssh' "
                f"--run-command 'printf \"auto enp1s0\\niface enp1s0 inet dhcp\\n\" > /etc/network/interfaces.d/enp1s0' "
                f"--run-command 'printf \"auto ens2\\niface ens2 inet dhcp\\n\" > /etc/network/interfaces.d/ens2' "
                f"--run-command 'mkdir -p /root/.ssh; ssh-keygen -A'"
            )
        else:
            log.info("  [KVM] Reusing cached VM image %s", KVM_VM_IMAGE)
        sudo(f"cp {KVM_VM_IMAGE} {KVM_VM_IMAGE}.run")

        log.info("  [KVM] Creating %d MB state disk ...", max(self.state_mb * 2, 256))
        sudo(f"qemu-img create -f qcow2 {KVM_STATE_DISK} {max(self.state_mb * 2, 256)}M")

        # Disable AppArmor security driver in libvirt — aa-teardown alone
        # is insufficient because libvirtd still queries AppArmor policy.
        sudo(
            "grep -q 'security_driver = .none.' /etc/libvirt/qemu.conf 2>/dev/null || "
            "{ echo 'security_driver = \"none\"' >> /etc/libvirt/qemu.conf && "
            "systemctl restart libvirtd && sleep 2; }",
            check=False
        )

        log.info("  [KVM] Launching VM ...")
        sudo(
            f"virt-install "
            f"--import "
            f"--name {KVM_VM_NAME} "
            f"--memory 1024 "
            f"--vcpus 1 "
            f"--os-variant=debian12 "
            f"--disk path={KVM_VM_IMAGE}.run,bus=virtio,format=qcow2 "
            f"--network network=default,model=virtio "
            f"--graphics none "
            f"--noautoconsole "
            f"--noreboot"
        )
        sudo(f"virsh start {KVM_VM_NAME}", check=False)

        log.info("  [KVM] Waiting for VM IP ...")
        # First wait 20s for DHCP, then try guest-agent dhclient as fallback
        for attempt in range(2):
            deadline = time.time() + (60 if attempt == 0 else 30)
            while time.time() < deadline:
                r2 = run(f"sudo -n virsh domifaddr {KVM_VM_NAME}", check=False, capture=True)
                if r2.stdout:
                    m = re.search(r"(\d+\.\d+\.\d+\.\d+)", r2.stdout)
                    if m:
                        self.vm_ip = m.group(1)
                        break
                time.sleep(2)
            if self.vm_ip:
                break
            if attempt == 0:
                log.info("  [KVM] No IP after 60s, trying dhclient via guest-agent ...")
                sudo(
                    f'virsh qemu-agent-command {KVM_VM_NAME} '
                    f'\'{{"execute":"guest-exec","arguments":{{"path":"/sbin/dhclient","arg":["enp1s0"]}}}}\' '
                    f'2>/dev/null || true',
                    check=False
                )
                sudo(
                    f'virsh qemu-agent-command {KVM_VM_NAME} '
                    f'\'{{"execute":"guest-exec","arguments":{{"path":"/sbin/dhclient","arg":["ens2"]}}}}\' '
                    f'2>/dev/null || true',
                    check=False
                )
        if self.vm_ip is None:
            raise RuntimeError("Failed to get VM IP address")
        log.info("  [KVM] VM IP: %s", self.vm_ip)

        log.info("  [KVM] Waiting for SSH ...")
        deadline = time.time() + 120
        while time.time() < deadline:
            r = run(f"sshpass -p 12345678 ssh -o StrictHostKeyChecking=no -o ConnectTimeout=3 root@{self.vm_ip} 'echo ok'",
                    check=False, capture=True)
            if r.returncode == 0:
                break
            time.sleep(2)
        else:
            raise RuntimeError(f"SSH to VM {self.vm_ip} timed out")

        log.info("  [KVM] Attaching state disk ...")
        sudo(
            f"virsh attach-disk {KVM_VM_NAME} "
            f"--source {KVM_STATE_DISK} "
            f"--target vdb "
            f"--subdriver qcow2 "
            f"--targetbus virtio "
            f"--live --persistent"
        )

        log.info("  [KVM] Formatting and mounting state disk inside VM ...")
        self._vm_cmd("echo -e 'n\\np\\n1\\n\\n\\nw' | fdisk /dev/vdb || true")
        self._vm_cmd("mkfs.ext4 -F /dev/vdb1")
        self._vm_cmd("mkdir -p /home/data && mount /dev/vdb1 /home/data")

        log.info("  [KVM] Building and copying filewriter binary ...")
        run(f"cd {self.filewriter_bin} && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /tmp/sdcap-writer main.go")
        run(f"sshpass -p 12345678 scp -o StrictHostKeyChecking=no /tmp/sdcap-writer root@{self.vm_ip}:/home/writer")

        log.info("  [KVM] Starting filewriter service in VM ...")
        # Use screen to detach the process from SSH session
        self._vm_cmd("apt-get install -y screen >/dev/null 2>&1 || true")
        self._vm_cmd(
            f"screen -dmS writer bash -c "
            f"'cd /home && STATEDIR=/home/data INITSIZE={self.state_mb} ./writer > /tmp/writer.log 2>&1'"
        )
        deadline = time.time() + 120
        while time.time() < deadline:
            try:
                req = urllib.request.Request(f"http://{self.vm_ip}:8000/", data=b"size=8", method="POST")
                req.add_header("Content-Type", "application/x-www-form-urlencoded")
                with urllib.request.urlopen(req, timeout=3) as resp:
                    if resp.status < 500:
                        break
            except Exception:
                pass
            time.sleep(2)
        else:
            # Log VM status for debugging
            self._vm_cmd("cat /tmp/writer.log 2>/dev/null; ps aux | grep writer")
            raise RuntimeError("Filewriter service in VM did not start within 120s")
        log.info("  [KVM] Filewriter service ready at http://%s:8000", self.vm_ip)

    def stop(self) -> None:
        sudo(f"virsh destroy {KVM_VM_NAME} 2>/dev/null || true", check=False)
        sudo(f"virsh undefine {KVM_VM_NAME} --remove-all-storage --snapshots-metadata 2>/dev/null || true", check=False)
        sudo(f"rm -rf {KVM_SNAP_DIR}/*", check=False)
        sudo(f"rm -f {KVM_VM_IMAGE}.run {KVM_STATE_DISK}", check=False)

    def initialize_after_seed(self) -> None:
        self.snap_idx = 0
        snap_file = KVM_SNAP_DIR / f"snap-{self.snap_idx}"
        sudo(
            f"virsh snapshot-create-as {KVM_VM_NAME} "
            f"--name {KVM_VM_NAME}-snap-{self.snap_idx} "
            f"--disk-only "
            f"--diskspec vdb,snapshot=external,file={snap_file} "
            f"--diskspec vda,snapshot=no "
            f"--atomic"
        )

    # QEMU limits backing chains to 200 layers. Flatten every 150 snapshots.
    CHAIN_FLATTEN_INTERVAL = 150

    def _flatten_chain(self) -> None:
        """Commit all snapshot layers back into the base state disk and reset."""
        log.info("  [KVM] Flattening backing chain at snap_idx=%d ...", self.snap_idx)
        # blockcommit without --shallow merges the ENTIRE chain into the base.
        # --active --pivot switches the live disk back to the base after commit.
        # --delete removes the overlay files after committing.
        sudo(
            f"virsh blockcommit {KVM_VM_NAME} vdb --active --pivot --delete --wait "
            f"2>/dev/null || true",
            check=False
        )
        # Delete all snapshot metadata
        for i in range(self.snap_idx + 1):
            sudo(f"virsh snapshot-delete {KVM_VM_NAME} {KVM_VM_NAME}-snap-{i} --metadata 2>/dev/null || true",
                 check=False)
        # Clean up any leftover snapshot files
        sudo(f"rm -f {KVM_SNAP_DIR}/snap-*", check=False)
        # Create a fresh base snapshot
        self.snap_idx = 0
        snap_file = KVM_SNAP_DIR / f"snap-{self.snap_idx}"
        sudo(
            f"virsh snapshot-create-as {KVM_VM_NAME} "
            f"--name {KVM_VM_NAME}-snap-{self.snap_idx} "
            f"--disk-only "
            f"--diskspec vdb,snapshot=external,file={snap_file} "
            f"--diskspec vda,snapshot=no "
            f"--atomic"
        )
        log.info("  [KVM] Chain flattened, reset to snap_idx=0")

    def capture(self, iteration: int | str) -> dict:
        # Flatten backing chain before hitting the 200-layer QEMU limit
        if self.snap_idx > 0 and self.snap_idx % self.CHAIN_FLATTEN_INTERVAL == 0:
            self._flatten_chain()

        self.snap_idx += 1
        snap_file = KVM_SNAP_DIR / f"snap-{self.snap_idx}"
        start = time.perf_counter()
        sudo(
            f"virsh snapshot-create-as {KVM_VM_NAME} "
            f"--name {KVM_VM_NAME}-snap-{self.snap_idx} "
            f"--disk-only "
            f"--diskspec vdb,snapshot=external,file={snap_file} "
            f"--diskspec vda,snapshot=no "
            f"--atomic"
        )
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        prev_snap_file = KVM_SNAP_DIR / f"snap-{self.snap_idx - 1}"
        artifact_bytes = bytes_of(prev_snap_file)
        return {
            "capture_ms": elapsed_ms,
            "artifact_bytes": artifact_bytes,
            "num_actions": -1,
        }

    def mount_path(self) -> Path:
        return self.live_dir

    def get_service_url(self) -> str:
        return f"http://{self.vm_ip}:8000/"

    def _vm_cmd(self, cmd: str) -> subprocess.CompletedProcess:
        return run(
            f"sshpass -p 12345678 ssh -o StrictHostKeyChecking=no root@{self.vm_ip} {shlex.quote(cmd)}",
            check=False, capture=True
        )


class CriuHarness(BaselineHarness):
    """CRIU checkpoint/restore baseline.

    Uses `docker checkpoint create` to capture container state (process memory +
    filesystem diff). The checkpoint directory IS the statediff artifact.

    Requires: Docker with CRIU support (--experimental), criu installed,
    container started with --security-opt seccomp=unconfined.
    """

    def __init__(self, root: Path, state_mb: int = 50):
        super().__init__("criu", root)
        self.state_mb = state_mb
        self.checkpoint_dir = self.meta_dir / "checkpoints"
        self.snap_idx = 0
        ensure_dir(self.checkpoint_dir)

    def start(self) -> None:
        reset_dir(self.live_dir)
        reset_dir(self.meta_dir)
        ensure_dir(self.checkpoint_dir)

    def stop(self) -> None:
        docker_rm(CONTAINER_NAME)
        sudo(f"rm -rf {self.checkpoint_dir}/*", check=False)

    def initialize_after_seed(self) -> None:
        # Create initial checkpoint to establish baseline, then restore
        self.snap_idx = 0
        init_dir = self.checkpoint_dir / "init"
        sudo(f"rm -rf {init_dir}", check=False)
        run(f"docker checkpoint create --leave-running {CONTAINER_NAME} init "
            f"--checkpoint-dir {self.checkpoint_dir} >/dev/null")

    def capture(self, iteration: int | str) -> dict:
        self.snap_idx += 1
        cp_name = f"cp_{self.snap_idx}"
        cp_dir = self.checkpoint_dir / cp_name
        sudo(f"rm -rf {cp_dir}", check=False)
        start = time.perf_counter()
        run(f"docker checkpoint create --leave-running {CONTAINER_NAME} {cp_name} "
            f"--checkpoint-dir {self.checkpoint_dir} >/dev/null")
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        # Checkpoint dir is owned by root, use sudo du to get size
        r = run(f"sudo -n du -sb {cp_dir}", check=False, capture=True)
        artifact_bytes = int(r.stdout.split()[0]) if r.returncode == 0 and r.stdout.strip() else 0
        # Clean up to avoid disk bloat
        sudo(f"rm -rf {cp_dir}", check=False)
        return {
            "capture_ms": elapsed_ms,
            "artifact_bytes": artifact_bytes,
            "num_actions": -1,
        }


class CriuIncrementalHarness(BaselineHarness):
    """CRIU incremental checkpoint baseline.

    Uses CRIU's soft-dirty page tracking for incremental checkpoints:
    1. Pre-dump clears soft-dirty bits (establishes baseline)
    2. Each capture only dumps pages dirtied since last checkpoint

    This dramatically reduces artifact size compared to full CRIU checkpoints
    because only the pages modified by the 8-byte write are captured (~4KB
    instead of ~4MB).

    Requires: criu installed, container with --security-opt seccomp=unconfined,
    Docker experimental mode.
    """

    def __init__(self, root: Path, state_mb: int = 50):
        super().__init__("criu-inc", root)
        self.state_mb = state_mb
        self.images_dir = self.meta_dir / "images"
        self.prev_dir: Path | None = None
        self.snap_idx = 0
        self.container_pid: int | None = None

    def start(self) -> None:
        reset_dir(self.live_dir)
        reset_dir(self.meta_dir)
        ensure_dir(self.images_dir)

    def stop(self) -> None:
        docker_rm(CONTAINER_NAME)
        sudo(f"rm -rf {self.images_dir}", check=False)

    def _get_container_pid(self) -> int:
        r = run(f"docker inspect --format '{{{{.State.Pid}}}}' {CONTAINER_NAME}",
                capture=True)
        pid = int(r.stdout.strip())
        if pid <= 0:
            raise RuntimeError(f"Container {CONTAINER_NAME} not running (pid={pid})")
        return pid

    def initialize_after_seed(self) -> None:
        self.container_pid = self._get_container_pid()
        log.info("  [CRIU-INC] Container PID: %d", self.container_pid)

        # Pre-dump: captures all memory pages and clears soft-dirty bits.
        # Subsequent incremental dumps only capture pages dirtied after this.
        self.snap_idx = 0
        pre_dir = self.images_dir / "pre"
        ensure_dir(pre_dir)
        sudo(
            f"criu pre-dump "
            f"--tree {self.container_pid} "
            f"--images-dir {pre_dir} "
            f"--leave-running "
            f"--shell-job "
            f"--tcp-established "
            f"2>/dev/null"
        )
        self.prev_dir = pre_dir
        log.info("  [CRIU-INC] Pre-dump complete, soft-dirty bits cleared")

    def capture(self, iteration: int | str) -> dict:
        self.snap_idx += 1
        cur_dir = self.images_dir / f"inc_{self.snap_idx}"
        ensure_dir(cur_dir)

        start = time.perf_counter()
        sudo(
            f"criu pre-dump "
            f"--tree {self.container_pid} "
            f"--images-dir {cur_dir} "
            f"--prev-images-dir {self.prev_dir} "
            f"--leave-running "
            f"--shell-job "
            f"--tcp-established "
            f"--track-mem "
            f"2>/dev/null"
        )
        elapsed_ms = (time.perf_counter() - start) * 1000.0

        # Measure artifact size (only the incremental pages file)
        r = run(f"sudo -n du -sb {cur_dir}", check=False, capture=True)
        artifact_bytes = int(r.stdout.split()[0]) if r.returncode == 0 and r.stdout.strip() else 0

        # Clean up previous dump to avoid disk bloat, keep current as next prev
        if self.prev_dir and self.prev_dir != cur_dir:
            sudo(f"rm -rf {self.prev_dir}", check=False)
        self.prev_dir = cur_dir

        return {
            "capture_ms": elapsed_ms,
            "artifact_bytes": artifact_bytes,
            "num_actions": -1,
        }


def make_harness(name: str, root: Path, state_mb: int = 50) -> BaselineHarness:
    if name == "fuselog":
        return FuselogHarness(root)
    if name == "rsync":
        return RsyncHarness(root)
    if name == "btrfs":
        return BtrfsHarness(root)
    if name == "kvm-qcow":
        return KvmQcowHarness(root, state_mb=state_mb)
    if name == "criu":
        return CriuHarness(root, state_mb=state_mb)
    if name == "criu-inc":
        return CriuIncrementalHarness(root, state_mb=state_mb)
    raise ValueError(f"unknown baseline {name}")


# ---------------------------------------------------------------------------
# Summarization
# ---------------------------------------------------------------------------

def summarize(rows: list[dict]) -> list[dict]:
    grouped: dict[str, list[dict]] = {}
    for row in rows:
        grouped.setdefault(row["baseline"], []).append(row)

    summaries = []
    for baseline, items in grouped.items():
        exec_vals = [x["execution_ms"] for x in items]
        capture_vals = [x["capture_ms"] for x in items]
        coord_vals = [x["coordination_ms"] for x in items]
        artifact_vals = [x["artifact_bytes"] for x in items]
        summaries.append({
            "baseline": baseline,
            "n": len(items),
            "state_mb": items[0]["state_mb"],
            "write_size": items[0]["write_size"],
            "execution_ms_avg": mean(exec_vals),
            "execution_ms_p50": median(exec_vals),
            "capture_ms_avg": mean(capture_vals),
            "capture_ms_p50": median(capture_vals),
            "coordination_ms_avg": mean(coord_vals),
            "coordination_ms_p50": median(coord_vals),
            "artifact_bytes_avg": mean(artifact_vals),
            "artifact_bytes_p50": median(artifact_vals),
            "total_ms_avg": mean([r["execution_ms"] + r["capture_ms"] + r["coordination_ms"] for r in items]),
            "total_ms_p50": median([r["execution_ms"] + r["capture_ms"] + r["coordination_ms"] for r in items]),
        })
    return summaries


# ---------------------------------------------------------------------------
# Worker main
# ---------------------------------------------------------------------------

BASELINE_DESCRIPTIONS = {
    "fuselog": "FUSE filesystem logging — captures only written bytes via Unix socket",
    "btrfs": "btrfs CoW snapshot + incremental send between read-only snapshots",
    "rsync": "rsync --write-batch — rolling checksum scan of entire state",
    "kvm-qcow": "KVM/QEMU qcow2 incremental disk snapshot via virsh snapshot-create-as",
    "criu": "CRIU container checkpoint — captures process memory + filesystem diff",
    "criu-inc": "CRIU incremental checkpoint — only dumps pages dirtied since last checkpoint",
}


def global_cleanup() -> None:
    """Kill all containers, unmount all FUSE/btrfs mounts, destroy VMs."""
    log.info("Global cleanup: stopping containers, unmounting filesystems, destroying VMs ...")
    docker_rm(CONTAINER_NAME)
    # FUSE
    for mount_root in [FUSELOG_SHM_WORKDIR, WORKDIR]:
        for mnt in mount_root.glob("**/live"):
            sudo(f"fusermount -u {mnt} >/dev/null 2>&1 || true", check=False)
            sudo(f"umount -l {mnt} >/dev/null 2>&1 || true", check=False)
    # btrfs
    for mnt in WORKDIR.glob("**/mnt"):
        sudo(f"umount {mnt} >/dev/null 2>&1 || true", check=False)
    # KVM
    sudo(f"virsh destroy {KVM_VM_NAME} 2>/dev/null || true", check=False)
    sudo(f"virsh undefine {KVM_VM_NAME} --remove-all-storage --snapshots-metadata 2>/dev/null || true", check=False)
    # Coordination receivers
    stop_coordination_receivers(DEFAULT_BACKUP_HOSTS)
    # Temp files
    run(f"rm -f /dev/shm/sdcap_coord_*.bin", check=False)


def worker_main(args: argparse.Namespace) -> int:
    results_dir = Path(args.results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)
    configure_logging(results_dir / "run.log", args.verbose)

    baselines = args.baselines
    state_mb = args.state_mb
    write_size = args.write_size
    num_requests = args.requests
    backup_hosts = getattr(args, "backup_hosts", DEFAULT_BACKUP_HOSTS)

    # Register cleanup on exit
    import atexit
    atexit.register(global_cleanup)

    # ---- Plan ----
    log.info("=" * 72)
    log.info("Statediff Capture Baseline Comparison")
    log.info("=" * 72)
    log.info("")
    log.info("Configuration:")
    log.info("  Baselines    : %s", ", ".join(baselines))
    log.info("  State size   : %d MB", state_mb)
    log.info("  Write size   : %d bytes", write_size)
    log.info("  Requests     : %d per baseline", num_requests)
    log.info("  Backup hosts : %s (for coordination measurement)", ", ".join(backup_hosts))
    log.info("  Results dir  : %s", results_dir)
    log.info("")
    log.info("Plan:")
    for i, bl in enumerate(baselines, start=1):
        desc = BASELINE_DESCRIPTIONS.get(bl, "unknown")
        log.info("  [%d/%d] %s", i, len(baselines), bl)
        log.info("         %s", desc)
    log.info("")
    log.info("Each baseline will:")
    log.info("  1. Clean up any leftover state from previous runs")
    log.info("  2. Set up the harness (mount FUSE, create btrfs, launch VM, etc.)")
    log.info("  3. Start the filewriter service (writes %d-byte random data to %dMB state)", write_size, state_mb)
    log.info("  4. Initialize baseline (drain init statediff / create base snapshot)")
    log.info("  5. Measure %d requests: execution + capture + coordination", num_requests)
    log.info("  6. Clean up completely before next baseline")
    log.info("=" * 72)

    # ---- Phase 1: Build image ----
    log.info("[Phase 1] Building filewriter Docker image ...")
    filewriter_dir = find_existing([
        Path(__file__).resolve().parent.parent / "services" / "filewriter",
        Path.cwd() / "services" / "filewriter",
        Path.cwd() / "filewriter",
    ])
    run(f"DOCKER_BUILDKIT=0 docker build -t {FILEWRITER_IMAGE} {filewriter_dir}")

    # ---- Phase 2: Run baselines ----
    log.info("[Phase 2] Starting coordination receivers on backup hosts ...")
    start_coordination_receivers(backup_hosts)

    log.info("[Phase 3] Running %d baselines ...", len(baselines))
    results: list[dict] = []
    exp_root = WORKDIR / "sdcap_baselines"
    fuselog_root = FUSELOG_SHM_WORKDIR / "sdcap_baselines"
    succeeded: list[str] = []
    failed: list[str] = []

    for idx, baseline in enumerate(baselines, start=1):
        log.info("")
        log.info("=" * 72)
        log.info("[%d/%d] Baseline: %s", idx, len(baselines), baseline)
        log.info("       %s", BASELINE_DESCRIPTIONS.get(baseline, ""))
        log.info("=" * 72)

        # Step 1: Isolate — clean up anything from previous baselines
        log.info("  [Step 1] Cleaning up previous state ...")
        docker_rm(CONTAINER_NAME)
        for mnt in list(FUSELOG_SHM_WORKDIR.glob("**/live")) + list(WORKDIR.glob("**/live")):
            sudo(f"fusermount -u {mnt} >/dev/null 2>&1 || true", check=False)
            sudo(f"umount -l {mnt} >/dev/null 2>&1 || true", check=False)
        for mnt in WORKDIR.glob("**/mnt"):
            sudo(f"umount {mnt} >/dev/null 2>&1 || true", check=False)
        sudo(f"virsh destroy {KVM_VM_NAME} 2>/dev/null || true", check=False)
        # Fresh work directories
        if baseline == "fuselog":
            harness_root = fuselog_root
            reset_dir(fuselog_root)
        else:
            harness_root = exp_root
            reset_dir(exp_root)
        ensure_dir(harness_root)

        harness = make_harness(baseline, harness_root, state_mb=state_mb)
        is_kvm = isinstance(harness, KvmQcowHarness)
        is_criu = isinstance(harness, (CriuHarness, CriuIncrementalHarness))

        try:
            # Step 2: Set up harness
            log.info("  [Step 2] Setting up %s harness ...", baseline)
            if is_kvm:
                harness.start()
                service_url = harness.get_service_url()
            else:
                if baseline == "fuselog":
                    reset_dir(harness.mount_path())
                    create_state_file(harness.mount_path() / "state.bin", state_mb)
                harness.start()
                if baseline != "fuselog":
                    create_state_file(harness.mount_path() / "state.bin", state_mb)

                # Step 3: Start container
                log.info("  [Step 3] Starting filewriter container (state=%dMB) ...", state_mb)
                docker_rm(CONTAINER_NAME)
                seccomp_opt = "--security-opt seccomp=unconfined" if is_criu else ""
                run(f"docker run -d --name {CONTAINER_NAME} "
                    f"-p {FILEWRITER_PORT}:8000 "
                    f"-e STATEDIR=/data "
                    f"-e INITSIZE={state_mb} "
                    f"{seccomp_opt} "
                    f"-v {harness.mount_path()}:/data "
                    f"{FILEWRITER_IMAGE}")
                service_url = f"http://127.0.0.1:{FILEWRITER_PORT}/"
                wait_http(service_url)

            # Step 4: Initialize
            log.info("  [Step 4] Initializing baseline (draining init state) ...")
            harness.initialize_after_seed()

            # Step 5: Measure
            log.info("  [Step 5] Measuring %d requests (write_size=%d) ...", num_requests, write_size)
            for i in range(num_requests):
                payload = f"size={write_size}".encode()

                execution_ms = http_post(service_url, payload, "application/x-www-form-urlencoded")
                capture = harness.capture(i)
                coordination_ms = measure_coordination_via_netcat(
                    capture["artifact_bytes"], backup_hosts
                )

                results.append({
                    "baseline": baseline,
                    "state_mb": state_mb,
                    "write_size": write_size,
                    "iteration": i,
                    "execution_ms": execution_ms,
                    "capture_ms": capture["capture_ms"],
                    "coordination_ms": coordination_ms,
                    "artifact_bytes": capture["artifact_bytes"],
                    "num_actions": capture["num_actions"],
                })

                if (i + 1) % 20 == 0 or i == num_requests - 1:
                    log.info("    [%s] %d/%d — exec=%.3fms cap=%.3fms coord=%.3fms artifact=%d B",
                             baseline, i + 1, num_requests, execution_ms,
                             capture["capture_ms"], coordination_ms,
                             capture["artifact_bytes"])

            # Per-baseline result
            bl_rows = [r for r in results if r["baseline"] == baseline]
            exec_p50 = median([r["execution_ms"] for r in bl_rows])
            cap_p50 = median([r["capture_ms"] for r in bl_rows])
            coord_p50 = median([r["coordination_ms"] for r in bl_rows])
            art_p50 = median([r["artifact_bytes"] for r in bl_rows])
            total_p50 = exec_p50 + cap_p50 + coord_p50
            log.info("  [Result] %s (p50): exec=%.3fms cap=%.3fms coord=%.3fms total=%.3fms artifact=%.0f B",
                     baseline, exec_p50, cap_p50, coord_p50, total_p50, art_p50)
            succeeded.append(baseline)

        except Exception:
            log.exception("[%d/%d] FAILED: %s", idx, len(baselines), baseline)
            failed.append(baseline)

        finally:
            # Step 6: Full cleanup for this baseline
            log.info("  [Step 6] Cleaning up %s ...", baseline)
            docker_rm(CONTAINER_NAME)
            harness.stop()
            # Extra cleanup to ensure isolation
            if baseline == "fuselog":
                sudo(f"rm -rf {fuselog_root}", check=False)
            elif baseline == "btrfs":
                sudo(f"umount {exp_root}/btrfs/meta/mnt >/dev/null 2>&1 || true", check=False)
                sudo(f"rm -rf {exp_root}", check=False)
            elif baseline == "kvm-qcow":
                sudo(f"virsh destroy {KVM_VM_NAME} 2>/dev/null || true", check=False)
                sudo(f"virsh undefine {KVM_VM_NAME} --remove-all-storage --snapshots-metadata 2>/dev/null || true", check=False)
                sudo(f"rm -rf {KVM_DISK_DIR}", check=False)
            elif baseline == "criu":
                sudo(f"rm -rf {exp_root}", check=False)
            else:
                sudo(f"rm -rf {exp_root}", check=False)
            log.info("  [Step 6] Cleanup complete for %s", baseline)

    # ---- Stop coordination receivers ----
    log.info("")
    log.info("Stopping coordination receivers ...")
    stop_coordination_receivers(backup_hosts)

    # ---- Phase 4: Write results ----
    log.info("")
    log.info("[Phase 4] Writing results ...")
    raw_path = results_dir / "eval_statediff_capture_baselines.csv"
    summary_path = results_dir / "eval_statediff_capture_baselines_summary.csv"
    summary_json_path = results_dir / "summary.json"

    summary_rows: list[dict] = []
    if results:
        with open(raw_path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(results[0].keys()))
            writer.writeheader()
            writer.writerows(results)

        summary_rows = summarize(results)
        with open(summary_path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(summary_rows[0].keys()))
            writer.writeheader()
            writer.writerows(summary_rows)
        summary_json_path.write_text(json.dumps(summary_rows, indent=2), encoding="utf-8")

        log.info("  Wrote %s", raw_path)
        log.info("  Wrote %s", summary_path)
        log.info("  Wrote %s", summary_json_path)

    # ---- Final Summary ----
    log.info("")
    log.info("=" * 72)
    log.info("FINAL SUMMARY")
    log.info("=" * 72)
    log.info("  Succeeded: %d/%d (%s)", len(succeeded), len(baselines), ", ".join(succeeded) or "none")
    if failed:
        log.info("  Failed   : %d/%d (%s)", len(failed), len(baselines), ", ".join(failed))
    log.info("")
    if summary_rows:
        log.info("  Latency breakdown (p50, in ms):")
        log.info("  %-10s %12s %12s %12s %12s %14s",
                 "Baseline", "Execution", "Capture", "Coordination", "Total", "Artifact (B)")
        log.info("  %s", "-" * 76)
        for row in summary_rows:
            total_p50 = row["execution_ms_p50"] + row["capture_ms_p50"] + row["coordination_ms_p50"]
            log.info("  %-10s %11.3fms %11.3fms %11.3fms %11.3fms %14.0f",
                     row["baseline"],
                     row["execution_ms_p50"], row["capture_ms_p50"],
                     row["coordination_ms_p50"], total_p50,
                     row["artifact_bytes_p50"])
        log.info("")
        log.info("  Statediff size (p50, in bytes):")
        log.info("  %-10s %14s", "Baseline", "Artifact P50")
        log.info("  %s", "-" * 28)
        for row in summary_rows:
            log.info("  %-10s %14.0f", row["baseline"], row["artifact_bytes_p50"])
    log.info("")
    log.info("  Results: %s", results_dir)
    log.info("=" * 72)

    return 0


# ---------------------------------------------------------------------------
# Driver main
# ---------------------------------------------------------------------------

def driver_main(args: argparse.Namespace) -> int:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_dir = RESULTS_BASE / f"microbench_sdcap_baselines_{timestamp}"
    results_dir.mkdir(parents=True, exist_ok=True)
    configure_logging(results_dir / "driver.log", args.verbose)

    remote_dir = args.remote_dir
    remote_dir_expr = remote_dir.replace("~/", "$HOME/") if remote_dir.startswith("~/") else remote_dir

    log.info("=" * 60)
    log.info("Statediff Capture Baselines — Driver")
    log.info("  Worker host  : %s", args.remote_host)
    log.info("  Remote dir   : %s", remote_dir)
    log.info("  Baselines    : %s", ", ".join(args.baselines))
    log.info("  State size   : %d MB", args.state_mb)
    log.info("  Write size   : %d bytes", args.write_size)
    log.info("  Requests     : %d per baseline", args.requests)
    log.info("  Results      : %s", results_dir)
    log.info("=" * 60)

    log.info("[Phase 1] Staging files to worker ...")
    ssh(args.remote_host, f"mkdir -p {remote_dir_expr}")
    files_to_stage = [
        str(Path(__file__).resolve()),
        str(ROOT_DIR / "services" / "filewriter"),
        str(ROOT_DIR / "xdn-fs" / "cpp"),
    ]
    run(f"rsync -az {' '.join(shlex.quote(f) for f in files_to_stage)} "
        f"{args.remote_host}:{remote_dir}/")

    log.info("[Phase 2] Running benchmark on worker ...")
    worker_cmd = [
        "python3", Path(__file__).name,
        "--worker-mode",
        f"--results-dir results/{results_dir.name}",
        f"--baselines {' '.join(args.baselines)}",
        f"--state-mb {args.state_mb}",
        f"--write-size {args.write_size}",
        f"--requests {args.requests}",
        f"--backup-hosts {' '.join(args.backup_hosts)}",
    ]
    if args.verbose:
        worker_cmd.append("--verbose")

    remote_cmd = f"cd {remote_dir_expr} && " + " ".join(worker_cmd)
    rc = 0
    try:
        ssh(args.remote_host, remote_cmd)
    except subprocess.CalledProcessError as exc:
        rc = exc.returncode
        log.exception("Remote benchmark failed.")

    log.info("[Phase 3] Collecting results from worker ...")
    remote_results = f"{remote_dir}/results/{results_dir.name}"
    remote_results_rsync = remote_results.replace("$HOME", "~")
    try:
        run(f"rsync -az {args.remote_host}:{remote_results_rsync}/ {results_dir}/")
    except subprocess.CalledProcessError:
        log.exception("Failed to copy remote results")

    # ---- Driver-side summary from collected results ----
    log.info("")
    log.info("=" * 72)
    log.info("FINAL SUMMARY")
    log.info("=" * 72)

    summary_csv = results_dir / "eval_statediff_capture_baselines_summary.csv"
    if summary_csv.exists():
        with open(summary_csv, encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        if rows:
            log.info("")
            log.info("  Latency breakdown (p50, in ms):")
            log.info("  %-10s %12s %12s %12s %12s %14s",
                     "Baseline", "Execution", "Capture", "Coordination", "Total", "Artifact (B)")
            log.info("  %s", "-" * 76)
            for row in rows:
                exec_p50 = float(row["execution_ms_p50"])
                cap_p50 = float(row["capture_ms_p50"])
                coord_p50 = float(row["coordination_ms_p50"])
                total_p50 = exec_p50 + cap_p50 + coord_p50
                art_p50 = float(row["artifact_bytes_p50"])
                log.info("  %-10s %11.3fms %11.3fms %11.3fms %11.3fms %14.0f",
                         row["baseline"], exec_p50, cap_p50, coord_p50, total_p50, art_p50)
            log.info("")
            log.info("  Statediff size (p50, in bytes):")
            log.info("  %-10s %14s", "Baseline", "Artifact P50")
            log.info("  %s", "-" * 28)
            for row in rows:
                log.info("  %-10s %14.0f", row["baseline"], float(row["artifact_bytes_p50"]))
    else:
        log.warning("  No summary CSV found at %s", summary_csv)

    log.info("")
    log.info("  Results: %s", results_dir)
    if rc != 0:
        log.warning("  Worker exited with code %d — some baselines may have failed", rc)
    log.info("=" * 72)
    return rc


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--remote-host", default=DEFAULT_REMOTE_HOST)
    parser.add_argument("--remote-dir", default=DEFAULT_REMOTE_DIR)
    parser.add_argument("--baselines", nargs="+", default=DEFAULT_BASELINES)
    parser.add_argument("--state-mb", type=int, default=DEFAULT_STATE_MB)
    parser.add_argument("--write-size", type=int, default=DEFAULT_WRITE_SIZE)
    parser.add_argument("--requests", type=int, default=DEFAULT_REQUESTS)
    parser.add_argument("--backup-hosts", nargs="+", default=DEFAULT_BACKUP_HOSTS,
                        help="Hosts to broadcast statediff to for coordination measurement")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--worker-mode", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--results-dir", default=None, help=argparse.SUPPRESS)
    return parser.parse_args()


if __name__ == "__main__":
    parsed = parse_args()
    if parsed.worker_mode:
        sys.exit(worker_main(parsed))
    else:
        sys.exit(driver_main(parsed))
