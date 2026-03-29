"""
sanity_check.py — Reusable per-request I/O sanity check module.

Measures per-request replication cost across XDN components:
  - Service container (e.g., PostgreSQL, MySQL): write bytes, fsyncs
  - GigaPaxos JVM (primary): write bytes, fsyncs
  - GigaPaxos JVM (backup): write bytes, fsyncs
  - Network TX from primary
  - Statediff size from fuselog

Uses bpftrace kernel tracepoints for accurate syscall-level measurement,
including writes through FUSE mounts. Automatically installs bpftrace
if not present on the target hosts.

Usage from any run_load_pb_*.py script:

    from coordination_size_sanity_check import SanityChecker

    checker = SanityChecker(
        primary="10.10.1.1",
        backups=["10.10.1.2", "10.10.1.3"],
        port=2300,
        service_name="tpcc",
        endpoint="/orders",
        db_container_pattern="postgres",
        db_process_name="postgres",
    )
    checker.run(payload_file="payloads.txt", results_dir=Path("results/..."))
"""

import csv
import os
import re
import subprocess
import time
import urllib.request
from pathlib import Path


# ── SSH / Process helpers ─────────────────────────────────────────────────────

def ssh_output(host, cmd, timeout=30):
    """Run SSH command and return stdout."""
    r = subprocess.run(
        ["ssh", "-o", "StrictHostKeyChecking=no", host, cmd],
        capture_output=True, text=True, timeout=timeout,
    )
    return r.stdout.strip()


def get_proc_io(host, pid):
    """Read /proc/<pid>/io and return dict of counters."""
    out = ssh_output(host, f"sudo cat /proc/{pid}/io")
    counters = {}
    for line in out.splitlines():
        if ":" in line:
            k, v = line.split(":", 1)
            counters[k.strip()] = int(v.strip())
    return counters


def get_jvm_pid(host):
    """Find the ReconfigurableNode JVM PID on a host."""
    return ssh_output(host,
        "ps -ef | grep 'java.*ReconfigurableNode' | grep -v grep | grep -v sudo "
        "| awk '{print $2}' | head -1")


def get_fuselog_pid(host):
    """Find the fuselog FUSE daemon PID."""
    pid = ssh_output(host, "pgrep -f '/usr/local/bin/fuselog' | head -1")
    return pid if pid else None


def get_db_container_pid(host, container_pattern, process_name):
    """Find a database container and its main server PID."""
    name = ssh_output(host,
        f"docker ps --format '{{{{.Names}}}}' | grep '{container_pattern}' | head -1")
    if not name:
        name = ssh_output(host,
            f"docker ps --format '{{{{.Names}}}} {{{{.Image}}}}' | grep '{container_pattern}' "
            f"| awk '{{print $1}}' | head -1")
    if not name:
        return None, None

    pid = ssh_output(host,
        f"docker exec {name} bash -c 'pgrep -x {process_name} | head -1' 2>/dev/null")
    if not pid:
        pid = ssh_output(host,
            f"docker top {name} -eo pid,comm | grep {process_name} | awk '{{print $1}}' | head -1")
    if not pid:
        pid = ssh_output(host,
            f"docker inspect {name} --format '{{{{.State.Pid}}}}'")
    return name, pid


def get_net_tx(host, iface=None):
    """Get TX bytes from /proc/net/dev."""
    if iface is None:
        iface = detect_net_iface(host)
    out = ssh_output(host, f"cat /proc/net/dev | grep {iface}")
    parts = out.split()
    return int(parts[9]) if len(parts) >= 10 else 0


def detect_net_iface(host, target_host=None):
    """Detect the network interface used for inter-node communication."""
    if target_host:
        out = ssh_output(host, f"ip route get {target_host} | head -1")
        if "dev " in out:
            return out.split("dev ")[1].split()[0]
    out = ssh_output(host, "ip -o -4 addr show | grep -v '127.0.0.1' | awk '{print $2}' | head -1")
    return out if out else "eth0"


# ── bpftrace helpers ─────────────────────────────────────────────────────────

def ensure_bpftrace(hosts):
    """Install bpftrace on hosts where it is not available.

    Uses apt-get on Debian/Ubuntu. Logs progress and returns list of
    hosts where bpftrace is now available.
    """
    ready = []
    for host in hosts:
        out = ssh_output(host, "which bpftrace 2>/dev/null")
        if out:
            ready.append(host)
            continue
        print(f"  [bpftrace] Installing on {host} ...")
        r = subprocess.run(
            ["ssh", "-o", "StrictHostKeyChecking=no", host,
             "sudo apt-get update -qq && sudo apt-get install -y -qq bpftrace"],
            capture_output=True, text=True, timeout=120,
        )
        if r.returncode == 0:
            ready.append(host)
            print(f"  [bpftrace] Installed on {host}")
        else:
            # Try snap as fallback
            r2 = subprocess.run(
                ["ssh", "-o", "StrictHostKeyChecking=no", host,
                 "sudo snap install bpftrace 2>/dev/null || true"],
                capture_output=True, text=True, timeout=120,
            )
            verify = ssh_output(host, "which bpftrace 2>/dev/null")
            if verify:
                ready.append(host)
                print(f"  [bpftrace] Installed via snap on {host}")
            else:
                print(f"  [bpftrace] WARNING: failed to install on {host}: {r.stderr[:200]}")
    return ready


def start_io_trace(host, tag):
    """Start bpftrace to trace both fsyncs and write bytes system-wide.

    Traces:
      - fsync/fdatasync/sync_file_range syscalls (count by pid+comm)
      - write/pwrite64 syscalls (sum bytes by pid+comm)

    This captures all processes including container children, JVM threads,
    and writes through FUSE mounts. Uses a script file to avoid shell
    quoting issues with SSH. Output goes to /tmp/bpf_io_{tag}.out.
    """
    bt_file = f"/tmp/bpf_io_{tag}.bt"
    out_file = f"/tmp/bpf_io_{tag}.out"
    bpf_script = (
        "tracepoint:syscalls:sys_enter_fsync,\n"
        "tracepoint:syscalls:sys_enter_fdatasync,\n"
        "tracepoint:syscalls:sys_enter_sync_file_range\n"
        "{ @fsync[pid, comm] = count(); }\n"
        "\n"
        "tracepoint:syscalls:sys_enter_write\n"
        "{ @wbytes[pid, comm] = sum(args->count); }\n"
        "\n"
        "tracepoint:syscalls:sys_enter_pwrite64\n"
        "{ @wbytes[pid, comm] = sum(args->count); }\n"
    )
    # Kill any previous bpftrace for this tag
    subprocess.run(
        ["ssh", "-o", "StrictHostKeyChecking=no", host,
         f"sudo pkill -9 -f 'bpftrace.*/tmp/bpf_io_{tag}' 2>/dev/null; "
         f"sudo rm -f {out_file}"],
        capture_output=True, text=True, timeout=10,
    )
    # Write bpftrace script to remote file via stdin pipe
    subprocess.run(
        ["ssh", "-o", "StrictHostKeyChecking=no", host,
         f"cat > {bt_file}"],
        input=bpf_script, capture_output=True, text=True, timeout=10,
    )
    # Run bpftrace from the script file in background, wait for it to attach
    subprocess.run(
        ["ssh", "-o", "StrictHostKeyChecking=no", host,
         f"sudo nohup bpftrace {bt_file} -o {out_file} "
         f"</dev/null >/dev/null 2>&1 & "
         f"sleep 0.5; "
         f"for i in $(seq 1 10); do "
         f"  if grep -q 'Attaching' {out_file} 2>/dev/null; then break; fi; "
         f"  sleep 0.2; "
         f"done"],
        capture_output=True, text=True, timeout=15,
    )


def stop_io_trace(host, tag, comm_filters=None):
    """Stop bpftrace and parse both fsync counts and write bytes.

    Args:
        comm_filters: dict mapping category name -> list of comm name substrings
                      to match. E.g., {"svc": ["postgres"], "jvm": ["Paxos", "java"]}
    Returns: dict with keys "{category}_fsync" and "{category}_wbytes" for each
             category in comm_filters. Also returns raw_lines for debugging.
    """
    # Send SIGINT to bpftrace to flush output, wait for it to exit cleanly
    bt_file = f"/tmp/bpf_io_{tag}.bt"
    out_file = f"/tmp/bpf_io_{tag}.out"
    # SIGINT triggers bpftrace to print results and exit; wait for it
    ssh_output(host,
        f"sudo pkill -INT -f 'bpftrace.*/tmp/bpf_io_{tag}' 2>/dev/null; "
        f"for i in $(seq 1 20); do "
        f"  if ! sudo pgrep -f 'bpftrace.*/tmp/bpf_io_{tag}' >/dev/null 2>&1; then break; fi; "
        f"  sleep 0.3; "
        f"done; "
        f"sudo pkill -9 -f 'bpftrace.*/tmp/bpf_io_{tag}' 2>/dev/null || true",
        timeout=30)
    out = ssh_output(host, f"sudo cat {out_file} 2>/dev/null")
    ssh_output(host, f"sudo rm -f {out_file} {bt_file} 2>/dev/null")

    # Parse two map types:
    #   @fsync[pid, comm]: count
    #   @wbytes[pid, comm]: sum
    fsync_entries = []   # (pid, comm, count)
    wbytes_entries = []  # (pid, comm, total_bytes)

    for line in out.splitlines():
        line = line.strip()
        if not line.startswith("@"):
            continue
        try:
            # Format: @mapname[pid, comm]: value
            map_name = line.split("[")[0].lstrip("@")
            bracket_content = line.split("[")[1].split("]:")[0]
            value_str = line.split("]:")[1].strip()
            value = int(value_str)
            parts = [p.strip() for p in bracket_content.split(",")]
            if len(parts) < 2:
                continue
            pid = int(parts[0])
            comm = parts[1]

            if map_name == "fsync":
                fsync_entries.append((pid, comm, value))
            elif map_name == "wbytes":
                wbytes_entries.append((pid, comm, value))
        except (ValueError, IndexError):
            continue

    # Categorize by comm name filters
    results = {}
    if comm_filters:
        for category, patterns in comm_filters.items():
            fsync_total = 0
            wbytes_total = 0
            for pid, comm, count in fsync_entries:
                if any(p.lower() in comm.lower() for p in patterns):
                    fsync_total += count
            for pid, comm, total in wbytes_entries:
                if any(p.lower() in comm.lower() for p in patterns):
                    wbytes_total += total
            results[f"{category}_fsync"] = fsync_total
            results[f"{category}_wbytes"] = wbytes_total
    else:
        results["all_fsync"] = sum(c for _, _, c in fsync_entries)
        results["all_wbytes"] = sum(c for _, _, c in wbytes_entries)

    return results, out


# ── SanityChecker class ──────────────────────────────────────────────────────

class SanityChecker:
    """Reusable per-request I/O sanity checker for XDN PB benchmarks."""

    FIELDNAMES = [
        "http_req_size", "svc_write", "svc_fsync",
        "jvm_write", "jvm_fsync", "bak_write", "bak_fsync",
        "net_tx", "diff_size",
    ]

    def __init__(self, primary, backups, port, service_name, endpoint,
                 db_container_pattern, db_process_name, n_samples=10):
        self.primary = primary
        self.backup_host = backups[0] if backups else None
        self.port = port
        self.service_name = service_name
        self.endpoint = endpoint
        self.db_container_pattern = db_container_pattern
        self.db_process_name = db_process_name
        self.n_samples = n_samples

    def run(self, payload_file, results_dir):
        """Run the sanity check and save results."""
        print("\n" + "=" * 70)
        print("  SANITY CHECK: Per-request I/O measurement")
        print("=" * 70)

        # Read first payload
        with open(payload_file, "r") as f:
            payload = f.readline().strip()
        if not payload:
            print("  ERROR: no payload in file")
            return None

        # Ensure bpftrace is available on all hosts
        all_hosts = [self.primary]
        if self.backup_host:
            all_hosts.append(self.backup_host)
        ready_hosts = ensure_bpftrace(all_hosts)
        if self.primary not in ready_hosts:
            print("  ERROR: bpftrace not available on primary, cannot proceed")
            return None
        bpf_on_backup = self.backup_host in ready_hosts if self.backup_host else False

        # Find PIDs
        db_name, db_pid = get_db_container_pid(
            self.primary, self.db_container_pattern, self.db_process_name)
        fuselog_pid = get_fuselog_pid(self.primary)
        jvm_pri_pid = get_jvm_pid(self.primary)
        jvm_bak_pid = get_jvm_pid(self.backup_host) if self.backup_host else None

        print(f"  Primary:    {self.primary}")
        print(f"  DB:         {db_name} (PID={db_pid})")
        print(f"  Fuselog:    PID={fuselog_pid}")
        print(f"  JVM (pri):  PID={jvm_pri_pid}")
        if self.backup_host:
            print(f"  Backup:     {self.backup_host} (bpftrace={'yes' if bpf_on_backup else 'NO'})")
            print(f"  JVM (bak):  PID={jvm_bak_pid}")

        if not db_pid or not jvm_pri_pid:
            print("  ERROR: could not find PIDs, skipping sanity check")
            return None

        # Detect network interface
        iface = detect_net_iface(self.primary, self.backup_host) if self.backup_host else None
        if iface:
            print(f"  Network:    {iface}")

        # HTTP request size
        http_payload_size = len(payload.encode())
        request_line = f"POST {self.endpoint} HTTP/1.1\r\n"
        headers_text = (
            f"Host: {self.primary}:{self.port}\r\n"
            f"Content-Type: application/json\r\n"
            f"XDN: {self.service_name}\r\n"
            f"Content-Length: {http_payload_size}\r\n"
            f"\r\n"
        )
        http_req_size = len(request_line.encode()) + len(headers_text.encode()) + http_payload_size
        print(f"\n  HTTP request: payload={http_payload_size} bytes, total~{http_req_size} bytes")
        print(f"  Measurement: bpftrace write/pwrite64 (captures FUSE writes) + fsync tracepoints")
        print(f"  Sending {self.n_samples} individual requests ...\n")

        # Header
        header = (f"  {'#':>3}  {'http_req':>9}  {'svc_write':>10}  {'svc_fsync':>10}  "
                  f"{'jvm_write':>10}  {'jvm_fsync':>10}  {'bak_write':>10}  {'bak_fsync':>10}  "
                  f"{'net_tx':>10}  {'diff_size':>10}")
        units = (f"  {'':>3}  {'(bytes)':>9}  {'(bytes)':>10}  {'(count)':>10}  "
                 f"{'(bytes)':>10}  {'(count)':>10}  {'(bytes)':>10}  {'(count)':>10}  "
                 f"{'(bytes)':>10}  {'(bytes)':>10}")
        print(header)
        print(units)
        print("  " + "-" * 110)

        results = []
        for i in range(1, self.n_samples + 1):
            row = self._measure_one_request(
                payload, db_pid, fuselog_pid, jvm_pri_pid, jvm_bak_pid,
                iface, http_req_size, i, bpf_on_backup)
            if row:
                results.append(row)

        if not results:
            print("  No successful samples")
            return None

        # Averages
        print("  " + "-" * 110)
        avg = {k: sum(r[k] for r in results) / len(results) for k in results[0]}
        print(f"  {'avg':>3}  {avg['http_req_size']:>9.0f}  {avg['svc_write']:>10.0f}  "
              f"{avg['svc_fsync']:>10.1f}  "
              f"{avg['jvm_write']:>10.0f}  {avg['jvm_fsync']:>10.1f}  "
              f"{avg['bak_write']:>10.0f}  {avg['bak_fsync']:>10.1f}  "
              f"{avg['net_tx']:>10.0f}  {avg['diff_size']:>10.0f}")

        print(f"\n  Legend:")
        print(f"    http_req_size = full HTTP request size (request line + headers + body)")
        print(f"    svc_write     = bytes written by DB process (bpftrace write+pwrite64, includes FUSE)")
        print(f"    svc_fsync     = fsyncs by DB process (bpftrace fsync+fdatasync+sync_file_range)")
        print(f"    jvm_write     = bytes written by GigaPaxos JVM primary (/proc/pid/io write_bytes)")
        print(f"    jvm_fsync     = fsyncs by GigaPaxos JVM primary (bpftrace)")
        print(f"    bak_write     = bytes written by GigaPaxos JVM backup (/proc/pid/io write_bytes)")
        print(f"    bak_fsync     = fsyncs by GigaPaxos JVM backup (bpftrace)")
        print(f"    net_tx        = primary network TX bytes delta")
        print(f"    diff_size     = statediff size captured by fuselog (from PBM sample log)")

        # Save CSV
        if results_dir:
            results_dir = Path(results_dir)
            results_dir.mkdir(parents=True, exist_ok=True)
            csv_path = results_dir / "sanity_check.csv"
            with open(csv_path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=self.FIELDNAMES)
                writer.writeheader()
                writer.writerows(results)
            print(f"\n  Saved to {csv_path}")

        print("=" * 70)
        return results

    def _measure_one_request(self, payload, db_pid, fuselog_pid,
                              jvm_pri_pid, jvm_bak_pid, iface,
                              http_req_size, sample_num, bpf_on_backup):
        """Send one request and measure all I/O deltas."""
        primary = self.primary
        backup_host = self.backup_host

        # Before — proc/io for JVM write_bytes (works for JVM since it writes to real disk)
        jvm_io_before = get_proc_io(primary, jvm_pri_pid) or {}
        bak_io_before = get_proc_io(backup_host, jvm_bak_pid) if jvm_bak_pid else {}
        net_before = get_net_tx(primary, iface) if iface else 0

        # Clear PBM samples
        subprocess.run(["ssh", primary, "rm -f /tmp/pbm_samples.log"],
                       capture_output=True, timeout=5)

        # Start bpftrace on primary (and backup if available)
        # start_io_trace waits internally for probes to attach
        start_io_trace(primary, "pri")
        if backup_host and bpf_on_backup:
            start_io_trace(backup_host, "bak")

        # Send ONE request
        url = f"http://{primary}:{self.port}{self.endpoint}"
        req = urllib.request.Request(
            url, data=payload.encode(),
            headers={"Content-Type": "application/json", "XDN": self.service_name},
            method="POST",
        )
        try:
            resp = urllib.request.urlopen(req, timeout=30)
            resp.read()
        except Exception as e:
            print(f"  {sample_num:>3}  ERROR: {e}")
            stop_io_trace(primary, "pri")
            if backup_host and bpf_on_backup:
                stop_io_trace(backup_host, "bak")
            return None

        time.sleep(1.0)  # let writes and replication flush

        # Stop bpftrace — filter by comm name patterns
        pri_comm_filters = {
            "svc": [self.db_process_name],     # e.g., ["postgres"]
            "fuse": ["fuselog"],
            "jvm": ["Paxos", "java", "Abstract"],
        }
        pri_results, pri_raw = stop_io_trace(primary, "pri", pri_comm_filters)
        svc_write = pri_results.get("svc_wbytes", 0)
        svc_fsync = pri_results.get("svc_fsync", 0)
        jvm_fsync = pri_results.get("jvm_fsync", 0)

        if backup_host and bpf_on_backup:
            bak_comm_filters = {
                "jvm": ["Paxos", "java", "Abstract"],
            }
            bak_results, bak_raw = stop_io_trace(backup_host, "bak", bak_comm_filters)
            bak_fsync = bak_results.get("jvm_fsync", 0)
        else:
            bak_fsync = 0

        # After — proc/io for JVM (writes to real disk, so write_bytes works)
        jvm_io_after = get_proc_io(primary, jvm_pri_pid) or {}
        bak_io_after = get_proc_io(backup_host, jvm_bak_pid) if jvm_bak_pid else {}
        net_after = get_net_tx(primary, iface) if iface else 0

        # Statediff size from PBM sample log
        diff_out = ssh_output(primary,
            "cat /tmp/pbm_samples.log 2>/dev/null | grep -oP 'diffSize=\\K[0-9]+' | tail -1")
        diff_size = int(diff_out) if diff_out else 0

        # Compute deltas
        jvm_write = jvm_io_after.get('write_bytes', 0) - jvm_io_before.get('write_bytes', 0)
        bak_write = bak_io_after.get('write_bytes', 0) - bak_io_before.get('write_bytes', 0) if bak_io_before else 0
        net_tx = net_after - net_before

        row = {
            "http_req_size": http_req_size,
            "svc_write": svc_write, "svc_fsync": svc_fsync,
            "jvm_write": jvm_write, "jvm_fsync": jvm_fsync,
            "bak_write": bak_write, "bak_fsync": bak_fsync,
            "net_tx": net_tx, "diff_size": diff_size,
        }

        print(f"  {sample_num:>3}  {http_req_size:>9}  {svc_write:>10}  {svc_fsync:>10}  "
              f"{jvm_write:>10}  {jvm_fsync:>10}  "
              f"{bak_write:>10}  {bak_fsync:>10}  "
              f"{net_tx:>10}  {diff_size:>10}")

        return row
