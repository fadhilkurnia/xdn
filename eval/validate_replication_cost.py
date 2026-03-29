"""
validate_replication_cost.py — Measure per-request replication cost:
fsyncs, bytes written, and bytes replicated for each system.

Sends a known number of requests to a running service and measures:
  1. fsyncs per request (via /proc/<pid>/io or strace)
  2. bytes written per request (via /proc/<pid>/io)
  3. bytes replicated per request (system-specific)

Run from the eval/ directory with a running cluster:
    python3 validate_replication_cost.py --system xdn --host 10.10.1.3 --port 2300 \
        --service synth --endpoint /workload --payload '{"txns":1,"ops":1,"write_size":100}'
    python3 validate_replication_cost.py --system rqlite --host 10.10.1.1 --port 80 \
        --endpoint /workload --payload '{"txns":1,"ops":1,"write_size":100}'
    python3 validate_replication_cost.py --system openebs --host 10.10.1.1 --port 30080 \
        --endpoint /workload --payload '{"txns":1,"ops":1,"write_size":100}'
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time
import urllib.request

# ── Helpers ───────────────────────────────────────────────────────────────────

def _ssh(host, cmd, capture=True):
    return subprocess.run(
        ["ssh", "-o", "StrictHostKeyChecking=no", host, cmd],
        capture_output=capture, text=True, check=False,
    )


def get_proc_io(host, pid):
    """Read /proc/<pid>/io and return dict of counters."""
    result = _ssh(host, f"sudo cat /proc/{pid}/io")
    if result.returncode != 0:
        return None
    counters = {}
    for line in result.stdout.strip().splitlines():
        key, val = line.split(":")
        counters[key.strip()] = int(val.strip())
    return counters


def get_container_pid(host, container_name_pattern):
    """Find the main PID of a Docker container matching the pattern."""
    result = _ssh(host, f"docker ps --format '{{{{.Names}}}}' | grep '{container_name_pattern}' | head -1")
    name = result.stdout.strip()
    if not name:
        return None, None
    result = _ssh(host, f"docker inspect {name} --format '{{{{.State.Pid}}}}'")
    pid = result.stdout.strip()
    return name, int(pid) if pid else None


def get_sqlite_pid(host, data_dir_pattern="/data"):
    """Find the PID of a process with the SQLite database open."""
    result = _ssh(host, f"sudo lsof 2>/dev/null | grep '{data_dir_pattern}' | grep -v FUSE | awk '{{print $2}}' | sort -u | head -1")
    pid = result.stdout.strip()
    return int(pid) if pid else None


def count_fsyncs_strace(host, pid, duration_sec=5):
    """Run strace on a PID for duration_sec and count fdatasync/fsync calls."""
    result = _ssh(host,
        f"sudo timeout {duration_sec + 2} strace -c -e fdatasync,fsync -p {pid} 2>&1 &"
        f" STRACE_PID=$!; sleep {duration_sec}; sudo kill -INT $STRACE_PID 2>/dev/null; wait $STRACE_PID 2>/dev/null"
    )
    # Parse strace -c output for syscall counts
    total = 0
    for line in result.stdout.splitlines() + result.stderr.splitlines() if hasattr(result, 'stderr') else result.stdout.splitlines():
        if 'fdatasync' in line or 'fsync' in line:
            parts = line.split()
            if parts and parts[0].isdigit():
                # strace -c format: % time     seconds  usecs/call     calls    errors syscall
                # Find the 'calls' column
                for i, p in enumerate(parts):
                    if p in ('fdatasync', 'fsync'):
                        total += int(parts[3])  # calls column
                        break
    return total


def send_requests(host, port, endpoint, payload, n_requests, service=None, method="POST"):
    """Send n_requests and return elapsed time."""
    headers = {"Content-Type": "application/json"}
    if service:
        headers["XDN"] = service

    success = 0
    t0 = time.time()
    for i in range(n_requests):
        try:
            req = urllib.request.Request(
                f"http://{host}:{port}{endpoint}",
                data=payload.encode() if payload else None,
                headers=headers,
                method=method,
            )
            resp = urllib.request.urlopen(req, timeout=30)
            resp.read()
            if resp.status < 400:
                success += 1
        except Exception as e:
            if i < 3:
                print(f"  WARNING: request {i+1} failed: {e}")
    elapsed = time.time() - t0
    return success, elapsed


def measure_proc_io_delta(host, pid, func):
    """Measure /proc/<pid>/io delta while func() runs. Returns delta dict."""
    before = get_proc_io(host, pid)
    if not before:
        return None
    func()
    time.sleep(0.5)  # let writes flush
    after = get_proc_io(host, pid)
    if not after:
        return None
    delta = {}
    for key in before:
        delta[key] = after[key] - before[key]
    return delta


# ── System-specific measurement ──────────────────────────────────────────────


def measure_xdn(host, port, service, endpoint, payload, n_requests):
    """Measure XDN replication cost."""
    print(f"\n=== XDN Replication Cost ({n_requests} requests) ===")

    # Find the app container PID (for proc/io)
    container_name, container_pid = get_container_pid(host, "xdn.io")
    print(f"  App container: {container_name} (PID={container_pid})")

    # Find the JVM PID (for Paxos proposal measurement)
    jvm_pid_result = _ssh(host,
        "ps -ef | grep 'java.*ReconfigurableNode' | grep -v grep | grep -v sudo | awk '{print $2}' | head -1")
    jvm_pid = jvm_pid_result.stdout.strip()
    print(f"  JVM PID: {jvm_pid}")

    # Measure proc/io delta on the container
    if container_pid:
        delta = measure_proc_io_delta(host, container_pid,
            lambda: send_requests(host, port, endpoint, payload, n_requests, service))
        if delta:
            print(f"\n  Container I/O delta ({n_requests} requests):")
            print(f"    write_bytes:   {delta.get('write_bytes', 0):>12} ({delta.get('write_bytes', 0) / n_requests:.0f} per request)")
            print(f"    syscw (writes):{delta.get('syscw', 0):>12} ({delta.get('syscw', 0) / n_requests:.1f} per request)")
    else:
        send_requests(host, port, endpoint, payload, n_requests, service)

    # Check PBM_SAMPLE file for statediff sizes (if available)
    result = _ssh(host, "cat /tmp/pbm_samples.log 2>/dev/null | tail -20")
    if result.stdout.strip():
        sizes = []
        for line in result.stdout.splitlines():
            m = re.search(r'diffSize=(\d+)', line)
            if m:
                sizes.append(int(m.group(1)))
        if sizes:
            avg_diff = sum(sizes) / len(sizes)
            print(f"\n  Statediff sizes (from PBM_SAMPLE):")
            print(f"    avg: {avg_diff:.0f} bytes")
            print(f"    min: {min(sizes)} bytes")
            print(f"    max: {max(sizes)} bytes")
            print(f"    samples: {len(sizes)}")


def measure_rqlite(host, port, endpoint, payload, n_requests, rqlite_host, rqlite_http_port=4001):
    """Measure rqlite replication cost."""
    print(f"\n=== rqlite Replication Cost ({n_requests} requests) ===")

    # Get rqlite status before
    try:
        with urllib.request.urlopen(f"http://{rqlite_host}:{rqlite_http_port}/status", timeout=5) as resp:
            status_before = json.load(resp)
    except Exception as e:
        print(f"  WARNING: could not get rqlite status: {e}")
        status_before = None

    # Send requests
    success, elapsed = send_requests(host, port, endpoint, payload, n_requests)
    print(f"  Sent {n_requests} requests, {success} succeeded, {elapsed:.1f}s")

    # Get rqlite status after
    try:
        with urllib.request.urlopen(f"http://{rqlite_host}:{rqlite_http_port}/status", timeout=5) as resp:
            status_after = json.load(resp)
    except Exception:
        status_after = None

    if status_before and status_after:
        store_b = status_before.get("store", {}).get("raft", {})
        store_a = status_after.get("store", {}).get("raft", {})

        commits_before = int(store_b.get("commit_index", 0))
        commits_after = int(store_a.get("commit_index", 0))
        delta_commits = commits_after - commits_before

        applied_before = int(store_b.get("applied_index", 0))
        applied_after = int(store_a.get("applied_index", 0))
        delta_applied = applied_after - applied_before

        print(f"\n  Raft log delta:")
        print(f"    commit_index: {commits_before} -> {commits_after} (delta={delta_commits})")
        print(f"    applied_index: {applied_before} -> {applied_after} (delta={delta_applied})")
        print(f"    Raft commits per request: {delta_commits / n_requests:.1f}")

    # Find rqlited PID and measure proc/io
    pid_result = _ssh(rqlite_host, "pgrep -f rqlited | head -1")
    rqlite_pid = pid_result.stdout.strip()
    if rqlite_pid:
        before_io = get_proc_io(rqlite_host, int(rqlite_pid))
        send_requests(host, port, endpoint, payload, n_requests)
        time.sleep(0.5)
        after_io = get_proc_io(rqlite_host, int(rqlite_pid))
        if before_io and after_io:
            write_delta = after_io['write_bytes'] - before_io['write_bytes']
            syscw_delta = after_io['syscw'] - before_io['syscw']
            print(f"\n  rqlited process I/O delta ({n_requests} requests):")
            print(f"    write_bytes:   {write_delta:>12} ({write_delta / n_requests:.0f} per request)")
            print(f"    syscw (writes):{syscw_delta:>12} ({syscw_delta / n_requests:.1f} per request)")


def measure_openebs(host, port, endpoint, payload, n_requests, worker_host):
    """Measure OpenEBS replication cost."""
    print(f"\n=== OpenEBS Replication Cost ({n_requests} requests) ===")

    # Get block device I/O stats before
    iostat_before = _ssh(worker_host, "cat /proc/diskstats | grep nvme")

    # Send requests
    success, elapsed = send_requests(host, port, endpoint, payload, n_requests)
    print(f"  Sent {n_requests} requests, {success} succeeded, {elapsed:.1f}s")
    time.sleep(1)

    # Get block device I/O stats after
    iostat_after = _ssh(worker_host, "cat /proc/diskstats | grep nvme")

    # Parse diskstats: field 10 = sectors written (512 bytes each)
    def parse_diskstats(output, device="nvme0n1"):
        for line in output.stdout.splitlines():
            parts = line.split()
            if len(parts) >= 14 and device in parts[2]:
                return int(parts[9])  # sectors written
        return 0

    sectors_before = parse_diskstats(iostat_before)
    sectors_after = parse_diskstats(iostat_after)
    delta_sectors = sectors_after - sectors_before
    delta_bytes = delta_sectors * 512

    print(f"\n  NVMe disk write delta:")
    print(f"    sectors written: {delta_sectors}")
    print(f"    bytes written: {delta_bytes:>12} ({delta_bytes / n_requests:.0f} per request)")

    # Find the app container and measure its proc/io
    container_name, container_pid = get_container_pid(worker_host, "synth-workload")
    if not container_pid:
        container_name, container_pid = get_container_pid(worker_host, "workload")
    if container_pid:
        before_io = get_proc_io(worker_host, container_pid)
        send_requests(host, port, endpoint, payload, n_requests)
        time.sleep(0.5)
        after_io = get_proc_io(worker_host, container_pid)
        if before_io and after_io:
            write_delta = after_io['write_bytes'] - before_io['write_bytes']
            syscw_delta = after_io['syscw'] - before_io['syscw']
            print(f"\n  App container I/O delta ({n_requests} requests):")
            print(f"    write_bytes:   {write_delta:>12} ({write_delta / n_requests:.0f} per request)")
            print(f"    syscw (writes):{syscw_delta:>12} ({syscw_delta / n_requests:.1f} per request)")


# ── Main ──────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--system", required=True, choices=["xdn", "rqlite", "openebs"],
                   help="System to measure")
    p.add_argument("--host", required=True, help="Target host for requests")
    p.add_argument("--port", type=int, required=True, help="Target port")
    p.add_argument("--service", default=None, help="XDN service name (for XDN header)")
    p.add_argument("--endpoint", default="/workload", help="HTTP endpoint")
    p.add_argument("--payload", default='{"txns":1,"ops":1,"write_size":100}',
                   help="JSON payload")
    p.add_argument("--n", type=int, default=100, help="Number of requests (default: 100)")
    p.add_argument("--rqlite-host", default="10.10.1.1",
                   help="rqlite leader host (for status API)")
    p.add_argument("--rqlite-http-port", type=int, default=4001,
                   help="rqlite HTTP port")
    p.add_argument("--worker-host", default="10.10.1.1",
                   help="OpenEBS worker host (for disk stats)")
    p.add_argument("--sweep-ops", action="store_true",
                   help="Sweep ops=1,2,5,10,20 and measure cost per ops value")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()

    if args.sweep_ops:
        ops_values = [1, 2, 5, 10, 20]
        print("=" * 60)
        print(f"Replication Cost Sweep: system={args.system}")
        print(f"  Sweeping ops={ops_values} with autocommit=true")
        print("=" * 60)

        for ops in ops_values:
            payload = json.dumps({"txns": 1, "ops": ops, "write_size": 100, "autocommit": True})
            print(f"\n{'='*60}")
            print(f"  ops={ops} (autocommit)")
            print(f"{'='*60}")

            if args.system == "xdn":
                measure_xdn(args.host, args.port, args.service, args.endpoint,
                            payload, args.n)
            elif args.system == "rqlite":
                measure_rqlite(args.host, args.port, args.endpoint, payload,
                               args.n, args.rqlite_host, args.rqlite_http_port)
            elif args.system == "openebs":
                measure_openebs(args.host, args.port, args.endpoint, payload,
                                args.n, args.worker_host)
    else:
        print("=" * 60)
        print(f"Replication Cost: system={args.system}")
        print(f"  host={args.host}:{args.port}")
        print(f"  endpoint={args.endpoint}")
        print(f"  payload={args.payload}")
        print(f"  n_requests={args.n}")
        print("=" * 60)

        if args.system == "xdn":
            measure_xdn(args.host, args.port, args.service, args.endpoint,
                        args.payload, args.n)
        elif args.system == "rqlite":
            measure_rqlite(args.host, args.port, args.endpoint, args.payload,
                           args.n, args.rqlite_host, args.rqlite_http_port)
        elif args.system == "openebs":
            measure_openebs(args.host, args.port, args.endpoint, args.payload,
                            args.n, args.worker_host)

    print(f"\n[Done]")
