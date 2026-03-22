#!/usr/bin/env python3
"""
bench_fuselog.py - Orchestrator for C++ vs Rust fuselog performance comparison.

This script:
1. Builds both fuselog binaries
2. Creates tmpfs-backed test directories
3. Mounts C++ fuselog, runs workloads + collector, unmounts
4. Mounts Rust fuselog (single-threaded + multi-threaded), runs same workloads, unmounts
5. Drops page cache between runs
6. Runs N repetitions per configuration, collects medians
7. Outputs CSV results + comparison tables

Usage:
    python3 bench_fuselog.py [FLAGS]

Flags:
    --impl cpp|rust|both       Which implementation to benchmark (default: both)
    --profiles COMMA_LIST      Workload profiles (default: seq-large,rand-small,many-files,create-unlink,mixed)
    --threads COMMA_LIST       Thread counts (default: 1,2,4,8)
    --reps N                   Repetitions per config (default: 5)
    --duration SECS            Per-run duration (default: 10)
    --output-dir DIR           Output directory (default: eval/results/fuselog_bench/)
    --skip-build               Skip building binaries
    --collect-interval-ms MS   Statediff collection interval (default: 500)
"""

import argparse
import csv
import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path
from statistics import median

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CPP_DIR = PROJECT_ROOT / "xdn-fs" / "cpp"
RUST_DIR = PROJECT_ROOT / "xdn-fs" / "rust"
EVAL_DIR = PROJECT_ROOT / "eval"

# Working directories
BENCH_DIR = Path("/tmp/xdn/bench")
MOUNT_DIR = BENCH_DIR / "mount"
STATE_DIR = BENCH_DIR / "state"
SOCKET_PATH = BENCH_DIR / "fuselog.sock"


def run(cmd, check=True, capture=False, timeout=120, **kwargs):
    """Run a shell command with logging."""
    print(f"  $ {cmd}")
    result = subprocess.run(
        cmd, shell=True, check=check,
        capture_output=capture, text=True, timeout=timeout, **kwargs
    )
    return result


def build_binaries(skip_build=False):
    """Build C++ and Rust fuselog binaries."""
    if skip_build:
        print("[BUILD] Skipping build (--skip-build)")
        return

    print("\n[BUILD] Building C++ fuselog...")
    run(f"cd {CPP_DIR} && g++ -Wall fuselogv2.cpp -o fuselog "
        f"-D_FILE_OFFSET_BITS=64 $(pkg-config fuse3 --cflags --libs) "
        f"-pthread -O3 -std=c++11")

    print("[BUILD] Building C++ fuselog-apply...")
    run(f"cd {CPP_DIR} && g++ -Wall fuselog-apply.cpp -o fuselog-apply -O3 -std=c++20")

    print("[BUILD] Building Rust fuselog...")
    run(f"cd {RUST_DIR} && cargo build --release", timeout=300)

    print("[BUILD] All binaries built successfully.")


def setup_dirs():
    """Create working directories."""
    for d in [BENCH_DIR, MOUNT_DIR, STATE_DIR]:
        d.mkdir(parents=True, exist_ok=True)


def cleanup_mount():
    """Force unmount any existing fuselog mount."""
    try:
        run(f"fusermount3 -u {MOUNT_DIR}", check=False)
    except Exception:
        pass
    try:
        run(f"fusermount -u {MOUNT_DIR}", check=False)
    except Exception:
        pass
    # Clean socket
    if SOCKET_PATH.exists():
        SOCKET_PATH.unlink()


def drop_caches():
    """Drop page cache (requires root)."""
    try:
        run("sync && echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null",
            check=False)
    except Exception:
        print("  Warning: failed to drop page caches (needs root)")


def mount_cpp_fuselog():
    """Mount C++ fuselog on MOUNT_DIR."""
    cleanup_mount()

    # Ensure state dir exists for the mount point
    STATE_DIR.mkdir(parents=True, exist_ok=True)

    # Copy state into mount point
    cpp_bin = CPP_DIR / "fuselog"
    env = f"FUSELOG_SOCKET_FILE={SOCKET_PATH}"
    cmd = (f"{env} {cpp_bin} -o allow_other "
           f"-o default_permissions {MOUNT_DIR}")
    run(cmd)
    time.sleep(1)  # Wait for mount

    # Verify mount
    result = run(f"mount | grep {MOUNT_DIR}", check=False, capture=True)
    if MOUNT_DIR.as_posix() not in (result.stdout or ""):
        print("  WARNING: Mount verification failed")
        return False
    return True


def mount_rust_fuselog(multi_threaded=False):
    """Mount Rust fuselog on MOUNT_DIR."""
    cleanup_mount()

    rust_bin = RUST_DIR / "target" / "release" / "fuselog_core"
    mt_flag = "--multi-threaded" if multi_threaded else ""
    env = f"FUSELOG_SOCKET_FILE={SOCKET_PATH}"
    cmd = f"{env} {rust_bin} {mt_flag} {MOUNT_DIR}"
    run(cmd)
    time.sleep(1)

    result = run(f"mount | grep {MOUNT_DIR}", check=False, capture=True)
    if MOUNT_DIR.as_posix() not in (result.stdout or ""):
        print("  WARNING: Mount verification failed")
        return False
    return True


def run_writer(profile, threads, duration):
    """Run bench_fuse_writer and capture output."""
    cmd = (f"cd {EVAL_DIR} && go run bench_fuse_writer.go "
           f"--dir {MOUNT_DIR} --profile {profile} "
           f"--threads {threads} --duration {duration}")
    result = run(cmd, capture=True, timeout=duration + 60)
    return parse_writer_output(result.stdout)


def run_collector(protocol, duration, interval_ms):
    """Run bench_fuse_collector and capture output."""
    cmd = (f"cd {EVAL_DIR} && go run bench_fuse_collector.go "
           f"--socket {SOCKET_PATH} --protocol {protocol} "
           f"--interval-ms {interval_ms} --duration {duration}")
    result = run(cmd, capture=True, timeout=duration + 60)
    return parse_collector_output(result.stdout)


def parse_writer_output(output):
    """Parse bench_fuse_writer output into a dict."""
    result = {}
    for line in output.split("\n"):
        if "Total ops:" in line:
            result["total_ops"] = int(line.split(":")[1].strip())
        elif "Total bytes:" in line:
            result["total_bytes"] = int(line.split(":")[1].strip().split()[0])
        elif "Throughput:" in line:
            result["throughput_mbps"] = float(line.split(":")[1].strip().split()[0])
        elif "Ops/sec:" in line:
            result["ops_per_sec"] = float(line.split(":")[1].strip())
        elif "Latency p50:" in line:
            result["lat_p50_us"] = parse_duration_us(line.split(":")[1].strip())
        elif "Latency p95:" in line:
            result["lat_p95_us"] = parse_duration_us(line.split(":")[1].strip())
        elif "Latency p99:" in line:
            result["lat_p99_us"] = parse_duration_us(line.split(":")[1].strip())
    return result


def parse_collector_output(output):
    """Parse bench_fuse_collector output into a dict."""
    result = {}
    for line in output.split("\n"):
        if "Total payload:" in line:
            result["total_payload_bytes"] = int(line.split(":")[1].strip().split()[0])
        elif "Avg payload:" in line:
            result["avg_payload_bytes"] = int(line.split(":")[1].strip().split()[0])
        elif "Capture lat p50:" in line:
            result["capture_lat_p50_us"] = parse_duration_us(line.split(":")[1].strip())
        elif "Capture lat p95:" in line:
            result["capture_lat_p95_us"] = parse_duration_us(line.split(":")[1].strip())
    return result


def parse_duration_us(s):
    """Parse a Go duration string (e.g., '1.234ms', '567µs', '12.3s') to microseconds."""
    s = s.strip()
    if s.endswith("µs") or s.endswith("us"):
        return float(re.sub(r'[µu]s$', '', s))
    elif s.endswith("ms"):
        return float(s[:-2]) * 1000
    elif s.endswith("s"):
        return float(s[:-1]) * 1_000_000
    elif s.endswith("ns"):
        return float(s[:-2]) / 1000
    return 0


def run_benchmark(impl_name, profile, threads, duration, collect_interval_ms, protocol):
    """Run a single benchmark iteration (writer + collector in parallel).

    Returns a dict with writer and collector results.
    """
    import threading

    writer_result = {}
    collector_result = {}

    def writer_thread():
        nonlocal writer_result
        writer_result = run_writer(profile, threads, duration)

    def collector_thread():
        nonlocal collector_result
        collector_result = run_collector(protocol, duration, collect_interval_ms)

    t_writer = threading.Thread(target=writer_thread)
    t_collector = threading.Thread(target=collector_thread)

    t_collector.start()
    time.sleep(0.5)  # Let collector connect first
    t_writer.start()

    t_writer.join()
    t_collector.join()

    combined = {
        "impl": impl_name,
        "profile": profile,
        "threads": threads,
    }
    combined.update({f"w_{k}": v for k, v in writer_result.items()})
    combined.update({f"c_{k}": v for k, v in collector_result.items()})
    return combined


def run_full_benchmark(args):
    """Run the full benchmark matrix."""
    setup_dirs()
    build_binaries(args.skip_build)

    profiles = args.profiles.split(",")
    thread_counts = [int(t) for t in args.threads.split(",")]
    impls = []

    if args.impl in ("cpp", "both"):
        impls.append(("cpp", "cpp", lambda: mount_cpp_fuselog()))
    if args.impl in ("rust", "both"):
        impls.append(("rust-st", "rust", lambda: mount_rust_fuselog(False)))
        impls.append(("rust-mt", "rust", lambda: mount_rust_fuselog(True)))

    all_results = []
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    for impl_name, protocol, mount_fn in impls:
        for profile in profiles:
            for threads in thread_counts:
                # Skip thread scaling for mixed (fixed 7 threads)
                if profile == "mixed" and threads != 1:
                    continue

                print(f"\n{'='*60}")
                print(f"[BENCH] {impl_name} | {profile} | threads={threads}")
                print(f"{'='*60}")

                rep_results = []
                for rep in range(args.reps):
                    print(f"\n  [Rep {rep+1}/{args.reps}]")

                    drop_caches()

                    # Mount
                    if not mount_fn():
                        print(f"  ERROR: Failed to mount {impl_name}")
                        continue

                    try:
                        result = run_benchmark(
                            impl_name, profile, threads,
                            args.duration, args.collect_interval_ms, protocol
                        )
                        rep_results.append(result)
                        print(f"    throughput={result.get('w_throughput_mbps', 'N/A')} MB/s, "
                              f"ops={result.get('w_ops_per_sec', 'N/A')}/s")
                    except Exception as e:
                        print(f"  ERROR: {e}")
                    finally:
                        cleanup_mount()

                # Compute medians across repetitions
                if rep_results:
                    median_result = compute_median(rep_results)
                    all_results.append(median_result)

    # Write results
    csv_path = output_dir / "fuselog_bench_results.csv"
    write_csv(all_results, csv_path)

    json_path = output_dir / "fuselog_bench_results.json"
    with open(json_path, "w") as f:
        json.dump(all_results, f, indent=2)

    # Print summary table
    print_summary_table(all_results)

    print(f"\nResults saved to {output_dir}/")


def compute_median(results):
    """Compute median of numeric fields across repetitions."""
    if not results:
        return {}
    median_result = {
        "impl": results[0]["impl"],
        "profile": results[0]["profile"],
        "threads": results[0]["threads"],
    }
    numeric_keys = [k for k in results[0] if k not in ("impl", "profile", "threads")]
    for key in numeric_keys:
        vals = [r[key] for r in results if key in r and r[key] is not None]
        if vals:
            median_result[key] = median(vals)
    return median_result


def write_csv(results, path):
    """Write results to CSV."""
    if not results:
        return
    keys = list(results[0].keys())
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for r in results:
            writer.writerow(r)


def print_summary_table(results):
    """Print a formatted comparison table."""
    if not results:
        return

    print(f"\n{'='*80}")
    print("BENCHMARK SUMMARY (median of repetitions)")
    print(f"{'='*80}")
    print(f"{'Impl':<10} {'Profile':<15} {'Threads':>7} {'MB/s':>10} {'Ops/s':>10} "
          f"{'p50(us)':>10} {'p95(us)':>10} {'CapP50(us)':>12}")
    print("-" * 80)

    for r in results:
        print(f"{r.get('impl',''):<10} {r.get('profile',''):<15} "
              f"{r.get('threads',''):>7} "
              f"{r.get('w_throughput_mbps', 0):>10.2f} "
              f"{r.get('w_ops_per_sec', 0):>10.1f} "
              f"{r.get('w_lat_p50_us', 0):>10.1f} "
              f"{r.get('w_lat_p95_us', 0):>10.1f} "
              f"{r.get('c_capture_lat_p50_us', 0):>12.1f}")


def main():
    parser = argparse.ArgumentParser(description="Fuselog benchmark orchestrator")
    parser.add_argument("--impl", default="both", choices=["cpp", "rust", "both"],
                        help="Which implementation to benchmark")
    parser.add_argument("--profiles", default="seq-large,rand-small,many-files,create-unlink,mixed",
                        help="Comma-separated workload profiles")
    parser.add_argument("--threads", default="1,2,4,8",
                        help="Comma-separated thread counts")
    parser.add_argument("--reps", type=int, default=5,
                        help="Repetitions per configuration")
    parser.add_argument("--duration", type=int, default=10,
                        help="Per-run duration in seconds")
    parser.add_argument("--output-dir", default=str(EVAL_DIR / "results" / "fuselog_bench"),
                        help="Output directory")
    parser.add_argument("--skip-build", action="store_true",
                        help="Skip building binaries")
    parser.add_argument("--collect-interval-ms", type=int, default=500,
                        help="Statediff collection interval in ms")

    args = parser.parse_args()
    run_full_benchmark(args)


if __name__ == "__main__":
    main()
