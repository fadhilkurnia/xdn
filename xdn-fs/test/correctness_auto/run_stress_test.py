#!/usr/bin/env python3
"""
Deterministic-ish concurrency stress test for fuselog-family
implementations, targeting the fid-table / statediff-stack tearing
class of race.

Scope: this verifies fuselogv2 itself -- the Tier-1 invariant that every
fid referenced by a statediff resolves in that same harvested batch's own
file table. It does NOT replay batches through fuselog-apply; that's a
separate concern this harness deliberately doesn't cover.

Exit code 0 = pass (invariant held for the whole run).
Exit code 1 = fail (Tier-1 invariant violated, or a parse/protocol error
              occurred against the harvest socket).

Usage:
    python3 run_stress_test.py \
        --fuselog-binary /usr/local/bin/fuselog \
        --duration 15

See README.md for the full design rationale.
"""

from __future__ import annotations

import argparse
import sys
import threading
import time

from driver import FuselogV2Driver
from harvester import Harvester
from workload import WorkloadGenerator


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--fuselog-binary", required=True)
    ap.add_argument("--duration", type=float, default=15.0,
                     help="Max wall-clock seconds for the write/harvest "
                          "phase (default: 15)")
    ap.add_argument("--max-iterations", type=int, default=200_000,
                     help="Max total writes across all writer threads "
                          "(default: 200000) -- whichever limit hits "
                          "first (duration or this) stops the run")
    ap.add_argument("--num-hot-threads", type=int, default=8)
    ap.add_argument("--files-per-hot-thread", type=int, default=4)
    ap.add_argument("--num-cold-files", type=int, default=5)
    ap.add_argument("--cold-write-interval", type=float, default=0.05)
    ap.add_argument("--dump-fuselog-log", action="store_true",
                     help="On failure, print fuselogv2's full stdout/stderr "
                          "log to the terminal (default: just print the "
                          "log file's path -- it's always saved to disk "
                          "regardless of this flag)")
    ap.add_argument("--always-clean", action="store_true",
                     help="Clean up the mount/work directory even on "
                          "failure (default: preserve it on failure for "
                          "inspection, clean on pass)")
    ap.add_argument("--work-root", default=None,
                     help="Directory for the mount/backing dir "
                          "(default: a fresh temp dir)")
    args = ap.parse_args()

    driver = FuselogV2Driver(
        fuselog_binary=args.fuselog_binary,
        work_root=args.work_root,
    )

    print(f"[stress] starting fuselogv2 ({args.fuselog_binary}) ...")
    handle = driver.start()
    print(f"[stress] mounted at {handle.writable_path}, "
          f"harvest socket at {handle.harvest_socket_path}")

    harvester = Harvester(handle.harvest_socket_path, handle.parser)
    workload = WorkloadGenerator(
        mount_path=handle.writable_path,
        num_hot_threads=args.num_hot_threads,
        files_per_hot_thread=args.files_per_hot_thread,
        num_cold_files=args.num_cold_files,
        cold_write_interval=args.cold_write_interval,
    )

    print(f"[stress] starting {args.num_hot_threads} hot writer threads "
          f"+ 1 cold writer thread, harvester racing freely ...")
    workload.start()

    harvester_thread = threading.Thread(target=harvester.run_loop, daemon=True)
    harvester_thread.start()

    deadline = time.time() + args.duration
    overall_ok = True

    try:
        while time.time() < deadline:
            if workload.total_iterations() >= args.max_iterations:
                print(f"[stress] reached max-iterations "
                      f"({args.max_iterations}), stopping early")
                break
            if not harvester_thread.is_alive():
                print("[stress] harvester stopped itself (violation "
                      "found), ending run early")
                break
            time.sleep(0.1)
    finally:
        print("[stress] stopping harvester and tearing down the mount "
              "(before waiting on writer threads -- a wedged daemon can "
              "leave a writer thread stuck in an uninterruptible pwrite() "
              "otherwise) ...")
        harvester.stop()
        driver.stop()
        print("[stress] stopping workload ...")
        workload.stop()
        harvester_thread.join(timeout=5)

    total_iters = workload.total_iterations()
    print(f"[stress] workload complete: {total_iters} total writes issued, "
          f"{harvester.stats.batches_harvested} batches harvested, "
          f"{harvester.stats.statediffs_seen} statediffs seen")

    if harvester.stats.violations:
        overall_ok = False
        print(f"\n[FAIL] Tier-1 invariant violated "
              f"({len(harvester.stats.violations)} violation(s)):")
        for v in harvester.stats.violations[:20]:
            print(f"    batch={v.batch_seq} kind={v.kind} detail={v.detail}")
        if len(harvester.stats.violations) > 20:
            print(f"    ... and {len(harvester.stats.violations) - 20} more")
        if args.dump_fuselog_log:
            print("\n[stress] fuselogv2 process output at time of failure:")
            print(driver.process_output())
        else:
            print(f"\n[stress] fuselogv2 log preserved at: {driver.log_path}")
    else:
        print("\n[ok] Tier-1: no invariant violations across "
              f"{harvester.stats.batches_harvested} batches")

    should_clean = overall_ok or args.always_clean
    if should_clean:
        driver.cleanup_work_root()
        print("[stress] cleaned up scratch/work directory")
    else:
        print(f"[stress] PRESERVING work root for inspection: "
              f"{driver.work_root}")

    print(f"\n=== RESULT: {'PASS' if overall_ok else 'FAIL'} ===")
    return 0 if overall_ok else 1


if __name__ == "__main__":
    sys.exit(main())
