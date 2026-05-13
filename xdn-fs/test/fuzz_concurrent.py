#!/usr/bin/env python3
"""Concurrent fuzz test for fuselog (Layer 4 — first cut).

Same end-to-end shape as fuzz_differential.py, but multi-threaded: N
threads each operate on a disjoint subtree (t0/, t1/, ...) under the
same fuselog mount, mirroring ops to a plain dir B in lockstep per
thread. Disjoint subtrees mean inter-thread ops never touch the same
file, so the final state is deterministic and tree(A) == tree(B) ==
tree(C) remains a well-defined assertion.

What this stresses that the single-threaded test does NOT:
  - push_statediff() Treiber-stack CAS under concurrent prepends
  - fid_mutex-guarded filename_to_fid map under concurrent inserts
  - FUSE kernel serialization / dispatch under concurrent VFS calls

Run:
  ./xdn-fs/test/fuzz_concurrent.py
  ./xdn-fs/test/fuzz_concurrent.py --threads 8 --num-ops-per-thread 1000
  ./xdn-fs/test/fuzz_concurrent.py --seed 12345
"""

import argparse
import os
import random
import shutil
import subprocess
import sys
import threading
import time

# Reuse helpers from the single-threaded driver.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fuzz_differential import (  # noqa: E402
    APPLY_BIN, APPLY_DIR, BASE_DIR, FUSELOG_BIN, MOUNT_DIR,
    PLAIN_DIR, STATEDIFF_FILE,
    apply_op, compare_trees, dump_failure, ensure_clean_dirs,
    harvest_statediff, log, op_summary, pick_op, snapshot_tree,
    start_fuselog, stop_fuselog, update_state,
)


def translate_op(op, prefix):
    """Return a copy of op with every path field rewritten as prefix/<path>.
    The symlink target field is content, not a real path reference, so it
    is left alone."""
    out = dict(op)
    for key in ("path", "from", "to", "src", "dst"):
        if key in out:
            out[key] = prefix + "/" + out[key]
    return out


def thread_worker(thread_id, prefix, base_seed, num_ops, start_event,
                  failure_event, results):
    """One worker thread.

    Maintains its own `entries` dict using paths *relative* to its subtree
    (e.g. "a", "x/b"). At apply time, paths are translated to absolute
    by prepending `prefix/`."""
    rng = random.Random(base_seed + thread_id * 1009)
    entries = {}
    op_log = []

    start_event.wait()

    for _ in range(num_ops):
        if failure_event.is_set():
            results[thread_id] = ("aborted", op_log)
            return
        op = pick_op(rng, entries)
        if op is None:
            continue
        abs_op = translate_op(op, prefix)
        try:
            apply_op(abs_op, MOUNT_DIR)
            apply_op(abs_op, PLAIN_DIR)
        except Exception as e:
            failure_event.set()
            results[thread_id] = ("error", op_summary(abs_op),
                                  repr(e), op_log)
            return
        update_state(op, entries)
        op_log.append(f"T{thread_id} " + op_summary(abs_op))

    results[thread_id] = ("ok", op_log)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=None,
                        help="base RNG seed (default: time-based)")
    parser.add_argument("--threads", type=int, default=4,
                        help="number of concurrent worker threads")
    parser.add_argument("--num-ops-per-thread", type=int, default=500,
                        help="ops to run per thread")
    parser.add_argument("--keep-on-pass", action="store_true",
                        help="Don't clean up dirs on success")
    args = parser.parse_args()

    if not FUSELOG_BIN.exists() or not APPLY_BIN.exists():
        log(f"error: binaries not found at {FUSELOG_BIN} / {APPLY_BIN}")
        log("       run ./bin/build_xdn_fuselog.sh cpp first")
        return 2

    seed = args.seed if args.seed is not None else int(time.time())
    log(f"==================================================")
    log(f" seed         = {seed}")
    log(f" threads      = {args.threads}")
    log(f" ops/thread   = {args.num_ops_per_thread}")
    log(f" base_dir     = {BASE_DIR}")
    log(f"==================================================")

    ensure_clean_dirs()
    fuselog_log_path = str(BASE_DIR / "fuselog.log")
    proc = None

    try:
        proc = start_fuselog()

        # Pre-create each thread's root subtree on A and B before any worker
        # starts. fuselog captures the MKDIR events; apply replays them so
        # C ends up with the same roots.
        prefixes = [f"t{i}" for i in range(args.threads)]
        for prefix in prefixes:
            os.mkdir(MOUNT_DIR / prefix, 0o755)
            os.mkdir(PLAIN_DIR / prefix, 0o755)

        start_event = threading.Event()
        failure_event = threading.Event()
        results = {}
        threads = []
        for tid, prefix in enumerate(prefixes):
            t = threading.Thread(
                target=thread_worker,
                args=(tid, prefix, seed, args.num_ops_per_thread,
                      start_event, failure_event, results),
                name=f"worker-{tid}",
            )
            threads.append(t)
            t.start()

        # Release all workers at the same time.
        start_event.set()
        for t in threads:
            t.join()

        # Collect logs and surface any per-thread errors.
        all_ops = []
        for tid in range(args.threads):
            entry = results.get(tid, ("missing",))
            status = entry[0]
            if status == "error":
                _, summary, err, log_part = entry
                log(f"T{tid} errored on {summary}: {err}")
                all_ops.extend(log_part)
                all_ops.append(f"T{tid} FAILED {summary} :: {err}")
            elif status in ("ok", "aborted"):
                all_ops.extend(entry[1])

        if failure_event.is_set():
            dump = dump_failure(seed, all_ops, b"", fuselog_log_path)
            log(f"failure artifacts: {dump}")
            return 1

        subprocess.run(["sync"], check=True)
        payload = harvest_statediff()
        with open(STATEDIFF_FILE, "wb") as f:
            f.write(payload)
        snap_a = snapshot_tree(MOUNT_DIR)

        stop_fuselog(proc)
        proc = None

        snap_b = snapshot_tree(PLAIN_DIR)

        env = os.environ.copy()
        env["FUSELOG_STATEDIFF_FILE"] = str(STATEDIFF_FILE)
        result = subprocess.run(
            [str(APPLY_BIN), str(APPLY_DIR) + "/"],
            env=env, capture_output=True, text=True,
        )
        if result.returncode != 0:
            log(f"fuselog-apply failed (rc={result.returncode}):")
            log(result.stdout)
            log(result.stderr)
            dump = dump_failure(seed, all_ops, payload, fuselog_log_path)
            log(f"failure artifacts: {dump}")
            return 1

        snap_c = snapshot_tree(APPLY_DIR)

        diffs_ab = compare_trees("A", snap_a, "B", snap_b)
        diffs_ac = compare_trees("A", snap_a, "C", snap_c)
        diffs_bc = compare_trees("B", snap_b, "C", snap_c)

        ok = not (diffs_ab or diffs_ac or diffs_bc)
        log(f"--------------------------------------------------")
        log(f" total ops    : {len(all_ops)}")
        log(f" payload      : {len(payload)} bytes")
        log(f" A entries    : {len(snap_a)}")
        log(f" B entries    : {len(snap_b)}")
        log(f" C entries    : {len(snap_c)}")
        log(f"--------------------------------------------------")

        if not ok:
            if diffs_ab:
                log(f"A != B ({len(diffs_ab)} differences):")
                for d in diffs_ab[:10]:
                    log(d)
            if diffs_ac:
                log(f"A != C ({len(diffs_ac)} differences):")
                for d in diffs_ac[:10]:
                    log(d)
            if diffs_bc:
                log(f"B != C ({len(diffs_bc)} differences):")
                for d in diffs_bc[:10]:
                    log(d)
            dump = dump_failure(seed, all_ops, payload, fuselog_log_path)
            log(f"FAIL  seed={seed}  artifacts={dump}")
            return 1

        log(f"PASS  seed={seed}")
        if not args.keep_on_pass:
            shutil.rmtree(BASE_DIR, ignore_errors=True)
        return 0

    finally:
        stop_fuselog(proc)


if __name__ == "__main__":
    sys.exit(main())
