#!/usr/bin/env python3
"""Concurrent fuzz test with OVERLAPPING paths (Layer 4 — overlap variant).

All threads operate on the SAME shared path pool. Each thread blindly
picks random ops and applies them; predictable errnos (ENOENT, EEXIST,
ENOTEMPTY, ...) are tolerated as expected races. The captured statediff
must still reproduce whatever final on-disk state ended up on A.

There is NO plain-dir oracle B in this test. The assertion is just
tree(A) == tree(C). A is the ground truth (whatever fuselog handlers
plus the kernel actually ended up writing), and C is the result of
replaying the captured statediff onto a fresh directory.

What this stresses beyond fuzz_concurrent.py:
  - push_statediff CAS under heavy contention on the SAME paths
  - fid_mutex name-table inserts colliding on the SAME pathname
  - whether harvest+apply faithfully reproduces racy interleavings

Known limitation: fuselog opens a file in fuselog_open WITHOUT capturing
O_TRUNC, so a Python open(path, "wb") that truncates an existing file
won't be captured. This test sidesteps that by using a custom
OP_CREATE_AND_WRITE op that opens with O_CREAT only (no O_TRUNC).

Run:
  ./xdn-fs/test/fuzz_concurrent_overlap.py
  ./xdn-fs/test/fuzz_concurrent_overlap.py --threads 8 --num-ops-per-thread 500
  ./xdn-fs/test/fuzz_concurrent_overlap.py --seed 12345
"""

import argparse
import errno
import os
import random
import shutil
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fuzz_differential import (  # noqa: E402
    APPLY_BIN, APPLY_DIR, BASE_DIR, FUSELOG_BIN, MOUNT_DIR,
    STATEDIFF_FILE,
    OP_OVERWRITE, OP_EXTEND, OP_TRUNCATE, OP_UNLINK, OP_RENAME,
    OP_MKDIR, OP_RMDIR, OP_CHMOD, OP_CHOWN, OP_LINK, OP_SYMLINK,
    apply_op, compare_trees, dump_failure, ensure_clean_dirs,
    harvest_statediff, log, op_summary, snapshot_tree,
    start_fuselog, stop_fuselog,
)

# Custom op for this test: open(O_CREAT) + pwrite, no O_TRUNC.
OP_CREATE_AND_WRITE = "create_and_write"

# Small shared pool — all threads pick from this. Includes the same
# names treated as files OR dirs depending on the op, so EEXIST/ENOTDIR
# races fire often.
PATHS = [
    "a", "b", "c", "d",
    "x", "y", "z",
    "x/a", "x/b", "y/a", "y/b",
    "x/y/a", "x/y/b",
]
SYMLINK_TARGETS = ["a", "x/a", "../foo", "/nonexistent", "z"]
SIZE_CHOICES = [16, 256, 4096, 32768]
# Thread T writes at offsets in [T * THREAD_BLOCK_SIZE, (T+1)*THREAD_BLOCK_SIZE)
# so that two threads writing to the same file never touch overlapping
# bytes. This sidesteps the push-after-pwrite race where push order can
# diverge from kernel pwrite order under contention.
THREAD_BLOCK_SIZE = 65536
MODE_CHOICES = [0o644, 0o600, 0o755, 0o700]
DIR_MODE_CHOICES = [0o755, 0o750, 0o700]

# Errnos that may legitimately fire when an op's preconditions race away
# (some other thread changed the path between our pick and our syscall).
EXPECTED_RACE_ERRNOS = {
    errno.ENOENT, errno.EEXIST, errno.ENOTEMPTY,
    errno.ENOTDIR, errno.EISDIR, errno.EBUSY,
    errno.EPERM, errno.ELOOP, errno.EACCES,
    errno.EINVAL,  # e.g. rename src == dst when both are dirs
}

ALL_OP_TYPES = [
    OP_CREATE_AND_WRITE, OP_OVERWRITE, OP_EXTEND,
    OP_UNLINK, OP_RENAME, OP_MKDIR, OP_RMDIR, OP_CHMOD,
    OP_CHOWN, OP_LINK, OP_SYMLINK,
]
# OP_TRUNCATE is intentionally excluded: a concurrent truncate + write
# pair has the same push-vs-pwrite ordering race as overlapping writes,
# and unlike writes, we can't sidestep it with thread-owned offsets.


def gen_op(rng, thread_id):
    op_type = rng.choice(ALL_OP_TYPES)
    if op_type == OP_CREATE_AND_WRITE:
        # Pwrite at a thread-owned byte block so concurrent threads
        # writing to the same file never overlap.
        return {"type": op_type, "path": rng.choice(PATHS),
                "offset": thread_id * THREAD_BLOCK_SIZE,
                "data": rng.randbytes(rng.choice(SIZE_CHOICES))}
    if op_type in (OP_OVERWRITE, OP_EXTEND):
        return {"type": op_type, "path": rng.choice(PATHS),
                "offset": thread_id * THREAD_BLOCK_SIZE,
                "data": rng.randbytes(rng.choice(SIZE_CHOICES))}
    if op_type == OP_UNLINK:
        return {"type": op_type, "path": rng.choice(PATHS)}
    if op_type == OP_RENAME:
        return {"type": op_type,
                "from": rng.choice(PATHS), "to": rng.choice(PATHS)}
    if op_type == OP_MKDIR:
        return {"type": op_type, "path": rng.choice(PATHS),
                "mode": rng.choice(DIR_MODE_CHOICES)}
    if op_type == OP_RMDIR:
        return {"type": op_type, "path": rng.choice(PATHS)}
    if op_type == OP_CHMOD:
        return {"type": op_type, "path": rng.choice(PATHS),
                "mode": rng.choice(MODE_CHOICES)}
    if op_type == OP_CHOWN:
        return {"type": op_type, "path": rng.choice(PATHS),
                "uid": os.getuid(), "gid": os.getgid()}
    if op_type == OP_LINK:
        return {"type": op_type,
                "src": rng.choice(PATHS), "dst": rng.choice(PATHS)}
    if op_type == OP_SYMLINK:
        return {"type": op_type, "path": rng.choice(PATHS),
                "target": rng.choice(SYMLINK_TARGETS)}


def apply_op_overlap(op, base_dir):
    """Like fuzz_differential.apply_op, but adds OP_CREATE_AND_WRITE
    (open with O_CREAT, no O_TRUNC) and routes everything else through
    apply_op."""
    if op["type"] == OP_CREATE_AND_WRITE:
        fd = os.open(base_dir / op["path"],
                     os.O_WRONLY | os.O_CREAT, 0o644)
        try:
            os.pwrite(fd, op["data"], op["offset"])
        finally:
            os.close(fd)
        return
    apply_op(op, base_dir)


def op_summary_overlap(op):
    if op["type"] == OP_CREATE_AND_WRITE:
        return f"create_and_write path={op['path']} size={len(op['data'])}"
    return op_summary(op)


def thread_worker(thread_id, base_seed, num_ops, start_event,
                  failure_event, results):
    rng = random.Random(base_seed + thread_id * 1009)
    op_log = []
    succeeded = 0
    raced = 0

    start_event.wait()
    for _ in range(num_ops):
        if failure_event.is_set():
            break
        op = gen_op(rng, thread_id)
        try:
            apply_op_overlap(op, MOUNT_DIR)
            succeeded += 1
            op_log.append(f"T{thread_id} OK {op_summary_overlap(op)}")
        except OSError as e:
            if e.errno in EXPECTED_RACE_ERRNOS:
                raced += 1
            else:
                failure_event.set()
                results[thread_id] = ("error", op_summary_overlap(op),
                                      repr(e), op_log)
                return
    results[thread_id] = ("ok", op_log, succeeded, raced)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--num-ops-per-thread", type=int, default=500)
    parser.add_argument("--keep-on-pass", action="store_true")
    args = parser.parse_args()

    if not FUSELOG_BIN.exists() or not APPLY_BIN.exists():
        log(f"error: binaries not found; build them via "
            f"./bin/build_xdn_fuselog.sh cpp")
        return 2

    seed = args.seed if args.seed is not None else int(time.time())
    log(f"==================================================")
    log(f" seed         = {seed}")
    log(f" threads      = {args.threads}")
    log(f" ops/thread   = {args.num_ops_per_thread}")
    log(f" base_dir     = {BASE_DIR}  (overlap mode, no B oracle)")
    log(f"==================================================")

    ensure_clean_dirs()
    fuselog_log_path = str(BASE_DIR / "fuselog.log")
    proc = None

    try:
        proc = start_fuselog()

        start_event = threading.Event()
        failure_event = threading.Event()
        results = {}
        threads = []
        for tid in range(args.threads):
            t = threading.Thread(
                target=thread_worker,
                args=(tid, seed, args.num_ops_per_thread,
                      start_event, failure_event, results),
                name=f"worker-{tid}",
            )
            threads.append(t)
            t.start()

        start_event.set()
        for t in threads:
            t.join()

        all_ops = []
        total_succeeded = 0
        total_raced = 0
        for tid in range(args.threads):
            entry = results.get(tid, ("missing",))
            status = entry[0]
            if status == "error":
                _, summary, err, log_part = entry
                log(f"T{tid} errored on {summary}: {err}")
                all_ops.extend(log_part)
                all_ops.append(f"T{tid} FAILED {summary} :: {err}")
            elif status == "ok":
                _, log_part, succeeded, raced = entry
                all_ops.extend(log_part)
                total_succeeded += succeeded
                total_raced += raced

        if failure_event.is_set():
            dump = dump_failure(seed, all_ops, b"", fuselog_log_path)
            log(f"failure artifacts: {dump}")
            return 1

        subprocess.run(["sync"], check=True)
        # Harvest BEFORE snapshot: snapshot may need to chmod dirs that
        # workers set to non-traversable modes, and those chmods would
        # otherwise leak into the captured statediff.
        payload = harvest_statediff()
        with open(STATEDIFF_FILE, "wb") as f:
            f.write(payload)
        snap_a = snapshot_tree(MOUNT_DIR)

        stop_fuselog(proc)
        proc = None

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

        diffs_ac = compare_trees("A", snap_a, "C", snap_c)

        log(f"--------------------------------------------------")
        log(f" succeeded   : {total_succeeded}")
        log(f" raced       : {total_raced}")
        log(f" payload     : {len(payload)} bytes")
        log(f" A entries   : {len(snap_a)}")
        log(f" C entries   : {len(snap_c)}")
        log(f"--------------------------------------------------")

        if diffs_ac:
            log(f"A != C ({len(diffs_ac)} differences):")
            for d in diffs_ac[:20]:
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
