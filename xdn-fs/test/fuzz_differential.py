#!/usr/bin/env python3
"""End-to-end differential tester for fuselog (Layer 3).

Mounts fuselog on /tmp/xdn-fuselog/A, mirrors random fs ops to a plain dir
/tmp/xdn-fuselog/B, harvests the captured statediff over the unix socket,
replays it via fuselog-apply onto /tmp/xdn-fuselog/C, then walks all three
trees and asserts they're byte-identical.

Three assertions fail differently:
  - A != B   fuselog FUSE handlers diverge from plain POSIX semantics
  - A != C   statediff capture is missing or corrupting data
  - B != C   independent confirmation; helps localize the bug above

Requires:
  - fuselog and fuselog-apply binaries built (./bin/build_xdn_fuselog.sh cpp)
  - fusermount3 in PATH
  - Linux (FUSE)

Run:
  ./xdn-fs/test/fuzz_differential.py                # 1000 ops, time-based seed
  ./xdn-fs/test/fuzz_differential.py --num-ops 100  # quick smoke
  ./xdn-fs/test/fuzz_differential.py --seed 12345   # replay a specific seed
"""

import argparse
import atexit
import errno
import hashlib
import os
import random
import shutil
import signal
import socket
import struct
import subprocess
import sys
import time
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
BASE_DIR = Path("/tmp/xdn-fuselog")
MOUNT_DIR = BASE_DIR / "A"          # fuselog mount (live)
PLAIN_DIR = BASE_DIR / "B"          # plain dir oracle
APPLY_DIR = BASE_DIR / "C"          # fuselog-apply target
SOCKET_PATH = BASE_DIR / "test.sock"
STATEDIFF_FILE = BASE_DIR / "statediff.bin"

FUSELOG_BIN = PROJECT_ROOT / "bin" / "fuselog"
APPLY_BIN = PROJECT_ROOT / "bin" / "fuselog-apply"

PATH_POOL = [f"f{i}" for i in range(8)]
SIZE_CHOICES = [1, 16, 256, 4096, 65536, 262144]

OP_WRITE_NEW = "write_new"
OP_OVERWRITE = "overwrite_at_offset"
OP_EXTEND = "extending_write"
OP_TRUNCATE = "truncate"
OP_UNLINK = "unlink"
OP_RENAME = "rename"


def log(msg):
    print(msg, flush=True)


def ensure_clean_dirs():
    """Recreate BASE_DIR from scratch."""
    if BASE_DIR.exists():
        # If MOUNT_DIR is currently a fuselog mount, unmount first.
        try:
            subprocess.run(["fusermount3", "-u", "-q", str(MOUNT_DIR)],
                           check=False)
        except FileNotFoundError:
            pass
        shutil.rmtree(BASE_DIR, ignore_errors=True)
    for d in (MOUNT_DIR, PLAIN_DIR, APPLY_DIR):
        d.mkdir(parents=True, exist_ok=False)


def start_fuselog():
    """Launch fuselog -f mounted on MOUNT_DIR, return Popen."""
    env = os.environ.copy()
    env["FUSELOG_SOCKET_FILE"] = str(SOCKET_PATH)
    env["FUSELOG_CAPTURE"] = "true"
    env["WRITE_COALESCING"] = "true"
    env["FUSELOG_PRUNE"] = "true"
    env["FUSELOG_COMPRESSION"] = "false"
    fuselog_log = open(BASE_DIR / "fuselog.log", "w")
    proc = subprocess.Popen(
        [str(FUSELOG_BIN), "-f", str(MOUNT_DIR)],
        env=env,
        stdout=fuselog_log,
        stderr=subprocess.STDOUT,
    )
    # Wait for the socket file to appear: proves init() finished.
    for _ in range(100):
        if SOCKET_PATH.exists():
            return proc
        if proc.poll() is not None:
            raise RuntimeError(
                f"fuselog exited early with code {proc.returncode}; "
                f"see {BASE_DIR}/fuselog.log")
        time.sleep(0.05)
    proc.terminate()
    raise RuntimeError("fuselog did not create socket within 5s")


def stop_fuselog(proc):
    """Unmount and wait for the fuselog process to exit."""
    if proc is None or proc.poll() is not None:
        return
    subprocess.run(["fusermount3", "-u", str(MOUNT_DIR)], check=False)
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.terminate()
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            proc.kill()


def harvest_statediff():
    """Send 'g' to fuselog's unix socket, return the V2 payload bytes
    (with the leading 8-byte size header stripped)."""
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(30)
    s.connect(str(SOCKET_PATH))
    s.sendall(b"g")
    # Read 8-byte little-endian size header.
    header = b""
    while len(header) < 8:
        chunk = s.recv(8 - len(header))
        if not chunk:
            raise RuntimeError("connection closed while reading size header")
        header += chunk
    (size,) = struct.unpack("<Q", header)
    payload = b""
    while len(payload) < size:
        chunk = s.recv(min(65536, size - len(payload)))
        if not chunk:
            raise RuntimeError("connection closed while reading payload")
        payload += chunk
    s.close()
    return payload


def apply_op(op, base_dir):
    """Mirror a single op into base_dir (either MOUNT_DIR or PLAIN_DIR).
    Returns None on success, propagates exceptions on failure (which will
    be reported as A!=B at compare time)."""
    op_type = op["type"]
    if op_type == OP_WRITE_NEW:
        path = base_dir / op["path"]
        with open(path, "wb") as f:
            f.write(op["data"])
    elif op_type == OP_OVERWRITE:
        path = base_dir / op["path"]
        fd = os.open(path, os.O_WRONLY)
        try:
            os.pwrite(fd, op["data"], op["offset"])
        finally:
            os.close(fd)
    elif op_type == OP_EXTEND:
        path = base_dir / op["path"]
        fd = os.open(path, os.O_WRONLY)
        try:
            os.pwrite(fd, op["data"], op["offset"])
        finally:
            os.close(fd)
    elif op_type == OP_TRUNCATE:
        path = base_dir / op["path"]
        os.truncate(path, op["size"])
    elif op_type == OP_UNLINK:
        path = base_dir / op["path"]
        os.unlink(path)
    elif op_type == OP_RENAME:
        os.rename(base_dir / op["from"], base_dir / op["to"])
    else:
        raise ValueError(f"unknown op: {op_type}")


def pick_op(rng, files):
    """Pick a random op given current file state (dict: path -> size).
    Returns a fully-realized op dict, or None if no valid op is possible
    (very unlikely with our pool of 8 paths)."""
    op_types = [OP_WRITE_NEW, OP_OVERWRITE, OP_EXTEND, OP_TRUNCATE,
                OP_UNLINK, OP_RENAME]
    # Try a few times to find a valid op given current state.
    for _ in range(20):
        op_type = rng.choice(op_types)
        if op_type == OP_WRITE_NEW:
            free = [p for p in PATH_POOL if p not in files]
            if not free:
                continue
            path = rng.choice(free)
            size = rng.choice(SIZE_CHOICES)
            data = rng.randbytes(size)
            return {"type": op_type, "path": path, "data": data}
        if op_type == OP_OVERWRITE:
            existing = [p for p, sz in files.items() if sz > 0]
            if not existing:
                continue
            path = rng.choice(existing)
            file_sz = files[path]
            length = rng.randint(1, min(file_sz, max(SIZE_CHOICES)))
            offset = rng.randint(0, max(0, file_sz - length))
            data = rng.randbytes(length)
            return {"type": op_type, "path": path,
                    "offset": offset, "data": data}
        if op_type == OP_EXTEND:
            existing = list(files.keys())
            if not existing:
                continue
            path = rng.choice(existing)
            file_sz = files[path]
            # Pick an offset that may overlap; new end > file_sz.
            extra = rng.choice(SIZE_CHOICES)
            offset = rng.randint(0, file_sz)
            length = (file_sz - offset) + extra
            data = rng.randbytes(length)
            return {"type": op_type, "path": path,
                    "offset": offset, "data": data}
        if op_type == OP_TRUNCATE:
            existing = list(files.keys())
            if not existing:
                continue
            path = rng.choice(existing)
            new_size = rng.choice([0] + SIZE_CHOICES)
            return {"type": op_type, "path": path, "size": new_size}
        if op_type == OP_UNLINK:
            existing = list(files.keys())
            if not existing:
                continue
            path = rng.choice(existing)
            return {"type": op_type, "path": path}
        if op_type == OP_RENAME:
            existing = list(files.keys())
            free = [p for p in PATH_POOL if p not in files]
            if not existing or not free:
                continue
            return {"type": op_type, "from": rng.choice(existing),
                    "to": rng.choice(free)}
    return None


def update_state(op, files):
    """Apply op's effect to the in-driver file size map."""
    t = op["type"]
    if t == OP_WRITE_NEW:
        files[op["path"]] = len(op["data"])
    elif t == OP_OVERWRITE:
        # File size unchanged.
        pass
    elif t == OP_EXTEND:
        files[op["path"]] = max(files[op["path"]],
                                op["offset"] + len(op["data"]))
    elif t == OP_TRUNCATE:
        files[op["path"]] = op["size"]
    elif t == OP_UNLINK:
        del files[op["path"]]
    elif t == OP_RENAME:
        files[op["to"]] = files.pop(op["from"])


def op_summary(op):
    """One-line, replay-friendly description of an op (no data bytes)."""
    t = op["type"]
    if t in (OP_WRITE_NEW,):
        return f"{t} path={op['path']} size={len(op['data'])}"
    if t in (OP_OVERWRITE, OP_EXTEND):
        return (f"{t} path={op['path']} offset={op['offset']} "
                f"len={len(op['data'])}")
    if t == OP_TRUNCATE:
        return f"{t} path={op['path']} size={op['size']}"
    if t == OP_UNLINK:
        return f"{t} path={op['path']}"
    if t == OP_RENAME:
        return f"{t} from={op['from']} to={op['to']}"
    return repr(op)


def snapshot_tree(root):
    """Walk root and return dict: relpath -> (mode, size, sha256_hex).
    Symlinks are not in MVP scope; if encountered, recorded as link target."""
    out = {}
    root_str = str(root)
    for dirpath, dirnames, filenames in os.walk(root_str):
        for name in filenames:
            full = os.path.join(dirpath, name)
            rel = os.path.relpath(full, root_str)
            st = os.lstat(full)
            with open(full, "rb") as f:
                h = hashlib.sha256(f.read()).hexdigest()
            out[rel] = (st.st_mode & 0o7777, st.st_size, h)
    return out


def compare_trees(name_a, snap_a, name_b, snap_b):
    """Return list of mismatch descriptions; empty list means match."""
    diffs = []
    keys = set(snap_a) | set(snap_b)
    for k in sorted(keys):
        a = snap_a.get(k)
        b = snap_b.get(k)
        if a != b:
            diffs.append(f"  {k}: {name_a}={a} {name_b}={b}")
    return diffs


def dump_failure(seed, op_log, payload, fuselog_log_path):
    """Save artifacts for offline reproduction."""
    dump_dir = Path(f"/tmp/fuselog-fuzz-fail-{seed}")
    if dump_dir.exists():
        shutil.rmtree(dump_dir)
    dump_dir.mkdir(parents=True)
    with open(dump_dir / "ops.log", "w") as f:
        f.write(f"seed={seed}\n")
        for i, line in enumerate(op_log):
            f.write(f"{i:05d} {line}\n")
    with open(dump_dir / "statediff.bin", "wb") as f:
        f.write(payload)
    for src, name in [
        (MOUNT_DIR, "A"), (PLAIN_DIR, "B"), (APPLY_DIR, "C")
    ]:
        if src.exists():
            shutil.make_archive(str(dump_dir / name), "tar", str(src))
    if Path(fuselog_log_path).exists():
        shutil.copy(fuselog_log_path, dump_dir / "fuselog.log")
    return dump_dir


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=None,
                        help="RNG seed (default: time-based)")
    parser.add_argument("--num-ops", type=int, default=1000,
                        help="Number of random ops (default 1000)")
    parser.add_argument("--keep-on-pass", action="store_true",
                        help="Don't clean up dirs on success")
    args = parser.parse_args()

    if not FUSELOG_BIN.exists() or not APPLY_BIN.exists():
        log(f"error: binaries not found at {FUSELOG_BIN} / {APPLY_BIN}")
        log("       run ./bin/build_xdn_fuselog.sh cpp first")
        return 2

    seed = args.seed if args.seed is not None else int(time.time())
    log(f"==================================================")
    log(f" seed     = {seed}")
    log(f" num_ops  = {args.num_ops}")
    log(f" base_dir = {BASE_DIR}")
    log(f"==================================================")
    rng = random.Random(seed)

    ensure_clean_dirs()
    fuselog_log_path = str(BASE_DIR / "fuselog.log")
    proc = None
    try:
        proc = start_fuselog()
        atexit.register(stop_fuselog, proc)

        files = {}
        op_log = []
        skipped = 0
        for i in range(args.num_ops):
            op = pick_op(rng, files)
            if op is None:
                skipped += 1
                continue
            try:
                apply_op(op, MOUNT_DIR)
                apply_op(op, PLAIN_DIR)
            except Exception as e:
                log(f"op #{i} {op_summary(op)} raised on apply: {e!r}")
                op_log.append(f"FAILED {op_summary(op)} :: {e!r}")
                dump = dump_failure(seed, op_log, b"", fuselog_log_path)
                log(f"failure artifacts: {dump}")
                return 1
            update_state(op, files)
            op_log.append(op_summary(op))

        # Flush kernel page cache for the fuselog mount so all writes have
        # actually been delivered to fuselog_write before we harvest.
        subprocess.run(["sync"], check=True)

        snap_a = snapshot_tree(MOUNT_DIR)
        payload = harvest_statediff()
        with open(STATEDIFF_FILE, "wb") as f:
            f.write(payload)

        stop_fuselog(proc)
        proc = None

        snap_b = snapshot_tree(PLAIN_DIR)

        # fuselog-apply needs trailing slash on target dir.
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
            dump = dump_failure(seed, op_log, payload, fuselog_log_path)
            log(f"failure artifacts: {dump}")
            return 1

        snap_c = snapshot_tree(APPLY_DIR)

        diffs_ab = compare_trees("A", snap_a, "B", snap_b)
        diffs_ac = compare_trees("A", snap_a, "C", snap_c)
        diffs_bc = compare_trees("B", snap_b, "C", snap_c)

        ok = not (diffs_ab or diffs_ac or diffs_bc)
        log(f"--------------------------------------------------")
        log(f" ops applied : {len(op_log)} (skipped {skipped})")
        log(f" payload     : {len(payload)} bytes")
        log(f" A files     : {len(snap_a)}")
        log(f" B files     : {len(snap_b)}")
        log(f" C files     : {len(snap_c)}")
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
            dump = dump_failure(seed, op_log, payload, fuselog_log_path)
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
