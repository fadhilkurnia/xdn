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
import stat
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

FILE_NAMES = ["a", "b", "c"]            # leaf names for files/symlinks/hardlinks
DIR_NAMES = ["x", "y"]                  # leaf names for directories
MAX_DEPTH = 3                           # "x/y/a" is depth 3
SYMLINK_TARGETS = ["a", "x/a", "x/y/b", "../foo", "/nonexistent", "z"]
SIZE_CHOICES = [1, 16, 256, 4096, 65536, 262144]
MODE_CHOICES = [0o644, 0o600, 0o664, 0o755, 0o750, 0o700]
DIR_MODE_CHOICES = [0o755, 0o750, 0o700, 0o770]

OP_WRITE_NEW = "write_new"
OP_OVERWRITE = "overwrite_at_offset"
OP_EXTEND = "extending_write"
OP_TRUNCATE = "truncate"
OP_UNLINK = "unlink"
OP_RENAME = "rename"
OP_MKDIR = "mkdir"
OP_RMDIR = "rmdir"
OP_CHMOD = "chmod"
OP_CHOWN = "chown"
OP_LINK = "link"
OP_SYMLINK = "symlink"

# Per-entry state shape:
#   {'type': 'file', 'size': int}    # mutable; shared by hardlinks
#   {'type': 'dir'}
#   {'type': 'symlink', 'target': str}


def log(msg):
    print(msg, flush=True)


def ensure_clean_dirs():
    """Recreate BASE_DIR from scratch. Tolerates a stale fuselog mount
    from a previous crashed run."""
    if BASE_DIR.exists():
        # If MOUNT_DIR is still a live fuselog mount, unmount aggressively.
        try:
            subprocess.run(["fusermount3", "-u", "-q", str(MOUNT_DIR)],
                           check=False)
            # Lazy unmount in case the above failed because of EBUSY.
            subprocess.run(["fusermount3", "-u", "-z", "-q", str(MOUNT_DIR)],
                           check=False)
        except FileNotFoundError:
            pass
        shutil.rmtree(BASE_DIR, ignore_errors=True)
        # If rmtree couldn't fully remove BASE_DIR (e.g. lingering mount),
        # fall back to clearing its contents one level at a time.
        if BASE_DIR.exists():
            for child in BASE_DIR.iterdir():
                shutil.rmtree(child, ignore_errors=True)
            try:
                BASE_DIR.rmdir()
            except OSError:
                pass
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    for d in (MOUNT_DIR, PLAIN_DIR, APPLY_DIR):
        d.mkdir(parents=True, exist_ok=True)


def start_fuselog(allow_other=False):
    """Launch fuselog -f mounted on MOUNT_DIR, return Popen.

    `allow_other=True` adds -o allow_other so other UIDs (e.g. a docker
    container running as a different user) can access the mount. The
    host must have `user_allow_other` enabled in /etc/fuse.conf."""
    env = os.environ.copy()
    env["FUSELOG_SOCKET_FILE"] = str(SOCKET_PATH)
    env["FUSELOG_CAPTURE"] = "true"
    env["WRITE_COALESCING"] = "true"
    env["FUSELOG_PRUNE"] = "true"
    env["FUSELOG_COMPRESSION"] = "false"
    # FUSELOG_DISABLE_SIMD is honoured by compute_diff_dispatch; propagate.
    if os.environ.get("FUSELOG_DISABLE_SIMD"):
        env["FUSELOG_DISABLE_SIMD"] = os.environ["FUSELOG_DISABLE_SIMD"]
    fuselog_log = open(BASE_DIR / "fuselog.log", "w")
    cmd = [str(FUSELOG_BIN), "-f"]
    if allow_other:
        cmd += ["-o", "allow_other"]
    cmd.append(str(MOUNT_DIR))
    proc = subprocess.Popen(
        cmd,
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
    (with the leading 8-byte size header stripped). Uses a bytearray
    accumulator and recv_into to avoid O(n^2) concat on large payloads
    (mysql/postgres harvests can be 100+ MB)."""
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(120)
    s.connect(str(SOCKET_PATH))
    s.sendall(b"g")

    header = bytearray(8)
    view = memoryview(header)
    pos = 0
    while pos < 8:
        n = s.recv_into(view[pos:], 8 - pos)
        if n == 0:
            raise RuntimeError("connection closed while reading size header")
        pos += n
    (size,) = struct.unpack("<Q", header)

    payload = bytearray(size)
    view = memoryview(payload)
    pos = 0
    while pos < size:
        n = s.recv_into(view[pos:], min(1 << 20, size - pos))
        if n == 0:
            raise RuntimeError("connection closed while reading payload")
        pos += n
    s.close()
    return bytes(payload)


def apply_op(op, base_dir):
    """Mirror a single op into base_dir (either MOUNT_DIR or PLAIN_DIR).
    Returns None on success, propagates exceptions on failure (which will
    be reported as A!=B at compare time)."""
    op_type = op["type"]
    if op_type == OP_WRITE_NEW:
        path = base_dir / op["path"]
        with open(path, "wb") as f:
            f.write(op["data"])
    elif op_type == OP_OVERWRITE or op_type == OP_EXTEND:
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
    elif op_type == OP_MKDIR:
        os.mkdir(base_dir / op["path"], op["mode"])
    elif op_type == OP_RMDIR:
        os.rmdir(base_dir / op["path"])
    elif op_type == OP_CHMOD:
        # follow_symlinks=True matches fuselog_chmod which calls chmod(2).
        os.chmod(base_dir / op["path"], op["mode"])
    elif op_type == OP_CHOWN:
        # lchown semantics match fuselog_chown. Chown to the current uid/gid
        # so we don't need root; fuselog still emits a CHOWN statediff.
        os.chown(base_dir / op["path"], op["uid"], op["gid"],
                 follow_symlinks=False)
    elif op_type == OP_LINK:
        os.link(base_dir / op["src"], base_dir / op["dst"])
    elif op_type == OP_SYMLINK:
        os.symlink(op["target"], base_dir / op["path"])
    else:
        raise ValueError(f"unknown op: {op_type}")


def files_of_type(entries, t):
    return [p for p, e in entries.items() if e["type"] == t]


def depth_of(path):
    """Depth counting: root '.' is 0, top-level entries are 1."""
    return 0 if path == "." else path.count("/") + 1


def join_path(parent, leaf):
    return leaf if parent == "." else parent + "/" + leaf


def existing_dirs(entries):
    """List of all dir paths, including the synthetic root '.'."""
    return ["."] + files_of_type(entries, "dir")


def pick_new_path(rng, entries, leaf_names):
    """Find a parent dir (anywhere up to MAX_DEPTH - 1) and a leaf name
    such that join(parent, leaf) doesn't already exist. Returns the new
    path or None if all combinations collide."""
    dirs = existing_dirs(entries)
    rng.shuffle(dirs)
    for parent in dirs:
        if depth_of(parent) >= MAX_DEPTH:
            continue
        leafs = list(leaf_names)
        rng.shuffle(leafs)
        for leaf in leafs:
            cand = join_path(parent, leaf)
            if cand not in entries:
                return cand
    return None


def is_empty_dir(entries, dir_path):
    """True if dir_path has no direct or transitive children."""
    if dir_path == ".":
        return False
    prefix = dir_path + "/"
    return not any(p.startswith(prefix) for p in entries)


def pick_op(rng, entries):
    """Pick a random op given current entries state.
    Returns a fully-realized op dict, or None if no valid op is possible."""
    op_types = [OP_WRITE_NEW, OP_OVERWRITE, OP_EXTEND, OP_TRUNCATE,
                OP_UNLINK, OP_RENAME, OP_MKDIR, OP_RMDIR, OP_CHMOD,
                OP_CHOWN, OP_LINK, OP_SYMLINK]
    cur_uid = os.getuid()
    cur_gid = os.getgid()

    for _ in range(40):
        op_type = rng.choice(op_types)
        if op_type == OP_WRITE_NEW:
            new_path = pick_new_path(rng, entries, FILE_NAMES)
            if new_path is None:
                continue
            size = rng.choice(SIZE_CHOICES)
            data = rng.randbytes(size)
            return {"type": op_type, "path": new_path, "data": data}
        if op_type == OP_OVERWRITE:
            existing = [p for p in files_of_type(entries, "file")
                        if entries[p]["size"] > 0]
            if not existing:
                continue
            path = rng.choice(existing)
            file_sz = entries[path]["size"]
            length = rng.randint(1, min(file_sz, max(SIZE_CHOICES)))
            offset = rng.randint(0, max(0, file_sz - length))
            data = rng.randbytes(length)
            return {"type": op_type, "path": path,
                    "offset": offset, "data": data}
        if op_type == OP_EXTEND:
            existing = files_of_type(entries, "file")
            if not existing:
                continue
            path = rng.choice(existing)
            file_sz = entries[path]["size"]
            extra = rng.choice(SIZE_CHOICES)
            offset = rng.randint(0, file_sz)
            length = (file_sz - offset) + extra
            data = rng.randbytes(length)
            return {"type": op_type, "path": path,
                    "offset": offset, "data": data}
        if op_type == OP_TRUNCATE:
            existing = files_of_type(entries, "file")
            if not existing:
                continue
            path = rng.choice(existing)
            new_size = rng.choice([0] + SIZE_CHOICES)
            return {"type": op_type, "path": path, "size": new_size}
        if op_type == OP_UNLINK:
            # Unlink regular files and symlinks. Hard links are also files;
            # unlinking one is fine (the other names persist).
            existing = (files_of_type(entries, "file") +
                        files_of_type(entries, "symlink"))
            if not existing:
                continue
            path = rng.choice(existing)
            return {"type": op_type, "path": path}
        if op_type == OP_RENAME:
            existing = files_of_type(entries, "file")
            new_path = pick_new_path(rng, entries, FILE_NAMES)
            if not existing or new_path is None:
                continue
            return {"type": op_type, "from": rng.choice(existing),
                    "to": new_path}
        if op_type == OP_MKDIR:
            new_path = pick_new_path(rng, entries, DIR_NAMES)
            if new_path is None:
                continue
            return {"type": op_type, "path": new_path,
                    "mode": rng.choice(DIR_MODE_CHOICES)}
        if op_type == OP_RMDIR:
            existing = [p for p in files_of_type(entries, "dir")
                        if is_empty_dir(entries, p)]
            if not existing:
                continue
            return {"type": op_type, "path": rng.choice(existing)}
        if op_type == OP_CHMOD:
            # Skip symlinks: chmod(2) follows symlinks and our symlink
            # targets may not exist as real files.
            candidates = (files_of_type(entries, "file") +
                          files_of_type(entries, "dir"))
            if not candidates:
                continue
            path = rng.choice(candidates)
            mode_pool = (MODE_CHOICES
                         if entries[path]["type"] == "file"
                         else DIR_MODE_CHOICES)
            return {"type": op_type, "path": path,
                    "mode": rng.choice(mode_pool)}
        if op_type == OP_CHOWN:
            candidates = list(entries.keys())
            if not candidates:
                continue
            return {"type": op_type, "path": rng.choice(candidates),
                    "uid": cur_uid, "gid": cur_gid}
        if op_type == OP_LINK:
            srcs = files_of_type(entries, "file")
            new_path = pick_new_path(rng, entries, FILE_NAMES)
            if not srcs or new_path is None:
                continue
            return {"type": op_type, "src": rng.choice(srcs),
                    "dst": new_path}
        if op_type == OP_SYMLINK:
            new_path = pick_new_path(rng, entries, FILE_NAMES)
            if new_path is None:
                continue
            return {"type": op_type, "path": new_path,
                    "target": rng.choice(SYMLINK_TARGETS)}
    return None


def update_state(op, entries):
    """Apply op's effect to the in-driver entries map.
    Hard links share their file-state dict, so size updates through one
    pathname are visible through the other(s)."""
    t = op["type"]
    if t == OP_WRITE_NEW:
        entries[op["path"]] = {"type": "file", "size": len(op["data"])}
    elif t == OP_OVERWRITE:
        pass  # file size unchanged
    elif t == OP_EXTEND:
        e = entries[op["path"]]
        e["size"] = max(e["size"], op["offset"] + len(op["data"]))
    elif t == OP_TRUNCATE:
        entries[op["path"]]["size"] = op["size"]
    elif t == OP_UNLINK:
        del entries[op["path"]]
    elif t == OP_RENAME:
        entries[op["to"]] = entries.pop(op["from"])
    elif t == OP_MKDIR:
        entries[op["path"]] = {"type": "dir"}
    elif t == OP_RMDIR:
        del entries[op["path"]]
    elif t == OP_CHMOD or t == OP_CHOWN:
        pass  # tracked metadata not used by op picker
    elif t == OP_LINK:
        # Share the same dict object so size updates propagate to both names.
        entries[op["dst"]] = entries[op["src"]]
    elif t == OP_SYMLINK:
        entries[op["path"]] = {"type": "symlink", "target": op["target"]}


def op_summary(op):
    """One-line, replay-friendly description of an op (no data bytes)."""
    t = op["type"]
    if t == OP_WRITE_NEW:
        return f"{t} path={op['path']} size={len(op['data'])}"
    if t in (OP_OVERWRITE, OP_EXTEND):
        return (f"{t} path={op['path']} offset={op['offset']} "
                f"len={len(op['data'])}")
    if t == OP_TRUNCATE:
        return f"{t} path={op['path']} size={op['size']}"
    if t in (OP_UNLINK, OP_RMDIR):
        return f"{t} path={op['path']}"
    if t == OP_RENAME:
        return f"{t} from={op['from']} to={op['to']}"
    if t == OP_MKDIR:
        return f"{t} path={op['path']} mode={oct(op['mode'])}"
    if t == OP_CHMOD:
        return f"{t} path={op['path']} mode={oct(op['mode'])}"
    if t == OP_CHOWN:
        return f"{t} path={op['path']} uid={op['uid']} gid={op['gid']}"
    if t == OP_LINK:
        return f"{t} src={op['src']} dst={op['dst']}"
    if t == OP_SYMLINK:
        return f"{t} path={op['path']} target={op['target']}"
    return repr(op)


def snapshot_tree(root):
    """Walk root and return dict: relpath -> (kind, mode, payload).
    For files, payload is (size, sha256_hex). For dirs, payload is None.
    For symlinks, payload is the target string (read via os.readlink).
    Does not follow symlinks.

    Tolerates dirs/files that the test workload set to permissions we
    can't traverse or read: lstat captures the *real* mode first, then
    we chmod to a walkable mode temporarily so we can recurse / hash
    contents. The recorded mode is always the original."""
    out = {}
    root_str = str(root)

    def _scandir(path):
        try:
            return list(os.scandir(path))
        except PermissionError:
            os.chmod(path, 0o755)
            return list(os.scandir(path))

    def _hash(path):
        try:
            with open(path, "rb") as f:
                return hashlib.sha256(f.read()).hexdigest()
        except PermissionError:
            os.chmod(path, 0o644)
            with open(path, "rb") as f:
                return hashlib.sha256(f.read()).hexdigest()

    def _lstat_or_fix(path):
        try:
            return os.lstat(path)
        except PermissionError:
            # The immediate parent might not have x; chmod-and-retry.
            try:
                os.chmod(os.path.dirname(path), 0o755)
            except OSError:
                return None
            try:
                return os.lstat(path)
            except OSError:
                return None

    def _record(path):
        st = _lstat_or_fix(path)
        if st is None:
            rel = os.path.relpath(path, root_str)
            out[rel] = ("inaccessible", None, None)
            return
        rel = os.path.relpath(path, root_str)
        mode = st.st_mode & 0o7777
        if stat.S_ISLNK(st.st_mode):
            out[rel] = ("symlink", mode, os.readlink(path))
        elif stat.S_ISDIR(st.st_mode):
            out[rel] = ("dir", mode, None)
            for entry in _scandir(path):
                _record(entry.path)
        elif stat.S_ISREG(st.st_mode):
            out[rel] = ("file", mode, (st.st_size, _hash(path)))

    for entry in _scandir(root_str):
        _record(entry.path)
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

        entries = {}
        op_log = []
        skipped = 0
        for i in range(args.num_ops):
            op = pick_op(rng, entries)
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
            update_state(op, entries)
            op_log.append(op_summary(op))

        # Flush kernel page cache for the fuselog mount so all writes have
        # actually been delivered to fuselog_write before we harvest.
        subprocess.run(["sync"], check=True)

        # Harvest BEFORE snapshot: snapshot may need to chmod some dirs
        # to traverse them, and those chmods would otherwise leak into
        # the captured statediff.
        payload = harvest_statediff()
        with open(STATEDIFF_FILE, "wb") as f:
            f.write(payload)
        snap_a = snapshot_tree(MOUNT_DIR)

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
