#!/usr/bin/env python3
"""Chooses which state-diff capturer the fuzz harnesses run against.

The Layer 3/4/5 harnesses were written around a fuselog FUSE mount, but
everything they do apart from starting and stopping it is mechanism-agnostic.
That mechanism-specific part lives here, so the same harnesses can also drive
the eBPF VFS capturer. The backend is picked with CAPTURE_BACKEND:

    fuselog (default)  FUSE mount; fuselog + fuselog-apply
    ebpf               VFS probes; statediff_vfs + statediff_apply_vfs

Each backend class below documents its own requirements and what it cannot
capture.
"""

from __future__ import annotations

import os
import signal
import socket
import subprocess
import time
from abc import ABC, abstractmethod
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent

FUSELOG = "fuselog"
EBPF = "ebpf"

# Mirrors fuzz_differential's OP_* string constants. Spelled as literals
# rather than imported because fuzz_differential imports this module, and a
# circular import would be the only thing gained.
EBPF_UNSUPPORTED_OPS = frozenset({"chmod", "chown", "link", "symlink"})


def backend_name() -> str:
    """The backend selected for this run, lowercased."""
    return os.environ.get("CAPTURE_BACKEND", FUSELOG).strip().lower()


def _log_tail(log_path, limit=3000):
    """Last `limit` characters of a capturer log, for failure messages.

    A recorder that dies during startup puts the reason here and nowhere else.
    Pointing at the path is not enough under CI, where reading it afterwards
    means downloading a build artifact, so the text is inlined into the error."""
    if not log_path:
        return ""
    try:
        with open(log_path, errors="replace") as f:
            text = f.read()
    except OSError as e:
        return f"\n--- {log_path} could not be read: {e} ---"
    if not text.strip():
        return f"\n--- {log_path} is empty ---"
    if len(text) > limit:
        text = text[-limit:]
    return f"\n--- tail of {log_path} ---\n{text}"


def _wait_for_socket(path: Path, proc: subprocess.Popen, what: str,
                     log_path: str | None, timeout: float = 10.0):
    """Block until `path` accepts a connection, or the process dies.

    Existence of the socket inode is not enough on its own: it appears
    between bind() and listen(), so a connect() is the only signal that the
    server is actually ready to answer a harvest."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if path.exists():
            try:
                s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                s.settimeout(0.2)
                s.connect(str(path))
                s.close()
                return proc
            except OSError:
                pass
        if proc.poll() is not None:
            raise RuntimeError(
                f"{what} exited early with code {proc.returncode} before its "
                f"harvest socket was ready{_log_tail(log_path)}")
        time.sleep(0.05)
    proc.terminate()
    raise RuntimeError(
        f"{what} did not open a usable harvest socket at {path} "
        f"within {timeout}s{_log_tail(log_path)}")


class CaptureBackend(ABC):
    """One capture mechanism, from the harness's point of view."""

    name: str = ""
    # True when the target directory is a mount point that must be unmounted.
    is_mount: bool = False
    # Ops the harness must not generate because this backend cannot see them.
    unsupported_ops: frozenset = frozenset()

    @property
    @abstractmethod
    def capture_bin(self) -> Path:
        ...

    @property
    @abstractmethod
    def apply_bin(self) -> Path:
        ...

    @abstractmethod
    def start(self, target_dir, socket_path, log_path,
              allow_other: bool = False) -> subprocess.Popen:
        """Begin capturing mutations under target_dir, serving harvests on
        socket_path. Returns once a harvest would succeed."""

    @abstractmethod
    def stop(self, proc: subprocess.Popen, target_dir) -> None:
        """Stop capturing and reap the process. Must tolerate a process that
        already died on its own."""

    @abstractmethod
    def run_apply(self, statediff_file, apply_dir) -> subprocess.CompletedProcess:
        """Replay a harvested batch onto apply_dir."""

    def force_cleanup(self, target_dir) -> None:
        """Best-effort teardown of state left by a previously crashed run."""

    def preflight(self) -> str | None:
        """Return a human-readable reason this backend cannot run here, or
        None if it can."""
        if not self.capture_bin.exists() or not self.apply_bin.exists():
            return (f"binaries not found at {self.capture_bin} / "
                    f"{self.apply_bin}")
        return None


class FuselogBackend(CaptureBackend):
    """The original backend: a live fuselog (or fusenode) FUSE mount.

    FUSELOG_BIN / FUSELOG_APPLY_BIN keep working exactly as before -- they are
    what lets this same harness A/B the high-level `fuselog` against the
    low-level `fusenode` recorder without forking op-gen or oracle logic.
    """

    name = FUSELOG
    is_mount = True
    unsupported_ops = frozenset()

    @property
    def capture_bin(self) -> Path:
        return Path(os.environ.get("FUSELOG_BIN", PROJECT_ROOT / "bin" / "fuselog"))

    @property
    def apply_bin(self) -> Path:
        return Path(os.environ.get("FUSELOG_APPLY_BIN",
                                   PROJECT_ROOT / "bin" / "fuselog-apply"))

    def start(self, target_dir, socket_path, log_path, allow_other=False):
        env = os.environ.copy()
        env["FUSELOG_SOCKET_FILE"] = str(socket_path)
        env["FUSELOG_CAPTURE"] = "true"
        env["WRITE_COALESCING"] = "true"
        env["FUSELOG_PRUNE"] = "true"
        env["FUSELOG_COMPRESSION"] = "false"
        # FUSELOG_DISABLE_SIMD is honoured by compute_diff_dispatch; propagate.
        if os.environ.get("FUSELOG_DISABLE_SIMD"):
            env["FUSELOG_DISABLE_SIMD"] = os.environ["FUSELOG_DISABLE_SIMD"]

        log_file = open(log_path, "w")
        cmd = [str(self.capture_bin), "-f"]
        if allow_other:
            # Lets other UIDs (e.g. a docker container running as a different
            # user) reach the mount. Needs user_allow_other in /etc/fuse.conf.
            cmd += ["-o", "allow_other"]
        cmd.append(str(target_dir))
        proc = subprocess.Popen(cmd, env=env, stdout=log_file,
                                stderr=subprocess.STDOUT)
        # Waiting for the socket proves fuselog's init() finished.
        return _wait_for_socket(Path(socket_path), proc, "fuselog",
                                str(log_path), timeout=5.0)

    def stop(self, proc, target_dir):
        if proc is None or proc.poll() is not None:
            return
        subprocess.run(["fusermount3", "-u", str(target_dir)], check=False)
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.terminate()
            try:
                proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                proc.kill()

    def force_cleanup(self, target_dir):
        try:
            subprocess.run(["fusermount3", "-u", "-q", str(target_dir)],
                           check=False)
            # Lazy unmount in case the above failed because of EBUSY.
            subprocess.run(["fusermount3", "-u", "-z", "-q", str(target_dir)],
                           check=False)
        except FileNotFoundError:
            pass

    def run_apply(self, statediff_file, apply_dir):
        env = os.environ.copy()
        env["FUSELOG_STATEDIFF_FILE"] = str(statediff_file)
        # fuselog-apply needs a trailing slash on the target dir.
        return subprocess.run([str(self.apply_bin), str(apply_dir) + "/"],
                              env=env, capture_output=True, text=True)


class EbpfBackend(CaptureBackend):
    """The eBPF VFS capturer: statediff_vfs + statediff_apply_vfs.

    Nothing is mounted. statediff_vfs attaches fentry/fexit/kprobe handlers to
    the VFS layer and filters events to the subtree rooted at target_dir, so
    the harness writes to an ordinary directory and the capturer observes
    those writes out of band.
    """

    name = EBPF
    is_mount = False
    unsupported_ops = EBPF_UNSUPPORTED_OPS

    @property
    def capture_bin(self) -> Path:
        return Path(os.environ.get(
            "EBPF_BIN", PROJECT_ROOT / "experiments" / "eBPF" / "statediff_vfs"))

    @property
    def apply_bin(self) -> Path:
        return Path(os.environ.get(
            "EBPF_APPLY_BIN",
            PROJECT_ROOT / "experiments" / "eBPF" / "statediff_apply_vfs"))

    def preflight(self):
        missing = super().preflight()
        if missing:
            return missing
        if os.geteuid() != 0:
            return ("the eBPF backend must run as root: loading BPF programs "
                    "needs privilege, the harvest socket is created 0600 and "
                    "root-owned, and statediff_vfs exits in socket mode once "
                    "it is reparented, so it cannot be launched via sudo from "
                    "an unprivileged harness -- re-run the whole harness "
                    "under sudo")
        if not Path("/sys/kernel/btf/vmlinux").exists():
            return ("/sys/kernel/btf/vmlinux is missing: this kernel has no "
                    "BTF, so the capturer cannot load")
        return None

    def start(self, target_dir, socket_path, log_path, allow_other=False):
        # allow_other is a FUSE mount option; there is no mount here, and any
        # UID can already reach an ordinary directory, so it is a no-op.
        del allow_other

        cmd = [str(self.capture_bin)]
        # mmap stores are invisible without this, which matters for anything
        # that maps its files -- SQLite's shm/WAL-index above all. Off by
        # default in the binary; on by default here because the L5 database
        # harnesses need it and it costs nothing for workloads that never mmap.
        if os.environ.get("EBPF_MMAP_SNAPSHOT", "1") not in ("0", "false", ""):
            cmd.append("--mmap-snapshot")
        cmd += ["--socket", str(socket_path), str(target_dir)]

        log_file = open(log_path, "w")
        # No sudo: we are already root (preflight enforces it) and must stay
        # the capturer's direct parent, or socket mode will stop on reparenting.
        proc = subprocess.Popen(cmd, stdout=log_file, stderr=subprocess.STDOUT)
        # Attaching ~20 probes and loading the skeleton takes appreciably
        # longer than a FUSE mount, so allow more headroom than fuselog gets.
        return _wait_for_socket(Path(socket_path), proc, "statediff_vfs",
                                str(log_path), timeout=30.0)

    def stop(self, proc, target_dir):
        del target_dir  # nothing is mounted
        if proc is None or proc.poll() is not None:
            return
        # SIGINT is the documented way to stop a capture cleanly; SIGKILL
        # would skip probe detach.
        proc.send_signal(signal.SIGINT)
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()

    def run_apply(self, statediff_file, apply_dir):
        return subprocess.run(
            [str(self.apply_bin), str(statediff_file), str(apply_dir)],
            capture_output=True, text=True)


_BACKENDS = {FUSELOG: FuselogBackend, EBPF: EbpfBackend}


def get_backend(name: str | None = None) -> CaptureBackend:
    name = (name or backend_name()).lower()
    if name not in _BACKENDS:
        raise SystemExit(
            f"unknown CAPTURE_BACKEND={name!r}; "
            f"expected one of {', '.join(sorted(_BACKENDS))}")
    return _BACKENDS[name]()
