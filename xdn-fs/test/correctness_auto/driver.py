"""
Drivers know how to start/stop one specific fuselog-family implementation
and report back the two things everything else in this project needs:
  - writable_path:      where writes actually get intercepted/captured.
                         For fuselogv2 this is its one and only directory
                         argument -- confirmed from source there is NO
                         separate backing/source directory; fuselogv2
                         mounts itself directly over this same path,
                         using a pre-mount fd to still reach the real
                         underlying storage underneath its own mount.
  - harvest_socket_path: where to connect to request a harvest batch

Nothing else in this project (workload generator, harvester, checkers)
should ever need to know it's specifically talking to a FUSE-based, C++
implementation. Adding a second implementation means writing one new Driver
subclass; nothing else changes.

Two exist: FuselogV2Driver (FUSE mount, FLG3 batches) and EbpfDriver (VFS
probes, VFS1 batches). They differ in what "writable_path" means -- a mount
for the first, an ordinary traced directory for the second -- but both hand
back the same DriverHandle, so the workload generator writes to it the same
way either way.
"""

from __future__ import annotations

import os
import shutil
import signal
import socket
import subprocess
import tempfile
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass

from parser import FuselogV2Parser, Vfs1Parser, WireFormatParser


@dataclass
class DriverHandle:
    writable_path: str
    harvest_socket_path: str
    parser: WireFormatParser
    harvest_request: bytes = b"g\n"


class Driver(ABC):
    @abstractmethod
    def start(self) -> DriverHandle:
        ...

    @abstractmethod
    def stop(self) -> None:
        ...

    def __enter__(self) -> DriverHandle:
        return self.start()

    def __exit__(self, exc_type, exc, tb) -> None:
        self.stop()


class FuselogV2Driver(Driver):
    """Drives the real fuselogv2 C++ binary as a subprocess.

    Confirmed against source:
      - FUSELOG_SOCKET_FILE env var overrides the default socket path
        (/tmp/fuselog.sock) -- ALWAYS set explicitly here to a unique
        per-run path, never rely on the shared default (see discussion:
        concurrent runs / leftover sockets from a crashed prior run would
        otherwise collide).
      - The ONLY positional argument is the single directory fuselogv2
        both mounts itself over AND uses (via a pre-mount fd) as the real
        storage location. There is no separate "-o source=" option --
        that was a wrong assumption carried over from passthrough_ll.c's
        different, two-directory architecture; fuselogv2's own main()
        and the safe_fd/fchdir technique in fuselog_init confirm this.
      - "-f" keeps it in the foreground so this process can manage its
        lifetime directly as a child, rather than it daemonizing.
    """

    def __init__(self, fuselog_binary: str, work_root: str | None = None,
                 extra_env: dict | None = None, mount_wait_timeout: float = 5.0):
        self.fuselog_binary = fuselog_binary
        self.work_root = work_root or tempfile.mkdtemp(prefix="fuselog_stress_")
        self.extra_env = extra_env or {}
        self.mount_wait_timeout = mount_wait_timeout

        self._proc: subprocess.Popen | None = None
        self._data_dir: str | None = None
        self._socket_path: str | None = None
        self._log_path: str | None = None
        self._log_file = None

    def start(self) -> DriverHandle:
        run_id = uuid.uuid4().hex[:12]
        # fuselogv2 takes exactly ONE directory: it mounts itself directly
        # over it, using a pre-mount fd (captured before fuse_main()) to
        # still reach the real underlying storage at that same path. There
        # is NO separate "-o source=" backing directory -- confirmed from
        # main()'s argument handling and the safe_fd/fchdir technique in
        # fuselog_init (this differs from passthrough_ll.c's two-directory
        # model; an earlier version of this driver wrongly assumed the
        # same convention applied here).
        self._data_dir = os.path.join(self.work_root, f"data_{run_id}")
        self._socket_path = os.path.join(
            tempfile.gettempdir(), f"fuselog_test_{run_id}.sock"
        )
        os.makedirs(self._data_dir, exist_ok=True)

        env = dict(os.environ)
        env["FUSELOG_SOCKET_FILE"] = self._socket_path
        env.setdefault("FUSELOG_CAPTURE", "true")
        env.update(self.extra_env)

        cmd = [
            self.fuselog_binary,
            "-f",
            self._data_dir,
        ]
        # A pipe has a small, fixed OS buffer -- if nothing drains it while
        # the process runs, fuselogv2 (very verbose: logs every mknod/write/
        # read-old-data event) can BLOCK on its own stdout write once that
        # buffer fills, independent of any real race condition. A real file
        # has no such ceiling.
        self._log_path = os.path.join(self.work_root, f"fuselog_{run_id}.log")
        self._log_file = open(self._log_path, "w")
        self._proc = subprocess.Popen(
            cmd, env=env,
            stdout=self._log_file, stderr=subprocess.STDOUT,
        )

        self._wait_for_mount()
        self._wait_for_socket()

        return DriverHandle(
            writable_path=self._data_dir,
            harvest_socket_path=self._socket_path,
            parser=FuselogV2Parser(),
        )

    def stop(self) -> None:
        # Unconditional -- if fuselogv2 already crashed on its own, the old
        # code skipped unmounting entirely, leaving a dangling mount.
        subprocess.run(
            ["fusermount3", "-u", self._data_dir],
            capture_output=True,
        )

        if self._proc is not None and self._proc.poll() is None:
            try:
                self._proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._proc.kill()
                self._proc.wait(timeout=5)

        for p in (self._socket_path,):
            if p and os.path.exists(p):
                try:
                    os.remove(p)
                except OSError:
                    pass

        if self._log_file is not None:
            self._log_file.close()

    @property
    def log_path(self) -> str | None:
        """Path to fuselogv2's stdout+stderr log file for this run."""
        return self._log_path

    def cleanup_work_root(self) -> None:
        """Remove the entire per-run data directory tree.
        Call explicitly -- not automatic, since callers may want to
        inspect a failed run's directory first."""
        shutil.rmtree(self.work_root, ignore_errors=True)

    def process_output(self) -> str:
        """Best-effort grab of whatever the subprocess has printed so far
        -- useful to include in a failure report. Reads from the log file
        directly, so this works whether the process is still running or
        has already exited."""
        if self._log_path is None or not os.path.exists(self._log_path):
            return ""
        with open(self._log_path) as f:
            return f.read()

    def _wait_for_mount(self) -> None:
        deadline = time.time() + self.mount_wait_timeout
        while time.time() < deadline:
            if self._proc.poll() is not None:
                out = self._proc.stdout.read() if self._proc.stdout else ""
                raise RuntimeError(
                    f"fuselogv2 exited early (code {self._proc.returncode}) "
                    f"before mount was ready:\n{out}"
                )
            # os.path.ismount is the simplest reliable signal that the
            # FUSE mount actually took effect.
            if os.path.ismount(self._data_dir):
                return
            time.sleep(0.05)
        raise TimeoutError(
            f"mount at {self._data_dir} did not become ready within "
            f"{self.mount_wait_timeout}s"
        )

    def _wait_for_socket(self) -> None:
        deadline = time.time() + self.mount_wait_timeout
        while time.time() < deadline:
            if os.path.exists(self._socket_path):
                # Confirm it's actually accept()-ing, not just that the
                # inode exists mid-bind().
                try:
                    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    s.settimeout(0.2)
                    s.connect(self._socket_path)
                    s.close()
                    return
                except OSError:
                    pass
            time.sleep(0.05)
        raise TimeoutError(
            f"harvest socket at {self._socket_path} did not become ready "
            f"within {self.mount_wait_timeout}s"
        )


class EbpfDriver(Driver):
    """Drives the eBPF VFS capturer (experiments/eBPF/statediff_vfs).

    Differences from FuselogV2Driver that the harness has to care about:

      - Nothing is mounted. The positional directory argument is an ordinary
        directory that the capturer *observes* via VFS probes, so there is no
        mount to wait for and none to unmount; os.path.ismount would never
        become true and must not be used as the readiness signal. The socket
        appearing is the only readiness signal there is.
      - It must run as root, and this process must be its direct parent:
        in socket mode statediff_vfs stops once it is reparented (that is
        what --no-parent-check suppresses), so it cannot be launched through
        sudo from an unprivileged harness. Fail early and clearly rather than
        hanging on a socket that will never appear.
      - SIGINT is the documented clean stop. SIGKILL would skip probe detach.
      - Loading the skeleton and attaching ~20 fentry/fexit/kprobe handlers
        takes appreciably longer than a FUSE mount, hence the larger default
        timeout.
    """

    def __init__(self, ebpf_binary: str, work_root: str | None = None,
                 extra_env: dict | None = None, start_timeout: float = 30.0,
                 mmap_snapshot: bool = True):
        self.ebpf_binary = ebpf_binary
        self.work_root = work_root or tempfile.mkdtemp(prefix="ebpf_stress_")
        self.extra_env = extra_env or {}
        self.start_timeout = start_timeout
        # mmap stores are invisible without this. Anything that maps its files
        # -- SQLite's shm/WAL-index above all -- needs it, and it costs nothing
        # for a workload that never mmaps, so it is on by default here even
        # though the binary defaults it off.
        self.mmap_snapshot = mmap_snapshot

        self._proc: subprocess.Popen | None = None
        self._data_dir: str | None = None
        self._socket_path: str | None = None
        self._log_path: str | None = None
        self._log_file = None

    def start(self) -> DriverHandle:
        if os.geteuid() != 0:
            raise RuntimeError(
                "EbpfDriver must run as root: loading BPF programs needs "
                "privilege, the harvest socket is created 0600 and root-owned, "
                "and statediff_vfs exits in socket mode once reparented, so it "
                "cannot be launched via sudo from an unprivileged harness -- "
                "re-run the whole harness under sudo"
            )

        run_id = uuid.uuid4().hex[:12]
        self._data_dir = os.path.join(self.work_root, f"data_{run_id}")
        self._socket_path = os.path.join(
            tempfile.gettempdir(), f"ebpf_test_{run_id}.sock"
        )
        # The capturer resolves and stats its target at startup, so the
        # directory has to exist before it is launched.
        os.makedirs(self._data_dir, exist_ok=True)

        env = dict(os.environ)
        env.update(self.extra_env)

        cmd = [self.ebpf_binary]
        if self.mmap_snapshot:
            cmd.append("--mmap-snapshot")
        cmd += ["--socket", self._socket_path, self._data_dir]

        self._log_path = os.path.join(self.work_root, f"ebpf_{run_id}.log")
        self._log_file = open(self._log_path, "w")
        self._proc = subprocess.Popen(
            cmd, env=env,
            stdout=self._log_file, stderr=subprocess.STDOUT,
        )

        self._wait_for_socket()

        return DriverHandle(
            writable_path=self._data_dir,
            harvest_socket_path=self._socket_path,
            parser=Vfs1Parser(),
            # statediff_vfs closes the connection on any byte that is not
            # exactly 'g'; fuselog's "g\n" would kill the session.
            harvest_request=b"g",
        )

    def stop(self) -> None:
        if self._proc is not None and self._proc.poll() is None:
            self._proc.send_signal(signal.SIGINT)
            try:
                self._proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self._proc.terminate()
                try:
                    self._proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self._proc.kill()

        if self._socket_path and os.path.exists(self._socket_path):
            try:
                os.remove(self._socket_path)
            except OSError:
                pass

        if self._log_file is not None:
            self._log_file.close()

    @property
    def log_path(self) -> str | None:
        """Path to the capturer's stdout+stderr log file for this run."""
        return self._log_path

    def cleanup_work_root(self) -> None:
        shutil.rmtree(self.work_root, ignore_errors=True)

    def process_output(self) -> str:
        if self._log_path is None or not os.path.exists(self._log_path):
            return ""
        with open(self._log_path) as f:
            return f.read()

    def _wait_for_socket(self) -> None:
        deadline = time.time() + self.start_timeout
        while time.time() < deadline:
            if os.path.exists(self._socket_path):
                # The inode appears between bind() and listen(), so only a
                # successful connect proves it will answer a harvest.
                try:
                    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    s.settimeout(0.2)
                    s.connect(self._socket_path)
                    s.close()
                    return
                except OSError:
                    pass
            if self._proc.poll() is not None:
                raise RuntimeError(
                    f"statediff_vfs exited early (code {self._proc.returncode}) "
                    f"before its harvest socket was ready\n"
                    f"--- tail of {self._log_path} ---\n"
                    f"{self.process_output()[-3000:]}"
                )
            time.sleep(0.05)
        raise TimeoutError(
            f"harvest socket at {self._socket_path} did not become ready "
            f"within {self.start_timeout}s\n"
            f"--- tail of {self._log_path} ---\n"
            f"{self.process_output()[-3000:]}"
        )
