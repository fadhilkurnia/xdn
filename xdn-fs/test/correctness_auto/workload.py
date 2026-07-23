"""
Writer threads: real concurrent syscalls against the mounted path, no
MySQL, no sleeps on the hot path -- maximizing genuine concurrency
density so the harness can reproduce the fid-orphaning race in far fewer
iterations than the original 50k-write MySQL repro needed.

Two deliberate populations of files: HOT (disjoint per-thread slice,
hammered continuously) and COLD (small set, rarely written -- mimics
the rarely-touched-file condition that let a fid go orphaned originally).
"""

from __future__ import annotations

import os
import struct
import threading
import time
from typing import List

BLOCK_SIZE = 4


def _build_payload(thread_id: int, iteration: int) -> bytes:
    header = struct.pack("<IQ", thread_id, iteration)
    fill_byte = (thread_id ^ iteration) & 0xFF
    return header + bytes([fill_byte]) * (BLOCK_SIZE - len(header))


class WorkloadGenerator:
    def __init__(self, mount_path: str, num_hot_threads: int = 8,
                 files_per_hot_thread: int = 4, num_cold_files: int = 5,
                 cold_write_interval: float = 0.05):
        self.mount_path = mount_path
        self.num_hot_threads = num_hot_threads
        self.files_per_hot_thread = files_per_hot_thread
        self.num_cold_files = num_cold_files
        self.cold_write_interval = cold_write_interval
        self._stop = threading.Event()
        self._threads: List[threading.Thread] = []
        # One counter slot per writer thread. Each thread only ever
        # increments its OWN slot, so no lock is needed -- and unlike the
        # old design (which only recorded a final count after the thread
        # exited), this is updated live, every iteration, so
        # total_iterations() is accurate to call WHILE the workload is
        # still running.
        self._counts: List[int] = [0] * (num_hot_threads + 1)  # +1 = cold

    def start(self) -> None:
        for tid in range(self.num_hot_threads):
            t = threading.Thread(target=self._hot_writer, args=(tid,), daemon=True)
            self._threads.append(t)
            t.start()

        cold_tid = self.num_hot_threads
        t = threading.Thread(target=self._cold_writer, args=(cold_tid,), daemon=True)
        self._threads.append(t)
        t.start()

    def stop(self, join_timeout: float = 5.0) -> None:
        self._stop.set()
        for t in self._threads:
            t.join(timeout=join_timeout)

    def _hot_writer(self, thread_id: int) -> None:
        paths = [
            os.path.join(self.mount_path, f"hot_{thread_id}_{i}.dat")
            for i in range(self.files_per_hot_thread)
        ]
        iteration = 0
        while not self._stop.is_set():
            path = paths[iteration % len(paths)]
            self._write_block(path, _build_payload(thread_id, iteration))
            iteration += 1
            self._counts[thread_id] = iteration

    def _cold_writer(self, thread_id: int) -> None:
        paths = [
            os.path.join(self.mount_path, f"cold_{i}.dat")
            for i in range(self.num_cold_files)
        ]
        iteration = 0
        while not self._stop.is_set():
            path = paths[iteration % len(paths)]
            self._write_block(path, _build_payload(thread_id, iteration))
            iteration += 1
            self._counts[thread_id] = iteration
            time.sleep(self.cold_write_interval)

    @staticmethod
    def _write_block(path: str, payload: bytes) -> bool:
        try:
            fd = os.open(path, os.O_WRONLY | os.O_CREAT, 0o644)
            try:
                os.pwrite(fd, payload, 0)
            finally:
                os.close(fd)
            return True
        except OSError:
            # Transient failure (e.g. mount tearing down) -- skip silently.
            return False

    def total_iterations(self) -> int:
        return sum(self._counts)
