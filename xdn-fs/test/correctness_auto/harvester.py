"""
Harvester: repeatedly requests batches over the harvest socket and runs
the Tier-1 invariant check on each one.

Protocol confirmed from fuselogv2.cpp's sock_listener:
  - connect
  - send a message containing the substring "g" (harvest request)
  - IMPORTANT: sending anything that matches neither "y" nor "g" kills
    the ENTIRE socket listener for the rest of the process's life (not
    just this connection) -- so this client only ever sends "g\n",
    nothing else, ever.
  - read exactly one batch's worth of bytes (self-describing via the
    wire format's own field lengths; the server does NOT close the
    connection afterwards)
  - close the connection ourselves once one batch has been fully parsed
"""

from __future__ import annotations

import socket
import struct
import threading
import time
from dataclasses import dataclass, field
from typing import List, Optional

from parser import ParseError, ParsedBatch, SocketReader, WireFormatParser

# Mirrors bench_common.py's own MAX_STATEDIFF_BYTES sanity ceiling for the
# size-prefix header -- a real, working reference for what counts as a
# plausible size versus a desynced/garbage one.
MAX_STATEDIFF_BYTES = 2 * 1024 * 1024 * 1024


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ParseError(
                f"socket closed after reading {len(buf)}/{n} bytes "
                f"of the size-prefix header"
            )
        buf.extend(chunk)
    return bytes(buf)


@dataclass
class Violation:
    batch_seq: int
    kind: str            # "ORPHANED_FID" or "PARSE_ERROR"
    detail: str


@dataclass
class HarvesterStats:
    batches_harvested: int = 0
    statediffs_seen: int = 0
    violations: List[Violation] = field(default_factory=list)
    all_batches: List[ParsedBatch] = field(default_factory=list)  # for reference
    raw_batch_bytes: List[bytes] = field(default_factory=list)    # for Tier-2


class Harvester:
    def __init__(self, socket_path: str, parser: WireFormatParser,
                 connect_timeout: float = 2.0, read_timeout: float = 10.0,
                 keep_batches: bool = True):
        self.socket_path = socket_path
        self.parser = parser
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.keep_batches = keep_batches
        self.stats = HarvesterStats()
        self._stop = threading.Event()
        self._sock: Optional[socket.socket] = None

    def stop(self) -> None:
        self._stop.set()
        self._close_sock()

    def _close_sock(self) -> None:
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None

    def run_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._harvest_once()
            except (ParseError, OSError) as e:
                if self._stop.is_set():
                    return # stop() closed our socket to unblock us -- expected
                self.stats.violations.append(Violation(
                    batch_seq=self.stats.batches_harvested,
                    kind="PARSE_ERROR",
                    detail=str(e),
                ))
                self._stop.set()
                return

    def _connect(self) -> socket.socket:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(self.connect_timeout)
        sock.connect(self.socket_path)
        # Separate, more generous timeout for the actual batch read --
        # conflating this with connect_timeout was hiding whether a
        # slow response is "just needed more time under load" versus
        # "genuinely wedged," which matter very differently here.
        sock.settimeout(self.read_timeout)
        return sock

    def _harvest_once(self) -> None:
        # Connect ONCE and reuse the same socket for every harvest --
        # matching capture_bench.py's single-persistent-connection usage
        # (and fuselogv2.cpp's own "assume a single client" design), rather
        # than triggering the server's accept() path on every single call.
        if self._sock is None:
            try:
                self._sock = self._connect()
            except OSError:
                time.sleep(0.01)
                return
        sock = self._sock

        try:
            sock.sendall(b"g\n")
        except OSError:
            self._close_sock()
            raise

        size_header = _recv_exact(sock, 8)
        size = struct.unpack("<q", size_header)[0]
        if size < 0 or size > MAX_STATEDIFF_BYTES:
            self._close_sock()
            raise ParseError(
                f"garbage size-prefix header: {size} "
                f"(raw bytes={size_header.hex()})"
            )

        if size == 0:
            batch = ParsedBatch(fid_to_path={}, statediffs=[])
            raw_bytes = b""
        else:
            reader = SocketReader(sock, capture=True)
            try:
                batch = self.parser.parse_batch_from_reader(reader)
                consumed = len(reader.captured_bytes())
                if consumed != size:
                    raise ParseError(
                        f"size-prefix declared {size} bytes but schema "
                        f"parsing consumed {consumed} bytes -- framing "
                        f"mismatch"
                    )
                raw_bytes = reader.captured_bytes()
            except ParseError:
                self.stats.last_failure_raw_bytes = reader.captured_bytes()
                self._close_sock()  # stream is desynced -- don't reuse it
                raise

        seq = self.stats.batches_harvested
        self.stats.batches_harvested += 1
        self.stats.statediffs_seen += len(batch.statediffs)
        if self.keep_batches:
            self.stats.all_batches.append(batch)
            self.stats.raw_batch_bytes.append(raw_bytes)

        self._check_invariant(seq, batch)

    def _check_invariant(self, seq: int, batch: ParsedBatch) -> None:
        """The core Tier-1 check: every fid referenced by a statediff in
        this batch must resolve in THIS SAME batch's own file table.
        This is the exact property the fid-registration/push and
        stack-exchange/map-moveout races (discussed at length) violate
        when they tear apart."""
        for sd in batch.statediffs:
            for fid in self._fids_referenced(sd):
                if fid not in batch.fid_to_path:
                    self.stats.violations.append(Violation(
                        batch_seq=seq,
                        kind="ORPHANED_FID",
                        detail=(
                            f"statediff type={sd.sd_type} references "
                            f"fid={fid} which is absent from this "
                            f"batch's own file table "
                            f"({len(batch.fid_to_path)} entries)"
                        ),
                    ))

    @staticmethod
    def _fids_referenced(sd) -> List[int]:
        fids = []
        if sd.fid is not None:
            fids.append(sd.fid)
        if sd.to_fid is not None:
            fids.append(sd.to_fid)
        return fids
