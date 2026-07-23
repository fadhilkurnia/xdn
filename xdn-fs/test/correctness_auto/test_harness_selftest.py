#!/usr/bin/env python3
"""
Self-test for the stress harness itself -- NOT a test of fuselogv2.

Runs a mock server implementing the real y/g socket protocol against
synthetic, hand-built batches (one well-formed, one with a deliberately
injected orphaned fid) and confirms:
  - a well-formed batch produces zero Tier-1 violations
  - a batch with a statediff referencing a fid absent from its own file
    table is correctly caught as an ORPHANED_FID violation

Run this whenever the harness itself changes, independent of whether a
real fuselogv2/fuselog-apply binary is available -- it validates the
checker's own logic, not the implementation under test.
"""
import os
import socket
import struct
import tempfile
import threading
import time

from harvester import Harvester
from parser import SD_TYPE_WRITE, FuselogV2Parser


def _build_batch(file_table, statediff_bytes, num_sd):
    payload = struct.pack("<Q", len(file_table))
    for fid, path in file_table:
        payload += struct.pack("<Q", fid) + struct.pack("<Q", len(path)) + path.encode()
    payload += struct.pack("<Q", num_sd) + statediff_bytes
    # Real fuselogv2 always prepends an 8-byte little-endian signed size
    # prefix ahead of the payload (confirmed against bench_common.py /
    # send_gathered_statediffs) -- the mock server has to match that framing
    # or harvester.py's _recv_exact(sock, 8) desyncs against the payload.
    return struct.pack("<q", len(payload)) + payload


def main() -> int:
    sock_path = os.path.join(tempfile.mkdtemp(), "mock.sock")

    good_batch = _build_batch(
        [(1, "a.dat")],
        bytes([SD_TYPE_WRITE]) + struct.pack("<Q", 1)
        + struct.pack("<Q", 3) + struct.pack("<Q", 0) + b"abc",
        1,
    )
    # Deliberately orphaned: statediff references fid=99, file table only has fid=1.
    bad_batch = _build_batch(
        [(1, "a.dat")],
        bytes([SD_TYPE_WRITE]) + struct.pack("<Q", 99)
        + struct.pack("<Q", 3) + struct.pack("<Q", 0) + b"xyz",
        1,
    )
    batches_to_send = [good_batch, bad_batch]
    served = []

    def mock_server():
        srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        srv.bind(sock_path)
        srv.listen(5)
        # ONE accept for the whole test, matching the harvester's persistent
        # socket -- it now connects once and reuses that connection for every
        # harvest, rather than reconnecting per batch.
        conn, _ = srv.accept()
        while len(served) < len(batches_to_send):
            req = conn.recv(100)
            if not req:
                break  # client disconnected before requesting all batches
            if b"g" in req:
                batch = batches_to_send[len(served)]
                conn.sendall(batch)
                served.append(batch)
        conn.close()
        srv.close()

    t = threading.Thread(target=mock_server, daemon=True)
    t.start()
    time.sleep(0.1)

    h = Harvester(sock_path, FuselogV2Parser())

    h._harvest_once()
    assert len(h.stats.violations) == 0, h.stats.violations
    print("[selftest] well-formed batch: 0 violations (correct)")

    h._harvest_once()
    assert len(h.stats.violations) == 1, h.stats.violations
    v = h.stats.violations[0]
    assert v.kind == "ORPHANED_FID"
    print(f"[selftest] injected-orphan batch: caught ORPHANED_FID "
          f"(correct): {v.detail}")

    print("[selftest] ALL SELF-TESTS PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
