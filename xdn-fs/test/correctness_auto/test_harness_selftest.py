#!/usr/bin/env python3
"""
Self-test for the stress harness itself -- NOT a test of fuselogv2.

Runs a mock server implementing the real y/g socket protocol against
synthetic, hand-built batches (one well-formed, one with a deliberately
injected orphaned fid) and confirms:
  - a well-formed batch produces zero Tier-1 violations
  - a batch with a statediff referencing a fid absent from its own file
    table is correctly caught as an ORPHANED_FID violation

VFS1 (eBPF capturer):
  - a well-formed batch produces zero violations
  - the request really is a bare b"g": statediff_vfs closes the connection
    on any other byte, so fuselog's b"g\n" would kill the session
  - a batch whose header payload_size disagrees with the records it carries
    is caught as a framing PARSE_ERROR
  - a batch carrying a path that escapes the target root ("../escape") is
    rejected rather than handed to the replay binary

Run this whenever the harness itself changes, independent of whether any
real capturer binary is available -- it validates the checker's own logic,
not the implementation under test. Needs no root and no BPF.
"""
import os
import socket
import struct
import tempfile
import threading
import time

from harvester import Harvester
from parser import (SD_TYPE_WRITE, SD_VFS_OP_MKDIR, SD_VFS_OP_RENAME,
                    SD_VFS_OP_WRITE, VFS1_FILE_HEADER, VFS1_MAGIC,
                    VFS1_RECORD_HEADER, VFS1_VERSION, FuselogV2Parser,
                    ParseError, Vfs1Parser)


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


def _flg3_suite() -> None:
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


def _vfs1_record(seq, op, path, new_path=b"", data=b"", offset=0, size=0,
                 mode=0, flags=0):
    return (VFS1_RECORD_HEADER.pack(seq, op, flags, offset, size, mode,
                                    len(path), len(new_path), len(data))
            + path + new_path + data)


def _build_vfs1_batch(records, payload_size=None):
    """Frame records as a VFS1 batch behind the u64 length prefix.

    payload_size counts the records only -- header, paths and data -- and
    excludes the 24-byte file header, matching batch_payload_size() in
    statediff_vfs.c. The override exists so a test can declare a size that
    disagrees with the records and prove the mismatch is caught."""
    payload = b"".join(records)
    declared = len(payload) if payload_size is None else payload_size
    body = VFS1_FILE_HEADER.pack(VFS1_MAGIC, VFS1_VERSION, 0,
                                 len(records), declared) + payload
    return struct.pack("<q", len(body)) + body


def _serve_once(sock_path, batches, seen_requests):
    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    srv.bind(sock_path)
    srv.listen(5)
    conn, _ = srv.accept()
    for batch in batches:
        req = conn.recv(100)
        if not req:
            break
        seen_requests.append(req)
        conn.sendall(batch)
    conn.close()
    srv.close()


def _vfs1_suite() -> None:
    good = _build_vfs1_batch([
        _vfs1_record(1, SD_VFS_OP_MKDIR, b"d", mode=0o755),
        _vfs1_record(2, SD_VFS_OP_WRITE, b"d/f", data=b"abc", size=3),
        _vfs1_record(3, SD_VFS_OP_RENAME, b"d/f", new_path=b"d/g"),
    ])
    # Declares 8 bytes more payload than the single record actually occupies.
    one = _vfs1_record(1, SD_VFS_OP_WRITE, b"a.dat", data=b"xyz", size=3)
    framing_mismatch = _build_vfs1_batch([one], payload_size=len(one) + 8)
    escaping_path = _build_vfs1_batch([
        _vfs1_record(1, SD_VFS_OP_WRITE, b"../escape", data=b"x", size=1),
    ])

    for label, batch, expect_violation in (
        ("well-formed", good, False),
        ("framing-mismatch", framing_mismatch, True),
        ("escaping-path", escaping_path, True),
    ):
        sock_path = os.path.join(tempfile.mkdtemp(), "mock_vfs1.sock")
        seen = []
        t = threading.Thread(target=_serve_once,
                             args=(sock_path, [batch], seen), daemon=True)
        t.start()
        time.sleep(0.1)

        h = Harvester(sock_path, Vfs1Parser(), request=b"g")
        try:
            h._harvest_once()
            failed = False
        except ParseError as e:
            failed = True
            detail = str(e)

        assert seen and seen[0] == b"g", (
            f"harvester must request with a bare b'g' for VFS1, sent {seen!r}")

        if expect_violation:
            assert failed, f"{label}: expected a ParseError, got a clean parse"
            print(f"[selftest] vfs1 {label}: rejected (correct): {detail}")
        else:
            assert not failed, f"{label}: unexpected ParseError"
            assert len(h.stats.violations) == 0, h.stats.violations
            batch_parsed = h.stats.all_batches[0]
            assert len(batch_parsed.statediffs) == 3, batch_parsed.statediffs
            print(f"[selftest] vfs1 {label}: 0 violations, "
                  f"{len(batch_parsed.statediffs)} statediffs, "
                  f"{len(batch_parsed.fid_to_path)} synthesized fids (correct)")


def main() -> int:
    _flg3_suite()
    _vfs1_suite()
    print("[selftest] ALL SELF-TESTS PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
