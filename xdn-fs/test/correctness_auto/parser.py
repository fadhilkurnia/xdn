"""
Wire-format parsers for fuselog-family statediff batches.

The harvest socket protocol and the statediff batch layout are two
different things:
  - The SOCKET PROTOCOL (how you ask for a batch) is implementation-specific
    setup, handled by the Driver, not this module.
  - The BATCH LAYOUT (the bytes you get back once you've asked) is what a
    WireFormatParser understands.

Two concrete parsers exist:

  - FuselogV2Parser, matching the "version 2" (apply2()) layout in
    fuselog-apply.cpp, verified byte-for-byte against that source rather
    than inferred.
  - Vfs1Parser, matching the "VFS1" layout produced by the eBPF capturer
    (experiments/eBPF), verified against include/statediff_vfs.h and the
    reader in statediff_apply_vfs.c.

They share nothing but this interface: FLG3 is fid-based and carries a file
table, VFS1 is path-based and carries none. A third implementation gets its
own class implementing WireFormatParser -- nothing else in this project
needs to change.
"""

from __future__ import annotations

import struct
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Optional

try:
    import zstandard  # type: ignore
    _HAVE_ZSTD = True
except ImportError:
    _HAVE_ZSTD = False


class ParseError(Exception):
    """Raised on any malformed/truncated/out-of-bounds batch data.

    Deliberately raised eagerly and loudly (see the length-sanity checks
    below) rather than letting a misparse silently produce plausible-looking
    garbage -- a parser that fails loudly on the wrong format is much safer
    than one that "succeeds" while having read nonsense.
    """


@dataclass(frozen=True)
class StateDiff:
    sd_type: int
    fid: Optional[int] = None
    to_fid: Optional[int] = None      # RENAME only
    offset: Optional[int] = None      # WRITE only
    size: Optional[int] = None        # WRITE/TRUNCATE only
    data: Optional[bytes] = None      # WRITE only (the actual payload)
    uid: Optional[int] = None
    gid: Optional[int] = None
    mode: Optional[int] = None
    symlink_target: Optional[bytes] = None  # SYMLINK only


# SD_TYPE constants -- must match fuselog-apply.cpp's #define block exactly.
SD_TYPE_WRITE = 0
SD_TYPE_UNLINK = 1
SD_TYPE_RENAME = 2
SD_TYPE_TRUNCATE = 3
SD_TYPE_CREATE = 4
SD_TYPE_LINK = 5
SD_TYPE_CHOWN = 6
SD_TYPE_CHMOD = 7
SD_TYPE_MKDIR = 8
SD_TYPE_RMDIR = 9
SD_TYPE_SYMLINK = 10

_KNOWN_TYPES = {
    SD_TYPE_WRITE, SD_TYPE_UNLINK, SD_TYPE_RENAME, SD_TYPE_TRUNCATE,
    SD_TYPE_CREATE, SD_TYPE_LINK, SD_TYPE_CHOWN, SD_TYPE_CHMOD,
    SD_TYPE_MKDIR, SD_TYPE_RMDIR, SD_TYPE_SYMLINK,
}

_ZSTD_MAGIC = 0xFD2FB528


class WireFormatParser(ABC):
    """Interface every fuselog-family wire-format parser must implement."""

    @abstractmethod
    def parse_batch(self, raw_bytes: bytes) -> "ParsedBatch":
        """Parse one harvested batch. Must raise ParseError on any
        malformed/truncated/out-of-bounds data -- never return a partial
        or best-effort result silently."""
        raise NotImplementedError


@dataclass
class ParsedBatch:
    fid_to_path: Dict[int, str]
    statediffs: List[StateDiff]


class _Reader(ABC):
    """Minimal interface the parsing walk needs. Implemented by both
    _BufferReader (whole-file/whole-batch already in memory -- used for
    Tier-2 replay checking against real statediff files) and
    _SocketReader (bytes pulled on demand directly off a live harvest
    socket -- used for Tier-1 live checking). Keeping the actual
    field-by-field schema walk in FuselogV2Parser and only swapping the
    reader means there is exactly one place the wire format is encoded,
    not two copies that could silently drift apart."""

    @abstractmethod
    def read(self, n: int) -> bytes:
        ...

    def read_u8(self) -> int:
        return self.read(1)[0]

    def read_u32(self) -> int:
        return struct.unpack("<I", self.read(4))[0]

    def read_u64(self) -> int:
        return struct.unpack("<Q", self.read(8))[0]


class _BufferReader(_Reader):
    """Bounds-checked reader over an in-memory buffer (a fully-loaded
    statediff file, or a fully zstd-decompressed batch)."""

    __slots__ = ("buf", "pos")

    MAX_REASONABLE_FIELD = 64 * 1024 * 1024  # 64 MiB, see _check()

    def __init__(self, buf: bytes):
        self.buf = buf
        self.pos = 0

    def read(self, n: int) -> bytes:
        if n < 0:
            raise ParseError(f"negative read length {n} at offset {self.pos}")
        if n > self.MAX_REASONABLE_FIELD:
            raise ParseError(
                f"field length {n} at offset {self.pos} exceeds sanity "
                f"ceiling ({self.MAX_REASONABLE_FIELD}) -- likely wrong "
                f"wire format, not a legitimate field"
            )
        if self.pos + n > len(self.buf):
            raise ParseError(
                f"unexpected end of data: need {n} bytes at offset "
                f"{self.pos}, only {len(self.buf) - self.pos} remain"
            )
        out = self.buf[self.pos:self.pos + n]
        self.pos += n
        return out


class SocketReader(_Reader):
    """Reads directly off a live, connected socket, on demand, with the
    same sanity ceiling as _BufferReader. No overall batch-length prefix
    exists in this protocol (confirmed from source: the first bytes are
    directly num_file, not a wrapping envelope size) and the server does
    NOT close the connection after sending one batch -- it waits for
    another request on the same connection. So the ONLY way to know
    where a batch ends is to walk the schema incrementally, stopping
    exactly after the last field of the last statediff is read. This
    reader assumes UNCOMPRESSED batches (FUSELOG_COMPRESSION=false, the
    default) -- see the check in read(), which fails loudly rather than
    silently mishandling a compressed live batch it wasn't built to
    stream-decompress.
    """

    MAX_REASONABLE_FIELD = 64 * 1024 * 1024

    def __init__(self, sock, recv_chunk: int = 65536, capture: bool = False):
        self.sock = sock
        self.recv_chunk = recv_chunk
        self._buf = bytearray()
        self._checked_for_zstd = False
        self._capture = capture
        self._captured = bytearray() if capture else None

    def captured_bytes(self) -> bytes:
        if not self._capture:
            raise RuntimeError("SocketReader was not constructed with capture=True")
        return bytes(self._captured)

    def read(self, n: int) -> bytes:
        if n < 0:
            raise ParseError(f"negative read length {n}")
        if n > self.MAX_REASONABLE_FIELD:
            raise ParseError(
                f"field length {n} exceeds sanity ceiling "
                f"({self.MAX_REASONABLE_FIELD}) -- likely wrong wire "
                f"format or a desynced stream"
            )
        while len(self._buf) < n:
            chunk = self.sock.recv(self.recv_chunk)
            if not chunk:
                raise ParseError(
                    f"socket closed with {len(self._buf)}/{n} bytes "
                    f"pending -- server disconnected mid-batch"
                )
            self._buf.extend(chunk)
            if not self._checked_for_zstd and len(self._buf) >= 4:
                self._checked_for_zstd = True
                magic = struct.unpack("<I", bytes(self._buf[:4]))[0]
                if magic == _ZSTD_MAGIC:
                    raise ParseError(
                        "live batch appears zstd-compressed (magic "
                        "matched), but SocketReader only supports "
                        "uncompressed live batches -- ensure "
                        "FUSELOG_COMPRESSION is not enabled for the "
                        "stress test"
                    )
        out = bytes(self._buf[:n])
        del self._buf[:n]
        if self._capture:
            self._captured.extend(out)
        return out





class FuselogV2Parser(WireFormatParser):
    """Parser for fuselog-apply.cpp's apply2() ("version 2") batch layout.

    Confirmed field-by-field against fuselog-apply.cpp source:
      - optional zstd frame (magic 0xFD2FB528, whole-file) wrapping
        everything below
      - num_file: u64
        repeated num_file times: fid: u64, path_len: u64, path: path_len bytes
      - num_statediff: u64
        repeated num_statediff times: sd_type: u8, then per-type fields:
          WRITE:    fid u64, size u64, offset u64, data[size]
          UNLINK:   fid u64
          RENAME:   from_fid u64, to_fid u64
          TRUNCATE: fid u64, size u64
          CREATE:   fid u64, uid u32, gid u32, mode u32
          LINK:     src_fid u64, new_fid u64
          CHOWN:    fid u64, uid u32, gid u32
          CHMOD:    fid u64, mode u32
          MKDIR:    fid u64, mode u32
          RMDIR:    fid u64
          SYMLINK:  fid u64, target_len u32, target[target_len],
                    uid u32, gid u32
    """

    def parse_batch(self, raw_bytes: bytes) -> ParsedBatch:
        """Parse a complete, already-in-memory batch (e.g. a whole
        statediff file read for Tier-2 replay checking). Handles the
        whole-file zstd case, unlike SocketReader's live path."""
        data = self._maybe_decompress(raw_bytes)
        return self.parse_batch_from_reader(_BufferReader(data))

    def parse_batch_from_reader(self, reader: "_Reader") -> ParsedBatch:
        """The actual schema walk, shared by both the buffer-backed and
        socket-backed reading paths -- the one place this wire format is
        encoded."""
        fid_to_path: Dict[int, str] = {}
        num_file = reader.read_u64()
        for _ in range(num_file):
            fid = reader.read_u64()
            path_len = reader.read_u64()
            path_bytes = reader.read(path_len)
            fid_to_path[fid] = path_bytes.decode("utf-8", errors="replace")

        statediffs: List[StateDiff] = []
        num_statediff = reader.read_u64()
        for _ in range(num_statediff):
            sd_type = reader.read_u8()
            if sd_type not in _KNOWN_TYPES:
                raise ParseError(
                    f"unknown statediff type {sd_type} -- likely wrong "
                    f"wire format for this parser"
                )
            statediffs.append(self._parse_one(sd_type, reader))

        return ParsedBatch(fid_to_path=fid_to_path, statediffs=statediffs)

    @staticmethod
    def _parse_one(sd_type: int, cur: "_Reader") -> StateDiff:
        if sd_type == SD_TYPE_WRITE:
            fid = cur.read_u64()
            size = cur.read_u64()
            offset = cur.read_u64()
            data = cur.read(size)
            return StateDiff(sd_type=sd_type, fid=fid, size=size,
                              offset=offset, data=data)
        elif sd_type == SD_TYPE_UNLINK:
            fid = cur.read_u64()
            return StateDiff(sd_type=sd_type, fid=fid)
        elif sd_type == SD_TYPE_RENAME:
            from_fid = cur.read_u64()
            to_fid = cur.read_u64()
            return StateDiff(sd_type=sd_type, fid=from_fid, to_fid=to_fid)
        elif sd_type == SD_TYPE_TRUNCATE:
            fid = cur.read_u64()
            size = cur.read_u64()
            return StateDiff(sd_type=sd_type, fid=fid, size=size)
        elif sd_type == SD_TYPE_CREATE:
            fid = cur.read_u64()
            uid = cur.read_u32()
            gid = cur.read_u32()
            mode = cur.read_u32()
            return StateDiff(sd_type=sd_type, fid=fid, uid=uid, gid=gid,
                              mode=mode)
        elif sd_type == SD_TYPE_LINK:
            src_fid = cur.read_u64()
            new_fid = cur.read_u64()
            return StateDiff(sd_type=sd_type, fid=src_fid, to_fid=new_fid)
        elif sd_type == SD_TYPE_CHOWN:
            fid = cur.read_u64()
            uid = cur.read_u32()
            gid = cur.read_u32()
            return StateDiff(sd_type=sd_type, fid=fid, uid=uid, gid=gid)
        elif sd_type == SD_TYPE_CHMOD:
            fid = cur.read_u64()
            mode = cur.read_u32()
            return StateDiff(sd_type=sd_type, fid=fid, mode=mode)
        elif sd_type == SD_TYPE_MKDIR:
            fid = cur.read_u64()
            mode = cur.read_u32()
            return StateDiff(sd_type=sd_type, fid=fid, mode=mode)
        elif sd_type == SD_TYPE_RMDIR:
            fid = cur.read_u64()
            return StateDiff(sd_type=sd_type, fid=fid)
        elif sd_type == SD_TYPE_SYMLINK:
            fid = cur.read_u64()
            target_len = cur.read_u32()
            target = cur.read(target_len)
            uid = cur.read_u32()
            gid = cur.read_u32()
            return StateDiff(sd_type=sd_type, fid=fid, uid=uid, gid=gid,
                              symlink_target=target)
        else:  # pragma: no cover -- guarded by the _KNOWN_TYPES check above
            raise ParseError(f"unhandled statediff type {sd_type}")

    @staticmethod
    def _maybe_decompress(raw_bytes: bytes) -> bytes:
        if len(raw_bytes) < 4:
            return raw_bytes
        magic = struct.unpack("<I", raw_bytes[:4])[0]
        if magic != _ZSTD_MAGIC:
            return raw_bytes
        if not _HAVE_ZSTD:
            raise ParseError(
                "batch is zstd-compressed (magic matched) but the "
                "'zstandard' package is not installed -- "
                "pip install zstandard"
            )
        try:
            return zstandard.ZstdDecompressor().decompress(raw_bytes)
        except zstandard.ZstdError as e:
            raise ParseError(f"zstd decompression failed: {e}") from e


# Types referencing a fid that must be resolvable in that SAME batch's
# file table, for the Tier-1 invariant check. Some types (CREATE, MKDIR,
# SYMLINK) *introduce* a fid rather than reference a pre-existing one in
# some producers -- but per the confirmed apply2() behavior, every type
# below does a fid_to_filename[...] lookup unconditionally, so all of them
# are subject to the same invariant.
FID_REFERENCING_TYPES = _KNOWN_TYPES


# ---------------------------------------------------------------------------
# VFS1 -- the eBPF capturer's batch layout
# ---------------------------------------------------------------------------
#
# Confirmed against experiments/eBPF/include/statediff_vfs.h (the struct
# definitions), statediff_vfs.c (batch_payload_size/fill_record_header/
# send_batch, the producer) and statediff_apply_vfs.c (the reader, which is
# the authority on which record shapes are legal).
#
# The shape difference that matters: FLG3 registers every path in a per-batch
# file table and refers to it by fid, so a statediff can reference a fid the
# table never registered -- the ORPHANED_FID race the Tier-1 check hunts.
# VFS1 inlines the path in every record, so that failure mode cannot exist.
# Its analogous invariants are framing ones: the declared payload_size must
# equal what the records actually occupy, and every path must be replay-safe.
# Both are enforced below, so a VFS1 batch is checked just as hard.

VFS1_MAGIC = 0x31534656          
VFS1_VERSION = 1

# enum statediff_vfs_op. The internal-only opcodes (MMAP, WRITEBACK, the
# native-AIO staging events) are deliberately absent: userspace converts them
# to their logical effects and must never serialize them, so seeing one in a
# batch is a producer bug and is rejected.
SD_VFS_OP_CREATE = 1
SD_VFS_OP_WRITE = 2
SD_VFS_OP_TRUNCATE = 3
SD_VFS_OP_UNLINK = 4
SD_VFS_OP_RENAME = 5
SD_VFS_OP_MKDIR = 6
SD_VFS_OP_RMDIR = 7
SD_VFS_OP_ZERO_RANGE = 14

# Fallocate-as-zero-range has no FLG3 equivalent, so it needs an SD_TYPE of
# its own rather than being folded into WRITE or TRUNCATE (it is neither: it
# zeroes a range and may or may not extend the file, per KEEP_SIZE).
SD_TYPE_ZERO_RANGE = 11

SD_VFS_ZERO_RANGE_F_KEEP_SIZE = 0x0001

_VFS1_OP_TO_SD_TYPE = {
    SD_VFS_OP_CREATE: SD_TYPE_CREATE,
    SD_VFS_OP_WRITE: SD_TYPE_WRITE,
    SD_VFS_OP_TRUNCATE: SD_TYPE_TRUNCATE,
    SD_VFS_OP_UNLINK: SD_TYPE_UNLINK,
    SD_VFS_OP_RENAME: SD_TYPE_RENAME,
    SD_VFS_OP_MKDIR: SD_TYPE_MKDIR,
    SD_VFS_OP_RMDIR: SD_TYPE_RMDIR,
    SD_VFS_OP_ZERO_RANGE: SD_TYPE_ZERO_RANGE,
}

# struct statediff_vfs_file_header: magic u32, version u16, flags u16,
# record_count u64, payload_size u64 -- __attribute__((packed)).
VFS1_FILE_HEADER = struct.Struct("<IHHQQ")
# struct statediff_vfs_record_header: seq u64, op u32, flags u32, offset u64,
# size u64, mode u32, path_len u32, new_path_len u32, data_len u32 -- packed.
VFS1_RECORD_HEADER = struct.Struct("<QIIQQIIII")
assert VFS1_FILE_HEADER.size == 24, VFS1_FILE_HEADER.size
assert VFS1_RECORD_HEADER.size == 48, VFS1_RECORD_HEADER.size

# Mirrors MAX_REPLAY_RECORDS / MAX_REPLAY_DATA in statediff_apply_vfs.c, so
# this parser rejects exactly what the replay binary would reject.
VFS1_MAX_RECORDS = 1_000_000
VFS1_MAX_DATA = 16 * 1024 * 1024
_VFS1_PATH_MAX = 4096


def vfs1_path_is_safe(path: str) -> bool:
    """Mirror of path_is_safe() in statediff_apply_vfs.c.

    A serialized path must stay relative and inside the target root, so an
    empty path, an absolute one, or any "." / ".." component is rejected.
    Checking it here means a batch that would escape the root is caught as a
    ParseError during harvesting rather than only at replay time."""
    if not path or path.startswith("/") or len(path) >= _VFS1_PATH_MAX:
        return False
    return not any(part in (".", "..") for part in path.split("/"))


class Vfs1Parser(WireFormatParser):
    """Parser for the eBPF capturer's VFS1 batch layout.

    Layout, little-endian throughout:
      - file header: magic u32 ("VFS1"), version u16, flags u16,
        record_count u64, payload_size u64
      - record_count records, each:
          seq u64, op u32, flags u32, offset u64, size u64, mode u32,
          path_len u32, new_path_len u32, data_len u32
          path[path_len]                 (not NUL-terminated)
          new_path[new_path_len]         (RENAME only)
          data[data_len]                 (WRITE only)

    payload_size counts the records only -- headers, paths and data -- and
    excludes the 24-byte file header. Over the socket the length prefix is
    24 + payload_size, and an empty capture is signalled by a length of 0
    with no header at all, which the Harvester handles before calling here.

    VFS1 carries no file table, so fid_to_path is synthesized: each distinct
    path gets a stable synthetic fid for this batch. That keeps ParsedBatch's
    shape (and the Harvester's fid check) working unchanged, and the check is
    trivially satisfied by construction -- correctly so, since VFS1 has no fid
    table that could tear.
    """

    def parse_batch(self, raw_bytes: bytes) -> ParsedBatch:
        return self.parse_batch_from_reader(_BufferReader(raw_bytes))

    def parse_batch_from_reader(self, reader: "_Reader") -> ParsedBatch:
        magic, version, flags, record_count, payload_size = \
            VFS1_FILE_HEADER.unpack(reader.read(VFS1_FILE_HEADER.size))

        if magic != VFS1_MAGIC:
            raise ParseError(
                f"bad VFS1 magic 0x{magic:08x} (expected "
                f"0x{VFS1_MAGIC:08x}) -- wrong wire format for this parser"
            )
        if version != VFS1_VERSION:
            # The format is explicitly versioned so an unknown version is
            # rejected rather than reinterpreted as version 1.
            raise ParseError(
                f"unsupported VFS1 version {version} (this parser "
                f"understands {VFS1_VERSION})"
            )
        if record_count > VFS1_MAX_RECORDS:
            raise ParseError(
                f"record_count {record_count} exceeds the replay ceiling "
                f"({VFS1_MAX_RECORDS})"
            )

        fid_to_path: Dict[int, str] = {}
        path_to_fid: Dict[str, int] = {}
        statediffs: List[StateDiff] = []
        accounted = 0

        def fid_for(path: str) -> int:
            if path not in path_to_fid:
                fid = len(path_to_fid) + 1
                path_to_fid[path] = fid
                fid_to_path[fid] = path
            return path_to_fid[path]

        for i in range(record_count):
            (seq, op, rec_flags, offset, size, mode,
             path_len, new_path_len, data_len) = VFS1_RECORD_HEADER.unpack(
                reader.read(VFS1_RECORD_HEADER.size))

            if data_len > VFS1_MAX_DATA:
                raise ParseError(
                    f"record {i + 1} (seq={seq}) declares {data_len} bytes "
                    f"of data, over the replay ceiling ({VFS1_MAX_DATA})"
                )

            path = self._read_path(reader, path_len, i, seq, "path")
            new_path = (self._read_path(reader, new_path_len, i, seq, "new_path")
                        if new_path_len else None)
            data = reader.read(data_len) if data_len else None

            accounted += (VFS1_RECORD_HEADER.size + path_len
                          + new_path_len + data_len)

            sd_type = _VFS1_OP_TO_SD_TYPE.get(op)
            if sd_type is None:
                raise ParseError(
                    f"record {i + 1} (seq={seq}) has op {op}, which is not a "
                    f"serializable VFS1 operation -- internal-only opcodes "
                    f"(mmap, writeback, native-AIO staging) must be converted "
                    f"to their logical effects before serialization"
                )

            self._check_shape(i, seq, op, new_path, data_len, size)
            statediffs.append(self._to_statediff(
                sd_type, op, fid_for(path),
                fid_for(new_path) if new_path else None,
                offset, size, mode, rec_flags, data))

        # The producer computes payload_size as the exact sum of every
        # record's header, path(s) and data, so a mismatch means the batch was
        # framed against a different set of records than it carries.
        if accounted != payload_size:
            raise ParseError(
                f"framing mismatch: header declares payload_size="
                f"{payload_size} but {record_count} record(s) account for "
                f"{accounted} bytes"
            )
        if flags:
            raise ParseError(f"unexpected VFS1 header flags 0x{flags:04x}")

        return ParsedBatch(fid_to_path=fid_to_path, statediffs=statediffs)

    @staticmethod
    def _read_path(reader, length: int, idx: int, seq: int, which: str) -> str:
        # read_string() in statediff_apply_vfs.c rejects a zero length and
        # anything at or past PATH_MAX before it reads a byte.
        if length == 0 or length >= _VFS1_PATH_MAX:
            raise ParseError(
                f"record {idx + 1} (seq={seq}) has an out-of-range "
                f"{which}_len of {length}"
            )
        raw = reader.read(length)
        path = raw.decode("utf-8", errors="replace")
        if not vfs1_path_is_safe(path):
            raise ParseError(
                f"record {idx + 1} (seq={seq}) carries an unsafe {which} "
                f"{path!r}: serialized paths must stay relative and must not "
                f"traverse outside the target root"
            )
        return path

    @staticmethod
    def _check_shape(idx: int, seq: int, op: int, new_path, data_len: int,
                     size: int) -> None:
        """Reject records whose shape statediff_apply_vfs.c would reject.

        Each replay arm asserts that fields the operation does not use are
        absent, so a malformed batch fails closed instead of being applied
        with a silently ignored field. Enforcing the same rules here means a
        producer bug surfaces at harvest time with the record number attached,
        rather than as a bare non-zero exit from the replay binary."""
        where = f"record {idx + 1} (seq={seq}, op={op})"
        if op == SD_VFS_OP_RENAME:
            if new_path is None:
                raise ParseError(f"{where}: RENAME without a new path")
            if data_len:
                raise ParseError(f"{where}: RENAME must not carry data")
            return
        if new_path is not None:
            raise ParseError(f"{where}: only RENAME may carry a new path")
        if op == SD_VFS_OP_WRITE:
            if data_len != size:
                raise ParseError(
                    f"{where}: WRITE declares size={size} but carries "
                    f"{data_len} bytes of data"
                )
            return
        if data_len:
            raise ParseError(
                f"{where}: only WRITE may carry data, but {data_len} "
                f"bytes are present"
            )

    @staticmethod
    def _to_statediff(sd_type, op, fid, to_fid, offset, size, mode, flags,
                      data) -> StateDiff:
        if op == SD_VFS_OP_WRITE:
            return StateDiff(sd_type=sd_type, fid=fid, offset=offset,
                             size=size, data=data)
        if op == SD_VFS_OP_RENAME:
            return StateDiff(sd_type=sd_type, fid=fid, to_fid=to_fid)
        if op == SD_VFS_OP_TRUNCATE:
            return StateDiff(sd_type=sd_type, fid=fid, size=size)
        if op == SD_VFS_OP_ZERO_RANGE:
            # offset/size carry the range; KEEP_SIZE rides in the record
            # flags, which StateDiff has no field for, so it is folded into
            # mode -- the only free slot -- and documented here rather than
            # widening the shared dataclass for one op.
            return StateDiff(sd_type=sd_type, fid=fid, offset=offset,
                             size=size, mode=flags)
        if op in (SD_VFS_OP_CREATE, SD_VFS_OP_MKDIR):
            return StateDiff(sd_type=sd_type, fid=fid, mode=mode)
        # UNLINK, RMDIR
        return StateDiff(sd_type=sd_type, fid=fid)
