"""
Wire-format parsers for fuselog-family statediff batches.

The harvest socket protocol and the statediff batch layout are two
different things:
  - The SOCKET PROTOCOL (how you ask for a batch) is implementation-specific
    setup, handled by the Driver, not this module.
  - The BATCH LAYOUT (the bytes you get back once you've asked) is what a
    WireFormatParser understands.

Only one concrete parser exists today: FuselogV2Parser, matching the
"version 2" (apply2()) layout in fuselog-apply.cpp, verified byte-for-byte
against that source rather than inferred. If a second implementation
(different language, different wire format) shows up later, it gets its
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
