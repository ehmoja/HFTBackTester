"""Event log time index (ts_recv_ns -> file offset)."""

from __future__ import annotations

from dataclasses import dataclass
import struct
from pathlib import Path
from typing import BinaryIO, Iterable

from mm_bt.core.errors import SchemaError

INDEX_MAGIC = b"MMEVLIDX"
INDEX_VERSION = 0
ENDIAN_LITTLE = 1

INDEX_HEADER_FMT = "<8sB B H I"
INDEX_HEADER_SIZE = struct.calcsize(INDEX_HEADER_FMT)

INDEX_ENTRY_FMT = "<q q"
INDEX_ENTRY_SIZE = struct.calcsize(INDEX_ENTRY_FMT)


@dataclass(frozen=True, slots=True)
class IndexEntry:
    ts_recv_ns: int
    offset: int


def _pack_header() -> bytes:
    return struct.pack(
        INDEX_HEADER_FMT,
        INDEX_MAGIC,
        INDEX_VERSION,
        ENDIAN_LITTLE,
        0,
        0,
    )


def _unpack_header(data: bytes) -> None:
    if len(data) != INDEX_HEADER_SIZE:
        raise SchemaError("invalid index header size")
    magic, version, endian, _flags, reserved = struct.unpack(
        INDEX_HEADER_FMT, data
    )
    if magic != INDEX_MAGIC:
        raise SchemaError("invalid index magic")
    if version != INDEX_VERSION:
        raise SchemaError(f"unsupported index version: {version}")
    if endian != ENDIAN_LITTLE:
        raise SchemaError(f"unsupported index endian: {endian}")
    if reserved != 0:
        raise SchemaError("non-zero index reserved field")


def _write_entries(f: BinaryIO, entries: Iterable[IndexEntry]) -> int:
    count = 0
    prev_ts: int | None = None
    prev_offset: int | None = None
    for entry in entries:
        if entry.ts_recv_ns < 0:
            raise SchemaError(f"negative index timestamp: {entry.ts_recv_ns}")
        if entry.offset < 0:
            raise SchemaError(f"negative index offset: {entry.offset}")
        if prev_ts is not None and entry.ts_recv_ns < prev_ts:
            raise SchemaError("index timestamps not monotone")
        if prev_offset is not None and entry.offset <= prev_offset:
            raise SchemaError("index offsets not increasing")
        f.write(struct.pack(INDEX_ENTRY_FMT, entry.ts_recv_ns, entry.offset))
        prev_ts = entry.ts_recv_ns
        prev_offset = entry.offset
        count += 1
    return count


def write_index(path: str | Path, entries: Iterable[IndexEntry]) -> int:
    p = Path(path)
    with p.open("wb") as f:
        f.write(_pack_header())
        return _write_entries(f, entries)


def read_index(path: str | Path) -> list[IndexEntry]:
    p = Path(path)
    with p.open("rb") as f:
        header = f.read(INDEX_HEADER_SIZE)
        _unpack_header(header)
        data = f.read()
    if len(data) % INDEX_ENTRY_SIZE != 0:
        raise SchemaError("index payload size mismatch")
    entries: list[IndexEntry] = []
    prev_ts: int | None = None
    prev_offset: int | None = None
    for offset in range(0, len(data), INDEX_ENTRY_SIZE):
        ts_recv_ns, rec_offset = struct.unpack_from(
            INDEX_ENTRY_FMT, data, offset
        )
        if ts_recv_ns < 0:
            raise SchemaError("negative index timestamp")
        if rec_offset < 0:
            raise SchemaError("negative index offset")
        if prev_ts is not None and ts_recv_ns < prev_ts:
            raise SchemaError("index timestamps not monotone")
        if prev_offset is not None and rec_offset <= prev_offset:
            raise SchemaError("index offsets not increasing")
        entries.append(IndexEntry(ts_recv_ns=ts_recv_ns, offset=rec_offset))
        prev_ts = ts_recv_ns
        prev_offset = rec_offset
    return entries
