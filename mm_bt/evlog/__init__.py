"""Binary event log (format, reader, writer, index)."""

from __future__ import annotations

from mm_bt.evlog.format import (
    ENDIAN_LITTLE,
    MAGIC,
    VERSION,
    EvlogHeader,
    RecordType,
    pack_header,
    read_header,
    unpack_header,
    unpack_record_header,
)
from mm_bt.evlog.index import IndexEntry, read_index, write_index
from mm_bt.evlog.reader import EvlogReader
from mm_bt.evlog.types import L2Batch, L2Update
from mm_bt.evlog.writer import EvlogWriter

EVLOG_VERSION = VERSION

__all__ = [
    "ENDIAN_LITTLE",
    "EVLOG_VERSION",
    "EvlogHeader",
    "EvlogReader",
    "EvlogWriter",
    "IndexEntry",
    "L2Batch",
    "L2Update",
    "MAGIC",
    "RecordType",
    "VERSION",
    "pack_header",
    "read_header",
    "read_index",
    "unpack_header",
    "unpack_record_header",
    "write_index",
]
