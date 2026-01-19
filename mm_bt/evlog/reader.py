"""Binary event log reader."""

from __future__ import annotations

import bisect
import struct
from pathlib import Path
from typing import BinaryIO, Iterator

from mm_bt.core.errors import SchemaError
from mm_bt.core.types import Lots, Side, Ticks, TsNs
from mm_bt.evlog.format import (
    L2_BATCH_HEADER_FMT,
    L2_BATCH_HEADER_SIZE,
    L2_UPDATE_FMT,
    L2_UPDATE_SIZE,
    RECORD_HEADER_SIZE,
    RecordType,
    read_header,
    unpack_record_header,
)
from mm_bt.evlog.index import IndexEntry, read_index
from mm_bt.evlog.types import L2Batch, L2Update


def _decode_l2_payload(payload: bytes) -> L2Batch:
    if len(payload) < L2_BATCH_HEADER_SIZE:
        raise SchemaError("l2 payload too small")
    ts_recv_ns, ts_exch_ns, resets_book, update_count = struct.unpack_from(
        L2_BATCH_HEADER_FMT, payload, 0
    )
    if ts_recv_ns < 0 or ts_exch_ns < 0:
        raise SchemaError("negative timestamp")
    if resets_book not in (0, 1):
        raise SchemaError(f"invalid resets_book flag: {resets_book}")
    expected_len = L2_BATCH_HEADER_SIZE + update_count * L2_UPDATE_SIZE
    if len(payload) != expected_len:
        raise SchemaError("l2 payload size mismatch")

    updates: list[L2Update] = []
    offset = L2_BATCH_HEADER_SIZE
    for _ in range(update_count):
        side_value, is_snapshot, r16, price_ticks, amount_lots, r32 = (
            struct.unpack_from(L2_UPDATE_FMT, payload, offset)
        )
        if r16 != 0 or r32 != 0:
            raise SchemaError("non-zero l2 update reserved fields")
        try:
            side = Side(side_value)
        except ValueError as exc:
            raise SchemaError(f"invalid side: {side_value}") from exc
        if is_snapshot not in (0, 1):
            raise SchemaError(f"invalid is_snapshot flag: {is_snapshot}")
        if price_ticks <= 0:
            raise SchemaError("non-positive price_ticks")
        if amount_lots < 0:
            raise SchemaError("negative amount_lots")
        updates.append(
            L2Update(
                side=side,
                price_ticks=Ticks(price_ticks),
                amount_lots=Lots(amount_lots),
                is_snapshot=bool(is_snapshot),
            )
        )
        offset += L2_UPDATE_SIZE

    return L2Batch(
        ts_recv_ns=TsNs(ts_recv_ns),
        ts_exch_ns=TsNs(ts_exch_ns),
        resets_book=bool(resets_book),
        updates=tuple(updates),
    )


class EvlogReader:
    def __init__(
        self,
        path: str | Path,
        *,
        index_path: str | Path | None = None,
    ) -> None:
        self._path = Path(path)
        self._file: BinaryIO | None = None
        self._header = None
        self._index: list[IndexEntry] | None = None
        if index_path is not None:
            self._index = read_index(index_path)
            self._index_ts = [entry.ts_recv_ns for entry in self._index]
        else:
            self._index_ts = None

    def __enter__(self) -> "EvlogReader":
        self._file = self._path.open("rb")
        self._header = read_header(self._file)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None

    def seek_time(self, ts_recv_ns: int) -> None:
        if self._file is None:
            raise SchemaError("reader is closed")
        if self._index is None or self._index_ts is None:
            raise SchemaError("index not available")
        idx = bisect.bisect_left(self._index_ts, ts_recv_ns)
        if idx >= len(self._index):
            self._file.seek(0, 2)
            return
        self._file.seek(self._index[idx].offset)

    def iter_l2_batches(self) -> Iterator[L2Batch]:
        if self._file is None:
            raise SchemaError("reader is closed")
        while True:
            header = self._file.read(RECORD_HEADER_SIZE)
            if not header:
                break
            if len(header) != RECORD_HEADER_SIZE:
                raise SchemaError("truncated record header")
            rec_type, payload_len = unpack_record_header(header)
            if payload_len % 8 != 0:
                raise SchemaError("payload length not 8-byte aligned")
            payload = self._file.read(payload_len)
            if len(payload) != payload_len:
                raise SchemaError("truncated payload")
            if rec_type == int(RecordType.L2_BATCH):
                yield _decode_l2_payload(payload)
            else:
                raise SchemaError(f"unknown record type: {rec_type}")

