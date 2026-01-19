"""Binary event log writer."""

from __future__ import annotations

import struct
from pathlib import Path
from typing import BinaryIO

from mm_bt.core.errors import SchemaError
from mm_bt.core.types import Side
from mm_bt.evlog.format import (
    L2_BATCH_HEADER_FMT,
    L2_BATCH_HEADER_SIZE,
    L2_UPDATE_FMT,
    L2_UPDATE_SIZE,
    RECORD_HEADER_FMT,
    RECORD_HEADER_SIZE,
    RecordType,
    pack_header,
)
from mm_bt.evlog.types import L2Batch


def _require_int64(value: int, field: str) -> None:
    if value < -(1 << 63) or value > (1 << 63) - 1:
        raise SchemaError(f"{field} out of int64 range: {value}")


class EvlogWriter:
    def __init__(
        self,
        path: str | Path,
        *,
        exchange_id: int = 0,
        symbol_id: int = 0,
        quantizer_hash: bytes | None = None,
    ) -> None:
        self._path = Path(path)
        self._file: BinaryIO | None = None
        self._exchange_id = exchange_id
        self._symbol_id = symbol_id
        self._quantizer_hash = quantizer_hash

    def __enter__(self) -> "EvlogWriter":
        self._file = self._path.open("wb")
        self._file.write(
            pack_header(
                exchange_id=self._exchange_id,
                symbol_id=self._symbol_id,
                quantizer_hash=self._quantizer_hash,
            )
        )
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None

    def tell(self) -> int:
        if self._file is None:
            raise SchemaError("writer is closed")
        return int(self._file.tell())

    def write_l2_batch(self, batch: L2Batch) -> None:
        if self._file is None:
            raise SchemaError("writer is closed")

        ts_recv_ns = int(batch.ts_recv_ns)
        ts_exch_ns = int(batch.ts_exch_ns)
        _require_int64(ts_recv_ns, "ts_recv_ns")
        _require_int64(ts_exch_ns, "ts_exch_ns")
        if ts_recv_ns < 0 or ts_exch_ns < 0:
            raise SchemaError("negative timestamp")

        updates = batch.updates
        update_count = len(updates)
        if update_count < 0 or update_count > (1 << 32) - 1:
            raise SchemaError(f"update_count out of u32 range: {update_count}")
        payload_len = L2_BATCH_HEADER_SIZE + update_count * L2_UPDATE_SIZE
        if payload_len % 8 != 0:
            raise SchemaError("l2 payload size not 8-byte aligned")

        payload = bytearray(payload_len)
        struct.pack_into(
            L2_BATCH_HEADER_FMT,
            payload,
            0,
            ts_recv_ns,
            ts_exch_ns,
            1 if batch.resets_book else 0,
            update_count,
        )

        offset = L2_BATCH_HEADER_SIZE
        for update in updates:
            side_value = int(update.side)
            if side_value not in (Side.BID, Side.ASK):
                raise SchemaError(f"invalid side: {side_value}")
            price_ticks = int(update.price_ticks)
            amount_lots = int(update.amount_lots)
            _require_int64(price_ticks, "price_ticks")
            _require_int64(amount_lots, "amount_lots")
            if price_ticks <= 0:
                raise SchemaError("non-positive price_ticks")
            if amount_lots < 0:
                raise SchemaError("negative amount_lots")
            struct.pack_into(
                L2_UPDATE_FMT,
                payload,
                offset,
                side_value,
                1 if update.is_snapshot else 0,
                0,
                price_ticks,
                amount_lots,
                0,
            )
            offset += L2_UPDATE_SIZE

        record_header = struct.pack(
            RECORD_HEADER_FMT,
            int(RecordType.L2_BATCH),
            0,
            0,
            payload_len,
        )
        if len(record_header) != RECORD_HEADER_SIZE:
            raise SchemaError("record header size mismatch")
        self._file.write(record_header)
        self._file.write(payload)

