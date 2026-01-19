"""Tardis CSV readers."""

from __future__ import annotations

import csv
import gzip
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, TextIO

from mm_bt.core.errors import SchemaError
from mm_bt.core.types import Side, parse_side

L2_HEADER = [
    "exchange",
    "symbol",
    "timestamp",
    "local_timestamp",
    "is_snapshot",
    "side",
    "price",
    "amount",
]


@dataclass(frozen=True, slots=True)
class L2Row:
    exchange: str
    symbol: str
    timestamp_us: int
    local_timestamp_us: int
    is_snapshot: bool
    side: Side
    price: str
    amount: str
    line_number: int
    source: str


def _parse_int_field(value: str, field: str) -> int:
    if value == "":
        raise SchemaError(f"{field} empty")
    if not value.isdigit():
        raise SchemaError(f"{field} not an integer: {value!r}")
    return int(value)


def _require_str(value: str, field: str) -> str:
    if value == "":
        raise SchemaError(f"{field} empty")
    return value


def _parse_bool_field(value: str, field: str) -> bool:
    v = value.lower()
    if v == "true":
        return True
    if v == "false":
        return False
    raise SchemaError(f"{field} invalid: {value!r}")


def _open_csv(path: str | Path) -> TextIO:
    p = Path(path)
    if p.suffix == ".gz":
        return gzip.open(p, mode="rt", encoding="utf-8", newline="")
    return p.open("rt", encoding="utf-8", newline="")


def iter_l2_rows(path: str | Path) -> Iterator[L2Row]:
    p = Path(path)
    with _open_csv(p) as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration as exc:
            raise SchemaError("empty CSV") from exc
        if header != L2_HEADER:
            raise SchemaError(f"unexpected header: {header!r}")

        for line_number, row in enumerate(reader, start=2):
            if len(row) != len(L2_HEADER):
                raise SchemaError(
                    f"row length {len(row)} != {len(L2_HEADER)} at line {line_number}"
                )
            exchange, symbol, ts, local_ts, is_snapshot, side, price, amount = row
            exchange = _require_str(exchange, "exchange")
            symbol = _require_str(symbol, "symbol")
            timestamp_us = _parse_int_field(ts, "timestamp")
            local_timestamp_us = _parse_int_field(local_ts, "local_timestamp")
            if timestamp_us < 0 or local_timestamp_us < 0:
                raise SchemaError(f"negative timestamp at line {line_number}")
            yield L2Row(
                exchange=exchange,
                symbol=symbol,
                timestamp_us=timestamp_us,
                local_timestamp_us=local_timestamp_us,
                is_snapshot=_parse_bool_field(is_snapshot, "is_snapshot"),
                side=parse_side(side),
                price=price,
                amount=amount,
                line_number=line_number,
                source=str(p),
            )

