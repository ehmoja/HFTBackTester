"""Core domain types."""

from __future__ import annotations

from enum import IntEnum
from typing import NewType

from mm_bt.core.errors import SchemaError

TsNs = NewType("TsNs", int)
Ticks = NewType("Ticks", int)
Lots = NewType("Lots", int)
QuoteAtoms = NewType("QuoteAtoms", int)
Bps = NewType("Bps", int)

Venue = NewType("Venue", str)
Symbol = NewType("Symbol", str)


class Side(IntEnum):
    BID = 0
    ASK = 1


def parse_side(value: str) -> Side:
    v = value.lower()
    if v == "bid":
        return Side.BID
    if v == "ask":
        return Side.ASK
    raise SchemaError(f"invalid side: {value!r}")

