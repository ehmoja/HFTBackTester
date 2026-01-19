"""Event log data structures."""

from __future__ import annotations

from dataclasses import dataclass

from mm_bt.core.types import Lots, Side, Ticks, TsNs


@dataclass(frozen=True, slots=True)
class L2Update:
    side: Side
    price_ticks: Ticks
    amount_lots: Lots
    is_snapshot: bool


@dataclass(frozen=True, slots=True)
class L2Batch:
    ts_recv_ns: TsNs
    ts_exch_ns: TsNs
    resets_book: bool
    updates: tuple[L2Update, ...]

