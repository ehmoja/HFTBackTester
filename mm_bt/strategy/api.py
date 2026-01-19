"""Strategy interface for the backtest engine."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from mm_bt.core.types import Lots, QuoteAtoms, Side, Ticks, TsNs


@dataclass(frozen=True, slots=True)
class BookSnapshot:
    bid_px: Ticks
    bid_qty: Lots
    ask_px: Ticks
    ask_qty: Lots


@dataclass(frozen=True, slots=True)
class StrategyContext:
    ts_recv_ns: TsNs
    cash: QuoteAtoms
    position: Lots


@dataclass(frozen=True, slots=True)
class MarketOrder:
    side: Side
    qty_lots: Lots


Action = MarketOrder


class Strategy(Protocol):
    def on_batch(
        self, ctx: StrategyContext, book: BookSnapshot
    ) -> tuple[Action, ...]:
        """Return actions for the current batch."""

