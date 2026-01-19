"""Dummy strategies for smoke testing."""

from __future__ import annotations

import random

from mm_bt.core.errors import SchemaError
from mm_bt.core.types import Lots, Side
from mm_bt.strategy.api import BookSnapshot, MarketOrder, Strategy, StrategyContext


class AlternatingMarketOrderStrategy(Strategy):
    def __init__(self, qty_lots: Lots) -> None:
        qty = int(qty_lots)
        if qty <= 0:
            raise SchemaError("qty_lots must be positive")
        self._qty_lots = qty_lots
        self._next_side = Side.BID

    def on_batch(
        self, ctx: StrategyContext, book: BookSnapshot
    ) -> tuple[MarketOrder, ...]:
        order = MarketOrder(side=self._next_side, qty_lots=self._qty_lots)
        self._next_side = Side.ASK if self._next_side == Side.BID else Side.BID
        return (order,)


class RandomMarketOrderStrategy(Strategy):
    """Deterministic per-batch random market orders (seeded RNG)."""

    def __init__(
        self,
        *,
        seed: int,
        order_pct: int,
        min_qty_lots: Lots,
        max_qty_lots: Lots,
    ) -> None:
        if order_pct < 0 or order_pct > 100:
            raise SchemaError("order_pct must be in [0, 100]")
        min_qty = int(min_qty_lots)
        max_qty = int(max_qty_lots)
        if min_qty <= 0:
            raise SchemaError("min_qty_lots must be positive")
        if max_qty < min_qty:
            raise SchemaError("max_qty_lots must be >= min_qty_lots")
        self._rng = random.Random(seed)
        self._order_pct = order_pct
        self._min_qty = min_qty
        self._max_qty = max_qty

    def on_batch(
        self, ctx: StrategyContext, book: BookSnapshot
    ) -> tuple[MarketOrder, ...]:
        if self._order_pct == 0:
            return ()
        if self._order_pct < 100 and self._rng.randrange(100) >= self._order_pct:
            return ()
        side = Side.BID if self._rng.randrange(2) == 0 else Side.ASK
        if self._min_qty == self._max_qty:
            qty = self._min_qty
        else:
            qty = self._rng.randrange(self._min_qty, self._max_qty + 1)
        return (MarketOrder(side=side, qty_lots=Lots(qty)),)
