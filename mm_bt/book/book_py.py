"""Reference L2 book implementation (correctness-first).

Applies incremental level updates. `batch.resets_book` clears the full book
before applying the batch; otherwise updates only touch referenced levels and
explicit deletes (amount==0) remove that price level.
"""

from __future__ import annotations

import bisect

from mm_bt.core.errors import SchemaError
from mm_bt.core.types import Lots, Side, Ticks
from mm_bt.evlog.types import L2Batch, L2Update


def _insert_price(prices: list[int], price: int) -> None:
    idx = bisect.bisect_left(prices, price)
    if idx < len(prices) and prices[idx] == price:
        return
    prices.insert(idx, price)


def _remove_price(prices: list[int], price: int) -> None:
    idx = bisect.bisect_left(prices, price)
    if idx < len(prices) and prices[idx] == price:
        prices.pop(idx)


class BookPy:
    def __init__(self, *, reject_crossed: bool = True) -> None:
        self._reject_crossed = reject_crossed
        self.reset()

    def reset(self) -> None:
        self._bids: dict[int, int] = {}
        self._asks: dict[int, int] = {}
        self._bid_prices: list[int] = []
        self._ask_prices: list[int] = []

    def apply_l2_batch(self, batch: L2Batch) -> None:
        if batch.resets_book:
            self.reset()
        for update in batch.updates:
            self._apply_update(update)
        if self._reject_crossed:
            self._check_crossed()

    def _apply_update(self, update: L2Update) -> None:
        price = int(update.price_ticks)
        amount = int(update.amount_lots)
        if price <= 0:
            raise SchemaError(f"non-positive price: {price}")
        if amount < 0:
            raise SchemaError(f"negative amount: {amount}")

        if update.side == Side.BID:
            self._apply_level(self._bids, self._bid_prices, price, amount)
        elif update.side == Side.ASK:
            self._apply_level(self._asks, self._ask_prices, price, amount)
        else:
            raise SchemaError(f"unknown side: {update.side}")

    @staticmethod
    def _apply_level(
        levels: dict[int, int],
        prices: list[int],
        price: int,
        amount: int,
    ) -> None:
        if amount == 0:
            if price in levels:
                del levels[price]
                _remove_price(prices, price)
            return
        levels[price] = amount
        _insert_price(prices, price)

    def _check_crossed(self) -> None:
        if not self._bid_prices or not self._ask_prices:
            return
        if self._bid_prices[-1] >= self._ask_prices[0]:
            raise SchemaError("crossed book")

    def best_bid_ask(
        self,
    ) -> tuple[Ticks | None, Lots | None, Ticks | None, Lots | None]:
        if not self._bid_prices:
            bid_px = None
            bid_qty = None
        else:
            bid_px_val = self._bid_prices[-1]
            bid_px = Ticks(bid_px_val)
            bid_qty = Lots(self._bids[bid_px_val])

        if not self._ask_prices:
            ask_px = None
            ask_qty = None
        else:
            ask_px_val = self._ask_prices[0]
            ask_px = Ticks(ask_px_val)
            ask_qty = Lots(self._asks[ask_px_val])

        return bid_px, bid_qty, ask_px, ask_qty

    def levels(
        self, side: Side, depth: int
    ) -> tuple[tuple[Ticks, ...], tuple[Lots, ...]]:
        if depth <= 0:
            return (), ()
        if side == Side.BID:
            prices = self._bid_prices
            levels = self._bids
            ordered = reversed(prices)
        elif side == Side.ASK:
            prices = self._ask_prices
            levels = self._asks
            ordered = iter(prices)
        else:
            raise SchemaError(f"unknown side: {side}")

        out_prices: list[Ticks] = []
        out_sizes: list[Lots] = []
        for price in ordered:
            out_prices.append(Ticks(price))
            out_sizes.append(Lots(levels[price]))
            if len(out_prices) >= depth:
                break
        return tuple(out_prices), tuple(out_sizes)
