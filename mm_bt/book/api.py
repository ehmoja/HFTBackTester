"""Order book API."""

from __future__ import annotations

from typing import Protocol

from mm_bt.core.types import Lots, Side, Ticks
from mm_bt.evlog.types import L2Batch


class Book(Protocol):
    def reset(self) -> None:
        """Reset the book to empty."""

    def apply_l2_batch(self, batch: L2Batch) -> None:
        """Apply a batch of L2 updates."""

    def best_bid_ask(
        self,
    ) -> tuple[Ticks | None, Lots | None, Ticks | None, Lots | None]:
        """Return best bid price/size and best ask price/size."""

    def levels(
        self, side: Side, depth: int
    ) -> tuple[tuple[Ticks, ...], tuple[Lots, ...]]:
        """Return price/size levels up to depth for a side."""

