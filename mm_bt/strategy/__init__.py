"""Strategy interfaces and basic test strategies."""

from __future__ import annotations

from mm_bt.strategy.api import (
    Action,
    BookSnapshot,
    MarketOrder,
    Strategy,
    StrategyContext,
)
from mm_bt.strategy.dummy import (
    AlternatingMarketOrderStrategy,
    RandomMarketOrderStrategy,
)

__all__ = [
    "Action",
    "AlternatingMarketOrderStrategy",
    "BookSnapshot",
    "MarketOrder",
    "RandomMarketOrderStrategy",
    "Strategy",
    "StrategyContext",
]
