import pytest

from mm_bt.core import SchemaError
from mm_bt.core.types import Lots, QuoteAtoms, Side, Ticks, TsNs
from mm_bt.strategy.api import BookSnapshot, StrategyContext
from mm_bt.strategy.dummy import RandomMarketOrderStrategy


def _ctx(ts_ns: int = 0) -> StrategyContext:
    return StrategyContext(
        ts_recv_ns=TsNs(ts_ns),
        cash=QuoteAtoms(0),
        position=Lots(0),
    )


def _book() -> BookSnapshot:
    return BookSnapshot(
        bid_px=Ticks(1),
        bid_qty=Lots(1),
        ask_px=Ticks(2),
        ask_qty=Lots(1),
    )


def test_random_strategy_deterministic() -> None:
    s1 = RandomMarketOrderStrategy(
        seed=7,
        order_pct=100,
        min_qty_lots=Lots(1),
        max_qty_lots=Lots(3),
    )
    s2 = RandomMarketOrderStrategy(
        seed=7,
        order_pct=100,
        min_qty_lots=Lots(1),
        max_qty_lots=Lots(3),
    )
    out1 = [s1.on_batch(_ctx(i), _book())[0] for i in range(5)]
    out2 = [s2.on_batch(_ctx(i), _book())[0] for i in range(5)]
    assert out1 == out2


def test_random_strategy_bounds() -> None:
    s = RandomMarketOrderStrategy(
        seed=1,
        order_pct=100,
        min_qty_lots=Lots(2),
        max_qty_lots=Lots(2),
    )
    order = s.on_batch(_ctx(), _book())[0]
    assert order.qty_lots == Lots(2)
    assert order.side in (Side.BID, Side.ASK)


def test_random_strategy_rejects_invalid_inputs() -> None:
    with pytest.raises(SchemaError):
        RandomMarketOrderStrategy(
            seed=1,
            order_pct=-1,
            min_qty_lots=Lots(1),
            max_qty_lots=Lots(1),
        )
    with pytest.raises(SchemaError):
        RandomMarketOrderStrategy(
            seed=1,
            order_pct=101,
            min_qty_lots=Lots(1),
            max_qty_lots=Lots(1),
        )
    with pytest.raises(SchemaError):
        RandomMarketOrderStrategy(
            seed=1,
            order_pct=10,
            min_qty_lots=Lots(0),
            max_qty_lots=Lots(1),
        )
    with pytest.raises(SchemaError):
        RandomMarketOrderStrategy(
            seed=1,
            order_pct=10,
            min_qty_lots=Lots(2),
            max_qty_lots=Lots(1),
        )
