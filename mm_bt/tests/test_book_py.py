import pytest

from mm_bt.book import BookPy
from mm_bt.core import SchemaError
from mm_bt.core import Lots, Side, Ticks, TsNs
from mm_bt.evlog import L2Batch, L2Update


def _batch(resets: bool, updates) -> L2Batch:
    return L2Batch(
        ts_recv_ns=TsNs(0),
        ts_exch_ns=TsNs(0),
        resets_book=resets,
        updates=tuple(updates),
    )


def test_apply_updates_and_best() -> None:
    book = BookPy()
    batch = _batch(
        True,
        [
            L2Update(Side.BID, Ticks(10), Lots(1), True),
            L2Update(Side.ASK, Ticks(11), Lots(2), True),
        ],
    )
    book.apply_l2_batch(batch)
    bid_px, bid_qty, ask_px, ask_qty = book.best_bid_ask()
    assert int(bid_px) == 10
    assert int(bid_qty) == 1
    assert int(ask_px) == 11
    assert int(ask_qty) == 2


def test_delete_level() -> None:
    book = BookPy()
    book.apply_l2_batch(
        _batch(
            True,
            [L2Update(Side.BID, Ticks(10), Lots(1), True)],
        )
    )
    book.apply_l2_batch(
        _batch(
            False,
            [L2Update(Side.BID, Ticks(10), Lots(0), False)],
        )
    )
    bid_px, bid_qty, _, _ = book.best_bid_ask()
    assert bid_px is None
    assert bid_qty is None


def test_duplicate_updates_last_wins() -> None:
    book = BookPy()
    book.apply_l2_batch(
        _batch(
            True,
            [
                L2Update(Side.BID, Ticks(10), Lots(1), True),
                L2Update(Side.BID, Ticks(10), Lots(3), True),
            ],
        )
    )
    bid_px, bid_qty, _, _ = book.best_bid_ask()
    assert int(bid_px) == 10
    assert int(bid_qty) == 3


def test_levels_ordering() -> None:
    book = BookPy()
    book.apply_l2_batch(
        _batch(
            True,
            [
                L2Update(Side.BID, Ticks(10), Lots(1), True),
                L2Update(Side.BID, Ticks(12), Lots(2), True),
                L2Update(Side.BID, Ticks(11), Lots(3), True),
                L2Update(Side.ASK, Ticks(20), Lots(4), True),
                L2Update(Side.ASK, Ticks(19), Lots(5), True),
            ],
        )
    )
    bid_prices, bid_sizes = book.levels(Side.BID, 2)
    assert [int(p) for p in bid_prices] == [12, 11]
    assert [int(s) for s in bid_sizes] == [2, 3]
    ask_prices, ask_sizes = book.levels(Side.ASK, 2)
    assert [int(p) for p in ask_prices] == [19, 20]
    assert [int(s) for s in ask_sizes] == [5, 4]


def test_crossing_rejected() -> None:
    book = BookPy(reject_crossed=True)
    batch = _batch(
        True,
        [
            L2Update(Side.BID, Ticks(10), Lots(1), True),
            L2Update(Side.ASK, Ticks(9), Lots(1), True),
        ],
    )
    with pytest.raises(SchemaError):
        book.apply_l2_batch(batch)


def test_incremental_updates_preserve_other_levels() -> None:
    book = BookPy()
    book.apply_l2_batch(
        _batch(
            True,
            [
                L2Update(Side.BID, Ticks(10), Lots(1), True),
                L2Update(Side.ASK, Ticks(11), Lots(2), True),
                L2Update(Side.ASK, Ticks(12), Lots(3), True),
            ],
        )
    )
    book.apply_l2_batch(
        _batch(
            False,
            [
                L2Update(Side.ASK, Ticks(11), Lots(0), False),
            ],
        )
    )
    bid_px, bid_qty, ask_px, ask_qty = book.best_bid_ask()
    assert int(bid_px) == 10
    assert int(bid_qty) == 1
    assert int(ask_px) == 12
    assert int(ask_qty) == 3
    ask_prices, _ = book.levels(Side.ASK, 10)
    assert [int(p) for p in ask_prices] == [12]
