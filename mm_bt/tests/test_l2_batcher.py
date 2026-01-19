import pytest

from mm_bt.core import FailurePolicy, QuarantineAction
from mm_bt.core import OrderingError, QuantizationError, SchemaError
from mm_bt.core import Quantizer
from mm_bt.core import Side
from mm_bt.ingest import iter_l2_batches
from mm_bt.ingest import ListQuarantineSink
from mm_bt.io import L2Row


def _row(
    *,
    line: int,
    local_ts: int,
    exch_ts: int,
    is_snapshot: bool,
    side: Side,
    price: str,
    amount: str,
    source: str = "test.csv",
) -> L2Row:
    return L2Row(
        exchange="binance",
        symbol="BTCUSDT",
        timestamp_us=exch_ts,
        local_timestamp_us=local_ts,
        is_snapshot=is_snapshot,
        side=side,
        price=price,
        amount=amount,
        line_number=line,
        source=source,
    )


def test_batch_atomicity_and_ordering() -> None:
    q = Quantizer.from_strings("1", "1")
    rows = [
        _row(
            line=2,
            local_ts=1000,
            exch_ts=900,
            is_snapshot=True,
            side=Side.BID,
            price="10",
            amount="1",
        ),
        _row(
            line=3,
            local_ts=1000,
            exch_ts=905,
            is_snapshot=True,
            side=Side.ASK,
            price="11",
            amount="2",
        ),
    ]
    batches = list(
        iter_l2_batches(rows, q, failure_policy=FailurePolicy.HARD_FAIL)
    )
    assert len(batches) == 1
    batch = batches[0]
    assert int(batch.ts_recv_ns) == 1000 * 1_000
    assert int(batch.ts_exch_ns) == 905 * 1_000
    assert batch.resets_book is True
    assert [u.side for u in batch.updates] == [Side.BID, Side.ASK]


def test_reset_semantics() -> None:
    q = Quantizer.from_strings("1", "1")
    rows = [
        _row(
            line=2,
            local_ts=1000,
            exch_ts=900,
            is_snapshot=True,
            side=Side.BID,
            price="10",
            amount="1",
        ),
        _row(
            line=3,
            local_ts=2000,
            exch_ts=901,
            is_snapshot=False,
            side=Side.BID,
            price="10",
            amount="2",
        ),
        _row(
            line=4,
            local_ts=3000,
            exch_ts=902,
            is_snapshot=True,
            side=Side.BID,
            price="10",
            amount="3",
        ),
    ]
    batches = list(
        iter_l2_batches(rows, q, failure_policy=FailurePolicy.HARD_FAIL)
    )
    assert [b.resets_book for b in batches] == [True, False, True]


def test_monotone_local_timestamp_enforced() -> None:
    q = Quantizer.from_strings("1", "1")
    rows = [
        _row(
            line=2,
            local_ts=2000,
            exch_ts=900,
            is_snapshot=True,
            side=Side.BID,
            price="10",
            amount="1",
        ),
        _row(
            line=3,
            local_ts=1000,
            exch_ts=901,
            is_snapshot=True,
            side=Side.ASK,
            price="11",
            amount="2",
        ),
    ]
    with pytest.raises(OrderingError):
        list(iter_l2_batches(rows, q, failure_policy=FailurePolicy.HARD_FAIL))


def test_mixed_snapshot_within_batch_rejected() -> None:
    q = Quantizer.from_strings("1", "1")
    rows = [
        _row(
            line=2,
            local_ts=1000,
            exch_ts=900,
            is_snapshot=True,
            side=Side.BID,
            price="10",
            amount="1",
        ),
        _row(
            line=3,
            local_ts=1000,
            exch_ts=901,
            is_snapshot=False,
            side=Side.ASK,
            price="11",
            amount="2",
        ),
    ]
    with pytest.raises(SchemaError):
        list(iter_l2_batches(rows, q, failure_policy=FailurePolicy.HARD_FAIL))


def test_duplicate_updates_preserve_order() -> None:
    q = Quantizer.from_strings("1", "1")
    rows = [
        _row(
            line=2,
            local_ts=1000,
            exch_ts=900,
            is_snapshot=True,
            side=Side.BID,
            price="10",
            amount="1",
        ),
        _row(
            line=3,
            local_ts=1000,
            exch_ts=901,
            is_snapshot=True,
            side=Side.BID,
            price="10",
            amount="2",
        ),
    ]
    batch = list(
        iter_l2_batches(rows, q, failure_policy=FailurePolicy.HARD_FAIL)
    )[0]
    assert len(batch.updates) == 2
    assert int(batch.updates[-1].amount_lots) == 2


def test_exchange_symbol_mismatch_rejected() -> None:
    q = Quantizer.from_strings("1", "1")
    rows = [
        _row(
            line=2,
            local_ts=1000,
            exch_ts=900,
            is_snapshot=True,
            side=Side.BID,
            price="10",
            amount="1",
        ),
        _row(
            line=3,
            local_ts=1000,
            exch_ts=901,
            is_snapshot=True,
            side=Side.ASK,
            price="11",
            amount="2",
        ),
    ]
    rows[1] = L2Row(
        exchange="bybit",
        symbol="ETHUSDT",
        timestamp_us=901,
        local_timestamp_us=1000,
        is_snapshot=True,
        side=Side.ASK,
        price="11",
        amount="2",
        line_number=3,
        source="test.csv",
    )
    with pytest.raises(SchemaError):
        list(iter_l2_batches(rows, q, failure_policy=FailurePolicy.HARD_FAIL))


def test_quarantine_records_payload() -> None:
    q = Quantizer.from_strings("1", "1")
    rows = [
        _row(
            line=2,
            local_ts=1000,
            exch_ts=900,
            is_snapshot=True,
            side=Side.BID,
            price="10",
            amount="-1",
        ),
    ]
    sink = ListQuarantineSink()
    with pytest.raises(QuantizationError):
        list(
            iter_l2_batches(
                rows,
                q,
                failure_policy=FailurePolicy.QUARANTINE,
                quarantine_sink=sink,
                source="test.csv",
            )
        )
    assert len(sink.records) == 1


def test_quarantine_skip_row() -> None:
    q = Quantizer.from_strings("1", "1")
    rows = [
        _row(
            line=2,
            local_ts=1000,
            exch_ts=900,
            is_snapshot=True,
            side=Side.BID,
            price="10",
            amount="-1",
        ),
        _row(
            line=3,
            local_ts=1000,
            exch_ts=901,
            is_snapshot=True,
            side=Side.ASK,
            price="11",
            amount="2",
        ),
    ]
    sink = ListQuarantineSink()
    batches = list(
        iter_l2_batches(
            rows,
            q,
            failure_policy=FailurePolicy.QUARANTINE,
            quarantine_action=QuarantineAction.SKIP_ROW,
            quarantine_sink=sink,
            source="test.csv",
        )
    )
    assert len(sink.records) == 1
    assert len(batches) == 1
    assert [u.side for u in batches[0].updates] == [Side.ASK]


def test_quarantine_skip_batch() -> None:
    q = Quantizer.from_strings("1", "1")
    rows = [
        _row(
            line=2,
            local_ts=1000,
            exch_ts=900,
            is_snapshot=True,
            side=Side.BID,
            price="10",
            amount="-1",
        ),
        _row(
            line=3,
            local_ts=1000,
            exch_ts=901,
            is_snapshot=True,
            side=Side.ASK,
            price="11",
            amount="2",
        ),
        _row(
            line=4,
            local_ts=2000,
            exch_ts=902,
            is_snapshot=False,
            side=Side.BID,
            price="10",
            amount="1",
        ),
    ]
    sink = ListQuarantineSink()
    batches = list(
        iter_l2_batches(
            rows,
            q,
            failure_policy=FailurePolicy.QUARANTINE,
            quarantine_action=QuarantineAction.SKIP_BATCH,
            quarantine_sink=sink,
            source="test.csv",
        )
    )
    assert len(sink.records) == 1
    assert len(batches) == 1
    assert int(batches[0].ts_recv_ns) == 2000 * 1_000
