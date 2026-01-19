import pytest

from mm_bt.core import SchemaError
from mm_bt.core import Side
from mm_bt.io import iter_l2_rows


def _write_l2(tmp_path, rows) -> str:
    path = tmp_path / "l2.csv"
    with path.open("w", encoding="utf-8", newline="") as f:
        f.write(
            "exchange,symbol,timestamp,local_timestamp,is_snapshot,side,price,amount\n"
        )
        for row in rows:
            f.write(",".join(row) + "\n")
    return str(path)


def test_iter_l2_rows_basic(tmp_path) -> None:
    path = _write_l2(
        tmp_path,
        [
            ["binance", "BTCUSDT", "100", "200", "true", "bid", "1.0", "2.0"],
        ],
    )
    rows = list(iter_l2_rows(path))
    assert len(rows) == 1
    row = rows[0]
    assert row.exchange == "binance"
    assert row.symbol == "BTCUSDT"
    assert row.timestamp_us == 100
    assert row.local_timestamp_us == 200
    assert row.is_snapshot is True
    assert row.side == Side.BID
    assert row.price == "1.0"
    assert row.amount == "2.0"
    assert row.source == path


def test_iter_l2_rows_rejects_bad_header(tmp_path) -> None:
    path = tmp_path / "l2.csv"
    path.write_text("bad,header\n", encoding="utf-8")
    with pytest.raises(SchemaError):
        list(iter_l2_rows(str(path)))


def test_iter_l2_rows_rejects_non_integer_ts(tmp_path) -> None:
    path = _write_l2(
        tmp_path,
        [
            ["binance", "BTCUSDT", "100.5", "200", "true", "bid", "1.0", "2.0"],
        ],
    )
    with pytest.raises(SchemaError):
        list(iter_l2_rows(path))


def test_iter_l2_rows_rejects_unknown_side(tmp_path) -> None:
    path = _write_l2(
        tmp_path,
        [
            ["binance", "BTCUSDT", "100", "200", "true", "buy", "1.0", "2.0"],
        ],
    )
    with pytest.raises(SchemaError):
        list(iter_l2_rows(path))


def test_iter_l2_rows_rejects_bad_snapshot_flag(tmp_path) -> None:
    path = _write_l2(
        tmp_path,
        [
            ["binance", "BTCUSDT", "100", "200", "yes", "bid", "1.0", "2.0"],
        ],
    )
    with pytest.raises(SchemaError):
        list(iter_l2_rows(path))


def test_iter_l2_rows_rejects_empty_exchange(tmp_path) -> None:
    path = _write_l2(
        tmp_path,
        [
            ["", "BTCUSDT", "100", "200", "true", "bid", "1.0", "2.0"],
        ],
    )
    with pytest.raises(SchemaError):
        list(iter_l2_rows(path))


def test_iter_l2_rows_rejects_empty_symbol(tmp_path) -> None:
    path = _write_l2(
        tmp_path,
        [
            ["binance", "", "100", "200", "true", "bid", "1.0", "2.0"],
        ],
    )
    with pytest.raises(SchemaError):
        list(iter_l2_rows(path))
