import pytest

from mm_bt.core import SchemaError
from mm_bt.io.infer_increments import infer_l2_increments


def _write_l2(tmp_path, rows, name: str = "l2.csv") -> str:
    path = tmp_path / name
    with path.open("w", encoding="utf-8", newline="") as f:
        f.write(
            "exchange,symbol,timestamp,local_timestamp,is_snapshot,side,price,amount\n"
        )
        for row in rows:
            f.write(",".join(row) + "\n")
    return str(path)


def test_infer_increments_smoke(tmp_path) -> None:
    path = _write_l2(
        tmp_path,
        [
            ["binance", "BTCUSDT", "1", "1", "true", "bid", "10.0", "1.0"],
            ["binance", "BTCUSDT", "2", "1", "true", "ask", "10.5", "1.1"],
            ["binance", "BTCUSDT", "3", "2", "false", "bid", "11.0", "0.0"],
        ],
    )
    price_inc, amount_inc = infer_l2_increments([path])
    assert price_inc == "0.5"
    assert amount_inc == "0.1"


def test_infer_increments_requires_distinct_values(tmp_path) -> None:
    path = _write_l2(
        tmp_path,
        [
            ["binance", "BTCUSDT", "1", "1", "true", "bid", "10.0", "1.0"],
            ["binance", "BTCUSDT", "2", "2", "false", "ask", "10.0", "1.0"],
        ],
    )
    with pytest.raises(SchemaError):
        infer_l2_increments([path])


def test_infer_increments_rejects_non_positive(tmp_path) -> None:
    path = _write_l2(
        tmp_path,
        [
            ["binance", "BTCUSDT", "1", "1", "true", "bid", "0", "1.0"],
            ["binance", "BTCUSDT", "2", "2", "false", "ask", "0", "1.0"],
        ],
    )
    with pytest.raises(SchemaError):
        infer_l2_increments([path])


def test_infer_increments_uses_first_1000_rows(tmp_path) -> None:
    rows = []
    for i in range(1000):
        price = "10.0" if i % 2 == 0 else "10.5"
        amount = "1.0" if i % 2 == 0 else "1.1"
        rows.append(
            [
                "binance",
                "BTCUSDT",
                str(i + 1),
                str(i + 1),
                "true",
                "bid",
                price,
                amount,
            ]
        )
    rows.append(
        ["binance", "BTCUSDT", "1001", "1001", "true", "bid", "10.2", "1.02"]
    )
    path = _write_l2(tmp_path, rows)
    price_inc, amount_inc = infer_l2_increments([path])
    assert price_inc == "0.5"
    assert amount_inc == "0.1"
