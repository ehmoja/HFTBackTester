import json

import pytest

from mm_bt.book import BookPy
from mm_bt.core import Quantizer
from mm_bt.core import SchemaError
from mm_bt.evlog import EvlogReader
from mm_bt.evlog import EVLOG_VERSION
from mm_bt.ingest import compile_l2_csv
from mm_bt.sim import iter_best_bid_ask


def _write_l2(tmp_path, rows, name: str = "l2.csv") -> str:
    path = tmp_path / name
    with path.open("w", encoding="utf-8", newline="") as f:
        f.write(
            "exchange,symbol,timestamp,local_timestamp,is_snapshot,side,price,amount\n"
        )
        for row in rows:
            f.write(",".join(row) + "\n")
    return str(path)


def test_compile_l2_smoke(tmp_path) -> None:
    path = _write_l2(
        tmp_path,
        [
            ["binance", "BTCUSDT", "900", "1000", "true", "bid", "10", "1"],
            ["binance", "BTCUSDT", "905", "1000", "true", "ask", "11", "2"],
            ["binance", "BTCUSDT", "910", "2000", "false", "bid", "10", "0"],
            ["binance", "BTCUSDT", "915", "2000", "false", "ask", "12", "1"],
        ],
    )
    out_dir = tmp_path / "out"
    q = Quantizer.from_strings("1", "1")
    result = compile_l2_csv(
        l2_path=path,
        output_dir=out_dir,
        quantizer=q,
    )

    assert result.evlog_path.exists()
    assert result.index_path.exists()
    assert result.manifest_path.exists()
    assert result.record_count == 2

    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["record_count"] == 2
    assert manifest["manifest_version"] == 1
    assert manifest["compiler_version"] == 1
    assert "inputs_sha256" in manifest
    assert manifest["inputs"][0]["path"] == path
    assert manifest["quantizer"]["price_increment"] == "1"
    assert manifest["exchange"] == "binance"
    assert manifest["symbol"] == "BTCUSDT"
    assert manifest["format_version"] == EVLOG_VERSION
    assert "sha256" in manifest["quantizer"]
    assert "compiler_sha256" in manifest

    book = BookPy()
    with EvlogReader(result.evlog_path, index_path=result.index_path) as reader:
        batches = list(reader.iter_l2_batches())
    assert len(batches) == 2
    book.apply_l2_batch(batches[0])
    bid_px, _, ask_px, _ = book.best_bid_ask()
    assert int(bid_px) == 10
    assert int(ask_px) == 11
    book.apply_l2_batch(batches[1])
    bid_px, bid_qty, ask_px, ask_qty = book.best_bid_ask()
    assert bid_px is None
    assert bid_qty is None
    assert int(ask_px) == 11
    assert int(ask_qty) == 2


def test_replay_helper(tmp_path) -> None:
    path = _write_l2(
        tmp_path,
        [
            ["binance", "BTCUSDT", "900", "1000", "true", "bid", "10", "1"],
            ["binance", "BTCUSDT", "905", "1000", "true", "ask", "11", "2"],
            ["binance", "BTCUSDT", "910", "2000", "false", "ask", "12", "1"],
        ],
    )
    out_dir = tmp_path / "out"
    q = Quantizer.from_strings("1", "1")
    result = compile_l2_csv(
        l2_path=path,
        output_dir=out_dir,
        quantizer=q,
    )

    snapshots = list(iter_best_bid_ask(result.evlog_path))
    assert len(snapshots) == 2
    _, first = snapshots[0]
    _, second = snapshots[1]
    assert int(first[0]) == 10
    assert int(first[2]) == 11
    assert int(second[2]) == 11


def test_compile_multiple_inputs_requires_prefix(tmp_path) -> None:
    path1 = _write_l2(
        tmp_path,
        [
            ["binance", "BTCUSDT", "900", "1000", "true", "bid", "10", "1"],
            ["binance", "BTCUSDT", "905", "1000", "true", "ask", "11", "2"],
        ],
        name="l2_1.csv",
    )
    path2 = _write_l2(
        tmp_path,
        [
            ["binance", "BTCUSDT", "910", "2000", "false", "bid", "10", "0"],
            ["binance", "BTCUSDT", "915", "2000", "false", "ask", "12", "1"],
        ],
        name="l2_2.csv",
    )
    out_dir = tmp_path / "out"
    q = Quantizer.from_strings("1", "1")
    with pytest.raises(SchemaError):
        compile_l2_csv(
            l2_paths=[path1, path2],
            output_dir=out_dir,
            quantizer=q,
        )


def test_compile_multiple_inputs_with_prefix(tmp_path) -> None:
    path1 = _write_l2(
        tmp_path,
        [
            ["binance", "BTCUSDT", "900", "1000", "true", "bid", "10", "1"],
            ["binance", "BTCUSDT", "905", "1000", "true", "ask", "11", "2"],
        ],
        name="l2_1.csv",
    )
    path2 = _write_l2(
        tmp_path,
        [
            ["binance", "BTCUSDT", "910", "2000", "false", "bid", "10", "0"],
            ["binance", "BTCUSDT", "915", "2000", "false", "ask", "12", "1"],
        ],
        name="l2_2.csv",
    )
    out_dir = tmp_path / "out"
    q = Quantizer.from_strings("1", "1")
    result = compile_l2_csv(
        l2_paths=[path1, path2],
        output_dir=out_dir,
        quantizer=q,
        output_prefix="binance-BTCUSDT-2020-01-01-incremental_book_L2",
    )
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert len(manifest["inputs"]) == 2
    assert manifest["record_count"] == 2
