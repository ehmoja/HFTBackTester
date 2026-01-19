from pathlib import Path

import pytest

from mm_bt.core import SchemaError
from mm_bt.io import locate_tardis_files


def _touch(path: Path) -> None:
    path.write_text("", encoding="utf-8")


def test_locator_date_dir_layout(tmp_path) -> None:
    root = tmp_path / "data"
    dir_path = root / "binance" / "incremental_book_L2" / "2020-01-01"
    dir_path.mkdir(parents=True)
    path = dir_path / "BTCUSDT.csv.gz"
    _touch(path)

    out = locate_tardis_files(
        root=root,
        exchange="binance",
        data_type="incremental_book_L2",
        date="2020-01-01",
        symbol_or_group="BTCUSDT",
    )
    assert out == (path,)


def test_locator_symbol_dir_layout(tmp_path) -> None:
    root = tmp_path / "data"
    dir_path = root / "binance" / "incremental_book_L2" / "BTCUSDT"
    dir_path.mkdir(parents=True)
    path = dir_path / "2020-01-01.csv"
    _touch(path)

    out = locate_tardis_files(
        root=root,
        exchange="binance",
        data_type="incremental_book_L2",
        date="2020-01-01",
        symbol_or_group="BTCUSDT",
    )
    assert out == (path,)


def test_locator_shards_sorted(tmp_path) -> None:
    root = tmp_path / "data"
    dir_path = root / "binance" / "incremental_book_L2" / "2020-01-01"
    dir_path.mkdir(parents=True)
    p1 = dir_path / "BTCUSDT-2.csv.gz"
    p2 = dir_path / "BTCUSDT-1.csv.gz"
    _touch(p1)
    _touch(p2)

    out = locate_tardis_files(
        root=root,
        exchange="binance",
        data_type="incremental_book_L2",
        date="2020-01-01",
        symbol_or_group="BTCUSDT",
    )
    assert out == (p2, p1)


def test_locator_ambiguous_layout(tmp_path) -> None:
    root = tmp_path / "data"
    dir_date = root / "binance" / "incremental_book_L2" / "2020-01-01"
    dir_date.mkdir(parents=True)
    _touch(dir_date / "BTCUSDT.csv.gz")

    dir_symbol = root / "binance" / "incremental_book_L2" / "BTCUSDT"
    dir_symbol.mkdir(parents=True)
    _touch(dir_symbol / "2020-01-01.csv")

    with pytest.raises(SchemaError):
        locate_tardis_files(
            root=root,
            exchange="binance",
            data_type="incremental_book_L2",
            date="2020-01-01",
            symbol_or_group="BTCUSDT",
        )


def test_locator_missing_files(tmp_path) -> None:
    root = tmp_path / "data"
    root.mkdir()
    with pytest.raises(SchemaError):
        locate_tardis_files(
            root=root,
            exchange="binance",
            data_type="incremental_book_L2",
            date="2020-01-01",
            symbol_or_group="BTCUSDT",
        )


def test_locator_missing_root(tmp_path) -> None:
    root = tmp_path / "missing"
    with pytest.raises(SchemaError):
        locate_tardis_files(
            root=root,
            exchange="binance",
            data_type="incremental_book_L2",
            date="2020-01-01",
            symbol_or_group="BTCUSDT",
        )


def test_locator_rejects_bad_date(tmp_path) -> None:
    with pytest.raises(SchemaError):
        locate_tardis_files(
            root=tmp_path,
            exchange="binance",
            data_type="incremental_book_L2",
            date="20200101",
            symbol_or_group="BTCUSDT",
        )
