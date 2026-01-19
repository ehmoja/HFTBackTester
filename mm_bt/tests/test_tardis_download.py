import gzip

import pytest

from mm_bt.core import SchemaError
from mm_bt.io import tardis_download
from mm_bt.io.tardis_download import build_download_plan
from mm_bt.io.tardis_download import canonical_tardis_path, download_tardis_csv_gz
from mm_bt.io.tardis_download import _validate_l2_gz_header as validate_l2_gz_header


def test_canonical_tardis_path_date_dir_layout(tmp_path) -> None:
    out = canonical_tardis_path(
        root=tmp_path,
        exchange="binance",
        data_type="incremental_book_L2",
        date="2020-01-01",
        symbol="BTCUSDT",
    )
    assert out == (
        tmp_path
        / "binance"
        / "incremental_book_L2"
        / "2020-01-01"
        / "BTCUSDT.csv.gz"
    )


def test_validate_l2_gz_header_ok(tmp_path) -> None:
    path = tmp_path / "ok.csv.gz"
    with gzip.open(path, "wt", encoding="utf-8", newline="") as f:
        f.write(
            "exchange,symbol,timestamp,local_timestamp,is_snapshot,side,price,amount\n"
        )
        f.write("binance,BTCUSDT,1,1,true,bid,1,1\n")
    validate_l2_gz_header(path)


def test_validate_l2_gz_header_rejects_drift(tmp_path) -> None:
    path = tmp_path / "bad.csv.gz"
    with gzip.open(path, "wt", encoding="utf-8", newline="") as f:
        f.write("exchange,symbol,timestamp\n")
        f.write("binance,BTCUSDT,1\n")
    with pytest.raises(SchemaError):
        validate_l2_gz_header(path)


def test_rejects_path_separators() -> None:
    with pytest.raises(SchemaError):
        canonical_tardis_path(
            root=".",
            exchange="binance",
            data_type="incremental_book_L2",
            date="2020-01-01",
            symbol="BTC/USDT",
        )


def test_build_download_plan(tmp_path) -> None:
    plan = build_download_plan(
        root=tmp_path,
        exchange="binance",
        data_type="incremental_book_L2",
        date="2020-01-01",
        symbol="BTCUSDT",
    )
    assert plan.exchange == "binance"
    assert plan.data_type == "incremental_book_L2"
    assert plan.date == "2020-01-01"
    assert plan.symbol == "BTCUSDT"
    assert plan.target_path == (
        tmp_path
        / "binance"
        / "incremental_book_L2"
        / "2020-01-01"
        / "BTCUSDT.csv.gz"
    )


def test_is_not_found_error() -> None:
    class _NotFound(Exception):
        code = 404

    assert tardis_download._is_not_found_error(_NotFound())
    assert tardis_download._is_not_found_error(Exception("Not Found"))
    assert not tardis_download._is_not_found_error(Exception("boom"))


def test_skip_existing_skips_sdk(tmp_path, monkeypatch) -> None:
    path = canonical_tardis_path(
        root=tmp_path,
        exchange="binance",
        data_type="incremental_book_L2",
        date="2020-01-01",
        symbol="BTCUSDT",
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"")

    def _require():
        raise AssertionError("sdk called")

    monkeypatch.setattr(tardis_download, "_require_tardis_dev", _require)
    out = download_tardis_csv_gz(
        root=tmp_path,
        exchange="binance",
        data_type="incremental_book_L2",
        date="2020-01-01",
        symbol="BTCUSDT",
        if_exists="skip",
        validate_header=False,
    )
    assert out == path


def test_relocates_flat_download(tmp_path, monkeypatch) -> None:
    flat = tmp_path / "binance_incremental_book_L2_2020-01-01_BTCUSDT.csv.gz"
    flat.write_bytes(b"")

    class _Datasets:
        def download(self, **_kwargs) -> None:
            return None

    monkeypatch.setattr(tardis_download, "_require_tardis_dev", lambda: _Datasets())
    out = download_tardis_csv_gz(
        root=tmp_path,
        exchange="binance",
        data_type="incremental_book_L2",
        date="2020-01-01",
        symbol="BTCUSDT",
        if_exists="error",
        validate_header=False,
    )
    expected = canonical_tardis_path(
        root=tmp_path,
        exchange="binance",
        data_type="incremental_book_L2",
        date="2020-01-01",
        symbol="BTCUSDT",
    )
    assert out == expected
    assert expected.exists()
    assert not flat.exists()
