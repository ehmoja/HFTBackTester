import pytest

from mm_bt.core import SchemaError
from mm_bt.io import StaticJsonProvider, TardisInstrumentMetaApiProvider


def _write_meta(tmp_path, payload) -> str:
    path = tmp_path / "meta.json"
    path.write_text(payload, encoding="utf-8")
    return str(path)


def test_static_meta_load_and_get(tmp_path) -> None:
    payload = """
    {"version":0,"instruments":[
        {"exchange":"binance","symbol":"BTCUSDT","date":"2020-01-01",
         "price_increment":"0.01","amount_increment":"0.001"}
    ]}
    """
    path = _write_meta(tmp_path, payload)
    provider = StaticJsonProvider(path)
    meta = provider.get("binance", "BTCUSDT", "2020-01-01")
    assert meta.price_increment == "0.01"
    assert meta.amount_increment == "0.001"


def test_static_meta_rejects_missing_fields(tmp_path) -> None:
    payload = """
    {"version":0,"instruments":[
        {"exchange":"binance","symbol":"BTCUSDT","date":"2020-01-01",
         "price_increment":"0.01"}
    ]}
    """
    path = _write_meta(tmp_path, payload)
    with pytest.raises(SchemaError):
        StaticJsonProvider(path)


def test_static_meta_rejects_duplicate(tmp_path) -> None:
    payload = """
    {"version":0,"instruments":[
        {"exchange":"binance","symbol":"BTCUSDT","date":"2020-01-01",
         "price_increment":"0.01","amount_increment":"0.001"},
        {"exchange":"binance","symbol":"BTCUSDT","date":"2020-01-01",
         "price_increment":"0.01","amount_increment":"0.001"}
    ]}
    """
    path = _write_meta(tmp_path, payload)
    with pytest.raises(SchemaError):
        StaticJsonProvider(path)


def test_static_meta_rejects_bad_date(tmp_path) -> None:
    payload = """
    {"version":0,"instruments":[
        {"exchange":"binance","symbol":"BTCUSDT","date":"20200101",
         "price_increment":"0.01","amount_increment":"0.001"}
    ]}
    """
    path = _write_meta(tmp_path, payload)
    with pytest.raises(SchemaError):
        StaticJsonProvider(path)


def test_static_meta_rejects_bad_decimal(tmp_path) -> None:
    payload = """
    {"version":0,"instruments":[
        {"exchange":"binance","symbol":"BTCUSDT","date":"2020-01-01",
         "price_increment":"0.01","amount_increment":"bad"}
    ]}
    """
    path = _write_meta(tmp_path, payload)
    with pytest.raises(SchemaError):
        StaticJsonProvider(path)


def test_tardis_meta_provider_fetch_and_cache(tmp_path) -> None:
    records = [
        {
            "symbol": "BTCUSDT",
            "priceIncrement": "0.01",
            "amountIncrement": "0.001",
        }
    ]
    calls = {"count": 0}

    def _fetcher(**_kwargs):
        calls["count"] += 1
        return records

    cache = tmp_path / "cache.json"
    provider = TardisInstrumentMetaApiProvider(
        cache_path=cache, fetcher=_fetcher, api_key="x"
    )
    meta = provider.get("binance", "BTCUSDT", "2020-01-01")
    assert meta.price_increment == "0.01"
    assert meta.amount_increment == "0.001"
    assert calls["count"] == 1

    provider2 = TardisInstrumentMetaApiProvider(
        cache_path=cache, fetcher=lambda **_kwargs: []
    )
    meta2 = provider2.get("binance", "BTCUSDT", "2020-01-01")
    assert meta2.price_increment == "0.01"
    assert meta2.amount_increment == "0.001"


def test_tardis_meta_provider_rejects_missing_symbol() -> None:
    def _fetcher(**_kwargs):
        return [{"symbol": "ETHUSDT", "priceIncrement": "0.1", "amountIncrement": "1"}]

    provider = TardisInstrumentMetaApiProvider(fetcher=_fetcher)
    with pytest.raises(SchemaError):
        provider.get("binance", "BTCUSDT", "2020-01-01")
