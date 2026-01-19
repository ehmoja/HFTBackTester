"""Instrument metadata providers."""

from __future__ import annotations

from dataclasses import dataclass
import importlib
import pkgutil
import json
import os
from pathlib import Path
from typing import Callable, Protocol

from mm_bt.core.decimal_ctx import parse_decimal
from mm_bt.core.errors import SchemaError
from mm_bt.core.fixedpoint import Quantizer


@dataclass(frozen=True, slots=True)
class InstrumentMeta:
    exchange: str
    symbol: str
    date: str
    price_increment: str
    amount_increment: str
    min_trade_amount: str | None

    def quantizer(self) -> Quantizer:
        return Quantizer.from_strings(
            self.price_increment, self.amount_increment
        )


class InstrumentMetaProvider(Protocol):
    def get(self, exchange: str, symbol: str, date: str) -> InstrumentMeta:
        """Return instrument metadata for exchange/symbol/date."""


def _validate_date(value: str) -> None:
    if len(value) != 10:
        raise SchemaError(f"invalid date: {value!r}")
    if value[4] != "-" or value[7] != "-":
        raise SchemaError(f"invalid date: {value!r}")
    y, m, d = value.split("-")
    if not (y.isdigit() and m.isdigit() and d.isdigit()):
        raise SchemaError(f"invalid date: {value!r}")


def _require_str(value: object, field: str) -> str:
    if not isinstance(value, str) or value == "":
        raise SchemaError(f"{field} must be non-empty string")
    return value


class StaticJsonProvider:
    def __init__(self, path: str | Path) -> None:
        p = Path(path)
        data = json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise SchemaError("instrument meta must be an object")
        allowed_top = {"version", "instruments"}
        if set(data.keys()) != allowed_top:
            raise SchemaError("unexpected top-level keys in instrument meta")
        if data["version"] != 0:
            raise SchemaError("unsupported instrument meta version")
        instruments = data["instruments"]
        if not isinstance(instruments, list):
            raise SchemaError("instruments must be a list")

        self._by_key: dict[tuple[str, str, str], InstrumentMeta] = {}
        for entry in instruments:
            if not isinstance(entry, dict):
                raise SchemaError("instrument entry must be an object")
            allowed = {
                "exchange",
                "symbol",
                "date",
                "price_increment",
                "amount_increment",
                "min_trade_amount",
            }
            if not set(entry.keys()).issubset(allowed):
                raise SchemaError("unexpected keys in instrument entry")
            exchange = _require_str(entry.get("exchange"), "exchange")
            symbol = _require_str(entry.get("symbol"), "symbol")
            date = _require_str(entry.get("date"), "date")
            _validate_date(date)
            price_increment = _require_str(
                entry.get("price_increment"), "price_increment"
            )
            amount_increment = _require_str(
                entry.get("amount_increment"), "amount_increment"
            )
            min_trade_amount = entry.get("min_trade_amount")
            if min_trade_amount is not None:
                min_trade_amount = _require_str(
                    min_trade_amount, "min_trade_amount"
                )

            # Validate decimals early.
            try:
                parse_decimal(price_increment)
                parse_decimal(amount_increment)
                if min_trade_amount is not None:
                    parse_decimal(min_trade_amount)
            except ValueError as exc:
                raise SchemaError(f"invalid decimal in instrument meta: {exc}") from exc

            meta = InstrumentMeta(
                exchange=exchange,
                symbol=symbol,
                date=date,
                price_increment=price_increment,
                amount_increment=amount_increment,
                min_trade_amount=min_trade_amount,
            )
            key = (exchange, symbol, date)
            if key in self._by_key:
                raise SchemaError(
                    f"duplicate instrument entry: {exchange}/{symbol}/{date}"
                )
            self._by_key[key] = meta

    def get(self, exchange: str, symbol: str, date: str) -> InstrumentMeta:
        key = (exchange, symbol, date)
        meta = self._by_key.get(key)
        if meta is None:
            raise SchemaError(
                f"instrument meta not found: {exchange}/{symbol}/{date}"
            )
        return meta


def _require_tardis_instruments_module():
    try:
        tardis_dev = importlib.import_module("tardis_dev")
    except ImportError as exc:
        raise SchemaError(
            "tardis-dev not installed; pip install tardis-dev"
        ) from exc

    candidates = [
        "tardis_dev.instruments",
        "tardis_dev.instruments_metadata",
        "tardis_dev.instruments_api",
    ]
    discovered = [m.name for m in pkgutil.iter_modules(tardis_dev.__path__)]
    for name in discovered:
        if "instrument" in name:
            candidates.append(f"tardis_dev.{name}")
    seen: set[str] = set()
    for name in candidates:
        if name in seen:
            continue
        seen.add(name)
        try:
            module = importlib.import_module(name)
        except ImportError:
            continue
        try:
            _resolve_instruments_fn(module)
        except SchemaError:
            continue
        return module
    raise SchemaError(
        "tardis-dev instruments module not found; "
        f"submodules={sorted(set(discovered))}"
    )


def _resolve_instruments_fn(module) -> Callable[..., object]:
    for name in ("get_instruments", "list_instruments", "get_exchange_instruments"):
        fn = getattr(module, name, None)
        if callable(fn):
            return fn
    for name in dir(module):
        if "instrument" not in name:
            continue
        if "get" not in name and "list" not in name:
            continue
        fn = getattr(module, name, None)
        if callable(fn):
            return fn
    raise SchemaError("tardis-dev instruments API not found")


def _call_instruments_fn(
    fn: Callable[..., object],
    *,
    exchange: str,
    date: str,
    symbol: str,
    api_key: str | None,
) -> object:
    base_kwargs = {"exchange": exchange}
    if api_key:
        base_kwargs["api_key"] = api_key

    attempts = [
        {"date": date, "symbols": [symbol]},
        {"date": date, "symbol": symbol},
        {"date": date},
        {"from_date": date, "to_date": date, "symbols": [symbol]},
        {"from_date": date, "to_date": date},
    ]
    last_exc: Exception | None = None
    for extra in attempts:
        kwargs = {**base_kwargs, **extra}
        try:
            return fn(**kwargs)
        except TypeError as exc:
            msg = str(exc)
            if "unexpected keyword" in msg or "got an unexpected keyword" in msg:
                last_exc = exc
                continue
            raise
    if last_exc is not None:
        raise SchemaError(
            "tardis-dev instruments API signature not supported"
        ) from last_exc
    raise SchemaError("tardis-dev instruments API call failed")


def _normalize_instrument_records(result: object) -> list[object]:
    if isinstance(result, list):
        return result
    if isinstance(result, dict):
        for key in ("data", "instruments", "items"):
            payload = result.get(key)
            if isinstance(payload, list):
                return payload
    for key in ("data", "instruments", "items"):
        payload = getattr(result, key, None)
        if isinstance(payload, list):
            return payload
    raise SchemaError("unexpected instruments API response")


def _record_get(record: object, key: str) -> object | None:
    if isinstance(record, dict):
        return record.get(key)
    return getattr(record, key, None)


def _read_field(
    record: object, *, names: tuple[str, ...], required: bool
) -> str | None:
    for name in names:
        value = _record_get(record, name)
        if value is not None:
            return str(value)
    if required:
        raise SchemaError(f"missing instrument field: {names[0]}")
    return None


def _select_instrument_record(
    records: list[object], *, symbol: str
) -> object:
    matches = []
    for record in records:
        rec_symbol = _read_field(
            record,
            names=("symbol", "id", "instrumentId", "instrument_id"),
            required=False,
        )
        if rec_symbol == symbol:
            matches.append(record)
    if len(matches) == 1:
        return matches[0]
    if len(matches) == 0 and len(records) == 1:
        return records[0]
    raise SchemaError(f"instrument meta not found for symbol: {symbol}")


def _fetch_tardis_instruments(
    *, exchange: str, date: str, symbol: str, api_key: str | None
) -> list[object]:
    module = _require_tardis_instruments_module()
    fn = _resolve_instruments_fn(module)
    result = _call_instruments_fn(
        fn,
        exchange=exchange,
        date=date,
        symbol=symbol,
        api_key=api_key,
    )
    return _normalize_instrument_records(result)


def _load_cache(path: Path) -> dict[tuple[str, str, str], InstrumentMeta]:
    if not path.exists():
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise SchemaError("instrument meta cache must be an object")
    allowed_top = {"version", "instruments"}
    if set(data.keys()) != allowed_top:
        raise SchemaError("unexpected top-level keys in instrument meta cache")
    if data["version"] != 0:
        raise SchemaError("unsupported instrument meta cache version")
    instruments = data["instruments"]
    if not isinstance(instruments, list):
        raise SchemaError("instruments must be a list")

    by_key: dict[tuple[str, str, str], InstrumentMeta] = {}
    for entry in instruments:
        if not isinstance(entry, dict):
            raise SchemaError("instrument entry must be an object")
        exchange = _require_str(entry.get("exchange"), "exchange")
        symbol = _require_str(entry.get("symbol"), "symbol")
        date = _require_str(entry.get("date"), "date")
        _validate_date(date)
        price_increment = _require_str(
            entry.get("price_increment"), "price_increment"
        )
        amount_increment = _require_str(
            entry.get("amount_increment"), "amount_increment"
        )
        min_trade_amount = entry.get("min_trade_amount")
        if min_trade_amount is not None:
            min_trade_amount = _require_str(
                min_trade_amount, "min_trade_amount"
            )
        try:
            parse_decimal(price_increment)
            parse_decimal(amount_increment)
            if min_trade_amount is not None:
                parse_decimal(min_trade_amount)
        except ValueError as exc:
            raise SchemaError(f"invalid decimal in instrument meta: {exc}") from exc
        meta = InstrumentMeta(
            exchange=exchange,
            symbol=symbol,
            date=date,
            price_increment=price_increment,
            amount_increment=amount_increment,
            min_trade_amount=min_trade_amount,
        )
        key = (exchange, symbol, date)
        by_key[key] = meta
    return by_key


def _write_cache(
    path: Path, by_key: dict[tuple[str, str, str], InstrumentMeta]
) -> None:
    instruments = []
    for key in sorted(by_key.keys()):
        meta = by_key[key]
        instruments.append(
            {
                "exchange": meta.exchange,
                "symbol": meta.symbol,
                "date": meta.date,
                "price_increment": meta.price_increment,
                "amount_increment": meta.amount_increment,
                "min_trade_amount": meta.min_trade_amount,
            }
        )
    payload = {"version": 0, "instruments": instruments}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")


class TardisInstrumentMetaApiProvider:
    def __init__(
        self,
        *,
        api_key: str | None = None,
        cache_path: str | Path | None = None,
        fetcher: Callable[..., list[object]] | None = None,
    ) -> None:
        self._api_key = api_key or os.getenv("TARDIS_API_KEY")
        self._fetcher = fetcher or _fetch_tardis_instruments
        self._cache_path = Path(cache_path) if cache_path else None
        self._by_key: dict[tuple[str, str, str], InstrumentMeta] = {}
        if self._cache_path is not None:
            self._by_key.update(_load_cache(self._cache_path))

    def get(self, exchange: str, symbol: str, date: str) -> InstrumentMeta:
        _validate_date(date)
        key = (exchange, symbol, date)
        cached = self._by_key.get(key)
        if cached is not None:
            return cached
        records = self._fetcher(
            exchange=exchange, date=date, symbol=symbol, api_key=self._api_key
        )
        record = _select_instrument_record(records, symbol=symbol)
        price_increment = _read_field(
            record,
            names=("priceIncrement", "price_increment", "tickSize", "tick_size"),
            required=True,
        )
        amount_increment = _read_field(
            record,
            names=(
                "amountIncrement",
                "amount_increment",
                "stepSize",
                "step_size",
                "lotSize",
                "lot_size",
            ),
            required=True,
        )
        min_trade_amount = _read_field(
            record,
            names=("minTradeAmount", "min_trade_amount"),
            required=False,
        )
        try:
            parse_decimal(price_increment)
            parse_decimal(amount_increment)
            if min_trade_amount is not None:
                parse_decimal(min_trade_amount)
        except ValueError as exc:
            raise SchemaError(f"invalid decimal in instrument meta: {exc}") from exc
        meta = InstrumentMeta(
            exchange=exchange,
            symbol=symbol,
            date=date,
            price_increment=price_increment,
            amount_increment=amount_increment,
            min_trade_amount=min_trade_amount,
        )
        self._by_key[key] = meta
        if self._cache_path is not None:
            _write_cache(self._cache_path, self._by_key)
        return meta
