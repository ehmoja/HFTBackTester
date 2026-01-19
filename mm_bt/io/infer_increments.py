"""Infer price/amount increments from Tardis L2 CSV data."""

from __future__ import annotations

import math
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Iterable, Sequence

from mm_bt.core.decimal_ctx import parse_decimal
from mm_bt.core.errors import SchemaError
from mm_bt.io.tardis_csv import L2Row, iter_l2_rows

_INFER_MAX_ROWS = 1000


@dataclass
class _IncrementStats:
    scale: int = 0
    gcd_value: int | None = None
    first_value: int | None = None
    has_distinct: bool = False

    def add(self, value: Decimal, *, field: str, allow_zero: bool) -> None:
        if value.is_nan() or value.is_infinite():
            raise SchemaError(f"{field} non-finite")
        if value == 0:
            if allow_zero:
                return
            raise SchemaError(f"{field} must be positive")
        if value < 0:
            raise SchemaError(f"{field} negative")
        exp = -value.as_tuple().exponent
        if exp < 0:
            exp = 0
        if exp > self.scale:
            factor = 10 ** (exp - self.scale)
            if self.gcd_value is not None:
                self.gcd_value *= factor
            if self.first_value is not None:
                self.first_value *= factor
            self.scale = exp
        scaled = value.scaleb(self.scale).to_integral_exact()
        val = int(scaled)
        if self.gcd_value is None:
            self.gcd_value = val
            self.first_value = val
            return
        if val != self.first_value:
            self.has_distinct = True
        self.gcd_value = math.gcd(self.gcd_value, val)

    def finish(self, *, field: str) -> Decimal:
        if self.gcd_value is None:
            raise SchemaError(f"{field} has no positive values")
        if not self.has_distinct:
            raise SchemaError(f"{field} has no distinct values to infer increment")
        return Decimal(self.gcd_value).scaleb(-self.scale)


def _parse_decimal_field(row: L2Row, value: str, field: str) -> Decimal:
    try:
        return parse_decimal(value)
    except ValueError as exc:
        raise SchemaError(
            f"{field} invalid at line {row.line_number} in {row.source}: {exc}"
        ) from exc


def _iter_rows(paths: Sequence[str | Path]) -> Iterable[L2Row]:
    for path in paths:
        yield from iter_l2_rows(path)


def infer_l2_increments(
    l2_paths: Sequence[str | Path],
) -> tuple[str, str]:
    """Infer increments using the first _INFER_MAX_ROWS rows only."""
    if not l2_paths:
        raise SchemaError("l2_paths must be non-empty")
    price_stats = _IncrementStats()
    amount_stats = _IncrementStats()
    seen = 0
    for row in _iter_rows(l2_paths):
        price = _parse_decimal_field(row, row.price, "price")
        amount = _parse_decimal_field(row, row.amount, "amount")
        price_stats.add(price, field="price", allow_zero=False)
        amount_stats.add(amount, field="amount", allow_zero=True)
        seen += 1
        if seen >= _INFER_MAX_ROWS:
            break
    price_inc = price_stats.finish(field="price")
    amount_inc = amount_stats.finish(field="amount")
    return (str(price_inc), str(amount_inc))
