"""Fixed-point quantization helpers."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, localcontext

from mm_bt.core.decimal_ctx import DECIMAL_CTX, parse_decimal
from mm_bt.core.errors import QuantizationError
from mm_bt.core.types import Lots, QuoteAtoms, Ticks


def _normalize_increment(increment: Decimal) -> Decimal:
    with localcontext(DECIMAL_CTX):
        if not increment.is_finite():
            raise QuantizationError("increment must be finite")
        if increment <= 0:
            raise QuantizationError("increment must be positive")
        return increment.normalize()


def _scaled_int(value: Decimal, scale: int) -> int:
    scaled = value.scaleb(scale)
    if scaled != scaled.to_integral_value():
        raise QuantizationError("value has more precision than increment")
    return int(scaled)


def _quantize_decimal(
    value: Decimal,
    increment: Decimal,
    *,
    allow_zero: bool,
    field: str,
) -> int:
    with localcontext(DECIMAL_CTX):
        inc = increment
        if not value.is_finite():
            raise QuantizationError(f"{field} must be finite")
        if value == 0:
            if allow_zero:
                return 0
            raise QuantizationError(f"{field} must be positive")
        if value < 0:
            raise QuantizationError(f"{field} must be non-negative")

        scale = -inc.as_tuple().exponent
        scaled_value = _scaled_int(value.normalize(), scale)
        scaled_inc = _scaled_int(inc, scale)
        if scaled_inc == 0:
            raise QuantizationError("increment underflow")
        if scaled_value % scaled_inc != 0:
            raise QuantizationError(f"{field} not a multiple of increment")
        return scaled_value // scaled_inc


@dataclass(frozen=True, slots=True)
class Quantizer:
    price_increment: Decimal
    amount_increment: Decimal

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "price_increment",
            _normalize_increment(self.price_increment),
        )
        object.__setattr__(
            self,
            "amount_increment",
            _normalize_increment(self.amount_increment),
        )

    @classmethod
    def from_strings(cls, price_increment: str, amount_increment: str) -> "Quantizer":
        return cls(
            price_increment=parse_decimal(price_increment),
            amount_increment=parse_decimal(amount_increment),
        )

    def quantize_price(self, value: str) -> Ticks:
        try:
            dec = parse_decimal(value)
        except ValueError as exc:
            raise QuantizationError(str(exc)) from exc
        ticks = _quantize_decimal(
            dec,
            self.price_increment,
            allow_zero=False,
            field="price",
        )
        return Ticks(ticks)

    def quantize_amount(self, value: str) -> Lots:
        try:
            dec = parse_decimal(value)
        except ValueError as exc:
            raise QuantizationError(str(exc)) from exc
        lots = _quantize_decimal(
            dec,
            self.amount_increment,
            allow_zero=True,
            field="amount",
        )
        return Lots(lots)

    def notional(self, price_ticks: Ticks, amount_lots: Lots) -> QuoteAtoms:
        return QuoteAtoms(int(price_ticks) * int(amount_lots))

