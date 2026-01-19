"""Central Decimal context and parsing."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation, Context, ROUND_HALF_EVEN, localcontext

DECIMAL_CTX = Context(prec=50, rounding=ROUND_HALF_EVEN)


def parse_decimal(value: str) -> Decimal:
    value = value.strip()
    if value == "":
        raise ValueError("empty decimal")
    with localcontext(DECIMAL_CTX):
        try:
            d = Decimal(value)
        except (InvalidOperation, ValueError) as exc:
            raise ValueError(f"invalid decimal: {value!r}") from exc
    if not d.is_finite():
        raise ValueError(f"non-finite decimal: {value!r}")
    return d

