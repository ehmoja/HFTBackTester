"""PnL and return series helpers."""

from __future__ import annotations

from typing import Sequence

from mm_bt.core.errors import SchemaError
from mm_bt.core.types import Bps

_BPS_SCALE = 10_000


def _round_half_even(numer: int, denom: int) -> int:
    if denom <= 0:
        raise SchemaError("denom must be positive")
    sign = 1
    if numer < 0:
        sign = -1
        numer = -numer
    q, r = divmod(numer, denom)
    twice_r = r * 2
    if twice_r > denom:
        q += 1
    elif twice_r == denom and (q % 2 == 1):
        q += 1
    return sign * q


def returns_from_equity(
    equity: Sequence[int], *, initial_cash: int
) -> tuple[Bps, ...]:
    """Compute per-step returns in basis points of initial_cash."""
    if initial_cash <= 0:
        raise SchemaError("initial_cash must be positive")
    if len(equity) < 2:
        raise SchemaError("insufficient equity points for returns")
    for value in equity:
        if not isinstance(value, int):
            raise SchemaError("equity values must be int")
    returns: list[Bps] = []
    prev = equity[0]
    for current in equity[1:]:
        delta = current - prev
        bps = _round_half_even(delta * _BPS_SCALE, initial_cash)
        returns.append(Bps(bps))
        prev = current
    return tuple(returns)

