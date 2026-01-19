import pytest

from mm_bt.core import SchemaError
from mm_bt.metrics import returns_from_equity


def test_returns_from_equity() -> None:
    equity = [1000, 900, 950]
    returns = returns_from_equity(equity, initial_cash=1000)
    assert returns == (-1000, 500)


def test_returns_rejects_short_series() -> None:
    with pytest.raises(SchemaError):
        returns_from_equity([1000], initial_cash=1000)


def test_returns_rounds_half_even() -> None:
    returns = returns_from_equity([0, 1], initial_cash=20000)
    assert returns == (0,)
    returns = returns_from_equity([0, 3], initial_cash=20000)
    assert returns == (2,)
