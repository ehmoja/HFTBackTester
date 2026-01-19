from decimal import Decimal

import pytest

from mm_bt.core import parse_decimal


def test_parse_decimal_accepts_whitespace() -> None:
    assert parse_decimal("  1.25  ") == Decimal("1.25")


def test_parse_decimal_accepts_scientific() -> None:
    assert parse_decimal("1e-3") == Decimal("0.001")


def test_parse_decimal_rejects_empty_after_strip() -> None:
    with pytest.raises(ValueError):
        parse_decimal("   ")
