from decimal import Decimal

import pytest

from mm_bt.core import QuantizationError
from mm_bt.core import Quantizer


def test_quantize_price_amount() -> None:
    q = Quantizer.from_strings("0.01", "0.001")
    price = q.quantize_price("10.01")
    amount = q.quantize_amount("0.002")
    assert int(price) == 1001
    assert int(amount) == 2
    assert int(q.notional(price, amount)) == 2002


def test_quantize_rejects_non_multiples() -> None:
    q = Quantizer.from_strings("0.01", "0.001")
    with pytest.raises(QuantizationError):
        q.quantize_price("10.001")
    with pytest.raises(QuantizationError):
        q.quantize_amount("0.0005")


def test_quantize_sign_rules() -> None:
    q = Quantizer.from_strings("1", "1")
    with pytest.raises(QuantizationError):
        q.quantize_price("0")
    with pytest.raises(QuantizationError):
        q.quantize_price("-1")
    assert int(q.quantize_amount("0")) == 0
    with pytest.raises(QuantizationError):
        q.quantize_amount("-1")


def test_quantizer_validates_increments() -> None:
    with pytest.raises(QuantizationError):
        Quantizer(Decimal("0"), Decimal("1"))
    with pytest.raises(QuantizationError):
        Quantizer(Decimal("-1"), Decimal("1"))
