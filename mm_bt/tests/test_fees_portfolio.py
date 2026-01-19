import pytest

from mm_bt.core import SchemaError
from mm_bt.core import Lots, QuoteAtoms, Side, Ticks
from mm_bt.sim import FixedBpsFeeModel
from mm_bt.sim import Portfolio


def test_fee_floor_rounding() -> None:
    model = FixedBpsFeeModel(10)
    fee = model.fee_atoms(QuoteAtoms(1050))
    assert int(fee) == 1


def test_portfolio_margin_and_short_checks() -> None:
    portfolio = Portfolio(cash=QuoteAtoms(5), position=Lots(0))
    with pytest.raises(SchemaError):
        portfolio.apply_fill(
            side=Side.BID,
            price_ticks=Ticks(10),
            qty_lots=Lots(1),
            fee_atoms=QuoteAtoms(0),
            allow_short=False,
            allow_margin=False,
        )
    with pytest.raises(SchemaError):
        portfolio.apply_fill(
            side=Side.ASK,
            price_ticks=Ticks(10),
            qty_lots=Lots(1),
            fee_atoms=QuoteAtoms(0),
            allow_short=False,
            allow_margin=True,
        )
