"""Portfolio ledger."""

from __future__ import annotations

from dataclasses import dataclass

from mm_bt.core.errors import SchemaError
from mm_bt.core.types import Lots, QuoteAtoms, Side, Ticks


@dataclass
class Portfolio:
    cash: QuoteAtoms
    position: Lots

    def apply_fill(
        self,
        *,
        side: Side,
        price_ticks: Ticks,
        qty_lots: Lots,
        fee_atoms: QuoteAtoms,
        allow_short: bool,
        allow_margin: bool,
    ) -> None:
        qty = int(qty_lots)
        if qty <= 0:
            raise SchemaError("qty_lots must be positive")
        price = int(price_ticks)
        if price <= 0:
            raise SchemaError("price_ticks must be positive")
        notional = price * qty
        fee = int(fee_atoms)
        if fee < 0:
            raise SchemaError("fee_atoms must be non-negative")

        cash = int(self.cash)
        position = int(self.position)
        if side == Side.BID:
            total = notional + fee
            if not allow_margin and cash < total:
                raise SchemaError("insufficient cash for buy")
            cash -= total
            position += qty
        elif side == Side.ASK:
            if not allow_short and position < qty:
                raise SchemaError("insufficient position for sell")
            cash += notional - fee
            position -= qty
        else:
            raise SchemaError(f"invalid side: {side}")

        self.cash = QuoteAtoms(cash)
        self.position = Lots(position)

    def equity(self, mark_price_ticks: Ticks) -> QuoteAtoms:
        mark = int(mark_price_ticks)
        if mark <= 0:
            raise SchemaError("mark_price_ticks must be positive")
        return QuoteAtoms(int(self.cash) + int(self.position) * mark)

