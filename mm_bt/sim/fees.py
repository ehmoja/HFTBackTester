"""Fee models."""

from __future__ import annotations

from dataclasses import dataclass

from mm_bt.core.errors import SchemaError
from mm_bt.core.types import QuoteAtoms


@dataclass(frozen=True, slots=True)
class FixedBpsFeeModel:
    bps: int

    def __post_init__(self) -> None:
        if self.bps < 0:
            raise SchemaError("fee bps must be non-negative")
        if self.bps > 10_000:
            raise SchemaError("fee bps too large")

    def fee_atoms(self, notional: QuoteAtoms) -> QuoteAtoms:
        value = int(notional)
        if value < 0:
            raise SchemaError("notional must be non-negative")
        fee = (value * self.bps) // 10_000
        return QuoteAtoms(fee)

