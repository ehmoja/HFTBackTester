"""Core primitives for the backtest engine."""

from __future__ import annotations

from mm_bt.core.config import FailurePolicy, QuarantineAction
from mm_bt.core.decimal_ctx import DECIMAL_CTX, parse_decimal
from mm_bt.core.errors import (
    BacktestError,
    DeterminismError,
    OrderingError,
    QuantizationError,
    QuarantineError,
    SchemaError,
)
from mm_bt.core.fixedpoint import Quantizer
from mm_bt.core.hashing import (
    hash_bytes,
    hash_file,
    hash_json,
    hash_json_bytes,
    hash_text,
    hash_text_bytes,
    hash_text_u64,
    stable_json_dumps,
)
from mm_bt.core.time import OrderingKey, compare_ordering_key
from mm_bt.core.types import (
    Bps,
    Lots,
    QuoteAtoms,
    Side,
    Symbol,
    Ticks,
    TsNs,
    Venue,
    parse_side,
)

__all__ = [
    "Bps",
    "BacktestError",
    "DECIMAL_CTX",
    "DeterminismError",
    "FailurePolicy",
    "Lots",
    "OrderingError",
    "OrderingKey",
    "QuantizationError",
    "Quantizer",
    "QuarantineAction",
    "QuarantineError",
    "QuoteAtoms",
    "SchemaError",
    "Side",
    "Symbol",
    "Ticks",
    "TsNs",
    "Venue",
    "compare_ordering_key",
    "hash_bytes",
    "hash_file",
    "hash_json",
    "hash_json_bytes",
    "hash_text",
    "hash_text_bytes",
    "hash_text_u64",
    "parse_decimal",
    "parse_side",
    "stable_json_dumps",
]
