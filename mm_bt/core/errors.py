"""Exception hierarchy for the backtest engine."""


class BacktestError(Exception):
    """Base error for backtest failures."""


class SchemaError(BacktestError):
    """Input schema or parsing error."""


class OrderingError(BacktestError):
    """Ordering or monotonicity violation."""


class QuantizationError(BacktestError):
    """Fixed-point quantization failure."""


class QuarantineError(BacktestError):
    """Quarantine-mode error with a recorded payload."""


class DeterminismError(BacktestError):
    """Non-determinism detected."""

