"""Sharpe ratio and deflated Sharpe metrics."""

from __future__ import annotations

import math
from typing import Sequence

from mm_bt.core.errors import SchemaError
from mm_bt.core.types import Bps

_BPS_SCALE = 10_000


def _prepare_returns(returns: Sequence[Bps]) -> list[float]:
    if not returns:
        raise SchemaError("returns must be non-empty")
    out: list[float] = []
    for value in returns:
        if not isinstance(value, int):
            raise SchemaError("returns must be int bps")
        fval = int(value) / _BPS_SCALE
        if not math.isfinite(fval):
            raise SchemaError("returns must be finite")
        out.append(fval)
    return out


def _mean(values: Sequence[float]) -> float:
    return sum(values) / len(values)


def _variance(values: Sequence[float], mean: float) -> float:
    acc = 0.0
    for v in values:
        diff = v - mean
        acc += diff * diff
    return acc / (len(values) - 1)


def _moment(values: Sequence[float], mean: float, power: int) -> float:
    acc = 0.0
    for v in values:
        acc += (v - mean) ** power
    return acc / len(values)


def sharpe_ratio(returns: Sequence[Bps]) -> float:
    returns_f = _prepare_returns(returns)
    if len(returns) < 2:
        raise SchemaError("insufficient returns for Sharpe")
    mean = _mean(returns_f)
    var = _variance(returns_f, mean)
    if var <= 0.0:
        return 0.0
    return mean / math.sqrt(var)


def _skew_kurtosis(returns: Sequence[Bps]) -> tuple[float, float]:
    returns_f = _prepare_returns(returns)
    if len(returns) < 3:
        raise SchemaError("insufficient returns for skew/kurtosis")
    mean = _mean(returns_f)
    m2 = _moment(returns_f, mean, 2)
    if m2 <= 0.0:
        raise SchemaError("zero variance returns")
    m3 = _moment(returns_f, mean, 3)
    m4 = _moment(returns_f, mean, 4)
    skew = m3 / (m2 ** 1.5)
    kurtosis = m4 / (m2 * m2)
    return skew, kurtosis


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _norm_ppf(p: float) -> float:
    if p <= 0.0 or p >= 1.0:
        raise SchemaError("p must be in (0,1)")
    # Acklam's approximation.
    a = [
        -3.969683028665376e01,
        2.209460984245205e02,
        -2.759285104469687e02,
        1.383577518672690e02,
        -3.066479806614716e01,
        2.506628277459239e00,
    ]
    b = [
        -5.447609879822406e01,
        1.615858368580409e02,
        -1.556989798598866e02,
        6.680131188771972e01,
        -1.328068155288572e01,
    ]
    c = [
        -7.784894002430293e-03,
        -3.223964580411365e-01,
        -2.400758277161838e00,
        -2.549732539343734e00,
        4.374664141464968e00,
        2.938163982698783e00,
    ]
    d = [
        7.784695709041462e-03,
        3.224671290700398e-01,
        2.445134137142996e00,
        3.754408661907416e00,
    ]
    plow = 0.02425
    phigh = 1 - plow
    if p < plow:
        q = math.sqrt(-2 * math.log(p))
        return (
            (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5])
            / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
        )
    if p > phigh:
        q = math.sqrt(-2 * math.log(1 - p))
        return -(
            (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5])
            / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
        )
    q = p - 0.5
    r = q * q
    return (
        (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q
        / (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1)
    )


def _psr_denominator(sr_hat: float, skew: float, kurtosis: float) -> float:
    denom = 1.0 - skew * sr_hat + ((kurtosis - 1.0) / 4.0) * sr_hat * sr_hat
    if denom <= 0.0:
        raise SchemaError("invalid PSR denominator")
    return math.sqrt(denom)


def probabilistic_sharpe_ratio(
    returns: Sequence[Bps], *, sr_benchmark: float
) -> float:
    if not math.isfinite(sr_benchmark):
        raise SchemaError("sr_benchmark must be finite")
    if len(returns) < 3:
        raise SchemaError("insufficient returns for PSR")
    sr_hat = sharpe_ratio(returns)
    skew, kurtosis = _skew_kurtosis(returns)
    denom = _psr_denominator(sr_hat, skew, kurtosis)
    z = (sr_hat - sr_benchmark) * math.sqrt(len(returns) - 1) / denom
    return _norm_cdf(z)


def deflated_sharpe_ratio(
    returns: Sequence[Bps], *, sr_benchmark: float, n_trials: int
) -> float:
    """Deflate PSR using quantile 1 - 1/n_trials for the benchmark uplift."""
    if n_trials < 1:
        raise SchemaError("n_trials must be >= 1")
    if not math.isfinite(sr_benchmark):
        raise SchemaError("sr_benchmark must be finite")
    if len(returns) < 3:
        raise SchemaError("insufficient returns for DSR")
    sr_hat = sharpe_ratio(returns)
    skew, kurtosis = _skew_kurtosis(returns)
    denom = _psr_denominator(sr_hat, skew, kurtosis)
    if n_trials == 1:
        sr_star = sr_benchmark
    else:
        z = _norm_ppf(1.0 - (1.0 / n_trials))
        sr_star = sr_benchmark + z * (denom / math.sqrt(len(returns) - 1))
    z_star = (sr_hat - sr_star) * math.sqrt(len(returns) - 1) / denom
    return _norm_cdf(z_star)

