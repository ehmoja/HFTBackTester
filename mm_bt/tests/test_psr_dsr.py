from mm_bt.experiments import (
    deflated_sharpe_ratio,
    probabilistic_sharpe_ratio,
    sharpe_ratio,
)


def test_sharpe_zero_variance_returns_zero() -> None:
    returns = [100, 100, 100, 100]
    assert sharpe_ratio(returns) == 0.0


def test_psr_increases_with_mean() -> None:
    low = [0, 0, 10, 0]
    high = [0, 0, 20, 0]
    psr_low = probabilistic_sharpe_ratio(low, sr_benchmark=0.0)
    psr_high = probabilistic_sharpe_ratio(high, sr_benchmark=0.0)
    assert psr_high > psr_low


def test_dsr_decreases_with_trials() -> None:
    returns = [0, -10, 0, 10]
    dsr_small = deflated_sharpe_ratio(
        returns, sr_benchmark=0.0, n_trials=10
    )
    dsr_large = deflated_sharpe_ratio(
        returns, sr_benchmark=0.0, n_trials=100
    )
    assert dsr_large <= dsr_small


def test_psr_output_range() -> None:
    returns = [0, -10, 0, 10]
    psr = probabilistic_sharpe_ratio(returns, sr_benchmark=0.0)
    assert 0.0 <= psr <= 1.0
