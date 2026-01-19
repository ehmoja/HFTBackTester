"""Simulation loop: fees, portfolio, tape, replay, execution."""

from __future__ import annotations

from mm_bt.sim.exchange import Fill, RunConfig, RunResult, run_backtest
from mm_bt.sim.fees import FixedBpsFeeModel
from mm_bt.sim.portfolio import Portfolio
from mm_bt.sim.replay import iter_best_bid_ask
from mm_bt.sim.tape import TapeWriter

__all__ = [
    "Fill",
    "FixedBpsFeeModel",
    "Portfolio",
    "RunConfig",
    "RunResult",
    "TapeWriter",
    "iter_best_bid_ask",
    "run_backtest",
]
