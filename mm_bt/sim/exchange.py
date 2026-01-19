"""Deterministic replay + execution for market orders."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from mm_bt.book.api import Book
from mm_bt.book.book_py import BookPy
from mm_bt.core.errors import SchemaError
from mm_bt.core.types import Bps, Lots, QuoteAtoms, Side, Ticks, TsNs
from mm_bt.evlog.reader import EvlogReader
from mm_bt.experiments.psr_dsr import (
    deflated_sharpe_ratio,
    probabilistic_sharpe_ratio,
    sharpe_ratio,
)
from mm_bt.metrics.pnl import returns_from_equity
from mm_bt.sim.fees import FixedBpsFeeModel
from mm_bt.sim.portfolio import Portfolio
from mm_bt.sim.tape import TapeWriter
from mm_bt.strategy.api import BookSnapshot, MarketOrder, Strategy, StrategyContext


@dataclass(frozen=True, slots=True)
class Fill:
    ts_recv_ns: TsNs
    side: Side
    price_ticks: Ticks
    qty_lots: Lots
    notional: QuoteAtoms
    fee_atoms: QuoteAtoms


@dataclass(frozen=True, slots=True)
class RunConfig:
    initial_cash: QuoteAtoms
    initial_position: Lots
    allow_short: bool
    allow_margin: bool
    sr_benchmark: float
    dsr_trials: int
    skip_initial_missing_book: bool = False
    ignore_risk_rejects: bool = False


@dataclass(frozen=True, slots=True)
class RunResult:
    fills: tuple[Fill, ...]
    equity_curve: tuple[tuple[TsNs, QuoteAtoms], ...]
    returns: tuple[Bps, ...]
    sharpe: float
    psr: float
    dsr: float


def _ensure_snapshot_ready(
    bid_px: Ticks | None,
    bid_qty: Lots | None,
    ask_px: Ticks | None,
    ask_qty: Lots | None,
) -> BookSnapshot:
    if bid_px is None or bid_qty is None or ask_px is None or ask_qty is None:
        raise SchemaError("missing best bid/ask")
    if int(bid_qty) <= 0 or int(ask_qty) <= 0:
        raise SchemaError("non-positive top-of-book size")
    return BookSnapshot(
        bid_px=bid_px,
        bid_qty=bid_qty,
        ask_px=ask_px,
        ask_qty=ask_qty,
    )


def _execute_market_order(
    *,
    ts_recv_ns: TsNs,
    order: MarketOrder,
    book: BookSnapshot,
    portfolio: Portfolio,
    fee_model: FixedBpsFeeModel,
    allow_short: bool,
    allow_margin: bool,
    ignore_risk_rejects: bool,
) -> Fill | None:
    qty = int(order.qty_lots)
    if qty <= 0:
        raise SchemaError("qty_lots must be positive")
    if order.side == Side.BID:
        price = book.ask_px
        available = book.ask_qty
    elif order.side == Side.ASK:
        price = book.bid_px
        available = book.bid_qty
    else:
        raise SchemaError(f"invalid side: {order.side}")
    if qty > int(available):
        raise SchemaError("market order exceeds top-of-book size")

    notional = QuoteAtoms(int(price) * qty)
    fee_atoms = fee_model.fee_atoms(notional)
    if order.side == Side.BID:
        if not allow_margin:
            total = int(notional) + int(fee_atoms)
            if int(portfolio.cash) < total:
                if ignore_risk_rejects:
                    return None
                raise SchemaError("insufficient cash for buy")
    elif order.side == Side.ASK:
        if not allow_short and int(portfolio.position) < qty:
            if ignore_risk_rejects:
                return None
            raise SchemaError("insufficient position for sell")
    portfolio.apply_fill(
        side=order.side,
        price_ticks=price,
        qty_lots=order.qty_lots,
        fee_atoms=fee_atoms,
        allow_short=allow_short,
        allow_margin=allow_margin,
    )
    return Fill(
        ts_recv_ns=ts_recv_ns,
        side=order.side,
        price_ticks=price,
        qty_lots=order.qty_lots,
        notional=notional,
        fee_atoms=fee_atoms,
    )


def run_backtest(
    *,
    evlog_path: str | Path,
    strategy: Strategy,
    fee_model: FixedBpsFeeModel,
    config: RunConfig,
    index_path: str | Path | None = None,
    book: Book | None = None,
    tape: TapeWriter | None = None,
) -> RunResult:
    initial_cash = int(config.initial_cash)
    if initial_cash <= 0:
        raise SchemaError("initial_cash must be positive")
    initial_position = int(config.initial_position)
    if not config.allow_short and initial_position < 0:
        raise SchemaError("initial_position short not allowed")

    portfolio = Portfolio(
        cash=config.initial_cash,
        position=config.initial_position,
    )
    active_book = book if book is not None else BookPy()

    fills: list[Fill] = []
    equity_curve: list[tuple[TsNs, QuoteAtoms]] = []
    action_id = 0
    fill_id = 0
    seen_ready_book = False

    with EvlogReader(evlog_path, index_path=index_path) as reader:
        for batch in reader.iter_l2_batches():
            active_book.apply_l2_batch(batch)
            bid_px, bid_qty, ask_px, ask_qty = active_book.best_bid_ask()
            if (
                bid_px is None
                or bid_qty is None
                or ask_px is None
                or ask_qty is None
            ):
                if config.skip_initial_missing_book and not seen_ready_book:
                    continue
                raise SchemaError("missing best bid/ask")
            snapshot = _ensure_snapshot_ready(bid_px, bid_qty, ask_px, ask_qty)
            seen_ready_book = True
            ctx = StrategyContext(
                ts_recv_ns=batch.ts_recv_ns,
                cash=portfolio.cash,
                position=portfolio.position,
            )
            actions = strategy.on_batch(ctx, snapshot)
            if actions is None:
                raise SchemaError("strategy returned no actions iterable")
            try:
                iterator = iter(actions)
            except TypeError as exc:
                raise SchemaError("strategy actions not iterable") from exc
            for action in iterator:
                action_id += 1
                if not isinstance(action, MarketOrder):
                    raise SchemaError("unsupported action type")
                if tape is not None:
                    tape.record_action(
                        ts_recv_ns=batch.ts_recv_ns,
                        action_id=action_id,
                        side=action.side,
                        qty_lots=action.qty_lots,
                    )
                fill = _execute_market_order(
                    ts_recv_ns=batch.ts_recv_ns,
                    order=action,
                    book=snapshot,
                    portfolio=portfolio,
                    fee_model=fee_model,
                    allow_short=config.allow_short,
                    allow_margin=config.allow_margin,
                    ignore_risk_rejects=config.ignore_risk_rejects,
                )
                if fill is None:
                    continue
                fill_id += 1
                if tape is not None:
                    tape.record_fill(
                        ts_recv_ns=batch.ts_recv_ns,
                        fill_id=fill_id,
                        action_id=action_id,
                        side=fill.side,
                        price_ticks=fill.price_ticks,
                        qty_lots=fill.qty_lots,
                        notional=fill.notional,
                        fee_atoms=fill.fee_atoms,
                    )
                fills.append(fill)

            # Liquidation value: bid for long/flat, ask for short.
            mark_px = (
                snapshot.bid_px
                if int(portfolio.position) >= 0
                else snapshot.ask_px
            )
            equity = portfolio.equity(mark_px)
            equity_curve.append((batch.ts_recv_ns, equity))
            if tape is not None:
                tape.record_equity(
                    ts_recv_ns=batch.ts_recv_ns,
                    cash=portfolio.cash,
                    position=portfolio.position,
                    equity=equity,
                )

    equity_values = [int(value) for _, value in equity_curve]
    returns = returns_from_equity(equity_values, initial_cash=initial_cash)
    sharpe = sharpe_ratio(returns)
    psr = probabilistic_sharpe_ratio(
        returns, sr_benchmark=config.sr_benchmark
    )
    dsr = deflated_sharpe_ratio(
        returns,
        sr_benchmark=config.sr_benchmark,
        n_trials=config.dsr_trials,
    )
    return RunResult(
        fills=tuple(fills),
        equity_curve=tuple(equity_curve),
        returns=returns,
        sharpe=sharpe,
        psr=psr,
        dsr=dsr,
    )
