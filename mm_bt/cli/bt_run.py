"""Run a backtest from an evlog."""

from __future__ import annotations

import argparse

from mm_bt.core.errors import SchemaError
from mm_bt.core.types import Lots, QuoteAtoms
from mm_bt.sim.exchange import RunConfig, run_backtest
from mm_bt.sim.fees import FixedBpsFeeModel
from mm_bt.sim.tape import TapeWriter
from mm_bt.strategy.dummy import (
    AlternatingMarketOrderStrategy,
    RandomMarketOrderStrategy,
)


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run backtest on evlog")
    parser.add_argument("--evlog", required=True, help="path to evlog file")
    parser.add_argument("--index", help="path to evlog index")
    parser.add_argument("--initial-cash", required=True, help="quote atoms int")
    parser.add_argument(
        "--initial-position", default="0", help="position lots int"
    )
    parser.add_argument("--fee-bps", default="0", help="fee bps int")
    parser.add_argument("--allow-short", action="store_true")
    parser.add_argument("--allow-margin", action="store_true")
    parser.add_argument(
        "--ignore-risk-rejects",
        action="store_true",
        help="ignore orders rejected by cash/position limits",
    )
    parser.add_argument(
        "--skip-initial-missing-book",
        action="store_true",
        help="skip batches until best bid/ask is available",
    )
    parser.add_argument("--sr-benchmark", default="0.0", help="SR benchmark")
    parser.add_argument("--dsr-trials", default="1", help="DSR trials int")
    parser.add_argument("--strategy", default="dummy", help="strategy id")
    parser.add_argument("--qty-lots", default="1", help="dummy qty lots int")
    parser.add_argument("--seed", help="random strategy seed int")
    parser.add_argument(
        "--order-pct",
        default="10",
        help="random strategy order probability percent",
    )
    parser.add_argument(
        "--min-qty-lots",
        default="1",
        help="random strategy min qty lots int",
    )
    parser.add_argument(
        "--max-qty-lots",
        default="5",
        help="random strategy max qty lots int",
    )
    parser.add_argument("--tape", help="path to output tape jsonl")
    return parser.parse_args(argv)


def _parse_int(value: str, field: str) -> int:
    try:
        return int(value)
    except ValueError as exc:
        raise SchemaError(f"{field} must be int") from exc


def _parse_float(value: str, field: str) -> float:
    try:
        return float(value)
    except ValueError as exc:
        raise SchemaError(f"{field} must be float") from exc


def _resolve_strategy(args: argparse.Namespace):
    if args.strategy == "dummy":
        qty = _parse_int(args.qty_lots, "qty-lots")
        return AlternatingMarketOrderStrategy(Lots(qty))
    if args.strategy == "random":
        if args.seed is None:
            raise SchemaError("seed required for random strategy")
        seed = _parse_int(args.seed, "seed")
        order_pct = _parse_int(args.order_pct, "order-pct")
        min_qty = _parse_int(args.min_qty_lots, "min-qty-lots")
        max_qty = _parse_int(args.max_qty_lots, "max-qty-lots")
        return RandomMarketOrderStrategy(
            seed=seed,
            order_pct=order_pct,
            min_qty_lots=Lots(min_qty),
            max_qty_lots=Lots(max_qty),
        )
    raise SchemaError("unsupported strategy")


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    initial_cash = _parse_int(args.initial_cash, "initial-cash")
    initial_position = _parse_int(args.initial_position, "initial-position")
    fee_bps = _parse_int(args.fee_bps, "fee-bps")
    sr_benchmark = _parse_float(args.sr_benchmark, "sr-benchmark")
    dsr_trials = _parse_int(args.dsr_trials, "dsr-trials")

    strategy = _resolve_strategy(args)
    config = RunConfig(
        initial_cash=QuoteAtoms(initial_cash),
        initial_position=Lots(initial_position),
        allow_short=args.allow_short,
        allow_margin=args.allow_margin,
        sr_benchmark=sr_benchmark,
        dsr_trials=dsr_trials,
        skip_initial_missing_book=args.skip_initial_missing_book,
        ignore_risk_rejects=args.ignore_risk_rejects,
    )
    fee_model = FixedBpsFeeModel(fee_bps)

    tape = None
    run_meta = {
        "evlog": args.evlog,
        "strategy": args.strategy,
        "fee_bps": fee_bps,
        "initial_cash": initial_cash,
        "initial_position": initial_position,
    }
    if args.tape:
        tape = TapeWriter(args.tape, run_meta=run_meta)
        with tape:
            result = run_backtest(
                evlog_path=args.evlog,
                index_path=args.index,
                strategy=strategy,
                fee_model=fee_model,
                config=config,
                tape=tape,
            )
    else:
        result = run_backtest(
            evlog_path=args.evlog,
            index_path=args.index,
            strategy=strategy,
            fee_model=fee_model,
            config=config,
        )

    final_equity = int(result.equity_curve[-1][1])
    print(
        f"fills={len(result.fills)} final_equity={final_equity} "
        f"sharpe={result.sharpe:.6f} psr={result.psr:.6f} "
        f"dsr={result.dsr:.6f}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
