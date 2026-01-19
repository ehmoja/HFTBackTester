import pytest

from mm_bt.core import SchemaError
from mm_bt.core import Quantizer
from mm_bt.core import Lots, QuoteAtoms, Side
from mm_bt.ingest import compile_l2_csv
from mm_bt.sim import RunConfig, run_backtest
from mm_bt.sim import FixedBpsFeeModel
from mm_bt.strategy import MarketOrder
from mm_bt.strategy import AlternatingMarketOrderStrategy
from mm_bt.experiments import sharpe_ratio


def _write_l2(tmp_path, rows) -> str:
    path = tmp_path / "l2.csv"
    with path.open("w", encoding="utf-8", newline="") as f:
        f.write(
            "exchange,symbol,timestamp,local_timestamp,is_snapshot,side,price,amount\n"
        )
        for row in rows:
            f.write(",".join(row) + "\n")
    return str(path)


def test_run_backtest_dummy_strategy(tmp_path) -> None:
    path = _write_l2(
        tmp_path,
        [
            ["binance", "BTCUSDT", "900", "1000", "true", "bid", "10", "5"],
            ["binance", "BTCUSDT", "905", "1000", "true", "ask", "11", "5"],
            ["binance", "BTCUSDT", "910", "2000", "false", "bid", "10", "5"],
            ["binance", "BTCUSDT", "912", "2000", "false", "ask", "11", "0"],
            ["binance", "BTCUSDT", "915", "2000", "false", "ask", "12", "5"],
            ["binance", "BTCUSDT", "920", "3000", "false", "bid", "10", "0"],
            ["binance", "BTCUSDT", "922", "3000", "false", "bid", "11", "5"],
            ["binance", "BTCUSDT", "925", "3000", "false", "ask", "12", "5"],
            ["binance", "BTCUSDT", "930", "4000", "false", "bid", "11", "5"],
            ["binance", "BTCUSDT", "932", "4000", "false", "ask", "12", "0"],
            ["binance", "BTCUSDT", "935", "4000", "false", "ask", "13", "5"],
        ],
    )
    q = Quantizer.from_strings("1", "1")
    result = compile_l2_csv(
        l2_path=path,
        output_dir=tmp_path / "out",
        quantizer=q,
    )

    strategy = AlternatingMarketOrderStrategy(Lots(1))
    config = RunConfig(
        initial_cash=QuoteAtoms(1000),
        initial_position=Lots(0),
        allow_short=False,
        allow_margin=False,
        sr_benchmark=0.0,
        dsr_trials=10,
    )
    fees = FixedBpsFeeModel(0)
    run = run_backtest(
        evlog_path=result.evlog_path,
        index_path=result.index_path,
        strategy=strategy,
        fee_model=fees,
        config=config,
    )
    assert len(run.fills) == 4
    assert len(run.equity_curve) == 4
    assert len(run.returns) == 3
    expected_returns = [0, -10, 0]
    assert list(run.returns) == expected_returns
    expected_sharpe = sharpe_ratio(expected_returns)
    assert run.sharpe == pytest.approx(expected_sharpe)
    assert 0.0 <= run.psr <= 1.0
    assert 0.0 <= run.dsr <= 1.0


def test_market_order_exceeds_top_of_book(tmp_path) -> None:
    path = _write_l2(
        tmp_path,
        [
            ["binance", "BTCUSDT", "900", "1000", "true", "bid", "10", "1"],
            ["binance", "BTCUSDT", "905", "1000", "true", "ask", "11", "1"],
        ],
    )
    q = Quantizer.from_strings("1", "1")
    result = compile_l2_csv(
        l2_path=path,
        output_dir=tmp_path / "out",
        quantizer=q,
    )
    strategy = AlternatingMarketOrderStrategy(Lots(2))
    config = RunConfig(
        initial_cash=QuoteAtoms(1000),
        initial_position=Lots(0),
        allow_short=False,
        allow_margin=False,
        sr_benchmark=0.0,
        dsr_trials=10,
    )
    fees = FixedBpsFeeModel(0)
    with pytest.raises(SchemaError):
        run_backtest(
            evlog_path=result.evlog_path,
            index_path=result.index_path,
            strategy=strategy,
            fee_model=fees,
            config=config,
        )


def test_short_disallowed(tmp_path) -> None:
    path = _write_l2(
        tmp_path,
        [
            ["binance", "BTCUSDT", "900", "1000", "true", "bid", "10", "1"],
            ["binance", "BTCUSDT", "905", "1000", "true", "ask", "11", "1"],
        ],
    )
    q = Quantizer.from_strings("1", "1")
    result = compile_l2_csv(
        l2_path=path,
        output_dir=tmp_path / "out",
        quantizer=q,
    )

    class SellFirstStrategy:
        def on_batch(self, ctx, book):
            return (MarketOrder(side=Side.ASK, qty_lots=Lots(1)),)

    config = RunConfig(
        initial_cash=QuoteAtoms(1000),
        initial_position=Lots(0),
        allow_short=False,
        allow_margin=False,
        sr_benchmark=0.0,
        dsr_trials=10,
    )
    fees = FixedBpsFeeModel(0)
    with pytest.raises(SchemaError):
        run_backtest(
            evlog_path=result.evlog_path,
            index_path=result.index_path,
            strategy=SellFirstStrategy(),
            fee_model=fees,
            config=config,
        )


def test_ignore_risk_rejects(tmp_path) -> None:
    path = _write_l2(
        tmp_path,
        [
            ["binance", "BTCUSDT", "900", "1000", "true", "bid", "10", "5"],
            ["binance", "BTCUSDT", "905", "1000", "true", "ask", "11", "5"],
            ["binance", "BTCUSDT", "910", "2000", "false", "bid", "10", "5"],
            ["binance", "BTCUSDT", "915", "2000", "false", "ask", "11", "5"],
        ],
    )
    q = Quantizer.from_strings("1", "1")
    result = compile_l2_csv(
        l2_path=path,
        output_dir=tmp_path / "out",
        quantizer=q,
    )

    class SellOnlyStrategy:
        def on_batch(self, ctx, book):
            return (MarketOrder(side=Side.ASK, qty_lots=Lots(1)),)

    config = RunConfig(
        initial_cash=QuoteAtoms(1000),
        initial_position=Lots(0),
        allow_short=False,
        allow_margin=False,
        sr_benchmark=0.0,
        dsr_trials=10,
        ignore_risk_rejects=True,
    )
    fees = FixedBpsFeeModel(0)
    run = run_backtest(
        evlog_path=result.evlog_path,
        index_path=result.index_path,
        strategy=SellOnlyStrategy(),
        fee_model=fees,
        config=config,
    )
    assert len(run.fills) == 0
    assert len(run.equity_curve) == 2


def test_initial_short_disallowed(tmp_path) -> None:
    path = _write_l2(
        tmp_path,
        [
            ["binance", "BTCUSDT", "900", "1000", "true", "bid", "10", "1"],
            ["binance", "BTCUSDT", "905", "1000", "true", "ask", "11", "1"],
        ],
    )
    q = Quantizer.from_strings("1", "1")
    result = compile_l2_csv(
        l2_path=path,
        output_dir=tmp_path / "out",
        quantizer=q,
    )
    strategy = AlternatingMarketOrderStrategy(Lots(1))
    config = RunConfig(
        initial_cash=QuoteAtoms(1000),
        initial_position=Lots(-1),
        allow_short=False,
        allow_margin=False,
        sr_benchmark=0.0,
        dsr_trials=10,
    )
    fees = FixedBpsFeeModel(0)
    with pytest.raises(SchemaError):
        run_backtest(
            evlog_path=result.evlog_path,
            index_path=result.index_path,
            strategy=strategy,
            fee_model=fees,
            config=config,
        )


def test_skip_initial_missing_book(tmp_path) -> None:
    path = _write_l2(
        tmp_path,
        [
            ["binance", "BTCUSDT", "900", "1000", "true", "bid", "10", "5"],
            ["binance", "BTCUSDT", "905", "2000", "false", "ask", "11", "5"],
            ["binance", "BTCUSDT", "910", "3000", "false", "bid", "10", "5"],
            ["binance", "BTCUSDT", "915", "3000", "false", "ask", "11", "5"],
        ],
    )
    q = Quantizer.from_strings("1", "1")
    result = compile_l2_csv(
        l2_path=path,
        output_dir=tmp_path / "out",
        quantizer=q,
    )
    strategy = AlternatingMarketOrderStrategy(Lots(1))
    config = RunConfig(
        initial_cash=QuoteAtoms(1000),
        initial_position=Lots(0),
        allow_short=False,
        allow_margin=False,
        sr_benchmark=0.0,
        dsr_trials=10,
        skip_initial_missing_book=True,
    )
    fees = FixedBpsFeeModel(0)
    run = run_backtest(
        evlog_path=result.evlog_path,
        index_path=result.index_path,
        strategy=strategy,
        fee_model=fees,
        config=config,
    )
    assert len(run.fills) == 2
