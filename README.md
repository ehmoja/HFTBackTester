# A Lightweight, Performant L2 Data Backtester

Deterministic two-stage backtest pipeline for crypto L2 order book data (`incremental_book_L2`) exported by Tardis.dev.

- Compile stage: strict CSV → validate → quantize (fixed-point ints) → binary event log (`.evlog` + `.idx` + manifest).
- Replay stage: deterministic event replay → book reconstruction → strategy loop → fills + equity curve + Sharpe/PSR/DSR.

## Features

- Deterministic artifacts: compiler emits input/output hashes and fails if inputs change mid-compile.
- Fail-loud ingestion: strict schema, monotone `local_timestamp`, exact divisibility to instrument increments.
- Fixed-point economics in core (`Ticks`, `Lots`, `QuoteAtoms`); `Decimal` only at ingestion boundaries.
- Configurable failure policy: hard-fail or quarantine (skip-row / skip-batch / halt).
- Reference Python L2 book implementation (crossing detection; no silent “repairs”).
- Minimal sim loop (v1): top-of-book market orders, fee model, optional JSONL tape output.

## Getting started

All commands below assume you are running from the repository root (so `mm_bt/` is importable).

### 0) Requirements

- Python 3.12+
- Optional: `tardis-dev` (dataset download + instrument metadata API)
- Optional: `pytest` (tests)

### 1) Create an environment

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt
```

Runtime is stdlib-only; `tardis-dev` is only needed for downloading data and instrument metadata.

### 2) Acquire data

The engine expects Tardis “downloadable CSV” datasets on disk in this canonical layout:

```
{root}/{exchange}/{data_type}/{date}/{symbol}.csv.gz
```

Example:

```
./data/binance/incremental_book_L2/2024-01-01/BTCUSDT.csv.gz
```

You can download into that layout (requires `tardis-dev` + network):

```bash
python -m mm_bt.cli.tardis_download \
  --out-root ./data \
  --exchange binance \
  --symbol BTCUSDT \
  --date 2024-01-01 \
  --data-type incremental_book_L2
```

### 3) Compile CSV → evlog

```bash
python -m mm_bt.cli.bt_compile \
  --tardis-root ./data \
  --exchange binance \
  --symbol BTCUSDT \
  --date 2024-01-01 \
  --out ./evlog_out
```

If you already have a single `*.csv`/`*.csv.gz` file, you can point the compiler at it directly with `--l2 /path/to/file.csv.gz`.

Quantization configuration (pick one):

- Explicit increments: `--price-increment ... --amount-increment ...`
- Static instrument meta JSON: `--instrument-meta ./instrument_meta.json` (plus `--exchange/--symbol/--date`)
- Tardis instrument metadata API (requires `tardis-dev`): `--exchange/--symbol/--date` (uses `TARDIS_API_KEY` or `--tardis-api-key`)
- Fallback: infer increments from the L2 CSV

Outputs:

- `*.evlog`: binary event log of L2 batches
- `*.idx`: time→offset index (optional at run-time, but recommended)
- `*.manifest.json`: hashes, format versions, quantizer params
- `*.quarantine.jsonl`: only when `--failure-policy quarantine` is used (default output path)

### 4) Run a backtest

```bash
python -m mm_bt.cli.bt_run \
  --evlog ./evlog_out/binance-BTCUSDT-2024-01-01-incremental_book_L2.evlog \
  --index ./evlog_out/binance-BTCUSDT-2024-01-01-incremental_book_L2.idx \
  --initial-cash 1000000000 \
  --strategy dummy \
  --qty-lots 1 \
  --fee-bps 0 \
  --tape ./evlog_out/tape.jsonl
```

Units:

- `Ticks = price / price_increment`
- `Lots = amount / amount_increment`
- `QuoteAtoms = Ticks * Lots` (quote notional divided by `price_increment * amount_increment`)

Built-in strategies (CLI):

- `--strategy dummy`: alternating buy/sell market orders
- `--strategy random`: seeded per-batch RNG market orders (requires `--seed`)

### 5) Run tests

```bash
pytest -q
```

## Design choices / invariants

- Primary ordering time is Tardis `local_timestamp` (receive time), converted as `ts_recv_ns = local_timestamp_us * 1_000` (exchange timestamps are not assumed monotone).
- Rows with identical `local_timestamp` are one message; apply the full batch before reading book state.
- `local_timestamp` must be non-decreasing within a file; there is no reorder buffer.
- `incremental_book_L2` rows are level updates (not deltas); `amount=0` deletes that price level.
- `is_snapshot` false→true resets the local book state.
- Economic quantities are fixed-point ints; parsing/quantization is exact, once, at ingestion (no hidden rounding).

## Scope (current v1)

- L2 only (no trades stream, no queue/depletion model).
- Market orders only; fill model is top-of-book and rejects if size exceeds available.
- Single-symbol/day per compiled evlog.

See `BACKTEST_ENGINE_SPEC_V2.md` for the intended v2 model and full invariants.
