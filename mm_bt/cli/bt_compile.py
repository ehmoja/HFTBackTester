"""Compile L2 CSV into evlog."""

from __future__ import annotations

import argparse
import os

from mm_bt.core.config import FailurePolicy, QuarantineAction
from mm_bt.core.errors import SchemaError
from mm_bt.core.fixedpoint import Quantizer
from mm_bt.ingest.compiler import compile_l2_csv
from mm_bt.io.infer_increments import infer_l2_increments
from mm_bt.io.instrument_meta import (
    StaticJsonProvider,
    TardisInstrumentMetaApiProvider,
)
from mm_bt.io.tardis_locator import TardisLocator


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compile Tardis L2 CSV")
    parser.add_argument("--l2", help="path to L2 CSV(.gz)")
    parser.add_argument("--tardis-root", help="tardis data root directory")
    parser.add_argument("--out", required=True, help="output directory")
    parser.add_argument("--price-increment", help="price increment as string")
    parser.add_argument("--amount-increment", help="amount increment as string")
    parser.add_argument(
        "--instrument-meta", help="path to static instrument meta JSON"
    )
    parser.add_argument(
        "--instrument-meta-cache",
        help="path to instrument meta cache JSON",
    )
    parser.add_argument("--exchange", help="exchange id for instrument meta")
    parser.add_argument("--symbol", help="symbol for instrument meta")
    parser.add_argument("--date", help="YYYY-MM-DD for instrument meta")
    parser.add_argument("--tardis-api-key", help="tardis API key")
    parser.add_argument(
        "--data-type",
        default="incremental_book_L2",
        help="tardis data type",
    )
    parser.add_argument(
        "--failure-policy",
        choices=[p.value for p in FailurePolicy],
        default=FailurePolicy.HARD_FAIL.value,
    )
    parser.add_argument(
        "--quarantine-action",
        choices=[a.value for a in QuarantineAction],
        default=QuarantineAction.HALT.value,
        help="behavior after quarantine event",
    )
    parser.add_argument(
        "--quarantine-out",
        help="path to quarantine jsonl output",
    )
    return parser.parse_args(argv)


def _resolve_quantizer(
    args: argparse.Namespace, l2_paths: list[str]
) -> Quantizer:
    if args.instrument_meta is not None:
        if args.price_increment or args.amount_increment:
            raise SchemaError("cannot mix instrument meta with increments")
        if not (args.exchange and args.symbol and args.date):
            raise SchemaError("exchange/symbol/date required for instrument meta")
        provider = StaticJsonProvider(args.instrument_meta)
        meta = provider.get(args.exchange, args.symbol, args.date)
        return meta.quantizer()

    if args.price_increment is not None or args.amount_increment is not None:
        if args.price_increment is None or args.amount_increment is None:
            raise SchemaError("price-increment and amount-increment required")
        return Quantizer.from_strings(
            args.price_increment, args.amount_increment
        )

    if args.exchange and args.symbol and args.date:
        provider = TardisInstrumentMetaApiProvider(
            api_key=args.tardis_api_key or os.getenv("TARDIS_API_KEY"),
            cache_path=args.instrument_meta_cache,
        )
        try:
            meta = provider.get(args.exchange, args.symbol, args.date)
            return meta.quantizer()
        except SchemaError:
            pass

    price_inc, amount_inc = infer_l2_increments(l2_paths)
    return Quantizer.from_strings(price_inc, amount_inc)


def _resolve_l2_paths(args: argparse.Namespace) -> list[str]:
    if args.l2 and args.tardis_root:
        raise SchemaError("cannot mix --l2 with --tardis-root")
    if args.l2:
        return [args.l2]
    if args.tardis_root:
        if not (args.exchange and args.symbol and args.date):
            raise SchemaError(
                "exchange/symbol/date required for tardis locator"
            )
        locator = TardisLocator(args.tardis_root)
        paths = locator.find(
            exchange=args.exchange,
            data_type=args.data_type,
            date=args.date,
            symbol_or_group=args.symbol,
        )
        return [str(p) for p in paths]
    raise SchemaError("--l2 or --tardis-root required")


def _output_prefix(args: argparse.Namespace) -> str | None:
    if args.tardis_root:
        safe = lambda value: value.replace("/", "_")
        return f"{safe(args.exchange)}-{safe(args.symbol)}-{args.date}-{safe(args.data_type)}"
    return None


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    l2_paths = _resolve_l2_paths(args)
    quantizer = _resolve_quantizer(args, l2_paths)
    result = compile_l2_csv(
        l2_paths=l2_paths,
        output_dir=args.out,
        quantizer=quantizer,
        failure_policy=FailurePolicy(args.failure_policy),
        quarantine_action=QuarantineAction(args.quarantine_action),
        quarantine_path=args.quarantine_out,
        output_prefix=_output_prefix(args),
    )
    print(
        f"evlog={result.evlog_path} index={result.index_path} "
        f"manifest={result.manifest_path} records={result.record_count}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
