"""Download free Tardis CSV test data into the repo's canonical layout."""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from datetime import date as Date

from mm_bt.core.errors import SchemaError
from mm_bt.io.tardis_download import (
    DownloadNotFound,
    build_download_plan,
    download_tardis_csv_gz,
)


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Download Tardis datasets into a canonical on-disk layout"
    )
    p.add_argument("--out-root", required=True, help="output root directory")
    p.add_argument("--api-key", help="tardis API key (optional for free data)")
    p.add_argument(
        "--exchange",
        action="append",
        required=True,
        help="exchange id (repeatable)",
    )
    p.add_argument(
        "--symbol",
        action="append",
        required=True,
        help="symbol or group (repeatable)",
    )
    p.add_argument(
        "--data-type",
        default="incremental_book_L2",
        help="tardis data type",
    )
    p.add_argument(
        "--date",
        action="append",
        default=[],
        help="YYYY-MM-DD (repeatable)",
    )
    p.add_argument(
        "--month",
        action="append",
        default=[],
        help="YYYY-MM; expands to YYYY-MM-01 (repeatable)",
    )
    p.add_argument(
        "--if-exists",
        choices=["error", "skip", "overwrite"],
        default="error",
        help="existing target behavior",
    )
    p.add_argument(
        "--on-missing",
        choices=["error", "skip"],
        default="error",
        help="behavior when a dataset is missing",
    )
    p.add_argument(
        "--no-validate-header",
        action="store_true",
        help="skip incremental_book_L2 header validation",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="print planned downloads only",
    )
    return p.parse_args(argv)


def _parse_month_to_date(value: str) -> str:
    if len(value) != 7 or value[4] != "-":
        raise SchemaError(f"invalid month: {value!r}")
    y, m = value.split("-")
    if not (y.isdigit() and m.isdigit()):
        raise SchemaError(f"invalid month: {value!r}")
    try:
        d = Date(int(y), int(m), 1)
    except ValueError as exc:
        raise SchemaError(f"invalid month: {value!r}") from exc
    return d.isoformat()


@dataclass(frozen=True, slots=True)
class _Job:
    exchange: str
    symbol: str
    date: str


def _expand_jobs(args: argparse.Namespace) -> list[_Job]:
    dates: list[str] = []
    dates.extend(args.date)
    dates.extend(_parse_month_to_date(m) for m in args.month)
    if not dates:
        raise SchemaError("--date or --month required")

    jobs: list[_Job] = []
    for exchange in args.exchange:
        for symbol in args.symbol:
            for d in dates:
                jobs.append(_Job(exchange=exchange, symbol=symbol, date=d))
    return jobs


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    jobs = _expand_jobs(args)
    validate_header = not args.no_validate_header

    for job in jobs:
        plan = build_download_plan(
            root=args.out_root,
            exchange=job.exchange,
            data_type=args.data_type,
            date=job.date,
            symbol=job.symbol,
        )
        if args.dry_run:
            print(
                f"{plan.exchange} {plan.data_type} {plan.date} "
                f"{plan.symbol} -> {plan.target_path}"
            )
            continue

        try:
            path = download_tardis_csv_gz(
                root=args.out_root,
                exchange=job.exchange,
                data_type=args.data_type,
                date=job.date,
                symbol=job.symbol,
                api_key=args.api_key or os.getenv("TARDIS_API_KEY"),
                if_exists=args.if_exists,
                validate_header=validate_header,
            )
        except DownloadNotFound as exc:
            if args.on_missing == "skip":
                print(
                    f"missing {plan.exchange} {plan.data_type} "
                    f"{plan.date} {plan.symbol}"
                )
                continue
            raise
        print(str(path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
