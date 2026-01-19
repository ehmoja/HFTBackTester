"""Download and mirror Tardis CSV datasets into the canonical on-disk layout.

Requires `tardis-dev`.

Layout (matches `mm_bt.io.tardis_locator` date-dir layout):

  {root}/{exchange}/{data_type}/{date}/{symbol}.csv.gz
"""

from __future__ import annotations

import csv
import gzip
import os
from dataclasses import dataclass
from pathlib import Path

from mm_bt.core.errors import SchemaError
from mm_bt.io.tardis_csv import L2_HEADER


def _require_component(value: str, field: str) -> str:
    if value == "":
        raise SchemaError(f"{field} empty")
    if any(sep in value for sep in ("/", "\\", "\x00")):
        raise SchemaError(f"{field} contains path separator: {value!r}")
    if value in (".", ".."):
        raise SchemaError(f"{field} invalid: {value!r}")
    return value


def _validate_date(value: str) -> None:
    if len(value) != 10 or value[4] != "-" or value[7] != "-":
        raise SchemaError(f"invalid date: {value!r}")
    y, m, d = value.split("-")
    if not (y.isdigit() and m.isdigit() and d.isdigit()):
        raise SchemaError(f"invalid date: {value!r}")


def _require_tardis_dev():
    try:
        from tardis_dev import datasets
    except ImportError as exc:
        raise SchemaError(
            "tardis-dev not installed; pip install tardis-dev"
        ) from exc
    return datasets


def _is_not_found_error(exc: BaseException) -> bool:
    code = getattr(exc, "status_code", None) or getattr(exc, "code", None)
    if code == 404:
        return True
    msg = str(exc).lower()
    if "404" in msg or "not found" in msg:
        return True
    return False


def _validate_l2_gz_header(path: Path) -> None:
    try:
        with gzip.open(path, mode="rt", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            try:
                header = next(reader)
            except StopIteration as exc:
                raise SchemaError(f"empty gzip CSV: {path}") from exc
    except OSError as exc:
        raise SchemaError(f"invalid gzip CSV: {path}") from exc
    if header != L2_HEADER:
        raise SchemaError(f"unexpected L2 header in {path}: {header!r}")


class DownloadNotFound(SchemaError):
    """Requested dataset not found."""


@dataclass(frozen=True, slots=True)
class DownloadPlan:
    exchange: str
    data_type: str
    date: str
    symbol: str
    target_path: Path


def _find_flat_downloads(
    *,
    root: Path,
    exchange: str,
    data_type: str,
    date: str,
    symbol: str,
) -> tuple[Path, ...]:
    prefix = f"{exchange}_{data_type}_{date}_{symbol}"
    matches: list[Path] = []
    for base in (root, root / exchange):
        if not base.exists() or not base.is_dir():
            continue
        for path in base.glob(f"{prefix}*.csv.gz"):
            if path.is_file():
                matches.append(path)
    return tuple(sorted(matches, key=lambda p: p.name))


def _relocate_flat_downloads(
    *,
    root: Path,
    exchange: str,
    data_type: str,
    date: str,
    symbol: str,
    target_dir: Path,
    if_exists: str,
) -> tuple[Path, ...]:
    matches = _find_flat_downloads(
        root=root,
        exchange=exchange,
        data_type=data_type,
        date=date,
        symbol=symbol,
    )
    if not matches:
        return ()
    target_dir.mkdir(parents=True, exist_ok=True)
    prefix = f"{exchange}_{data_type}_{date}_{symbol}"
    moved: list[Path] = []
    for src in matches:
        suffix = src.name[len(prefix) : -len(".csv.gz")]
        dest_name = f"{symbol}{suffix}.csv.gz"
        dest = target_dir / dest_name
        if dest.exists():
            if if_exists == "skip":
                continue
            if if_exists == "error":
                raise SchemaError(f"target already exists: {dest}")
            dest.unlink()
        os.replace(src, dest)
        moved.append(dest)
    return tuple(moved)


def canonical_tardis_path(
    *,
    root: str | Path,
    exchange: str,
    data_type: str,
    date: str,
    symbol: str,
) -> Path:
    root_path = Path(root)
    _require_component(exchange, "exchange")
    _require_component(data_type, "data_type")
    _require_component(symbol, "symbol")
    _validate_date(date)
    return (
        root_path
        / exchange
        / data_type
        / date
        / f"{symbol}.csv.gz"
    )


def build_download_plan(
    *,
    root: str | Path,
    exchange: str,
    data_type: str,
    date: str,
    symbol: str,
) -> DownloadPlan:
    return DownloadPlan(
        exchange=exchange,
        data_type=data_type,
        date=date,
        symbol=symbol,
        target_path=canonical_tardis_path(
            root=root,
            exchange=exchange,
            data_type=data_type,
            date=date,
            symbol=symbol,
        ),
    )


def download_tardis_csv_gz(
    *,
    root: str | Path,
    exchange: str,
    data_type: str,
    date: str,
    symbol: str,
    api_key: str | None = None,
    if_exists: str = "error",
    validate_header: bool = True,
) -> Path:
    root_path = Path(root)
    if root_path.exists() and not root_path.is_dir():
        raise SchemaError(f"tardis root not a directory: {root_path}")

    path = canonical_tardis_path(
        root=root_path,
        exchange=exchange,
        data_type=data_type,
        date=date,
        symbol=symbol,
    )
    if if_exists not in ("error", "skip", "overwrite"):
        raise SchemaError(f"invalid if_exists: {if_exists!r}")

    if path.exists():
        if if_exists == "skip":
            if validate_header and data_type == "incremental_book_L2":
                _validate_l2_gz_header(path)
            return path
        if if_exists == "error":
            raise SchemaError(f"target already exists: {path}")
        path.unlink()

    datasets = _require_tardis_dev()
    try:
        datasets.download(
            exchange=exchange,
            data_types=[data_type],
            from_date=date,
            to_date=date,
            symbols=[symbol],
            api_key=api_key,
            download_dir=str(root_path),
        )
    except Exception as exc:  # pragma: no cover - depends on tardis-dev internals
        if _is_not_found_error(exc):
            raise DownloadNotFound(
                f"dataset not found: exchange={exchange} "
                f"data_type={data_type} date={date} symbol={symbol}"
            ) from exc
        raise

    if path.exists():
        if validate_header and data_type == "incremental_book_L2":
            _validate_l2_gz_header(path)
        return path

    moved = _relocate_flat_downloads(
        root=root_path,
        exchange=exchange,
        data_type=data_type,
        date=date,
        symbol=symbol,
        target_dir=path.parent,
        if_exists=if_exists,
    )
    if moved:
        if validate_header and data_type == "incremental_book_L2":
            for moved_path in moved:
                _validate_l2_gz_header(moved_path)
        if path.exists():
            return path
        return moved[0]

    raise DownloadNotFound(
        f"dataset not found: exchange={exchange} "
        f"data_type={data_type} date={date} symbol={symbol}"
    )
