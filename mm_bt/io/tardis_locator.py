"""Locate Tardis CSV files on disk."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from mm_bt.core.errors import SchemaError


def _validate_date(value: str) -> None:
    if len(value) != 10 or value[4] != "-" or value[7] != "-":
        raise SchemaError(f"invalid date: {value!r}")
    y, m, d = value.split("-")
    if not (y.isdigit() and m.isdigit() and d.isdigit()):
        raise SchemaError(f"invalid date: {value!r}")


def _require_str(value: str, field: str) -> str:
    if value == "":
        raise SchemaError(f"{field} empty")
    return value


def _strip_csv_suffix(name: str) -> str | None:
    if name.endswith(".csv.gz"):
        return name[: -len(".csv.gz")]
    if name.endswith(".csv"):
        return name[: -len(".csv")]
    return None


def _match_prefix(base: str, prefix: str) -> bool:
    if base == prefix:
        return True
    for sep in ("_", "-"):
        if base.startswith(prefix + sep):
            return True
    return False


def _collect_matches(dir_path: Path, key: str) -> list[Path]:
    if not dir_path.exists():
        return []
    if not dir_path.is_dir():
        raise SchemaError(f"not a directory: {dir_path}")
    matches: list[Path] = []
    for entry in dir_path.iterdir():
        if not entry.is_file():
            continue
        base = _strip_csv_suffix(entry.name)
        if base is None:
            continue
        if _match_prefix(base, key):
            matches.append(entry)
    return sorted(matches, key=lambda p: p.name)


def _layout_date_dir(
    root: Path,
    exchange: str,
    data_type: str,
    date: str,
    symbol_or_group: str,
) -> list[Path]:
    dir_path = root / exchange / data_type / date
    return _collect_matches(dir_path, symbol_or_group)


def _layout_symbol_dir(
    root: Path,
    exchange: str,
    data_type: str,
    date: str,
    symbol_or_group: str,
) -> list[Path]:
    dir_path = root / exchange / data_type / symbol_or_group
    return _collect_matches(dir_path, date)


def locate_tardis_files(
    *,
    root: str | Path,
    exchange: str,
    data_type: str,
    date: str,
    symbol_or_group: str,
) -> tuple[Path, ...]:
    root_path = Path(root)
    if not root_path.exists():
        raise SchemaError(f"tardis root not found: {root_path}")
    if not root_path.is_dir():
        raise SchemaError(f"tardis root not a directory: {root_path}")
    exchange = _require_str(exchange, "exchange")
    data_type = _require_str(data_type, "data_type")
    symbol_or_group = _require_str(symbol_or_group, "symbol_or_group")
    _validate_date(date)

    matches_date_dir = _layout_date_dir(
        root_path, exchange, data_type, date, symbol_or_group
    )
    matches_symbol_dir = _layout_symbol_dir(
        root_path, exchange, data_type, date, symbol_or_group
    )
    if matches_date_dir and matches_symbol_dir:
        raise SchemaError(
            "ambiguous tardis layout: matches found in date and symbol dirs"
        )
    matches = matches_date_dir or matches_symbol_dir
    if not matches:
        raise SchemaError("no tardis files found")
    return tuple(sorted(matches, key=lambda p: str(p)))


@dataclass(frozen=True, slots=True)
class TardisLocator:
    root: Path

    def __init__(self, root: str | Path) -> None:
        object.__setattr__(self, "root", Path(root))

    def find(
        self,
        *,
        exchange: str,
        data_type: str,
        date: str,
        symbol_or_group: str,
    ) -> tuple[Path, ...]:
        return locate_tardis_files(
            root=self.root,
            exchange=exchange,
            data_type=data_type,
            date=date,
            symbol_or_group=symbol_or_group,
        )

