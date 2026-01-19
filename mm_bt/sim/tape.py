"""Action/fill/equity tape writer."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import BinaryIO

from mm_bt.core.errors import SchemaError
from mm_bt.core.types import Lots, QuoteAtoms, Side, Ticks, TsNs


def _write_json_line(f: BinaryIO, record: dict[str, object]) -> None:
    payload = json.dumps(
        record, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )
    f.write(payload.encode("utf-8") + b"\n")


@dataclass
class TapeWriter:
    path: str | Path
    run_meta: dict[str, object] | None = None
    _file: BinaryIO | None = None

    def __enter__(self) -> "TapeWriter":
        if self._file is not None:
            raise SchemaError("tape already open")
        self._file = Path(self.path).open("wb")
        if self.run_meta is not None:
            if "type" in self.run_meta:
                raise SchemaError("run_meta cannot override type")
            _write_json_line(self._file, {"type": "header", **self.run_meta})
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None

    def _require_open(self) -> BinaryIO:
        if self._file is None:
            raise SchemaError("tape is not open")
        return self._file

    def record_action(
        self,
        *,
        ts_recv_ns: TsNs,
        action_id: int,
        side: Side,
        qty_lots: Lots,
    ) -> None:
        if action_id <= 0:
            raise SchemaError("action_id must be positive")
        if int(qty_lots) <= 0:
            raise SchemaError("qty_lots must be positive")
        _write_json_line(
            self._require_open(),
            {
                "type": "action",
                "ts_recv_ns": int(ts_recv_ns),
                "action_id": action_id,
                "side": "bid" if side == Side.BID else "ask",
                "qty_lots": int(qty_lots),
            },
        )

    def record_fill(
        self,
        *,
        ts_recv_ns: TsNs,
        fill_id: int,
        action_id: int,
        side: Side,
        price_ticks: Ticks,
        qty_lots: Lots,
        notional: QuoteAtoms,
        fee_atoms: QuoteAtoms,
    ) -> None:
        if fill_id <= 0:
            raise SchemaError("fill_id must be positive")
        if action_id <= 0:
            raise SchemaError("action_id must be positive")
        if int(price_ticks) <= 0:
            raise SchemaError("price_ticks must be positive")
        if int(qty_lots) <= 0:
            raise SchemaError("qty_lots must be positive")
        _write_json_line(
            self._require_open(),
            {
                "type": "fill",
                "ts_recv_ns": int(ts_recv_ns),
                "fill_id": fill_id,
                "action_id": action_id,
                "side": "bid" if side == Side.BID else "ask",
                "price_ticks": int(price_ticks),
                "qty_lots": int(qty_lots),
                "notional": int(notional),
                "fee_atoms": int(fee_atoms),
            },
        )

    def record_equity(
        self,
        *,
        ts_recv_ns: TsNs,
        cash: QuoteAtoms,
        position: Lots,
        equity: QuoteAtoms,
    ) -> None:
        _write_json_line(
            self._require_open(),
            {
                "type": "equity",
                "ts_recv_ns": int(ts_recv_ns),
                "cash": int(cash),
                "position": int(position),
                "equity": int(equity),
            },
        )

