"""Batch incremental L2 updates by local timestamp."""

from __future__ import annotations

from typing import Iterable, Iterator

from mm_bt.core.config import FailurePolicy, QuarantineAction
from mm_bt.core.errors import OrderingError, QuantizationError, SchemaError
from mm_bt.core.fixedpoint import Quantizer
from mm_bt.core.types import TsNs
from mm_bt.evlog.types import L2Batch, L2Update
from mm_bt.ingest.quarantine import QuarantineRecord, QuarantineSink, record_quarantine
from mm_bt.io.tardis_csv import L2Row


def _handle_error(
    exc: Exception,
    *,
    policy: FailurePolicy,
    action: QuarantineAction,
    sink: QuarantineSink | None,
    source: str,
    line_number: int,
    payload: object,
) -> QuarantineAction:
    record_quarantine(
        policy,
        sink,
        QuarantineRecord(
            reason=str(exc),
            source=source,
            line_number=line_number,
            payload=payload,
        ),
    )
    if policy == FailurePolicy.HARD_FAIL or action == QuarantineAction.HALT:
        raise exc
    return action


def iter_l2_batches(
    rows: Iterable[L2Row],
    quantizer: Quantizer,
    *,
    failure_policy: FailurePolicy,
    quarantine_action: QuarantineAction = QuarantineAction.HALT,
    quarantine_sink: QuarantineSink | None = None,
    source: str = "",
) -> Iterator[L2Batch]:
    """
    Invariants:
    - local_timestamp is non-decreasing; no reordering.
    - within a batch, is_snapshot is constant.
    - updates are emitted in file order.
    - quarantine_action controls post-error handling for quarantined rows/batches.
    """

    prev_local_ts: int | None = None
    prev_is_snapshot = False
    expected_exchange: str | None = None
    expected_symbol: str | None = None
    last_source: str | None = None
    batch_local_ts: int | None = None
    batch_is_snapshot: bool | None = None
    batch_resets_book: bool | None = None
    batch_ts_exch_us: int | None = None
    updates: list[L2Update] = []
    skip_batch = False

    for row in rows:
        row_source = source or row.source
        last_source = row_source
        if expected_exchange is None:
            expected_exchange = row.exchange
            expected_symbol = row.symbol
        elif row.exchange != expected_exchange or row.symbol != expected_symbol:
            action = _handle_error(
                SchemaError(
                    f"mixed exchange/symbol in stream: "
                    f"{row.exchange}/{row.symbol}"
                ),
                policy=failure_policy,
                action=quarantine_action,
                sink=quarantine_sink,
                source=row_source,
                line_number=row.line_number,
                payload=row,
            )
            if action == QuarantineAction.SKIP_ROW:
                prev_local_ts = row.local_timestamp_us
                continue
            if action == QuarantineAction.SKIP_BATCH:
                if batch_local_ts is None:
                    batch_local_ts = row.local_timestamp_us
                skip_batch = True
                updates = []
                batch_ts_exch_us = None
                prev_local_ts = row.local_timestamp_us
                continue

        if prev_local_ts is not None and row.local_timestamp_us < prev_local_ts:
            action = _handle_error(
                OrderingError(
                    f"local_timestamp decreased: {row.local_timestamp_us} < {prev_local_ts}"
                ),
                policy=failure_policy,
                action=quarantine_action,
                sink=quarantine_sink,
                source=row_source,
                line_number=row.line_number,
                payload=row,
            )
            if action in (QuarantineAction.SKIP_ROW, QuarantineAction.SKIP_BATCH):
                continue

        if skip_batch:
            if batch_local_ts is not None and row.local_timestamp_us == batch_local_ts:
                prev_local_ts = row.local_timestamp_us
                continue
            skip_batch = False
            batch_local_ts = None
            batch_is_snapshot = None
            batch_resets_book = None
            batch_ts_exch_us = None
            updates = []

        if batch_local_ts is None:
            batch_local_ts = row.local_timestamp_us
            batch_is_snapshot = row.is_snapshot
            batch_resets_book = (not prev_is_snapshot) and row.is_snapshot
        elif row.local_timestamp_us != batch_local_ts:
            if updates:
                if batch_ts_exch_us is None:
                    action = _handle_error(
                        SchemaError("missing exchange timestamp in batch"),
                        policy=failure_policy,
                        action=quarantine_action,
                        sink=quarantine_sink,
                        source=row_source,
                        line_number=row.line_number,
                        payload=row,
                    )
                    if action in (
                        QuarantineAction.SKIP_ROW,
                        QuarantineAction.SKIP_BATCH,
                    ):
                        updates = []
                if updates:
                    yield L2Batch(
                        ts_recv_ns=TsNs(batch_local_ts * 1_000),
                        ts_exch_ns=TsNs(batch_ts_exch_us * 1_000),
                        resets_book=bool(batch_resets_book),
                        updates=tuple(updates),
                    )
                    prev_is_snapshot = bool(batch_is_snapshot)
            batch_local_ts = row.local_timestamp_us
            batch_is_snapshot = row.is_snapshot
            batch_resets_book = (not prev_is_snapshot) and row.is_snapshot
            batch_ts_exch_us = None
            updates = []

        if batch_is_snapshot is not None and row.is_snapshot != batch_is_snapshot:
            action = _handle_error(
                SchemaError(
                    "mixed is_snapshot values within a local_timestamp batch"
                ),
                policy=failure_policy,
                action=quarantine_action,
                sink=quarantine_sink,
                source=row_source,
                line_number=row.line_number,
                payload=row,
            )
            if action == QuarantineAction.SKIP_ROW:
                prev_local_ts = row.local_timestamp_us
                continue
            if action == QuarantineAction.SKIP_BATCH:
                skip_batch = True
                updates = []
                batch_ts_exch_us = None
                prev_local_ts = row.local_timestamp_us
                continue

        try:
            price_ticks = quantizer.quantize_price(row.price)
            amount_lots = quantizer.quantize_amount(row.amount)
        except QuantizationError as exc:
            action = _handle_error(
                QuantizationError(f"{exc} at line {row.line_number}"),
                policy=failure_policy,
                action=quarantine_action,
                sink=quarantine_sink,
                source=row_source,
                line_number=row.line_number,
                payload=row,
            )
            if action == QuarantineAction.SKIP_ROW:
                prev_local_ts = row.local_timestamp_us
                continue
            if action == QuarantineAction.SKIP_BATCH:
                skip_batch = True
                updates = []
                batch_ts_exch_us = None
                prev_local_ts = row.local_timestamp_us
                continue

        updates.append(
            L2Update(
                side=row.side,
                price_ticks=price_ticks,
                amount_lots=amount_lots,
                is_snapshot=row.is_snapshot,
            )
        )
        batch_ts_exch_us = row.timestamp_us
        prev_local_ts = row.local_timestamp_us

    if batch_local_ts is not None:
        if updates:
            if batch_ts_exch_us is None:
                _handle_error(
                    SchemaError("missing exchange timestamp in batch"),
                    policy=failure_policy,
                    action=quarantine_action,
                    sink=quarantine_sink,
                    source=source or last_source or "",
                    line_number=0,
                    payload=None,
                )
            else:
                yield L2Batch(
                    ts_recv_ns=TsNs(batch_local_ts * 1_000),
                    ts_exch_ns=TsNs(batch_ts_exch_us * 1_000),
                    resets_book=bool(batch_resets_book),
                    updates=tuple(updates),
                )

