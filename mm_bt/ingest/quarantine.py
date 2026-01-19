"""Quarantine recording utilities."""

from __future__ import annotations

from dataclasses import dataclass, field, asdict, is_dataclass
from enum import Enum
import json
from pathlib import Path
from typing import Any, Protocol, TextIO

from mm_bt.core.config import FailurePolicy
from mm_bt.core.errors import SchemaError


@dataclass(frozen=True, slots=True)
class QuarantineRecord:
    reason: str
    source: str
    line_number: int
    payload: object


class QuarantineSink(Protocol):
    def record(self, record: QuarantineRecord) -> None:
        """Record a quarantine event."""


@dataclass
class ListQuarantineSink:
    records: list[QuarantineRecord] = field(default_factory=list)

    def record(self, record: QuarantineRecord) -> None:
        self.records.append(record)


def record_quarantine(
    policy: FailurePolicy,
    sink: QuarantineSink | None,
    record: QuarantineRecord,
) -> None:
    """Record a quarantine event without suppressing the caller's exception."""
    if policy == FailurePolicy.QUARANTINE and sink is not None:
        sink.record(record)


def _normalize_payload(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.name.lower()
    if is_dataclass(value):
        return _normalize_payload(asdict(value))
    if isinstance(value, dict):
        return {str(k): _normalize_payload(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalize_payload(v) for v in value]
    return value


@dataclass
class JsonlQuarantineSink:
    path: str | Path
    _file: TextIO | None = None

    def __enter__(self) -> "JsonlQuarantineSink":
        if self._file is not None:
            raise SchemaError("quarantine sink already open")
        self._file = Path(self.path).open("w", encoding="utf-8", newline="")
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None

    def record(self, record: QuarantineRecord) -> None:
        if self._file is None:
            raise SchemaError("quarantine sink is not open")
        payload = {
            "reason": record.reason,
            "source": record.source,
            "line_number": record.line_number,
            "payload": _normalize_payload(record.payload),
        }
        data = json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        )
        self._file.write(data + "\n")

