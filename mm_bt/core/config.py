"""Run configuration primitives."""

from __future__ import annotations

from enum import Enum


class FailurePolicy(str, Enum):
    HARD_FAIL = "hard_fail"
    QUARANTINE = "quarantine"


class QuarantineAction(str, Enum):
    HALT = "halt"
    SKIP_ROW = "skip_row"
    SKIP_BATCH = "skip_batch"

