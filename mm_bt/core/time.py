"""Ordering primitives."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True, order=True)
class OrderingKey:
    ts_recv_ns: int
    stream_rank: int
    seq_in_stream: int


def compare_ordering_key(a: OrderingKey, b: OrderingKey) -> int:
    if a < b:
        return -1
    if a > b:
        return 1
    return 0

