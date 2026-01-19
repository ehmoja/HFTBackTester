"""Minimal L2 replay helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Iterator

from mm_bt.book.api import Book
from mm_bt.book.book_py import BookPy
from mm_bt.core.types import Lots, Ticks, TsNs
from mm_bt.evlog.reader import EvlogReader


def iter_best_bid_ask(
    evlog_path: str | Path,
    *,
    index_path: str | Path | None = None,
    book: Book | None = None,
) -> Iterator[tuple[TsNs, tuple[Ticks | None, Lots | None, Ticks | None, Lots | None]]]:
    active_book = book if book is not None else BookPy()
    with EvlogReader(evlog_path, index_path=index_path) as reader:
        for batch in reader.iter_l2_batches():
            active_book.apply_l2_batch(batch)
            yield batch.ts_recv_ns, active_book.best_bid_ask()

