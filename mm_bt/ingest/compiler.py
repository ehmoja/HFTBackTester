"""Compile Tardis L2 CSV into binary event log."""

from __future__ import annotations

import itertools
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence
from contextlib import nullcontext

from mm_bt.core.config import FailurePolicy, QuarantineAction
from mm_bt.core.errors import DeterminismError, SchemaError
from mm_bt.core.hashing import (
    hash_file,
    hash_json,
    hash_json_bytes,
    hash_text,
    hash_text_u64,
    stable_json_dumps,
)
from mm_bt.core.fixedpoint import Quantizer
from mm_bt.evlog.format import VERSION as EVLOG_VERSION
from mm_bt.evlog.index import IndexEntry, write_index
from mm_bt.evlog.writer import EvlogWriter
from mm_bt.ingest.l2_batcher import iter_l2_batches
from mm_bt.ingest.quarantine import JsonlQuarantineSink
from mm_bt.io.tardis_csv import iter_l2_rows

MANIFEST_VERSION = 1
COMPILER_VERSION = 1


@dataclass(frozen=True, slots=True)
class CompileResult:
    evlog_path: Path
    index_path: Path
    manifest_path: Path
    record_count: int


def _base_name(path: Path) -> str:
    name = path.name
    if name.endswith(".gz"):
        name = name[: -len(".gz")]
    if name.endswith(".csv"):
        name = name[: -len(".csv")]
    return name


def _validate_output_prefix(prefix: str) -> None:
    if prefix == "":
        raise SchemaError("output_prefix empty")
    if Path(prefix).name != prefix:
        raise SchemaError("output_prefix must be a basename")


def _manifest_payload(
    *,
    inputs: list[dict[str, str]],
    inputs_hash: str,
    evlog_path: Path,
    evlog_hash: str,
    index_path: Path,
    index_hash: str,
    compiler_hash: str,
    record_count: int,
    quantizer: Quantizer,
    exchange: str,
    symbol: str,
    exchange_id: int,
    symbol_id: int,
    quantizer_hash: str,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "manifest_version": MANIFEST_VERSION,
        "compiler_version": COMPILER_VERSION,
        "compiler_sha256": compiler_hash,
        "inputs": inputs,
        "inputs_sha256": inputs_hash,
        "evlog": {
            "path": str(evlog_path),
            "sha256": evlog_hash,
        },
        "index": {
            "path": str(index_path),
            "sha256": index_hash,
        },
        "record_count": record_count,
        "exchange": exchange,
        "symbol": symbol,
        "exchange_id": exchange_id,
        "symbol_id": symbol_id,
        "quantizer": {
            "price_increment": str(quantizer.price_increment),
            "amount_increment": str(quantizer.amount_increment),
            "sha256": quantizer_hash,
        },
        "format_version": EVLOG_VERSION,
    }
    payload["manifest_sha256"] = hash_text(stable_json_dumps(payload))
    return payload


def compile_l2_csv(
    *,
    l2_path: str | Path | None = None,
    l2_paths: Sequence[str | Path] | None = None,
    output_dir: str | Path,
    quantizer: Quantizer,
    failure_policy: FailurePolicy = FailurePolicy.HARD_FAIL,
    quarantine_action: QuarantineAction = QuarantineAction.HALT,
    quarantine_path: str | Path | None = None,
    output_prefix: str | None = None,
) -> CompileResult:
    if (l2_path is None) == (l2_paths is None):
        raise SchemaError("exactly one of l2_path or l2_paths is required")
    if l2_paths is None:
        paths = [Path(l2_path)]
    else:
        if not l2_paths:
            raise SchemaError("l2_paths must be non-empty")
        paths = [Path(p) for p in l2_paths]
    path_keys = [str(p) for p in paths]
    if len(set(path_keys)) != len(path_keys):
        raise SchemaError("duplicate input paths")
    for path in paths:
        if not path.exists():
            raise SchemaError(f"input path not found: {path}")
        if not path.is_file():
            raise SchemaError(f"input path not a file: {path}")
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if output_prefix is not None:
        _validate_output_prefix(output_prefix)
        base = output_prefix
    elif len(paths) == 1:
        base = _base_name(paths[0])
    else:
        raise SchemaError("output_prefix required for multiple input files")
    evlog_path = out_dir / f"{base}.evlog"
    index_path = out_dir / f"{base}.idx"
    manifest_path = out_dir / f"{base}.manifest.json"
    quarantine_path = (
        out_dir / f"{base}.quarantine.jsonl"
        if quarantine_path is None and failure_policy == FailurePolicy.QUARANTINE
        else quarantine_path
    )

    input_entries = [
        {"path": str(path), "sha256": hash_file(path)} for path in paths
    ]
    inputs_hash = hash_json(input_entries)

    entries: list[IndexEntry] = []
    record_count = 0
    row_iters = [iter_l2_rows(path) for path in paths]
    first_row = None
    first_iter = None
    remaining_iters = []
    for idx, row_iter in enumerate(row_iters):
        try:
            first_row = next(row_iter)
            first_iter = row_iter
            remaining_iters = list(row_iters[idx + 1 :])
            break
        except StopIteration:
            continue
    if first_row is None or first_iter is None:
        raise SchemaError("no rows in input files")
    exchange = first_row.exchange
    symbol = first_row.symbol
    exchange_id = hash_text_u64(exchange)
    symbol_id = hash_text_u64(symbol)
    quantizer_payload = {
        "price_increment": str(quantizer.price_increment),
        "amount_increment": str(quantizer.amount_increment),
    }
    quantizer_hash_hex = hash_json(quantizer_payload)
    quantizer_hash_bytes = hash_json_bytes(quantizer_payload)
    sink_context = (
        JsonlQuarantineSink(quarantine_path)
        if failure_policy == FailurePolicy.QUARANTINE and quarantine_path is not None
        else nullcontext(None)
    )
    with sink_context as sink:
        with EvlogWriter(
            evlog_path,
            exchange_id=exchange_id,
            symbol_id=symbol_id,
            quantizer_hash=quantizer_hash_bytes,
        ) as writer:
            for batch in iter_l2_batches(
                itertools.chain([first_row], first_iter, *remaining_iters),
                quantizer,
                failure_policy=failure_policy,
                quarantine_action=quarantine_action,
                quarantine_sink=sink,
                source="",
            ):
                offset = writer.tell()
                writer.write_l2_batch(batch)
                entries.append(
                    IndexEntry(ts_recv_ns=int(batch.ts_recv_ns), offset=offset)
                )
                record_count += 1

    write_index(index_path, entries)

    for entry in input_entries:
        path = Path(entry["path"])
        current_hash = hash_file(path)
        if current_hash != entry["sha256"]:
            raise DeterminismError(f"input changed during compile: {path}")
    compiler_hash = hash_file(Path(__file__))
    evlog_hash = hash_file(evlog_path)
    index_hash = hash_file(index_path)
    manifest = _manifest_payload(
        inputs=input_entries,
        inputs_hash=inputs_hash,
        evlog_path=evlog_path,
        evlog_hash=evlog_hash,
        index_path=index_path,
        index_hash=index_hash,
        compiler_hash=compiler_hash,
        record_count=record_count,
        quantizer=quantizer,
        exchange=exchange,
        symbol=symbol,
        exchange_id=exchange_id,
        symbol_id=symbol_id,
        quantizer_hash=quantizer_hash_hex,
    )
    manifest_path.write_text(
        stable_json_dumps(manifest) + "\n",
        encoding="utf-8",
    )
    return CompileResult(
        evlog_path=evlog_path,
        index_path=index_path,
        manifest_path=manifest_path,
        record_count=record_count,
    )

