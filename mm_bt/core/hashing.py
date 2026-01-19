"""Stable content hashing utilities."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path


def hash_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def hash_text(text: str) -> str:
    return hash_bytes(text.encode("utf-8"))


def hash_text_bytes(text: str) -> bytes:
    return hashlib.sha256(text.encode("utf-8")).digest()


def stable_json_dumps(payload: object) -> str:
    return json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )


def hash_json(payload: object) -> str:
    return hash_text(stable_json_dumps(payload))


def hash_json_bytes(payload: object) -> bytes:
    return hashlib.sha256(stable_json_dumps(payload).encode("utf-8")).digest()


def hash_text_u64(text: str) -> int:
    digest = hashlib.sha256(text.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "little")


def hash_file(path: str | Path, chunk_size: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()

