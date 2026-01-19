"""Binary event log format (v0)."""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
import struct

from mm_bt.core.errors import SchemaError

MAGIC = b"MMEVLOG\x00"
VERSION = 1
ENDIAN_LITTLE = 1

HEADER_BASE_FMT = "<8sB B H I"
HEADER_BASE_SIZE = struct.calcsize(HEADER_BASE_FMT)

HEADER_V1_EXTRA_FMT = "<Q Q 32s"
HEADER_V1_EXTRA_SIZE = struct.calcsize(HEADER_V1_EXTRA_FMT)
HEADER_V1_SIZE = HEADER_BASE_SIZE + HEADER_V1_EXTRA_SIZE

RECORD_HEADER_FMT = "<B B H I"
RECORD_HEADER_SIZE = struct.calcsize(RECORD_HEADER_FMT)

L2_BATCH_HEADER_FMT = "<q q B 3x I"
L2_BATCH_HEADER_SIZE = struct.calcsize(L2_BATCH_HEADER_FMT)

L2_UPDATE_FMT = "<B B H q q I"
L2_UPDATE_SIZE = struct.calcsize(L2_UPDATE_FMT)


class RecordType(IntEnum):
    L2_BATCH = 1


@dataclass(frozen=True, slots=True)
class EvlogHeader:
    version: int
    endian: int
    flags: int
    exchange_id: int | None
    symbol_id: int | None
    quantizer_hash: bytes | None


def pack_header(
    *,
    flags: int = 0,
    exchange_id: int = 0,
    symbol_id: int = 0,
    quantizer_hash: bytes | None = None,
) -> bytes:
    if quantizer_hash is None:
        quantizer_hash = b"\x00" * 32
    if len(quantizer_hash) != 32:
        raise SchemaError("quantizer_hash must be 32 bytes")
    if exchange_id < 0 or exchange_id > (1 << 64) - 1:
        raise SchemaError("exchange_id out of range")
    if symbol_id < 0 or symbol_id > (1 << 64) - 1:
        raise SchemaError("symbol_id out of range")
    base = struct.pack(
        HEADER_BASE_FMT,
        MAGIC,
        VERSION,
        ENDIAN_LITTLE,
        flags,
        0,
    )
    extra = struct.pack(
        HEADER_V1_EXTRA_FMT,
        exchange_id,
        symbol_id,
        quantizer_hash,
    )
    return base + extra


def unpack_header(data: bytes) -> EvlogHeader:
    if len(data) not in (HEADER_BASE_SIZE, HEADER_V1_SIZE):
        raise SchemaError("invalid evlog header size")
    magic, version, endian, flags, reserved = struct.unpack(
        HEADER_BASE_FMT, data[:HEADER_BASE_SIZE]
    )
    if magic != MAGIC:
        raise SchemaError("invalid evlog magic")
    if version not in (0, VERSION):
        raise SchemaError(f"unsupported evlog version: {version}")
    if endian != ENDIAN_LITTLE:
        raise SchemaError(f"unsupported evlog endian: {endian}")
    if reserved != 0:
        raise SchemaError("non-zero header reserved field")
    if version == 0:
        if len(data) != HEADER_BASE_SIZE:
            raise SchemaError("invalid v0 header size")
        return EvlogHeader(
            version=version,
            endian=endian,
            flags=flags,
            exchange_id=None,
            symbol_id=None,
            quantizer_hash=None,
        )
    if len(data) != HEADER_V1_SIZE:
        raise SchemaError("invalid v1 header size")
    exchange_id, symbol_id, quantizer_hash = struct.unpack(
        HEADER_V1_EXTRA_FMT, data[HEADER_BASE_SIZE:]
    )
    return EvlogHeader(
        version=version,
        endian=endian,
        flags=flags,
        exchange_id=exchange_id,
        symbol_id=symbol_id,
        quantizer_hash=quantizer_hash,
    )


def read_header(f) -> EvlogHeader:
    base = f.read(HEADER_BASE_SIZE)
    if not base:
        raise SchemaError("missing evlog header")
    if len(base) != HEADER_BASE_SIZE:
        raise SchemaError("truncated evlog header")
    magic, version, endian, flags, reserved = struct.unpack(
        HEADER_BASE_FMT, base
    )
    if magic != MAGIC:
        raise SchemaError("invalid evlog magic")
    if version == 0:
        if endian != ENDIAN_LITTLE:
            raise SchemaError(f"unsupported evlog endian: {endian}")
        if reserved != 0:
            raise SchemaError("non-zero header reserved field")
        return EvlogHeader(
            version=version,
            endian=endian,
            flags=flags,
            exchange_id=None,
            symbol_id=None,
            quantizer_hash=None,
        )
    if version != VERSION:
        raise SchemaError(f"unsupported evlog version: {version}")
    extra = f.read(HEADER_V1_EXTRA_SIZE)
    if len(extra) != HEADER_V1_EXTRA_SIZE:
        raise SchemaError("truncated evlog header")
    exchange_id, symbol_id, quantizer_hash = struct.unpack(
        HEADER_V1_EXTRA_FMT, extra
    )
    if endian != ENDIAN_LITTLE:
        raise SchemaError(f"unsupported evlog endian: {endian}")
    if reserved != 0:
        raise SchemaError("non-zero header reserved field")
    return EvlogHeader(
        version=version,
        endian=endian,
        flags=flags,
        exchange_id=exchange_id,
        symbol_id=symbol_id,
        quantizer_hash=quantizer_hash,
    )


def unpack_record_header(data: bytes) -> tuple[int, int]:
    if len(data) != RECORD_HEADER_SIZE:
        raise SchemaError("invalid record header size")
    rec_type, flags, reserved, length = struct.unpack(RECORD_HEADER_FMT, data)
    if flags != 0:
        raise SchemaError("non-zero record flags")
    if reserved != 0:
        raise SchemaError("non-zero record reserved field")
    return rec_type, length

