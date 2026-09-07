"""Decoder for the `QKH1` columnar snapshot format.

A snapshot is the compiled, immutable answer to "what did this dataset look like over this
window" -- a binary a consumer can memory-map or stream without re-parsing ndjson, and whose
bytes are pinned by a schema hash and a trailing checksum so a backtest can cite exactly which
artifact it ran against. This module owns decoding because the format's contract -- byte layout,
`NULL_SENTINEL`, the scale-8 fixed-point encoding of numbers -- must be readable by a bare
consumer with no access to `hub`, the same way `hubread.record` owns the journal's record shape.

Encoding lives in `hub.snapshot` (the compiler is a writer); this module only ever reads.
"""
from __future__ import annotations

import hashlib
import struct
from dataclasses import dataclass
from decimal import Decimal

from hubread.errors import StoreError
from hubread.record import Availability, Record, decimal_str

MAGIC = b"QKH1"
VERSION = 1
SCALE = 8

#: The sentinel for an absent value in any i64 column -- a number, a timestamp, or an enum
#: ordinal that a record legitimately does not have. `i64.MIN` is chosen because scale-8 decimal
#: values, epoch-ms timestamps and enum ordinals never legitimately reach it, so it can never be
#: confused with real data.
NULL_SENTINEL = -(2**63)

#: The closed set of payload kinds a snapshot stores as an i64 column. `string` fields are
#: deliberately absent: they live in the journal only, so a snapshot never has to size a column
#: for unbounded text.
_TYPE_CODE_BY_NAME = {"number": 0, "bool": 1, "timestamp": 2, "enum": 3}
_TYPE_NAME_BY_CODE = {code: name for name, code in _TYPE_CODE_BY_NAME.items()}

#: Fixed per-record body columns before the per-field columns start: known_at, effective_at,
#: period_start, period_end (8 bytes each), scope_idx, key_idx, revision (4 bytes each),
#: availability (1 byte), seq (8 bytes).
_FIXED_ROW_BYTES = 8 * 4 + 4 * 3 + 1 + 8


def sha256_of(data: bytes) -> str:
    """Hex-encoded SHA-256 of `data`, the one hashing form used for a snapshot's trailer."""
    return hashlib.sha256(data).hexdigest()


def field_type_code(name: str) -> int:
    """The stable u8 code a snapshot's field table stores for a storable field type name."""
    try:
        return _TYPE_CODE_BY_NAME[name]
    except KeyError as e:
        raise StoreError(f"snapshot cannot store field type {name!r}") from e


@dataclass(frozen=True)
class SnapshotField:
    """One column of a snapshot's field table: enough to interpret its i64 values."""

    name: str
    type: str
    unit: str


@dataclass(frozen=True)
class SnapshotHeader:
    """Everything a snapshot's header declares, decoded and validated.

    Kept separate from the record list so a caller who only needs to check the schema hash or
    the record count -- a manifest builder, a coverage check -- never has to decode the body.
    """

    version: int
    schema_hash: bytes
    dataset: str
    record_count: int
    field_count: int
    scale: int
    fields: tuple[SnapshotField, ...]
    scopes: tuple[str, ...]
    keys: tuple[str, ...]


class _Cursor:
    """A bounds-checked read position into a snapshot buffer.

    Every read goes through this so a corrupt length anywhere in the header turns into a clean
    `StoreError` at the point of failure instead of a Python slicing operation silently returning
    fewer bytes than expected and letting corruption propagate unnoticed into later fields.
    """

    def __init__(self, data: bytes) -> None:
        self.data = data
        self.offset = 0

    def take(self, n: int) -> bytes:
        end = self.offset + n
        if n < 0 or end > len(self.data):
            raise StoreError("truncated snapshot: unexpected end of buffer")
        chunk = self.data[self.offset : end]
        self.offset = end
        return chunk

    def i32(self) -> int:
        return int(struct.unpack("<i", self.take(4))[0])

    def i64(self) -> int:
        return int(struct.unpack("<q", self.take(8))[0])

    def u8(self) -> int:
        return self.take(1)[0]

    def string(self) -> str:
        length = self.i32()
        if length < 0:
            raise StoreError("truncated snapshot: negative string length")
        try:
            return self.take(length).decode("utf-8")
        except UnicodeDecodeError as e:
            raise StoreError(f"truncated snapshot: invalid utf-8: {e}") from e


def _decode_field_value(field: SnapshotField, raw: int, scale: int) -> object:
    """Reverse of `hub.snapshot._encode_field_value` for one column's stored i64."""
    if raw == NULL_SENTINEL:
        return None
    if field.type == "number":
        return decimal_str(Decimal(raw).scaleb(-scale))
    if field.type == "bool":
        return bool(raw)
    return raw  # timestamp (epoch ms) and enum (ordinal) both travel as plain int.


def decode(data: bytes) -> tuple[SnapshotHeader, list[Record]]:
    """Validate and decode a `QKH1` buffer, returning its header and every record in it.

    Validates the magic, the version, the exact expected byte length, and the trailing SHA-256
    over everything before it -- a truncated buffer or a single flipped byte must raise rather
    than silently hand back a plausible-looking but wrong fact.
    """
    if data[:4] != MAGIC:
        raise StoreError(f"not a QKH1 snapshot: bad magic {data[:4]!r}")
    cursor = _Cursor(data)
    cursor.take(4)  # magic, already checked
    version = cursor.i32()
    if version != VERSION:
        raise StoreError(f"unsupported snapshot version {version}")
    schema_hash = cursor.take(32)
    dataset = cursor.string()
    record_count = cursor.i32()
    field_count = cursor.i32()
    scale = cursor.i32()
    if record_count < 0 or field_count < 0 or scale < 0:
        raise StoreError("corrupt snapshot: negative count in header")

    fields = []
    for _ in range(field_count):
        name = cursor.string()
        code = cursor.u8()
        if code not in _TYPE_NAME_BY_CODE:
            raise StoreError(f"corrupt snapshot: unknown field type code {code}")
        unit = cursor.string()
        fields.append(SnapshotField(name=name, type=_TYPE_NAME_BY_CODE[code], unit=unit))

    scopes = [cursor.string() for _ in range(cursor.i32())]
    keys = [cursor.string() for _ in range(cursor.i32())]

    header_len = cursor.offset
    row_bytes = _FIXED_ROW_BYTES + field_count * 8
    expected_len = header_len + record_count * row_bytes + 32
    if len(data) != expected_len:
        raise StoreError(f"corrupt snapshot: expected {expected_len} bytes, got {len(data)}")

    trailer = data[-32:]
    if hashlib.sha256(data[:-32]).digest() != trailer:
        raise StoreError("corrupt snapshot: trailer checksum does not match")

    availability_by_ordinal = list(Availability)
    records = []
    for _ in range(record_count):
        known_at = cursor.i64()
        effective_at = cursor.i64()
        period_start = cursor.i64()
        period_end = cursor.i64()
        scope_idx = cursor.i32()
        key_idx = cursor.i32()
        revision = cursor.i32()
        availability_ord = cursor.u8()
        seq = cursor.i64()
        if not (0 <= scope_idx < len(scopes)) or not (0 <= key_idx < len(keys)):
            raise StoreError("corrupt snapshot: scope or key index out of range")
        if not (0 <= availability_ord < len(availability_by_ordinal)):
            raise StoreError(f"corrupt snapshot: unknown availability ordinal {availability_ord}")
        payload = {}
        for field in fields:
            raw = cursor.i64()
            payload[field.name] = _decode_field_value(field, raw, scale)
        records.append(
            Record.create(
                dataset=dataset,
                scope=scopes[scope_idx],
                key=keys[key_idx],
                revision=revision,
                known_at=known_at,
                effective_at=effective_at,
                availability=availability_by_ordinal[availability_ord],
                source="snapshot",
                fields=payload,
                period_start=None if period_start == NULL_SENTINEL else period_start,
                period_end=None if period_end == NULL_SENTINEL else period_end,
                seq=seq,
            )
        )

    header = SnapshotHeader(
        version=version,
        schema_hash=schema_hash,
        dataset=dataset,
        record_count=record_count,
        field_count=field_count,
        scale=scale,
        fields=tuple(fields),
        scopes=tuple(scopes),
        keys=tuple(keys),
    )
    return header, records
