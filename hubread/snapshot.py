"""Decoder for the `QKH1` columnar snapshot format.

A snapshot is the compiled, immutable answer to "what did this dataset look like over this
window" -- a binary a consumer can memory-map or stream without re-parsing ndjson, and whose
bytes are pinned by a schema hash and a trailing checksum so a backtest can cite exactly which
artifact it ran against. This module owns decoding because the format's contract -- byte layout,
`NULL_SENTINEL`, the scale-8 fixed-point encoding of numbers -- must be readable by a bare
consumer with no access to `hub`, the same way `hubread.record` owns the journal's record shape.

The body is genuinely columnar: one contiguous block per column holding every record's value for
that column, in sorted record order -- not one record's worth of columns at a time. That is what
lets a range scan over a single column (say, `known_at`, to binary-search a window) be one
contiguous read, and what lets a future non-Python reader memory-map one column without touching
the rest.

`source`, `source_version`, `parser` and `raw_ref` are dictionary-encoded exactly like `scope`
and `key`, so a record decoded from a snapshot carries the same provenance as the record that was
journaled and compares equal to it -- the replay-equivalence property the rest of the product
depends on. Encoding lives in `hub.snapshot` (the compiler is a writer); this module only reads.
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

#: A null `raw_ref` has no entry in the raw_ref dictionary at all, so it is encoded as this index
#: rather than pointing at a dictionary slot -- there is no string to point at.
NULL_DICT_INDEX = -1

#: The closed set of payload kinds a snapshot stores as an i64 column. `string` fields are
#: deliberately absent: they live in the journal only, so a snapshot never has to size a column
#: for unbounded text.
_TYPE_CODE_BY_NAME = {"number": 0, "bool": 1, "timestamp": 2, "enum": 3}
_TYPE_NAME_BY_CODE = {code: name for name, code in _TYPE_CODE_BY_NAME.items()}

#: The six dictionaries, in the exact header order the format spec fixes.
DICTIONARY_NAMES = ("scope", "key", "source", "source_version", "parser", "raw_ref")

#: Fixed per-record columns before the per-field columns start: known_at, effective_at,
#: period_start, period_end (8 bytes each), scope_idx, key_idx, revision (4 bytes each),
#: availability (1 byte), seq (8 bytes), then source_idx, source_version_idx, parser_idx,
#: raw_ref_idx (4 bytes each).
_FIXED_ROW_BYTES = 8 * 4 + 4 * 3 + 1 + 8 + 4 * 4


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
    sources: tuple[str, ...]
    source_versions: tuple[str, ...]
    parsers: tuple[str, ...]
    raw_refs: tuple[str, ...]


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

    def column(self, fmt: str, count: int) -> tuple[int, ...]:
        """Read `count` contiguous little-endian `fmt` values as one column."""
        if count == 0:
            return ()
        size = struct.calcsize(fmt) * count
        result: tuple[int, ...] = struct.unpack(f"<{count}{fmt}", self.take(size))
        return result


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

    dictionaries: dict[str, list[str]] = {}
    for dict_name in DICTIONARY_NAMES:
        count = cursor.i32()
        dictionaries[dict_name] = [cursor.string() for _ in range(count)]
    scopes, keys = dictionaries["scope"], dictionaries["key"]
    sources, source_versions = dictionaries["source"], dictionaries["source_version"]
    parsers, raw_refs = dictionaries["parser"], dictionaries["raw_ref"]

    header_len = cursor.offset
    row_bytes = _FIXED_ROW_BYTES + field_count * 8
    expected_len = header_len + record_count * row_bytes + 32
    if len(data) != expected_len:
        raise StoreError(f"corrupt snapshot: expected {expected_len} bytes, got {len(data)}")

    trailer = data[-32:]
    if hashlib.sha256(data[:-32]).digest() != trailer:
        raise StoreError("corrupt snapshot: trailer checksum does not match")

    n = record_count
    known_ats = cursor.column("q", n)
    effective_ats = cursor.column("q", n)
    period_starts = cursor.column("q", n)
    period_ends = cursor.column("q", n)
    scope_idxs = cursor.column("i", n)
    key_idxs = cursor.column("i", n)
    revisions = cursor.column("i", n)
    availabilities = cursor.column("B", n)
    seqs = cursor.column("q", n)
    source_idxs = cursor.column("i", n)
    source_version_idxs = cursor.column("i", n)
    parser_idxs = cursor.column("i", n)
    raw_ref_idxs = cursor.column("i", n)
    field_columns = [cursor.column("q", n) for _ in fields]

    availability_by_ordinal = list(Availability)

    def _lookup(dictionary: list[str], idx: int, what: str) -> str:
        if not (0 <= idx < len(dictionary)):
            raise StoreError(f"corrupt snapshot: {what} index {idx} out of range")
        return dictionary[idx]

    records = []
    for i in range(n):
        avail_ord = availabilities[i]
        if not (0 <= avail_ord < len(availability_by_ordinal)):
            raise StoreError(f"corrupt snapshot: unknown availability ordinal {avail_ord}")
        raw_ref_idx = raw_ref_idxs[i]
        raw_ref = None if raw_ref_idx == NULL_DICT_INDEX else _lookup(raw_refs, raw_ref_idx, "raw_ref")
        payload = {field.name: _decode_field_value(field, field_columns[j][i], scale) for j, field in enumerate(fields)}
        records.append(
            Record.create(
                dataset=dataset,
                scope=_lookup(scopes, scope_idxs[i], "scope"),
                key=_lookup(keys, key_idxs[i], "key"),
                revision=revisions[i],
                known_at=known_ats[i],
                effective_at=effective_ats[i],
                availability=availability_by_ordinal[avail_ord],
                source=_lookup(sources, source_idxs[i], "source"),
                source_version=_lookup(source_versions, source_version_idxs[i], "source_version"),
                parser=_lookup(parsers, parser_idxs[i], "parser"),
                raw_ref=raw_ref,
                fields=payload,
                period_start=None if period_starts[i] == NULL_SENTINEL else period_starts[i],
                period_end=None if period_ends[i] == NULL_SENTINEL else period_ends[i],
                seq=seqs[i],
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
        sources=tuple(sources),
        source_versions=tuple(source_versions),
        parsers=tuple(parsers),
        raw_refs=tuple(raw_refs),
    )
    return header, records
