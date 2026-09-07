"""Encoder for the `QKH1` columnar snapshot format.

The compiler is a writer on top of a format `hubread.snapshot` owns: this module turns a
schema and a batch of records into the exact bytes a bare consumer can decode. It sorts before
encoding so the compiler's determinism test is not at the mercy of the caller's iteration order,
and it refuses -- rather than rounds -- a number that would lose precision at scale 8, because a
silently rounded number is a fact that has quietly changed.
"""
from __future__ import annotations

import struct
from decimal import Decimal, InvalidOperation
from typing import Any

from hub.errors import RecordError, StoreError
from hub.record import Availability, Record, sort_key
from hub.schema import DatasetSchema, FieldSpec, FieldType
from hubread.snapshot import MAGIC, NULL_SENTINEL, SCALE, VERSION, field_type_code, sha256_of


def _encode_field_value(spec: FieldSpec, value: Any) -> int:
    """The i64 stored for one field's value, or `NULL_SENTINEL` if it is absent.

    Numbers are converted to scale-8 fixed point exactly: a value needing more decimal places
    than the scale allows raises rather than rounding, because a silently rounded fact is worse
    than one the compiler refuses to encode.
    """
    if value is None:
        return NULL_SENTINEL
    if spec.type == FieldType.NUMBER:
        try:
            scaled = Decimal(str(value)).scaleb(SCALE)
        except InvalidOperation as e:
            raise StoreError(f"field {spec.name!r} value {value!r} is not a decimal") from e
        if scaled != scaled.to_integral_value():
            raise StoreError(f"field {spec.name!r} value {value!r} needs more than {SCALE} decimal places")
        return int(scaled)
    if spec.type == FieldType.BOOL:
        if not isinstance(value, bool):
            raise StoreError(f"field {spec.name!r} value {value!r} is not a bool")
        return 1 if value else 0
    if spec.type in (FieldType.TIMESTAMP, FieldType.ENUM):
        if not isinstance(value, int) or isinstance(value, bool):
            raise StoreError(f"field {spec.name!r} value {value!r} is not an int")
        return value
    raise StoreError(f"field {spec.name!r} has type {spec.type!r}, which a snapshot cannot store")


def _pack_str(text: str) -> bytes:
    body = text.encode("utf-8")
    return struct.pack("<i", len(body)) + body


def encode(schema: DatasetSchema, records: list[Record]) -> bytes:
    """Encode `records` under `schema` as a `QKH1` buffer.

    Sorts by `hubread.record.sort_key` before encoding regardless of the caller's input order,
    so encoding the same logical set of records twice -- or a shuffled copy of it -- produces
    byte-identical output; the compiler's determinism guarantee depends on that holding here,
    not on every caller remembering to sort first.
    """
    ordered = sorted(records, key=sort_key)
    if any(r.dataset != schema.name for r in ordered):
        raise RecordError(f"record dataset does not match schema {schema.name!r}")

    scopes = sorted({r.scope for r in ordered})
    keys = sorted({r.key for r in ordered})
    scope_index = {scope: i for i, scope in enumerate(scopes)}
    key_index = {key: i for i, key in enumerate(keys)}
    storable_fields = [f for f in schema.fields.values() if f.type != FieldType.STRING]

    schema_hash_hex = schema.hash().removeprefix("sha256:")
    schema_hash = bytes.fromhex(schema_hash_hex)

    out = bytearray()
    out += MAGIC
    out += struct.pack("<i", VERSION)
    out += schema_hash
    out += _pack_str(schema.name)
    out += struct.pack("<i", len(ordered))
    out += struct.pack("<i", len(storable_fields))
    out += struct.pack("<i", SCALE)
    for f in storable_fields:
        out += _pack_str(f.name)
        out += struct.pack("<B", field_type_code(f.type.value))
        out += _pack_str(f.unit)
    out += struct.pack("<i", len(scopes))
    for scope in scopes:
        out += _pack_str(scope)
    out += struct.pack("<i", len(keys))
    for key in keys:
        out += _pack_str(key)

    availability_ordinal = {a: i for i, a in enumerate(Availability)}
    for record in ordered:
        out += struct.pack("<q", record.known_at)
        out += struct.pack("<q", record.effective_at)
        out += struct.pack("<q", record.period_start if record.period_start is not None else NULL_SENTINEL)
        out += struct.pack("<q", record.period_end if record.period_end is not None else NULL_SENTINEL)
        out += struct.pack("<i", scope_index[record.scope])
        out += struct.pack("<i", key_index[record.key])
        out += struct.pack("<i", record.revision)
        out += struct.pack("<B", availability_ordinal[record.availability])
        out += struct.pack("<q", record.seq)
        for f in storable_fields:
            out += struct.pack("<q", _encode_field_value(f, record.fields.get(f.name)))

    trailer = bytes.fromhex(sha256_of(bytes(out)))
    return bytes(out) + trailer
