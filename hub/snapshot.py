"""Encoder for the `QKH1` columnar snapshot format.

The compiler is a writer on top of a format `hubread.snapshot` owns: this module turns a
schema and a batch of records into the exact bytes a bare consumer can decode. It sorts before
encoding so the compiler's determinism test is not at the mercy of the caller's iteration order,
it writes the body as one contiguous block per column (not one record's columns at a time) to
match the format's genuinely-columnar contract, and it refuses -- rather than rounds -- a number
that would lose precision at scale 8, because a silently rounded number is a fact that has
quietly changed.

`source`, `source_version`, `parser` and `raw_ref` are dictionary-encoded alongside `scope` and
`key`, so a record decoded from the snapshot this produces carries the same provenance as the
record that was journaled, and compares equal to it.
"""
from __future__ import annotations

import struct
from decimal import Decimal, InvalidOperation
from typing import Any

from hub.errors import RecordError, StoreError
from hub.record import Availability, Record, sort_key
from hub.schema import DatasetSchema, FieldSpec, FieldType
from hubread.snapshot import MAGIC, NULL_DICT_INDEX, NULL_SENTINEL, SCALE, VERSION, field_type_code, sha256_of


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


def _pack_column(fmt: str, values: list[int]) -> bytes:
    """Pack `values` as one contiguous little-endian block -- a genuine column, not per-record
    interleaving. Empty is handled explicitly because `struct.pack("<0q")` with no arguments is
    well-defined but reads oddly next to the non-empty case."""
    if not values:
        return b""
    return struct.pack(f"<{len(values)}{fmt}", *values)


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
    sources = sorted({r.source for r in ordered})
    source_versions = sorted({r.source_version for r in ordered})
    parsers = sorted({r.parser for r in ordered})
    raw_refs = sorted({r.raw_ref for r in ordered if r.raw_ref is not None})

    scope_index = {v: i for i, v in enumerate(scopes)}
    key_index = {v: i for i, v in enumerate(keys)}
    source_index = {v: i for i, v in enumerate(sources)}
    source_version_index = {v: i for i, v in enumerate(source_versions)}
    parser_index = {v: i for i, v in enumerate(parsers)}
    raw_ref_index = {v: i for i, v in enumerate(raw_refs)}

    storable_fields = [f for f in schema.fields.values() if f.type != FieldType.STRING]
    schema_hash = bytes.fromhex(schema.hash().removeprefix("sha256:"))

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
    for dictionary in (scopes, keys, sources, source_versions, parsers, raw_refs):
        out += struct.pack("<i", len(dictionary))
        for value in dictionary:
            out += _pack_str(value)

    availability_ordinal = {a: i for i, a in enumerate(Availability)}

    out += _pack_column("q", [r.known_at for r in ordered])
    out += _pack_column("q", [r.effective_at for r in ordered])
    out += _pack_column("q", [r.period_start if r.period_start is not None else NULL_SENTINEL for r in ordered])
    out += _pack_column("q", [r.period_end if r.period_end is not None else NULL_SENTINEL for r in ordered])
    out += _pack_column("i", [scope_index[r.scope] for r in ordered])
    out += _pack_column("i", [key_index[r.key] for r in ordered])
    out += _pack_column("i", [r.revision for r in ordered])
    out += _pack_column("B", [availability_ordinal[r.availability] for r in ordered])
    out += _pack_column("q", [r.seq for r in ordered])
    out += _pack_column("i", [source_index[r.source] for r in ordered])
    out += _pack_column("i", [source_version_index[r.source_version] for r in ordered])
    out += _pack_column("i", [parser_index[r.parser] for r in ordered])
    out += _pack_column("i", [raw_ref_index[r.raw_ref] if r.raw_ref is not None else NULL_DICT_INDEX for r in ordered])
    for f in storable_fields:
        out += _pack_column("q", [_encode_field_value(f, r.fields.get(f.name)) for r in ordered])

    trailer = bytes.fromhex(sha256_of(bytes(out)))
    return bytes(out) + trailer
