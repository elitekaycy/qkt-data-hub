"""The record: the atom every consumer reads, and the invariants that make it trustworthy.

A record has a frozen envelope owned by this module and a payload owned by the dataset's
schema. Only one envelope field decides visibility -- `known_at` -- and the whole point of the
product is that it is recorded rather than guessed. `effective_at` says when the fact applies,
`period_*` what it describes; keeping the three apart is what lets a consumer legitimately see
a schedule published in advance without also seeing the outcome.

Numbers travel as decimal strings. A float would make `0.1 + 0.2` a different fact on a
different machine, and this module is the boundary where that is prevented once for everyone.

This lives in the reader package, not the hub, because the format is what a consumer must
agree with. A writer that defined its own copy could drift from it silently.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Any

from hubread.errors import RecordError

ENVELOPE_VERSION = 1

# Bounds a plausible epoch-millisecond timestamp: 2000-01-01 to 2100-01-01. A value outside
# this is a unit mistake (seconds, microseconds) rather than a real time, and silently
# accepting it would put records in the wrong century of the journal.
MIN_TIMESTAMP_MS = 946_684_800_000
MAX_TIMESTAMP_MS = 4_102_444_800_000

DATASET_RE = re.compile(r"^[a-z][a-z0-9_]*(\.[a-z0-9_]+){1,4}$")
SCOPE_RE = re.compile(r"^[A-Za-z0-9_.:+-]{1,64}$")
FIELD_NAME_RE = re.compile(r"^[a-z][a-z0-9_]*$")

DOMAINS = frozenset({"macro", "cal", "rates", "cb", "pos", "inv", "corp", "venue", "mkt", "text", "hub"})

RESERVED_FIELD_NAMES = frozenset({"known_at", "effective_at", "revision", "value"})


class Availability(StrEnum):
    """How `known_at` was obtained. A reader may refuse the weakest class outright."""

    OBSERVED = "observed"
    """We recorded it from our own ingest clock at first observation. Highest trust."""
    PUBLISHED = "published"
    """The source supplied a publication or acceptance timestamp we validated."""
    DERIVED = "derived"
    """Estimated from a schedule or fixed lag during backfill. Lowest trust."""


def canonical_json(payload: Any) -> str:
    """The one serialization used for hashing, so an id is stable across processes and hosts."""
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def decimal_str(value: Decimal | int | str) -> str:
    """Canonical decimal text: no exponent, no leading `+`, no `-0`, no trailing-zero drift.

    Two collectors that read `0.30` and `.3` must produce the same bytes, or the deduplicator
    sees a revision where nothing changed and the journal grows a lie.
    """
    try:
        d = Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as e:
        raise RecordError(f"not a decimal: {value!r}") from e
    if not d.is_finite():
        raise RecordError(f"not a finite decimal: {value!r}")
    if d == 0:
        return "0"
    text = format(d.normalize(), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _check_timestamp(name: str, value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise RecordError(f"{name} must be an integer epoch-ms, got {value!r}")
    if not MIN_TIMESTAMP_MS <= value <= MAX_TIMESTAMP_MS:
        raise RecordError(f"{name}={value} is outside the plausible epoch-ms range")
    return value


@dataclass(frozen=True, slots=True)
class Record:
    """One fact, at one revision, as it was knowable at `known_at`.

    Construct through `Record.create` (which stamps the content id) or `Record.from_json`. The
    constructor validates the envelope but not the payload against a schema -- that is
    `hub.schema.DatasetSchema.validate_payload`, applied by the pipeline before journaling.
    """

    dataset: str
    scope: str
    key: str
    revision: int
    known_at: int
    effective_at: int
    availability: Availability
    source: str
    fields: dict[str, Any] = field(default_factory=dict)
    period_start: int | None = None
    period_end: int | None = None
    source_version: str = ""
    parser: str = ""
    raw_ref: str | None = None
    seq: int = 0
    id: str = ""
    v: int = ENVELOPE_VERSION

    def __post_init__(self) -> None:
        if self.v != ENVELOPE_VERSION:
            raise RecordError(f"unsupported envelope version {self.v}")
        if not DATASET_RE.match(self.dataset):
            raise RecordError(f"dataset {self.dataset!r} is not a dotted lowercase name")
        if self.dataset.split(".", 1)[0] not in DOMAINS:
            raise RecordError(f"dataset {self.dataset!r} uses an unregistered domain")
        if not SCOPE_RE.match(self.scope):
            raise RecordError(f"scope {self.scope!r} is not a valid scope token")
        if not self.key:
            raise RecordError("key must not be empty")
        if isinstance(self.revision, bool) or not isinstance(self.revision, int) or self.revision < 1:
            raise RecordError(f"revision must be a positive integer, got {self.revision!r}")
        _check_timestamp("known_at", self.known_at)
        _check_timestamp("effective_at", self.effective_at)
        if self.period_start is not None:
            _check_timestamp("period_start", self.period_start)
        if self.period_end is not None:
            _check_timestamp("period_end", self.period_end)
        if self.period_start is not None and self.period_end is not None and self.period_end < self.period_start:
            raise RecordError("period_end precedes period_start")
        if not isinstance(self.availability, Availability):
            raise RecordError(f"availability must be an Availability, got {self.availability!r}")
        if not self.source:
            raise RecordError("source must not be empty")
        if isinstance(self.seq, bool) or not isinstance(self.seq, int) or self.seq < 0:
            raise RecordError(f"seq must be a non-negative integer, got {self.seq!r}")
        for name, value in self.fields.items():
            if not FIELD_NAME_RE.match(name):
                raise RecordError(f"field name {name!r} is not lowercase snake_case")
            if name in RESERVED_FIELD_NAMES:
                raise RecordError(f"field name {name!r} collides with an envelope name")
            if not (value is None or isinstance(value, (str, bool, int))):
                raise RecordError(
                    f"field {name} must be a decimal string, bool, int or null -- got {type(value).__name__}"
                )

    # -- identity ------------------------------------------------------------------------

    def identity_payload(self) -> dict[str, Any]:
        """Everything that defines this record, excluding `seq` and `id`.

        `seq` is a per-writer counter, so including it would make the same fact hash
        differently on a rebuild. Excluding it makes `id` a true content address and lets a
        consumer de-duplicate across a restart.
        """
        payload: dict[str, Any] = {
            "v": self.v,
            "dataset": self.dataset,
            "scope": self.scope,
            "key": self.key,
            "revision": self.revision,
            "known_at": self.known_at,
            "effective_at": self.effective_at,
            "availability": str(self.availability),
            "source": self.source,
            "source_version": self.source_version,
            "parser": self.parser,
            "fields": self.fields,
        }
        if self.period_start is not None:
            payload["period_start"] = self.period_start
        if self.period_end is not None:
            payload["period_end"] = self.period_end
        if self.raw_ref is not None:
            payload["raw_ref"] = self.raw_ref
        return payload

    def compute_id(self) -> str:
        return "sha256:" + hashlib.sha256(canonical_json(self.identity_payload()).encode("utf-8")).hexdigest()

    def payload_hash(self) -> str:
        """Hash of the payload alone -- what the deduplicator compares to spot a real change."""
        return hashlib.sha256(canonical_json(self.fields).encode("utf-8")).hexdigest()

    # -- serialization -------------------------------------------------------------------

    def to_json(self) -> str:
        payload = self.identity_payload()
        payload["seq"] = self.seq
        payload["id"] = self.id or self.compute_id()
        return canonical_json(payload)

    @classmethod
    def create(
        cls,
        *,
        dataset: str,
        scope: str,
        key: str,
        revision: int,
        known_at: int,
        effective_at: int,
        availability: Availability,
        source: str,
        fields: dict[str, Any] | None = None,
        period_start: int | None = None,
        period_end: int | None = None,
        source_version: str = "",
        parser: str = "",
        raw_ref: str | None = None,
        seq: int = 0,
    ) -> Record:
        """Build a record and stamp its content-addressed id."""
        return cls(
            dataset=dataset,
            scope=scope,
            key=key,
            revision=revision,
            known_at=known_at,
            effective_at=effective_at,
            availability=availability,
            source=source,
            fields=dict(fields or {}),
            period_start=period_start,
            period_end=period_end,
            source_version=source_version,
            parser=parser,
            raw_ref=raw_ref,
            seq=seq,
        ).with_id()

    def with_id(self) -> Record:
        return self.replace(id=self.compute_id())

    def replace(self, **changes: Any) -> Record:
        current: dict[str, Any] = {
            "dataset": self.dataset,
            "scope": self.scope,
            "key": self.key,
            "revision": self.revision,
            "known_at": self.known_at,
            "effective_at": self.effective_at,
            "availability": self.availability,
            "source": self.source,
            "fields": dict(self.fields),
            "period_start": self.period_start,
            "period_end": self.period_end,
            "source_version": self.source_version,
            "parser": self.parser,
            "raw_ref": self.raw_ref,
            "seq": self.seq,
            "id": self.id,
            "v": self.v,
        }
        current.update(changes)
        return Record(**current)

    @classmethod
    def from_json(cls, text: str) -> Record:
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as e:
            raise RecordError(f"record is not valid JSON: {e}") from e
        return cls.from_dict(payload)

    @classmethod
    def from_dict(cls, payload: Any) -> Record:
        if not isinstance(payload, dict):
            raise RecordError(f"record must be a JSON object, got {type(payload).__name__}")
        required = {"dataset", "scope", "key", "revision", "known_at", "effective_at", "availability", "source"}
        if missing := required - set(payload):
            raise RecordError(f"record is missing envelope field(s): {', '.join(sorted(missing))}")
        raw_availability = payload["availability"]
        try:
            availability = Availability(raw_availability)
        except ValueError as e:
            raise RecordError(f"unknown availability {raw_availability!r}") from e
        fields = payload.get("fields", {})
        if not isinstance(fields, dict):
            raise RecordError("fields must be a JSON object")
        record = cls(
            dataset=payload["dataset"],
            scope=payload["scope"],
            key=payload["key"],
            revision=payload["revision"],
            known_at=payload["known_at"],
            effective_at=payload["effective_at"],
            availability=availability,
            source=payload["source"],
            fields=fields,
            period_start=payload.get("period_start"),
            period_end=payload.get("period_end"),
            source_version=payload.get("source_version", ""),
            parser=payload.get("parser", ""),
            raw_ref=payload.get("raw_ref"),
            seq=payload.get("seq", 0),
            id=payload.get("id", ""),
            v=payload.get("v", ENVELOPE_VERSION),
        )
        expected = record.compute_id()
        if record.id and record.id != expected:
            raise RecordError(f"record id {record.id} does not match its content ({expected})")
        return record if record.id else record.with_id()


def sort_key(record: Record) -> tuple[int, str, str, int, int]:
    """The one ordering used by the compiler, the reader and every test.

    Ordering by `known_at` first is what makes a snapshot answerable with a binary search for
    "everything visible at T"; the rest of the tuple only breaks ties, deterministically.
    """
    return (record.known_at, record.scope, record.key, record.revision, record.seq)
