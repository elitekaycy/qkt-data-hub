"""Candidates in, journalled records out, with every guard the store's promises depend on.

The stages are deliberately separable and pure: normalise, build the identity key, validate
against the schema, derive, deduplicate, then stamp availability. Only the last step consults a
clock, and only the writer touches disk. That split is what lets the whole pipeline be re-run
over the raw archive to produce corrected revisions rather than edited history.

Two rules here are load-bearing and easy to lose in a refactor. Live collection always stamps
`observed` from the ingest clock, never a timestamp the provider suggested -- a source telling us
when we knew something is a source we have to trust about our own past. And a record whose
`known_at` runs ahead of the writer's clock is quarantined rather than stored, because a
mis-clocked hub would otherwise publish facts from the future that every consumer would honour.
"""
from __future__ import annotations

import datetime as dt
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any

from hub.collectors import Candidate
from hub.dedupe import RevisionIndex, assign
from hub.derive import compile_expr
from hub.quarantine import Quarantine
from hub.schema import DatasetSchema
from hubread.errors import HubError
from hubread.record import Availability, Record

DEFAULT_SKEW_TOLERANCE_MS = 5_000


@dataclass(slots=True)
class IngestResult:
    """What one ingest pass produced, in the terms an operator alerts on."""

    records: list[Record] = field(default_factory=list)
    quarantined: int = 0
    duplicates: int = 0
    revisions: int = 0

    @property
    def written(self) -> int:
        return len(self.records)


def _iso_day(ms: int) -> str:
    return dt.datetime.fromtimestamp(ms / 1000, dt.UTC).strftime("%Y-%m-%d")


def build_key(schema: DatasetSchema, candidate: Candidate) -> str:
    """The identity of the thing, stable across every revision of it.

    Built only from what the schema names, and never from `known_at` -- an identity that moved
    when we learned something would make each correction a new fact instead of a new revision,
    which is the whole mechanism this store relies on.
    """
    parts: list[str] = []
    for name in schema.key:
        if name == "scope":
            parts.append(candidate.scope)
        elif name == "effective_at":
            parts.append(_iso_day(candidate.effective_at) + "T" + str(candidate.effective_at))
        elif name == "period_start":
            parts.append(_iso_day(candidate.period_start) if candidate.period_start is not None else "-")
        elif name == "period_end":
            parts.append(_iso_day(candidate.period_end) if candidate.period_end is not None else "-")
        elif name == "title":
            parts.append(candidate.title or str(candidate.fields.get("title", "")))
        else:
            parts.append("" if candidate.fields.get(name) is None else str(candidate.fields[name]))
    return "|".join(parts)


def derive_fields(
    schema: DatasetSchema,
    payload: dict[str, Any],
    envelope: dict[str, Any],
    history: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    """Compute every declared derived field over the payload and this scope's prior payloads.

    The envelope timestamps are merged into the payload the expression sees, because `since`
    and `until` read `known_at` from that mapping while the record keeps it on the envelope.
    Without the merge those functions would silently return nothing forever.
    """
    merged = {**payload, **envelope}
    out = dict(payload)
    # `derived_fields()` comes back in dependency order, so feeding each result straight back
    # into the mapping the next expression sees is what lets a z-score be taken over a surprise
    # that this same pass just computed.
    for spec in schema.derived_fields():
        value = compile_expr(spec.derived)(merged, history)
        out[spec.name] = value
        merged[spec.name] = value
    return out


def ingest(
    candidates: Iterable[Candidate],
    schema: DatasetSchema,
    index: RevisionIndex,
    history: dict[str, list[dict[str, Any]]],
    now_ms: int,
    *,
    source: str,
    source_version: str = "",
    parser: str = "",
    raw_ref: str | None = None,
    quarantine: Quarantine | None = None,
    live: bool = True,
    skew_tolerance_ms: int = DEFAULT_SKEW_TOLERANCE_MS,
    clock: Callable[[], int] | None = None,
) -> IngestResult:
    """Run one batch of candidates through every stage and return what survived.

    `history` is keyed by scope so a z-score of a US surprise is never computed across EUR
    observations; the caller owns it across batches so derivations see the dataset's real past.
    """
    result = IngestResult()
    wall = clock() if clock is not None else now_ms
    for candidate in candidates:
        try:
            known_at = candidate.known_at if candidate.known_at is not None else now_ms
            availability = Availability.OBSERVED if live else candidate.availability
            if live:
                known_at = now_ms
            if known_at > wall + skew_tolerance_ms:
                raise HubError(f"known_at {known_at} runs ahead of the writer clock {wall}")
            key = build_key(schema, candidate)
            envelope = {"known_at": known_at, "effective_at": candidate.effective_at}
            scope_history = history.setdefault(candidate.scope, [])
            payload = derive_fields(schema, candidate.fields, envelope, scope_history)
            payload = schema.validate_payload(payload)
            record = Record.create(
                dataset=schema.name,
                scope=candidate.scope,
                key=key,
                revision=1,
                known_at=known_at,
                effective_at=candidate.effective_at,
                period_start=candidate.period_start,
                period_end=candidate.period_end,
                availability=availability,
                source=source,
                source_version=source_version,
                parser=parser,
                raw_ref=raw_ref,
                fields=payload,
            )
        except (HubError, ValueError, ArithmeticError) as e:
            result.quarantined += 1
            if quarantine is not None:
                quarantine.write(schema.name, str(e), candidate.fields, now_ms, raw_ref)
            continue
        assigned = assign(index, record)
        if assigned is None:
            result.duplicates += 1
            continue
        if assigned.revision > 1:
            result.revisions += 1
        history.setdefault(candidate.scope, []).append(dict(assigned.fields))
        result.records.append(assigned)
    return result
