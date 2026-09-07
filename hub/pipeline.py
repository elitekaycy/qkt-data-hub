"""Candidates in, journalled records out, with every guard the store's promises depend on.

The stages are deliberately separable and pure: normalise, build the identity key, validate
against the schema, derive, deduplicate, then stamp availability. Only the last step consults a
clock, and only the writer touches disk. That split is what lets the whole pipeline be re-run
over the raw archive to produce corrected revisions rather than edited history.

Three rules here are load-bearing and easy to lose in a refactor. Live collection always stamps
`observed` from the ingest clock, never a timestamp the provider suggested -- a source telling us
when we knew something is a source we have to trust about our own past. A record whose
`known_at` runs ahead of the writer's clock is quarantined rather than stored, because a
mis-clocked hub would otherwise publish facts from the future that every consumer would honour.
And a live candidate describing a fact older than its source's declared `max_observed_age_ms` is
quarantined too: a source with no incremental fetch mode (a CSV endpoint that always returns its
provider's whole history, say) would otherwise claim to have *just observed* a decades-old value
every time it is polled, fabricating `known_at` in the other temporal direction. Both stamp checks
also share a subtler failure this project has already hit: `derive`'s history for a scope is
walked in the order candidates are appended, and a live batch spanning years interleaves ahead of
or behind whatever the store already holds for that range, scrambling every `lag`/`diff`/`zscore`
computed from it. Rejecting the stale candidates before they reach `derive` is what keeps that
walk in the single direction it assumes.
"""
from __future__ import annotations

import bisect
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


class ScopeHistory:
    """One scope's facts, always produced in ascending `effective_at` order regardless of the
    order candidates are appended in.

    `derive`'s `lag`/`diff`/`zscore`/`pct_rank` read "N payloads back" positionally, from the end
    of whatever sequence they are handed -- they have no notion of a date, only of "everything
    before me, in order." That is exactly right for ticks that arrive one at a time, strictly in
    order, where "the end of history so far" and "immediately before the current fact" are the
    same thing. It stops being right the moment a batch is not strictly increasing in
    `effective_at`, and two things break that in practice: a live collector whose only fetch mode
    returns its provider's entire history hands `ingest` candidates spanning decades in one
    batch; and a corrected key -- exactly what "revision" means -- had, before this class
    existed, produced one journal-order slot per revision on every history rebuild
    (`_history_for`, and the compiler's own re-derivation), counting a single trading day twice.
    Both scramble a naive "just append" history the same way: a fact from 1962 lands after one
    from 2018 because it was *processed* after it, not because it happened after it.

    `before(effective_at, key)` is the fix -- it hands `derive` only the facts that genuinely
    precede this one on the fact's own timeline, however the batch happened to be ordered, and
    excludes this key's own prior revision rather than mistaking it for "the previous day". A
    key always has exactly one slot here, updated in place on revision, at the position its own
    `effective_at` puts it -- which a revision never moves, since a key's identity is built from
    the same period the schema declares, but a hypothetical schema where it could is handled
    the same way rather than assumed away.
    """

    def __init__(self) -> None:
        self._order: list[tuple[int, str]] = []  # (effective_at, key), ascending
        self._fields: dict[str, dict[str, Any]] = {}

    def upsert(self, effective_at: int, key: str, fields: dict[str, Any]) -> None:
        existing = self._position(key)
        if existing is not None:
            existing_effective_at, _ = self._order[existing]
            if existing_effective_at != effective_at:
                del self._order[existing]
                bisect.insort(self._order, (effective_at, key))
        else:
            bisect.insort(self._order, (effective_at, key))
        self._fields[key] = fields

    def before(self, effective_at: int, key: str) -> list[dict[str, Any]]:
        """Every fact that precedes `(effective_at, key)` on the timeline, oldest first.

        `bisect_left` on the same `(effective_at, key)` ordering `upsert` maintains finds the
        position this fact would occupy, which is correct for everything with a strictly
        earlier `effective_at`. It is not enough on its own to exclude a prior revision of this
        same key: a real source (two datasets here disagreed on the wall-clock hour a shared
        calendar day resolves to, one true one buggy) can compute a genuinely different
        `effective_at` for what is still the same fact, in which case its stale entry sorts on
        the wrong side of the cut and `derive` would see this fact as its own immediate
        predecessor -- an always-zero `diff`. The explicit filter is what makes this correct
        regardless of whether `effective_at` moved.
        """
        cut = bisect.bisect_left(self._order, (effective_at, key))
        return [self._fields[k] for _, k in self._order[:cut] if k != key]

    def payloads(self) -> list[dict[str, Any]]:
        """Every currently known fact for this scope, oldest `effective_at` first.

        For inspection only -- `derive` must never see this unfiltered, because it does not
        distinguish "before me" from "after me"; use `before` for that.
        """
        return [self._fields[key] for _, key in self._order]

    def _position(self, key: str) -> int | None:
        for i, (_, existing_key) in enumerate(self._order):
            if existing_key == key:
                return i
        return None


def ingest(
    candidates: Iterable[Candidate],
    schema: DatasetSchema,
    index: RevisionIndex,
    history: dict[str, ScopeHistory],
    now_ms: int,
    *,
    source: str,
    source_version: str = "",
    parser: str = "",
    raw_ref: str | None = None,
    quarantine: Quarantine | None = None,
    live: bool = True,
    skew_tolerance_ms: int = DEFAULT_SKEW_TOLERANCE_MS,
    max_observed_age_ms: int | None = None,
    clock: Callable[[], int] | None = None,
) -> IngestResult:
    """Run one batch of candidates through every stage and return what survived.

    `history` is keyed by scope so a z-score of a US surprise is never computed across EUR
    observations; the caller owns it across batches so derivations see the dataset's real past.
    `max_observed_age_ms` is the source's own `Source.max_observed_age_ms` (see that protocol
    member); it is ignored for a backfill (`live=False`), which exists specifically to submit
    old facts honestly, stamped `derived`.
    """
    result = IngestResult()
    wall = clock() if clock is not None else now_ms
    for candidate in candidates:
        try:
            known_at = candidate.known_at if candidate.known_at is not None else now_ms
            availability = Availability.OBSERVED if live else candidate.availability
            if live:
                known_at = now_ms
                if max_observed_age_ms is not None and wall - candidate.effective_at > max_observed_age_ms:
                    raise HubError(
                        f"effective_at {candidate.effective_at} is more than "
                        f"{max_observed_age_ms}ms old; this source cannot claim to have just "
                        "observed it live -- backfill it instead"
                    )
            if known_at > wall + skew_tolerance_ms:
                raise HubError(f"known_at {known_at} runs ahead of the writer clock {wall}")
            key = build_key(schema, candidate)
            envelope = {"known_at": known_at, "effective_at": candidate.effective_at}
            scope_history = history.setdefault(candidate.scope, ScopeHistory())
            payload = derive_fields(
                schema, candidate.fields, envelope, scope_history.before(candidate.effective_at, key)
            )
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
        scope_history.upsert(candidate.effective_at, key, dict(assigned.fields))
        result.records.append(assigned)
    return result
