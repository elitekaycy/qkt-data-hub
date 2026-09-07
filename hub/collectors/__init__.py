"""What a source is, and the one shape every provider is reduced to.

A collector does I/O and nothing else. It returns the bytes it got, or `None` when it could not
get them -- it never raises into the scheduler, because one provider being down must not stop
the others, and it never decides what a record means. Everything downstream of `fetch` is a pure
function of those bytes plus a versioned parser, which is what lets a wrong parse be corrected
later by re-running it over the archive.

The `Source` protocol is deliberately the same shape the sibling qkt-guardrails proved: fetch
returns `None` on failure so the caller keeps its last known good result rather than blanking.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from hub.rawstore import RawBlob
from hubread.record import Availability

__all__ = ["Candidate", "RawBlob", "Source"]


@dataclass(frozen=True, slots=True)
class Candidate:
    """A parsed row before it is a record: typed payload plus the envelope facts a parser knows.

    A parser can say what a fact describes and when it applies. It cannot say when we knew it --
    that is the ingest clock's job, or an explicit backfill's -- so `known_at` is optional here
    and filled in by the pipeline, which is what keeps a collector from inventing availability.
    """

    scope: str
    key: str
    effective_at: int
    fields: dict[str, Any]
    period_start: int | None = None
    period_end: int | None = None
    known_at: int | None = None
    availability: Availability = Availability.OBSERVED
    title: str = ""
    extra: dict[str, str] = field(default_factory=dict)


class Source(Protocol):
    """A provider of raw bytes and the transformer that turns them into candidates.

    `fetch` returns `None` on any failure and must not raise; `parse` must not do I/O, so it can
    be re-run over the raw archive to produce corrected revisions.
    """

    @property
    def name(self) -> str: ...

    @property
    def parser(self) -> str: ...

    def fetch(self, timeout_seconds: float) -> RawBlob | None: ...

    def parse(self, blob: RawBlob) -> list[Candidate]: ...
