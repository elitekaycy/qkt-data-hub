"""Scheduled releases from a published calendar, one upcoming event at a time.

A release calendar (FOMC statements, CPI, the employment report) is published months ahead, so a
strategy may legitimately know the next release time. What it may not do is see a fact before the
store could have recorded it: a consumer reads the latest record of a stream, and qkt delivers a
live record when it is appended, so a year of future events journaled at once would be invisible
to a running session and visible to a backtest. The store therefore holds exactly one record per
event, written when that event becomes the next one: `PUBLISH_LAG_MS` after the previous event.

Live collection and backfill share `published_at`, the single rule for when an event's record
first appears, so a backtest over backfilled history and a live session see the same `at` field
change at the same instants (live may run up to one poll later, never earlier).

The calendar itself is a CSV next to the dataset files, maintained by hand from the official
pages and reviewed like code. A correction is a new revision, written when the file changes.
"""
from __future__ import annotations

import csv
import datetime as dt
import io
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from hub.collectors import Candidate
from hub.collectors.declarative import Cadence
from hub.logging import log
from hub.rawstore import RawBlob
from hubread.errors import ConfigError
from hubread.record import Availability

#: Ordinals of the `kind` enum, in the order every calendar dataset declares them.
KINDS: tuple[str, ...] = ("fomc", "cpi", "nfp")

#: How long after an event its successor is published. Backfill uses this exact lag; live
#: collection never publishes earlier than it, so a live session can only see the next event
#: at the same instant as a backtest or a few seconds after it.
PUBLISH_LAG_MS = 60_000

_OPTION_KEYS = frozenset({"schedule", "kinds", "scope"})


@dataclass(frozen=True, slots=True)
class Event:
    kind: str
    at: int  # epoch ms UTC


def load_events(text: str, kinds: Sequence[str]) -> list[Event]:
    """Parse the calendar CSV (`kind,date,time,zone`), keep `kinds`, and sort by time.

    A naive time is meaningless for a release calendar, so every row names its zone; an unknown
    kind, a malformed date or a duplicate instant is refused rather than skipped.
    """
    out: list[Event] = []
    rows = csv.DictReader(line for line in io.StringIO(text) if line.strip() and not line.startswith("#"))
    for n, row in enumerate(rows, start=2):
        kind = (row.get("kind") or "").strip()
        if kind not in KINDS:
            raise ConfigError(f"schedule line {n}: unknown kind {kind!r}")
        if kind not in kinds:
            continue
        try:
            day = dt.date.fromisoformat((row.get("date") or "").strip())
            clock = dt.time.fromisoformat((row.get("time") or "").strip())
            zone = ZoneInfo((row.get("zone") or "").strip())
        except (ValueError, KeyError) as e:
            raise ConfigError(f"schedule line {n}: {e}") from e
        at = int(dt.datetime.combine(day, clock, tzinfo=zone).timestamp() * 1000)
        out.append(Event(kind, at))
    out.sort(key=lambda e: e.at)
    for a, b in zip(out, out[1:], strict=False):
        if a.at == b.at:
            raise ConfigError(f"schedule has two events at {a.at} ({a.kind}, {b.kind})")
    return out


def published_at(events: Sequence[Event], index: int, history_start_ms: int) -> int:
    """When the record for `events[index]` first appears: the previous event plus the lag."""
    if index == 0:
        return history_start_ms
    return events[index - 1].at + PUBLISH_LAG_MS


def next_event(events: Sequence[Event], now_ms: int) -> Event | None:
    """The event a consumer sees as upcoming at `now_ms`: each event stays current until its own
    `at + PUBLISH_LAG_MS`, the instant `published_at` hands over to its successor."""
    for event in events:
        if now_ms < event.at + PUBLISH_LAG_MS:
            return event
    return None


def candidate_for(event: Event, scope: str) -> Candidate:
    return Candidate(
        scope=scope,
        key="",
        effective_at=event.at,
        fields={"at": event.at, "kind": KINDS.index(event.kind)},
        period_start=event.at,
        known_at=None,
        availability=Availability.OBSERVED,
        title=event.kind,
    )


class ScheduleSource:
    """Live half: reads the calendar file and offers the one event that is next right now."""

    def __init__(
        self,
        dataset: str,
        schedule: Path,
        kinds: Sequence[str],
        scope: str = "USD",
        *,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._dataset = dataset
        self._schedule = schedule
        self._kinds = tuple(kinds)
        self._scope = scope
        self._clock = clock
        # Poll every few seconds around each release so the successor appears within seconds
        # of `PUBLISH_LAG_MS`, and once a minute otherwise.
        self.cadence = Cadence(
            steady_seconds=60.0, retry_seconds=60.0, near_after_seconds=180.0, near_every_seconds=5.0
        )

    @classmethod
    def from_config(cls, dataset: str, options: dict[str, Any], datasets_dir: Path) -> ScheduleSource:
        unknown = set(options) - _OPTION_KEYS
        if unknown:
            raise ConfigError(f"{dataset}.collector: unknown key(s) {sorted(unknown)!r}")
        if "schedule" not in options or "kinds" not in options:
            raise ConfigError(f"{dataset}.collector: a schedule collector needs 'schedule' and 'kinds'")
        kinds = options["kinds"]
        if not isinstance(kinds, list) or not kinds or any(k not in KINDS for k in kinds):
            raise ConfigError(f"{dataset}.collector: kinds must be a non-empty list of {list(KINDS)!r}")
        path = (datasets_dir / str(options["schedule"])).resolve()
        if not path.is_file():
            raise ConfigError(f"{dataset}.collector: schedule file not found: {path}")
        # Parse once at load so a broken calendar fails the daemon at start, not at 08:29.
        load_events(path.read_text(encoding="utf-8"), kinds)
        return cls(dataset, path, kinds, str(options.get("scope", "USD")))

    @property
    def name(self) -> str:
        return self._dataset

    @property
    def parser(self) -> str:
        return "schedule@1"

    @property
    def max_observed_age_ms(self) -> int | None:
        return None

    def fetch(self, timeout_seconds: float) -> RawBlob | None:
        try:
            body = self._schedule.read_bytes()
        except OSError as e:
            log(f"collect[{self._dataset}] cannot read schedule: {e}")
            return None
        return RawBlob(
            body=body,
            content_type="text/csv",
            fetched_at=int(self._clock() * 1000),
            request={"path": str(self._schedule)},
        )

    def parse(self, blob: RawBlob) -> list[Candidate]:
        events = load_events(blob.body.decode("utf-8"), self._kinds)
        event = next_event(events, blob.fetched_at)
        return [] if event is None else [candidate_for(event, self._scope)]
