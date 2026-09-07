"""The point-in-time reader: the one property every consumer of this store depends on.

`as_of(dataset, scope, t)` must never return a fact that was not yet knowable at `t` -- that is
the whole reason this product exists, and every other method here exists to serve it or the
transports built on top of it (spec section 8.1). This module is transport-independent: a local
tail, a stream server, and a backtest replay are all just different ways of calling the same
methods against the same bytes.

Standalone by construction: this module imports nothing outside `hubread` and the standard
library. A kill-switch daemon or a trading engine embeds this file (and its package) alone and
can read a hub root -- live journal or compiled snapshot -- without ever depending on the writer.

`.schema(name)` reads the dataset's field table out of the manifest rather than parsing the
dataset's YAML: the manifest's `fields` array (written by `hub.compiler.compile_all`) is a
complete, already-typed copy of what a reader needs, so this module carries no YAML parser and
never touches `hub.schema` -- a consumer needs nothing but this package and the manifest.
"""
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from hubread.errors import StoreError
from hubread.journal import JournalTail, read_range
from hubread.policy import Policy
from hubread.record import MIN_TIMESTAMP_MS, Availability, Record, sort_key
from hubread.snapshot import decode, sha256_of

MANIFEST_FILENAME = "manifest.json"
HEARTBEAT_FILENAME = "heartbeat"

#: `Policy` is frozen and immutable, so one shared instance is safe as the default for every
#: `Reader` that does not supply its own -- module-level rather than a call in the signature so
#: linting cannot mistake it for a mutable-default hazard.
_DEFAULT_POLICY = Policy()


@dataclass(frozen=True, slots=True)
class SchemaField:
    """One field of a dataset, as recorded in the manifest -- enough to interpret a value
    without ever having parsed the schema YAML that declared it."""

    name: str
    type: str
    unit: str
    values: tuple[str, ...]
    null_policy: str
    derived: str
    strategy: bool


@dataclass(frozen=True, slots=True)
class Schema:
    """A dataset's shape, as a reader needs it: identity, key, and field table.

    Built entirely from the manifest (see module docstring) -- there is deliberately no method
    here that recomputes a derived field or validates a payload; those are write-side concerns
    that stay in `hub.schema` and `hub.derive`. A reader only ever interprets values the hub has
    already computed and verified.
    """

    name: str
    schema_hash: str
    scope_kind: str
    key: tuple[str, ...]
    fields: tuple[SchemaField, ...]


@dataclass(frozen=True, slots=True)
class Health:
    """The hub root's liveness, as of the moment `Reader.health()` was called.

    `stale` is a flag for the caller to act on, never a reason for this reader to withhold or
    blank data on its own -- a stale heartbeat means "the writer may be behind", not "the facts
    already on disk stopped being true".
    """

    heartbeat_at: int | None
    age_ms: int | None
    stale: bool


@dataclass(frozen=True, slots=True)
class _WindowInfo:
    """One manifest window, with its calendar bounds pre-converted to `known_at` millisecond
    bounds so `Reader.range` can test overlap without re-parsing dates on every call."""

    from_day: str
    to_day: str
    file: str
    sha256: str
    from_ms: int
    to_ms: int


@dataclass(frozen=True, slots=True)
class _DatasetInfo:
    """The manifest's whole entry for one dataset, kept as-parsed for `Reader` to serve from."""

    schema_hash: str
    scope_kind: str
    key: tuple[str, ...]
    fields: tuple[SchemaField, ...]
    last_seq: int
    last_known_at: int | None
    windows: tuple[_WindowInfo, ...]


def _day_bounds_ms(from_day: str, to_day: str) -> tuple[int, int]:
    """The inclusive `known_at` millisecond span `[from_day 00:00:00.000, to_day 23:59:59.999]`
    UTC that a manifest window's calendar dates cover."""
    start = datetime.strptime(from_day, "%Y-%m-%d").replace(tzinfo=UTC)
    end = datetime.strptime(to_day, "%Y-%m-%d").replace(tzinfo=UTC) + timedelta(days=1) - timedelta(milliseconds=1)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)


def _parse_manifest(raw: dict[str, Any]) -> dict[str, _DatasetInfo]:
    out: dict[str, _DatasetInfo] = {}
    for name, entry in raw.get("datasets", {}).items():
        fields = tuple(
            SchemaField(
                name=f["name"],
                type=f["type"],
                unit=f["unit"],
                values=tuple(f.get("values", [])),
                null_policy=f["null_policy"],
                derived=f.get("derived", ""),
                strategy=f["strategy"],
            )
            for f in entry.get("fields", [])
        )
        windows = []
        for w in entry.get("windows", []):
            from_ms, to_ms = _day_bounds_ms(w["from"], w["to"])
            windows.append(
                _WindowInfo(from_day=w["from"], to_day=w["to"], file=w["file"], sha256=w["sha256"],
                             from_ms=from_ms, to_ms=to_ms)
            )
        out[name] = _DatasetInfo(
            schema_hash=entry["schema_hash"],
            scope_kind=entry["scope_kind"],
            key=tuple(entry.get("key", [])),
            fields=fields,
            last_seq=entry.get("last_seq", 0),
            last_known_at=entry.get("last_known_at"),
            windows=tuple(sorted(windows, key=lambda w: w.from_ms)),
        )
    return out


class Reader:
    """Reads a hub root -- journal, snapshot, and manifest -- under one consistent policy.

    Every visibility decision in this class reduces to one comparison: a record's *effective
    visibility time*, `known_at + policy.min_lag_ms`, against the caller's query time `t`. That
    single rule is what makes `as_of` incapable of returning a record from the future, no matter
    which dataset, scope, or transport is asking.
    """

    def __init__(self, root: Path | str, policy: Policy = _DEFAULT_POLICY) -> None:
        self.root = Path(root)
        self.policy = policy
        self._manifest: dict[str, _DatasetInfo] = self._load_manifest()

    def _load_manifest(self) -> dict[str, _DatasetInfo]:
        path = self.root / MANIFEST_FILENAME
        if not path.is_file():
            return {}
        raw = json.loads(path.read_text(encoding="utf-8"))
        return _parse_manifest(raw)

    # -- manifest view -----------------------------------------------------------------------

    def datasets(self) -> list[str]:
        """Every dataset the manifest has compiled at least one window for, sorted by name."""
        return sorted(self._manifest)

    def schema(self, name: str) -> Schema:
        """The dataset's field table, read from the manifest alone -- see the module docstring
        for why this never parses the dataset's YAML schema."""
        info = self._manifest.get(name)
        if info is None:
            raise StoreError(f"unknown dataset {name!r}: not present in the manifest at {self.root}")
        return Schema(
            name=name, schema_hash=info.schema_hash, scope_kind=info.scope_kind, key=info.key, fields=info.fields
        )

    # -- live tail -----------------------------------------------------------------------------

    def records(self, dataset: str, from_seq: int = 0) -> list[Record]:
        """New records appended to `dataset`'s journal since `from_seq`, in `seq` order.

        A single poll rather than a blocking stream: the caller (an event loop, a test, a
        one-shot replay) decides when to ask again. `JournalTail` already guarantees no record
        with `seq <= from_seq` is returned twice.
        """
        return JournalTail(self.root, dataset, from_seq=from_seq).poll()

    # -- historical range ------------------------------------------------------------------------

    def range(self, dataset: str, known_from: int, known_to: int) -> list[Record]:
        """Every record of `dataset` with `known_from <= known_at <= known_to`, in `sort_key`
        order, reading compiled snapshots where the manifest has them and the live journal for
        everything else.

        A snapshot is only ever trusted after its bytes are re-hashed and compared against the
        manifest's recorded SHA-256 (`_decode_window`); a mismatch fails closed rather than
        silently serving a corrupted or superseded file.
        """
        info = self._manifest.get(dataset)
        if info is None or not info.windows:
            return sorted(read_range(self.root, dataset, known_from, known_to), key=sort_key)

        records: list[Record] = []
        cursor = known_from
        for window in info.windows:
            if window.to_ms < known_from or window.from_ms > known_to:
                continue
            if cursor < window.from_ms:
                records.extend(read_range(self.root, dataset, cursor, window.from_ms - 1))
            records.extend(
                r for r in self._decode_window(window) if known_from <= r.known_at <= known_to
            )
            cursor = max(cursor, window.to_ms + 1)
            if cursor > known_to:
                break
        if cursor <= known_to:
            records.extend(read_range(self.root, dataset, cursor, known_to))

        records.sort(key=sort_key)
        return records

    def _decode_window(self, window: _WindowInfo) -> list[Record]:
        path = self.root / window.file
        data = path.read_bytes()
        actual_hash = sha256_of(data)
        if actual_hash != window.sha256:
            raise StoreError(
                f"snapshot {path} hash {actual_hash} does not match manifest hash {window.sha256}"
            )
        _, records = decode(data)
        return records

    # -- point in time -----------------------------------------------------------------------

    def _visible_records(self, dataset: str, scope: str, t: int) -> list[Record]:
        """Every record of `dataset`/`scope` whose effective visibility time -- `known_at` plus
        `policy.min_lag_ms` -- is at or before `t`, minus any `derived` record when
        `policy.refuse_derived` is set. `known_at <= t` always holds here because `min_lag_ms`
        is never negative, which is what makes `as_of` safe to bound its journal read by `t`.
        """
        candidates = read_range(self.root, dataset, MIN_TIMESTAMP_MS, t)
        out = []
        for record in candidates:
            if record.scope != scope:
                continue
            if self.policy.refuse_derived and record.availability == Availability.DERIVED:
                continue
            if record.known_at + self.policy.min_lag_ms > t:
                continue
            out.append(record)
        return out

    def as_of(self, dataset: str, scope: str, t: int) -> dict[str, Record]:
        """The latest fact known, per `key`, as of `t`: for each key, the highest revision whose
        effective visibility time is at or before `t`.

        This is the property the whole product rests on -- a record with `known_at > t` (after
        `min_lag_ms`) can never appear in the result, for any dataset, scope or policy.
        """
        latest: dict[str, Record] = {}
        for record in self._visible_records(dataset, scope, t):
            current = latest.get(record.key)
            if current is None or (record.revision, record.known_at, record.seq) > (
                current.revision,
                current.known_at,
                current.seq,
            ):
                latest[record.key] = record
        return latest

    def windows(self, dataset: str, scope: str, t: int, ahead_ms: int, pad_ms: int) -> list[tuple[int, int]]:
        """`(start, end)` UTC-ms intervals, padded by `pad_ms`, around every event visible at `t`
        whose `effective_at` falls within `ahead_ms` ahead of `t`.

        This is what a safety daemon consults to decide "is a release imminent": each interval is
        a scheduled fact's `effective_at`, not `known_at` -- the schedule may have been known for
        days, but the window that matters for risk is centred on when it takes effect.
        """
        latest = self.as_of(dataset, scope, t)
        intervals = [
            (record.effective_at - pad_ms, record.effective_at + pad_ms)
            for record in latest.values()
            if t <= record.effective_at <= t + ahead_ms
        ]
        return sorted(intervals)

    # -- health --------------------------------------------------------------------------------

    def health(self) -> Health:
        """The hub root's heartbeat freshness against `policy.stale_after_ms`.

        A missing heartbeat file is reported stale with no age, rather than raising -- a reader
        opened against a hub root that has never started a writer is a legitimate state (an
        empty backtest fixture, a not-yet-provisioned root), and the caller decides what to do
        with a stale or absent heartbeat, not this method.
        """
        path = self.root / HEARTBEAT_FILENAME
        try:
            mtime_ms = int(os.stat(path).st_mtime * 1000)
        except OSError:
            return Health(heartbeat_at=None, age_ms=None, stale=True)
        age_ms = int(time.time() * 1000) - mtime_ms
        return Health(heartbeat_at=mtime_ms, age_ms=age_ms, stale=age_ms > self.policy.stale_after_ms)
