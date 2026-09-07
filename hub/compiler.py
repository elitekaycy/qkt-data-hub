"""The compiler: turns a dataset's journal into a deterministic, self-verifying snapshot.

This is the one place the determinism contract (spec section 6) is exercised end to end.
Compiling the same journal lines in any order must yield byte-identical output, because a
snapshot's SHA-256 is what a backtest cites as its evidence, and a hash that depended on write
order or read order would make that citation meaningless. Two things make that hold here: the
journal read (`hubread.journal.read_range`) always returns records in `sort_key` order regardless
of on-disk line order, and `hub.snapshot.encode` sorts again before writing -- so neither this
module's own iteration order, nor a shuffled journal file, can change a byte of the result.

The other half of the job is re-deriving trust: every derived field this dataset declares is
recomputed here, against the same history a live pipeline would have seen, and checked against
what the journal actually recorded. A silent drift here -- a parser upgrade, a changed
expression, a hand-edited journal line -- is exactly the failure a backtest must never be allowed
to run on unnoticed, so a mismatch is a hard error, not a warning or a quarantine.
"""
from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

from hub.derive import compile_expr
from hub.errors import ConfigError, SchemaError, StoreError
from hub.manifest import DatasetManifest, FieldEntry, Manifest, WindowEntry
from hub.record import Record, sort_key
from hub.schema import DatasetSchema, load_schema
from hub.snapshot import encode
from hubread.journal import read_range
from hubread.record import MAX_TIMESTAMP_MS, MIN_TIMESTAMP_MS
from hubread.snapshot import sha256_of

#: A compile window is a calendar month, `YYYY-MM`. Yearly windows for slow datasets are a
#: documented extension point (spec 4.9) but no schema key selects one yet, so only the default
#: is implemented; a caller asking for anything else fails closed rather than guessing.
_WINDOW_RE = re.compile(r"^\d{4}-\d{2}$")


class DatasetRegistry(Protocol):
    """What `compile_all` needs from a registry: the dataset names it knows and their schemas.

    Deliberately a structural protocol rather than a concrete import -- the registry that loads
    `datasets/*.yaml` is a later task's module, and the compiler must not have to change when
    that module lands. Anything exposing these two methods (a real registry, or a test double
    that is just a `dict`-backed stand-in) satisfies this.
    """

    def datasets(self) -> Iterable[str]: ...

    def schema(self, name: str) -> DatasetSchema: ...


@dataclass(frozen=True, slots=True)
class WindowResult:
    """What one `compile_dataset` call produced: enough to both use the snapshot immediately and
    to record it in the manifest without re-reading the file."""

    path: Path
    sha256: str
    records: int
    from_day: str
    to_day: str
    raw_deps: tuple[str, ...] = ()


def _month_bounds(window: str) -> tuple[str, str, int, int]:
    """`from_day`, `to_day` (UTC calendar dates) and the inclusive `known_at` millisecond bounds
    a calendar-month window covers.

    The upper bound is computed as "one millisecond before the next month begins" rather than by
    constructing a 23:59:59.999 timestamp directly, so a month's length (28, 29, 30 or 31 days)
    is never hand-coded here and can never drift from what the calendar actually says.
    """
    if not _WINDOW_RE.match(window):
        raise ConfigError(f"window {window!r} must be a calendar month 'YYYY-MM'")
    year, month = (int(p) for p in window.split("-"))
    first = datetime(year, month, 1, tzinfo=UTC)
    next_first = datetime(year + 1, 1, 1, tzinfo=UTC) if month == 12 else datetime(year, month + 1, 1, tzinfo=UTC)
    known_from = int(first.timestamp() * 1000)
    known_to = int(next_first.timestamp() * 1000) - 1
    last_day = next_first - timedelta(milliseconds=1)
    return first.strftime("%Y-%m-%d"), last_day.strftime("%Y-%m-%d"), known_from, known_to


def _payload_with_envelope(record: Record) -> dict[str, Any]:
    """The mapping a compiled derived expression must see: declared fields plus the two envelope
    timestamps `since`/`until` read from the payload, not from the `Record` object itself.

    This merge is the one cross-task ruling that makes derived-field recomputation possible at
    all: `hub.derive` deliberately has no access to a `Record`, only to plain mappings, so
    whatever calls a compiled expression is responsible for building that mapping correctly.
    Skipping this merge does not raise -- it makes `since`/`until` return `None` forever, which
    is exactly the silent failure mode this module exists to catch instead of reproduce.
    """
    payload = dict(record.fields)
    payload["known_at"] = record.known_at
    payload["effective_at"] = record.effective_at
    return payload


def _verify_derived_fields(schema: DatasetSchema, records: Sequence[Record]) -> None:
    """Recomputes every derived field of `schema` for every record and raises on the first
    mismatch against what the journal actually recorded.

    History is scoped per `scope` and ordered by `known_at`, never mixed across scopes -- a
    z-score of a US surprise must never see EUR history, because the two series have nothing to
    do with each other and mixing them would make the statistic meaningless. Records are grouped
    into their scope with `records`' own relative order preserved from the caller (already
    `sort_key` order), so `known_at` order within a scope falls out for free.
    """
    derived_specs = schema.derived_fields()
    if not derived_specs:
        return
    exprs = {spec.name: compile_expr(spec.derived) for spec in derived_specs}

    by_scope: dict[str, list[Record]] = {}
    for record in records:
        by_scope.setdefault(record.scope, []).append(record)

    for scope_records in by_scope.values():
        history: list[Mapping[str, Any]] = []
        for record in scope_records:
            payload = _payload_with_envelope(record)
            for name, expr in exprs.items():
                expected = expr(payload, history)
                actual = record.fields.get(name)
                if expected != actual:
                    raise StoreError(
                        f"{schema.name}: key {record.key!r} field {name!r} recomputed as "
                        f"{expected!r} but the journal recorded {actual!r}"
                    )
            history.append(payload)


def compile_dataset(root: Path | str, schema: DatasetSchema, window: str) -> WindowResult:
    """Compiles one dataset's calendar-month `window` into a snapshot file, verifying every
    derived field along the way.

    History for derived-field recomputation is read from the start of the journal, not just from
    within `window`: a z-score or a lag computed at the start of a month legitimately depends on
    values from the month before, and limiting history to the window itself would report every
    such value as a mismatch. Verification therefore covers everything known up to the end of
    `window`, which also means compiling a later window re-checks all of history up to that
    point -- a second, free audit pass rather than a cost anyone has to ask for.
    """
    root = Path(root)
    from_day, to_day, known_from, known_to = _month_bounds(window)

    history = read_range(root, schema.name, MIN_TIMESTAMP_MS, known_to)
    _verify_derived_fields(schema, history)

    window_records = [r for r in history if known_from <= r.known_at <= known_to]
    window_records.sort(key=sort_key)

    data = encode(schema, window_records)
    digest = sha256_of(data)

    schema_hash_hex = schema.hash().removeprefix("sha256:")
    path = root / "snapshot" / schema.name / schema_hash_hex[:8] / f"{from_day}_{to_day}.qkh"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)

    raw_deps = tuple(sorted({r.raw_ref for r in window_records if r.raw_ref is not None}))
    return WindowResult(
        path=path, sha256=digest, records=len(window_records), from_day=from_day, to_day=to_day, raw_deps=raw_deps
    )


def _discover_windows(root: Path, dataset: str) -> list[str]:
    """Every calendar month that has at least one journal day-file for `dataset`, oldest first.

    Journal day files are named `YYYY-MM-DD.ndjson`, so the month is just the filename's first
    seven characters; a set-then-sort is enough because the names already sort chronologically.
    """
    directory = root / "journal" / dataset
    if not directory.is_dir():
        return []
    months = {p.stem[:7] for p in directory.glob("*.ndjson")}
    return sorted(months)


def _field_entries(schema: DatasetSchema) -> tuple[FieldEntry, ...]:
    """The manifest's compact copy of `schema`'s field table (spec 5.2), so a reader never has
    to parse the dataset's YAML to know what a compiled record's fields mean."""
    return tuple(
        FieldEntry(
            name=f.name,
            type=f.type.value,
            unit=f.unit,
            values=f.values,
            null_policy=f.null_policy,
            derived=f.derived,
            strategy=f.strategy,
        )
        for f in schema.fields.values()
    )


def compile_all(root: Path | str, registry: DatasetRegistry) -> Manifest:
    """Compiles every window that has journal data for every dataset `registry` knows, and
    writes the result as `manifest.json`.

    Idempotent: recompiling with an unchanged journal reproduces the same per-window hashes, so
    running this repeatedly (a scheduled job, a manual rebuild) never perturbs a hash a backtest
    has already cited.
    """
    root = Path(root)
    manifest = Manifest.load(root)

    for name in registry.datasets():
        schema = registry.schema(name)
        windows = _discover_windows(root, name)
        if not windows:
            continue

        entries = [
            WindowEntry(
                from_day=result.from_day,
                to_day=result.to_day,
                file=str(result.path.relative_to(root)),
                sha256=result.sha256,
                records=result.records,
                raw_deps=result.raw_deps,
            )
            for result in (compile_dataset(root, schema, window) for window in windows)
        ]

        all_records = read_range(root, name, MIN_TIMESTAMP_MS, MAX_TIMESTAMP_MS)
        last_seq = max((r.seq for r in all_records), default=0)
        last_known_at = max((r.known_at for r in all_records), default=None)

        manifest.datasets[name] = DatasetManifest(
            schema_hash=schema.hash(),
            schema_path=str(getattr(schema, "source_path", "") or f"datasets/{name}.yaml"),
            scope_kind=schema.scope_kind,
            key=schema.key,
            fields=_field_entries(schema),
            last_seq=last_seq,
            last_known_at=last_known_at,
            windows=entries,
        )

    manifest.save(root)
    return manifest


def verify(root: Path | str, registry: object | None = None) -> list[str]:
    """Recompiles every window the manifest at `root` knows about and reports every problem
    found, so a fleet health check can distinguish "clean" (empty list) from every way a compiled
    store can rot: a snapshot whose on-disk bytes no longer match its manifest hash (bit rot, a
    hand edit), a window that recompiles to something different (a parser or expression change
    silently rewriting history, or tampered journal data), or a snapshot file that is simply gone.

    Never raises for a problem it can attribute to one dataset or window -- a `StoreError` from a
    failed recompilation (for example, `_verify_derived_fields` catching a tampered value) is
    caught and reported as one more problem string, so one bad dataset does not stop the audit of
    every other one.
    """
    root = Path(root)
    manifest = Manifest.load(root)
    problems: list[str] = []

    for name, dataset_manifest in manifest.datasets.items():
        # Prefer a schema the caller already loaded. The recorded path is a fallback for an
        # audit run against a store whose dataset directory is not to hand, and it is resolved
        # as given before being resolved against the root, because datasets commonly live
        # outside the store they populate.
        schema = None
        if registry is not None:
            try:
                schema = registry.schema(name)  # type: ignore[attr-defined]
            except Exception:  # noqa: BLE001 - an unknown dataset just falls back to the path
                schema = None
        if schema is None:
            recorded = Path(dataset_manifest.schema_path)
            candidates = [recorded, root / recorded]
            for candidate in candidates:
                try:
                    schema = load_schema(candidate)
                    break
                except (SchemaError, OSError):
                    continue
            if schema is None:
                tried = " or ".join(str(c) for c in candidates)
                problems.append(f"{name}: cannot load schema at {tried}")
                continue

        for entry in dataset_manifest.windows:
            label = f"{name} window {entry.from_day}_{entry.to_day}"
            file_path = root / entry.file
            if not file_path.is_file():
                problems.append(f"{label}: missing snapshot file {file_path}")
                continue

            on_disk_hash = sha256_of(file_path.read_bytes())
            if on_disk_hash != entry.sha256:
                problems.append(
                    f"{label}: snapshot bytes ({on_disk_hash}) no longer match manifest hash ({entry.sha256})"
                )

            window = entry.from_day[:7]
            try:
                recompiled = compile_dataset(root, schema, window)
            except StoreError as e:
                problems.append(f"{label}: recompilation failed: {e}")
                continue
            if recompiled.sha256 != entry.sha256:
                problems.append(
                    f"{label}: recompiled hash ({recompiled.sha256}) differs from manifest hash ({entry.sha256})"
                )

    return problems
