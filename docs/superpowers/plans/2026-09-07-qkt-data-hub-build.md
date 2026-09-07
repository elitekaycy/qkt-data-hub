# qkt-data-hub Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the qkt-data-hub product end to end — a point-in-time fact store that acquires non-price data, writes it as one append-only record format, compiles deterministic snapshots, serves live tails and history from the same bytes, and is consumed by qkt for backtest and live with no look-ahead.

**Architecture:** A pure functional pipeline (parse → clean → normalise → validate → deduplicate → derive) sits between I/O-only collectors and an append-only journal. A compiler folds journals into content-hashed `QKH1` snapshots plus a manifest. Consumers read through one transport-independent reader whose only visibility rule is `known_at <= T`. Everything is stdlib-only so the reader can be embedded in a safety daemon with zero supply chain.

**Tech Stack:** Python 3.12 stdlib only (no runtime dependencies), `unittest`, `ruff`, `mypy --strict`, Docker (`python:3.12-slim`), GitHub Actions → GHCR.

**Spec:** `docs/spec/2026-09-07-qkt-data-hub-design.md` (committed, `1e95284`). Engine-side companion: `../qkt/docs/superpowers/specs/2026-09-07-hub-stream-binding-design.md` (committed on `docs/hub-stream-binding-spec`).

## Global Constraints

- **Python 3.12+, zero runtime dependencies.** `ruff` and `mypy` are dev-only, never in the image. No PyYAML — `hub/simpleyaml.py` is the restricted loader.
- **Numbers are decimal strings, never floats.** `hub.record.decimal_str` is the only producer of canonical decimal text.
- **All timestamps are UTC epoch milliseconds, `int`.** Naive source timestamps are rejected, never assumed.
- **Append-only.** No file under `raw/`, `journal/`, `snapshot/` is ever modified in place. Corrections are new revisions.
- **`known_at` is recorded, not inferred**, whenever collection is live. Inference is confined to explicit backfill and stamped `availability = derived`.
- **Deterministic ordering everywhere:** `(known_at, scope, key, revision, seq)` — `hub.record.sort_key`.
- **Fail closed.** Unknown schema key, unknown config key, unparseable record → error or quarantine, never a silent default.
- **Line length 120**, ruff rules `E,F,I,UP,B`, `mypy --strict` clean over `hub` and `hubread`.
- **Commit hygiene:** Conventional Commits, subject only, imperative, lowercase, ≤70 chars, no body, no attribution trailer of any kind. Types: `feat|fix|refactor|docs|test|style|build|chore`. Scopes: `record|schema|collect|pipeline|journal|compile|serve|read|cli|docker|ci|docs`.
- **No emoji anywhere.** No AI references in code, comments, commits, or docs.
- **Every public module, class and function gets a docstring** explaining *why*, not what — matching the sibling repos' voice.

---

## File Structure

| Path | Responsibility |
|---|---|
| `hub/errors.py` | One exception per failure class. **Done.** |
| `hub/simpleyaml.py` | Restricted YAML subset loader, nested maps/lists/flow. **Done.** |
| `hub/record.py` | The envelope, invariants, canonical JSON, content id, sort key. **Done.** |
| `hub/schema.py` | `FieldSpec`, `DatasetSchema`, load + hash + payload validation |
| `hub/parse.py` | Named parse functions raw text → typed value (percent, human number, ISO-8601, zoned date, enum) |
| `hub/derive.py` | Closed expression language for derived fields, evaluated over history |
| `hub/pipeline.py` | clean → normalise → validate → dedupe → derive, as pure functions over candidates |
| `hub/dedupe.py` | Per-`key` revision index; exact duplicate drop, changed payload → revision+1 |
| `hub/quarantine.py` | Rejected candidates with reason, counted and written |
| `hub/rawstore.py` | Content-addressed raw blob archive with request metadata sidecar |
| `hub/journal.py` | Append-only NDJSON writer (single writer, lock, fsync) + reader/tailer |
| `hub/snapshot.py` | `QKH1` binary encode/decode |
| `hub/compiler.py` | Journal → snapshot windows + manifest; deterministic, idempotent |
| `hub/manifest.py` | Manifest read/write, coverage and hash bookkeeping |
| `hub/registry.py` | Dataset registry: load `datasets/*.yaml`, resolve schemas, scopes |
| `hub/collectors/__init__.py` | `RawBlob`, `Source` protocol, `CollectResult` |
| `hub/collectors/http.py` | HTTP fetch with backoff, ETag, User-Agent, content-type check |
| `hub/collectors/declarative.py` | YAML-declared JSON/CSV collector: record_path, where, field mapping |
| `hub/collectors/derived.py` | Batch collectors whose inputs are other datasets |
| `hub/collectors/health.py` | `hub.health` emitter |
| `hub/scheduler.py` | Per-source cadence, near-event tightening, circuit breaker |
| `hub/server.py` | stdlib HTTP: `/stream`, `/snapshot`, `/as_of`, `/records`, `/health` |
| `hub/config.py` | `hub.yaml` runtime config, strict unknown-key rejection |
| `hub/logging.py` | One log line format, cadence-gated |
| `hub/__main__.py` | CLI: `run`, `collect`, `compile`, `validate`, `verify`, `as-of`, `ls`, `backfill` |
| `hubread/` | Stdlib-only consumer library: `Reader`, `Policy`, journal tail, snapshot read |
| `datasets/*.yaml` | Dataset schemas + collector declarations |
| `scopes.yaml` | Instrument → scopes map |
| `tests/` | One test module per hub module |

---

## Task 1: Dataset schema — field types, load, hash, payload validation

**Files:**
- Create: `hub/schema.py`
- Test: `tests/test_schema.py`

**Interfaces:**
- Consumes: `hub.simpleyaml.load_path`, `hub.record.FIELD_NAME_RE`, `hub.record.RESERVED_FIELD_NAMES`, `hub.errors.SchemaError`
- Produces:
  - `class FieldType(StrEnum)`: `NUMBER`, `BOOL`, `TIMESTAMP`, `ENUM`, `STRING`
  - `@dataclass(frozen=True) class FieldSpec`: `name: str`, `type: FieldType`, `unit: str = ""`, `values: tuple[str, ...] = ()`, `null_policy: str = "forbid"`, `derived: str = ""`, `strategy: bool = True`
  - `@dataclass(frozen=True) class DatasetSchema`: `name: str`, `version: int`, `scope_kind: str`, `key: tuple[str, ...]`, `fields: dict[str, FieldSpec]`, `title: str`, `value_alias: str`, `quality: dict`, `raw: dict`
    - `.hash() -> str` (`"sha256:"` + hash of canonical JSON of `raw`)
    - `.strategy_fields() -> tuple[FieldSpec, ...]` (declaration order, `strategy=True` only)
    - `.derived_fields() -> tuple[FieldSpec, ...]`
    - `.validate_payload(fields: dict) -> dict` — returns normalised payload, raises `SchemaError`
  - `def load_schema(path: Path) -> DatasetSchema`

- [ ] **Step 1: Write the failing tests**

```python
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from hub.errors import SchemaError
from hub.schema import DatasetSchema, FieldType, load_schema

SCHEMA = """
dataset: macro.us.cpi
version: 1
title: US CPI releases
scope_kind: currency
key: [period_start, scope, title]
fields:
  title:      { type: string, strategy: false }
  impact:     { type: enum, values: [low, medium, high, holiday] }
  actual:     { type: number, unit: pct, null_policy: allow }
  forecast:   { type: number, unit: pct, null_policy: allow }
  surprise:   { type: number, unit: pct, derived: "actual - forecast" }
quality:
  range: { actual: [-5, 5] }
"""


class SchemaTest(unittest.TestCase):
    def _load(self, text: str) -> DatasetSchema:
        with TemporaryDirectory() as d:
            p = Path(d) / "macro.us.cpi.yaml"
            p.write_text(text)
            return load_schema(p)

    def test_loads_fields_in_declaration_order(self):
        s = self._load(SCHEMA)
        self.assertEqual(s.name, "macro.us.cpi")
        self.assertEqual([f.name for f in s.strategy_fields()], ["impact", "actual", "forecast", "surprise"])
        self.assertEqual(s.fields["impact"].type, FieldType.ENUM)
        self.assertEqual(s.fields["impact"].values, ("low", "medium", "high", "holiday"))
        self.assertEqual(s.fields["actual"].unit, "pct")

    def test_hash_is_stable_and_content_addressed(self):
        self.assertEqual(self._load(SCHEMA).hash(), self._load(SCHEMA).hash())
        self.assertNotEqual(self._load(SCHEMA).hash(), self._load(SCHEMA.replace("version: 1", "version: 2")).hash())

    def test_rejects_unknown_top_level_key(self):
        with self.assertRaisesRegex(SchemaError, "unknown"):
            self._load(SCHEMA + "\nnonsense: 1\n")

    def test_rejects_field_name_colliding_with_envelope(self):
        with self.assertRaisesRegex(SchemaError, "known_at"):
            self._load(SCHEMA.replace("  actual:", "  known_at:"))

    def test_rejects_key_naming_an_undeclared_field(self):
        with self.assertRaisesRegex(SchemaError, "nope"):
            self._load(SCHEMA.replace("key: [period_start, scope, title]", "key: [nope]"))

    def test_validate_payload_accepts_declared_fields(self):
        s = self._load(SCHEMA)
        out = s.validate_payload({"impact": 2, "actual": "0.3", "forecast": "0.2", "surprise": "0.1", "title": "CPI"})
        self.assertEqual(out["actual"], "0.3")

    def test_validate_payload_rejects_undeclared_field(self):
        with self.assertRaisesRegex(SchemaError, "undeclared"):
            self._load(SCHEMA).validate_payload({"impact": 2, "surprise_zz": "1"})

    def test_validate_payload_rejects_null_when_forbidden(self):
        with self.assertRaisesRegex(SchemaError, "null"):
            self._load(SCHEMA).validate_payload({"impact": None})

    def test_validate_payload_rejects_out_of_range(self):
        with self.assertRaisesRegex(SchemaError, "range"):
            self._load(SCHEMA).validate_payload({"impact": 2, "actual": "99"})

    def test_validate_payload_canonicalises_decimals(self):
        out = self._load(SCHEMA).validate_payload({"impact": 2, "actual": "0.30"})
        self.assertEqual(out["actual"], "0.3")
```

- [ ] **Step 2: Run to verify it fails**

Run: `python -m unittest tests.test_schema -v`
Expected: FAIL, `ModuleNotFoundError: No module named 'hub.schema'`

- [ ] **Step 3: Implement `hub/schema.py`**

Requirements the tests encode, plus these that the spec requires and the tests above do not reach: `scope_kind` must be one of `currency|instrument|issuer|all`; enum `values` must be unique lowercase tokens; `null_policy` must be one of `forbid|allow|allow_until_release`; a `derived` expression may only reference declared non-derived field names and envelope timestamps (validation of the expression itself is Task 3, which exposes `hub.derive.referenced_names`); `value_alias`, when present, must name a declared field.

- [ ] **Step 4: Run to verify it passes**

Run: `python -m unittest tests.test_schema -v` → all pass. Then `python -m ruff check hub tests && python -m mypy`.

- [ ] **Step 5: Commit**

```bash
git add hub/schema.py tests/test_schema.py
git commit -m "feat(schema): dataset schema with typed fields and content hash"
```

---

## Task 2: Parse functions — raw text to typed values

**Files:**
- Create: `hub/parse.py`
- Test: `tests/test_parse.py`

**Interfaces:**
- Consumes: `hub.errors.ParseError`, `hub.record.decimal_str`
- Produces:
  - `def percent_or_number(raw: str) -> str | None` — strips `%`, `K`/`M`/`B` suffixes, thousands separators; `""`/`-`/`n/a`/`—` → `None`
  - `def human_number(raw: str) -> str | None`
  - `def iso8601_with_offset(raw: str) -> int` — **rejects a naive timestamp**
  - `def date_in_zone(raw: str, zone: str, hour: int = 0) -> int`
  - `def enum_ordinal(raw: str, mapping: dict[str, int]) -> int | None`
  - `def boolean(raw: str) -> bool | None`
  - `PARSERS: dict[str, Callable[..., object]]` — name → function, for declarative collectors
  - `def apply(name: str, raw: object, **kwargs: object) -> object`

- [ ] **Step 1: Write the failing tests**

```python
import unittest

from hub.errors import ParseError
from hub.parse import date_in_zone, enum_ordinal, iso8601_with_offset, percent_or_number


class ParseTest(unittest.TestCase):
    def test_percent_strips_sign_and_canonicalises(self):
        self.assertEqual(percent_or_number("0.30%"), "0.3")
        self.assertEqual(percent_or_number("-1.2%"), "-1.2")
        self.assertEqual(percent_or_number("1,234"), "1234")

    def test_percent_maps_sentinels_to_none(self):
        for token in ("", " ", "-", "n/a", "N/A", "—"):
            self.assertIsNone(percent_or_number(token))

    def test_human_suffixes(self):
        self.assertEqual(percent_or_number("1.2K"), "1200")
        self.assertEqual(percent_or_number("3.4M"), "3400000")
        self.assertEqual(percent_or_number("2B"), "2000000000")

    def test_iso8601_with_offset_converts_to_utc_ms(self):
        # 08:15 New York in daylight time is 12:15 UTC.
        self.assertEqual(iso8601_with_offset("2026-09-10T08:15:00-04:00"), 1789042500000)

    def test_iso8601_rejects_naive_timestamp(self):
        with self.assertRaisesRegex(ParseError, "offset"):
            iso8601_with_offset("2026-09-10T08:15:00")

    def test_date_in_zone_handles_dst(self):
        summer = date_in_zone("2026-07-01", "America/New_York", hour=8)
        winter = date_in_zone("2026-01-05", "America/New_York", hour=8)
        self.assertEqual((summer % 86_400_000) // 3_600_000, 12)
        self.assertEqual((winter % 86_400_000) // 3_600_000, 13)

    def test_enum_ordinal_is_case_insensitive_and_none_on_miss(self):
        mapping = {"low": 0, "high": 2}
        self.assertEqual(enum_ordinal("HIGH", mapping), 2)
        self.assertIsNone(enum_ordinal("weird", mapping))
```

- [ ] **Step 2: Run to verify it fails.** Expected: `ModuleNotFoundError`.
- [ ] **Step 3: Implement `hub/parse.py`.** Use `zoneinfo` (stdlib) for zones. `iso8601_with_offset` uses `datetime.fromisoformat` then requires `tzinfo is not None and utcoffset() is not None`.
- [ ] **Step 4: Run tests, ruff, mypy — all clean.**
- [ ] **Step 5: Commit**

```bash
git add hub/parse.py tests/test_parse.py
git commit -m "feat(collect): parse functions for typed values from raw feeds"
```

---

## Task 3: Derived-field expression language

**Files:**
- Create: `hub/derive.py`
- Test: `tests/test_derive.py`

**Interfaces:**
- Consumes: `hub.record.Record`, `hub.record.decimal_str`, `hub.errors.SchemaError`
- Produces:
  - `def referenced_names(expr: str) -> frozenset[str]`
  - `def compile_expr(expr: str) -> Expr` where `Expr = Callable[[Mapping[str, object], Sequence[Mapping[str, object]]], str | None]` — arguments are the current payload and the dataset's prior payloads in `known_at` order
  - Supported: `+ - * /`, parentheses, decimal literals, field names, and the functions `zscore(field, window=N, min_obs=M)`, `lag(field, n)`, `diff(field, n)`, `pct_rank(field, window=N)`, `since(ts_field)`, `until(ts_field)`
  - Raises `SchemaError` on an unknown name or function at compile time

- [ ] **Step 1: Write the failing tests**

```python
import unittest

from hub.derive import compile_expr, referenced_names
from hub.errors import SchemaError


class DeriveTest(unittest.TestCase):
    def test_referenced_names(self):
        self.assertEqual(referenced_names("actual - forecast"), frozenset({"actual", "forecast"}))

    def test_arithmetic_is_exact_decimal(self):
        f = compile_expr("actual - forecast")
        self.assertEqual(f({"actual": "0.3", "forecast": "0.2"}, []), "0.1")

    def test_none_input_yields_none(self):
        self.assertIsNone(compile_expr("actual - forecast")({"actual": None, "forecast": "0.2"}, []))

    def test_division_by_zero_yields_none(self):
        self.assertIsNone(compile_expr("actual / forecast")({"actual": "1", "forecast": "0"}, []))

    def test_zscore_uses_history_and_respects_min_obs(self):
        history = [{"surprise": str(v)} for v in (0, 0, 0, 1, -1)]
        f = compile_expr("zscore(surprise, window=5, min_obs=4)")
        self.assertIsNotNone(f({"surprise": "2"}, history))
        self.assertIsNone(f({"surprise": "2"}, history[:2]))

    def test_lag_reads_prior_payload(self):
        self.assertEqual(compile_expr("lag(actual, 1)")({"actual": "3"}, [{"actual": "1"}, {"actual": "2"}]), "2")

    def test_unknown_function_fails_at_compile_time(self):
        with self.assertRaisesRegex(SchemaError, "unknown"):
            compile_expr("magic(actual)")
```

- [ ] **Step 2: Run to verify it fails.**
- [ ] **Step 3: Implement `hub/derive.py`** with a recursive-descent parser over a token list. Do **not** use `eval`. All arithmetic through `decimal.Decimal` with a fixed `Context(prec=28)`; results through `decimal_str`.
- [ ] **Step 4: Run tests, ruff, mypy.**
- [ ] **Step 5: Commit**

```bash
git add hub/derive.py tests/test_derive.py
git commit -m "feat(pipeline): closed expression language for derived fields"
```

---

## Task 4: Deduplication and revision assignment

**Files:**
- Create: `hub/dedupe.py`
- Test: `tests/test_dedupe.py`

**Interfaces:**
- Consumes: `hub.record.Record`
- Produces:
  - `@dataclass class RevisionIndex`: `.seen(dataset, key) -> tuple[int, str] | None`, `.observe(record) -> None`, `.rebuild_from(records: Iterable[Record]) -> None`
  - `def assign(index: RevisionIndex, candidate: Record) -> Record | None` — returns `None` for an exact duplicate, otherwise the record with `revision` set

- [ ] **Step 1: Write the failing tests**

```python
import unittest

from hub.dedupe import RevisionIndex, assign
from hub.record import Availability, Record

BASE = dict(
    dataset="macro.us.cpi", scope="USD", key="2026-08|USD|CPI", known_at=1_760_000_000_000,
    effective_at=1_760_000_000_000, availability=Availability.OBSERVED, source="test", revision=1,
)


class DedupeTest(unittest.TestCase):
    def test_first_sighting_is_revision_one(self):
        idx = RevisionIndex()
        out = assign(idx, Record.create(**BASE, fields={"actual": None}))
        self.assertEqual(out.revision, 1)

    def test_identical_payload_is_dropped(self):
        idx = RevisionIndex()
        assign(idx, Record.create(**BASE, fields={"actual": None}))
        self.assertIsNone(assign(idx, Record.create(**{**BASE, "known_at": BASE["known_at"] + 60_000},
                                                    fields={"actual": None})))

    def test_changed_payload_becomes_next_revision(self):
        idx = RevisionIndex()
        assign(idx, Record.create(**BASE, fields={"actual": None}))
        out = assign(idx, Record.create(**{**BASE, "known_at": BASE["known_at"] + 60_000},
                                        fields={"actual": "0.3"}))
        self.assertEqual(out.revision, 2)

    def test_rebuild_from_journal_restores_state(self):
        idx = RevisionIndex()
        r1 = assign(idx, Record.create(**BASE, fields={"actual": None}))
        fresh = RevisionIndex()
        fresh.rebuild_from([r1])
        self.assertIsNone(assign(fresh, Record.create(**BASE, fields={"actual": None})))
```

- [ ] **Step 2–4:** fail, implement, pass, ruff, mypy.
- [ ] **Step 5: Commit**

```bash
git add hub/dedupe.py tests/test_dedupe.py
git commit -m "feat(pipeline): revision assignment and exact-duplicate drop"
```

---

## Task 5: Journal — append-only writer, reader, tailer

**Files:**
- Create: `hub/journal.py`
- Test: `tests/test_journal.py`

**Interfaces:**
- Consumes: `hub.record.Record`, `hub.errors.StoreError`
- Produces:
  - `class JournalWriter(root: Path)`: `.append(record) -> Record` (assigns `seq`, returns stamped record), `.close()`, context manager, exclusive lock via `fcntl.flock` on `<root>/.writer.lock`; a second writer raises `StoreError`
  - `def read_range(root, dataset, known_from, known_to) -> list[Record]`
  - `class JournalTail(root, dataset, from_seq=0)`: `.poll() -> list[Record]` — ignores a trailing partial line, de-duplicates `seq <= last_seq`, follows the day roll
  - Path layout `<root>/journal/<dataset>/<YYYY-MM-DD>.ndjson`, dated by `known_at`

- [ ] **Step 1: Write the failing tests** — cover: seq starts at 1 and increases; restart resumes seq from the tail; a partial trailing line is ignored then delivered once completed; day roll is followed; a malformed line raises on `read_range` but is skipped-and-counted by `JournalTail`; two writers on one root conflict.

```python
def test_partial_trailing_line_is_ignored_then_delivered(self):
    # Write a record, then half of another, poll; then complete it and poll again.
    ...
    self.assertEqual([r.seq for r in tail.poll()], [1])
    path.write_text(path.read_text() + rest + "\n")
    self.assertEqual([r.seq for r in tail.poll()], [2])
```

- [ ] **Step 2–4:** fail, implement, pass, ruff, mypy.
- [ ] **Step 5: Commit**

```bash
git add hub/journal.py tests/test_journal.py
git commit -m "feat(journal): append-only writer with single-writer lock and tailer"
```

---

## Task 6: `QKH1` snapshot format

**Files:**
- Create: `hub/snapshot.py`
- Test: `tests/test_snapshot.py`

**Interfaces:**
- Produces: `def encode(schema, records) -> bytes`, `def decode(data) -> tuple[SnapshotHeader, list[Record]]`, `def sha256_of(data) -> str`
- Layout exactly as the spec's Section 5.1: magic `QKH1`, version, schema hash, dataset name, record count, field table, scope and key dictionaries, columnar `int64` blocks, `NULL_SENTINEL = -2**63`, trailing SHA-256.

- [ ] **Step 1: Write the failing tests** — round-trip equality for every field type; nulls survive; a truncated buffer raises `StoreError`; a flipped byte fails the trailer check; encoding the same records twice is byte-identical; scale-8 decimals round-trip exactly (`"1850.5"` → `185050000000` → `"1850.5"`); a value needing more than scale 8 raises rather than silently rounding.
- [ ] **Step 2–4:** fail, implement, pass, ruff, mypy.
- [ ] **Step 5: Commit**

```bash
git add hub/snapshot.py tests/test_snapshot.py
git commit -m "feat(compile): QKH1 columnar snapshot encode and decode"
```

---

## Task 7: Compiler and manifest

**Files:**
- Create: `hub/compiler.py`, `hub/manifest.py`
- Test: `tests/test_compiler.py`

**Interfaces:**
- Produces: `def compile_dataset(root, schema, window) -> WindowResult`, `def compile_all(root, registry) -> Manifest`, `Manifest.load/save`, `def verify(root) -> list[str]` (returns problems; empty means clean)
- Windows are calendar months by default (`retention`/`window` in schema may set `year`).

- [ ] **Step 1: Write the failing tests** — the determinism test is the important one:

```python
def test_shuffled_journal_lines_compile_to_identical_bytes(self):
    ordered = compile_dataset(root, schema, "2026-09")
    shuffle_journal_lines(root, seed=7)
    self.assertEqual(compile_dataset(root, schema, "2026-09").sha256, ordered.sha256)

def test_recompiling_is_idempotent(self): ...
def test_manifest_records_coverage_and_hashes(self): ...
def test_derived_fields_recomputed_match_journalled_values(self): ...
```

- [ ] **Step 2–4:** fail, implement, pass, ruff, mypy.
- [ ] **Step 5: Commit**

```bash
git add hub/compiler.py hub/manifest.py tests/test_compiler.py
git commit -m "feat(compile): deterministic snapshot compiler and manifest"
```

---

## Task 8: The reader library (`hubread`) and the look-ahead property test

**Files:**
- Create: `hubread/__init__.py`, `hubread/policy.py`, `hubread/reader.py`
- Test: `tests/test_reader.py`

**Interfaces:**
- Produces:
  - `@dataclass(frozen=True) class Policy`: `min_lag_ms=0`, `refuse_derived=False`, `stale_after_ms=900_000`, `skew_tolerance_ms=5_000`
  - `class Reader(root: Path, policy: Policy = Policy())`: `.datasets()`, `.schema(name)`, `.records(dataset, from_seq)`, `.range(dataset, known_from, known_to)`, `.as_of(dataset, scope, t)`, `.windows(dataset, scope, t, ahead_ms, pad_ms)`, `.health()`
- **Stdlib only, and importable without `hub`** — the guardian embeds this.

- [ ] **Step 1: Write the failing tests** — including the two properties the whole product rests on:

```python
def test_as_of_never_returns_a_record_from_the_future(self):
    for t in sample_times:
        for rec in reader.as_of("macro.us.cpi", "USD", t).values():
            self.assertLessEqual(rec.known_at, t)

def test_a_revision_is_invisible_before_its_own_known_at(self):
    # r2 revises r1 at t2. At t2-1 the reader must still answer r1.
    self.assertEqual(reader.as_of(ds, "USD", t2 - 1)[key].revision, 1)
    self.assertEqual(reader.as_of(ds, "USD", t2)[key].revision, 2)

def test_min_lag_delays_visibility(self): ...
def test_refuse_derived_hides_backfilled_records(self): ...
def test_snapshot_and_journal_paths_agree(self):
    self.assertEqual(from_snapshot, from_journal)
```

- [ ] **Step 2–4:** fail, implement, pass, ruff, mypy.
- [ ] **Step 5: Commit**

```bash
git add hubread tests/test_reader.py
git commit -m "feat(read): point-in-time reader with policy and look-ahead property tests"
```

---

## Task 9: Raw archive, quarantine, and the collector contract

**Files:**
- Create: `hub/rawstore.py`, `hub/quarantine.py`, `hub/collectors/__init__.py`, `hub/collectors/http.py`
- Test: `tests/test_rawstore.py`, `tests/test_http_collector.py`

**Interfaces:**
- Produces:
  - `@dataclass(frozen=True) class RawBlob`: `body: bytes`, `content_type: str`, `fetched_at: int`, `request: dict[str, str]`; `.sha256() -> str`
  - `class RawStore(root)`: `.put(blob) -> str` (content address, idempotent), `.get(ref) -> RawBlob`
  - `class Quarantine(root)`: `.write(dataset, reason, payload) -> None`, `.count(dataset, day) -> int`
  - `Protocol Source`: `name: str`, `fetch(timeout: float) -> RawBlob | None`, `parse(blob: RawBlob) -> list[Candidate]`
  - `class HttpSource`: ETag/If-Modified-Since, declared User-Agent, **content-type check that fails loud** (the guardian's HTML-instead-of-JSON failure), returns `None` on any network error rather than raising
- Tests use a local `http.server` fixture, never the network.

- [ ] **Steps 1–5** as above; commit:

```bash
git commit -m "feat(collect): raw archive, quarantine and the HTTP source contract"
```

---

## Task 10: Declarative collector engine

**Files:**
- Create: `hub/collectors/declarative.py`, `hub/registry.py`, `hub/config.py`
- Test: `tests/test_declarative.py`, `tests/test_config.py`

**Interfaces:**
- Produces: `def build_source(schema) -> Source`, `class Registry`: `.datasets()`, `.schema(name)`, `.source(name)`; `class HubConfig` with **strict unknown-key rejection at every level**
- The declarative mapping supports: `record_path` (`$[*]` and `$.a.b[*]`), `where` (equality and membership), `scope.from`/`scope.map`, `effective_at.from`/`.parse`, `known_at: ingest_time|from field`, `fields.<name>.from|parse|map|null_if`, `key`.

- [ ] **Step 1: Write the failing tests** — parse a captured ForexFactory fixture into candidates; assert the New York offset is converted; assert a `High` row with an empty `actual` yields a null rather than a zero; assert an unknown mapping key is rejected at load.
- [ ] **Steps 2–5**; commit:

```bash
git commit -m "feat(collect): declarative collector engine and dataset registry"
```

---

## Task 11: Pipeline wiring and the scheduler

**Files:**
- Create: `hub/pipeline.py`, `hub/scheduler.py`, `hub/logging.py`
- Test: `tests/test_pipeline.py`, `tests/test_scheduler.py`

**Interfaces:**
- Produces: `def ingest(candidates, schema, index, history, clock) -> IngestResult` (records, quarantined, counters); `class Scheduler` with per-source steady/retry cadence, near-event tightening, circuit breaker after N failures, jitter
- Pipeline enforces: live collection is always `observed`; `known_at <= now + skew_tolerance`; `known_at` non-decreasing per dataset unless `availability != observed`.

- [ ] **Steps 1–5**; commit:

```bash
git commit -m "feat(pipeline): ingest stages, counters and source scheduling"
```

---

## Task 12: Datasets — calendar, health, and a derived calendar

**Files:**
- Create: `datasets/cal.high_impact.yaml`, `datasets/hub.health.yaml`, `datasets/cal.next_event.yaml`, `scopes.yaml`, `hub/collectors/derived.py`, `hub/collectors/health.py`
- Test: `tests/test_datasets.py`, `tests/fixtures/forexfactory_thisweek.json`

**Interfaces:**
- `cal.high_impact`: scope = currency, fields `title(string)`, `impact(enum)`, `forecast`, `previous`, `actual`, `surprise(derived)`
- `cal.next_event`: derived per scope, fields `next_high_at(timestamp)`, `last_high_at(timestamp)` — this is what a strategy subtracts from `NOW.epoch_ms`
- `hub.health`: fields `last_heartbeat_at(timestamp)`, `datasets_stale(number)`, `quarantined_today(number)`

- [ ] **Steps 1–5**; commit:

```bash
git commit -m "feat(collect): calendar, derived next-event and hub health datasets"
```

---

## Task 13: CLI

**Files:**
- Create: `hub/__main__.py`
- Test: `tests/test_cli.py`

**Interfaces:**
- `hub run` (scheduler + writer + compiler + server), `hub collect <dataset> [--once]`, `hub compile [--dataset] [--window]`, `hub validate [path]`, `hub verify` (recompile and compare hashes), `hub as-of <dataset> <scope> <iso8601>`, `hub ls`, `hub backfill <dataset> --from --to`
- Exit codes: `0` success, `2` usage, `3` data problem. Never exit 0 on a failed verify.

- [ ] **Steps 1–5**; commit:

```bash
git commit -m "feat(cli): run, collect, compile, verify, as-of and backfill verbs"
```

---

## Task 14: HTTP server — stream, snapshot, query, health

**Files:**
- Create: `hub/server.py`
- Test: `tests/test_server.py`

**Interfaces:**
- `GET /health`, `GET /manifest`, `GET /stream/<dataset>?from_seq=N` (server-sent events, same `seq`), `GET /snapshot/<dataset>/<window>` (identical bytes to the file), `GET /as_of?dataset=&scope=&t=`, `GET /records?dataset=&from=&to=`
- Loopback bind by default; optional bearer token; read-only — no route mutates.

- [ ] **Steps 1–5**; commit:

```bash
git commit -m "feat(serve): read-only stream, snapshot and query endpoints"
```

---

## Task 15: Docker, CI, release

**Files:**
- Create: `Dockerfile`, `.dockerignore`, `docker-compose.example.yml`, `.github/workflows/ci.yml`, `.github/workflows/release.yml`

**Requirements:** `python:3.12-slim`, non-root uid 10001, `VOLUME /data`, `HEALTHCHECK` that checks heartbeat freshness (not "process alive"), `ENTRYPOINT ["python", "-m", "hub"]`. CI: ruff → mypy → unittest → version agreement → docker smoke (build image, run the pipeline against a local fixture server, assert a snapshot appears and `hub verify` exits 0) → publish to GHCR on tag.

- [ ] **Steps 1–5**; commit:

```bash
git commit -m "build(docker): image, compose example and CI with a pipeline smoke test"
```

---

## Task 16: Documentation

**Files:**
- Create: `README.md`, `AGENTS.md`, `CLAUDE.md`, `CHANGELOG.md`, `docs/FORMAT.md`, `docs/DATA-ENGINEERING.md`, `docs/EXTENDING.md`, `docs/OPERATIONS.md`, `docs/assets/*.svg`

**Requirements:** README in the sibling repos' shape — centred logo, one-sentence positioning, badges (ci, licence, GHCR, python), an ASCII architecture diagram, Features, the record format, Quickstart, Configuration, Extending, Why no dependencies. `AGENTS.md`/`CLAUDE.md` carry the data-engineering hygiene rules (append-only, three timestamps, decimal strings, fail closed, one writer) so an agent reads them before touching anything.

- [ ] Commit: `docs: readme, agent instructions and format reference`

---

## Task 17: Publish and run for real

- [ ] `gh repo create elitekaycy/qkt-data-hub --public --source . --push`
- [ ] `docker build -t qkt-data-hub:local .`
- [ ] Run the pipeline against the live calendar feed; confirm records land, a snapshot compiles, `hub verify` exits 0, and `hub as-of` answers correctly.
- [ ] Confirm the archive captures consensus that the rolling weekly feed will otherwise lose (spec Phase 0).

---

## Task 18: Engine side — implement `HUB:` per the committed qkt spec

Separate plan, written after Task 17 lands, in the qkt repo at `docs/superpowers/plans/2026-09-07-hub-stream-binding.md`, following that repo's own skill (branch off `dev`, ktlint-clean, no attribution trailer, parity tests). Gated on the byte-identity test for strategies that bind no hub stream.

---

## Self-Review

**Spec coverage.** Section 3 (data model) → Tasks 1, 3; Section 4 (pipeline stages) → Tasks 2, 4, 9, 10, 11; Section 5 (storage/formats) → Tasks 5, 6, 7; Section 6 (determinism contract) → Tasks 6, 7, 8 (shuffle-and-hash, look-ahead, vintage isolation, replay equivalence); Section 7 (extending) → Tasks 10, 12; Section 8 (consumer contract) → Tasks 8, 14, 18; Section 9 (unstructured/corporate) → deferred to the spec's Phases 3–4, out of this plan by design; Section 10 (scale/ops/security) → Tasks 14, 15; Section 11 (testing) → every task; Section 12 (delivery Phase 0/1) → Tasks 12, 17.

**Gap found and closed:** the spec's replay-equivalence test had no home; it is now the last assertion in Task 8 (`test_snapshot_and_journal_paths_agree`).

**Type consistency:** `Record`, `DatasetSchema`, `FieldSpec`, `RawBlob`, `Policy`, `Reader` names are used identically across Tasks 1–14. `Candidate` is introduced in Task 9 and consumed in Tasks 10–11.
