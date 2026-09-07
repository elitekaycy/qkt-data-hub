# The format

Two artifacts, one schema. A journal is what a writer appends to; a snapshot is what a compiler
folds a window of that journal into. They carry the same records, which is what makes live and
history the same bytes rather than two pipelines that agree by hope.

## The record envelope

Frozen and versioned as a whole (`v: 1`). Adding a field is a version bump.

| Field | Type | Meaning |
|---|---|---|
| `v` | int | envelope version |
| `dataset` | string | dotted lowercase name; the first segment must be a registered domain |
| `scope` | string | currency, `VENUE:SYMBOL`, `ISSUER:<ticker>`, or `ALL` |
| `key` | string | identity of the thing, stable across every revision of it |
| `revision` | int | 1-based, increments per key on each changed payload |
| `known_at` | int | epoch ms UTC. **The only field the visibility rule reads** |
| `effective_at` | int | epoch ms UTC; when the fact happens or begins to apply |
| `period_start`, `period_end` | int or null | the interval the fact describes |
| `availability` | enum | `observed`, `published`, or `derived` |
| `source`, `source_version`, `parser` | string | where it came from and what read it |
| `raw_ref` | string or null | content hash of the archived bytes it was parsed from |
| `seq` | int | per-dataset writer sequence, strictly increasing |
| `id` | string | sha256 over the canonical JSON of everything above except `seq` and `id` |
| `fields` | object | the payload the dataset's schema declares |

`seq` is excluded from `id` on purpose: the same fact must hash identically on a rebuild, which
is what lets a consumer de-duplicate across a restart.

### Payload values

- **number** — a JSON *string* holding a plain decimal. Never a float.
- **bool** — JSON `true`/`false`.
- **timestamp** — int, epoch ms UTC.
- **enum** — an int ordinal; the schema maps ordinals to names.
- **string** — UTF-8, and never read by a strategy. String fields are marked `strategy: false`
  and are stored in the journal only, never in a snapshot.
- `null` is permitted only where the field's `null_policy` allows it.

## Availability

How `known_at` was obtained, and therefore how much to trust it.

| Class | Meaning | Produced by |
|---|---|---|
| `observed` | recorded from our own ingest clock at first sight | live collection, always |
| `published` | the source supplied a publication or acceptance timestamp we validated | vintage-aware backfill |
| `derived` | estimated from a schedule or a fixed lag | explicit backfill only |

A consumer may refuse `derived` entirely (`Policy(refuse_derived=True)`). Nothing ever upgrades
a record from `derived` to `observed`.

## The journal

`<root>/journal/<dataset>/<YYYY-MM-DD>.ndjson`, the day taken from `known_at` in UTC. One record
per line, UTF-8, newline-terminated, append-only.

One writer per root, enforced by `flock`. Two writers would interleave `seq` values and corrupt
the ordering everything downstream depends on. Each append is a single write of the whole line
followed by an fsync, so a crash can truncate a line but never interleave two.

A reader must ignore a trailing line with no terminating newline: it may still be in flight. On
the next writer start that torn fragment is truncated away and written to `quarantine/` with the
reason, because it was never a committed record and leaving it would corrupt the next append.

## The `QKH1` snapshot

Little-endian, columnar, with a trailing SHA-256 over everything above it. One contiguous block
per column, in sorted record order, so a range scan over `known_at` is one contiguous read.

```
header  MAGIC "QKH1" (4) | version i32 | schema_hash bytes[32]
        | dataset_len i32 | dataset utf8
        | record_count i32 | field_count i32 | scale i32 (=8)
        | field table: per field { name_len i32, name utf8, type u8, unit_len i32, unit utf8 }
        | dictionaries: scope | key | source | source_version | parser | raw_ref
body    known_at i64 | effective_at i64 | period_start i64* | period_end i64*
        | scope_idx i32 | key_idx i32 | revision i32 | availability u8 | seq i64
        | source_idx i32 | source_version_idx i32 | parser_idx i32 | raw_ref_idx i32
        | one i64 column per field, in field-table order
        * absent values use NULL_SENTINEL = i64.MIN; a null raw_ref uses index -1
trailer sha256 (32)
```

Numbers are stored as integers scaled by 1e8. A value needing more than eight decimal places
raises rather than rounding, because silent rounding is how a fact quietly changes.

`encode` sorts internally, so the same records produce byte-identical output regardless of the
order they arrive in. That property is what the determinism test asserts.

## Ordering

`(known_at, scope, key, revision, seq)`, everywhere — the compiler, the reader and every test.
`known_at` first is what makes "everything visible at T" answerable by a scan that stops early.

## The manifest

`<root>/manifest.json` records, per dataset: the schema hash and path, scope kind, last `seq`,
last `known_at`, and every compiled window with its file, SHA-256, record count and the raw blob
hashes it depends on. A backtest cites those hashes; `verify` recompiles every window and
compares.
