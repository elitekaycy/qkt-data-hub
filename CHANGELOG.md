# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.3] - 2026-09-07

### Fixed

- **`JournalWriter` exhausted the process file-descriptor limit on a multi-year backfill.** It
  cached one open descriptor per calendar day for its whole lifetime; a historical load spanning
  years opens one file per day and hit the container default limit (1024) partway through,
  failing with `OSError: Too many open files` and leaving an incomplete journal -- observed
  loading the FRED rate series onto a fresh store. The cache is now capped at 128 descriptors,
  least-recently-used eviction, which a live collector (writing only today's file) never reaches.

## [0.1.2] - 2026-09-07

### Added

- **Four Treasury and policy-rate datasets** (`rates.us.dgs2`, `rates.us.dgs10`,
  `rates.us.fedfunds`, `rates.us.dfedtaru`, `rates.us.dfedtarl`): the short and long ends of the
  curve, the effective funds rate, and the FOMC target band, each with a live CSV collector and
  the same next-business-day-13:00-UTC backfill lag as `rates.us.dfii10`.
- **`pos.cftc.cot_gold`**: CFTC Commitments of Traders positioning for COMEX gold (class C),
  reported Tuesdays and published the following Friday at 15:30 ET; a live JSON collector and
  `tools/cftc_backfill.py` load history stamped `derived` with that exact lag. Includes a
  `net_noncomm` derived field.
- `date_in_zone` now accepts a source's floating-timestamp date serialization (Socrata's
  `"2026-09-01T00:00:00.000"`) in addition to a bare `YYYY-MM-DD`, needed by the CFTC feed and
  reusable by any future dataset from a similar platform.

## [0.1.1] - 2026-09-07

### Added

- **`rates.us.dfii10`**: the 10-year TIPS real yield (FRED `DFII10`), one observation per US
  business day, with a live CSV collector and a derived one-day change. The strongest documented
  slow driver of gold.
- **`tools/fred_backfill.py`**: turns a FRED daily CSV into `backfill` records stamped `derived`
  with an explicit availability rule (next US business day, 13:00 UTC), the same window the
  engine's macro path assumes. Used to load 2018-2026 in one pass; a strict consumer may refuse
  the result.

### Changed

- `value` is no longer a reserved field name. A single-field dataset names its one field `value`
  so a consumer reads `alias.value` with no schema lookup at compile time. `value_alias` remains
  for datasets that want a differently named field to answer to `.value` as well.

## [0.1.0] - 2026-09-07

First working store: acquire, journal, compile, read, all under one record format.

### Added

- **The record format** (`hubread/record.py`). A frozen envelope carrying `dataset`, `scope`,
  `key`, `revision`, the three timestamps (`period_*`, `effective_at`, `known_at`),
  `availability`, provenance (`source`, `source_version`, `parser`, `raw_ref`), `seq` and a
  content-addressed `id`. Numbers are canonical decimal strings; timestamps are UTC epoch
  milliseconds.
- **Dataset schemas** (`hub/schema.py`). Typed fields, an identity key, quality ranges and
  derived expressions, content-hashed so a backtest can cite exactly which contract it ran
  against. Unknown keys are rejected at every level of the file.
- **A closed expression language** for derived fields (`hub/derive.py`): arithmetic plus
  `zscore`, `lag`, `diff`, `pct_rank`, `since`, `until`. Hand-written tokenizer and
  recursive-descent parser, never `eval`. Total: a missing input or a division by zero yields
  nothing rather than raising, and decimal overflow is caught.
- **The append-only journal** (`hub/journal.py`, `hubread/journal.py`) with an exclusive
  single-writer lock, line-atomic appends, a tailer that ignores an incomplete trailing line
  and follows the day roll, and torn-tail repair that quarantines the discarded bytes.
- **The `QKH1` snapshot format** (`hub/snapshot.py`, `hubread/snapshot.py`): columnar, scaled
  integers at scale 8, dictionary-encoded scope, key and provenance, a trailing SHA-256, and
  byte-identical output regardless of input order.
- **A deterministic compiler and manifest** (`hub/compiler.py`, `hub/manifest.py`) that
  recomputes and re-verifies every derived field, so a parser or expression change cannot
  silently rewrite history.
- **The point-in-time reader** (`hubread/`), standalone and stdlib-only, with a policy for
  minimum lag, refusing `derived` availability, and staleness.
- **Acquisition**: content-addressed raw archive, quarantine, an HTTP source that refuses a
  wrong content type even on a 200, and a declarative collector engine so a new dataset is a
  YAML file rather than code.
- **The pipeline** (`hub/pipeline.py`): key building, derivation in dependency order, schema
  validation, deduplication and revision assignment, with live ingest always stamping
  `observed` from the ingest clock and quarantining any record whose `known_at` runs ahead of
  it.
- **The scheduler** (`hub/scheduler.py`): per-source cadence and backoff, cadence tightening
  near a known event, and a heartbeat file as the liveness signal.
- **A CLI** (`hub/__main__.py`): `ls`, `validate`, `collect`, `compile`, `verify`, `as-of`,
  `health`, `backfill`, `run`.
- **The `cal.high_impact` dataset**: high-impact scheduled releases with forecast, previous,
  actual, and derived surprise and surprise z-score.
- **Docker image** on `python:3.12-slim`, non-root, with a healthcheck that tests heartbeat
  freshness rather than process liveness, and CI that runs the real pipeline in the image,
  asserts a re-collect of unchanged bytes adds nothing, and asserts `verify` fails a tampered
  snapshot.

### Notes

- The economic-calendar provider serves only the current week; last week, next week and this
  month all return 404. Consensus not captured while the week is live is lost permanently,
  which is why this store exists now rather than later.
