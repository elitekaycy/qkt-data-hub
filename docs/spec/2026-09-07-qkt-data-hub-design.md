# qkt-data-hub — Design Specification

> **Status:** Draft v0.1, 2026-09-07. Scoping spec for a new sibling product.
> **Owner:** elitekaycy. **Reviewers:** none yet.
> **Companion:** `qkt/docs/superpowers/specs/2026-09-07-hub-stream-binding-design.md`,
> the engine-side change that binds `HUB:` datasets into the DSL. This document is the hub;
> that one is the reader. Section 8 here fixes the contract between them.

---

## 0. Summary

qkt-data-hub is a standalone point-in-time fact store and data pipeline. It acquires
non-price information that trading strategies can react to — scheduled economic releases and
their consensus, published macro series and their revisions, positioning reports, inventories,
central-bank communication, corporate earnings and filings, and the facts of the trading venue
itself — and turns it into one universal, versioned, append-only record format that is
**byte-identical whether a consumer reads it as a finished historical snapshot or tails it
live**.

The engine (qkt) owns a reader and nothing else. Every fetch, parse, clean, normalise,
validate, deduplicate, derive and revise step lives here. A strategy in the qkt DSL binds a
dataset by name and reads typed fields; a backtest and a live session see the same bytes at the
same event time; the guardian reads the same journal to arm its news rung without a network
call; anything else reads it over a stream or query API.

The correctness property the whole product exists to protect is **no look-ahead**: a consumer
standing at time T sees only records whose `known_at` is at or before T, and `known_at` is
never guessed when it can be recorded.

### Non-goals

- Not a market-data (tick/bar) store. Prices live in qkt's tick and bar stores.
- Not a research environment. It produces inputs; qkt-forge and the labs consume them.
- Not a risk engine and not a brake. It informs; it never decides or trades.
- Not a general document store. Unstructured input is admitted only to be reduced to typed,
  versioned records.
- Not a prediction service. Section 2 is explicit that the evidence supports conditioning and
  volatility far more than direction.

---

## 1. Why this exists

### 1.1 The gap

qkt already has a point-in-time path for one shape of data: daily published series, via the
`MACRO:` stream prefix (`FredSeriesFetcher`, `MacroSeriesStore`, `MacroSeriesFeed`,
issue #440). It proved the central mechanism — stamp each value at the instant it became
knowable and merge it into the tick stream — and it has three structural problems this product
removes:

1. **Provider code inside the trading process.** The engine knows FRED's API, polls a third
   party from inside the live loop, and would grow one such client per source.
2. **Two data paths claiming parity.** Live fetches over the network; backtest reads a CSV. The
   CI parity test cannot cover external data because the paths differ.
3. **One shape only.** A release with actual, forecast, previous and surprise is not a scalar.
   A schedule is forward-looking. A holiday is an interval. A filing has an acceptance time
   distinct from its period. None of these fit "one number per day".

### 1.2 The retail-CFD framing

The consumer trades broker CFDs on minute-to-hour horizons on prop-firm and retail accounts.
This has three consequences for what to model first:

- **Venue facts outrank macro facts.** Session hours, holiday early closes, swap and
  triple-swap days, dividend adjustments on index and stock CFDs, energy contract rollovers,
  margin changes around events, and the broker's own measured spread and slippage profile are
  all deterministic, knowable in advance, exactly matched to the account, and bear directly on
  the cost problem that killed the first live book. Most can be produced from data we already
  hold.
- **The tradeable part of an announcement is not the jump.** Tick studies (Andersen, Bollerslev,
  Diebold and Vega 2003) show the FX reaction to a macro surprise completes within minutes; the
  measured stop slippage on XAUUSD around events on our venue is hundreds of points. What a
  retail CFD account can trade is the hours after (volatility regime, continuation, reversal)
  and what it can do best is condition: avoid, size down, size up, switch.
- **Point-in-time discipline is not optional at this horizon.** A five-minute strategy that
  sees a 12:30 UTC release at 12:30:00 in backtest but at 12:31 live is mis-specified by twelve
  bars. Timestamps must be exact to the second and correct across time zones.

### 1.3 Standalone by design

The hub is its own repository, container and release train, in the same family as
`qkt-guardrails` and `qkt-insights`. It has no dependency on qkt code. qkt depends on the hub
only through the record format and the reader contract in Section 8. Guardrails depends on it
only through a file the guardian can read with the standard library. This is the same
isolation posture the fleet already enforces ("engine-independent forever"), applied to data.

---

## 2. Domain model: what strategies can react to, and what the evidence says

This section fixes *what* we model and *why*, so that datasets are added on evidence rather
than availability. Each class states the entity, its natural cadence, its timestamp semantics,
how it revises, the strength of evidence for a price effect at our horizon, and the first
sources. Evidence grades: **strong** (replicated across decades and markets), **moderate**
(documented, regime-dependent or decaying), **weak** (mixed or long-horizon only).

### 2.1 Class A — Scheduled macro releases and their consensus

- **Entity:** one release of one indicator for one jurisdiction (US CPI m/m for August 2026).
- **Cadence:** monthly/weekly/quarterly by indicator; the *schedule* is published months ahead.
- **Timestamps:** `effective_at` = scheduled release instant. The schedule row's `known_at` =
  when we first observed the row. The actual's `known_at` = when we first observed the actual
  filled in (adaptive polling makes this seconds, not hours). `period` = the reference month or
  week.
- **Revision:** consensus drifts up to the release (each drift is a revision); the actual is a
  revision; the source's later corrections are revisions.
- **Evidence:** **strong** that the *surprise* (actual minus consensus) moves FX, gold and
  rates within minutes (ABDV 2003; Roache and Rossi 2010 for gold); **strong** that
  announcement-day volatility is elevated and predictable; **moderate/regime-dependent** for
  direction beyond the first minutes; sign of the equity reaction flips with the policy regime
  (Boyd, Hu and Jagannathan 2005).
- **Horizon fit:** high — the calendar and the surprise are the two things a minutes-to-hours
  strategy most needs.
- **First sources:** ForexFactory weekly JSON (carries `forecast`, `previous`, `actual`;
  **serves only the current rolling week** — verified 2026-09-06 — so an archive must start
  now); BLS and BEA release schedules; Eurostat and ONS calendars.
- **Fields:** `actual, forecast, previous, surprise, surprise_z, revised_previous, unit,
  impact` plus per-scope derived `next_high_at, last_high_at`.

### 2.2 Class B — Published continuous series with vintages

- **Entity:** one series (10y TIPS real yield `DFII10`, Fed funds effective, 2y/10y nominal).
- **Cadence:** daily business days; some weekly/monthly.
- **Timestamps:** `period` = observation date; `known_at` = publication instant. FRED
  publishes most daily yields the next business morning. For revisable series, ALFRED provides
  exact vintage dates.
- **Revision:** yields effectively never; employment, GDP, inflation aggregates revise for
  years. Every vintage is a revision with its own `known_at`.
- **Evidence:** **strong** for real yields as gold's dominant slow driver (Erb and Harvey 2013;
  regime break 2022–24 when central-bank buying decoupled it); **strong** that macro *levels*
  do not forecast FX at short horizons (Meese and Rogoff 1983 and forty years of failure to
  overturn it); PPP mean-reversion half-life of years (Rogoff 1996).
- **Horizon fit:** medium as a *conditioner* (regime, trend of real yields), nil as a signal.
- **First sources:** FRED CSV (keyless, verified) for daily yields; FRED/ALFRED API (free key)
  for vintages. This class is the migration target for today's `MACRO:` path.

### 2.3 Class C — Positioning and flows

- **Entity:** CFTC Commitments of Traders by contract and trader class; ETF holdings (GLD
  ounces); broker retail sentiment.
- **Cadence:** CoT weekly — **describes Tuesday, published Friday 20:30 UTC** (the canonical
  three-timestamp example); ETF holdings daily.
- **Evidence:** **moderate** for open-interest growth predicting commodity returns (Hong and
  Yogo 2012); **weak** for CoT extremes as FX contrarian signals; **weak** for retail sentiment.
- **Horizon fit:** low for entries; useful as a slow regime feature.
- **First sources:** CFTC public reporting API (Socrata, open, verified).

### 2.4 Class D — Inventories and physical supply

- **Entity:** EIA weekly petroleum status (crude, gasoline, distillate stocks); API report;
  Baker Hughes rig count; USDA WASDE; LME/COMEX warehouse stocks.
- **Cadence:** weekly (EIA Wednesday 14:30 UTC; API Tuesday evening), monthly (WASDE).
- **Evidence:** **strong** immediate price response to the inventory surprise against
  consensus (Bu 2014; Ye and Karali 2016); WASDE jumps documented (Adjemian 2012).
- **Horizon fit:** high for energy CFDs if traded.
- **First sources:** EIA API v2 (free key required).

### 2.5 Class E — Central-bank decisions and communication

- **Entity:** a decision (rate, balance-sheet), a statement, minutes, a press conference, a
  speech, dot plots.
- **Cadence:** scheduled meetings (published a year ahead), plus unscheduled speeches.
- **Timestamps:** decision instant; statement publication; presser start/end window.
- **Evidence:** **strong** that the *path* surprise (change in expected future rates read
  from communication) moves 2y yields and FX more than the decision itself (Gürkaynak, Sack
  and Swanson 2005); pre-FOMC equity drift documented then faded (Lucca and Moench 2015);
  statement tone measurable and market-relevant (Hansen and McMahon 2016).
- **Horizon fit:** high for the window (avoid/size) and the post-window regime; moderate for
  text-derived direction.
- **First sources:** Federal Reserve FOMC calendar (open, verified); ECB calendar (open,
  verified); BoE, BoJ, RBA, RBNZ, SNB, BoC calendars. Text via Class I.

### 2.6 Class F — Corporate: earnings, guidance, financial statements, corporate actions

- **Entities:** earnings event (report datetime, pre/post market, consensus EPS and revenue,
  actual, surprise, guidance); periodic filing (10-Q/10-K/annual report) with as-reported
  statement line items; corporate action (dividend ex-date and amount, split, index
  inclusion).
- **Cadence:** quarterly with a dense season; actions ad hoc but announced ahead.
- **Timestamps:** **the filing acceptance timestamp is the true `known_at`** for statement
  data (SEC EDGAR publishes it to the second); the earnings release time is the `known_at` for
  actuals; the fiscal period end is the `period`. Never use the period end as availability.
- **Revision:** restatements, reclassifications, standardisation changes — all revisions.
- **Evidence:** **strong historically** for post-earnings-announcement drift (Ball and Brown
  1968; Bernard and Thomas 1989), **weakening to marginal for large caps** since ~2010
  (Martineau 2021); **strong but cross-sectional and monthly** for statement factors —
  accruals (Sloan 1996), profitability (Novy-Marx 2013), investment — which need hundreds of
  names, not one CFD; earnings-announcement premium and announcement-day vol are reliable.
- **Horizon fit:** high for *event windows* on stock and index CFDs (avoid, size, vol regime);
  low for direction on a single name intraday.
- **First sources:** SEC EDGAR (filing index with acceptance datetime; XBRL Financial
  Statement Data Sets; public domain); company IR calendars; a consensus vendor later.

### 2.7 Class G — Venue facts (highest expected value for this consumer)

- **Entities:** symbol specification (contract size, digits, volume step/min/max, stops
  level); trading session schedule and holiday exceptions; swap rates and triple-swap day;
  dividend adjustments on CFDs; rollover schedule for futures-based CFDs; margin requirement
  changes; measured cost profiles (spread by hour and day-of-week, slippage around events,
  time-to-fill) built from our own tick archive and fill history.
- **Cadence:** specs change rarely; swaps daily; sessions weekly with holiday overrides;
  margin changes announced days ahead; cost profiles recomputed nightly.
- **Timestamps:** `effective_at` = when the fact applies; `known_at` = when the broker
  announced it or when we measured it.
- **Evidence:** not a question of literature — these are the accounting identities of the
  account. The first live post-mortem found the book cost-dominated with all three strategies
  failing the first cost gate; this class is the direct answer.
- **Horizon fit:** highest.
- **First sources:** gateway `/symbol_info` (specs, swaps, sessions); broker announcements
  (declarative HTML/email later); our own tick store and deal history (derived collectors).

### 2.8 Class H — Market-derived regime facts

- **Entities:** realised volatility percentile, trend/choppiness state, cross-asset correlation
  regime, "risk-off" composite — computed by us from prices we hold.
- **Why here and not in the DSL:** the DSL can compute indicators on its own streams, but a
  regime that spans many symbols, uses long histories or expensive estimators (HMM, EGARCH)
  belongs in a batch job with lineage, so every strategy sees the *same* regime label.
- **Evidence:** volatility clustering and regime persistence are among the most robust facts
  in finance; the sign of macro reactions flips with regime (2.1, 2.2).
- **Horizon fit:** high as a gate.
- **Sources:** qkt bar store on bot2; computed nightly; `availability = observed`.

### 2.9 Class I — Unstructured text reduced to typed facts

- **Entities:** headlines, central-bank statements, earnings-call transcripts, broker
  notices.
- **Output:** never text to a strategy. Extractors reduce text to numbers and enums (a
  hawkishness score, a guidance-direction enum, an "unscheduled event" flag) with a frozen
  extractor identity (rule version or model id + prompt hash), an evaluation-set score, and a
  pointer to the raw blob.
- **Evidence:** **moderate** for statement tone and news sentiment (Tetlock 2007; Loughran and
  McDonald 2011); fragile to extractor drift, which is why extractor identity is part of the
  record.
- **Horizon fit:** moderate; treated as Phase 4.

### 2.10 Class J — Hub meta

- `hub.health` (heartbeat, per-dataset freshness), `hub.schema` (registry snapshot), emitted
  as ordinary datasets so consumers gate on hub liveness with the same syntax as everything
  else.

### 2.11 Cross-cutting facts that shape the design

1. **The surprise is the signal, not the level.** Every strong result in 2.1 and 2.4 is
   actual-minus-consensus. Without point-in-time consensus, the research cannot be reproduced.
   Consensus history is the scarce, expensive asset; we start recording it now.
2. **Vintages or leakage.** Revised series encode the future. Every value carries the
   `known_at` of *its* vintage.
3. **Three timestamps per datum**, always: what it describes, when it happened, when we could
   know it.
4. **Time zones are a proven trap.** The one feed we already poll stamps New York offsets;
   DST shifts release hours; government shutdowns reschedule releases.
5. **Volatility is more predictable than direction.** Design consumers to condition and size,
   not to forecast.
6. **Regimes flip signs.** Every dataset that claims a directional effect must be paired with
   a regime dataset (2.8) in research, or the strategy will learn the wrong decade.
7. **Extractor drift is data drift.** For text, the extractor's identity is part of the
   record's identity.

---

## 3. Core data model

### 3.1 The record

A record is the atomic unit. It is a JSON object on one line of a journal, and one row of a
snapshot. It has a **frozen envelope** (owned by this spec, versioned as a whole) and a
**declared payload** (owned by the dataset's schema).

```json
{
  "v": 1,
  "dataset": "macro.us.cpi",
  "scope": "USD",
  "key": "2026-08|USD|CPI m/m",
  "revision": 2,
  "known_at": 1757507401234,
  "effective_at": 1757507400000,
  "period_start": 1754006400000,
  "period_end": 1756684799999,
  "availability": "observed",
  "source": "forexfactory",
  "source_version": "ff_calendar_thisweek.json",
  "parser": "decl/forexfactory@3",
  "raw_ref": "sha256:9f2c…",
  "seq": 48213,
  "id": "sha256:1b7e…",
  "fields": {
    "actual": "0.3",
    "forecast": "0.2",
    "previous": "0.2",
    "surprise": "0.1",
    "surprise_z": "0.83",
    "impact": 3
  }
}
```

#### Envelope fields (frozen; adding a field is a `v` bump)

| Field | Type | Semantics | Invariants |
|---|---|---|---|
| `v` | int | envelope version | `1` |
| `dataset` | string | dataset name (3.5) | must exist in the schema registry |
| `scope` | string | what the record applies to: ISO 4217 currency, `VENUE:SYMBOL`, `ISSUER:<ticker>`, or `ALL` | must match the dataset's `scope_kind` |
| `key` | string | identity of the *thing* across revisions, built per the dataset's `key` rule | deterministic from payload/raw |
| `revision` | int | 1-based, increments per `key` on each changed payload | strictly increasing per key |
| `known_at` | int, epoch ms UTC | when this record became knowable to us | required; never in the future of the writer's clock beyond skew tolerance; non-decreasing within a dataset journal |
| `effective_at` | int, epoch ms UTC | when the fact happens or begins to apply | required; may be in the future (a schedule) |
| `period_start`, `period_end` | int, epoch ms UTC, nullable | the reference period the fact describes | `start <= end` |
| `availability` | enum | `observed` / `published` / `derived` (3.4) | required |
| `source` | string | provider id | registered |
| `source_version` | string | provider-side version/endpoint identity | free |
| `parser` | string | `decl/<name>@<n>` or `code/<module>@<n>` | registered |
| `raw_ref` | string, nullable | content hash of the raw blob this record was parsed from | resolvable in the raw archive |
| `seq` | int | per-dataset writer sequence | strictly increasing per dataset |
| `id` | string | `sha256` over canonical JSON of the record minus `seq` and `id` | idempotency key |
| `fields` | object | payload per schema | every key declared; types match |

#### Payload representation

- **number:** JSON *string* holding a decimal (`"0.3"`, `"-1.25e-3"` not allowed; plain
  decimal only). Consumers parse with arbitrary precision. Floats never appear.
- **bool:** JSON `true`/`false`.
- **timestamp:** int, epoch ms UTC.
- **enum:** int ordinal; the schema maps ordinal to name; consumers may present the name.
- **string:** UTF-8; **never consumed by strategies**; allowed for human fields (`title`)
  and for log/observability surfaces only. A schema marks string fields `strategy: false`.
- `null` is allowed only where the schema's `null_policy` permits; the record is otherwise
  rejected.

### 3.2 The dataset schema

One YAML file per dataset in `datasets/<name>.yaml`. It declares payload fields, key rule,
scope kind, derivations, quality rules, retention, and the collector (Section 4). The schema is
content-hashed; the hash appears in the manifest and in every snapshot header; a consumer
compiles a strategy against a specific schema hash.

```yaml
dataset: macro.us.cpi
version: 1
title: US Consumer Price Index releases
scope_kind: currency          # currency | instrument | issuer | all
key: [period_start, scope, title]
timestamps:
  effective_at: release_time
  period: reference_month
fields:
  title:      { type: string, strategy: false }
  impact:     { type: enum, values: [low, medium, high, holiday] }
  actual:     { type: number, unit: pct, null_policy: allow_until_release }
  forecast:   { type: number, unit: pct, null_policy: allow }
  previous:   { type: number, unit: pct, null_policy: allow }
  surprise:   { type: number, unit: pct, derived: "actual - forecast" }
  surprise_z: { type: number, derived: "zscore(surprise, window=24, min_obs=8)" }
quality:
  range: { actual: [-5, 5], forecast: [-5, 5] }
  monotonic_known_at: true
retention: forever
```

Rules:

- Field names are `[a-z][a-z0-9_]*`, unique, and may not collide with the reserved envelope
  names exposed to consumers (`known_at`, `effective_at`, `revision`, `value`).
- `derived` expressions use a closed, deterministic expression language (arithmetic, `zscore`,
  `pct_rank`, `diff`, `lag`, `ewm`, `since`, `until`) evaluated by the compiler over the
  dataset's own history in `known_at` order. A derived field's inputs must be declared fields
  or envelope timestamps. Derivations are pure and versioned with the schema.
- A single-field dataset may declare `value_alias: <field>` so `.value` resolves for consumers
  (the `MACRO:` compatibility path).
- Changing a field's type or unit is a new dataset version and a new snapshot lineage; adding a
  nullable field is not.

### 3.3 Keys, revisions and deduplication

- `key` identifies the *thing*: the same release, the same series observation, the same
  filing. It is built from declared payload/envelope fields, never from `known_at`.
- Two candidate records with the same `key` and **identical payload** (canonical JSON) are
  duplicates; the later one is dropped before journaling.
- Same `key`, **different payload** → `revision + 1`, new `known_at`. This covers consensus
  drift, actual fill-in, source corrections, restatements and re-parses.
- A re-parse with a new `parser` version over the raw archive never overwrites; it appends
  revisions. The old revisions remain readable and pinnable.
- Consumers reading "as of T" receive, per `key`, the highest revision with `known_at <= T`.

### 3.4 Time semantics and availability classes

Three timestamps per record (3.1). The **visibility predicate is only ever
`known_at <= T`**.

`availability` says how `known_at` was obtained:

| Class | Meaning | Trust |
|---|---|---|
| `observed` | we recorded `known_at` from our own ingest clock at first observation | highest; the default for anything collected live |
| `published` | the source supplies a publication/acceptance timestamp we have validated (ALFRED vintage dates, EDGAR acceptance datetime, BLS release stamp) | high; permitted for backfill |
| `derived` | estimated from a schedule or a fixed lag rule for bootstrapped history (e.g. "FRED daily yields: next business day 13:00 UTC") | lowest; consumers may refuse it |

Rules:

- Live collection always yields `observed`. `derived` is only produced by explicit backfill
  jobs and is never upgraded silently.
- Every dataset declares a `min_lag_ms` floor the compiler adds to `derived` records; a
  consumer policy may add more or refuse the class outright.
- All timestamps are UTC epoch ms. Source timestamps with offsets are converted at parse time;
  **naive timestamps are rejected**. DST is handled by converting from the source's named zone,
  never by adding a fixed offset.
- Rescheduled events produce a revision of the schedule record with a new `effective_at`; the
  old `effective_at` is retained in history.
- Writer clock skew: `known_at` may not exceed the writer's clock by more than a configured
  tolerance (default 5 s); violations quarantine the record.

### 3.5 Naming and scopes

Dataset names: `<domain>.<region-or-venue>.<subject>[.<qualifier>]`, lowercase, segments
`[a-z0-9_]+`, dot-separated, 2–5 segments. Domains are a closed registry:

`macro` `cal` `rates` `cb` `pos` `inv` `corp` `venue` `mkt` `text` `hub`

Examples: `macro.us.cpi`, `cal.high_impact` (scope-parametric by currency), `rates.us.dfii10`,
`cb.fed.fomc`, `pos.cftc.cot_gold`, `inv.us.eia_crude`, `corp.us.earnings`,
`venue.exness.swaps`, `venue.exness.sessions`, `mkt.regime.vol_pctile`, `hub.health`.

**Scope resolution.** `scopes.yaml` maps each venue instrument to the scopes it is exposed
to, so a consumer can ask "everything relevant to `EXNESS:XAUUSD`" and receive `USD`, `XAU`,
`metals`, `ALL`. This map is versioned data with its own dataset (`hub.scopes`) so a strategy's
`SCOPE AUTO` binding is reproducible.

---

## 4. The pipeline

Every dataset flows through the same stages. Each stage has a declared input, output,
invariants, failure handling and tests. Stages are pure functions of their inputs except
Acquire (I/O) and Journal (append). This is what makes the pipeline replayable: re-running
stages 2–7 over the raw archive must reproduce the journal's payloads exactly.

```
 Acquire → Parse → Clean → Normalise → Validate → Deduplicate → Derive → Journal → Compile → Serve
   (I/O)   pure    pure      pure       pure        pure        pure    append    pure     read
             ↓ raw archive (content-addressed, forever)                     ↓ snapshot + manifest
```

### 4.1 Acquire

- **Collectors** are either **declarative** (a YAML mapping for HTTP JSON/CSV/HTML-table
  endpoints and for files on disk) or **code** (a Python class implementing
  `fetch(timeout) -> RawBlob | None`). Both produce a `RawBlob` (bytes + content type +
  fetched-at + request identity), nothing else.
- **Cadence** per collector: `cadence`, optional `cadence_near_event` (window and interval)
  so an actual is observed within a minute of release, optional cron for scheduled
  publications, jitter to avoid synchronised bursts.
- **Backoff** per source: steady interval on success, retry interval on failure, exponential
  to a cap, circuit breaker after N consecutive failures with an alert. One dead source never
  blocks another (one thread or task per collector; a bounded worker pool).
- **Raw archive:** every blob is stored content-addressed under `raw/<source>/<sha256[0:2]>/
  <sha256>` with a sidecar of request metadata, before parsing begins. Identical blobs are not
  stored twice. Retention: forever, compressed.
- **Politeness:** per-host rate limits, a distinct User-Agent, conditional requests
  (ETag/If-Modified-Since) where supported, and licence notes per source (Appendix C).
- **Failure:** network/HTTP errors → backoff; a 2xx with an unexpected content type → archive,
  quarantine, alert (this is how the ForexFactory "HTML instead of JSON" failure was caught in
  the guardian).

### 4.2 Parse

- Input: `RawBlob` + parser identity. Output: a list of **candidate records** (envelope
  partially filled, payload as raw strings).
- Declarative parsers select records (`record_path`, JSONPath or CSV row), filter
  (`where`), and map raw fields to candidate fields with named parse functions
  (`percent_or_number`, `human_number` for `1.2K/3.4M/5B`, `iso8601_with_offset`,
  `date_in_zone(America/New_York)`, `enum(map)`).
- Code parsers are functions `parse(blob) -> list[Candidate]`; they may not do I/O.
- A parser failure on one record skips that record and emits a quarantine entry; a failure to
  parse the blob at all quarantines the blob. Parsers never raise into the scheduler.
- Parser identity `decl/<name>@<n>` or `code/<module>@<n>` is stamped on every candidate.

### 4.3 Clean

Deterministic string-level hygiene before typing: trim, Unicode NFC normalisation, remove
thousands separators per declared locale, map sentinels (`-`, `n/a`, `—`, empty) to null,
strip units that the schema already declares (`%`, `K`, `bps`), collapse whitespace in titles.
Cleaning rules are named and versioned with the parser.

### 4.4 Normalise

- **Units:** convert to the field's declared canonical unit (`pct` stays percent points, not
  fractions; `bps` to percent where declared; thousands to units). The conversion is declared,
  not inferred.
- **Scopes:** map provider codes to canonical scopes (`country: "USD"` → `USD`;
  `"All"` → `ALL`; venue symbols through `scopes.yaml`).
- **Timestamps:** to epoch ms UTC from the declared zone or offset; reject naive.
- **Symbols:** venue symbol normalisation (`XAUUSDm` ↔ `EXNESS:XAUUSD`) uses the same
  suffix rules qkt's broker profiles use, declared once in `scopes.yaml`.
- **Numbers:** to canonical decimal strings (no exponent, no leading `+`, `-0` → `0`).

### 4.5 Validate

Applied to every record before journaling:

1. Envelope completeness and types (3.1).
2. Payload matches schema: declared fields only, types, enums, null policy.
3. Quality rules: declared ranges, allowed scopes, `effective_at` plausibility (within
   ±N years), `period_start <= period_end`.
4. Temporal invariants: `known_at` non-decreasing within the dataset journal (a late arrival
   with an earlier `known_at` is accepted only when `availability != observed` and logged);
   `known_at` not beyond writer clock + tolerance.
5. Availability consistency: live collection must be `observed`.

Violations go to `quarantine/<dataset>/<date>.ndjson` with the reason and the candidate; a
counter and an alert threshold per dataset make silent decay impossible.

### 4.6 Deduplicate

Per 3.3. Implemented with a per-dataset index of `(key → latest revision, payload hash)` kept
in a small local SQLite database rebuilt from the journal on start. Exact duplicates are
dropped and counted; changed payloads become revisions.

### 4.7 Derive

- Derived fields (3.2) are computed here for **live** records using the dataset's history
  in `known_at` order, and recomputed identically by the compiler for **snapshots**. Both
  use the same library function; a test asserts equality.
- Cross-dataset derivations (e.g. `cal.high_impact.next_high_at` per scope) are their own
  datasets produced by **derived collectors**: batch jobs whose inputs are other datasets'
  snapshots (pinned by hash) and whose outputs carry `availability = observed` and a
  `raw_ref` pointing at the input manifest.
- Market-derived regimes (2.8) and venue cost profiles (2.7) are derived collectors over the
  qkt bar/tick stores; their input coverage is recorded in the record's provenance.

### 4.8 Journal

- Layout: `journal/<dataset>/<YYYY-MM-DD>.ndjson`, dated by `known_at`, one JSON record per
  line, UTF-8, `\n` terminated, **append-only**.
- Exactly one writer process per hub root. It holds an exclusive lock file; a second writer
  refuses to start.
- Writes are line-atomic (single `write` of the whole line), followed by `fsync` at a
  configurable cadence (default every write for `cal`/`venue`, every second otherwise).
- `seq` is assigned at write time and persisted; a restart resumes from the last `seq` per
  dataset by reading the tail of the current file.
- Files are never rewritten. A bad line discovered later is superseded by a new revision.
- Readers detect partial trailing lines (crash mid-write) by the absence of the terminating
  newline and ignore them until completed.

### 4.9 Compile

- Input: a dataset's journal files over a range, its schema. Output: a **snapshot** file
  `snapshot/<dataset>/<schema_hash[0:8]>/<from>_<to>.qkh` plus an entry in `manifest.json`.
- Sorted by `(known_at, scope, key, revision, seq)`. Derived fields recomputed and verified
  against journaled values.
- **Deterministic:** compiling the same journal lines in any order yields a byte-identical
  file; the SHA-256 of the file is its identity. A CI test shuffles input lines and asserts
  hash equality.
- Snapshot windows are calendar-aligned (monthly by default; yearly for slow datasets) so a
  backtest range maps to a fixed set of files; the manifest lists coverage per window.
- The manifest records, per dataset: schema hash, windows with file hashes, coverage
  `[from, to]`, record counts, last `known_at`, last `seq`, and the raw-archive hashes the
  window depends on (lineage).

### 4.10 Serve

Three transports over one log (Section 8):

1. **Local tail** (default, same host): consumers mount `hub_root` read-only and tail
   `journal/` with a per-dataset `seq` cursor; historical reads open `snapshot/`.
2. **Stream server** (cross-host): the hub serves `GET /stream/<dataset>?from_seq=N` as
   server-sent events emitting the same records with the same `seq`, so a remote consumer can
   resume from any offset; `GET /snapshot/<dataset>/<window>` serves the same bytes as the
   file.
3. **Query API** (humans, dashboards, research): `GET /as_of?dataset=&scope=&t=` and
   `GET /records?dataset=&from=&to=&scope=` over snapshots; read-only.

All bind to loopback by default and are exposed only through the same tunnel/HTTPS conventions
the fleet uses.

### 4.11 Observe

- Metrics per dataset: records/day, revisions/day, quarantine/day, last `known_at` age,
  collector latency, backoff state.
- `hub.health` dataset (2.10) emitted every 60 s; `hub.schema` on any registry change.
- Envelopes to qkt-insights `/ingest` (`hub.health`, `hub.dataset.stale`,
  `hub.collector.failed`, `hub.quarantine.threshold`) using the existing `v:1` contract with
  a hub `instanceId`.
- Telegram alerts through the same bot/chat the fleet uses for: source down past threshold,
  quarantine spike, writer lock conflict, snapshot hash mismatch, disk pressure.

---

## 5. Storage layout and formats

```
<hub_root>/
  hub.yaml                      # runtime config (paths, server, policy defaults)
  datasets/<name>.yaml          # dataset schemas + collectors (registry)
  scopes.yaml                   # instrument -> scopes map (also emitted as hub.scopes)
  raw/<source>/<aa>/<sha256>    # content-addressed raw blobs (+ .meta.json)
  journal/<dataset>/<YYYY-MM-DD>.ndjson
  quarantine/<dataset>/<YYYY-MM-DD>.ndjson
  snapshot/<dataset>/<schema8>/<from>_<to>.qkh
  manifest.json                 # datasets, schema hashes, windows, file hashes, coverage
  heartbeat                     # touched every 10 s by the writer
  state/                        # dedupe index sqlite, collector cursors, seq cursors
```

### 5.1 Snapshot binary `QKH1` (qkt-hub-snapshot-v1)

Little-endian; mirrors the QKT1/QKB1 conventions so existing readers are a template. The body is
**genuinely columnar**: one contiguous block per column, every value of that column for every
record, in sorted record order. That is what makes a range scan over `known_at` a single
contiguous read and lets a future non-Python reader memory-map one column without touching the
rest.

```
header  MAGIC "QKH1" (4) | version i32 (=1) | schema_hash bytes[32]
        | dataset_len i32 | dataset utf8
        | record_count i32 | field_count i32 | scale i32 (=8)
        | field table: per field { name_len i32, name utf8, type u8, unit_len i32, unit utf8 }
        | dictionaries, each: count i32, then { len i32, utf8 } repeated
            scope | key | source | source_version | parser | raw_ref
body    columnar -- record_count values per column, in sorted record order:
        known_at i64 | effective_at i64 | period_start i64* | period_end i64*
        | scope_idx i32 | key_idx i32 | revision i32 | availability u8 | seq i64
        | source_idx i32 | source_version_idx i32 | parser_idx i32 | raw_ref_idx i32
        | then one i64 column per field, in field-table order
        * and absent numbers use NULL_SENTINEL = i64.MIN
trailer sha256 of everything above (32)
```

**Provenance is part of the snapshot, not just the journal.** `source`, `source_version`,
`parser` and `raw_ref` are dictionary-encoded exactly as `scope` and `key` are. They are
low-cardinality -- a dataset has one or two sources and a handful of parser versions -- so the
cost is a few bytes per record, and without them a record read from a snapshot would not equal
the same record read from the journal. That equality is the replay-equivalence property
(Section 11) and the lineage the determinism contract (Section 6, rule 5) requires; a backtest
cites a snapshot, so a snapshot that cannot say where its facts came from is not auditable.
A null `raw_ref` is encoded as dictionary index `-1`.

Strings (`strategy: false` fields) are not stored in snapshots; they live in the journal only.
Exact size validation (`len == header + body + 32`) is mandatory in every reader.

### 5.2 Manifest (excerpt)

```json
{
  "v": 1,
  "generated_at": 1757520000000,
  "datasets": {
    "macro.us.cpi": {
      "schema_hash": "sha256:…",
      "schema_path": "datasets/macro.us.cpi.yaml",
      "scope_kind": "currency",
      "last_seq": 48213,
      "last_known_at": 1757507401234,
      "windows": [
        {"from": "2026-09-01", "to": "2026-09-30", "file": "snapshot/macro.us.cpi/1b7e9a02/2026-09-01_2026-09-30.qkh",
         "sha256": "…", "records": 812, "raw_deps": ["sha256:…"]}
      ]
    }
  }
}
```

### 5.3 Retention

Raw and journals: forever. Snapshots: all windows referenced by any pinned run (qkt evidence
records cite snapshot hashes), plus the last two compilations of every window. Quarantine:
one year. State: rebuildable, not backed up.

---

## 6. Determinism and reproducibility contract

1. **Append-only everywhere.** No file under `raw/`, `journal/`, `snapshot/` is ever modified.
2. **`known_at` is recorded, not inferred**, whenever collection is live. Inference is
   confined to explicit backfill and labelled `derived`.
3. **Pure stages.** Parse→Derive are functions of (raw blob, schema, parser version, prior
   history). Re-running them reproduces payloads exactly.
4. **Deterministic ordering.** `(known_at, scope, key, revision, seq)` everywhere.
5. **Content addressing.** Raw blobs, snapshots, schemas and the manifest are identified by
   SHA-256; every record links to its raw blob; every snapshot links to its raw dependencies.
6. **Unknown is explicit.** Null is a declared state; consumers see it as undefined, never
   zero.
7. **Extractor identity is data.** For text-derived fields, model id, prompt hash and
   evaluation-set score are part of the parser identity.
8. **A consumer names what it read.** The qkt evidence record carries the manifest hash and
   the per-dataset snapshot hashes; a research result without them is not reproducible and
   the funnel treats it as such.

Tests that pin the contract (Section 11): shuffle-and-hash, replay-equivalence, look-ahead
property, vintage isolation, cross-language reader golden files.

---

## 7. Extending the hub

Adding a dataset touches this repository only. The three cases, from least to most work:

### 7.1 Declarative source (most feeds)

One file:

```yaml
dataset: cal.high_impact
version: 1
scope_kind: currency
key: [effective_at, scope, title]
collector:
  kind: http_json
  url: https://nfs.faireconomy.media/ff_calendar_thisweek.json
  headers: { User-Agent: "qkt-data-hub/0.1 (+ops contact)" }
  cadence: 6h
  cadence_near_event: { before: 15m, after: 30m, every: 30s, anchor: effective_at }
  record_path: "$[*]"
  where: { impact: [High, Holiday] }
  scope: { from: country, map: { All: ALL } }
  effective_at: { from: date, parse: iso8601_with_offset }
  known_at: ingest_time
  fields:
    title:    { from: title }
    impact:   { from: impact, parse: enum, map: { Low: 0, Medium: 1, High: 2, Holiday: 3 } }
    forecast: { from: forecast, parse: percent_or_number, null_if: [""] }
    previous: { from: previous, parse: percent_or_number, null_if: [""] }
    actual:   { from: actual,   parse: percent_or_number, null_if: ["", null] }
fields:
  title:    { type: string, strategy: false }
  impact:   { type: enum, values: [low, medium, high, holiday] }
  forecast: { type: number, unit: raw, null_policy: allow }
  previous: { type: number, unit: raw, null_policy: allow }
  actual:   { type: number, unit: raw, null_policy: allow_until_release }
  surprise: { type: number, unit: raw, derived: "actual - forecast" }
quality:
  monotonic_known_at: true
```

Then `hub validate datasets/cal.high_impact.yaml` (schema + a dry run against the live
endpoint or a fixture), `hub enable cal.high_impact`. The manifest updates; consumers can bind.

### 7.2 Code collector or parser (anything a mapping cannot express)

A module under `collectors/<source>/` exposing `fetch()` and/or `parse()` with the signatures
in 4.1/4.2, registered in the dataset YAML as `collector: { kind: code, module:
collectors.edgar.filings }`. Fixture-driven tests are mandatory (captured raw blob → expected
records). Nothing else changes.

### 7.3 Derived dataset

A batch job declared as `collector: { kind: derived, inputs: [macro.us.cpi@schema8, …],
schedule: "0 1 * * *" }` with a pure `compute(inputs) -> records` function. Inputs are
resolved to pinned snapshot hashes at run time and recorded in provenance.

### 7.4 New *kind* of data

A new domain segment (3.5), possibly a new parse function or field type. Field types are
closed (number, bool, timestamp, enum, string); a genuinely new type is an envelope `v`
bump and a reader change in every language, so it is a deliberate, rare event. Everything in
Section 2 fits the existing types.

---

## 8. Consumer contract

### 8.1 Reader semantics (transport-independent)

```
Reader.datasets() -> manifest view
Reader.schema(dataset) -> schema + hash
Reader.records(dataset, from_seq) -> iterator of records in seq order   # live tail
Reader.range(dataset, known_from, known_to) -> iterator in sorted order  # historical
Reader.as_of(dataset, scope, t) -> latest revision per key with known_at <= t
Reader.windows(dataset, scope, t, ahead) -> intervals [effective_at ± pad] for events
```

Policy applied inside the reader, declared by the consumer: `min_lag_ms`, `refuse_derived`,
`stale_after_ms` (compares heartbeat age; on stale the reader keeps last-known values and
raises a health flag; it never blanks).

### 8.2 qkt (engine) — summary of the companion spec

- New stream prefix `HUB:` bound in `SYMBOLS`; a `MarketSource` answering to the prefix:
  historical path reads snapshots (verifying hashes) and feeds the existing k-way merge; live
  path is a `LiveTickSource` tailing the journal on one thread per process, feeding the existing
  live tick queue → inbound queue → engine thread. **No new engine queue.**
- One hub record becomes one tick per field, symbol `HUB:<dataset>/<field>` (plus envelope
  streams `known_at`, `effective_at`, `revision`), all stamped `known_at`, published as
  immediately-closed event candles (the mechanism `MACRO:` uses today, generalised from a
  hard-coded prefix to a stream-kind property in both the candle hub and the price-validity
  gate).
- The compiler expands a `HUB:` alias into hidden per-field streams from the schema in the
  manifest; unknown field → compile error; `.value` for single-field datasets.
- Config `hub:` block: `root`, `policy`, `datasets` — **never** field or parse definitions;
  added to strict unknown-key validation.
- Evidence record gains `hub_manifest_sha256` and per-dataset snapshot hashes.
- Documented inherent divergence: live visibility lags `known_at` by tail latency
  (sub-second); added to the parity catalogue.
- Known DSL semantics consumers rely on: undefined propagates with three-valued logic, rules
  fire only on true, `IS NULL` is the non-propagating test; exits gated on hub fields must
  carry an `IS NULL` fallback.

Example binding:

```
SYMBOLS
    gold = EXNESS:XAUUSD EVERY 5m
    cpi  = HUB:macro.us.cpi EVERY 1d
    cal  = HUB:cal.high_impact.USD EVERY 1d
    hub  = HUB:hub.health EVERY 1m

RULES
    WHEN NOW.epoch_ms - hub.last_heartbeat_at > 900000 AND POSITION.gold != 0
    THEN CLOSE gold ; LOG "hub stale"

    WHEN cal.next_high_at - NOW.epoch_ms < 1800000 AND POSITION.gold != 0
    THEN CLOSE gold

    WHEN cpi.surprise_z > 1.0 AND cpi.surprise IS NOT NULL
     AND NOW.epoch_ms - cal.last_high_at > 900000 AND POSITION.gold = 0
    THEN SELL gold SIZING 0.5 PCT RISK
```

(Open: the lexer's acceptance of dotted dataset names in the symbol position must be checked;
fallback is underscores or a quoted string — a parser decision, not an architectural one.)

### 8.3 qkt-guardrails

A `JournalCalendarSource` implementing the guardian's existing `Source` protocol reads
`journal/cal.high_impact/` with the standard library, converts records to its `Event`
windows, and **returns `None` when the heartbeat is older than its threshold** so the cache
keeps last-known windows instead of unarming the NEWS rung. This removes the third-party CDN
from the brake path. The hub is explicitly a non-engine writer; the guardian's
"engine-independent" rule is preserved.

### 8.4 qkt-insights

Receives hub health/failure envelopes over the existing `/ingest` contract; may later
subscribe to `/stream` to annotate charts with releases.

### 8.5 Reader libraries

Reference implementations against shared golden files: Python (stdlib only, used by the hub's
own tests, guardrails and the labs), Kotlin (inside qkt), TypeScript (insights, optional). The
golden set is a fixed journal + snapshot + manifest; each reader must reproduce the same
`as_of` answers for a table of `(dataset, scope, t)` queries.

---

## 9. Unstructured and corporate pipelines (Phase 3–4 detail)

### 9.1 Text → typed facts

```
raw blob → segment (document, section) → extract (rules | model) → candidate fields
        → validate against schema → journal with parser = code/<extractor>@<n> {model_id, prompt_hash}
```

- **Rules first.** Numeric releases (BLS text, EIA summaries) are regex/grammar extractions;
  they are deterministic and cheap. Models are used only where rules cannot express the
  target (tone, guidance direction).
- **Frozen models.** A model-based extractor pins model id, prompt hash, temperature 0 and a
  decoding seed where available; any change is a new parser version and a new revision
  lineage. Model outputs are `observed` at the time we produced them, and `effective_at` is
  the document's publication time.
- **Evaluation set.** Every extractor ships with a labelled set and a score; the score is
  recorded in the parser registry and surfaced in `hub.schema`. Extractors below threshold are
  not enabled for strategy-visible fields.
- **Human-in-loop** is an ordinary revision with `source = review`.

### 9.2 Corporate data

- Timestamps: `period_end` = fiscal period end; `effective_at` = report/filing publication;
  `known_at` = EDGAR acceptance datetime (`published`) or our observation (`observed`).
- As-reported line items are stored as-reported, keyed by `(issuer, period_end, statement,
  line_item, fiscal_year_variant)`; standardisation is a derived dataset with its own version.
- Restatements are revisions of the same key; the pre-restatement value remains what a
  strategy would have seen at the time.
- Consensus (EPS, revenue) is a separate dataset with drift revisions, exactly like Class A.
- Corporate actions carry `effective_at` = ex-date and are the source for venue dividend
  adjustment expectations on stock/index CFDs.

---

## 10. Scale, operations and security

### 10.1 Volumes (order of magnitude, for sizing not for limits)

| Class | Records/day | Notes |
|---|---|---|
| Calendar + consensus drift | 10² | more revisions during a dense week |
| Continuous series | 10² | ~50 series |
| Positioning/inventories | 10¹ | weekly |
| Venue facts, static/daily | 10² | swaps, sessions across ~100 symbols |
| Venue cost profiles, hourly | 10³ | ~100 symbols × 24 |
| Corporate (US large caps) | 10³ in season | filings + consensus |
| Text-derived | 10² | |
| Hub health | 10³ | 60 s cadence |

Total well under one record per second sustained. The engine already ingests thousands of
market ticks per second through the same inbound path; transport is not the bottleneck. Storage
is dominated by raw blobs (compressed HTML/JSON), tens of GB per year at most.

### 10.2 Runtime

- Python 3.12. Hub core dependencies kept minimal (YAML, HTTP client); **readers are
  stdlib-only**. Type-checked (`mypy --strict`), linted (`ruff`), tested (`pytest`) in CI;
  Docker image published to GHCR on tag; pinned tags on the bots, never `latest`.
- One container: scheduler + collectors + writer + compiler + servers. Single writer per
  `hub_root` enforced by a lock. Horizontal scale, if ever needed, is by sharding datasets
  across hub instances with distinct roots, not by multiple writers to one root.
- Deployment joins the bots' compose files: a `qkt-data-hub` service with the `hub_root`
  volume mounted read-only into `qkt` and each guardian; loopback-bound ports; the same
  `${VAR:?}` env conventions; GH Actions vars rendered to `.env`.
- Multi-host: bot1 and bot2 each run a hub (identical config, deterministic outputs) **or** one
  hub serves the other over the stream transport. Default: one hub per host for independence,
  with a nightly cross-check that both manifests agree on snapshot hashes; disagreement is an
  alert, and it is the test that the pipeline is truly deterministic.
- Backfill jobs are separate CLI invocations that write `derived`/`published` records under an
  explicit `--backfill` flag; they cannot run concurrently with the live writer for the same
  dataset.

### 10.3 Security and trust

- The hub holds **no broker credentials** and no path to the gateway. It cannot trade.
- Provider API keys via environment only; never in records, raw metadata or logs.
- Consumers mount `hub_root` read-only. The stream and query APIs are read-only and
  loopback-bound; if exposed, behind the fleet's HTTPS/password conventions with a bearer token
  per consumer (fixing the single-shared-token pattern the insights ingest uses today).
- Licence register per source (Appendix C). Sources whose terms forbid redistribution are
  served only to our own consumers on the same host and never over the public stream.
- Supply chain: pinned dependencies, lockfile, image digest pinning on the bots.

---

## 11. Testing strategy

| Layer | Test | Pins |
|---|---|---|
| Parsers | fixture blobs → expected candidates (one per source, plus malformed variants) | parse correctness, tz handling, sentinel handling |
| Normalise/Validate | property tests: random valid records survive; every declared violation quarantines | schema enforcement |
| Dedup/Revision | sequence of candidates → expected revisions | 3.3 rules |
| Derive | live path vs compiler path equality on the same history | one implementation |
| Journal | crash mid-line → reader ignores partial; restart resumes `seq` | durability |
| Compile | shuffle lines → identical sha256; recompile → identical | determinism |
| Look-ahead | for random `t`, every record from `as_of(t)` has `known_at <= t`; vintage test: revised value invisible before its `known_at` | the correctness property |
| Replay equivalence | tail a day live, record visibility sequence; read the same day's snapshot; sequences equal | parity |
| Readers | Python/Kotlin/TS against golden files | cross-language contract |
| Collectors | live smoke against each source with a strict content-type check (CI, allowed to skip on network failure but never to pass on wrong content) | source drift |
| Operations | heartbeat stale → `hub.health` reflects; guardian source returns `None` | fail-closed behaviour |

---

## 12. Delivery plan

**Phase 0 — this week, before any code in this repo.** Start archiving the ForexFactory
weekly JSON on every guardian poll to a dated file on bot1. It is the only free source of
consensus we have, it serves only the current week, and every unarchived week is permanently
lost. Zero risk; a few lines in the existing fetch path or a cron beside it.

**Phase 1 — foundation + venue facts + calendar (4–6 weeks).** Envelope, schema registry,
declarative collector engine, clean/normalise/validate/dedup, journal writer, compiler,
manifest, local-tail reader (Python), `hub.health`. Datasets: `cal.high_impact`,
`venue.<broker>.specs|swaps|sessions`, `venue.<broker>.cost_profile` (derived from our tick
store), `hub.scopes`. Guardian `JournalCalendarSource`. Companion qkt spec written and the
`HUB:` binding implemented for numeric fields.

**Phase 2 — macro migration (2–3 weeks).** `rates.us.*` from FRED CSV/API with `published`
vintages where available; `MACRO:` becomes an alias; FRED code removed from qkt. CFTC CoT.
Stream server for cross-host.

**Phase 3 — corporate (4–6 weeks).** EDGAR filings index with acceptance timestamps; earnings
calendar; corporate actions; venue dividend-adjustment expectations. First index/stock CFD
consumers.

**Phase 4 — text (ongoing).** FOMC/ECB statement extractors (rules first), headline flags,
transcript tone with frozen models and evaluation sets.

Each phase ends with the determinism tests green, a drill of the failure modes in 4.1/4.5/8.3,
and a research note in qkt-research-lab that uses the new datasets through the funnel — as
gates with a pre-registered hypothesis, never as a feature pool for the optimiser (Section
2.11, point 6, and the first live post-mortem).

---

## 13. Risks and open questions

1. **Consensus history.** Free archives do not exist; our own archive starts at Phase 0. Any
   research on surprises before that date must either buy history or be labelled as using
   `derived` consensus. Decision needed on a vendor by Phase 2.
2. **ForexFactory terms.** The feed is unofficial. Mitigation: archive for our own use, polite
   cadence, and evaluate a licensed calendar (Econoday, Trading Economics, FXStreet) for
   Phase 2.
3. **Dotted names in the DSL** (8.2). Parser decision pending.
4. **Strings to strategies.** Deliberately excluded. If a real need appears, enums are the
   answer, not strings.
5. **Two hubs vs one** for bot1/bot2. Default is two with a cross-check; revisit when a third
   host appears.
6. **Model-based extractors and determinism.** Vendor models change under the same name; only
   pinned open models or snapshot-dated APIs are acceptable for strategy-visible fields.
7. **Over-fitting pressure.** The hub makes it cheap to add features. The funnel's rules on
   pre-registration and single tests are the only defence and must be enforced in
   qkt-research-lab, not here.

---

## Appendix A — Initial dataset catalogue

| Dataset | Class | Scope kind | Cadence | Source | Availability | Phase |
|---|---|---|---|---|---|---|
| `cal.high_impact` | A | currency | 6h + near-event 30s | ForexFactory JSON | observed | 1 |
| `cal.holidays` | A/G | currency | daily | ForexFactory (Holiday rows), venue notices | observed | 1 |
| `venue.exness.specs` | G | instrument | daily | gateway `/symbol_info` | observed | 1 |
| `venue.exness.swaps` | G | instrument | daily | gateway `/symbol_info` | observed | 1 |
| `venue.exness.sessions` | G | instrument | weekly + overrides | gateway + notices | observed | 1 |
| `venue.exness.cost_profile` | G | instrument | nightly | derived from tick store + deals | observed | 1 |
| `venue.the5ers_hs.*` | G | instrument | as above | second venue | observed | 1 |
| `hub.health`, `hub.schema`, `hub.scopes` | J | all | 60s / on change | hub | observed | 1 |
| `rates.us.dgs2`, `dgs10`, `dfii10`, `fedfunds` | B | currency | daily | FRED CSV/API | published/derived | 2 |
| `cb.fed.fomc`, `cb.ecb.gc` | E | currency | per meeting | Fed/ECB calendars | observed | 2 |
| `pos.cftc.cot_<contract>` | C | instrument | weekly | CFTC Socrata | published | 2 |
| `inv.us.eia_petroleum` | D | instrument | weekly | EIA v2 | observed | 2 |
| `mkt.regime.vol_pctile`, `mkt.regime.trend_state` | H | instrument | nightly | derived from bar store | observed | 2 |
| `corp.us.filings` | F | issuer | continuous | EDGAR | published | 3 |
| `corp.us.earnings` | F | issuer | per event | EDGAR 8-K + IR calendars (+ vendor consensus) | observed | 3 |
| `corp.us.actions` | F | issuer | per event | EDGAR/vendor | observed | 3 |
| `text.cb.fed_statement_tone` | I | currency | per statement | derived from `cb.fed.fomc` docs | observed | 4 |

## Appendix B — Source reachability, checked 2026-09-06 from the workstation

| Source | Result |
|---|---|
| ForexFactory `ff_calendar_thisweek.json` | 200; fields `country,date,forecast,impact,previous,title` (+`actual` when filled); **only `thisweek` exists** — `lastweek`, `nextweek`, `thismonth` return 404 |
| CFTC public reporting (Socrata) | 200, open |
| Federal Reserve FOMC calendar | 200, open |
| ECB calendar | 200, open |
| FRED `fredgraph.csv?id=DFII10` | works keyless, returns current daily values |
| FRED / ALFRED JSON API | 400 without key (free key) |
| EIA API v2 | 403 without key (free key) |
| BLS schedule pages | 403 to the default agent string |

## Appendix C — Licence register (to be completed before enabling each source)

| Source | Terms summary | Redistribution | Attribution |
|---|---|---|---|
| ForexFactory JSON | unofficial feed; ToS restricts scraping/redistribution | internal only | n/a |
| FRED/ALFRED | free with key; attribution required; some series third-party | internal | "Source: FRED" |
| CFTC | US Government work, public domain | yes | courtesy |
| EIA | public domain | yes | courtesy |
| SEC EDGAR | public domain; fair-access rate limits; declared User-Agent required | yes | n/a |
| Fed / ECB sites | public information; terms per site | internal | n/a |

## Appendix D — Glossary

- **known_at** — the instant a record became knowable to us; the only field the visibility
  predicate reads.
- **effective_at** — when the fact happens or begins to apply.
- **period** — the reference interval a fact describes.
- **revision** — a later record for the same key with a changed payload.
- **availability** — how `known_at` was obtained: observed, published, derived.
- **snapshot** — a compiled, sorted, content-hashed binary of a dataset window.
- **manifest** — the registry of datasets, schema hashes, windows and coverage.
- **derived collector** — a batch job whose inputs are pinned snapshots of other datasets.
- **scope** — the currency, instrument, issuer or ALL a record applies to.
