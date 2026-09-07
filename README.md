<h1 align="center">qkt-data-hub</h1>

<h3 align="center">A point-in-time fact store for trading systems. One record format, and the same bytes whether you tail it live or read it back as history.</h3>

<p align="center">
  <a href="https://github.com/elitekaycy/qkt-data-hub/actions/workflows/ci.yml"><img src="https://github.com/elitekaycy/qkt-data-hub/actions/workflows/ci.yml/badge.svg" alt="ci"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-blue.svg" alt="license"></a>
  <a href="https://github.com/elitekaycy/qkt-data-hub/pkgs/container/qkt-data-hub"><img src="https://img.shields.io/badge/ghcr.io-qkt--data--hub-2496ED?logo=docker&logoColor=white" alt="container"></a>
  <img src="https://img.shields.io/badge/python-3.12%20stdlib%20only-3776AB?logo=python&logoColor=white" alt="python">
</p>

---

> Sibling project of [qkt](https://github.com/elitekaycy/qkt), [qkt-insights](https://github.com/elitekaycy/qkt-insights) and [qkt-guardrails](https://github.com/elitekaycy/qkt-guardrails) — same brand, same engineering style.

A backtest that reads today's revised numbers is not a backtest. It is a report on what you would
have done if you had known things you did not know, and it looks like signal right up until it
costs money. The usual fixes are worse: a second "historical" pipeline that drifts from the live
one, or a spreadsheet of numbers nobody can trace back to a source.

**qkt-data-hub is one store with one rule.** Every fact carries the instant it became knowable,
and a consumer standing at time `T` sees only what satisfies `known_at <= T`. Live and history are
the same bytes: the journal a collector appends to is the input the snapshot compiler folds, so a
strategy in backtest and the same strategy in production read one format, not two pipelines.

```
                    collectors (I/O only, one per source)
                              |
   raw/  <-- every byte kept, content-addressed, forever
                              |
   parse -> clean -> normalise -> validate -> deduplicate -> derive     (pure, replayable)
                              |
   journal/<dataset>/<day>.ndjson        append-only, one writer, seq-ordered
                              |
   compile (deterministic)  ->  snapshot/<dataset>/<schema>/<window>.qkh + manifest.json
                              |
        +---------------------+----------------------+
        |                     |                      |
   qkt engine            qkt-guardrails         anything else
   HUB: streams          stdlib file read       stream / query API
   backtest + live       no network in the brake
```

## Why it looks like this

- **Three timestamps, never two.** `period` is what a fact describes, `effective_at` is when it
  happens, `known_at` is when we could know it. Keeping them apart is what lets a strategy
  legitimately see a release scheduled for Friday without also seeing Friday's number.
- **`known_at` is recorded, not guessed.** Live collection stamps it from the ingest clock. A
  backfilled record is stamped `derived` and a strict consumer can refuse it outright.
- **Append only.** A correction is a new revision of the same key, so what you would have seen
  last month is still exactly what you would have seen last month.
- **Determinism is tested, not asserted.** Shuffle a journal's lines on disk and recompile: the
  snapshot is byte-identical. Tamper with a derived value and compilation fails loudly, naming
  the field, the expected value and the recorded one.
- **Zero runtime dependencies.** Python 3.12 stdlib only, including the YAML loader. The reader
  is a separate package that never imports the writer, so a kill-switch daemon can vendor it and
  still add nothing to its supply chain. CI fails the build if either rule is broken.

## Quickstart

```bash
docker build -t qkt-data-hub:local .
mkdir -p ./hub-data && chmod 777 ./hub-data

docker run --rm -v "$PWD/hub-data:/data" qkt-data-hub:local ls
docker run --rm -v "$PWD/hub-data:/data" qkt-data-hub:local collect
docker run --rm -v "$PWD/hub-data:/data" qkt-data-hub:local compile
docker run --rm -v "$PWD/hub-data:/data" qkt-data-hub:local verify

# What would a consumer have seen at a given instant?
docker run --rm -v "$PWD/hub-data:/data" qkt-data-hub:local \
  as-of cal.high_impact USD 2026-09-12T00:00:00+00:00
```

Or without Docker, from a checkout:

```bash
python -m hub --root ./hub-data run --once
python -m hub --root ./hub-data verify
```

## The record

One JSON object per journal line. The envelope is frozen and owned by the format; the payload is
declared per dataset.

```json
{"v":1,"dataset":"cal.high_impact","scope":"USD","key":"2026-09-11T1789129800000|USD|CPI m/m",
 "revision":2,"known_at":1789129801234,"effective_at":1789129800000,
 "availability":"observed","source":"cal.high_impact","parser":"decl/http_json@1",
 "raw_ref":"sha256:050a82...","seq":48213,"id":"sha256:1b7e...",
 "fields":{"title":"CPI m/m","impact":2,"forecast":"0.4","previous":"0.1",
           "actual":"0.3","surprise":"-0.1","surprise_z":"-0.83"}}
```

Numbers travel as decimal strings, never floats, because `0.1 + 0.2` must not be a different
fact on a different machine. Timestamps are UTC epoch milliseconds; a naive source timestamp is
rejected rather than assumed. A missing value is `null` and stays `null` through every
derivation — the moment "no data" becomes `0`, every comparison downstream lies.

## Adding a dataset

A dataset is a file, not code. This is the whole of the shipped economic-calendar dataset's
acquisition half:

```yaml
collector:
  kind: http_json
  url: https://nfs.faireconomy.media/ff_calendar_thisweek.json
  cadence: 30m
  cadence_near_event: { before: 15m, after: 30m, every: 30s, anchor: effective_at }
  record_path: "$[*]"
  where: { impact: [High, Holiday] }
  scope: { from: country, map: { All: ALL } }
  effective_at: { from: date, parse: iso8601_with_offset }
  known_at: ingest_time
  fields:
    actual: { from: actual, parse: percent_or_number, null_if: ["", "-"] }
fields:
  actual:   { type: number, null_policy: allow_until_release }
  forecast: { type: number, null_policy: allow }
  surprise:   { type: number, derived: "actual - forecast" }
  surprise_z: { type: number, derived: "zscore(surprise, window=24, min_obs=8)" }
```

The tighter cadence near an event is not a nicety. A consensus feed fills in an `actual` within
seconds of a release, and a half-hourly poll would record that arrival half an hour late, which
would make `known_at` a false claim about our own past.

Derived fields are computed by the hub, in dependency order, and **re-verified on every
compile** — so a parser change that would silently rewrite history fails the build instead.

Unknown keys are rejected at every level of the file. A typo like `nul_if` must never quietly
disable a check.

For a source no mapping can express — a PDF, a page needing a session, a model scoring text — a
collector is a small Python class with the same two methods, and everything downstream is
unchanged. See [docs/EXTENDING.md](docs/EXTENDING.md).

## Loading history

Live collection records `known_at` as it happens. History cannot be observed after the fact, so
it enters through `backfill`, stamped `derived` with an availability rule you can read and a
strict consumer can refuse:

```bash
python3 tools/fred_backfill.py tests/fixtures/fred_dfii10.csv 2018-01-01 > /tmp/dfii10.ndjson
python -m hub --root ./hub-data backfill rates.us.dfii10 --file /tmp/dfii10.ndjson --source fred
python -m hub --root ./hub-data compile && python -m hub --root ./hub-data verify
python -m hub --root ./hub-data as-of rates.us.dfii10 USD 2024-06-03T12:00:00+00:00
```

That last command answers with Thursday's value on a Monday morning: Friday's observation is not
knowable until the next business day at 13:00 UTC, and the store says so rather than guessing.

## Consuming it

```python
from hubread.reader import Reader
from hubread.policy import Policy

reader = Reader("/var/lib/qkt-hub", Policy(refuse_derived=True))
facts = reader.as_of("cal.high_impact", "USD", 1789129800000)
windows = reader.windows("cal.high_impact", "USD", now, ahead_ms=1_800_000, pad_ms=300_000)
```

A single-field dataset names its field `value`, so a consumer reads `alias.value` with no schema
lookup. `hubread` is stdlib-only and never imports `hub`. Vendor it, or mount the store read-only and
point at it. The [qkt](https://github.com/elitekaycy/qkt) engine binds datasets as `HUB:` streams
so a strategy reads `cpi.surprise_z` the way it reads a candle field, identically in backtest and
live.

## Commands

| Command | What it does |
|---|---|
| `ls` | every dataset with its schema hash |
| `validate` | load and check every schema, then exit |
| `collect [dataset]` | fetch once and journal what survives the pipeline |
| `compile` | fold journals into snapshots and rewrite the manifest |
| `verify` | recompile every window and compare against the manifest |
| `as-of <dataset> <scope> <instant>` | exactly what a consumer would have seen |
| `health` | heartbeat freshness; the container healthcheck |
| `backfill <dataset> --file` | import history, stamped `derived` |
| `run [--once]` | the daemon: poll, compile, beat |

Exit codes are part of the contract: `0` success, `2` a usage mistake, `3` a data problem.
`verify` never exits `0` on a store it could not confirm.

## Documentation

- [docs/FORMAT.md](docs/FORMAT.md) — the record envelope, the `QKH1` snapshot layout, the manifest
- [docs/DATA-ENGINEERING.md](docs/DATA-ENGINEERING.md) — the pipeline stages and the rules each enforces
- [docs/EXTENDING.md](docs/EXTENDING.md) — adding a dataset, a parser, a derived series
- [docs/OPERATIONS.md](docs/OPERATIONS.md) — deploying, alerting, and what to do when a source dies
- [docs/spec/](docs/spec/) — the design specification this was built from

## Why no dependencies?

The same reason its sibling kill-switch has none. Every dependency is a way for the store to
fail, and this store is what a backtest's credibility rests on. A YAML parser restricted to the
subset a schema actually needs is a hundred lines that fail loudly on anything unusual; a general
one is a supply chain. `ruff` and `mypy` are development tools and never enter the image.

## Licence

Apache 2.0. See [LICENSE](LICENSE).
