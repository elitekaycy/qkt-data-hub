# Adding data

Three cases, from least work to most. None of them touches the qkt engine: a strategy binds a
dataset by name, and the fields exist because the schema says so.

## 1. A feed a mapping can describe

One file in `datasets/`. JSON and CSV over HTTP are covered.

```yaml
dataset: inv.us.eia_crude
version: 1
title: EIA weekly crude stocks
scope_kind: instrument
key: [period_start, scope]
collector:
  kind: http_json
  url: https://api.eia.gov/v2/...
  cadence: 1h
  record_path: "$.response.data[*]"
  scope: { const: "CL" }
  effective_at: { from: period, parse: date_in_zone, zone: America/New_York, hour: 10 }
  known_at: ingest_time
  fields:
    stocks: { from: value, parse: percent_or_number, null_if: ["", "-"] }
fields:
  stocks: { type: number, unit: kbbl }
  change: { type: number, derived: "diff(stocks, 1)" }
```

Then `hub validate` and `hub collect inv.us.eia_crude`. Nothing else changes.

Available parsers: `percent_or_number`, `human_number`, `iso8601_with_offset`, `date_in_zone`,
`enum_ordinal`, `boolean`. Unknown keys anywhere in the file are rejected at load.

## 2. A source no mapping can describe

A PDF, a page needing a session, a model scoring text. Write a class with two methods and
register it; everything downstream is unchanged.

```python
class MySource:
    name = "my-source"
    parser = "code/my_source@1"

    def fetch(self, timeout_seconds: float) -> RawBlob | None:
        """Return bytes, or None on any failure. Never raise into the scheduler."""

    def parse(self, blob: RawBlob) -> list[Candidate]:
        """Pure. No I/O — a corrected parser must be re-runnable over the archive."""
```

Bump `@1` to `@2` when you change the parse. Reprocessing then produces new revisions rather
than overwriting, and the old ones stay readable.

## 3. A dataset derived from other datasets

A batch job whose inputs are pinned snapshots of other datasets. Its output records carry
`availability: observed` and a `raw_ref` pointing at the input manifest, so the lineage is
traceable.

## Derived fields

Declared in the schema, computed by the hub, recomputed and verified on every compile.

```yaml
surprise:   { type: number, derived: "actual - forecast" }
surprise_z: { type: number, derived: "zscore(surprise, window=24, min_obs=8)" }
```

Chaining is normal and evaluated in dependency order. A cycle is rejected at load. The available
functions are `zscore`, `lag`, `diff`, `pct_rank`, `since`, `until`, plus arithmetic — and those
six names are reserved, so a field may not be called `diff`.

An expression may also read the envelope timestamps `known_at` and `effective_at`.

## Naming

`<domain>.<region-or-venue>.<subject>`, lowercase. The domain must be one of `macro`, `cal`,
`rates`, `cb`, `pos`, `inv`, `corp`, `venue`, `mkt`, `text`, `hub`. A new domain is a deliberate
change to the format's registry, not something a dataset file can invent.
