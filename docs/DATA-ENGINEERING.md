# The pipeline, stage by stage

Each stage has one job and one rule it enforces. Everything between `Acquire` and `Journal` is a
pure function of its inputs, which is what makes the whole pipeline replayable: re-run a fixed
parser over the archived bytes and you get corrected revisions, not edited history.

```
Acquire -> Parse -> Clean -> Normalise -> Validate -> Deduplicate -> Derive -> Journal -> Compile -> Serve
 (I/O)                        (pure)                                            (append)   (pure)   (read)
    |                                                                              |
    +-- raw/ : every byte, content-addressed, forever                              +-- snapshot/ + manifest
```

## Acquire

A collector does I/O and nothing else. `fetch` returns bytes or `None`; it never raises into the
scheduler, because one provider being down must not stop the others.

**Rule: a wrong content type is a hard failure, even on a 200.** The observed failure in a
sibling project was an HTML error page served with a 200 while the JSON parser produced nothing
and the system carried on looking healthy. Every source declares what it expects.

Every response is archived content-addressed *before* parsing begins. Identical bytes are stored
once, so a re-fetch of an unchanged page costs nothing.

## Parse

Bytes to candidate rows. Declarative for JSON and CSV feeds; a small class for anything a mapping
cannot express. A parser may not do I/O — that is what lets it be re-run over the archive.

**Rule: a parser may say what a fact describes and when it applies, never when we knew it.**
Availability is the ingest clock's job, or an explicit backfill's.

## Clean and normalise

Trim, strip thousands separators, map sentinels (`""`, `-`, `n/a`, `—`) to null, convert units to
the declared canonical unit, map provider codes to canonical scopes, and convert timestamps to
UTC epoch milliseconds from the declared zone.

**Rule: a naive timestamp is rejected, never assumed to be UTC.** A sibling project shipped a bug
where a New York timestamp was read as UTC and every event window moved four hours.

**Rule: unknown is `None`, never `0`.** A pre-release row has no `actual`. Turning that into zero
would invent a surprise of exactly minus the forecast on every upcoming release.

## Validate

Envelope completeness, payload against the schema, declared quality ranges, and temporal
invariants. Range comparisons use `Decimal`, not `float`: this check gates whether a record is
quarantined, and `float(100000000000000003) == float(100000000000000000)`.

**Rule: fail closed.** A violation is quarantined with its reason and counted, never dropped in a
`continue`. A dataset whose source changed shape does not fail loudly on its own — it just goes
quiet — so the daily quarantine count is what an operator alerts on.

**Rule: a `known_at` ahead of the writer's clock is quarantined.** A mis-clocked hub would
otherwise publish facts from the future that every consumer would honour.

## Deduplicate

Same `(dataset, key)` and same payload hash is a duplicate and is dropped. Same key, different
payload is `revision + 1` with a fresh `known_at`. A collector re-fetching an unchanged row must
not grow the journal; a consensus that drifts must.

**Rule: identity never includes `known_at`.** An identity that moved when we learned something
would make each correction a new fact instead of a new revision.

## Derive

Declared expressions computed in dependency order, over the payload plus this scope's prior
payloads. History is per scope: a z-score of a US surprise must never see EUR observations.

**Rule: the compiler recomputes and re-verifies every derived value.** A mismatch is a hard error
naming the field, the expected value and the recorded one. That is the check that catches a
parser or expression change silently rewriting history.

## Journal

Append-only, one writer per root, line-atomic, day-partitioned by `known_at`. See
[FORMAT.md](FORMAT.md).

## Compile

Journal window to snapshot, sorted, hashed, recorded in the manifest.

**Rule: determinism is tested.** Shuffle the journal's lines on disk and recompile; the bytes and
the hash must be identical. Recompiling twice must be identical. Both are asserted in CI.

## Serve

A local tail for a consumer on the same host, a snapshot read for history, and a read-only
HTTP surface for anything else. The visibility rule is applied inside the reader, once, so no
consumer can forget it.
