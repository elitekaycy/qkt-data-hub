# Running it

## Deployment

One hub per host. Consumers mount the store **read-only** — the hub is the only writer, and a
second writer would interleave sequence numbers and corrupt the ordering everything depends on.
`docker-compose.example.yml` is the shape.

The hub holds no broker credentials and has no route to a trading gateway. It informs; it cannot
trade. Keep it that way.

Pin the image tag. Never `:latest` where the output feeds a real account.

## Health

The container healthcheck runs `hub health`, which reads the heartbeat file's age against
`policy.stale_after_ms`. This tests liveness, not existence: a hub that has hung still has a
running process, but its heartbeat stops advancing.

`hub verify` recompiles every window and compares against the manifest. Run it after any restore,
before any claim that a backtest is reproducible, and on a timer. It exits `3` on any problem and
never exits `0` on a store it could not confirm.

## What to watch

| Signal | Why it matters |
|---|---|
| heartbeat age | the writer is alive and keeping up |
| quarantine count per dataset per day | a source changed shape; this is the early warning |
| consecutive collect failures | logged, and alerted at the configured threshold |
| `verify` exit code | the compiled store still matches its manifest |
| records and revisions per day | a feed that silently went quiet looks exactly like a calm week |

A source that stops answering does not blank anything. The last known records stay, the reader
keeps serving them, and staleness is reported for the consumer to act on. Losing a feed must
never remove protection a consumer already had.

## When a source dies

1. `hub collect <dataset>` by hand and read the log line. A wrong content type says the provider
   changed shape; a timeout says it is down.
2. Check `quarantine/<dataset>/<today>.ndjson` for rejected rows and their reasons.
3. If the shape changed, fix the mapping, bump the parser version, and re-run the parse over the
   archived blobs. The corrected values arrive as new revisions; history is preserved.

## Backfill

`hub backfill` is deliberately a separate verb from `collect`. It is the only path that may write
a `known_at` it did not observe, and those records are stamped `derived` so a strict consumer can
refuse them. Never run it against a dataset while the daemon is collecting the same one.

## Restore

`raw/` and `journal/` are the store. `snapshot/` and `manifest.json` are derived and can be
rebuilt with `hub compile`. Back up the first two; verify the rest.

## Web view

`hub ui` serves a read-only page over the store: a node graph of the pipeline (sources, collect,
raw archive, pipeline, journal, compile, verify, readers) coloured by health, every dataset with
its record count and newest fact, records written per day, any field's value over time as a
strategy saw it, the latest records, the quarantine log, and an as-of explorer that shows exactly
what a `HUB:` stream reads at an instant. It reads the same files consumers read plus the
writer's status files under `<root>/status/`, and never writes.

```bash
python -m hub --root ./hub-data ui                 # http://127.0.0.1:8090
python -m hub --root ./hub-data ui --host 0.0.0.0  # only behind a firewall or tunnel
```

Link to one dataset with `#<dataset>`, e.g. `http://127.0.0.1:8090/#cal.us_high`.

## Release calendars

`cal.us_fomc`, `cal.us_cpi`, `cal.us_nfp` and `cal.us_high` (all three) come from
`datasets/schedules/us_events.csv`, transcribed from the Fed's FOMC calendars and the BLS
release archives and schedules. Each record carries `at` (the release instant) and `kind`, and is
written when its event becomes the next one: one minute after the previous event. That is the
only way a consumer that reads the newest record can see the next release without ever seeing a
fact early, and it is identical live and in history:

```bash
for d in us_fomc us_cpi us_nfp us_high; do
  python3 tools/schedule_backfill.py datasets/cal.$d.yaml 2018-01-01 > /tmp/$d.ndjson
  python -m hub --root ./hub-data backfill cal.$d --file /tmp/$d.ndjson --source us-schedule
done
python -m hub --root ./hub-data compile && python -m hub --root ./hub-data verify
```

After that the daemon's schedule collector keeps each one current, and a live collect over a
backfilled store reports the current event as a duplicate: the two paths wrote the same record.
When an agency moves a release, edit the CSV; the collector writes the change as a new revision.
Add the next year's dates when the agencies publish them, well before the last listed event.

A strategy measures time to the next release with `cal.at - NOW.epoch_ms`:

```
SYMBOLS
    eu  = EXNESS:EURUSD EVERY 5m,
    cal = HUB:cal.us_high.USD EVERY 1d
RULES
    WHEN cal.at - NOW.epoch_ms > 0 AND cal.at - NOW.epoch_ms <= 1800000 AND POSITION.eu != 0
    THEN CLOSE eu          -- flat for the 30 minutes before any FOMC, CPI or payrolls release
```

## Proving qkt reads the store the way hubread does

`tools/qkt_conformance.py` derives from `hubread` the exact bar a probe strategy must enter
before each event, runs the probe through a real qkt backtest on the same store, and fails on any
difference. Run it after changing the format, the calendar or the qkt version:

```bash
python3 tools/qkt_conformance.py --store ./hub-data --dataset cal.us_high \
  --data-root <price data> --from 2018-01-01 --to 2025-01-01 --image ghcr.io/elitekaycy/qkt:v0.63.1
```

## Rules a collector must keep for live and backtest to agree

- Write a record at its own `known_at`. The writer refuses a `known_at` ahead of its clock. qkt
  (from the release after 0.63.1) also holds any record it reads before its `known_at` and delivers
  it at that instant, and seeds each hub stream with its newest visible record when a session
  starts, so a restart is never blind to the current fact.
- Scopes are upper case `A-Z 0-9 _ : -` with no dot (`ISSUER:BRK-B`); qkt reads the scope as the
  last dot-separated segment of the stream name.
- Numbers must fit 8 decimals in a signed 64-bit integer (about +/-92 billion); store large totals
  in millions or billions. Instants belong in `timestamp` fields, not `number`.
