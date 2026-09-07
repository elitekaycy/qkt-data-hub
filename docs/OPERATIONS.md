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
