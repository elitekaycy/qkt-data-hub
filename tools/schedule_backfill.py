"""Turn a schedule-collected calendar dataset's history into backfill records for `hub backfill`.

The live `ScheduleSource` writes one record per event, at the moment that event becomes the next
one. This tool writes the same records for the past, stamping each with the instant the live
collector would have written it (`hub.collectors.schedule.published_at`) and `derived`
availability, so a backtest over this history and a live session agree on when every `at` value
changed. Only records whose publication instant has already passed are emitted: the hub refuses
a `known_at` in the future, and so does this tool.

Usage:
    python3 tools/schedule_backfill.py datasets/cal.us_high.yaml 2018-01-01 > /tmp/us_high.ndjson
    python3 -m hub --root <store> backfill cal.us_high --file /tmp/us_high.ndjson --source us-schedule
"""
from __future__ import annotations

import datetime as dt
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hub.collectors.schedule import KINDS, load_events, published_at  # noqa: E402
from hub.simpleyaml import load_path  # noqa: E402


def main(dataset_file: str, since: str, now_ms: int | None = None) -> int:
    spec = load_path(Path(dataset_file))
    collector = spec["collector"]
    scope = str(collector.get("scope", "USD"))
    schedule = Path(dataset_file).resolve().parent / str(collector["schedule"])
    events = load_events(schedule.read_text(encoding="utf-8"), collector["kinds"])
    start = int(dt.datetime.combine(dt.date.fromisoformat(since), dt.time(0, 0), tzinfo=dt.UTC).timestamp() * 1000)
    now = now_ms if now_ms is not None else int(time.time() * 1000)
    events = [e for e in events if e.at >= start]
    for i, event in enumerate(events):
        known = published_at(events, i, start)
        if known > now:
            break
        record = {
            "scope": scope,
            "effective_at": event.at,
            "period_start": event.at,
            "known_at": known,
            "availability": "derived",
            "title": event.kind,
            "fields": {"at": event.at, "kind": KINDS.index(event.kind)},
        }
        sys.stdout.write(json.dumps(record, separators=(",", ":")) + "\n")
    return 0


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(__doc__, file=sys.stderr)
        raise SystemExit(2)
    raise SystemExit(main(sys.argv[1], sys.argv[2]))
