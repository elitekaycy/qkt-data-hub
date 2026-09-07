"""Turn a FRED daily CSV into backfill records for `hub backfill`.

FRED's CSV gives one row per observation date and nothing about when each value was published.
The hub refuses to guess `known_at` on the live path, so history has to enter through `backfill`,
stamped `derived` with an explicit availability rule a consumer can see and refuse: a value dated
D is treated as knowable on the next US business day at 13:00 UTC. That is the same conservative
window the engine's macro-series path assumes, so a backtest that migrates from `MACRO:` to `HUB:`
sees the same facts at the same instants.

Usage:
    python3 tools/fred_backfill.py tests/fixtures/fred_dfii10.csv > /tmp/dfii10.ndjson
    python3 -m hub --root <store> backfill rates.us.dfii10 --file /tmp/dfii10.ndjson --source fred
"""
from __future__ import annotations

import csv
import datetime as dt
import json
import sys
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo

# Matches every shipped FRED dataset's live collector exactly (`date_in_zone(..., "America/New_York",
# hour=16)`): a backfilled and a live-observed record for the same calendar day must compute the
# identical `effective_at`, or a later live re-observation of an unchanged value looks like a
# revision instead of a duplicate, and `ScopeHistory` sees two records claiming the same key at two
# different instants. Fixed at UTC+... would be simpler and was wrong for exactly that reason.
NY = ZoneInfo("America/New_York")
RELEASE_UTC_HOUR = 13


def next_business_day(day: dt.date) -> dt.date:
    nxt = day + dt.timedelta(days=1)
    while nxt.weekday() >= 5:
        nxt += dt.timedelta(days=1)
    return nxt


def main(path: str, since: str = "2000-01-01") -> int:
    floor = dt.date.fromisoformat(since)
    with open(path, encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        date_col = next(c for c in reader.fieldnames or [] if c.lower() in ("observation_date", "date"))
        value_col = next(c for c in reader.fieldnames or [] if c != date_col)
        for row in reader:
            day = dt.date.fromisoformat(row[date_col].strip())
            if day < floor:
                continue
            raw = row[value_col].strip()
            try:
                value = str(Decimal(raw)) if raw not in ("", ".") else None
            except InvalidOperation:
                value = None
            observed = dt.datetime.combine(day, dt.time(16, 0), tzinfo=NY)
            release = dt.datetime.combine(next_business_day(day), dt.time(RELEASE_UTC_HOUR, 0), tzinfo=dt.UTC)
            period_start = dt.datetime.combine(day, dt.time(0, 0), tzinfo=dt.UTC)
            record = {
                "scope": "USD",
                "effective_at": int(observed.timestamp() * 1000),
                "known_at": int(release.timestamp() * 1000),
                "period_start": int(period_start.timestamp() * 1000),
                "availability": "derived",
                "fields": {"value": value},
            }
            sys.stdout.write(json.dumps(record, separators=(",", ":")) + "\n")
    return 0


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__, file=sys.stderr)
        raise SystemExit(2)
    raise SystemExit(main(sys.argv[1], *(sys.argv[2:3])))
