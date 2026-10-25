"""Turn a FRED daily CSV into backfill records for `hub backfill`.

FRED's CSV gives one row per observation date and nothing about when each value was published.
The hub refuses to guess `known_at` on the live path, so history has to enter through `backfill`,
stamped `derived` with an explicit availability rule a consumer can see and refuse. The series
here come from the Fed's H.15 release, posted at 4:15 p.m. New York time on each federal business
day with the previous business day's rates, and never on a holiday or a closure. So:

- a daily value dated D is knowable at 16:30 New York on the next federal business day after D;
- a monthly average (pass `monthly`) is knowable at 16:30 New York on the second federal business
  day after its month ends.

An earlier rule stamped daily values at 13:00 UTC on the next weekday, several hours before the
4:15 p.m. posting and a day early after a holiday, and used that daily rule for the monthly
FEDFUNDS series, making each month's average visible a month early.

Usage:
    python3 tools/fred_backfill.py tests/fixtures/fred_dfii10.csv 2018-01-01 > /tmp/dfii10.ndjson
    python3 tools/fred_backfill.py FEDFUNDS.csv 2018-01-01 monthly > /tmp/fedfunds.ndjson
    python3 -m hub --root <store> backfill rates.us.dfii10 --file /tmp/dfii10.ndjson --source fred
"""
from __future__ import annotations

import csv
import datetime as dt
import json
import sys
from decimal import Decimal, InvalidOperation
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools.us_calendar import next_business_day  # noqa: E402

# Matches every shipped FRED dataset's live collector exactly (`date_in_zone(..., "America/New_York",
# hour=16)`): a backfilled and a live-observed record for the same calendar day must compute the
# identical `effective_at`, or a later live re-observation of an unchanged value looks like a
# revision instead of a duplicate, and `ScopeHistory` sees two records claiming the same key at two
# different instants. Fixed at UTC+... would be simpler and was wrong for exactly that reason.
NY = ZoneInfo("America/New_York")
RELEASE_TIME = dt.time(16, 30)


def published_at(day: dt.date, cadence: str) -> dt.datetime:
    """When the value dated `day` was posted, under the rule in the module docstring."""
    if cadence == "monthly":
        month_end = (day.replace(day=28) + dt.timedelta(days=4)).replace(day=1) - dt.timedelta(days=1)
        return dt.datetime.combine(next_business_day(month_end, 2), RELEASE_TIME, tzinfo=NY)
    if cadence == "daily":
        return dt.datetime.combine(next_business_day(day), RELEASE_TIME, tzinfo=NY)
    raise SystemExit(f"cadence must be daily or monthly, got {cadence!r}")


def main(path: str, since: str = "2000-01-01", cadence: str = "daily") -> int:
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
            release = published_at(day, cadence)
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
    raise SystemExit(main(sys.argv[1], *(sys.argv[2:4])))
