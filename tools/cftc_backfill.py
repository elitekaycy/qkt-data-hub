"""Turn a CFTC Commitments of Traders JSON export into backfill records for `hub backfill`.

The CFTC publishes each Tuesday's positioning the following Friday at 15:30 ET. A CSV or JSON
export of the historical series carries only the report date, not the publication instant, so
history has to enter through `backfill`, stamped `derived` with an explicit availability rule a
consumer can see and refuse: a report dated Tuesday D is treated as knowable the following
Friday at 20:30 UTC (15:30 ET plus the fixed 5-hour offset the live collector's own zone
resolution would also produce for a non-DST-sensitive publication time -- the CFTC always
publishes on the U.S. business calendar, so this stays a conservative, declared estimate rather
than an exact vintage).

Usage:
    curl -s 'https://publicreporting.cftc.gov/resource/6dca-aqww.json?\\
market_and_exchange_names=GOLD%20-%20COMMODITY%20EXCHANGE%20INC.&$limit=50000&\\
$order=report_date_as_yyyy_mm_dd' -o cot_gold.json
    python3 tools/cftc_backfill.py cot_gold.json 2018-01-01 XAUUSD > /tmp/cot_gold.ndjson
    python3 -m hub --root <store> backfill pos.cftc.cot_gold --file /tmp/cot_gold.ndjson --source cftc
"""
from __future__ import annotations

import datetime as dt
import json
import sys
from decimal import Decimal, InvalidOperation

PUBLISH_UTC_HOUR = 20
PUBLISH_UTC_MINUTE = 30

_FIELDS = {
    "open_interest": "open_interest_all",
    "noncomm_long": "noncomm_positions_long_all",
    "noncomm_short": "noncomm_positions_short_all",
    "comm_long": "comm_positions_long_all",
    "comm_short": "comm_positions_short_all",
}


def next_friday(day: dt.date) -> dt.date:
    days_ahead = (4 - day.weekday()) % 7
    return day + dt.timedelta(days=days_ahead or 7)


def _number(raw: object) -> str | None:
    if raw in (None, "", "."):
        return None
    try:
        return str(Decimal(str(raw)))
    except InvalidOperation:
        return None


def main(path: str, since: str, scope: str) -> int:
    floor = dt.date.fromisoformat(since)
    with open(path, encoding="utf-8") as fh:
        rows = json.load(fh)
    for row in rows:
        day = dt.date.fromisoformat(row["report_date_as_yyyy_mm_dd"][:10])
        if day < floor:
            continue
        published = next_friday(day)
        known_at = dt.datetime.combine(
            published, dt.time(PUBLISH_UTC_HOUR, PUBLISH_UTC_MINUTE), tzinfo=dt.UTC
        )
        period_start = dt.datetime.combine(day, dt.time(0, 0), tzinfo=dt.UTC)
        record = {
            "scope": scope,
            "effective_at": int(period_start.timestamp() * 1000),
            "known_at": int(known_at.timestamp() * 1000),
            "period_start": int(period_start.timestamp() * 1000),
            "availability": "derived",
            "fields": {dst: _number(row.get(src)) for dst, src in _FIELDS.items()},
        }
        sys.stdout.write(json.dumps(record, separators=(",", ":")) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else "2000-01-01",
                          sys.argv[3] if len(sys.argv) > 3 else "XAUUSD"))
