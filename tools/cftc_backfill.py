"""Turn a CFTC Commitments of Traders JSON export into backfill records for `hub backfill`.

The CFTC publishes each report at 15:30 New York time, normally on the Friday after its "as of"
date, later in holiday weeks and much later after a lapse in appropriations. A historical export
carries only the report date, so history enters through `backfill`, stamped `derived`, at the
release instant `tools/cot_release.py` computes from the CFTC's own schedules (checked against
every published 2025-2026 date in `tests/test_cot_release.py`). An earlier version stamped every
report on its Friday, which during the 2025 shutdown made reports visible weeks before release.

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
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools.cot_release import release_instant_ms  # noqa: E402

_FIELDS = {
    "open_interest": "open_interest_all",
    "noncomm_long": "noncomm_positions_long_all",
    "noncomm_short": "noncomm_positions_short_all",
    "comm_long": "comm_positions_long_all",
    "comm_short": "comm_positions_short_all",
}


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
        known_at_ms = release_instant_ms(day)
        period_start = dt.datetime.combine(day, dt.time(0, 0), tzinfo=dt.UTC)
        record = {
            "scope": scope,
            "effective_at": int(period_start.timestamp() * 1000),
            "known_at": known_at_ms,
            "period_start": int(period_start.timestamp() * 1000),
            "availability": "derived",
            "fields": {dst: _number(row.get(src)) for dst, src in _FIELDS.items()},
        }
        sys.stdout.write(json.dumps(record, separators=(",", ":")) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else "2000-01-01",
                          sys.argv[3] if len(sys.argv) > 3 else "XAUUSD"))
