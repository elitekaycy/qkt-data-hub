"""Turn a perpetual funding history (`time,rate,price` CSV, epoch ms) into backfill records.

A funding print is fixed and published at its funding instant, so `known_at = effective_at =
time`. The archive's times are kept exactly (Binance stamps some prints a millisecond late, e.g.
...00001), because the live collector keys each print by the same `fundingTime`; a rounded time
would make a re-observed print look like a different fact. An empty price is stored as null.

Usage:
    python3 tools/funding_backfill.py BTCUSDT.csv BTCUSD 2019-01-01 > /tmp/funding.ndjson
    python3 -m hub --root <store> backfill rates.crypto.btc_funding --file /tmp/funding.ndjson --source binance
"""
from __future__ import annotations

import csv
import datetime as dt
import json
import sys
import time
from decimal import Decimal, InvalidOperation


def _number(raw: str | None) -> str | None:
    text = (raw or "").strip()
    if not text:
        return None
    try:
        return str(Decimal(text))
    except InvalidOperation:
        return None


def main(path: str, scope: str, since: str) -> int:
    floor = int(dt.datetime.combine(dt.date.fromisoformat(since), dt.time(0), tzinfo=dt.UTC).timestamp() * 1000)
    now_ms = int(time.time() * 1000)
    written = 0
    with open(path, encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            at = int(row["time"])
            if at < floor or at > now_ms:
                continue
            rate = _number(row.get("rate"))
            if rate is None:
                continue  # a print without a rate is not a fact
            record = {
                "scope": scope,
                "effective_at": at,
                "period_start": at,
                "known_at": at,
                "availability": "derived",
                "fields": {"rate": rate, "mark_price": _number(row.get("price"))},
            }
            sys.stdout.write(json.dumps(record, separators=(",", ":")) + "\n")
            written += 1
    print(f"{written} funding record(s)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print(__doc__, file=sys.stderr)
        raise SystemExit(2)
    raise SystemExit(main(*sys.argv[1:4]))
