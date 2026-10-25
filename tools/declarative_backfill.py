"""Backfill what a declarative source returns right now, for history a live poll may not claim.

A feed that returns a recent window (Binance's last 30 funding prints, the last 30 days of an
index) hands a newly started hub rows that are older than the dataset's `max_observed_age`, so
live collection refuses them: it cannot honestly say it just observed them. This tool fetches the
source once through the dataset's own declarative mapping, so the values are parsed exactly as a
live poll parses them, and writes them as backfill records stamped `derived` with an explicit
availability rule: `known_at = effective_at + lag`.

Usage:
    python3 tools/declarative_backfill.py datasets/rates.crypto.btc_funding.yaml --lag 0s > /tmp/f.ndjson
    python3 -m hub --root <store> backfill rates.crypto.btc_funding --file /tmp/f.ndjson --source binance
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hub.collectors.declarative import DeclarativeSource, duration_seconds  # noqa: E402
from hub.schema import load_schema  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("dataset_file")
    ap.add_argument("--lag", required=True, help="how long after effective_at the value was knowable, e.g. 0s, 1h")
    a = ap.parse_args()
    schema = load_schema(Path(a.dataset_file))
    collector = schema.raw.get("collector")
    if not isinstance(collector, dict) or not str(collector.get("kind", "")).startswith("http_"):
        print(f"{schema.name} has no declarative collector", file=sys.stderr)
        return 2
    lag_ms = int(duration_seconds(a.lag, "--lag") * 1000)
    blob = DeclarativeSource(schema.name, collector).fetch(30.0)
    if blob is None:
        print("source returned nothing usable", file=sys.stderr)
        return 3
    now = int(time.time() * 1000)
    written = 0
    for c in DeclarativeSource(schema.name, collector).parse(blob):
        if "parse_error" in c.extra:
            continue
        known_at = c.effective_at + lag_ms
        if known_at > now:
            continue  # not knowable yet; the live collector will record it when it is
        row = {
            "scope": c.scope,
            "effective_at": c.effective_at,
            "period_start": c.period_start,
            "known_at": known_at,
            "availability": "derived",
            "title": c.title,
            "fields": c.fields,
        }
        sys.stdout.write(json.dumps(row, separators=(",", ":")) + "\n")
        written += 1
    print(f"{written} record(s) for {schema.name}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
