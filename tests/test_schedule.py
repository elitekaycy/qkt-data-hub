"""Tests for the schedule collector and its backfill: the two must agree on every instant.

A strategy reads the latest visible record, so the property that makes a calendar safe to trade
is that "the event the live collector offers at T" equals "the newest backfilled record whose
known_at <= T", for every T. These tests pin that, plus the rules that keep the calendar honest:
no event published before its predecessor has passed, no unknown kind, no duplicate instant.
"""
import datetime as dt
import io
import json
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory
from zoneinfo import ZoneInfo

from hub.collectors.schedule import PUBLISH_LAG_MS, ScheduleSource, load_events, next_event, published_at
from hubread.errors import ConfigError
from tools import schedule_backfill

NY = ZoneInfo("America/New_York")
CSV = """# test calendar
kind,date,time,zone,source
fomc,2024-01-31,14:00,America/New_York,test
nfp,2024-02-02,08:30,America/New_York,test
cpi,2024-02-13,08:30,America/New_York,test
fomc,2024-03-20,14:00,America/New_York,test
nfp,2024-07-05,08:30,America/New_York,test
"""
DATASET = """
dataset: cal.us_high
version: 1
title: test
scope_kind: currency
key: [effective_at, scope]
collector:
  kind: code
  module: hub.collectors.schedule
  class: ScheduleSource
  schedule: us_events.csv
  kinds: [fomc, cpi, nfp]
fields:
  at:   { type: timestamp, null_policy: forbid }
  kind: { type: enum, values: [fomc, cpi, nfp], null_policy: forbid }
"""


def ny_ms(day: str, hhmm: str) -> int:
    at = dt.datetime.combine(dt.date.fromisoformat(day), dt.time.fromisoformat(hhmm), tzinfo=NY)
    return int(at.timestamp() * 1000)


class ScheduleTest(unittest.TestCase):
    def test_converts_new_york_local_times_across_daylight_saving(self):
        events = load_events(CSV, ["fomc", "cpi", "nfp"])
        self.assertEqual(events[0].at, ny_ms("2024-01-31", "14:00"))
        # 2024-07-05 is EDT: 08:30 New York is 12:30 UTC, not 13:30.
        self.assertEqual(events[-1].at, int(dt.datetime(2024, 7, 5, 12, 30, tzinfo=dt.UTC).timestamp() * 1000))

    def test_keeps_only_the_requested_kinds(self):
        self.assertEqual([e.kind for e in load_events(CSV, ["fomc"])], ["fomc", "fomc"])

    def test_refuses_unknown_kinds_and_duplicate_instants(self):
        with self.assertRaisesRegex(ConfigError, "unknown kind"):
            load_events(CSV + "ecb,2024-04-11,08:15,Europe/Berlin,test\n", ["fomc"])
        with self.assertRaisesRegex(ConfigError, "two events"):
            load_events(CSV + "cpi,2024-01-31,14:00,America/New_York,test\n", ["fomc", "cpi"])

    def test_an_event_is_current_until_one_lag_after_it(self):
        events = load_events(CSV, ["fomc", "cpi", "nfp"])
        first, second = events[0], events[1]
        self.assertEqual(next_event(events, first.at), first)
        self.assertEqual(next_event(events, first.at + PUBLISH_LAG_MS - 1), first)
        self.assertEqual(next_event(events, first.at + PUBLISH_LAG_MS), second)
        self.assertIsNone(next_event(events, events[-1].at + PUBLISH_LAG_MS))

    def test_live_offer_equals_latest_backfilled_record_at_every_instant(self):
        events = load_events(CSV, ["fomc", "cpi", "nfp"])
        start = ny_ms("2024-01-01", "00:00")
        published = [(published_at(events, i, start), e) for i, e in enumerate(events)]
        t = start
        while t < events[-1].at + 2 * PUBLISH_LAG_MS:
            visible = [e for known, e in published if known <= t]
            backfilled = visible[-1] if visible and t < visible[-1].at + PUBLISH_LAG_MS else None
            self.assertEqual(next_event(events, t), backfilled, t)
            # Step finely around every boundary and coarsely in between.
            near = any(abs(t - (e.at + PUBLISH_LAG_MS)) < 120_000 for e in events)
            t += 1_000 if near else 3_600_000

    def test_collector_and_backfill_tool_produce_the_same_records(self):
        with TemporaryDirectory() as d:
            Path(d, "us_events.csv").write_text(CSV)
            Path(d, "cal.us_high.yaml").write_text(DATASET)
            now = ny_ms("2024-12-31", "00:00")
            buf = io.StringIO()
            with redirect_stdout(buf):
                schedule_backfill.main(str(Path(d, "cal.us_high.yaml")), "2024-01-01", now_ms=now)
            rows = [json.loads(line) for line in buf.getvalue().splitlines()]
            options = {"schedule": "us_events.csv", "kinds": ["fomc", "cpi", "nfp"]}
            source = ScheduleSource.from_config("cal.us_high", options, Path(d))
            for row in rows:
                source._clock = lambda k=row["known_at"]: k / 1000  # noqa: B023 - bound per row
                (cand,) = source.parse(source.fetch(1.0))
                self.assertEqual(cand.fields, row["fields"])
                self.assertEqual(cand.effective_at, row["effective_at"])

    def test_backfill_never_emits_a_future_known_at(self):
        with TemporaryDirectory() as d:
            Path(d, "us_events.csv").write_text(CSV)
            Path(d, "cal.us_high.yaml").write_text(DATASET)
            now = ny_ms("2024-02-10", "00:00")
            buf = io.StringIO()
            with redirect_stdout(buf):
                schedule_backfill.main(str(Path(d, "cal.us_high.yaml")), "2024-01-01", now_ms=now)
            rows = [json.loads(line) for line in buf.getvalue().splitlines()]
            self.assertTrue(rows)
            self.assertTrue(all(r["known_at"] <= now for r in rows))
            # The CPI of 2024-02-13 becomes known one lag after the 2024-02-02 report: still past.
            self.assertEqual(rows[-1]["title"], "cpi")

    def test_from_config_fails_closed(self):
        with TemporaryDirectory() as d:
            Path(d, "us_events.csv").write_text(CSV)
            with self.assertRaisesRegex(ConfigError, "unknown key"):
                ScheduleSource.from_config("x", {"schedule": "us_events.csv", "kinds": ["fomc"], "typo": 1}, Path(d))
            with self.assertRaisesRegex(ConfigError, "not found"):
                ScheduleSource.from_config("x", {"schedule": "missing.csv", "kinds": ["fomc"]}, Path(d))
            with self.assertRaisesRegex(ConfigError, "kinds"):
                ScheduleSource.from_config("x", {"schedule": "us_events.csv", "kinds": ["ecb"]}, Path(d))


if __name__ == "__main__":
    unittest.main()
