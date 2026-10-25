"""The FRED and CFTC backfill tools never emit a record before its publication instant.

A record whose `known_at` is still in the future would be refused by the hub's writer and land in
quarantine on every rebuild; the tools leave it for the live collector, which records it once it
is published.
"""
import contextlib
import datetime as dt
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from tools import cftc_backfill, fred_backfill


def emitted(run) -> list[dict]:
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        run()
    return [json.loads(line) for line in out.getvalue().splitlines()]


def epoch(iso: str) -> float:
    return dt.datetime.fromisoformat(iso).timestamp()


class BackfillToolsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.dir = Path(tempfile.mkdtemp())

    def test_fred_skips_a_value_the_next_h15_has_not_posted(self) -> None:
        csv = self.dir / "dgs10.csv"
        csv.write_text("observation_date,DGS10\n2026-10-21,4.01\n2026-10-22,4.02\n2026-10-23,4.03\n")
        # Thursday 10-22 posts at Friday 16:15 NY; Friday's value waits for Monday 10-26.
        with mock.patch("time.time", return_value=epoch("2026-10-24T12:00:00+00:00")):
            rows = emitted(lambda: fred_backfill.main(str(csv), "2026-01-01", "daily"))
        self.assertEqual([r["fields"]["value"] for r in rows], ["4.01", "4.02"])

    def test_cftc_skips_a_report_released_after_now(self) -> None:
        export = self.dir / "cot.json"
        export.write_text(json.dumps([
            {"report_date_as_yyyy_mm_dd": "2026-10-13T00:00:00.000", "open_interest_all": "1"},
            {"report_date_as_yyyy_mm_dd": "2026-10-20T00:00:00.000", "open_interest_all": "2"},
        ]))
        # 10-20's report is released Friday 10-23 at 15:30 New York (19:30 UTC).
        with mock.patch("time.time", return_value=epoch("2026-10-23T19:29:00+00:00")):
            rows = emitted(lambda: cftc_backfill.main(str(export), "2026-01-01", "XAUUSD"))
        self.assertEqual([r["fields"]["open_interest"] for r in rows], ["1"])


if __name__ == "__main__":
    unittest.main()
