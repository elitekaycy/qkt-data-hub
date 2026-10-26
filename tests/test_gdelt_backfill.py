"""The GDELT backfill produces the live collector's numbers, marks source gaps, and never marks a
month done when a listed batch could not be read."""
import datetime as dt
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from hub.collectors import gdelt
from tools import gdelt_backfill

FIXTURE = Path(__file__).parent / "fixtures" / "gdelt_export_20261026010000.csv"
HOUR_END = int(dt.datetime(2026, 10, 26, 1, tzinfo=dt.UTC).timestamp() * 1000)


class GdeltBackfillTest(unittest.TestCase):
    def setUp(self):
        stamps = gdelt.batch_stamps(HOUR_END)
        # GDELT listed three of the hour's four batches.
        self.master = {s: (f"md5-{s}", f"https://x/{gdelt.stamp_text(s)}.export.CSV.zip") for s in stamps[:3]}

    def test_an_hour_matches_the_live_collector_and_counts_the_unlisted_batch(self):
        with mock.patch.object(gdelt_backfill, "fetch", lambda url, md5: FIXTURE.read_bytes()):
            rows = {r["scope"]: r for r in gdelt_backfill.hour_rows(HOUR_END, self.master)}
        events, _ = gdelt.parse_export(FIXTURE.read_bytes())
        stamps = gdelt.batch_stamps(HOUR_END)
        listed = {s: (events if s in self.master else None) for s in stamps}
        live = {c.scope: c for c in gdelt.hour_candidates(HOUR_END, listed)}
        self.assertEqual(set(rows), set(live))
        for scope, row in rows.items():
            self.assertEqual(row["fields"], live[scope].fields)
            self.assertEqual(row["fields"]["batches"], "3")
            self.assertEqual(row["known_at"], HOUR_END + gdelt.PUBLISH_LAG_MS)
            self.assertEqual(row["availability"], "derived")

    def test_a_month_with_an_unreadable_listed_batch_is_not_marked_done(self):
        out = Path(tempfile.mkdtemp())

        def flaky(url, md5):
            if "20261026003000" in url:
                raise RuntimeError("download failed")
            return FIXTURE.read_bytes()

        hours = [HOUR_END]
        with mock.patch.object(gdelt_backfill, "fetch", flaky), mock.patch.object(
            gdelt_backfill, "month_hours", lambda month: hours
        ):
            ok = gdelt_backfill.run_month("2026-10", out, self.master, workers=2, now_ms=HOUR_END + 10 * 60_000)
        self.assertFalse(ok)
        self.assertFalse((out / "2026-10.done").exists())
        self.assertIn("20261026010000", (out / "2026-10.failures").read_text())
        with mock.patch.object(gdelt_backfill, "fetch", lambda url, md5: FIXTURE.read_bytes()), mock.patch.object(
            gdelt_backfill, "month_hours", lambda month: hours
        ):
            done = gdelt_backfill.run_month("2026-10", out, self.master, workers=2, now_ms=HOUR_END + 10 * 60_000)
        self.assertTrue(done)
        rows = [json.loads(line) for line in (out / "2026-10.ndjson").read_text().splitlines()]
        self.assertIn("GLOBAL", {r["scope"] for r in rows})
        self.assertTrue((out / "2026-10.done").exists())

    def test_a_listed_batch_gdelt_does_not_serve_is_a_source_gap_not_a_failure(self):
        def served(url, md5):
            if "20261026003000" in url:
                raise gdelt_backfill.SourceGap(url)
            return FIXTURE.read_bytes()

        gaps: list[str] = []
        with mock.patch.object(gdelt_backfill, "fetch", served):
            rows = gdelt_backfill.hour_rows(HOUR_END, self.master, gaps)
        self.assertEqual({r["fields"]["batches"] for r in rows}, {"2"})
        self.assertEqual(len(gaps), 1)

    def test_an_hour_is_not_written_before_it_could_be_published(self):
        hours = gdelt_backfill.month_hours("2026-10")
        self.assertEqual(hours[0], int(dt.datetime(2026, 10, 1, 1, tzinfo=dt.UTC).timestamp() * 1000))
        self.assertEqual(hours[-1], int(dt.datetime(2026, 11, 1, tzinfo=dt.UTC).timestamp() * 1000))
        out = Path(tempfile.mkdtemp())
        with mock.patch.object(gdelt_backfill, "hour_rows", lambda h, m, g=None: [{"h": h}]):
            gdelt_backfill.run_month("2026-10", out, {}, workers=2, now_ms=HOUR_END + 4 * 60_000)
        written = [json.loads(line)["h"] for line in (out / "2026-10.ndjson").read_text().splitlines()]
        self.assertEqual(max(written), HOUR_END - gdelt.HOUR_MS)


if __name__ == "__main__":
    unittest.main()
