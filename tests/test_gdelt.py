"""The GDELT hourly aggregates: parsing, counting, weighting, gaps, and live/backfill sameness."""
import datetime as dt
import unittest
from decimal import Decimal
from pathlib import Path
from unittest import mock

from hub.collectors import gdelt
from hub.collectors.gdelt import Event, GdeltEventsSource

FIXTURE = Path(__file__).parent / "fixtures" / "gdelt_export_20261026010000.csv"
HOUR_END = int(dt.datetime(2026, 10, 26, 1, tzinfo=dt.UTC).timestamp() * 1000)


def ev(id_, country="US", root="01", base="010", quad=1, goldstein="0", articles=1, tone="0"):
    return Event(id_, base + "0", base, root, quad, Decimal(goldstein), articles, Decimal(tone), country)


class GdeltTest(unittest.TestCase):
    def test_a_real_export_parses_every_row(self):
        events, bad = gdelt.parse_export(FIXTURE.read_bytes())
        self.assertEqual((len(events), bad), (150, 0))
        us = gdelt.aggregate(events, gdelt.REGIONS["US"])
        self.assertEqual((us["events"], us["articles"]), ("34", "182"))  # counted independently with awk
        self.assertEqual(us["articles"], str(sum(e.articles for e in events if e.country == "US")))

    def test_a_malformed_line_is_counted_and_skipped(self):
        good = FIXTURE.read_bytes().splitlines()[0]
        cols = good.split(b"\t")
        cols[33] = b"x"  # NumArticles not a number
        events, bad = gdelt.parse_export(good + b"\nnot\ta\trow\n" + b"\t".join(cols))
        self.assertEqual((len(events), bad), (1, 2))

    def test_counts_by_code_and_article_weighted_means(self):
        events = [
            ev("1", root="13", quad=3, goldstein="-4", articles=3, tone="-2"),  # threat, verbal conflict
            ev("2", root="14", quad=3, goldstein="-6", articles=1, tone="-6"),  # protest
            ev("3", root="16", base="163", quad=4, goldstein="-8", articles=2, tone="-4"),  # sanctions
            ev("4", root="19", quad=4, goldstein="-10", articles=2, tone="-8"),  # fight: violence
            ev("4", root="19", quad=4, goldstein="-10", articles=2, tone="-8"),  # the same event again
            ev("5", country="FR", root="18", quad=4, articles=5),  # elsewhere
        ]
        us = gdelt.aggregate(events, gdelt.REGIONS["US"])
        self.assertEqual(
            us,
            {
                "events": "4", "articles": "8", "material_conflict": "2", "verbal_conflict": "2", "threats": "1",
                "protests": "1", "sanctions": "1", "violence": "1",
                # (-4*3 - 6*1 - 8*2 - 10*2) / 8 = -54/8 ; (-2*3 - 6 - 4*2 - 8*2) / 8 = -36/8
                "goldstein": "-6.75000000", "tone": "-4.50000000",
            },
        )
        self.assertEqual(gdelt.aggregate(events, None)["events"], "5")
        self.assertIsNone(gdelt.aggregate(events, gdelt.REGIONS["JP"]))

    def test_a_missing_batch_is_counted_and_an_empty_hour_has_no_record(self):
        stamps = gdelt.batch_stamps(HOUR_END)
        self.assertEqual([gdelt.stamp_text(s)[-6:] for s in stamps], ["001500", "003000", "004500", "010000"])
        batches = {stamps[0]: [ev("1")], stamps[1]: None, stamps[2]: [ev("2")], stamps[3]: [ev("3", country="JA")]}
        cands = {c.scope: c for c in gdelt.hour_candidates(HOUR_END, batches)}
        self.assertEqual(set(cands), {"GLOBAL", "US", "JP"})
        self.assertEqual(cands["US"].fields["batches"], "3")
        self.assertEqual((cands["US"].effective_at, cands["US"].period_start), (HOUR_END, HOUR_END - gdelt.HOUR_MS))
        self.assertEqual(gdelt.hour_candidates(HOUR_END, dict.fromkeys(stamps)), [])

    def test_the_archived_form_parses_to_exactly_the_same_records(self):
        events, _ = gdelt.parse_export(FIXTURE.read_bytes())
        stamps = gdelt.batch_stamps(HOUR_END)
        batches = {stamps[0]: None, stamps[1]: ("m1", events[:60]), stamps[2]: ("m2", events[60:]), stamps[3]: None}
        hour, expanded = gdelt.expand(gdelt.compact(HOUR_END, batches))
        self.assertEqual(hour, HOUR_END)
        direct = gdelt.hour_candidates(HOUR_END, {s: (None if b is None else b[1]) for s, b in batches.items()})
        self.assertEqual(gdelt.hour_candidates(hour, expanded), direct)

    def test_live_fetches_the_newest_hour_past_the_lag_once(self):
        events, _ = gdelt.parse_export(FIXTURE.read_bytes())
        clock = HOUR_END / 1000 + 4 * 60  # 01:04: the 01:00 hour is not past its lag yet
        source = GdeltEventsSource("text.gdelt.events", clock=lambda: clock)
        self.assertEqual(source.latest_hour_end(), HOUR_END - gdelt.HOUR_MS)
        clock = HOUR_END / 1000 + 6 * 60
        source = GdeltEventsSource("text.gdelt.events", clock=lambda: clock)
        calls = []

        def download(stamp, timeout):
            calls.append(stamp)
            return None if stamp == HOUR_END else ("md5", FIXTURE.read_bytes())

        with mock.patch.object(gdelt, "download_batch", download):
            blob = source.fetch(5.0)
            again = source.fetch(5.0)
        self.assertIs(again, blob)
        self.assertEqual(len(calls), 4)
        global_ = next(c for c in source.parse(blob) if c.scope == "GLOBAL")
        self.assertEqual((global_.fields["events"], global_.fields["batches"]), (str(len(events)), "3"))


if __name__ == "__main__":
    unittest.main()
