"""Tests for the acquisition half: raw archive, quarantine, HTTP source, declarative parsing.

Every test here runs against captured bytes or a local fixture server. Nothing reaches the
network: a test that depends on a third party is a test that fails for reasons unrelated to the
change being made, and this suite has to be trustworthy enough to gate a release.
"""
from __future__ import annotations

import datetime as dt
import http.server
import json
import pathlib
import tempfile
import threading
import unittest
from typing import Any

from hub.collectors import Candidate
from hub.collectors.declarative import DeclarativeSource, duration_seconds
from hub.collectors.http import HttpSource
from hub.dedupe import RevisionIndex
from hub.pipeline import build_key, ingest
from hub.quarantine import Quarantine
from hub.rawstore import RawBlob, RawStore
from hub.registry import Registry
from hub.schema import load_schema
from hubread.errors import ConfigError, StoreError
from hubread.record import Availability

FIXTURES = pathlib.Path(__file__).parent / "fixtures"
DATASETS = pathlib.Path(__file__).resolve().parents[1] / "datasets"
CALENDAR = DATASETS / "cal.high_impact.yaml"
REAL_YIELD = DATASETS / "rates.us.dfii10.yaml"


def _serve(body: bytes, content_type: str, status: int = 200) -> tuple[str, Any]:
    """Start a one-request local HTTP server and return its URL and the server object."""

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - the stdlib dictates this name
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args: Any) -> None:
            return

    server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return f"http://127.0.0.1:{server.server_port}/feed", server


class RawStoreTest(unittest.TestCase):
    def setUp(self) -> None:
        self.root = pathlib.Path(tempfile.mkdtemp())
        self.store = RawStore(self.root)
        self.blob = RawBlob(body=b"hello", content_type="application/json", fetched_at=1_760_000_000_000)

    def test_put_is_content_addressed_and_idempotent(self) -> None:
        first = self.store.put(self.blob)
        second = self.store.put(RawBlob(body=b"hello", content_type="text/csv", fetched_at=1))
        self.assertEqual(first, second)
        self.assertTrue(self.store.has(first))

    def test_get_returns_the_original_bytes(self) -> None:
        digest = self.store.put(self.blob)
        self.assertEqual(self.store.get(digest).body, b"hello")

    def test_get_raises_for_an_unknown_digest(self) -> None:
        with self.assertRaises(StoreError):
            self.store.get("sha256:" + "0" * 64)


class QuarantineTest(unittest.TestCase):
    def test_write_then_count_for_the_day(self) -> None:
        root = pathlib.Path(tempfile.mkdtemp())
        q = Quarantine(root)
        now = 1_760_000_000_000
        q.write("cal.high_impact", "bad row", {"a": 1}, now)
        q.write("cal.high_impact", "bad row", {"a": 2}, now)
        self.assertEqual(q.count("cal.high_impact", now), 2)
        self.assertEqual(q.count("cal.high_impact", now + 86_400_000), 0)

    def test_unserialisable_payload_is_still_recorded(self) -> None:
        root = pathlib.Path(tempfile.mkdtemp())
        q = Quarantine(root)
        q.write("cal.high_impact", "weird", {1, 2, 3}, 1_760_000_000_000)
        self.assertEqual(q.count("cal.high_impact", 1_760_000_000_000), 1)


class HttpSourceTest(unittest.TestCase):
    def test_fetch_returns_the_body(self) -> None:
        url, server = _serve(b'[{"a": 1}]', "application/json")
        try:
            blob = HttpSource("t", url, expect_content_type="json").fetch(5.0)
            self.assertIsNotNone(blob)
            assert blob is not None
            self.assertEqual(blob.body, b'[{"a": 1}]')
        finally:
            server.shutdown()
            server.server_close()

    def test_wrong_content_type_is_refused_even_on_200(self) -> None:
        # The observed failure mode in a sibling project: an HTML error page served with a 200
        # while the JSON parser quietly produced nothing and the system looked healthy.
        url, server = _serve(b"<html>down for maintenance</html>", "text/html")
        try:
            self.assertIsNone(HttpSource("t", url, expect_content_type="json").fetch(5.0))
        finally:
            server.shutdown()
            server.server_close()

    def test_empty_body_is_refused(self) -> None:
        url, server = _serve(b"", "application/json")
        try:
            self.assertIsNone(HttpSource("t", url, expect_content_type="json").fetch(5.0))
        finally:
            server.shutdown()
            server.server_close()

    def test_server_error_returns_none_rather_than_raising(self) -> None:
        url, server = _serve(b"nope", "application/json", status=500)
        try:
            self.assertIsNone(HttpSource("t", url, expect_content_type="json").fetch(5.0))
        finally:
            server.shutdown()
            server.server_close()


class DurationTest(unittest.TestCase):
    def test_units(self) -> None:
        self.assertEqual(duration_seconds("30s", "p"), 30.0)
        self.assertEqual(duration_seconds("15m", "p"), 900.0)
        self.assertEqual(duration_seconds("6h", "p"), 21600.0)
        self.assertEqual(duration_seconds("1d", "p"), 86400.0)
        self.assertEqual(duration_seconds("45", "p"), 45.0)

    def test_bad_duration_raises(self) -> None:
        with self.assertRaises(ConfigError):
            duration_seconds("soon", "p")


class DeclarativeCalendarTest(unittest.TestCase):
    """The shipped calendar dataset, parsed from the real captured feed."""

    def setUp(self) -> None:
        self.schema = load_schema(CALENDAR)
        self.source = DeclarativeSource("cal.high_impact", self.schema.raw["collector"])
        self.blob = RawBlob(
            body=(FIXTURES / "forexfactory_thisweek.json").read_bytes(),
            content_type="application/json",
            fetched_at=1_760_000_000_000,
        )
        self.candidates = self.source.parse(self.blob)

    def test_filters_to_the_declared_impact_levels(self) -> None:
        rows = json.loads(self.blob.body)
        wanted = [r for r in rows if r.get("impact") in ("High", "Holiday")]
        self.assertEqual(len(self.candidates), len(wanted))
        self.assertLess(len(self.candidates), len(rows))

    def test_new_york_offset_is_converted_to_utc(self) -> None:
        rows = json.loads(self.blob.body)
        row = next(r for r in rows if r.get("impact") == "High" and r["date"].endswith("-04:00"))
        want = int(dt.datetime.fromisoformat(row["date"]).timestamp() * 1000)
        match = next(c for c in self.candidates if c.effective_at == want)
        self.assertEqual(match.scope, row["country"].upper())

    def test_an_unfilled_actual_is_none_not_zero(self) -> None:
        # A pre-release row has no `actual` key at all. Turning that into 0 would invent a
        # surprise of exactly minus the forecast on every upcoming release.
        self.assertTrue(any(c.fields["actual"] is None for c in self.candidates))
        self.assertFalse(any(c.fields["actual"] == "0" for c in self.candidates))

    def test_scope_is_a_currency_code_and_all_is_mapped(self) -> None:
        for candidate in self.candidates:
            self.assertRegex(candidate.scope, r"^[A-Z]{3}$|^ALL$")

    def test_unknown_collector_key_is_rejected(self) -> None:
        collector = dict(self.schema.raw["collector"])
        collector["nonsense"] = 1
        with self.assertRaisesRegex(ConfigError, "unknown"):
            DeclarativeSource("cal.high_impact", collector)

    def test_parse_does_no_io(self) -> None:
        # Re-parsing archived bytes must be possible with the network gone; if `parse` reached
        # out, a corrected parser could never be re-run over history.
        def explode(*_a: Any, **_k: Any) -> None:
            raise AssertionError("parse performed I/O")

        source = DeclarativeSource("cal.high_impact", self.schema.raw["collector"], opener=explode)
        self.assertEqual(len(source.parse(self.blob)), len(self.candidates))


class PipelineTest(unittest.TestCase):
    def setUp(self) -> None:
        self.schema = load_schema(CALENDAR)
        source = DeclarativeSource("cal.high_impact", self.schema.raw["collector"])
        self.candidates = source.parse(
            RawBlob(
                body=(FIXTURES / "forexfactory_thisweek.json").read_bytes(),
                content_type="application/json",
                fetched_at=0,
            )
        )
        self.now = 1_789_000_000_000

    def _ingest(self, **kwargs: Any) -> Any:
        return ingest(
            self.candidates,
            self.schema,
            kwargs.pop("index", RevisionIndex()),
            kwargs.pop("history", {}),
            kwargs.pop("now_ms", self.now),
            source="forexfactory",
            **kwargs,
        )

    def test_every_candidate_becomes_a_record(self) -> None:
        result = self._ingest()
        self.assertEqual(result.written, len(self.candidates))
        self.assertEqual(result.quarantined, 0)

    def test_live_ingest_stamps_observed_from_the_ingest_clock(self) -> None:
        # A provider must never be able to tell us when we knew something.
        for record in self._ingest().records:
            self.assertEqual(record.availability, Availability.OBSERVED)
            self.assertEqual(record.known_at, self.now)

    def test_reingesting_the_same_payload_writes_nothing(self) -> None:
        index, history = RevisionIndex(), {}
        first = self._ingest(index=index, history=history)
        second = self._ingest(index=index, history=history, now_ms=self.now + 60_000)
        self.assertEqual(first.written, len(self.candidates))
        self.assertEqual(second.written, 0)
        self.assertEqual(second.duplicates, len(self.candidates))

    def test_a_clock_ahead_of_the_writer_is_quarantined(self) -> None:
        root = pathlib.Path(tempfile.mkdtemp())
        quarantine = Quarantine(root)
        result = ingest(
            self.candidates,
            self.schema,
            RevisionIndex(),
            {},
            self.now,
            source="forexfactory",
            quarantine=quarantine,
            clock=lambda: self.now - 3_600_000,
        )
        self.assertEqual(result.written, 0)
        self.assertEqual(result.quarantined, len(self.candidates))
        self.assertGreater(quarantine.count("cal.high_impact", self.now), 0)

    def test_key_is_stable_across_revisions_and_excludes_known_at(self) -> None:
        candidate = self.candidates[0]
        self.assertEqual(build_key(self.schema, candidate), build_key(self.schema, candidate))
        self.assertNotIn(str(self.now), build_key(self.schema, candidate))


class MaxObservedAgeTest(unittest.TestCase):
    """A source with no incremental fetch mode -- an http_csv endpoint that always returns its
    provider's whole history, as every FRED collector here does -- hands the pipeline candidates
    spanning decades on every poll. Journaling all of that as `observed` fabricates `known_at`
    for every date but the newest, and it does something worse: `derive`'s per-scope history is
    a list appended to in candidate order, so a batch spanning years, run against a scope that
    already has backfilled history, interleaves ahead of or behind what is already stored and
    scrambles every `diff`/`lag`/`zscore` computed from it. This was found by actually deploying
    the hub: `rates.us.dfii10` is a real dataset with a real `diff(value, 1)` field, backfilled
    for 2018-2026 and then live-collected once against the full FRED series back to 1962 -- 1058
    of 2264 already-correct records were silently revised with a fabricated `change_1d`, all of
    it passing `verify` because the compiler's own re-derivation walks history the same way.
    `max_observed_age_ms` exists to make that batch never reach `derive` in the first place.
    """

    DAY_MS = 86_400_000

    def setUp(self) -> None:
        self.schema = load_schema(REAL_YIELD)
        # Four consecutive trading days plus one candidate decades earlier, standing in for the
        # oldest rows a full-history CSV refetch would include.
        self.day1 = 1_514_851_200_000  # 2018-01-02T00:00:00Z
        self.day2 = self.day1 + self.DAY_MS
        self.day3 = self.day2 + self.DAY_MS
        self.day4 = self.day3 + self.DAY_MS
        self.ancient = 1_262_563_200_000  # 2010-01-04T00:00:00Z -- 8 years before day 1

    def _candidate(self, period_start: int, value: str, known_at: int) -> Candidate:
        return Candidate(
            scope="USD",
            key="",
            effective_at=period_start,
            fields={"value": value},
            period_start=period_start,
            known_at=known_at,
            availability=Availability.DERIVED,
        )

    def _backfill(self) -> tuple[RevisionIndex, dict]:
        """Seed index/history exactly as `_history_for` would after a real backfill: three
        ascending trading days, each with a genuinely staggered `known_at`."""
        candidates = [
            self._candidate(self.day1, "1.00", self.day1 + self.DAY_MS),
            self._candidate(self.day2, "1.10", self.day2 + self.DAY_MS),
            self._candidate(self.day3, "1.20", self.day3 + self.DAY_MS),
        ]
        index, history = RevisionIndex(), {}
        result = ingest(
            candidates, self.schema, index, history, self.day3 + 2 * self.DAY_MS, source="fred", live=False
        )
        self.assertEqual(result.written, 3)
        return index, history

    def _day1_change_1d(self, index: RevisionIndex, history: dict) -> str | None:
        # Re-derive nothing; read straight off what ingest already computed and stored. Day 1
        # is always the first entry appended for this scope.
        return history["USD"][0]["change_1d"]

    def test_backfill_alone_leaves_the_first_day_with_no_prior(self) -> None:
        index, history = self._backfill()
        self.assertIsNone(self._day1_change_1d(index, history))

    def test_an_unbounded_wide_batch_corrupts_already_backfilled_history(self) -> None:
        # Pins the exact failure observed in production: with no bound, the ancient candidate
        # is appended ahead of nothing (it is new), but by the time the batch revisits day 1,
        # `scope_history[-1]` is now the ancient row instead of "no prior" -- the earliest
        # legitimate observation acquires a fabricated change.
        index, history = self._backfill()
        wide_batch = [
            self._candidate(self.ancient, "5.00", 0),
            self._candidate(self.day1, "1.00", 0),
            self._candidate(self.day2, "1.10", 0),
            self._candidate(self.day3, "1.20", 0),
            self._candidate(self.day4, "1.30", 0),
        ]
        now = self.day4 + 1_000
        result = ingest(wide_batch, self.schema, index, history, now, source="fred", live=True)
        self.assertGreater(result.revisions, 0)
        day1_revision = index.seen(self.schema.name, f"{build_key(self.schema, wide_batch[1])}")
        self.assertIsNotNone(day1_revision)
        self.assertGreater(day1_revision[0], 1, "day 1 should not have been revised at all")

    def test_max_observed_age_rejects_the_stale_candidates_and_leaves_history_intact(self) -> None:
        index, history = self._backfill()
        wide_batch = [
            self._candidate(self.ancient, "5.00", 0),
            self._candidate(self.day1, "1.00", 0),
            self._candidate(self.day2, "1.10", 0),
            self._candidate(self.day3, "1.20", 0),
            self._candidate(self.day4, "1.30", 0),
        ]
        now = self.day4 + 1_000
        root = pathlib.Path(tempfile.mkdtemp())
        result = ingest(
            wide_batch,
            self.schema,
            index,
            history,
            now,
            source="fred",
            live=True,
            max_observed_age_ms=12 * 3_600_000,  # 12h: rejects anything a day or more old
            quarantine=Quarantine(root),
        )
        self.assertEqual(result.written, 1, "only day 4 is recent enough to be live-observed")
        self.assertEqual(result.quarantined, 4)
        self.assertEqual(result.revisions, 0, "the three already-backfilled days must stay untouched")
        day1_key = build_key(self.schema, wide_batch[1])
        self.assertEqual(index.seen(self.schema.name, day1_key)[0], 1)
        day4_record = result.records[0]
        self.assertEqual(day4_record.fields["change_1d"], "0.1")

    def test_backfill_is_never_subject_to_the_age_bound(self) -> None:
        # The whole point of `backfill` is submitting old facts honestly, stamped `derived`; the
        # guard only ever applies to a live (`observed`) collect.
        index, history = RevisionIndex(), {}
        result = ingest(
            [self._candidate(self.ancient, "1.00", self.ancient + self.DAY_MS)],
            self.schema,
            index,
            history,
            self.ancient + self.DAY_MS,
            source="fred",
            live=False,
            max_observed_age_ms=1,
        )
        self.assertEqual(result.written, 1)
        self.assertEqual(result.quarantined, 0)


class RegistryTest(unittest.TestCase):
    def test_loads_the_shipped_datasets(self) -> None:
        registry = Registry.load(CALENDAR.parent)
        self.assertIn("cal.high_impact", registry.datasets())
        self.assertIn("cal.high_impact", registry.collecting())
        self.assertEqual(registry.schema("cal.high_impact").scope_kind, "currency")

    def test_unknown_dataset_raises(self) -> None:
        registry = Registry.load(CALENDAR.parent)
        with self.assertRaises(ConfigError):
            registry.schema("nope.nope")

    def test_missing_directory_raises(self) -> None:
        with self.assertRaises(ConfigError):
            Registry.load(pathlib.Path(tempfile.mkdtemp()) / "absent")


if __name__ == "__main__":
    unittest.main()
