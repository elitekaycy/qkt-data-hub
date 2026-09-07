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
CALENDAR = pathlib.Path(__file__).resolve().parents[1] / "datasets" / "cal.high_impact.yaml"


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
