"""Tests for the point-in-time reader: the look-ahead property, revision visibility, policy,
and the replay-equivalence between reading a compiled snapshot and reading the raw journal.

`hubread` is what a kill-switch daemon or a trading engine vendors on its own, so every test here
exercises it exactly as that consumer would: through `hubread.reader.Reader` and
`hubread.policy.Policy` alone. Building the fixtures still goes through `hub` (the writer and
compiler), because a reader test needs something written before it can read it back.
"""
import random
import unittest
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from hub.compiler import compile_all
from hub.journal import JournalWriter
from hub.record import Availability, Record
from hub.schema import load_schema
from hubread.journal import read_range
from hubread.policy import Policy
from hubread.reader import Reader

SCHEMA_YAML = """
dataset: macro.test.reader
version: 1
title: reader test dataset
scope_kind: currency
key: [scope, effective_at]
fields:
  actual:   { type: number, unit: pct, null_policy: allow }
  forecast: { type: number, unit: pct, null_policy: allow }
"""


def ms(year, month, day, hour=0, minute=0, second=0) -> int:
    return int(datetime(year, month, day, hour, minute, second, tzinfo=UTC).timestamp() * 1000)


class _StubRegistry:
    def __init__(self, schema):
        self._schema = schema

    def datasets(self):
        return [self._schema.name]

    def schema(self, name):
        return self._schema


class ReaderTestBase(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.root = Path(self._tmp.name)
        (self.root / "datasets").mkdir()
        schema_path = self.root / "datasets" / "macro.test.reader.yaml"
        schema_path.write_text(SCHEMA_YAML)
        self.schema = load_schema(schema_path)

    def tearDown(self):
        self._tmp.cleanup()

    def _append(self, *, scope, known_at, revision=1, effective_at=None, availability=Availability.OBSERVED,
                actual="1", forecast="0.5", key=None):
        effective_at = known_at if effective_at is None else effective_at
        record = Record.create(
            dataset=self.schema.name,
            scope=scope,
            key=key or f"{scope}|{effective_at}",
            revision=revision,
            known_at=known_at,
            effective_at=effective_at,
            availability=availability,
            source="test",
            fields={"actual": actual, "forecast": forecast},
        )
        with JournalWriter(self.root) as writer:
            return writer.append(record)


class LookAheadPropertyTest(ReaderTestBase):
    def test_as_of_never_returns_a_record_from_the_future(self):
        rnd = random.Random(11)
        base = ms(2026, 9, 1)
        known_ats = sorted(base + rnd.randint(0, 20) * 3_600_000 for _ in range(30))
        for i, known_at in enumerate(known_ats):
            self._append(scope="USD", known_at=known_at, effective_at=known_at, key=f"k{i}")

        reader = Reader(self.root)
        sample_times = [base - 3_600_000 + rnd.randint(0, 30) * 3_600_000 for _ in range(200)]
        for t in sample_times:
            for record in reader.as_of("macro.test.reader", "USD", t).values():
                self.assertLessEqual(record.known_at, t)


class RevisionVisibilityTest(ReaderTestBase):
    def test_a_revision_is_invisible_before_its_own_known_at(self):
        t1 = ms(2026, 9, 1)
        t2 = ms(2026, 9, 2)
        key = "USD|EVENT"
        self._append(scope="USD", known_at=t1, revision=1, key=key, actual="1")
        self._append(scope="USD", known_at=t2, revision=2, key=key, actual="2")

        reader = Reader(self.root)
        self.assertEqual(reader.as_of("macro.test.reader", "USD", t2 - 1)[key].revision, 1)
        self.assertEqual(reader.as_of("macro.test.reader", "USD", t2)[key].revision, 2)


class MinLagTest(ReaderTestBase):
    def test_min_lag_delays_visibility_by_exactly_that_amount(self):
        known_at = ms(2026, 9, 1)
        key = "USD|LAGGED"
        self._append(scope="USD", known_at=known_at, key=key)

        reader = Reader(self.root, policy=Policy(min_lag_ms=1_000))
        self.assertNotIn(key, reader.as_of("macro.test.reader", "USD", known_at + 999))
        self.assertIn(key, reader.as_of("macro.test.reader", "USD", known_at + 1_000))


class RefuseDerivedTest(ReaderTestBase):
    def test_refuse_derived_hides_backfilled_records_and_shows_them_when_allowed(self):
        known_at = ms(2026, 9, 1)
        key = "USD|BACKFILLED"
        self._append(scope="USD", known_at=known_at, key=key, availability=Availability.DERIVED)

        permissive = Reader(self.root, policy=Policy(refuse_derived=False))
        strict = Reader(self.root, policy=Policy(refuse_derived=True))

        self.assertIn(key, permissive.as_of("macro.test.reader", "USD", known_at))
        self.assertNotIn(key, strict.as_of("macro.test.reader", "USD", known_at))


class ReplayEquivalenceTest(ReaderTestBase):
    def test_snapshot_and_journal_paths_agree(self):
        for day in range(1, 6):
            known_at = ms(2026, 9, day)
            self._append(scope="USD", known_at=known_at, key=f"USD|{day}", actual=str(day))
        for day in range(1, 4):
            known_at = ms(2026, 9, day, hour=1)
            self._append(scope="EUR", known_at=known_at, key=f"EUR|{day}", actual=str(day))

        compile_all(self.root, _StubRegistry(self.schema))

        known_from = ms(2026, 9, 1)
        known_to = ms(2026, 9, 30, 23, 59, 59)

        from_journal = sorted(read_range(self.root, self.schema.name, known_from, known_to), key=lambda r: r.id)

        reader = Reader(self.root)
        dataset_info = reader._manifest[self.schema.name]
        self.assertTrue(dataset_info.windows, "expected a compiled window to exercise the snapshot path")
        from_snapshot = sorted(reader.range(self.schema.name, known_from, known_to), key=lambda r: r.id)

        self.assertEqual(from_snapshot, from_journal)
        self.assertTrue(from_journal, "fixture produced no records; the test would pass vacuously")


class ManifestViewTest(ReaderTestBase):
    def test_schema_reads_field_table_from_manifest_without_yaml(self):
        self._append(scope="USD", known_at=ms(2026, 9, 1), key="USD|1")
        compile_all(self.root, _StubRegistry(self.schema))

        reader = Reader(self.root)
        self.assertEqual(reader.datasets(), [self.schema.name])
        schema = reader.schema(self.schema.name)
        self.assertEqual(schema.scope_kind, "currency")
        self.assertEqual({f.name for f in schema.fields}, {"actual", "forecast"})


class HealthTest(ReaderTestBase):
    def test_health_reports_stale_when_heartbeat_is_missing(self):
        reader = Reader(self.root)
        health = reader.health()
        self.assertTrue(health.stale)
        self.assertIsNone(health.heartbeat_at)

    def test_health_reports_fresh_heartbeat_as_not_stale(self):
        (self.root / "heartbeat").write_text("")
        reader = Reader(self.root, policy=Policy(stale_after_ms=900_000))
        health = reader.health()
        self.assertFalse(health.stale)
        self.assertIsNotNone(health.age_ms)


class WindowsTest(ReaderTestBase):
    def test_windows_returns_padded_intervals_for_imminent_effective_events(self):
        t = ms(2026, 9, 1, 12)
        soon = t + 10 * 60_000
        far = t + 3 * 3_600_000
        self._append(scope="USD", known_at=t - 60_000, effective_at=soon, key="USD|SOON")
        self._append(scope="USD", known_at=t - 60_000, effective_at=far, key="USD|FAR")

        reader = Reader(self.root)
        intervals = reader.windows("macro.test.reader", "USD", t, ahead_ms=30 * 60_000, pad_ms=1_000)

        self.assertEqual(intervals, [(soon - 1_000, soon + 1_000)])


if __name__ == "__main__":
    unittest.main()
