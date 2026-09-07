"""Tests for the snapshot compiler and manifest: the determinism contract, end to end.

These pin the properties the rest of the product leans on: compiling is insensitive to the
on-disk order of journal lines, recompiling is a no-op on an unchanged journal, the manifest
records exactly what was compiled, a tampered derived value is refused rather than silently
re-derived, and z-score history never leaks across scopes.
"""
import random
import unittest
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from hub.compiler import compile_all, compile_dataset, verify
from hub.derive import compile_expr
from hub.errors import StoreError
from hub.journal import JournalWriter
from hub.manifest import Manifest
from hub.record import Availability, Record
from hub.schema import load_schema
from hubread.journal import journal_path

SCHEMA_YAML = """
dataset: macro.test.compiler
version: 1
title: compiler test dataset
scope_kind: currency
key: [scope, effective_at]
fields:
  actual:      { type: number, unit: pct, null_policy: allow }
  forecast:    { type: number, unit: pct, null_policy: allow }
  surprise:    { type: number, unit: pct, null_policy: allow, derived: "actual - forecast" }
  surprise_z:  { type: number, unit: pct, null_policy: allow, derived: "zscore(actual, window=5, min_obs=3)" }
"""


def ms(year, month, day, hour=0, minute=0) -> int:
    return int(datetime(year, month, day, hour, minute, tzinfo=UTC).timestamp() * 1000)


class _StubRegistry:
    """The minimal `hub.compiler.DatasetRegistry` a test needs -- the real registry loading
    `datasets/*.yaml` does not exist yet, so this stands in for it."""

    def __init__(self, schema):
        self._schema = schema

    def datasets(self):
        return [self._schema.name]

    def schema(self, name):
        assert name == self._schema.name
        return self._schema


class CompilerTest(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.root = Path(self._tmp.name)
        (self.root / "datasets").mkdir()
        schema_path = self.root / "datasets" / "macro.test.compiler.yaml"
        schema_path.write_text(SCHEMA_YAML)
        self.schema = load_schema(schema_path)
        self._history: dict[str, list[str]] = {}

    def tearDown(self):
        self._tmp.cleanup()

    def _write(self, scope, known_at, actual, forecast, revision=1):
        """Appends a record with correctly-derived `surprise`/`surprise_z`, tracking each
        scope's own `actual` history across calls so the journalled `surprise_z` matches what
        the compiler will recompute (a fresh `JournalWriter` reopens the same journal each call,
        exactly as separate collector runs would)."""
        history = self._history.setdefault(scope, [])
        surprise = compile_expr("actual - forecast")({"actual": actual, "forecast": forecast}, [])
        surprise_z = compile_expr("zscore(actual, window=5, min_obs=3)")(
            {"actual": actual}, [{"actual": v} for v in history]
        )
        record = Record.create(
            dataset=self.schema.name,
            scope=scope,
            key=f"{scope}|{known_at}",
            revision=revision,
            known_at=known_at,
            effective_at=known_at,
            availability=Availability.OBSERVED,
            source="test",
            fields={"actual": actual, "forecast": forecast, "surprise": surprise, "surprise_z": surprise_z},
        )
        with JournalWriter(self.root) as writer:
            appended = writer.append(record)
        history.append(actual)
        return appended

    def _write_with_zscore(self, scope, known_at, actual, forecast, history_actuals):
        """Appends a record whose `surprise_z` is computed correctly from `history_actuals`
        (that scope's own prior `actual` values, oldest first) -- exactly what the compiler is
        expected to recompute, so a correctly-scoped compiler accepts it."""
        payload = {"actual": actual}
        history = [{"actual": v} for v in history_actuals]
        surprise_z = compile_expr("zscore(actual, window=5, min_obs=3)")(payload, history)
        surprise = compile_expr("actual - forecast")({"actual": actual, "forecast": forecast}, [])
        record = Record.create(
            dataset=self.schema.name,
            scope=scope,
            key=f"{scope}|{known_at}",
            revision=1,
            known_at=known_at,
            effective_at=known_at,
            availability=Availability.OBSERVED,
            source="test",
            fields={"actual": actual, "forecast": forecast, "surprise": surprise, "surprise_z": surprise_z},
        )
        with JournalWriter(self.root) as writer:
            return writer.append(record)

    # -- determinism -----------------------------------------------------------------------

    def test_shuffled_journal_lines_compile_to_identical_bytes(self):
        # `actual` is held constant so `surprise_z`'s variance is always zero (a defined `None`,
        # per `hub.derive._zscore`) -- this test is about journal-line order, not about
        # exercising a real z-score value against the snapshot's scale-8 encoding limit.
        day = ms(2026, 9, 5)
        for i in range(5):
            self._write("USD", day + i * 60_000, actual="1", forecast=str(i))

        ordered = compile_dataset(self.root, self.schema, "2026-09")

        path = journal_path(self.root, self.schema.name, "2026-09-05")
        lines = path.read_text(encoding="utf-8").splitlines()
        random.Random(7).shuffle(lines)
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")

        shuffled = compile_dataset(self.root, self.schema, "2026-09")
        self.assertEqual(ordered.sha256, shuffled.sha256)
        self.assertEqual(ordered.records, shuffled.records)

    def test_recompiling_is_idempotent(self):
        self._write("USD", ms(2026, 9, 1), actual="1", forecast="0.5")
        self._write("USD", ms(2026, 9, 2), actual="2", forecast="1")

        first = compile_dataset(self.root, self.schema, "2026-09")
        second = compile_dataset(self.root, self.schema, "2026-09")

        self.assertEqual(first.sha256, second.sha256)
        self.assertEqual(first.path.read_bytes(), second.path.read_bytes())

    # -- manifest ----------------------------------------------------------------------------

    def test_manifest_records_coverage_and_hashes(self):
        self._write("USD", ms(2026, 9, 1), actual="1", forecast="0.5")
        self._write("USD", ms(2026, 9, 15), actual="2", forecast="1")
        expected = compile_dataset(self.root, self.schema, "2026-09")

        manifest = compile_all(self.root, _StubRegistry(self.schema))
        reloaded = Manifest.load(self.root)

        for view in (manifest, reloaded):
            dm = view.datasets[self.schema.name]
            self.assertEqual(dm.schema_hash, self.schema.hash())
            self.assertEqual(dm.scope_kind, "currency")
            self.assertEqual(len(dm.windows), 1)
            window = dm.windows[0]
            self.assertEqual(window.from_day, "2026-09-01")
            self.assertEqual(window.to_day, "2026-09-30")
            self.assertEqual(window.sha256, expected.sha256)
            self.assertEqual(window.records, 2)
            self.assertEqual(dm.last_known_at, ms(2026, 9, 15))
            self.assertEqual({f.name for f in dm.fields}, {"actual", "forecast", "surprise", "surprise_z"})

        self.assertEqual(verify(self.root), [])

    def test_compile_all_is_idempotent_across_processes(self):
        self._write("USD", ms(2026, 9, 1), actual="1", forecast="0.5")
        first = compile_all(self.root, _StubRegistry(self.schema))
        second = compile_all(self.root, _StubRegistry(self.schema))
        self.assertEqual(
            first.datasets[self.schema.name].windows[0].sha256,
            second.datasets[self.schema.name].windows[0].sha256,
        )

    # -- derived-field verification ------------------------------------------------------------

    def test_derived_fields_recomputed_match_journalled_values(self):
        self._write("USD", ms(2026, 9, 1), actual="1", forecast="0.4")
        self._write("USD", ms(2026, 9, 2), actual="2", forecast="1.5")
        # Raises only if recomputation disagrees with what was journalled; a clean compile means
        # every derived value already matched.
        compile_dataset(self.root, self.schema, "2026-09")

    def test_tampered_derived_value_fails_loudly(self):
        self._write("USD", ms(2026, 9, 1), actual="1", forecast="0.4")
        path = journal_path(self.root, self.schema.name, "2026-09-01")
        text = path.read_text(encoding="utf-8")
        self.assertIn('"surprise":"0.6"', text)
        tampered = text.replace('"surprise":"0.6"', '"surprise":"99"')
        self.assertNotEqual(text, tampered)
        path.write_text(tampered, encoding="utf-8")

        with self.assertRaisesRegex(StoreError, "macro.test.compiler"):
            compile_dataset(self.root, self.schema, "2026-09")

    # -- scope isolation ------------------------------------------------------------------------

    def test_zscore_is_computed_per_scope_not_across_scopes(self):
        usd_history = ["1", "1", "1", "1"]
        eur_history = ["100", "100", "100", "100"]
        for i, v in enumerate(usd_history):
            self._write_with_zscore("USD", ms(2026, 9, 1, hour=i), actual=v, forecast="0", history_actuals=[])
        for i, v in enumerate(eur_history):
            self._write_with_zscore("EUR", ms(2026, 9, 1, hour=i + 10), actual=v, forecast="0", history_actuals=[])
        # USD's current value is wildly anomalous against EUR's history (and vice versa); if the
        # compiler mixed scopes into one history this journalled (per-scope-correct) surprise_z
        # would fail to reproduce and compilation would raise.
        self._write_with_zscore("USD", ms(2026, 9, 2), actual="50", forecast="0", history_actuals=usd_history)
        self._write_with_zscore("EUR", ms(2026, 9, 2), actual="5000", forecast="0", history_actuals=eur_history)

        result = compile_dataset(self.root, self.schema, "2026-09")
        self.assertEqual(result.records, 10)


if __name__ == "__main__":
    unittest.main()
