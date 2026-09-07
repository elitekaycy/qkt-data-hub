"""Tests for the QKH1 snapshot format: encode/decode round trip and corruption handling.

The compiler's whole value proposition rests on a snapshot being deterministic (the same
records always produce the same bytes) and self-verifying (a consumer can detect truncation or
bit rot without a second source of truth). These tests pin both, plus the scale-8 fixed-point
contract for numbers, which is the one place a silent precision loss could hide.
"""
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from hub.errors import StoreError
from hub.journal import JournalWriter
from hub.record import Availability, Record
from hub.schema import load_schema
from hub.snapshot import encode
from hubread.snapshot import decode

SCHEMA_YAML = """
dataset: mkt.test.snap
version: 1
title: snapshot format test dataset
scope_kind: instrument
key: [scope, effective_at]
fields:
  title:    { type: string, strategy: false }
  price:    { type: number, unit: usd, null_policy: allow }
  active:   { type: bool, null_policy: allow }
  asof:     { type: timestamp, null_policy: allow }
  impact:   { type: enum, values: [low, medium, high], null_policy: allow }
"""

BASE = dict(
    dataset="mkt.test.snap",
    revision=1,
    availability=Availability.OBSERVED,
    source="test",
)


def load_test_schema():
    with TemporaryDirectory() as d:
        p = Path(d) / "mkt.test.snap.yaml"
        p.write_text(SCHEMA_YAML)
        return load_schema(p)


def make(scope="EURUSD", known_at=1_757_260_800_000, effective_at=1_757_260_800_000, seq=1, **fields):
    return Record.create(
        **BASE,
        scope=scope,
        key=f"{scope}|{effective_at}",
        known_at=known_at,
        effective_at=effective_at,
        seq=seq,
        fields=fields,
    )


class SnapshotRoundTripTest(unittest.TestCase):
    def setUp(self):
        self.schema = load_test_schema()

    def test_round_trips_every_storable_field_type(self):
        record = make(price="1850.5", active=True, asof=1_757_000_000_000, impact=2, title="ignored")
        header, records = decode(encode(self.schema, [record]))
        self.assertEqual(header.dataset, "mkt.test.snap")
        self.assertEqual(header.record_count, 1)
        self.assertEqual({f.name for f in header.fields}, {"price", "active", "asof", "impact"})
        decoded = records[0]
        self.assertEqual(decoded.fields["price"], "1850.5")
        self.assertEqual(decoded.fields["active"], True)
        self.assertEqual(decoded.fields["asof"], 1_757_000_000_000)
        self.assertEqual(decoded.fields["impact"], 2)
        self.assertNotIn("title", decoded.fields)

    def test_string_fields_are_not_stored(self):
        record = make(title="US CPI", price="1", active=False, asof=1, impact=0)
        header, _ = decode(encode(self.schema, [record]))
        self.assertNotIn("title", {f.name for f in header.fields})

    def test_nulls_survive(self):
        record = make(price=None, active=None, asof=None, impact=None)
        _, records = decode(encode(self.schema, [record]))
        self.assertEqual(records[0].fields, {"price": None, "active": None, "asof": None, "impact": None})

    def test_envelope_and_ordering_fields_round_trip(self):
        r1 = make(scope="EURUSD", known_at=1_757_260_800_000, seq=1, price="1")
        r2 = make(scope="GBPUSD", known_at=1_757_260_900_000, seq=2, price="2", asof=None)
        r2 = r2.replace(period_start=1_757_000_000_000, period_end=1_757_100_000_000).with_id()
        _, records = decode(encode(self.schema, [r1, r2]))
        by_seq = {r.seq: r for r in records}
        self.assertEqual(by_seq[1].scope, "EURUSD")
        self.assertEqual(by_seq[1].known_at, 1_757_260_800_000)
        self.assertIsNone(by_seq[1].period_start)
        self.assertEqual(by_seq[2].period_start, 1_757_000_000_000)
        self.assertEqual(by_seq[2].period_end, 1_757_100_000_000)

    def test_decimal_with_half_step_round_trips_exactly(self):
        record = make(price="1850.5")
        _, records = decode(encode(self.schema, [record]))
        self.assertEqual(records[0].fields["price"], "1850.5")

    def test_value_needing_more_than_scale_precision_raises(self):
        record = make(price="1.123456789")
        with self.assertRaises(StoreError):
            encode(self.schema, [record])

    def test_encoding_twice_is_byte_identical(self):
        records = [make(scope="EURUSD", seq=1, price="1"), make(scope="GBPUSD", known_at=1_757_260_900_000, seq=2)]
        self.assertEqual(encode(self.schema, records), encode(self.schema, records))

    def test_encoding_a_shuffled_copy_is_byte_identical(self):
        records = [
            make(scope="EURUSD", known_at=1_757_260_800_000, seq=1, price="1"),
            make(scope="GBPUSD", known_at=1_757_260_900_000, seq=2, price="2"),
            make(scope="USDJPY", known_at=1_757_261_000_000, seq=3, price="3"),
        ]
        forward = encode(self.schema, records)
        shuffled = encode(self.schema, list(reversed(records)))
        self.assertEqual(forward, shuffled)

    def test_truncated_buffer_raises(self):
        data = encode(self.schema, [make()])
        with self.assertRaises(StoreError):
            decode(data[:-10])

    def test_flipped_byte_fails_trailer_check(self):
        data = bytearray(encode(self.schema, [make()]))
        data[-33] ^= 0xFF
        with self.assertRaises(StoreError):
            decode(bytes(data))

    def test_encode_empty_record_list(self):
        header, records = decode(encode(self.schema, []))
        self.assertEqual(header.record_count, 0)
        self.assertEqual(records, [])

    def test_absent_field_and_explicit_none_both_decode_to_none(self):
        absent = make(price="1")
        explicit_none = make(price="1", active=None, asof=None, impact=None)
        _, from_absent = decode(encode(self.schema, [absent]))
        _, from_none = decode(encode(self.schema, [explicit_none]))
        expected = {"price": "1", "active": None, "asof": None, "impact": None}
        self.assertEqual(from_absent[0].fields, expected)
        self.assertEqual(from_none[0].fields, expected)

    def test_record_round_trips_through_journal_and_snapshot_with_full_equality(self):
        with TemporaryDirectory() as d:
            root = Path(d)
            original = make(scope="EURUSD", price="1.5", active=True, asof=1_757_000_000_000, impact=1)
            original = original.replace(source_version="v2", parser="fx-parser", raw_ref="sha256:abc123").with_id()
            with JournalWriter(root) as w:
                stamped = w.append(original)
            header, records = decode(encode(self.schema, [stamped]))
            self.assertEqual(records[0], stamped)
            self.assertIn("v2", header.source_versions)
            self.assertIn("fx-parser", header.parsers)
            self.assertIn("sha256:abc123", header.raw_refs)

    def test_null_raw_ref_round_trips_to_none(self):
        record = make(price="1")
        self.assertIsNone(record.raw_ref)
        _, records = decode(encode(self.schema, [record]))
        self.assertIsNone(records[0].raw_ref)


if __name__ == "__main__":
    unittest.main()
