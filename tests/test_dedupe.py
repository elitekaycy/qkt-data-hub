import unittest

from hub.dedupe import RevisionIndex, assign
from hub.record import Availability, Record

BASE = dict(
    dataset="macro.us.cpi", scope="USD", key="2026-08|USD|CPI", known_at=1_760_000_000_000,
    effective_at=1_760_000_000_000, availability=Availability.OBSERVED, source="test", revision=1,
)


class DedupeTest(unittest.TestCase):
    def test_first_sighting_is_revision_one(self):
        idx = RevisionIndex()
        out = assign(idx, Record.create(**BASE, fields={"actual": None}))
        self.assertEqual(out.revision, 1)

    def test_identical_payload_is_dropped(self):
        idx = RevisionIndex()
        assign(idx, Record.create(**BASE, fields={"actual": None}))
        self.assertIsNone(assign(idx, Record.create(**{**BASE, "known_at": BASE["known_at"] + 60_000},
                                                    fields={"actual": None})))

    def test_changed_payload_becomes_next_revision(self):
        idx = RevisionIndex()
        assign(idx, Record.create(**BASE, fields={"actual": None}))
        out = assign(idx, Record.create(**{**BASE, "known_at": BASE["known_at"] + 60_000},
                                        fields={"actual": "0.3"}))
        self.assertEqual(out.revision, 2)

    def test_rebuild_from_journal_restores_state(self):
        idx = RevisionIndex()
        r1 = assign(idx, Record.create(**BASE, fields={"actual": None}))
        fresh = RevisionIndex()
        fresh.rebuild_from([r1])
        self.assertIsNone(assign(fresh, Record.create(**BASE, fields={"actual": None})))

    def test_assigned_record_round_trips_through_json(self):
        idx = RevisionIndex()
        first = assign(idx, Record.create(**BASE, fields={"actual": None}))
        second = assign(idx, Record.create(**{**BASE, "known_at": BASE["known_at"] + 60_000},
                                            fields={"actual": "0.3"}))
        for record in (first, second):
            round_tripped = Record.from_json(record.to_json())
            self.assertEqual(round_tripped.id, record.id)
            self.assertEqual(round_tripped.revision, record.revision)

    def test_rebuild_from_takes_highest_revision_not_last(self):
        idx = RevisionIndex()
        r1 = assign(idx, Record.create(**BASE, fields={"actual": None}))
        r2 = assign(idx, Record.create(**{**BASE, "known_at": BASE["known_at"] + 60_000},
                                        fields={"actual": "0.3"}))
        fresh = RevisionIndex()
        # Journal order by known_at is not guaranteed monotonic in revision, so feed the
        # higher-revision record first and confirm it still wins.
        fresh.rebuild_from([r2, r1])
        self.assertEqual(fresh.seen(r1.dataset, r1.key), (r2.revision, r2.payload_hash()))

    def test_different_keys_are_independent(self):
        idx = RevisionIndex()
        out_a = assign(idx, Record.create(**BASE, fields={"actual": None}))
        other = {**BASE, "key": "2026-09|USD|CPI"}
        out_b = assign(idx, Record.create(**other, fields={"actual": None}))
        self.assertEqual(out_a.revision, 1)
        self.assertEqual(out_b.revision, 1)


if __name__ == "__main__":
    unittest.main()
