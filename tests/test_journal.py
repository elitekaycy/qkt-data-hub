"""Tests for the journal: append-only writer, historical range read, and live tailer.

These pin the invariants the rest of the product leans on without re-checking: `seq` is a
strictly increasing per-dataset counter that survives a writer restart, only one writer may
hold a root at a time, a line is either fully written or treated as not-yet-written, and a
live tail never raises on a single bad line while a historical read never silently drops one.
"""
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from hub.errors import StoreError
from hub.journal import JournalWriter
from hub.record import Availability, Record
from hubread.journal import JournalTail, journal_path, read_range

BASE = dict(
    dataset="macro.us.cpi",
    scope="USD",
    key="2026-08|USD|CPI",
    revision=1,
    known_at=1_757_260_800_000,  # 2025-09-07T12:00:00Z
    effective_at=1_757_260_800_000,
    availability=Availability.OBSERVED,
    source="test",
)


def make(**overrides):
    kwargs = {**BASE, **overrides}
    fields = kwargs.pop("fields", {"actual": "0.3"})
    return Record.create(fields=fields, **kwargs)


class JournalWriterTest(unittest.TestCase):
    def test_seq_starts_at_one_and_increases(self):
        with TemporaryDirectory() as d:
            with JournalWriter(Path(d)) as w:
                r1 = w.append(make())
                r2 = w.append(make(known_at=BASE["known_at"] + 1000))
                self.assertEqual(r1.seq, 1)
                self.assertEqual(r2.seq, 2)

    def test_append_returns_record_with_stamped_id(self):
        with TemporaryDirectory() as d:
            with JournalWriter(Path(d)) as w:
                stamped = w.append(make())
                self.assertEqual(stamped.seq, 1)
                self.assertEqual(stamped.id, stamped.compute_id())

    def test_restart_resumes_seq_from_tail(self):
        with TemporaryDirectory() as d:
            root = Path(d)
            with JournalWriter(root) as w:
                w.append(make())
                w.append(make(known_at=BASE["known_at"] + 1000))
            with JournalWriter(root) as w2:
                r3 = w2.append(make(known_at=BASE["known_at"] + 2000))
                self.assertEqual(r3.seq, 3)

    def test_second_writer_on_same_root_conflicts(self):
        with TemporaryDirectory() as d:
            root = Path(d)
            with JournalWriter(root):
                with self.assertRaises(StoreError):
                    JournalWriter(root)

    def test_closed_writer_releases_lock_for_next_writer(self):
        with TemporaryDirectory() as d:
            root = Path(d)
            w = JournalWriter(root)
            w.append(make())
            w.close()
            with JournalWriter(root) as w2:
                r = w2.append(make(known_at=BASE["known_at"] + 1000))
                self.assertEqual(r.seq, 2)

    def test_day_roll_writes_a_new_dated_file(self):
        with TemporaryDirectory() as d:
            root = Path(d)
            with JournalWriter(root) as w:
                w.append(make(known_at=BASE["known_at"]))
                w.append(make(known_at=BASE["known_at"] + 86_400_000))
            p1 = journal_path(root, BASE["dataset"], "2025-09-07")
            p2 = journal_path(root, BASE["dataset"], "2025-09-08")
            self.assertTrue(p1.exists())
            self.assertTrue(p2.exists())


class ReadRangeTest(unittest.TestCase):
    def test_reads_records_in_range(self):
        with TemporaryDirectory() as d:
            root = Path(d)
            with JournalWriter(root) as w:
                w.append(make(known_at=BASE["known_at"]))
                w.append(make(known_at=BASE["known_at"] + 1000))
            recs = read_range(root, BASE["dataset"], BASE["known_at"], BASE["known_at"] + 1000)
            self.assertEqual([r.seq for r in recs], [1, 2])

    def test_malformed_line_raises(self):
        with TemporaryDirectory() as d:
            root = Path(d)
            with JournalWriter(root) as w:
                w.append(make())
            p = journal_path(root, BASE["dataset"], "2025-09-07")
            with open(p, "a") as f:
                f.write("not json\n")
            with self.assertRaises(StoreError):
                read_range(root, BASE["dataset"], BASE["known_at"], BASE["known_at"] + 999_999)


class JournalTailTest(unittest.TestCase):
    def test_partial_trailing_line_is_ignored_then_delivered(self):
        with TemporaryDirectory() as d:
            root = Path(d)
            with JournalWriter(root) as w:
                r1 = w.append(make())
            path = journal_path(root, BASE["dataset"], "2025-09-07")
            r2 = make(known_at=BASE["known_at"] + 1000).replace(seq=2)
            r2 = r2.with_id()
            full_line = r2.to_json() + "\n"
            split = len(full_line) // 2
            head, rest = full_line[:split], full_line[split:]
            with open(path, "a") as f:
                f.write(head)

            tail = JournalTail(root, BASE["dataset"])
            self.assertEqual([r.seq for r in tail.poll()], [r1.seq])

            with open(path, "a") as f:
                f.write(rest)
            self.assertEqual([r.seq for r in tail.poll()], [2])

    def test_poll_deduplicates_seq_at_or_below_last_seq(self):
        with TemporaryDirectory() as d:
            root = Path(d)
            with JournalWriter(root) as w:
                w.append(make())
            tail = JournalTail(root, BASE["dataset"])
            first = tail.poll()
            self.assertEqual([r.seq for r in first], [1])
            self.assertEqual(tail.poll(), [])

    def test_follows_day_roll(self):
        with TemporaryDirectory() as d:
            root = Path(d)
            tail = JournalTail(root, BASE["dataset"])
            with JournalWriter(root) as w:
                w.append(make(known_at=BASE["known_at"]))
                self.assertEqual([r.seq for r in tail.poll()], [1])
                w.append(make(known_at=BASE["known_at"] + 86_400_000))
                self.assertEqual([r.seq for r in tail.poll()], [2])

    def test_malformed_line_is_skipped_and_counted_not_raised(self):
        with TemporaryDirectory() as d:
            root = Path(d)
            with JournalWriter(root) as w:
                w.append(make())
            path = journal_path(root, BASE["dataset"], "2025-09-07")
            with open(path, "a") as f:
                f.write("not json\n")
                r2 = make(known_at=BASE["known_at"] + 1000).replace(seq=2).with_id()
                f.write(r2.to_json() + "\n")
            tail = JournalTail(root, BASE["dataset"])
            got = tail.poll()
            self.assertEqual([r.seq for r in got], [1, 2])
            self.assertEqual(tail.skipped, 1)

    def test_from_seq_skips_already_known_records(self):
        with TemporaryDirectory() as d:
            root = Path(d)
            with JournalWriter(root) as w:
                w.append(make())
                w.append(make(known_at=BASE["known_at"] + 1000))
            tail = JournalTail(root, BASE["dataset"], from_seq=1)
            self.assertEqual([r.seq for r in tail.poll()], [2])


if __name__ == "__main__":
    unittest.main()
