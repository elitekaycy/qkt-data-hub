"""Tests for the journal: append-only writer, historical range read, and live tailer.

These pin the invariants the rest of the product leans on without re-checking: `seq` is a
strictly increasing per-dataset counter that survives a writer restart, only one writer may
hold a root at a time, a line is either fully written or treated as not-yet-written, and a
live tail never raises on a single bad line while a historical read never silently drops one.
"""
import json
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

    def test_a_multi_year_backfill_does_not_exhaust_the_process_file_limit(self):
        # A one-pass historical load spanning years touches one journal file per calendar day
        # -- thousands for a multi-year daily series. A cache that never evicts holds every one
        # of those descriptors open for the writer's whole lifetime and exhausts the process's
        # limit partway through, as it did against the container default of 1024 on a real
        # multi-year FRED backfill. Lowering the limit here well below the number of distinct
        # days written proves the cache evicts rather than merely happening to fit today.
        import resource

        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        # Comfortably above the writer's own cache cap (room for stdio, the lock file and the
        # repair pass) but far below the 1500 distinct days this test writes.
        low_limit = 200
        self.assertLess(low_limit, 1500, "test assumption: fewer fds than days written below")
        resource.setrlimit(resource.RLIMIT_NOFILE, (low_limit, hard))
        try:
            with TemporaryDirectory() as d:
                root = Path(d)
                day_ms = 86_400_000
                with JournalWriter(root) as w:
                    for day in range(1500):
                        w.append(make(known_at=BASE["known_at"] + day * day_ms))
                lines = read_range(
                    root, BASE["dataset"], BASE["known_at"], BASE["known_at"] + 1500 * day_ms
                )
                self.assertEqual(len(lines), 1500)
                self.assertEqual([r.seq for r in lines], list(range(1, 1501)))
        finally:
            resource.setrlimit(resource.RLIMIT_NOFILE, (soft, hard))


class JournalWriterRepairTest(unittest.TestCase):
    def test_torn_tail_is_repaired_and_quarantined_on_writer_open(self):
        with TemporaryDirectory() as d:
            root = Path(d)
            with JournalWriter(root) as w:
                w.append(make(known_at=BASE["known_at"]))
            path = journal_path(root, BASE["dataset"], "2025-09-07")

            torn = make(known_at=BASE["known_at"] + 1000).replace(seq=2).with_id()
            full_line = torn.to_json() + "\n"
            fragment = full_line[: len(full_line) // 2]
            with open(path, "a") as f:
                f.write(fragment)

            with JournalWriter(root) as w2:
                r3 = w2.append(make(known_at=BASE["known_at"] + 2000))
                # The torn record was never committed, so seq resumes right after the one
                # complete record on disk rather than after the fragment's intended seq.
                self.assertEqual(r3.seq, 2)

            recs = read_range(root, BASE["dataset"], BASE["known_at"], BASE["known_at"] + 999_999)
            self.assertEqual([r.seq for r in recs], [1, 2])

            quarantine_path = root / "quarantine" / BASE["dataset"] / "2025-09-07.ndjson"
            self.assertTrue(quarantine_path.exists())
            entry = json.loads(quarantine_path.read_text().splitlines()[0])
            self.assertEqual(entry["reason"], "torn_tail_on_writer_open")
            self.assertEqual(entry["dataset"], BASE["dataset"])
            self.assertEqual(entry["discarded"], fragment)


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

    def test_orphaned_partial_line_in_superseded_day_does_not_starve_later_day(self):
        with TemporaryDirectory() as d:
            root = Path(d)
            with JournalWriter(root) as w:
                w.append(make(known_at=BASE["known_at"]))
            path1 = journal_path(root, BASE["dataset"], "2025-09-07")
            torn = make(known_at=BASE["known_at"] + 1000).replace(seq=2).with_id()
            fragment = (torn.to_json() + "\n")[:10]
            with open(path1, "a") as f:
                f.write(fragment)

            # Written directly to the file, bypassing the writer's repair-on-open, so day 1
            # is left with a permanently orphaned fragment while day 2 already has a complete,
            # well-formed record.
            day2_record = make(known_at=BASE["known_at"] + 86_400_000).replace(seq=2).with_id()
            path2 = journal_path(root, BASE["dataset"], "2025-09-08")
            path2.parent.mkdir(parents=True, exist_ok=True)
            path2.write_text(day2_record.to_json() + "\n")

            tail = JournalTail(root, BASE["dataset"])
            got = tail.poll()
            self.assertEqual([r.seq for r in got], [1, 2])
            self.assertEqual(tail.skipped, 1)


if __name__ == "__main__":
    unittest.main()
