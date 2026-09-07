"""The journal's write side: the single append-only writer every collector goes through.

Everything downstream -- dedupe, the compiler, a live tail -- depends on `seq` being a strict,
gapless, per-dataset counter and on a journal line never being half-written when a reader sees
it. This module is the only place that assigns `seq` and the only place that holds the
process-wide writer lock, because two writers racing on the same root would interleave `seq`
values and corrupt the ordering the whole product rests on.
"""
from __future__ import annotations

import fcntl
import os
from pathlib import Path
from types import TracebackType

from hub.errors import RecordError, StoreError
from hub.record import Record, canonical_json
from hubread.journal import day_str, journal_path, list_day_files


class JournalWriter:
    """The one process allowed to append to a hub root's journal at a time.

    Holds an exclusive, non-blocking `flock` on `<root>/.writer.lock` for its whole lifetime,
    so a second writer started against the same root fails fast at construction rather than
    silently interleaving `seq` values with this one. On open, before any append, it repairs
    every dataset's journal files: a torn trailing line left by a process that died mid
    `os.write` is truncated off and preserved in quarantine, so a restart never concatenates a
    new record onto a dead fragment and produces one unparseable line forever. Each dataset's
    file descriptor is then opened once, in append mode, and reused; every write is a single
    `os.write` of a complete line followed by an `fsync`, so a reader can never observe a line
    that is neither fully absent nor fully present.
    """

    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        (self.root / "journal").mkdir(parents=True, exist_ok=True)
        self._lock_file = open(self.root / ".writer.lock", "a+")
        try:
            fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as e:
            self._lock_file.close()
            raise StoreError(f"another writer already holds the journal at {self.root}") from e
        self._seq: dict[str, int] = {}
        self._fds: dict[Path, int] = {}
        self._closed = False
        self._repair_torn_tails()

    def append(self, record: Record) -> Record:
        """Stamp `record` with the next `seq` for its dataset, append it, and return the stamp.

        `seq` is deliberately excluded from `identity_payload`, so re-stamping and recomputing
        the content id here is cheap -- but a caller still needs the stamped record back to
        know what was actually written and to link it to later reads.
        """
        if self._closed:
            raise StoreError("cannot append: this writer is closed")
        seq = self._next_seq(record.dataset)
        stamped = record.replace(seq=seq).with_id()
        path = journal_path(self.root, record.dataset, day_str(stamped.known_at))
        path.parent.mkdir(parents=True, exist_ok=True)
        line = (stamped.to_json() + "\n").encode("utf-8")
        fd = self._fd_for(path)
        os.write(fd, line)
        os.fsync(fd)
        self._seq[record.dataset] = seq
        return stamped

    def close(self) -> None:
        """Close every open file descriptor and release the writer lock.

        Safe to call more than once so a `with` block and an explicit `close()` never conflict.
        """
        if self._closed:
            return
        for fd in self._fds.values():
            os.close(fd)
        self._fds.clear()
        fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_UN)
        self._lock_file.close()
        self._closed = True

    def __enter__(self) -> JournalWriter:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def _fd_for(self, path: Path) -> int:
        fd = self._fds.get(path)
        if fd is None:
            fd = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o644)
            self._fds[path] = fd
        return fd

    def _next_seq(self, dataset: str) -> int:
        if dataset not in self._seq:
            self._seq[dataset] = self._resume_seq(dataset)
        return self._seq[dataset] + 1

    def _resume_seq(self, dataset: str) -> int:
        """The last `seq` already on disk for `dataset`, so a restarted writer does not
        restart numbering at 1 and collide with what a reader has already seen.

        Reads only the newest day file's lines, not the whole dataset history. By the time
        this runs, `_repair_torn_tails` has already truncated any unterminated trailing line,
        so the newest file's last line is either absent or a complete, parseable record.
        """
        days = list_day_files(self.root, dataset)
        if not days:
            return 0
        newest = days[-1]
        lines = [ln for ln in newest.read_text(encoding="utf-8").split("\n") if ln]
        if not lines:
            return 0
        try:
            return Record.from_json(lines[-1]).seq
        except RecordError as e:
            raise StoreError(f"{newest}: cannot resume seq, malformed tail") from e

    def _repair_torn_tails(self) -> None:
        """Truncate every dataset's journal files back to their last complete line.

        A journal line is only ever written as one atomic `os.write` of `line + "\\n"`. If a
        process dies mid-write, the bytes already on disk are a fragment of a record that was
        never actually committed -- the write that would have completed it never finished.
        Discarding that fragment is repair, not rewriting history: it removes something that
        was never a fact in the first place. The discarded bytes are not simply dropped; they
        are recorded in quarantine so nothing vanishes without a trace, in an append-only store
        whose whole value is auditability.

        Runs once, while this writer alone holds the root's lock, before any dataset's file
        descriptor is opened for append -- so a fresh `O_APPEND` write can never land on top of
        a torn fragment and turn two good records into one permanently unparseable line.
        """
        journal_root = self.root / "journal"
        if not journal_root.is_dir():
            return
        for dataset_dir in sorted(journal_root.iterdir()):
            if not dataset_dir.is_dir():
                continue
            dataset = dataset_dir.name
            for path in sorted(dataset_dir.glob("*.ndjson")):
                self._repair_tail(dataset, path)

    def _repair_tail(self, dataset: str, path: Path) -> None:
        data = path.read_bytes()
        if not data or data.endswith(b"\n"):
            return
        good_end = data.rfind(b"\n") + 1  # 0 if the file has no complete line at all
        discarded = data[good_end:]
        with open(path, "r+b") as f:
            f.truncate(good_end)
            f.flush()
            os.fsync(f.fileno())
        self._quarantine_torn_tail(dataset, path, good_end, discarded)

    def _quarantine_torn_tail(self, dataset: str, path: Path, offset: int, discarded: bytes) -> None:
        quarantine_path = self.root / "quarantine" / dataset / path.name
        quarantine_path.parent.mkdir(parents=True, exist_ok=True)
        record = {
            "dataset": dataset,
            "file": str(path),
            "offset": offset,
            "discarded": discarded.decode("utf-8", errors="replace"),
            "reason": "torn_tail_on_writer_open",
        }
        line = (canonical_json(record) + "\n").encode("utf-8")
        fd = os.open(quarantine_path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o644)
        try:
            os.write(fd, line)
            os.fsync(fd)
        finally:
            os.close(fd)
