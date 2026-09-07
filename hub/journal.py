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
from hub.record import Record
from hubread.journal import day_str, journal_path, list_day_files


class JournalWriter:
    """The one process allowed to append to a hub root's journal at a time.

    Holds an exclusive, non-blocking `flock` on `<root>/.writer.lock` for its whole lifetime,
    so a second writer started against the same root fails fast at construction rather than
    silently interleaving `seq` values with this one. Each dataset's file descriptor is opened
    once, in append mode, and reused; every write is a single `os.write` of a complete line
    followed by an `fsync`, so a reader can never observe a line that is neither fully absent
    nor fully present.
    """

    def __init__(self, root: Path, flush_every: int = 1) -> None:
        self.root = Path(root)
        self.flush_every = flush_every
        (self.root / "journal").mkdir(parents=True, exist_ok=True)
        self._lock_file = open(self.root / ".writer.lock", "a+")
        try:
            fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as e:
            self._lock_file.close()
            raise StoreError(f"another writer already holds the journal at {self.root}") from e
        self._seq: dict[str, int] = {}
        self._fds: dict[Path, int] = {}
        self._writes_since_flush: dict[Path, int] = {}
        self._closed = False

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
        pending = self._writes_since_flush.get(path, 0) + 1
        if pending >= self.flush_every:
            os.fsync(fd)
            pending = 0
        self._writes_since_flush[path] = pending
        self._seq[record.dataset] = seq
        return stamped

    def close(self) -> None:
        """Flush, close every open file descriptor and release the writer lock.

        Safe to call more than once so a `with` block and an explicit `close()` never conflict.
        """
        if self._closed:
            return
        for path, fd in self._fds.items():
            if self._writes_since_flush.get(path):
                os.fsync(fd)
            os.close(fd)
        self._fds.clear()
        self._writes_since_flush.clear()
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

        Reads only the newest day file's lines, not the whole dataset history. A last line
        with no trailing newline is a crash-truncated write in progress, not a fact yet, so it
        is ignored in favour of the line before it; if that one is also unreadable the file is
        genuinely corrupt and resuming would guess a `seq`, which is worse than refusing.
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
        except RecordError:
            if len(lines) == 1:
                return 0
            try:
                return Record.from_json(lines[-2]).seq
            except RecordError as e:
                raise StoreError(f"{newest}: cannot resume seq, malformed tail") from e
