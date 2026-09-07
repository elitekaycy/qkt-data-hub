"""The journal's read side: historical range reads and a live tail, over plain ndjson files.

This module owns the on-disk layout (`<root>/journal/<dataset>/<YYYY-MM-DD>.ndjson`, dated by
each record's `known_at` in UTC) because a consumer that only ever reads -- a kill-switch
daemon, a backtest replaying a day live -- must be able to open the store without vendoring the
writer. It imports nothing but `hubread.record` and the stdlib, so it can be embedded alone.
"""
from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from hubread.errors import RecordError, StoreError
from hubread.record import Record, sort_key


def day_str(known_at: int) -> str:
    """The UTC calendar day a `known_at` epoch-ms timestamp falls on, as `YYYY-MM-DD`.

    This is the one place that decides which file a record belongs in; using UTC everywhere
    keeps the layout independent of the host's local timezone.
    """
    return datetime.fromtimestamp(known_at / 1000, tz=UTC).strftime("%Y-%m-%d")


def journal_path(root: Path, dataset: str, day: str) -> Path:
    """The path of one dataset's journal file for one UTC calendar day.

    Centralising this means the writer, the tailer and `read_range` can never disagree about
    where a record lives.
    """
    return Path(root) / "journal" / dataset / f"{day}.ndjson"


def list_day_files(root: Path, dataset: str) -> list[Path]:
    """All of a dataset's day files, oldest first.

    `YYYY-MM-DD` filenames sort lexicographically in calendar order, so a plain name sort is
    enough to get chronological order without parsing each name into a date.
    """
    directory = Path(root) / "journal" / dataset
    if not directory.is_dir():
        return []
    return sorted((p for p in directory.iterdir() if p.suffix == ".ndjson"), key=lambda p: p.name)


def read_range(root: Path, dataset: str, known_from: int, known_to: int) -> list[Record]:
    """Every record of `dataset` with `known_from <= known_at <= known_to`, in `sort_key` order.

    Raises on a malformed line rather than skipping it: a historical read is the thing a
    backtest or an audit trusts completely, so a corrupt byte must stop the read rather than
    quietly produce an incomplete-but-plausible-looking answer.
    """
    out: list[Record] = []
    for path in list_day_files(root, dataset):
        if path.stem < day_str(known_from) or path.stem > day_str(known_to):
            continue
        text = path.read_text(encoding="utf-8")
        for lineno, line in enumerate(text.split("\n"), start=1):
            if not line:
                continue
            try:
                record = Record.from_json(line)
            except RecordError as e:
                raise StoreError(f"{path}:{lineno}: malformed journal record: {e}") from e
            if known_from <= record.known_at <= known_to:
                out.append(record)
    out.sort(key=sort_key)
    return out


class JournalTail:
    """Follows a dataset's journal as it grows, delivering newly appended records once each.

    A live consumer cannot afford the two failure modes a historical reader can: it must not
    die on a single corrupt line (there is no way to "retry" a live feed past a bad byte), and
    it must not choke on a line still being written -- a crash or an in-flight `append` can
    leave a trailing line with no terminating newline, and that line is not yet a fact until
    the newline lands. Both are handled by tracking a byte offset into the current day file and
    only ever consuming up to the last complete line seen.
    """

    def __init__(self, root: Path, dataset: str, from_seq: int = 0) -> None:
        self.root = Path(root)
        self.dataset = dataset
        self.last_seq = from_seq
        self.skipped = 0
        self._days: list[str] = []
        self._index = -1
        self._offset = 0

    def poll(self) -> list[Record]:
        """Return newly available records in `seq` order since the last call.

        Drops any `seq <= last_seq` so a tail restarted with `from_seq` -- or one that re-reads
        a file it already partially consumed -- never delivers the same record twice.
        """
        self._refresh_days()
        out: list[Record] = []
        while 0 <= self._index < len(self._days):
            path = journal_path(self.root, self.dataset, self._days[self._index])
            with open(path, "rb") as f:
                f.seek(self._offset)
                chunk = f.read()
            last_newline = chunk.rfind(b"\n")
            if last_newline == -1:
                complete, remainder = b"", chunk
            else:
                complete, remainder = chunk[: last_newline + 1], chunk[last_newline + 1 :]
            self._offset += len(complete)
            for line in complete.decode("utf-8").split("\n"):
                if not line:
                    continue
                try:
                    record = Record.from_json(line)
                except RecordError:
                    self.skipped += 1
                    continue
                if record.seq <= self.last_seq:
                    continue
                out.append(record)
                self.last_seq = record.seq
            has_next_day = self._index + 1 < len(self._days)
            if has_next_day and not remainder:
                self._index += 1
                self._offset = 0
                continue
            break
        return out

    def _refresh_days(self) -> None:
        days = [p.stem for p in list_day_files(self.root, self.dataset)]
        if days != self._days:
            self._days = days
            if self._index == -1 and days:
                self._index = 0
