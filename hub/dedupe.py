"""Revision assignment and exact-duplicate suppression for the ingest pipeline.

A collector re-fetches its source on every run and has no way to know, on its own, whether the
row it just parsed is new information or the same fact seen again. Deciding that here -- once,
for every dataset -- is what keeps the journal an honest history instead of growing one entry
per poll. The rule is simple: same `(dataset, key)` and an unchanged payload is a duplicate and
is dropped before it reaches the journal; a changed payload is a new revision of the same fact,
never a silent overwrite of the old one.

`known_at` is deliberately excluded from the comparison. A collector's poll clock varies run to
run even when the underlying fact has not moved, and folding that jitter into "did this change"
would make the journal grow forever on data that is, in fact, unchanged.
"""
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field

from hubread.record import Record


@dataclass
class RevisionIndex:
    """Latest known `(revision, payload_hash)` per `(dataset, key)`.

    One index instance covers a whole hub root, since a single collector run can touch many
    datasets. Only the highest revision observed for a key is kept -- see `observe` -- because
    that is what both a live `assign` call and a journal replay need to agree on.
    """

    _state: dict[tuple[str, str], tuple[int, str]] = field(default_factory=dict)

    def seen(self, dataset: str, key: str) -> tuple[int, str] | None:
        """The `(revision, payload_hash)` last recorded for this key, or `None` if never seen.

        `assign` uses this to decide whether a candidate is the first sighting of a fact, an
        unchanged repeat, or a genuine revision.
        """
        return self._state.get((dataset, key))

    def observe(self, record: Record) -> None:
        """Fold one record into the index, keeping the highest revision seen for its key.

        Journal order is by `known_at`, which is not guaranteed to be monotonic in `revision`
        across an availability backfill (a `derived` record for an old period can be journaled
        after a later revision of a different period). Comparing revisions rather than simply
        overwriting is what makes replay order-independent.
        """
        state_key = (record.dataset, record.key)
        current = self._state.get(state_key)
        if current is None or record.revision > current[0]:
            self._state[state_key] = (record.revision, record.payload_hash())

    def rebuild_from(self, records: Iterable[Record]) -> None:
        """Replay a journal into this index so a restarted writer does not restart revisions at 1.

        Equivalent to calling `observe` on every record in order, exposed separately because
        "load the index from disk" is a distinct operation from "fold in one new record" at the
        call sites that use it.
        """
        for record in records:
            self.observe(record)


def assign(index: RevisionIndex, candidate: Record) -> Record | None:
    """Decide a candidate's revision against the index, or drop it as an exact duplicate.

    Whatever revision the caller stamped on `candidate` is discarded and replaced: the first
    sighting of a `(dataset, key)` is always revision 1, and a payload that differs from the
    last-seen one is always `previous + 1`. This is what lets a collector construct records
    without tracking revision state itself -- the index is the single source of truth for it.

    `Record.replace` does not recompute the content id, because most callers change fields the
    id does not depend on. Revision is not one of those, so the id is re-stamped here with
    `with_id()` before the record is returned -- skipping that would hand back a record whose
    id no longer matches its content, which `Record.from_json` would later reject.
    """
    prior = index.seen(candidate.dataset, candidate.key)
    if prior is not None and prior[1] == candidate.payload_hash():
        return None
    next_revision = 1 if prior is None else prior[0] + 1
    assigned = candidate.replace(revision=next_revision).with_id()
    index.observe(assigned)
    return assigned
