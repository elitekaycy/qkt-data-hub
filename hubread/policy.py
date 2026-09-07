"""The reader's policy: what a consumer decides about visibility, independent of the data itself.

The hub records `known_at` and writes it once; how cautiously a consumer trusts that record is
the consumer's own call, not the hub's. A backtest wants the sharpest possible point-in-time
view (`min_lag_ms=0`); a live safety daemon may want an extra margin against a source's own
publication jitter (`min_lag_ms>0`), or to refuse anything estimated during backfill entirely
(`refuse_derived=True`). Keeping this as data the caller constructs, rather than a single
hard-coded rule inside `Reader`, is what lets the same store serve both without either policy
leaking into the other's answers.
"""
from __future__ import annotations

from dataclasses import dataclass

from hubread.errors import ConfigError


@dataclass(frozen=True, slots=True)
class Policy:
    """Visibility and health rules a `Reader` applies on top of the raw journal/snapshot data.

    `min_lag_ms`: extra delay added to `known_at` before a record counts as visible -- a margin
    against a source's own clock or publication jitter, on top of the recorded timestamp.

    `refuse_derived`: when true, a record whose `availability` is `derived` (estimated during
    backfill, not actually observed at the time) is invisible entirely, never merely deprioritised
    -- a consumer that cannot tolerate a guessed timestamp should not have to filter it out itself.

    `stale_after_ms`: how old the hub's heartbeat may be before `Reader.health()` reports the
    store stale. Staleness is a signal for the caller to act on (keep last-known values rather
    than trust a live tail that has gone quiet); the reader itself never blanks data because the
    heartbeat is old.

    `skew_tolerance_ms`: how far a consumer's own clock may disagree with the hub's before a
    caller should treat a "now" comparison against `known_at` as unreliable. `Reader` does not
    use this value itself -- it has no independent clock to compare against -- but carries it so
    a caller has one place to declare the tolerance it applies around any of this reader's answers.
    """

    min_lag_ms: int = 0
    refuse_derived: bool = False
    stale_after_ms: int = 900_000
    skew_tolerance_ms: int = 5_000

    def __post_init__(self) -> None:
        for name in ("min_lag_ms", "stale_after_ms", "skew_tolerance_ms"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ConfigError(f"Policy.{name} must be a non-negative integer, got {value!r}")
        if not isinstance(self.refuse_derived, bool):
            raise ConfigError(f"Policy.refuse_derived must be a bool, got {self.refuse_derived!r}")
