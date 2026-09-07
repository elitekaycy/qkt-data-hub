"""One log line shape, and a cadence gate so a repeating fault does not become the log.

A collector that fails every retry for a day would otherwise write thousands of identical lines
and bury the one event an operator needs to see. `log_every` keeps the first occurrence and then
one in N, which is enough to show a fault is ongoing without drowning everything else.
"""
from __future__ import annotations

import datetime as dt
import sys
import threading

_counts: dict[str, int] = {}
_lock = threading.Lock()


def log(*parts: object) -> None:
    """Write one UTC-stamped line to stderr. Stderr so piping stdout stays data-only."""
    stamp = dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(stamp, *parts, file=sys.stderr, flush=True)


def log_every(tag: str, n: int, *parts: object) -> None:
    """Log the first occurrence of `tag`, then every nth, so an ongoing fault stays visible."""
    with _lock:
        count = _counts.get(tag, 0) + 1
        _counts[tag] = count
    if count == 1 or count % max(1, n) == 0:
        log(*parts, f"(occurrence {count})")


def reset_counters() -> None:
    """Test hook: forget cadence state so one test's counts cannot leak into another's."""
    with _lock:
        _counts.clear()
