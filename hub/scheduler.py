"""When each source is polled, and what happens when one stops answering.

Two ideas do all the work here. Sources fail independently: a dead provider backs off on its own
timer and never delays another, because one feed's outage taking the whole store offline is the
failure this design exists to avoid. And cadence tightens near a scheduled event, because a
consensus feed fills in an actual within seconds of a release and a half-hourly poll would record
that arrival half an hour late -- which would make `known_at` a claim about our own past that is
simply untrue.

The heartbeat file is the store's liveness signal. A consumer reads its mtime rather than asking
the hub whether it is well, so a hub that has hung cannot answer that it is fine.
"""
from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from hub.collectors.declarative import Cadence
from hub.compiler import compile_all
from hub.config import HubConfig
from hub.journal import JournalWriter
from hub.logging import log, log_every
from hub.registry import Registry
from hubread.errors import HubError
from hubread.journal import read_range

_NEAR_EVENT_HORIZON_MS = 7 * 86_400_000


@dataclass
class SourceState:
    """Per-source timing and failure history, kept so one provider cannot affect another."""

    next_due: float = 0.0
    consecutive_failures: int = 0
    last_success: float = 0.0
    alerted: bool = False
    events: tuple[int, ...] = field(default_factory=tuple)


class Scheduler:
    """Polls every collecting dataset on its own cadence and compiles on a slower timer."""

    def __init__(
        self,
        config: HubConfig,
        registry: Registry,
        *,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._config = config
        self._registry = registry
        self._clock = clock
        self._sleep = sleep
        self._state: dict[str, SourceState] = {name: SourceState() for name in registry.collecting()}
        self._next_compile = 0.0
        self._next_heartbeat = 0.0

    # -- cadence -------------------------------------------------------------------------

    def _known_events(self, dataset: str) -> tuple[int, ...]:
        """Upcoming `effective_at` instants this dataset already knows about.

        Read from the journal rather than from a provider, because the schedule is itself a fact
        we recorded: knowing on Monday that a release lands Friday is not look-ahead, and it is
        what lets the poller be awake at the moment the number appears.
        """
        now_ms = int(self._clock() * 1000)
        try:
            records = read_range(self._config.root, dataset, 0, 4_102_444_800_000)
        except HubError:
            return ()
        lower = now_ms - 86_400_000
        upper = now_ms + _NEAR_EVENT_HORIZON_MS
        return tuple(sorted({r.effective_at for r in records if lower <= r.effective_at <= upper}))

    def _interval_for(self, dataset: str, state: SourceState) -> float:
        source = self._registry.source(dataset)
        cadence: Cadence | None = getattr(source, "cadence", None)
        if cadence is None:
            return self._config.retry_seconds
        if cadence.near_every_seconds <= 0:
            return float(cadence.steady_seconds)
        now_ms = int(self._clock() * 1000)
        before_ms = cadence.near_before_seconds * 1000
        after_ms = cadence.near_after_seconds * 1000
        for event_ms in state.events:
            if event_ms - before_ms <= now_ms <= event_ms + after_ms:
                return float(cadence.near_every_seconds)
        return float(cadence.steady_seconds)

    # -- running -------------------------------------------------------------------------

    def _beat(self) -> None:
        heartbeat = Path(self._config.root) / "heartbeat"
        heartbeat.parent.mkdir(parents=True, exist_ok=True)
        heartbeat.write_text(str(int(self._clock() * 1000)), encoding="utf-8")

    def _poll_due(self) -> int:
        from hub.__main__ import collect_once

        now = self._clock()
        due = [name for name, state in self._state.items() if now >= state.next_due]
        if not due:
            return 0
        written = 0
        writer = JournalWriter(self._config.root)
        try:
            for name in due:
                state = self._state[name]
                state.events = self._known_events(name)
                try:
                    written += collect_once(
                        self._config.root,
                        self._registry,
                        name,
                        timeout=self._config.timeout_seconds,
                        writer=writer,
                    )
                    state.consecutive_failures = 0
                    state.alerted = False
                    state.last_success = now
                    state.next_due = now + self._interval_for(name, state)
                except HubError as e:
                    state.consecutive_failures += 1
                    state.next_due = now + self._config.retry_seconds
                    log_every(f"collect-fail:{name}", 10, f"collect[{name}] failed: {e}")
                    if state.consecutive_failures >= self._config.failures_before_alert and not state.alerted:
                        state.alerted = True
                        log(f"ALERT collect[{name}] has failed {state.consecutive_failures} times in a row")
        finally:
            writer.close()
        return written

    def run(self, *, once: bool = False) -> int:
        """Poll, compile and beat until interrupted. `once` does a single pass, for CI and smoke."""
        log(f"hub starting: root={self._config.root} datasets={self._registry.datasets()}")
        if self._config.compile_on_start or once:
            self._next_compile = 0.0
        while True:
            now = self._clock()
            self._poll_due()
            if now >= self._next_compile:
                try:
                    compile_all(self._config.root, self._registry)
                    log("compiled snapshots and rewrote the manifest")
                except HubError as e:
                    log(f"compile failed: {e}")
                self._next_compile = now + self._config.compile_every_seconds
            if now >= self._next_heartbeat:
                self._beat()
                self._next_heartbeat = now + self._config.heartbeat_seconds
            if once:
                return 0
            soonest = min((s.next_due for s in self._state.values()), default=now + 60.0)
            wait = max(1.0, min(soonest - self._clock(), 60.0))
            self._sleep(wait)
