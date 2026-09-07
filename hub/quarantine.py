"""Rejected input, kept with its reason, so decay is visible instead of silent.

A dataset whose source changed shape does not usually fail loudly -- it starts producing rows
that no longer validate, and if those were dropped in a `continue` the store would simply grow
quieter while looking healthy. Everything the pipeline refuses lands here with the reason, and
the daily count is what an operator alerts on.
"""
from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any


def _day(now_ms: int) -> str:
    return dt.datetime.fromtimestamp(now_ms / 1000, dt.UTC).strftime("%Y-%m-%d")


class Quarantine:
    """Append-only rejected-input log under `<root>/quarantine/<dataset>/<YYYY-MM-DD>.ndjson`."""

    def __init__(self, root: Path) -> None:
        self._root = Path(root) / "quarantine"

    def write(self, dataset: str, reason: str, payload: Any, now_ms: int, raw_ref: str | None = None) -> None:
        """Record one rejection. `payload` is whatever was refused, serialised best-effort."""
        path = self._root / dataset / f"{_day(now_ms)}.ndjson"
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            body = json.loads(json.dumps(payload, default=str))
        except (TypeError, ValueError):
            body = repr(payload)
        line = json.dumps(
            {"dataset": dataset, "reason": reason, "at": now_ms, "raw_ref": raw_ref, "payload": body},
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(line + "\n")

    def count(self, dataset: str, now_ms: int) -> int:
        """How many rejections that dataset logged on the UTC day containing `now_ms`."""
        path = self._root / dataset / f"{_day(now_ms)}.ndjson"
        if not path.exists():
            return 0
        return sum(1 for line in path.read_text(encoding="utf-8").splitlines() if line.strip())
