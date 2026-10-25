"""What the writer last did, kept beside the store so an operator can see it without a log.

The journal says what the store knows; it cannot say that a source has been failing for an hour
or that the last compile was yesterday. Those facts live in the daemon's log, which a person
checking the hub from a browser cannot see. This module keeps the latest outcome of each step
under `<root>/status/` as small JSON documents, replaced atomically, so the read-only UI and any
monitor read the same answer.

Nothing here is market data and nothing in it is cited by a backtest: a status file may be
deleted at any time and the store is unchanged.
"""
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any


def _status_dir(root: Path) -> Path:
    path = Path(root) / "status"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _replace(path: Path, doc: dict[str, Any]) -> None:
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        json.dump(doc, fh, sort_keys=True, separators=(",", ":"))
    os.replace(tmp, path)


def read(root: Path, name: str) -> dict[str, Any]:
    path = Path(root) / "status" / f"{name}.json"
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return doc if isinstance(doc, dict) else {}


def record_collect(root: Path, dataset: str, outcome: dict[str, Any]) -> None:
    """Remember one dataset's latest collect outcome (counts, error, raw blob) and its last success."""
    path = _status_dir(root) / "collect.json"
    doc = read(root, "collect")
    entry = dict(doc.get(dataset, {}))
    entry.update(outcome)
    entry["runs"] = int(entry.get("runs", 0)) + 1
    if outcome.get("ok"):
        entry["last_success_at"] = outcome["at"]
        entry["consecutive_failures"] = 0
    else:
        entry["consecutive_failures"] = int(entry.get("consecutive_failures", 0)) + 1
    doc[dataset] = entry
    _replace(path, doc)


def record_step(root: Path, step: str, outcome: dict[str, Any]) -> None:
    """Remember the latest outcome of a store-wide step (`compile`, `verify`)."""
    _replace(_status_dir(root) / f"{step}.json", outcome)
