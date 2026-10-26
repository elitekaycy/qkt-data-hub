"""A read-only web view of one hub store: the pipeline, each dataset's health, and its facts.

An operator should not need a shell to answer "is the hub working, and what does a strategy see
right now?". This server answers from the same files every consumer reads -- the journal, the
manifest, the quarantine log, the heartbeat and the writer's status files -- and it never writes,
fetches from a provider or holds a lock, so it is safe to run beside the daemon and to point at a
store mounted read-only. It binds to localhost unless told otherwise.

Standard library only, like the rest of the hub: `http.server` for transport and one static page
with hand-drawn SVG, so the view works offline and adds nothing to the supply chain.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import threading
import time
from collections import Counter
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from hub import status
from hub.registry import Registry
from hubread.journal import list_day_files, read_day_file
from hubread.policy import Policy
from hubread.reader import Reader

_PAGE = Path(__file__).with_name("ui.html")
_FAR_FUTURE_MS = 4_102_444_800_000
_RECENT_DAYS = 60
# One store read at a time. The page polls every ten seconds, and a full read of a large journal
# (GDELT's hourly history is hundreds of thousands of records) can take longer than that: without
# this, polls stacked up in threads, held the GIL and gigabytes, and starved the collector beside them.
_READ_LOCK = threading.Lock()


@dataclass(frozen=True)
class _DayStats:
    records: int
    first_known_at: int  # all three meaningless when `records` is 0
    last_known_at: int
    last_seq: int
    scopes: frozenset[str]
    availability: Counter[str]


# Per day file, keyed by (size, mtime): the journal is append-only, so a file that has not changed
# needs reading once per process, and a poll re-reads only the day being written.
_DAY_STATS: dict[str, tuple[tuple[int, int], _DayStats]] = {}


def _day_files(root: Path, dataset: str) -> list[tuple[str, str, int, int]]:
    """`(day, path, size, mtime_ns)` of each of a dataset's day files, oldest first.

    `os.scandir` rather than `list_day_files`: a `Path` and a `stat()` per file were most of a poll's
    cost across GDELT's 3,600 day files.
    """
    try:
        listing = os.scandir(os.path.join(root, "journal", dataset))
    except FileNotFoundError:
        return []
    with listing:
        out = [
            (e.name[: -len(".ndjson")], e.path, st.st_size, st.st_mtime_ns)
            for e in listing
            if e.name.endswith(".ndjson")
            for st in [e.stat()]
        ]
    out.sort()
    return out


def _day_stats(path: str, size: int, mtime_ns: int) -> _DayStats:
    stamp = (size, mtime_ns)
    cached = _DAY_STATS.get(path)
    if cached and cached[0] == stamp:
        return cached[1]
    rows = read_day_file(Path(path))
    stats = _DayStats(
        records=len(rows),
        first_known_at=min((r.known_at for r in rows), default=0),
        last_known_at=max((r.known_at for r in rows), default=0),
        last_seq=max((r.seq for r in rows), default=0),
        scopes=frozenset(r.scope for r in rows),
        availability=Counter(str(r.availability) for r in rows),
    )
    _DAY_STATS[path] = (stamp, stats)
    return stats


def _now_ms() -> int:
    return int(time.time() * 1000)


def _day(ms: int) -> str:
    return dt.datetime.fromtimestamp(ms / 1000, dt.UTC).strftime("%Y-%m-%d")


def _collector_view(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        return {"kind": "backfill only"}
    out: dict[str, Any] = {"kind": str(raw.get("kind", ""))}
    for key in ("url", "cadence", "schedule", "kinds", "module", "class"):
        if key in raw:
            out[key] = raw[key]
    return out


def _quarantine(root: Path, dataset: str, now: int) -> dict[str, Any]:
    folder = root / "quarantine" / dataset
    days: Counter[str] = Counter()
    latest: list[dict[str, Any]] = []
    if folder.is_dir():
        for path in sorted(folder.glob("*.ndjson")):
            for line in path.read_text(encoding="utf-8").splitlines():
                try:
                    row = json.loads(line)
                except ValueError:
                    continue
                days[path.stem] += 1
                latest.append({"at": row.get("at"), "reason": str(row.get("reason", ""))[:300]})
    latest.sort(key=lambda r: r["at"] or 0, reverse=True)
    week = {_day(now - i * 86_400_000) for i in range(7)}
    return {"total": sum(days.values()), "last7": sum(v for k, v in days.items() if k in week), "latest": latest[:10]}


def _raw_store(root: Path) -> dict[str, int]:
    blobs = 0
    size = 0
    raw = root / "raw"
    if raw.is_dir():
        for path in raw.rglob("*"):
            if path.is_file():
                blobs += 1
                size += path.stat().st_size
    return {"blobs": blobs, "bytes": size}


def state(root: Path, registry: Registry, policy: Policy) -> dict[str, Any]:
    """Everything the pipeline view needs, read fresh from the store on every call."""
    now = _now_ms()
    reader = Reader(root, policy)
    manifest = reader._manifest  # the manifest is the reader's to parse; the view reuses it
    collect = status.read(root, "collect")
    since_day = _day(now - (_RECENT_DAYS - 1) * 86_400_000)
    datasets = []
    for name in registry.datasets():
        schema = registry.schema(name)
        files = _day_files(root, name)
        days = [(day, _day_stats(path, size, mtime)) for day, path, size, mtime in files]
        stats = [d for _, d in days]
        filled = [d for d in stats if d.records]  # an empty day file has no known_at or seq
        availability: Counter[str] = Counter()
        for d in stats:
            availability.update(d.availability)
        info = manifest.get(name)
        datasets.append(
            {
                "name": name,
                "title": schema.title,
                "scope_kind": schema.scope_kind,
                "fields": [
                    {"name": f.name, "type": str(f.type), "derived": bool(f.derived), "strategy": f.strategy}
                    for f in schema.fields.values()
                ],
                "collector": _collector_view(schema.raw.get("collector")),
                "journal": {
                    "records": sum(d.records for d in stats),
                    "day_files": len(files),
                    "first_known_at": min((d.first_known_at for d in filled), default=None),
                    "last_known_at": max((d.last_known_at for d in filled), default=None),
                    "last_seq": max((d.last_seq for d in filled), default=None),
                    "scopes": sorted(set().union(*(d.scopes for d in stats))),
                    "availability": dict(availability),
                    # Records per UTC day over the last `_RECENT_DAYS` days, today included.
                    "per_day": {day: d.records for day, d in days if day >= since_day and d.records},
                },
                "manifest": {
                    "windows": len(info.windows) if info else 0,
                    "last_seq": info.last_seq if info else None,
                },
                "quarantine": _quarantine(root, name, now),
                "collect": collect.get(name),
            }
        )
    health = reader.health()
    return {
        "now": now,
        "root": str(root),
        "heartbeat": {"at": health.heartbeat_at, "age_ms": health.age_ms, "stale": health.stale},
        "compile": status.read(root, "compile"),
        "verify": status.read(root, "verify"),
        "raw": _raw_store(root),
        "datasets": datasets,
    }


def _record_view(record: Any) -> dict[str, Any]:
    return {
        "known_at": record.known_at,
        "effective_at": record.effective_at,
        "scope": record.scope,
        "key": record.key,
        "revision": record.revision,
        "availability": str(record.availability),
        "seq": record.seq,
        "source": record.source,
        "parser": record.parser,
        "raw_ref": record.raw_ref,
        "fields": record.fields,
    }


def records(root: Path, dataset: str, limit: int) -> list[dict[str, Any]]:
    """The `limit` most recently journalled records (highest `seq`), reading only the files that hold them.

    Day files are dated by `known_at`, not `seq` (a backfill writes old days last), so files are
    taken by their highest `seq` until no unread file can beat the `limit`-th record found.
    """
    files = sorted(
        ((d.last_seq, Path(path)) for _, path, size, mtime in _day_files(root, dataset)
         for d in [_day_stats(path, size, mtime)] if d.records),
        reverse=True,
    )
    rows: list[Any] = []
    for last_seq, path in files:
        if len(rows) >= limit and last_seq < rows[limit - 1].seq:
            break
        rows.extend(read_day_file(path))
        rows.sort(key=lambda r: r.seq, reverse=True)
    return [_record_view(r) for r in rows[:limit]]


def series(root: Path, dataset: str, scope: str, field: str, limit: int = 5000) -> list[list[Any]]:
    """The last `limit` values of one field in one scope by `known_at`, reading newest day files first."""
    rows: list[Any] = []
    for path in reversed(list_day_files(root, dataset)):
        if len(rows) >= limit:
            break  # every record in an older file is known earlier than all `limit` already found
        rows.extend(r for r in read_day_file(path) if r.scope == scope and field in r.fields)
    rows.sort(key=lambda r: (r.known_at, r.seq))
    out = []
    for r in rows[-limit:]:
        value = r.fields[field]
        out.append([r.known_at, r.effective_at, value if isinstance(value, (int, bool)) else str(value)])
    return out


def as_of(root: Path, policy: Policy, dataset: str, scope: str, at: int) -> list[dict[str, Any]]:
    facts = Reader(root, policy).as_of(dataset, scope, at)
    return [_record_view(facts[k]) for k in sorted(facts)]


def _parse_instant(text: str) -> int:
    text = text.strip()
    if text.isdigit():
        return int(text)
    parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("an instant needs a zone, e.g. 2024-03-20T18:00:00Z")
    return int(parsed.timestamp() * 1000)


def make_handler(root: Path, registry: Registry, policy: Policy) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "qkt-data-hub-ui"

        def log_message(self, fmt: str, *args: Any) -> None:  # keep the daemon's log readable
            return

        def _send(self, code: int, body: bytes, content_type: str) -> None:
            self.send_response(code)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def _json(self, doc: Any, code: int = 200) -> None:
            self._send(code, json.dumps(doc, separators=(",", ":")).encode(), "application/json")

        def do_GET(self) -> None:  # noqa: N802 - http.server's naming
            url = urlparse(self.path)
            q = {k: v[0] for k, v in parse_qs(url.query).items()}
            try:
                if url.path in ("/", "/index.html"):
                    self._send(200, _PAGE.read_bytes(), "text/html; charset=utf-8")
                    return
                with _READ_LOCK:
                    doc = self._answer(url.path, q)
                self._json(*doc)
            except (BrokenPipeError, ConnectionResetError):
                return  # the viewer left before the answer; nothing to tell it
            except KeyError as e:
                self._json({"error": f"missing parameter {e}"}, 400)
            except (ValueError, OSError) as e:
                self._json({"error": str(e)}, 400)

        def _answer(self, path: str, q: dict[str, str]) -> tuple[Any, int]:
            if path == "/api/state":
                return state(root, registry, policy), 200
            if path == "/api/records":
                return records(root, q["dataset"], min(int(q.get("limit", "50")), 500)), 200
            if path == "/api/series":
                return series(root, q["dataset"], q["scope"], q["field"]), 200
            if path == "/api/as-of":
                return as_of(root, policy, q["dataset"], q["scope"], _parse_instant(q["at"])), 200
            return {"error": "not found"}, 404

    return Handler


def bind(root: Path, registry: Registry, policy: Policy, host: str, port: int) -> ThreadingHTTPServer:
    """Bind the view now, so a taken port fails at start instead of after the daemon is up."""
    httpd = ThreadingHTTPServer((host, port), make_handler(Path(root), registry, policy))
    print(f"hub ui on http://{host}:{httpd.server_address[1]} (read-only) for {root}", flush=True)
    return httpd


def serve_in_background(root: Path, registry: Registry, policy: Policy, host: str, port: int) -> ThreadingHTTPServer:
    """Serve the view from a daemon thread beside the collector: one container, one process."""
    httpd = bind(root, registry, policy, host, port)
    threading.Thread(target=httpd.serve_forever, name="hub-ui", daemon=True).start()
    return httpd


def serve(root: Path, registry: Registry, policy: Policy, host: str, port: int) -> None:
    httpd = bind(root, registry, policy, host, port)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
