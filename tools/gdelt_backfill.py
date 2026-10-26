"""Backfill `news.gdelt.events` from GDELT's archive, month by month, resumably.

Every hour is computed by the same `hub.collectors.gdelt` functions the live collector uses, from
the export batches GDELT's master file list names, each checked against the MD5 the list gives.
A batch the list does not name was never published and is counted as missing (`batches` < 4); a
batch the list names but that cannot be downloaded after retries fails its month, which is then
not marked done, so a rerun picks it up rather than recording a gap that is not GDELT's.

Records are stamped `derived`, visible `PUBLISH_LAG_MS` after their hour ends, the instant the
live collector first could publish them.

Usage:
    python3 tools/gdelt_backfill.py --out /var/tmp/gdelt-backfill --since 2015-03 --until 2026-10
    for f in /var/tmp/gdelt-backfill/*.ndjson; do
      python3 -m hub --root <store> backfill news.gdelt.events --file "$f" --source gdelt
    done
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import io
import json
import sys
import time
import urllib.request
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hub.collectors import gdelt  # noqa: E402

MASTER_URL = gdelt.BASE_URL + "masterfilelist.txt"


def load_master(path: Path | None) -> dict[int, tuple[str, str]]:
    """Every export batch GDELT lists: stamp (epoch ms) -> (md5, url)."""
    if path is not None and path.is_file():
        text = path.read_text(encoding="utf-8", errors="replace")
    else:
        with urllib.request.urlopen(MASTER_URL, timeout=120) as resp:  # noqa: S310 - fixed https host
            text = resp.read().decode("utf-8", errors="replace")
        if path is not None:
            path.write_text(text, encoding="utf-8")
    out: dict[int, tuple[str, str]] = {}
    for line in text.splitlines():
        parts = line.split()
        if len(parts) != 3 or not parts[2].endswith(".export.CSV.zip"):
            continue
        stamp = parts[2].rsplit("/", 1)[1][:14]
        try:
            moment = dt.datetime.strptime(stamp, "%Y%m%d%H%M%S").replace(tzinfo=dt.UTC)
        except ValueError:
            continue
        out[int(moment.timestamp() * 1000)] = (parts[1], parts[2].replace("http://", "https://"))
    return out


def fetch(url: str, md5: str, attempts: int = 4) -> bytes:
    """The CSV inside one listed batch, its zip verified against the listed MD5."""
    error: Exception | None = None
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(url, timeout=60) as resp:  # noqa: S310 - fixed https host
                data = resp.read()
            if hashlib.md5(data).hexdigest() != md5:  # noqa: S324 - GDELT publishes MD5s
                raise ValueError(f"md5 mismatch for {url}")
            with zipfile.ZipFile(io.BytesIO(data)) as zf:
                (name,) = zf.namelist()
                return zf.read(name)
        except (OSError, ValueError, zipfile.BadZipFile) as e:
            error = e
            time.sleep(2**attempt)
    raise RuntimeError(f"{url}: {error}")


def hour_rows(hour_end: int, master: dict[int, tuple[str, str]]) -> list[dict[str, object]]:
    batches: dict[int, list[gdelt.Event] | None] = {}
    for stamp in gdelt.batch_stamps(hour_end):
        listed = master.get(stamp)
        batches[stamp] = None if listed is None else gdelt.parse_export(fetch(listed[1], listed[0]))[0]
    rows = []
    for c in gdelt.hour_candidates(hour_end, batches):
        rows.append(
            {
                "scope": c.scope,
                "effective_at": c.effective_at,
                "period_start": c.period_start,
                "period_end": c.period_end,
                "known_at": hour_end + gdelt.PUBLISH_LAG_MS,
                "availability": "derived",
                "fields": c.fields,
            }
        )
    return rows


def month_hours(month: str) -> list[int]:
    first = dt.datetime.strptime(month, "%Y-%m").replace(tzinfo=dt.UTC)
    nxt = (first + dt.timedelta(days=32)).replace(day=1)
    start = int(first.timestamp() * 1000) + gdelt.HOUR_MS
    end = int(nxt.timestamp() * 1000)
    return list(range(start, end + 1, gdelt.HOUR_MS))


def run_month(month: str, out: Path, master: dict[int, tuple[str, str]], workers: int, now_ms: int) -> bool:
    done = out / f"{month}.done"
    if done.exists():
        return True
    hours = [h for h in month_hours(month) if h + gdelt.PUBLISH_LAG_MS <= now_ms]
    failures: list[str] = []

    def one(hour: int) -> list[dict[str, object]]:
        try:
            return hour_rows(hour, master)
        except RuntimeError as e:
            failures.append(f"{gdelt.stamp_text(hour)} {e}")
            return []

    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(one, hours))
    rows = [row for rs in results for row in rs]
    (out / f"{month}.ndjson").write_text("".join(json.dumps(r, separators=(",", ":")) + "\n" for r in rows))
    listed = sum(1 for h in hours for s in gdelt.batch_stamps(h) if s in master)
    print(f"{month}: {len(hours)} hours, {listed}/{4 * len(hours)} batches listed, {len(rows)} records, "
          f"{len(failures)} failed", file=sys.stderr, flush=True)
    if failures:
        (out / f"{month}.failures").write_text("\n".join(failures) + "\n")
        return False
    done.write_text("ok\n")
    return True


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--since", default="2015-03")
    ap.add_argument("--until", required=True, help="last month to backfill, YYYY-MM")
    ap.add_argument("--workers", type=int, default=8)
    a = ap.parse_args()
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    master = load_master(out / "masterfilelist.txt")
    now_ms = int(time.time() * 1000)
    month = dt.datetime.strptime(a.since, "%Y-%m").date()
    last = dt.datetime.strptime(a.until, "%Y-%m").date()
    ok = True
    while month <= last:
        ok &= run_month(month.strftime("%Y-%m"), out, master, a.workers, now_ms)
        month = (month + dt.timedelta(days=32)).replace(day=1)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
