"""Hourly news-event aggregates from GDELT 2.0, one record per region per hour.

GDELT codes the world's news every 15 minutes into events (who did what to whom, where), each with
a CAMEO event code, a quad class (verbal/material cooperation/conflict), a Goldstein stability
score, the number of articles that covered it and their average tone. A strategy cannot read
headlines, so this collector turns each hour into numbers per region: how much was reported, how
much of it was conflict, threats, protests, sanctions or violence, and how the coverage read.

Timing. An hour's record covers the four export batches stamped in it (:15, :30, :45 and :00 at
its end, the batch stamp being GDELT's `DATEADDED`), and becomes visible `PUBLISH_LAG_MS` after
the hour ends. GDELT posts a batch before the time it is stamped with (measured on bot2:
`/var/tmp/gdelt-lag`), so the lag only adds margin. Live collection and the backfill
(`tools/gdelt_backfill.py`) share every function here, so both produce the same numbers and the
same instants.

Integrity. A batch GDELT never published is counted in `batches` (0-4), never treated as a quiet
quarter-hour; an hour with no batch at all has no record. Events are counted once per hour by
GDELT's own event id. Means are weighted by article count and rounded half-even to 8 places.

The raw archive keeps only the columns used here plus each original file's MD5, about 1 GB a
year, instead of GDELT's full exports at about 9 GB.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import io
import json
import time
import urllib.error
import urllib.request
import zipfile
import zlib
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from decimal import ROUND_HALF_EVEN, Decimal
from pathlib import Path
from typing import Any

from hub.collectors import Candidate
from hub.collectors.declarative import Cadence
from hub.logging import log
from hub.rawstore import RawBlob
from hubread.errors import ConfigError
from hubread.record import Availability

BASE_URL = "https://data.gdeltproject.org/gdeltv2/"
BATCH_MS = 15 * 60 * 1000
HOUR_MS = 60 * 60 * 1000
PUBLISH_LAG_MS = 5 * 60 * 1000
_EIGHT = Decimal("0.00000001")

# FIPS 10-4 country codes, as GDELT writes ActionGeo_CountryCode.
_EU = "AU BE BU HR CY EZ DA EN FI FR GM GR HU EI IT LG LH LU MT NL PL PO RO LO SI SP SW"
REGIONS: dict[str, frozenset[str] | None] = {
    "GLOBAL": None,
    "US": frozenset({"US"}),
    "EU": frozenset(_EU.split()),
    "UK": frozenset({"UK"}),
    "CN": frozenset({"CH", "HK"}),
    "JP": frozenset({"JA"}),
    "RU_UA": frozenset({"RS", "UP"}),
    "MIDEAST": frozenset("IS GZ WE IR IZ SY LE JO SA YM KU QA AE BA MU EG".split()),
}

# Export columns (GDELT 2.0 event table, 61 tab-separated columns).
_ID, _CODE, _BASE, _ROOT, _QUAD, _GOLDSTEIN, _ARTICLES, _TONE, _COUNTRY, _ADDED = 0, 26, 27, 28, 29, 30, 33, 34, 53, 59
_COLUMNS = 61
_VIOLENCE_ROOTS = frozenset({"18", "19", "20"})

FIELDS = (
    "events", "articles", "material_conflict", "verbal_conflict", "threats", "protests",
    "sanctions", "violence", "goldstein", "tone", "batches",
)


@dataclass(frozen=True, slots=True)
class Event:
    id: str
    code: str
    base: str
    root: str
    quad: int
    goldstein: Decimal
    articles: int
    tone: Decimal
    country: str


def parse_export(tsv: bytes) -> tuple[list[Event], int]:
    """The events in one export file and the number of malformed lines skipped."""
    out: list[Event] = []
    bad = 0
    for line in tsv.decode("utf-8", errors="replace").splitlines():
        cols = line.split("\t")
        if len(cols) != _COLUMNS:
            bad += 1
            continue
        try:
            out.append(
                Event(
                    id=cols[_ID],
                    code=cols[_CODE],
                    base=cols[_BASE],
                    root=cols[_ROOT],
                    quad=int(cols[_QUAD]),
                    goldstein=Decimal(cols[_GOLDSTEIN]),
                    articles=int(cols[_ARTICLES]),
                    tone=Decimal(cols[_TONE]),
                    country=cols[_COUNTRY],
                )
            )
        except (ValueError, ArithmeticError):
            bad += 1
    return out, bad


def _mean(total: Decimal, weight: int) -> str:
    return str((total / weight).quantize(_EIGHT, rounding=ROUND_HALF_EVEN))


def aggregate(events: Iterable[Event], countries: frozenset[str] | None) -> dict[str, str | None] | None:
    """One hour's numbers for one region, or None when the region had no events that hour."""
    seen: set[str] = set()
    n = articles = material = verbal = threats = protests = sanctions = violence = 0
    goldstein = tone = Decimal(0)
    for e in events:
        if e.id in seen or (countries is not None and e.country not in countries):
            continue
        seen.add(e.id)
        n += 1
        articles += e.articles
        material += e.quad == 4
        verbal += e.quad == 3
        threats += e.root == "13"
        protests += e.root == "14"
        sanctions += e.base == "163"
        violence += e.root in _VIOLENCE_ROOTS
        goldstein += e.goldstein * e.articles
        tone += e.tone * e.articles
    if n == 0:
        return None
    return {
        "events": str(n),
        "articles": str(articles),
        "material_conflict": str(material),
        "verbal_conflict": str(verbal),
        "threats": str(threats),
        "protests": str(protests),
        "sanctions": str(sanctions),
        "violence": str(violence),
        "goldstein": _mean(goldstein, articles) if articles else None,
        "tone": _mean(tone, articles) if articles else None,
    }


def batch_stamps(hour_end_ms: int) -> list[int]:
    """The four batch stamps an hour covers: :15, :30, :45 and the hour's end."""
    return [hour_end_ms - HOUR_MS + BATCH_MS * i for i in range(1, 5)]


def stamp_text(ms: int) -> str:
    return dt.datetime.fromtimestamp(ms / 1000, dt.UTC).strftime("%Y%m%d%H%M%S")


def hour_candidates(hour_end_ms: int, batches: dict[int, list[Event] | None]) -> list[Candidate]:
    """Every region's record for the hour ending `hour_end_ms`, from its batches (None = never published)."""
    present = [events for events in batches.values() if events is not None]
    if not present:
        return []
    events = [e for batch in present for e in batch]
    out: list[Candidate] = []
    for region, countries in REGIONS.items():
        fields = aggregate(events, countries)
        if fields is None:
            continue
        fields["batches"] = str(len(present))
        out.append(
            Candidate(
                scope=region,
                key="",
                effective_at=hour_end_ms,
                fields=fields,
                period_start=hour_end_ms - HOUR_MS,
                period_end=hour_end_ms,
                availability=Availability.OBSERVED,
            )
        )
    return out


def compact(hour_end_ms: int, batches: dict[int, tuple[str, list[Event]] | None]) -> bytes:
    """The raw-archive form of one hour: a JSON header (each batch's MD5) and the used columns, zlib'd."""
    header = {
        "hour_end": hour_end_ms,
        "batches": {stamp_text(s): (None if b is None else b[0]) for s, b in sorted(batches.items())},
    }
    lines = [json.dumps(header, sort_keys=True)]
    for stamp, batch in sorted(batches.items()):
        if batch is None:
            continue
        for e in batch[1]:
            lines.append(
                "\t".join(
                    [stamp_text(stamp), e.id, e.code, e.base, e.root, str(e.quad), str(e.goldstein),
                     str(e.articles), str(e.tone), e.country]
                )
            )
    return zlib.compress(("\n".join(lines) + "\n").encode("utf-8"), 9)


def expand(body: bytes) -> tuple[int, dict[int, list[Event] | None]]:
    """Inverse of `compact`: the hour and its batches, a batch never published as None."""
    text = zlib.decompress(body).decode("utf-8").splitlines()
    header = json.loads(text[0])
    stamps = {
        s: int(dt.datetime.strptime(s, "%Y%m%d%H%M%S").replace(tzinfo=dt.UTC).timestamp() * 1000)
        for s in header["batches"]
    }
    batches: dict[int, list[Event] | None] = {
        stamps[s]: (None if md5 is None else []) for s, md5 in header["batches"].items()
    }
    for line in text[1:]:
        c = line.split("\t")
        events = batches[stamps[c[0]]]
        assert events is not None
        events.append(Event(c[1], c[2], c[3], c[4], int(c[5]), Decimal(c[6]), int(c[7]), Decimal(c[8]), c[9]))
    return int(header["hour_end"]), batches


def download_batch(stamp_ms: int, timeout_seconds: float) -> tuple[str, bytes] | None:
    """One export batch as (md5 of the zip, the CSV inside), or None if GDELT never published it."""
    url = f"{BASE_URL}{stamp_text(stamp_ms)}.export.CSV.zip"
    try:
        with urllib.request.urlopen(url, timeout=timeout_seconds) as resp:  # noqa: S310 - fixed https host
            data = resp.read()
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        (name,) = zf.namelist()
        return hashlib.md5(data).hexdigest(), zf.read(name)  # noqa: S324 - GDELT publishes MD5s


class GdeltEventsSource:
    """Live half: every poll fetches the newest hour that is complete and past its publication lag."""

    def __init__(self, dataset: str, *, clock: Callable[[], float] = time.time) -> None:
        self._dataset = dataset
        self._clock = clock
        self._last: RawBlob | None = None
        self.cadence = Cadence(steady_seconds=300.0, retry_seconds=120.0)

    @classmethod
    def from_config(cls, dataset: str, options: dict[str, Any], datasets_dir: Path) -> GdeltEventsSource:
        if options:
            raise ConfigError(f"{dataset}.collector: unknown key(s) {sorted(options)!r}")
        return cls(dataset)

    @property
    def name(self) -> str:
        return self._dataset

    @property
    def parser(self) -> str:
        return "gdelt-events@1"

    @property
    def max_observed_age_ms(self) -> int | None:
        # An hour more than two hours old was missed live; it belongs to the backfill, stamped derived.
        return 2 * HOUR_MS

    def latest_hour_end(self) -> int:
        now = int(self._clock() * 1000)
        return (now - PUBLISH_LAG_MS) // HOUR_MS * HOUR_MS

    def fetch(self, timeout_seconds: float) -> RawBlob | None:
        hour_end = self.latest_hour_end()
        if self._last is not None and self._last.request.get("hour_end") == stamp_text(hour_end):
            return self._last  # this hour is already fetched; the pipeline counts it as a duplicate
        batches: dict[int, tuple[str, list[Event]] | None] = {}
        try:
            for stamp in batch_stamps(hour_end):
                got = download_batch(stamp, timeout_seconds)
                batches[stamp] = None if got is None else (got[0], parse_export(got[1])[0])
        except (OSError, zipfile.BadZipFile, ValueError) as e:
            log(f"collect[{self._dataset}] gdelt fetch failed: {e}")
            return None
        self._last = RawBlob(
            body=compact(hour_end, batches),
            content_type="application/x-qkt-gdelt-hour",
            fetched_at=int(self._clock() * 1000),
            request={"hour_end": stamp_text(hour_end)},
        )
        return self._last

    def parse(self, blob: RawBlob) -> list[Candidate]:
        hour_end, batches = expand(blob.body)
        return hour_candidates(hour_end, batches)
