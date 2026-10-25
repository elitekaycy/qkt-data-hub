"""Prove the store's own reader and the qkt engine see a calendar dataset identically.

The format has two independent readers: `hubread` here and qkt's Kotlin reader. Both promise
that a record is visible from its `known_at`, but nothing else would catch them drifting apart.
This tool derives, from `hubread` alone, the exact bar at which a probe strategy must enter
before every event in a window, runs that probe through a real qkt backtest against the same
store, and requires qkt's entry times to match one for one.

The probe enters on the first 5-minute bar close where the next event is at most `--lead` away
and still ahead, then exits two hours later, so each event yields exactly one entry. A record
counts as visible on a bar that closes strictly after its `known_at`: qkt processes a market tick
before a hub fact that shares its millisecond, so a fact is seen on the following evaluation.

Usage (on a host with Docker and price data):
    python3 tools/qkt_conformance.py --store <hub root> --dataset cal.us_high --scope USD \
      --data-root <prices> --symbol BACKTEST:EURUSD --from 2018-01-01 --to 2025-01-01 \
      --image ghcr.io/elitekaycy/qkt:v0.63.1
Exit code 0 only when every expected entry matches a qkt entry and qkt made no others.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import subprocess
import sys
import tempfile
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hubread.journal import read_range  # noqa: E402
from hubread.reader import Reader  # noqa: E402

BAR_MS = 300_000
JAVA_IMAGE = "eclipse-temurin:21-jre"
NY = ZoneInfo("America/New_York")


def ms(day: str) -> int:
    return int(dt.datetime.combine(dt.date.fromisoformat(day), dt.time(0, 0), tzinfo=dt.UTC).timestamp() * 1000)


def expected_entries(records: list, scope: str, lead_ms: int, start: int, end: int) -> list[int]:
    """First bar close per event at which the probe's condition holds, from `hubread` records."""
    out = []
    for r in sorted((r for r in records if r.scope == scope), key=lambda r: r.known_at):
        at = int(r.fields["at"])
        first = max(-(-(at - lead_ms) // BAR_MS) * BAR_MS, (r.known_at // BAR_MS + 1) * BAR_MS)
        if first < at and start <= first < end:
            out.append(first)
    return out


def probe(dataset: str, scope: str, symbol: str, lead_ms: int) -> str:
    return (
        "STRATEGY hub_conformance VERSION 1\n"
        "SYMBOLS\n"
        f"    px  = {symbol} EVERY 5m,\n"
        f"    cal = HUB:{dataset}.{scope} EVERY 1d\n"
        "RULES\n"
        f"    WHEN cal.at - NOW.epoch_ms > 0 AND cal.at - NOW.epoch_ms <= {lead_ms} AND POSITION.px = 0\n"
        "    THEN BUY px SIZING 0.01 EXIT AFTER 2h\n"
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--store", required=True)
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--scope", default="USD")
    ap.add_argument("--data-root", required=True)
    ap.add_argument("--symbol", default="BACKTEST:EURUSD")
    ap.add_argument("--from", dest="start", required=True)
    ap.add_argument("--to", dest="end", required=True)
    ap.add_argument("--lead", type=int, default=3_600_000, help="ms before an event the probe may enter")
    ap.add_argument("--image", help="a released qkt image, e.g. ghcr.io/elitekaycy/qkt:v0.63.1")
    ap.add_argument("--qkt-dist", help="a local qkt build (build/install/qkt), run in a Java 21 container instead")
    ap.add_argument("--extra-mount", action="append", default=[], help="read-only host path the price data links into")
    a = ap.parse_args()
    if bool(a.image) == bool(a.qkt_dist):
        ap.error("give exactly one of --image or --qkt-dist")

    start, end = ms(a.start), ms(a.end)
    store = Path(a.store).resolve()
    snap = Reader(store).range(a.dataset, 0, end)
    jour = read_range(store, a.dataset, 0, end)
    if sorted(r.id for r in snap) != sorted(r.id for r in jour):
        print("FAIL: snapshot and journal hold different records")
        return 1
    want = expected_entries(snap, a.scope, a.lead, start, end)

    with tempfile.TemporaryDirectory(dir=store.parent) as work:
        w = Path(work)
        (w / "probe.qkt").write_text(probe(a.dataset, a.scope, a.symbol, a.lead))
        data = Path(a.data_root).resolve()
        cmd = [
            "docker", "run", "--rm", "--user", "root", "-v", f"{w}:{w}", "-v", f"{store}:{store}:ro",
            "-v", f"{data}:{data}:ro",
            *[x for m in a.extra_mount for x in ("-v", f"{m}:{m}:ro")],
            "-w", str(w),
            *(
                ["--entrypoint", "qkt", a.image]
                if a.image
                else ["-v", f"{Path(a.qkt_dist).resolve()}:/opt/qkt:ro", "--entrypoint", "/opt/qkt/bin/qkt", JAVA_IMAGE]
            ),
            "backtest", "probe.qkt",
            "--from", a.start, "--to", a.end, "--data-root", str(data), "--hub-root", str(store),
            "--starting-balance", "100000", "--bars", "--no-fetch", "--allow-incomplete", "--report-dir", "rep",
        ]
        run = subprocess.run(cmd, capture_output=True, text=True)
        trades = w / "rep" / "trades.csv"
        if run.returncode != 0 or not trades.exists():
            print("FAIL: qkt backtest did not run\n" + (run.stderr or run.stdout)[-1500:])
            return 1
        got = sorted(int(r["timestamp"]) for r in csv.DictReader(trades.open()) if r["side"] == "BUY")

    missing = sorted(set(want) - set(got))
    extra = sorted(set(got) - set(want))
    fmt = lambda t: dt.datetime.fromtimestamp(t / 1000, NY).strftime("%Y-%m-%d %H:%M NY")  # noqa: E731
    print(f"{a.dataset}.{a.scope} {a.start}..{a.end}: {len(snap)} records, {len(want)} expected entries, "
          f"{len(got)} qkt entries, {len(want) - len(missing)} matched")
    for t in missing[:20]:
        print("  expected, qkt did not enter:", fmt(t))
    for t in extra[:20]:
        print("  qkt entered, not expected:   ", fmt(t))
    ok = not missing and not extra
    print("PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
