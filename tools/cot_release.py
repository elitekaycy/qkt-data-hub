"""When each CFTC Commitments of Traders report was actually published.

A COT report carries its "as of" date (normally a Tuesday) but not the instant it became public,
and backfill must stamp that instant honestly. The CFTC publishes at 15:30 New York time on the
Friday after the report, except:

- a federal holiday (or a one-off federal closure) on the Wednesday, Thursday or Friday of the
  release week moves the release to the next business day (the CFTC's own schedules mark every
  such date, e.g. 2026-01-05, 2026-06-22, 2026-11-30);
- a lapse in appropriations stops publication, and the backlog is then released on a published
  catch-up schedule (2019: press release 7864-19; 2025: 9138-25, accelerated by 9147-25).

`tests/test_cot_release.py` checks this against every date the CFTC has published for 2025-2026.
"""
from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools.us_calendar import closed  # noqa: E402

NY = ZoneInfo("America/New_York")
RELEASE_TIME = dt.time(15, 30)

# Reports published on a catch-up schedule after a lapse in appropriations: report date -> release date.
CATCH_UP: dict[dt.date, dt.date] = {
    dt.date.fromisoformat(k): dt.date.fromisoformat(v)
    for k, v in {
        # 7864-19: first delayed report on Fri 2019-02-01, then one each Tuesday and Friday until current.
        "2018-12-24": "2019-02-01",
        "2018-12-31": "2019-02-05",
        "2019-01-08": "2019-02-08",
        "2019-01-15": "2019-02-12",
        "2019-01-22": "2019-02-15",
        "2019-01-29": "2019-02-19",
        "2019-02-05": "2019-02-22",
        "2019-02-12": "2019-02-26",
        "2019-02-19": "2019-03-01",
        "2019-02-26": "2019-03-05",
        # 9138-25 for the reports released before 9147-25 accelerated the rest.
        "2025-09-30": "2025-11-19",
        "2025-10-07": "2025-11-21",
        "2025-10-14": "2025-11-25",
        "2025-10-21": "2025-12-02",
        "2025-10-28": "2025-12-05",
        "2025-11-04": "2025-12-09",
        # 9147-25 (2025-12-09): backlog cleared by 2025-12-29.
        "2025-11-10": "2025-12-10",
        "2025-11-18": "2025-12-12",
        "2025-11-25": "2025-12-15",
        "2025-12-02": "2025-12-17",
        "2025-12-09": "2025-12-19",
        "2025-12-16": "2025-12-23",
        "2025-12-23": "2025-12-29",
    }.items()
}


def release_date(report: dt.date) -> dt.date:
    """The day the report dated `report` was (or is scheduled to be) published."""
    if report in CATCH_UP:
        return CATCH_UP[report]
    friday = report + dt.timedelta(days=(4 - report.weekday()) % 7 or 7)
    # Wednesday to Friday are the processing days. A Tuesday holiday already moved the report's
    # "as of" date back to Monday and leaves the Friday release alone (2025-11-10 -> 2025-11-14);
    # a closure on Wednesday, Thursday or Friday delays it (2026-11-11 -> 2026-11-16).
    processing = (friday - dt.timedelta(days=i) for i in range(3))
    if not any(closed(d) for d in processing):
        return friday
    day = friday + dt.timedelta(days=1)
    while closed(day):
        day += dt.timedelta(days=1)
    return day


def release_instant_ms(report: dt.date) -> int:
    """Epoch milliseconds of the release: 15:30 New York time on `release_date`."""
    return int(dt.datetime.combine(release_date(report), RELEASE_TIME, tzinfo=NY).timestamp() * 1000)
