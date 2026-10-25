"""US federal business days, for stamping when a US agency could have published something.

Federal statistical releases (the Fed's H.15, the CFTC's Commitments of Traders) are not posted
on federal holidays or on days federal offices are closed by executive order, so a backfill that
assumes "the next weekday" makes a fact visible a day early around every holiday.
"""
from __future__ import annotations

import datetime as dt

# One-off federal closures declared by executive order or for a day of mourning.
SPECIAL_CLOSURES = frozenset(
    dt.date.fromisoformat(d)
    for d in (
        "2018-12-05",  # President George H. W. Bush, day of mourning
        "2018-12-24",
        "2019-12-24",
        "2020-12-24",
        "2024-12-24",
        "2025-01-09",  # President Jimmy Carter, day of mourning
        "2025-12-24",
        "2025-12-26",
    )
)


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> dt.date:
    first = dt.date(year, month, 1)
    return first + dt.timedelta(days=(weekday - first.weekday()) % 7 + 7 * (n - 1))


def _last_weekday(year: int, month: int, weekday: int) -> dt.date:
    last = dt.date(year, month + 1, 1) - dt.timedelta(days=1) if month < 12 else dt.date(year, 12, 31)
    return last - dt.timedelta(days=(last.weekday() - weekday) % 7)


def _observed(day: dt.date) -> dt.date:
    if day.weekday() == 5:
        return day - dt.timedelta(days=1)
    if day.weekday() == 6:
        return day + dt.timedelta(days=1)
    return day


def federal_holidays(year: int) -> set[dt.date]:
    """US federal holidays as observed (a Saturday holiday on the Friday, a Sunday one on the Monday)."""
    days = {
        _observed(dt.date(year, 1, 1)),
        _nth_weekday(year, 1, 0, 3),  # Martin Luther King Jr. Day
        _nth_weekday(year, 2, 0, 3),  # Washington's Birthday
        _last_weekday(year, 5, 0),  # Memorial Day
        _observed(dt.date(year, 7, 4)),
        _nth_weekday(year, 9, 0, 1),  # Labor Day
        _nth_weekday(year, 10, 0, 2),  # Columbus Day
        _observed(dt.date(year, 11, 11)),
        _nth_weekday(year, 11, 3, 4),  # Thanksgiving
        _observed(dt.date(year, 12, 25)),
    }
    if year >= 2021:
        days.add(_observed(dt.date(year, 6, 19)))  # Juneteenth, first observed 2021-06-18
    # New Year's Day of the next year observed on this year's Friday 31 December.
    if dt.date(year + 1, 1, 1).weekday() == 5:
        days.add(dt.date(year, 12, 31))
    return days


def closed(day: dt.date) -> bool:
    return day.weekday() >= 5 or day in SPECIAL_CLOSURES or day in federal_holidays(day.year)


def next_business_day(day: dt.date, n: int = 1) -> dt.date:
    """The `n`th federal business day after `day`."""
    out = day
    for _ in range(n):
        out += dt.timedelta(days=1)
        while closed(out):
            out += dt.timedelta(days=1)
    return out
