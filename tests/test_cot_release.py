"""The COT publication rule against every release date the CFTC itself has published.

A positioning report stamped knowable before it was published is look-ahead, so the rule that
stamps backfilled reports is held to the CFTC's own schedules: the 2026 release calendar (holiday
delays marked) and the original and catch-up dates in press releases 9138-25 and 9147-25.
"""
import datetime as dt
import unittest

from tools.cot_release import release_date, release_instant_ms

# Release dates from https://www.cftc.gov/MarketReports/CommitmentsofTraders/ReleaseSchedule/index.htm
SCHEDULE_2026 = (
    "01-05 01-09 01-16 01-23 01-30 02-06 02-13 02-20 02-27 03-06 03-13 03-20 03-27 04-03 04-10 04-17 "
    "04-24 05-01 05-08 05-15 05-22 05-29 06-05 06-12 06-22 06-26 07-06 07-10 07-17 07-24 07-31 08-07 "
    "08-14 08-21 08-28 09-04 09-11 09-18 09-25 10-02 10-09 10-16 10-23 10-30 11-06 11-16 11-20 11-30 "
    "12-04 12-11 12-18 12-28"
).split()

# "Original Publish Date" column of 9138-25: what the normal rule (holidays included) would have given.
ORIGINAL_2025 = {
    "2025-09-30": "2025-10-03", "2025-10-07": "2025-10-10", "2025-10-14": "2025-10-17",
    "2025-10-21": "2025-10-24", "2025-10-28": "2025-10-31", "2025-11-04": "2025-11-07",
    "2025-11-10": "2025-11-14", "2025-11-18": "2025-11-21", "2025-11-25": "2025-12-01",
    "2025-12-02": "2025-12-05", "2025-12-09": "2025-12-12", "2025-12-16": "2025-12-19",
    "2025-12-23": "2025-12-29", "2025-12-30": "2026-01-05", "2026-01-06": "2026-01-09",
    "2026-01-13": "2026-01-16", "2026-01-20": "2026-01-23",
}


def _tuesday_before(release: dt.date) -> dt.date:
    # A 2026 release carries the report of the latest Tuesday at least three days earlier.
    day = release - dt.timedelta(days=3)
    while day.weekday() != 1:
        day -= dt.timedelta(days=1)
    return day


class CotReleaseTest(unittest.TestCase):
    def test_every_2026_release_date(self):
        for md in SCHEDULE_2026:
            release = dt.date.fromisoformat(f"2026-{md}")
            report = _tuesday_before(release)
            if report.year < 2026 and report != dt.date(2025, 12, 30):
                continue
            self.assertEqual(release_date(report), release, report)

    def test_the_normal_rule_reproduces_the_original_2025_dates(self):
        from tools import cot_release

        saved = dict(cot_release.CATCH_UP)
        cot_release.CATCH_UP.clear()
        try:
            for report, release in ORIGINAL_2025.items():
                self.assertEqual(release_date(dt.date.fromisoformat(report)), dt.date.fromisoformat(release), report)
        finally:
            cot_release.CATCH_UP.update(saved)

    def test_the_2025_shutdown_reports_carry_their_catch_up_dates(self):
        self.assertEqual(release_date(dt.date(2025, 10, 7)), dt.date(2025, 11, 21))
        self.assertEqual(release_date(dt.date(2025, 12, 23)), dt.date(2025, 12, 29))
        self.assertEqual(release_date(dt.date(2025, 12, 30)), dt.date(2026, 1, 5))

    def test_every_announced_change_since_2008(self):
        # COT Historical Special Announcements: report date -> publication date as announced.
        announced = {
            "2008-12-22": "2008-12-29",  # "will be for the prior Monday's open interest positions"
            "2008-12-30": "2009-01-05",
            "2013-10-01": "2013-10-25",  # 6745-13, first delayed report
            "2014-12-23": "2014-12-30",
            "2015-06-30": "2015-07-06",  # July 3 holiday
            "2018-12-24": "2019-02-01",  # Monday data before the 2018 Christmas, then 7864-19
            "2020-12-21": "2020-12-28",  # Monday data, Thursday 24 December added holiday
            "2021-06-15": "2021-06-21",  # Juneteenth's first observance
            "2023-01-31": "2023-02-24",  # ION cyber incident
            "2023-02-07": "2023-03-03",
            "2023-02-14": "2023-03-08",
            "2023-02-21": "2023-03-10",
            "2023-02-28": "2023-03-14",
            "2023-03-07": "2023-03-16",
            "2023-03-14": "2023-03-21",
            "2025-01-07": "2025-01-13",  # President Carter, day of mourning
        }
        for report, release in announced.items():
            self.assertEqual(release_date(dt.date.fromisoformat(report)), dt.date.fromisoformat(release), report)

    def test_the_2013_catch_up_is_never_earlier_than_the_announced_plan_allows(self):
        # Two reports the week of 28 October, two more by 8 November, the schedule resumed 8 November.
        self.assertEqual(release_date(dt.date(2013, 10, 8)), dt.date(2013, 11, 1))
        self.assertEqual(release_date(dt.date(2013, 10, 29)), dt.date(2013, 11, 8))
        self.assertEqual(release_date(dt.date(2013, 11, 5)), dt.date(2013, 11, 8))

    def test_a_report_after_the_ion_backlog_is_back_on_friday(self):
        self.assertEqual(release_date(dt.date(2023, 3, 21)), dt.date(2023, 3, 24))

    def test_release_is_15_30_new_york_in_summer_and_winter(self):
        summer = dt.datetime.fromtimestamp(release_instant_ms(dt.date(2024, 7, 9)) / 1000, dt.UTC)
        winter = dt.datetime.fromtimestamp(release_instant_ms(dt.date(2024, 1, 9)) / 1000, dt.UTC)
        self.assertEqual((summer.hour, summer.minute), (19, 30))
        self.assertEqual((winter.hour, winter.minute), (20, 30))


if __name__ == "__main__":
    unittest.main()
