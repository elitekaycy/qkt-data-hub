"""When a backfilled FRED value becomes visible: never before the Fed's H.15 posting.

H.15 posts at 4:15 p.m. New York on federal business days with the previous business day's rates.
These pin the cases the earlier rule got wrong: the posting time itself, a holiday Monday, and a
monthly average that cannot be known until its month is over.
"""
import datetime as dt
import unittest
from zoneinfo import ZoneInfo

from tools.fred_backfill import published_at

NY = ZoneInfo("America/New_York")


def at(day: str, hhmm: str = "16:30") -> dt.datetime:
    return dt.datetime.combine(dt.date.fromisoformat(day), dt.time.fromisoformat(hhmm), tzinfo=NY)


class FredReleaseTest(unittest.TestCase):
    def test_a_daily_value_is_visible_after_the_next_business_days_posting(self):
        self.assertEqual(published_at(dt.date(2024, 6, 12), "daily"), at("2024-06-13"))

    def test_a_friday_value_waits_for_monday_and_a_holiday_monday_for_tuesday(self):
        self.assertEqual(published_at(dt.date(2024, 6, 14), "daily"), at("2024-06-17"))
        # 2024-09-02 was Labor Day: Friday 30 August's rates were posted on Tuesday.
        self.assertEqual(published_at(dt.date(2024, 8, 30), "daily"), at("2024-09-03"))

    def test_a_value_is_never_visible_before_4_15_pm_new_york(self):
        for day in (dt.date(2024, 1, 10), dt.date(2024, 7, 10)):
            stamp = published_at(day, "daily")
            self.assertGreaterEqual(stamp.astimezone(NY).time(), dt.time(16, 15))

    def test_a_monthly_average_waits_until_after_its_month(self):
        # FEDFUNDS for May 2026 is dated 2026-05-01 but cannot be known before June.
        self.assertEqual(published_at(dt.date(2026, 5, 1), "monthly"), at("2026-06-02"))
        self.assertGreater(published_at(dt.date(2026, 5, 1), "monthly"), at("2026-05-31"))


if __name__ == "__main__":
    unittest.main()
