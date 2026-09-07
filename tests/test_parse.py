import unittest

from hub.errors import ParseError
from hub.parse import (
    PARSERS,
    apply,
    boolean,
    date_in_zone,
    enum_ordinal,
    human_number,
    iso8601_with_offset,
    percent_or_number,
)


class ParseTest(unittest.TestCase):
    def test_percent_strips_sign_and_canonicalises(self):
        self.assertEqual(percent_or_number("0.30%"), "0.3")
        self.assertEqual(percent_or_number("-1.2%"), "-1.2")
        self.assertEqual(percent_or_number("1,234"), "1234")

    def test_percent_maps_sentinels_to_none(self):
        for token in ("", " ", "-", "n/a", "N/A", "—"):
            self.assertIsNone(percent_or_number(token))

    def test_percent_maps_none_to_none(self):
        self.assertIsNone(percent_or_number(None))

    def test_human_suffixes(self):
        self.assertEqual(percent_or_number("1.2K"), "1200")
        self.assertEqual(percent_or_number("3.4M"), "3400000")
        self.assertEqual(percent_or_number("2B"), "2000000000")

    def test_thousands_separator_combined_with_suffix(self):
        # "1,234K" -> comma stripped first (1234), then the K suffix applies: 1234 * 1e3.
        self.assertEqual(human_number("1,234K"), "1234000")

    def test_human_number_is_case_insensitive_on_suffix(self):
        self.assertEqual(human_number("1.2k"), "1200")
        self.assertEqual(human_number("1.2K"), "1200")

    def test_human_number_sentinels_to_none(self):
        for token in ("", " ", "-", "n/a", "N/A", "—", None):
            self.assertIsNone(human_number(token))

    def test_human_number_rejects_garbage(self):
        with self.assertRaises(ParseError):
            human_number("not-a-number")

    def test_iso8601_with_offset_converts_to_utc_ms(self):
        # 08:15 New York in daylight time is 12:15 UTC.
        self.assertEqual(iso8601_with_offset("2026-09-10T08:15:00-04:00"), 1789042500000)

    def test_iso8601_rejects_naive_timestamp(self):
        with self.assertRaisesRegex(ParseError, "offset"):
            iso8601_with_offset("2026-09-10T08:15:00")

    def test_iso8601_rejects_garbage(self):
        with self.assertRaises(ParseError):
            iso8601_with_offset("not-a-timestamp")

    def test_date_in_zone_handles_dst(self):
        summer = date_in_zone("2026-07-01", "America/New_York", hour=8)
        winter = date_in_zone("2026-01-05", "America/New_York", hour=8)
        self.assertEqual((summer % 86_400_000) // 3_600_000, 12)
        self.assertEqual((winter % 86_400_000) // 3_600_000, 13)

    def test_date_in_zone_unknown_zone_raises_parse_error(self):
        with self.assertRaises(ParseError):
            date_in_zone("2026-07-01", "Nowhere/Imaginary", hour=8)

    def test_enum_ordinal_is_case_insensitive_and_none_on_miss(self):
        mapping = {"low": 0, "high": 2}
        self.assertEqual(enum_ordinal("HIGH", mapping), 2)
        self.assertIsNone(enum_ordinal("weird", mapping))

    def test_enum_ordinal_sentinel_is_none(self):
        mapping = {"low": 0, "high": 2}
        self.assertIsNone(enum_ordinal("-", mapping))

    def test_boolean_recognises_common_spellings(self):
        self.assertTrue(boolean("true"))
        self.assertTrue(boolean("Yes"))
        self.assertFalse(boolean("false"))
        self.assertFalse(boolean("No"))

    def test_boolean_sentinel_is_none(self):
        self.assertIsNone(boolean("n/a"))
        self.assertIsNone(boolean(None))

    def test_boolean_rejects_unrecognised_token(self):
        with self.assertRaises(ParseError):
            boolean("maybe")

    def test_apply_dispatches_by_name(self):
        self.assertEqual(apply("percent_or_number", "0.30%"), "0.3")
        self.assertEqual(apply("date_in_zone", "2026-07-01", zone="America/New_York", hour=8), 1782907200000)

    def test_apply_unknown_name_raises_parse_error(self):
        with self.assertRaises(ParseError):
            apply("does_not_exist", "1")

    def test_parsers_table_covers_every_public_parser(self):
        expected = {
            "human_number": human_number,
            "percent_or_number": percent_or_number,
            "iso8601_with_offset": iso8601_with_offset,
            "date_in_zone": date_in_zone,
            "enum_ordinal": enum_ordinal,
            "boolean": boolean,
        }
        for name, function in expected.items():
            self.assertIn(name, PARSERS)
            self.assertIs(PARSERS[name], function)


if __name__ == "__main__":
    unittest.main()
