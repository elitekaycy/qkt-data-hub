"""Tests for the scope token rule a consumer's stream symbol depends on.

qkt addresses a scope as the last dot-separated, upper-case segment of `HUB:<dataset>.<SCOPE>/<field>`,
so a scope with a dot or in lower case would be read as part of the dataset name and match nothing.
"""
import unittest

from hubread.record import SCOPE_RE


class ScopeTokenTest(unittest.TestCase):
    def test_accepts_every_scope_kind(self):
        scopes = ("USD", "ALL", "EXNESS:XAUUSD", "DERIBIT:BTC-PERPETUAL", "ISSUER:BRK-B", "BINANCE:BTCUSDT", "CME:ESH5")
        for scope in scopes:
            self.assertTrue(SCOPE_RE.match(scope), scope)

    def test_rejects_a_dot_lower_case_or_no_letter(self):
        for scope in ("ISSUER:BRK.B", "usd", "binance:btcusdt", "Usd", "1234", "", "A" * 65, "EUR+USD"):
            self.assertIsNone(SCOPE_RE.match(scope), scope)


if __name__ == "__main__":
    unittest.main()
