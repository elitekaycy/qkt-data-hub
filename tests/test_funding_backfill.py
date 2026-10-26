"""The funding backfill keeps the archive's exact instants and never invents a rate."""
import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from tools import funding_backfill


class FundingBackfillTest(unittest.TestCase):
    def test_exact_instants_null_price_and_no_rate_skipped(self):
        path = Path(tempfile.mkdtemp()) / "f.csv"
        path.write_text(
            "time,rate,price\n"
            "1568102400000,0.00010000,\n"
            "1791187200001,0.00004186,86221.60000000\n"
            "1791216000000,,85225.4\n"
            "1514764799999,0.0001,1\n"
        )
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            funding_backfill.main(str(path), "BTCUSD", "2018-01-01")
        rows = [json.loads(line) for line in out.getvalue().splitlines()]
        self.assertEqual([r["effective_at"] for r in rows], [1568102400000, 1791187200001])
        self.assertTrue(all(r["known_at"] == r["effective_at"] for r in rows))
        self.assertIsNone(rows[0]["fields"]["mark_price"])
        self.assertEqual(rows[1]["fields"], {"rate": "0.00004186", "mark_price": "86221.60000000"})


if __name__ == "__main__":
    unittest.main()
