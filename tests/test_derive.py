"""Tests for the derived-field expression language.

These pin the closed vocabulary a dataset schema may use for a derived field: exact decimal
arithmetic, total (non-raising) handling of missing data and division by zero, and compile-time
rejection of anything not in the vocabulary -- because a typo in a schema should fail loudly at
load, not silently return null forever in production.
"""
import unittest

from hub.derive import RESERVED_EXPRESSION_NAMES, compile_expr, referenced_names
from hub.errors import SchemaError


class DeriveTest(unittest.TestCase):
    def test_referenced_names(self) -> None:
        self.assertEqual(referenced_names("actual - forecast"), frozenset({"actual", "forecast"}))

    def test_arithmetic_is_exact_decimal(self) -> None:
        f = compile_expr("actual - forecast")
        self.assertEqual(f({"actual": "0.3", "forecast": "0.2"}, []), "0.1")

    def test_none_input_yields_none(self) -> None:
        self.assertIsNone(compile_expr("actual - forecast")({"actual": None, "forecast": "0.2"}, []))

    def test_division_by_zero_yields_none(self) -> None:
        self.assertIsNone(compile_expr("actual / forecast")({"actual": "1", "forecast": "0"}, []))

    def test_zscore_uses_history_and_respects_min_obs(self) -> None:
        history = [{"surprise": str(v)} for v in (0, 0, 0, 1, -1)]
        f = compile_expr("zscore(surprise, window=5, min_obs=4)")
        self.assertIsNotNone(f({"surprise": "2"}, history))
        self.assertIsNone(f({"surprise": "2"}, history[:2]))

    def test_lag_reads_prior_payload(self) -> None:
        self.assertEqual(compile_expr("lag(actual, 1)")({"actual": "3"}, [{"actual": "1"}, {"actual": "2"}]), "2")

    def test_unknown_function_fails_at_compile_time(self) -> None:
        with self.assertRaisesRegex(SchemaError, "unknown"):
            compile_expr("magic(actual)")

    def test_bare_function_name_fails_at_compile_time(self) -> None:
        with self.assertRaisesRegex(SchemaError, "reserved function name"):
            compile_expr("zscore + 1")

    def test_unary_minus_and_parentheses(self) -> None:
        f = compile_expr("-(actual - forecast)")
        self.assertEqual(f({"actual": "0.3", "forecast": "0.2"}, []), "-0.1")

    def test_multiplication_and_precedence(self) -> None:
        f = compile_expr("actual + forecast * 2")
        self.assertEqual(f({"actual": "1", "forecast": "2"}, []), "5")

    def test_diff_is_current_minus_lag(self) -> None:
        f = compile_expr("diff(actual, 1)")
        self.assertEqual(f({"actual": "3"}, [{"actual": "1"}, {"actual": "2"}]), "1")

    def test_diff_none_when_history_too_short(self) -> None:
        f = compile_expr("diff(actual, 2)")
        self.assertIsNone(f({"actual": "3"}, [{"actual": "1"}]))

    def test_pct_rank_fraction_strictly_below(self) -> None:
        history = [{"x": str(v)} for v in (1, 2, 3, 4)]
        f = compile_expr("pct_rank(x, window=4)")
        self.assertEqual(f({"x": "3"}, history), "0.5")

    def test_since_uses_known_at_reference(self) -> None:
        f = compile_expr("since(published_at)")
        self.assertEqual(f({"published_at": 1000, "known_at": 1500}, []), "500")

    def test_until_uses_known_at_reference(self) -> None:
        f = compile_expr("until(effective_at)")
        self.assertEqual(f({"effective_at": 2000, "known_at": 1500}, []), "500")

    def test_since_none_without_known_at(self) -> None:
        f = compile_expr("since(published_at)")
        self.assertIsNone(f({"published_at": 1000}, []))

    def test_lag_none_field_yields_none(self) -> None:
        f = compile_expr("lag(actual, 1)")
        self.assertIsNone(f({"actual": "3"}, [{"actual": None}]))

    def test_referenced_names_from_function_calls(self) -> None:
        self.assertEqual(referenced_names("lag(actual, 1)"), frozenset({"actual"}))

    def test_overflow_yields_none_not_exception(self) -> None:
        f = compile_expr("actual * forecast")
        self.assertIsNone(f({"actual": "1e500000", "forecast": "1e500000"}, []))

    def test_reserved_names_match_supported_functions(self) -> None:
        self.assertEqual(RESERVED_EXPRESSION_NAMES, {"zscore", "lag", "diff", "pct_rank", "since", "until"})

    def test_zscore_keyword_args_accepted_in_either_order(self) -> None:
        history = [{"surprise": str(v)} for v in (0, 0, 0, 1, -1)]
        forward = compile_expr("zscore(surprise, window=5, min_obs=4)")
        reversed_order = compile_expr("zscore(surprise, min_obs=4, window=5)")
        payload = {"surprise": "2"}
        self.assertEqual(forward(payload, history), reversed_order(payload, history))

    def test_zscore_computes_on_partial_window_above_min_obs(self) -> None:
        # History is shorter than the requested window (5) but at or above min_obs (3): the
        # z-score must be computed on the partial window, not withheld as if data were missing.
        history = [{"surprise": str(v)} for v in (0, 0, 1)]
        f = compile_expr("zscore(surprise, window=5, min_obs=3)")
        self.assertIsNotNone(f({"surprise": "2"}, history))


if __name__ == "__main__":
    unittest.main()


class LookbackTest(unittest.TestCase):
    def test_each_function_reports_how_far_back_it_reads(self):
        from hub.derive import lookback

        self.assertEqual(lookback("actual - forecast"), 0)
        self.assertEqual(lookback("diff(value, 1)"), 1)
        self.assertEqual(lookback("lag(value, 3) + zscore(value, window=24, min_obs=8)"), 24)
        self.assertEqual(lookback("-pct_rank(value, window=10)"), 10)
        self.assertEqual(lookback("since(at)"), 0)

    def test_a_limited_history_is_exactly_the_tail_of_the_full_one(self):
        import random

        from hub.pipeline import ScopeHistory

        rng = random.Random(11)
        history = ScopeHistory()
        days = list(range(300))
        rng.shuffle(days)
        for d in days + rng.sample(days, 40):  # out of order, then revisions of existing keys
            history.upsert(d * 1000, f"k{d}", {"v": rng.random()})
        for d in rng.sample(range(300), 60):
            for limit in (0, 1, 5, 24, 400):
                full = history.before(d * 1000, f"k{d}")
                self.assertEqual(history.before(d * 1000, f"k{d}", limit=limit), full[-limit:] if limit else [])
