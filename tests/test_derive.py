"""Tests for the derived-field expression language.

These pin the closed vocabulary a dataset schema may use for a derived field: exact decimal
arithmetic, total (non-raising) handling of missing data and division by zero, and compile-time
rejection of anything not in the vocabulary -- because a typo in a schema should fail loudly at
load, not silently return null forever in production.
"""
import unittest

from hub.derive import compile_expr, referenced_names
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
        with self.assertRaisesRegex(SchemaError, "unknown"):
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


if __name__ == "__main__":
    unittest.main()
