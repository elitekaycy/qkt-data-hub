"""Tests for the dataset schema: field types, load-time validation, content hash, payload checks.

A schema is config every other stage trusts implicitly -- the pipeline validates payloads
against it, and a backtest cites its hash for reproducibility. These tests pin the load-time
fail-closed rules (unknown keys, reserved names, bad references) and the payload normalisation
a consumer relies on (canonical decimals, checked enums, checked ranges).
"""
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from hub.errors import SchemaError
from hub.schema import DatasetSchema, FieldType, load_schema

SCHEMA = """
dataset: macro.us.cpi
version: 1
title: US CPI releases
scope_kind: currency
key: [period_start, scope, title]
fields:
  title:      { type: string, strategy: false }
  impact:     { type: enum, values: [low, medium, high, holiday] }
  actual:     { type: number, unit: pct, null_policy: allow }
  forecast:   { type: number, unit: pct, null_policy: allow }
  surprise:   { type: number, unit: pct, derived: "actual - forecast" }
quality:
  range: { actual: [-5, 5] }
"""


class SchemaTest(unittest.TestCase):
    def _load(self, text: str) -> DatasetSchema:
        with TemporaryDirectory() as d:
            p = Path(d) / "macro.us.cpi.yaml"
            p.write_text(text)
            return load_schema(p)

    def test_loads_fields_in_declaration_order(self):
        s = self._load(SCHEMA)
        self.assertEqual(s.name, "macro.us.cpi")
        self.assertEqual([f.name for f in s.strategy_fields()], ["impact", "actual", "forecast", "surprise"])
        self.assertEqual(s.fields["impact"].type, FieldType.ENUM)
        self.assertEqual(s.fields["impact"].values, ("low", "medium", "high", "holiday"))
        self.assertEqual(s.fields["actual"].unit, "pct")

    def test_hash_is_stable_and_content_addressed(self):
        self.assertEqual(self._load(SCHEMA).hash(), self._load(SCHEMA).hash())
        self.assertNotEqual(self._load(SCHEMA).hash(), self._load(SCHEMA.replace("version: 1", "version: 2")).hash())

    def test_rejects_unknown_top_level_key(self):
        with self.assertRaisesRegex(SchemaError, "unknown"):
            self._load(SCHEMA + "\nnonsense: 1\n")

    def test_rejects_field_name_colliding_with_envelope(self):
        with self.assertRaisesRegex(SchemaError, "known_at"):
            self._load(SCHEMA.replace("  actual:", "  known_at:"))

    def test_rejects_key_naming_an_undeclared_field(self):
        with self.assertRaisesRegex(SchemaError, "nope"):
            self._load(SCHEMA.replace("key: [period_start, scope, title]", "key: [nope]"))

    def test_validate_payload_accepts_declared_fields(self):
        s = self._load(SCHEMA)
        out = s.validate_payload({"impact": 2, "actual": "0.3", "forecast": "0.2", "surprise": "0.1", "title": "CPI"})
        self.assertEqual(out["actual"], "0.3")

    def test_validate_payload_rejects_undeclared_field(self):
        with self.assertRaisesRegex(SchemaError, "undeclared"):
            self._load(SCHEMA).validate_payload({"impact": 2, "surprise_zz": "1"})

    def test_validate_payload_rejects_null_when_forbidden(self):
        with self.assertRaisesRegex(SchemaError, "null"):
            self._load(SCHEMA).validate_payload({"impact": None})

    def test_validate_payload_rejects_out_of_range(self):
        with self.assertRaisesRegex(SchemaError, "range"):
            self._load(SCHEMA).validate_payload({"impact": 2, "actual": "99"})

    def test_validate_payload_canonicalises_decimals(self):
        out = self._load(SCHEMA).validate_payload({"impact": 2, "actual": "0.30"})
        self.assertEqual(out["actual"], "0.3")

    # -- additional requirements from the brief, prose section --

    def test_rejects_bad_scope_kind(self):
        with self.assertRaisesRegex(SchemaError, "scope_kind"):
            self._load(SCHEMA.replace("scope_kind: currency", "scope_kind: planet"))

    def test_rejects_empty_enum_values(self):
        with self.assertRaisesRegex(SchemaError, "values"):
            self._load(SCHEMA.replace(
                "impact:     { type: enum, values: [low, medium, high, holiday] }",
                "impact:     { type: enum, values: [] }",
            ))

    def test_rejects_duplicate_enum_values(self):
        with self.assertRaisesRegex(SchemaError, "unique"):
            self._load(SCHEMA.replace(
                "impact:     { type: enum, values: [low, medium, high, holiday] }",
                "impact:     { type: enum, values: [low, low] }",
            ))

    def test_rejects_uppercase_enum_values(self):
        with self.assertRaisesRegex(SchemaError, "lowercase"):
            self._load(SCHEMA.replace(
                "impact:     { type: enum, values: [low, medium, high, holiday] }",
                "impact:     { type: enum, values: [Low, medium] }",
            ))

    def test_rejects_bad_null_policy(self):
        with self.assertRaisesRegex(SchemaError, "null_policy"):
            self._load(SCHEMA.replace("null_policy: allow", "null_policy: sometimes"))

    def test_null_policy_defaults_to_forbid(self):
        s = self._load(SCHEMA)
        self.assertEqual(s.fields["impact"].null_policy, "forbid")

    def test_value_alias_must_name_a_declared_field(self):
        with self.assertRaisesRegex(SchemaError, "value_alias"):
            self._load(SCHEMA + "\nvalue_alias: nope\n")

    def test_value_alias_accepts_declared_field(self):
        s = self._load(SCHEMA + "\nvalue_alias: actual\n")
        self.assertEqual(s.value_alias, "actual")

    def test_derived_field_may_chain_and_is_ordered_by_dependency(self):
        # A surprise and its z-score is the format's own worked example, so chaining is the
        # normal case. What matters is that the dependency is evaluated first, every time.
        chained = SCHEMA.replace(
            'surprise:   { type: number, unit: pct, derived: "actual - forecast" }\n',
            'surprise:   { type: number, unit: pct, derived: "actual - forecast" }\n'
            '  double_surprise: { type: number, derived: "surprise * 2" }\n',
        )
        schema = self._load(chained)
        order = [f.name for f in schema.derived_fields()]
        self.assertLess(order.index("surprise"), order.index("double_surprise"))

    def test_derived_cycle_is_rejected(self):
        cyclic = SCHEMA.replace(
            'surprise:   { type: number, unit: pct, derived: "actual - forecast" }',
            'a_field: { type: number, derived: "b_field + 1" }\n'
            '  b_field: { type: number, derived: "a_field + 1" }',
        )
        with self.assertRaisesRegex(SchemaError, "cycle"):
            self._load(cyclic)

    def test_derived_self_reference_is_rejected(self):
        selfref = SCHEMA.replace(
            'surprise:   { type: number, unit: pct, derived: "actual - forecast" }',
            'surprise:   { type: number, unit: pct, derived: "surprise + 1" }',
        )
        with self.assertRaisesRegex(SchemaError, "itself"):
            self._load(selfref)

    def test_derived_field_can_reference_envelope_timestamps(self):
        s = self._load(SCHEMA.replace(
            'surprise:   { type: number, unit: pct, derived: "actual - forecast" }',
            'surprise:   { type: number, unit: pct, derived: "actual - forecast + known_at - effective_at" }',
        ))
        self.assertEqual(s.fields["surprise"].derived, "actual - forecast + known_at - effective_at")

    def test_rejects_field_name_colliding_with_expression_function(self):
        with self.assertRaisesRegex(SchemaError, "reserved"):
            self._load(SCHEMA.replace("  actual:", "  zscore:"))

    def test_rejects_field_name_not_matching_field_name_re(self):
        with self.assertRaisesRegex(SchemaError, "field name"):
            self._load(SCHEMA.replace("  actual:", "  Actual:"))

    def test_rejects_key_not_a_list(self):
        with self.assertRaisesRegex(SchemaError, "key"):
            self._load(SCHEMA.replace("key: [period_start, scope, title]", "key: period_start"))

    def test_rejects_empty_key(self):
        with self.assertRaisesRegex(SchemaError, "key"):
            self._load(SCHEMA.replace("key: [period_start, scope, title]", "key: []"))

    def test_key_may_name_envelope_names(self):
        s = self._load(SCHEMA.replace("key: [period_start, scope, title]", "key: [effective_at, scope]"))
        self.assertEqual(s.key, ("effective_at", "scope"))

    def test_rejects_unknown_quality_range_field(self):
        with self.assertRaisesRegex(SchemaError, "range"):
            self._load(SCHEMA.replace("range: { actual: [-5, 5] }", "range: { nope: [-5, 5] }"))

    def test_rejects_unknown_key_nested_in_field(self):
        with self.assertRaisesRegex(SchemaError, "unknown"):
            self._load(SCHEMA.replace(
                "actual:     { type: number, unit: pct, null_policy: allow }",
                "actual:     { type: number, unit: pct, null_policy: allow, bogus: 1 }",
            ))

    def test_derived_fields_helper(self):
        s = self._load(SCHEMA)
        self.assertEqual([f.name for f in s.derived_fields()], ["surprise"])

    def test_strategy_false_field_excluded_from_strategy_fields_but_still_validated(self):
        s = self._load(SCHEMA)
        self.assertNotIn("title", [f.name for f in s.strategy_fields()])
        with self.assertRaisesRegex(SchemaError, "type"):
            s.validate_payload({"impact": 2, "title": 123})

    def test_validate_payload_rejects_wrong_type(self):
        with self.assertRaisesRegex(SchemaError, "type"):
            self._load(SCHEMA).validate_payload({"impact": "not-an-int"})

    def test_validate_payload_rejects_out_of_range_enum_ordinal(self):
        with self.assertRaisesRegex(SchemaError, "ordinal"):
            self._load(SCHEMA).validate_payload({"impact": 99})

    def test_validate_payload_timestamp_field(self):
        with TemporaryDirectory() as d:
            p = Path(d) / "cal.event.yaml"
            p.write_text(
                "dataset: cal.event\nversion: 1\ntitle: t\nscope_kind: currency\n"
                "key: [period_start]\nfields:\n  ts: { type: timestamp }\n"
            )
            s = load_schema(p)
        out = s.validate_payload({"ts": 1700000000000})
        self.assertEqual(out["ts"], 1700000000000)
        with self.assertRaisesRegex(SchemaError, "type"):
            s.validate_payload({"ts": "not-an-int"})

    # -- fix round 1: number-field type checking, decimal range comparison, key/derived --

    def test_validate_payload_rejects_bool_for_number_field(self):
        with self.assertRaisesRegex(SchemaError, "type"):
            self._load(SCHEMA).validate_payload({"impact": 2, "actual": True})

    def test_validate_payload_rejects_non_numeric_string_for_number_field(self):
        with self.assertRaisesRegex(SchemaError, "not a valid number"):
            self._load(SCHEMA).validate_payload({"impact": 2, "actual": "abc"})

    def test_validate_payload_rejects_list_for_number_field(self):
        with self.assertRaisesRegex(SchemaError, "type"):
            self._load(SCHEMA).validate_payload({"impact": 2, "actual": [1, 2]})

    def test_validate_payload_rejects_dict_for_number_field(self):
        with self.assertRaisesRegex(SchemaError, "type"):
            self._load(SCHEMA).validate_payload({"impact": 2, "actual": {"a": 1}})

    def test_validate_payload_rejects_null_for_number_field_when_forbidden(self):
        with TemporaryDirectory() as d:
            p = Path(d) / "cal.strict.yaml"
            p.write_text(
                "dataset: cal.strict\nversion: 1\ntitle: t\nscope_kind: currency\n"
                "key: [period_start]\nfields:\n  amount: { type: number }\n"
            )
            s = load_schema(p)
        with self.assertRaisesRegex(SchemaError, "null"):
            s.validate_payload({"amount": None})

    def test_quality_range_compares_as_decimal_not_float(self):
        with TemporaryDirectory() as d:
            p = Path(d) / "macro.precise.yaml"
            p.write_text(
                "dataset: macro.precise\nversion: 1\ntitle: t\nscope_kind: currency\n"
                "key: [period_start]\nfields:\n  amount: { type: number }\n"
                "quality:\n  range: { amount: [0, 100000000000000000] }\n"
            )
            s = load_schema(p)
        # Differs from the bound only beyond the 15th significant digit; float(...) collapses
        # these to equal values and would wrongly accept it as in-range.
        with self.assertRaisesRegex(SchemaError, "range"):
            s.validate_payload({"amount": "100000000000000003"})

    def test_rejects_derived_field_named_in_key(self):
        with self.assertRaisesRegex(SchemaError, "derived"):
            self._load(SCHEMA.replace(
                "key: [period_start, scope, title]",
                "key: [period_start, scope, surprise]",
            ))

    def test_validate_payload_bool_field(self):
        with TemporaryDirectory() as d:
            p = Path(d) / "cal.flag.yaml"
            p.write_text(
                "dataset: cal.flag\nversion: 1\ntitle: t\nscope_kind: currency\n"
                "key: [period_start]\nfields:\n  is_holiday: { type: bool }\n"
            )
            s = load_schema(p)
        out = s.validate_payload({"is_holiday": True})
        self.assertEqual(out["is_holiday"], True)
        with self.assertRaisesRegex(SchemaError, "type"):
            s.validate_payload({"is_holiday": "yes"})


if __name__ == "__main__":
    unittest.main()
