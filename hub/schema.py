"""The dataset schema: field types, identity key, and derived fields, loaded and hashed.

A schema is the one document every other stage of the hub validates against -- a collector's
output, the pipeline's payload normalisation, a backtest's citation of exactly which contract it
ran against. `load_schema` is therefore fail-closed: an unknown key anywhere in the document, a
field name that collides with the envelope or a derived-expression function, or a reference to
something undeclared is rejected at load time rather than silently ignored, because a typo here
would otherwise disable a check nobody notices is missing until a backtest is unreproducible.

`.hash()` is computed over the raw parsed document, not over the typed dataclasses -- so it
changes whenever any declared byte of meaning changes, including a key this module does not
itself interpret yet.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Any

from hub.derive import RESERVED_EXPRESSION_NAMES, referenced_names
from hub.simpleyaml import load_path
from hubread.errors import RecordError, SchemaError
from hubread.record import FIELD_NAME_RE, RESERVED_FIELD_NAMES, canonical_json, decimal_str

#: Envelope timestamp names a derived expression may read in addition to declared fields --
#: the pipeline merges these into the payload it hands a compiled expression at evaluation time.
_ENVELOPE_EXPRESSION_NAMES = frozenset({"known_at", "effective_at"})

#: Envelope names a `key` list may name in addition to declared fields, because the identity key
#: of a record often includes envelope data (when a fact took effect, which scope it is about)
#: rather than only payload fields.
_ENVELOPE_KEY_NAMES = frozenset({"scope", "period_start", "period_end", "effective_at"})

_SCOPE_KINDS = frozenset({"currency", "instrument", "issuer", "all"})
_NULL_POLICIES = frozenset({"forbid", "allow", "allow_until_release"})

# `collector` and `retention` are carried but not interpreted here: the schema owns what a
# record MEANS, while the collector block owns where its bytes come from and is validated by
# `hub.collectors.declarative`, which is the only code that can say whether a mapping is
# well formed. Listing them keeps the strict-unknown-key rule intact for everything else.
_TOP_LEVEL_KEYS = frozenset(
    {
        "dataset",
        "version",
        "title",
        "scope_kind",
        "key",
        "fields",
        "quality",
        "value_alias",
        "collector",
        "retention",
    }
)
_FIELD_KEYS = frozenset({"type", "unit", "values", "null_policy", "derived", "strategy"})
_QUALITY_KEYS = frozenset({"range"})


class FieldType(StrEnum):
    """The closed set of payload value kinds a schema may declare a field as."""

    NUMBER = "number"
    BOOL = "bool"
    TIMESTAMP = "timestamp"
    ENUM = "enum"
    STRING = "string"


@dataclass(frozen=True)
class FieldSpec:
    """One declared payload field: its type, and the rules `validate_payload` checks it against."""

    name: str
    type: FieldType
    unit: str = ""
    values: tuple[str, ...] = ()
    null_policy: str = "forbid"
    derived: str = ""
    strategy: bool = True


@dataclass(frozen=True)
class DatasetSchema:
    """A loaded, validated dataset schema: identity, declared fields, and the raw document
    that `.hash()` is computed over."""

    name: str
    version: int
    scope_kind: str
    key: tuple[str, ...]
    fields: dict[str, FieldSpec]
    title: str
    value_alias: str
    quality: dict[str, Any]
    derived_order: tuple[str, ...]
    raw: dict[str, Any] = field(repr=False)

    def hash(self) -> str:
        """Content address of the schema: changes whenever any declared byte of meaning does,
        so a backtest can cite this string and know exactly which contract it ran against."""
        digest = hashlib.sha256(canonical_json(self.raw).encode("utf-8")).hexdigest()
        return f"sha256:{digest}"

    def strategy_fields(self) -> tuple[FieldSpec, ...]:
        """Declared fields a strategy may read, in declaration order, excluding `strategy: false`
        fields such as free-text titles that exist for humans, not for a model."""
        return tuple(f for f in self.fields.values() if f.strategy)

    def derived_fields(self) -> tuple[FieldSpec, ...]:
        """Declared fields computed by an expression, in dependency order.

        The order matters and is fixed at load: a z-score over a surprise must be evaluated
        after the surprise itself, and the live pipeline and the compiler's recompute must walk
        the same sequence or they would disagree about the same record.
        """
        return tuple(self.fields[name] for name in self.derived_order)

    def validate_payload(self, fields: dict[str, Any]) -> dict[str, Any]:
        """Checks `fields` against the declared types, null policy, and quality ranges, and
        returns a NORMALISED payload -- numbers as canonical decimal text, everything else in
        its declared Python type -- so two payloads that mean the same fact hash identically.
        """
        out: dict[str, Any] = {}
        for name, value in fields.items():
            spec = self.fields.get(name)
            if spec is None:
                raise SchemaError(f"{self.name}: undeclared field {name!r} in payload")
            out[name] = self._validate_field(spec, value)
        range_map = self.quality.get("range", {})
        for name, bounds in range_map.items():
            if name in out and out[name] is not None:
                lo, hi = bounds
                num = Decimal(out[name])
                if num < Decimal(str(lo)) or num > Decimal(str(hi)):
                    raise SchemaError(f"{self.name}: field {name!r} value {out[name]!r} out of range {bounds!r}")
        return out

    def _validate_field(self, spec: FieldSpec, value: Any) -> Any:
        if value is None:
            if spec.null_policy == "forbid":
                raise SchemaError(f"{self.name}: field {spec.name!r} is null but null_policy is forbid")
            # `allow` and `allow_until_release` are both accepted here without distinction --
            # the embargo behaviour that would make `allow_until_release` differ from `allow`
            # (masking a null only until the dataset's scheduled release) is deliberately
            # deferred; do not assume it is enforced anywhere yet.
            return None
        if spec.type == FieldType.NUMBER:
            # `bool` is an `int` subclass, so it must be rejected explicitly here or `True`
            # would sail through `decimal_str` as the number 1.
            if isinstance(value, bool) or not isinstance(value, (int, str)):
                raise SchemaError(f"{self.name}: field {spec.name!r} expected type number, got {value!r}")
            try:
                return decimal_str(value)
            except RecordError as e:
                raise SchemaError(f"{self.name}: field {spec.name!r} is not a valid number: {value!r}") from e
        if spec.type == FieldType.BOOL:
            if not isinstance(value, bool):
                raise SchemaError(f"{self.name}: field {spec.name!r} expected type bool, got {value!r}")
            return value
        if spec.type == FieldType.TIMESTAMP:
            if not isinstance(value, int) or isinstance(value, bool):
                raise SchemaError(f"{self.name}: field {spec.name!r} expected type timestamp (int), got {value!r}")
            return value
        if spec.type == FieldType.ENUM:
            if not isinstance(value, int) or isinstance(value, bool):
                raise SchemaError(f"{self.name}: field {spec.name!r} expected type enum (int ordinal), got {value!r}")
            if not (0 <= value < len(spec.values)):
                raise SchemaError(
                    f"{self.name}: field {spec.name!r} ordinal {value!r} out of range for values {spec.values!r}"
                )
            return value
        if spec.type == FieldType.STRING:
            if not isinstance(value, str):
                raise SchemaError(f"{self.name}: field {spec.name!r} expected type string, got {value!r}")
            return value
        raise AssertionError(f"unreachable field type {spec.type!r}")


def _require_mapping(value: Any, what: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise SchemaError(f"{what} must be a mapping, got {value!r}")
    return value


def _reject_unknown_keys(mapping: dict[str, Any], allowed: frozenset[str], what: str) -> None:
    unknown = set(mapping) - allowed
    if unknown:
        raise SchemaError(f"{what}: unknown key(s) {sorted(unknown)!r}, allowed: {sorted(allowed)!r}")


def _load_field_spec(name: str, raw_spec: Any, dataset_name: str) -> FieldSpec:
    spec_map = _require_mapping(raw_spec, f"{dataset_name}: field {name!r}")
    _reject_unknown_keys(spec_map, _FIELD_KEYS, f"{dataset_name}: field {name!r}")

    if "type" not in spec_map:
        raise SchemaError(f"{dataset_name}: field {name!r} is missing required key 'type'")
    raw_type = spec_map["type"]
    try:
        field_type = FieldType(raw_type)
    except ValueError as e:
        raise SchemaError(f"{dataset_name}: field {name!r} has unknown type {raw_type!r}") from e

    unit = spec_map.get("unit", "")
    if not isinstance(unit, str):
        raise SchemaError(f"{dataset_name}: field {name!r} unit must be a string")

    raw_values = spec_map.get("values", [])
    if field_type == FieldType.ENUM:
        if not isinstance(raw_values, list) or len(raw_values) == 0:
            raise SchemaError(f"{dataset_name}: field {name!r} enum values must be a non-empty list")
        if not all(isinstance(v, str) for v in raw_values):
            raise SchemaError(f"{dataset_name}: field {name!r} enum values must all be strings")
        if any(v != v.lower() for v in raw_values):
            raise SchemaError(f"{dataset_name}: field {name!r} enum values must be lowercase")
        if len(set(raw_values)) != len(raw_values):
            raise SchemaError(f"{dataset_name}: field {name!r} enum values must be unique")
        values = tuple(raw_values)
    elif raw_values:
        raise SchemaError(f"{dataset_name}: field {name!r} values is only valid for type enum")
    else:
        values = ()

    null_policy = spec_map.get("null_policy", "forbid")
    if null_policy not in _NULL_POLICIES:
        raise SchemaError(
            f"{dataset_name}: field {name!r} null_policy {null_policy!r} must be one of {sorted(_NULL_POLICIES)!r}"
        )

    derived = spec_map.get("derived", "")
    if not isinstance(derived, str):
        raise SchemaError(f"{dataset_name}: field {name!r} derived must be a string expression")

    strategy = spec_map.get("strategy", True)
    if not isinstance(strategy, bool):
        raise SchemaError(f"{dataset_name}: field {name!r} strategy must be a boolean")

    return FieldSpec(
        name=name,
        type=field_type,
        unit=unit,
        values=values,
        null_policy=null_policy,
        derived=derived,
        strategy=strategy,
    )


def _derived_order(fields: dict[str, FieldSpec], dataset_name: str) -> tuple[str, ...]:
    """Validate every `derived:` expression and return the order to evaluate them in.

    A derived field may reference declared fields -- including another derived field -- plus the
    two envelope timestamps the pipeline merges in at evaluation time. Chaining is the normal
    case, not an edge case: a surprise is `actual - forecast` and its z-score is computed over
    that surprise, which is the shape the format's own worked example uses.

    What is genuinely unsafe is a CYCLE, because then no evaluation order exists and the result
    would depend on iteration order rather than on the declaration. Cycles are rejected here,
    and the topological order is returned so every consumer -- the live pipeline and the
    compiler's recompute-and-verify -- evaluates in the same sequence and cannot disagree.
    """
    declared = set(fields)
    available = declared | _ENVELOPE_EXPRESSION_NAMES
    dependencies: dict[str, set[str]] = {}
    for spec in fields.values():
        if not spec.derived:
            continue
        try:
            names = referenced_names(spec.derived)
        except SchemaError as e:
            raise SchemaError(f"{dataset_name}: field {spec.name!r} derived expression: {e}") from e
        if unknown := names - available:
            raise SchemaError(
                f"{dataset_name}: field {spec.name!r} derived expression references undeclared "
                f"name(s) {sorted(unknown)!r}"
            )
        if spec.name in names:
            raise SchemaError(f"{dataset_name}: field {spec.name!r} derived expression references itself")
        dependencies[spec.name] = {n for n in names if n in fields and fields[n].derived}

    order: list[str] = []
    state: dict[str, int] = {}

    def visit(name: str, trail: tuple[str, ...]) -> None:
        mark = state.get(name, 0)
        if mark == 2:
            return
        if mark == 1:
            cycle = " -> ".join([*trail[trail.index(name) :], name])
            raise SchemaError(f"{dataset_name}: derived fields form a cycle: {cycle}")
        state[name] = 1
        for dependency in sorted(dependencies[name]):
            visit(dependency, (*trail, name))
        state[name] = 2
        order.append(name)

    for name in sorted(dependencies):
        visit(name, ())
    return tuple(order)


def load_schema(path: Path) -> DatasetSchema:
    """Loads and fully validates a dataset schema file, failing closed on anything it does not
    recognise -- an unknown key at any level, a bad reference, a reserved name -- so a typo in
    the config is caught here rather than silently disabling a check downstream.
    """
    raw = load_path(path)
    doc = _require_mapping(raw, str(path))
    _reject_unknown_keys(doc, _TOP_LEVEL_KEYS, str(path))

    for required in ("dataset", "version", "title", "scope_kind", "key", "fields"):
        if required not in doc:
            raise SchemaError(f"{path}: missing required key {required!r}")

    dataset_name = doc["dataset"]
    if not isinstance(dataset_name, str) or not dataset_name:
        raise SchemaError(f"{path}: dataset must be a non-empty string")

    version = doc["version"]
    if not isinstance(version, int) or isinstance(version, bool):
        raise SchemaError(f"{dataset_name}: version must be an integer")

    title = doc["title"]
    if not isinstance(title, str):
        raise SchemaError(f"{dataset_name}: title must be a string")

    scope_kind = doc["scope_kind"]
    if scope_kind not in _SCOPE_KINDS:
        raise SchemaError(f"{dataset_name}: scope_kind {scope_kind!r} must be one of {sorted(_SCOPE_KINDS)!r}")

    raw_fields = _require_mapping(doc["fields"], f"{dataset_name}: fields")
    fields: dict[str, FieldSpec] = {}
    for name, raw_spec in raw_fields.items():
        if not FIELD_NAME_RE.match(name):
            raise SchemaError(f"{dataset_name}: {name!r} is not a valid field name")
        if name in RESERVED_FIELD_NAMES:
            raise SchemaError(f"{dataset_name}: field name {name!r} collides with envelope name {name!r}")
        if name in RESERVED_EXPRESSION_NAMES:
            raise SchemaError(
                f"{dataset_name}: field name {name!r} collides with a reserved expression function "
                f"name and could never be referenced from a derived expression"
            )
        fields[name] = _load_field_spec(name, raw_spec, dataset_name)

    derived_order = _derived_order(fields, dataset_name)

    raw_key = doc["key"]
    if not isinstance(raw_key, list) or len(raw_key) == 0:
        raise SchemaError(f"{dataset_name}: key must be a non-empty list")
    if not all(isinstance(k, str) for k in raw_key):
        raise SchemaError(f"{dataset_name}: key entries must all be strings")
    declared_names = set(fields)
    for k in raw_key:
        if k not in declared_names and k not in _ENVELOPE_KEY_NAMES:
            raise SchemaError(f"{dataset_name}: key names undeclared field {k!r}")
        if k in fields and fields[k].derived:
            raise SchemaError(
                f"{dataset_name}: key names derived field {k!r}; a derived value recomputes "
                f"whenever its inputs are corrected, so it cannot serve as the stable identity "
                f"of a fact across revisions"
            )
    key = tuple(raw_key)

    quality = _require_mapping(doc.get("quality", {}), f"{dataset_name}: quality")
    _reject_unknown_keys(quality, _QUALITY_KEYS, f"{dataset_name}: quality")
    range_map = quality.get("range", {})
    if range_map:
        range_map = _require_mapping(range_map, f"{dataset_name}: quality.range")
        for fname, bounds in range_map.items():
            if fname not in declared_names:
                raise SchemaError(f"{dataset_name}: quality.range names undeclared field {fname!r}")
            if not isinstance(bounds, list) or len(bounds) != 2:
                raise SchemaError(f"{dataset_name}: quality.range[{fname!r}] must be a two-element [min, max]")

    value_alias = doc.get("value_alias", "")
    if not isinstance(value_alias, str):
        raise SchemaError(f"{dataset_name}: value_alias must be a string")
    if value_alias and value_alias not in declared_names:
        raise SchemaError(f"{dataset_name}: value_alias names undeclared field {value_alias!r}")

    return DatasetSchema(
        name=dataset_name,
        version=version,
        scope_kind=scope_kind,
        key=key,
        fields=fields,
        title=title,
        value_alias=value_alias,
        quality=quality,
        derived_order=derived_order,
        raw=doc,
    )
