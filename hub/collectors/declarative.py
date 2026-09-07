"""A collector declared in YAML rather than written in Python.

Most feeds are a JSON array or a CSV table where each row is a fact, and writing a class per
feed would mean the same select-filter-map logic re-implemented once per provider, each with its
own bugs. Declaring the mapping instead means a new dataset is a file review, not a code review,
and the one shared implementation is the one that gets hardened.

The mapping is validated at load, strictly: an unknown key is a hard error. A silently ignored
`nul_if` would produce a dataset that looks fine and quietly stores zeroes where the source said
nothing, which is exactly the failure this project exists to make impossible.
"""
from __future__ import annotations

import csv
import io
import json
from dataclasses import dataclass
from typing import Any

from hub.collectors import Candidate
from hub.collectors.http import HttpSource
from hub.parse import apply as apply_parser
from hub.rawstore import RawBlob
from hubread.errors import ConfigError, ParseError
from hubread.record import Availability

_COLLECTOR_KEYS = frozenset(
    {
        "kind",
        "url",
        "headers",
        "cadence",
        "cadence_near_event",
        "record_path",
        "where",
        "scope",
        "effective_at",
        "known_at",
        "period_start",
        "period_end",
        "fields",
        "availability",
        "csv_value_column",
    }
)
_NEAR_EVENT_KEYS = frozenset({"before", "after", "every", "anchor"})
_SCOPE_KEYS = frozenset({"from", "map", "const", "upper"})
_TIME_KEYS = frozenset({"from", "parse", "zone", "hour", "const"})
_FIELD_KEYS = frozenset({"from", "parse", "map", "null_if", "const", "zone", "hour"})
_KINDS = frozenset({"http_json", "http_csv"})


def _require_map(value: Any, path: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigError(f"{path} must be a mapping")
    return value


def _reject_unknown(path: str, got: Any, allowed: frozenset[str]) -> dict[str, Any]:
    block = _require_map(got, path)
    if unknown := set(block) - allowed:
        raise ConfigError(f"{path}: unknown key(s) {sorted(unknown)}, allowed: {sorted(allowed)}")
    return block


@dataclass(frozen=True, slots=True)
class Cadence:
    """How often to fetch, and how much faster to fetch near a scheduled event.

    The tighter cadence exists because a consensus feed fills in an actual within seconds of a
    release, and a six-hour poll would record that arrival six hours late -- turning an
    `observed` availability into a lie about when we could have known.
    """

    steady_seconds: float
    retry_seconds: float = 300.0
    near_before_seconds: float = 0.0
    near_after_seconds: float = 0.0
    near_every_seconds: float = 0.0
    anchor: str = "effective_at"


_UNITS = {"s": 1.0, "m": 60.0, "h": 3600.0, "d": 86400.0}


def duration_seconds(text: object, path: str) -> float:
    """Parse `30s`, `15m`, `6h`, `1d` into seconds. A bare number is seconds."""
    raw = str(text).strip().lower()
    if not raw:
        raise ConfigError(f"{path}: empty duration")
    if raw[-1] in _UNITS:
        try:
            return float(raw[:-1]) * _UNITS[raw[-1]]
        except ValueError as e:
            raise ConfigError(f"{path}: bad duration {text!r}") from e
    try:
        return float(raw)
    except ValueError as e:
        raise ConfigError(f"{path}: bad duration {text!r}") from e


def _select(payload: Any, record_path: str) -> list[Any]:
    """Resolve a deliberately tiny subset of JSONPath: `$[*]` and `$.a.b[*]`.

    A full JSONPath engine would be a dependency and a surface for surprises; every feed in the
    catalogue is either a top-level array or one array nested under named keys.
    """
    path = record_path.strip()
    if not path.startswith("$"):
        raise ConfigError(f"record_path must start with '$': {record_path!r}")
    body = path[1:]
    if not body.endswith("[*]"):
        raise ConfigError(f"record_path must end with '[*]': {record_path!r}")
    node = payload
    for part in [p for p in body[:-3].split(".") if p]:
        if not isinstance(node, dict) or part not in node:
            return []
        node = node[part]
    return list(node) if isinstance(node, list) else []


def _matches(row: dict[str, Any], where: dict[str, Any]) -> bool:
    for key, want in where.items():
        got = row.get(key)
        if isinstance(want, list):
            if not any(str(got).strip().lower() == str(w).strip().lower() for w in want):
                return False
        elif str(got).strip().lower() != str(want).strip().lower():
            return False
    return True


class DeclarativeSource:
    """A `Source` built from a dataset schema's `collector:` block."""

    def __init__(self, dataset: str, collector: dict[str, Any], *, opener: Any = None) -> None:
        block = _reject_unknown(f"{dataset}.collector", collector, _COLLECTOR_KEYS)
        self._dataset = dataset
        self._kind = str(block.get("kind", "")).strip()
        if self._kind not in _KINDS:
            raise ConfigError(f"{dataset}.collector.kind must be one of {sorted(_KINDS)}, got {self._kind!r}")
        url = block.get("url")
        if not isinstance(url, str) or not url:
            raise ConfigError(f"{dataset}.collector.url is required")
        self._url = url
        headers = {str(k): str(v) for k, v in dict(block.get("headers") or {}).items()}
        expect = "json" if self._kind == "http_json" else "csv"
        kwargs: dict[str, Any] = {"expect_content_type": expect, "headers": headers}
        if opener is not None:
            kwargs["opener"] = opener
        self._http = HttpSource(dataset, url, **kwargs)
        self._record_path = str(block.get("record_path", "$[*]"))
        self._where = _require_map(block.get("where") or {}, f"{dataset}.collector.where")
        self._scope = _reject_unknown(f"{dataset}.collector.scope", block.get("scope") or {}, _SCOPE_KEYS)
        self._effective = _reject_unknown(
            f"{dataset}.collector.effective_at", block.get("effective_at") or {}, _TIME_KEYS
        )
        self._known_at = block.get("known_at", "ingest_time")
        self._period_start = (
            _reject_unknown(f"{dataset}.collector.period_start", block["period_start"], _TIME_KEYS)
            if "period_start" in block
            else None
        )
        self._fields: dict[str, dict[str, Any]] = {}
        for name, spec in _require_map(block.get("fields") or {}, f"{dataset}.collector.fields").items():
            self._fields[name] = _reject_unknown(f"{dataset}.collector.fields.{name}", spec, _FIELD_KEYS)
        self._availability = Availability(str(block.get("availability", "observed")))
        self._csv_value_column = block.get("csv_value_column")
        near = block.get("cadence_near_event")
        near_block = _reject_unknown(f"{dataset}.collector.cadence_near_event", near, _NEAR_EVENT_KEYS) if near else {}
        self.cadence = Cadence(
            steady_seconds=duration_seconds(block.get("cadence", "6h"), f"{dataset}.collector.cadence"),
            near_before_seconds=duration_seconds(near_block.get("before", "0s"), "cadence_near_event.before"),
            near_after_seconds=duration_seconds(near_block.get("after", "0s"), "cadence_near_event.after"),
            near_every_seconds=duration_seconds(near_block.get("every", "0s"), "cadence_near_event.every"),
            anchor=str(near_block.get("anchor", "effective_at")),
        )

    @property
    def name(self) -> str:
        return self._dataset

    @property
    def parser(self) -> str:
        return f"decl/{self._kind}@1"

    @property
    def url(self) -> str:
        return self._url

    def fetch(self, timeout_seconds: float) -> RawBlob | None:
        return self._http.fetch(timeout_seconds)

    # -- parsing -------------------------------------------------------------------------

    def _rows(self, blob: RawBlob) -> list[dict[str, Any]]:
        text = blob.body.decode("utf-8", errors="strict")
        if self._kind == "http_json":
            payload = json.loads(text)
            return [r for r in _select(payload, self._record_path) if isinstance(r, dict)]
        reader = csv.DictReader(io.StringIO(text))
        return [dict(r) for r in reader]

    def _resolve_time(self, spec: dict[str, Any], row: dict[str, Any], path: str) -> int | None:
        if "const" in spec:
            return int(spec["const"])
        source_key = spec.get("from")
        if source_key is None:
            return None
        raw = row.get(str(source_key))
        parser_name = str(spec.get("parse", "iso8601_with_offset"))
        kwargs: dict[str, Any] = {}
        if "zone" in spec:
            kwargs["zone"] = str(spec["zone"])
        if "hour" in spec:
            kwargs["hour"] = int(spec["hour"])
        value = apply_parser(parser_name, raw, **kwargs)
        if value is None:
            return None
        if not isinstance(value, int):
            raise ParseError(f"{path}: parser {parser_name} did not produce a timestamp")
        return value

    def _resolve_scope(self, row: dict[str, Any]) -> str | None:
        if "const" in self._scope:
            return str(self._scope["const"])
        source_key = self._scope.get("from")
        if source_key is None:
            return "ALL"
        raw = row.get(str(source_key))
        if raw is None:
            return None
        token = str(raw).strip()
        mapping = {str(k): str(v) for k, v in dict(self._scope.get("map") or {}).items()}
        token = mapping.get(token, token)
        return token.upper() if self._scope.get("upper", True) else token

    def _resolve_field(self, name: str, spec: dict[str, Any], row: dict[str, Any]) -> Any:
        if "const" in spec:
            return spec["const"]
        source_key = str(spec.get("from", name))
        raw = row.get(source_key)
        for sentinel in spec.get("null_if") or []:
            if raw == sentinel or (isinstance(raw, str) and raw.strip() == str(sentinel).strip()):
                return None
        parser_name = spec.get("parse")
        if parser_name is None:
            return None if raw is None else str(raw)
        kwargs: dict[str, Any] = {}
        if "map" in spec:
            kwargs["mapping"] = {str(k).lower(): int(v) for k, v in dict(spec["map"]).items()}
        if "zone" in spec:
            kwargs["zone"] = str(spec["zone"])
        if "hour" in spec:
            kwargs["hour"] = int(spec["hour"])
        return apply_parser(str(parser_name), raw, **kwargs)

    def parse(self, blob: RawBlob) -> list[Candidate]:
        """Pure transform from archived bytes to candidates. Never does I/O.

        A row that cannot yield a scope or an `effective_at` is skipped rather than guessed at:
        a fact whose subject or timing is unknown is not a fact.
        """
        out: list[Candidate] = []
        for row in self._rows(blob):
            if self._where and not _matches(row, self._where):
                continue
            scope = self._resolve_scope(row)
            if scope is None:
                continue
            effective_at = self._resolve_time(self._effective, row, f"{self._dataset}.effective_at")
            if effective_at is None:
                continue
            fields = {name: self._resolve_field(name, spec, row) for name, spec in self._fields.items()}
            period_start = (
                self._resolve_time(self._period_start, row, f"{self._dataset}.period_start")
                if self._period_start
                else None
            )
            known_at = effective_at if self._known_at == "effective_at" else None
            out.append(
                Candidate(
                    scope=scope,
                    key="",
                    effective_at=effective_at,
                    fields=fields,
                    period_start=period_start,
                    known_at=known_at,
                    availability=self._availability,
                    title=str(row.get("title", "")),
                )
            )
        return out
