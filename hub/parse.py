"""Raw feed text to typed values, with unknown data staying unknown rather than becoming zero.

Every collector reads bytes a third party formatted for a human: percentages with a trailing
sign, "1.2K" instead of "1200", a dash where a number is missing, a timestamp with or without
an offset. This module is the one place those conventions are decoded, so a schema's `unit`
and `type` declarations mean the same thing regardless of which collector produced the value.

Two rules protect the rest of the pipeline. First, every numeric result is canonicalised through
`decimal_str`, so two collectors that captured the same fact as `0.30` and `.3` hash identically
and the deduplicator does not manufacture a false revision. Second, a naive timestamp is refused
rather than assumed to be UTC -- a sibling project shipped exactly that bug and every event
window it recorded was off by the source's UTC offset.
"""
from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, date, datetime, time
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from hubread.errors import ParseError
from hubread.record import decimal_str

_EPOCH_UTC = datetime(1970, 1, 1, tzinfo=UTC)

# Tokens a source uses to mean "no value here" rather than zero. Treating any of these as 0
# would fabricate data that was never observed; treating them as a parse failure would stop
# an entire ingest run over a single missing field. Both are wrong, so they become `None`.
_SENTINELS = frozenset({"-", "n/a", "—"})

_HUMAN_SUFFIXES: dict[str, Decimal] = {"k": Decimal("1e3"), "m": Decimal("1e6"), "b": Decimal("1e9")}


def _is_sentinel(raw: object) -> bool:
    """True for anything a source or an upstream decoder uses to spell "unknown"."""
    if raw is None:
        return True
    if not isinstance(raw, str):
        return False
    return raw.strip().lower() in _SENTINELS or raw.strip() == ""


def _require_str(raw: object) -> str:
    if not isinstance(raw, str):
        raise ParseError(f"expected a string, got {type(raw).__name__}: {raw!r}")
    return raw


def human_number(raw: object) -> str | None:
    """Decode a number written with thousands separators and/or a K/M/B magnitude suffix.

    The suffix is case-insensitive because sources are inconsistent about it ("1.2k" and
    "1.2K" appear from the same feed on different days). The result always passes through
    `decimal_str` so it matches byte-for-byte with the same value spelled a different way.
    """
    if _is_sentinel(raw):
        return None
    text = _require_str(raw).strip().replace(",", "")
    if not text:
        return None
    suffix = text[-1].lower()
    magnitude = _HUMAN_SUFFIXES.get(suffix)
    if magnitude is not None:
        text = text[:-1]
    try:
        value = Decimal(text)
    except InvalidOperation as e:
        raise ParseError(f"not a number: {raw!r}") from e
    if magnitude is not None:
        value *= magnitude
    return decimal_str(value)


def percent_or_number(raw: object) -> str | None:
    """Decode a plain number or a percentage, keeping percentages in percent points.

    The schema declares `unit: pct` for these fields, so the value is stored as the source
    printed it (e.g. "0.3" for "0.30%"), not divided by 100 -- dividing here would silently
    change the unit a schema author is relying on.
    """
    if _is_sentinel(raw):
        return None
    text = _require_str(raw).strip()
    if text.endswith("%"):
        text = text[:-1].strip()
    return human_number(text)


def iso8601_with_offset(raw: object) -> int:
    """Parse an ISO-8601 timestamp that MUST carry a UTC offset, to epoch milliseconds.

    A timestamp with no offset is ambiguous about which wall clock it was written against.
    Guessing UTC for it is how a New York release time silently becomes four hours wrong;
    refusing it here forces the collector to fix the source mapping instead of the pipeline
    quietly mis-recording history.
    """
    text = _require_str(raw).strip()
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as e:
        raise ParseError(f"not an iso8601 timestamp: {raw!r}") from e
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ParseError(f"timestamp {raw!r} has no UTC offset")
    delta = parsed - _EPOCH_UTC
    return delta.days * 86_400_000 + delta.seconds * 1000 + delta.microseconds // 1000


def date_in_zone(raw: object, zone: str, hour: int = 0) -> int:
    """Anchor a bare calendar date to a wall-clock hour in a named zone, in epoch milliseconds.

    Sources that publish a release calendar often give only a date, with the hour implied by
    the exchange's local time and the offset then decided by whichever side of a DST
    transition the date falls on. Resolving that here, via `zoneinfo`, is what keeps a July
    release and a January release at the same nominal hour landing on the correct UTC hour
    instead of a fixed offset baked in at collector-writing time.
    """
    text = _require_str(raw).strip()
    try:
        day = date.fromisoformat(text)
    except ValueError as e:
        raise ParseError(f"not an iso8601 date: {raw!r}") from e
    try:
        tz = ZoneInfo(zone)
    except ZoneInfoNotFoundError as e:
        raise ParseError(f"unknown zone: {zone!r}") from e
    anchored = datetime.combine(day, time(hour=hour), tzinfo=tz)
    delta = anchored - _EPOCH_UTC
    return delta.days * 86_400_000 + delta.seconds * 1000 + delta.microseconds // 1000


def enum_ordinal(raw: object, mapping: dict[str, int]) -> int | None:
    """Map a source's category label to the schema's ordinal, or `None` for an unmapped label.

    Matching is case-insensitive because sources are inconsistent about capitalisation for the
    same category. A label the mapping does not cover is unknown data, not a malformed
    document, so it returns `None` rather than raising -- an unlisted category should not stop
    an otherwise-good record from being journaled.
    """
    if _is_sentinel(raw):
        return None
    text = _require_str(raw).strip().lower()
    return mapping.get(text)


_TRUE_TOKENS = frozenset({"true", "yes", "y", "1"})
_FALSE_TOKENS = frozenset({"false", "no", "n", "0"})


def boolean(raw: object) -> bool | None:
    """Decode a source's yes/no spelling, refusing tokens that are neither.

    Unlike `enum_ordinal`, a boolean field has no open category space -- a token that is
    neither a known true- nor false-spelling is a formatting problem worth surfacing, not a
    third truth value to fold into `None`.
    """
    if _is_sentinel(raw):
        return None
    text = _require_str(raw).strip().lower()
    if text in _TRUE_TOKENS:
        return True
    if text in _FALSE_TOKENS:
        return False
    raise ParseError(f"not a recognised boolean: {raw!r}")


PARSERS: dict[str, Callable[..., object]] = {
    "human_number": human_number,
    "percent_or_number": percent_or_number,
    "iso8601_with_offset": iso8601_with_offset,
    "date_in_zone": date_in_zone,
    "enum_ordinal": enum_ordinal,
    "boolean": boolean,
}
"""Name to function, so a declarative collector config can select a parser by string."""


def apply(name: str, raw: object, **kwargs: object) -> object:
    """Dispatch to a named parser, failing closed on a name the config author mistyped.

    A collector config is data, not code -- a bad parser name here must produce a clear
    `ParseError` at load or ingest time, never a silent `KeyError` deep in a pipeline run.
    """
    try:
        parser = PARSERS[name]
    except KeyError as e:
        raise ParseError(f"unknown parser: {name!r}") from e
    return parser(raw, **kwargs)
