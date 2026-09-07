"""One exception per failure class, so a caller can tell a bad config from bad data.

The hub fails loud on anything it cannot interpret. A record it cannot validate is quarantined
rather than dropped silently; a schema or config it cannot parse stops the process at load,
because a hub running on a misread schema writes wrong history that later looks authentic.

These live with the reader because a consumer decoding an artifact needs the same vocabulary
for failure that the writer used when producing it.
"""
from __future__ import annotations


class HubError(Exception):
    """Base for every error this package raises."""


class ConfigError(HubError):
    """A config or schema file is malformed, or names something that does not exist."""


class SchemaError(ConfigError):
    """A dataset schema is invalid, or a record does not match its dataset's schema."""


class RecordError(HubError):
    """A record's envelope is missing a field, has a wrong type, or breaks an invariant."""


class ParseError(HubError):
    """A raw value could not be turned into the declared type."""


class StoreError(HubError):
    """A journal, snapshot, or raw-archive file is unreadable or inconsistent."""
