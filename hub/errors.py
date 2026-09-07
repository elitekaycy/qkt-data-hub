"""Re-export of the format's error types, which live with the reader.

`hubread` owns the record format so a consumer can embed it with zero coupling; the hub is a
writer on top of it. Hub code imports errors from here so a reader-side change stays one edit.
"""
from hubread.errors import (
    ConfigError,
    HubError,
    ParseError,
    RecordError,
    SchemaError,
    StoreError,
)

__all__ = ["ConfigError", "HubError", "ParseError", "RecordError", "SchemaError", "StoreError"]
