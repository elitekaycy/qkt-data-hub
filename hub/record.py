"""Re-export of the record type, which lives with the reader (`hubread.record`).

The reader library defines the format; the hub writes it. Keeping one definition means a
writer and a reader can never silently disagree about what a record is.
"""
from hubread.record import (
    ENVELOPE_VERSION,
    Availability,
    Record,
    canonical_json,
    decimal_str,
    sort_key,
)

__all__ = [
    "ENVELOPE_VERSION",
    "Availability",
    "Record",
    "canonical_json",
    "decimal_str",
    "sort_key",
]
