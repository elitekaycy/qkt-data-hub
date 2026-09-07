"""The qkt-data-hub reader: the format, and how to read it point-in-time.

This package is deliberately standalone and stdlib-only. A consumer that must never grow a
dependency -- a kill-switch daemon, a trading engine's data path -- vendors or installs this
alone and can decode every artifact the hub writes. The hub imports it rather than the other
way round, so the writer and the reader cannot drift apart.
"""

__version__ = "0.1.4"
