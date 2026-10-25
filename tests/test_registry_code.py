"""Tests for registering a hand-written collector by dotted path (`collector.kind: code`).

A feed no declarative mapping can express is still added by a dataset file, so the registry must
import the named class, and must refuse a file that names a class it cannot find.
"""
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from hub.registry import Registry
from hubread.errors import ConfigError

DATASET = """
dataset: cal.test_code
version: 1
title: code collector test
scope_kind: currency
key: [effective_at, scope]
collector:
  kind: code
  module: tests.test_registry_code
  class: {cls}
fields:
  at: {{ type: timestamp, null_policy: allow }}
"""


class StubSource:
    name = "cal.test_code"


class RegistryCodeCollectorTest(unittest.TestCase):
    def _load(self, cls: str) -> Registry:
        with TemporaryDirectory() as d:
            Path(d, "cal.test_code.yaml").write_text(DATASET.format(cls=cls))
            return Registry.load(d)

    def test_instantiates_the_named_class(self):
        registry = self._load("StubSource")
        self.assertIsInstance(registry.source("cal.test_code"), StubSource)

    def test_refuses_a_class_that_does_not_exist(self):
        with self.assertRaisesRegex(ConfigError, "not found"):
            self._load("Missing")


if __name__ == "__main__":
    unittest.main()
