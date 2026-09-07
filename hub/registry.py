"""Every dataset the hub knows about, loaded once and validated strictly.

A dataset is a file, not code, so the registry is just "read the directory". Loading is strict
and eager on purpose: a schema that would fail must fail at start-up, when an operator is
watching, rather than at 03:00 on the first row that happens to exercise the broken field.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from hub.collectors.declarative import DeclarativeSource
from hub.schema import DatasetSchema, load_schema
from hubread.errors import ConfigError


class Registry:
    """The loaded set of dataset schemas, plus a collector for each that declares one."""

    def __init__(self, schemas: dict[str, DatasetSchema], sources: dict[str, Any]) -> None:
        self._schemas = schemas
        self._sources = sources

    @classmethod
    def load(cls, datasets_dir: Path | str, *, opener: Any = None) -> Registry:
        directory = Path(datasets_dir)
        if not directory.is_dir():
            raise ConfigError(f"datasets directory not found: {directory}")
        schemas: dict[str, DatasetSchema] = {}
        sources: dict[str, Any] = {}
        for path in sorted(directory.glob("*.yaml")):
            schema = load_schema(path)
            if schema.name in schemas:
                raise ConfigError(f"duplicate dataset {schema.name} in {directory}")
            schemas[schema.name] = schema
            collector = schema.raw.get("collector")
            if isinstance(collector, dict) and str(collector.get("kind", "")).startswith("http_"):
                sources[schema.name] = DeclarativeSource(schema.name, collector, opener=opener)
        return cls(schemas, sources)

    def datasets(self) -> list[str]:
        return sorted(self._schemas)

    def schema(self, name: str) -> DatasetSchema:
        if name not in self._schemas:
            raise ConfigError(f"unknown dataset {name!r}; known: {self.datasets()}")
        return self._schemas[name]

    def source(self, name: str) -> Any:
        return self._sources.get(name)

    def collecting(self) -> list[str]:
        """Datasets that fetch from a provider, as opposed to derived or internal ones."""
        return sorted(self._sources)
