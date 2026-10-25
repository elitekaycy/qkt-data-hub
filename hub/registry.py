"""Every dataset the hub knows about, loaded once and validated strictly.

A dataset is a file, not code, so the registry is just "read the directory". Loading is strict
and eager on purpose: a schema that would fail must fail at start-up, when an operator is
watching, rather than at 03:00 on the first row that happens to exercise the broken field.
"""
from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any

from hub.collectors.declarative import DeclarativeSource
from hub.schema import DatasetSchema, load_schema
from hubread.errors import ConfigError


def _load_code_source(dataset: str, collector: dict[str, Any], datasets_dir: Path) -> Any:
    """Instantiate a hand-written `Source` named by `collector.module`/`collector.class`.

    This is the registration point `docs/EXTENDING.md` describes for a feed no declarative
    mapping can express (a rolling-window URL, a POST body, a PDF). The class is imported by
    dotted path rather than pre-wired here, so adding one is still a dataset-directory change,
    not an edit to this file.
    """
    module_name = collector.get("module")
    class_name = collector.get("class")
    if not module_name or not class_name:
        raise ConfigError(f"{dataset}.collector: kind 'code' requires 'module' and 'class'")
    module = importlib.import_module(str(module_name))
    cls = getattr(module, str(class_name), None)
    if cls is None:
        raise ConfigError(f"{dataset}.collector: {module_name}.{class_name} not found")
    options = {k: v for k, v in collector.items() if k not in ("kind", "module", "class")}
    # A collector that takes settings (a schedule file, a list of event kinds) declares a
    # `from_config` constructor; one that does not must not be handed settings it would ignore,
    # because a silently ignored key is a check that never runs.
    factory = getattr(cls, "from_config", None)
    if factory is not None:
        return factory(dataset, options, datasets_dir)
    if options:
        raise ConfigError(f"{dataset}.collector: {class_name} takes no options, got {sorted(options)!r}")
    return cls()


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
            if isinstance(collector, dict):
                kind = str(collector.get("kind", ""))
                if kind.startswith("http_"):
                    sources[schema.name] = DeclarativeSource(schema.name, collector, opener=opener)
                elif kind == "code":
                    sources[schema.name] = _load_code_source(schema.name, collector, directory)
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
