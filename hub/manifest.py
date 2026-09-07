"""The manifest: the one file that tells a consumer what the hub has compiled and where.

A backtest cites a manifest hash and the per-dataset snapshot hashes it read (determinism
contract, rule 8) -- so this module's job is to make that citation checkable: every window this
records names an exact file and an exact SHA-256, and `verify` can walk the whole thing and say
whether the bytes on disk still match what was promised.

Each dataset's field table is written here too, not only into its schema YAML, because a reader
that only ever consumes compiled data (`hubread.reader.Reader.schema`) must be able to answer
"what fields does this dataset have" without parsing YAML or importing the hub's schema loader --
the manifest is the one artifact a stdlib-only consumer needs.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

#: The manifest format version. Bumped only if the JSON shape below changes incompatibly.
MANIFEST_VERSION = 1

MANIFEST_FILENAME = "manifest.json"


@dataclass(frozen=True, slots=True)
class WindowEntry:
    """One compiled snapshot window: its coverage, where it lives, and what it depends on.

    `raw_deps` is the lineage a consumer needs to answer "what raw archives does this window's
    evidence ultimately rest on" (determinism contract, rule 5) without re-reading every record.
    """

    from_day: str
    to_day: str
    file: str
    sha256: str
    records: int
    raw_deps: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "from": self.from_day,
            "to": self.to_day,
            "file": self.file,
            "sha256": self.sha256,
            "records": self.records,
            "raw_deps": list(self.raw_deps),
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> WindowEntry:
        return cls(
            from_day=raw["from"],
            to_day=raw["to"],
            file=raw["file"],
            sha256=raw["sha256"],
            records=raw["records"],
            raw_deps=tuple(raw.get("raw_deps", [])),
        )


@dataclass(frozen=True, slots=True)
class FieldEntry:
    """One schema field, as recorded in the manifest.

    This is a deliberately smaller shape than `hub.schema.FieldSpec`: it carries only what a
    reader needs to interpret a compiled value (name, type, unit, enum labels, whether it is
    strategy-visible or derived) -- so `hubread.reader.Reader.schema` never has to import the
    hub's YAML-backed schema loader to answer a consumer's question.
    """

    name: str
    type: str
    unit: str
    values: tuple[str, ...]
    null_policy: str
    derived: str
    strategy: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "type": self.type,
            "unit": self.unit,
            "values": list(self.values),
            "null_policy": self.null_policy,
            "derived": self.derived,
            "strategy": self.strategy,
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> FieldEntry:
        return cls(
            name=raw["name"],
            type=raw["type"],
            unit=raw["unit"],
            values=tuple(raw.get("values", [])),
            null_policy=raw["null_policy"],
            derived=raw.get("derived", ""),
            strategy=raw["strategy"],
        )


@dataclass(slots=True)
class DatasetManifest:
    """Everything the manifest records about one dataset: identity, coverage, and schema shape.

    `windows` is kept in ascending `from_day` order so `save` writes deterministic JSON and a
    human reading the file sees coverage chronologically.
    """

    schema_hash: str
    schema_path: str
    scope_kind: str
    key: tuple[str, ...]
    fields: tuple[FieldEntry, ...]
    last_seq: int = 0
    last_known_at: int | None = None
    windows: list[WindowEntry] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_hash": self.schema_hash,
            "schema_path": self.schema_path,
            "scope_kind": self.scope_kind,
            "key": list(self.key),
            "fields": [f.to_dict() for f in self.fields],
            "last_seq": self.last_seq,
            "last_known_at": self.last_known_at,
            "windows": [w.to_dict() for w in sorted(self.windows, key=lambda w: w.from_day)],
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> DatasetManifest:
        return cls(
            schema_hash=raw["schema_hash"],
            schema_path=raw["schema_path"],
            scope_kind=raw["scope_kind"],
            key=tuple(raw.get("key", [])),
            fields=tuple(FieldEntry.from_dict(f) for f in raw.get("fields", [])),
            last_seq=raw.get("last_seq", 0),
            last_known_at=raw.get("last_known_at"),
            windows=[WindowEntry.from_dict(w) for w in raw.get("windows", [])],
        )


@dataclass(slots=True)
class Manifest:
    """The hub root's compiled state: one `DatasetManifest` per dataset that has been compiled.

    `load` never raises on a missing file -- a hub root before its first compile has no manifest
    yet, and treating that as an error would make `compile_all` unable to bootstrap one.
    """

    v: int = MANIFEST_VERSION
    generated_at: int = 0
    datasets: dict[str, DatasetManifest] = field(default_factory=dict)

    @classmethod
    def load(cls, root: Path | str) -> Manifest:
        """Reads `<root>/manifest.json`, or returns an empty manifest if it does not exist yet."""
        path = Path(root) / MANIFEST_FILENAME
        if not path.is_file():
            return cls()
        raw = json.loads(path.read_text(encoding="utf-8"))
        return cls(
            v=raw.get("v", MANIFEST_VERSION),
            generated_at=raw.get("generated_at", 0),
            datasets={name: DatasetManifest.from_dict(d) for name, d in raw.get("datasets", {}).items()},
        )

    def save(self, root: Path | str) -> None:
        """Writes this manifest to `<root>/manifest.json`, keys sorted for a stable diff.

        Only the JSON's key order and dataset order are made deterministic here; `generated_at`
        is a wall-clock stamp and is expected to change between saves -- the bytes this promises
        to stay fixed are the *snapshot* files a window's `sha256` names, not the manifest itself.
        """
        path = Path(root) / MANIFEST_FILENAME
        path.parent.mkdir(parents=True, exist_ok=True)
        doc = {
            "v": self.v,
            "generated_at": self.generated_at,
            "datasets": {name: self.datasets[name].to_dict() for name in sorted(self.datasets)},
        }
        path.write_text(json.dumps(doc, indent=2, sort_keys=False) + "\n", encoding="utf-8")
